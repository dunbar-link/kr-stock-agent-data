"""
refresh_portfolio_prices.py

전체 daily_run 없이 보유종목 현재가만 빠르게 갱신합니다.
- portfolio.json / wababa-ai-portfolio.json 읽기 전용
- recommendation-history.json 의 portfolioSummary / aiPortfolioSummary 갱신
- 매수단가·수량·매수금액은 변경하지 않음
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import pandas as pd
    from pykrx import stock
except ImportError as e:
    print(json.dumps({"ok": False, "error": f"pykrx 임포트 실패: {e}"}))
    sys.exit(1)


ROOT_DIR = Path(__file__).resolve().parents[1]
PORTFOLIO_PATH = ROOT_DIR / "portfolio.json"
AI_PORTFOLIO_PATH = ROOT_DIR / "wababa-ai-portfolio.json"
RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"


def now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


def safe_number(value, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    try:
        result = float(value)
        return fallback if (result != result) else result  # NaN check
    except (TypeError, ValueError):
        return fallback


def read_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"파일 없음: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def find_latest_business_day() -> str:
    """pykrx 로 가장 최근 거래일을 찾아 YYYYMMDD 문자열로 반환합니다."""
    kst = now_kst()
    for days_back in range(0, 14):
        candidate_dt = kst - timedelta(days=days_back)
        candidate = candidate_dt.strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_ticker(candidate, market="KOSPI")
            if df is None or df.empty:
                continue

            # pykrx 1.2.7은 비영업일(토/일/공휴일)에도 empty=False인 DataFrame을
            # 반환하지만 모든 값이 0 — 종가 합계로 실제 거래 여부를 검증한다.
            if "종가" in df.columns:
                close_series = df["종가"]
            else:
                close_series = df.iloc[:, 3]
            close_sum = pd.to_numeric(close_series, errors="coerce").fillna(0).sum()

            if close_sum > 0:
                print(f"[INFO] 최근 거래일: {candidate}", flush=True)
                return candidate

            print(f"[INFO] 비영업일(종가 합계=0) 건너뜀: {candidate}", flush=True)
        except Exception as err:
            print(f"[WARN] 거래일 확인 실패 {candidate}: {err}", flush=True)
    raise RuntimeError("최근 거래일을 찾지 못했습니다. (14일 탐색 실패)")


def fetch_price_map(base_date: str) -> dict[str, float]:
    """KOSPI + KOSDAQ 종가를 티커 → 가격 dict 로 반환합니다."""
    price_map: dict[str, float] = {}

    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(base_date, market=market)
            if df is None or df.empty:
                print(f"[WARN] {market} 데이터 없음", flush=True)
                continue
            df = df.reset_index()
            # 티커 컬럼 이름은 '티커'
            ticker_col = next((c for c in df.columns if "티커" in c or c == "Ticker"), None)
            close_col = next((c for c in df.columns if "종가" in c or c == "Close"), None)
            if ticker_col is None or close_col is None:
                print(f"[WARN] {market} 컬럼 인식 실패: {list(df.columns)}", flush=True)
                continue
            for _, row in df.iterrows():
                ticker = str(row[ticker_col]).zfill(6)
                price = safe_number(row[close_col])
                if ticker and price > 0:
                    price_map[ticker] = price
            print(f"[INFO] {market} 가격 로드 완료: {len(df)}개", flush=True)
        except Exception as err:
            print(f"[WARN] {market} 조회 실패: {err}", flush=True)

    return price_map


def get_holding_codes(portfolio: dict) -> list[str]:
    positions = portfolio.get("positions", [])
    if not isinstance(positions, list):
        return []
    codes = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        code = str(pos.get("code") or pos.get("stockCode") or "").strip().zfill(6)
        if code and code not in codes:
            codes.append(code)
    return codes


def refresh_positions(
    positions: list,
    price_map: dict[str, float],
) -> tuple[list, float, float, float]:
    """
    positions 리스트의 현재가·평가금액·평가손익·수익률을 갱신합니다.
    반환: (갱신된 positions, total_buy_amount, total_eval_amount, total_profit_amount)
    """
    updated = []
    total_buy = 0.0
    total_eval = 0.0

    for pos in positions:
        if not isinstance(pos, dict):
            updated.append(pos)
            continue

        code = str(pos.get("code") or "").strip().zfill(6)
        buy_price = safe_number(pos.get("buyPrice"))
        quantity = safe_number(pos.get("quantity"))
        buy_amount = safe_number(pos.get("buyAmount"), buy_price * quantity)

        # 현재가: price_map 에 있으면 갱신, 없으면 기존값 유지
        current_price = price_map.get(code, safe_number(pos.get("currentPrice"), buy_price))

        eval_amount = round(current_price * quantity, 0) if current_price > 0 and quantity > 0 else 0.0
        profit_amount = round(eval_amount - buy_amount, 0)
        profit_rate = round((profit_amount / buy_amount) * 100, 2) if buy_amount > 0 else 0.0

        new_pos = {
            **pos,
            "currentPrice": current_price,
            "evaluationAmount": eval_amount,
            "profitAmount": profit_amount,
            "profitRate": profit_rate,
        }
        updated.append(new_pos)
        total_buy += buy_amount
        total_eval += eval_amount

    total_profit = round(total_eval - total_buy, 0)
    return updated, total_buy, total_eval, total_profit


def refresh_summary(summary: dict, positions: list, price_map: dict[str, float]) -> dict:
    """portfolioSummary / aiPortfolioSummary 를 갱신합니다."""
    updated_positions, total_buy, total_eval, total_profit = refresh_positions(
        positions, price_map
    )

    cash = safe_number(summary.get("cash"))
    initial_capital = safe_number(summary.get("initialCapital"), cash)
    total_asset = cash + total_eval
    total_profit_rate = round(((total_asset - initial_capital) / initial_capital) * 100, 2) if initial_capital > 0 else 0.0
    invested_rate = round((total_eval / total_asset) * 100, 2) if total_asset > 0 else 0.0
    cash_rate = round((cash / total_asset) * 100, 2) if total_asset > 0 else 100.0

    return {
        **summary,
        "totalBuyAmount": round(total_buy, 0),
        "totalEvaluationAmount": round(total_eval, 0),
        "totalProfitAmount": total_profit,
        "totalAssetAmount": round(total_asset, 0),
        "totalProfitRate": total_profit_rate,
        "investedRate": invested_rate,
        "cashRate": cash_rate,
        "positions": updated_positions,
        "priceRefreshedAt": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main() -> dict:
    print("[START] 보유종목 현재가 갱신 시작", flush=True)

    # 파일 읽기
    try:
        portfolio = read_json(PORTFOLIO_PATH)
        ai_portfolio = read_json(AI_PORTFOLIO_PATH)
        history = read_json(RECOMMENDATION_HISTORY_PATH)
    except FileNotFoundError as e:
        return {"ok": False, "error": str(e)}

    # 보유 종목 코드 수집
    wababa_codes = get_holding_codes(portfolio)
    ai_codes = get_holding_codes(ai_portfolio)
    all_codes = list(set(wababa_codes + ai_codes))
    print(f"[INFO] 보유종목: 와바바={wababa_codes}, AI={ai_codes}", flush=True)

    if not all_codes:
        return {"ok": True, "message": "보유 종목 없음 — 갱신 스킵", "updatedCount": 0}

    # 최근 거래일 + 현재가 조회
    try:
        base_date = find_latest_business_day()
        price_map = fetch_price_map(base_date)
    except Exception as e:
        return {"ok": False, "error": f"현재가 조회 실패: {e}"}

    # 조회된 종목 필터링
    found = {code: price_map[code] for code in all_codes if code in price_map}
    missing = [code for code in all_codes if code not in price_map]
    if missing:
        print(f"[WARN] 가격 조회 안 된 종목: {missing}", flush=True)

    # portfolioSummary 갱신
    portfolio_summary = history.get("portfolioSummary")
    if isinstance(portfolio_summary, dict):
        positions = portfolio_summary.get("positions", [])
        if isinstance(positions, list):
            history["portfolioSummary"] = refresh_summary(portfolio_summary, positions, found)
            print(f"[INFO] 와바바펀드 포지션 {len(positions)}개 갱신", flush=True)

    # aiPortfolioSummary 갱신
    ai_summary = history.get("aiPortfolioSummary")
    if isinstance(ai_summary, dict):
        ai_positions = ai_summary.get("positions", [])
        if isinstance(ai_positions, list):
            history["aiPortfolioSummary"] = refresh_summary(ai_summary, ai_positions, found)
            print(f"[INFO] 와바바AI펀드 포지션 {len(ai_positions)}개 갱신", flush=True)

    # 저장
    try:
        write_json(RECOMMENDATION_HISTORY_PATH, history)
    except Exception as e:
        return {"ok": False, "error": f"JSON 저장 실패: {e}"}

    result = {
        "ok": True,
        "baseDate": base_date,
        "priceRefreshedAt": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "foundCodes": list(found.keys()),
        "missingCodes": missing,
        "wababaPositionCount": len(wababa_codes),
        "aiPositionCount": len(ai_codes),
        "wababaTotalProfitRate": history.get("portfolioSummary", {}).get("totalProfitRate"),
        "aiTotalProfitRate": history.get("aiPortfolioSummary", {}).get("totalProfitRate"),
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


if __name__ == "__main__":
    outcome = main()
    if not outcome.get("ok"):
        sys.exit(1)
