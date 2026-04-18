import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pykrx import stock


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "financial-universe-real.json"


def find_latest_business_day() -> str:
    now_kst = datetime.now(timezone(timedelta(hours=9)))

    for days_back in range(0, 14):
        candidate = (now_kst - timedelta(days=days_back)).strftime("%Y%m%d")

        try:
            df = stock.get_market_ohlcv_by_ticker(candidate, market="KOSPI")
            if df is not None and not df.empty:
                print(f"[INFO] 사용 거래일: {candidate}")
                return candidate
        except Exception as error:
            print(f"[WARN] 거래일 확인 실패: {candidate} / {error}")

    raise RuntimeError("최근 영업일을 찾지 못했습니다 (14일 범위 탐색 실패).")


def safe_number(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    try:
        value = float(value)
        if pd.isna(value):
            return None
        return value
    except Exception:
        return None


def safe_get_ohlcv(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_ohlcv_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "price"])
        return frame.rename(columns={"티커": "symbol", "종가": "price"})[
            ["symbol", "price"]
        ]
    except Exception as error:
        print(f"[WARN] OHLCV 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "price"])


def safe_get_fundamental(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_fundamental_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "PER", "PBR", "DIV"])
        return frame.rename(
            columns={
                "티커": "symbol",
                "PER": "PER",
                "PBR": "PBR",
                "DIV": "DIV",
            }
        )[["symbol", "PER", "PBR", "DIV"]]
    except Exception as error:
        print(f"[WARN] 펀더멘털 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "PER", "PBR", "DIV"])


def safe_get_market_cap(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_cap_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "marketCap"])
        return frame.rename(
            columns={
                "티커": "symbol",
                "시가총액": "marketCap",
            }
        )[["symbol", "marketCap"]]
    except Exception as error:
        print(f"[WARN] 시가총액 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "marketCap"])


def safe_get_ticker_list(base_date: str, market: str) -> list[str]:
    try:
        tickers = stock.get_market_ticker_list(base_date, market=market)
        if tickers:
            return tickers
        return []
    except Exception as error:
        print(f"[WARN] 티커 목록 조회 실패 ({market}): {error}")
        return []


def get_market_frame(base_date: str, market: str) -> pd.DataFrame:
    tickers = safe_get_ticker_list(base_date, market)

    if not tickers:
        return pd.DataFrame(
            columns=[
                "symbol",
                "corpName",
                "marketName",
                "industryName",
                "price",
                "marketCap",
                "PER",
                "PBR",
                "DIV",
            ]
        )

    base = pd.DataFrame({"symbol": tickers})

    ohlcv = safe_get_ohlcv(base_date, market)
    fundamental = safe_get_fundamental(base_date, market)
    market_cap = safe_get_market_cap(base_date, market)

    merged = (
        base.merge(ohlcv, on="symbol", how="left")
        .merge(fundamental, on="symbol", how="left")
        .merge(market_cap, on="symbol", how="left")
    )

    names = []
    for symbol in merged["symbol"].tolist():
        try:
            names.append(stock.get_market_ticker_name(symbol))
        except Exception:
            names.append(symbol)
        time.sleep(0.005)

    merged["corpName"] = names
    merged["marketName"] = market
    merged["industryName"] = ""

    return merged[
        [
            "symbol",
            "corpName",
            "marketName",
            "industryName",
            "price",
            "marketCap",
            "PER",
            "PBR",
            "DIV",
        ]
    ]


def build_payload() -> dict:
    base_date = find_latest_business_day()

    kospi = get_market_frame(base_date, "KOSPI")
    kosdaq = get_market_frame(base_date, "KOSDAQ")

    merged = pd.concat([kospi, kosdaq], ignore_index=True)

    items = []
    for _, row in merged.iterrows():
        items.append(
            {
                "symbol": str(row["symbol"]).zfill(6),
                "corpName": row["corpName"],
                "marketName": row["marketName"],
                "industryName": row["industryName"],
                "price": safe_number(row["price"]),
                "marketCap": safe_number(row["marketCap"]),
                "PER": safe_number(row["PER"]),
                "PBR": safe_number(row["PBR"]),
                "ROE": None,
                "salesGrowth": None,
                "opIncomeGrowth": None,
                "debtRatio": None,
                "divYield": safe_number(row["DIV"]),
                "opMargin": None,
                "netMargin": None,
                "salesCagr3Y": None,
                "EPSGrowth3Y": None,
                "updatedAt": f"{base_date[:4]}-{base_date[4:6]}-{base_date[6:8]}",
            }
        )

    return {
        "data": items,
        "meta": {
            "provider": "pykrx-daily-snapshot",
            "version": 2,
            "count": len(items),
            "baseDate": f"{base_date[:4]}-{base_date[4:6]}-{base_date[6:8]}",
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "krxLoginConfigured": bool(os.environ.get("KRX_ID")) and bool(os.environ.get("KRX_PW")),
        },
    }


def main():
    payload = build_payload()
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"written: {OUTPUT_PATH}")
    print(f"count: {payload['meta']['count']}")
    print(f"baseDate: {payload['meta']['baseDate']}")


if __name__ == "__main__":
    main()