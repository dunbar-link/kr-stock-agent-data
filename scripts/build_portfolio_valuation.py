"""
build_portfolio_valuation.py

와바바펀드 / 와바바AI펀드의 현재가 기준 평가 요약을 read-only로 생성한다.
- 원본 portfolio.json / wababa-ai-portfolio.json: 읽기 전용 (수량/평단/현금/장부 수정 없음)
- 현재가 원천: recommendation-history.json 의 portfolioSummary / aiPortfolioSummary
  (이미 refresh_portfolio_prices.py 가 pykrx 종가로 갱신한 결과 — 이 스크립트는 네트워크 호출 없음)
- 출력: reports/portfolio-valuation-latest.json / .md  (평가 요약만, 장부 아님)

계산 원칙:
  investedAmount = Σ quantity * averagePrice
  marketValue    = Σ quantity * currentPrice
  totalAsset     = cash + marketValue
  unrealizedPL   = marketValue - investedAmount
  totalPL        = realizedProfit + unrealizedPL
  returnRate     = totalPL / initialCapital * 100   (initialCapital 이 명확할 때만)
현재가 누락 종목이 있으면 해당 펀드 marketValue/returnRate 는 UNKNOWN + WARNING.

매매/장부수정/추천확정 아님. 홈페이지/예약 변경 없음.
"""

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_PATH = ROOT / "portfolio.json"
AI_PORTFOLIO_PATH = ROOT / "wababa-ai-portfolio.json"
RECO_PATH = ROOT / "recommendation-history.json"
REPORTS_DIR = ROOT / "reports"
OUT_JSON = REPORTS_DIR / "portfolio-valuation-latest.json"
OUT_MD = REPORTS_DIR / "portfolio-valuation-latest.md"

SECRET_PATTERNS = ["ghp_", "xox", "sk-", "AKIA", "-----BEGIN",
                   "hooks.slack.com", "discord.com/api/webhooks",
                   "SERVICE_ROLE", "webhookUrl", "AuthKey", "password"]


def now_kst_str():
    return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")


def read_json(path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def num(v):
    try:
        if v is None:
            return None
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except (TypeError, ValueError):
        return None


def r0(x):
    return None if x is None else int(round(x))


def r2(x):
    return None if x is None else round(x, 2)


def build_fund(fund_obj, summary, label):
    """fund_obj: portfolio.json/wababa-ai-portfolio.json (원본, 확정값)
       summary: recommendation-history 의 (ai)portfolioSummary (현재가 포함)"""
    warnings = []
    out = {
        "label": label,
        "startDate": None, "cash": None, "realizedProfitLoss": None,
        "holdingsCount": None, "investedAmount": None, "marketValue": None,
        "totalAsset": None, "unrealizedProfitLoss": None, "totalProfitLoss": None,
        "returnRate": None, "holdings": [],
    }

    if fund_obj is None:
        warnings.append("%s 원본 파일 없음" % label)
        return out, warnings, ["원본 portfolio 파일 없음"]

    out["startDate"] = fund_obj.get("fundStartDate")
    out["cash"] = r0(num(fund_obj.get("cash")))
    out["realizedProfitLoss"] = r0(num(fund_obj.get("realizedProfit")))
    initial_capital = num(fund_obj.get("initialCapital"))
    src_positions = fund_obj.get("positions") or []
    out["holdingsCount"] = len(src_positions)

    blocked = []
    if summary is None:
        warnings.append("%s 현재가 원천(recommendation-history summary) 없음 -> 평가 UNKNOWN" % label)
        out["investedAmount"] = "UNKNOWN"
        out["marketValue"] = "UNKNOWN"
        out["totalAsset"] = "UNKNOWN"
        out["unrealizedProfitLoss"] = "UNKNOWN"
        out["totalProfitLoss"] = "UNKNOWN"
        out["returnRate"] = "UNKNOWN"
        blocked.append("현재가 원천 없음")
        # holdings (현재가 없이 매입정보만)
        for p in src_positions:
            q = num(p.get("quantity"))
            bp = num(p.get("buyPrice"))
            out["holdings"].append({
                "code": p.get("code"), "name": p.get("name"),
                "quantity": r0(q), "averagePrice": r0(bp),
                "currentPrice": "UNKNOWN",
                "investedAmount": r0(q * bp) if (q is not None and bp is not None) else None,
                "marketValue": "UNKNOWN", "profitLoss": "UNKNOWN", "returnRate": "UNKNOWN",
            })
        return out, warnings, blocked

    # 현재가 원천(summary.positions) 기준 평가
    sum_positions = summary.get("positions") or []
    # 종목 수 교차검증
    if len(sum_positions) != len(src_positions):
        warnings.append("%s 보유 종목 수 불일치: 원본 %d vs 평가원천 %d (평가원천이 더 최신일 수 있음)"
                        % (label, len(src_positions), len(sum_positions)))

    inv_sum = 0.0
    mv_sum = 0.0
    missing_price = False
    for p in sum_positions:
        code = p.get("code")
        name = p.get("name")
        q = num(p.get("quantity"))
        bp = num(p.get("buyPrice"))
        cp = num(p.get("currentPrice"))
        inv = (q * bp) if (q is not None and bp is not None) else None
        if cp is None or cp <= 0:
            missing_price = True
            warnings.append("%s 현재가 누락/0: %s(%s)" % (label, name, code))
            mv = None
            pl = None
            rr = None
            cp_out = "UNKNOWN"
        else:
            mv = (q * cp) if q is not None else None
            pl = (mv - inv) if (mv is not None and inv is not None) else None
            rr = (pl / inv * 100.0) if (pl is not None and inv not in (None, 0)) else None
            cp_out = r0(cp)
            if inv is not None:
                inv_sum += inv
            if mv is not None:
                mv_sum += mv
        out["holdings"].append({
            "code": code, "name": name,
            "quantity": r0(q), "averagePrice": r0(bp), "currentPrice": cp_out,
            "investedAmount": r0(inv), "marketValue": r0(mv),
            "profitLoss": r0(pl), "returnRate": r2(rr),
        })

    out["investedAmount"] = r0(inv_sum)
    if missing_price:
        out["marketValue"] = "UNKNOWN"
        out["totalAsset"] = "UNKNOWN"
        out["unrealizedProfitLoss"] = "UNKNOWN"
        out["totalProfitLoss"] = "UNKNOWN"
        out["returnRate"] = "UNKNOWN"
        blocked.append("일부 종목 현재가 누락 -> 펀드 평가 UNKNOWN")
        return out, warnings, blocked

    cash = num(fund_obj.get("cash")) or 0.0
    realized = num(fund_obj.get("realizedProfit")) or 0.0
    total_asset = cash + mv_sum
    unrealized = mv_sum - inv_sum
    total_pl = realized + unrealized
    out["marketValue"] = r0(mv_sum)
    out["totalAsset"] = r0(total_asset)
    out["unrealizedProfitLoss"] = r0(unrealized)
    out["totalProfitLoss"] = r0(total_pl)
    if initial_capital and initial_capital > 0:
        out["returnRate"] = r2(total_pl / initial_capital * 100.0)
    else:
        out["returnRate"] = "UNKNOWN"
        warnings.append("%s 기준 원금(initialCapital) 불명확 -> returnRate UNKNOWN" % label)

    # 원천 요약과 교차검증(반올림 오차 허용)
    src_mv = num(summary.get("totalEvaluationAmount"))
    if src_mv is not None and abs(src_mv - mv_sum) > max(1000.0, src_mv * 0.005):
        warnings.append("%s 평가금액 교차검증 차이: 계산 %d vs 원천 %d" % (label, r0(mv_sum), r0(src_mv)))

    return out, warnings, blocked


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    fund = read_json(PORTFOLIO_PATH)
    ai_fund = read_json(AI_PORTFOLIO_PATH)
    reco = read_json(RECO_PATH)

    price_as_of = None
    price_source = "recommendation-history.json portfolioSummary (refresh_portfolio_prices.py / pykrx 종가)"
    psum = asum = None
    if reco is not None:
        psum = reco.get("portfolioSummary")
        asum = reco.get("aiPortfolioSummary")
        price_as_of = reco.get("baseDate") or reco.get("generatedAt")
    else:
        price_source = "UNKNOWN (recommendation-history.json 없음)"

    wf, wwarn, wblock = build_fund(fund, psum, "와바바펀드")
    af, awarn, ablock = build_fund(ai_fund, asum, "와바바AI펀드")

    warnings = wwarn + awarn
    blocked = wblock + ablock

    # priceSource 종류 표기 (전일/최근 종가)
    price_type = "최근 거래일 종가(pykrx, recommendation-history 경유)"

    # verdict
    rank = 0
    if reco is None:
        rank = 2
        blocked.append("현재가 원천(recommendation-history.json) 없음")
    if any(isinstance(x.get("returnRate"), str) for x in (wf, af)):
        rank = max(rank, 1)
    if warnings:
        rank = max(rank, 1)
    verdict = ["PASS", "WARNING", "BLOCKED"][rank]
    reason = "현재가 평가 확정" if verdict == "PASS" else "; ".join((blocked + warnings)[:4])

    payload = {
        "generatedAt": now_kst_str(),
        "priceAsOf": price_as_of,
        "priceSource": price_source,
        "priceType": price_type,
        "verdict": verdict,
        "reason": reason,
        "wababaFund": wf,
        "wababaAiFund": af,
        "blockedReasons": blocked,
        "warnings": warnings,
    }

    json_text = json.dumps(payload, ensure_ascii=False, indent=2)

    # secret 스캔
    secret_clean = not any(pat in json_text for pat in SECRET_PATTERNS)
    if not secret_clean:
        payload["verdict"] = "BLOCKED"
        payload["reason"] = "secret 패턴 감지 - 출력 보류"
        print(json.dumps({"ok": False, "verdict": "BLOCKED", "reason": "secret detected"}, ensure_ascii=False))
        return

    with OUT_JSON.open("w", encoding="utf-8") as f:
        f.write(json_text)

    # Markdown
    def fund_md(f):
        lines = []
        lines.append("| 필드 | 값 |")
        lines.append("|---|---|")
        for k in ["startDate", "cash", "realizedProfitLoss", "holdingsCount",
                  "investedAmount", "marketValue", "totalAsset",
                  "unrealizedProfitLoss", "totalProfitLoss", "returnRate"]:
            lines.append("| %s | %s |" % (k, f.get(k)))
        lines.append("")
        lines.append("| 종목 | 코드 | 수량 | 평단 | 현재가 | 매입 | 평가 | 손익 | 수익률% |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for h in f.get("holdings", []):
            lines.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                h.get("name"), h.get("code"), h.get("quantity"), h.get("averagePrice"),
                h.get("currentPrice"), h.get("investedAmount"), h.get("marketValue"),
                h.get("profitLoss"), h.get("returnRate")))
        return "\n".join(lines)

    md = []
    md.append("전체 판정: %s" % verdict)
    md.append("")
    md.append("# 와바바 포트폴리오 현재가 평가 요약 (read-only)")
    md.append("")
    md.append("- 생성 시각: %s" % payload["generatedAt"])
    md.append("- 가격 기준일(priceAsOf): %s" % price_as_of)
    md.append("- 가격 원천: %s" % price_source)
    md.append("- 가격 종류: %s" % price_type)
    md.append("- 판정 사유: %s" % reason)
    md.append("- 성격: 평가 요약만. 매매/장부수정/추천확정 아님. 원본 portfolio 미수정.")
    md.append("")
    md.append("## 와바바펀드")
    md.append("")
    md.append(fund_md(wf))
    md.append("")
    md.append("## 와바바AI펀드")
    md.append("")
    md.append(fund_md(af))
    md.append("")
    if blocked:
        md.append("## 차단/UNKNOWN 사유")
        md.append("")
        for b in blocked:
            md.append("- %s" % b)
        md.append("")
    md.append("## 경고(warnings)")
    md.append("")
    if warnings:
        for w in warnings:
            md.append("- %s" % w)
    else:
        md.append("- 없음")
    md.append("")
    md.append("## 하지 않은 것 (안전)")
    md.append("")
    md.append("- 매수/매도 실행 없음 / 장부 수량·평단·현금 수정 없음")
    md.append("- portfolio.json / wababa-ai-portfolio.json 원본 미수정 (read-only)")
    md.append("- 네트워크 신규 호출 없음 (recommendation-history.json 의 기존 종가 사용)")
    md.append("- 홈페이지/예약 변경 없음 / secret·token·.env·webhook 미노출")

    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("\n".join(md))

    print(json.dumps({
        "ok": True, "verdict": verdict, "priceAsOf": price_as_of,
        "wababaFund": {"totalAsset": wf.get("totalAsset"), "returnRate": wf.get("returnRate"),
                       "holdingsCount": wf.get("holdingsCount")},
        "wababaAiFund": {"totalAsset": af.get("totalAsset"), "returnRate": af.get("returnRate"),
                         "holdingsCount": af.get("holdingsCount")},
        "out": str(OUT_JSON),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
