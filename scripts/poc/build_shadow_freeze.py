"""build_shadow_freeze — 공식 연간 top30 / TTM 실험 top30 동일시점 freeze (Phase MF-TTM-SHADOW-PORTFOLIO-POC).

조회 전용. 실주문 없음. 신규 DART 호출 0(캐시/기존 산출물). 미래 소급 없음.
- annual(전략A): calculate_book_faithful_magic_ranking() 공식 rank 상위 30.
- ttm(전략B): ttm-comparison-full.json experimentalCombinedRank 상위 30(PASS계열, WARNING/BLOCKED 제외).
- entryPrice = universe 종가(priceAsOfDate). 동일가중(종목당 100만), 정수 주식, 잔여현금 기록.

출력:
  scripts/poc/fixtures/shadow-freeze-latest.json   (소형 fixture, commit)
실행: python scripts/poc/build_shadow_freeze.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import build_magic_formula_fund as F

ROOT = SCRIPTS.parent
COMPARISON = ROOT / "_cache" / "ttm-poc-output" / "ttm-comparison-full.json"
CFG = Path(__file__).resolve().parent / "shadow_portfolio_config.json"
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

REPO2_COMMIT = "beb28ea"   # 기록용(참고). 실제 값은 최종보고/커밋에서 확정.


def build_holdings(ranked, price_by_code, cfg):
    per = cfg["perStockCapitalKrw"]
    holdings, invested_total = [], 0
    for item in ranked:
        code = item["stockCode"]
        price = price_by_code.get(code)
        if price is None or price <= 0:
            holdings.append({**item, "entryPrice": price, "shares": 0, "investedAmount": 0,
                             "weight": 0.0, "priceMissing": True})
            continue
        shares = int(per // price)          # 정수 주식, 소수점 없음
        invested = shares * price
        invested_total += invested
        holdings.append({**item, "entryPrice": price, "shares": shares,
                         "investedAmount": invested, "weight": round(invested / cfg["initialCapitalKrw"], 6),
                         "priceMissing": False})
    cash = cfg["initialCapitalKrw"] - invested_total
    return holdings, cash


def main() -> int:
    cfg = json.loads(CFG.read_text(encoding="utf-8"))
    comp = json.loads(COMPARISON.read_text(encoding="utf-8"))
    price_as_of = comp["priceAsOfDate"]
    freeze_date = price_as_of

    # universe 종가 + annual 공식 ranking
    rows_uni, meta = F.load_universe()
    price_by_code = {str(r.get("symbol") or r.get("code")): F.num(r.get("price")) for r in rows_uni}
    bl = F.read_json(F.BLACKLIST_PATH, []) or []
    blacklist = set(str(x).strip() for x in bl) if isinstance(bl, list) else set()
    final, _, _ = F.calculate_book_faithful_magic_ranking(rows_uni, blacklist)
    annual_by_code = {s["code"]: s for s in final}

    # 전략 A: 공식 rank 상위 30
    annual_top = sorted(final, key=lambda s: s["rank"])[:30]
    annual_ranked = [{"stockCode": s["code"], "companyName": s["name"], "industry": s.get("industryName"),
                      "rankAtFreeze": s["rank"], "qualityStatus": "OFFICIAL_ANNUAL",
                      "sourceTrace": {"combinedRank": s["combinedRank"], "returnOnCapital": s["returnOnCapital"],
                                      "earningsYield": s["earningsYield"]}} for s in annual_top]

    # 전략 B: TTM 실험 experimentalCombinedRank 상위 30 (PASS 계열만; WARNING/BLOCKED 는 comp.rows 에 없음)
    ttm_rows = [r for r in comp["rows"] if r["ttmExperiment"]["experimentalCombinedRank"]]
    ttm_top = sorted(ttm_rows, key=lambda r: r["ttmExperiment"]["experimentalCombinedRank"])[:30]
    ttm_ranked = [{"stockCode": r["stockCode"], "companyName": r["companyName"], "industry": r["industry"],
                   "rankAtFreeze": r["ttmExperiment"]["experimentalCombinedRank"], "qualityStatus": r["qualityStatus"],
                   "sourceTrace": {"experimentalReturnOnCapital": r["ttmExperiment"]["experimentalReturnOnCapital"],
                                   "experimentalEbitEv": r["ttmExperiment"]["experimentalEbitEv"],
                                   "ttmOperatingIncome": r["ttmExperiment"]["operatingIncome"]}} for r in ttm_top]

    # WARNING/BLOCKED 미포함 증명
    ttm_codes = set(r["stockCode"] for r in ttm_top)
    bad_in_ttm = [c for c in ttm_codes
                  if comp and next((x for x in comp["rows"] if x["stockCode"] == c), {}).get("qualityStatus")
                  not in ("PASS", "PASS_WITH_TRANSITION_NOTE", "PASS_OFFICIAL_IR_CONFIRMED")]

    annual_holdings, annual_cash = build_holdings(annual_ranked, price_by_code, cfg)
    ttm_holdings, ttm_cash = build_holdings(ttm_ranked, price_by_code, cfg)

    annual_codes = set(r["stockCode"] for r in annual_ranked)
    common = sorted(annual_codes & ttm_codes)
    annual_only = sorted(annual_codes - ttm_codes)
    ttm_only = sorted(ttm_codes - annual_codes)

    source_meta = {
        "freezeId": f"shadow-{freeze_date}",
        "freezeDate": freeze_date, "priceAsOfDate": price_as_of,
        "annualFinancialYear": comp["annualFinancialYear"], "ttmAsOfQuarter": comp["ttmAsOfQuarter"],
        "annualRankingGeneratedAt": comp["generatedAt"], "ttmRankingGeneratedAt": comp["generatedAt"],
        "sourceCommitAnnual": REPO2_COMMIT, "sourceCommitTtm": REPO2_COMMIT,
        "codeSetHash": comp.get("codeSetHash"),
        "leakageNote": "재무 최신=2026Q1(5월 공시) < 가격일 → look-ahead 없음. 발표일 이후 종가 사용.",
    }

    out = {
        "schemaVersion": "1.0.0",
        "config": {k: cfg[k] for k in ("portfolioSize", "weighting", "initialCapitalKrw", "perStockCapitalKrw",
                                       "transactionCostAssumption", "benchmark", "rebalancing")},
        "sourceMetadata": source_meta,
        "disclaimer": cfg["disclaimer"],
        "warningBlockedExcludedFromTtm": {"ttmTop30HasNonPass": len(bad_in_ttm), "codes": bad_in_ttm},
        "overlap": {"commonCount": len(common), "common": common,
                    "annualOnly": annual_only, "ttmOnly": ttm_only},
        "strategies": {
            "annual": {"strategyType": "OFFICIAL_ANNUAL", "initialCapital": cfg["initialCapitalKrw"],
                       "cash": annual_cash, "investedAmount": cfg["initialCapitalKrw"] - annual_cash,
                       "holdings": annual_holdings},
            "ttm": {"strategyType": "TTM_EXPERIMENT", "initialCapital": cfg["initialCapitalKrw"],
                    "cash": ttm_cash, "investedAmount": cfg["initialCapitalKrw"] - ttm_cash,
                    "holdings": ttm_holdings},
        },
    }
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    (FIXTURE_DIR / "shadow-freeze-latest.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "freezeDate": freeze_date, "priceAsOfDate": price_as_of,
        "annual_top30": len(annual_ranked), "ttm_top30": len(ttm_ranked),
        "common": len(common), "annualOnly": len(annual_only), "ttmOnly": len(ttm_only),
        "annualCash": annual_cash, "ttmCash": ttm_cash,
        "ttmTop30_nonPass": len(bad_in_ttm), "priceMissing_annual": sum(1 for h in annual_holdings if h["priceMissing"]),
        "priceMissing_ttm": sum(1 for h in ttm_holdings if h["priceMissing"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
