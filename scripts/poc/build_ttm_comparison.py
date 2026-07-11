"""build_ttm_comparison — 공식 연간 마법공식값 vs TTM 실험값 비교 데이터셋 (Phase MF-TTM-READONLY-COMPARISON-DASHBOARD-POC).

조회 전용. 운영 반영 없음. 신규 DART 호출 0(캐시 offline).
- annual: 운영 canonical 함수 calculate_book_faithful_magic_ranking() 결과 그대로(공식값).
- ttmExperiment: 동일 산식(EBIT=영업이익, EV=시총+부채-현금, RoC=EBIT/(순운전자본+유형자산))에
  TTM 입력(최근 4분기 영업이익 합 + 최신 분기말 BS)을 넣어 계산한 '실험값'. PASS 계열 + TTM 완성만.
- experimental 순위는 실험 대상 집합 내에서 산출(공식 순위 덮어쓰기 아님).

출력(TEMP, gitignore):
  _cache/ttm-poc-output/ttm-comparison-full.json     전체
  _cache/ttm-poc-output/ttm-comparison-fixture20.json 대표 20종(REPO1 fixture 원본)
  _cache/ttm-poc-output/ttm-comparison-stats.json     Gate E 통계
실행: python scripts/poc/build_ttm_comparison.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import build_magic_formula_fund as F

ROOT = SCRIPTS.parent
TTM_RESULT = ROOT / "_cache" / "ttm-poc-output" / "ttm-all1316-result.json"
OFFICIAL = ROOT / "_cache" / "ttm-poc-output" / "official-eligible.json"
OUT_DIR = ROOT / "_cache" / "ttm-poc-output"

MC_TO_WON = F.CONFIG["marketCapToWon"]
PASS_LIKE = {"PASS", "PASS_WITH_TRANSITION_NOTE", "PASS_OFFICIAL_IR_CONFIRMED"}


def pct(new, old):
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / abs(old) * 100, 2)


def main() -> int:
    # 1) annual 공식값(canonical 함수)
    rows, meta = F.load_universe()
    base_date = meta.get("baseDate", "unknown")
    bl = F.read_json(F.BLACKLIST_PATH, []) or []
    blacklist = set(str(x).strip() for x in bl) if isinstance(bl, list) else set()
    final, excluded, cov = F.calculate_book_faithful_magic_ranking(rows, blacklist)
    annual = {s["code"]: s for s in final}
    formula_version = "book-faithful-v1 (canonical, 연간 결산 기준)"

    # 2) TTM 통합 결과
    ttm_doc = json.loads(TTM_RESULT.read_text(encoding="utf-8"))
    ttm = {r["stockCode"]: r for r in ttm_doc["results"]}
    code_set_hash = json.loads(OFFICIAL.read_text(encoding="utf-8")).get("codeSetHash")

    # 3) 실험 대상: PASS 계열 + TTM 완성(3개 손익 모두)
    experiment_codes = []
    for code, a in annual.items():
        t = ttm.get(code)
        if not t:
            continue
        if t.get("qualityStatus") not in PASS_LIKE:
            continue
        if t.get("ttmOperatingIncome") is None or t.get("ttmRevenue") is None or t.get("ttmNetIncome") is None:
            continue
        experiment_codes.append(code)

    # 4) experimental EBIT/EV/RoC/EY (annual 과 동일 산식, TTM 입력)
    exp_metrics = {}
    for code in experiment_codes:
        a = annual[code]; t = ttm[code]
        ebit = t["ttmOperatingIncome"]                 # 최근 4분기 영업이익 합 = 연간 EBIT 정의와 동일(기간 롤링)
        ca, cl, ppe = t.get("latestCurrentAssets"), t.get("latestCurrentLiabilities"), t.get("latestPpe")
        cash, tl = t.get("latestCash"), t.get("latestTotalDebt")
        mc = a.get("marketCap")
        reason = None
        capital_base = ev = roc = ey = None
        if None in (ca, cl, ppe, cash, tl, mc):
            reason = "BS/시총 결측"
        else:
            capital_base = ca - cl + ppe
            ev = mc * MC_TO_WON + tl - cash
            if ebit <= 0:
                reason = "TTM EBIT<=0 (실험 순위 제외, annual 과 동일 유효성 기준)"
            elif capital_base <= 0:
                reason = "TTM 투입자본<=0"
            elif ev <= 0:
                reason = "TTM EV<=0"
            else:
                roc = round(ebit / capital_base, 6)
                ey = round(ebit / ev, 6)
        exp_metrics[code] = {"ebit": ebit, "capitalBase": capital_base, "ev": ev,
                             "returnOnCapital": roc, "earningsYield": ey, "excludeReason": reason}

    # 5) experimental 순위(유효 종목만) — annual 과 동일 정렬 규칙(RoC/EY 내림차순, tie=code)
    valid = [c for c in experiment_codes if exp_metrics[c]["returnOnCapital"] is not None]
    for i, c in enumerate(sorted(valid, key=lambda c: (-exp_metrics[c]["returnOnCapital"], c)), 1):
        exp_metrics[c]["profitabilityRank"] = i
    for i, c in enumerate(sorted(valid, key=lambda c: (-exp_metrics[c]["earningsYield"], c)), 1):
        exp_metrics[c]["valueRank"] = i
    for c in valid:
        exp_metrics[c]["combinedRank"] = exp_metrics[c]["profitabilityRank"] + exp_metrics[c]["valueRank"]
    # 동일 집합 내 annual 재순위(selection bias 최소화한 순수 순위변화 비교용)
    for i, c in enumerate(sorted(valid, key=lambda c: (-annual[c]["returnOnCapital"], c)), 1):
        exp_metrics[c]["annualProfitabilityRankInSubset"] = i
    for i, c in enumerate(sorted(valid, key=lambda c: (-annual[c]["earningsYield"], c)), 1):
        exp_metrics[c]["annualValueRankInSubset"] = i
    for c in valid:
        exp_metrics[c]["annualCombinedRankInSubset"] = \
            exp_metrics[c]["annualProfitabilityRankInSubset"] + exp_metrics[c]["annualValueRankInSubset"]

    # 6) rows
    rows_out = []
    for code in experiment_codes:
        a = annual[code]; t = ttm[code]; e = exp_metrics[code]
        rows_out.append({
            "stockCode": code, "companyName": a.get("name"), "industry": a.get("industryName"),
            "fsDiv": t.get("fsDiv"), "qualityStatus": t.get("qualityStatus"),
            "annual": {
                "revenue": None, "operatingIncome": a.get("EBIT"), "netIncome": None,
                "currentAssets": a.get("currentAssets"), "currentLiabilities": a.get("currentLiabilities"),
                "ppe": a.get("propertyPlantAndEquipment"), "cash": a.get("cashAndCashEquivalents"),
                "totalDebt": a.get("totalLiabilities"),
                "ebitEv": a.get("earningsYield"), "returnOnCapital": a.get("returnOnCapital"),
                "valueRank": a.get("valueRank"), "profitabilityRank": a.get("profitabilityRank"),
                "combinedRank": a.get("combinedRank"), "officialRankInEligible": a.get("rank"),
                "annualCombinedRankInSubset": e.get("annualCombinedRankInSubset"),
            },
            "ttmExperiment": {
                "revenue": t.get("ttmRevenue"), "operatingIncome": t.get("ttmOperatingIncome"),
                "netIncome": t.get("ttmNetIncome"),
                "latestCurrentAssets": t.get("latestCurrentAssets"), "latestCurrentLiabilities": t.get("latestCurrentLiabilities"),
                "latestPpe": t.get("latestPpe"), "latestCash": t.get("latestCash"), "latestTotalDebt": t.get("latestTotalDebt"),
                "experimentalEbitEv": e.get("earningsYield"), "experimentalReturnOnCapital": e.get("returnOnCapital"),
                "experimentalValueRank": e.get("valueRank"), "experimentalProfitabilityRank": e.get("profitabilityRank"),
                "experimentalCombinedRank": e.get("combinedRank"), "excludeReason": e.get("excludeReason"),
                "quarterlyOperatingIncome": t.get("quarterValues", {}).get("operatingIncome"),
                "quarterlyRevenue": t.get("quarterValues", {}).get("revenue"),
            },
            "comparison": {
                "operatingIncomeChangePct": pct(t.get("ttmOperatingIncome"), a.get("EBIT")),
                "valueRankChange": (e["valueRank"] - e["annualValueRankInSubset"]) if e.get("valueRank") else None,
                "profitabilityRankChange": (e["profitabilityRank"] - e["annualProfitabilityRankInSubset"]) if e.get("profitabilityRank") else None,
                "combinedRankChange": (e["combinedRank"] - e["annualCombinedRankInSubset"]) if e.get("combinedRank") else None,
            },
            "sourceTrace": {
                "ttmWindow": ttm_doc["summary"].get("asOf"), "annualFinancialYear": 2025,
                "ttmAsOfQuarter": "2026Q1", "latestBsQuarter": "2026Q1(or 2025FY fallback)",
            },
            "warningNotes": [t.get("qualityStatus")] + ([e["excludeReason"]] if e.get("excludeReason") else []),
        })

    excluded_warning = sum(1 for c, a in annual.items()
                           if ttm.get(c, {}).get("qualityStatus") == "WARNING_EXTERNAL_CONFIRMATION")
    excluded_blocked = sum(1 for c, a in annual.items()
                           if ttm.get(c, {}).get("qualityStatus") == "BLOCKED_DATA_INCONSISTENCY")
    excluded_incomplete = sum(1 for c, a in annual.items()
                              if ttm.get(c) and ttm[c].get("qualityStatus") in PASS_LIKE
                              and ttm[c].get("ttmOperatingIncome") is None)

    dataset = {
        "schemaVersion": "1.0.0",
        "generatedAt": base_date, "universeBaseDate": base_date, "priceAsOfDate": base_date,
        "annualFinancialYear": 2025, "ttmAsOfQuarter": "2026Q1",
        "formulaVersionAnnual": formula_version,
        "experimentVersion": "ttm-exp-v1 (EBIT=최근4분기영업이익합, BS=최신분기말, 산식은 annual 동일)",
        "codeSetHash": code_set_hash,
        "includedCount": len(rows_out), "experimentalRankedCount": len(valid),
        "excludedWarningCount": excluded_warning, "excludedBlockedCount": excluded_blocked,
        "excludedIncompleteCount": excluded_incomplete,
        "disclaimer": "조회 전용 PoC. 공식 연간 마법공식 미반영. 투자 추천 아님. 백테스트 없음(순위 민감도만 표시).",
        "rows": rows_out,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "ttm-comparison-full.json").write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- Gate E 통계 ----
    ranked = [r for r in rows_out if r["ttmExperiment"]["experimentalCombinedRank"]]
    ann_top100 = set(c for c, a in annual.items() if a.get("rank") and a["rank"] <= 100)
    exp_top100 = set(r["stockCode"] for r in sorted(ranked, key=lambda r: r["ttmExperiment"]["experimentalCombinedRank"])[:100])
    changes = [r["comparison"]["combinedRankChange"] for r in ranked if r["comparison"]["combinedRankChange"] is not None]
    changes_sorted = sorted(changes)
    n = len(changes_sorted)
    median = changes_sorted[n // 2] if n else None
    stats = {
        "includedExperiment": len(rows_out), "experimentalRanked": len(valid),
        "annualTop100_vs_expTop100_overlap": len(ann_top100 & exp_top100),
        "expTop100_newEntrants": len(exp_top100 - ann_top100),
        "expTop100_dropouts": len(ann_top100 - exp_top100),
        "avgCombinedRankChange_subset": round(sum(changes) / n, 2) if n else None,
        "medianCombinedRankChange_subset": median,
        "bigMovers_over50": sum(1 for c in changes if abs(c) >= 50),
        "byStatus": {s: sum(1 for r in rows_out if r["qualityStatus"] == s) for s in PASS_LIKE},
        "selectionBias": f"WARNING {excluded_warning} + BLOCKED {excluded_blocked} + 미완성 {excluded_incomplete} 제외 → "
                         f"실험 모집단이 공식 eligible({len(annual)})보다 작음. 순위변화는 동일 실험집합 내 재순위 기준(subset)으로 계산해 편향 최소화. "
                         f"공식 top100 중 실험 제외로 빠진 종목은 별도 확인 필요.",
        "note": "수익률 개선 주장 아님. 백테스트 없음. 데이터 신선도·순위 민감도만.",
    }
    (OUT_DIR / "ttm-comparison-stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- 대표 20종 fixture (REPO1 commit 원본) ----
    def pick(pred, k):
        return [r for r in rows_out if pred(r)][:k]
    fixture, seen = [], set()
    buckets = [
        lambda r: r["comparison"].get("combinedRankChange") is not None and abs(r["comparison"]["combinedRankChange"]) <= 3,   # 유사
        lambda r: (r["comparison"].get("combinedRankChange") or 0) <= -50,   # 급상승(순위 숫자 감소)
        lambda r: (r["comparison"].get("combinedRankChange") or 0) >= 50,    # 급하락
        lambda r: r["qualityStatus"] == "PASS_WITH_TRANSITION_NOTE",
        lambda r: r["qualityStatus"] == "PASS_OFFICIAL_IR_CONFIRMED",
        lambda r: r["fsDiv"] == "OFS",
        lambda r: r["fsDiv"] == "CFS",
    ]
    for b in buckets:
        for r in pick(b, 3):
            if r["stockCode"] not in seen:
                fixture.append(r); seen.add(r["stockCode"])
            if len(fixture) >= 20:
                break
        if len(fixture) >= 20:
            break
    # 20 미달 시 실험순위 상위에서 결정론적으로 보강
    for r in sorted(ranked, key=lambda r: r["ttmExperiment"]["experimentalCombinedRank"]):
        if len(fixture) >= 20:
            break
        if r["stockCode"] not in seen:
            fixture.append(r); seen.add(r["stockCode"])
    # 제외 사례(WARNING/BLOCKED/미완성) 대표도 포함
    def excl_by(status, k):
        out = []
        for c, a in annual.items():
            t = ttm.get(c)
            if t and t.get("qualityStatus") == status:
                out.append({"stockCode": c, "companyName": a.get("name"), "qualityStatus": status,
                            "excludeReason": status, "officialCombinedRank": a.get("combinedRank")})
            if len(out) >= k:
                break
        return out
    excl_examples = excl_by("WARNING_EXTERNAL_CONFIRMATION", 3) + excl_by("BLOCKED_DATA_INCONSISTENCY", 2)
    (OUT_DIR / "ttm-comparison-fixture20.json").write_text(
        json.dumps({"schemaVersion": dataset["schemaVersion"], "generatedAt": base_date,
                    "annualFinancialYear": 2025, "ttmAsOfQuarter": "2026Q1",
                    "formulaVersionAnnual": formula_version, "codeSetHash": code_set_hash,
                    "summary": {"includedExperiment": len(rows_out), "excludedWarning": excluded_warning,
                                "excludedBlocked": excluded_blocked, "excludedIncomplete": excluded_incomplete,
                                "experimentalRanked": len(valid), "officialEligible": len(annual)},
                    "stats": stats,
                    "disclaimer": dataset["disclaimer"],
                    "rows": fixture, "excludedExamples": excl_examples}, ensure_ascii=False, indent=2),
        encoding="utf-8")

    print(json.dumps({"included": len(rows_out), "experimentalRanked": len(valid),
                      "excludedWarning": excluded_warning, "excludedBlocked": excluded_blocked,
                      "excludedIncomplete": excluded_incomplete, "fixture20": len(fixture),
                      "top100Overlap": stats["annualTop100_vs_expTop100_overlap"],
                      "expTop100NewEntrants": stats["expTop100_newEntrants"],
                      "avgRankChange": stats["avgCombinedRankChange_subset"],
                      "medianRankChange": stats["medianCombinedRankChange_subset"],
                      "bigMovers50": stats["bigMovers_over50"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
