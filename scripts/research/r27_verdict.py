#!/usr/bin/env python3
"""R27 SIZE 거래가능성 최종 판정 + 실행후보 선정.

WABABA-SIZE-TRADABILITY-VALIDATION-R27

precommit QUALIFICATION 을 **그대로** 적용한다(§31·§43). coverage gate 를 통과하지
못하면 좋은 결과를 만들려고 기간을 옮기지 않고 SIZE_DATA_INSUFFICIENT 로 끝낸다.

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r27_precommit import (EXECUTION_CANDIDATE, HORIZONS,  # noqa: E402
                           NEXT_TASK_RULE, QUALIFICATION)

RD = Path(__file__).resolve().parents[2] / "reports" / "research"
PH = HORIZONS["primary"]


def L(n):
    p = RD / f"r27-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def main() -> int:
    cov = L("liquidity-coverage")
    ba = L("before-after")
    ie = L("included-excluded")
    attr = L("tradability-attrition")
    sr = L("subperiod-rolling")
    ex = L("exchange")
    conc = L("concentration")
    dd = L("delisting-distress")
    part = L("participation")
    sc = L("sensitivity-cost")
    repro = L("size-reproduction")

    # ── coverage gate 먼저 (§30) ──────────────────────────────────────
    if not cov or not cov.get("coverageGatePass"):
        out = {
            "task": "R27", "verdict": "SIZE_DATA_INSUFFICIENT",
            "verdictWhy": [
                "coverage gate 미달(§30).",
                (f"연도별 90% 이상 coverage 를 만족한 해 "
                 f"{(cov or {}).get('yearsMeetingThreshold')}개 < 요구 "
                 f"{(cov or {}).get('minYearsCovered')}개"),
                (f"필요 거래일 {(cov or {}).get('daysRequired')} 중 확보 "
                 f"{(cov or {}).get('daysCollected')} "
                 f"({(cov or {}).get('dayCoveragePct')}%)"),
                "coverage 가 좋은 최근 기간만 primary 로 바꾸지 않았다(§30).",
            ],
            "coverageGatePass": False,
            "dayCoveragePct": (cov or {}).get("dayCoveragePct"),
            "yearsMeetingThreshold": (cov or {}).get("yearsMeetingThreshold"),
            "sizeReproductionExact": (repro or {}).get("allExactMatch"),
            "executionCandidate": "NONE_PENDING_DATA",
            "nextTaskRule": "D", "nextTask": NEXT_TASK_RULE["D"],
            "portfolioResearchEntryAllowed": False,
            "thresholdsUnchanged": True,
            "noParameterRescue": True,
        }
        (RD / "r27-verdict-latest.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=float),
            encoding="utf-8")
        (RD / "r27-bm-vs-size-latest.json").write_text(
            json.dumps({"task": "R27", "status": "NOT_EVALUATED",
                        "why": ("coverage gate 미달로 동일 거래가능성 기준 비교를 "
                                "수행하지 않았다. 근거 없는 비교를 만들지 않는다(§32).")},
                       ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"verdict": out["verdict"],
                          "dayCoveragePct": out["dayCoveragePct"],
                          "next": "D"}, ensure_ascii=False))
        return 0

    # ── coverage 통과 시 정식 판정 ────────────────────────────────────
    s = ba["byHorizon"][f"{PH}M"]
    b_size = s["BEFORE"]["SIZE"]
    a_size = s["AFTER"]["SIZE"]
    a_bm = s["AFTER"]["BM"]
    delta = s.get("delta", {}).get("SIZE", {})
    after_excess = a_size["meanExcessAnnPct"]
    retention = delta.get("retentionRatio")
    inc = ie["byArm"]["SIZE"]
    nontradable_alpha = bool(inc.get("nontradableAlpha"))
    attrition = attr["byArm"]["SIZE"]["attritionPct"]
    subp = sr["subperiodAfter"]["SIZE"]
    rollp = (sr["rollingAfter"]["SIZE"][f"{PH}M"]["overlapping"] or {}).get(
        "positiveRatio")
    kospi = (ex["byExchange"]["KOSPI"]["sizeExcessVsControl"] or {}).get("meanPct")
    kosdaq = (ex["byExchange"]["KOSDAQ"]["sizeExcessVsControl"] or {}).get("meanPct")
    flip3 = bool(conc["signFlips"]["SIZE"])

    S = QUALIFICATION["SIZE_EXECUTABLE_STRONG"]
    checks = {
        f"AFTER TOP−CONTROL >= {S['afterTopMinusControlPctMin']}%p":
            after_excess >= S["afterTopMinusControlPctMin"],
        f"retention >= {S['retentionRatioMin']}":
            retention is not None and retention >= S["retentionRatioMin"],
        "included(tradable) spread 양(+)":
            (inc.get("includedTopAnnPct") or 0) > 0,
        "부분기간 과반 양(+)":
            (subp.get("positiveRatio") or 0) >= S["minSubperiodPositiveRatio"],
        "롤링 과반 양(+)":
            (rollp or 0) >= S["minRollingPositiveRatio"],
        "KOSPI·KOSDAQ 방향 동일":
            kospi is not None and kosdaq is not None
            and (kospi > 0) == (kosdaq > 0) and kospi > 0,
        "top3 제거 후 부호 유지": not flip3,
        "nontradable alpha 아님": not nontradable_alpha,
    }

    if nontradable_alpha:
        verdict = "SIZE_ALPHA_LARGELY_NONTRADABLE"
        why = [f"excluded {inc.get('excludedTopAnnPct')}% vs included "
               f"{inc.get('includedTopAnnPct')}% — 초과수익이 거래 불가 종목에 몰려 있다",
               "실제 개인투자자가 살 수 없었던 수익이다(§22)."]
    elif flip3 or (attrition is not None
                   and attrition >= QUALIFICATION["SIZE_FRAGILE"]["attritionMaxPct"]):
        verdict = "SIZE_FRAGILE"
        why = ([f"top3 제거 시 부호 반전"] if flip3 else []) + \
              ([f"attrition {attrition}% >= "
                f"{QUALIFICATION['SIZE_FRAGILE']['attritionMaxPct']}%"]
               if attrition is not None
               and attrition >= QUALIFICATION["SIZE_FRAGILE"]["attritionMaxPct"]
               else [])
    elif all(checks.values()):
        verdict = "SIZE_EXECUTABLE_STRONG"
        why = list(checks)
    else:
        P = QUALIFICATION["SIZE_EXECUTABLE_PROMISING"]
        if (after_excess >= P["afterTopMinusControlPctMin"]
                and (inc.get("includedTopAnnPct") or 0) > 0
                and (subp.get("positiveRatio") or 0)
                >= P["minSubperiodPositiveRatio"]):
            verdict = "SIZE_EXECUTABLE_PROMISING"
            why = ["STRONG 미충족: "
                   + ", ".join(k for k, v in checks.items() if not v)]
        else:
            verdict = "SIZE_ALPHA_LARGELY_NONTRADABLE"
            why = ["PROMISING 기준도 미충족",
                   f"AFTER TOP−CONTROL {after_excess}%p"]

    # ── §32 BM vs SIZE 동일 기준 비교 ─────────────────────────────────
    cmp_rows = {
        "size36mTopAnnPct": a_size["meanTopAnnPct"],
        "bm36mTopAnnPct": a_bm["meanTopAnnPct"],
        "size36mExcessPct": a_size["meanExcessAnnPct"],
        "bm36mExcessPct": a_bm["meanExcessAnnPct"],
        "size36mSpreadPct": a_size["meanSpreadAnnPct"],
        "bm36mSpreadPct": a_bm["meanSpreadAnnPct"],
        "sizeAttritionPct": attrition,
        "bmAttritionPct": attr["byArm"]["BM"]["attritionPct"],
        "sizeMedianTradedValueKrw":
            part["byArm"]["SIZE"]["medianTradedValueKrw"],
        "bmMedianTradedValueKrw": part["byArm"]["BM"]["medianTradedValueKrw"],
        "sizeMedianParticipation": part["byArm"]["SIZE"]["medianParticipation"],
        "bmMedianParticipation": part["byArm"]["BM"]["medianParticipation"],
        "sizeDelistingTradablePct":
            dd["byGroup"]["SIZE_TRADABLE"]["delistingRatePct"],
        "bmDelistingTradablePct":
            dd["byGroup"]["BM_TRADABLE"]["delistingRatePct"],
        "sizeTop3Pct": conc["byArm"]["SIZE"]["top3Pct"],
        "bmTop3Pct": conc["byArm"]["BM"]["top3Pct"],
        "sizeHighCostTopPct": sc["cost"]["SIZE"]["highCostTopAnnPct"],
        "bmHighCostTopPct": sc["cost"]["BM"]["highCostTopAnnPct"],
    }
    size_ok = verdict in ("SIZE_EXECUTABLE_STRONG", "SIZE_EXECUTABLE_PROMISING")
    bm_ok = a_bm["meanExcessAnnPct"] > 0
    if size_ok and bm_ok:
        cand, rule = "BOTH_SEPARATE", "B"
    elif size_ok:
        cand, rule = "SIZE", "A"
    elif bm_ok:
        cand, rule = "BM", "C"
    else:
        cand, rule = "NONE", "E"

    out = {
        "task": "R27", "verdict": verdict, "verdictWhy": why,
        "primaryHorizon": f"{PH}M",
        "qualificationFromPrecommit": QUALIFICATION,
        "thresholdsUnchanged": True, "noParameterRescue": True,
        "checks": checks,
        "coverageGatePass": True,
        "dayCoveragePct": cov["dayCoveragePct"],
        "sizeReproductionExact": repro["allExactMatch"],
        "beforeAfter36M": {
            "sizeBeforeTopPct": b_size["meanTopAnnPct"],
            "sizeAfterTopPct": a_size["meanTopAnnPct"],
            "sizeBeforeExcessPct": b_size["meanExcessAnnPct"],
            "sizeAfterExcessPct": a_size["meanExcessAnnPct"],
            "retentionRatio": retention},
        "includedExcluded": inc,
        "attritionPct": attrition,
        "executionCandidate": cand,
        "executionCandidateMeaning": EXECUTION_CANDIDATE["bothSeparateMeaning"],
        "bmVsSize": cmp_rows,
        "nextTaskRule": rule, "nextTask": NEXT_TASK_RULE[rule],
        "portfolioResearchEntryAllowed": cand != "NONE",
        "portfolioOptimizationDone": False,
    }
    (RD / "r27-verdict-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    (RD / "r27-bm-vs-size-latest.json").write_text(
        json.dumps({"task": "R27", "criteria": EXECUTION_CANDIDATE["criteria"],
                    "comparison": cmp_rows, "executionCandidate": cand,
                    "notReturnOnly": EXECUTION_CANDIDATE["notReturnOnly"]},
                   ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    print(json.dumps({"verdict": verdict, "candidate": cand,
                      "failed": [k for k, v in checks.items() if not v]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
