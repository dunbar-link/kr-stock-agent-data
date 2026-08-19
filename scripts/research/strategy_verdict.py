#!/usr/bin/env python3
"""전략 후보/사업 판정을 **규칙으로** 도출한다 (WABABA-BENCHMARK-SURVIVORSHIP-FIX-...-R5).

사람이 손으로 쓴 결론을 파일에 박지 않는다. baseline/matrix/robustness 산출물에서
사전 정의된 규칙으로만 판정을 계산해 canonical 판정 파일을 만든다(재실행하면 같은 결과).

판정 규칙
---------
candidateVerdict (§4)
  ROBUST_CANDIDATE   plateau robustCells >= 3
  WEAK_CANDIDATE     robustCells 1~2
  OVERFIT_PEAK       robustCells 0 이고 단독 peak 존재
  NO_ROBUST_STRATEGY 그 외

businessVerdict (§8)
  SCALE   ROBUST_CANDIDATE + 고비용 시나리오에서도 공정 benchmark 초과
          + 국면 3종 모두 평균초과 > 0 + 최장 수중기간 <= 60개월
  OBSERVE WEAK_CANDIDATE 이거나, 초과가 있으나 위 조건 중 하나 이상 미달
  HOLD    그 외 (우위 불명확 / 과최적화 / 운용부담 대비 가치 낮음)

안전: read-only 계산 + 판정 파일 1개 write. 네트워크 0 · canonical 장부 미접근.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
OUT = RD / "strategy-verdict-latest.json"
MAX_TOLERABLE_UNDERWATER_MONTHS = 60      # 5년 초과 수중은 일반인이 못 버틴다


def load(name):
    try:
        return json.loads((RD / name).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def main() -> int:
    base, mtx, rob = load("baseline-latest.json"), load("matrix-latest.json"), load("robustness-latest.json")
    if not (base and mtx and rob):
        OUT.write_text(json.dumps({"verdict": "UNKNOWN", "reason": "산출물 누락"},
                                  ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"verdict": "UNKNOWN"}, ensure_ascii=False))
        return 1

    bm = base.get("benchmarks", {})
    fair = bm.get("EQUAL_MONTHLY", {}).get("cagr")
    plateau = mtx.get("plateau", {})
    cand = plateau.get("verdict", "NO_ROBUST_STRATEGY")

    mrows = [r for r in mtx.get("matrix", []) if r.get("excessVsFair") is not None]
    rrows = [r for r in rob.get("robustness", []) if r.get("excessVsFair") is not None]
    beat_m = [r for r in mrows if r["excessVsFair"] > 0]
    beat_r = [r for r in rrows if r["excessVsFair"] > 0]
    cost_high = next((r for r in rrows if r["tag"] == "COST_HIGH"), None)
    reg = rob.get("regimeSummary", {})
    all_regimes_positive = bool(reg) and all(v["meanExcess"] > 0 for v in reg.values())
    starts = [r for r in rrows if r["tag"].startswith("STARTOFFSET")]
    starts_beating = sum(1 for r in starts if r["excessVsFair"] > 0)
    subs = [r for r in rrows if r["tag"].startswith("SUB_")]
    subs_beating = sum(1 for r in subs if r["excessVsFair"] > 0)

    worst_uw = max((r.get("maxUnderwaterMonths") or 0) for r in base.get("baseline", [])) if base.get("baseline") else None
    best_row = max(mrows, key=lambda r: r["excessVsFair"]) if mrows else None

    conds = {
        "candidateIsRobust": cand == "ROBUST_CANDIDATE",
        "beatsFairUnderHighCost": bool(cost_high and cost_high["excessVsFair"] > 0),
        "allRegimesPositiveExcess": all_regimes_positive,
        "startMonthMajorityBeats": bool(starts) and starts_beating > len(starts) / 2,
        "subperiodMajorityBeats": bool(subs) and subs_beating > len(subs) / 2,
        "underwaterTolerable": bool(worst_uw is not None and worst_uw <= MAX_TOLERABLE_UNDERWATER_MONTHS),
    }
    if conds["candidateIsRobust"] and conds["beatsFairUnderHighCost"] \
       and conds["allRegimesPositiveExcess"] and conds["underwaterTolerable"]:
        business = "SCALE"
    elif cand == "WEAK_CANDIDATE" or (beat_m and conds["beatsFairUnderHighCost"]):
        business = "OBSERVE"
    else:
        business = "HOLD"

    out = {
        "schema": "WABABA_STRATEGY_VERDICT_V1",
        "taskId": "WABABA-BENCHMARK-SURVIVORSHIP-FIX-AND-STRATEGY-REASSESS-R5",
        "formulaLabel": "APPROXIMATE_MAGIC_FORMULA",
        "originalStyle": {
            "label": "ORIGINAL_STYLE_MAGIC_FORMULA",
            "status": "NOT_COMPUTABLE_WITH_CURRENT_CACHE",
            "reason": "DART corp_code→종목코드 매핑 부재(dart-corp-codes.json 3,960건 중 stock_code 보유 0건). "
                      "또한 가용 구간이 2023-04~2026-08(약 3.3년)뿐이라 19.58년 장기 결론의 대체재가 될 수 없다.",
        },
        "period": {"start": base["meta"]["backtestStart"], "end": base["meta"]["backtestEnd"],
                   "months": base["meta"]["contiguousMonths"]},
        "benchmarkFairCagr": fair,
        "benchmarks": bm,
        "candidateVerdict": cand,
        "businessVerdict": business,
        "conditions": conds,
        "evidence": {
            "matrixBeatingFair": len(beat_m), "matrixTotal": len(mrows),
            "robustBeatingFair": len(beat_r), "robustTotal": len(rrows),
            "startMonthsBeating": starts_beating, "startMonthsTotal": len(starts),
            "subperiodsBeating": subs_beating, "subperiodsTotal": len(subs),
            "regimeSummary": reg,
            "plateauRobustCells": plateau.get("robustCount"),
            "plateauIsolatedPeaks": plateau.get("peakCount"),
            "bestExcessTag": best_row["tag"] if best_row else None,
            "bestExcess": best_row["excessVsFair"] if best_row else None,
            "worstUnderwaterMonths": worst_uw,
        },
        "finalStrategyModels": [] if business == "HOLD" else None,
        "realOrderCount": 0, "brokerApiCallCount": 0,
    }
    RD.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"candidateVerdict": cand, "businessVerdict": business,
                      "out": str(OUT)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
