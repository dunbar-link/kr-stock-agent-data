#!/usr/bin/env python3
"""R33B 실행 드라이버 — gate 확인 → 번역 성과 산출 → R32 결정정책 실행.

WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B

성과 gate 가 열리기 전에는 성과 함수를 부르지 않는다(fail-closed).
gate 가 열린 뒤에도 factor·universe·유동성·비용·tie-break·결정정책은 전부 동결본이다.

안전: 읽기·계산 전용. 네트워크 0 · R31 write 0 · 실주문 0.
"""
from __future__ import annotations

import datetime
import json
import sys
import zoneinfo
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def main() -> int:
    import r33a_availability as AV
    import r33b_historical_validation as V
    import r33b_precommit as P

    # ── PHASE 1·5: gate 조건 (성과 접근 전) ─────────────────────────
    guard = AV._PerfGuard()
    with guard:
        cohorts = V.load_cohorts()
        book = V.OpenBook()
        gate = V.completeness_gate(cohorts, book)
    pre_calls = guard.calls

    contract = P.contract_valid()
    plan = json.loads((RD / "r33b-missing-date-plan-latest.json")
                      .read_text(encoding="utf-8"))
    g = V.open_gate(gate, contract["match"], P.spec_hash(),
                    plan["planHash"], pre_calls)
    print(json.dumps({"gate": g["conditions"], "open": g["open"],
                      "preGatePerformanceCalls": pre_calls}, ensure_ascii=False))
    if not g["open"]:
        print("PERFORMANCE_GATE: CLOSED — 성과 계산을 실행하지 않는다.")
        return 1

    opened_at = datetime.datetime.now(
        zoneinfo.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    print(f"PERFORMANCE_GATE: OPEN at {opened_at}")

    # ── PHASE 7: 번역 성과 ──────────────────────────────────────────
    cmap = {c["signalDate"]: c for c in cohorts}
    R27T = V.make_translated_r27(cmap, book)
    K = R27T()
    R32T = V.make_translated_r32(K)
    E = R32T()
    print(f"paired rows: {len(E.rows)} ({E.dates[0]} .. {E.dates[-1]})")

    # R32.run() 이 내는 구조를 그대로 쓴다 — r32_decide.build_inputs 가
    # 기대하는 키("before"/"performance"/…)와 정확히 일치시키기 위해서다.
    res = E.run()
    res["task"] = "R33B"
    res["executionStats"] = K.stat
    res["gate"] = {**g, "openedAt": opened_at,
                   "preGatePerformanceFunctionCalls": pre_calls}
    res["completeness"] = gate
    res["hashes"] = {"contract": contract["contractHash"],
                     "spec": P.spec_hash(), "plan": plan["planHash"]}
    res["translationRule"] = P.TRANSLATION_RULE
    (RD / "r33b-translated-results-latest.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    print("saved r33b-translated-results-latest.json")

    # ── PHASE 9: 동결 R32 결정정책 그대로 실행 ──────────────────────
    import hashlib

    import r32_decide as D
    inp = D.build_inputs(res, baseline_pass=True)
    dec = D.decide(inp)
    out = {"task": "R33B", "translationRule": P.TRANSLATION_RULE,
           "contractHash": contract["contractHash"], "specHash": P.spec_hash(),
           "planHash": plan["planHash"],
           "decisionPolicySha256": hashlib.sha256(
               (Path(__file__).resolve().parent / "r32_decide.py").read_bytes()
           ).hexdigest(),
           "decisionInputs": inp, "decision": dec,
           "cohorts": res["matchedSample"]["commonCohortCount"],
           "executionStats": K.stat}
    (RD / "r33b-decision-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    perf = res["performance"]
    print("\n=== TRANSLATED PERFORMANCE (36M, BASE gate) ===")
    print(json.dumps({
        "cohorts": res["matchedSample"]["commonCohortCount"],
        "period": [res["period"]["firstSignalDate"], res["period"]["lastSignalDate"]],
        "SIZE": perf["SIZE"]["meanTopAnnPct"], "BM": perf["BM"]["meanTopAnnPct"],
        "CONTROL": perf["CONTROL"]["meanTopAnnPct"],
        "sizeMinusBm": (perf["directDifference"] or {}).get("meanPct"),
        "net": {a: {b: perf[a]["costCurve"][b]["netPct"]
                    for b in ("0bp", "25bp", "50bp", "100bp")}
                for a in ("SIZE", "BM")},
        "costRankReversal": perf["costRankReversal"],
    }, ensure_ascii=False, default=float))
    print("\n=== EXECUTION STATS ===")
    print(json.dumps(K.stat, ensure_ascii=False))
    print("\n=== DECISION ===")
    print(json.dumps({"decision": dec["decision"],
                      "winnerRuleStep": dec.get("winnerRuleStep"),
                      "reasonCodes": dec["reasonCodes"],
                      "soloPrimaryChecks": dec.get("soloPrimaryChecks"),
                      "rejections": dec["rejections"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
