#!/usr/bin/env python3
"""R33B1 실행 — filled-book turnover 를 주입한 결정적 전체 재실행.

WABABA-R33B-SUPPLEMENT-AND-NOFILL-PROVENANCE-AUDIT-R33B1

바뀌는 것은 **비용에 들어가는 turnover 의 구성원 정의** 하나뿐이다.
동일 163 cohort · 동일 entry/exit · 동일 no-fill · 동일 cost scenario ·
동일 factor 정의 · 동일 universe · 동일 threshold · 동일 tie-break ·
동일 R32 결정정책.

R32/R33A/R33B 동결 파일은 수정하지 않는다 — 실행 시점에만 turnover 함수를 주입한다.

안전: 네트워크 0 · API 0 · 인증키 접근 0 · R31 write 0 · 실주문 0.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import sys
import zoneinfo
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def main() -> int:
    import r32_decide as D
    import r32_engine as E
    import r33b1_provenance_audit as A
    import r33b_historical_validation as V
    import r33b_precommit as P
    from r27_collect import trading_calendar

    contract = P.contract_valid()
    if not contract["match"]:
        print("BLOCKED: contract hash divergence")
        return 1

    cohorts = V.load_cohorts()
    cmap = {c["signalDate"]: c for c in cohorts}
    book = V.OpenBook()
    cal = trading_calendar()

    R27T = V.make_translated_r27(cmap, book)
    K = R27T()

    # ── 최소 수정 주입: cost 에 들어가는 turnover 를 filled book 으로 ──
    orig = E.turnover_of
    filled_fn, tstats = A.make_filled_turnover_of(book, cmap, cal)
    E.turnover_of = filled_fn
    try:
        R32T = V.make_translated_r32(K)
        res = R32T().run()
    finally:
        E.turnover_of = orig

    res["task"] = "R33B1"
    res["executionStats"] = K.stat
    res["turnoverBasis"] = "FILLED_BOOK"
    res["turnoverFallback"] = tstats

    inp = D.build_inputs(res, baseline_pass=True)
    dec = D.decide(inp)

    pre = json.loads((RD / "r33b-translated-results-latest.json")
                     .read_text(encoding="utf-8"))
    pp, cp = pre["performance"], res["performance"]
    delta = {a: {"grossPct": {"pre": pp[a]["meanTopAnnPct"],
                              "post": cp[a]["meanTopAnnPct"]},
                 "annualTurnover": {
                     "pre": pp[a]["turnover"]["annualTurnover"],
                     "post": cp[a]["turnover"]["annualTurnover"]},
                 "net": {b: {"pre": pp[a]["costCurve"][b]["netPct"],
                             "post": cp[a]["costCurve"][b]["netPct"]}
                         for b in ("0bp", "25bp", "50bp", "100bp")}}
             for a in ("SIZE", "BM")}

    out = {
        "task": "R33B1", "taskId": A.TASK_ID,
        "generatedAt": datetime.datetime.now(
            zoneinfo.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds"),
        "contractHash": contract["contractHash"], "specHash": P.spec_hash(),
        "turnoverBasis": "FILLED_BOOK",
        "turnoverFallbackDates": tstats,
        "decisionPolicySha256": hashlib.sha256(
            (Path(__file__).resolve().parent / "r32_decide.py").read_bytes()
        ).hexdigest(),
        "preAuditDecision": "BOTH_SEPARATE",
        "postAuditDecision": dec["decision"],
        "decision": dec, "decisionInputs": inp,
        "prePostDelta": delta,
        "cohorts": res["matchedSample"]["commonCohortCount"],
        "executionStats": K.stat,
    }
    (RD / "r33b1-postaudit-results-latest.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    (RD / "r33b1-postaudit-decision-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    print(json.dumps({
        "cohorts": out["cohorts"],
        "turnoverBasis": "FILLED_BOOK",
        "fallback": tstats,
        "turnover": {a: delta[a]["annualTurnover"] for a in ("SIZE", "BM")},
        "net100bp": {a: delta[a]["net"]["100bp"] for a in ("SIZE", "BM")},
        "preAuditDecision": "BOTH_SEPARATE",
        "postAuditDecision": dec["decision"],
        "winnerRuleStep": dec.get("winnerRuleStep"),
    }, ensure_ascii=False, default=float))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
