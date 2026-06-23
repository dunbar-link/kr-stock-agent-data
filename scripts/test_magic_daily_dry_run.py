#!/usr/bin/env python3
"""magic_daily_dry_run.dry_run_core 테스트 (Phase 45-AUTO2). 네트워크 0(calendar/opens 주입). 저장 0."""
from __future__ import annotations

import copy

import magic_rolling_engine as E
import magic_daily_dry_run as D

D1, D2 = "2030-01-02", "2030-01-03"   # 주입 캘린더 기준(수/목)
HELD = [f"{i:06d}" for i in range(1, 11)]      # batch1 보유(canonical)
TOP = [f"{i:06d}" for i in range(2, 12)]       # TOP10: 000001 탈락, 000011 신규
UNION = [f"{i:06d}" for i in range(1, 12)]
PX = {f"{i:06d}": 1000.0 + i * 7 for i in range(1, 12)}


def build_canonical_day1():
    cal = E.make_calendar([D1, D2])
    st = E.empty_official_state()
    rk = [{"code": c, "name": f"S{c}", "rank": i, "combinedRank": i, "profitabilityRank": i,
           "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2} for i, c in enumerate(HELD, 1)]
    px1 = {c: 1000.0 for c in HELD}
    timing = {"signalAsOfDate": "2030-01-01", "rankingGeneratedAt": f"{D1}T08:00:00+09:00",
              "executionDate": D1, "executionMarketOpenAt": f"{D1}T09:00:00+09:00",
              "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    st, _ = E.plan_official_day(st, D1, rk, px1, px1, cal, now=f"{D1}T18:00:00+09:00", timing=timing, trading_day_index=1)
    return st


def mk_manifest(gen=f"{D1}T18:00:00+09:00", base=D1):
    return {"signalAsOfDate": D1, "nextExecutionDateCandidate": D2, "rankingGeneratedAt": gen,
            "universeBaseDate": base}


def mk_rankings():
    return {"top10": [{"code": c, "name": f"S{c}", "rank": i, "combinedRank": i, "profitabilityRank": i,
                       "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2} for i, c in enumerate(TOP, 1)]}


CAL = E.make_calendar([D1, D2])
NOW = f"{D2}T15:45:00+09:00"


def t1_completed_eval_union():
    canon = build_canonical_day1()
    res = D.dry_run_core(canon, mk_manifest(), mk_rankings(), CAL, dict(PX), now_iso=NOW)
    assert res["status"] == "COMPLETED", res
    assert res["proposedSequence"] == 2 and res["buyCount"] == 10 and res["sellCount"] == 0
    # 신규 000011 매수, 보유-비TOP10 000001은 eval union으로 평가됨(missing 없음)
    assert res["missingEvalCodes"] == [], res["missingEvalCodes"]
    assert any(p["code"] == "000011" for p in res["plan"])
    assert res["holdingsMarketValuePreview"] and res["holdingsMarketValuePreview"] > 0


def t2_lookahead_blocked():
    canon = build_canonical_day1()
    res = D.dry_run_core(canon, mk_manifest(gen=f"{D2}T09:30:00+09:00"), mk_rankings(), CAL, dict(PX), now_iso=NOW)
    assert res["status"] == "BLOCKED" and res["blockedCode"] == "BLOCKED_LOOKAHEAD", res


def t3_missing_top_open_blocked():
    canon = build_canonical_day1()
    opens = {k: v for k, v in PX.items() if k != "000011"}   # TOP10의 000011 누락
    res = D.dry_run_core(canon, mk_manifest(), mk_rankings(), CAL, opens, now_iso=NOW)
    assert res["status"] == "BLOCKED" and res["blockedCode"] == "BLOCKED_MISSING_OPEN_PRICE", res


def t4_prev_mismatch_blocked():
    canon = build_canonical_day1()
    bad_cal = E.make_calendar(["2030-01-01", D2])   # prev(D2)=2030-01-01 != signalAsOf D1
    res = D.dry_run_core(canon, mk_manifest(), mk_rankings(), bad_cal, dict(PX), now_iso=NOW)
    assert res["status"] == "BLOCKED" and res["blockedCode"] == "BLOCKED_LOOKAHEAD", res


def t5_input_canonical_not_mutated():
    canon = build_canonical_day1()
    before = copy.deepcopy(canon)
    D.dry_run_core(canon, mk_manifest(), mk_rankings(), CAL, dict(PX), now_iso=NOW)
    assert canon == before, "dry_run_core가 입력 canonical을 변경하면 안 됨(deepcopy)"


def t6_held_non_top10_valued():
    canon = build_canonical_day1()
    res = D.dry_run_core(canon, mk_manifest(), mk_rankings(), CAL, dict(PX), now_iso=NOW)
    assert res["status"] == "COMPLETED"
    assert "000001" in res["openPrices"] and res["openPrices"]["000001"] == PX["000001"]
    assert res["productionWriteCount"] == 0 and res["officialStartDatePersisted"] is False


TESTS = [
    ("1  COMPLETED + eval union(보유-비TOP10 평가)", t1_completed_eval_union),
    ("2  look-ahead BLOCKED", t2_lookahead_blocked),
    ("3  TOP10 시가 누락 BLOCKED", t3_missing_top_open_blocked),
    ("4  prev!=signalAsOf BLOCKED", t4_prev_mismatch_blocked),
    ("5  입력 canonical 불변", t5_input_canonical_not_mutated),
    ("6  보유-비TOP10 000001 평가가 반영", t6_held_non_top10_valued),
]


def main():
    p = f = 0
    for name, fn in TESTS:
        try:
            fn(); print(f"[PASS] {name}"); p += 1
        except AssertionError as e:
            print(f"[FAIL] {name} -> {e}"); f += 1
        except Exception as e:  # noqa: BLE001
            import traceback; print(f"[ERROR] {name} -> {type(e).__name__}: {e}"); traceback.print_exc(); f += 1
    print(f"\n결과: {p} passed, {f} failed (총 {len(TESTS)})")
    return 0 if f == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
