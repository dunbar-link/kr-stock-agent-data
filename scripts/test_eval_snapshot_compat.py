#!/usr/bin/env python3
"""evaluationSnapshots/evalSnapshots 스키마 호환 테스트 (Phase 45-E12.1).

canonical 파일은 evaluationSnapshots 키를 쓰지만 엔진은 과거 evalSnapshots를 append하던 불일치(KeyError)를
in-memory 정규화로 해결한 것을 검증한다. 실행: python scripts/test_eval_snapshot_compat.py
"""
from __future__ import annotations

import copy

import magic_rolling_engine as E

CODES = [f"{i:06d}" for i in range(1, 11)]
PX = {c: 1000.0 for c in CODES}


def mk_ranking(codes):
    return [{"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
             "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.5, "earningsYield": 0.2}
            for i, c in enumerate(codes)]


def day1(date="2026-06-17"):
    cal = E.make_calendar([date]); st = E.empty_official_state()
    timing = {"signalAsOfDate": "2026-06-16", "rankingGeneratedAt": "2026-06-16T18:00:00+09:00",
              "executionDate": date, "executionMarketOpenAt": f"{date}T09:00:00+09:00",
              "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    st, _ = E.plan_official_day(st, date, mk_ranking(CODES), PX, PX, cal, now=f"{date}T18:00:00+09:00", timing=timing)
    return st  # 표준 evaluationSnapshots 보유


def to_legacy(st):
    s = copy.deepcopy(st)
    s["evalSnapshots"] = s.pop("evaluationSnapshots")
    return s


def plan_day2(state, idx=3, date="2026-06-19"):
    cal = E.make_calendar(["2026-06-17", "2026-06-18", "2026-06-19"])
    timing = {"signalAsOfDate": "2026-06-18", "rankingGeneratedAt": "2026-06-18T18:00:00+09:00",
              "executionDate": date, "executionMarketOpenAt": f"{date}T09:00:00+09:00",
              "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    return E.plan_official_day(state, date, mk_ranking(CODES), PX, PX, cal,
                              now=f"{date}T18:00:00+09:00", timing=timing, trading_day_index=idx)


def t1_evaluation_only_ok():
    st = day1()
    assert "evaluationSnapshots" in st and "evalSnapshots" not in st
    st2, res = plan_day2(st)
    assert res["runStatus"] == E.COMPLETED and st2["officialSequence"] == 2


def t2_legacy_evalsnapshots_compat():
    leg = to_legacy(day1())   # evalSnapshots만(표준 키 없음)
    assert "evaluationSnapshots" not in leg and "evalSnapshots" in leg
    st2, res = plan_day2(leg)
    assert res["runStatus"] == E.COMPLETED
    assert "evaluationSnapshots" in st2 and "evalSnapshots" not in st2   # 정규화됨


def t3_neither_key_empty_array():
    s = day1(); s.pop("evaluationSnapshots", None); s.pop("evalSnapshots", None)
    st2, res = plan_day2(s)
    assert res["runStatus"] == E.COMPLETED
    assert isinstance(st2["evaluationSnapshots"], list) and len(st2["evaluationSnapshots"]) == 1


def t4_alias_conflict_blocked():
    s = day1()
    s["evalSnapshots"] = [{"date": "2026-06-17", "totalAsset": 999}]   # 내용 다름
    st2, res = plan_day2(s)
    assert res["runStatus"] == E.BLOCKED_EVALUATION_SNAPSHOT_ALIAS_CONFLICT, res["runStatus"]


def t5_input_object_not_mutated():
    st = day1(); before = copy.deepcopy(st)
    plan_day2(st)
    assert st == before, "plan_official_day가 입력 state를 변경하면 안 됨(deepcopy)"


def t6_new_snapshot_only_in_evaluation():
    st2, _ = plan_day2(day1())
    assert "evalSnapshots" not in st2          # 중복 키 미생성
    assert len(st2["evaluationSnapshots"]) == 2 # day1 + day2


def t7_day2_preview_no_keyerror():
    st2, res = plan_day2(day1())
    nb = st2["batches"][-1]
    assert res["runStatus"] == E.COMPLETED and res["buyBatchId"] == "MF-BATCH-2026-06-19"
    assert nb["buyTradingDayIndex"] == 3 and nb["plannedSellTradingDayIndex"] == 53
    assert res["buyCount"] == 10 and res["sellCount"] == 0


def t8_missedrun_state_compat():
    # 1일차 → 06-18 MISSED_RUN → 06-19 2일차: evaluationSnapshots 호환 + index 모델 동작
    st, _, _ = E.apply_missed_run(day1(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    assert st["officialTradingDayIndex"] == 2
    st2, res = plan_day2(st, idx=3)
    assert res["runStatus"] == E.COMPLETED and st2["officialSequence"] == 2
    nb = st2["batches"][-1]
    assert nb["buyTradingDayIndex"] == 3 and nb["plannedSellTradingDayIndex"] == 53


def t9_normalize_helper_direct():
    a = E.normalize_eval_snapshots({"evalSnapshots": [1, 2]})
    assert a["evaluationSnapshots"] == [1, 2] and "evalSnapshots" not in a
    b = E.normalize_eval_snapshots({})
    assert b["evaluationSnapshots"] == []
    c = E.normalize_eval_snapshots({"evaluationSnapshots": [1], "evalSnapshots": [1]})
    assert c["evaluationSnapshots"] == [1] and "evalSnapshots" not in c
    try:
        E.normalize_eval_snapshots({"evaluationSnapshots": [1], "evalSnapshots": [2]})
        assert False, "내용 불일치는 예외여야 함"
    except E.EvalSnapshotAliasConflict:
        pass


TESTS = [
    ("1  evaluationSnapshots만→성공", t1_evaluation_only_ok),
    ("2  legacy evalSnapshots만→in-memory 호환", t2_legacy_evalsnapshots_compat),
    ("3  둘 다 없음→빈 배열 정상", t3_neither_key_empty_array),
    ("4  둘 다 다름→ALIAS_CONFLICT", t4_alias_conflict_blocked),
    ("5  입력 객체 불변", t5_input_object_not_mutated),
    ("6  새 snapshot은 evaluationSnapshots에만", t6_new_snapshot_only_in_evaluation),
    ("7  2일차 preview KeyError 없음", t7_day2_preview_no_keyerror),
    ("8  MISSED_RUN state 호환", t8_missedrun_state_compat),
    ("9  normalize 헬퍼 직접", t9_normalize_helper_direct),
]


def main():
    passed, failed = 0, 0
    for name, fn in TESTS:
        try:
            fn(); print(f"[PASS] {name}"); passed += 1
        except AssertionError as e:
            print(f"[FAIL] {name}  -> {e}"); failed += 1
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {name}  -> {type(e).__name__}: {e}"); failed += 1
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(TESTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
