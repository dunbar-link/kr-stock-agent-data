#!/usr/bin/env python3
"""magic_morning_combined_report.classify_morning / to_txt 테스트.

시간/OS/네트워크/파일 전부 주입 → 순수 판정 로직만 검증. read-only."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import magic_morning_combined_report as M

KST = timezone(timedelta(hours=9))
TODAY = "2026-07-10"       # 금(거래일)
YDAY = "2026-07-09"        # 목(거래일)
NON_TRADING_TODAY = "2026-07-11"   # 토
MORNING = datetime(2026, 7, 10, 7, 40, tzinfo=KST)

CORE = ("Wababa Magic Daily Signal", "Wababa Magic Daily Dry Run", "Wababa Magic Daily Status")
REPORT = ("Wababa Magic Daily Observe Report", "Wababa Magic Morning Combined Report")


def _sched(states=None, results=None, drop=()):
    states = states or {}
    results = results or {}
    out = []
    for n in CORE + REPORT:
        if n in drop:
            continue
        out.append({"name": n, "found": True, "state": states.get(n, "Ready"),
                    "lastRunTime": "2026-07-09T16:05:00", "lastTaskResult": results.get(n, 0),
                    "nextRunTime": "2026-07-10T16:05:00"})
    return out


SCHED_OK = _sched()
GOOD_DR = {"status": "COMPLETED", "runStatus": "COMPLETED", "proposedSequence": 13,
           "proposedBatchId": "MF-BATCH-2026-07-09", "signalAsOfDate": "2026-07-08",
           "buyCount": 10, "sellCount": 0, "missingEvalCodes": [], "productionWriteCount": 0,
           "readOnlyUnchanged": True, "lookAheadValidationPassed": True, "noFakeTrade": True,
           "openPrices": {str(i): 1.0 for i in range(13)}}
EVID = {"top10Count": 10, "top100Count": 100, "evMethod": "m",
        "rank1": {"code": "046940", "name": "우원개발", "earningsYield": 0.49, "returnOnCapital": 0.84,
                  "signalClosePrice": 3475.0, "valueRank": 1, "profitabilityRank": 11, "combinedRank": 12}}
LIVE_OK = {"jsonHttp": 200, "performanceHttp": 200, "rankingsHttp": 200, "deployedSequence": 13,
           "deployedTotalAsset": 49340818, "tradeDayCount": 13, "rankingsDataDate": "2026-07-08",
           "rankingsCheapCount": 100}
CLEAN = {"head": "abc", "branch": "main", "modified": [], "stagedCount": 0, "behind": 0}
KNOWN2 = {"head": "abc", "branch": "main", "modified": ["financial-universe-real.json"], "stagedCount": 0, "behind": 0}


def _run(**kw):
    base = dict(today=TODAY, yesterday=YDAY, now_dt=MORNING, is_today_trading=True,
                is_yesterday_trading=True, canonical={"officialSequence": 13}, y_dryrun=GOOD_DR,
                evidence=EVID, sched=SCHED_OK, live=LIVE_OK, repo1=CLEAN, repo2=CLEAN, backup_done=True)
    base.update(kw)
    return M.classify_morning(**base)


def t1_already_applied_pass():
    r = _run()
    assert r["verdict"] == "PASS", r
    assert r["closeoutState"] == "ALREADY_APPLIED", r
    assert r["todayExpectedSequence"] == 14, r


def t2_ready_to_closeout_pass():
    r = _run(canonical={"officialSequence": 12}, live=dict(LIVE_OK, deployedSequence=12))
    assert r["verdict"] == "PASS", r
    assert r["closeoutState"] == "READY_TO_CLOSEOUT", r


def t3_wait_yesterday_missing():
    r = _run(y_dryrun=None)
    assert r["verdict"] == "WAIT", r


def t4_blocked_dryrun_not_completed():
    r = _run(y_dryrun=dict(GOOD_DR, status="BLOCKED", runStatus="BLOCKED", blockedCode="BLOCKED_LOOKAHEAD"))
    assert r["verdict"] == "BLOCKED", r


def t5_blocked_canonical_live_mismatch():
    r = _run(live=dict(LIVE_OK, deployedSequence=12))   # canon 13 != live 12
    assert r["verdict"] == "BLOCKED", r
    assert any("live seq" in b for b in r["blocked"]), r


def t6_blocked_staged():
    r = _run(repo1=dict(CLEAN, stagedCount=1))
    assert r["verdict"] == "BLOCKED", r
    assert any("staged" in b for b in r["blocked"]), r


def t7_blocked_core_scheduler_disabled():
    r = _run(sched=_sched(states={"Wababa Magic Daily Status": "Disabled"}))
    assert r["verdict"] == "BLOCKED", r
    assert any("Disabled" in b for b in r["blocked"]), r


def t8_blocked_repo_behind():
    r = _run(repo2=dict(CLEAN, behind=2))
    assert r["verdict"] == "BLOCKED", r
    assert any("behind" in b for b in r["blocked"]), r


def t9_pass_with_known_warn_only():
    r = _run(repo2=KNOWN2)
    assert r["verdict"] == "PASS", r
    assert any("financial-universe" in w for w in r["knownWarns"]), r


def t10_warning_morning_task_missing():
    r = _run(sched=_sched(drop=("Wababa Magic Morning Combined Report",)))
    assert r["verdict"] == "WARNING", r
    assert any("Morning Combined" in w for w in r["actionWarns"]), r


def t11_wait_non_trading_today_already_applied():
    r = _run(today=NON_TRADING_TODAY, is_today_trading=False)
    assert r["verdict"] == "WAIT", r
    assert "비거래일" in r["hint"], r


def t12_warning_repo1_public_dirty():
    dirty = dict(CLEAN, modified=["public/data/recommendation-history.json"])
    r = _run(repo1=dirty)
    assert r["verdict"] == "WARNING", r


def _full(o_extra):
    base = {"today": TODAY, "yesterday": YDAY, "createdAt": "2026-07-10T07:40:00+09:00",
            "canonical": {"officialSequence": 13}, "yesterdayDryRun": GOOD_DR,
            "yesterdaySignal": {}, "yesterdayStatus": {}, "evidence": EVID, "scheduler": SCHED_OK,
            "live": LIVE_OK, "repo1": CLEAN, "repo2": CLEAN, "backupDone": True}
    base.update(o_extra)
    return base


def t13_txt_already_applied_phrasing():
    o = _full({"verdict": "PASS", "hint": "h", "closeoutState": "ALREADY_APPLIED",
               "yesterdaySequence": 13, "yesterdayBatchId": "MF-BATCH-2026-07-09",
               "todayExpectedSequence": 14, "todayExpectedBatchId": "MF-BATCH-2026-07-10",
               "canonicalSequence": 13, "blocked": [], "actionWarns": [], "knownWarns": []})
    txt = M.to_txt(o)
    assert txt.splitlines()[0] == "전체 판정: PASS", txt.splitlines()[0]
    assert "[자동화 채팅창에 붙일 요청]" in txt
    assert "closeout 지시문은 만들지 말고" in txt, "ALREADY_APPLIED 안내 문구 누락"


def t14_txt_ready_to_closeout_phrasing():
    o = _full({"verdict": "PASS", "hint": "h", "closeoutState": "READY_TO_CLOSEOUT",
               "yesterdaySequence": 13, "yesterdayBatchId": "MF-BATCH-2026-07-09",
               "todayExpectedSequence": 14, "todayExpectedBatchId": "MF-BATCH-2026-07-10",
               "canonicalSequence": 12, "blocked": [], "actionWarns": [], "knownWarns": []})
    txt = M.to_txt(o)
    assert "seq13 ONE-SHOT-CLOSEOUT" in txt, "READY_TO_CLOSEOUT closeout 요청 문구 누락"
    assert M.next_action_line(o).startswith("Phase MF-SEQ13-ONE-SHOT-CLOSEOUT"), M.next_action_line(o)


def t15_txt_blocked_phrasing():
    o = _full({"verdict": "BLOCKED", "hint": "h", "closeoutState": "ALREADY_APPLIED",
               "yesterdaySequence": 13, "yesterdayBatchId": "MF-BATCH-2026-07-09",
               "todayExpectedSequence": 14, "todayExpectedBatchId": "MF-BATCH-2026-07-10",
               "canonicalSequence": 13, "blocked": ["x"], "actionWarns": [], "knownWarns": []})
    txt = M.to_txt(o)
    assert "blocker 원인분리 지시문" in txt, "BLOCKED 요청 문구 누락"


TESTS = [
    ("1  어제 반영완료 → ALREADY_APPLIED PASS", t1_already_applied_pass),
    ("2  어제 미반영+PASS → READY_TO_CLOSEOUT", t2_ready_to_closeout_pass),
    ("3  어제 산출물 없음 → WAIT", t3_wait_yesterday_missing),
    ("4  어제 dry-run 미완료 → BLOCKED", t4_blocked_dryrun_not_completed),
    ("5  canonical/live 불일치 → BLOCKED", t5_blocked_canonical_live_mismatch),
    ("6  staged 존재 → BLOCKED", t6_blocked_staged),
    ("7  핵심 스케줄러 Disabled → BLOCKED", t7_blocked_core_scheduler_disabled),
    ("8  repo behind → BLOCKED", t8_blocked_repo_behind),
    ("9  구조적 known-warn만 → PASS", t9_pass_with_known_warn_only),
    ("10 Morning task 미등록 → WARNING", t10_warning_morning_task_missing),
    ("11 오늘 휴장+어제완료 → WAIT", t11_wait_non_trading_today_already_applied),
    ("12 REPO1 public 미커밋 → WARNING", t12_warning_repo1_public_dirty),
    ("13 txt ALREADY_APPLIED 문구", t13_txt_already_applied_phrasing),
    ("14 txt READY_TO_CLOSEOUT 문구", t14_txt_ready_to_closeout_phrasing),
    ("15 txt BLOCKED 문구", t15_txt_blocked_phrasing),
]


def main():
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
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
