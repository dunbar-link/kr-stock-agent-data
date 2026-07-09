#!/usr/bin/env python3
"""magic_daily_observe_report.classify / to_txt 테스트 (Phase MF-DAILY-OBSERVE-AUTOMATION).

now_dt·is_trading_day·수집결과를 전부 주입해 시간/OS/네트워크 의존 0. read-only 순수 로직만 검증."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import magic_daily_observe_report as OB

KST = timezone(timedelta(hours=9))
TRADING = "2026-07-09"        # 목
NON_TRADING = "2026-07-11"    # 토
BEFORE_CLOSE = datetime(2026, 7, 9, 14, 0, tzinfo=KST)
PRE_SCHED = datetime(2026, 7, 9, 15, 50, tzinfo=KST)   # 장마감 후·status(16:05) 전
AFTER = datetime(2026, 7, 9, 16, 10, tzinfo=KST)

TASKS = ("Wababa Magic Daily Signal", "Wababa Magic Daily Dry Run", "Wababa Magic Daily Status")
SCHED_OK = [{"name": n, "found": True, "lastRunTime": "2026-07-09T16:05:00",
             "lastTaskResult": 0, "nextRunTime": "2026-07-10T16:05:00"} for n in TASKS]
SCHED_FAIL = [dict(s, lastTaskResult=(1 if s["name"].endswith("Status") else 0)) for s in SCHED_OK]

GOOD_DR = {"status": "COMPLETED", "runStatus": "COMPLETED", "proposedSequence": 13,
           "proposedBatchId": "MF-BATCH-2026-07-09", "signalAsOfDate": "2026-07-08",
           "buyCount": 10, "sellCount": 0, "missingEvalCodes": [], "productionWriteCount": 0,
           "readOnlyUnchanged": True, "lookAheadValidationPassed": True, "noFakeTrade": True,
           "openPrices": {str(i): 1.0 for i in range(13)}}
EVID = {"top10Count": 10, "top100Count": 100, "evMethod": "marketCap_x",
        "rank1": {"code": "046940", "name": "우원개발", "earningsYield": 0.49, "returnOnCapital": 0.84,
                  "signalClosePrice": 3475.0, "valueRank": 1, "profitabilityRank": 11, "combinedRank": 12}}
LIVE_OK = {"jsonHttp": 200, "performanceHttp": 200, "rankingsHttp": 200,
           "deployedSequence": 12, "deployedTotalAsset": 49340818, "tradeDayCount": 12}
CLEAN = {"path": "x", "head": "abc", "branch": "master", "modified": [], "stagedCount": 0}
KNOWN2 = {"path": "x", "head": "abc", "branch": "main", "modified": ["financial-universe-real.json"], "stagedCount": 0}
KNOWN1 = {"path": "x", "head": "abc", "branch": "master", "modified": ["next-env.d.ts"], "stagedCount": 0}


def _run(**kw):
    base = dict(today=TRADING, now_dt=AFTER, is_trading_day=True, canonical={"officialSequence": 12},
                dryrun=GOOD_DR, evidence=EVID, sched=SCHED_OK, live=LIVE_OK, repo1=CLEAN, repo2=CLEAN)
    base.update(kw)
    return OB.classify(**base)


def t1_non_trading_wait():
    r = _run(is_trading_day=False, today=NON_TRADING)
    assert r["verdict"] == "WAIT", r


def t2_before_close_wait():
    r = _run(now_dt=BEFORE_CLOSE, dryrun=None)
    assert r["verdict"] == "WAIT", r


def t3_pre_scheduler_wait():
    r = _run(now_dt=PRE_SCHED, dryrun=None)
    assert r["verdict"] == "WAIT" and "status" in r["hint"], r


def t4_pass_needed_clean():
    r = _run()
    assert r["verdict"] == "PASS", r
    assert r["closeoutState"] == "NEEDED", r
    assert r["actualProposedSequence"] == 13 and r["expectedProposedSequence"] == 13, r


def t5_pass_with_expected_known_warn():
    # 구조적 known-warn(financial-universe / next-env)만 있으면 PASS 유지
    r = _run(repo1=KNOWN1, repo2=KNOWN2)
    assert r["verdict"] == "PASS", r
    assert any("financial-universe" in w for w in r["knownWarns"]), r
    assert any("next-env" in w for w in r["knownWarns"]), r
    assert r["actionWarns"] == [], r


def t6_warning_repo1_public_dirty():
    dirty = dict(CLEAN, modified=["public/data/recommendation-history.json"])
    r = _run(repo1=dirty)
    assert r["verdict"] == "WARNING", r
    assert any("recommendation-history" in w for w in r["actionWarns"]), r


def t7_blocked_dryrun_not_completed():
    bad = dict(GOOD_DR, status="BLOCKED", runStatus="BLOCKED", blockedCode="BLOCKED_LOOKAHEAD")
    r = _run(dryrun=bad)
    assert r["verdict"] == "BLOCKED", r
    assert any("blockedCode" in b or "COMPLETED" in b for b in r["blocked"]), r


def t8_blocked_scheduler_fail():
    r = _run(sched=SCHED_FAIL)
    assert r["verdict"] == "BLOCKED", r
    assert any("LastTaskResult" in b for b in r["blocked"]), r


def t9_blocked_missing_evidence():
    r = _run(evidence={"top10Count": 0, "top100Count": 0, "evMethod": None, "rank1": None})
    assert r["verdict"] == "BLOCKED", r
    assert any("top10" in b or "top100" in b for b in r["blocked"]), r


def t10_blocked_live_500():
    r = _run(live=dict(LIVE_OK, performanceHttp=500))
    assert r["verdict"] == "BLOCKED", r
    assert any("performance HTTP 500" in b for b in r["blocked"]), r


def t11_already_applied_pass():
    # 이미 seq13 반영(canonical=13, dry-run=13) + live도 13(publish 완료) → clean PASS
    r = _run(canonical={"officialSequence": 13}, live=dict(LIVE_OK, deployedSequence=13))
    assert r["verdict"] == "PASS", r
    assert r["closeoutState"] == "ALREADY_APPLIED", r


def t12_buycount_not_ten_blocked():
    r = _run(dryrun=dict(GOOD_DR, buyCount=9))
    assert r["verdict"] == "BLOCKED", r
    assert any("buyCount" in b for b in r["blocked"]), r


def t13_txt_first_line_and_request():
    o = {"verdict": "PASS", "hint": "seq=13 closeout 가능", "closeoutState": "NEEDED",
         "canonicalSequence": 12, "expectedProposedSequence": 13, "actualProposedSequence": 13,
         "batchId": "MF-BATCH-2026-07-09", "signalAsOfDate": "2026-07-08",
         "blocked": [], "actionWarns": [], "knownWarns": ["REPO2 financial-universe-real.json M"],
         "date": TRADING, "createdAt": "2026-07-09T16:10:00+09:00",
         "canonical": {"officialSequence": 12, "officialAvailableCash": 38000000},
         "dryRun": GOOD_DR, "evidence": EVID, "scheduler": SCHED_OK, "live": LIVE_OK,
         "repo1": CLEAN, "repo2": KNOWN2}
    txt = OB.to_txt(o)
    assert txt.splitlines()[0] == "전체 판정: PASS", txt.splitlines()[0]
    assert "MF-SEQ13-ONE-SHOT-CLOSEOUT" in txt, "closeout phase 누락"
    assert "[자동화 채팅창에 붙일 요청]" in txt, "자동화 요청 섹션 누락"


def t14_next_task_blocked_phase():
    r = _run(sched=SCHED_FAIL)
    o = dict(r, date=TRADING)
    line = OB.next_task_line(o)
    assert "BLOCKER-FIX" in line, line


TESTS = [
    ("1  비거래일 WAIT", t1_non_trading_wait),
    ("2  장마감 전 WAIT", t2_before_close_wait),
    ("3  status 스케줄러 전 WAIT", t3_pre_scheduler_wait),
    ("4  정상 NEEDED PASS", t4_pass_needed_clean),
    ("5  구조적 known-warn만 → PASS", t5_pass_with_expected_known_warn),
    ("6  REPO1 public 미커밋 → WARNING", t6_warning_repo1_public_dirty),
    ("7  dry-run 미완료 → BLOCKED", t7_blocked_dryrun_not_completed),
    ("8  스케줄러 실패 → BLOCKED", t8_blocked_scheduler_fail),
    ("9  evidence 누락 → BLOCKED", t9_blocked_missing_evidence),
    ("10 live 500 → BLOCKED", t10_blocked_live_500),
    ("11 이미 반영 → ALREADY_APPLIED PASS", t11_already_applied_pass),
    ("12 buyCount!=10 → BLOCKED", t12_buycount_not_ten_blocked),
    ("13 txt 첫 줄 판정·요청 섹션", t13_txt_first_line_and_request),
    ("14 BLOCKED 다음작업 phase", t14_next_task_blocked_phase),
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
