#!/usr/bin/env python3
"""magic_daily_status.classify 테스트 (Phase 45-AUTO2). now_dt 주입(시간 결정성). read-only."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import magic_daily_status as ST

KST = timezone(timedelta(hours=9))
NON_TRADING = "2026-06-20"   # 토
TRADING = "2026-06-24"       # 수
BEFORE = datetime(2026, 6, 24, 14, 0, tzinfo=KST)
AFTER = datetime(2026, 6, 24, 16, 0, tzinfo=KST)
CANON = {"officialSequence": 3, "officialTradingDayIndex": 5}


def t1_non_trading_no_action():
    o, a, _ = ST.classify(NON_TRADING, CANON, None, None, now_dt=AFTER)
    assert (o, a) == ("WAIT", "NO_ACTION"), (o, a)


def t2_before_close_wait():
    o, a, _ = ST.classify(TRADING, CANON, None, None, now_dt=BEFORE)
    assert (o, a) == ("WAIT", "WAIT_MARKET_CLOSE"), (o, a)


def t3_signal_ready_dry_run_ready():
    sig = {"data": {"status": "READY", "signalAsOfDate": TRADING, "nextExecutionDateCandidate": "2026-06-25"}}
    o, a, _ = ST.classify(TRADING, CANON, sig, None, now_dt=AFTER)
    assert (o, a) == ("PASS", "DRY_RUN_READY"), (o, a)


def t4_dryrun_completed_approval_required():
    sig = {"data": {"status": "READY", "nextExecutionDateCandidate": "2026-06-25"}}
    dr = {"data": {"status": "COMPLETED", "proposedSequence": 4, "proposedBatchId": "MF-BATCH-2026-06-25", "buyCount": 10}}
    o, a, hint = ST.classify(TRADING, CANON, sig, dr, now_dt=AFTER)
    assert (o, a) == ("PENDING_APPROVAL", "APPROVAL_TICKET_REQUIRED"), (o, a)
    assert "approval" in hint.lower() or "승인" in hint


def t5_blocked_needs_review():
    dr = {"data": {"status": "BLOCKED", "phase": "DRY_RUN", "blockedCode": "BLOCKED_MISSING_OPEN_PRICE",
                   "reason": "TOP10 missing"}}
    o, a, _ = ST.classify(TRADING, CANON, None, dr, now_dt=AFTER)
    assert (o, a) == ("BLOCKED", "BLOCKED_NEEDS_REVIEW"), (o, a)


def t6_signal_missing_signal_ready():
    o, a, _ = ST.classify(TRADING, CANON, None, None, now_dt=AFTER)
    assert (o, a) == ("PASS", "SIGNAL_READY"), (o, a)


TESTS = [
    ("1  비거래일 NO_ACTION", t1_non_trading_no_action),
    ("2  장마감 전 WAIT_MARKET_CLOSE", t2_before_close_wait),
    ("3  signal READY → DRY_RUN_READY", t3_signal_ready_dry_run_ready),
    ("4  dry-run COMPLETED → APPROVAL_TICKET_REQUIRED", t4_dryrun_completed_approval_required),
    ("5  BLOCKED → BLOCKED_NEEDS_REVIEW", t5_blocked_needs_review),
    ("6  신호 없음 → SIGNAL_READY", t6_signal_missing_signal_ready),
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
