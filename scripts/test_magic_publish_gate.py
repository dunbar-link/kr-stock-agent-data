#!/usr/bin/env python3
"""magic_publish_gate 테스트 — 거래 당일 17:00 Auto Publish 선행 gate.

WABABA-AUTO-PUBLISH-SAME-DAY-SCHEDULE-ALIGNMENT §13 시간 계약 fixture.
evaluate() 는 순수 함수라 파일 접근 0. 실제 canonical·상태파일을 건드리지 않는다(write 0).
"""
from __future__ import annotations

import json
from pathlib import Path

import magic_publish_gate as G

# 감사 격리 marker 는 *운영 상태*다. 테스트가 그걸 읽으면 격리 활성 시 무관하게 실패한다.
# 테스트 동안만 marker 경로를 임시 위치로 돌려 격리 없음 상태로 격리한다(운영 marker 미접촉).
import tempfile as _tf, audit_quarantine as _AQ
_AQ.MARKER_PATH = __import__('pathlib').Path(_tf.mkdtemp()) / 'audit-quarantine.json'



ROOT = Path(__file__).resolve().parents[1]

# 실제 거래일 (2026-07-24 금, 07-27 월). 07-25 토 / 07-26 일.
FRI = "2026-07-24"
MON = "2026-07-27"
SAT = "2026-07-25"
SUN = "2026-07-26"


def canonical(*, latest: str, cal_last: str | None = None, seq: int = 17, tdi: int = 19) -> dict:
    """gate 가 보는 최소 canonical 형태(dailyLedger COMPLETED + officialExecutionCalendar)."""
    return {
        "officialSequence": seq,
        "officialTradingDayIndex": tdi,
        "officialExecutionCalendar": [cal_last or latest],
        "missedRuns": [],
        "dailyLedger": [{"date": latest, "runStatus": "COMPLETED"}],
    }


def apply_status(*, date: str, status: str = "APPLIED_AUTOMATICALLY", verdict: str = "PASS",
                 real: int = 0, broker: int = 0) -> dict:
    return {"date": date, "status": status, "verdict": verdict,
            "realOrderCount": real, "brokerApiCallCount": broker, "founderAction": "없음"}


def quality_ok(date_iso: str) -> dict:
    """그 거래일 KRX 수집 품질 PASS 증거(WABABA-KRX-FUNDAMENTAL-RECOVERY-R1)."""
    import krx_data_quality as Q
    return Q.build_status(date_iso=date_iso, verdict="PASS",
                          markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}, universe_count=2700)


def ev(today, canon, appl, *, lock=False, model_err=None, quality="ok"):
    """quality: 'ok' = PASS 증거 주입 / None = 증거 없음 / dict = 그대로 주입."""
    qs = quality_ok(today) if quality == "ok" else quality
    return G.evaluate(today_iso=today, canonical=canon, apply_status=appl,
                      lock_held=lock, public_model_error=model_err, quality_status=qs)


# ── 1) 실제 거래일 + 당일 apply 완료 → PROCEED ───────────────────────────────
def t01_monday_apply_then_publish_same_day():
    r = ev(MON, canonical(latest=MON), apply_status(date=MON))
    assert r["decision"] == G.PROCEED, r
    assert r["verdict"] == "PASS"
    assert r["publishTargetDate"] == MON
    assert all(r["checks"].values()), r["checks"]


def t02_friday_apply_then_publish_same_day():
    r = ev(FRI, canonical(latest=FRI), apply_status(date=FRI))
    assert r["decision"] == G.PROCEED, r
    assert r["publishTargetDate"] == FRI


def t03_no_action_already_current_is_ok():
    r = ev(MON, canonical(latest=MON), apply_status(date=MON, status="NO_ACTION_ALREADY_CURRENT"))
    assert r["decision"] == G.PROCEED, r


# ── 2) 주말·휴장일 self-skip ─────────────────────────────────────────────────
def t04_saturday_self_skip():
    r = ev(SAT, canonical(latest=FRI), apply_status(date=FRI))
    assert r["decision"] == G.SKIP_NON_TRADING_DAY, r
    assert r["verdict"] == "PASS"          # self-skip 은 정상(실패 아님)
    assert r["publishTargetDate"] is None


def t05_sunday_self_skip():
    r = ev(SUN, canonical(latest=FRI), apply_status(date=FRI))
    assert r["decision"] == G.SKIP_NON_TRADING_DAY, r
    assert r["verdict"] == "PASS"


def t06_krx_holiday_self_skip():
    """KRX 휴장표에 있는 날은 평일이어도 self-skip(달력 평일 판정만으로 publish 금지)."""
    import magic_daily_common as C
    hol = sorted(C._krx_holidays())
    weekday_hols = [h for h in hol if __import__("datetime").date.fromisoformat(h).weekday() < 5]
    assert weekday_hols, "휴장표에 평일 휴장일이 있어야 이 테스트가 의미 있음"
    h = weekday_hols[-1]
    r = ev(h, canonical(latest=FRI), apply_status(date=FRI))
    assert r["decision"] == G.SKIP_NON_TRADING_DAY, (h, r)
    assert r["checks"]["actualTradingDay"] is False


def t07_holiday_skip_does_not_touch_ledger():
    r = ev(SUN, canonical(latest=FRI), apply_status(date=FRI))
    # self-skip 은 sequence 증가·가짜 거래를 만들지 않는다
    assert r["realOrderCount"] == 0 and r["brokerApiCallCount"] == 0
    assert r["productionWriteCount"] == 0 and r["filesWritten"] == 0


# ── 3) Apply 선행 미완료 → BLOCKED (publish 0) ───────────────────────────────
def t08_apply_status_missing_blocked():
    r = ev(MON, canonical(latest=MON), None)
    assert r["decision"] == "BLOCKED_APPLY_RECEIPT_MISSING", r
    assert r["verdict"] == "BLOCKED"


def t09_apply_not_today_blocked():
    """월요일인데 apply 기록이 금요일 것 → 당일 선행 미완료."""
    r = ev(MON, canonical(latest=FRI, cal_last=FRI), apply_status(date=FRI))
    assert r["decision"] == "BLOCKED_APPLY_NOT_TODAY", r
    assert r["verdict"] == "BLOCKED"


def t10_apply_blocked_verdict_blocked():
    r = ev(MON, canonical(latest=MON),
           apply_status(date=MON, status="BLOCKED_GATE_FAILED", verdict="BLOCKED"))
    assert r["decision"] == "BLOCKED_APPLY_NOT_PASS", r


def t11_apply_skipped_non_trading_on_trading_day_blocked():
    """거래일인데 apply 가 non-trading-day 로 self-skip 했다면 판정 불일치 → 막는다."""
    r = ev(MON, canonical(latest=MON),
           apply_status(date=MON, status="SKIPPED_NON_TRADING_DAY", verdict="PASS"))
    assert r["decision"] == "BLOCKED_APPLY_NOT_PASS", r


def t12_apply_in_progress_blocked():
    """17:00 시점에 Auto Apply 가 아직 lock 보유 중이면 동시 publish 금지(§8)."""
    r = ev(MON, canonical(latest=MON), apply_status(date=MON), lock=True)
    assert r["decision"] == "BLOCKED_APPLY_IN_PROGRESS", r
    assert r["checks"]["applyNotInProgress"] is False


def t13_real_order_nonzero_blocked():
    r = ev(MON, canonical(latest=MON), apply_status(date=MON, real=1))
    assert r["decision"] == "BLOCKED_REAL_ORDER_PATH_DETECTED", r


def t14_broker_call_nonzero_blocked():
    r = ev(MON, canonical(latest=MON), apply_status(date=MON, broker=2))
    assert r["decision"] == "BLOCKED_REAL_ORDER_PATH_DETECTED", r


# ── 4) canonical 정합 ────────────────────────────────────────────────────────
def t15_unapplied_trading_days_blocked():
    """canonical 이 금요일까지인데 오늘 월요일이면 월요일이 미반영 → BLOCKED."""
    canon = canonical(latest=FRI, cal_last=FRI)
    r = ev(MON, canon, apply_status(date=MON))
    assert r["decision"] == "BLOCKED_CANONICAL_LEDGER_BEHIND", r
    assert MON in r["unappliedDates"], r


def t16_canonical_latest_not_today_blocked():
    """미반영 목록은 비었는데(missedRuns 처리) canonical 최신이 당일이 아니면 불일치."""
    canon = canonical(latest=FRI, cal_last=FRI)
    canon["missedRuns"] = [{"date": MON}]     # 월요일은 missed 처리 → unapplied 에서 제외
    r = ev(MON, canon, apply_status(date=MON))
    assert r["decision"] == "BLOCKED_CANONICAL_DATE_MISMATCH", r
    assert r["canonicalLatestDate"] == FRI


def t17_canonical_unreadable_blocked():
    r = ev(MON, None, apply_status(date=MON))
    assert r["decision"] == "BLOCKED_CANONICAL_UNREADABLE", r


def t18c_krx_quality_evidence_missing_blocked():
    """KRX 수집 품질 증거가 없으면 publish 금지(fail-closed, 계약 A)."""
    r = ev(MON, canonical(latest=MON), apply_status(date=MON), quality=None)
    assert r["decision"] == "BLOCKED_KRX_QUALITY_EVIDENCE_MISSING", r
    assert r["checks"]["krxDataQualityPass"] is False


def t18d_krx_quality_invalid_blocked():
    """펀더멘털 INVALID 거래일은 홈페이지 반영도 막는다."""
    import krx_data_quality as Q
    bad = Q.build_status(date_iso=MON, verdict="INVALID",
                         markets={"KOSPI": "INVALID", "KOSDAQ": "PASS"},
                         reason="필수 columns 누락")
    r = ev(MON, canonical(latest=MON), apply_status(date=MON), quality=bad)
    assert r["decision"] == "BLOCKED_KRX_DATA_QUALITY_INVALID", r
    assert r["verdict"] == "BLOCKED"


def t18e_krx_quality_single_market_fail_blocked():
    """KOSPI·KOSDAQ 둘 다 PASS 여야 공식 산출물 진행(계약 B)."""
    import krx_data_quality as Q
    half = Q.build_status(date_iso=MON, verdict="PASS",
                          markets={"KOSPI": "PASS", "KOSDAQ": "INVALID"})
    r = ev(MON, canonical(latest=MON), apply_status(date=MON), quality=half)
    assert r["decision"] == "BLOCKED_KRX_MARKET_NOT_PASS", r


def t18_public_model_invalid_blocked():
    r = ev(MON, canonical(latest=MON), apply_status(date=MON),
           model_err="MappingValidationError: seq 불연속")
    assert r["decision"] == "BLOCKED_PUBLIC_MODEL_INVALID", r
    assert r["checks"]["publicModelGate"] is False


# ── 5) 불변식 ────────────────────────────────────────────────────────────────
def t19_all_decisions_report_zero_orders():
    cases = [
        ev(MON, canonical(latest=MON), apply_status(date=MON)),
        ev(SUN, canonical(latest=FRI), apply_status(date=FRI)),
        ev(MON, canonical(latest=MON), None),
        ev(MON, canonical(latest=MON), apply_status(date=MON), lock=True),
    ]
    for r in cases:
        assert r["realOrderCount"] == 0, r
        assert r["brokerApiCallCount"] == 0, r
        assert r["productionWriteCount"] == 0, r
        assert r["filesWritten"] == 0, r


def t20_blocked_is_not_disguised_as_success():
    """선행 미완료는 NO_CHANGE/PASS 로 위장되지 않는다 — 구분 가능한 BLOCKED_* + verdict BLOCKED."""
    for r in [ev(MON, canonical(latest=MON), None),
              ev(MON, canonical(latest=MON), apply_status(date=FRI)),
              ev(MON, canonical(latest=MON), apply_status(date=MON, status="X", verdict="BLOCKED"))]:
        assert r["verdict"] == "BLOCKED", r
        assert r["decision"].startswith("BLOCKED_"), r
        assert r["decision"] != "NO_CHANGE"
        assert r["decision"] != G.SKIP_NON_TRADING_DAY


def t21_founder_action_present_on_blocked():
    r = ev(MON, canonical(latest=MON), None)
    assert str(r.get("founderAction") or "").strip(), r


def t22_gate_script_writes_nothing():
    """실제 실행 경로도 write 0 인지 — canonical/상태파일 mtime·sha 불변 확인."""
    import hashlib
    canon_p = ROOT / "magic-formula-official-state.json"
    status_p = ROOT / "reports" / "magic-auto-apply-status-latest.json"
    before = {}
    for p in (canon_p, status_p):
        if p.exists():
            before[p] = (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
    r = G.run()
    assert r["filesWritten"] == 0 and r["productionWriteCount"] == 0, r
    for p, (mt, sha) in before.items():
        assert p.stat().st_mtime_ns == mt, f"{p.name} mtime 변경됨"
        assert hashlib.sha256(p.read_bytes()).hexdigest() == sha, f"{p.name} 내용 변경됨"


def t23_run_today_matches_actual_calendar():
    """실제 실행: 오늘이 거래일이 아니면 SKIP, 거래일이면 PROCEED 또는 BLOCKED_* 중 하나."""
    import magic_daily_common as C
    r = G.run()
    today = C.today_kst_iso()
    if not C.is_krx_trading_day(today):
        assert r["decision"] == G.SKIP_NON_TRADING_DAY, r
    else:
        assert r["decision"] == G.PROCEED or r["decision"].startswith("BLOCKED_"), r


TESTS = [
    ("01 월 거래일 + 당일 apply → PROCEED(같은 날 publish)", t01_monday_apply_then_publish_same_day),
    ("02 금 거래일 + 당일 apply → PROCEED(금요일 저녁 publish)", t02_friday_apply_then_publish_same_day),
    ("03 NO_ACTION_ALREADY_CURRENT 도 정상 선행", t03_no_action_already_current_is_ok),
    ("04 토요일 → self-skip(PASS)", t04_saturday_self_skip),
    ("05 일요일 → self-skip(PASS)", t05_sunday_self_skip),
    ("06 KRX 휴장일(평일) → self-skip", t06_krx_holiday_self_skip),
    ("07 self-skip 은 장부·주문 0", t07_holiday_skip_does_not_touch_ledger),
    ("08 apply 상태파일 없음 → BLOCKED", t08_apply_status_missing_blocked),
    ("09 apply 기록이 당일 아님 → BLOCKED", t09_apply_not_today_blocked),
    ("10 apply BLOCKED → publish BLOCKED", t10_apply_blocked_verdict_blocked),
    ("11 거래일인데 apply self-skip → BLOCKED(불일치)", t11_apply_skipped_non_trading_on_trading_day_blocked),
    ("12 apply lock 보유 중 → 동시 publish 금지", t12_apply_in_progress_blocked),
    ("13 apply realOrderCount != 0 → BLOCKED", t13_real_order_nonzero_blocked),
    ("14 apply brokerApiCallCount != 0 → BLOCKED", t14_broker_call_nonzero_blocked),
    ("15 미반영 거래일 존재 → BLOCKED", t15_unapplied_trading_days_blocked),
    ("16 canonical 최신 != 당일 → BLOCKED", t16_canonical_latest_not_today_blocked),
    ("17 canonical 판독 불가 → BLOCKED", t17_canonical_unreadable_blocked),
    ("18 public 변환 gate 실패 → BLOCKED", t18_public_model_invalid_blocked),
    ("18c KRX 품질 증거 없음 → BLOCKED", t18c_krx_quality_evidence_missing_blocked),
    ("18d KRX 품질 INVALID → BLOCKED", t18d_krx_quality_invalid_blocked),
    ("18e 한 시장만 PASS → BLOCKED", t18e_krx_quality_single_market_fail_blocked),
    ("19 모든 판정에서 실주문·브로커·write 0", t19_all_decisions_report_zero_orders),
    ("20 선행 미완료를 성공으로 위장하지 않음", t20_blocked_is_not_disguised_as_success),
    ("21 BLOCKED 에 Founder 행동 1개 존재", t21_founder_action_present_on_blocked),
    ("22 실제 실행 경로도 write 0(canonical·상태파일 불변)", t22_gate_script_writes_nothing),
    ("23 실제 실행 판정이 실제 캘린더와 일치", t23_run_today_matches_actual_calendar),
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
