#!/usr/bin/env python3
"""Auto Publish 선행 gate — 거래 당일 17:00 홈페이지 반영 전 필수 검사 (read-only, write 0).

WABABA-AUTO-PUBLISH-SAME-DAY-SCHEDULE-ALIGNMENT.
Auto Publish 는 "평일 17:00" 이라는 달력 조건만으로 publish 하지 않는다. 이 스크립트가
한국 증시 실제 거래일 여부 + 당일 Auto Apply 선행 완료를 확인해 PROCEED/SKIP/BLOCKED 를
결정하고, PowerShell wrapper 는 그 결정에만 따른다.

검사 항목(§7):
  1) 당일이 실제 거래일(KRX 휴장표 반영, 평일 추정만으로 판단하지 않음)
  2) 당일 Auto Apply 실행 기록 존재
  3) Auto Apply 전체 판정이 PASS(APPLIED_AUTOMATICALLY 또는 정상 no-action)
  4) canonical 최신 executionDate 가 publish 대상 실제 거래일과 일치
  5) receipt·SHA 체인·sequence 검증 PASS(canonical 무결성 검증 = public 변환 gate 재사용)
  6) 미반영 거래일 0(운영계약상 정상)
  7) public 변환 gate PASS(build_magic_official_public 검증 통과)
  8) 실주문 수 0
  9) 브로커 호출 수 0
추가) Auto Apply 프로세스가 아직 lock 을 들고 있으면 동시 publish 금지(§8 지연 안전장치)

어떤 파일도 쓰지 않는다. 결정만 stdout JSON 으로 낸다.
exit 0 = PROCEED, 10 = SKIP(정상 self-skip), 2 = BLOCKED(선행 미완료 — non-success)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import magic_daily_common as C  # noqa: E402
import magic_daily_auto_apply as A  # noqa: E402

CANONICAL_PATH = ROOT / "magic-formula-official-state.json"
APPLY_STATUS_JSON = ROOT / "reports" / "magic-auto-apply-status-latest.json"

PROCEED = "PROCEED"
SKIP_NON_TRADING_DAY = "SKIPPED_NON_TRADING_DAY"

# Auto Apply 가 "정상 종료"로 인정되는 status (그 외는 선행 미완료로 본다)
APPLY_OK_STATUSES = {"APPLIED_AUTOMATICALLY", "NO_ACTION_ALREADY_CURRENT"}

EXIT_PROCEED, EXIT_SKIP, EXIT_BLOCKED = 0, 10, 2

# 격리 모듈 자체를 못 읽을 때 쓸 코드(fail-closed 기본값)
AQ_FALLBACK_CODE = "BLOCKED_AUDIT_QUARANTINE_ACTIVE"


def _latest_completed_date(canonical: dict):
    days = [d for d in (canonical.get("dailyLedger") or []) if d.get("runStatus") == "COMPLETED"]
    if not days:
        return None
    return str(sorted(days, key=lambda d: str(d.get("date")))[-1].get("date"))


def _result(decision: str, *, verdict: str, reason: str, today: str,
            checks: dict | None = None, **extra) -> dict:
    return {
        "stage": "PUBLISH_GATE",
        "decision": decision,
        "verdict": verdict,
        "today": today,
        "reason": reason,
        "checks": checks or {},
        # publish 단계는 절대 주문/브로커를 만들지 않는다(고정 0).
        "realOrderCount": 0,
        "brokerApiCallCount": 0,
        "productionWriteCount": 0,
        "filesWritten": 0,
        **extra,
    }


def evaluate(*, today_iso: str, canonical: dict | None, apply_status: dict | None,
             lock_held: bool, public_model_error: str | None,
             quality_status: dict | None = None) -> dict:
    """순수 판정(파일 접근 없음 — 호출자가 읽어서 넘긴다). 테스트에서 그대로 재사용."""
    checks: dict = {}

    # 0) 감사 격리 — 최우선. 무결성 감사 중에는 public 반영도 하지 않는다.
    try:
        import audit_quarantine as AQ
        aq_ok, aq_code, aq_detail = AQ.gate()
    except Exception as e:  # noqa: BLE001 — 격리 모듈 실패도 fail-closed
        aq_ok, aq_code, aq_detail = False, AQ_FALLBACK_CODE, f"격리 게이트 평가 실패: {e}"
    checks["auditQuarantineClear"] = aq_ok
    if not aq_ok:
        return _result(aq_code, verdict="BLOCKED", today=today_iso,
                       reason=aq_detail, checks=checks,
                       founderAction="무결성 감사 완료 후 해제(자동 해제 없음)")

    # 1) 실제 거래일 — 주말/공휴일/임시휴장은 여기서 정상 self-skip 된다.
    is_td = C.is_krx_trading_day(today_iso)
    checks["actualTradingDay"] = is_td
    if not is_td:
        return _result(SKIP_NON_TRADING_DAY, verdict="PASS", today=today_iso,
                       reason=f"{today_iso} 은 한국 증시 실제 거래일이 아님 — publish self-skip",
                       checks=checks, publishTargetDate=None)

    # 추가) Auto Apply 가 아직 돌고 있으면 동시 publish 금지(기존 lock 재사용)
    checks["applyNotInProgress"] = not lock_held
    if lock_held:
        return _result("BLOCKED_APPLY_IN_PROGRESS", verdict="BLOCKED", today=today_iso,
                       reason="Auto Apply 가 아직 lock 보유 중 — 동시 publish 금지(다음 재시도에서 확인)",
                       checks=checks, founderAction="없음(재시도 대기). 반복되면 lock 보유 프로세스 확인")

    # 2) 당일 Auto Apply 실행 기록
    has_status = isinstance(apply_status, dict) and bool(apply_status)
    checks["applyStatusExists"] = has_status
    if not has_status:
        return _result("BLOCKED_APPLY_RECEIPT_MISSING", verdict="BLOCKED", today=today_iso,
                       reason="Auto Apply 상태 파일 없음 — 선행 완료를 확인할 수 없음",
                       checks=checks, founderAction="16:25 Wababa Magic Daily Auto Apply 실행 여부 확인")

    apply_date = str(apply_status.get("date") or "")
    apply_status_name = str(apply_status.get("status") or "")
    apply_verdict = str(apply_status.get("verdict") or "")
    checks["applyRanToday"] = (apply_date == today_iso)
    if apply_date != today_iso:
        return _result("BLOCKED_APPLY_NOT_TODAY", verdict="BLOCKED", today=today_iso,
                       reason=f"Auto Apply 기록이 당일({today_iso}) 것이 아님(기록 {apply_date or '없음'})",
                       checks=checks, applyDate=apply_date, applyStatus=apply_status_name,
                       founderAction="16:25 Auto Apply 실행 여부 확인")

    # 당일이 거래일인데 apply 가 non-trading-day 로 self-skip 했다면 판정 불일치 → 막는다.
    checks["applyVerdictPass"] = (apply_verdict == "PASS" and apply_status_name in APPLY_OK_STATUSES)
    if not checks["applyVerdictPass"]:
        return _result("BLOCKED_APPLY_NOT_PASS", verdict="BLOCKED", today=today_iso,
                       reason=f"Auto Apply 정상 완료 아님(status={apply_status_name or '?'}, verdict={apply_verdict or '?'})",
                       checks=checks, applyStatus=apply_status_name, applyVerdict=apply_verdict,
                       founderAction=str(apply_status.get("founderAction") or "Auto Apply 실패 원인 확인"))

    # 8·9) 실주문/브로커 호출 0 (apply 기록 기준)
    checks["applyRealOrderZero"] = (int(apply_status.get("realOrderCount") or 0) == 0)
    checks["applyBrokerCallZero"] = (int(apply_status.get("brokerApiCallCount") or 0) == 0)
    if not (checks["applyRealOrderZero"] and checks["applyBrokerCallZero"]):
        return _result("BLOCKED_REAL_ORDER_PATH_DETECTED", verdict="BLOCKED", today=today_iso,
                       reason="Auto Apply 기록에 실주문/브로커 호출이 0 이 아님 — publish 중단",
                       checks=checks, founderAction="즉시 실주문 경로 점검")

    # canonical 필요
    checks["canonicalReadable"] = isinstance(canonical, dict) and bool(canonical)
    if not checks["canonicalReadable"]:
        return _result("BLOCKED_CANONICAL_UNREADABLE", verdict="BLOCKED", today=today_iso,
                       reason="canonical 장부를 읽을 수 없음", checks=checks,
                       founderAction="canonical 파일 확인")

    # 6) 미반영 거래일 0
    pending = A.unapplied_execution_dates(canonical, today_iso)
    checks["noUnappliedTradingDays"] = (len(pending) == 0)
    if pending:
        return _result("BLOCKED_CANONICAL_LEDGER_BEHIND", verdict="BLOCKED", today=today_iso,
                       reason=f"미반영 거래일 {len(pending)}건({', '.join(pending)}) — canonical 이 최신이 아님",
                       checks=checks, unappliedDates=pending,
                       founderAction="미반영 거래일 반영 후 재실행")

    # 4) canonical 최신 executionDate == publish 대상 실제 거래일(= 당일)
    latest = _latest_completed_date(canonical)
    checks["canonicalLatestIsToday"] = (latest == today_iso)
    if latest != today_iso:
        return _result("BLOCKED_CANONICAL_DATE_MISMATCH", verdict="BLOCKED", today=today_iso,
                       reason=f"canonical 최신 executionDate({latest}) != publish 대상 거래일({today_iso})",
                       checks=checks, canonicalLatestDate=latest,
                       founderAction="canonical 최신 상태 확인")

    # 5·7) public 변환 gate = canonical 무결성 검증(sequence·SHA 체인·lot/ledger 정합) 통과
    checks["publicModelGate"] = (public_model_error is None)
    if public_model_error is not None:
        return _result("BLOCKED_PUBLIC_MODEL_INVALID", verdict="BLOCKED", today=today_iso,
                       reason=f"public 변환 gate 실패: {public_model_error}", checks=checks,
                       founderAction="canonical 무결성 원인 확인")

    # KRX 수집 품질 gate — 그 거래일 데이터가 INVALID 면 홈페이지 반영도 막는다(계약 A).
    #   quality_status 는 호출자가 읽어서 넘긴다(순수 판정 유지). 증거 없으면 PASS 로 보지 않는다.
    import krx_data_quality as Q
    q_ok, q_code, q_detail = Q.evaluate(quality_status, today_iso)
    checks["krxDataQualityPass"] = q_ok
    if not q_ok:
        return _result(f"BLOCKED_{q_code}", verdict="BLOCKED", today=today_iso,
                       reason=f"KRX 데이터 품질 미통과: {q_detail}", checks=checks,
                       founderAction="해당 거래일 KRX 수집 재검증 후 재실행")

    return _result(PROCEED, verdict="PASS", today=today_iso,
                   reason=f"당일({today_iso}) Auto Apply 선행 완료 확인 — public publish 진행",
                   checks=checks, publishTargetDate=today_iso,
                   canonicalLatestDate=latest, applyStatus=apply_status_name,
                   officialSequence=canonical.get("officialSequence"),
                   officialTradingDayIndex=canonical.get("officialTradingDayIndex"))


def run(*, today_iso: str | None = None, canonical_path: Path = CANONICAL_PATH,
        apply_status_path: Path = APPLY_STATUS_JSON, lock_path: Path | None = None) -> dict:
    today_iso = today_iso or C.today_kst_iso()
    lock_path = lock_path or A.LOCK_PATH

    canonical = None
    try:
        canonical = json.loads(Path(canonical_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        canonical = None

    apply_status = None
    try:
        # write_durable_status 는 BOM 없이 쓰지만 PowerShell 경유 파일이 있을 수 있어 utf-8-sig 로 읽는다.
        apply_status = json.loads(Path(apply_status_path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        apply_status = None

    # public 변환 gate: canonical 전체 무결성 검증(read-only, write 0)
    public_model_error = None
    if canonical is not None:
        try:
            import build_magic_official_public as P
            P.build_magic_official_public(state_path=canonical_path)
        except Exception as e:  # noqa: BLE001
            public_model_error = f"{type(e).__name__}: {e}"

    # 그 거래일 KRX 수집 품질 증거(없으면 None → gate 가 fail-closed 판정)
    import krx_data_quality as Q
    quality_status = Q.read_status(today_iso)

    return evaluate(today_iso=today_iso, canonical=canonical, apply_status=apply_status,
                    lock_held=Path(lock_path).exists(), public_model_error=public_model_error,
                    quality_status=quality_status)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Auto Publish 선행 gate (read-only, write 0)")
    ap.add_argument("--today", default=None, help="판정 기준일(YYYY-MM-DD). 기본 오늘(KST)")
    ap.add_argument("--canonical-path", default=str(CANONICAL_PATH))
    ap.add_argument("--apply-status-path", default=str(APPLY_STATUS_JSON))
    args = ap.parse_args(argv)

    r = run(today_iso=args.today, canonical_path=Path(args.canonical_path),
            apply_status_path=Path(args.apply_status_path))
    print(json.dumps(r, ensure_ascii=False), flush=True)
    if r["decision"] == PROCEED:
        return EXIT_PROCEED
    if r["decision"] == SKIP_NON_TRADING_DAY:
        return EXIT_SKIP
    return EXIT_BLOCKED


if __name__ == "__main__":
    sys.exit(main())
