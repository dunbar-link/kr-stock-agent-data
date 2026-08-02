#!/usr/bin/env python3
"""마법공식 가상 장부 — 무인 자동 반영 runner (Phase MF-CANONICAL-UNATTENDED-AUTO-APPLY).

대장 정책 결정(2026-07-25): 검증이 *전부 PASS* 인 공식 거래일은 사람 승인 없이 가상 canonical
장부에 자동 반영한다. 이 정책은 **와바바 내부 가상 장부에만** 적용된다.

■ 영구 금지(이 파일과 호출 경로 전체)
  실주문 · 증권사/브로커 API · 주문객체 전송 · 계좌조회/인증 · 주문·체결번호 생성 ·
  SMTP 실제 발송 · public publish · deploy · 공식 산식 변경 · TTM 혼입 ·
  과거 가격 임의 대체 · canonical 직접 수기 편집 · 실패 게이트 우회.
  → 이 파일은 broker/order/SMTP 관련 import·호출을 일절 갖지 않는다(테스트로 정적 고정).

■ 안전 경로 재사용(우회 apply 함수 신규 작성 금지)
  magic_daily_signal → magic_daily_dry_run → magic_make_approval_ticket →
  magic_apply_from_approval(verify/build-receipt/dry-run-apply/apply) → apply_magic_official_day
  실제 장부 write 는 기존 apply_official_append 만 사용한다(원자적 write + snapshot witness).

■ 사람 승인 게이트 → 정책 승인 게이트
  apply 게이트가 요구하는 것은 status=APPROVED · approval.approved=true · approvalPhrase · confirmToken 이다.
  approvedBy 는 검증 대상이 아니므로(자유 필드) **사람 이름을 사칭하지 않고** 정책 주체를 명시한다.
  무결성 보장은 승인 필드가 아니라 SHA 체인(canonical/signal/rankings/universe/dryRunLog) +
  엔진 재계산 대조가 담당하므로, 사람 확인을 정책 확인으로 바꿔도 암호학적 보증은 그대로다.

■ 1회 실행 = 최대 1거래일
  여러 날 밀려도 가장 오래된 미반영 거래일 1건만 처리한다(순서 역전·일괄 적용 금지).
  나머지는 다음 scheduler 주기에서 처리한다.

예) python scripts/magic_daily_auto_apply.py            # 오늘(KST) 기준 1건 처리
    python scripts/magic_daily_auto_apply.py --dry-run  # 게이트까지만(장부 write 0)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import date as _date, timedelta
from pathlib import Path

import magic_daily_common as C

# ── 정책 식별(사람 사칭 금지) ───────────────────────────────────────────────────
AUTO_APPROVER = "WABABA_AUTO_POLICY_V1"
APPROVAL_MODE = "UNATTENDED_POLICY"
POLICY_VERSION = "MF_AUTO_APPLY_V1"
FOUNDER_POLICY_AUTHORIZED_AT = "2026-07-25"   # 대장이 무인 전환을 승인한 날

AUTO_ROOT = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-auto-approval"))
LOCK_PATH = AUTO_ROOT / ".auto-apply.lock"
BACKUP_DIR = AUTO_ROOT / "_canonical-backup"

# ── 라우팅(표시만; 실제 SMTP 발송 없음) ─────────────────────────────────────────
OPS_ARCHIVE_RECIPIENT = "bridge.ai.office@gmail.com"
FOUNDER_ALERT_RECIPIENT = "duria2002@gmail.com"

# ── 상태 코드 ───────────────────────────────────────────────────────────────────
APPLIED_AUTOMATICALLY = "APPLIED_AUTOMATICALLY"
NO_ACTION_ALREADY_CURRENT = "NO_ACTION_ALREADY_CURRENT"
SKIPPED_NON_TRADING_DAY = "SKIPPED_NON_TRADING_DAY"
SKIPPED_NOT_READY = "SKIPPED_NOT_READY"
BLOCKED_GATE_FAILED = "BLOCKED_GATE_FAILED"
BLOCKED_LOCK_BUSY = "BLOCKED_LOCK_BUSY"
BLOCKED_CANONICAL_MISMATCH = "BLOCKED_CANONICAL_MISMATCH"
DRY_RUN_ONLY = "DRY_RUN_ONLY"

# ── 강제 BLOCKED 사유 코드(Gate D) ──────────────────────────────────────────────
B_SIGNAL_MISSING = "SIGNAL_MISSING"
B_DRY_RUN_MISSING = "DRY_RUN_MISSING"
B_CANONICAL_BEFORE_MISMATCH = "CANONICAL_BEFORE_MISMATCH"
B_CANONICAL_SEQUENCE_GAP = "CANONICAL_SEQUENCE_GAP"
B_EXECUTION_DATE_GAP = "EXECUTION_DATE_GAP"
B_EXECUTION_DATE_OUT_OF_ORDER = "EXECUTION_DATE_OUT_OF_ORDER"
B_PRICE_MISSING = "PRICE_MISSING"
B_PRICE_SOURCE_MISMATCH = "PRICE_SOURCE_MISMATCH"
B_FALLBACK_PRICE_USED = "FALLBACK_PRICE_USED"
B_TOP10_COUNT_NOT_10 = "TOP10_COUNT_NOT_10"
B_DUPLICATE_STOCK_CODE = "DUPLICATE_STOCK_CODE"
B_DUPLICATE_BATCH = "DUPLICATE_BATCH"
B_NEGATIVE_CASH = "NEGATIVE_CASH"
B_INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
B_FORMULA_VERSION_MISMATCH = "FORMULA_VERSION_MISMATCH"
B_TTM_CONTAMINATION = "TTM_CONTAMINATION"
B_DRY_RUN_NOT_COMPLETED = "DRY_RUN_NOT_COMPLETED"
B_LOOKAHEAD_FAILED = "LOOKAHEAD_FAILED"
B_MATURITY_TDI_MISMATCH = "MATURITY_TDI_MISMATCH"
B_REAL_ORDER_PATH_DETECTED = "REAL_ORDER_PATH_DETECTED"
# WABABA-KRX-FUNDAMENTAL-RECOVERY-R1 — 그 거래일 KRX 수집 품질이 PASS 라는 증거가 없으면 apply 금지.
B_KRX_DATA_QUALITY = "KRX_DATA_QUALITY_NOT_PASS"
B_PUBLIC_WRITE_DETECTED = "PUBLIC_WRITE_DETECTED"

OFFICIAL_FORMULA_VERSION = "book-faithful-v1-2026-43B5"


def _sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha_file(p) -> str | None:
    try:
        return _sha_bytes(Path(p).read_bytes())
    except OSError:
        return None


def engine_rules() -> dict:
    import magic_rolling_engine as E
    return {"topN": E.TOP_N, "holdTradingDays": E.HOLD_TRADING_DAYS,
            "maxOpenBatches": E.MAX_OPEN_BATCHES, "initialBatchCapital": E.INITIAL_BATCH_CAPITAL,
            "priceSourceTrade": E.PRICE_SOURCE_TRADE}


# ── 프로세스 lock (fail-closed; stale 자동 삭제 금지 — C:\work\CLAUDE.md §3-1) ──
def acquire_lock(lock_path: Path = LOCK_PATH, now: str | None = None) -> dict:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        try:
            held = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            held = {}
        return {"acquired": False, "holder": held,
                "reason": "다른 auto-apply 프로세스가 lock 보유 중(또는 비정상 종료 잔존 lock). "
                          "잔존 lock 은 자동 삭제하지 않는다(사람 판정)."}
    lock_path.write_text(json.dumps(
        {"pid": os.getpid(), "acquiredAt": now or C.now_kst().isoformat(),
         "policyVersion": POLICY_VERSION}, ensure_ascii=False), encoding="utf-8")
    return {"acquired": True, "holder": None, "reason": ""}


def release_lock(lock_path: Path = LOCK_PATH) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except OSError:
        pass


# ── 거래일/미반영일 ─────────────────────────────────────────────────────────────
def previous_trading_day(date_iso: str, limit: int = 10) -> str | None:
    cur = _date.fromisoformat(date_iso) - timedelta(days=1)
    for _ in range(limit):
        if C.is_krx_trading_day(cur.isoformat()):
            return cur.isoformat()
        cur -= timedelta(days=1)
    return None


def unapplied_execution_dates(canonical: dict, today_iso: str) -> list:
    """canonical 마지막 반영일 이후 ~ 오늘(포함)까지의 미반영 거래일(missedRuns 제외), 오래된 순."""
    cal = canonical.get("officialExecutionCalendar") or []
    if not cal:
        return []
    missed = {str(m.get("date")) for m in (canonical.get("missedRuns") or [])}
    try:
        last = _date.fromisoformat(str(cal[-1])[:10])
        today = _date.fromisoformat(str(today_iso)[:10])
    except (TypeError, ValueError):
        return []
    out, cur = [], last + timedelta(days=1)
    while cur <= today:
        iso = cur.isoformat()
        if C.is_krx_trading_day(iso) and iso not in missed:
            out.append(iso)
        cur += timedelta(days=1)
    return out


# ── 게이트 (AUTO_APPROVAL_ELIGIBLE) ─────────────────────────────────────────────
def evaluate_auto_approval_gates(*, canonical: dict, target_exec_date: str, dry_run: dict | None,
                                 ranking: dict | None, rules: dict,
                                 quality_status: dict | None = None) -> dict:
    """전 항목 PASS 여야만 eligible=True. WARNING 개념 없음 — 하나라도 실패하면 자동 apply 금지."""
    blocked, checks = [], {}

    def chk(name, cond, code=None, detail=""):
        checks[name] = bool(cond)
        if not cond and code:
            blocked.append({"code": code, "detail": detail or name})
        return bool(cond)

    seq_before = int(canonical.get("officialSequence") or 0)
    tdi_before = int(canonical.get("officialTradingDayIndex") or 0)
    cash_before = float(canonical.get("officialAvailableCash") or 0)
    cal = canonical.get("officialExecutionCalendar") or []

    # KRX 수집 품질 게이트 — 펀더멘털/시세/시총이 INVALID 인 거래일은 여기서 끊는다(계약 A).
    #   증거가 아예 없으면 PASS 로 간주하지 않는다(fail-closed).
    #   이 gate 가 막으면 canonical write·sequence 증가·신규 lot·FIFO 매도가 모두 0 이 된다.
    #   quality_status 미지정이면 디스크에서 읽는다(운영 경로). 테스트는 fixture 를 주입한다.
    try:
        import krx_data_quality as _Q
        _qs = quality_status if quality_status is not None else _Q.read_status(target_exec_date)
        _q_ok, _q_code, _q_detail = _Q.evaluate(_qs, target_exec_date)
    except Exception as _e:  # noqa: BLE001 — 품질 모듈 자체 실패도 fail-closed
        _q_ok, _q_code, _q_detail = False, B_KRX_DATA_QUALITY, f"품질 게이트 평가 실패: {_e}"
    if not chk("krxDataQualityPass", _q_ok, B_KRX_DATA_QUALITY, _q_detail or _q_code):
        return {"eligible": False, "blockedCodes": blocked, "checks": checks}

    if ranking is None:
        chk("signalPackagePresent", False, B_SIGNAL_MISSING, f"{target_exec_date} 신호 패키지 없음")
        return {"eligible": False, "blockedCodes": blocked, "checks": checks}
    chk("signalPackagePresent", True)

    if dry_run is None:
        chk("dryRunPresent", False, B_DRY_RUN_MISSING, f"{target_exec_date} dry-run 산출물 없음")
        return {"eligible": False, "blockedCodes": blocked, "checks": checks}
    chk("dryRunPresent", True)

    # 실행일 정합
    chk("executionDateMatches", dry_run.get("executionDate") == target_exec_date,
        B_EXECUTION_DATE_GAP, f"dry-run executionDate={dry_run.get('executionDate')} != {target_exec_date}")
    if cal:
        chk("executionDateAfterLast", str(target_exec_date) > str(cal[-1]),
            B_EXECUTION_DATE_OUT_OF_ORDER, f"{target_exec_date} <= 장부 마지막 {cal[-1]}")
    prev_td = previous_trading_day(target_exec_date)
    chk("signalAsOfIsPrevTradingDay", dry_run.get("signalAsOfDate") == prev_td,
        B_EXECUTION_DATE_GAP, f"signalAsOf={dry_run.get('signalAsOfDate')} != 직전거래일 {prev_td}")

    # dry-run 상태
    chk("dryRunCompleted", dry_run.get("runStatus") == "COMPLETED" or dry_run.get("status") == "COMPLETED",
        B_DRY_RUN_NOT_COMPLETED, f"runStatus={dry_run.get('runStatus')}")
    chk("lookAheadValidationPassed", dry_run.get("lookAheadValidationPassed") is True,
        B_LOOKAHEAD_FAILED, "lookAheadValidationPassed != true")
    chk("readOnlyUnchanged", dry_run.get("readOnlyUnchanged") is True,
        B_DRY_RUN_NOT_COMPLETED, "readOnlyUnchanged != true")
    chk("dryRunProductionWriteZero", int(dry_run.get("productionWriteCount") or 0) == 0,
        B_PUBLIC_WRITE_DETECTED, "dry-run productionWriteCount != 0")

    # 시퀀스/인덱스 정합
    chk("sequenceIsNextOne", int(dry_run.get("proposedSequence") or 0) == seq_before + 1,
        B_CANONICAL_SEQUENCE_GAP,
        f"proposedSequence={dry_run.get('proposedSequence')} != {seq_before + 1}")
    chk("tradingDayIndexIsNextOne", int(dry_run.get("buyTradingDayIndex") or 0) == tdi_before + 1,
        B_CANONICAL_SEQUENCE_GAP,
        f"buyTradingDayIndex={dry_run.get('buyTradingDayIndex')} != {tdi_before + 1}")
    expected_maturity = tdi_before + 1 + int(rules["holdTradingDays"])
    chk("maturityTdiCorrect", int(dry_run.get("plannedSellTradingDayIndex") or 0) == expected_maturity,
        B_MATURITY_TDI_MISMATCH,
        f"plannedSellTradingDayIndex={dry_run.get('plannedSellTradingDayIndex')} != {expected_maturity}")

    # top10 / 가격
    top10 = ranking.get("top10") or []
    codes = [str(t.get("code")) for t in top10]
    chk("top10CountIs10", len(top10) == int(rules["topN"]),
        B_TOP10_COUNT_NOT_10, f"top10 {len(top10)}종")
    chk("noDuplicateStockCode", len(set(codes)) == len(codes),
        B_DUPLICATE_STOCK_CODE, "top10 내 종목코드 중복")
    opens = dry_run.get("openPrices") or {}
    missing = [c for c in codes if not opens.get(c) or float(opens.get(c) or 0) <= 0]
    chk("noMissingOpenPrice", not missing, B_PRICE_MISSING, f"시가 누락 {missing}")
    chk("noMissingEvalCodes", not (dry_run.get("missingEvalCodes") or []),
        B_PRICE_MISSING, f"missingEvalCodes={dry_run.get('missingEvalCodes')}")
    chk("priceSourceIsOpen", str(rules["priceSourceTrade"]) == "pykrx_open",
        B_PRICE_SOURCE_MISMATCH, "체결 가격원천이 pykrx_open 이 아님")
    # dry-run 계획의 amount = openPrice * quantity 재검산(임의 대체가격 탐지)
    bad_price = []
    for p in (dry_run.get("plan") or []):
        op, q, amt = p.get("openPrice"), p.get("quantity"), p.get("amount")
        if op is None or q is None or amt is None or abs(float(op) * int(q) - float(amt)) > 0.01:
            bad_price.append(p.get("code"))
        elif float(opens.get(str(p.get("code")), 0)) != float(op):
            bad_price.append(p.get("code"))
    chk("planPriceConsistent", not bad_price, B_FALLBACK_PRICE_USED,
        f"계획가/시가 불일치(대체가 의심) {bad_price}")

    # 공식 산식 / TTM
    fv = ranking.get("formulaVersion")
    chk("formulaVersionOfficial", fv == OFFICIAL_FORMULA_VERSION,
        B_FORMULA_VERSION_MISMATCH, f"formulaVersion={fv}")
    blob = json.dumps(ranking, ensure_ascii=False)
    chk("noTtmContamination", ("ttmExperiment" not in blob) and ("experimentalRank" not in blob),
        B_TTM_CONTAMINATION, "신호 패키지에 TTM 실험 필드 혼입")

    # 자금
    alloc = float(dry_run.get("allocatedCapital") or 0)
    chk("allocatedIsBatchBudget", abs(alloc - float(rules["initialBatchCapital"])) < 0.01
        or int(dry_run.get("proposedSequence") or 0) > int(rules["maxOpenBatches"]),
        B_INSUFFICIENT_CASH, f"allocatedCapital={alloc}")
    chk("sufficientCash", cash_before >= alloc, B_INSUFFICIENT_CASH,
        f"가용현금 {cash_before} < 필요 {alloc}")
    cash_after = dry_run.get("officialAvailableCashAfterPreview")
    chk("cashAfterNotNegative", cash_after is not None and float(cash_after) >= 0,
        B_NEGATIVE_CASH, f"예상 잔여현금 {cash_after}")

    # 중복 batch
    bid = dry_run.get("proposedBatchId")
    chk("noDuplicateBatch", not any(b.get("batchId") == bid for b in (canonical.get("batches") or [])),
        B_DUPLICATE_BATCH, f"batchId {bid} 이미 존재")

    # 실주문/공개 write
    chk("realOrderZero", int(dry_run.get("realOrderCount") or 0) == 0,
        B_REAL_ORDER_PATH_DETECTED, "dry-run realOrderCount != 0")

    return {"eligible": not blocked, "blockedCodes": blocked, "checks": checks}


# ── 자동 승인 필드 주입(사람 사칭 금지) ─────────────────────────────────────────
def apply_auto_approval(ticket: dict, gate_summary_sha: str, *, now: str) -> dict:
    """apply 게이트가 요구하는 승인 필드를 *정책 주체* 로 채운다. 사람 이름을 쓰지 않는다.
    approval 블록은 ticket 동일성 판정(_CORE_KEYS)에서 제외되므로 메타 확장이 안전하다."""
    t = json.loads(json.dumps(ticket, ensure_ascii=False))  # 깊은 복사(원본 불변)
    t["status"] = "APPROVED"
    ap = t.get("approval") or {}
    ap.update({
        "approved": True,
        "approvedBy": AUTO_APPROVER,
        "approvedAt": now,
        "approvalNotes": "무인 정책 자동승인 — 전체 강제 게이트 PASS. 사람 승인 아님.",
        "approvalMode": APPROVAL_MODE,
        "policyVersion": POLICY_VERSION,
        "approvalBasis": gate_summary_sha,
        "founderPolicyAuthorized": True,
        "founderPolicyAuthorizedAt": FOUNDER_POLICY_AUTHORIZED_AT,
        "realOrderAuthorized": False,
    })
    t["approval"] = ap
    return t


def routing_for(verdict: str) -> dict:
    if verdict == "PASS":
        return {"to": [OPS_ARCHIVE_RECIPIENT], "purpose": "ARCHIVE",
                "founderNotified": False, "founderRecipientCount": 0}
    return {"to": [FOUNDER_ALERT_RECIPIENT], "purpose": "EXCEPTION_ALERT",
            "founderNotified": True, "founderRecipientCount": 1}


def _base_result(status: str, *, verdict: str, now: str, **extra) -> dict:
    r = {"status": status, "phase": "AUTO_APPLY", "verdict": verdict,
         "policyVersion": POLICY_VERSION, "approvedBy": AUTO_APPROVER,
         "realOrderCount": 0, "brokerApiCallCount": 0, "smtpCallCount": 0,
         "emailSent": False, "publicCopyCount": 0, "deployCount": 0,
         "createdAt": now}
    r.update(extra)
    r["routing"] = routing_for(verdict)
    r["founderNotified"] = r["routing"]["founderNotified"]
    r["founderRecipientCount"] = r["routing"]["founderRecipientCount"]
    return r


# ── 메인 오케스트레이션 ─────────────────────────────────────────────────────────
def run_auto_apply(*, today_iso: str | None = None, canonical_path: Path | None = None,
                   snapshot_dir: Path | None = None, auto_root: Path | None = None,
                   lock_path: Path | None = None, do_apply: bool = True,
                   rederive_fn=None, now: str | None = None) -> dict:
    """1회 실행 = 최대 1거래일. 전 게이트 PASS 일 때만 기존 안전 apply 경로로 장부에 append."""
    import apply_magic_official_day as A

    now = now or C.now_kst().isoformat()
    today_iso = today_iso or C.today_kst_iso()
    canonical_path = Path(canonical_path or C.CANONICAL_PATH)
    snapshot_dir = Path(snapshot_dir or A.DEFAULT_SNAPSHOT_DIR)
    auto_root = Path(auto_root or AUTO_ROOT)
    lock_path = Path(lock_path or (auto_root / ".auto-apply.lock"))
    rules = engine_rules()

    if not C.is_krx_trading_day(today_iso):
        return _base_result(SKIPPED_NON_TRADING_DAY, verdict="PASS", now=now, date=today_iso,
                            reason=f"{today_iso} 비거래일 — 자동 반영 대상 없음", canonicalChanged=False)

    lock = acquire_lock(lock_path, now=now)
    if not lock["acquired"]:
        return _base_result(BLOCKED_LOCK_BUSY, verdict="BLOCKED", now=now, date=today_iso,
                            reason=lock["reason"], lockHolder=lock["holder"], canonicalChanged=False,
                            founderAction="잔존 auto-apply lock 확인 후 사람이 정리")
    try:
        try:
            canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            return _base_result(BLOCKED_CANONICAL_MISMATCH, verdict="BLOCKED", now=now, date=today_iso,
                                reason=f"canonical 읽기 실패: {type(e).__name__}", canonicalChanged=False,
                                founderAction="canonical 장부 파일 상태 확인")

        canonical_sha_before = _sha_file(canonical_path)
        seq_before = int(canonical.get("officialSequence") or 0)
        tdi_before = int(canonical.get("officialTradingDayIndex") or 0)
        cash_before = float(canonical.get("officialAvailableCash") or 0)
        lots_before = len(canonical.get("itemLots") or [])

        pending = unapplied_execution_dates(canonical, today_iso)
        if not pending:
            return _base_result(NO_ACTION_ALREADY_CURRENT, verdict="PASS", now=now, date=today_iso,
                                reason="미반영 거래일 없음 — 장부가 최신", canonicalChanged=False,
                                officialSequence=seq_before, officialTradingDayIndex=tdi_before,
                                officialAvailableCash=cash_before, itemLots=lots_before,
                                canonicalSha256=canonical_sha_before, pendingDates=[])

        target = pending[0]                       # ★ 가장 오래된 1건만
        out_dir = auto_root / target
        out_dir.mkdir(parents=True, exist_ok=True)

        # dry-run 산출물 확보(현재 canonical 기준으로 유효해야 함; 아니면 재도출)
        dr_path = C.REPORTS_DIR / f"dry-run-{target}.json"
        dry_run = None
        if dr_path.exists():
            try:
                dry_run = json.loads(dr_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                dry_run = None
        stale = (dry_run is None) or (int(dry_run.get("proposedSequence") or 0) != seq_before + 1)
        rederived = False
        if stale:
            prev_td = previous_trading_day(target)
            fn = rederive_fn or _rederive_dry_run
            ok, detail = fn(prev_td)
            rederived = True
            if ok and dr_path.exists():
                try:
                    dry_run = json.loads(dr_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    dry_run = None
            if dry_run is None or int(dry_run.get("proposedSequence") or 0) != seq_before + 1:
                return _base_result(SKIPPED_NOT_READY, verdict="BLOCKED", now=now, date=today_iso,
                                    targetExecutionDate=target, canonicalChanged=False,
                                    reason=f"dry-run 재도출 실패 또는 시퀀스 불일치({detail})",
                                    rederived=rederived,
                                    founderAction="signal/dry-run 파이프라인 상태 확인")

        sig_date = dry_run.get("signalAsOfDate")
        pkg_dir = C.TEMP_ROOT / str(sig_date)
        ranking = None
        rk_path = pkg_dir / "rankings.json"
        if rk_path.exists():
            try:
                ranking = json.loads(rk_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                ranking = None

        gates = evaluate_auto_approval_gates(canonical=canonical, target_exec_date=target,
                                             dry_run=dry_run, ranking=ranking, rules=rules)
        gate_summary = {"executionDate": target, "evaluatedAt": now,
                        "canonicalSha256Before": canonical_sha_before,
                        "sequenceBefore": seq_before, "tradingDayIndexBefore": tdi_before,
                        "eligible": gates["eligible"], "blockedCodes": gates["blockedCodes"],
                        "checks": gates["checks"], "policyVersion": POLICY_VERSION,
                        "rederivedDryRun": rederived}
        gate_bytes = json.dumps(gate_summary, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        (out_dir / "gate-summary.json").write_bytes(gate_bytes)
        gate_sha = _sha_bytes(gate_bytes)

        if not gates["eligible"]:
            codes = [b["code"] for b in gates["blockedCodes"]]
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=codes, gateSummaryPath=str(out_dir / "gate-summary.json"),
                                reason=f"강제 게이트 실패: {', '.join(codes[:3])}",
                                founderAction=f"{codes[0]} 원인 확인")

        if not do_apply:
            return _base_result(DRY_RUN_ONLY, verdict="PASS", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                gateSummaryPath=str(out_dir / "gate-summary.json"),
                                reason="게이트 전체 PASS — --dry-run 이라 장부 write 생략")

        # ── ticket → 정책 자동승인 → verify → receipt → dry-run-apply → apply ──
        seq_next = seq_before + 1
        tpath = out_dir / f"approval-ticket-seq{seq_next}.json"
        rpath = out_dir / f"execution-receipt-seq{seq_next}.json"
        tres = _run_script("magic_make_approval_ticket.py",
                           ["--execution-date", target, "--ticket-out", str(tpath),
                            "--canonical-path", str(canonical_path), "--json"])
        if tres.get("status") not in ("TICKET_CREATED", "ALREADY_TICKETED") or not tpath.exists():
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["TICKET_CREATE_FAILED"],
                                reason="approval ticket 생성 실패",
                                founderAction="ticket 생성 실패 원인 확인")

        ticket = json.loads(tpath.read_text(encoding="utf-8"))
        if ticket.get("canonicalBeforeSha256") != canonical_sha_before:
            return _base_result(BLOCKED_CANONICAL_MISMATCH, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=[B_CANONICAL_BEFORE_MISMATCH],
                                reason="ticket canonicalBeforeSha != 현재 canonical",
                                founderAction="canonical 변경 원인 확인")
        approved = apply_auto_approval(ticket, gate_sha, now=now)
        tpath.write_text(json.dumps(approved, ensure_ascii=False, indent=2), encoding="utf-8")

        vres = _af_cli(tpath, "verify", canonical_path, snapshot_dir, rpath)
        if vres.get("status") != "VERIFY_OK":
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["RECEIPT_VERIFY_FAILED"],
                                reason=f"verify 실패: {vres.get('blockedCode')}",
                                founderAction="ticket verify 실패 원인 확인")

        bres = _af_cli(tpath, "build-receipt", canonical_path, snapshot_dir, rpath)
        if bres.get("status") != "RECEIPT_BUILT":
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["RECEIPT_VERIFY_FAILED"],
                                reason=f"receipt 생성 실패: {bres.get('blockedCode')}",
                                founderAction="receipt 생성 실패 원인 확인")

        dres = _af_cli(tpath, "dry-run-apply", canonical_path, snapshot_dir, rpath)
        if dres.get("status") != "DRY_RUN_APPLY_OK":
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["DRY_RUN_APPLY_FAILED"],
                                reason=f"dry-run-apply 실패: {dres.get('blockedCode')}",
                                founderAction="dry-run-apply 실패 원인 확인")

        # canonical pre-backup (실패 시 apply 금지)
        bdir = auto_root / "_canonical-backup"
        bdir.mkdir(parents=True, exist_ok=True)
        bpath = bdir / f"canonical.PRE-{target}.json"
        try:
            bpath.write_bytes(canonical_path.read_bytes())
        except OSError as e:
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["BACKUP_FAILED"], reason=f"pre-backup 실패: {e}",
                                founderAction="백업 경로 확인")

        ares = _af_cli(tpath, "apply", canonical_path, snapshot_dir, rpath,
                       confirm=ticket.get("confirmToken") or f"APPLY_OFFICIAL_DAY_{target}")
        if ares.get("status") != "APPLIED":
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=False,
                                blockedCodes=["APPLY_FAILED"],
                                reason=f"apply 실패: {ares.get('blockedCode')} — 장부 변경 없음",
                                backupPath=str(bpath),
                                founderAction="apply 실패 원인 확인(백업 보존됨)")

        # ── 사후 검증 ──
        post = json.loads(canonical_path.read_text(encoding="utf-8"))
        post_sha = _sha_file(canonical_path)
        snap_path = Path(ares.get("snapshotPath") or (snapshot_dir / f"{target}.json"))
        snap_ok = False
        if snap_path.exists():
            try:
                snap_ok = json.loads(snap_path.read_text(encoding="utf-8")).get(
                    "canonicalStateSha256") == post_sha
            except (OSError, ValueError):
                snap_ok = False
        post_checks = {
            "sequencePlusOne": int(post.get("officialSequence") or 0) == seq_before + 1,
            "tdiPlusOne": int(post.get("officialTradingDayIndex") or 0) == tdi_before + 1,
            "lotsPlusTen": len(post.get("itemLots") or []) == lots_before + int(rules["topN"]),
            "cashNotNegative": float(post.get("officialAvailableCash") or 0) >= 0,
            "snapshotShaMatches": snap_ok,
            "formulaVersionUnchanged": post.get("formulaVersion") == canonical.get("formulaVersion"),
            "calendarAppended": (post.get("officialExecutionCalendar") or [])[-1:] == [target],
        }
        if not all(post_checks.values()):
            failed = [k for k, v in post_checks.items() if not v]
            return _base_result(BLOCKED_GATE_FAILED, verdict="BLOCKED", now=now, date=today_iso,
                                targetExecutionDate=target, canonicalChanged=True,
                                blockedCodes=["CANONICAL_POST_VERIFY_FAILED"],
                                postChecks=post_checks, backupPath=str(bpath),
                                reason=f"사후 검증 실패 {failed} — 추가 apply 중단, 백업 보존",
                                founderAction="장부 사후검증 실패 — 백업 대조 필요")

        result = _base_result(
            APPLIED_AUTOMATICALLY, verdict="PASS", now=now, date=today_iso,
            targetExecutionDate=target, canonicalChanged=True,
            officialSequence=post.get("officialSequence"),
            officialTradingDayIndex=post.get("officialTradingDayIndex"),
            officialAvailableCash=post.get("officialAvailableCash"),
            itemLots=len(post.get("itemLots") or []),
            batchId=ares.get("batchId"),
            canonicalSha256Before=canonical_sha_before, canonicalSha256After=post_sha,
            snapshotPath=str(snap_path), backupPath=str(bpath),
            ticketPath=str(tpath), receiptPath=str(rpath),
            gateSummaryPath=str(out_dir / "gate-summary.json"),
            postChecks=post_checks, remainingPendingDates=pending[1:],
            reason="전체 게이트 PASS — 가상 장부 자동 반영 완료(실주문 없음)")
        (out_dir / "apply-result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    finally:
        release_lock(lock_path)


def _run_script(script_name: str, args: list) -> dict:
    """저장소의 기존 검증된 CLI 를 그대로 호출한다(apply/ticket 로직 복제·우회 0)."""
    cmd = [sys.executable, str(Path(__file__).with_name(script_name)), *args]
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", env=env,
                       cwd=str(Path(__file__).resolve().parents[1]))
    s = p.stdout or ""
    try:
        return json.loads(s[s.index("{"):s.rindex("}") + 1])
    except (ValueError, json.JSONDecodeError):
        return {"status": "BLOCKED", "blockedCode": "CLI_PARSE_FAILED",
                "reason": ((p.stderr or s) or "")[-300:], "returncode": p.returncode}


def _af_cli(ticket_path: Path, mode: str, canonical_path: Path, snapshot_dir: Path,
            receipt_out: Path, confirm: str = "") -> dict:
    args = ["--ticket", str(ticket_path), "--mode", mode, "--json",
            "--canonical-path", str(canonical_path), "--snapshot-dir", str(snapshot_dir),
            "--receipt-out", str(receipt_out)]
    if confirm:
        args += ["--confirm", confirm]
    return _run_script("magic_apply_from_approval.py", args)


def _rederive_dry_run(signal_date: str | None):
    """현재 canonical 기준으로 dry-run 재도출(기존 스크립트 그대로 호출; 가격은 pykrx 실조회)."""
    if not signal_date:
        return False, "직전 거래일 산출 실패"
    cmd = [sys.executable, str(Path(__file__).with_name("magic_daily_dry_run.py")),
           "--signal-date", signal_date]
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", env=env,
                       cwd=str(Path(__file__).resolve().parents[1]))
    return p.returncode == 0, f"dry-run rc={p.returncode}"


# ── 내구성 상태 산출물(08:40 Founder 종합보고 수집원) ──────────────────────────
# TEMP 는 휘발성이라 종합보고가 읽을 수 없다. REPO2 reports/ 에 latest 를 남긴다
# (해당 폴더는 .gitignore 대상이라 stage 되지 않는다).
DURABLE_STATUS_JSON = C.ROOT / "reports" / "magic-auto-apply-status-latest.json"
DURABLE_STATUS_MD = C.ROOT / "reports" / "magic-auto-apply-status-latest.md"


def write_durable_status(result: dict, *, json_path: Path | None = None,
                         md_path: Path | None = None) -> None:
    """08:40 종합보고가 읽을 최신 상태를 REPO2 reports/ 에 기록한다(외부 발송 0)."""
    jp = Path(json_path or DURABLE_STATUS_JSON)
    mp = Path(md_path or DURABLE_STATUS_MD)
    jp.parent.mkdir(parents=True, exist_ok=True)
    jp.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    v = result.get("verdict", "UNKNOWN")
    L = [f"전체 판정: {v}", "",
         "# 와바바 마법공식 — 가상 장부 무인 자동반영 상태", "",
         f"- 기준일: {result.get('date')}",
         f"- 상태: {result.get('status')}",
         f"- 대상 거래일: {result.get('targetExecutionDate') or '없음'}",
         f"- 장부 변경: {'있음' if result.get('canonicalChanged') else '없음'}"]
    if result.get("officialSequence") is not None:
        L.append(f"- seq / TDI: {result.get('officialSequence')} / {result.get('officialTradingDayIndex')}")
        L.append(f"- 가상 현금: {result.get('officialAvailableCash')}")
        L.append(f"- 총 lot: {result.get('itemLots')}")
    if result.get("blockedCodes"):
        L.append(f"- 차단 사유: {', '.join(result['blockedCodes'])}")
    L += [f"- 미반영 잔여: {len(result.get('remainingPendingDates') or [])}건",
          f"- 실주문 {result.get('realOrderCount', 0)} · 브로커 {result.get('brokerApiCallCount', 0)} · "
          f"SMTP {result.get('smtpCallCount', 0)} · public {result.get('publicCopyCount', 0)}",
          f"- 사유: {result.get('reason', '')}",
          f"- 생성: {result.get('createdAt')}"]
    mp.write_text("\n".join(L) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="마법공식 가상 장부 무인 자동 반영(1회 실행 = 최대 1거래일, 실주문 없음)")
    ap.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (생략 시 오늘 KST)")
    ap.add_argument("--dry-run", action="store_true", help="게이트까지만 평가(장부 write 0)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    r = run_auto_apply(today_iso=args.date, do_apply=not args.dry_run)
    C.write_json_report(C.REPORTS_DIR / f"auto-apply-{args.date or C.today_kst_iso()}.json", r)
    if not args.dry_run:
        write_durable_status(r)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[AUTO_APPLY {r.get('date')}] status={r['status']} verdict={r['verdict']} "
              f"target={r.get('targetExecutionDate')} canonicalChanged={r.get('canonicalChanged')} "
              f"founderNotified={r.get('founderNotified')}")
    return 0 if r["verdict"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
