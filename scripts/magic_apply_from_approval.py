#!/usr/bin/env python3
"""마법공식 B티어 — approval-ticket 기반 고정 실행기 (Phase 45-AUTO3).

승인된 ticket을 근거로만 receipt 생성·dry-run apply·apply --confirm 을 수행한다.
- 기본 실행(verify/build-receipt/dry-run-apply)은 canonical write 0(검증·TEMP receipt만).
- 실제 장부 저장(mode=apply)은 ticket.status=APPROVED + approval.approved=true + 승인문구 + confirm 토큰
  + canonical/signal/dry-run/receipt SHA가 *모두* 일치할 때만. 하나라도 어긋나면 BLOCKED(변경 0).
- 장부 append 로직은 복제하지 않고 apply_magic_official_day(build_execution_receipt_v2 / apply_official_append)
  를 호출한다(복제 0). public 반영·git push·deploy는 이 스크립트 범위 아님.
- ★ 이번 Phase에서 실제 canonical 에는 mode=apply 를 실행하지 않는다(테스트 fixture/주입에서만 검증).

예) python scripts/magic_apply_from_approval.py --ticket <ticket.json> --mode verify
    python scripts/magic_apply_from_approval.py --ticket <ticket.json> --mode build-receipt
    python scripts/magic_apply_from_approval.py --ticket <ticket.json> --mode dry-run-apply
    python scripts/magic_apply_from_approval.py --ticket <ticket.json> --mode apply --confirm "APPLY_OFFICIAL_DAY_<date>"
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import apply_magic_official_day as A
import magic_make_approval_ticket as T

MODES = ("verify", "build-receipt", "dry-run-apply", "apply")
TICKET_SCHEMA_VERSION = T.TICKET_SCHEMA_VERSION


def _sha_file(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except OSError:
        return None


def _now_iso():
    import magic_daily_common as C
    return C.now_kst().isoformat()


def _blocked(code: str, mode: str, ticket: dict, reason: str, *, evidence=None,
             recommended_fix=None, now=None) -> dict:
    return {
        "status": "BLOCKED", "stage": "APPLY_FROM_APPROVAL", "mode": mode, "blockedCode": code,
        "ticketId": (ticket or {}).get("ticketId"), "executionDate": (ticket or {}).get("executionDate"),
        "actionType": (ticket or {}).get("actionType"),
        "autoStopped": True, "noFakeTrade": True, "fallbackPriceAllowed": False,
        "canonicalChanged": False, "publicChanged": False,
        "productionWriteCount": 0, "publicCopyCount": 0, "realOrderCount": 0,
        "reason": reason, "evidence": evidence or [], "recommendedManualFix": recommended_fix,
        "createdAt": now or _now_iso(),
    }


def _resolve_paths(ticket: dict, *, signal_pkg_dir=None, dry_run_log_path=None):
    pkg = Path(signal_pkg_dir) if signal_pkg_dir else (
        Path(ticket["signalPackagePath"]) if ticket.get("signalPackagePath") else None)
    log = Path(dry_run_log_path) if dry_run_log_path else (
        Path(ticket["dryRunLogPath"]) if ticket.get("dryRunLogPath") else None)
    return pkg, log


def verify_ticket_integrity(ticket: dict, *, canonical_path, signal_pkg_dir=None, dry_run_log_path=None):
    """(ok, blockedCode|None, reason). canonical/signal/dry-run-log SHA가 ticket 발급시점과 동일한지."""
    if ticket.get("schemaVersion") != TICKET_SCHEMA_VERSION:
        return False, "BLOCKED_TICKET_INPUT_INVALID", f"schemaVersion != {TICKET_SCHEMA_VERSION}"
    cur_canon = _sha_file(canonical_path)
    if not cur_canon:
        return False, "BLOCKED_CANONICAL_CHANGED", f"canonical missing: {canonical_path}"
    if cur_canon != ticket.get("canonicalBeforeSha256"):
        return False, "BLOCKED_CANONICAL_CHANGED", "canonical SHA != ticket.canonicalBeforeSha256 (canonical changed)"
    pkg, log = _resolve_paths(ticket, signal_pkg_dir=signal_pkg_dir, dry_run_log_path=dry_run_log_path)
    if pkg is not None:
        man, rk, uni = (_sha_file(pkg / "manifest.json"), _sha_file(pkg / "rankings.json"),
                        _sha_file(pkg / "universe.json"))
        if (man != ticket.get("signalPackageManifestSha256") or rk != ticket.get("rankingsSha256")
                or uni != ticket.get("universeSha256")):
            return False, "BLOCKED_SIGNAL_PACKAGE_CHANGED", "signal package SHA != ticket"
    if log is not None:
        if _sha_file(log) != ticket.get("dryRunLogSha256"):
            return False, "BLOCKED_DRY_RUN_LOG_CHANGED", "dry-run log SHA != ticket"
    return True, None, "ok"


def receipt_matches_ticket(receipt: dict, ticket: dict):
    """receipt 핵심값이 ticket.dryRunResult 제안과 일치하는지(불일치→apply 금지)."""
    dr = ticket.get("dryRunResult") or {}
    checks = [
        (int(receipt.get("sequence") or 0) == dr.get("proposedSequence"),
         f"sequence {receipt.get('sequence')} != ticket {dr.get('proposedSequence')}"),
        (receipt.get("batchId") == dr.get("proposedBatchId"),
         f"batchId {receipt.get('batchId')} != ticket {dr.get('proposedBatchId')}"),
        (int(receipt.get("buyCount") or 0) == dr.get("buyCount"),
         f"buyCount {receipt.get('buyCount')} != ticket {dr.get('buyCount')}"),
        (int(receipt.get("sellCount") or 0) == dr.get("sellCount"),
         f"sellCount {receipt.get('sellCount')} != ticket {dr.get('sellCount')}"),
        (A._approx(receipt.get("totalInvested"), dr.get("totalInvested")),
         f"totalInvested {receipt.get('totalInvested')} != ticket {dr.get('totalInvested')}"),
        (A._approx(receipt.get("cashReserve"), dr.get("cashReserve")),
         f"cashReserve {receipt.get('cashReserve')} != ticket {dr.get('cashReserve')}"),
    ]
    for ok, why in checks:
        if not ok:
            return False, why
    return True, "ok"


def _build_receipt(ticket, *, canonical_path, signal_pkg_dir, dry_run_log_path, now_iso, receipt_builder):
    pkg, log = _resolve_paths(ticket, signal_pkg_dir=signal_pkg_dir, dry_run_log_path=dry_run_log_path)
    return receipt_builder(pkg, log, canonical_path, created_at=now_iso)


def run_mode(ticket: dict, mode: str, *, confirm: str = "", canonical_path, snapshot_dir,
             signal_pkg_dir=None, dry_run_log_path=None, receipt_out=None, now_iso=None,
             receipt_builder=None, apply_fn=None) -> dict:
    """모드별 실행. verify/build-receipt/dry-run-apply는 canonical write 0. apply는 승인 전부 일치 시만."""
    now_iso = now_iso or _now_iso()
    receipt_builder = receipt_builder or A.build_execution_receipt_v2
    apply_fn = apply_fn or A.apply_official_append
    if mode not in MODES:
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", mode, ticket, f"unknown mode {mode}", now=now_iso)
    execution_date = ticket.get("executionDate")
    pkg, log = _resolve_paths(ticket, signal_pkg_dir=signal_pkg_dir, dry_run_log_path=dry_run_log_path)

    # 공통 무결성(SHA) — 모든 모드 선행
    ok, code, why = verify_ticket_integrity(ticket, canonical_path=canonical_path,
                                            signal_pkg_dir=pkg, dry_run_log_path=log)
    if not ok:
        return _blocked(code, mode, ticket, why, now=now_iso,
                        recommended_fix="ticket 재발급(magic_make_approval_ticket.py)")

    # ---- verify ----
    if mode == "verify":
        return {"status": "VERIFY_OK", "stage": "APPLY_FROM_APPROVAL", "mode": mode,
                "ticketId": ticket.get("ticketId"), "executionDate": execution_date,
                "approvalStatus": ticket.get("status"),
                "approved": (ticket.get("approval") or {}).get("approved"),
                "canonicalShaMatch": True, "signalPackageShaMatch": pkg is not None,
                "dryRunLogShaMatch": log is not None,
                "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
                "noFakeTrade": True, "createdAt": now_iso}

    # ---- build-receipt (TEMP receipt만) ----
    if mode == "build-receipt":
        if ticket.get("status") in ("REJECTED", "EXPIRED"):
            return _blocked("BLOCKED_TICKET_NOT_APPROVED", mode, ticket,
                            f"ticket status {ticket.get('status')} (cannot prepare)", now=now_iso)
        try:
            receipt = _build_receipt(ticket, canonical_path=canonical_path, signal_pkg_dir=pkg,
                                     dry_run_log_path=log, now_iso=now_iso, receipt_builder=receipt_builder)
        except Exception as e:  # noqa: BLE001
            return _blocked("BLOCKED_RECEIPT_MISMATCH", mode, ticket,
                            f"receipt build failed: {type(e).__name__}: {e}", now=now_iso)
        m_ok, m_why = receipt_matches_ticket(receipt, ticket)
        if not m_ok:
            return _blocked("BLOCKED_RECEIPT_MISMATCH", mode, ticket, m_why, now=now_iso)
        out = Path(receipt_out) if receipt_out else T.default_receipt_path(execution_date)
        # idempotency: 동일 receipt면 그대로, 다르면 CONFLICT(자동 덮어쓰기 0)
        if out.exists():
            try:
                existing = json.loads(out.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                existing = None
            same = (existing and {k: v for k, v in existing.items() if k not in ("createdAt", "receiptSha256")}
                    == {k: v for k, v in receipt.items() if k not in ("createdAt", "receiptSha256")})
            if not same:
                return _blocked("BLOCKED_RECEIPT_MISMATCH", mode, ticket,
                                "existing TEMP receipt differs (no auto-overwrite)", now=now_iso,
                                evidence=[str(out)])
            return {"status": "RECEIPT_READY", "stage": "APPLY_FROM_APPROVAL", "mode": mode,
                    "ticketId": ticket.get("ticketId"), "executionDate": execution_date,
                    "receiptPath": str(out), "receiptSha256": existing.get("receiptSha256"),
                    "alreadyPrepared": True, "productionWriteCount": 0, "publicCopyCount": 0,
                    "canonicalChanged": False, "noFakeTrade": True, "createdAt": now_iso}
        A.atomic_write_bytes(out, A.canonical_bytes(receipt))
        return {"status": "RECEIPT_BUILT", "stage": "APPLY_FROM_APPROVAL", "mode": mode,
                "ticketId": ticket.get("ticketId"), "executionDate": execution_date,
                "receiptPath": str(out), "receiptSha256": receipt.get("receiptSha256"),
                "sequence": receipt.get("sequence"), "batchId": receipt.get("batchId"),
                "totalInvested": receipt.get("totalInvested"), "cashReserve": receipt.get("cashReserve"),
                "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
                "noFakeTrade": True, "createdAt": now_iso}

    # build receipt (dry-run-apply / apply 공통; in-memory)
    if ticket.get("status") in ("REJECTED", "EXPIRED"):
        return _blocked("BLOCKED_TICKET_NOT_APPROVED", mode, ticket,
                        f"ticket status {ticket.get('status')}", now=now_iso)
    try:
        receipt = _build_receipt(ticket, canonical_path=canonical_path, signal_pkg_dir=pkg,
                                 dry_run_log_path=log, now_iso=now_iso, receipt_builder=receipt_builder)
    except Exception as e:  # noqa: BLE001
        return _blocked("BLOCKED_RECEIPT_MISMATCH", mode, ticket,
                        f"receipt build failed: {type(e).__name__}: {e}", now=now_iso)
    m_ok, m_why = receipt_matches_ticket(receipt, ticket)
    if not m_ok:
        return _blocked("BLOCKED_RECEIPT_MISMATCH", mode, ticket, m_why, now=now_iso)

    # ---- dry-run-apply (canonical write 0) ----
    if mode == "dry-run-apply":
        res = apply_fn(receipt, do_apply=False, state_path=canonical_path,
                       snapshot_dir=snapshot_dir, signal_pkg_dir=pkg)
        ok_dry = res.get("status") == A.DRY_RUN_OK
        return {"status": "DRY_RUN_APPLY_OK" if ok_dry else "BLOCKED",
                "blockedCode": None if ok_dry else "BLOCKED_RECEIPT_MISMATCH",
                "stage": "APPLY_FROM_APPROVAL", "mode": mode, "ticketId": ticket.get("ticketId"),
                "executionDate": execution_date, "applyStatus": res.get("status"),
                "officialSequence": res.get("officialSequence"), "batchId": res.get("batchId"),
                "officialAvailableCash": res.get("officialAvailableCash"),
                "productionWriteCount": res.get("productionWriteCount", 0),
                "publicCopyCount": 0, "canonicalChanged": False, "noFakeTrade": True,
                "reason": res.get("reason"), "createdAt": now_iso}

    # ---- apply (장부 저장; 승인 게이트 전부 통과 시만) ----
    approval = ticket.get("approval") or {}
    if ticket.get("status") != "APPROVED" or approval.get("approved") is not True:
        return _blocked("BLOCKED_TICKET_NOT_APPROVED", mode, ticket,
                        f"ticket not APPROVED (status={ticket.get('status')}, approved={approval.get('approved')})",
                        now=now_iso, recommended_fix="사람이 ticket.approval.approved=true 로 승인 후 재실행")
    expected_phrase = f"APPROVE_OFFICIAL_APPLY_{execution_date}"
    if approval.get("approvalPhrase") != expected_phrase:
        return _blocked("BLOCKED_APPROVAL_PHRASE_MISMATCH", mode, ticket,
                        f"approvalPhrase != {expected_phrase}", now=now_iso)
    expected_confirm = ticket.get("confirmToken") or f"APPLY_OFFICIAL_DAY_{execution_date}"
    if confirm != expected_confirm:
        return _blocked("BLOCKED_APPLY_CONFIRM_MISMATCH", mode, ticket,
                        f"--confirm != {expected_confirm}", now=now_iso)
    res = apply_fn(receipt, do_apply=True, confirm=expected_confirm, state_path=canonical_path,
                   snapshot_dir=snapshot_dir, signal_pkg_dir=pkg)
    applied = res.get("status") in (A.APPLIED_APPEND, A.APPLIED, A.ALREADY_PROCESSED)
    return {"status": "APPLIED" if applied else "BLOCKED",
            "blockedCode": None if applied else "BLOCKED_RECEIPT_MISMATCH",
            "stage": "APPLY_FROM_APPROVAL", "mode": mode, "ticketId": ticket.get("ticketId"),
            "executionDate": execution_date, "applyStatus": res.get("status"),
            "officialSequence": res.get("officialSequence"), "batchId": res.get("batchId"),
            "canonicalStateSha256": res.get("canonicalStateSha256"),
            "snapshotPath": res.get("snapshotPath"),
            "productionWriteCount": res.get("productionWriteCount", 0),
            "realOrderCount": res.get("realOrderCount", 0), "publicCopyCount": 0,
            "canonicalChanged": applied and res.get("status") != A.ALREADY_PROCESSED,
            "noFakeTrade": True, "reason": res.get("reason"), "createdAt": now_iso}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="approval-ticket 기반 실행기(verify/build-receipt/dry-run-apply/apply)")
    ap.add_argument("--ticket", required=True, help="approval-ticket.json 경로")
    ap.add_argument("--mode", required=True, choices=MODES)
    ap.add_argument("--confirm", default="", help="mode=apply 전용: APPLY_OFFICIAL_DAY_<executionDate>")
    ap.add_argument("--canonical-path", default=str(A.DEFAULT_STATE_PATH))
    ap.add_argument("--snapshot-dir", default=str(A.DEFAULT_SNAPSHOT_DIR))
    ap.add_argument("--signal-package", default=None, help="신호 패키지(생략 시 ticket 값)")
    ap.add_argument("--dry-run-log", default=None, help="dry-run 로그(생략 시 ticket 값)")
    ap.add_argument("--receipt-out", default=None, help="build-receipt 출력(생략 시 TEMP/<execDate>/execution-receipt.json)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    now_iso = _now_iso()

    try:
        ticket = json.loads(Path(args.ticket).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        r = _blocked("BLOCKED_TICKET_INPUT_INVALID", args.mode, {}, f"ticket read error: {e}", now=now_iso)
        print(json.dumps(r, ensure_ascii=False, indent=2) if args.json else f"[APPLY] BLOCKED {r['reason']}")
        return 2

    r = run_mode(ticket, args.mode, confirm=args.confirm, canonical_path=args.canonical_path,
                 snapshot_dir=args.snapshot_dir, signal_pkg_dir=args.signal_package,
                 dry_run_log_path=args.dry_run_log, receipt_out=args.receipt_out, now_iso=now_iso)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[APPLY {args.mode}] {r['status']} {r.get('blockedCode') or ''} "
              f"exec={r.get('executionDate')} apply={r.get('applyStatus','')}")
        if r.get("receiptPath"):
            print(f"  receipt: {r['receiptPath']}")
        if r.get("reason"):
            print(f"  reason: {r['reason']}")
    return 0 if r["status"] in ("VERIFY_OK", "RECEIPT_BUILT", "RECEIPT_READY",
                                "DRY_RUN_APPLY_OK", "APPLIED") else 2


if __name__ == "__main__":
    sys.exit(main())
