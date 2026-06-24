#!/usr/bin/env python3
"""magic_apply_from_approval 테스트 (Phase 45-AUTO3).

receipt_builder/apply_fn 주입으로 엔진·네트워크 0. 실제 canonical write 0(모든 모드에서 검증).
apply 모드는 승인+승인문구+confirm+SHA가 전부 일치할 때만 do_apply=True 가 호출됨을 확인한다.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import apply_magic_official_day as A
import magic_apply_from_approval as AP

ED = "2026-06-25"
SIG = "2026-06-24"
CONFIRM = f"APPLY_OFFICIAL_DAY_{ED}"
PHRASE = f"APPROVE_OFFICIAL_APPLY_{ED}"
NOW = "2026-06-25T16:20:00+09:00"
DR = {"proposedSequence": 4, "proposedBatchId": "MF-BATCH-2026-06-25", "buyTradingDayIndex": 6,
      "plannedSellTradingDayIndex": 56, "buyCount": 10, "sellCount": 0,
      "totalInvested": 900_000.0, "cashReserve": 100_000.0, "readOnlyUnchanged": True,
      "lookAheadValidationPassed": True, "missingEvalCodes": []}


def _sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _setup(root: Path):
    canon = root / "canonical.json"
    canon.write_text(json.dumps({"officialSequence": 3, "x": 1}), encoding="utf-8")
    pkg = root / "pkg"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "manifest.json").write_text(json.dumps({"m": 1}), encoding="utf-8")
    (pkg / "rankings.json").write_text(json.dumps({"r": 1}), encoding="utf-8")
    (pkg / "universe.json").write_text(json.dumps({"u": 1}), encoding="utf-8")
    log = root / "d.log"
    log.write_text("RESULT_JSON_BEGIN\n{}\nRESULT_JSON_END\nEXIT_CODE=0", encoding="utf-8")
    snaps = root / "snaps"
    snaps.mkdir(parents=True, exist_ok=True)
    return canon, pkg, log, snaps


def _ticket(canon, pkg, log, *, status="PENDING_APPROVAL", approved=False, phrase=PHRASE):
    return {
        "schemaVersion": AP.TICKET_SCHEMA_VERSION, "ticketId": f"MF-APPROVAL-{ED}-OFFICIAL_APPLY",
        "actionType": "OFFICIAL_APPLY", "status": status, "executionDate": ED, "signalAsOfDate": SIG,
        "canonicalBeforeSha256": _sha(canon), "signalPackagePath": str(pkg),
        "signalPackageManifestSha256": _sha(pkg / "manifest.json"),
        "rankingsSha256": _sha(pkg / "rankings.json"), "universeSha256": _sha(pkg / "universe.json"),
        "dryRunLogPath": str(log), "dryRunLogSha256": _sha(log), "dryRunResult": dict(DR),
        "confirmToken": CONFIRM,
        "approval": {"approved": approved, "approvedBy": None, "approvedAt": None,
                     "approvalPhrase": phrase, "approvalNotes": None},
        "noFakeTrade": True, "fallbackPriceAllowed": False,
    }


def _mk_receipt(pkg, log, canon, *, created_at=None):
    return {"schemaVersion": A.RECEIPT_V2_SCHEMA_VERSION, "sequence": 4,
            "batchId": "MF-BATCH-2026-06-25", "buyCount": 10, "sellCount": 0,
            "totalInvested": 900_000.0, "cashReserve": 100_000.0,
            "officialAvailableCashBefore": 46_000_000.0, "officialAvailableCashAfter": 45_000_000.0,
            "receiptSha256": "f" * 64}


def _apply_factory(calls):
    def _apply(receipt, *, do_apply=False, confirm="", state_path=None, snapshot_dir=None, signal_pkg_dir=None):
        calls.append({"do_apply": do_apply, "confirm": confirm})
        if do_apply:
            return {"status": A.APPLIED_APPEND, "officialSequence": 4, "batchId": "MF-BATCH-2026-06-25",
                    "productionWriteCount": 0, "realOrderCount": 0, "canonicalStateSha256": "g" * 64,
                    "snapshotPath": "snap.json"}
        return {"status": A.DRY_RUN_OK, "officialSequence": 4, "batchId": "MF-BATCH-2026-06-25",
                "officialAvailableCash": 45_000_000.0, "productionWriteCount": 0}
    return _apply


def _run(ticket, mode, canon, pkg, log, snaps, *, confirm="", calls=None, receipt_out=None):
    calls = calls if calls is not None else []
    return AP.run_mode(ticket, mode, confirm=confirm, canonical_path=canon, snapshot_dir=snaps,
                       receipt_builder=_mk_receipt, apply_fn=_apply_factory(calls), now_iso=NOW,
                       receipt_out=receipt_out), calls


# ===== 테스트 =====

def t9_verify_write0():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        h0 = _sha(canon)
        r, _ = _run(_ticket(canon, pkg, log), "verify", canon, pkg, log, snaps)
        assert r["status"] == "VERIFY_OK", r
        assert r["productionWriteCount"] == 0 and _sha(canon) == h0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t10_build_receipt_temp_only():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        h0 = _sha(canon)
        out = root / "receipt.json"
        r, _ = _run(_ticket(canon, pkg, log), "build-receipt", canon, pkg, log, snaps, receipt_out=out)
        assert r["status"] == "RECEIPT_BUILT", r
        assert out.exists() and r["receiptPath"] == str(out)
        assert _sha(canon) == h0  # canonical 불변
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t11_dry_run_apply_canonical_write0():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        h0 = _sha(canon)
        r, calls = _run(_ticket(canon, pkg, log), "dry-run-apply", canon, pkg, log, snaps)
        assert r["status"] == "DRY_RUN_APPLY_OK", r
        assert calls and calls[0]["do_apply"] is False
        assert _sha(canon) == h0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t12_apply_not_approved_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        h0 = _sha(canon)
        r, calls = _run(_ticket(canon, pkg, log, status="PENDING_APPROVAL", approved=False),
                        "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_TICKET_NOT_APPROVED", r
        assert not any(c["do_apply"] for c in calls)  # 장부 적용 호출 0
        assert _sha(canon) == h0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t13_apply_phrase_mismatch_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        tk = _ticket(canon, pkg, log, status="APPROVED", approved=True, phrase="WRONG_PHRASE")
        r, calls = _run(tk, "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_APPROVAL_PHRASE_MISMATCH", r
        assert not any(c["do_apply"] for c in calls)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t14_canonical_changed_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        tk = _ticket(canon, pkg, log, status="APPROVED", approved=True)
        canon.write_text(json.dumps({"officialSequence": 3, "x": 999}), encoding="utf-8")  # canonical 변경
        r, calls = _run(tk, "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_CANONICAL_CHANGED", r
        assert not any(c["do_apply"] for c in calls)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t15_signal_package_changed_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        tk = _ticket(canon, pkg, log, status="APPROVED", approved=True)
        (pkg / "manifest.json").write_text(json.dumps({"tampered": True}), encoding="utf-8")
        r, calls = _run(tk, "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_SIGNAL_PACKAGE_CHANGED", r
        assert not any(c["do_apply"] for c in calls)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t16_approved_apply_executes():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        tk = _ticket(canon, pkg, log, status="APPROVED", approved=True, phrase=PHRASE)
        r, calls = _run(tk, "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert r["status"] == "APPLIED", r
        assert any(c["do_apply"] and c["confirm"] == CONFIRM for c in calls), calls
        assert r["applyStatus"] == A.APPLIED_APPEND and r["officialSequence"] == 4
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t17_no_live_canonical_write_any_mode():
    root = Path(tempfile.mkdtemp())
    try:
        canon, pkg, log, snaps = _setup(root)
        h0 = _sha(canon)
        # 모든 비-apply 모드 + 미승인 apply: 실제 canonical 파일 불변
        for mode in ("verify", "build-receipt", "dry-run-apply"):
            _run(_ticket(canon, pkg, log), mode, canon, pkg, log, snaps, receipt_out=root / f"r_{mode}.json")
            assert _sha(canon) == h0, f"{mode} mutated canonical"
        _run(_ticket(canon, pkg, log, approved=False), "apply", canon, pkg, log, snaps, confirm=CONFIRM)
        assert _sha(canon) == h0, "unapproved apply mutated canonical"
        # confirm 토큰 불일치(승인됐어도) → BLOCKED, do_apply 호출 0
        calls = []
        r, calls = _run(_ticket(canon, pkg, log, status="APPROVED", approved=True),
                        "apply", canon, pkg, log, snaps, confirm="WRONG")
        assert r["blockedCode"] == "BLOCKED_APPLY_CONFIRM_MISMATCH", r
        assert not any(c["do_apply"] for c in calls)
    finally:
        shutil.rmtree(root, ignore_errors=True)


TESTS = [
    ("9  verify write 0", t9_verify_write0),
    ("10 build-receipt TEMP receipt만(canonical 불변)", t10_build_receipt_temp_only),
    ("11 dry-run-apply canonical write 0(do_apply=False)", t11_dry_run_apply_canonical_write0),
    ("12 apply approved=false → BLOCKED", t12_apply_not_approved_blocked),
    ("13 approvalPhrase 불일치 → BLOCKED", t13_apply_phrase_mismatch_blocked),
    ("14 canonicalBeforeSha 불일치 → BLOCKED", t14_canonical_changed_blocked),
    ("15 signal package SHA 불일치 → BLOCKED", t15_signal_package_changed_blocked),
    ("16 승인+문구+confirm+SHA 일치 → APPLIED(do_apply)", t16_approved_apply_executes),
    ("17 모든 모드 live canonical write 금지", t17_no_live_canonical_write_any_mode),
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
