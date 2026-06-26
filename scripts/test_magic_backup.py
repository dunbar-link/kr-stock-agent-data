#!/usr/bin/env python3
"""magic_backup 테스트 (Phase 45-AUTO4-B). 전부 temp dir(실 OneDrive/REPO2 미접근). 소스 read-only 복사 검증."""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import magic_backup as B

NOW = "2026-06-26T18:00:00+09:00"
ED = "2026-06-26"


def _setup(root: Path):
    """temp 소스(canonical/snapshot/ticket/receipt/dry-run-log/signal pkg) 구성. (canon, snapdir, approval_root, dest)."""
    canon = root / "magic-formula-official-state.json"
    canon.write_text(json.dumps({"officialSequence": 5, "x": 1}), encoding="utf-8")
    snapdir = root / "snaps"
    snapdir.mkdir(parents=True, exist_ok=True)
    (snapdir / f"{ED}.json").write_text(json.dumps({"executionDate": ED, "seq": 5}), encoding="utf-8")
    pkg = root / "signal-2026-06-25"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "manifest.json").write_text(json.dumps({"m": 1}), encoding="utf-8")
    (pkg / "rankings.json").write_text(json.dumps({"r": 1}), encoding="utf-8")
    (pkg / "universe.json").write_text(json.dumps({"u": 1}), encoding="utf-8")
    log = root / "dry.log"
    log.write_text("RESULT_JSON_BEGIN\n{}\nRESULT_JSON_END\nEXIT_CODE=0", encoding="utf-8")
    approval_root = root / "approval"
    appdir = approval_root / ED
    appdir.mkdir(parents=True, exist_ok=True)
    (appdir / "approval-ticket.json").write_text(json.dumps({
        "ticketId": f"MF-APPROVAL-{ED}-OFFICIAL_APPLY", "signalPackagePath": str(pkg),
        "dryRunLogPath": str(log), "dryRunResult": {"proposedSequence": 5}}), encoding="utf-8")
    (appdir / "execution-receipt.json").write_text(json.dumps({"sequence": 5}), encoding="utf-8")
    dest = root / "backup"
    return canon, snapdir, approval_root, dest


def _run(root, dest):
    return B.run_backup(ED, dest_root=dest, now=NOW,
                        canonical_path=root / "magic-formula-official-state.json",
                        snapshot_dir=root / "snaps", approval_root=root / "approval")


# ===== 테스트 =====

def t1_backed_up_all_match():
    root = Path(tempfile.mkdtemp())
    try:
        canon, snapdir, approval_root, dest = _setup(root)
        r = _run(root, dest)
        assert r["status"] == "BACKED_UP", r
        assert r["allMatch"] is True and r["backupCount"] == 8
        # 8개 파일 + manifest 존재
        assert (dest / ED / "backup-manifest.json").exists()
        assert (dest / ED / "canonical" / "magic-formula-official-state.json").exists()
        assert (dest / ED / "snapshots" / f"{ED}.json").exists()
        man = json.loads((dest / ED / "backup-manifest.json").read_text(encoding="utf-8"))
        assert man["officialSequence"] == 5 and man["backupType"] == "OFFICIAL_BATCH5"
        assert all(f["match"] for f in man["files"].values())
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t2_already_backed_up_idempotent():
    root = Path(tempfile.mkdtemp())
    try:
        canon, snapdir, approval_root, dest = _setup(root)
        assert _run(root, dest)["status"] == "BACKED_UP"
        r2 = _run(root, dest)
        assert r2["status"] == "ALREADY_BACKED_UP", r2
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t3_conflict_no_overwrite():
    root = Path(tempfile.mkdtemp())
    try:
        canon, snapdir, approval_root, dest = _setup(root)
        assert _run(root, dest)["status"] == "BACKED_UP"
        before = (dest / ED / "canonical" / "magic-formula-official-state.json").read_bytes()
        # 소스 canonical 변경 후 재백업 → 기존 백업과 달라 CONFLICT(덮어쓰기 0)
        canon.write_text(json.dumps({"officialSequence": 5, "x": 999}), encoding="utf-8")
        r = _run(root, dest)
        assert r["status"] == "BLOCKED_BACKUP_CONFLICT", r
        assert (dest / ED / "canonical" / "magic-formula-official-state.json").read_bytes() == before
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t4_missing_source_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        canon, snapdir, approval_root, dest = _setup(root)
        (snapdir / f"{ED}.json").unlink()  # snapshot 누락
        r = _run(root, dest)
        assert r["status"] == "BLOCKED_BACKUP_INPUT_INVALID", r
        assert r["backupCount"] == 0 and not (dest / ED).exists()
    finally:
        shutil.rmtree(root, ignore_errors=True)


TESTS = [
    ("1  BACKED_UP + allMatch(8파일+manifest)", t1_backed_up_all_match),
    ("2  재실행 ALREADY_BACKED_UP(idempotent)", t2_already_backed_up_idempotent),
    ("3  소스 변경 후 재백업 BLOCKED_BACKUP_CONFLICT(덮어쓰기 0)", t3_conflict_no_overwrite),
    ("4  소스 누락 BLOCKED_BACKUP_INPUT_INVALID(생성 0)", t4_missing_source_blocked),
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
