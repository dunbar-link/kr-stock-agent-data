#!/usr/bin/env python3
"""마법공식 OFFICIAL — AUTO4-B 후속 공식 백업 (Phase 45-AUTO4-B).

승인·적용 완료된 executionDate의 산출물을 OneDrive 백업 위치
(WababaBackup/magic-formula-official/<execDate>/)에 *동일 구조*로 보존한다.
대상: canonical + snapshot + evidence(approval-ticket/execution-receipt/dry-run-log) + signal(manifest/rankings/universe).

- read-only 소스 복사 + SHA 검증(sourceSha256==backupSha256) + backup-manifest.json.
- canonical/public/REPO1/운영 데이터 변경 0. signal/dry-run 경로는 approval-ticket에서 도출(복제 0).
- 기존 백업이 있으면 덮어쓰지 않음: 내용 동일 → ALREADY_BACKED_UP, 다르면 → BLOCKED_BACKUP_CONFLICT.

예) python scripts/magic_backup.py --execution-date 2026-06-26
    python scripts/magic_backup.py --execution-date 2026-06-26 --json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "magic-formula-official-state.json"
SNAPSHOT_DIR = ROOT / "data" / "magic-formula-official" / "snapshots"
APPROVAL_ROOT = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-approval"))
BACKUP_ROOT = Path(os.path.expandvars(r"%USERPROFILE%\OneDrive\WababaBackup\magic-formula-official"))


def _sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _now_iso() -> str:
    import magic_daily_common as C
    return C.now_kst().isoformat()


def plan_files(execution_date: str, *, canonical_path=CANONICAL_PATH, snapshot_dir=SNAPSHOT_DIR,
               approval_root=APPROVAL_ROOT):
    """백업 대상 (relpath -> source path) 매핑 + ticket dict. signal/dry-run 경로는 ticket에서 도출."""
    appdir = Path(approval_root) / execution_date
    ticket_path = appdir / "approval-ticket.json"
    ticket = json.loads(ticket_path.read_text(encoding="utf-8"))
    pkg = Path(ticket["signalPackagePath"])
    log = Path(ticket["dryRunLogPath"])
    mapping = {
        "canonical/magic-formula-official-state.json": Path(canonical_path),
        f"snapshots/{execution_date}.json": Path(snapshot_dir) / f"{execution_date}.json",
        "evidence/approval-ticket.json": ticket_path,
        "evidence/execution-receipt.json": appdir / "execution-receipt.json",
        "evidence/dry-run-log.txt": log,
        "signal/manifest.json": pkg / "manifest.json",
        "signal/rankings.json": pkg / "rankings.json",
        "signal/universe.json": pkg / "universe.json",
    }
    return mapping, ticket


def run_backup(execution_date: str, *, dest_root=BACKUP_ROOT, now=None,
               canonical_path=CANONICAL_PATH, snapshot_dir=SNAPSHOT_DIR, approval_root=APPROVAL_ROOT) -> dict:
    now = now or _now_iso()
    dest = Path(dest_root) / execution_date

    try:
        mapping, ticket = plan_files(execution_date, canonical_path=canonical_path,
                                     snapshot_dir=snapshot_dir, approval_root=approval_root)
    except (OSError, ValueError, KeyError) as e:
        return {"status": "BLOCKED_BACKUP_INPUT_INVALID", "executionDate": execution_date,
                "reason": f"{type(e).__name__}: {e}", "backupCount": 0, "createdAt": now}

    missing = [rel for rel, src in mapping.items() if not Path(src).exists()]
    if missing:
        return {"status": "BLOCKED_BACKUP_INPUT_INVALID", "executionDate": execution_date,
                "reason": f"missing source files: {missing}", "backupCount": 0, "createdAt": now}

    # 기존 백업 충돌(자동 덮어쓰기 금지)
    if dest.exists() and any(dest.iterdir()):
        same = all((dest / rel).exists() and _sha(dest / rel) == _sha(src) for rel, src in mapping.items())
        if same:
            return {"status": "ALREADY_BACKED_UP", "executionDate": execution_date, "backupDir": str(dest),
                    "backupCount": len(mapping), "createdAt": now,
                    "reason": "identical backup already exists (no overwrite)"}
        return {"status": "BLOCKED_BACKUP_CONFLICT", "executionDate": execution_date, "backupDir": str(dest),
                "reason": "existing backup differs (no overwrite)", "backupCount": 0, "createdAt": now}

    files = {}
    for rel, src in mapping.items():
        dpath = dest / rel
        dpath.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dpath)
        ssha, bsha = _sha(src), _sha(dpath)
        files[rel] = {"sourceSha256": ssha, "backupSha256": bsha, "match": ssha == bsha,
                      "bytes": dpath.stat().st_size}

    seq = (ticket.get("dryRunResult") or {}).get("proposedSequence") or ticket.get("proposedSequence")
    manifest = {
        "backupType": f"OFFICIAL_BATCH{seq}" if seq is not None else "OFFICIAL_BACKUP",
        "phase": "45-AUTO4-B", "executionDate": execution_date, "officialSequence": seq, "createdAt": now,
        "canonicalStateSha256": files["canonical/magic-formula-official-state.json"]["sourceSha256"],
        "snapshotSha256": files[f"snapshots/{execution_date}.json"]["sourceSha256"],
        "approvalTicketSha256": files["evidence/approval-ticket.json"]["sourceSha256"],
        "executionReceiptSha256": files["evidence/execution-receipt.json"]["sourceSha256"],
        "dryRunLogSha256": files["evidence/dry-run-log.txt"]["sourceSha256"],
        "signalManifestSha256": files["signal/manifest.json"]["sourceSha256"],
        "rankingsSha256": files["signal/rankings.json"]["sourceSha256"],
        "universeSha256": files["signal/universe.json"]["sourceSha256"],
        "files": files,
    }
    (dest / "backup-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    all_match = all(f["match"] for f in files.values())
    return {"status": "BACKED_UP" if all_match else "BLOCKED_BACKUP_SHA_MISMATCH",
            "executionDate": execution_date, "officialSequence": seq, "backupDir": str(dest),
            "backupCount": len(files), "allMatch": all_match,
            "files": {k: v["match"] for k, v in files.items()},
            "canonicalStateSha256": manifest["canonicalStateSha256"][:16], "createdAt": now}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 OFFICIAL 백업(canonical+snapshot+evidence+signal → OneDrive)")
    ap.add_argument("--execution-date", required=True, help="YYYY-MM-DD (승인·적용 완료된 체결일)")
    ap.add_argument("--dest-root", default=str(BACKUP_ROOT))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    r = run_backup(args.execution_date, dest_root=Path(args.dest_root))
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[BACKUP {args.execution_date}] {r['status']} dir={r.get('backupDir')} "
              f"files={r.get('backupCount')} allMatch={r.get('allMatch')}")
    return 0 if r["status"] in ("BACKED_UP", "ALREADY_BACKED_UP") else 2


if __name__ == "__main__":
    sys.exit(main())
