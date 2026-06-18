#!/usr/bin/env python3
"""와바바 마법공식 — MISSED_RUN 기록 + 거래일 index 마이그레이션 (Phase 45-E10).

목적
----
시가 전 신호 패키지가 없어 공식 거래를 만들 수 없는 날(예: 2026-06-18)을 *가짜 거래 없이* MISSED_RUN으로
기록한다. 실제 KRX 거래일 순번(officialTradingDayIndex)만 +1 하고, 성공 batch 순번(officialSequence)·
거래·배치·자금·원장·평가는 전혀 바꾸지 않는다. 각 lot의 50거래일 보유는 buyTradingDayIndex 기준으로 유지된다.

원칙
----
- 코어 로직(수량/자금/FIFO/index)은 magic_rolling_engine 재사용(복제 0). 원자적 저장/해시는 apply_magic_official_day 재사용.
- canonical read→migrate→apply_missed_run→검증→원자 저장→idempotent→OneDrive 백업. production 외 쓰기 0.
- 거래일 판정은 실제 KRX OHLCV(pykrx)로만. 평일 추정 금지. 미검증 시 BLOCKED, 변경 0.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import magic_rolling_engine as E
import run_magic_rolling_dry_run as W            # build_krx_calendar (pykrx 실거래일)
import apply_magic_official_day as A             # canonical_bytes / atomic_write_bytes / _sha_*

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_PATH = A.DEFAULT_STATE_PATH
DEFAULT_SNAPSHOT_DIR = A.DEFAULT_SNAPSHOT_DIR
REPO1_ROOT = Path("C:/work/kr-stock-agent")
DEFAULT_BACKUP_ROOT = Path("C:/Users/duria/OneDrive")
KST = timezone(timedelta(hours=9))

RECEIPT_SCHEMA_VERSION = "missed-run-receipt-v1"
SNAPSHOT_SCHEMA_VERSION = "magic-official-missed-snapshot-v1"
BACKUP_MANIFEST_SCHEMA = "wababa-backup-manifest-v1"

# 상태
RECORDED = "MISSED_RUN_RECORDED"
ALREADY_RECORDED = "ALREADY_RECORDED"
DRY_RUN_OK = "DRY_RUN_OK"
BLOCKED_TRADING_DAY_UNVERIFIED = "BLOCKED_TRADING_DAY_UNVERIFIED"
BLOCKED_DATE_IS_COMPLETED = "BLOCKED_DATE_IS_COMPLETED"
BLOCKED_INVARIANT_VIOLATION = "BLOCKED_INVARIANT_VIOLATION"
BLOCKED_APPLY_CONFIRMATION_REQUIRED = "BLOCKED_APPLY_CONFIRMATION_REQUIRED"
BLOCKED_ATOMIC_WRITE_FAILED = "BLOCKED_ATOMIC_WRITE_FAILED"
BLOCKED_STATE_NOT_FOUND = "BLOCKED_STATE_NOT_FOUND"


def _now_iso():
    return datetime.now(KST).isoformat()


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ===== 거래일 검증 (실제 KRX OHLCV; 평일 추정 금지) =====

def verify_krx_trading_day(date: str, pykrx_stock=None) -> bool:
    cal = W.build_krx_calendar(date, pykrx_stock=pykrx_stock)
    return bool(cal) and E.classify_trading_day(date, cal) == "TRADING"


# ===== MISSED_RUN 상태 빌드 (engine 재사용) =====

def build_missed_state(canonical: dict, date: str, reason: str, *, now: str):
    """migrate(거래일 index) → apply_missed_run. (새 state, entry, already) 반환. 입력 불변."""
    return E.apply_missed_run(canonical, date, reason, now=now)


# ===== 불변 검증 (2026-06-17 등 기존 COMPLETED 보존) =====

_IDX_FIELDS = ("buyTradingDayIndex", "plannedSellTradingDayIndex")


def _strip_idx(items):
    return [{k: v for k, v in it.items() if k not in _IDX_FIELDS} for it in (items or [])]


def check_invariants(before: dict, after: dict):
    """기존 거래/배치/자금/원장/평가가 불변인지(거래일 index 필드 추가만 허용) 검증."""
    checks = {
        "officialStartDate": before.get("officialStartDate") == after.get("officialStartDate"),
        "officialSequence": before.get("officialSequence") == after.get("officialSequence"),
        "officialAvailableCash": before.get("officialAvailableCash") == after.get("officialAvailableCash"),
        "batches": _strip_idx(before.get("batches")) == _strip_idx(after.get("batches")),
        "itemLots": _strip_idx(before.get("itemLots")) == _strip_idx(after.get("itemLots")),
        "buyLedger": before.get("buyLedger") == after.get("buyLedger"),
        "sellLedger": (before.get("sellLedger") or []) == (after.get("sellLedger") or []) == (after.get("sellLedger") or []),
        "sellLedgerEmpty": len(after.get("sellLedger") or []) == 0,
        "dailyLedger": before.get("dailyLedger") == after.get("dailyLedger"),
        "pilot": before.get("pilot") == after.get("pilot"),
        "totalInvested": _batch_total_invested(before) == _batch_total_invested(after),
    }
    bad = [k for k, ok in checks.items() if not ok]
    return (not bad), bad


def _batch_total_invested(state):
    return [(b.get("batchId"), b.get("totalInvested"), b.get("cashReserve"))
            for b in (state.get("batches") or [])]


# ===== receipt / snapshot =====

def build_receipt(date: str, before: dict, after: dict, entry: dict, *, verified: bool,
                  canonical_before_sha: str, now: str) -> dict:
    r = {
        "schemaVersion": RECEIPT_SCHEMA_VERSION,
        "date": date,
        "verifiedKrxTradingDay": verified,
        "reason": entry.get("reason"),
        "officialTradingDayIndexBefore": before.get("officialTradingDayIndex", len(before.get("officialTradingCalendar") or [])),
        "officialTradingDayIndexAfter": after.get("officialTradingDayIndex"),
        "officialSequenceBefore": before.get("officialSequence"),
        "officialSequenceAfter": after.get("officialSequence"),
        "signalPackagePresentBeforeOpen": False,
        "syntheticTradesCreated": False,
        "canonicalBeforeSha256": canonical_before_sha,
        "createdAt": now,
    }
    r["receiptSha256"] = A._sha_bytes(A.canonical_bytes({k: v for k, v in r.items() if k != "receiptSha256"}))
    return r


def _last_completed(after: dict):
    cal = after.get("officialExecutionCalendar") or after.get("officialTradingCalendar") or []
    last_date = cal[-1] if cal else None
    last_daily = None
    for d in (after.get("dailyLedger") or []):
        if d.get("date") == last_date:
            last_daily = d
    return last_date, last_daily


def build_snapshot(after: dict, receipt: dict, canonical_sha: str) -> dict:
    last_date, last_daily = _last_completed(after)
    last_asset = (last_daily or {}).get("totalAsset")
    return {
        "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
        "date": receipt["date"],
        "status": E.MISSED_RUN,
        "reason": receipt["reason"],
        "tradeCreated": False, "BUY": 0, "SELL": 0, "batchCreated": False,
        "officialTradingDayIndex": after.get("officialTradingDayIndex"),
        "officialSequence": after.get("officialSequence"),
        "lastCompletedTradingDate": last_date,
        "valuationUpdated": False,                       # 누락일 총자산 재평가 안 함
        "lastKnownTotalAsset": last_asset,               # 직전 COMPLETED 기준값
        "lastKnownTotalAssetAsOf": last_date,            # 기준일 분리(누락일 평가 아님)
        "receiptSha256": receipt["receiptSha256"],
        "canonicalStateSha256": canonical_sha,
        "createdAt": receipt["createdAt"],
    }


# ===== OneDrive 백업 (기존 체계 재사용; 자동 덮어쓰기 금지) =====

def backup_to_onedrive(state_path, snapshot_path, receipt_path, *, backup_root, date: str) -> dict:
    sr = Path(backup_root)
    if not sr.exists():
        return {"status": "BLOCKED_NO_SYNC_BACKUP_TARGET", "backupRoot": str(sr)}
    for bad in (ROOT, REPO1_ROOT):   # 저장소 내부로의 백업만 차단(TEMP/OneDrive는 허용)
        if str(sr.resolve()).lower().startswith(str(bad.resolve()).lower()):
            return {"status": "BLOCKED_UNSAFE_BACKUP_ROOT", "backupRoot": str(sr)}
    base = sr / "WababaBackup" / "magic-formula-official"
    final = base / date
    sources = [("canonical/magic-formula-official-state.json", Path(state_path)),
               (f"snapshots/{date}.json", Path(snapshot_path)),
               ("evidence/missed-run-receipt.json", Path(receipt_path))]
    src_sha = {rel: A._sha_file(p) for rel, p in sources}

    if final.exists():
        same = all((final / rel).exists() and A._sha_file(final / rel) == src_sha[rel] for rel, _ in sources)
        return {"status": "ALREADY_BACKED_UP" if same else "BLOCKED_BACKUP_CONFLICT", "finalPath": str(final)}

    work = base / f".{date}.tmp"
    if work.exists():
        shutil.rmtree(work, ignore_errors=True)
    try:
        for rel, p in sources:
            dst = work / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst)
        mismatch = [rel for rel, _ in sources if A._sha_file(work / rel) != src_sha[rel]]
        if mismatch:
            raise RuntimeError(f"backup sha mismatch {mismatch}")
        man = {
            "schemaVersion": BACKUP_MANIFEST_SCHEMA, "backupDate": date, "kind": "MISSED_RUN",
            "canonicalStateSha256": src_sha["canonical/magic-formula-official-state.json"],
            "snapshotSha256": src_sha[f"snapshots/{date}.json"],
            "missedRunReceiptSha256": src_sha["evidence/missed-run-receipt.json"],
            "sourcePaths": {rel: str(p) for rel, p in sources},
            "filesCount": len(sources), "sourceFilesUnchanged": True, "createdAt": _now_iso(),
        }
        (work / "backup-manifest.json").write_text(
            json.dumps(man, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        os.replace(work, final)
    except Exception as e:  # noqa: BLE001
        shutil.rmtree(work, ignore_errors=True)
        return {"status": "BLOCKED_ATOMIC_BACKUP_FAILED", "reason": f"{type(e).__name__}: {e}"}
    ok = all(A._sha_file(final / rel) == src_sha[rel] for rel, _ in sources)
    return {"status": "BACKED_UP" if ok else "BLOCKED_BACKUP_VERIFY_FAILED", "finalPath": str(final),
            "allShaMatch": ok, "filesCount": len(sources)}


# ===== 오케스트레이션 =====

def record_missed_run(date: str, *, reason: str = E.MISSED_RUN_NO_PREOPEN_SIGNAL, do_apply: bool = False,
                      confirm: str = "", state_path=DEFAULT_STATE_PATH, snapshot_dir=DEFAULT_SNAPSHOT_DIR,
                      receipt_path=None, backup_root=None, pykrx_stock=None, now=None) -> dict:
    state_path = Path(state_path)
    snapshot_dir = Path(snapshot_dir)
    now = now or _now_iso()
    base = {"date": date, "productionWriteCount": 0, "realOrderCount": 0, "statePath": str(state_path)}

    if not state_path.exists():
        return {**base, "status": BLOCKED_STATE_NOT_FOUND, "blocked": True, "reason": f"no canonical: {state_path}"}

    # 1) 거래일 검증
    verified = verify_krx_trading_day(date, pykrx_stock=pykrx_stock)
    if not verified:
        return {**base, "status": BLOCKED_TRADING_DAY_UNVERIFIED, "blocked": True,
                "reason": f"{date} not a verified KRX trading day"}

    canonical = _read_json(state_path)
    canonical_before_sha = A._sha_file(state_path)
    before = E.migrate_official_state_indices(canonical)   # 비교 기준(index 필드 포함 migrated)

    # 거래일이 이미 COMPLETED(성공 batch)면 MISSED 불가
    if date in (before.get("officialExecutionCalendar") or before.get("officialTradingCalendar") or []):
        return {**base, "status": BLOCKED_DATE_IS_COMPLETED, "blocked": True,
                "reason": f"{date} is a COMPLETED execution date"}

    new_state, entry, already = build_missed_state(canonical, date, reason, now=now)

    ok, bad = check_invariants(before, new_state)
    if not ok:
        return {**base, "status": BLOCKED_INVARIANT_VIOLATION, "blocked": True, "reason": f"changed: {bad}"}

    canonical_bytes = A.canonical_bytes(new_state)
    canonical_sha = A._sha_bytes(canonical_bytes)
    receipt = build_receipt(date, before, new_state, entry, verified=verified,
                            canonical_before_sha=canonical_before_sha, now=now)
    snapshot = build_snapshot(new_state, receipt, canonical_sha)
    snapshot_bytes = A.canonical_bytes(snapshot)
    snap_path = snapshot_dir / f"{date}.json"

    summary = {
        **base,
        "verifiedKrxTradingDay": verified,
        "officialTradingDayIndexBefore": receipt["officialTradingDayIndexBefore"],
        "officialTradingDayIndexAfter": new_state.get("officialTradingDayIndex"),
        "officialSequence": new_state.get("officialSequence"),
        "officialKrxTradingCalendar": new_state.get("officialKrxTradingCalendar"),
        "officialExecutionCalendar": new_state.get("officialExecutionCalendar"),
        "missedRunCount": len(new_state.get("missedRuns") or []),
        "lastKnownTotalAsset": snapshot["lastKnownTotalAsset"],
        "lastKnownTotalAssetAsOf": snapshot["lastKnownTotalAssetAsOf"],
        "canonicalStateSha256": canonical_sha,
        "snapshotPath": str(snap_path),
        "snapshotSha256": A._sha_bytes(snapshot_bytes),
        "receiptSha256": receipt["receiptSha256"],
    }

    if not do_apply:
        return {**summary, "status": DRY_RUN_OK, "blocked": False, "alreadyRecorded": already,
                "reason": "validated; no write (use --apply --confirm to persist)"}

    if confirm != f"RECORD_MISSED_RUN_{date}":
        return {**summary, "status": BLOCKED_APPLY_CONFIRMATION_REQUIRED, "blocked": True,
                "reason": f"confirm must be 'RECORD_MISSED_RUN_{date}'"}

    # idempotency: 이미 동일 MISSED_RUN이 기록돼 있으면(엔트리 존재 + canonical 동일) 재기록 0(쓰기 0).
    state_same = state_path.exists() and state_path.read_bytes() == canonical_bytes
    if already and state_same:
        return {**summary, "status": ALREADY_RECORDED, "blocked": False,
                "reason": "identical missed-run already recorded (hash match; no rewrite)"}

    # receipt(TEMP) 기록
    if receipt_path is not None:
        try:
            A.atomic_write_bytes(receipt_path, A.canonical_bytes(receipt))
        except Exception as e:  # noqa: BLE001
            return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True, "reason": f"receipt: {e}"}

    # 원자적 저장(canonical + snapshot)
    try:
        A.atomic_write_bytes(state_path, canonical_bytes)
        A.atomic_write_bytes(snap_path, snapshot_bytes)
    except Exception as e:  # noqa: BLE001
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True, "reason": str(e)}
    if A._sha_file(state_path) != canonical_sha or A._sha_file(snap_path) != summary["snapshotSha256"]:
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True, "reason": "post-write hash mismatch"}

    # OneDrive 백업(선택)
    backup = None
    if backup_root is not None and receipt_path is not None:
        backup = backup_to_onedrive(state_path, snap_path, receipt_path, backup_root=backup_root, date=date)

    return {**summary, "status": RECORDED, "blocked": False, "backup": backup,
            "reason": "MISSED_RUN recorded (append-only; no trade/batch/cash change)"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="MISSED_RUN 기록 (가짜 거래 0; 거래일 index만 +1)")
    ap.add_argument("--date", default="2026-06-18")
    ap.add_argument("--reason", default=E.MISSED_RUN_NO_PREOPEN_SIGNAL)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm", default="")
    ap.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    ap.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    ap.add_argument("--receipt-out", default=None)
    ap.add_argument("--backup-root", default=None)
    args = ap.parse_args(argv)
    res = record_missed_run(args.date, reason=args.reason, do_apply=args.apply, confirm=args.confirm,
                            state_path=args.state_path, snapshot_dir=args.snapshot_dir,
                            receipt_path=args.receipt_out, backup_root=args.backup_root)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] in (RECORDED, ALREADY_RECORDED, DRY_RUN_OK) else 2


if __name__ == "__main__":
    sys.exit(main())
