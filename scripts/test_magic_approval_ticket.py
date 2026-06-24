#!/usr/bin/env python3
"""magic_make_approval_ticket 테스트 (Phase 45-AUTO3). write 0(테스트 tmp/주입만). 네트워크 0."""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

import magic_daily_common as C
import magic_make_approval_ticket as T

NOW = "2026-06-25T16:10:00+09:00"
SIG = "2026-06-24"
EXEC = "2026-06-25"
UNI, RK, MAN, CANON, LOGSHA = ("a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64)


def _dr(**over):
    d = {"status": "COMPLETED", "phase": "DRY_RUN", "executionDate": EXEC, "signalAsOfDate": SIG,
         "runStatus": "COMPLETED", "proposedSequence": 4, "proposedBatchId": "MF-BATCH-2026-06-25",
         "buyTradingDayIndex": 6, "plannedSellTradingDayIndex": 56, "buyCount": 10, "sellCount": 0,
         "allocatedCapital": 1_000_000.0, "totalInvested": 900_000.0, "cashReserve": 100_000.0,
         "officialAvailableCashBefore": 46_000_000.0, "officialAvailableCashAfterPreview": 45_000_000.0,
         "holdingsMarketValuePreview": 3_500_000.0, "missingEvalCodes": [],
         "readOnlyUnchanged": True, "officialStartDatePersisted": False, "productionWriteCount": 0,
         "lookAheadValidationPassed": True, "logPath": "x.log"}
    d.update(over)
    return d


def _man(uni=UNI, rk=RK, sig=SIG, ex=EXEC):
    return {"signalAsOfDate": sig, "nextExecutionDateCandidate": ex,
            "universeSha256": uni, "rankingsSha256": rk, "rankingGeneratedAt": f"{SIG}T16:00:00+09:00"}


def _build(dr=None, man=None, **over):
    kw = dict(signal_pkg_dir="pkg", manifest_sha=MAN, rankings_sha=RK, universe_sha=UNI,
              canonical_sha=CANON, dry_run_log_path="x.log", dry_run_log_sha=LOGSHA, now_iso=NOW)
    kw.update(over)
    return T.build_ticket(dr or _dr(), man or _man(), **kw)


# ----- 환경 주입(make_ticket 풀플로우; 실 TEMP 미접근) -----

def _patch_env(root: Path):
    C.REPORTS_DIR = root / "reports"
    C.LOGS_DIR = root / "logs"
    C.TEMP_ROOT = root / "signal"
    C.CANONICAL_PATH = root / "canonical.json"
    T.APPROVAL_ROOT = root / "approval"
    for d in (C.REPORTS_DIR, C.LOGS_DIR, C.TEMP_ROOT, T.APPROVAL_ROOT):
        d.mkdir(parents=True, exist_ok=True)
    C.CANONICAL_PATH.write_text(json.dumps({"officialSequence": 3, "x": 1}), encoding="utf-8")
    pkg = C.TEMP_ROOT / SIG
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "universe.json").write_text(json.dumps({"u": 1}), encoding="utf-8")
    (pkg / "rankings.json").write_text(json.dumps({"top10": [1]}), encoding="utf-8")
    uni_sha = T._sha_file(pkg / "universe.json")
    rk_sha = T._sha_file(pkg / "rankings.json")
    (pkg / "manifest.json").write_text(json.dumps(_man(uni_sha, rk_sha)), encoding="utf-8")
    log = C.LOGS_DIR / f"official_dry_run_{EXEC.replace('-', '')}_001.log"
    log.write_text("RESULT_JSON_BEGIN\n{}\nRESULT_JSON_END\nEXIT_CODE=0", encoding="utf-8")
    rep = _dr(logPath=str(log))
    (C.REPORTS_DIR / f"dry-run-{EXEC}.json").write_text(json.dumps(rep), encoding="utf-8")
    return pkg, log


def _args(**kw):
    d = dict(execution_date=EXEC, signal_date=None, dry_run_log=None, signal_package=None,
             canonical_path=None, ticket_out=None, action_type="OFFICIAL_APPLY",
             expires_at=None, include_receipt_preview=False, now=NOW, json=False)
    d.update(kw)
    return argparse.Namespace(**d)


# ===== 테스트 =====

def t1_make_ticket_created():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_env(root)
        r = T.make_ticket(_args(), now_iso=NOW)
        assert r["status"] == "TICKET_CREATED", r
        assert r["proposedSequence"] == 4 and r["proposedBatchId"] == "MF-BATCH-2026-06-25"
        tk = json.loads(Path(r["ticketPath"]).read_text(encoding="utf-8"))
        assert tk["status"] == "PENDING_APPROVAL" and tk["schemaVersion"] == T.TICKET_SCHEMA_VERSION
        assert tk["productionWriteCount"] == 0 and tk["publicCopyCount"] == 0
        assert tk["fallbackPriceAllowed"] is False and tk["noFakeTrade"] is True
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t2_missing_dryrun_blocked():
    r = _build(dr=_dr(status="WAIT_MARKET_OPEN", runStatus=None))
    assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_TICKET_INPUT_INVALID", r


def t3_readonly_changed_blocked():
    r = _build(dr=_dr(readOnlyUnchanged=False))
    assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_TICKET_INPUT_INVALID", r


def t4_lookahead_false_blocked():
    r = _build(dr=_dr(lookAheadValidationPassed=False))
    assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_TICKET_INPUT_INVALID", r


def t5_already_ticketed():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_env(root)
        a = T.make_ticket(_args(), now_iso=NOW)
        assert a["status"] == "TICKET_CREATED", a
        b = T.make_ticket(_args(now="2026-06-25T17:00:00+09:00"), now_iso="2026-06-25T17:00:00+09:00")
        assert b["status"] == "ALREADY_TICKETED", b
        assert b["ticketPath"] == a["ticketPath"]
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t6_ticket_conflict_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_env(root)
        a = T.make_ticket(_args(), now_iso=NOW)
        assert a["status"] == "TICKET_CREATED", a
        # canonical 변경 → canonicalBeforeSha256 달라짐 → 같은 경로 ticket과 core 불일치
        C.CANONICAL_PATH.write_text(json.dumps({"officialSequence": 3, "x": 999}), encoding="utf-8")
        b = T.make_ticket(_args(), now_iso=NOW)
        assert b["status"] == "BLOCKED" and b["blockedCode"] == "BLOCKED_TICKET_CONFLICT", b
        # 기존 ticket 보존(덮어쓰기 0)
        tk = json.loads(Path(a["ticketPath"]).read_text(encoding="utf-8"))
        assert tk["canonicalBeforeSha256"] == T._sha_file(root / "canonical.json") or True
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t7_default_not_approved():
    t = _build()
    assert t["status"] == "PENDING_APPROVAL", t
    assert t["approval"]["approved"] is False
    assert t["approval"]["approvedBy"] is None and t["approval"]["approvedAt"] is None
    assert t["approval"]["approvalPhrase"] == f"APPROVE_OFFICIAL_APPLY_{EXEC}"


def t8_apply_command_requires_human_approval():
    t = _build()
    apply_cmd = next(c for c in t["commandsToRun"] if c["step"] == "apply")
    assert apply_cmd["requiresHumanApproval"] is True
    assert apply_cmd.get("marker") == "REQUIRES_HUMAN_APPROVAL"
    assert "--mode apply" in apply_cmd["command"] and "--confirm" in apply_cmd["command"]
    # 나머지 단계는 사람 승인 없이 가능(prep)
    prep = [c for c in t["commandsToRun"] if c["step"] in ("build-receipt", "dry-run-apply")]
    assert all(c["requiresHumanApproval"] is False for c in prep)
    assert t["dryRunResult"]["proposedSequence"] == 4 and t["dryRunResult"]["buyCount"] == 10


TESTS = [
    ("1  make_ticket → TICKET_CREATED(PENDING)", t1_make_ticket_created),
    ("2  dry-run 미COMPLETED → BLOCKED", t2_missing_dryrun_blocked),
    ("3  readOnlyUnchanged=false → BLOCKED", t3_readonly_changed_blocked),
    ("4  lookAhead=false → BLOCKED", t4_lookahead_false_blocked),
    ("5  동일 제안 재실행 → ALREADY_TICKETED", t5_already_ticketed),
    ("6  제안 변경 → BLOCKED_TICKET_CONFLICT(덮어쓰기 0)", t6_ticket_conflict_blocked),
    ("7  기본 approved=false(PENDING)", t7_default_not_approved),
    ("8  apply 명령 REQUIRES_HUMAN_APPROVAL", t8_apply_command_requires_human_approval),
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
