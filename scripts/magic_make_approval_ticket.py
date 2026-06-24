#!/usr/bin/env python3
"""마법공식 B티어 — approval-ticket 생성기 (Phase 45-AUTO3).

최신 dry-run COMPLETED 결과를 읽어 *사람이 승인할 수 있는* PENDING_APPROVAL ticket을 TEMP에만 만든다.
- canonical write 0 · public write 0 · git write 0 · 실제 주문 0 · fallback 가격 0.
- 티켓은 *승인 요청 문서*일 뿐 승인 파일이 아니다. status=PENDING_APPROVAL, approved=false 기본.
- receipt 생성은 기본 안 함. --include-receipt-preview 시 메모리에서만 receipt를 만들어 preview 값만 담는다.
- 같은 제안이 이미 티켓이면 ALREADY_TICKETED(덮어쓰기 0). 내용 다르면 BLOCKED_TICKET_CONFLICT(덮어쓰기 0).

승인/실행은 magic_apply_from_approval.py(verify/build-receipt/dry-run-apply/apply)가 담당한다.
이 스크립트는 장부 저장·apply --confirm 기능을 절대 갖지 않는다.

예) python scripts/magic_make_approval_ticket.py                       # 최신 dry-run COMPLETED → ticket
    python scripts/magic_make_approval_ticket.py --execution-date 2026-06-25 --json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import magic_daily_common as C

TICKET_SCHEMA_VERSION = "magic-approval-ticket-v1"
APPROVAL_ROOT = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-approval"))

ACTION_TYPES = ("OFFICIAL_APPLY", "PUBLIC_PUBLISH", "MISSED_RUN")

# ticket 동일성 판정용 핵심 키(createdAt/expiresAt/approval/dryRunLog 경로·SHA·preview 제외 →
# 로그 timestamp가 바뀌어도 같은 제안이면 ALREADY_TICKETED).
_CORE_KEYS = ("schemaVersion", "ticketId", "actionType", "executionDate", "signalAsOfDate",
              "canonicalBeforeSha256", "signalPackageManifestSha256", "rankingsSha256",
              "universeSha256", "dryRunResult")


def _sha_file(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except OSError:
        return None


def ticket_dir(execution_date: str) -> Path:
    return APPROVAL_ROOT / execution_date


def default_ticket_path(execution_date: str) -> Path:
    return ticket_dir(execution_date) / "approval-ticket.json"


def default_receipt_path(execution_date: str) -> Path:
    """magic_apply_from_approval --mode build-receipt 가 쓰는 TEMP receipt 경로(저장소 밖)."""
    return ticket_dir(execution_date) / "execution-receipt.json"


def _atomic_write_json(path: Path, obj) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return str(path)


def _ticket_core(t: dict) -> dict:
    return {k: t.get(k) for k in _CORE_KEYS}


def _blocked(code: str, execution_date, reason: str, *, signal_as_of=None,
             evidence=None, recommended_fix=None, now=None, ticket_path=None) -> dict:
    """B티어 공통 BLOCKED 구조(write 0 명시)."""
    return {
        "status": "BLOCKED", "stage": "APPROVAL_TICKET", "blockedCode": code,
        "actionType": "OFFICIAL_APPLY", "executionDate": execution_date, "signalAsOfDate": signal_as_of,
        "ticketPath": str(ticket_path) if ticket_path else None,
        "autoStopped": True, "noFakeTrade": True, "fallbackPriceAllowed": False,
        "canonicalChanged": False, "publicChanged": False,
        "productionWriteCount": 0, "publicCopyCount": 0,
        "reason": reason, "evidence": evidence or [], "recommendedManualFix": recommended_fix,
        "createdAt": now or C.now_kst().isoformat(),
    }


def build_ticket(dry_run_report: dict, manifest: dict, *, signal_pkg_dir, manifest_sha: str,
                 rankings_sha: str, universe_sha: str, canonical_sha: str,
                 dry_run_log_path, dry_run_log_sha: str, now_iso: str,
                 action_type: str = "OFFICIAL_APPLY", expires_at=None,
                 receipt_preview=None):
    """dry-run COMPLETED 보고 + 신호 패키지 + canonical SHA로 PENDING_APPROVAL ticket dict 생성.

    조건 불충족 시 BLOCKED_TICKET_INPUT_INVALID dict 반환(write 0). 순수 함수(파일 접근은 호출자)."""
    dr = dry_run_report or {}
    execution_date = dr.get("executionDate")
    signal_as_of = dr.get("signalAsOfDate")

    # --- 1) dry-run COMPLETED 게이트(가짜거래/미완료/look-ahead/write 차단) ---
    gates = [
        (dr.get("status") == "COMPLETED", f"dry-run status={dr.get('status')!r} != COMPLETED"),
        (dr.get("runStatus") == "COMPLETED", f"dry-run runStatus={dr.get('runStatus')!r} != COMPLETED"),
        (dr.get("readOnlyUnchanged") is True, "dry-run readOnlyUnchanged != true"),
        (dr.get("lookAheadValidationPassed") is True, "dry-run lookAheadValidationPassed != true"),
        (dr.get("productionWriteCount") == 0, f"dry-run productionWriteCount={dr.get('productionWriteCount')} != 0"),
        (dr.get("officialStartDatePersisted") is False, "dry-run officialStartDatePersisted != false"),
        (not (dr.get("missingEvalCodes") or []), f"dry-run missingEvalCodes not empty: {dr.get('missingEvalCodes')}"),
    ]
    for ok, why in gates:
        if not ok:
            return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date, why,
                            signal_as_of=signal_as_of, now=now_iso,
                            recommended_fix="magic_daily_dry_run.py 재실행 후 COMPLETED 확인")

    # --- 2) 신호 패키지 SHA 정합성(라벨만 신뢰 금지) ---
    pkg_gates = [
        (manifest.get("universeSha256") == universe_sha, "signal package universe.json SHA != manifest"),
        (manifest.get("rankingsSha256") == rankings_sha, "signal package rankings.json SHA != manifest"),
        (str(manifest.get("signalAsOfDate")) == str(signal_as_of),
         f"manifest.signalAsOfDate {manifest.get('signalAsOfDate')} != dry-run {signal_as_of}"),
        (str(manifest.get("nextExecutionDateCandidate")) == str(execution_date),
         f"manifest.nextExecutionDateCandidate {manifest.get('nextExecutionDateCandidate')} != {execution_date}"),
    ]
    for ok, why in pkg_gates:
        if not ok:
            return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date, why,
                            signal_as_of=signal_as_of, evidence=[str(signal_pkg_dir)], now=now_iso,
                            recommended_fix="magic_daily_signal.py 재생성 / 신호 패키지 무결성 확인")

    # --- 3) canonical 현재 SHA 필수 ---
    if not canonical_sha:
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date,
                        "canonical state SHA unavailable (file missing)", signal_as_of=signal_as_of, now=now_iso)
    if not dry_run_log_sha:
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date,
                        f"dry-run log SHA unavailable: {dry_run_log_path}", signal_as_of=signal_as_of, now=now_iso)

    ticket_id = f"MF-APPROVAL-{execution_date}-{action_type}"
    tpath = default_ticket_path(execution_date)
    confirm_token = f"APPLY_OFFICIAL_DAY_{execution_date}"
    approval_phrase = f"APPROVE_OFFICIAL_APPLY_{execution_date}"

    dry_run_result = {
        "runStatus": dr.get("runStatus"),
        "proposedSequence": dr.get("proposedSequence"),
        "proposedBatchId": dr.get("proposedBatchId"),
        "buyTradingDayIndex": dr.get("buyTradingDayIndex"),
        "plannedSellTradingDayIndex": dr.get("plannedSellTradingDayIndex"),
        "buyCount": dr.get("buyCount"),
        "sellCount": dr.get("sellCount"),
        "totalInvested": dr.get("totalInvested"),
        "cashReserve": dr.get("cashReserve"),
        "officialAvailableCashBefore": dr.get("officialAvailableCashBefore"),
        "officialAvailableCashAfterPreview": dr.get("officialAvailableCashAfterPreview"),
        "holdingsMarketValuePreview": dr.get("holdingsMarketValuePreview"),
        "missingEvalCodes": dr.get("missingEvalCodes") or [],
        "readOnlyUnchanged": dr.get("readOnlyUnchanged"),
        "lookAheadValidationPassed": dr.get("lookAheadValidationPassed"),
    }

    commands_to_run = [
        {"step": "build-receipt", "requiresHumanApproval": False,
         "command": f"python scripts/magic_apply_from_approval.py --ticket {tpath} --mode build-receipt"},
        {"step": "dry-run-apply", "requiresHumanApproval": False,
         "command": f"python scripts/magic_apply_from_approval.py --ticket {tpath} --mode dry-run-apply"},
        {"step": "apply", "requiresHumanApproval": True, "marker": "REQUIRES_HUMAN_APPROVAL",
         "command": (f'python scripts/magic_apply_from_approval.py --ticket {tpath} '
                     f'--mode apply --confirm "{confirm_token}"'),
         "note": "ticket.approval.approved=true(사람) + approvalPhrase + confirm 토큰 모두 일치해야 실행"},
    ]

    risks = [
        "apply 단계는 canonical 장부 write(모의펀드 내부, 실제 주문 아님). 되돌리기: OneDrive 백업 + idempotent 재적용",
        "apply는 사람 승인(approved=true)·승인문구·confirm 토큰·canonical/signal/dry-run/receipt SHA가 모두 일치할 때만 실행",
        "fallback 가격 금지 · 가짜 거래 금지 · 11위 이하 대체 금지 · 수량 임의변경 금지",
        "public 반영·git push·Vercel deploy는 이 티켓 범위 아님(별도 사람 승인)",
    ]
    blocked_conditions = [
        "BLOCKED_TICKET_NOT_APPROVED", "BLOCKED_APPROVAL_PHRASE_MISMATCH", "BLOCKED_APPLY_CONFIRM_MISMATCH",
        "BLOCKED_CANONICAL_CHANGED", "BLOCKED_SIGNAL_PACKAGE_CHANGED", "BLOCKED_DRY_RUN_LOG_CHANGED",
        "BLOCKED_RECEIPT_MISMATCH",
    ]

    ticket = {
        "schemaVersion": TICKET_SCHEMA_VERSION,
        "ticketId": ticket_id,
        "actionType": action_type,
        "status": "PENDING_APPROVAL",
        "executionDate": execution_date,
        "signalAsOfDate": signal_as_of,
        "createdAt": now_iso,
        "expiresAt": expires_at,
        "canonicalBeforeSha256": canonical_sha,
        "signalPackagePath": str(signal_pkg_dir),
        "signalPackageManifestSha256": manifest_sha,
        "rankingsSha256": rankings_sha,
        "universeSha256": universe_sha,
        "dryRunLogPath": str(dry_run_log_path),
        "dryRunLogSha256": dry_run_log_sha,
        "dryRunResult": dry_run_result,
        "confirmToken": confirm_token,
        "commandsToRun": commands_to_run,
        "risks": risks,
        "blockedConditions": blocked_conditions,
        "approval": {
            "approved": False, "approvedBy": None, "approvedAt": None,
            "approvalPhrase": approval_phrase, "approvalNotes": None,
        },
        "noFakeTrade": True,
        "fallbackPriceAllowed": False,
        "productionWriteCount": 0,
        "publicCopyCount": 0,
    }
    if receipt_preview is not None:
        ticket["receiptPreview"] = receipt_preview
    return ticket


# ===== 입력 해석(파일 접근) =====

def _load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def resolve_inputs(args, now_iso):
    """CLI 인자 → (dry_run_report, manifest, signal_pkg_dir, dry_run_log_path) 또는 blocked dict."""
    # 1) dry-run 보고 찾기
    if args.execution_date:
        rep_path = C.REPORTS_DIR / f"dry-run-{args.execution_date}.json"
        rep = {"path": str(rep_path), "data": _load_json(rep_path)} if rep_path.exists() else None
    elif args.signal_date:
        man_dir = Path(args.signal_package) if args.signal_package else (C.TEMP_ROOT / args.signal_date)
        man = _load_json(man_dir / "manifest.json")
        ed = man.get("nextExecutionDateCandidate")
        rep_path = C.REPORTS_DIR / f"dry-run-{ed}.json"
        rep = {"path": str(rep_path), "data": _load_json(rep_path)} if rep_path.exists() else None
    else:
        rep = C.latest_report("dry-run")
    if not rep or not isinstance(rep.get("data"), dict):
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", args.execution_date,
                        "no dry-run report found", now=now_iso,
                        recommended_fix="magic_daily_dry_run.py 먼저 실행")
    dr = rep["data"]
    execution_date = dr.get("executionDate")
    signal_as_of = dr.get("signalAsOfDate")

    # 2) 신호 패키지
    pkg_dir = Path(args.signal_package) if args.signal_package else (C.TEMP_ROOT / str(signal_as_of))
    if not (pkg_dir / "manifest.json").exists():
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date,
                        f"signal package manifest missing: {pkg_dir}", signal_as_of=signal_as_of, now=now_iso)
    manifest = _load_json(pkg_dir / "manifest.json")

    # 3) dry-run 로그(보고의 logPath 우선, 없으면 최신 자동 탐색)
    log_path = None
    if args.dry_run_log:
        log_path = Path(args.dry_run_log)
    elif dr.get("logPath"):
        log_path = Path(dr["logPath"])
    else:
        stamp = str(execution_date).replace("-", "")
        cands = sorted(C.LOGS_DIR.glob(f"official_dry_run_{stamp}_*.log"), key=lambda p: p.stat().st_mtime) \
            if C.LOGS_DIR.exists() else []
        log_path = cands[-1] if cands else None
    if not log_path or not Path(log_path).exists():
        return _blocked("BLOCKED_TICKET_INPUT_INVALID", execution_date,
                        f"dry-run log missing for {execution_date}", signal_as_of=signal_as_of, now=now_iso,
                        recommended_fix="magic_daily_dry_run.py 재실행(COMPLETED 로그 생성)")
    return dr, manifest, pkg_dir, Path(log_path)


def make_ticket(args, *, now_iso) -> dict:
    resolved = resolve_inputs(args, now_iso)
    if isinstance(resolved, dict) and resolved.get("status") == "BLOCKED":
        return resolved
    dr, manifest, pkg_dir, log_path = resolved
    canonical_path = Path(args.canonical_path) if args.canonical_path else C.CANONICAL_PATH

    receipt_preview = None
    if args.include_receipt_preview:
        try:
            import apply_magic_official_day as A
            rc = A.build_execution_receipt_v2(pkg_dir, log_path, canonical_path, created_at=now_iso)
            receipt_preview = {
                "schemaVersion": rc.get("schemaVersion"), "sequence": rc.get("sequence"),
                "batchId": rc.get("batchId"), "buyCount": rc.get("buyCount"),
                "totalInvested": rc.get("totalInvested"), "cashReserve": rc.get("cashReserve"),
                "officialAvailableCashBefore": rc.get("officialAvailableCashBefore"),
                "officialAvailableCashAfter": rc.get("officialAvailableCashAfter"),
                "canonicalBeforeSha256": rc.get("canonicalBeforeSha256"),
                "receiptSha256": rc.get("receiptSha256"),
                "note": "preview only — receipt 파일은 magic_apply_from_approval --mode build-receipt 가 생성",
            }
        except Exception as e:  # noqa: BLE001
            return _blocked("BLOCKED_TICKET_INPUT_INVALID", dr.get("executionDate"),
                            f"receipt preview build failed: {type(e).__name__}: {e}",
                            signal_as_of=dr.get("signalAsOfDate"), now=now_iso)

    ticket = build_ticket(
        dr, manifest, signal_pkg_dir=pkg_dir,
        manifest_sha=_sha_file(pkg_dir / "manifest.json"),
        rankings_sha=_sha_file(pkg_dir / "rankings.json"),
        universe_sha=_sha_file(pkg_dir / "universe.json"),
        canonical_sha=_sha_file(canonical_path),
        dry_run_log_path=log_path, dry_run_log_sha=_sha_file(log_path),
        now_iso=now_iso, action_type=args.action_type, expires_at=args.expires_at,
        receipt_preview=receipt_preview)
    if ticket.get("status") == "BLOCKED":
        return ticket

    # idempotency: 같은 제안이면 ALREADY_TICKETED, 다르면 CONFLICT (자동 덮어쓰기 0)
    out = Path(args.ticket_out) if args.ticket_out else default_ticket_path(ticket["executionDate"])
    if out.exists():
        try:
            existing = _load_json(out)
        except (OSError, ValueError):
            existing = None
        if existing and _ticket_core(existing) == _ticket_core(ticket):
            return {"status": "ALREADY_TICKETED", "stage": "APPROVAL_TICKET",
                    "ticketId": existing.get("ticketId"), "ticketPath": str(out),
                    "executionDate": ticket["executionDate"], "approvalStatus": existing.get("status"),
                    "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
                    "reason": "identical approval ticket already exists (no overwrite)",
                    "createdAt": now_iso}
        return _blocked("BLOCKED_TICKET_CONFLICT", ticket["executionDate"],
                        "existing approval ticket has different proposal (no auto-overwrite)",
                        signal_as_of=ticket["signalAsOfDate"], ticket_path=out, now=now_iso,
                        recommended_fix="기존 ticket 검토 후 사람이 직접 정리")

    path = _atomic_write_json(out, ticket)
    return {"status": "TICKET_CREATED", "stage": "APPROVAL_TICKET",
            "ticketId": ticket["ticketId"], "ticketPath": path,
            "actionType": ticket["actionType"], "executionDate": ticket["executionDate"],
            "signalAsOfDate": ticket["signalAsOfDate"], "approvalStatus": ticket["status"],
            "proposedSequence": ticket["dryRunResult"]["proposedSequence"],
            "proposedBatchId": ticket["dryRunResult"]["proposedBatchId"],
            "buyCount": ticket["dryRunResult"]["buyCount"], "sellCount": ticket["dryRunResult"]["sellCount"],
            "canonicalBeforeSha256": ticket["canonicalBeforeSha256"][:16],
            "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
            "noFakeTrade": True, "createdAt": now_iso}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 approval-ticket 생성기(TEMP only, 장부/public/git write 0)")
    ap.add_argument("--execution-date", default=None, help="체결일 YYYY-MM-DD (dry-run 보고 키)")
    ap.add_argument("--signal-date", default=None, help="신호일 YYYY-MM-DD (manifest로 executionDate 해석)")
    ap.add_argument("--dry-run-log", default=None, help="dry-run 로그 경로(생략 시 보고 logPath/최신 탐색)")
    ap.add_argument("--signal-package", default=None, help="신호 패키지 디렉터리(생략 시 TEMP/<signalDate>)")
    ap.add_argument("--canonical-path", default=None, help="canonical 경로(생략 시 기본 canonical)")
    ap.add_argument("--ticket-out", default=None, help="ticket 저장 경로(생략 시 TEMP/<execDate>/approval-ticket.json)")
    ap.add_argument("--action-type", default="OFFICIAL_APPLY", choices=ACTION_TYPES)
    ap.add_argument("--expires-at", default=None, help="만료 ISO(선택)")
    ap.add_argument("--include-receipt-preview", action="store_true", help="receipt preview 값만 포함(파일 생성 0)")
    ap.add_argument("--now", default=None, help="tz-aware ISO(테스트용)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    now_iso = args.now or C.now_kst().isoformat()

    try:
        r = make_ticket(args, now_iso=now_iso)
    except (OSError, ValueError) as e:
        r = _blocked("BLOCKED_TICKET_INPUT_INVALID", args.execution_date,
                     f"{type(e).__name__}: {e}", now=now_iso)

    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[TICKET] {r['status']} {r.get('blockedCode','')} "
              f"exec={r.get('executionDate')} seq={r.get('proposedSequence','')} "
              f"{r.get('proposedBatchId','')}")
        if r.get("ticketPath"):
            print(f"  ticket: {r['ticketPath']}")
        if r.get("reason"):
            print(f"  reason: {r['reason']}")
    return 0 if r["status"] in ("TICKET_CREATED", "ALREADY_TICKETED") else 2


if __name__ == "__main__":
    sys.exit(main())
