#!/usr/bin/env python3
"""마법공식 B티어 — public 반영 plan/apply (Phase 45-AUTO3).

canonical(read-only) → public 3키(magicOfficialSummary·magicOfficialPortfolio·magicOfficialTradeDays)를
파생하고, REPO1 public(recommendation-history.json)에 *그 3키만* 표적 갱신한다.
- 매핑은 build_magic_official_public 재사용(복제 0). 와바바/AI/PILOT 등 나머지 키는 절대 건드리지 않는다.
- mode=plan: write 0. 현재 canonical seq vs REPO1 public seq 차이·바뀔 3키 요약·표적 파일·필요 승인만 보고.
- mode=apply: 승인된 PUBLIC_PUBLISH ticket + REPO1 clean + drift 0(3키 외 변경 0)일 때만 표적 갱신.
- ★ 이번 Phase에서 실제 REPO1 파일은 수정하지 않는다(plan/검증·테스트 fixture만). git push·deploy 범위 밖.

예) python scripts/magic_publish_public.py --mode plan
    python scripts/magic_publish_public.py --mode plan --json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import build_magic_official_public as P

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "magic-formula-official-state.json"
REPO1_ROOT = Path("C:/work/kr-stock-agent")
REPO1_PUBLIC_PATH = REPO1_ROOT / "public" / "data" / "recommendation-history.json"
OFFICIAL_PUBLIC_KEYS = list(P.OFFICIAL_PUBLIC_KEYS)


def _sha_file(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except OSError:
        return None


def _now_iso():
    import magic_daily_common as C
    return C.now_kst().isoformat()


def _git_status_porcelain(repo_root) -> str:
    try:
        out = subprocess.run(["git", "-C", str(repo_root), "status", "--porcelain"],
                             capture_output=True, text=True, timeout=30)
        return out.stdout
    except (OSError, subprocess.SubprocessError):
        return "GIT_ERROR"


def _repo1_dirty_for_public(porcelain: str, repo1_path) -> bool:
    """REPO1 public 파일이 이미 미스테이지/수정 상태면 dirty(자동 덮어쓰기 금지)."""
    if porcelain == "GIT_ERROR":
        return True
    try:
        rel = Path(repo1_path).resolve().relative_to(REPO1_ROOT.resolve()).as_posix()
    except (ValueError, OSError):
        rel = Path(repo1_path).name
    for line in porcelain.splitlines():
        if rel and rel in line.replace("\\", "/"):
            return True
    return False


def _blocked(code: str, mode: str, reason: str, *, evidence=None, recommended_fix=None, now=None) -> dict:
    return {
        "status": "BLOCKED", "stage": "PUBLISH_PUBLIC", "mode": mode, "blockedCode": code,
        "autoStopped": True, "noFakeTrade": True, "canonicalChanged": False, "publicChanged": False,
        "productionWriteCount": 0, "publicCopyCount": 0,
        "reason": reason, "evidence": evidence or [], "recommendedManualFix": recommended_fix,
        "createdAt": now or _now_iso(),
    }


def build_plan(public_model: dict, repo1_doc: dict, *, repo1_path, canonical_sha: str, now=None) -> dict:
    """순수 plan(write 0). 바뀔 3키·seq 차이·표적 파일·필요 승인 요약."""
    summary = public_model.get("magicOfficialSummary") or {}
    canon_seq = summary.get("officialSequence")
    canon_asset = summary.get("totalAsset")
    repo1_summary = (repo1_doc.get("magicOfficialSummary") or {})
    repo1_seq = repo1_summary.get("officialSequence")
    repo1_asset = repo1_summary.get("totalAsset")

    changed_keys = [k for k in OFFICIAL_PUBLIC_KEYS if repo1_doc.get(k) != public_model.get(k)]
    untouched_keys = [k for k in repo1_doc.keys() if k not in OFFICIAL_PUBLIC_KEYS]
    trade_days = public_model.get("magicOfficialTradeDays") or []
    holdings = (public_model.get("magicOfficialPortfolio") or {}).get("holdings") or []

    return {
        "status": "PLAN_OK", "stage": "PUBLISH_PUBLIC", "mode": "plan",
        "currentCanonicalSeq": canon_seq, "currentRepo1PublicSeq": repo1_seq,
        "updateNeeded": bool(changed_keys),
        "changedKeys": changed_keys, "keysToUpdate": OFFICIAL_PUBLIC_KEYS,
        "untouchedKeyCount": len(untouched_keys), "repo1TotalKeyCount": len(repo1_doc),
        "magicOfficialPreview": {
            "seqBefore": repo1_seq, "seqAfter": canon_seq,
            "totalAssetBefore": repo1_asset, "totalAssetAfter": canon_asset,
            "tradeDayCountAfter": len(trade_days), "holdingsCountAfter": len(holdings),
            "dataDateAfter": summary.get("dataDate"),
        },
        "repo1TargetFile": str(repo1_path),
        "canonicalSha256": (canonical_sha or "")[:16],
        "publicModelSummarySha256": summary.get("sourceStateSha256", "")[:16],
        "requiredApproval": "PUBLIC_PUBLISH ticket(사람 승인) + REPO1 clean + drift 0",
        "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False, "publicChanged": False,
        "createdAt": now or _now_iso(),
    }


def plan(canonical_path=CANONICAL_PATH, repo1_path=REPO1_PUBLIC_PATH, *, now=None) -> dict:
    now = now or _now_iso()
    try:
        public_model = P.build_magic_official_public(state_path=canonical_path)
    except (FileNotFoundError, P.MappingValidationError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", "plan", f"{type(e).__name__}: {e}", now=now)
    try:
        repo1_doc = json.loads(Path(repo1_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", "plan",
                        f"REPO1 public read error: {e}", evidence=[str(repo1_path)], now=now)
    return build_plan(public_model, repo1_doc, repo1_path=repo1_path,
                      canonical_sha=_sha_file(canonical_path), now=now)


def _compute_drift(repo1_doc: dict, public_model: dict):
    """(new_doc, driftReason|None). 3키만 교체하고 나머지 키/값 보존 검증."""
    new_doc = dict(repo1_doc)
    for k in OFFICIAL_PUBLIC_KEYS:
        new_doc[k] = public_model.get(k)
    if set(new_doc.keys()) != set(repo1_doc.keys()):
        return None, "top-level key set changed (3키 외 추가/삭제 감지)"
    for k in repo1_doc.keys():
        if k in OFFICIAL_PUBLIC_KEYS:
            continue
        if new_doc[k] != repo1_doc[k]:
            return None, f"untouched key drift: {k}"
    return new_doc, None


def apply(ticket: dict, *, canonical_path=CANONICAL_PATH, repo1_path=REPO1_PUBLIC_PATH,
          git_status_fn=None, do_write: bool = False, now=None) -> dict:
    """승인된 PUBLIC_PUBLISH ticket 기반 표적 갱신. 승인/clean/drift 게이트 전부 통과 시만.
    do_write=False(기본)면 검증까지만(REPO1 write 0). ★ 이번 Phase는 do_write=False로만 사용."""
    now = now or _now_iso()
    git_status_fn = git_status_fn or (lambda: _git_status_porcelain(REPO1_ROOT))
    mode = "apply"

    if (ticket or {}).get("actionType") != "PUBLIC_PUBLISH":
        return _blocked("BLOCKED_TICKET_NOT_APPROVED", mode, "ticket.actionType != PUBLIC_PUBLISH", now=now)
    approval = (ticket or {}).get("approval") or {}
    if ticket.get("status") != "APPROVED" or approval.get("approved") is not True:
        return _blocked("BLOCKED_TICKET_NOT_APPROVED", mode,
                        f"ticket not APPROVED (status={ticket.get('status')}, approved={approval.get('approved')})",
                        now=now, recommended_fix="사람이 PUBLIC_PUBLISH ticket 승인 후 재실행")

    porcelain = git_status_fn()
    if _repo1_dirty_for_public(porcelain, repo1_path):
        return _blocked("BLOCKED_REPO1_NOT_CLEAN", mode,
                        "REPO1 public 파일이 이미 변경/스테이지 상태(자동 덮어쓰기 금지)",
                        evidence=[str(repo1_path)], now=now,
                        recommended_fix="REPO1 working tree 정리 후 재시도")

    try:
        public_model = P.build_magic_official_public(state_path=canonical_path)
    except (FileNotFoundError, P.MappingValidationError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", mode, f"{type(e).__name__}: {e}", now=now)
    try:
        repo1_doc = json.loads(Path(repo1_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", mode, f"REPO1 read error: {e}", now=now)

    new_doc, drift = _compute_drift(repo1_doc, public_model)
    if drift is not None:
        return _blocked("BLOCKED_PUBLIC_DRIFT", mode, drift, now=now)

    changed_keys = [k for k in OFFICIAL_PUBLIC_KEYS if repo1_doc.get(k) != public_model.get(k)]
    summary = {
        "status": "PUBLISH_VERIFIED" if not do_write else "PUBLISHED",
        "stage": "PUBLISH_PUBLIC", "mode": mode, "ticketId": ticket.get("ticketId"),
        "changedKeys": changed_keys, "keysToUpdate": OFFICIAL_PUBLIC_KEYS,
        "repo1TargetFile": str(repo1_path), "untouchedKeyCount": len(repo1_doc) - len(OFFICIAL_PUBLIC_KEYS),
        "canonicalSha256": (_sha_file(canonical_path) or "")[:16],
        "productionWriteCount": 0, "publicCopyCount": 0,
        "canonicalChanged": False, "publicChanged": False, "createdAt": now,
    }
    if not do_write:
        summary["reason"] = "verified; no write (이번 Phase 정책: REPO1 미수정)"
        return summary

    # do_write=True 경로(AUTO4 이후): atomic write + 재읽기로 3키 외 불변 재검증
    import os
    data = json.dumps(new_doc, ensure_ascii=False, indent=2).encode("utf-8")
    tmp = Path(repo1_path).with_name(Path(repo1_path).name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, repo1_path)
    after = json.loads(Path(repo1_path).read_text(encoding="utf-8"))
    if set(after.keys()) != set(repo1_doc.keys()):
        return _blocked("BLOCKED_PUBLIC_DRIFT", mode, "post-write key set changed", now=now)
    for k in repo1_doc.keys():
        if k not in OFFICIAL_PUBLIC_KEYS and after[k] != repo1_doc[k]:
            return _blocked("BLOCKED_PUBLIC_DRIFT", mode, f"post-write untouched key drift: {k}", now=now)
    summary.update({"publicChanged": True, "publicCopyCount": 1})
    summary["reason"] = "REPO1 public magicOfficial 3키 표적 갱신 완료"
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 public 반영 plan/apply (plan write 0; 이번 Phase apply 미실행)")
    ap.add_argument("--mode", default="plan", choices=("plan", "apply"))
    ap.add_argument("--canonical-path", default=str(CANONICAL_PATH))
    ap.add_argument("--repo1-path", default=str(REPO1_PUBLIC_PATH))
    ap.add_argument("--ticket", default=None, help="mode=apply 전용 PUBLIC_PUBLISH ticket 경로")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    now = _now_iso()

    if args.mode == "plan":
        r = plan(Path(args.canonical_path), Path(args.repo1_path), now=now)
    else:
        # 이번 Phase: apply는 검증까지만(do_write=False 강제). 실제 REPO1 수정은 사람 직접 + AUTO4.
        if not args.ticket:
            r = _blocked("BLOCKED_TICKET_NOT_APPROVED", "apply", "PUBLIC_PUBLISH ticket 경로 필요", now=now)
        else:
            try:
                ticket = json.loads(Path(args.ticket).read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                ticket = None
                r = _blocked("BLOCKED_TICKET_NOT_APPROVED", "apply", f"ticket read error: {e}", now=now)
            if ticket is not None:
                r = apply(ticket, canonical_path=Path(args.canonical_path),
                          repo1_path=Path(args.repo1_path), do_write=False, now=now)

    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        if r["status"] == "PLAN_OK":
            pv = r["magicOfficialPreview"]
            print(f"[PUBLISH plan] updateNeeded={r['updateNeeded']} "
                  f"canonSeq={r['currentCanonicalSeq']} repo1Seq={r['currentRepo1PublicSeq']}")
            print(f"  seq {pv['seqBefore']}→{pv['seqAfter']} · asset {pv['totalAssetBefore']}→{pv['totalAssetAfter']} "
                  f"· tradeDays {pv['tradeDayCountAfter']} · holdings {pv['holdingsCountAfter']}")
            print(f"  changedKeys={r['changedKeys']} untouched={r['untouchedKeyCount']} target={r['repo1TargetFile']}")
        else:
            print(f"[PUBLISH {args.mode}] {r['status']} {r.get('blockedCode','')} {r.get('reason','')}")
    return 0 if r["status"] in ("PLAN_OK", "PUBLISH_VERIFIED", "PUBLISHED") else 2


if __name__ == "__main__":
    sys.exit(main())
