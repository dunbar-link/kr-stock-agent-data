#!/usr/bin/env python3
"""마법공식 B티어 — public 반영 plan/apply (Phase 45-AUTO3).

canonical(read-only) → public 3키(magicOfficialSummary·magicOfficialPortfolio·magicOfficialTradeDays)를
파생하고, REPO1 public(recommendation-history.json)에 *그 3키만* 표적 갱신한다.
- 매핑은 build_magic_official_public 재사용(복제 0). 와바바/AI/PILOT 등 나머지 키는 절대 건드리지 않는다.
- mode=plan: write 0. 현재 canonical seq vs REPO1 public seq 차이·바뀔 3키 요약·표적 파일·confirm 토큰 보고.
- mode=apply: 명시 confirm(PUBLISH_MAGIC_PUBLIC_<dataDate>) + REPO1 clean + canonical SHA 정합 + drift 0(3키 외
  변경 0)이 *모두* 통과할 때만 REPO1 public/data 3키 표적 갱신. confirm 없음/오타 → BLOCKED, write 0.
- git add/commit/push·Vercel deploy는 범위 밖(사람 직접). 실제 주문 없음.

예) python scripts/magic_publish_public.py --mode plan
    python scripts/magic_publish_public.py --mode apply --verify-only --confirm PUBLISH_MAGIC_PUBLIC_2026-06-25  # 게이트만(write 0)
    python scripts/magic_publish_public.py --mode apply --confirm PUBLISH_MAGIC_PUBLIC_2026-06-25               # 실제 3키 반영
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
        "confirmToken": f"PUBLISH_MAGIC_PUBLIC_{summary.get('dataDate')}",
        "requiredApproval": "명시 confirm(PUBLISH_MAGIC_PUBLIC_<dataDate>) + REPO1 clean + canonical SHA 정합 + drift 0",
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


def expected_confirm_token(public_model: dict) -> str:
    """canonical dataDate 기반 confirm 토큰. 예: PUBLISH_MAGIC_PUBLIC_2026-06-25."""
    data_date = (public_model.get("magicOfficialSummary") or {}).get("dataDate")
    return f"PUBLISH_MAGIC_PUBLIC_{data_date}"


def apply(*, canonical_path=CANONICAL_PATH, repo1_path=REPO1_PUBLIC_PATH, confirm: str = "",
          git_status_fn=None, do_write: bool = False, now=None) -> dict:
    """confirm 게이트 기반 public 3키 표적 갱신. 아래 게이트가 *모두* 통과할 때만 REPO1 public/data write.
      1) confirm == PUBLISH_MAGIC_PUBLIC_<canonical dataDate>  (없으면 BLOCKED_CONFIRM_REQUIRED, 오타면 _MISMATCH)
      2) REPO1 public working tree clean
      3) canonical SHA == public model sourceStateSha256 (모델이 현재 canonical 파생)
      4) updateNeeded(magicOfficial 3키 차이 존재) · drift 0(3키 외 보존)
    do_write=False면 게이트 검증까지만(REPO1 write 0). do_write=True일 때만 실제 atomic write."""
    now = now or _now_iso()
    git_status_fn = git_status_fn or (lambda: _git_status_porcelain(REPO1_ROOT))
    mode = "apply"

    try:
        public_model = P.build_magic_official_public(state_path=canonical_path)
    except (FileNotFoundError, P.MappingValidationError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", mode, f"{type(e).__name__}: {e}", now=now)
    try:
        repo1_doc = json.loads(Path(repo1_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_PUBLIC_MODEL_INVALID", mode, f"REPO1 read error: {e}", now=now)

    summary_model = public_model.get("magicOfficialSummary") or {}
    canon_seq = summary_model.get("officialSequence")
    repo1_seq = (repo1_doc.get("magicOfficialSummary") or {}).get("officialSequence")
    expected = expected_confirm_token(public_model)

    # 1) confirm 게이트 — write 전 최우선(실수 방지)
    if not confirm:
        return {**_blocked("BLOCKED_CONFIRM_REQUIRED", mode,
                           f"--confirm 필요(기대 '{expected}')", now=now), "expectedConfirm": expected}
    if confirm != expected:
        return {**_blocked("BLOCKED_CONFIRM_MISMATCH", mode,
                           f"--confirm 불일치(기대 '{expected}')", now=now), "expectedConfirm": expected}

    # 2) REPO1 clean
    if _repo1_dirty_for_public(git_status_fn(), repo1_path):
        return _blocked("BLOCKED_REPO1_NOT_CLEAN", mode,
                        "REPO1 public 파일이 이미 변경/스테이지 상태(자동 덮어쓰기 금지)",
                        evidence=[str(repo1_path)], now=now,
                        recommended_fix="REPO1 working tree 정리 후 재시도")

    # 3) canonical SHA 정합(모델이 현재 canonical에서 파생됐는지)
    canon_sha = _sha_file(canonical_path)
    model_sha = summary_model.get("sourceStateSha256")
    if canon_sha and model_sha and canon_sha != model_sha:
        return _blocked("BLOCKED_CANONICAL_SHA_MISMATCH", mode,
                        "canonical SHA != public model sourceStateSha256", now=now)

    # 4) updateNeeded / drift
    changed_keys = [k for k in OFFICIAL_PUBLIC_KEYS if repo1_doc.get(k) != public_model.get(k)]
    if not changed_keys:
        return {"status": "ALREADY_CURRENT", "stage": "PUBLISH_PUBLIC", "mode": mode,
                "currentCanonicalSeq": canon_seq, "currentRepo1PublicSeq": repo1_seq,
                "changedKeys": [], "productionWriteCount": 0, "publicCopyCount": 0,
                "canonicalChanged": False, "publicChanged": False,
                "reason": "public already current (no magicOfficial change)", "createdAt": now}
    new_doc, drift = _compute_drift(repo1_doc, public_model)
    if drift is not None:
        return _blocked("BLOCKED_PUBLIC_DRIFT", mode, drift, now=now)

    base = {
        "stage": "PUBLISH_PUBLIC", "mode": mode, "confirm": confirm,
        "currentCanonicalSeq": canon_seq, "currentRepo1PublicSeq": repo1_seq,
        "changedKeys": changed_keys, "keysToUpdate": OFFICIAL_PUBLIC_KEYS,
        "repo1TargetFile": str(repo1_path), "untouchedKeyCount": len(repo1_doc) - len(OFFICIAL_PUBLIC_KEYS),
        "canonicalSha256": (canon_sha or "")[:16], "canonicalChanged": False, "createdAt": now,
    }
    if not do_write:
        return {**base, "status": "PUBLISH_VERIFIED", "productionWriteCount": 0, "publicCopyCount": 0,
                "publicChanged": False, "reason": "confirm·clean·sha·drift 게이트 통과(검증만, write 0)"}

    # do_write=True: atomic write + 재읽기로 3키 외 불변 재검증
    import os
    before_sha = _sha_file(repo1_path)
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
    after_seq = (after.get("magicOfficialSummary") or {}).get("officialSequence")
    return {**base, "status": "PUBLISHED", "productionWriteCount": 0, "publicCopyCount": 1,
            "publicChanged": True, "publicSeqBefore": repo1_seq, "publicSeqAfter": after_seq,
            "beforeSha256": (before_sha or "")[:16], "afterSha256": (_sha_file(repo1_path) or "")[:16],
            "reason": "REPO1 public magicOfficial 3키 표적 갱신 완료"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 public 반영 plan/apply (plan write 0; apply는 confirm 게이트)")
    ap.add_argument("--mode", default="plan", choices=("plan", "apply"))
    ap.add_argument("--canonical-path", default=str(CANONICAL_PATH))
    ap.add_argument("--repo1-path", default=str(REPO1_PUBLIC_PATH))
    ap.add_argument("--confirm", default="", help="mode=apply 전용: PUBLISH_MAGIC_PUBLIC_<canonical dataDate>")
    ap.add_argument("--verify-only", action="store_true",
                    help="apply 게이트만 검증하고 write 0(REPO1 미수정). 실 반영 전 점검용")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    now = _now_iso()

    if args.mode == "plan":
        r = plan(Path(args.canonical_path), Path(args.repo1_path), now=now)
    else:
        # apply: confirm 일치 + clean + sha + drift 0 전부 통과 시에만 REPO1 public/data write.
        # --verify-only면 게이트만 확인(write 0).
        r = apply(canonical_path=Path(args.canonical_path), repo1_path=Path(args.repo1_path),
                  confirm=args.confirm, do_write=not args.verify_only, now=now)

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
    return 0 if r["status"] in ("PLAN_OK", "PUBLISH_VERIFIED", "PUBLISHED", "ALREADY_CURRENT") else 2


if __name__ == "__main__":
    sys.exit(main())
