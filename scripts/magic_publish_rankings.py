#!/usr/bin/env python3
"""마법공식 순위(top100) public 반영 plan/apply — 독립 파이프라인 (Phase MF-RANKING-PUBLISH-BUILD).

signal rankings.json → build_magic_rankings로 magicOfficialRankings를 파생하고,
REPO1 public/data/recommendation-history.json에 *magicOfficialRankings 키만* additive 갱신한다.
- 기존 3키 publish(magic_publish_public)와 완전 분리: 이 스크립트는 magicOfficialRankings 외
  어떤 키도 건드리지 않는다(drift 0 재검증).
- plan: write 0. apply: confirm 토큰 + REPO1 clean + drift 0 게이트가 *모두* 통과할 때만 write.
- git add/commit/push·Vercel deploy는 범위 밖(사람 직접). 실제 주문 없음.

예) python scripts/magic_publish_rankings.py --rankings <signal>/rankings.json --mode plan
    python scripts/magic_publish_rankings.py --rankings <...> --mode apply --verify-only --confirm PUBLISH_MAGIC_RANKINGS_2026-07-08
    python scripts/magic_publish_rankings.py --rankings <...> --mode apply --confirm PUBLISH_MAGIC_RANKINGS_2026-07-08
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import build_magic_rankings as BR
import magic_publish_public as PUB   # git status/dirty 헬퍼 재사용(그 모듈 무변경)

ROOT = Path(__file__).resolve().parents[1]
REPO1_ROOT = Path("C:/work/kr-stock-agent")
REPO1_PUBLIC_PATH = REPO1_ROOT / "public" / "data" / "recommendation-history.json"
RANKINGS_KEY = BR.RANKINGS_KEY   # "magicOfficialRankings"


def _now_iso():
    import magic_daily_common as C
    return C.now_kst().isoformat()


def expected_confirm_token(rankings: dict) -> str:
    """dataDate 기반 confirm 토큰. 예: PUBLISH_MAGIC_RANKINGS_2026-07-08."""
    return f"PUBLISH_MAGIC_RANKINGS_{rankings.get('dataDate')}"


def _blocked(code: str, mode: str, reason: str, *, evidence=None, now=None) -> dict:
    return {"status": "BLOCKED", "stage": "PUBLISH_RANKINGS", "mode": mode, "blockedCode": code,
            "autoStopped": True, "noFakeTrade": True, "publicChanged": False,
            "productionWriteCount": 0, "publicCopyCount": 0,
            "reason": reason, "evidence": evidence or [], "createdAt": now or _now_iso()}


def _load_repo1(repo1_path):
    return json.loads(Path(repo1_path).read_text(encoding="utf-8"))


def plan(rankings_path, *, repo1_path=REPO1_PUBLIC_PATH, now=None) -> dict:
    now = now or _now_iso()
    try:
        rankings = BR.build_magic_rankings(rankings_path)
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_RANKINGS_BUILD_FAILED", "plan", f"{type(e).__name__}: {e}", now=now)
    try:
        repo1_doc = _load_repo1(repo1_path)
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_PUBLIC_READ_FAILED", "plan", f"REPO1 read error: {e}", now=now)
    cur = repo1_doc.get(RANKINGS_KEY)
    return {
        "status": "PLAN_OK", "stage": "PUBLISH_RANKINGS", "mode": "plan",
        "updateNeeded": cur != rankings, "key": RANKINGS_KEY,
        "dataDate": rankings.get("dataDate"), "signalAsOfDate": rankings.get("signalAsOfDate"),
        "rankingScope": rankings.get("rankingScope"), "eligibleCount": rankings.get("eligibleCount"),
        "cheapTop100Count": len(rankings.get("cheapTop100") or []),
        "qualityTop100Count": len(rankings.get("qualityTop100") or []),
        "combinedTop10Count": len(rankings.get("combinedTop10") or []),
        "repo1TargetFile": str(repo1_path), "repo1HasKey": cur is not None,
        "repo1TotalKeyCount": len(repo1_doc), "confirmToken": expected_confirm_token(rankings),
        "requiredApproval": "명시 confirm(PUBLISH_MAGIC_RANKINGS_<dataDate>) + REPO1 clean + drift 0(magicOfficialRankings 외 불변)",
        "productionWriteCount": 0, "publicCopyCount": 0, "publicChanged": False, "createdAt": now,
    }


def apply(rankings_path, *, repo1_path=REPO1_PUBLIC_PATH, confirm: str = "",
          git_status_fn=None, do_write: bool = False, now=None) -> dict:
    """confirm 게이트 기반 magicOfficialRankings 키 단독 갱신. 게이트 모두 통과 시에만 write.
      1) confirm == PUBLISH_MAGIC_RANKINGS_<dataDate>
      2) REPO1 public working tree clean
      3) do_write 후 magicOfficialRankings 외 모든 키 불변(drift 0) 재검증
    do_write=False면 게이트 검증까지만(write 0)."""
    now = now or _now_iso()
    git_status_fn = git_status_fn or (lambda: PUB._git_status_porcelain(REPO1_ROOT))
    mode = "apply"
    try:
        rankings = BR.build_magic_rankings(rankings_path)
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_RANKINGS_BUILD_FAILED", mode, f"{type(e).__name__}: {e}", now=now)
    try:
        repo1_doc = _load_repo1(repo1_path)
    except (OSError, ValueError) as e:
        return _blocked("BLOCKED_PUBLIC_READ_FAILED", mode, f"REPO1 read error: {e}", now=now)
    expected = expected_confirm_token(rankings)

    # 1) confirm
    if not confirm:
        return {**_blocked("BLOCKED_CONFIRM_REQUIRED", mode, f"--confirm 필요(기대 '{expected}')", now=now), "expectedConfirm": expected}
    if confirm != expected:
        return {**_blocked("BLOCKED_CONFIRM_MISMATCH", mode, f"--confirm 불일치(기대 '{expected}')", now=now), "expectedConfirm": expected}

    # 2) REPO1 clean
    if PUB._repo1_dirty_for_public(git_status_fn(), repo1_path):
        return _blocked("BLOCKED_REPO1_NOT_CLEAN", mode,
                        "REPO1 public 파일이 이미 변경/스테이지 상태(자동 덮어쓰기 금지)",
                        evidence=[str(repo1_path)], now=now)

    # 3) updateNeeded
    if repo1_doc.get(RANKINGS_KEY) == rankings:
        return {"status": "ALREADY_CURRENT", "stage": "PUBLISH_RANKINGS", "mode": mode,
                "key": RANKINGS_KEY, "publicChanged": False, "productionWriteCount": 0,
                "publicCopyCount": 0, "reason": "magicOfficialRankings already current", "createdAt": now}

    new_doc = dict(repo1_doc)
    new_doc[RANKINGS_KEY] = rankings
    base = {"stage": "PUBLISH_RANKINGS", "mode": mode, "confirm": confirm, "key": RANKINGS_KEY,
            "dataDate": rankings.get("dataDate"), "repo1TargetFile": str(repo1_path),
            "repo1HadKey": repo1_doc.get(RANKINGS_KEY) is not None,
            "untouchedKeyCount": len(repo1_doc) - (1 if repo1_doc.get(RANKINGS_KEY) is not None else 0),
            "createdAt": now, "productionWriteCount": 0}
    if not do_write:
        return {**base, "status": "PUBLISH_VERIFIED", "publicCopyCount": 0, "publicChanged": False,
                "reason": "confirm·clean 게이트 통과(검증만, write 0)"}

    # do_write=True: atomic write + 재읽기로 magicOfficialRankings 외 불변 재검증
    before_sha = hashlib.sha256(Path(repo1_path).read_bytes()).hexdigest()[:16]
    data = json.dumps(new_doc, ensure_ascii=False, indent=2).encode("utf-8")
    tmp = Path(repo1_path).with_name(Path(repo1_path).name + ".rank.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, repo1_path)
    after = json.loads(Path(repo1_path).read_text(encoding="utf-8"))
    if set(after.keys()) != set(repo1_doc.keys()) | {RANKINGS_KEY}:
        return _blocked("BLOCKED_PUBLIC_DRIFT", mode, "post-write key set unexpected", now=now)
    for k in repo1_doc.keys():
        if k != RANKINGS_KEY and after.get(k) != repo1_doc.get(k):
            return _blocked("BLOCKED_PUBLIC_DRIFT", mode, f"post-write untouched key drift: {k}", now=now)
    after_sha = hashlib.sha256(Path(repo1_path).read_bytes()).hexdigest()[:16]
    return {**base, "status": "PUBLISHED", "publicCopyCount": 1, "publicChanged": True,
            "beforeSha256": before_sha, "afterSha256": after_sha,
            "reason": "REPO1 public magicOfficialRankings 키 additive 갱신 완료(3키 외 불변)"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="magicOfficialRankings public 반영 plan/apply(plan write 0; apply는 confirm 게이트)")
    ap.add_argument("--rankings", required=True, help="signal rankings.json 경로")
    ap.add_argument("--mode", default="plan", choices=("plan", "apply"))
    ap.add_argument("--repo1-path", default=str(REPO1_PUBLIC_PATH))
    ap.add_argument("--confirm", default="", help="mode=apply 전용: PUBLISH_MAGIC_RANKINGS_<dataDate>")
    ap.add_argument("--verify-only", action="store_true", help="apply 게이트만(write 0)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    if args.mode == "plan":
        r = plan(args.rankings, repo1_path=Path(args.repo1_path))
    else:
        r = apply(args.rankings, repo1_path=Path(args.repo1_path), confirm=args.confirm,
                  do_write=not args.verify_only)
    print(json.dumps(r, ensure_ascii=False, indent=2) if args.json else r.get("status"))
    return 0 if r.get("status") in ("PLAN_OK", "PUBLISH_VERIFIED", "PUBLISHED", "ALREADY_CURRENT") else 2


if __name__ == "__main__":
    import sys
    sys.exit(main())
