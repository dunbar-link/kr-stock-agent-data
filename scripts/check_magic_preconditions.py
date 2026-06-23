#!/usr/bin/env python3
"""마법공식 OFFICIAL 선결 상태 read-only 점검 (Phase 45-E19).

긴 인라인 python -c precondition 확인을 대체하는 *고정* 스크립트.
read-only 전용: 파일/장부/git 쓰기 0. git status/rev-parse/ls-remote/diff --cached(읽기)만 사용.

예) python scripts/check_magic_preconditions.py --expect-sequence 3 --expect-trading-index 5
    python scripts/check_magic_preconditions.py --signal-package <DIR> --execution-date 2026-06-23 --json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANONICAL = ROOT / "magic-formula-official-state.json"
KST = timezone(timedelta(hours=9))


def _sha_file(p) -> str | None:
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except OSError:
        return None


def _git(*args) -> str:
    try:
        out = subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True, timeout=30)
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def gather(args) -> dict:
    blocked = []
    head = _git("rev-parse", "--short", "HEAD")
    head_full = _git("rev-parse", "HEAD")
    origin = (_git("ls-remote", "origin", "main").split("\t") or [""])[0]
    staged = [x for x in _git("diff", "--cached", "--name-only").splitlines() if x.strip()]

    canon_path = Path(args.canonical_path)
    canon_sha = _sha_file(canon_path)
    canon = {}
    if canon_path.exists():
        try:
            canon = json.loads(canon_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            blocked.append(f"canonical read error: {e}")
    else:
        blocked.append(f"canonical missing: {canon_path}")

    seq = canon.get("officialSequence")
    idx = canon.get("officialTradingDayIndex")
    batches = canon.get("batches") or []
    buy = canon.get("buyLedger") or []
    sell = canon.get("sellLedger") or []
    missed = canon.get("missedRuns") or []

    pkg_check = None
    if args.signal_package:
        pkg = Path(args.signal_package)
        try:
            man = json.loads((pkg / "manifest.json").read_text(encoding="utf-8"))
            uni_ok = _sha_file(pkg / "universe.json") == man.get("universeSha256")
            rk_ok = _sha_file(pkg / "rankings.json") == man.get("rankingsSha256")
            pkg_check = {"path": str(pkg), "packageStatus": man.get("packageStatus"),
                         "signalAsOfDate": man.get("signalAsOfDate"),
                         "universeBaseDate": man.get("universeBaseDate"),
                         "nextExecutionDateCandidate": man.get("nextExecutionDateCandidate"),
                         "universeShaMatch": uni_ok, "rankingsShaMatch": rk_ok,
                         "top10Count": man.get("top10Count")}
            if not (uni_ok and rk_ok):
                blocked.append("signal package SHA mismatch")
            if man.get("packageStatus") != "READY_FOR_EXECUTION_OPEN":
                blocked.append(f"packageStatus {man.get('packageStatus')} != READY_FOR_EXECUTION_OPEN")
            if args.execution_date and man.get("nextExecutionDateCandidate") != args.execution_date:
                blocked.append(f"nextExecutionDateCandidate {man.get('nextExecutionDateCandidate')} "
                               f"!= {args.execution_date}")
            if args.signal_date and man.get("signalAsOfDate") != args.signal_date:
                blocked.append(f"signalAsOfDate {man.get('signalAsOfDate')} != {args.signal_date}")
        except (OSError, ValueError) as e:
            blocked.append(f"signal package read error: {e}")

    if args.expect_sequence is not None and seq != args.expect_sequence:
        blocked.append(f"officialSequence {seq} != expect {args.expect_sequence}")
    if args.expect_trading_index is not None and idx != args.expect_trading_index:
        blocked.append(f"officialTradingDayIndex {idx} != expect {args.expect_trading_index}")
    if args.expect_head and not (head == args.expect_head or head_full == args.expect_head
                                 or head_full.startswith(args.expect_head)):
        blocked.append(f"HEAD {head} != expect {args.expect_head}")
    if args.expect_head and origin and not origin.startswith(args.expect_head) and head != args.expect_head:
        # origin/main이 기대 HEAD와 다르면 경고(직접 BLOCKED은 HEAD로만)
        pass

    return {
        "head": head, "originMain": origin[:12], "headEqualsOrigin": bool(head and origin.startswith(head)),
        "stagedCount": len(staged), "stagedFiles": staged,
        "nowKST": datetime.now(KST).isoformat(),
        "canonicalPath": str(canon_path), "canonicalSha256": (canon_sha or "")[:16],
        "officialSequence": seq, "officialTradingDayIndex": idx,
        "batchCount": len(batches), "buyCount": len(buy), "sellCount": len(sell),
        "missedRunCount": len(missed),
        "officialAvailableCash": canon.get("officialAvailableCash"),
        "signalPackage": pkg_check,
        "blocked": blocked, "status": "PASS" if not blocked else "BLOCKED",
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 OFFICIAL 선결 read-only 점검(쓰기 0)")
    ap.add_argument("--signal-date", default=None)
    ap.add_argument("--execution-date", default=None)
    ap.add_argument("--signal-package", default=None)
    ap.add_argument("--canonical-path", default=str(DEFAULT_CANONICAL))
    ap.add_argument("--expect-sequence", type=int, default=None)
    ap.add_argument("--expect-trading-index", type=int, default=None)
    ap.add_argument("--expect-head", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    r = gather(args)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"HEAD={r['head']} origin/main={r['originMain']} (HEAD==origin: {r['headEqualsOrigin']})")
        print(f"staged={r['stagedCount']} {r['stagedFiles'] if r['stagedFiles'] else ''}")
        print(f"NOW KST={r['nowKST']}")
        print(f"canonical seq={r['officialSequence']} idx={r['officialTradingDayIndex']} "
              f"batch={r['batchCount']} BUY={r['buyCount']} SELL={r['sellCount']} "
              f"missed={r['missedRunCount']} availCash={r['officialAvailableCash']}")
        print(f"canonical SHA={r['canonicalSha256']}")
        if r["signalPackage"]:
            p = r["signalPackage"]
            print(f"signal pkg: status={p['packageStatus']} signalAsOf={p['signalAsOfDate']} "
                  f"nextExec={p['nextExecutionDateCandidate']} uniShaMatch={p['universeShaMatch']} "
                  f"rkShaMatch={p['rankingsShaMatch']} top10={p['top10Count']}")
        print(f"STATUS={r['status']}" + (f"  blocked={r['blocked']}" if r["blocked"] else ""))
    return 0 if r["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
