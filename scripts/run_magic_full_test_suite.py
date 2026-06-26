#!/usr/bin/env python3
"""마법공식 OFFICIAL 전체 테스트 suite 고정 실행 (Phase 45-E19).

shell for-loop / grep 파이프 확장을 대체하는 *고정* 명령. 각 테스트를 subprocess로 실행하고
파일별·합계 passed/failed를 요약한다. 모두 PASS여야 exit 0. 운영 데이터/장부/public/네트워크 write 0
(테스트 자체가 tmp/in-memory만 사용). PYTHONPATH=scripts 자동 처리.

예) python scripts/run_magic_full_test_suite.py
    python scripts/run_magic_full_test_suite.py --json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent

FULL_SUITE = [
    "test_magic_rolling_engine.py",
    "test_apply_magic_official_day.py",
    "test_apply_magic_official_day_v2.py",
    "test_eval_snapshot_compat.py",
    "test_record_magic_missed_run.py",
    "test_build_magic_official_public.py",
    "test_build_magic_signal_package.py",
    "test_run_magic_rolling_dry_run.py",
    "test_magic_daily_signal.py",
    "test_magic_daily_dry_run.py",
    "test_magic_daily_status.py",
    "test_magic_approval_ticket.py",
    "test_magic_apply_from_approval.py",
    "test_magic_publish_public.py",
    "test_magic_live_verify.py",
    "test_magic_backup.py",
]

_SUMMARY_RE = re.compile(r"(\d+)\s*passed,\s*(\d+)\s*failed")


def run_suite(test_files, *, fail_fast=False) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SCRIPTS) + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUTF8"] = "1"
    results, tot_p, tot_f = [], 0, 0
    for tf in test_files:
        path = SCRIPTS / tf
        if not path.exists():
            results.append({"file": tf, "passed": 0, "failed": 0, "exit": -1, "note": "missing"})
            tot_f += 1
            if fail_fast:
                break
            continue
        proc = subprocess.run([sys.executable, "-X", "utf8", str(path)],
                              capture_output=True, text=True, env=env, cwd=str(SCRIPTS))
        m = None
        for line in reversed(proc.stdout.splitlines()):
            m = _SUMMARY_RE.search(line)
            if m:
                break
        p, f = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        if m is None and proc.returncode != 0:
            f = max(f, 1)
        tot_p += p
        tot_f += f
        results.append({"file": tf, "passed": p, "failed": f, "exit": proc.returncode})
        if fail_fast and (f or proc.returncode != 0):
            break
    return {"results": results, "totalPassed": tot_p, "totalFailed": tot_f,
            "status": "PASS" if tot_f == 0 and all(r["exit"] == 0 for r in results) else "FAIL"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 전체 테스트 suite 고정 실행")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fail-fast", action="store_true")
    args = ap.parse_args(argv)
    r = run_suite(FULL_SUITE, fail_fast=args.fail_fast)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        for x in r["results"]:
            print(f"  {x['file']:42} {x['passed']:>3} passed, {x['failed']:>2} failed (exit {x['exit']})")
        print(f"=== TOTAL: {r['totalPassed']} passed, {r['totalFailed']} failed → {r['status']} ===")
    return 0 if r["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
