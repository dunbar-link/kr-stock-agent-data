#!/usr/bin/env python3
"""마법공식 OFFICIAL 핵심 회귀 테스트 고정 실행 (Phase 45-E19).

apply/eval/missed-run/public 핵심 4종만 빠르게 돌리는 *고정* 명령(전체는 run_magic_full_test_suite.py).
run_suite 로직은 full suite 모듈에서 재사용(복제 0). 모두 PASS여야 exit 0.

예) python scripts/run_magic_regression_tests.py
    python scripts/run_magic_regression_tests.py --json --fail-fast
"""
from __future__ import annotations

import argparse
import json
import sys

from run_magic_full_test_suite import run_suite

REGRESSION = [
    "test_apply_magic_official_day_v2.py",
    "test_eval_snapshot_compat.py",
    "test_record_magic_missed_run.py",
    "test_build_magic_official_public.py",
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 핵심 회귀 테스트 고정 실행")
    ap.add_argument("--quick", action="store_true", help="(현재 동일; 향후 축약용 플래그)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fail-fast", action="store_true")
    args = ap.parse_args(argv)
    r = run_suite(REGRESSION, fail_fast=args.fail_fast)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        for x in r["results"]:
            print(f"  {x['file']:42} {x['passed']:>3} passed, {x['failed']:>2} failed (exit {x['exit']})")
        print(f"=== REGRESSION TOTAL: {r['totalPassed']} passed, {r['totalFailed']} failed → {r['status']} ===")
    return 0 if r["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
