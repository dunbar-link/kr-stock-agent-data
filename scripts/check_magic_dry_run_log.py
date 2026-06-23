#!/usr/bin/env python3
"""마법공식 OFFICIAL dry-run 로그 read-only 검증 (Phase 45-E19).

긴 인라인 python -c 로그 파싱을 대체하는 *고정* 스크립트.
official_dry_run_<date>_*.log(PowerShell Tee=UTF-16)에서 RESULT_JSON·EXIT_CODE·pykrx_open을
파싱·검증한다. 파일/장부 쓰기 0. receipt/apply 미실행.

예) python scripts/check_magic_dry_run_log.py --date 2026-06-23 \
        --expect-sequence 3 --expect-buy-index 5 --expect-sell-index 55 \
        --expect-buy-count 10 --expect-sell-count 0
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

LOG_DIR = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-signal\logs"))


def read_log_text(path) -> str:
    raw = Path(path).read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    for enc in ("utf-8-sig", "utf-8", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def find_latest_log(date: str) -> Path | None:
    stamp = date.replace("-", "")
    cands = sorted(LOG_DIR.glob(f"official_dry_run_{stamp}_*.log"),
                   key=lambda p: p.stat().st_mtime)
    return cands[-1] if cands else None


def check(args) -> dict:
    blocked = []
    log_path = Path(args.log) if args.log else find_latest_log(args.date)
    if not log_path or not Path(log_path).exists():
        return {"status": "BLOCKED", "blocked": [f"dry-run log not found for {args.date}"],
                "logPath": str(log_path) if log_path else None}
    txt = read_log_text(log_path)

    exit0 = "EXIT_CODE=0" in txt
    if not exit0:
        blocked.append("EXIT_CODE != 0")
    has_pykrx = bool(re.search(r"pykrx_open[^\n]*?\{[^{}]*\}", txt))
    if not has_pykrx:
        blocked.append("pykrx_open line missing")

    res = {}
    try:
        a = txt.index("RESULT_JSON_BEGIN"); a = txt.index("{", a)
        e = txt.index("RESULT_JSON_END", a); b = txt.rindex("}", a, e) + 1
        res = json.loads(txt[a:b])
    except (ValueError, json.JSONDecodeError) as ex:
        blocked.append(f"RESULT_JSON parse error: {ex}")

    for k, want in (("runStatus", "COMPLETED"), ("readOnlyUnchanged", True),
                    ("lookAheadValidationPassed", True), ("productionWriteCount", 0),
                    ("officialStartDatePersisted", False)):
        if res.get(k) != want:
            blocked.append(f"{k}={res.get(k)!r} != {want!r}")
    miss = res.get("missingEvalCodes")
    if miss:
        blocked.append(f"missingEvalCodes not empty: {miss}")

    exp = {
        "proposedSequence": args.expect_sequence, "buyTradingDayIndex": args.expect_buy_index,
        "plannedSellTradingDayIndex": args.expect_sell_index,
        "buyCount": args.expect_buy_count, "sellCount": args.expect_sell_count,
    }
    for k, v in exp.items():
        if v is not None and res.get(k) != v:
            blocked.append(f"{k}={res.get(k)} != expect {v}")

    return {
        "logPath": str(log_path), "exitCode0": exit0, "pykrxOpenLine": has_pykrx,
        "runStatus": res.get("runStatus"), "proposedSequence": res.get("proposedSequence"),
        "proposedBatchId": res.get("proposedBatchId"),
        "buyTradingDayIndex": res.get("buyTradingDayIndex"),
        "plannedSellTradingDayIndex": res.get("plannedSellTradingDayIndex"),
        "buyCount": res.get("buyCount"), "sellCount": res.get("sellCount"),
        "totalInvested": res.get("totalInvested"), "cashReserve": res.get("cashReserve"),
        "officialAvailableCashBefore": res.get("officialAvailableCashBefore"),
        "officialAvailableCashAfterPreview": res.get("officialAvailableCashAfterPreview"),
        "holdingsMarketValuePreview": res.get("holdingsMarketValuePreview"),
        "missingEvalCodes": res.get("missingEvalCodes"),
        "readOnlyUnchanged": res.get("readOnlyUnchanged"),
        "blocked": blocked, "status": "PASS" if not blocked else "BLOCKED",
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 dry-run 로그 read-only 검증(쓰기 0)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--log", default=None, help="로그 경로(생략 시 최신 자동 탐색)")
    ap.add_argument("--expect-sequence", type=int, default=None)
    ap.add_argument("--expect-buy-index", type=int, default=None)
    ap.add_argument("--expect-sell-index", type=int, default=None)
    ap.add_argument("--expect-buy-count", type=int, default=None)
    ap.add_argument("--expect-sell-count", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    r = check(args)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"logPath={r.get('logPath')}")
        print(f"runStatus={r.get('runStatus')} seq={r.get('proposedSequence')} "
              f"batch={r.get('proposedBatchId')} buyIdx={r.get('buyTradingDayIndex')} "
              f"sellIdx={r.get('plannedSellTradingDayIndex')} BUY={r.get('buyCount')} SELL={r.get('sellCount')}")
        print(f"totalInvested={r.get('totalInvested')} cashReserve={r.get('cashReserve')} "
              f"cash {r.get('officialAvailableCashBefore')}→{r.get('officialAvailableCashAfterPreview')} "
              f"holdings={r.get('holdingsMarketValuePreview')} missingEval={r.get('missingEvalCodes')}")
        print(f"readOnlyUnchanged={r.get('readOnlyUnchanged')} exitCode0={r.get('exitCode0')}")
        print(f"STATUS={r['status']}" + (f"  blocked={r['blocked']}" if r["blocked"] else ""))
    return 0 if r["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
