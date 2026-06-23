#!/usr/bin/env python3
"""마법공식 A티어 — OFFICIAL read-only dry-run 일일 자동화 (Phase 45-AUTO2).

READY 신호 패키지를 읽어 다음 거래일(executionDate)의 OFFICIAL batch 후보를 *read-only*로 검증한다.
날짜별 runner를 매번 새로 만들던 방식(E13/E17)을 파라미터화로 대체. 매수가=TOP10, 평가가=보유∪TOP10
(eval union; 보유 비-TOP10 종목 포함, fallback 0). canonical/public 변경 0, receipt/apply 미실행.

예) python scripts/magic_daily_dry_run.py                    # 오늘(KST) executionDate 후보 dry-run
    python scripts/magic_daily_dry_run.py --signal-date 2026-06-23
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import socket
import sys
import threading
from pathlib import Path

import magic_rolling_engine as E
import run_magic_rolling_dry_run as W
import magic_daily_common as C

socket.setdefaulttimeout(45)
PYKRX_TIMEOUT_S = 120


def _sha_file(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except OSError:
        return None


def _call_timeout(fn, timeout, label, *a, **k):
    box = {}
    def run():
        try:
            box["r"] = fn(*a, **k)
        except BaseException as e:  # noqa: BLE001
            box["e"] = e
    th = threading.Thread(target=run, name=label, daemon=True)
    th.start(); th.join(timeout)
    if th.is_alive():
        raise TimeoutError(f"{label} exceeded {timeout}s (pykrx stall)")
    if "e" in box:
        raise box["e"]
    return box.get("r")


def find_ready_package_for_today(today_iso: str):
    """nextExecutionDateCandidate == today 인 READY 패키지 디렉터리(없으면 None)."""
    if not C.TEMP_ROOT.exists():
        return None
    for d in sorted(C.TEMP_ROOT.glob("20*-*-*"), reverse=True):
        man = d / "manifest.json"
        if not man.exists():
            continue
        try:
            m = json.loads(man.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if m.get("packageStatus") == "READY_FOR_EXECUTION_OPEN" and m.get("nextExecutionDateCandidate") == today_iso:
            return d
    return None


def dry_run_core(canonical: dict, manifest: dict, rankings: dict, calendar, opens_all: dict,
                 *, now_iso: str) -> dict:
    """read-only dry-run 코어(네트워크 0; calendar/opens 주입). RESULT_JSON형 dict 또는 blocked dict 반환."""
    signal_as_of = manifest["signalAsOfDate"]
    execution_date = manifest["nextExecutionDateCandidate"]
    ranking_generated_at = manifest["rankingGeneratedAt"]
    universe_base_date = manifest.get("universeBaseDate")
    market_open_at = f"{execution_date}T09:00:00+09:00"

    gen_dt, open_dt = W._parse_aware(ranking_generated_at), W._parse_aware(market_open_at)
    if gen_dt is None or open_dt is None or gen_dt >= open_dt:
        return C.blocked_report("DRY_RUN", "BLOCKED_LOOKAHEAD", execution_date,
                                f"rankingGeneratedAt({ranking_generated_at}) !< open({market_open_at})",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)
    if not calendar or E.classify_trading_day(execution_date, calendar) != "TRADING":
        return C.blocked_report("DRY_RUN", "BLOCKED_CANONICAL_STATE_MISMATCH", execution_date,
                                f"{execution_date} not a verified KRX trading day",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)
    prev_td = W.previous_krx_trading_day(execution_date, calendar)
    if prev_td != signal_as_of:
        return C.blocked_report("DRY_RUN", "BLOCKED_LOOKAHEAD", execution_date,
                                f"previousKrxTradingDay({execution_date})={prev_td} != signalAsOf {signal_as_of}",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)
    if str(universe_base_date) != str(signal_as_of):
        return C.blocked_report("DRY_RUN", "BLOCKED_LOOKAHEAD", execution_date,
                                f"universeBaseDate {universe_base_date} != signalAsOf {signal_as_of}",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)

    top_codes = [str(t["code"]).zfill(6) for t in rankings["top10"]]
    held_codes = sorted({str(l["code"]).zfill(6) for l in (canonical.get("itemLots") or [])
                         if l.get("status") == "OPEN"})
    missing_top = [c for c in top_codes if not opens_all.get(c) or opens_all[c] <= 0]
    if missing_top:
        return C.blocked_report("DRY_RUN", "BLOCKED_MISSING_OPEN_PRICE", execution_date,
                                f"TOP10 missing pykrx_open: {missing_top} (fallback 금지)",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)
    buy_opens = {c: float(opens_all[c]) for c in top_codes}
    eval_opens = {c: float(v) for c, v in opens_all.items() if v and v > 0}
    missing_held = [c for c in held_codes if not opens_all.get(c) or opens_all[c] <= 0]

    ranking = [{"code": str(t["code"]).zfill(6), "name": t.get("name"), "rank": t.get("rank"),
                "combinedRank": t.get("combinedRank"), "profitabilityRank": t.get("profitabilityRank"),
                "valueRank": t.get("valueRank"), "returnOnCapital": t.get("returnOnCapital"),
                "earningsYield": t.get("earningsYield")} for t in rankings["top10"]]
    timing = {"signalAsOfDate": signal_as_of, "rankingGeneratedAt": ranking_generated_at,
              "executionDate": execution_date, "executionMarketOpenAt": market_open_at,
              "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    current_index = (canonical.get("officialTradingDayIndex") or 0) + 1
    avail_before = canonical.get("officialAvailableCash")
    new_state, result = E.plan_official_day(copy.deepcopy(canonical), execution_date, ranking,
                                            buy_opens, eval_opens, calendar, now=now_iso, timing=timing,
                                            trading_day_index=current_index)
    if result.get("runStatus") != E.COMPLETED:
        return C.blocked_report("DRY_RUN", "BLOCKED_DRY_RUN_NOT_COMPLETED", execution_date,
                                f"runStatus={result.get('runStatus')} reason={result.get('runReason')}",
                                execution_date=execution_date, signal_as_of=signal_as_of, now=now_iso)
    nb = new_state["batches"][-1]
    es = (new_state.get("evaluationSnapshots") or [{}])[-1]
    return {
        "status": "COMPLETED", "phase": "DRY_RUN", "executionDate": execution_date,
        "signalAsOfDate": signal_as_of, "runStatus": result.get("runStatus"),
        "proposedSequence": new_state.get("officialSequence"), "proposedBatchId": result.get("buyBatchId"),
        "buyTradingDayIndex": nb.get("buyTradingDayIndex"),
        "plannedSellTradingDayIndex": nb.get("plannedSellTradingDayIndex"),
        "buyCount": result.get("buyCount"), "sellCount": result.get("sellCount"),
        "allocatedCapital": result.get("allocatedCapital"), "totalInvested": result.get("totalInvested"),
        "cashReserve": result.get("cashReserve"), "officialAvailableCashBefore": avail_before,
        "officialAvailableCashAfterPreview": new_state.get("officialAvailableCash"),
        "holdingsMarketValuePreview": es.get("holdingsMarketValue"),
        "missingEvalCodes": es.get("missingEvalCodes"), "missingHeldOpen": missing_held,
        "plan": result.get("plan") or [], "openPrices": opens_all,
        "lookAheadValidationPassed": True, "productionWriteCount": 0,
        "officialStartDatePersisted": False, "noFakeTrade": True, "createdAt": now_iso,
    }


def write_dry_run_log(res: dict, *, unchanged: bool) -> str:
    """check_magic_dry_run_log.py / build_execution_receipt_v2 가 파싱하는 로그 형식(UTF-8)."""
    ed = res.get("executionDate")
    stamp = C.now_kst().strftime("%Y%m%d_%H%M%S")
    log = C.LOGS_DIR / f"official_dry_run_{str(ed).replace('-','')}_{stamp}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"=== magic_daily_dry_run executionDate={ed} {C.now_kst().isoformat()} ==="]
    if res.get("openPrices") is not None:
        opens = {k: res["openPrices"][k] for k in sorted(res["openPrices"])}
        lines.append(f"pykrx_open buy(TOP10) {len(res.get('plan') or [])}/10 · "
                     f"eval(union) {len(opens)}/{len(opens)}: {opens}")
    rj = {k: res.get(k) for k in ("executionDate", "signalAsOfDate", "runStatus", "proposedSequence",
          "proposedBatchId", "buyTradingDayIndex", "plannedSellTradingDayIndex", "buyCount", "sellCount",
          "allocatedCapital", "totalInvested", "cashReserve", "officialAvailableCashBefore",
          "officialAvailableCashAfterPreview", "holdingsMarketValuePreview", "missingEvalCodes",
          "lookAheadValidationPassed", "productionWriteCount", "officialStartDatePersisted")}
    rj["readOnlyUnchanged"] = unchanged
    lines += ["RESULT_JSON_BEGIN", json.dumps(rj, ensure_ascii=False, indent=2), "RESULT_JSON_END",
              f"EXIT_CODE={0 if res.get('runStatus') == 'COMPLETED' and unchanged else 4}"]
    log.write_text("\n".join(lines), encoding="utf-8")
    return str(log)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 OFFICIAL read-only dry-run 일일 자동화(저장 0)")
    ap.add_argument("--signal-date", default=None, help="신호 패키지 signalAsOfDate(생략 시 오늘=executionDate 후보 자동탐색)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    now_iso = C.now_kst().isoformat()
    today = C.today_kst_iso()

    if args.signal_date:
        pkg_dir = C.TEMP_ROOT / args.signal_date
    else:
        pkg_dir = find_ready_package_for_today(today)
    if not pkg_dir or not (pkg_dir / "manifest.json").exists():
        r = C.blocked_report("DRY_RUN", "BLOCKED_SIGNAL_PACKAGE_MISSING", today,
                             f"no READY signal package for executionDate {today}", now=now_iso,
                             recommended_fix="magic_daily_signal.py 먼저 실행 / --signal-date 지정")
        C.write_json_report(C.REPORTS_DIR / f"dry-run-{today}.json", r)
        print(json.dumps(r, ensure_ascii=False, indent=2) if args.json else f"[DRY_RUN] {r['status']} {r.get('blockedCode')}")
        return 2

    manifest = json.loads((pkg_dir / "manifest.json").read_text(encoding="utf-8"))
    rankings = json.loads((pkg_dir / "rankings.json").read_text(encoding="utf-8"))
    execution_date = manifest.get("nextExecutionDateCandidate")
    canonical = json.loads(C.CANONICAL_PATH.read_text(encoding="utf-8"))

    # executionDate 개장 전이면 시가 없음 → WAIT(에러 아님)
    if C.now_kst() < C.market_open_dt(execution_date):
        r = {"status": "WAIT_MARKET_OPEN", "phase": "DRY_RUN", "executionDate": execution_date,
             "signalAsOfDate": manifest.get("signalAsOfDate"), "noFakeTrade": True,
             "reason": f"now < {execution_date} 09:00 (개장 전, 시가 미확정)", "createdAt": now_iso}
        C.write_json_report(C.REPORTS_DIR / f"dry-run-{execution_date}.json", r)
        print(json.dumps(r, ensure_ascii=False, indent=2) if args.json else f"[DRY_RUN] WAIT_MARKET_OPEN {execution_date}")
        return 0

    sha_before = _sha_file(C.CANONICAL_PATH)
    try:
        cal = _call_timeout(W.build_krx_calendar, PYKRX_TIMEOUT_S, "build_krx_calendar", execution_date)
        codes = sorted({str(t["code"]).zfill(6) for t in rankings["top10"]}
                       | {str(l["code"]).zfill(6) for l in (canonical.get("itemLots") or []) if l.get("status") == "OPEN"})
        opens_all = _call_timeout(W.fetch_open_prices_pykrx, PYKRX_TIMEOUT_S, "fetch_open_prices_pykrx", execution_date, codes)
    except TimeoutError as te:
        r = C.blocked_report("DRY_RUN", "BLOCKED_NETWORK_TIMEOUT", execution_date, str(te),
                             execution_date=execution_date, signal_as_of=manifest.get("signalAsOfDate"), now=now_iso)
        C.write_json_report(C.REPORTS_DIR / f"dry-run-{execution_date}.json", r)
        print(json.dumps(r, ensure_ascii=False, indent=2) if args.json else f"[DRY_RUN] BLOCKED_NETWORK_TIMEOUT")
        return 2

    res = dry_run_core(canonical, manifest, rankings, cal, opens_all, now_iso=now_iso)
    unchanged = _sha_file(C.CANONICAL_PATH) == sha_before
    res["readOnlyUnchanged"] = unchanged
    if res.get("status") == "COMPLETED":
        res["logPath"] = write_dry_run_log(res, unchanged=unchanged)
    C.write_json_report(C.REPORTS_DIR / f"dry-run-{execution_date}.json", res)

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        print(f"[DRY_RUN {execution_date}] status={res['status']} seq={res.get('proposedSequence')} "
              f"batch={res.get('proposedBatchId')} BUY={res.get('buyCount')} "
              f"unchanged={unchanged} {res.get('blockedCode','')}")
    return 0 if res.get("status") == "COMPLETED" and unchanged else 2


if __name__ == "__main__":
    sys.exit(main())
