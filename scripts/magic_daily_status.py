#!/usr/bin/env python3
"""마법공식 A티어 — 일일 운용 상태 요약 (Phase 45-AUTO2).

canonical 상태 + 최근 signal/dry-run/BLOCKED 리포트를 read-only로 종합해, 대장이 복붙 없이
PASS/BLOCKED/다음 action만 보게 한다. write 0(TEMP report만). 장부/public 미접근.

다음 action: WAIT_MARKET_CLOSE / SIGNAL_READY / DRY_RUN_READY / APPROVAL_TICKET_REQUIRED /
            BLOCKED_NEEDS_REVIEW / NO_ACTION

예) python scripts/magic_daily_status.py
"""
from __future__ import annotations

import argparse
import json
import sys

import magic_daily_common as C


def _is_blocked(rep) -> bool:
    return bool(rep) and isinstance(rep.get("data"), dict) and str(rep["data"].get("status", "")).startswith("BLOCKED")


def classify(today: str, canonical: dict, signal, dryrun, *, now_dt=None) -> tuple[str, str, str]:
    """(overall, nextAction, hint). now_dt 주입 가능(테스트)."""
    now_dt = now_dt or C.now_kst()
    if not C.is_krx_trading_day(today):
        return "WAIT", "NO_ACTION", "오늘은 KRX 비거래일 — 자동화 작업 없음"
    if now_dt <= C.market_close_dt(today):
        return "WAIT", "WAIT_MARKET_CLOSE", "장마감(15:30 KST) 후 magic_daily_signal 자동 실행 예정"
    sig = signal.get("data") if signal else None
    dr = dryrun.get("data") if dryrun else None
    if _is_blocked(signal) or _is_blocked(dryrun):
        b = (signal if _is_blocked(signal) else dryrun)["data"]
        return "BLOCKED", "BLOCKED_NEEDS_REVIEW", f"{b.get('phase')} {b.get('blockedCode')}: {b.get('reason')}"
    if dr and dr.get("status") == "COMPLETED":
        return ("PENDING_APPROVAL", "APPROVAL_TICKET_REQUIRED",
                f"dry-run COMPLETED(seq {dr.get('proposedSequence')}, {dr.get('proposedBatchId')}). "
                "장부 저장은 approval-ticket+사람 승인 필요(자동 금지)")
    if sig and sig.get("status") in ("READY", "ALREADY_PREPARED"):
        return ("PASS", "DRY_RUN_READY",
                f"신호 READY(nextExec {sig.get('nextExecutionDateCandidate')}). "
                "다음 거래일 개장 후 magic_daily_dry_run 자동 실행 예정")
    return "PASS", "SIGNAL_READY", "신호 미생성 — magic_daily_signal 실행 필요(또는 장마감 대기)"


def build_status(today: str) -> dict:
    canon = C.load_canonical_summary()
    signal = C.latest_report("signal")
    dryrun = C.latest_report("dry-run")
    overall, action, hint = classify(today, canon, signal, dryrun)
    sig, dr = (signal or {}).get("data"), (dryrun or {}).get("data")
    return {
        "date": today, "overall": overall, "nextAction": action, "nextManualHint": hint,
        "canonical": canon,
        "signal": ({"status": sig.get("status"), "signalAsOfDate": sig.get("signalAsOfDate"),
                    "nextExecutionDateCandidate": sig.get("nextExecutionDateCandidate"),
                    "universeSha256": (sig.get("universeSha256") or "")[:16]} if sig else None),
        "dryRun": ({"status": dr.get("status"), "proposedSequence": dr.get("proposedSequence"),
                    "proposedBatchId": dr.get("proposedBatchId"), "buyCount": dr.get("buyCount"),
                    "blockedCode": dr.get("blockedCode"),
                    "readOnlyUnchanged": dr.get("readOnlyUnchanged")} if dr else None),
        "signalReportPath": signal.get("path") if signal else None,
        "dryRunReportPath": dryrun.get("path") if dryrun else None,
        "createdAt": C.now_kst().isoformat(),
    }


def to_markdown(s: dict) -> str:
    c = s["canonical"]
    lines = [f"# 마법공식 OFFICIAL 일일 상태 — {s['date']}", "",
             f"- **종합**: {s['overall']} · **다음 action**: {s['nextAction']}",
             f"- 안내: {s['nextManualHint']}", "",
             "## canonical",
             f"- seq {c.get('officialSequence')} · tradingDayIndex {c.get('officialTradingDayIndex')} · "
             f"batch {c.get('batchCount')} · BUY {c.get('buyCount')} / SELL {c.get('sellCount')} · "
             f"missed {c.get('missedRunCount')}",
             f"- availCash {c.get('officialAvailableCash')} · execCal {c.get('officialExecutionCalendar')} · "
             f"sha {c.get('canonicalSha256')}", ""]
    if s["signal"]:
        sg = s["signal"]
        lines += ["## 최근 signal", f"- {sg['status']} · signalAsOf {sg['signalAsOfDate']} · "
                  f"nextExec {sg['nextExecutionDateCandidate']} · uniSha {sg['universeSha256']}", ""]
    if s["dryRun"]:
        d = s["dryRun"]
        lines += ["## 최근 dry-run", f"- {d['status']} · seq {d.get('proposedSequence')} · "
                  f"{d.get('proposedBatchId')} · BUY {d.get('buyCount')} · "
                  f"unchanged {d.get('readOnlyUnchanged')} {d.get('blockedCode') or ''}", ""]
    lines += ["> 장부 저장·public 반영·git push·배포는 자동화하지 않음(approval-ticket + 사람 승인)."]
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 일일 운용 상태 요약(read-only)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    today = C.today_kst_iso()
    s = build_status(today)
    C.write_json_report(C.REPORTS_DIR / f"daily-status-{today}.json", s)
    md = to_markdown(s)
    (C.REPORTS_DIR / f"daily-status-{today}.md").write_text(md, encoding="utf-8")
    if args.json:
        print(json.dumps(s, ensure_ascii=False, indent=2))
    else:
        print(f"[STATUS {today}] {s['overall']} → {s['nextAction']}")
        print(f"  {s['nextManualHint']}")
        print(f"  canonical seq={s['canonical'].get('officialSequence')} "
              f"idx={s['canonical'].get('officialTradingDayIndex')} "
              f"asset_cash={s['canonical'].get('officialAvailableCash')}")
    return 0 if s["overall"] != "BLOCKED" else 2


if __name__ == "__main__":
    sys.exit(main())
