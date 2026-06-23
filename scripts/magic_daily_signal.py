#!/usr/bin/env python3
"""마법공식 A티어 — 종가 신호 패키지 일일 자동 생성 (Phase 45-AUTO2).

거래일 장마감(15:30 KST) 후 signalAsOfDate 종가 신호 패키지를 TEMP에만 생성한다.
canonical/public/REPO1 write 0. receipt/apply 미실행. 기존 build_magic_signal_package 재사용(복제 0).
비거래일 self-skip, 장마감 전 BLOCKED, 기존 READY면 ALREADY_PREPARED, 해시 충돌 BLOCKED.

예) python scripts/magic_daily_signal.py            # 오늘(KST) 거래일 종가 신호
    python scripts/magic_daily_signal.py --signal-date 2026-06-24
"""
from __future__ import annotations

import argparse
import json
import sys

import build_magic_signal_package as B
import magic_daily_common as C


def run_signal(signal_date: str, *, now=None, output_dir=None, build_payload_fn=None, ranking_fn=None) -> dict:
    from datetime import datetime
    now = now or C.now_kst().isoformat()
    try:
        now_dt = datetime.fromisoformat(now)
    except ValueError:
        now_dt = C.now_kst()
    out_dir = output_dir or str(B._default_output_dir(signal_date))
    phase = "SIGNAL"

    # 1) 거래일 self-skip(큐레이션 KRX 캘린더, 네트워크 0)
    if not C.is_krx_trading_day(signal_date):
        return {"status": "SELF_SKIPPED_NON_TRADING_DAY", "phase": phase, "date": signal_date,
                "signalAsOfDate": signal_date, "reason": f"{signal_date} not a KRX trading day",
                "autoStopped": True, "noFakeTrade": True, "createdAt": now}

    # 2) 장마감 전이면 BLOCKED(신호는 종가 확정 후에만)
    if now_dt <= C.market_close_dt(signal_date):
        return C.blocked_report(phase, "BLOCKED_MARKET_NOT_CLOSED", signal_date,
                                f"now {now} <= marketClose {C.market_close_dt(signal_date).isoformat()}",
                                signal_as_of=signal_date, now=now,
                                recommended_fix="장마감(15:30 KST) 이후 재실행")

    # 3) 신호 패키지 생성(TEMP only). build_payload_fn/ranking_fn은 테스트 주입용.
    kw = {"now": now, "output_dir": out_dir}
    if build_payload_fn is not None:
        kw["build_payload_fn"] = build_payload_fn
    if ranking_fn is not None:
        kw["ranking_fn"] = ranking_fn
    res = B.build_signal_package(signal_date, **kw)
    ps = res.get("packageStatus")

    if ps in (B.READY, B.ALREADY_PREPARED):
        norm = "READY" if ps == B.READY else "ALREADY_PREPARED"
        return {"status": norm, "phase": phase, "date": signal_date,
                "signalAsOfDate": res.get("signalAsOfDate"), "packageStatus": ps,
                "universeBaseDate": res.get("universeBaseDate"),
                "nextExecutionDateCandidate": res.get("nextExecutionDateCandidate"),
                "top10Count": res.get("top10Count"), "universeCount": res.get("universeCount"),
                "universeSha256": res.get("universeSha256"), "rankingsSha256": res.get("rankingsSha256"),
                "productionWriteCount": res.get("productionWriteCount", 0),
                "publicCopyCount": res.get("publicCopyCount", 0),
                "outputDir": res.get("outputDir", out_dir),
                "autoStopped": False, "noFakeTrade": True, "createdAt": now}

    # 4) 그 외는 BLOCKED 매핑
    code_map = {B.BLOCKED_PACKAGE_CONFLICT: "BLOCKED_PACKAGE_CONFLICT",
                B.BLOCKED_SIGNAL_MARKET_NOT_CLOSED: "BLOCKED_MARKET_NOT_CLOSED",
                B.BLOCKED_SIGNAL_UNIVERSE_NOT_READY: "BLOCKED_SIGNAL_UNIVERSE_NOT_READY"}
    code = code_map.get(ps, ps or "BLOCKED_SIGNAL_GENERATION")
    return C.blocked_report(phase, code, signal_date, res.get("runReason") or "signal package not READY",
                            signal_as_of=signal_date, now=now, evidence=[out_dir],
                            recommended_fix="signal 입력/거래일/유니버스 baseDate 확인")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 종가 신호 패키지 일일 자동 생성(TEMP only)")
    ap.add_argument("--signal-date", default=None, help="YYYY-MM-DD (생략 시 오늘 KST)")
    ap.add_argument("--now", default=None, help="tz-aware ISO(테스트용)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    signal_date = args.signal_date or C.today_kst_iso()
    r = run_signal(signal_date, now=args.now)
    C.write_json_report(C.REPORTS_DIR / f"signal-{signal_date}.json", r)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[SIGNAL {signal_date}] status={r['status']} "
              f"nextExec={r.get('nextExecutionDateCandidate')} "
              f"uniSha={(r.get('universeSha256') or '')[:12]} reason={r.get('reason','')}")
    # self-skip / READY / ALREADY_PREPARED → 0, BLOCKED → 2
    return 0 if r["status"] in ("READY", "ALREADY_PREPARED", "SELF_SKIPPED_NON_TRADING_DAY") else 2


if __name__ == "__main__":
    sys.exit(main())
