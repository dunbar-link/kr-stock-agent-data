#!/usr/bin/env python3
"""PHASE 3~5 — baseline · coarse parameter matrix · robustness 실행기.

네트워크를 쓰지 않는다. `_cache/pit-snapshots/` 의 point-in-time 스냅샷만 읽는다.
(스냅샷 구축은 build_pit_snapshots.py 담당 — 관심사 분리)

사용:
  python scripts/research/run_backtest_matrix.py --mode baseline
  python scripts/research/run_backtest_matrix.py --mode matrix   --out reports/research/matrix.json
  python scripts/research/run_backtest_matrix.py --mode robust   --out reports/research/robust.json
  python scripts/research/run_backtest_matrix.py --mode selftest

안전: read-only + --out 리포트 1개. 실주문 0 · 브로커 0 · canonical 미접근.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import (  # noqa: E402
    benchmark_series, evaluate, load_names, load_snapshots, rank_universe, run_backtest,
)

HOLD_MONTHS = [2, 3, 6, 9, 12, 18, 24, 36]
N_STOCKS = [10, 20, 30, 40, 50]
CONTRIBUTIONS = ["LUMP_SUM", "MONTHLY_DCA", "QUARTERLY_DCA", "INITIAL_PLUS_MONTHLY"]


def contiguous_span(dates):
    """스냅샷이 연속인 최장 구간(월 단위). 비어 있는 달이 있으면 그 앞에서 끊는다."""
    if not dates:
        return []
    best, cur = [], [dates[0]]
    for prev, d in zip(dates, dates[1:]):
        py, pm = int(prev[:4]), int(prev[5:7])
        cy, cm = int(d[:4]), int(d[5:7])
        if (cy - py) * 12 + (cm - pm) == 1:
            cur.append(d)
        else:
            if len(cur) > len(best):
                best = cur
            cur = [d]
    return cur if len(cur) > len(best) else best


def summarize(tag, params, res, ev):
    return {"tag": tag, "params": params,
            "years": None if not ev else round(ev["years"], 2),
            "start": None if not ev else ev["start"], "end": None if not ev else ev["end"],
            "twrCagr": None if not ev else ev["twrCagr"], "irr": None if not ev else ev["irr"],
            "mdd": None if not ev else ev["mdd"],
            "maxUnderwaterMonths": None if not ev else ev["maxUnderwaterMonths"],
            "vol": None if not ev else ev["volAnnual"], "sharpe": None if not ev else ev["sharpe"],
            "sortino": None if not ev else ev["sortino"],
            "rolling3y": None if not ev else ev["rolling3y"],
            "trades": None if not res else res["trades"],
            "costPctOfContrib": None if not ev else ev["costPctOfContrib"],
            "turnoverPerYear": None if not ev else ev["turnoverPerYear"],
            "delistEvents": None if not res else res["delistEvents"],
            "finalNav": None if not ev else ev["finalNav"],
            "totalContributed": None if not ev else ev["totalContributed"]}


def one(snaps, names, dates, **kw):
    res = run_backtest(snaps, names, dates=dates, **kw)
    if not res:
        return None, None
    ev = evaluate(res, contribution=kw.get("contribution", "LUMP_SUM"))
    return res, ev


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="baseline",
                    choices=["baseline", "matrix", "robust", "selftest", "coverage"])
    ap.add_argument("--out", default="")
    ap.add_argument("--start", default="")
    ap.add_argument("--end", default="")
    args = ap.parse_args(argv)

    snaps = load_snapshots()
    names = load_names()
    all_dates = sorted(snaps.keys())
    dates = contiguous_span(all_dates)
    if args.start:
        dates = [d for d in dates if d >= args.start]
    if args.end:
        dates = [d for d in dates if d <= args.end]

    meta = {"snapshotsTotal": len(all_dates),
            "snapshotFirst": all_dates[0] if all_dates else None,
            "snapshotLast": all_dates[-1] if all_dates else None,
            "contiguousMonths": len(dates),
            "backtestStart": dates[0] if dates else None,
            "backtestEnd": dates[-1] if dates else None,
            "namesLoaded": len(names),
            "realOrderCount": 0, "brokerApiCallCount": 0, "networkCalls": 0}

    if args.mode == "coverage":
        rows = []
        for d in all_dates:
            ranked = rank_universe(snaps[d], names)
            rows.append({"date": d, "snapshotRows": len(snaps[d]), "eligible": len(ranked),
                         "top1": ranked[0]["ticker"] if ranked else None})
        out = {"meta": meta, "coverage": rows}
    elif args.mode == "selftest":
        # 결정성 + point-in-time 위생 검사(네트워크 0, 같은 입력 → 같은 출력)
        checks = []
        if len(dates) >= 15:
            a = run_backtest(snaps, names, dates=dates, hold_months=12, n_stocks=20)
            b = run_backtest(snaps, names, dates=dates, hold_months=12, n_stocks=20)
            checks.append({"check": "deterministic",
                           "pass": bool(a and b and a["navSeries"] == b["navSeries"])})
            r0 = rank_universe(snaps[dates[0]], names)
            checks.append({"check": "ranking_nonempty", "pass": len(r0) > 0, "n": len(r0)})
            checks.append({"check": "no_future_ticker_in_first_rank",
                           "pass": all(t["ticker"] in snaps[dates[0]] for t in r0[:20])})
            ev = evaluate(a) if a else None
            checks.append({"check": "evaluate_ok", "pass": ev is not None})
            checks.append({"check": "nav_positive",
                           "pass": bool(a and all(x["nav"] > 0 for x in a["navSeries"]))})
        else:
            checks.append({"check": "enough_snapshots", "pass": False,
                           "note": f"contiguous months={len(dates)} (<15)"})
        out = {"meta": meta, "selftest": checks,
               "verdict": "PASS" if all(c.get("pass") for c in checks) else "FAIL"}
    elif args.mode == "baseline":
        rows = []
        # 원전에 가장 가까운 기준선: 20종목 · 12개월 보유 · 거치식 연 1회 교체
        for tag, kw in [
            ("BASELINE_GREENBLATT_LIKE", dict(hold_months=12, n_stocks=20, contribution="LUMP_SUM")),
            ("BASELINE_30", dict(hold_months=12, n_stocks=30, contribution="LUMP_SUM")),
            ("WABABA_50D_LIKE", dict(hold_months=2, n_stocks=10, contribution="LUMP_SUM")),
            ("WABABA_50D_LIKE_3M", dict(hold_months=3, n_stocks=10, contribution="LUMP_SUM")),
            ("DCA_12M_20", dict(hold_months=12, n_stocks=20, contribution="MONTHLY_DCA")),
        ]:
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(tag, kw, res, ev))
        bench = benchmark_series(snaps, dates, names)
        out = {"meta": meta, "baseline": rows,
               "benchmarkEqualWeight": {"first": bench[0], "last": bench[-1]} if bench else None}
    elif args.mode == "matrix":
        rows = []
        for h in HOLD_MONTHS:
            for n in N_STOCKS:
                kw = dict(hold_months=h, n_stocks=n, contribution="LUMP_SUM")
                res, ev = one(snaps, names, dates, **kw)
                rows.append(summarize(f"H{h}_N{n}_LUMP", kw, res, ev))
        for c in CONTRIBUTIONS:
            for h in (6, 12, 24):
                kw = dict(hold_months=h, n_stocks=20, contribution=c)
                res, ev = one(snaps, names, dates, **kw)
                rows.append(summarize(f"H{h}_N20_{c}", kw, res, ev))
        out = {"meta": meta, "matrix": rows}
    else:  # robust
        rows = []
        base = dict(hold_months=12, n_stocks=20, contribution="LUMP_SUM")
        for cost, tax, tag in [(0, 0, "COST_ZERO"), (15, 20, "COST_BASE"), (30, 20, "COST_HIGH")]:
            kw = dict(base, cost_bps=cost, sell_tax_bps=tax)
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(f"{tag}", kw, res, ev))
        for hc, tag in [(0.0, "DELIST_0"), (0.3, "DELIST_30"), (1.0, "DELIST_100")]:
            kw = dict(base, delist_haircut=hc)
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(tag, kw, res, ev))
        for mc, tag in [(0, "MCAP_0"), (300, "MCAP_300"), (1000, "MCAP_1000"), (3000, "MCAP_3000")]:
            kw = dict(base, min_market_cap=mc)
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(tag, kw, res, ev))
        for mkt in ("COMBINED", "KOSPI", "KOSDAQ"):
            kw = dict(base, market=mkt)
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(f"MKT_{mkt}", kw, res, ev))
        for ef, tag in [(True, "EXFIN_ON"), (False, "EXFIN_OFF")]:
            kw = dict(base, exclude_financial=ef)
            res, ev = one(snaps, names, dates, **kw)
            rows.append(summarize(tag, kw, res, ev))
        # 시작 월 민감도: 12개 시작 시점을 밀어가며 같은 규칙 재실행
        for off in range(0, min(12, max(0, len(dates) - 40))):
            sub = dates[off:]
            res, ev = one(snaps, names, sub, **base)
            rows.append(summarize(f"STARTOFFSET_{off}", dict(base, startOffset=off), res, ev))
        # subperiod: 5년 단위 분할
        for i in range(0, len(dates) - 60, 60):
            sub = dates[i:i + 61]
            res, ev = one(snaps, names, sub, **base)
            rows.append(summarize(f"SUB_{sub[0][:7]}_{sub[-1][:7]}", dict(base), res, ev))
        out = {"meta": meta, "robustness": rows}

    txt = json.dumps(out, ensure_ascii=False, indent=2, default=float)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(txt, encoding="utf-8")
        print(f"written {args.out}", file=sys.stderr)
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
