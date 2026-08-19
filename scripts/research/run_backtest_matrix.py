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
    benchmark_series, evaluate, load_names, load_snapshots, official_index_series,
    rank_universe, run_backtest,
)


def build_benchmarks(snaps, dates, names):
    """공정 benchmark 묶음 — 전략과 **같은 생존조건**(선정시점 필터·상장폐지 청산)을 쓴다.
    공식 지수는 산출주체가 달라 별도 항목으로 분리한다(혼동 금지)."""
    out = {}
    for key, kw in (
        ("EQUAL_MONTHLY", dict(weighting="EQUAL", rebalance="MONTHLY")),
        ("EQUAL_MONTHLY_COST", dict(weighting="EQUAL", rebalance="MONTHLY", cost_bps=15.0, sell_tax_bps=20.0)),
        ("EQUAL_BUY_HOLD", dict(weighting="EQUAL", rebalance="BUY_HOLD")),
        ("CAP_MONTHLY", dict(weighting="CAP", rebalance="MONTHLY")),
        ("EQUAL_MONTHLY_DELIST100", dict(weighting="EQUAL", rebalance="MONTHLY", delist_haircut=1.0)),
    ):
        b = benchmark_series(snaps, dates, names, **kw)
        out[key] = {k: b[k] for k in ("cagr", "final", "years", "delistEvents",
                                      "delistWeightSum", "turnoverPerYear", "weighting",
                                      "rebalance", "costBps", "sellTaxBps", "delistHaircut")}
    for mkt in ("KOSPI", "KOSDAQ"):
        b = benchmark_series(snaps, dates, names, market=mkt)
        out[f"EQUAL_MONTHLY_{mkt}"] = {k: b[k] for k in ("cagr", "final", "delistEvents", "turnoverPerYear")}
        o = official_index_series(dates, mkt)
        if o:
            out[f"OFFICIAL_{mkt}"] = {"cagr": o["cagr"], "final": o["final"],
                                      "source": o["source"], "note": o["note"]}
    return out


def regime_windows(snaps, dates, names, *, window=24):
    """bull / bear / sideways 구간 분류 — 공정 benchmark(동일가중 월리밸) 자체 수익률로 나눈다.
    전략 성과로 국면을 정의하면 순환논리가 되므로 시장 기준으로만 분류한다."""
    b = benchmark_series(snaps, dates, names)
    ser = {x["date"]: x["index"] for x in b["series"]}
    out = []
    for i in range(0, len(dates) - window, window):
        sub = dates[i:i + window + 1]
        a, z = ser.get(sub[0]), ser.get(sub[-1])
        if not a or not z or a <= 0:
            continue
        ann = (z / a) ** (12.0 / window) - 1.0
        regime = "BULL" if ann > 0.10 else ("BEAR" if ann < -0.05 else "SIDEWAYS")
        out.append({"start": sub[0], "end": sub[-1], "benchAnnual": ann, "regime": regime,
                    "dates": sub})
    return out

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
        # 코드 결함과 데이터 부족을 **구분**한다.
        #   랭킹이 비는 것은 1999~2006 구간의 BPS 결손 때문이지 코드 버그가 아니다(데이터 문서와 일치).
        #   그래서 코드 검사는 '유효한 fundamental 이 있는 스냅샷'으로 하고,
        #   데이터 적합성은 별도 판정(WAIT_INSUFFICIENT_DATA)으로 낸다.
        checks = []
        usable = [d for d in all_dates if len(rank_universe(snaps[d], names)) > 0]
        meta["usableSnapshots"] = len(usable)
        meta["usableFirst"] = usable[0] if usable else None
        meta["usableLast"] = usable[-1] if usable else None
        usable_span = contiguous_span(usable)
        meta["usableContiguousMonths"] = len(usable_span)

        if usable:
            probe = usable[-1]
            r0 = rank_universe(snaps[probe], names)
            checks.append({"check": "ranking_nonempty", "pass": len(r0) > 0,
                           "n": len(r0), "probeDate": probe})
            checks.append({"check": "ranking_deterministic",
                           "pass": [x["ticker"] for x in r0] ==
                                   [x["ticker"] for x in rank_universe(snaps[probe], names)]})
            checks.append({"check": "no_unlisted_ticker_in_rank",
                           "pass": all(t["ticker"] in snaps[probe] for t in r0)})
            checks.append({"check": "combined_rank_is_sum",
                           "pass": all(t["combinedRank"] == t["profitabilityRank"] + t["valueRank"]
                                       for t in r0)})
        else:
            checks.append({"check": "ranking_nonempty", "pass": False,
                           "note": "유효 fundamental 스냅샷 0 — 데이터 문제"})

        if len(dates) >= 15:
            a = run_backtest(snaps, names, dates=dates, hold_months=12, n_stocks=20)
            b = run_backtest(snaps, names, dates=dates, hold_months=12, n_stocks=20)
            checks.append({"check": "deterministic",
                           "pass": bool(a and b and a["navSeries"] == b["navSeries"])})
            checks.append({"check": "evaluate_ok", "pass": evaluate(a) is not None if a else False})
            checks.append({"check": "nav_positive",
                           "pass": bool(a and all(x["nav"] > 0 for x in a["navSeries"]))})

        code_ok = all(c.get("pass") for c in checks)
        # 연구를 실제로 돌리려면 '유효 데이터가 연속 24개월 이상' 필요하다.
        data_ok = len(usable_span) >= 24
        out = {"meta": meta, "selftest": checks,
               "codeVerdict": "PASS" if code_ok else "FAIL",
               "dataVerdict": "READY" if data_ok else "WAIT_INSUFFICIENT_DATA",
               "dataNote": (f"유효 연속구간 {len(usable_span)}개월 (<24) — "
                            f"1999~2006 은 BPS 결손으로 랭킹 산출 불가(설계대로). "
                            f"2007-01~2026-08 수집이 끝나야 baseline/matrix/robust 실행 가능."),
               "verdict": "PASS" if (code_ok and data_ok) else
                          ("WAIT_INSUFFICIENT_DATA" if code_ok else "FAIL")}
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
        bm = build_benchmarks(snaps, dates, names)
        fair = bm["EQUAL_MONTHLY"]["cagr"]
        for r in rows:
            r["benchmarkFairCagr"] = fair
            r["excessVsFair"] = (r["twrCagr"] - fair) if r.get("twrCagr") is not None else None
        out = {"meta": meta, "baseline": rows, "benchmarks": bm,
               "benchmarkNote": "EQUAL_MONTHLY = 전략과 동일 생존조건(선정시점 필터·상장폐지 청산)의 "
                                "동일가중 시장 포트폴리오. 공식 지수(OFFICIAL_*)는 산출주체가 달라 별도."}
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
        bm = build_benchmarks(snaps, dates, names)
        fair = bm["EQUAL_MONTHLY"]["cagr"]
        for r in rows:
            r["benchmarkFairCagr"] = fair
            r["excessVsFair"] = (r["twrCagr"] - fair) if r.get("twrCagr") is not None else None
        # ── plateau 판정 — 인접 격자(보유기간 ±1칸, 종목수 ±1칸)와 비교 ──────────────
        grid = {}
        for r in rows:
            p = r["params"]
            if r.get("twrCagr") is not None and p.get("contribution") == "LUMP_SUM" \
               and r["tag"].endswith("_LUMP"):
                grid[(p["hold_months"], p["n_stocks"])] = r["twrCagr"]
        plateau = []
        for (h, n), v in sorted(grid.items()):
            hi, ni = HOLD_MONTHS.index(h), N_STOCKS.index(n)
            neigh = []
            for dh, dn in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                a, b2 = hi + dh, ni + dn
                if 0 <= a < len(HOLD_MONTHS) and 0 <= b2 < len(N_STOCKS):
                    k = (HOLD_MONTHS[a], N_STOCKS[b2])
                    if k in grid:
                        neigh.append(grid[k])
            if not neigh:
                continue
            worst = min(neigh)
            # 자기만 좋고 이웃이 급락하면 단독 peak(과최적화 의심)
            is_peak = v > fair and worst < fair and (v - worst) > 0.05
            plateau.append({"hold": h, "n": n, "cagr": v, "neighborMin": worst,
                            "neighborMean": sum(neigh) / len(neigh),
                            "beatsFair": v > fair,
                            "neighborsBeatFair": all(x > fair for x in neigh),
                            "isolatedPeak": is_peak})
        robust = [p for p in plateau if p["beatsFair"] and p["neighborsBeatFair"]]
        peaks = [p for p in plateau if p["isolatedPeak"]]
        out = {"meta": meta, "matrix": rows, "benchmarks": bm,
               "plateau": {"cells": plateau, "robustCells": robust, "isolatedPeaks": peaks,
                           "robustCount": len(robust), "peakCount": len(peaks),
                           "verdict": ("ROBUST_CANDIDATE" if len(robust) >= 3 else
                                       "WEAK_CANDIDATE" if len(robust) >= 1 else
                                       "OVERFIT_PEAK" if peaks else "NO_ROBUST_STRATEGY")}}
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
        bm = build_benchmarks(snaps, dates, names)
        fair = bm["EQUAL_MONTHLY"]["cagr"]
        for r in rows:
            r["benchmarkFairCagr"] = fair
            r["excessVsFair"] = (r["twrCagr"] - fair) if r.get("twrCagr") is not None else None
        # ── bull / bear / sideways — 시장 국면별로 전략이 견디는가 ──────────────────
        regimes = []
        for w in regime_windows(snaps, dates, names, window=24):
            res, ev = one(snaps, names, w["dates"], **base)
            # 같은 구간의 공정 benchmark 연율
            bsub = benchmark_series(snaps, w["dates"], names)
            regimes.append({"start": w["start"], "end": w["end"], "regime": w["regime"],
                            "benchAnnual": bsub["cagr"],
                            "stratAnnual": (ev["twrCagr"] if ev else None),
                            "excess": ((ev["twrCagr"] - bsub["cagr"]) if ev and ev.get("twrCagr") is not None else None),
                            "stratMdd": (ev["mdd"] if ev else None)})
        byreg = {}
        for r in regimes:
            if r["excess"] is None:
                continue
            byreg.setdefault(r["regime"], []).append(r["excess"])
        regime_summary = {k: {"windows": len(v), "meanExcess": sum(v) / len(v),
                              "winRate": sum(1 for x in v if x > 0) / len(v)}
                          for k, v in byreg.items()}
        beat = [r for r in rows if r.get("excessVsFair") is not None and r["excessVsFair"] > 0]
        out = {"meta": meta, "robustness": rows, "benchmarks": bm,
               "regimes": regimes, "regimeSummary": regime_summary,
               "beatsFairCount": len(beat), "totalRows": len(rows)}

    txt = json.dumps(out, ensure_ascii=False, indent=2, default=float)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(txt, encoding="utf-8")
        print(f"written {args.out}", file=sys.stderr)
        # --out 이 있으면 결과 전문을 stdout 으로 다시 뱉지 않는다.
        #   Windows 콘솔 기본 코덱(cp949)이 '—' 같은 문자를 못 써서 UnicodeEncodeError 로 죽고,
        #   **파일은 정상 저장됐는데 exit 1** 이 됐다. 그러면 자동 연구 체인이 이를
        #   HARD_FAILURE 로 오판한다(pit_pipeline_state.run_research 는 returncode 로 판정).
        print(json.dumps({"written": args.out, "bytes": len(txt)}, ensure_ascii=False))
        return 0
    # 콘솔 직접 출력 경로에서도 인코딩 때문에 죽지 않게 한다.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    try:
        print(txt)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(txt.encode("utf-8", errors="replace"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
