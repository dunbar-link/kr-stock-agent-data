#!/usr/bin/env python3
"""R6 실행기 — coarse matrix → 후보 축소 → deep robustness.

WABABA-CAPITAL-DEPLOYMENT-AND-HOLDING-RULE-MATRIX-R6

원칙
  - 모든 조합에 비싼 robustness 를 돌리지 않는다. coarse → 상위 후보만 심층.
  - 공정 benchmark(R5 수정판, survivorship-free)를 그대로 재사용한다.
  - 최고 CAGR 1개를 뽑지 않는다. 인접 parameter 안정성(plateau)을 본다.
  - 현행 LEGACY_50D 를 유리하게 만드는 설계를 하지 않는다(같은 격자에서 같은 조건으로 평가).

사용:
  python scripts/research/run_r6_matrix.py --mode audit    --out reports/research/r6-audit.json
  python scripts/research/run_r6_matrix.py --mode matrix   --out reports/research/r6-matrix.json
  python scripts/research/run_r6_matrix.py --mode deep     --out reports/research/r6-deep.json

안전: 네트워크 0 · canonical 미접근 · --out 리포트만 write.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import benchmark_series, load_names, load_snapshots  # noqa: E402
from deployment_engine import HOLD_TD_LABEL, evaluate, simulate  # noqa: E402
from run_backtest_matrix import build_benchmarks, contiguous_span, regime_windows  # noqa: E402

CAPITAL = 50_000_000
MONTHLY = 500_000

HOLDS = [2, 3, 6, 9, 12, 18, 24, 36]
NSTOCKS = [10, 20, 30, 40]
DEPLOYMENTS = [("LUMP_SUM", 1), ("STAGGERED_3", 3), ("STAGGERED_6", 6),
               ("STAGGERED_12", 12), ("STAGGERED_25", 25), ("STAGGERED_50", 50)]
REPLACEMENTS = ["MATURITY_REPLACE", "FIXED_MATURITY", "RANK_RETENTION", "PERIODIC_REBALANCE"]


def run(sn, nm, ds, tag, **kw):
    res = simulate(sn, nm, ds, **kw)
    if not res:
        return None
    ev = evaluate(res)
    if not ev:
        return None
    row = {"tag": tag, "params": {k: v for k, v in kw.items()
                                 if k in ("initial_capital", "deployment", "stagger_months",
                                          "monthly_amount", "buy_every", "n_stocks",
                                          "hold_months", "replacement", "rebalance_months",
                                          "market", "min_market_cap", "cost_bps",
                                          "sell_tax_bps", "delist_haircut")}}
    row.update({k: ev[k] for k in ("years", "twrCagr", "irr", "terminalWealth", "totalContributed",
                                   "totalReturnOnContrib", "mdd", "maxUnderwaterMonths", "vol",
                                   "sharpe", "sortino", "trades", "costPctOfContrib",
                                   "turnoverPerYear", "delistEvents", "idleCashRatioAvg",
                                   "idleCashRatioMax", "avgConcurrentPositions",
                                   "maxConcurrentPositions", "uniqueTickersEver",
                                   "rolling3y", "rolling5y")})
    row["holdTradingDaysApprox"] = HOLD_TD_LABEL.get(kw.get("hold_months"), "")
    return row


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="matrix", choices=["audit", "matrix", "deep"])
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)

    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    bm = build_benchmarks(sn, ds, nm)
    fair = bm["EQUAL_MONTHLY"]["cagr"]
    meta = {"start": ds[0], "end": ds[-1], "months": len(ds),
            "initialCapital": CAPITAL, "monthlyAmount": MONTHLY,
            "benchmarkFairCagr": fair, "networkCalls": 0,
            "realOrderCount": 0, "brokerApiCallCount": 0}

    if args.mode == "audit":
        # R5 가 실제로 무엇을 테스트했는지 축 단위로 판정
        try:
            r5 = json.loads(Path("reports/research/matrix-latest.json").read_text(encoding="utf-8"))
            r5rows = r5.get("matrix", [])
        except (OSError, ValueError):
            r5rows = []
        dims = {}
        for r in r5rows:
            for k, v in (r.get("params") or {}).items():
                dims.setdefault(k, set()).add(str(v))
        axes = {
            "holding period": ("TESTED", f"{len(dims.get('hold_months', []))}개 값"),
            "number of stocks": ("TESTED", f"{len(dims.get('n_stocks', []))}개 값"),
            "capital deployment (분할투입)": ("NOT_TESTED", "LUMP_SUM 은 t0 전액 일시투입뿐 — 5천만원을 시간축에 나누는 축이 없었다"),
            "purchase cadence (매수주기)": ("PARTIALLY_TESTED", "QUARTERLY_DCA 로 간접 3개월만. 매수주기 독립축 아님"),
            "rebalance / replacement": ("NOT_TESTED", "만기매도 후 즉시 재매수(MATURITY_REPLACE) 한 가지뿐"),
            "DCA / lump sum": ("TESTED", "LUMP_SUM/MONTHLY_DCA/QUARTERLY_DCA/INITIAL_PLUS_MONTHLY 4종"),
            "market (KOSPI/KOSDAQ/COMBINED)": ("PARTIALLY_TESTED", "matrix 에는 없고 robustness 3행에만"),
            "market-cap floor": ("PARTIALLY_TESTED", "matrix 에는 없고 robustness 4행에만"),
            "cost": ("PARTIALLY_TESTED", "robustness 3행(COST_ZERO/BASE/HIGH)"),
            "tax": ("PARTIALLY_TESTED", "매도 거래세 20bp 고정 — 축으로 변주하지 않음"),
            "initial capital 5천만원": ("NOT_TESTED", "R5 는 기본값 10,000,000원으로 실행됨(params 미기록)"),
            "idle cash 측정": ("NOT_TESTED", "현금비중 지표 자체가 없었다"),
            "concurrent positions / unique holdings": ("NOT_TESTED", "선정 종목수와 동시 보유 종목수를 구분하지 않았다"),
        }
        out = {"meta": meta, "r5RowCount": len(r5rows),
               "r5Dimensions": {k: sorted(v) for k, v in dims.items()},
               "axisAudit": {k: {"verdict": v[0], "note": v[1]} for k, v in axes.items()},
               "notTested": [k for k, v in axes.items() if v[0] == "NOT_TESTED"],
               "partiallyTested": [k for k, v in axes.items() if v[0] == "PARTIALLY_TESTED"]}

    elif args.mode == "matrix":
        rows = []
        # (1) 거치식 5천만원 — 투입방식 × 보유기간 (종목 20 고정)
        for dep, k in DEPLOYMENTS:
            for h in HOLDS:
                r = run(sn, nm, ds, f"DEP_{dep}_H{h}_N20", initial_capital=CAPITAL,
                        deployment=("LUMP_SUM" if dep == "LUMP_SUM" else "STAGGERED"),
                        stagger_months=k, n_stocks=20, hold_months=h,
                        replacement="MATURITY_REPLACE")
                if r:
                    r["deploymentLabel"] = dep
                    rows.append(r)
        # (2) 종목수 × 보유기간 (투입방식은 대표 2종)
        for dep, k in (("LUMP_SUM", 1), ("STAGGERED_12", 12)):
            for h in HOLDS:
                for n in NSTOCKS:
                    if n == 20:
                        continue          # (1)에서 이미 커버
                    r = run(sn, nm, ds, f"{dep}_H{h}_N{n}", initial_capital=CAPITAL,
                            deployment=("LUMP_SUM" if dep == "LUMP_SUM" else "STAGGERED"),
                            stagger_months=k, n_stocks=n, hold_months=h,
                            replacement="MATURITY_REPLACE")
                    if r:
                        r["deploymentLabel"] = dep
                        rows.append(r)
        # (3) 교체규칙 비교 (대표 보유기간 3종 × 종목 20)
        for rep in REPLACEMENTS:
            for h in (6, 12, 24):
                r = run(sn, nm, ds, f"REPL_{rep}_H{h}_N20", initial_capital=CAPITAL,
                        deployment="LUMP_SUM", n_stocks=20, hold_months=h,
                        replacement=rep, rebalance_months=h)
                if r:
                    r["deploymentLabel"] = "LUMP_SUM"
                    rows.append(r)
        # (4) 매수주기 비교
        for be in (1, 3):
            for h in (6, 12, 24):
                r = run(sn, nm, ds, f"CADENCE_EVERY{be}M_H{h}_N20", initial_capital=CAPITAL,
                        deployment="STAGGERED", stagger_months=12, buy_every=be,
                        n_stocks=20, hold_months=h, replacement="MATURITY_REPLACE")
                if r:
                    r["deploymentLabel"] = "STAGGERED_12"
                    rows.append(r)
        # (5) 적립식 / 초기+적립 (현금흐름이 달라 별도 집계)
        for h in (6, 12, 24):
            for n in (20, 30):
                r = run(sn, nm, ds, f"DCA_H{h}_N{n}", initial_capital=0,
                        deployment="NONE", monthly_amount=MONTHLY, n_stocks=n,
                        hold_months=h, replacement="MATURITY_REPLACE")
                if r:
                    r["deploymentLabel"] = "MONTHLY_DCA"
                    rows.append(r)
                r2 = run(sn, nm, ds, f"INIT_PLUS_H{h}_N{n}", initial_capital=CAPITAL,
                         deployment="STAGGERED", stagger_months=12, monthly_amount=MONTHLY,
                         n_stocks=n, hold_months=h, replacement="MATURITY_REPLACE")
                if r2:
                    r2["deploymentLabel"] = "INITIAL_PLUS_MONTHLY"
                    rows.append(r2)
        # (6) LEGACY_50D head-to-head — 월 단위 최선 근사
        legacy = run(sn, nm, ds, "LEGACY_50D_APPROX", initial_capital=CAPITAL,
                     deployment="STAGGERED", stagger_months=50, buy_every=1,
                     n_stocks=10, hold_months=2, replacement="MATURITY_REPLACE")
        if legacy:
            legacy["deploymentLabel"] = "STAGGERED_50"
            legacy["note"] = ("LEGACY_50D 는 '50 거래일 분할·50거래일 보유'다. 월 스냅샷이라 "
                              "'50개월 분할·2개월 보유'로 근사했다 — 분할 회수는 같고 시간축은 다르다(한계).")
            rows.append(legacy)
        legacy2 = run(sn, nm, ds, "LEGACY_50D_APPROX_TD", initial_capital=CAPITAL,
                      deployment="STAGGERED", stagger_months=3, buy_every=1,
                      n_stocks=10, hold_months=2, replacement="MATURITY_REPLACE")
        if legacy2:
            legacy2["deploymentLabel"] = "STAGGERED_3"
            legacy2["note"] = "LEGACY_50D 시간축 근사(50거래일 ≈ 2.4개월 분할 → 3개월 분할, 2개월 보유)"
            rows.append(legacy2)

        for r in rows:
            r["excessVsFair"] = (r["twrCagr"] - fair) if r.get("twrCagr") is not None else None

        # plateau — 거치식 격자에서만(현금흐름 동일해야 비교 가능)
        grid = {}
        for r in rows:
            if r.get("deploymentLabel") in ("LUMP_SUM", "STAGGERED_12") and \
               r["params"].get("replacement") == "MATURITY_REPLACE" and \
               not r["params"].get("monthly_amount") and r.get("twrCagr") is not None and \
               r["params"].get("buy_every", 1) == 1:
                grid[(r["deploymentLabel"], r["params"]["hold_months"], r["params"]["n_stocks"])] = r["twrCagr"]
        cells = []
        for (dep, h, n), v in sorted(grid.items()):
            hi, ni = HOLDS.index(h), NSTOCKS.index(n)
            neigh = []
            for dh, dn in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                a, b2 = hi + dh, ni + dn
                if 0 <= a < len(HOLDS) and 0 <= b2 < len(NSTOCKS):
                    kk = (dep, HOLDS[a], NSTOCKS[b2])
                    if kk in grid:
                        neigh.append(grid[kk])
            if not neigh:
                continue
            cells.append({"deployment": dep, "hold": h, "n": n, "cagr": v,
                          "neighborMin": min(neigh), "neighborMean": sum(neigh) / len(neigh),
                          "beatsFair": v > fair,
                          "neighborsBeatFair": all(x > fair for x in neigh),
                          "isolatedPeak": v > fair and min(neigh) < fair and (v - min(neigh)) > 0.05})
        robust = [c for c in cells if c["beatsFair"] and c["neighborsBeatFair"]]
        peaks = [c for c in cells if c["isolatedPeak"]]
        out = {"meta": meta, "benchmarks": bm, "rows": rows,
               "rowCount": len(rows),
               "plateau": {"cells": cells, "robustCount": len(robust), "peakCount": len(peaks),
                           "robustCells": robust, "isolatedPeaks": peaks,
                           "verdict": ("ROBUST_PLATEAU" if len(robust) >= 3 else
                                       "PROMISING_BUT_WEAK" if len(robust) >= 1 else
                                       "OVERFIT_PEAK" if peaks else "UNDERPERFORMING")}}

    else:  # deep — 상위 후보만 심층
        try:
            mx = json.loads(Path("reports/research/r6-matrix-latest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            print(json.dumps({"error": "r6-matrix-latest.json 없음 — --mode matrix 먼저"}, ensure_ascii=False))
            return 1
        cand = [r for r in mx["rows"] if r.get("excessVsFair") is not None]
        cand.sort(key=lambda r: r["excessVsFair"], reverse=True)
        top = cand[:4]
        deep = []
        for t in top:
            p = dict(t["params"])
            base = {k: p.get(k) for k in ("initial_capital", "deployment", "stagger_months",
                                          "monthly_amount", "buy_every", "n_stocks",
                                          "hold_months", "replacement", "rebalance_months")
                    if p.get(k) is not None}
            item = {"tag": t["tag"], "baseExcess": t["excessVsFair"], "sensitivity": [], "subperiods": [], "regimes": []}
            # 비용 / 상장폐지 민감도
            for cb, tx, lbl in ((0, 0, "COST_ZERO"), (15, 20, "COST_BASE"), (30, 20, "COST_HIGH")):
                r = run(sn, nm, ds, f"{t['tag']}__{lbl}", **{**base, "cost_bps": cb, "sell_tax_bps": tx})
                if r:
                    item["sensitivity"].append({"axis": lbl, "cagr": r["twrCagr"],
                                                "excess": r["twrCagr"] - fair})
            for hc, lbl in ((0.0, "DELIST_0"), (0.3, "DELIST_30"), (1.0, "DELIST_100")):
                r = run(sn, nm, ds, f"{t['tag']}__{lbl}", **{**base, "delist_haircut": hc})
                if r:
                    item["sensitivity"].append({"axis": lbl, "cagr": r["twrCagr"],
                                                "excess": r["twrCagr"] - fair})
            # 시작월 민감도
            beats = 0
            tot = 0
            for off in range(0, 12):
                sub = ds[off:]
                r = run(sn, nm, sub, f"{t['tag']}__START{off}", **base)
                if r:
                    tot += 1
                    bsub = benchmark_series(sn, sub, nm)["cagr"]
                    if r["twrCagr"] > bsub:
                        beats += 1
            item["startMonthBeats"] = f"{beats}/{tot}"
            # 5년 하위구간 — **마지막 꼬리 구간까지 반드시 덮는다.**
            #   range(0, len-60, 60) 만 쓰면 236개월 중 뒤 56개월이 어느 구간에도 안 들어간다.
            #   그러면 "전 구간은 초과인데 모든 하위구간은 열위" 같은 해석 불가능한 표가 나온다(실측).
            starts = list(range(0, max(1, len(ds) - 60), 60))
            if starts and starts[-1] + 60 < len(ds) - 1:
                starts.append(len(ds) - 61)
            for i in starts:
                sub = ds[i:i + 61]
                if len(sub) < 25:
                    continue
                r = run(sn, nm, sub, f"{t['tag']}__SUB{i}", **base)
                if r:
                    bsub = benchmark_series(sn, sub, nm)["cagr"]
                    item["subperiods"].append({"start": sub[0], "end": sub[-1],
                                               "cagr": r["twrCagr"], "bench": bsub,
                                               "excess": r["twrCagr"] - bsub})
            # 시장 국면 — 보유기간보다 창이 짧으면 시뮬레이션이 아예 성립하지 않는다(None 반환).
            #   24개월 보유를 24개월 창에서 재는 것은 불가능하므로 창을 보유기간의 2배 이상으로 잡는다.
            reg_win = max(24, int(base.get("hold_months", 12)) * 2 + 6)
            for w in regime_windows(sn, ds, nm, window=reg_win):
                r = run(sn, nm, w["dates"], f"{t['tag']}__{w['regime']}", **base)
                if r:
                    bsub = benchmark_series(sn, w["dates"], nm)["cagr"]
                    item["regimes"].append({"regime": w["regime"], "start": w["start"],
                                            "excess": r["twrCagr"] - bsub})
            byreg = {}
            for r in item["regimes"]:
                byreg.setdefault(r["regime"], []).append(r["excess"])
            item["regimeSummary"] = {k: {"n": len(v), "meanExcess": sum(v) / len(v),
                                         "winRate": sum(1 for x in v if x > 0) / len(v)}
                                     for k, v in byreg.items()}
            deep.append(item)
        out = {"meta": meta, "benchmarks": bm, "deep": deep}

    txt = json.dumps(out, ensure_ascii=False, indent=2, default=float)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(txt, encoding="utf-8")
        print(json.dumps({"written": args.out, "bytes": len(txt)}, ensure_ascii=False))
        return 0
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
