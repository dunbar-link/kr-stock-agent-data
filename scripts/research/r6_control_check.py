#!/usr/bin/env python3
"""R6 자기감사(§19) — 이상한 결과를 '좋다'고 보고하기 전에 원인을 분리한다.

두 가지를 확인한다.

1) 대조군(control): 같은 구조(투입·보유·교체·비용·universe)에서 선정 규칙만 바꿔 돌린다.
     MAGIC / LARGEST_CAP / TICKER
   → 셋 다 비슷하게 부진하면 부진의 원인은 **운용 구조/집중도**이고,
     MAGIC 만 크게 나쁘면 **순위 자체**가 문제다. 둘은 전혀 다른 결론이다.

2) 단독 peak 검증: 전 구간 초과인데 모든 5년 구간이 열위인 조합은
   경로의존 아티팩트일 수 있다. 시작월을 밀어가며 재현되는지 본다.

네트워크 0 · canonical 미접근 · --out 리포트만 write.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import benchmark_series, load_names, load_snapshots  # noqa: E402
from deployment_engine import evaluate, simulate  # noqa: E402
from run_backtest_matrix import build_benchmarks, contiguous_span  # noqa: E402

CAPITAL = 50_000_000


def one(sn, nm, ds, **kw):
    r = simulate(sn, nm, ds, **kw)
    return evaluate(r) if r else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)

    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    bm = build_benchmarks(sn, ds, nm)
    fair = bm["EQUAL_MONTHLY"]["cagr"]

    out = {"meta": {"start": ds[0], "end": ds[-1], "months": len(ds),
                    "benchmarkFairCagr": fair, "initialCapital": CAPITAL,
                    "networkCalls": 0, "realOrderCount": 0, "brokerApiCallCount": 0},
           "control": [], "peakReplication": []}

    # ── 1) 대조군: 선정 규칙만 교체 ────────────────────────────────────────────
    for hold, n in ((12, 20), (24, 10), (24, 20)):
        for sel in ("MAGIC", "LARGEST_CAP", "TICKER"):
            ev = one(sn, nm, ds, initial_capital=CAPITAL, deployment="STAGGERED",
                     stagger_months=12, n_stocks=n, hold_months=hold,
                     replacement="MATURITY_REPLACE", selection=sel)
            if ev:
                out["control"].append({
                    "hold": hold, "n": n, "selection": sel,
                    "cagr": ev["twrCagr"], "excessVsFair": ev["twrCagr"] - fair,
                    "mdd": ev["mdd"], "terminalWealth": ev["terminalWealth"],
                })

    # ── 2) 단독 peak 재현성: LUMP_SUM_H24_N10 을 시작월 12회 이동 ──────────────
    base = dict(initial_capital=CAPITAL, deployment="LUMP_SUM", n_stocks=10,
                hold_months=24, replacement="MATURITY_REPLACE")
    for off in range(12):
        sub = ds[off:]
        ev = one(sn, nm, sub, **base)
        if ev:
            b = benchmark_series(sn, sub, nm)["cagr"]
            out["peakReplication"].append({"startOffsetMonths": off, "start": sub[0],
                                           "cagr": ev["twrCagr"], "bench": b,
                                           "excess": ev["twrCagr"] - b})
    ex = [x["excess"] for x in out["peakReplication"]]
    if ex:
        out["peakSummary"] = {"n": len(ex), "beats": sum(1 for x in ex if x > 0),
                              "mean": sum(ex) / len(ex), "min": min(ex), "max": max(ex),
                              "verdict": ("REPRODUCIBLE" if sum(1 for x in ex if x > 0) >= len(ex) * 0.7
                                          else "PATH_DEPENDENT_ARTIFACT")}

    # 대조군 요약
    byhold = {}
    for c in out["control"]:
        byhold.setdefault(f"H{c['hold']}_N{c['n']}", {})[c["selection"]] = c["excessVsFair"]
    out["controlSummary"] = {
        k: {**v, "magicMinusLargest": (v.get("MAGIC", 0) - v.get("LARGEST_CAP", 0)),
            "magicMinusTicker": (v.get("MAGIC", 0) - v.get("TICKER", 0))}
        for k, v in byhold.items()
    }
    allneg = all(c["excessVsFair"] < 0 for c in out["control"] if c["selection"] != "MAGIC")
    out["diagnosis"] = ("STRUCTURAL_UNDERPERFORMANCE" if allneg
                        else "RANKING_SPECIFIC_UNDERPERFORMANCE")

    txt = json.dumps(out, ensure_ascii=False, indent=2, default=float)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(txt, encoding="utf-8")
        print(json.dumps({"written": args.out, "diagnosis": out["diagnosis"],
                          "peakVerdict": out.get("peakSummary", {}).get("verdict")},
                         ensure_ascii=False))
        return 0
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
