#!/usr/bin/env python3
"""R8 §17 — 시장 국면(bull / bear / sideways)별 후보 성과.

국면은 **benchmark 수익률**로 정의한다. 전략 성과로 국면을 나누면 순환논리가 된다.
보유기간보다 창이 짧으면 시뮬레이션이 성립하지 않으므로 창을 보유기간의 2배+로 잡는다
(R6 에서 24개월 보유를 24개월 창으로 재려다 국면분석이 통째로 비었던 실사고 반영).

산출: reports/research/r8-regime-latest.json
안전: 네트워크 0 · production 미변경.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from run_backtest_matrix import contiguous_span, regime_windows  # noqa: E402
from run_r8_portfolio import key, one  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def main() -> int:
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)

    deep = json.loads((RD / "r8-deep-latest.json").read_text(encoding="utf-8"))
    # 실행 가능한 단일 코호트 후보만 국면 분석한다(최종 후보가 그쪽에서 나온다).
    cands = [d["base"]["params"] for d in deep["deep"]
             if d["base"]["params"].get("buy_every", 1) == d["base"]["params"]["hold"]]

    out = {}
    for p0 in cands:
        base = {"factor": p0["factor"], "percentile": p0["percentile"],
                "holdings": p0["holdings"], "hold": p0["hold"],
                "deployment": p0["deployment"], "replacement": p0["replacement"],
                "buy_every": p0.get("buy_every", 1)}
        tag = key(monthly=0, **base)
        win = max(24, base["hold"] * 2 + 12)
        rows = []
        for w in regime_windows(sn, ds, nm, window=win):
            sidx = ew_universe_index(cache, w["dates"])
            r = one(cache, w["dates"], sidx, **base)
            if r:
                rows.append({"regime": w["regime"], "start": w["start"], "end": w["end"],
                             "benchAnnual": w["benchAnnual"], "cagr": r["twrCagr"],
                             "excess": r["excess"], "mdd": r["mdd"]})
        by = {}
        for r in rows:
            by.setdefault(r["regime"], []).append(r["excess"])
        out[tag] = {"windowMonths": win, "windows": rows,
                    "summary": {k: {"n": len(v), "meanExcess": sum(v) / len(v),
                                    "winRate": sum(1 for x in v if x > 0) / len(v)}
                                for k, v in by.items()}}
        print(f"[r8] regime {tag}: {len(rows)} windows", file=sys.stderr)

    (RD / "r8-regime-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    print(json.dumps({"candidates": list(out)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
