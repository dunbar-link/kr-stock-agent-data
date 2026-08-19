#!/usr/bin/env python3
"""benchmark 수정 전/후 대조 — survivorship 편의가 실제로 얼마였는지 정량화.

구 구현(공통 종목만 사용)을 그대로 재현해 신 구현과 나란히 놓는다.
네트워크 0 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import (  # noqa: E402
    MARKETCAP_WON_PER_UNIT, _excluded_by_name, benchmark_series, load_names,
    load_snapshots, official_index_series,
)
from run_backtest_matrix import contiguous_span  # noqa: E402


def legacy_benchmark(snapshots, dates, names, *, market="COMBINED", min_market_cap=300):
    """구 구현 그대로(결함 재현용) — 전월·당월 공통 종목만 사용."""
    prev, series, idx = None, [], 100.0
    for d in dates:
        snap = snapshots[d]
        cur = {}
        for t, r in snap.items():
            if market != "COMBINED" and r["market"] != market:
                continue
            if not t.endswith("0") or not r["close"] or not r["marketCap"]:
                continue
            if r["marketCap"] < min_market_cap * MARKETCAP_WON_PER_UNIT:
                continue
            if _excluded_by_name(names.get(t, ""), True, True):
                continue
            cur[t] = r["close"]
        if prev:
            common = [t for t in cur if t in prev and prev[t] > 0]
            if common:
                idx *= sum(cur[t] / prev[t] for t in common) / len(common)
        series.append({"date": d, "index": idx})
        prev = cur
    return series


def main():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    years = None
    print(f"기간 {ds[0]} ~ {ds[-1]}  ({len(ds)}개월)")
    print()

    lg = legacy_benchmark(sn, ds, nm)
    from backtest_engine import _to_ord_bm
    years = (_to_ord_bm(ds[-1]) - _to_ord_bm(ds[0])) / 365.25
    lg_cagr = (lg[-1]["index"] / 100.0) ** (1.0 / years) - 1.0
    print(f"[구 결함판] 공통종목만            CAGR {100*lg_cagr:6.2f}%   최종 {lg[-1]['index']:8.1f}")
    print()

    rows = []
    for w, rb, tag in (("EQUAL", "MONTHLY", "동일가중 월리밸"),
                       ("EQUAL", "BUY_HOLD", "동일가중 buy&hold"),
                       ("CAP", "MONTHLY", "시총가중 월리밸")):
        b = benchmark_series(sn, ds, nm, weighting=w, rebalance=rb)
        rows.append((tag, b))
        print(f"[신 공정판] {tag:20s} CAGR {100*b['cagr']:6.2f}%   최종 {b['final']:8.1f}   "
              f"폐지 {b['delistEvents']:4d}건(비중합 {b['delistWeightSum']:.2f})  회전 {b['turnoverPerYear']:.2f}/년")
    # 비용 반영판
    bc = benchmark_series(sn, ds, nm, weighting="EQUAL", rebalance="MONTHLY",
                          cost_bps=15.0, sell_tax_bps=20.0)
    print(f"[신 공정판] 동일가중 월리밸+비용   CAGR {100*bc['cagr']:6.2f}%   최종 {bc['final']:8.1f}")
    bh = benchmark_series(sn, ds, nm, weighting="EQUAL", rebalance="MONTHLY", delist_haircut=1.0)
    print(f"[신 공정판] 동일가중 폐지100%손실  CAGR {100*bh['cagr']:6.2f}%")
    print()
    print(f"→ survivorship 편의 = {100*(lg_cagr - rows[0][1]['cagr']):.2f}%p/년 "
          f"(구 {100*lg_cagr:.2f}% vs 신 {100*rows[0][1]['cagr']:.2f}%)")
    print()
    for mkt in ("KOSPI", "KOSDAQ"):
        b = benchmark_series(sn, ds, nm, market=mkt)
        o = official_index_series(ds, mkt)
        oc = f"{100*o['cagr']:6.2f}%" if o else "  n/a "
        print(f"[{mkt}] 자체 동일가중 {100*b['cagr']:6.2f}%   |   공식 지수(시총가중) {oc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
