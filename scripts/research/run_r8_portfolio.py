#!/usr/bin/env python3
"""R8 실행기 — R7 재현 → coarse matrix → 후보축소 → deep robustness → 최종 판정.

WABABA-ROBUST-FACTOR-PORTFOLIO-R8

§12 준수: matrix 규격을 **결과를 보기 전에** 파일로 저장하고, 그 규격대로만 실행한다.
          결과를 보고 조합을 추가하거나 임계값을 바꾸지 않는다.

모드:
  --mode reproduce   R7 BM Top10% 12M(≈15.83%/년) 을 새 엔진에서 독립 재현
  --mode matrix      사전규격 coarse matrix 실행
  --mode deep        상위 후보만 심층(하위구간·시작월·국면·시장·size·control·비용·상폐)
  --mode report      최종 산출물 2개 생성

안전: 네트워크 0 · canonical 미접근 · 홈페이지 미수정 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import (  # noqa: E402
    RankCache, evaluate, ew_universe_index, run_benchmark_same_schedule, run_portfolio,
)
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
CAPITAL = 50_000_000
MONTHLY = 500_000

# ── 사전 확정 matrix 규격 (§12 — 실행 전 저장) ────────────────────────────────
SPEC = {
    "capital": CAPITAL, "monthlyAmount": MONTHLY,
    "core": {"factor": ["BM"], "percentile": [0.10, 0.20, 0.30],
             "holdings": [10, 20, 30, 40], "hold": [6, 12, 18, 24],
             "deployment": ["STAG12M"], "replacement": ["FIXED_MATURITY_REPLACE"]},
    "deploymentSweep": {"factor": ["BM"], "percentile": [0.20], "holdings": [20],
                        "hold": [6, 12, 18, 24],
                        "deployment": ["LUMP_SUM", "STAG6M", "STAG12M", "STAG24M", "MONTHLY_ROTATION"],
                        "replacement": ["FIXED_MATURITY_REPLACE"]},
    "replacementSweep": {"factor": ["BM"], "percentile": [0.20], "holdings": [20],
                         "hold": [6, 12, 18, 24], "deployment": ["STAG12M"],
                         "replacement": ["FIXED_MATURITY_REPLACE", "RANK_RETENTION", "PERIODIC_REBALANCE"]},
    "factorSweep": {"factor": ["BM", "EY", "BM_EY"], "percentile": [0.20],
                    "holdings": [20, 30], "hold": [12, 24], "deployment": ["STAG12M"],
                    "replacement": ["FIXED_MATURITY_REPLACE"]},
    "contribution": {"modes": ["MONTHLY_DCA", "INITIAL_PLUS_MONTHLY"],
                     "holdings": [20, 30], "hold": [12, 24]},
    # ★ 매수주기를 보유기간과 같게 하면 '한 코호트만' 굴린다 = 실제 보유종목수 = N.
    #   buy_every=1(매월 매수)이면 N×hold 개 lot 이 겹쳐 쌓여 사실상 수백 종목이 된다
    #   (실측: N20/H12 가 평균 129종목·연 419거래·비용 47%). 사람이 실행할 수 없다.
    #   §5 의 'actual holdings vs concurrent unique holdings' 구분을 이 축으로 실현한다.
    "cohortSweep": {"factor": ["BM"], "percentile": [0.10, 0.20],
                    "holdings": [20, 30, 40], "hold": [12, 24],
                    "deployment": ["STAG12M"], "replacement": ["FIXED_MATURITY_REPLACE"],
                    "buyEvery": "hold_months (단일 코호트)"},
    "legacy": "LEGACY_50D_APPROX (10종목·2개월 보유·50분할)",
    "controls": "최종 후보와 동일 구조 · 선정만 RANDOM / EW_UNIVERSE",
    "note": "결과를 보고 조합을 추가하지 않는다. 이 규격이 실행 전에 고정됐다.",
}


def key(**kw):
    # 적립(monthly) 유무를 태그에 반드시 넣는다. 안 넣으면 거치식과 적립식이 같은 태그가 되어
    # 보고서에서 서로 다른 현금흐름의 결과가 한 줄처럼 보인다(실측 확인).
    suffix = f"_M{int(kw.get('monthly', 0)) // 10000}" if kw.get("monthly") else ""
    # 매수주기가 보유기간과 같으면 단일 코호트(실제 보유 = N). 태그로 구분해야 두 구조가 섞이지 않는다.
    if kw.get("buy_every", 1) and kw.get("buy_every", 1) == kw.get("hold"):
        suffix += "_COHORT"
    elif kw.get("buy_every", 1) != 1:
        suffix += f"_B{kw['buy_every']}"
    return (f"{kw['factor']}_P{int(kw['percentile']*100)}_N{kw['holdings']}"
            f"_H{kw['hold']}_{kw['deployment']}_{kw['replacement'][:4]}{suffix}")


def one(cache, dates, idx, *, factor, percentile, holdings, hold, deployment,
        replacement, monthly=0, selection="FACTOR", seed=20260820, buy_every=1, **extra):
    res = run_portfolio(cache, dates, factor=factor, percentile=percentile,
                        n_holdings=holdings, hold_months=hold, deployment=deployment,
                        replacement=replacement, capital=(0 if deployment == "NONE" else CAPITAL),
                        monthly_amount=monthly, selection=selection, seed=seed,
                        buy_every=buy_every, rebalance_months=hold, **extra)
    if not res:
        return None
    bench = run_benchmark_same_schedule(idx, dates, tranches=res["tranches"],
                                        monthly_amount=res["monthlyAmount"])
    ev = evaluate(res, bench)
    if not ev:
        return None
    ev["tag"] = key(factor=factor, percentile=percentile, holdings=holdings, hold=hold,
                    deployment=deployment, replacement=replacement, monthly=monthly,
                    buy_every=buy_every)
    ev["params"] = {"factor": factor, "percentile": percentile, "holdings": holdings,
                    "hold": hold, "deployment": deployment, "replacement": replacement,
                    "monthly": monthly, "selection": selection, "buy_every": buy_every}
    return ev


def build_matrix_specs():
    """사전 규격을 실제 조합 리스트로 전개(중복 제거)."""
    out, seen = [], set()

    def add(f, p, n, h, dep, rep, monthly=0, buy_every=1):
        k = (f, p, n, h, dep, rep, monthly, buy_every)
        if k in seen:
            return
        seen.add(k)
        out.append({"factor": f, "percentile": p, "holdings": n, "hold": h,
                    "deployment": dep, "replacement": rep, "monthly": monthly,
                    "buy_every": buy_every})

    c = SPEC["core"]
    for p in c["percentile"]:
        for n in c["holdings"]:
            for h in c["hold"]:
                add("BM", p, n, h, "STAG12M", "FIXED_MATURITY_REPLACE")
    d = SPEC["deploymentSweep"]
    for h in d["hold"]:
        for dep in d["deployment"]:
            add("BM", 0.20, 20, h, dep, "FIXED_MATURITY_REPLACE")
    r = SPEC["replacementSweep"]
    for h in r["hold"]:
        for rep in r["replacement"]:
            add("BM", 0.20, 20, h, "STAG12M", rep)
    f = SPEC["factorSweep"]
    for fac in f["factor"]:
        for n in f["holdings"]:
            for h in f["hold"]:
                add(fac, 0.20, n, h, "STAG12M", "FIXED_MATURITY_REPLACE")
    for n in SPEC["contribution"]["holdings"]:
        for h in SPEC["contribution"]["hold"]:
            add("BM", 0.20, n, h, "NONE", "FIXED_MATURITY_REPLACE", MONTHLY)
            add("BM", 0.20, n, h, "STAG12M", "FIXED_MATURITY_REPLACE", MONTHLY)
    cs = SPEC["cohortSweep"]
    for p in cs["percentile"]:
        for n in cs["holdings"]:
            for h in cs["hold"]:
                # buy_every = hold → 한 코호트만 굴린다(실제 보유 = N종목)
                add("BM", p, n, h, "STAG12M", "FIXED_MATURITY_REPLACE", 0, h)
    add("MF", 0.20, 10, 2, "STAG12M", "FIXED_MATURITY_REPLACE")     # LEGACY 근사
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="matrix",
                    choices=["reproduce", "matrix", "deep", "report"])
    args = ap.parse_args(argv)

    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    RD.mkdir(parents=True, exist_ok=True)
    print(f"[r8] {ds[0]} ~ {ds[-1]} ({len(ds)}m)", file=sys.stderr)

    if args.mode == "reproduce":
        # R7 정의에 최대한 맞춘 설정: 상위10% 풀에서 넓게(대략 150종목) · 12M 보유 ·
        # 매월 진입 · 비용 0 · 일시투입 아님 → R7 은 '매월 진입한 코호트의 평균'이었다.
        rows = []
        for lbl, kw in (
            ("R7_LIKE_no_cost", dict(factor="BM", percentile=0.10, n_holdings=150,
                                     hold_months=12, deployment="MONTHLY_ROTATION",
                                     replacement="FIXED_MATURITY_REPLACE",
                                     cost_bps=0.0, sell_tax_bps=0.0)),
            ("R8_REALISTIC_20", dict(factor="BM", percentile=0.10, n_holdings=20,
                                     hold_months=12, deployment="STAG12M",
                                     replacement="FIXED_MATURITY_REPLACE")),
        ):
            res = run_portfolio(cache, ds, capital=CAPITAL, **kw)
            b = run_benchmark_same_schedule(idx, ds, tranches=res["tranches"])
            ev = evaluate(res, b)
            ev["label"] = lbl
            rows.append(ev)
        # 지수 자체의 연율(=R7 universe 8.81% 대조)
        years = len(ds) / 12
        ew_cagr = idx[-1] ** (1 / years) - 1
        out = {"r7Target": {"bmTop10_12M": 15.83, "universe": 8.81},
               "ewUniverseIndexCagr": ew_cagr, "rows": rows,
               "note": ("R7 은 '매월 코호트의 12M forward return 평균'(비용·현금·정수주 없음)이고 "
                        "R8 은 실제 장부(정수주·비용·현금)다. 완전 동일 정의가 아니므로 "
                        "차이를 수치로 남긴다.")}
        (RD / "r8-reproduce-latest.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
        print(json.dumps({"ewUniverseCagr": round(100 * ew_cagr, 2),
                          "rows": [{"label": r["label"], "cagr": round(100 * r["twrCagr"], 2),
                                    "bench": round(100 * r["benchCagr"], 2),
                                    "excess": round(100 * r["excess"], 2)} for r in rows]},
                         ensure_ascii=False))
        return 0

    if args.mode == "matrix":
        specs = build_matrix_specs()
        (RD / "r8-matrix-spec-latest.json").write_text(
            json.dumps({"spec": SPEC, "expandedCombos": len(specs), "combos": specs},
                       ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[r8] spec saved: {len(specs)} combos", file=sys.stderr)
        rows = []
        for i, s in enumerate(specs, 1):
            r = one(cache, ds, idx, **s)
            if r:
                rows.append(r)
            if i % 20 == 0:
                print(f"[r8] {i}/{len(specs)}", file=sys.stderr)
        out = {"period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
               "specCombos": len(specs), "rows": rows,
               "ewIndexCagr": idx[-1] ** (12 / len(ds)) - 1}
        (RD / "r8-matrix-latest.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
        beat = [r for r in rows if (r.get("excess") or 0) > 0]
        print(json.dumps({"rows": len(rows), "beatsBench": len(beat)}, ensure_ascii=False))
        return 0

    if args.mode == "deep":
        mx = json.loads((RD / "r8-matrix-latest.json").read_text(encoding="utf-8"))
        rows = [r for r in mx["rows"] if r.get("excess") is not None
                and not r["params"].get("monthly")]
        # 후보축소: CAGR 만이 아니라 excess·MDD·수중·회전을 함께 본다(§13)
        def score(r):
            return (r["excess"]
                    - 0.10 * abs(r["mdd"])
                    - 0.0005 * (r["maxUnderwaterMonths"] or 0)
                    - 0.02 * (r["turnoverPerYear"] or 0))
        rows.sort(key=score, reverse=True)
        # §13·§21: 후보축소는 성과만이 아니라 **실행가능성**도 기준이다.
        #   ladder(매월 매수 → 수백 종목 누적)와 cohort(보유기간마다 1회 매수 → 실제 N종목)는
        #   전혀 다른 실행부담을 가지므로 두 구조에서 각각 상위를 뽑아 심층검증한다.
        #   (성과만으로 자르면 부담이 큰 ladder 만 남아 사람이 못 쓰는 후보만 검증하게 된다)
        ladder = [r for r in rows if r["params"].get("buy_every", 1) != r["params"]["hold"]]
        cohort = [r for r in rows if r["params"].get("buy_every", 1) == r["params"]["hold"]]
        top = ladder[:4] + cohort[:4]
        deep = []
        for t in top:
            p = t["params"]
            base = dict(factor=p["factor"], percentile=p["percentile"], holdings=p["holdings"],
                        hold=p["hold"], deployment=p["deployment"], replacement=p["replacement"],
                        buy_every=p.get("buy_every", 1))
            item = {"tag": t["tag"], "base": t, "neighborhood": [], "subperiods": [],
                    "startMonths": [], "segments": {}, "sizes": {}, "controls": {},
                    "sensitivity": []}
            # 인접 parameter (§14)
            for kk, vals in (("percentile", [0.10, 0.20, 0.30]),
                             ("holdings", [10, 20, 30, 40]),
                             ("hold", [6, 12, 18, 24]),
                             ("deployment", ["LUMP_SUM", "STAG6M", "STAG12M", "STAG24M"])):
                for v in vals:
                    if v == base[kk]:
                        continue
                    nb = dict(base)
                    nb[kk] = v
                    r = one(cache, ds, idx, **nb)
                    if r:
                        item["neighborhood"].append({"axis": kk, "value": str(v),
                                                     "cagr": r["twrCagr"], "excess": r["excess"]})
            # 하위구간 (R7 과 같은 경계)
            for lab, lo, hi in (("2007-2011", "2007-01", "2011-12"), ("2012-2016", "2012-01", "2016-12"),
                                ("2017-2021", "2017-01", "2021-12"), ("2022-현재", "2022-01", "2099-12")):
                sub = [d for d in ds if lo <= d[:7] <= hi]
                if len(sub) < base["hold"] + 6:
                    continue
                sidx = ew_universe_index(cache, sub)
                r = one(cache, sub, sidx, **base)
                if r:
                    item["subperiods"].append({"period": lab, "cagr": r["twrCagr"],
                                               "excess": r["excess"], "mdd": r["mdd"]})
            # 시작월 12회
            for off in range(12):
                sub = ds[off:]
                sidx = ew_universe_index(cache, sub)
                r = one(cache, sub, sidx, **base)
                if r:
                    item["startMonths"].append({"offset": off, "cagr": r["twrCagr"],
                                                "excess": r["excess"], "mdd": r["mdd"]})
            # 시장 / size
            for mkt in ("KOSPI", "KOSDAQ"):
                c2 = RankCache(sn, nm, market=mkt)
                i2 = ew_universe_index(c2, ds)
                r = one(c2, ds, i2, **base)
                if r:
                    item["segments"][mkt] = {"cagr": r["twrCagr"], "excess": r["excess"]}
            for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
                c3 = RankCache(sn, nm, size_bucket=b)
                i3 = ew_universe_index(c3, ds)
                r = one(c3, ds, i3, **base)
                if r:
                    item["sizes"][lbl] = {"cagr": r["twrCagr"], "excess": r["excess"]}
            # 대조군 — 동일 구조, 선정만 교체 (§20)
            for sel in ("RANDOM", "EW_UNIVERSE"):
                r = one(cache, ds, idx, **base, selection=sel)
                if r:
                    item["controls"][sel] = {"cagr": r["twrCagr"], "excess": r["excess"]}
            # 비용 / 상장폐지 민감도
            for lbl, kw in (("COST_HIGH", dict(cost_bps=30.0, sell_tax_bps=20.0)),
                            ("DELIST_100", dict(delist_haircut=1.0))):
                res = run_portfolio(cache, ds, factor=base["factor"], percentile=base["percentile"],
                                    n_holdings=base["holdings"], hold_months=base["hold"],
                                    deployment=base["deployment"], replacement=base["replacement"],
                                    capital=CAPITAL, rebalance_months=base["hold"], **kw)
                if res:
                    b2 = run_benchmark_same_schedule(idx, ds, tranches=res["tranches"])
                    e2 = evaluate(res, b2)
                    item["sensitivity"].append({"axis": lbl, "cagr": e2["twrCagr"],
                                                "excess": e2["excess"]})
            deep.append(item)
            print(f"[r8] deep {t['tag']} done", file=sys.stderr)
        (RD / "r8-deep-latest.json").write_text(
            json.dumps({"deep": deep}, ensure_ascii=False, indent=2, default=float),
            encoding="utf-8")
        print(json.dumps({"deepCount": len(deep)}, ensure_ascii=False))
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
