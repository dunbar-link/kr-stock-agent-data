#!/usr/bin/env python3
"""R9 — frozen candidate 를 FAIL 시키려고 공격하는 검증 실행기.

WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9

목적은 "얼마나 좋은가"가 아니라 "공격해도 남는가"다(§0).
frozen candidate 의 파라미터는 어디에서도 변경하지 않는다(§18).
판정규칙은 `r9_precommit.py` 가 **결과를 보기 전에** 저장한 것만 쓴다(§17).

모드
  repro        §1  R8 재현 게이트 (실패 시 이후 전부 중단)
  core         §3·5·10·11·12·14·15·19  base 원장 기반 해부
  stress       §6·7·8  상장폐지 / 비용·슬리피지 / 유동성
  walkforward  §2·4  retrospective walk-forward + rolling cohort 분포
  controls     §13  RANDOM / SIZE / MARKET matched 통제군
  dataquality  §9   PBR·BM 데이터 품질 + look-ahead 감사
  benchmarks   §16  benchmark 도전
  verdict      §17  사전확정 게이트 적용 → 최종 판정
  all          위 전부 순서대로

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 홈페이지 미수정 ·
      실주문 0 · 브로커 0 · 유료데이터 0 · 배포 0.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r8_portfolio import run_portfolio as r8_run  # noqa: E402
from r8_portfolio import evaluate as r8_eval  # noqa: E402
from r8_portfolio import run_benchmark_same_schedule as r8_bench  # noqa: E402
from r9_engine import (  # noqa: E402
    LiquidityRankCache, bench_same_schedule, chain_stats, evaluate, monthly_twr,
    run_case, simulate, slice_index,
)
from r9_precommit import (  # noqa: E402
    COST_LEVELS, CONTROL_SEEDS, DELIST_LEVELS, ERAS, FROZEN, GATES,
    LIQUIDITY_STRESS, REPRO_TOLERANCE, VERDICT_RULES,
)
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
P = FROZEN["params"]
BASE_KW = dict(factor=P["factor"], percentile=P["percentile"], n_holdings=P["holdings"],
               hold_months=P["hold"], deployment=P["deployment"],
               replacement=P["replacement"], buy_every=P["buy_every"],
               capital=FROZEN["initialCapitalKrw"])


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r9-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r9] saved {p.name}", file=sys.stderr)


def load(name):
    return json.loads((RD / f"r9-{name}-latest.json").read_text(encoding="utf-8"))


def ctx():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    return sn, nm, ds, cache, idx


def quant(xs, q):
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def dist(xs):
    if not xs:
        return None
    return {"n": len(xs), "median": quant(xs, 0.50), "p10": quant(xs, 0.10),
            "p25": quant(xs, 0.25), "p75": quant(xs, 0.75), "p90": quant(xs, 0.90),
            "min": min(xs), "max": max(xs), "mean": sum(xs) / len(xs)}


# ═════════════════════════════ §1 재현 게이트 ═════════════════════════════
def mode_repro(sn, nm, ds, cache, idx):
    r8res = r8_run(cache, ds, factor=P["factor"], percentile=P["percentile"],
                   n_holdings=P["holdings"], hold_months=P["hold"],
                   deployment=P["deployment"], replacement=P["replacement"],
                   capital=FROZEN["initialCapitalKrw"], buy_every=P["buy_every"],
                   rebalance_months=P["hold"])
    r8b = r8_bench(idx, ds, tranches=r8res["tranches"])
    r8ev = r8_eval(r8res, r8b)
    _, r9ev = run_case(cache, ds, idx, **BASE_KW)

    tol = REPRO_TOLERANCE
    m = FROZEN["r8Measured"]
    checks = []

    def chk(cid, r8v, r9v, reported, tolv, unit="%p"):
        d_engine = None if (r8v is None or r9v is None) else abs(r8v - r9v)
        d_report = None if (r9v is None or reported is None) else abs(r9v - reported)
        ok = ((d_engine is None or d_engine <= tolv) and
              (d_report is None or d_report <= tolv))
        checks.append({"metric": cid, "r8Engine": r8v, "r9Engine": r9v,
                       "r8Report": reported, "diffEngine": d_engine,
                       "diffVsReport": d_report, "tolerance": tolv,
                       "unit": unit, "pass": ok})

    chk("cagrPct", pct(r8ev["twrCagr"]), pct(r9ev["twrCagr"]), m["cagrPct"], tol["cagrPctAbs"])
    chk("benchPct", pct(r8ev["benchCagr"]), pct(r9ev["benchCagr"]), m["benchPct"], tol["benchPctAbs"])
    chk("excessPct", pct(r8ev["excess"]), pct(r9ev["excess"]), m["excessPct"], tol["excessPctAbs"])
    chk("mddPct", pct(r8ev["mdd"]), pct(r9ev["mdd"]), m["mddPct"], tol["mddPctAbs"])
    chk("tradesPerYear", round(r8ev["trades"] / r8ev["years"]),
        round(r9ev["trades"] / r9ev["years"]), m["tradesPerYear"], tol["tradesAbs"], "건")
    chk("avgPositions", round(r8ev["avgPositions"], 2), round(r9ev["avgPositions"], 2),
        m["avgPositions"], tol["avgPositionsAbs"], "종목")
    chk("underwaterMonths", r8ev["maxUnderwaterMonths"], r9ev["maxUnderwaterMonths"],
        m["underwaterMonths"], tol["underwaterMonthsAbs"], "개월")

    # 시작월 12회 (R8 보고 10/12 재현)
    beats = 0
    total = 0
    starts = []
    for off in range(12):
        sub = ds[off:]
        _, ev = run_case(cache, sub, slice_index(idx, off), **BASE_KW)
        if ev and ev.get("excess") is not None:
            total += 1
            beats += 1 if ev["excess"] > 0 else 0
            starts.append({"offset": off, "start": sub[0], "cagrPct": pct(ev["twrCagr"]),
                           "benchPct": pct(ev["benchCagr"]), "excessPct": pct(ev["excess"])})
    checks.append({"metric": "startMonthBeats", "r9Engine": f"{beats}/{total}",
                   "r8Report": m["startMonthBeats"],
                   "pass": f"{beats}/{total}" == m["startMonthBeats"]})

    # ── R8 stress 수치가 '이 후보'로 계산됐는지 검사 ────────────────────────
    #   R8 deep 모드의 sensitivity 호출은 buy_every 를 넘기지 않는다(기본 1 = 매월 ladder).
    #   그래서 cohort 후보의 survivesCost / survivesDelisting 판정이 실제로는
    #   전혀 다른 구조(연 600건·300종목)의 숫자로 내려졌다. 재현해서 확인한다.
    r8bug = []
    for lbl, kw, reported in (("COST_HIGH", dict(cost_bps=30.0, sell_tax_bps=20.0), 1.85),
                              ("DELIST_100", dict(delist_haircut=1.0), 0.86)):
        common = dict(factor=P["factor"], percentile=P["percentile"],
                      n_holdings=P["holdings"], hold_months=P["hold"],
                      deployment=P["deployment"], replacement=P["replacement"],
                      capital=FROZEN["initialCapitalKrw"], rebalance_months=P["hold"])
        rb = r8_run(cache, ds, **common, **kw)                        # buy_every 미전달 = ladder
        eb = r8_eval(rb, r8_bench(idx, ds, tranches=rb["tranches"]))
        rf = r8_run(cache, ds, **common, buy_every=P["buy_every"], **kw)   # cohort = 실제 후보
        ef = r8_eval(rf, r8_bench(idx, ds, tranches=rf["tranches"]))
        r8bug.append({
            "axis": lbl, "r8ReportedExcessPct": reported,
            "asR8Called_ladder": {"excessPct": pct(eb["excess"]),
                                  "tradesPerYear": round(eb["trades"] / eb["years"]),
                                  "avgPositions": round(eb["avgPositions"])},
            "frozenCohort_correct": {"excessPct": pct(ef["excess"]),
                                     "tradesPerYear": round(ef["trades"] / ef["years"]),
                                     "avgPositions": round(ef["avgPositions"])},
            "r8NumberMatchesLadder": abs(pct(eb["excess"]) - reported) <= 0.05})

    ok = all(c["pass"] for c in checks)
    out = {"gate": "R8_REPRODUCTION", "pass": ok,
           "r8SensitivityStructureBug": {
               "found": all(r["r8NumberMatchesLadder"] for r in r8bug),
               "rows": r8bug,
               "impact": ("R8 이 후보 A 에 붙인 survivesCost:X / survivesDelisting:X 는 "
                          "cohort 후보가 아니라 매월매수 ladder 구조로 계산된 값이다. "
                          "R9 는 전 구간에서 cohort 구조로만 스트레스를 준다."),
               "action": ("R8 산출물은 재현성 보존을 위해 수정하지 않는다. R9 가 올바른 "
                          "구조로 재계산한 값을 정본으로 쓴다.")},
           "engineBitExact": all(c.get("diffEngine") in (None, 0) or
                                 (c.get("diffEngine") or 0) < 1e-9 for c in checks[:6]),
           "period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
           "tolerance": tol, "checks": checks, "startMonths": starts,
           "base": {"cagrPct": pct(r9ev["twrCagr"]), "benchPct": pct(r9ev["benchCagr"]),
                    "excessPct": pct(r9ev["excess"]), "mddPct": pct(r9ev["mdd"]),
                    "underwaterMonths": r9ev["maxUnderwaterMonths"],
                    "worst1yPct": pct(r9ev["worst1y"]), "worst3yPct": pct(r9ev["worst3y"]),
                    "terminalWealth": r9ev["terminalWealth"],
                    "benchTerminal": r9ev["benchTerminal"],
                    "contributed": r9ev["contributed"],
                    "tradesPerYear": round(r9ev["trades"] / r9ev["years"], 1),
                    "avgPositions": round(r9ev["avgPositions"], 1),
                    "maxPositions": r9ev["maxPositions"],
                    "uniqueEver": r9ev["uniqueEver"],
                    "delistEvents": r9ev["delistEvents"],
                    "costPctOfContrib": pct(r9ev["costPctOfContrib"]),
                    "roll1yNegSharePct": pct(r9ev["roll1yNegShare"])},
           "note": ("R9 엔진은 R8 엔진과 별개 코드다. base 설정에서 소수점까지 같아야 "
                    "'같은 후보'를 검증한 것이 된다.")}
    save("repro", out)
    print(json.dumps({"reproPass": ok, "bitExact": out["engineBitExact"],
                      "baseExcessPct": out["base"]["excessPct"]}, ensure_ascii=False))
    return 0 if ok else 3


# ═════════════════════════════ §3·5·10~15·19 core ═════════════════════════════
def era_of(iso):
    ym = iso[:7]
    for e in ERAS:
        if e["from"] <= ym <= e["to"]:
            return e["id"]
    return "OUT"


def mode_core(sn, nm, ds, cache, idx):
    res, ev = run_case(cache, ds, idx, **BASE_KW)
    bench = bench_same_schedule(idx, ds, tranches=res["tranches"])
    srets = monthly_twr(res["nav"], res["inflow"])
    brets = monthly_twr(bench["nav"], res["inflow"])
    # 월 k 의 수익률은 dates[k+1] 에 귀속된다
    rdates = ds[1:]

    # ── §3 leave-one-era-out (수익률 splice) ──────────────────────────────
    full_s, full_b = chain_stats(srets), chain_stats(brets)
    loo = []
    for e in ERAS:
        keep = [j for j, d in enumerate(rdates) if not (e["from"] <= d[:7] <= e["to"])]
        drop = len(rdates) - len(keep)
        if drop == 0 or len(keep) < 36:
            loo.append({"era": e["id"], "label": e["label"], "droppedMonths": drop,
                        "skipped": True})
            continue
        s = chain_stats([srets[j] for j in keep])
        b = chain_stats([brets[j] for j in keep])
        loo.append({"era": e["id"], "label": e["label"], "from": e["from"], "to": e["to"],
                    "droppedMonths": drop, "cagrPct": pct(s["cagr"]),
                    "benchPct": pct(b["cagr"]),
                    "excessPct": pct(s["cagr"] - b["cagr"]),
                    "mddPct": pct(s["mdd"]), "positive": s["cagr"] - b["cagr"] > 0})
    # 각 시대 단독 성과
    era_only = []
    for e in ERAS:
        sel = [j for j, d in enumerate(rdates) if e["from"] <= d[:7] <= e["to"]]
        if len(sel) < 6:
            continue
        s = chain_stats([srets[j] for j in sel])
        b = chain_stats([brets[j] for j in sel])
        era_only.append({"era": e["id"], "label": e["label"], "months": len(sel),
                         "cagrPct": pct(s["cagr"]), "benchPct": pct(b["cagr"]),
                         "excessPct": pct(s["cagr"] - b["cagr"]),
                         "mddPct": pct(s["mdd"]),
                         "totalReturnPct": pct(s["growth"] - 1),
                         "benchTotalPct": pct(b["growth"] - 1)})

    # ── §5 극단구간 자동 식별 (benchmark 지수 기준) ────────────────────────
    extremes = []
    peak, pi = idx[0], 0
    ep = []
    trough, ti = idx[0], 0
    for i in range(1, len(idx)):
        if idx[i] >= peak:
            if trough / peak - 1 <= -0.20:
                ep.append((pi, ti, i))
            peak, pi, trough, ti = idx[i], i, idx[i], i
        elif idx[i] < trough:
            trough, ti = idx[i], i
    if trough / peak - 1 <= -0.20:
        ep.append((pi, ti, None))
    for a, b, rec in ep:
        sel = list(range(a, b))          # rets index j = month j+1
        if not sel:
            continue
        s = chain_stats([srets[j] for j in sel if j < len(srets)])
        bb = chain_stats([brets[j] for j in sel if j < len(brets)])
        if not s or not bb:
            continue
        extremes.append({"type": "BENCH_DRAWDOWN", "from": ds[a], "to": ds[b],
                         "months": len(sel),
                         "benchTotalPct": pct(bb["growth"] - 1),
                         "candTotalPct": pct(s["growth"] - 1),
                         "excessTotalPct": pct(s["growth"] - bb["growth"]),
                         "candMddPct": pct(s["mdd"]),
                         "recoveredAt": ds[rec] if rec else None,
                         "recoveryMonths": (rec - b) if rec else None})
    # 최악/최고 12개월 benchmark 창
    win = []
    for a in range(0, len(brets) - 12):
        bg = 1.0
        sg = 1.0
        for j in range(a, a + 12):
            bg *= 1 + brets[j]
            sg *= 1 + srets[j]
        win.append((bg, sg, a))
    win.sort()
    for lbl, items in (("WORST_12M_BENCH", win[:3]), ("BEST_12M_BENCH", win[-3:])):
        for bg, sg, a in items:
            # 구간 라벨은 기간 시작월 ds[a] ~ 종료월 ds[a+12] 이다(수익률 j=a..a+11).
            extremes.append({"type": lbl, "from": ds[a], "to": ds[a + 12],
                             "months": 12, "benchTotalPct": pct(bg - 1),
                             "candTotalPct": pct(sg - 1),
                             "excessTotalPct": pct(sg - bg)})
    # 장기 횡보: benchmark 36개월 총수익 절대값 최소
    side = []
    for a in range(0, len(brets) - 36):
        bg = 1.0
        sg = 1.0
        for j in range(a, a + 36):
            bg *= 1 + brets[j]
            sg *= 1 + srets[j]
        side.append((abs(bg - 1), bg, sg, a))
    side.sort()
    if side:
        _, bg, sg, a = side[0]
        extremes.append({"type": "FLAT_36M_BENCH", "from": ds[a],
                         "to": ds[a + 36], "months": 36,
                         "benchTotalPct": pct(bg - 1), "candTotalPct": pct(sg - 1),
                         "excessTotalPct": pct(sg - bg)})

    # ── 원장 해부 ─────────────────────────────────────────────────────────
    lots = res["ledger"] + res["openLots"]
    tot_pnl = sum(l["pnl"] for l in lots)
    tot_basis = sum(l["costBasis"] for l in lots)

    def group(keyfn):
        g = {}
        for l in lots:
            k = keyfn(l)
            e = g.setdefault(k, {"lots": 0, "basis": 0.0, "pnl": 0.0, "wins": 0,
                                 "delisted": 0})
            e["lots"] += 1
            e["basis"] += l["costBasis"]
            e["pnl"] += l["pnl"]
            e["wins"] += 1 if l["pnl"] > 0 else 0
            e["delisted"] += 1 if l["delisted"] else 0
        for k, e in g.items():
            e["basisSharePct"] = round(100 * e["basis"] / tot_basis, 2) if tot_basis else None
            e["pnlSharePct"] = round(100 * e["pnl"] / tot_pnl, 2) if tot_pnl else None
            e["avgRetPct"] = round(100 * e["pnl"] / e["basis"], 2) if e["basis"] else None
            e["winRatePct"] = round(100 * e["wins"] / e["lots"], 1)
            e["delistRatePct"] = round(100 * e["delisted"] / e["lots"], 2)
        return g

    by_market = group(lambda l: l["market"])
    by_era = group(lambda l: era_of(ds[l["buyIdx"]]))
    by_market_era = {}
    for l in lots:
        k = f"{l['market']}|{era_of(ds[l['buyIdx']])}"
        by_market_era.setdefault(k, 0.0)
        by_market_era[k] += l["pnl"]

    # size tercile (매수 시점 universe 내 시총 순위)
    for l in lots:
        u = cache.universe(ds[l["buyIdx"]])
        caps = sorted((r["marketCap"] or 0) for r in u.values())
        c = l["marketCapAtBuy"] or 0
        lo = sum(1 for x in caps if x < c)
        l["_capPctile"] = lo / len(caps) if caps else None
    def size_bucket(l):
        q = l.get("_capPctile")
        if q is None:
            return "?"
        return "SMALL" if q < 1 / 3 else ("MID" if q < 2 / 3 else "LARGE")
    by_size = group(size_bucket)

    # ── §14 concentration ────────────────────────────────────────────────
    per_ticker = {}
    for l in lots:
        per_ticker.setdefault(l["ticker"], 0.0)
        per_ticker[l["ticker"]] += l["pnl"]
    ranked = sorted(per_ticker.items(), key=lambda kv: -kv[1])
    years = ev["years"]
    growth = (1 + ev["twrCagr"]) ** years
    conc = {"uniqueTickers": len(per_ticker), "totalPnl": tot_pnl,
            "top": [{"ticker": t, "name": nm.get(t, t), "pnl": v,
                     "pnlSharePct": round(100 * v / tot_pnl, 2) if tot_pnl else None}
                    for t, v in ranked[:20]], "removal": []}
    pos = [v for v in per_ticker.values() if v > 0]
    tp = sum(pos)
    conc["hhiPositiveContrib"] = round(sum((v / tp) ** 2 for v in pos), 5) if tp else None
    conc["top5SharePct"] = round(100 * sum(v for _, v in ranked[:5]) / tot_pnl, 2) if tot_pnl else None
    conc["top10SharePct"] = round(100 * sum(v for _, v in ranked[:10]) / tot_pnl, 2) if tot_pnl else None
    for k in (1, 5, 10, 20):
        drop = sum(v for _, v in ranked[:k])
        adj_terminal = ev["terminalWealth"] - drop
        f = adj_terminal / ev["terminalWealth"] if ev["terminalWealth"] else 0
        g2 = growth * f
        c2 = g2 ** (1 / years) - 1 if g2 > 0 else None
        banned = [t for t, _ in ranked[:k]]
        _, ev2 = run_case(cache, ds, idx, banned=banned, **BASE_KW)
        conc["removal"].append({
            "topK": k, "removedPnl": drop, "tickers": banned,
            "firstOrder": {"cagrPct": pct(c2),
                           "excessPct": pct(c2 - ev["benchCagr"]) if c2 is not None else None},
            "resimulated": {"cagrPct": pct(ev2["twrCagr"]), "benchPct": pct(ev2["benchCagr"]),
                            "excessPct": pct(ev2["excess"])} if ev2 else None})

    # ── §15 return distribution ──────────────────────────────────────────
    rets_l = [l["ret"] for l in lots if l["ret"] is not None]
    rd = {"positions": len(rets_l), "medianRetPct": pct(quant(rets_l, 0.50)),
          "meanRetPct": pct(sum(rets_l) / len(rets_l)) if rets_l else None,
          "winRatePct": pct(sum(1 for r in rets_l if r > 0) / len(rets_l)) if rets_l else None,
          "lossRatePct": pct(sum(1 for r in rets_l if r <= 0) / len(rets_l)) if rets_l else None,
          "bigWinRatePct(>=+50%)": pct(sum(1 for r in rets_l if r >= 0.5) / len(rets_l)) if rets_l else None,
          "hugeWinRatePct(>=+100%)": pct(sum(1 for r in rets_l if r >= 1.0) / len(rets_l)) if rets_l else None,
          "bigLossRatePct(<=-50%)": pct(sum(1 for r in rets_l if r <= -0.5) / len(rets_l)) if rets_l else None,
          "wipeoutRatePct(<=-90%)": pct(sum(1 for r in rets_l if r <= -0.9) / len(rets_l)) if rets_l else None,
          "percentiles": {k: pct(v) for k, v in (dist(rets_l) or {}).items()
                          if k not in ("n",)},
          "shape": None}
    if rets_l:
        med = quant(rets_l, 0.50)
        rd["shape"] = ("MANY_SMALL_WINS" if med > 0.05 else
                       ("FEW_BIG_WINNERS" if (rd["hugeWinRatePct(>=+100%)"] or 0) > 10 and med <= 0.05
                        else "MIXED"))

    # ── §10 distress exposure (picks vs universe) ────────────────────────
    dexp = {"pickPbr": None, "universePbr": None, "byBuyDate": []}
    pick_pbr, uni_pbr = [], []
    pick_neg_bps = uni_neg_bps = pick_lowpbr = uni_lowpbr = 0
    pick_n = uni_n = 0
    for pl in res["pickLog"]:
        d = pl["date"]
        u = cache.universe(d)
        snap = sn[d]
        up = [r["PBR"] for r in u.values() if r["PBR"] and r["PBR"] > 0]
        uni_pbr += up
        uni_n += len(u)
        uni_neg_bps += sum(1 for r in u.values() if r["BPS"] is not None and r["BPS"] <= 0)
        uni_lowpbr += sum(1 for r in u.values() if r["PBR"] and 0 < r["PBR"] < 0.2)
        pp = [p["pbr"] for p in pl["picks"] if p["pbr"]]
        pick_pbr += pp
        pick_n += len(pl["picks"])
        pick_neg_bps += sum(1 for p in pl["picks"]
                            if (snap.get(p["ticker"], {}).get("BPS") or 1) <= 0)
        pick_lowpbr += sum(1 for p in pl["picks"] if p["pbr"] and 0 < p["pbr"] < 0.2)
        # 24개월 후 상장폐지율 비교
        j = ds.index(d)
        k = min(j + P["hold"], len(ds) - 1)
        fut = sn[ds[k]]
        u_gone = sum(1 for t in u if t not in fut or not fut[t]["close"])
        p_gone = sum(1 for p in pl["picks"]
                     if p["ticker"] not in fut or not fut[p["ticker"]]["close"])
        dexp["byBuyDate"].append({
            "date": d, "picks": len(pl["picks"]), "universe": len(u),
            "pickMedianPbr": quant(pp, 0.5), "universeMedianPbr": quant(up, 0.5),
            "pickDelist24mPct": round(100 * p_gone / len(pl["picks"]), 2) if pl["picks"] else None,
            "universeDelist24mPct": round(100 * u_gone / len(u), 2) if u else None,
            "pickKosdaqPct": round(100 * sum(1 for p in pl["picks"] if p["market"] == "KOSDAQ")
                                   / len(pl["picks"]), 1) if pl["picks"] else None,
            "universeKosdaqPct": round(100 * sum(1 for r in u.values() if r["market"] == "KOSDAQ")
                                       / len(u), 1) if u else None})
    dexp["pickPbr"] = dist(pick_pbr)
    dexp["universePbr"] = dist(uni_pbr)
    dexp["pickNegBpsPct"] = round(100 * pick_neg_bps / pick_n, 2) if pick_n else None
    dexp["universeNegBpsPct"] = round(100 * uni_neg_bps / uni_n, 2) if uni_n else None
    dexp["pickExtremeLowPbrPct"] = round(100 * pick_lowpbr / pick_n, 2) if pick_n else None
    dexp["universeExtremeLowPbrPct"] = round(100 * uni_lowpbr / uni_n, 2) if uni_n else None
    dexp["lotDelistRatePct"] = round(100 * sum(1 for l in lots if l["delisted"]) / len(lots), 2)
    dexp["crashRatePct(<=-50%)"] = rd["bigLossRatePct(<=-50%)"]

    # ── §11 KOSDAQ dependency / §12 size exposure: 시장·size 단독 재실행 ──
    seg = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        c2 = RankCache(sn, nm, market=mkt)
        i2 = ew_universe_index(c2, ds)
        _, e2 = run_case(c2, ds, i2, **BASE_KW)
        if e2:
            seg[mkt] = {"cagrPct": pct(e2["twrCagr"]), "benchPct": pct(e2["benchCagr"]),
                        "excessPct": pct(e2["excess"]), "mddPct": pct(e2["mdd"])}
    sizes = {}
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        c3 = RankCache(sn, nm, size_bucket=b)
        i3 = ew_universe_index(c3, ds)
        _, e3 = run_case(c3, ds, i3, **BASE_KW)
        if e3:
            sizes[lbl] = {"cagrPct": pct(e3["twrCagr"]), "benchPct": pct(e3["benchCagr"]),
                          "excessPct": pct(e3["excess"])}

    # size tilt: pick 시총 percentile
    tilt = dist([l["_capPctile"] for l in lots if l.get("_capPctile") is not None])

    # ── §19 real money risk ──────────────────────────────────────────────
    nav = res["nav"]
    con, c = [], 0.0
    for x in res["inflow"]:
        c += x
        con.append(c)
    peak = nav[0]
    worst_dd_won = 0
    worst_at = None
    for i, v in enumerate(nav):
        peak = max(peak, v)
        if peak - v > worst_dd_won:
            worst_dd_won = peak - v
            worst_at = ds[i]
    # 5천만원이 최악에 얼마까지 내려갔나 (투입 완료 후 최저 계좌평가액)
    after = [(i, v) for i, v in enumerate(nav) if con[i] >= FROZEN["initialCapitalKrw"] - 1]
    low_i, low_v = min(after, key=lambda kv: kv[1]) if after else (None, None)
    # 원금(5천만) 대비 최저점
    ratio_low = min((v / con[i] for i, v in enumerate(nav) if con[i] > 0), default=None)
    ratio_low_at = None
    if ratio_low is not None:
        for i, v in enumerate(nav):
            if con[i] > 0 and abs(v / con[i] - ratio_low) < 1e-12:
                ratio_low_at = ds[i]
                break
    # 최악 코호트는 walkforward 모드에서 채운다
    risk = {"initialCapitalKrw": FROZEN["initialCapitalKrw"],
            "terminalWealth": round(nav[-1]),
            "benchTerminal": round(ev["benchTerminal"]),
            "worstDrawdownKrw": round(worst_dd_won), "worstDrawdownAt": worst_at,
            "mddPct": pct(ev["mdd"]),
            "lowestAccountValueKrw": round(low_v) if low_v is not None else None,
            "lowestAccountValueAt": ds[low_i] if low_i is not None else None,
            "lowestValueVsContributedPct": pct(ratio_low),
            "lowestValueVsContributedAt": ratio_low_at,
            "worst1yPct": pct(ev["worst1y"]),
            "worst1yKrwOn50m": round(FROZEN["initialCapitalKrw"] * (1 + ev["worst1y"]))
            if ev["worst1y"] is not None else None,
            "worst3yAnnualPct": pct(ev["worst3y"]),
            "worst3yTotalPct": pct((1 + ev["worst3y"]) ** 3 - 1) if ev["worst3y"] is not None else None,
            "longestUnderwaterMonths": ev["maxUnderwaterMonths"],
            "roll1yNegSharePct": pct(ev["roll1yNegShare"]),
            "note": "모두 과거 실측치다. 미래 예측이 아니다."}

    # benchmark 대비 부진 지속 기간
    rel = [res["nav"][i] / bench["nav"][i] if bench["nav"][i] > 0 else 1.0
           for i in range(len(ds))]
    rp, ruw, rcur = rel[0], 0, 0
    for v in rel:
        if v >= rp:
            rp, rcur = v, 0
        else:
            rcur += 1
            ruw = max(ruw, rcur)
    risk["longestBenchUnderperformMonths"] = ruw

    out = {"base": {k: (pct(ev[k]) if k in ("twrCagr", "benchCagr", "excess", "mdd",
                                            "worst1y", "worst3y", "roll1yNegShare",
                                            "costPctOfContrib", "vol")
                        else ev[k]) for k in ev},
           "leaveOneEraOut": {"full": {"cagrPct": pct(full_s["cagr"]),
                                       "benchPct": pct(full_b["cagr"]),
                                       "excessPct": pct(full_s["cagr"] - full_b["cagr"])},
                              "method": ("월별 TWR 시계열에서 해당 시대의 달을 제거하고 "
                                         "전략·benchmark 를 동일하게 재연쇄한다."),
                              "rows": loo,
                              "positiveCount": sum(1 for r in loo if r.get("positive")),
                              "total": sum(1 for r in loo if not r.get("skipped"))},
           "eraStandalone": era_only,
           "extremePeriods": extremes,
           "pnlByMarket": by_market, "pnlByEra": by_era,
           "pnlByMarketEra": {k: round(v) for k, v in sorted(by_market_era.items())},
           "pnlBySize": by_size, "sizeTiltPctileInUniverse": tilt,
           "marketSegments": seg, "sizeControls": sizes,
           "concentration": conc, "returnDistribution": rd,
           "distressExposure": dexp, "realMoneyRisk": risk,
           "burden": {"tradesPerYear": round(ev["trades"] / ev["years"], 1),
                      "buyMonths": res["buyMonths"], "avgPositions": round(ev["avgPositions"], 1),
                      "maxPositions": ev["maxPositions"], "uniqueEver": ev["uniqueEver"],
                      "level": "SIMPLE" if (ev["trades"] / ev["years"] <= 60
                                            and ev["avgPositions"] <= 50) else "COMPLEX"}}
    save("core", out)
    print(json.dumps({"looPositive": out["leaveOneEraOut"]["positiveCount"],
                      "looTotal": out["leaveOneEraOut"]["total"],
                      "top5SharePct": conc["top5SharePct"],
                      "kosdaqPnlSharePct": by_market.get("KOSDAQ", {}).get("pnlSharePct")},
                     ensure_ascii=False))
    return 0


# ═════════════════════════════ §6·7·8 stress ═════════════════════════════
def mode_stress(sn, nm, ds, cache, idx):
    base_res, base_ev = run_case(cache, ds, idx, **BASE_KW)

    # ── §7 비용 / 슬리피지 ────────────────────────────────────────────────
    cost_rows = []
    for lv in COST_LEVELS:
        _, e = run_case(cache, ds, idx, cost_bps=lv["feeBps"],
                        sell_tax_bps=lv["sellTaxBps"], slippage_bps=lv["slippageBps"],
                        **BASE_KW)
        cost_rows.append({"level": lv["id"], **{k: lv[k] for k in
                                                ("feeBps", "sellTaxBps", "slippageBps")},
                          "note": lv.get("note"),
                          "cagrPct": pct(e["twrCagr"]), "benchPct": pct(e["benchCagr"]),
                          "excessPct": pct(e["excess"]), "mddPct": pct(e["mdd"]),
                          "costDragPct": pct(base_ev["twrCagr"] - e["twrCagr"]),
                          "costPaidKrw": round(e["costPaid"]),
                          "slipPaidKrw": round(e["slipPaid"]),
                          "costPctOfContrib": pct(e["costPctOfContrib"]),
                          "terminalWealth": round(e["terminalWealth"])})

    # ── §6 상장폐지 ───────────────────────────────────────────────────────
    de_rows = []
    for lv in DELIST_LEVELS:
        h = lv["haircut"]
        # ASYMMETRIC — 전략만 haircut (R8 방식)
        _, ea = run_case(cache, ds, idx, delist_haircut=h, **BASE_KW)
        # SYMMETRIC — benchmark 지수에도 같은 haircut (방법론적으로 옳음)
        idx_h = ew_universe_index(cache, ds, delist_haircut=h)
        res_s = simulate(cache, ds, delist_haircut=h, **BASE_KW)
        bs = bench_same_schedule(idx_h, ds, tranches=res_s["tranches"])
        es = evaluate(res_s, bs)
        lot_del = [l for l in (res_s["ledger"] + res_s["openLots"]) if l["delisted"]]
        de_rows.append({
            "level": lv["id"], "haircut": h, "note": lv.get("note"),
            "delistEvents": es["delistEvents"],
            "delistedLots": len(lot_del),
            "delistLossKrw": round(res_s["delistLoss"]),
            "delistLossPctOfContrib": pct(res_s["delistLoss"] / es["contributed"])
            if es["contributed"] else None,
            "asymmetric": {"cagrPct": pct(ea["twrCagr"]), "benchPct": pct(ea["benchCagr"]),
                           "excessPct": pct(ea["excess"]), "mddPct": pct(ea["mdd"])},
            "symmetric": {"cagrPct": pct(es["twrCagr"]), "benchPct": pct(es["benchCagr"]),
                          "excessPct": pct(es["excess"]), "mddPct": pct(es["mdd"])}})

    # ── §8 유동성 / 체결가능성 ────────────────────────────────────────────
    liq_rows = []
    for lv in LIQUIDITY_STRESS:
        c = LiquidityRankCache(sn, nm, ds, min_market_cap=lv["minMarketCapEok"],
                               exclude_stale=lv["excludeStalePrice"])
        i2 = ew_universe_index(c, ds)
        _, e = run_case(c, ds, i2, **BASE_KW)
        usz = [len(c.universe(d)) for d in ds]
        liq_rows.append({"level": lv["id"], "minMarketCapEok": lv["minMarketCapEok"],
                         "excludeStalePrice": lv["excludeStalePrice"],
                         "note": lv.get("note"),
                         "avgUniverseSize": round(sum(usz) / len(usz), 1),
                         "cagrPct": pct(e["twrCagr"]) if e else None,
                         "benchPct": pct(e["benchCagr"]) if e else None,
                         "excessPct": pct(e["excess"]) if e else None,
                         "mddPct": pct(e["mdd"]) if e else None})

    # 체결 가능성 진단 — 스냅샷에 거래량이 없으므로 가격·시총·정지 proxy 만
    picks = [p for pl in base_res["pickLog"] for p in pl["picks"]]
    prices = [p["close"] for p in picks]
    caps = [p["marketCap"] for p in picks if p["marketCap"]]
    nav_at = {pl["idx"]: base_res["nav"][pl["idx"]] for pl in base_res["pickLog"]}
    impact = []
    for pl in base_res["pickLog"]:
        per_name = nav_at[pl["idx"]] / max(1, len(pl["picks"]))
        for p in pl["picks"]:
            if p["marketCap"]:
                impact.append(per_name / p["marketCap"])
    stale = halted = newly = 0
    for pl in base_res["pickLog"]:
        j = ds.index(pl["date"])
        for p in pl["picks"]:
            t = p["ticker"]
            if j >= 2:
                a = sn[ds[j - 1]].get(t, {}).get("close")
                b = sn[ds[j - 2]].get(t, {}).get("close")
                if a and b and p["close"] == a == b:
                    stale += 1
            if j >= 1 and t not in sn[ds[j - 1]]:
                newly += 1
    for d in ds:
        for t, r in sn[d].items():
            if r["close"] is None:
                halted += 1
    diag = {"pickCount": len(picks),
            "priceDist": dist(prices),
            "pricesBelow1000Pct": round(100 * sum(1 for x in prices if x < 1000) / len(prices), 2),
            "pricesBelow500Pct": round(100 * sum(1 for x in prices if x < 500) / len(prices), 2),
            "marketCapDistEok": {k: (round(v / 1e8) if isinstance(v, (int, float)) else v)
                                 for k, v in (dist(caps) or {}).items()},
            "positionVsMarketCapPct": {k: pct(v) for k, v in (dist(impact) or {}).items()
                                       if k != "n"},
            "stalePricePicksPct": round(100 * stale / len(picks), 2),
            "newlyListedPicksPct": round(100 * newly / len(picks), 2),
            "snapshotRowsWithNoClose": halted,
            "dataLimitation": ("스냅샷 컬럼은 ticker,market,close,marketCap,shares,PER,PBR,"
                              "EPS,BPS,DIV,DPS 뿐이다. 거래량·거래대금·거래정지 플래그가 "
                              "없으므로 유동성 검증은 가격·시총·종가무변동 proxy 로만 했다. "
                              "실제 체결 가능성 검증은 미완이다.")}

    out = {"costStress": {"levels": cost_rows,
                          "note": ("benchmark(동일가중 월 리밸런싱 지수)에는 비용을 물리지 "
                                   "않았다. 즉 benchmark 가 비현실적으로 싸다 — 전략에만 "
                                   "불리한 보수적 비교다.")},
           "delistingStress": {"gateMode": "SYMMETRIC", "levels": de_rows},
           "liquidityStress": {"levels": liq_rows, "diagnostics": diag,
                               "note": "BASE frozen universe 결과와 별도로 표시(§8)."},
           "base": {"cagrPct": pct(base_ev["twrCagr"]), "excessPct": pct(base_ev["excess"])}}
    save("stress", out)
    print(json.dumps({"costHighExcessPct": cost_rows[-1]["excessPct"],
                      "delist100SymExcessPct": de_rows[-1]["symmetric"]["excessPct"],
                      "delist100AsymExcessPct": de_rows[-1]["asymmetric"]["excessPct"],
                      "liqExcessPct": [r["excessPct"] for r in liq_rows]},
                     ensure_ascii=False))
    return 0


# ═════════════════════════ §2·4 walk-forward / cohort ═════════════════════════
def bm_signal(sn, cache, dates, lo, hi, horizon=12):
    """구간 [lo,hi) 에서 BM 상위20% vs 하위20% 의 forward return 차이(연율 근사).
    '과거 정보만으로 BM 신호가 보였는가'를 문서화하는 용도(§2)."""
    from factor_research import factor_values
    tops, bots = [], []
    for i in range(lo, hi - horizon):
        d = dates[i]
        u = cache.universe(d)
        fv = factor_values(u).get("BM", {})
        items = sorted(((t, v) for t, v in fv.items() if t in u), key=lambda kv: -kv[1])
        if len(items) < 50:
            continue
        k = max(1, len(items) // 5)
        fut = sn[dates[i + horizon]]

        def mret(sel):
            rs = []
            for t, _ in sel:
                p0 = u[t]["close"]
                r1 = fut.get(t)
                p1 = r1["close"] if (r1 and r1["close"]) else None
                if p0 and p1:
                    rs.append(p1 / p0 - 1)
                elif p0:
                    rs.append(-1.0)          # 상장폐지 = 전액 손실로 보수 처리
            return sum(rs) / len(rs) if rs else None
        a, b = mret(items[:k]), mret(items[-k:])
        if a is not None and b is not None:
            tops.append(a)
            bots.append(b)
    if not tops:
        return None
    ta = sum(tops) / len(tops)
    ba = sum(bots) / len(bots)
    return {"months": len(tops), "topQuintileFwd12mPct": pct(ta),
            "bottomQuintileFwd12mPct": pct(ba), "spreadPct": pct(ta - ba),
            "signalPositive": ta > ba}


def min_months_for(h, hold=None):
    """horizon h 개월 동안 규칙이 **현금에 잠기지 않고** 굴러가려면 필요한 최소 창 길이.

    ★ R9 자체감사에서 잡은 harness 결함(§23)
      buy_every=24·hold=24 규칙은 `i + hold < n` 일 때만 매수한다. 창이 48개월이면
      i=0 에서 1회만 사고 i=24 는 (24+24 !< 48) 차단돼 **뒤 24개월을 100% 현금**으로
      보낸다. 그 상태로 재면 규칙이 아니라 '반만 투자한 계좌'를 재는 것이다.
      (첫 측정에서 walk-forward 3/29 라는 값이 나왔던 원인 — 규칙 결함이 아니라
       검증 창 설계 결함이었다. frozen candidate 는 그대로 두고 창 길이만 고쳤다.)
    """
    hold = hold or P["hold"]
    be = P["buy_every"]
    last_needed_buy = be * ((max(1, h) - 1) // be)
    return last_needed_buy + hold + 1


def mode_walkforward(sn, nm, ds, cache, idx):
    n = len(ds)
    need = P["hold"] + 4

    # ── §2 retrospective walk-forward ────────────────────────────────────
    # 창 길이는 코호트가 통째로 들어가는 길이만 쓴다(위 min_months_for 주석 참조).
    #   49개월 = 코호트 2개 · 73개월 = 코호트 3개. 둘 다 현금잠김 0.
    rows = []
    for test_len, kind in ((49, "EXPANDING_TRAIN_2COHORT_49M_TEST"),
                           (73, "EXPANDING_TRAIN_3COHORT_73M_TEST")):
        for cut in range(60, n - test_len + 1, 12):
            ins = bm_signal(sn, cache, ds, 0, cut)
            sub = ds[cut:cut + test_len]
            _, e = run_case(cache, sub, slice_index(idx, cut)[:len(sub)], **BASE_KW)
            rows.append({"kind": kind, "testMonths": test_len,
                         "trainFrom": ds[0], "trainTo": ds[cut - 1],
                         "testFrom": sub[0], "testTo": sub[-1],
                         "inSampleBmSignal": ins,
                         "cagrPct": pct(e["twrCagr"]) if e else None,
                         "benchPct": pct(e["benchCagr"]) if e else None,
                         "excessPct": pct(e["excess"]) if e else None,
                         "mddPct": pct(e["mdd"]) if e else None,
                         "positive": bool(e and e["excess"] and e["excess"] > 0)})
    roll = []
    for cut in range(60, n - 49 + 1, 12):
        ins = bm_signal(sn, cache, ds, cut - 60, cut)
        sub = ds[cut:cut + 49]
        _, e = run_case(cache, sub, slice_index(idx, cut)[:len(sub)], **BASE_KW)
        roll.append({"kind": "ROLLING60_TRAIN_2COHORT_49M_TEST", "testMonths": 49,
                     "trainFrom": ds[cut - 60], "trainTo": ds[cut - 1],
                     "testFrom": sub[0], "testTo": sub[-1],
                     "inSampleBmSignal": ins,
                     "cagrPct": pct(e["twrCagr"]) if e else None,
                     "benchPct": pct(e["benchCagr"]) if e else None,
                     "excessPct": pct(e["excess"]) if e else None,
                     "positive": bool(e and e["excess"] and e["excess"] > 0)})
    # expanding-to-end
    tail = []
    for cut in range(60, n - need, 24):
        sub = ds[cut:]
        _, e = run_case(cache, sub, slice_index(idx, cut), **BASE_KW)
        tail.append({"kind": "EXPANDING_TRAIN_TEST_TO_END",
                     "trainTo": ds[cut - 1], "testFrom": sub[0], "testTo": sub[-1],
                     "testMonths": len(sub),
                     "cagrPct": pct(e["twrCagr"]) if e else None,
                     "benchPct": pct(e["benchCagr"]) if e else None,
                     "excessPct": pct(e["excess"]) if e else None,
                     "positive": bool(e and e["excess"] and e["excess"] > 0)})

    allw = rows + roll + tail
    npos = sum(1 for r in allw if r["positive"])
    wf = {"honesty": ("R7/R8 이 전체 2007~2026 데이터로 후보를 찾았다. 따라서 이것은 "
                      "진정한 untouched holdout 이 아니다. 과거 정보 → 규칙 고정 → "
                      "이후 구간 평가 구조만 재현한 retrospective walk-forward 다(§2)."),
          "harnessFix": ("검증 창은 24개월 코호트가 통째로 들어가는 길이(49·73개월)만 쓴다. "
                         "48개월 창은 두 번째 코호트 매수가 차단돼 뒤 24개월이 100% 현금이 "
                         "되므로 규칙 성능이 아니라 창 설계 결함을 재게 된다(§23 자체감사)."),
          "windows": allw, "positive": npos, "total": len(allw),
          "positiveRatePct": round(100 * npos / len(allw), 1) if allw else None,
          "byKind": {}}
    for k in sorted({r["kind"] for r in allw}):
        sel = [r for r in allw if r["kind"] == k]
        wf["byKind"][k] = {"n": len(sel),
                           "positive": sum(1 for r in sel if r["positive"]),
                           "medianExcessPct": quant([r["excessPct"] for r in sel
                                                     if r["excessPct"] is not None], 0.5)}

    # ── §4 rolling cohort distribution ───────────────────────────────────
    horizons = [12, 36, 60, 120]
    cohorts = []
    for o in range(0, n - need + 1):
        sub = ds[o:]
        sidx = slice_index(idx, o)
        res, e = run_case(cache, sub, sidx, **BASE_KW)
        if not res:
            continue
        bn = bench_same_schedule(sidx, sub, tranches=res["tranches"])["nav"]
        sr = monthly_twr(res["nav"], res["inflow"])
        br = monthly_twr(bn, res["inflow"])
        con, c = [], 0.0
        for x in res["inflow"]:
            c += x
            con.append(c)
        row = {"offset": o, "start": sub[0], "months": len(sub), "h": {}}
        for h in horizons:
            # 현금잠김 창은 제외한다(§23 harness 결함 — min_months_for 주석 참조)
            if h >= len(sub) or len(sub) < min_months_for(h):
                continue
            sg = bg = 1.0
            for j in range(h):
                sg *= 1 + sr[j]
                bg *= 1 + br[j]
            sa = sg ** (12 / h) - 1
            ba = bg ** (12 / h) - 1
            row["h"][str(h)] = {
                "annualPct": pct(sa), "benchAnnualPct": pct(ba),
                "excessPct": pct(sa - ba), "totalPct": pct(sg - 1),
                "accountVsContributedPct": pct(res["nav"][h] / con[h] - 1) if con[h] else None,
                "accountKrw": round(res["nav"][h]),
                "loss": res["nav"][h] < con[h]}
        cohorts.append(row)

    summary = {}
    for h in horizons:
        ex = [c["h"][str(h)]["excessPct"] for c in cohorts if str(h) in c["h"]]
        ab = [c["h"][str(h)]["annualPct"] for c in cohorts if str(h) in c["h"]]
        acc = [c["h"][str(h)]["accountVsContributedPct"] for c in cohorts
               if str(h) in c["h"] and c["h"][str(h)]["accountVsContributedPct"] is not None]
        loss = [c["h"][str(h)]["loss"] for c in cohorts if str(h) in c["h"]]
        if not ex:
            continue
        summary[str(h)] = {
            "cohorts": len(ex),
            "excess": {k: (round(v, 2) if isinstance(v, float) else v)
                       for k, v in (dist(ex) or {}).items()},
            "annual": {k: (round(v, 2) if isinstance(v, float) else v)
                       for k, v in (dist(ab) or {}).items()},
            "accountVsContributed": {k: (round(v, 2) if isinstance(v, float) else v)
                                     for k, v in (dist(acc) or {}).items()},
            "positiveExcessRatePct": round(100 * sum(1 for x in ex if x > 0) / len(ex), 1),
            "lossRatePct": round(100 * sum(1 for x in loss if x) / len(loss), 1)}

    worst = None
    if cohorts:
        c60 = [c for c in cohorts if "60" in c["h"]]
        if c60:
            w = min(c60, key=lambda c: c["h"]["60"]["accountVsContributedPct"])
            worst = {"start": w["start"], "horizonMonths": 60,
                     "accountVsContributedPct": w["h"]["60"]["accountVsContributedPct"],
                     "accountKrw": w["h"]["60"]["accountKrw"],
                     "annualPct": w["h"]["60"]["annualPct"],
                     "benchAnnualPct": w["h"]["60"]["benchAnnualPct"],
                     "excessPct": w["h"]["60"]["excessPct"]}
    worst1y = None
    c12 = [c for c in cohorts if "12" in c["h"]]
    if c12:
        w = min(c12, key=lambda c: c["h"]["12"]["accountVsContributedPct"])
        worst1y = {"start": w["start"], "horizonMonths": 12,
                   "accountVsContributedPct": w["h"]["12"]["accountVsContributedPct"],
                   "accountKrw": w["h"]["12"]["accountKrw"]}
    worst3y = None
    c36 = [c for c in cohorts if "36" in c["h"]]
    if c36:
        w = min(c36, key=lambda c: c["h"]["36"]["accountVsContributedPct"])
        worst3y = {"start": w["start"], "horizonMonths": 36,
                   "accountVsContributedPct": w["h"]["36"]["accountVsContributedPct"],
                   "accountKrw": w["h"]["36"]["accountKrw"]}

    out = {"walkForward": wf,
           "cohortDistribution": {
               "method": ("각 시작월마다 5천만원으로 frozen rule 을 새로 시작해 끝까지 "
                          "굴린 뒤, 1/3/5/10년 시점을 잘라 본다. 코호트는 서로 겹치므로 "
                          "완전 독립이 아니다(반독립) — 그대로 명시한다."),
               "horizons": horizons, "summary": summary,
               "worstStart5y": worst, "worstStart1y": worst1y, "worstStart3y": worst3y,
               "rows": cohorts}}
    save("walkforward", out)
    print(json.dumps({"wfPositive": f"{npos}/{len(allw)}",
                      "cohort5yPositiveExcessPct": summary.get("60", {}).get("positiveExcessRatePct"),
                      "cohort5yExcessP10": summary.get("60", {}).get("excess", {}).get("p10")},
                     ensure_ascii=False))
    return 0


# ═════════════════════════════ §13 controls ═════════════════════════════
def mode_controls(sn, nm, ds, cache, idx):
    _, base = run_case(cache, ds, idx, **BASE_KW)
    modes = ("RANDOM", "SIZE_MATCHED", "MARKET_MATCHED")
    out = {"candidate": {"cagrPct": pct(base["twrCagr"]), "excessPct": pct(base["excess"])},
           "matching": ("통제군은 시작일·현금투입 스케줄·종목수·보유기간·교체주기·비용이 "
                        "candidate 와 완전히 같다. selection factor 만 제거했다."),
           "controls": {}}
    for mode in modes:
        rows = []
        for s in CONTROL_SEEDS:
            _, e = run_case(cache, ds, idx, selection=mode, seed=s, **BASE_KW)
            if e:
                rows.append({"seed": s, "cagrPct": pct(e["twrCagr"]),
                             "excessPct": pct(e["excess"]), "mddPct": pct(e["mdd"])})
        ex = [r["excessPct"] for r in rows]
        cand = pct(base["excess"])
        better = sum(1 for x in ex if x < cand)
        out["controls"][mode] = {
            "seeds": len(rows), "excess": {k: (round(v, 2) if isinstance(v, float) else v)
                                           for k, v in (dist(ex) or {}).items()},
            "candidateExcessPct": cand,
            "candidatePercentile": round(100 * better / len(ex), 1) if ex else None,
            "controlsBeatingCandidate": sum(1 for x in ex if x >= cand),
            "meanExcessPct": round(sum(ex) / len(ex), 2) if ex else None,
            "rows": rows}
    save("controls", out)
    print(json.dumps({m: {"pctile": out["controls"][m]["candidatePercentile"],
                          "meanExcess": out["controls"][m]["meanExcessPct"]}
                      for m in modes}, ensure_ascii=False))
    return 0


# ═════════════════════════════ §9 data quality ═════════════════════════════
def mode_dataquality(sn, nm, ds, cache, idx):
    res, _ = run_case(cache, ds, idx, **BASE_KW)

    # universe 수준 PBR/BPS 결측·이상치
    tot = miss = zero = neg = ext_hi = ext_lo = bps_miss = bps_neg = 0
    for d in ds:
        for t, r in cache.universe(d).items():
            tot += 1
            p, b = r["PBR"], r["BPS"]
            if p is None:
                miss += 1
            elif p == 0:
                zero += 1
            elif p < 0:
                neg += 1
            elif p > 100:
                ext_hi += 1
            elif p < 0.01:
                ext_lo += 1
            if b is None:
                bps_miss += 1
            elif b <= 0:
                bps_neg += 1
    quality = {"universeRowMonths": tot,
               "pbrMissingPct": round(100 * miss / tot, 3),
               "pbrZeroPct": round(100 * zero / tot, 3),
               "pbrNegativePct": round(100 * neg / tot, 3),
               "pbrExtremeHighPct(>100)": round(100 * ext_hi / tot, 3),
               "pbrExtremeLowPct(<0.01)": round(100 * ext_lo / tot, 3),
               "bpsMissingPct": round(100 * bps_miss / tot, 3),
               "bpsNonPositivePct": round(100 * bps_neg / tot, 3)}

    # PBR 내부정합성: PBR ≈ close / BPS ?
    diffs = []
    for d in ds[::12]:
        for t, r in cache.universe(d).items():
            p, c, b = r["PBR"], r["close"], r["BPS"]
            if p and c and b and b > 0:
                imp = c / b
                diffs.append(abs(imp - p) / p)
    quality["pbrVsCloseOverBps"] = {
        "checked": len(diffs),
        "medianRelDiffPct": pct(quant(diffs, 0.5)),
        "within5PctShare": pct(sum(1 for x in diffs if x <= 0.05) / len(diffs)) if diffs else None,
        "interpretation": ("PBR = 당일 종가 / 직전 공시 BPS 로 일관되면 그 시점 정보만 "
                           "쓴 것이다. 큰 불일치가 다수면 KRX 값이 다른 기준이라는 뜻이다.")}

    # BPS 개정 시점 분포 = publication timing (look-ahead 핵심 검사)
    month_hist = {}
    gaps = []
    sample = sorted({t for d in ds[::6] for t in cache.universe(d)})[:1500]
    for t in sample:
        prev = None
        prev_i = None
        for i, d in enumerate(ds):
            r = sn[d].get(t)
            if not r:
                continue
            b = r["BPS"]
            if b is None:
                continue
            if prev is not None and abs(b - prev) > 1e-9:
                mm = int(d[5:7])
                month_hist[mm] = month_hist.get(mm, 0) + 1
                if prev_i is not None:
                    gaps.append(i - prev_i)
                prev_i = i
            elif prev is None:
                prev_i = i
            prev = b
    tot_ch = sum(month_hist.values()) or 1
    quality["bpsRevisionMonthHistogram"] = {str(k): month_hist.get(k, 0) for k in range(1, 13)}
    quality["bpsRevisionMonthSharePct"] = {str(k): round(100 * month_hist.get(k, 0) / tot_ch, 1)
                                           for k in range(1, 13)}
    quality["bpsRevisionMedianGapMonths"] = quant(gaps, 0.5)
    quality["bpsRevisionTickersSampled"] = len(sample)
    q1 = sum(month_hist.get(m, 0) for m in (1, 2)) / tot_ch
    q_apr = sum(month_hist.get(m, 0) for m in (4, 5)) / tot_ch
    quality["lookAheadVerdict"] = {
        "janFebRevisionSharePct": round(100 * q1, 1),
        "aprMayRevisionSharePct": round(100 * q_apr, 1),
        "test": ("결산기말(12월) 직후 1~2월에 BPS 가 몰려 바뀌면 공시 이전 재무를 쓴 것이라 "
                 "look-ahead 다. 사업보고서 제출월(3~4월)·분기보고서(5·8·11월) 이후에 "
                 "바뀌면 PIT 로 정상이다."),
        "leakSuspected": q1 > q_apr}

    # 결측 제외로 인한 선택편향: PBR 유효 vs 무효 종목의 24개월 forward return
    val_r, inv_r = [], []
    for i in range(0, len(ds) - P["hold"], 6):
        d = ds[i]
        fut = sn[ds[i + P["hold"]]]
        for t, r in cache.universe(d).items():
            p0 = r["close"]
            if not p0:
                continue
            f = fut.get(t)
            p1 = f["close"] if (f and f["close"]) else 0.0
            ret = p1 / p0 - 1
            (val_r if (r["PBR"] and r["PBR"] > 0) else inv_r).append(ret)
    quality["missingDataSelectionBias"] = {
        "validPbrFwd24m": {"n": len(val_r),
                           "meanPct": pct(sum(val_r) / len(val_r)) if val_r else None,
                           "medianPct": pct(quant(val_r, 0.5))},
        "invalidPbrFwd24m": {"n": len(inv_r),
                             "meanPct": pct(sum(inv_r) / len(inv_r)) if inv_r else None,
                             "medianPct": pct(quant(inv_r, 0.5))},
        "invalidShareOfUniversePct": round(100 * len(inv_r) / (len(val_r) + len(inv_r)), 2)
        if (val_r or inv_r) else None,
        "note": ("결측 PBR 종목을 버리는 것이 성과를 만들어낸 것이라면, 버려진 쪽의 "
                 "forward return 이 크게 낮아야 한다. 상장폐지는 -100% 로 처리했다.")}

    # picks 의 PBR
    pp = [p["pbr"] for pl in res["pickLog"] for p in pl["picks"] if p["pbr"]]
    quality["pickPbr"] = dist(pp)
    quality["pickPbrBelow0_3Pct"] = round(
        100 * sum(1 for x in pp if x < 0.3) / len(pp), 1) if pp else None

    # 치명적 결함 판정
    fatal = []
    if quality["lookAheadVerdict"]["leakSuspected"]:
        fatal.append("BPS 개정이 공시 이전(1~2월)에 몰려 look-ahead 의심")
    if (quality["pbrVsCloseOverBps"]["within5PctShare"] or 0) < 80:
        fatal.append("PBR 이 종가/BPS 와 크게 불일치 — 값의 출처 불명")
    bias = None
    if val_r and inv_r:
        bias = (sum(val_r) / len(val_r)) - (sum(inv_r) / len(inv_r))
        if bias > 0.30:
            fatal.append(f"결측 제외로 인한 선택편향 과대(24개월 평균 차 {pct(bias)}%p)")
    quality["fatalIssues"] = fatal
    quality["fatalCount"] = len(fatal)
    quality["selectionBiasSpreadPct"] = pct(bias) if bias is not None else None
    save("dataquality", quality)
    print(json.dumps({"fatal": fatal, "leakSuspected": quality["lookAheadVerdict"]["leakSuspected"],
                      "pbrConsistencyWithin5Pct": quality["pbrVsCloseOverBps"]["within5PctShare"]},
                     ensure_ascii=False))
    return 0


# ═════════════════════════════ §16 benchmarks ═════════════════════════════
def official_monthly(name, ds):
    p = ROOT / "_cache" / "pit-snapshots" / "_official-index.json"
    if not p.exists():
        return None
    d = json.loads(p.read_text(encoding="utf-8")).get(name)
    if not d:
        return None
    keys = sorted(d)
    out = []
    import bisect
    for x in ds:
        j = bisect.bisect_right(keys, x) - 1
        if j < 0:
            return None
        out.append(d[keys[j]])
    b0 = out[0]
    return [v / b0 for v in out]


def mode_benchmarks(sn, nm, ds, cache, idx):
    res, ev = run_case(cache, ds, idx, **BASE_KW)
    tr = res["tranches"]
    con, c = [], 0.0
    for x in res["inflow"]:
        c += x
        con.append(c)
    years = ev["years"]

    def cagr_of(nav):
        twr, pn, pc = 1.0, nav[0], con[0]
        for k in range(1, len(nav)):
            base = pn + (con[k] - pc)
            if base > 0 and nav[k] > 0:
                twr *= nav[k] / base
            pn, pc = nav[k], con[k]
        return twr ** (1 / years) - 1 if twr > 0 else None

    rows = []

    def add(label, series, note=None):
        if not series:
            return
        bn = bench_same_schedule(series, ds, tranches=tr)["nav"]
        cg = cagr_of(bn)
        rows.append({"benchmark": label, "cagrPct": pct(cg),
                     "candidateExcessPct": pct(ev["twrCagr"] - cg) if cg is not None else None,
                     "terminal": round(bn[-1]), "note": note})

    add("FAIR_EW_UNIVERSE", idx, "R8 기본 benchmark — survivorship-free 동일가중, 동일 투입스케줄")
    add("OFFICIAL_KOSPI", official_monthly("KOSPI", ds), "KRX 공식 KOSPI(시총가중, 배당 제외)")
    add("OFFICIAL_KOSDAQ", official_monthly("KOSDAQ", ds), "KRX 공식 KOSDAQ")
    kp, kq = official_monthly("KOSPI", ds), official_monthly("KOSDAQ", ds)
    if kp and kq:
        # 시총가중 결합 proxy — universe 실측 시총 비중으로 매월 가중
        w = []
        for d in ds:
            u = cache.universe(d)
            a = sum(r["marketCap"] or 0 for r in u.values() if r["market"] == "KOSPI")
            b = sum(r["marketCap"] or 0 for r in u.values() if r["market"] == "KOSDAQ")
            w.append(a / (a + b) if (a + b) else 0.9)
        comb = [1.0]
        for i in range(1, len(ds)):
            g = w[i - 1] * (kp[i] / kp[i - 1]) + (1 - w[i - 1]) * (kq[i] / kq[i - 1])
            comb.append(comb[-1] * g)
        add("COMBINED_MARKET_PROXY", comb, "KOSPI/KOSDAQ 실측 시총비중 월별 가중")
    for mkt in ("KOSPI", "KOSDAQ"):
        c2 = RankCache(sn, nm, market=mkt)
        add(f"EW_{mkt}_UNIVERSE", ew_universe_index(c2, ds), f"{mkt} 동일가중 universe")

    # ★ 결측 PBR 제외 효과를 benchmark 로 분리한다 (§9 선택편향 → §16 benchmark 도전)
    #   전략은 PBR 이 없는 종목을 애초에 담을 수 없다. 그런데 기본 benchmark 는 그 종목을
    #   포함한다(investable_universe 는 PBR 유효성을 요구하지 않는다). 그 종목들의 이후
    #   수익률이 나쁘면, '담을 수 없어서 피한 것'이 BM alpha 로 오인된다.
    class ValidPbrCache(RankCache):
        def universe(self, d):
            if d not in self._uni:
                u = super().universe(d)
                self._uni[d] = {t: r for t, r in u.items() if r["PBR"] and r["PBR"] > 0}
            return self._uni[d]
    add("EW_VALID_PBR_UNIVERSE", ew_universe_index(ValidPbrCache(sn, nm), ds),
        "전략이 실제로 랭킹할 수 있었던 종목(PBR 유효)만의 동일가중 — 결측제외 효과 분리")

    # size-matched benchmark = SIZE_MATCHED 통제군 평균 CAGR
    sm = None
    try:
        ctl = load("controls")["controls"]["SIZE_MATCHED"]
        sm = ctl["cagrMean"] if "cagrMean" in ctl else None
        if sm is None:
            cg = [r["cagrPct"] for r in ctl["rows"]]
            sm = round(sum(cg) / len(cg), 2) if cg else None
        rows.append({"benchmark": "SIZE_MATCHED_CONTROL_MEAN", "cagrPct": sm,
                     "candidateExcessPct": round(pct(ev["twrCagr"]) - sm, 2) if sm else None,
                     "terminal": None,
                     "note": "동일 구조·동일 시총분포 랜덤선정 30 seed 평균(§13)"})
    except (OSError, ValueError, KeyError):
        pass

    worst = min((r for r in rows if r["candidateExcessPct"] is not None),
                key=lambda r: r["candidateExcessPct"], default=None)
    out = {"candidateCagrPct": pct(ev["twrCagr"]), "rows": rows,
           "worstCaseBenchmark": worst,
           "positiveAgainst": sum(1 for r in rows if (r["candidateExcessPct"] or 0) > 0),
           "total": len(rows),
           "note": ("후보에게 가장 유리한 benchmark 만 고르지 않는다(§16). 공식 지수는 "
                    "배당 제외 시총가중이라 동일가중 universe 와 성격이 다르다.")}
    save("benchmarks", out)
    print(json.dumps({"rows": [(r["benchmark"], r["candidateExcessPct"]) for r in rows],
                      "worst": worst and worst["benchmark"]}, ensure_ascii=False))
    return 0


# ═════════════════════════════ §17 verdict ═════════════════════════════
def mode_verdict():
    rep, core, stress = load("repro"), load("core"), load("stress")
    wf, ctl, dq, bm = load("walkforward"), load("controls"), load("dataquality"), load("benchmarks")
    ca = load("cashaudit")

    base_ex = rep["base"]["excessPct"]
    g = {}

    g["G1_BASE_EXCESS"] = {"pass": base_ex >= 2.00,
                           "evidence": f"base excess {base_ex:+.2f}%p (기준 >= +2.00%p)"}

    w = wf["walkForward"]
    g["G2_WALK_FORWARD"] = {"pass": w["positive"] > w["total"] / 2,
                            "evidence": f"out-of-sample 창 {w['positive']}/{w['total']} positive "
                                        f"({w['positiveRatePct']}%)"}

    loo = core["leaveOneEraOut"]
    g["G3_LEAVE_ONE_ERA_OUT"] = {"pass": loo["positiveCount"] >= 5,
                                 "evidence": f"시대 1개 제거 {loo['positiveCount']}/{loo['total']} "
                                             f"에서 excess > 0 (기준 >= 5)"}

    c5 = wf["cohortDistribution"]["summary"].get("60", {})
    rate = c5.get("positiveExcessRatePct")
    p10 = (c5.get("excess") or {}).get("p10")
    g["G4_COHORT_STABILITY"] = {
        "pass": bool(rate is not None and p10 is not None and rate >= 60 and p10 > -5.0),
        "evidence": f"5년 cohort positive-excess {rate}% (>=60%) · excess p10 {p10}%p (> -5.00)"}

    hi = [r for r in stress["costStress"]["levels"] if r["level"] == "HIGH"][0]
    g["G5_COST_HIGH"] = {"pass": (hi["excessPct"] or -1) > 0,
                         "evidence": f"HIGH(수수료 20bp·세 23bp·슬리피지 100bp) excess "
                                     f"{hi['excessPct']:+.2f}%p"}

    d100 = [r for r in stress["delistingStress"]["levels"] if r["level"] == "LOSS_100"][0]
    g["G6_DELIST_100"] = {"pass": (d100["symmetric"]["excessPct"] or -1) > 0,
                          "evidence": f"상폐 100% SYMMETRIC excess "
                                      f"{d100['symmetric']['excessPct']:+.2f}%p "
                                      f"(ASYMMETRIC {d100['asymmetric']['excessPct']:+.2f}%p)"}

    pctiles = {m: ctl["controls"][m]["candidatePercentile"] for m in ctl["controls"]}
    g["G7_MATCHED_CONTROLS"] = {
        "pass": all((v or 0) >= 90 for v in pctiles.values()),
        "evidence": " · ".join(f"{m} percentile {v}" for m, v in pctiles.items())}

    r5 = [r for r in core["concentration"]["removal"] if r["topK"] == 5][0]
    fo = r5["firstOrder"]["excessPct"]
    rs = (r5["resimulated"] or {}).get("excessPct")
    g["G8_CONCENTRATION"] = {
        "pass": bool(fo is not None and fo > 0 and rs is not None and rs > 0),
        "evidence": f"상위5 제거 — 1차근사 excess {fo:+.2f}%p · 재시뮬 excess "
                    f"{rs:+.2f}%p (둘 다 > 0 요구). 상위5 기여비중 "
                    f"{core['concentration']['top5SharePct']}%"}

    g["G9_DATA_QUALITY"] = {"pass": dq["fatalCount"] == 0,
                            "evidence": ("치명적 결함 0" if dq["fatalCount"] == 0
                                         else "; ".join(dq["fatalIssues"]))}

    b = core["burden"]
    g["G10_HUMAN_EXECUTABLE"] = {
        "pass": b["tradesPerYear"] <= 60 and b["avgPositions"] <= 50,
        "evidence": f"연 {b['tradesPerYear']}건 · 평균 {b['avgPositions']}종목 "
                    f"· 매수월 {b['buyMonths']}회 · {b['level']}"}

    for k in g:
        g[k]["rule"] = next(x["rule"] for x in GATES if x["id"] == k)

    npass = sum(1 for v in g.values() if v["pass"])
    fails = [k for k, v in g.items() if not v["pass"]]
    loo_major_neg = loo["positiveCount"] < loo["total"] / 2

    if base_ex <= 0 or not g["G9_DATA_QUALITY"]["pass"] or loo_major_neg:
        verdict = "REJECT"
    elif npass == len(g):
        verdict = "ROBUST_CANDIDATE"
    elif (base_ex > 0 and g["G9_DATA_QUALITY"]["pass"] and g["G10_HUMAN_EXECUTABLE"]["pass"]
          and (g["G5_COST_HIGH"]["pass"] or g["G6_DELIST_100"]["pass"]) and npass >= 6):
        verdict = "PROMISING_FORWARD_TEST"
    else:
        verdict = "FRAGILE"

    # ── post-hoc 발견 — 사전 게이트가 없던 항목. 판정을 바꾸지 않고 그대로 표면화한다.
    #    (결과를 보고 게이트를 추가하면 §17 위반이므로 '판정 미반영' 으로 명시한다)
    rows5 = {}
    for r in wf["cohortDistribution"]["rows"]:
        if "60" in r["h"]:
            rows5.setdefault(r["start"][:4], []).append(r["h"]["60"]["excessPct"])
    by_year = {y: {"n": len(v), "medianExcessPct": round(quant(v, 0.5), 2),
                   "positive": sum(1 for x in v if x > 0)}
               for y, v in sorted(rows5.items())}
    y2007 = by_year.get("2007", {})
    later = [x for y, v in rows5.items() if y != "2007" for x in v]
    valid_pbr = next((r for r in bm["rows"] if r["benchmark"] == "EW_VALID_PBR_UNIVERSE"), None)
    e6 = next((r for r in core["eraStandalone"] if r["era"] == "E6_TIGHTEN"), None)
    post = {
        "note": ("아래는 사전확정 게이트에 없던 항목이다. 판정에 반영하지 않았다 — "
                 "결과를 보고 게이트를 추가하는 것이 §17 위반이기 때문이다. "
                 "다만 Founder 가 판정 라벨만 보고 오해하지 않도록 그대로 적는다."),
        "startYearDependency": {
            "cohort5yExcessByStartYear": by_year,
            "start2007": y2007,
            "start2008AndLater": {"n": len(later),
                                  "medianExcessPct": round(quant(later, 0.5), 2),
                                  "positivePct": round(100 * sum(1 for x in later if x > 0)
                                                       / len(later), 1) if later else None},
            "reading": ("2007년 시작 코호트만 압도적으로 좋고 2008년 이후 시작은 중앙값이 "
                        "음수다. R8 의 '시작시점 12개 중 10개 초과'는 12개 모두 2007년 안의 "
                        "월별 offset 이라 같은 시대를 공유한다 — 안정성 근거가 아니었다.")},
        "validPbrBenchmark": {
            "candidateExcessVsFairEwPct": rep["base"]["excessPct"],
            "candidateExcessVsValidPbrEwPct": valid_pbr and valid_pbr["candidateExcessPct"],
            "attributableToMissingPbrExclusionPct": (
                round(rep["base"]["excessPct"] - valid_pbr["candidateExcessPct"], 2)
                if valid_pbr and valid_pbr["candidateExcessPct"] is not None else None),
            "reading": ("전략은 PBR 결측 종목을 담을 수 없다. 기본 benchmark 는 담는다. "
                        "그 종목들의 24개월 forward return 이 유효군보다 "
                        f"{dq['selectionBiasSpreadPct']}%p 낮다. 초과수익의 상당부분이 "
                        "'BM 이 좋아서'가 아니라 '분석 불가 종목을 못 담아서'다.")},
        "recentEra": {"E6_2022_2026_standaloneExcessPct": e6 and e6["excessPct"],
                      "reading": "가장 최근 시대(2022~2026, 56개월) 단독 초과수익은 음수다."},
        "concentration": {
            "top1SharePct": (core["concentration"]["top"][0]["pnlSharePct"]
                             if core["concentration"]["top"] else None),
            "top5SharePct": core["concentration"]["top5SharePct"],
            "reading": ("총 손익의 대부분이 극소수 종목에서 나왔다. 상위 5종목을 빼면 "
                        "1차근사 초과수익이 음수가 된다.")},
        "r8SensitivityStructureBug": rep.get("r8SensitivityStructureBug", {}).get("impact"),
        "entryScheduleDefect": {
            "avgCashFirst24mPct": (ca.get("entryScheduleDefect") or {}).get("firstCohortAvgCashPct"),
            "cash2007Pct": ca["cashProfile"]["byYear"].get("2007"),
            "cash2008Pct": ca["cashProfile"]["byYear"].get("2008"),
            "cash2025Pct": ca["cashProfile"]["byYear"].get("2025"),
            "corr_first24mMarket_vs_5yExcess":
                (ca.get("entryScheduleDefect") or {}).get("corr_first24mMarket_vs_5yExcess"),
            "when_first24m_marketFell":
                (ca.get("entryScheduleDefect") or {}).get("when_first24m_marketWorst25pct"),
            "when_first24m_marketRose":
                (ca.get("entryScheduleDefect") or {}).get("when_first24m_marketBest25pct"),
            "specVsCode": ca["specVsCode"],
            "endEffects": ("전체기간 숫자에는 서로 반대방향의 두 말단 효과가 섞여 있다. "
                           "진입대기 구간(2007~2008, 81% 현금)은 초과수익을 약 +32%p "
                           "과대계상하고, 말단 유휴 구간(2025~2026, 100% 현금 19개월)은 "
                           "약 -37%p 과소계상한다. 거의 상쇄되므로 전체 +4.60%p 자체는 "
                           "왜곡되지 않았지만, 두 구간 모두 규칙의 성능이 아니라 "
                           "스케줄 산물이다."),
            "reading": (ca.get("entryScheduleDefect") or {}).get("reading")},
    }

    worst_bm = bm["worstCaseBenchmark"]
    out = {"gates": g, "passed": npass, "total": len(g), "failed": fails,
           "postHocFindings": post,
           "baseExcessPct": base_ex, "verdict": verdict,
           "verdictRules": VERDICT_RULES,
           "reasoning": {
               "why": None,
               "worstCaseBenchmark": worst_bm,
               "kosdaqPnlSharePct": core["pnlByMarket"].get("KOSDAQ", {}).get("pnlSharePct"),
               "kospiOnlyExcessPct": core["marketSegments"].get("KOSPI", {}).get("excessPct"),
               "top5ContribSharePct": core["concentration"]["top5SharePct"]},
           "precommitRespected": True,
           "parameterChanges": 0,
           "researcherReading": {
               "label": "STRUCTURALLY_FRAGILE",
               "separateFromPrecommittedVerdict": True,
               "why": ("사전확정 규칙은 PROMISING_FORWARD_TEST 를 준다. 규칙은 바꾸지 않았다. "
                       "그러나 근거는 그 라벨보다 약하다. 살아남은 것: 완전투자 16년"
                       "(2009~2024) 구간의 종목선정 초과수익은 현금비중을 통제해도 남고, "
                       "비용·슬리피지·상장폐지 100% 스트레스와 통제군 대조도 통과한다. "
                       "무너진 것: (1) 2008년 이후 시작 코호트의 5년 초과수익 중앙값 "
                       "-2.53%p · positive 32% (2) out-of-sample 창 9/38 positive "
                       "(3) 총손익의 71%가 상위 5종목이고 제거하면 초과수익 음수 "
                       "(4) 5년 초과수익이 '진입 첫 24개월 시장방향'과 상관 -0.64 — 규칙이 "
                       "자본의 81%를 2년간 현금으로 두기 때문 (5) 결측 PBR 종목을 담을 수 "
                       "없다는 점을 benchmark 에 반영하면 초과수익이 +4.60%p → +1.72%p "
                       "(6) KOSPI 단독 -2.52%p · SMALL 구간 내 -3.28%p — 시장·size 노출을 "
                       "제거하면 alpha 가 남지 않는다."),
               "beforeAnyForwardTest": ("frozen 정의의 사양-코드 불일치(설명문은 '12개월 매월 "
                                        "분할 매수', 코드는 1/12만 매수 후 2년 현금)를 먼저 "
                                        "확정해야 한다. 어느 쪽이 규칙인지 정하지 않은 상태로 "
                                        "forward-test 를 시작하면 무엇을 검증하는지 알 수 없다."),
               "notAParameterRescue": ("위 진단은 frozen 파라미터를 하나도 바꾸지 않고 얻었다. "
                                       "대안 파라미터를 탐색하지 않았다(§18). 필요하면 별도 "
                                       "후속 연구 질문으로만 제안한다.")}}
    if verdict == "ROBUST_CANDIDATE":
        out["reasoning"]["why"] = "사전확정 게이트 10개 전부 통과."
    elif verdict == "PROMISING_FORWARD_TEST":
        out["reasoning"]["why"] = (
            f"base excess {base_ex:+.2f}%p 이 살아있고 데이터 결함 0·실행가능 · "
            f"게이트 {npass}/10 통과. 다만 {', '.join(fails)} 미달이므로 robust 라고 "
            f"부를 수 없다. 미래 paper forward-test 를 할 경제적 근거만 인정한다.")
        out["labelWarning"] = (
            "이 라벨은 사전확정 규칙을 글자 그대로 적용한 결과다. 규칙을 결과에 맞춰 "
            "바꾸지 않았다(§17·§18). 그러나 라벨이 근거보다 후하다는 점을 명시한다: "
            "사전 게이트에 'out-of-sample 안정성'과 '시작연도 의존성'을 필수항목으로 "
            "넣지 않은 것이 precommit 의 약점이었다. postHocFindings 를 같이 읽어야 한다. "
            "forward-test 는 '이 규칙이 좋다는 확인'이 아니라 '2012년 이후 부진이 계속되는지 "
            "확인하는 반증(falsification) 테스트'로 설계해야 한다.")
    elif verdict == "FRAGILE":
        out["reasoning"]["why"] = (
            f"base excess 는 {base_ex:+.2f}%p 이지만 게이트 {npass}/10 · 미달 "
            f"{', '.join(fails)}. 스트레스나 통제군에서 우위가 유지되지 않는다.")
    else:
        out["reasoning"]["why"] = (
            f"REJECT 조건 충족 — base excess {base_ex:+.2f}%p / 데이터결함 "
            f"{dq['fatalCount']}건 / leave-one-era-out {loo['positiveCount']}/{loo['total']}.")
    save("verdict", out)
    print(json.dumps({"verdict": verdict, "passed": f"{npass}/{len(g)}", "failed": fails},
                     ensure_ascii=False))
    return 0


# ═══════════ 초과수익 원천 분해 — 종목선정 alpha vs 현금타이밍 ═══════════
def mode_cashaudit(sn, nm, ds, cache, idx):
    """★ R9 최대 발견 지점.

    frozen candidate 는 deployment=STAG12M(12개월 분할 유입) 인데 매수는 24개월에 1회만
    허용된다(buy_every=24). 그래서 1번째 tranche(자본의 1/12)만 2007-01 에 투자되고
    나머지 11/12 는 **2009-01 까지 현금으로 대기**한다. 그 대기 구간이 정확히 글로벌
    금융위기였다. benchmark 는 같은 유입 스케줄을 받는 즉시 매월 지수를 사므로
    폭락장을 정면으로 맞았다.

    → '초과수익'이 종목선정(BM)에서 나온 것인지, 현금비중 타이밍에서 나온 것인지
      분리해야 한다. 방법: **benchmark 에 후보와 같은 월별 현금비중을 강제**한다.
      후보의 현금은 이자 0 이므로 benchmark 현금도 0 으로 두면 완전한 사과-사과 비교다.
      후보의 파라미터는 하나도 바꾸지 않는다(§18).
    """
    res, ev = run_case(cache, ds, idx, **BASE_KW)
    bench = bench_same_schedule(idx, ds, tranches=res["tranches"])
    srets = monthly_twr(res["nav"], res["inflow"])
    brets = monthly_twr(bench["nav"], res["inflow"])
    idx_ret = [idx[i] / idx[i - 1] - 1 for i in range(1, len(idx))]
    w = res["cashRatio"]

    # 현금비중 일치 benchmark: 전기말 현금비중만큼 지수 미보유
    cm = [(1.0 - w[k]) * idx_ret[k] for k in range(len(idx_ret))]
    s, b, c = chain_stats(srets), chain_stats(brets), chain_stats(cm)
    rdates = ds[1:]

    # 구간별 분해 — GFC 대기구간 / 그 이후 / 말단 유휴구간
    #   ★ 구간 귀속은 **기간 시작월**(ds[j]) 기준이다. 현금비중 w[j] 도 기간 시작 시점의
    #     값이므로 종료월(rdates[j]=ds[j+1]) 로 자르면 94% 현금이던 달의 수익률이
    #     '완전투자 구간' 으로 잘못 들어간다(실측: 2008-12→2009-01 한 달이 구간 비교를
    #     10%p 왜곡했다). 시작월 기준으로 맞춘다.
    def seg(lo, hi, label):
        sel = [j for j in range(len(rdates)) if lo <= ds[j][:7] <= hi]
        if not sel:
            return None
        ss = chain_stats([srets[j] for j in sel])
        bb = chain_stats([brets[j] for j in sel])
        cc = chain_stats([cm[j] for j in sel])
        return {"segment": label, "from": lo, "to": hi, "months": len(sel),
                "avgCashPct": pct(sum(w[j] for j in sel) / len(sel)),
                "candTotalPct": pct(ss["growth"] - 1),
                "fairBenchTotalPct": pct(bb["growth"] - 1),
                "cashMatchedBenchTotalPct": pct(cc["growth"] - 1),
                "excessVsFairPct": pct(ss["growth"] - bb["growth"]),
                "excessVsCashMatchedPct": pct(ss["growth"] - cc["growth"])}

    segs = [x for x in (seg("2007-01", "2008-12", "진입대기(90% 현금) — GFC 폭락 구간"),
                        seg("2009-01", "2024-12", "완전투자 구간"),
                        seg("2025-01", "2026-12", "말단 유휴(100% 현금) 구간"),
                        seg("2009-01", "2026-12", "GFC 대기구간 제외 전체")) if x]

    # 기여 배수 분해: 전체 성장배수를 구간 곱으로 나눈다
    gfc = next((x for x in segs if x["segment"].startswith("진입대기")), None)
    after = next((x for x in segs if x["segment"].startswith("GFC 대기구간 제외")), None)

    out = {
        "finding": ("frozen candidate 는 2007년 평균 71.9% · 2008년 평균 90.3% 를 현금으로 "
                    "들고 있었다(STAG12M 유입 + 24개월 1회 매수의 구조적 결과). 그 구간이 "
                    "정확히 글로벌 금융위기였다. benchmark 는 같은 현금을 매월 즉시 "
                    "지수에 넣었으므로 폭락을 전부 맞았다."),
        "specVsCode": ("R8 후보 설명문의 '12개월에 걸쳐 매월 약 4,166,000원씩 분할 매수'는 "
                       "코드 동작과 다르다. 코드는 2007-01 에 1/12(약 417만원)만 매수하고 "
                       "나머지 11/12 는 2009-01 까지 현금으로 둔다. R9 는 frozen 정의를 "
                       "바꾸지 않으므로 **코드 동작**(=R8 이 실제로 측정한 것)을 검증했다."),
        "cashProfile": {"avgCashRatioPct": pct(ev["avgCashRatio"]),
                        "byYear": {y: pct(sum(v) / len(v)) for y, v in
                                   ((yy, [w[i] for i, d in enumerate(ds) if d[:4] == yy])
                                    for yy in sorted({d[:4] for d in ds}))}},
        "fullPeriod": {
            "candCagrPct": pct(s["cagr"]),
            "fairBenchCagrPct": pct(b["cagr"]),
            "cashMatchedBenchCagrPct": pct(c["cagr"]),
            "excessVsFairPct": pct(s["cagr"] - b["cagr"]),
            "excessVsCashMatchedPct": pct(s["cagr"] - c["cagr"]),
            "interpretation": ("excessVsCashMatched 가 종목선정(BM) alpha 에 가까운 값이다. "
                              "excessVsFair 와의 차이는 현금비중 타이밍이 만든 것이다.")},
        "segments": segs,
        "attribution": None,
    }
    if gfc and after:
        gr_c = (1 + gfc["candTotalPct"] / 100) * (1 + after["candTotalPct"] / 100)
        gr_b = (1 + gfc["fairBenchTotalPct"] / 100) * (1 + after["fairBenchTotalPct"] / 100)
        out["attribution"] = {
            "gfcWindowRelativeMultiple": round((1 + gfc["candTotalPct"] / 100) /
                                               (1 + gfc["fairBenchTotalPct"] / 100), 3),
            "restOfPeriodRelativeMultiple": round((1 + after["candTotalPct"] / 100) /
                                                  (1 + after["fairBenchTotalPct"] / 100), 3),
            "totalRelativeMultiple": round(gr_c / gr_b, 3),
            "reading": ("전체 상대배수 중 GFC 대기구간이 만든 몫과 나머지 17년이 만든 몫. "
                        "나머지 17년의 상대배수가 1.0 근처면 그 기간엔 초과수익이 없었다는 뜻.")}
    # ── 진입 스케줄 결함이 시작연도 의존성을 만드는가 ────────────────────────
    #   신규 시작 시 첫 24개월은 자본의 ~80% 가 현금이다. 그러면 결과가 'BM 이 좋은가'
    #   보다 '첫 2년간 시장이 어땠는가'에 좌우된다. 상관관계로 확인한다.
    # ★ 파일명을 하드코딩하지 않는다. save/load 를 교체해 같은 검증을 다른 엔진에
    #   재사용할 때(R10) r9 파일을 잘못 읽는 사고가 난다(실측: R10 cashaudit 이
    #   r9 cohort rows 를 읽어 동일 수치를 출력했다). 반드시 load() 경유.
    ent = None
    try:
        rows = load("walkforward")["cohortDistribution"]["rows"]
    except (OSError, ValueError, KeyError):
        rows = None
    if rows:
        xs, ys = [], []
        for r in rows:
            o = r["offset"]
            if "60" not in r["h"] or o + 24 >= len(idx):
                continue
            xs.append(idx[o + 24] / idx[o] - 1)          # 첫 24개월 시장 수익률
            ys.append(r["h"]["60"]["excessPct"])         # 5년 초과수익
        if len(xs) > 10:
            mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
            sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
            sxx = sum((a - mx) ** 2 for a in xs)
            syy = sum((b - my) ** 2 for b in ys)
            corr = sxy / math.sqrt(sxx * syy) if sxx and syy else None
            lo = [b for a, b in zip(xs, ys) if a < quant(xs, 0.25)]
            hi = [b for a, b in zip(xs, ys) if a > quant(xs, 0.75)]
            ent = {"n": len(xs),
                   "corr_first24mMarket_vs_5yExcess": round(corr, 3) if corr else None,
                   "when_first24m_marketWorst25pct": {"n": len(lo),
                                                      "median5yExcessPct": round(quant(lo, 0.5), 2),
                                                      "positivePct": round(100 * sum(1 for x in lo if x > 0) / len(lo), 1)},
                   "when_first24m_marketBest25pct": {"n": len(hi),
                                                     "median5yExcessPct": round(quant(hi, 0.5), 2),
                                                     "positivePct": round(100 * sum(1 for x in hi if x > 0) / len(hi), 1)},
                   "firstCohortAvgCashPct": pct(sum(w[:24]) / 24),
                   "reading": ("완전투자 16년(2009~2024)에는 현금비중 일치 benchmark 대비 "
                               "초과수익이 그대로 남는다 — 그 구간의 종목선정 초과수익은 "
                               "진짜다. 문제는 **신규 진입자가 그 구간에 도달하는 경로**다. "
                               "규칙대로 시작하면 첫 24개월을 평균 81% 현금으로 보내고, "
                               "그 2년간 시장이 올랐으면 5년 초과수익 중앙값이 -8.11%p "
                               "(positive 10%), 떨어졌으면 +4.32%p (positive 68%) 로 "
                               "갈린다(상관 -0.64). 즉 결과가 종목선정 실력보다 "
                               "'진입 2년간의 시장 방향'이라는 통제 불가 우연에 지배된다. "
                               "이것이 진입 스케줄 결함이다.")}
    out["entryScheduleDefect"] = ent
    save("cashaudit", out)
    print(json.dumps({"excessVsFairPct": out["fullPeriod"]["excessVsFairPct"],
                      "excessVsCashMatchedPct": out["fullPeriod"]["excessVsCashMatchedPct"],
                      "attribution": out["attribution"],
                      "entryScheduleDefect": ent and
                      {k: ent[k] for k in ("corr_first24mMarket_vs_5yExcess",
                                           "when_first24m_marketWorst25pct",
                                           "when_first24m_marketBest25pct")}},
                     ensure_ascii=False))
    return 0


MODES = ("repro", "core", "stress", "walkforward", "controls", "dataquality",
         "benchmarks", "cashaudit", "verdict", "all")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    order = ("repro", "core", "stress", "walkforward", "controls", "dataquality",
             "benchmarks", "cashaudit", "verdict")
    todo = order if a.mode == "all" else (a.mode,)
    need_ctx = [m for m in todo if m != "verdict"]
    c = ctx() if need_ctx else None
    if c:
        print(f"[r9] {c[2][0]} ~ {c[2][-1]} ({len(c[2])}m)", file=sys.stderr)
    for m in todo:
        if m == "verdict":
            rc = mode_verdict()
        else:
            rc = globals()[f"mode_{m}"](*c)
        if rc:
            print(f"[r9] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
