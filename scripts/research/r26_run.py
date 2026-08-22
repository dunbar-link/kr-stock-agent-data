#!/usr/bin/env python3
"""R26 전체 실행 — 재현 · 결합 · incremental · 통제 · audit 산출물 생성.

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r26_analysis as R  # noqa: E402
from r25_analysis import Analyzer as R25Analyzer  # noqa: E402
from r25_analysis import agg as r25_agg  # noqa: E402
from r26_precommit import (BOOTSTRAP, COST, DISTRESS, HORIZONS,  # noqa: E402
                           LIQUIDITY, PRICE_BUCKETS, SECTOR)

RD = Path(__file__).resolve().parents[2] / "reports" / "research"
ARMS = R.ARMS
ALL_H = HORIZONS["all"]
PH = HORIZONS["primary"]
META = {"task": "R26", "analysisVersion": R.VERSION,
        "engine": "CANONICAL_TSR_R24", "primaryHorizon": PH}


def top_names(K, rows, arm):
    """모든 코호트의 TOP 분위 이름과 기여도."""
    contrib, tot = {}, 0.0
    for r in rows:
        c = r[arm]
        i, j = K.pos[c["startDate"]], K.pos[c["startDate"]] + c["months"]
        rs = [(t, K.tsr(i, j, t)) for t in c["topTickers"]]
        rs = [(t, x) for t, x in rs if x is not None]
        if not rs:
            continue
        w = 1.0 / len(rs)
        for t, x in rs:
            contrib[t] = contrib.get(t, 0.0) + w * x
            tot += w * x
    return contrib, tot


def main() -> int:
    K = R.Combo()
    print(f"[r26] months={len(K.dates)} scored={len(K.scores)}", file=sys.stderr)

    # ── 1. R25 정확 재현 + 공유 universe baseline (§8·§37) ────────────
    A25 = R25Analyzer()
    r25h = json.loads((RD / "r25-horizon-results-latest.json")
                      .read_text(encoding="utf-8"))["byFactor"]
    repro = {}
    for f in ("BM", "SIZE_SMALL"):
        repro[f] = {}
        for h in ALL_H:
            cs = A25.cohorts(f, h)
            a = r25_agg(cs)
            st = r25h[f].get(f"{h}M") or {}
            repro[f][f"{h}M"] = {
                "r25SpreadAnnPct": st.get("meanSpreadAnnPct"),
                "r26SpreadAnnPct": a["meanSpreadAnnPct"] if a else None,
                "r25Cohorts": st.get("cohorts"),
                "r26Cohorts": a["cohorts"] if a else None,
                "exactMatch": bool(a and st and
                                   abs(a["meanSpreadAnnPct"]
                                       - st["meanSpreadAnnPct"]) < 1e-9
                                   and a["cohorts"] == st["cohorts"])}
    del A25

    paired = {h: K.paired(h) for h in ALL_H}
    for h in HORIZONS["secondary"]:
        p = K.paired(h)
        if len(p) >= 24:
            paired[h] = p
    shared = {f"{h}M": {a: R.arm_stats(rows, a) for a in ARMS}
              for h, rows in paired.items()}
    R.save("base-reproduction", {
        **META,
        "r25ExactReproduction": repro,
        "allExactMatch": all(x["exactMatch"] for f in repro.values()
                             for x in f.values()),
        "sharedUniverseNote": (
            "아래 baseline 은 BM·SIZE 가 **동시에** 유효한 공유 universe 다. "
            "BM 은 R25 universe 와 동일해 수치가 같고, SIZE 는 PBR 이 없는 종목이 "
            "빠져 R25 와 다르다 — 이것이 공정 비교의 대가다(§8)."),
        "sharedUniverseBaselines": shared})

    # ── 2. 분위 사다리 (§11) ───────────────────────────────────────────
    quant = {}
    for a in ARMS:
        cs = [r[a] for r in paired[PH]]
        q = cs[0]["quantiles"]
        means = []
        for k in range(q):
            vs = [c["quantileCum"][k] for c in cs
                  if c["quantiles"] == q and c["quantileCum"][k] is not None]
            means.append(R.ann(statistics.fmean(vs), PH) if vs else None)
        ok = tot = 0
        for k in range(len(means) - 1):
            if means[k] is None or means[k + 1] is None:
                continue
            tot += 1
            ok += means[k] >= means[k + 1]
        quant[a] = {"quantiles": q, "cohorts": len(cs),
                    "quantileAnnPct": [None if m is None else round(m * 100, 3)
                                       for m in means],
                    "adjacentPairs": tot,
                    "monotonicity": round(ok / tot, 4) if tot else None,
                    "topOnlySpike": bool(
                        len(means) >= 3 and means[0] is not None
                        and means[1] is not None and means[2] is not None
                        and (means[0] - means[1]) > 2.0 * abs(means[1] - means[2]))}
    R.save("quantile-results", {**META, "byArm": quant,
                                "note": "최상위 bucket 하나만 튀면 OVERFIT 경고(§11)"})

    # ── 3. horizon 결과 + incremental (§9·§10) ────────────────────────
    horiz, inc = {}, {}
    for h, rows in paired.items():
        horiz[f"{h}M"] = {a: R.arm_stats(rows, a) for a in ARMS}
        inc[f"{h}M"] = {
            "incremental_vs_BM": R.incr(rows, "COMBO", "BM_ALONE"),
            "incremental_vs_SIZE": R.incr(rows, "COMBO", "SIZE_ALONE"),
            "incremental_vs_UNIVERSE": {
                "meanPct": round(statistics.fmean(
                    [r["COMBO"]["excessAnn"] for r in rows]) * 100, 3),
                "positiveRatio": round(sum(
                    1 for r in rows if r["COMBO"]["excessAnn"] > 0) / len(rows), 4)},
            "spreadIncremental_vs_BM": R.incr(rows, "COMBO", "BM_ALONE",
                                              key="spreadAnn"),
            "spreadIncremental_vs_SIZE": R.incr(rows, "COMBO", "SIZE_ALONE",
                                                key="spreadAnn"),
        }
    R.save("horizon-results", {**META, "horizons": sorted(paired),
                              "annualization": HORIZONS["annualization"],
                              "byHorizon": horiz})
    R.save("incremental-alpha", {
        **META, "metric": "TOP 분위 연율 TSR (짝지음)",
        "secondaryMetric": "TOP−BOTTOM spread (짝지음)",
        "byHorizon": inc,
        "primary": inc[f"{PH}M"]})

    # ── 4. 부분기간 (§12) ──────────────────────────────────────────────
    R.save("subperiod-results", {
        **META,
        "vsBM": {f"{h}M": R.subperiod(rows, "COMBO", "BM_ALONE")
                 for h, rows in paired.items()},
        "vsSIZE": {f"{h}M": R.subperiod(rows, "COMBO", "SIZE_ALONE")
                   for h, rows in paired.items()}})

    # ── 5. 롤링 (§13) ──────────────────────────────────────────────────
    roll = {}
    for h, rows in paired.items():
        no = R.non_overlapping(rows, h)
        roll[f"{h}M"] = {
            "overlapping": {"vsBM": R.incr(rows, "COMBO", "BM_ALONE"),
                            "vsSIZE": R.incr(rows, "COMBO", "SIZE_ALONE")},
            "nonOverlapping": {"cohorts": len(no),
                               "vsBM": R.incr(no, "COMBO", "BM_ALONE"),
                               "vsSIZE": R.incr(no, "COMBO", "SIZE_ALONE")}}
    R.save("rolling-cohorts", {**META, "byHorizon": roll})

    # ── 6. 부트스트랩 (§14) ────────────────────────────────────────────
    boot = {}
    for h, rows in paired.items():
        e = {}
        for nm, b in (("COMBO_minus_BM", "BM_ALONE"),
                      ("COMBO_minus_SIZE", "SIZE_ALONE")):
            xs = R.series(rows, "COMBO", b)
            e[nm] = {
                "movingBlock": R.boot_summary(
                    R.moving_block(xs, BOOTSTRAP["seed"]), xs),
                "yearLevel": R.boot_summary(
                    R.year_level(rows, xs, BOOTSTRAP["seed"]), xs)}
        boot[f"{h}M"] = e
    R.save("bootstrap", {**META, "method": BOOTSTRAP, "byHorizon": boot})

    # ── 7. 독립성 + 조건부 귀속 (§15·§17·§18) ─────────────────────────
    cb, cs_, ov = [], [], []
    for d in sorted(K.scores):
        s = K.scores[d]
        co = [x["COMBO"] for x in s.values()]
        cb.append(statistics.correlation(co, [x["bmRank"] for x in s.values()]))
        cs_.append(statistics.correlation(co, [x["sizeRank"] for x in s.values()]))
        n = max(1, len(s) // 10)
        srt = lambda k: {t for t, _ in sorted(  # noqa: E731
            s.items(), key=lambda kv: -kv[1][k])[:n]}
        top_c, top_b, top_s = srt("COMBO"), srt("bmRank"), srt("sizeRank")
        ov.append((len(top_c & top_b) / n, len(top_c & top_s) / n))
    indep = {
        "corrComboBmRank": round(statistics.fmean(cb), 4),
        "corrComboSizeRank": round(statistics.fmean(cs_), 4),
        "topOverlapWithBmTop": round(statistics.fmean([a for a, _ in ov]), 4),
        "topOverlapWithSizeTop": round(statistics.fmean([b for _, b in ov]), 4),
        "dominatedByOneFactor": bool(
            max(statistics.fmean(cb), statistics.fmean(cs_)) >= 0.95
            or max(statistics.fmean([a for a, _ in ov]),
                   statistics.fmean([b for _, b in ov])) >= 0.90),
        "rule": "상관 >= 0.95 또는 TOP 겹침 >= 90% 면 한쪽 지배(§15)"}

    cond = {"bmWithinSize": {}, "sizeWithinBm": {}}
    for band in ("SMALL", "MID", "LARGE"):
        rows = K.paired(PH, subset=None)
        sel = []
        for r in rows:
            sub = K.size_band(r["startDate"], band)
            c1 = K.cohort("BM_ALONE", K.pos[r["startDate"]], PH, subset=sub)
            if c1:
                sel.append(c1)
        a = r25_agg(sel)
        cond["bmWithinSize"][band] = {
            "cohorts": len(sel),
            "bmSpreadAnnPct": a["meanSpreadAnnPct"] if a else None,
            "bmTopAnnPct": round(statistics.fmean(
                [c["topAnn"] for c in sel]) * 100, 3) if sel else None}
    for band in ("CHEAP", "MID", "EXPENSIVE"):
        sel = []
        for d in sorted(K.scores):
            i = K.pos[d]
            sub = K.bm_band(d, band)
            c1 = K.cohort("SIZE_ALONE", i, PH, subset=sub)
            if c1:
                sel.append(c1)
        a = r25_agg(sel)
        cond["sizeWithinBm"][band] = {
            "cohorts": len(sel),
            "sizeSpreadAnnPct": a["meanSpreadAnnPct"] if a else None,
            "sizeTopAnnPct": round(statistics.fmean(
                [c["topAnn"] for c in sel]) * 100, 3) if sel else None}
    R.save("conditional-attribution", {
        **META, "independence": indep, "conditional": cond,
        "question17": "작은 주식이면 아무거나 좋은가, 작은 주식 안에서도 싼 게 더 좋은가",
        "question18": "싼 주식이면 아무거나 좋은가, 싼 주식 중에도 작은 게 더 좋은가"})

    # ── 8. 거래소 (§16) ────────────────────────────────────────────────
    exch = {}
    for ex in ("KOSPI", "KOSDAQ"):
        f = K.exchange_subset(ex)
        rows = []
        for i in range(len(K.dates) - PH):
            d = K.dates[i]
            if d not in K.scores:
                continue
            sub = f(d)
            row, ok = {}, True
            for a in ARMS:
                c = K.cohort(a, i, PH, subset=sub)
                if c is None:
                    ok = False
                    break
                row[a] = c
            if ok:
                row["startDate"] = d
                rows.append(row)
        exch[ex] = {"cohorts": len(rows),
                    "arms": {a: R.arm_stats(rows, a) for a in ARMS} if rows else None,
                    "vsBM": R.incr(rows, "COMBO", "BM_ALONE") if rows else None,
                    "vsSIZE": R.incr(rows, "COMBO", "SIZE_ALONE") if rows else None}
    both_bm = all((exch[e]["vsBM"] or {}).get("meanPct", -1) > 0
                  for e in ("KOSPI", "KOSDAQ"))
    exch["kosdaqDependent"] = bool(
        (exch["KOSDAQ"]["vsBM"] or {}).get("meanPct", 0) > 0
        and (exch["KOSPI"]["vsBM"] or {}).get("meanPct", 0) <= 0)
    exch["directionHoldsBothMarkets_vsBM"] = bool(both_bm)

    # ── 9. 유동성 / 저가주 (§19·§20·§21) ───────────────────────────────
    liq = {"status": LIQUIDITY["status"], "why": LIQUIDITY["why"],
           "unavailable": LIQUIDITY["unavailable"], "byArm": {}}
    rows = paired[PH]
    for a in ARMS:
        px, mc, bucket = [], [], {b["name"]: 0 for b in PRICE_BUCKETS}
        n = 0
        for r in rows:
            d = r["startDate"]
            for t in r[a]["topTickers"]:
                s = (K.snaps.get(d) or {}).get(t) or {}
                p, m = s.get("close"), s.get("marketCap")
                if p:
                    px.append(p)
                    for b in PRICE_BUCKETS:
                        if (b["lo"] is None or p >= b["lo"]) and \
                                (b["hi"] is None or p < b["hi"]):
                            bucket[b["name"]] += 1
                            break
                if m:
                    mc.append(m)
                n += 1
        cap = LIQUIDITY["notionalCapitalKrw"]
        names = statistics.fmean([r[a]["topCount"] for r in rows])
        per = cap / names if names else None
        liq["byArm"][a] = {
            "topNamesPerCohort": round(names, 1),
            "medianClose": round(statistics.median(px)) if px else None,
            "medianMarketCapKrw": round(statistics.median(mc)) if mc else None,
            "p10MarketCapKrw": round(R.pctl(sorted(mc), 0.10)) if mc else None,
            "priceBucketPct": {k: round(100.0 * x / n, 2)
                               for k, x in bucket.items()} if n else None,
            "notionalPerNameKrw": round(per) if per else None,
            "notionalPerNameVsMedianCapPct": (
                round(100.0 * per / statistics.median(mc), 5)
                if per and mc else None)}
    R.save("liquidity", {**META, **liq, "sector": SECTOR,
                         "notionalCapitalKrw": LIQUIDITY["notionalCapitalKrw"],
                         "notionalWhy": LIQUIDITY["notionalWhy"],
                         "priceBuckets": PRICE_BUCKETS})

    # ── 10. 부실 / 상폐 (§22·§23) ──────────────────────────────────────
    dist = {}
    for a in ARMS + ["UNIVERSE"]:
        n = dl = lo = eb = pl = ne = 0
        for r in rows:
            d = r["startDate"]
            i, j = K.pos[d], K.pos[d] + PH
            names = (list(K.scores[d]) if a == "UNIVERSE"
                     else r[a]["topTickers"])
            for t in names:
                s = (K.snaps.get(d) or {}).get(t) or {}
                v = (K.fv.get(d) or {}).get(t) or {}
                n += 1
                if K.eng.is_delisted_by(j, t):
                    dl += 1
                if s.get("close") is not None and s["close"] < 1000:
                    lo += 1
                if v.get("BM") is not None and v["BM"] > 5.0:
                    eb += 1
                if v.get("EARNINGS_PERSISTENCE") is not None \
                        and v["EARNINGS_PERSISTENCE"] <= 0.5:
                    pl += 1
                if s.get("EPS") is not None and s["EPS"] <= 0:
                    ne += 1
        f = lambda x: round(100.0 * x / n, 2) if n else None  # noqa: E731
        dist[a] = {"names": n, "delistingRatePct": f(dl),
                   "lowPriceRatePct": f(lo), "extremeBmRatePct": f(eb),
                   "persistentLossRatePct": f(pl), "negativeEpsRatePct": f(ne)}
    R.save("distress-delisting", {
        **META, "byArm": dist, "rules": DISTRESS,
        "delistingSemantics": ("canonical UNKNOWN_RECOVERY — 마지막 관측가 청산. "
                               "상한 추정이다(R16 정책)."),
        "comboWorseThanParents": bool(
            dist["COMBO"]["delistingRatePct"]
            > max(dist["BM_ALONE"]["delistingRatePct"],
                  dist["SIZE_ALONE"]["delistingRatePct"]))})

    # ── 11. 집중도 + 제거 (§24) ────────────────────────────────────────
    conc = {}
    for a in ARMS:
        contrib, tot = top_names(K, rows, a)
        top = sorted(contrib.items(), key=lambda kv: -kv[1])
        share = lambda k: (round(sum(v for _, v in top[:k]) / tot * 100, 2)  # noqa: E731
                           if tot else None)
        conc[a] = {"contributors": len(contrib), "top1Pct": share(1),
                   "top3Pct": share(3), "top5Pct": share(5),
                   "top10Pct": share(10),
                   "topNames": [t for t, _ in top[:10]]}
    removal = {}
    contrib_c, _ = top_names(K, rows, "COMBO")
    order = [t for t, _ in sorted(contrib_c.items(), key=lambda kv: -kv[1])]
    for k in (1, 3, 5, 10):
        ex = set(order[:k])
        rr = K.paired(PH, exclude=ex)
        removal[f"removeTop{k}"] = {
            "removed": sorted(ex), "cohorts": len(rr),
            "vsBM": R.incr(rr, "COMBO", "BM_ALONE"),
            "vsSIZE": R.incr(rr, "COMBO", "SIZE_ALONE")}
    base_bm = inc[f"{PH}M"]["incremental_vs_BM"]["meanPct"]
    base_sz = inc[f"{PH}M"]["incremental_vs_SIZE"]["meanPct"]
    flip3 = ((removal["removeTop3"]["vsBM"]["meanPct"] > 0) != (base_bm > 0)
             or (removal["removeTop3"]["vsSIZE"]["meanPct"] > 0) != (base_sz > 0))
    R.save("concentration", {**META, "byArm": conc, "removal": removal,
                             "baselineVsBmPct": base_bm,
                             "baselineVsSizePct": base_sz,
                             "signFlipsWhenTop3Removed": bool(flip3),
                             "fragileRule": "top3 제거 시 부호 뒤집히면 FRAGILE"})

    # ── 12. TSR 한계 노출 (§26) ────────────────────────────────────────
    lim = {}
    for a in ARMS + ["UNIVERSE"]:
        n = hit = 0
        for r in rows:
            d = r["startDate"]
            i, j = K.pos[d], K.pos[d] + PH
            span = set(K.dates[i + 1:j + 1])
            names = (list(K.scores[d]) if a == "UNIVERSE"
                     else r[a]["topTickers"])
            for t in names:
                n += 1
                if K.unresolved.get(t, set()) & span:
                    hit += 1
        lim[a] = {"names": n,
                  "unresolvedRatePct": round(100.0 * hit / n, 3) if n else None}
    diff = lim["COMBO"]["unresolvedRatePct"] - lim["UNIVERSE"]["unresolvedRatePct"]
    R.save("tsr-limitation", {
        **META,
        "foundation": {"expectedBiasPct": 1.734, "extremeDiscontinuityPct": 1.42},
        "byArm": lim, "comboVsUniverseDiffPp": round(diff, 3),
        "exposed": bool(diff >= 3.0),
        "rule": "COMBO TOP 미해결률이 universe 대비 3%p 이상이면 EXPOSED"})

    # ── 13. 회전율 + 비용 스트레스 (§27) ───────────────────────────────
    cost = {}
    ds = sorted(K.scores)
    for a in ARMS:
        turns = []
        prev = None
        for d in ds:
            s = K.scores[d]
            n = max(1, len(s) // (10 if len(s) >= 200 else 5))
            cur = {t for t, _ in sorted(s.items(),
                                        key=lambda kv: -kv[1][a])[:n]}
            if prev is not None and prev:
                turns.append(1.0 - len(cur & prev) / len(prev))
            prev = cur
        mt = statistics.fmean(turns) if turns else None
        annual = mt * 12 if mt is not None else None
        base = horiz[f"{PH}M"][a]["meanTopAnnPct"]
        drag = annual * COST["high"]["roundTripBps"] / 100.0 if annual else None
        cost[a] = {"monthlyTurnover": round(mt, 4) if mt else None,
                   "annualTurnover": round(annual, 3) if annual else None,
                   "baseTopAnnPct": base,
                   "highCostTopAnnPct": (round(base - drag, 3)
                                         if drag is not None else None),
                   "costDragPct": round(drag, 3) if drag is not None else None}
    if all(cost[a]["highCostTopAnnPct"] is not None for a in ARMS):
        cost["incrementalAfterHighCost"] = {
            "vsBM": round(cost["COMBO"]["highCostTopAnnPct"]
                          - cost["BM_ALONE"]["highCostTopAnnPct"], 3),
            "vsSIZE": round(cost["COMBO"]["highCostTopAnnPct"]
                            - cost["SIZE_ALONE"]["highCostTopAnnPct"], 3)}
    cost["assumptions"] = COST
    R.save("exchange-and-cost", {**META, "exchange": exch, "cost": cost})

    print(json.dumps({
        "primaryHorizon": f"{PH}M",
        "TOP": {a: horiz[f"{PH}M"][a]["meanTopAnnPct"] for a in ARMS},
        "vsBM": inc[f"{PH}M"]["incremental_vs_BM"]["meanPct"],
        "vsSIZE": inc[f"{PH}M"]["incremental_vs_SIZE"]["meanPct"],
        "r25ReproExact": all(x["exactMatch"] for f in repro.values()
                             for x in f.values())}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
