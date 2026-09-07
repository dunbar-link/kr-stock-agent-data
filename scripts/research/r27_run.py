#!/usr/bin/env python3
"""R27 전체 실행 — 재현 · coverage · before/after · audit 산출물 생성.

WABABA-SIZE-TRADABILITY-VALIDATION-R27

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r27_analysis as A  # noqa: E402
from r25_analysis import Analyzer as R25Analyzer  # noqa: E402
from r25_analysis import agg as r25_agg  # noqa: E402
from r26_analysis import Combo, arm_stats as r26_arm_stats  # noqa: E402
from r27_analysis import (ARMS, ORDER, PH, R27, arm_stats, diff_series,  # noqa: E402
                          participation_bucket, price_bucket, summarize_diff)
from r27_collect import LOOKBACK  # noqa: E402
from r27_precommit import (CAPITAL, COST, COVERAGE, HORIZONS,  # noqa: E402
                           MARKETCAP_STATS, PARTICIPATION, PRICE_BUCKETS,
                           PRIMARY_GATE, SENSITIVITY, SUSPENSION)
from r26_analysis import boot_summary, moving_block, non_overlapping  # noqa: E402
from r25_analysis import pctl  # noqa: E402
from r25_precommit import SUBPERIODS  # noqa: E402

RD = Path(__file__).resolve().parents[2] / "reports" / "research"
ALL_H = HORIZONS["all"]
META = {"task": "R27", "analysisVersion": A.VERSION,
        "engine": "CANONICAL_TSR_R24", "primaryHorizon": PH,
        "primaryGate": PRIMARY_GATE["name"],
        "thresholdKrw": PRIMARY_GATE["thresholdKrw"]}


def main() -> int:
    K = R27()
    print(f"[r27] months={len(K.dates)} scored={len(K.base)}", file=sys.stderr)

    # ── 1. §6 SIZE / §7 BM exact reproduction ─────────────────────────
    A25 = R25Analyzer()
    r25h = json.loads((RD / "r25-horizon-results-latest.json")
                      .read_text(encoding="utf-8"))["byFactor"]
    repro25 = {}
    for f in ("SIZE_SMALL", "BM"):
        repro25[f] = {}
        for h in ALL_H + HORIZONS["secondary"]:
            cs = A25.cohorts(f, h)
            a = r25_agg(cs)
            st = r25h[f].get(f"{h}M") or {}
            repro25[f][f"{h}M"] = {
                "r25SpreadAnnPct": st.get("meanSpreadAnnPct"),
                "r27SpreadAnnPct": a["meanSpreadAnnPct"] if a else None,
                "exactMatch": bool(a and st and abs(
                    a["meanSpreadAnnPct"] - st["meanSpreadAnnPct"]) < 1e-9)}
    del A25

    KC = Combo()
    r26h = json.loads((RD / "r26-horizon-results-latest.json")
                      .read_text(encoding="utf-8"))["byHorizon"]
    repro26 = {}
    for h in ALL_H:
        rows = KC.paired(h)
        for arm, key in (("SIZE_ALONE", "SIZE_ALONE"), ("BM_ALONE", "BM_ALONE")):
            s = r26_arm_stats(rows, arm)
            st = (r26h.get(f"{h}M") or {}).get(key) or {}
            repro26.setdefault(key, {})[f"{h}M"] = {
                "r26TopAnnPct": st.get("meanTopAnnPct"),
                "r27TopAnnPct": s["meanTopAnnPct"] if s else None,
                "exactMatch": bool(s and st and abs(
                    s["meanTopAnnPct"] - st["meanTopAnnPct"]) < 1e-9)}
    del KC

    all_exact = (all(x["exactMatch"] for f in repro25.values() for x in f.values())
                 and all(x["exactMatch"] for f in repro26.values()
                         for x in f.values()))
    A.save("size-reproduction", {
        **META, "r25": repro25["SIZE_SMALL"], "r26": repro26["SIZE_ALONE"],
        "r26Anchor36mTop": 19.15, "allExactMatch": all_exact})
    A.save("bm-reference", {
        **META, "r25": repro25["BM"], "r26": repro26["BM_ALONE"],
        "r26Anchor36mTop": 15.01,
        "why": "BM 연구를 다시 하는 것이 아니라 동일 gate 적용을 위한 reference(§7)"})
    if not all_exact:
        print("[r27] REPRODUCTION MISMATCH — 중단", file=sys.stderr)
        A.save("verdict", {"task": "R27", "verdict": "BLOCKED_REPRODUCTION_MISMATCH",
                           "why": "R25/R26 재현 실패. 원인 해결 전 진행 금지(§6)."})
        return 2

    # ── 2. §30 coverage ───────────────────────────────────────────────
    by_year, by_ex, by_size = {}, {}, {}
    for d in K.dates:
        b = K.base.get(d)
        if not b:
            continue
        uni = list(b["BM"])
        met = K.tr.metrics.get(d) or {}
        y = d[:4]
        yy = by_year.setdefault(y, {"n": 0, "have": 0})
        yy["n"] += len(uni)
        yy["have"] += sum(1 for t in uni if t in met)
        for t in uni:
            ex = (K.snaps.get(d) or {}).get(t, {}).get("market") or "UNKNOWN"
            e = by_ex.setdefault(ex, {"n": 0, "have": 0})
            e["n"] += 1
            e["have"] += (t in met)
        caps = sorted((K.snaps[d][t]["marketCap"], t) for t in uni
                      if (K.snaps.get(d) or {}).get(t, {}).get("marketCap"))
        n3 = len(caps) // 3
        for band, seg in (("SMALL", caps[:n3]), ("MID", caps[n3:2 * n3]),
                          ("LARGE", caps[2 * n3:])):
            s = by_size.setdefault(band, {"n": 0, "have": 0})
            s["n"] += len(seg)
            s["have"] += sum(1 for _, t in seg if t in met)

    def pct(d):
        return {k: {"names": v["n"], "withLiquidity": v["have"],
                    "coveragePct": round(100.0 * v["have"] / v["n"], 2)
                    if v["n"] else 0.0} for k, v in sorted(d.items())}
    ycov = pct(by_year)
    years_ok = [y for y, v in ycov.items()
                if v["coveragePct"] >= COVERAGE["minCoveragePctPerYear"]]
    cov_pass = len(years_ok) >= COVERAGE["minYearsCovered"]
    inv = json.loads((RD / "r27-data-inventory-latest.json")
                     .read_text(encoding="utf-8"))
    A.save("liquidity-coverage", {
        **META, "lookbackTradingDays": LOOKBACK,
        "daysRequired": inv["daysRequired"],
        "daysCollected": inv["daysCollectedTotal"],
        "dayCoveragePct": inv["coveragePct"],
        "byYear": ycov, "byExchange": pct(by_ex), "bySizeBucket": pct(by_size),
        "minCoveragePctPerYear": COVERAGE["minCoveragePctPerYear"],
        "yearsMeetingThreshold": len(years_ok),
        "minYearsCovered": COVERAGE["minYearsCovered"],
        "coverageGatePass": cov_pass,
        "rule": COVERAGE["rule"], "noSecretWindowShift": COVERAGE["noSecretWindowShift"],
        "suspension": SUSPENSION})

    # ★ §30 coverage gate — 미달이면 하류 수치를 만들지 않는다.
    #   근거가 없는 before/after·참여율·판정 숫자를 생산하면 그 자체가 거짓이 된다.
    #   coverage 가 좋은 최근 기간만 몰래 primary 로 바꾸지도 않는다.
    if not cov_pass:
        print(f"[r27] COVERAGE GATE FAIL — 하류 분석 중단 "
              f"(day coverage {inv['coveragePct']}%, "
              f"years ok {len(years_ok)}/{COVERAGE['minYearsCovered']})",
              file=sys.stderr)
        print(json.dumps({"reproExact": all_exact, "coverageGatePass": False,
                          "dayCoveragePct": inv["coveragePct"],
                          "yearsMeetingThreshold": len(years_ok),
                          "downstreamAnalysis": "NOT_RUN"},
                         ensure_ascii=False))
        return 0

    # ── 3. §20 before / after ─────────────────────────────────────────
    before = {h: K.paired(h, tradable=False) for h in ALL_H}
    after = {h: K.paired(h, tradable=True) for h in ALL_H}
    ba = {}
    for h in ALL_H:
        ba[f"{h}M"] = {
            "BEFORE": {a: arm_stats(before[h], a) for a in ARMS},
            "AFTER": {a: arm_stats(after[h], a) for a in ARMS},
        }
        for a in ("SIZE", "BM"):
            b_, a_ = ba[f"{h}M"]["BEFORE"][a], ba[f"{h}M"]["AFTER"][a]
            if b_ and a_:
                ba[f"{h}M"].setdefault("delta", {})[a] = {
                    "topAnnPct": round(a_["meanTopAnnPct"] - b_["meanTopAnnPct"], 3),
                    "spreadAnnPct": round(a_["meanSpreadAnnPct"]
                                          - b_["meanSpreadAnnPct"], 3),
                    "excessAnnPct": round(a_["meanExcessAnnPct"]
                                          - b_["meanExcessAnnPct"], 3),
                    "retentionRatio": (round(a_["meanExcessAnnPct"]
                                             / b_["meanExcessAnnPct"], 4)
                                       if b_["meanExcessAnnPct"] else None),
                    "eligibleBefore": b_["meanEligible"],
                    "eligibleAfter": a_["meanEligible"]}
    A.save("before-after", {**META, "byHorizon": ba,
                            "fairness": "BM·SIZE·CONTROL 모두 같은 tradable universe(§18)"})

    # ── 4. §15·§16 참여율 · §12 저가주 · §13 시총 ─────────────────────
    part, pricex, capstat = {}, {}, {}
    rows = before[PH]
    for a in ("SIZE", "BM"):
        pb = {n: 0 for n in PARTICIPATION["bucketNames"]}
        prb = {b["name"]: 0 for b in PRICE_BUCKETS}
        caps, tvs, n = [], [], 0
        for r in rows:
            d = r["startDate"]
            met = K.tr.metrics.get(d) or {}
            for t in r[a]["topTickers"]:
                n += 1
                s = (K.snaps.get(d) or {}).get(t) or {}
                if s.get("close"):
                    prb[price_bucket(s["close"])] += 1
                if s.get("marketCap"):
                    caps.append(s["marketCap"])
                m = met.get(t)
                if m and m["median20TradedValue"] > 0:
                    tvs.append(m["median20TradedValue"])
                    pb[participation_bucket(ORDER / m["median20TradedValue"])] += 1
        part[a] = {"names": n, "withLiquidity": len(tvs),
                   "medianTradedValueKrw": round(statistics.median(tvs))
                   if tvs else None,
                   "p10TradedValueKrw": round(pctl(sorted(tvs), 0.10))
                   if tvs else None,
                   "orderPerNameKrw": ORDER,
                   "medianParticipation": round(
                       ORDER / statistics.median(tvs), 6) if tvs else None,
                   "bucketPct": {k: round(100.0 * v / len(tvs), 2)
                                 for k, v in pb.items()} if tvs else None}
        pricex[a] = {k: round(100.0 * v / n, 2) for k, v in prb.items()} if n else None
        cs = sorted(caps)
        capstat[a] = {"min": cs[0] if cs else None,
                      "p10": pctl(cs, 0.10), "p25": pctl(cs, 0.25),
                      "median": statistics.median(cs) if cs else None,
                      "p75": pctl(cs, 0.75), "p90": pctl(cs, 0.90)}
    # control(전체 eligible) 참고
    ctl_caps, ctl_tv = [], []
    for r in rows:
        d = r["startDate"]
        met = K.tr.metrics.get(d) or {}
        for t in K.base[d]["BM"]:
            s = (K.snaps.get(d) or {}).get(t) or {}
            if s.get("marketCap"):
                ctl_caps.append(s["marketCap"])
            m = met.get(t)
            if m and m["median20TradedValue"] > 0:
                ctl_tv.append(m["median20TradedValue"])
    capstat["CONTROL"] = {"min": min(ctl_caps) if ctl_caps else None,
                          "p10": pctl(sorted(ctl_caps), 0.10),
                          "median": statistics.median(ctl_caps) if ctl_caps else None,
                          "p90": pctl(sorted(ctl_caps), 0.90)}
    part["CONTROL"] = {"medianTradedValueKrw": round(statistics.median(ctl_tv))
                       if ctl_tv else None}
    A.save("participation", {**META, "capital": CAPITAL,
                             "buckets": PARTICIPATION, "byArm": part,
                             "marketCap": capstat,
                             "marketCapStats": MARKETCAP_STATS})
    A.save("low-price", {**META, "buckets": PRICE_BUCKETS, "byArm": pricex,
                         "notAFilter": "저가주는 gate 의 제외조건이 아니다(§12·§21)"})

    # ── 5. §21 attrition · §22 included / excluded ────────────────────
    attr = {a: {r: 0 for r in A.REASONS} for a in ("SIZE", "BM")}
    attr_tot = {"SIZE": 0, "BM": 0}
    inc_exc = {}
    for a in ("SIZE", "BM"):
        inc, exc = [], []
        for r in rows:
            d = r["startDate"]
            i, j = K.pos[d], K.pos[d] + PH
            for t in r[a]["topTickers"]:
                ok, why = K.tr.evaluate(d, t)
                attr_tot[a] += 1
                if not ok:
                    attr[a][why] += 1
                v = K.tsr(i, j, t)
                if v is None:
                    continue
                (inc if ok else exc).append(v)
        h = PH
        inc_ann = A.ann(statistics.fmean(inc), h) if inc else None
        exc_ann = A.ann(statistics.fmean(exc), h) if exc else None
        inc_exc[a] = {
            "includedNames": len(inc), "excludedNames": len(exc),
            "includedTopAnnPct": round(inc_ann * 100, 3) if inc_ann else None,
            "excludedTopAnnPct": round(exc_ann * 100, 3) if exc_ann else None,
            "excludedShare": round(len(exc) / (len(inc) + len(exc)), 4)
            if (inc or exc) else None}
        if inc_ann is not None and exc_ann is not None:
            inc_exc[a]["nontradableAlpha"] = bool(
                exc_ann >= inc_ann * 2 or inc_ann <= 0)
    A.save("included-excluded", {**META, "byArm": inc_exc,
                                 "definition": "같은 TOP 분위를 gate 로 둘로 나눔(§22)",
                                 "flagRule": "excluded >= included×2 또는 included<=0 이면 SIZE_NONTRADABLE_ALPHA"})
    A.save("tradability-attrition", {
        **META,
        "byArm": {a: {"topNames": attr_tot[a],
                      "excluded": sum(attr[a].values()),
                      "attritionPct": round(100.0 * sum(attr[a].values())
                                            / attr_tot[a], 2) if attr_tot[a] else None,
                      "reasons": attr[a],
                      "reasonPct": {k: round(100.0 * v / attr_tot[a], 2)
                                    for k, v in attr[a].items()} if attr_tot[a] else None}
                  for a in ("SIZE", "BM")},
        "lowPriceNotAnExclusion": "LOW_PRICE 는 gate 조건이 아니므로 탈락사유에 없다(§21)"})

    # ── 6. §23 상폐 · §24 부실 ────────────────────────────────────────
    dd = {}
    for a in ("SIZE", "BM"):
        for grp in ("TRADABLE", "NONTRADABLE"):
            n = dl = lo = eb = pl = ne = 0
            for r in rows:
                d = r["startDate"]
                j = K.pos[d] + PH
                for t in r[a]["topTickers"]:
                    ok, _ = K.tr.evaluate(d, t)
                    if (grp == "TRADABLE") != ok:
                        continue
                    s = (K.snaps.get(d) or {}).get(t) or {}
                    v = (K.fv.get(d) or {}).get(t) or {}
                    n += 1
                    dl += K.eng.is_delisted_by(j, t)
                    lo += bool(s.get("close") is not None and s["close"] < 1000)
                    eb += bool(v.get("BM") is not None and v["BM"] > 5.0)
                    pl += bool(v.get("EARNINGS_PERSISTENCE") is not None
                               and v["EARNINGS_PERSISTENCE"] <= 0.5)
                    ne += bool(s.get("EPS") is not None and s["EPS"] <= 0)
            f = (lambda x: round(100.0 * x / n, 2)) if n else (lambda x: None)
            dd[f"{a}_{grp}"] = {"names": n, "delistingRatePct": f(dl),
                                "lowPriceRatePct": f(lo),
                                "extremeBmRatePct": f(eb),
                                "persistentLossRatePct": f(pl),
                                "negativeEpsRatePct": f(ne)}
    A.save("delisting-distress", {
        **META, "byGroup": dd,
        "delistingSemantics": "canonical UNKNOWN_RECOVERY (상한 추정)",
        "noNewFilter": "새 quality filter 를 추가하지 않았다(§24)"})

    # ── 7. §27 subperiod / rolling · §28 거래소 ───────────────────────
    def subper(rws, a):
        out = {}
        for sp in SUBPERIODS:
            sel = [r for r in rws if sp["start"] <= r["startDate"] <= sp["end"]]
            xs = diff_series(sel, a, "CONTROL") if sel else []
            v = summarize_diff(xs)
            out[sp["name"]] = {"cohorts": len(sel),
                               "meanPct": v["meanPct"] if v else None}
        pos = sum(1 for x in out.values()
                  if x["meanPct"] is not None and x["meanPct"] > 0)
        tot = sum(1 for x in out.values() if x["meanPct"] is not None)
        return {"byPeriod": out, "positive": pos, "total": tot,
                "positiveRatio": round(pos / tot, 4) if tot else None}

    roll = {}
    for a in ("SIZE", "BM"):
        roll[a] = {}
        for h in ALL_H:
            ov = diff_series(after[h], a, "CONTROL")
            no = diff_series(non_overlapping(after[h], h, ARMS), a, "CONTROL")
            roll[a][f"{h}M"] = {"overlapping": summarize_diff(ov),
                                "nonOverlapping": summarize_diff(no)}
    A.save("subperiod-rolling", {
        **META, "subperiods": SUBPERIODS,
        "subperiodAfter": {a: subper(after[PH], a) for a in ("SIZE", "BM")},
        "subperiodBefore": {a: subper(before[PH], a) for a in ("SIZE", "BM")},
        "rollingAfter": roll,
        "metric": "TOP − CONTROL (같은 tradable universe)"})

    exch = {}
    for ex in ("KOSPI", "KOSDAQ"):
        rws = []
        for i in range(len(K.dates) - PH):
            d = K.dates[i]
            if d not in K.base:
                continue
            sub = K.tr.tradable_set(d, set(K.base[d]["BM"])) & K.exchange_subset(d, ex)
            row, ok = {}, True
            for a in ARMS:
                c = K.cohort(a, i, PH, subset=sub)
                if c is None:
                    ok = False
                    break
                row[a] = c
            if ok:
                row["startDate"] = d
                rws.append(row)
        exch[ex] = {"cohorts": len(rws),
                    "arms": {a: arm_stats(rws, a) for a in ARMS} if rws else None,
                    "sizeExcessVsControl": summarize_diff(
                        diff_series(rws, "SIZE", "CONTROL")) if rws else None,
                    "bmExcessVsControl": summarize_diff(
                        diff_series(rws, "BM", "CONTROL")) if rws else None}
    A.save("exchange", {**META, "byExchange": exch})

    # ── 8. §29 집중도 ─────────────────────────────────────────────────
    conc = {}
    for a in ("SIZE", "BM"):
        contrib, tot = {}, 0.0
        for r in after[PH]:
            d = r["startDate"]
            i, j = K.pos[d], K.pos[d] + PH
            rs = [(t, K.tsr(i, j, t)) for t in r[a]["topTickers"]]
            rs = [(t, x) for t, x in rs if x is not None]
            if not rs:
                continue
            w = 1.0 / len(rs)
            for t, x in rs:
                contrib[t] = contrib.get(t, 0.0) + w * x
                tot += w * x
        top = sorted(contrib.items(), key=lambda kv: -kv[1])
        sh = (lambda k: round(sum(v for _, v in top[:k]) / tot * 100, 2)) \
            if tot else (lambda k: None)
        conc[a] = {"contributors": len(contrib), "top1Pct": sh(1),
                   "top3Pct": sh(3), "top5Pct": sh(5), "top10Pct": sh(10),
                   "topNames": [t for t, _ in top[:10]]}
    ex3 = {t for t, _ in sorted(
        {t: v for t, v in ((t, v) for t, v in [])}.items())}  # placeholder
    removal = {}
    for a in ("SIZE", "BM"):
        names = conc[a]["topNames"][:3]
        rr = K.paired(PH, tradable=True, exclude=set(names))
        removal[a] = {"removed": names,
                      "excessVsControl": summarize_diff(
                          diff_series(rr, a, "CONTROL"))}
    base_ex = {a: summarize_diff(diff_series(after[PH], a, "CONTROL"))
               for a in ("SIZE", "BM")}
    flip = {a: bool(removal[a]["excessVsControl"]
                    and base_ex[a]
                    and (removal[a]["excessVsControl"]["meanPct"] > 0)
                    != (base_ex[a]["meanPct"] > 0)) for a in ("SIZE", "BM")}
    A.save("concentration", {**META, "byArm": conc, "baseline": base_ex,
                             "removeTop3": removal, "signFlips": flip})

    # ── 9. §26 민감도 + §25 비용 ──────────────────────────────────────
    sens = {}
    for name, cfg in SENSITIVITY.items():
        if name == "rule":
            continue
        rws = K.paired(PH, tradable=True, threshold=cfg["thresholdKrw"])
        sens[name] = {"thresholdKrw": cfg["thresholdKrw"],
                      "participation": cfg["participation"],
                      "cohorts": len(rws),
                      "arms": {a: arm_stats(rws, a) for a in ARMS} if rws else None,
                      "sizeExcessVsControlPct": (summarize_diff(
                          diff_series(rws, "SIZE", "CONTROL")) or {}).get("meanPct")
                      if rws else None,
                      "bmExcessVsControlPct": (summarize_diff(
                          diff_series(rws, "BM", "CONTROL")) or {}).get("meanPct")
                      if rws else None}
    sens["rule"] = SENSITIVITY["rule"]

    cost = {}
    ds_sorted = sorted(K.base)
    for a in ("SIZE", "BM"):
        turns, prev = [], None
        for d in ds_sorted:
            sub = K.tr.tradable_set(d, set(K.base[d]["BM"]))
            vals = {t: K.base[d][a][t] for t in sub}
            if len(vals) < 100:
                prev = None
                continue
            n = max(1, len(vals) // (10 if len(vals) >= 200 else 5))
            cur = {t for t, _ in sorted(vals.items(), key=lambda kv: -kv[1])[:n]}
            if prev:
                turns.append(1.0 - len(cur & prev) / len(prev))
            prev = cur
        mt = statistics.fmean(turns) if turns else None
        annual = mt * 12 if mt is not None else None
        base = (ba[f"{PH}M"]["AFTER"][a] or {}).get("meanTopAnnPct")
        drag = annual * COST["high"]["roundTripBps"] / 100.0 if annual else None
        cost[a] = {"monthlyTurnover": round(mt, 4) if mt else None,
                   "annualTurnover": round(annual, 3) if annual else None,
                   "baseTopAnnPct": base,
                   "highCostTopAnnPct": round(base - drag, 3)
                   if (base is not None and drag is not None) else None}
    cost["assumptions"] = COST
    A.save("sensitivity-cost", {**META, "sensitivity": sens, "cost": cost})

    s36 = ba[f"{PH}M"]
    print(json.dumps({
        "reproExact": all_exact,
        "coverageGatePass": cov_pass,
        "dayCoveragePct": inv["coveragePct"],
        "BEFORE": {a: (s36["BEFORE"][a] or {}).get("meanTopAnnPct") for a in ARMS},
        "AFTER": {a: (s36["AFTER"][a] or {}).get("meanTopAnnPct") for a in ARMS},
        "sizeAttritionPct": round(100.0 * sum(attr["SIZE"].values())
                                  / attr_tot["SIZE"], 2) if attr_tot["SIZE"] else None,
        "includedExcluded": {a: {"inc": inc_exc[a]["includedTopAnnPct"],
                                 "exc": inc_exc[a]["excludedTopAnnPct"]}
                             for a in ("SIZE", "BM")}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
