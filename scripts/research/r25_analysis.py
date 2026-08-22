#!/usr/bin/env python3
"""R25 factor 분석 — 분위·horizon·통제·부분기간·롤링·부트스트랩·집중도·부실.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

precommit 에 고정한 규칙만 적용한다(§2). 결과를 보고 규칙을 바꾸지 않는다.
모든 수익률은 R24 승인 canonical TSR 엔진 하나로만 계산한다(§3).

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import math
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402
from r25_engine import R25Engine, load_snapshots  # noqa: E402
from r25_factors import compute  # noqa: E402
from r25_precommit import (CONCENTRATION, DISTRESS, ELIGIBILITY,  # noqa: E402
                           FACTORS, HORIZONS, QUANTILES, STATS, SUBPERIODS,
                           TSR_LIMITATION)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
ANALYSIS_VERSION = "r25-analysis-1"
ALL_H = HORIZONS["primary"] + HORIZONS["secondary"]


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r25-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r25] saved {p.name}", file=sys.stderr)


def ann(cum, months):
    """누적 → 연율. 사전 고정 공식(§8)."""
    if cum is None or months <= 0:
        return None
    if cum <= -1.0:
        return -1.0
    return (1.0 + cum) ** (12.0 / months) - 1.0


def buckets(ranked, n):
    """정렬된 리스트를 n 등분. 남는 원소는 앞 구간부터 하나씩."""
    m, r = divmod(len(ranked), n)
    out, s = [], 0
    for k in range(n):
        e = s + m + (1 if k < r else 0)
        out.append(ranked[s:e])
        s = e
    return out


class Analyzer:
    def __init__(self):
        cap = C.load_capital_series()
        self.dates = contiguous_span(sorted(cap))
        self.eng = R25Engine(self.dates, cap=cap)
        self.snaps = load_snapshots(start=self.dates[0], end=self.dates[-1])
        self.fv, _ = compute(self.snaps, self.dates)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self._tsr = {}
        self.unresolved = self._load_unresolved()

    def _load_unresolved(self):
        p = RD / "r24-full-reconciliation-latest.json"
        rows = json.loads(p.read_text(encoding="utf-8"))["rows"]
        out = {}
        for m in rows:
            if not m.get("resolvedWealth"):
                out.setdefault(m["ticker"], set()).add(m["date"])
        return out

    def tsr(self, i, j, t):
        k = (i, j, t)
        v = self._tsr.get(k, 0)
        if v != 0:
            return v
        v = self.eng.tsr(i, j, t)
        self._tsr[k] = v
        return v

    # ── 코호트 1개: 한 factor · 한 시작월 · 한 horizon ────────────────
    def cohort(self, factor, i, h, subset=None):
        j = i + h
        if j >= len(self.dates):
            return None
        d = self.dates[i]
        vals = self.fv.get(d) or {}
        elig = {t: v[factor] for t, v in vals.items() if factor in v}
        if subset is not None:
            elig = {t: x for t, x in elig.items() if t in subset}
        n = len(elig)
        if n < ELIGIBILITY["minUniversePerMonth"]:
            return None
        q = 10 if n >= 200 else 5
        direction = FACTORS[factor]["direction"]
        ranked = sorted(elig.items(), key=lambda kv: (kv[1], kv[0]),
                        reverse=(direction > 0))
        # ranked[0] 이 가설상 '가장 좋은' 종목
        bs = buckets([t for t, _ in ranked], q)
        qret, qcount = [], []
        for b in bs:
            rs = [self.tsr(i, j, t) for t in b]
            rs = [x for x in rs if x is not None]
            qcount.append(len(rs))
            qret.append(statistics.fmean(rs) if rs else None)
        allr = [self.tsr(i, j, t) for t in elig]
        allr = [x for x in allr if x is not None]
        if not allr or qret[0] is None or qret[-1] is None:
            return None
        return {"startDate": d, "endDate": self.dates[j], "months": h,
                "quantiles": q, "eligible": n,
                "quantileCum": qret, "quantileCount": qcount,
                "topCum": qret[0], "bottomCum": qret[-1],
                "universeCum": statistics.fmean(allr),
                "topAnn": ann(qret[0], h), "bottomAnn": ann(qret[-1], h),
                "universeAnn": ann(statistics.fmean(allr), h),
                "spreadAnn": ann(qret[0], h) - ann(qret[-1], h),
                "excessAnn": ann(qret[0], h) - ann(statistics.fmean(allr), h),
                "topTickers": [t for t, _ in ranked][:len(bs[0])]}

    def cohorts(self, factor, h, subset_fn=None):
        out = []
        for i in range(len(self.dates) - h):
            sub = subset_fn(self.dates[i]) if subset_fn else None
            c = self.cohort(factor, i, h, subset=sub)
            if c:
                out.append(c)
        return out

    # ── 그룹 subset 생성기 ────────────────────────────────────────────
    def exchange_subset(self, ex):
        def f(d):
            return {t for t, r in (self.snaps.get(d) or {}).items()
                    if r.get("market") == ex}
        return f

    def size_subset(self, band):
        def f(d):
            rows = self.snaps.get(d) or {}
            caps = sorted(((r["marketCap"], t) for t, r in rows.items()
                           if r.get("marketCap")), key=lambda x: x[0])
            n = len(caps)
            if n < 30:
                return set()
            a, b = n // 3, 2 * n // 3
            seg = {"SMALL": caps[:a], "MID": caps[a:b], "LARGE": caps[b:]}[band]
            return {t for _, t in seg}
        return f

    def size_exchange_subset(self, band, ex):
        sf, ef = self.size_subset(band), self.exchange_subset(ex)
        return lambda d: sf(d) & ef(d)


# ══════════════ 집계 ══════════════
def agg(cs):
    if not cs:
        return None
    sp = [c["spreadAnn"] for c in cs]
    return {"cohorts": len(cs),
            "meanSpreadAnnPct": round(statistics.fmean(sp) * 100, 3),
            "medianSpreadAnnPct": round(statistics.median(sp) * 100, 3),
            "positiveRate": round(sum(1 for x in sp if x > 0) / len(sp), 4),
            "meanTopAnnPct": round(
                statistics.fmean([c["topAnn"] for c in cs]) * 100, 3),
            "meanBottomAnnPct": round(
                statistics.fmean([c["bottomAnn"] for c in cs]) * 100, 3),
            "meanUniverseAnnPct": round(
                statistics.fmean([c["universeAnn"] for c in cs]) * 100, 3),
            "meanExcessAnnPct": round(
                statistics.fmean([c["excessAnn"] for c in cs]) * 100, 3)}


def monotonicity(cs, h):
    """분위 순서와 같은 방향으로 움직이는 인접쌍 비율."""
    if not cs:
        return None
    q = cs[0]["quantiles"]
    means = []
    for k in range(q):
        vs = [c["quantileCum"][k] for c in cs
              if c["quantiles"] == q and c["quantileCum"][k] is not None]
        means.append(ann(statistics.fmean(vs), h) if vs else None)
    ok = tot = 0
    for k in range(len(means) - 1):
        if means[k] is None or means[k + 1] is None:
            continue
        tot += 1
        if means[k] >= means[k + 1]:
            ok += 1
    return {"quantiles": q,
            "quantileAnnPct": [None if m is None else round(m * 100, 3)
                               for m in means],
            "adjacentPairs": tot,
            "monotonicity": round(ok / tot, 4) if tot else None}


def non_overlapping(cs, h):
    out, last = [], None
    for c in cs:
        if last is None or c["startDate"] >= last:
            out.append(c)
            last = c["endDate"]
    return out


def pctl(xs, p):
    if not xs:
        return None
    s = sorted(xs)
    k = (len(s) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] if lo == hi else s[lo] + (s[hi] - s[lo]) * (k - lo)


def bootstrap(sp):
    """moving-block bootstrap. 블록·재표본수·시드 전부 사전 고정(§14)."""
    n = len(sp)
    b = STATS["blockMonths"]
    if n < b + 1:
        return None
    rng = random.Random(STATS["seed"])
    nb = math.ceil(n / b)
    means = []
    for _ in range(STATS["resamples"]):
        s = []
        for _ in range(nb):
            st = rng.randrange(0, n - b + 1)
            s.extend(sp[st:st + b])
        means.append(statistics.fmean(s[:n]))
    means.sort()
    lo = means[int(0.025 * (len(means) - 1))]
    hi = means[int(0.975 * (len(means) - 1))]
    return {"method": STATS["method"], "blockMonths": b,
            "resamples": STATS["resamples"], "seed": STATS["seed"],
            "observations": n,
            "meanSpreadAnnPct": round(statistics.fmean(sp) * 100, 3),
            "ci95LowPct": round(lo * 100, 3), "ci95HighPct": round(hi * 100, 3),
            "pSpreadPositive": round(
                sum(1 for m in means if m > 0) / len(means), 4)}


def concentration(A, cs, h):
    """TOP 분위 내 종목별 기여도. top3 제거 후 spread 부호까지 본다(§15)."""
    tot, contrib = 0.0, {}
    kept = []
    for c in cs:
        i = A.pos[c["startDate"]]
        j = i + h
        rs = [(t, A.tsr(i, j, t)) for t in c["topTickers"]]
        rs = [(t, x) for t, x in rs if x is not None]
        if not rs:
            continue
        w = 1.0 / len(rs)
        for t, x in rs:
            contrib[t] = contrib.get(t, 0.0) + w * x
            tot += w * x
        ex = sorted(rs, key=lambda kv: -kv[1])[:3]
        exset = {t for t, _ in ex}
        rest = [x for t, x in rs if t not in exset]
        if rest:
            kept.append(ann(statistics.fmean(rest), h) - c["bottomAnn"])
    if not contrib or tot == 0:
        return None
    top = sorted(contrib.items(), key=lambda kv: -kv[1])
    def share(k):
        return round(sum(v for _, v in top[:k]) / tot * 100, 2)
    ex3 = round(statistics.fmean(kept) * 100, 3) if kept else None
    base = round(statistics.fmean([c["spreadAnn"] for c in cs]) * 100, 3)
    flag = (share(3) >= 50.0 and ex3 is not None
            and (ex3 <= 0) != (base <= 0))
    return {"topContributorsCount": len(contrib),
            "top1Pct": share(1), "top3Pct": share(3),
            "top5Pct": share(5), "top10Pct": share(10),
            "spreadAnnPct": base, "spreadExcludingTop3AnnPct": ex3,
            "signFlipsWhenTop3Removed": (ex3 is not None
                                         and (ex3 <= 0) != (base <= 0)),
            "concentratedSignal": bool(flag),
            "rule": CONCENTRATION["flagRule"],
            "topNames": [t for t, _ in top[:10]]}


def distress(A, cs, h, factor):
    """TOP/BOTTOM 의 상폐·적자·저가·극단 밸류에이션 노출(§16)."""
    def band(getter):
        n = dl = lo = pb = pe = pl = 0
        for c in cs:
            i = A.pos[c["startDate"]]
            j = i + h
            d = c["startDate"]
            for t in getter(c):
                r = (A.snaps.get(d) or {}).get(t) or {}
                n += 1
                if A.eng.is_delisted_by(j, t):
                    dl += 1
                if r.get("close") is not None and r["close"] < 1000:
                    lo += 1
                p = r.get("PBR")
                if p is not None and (p < 0.2 or p > 10):
                    pb += 1
                q = r.get("PER")
                if q is not None and (q < 0 or q > 100):
                    pe += 1
                v = (A.fv.get(d) or {}).get(t) or {}
                if v.get("EARNINGS_PERSISTENCE") is not None \
                        and v["EARNINGS_PERSISTENCE"] <= 0.5:
                    pl += 1
        if not n:
            return None
        f = lambda x: round(100.0 * x / n, 2)  # noqa: E731
        return {"names": n, "delistingRatePct": f(dl),
                "lowPriceRatePct": f(lo), "extremePbrRatePct": f(pb),
                "extremePerRatePct": f(pe), "persistentLossRatePct": f(pl)}

    def bottom_of(c):
        i = A.pos[c["startDate"]]
        vals = A.fv.get(c["startDate"]) or {}
        elig = {t: v[factor] for t, v in vals.items() if factor in v}
        ranked = sorted(elig.items(), key=lambda kv: (kv[1], kv[0]),
                        reverse=(FACTORS[factor]["direction"] > 0))
        bs = buckets([t for t, _ in ranked], c["quantiles"])
        return bs[-1]

    return {"TOP": band(lambda c: c["topTickers"]),
            "BOTTOM": band(bottom_of),
            "lowPriceRule": DISTRESS["lowPrice"],
            "extremePbrRule": DISTRESS["extremePbr"],
            "extremePerRule": DISTRESS["extremePer"],
            "note": DISTRESS["why"]}


def limitation_exposure(A, cs, h, factor):
    """R24 미해결 자본행위가 TOP/BOTTOM 에 몰려 있는지(§17)."""
    def rate(getter):
        n = hit = 0
        for c in cs:
            i, j = A.pos[c["startDate"]], A.pos[c["startDate"]] + h
            span = set(A.dates[i + 1:j + 1])
            for t in getter(c):
                n += 1
                if A.unresolved.get(t, set()) & span:
                    hit += 1
        return (n, round(100.0 * hit / n, 3) if n else None)

    def bottom_of(c):
        vals = A.fv.get(c["startDate"]) or {}
        elig = {t: v[factor] for t, v in vals.items() if factor in v}
        ranked = sorted(elig.items(), key=lambda kv: (kv[1], kv[0]),
                        reverse=(FACTORS[factor]["direction"] > 0))
        return buckets([t for t, _ in ranked], c["quantiles"])[-1]

    nt, rt = rate(lambda c: c["topTickers"])
    nb, rb = rate(bottom_of)
    diff = None if rt is None or rb is None else round(rt - rb, 3)
    return {"topNames": nt, "topUnresolvedRatePct": rt,
            "bottomNames": nb, "bottomUnresolvedRatePct": rb,
            "diffPp": diff,
            "exposed": bool(diff is not None and abs(diff) >= 3.0),
            "rule": TSR_LIMITATION["flagRule"]}


def subperiod_split(cs):
    out = {}
    for sp in SUBPERIODS:
        sel = [c for c in cs if sp["start"] <= c["startDate"] <= sp["end"]]
        a = agg(sel)
        out[sp["name"]] = {"cohorts": len(sel),
                           "meanSpreadAnnPct": a["meanSpreadAnnPct"] if a else None,
                           "positiveRate": a["positiveRate"] if a else None}
    pos = sum(1 for v in out.values()
              if v["meanSpreadAnnPct"] is not None and v["meanSpreadAnnPct"] > 0)
    tot = sum(1 for v in out.values() if v["meanSpreadAnnPct"] is not None)
    return {"byPeriod": out, "positive": pos, "total": tot,
            "positiveRatio": round(pos / tot, 4) if tot else None}


def main() -> int:
    A = Analyzer()
    print(f"[r25] months={len(A.dates)} {A.dates[0]}~{A.dates[-1]}",
          file=sys.stderr)

    quant, horiz, subs, ctrl, roll, boot, conc, dist, lim = (
        {}, {}, {}, {}, {}, {}, {}, {}, {})
    base = {}

    for name in FACTORS:
        horiz[name], quant[name] = {}, {}
        for h in ALL_H:
            cs = A.cohorts(name, h)
            if h == 12:
                base[name] = cs
            a = agg(cs)
            horiz[name][f"{h}M"] = a
            quant[name][f"{h}M"] = monotonicity(cs, h)
        print(f"[r25] {name} horizons done", file=sys.stderr)

        cs = base[name]
        subs[name] = subperiod_split(cs)
        sp = [c["spreadAnn"] for c in cs]
        boot[name] = bootstrap(sp)
        conc[name] = concentration(A, cs, 12)
        dist[name] = distress(A, cs, 12, name)
        lim[name] = limitation_exposure(A, cs, 12, name)

        # rolling cohorts
        roll[name] = {}
        for h in HORIZONS["primary"]:
            ov = A.cohorts(name, h)
            no = non_overlapping(ov, h)
            def stat(x):
                s = [c["spreadAnn"] for c in x]
                if not s:
                    return None
                return {"cohorts": len(s),
                        "medianSpreadAnnPct": round(statistics.median(s) * 100, 3),
                        "positiveRate": round(
                            sum(1 for v in s if v > 0) / len(s), 4),
                        "p10Pct": round(pctl(s, 0.10) * 100, 3),
                        "p90Pct": round(pctl(s, 0.90) * 100, 3)}
            roll[name][f"{h}M"] = {"overlapping": stat(ov),
                                   "nonOverlapping": stat(no)}

        # size / exchange controls
        c = {"raw": horiz[name]["12M"]}
        for ex in ("KOSPI", "KOSDAQ"):
            c[f"exchange_{ex}"] = agg(A.cohorts(name, 12, A.exchange_subset(ex)))
        for b in ("SMALL", "MID", "LARGE"):
            c[f"size_{b}"] = agg(A.cohorts(name, 12, A.size_subset(b)))
        cells = []
        for b in ("SMALL", "MID", "LARGE"):
            for ex in ("KOSPI", "KOSDAQ"):
                r = agg(A.cohorts(name, 12, A.size_exchange_subset(b, ex)))
                c[f"cell_{b}_{ex}"] = r
                if r:
                    cells.append(r["meanSpreadAnnPct"])

        def avg(keys):
            v = [c[k]["meanSpreadAnnPct"] for k in keys if c.get(k)]
            return round(statistics.fmean(v), 3) if v else None
        c["sizeMatchedSpreadAnnPct"] = avg(
            ["size_SMALL", "size_MID", "size_LARGE"])
        c["exchangeMatchedSpreadAnnPct"] = avg(
            ["exchange_KOSPI", "exchange_KOSDAQ"])
        c["sizeExchangeMatchedSpreadAnnPct"] = (
            round(statistics.fmean(cells), 3) if cells else None)
        c["rawSpreadAnnPct"] = (horiz[name]["12M"]["meanSpreadAnnPct"]
                                if horiz[name]["12M"] else None)
        ctrl[name] = c
        print(f"[r25] {name} controls done", file=sys.stderr)

    meta = {"task": "R25", "analysisVersion": ANALYSIS_VERSION,
            "engine": "CANONICAL_TSR_R24", "months": len(A.dates),
            "period": {"start": A.dates[0], "end": A.dates[-1]},
            "engineStats": A.eng.stats}
    save("quantile-results", {**meta, "rule": QUANTILES, "byFactor": quant})
    save("horizon-results", {**meta, "horizons": ALL_H,
                             "annualization": HORIZONS["annualization"],
                             "byFactor": horiz})
    save("subperiod-results", {**meta, "subperiods": SUBPERIODS,
                               "byFactor": subs})
    save("size-exchange-controls", {**meta, "byFactor": ctrl})
    save("rolling-cohorts", {**meta, "byFactor": roll})
    save("bootstrap", {**meta, "method": STATS, "byFactor": boot})
    save("concentration", {**meta, "byFactor": conc})
    save("distress", {**meta, "byFactor": dist})
    save("tsr-limitation-exposure", {
        **meta,
        "foundationLimitation": {
            "verdict": "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "extremeDiscontinuityPct": 1.42, "expectedBiasPct": 1.734},
        "byFactor": lim})

    print(json.dumps({"factors": len(FACTORS),
                      "spread12M": {k: (v["12M"]["meanSpreadAnnPct"]
                                        if v.get("12M") else None)
                                    for k, v in horiz.items()}},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
