#!/usr/bin/env python3
"""R26 BM × SIZE 결합 incremental alpha 검증.

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26

세 팔(BM 단독 / SIZE 단독 / COMBO)을 **완전히 같은 조건**에서 비교한다(§8).
결합은 precommit 에 고정한 하나뿐이다(§6). 새 탐색을 하지 않는다.

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
from r25_analysis import agg, ann, buckets, pctl  # noqa: E402
from r25_analysis import Analyzer as R25Analyzer  # noqa: E402
from r25_engine import R25Engine, load_snapshots  # noqa: E402
from r25_factors import compute as compute_factors  # noqa: E402
from r26_precommit import (BOOTSTRAP, COST, DISTRESS, ELIGIBILITY,  # noqa: E402
                           HORIZONS, LIQUIDITY, PRICE_BUCKETS, SUBPERIODS)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
VERSION = "r26-analysis-1"
ARMS = ["BM_ALONE", "SIZE_ALONE", "COMBO"]
ALL_H = HORIZONS["all"]
PRIMARY_H = HORIZONS["primary"]


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r26-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r26] saved {p.name}", file=sys.stderr)


def pct_rank(vals):
    """{t: v} → {t: 0~1 백분위}. 큰 값이 1 에 가깝다. 동점은 ticker 오름차순."""
    items = sorted(vals.items(), key=lambda kv: (kv[1], kv[0]))
    n = len(items)
    if n <= 1:
        return {k: 0.5 for k, _ in items}
    return {k: i / (n - 1) for i, (k, _) in enumerate(items)}


class Combo:
    def __init__(self):
        cap = C.load_capital_series()
        self.dates = contiguous_span(sorted(cap))
        self.eng = R25Engine(self.dates, cap=cap)
        self.snaps = load_snapshots(start=self.dates[0], end=self.dates[-1])
        self.fv, _ = compute_factors(self.snaps, self.dates)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self._tsr = {}
        self.unresolved = self._load_unresolved()
        self.scores = self._build_scores()

    def _load_unresolved(self):
        rows = json.loads((RD / "r24-full-reconciliation-latest.json")
                          .read_text(encoding="utf-8"))["rows"]
        out = {}
        for m in rows:
            if not m.get("resolvedWealth"):
                out.setdefault(m["ticker"], set()).add(m["date"])
        return out

    def _build_scores(self):
        """공유 universe(BM·SIZE 동시 유효) 위에서 세 팔의 점수를 만든다(§8)."""
        out = {}
        for d in self.dates:
            v = self.fv.get(d) or {}
            elig = {t: x for t, x in v.items()
                    if "BM" in x and "SIZE_SMALL" in x}
            if len(elig) < ELIGIBILITY["minUniversePerMonth"]:
                continue
            rb = pct_rank({t: x["BM"] for t, x in elig.items()})
            rs = pct_rank({t: x["SIZE_SMALL"] for t, x in elig.items()})
            out[d] = {t: {"BM_ALONE": elig[t]["BM"],
                          "SIZE_ALONE": elig[t]["SIZE_SMALL"],
                          "COMBO": 0.5 * rb[t] + 0.5 * rs[t],
                          "bmRank": rb[t], "sizeRank": rs[t]}
                      for t in elig}
        return out

    def tsr(self, i, j, t):
        k = (i, j, t)
        v = self._tsr.get(k, 0)
        if v != 0:
            return v
        v = self.eng.tsr(i, j, t)
        self._tsr[k] = v
        return v

    # ── 코호트 ────────────────────────────────────────────────────────
    def cohort(self, arm, i, h, subset=None, exclude=None):
        j = i + h
        if j >= len(self.dates):
            return None
        d = self.dates[i]
        sc = self.scores.get(d)
        if not sc:
            return None
        elig = {t: s[arm] for t, s in sc.items()
                if subset is None or t in subset}
        n = len(elig)
        if n < ELIGIBILITY["minUniversePerMonth"]:
            return None
        q = 10 if n >= 200 else 5
        ranked = [t for t, _ in sorted(elig.items(),
                                       key=lambda kv: (kv[1], kv[0]),
                                       reverse=True)]
        bs = buckets(ranked, q)
        top = [t for t in bs[0] if not (exclude and t in exclude)]
        qret = []
        for b in bs:
            rs = [self.tsr(i, j, t) for t in b]
            rs = [x for x in rs if x is not None]
            qret.append(statistics.fmean(rs) if rs else None)
        tr = [self.tsr(i, j, t) for t in top]
        tr = [x for x in tr if x is not None]
        allr = [self.tsr(i, j, t) for t in elig]
        allr = [x for x in allr if x is not None]
        if not tr or not allr or qret[-1] is None:
            return None
        topc = statistics.fmean(tr)
        return {"arm": arm, "startDate": d, "endDate": self.dates[j],
                "months": h, "quantiles": q, "eligible": n,
                "quantileCum": qret,
                "topAnn": ann(topc, h), "bottomAnn": ann(qret[-1], h),
                "universeAnn": ann(statistics.fmean(allr), h),
                "spreadAnn": ann(topc, h) - ann(qret[-1], h),
                "excessAnn": ann(topc, h) - ann(statistics.fmean(allr), h),
                "topTickers": bs[0], "topCount": len(top)}

    def paired(self, h, subset=None, exclude=None):
        """같은 시작월에서 세 팔을 동시에 만든다 — 짝지음 비교(§10)."""
        out = []
        for i in range(len(self.dates) - h):
            row = {}
            ok = True
            for arm in ARMS:
                c = self.cohort(arm, i, h, subset=subset, exclude=exclude)
                if c is None:
                    ok = False
                    break
                row[arm] = c
            if ok:
                row["startDate"] = self.dates[i]
                out.append(row)
        return out

    # ── subset 생성기 ─────────────────────────────────────────────────
    def exchange_subset(self, ex):
        def f(d):
            return {t for t, r in (self.snaps.get(d) or {}).items()
                    if r.get("market") == ex}
        return f

    def size_band(self, d, band):
        sc = self.scores.get(d) or {}
        caps = sorted(((self.snaps[d][t]["marketCap"], t) for t in sc
                       if (self.snaps.get(d) or {}).get(t, {}).get("marketCap")),
                      key=lambda x: x[0])
        n = len(caps)
        if n < 30:
            return set()
        a, b = n // 3, 2 * n // 3
        seg = {"SMALL": caps[:a], "MID": caps[a:b], "LARGE": caps[b:]}[band]
        return {t for _, t in seg}

    def bm_band(self, d, band):
        sc = self.scores.get(d) or {}
        rk = sorted(((s["bmRank"], t) for t, s in sc.items()), key=lambda x: x[0])
        n = len(rk)
        if n < 30:
            return set()
        a, b = n // 3, 2 * n // 3
        seg = {"CHEAP": rk[b:], "MID": rk[a:b], "EXPENSIVE": rk[:a]}[band]
        return {t for _, t in seg}


# ══════════════ 집계 ══════════════
def arm_stats(rows, arm):
    cs = [r[arm] for r in rows]
    a = agg(cs)
    if not a:
        return None
    a["meanTopAnnPct"] = round(
        statistics.fmean([c["topAnn"] for c in cs]) * 100, 3)
    a["medianTopAnnPct"] = round(
        statistics.median([c["topAnn"] for c in cs]) * 100, 3)
    return a


def incr(rows, a, b, key="topAnn"):
    """짝지음 incremental. a − b 를 코호트마다 계산한 뒤 요약한다."""
    d = [r[a][key] - r[b][key] for r in rows]
    if not d:
        return None
    return {"cohorts": len(d),
            "meanPct": round(statistics.fmean(d) * 100, 3),
            "medianPct": round(statistics.median(d) * 100, 3),
            "positiveRatio": round(sum(1 for x in d if x > 0) / len(d), 4),
            "p10Pct": round(pctl(d, 0.10) * 100, 3),
            "p25Pct": round(pctl(d, 0.25) * 100, 3),
            "p75Pct": round(pctl(d, 0.75) * 100, 3),
            "p90Pct": round(pctl(d, 0.90) * 100, 3),
            "worstPct": round(min(d) * 100, 3),
            "bestPct": round(max(d) * 100, 3)}


def series(rows, a, b, key="topAnn"):
    return [r[a][key] - r[b][key] for r in rows]


def moving_block(xs, seed, block=None, resamples=None):
    n = len(xs)
    b = block or BOOTSTRAP["blockMonths"]
    R = resamples or BOOTSTRAP["resamples"]
    if n < b + 1:
        return None
    rng = random.Random(seed)
    nb = math.ceil(n / b)
    means = []
    for _ in range(R):
        s = []
        for _ in range(nb):
            st = rng.randrange(0, n - b + 1)
            s.extend(xs[st:st + b])
        means.append(statistics.fmean(s[:n]))
    return means


def year_level(rows, xs, seed, resamples=None):
    """연 단위 블록 재표본 — 시계열 구조를 해(year)로 보존한다."""
    R = resamples or BOOTSTRAP["resamples"]
    by = {}
    for r, x in zip(rows, xs):
        by.setdefault(r["startDate"][:4], []).append(x)
    years = sorted(by)
    if len(years) < 5:
        return None
    rng = random.Random(seed)
    means = []
    for _ in range(R):
        s = []
        for _ in range(len(years)):
            s.extend(by[years[rng.randrange(len(years))]])
        means.append(statistics.fmean(s))
    return means


def boot_summary(means, obs):
    if not means:
        return None
    m = sorted(means)
    return {"observations": len(obs),
            "meanPct": round(statistics.fmean(obs) * 100, 3),
            "medianPct": round(statistics.median(obs) * 100, 3),
            "ci95LowPct": round(m[int(0.025 * (len(m) - 1))] * 100, 3),
            "ci95HighPct": round(m[int(0.975 * (len(m) - 1))] * 100, 3),
            "pExcessAbove0": round(sum(1 for x in m if x > 0) / len(m), 4),
            "pExcessAbove1pp": round(sum(1 for x in m if x > 0.01) / len(m), 4),
            "pExcessAbove2pp": round(sum(1 for x in m if x > 0.02) / len(m), 4)}


def non_overlapping(rows, h, arms=None):
    """겹치지 않는 코호트만 남긴다.

    ★ 2026-09-07 최소 수정: arm 이름을 R26 것으로 **하드코딩**하고 있었다
      (`r[ARMS[0]]`). R27 은 arm 이름이 다르므로(SIZE/BM/CONTROL) 이 함수를
      재사용하면 KeyError 가 난다. R27 은 coverage gate 에서 늘 멈춰서 이 경로가
      한 번도 실행된 적이 없었고, R31 이 데이터를 채워 gate 를 통과하자 드러났다.

      arms 를 인자로 받되 **기본값은 기존 R26 ARMS 그대로**다 — 인자를 주지 않는
      기존 호출부(R26)의 동작은 한 글자도 바뀌지 않는다. 코호트의 arm 들은 같은
      기간을 공유하므로 어느 arm 의 endDate 를 읽든 결과는 동일하다.
    """
    use = arms or ARMS
    out, last = [], None
    for r in rows:
        if last is None or r["startDate"] >= last:
            out.append(r)
            key = next((a for a in use if a in r), None)
            if key is None:
                continue
            last = r[key]["endDate"]
    return out


def subperiod(rows, a, b):
    out = {}
    for sp in SUBPERIODS:
        sel = [r for r in rows if sp["start"] <= r["startDate"] <= sp["end"]]
        v = incr(sel, a, b) if sel else None
        out[sp["name"]] = {"cohorts": len(sel),
                           "meanPct": v["meanPct"] if v else None,
                           "positiveRatio": v["positiveRatio"] if v else None}
    pos = sum(1 for x in out.values()
              if x["meanPct"] is not None and x["meanPct"] > 0)
    tot = sum(1 for x in out.values() if x["meanPct"] is not None)
    return {"byPeriod": out, "positive": pos, "total": tot,
            "positiveRatio": round(pos / tot, 4) if tot else None}
