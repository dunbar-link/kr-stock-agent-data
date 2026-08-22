#!/usr/bin/env python3
"""R27 거래가능성 분석 — 20거래일 유동성 지표 · PRIMARY gate · 공정 비교.

WABABA-SIZE-TRADABILITY-VALIDATION-R27

BM 과 SIZE 에 **완전히 같은** tradable universe 를 적용한다(§7·§18).
결정일 직전 20거래일만 쓴다 — 결정일 당일·이후 거래량은 look-ahead 다(§8).
결측을 0 으로 취급하지 않는다(§30).

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402
from r25_analysis import agg, ann, buckets, pctl  # noqa: E402
from r25_engine import R25Engine, load_snapshots  # noqa: E402
from r25_factors import compute as compute_factors  # noqa: E402
from r26_analysis import (boot_summary, moving_block, non_overlapping,  # noqa: E402
                          pct_rank, year_level)
from r27_collect import LOOKBACK, load_day, trading_calendar  # noqa: E402
from r27_precommit import (CAPITAL, HORIZONS, PARTICIPATION,  # noqa: E402
                           PRICE_BUCKETS, PRIMARY_GATE, SENSITIVITY)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
VERSION = "r27-analysis-1"
ARMS = ["SIZE", "BM", "CONTROL"]
ALL_H = HORIZONS["all"]
PH = HORIZONS["primary"]
ORDER = CAPITAL["orderPerNameKrw"]

# 탈락 사유 (§21)
REASONS = ["MISSING_DATA", "NOT_TRADED_ON_DECISION_DATE", "INSUFFICIENT_DAYS",
           "ZERO_VOLUME_DAYS", "LOW_TRADED_VALUE"]


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r27-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r27] saved {p.name}", file=sys.stderr)


def price_bucket(p):
    for b in PRICE_BUCKETS:
        if (b["lo"] is None or p >= b["lo"]) and (b["hi"] is None or p < b["hi"]):
            return b["name"]
    return PRICE_BUCKETS[-1]["name"]


def participation_bucket(x):
    names = PARTICIPATION["bucketNames"]
    cuts = PARTICIPATION["buckets"]
    for i, c in enumerate(cuts):
        if x <= c:
            return names[i]
    return names[-1]


class Tradability:
    """결정일별 종목 유동성 지표 + PRIMARY gate 판정."""

    def __init__(self, decision_dates):
        self.cal = trading_calendar()
        self.pos = {d: i for i, d in enumerate(self.cal)}
        self.dates = list(decision_dates)
        self.metrics = {}      # {decisionDate: {ticker: {...}}}
        self._build()

    def _window(self, d):
        i = self.pos.get(d)
        if i is None:
            prev = [k for k, x in enumerate(self.cal) if x <= d]
            if not prev:
                return []
            i = prev[-1] + 1
        return self.cal[max(0, i - LOOKBACK):i]      # 결정일 **직전**만

    def _build(self):
        for d in self.dates:
            win = self._window(d)
            days = [load_day(x) for x in win]
            days = [x for x in days if x]
            if not days:
                self.metrics[d] = {}
                continue
            acc = {}
            for frame in days:
                for t, r in frame.items():
                    a = acc.setdefault(t, {"tv": [], "vol": [], "zero": 0})
                    a["tv"].append(r["tradedValue"])
                    a["vol"].append(r["volume"])
                    if r["volume"] <= 0:
                        a["zero"] += 1
            last = days[-1]
            out = {}
            for t, a in acc.items():
                lastrow = last.get(t)
                out[t] = {
                    "observedDays20": len(a["tv"]),
                    "zeroVolumeDays20": a["zero"],
                    "median20TradedValue": statistics.median(a["tv"]),
                    "mean20TradedValue": statistics.fmean(a["tv"]),
                    "median20Volume": statistics.median(a["vol"]),
                    # 결정일 '당일' 거래 여부는 look-ahead 가 아니라 **직전 거래일**
                    # 관측으로 판정한다(결정일 데이터는 쓰지 않는다).
                    "tradedOnLastDay": bool(lastrow and lastrow["volume"] > 0),
                }
            self.metrics[d] = out

    def evaluate(self, d, t, threshold=None):
        """(tradable, reason). reason 은 탈락 사유(통과면 None)."""
        thr = PRIMARY_GATE["thresholdKrw"] if threshold is None else threshold
        m = (self.metrics.get(d) or {}).get(t)
        if not m:
            return False, "MISSING_DATA"
        if not m["tradedOnLastDay"]:
            return False, "NOT_TRADED_ON_DECISION_DATE"
        if m["observedDays20"] < 15:
            return False, "INSUFFICIENT_DAYS"
        if m["zeroVolumeDays20"] > 5:
            return False, "ZERO_VOLUME_DAYS"
        if m["median20TradedValue"] < thr:
            return False, "LOW_TRADED_VALUE"
        return True, None

    def tradable_set(self, d, universe, threshold=None):
        return {t for t in universe if self.evaluate(d, t, threshold)[0]}


class R27:
    def __init__(self):
        cap = C.load_capital_series()
        self.dates = contiguous_span(sorted(cap))
        self.eng = R25Engine(self.dates, cap=cap)
        self.snaps = load_snapshots(start=self.dates[0], end=self.dates[-1])
        self.fv, _ = compute_factors(self.snaps, self.dates)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self._tsr = {}
        self.tr = Tradability(self.dates)
        self.base = self._shared_universe()

    def _shared_universe(self):
        """R26 과 동일한 공유 universe(BM·SIZE 동시 유효) + 백분위."""
        out = {}
        for d in self.dates:
            v = self.fv.get(d) or {}
            elig = {t: x for t, x in v.items()
                    if "BM" in x and "SIZE_SMALL" in x}
            if len(elig) < 100:
                continue
            out[d] = {"BM": {t: x["BM"] for t, x in elig.items()},
                      "SIZE": {t: x["SIZE_SMALL"] for t, x in elig.items()}}
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
        b = self.base.get(d)
        if not b:
            return None
        uni = set(b["BM"])
        if subset is not None:
            uni &= subset
        if len(uni) < 100:
            return None
        allr = [self.tsr(i, j, t) for t in uni]
        allr = [x for x in allr if x is not None]
        if not allr:
            return None
        ctrl = statistics.fmean(allr)
        if arm == "CONTROL":
            return {"arm": arm, "startDate": d, "endDate": self.dates[j],
                    "months": h, "eligible": len(uni), "quantiles": 0,
                    "topAnn": ann(ctrl, h), "bottomAnn": ann(ctrl, h),
                    "universeAnn": ann(ctrl, h), "spreadAnn": 0.0,
                    "excessAnn": 0.0, "topTickers": [], "quantileCum": []}
        vals = {t: b[arm][t] for t in uni}
        q = 10 if len(uni) >= 200 else 5
        ranked = [t for t, _ in sorted(vals.items(),
                                       key=lambda kv: (kv[1], kv[0]),
                                       reverse=True)]
        bs = buckets(ranked, q)
        top = [t for t in bs[0] if not (exclude and t in exclude)]
        qret = []
        for bb in bs:
            rs = [self.tsr(i, j, t) for t in bb]
            rs = [x for x in rs if x is not None]
            qret.append(statistics.fmean(rs) if rs else None)
        tr = [self.tsr(i, j, t) for t in top]
        tr = [x for x in tr if x is not None]
        if not tr or qret[-1] is None:
            return None
        topc = statistics.fmean(tr)
        return {"arm": arm, "startDate": d, "endDate": self.dates[j],
                "months": h, "eligible": len(uni), "quantiles": q,
                "quantileCum": qret,
                "topAnn": ann(topc, h), "bottomAnn": ann(qret[-1], h),
                "universeAnn": ann(ctrl, h),
                "spreadAnn": ann(topc, h) - ann(qret[-1], h),
                "excessAnn": ann(topc, h) - ann(ctrl, h),
                "topTickers": bs[0], "topKept": top}

    def paired(self, h, tradable=False, threshold=None, exclude=None):
        """세 팔을 같은 시작월·같은 universe 로 동시에 만든다(§18)."""
        out = []
        for i in range(len(self.dates) - h):
            d = self.dates[i]
            if d not in self.base:
                continue
            sub = None
            if tradable:
                sub = self.tr.tradable_set(d, set(self.base[d]["BM"]), threshold)
            row, ok = {}, True
            for a in ARMS:
                c = self.cohort(a, i, h, subset=sub, exclude=exclude)
                if c is None:
                    ok = False
                    break
                row[a] = c
            if ok:
                row["startDate"] = d
                out.append(row)
        return out

    def exchange_subset(self, d, ex):
        return {t for t, r in (self.snaps.get(d) or {}).items()
                if r.get("market") == ex}


# ── 집계 helper ───────────────────────────────────────────────────────
def arm_stats(rows, arm):
    cs = [r[arm] for r in rows]
    if not cs:
        return None
    return {"cohorts": len(cs),
            "meanTopAnnPct": round(statistics.fmean(
                [c["topAnn"] for c in cs]) * 100, 3),
            "medianTopAnnPct": round(statistics.median(
                [c["topAnn"] for c in cs]) * 100, 3),
            "meanSpreadAnnPct": round(statistics.fmean(
                [c["spreadAnn"] for c in cs]) * 100, 3),
            "meanExcessAnnPct": round(statistics.fmean(
                [c["excessAnn"] for c in cs]) * 100, 3),
            "meanUniverseAnnPct": round(statistics.fmean(
                [c["universeAnn"] for c in cs]) * 100, 3),
            "meanEligible": round(statistics.fmean(
                [c["eligible"] for c in cs]), 1)}


def diff_series(rows, a, b, key="topAnn"):
    return [r[a][key] - r[b][key] for r in rows]


def summarize_diff(xs):
    if not xs:
        return None
    return {"cohorts": len(xs),
            "meanPct": round(statistics.fmean(xs) * 100, 3),
            "medianPct": round(statistics.median(xs) * 100, 3),
            "positiveRatio": round(sum(1 for x in xs if x > 0) / len(xs), 4),
            "p10Pct": round(pctl(xs, 0.10) * 100, 3),
            "p90Pct": round(pctl(xs, 0.90) * 100, 3),
            "worstPct": round(min(xs) * 100, 3),
            "bestPct": round(max(xs) * 100, 3)}
