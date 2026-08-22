#!/usr/bin/env python3
"""R25 canonical TSR 엔진 어댑터 — R24 승인 엔진을 O(1) 조회로 만든다.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

새 수익률 정의를 만들지 않는다(§3). R16 CanonicalWealth + R17 RightsWealth
정책 + R17~R24 직접판정 override 를 **그대로** 쓰되, factor 연구가 요구하는
수백만 회 조회를 감당하도록 월별 누적계수로 접는다.

  월 계수 effective_m(k,t) = m(k)·(1+y(k)) − extPerShare(k)/p(k)
  TSR(i→j)                 = (cum[j]/cum[i])·(p_j/p_i) − 1

  m            : 기계적 주식수 배수(분할·무상증자·주식배당) 또는 (1+r) 청약
  y            : 그 달 배당수익률(재투자)
  extPerShare  : 청약 납입금 r·K (외부 납입 — 수익이 아니다)

RightsWealth.run 과 수학적으로 동치이며, 회귀 테스트가 실측으로 강제한다.

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import csv
import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SNAP = ROOT / "_cache" / "pit-snapshots"

ENGINE_VERSION = "r25-engine-1 (CANONICAL_TSR_R24)"


def _f(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v and abs(v) != float("inf") else None


def load_snapshots(start=None, end=None):
    """월별 PIT 스냅샷 전체 필드. {date: {ticker: row}}"""
    out = {}
    for p in sorted(SNAP.glob("*.csv.gz")):
        d = p.name[:-7]
        if (start and d < start) or (end and d > end):
            continue
        rows = {}
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                t = r.get("ticker")
                if not t:
                    continue
                rows[t] = {
                    "market": r.get("market"),
                    "close": _f(r.get("close")),
                    "marketCap": _f(r.get("marketCap")),
                    "shares": _f(r.get("shares")),
                    "PER": _f(r.get("PER")), "PBR": _f(r.get("PBR")),
                    "EPS": _f(r.get("EPS")), "BPS": _f(r.get("BPS")),
                    "DIV": _f(r.get("DIV")), "DPS": _f(r.get("DPS")),
                }
        out[d] = rows
    return out


class R25Engine:
    """R24 승인 canonical TSR 을 누적계수로 접은 조회 엔진."""

    def __init__(self, dates=None, cap=None, overrides=None):
        self.cap = cap if cap is not None else C.load_capital_series()
        self.dates = list(dates) if dates else contiguous_span(sorted(self.cap))
        self.eng = C.CanonicalWealth(self.dates, cap=self.cap)
        self.pos = self.eng.pos
        self.ov = overrides if overrides is not None else load_overrides()
        self.rw = W.RightsWealth(self.eng, [])
        self.rw.ov = self.ov
        self.stats = {"MECHANICAL_OVERRIDE": 0, "RIGHTS_EXERCISED": 0,
                      "RIGHTS_LAPSED": 0, "R16_MECHANICAL": 0}
        self._build()

    # ── 월 계수 ───────────────────────────────────────────────────────
    def _build(self):
        n = len(self.dates)
        self.cum = [dict() for _ in range(n)]
        run = {t: 1.0 for t in self.cap.get(self.dates[0], {})}
        self.cum[0] = dict(run)
        for k in range(1, n):
            month = self.cap.get(self.dates[k], {})
            for t in month:
                v = run.get(t, 1.0)
                p_cur = self.eng.price(k, t)
                m, ext_ps = 1.0, 0.0
                plan = self.ov.get((t, self.dates[k]))
                e = self.eng.events[k].get(t)
                if plan:
                    if plan["kind"] == "MECHANICAL":
                        m = plan["factor"]
                        self.stats["MECHANICAL_OVERRIDE"] += 1
                    elif plan["kind"] == "RIGHTS":
                        kk, r = plan["issuePrice"], plan["ratio"]
                        if p_cur and kk < p_cur:      # R17 합리적 청약
                            m, ext_ps = 1.0 + r, r * kk
                            self.stats["RIGHTS_EXERCISED"] += 1
                        else:                          # 합리적 실권
                            self.stats["RIGHTS_LAPSED"] += 1
                elif e and e["adjust"]:
                    m = e["shareRatio"]
                    self.stats["R16_MECHANICAL"] += 1
                y, _fl = self.eng.dividend_state(k, t)
                eff = m * (1.0 + y)
                if ext_ps and p_cur:
                    eff -= ext_ps / p_cur
                run[t] = v * eff
            self.cum[k] = dict(run)

    # ── 공개 API ──────────────────────────────────────────────────────
    def price(self, i, t):
        return self.eng.price(i, t)

    def tsr(self, i, j, t, haircut=0.0):
        """canonical TSR (i → j). 상폐는 마지막 관측가로 청산(UNKNOWN_RECOVERY)."""
        if j <= i:
            return None
        p0 = self.eng.price(i, t)
        a = self.cum[i].get(t)
        b = self.cum[j].get(t)
        if not p0 or p0 <= 0 or a is None or a <= 0:
            return None
        if b is None:
            b = self._last_cum(j, t)
            if b is None:
                return None
        p1 = self.eng.price(j, t)
        delisted = False
        if p1 is None:
            p1 = self.eng.last_price(j, t)
            if p1 is None:
                return None
            p1 *= (1.0 - haircut)
            delisted = True
        self._last_delisted = delisted
        return (b / a) * p1 / p0 - 1.0

    def _last_cum(self, j, t):
        for k in range(j, -1, -1):
            v = self.cum[k].get(t)
            if v is not None:
                return v
        return None

    def is_delisted_by(self, j, t):
        """j 시점에 가격이 없으면 그 사이에 상장폐지된 것으로 본다."""
        return self.eng.price(j, t) is None

    def cagr(self, i, j, t, haircut=0.0):
        cum = self.tsr(i, j, t, haircut=haircut)
        if cum is None or cum <= -1.0:
            return None if cum is None else -1.0
        return (1.0 + cum) ** (12.0 / (j - i)) - 1.0


def load_overrides(path=None):
    """R17~R24 직접판정을 r17_wealth 계획으로 변환한다. 정본은 R24 재분류."""
    import json
    p = path or (RD / "r24-full-reconciliation-latest.json")
    rows = json.loads(Path(p).read_text(encoding="utf-8"))["rows"]
    eng_stub = type("S", (), {"stats": {"MECHANICAL_APPLIED": 0,
                                        "RIGHTS_PLANNED": 0,
                                        "RIGHTS_TERMS_MISSING": 0,
                                        "NO_ADJUST": 0}})()
    rw = W.RightsWealth.__new__(W.RightsWealth)
    rw.stats = eng_stub.stats
    ov = {}
    for m in rows:
        if not m.get("label"):
            continue
        ov[(m["ticker"], m["date"])] = W.RightsWealth._plan(rw, m)
    return ov
