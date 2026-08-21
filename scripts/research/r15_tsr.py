#!/usr/bin/env python3
"""R15 TSR 엔진 — 분할 조정 + 배당 포함 Total Shareholder Return.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

현행 `factor_research.fwd_return` 은 close 만 쓴다:
    return = P_j / P_i - 1
이것은 (1) 액면분할 미조정 (2) 배당 미포함 이라 실제 주주수익이 아니다.

이 엔진은 **보유 주식수를 추적**한다.
    보유주식 s 는 분할/무상 사건마다 배율만큼 늘어나고,
    배당은 매월 (DPS/12)/P 만큼 재투자되어 s 를 늘린다.
    최종 wealth = s_j × P_j (+ 무재투자 방식이면 적립현금)

정의는 전부 `r15_precommit.py` 에 수정 결과 이전에 고정됐다. 여기서 바꾸지 않는다.

안전: 계산 전용 · 네트워크 0 · 파일 write 0(호출자가 저장) · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r15_precommit import DIVIDEND_RULE, SPLIT_RULE  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SNAP_DIR = ROOT / "_cache" / "pit-snapshots"

MIN_RATIO = SPLIT_RULE["minRatio"]
TOL = SPLIT_RULE["tolerance"]


def _f(x):
    if x is None or x == "" or x == "nan":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if (v != v or v in (float("inf"), float("-inf"))) else v


def load_capital_series(snap_dir: Path = SNAP_DIR):
    """{date: {ticker: {close, shares, dps}}} — shares·DPS 는 기존 로더가 안 읽는다."""
    out = {}
    for p in sorted(snap_dir.glob("[12]*.csv.gz")):
        iso = p.name.split(".")[0]
        row = {}
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            head = fh.readline().rstrip("\n").split(",")
            ix = {c: i for i, c in enumerate(head)}
            need = ("ticker", "close", "shares", "DPS")
            if any(k not in ix for k in need):
                continue
            for line in fh:
                c = line.rstrip("\n").split(",")
                if len(c) < len(head):
                    continue
                row[c[ix["ticker"]]] = {
                    "close": _f(c[ix["close"]]), "shares": _f(c[ix["shares"]]),
                    "dps": _f(c[ix["DPS"]])}
        if row:
            out[iso] = row
    return out


def is_split(prev, cur):
    """분할/무상 판정 — 주식수 배율과 가격 역배율이 함께 움직일 때만 True.

    유상증자(주식수↑·가격 비례하락 안 함)와 자사주 소각(주식수↓·가격 비례상승 안 함)은
    이 조건에서 걸러진다. 실측으로 확인된 서명이다(precommit 참조).
    """
    if not prev or not cur:
        return None
    p0, p1 = prev.get("close"), cur.get("close")
    s0, s1 = prev.get("shares"), cur.get("shares")
    if not (p0 and p1 and s0 and s1) or p0 <= 0 or p1 <= 0 or s0 <= 0 or s1 <= 0:
        return None
    sr = s1 / s0
    if not (sr >= MIN_RATIO or sr <= 1.0 / MIN_RATIO):
        return None
    pr = p0 / p1
    if abs(pr / sr - 1.0) < TOL:
        return sr
    return None


class TsrIndex:
    """날짜 인덱스별 보유주식 배율(분할 누적)과 월 소득수익률을 미리 만든다."""

    def __init__(self, dates, cap=None):
        self.dates = list(dates)
        self.cap = cap if cap is not None else load_capital_series()
        self.pos = {d: i for i, d in enumerate(self.dates)}
        # splitFactor[i][t] = i-1 → i 로 넘어갈 때 보유주식에 곱할 배율
        self.split = [dict() for _ in self.dates]
        self.events = []
        self._cum = None
        for i in range(1, len(self.dates)):
            a = self.cap.get(self.dates[i - 1], {})
            b = self.cap.get(self.dates[i], {})
            for t, cur in b.items():
                r = is_split(a.get(t), cur)
                if r:
                    self.split[i][t] = r
                    self.events.append({"fromDate": self.dates[i - 1],
                                        "toDate": self.dates[i], "ticker": t,
                                        "shareRatio": r,
                                        "priceRatio": (a[t]["close"] / cur["close"]),
                                        "rawEngineReturnPct":
                                            100 * (cur["close"] / a[t]["close"] - 1)})

    # ── 누적계수 (성능 핵심) ──────────────────────────────────────────
    def _build_cum(self):
        """cum[i][t] = 0→i 동안 보유 1주가 몇 주가 됐는지(분할 × 배당재투자).

        tsr(i,j) 를 매번 개월 순회로 계산하면 84개월 horizon 에서 84배 느려져
        전체 재실행이 사실상 불가능하다(실측: 재실행이 끝나지 않음).
        누적곱을 미리 만들어 두면 배율 = cum[j][t] / cum[i][t] 로 **O(1)** 이 된다.
        재투자 방식은 경로 독립이라 이 변환이 정확히 동치다.
        """
        if self._cum is not None:
            return
        self._cum = [dict() for _ in self.dates]
        run = {}
        for t in self.cap.get(self.dates[0], {}):
            run[t] = 1.0
        self._cum[0] = dict(run)
        for i in range(1, len(self.dates)):
            snap = self.cap.get(self.dates[i], {})
            for t in snap:
                v = run.get(t, 1.0)
                f = self.split[i].get(t)
                if f:
                    v *= f
                y = self.monthly_income_yield(i, t)
                if y:
                    v *= (1.0 + y)
                run[t] = v
            self._cum[i] = dict(run)

    def growth(self, i, j, t):
        """i→j 동안 보유주식 배율(분할 + 배당재투자). 없으면 None."""
        if self._cum is None:
            self._build_cum()
        a = self._cum[i].get(t)
        b = self._cum[j].get(t)
        if a is None or b is None or a <= 0:
            return None
        return b / a

    def tsr_fast(self, i, j, t, haircut=0.0):
        """재투자 TSR 의 O(1) 구현. tsr(reinvest=True) 와 수치적으로 동치."""
        p0 = self.price(i, t)
        if not p0 or p0 <= 0:
            return None
        g = self.growth(i, j, t)
        if g is None:
            return None
        p1 = self.price(j, t)
        if p1 is None:
            p1 = self.last_price(j, t)
            if p1 is None:
                return None
            p1 *= (1.0 - haircut)
        return g * p1 / p0 - 1.0

    def price(self, i, t):
        r = self.cap.get(self.dates[i], {}).get(t)
        return r["close"] if r and r["close"] else None

    def last_price(self, i, t):
        """i 이하에서 마지막으로 관측된 종가(상장폐지 청산용)."""
        for k in range(i, -1, -1):
            p = self.price(k, t)
            if p:
                return p
        return None

    def monthly_income_yield(self, i, t):
        """그 달의 소득수익률 = (직전 연간 DPS / 12) / 종가."""
        r = self.cap.get(self.dates[i], {}).get(t)
        if not r or not r["close"] or r["close"] <= 0:
            return 0.0
        d = r["dps"]
        if not d or d <= 0:
            return 0.0
        return (d / 12.0) / r["close"]

    # ────────────────── 핵심 API ──────────────────
    def tsr(self, i, j, t, *, reinvest=True, include_dividend=True,
            adjust_split=True, haircut=0.0):
        """i → j 보유 시 실제 주주수익률.

        보유주식 s 로 시작(=1). 각 달마다
          - 분할/무상이면 s *= 배율
          - 배당은 재투자면 s *= (1+월수익률), 아니면 현금으로 적립
        마지막에 종목이 사라졌으면 마지막 관측가로 청산(R7 정본과 동일).
        """
        p0 = self.price(i, t)
        if not p0 or p0 <= 0:
            return None
        s = 1.0
        cash = 0.0
        for k in range(i + 1, j + 1):
            if adjust_split:
                f = self.split[k].get(t)
                if f:
                    s *= f
            if include_dividend:
                y = self.monthly_income_yield(k, t)
                if y:
                    if reinvest:
                        s *= (1.0 + y)
                    else:
                        pk = self.price(k, t)
                        if pk:
                            cash += s * y * pk
        p1 = self.price(j, t)
        gone = False
        if p1 is None:
            p1 = self.last_price(j, t)
            if p1 is None:
                return None
            p1 *= (1.0 - haircut)
            gone = True
        wealth = s * p1 + cash
        return wealth / p0 - 1.0

    def price_return(self, i, j, t, haircut=0.0):
        """현행 엔진과 동일한 미조정 가격수익률(비교용)."""
        return self.tsr(i, j, t, reinvest=False, include_dividend=False,
                        adjust_split=False, haircut=haircut)

    def split_adjusted_return(self, i, j, t, haircut=0.0):
        return self.tsr(i, j, t, reinvest=False, include_dividend=False,
                        adjust_split=True, haircut=haircut)


def make_fwd_return(tsr_index, *, reinvest=True, include_dividend=True,
                    adjust_split=True):
    """factor_research.fwd_return 과 같은 시그니처의 대체 함수를 만든다.

    quantile_panel 이 `fwd_return(px, i, j, ticker, haircut)` 으로 부르므로
    px 는 무시하고 TsrIndex 를 쓴다.
    """
    if reinvest and include_dividend and adjust_split:
        tsr_index._build_cum()          # 누적계수 O(1) 경로

        def fast(px, i, j, ticker, haircut=0.0):
            return tsr_index.tsr_fast(i, j, ticker, haircut=haircut)
        return fast

    def fn(px, i, j, ticker, haircut=0.0):
        return tsr_index.tsr(i, j, ticker, reinvest=reinvest,
                             include_dividend=include_dividend,
                             adjust_split=adjust_split, haircut=haircut)
    return fn


__all__ = ["TsrIndex", "load_capital_series", "is_split", "make_fwd_return",
           "MIN_RATIO", "TOL"]
