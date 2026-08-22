#!/usr/bin/env python3
"""R16 canonical wealth engine — 주주가 실제로 가진 wealth 를 계산한다.

WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16

R15 엔진의 한계를 넘는다:
  R15 는 주식수 배율과 가격 역배율만 보고 '분할'로 판정했다. 그 서명은 **유상증자와
  구분되지 않는다**(실측: 시총이 함께 뛴 359건). 유상증자를 무상으로 조정하면
  주주가 내지 않은 돈으로 가짜 wealth 가 생긴다.
  R16 은 **시총 연속성**을 필수 2차 신호로 추가하고, 조정하지 않는 사건을
  명시적으로 분류해 ledger 에 남긴다.

정의는 전부 `r16_precommit.py` 에 결과 이전에 고정됐다. 여기서 바꾸지 않는다.

방식(§22): RAW_PRICE + EXPLICIT_EVENT_LEDGER 하나만 쓴다. adjusted 시리즈와 혼합 금지.

안전: 계산 전용 · 네트워크 0 · 파일 write 0(호출자가 저장) · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r15_tsr import load_capital_series  # noqa: E402  스냅샷 로더 재사용
from r16_precommit import ANOMALY, CLASSIFIER, DELISTING  # noqa: E402

MIN_SR = CLASSIFIER["minShareRatio"]
TOL = CLASSIFIER["proportionalTolerance"]
MC_LO, MC_HI = CLASSIFIER["mcapContinuityBand"]

def rel_err(sr, pr):
    """로그공간 상대오차. 사건 크기에 비례해 허용폭이 커진다."""
    ls = math.log(sr)
    if ls == 0.0:
        return float("inf")
    return abs(math.log(pr) - ls) / abs(ls)


MECHANICAL = "MECHANICAL_SHARE_CHANGE"
RIGHTS = "SUSPECTED_RIGHTS_ISSUE"
DOWN_NP = "SHARES_DOWN_NONPROPORTIONAL"
UP_NP = "SHARES_UP_NONPROPORTIONAL"
NONE = "NO_EVENT"


def classify(prev, cur):
    """사건 분류 — 로그 상대오차 기준(§35 2차 자체수정).

    ★ 자체수정 1: 초안은 시총을 '독립 3번째 신호'로 썼으나, 스냅샷 marketCap 은
      close × shares 와 정확히 일치한다(불일치 0/2,035). mcapRatio = shareRatio /
      priceRatio 이므로 비례성 검정과 동치이고 독립 정보가 없다. 독립 신호는
      주식수·가격 2개뿐이며 **유상증자를 무상증자와 분리할 독립 근거는 없다.**

    ★ 자체수정 2: 절대오차 |priceRatio/shareRatio - 1| 은 사건 크기에 스케일이
      맞지 않는다. 전수 실측(주식수 1.2배 이상 변동 9,032건)에서 이 값의 분포는
      0~0.30 구간이 거의 균일해 **골짜기가 없었다** — 어떤 임계값도 자의적이라는 뜻.
      원인: 월간 스냅샷이라 사건과 그 달의 주가변동이 섞이고, 자본거래 전후 소형주는
      월 25% 이상 움직이는 일이 흔하다(정수비 사건의 절대오차 중앙값 0.254).

      대신 로그공간 상대오차를 쓴다:
          relErr = |ln(priceRatio) - ln(shareRatio)| / |ln(shareRatio)|
      사건이 클수록 허용폭이 커져 스케일이 맞는다. 실측 분리력(정수비 vs 비정수비
      대조군): 절대오차 중앙값 격차 0.254/0.324(1.3배) → 로그 상대오차 0.245/0.834
      (3.4배). 임계 0.30~0.40 이 평탄한 최대분리 구간이라 중점 0.35 를 쓴다.

      검증: 삼성 50:1 분할 0.008 통과 · 1:5 역분할 0.000 통과 ·
            자사주 소각(주식수 0.8배, 가격 무변동) 1.000 배제.

    ★ 남는 한계(숨기지 않는다): 최적 임계에서도 비정수비 사건의 18.3% 가 통과하고
      정수비 사건의 46% 가 탈락한다. 개별 사건 분류는 여전히 신뢰할 수 없으며,
      이것이 FOUNDATION_BLOCKED 판정의 근거다.

    반환: (class, shareRatio, priceRatio, mcapRatio)
    """
    if not prev or not cur:
        return NONE, None, None, None
    p0, p1 = prev.get("close"), cur.get("close")
    s0, s1 = prev.get("shares"), cur.get("shares")
    if not (p0 and p1 and s0 and s1) or min(p0, p1, s0, s1) <= 0:
        return NONE, None, None, None
    sr = s1 / s0
    if 1.0 / MIN_SR < sr < MIN_SR:
        return NONE, sr, p0 / p1, (p1 * s1) / (p0 * s0)
    pr = p0 / p1
    mcr = (p1 * s1) / (p0 * s0)
    mech = rel_err(sr, pr) < TOL
    if mech and MC_LO <= mcr <= MC_HI:
        return MECHANICAL, sr, pr, mcr
    if sr > 1 and mcr > MC_HI:
        return RIGHTS, sr, pr, mcr          # 시총이 뛰었다 = 새 자본 유입
    if sr < 1:
        return DOWN_NP, sr, pr, mcr         # 자사주 소각 등 — 보유수 불변
    return UP_NP, sr, pr, mcr               # 판정 불가 — 조정하지 않는다


class CanonicalWealth:
    """event-driven wealth ledger 기반 canonical TSR 엔진."""

    def __init__(self, dates, cap=None):
        self.dates = list(dates)
        self.cap = cap if cap is not None else load_capital_series()
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self.events = [dict() for _ in self.dates]     # i: {ticker: event}
        self.counts = {MECHANICAL: 0, RIGHTS: 0, DOWN_NP: 0, UP_NP: 0}
        for i in range(1, len(self.dates)):
            a = self.cap.get(self.dates[i - 1], {})
            b = self.cap.get(self.dates[i], {})
            for t, cur in b.items():
                cls, sr, pr, mcr = classify(a.get(t), cur)
                if cls == NONE:
                    continue
                self.events[i][t] = {"class": cls, "shareRatio": sr,
                                     "priceRatio": pr, "mcapRatio": mcr,
                                     "date": self.dates[i],
                                     "adjust": cls == MECHANICAL,
                                     "provenance": "DERIVED_FROM_SHARES_PRICE_MCAP"}
                self.counts[cls] += 1
        self._cum = None

    # ── 배당 ──────────────────────────────────────────────────────────
    def dividend_state(self, i, t):
        """(월 소득수익률, flag). DPS 결측과 0 을 구분한다(§20)."""
        r = self.cap.get(self.dates[i], {}).get(t)
        if not r or not r.get("close") or r["close"] <= 0:
            return 0.0, "NO_PRICE"
        d = r.get("dps")
        if d is None:
            return 0.0, "DPS_MISSING"
        if d <= 0:
            return 0.0, "DPS_ZERO"
        return (d / 12.0) / r["close"], "DPS_PRESENT"

    def price(self, i, t):
        r = self.cap.get(self.dates[i], {}).get(t)
        return r["close"] if r and r.get("close") else None

    def last_price(self, i, t):
        for k in range(i, -1, -1):
            p = self.price(k, t)
            if p:
                return p
        return None

    # ── 누적계수 (O(1) 조회) ──────────────────────────────────────────
    def _build_cum(self):
        if self._cum is not None:
            return
        self._cum = [dict() for _ in self.dates]
        run = {t: 1.0 for t in self.cap.get(self.dates[0], {})}
        self._cum[0] = dict(run)
        for i in range(1, len(self.dates)):
            for t in self.cap.get(self.dates[i], {}):
                v = run.get(t, 1.0)
                e = self.events[i].get(t)
                if e and e["adjust"]:
                    v *= e["shareRatio"]
                y, _ = self.dividend_state(i, t)
                if y:
                    v *= (1.0 + y)
                run[t] = v
            self._cum[i] = dict(run)

    # ══════════════ 공개 API (§28) ══════════════
    def get_total_return(self, ticker, start_date, end_date, mode="CANONICAL_TSR",
                         haircut=0.0):
        """canonical 총주주수익률.

        반환: ending wealth · cumulative return · CAGR · event count ·
              data-quality flags · unsupported-event flags
        mode: CANONICAL_TSR(배당재투자) / NO_REINVEST / PRICE_ONLY_UNSAFE
        """
        i = self.pos.get(start_date)
        j = self.pos.get(end_date)
        if i is None or j is None or j <= i:
            return None
        return self._run(i, j, ticker, mode=mode, haircut=haircut)

    def _run(self, i, j, t, *, mode="CANONICAL_TSR", haircut=0.0, ledger=False):
        p0 = self.price(i, t)
        if not p0 or p0 <= 0:
            return None
        reinvest = mode == "CANONICAL_TSR"
        use_div = mode in ("CANONICAL_TSR", "NO_REINVEST")
        adjust = mode != "PRICE_ONLY_UNSAFE"
        shares = 1.0
        cash = 0.0
        external = 0.0
        rows = []
        flags = {"DPS_MISSING": 0, "DPS_ZERO": 0, "DPS_PRESENT": 0}
        ev_counts = {MECHANICAL: 0, RIGHTS: 0, DOWN_NP: 0, UP_NP: 0}
        for k in range(i + 1, j + 1):
            w_before = shares * (self.price(k - 1, t) or 0) + cash
            e = self.events[k].get(t)
            if e:
                ev_counts[e["class"]] += 1
                if adjust and e["adjust"]:
                    shares *= e["shareRatio"]
            div_cash = 0.0
            if use_div:
                y, fl = self.dividend_state(k, t)
                flags[fl] = flags.get(fl, 0) + 1
                if y:
                    if reinvest:
                        shares *= (1.0 + y)
                    else:
                        pk = self.price(k, t)
                        if pk:
                            div_cash = shares * y * pk
                            cash += div_cash
            if ledger:
                pk = self.price(k, t)
                rows.append({
                    "date": self.dates[k], "ticker": t,
                    "sharesHeld": shares, "cash": round(cash),
                    "price": pk, "marketValue": round(shares * pk) if pk else None,
                    "totalWealth": round(shares * pk + cash) if pk else None,
                    "dividendCash": round(div_cash),
                    "rightsValue": 0.0, "capitalContribution": 0.0,
                    "corporateActionEvent": e["class"] if e else None,
                    "splitFactor": e["shareRatio"] if (e and e["adjust"]) else None,
                    "shareCountBefore": (self.cap[self.dates[k - 1]].get(t) or {}).get("shares"),
                    "shareCountAfter": (self.cap[self.dates[k]].get(t) or {}).get("shares"),
                    "source": "PIT_SNAPSHOT",
                    "provenance": e["provenance"] if e else "NO_EVENT",
                    "wealthBefore": round(w_before),
                    "externalCashFlow": 0.0,
                    "wealthAfter": round(shares * pk + cash) if pk else None,
                })
        p1 = self.price(j, t)
        recovery = None
        if p1 is None:
            p1 = self.last_price(j, t)
            if p1 is None:
                return None
            p1 *= (1.0 - haircut)
            recovery = DELISTING["canonicalDefault"]
        wealth = shares * p1 + cash
        months = j - i
        cum = wealth / p0 - 1.0
        out = {
            "ticker": t, "startDate": self.dates[i], "endDate": self.dates[j],
            "months": months, "mode": mode,
            "startPrice": p0, "endPrice": p1,
            "endingShares": shares, "endingCash": round(cash),
            "endingWealth": wealth,
            "cumulativeReturn": cum,
            "cagr": ((1.0 + cum) ** (12.0 / months) - 1.0) if cum > -1 else None,
            "externalContribution": external,
            "eventCounts": ev_counts,
            "eventCountTotal": sum(ev_counts.values()),
            "dataQualityFlags": flags,
            "unsupportedEventFlags": {
                "SUSPECTED_RIGHTS_NOT_ADJUSTED": ev_counts[RIGHTS],
                "SHARES_UP_UNRESOLVED": ev_counts[UP_NP],
                "MERGER": "UNSUPPORTED_DATA", "SPINOFF": "UNSUPPORTED_DATA",
            },
            "delistingRecovery": recovery,
        }
        if ledger:
            out["ledger"] = rows
        return out

    def wealth_index(self, i, j, t, haircut=0.0):
        """O(1) canonical TSR (누적계수). _run(CANONICAL_TSR) 와 동치."""
        self._build_cum()
        p0 = self.price(i, t)
        a = self._cum[i].get(t)
        b = self._cum[j].get(t)
        if not p0 or p0 <= 0 or a is None or b is None or a <= 0:
            return None
        p1 = self.price(j, t)
        if p1 is None:
            p1 = self.last_price(j, t)
            if p1 is None:
                return None
            p1 *= (1.0 - haircut)
        return (b / a) * p1 / p0 - 1.0

    # ── §29 raw-close guard 용 helper ──────────────────────────────
    def fwd_return(self, px, i, j, ticker, haircut=0.0):
        """factor_research.fwd_return 대체 시그니처. 향후 연구는 이것만 쓴다."""
        return self.wealth_index(i, j, ticker, haircut=haircut)


def anomaly_scan(eng, sample_every=1):
    """§25 — canonical 적용 후 이상치 자동탐지."""
    out = []
    for i in range(1, len(eng.dates), sample_every):
        a = eng.cap.get(eng.dates[i - 1], {})
        b = eng.cap.get(eng.dates[i], {})
        for t, cur in b.items():
            p = a.get(t)
            if not p or not (p.get("close") and cur.get("close")):
                continue
            r = eng.wealth_index(i - 1, i, t)
            if r is None:
                continue
            reasons = []
            if r > ANOMALY["monthlyReturnOver"]:
                reasons.append("MONTHLY_RETURN_OVER")
            if r < ANOMALY["monthlyReturnUnder"]:
                reasons.append("MONTHLY_RETURN_UNDER")
            if p.get("shares") and cur.get("shares"):
                sr = cur["shares"] / p["shares"]
                if sr > ANOMALY["shareRatioOver"]:
                    reasons.append("SHARE_RATIO_OVER")
                if sr < ANOMALY["shareRatioUnder"]:
                    reasons.append("SHARE_RATIO_UNDER")
            if not reasons:
                continue
            e = eng.events[i].get(t)
            out.append({"date": eng.dates[i], "ticker": t,
                        "canonicalMonthlyReturn": r,
                        "event": e["class"] if e else None,
                        "reasons": reasons,
                        "classification": ("TRUE_EVENT" if e else "UNRESOLVED")})
    return out


__all__ = ["CanonicalWealth", "classify", "anomaly_scan",
           "MECHANICAL", "RIGHTS", "DOWN_NP", "UP_NP", "NONE"]
