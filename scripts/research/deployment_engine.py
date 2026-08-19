#!/usr/bin/env python3
"""R6 — 자본 투입방식 × 매수주기 × 종목수 × 보유기간 × 교체규칙 시뮬레이터.

WABABA-CAPITAL-DEPLOYMENT-AND-HOLDING-RULE-MATRIX-R6

왜 새 모듈인가
--------------
R5 엔진(backtest_engine.run_backtest)은 축이 3개(보유기간·종목수·contribution)뿐이고
**"5천만원을 시간축에 어떻게 나눠 넣는가"** 와 **"만기에 무엇으로 교체하는가"** 를 표현하지 못한다.
R5 결과의 재현성을 지키기 위해 그 함수를 고치지 않고, 여기서 축을 확장한다.
PIT·survivorship·상장폐지·비용·benchmark 처리는 R5에서 검증된 것을 **그대로 재사용**한다.

축
--
deployment   초기자본이 시장에 들어가는 방식
             LUMP_SUM        t0 에 전액
             STAGGERED_<K>   K개월에 걸쳐 균등 분할 투입 (K=2,3,6,12,25,50)
                             ※ LEGACY_50D 는 '50 거래일 분할'이다. 스냅샷이 월 단위라
                               일 단위 분할을 그대로 재현할 수 없어 **월 단위 근사**로만 비교한다(한계 명시).
buy_every    매수주기(개월). 1=매월, 3=분기
monthly_amount  매월 추가 적립금(0이면 순수 거치식)
n_stocks     1회 매수 시 담는 종목 수(= selection count). 동시 보유 종목수와 다르다.
hold_months  각 lot 보유기간(개월)
replacement  만기 도래분 처리
             MATURITY_REPLACE  만기 매도 → 그 자리에서 현재 상위 N 재매수(R5 기본)
             FIXED_MATURITY    만기 매도 → 현금으로 두고 다음 매수주기에 재투입
             RANK_RETENTION    만기여도 현재 순위가 유지구간(top N×retention_mult) 안이면 계속 보유
             PERIODIC_REBALANCE 만기 개념 없이 P개월마다 전량 재평가 후 상위 N 재구성

안전: 계산 전용. 네트워크 0 · 파일 write 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import _to_ord, rank_universe  # noqa: E402

# 거래일 ↔ 개월 대응(월 스냅샷이라 개월로 돌리되 거래일 등가를 함께 표기한다)
TRADING_DAYS_PER_MONTH = 21
HOLD_TD_LABEL = {2: "≈42TD(50D 근사)", 3: "63TD", 6: "126TD", 9: "189TD",
                 12: "252TD", 18: "378TD", 24: "504TD", 36: "756TD"}


class Lot:
    __slots__ = ("ticker", "qty", "buyPrice", "buyIdx", "maturityIdx")

    def __init__(self, ticker, qty, buy_price, buy_idx, maturity_idx):
        self.ticker = ticker
        self.qty = qty
        self.buyPrice = buy_price
        self.buyIdx = buy_idx
        self.maturityIdx = maturity_idx


def simulate(snapshots, names, dates, *,
             initial_capital=50_000_000,
             deployment="LUMP_SUM",
             stagger_months=1,
             monthly_amount=0,
             buy_every=1,
             n_stocks=20,
             hold_months=12,
             replacement="MATURITY_REPLACE",
             retention_mult=2.0,
             rebalance_months=12,
             market="COMBINED",
             min_market_cap=300,
             exclude_financial=True,
             exclude_utility=True,
             cost_bps=15.0,
             sell_tax_bps=20.0,
             delist_haircut=0.0,
             selection="MAGIC"):
    """월 단위 시뮬레이션. 같은 입력 → 같은 출력(난수 없음).

    selection : 종목 선정 규칙 — **대조군(control)** 용.
        MAGIC        Magic Formula 종합순위 상위 N (연구 대상)
        LARGEST_CAP  시가총액 상위 N (순위 정보를 쓰지 않는 소박한 규칙)
        TICKER       종목코드 오름차순 N (정보량 0에 가까운 대조군)
      같은 구조(투입·보유·교체·비용)에서 MAGIC 과 비교하면
      "성과 부진이 구조 때문인가, 순위 때문인가"를 분리할 수 있다.
    """
    if len(dates) < hold_months + 3:
        return None

    cash = 0.0
    contributed = 0.0
    lots: list[Lot] = []
    last_price: dict[str, float] = {}
    nav_series = []
    trades = 0
    buy_notional = 0.0
    cost_paid = 0.0
    delist_events = 0
    idle_ratios = []
    pos_counts = []
    unique_ever = set()
    ranked_cache: dict[int, list] = {}

    # 초기자본 투입 스케줄
    if deployment == "LUMP_SUM":
        tranches = [initial_capital]
    elif deployment.startswith("STAGGERED"):
        k = max(1, int(stagger_months))
        tranches = [initial_capital / k] * k
    else:                                   # 순수 적립식 — 초기자본 없음
        tranches = []

    def ranked_at(i):
        if i not in ranked_cache:
            base_list = rank_universe(snapshots[dates[i]], names, market=market,
                                      min_market_cap=min_market_cap,
                                      exclude_financial=exclude_financial,
                                      exclude_utility=exclude_utility)
            # 대조군은 **같은 투자가능 universe** 에서 정렬 기준만 바꾼다(universe 를 넓히지 않는다).
            if selection == "LARGEST_CAP":
                base_list = sorted(base_list, key=lambda s: (-(s["marketCap"] or 0), s["ticker"]))
            elif selection == "TICKER":
                base_list = sorted(base_list, key=lambda s: s["ticker"])
            ranked_cache[i] = base_list
        return ranked_cache[i]

    def price_of(t, snap, fallback):
        r = snap.get(t)
        if r and r["close"]:
            return r["close"], False
        return (last_price.get(t) or fallback) * (1.0 - delist_haircut), True

    for i, d in enumerate(dates):
        snap = snapshots[d]
        for t, r in snap.items():
            if r["close"]:
                last_price[t] = r["close"]

        # ── 1) 매도 ──────────────────────────────────────────────────────────
        keep: list[Lot] = []
        to_sell: list[Lot] = []
        if replacement == "PERIODIC_REBALANCE":
            if i > 0 and i % max(1, rebalance_months) == 0:
                to_sell = lots
                lots = []
            else:
                keep = lots
        else:
            matured = [l for l in lots if l.maturityIdx <= i]
            keep = [l for l in lots if l.maturityIdx > i]
            if replacement == "RANK_RETENTION" and matured:
                top = ranked_at(i)[: int(n_stocks * retention_mult)]
                keep_set = {x["ticker"] for x in top}
                for l in matured:
                    if l.ticker in keep_set:
                        l.maturityIdx = i + hold_months   # 순위 유지 → 보유 연장
                        keep.append(l)
                    else:
                        to_sell.append(l)
            else:
                to_sell = matured
            lots = keep
        if replacement == "PERIODIC_REBALANCE" and keep:
            lots = keep

        for l in to_sell:
            px, gone = price_of(l.ticker, snap, l.buyPrice)
            if gone:
                delist_events += 1
            gross = px * l.qty
            fee = gross * (cost_bps + sell_tax_bps) / 10000.0
            cash += gross - fee
            cost_paid += fee
            trades += 1

        # ── 2) 자금 유입 ─────────────────────────────────────────────────────
        inflow = 0.0
        if i < len(tranches):
            inflow += tranches[i]
        if monthly_amount:
            inflow += monthly_amount
        cash += inflow
        contributed += inflow

        # ── 3) 매수 ──────────────────────────────────────────────────────────
        #   마지막 hold 구간에서는 신규 매수를 멈춘다(만기 전 강제청산 왜곡 방지).
        can_buy = (i % max(1, buy_every) == 0) and (i + hold_months < len(dates))
        if replacement == "PERIODIC_REBALANCE":
            can_buy = (i == 0) or (i % max(1, rebalance_months) == 0)
            can_buy = can_buy and (i + 1 < len(dates))
        if can_buy and cash > 0:
            picks = ranked_at(i)[:n_stocks]
            if picks:
                per = cash / len(picks)
                for s in picks:
                    px = s["close"]
                    budget = per / (1.0 + cost_bps / 10000.0)
                    qty = int(budget // px)
                    if qty <= 0:
                        continue
                    gross = px * qty
                    fee = gross * cost_bps / 10000.0
                    if gross + fee > cash:
                        continue
                    cash -= gross + fee
                    buy_notional += gross
                    cost_paid += fee
                    trades += 1
                    lots.append(Lot(s["ticker"], qty, px, i, i + hold_months))
                    unique_ever.add(s["ticker"])

        # ── 4) 평가 ──────────────────────────────────────────────────────────
        hv = 0.0
        for l in lots:
            px, _ = price_of(l.ticker, snap, l.buyPrice)
            hv += px * l.qty
        nav = cash + hv
        nav_series.append({"date": d, "nav": nav, "cash": cash, "holdings": hv,
                           "contributed": contributed, "lots": len(lots),
                           "tickers": len({l.ticker for l in lots})})
        if nav > 0:
            idle_ratios.append(cash / nav)
        pos_counts.append(len({l.ticker for l in lots}))

    return {
        "navSeries": nav_series, "trades": trades, "buyNotional": buy_notional,
        "costPaid": cost_paid, "delistEvents": delist_events,
        "idleCashRatioAvg": (sum(idle_ratios) / len(idle_ratios)) if idle_ratios else None,
        "idleCashRatioMax": max(idle_ratios) if idle_ratios else None,
        "avgConcurrentPositions": (sum(pos_counts) / len(pos_counts)) if pos_counts else 0,
        "maxConcurrentPositions": max(pos_counts) if pos_counts else 0,
        "uniqueTickersEver": len(unique_ever),
        "finalNav": nav_series[-1]["nav"] if nav_series else 0.0,
        "totalContributed": contributed,
    }


def _xirr(flows):
    if not flows:
        return None
    t0 = flows[0][0]

    def npv(r):
        return sum(c / ((1.0 + r) ** ((t - t0) / 365.25)) for t, c in flows)

    lo, hi = -0.95, 5.0
    flo = npv(lo)
    if flo * npv(hi) > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2.0
        fm = npv(mid)
        if abs(fm) < 1e-6:
            return mid
        if flo * fm < 0:
            hi = mid
        else:
            lo, flo = mid, fm
    return (lo + hi) / 2.0


def evaluate(res):
    """현금흐름이 다른 전략을 공정하게 비교하기 위해 TWR / IRR / terminal wealth 를 분리한다."""
    ns = res["navSeries"]
    if len(ns) < 13:
        return None
    navs = [x["nav"] for x in ns]
    con = [x["contributed"] for x in ns]
    dates = [x["date"] for x in ns]
    years = (_to_ord(dates[-1]) - _to_ord(dates[0])) / 365.25

    # 시간가중(TWR) — 유입 효과 제거
    twr, pn, pc = 1.0, navs[0], con[0]
    for k in range(1, len(navs)):
        base = pn + (con[k] - pc)
        if base > 0:
            twr *= navs[k] / base
        pn, pc = navs[k], con[k]
    twr_cagr = twr ** (1.0 / years) - 1.0 if years > 0 and twr > 0 else None

    # 금액가중(IRR)
    flows, pc = [], 0.0
    for x in ns:
        add = x["contributed"] - pc
        if add > 0:
            flows.append((_to_ord(x["date"]), -add))
        pc = x["contributed"]
    flows.append((_to_ord(dates[-1]), navs[-1]))
    irr = _xirr(flows)

    peak, mdd, uw, cur = navs[0], 0.0, 0, 0
    for v in navs:
        if v >= peak:
            peak, cur = v, 0
        else:
            cur += 1
            uw = max(uw, cur)
            mdd = min(mdd, v / peak - 1.0)

    rets = []
    for k in range(1, len(navs)):
        base = navs[k - 1] + (con[k] - con[k - 1])
        if base > 0:
            rets.append(navs[k] / base - 1.0)
    mean = sum(rets) / len(rets) if rets else 0.0
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1) if len(rets) > 1 else 0.0
    vol = math.sqrt(var) * math.sqrt(12)
    downs = [r for r in rets if r < 0]
    dvol = math.sqrt(sum(r * r for r in downs) / len(downs)) * math.sqrt(12) if downs else 0.0

    def rolling(m):
        out = []
        for k in range(len(navs) - m):
            base = navs[k] + (con[k + m] - con[k])
            if base > 0:
                out.append((navs[k + m] / base) ** (12.0 / m) - 1.0)
        return out

    def stat(a):
        if not a:
            return None
        s = sorted(a)
        return {"n": len(a), "min": s[0], "p10": s[int(len(s) * 0.1)], "median": s[len(s) // 2],
                "p90": s[int(len(s) * 0.9)], "max": s[-1],
                "negShare": sum(1 for x in a if x < 0) / len(a)}

    return {
        "years": years, "start": dates[0], "end": dates[-1],
        "terminalWealth": navs[-1], "totalContributed": con[-1],
        "totalReturnOnContrib": (navs[-1] / con[-1] - 1.0) if con[-1] else None,
        "twrCagr": twr_cagr, "irr": irr, "mdd": mdd, "maxUnderwaterMonths": uw,
        "vol": vol, "sharpe": (mean * 12 / vol) if vol > 0 else None,
        "sortino": (mean * 12 / dvol) if dvol > 0 else None,
        "rolling1y": stat(rolling(12)), "rolling3y": stat(rolling(36)), "rolling5y": stat(rolling(60)),
        "trades": res["trades"], "costPaid": res["costPaid"],
        "costPctOfContrib": (res["costPaid"] / con[-1]) if con[-1] else None,
        "turnoverPerYear": (res["buyNotional"] / (sum(navs) / len(navs)) / years) if years > 0 else None,
        "delistEvents": res["delistEvents"],
        "idleCashRatioAvg": res["idleCashRatioAvg"], "idleCashRatioMax": res["idleCashRatioMax"],
        "avgConcurrentPositions": res["avgConcurrentPositions"],
        "maxConcurrentPositions": res["maxConcurrentPositions"],
        "uniqueTickersEver": res["uniqueTickersEver"],
    }
