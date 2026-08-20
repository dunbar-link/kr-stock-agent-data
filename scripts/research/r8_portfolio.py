#!/usr/bin/env python3
"""R8 — BM 가치신호를 '사람이 실행 가능한 5천만원 규칙'으로 바꿀 수 있는지 검증하는 엔진.

WABABA-ROBUST-FACTOR-PORTFOLIO-R8

R7 은 신호(분위 forward return)를 봤다. R8 은 **실제 원 단위 장부**로 돌린다.
  - 정수주 매수(소수점 주식 없음), 잔여현금 보유
  - 수수료·매도세·상장폐지 처리는 R5/R6 과 동일 가정
  - PIT · survivorship-free · 미래 재무 미사용

★ 가장 중요한 공정성 장치 (§10)
   benchmark 에도 **같은 현금 투입 스케줄**을 적용한다.
   전략만 12개월 분할하고 benchmark 는 1일차 일시투입으로 비교하면
   "분할투입 효과"가 "BM alpha"로 둔갑한다. 그걸 구조적으로 막는다.

안전: 계산 전용. 네트워크 0 · 파일 write 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe  # noqa: E402
from factor_research import factor_values  # noqa: E402

COST_BPS = 15.0
SELL_TAX_BPS = 20.0


# ───────────────────────── 랭킹 캐시 (성능 핵심) ─────────────────────────
class RankCache:
    """(date, factor) 랭킹을 한 번만 계산한다. matrix 수백 조합이 이걸 공유한다."""

    def __init__(self, snapshots, names, *, market="COMBINED", min_market_cap=300,
                 size_bucket=None):
        self.sn, self.nm = snapshots, names
        self.market, self.mmc, self.size_bucket = market, min_market_cap, size_bucket
        self._uni, self._rank = {}, {}

    def universe(self, d):
        if d not in self._uni:
            u = investable_universe(self.sn[d], self.nm, market=self.market,
                                    min_market_cap=self.mmc)
            if self.size_bucket is not None and u:
                caps = sorted(((r["marketCap"] or 0), t) for t, r in u.items())
                n = len(caps)
                lo, hi = (n * self.size_bucket) // 3, (n * (self.size_bucket + 1)) // 3
                keep = {t for _, t in caps[lo:hi]}
                u = {t: r for t, r in u.items() if t in keep}
            self._uni[d] = u
        return self._uni[d]

    def ranked(self, d, factor):
        key = (d, factor)
        if key not in self._rank:
            u = self.universe(d)
            fv = factor_values(u)
            vals = fv.get(factor, {})
            if factor == "BM_EY":            # 사전 확정 단순 결합: 두 순위합
                bm, ey = fv.get("BM", {}), fv.get("EY", {})
                common = set(bm) & set(ey)
                rb = {t: i for i, t in enumerate(sorted(common, key=lambda x: -bm[x]))}
                re_ = {t: i for i, t in enumerate(sorted(common, key=lambda x: -ey[x]))}
                vals = {t: -(rb[t] + re_[t]) for t in common}
            items = sorted(((t, v) for t, v in vals.items() if t in u),
                           key=lambda kv: (-kv[1], kv[0]))
            self._rank[key] = [t for t, _ in items]
        return self._rank[key]


# ───────────────────────── 동일가중 universe 지수 ─────────────────────────
def ew_universe_index(cache, dates, *, delist_haircut=0.0):
    """benchmark 용 동일가중 시장 지수(월 리밸런싱, survivorship-free).
    전략과 같은 상장폐지 처리를 쓴다."""
    idx = [1.0]
    last = {}
    for i in range(len(dates)):
        snap = cache.sn[dates[i]]
        for t, r in snap.items():
            if r["close"]:
                last[t] = r["close"]
        if i == 0:
            continue
        prev_u = cache.universe(dates[i - 1])
        g, n = 0.0, 0
        for t, r0 in prev_u.items():
            p0 = r0["close"]
            if not p0:
                continue
            r1 = snap.get(t)
            p1 = r1["close"] if (r1 and r1["close"]) else (last.get(t) or p0) * (1.0 - delist_haircut)
            g += p1 / p0
            n += 1
        idx.append(idx[-1] * (g / n if n else 1.0))
    return idx


# ───────────────────────── 투입 스케줄 ─────────────────────────
def deployment_tranches(kind, capital, n_months):
    """초기자본을 언제 얼마씩 넣는지. 모든 방식의 총액은 동일하다."""
    if kind == "LUMP_SUM":
        return {0: capital}
    if kind.startswith("STAG"):
        k = int(kind.replace("STAG", "").replace("M", ""))
        k = max(1, min(k, n_months - 1))
        return {i: capital / k for i in range(k)}
    if kind == "MONTHLY_ROTATION":
        # 12개월에 걸쳐 넣되 이후에도 매월 회전(= 12분할과 동일 투입, 매수는 매월)
        k = 12
        return {i: capital / k for i in range(k)}
    if kind == "NONE":
        return {}
    raise ValueError(kind)


# ───────────────────────── 포트폴리오 시뮬레이터 ─────────────────────────
def run_portfolio(cache, dates, *, factor="BM", percentile=0.10, n_holdings=20,
                  hold_months=12, deployment="STAG12M", replacement="FIXED_MATURITY_REPLACE",
                  capital=50_000_000, monthly_amount=0, buy_every=1,
                  retention_pct=0.30, rebalance_months=12,
                  cost_bps=COST_BPS, sell_tax_bps=SELL_TAX_BPS, delist_haircut=0.0,
                  selection="FACTOR", seed=20260820):
    """실제 원 단위 장부. 정수주만 매수한다.

    selection: FACTOR(팩터 상위) / RANDOM(대조군) / EW_UNIVERSE(대조군)
    percentile: 선정 풀 = 상위 X%. 그 안에서 상위 n_holdings 종목을 담는다.
    """
    n = len(dates)
    if n < hold_months + 4:
        return None
    rng = random.Random(seed)
    tranches = deployment_tranches(deployment, capital, n)

    cash = 0.0
    contributed = 0.0
    lots = []           # {ticker, qty, buyPrice, maturityIdx}
    last_price = {}
    nav_hist, cash_hist, pos_hist = [], [], []
    trades = buys = sells = 0
    cost_paid = 0.0
    buy_notional = 0.0
    delist_events = 0
    buy_months = set()
    unique_ever = set()

    def px_of(t, snap, fallback):
        r = snap.get(t)
        if r and r["close"]:
            return r["close"], False
        return (last_price.get(t) or fallback) * (1.0 - delist_haircut), True

    for i, d in enumerate(dates):
        snap = cache.sn[d]
        for t, r in snap.items():
            if r["close"]:
                last_price[t] = r["close"]

        # ── 매도 ────────────────────────────────────────────────────────────
        to_sell, keep = [], []
        if replacement == "PERIODIC_REBALANCE":
            if i > 0 and i % max(1, rebalance_months) == 0:
                to_sell, lots = lots, []
            else:
                keep = lots
        else:
            matured = [l for l in lots if l["maturityIdx"] <= i]
            keep = [l for l in lots if l["maturityIdx"] > i]
            if replacement == "RANK_RETENTION" and matured:
                pool = cache.ranked(d, factor)
                keep_set = set(pool[: max(1, int(len(pool) * retention_pct))])
                for l in matured:
                    if l["ticker"] in keep_set:
                        l["maturityIdx"] = i + hold_months     # 순위 유지 → 연장
                        keep.append(l)
                    else:
                        to_sell.append(l)
            else:
                to_sell = matured
        lots = keep

        for l in to_sell:
            p, gone = px_of(l["ticker"], snap, l["buyPrice"])
            if gone:
                delist_events += 1
            gross = p * l["qty"]
            fee = gross * (cost_bps + sell_tax_bps) / 10000.0
            cash += gross - fee
            cost_paid += fee
            trades += 1
            sells += 1

        # ── 자금 유입 ───────────────────────────────────────────────────────
        inflow = tranches.get(i, 0.0) + (monthly_amount or 0.0)
        cash += inflow
        contributed += inflow

        # ── 매수 ────────────────────────────────────────────────────────────
        can_buy = (i % max(1, buy_every) == 0) and (i + hold_months < n)
        if replacement == "PERIODIC_REBALANCE":
            can_buy = (i == 0 or i % max(1, rebalance_months) == 0) and (i + 1 < n)
        if can_buy and cash > 0:
            uni = cache.universe(d)
            if selection == "FACTOR":
                # percentile = 선정 **풀의 폭**, n_holdings = 실제 담는 종목수.
                #   pool[:n_holdings] 로 자르면 폭이 무엇이든 항상 최상위 N 이 뽑혀
                #   percentile 축이 무의미해진다(P10/P20/P30 결과가 완전히 동일해짐 — 실측 확인).
                #   → 풀 전체에 **균등 간격**으로 배분해 '폭'이 실제로 반영되게 한다.
                pool = cache.ranked(d, factor)
                width = max(n_holdings, int(len(pool) * percentile))
                pool = pool[:width]
                if len(pool) <= n_holdings:
                    picks = pool
                else:
                    step = len(pool) / n_holdings
                    picks = [pool[min(len(pool) - 1, int(k * step))] for k in range(n_holdings)]
            elif selection == "RANDOM":
                tick = sorted(uni)
                picks = rng.sample(tick, min(n_holdings, len(tick)))
            else:                                   # EW_UNIVERSE 대조군
                tick = sorted(uni)
                picks = tick[:n_holdings]
            if picks:
                per = cash / len(picks)
                bought = 0
                for t in picks:
                    r = uni.get(t)
                    p = r["close"] if r else None
                    if not p or p <= 0:
                        continue
                    budget = per / (1.0 + cost_bps / 10000.0)
                    qty = int(budget // p)          # 정수주만
                    if qty <= 0:
                        continue
                    gross = p * qty
                    fee = gross * cost_bps / 10000.0
                    if gross + fee > cash:
                        continue
                    cash -= gross + fee
                    cost_paid += fee
                    buy_notional += gross
                    trades += 1
                    buys += 1
                    bought += 1
                    lots.append({"ticker": t, "qty": qty, "buyPrice": p,
                                 "maturityIdx": i + hold_months})
                    unique_ever.add(t)
                if bought:
                    buy_months.add(i)

        # ── 평가 ────────────────────────────────────────────────────────────
        hv = 0.0
        for l in lots:
            p, _ = px_of(l["ticker"], snap, l["buyPrice"])
            hv += p * l["qty"]
        nav = cash + hv
        nav_hist.append(nav)
        cash_hist.append(cash / nav if nav > 0 else 0.0)
        pos_hist.append(len({l["ticker"] for l in lots}))

    return {"dates": list(dates), "nav": nav_hist, "cashRatio": cash_hist,
            "positions": pos_hist, "contributed": contributed, "trades": trades,
            "buys": buys, "sells": sells, "costPaid": cost_paid,
            "buyNotional": buy_notional, "delistEvents": delist_events,
            "buyMonths": len(buy_months), "uniqueEver": len(unique_ever),
            "tranches": tranches, "monthlyAmount": monthly_amount}


def run_benchmark_same_schedule(idx, dates, *, tranches, monthly_amount=0):
    """★ 전략과 **동일한 현금 투입 스케줄**로 동일가중 시장지수를 사는 benchmark.
    분할투입 효과를 benchmark 도 똑같이 누리게 해서 BM alpha 만 남긴다."""
    units = 0.0
    contributed = 0.0
    nav = []
    for i in range(len(dates)):
        inflow = tranches.get(i, 0.0) + (monthly_amount or 0.0)
        if inflow:
            units += inflow / idx[i]
            contributed += inflow
        nav.append(units * idx[i])
    return {"nav": nav, "contributed": contributed}


# ───────────────────────── 성과 지표 ─────────────────────────
def _ord(iso):
    from datetime import date
    y, m, d = (int(x) for x in iso.split("-"))
    return date(y, m, d).toordinal()


def xirr(flows):
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
        mid = (lo + hi) / 2
        fm = npv(mid)
        if abs(fm) < 1e-6:
            return mid
        if flo * fm < 0:
            hi = mid
        else:
            lo, flo = mid, fm
    return (lo + hi) / 2


def evaluate(res, bench=None):
    nav, dates = res["nav"], res["dates"]
    if len(nav) < 13:
        return None
    years = (_ord(dates[-1]) - _ord(dates[0])) / 365.25
    tr = res["tranches"]
    ma = res.get("monthlyAmount") or 0

    # 유입 시계열 복원 → TWR / XIRR 분리
    con = []
    c = 0.0
    for i in range(len(nav)):
        c += tr.get(i, 0.0) + ma
        con.append(c)

    twr, pn, pc = 1.0, nav[0], con[0]
    for k in range(1, len(nav)):
        base = pn + (con[k] - pc)
        if base > 0:
            twr *= nav[k] / base
        pn, pc = nav[k], con[k]
    twr_cagr = twr ** (1 / years) - 1 if years > 0 and twr > 0 else None

    flows, pc = [], 0.0
    for i, d in enumerate(dates):
        add = con[i] - pc
        if add > 0:
            flows.append((_ord(d), -add))
        pc = con[i]
    flows.append((_ord(dates[-1]), nav[-1]))
    irr = xirr(flows)

    peak, mdd, uw, cur = nav[0], 0.0, 0, 0
    for v in nav:
        if v >= peak:
            peak, cur = v, 0
        else:
            cur += 1
            uw = max(uw, cur)
            mdd = min(mdd, v / peak - 1)

    rets = []
    for k in range(1, len(nav)):
        base = nav[k - 1] + (con[k] - con[k - 1])
        if base > 0:
            rets.append(nav[k] / base - 1)
    mean = sum(rets) / len(rets) if rets else 0
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1) if len(rets) > 1 else 0
    vol = math.sqrt(var) * math.sqrt(12)

    def roll(m):
        out = []
        for k in range(len(nav) - m):
            base = nav[k] + (con[k + m] - con[k])
            if base > 0:
                out.append((nav[k + m] / base) ** (12 / m) - 1)
        return out
    r1, r3 = roll(12), roll(36)

    out = {
        "years": round(years, 2), "terminalWealth": nav[-1], "contributed": con[-1],
        "twrCagr": twr_cagr, "irr": irr, "mdd": mdd, "maxUnderwaterMonths": uw,
        "vol": vol, "sharpe": (mean * 12 / vol) if vol > 0 else None,
        "worst1y": min(r1) if r1 else None, "worst3y": min(r3) if r3 else None,
        "roll1yNegShare": (sum(1 for x in r1 if x < 0) / len(r1)) if r1 else None,
        "trades": res["trades"], "buys": res["buys"], "sells": res["sells"],
        "buyMonths": res["buyMonths"], "costPaid": res["costPaid"],
        "costPctOfContrib": res["costPaid"] / con[-1] if con[-1] else None,
        "turnoverPerYear": (res["buyNotional"] / (sum(nav) / len(nav)) / years) if years > 0 else None,
        "avgCashRatio": sum(res["cashRatio"]) / len(res["cashRatio"]),
        "avgPositions": sum(res["positions"]) / len(res["positions"]),
        "maxPositions": max(res["positions"]), "uniqueEver": res["uniqueEver"],
        "delistEvents": res["delistEvents"],
    }
    if bench:
        bn = bench["nav"]
        bcon = []
        c = 0.0
        for i in range(len(bn)):
            c += tr.get(i, 0.0) + ma
            bcon.append(c)
        btwr, pn, pc = 1.0, bn[0], bcon[0]
        for k in range(1, len(bn)):
            base = pn + (bcon[k] - pc)
            if base > 0 and bn[k] > 0:
                btwr *= bn[k] / base
            pn, pc = bn[k], bcon[k]
        b_cagr = btwr ** (1 / years) - 1 if years > 0 and btwr > 0 else None
        out["benchCagr"] = b_cagr
        out["benchTerminal"] = bn[-1]
        out["excess"] = (twr_cagr - b_cagr) if (twr_cagr is not None and b_cagr is not None) else None
    return out
