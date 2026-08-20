#!/usr/bin/env python3
"""R9 엔진 — frozen candidate 를 공격하기 위한 확장 시뮬레이터.

WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9

R8 의 `r8_portfolio.run_portfolio` 는 손대지 않는다(재현성 보존).
대신 같은 회계를 하면서 **공격에 필요한 계측기**를 추가한 별도 엔진을 둔다.

  추가 1) 슬리피지 — 매수 p*(1+s) / 매도 p*(1-s)
  추가 2) lot 원장 — 종목·시장·시총·매수가·매도가·수량·손익(원) 전부 기록
  추가 3) 월별 TWR 수익률 시계열(전략·benchmark) — leave-one-era-out / 구간분해용
  추가 4) 선정 로그 — 매수 시점의 picks 와 그 속성(PBR·시총·시장) 기록
  추가 5) 제외집합(banned) — 기여 상위종목 제거 재시뮬레이션용
  추가 6) matched control 선정 — SIZE_MATCHED / MARKET_MATCHED / RANDOM
  추가 7) 유동성 하한 / stale-price 제외 universe

★ 재현 게이트: base 설정에서 R8 결과와 **완전히 같은 숫자**가 나와야 한다.
  다르면 R9 는 R8 후보가 아닌 다른 것을 검증하게 되므로 즉시 중단(§1).

frozen candidate 의 파라미터는 이 파일에서 절대 튜닝하지 않는다(§18).
안전: 계산 전용. 네트워크 0 · 파일 write 0(호출자가 저장) · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe  # noqa: E402
from r8_portfolio import RankCache, deployment_tranches, ew_universe_index  # noqa: E402

COST_BPS = 15.0
SELL_TAX_BPS = 20.0


# ───────────────────── 유동성 제약이 걸린 RankCache ─────────────────────
class LiquidityRankCache(RankCache):
    """min_market_cap 상향 + '직전 2개월 종가 무변동'(거래부진 proxy) 제외.

    스냅샷에 거래량·거래대금 컬럼이 없다(실측: ticker,market,close,marketCap,shares,
    PER,PBR,EPS,BPS,DIV,DPS). 따라서 거래부진은 **종가 무변동**으로만 근사한다.
    이 한계는 보고서에 그대로 적는다 — 유동성 검증을 했다고 과장하지 않는다.
    """

    def __init__(self, snapshots, names, dates, *, min_market_cap=300,
                 exclude_stale=False, **kw):
        super().__init__(snapshots, names, min_market_cap=min_market_cap, **kw)
        self.dates = list(dates)
        self.exclude_stale = exclude_stale
        self._pos = {d: i for i, d in enumerate(self.dates)}

    def universe(self, d):
        if d in self._uni:
            return self._uni[d]
        u = super().universe(d)
        if self.exclude_stale:
            i = self._pos.get(d)
            if i is not None and i >= 2:
                p1, p2 = self.sn[self.dates[i - 1]], self.sn[self.dates[i - 2]]
                drop = set()
                for t, r in u.items():
                    a = p1.get(t, {}).get("close")
                    b = p2.get(t, {}).get("close")
                    if a and b and r["close"] == a == b:
                        drop.add(t)
                u = {t: r for t, r in u.items() if t not in drop}
            self._uni[d] = u
        return u


# ───────────────────────────── 시뮬레이터 ─────────────────────────────
def simulate(cache, dates, *, factor="BM", percentile=0.20, n_holdings=40,
             hold_months=24, deployment="STAG12M",
             replacement="FIXED_MATURITY_REPLACE", capital=50_000_000,
             buy_every=24, cost_bps=COST_BPS, sell_tax_bps=SELL_TAX_BPS,
             slippage_bps=0.0, delist_haircut=0.0, selection="FACTOR",
             seed=20260820, banned=None, names=None):
    """frozen candidate 회계. 정수주만 · 잔여현금 보유 · 월 스냅샷 종가 체결."""
    n = len(dates)
    if n < hold_months + 4:
        return None
    rng = random.Random(seed)
    banned = set(banned or ())
    tranches = deployment_tranches(deployment, capital, n)
    slip = slippage_bps / 10000.0

    cash = 0.0
    lots = []
    last_price = {}
    nav_hist, cash_hist, pos_hist, inflow_hist = [], [], [], []
    trades = buys = sells = 0
    cost_paid = slip_paid = 0.0
    buy_notional = 0.0
    delist_events = 0
    delist_loss = 0.0
    buy_months = set()
    unique_ever = set()
    ledger = []          # 청산된 lot
    pick_log = []        # 매수 시점 선정 기록

    def px_of(t, snap, fallback):
        r = snap.get(t)
        if r and r["close"]:
            return r["close"], False
        return (last_price.get(t) or fallback) * (1.0 - delist_haircut), True

    def market_of(t, snap):
        r = snap.get(t)
        return r["market"] if r else last_market.get(t, "?")

    last_market = {}

    for i, d in enumerate(dates):
        snap = cache.sn[d]
        for t, r in snap.items():
            if r["close"]:
                last_price[t] = r["close"]
            last_market[t] = r["market"]

        # ── 매도 ───────────────────────────────────────────────────────────
        matured = [l for l in lots if l["maturityIdx"] <= i]
        lots = [l for l in lots if l["maturityIdx"] > i]
        for l in matured:
            p, gone = px_of(l["ticker"], snap, l["buyPrice"])
            if gone:
                delist_events += 1
            exec_p = p * (1.0 - slip) if not gone else p
            gross = exec_p * l["qty"]
            fee = gross * (cost_bps + sell_tax_bps) / 10000.0
            slip_paid += (p - exec_p) * l["qty"]
            cash += gross - fee
            cost_paid += fee
            trades += 1
            sells += 1
            cost_basis = l["execPrice"] * l["qty"]
            pnl = gross - fee - cost_basis
            if gone:
                delist_loss += cost_basis - (gross - fee)
            ledger.append({"ticker": l["ticker"], "market": l["market"],
                           "buyIdx": l["buyIdx"], "sellIdx": i,
                           "qty": l["qty"], "buyPrice": l["buyPrice"],
                           "execPrice": l["execPrice"], "sellPrice": p,
                           "marketCapAtBuy": l["marketCapAtBuy"],
                           "pbrAtBuy": l["pbrAtBuy"], "delisted": gone,
                           "costBasis": cost_basis, "proceeds": gross - fee,
                           "pnl": pnl, "ret": (gross - fee) / cost_basis - 1 if cost_basis else None})

        # ── 자금 유입 ──────────────────────────────────────────────────────
        inflow = tranches.get(i, 0.0)
        cash += inflow
        inflow_hist.append(inflow)

        # ── 매수 ───────────────────────────────────────────────────────────
        can_buy = (i % max(1, buy_every) == 0) and (i + hold_months < n)
        if can_buy and cash > 0:
            uni = cache.universe(d)
            picks = _pick(cache, d, uni, factor=factor, percentile=percentile,
                          n_holdings=n_holdings, selection=selection, rng=rng,
                          banned=banned)
            if picks:
                per = cash / len(picks)
                bought = 0
                rec = []
                for t in picks:
                    r = uni.get(t)
                    p = r["close"] if r else None
                    if not p or p <= 0:
                        continue
                    exec_p = p * (1.0 + slip)
                    budget = per / (1.0 + cost_bps / 10000.0)
                    qty = int(budget // exec_p)
                    if qty <= 0:
                        continue
                    gross = exec_p * qty
                    fee = gross * cost_bps / 10000.0
                    if gross + fee > cash:
                        continue
                    cash -= gross + fee
                    cost_paid += fee
                    slip_paid += (exec_p - p) * qty
                    buy_notional += gross
                    trades += 1
                    buys += 1
                    bought += 1
                    lots.append({"ticker": t, "qty": qty, "buyPrice": p,
                                 "execPrice": exec_p, "buyIdx": i,
                                 "market": r["market"],
                                 "marketCapAtBuy": r["marketCap"],
                                 "pbrAtBuy": r["PBR"],
                                 "maturityIdx": i + hold_months})
                    unique_ever.add(t)
                    rec.append({"ticker": t, "market": r["market"], "pbr": r["PBR"],
                                "marketCap": r["marketCap"], "close": p, "qty": qty})
                if bought:
                    buy_months.add(i)
                pick_log.append({"idx": i, "date": d, "universeSize": len(uni),
                                 "requested": len(picks), "bought": bought,
                                 "picks": rec})

        # ── 평가 ───────────────────────────────────────────────────────────
        hv = 0.0
        for l in lots:
            p, _ = px_of(l["ticker"], snap, l["buyPrice"])
            hv += p * l["qty"]
        nav = cash + hv
        nav_hist.append(nav)
        cash_hist.append(cash / nav if nav > 0 else 0.0)
        pos_hist.append(len({l["ticker"] for l in lots}))

    # 만기 미도달 잔여 lot 도 원장에 남긴다(미실현) — 기여도 분석에 필요
    final_snap = cache.sn[dates[-1]]
    open_lots = []
    for l in lots:
        p, gone = px_of(l["ticker"], final_snap, l["buyPrice"])
        gross = p * l["qty"]
        cost_basis = l["execPrice"] * l["qty"]
        open_lots.append({"ticker": l["ticker"], "market": l["market"],
                          "buyIdx": l["buyIdx"], "sellIdx": None, "qty": l["qty"],
                          "buyPrice": l["buyPrice"], "execPrice": l["execPrice"],
                          "sellPrice": p, "marketCapAtBuy": l["marketCapAtBuy"],
                          "pbrAtBuy": l["pbrAtBuy"], "delisted": gone,
                          "costBasis": cost_basis, "proceeds": gross,
                          "pnl": gross - cost_basis,
                          "ret": gross / cost_basis - 1 if cost_basis else None})

    return {"dates": list(dates), "nav": nav_hist, "cashRatio": cash_hist,
            "positions": pos_hist, "inflow": inflow_hist,
            "contributed": sum(inflow_hist), "trades": trades, "buys": buys,
            "sells": sells, "costPaid": cost_paid, "slipPaid": slip_paid,
            "buyNotional": buy_notional, "delistEvents": delist_events,
            "delistLoss": delist_loss, "buyMonths": len(buy_months),
            "uniqueEver": len(unique_ever), "tranches": tranches,
            "ledger": ledger, "openLots": open_lots, "pickLog": pick_log}


def _pick(cache, d, uni, *, factor, percentile, n_holdings, selection, rng, banned):
    """선정. FACTOR 는 R8 과 **완전히 동일한 균등간격 배분** 규칙을 쓴다."""
    pool = [t for t in cache.ranked(d, factor) if t not in banned]
    width = max(n_holdings, int(len(cache.ranked(d, factor)) * percentile))
    pool = pool[:width]
    if len(pool) <= n_holdings:
        fac = pool
    else:
        step = len(pool) / n_holdings
        fac = [pool[min(len(pool) - 1, int(k * step))] for k in range(n_holdings)]

    if selection == "FACTOR":
        return fac
    avail = sorted(t for t in uni if t not in banned)
    if not avail:
        return []
    if selection == "RANDOM":
        return rng.sample(avail, min(n_holdings, len(avail)))
    if selection == "SIZE_MATCHED":
        caps = sorted(((uni[t]["marketCap"] or 0), t) for t in avail)
        order = [t for _, t in caps]
        buckets = {}
        nn = len(order)
        for j, t in enumerate(order):
            buckets.setdefault(min(9, (j * 10) // nn), []).append(t)
        pos = {t: min(9, (j * 10) // nn) for j, t in enumerate(order)}
        out, used = [], set()
        for t in fac:
            b = pos.get(t)
            cand = [x for x in buckets.get(b, avail) if x not in used] or \
                   [x for x in avail if x not in used]
            if not cand:
                break
            c = rng.choice(cand)
            used.add(c)
            out.append(c)
        return out
    if selection == "MARKET_MATCHED":
        by = {}
        for t in avail:
            by.setdefault(uni[t]["market"], []).append(t)
        out, used = [], set()
        for t in fac:
            m = uni[t]["market"] if t in uni else None
            cand = [x for x in by.get(m, avail) if x not in used] or \
                   [x for x in avail if x not in used]
            if not cand:
                break
            c = rng.choice(cand)
            used.add(c)
            out.append(c)
        return out
    if selection == "SIZE_MARKET_MATCHED":
        # R10 §11 — 시장(거래소) **과** 시총 5분위를 동시에 맞춘 통제군.
        #   size 만 맞추거나 market 만 맞추면 남은 축으로 노출이 새어나간다.
        #   두 축을 동시에 고정했을 때도 BM 이 이기는지 보는 가장 엄격한 통제군이다.
        buckets, pos = {}, {}
        for m in {uni[t]["market"] for t in avail}:
            grp = sorted(((uni[t]["marketCap"] or 0), t) for t in avail
                         if uni[t]["market"] == m)
            nn = len(grp)
            for j, (_, t) in enumerate(grp):
                b = (m, min(4, (j * 5) // nn) if nn else 0)
                buckets.setdefault(b, []).append(t)
                pos[t] = b
        out, used = [], set()
        for t in fac:
            b = pos.get(t)
            cand = [x for x in buckets.get(b, avail) if x not in used] or \
                   [x for x in avail if x not in used]
            if not cand:
                break
            c = rng.choice(cand)
            used.add(c)
            out.append(c)
        return out
    raise ValueError(selection)


def bench_same_schedule(idx, dates, *, tranches):
    """전략과 동일한 현금 투입 스케줄로 동일가중 시장지수를 사는 benchmark."""
    units = 0.0
    nav = []
    for i in range(len(dates)):
        inflow = tranches.get(i, 0.0)
        if inflow:
            units += inflow / idx[i]
        nav.append(units * idx[i])
    return {"nav": nav}


# ───────────────────────────── 성과 지표 ─────────────────────────────
def _ord(iso):
    from datetime import date
    y, m, dd = (int(x) for x in iso.split("-"))
    return date(y, m, dd).toordinal()


def monthly_twr(nav, inflow):
    """월별 TWR 수익률. inflow[k] 는 k 시점에 들어온 현금(그 달 NAV 에 이미 포함)."""
    out = []
    for k in range(1, len(nav)):
        base = nav[k - 1] + inflow[k]
        out.append(nav[k] / base - 1 if base > 0 else 0.0)
    return out


def chain_stats(rets, months_per_year=12):
    """수익률 시계열 → CAGR / MDD / 최장수중. leave-one-era-out 에 쓴다."""
    if not rets:
        return None
    nav, peak, mdd, uw, cur = 1.0, 1.0, 0.0, 0, 0
    for r in rets:
        nav *= (1.0 + r)
        if nav >= peak:
            peak, cur = nav, 0
        else:
            cur += 1
            uw = max(uw, cur)
            mdd = min(mdd, nav / peak - 1)
    years = len(rets) / months_per_year
    cagr = nav ** (1 / years) - 1 if years > 0 and nav > 0 else None
    return {"cagr": cagr, "mdd": mdd, "maxUnderwaterMonths": uw,
            "months": len(rets), "years": years, "growth": nav}


def evaluate(res, bench=None):
    """R8 `r8_portfolio.evaluate` 와 동일 정의(재현 게이트를 통과해야 한다)."""
    nav, dates, inflow = res["nav"], res["dates"], res["inflow"]
    if len(nav) < 13:
        return None
    years = (_ord(dates[-1]) - _ord(dates[0])) / 365.25
    con, c = [], 0.0
    for x in inflow:
        c += x
        con.append(c)

    twr, pn, pc = 1.0, nav[0], con[0]
    for k in range(1, len(nav)):
        base = pn + (con[k] - pc)
        if base > 0:
            twr *= nav[k] / base
        pn, pc = nav[k], con[k]
    twr_cagr = twr ** (1 / years) - 1 if years > 0 and twr > 0 else None

    peak, mdd, uw, cur = nav[0], 0.0, 0, 0
    for v in nav:
        if v >= peak:
            peak, cur = v, 0
        else:
            cur += 1
            uw = max(uw, cur)
            mdd = min(mdd, v / peak - 1)

    rets = monthly_twr(nav, inflow)
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
        "twrCagr": twr_cagr, "mdd": mdd, "maxUnderwaterMonths": uw, "vol": vol,
        "worst1y": min(r1) if r1 else None, "worst3y": min(r3) if r3 else None,
        "best1y": max(r1) if r1 else None,
        "roll1yNegShare": (sum(1 for x in r1 if x < 0) / len(r1)) if r1 else None,
        "trades": res["trades"], "buys": res["buys"], "sells": res["sells"],
        "tradesPerYear": res["trades"] / years if years else None,
        "costPaid": res["costPaid"], "slipPaid": res["slipPaid"],
        "costPctOfContrib": res["costPaid"] / con[-1] if con[-1] else None,
        "avgCashRatio": sum(res["cashRatio"]) / len(res["cashRatio"]),
        "avgPositions": sum(res["positions"]) / len(res["positions"]),
        "maxPositions": max(res["positions"]), "uniqueEver": res["uniqueEver"],
        "delistEvents": res["delistEvents"], "delistLoss": res["delistLoss"],
    }
    if bench:
        bn = bench["nav"]
        btwr, pn, pc = 1.0, bn[0], con[0]
        for k in range(1, len(bn)):
            base = pn + (con[k] - pc)
            if base > 0 and bn[k] > 0:
                btwr *= bn[k] / base
            pn, pc = bn[k], con[k]
        b_cagr = btwr ** (1 / years) - 1 if years > 0 and btwr > 0 else None
        out["benchCagr"] = b_cagr
        out["benchTerminal"] = bn[-1]
        out["excess"] = ((twr_cagr - b_cagr)
                         if (twr_cagr is not None and b_cagr is not None) else None)
    return out


def run_case(cache, dates, idx, **kw):
    """simulate + benchmark + evaluate 한 번에."""
    res = simulate(cache, dates, **kw)
    if not res:
        return None, None
    b = bench_same_schedule(idx, dates, tranches=res["tranches"])
    ev = evaluate(res, b)
    return res, ev


def slice_index(idx, start):
    """부분구간 EW 지수 = 전체 지수의 재정규화(연쇄지수라 정확히 동일)."""
    base = idx[start]
    return [v / base for v in idx[start:]]


__all__ = ["LiquidityRankCache", "simulate", "bench_same_schedule", "evaluate",
           "run_case", "monthly_twr", "chain_stats", "slice_index",
           "RankCache", "ew_universe_index"]
