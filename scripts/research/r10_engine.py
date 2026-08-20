#!/usr/bin/env python3
"""R10 Phase C — 사전규격 준수 엔진 (SPEC_CONFORMANCE_FIX).

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

Phase B 판정 = CASE_1 / SPEC_MONTHLY_STAGGER_CONFIRMED.
R8·R9 코드는 초기자본의 11/12 를 최대 24개월간 현금으로 두었다 — 사전규격 위반이다.
이 파일은 그 **구현 오류만** 고친다.

절대 변경하지 않은 것 (지시문 §5)
  BM = 1/PBR · percentile 0.20 · holdings 40 · hold 24 · PIT universe ·
  ranking logic · investability filter · benchmark 정의 · 거래비용 가정 ·
  상장폐지 가정 · 데이터 출처 · 교체(rebalance) 의미 · 판정 gate threshold
  → benchmark·평가지표는 r9_engine 것을 그대로 import 해서 쓴다(재작성 금지).

바꾼 것 (딱 하나)
  초기자본 12개월 분할이 **실제로 시장에 투입**되도록 매수 로직만 최소 수정.

구조 (두 사전제약을 동시에 만족하는 유일한 해석 — r10_forensics.conformance_reading)
  제약1 deployment : 5천만원이 12개월에 걸쳐 실제로 시장에 들어간다   (증거 E1·E2)
  제약2 cohort     : 동시 보유종목수 = 40, N×hold lot 적층 금지        (증거 E4)
  → 하나의 40종목 코호트를 12개월에 걸쳐 쌓아 올린다.
     선정월(i % 24 == 0)  : BM 상위 20% 풀에서 40종목 선정 + 그 달 tranche 투입
     tranche 월 i=1..11   : 재선정 없이 같은 40종목에 1/12 추가 매수
     만기 = 선정월 + 24   : 전량 매도 → 재선정 → 매도대금 전액 재투자

만기 단위는 **코호트**다. '만기 후 전량 교체'가 한 시점의 전량 매도를 요구하기 때문이다.
민감도로 tranche 별 개별 만기(PER_TRANCHE) 도 계산해 이 해석이 결과를 좌우하지 않음을 보인다.

결과를 좋게 만들기 위한 cash drag 조정 금지 · 새 parameter 탐색 금지(§12).
안전: 계산 전용 · 네트워크 0 · 파일 write 0(호출자가 저장) · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r8_portfolio import deployment_tranches  # noqa: E402
from r9_engine import _pick  # noqa: E402  선정 로직 재사용 — 랭킹 규칙 변경 0
from r9_engine import (  # noqa: E402  benchmark·지표는 R9 것을 그대로 쓴다
    COST_BPS, SELL_TAX_BPS, LiquidityRankCache, bench_same_schedule, chain_stats,
    evaluate, monthly_twr, slice_index,
)

MATURITY_COHORT = "COHORT"
MATURITY_PER_TRANCHE = "PER_TRANCHE"


def simulate(cache, dates, *, factor="BM", percentile=0.20, n_holdings=40,
             hold_months=24, deployment="STAG12M",
             replacement="FIXED_MATURITY_REPLACE", capital=50_000_000,
             buy_every=24, cost_bps=COST_BPS, sell_tax_bps=SELL_TAX_BPS,
             slippage_bps=0.0, delist_haircut=0.0, selection="FACTOR",
             seed=20260820, banned=None, names=None,
             maturity_unit=MATURITY_COHORT, pick_fn=None, fractional_shares=False):
    """사전규격 준수 회계. 정수주만 · 월 스냅샷 종가 체결 · 레버리지 0 · 음수현금 0."""
    n = len(dates)
    if n < hold_months + 4:
        return None
    rng = random.Random(seed)
    banned = set(banned or ())
    tranches = deployment_tranches(deployment, capital, n)
    slip = slippage_bps / 10000.0

    cash = 0.0
    lots = []
    last_price, last_market = {}, {}
    nav_hist, cash_hist, pos_hist, inflow_hist = [], [], [], []
    trades = buys = sells = 0
    cost_paid = slip_paid = 0.0
    buy_notional = 0.0
    delist_events = 0
    delist_loss = 0.0
    buy_months = set()
    unique_ever = set()
    ledger, pick_log, capital_ledger = [], [], []

    cohort_tickers = []          # 현재 코호트 구성 (재선정 없이 top-up 대상)
    cohort_id = -1
    cohort_maturity = None
    # ★ 실제 주문 건수 = (월, 종목) 단위. lot 단위 trades 는 12개 tranche lot 을 파는 것을
    #   12건으로 세지만, 사람은 한 종목 전량을 **한 번** 매도한다. 실행부담(§G10)은
    #   주문 건수로 재는 것이 맞으므로 둘을 함께 기록하고 보고서에 병기한다.
    buy_orders, sell_orders = set(), set()

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
            last_market[t] = r["market"]
        cash_begin = cash

        # ── 매도 (만기 도래분 전량) ─────────────────────────────────────────
        matured = [l for l in lots if l["maturityIdx"] <= i]
        lots = [l for l in lots if l["maturityIdx"] > i]
        sale_proceeds = 0.0
        for l in matured:
            p, gone = px_of(l["ticker"], snap, l["buyPrice"])
            if gone:
                delist_events += 1
            exec_p = p * (1.0 - slip) if not gone else p
            gross = exec_p * l["qty"]
            fee = gross * (cost_bps + sell_tax_bps) / 10000.0
            slip_paid += (p - exec_p) * l["qty"]
            cash += gross - fee
            sale_proceeds += gross - fee
            cost_paid += fee
            trades += 1
            sells += 1
            sell_orders.add((i, l["ticker"]))
            cost_basis = l["execPrice"] * l["qty"]
            pnl = gross - fee - cost_basis
            if gone:
                delist_loss += cost_basis - (gross - fee)
            ledger.append({"ticker": l["ticker"], "market": l["market"],
                           "cohort": l["cohort"], "buyIdx": l["buyIdx"], "sellIdx": i,
                           "qty": l["qty"], "buyPrice": l["buyPrice"],
                           "execPrice": l["execPrice"], "sellPrice": p,
                           "marketCapAtBuy": l["marketCapAtBuy"],
                           "pbrAtBuy": l["pbrAtBuy"], "delisted": gone,
                           "costBasis": cost_basis, "proceeds": gross - fee,
                           "pnl": pnl,
                           "ret": (gross - fee) / cost_basis - 1 if cost_basis else None})
        if matured:
            cohort_tickers, cohort_maturity = [], None

        # ── 자금 유입 (초기자본 12분할) ──────────────────────────────────────
        inflow = tranches.get(i, 0.0)
        cash += inflow
        inflow_hist.append(inflow)

        # ── 매수 ────────────────────────────────────────────────────────────
        # ★ 사전규격 준수의 핵심. 두 종류의 매수가 있다.
        #    (a) 선정월  : 재선정 후 매수 (i % buy_every == 0)
        #    (b) tranche월: 재선정 없이 **기존 코호트에 추가 투입** ← R8/R9 가 빠뜨린 것
        is_selection = (i % max(1, buy_every) == 0) and (i + hold_months < n)
        has_tranche = inflow > 0
        picks = []
        kind = None
        if is_selection:
            uni = cache.universe(d)
            # pick_fn 주입점 (R11 fair control). 미지정이면 R10 동작 그대로.
            chooser = pick_fn or _pick
            picks = chooser(cache, d, uni, factor=factor, percentile=percentile,
                            n_holdings=n_holdings, selection=selection, rng=rng,
                            banned=banned)
            if picks:
                cohort_id += 1
                cohort_tickers = list(picks)
                cohort_maturity = i + hold_months
                kind = "SELECT"
        elif has_tranche and cohort_tickers:
            uni = cache.universe(d)
            # 재선정 금지 — 같은 코호트 종목만 추가 매수. 상장폐지·거래정지분은 건너뛴다.
            picks = [t for t in cohort_tickers if t in uni]
            kind = "TOPUP"

        deployed = 0.0
        bought = 0
        if picks and cash > 0:
            per = cash / len(picks)
            rec = []
            for t in picks:
                r = (cache.universe(d)).get(t)
                p = r["close"] if r else None
                if not p or p <= 0:
                    continue
                exec_p = p * (1.0 + slip)
                budget = per / (1.0 + cost_bps / 10000.0)
                qty = (budget / exec_p) if fractional_shares else int(budget // exec_p)
                if qty <= 0:
                    continue
                gross = exec_p * qty
                fee = gross * cost_bps / 10000.0
                if gross + fee > cash:               # 음수현금·레버리지 금지
                    continue
                cash -= gross + fee
                cost_paid += fee
                slip_paid += (exec_p - p) * qty
                buy_notional += gross
                deployed += gross + fee
                trades += 1
                buys += 1
                buy_orders.add((i, t))
                bought += 1
                mat = (cohort_maturity if maturity_unit == MATURITY_COHORT
                       else i + hold_months)
                lots.append({"ticker": t, "qty": qty, "buyPrice": p, "execPrice": exec_p,
                             "buyIdx": i, "cohort": cohort_id, "market": r["market"],
                             "marketCapAtBuy": r["marketCap"], "pbrAtBuy": r["PBR"],
                             "maturityIdx": mat})
                unique_ever.add(t)
                rec.append({"ticker": t, "market": r["market"], "pbr": r["PBR"],
                            "marketCap": r["marketCap"], "close": p, "qty": qty})
            if bought:
                buy_months.add(i)
                pick_log.append({"idx": i, "date": d, "kind": kind,
                                 "cohort": cohort_id,
                                 "universeSize": len(cache.universe(d)),
                                 "requested": len(picks), "bought": bought,
                                 "picks": rec})

        # ── 평가 ────────────────────────────────────────────────────────────
        hv = 0.0
        for l in lots:
            p, _ = px_of(l["ticker"], snap, l["buyPrice"])
            hv += p * l["qty"]
        nav = cash + hv
        nav_hist.append(nav)
        cash_hist.append(cash / nav if nav > 0 else 0.0)
        pos_hist.append(len({l["ticker"] for l in lots}))

        capital_ledger.append({
            "idx": i, "date": d, "cohort": cohort_id if cohort_tickers else None,
            "action": kind, "beginningCash": round(cash_begin),
            "scheduledDeployment": round(inflow),
            "maturedSaleProceeds": round(sale_proceeds),
            "actualBuyAmount": round(deployed), "positionsBought": bought,
            "remainingCash": round(cash), "investedMarketValue": round(hv),
            "nav": round(nav), "positions": pos_hist[-1],
            "cashRatioPct": round(100 * cash_hist[-1], 2)})

    final_snap = cache.sn[dates[-1]]
    open_lots = []
    for l in lots:
        p, gone = px_of(l["ticker"], final_snap, l["buyPrice"])
        gross = p * l["qty"]
        cost_basis = l["execPrice"] * l["qty"]
        open_lots.append({"ticker": l["ticker"], "market": l["market"],
                          "cohort": l["cohort"], "buyIdx": l["buyIdx"], "sellIdx": None,
                          "qty": l["qty"], "buyPrice": l["buyPrice"],
                          "execPrice": l["execPrice"], "sellPrice": p,
                          "marketCapAtBuy": l["marketCapAtBuy"],
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
            "buyOrders": len(buy_orders), "sellOrders": len(sell_orders),
            "orders": len(buy_orders) + len(sell_orders),
            "ledger": ledger, "openLots": open_lots, "pickLog": pick_log,
            "capitalLedger": capital_ledger, "cohorts": cohort_id + 1,
            "maturityUnit": maturity_unit}


def run_case(cache, dates, idx, **kw):
    res = simulate(cache, dates, **kw)
    if not res:
        return None, None
    b = bench_same_schedule(idx, dates, tranches=res["tranches"])
    return res, evaluate(res, b)


# ─────────────────── §6 자본 흐름 원장 검증 ───────────────────
def audit_capital_flow(res, capital=50_000_000, deploy_months=12):
    """지시문 §6 필수 assertion. 하나라도 깨지면 구현이 사전규격을 못 지킨 것이다."""
    cl = res["capitalLedger"]
    checks = []

    def a(name, cond, detail=""):
        checks.append({"assertion": name, "pass": bool(cond), "detail": detail})

    sched = sum(r["scheduledDeployment"] for r in cl[:deploy_months])
    a("초기 12개월 예정투입 합계 == 초기자본",
      abs(sched - capital) <= deploy_months,
      f"{sched:,} vs {capital:,}")

    # 초기 12개월에 실제로 투입된 금액
    actual = sum(r["actualBuyAmount"] for r in cl[:deploy_months])
    a("초기 12개월 실제매수액이 예정투입의 97% 이상 (반올림 잔여만 허용)",
      actual >= 0.97 * sched, f"실제 {actual:,} / 예정 {sched:,} "
      f"({100*actual/sched:.2f}%)")

    first24 = cl[:24]
    avg24 = sum(r["cashRatioPct"] for r in first24) / len(first24)
    a("첫 24개월 평균 현금비중 < 10% (81% 현금대기 구조 부재)",
      avg24 < 10.0, f"{avg24:.2f}%")

    a("음수현금 없음", all(r["remainingCash"] >= -1 for r in cl),
      f"min {min(r['remainingCash'] for r in cl):,}")
    a("레버리지 없음 (투자평가액 <= NAV)",
      all(r["investedMarketValue"] <= r["nav"] + 1 for r in cl))

    # 자본 창조 금지: 총 유입은 초기자본과 정확히 같아야 한다
    a("총 현금유입 == 초기자본 (자본 창조 없음)",
      abs(res["contributed"] - capital) < 1.0,
      f"{res['contributed']:,.0f} vs {capital:,}")

    # 코호트 만기 타이밍 · 24개월 보유
    lots = res["ledger"] + res["openLots"]
    by_cohort = {}
    for l in lots:
        by_cohort.setdefault(l["cohort"], []).append(l)
    bad_mat = []
    for c, ls in by_cohort.items():
        sells = {l["sellIdx"] for l in ls if l["sellIdx"] is not None}
        if len(sells) > 1:
            bad_mat.append((c, sorted(sells)))
    a("코호트별 전량 매도가 한 시점에 이루어짐 (만기 후 전량 교체)",
      not bad_mat, str(bad_mat[:3]))

    sel = [l["buyIdx"] for l in lots if l["cohort"] == 0]
    holds = []
    for c, ls in by_cohort.items():
        s = [l["sellIdx"] for l in ls if l["sellIdx"] is not None]
        if s:
            holds.append(max(s) - min(l["buyIdx"] for l in ls))
    a("코호트 보유기간 == 24개월", all(h == 24 for h in holds),
      f"실측 {sorted(set(holds))}")

    # 동시 보유종목수
    a("최대 동시 보유종목수 <= 40 (N×hold 적층 없음)",
      max(res["positions"]) <= 40, f"max {max(res['positions'])}")

    # 중복 자본 생성 금지: 매수액 합 <= 유입 + 매도대금 합
    tot_buy = sum(r["actualBuyAmount"] for r in cl)
    tot_in = sum(r["scheduledDeployment"] for r in cl) + \
        sum(r["maturedSaleProceeds"] for r in cl)
    a("총 매수액 <= 총 유입 + 총 매도대금 (자본 중복 생성 없음)",
      tot_buy <= tot_in + 1000, f"매수 {tot_buy:,} vs 가용 {tot_in:,}")

    # 미래 현금 사용 금지 = 매달 매수액이 그 달 가용현금을 넘지 않음
    a("미래 현금 사용 없음 (월별 매수액 <= 기초현금 + 당월유입 + 당월매도)",
      all(r["actualBuyAmount"] <= r["beginningCash"] + r["scheduledDeployment"]
          + r["maturedSaleProceeds"] + 1000 for r in cl))

    return {"pass": all(c["pass"] for c in checks),
            "checks": checks,
            "first24mAvgCashPct": round(avg24, 2),
            "steadyStateAvgCashPct": round(
                sum(r["cashRatioPct"] for r in cl[24:]) / max(1, len(cl[24:])), 2),
            "cohorts": res["cohorts"],
            "first36Months": cl[:36]}


__all__ = ["simulate", "run_case", "audit_capital_flow", "bench_same_schedule",
           "evaluate", "monthly_twr", "chain_stats", "slice_index",
           "LiquidityRankCache", "MATURITY_COHORT", "MATURITY_PER_TRANCHE"]
