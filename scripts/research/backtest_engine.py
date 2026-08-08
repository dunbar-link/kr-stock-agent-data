#!/usr/bin/env python3
"""PHASE 2 — 한국 마법공식 장기 point-in-time 백테스트 엔진.

설계 원칙
---------
1. POINT-IN-TIME : 시점 t 의 결정에는 t 스냅샷에 담긴 정보만 쓴다.
   스냅샷은 그 날 KRX 가 실제로 공표한 값(종가·시총·PER·PBR·EPS·BPS)이다.
   미래에 정정된 재무로 과거를 다시 계산하지 않는다.
2. NO SURVIVORSHIP : universe 는 "그 날 상장돼 있던 종목"이다.
   나중에 상장폐지된 종목도 그 시점엔 매수 후보였으므로 포함된다.
   보유 중 상장폐지되면 마지막 관측가 × (1 - delistHaircut) 로 청산한다(민감도 검사 대상).
3. NO LOOK-AHEAD : 매수/매도는 항상 그 스냅샷 종가로 체결한다.
   순위 계산과 체결에 같은 날 데이터를 쓰되, 그 날 이후 정보는 일절 쓰지 않는다.
4. 결정적 : 같은 입력 → 같은 결과. 난수 없음.

전략 공식 (저장소 approx 모드와 동일 — build_magic_formula_fund.calculate_approx_magic_ranking)
   profitabilityRank : ROE 내림차순      (ROE = EPS / BPS)
   valueRank         : PER 오름차순      (EarningsYield = 1/PER)
   combinedRank      : 두 순위의 합 (낮을수록 우수)
   제외              : PER<=0, ROE<=0, 시총 < minMarketCap, 금융/지주, (옵션) 유틸리티

  ※ 원전(Greenblatt)은 EBIT/EV · EBIT/(순운전자본+유형자산) 이다.
    그 버전은 DART 재무가 필요하고 로컬 캐시가 FY2022~2025 뿐이라 장기 백테스트가 불가능하다.
    ROE/PER 근사는 저장소가 이미 쓰던 approx 모드이고, KRX 공표값이라 1999년까지 PIT 로 소급된다.
    이 한계는 보고서에 그대로 명시한다(원전이라고 부르지 않는다).

안전: 계산 전용. 네트워크 0 · 파일 write 0(호출자가 저장). 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import gzip
import math
from bisect import bisect_left
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SNAP_DIR = ROOT / "_cache" / "pit-snapshots"

# 저장소 CONFIG 와 동일한 제외 키워드(build_magic_formula_fund.CONFIG)
FINANCE_NAME_KEYWORDS = ("증권", "은행", "지주", "홀딩스", "캐피탈", "보험", "파이낸셜", "카드", "금융")
UTILITY_NAME_KEYWORDS = ("한국전력", "한국가스공사", "지역난방", "수자원")
# SPAC(기업인수목적회사) · 리츠 · 우선주는 마법공식 대상이 아니다.
SPAC_KEYWORDS = ("스팩", "기업인수목적")
REIT_KEYWORDS = ("리츠",)

MARKETCAP_WON_PER_UNIT = 100_000_000    # minMarketCap 단위 = 억원


# ─────────────────────────── 데이터 로딩 ───────────────────────────

def _f(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) or math.isinf(v) else v


def load_snapshots(snap_dir: Path = SNAP_DIR):
    """{'YYYY-MM-DD': {ticker: row}} — 전 기간을 메모리에 올린다(월별이라 충분히 작다)."""
    out = {}
    for p in sorted(snap_dir.glob("[12]*.csv.gz")):
        iso = p.name.split(".")[0]
        rows = {}
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            head = fh.readline().rstrip("\n").split(",")
            idx = {c: i for i, c in enumerate(head)}
            for line in fh:
                c = line.rstrip("\n").split(",")
                if len(c) < len(head):
                    continue
                t = c[idx["ticker"]]
                rows[t] = {
                    "market": c[idx["market"]],
                    "close": _f(c[idx["close"]]),
                    "marketCap": _f(c[idx["marketCap"]]),
                    "PER": _f(c[idx["PER"]]),
                    "PBR": _f(c[idx["PBR"]]),
                    "EPS": _f(c[idx["EPS"]]),
                    "BPS": _f(c[idx["BPS"]]),
                }
        if rows:
            out[iso] = rows
    return out


def load_names(snap_dir: Path = SNAP_DIR):
    import json
    p = snap_dir / "_ticker-names.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


# ─────────────────────────── 랭킹 ───────────────────────────

def _excluded_by_name(name: str, exclude_financial: bool, exclude_utility: bool) -> bool:
    if not name:
        return False
    for k in SPAC_KEYWORDS + REIT_KEYWORDS:
        if k in name:
            return True
    if exclude_financial and any(k in name for k in FINANCE_NAME_KEYWORDS):
        return True
    if exclude_utility and any(k in name for k in UTILITY_NAME_KEYWORDS):
        return True
    return False


def rank_universe(snap, names, *, market="COMBINED", min_market_cap=300,
                  exclude_financial=True, exclude_utility=True):
    """t 시점 마법공식 순위. 저장소 approx 모드와 같은 규칙."""
    elig = []
    for t, r in snap.items():
        if market != "COMBINED" and r["market"] != market:
            continue
        # 우선주(코드 끝자리가 0이 아님)는 본주와 중복이라 제외한다.
        if not t.endswith("0"):
            continue
        per, eps, bps = r["PER"], r["EPS"], r["BPS"]
        mc, px = r["marketCap"], r["close"]
        if per is None or eps is None or bps is None or mc is None or px is None or px <= 0:
            continue
        if per <= 0 or bps <= 0:
            continue
        roe = eps / bps
        if roe <= 0:
            continue
        if mc < min_market_cap * MARKETCAP_WON_PER_UNIT:
            continue
        if _excluded_by_name(names.get(t, ""), exclude_financial, exclude_utility):
            continue
        elig.append({"ticker": t, "ROE": roe, "PER": per, "close": px, "marketCap": mc})
    if not elig:
        return []
    for i, s in enumerate(sorted(elig, key=lambda s: (-s["ROE"], s["ticker"])), 1):
        s["profitabilityRank"] = i
    for i, s in enumerate(sorted(elig, key=lambda s: (s["PER"], s["ticker"])), 1):
        s["valueRank"] = i
    for s in elig:
        s["combinedRank"] = s["profitabilityRank"] + s["valueRank"]
    final = sorted(elig, key=lambda s: (s["combinedRank"], s["PER"], s["ticker"]))
    for i, s in enumerate(final, 1):
        s["rank"] = i
    return final


# ─────────────────────────── 백테스트 ───────────────────────────

class Batch:
    __slots__ = ("buyDate", "maturityIdx", "lots")

    def __init__(self, buy_date, maturity_idx, lots):
        self.buyDate = buy_date
        self.maturityIdx = maturity_idx
        self.lots = lots            # [{ticker, qty, buyPrice, lastPrice}]


def run_backtest(snapshots, names, *, dates=None, hold_months=12, n_stocks=20,
                 contribution="LUMP_SUM", initial_capital=10_000_000,
                 monthly_amount=500_000, market="COMBINED", min_market_cap=300,
                 exclude_financial=True, exclude_utility=True,
                 cost_bps=15.0, sell_tax_bps=20.0, delist_haircut=0.0,
                 start=None, end=None):
    """월 단위 배치 롤링 백테스트.

    contribution:
      LUMP_SUM             초기자금 전액을 t0 에 1개 배치로. 만기마다 전량 재투자(=holdMonths 리밸런싱).
      MONTHLY_DCA          매월 monthly_amount 로 새 배치. 만기 배치 매도대금은 그 달 배치에 합산.
      QUARTERLY_DCA        3개월마다 monthly_amount*3 로 새 배치.
      INITIAL_PLUS_MONTHLY 초기자금을 hold_months 개로 균등 분할해 진입 + 매월 적립.

    비용: 매수 cost_bps, 매도 cost_bps + sell_tax_bps (bp = 0.01%).
    """
    ds = dates if dates is not None else sorted(snapshots.keys())
    if start:
        ds = [d for d in ds if d >= start]
    if end:
        ds = [d for d in ds if d <= end]
    if len(ds) < hold_months + 2:
        return None

    cash = 0.0
    contributed = 0.0
    batches: list[Batch] = []
    nav_series = []
    trades = 0
    buy_notional = 0.0
    sell_notional = 0.0
    cost_paid = 0.0
    delist_events = 0
    delist_notional = 0.0
    last_price = {}                      # 상장폐지 대비 마지막 관측가

    initial_slice = (initial_capital / hold_months) if contribution == "INITIAL_PLUS_MONTHLY" else 0.0
    initial_remaining = initial_capital if contribution == "INITIAL_PLUS_MONTHLY" else 0.0

    for i, d in enumerate(ds):
        snap = snapshots[d]
        for t, r in snap.items():
            if r["close"]:
                last_price[t] = r["close"]

        # ── 1) 만기 배치 매도 ───────────────────────────────────────────
        matured, keep = [], []
        for b in batches:
            (matured if b.maturityIdx <= i else keep).append(b)
        batches = keep
        for b in matured:
            for lot in b.lots:
                r = snap.get(lot["ticker"])
                px = r["close"] if r and r["close"] else None
                if px is None:                       # 보유 중 상장폐지 → 마지막 관측가에 haircut
                    px = (last_price.get(lot["ticker"]) or lot["buyPrice"]) * (1.0 - delist_haircut)
                    delist_events += 1
                    delist_notional += px * lot["qty"]
                gross = px * lot["qty"]
                fee = gross * (cost_bps + sell_tax_bps) / 10000.0
                cash += gross - fee
                sell_notional += gross
                cost_paid += fee
                trades += 1

        # ── 2) 자금 유입 ────────────────────────────────────────────────
        new_money = 0.0
        if i == 0 and contribution in ("LUMP_SUM", "MONTHLY_DCA", "QUARTERLY_DCA"):
            new_money += initial_capital if contribution == "LUMP_SUM" else 0.0
        if contribution == "MONTHLY_DCA":
            new_money += monthly_amount
        elif contribution == "QUARTERLY_DCA" and i % 3 == 0:
            new_money += monthly_amount * 3
        elif contribution == "INITIAL_PLUS_MONTHLY":
            if initial_remaining > 0:
                take = min(initial_slice, initial_remaining)
                new_money += take
                initial_remaining -= take
            new_money += monthly_amount
        cash += new_money
        contributed += new_money

        # ── 3) 신규 배치 매수 ───────────────────────────────────────────
        is_buy_month = True
        if contribution == "LUMP_SUM":
            is_buy_month = (i == 0) or any(b.maturityIdx == i for b in matured) or not batches
        elif contribution == "QUARTERLY_DCA":
            is_buy_month = (i % 3 == 0)
        if is_buy_month and cash > 0 and i + hold_months < len(ds):
            ranked = rank_universe(snap, names, market=market, min_market_cap=min_market_cap,
                                   exclude_financial=exclude_financial,
                                   exclude_utility=exclude_utility)
            picks = ranked[:n_stocks]
            if picks:
                per_stock = cash / len(picks)
                lots = []
                for s in picks:
                    px = s["close"]
                    budget = per_stock / (1.0 + cost_bps / 10000.0)
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
                    lots.append({"ticker": s["ticker"], "qty": qty, "buyPrice": px})
                if lots:
                    batches.append(Batch(d, i + hold_months, lots))

        # ── 4) NAV 평가 ─────────────────────────────────────────────────
        holdings_value = 0.0
        for b in batches:
            for lot in b.lots:
                r = snap.get(lot["ticker"])
                px = r["close"] if r and r["close"] else (last_price.get(lot["ticker"]) or lot["buyPrice"])
                holdings_value += px * lot["qty"]
        nav_series.append({"date": d, "nav": cash + holdings_value, "cash": cash,
                           "holdings": holdings_value, "contributed": contributed,
                           "positions": sum(len(b.lots) for b in batches)})

    return {
        "navSeries": nav_series,
        "trades": trades,
        "buyNotional": buy_notional,
        "sellNotional": sell_notional,
        "costPaid": cost_paid,
        "delistEvents": delist_events,
        "delistNotional": delist_notional,
        "finalNav": nav_series[-1]["nav"] if nav_series else 0.0,
        "totalContributed": contributed,
    }


# ─────────────────────────── 성과 지표 ───────────────────────────

def _xirr(cashflows, guess=0.1):
    """적립식은 단순 CAGR 이 의미 없다 → money-weighted 수익률(IRR, 연율)."""
    if not cashflows:
        return None
    t0 = cashflows[0][0]
    def npv(r):
        s = 0.0
        for t, c in cashflows:
            yrs = (t - t0) / 365.25
            s += c / ((1.0 + r) ** yrs)
        return s
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


def _to_ord(iso):
    from datetime import date
    y, m, d = (int(x) for x in iso.split("-"))
    return date(y, m, d).toordinal()


def evaluate(result, *, monthly_amount=0.0, initial_capital=0.0, contribution="LUMP_SUM"):
    """CAGR 하나로 판단하지 않는다 — 일반인이 평생 유지 가능한가를 보는 지표들."""
    ns = result["navSeries"]
    if len(ns) < 13:
        return None
    navs = [x["nav"] for x in ns]
    dates = [x["date"] for x in ns]
    years = (_to_ord(dates[-1]) - _to_ord(dates[0])) / 365.25

    # 적립식은 TWR(시간가중)과 IRR(금액가중)을 분리해서 본다.
    contributed = [x["contributed"] for x in ns]
    twr = 1.0
    prev_nav, prev_con = navs[0], contributed[0]
    for k in range(1, len(navs)):
        inflow = contributed[k] - prev_con
        base = prev_nav + inflow
        if base > 0:
            twr *= navs[k] / base
        prev_nav, prev_con = navs[k], contributed[k]
    twr_cagr = twr ** (1.0 / years) - 1.0 if years > 0 and twr > 0 else None

    flows = []
    prev_con = 0.0
    for x in ns:
        inflow = x["contributed"] - prev_con
        if inflow > 0:
            flows.append((_to_ord(x["date"]), -inflow))
        prev_con = x["contributed"]
    flows.append((_to_ord(dates[-1]), navs[-1]))
    irr = _xirr(flows)

    # MDD 는 적립식에서도 "고점 대비 NAV 낙폭"으로 본다(체감 손실).
    peak, mdd, mdd_date, peak_date, worst_recovery = navs[0], 0.0, None, dates[0], 0
    cur_under = 0
    for k, v in enumerate(navs):
        if v >= peak:
            peak, peak_date = v, dates[k]
            cur_under = 0
        else:
            cur_under += 1
            worst_recovery = max(worst_recovery, cur_under)
            dd = v / peak - 1.0
            if dd < mdd:
                mdd, mdd_date = dd, dates[k]

    rets = []
    for k in range(1, len(navs)):
        inflow = contributed[k] - contributed[k - 1]
        base = navs[k - 1] + inflow
        if base > 0:
            rets.append(navs[k] / base - 1.0)
    mean = sum(rets) / len(rets) if rets else 0.0
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1) if len(rets) > 1 else 0.0
    vol_a = math.sqrt(var) * math.sqrt(12)
    downs = [r for r in rets if r < 0]
    dvar = sum(r * r for r in downs) / len(downs) if downs else 0.0
    dvol_a = math.sqrt(dvar) * math.sqrt(12)
    sharpe = ((mean * 12) / vol_a) if vol_a > 0 else None
    sortino = ((mean * 12) / dvol_a) if dvol_a > 0 else None

    def rolling(mons):
        out = []
        for k in range(len(navs) - mons):
            base = navs[k] + (contributed[k + mons] - contributed[k])
            if base > 0:
                out.append((navs[k + mons] / base) ** (12.0 / mons) - 1.0)
        return out

    r1, r3, r5 = rolling(12), rolling(36), rolling(60)

    def stat(a):
        if not a:
            return None
        s = sorted(a)
        return {"n": len(a), "min": s[0], "p10": s[int(len(s) * 0.1)],
                "median": s[len(s) // 2], "p90": s[int(len(s) * 0.9)], "max": s[-1],
                "negShare": sum(1 for x in a if x < 0) / len(a)}

    turnover = (result["buyNotional"] / (sum(navs) / len(navs))) / years if years > 0 else None

    return {
        "years": years, "start": dates[0], "end": dates[-1], "months": len(navs),
        "finalNav": navs[-1], "totalContributed": contributed[-1],
        "totalReturnOnContrib": (navs[-1] / contributed[-1] - 1.0) if contributed[-1] else None,
        "twrCagr": twr_cagr, "irr": irr,
        "mdd": mdd, "mddDate": mdd_date, "peakDateBeforeMdd": peak_date,
        "maxUnderwaterMonths": worst_recovery,
        "volAnnual": vol_a, "sharpe": sharpe, "sortino": sortino,
        "rolling1y": stat(r1), "rolling3y": stat(r3), "rolling5y": stat(r5),
        "trades": result["trades"], "costPaid": result["costPaid"],
        "costPctOfContrib": (result["costPaid"] / contributed[-1]) if contributed[-1] else None,
        "turnoverPerYear": turnover,
        "delistEvents": result["delistEvents"],
    }


def benchmark_series(snapshots, dates, names, *, market="COMBINED", min_market_cap=300):
    """동일가중 시장 포트폴리오(마법공식과 같은 universe 필터, 순위만 안 씀).
    '복잡한 공식을 쓸 이유가 있었는가'를 판정하는 기준선."""
    prev, series, idx = None, [], 100.0
    for d in dates:
        snap = snapshots[d]
        cur = {}
        for t, r in snap.items():
            if market != "COMBINED" and r["market"] != market:
                continue
            if not t.endswith("0") or not r["close"] or not r["marketCap"]:
                continue
            if r["marketCap"] < min_market_cap * MARKETCAP_WON_PER_UNIT:
                continue
            if _excluded_by_name(names.get(t, ""), True, True):
                continue
            cur[t] = r["close"]
        if prev:
            common = [t for t in cur if t in prev and prev[t] > 0]
            if common:
                idx *= sum(cur[t] / prev[t] for t in common) / len(common)
        series.append({"date": d, "index": idx})
        prev = cur
    return series
