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


def investable_universe(snap, names, *, market="COMBINED", min_market_cap=300):
    """t 시점 '살 수 있었던' 종목 집합. 전략 rank_universe 와 **같은 투자가능 조건**을 쓴다
    (단 순위 산출에 필요한 PER/ROE 유효성은 요구하지 않는다 — benchmark 는 순위를 안 쓰므로)."""
    out = {}
    for t, r in snap.items():
        if market != "COMBINED" and r["market"] != market:
            continue
        if not t.endswith("0") or not r["close"] or not r["marketCap"]:
            continue
        if r["marketCap"] < min_market_cap * MARKETCAP_WON_PER_UNIT:
            continue
        if _excluded_by_name(names.get(t, ""), True, True):
            continue
        out[t] = r
    return out


def benchmark_series(snapshots, dates, names, *, market="COMBINED", min_market_cap=300,
                     weighting="EQUAL", delist_haircut=0.0, cost_bps=0.0, sell_tax_bps=0.0,
                     rebalance="MONTHLY"):
    """survivorship-free 시장 benchmark.

    ★ R5 결함 수정 (WABABA-BENCHMARK-SURVIVORSHIP-FIX-AND-STRATEGY-REASSESS-R5)
    -------------------------------------------------------------------------
    이전 구현은 월 수익률을 `common = 전월과 당월에 **둘 다 있는** 종목` 으로만 계산했다.
    그래서 그 달에 상장폐지된 종목의 **마지막 손실이 통째로 사라졌다**.
    또 시총 하한을 매월 재적용해, 급락으로 하한 아래로 떨어진 종목(=손실 확정분)도
    그 달 수익률에서 빠졌다. 두 경로 모두 benchmark 를 위로 편향시킨다.
    전략은 상장폐지·급락 손실을 그대로 먹는데 benchmark 는 안 먹으니 비교가 불공정했다.

    수정 원칙: **전략과 완전히 같은 생존조건**을 적용한다.
      - 필터(시총·금융/지주·SPAC·우선주)는 **선정 시점에만** 적용한다.
        선정 후에는 보유 중이므로 시총이 내려가도 강제로 빼지 않는다.
      - 보유 중 종목이 다음 스냅샷에서 사라지면(상장폐지·거래종료)
        마지막 관측가 × (1 - delist_haircut) 로 청산한다 — run_backtest 와 동일 규칙.
      - 현재 살아남은 종목만 남기는 current-universe bias 를 만들지 않는다.

    weighting : EQUAL(동일가중) | CAP(시가총액가중 — 공식 지수 성격에 가까움)
    rebalance : MONTHLY(매월 재편입) | BUY_HOLD(최초 1회 선정 후 그대로 보유)
    비용      : 매월 재편입분에만 부과(BUY_HOLD 는 최초 매수 1회분만).
    """
    last_price = {}
    idx = 100.0
    series = []
    holdings = None            # {ticker: (기준가, 비중)}
    cash_weight = 0.0          # 상장폐지 청산분(더 이상 성장하지 않음)
    delist_events = 0
    delist_weight = 0.0
    turnover_sum = 0.0

    for i, d in enumerate(dates):
        snap = snapshots[d]
        for t, r in snap.items():
            if r["close"]:
                last_price[t] = r["close"]

        # ── 1) 지난달 보유분을 이번 달까지 들고 온 수익률 ────────────────────────
        if holdings is not None:
            growth = cash_weight          # 청산분은 현금으로 남아 성장률 1.0
            ended = {}                    # 이번 달 종료 시점 가치(비중 재계산용)
            liquidated = 0.0
            for t, (p0, w) in holdings.items():
                r = snap.get(t)
                if r and r["close"]:
                    p1 = r["close"]
                    ended[t] = (p1, w * (p1 / p0))
                else:
                    # 상장폐지/거래종료 — 전략과 동일하게 마지막 관측가에 haircut 적용해 **청산**한다.
                    #   청산 후에는 보유목록에서 빼서 현금으로 남긴다.
                    #   (빼지 않으면 폐지 종목이 매달 다시 '폐지 이벤트'로 세어져 통계가 망가진다)
                    p1 = (last_price.get(t) or p0) * (1.0 - delist_haircut)
                    delist_events += 1
                    delist_weight += w
                    liquidated += w * (p1 / p0)
                growth += w * (p1 / p0)
            idx *= growth
            if growth > 0:
                cash_weight = (cash_weight + liquidated) / growth
                holdings = {t: (px, val / growth) for t, (px, val) in ended.items()}
            else:
                cash_weight, holdings = 1.0, {}

        # ── 2) 재편입 ────────────────────────────────────────────────────────────
        need_select = (holdings is None) or (rebalance == "MONTHLY")
        if need_select:
            uni = investable_universe(snap, names, market=market, min_market_cap=min_market_cap)
            if uni:
                if weighting == "CAP":
                    tot = sum(r["marketCap"] for r in uni.values())
                    new_w = {t: (r["marketCap"] / tot) for t, r in uni.items()} if tot > 0 else {}
                else:
                    new_w = {t: 1.0 / len(uni) for t in uni}
                # 회전율 = 비중 변화 절대값 합 / 2 (편도)
                if holdings:
                    keys = set(new_w) | set(holdings)
                    to = sum(abs(new_w.get(t, 0.0) - holdings.get(t, (0, 0.0))[1]) for t in keys) / 2.0
                else:
                    to = 1.0
                turnover_sum += to
                if cost_bps or sell_tax_bps:
                    # 편도 교체분에만 비용. 매도측엔 거래세도 부과(전략과 같은 가정).
                    idx *= (1.0 - to * (cost_bps + (cost_bps + sell_tax_bps)) / 10000.0)
                holdings = {t: (uni[t]["close"], w) for t, w in new_w.items()}
                cash_weight = 0.0          # 재편입 시 청산 현금은 다시 시장에 투입된다

        series.append({"date": d, "index": idx, "positions": len(holdings or {})})

    years = max(1e-9, (_to_ord_bm(dates[-1]) - _to_ord_bm(dates[0])) / 365.25)
    return {"series": series, "final": idx,
            "cagr": (idx / 100.0) ** (1.0 / years) - 1.0,
            "years": years, "delistEvents": delist_events,
            "delistWeightSum": delist_weight,
            "turnoverPerYear": turnover_sum / years,
            "weighting": weighting, "rebalance": rebalance,
            "market": market, "minMarketCap": min_market_cap,
            "delistHaircut": delist_haircut,
            "costBps": cost_bps, "sellTaxBps": sell_tax_bps}


def _to_ord_bm(iso):
    from datetime import date
    y, m, d = (int(x) for x in iso.split("-"))
    return date(y, m, d).toordinal()


def official_index_series(dates, name="KOSPI", path=None):
    """공식 KRX 지수(시가총액가중). 자체 동일가중 benchmark 와 **다른 것**이다.
    지수사업자가 편입/제외를 처리하므로 우리 규칙과 생존조건이 다르다 — 참고용으로만 쓴다."""
    import json as _json
    from pathlib import Path as _P
    p = _P(path) if path else (SNAP_DIR / "_official-index.json")
    try:
        raw = _json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    ser = raw.get(name) or {}
    if not ser:
        return None
    keys = sorted(ser)
    out, base = [], None
    for d in dates:
        # 그 날 값이 없으면 그 날 이전 가장 가까운 값(휴장 대응)
        k = d if d in ser else None
        if k is None:
            lo, hi = 0, len(keys) - 1
            cand = None
            while lo <= hi:
                mid = (lo + hi) // 2
                if keys[mid] <= d:
                    cand = keys[mid]; lo = mid + 1
                else:
                    hi = mid - 1
            k = cand
        if k is None:
            continue
        v = ser[k]
        if base is None:
            base = v
        out.append({"date": d, "index": 100.0 * v / base})
    if not out or base is None:
        return None
    years = max(1e-9, (_to_ord_bm(out[-1]["date"]) - _to_ord_bm(out[0]["date"])) / 365.25)
    return {"series": out, "final": out[-1]["index"],
            "cagr": (out[-1]["index"] / 100.0) ** (1.0 / years) - 1.0,
            "years": years, "source": f"KRX official {name} index (cap-weighted)",
            "note": "지수사업자 편입/제외 규칙 — 자체 benchmark 와 생존조건이 다르다"}
