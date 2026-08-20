#!/usr/bin/env python3
"""R7 — 팩터 신호 탐색 (portfolio 최적화 이전 단계).

WABABA-KOREA-FACTOR-SIGNAL-DISCOVERY-R7

질문: "한국 시장에서 point-in-time 기준으로 미래수익과 **반복적으로** 연결되는
       가치/수익성 신호가 실제 존재하는가?"

R6 까지는 portfolio 운용 파라미터를 뒤졌다. 이번엔 그 앞 단계 — 신호 자체를 본다.
신호가 확인된 것만 다음 portfolio 연구로 보낸다.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
사전 확정 명세 (§11 — 실행 **전에** 고정한다. 결과를 보고 바꾸지 않는다)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FACTORS  경제적 설명이 되는 소수만. 자동 대량생성 금지.
  EY      = 1/PER          이익수익률(VALUE)      — PER>0 인 흑자기업만 정의됨(선택편의 주의)
  BM      = 1/PBR          장부/시가(VALUE)       — 커버리지가 가장 넓다
  ROE     = EPS/BPS        수익성(QUALITY)
  DY      = DIV            배당수익률(%)
  SIZE    = -marketCap     소형주 프리미엄(CONTROL 겸 factor)
  MF      = rank(ROE↓)+rank(PER↑)   기존 APPROX Magic Formula(비교 대상)
  BM_ROE  = rank(BM↓)+rank(ROE↓)    결합 후보(단독 신호 확인 후에만 해석)

QUANTILE 10분위. D1 = 가장 매력적. 분위 내 동일가중.
HORIZON  3 / 6 / 12 / 24 개월 forward return.
UNIVERSE R5·R6 와 동일한 투자가능 필터(보통주·시총 300억↑·금융/지주/유틸/SPAC/리츠 제외).
DELIST   보유 중 상장폐지 = 마지막 관측가 × (1-haircut). 기본 0, 민감도 1.0 별도 측정.
SUBPERIOD 2007-2011 / 2012-2016 / 2017-2021 / 2022-현재.

판정 기준(사전 확정)
  STRONG      12M TOP-BOTTOM 연율 spread > 3%p  AND 4개 하위구간 중 3개 이상 같은 방향(양)
              AND 분위 gradient 상관 <= -0.6   AND TOP > universe
              AND size 3분위 중 2개 이상에서 spread 양수
  PROMISING   spread > 0 AND 하위구간 3/4 동일 방향, 그러나 위 조건 일부 미충족
  WEAK        spread > 0 이지만 하위구간 2/4 이하
  NO_SIGNAL   |12M 연율 spread| < 1%p 또는 방향 불일치
  INVERTED    spread < -1%p 이고 하위구간 3/4 이상 음수
  UNRELIABLE  관측치 부족(분위당 평균 10종목 미만) 또는 데이터 결함

안전: 계산 전용. 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe, load_names, load_snapshots  # noqa: E402

HORIZONS = [3, 6, 12, 24]
N_QUANTILES = 10
MIN_PER_QUANTILE = 10
SUBPERIODS = [("2007-2011", "2007-01", "2011-12"), ("2012-2016", "2012-01", "2016-12"),
              ("2017-2021", "2017-01", "2021-12"), ("2022-현재", "2022-01", "2099-12")]
RANDOM_SEED = 20260820

# 판정 임계값(사전 확정)
STRONG_SPREAD = 0.03
NOISE_BAND = 0.01
GRADIENT_CORR = -0.6


# ──────────────────────────── 팩터 정의 ────────────────────────────
def factor_values(uni):
    """t 시점 투자가능 종목별 팩터값. 값이 정의되지 않으면 그 팩터에서 제외(결측을 0으로 채우지 않는다)."""
    out = {}
    rows = []
    for t, r in uni.items():
        per, pbr, eps, bps = r["PER"], r["PBR"], r["EPS"], r["BPS"]
        mc, div = r["marketCap"], r.get("DIV")
        rows.append((t, per, pbr, eps, bps, mc, div))

    def put(name, pairs):
        # pairs = [(ticker, value)] — 값이 큰 쪽이 '매력적'이 되도록 부호를 맞춘다.
        out[name] = {t: v for t, v in pairs if v is not None and math.isfinite(v)}

    put("EY", [(t, (1.0 / per) if (per and per > 0) else None) for t, per, *_ in rows])
    put("BM", [(t, (1.0 / pbr) if (pbr and pbr > 0) else None) for t, _, pbr, *_ in rows])
    put("ROE", [(t, (eps / bps) if (eps is not None and bps and bps > 0) else None)
                for t, _, _, eps, bps, _, _ in rows])
    put("DY", [(t, div if (div is not None and div > 0) else None)
               for t, _, _, _, _, _, div in rows])
    put("SIZE", [(t, (-mc) if mc else None) for t, _, _, _, _, mc, _ in rows])

    # 결합: 순위합(낮을수록 우수) → 부호 반전해 '큰 값 = 매력적'으로 통일
    def rank_map(d, reverse):
        s = sorted(d.items(), key=lambda kv: (-kv[1] if reverse else kv[1], kv[0]))
        return {t: i + 1 for i, (t, _) in enumerate(s)}

    per_map = {t: per for t, per, *_ in rows if per and per > 0}
    roe_map = out.get("ROE", {})
    bm_map = out.get("BM", {})
    if per_map and roe_map:
        rp = rank_map(per_map, reverse=False)      # PER 낮을수록 좋음
        rq = rank_map(roe_map, reverse=True)       # ROE 높을수록 좋음
        common = set(rp) & set(rq)
        out["MF"] = {t: -(rp[t] + rq[t]) for t in common}
    if bm_map and roe_map:
        rb = rank_map(bm_map, reverse=True)
        rq2 = rank_map(roe_map, reverse=True)
        common = set(rb) & set(rq2)
        out["BM_ROE"] = {t: -(rb[t] + rq2[t]) for t in common}
    return out


# ──────────────────────────── forward return ────────────────────────────
def build_price_index(snapshots, dates):
    """t 시점 가격과 '마지막 관측가'를 미리 만들어 둔다(상장폐지 처리용)."""
    px = []
    last = {}
    for d in dates:
        snap = snapshots[d]
        cur = {}
        for t, r in snap.items():
            if r["close"]:
                cur[t] = r["close"]
                last[t] = r["close"]
        px.append((cur, dict(last)))
    return px


def fwd_return(px, i, j, ticker, haircut=0.0):
    """i → j 수익률. j 시점에 없으면 마지막 관측가 × (1-haircut) 로 청산(생존편의 제거)."""
    p0 = px[i][0].get(ticker)
    if not p0 or p0 <= 0:
        return None
    p1 = px[j][0].get(ticker)
    if p1 is None:
        p1 = px[j][1].get(ticker)
        if p1 is None:
            return None
        p1 *= (1.0 - haircut)
    return p1 / p0 - 1.0


# ──────────────────────────── 분위 테스트 ────────────────────────────
def quantile_panel(snapshots, names, dates, *, market="COMBINED", min_market_cap=300,
                   haircut=0.0, size_bucket=None, factors=None,
                   extra_value_fn=None, universe_filter=None):
    # R13 가산 주입점 (기본값 None → R7 동작 완전 불변, 회귀로 고정)
    #   extra_value_fn(uni, d, i) -> {factorName: {ticker: value}}
    #       날짜 문맥이 필요한 factor(과거 스냅샷 기반·DART 공시일 기반)를 넣는다.
    #   universe_filter(uni, d, i) -> uni
    #       BM 상위 20% 내부 분석처럼 모집단을 좁힐 때 쓴다.
    """각 시점 × 팩터 × 분위 × horizon 의 forward return 을 모은다."""
    px = build_price_index(snapshots, dates)
    acc = {}          # factor -> horizon -> quantile -> [returns]
    uni_acc = {}      # horizon -> [universe mean returns]
    counts = {}
    for i, d in enumerate(dates):
        uni = investable_universe(snapshots[d], names, market=market, min_market_cap=min_market_cap)
        if not uni:
            continue
        # size 통제: 시총 3분위 중 지정 버킷만
        if size_bucket is not None:
            caps = sorted(((r["marketCap"] or 0), t) for t, r in uni.items())
            n = len(caps)
            lo, hi = (n * size_bucket) // 3, (n * (size_bucket + 1)) // 3
            keep = {t for _, t in caps[lo:hi]}
            uni = {t: r for t, r in uni.items() if t in keep}
            if len(uni) < N_QUANTILES * MIN_PER_QUANTILE // 2:
                continue
        if universe_filter is not None:
            uni = universe_filter(uni, d, i)
            if not uni:
                continue
        fv = factor_values(uni)
        if extra_value_fn is not None:
            for k, v in (extra_value_fn(uni, d, i) or {}).items():
                fv[k] = v
        if factors:
            fv = {k: v for k, v in fv.items() if k in factors}
        for h in HORIZONS:
            j = i + h
            if j >= len(dates):
                continue
            rets_all = []
            for t in uni:
                r = fwd_return(px, i, j, t, haircut)
                if r is not None:
                    rets_all.append(r)
            if rets_all:
                uni_acc.setdefault(h, []).append(sum(rets_all) / len(rets_all))
            for fname, vals in fv.items():
                items = [(t, v) for t, v in vals.items() if t in uni]
                if len(items) < N_QUANTILES * MIN_PER_QUANTILE:
                    continue
                items.sort(key=lambda kv: (-kv[1], kv[0]))       # 큰 값 = 매력적 = D1
                n = len(items)
                counts.setdefault(fname, []).append(n)
                for q in range(N_QUANTILES):
                    a, b = (n * q) // N_QUANTILES, (n * (q + 1)) // N_QUANTILES
                    rs = []
                    for t, _ in items[a:b]:
                        r = fwd_return(px, i, j, t, haircut)
                        if r is not None:
                            rs.append(r)
                    if rs:
                        acc.setdefault(fname, {}).setdefault(h, {}).setdefault(q + 1, []) \
                           .append(sum(rs) / len(rs))
    return acc, uni_acc, counts


def ann(r, months):
    if r is None or r <= -1:
        return None
    return (1.0 + r) ** (12.0 / months) - 1.0


def summarize_factor(acc, uni_acc, fname):
    out = {}
    for h in HORIZONS:
        qd = acc.get(fname, {}).get(h)
        if not qd:
            continue
        means = {}
        for q, arr in sorted(qd.items()):
            means[q] = sum(arr) / len(arr)
        top, bot = means.get(1), means.get(N_QUANTILES)
        uni = (sum(uni_acc.get(h, [])) / len(uni_acc[h])) if uni_acc.get(h) else None
        # 분위 gradient — 분위 index 와 평균수익 사이 순위상관(음수면 D1 이 가장 좋다)
        ks = sorted(means)
        grad = _spearman(ks, [means[k] for k in ks]) if len(ks) >= 5 else None
        n_obs = len(qd.get(1, []))
        out[h] = {
            "quantileMeans": {str(k): means[k] for k in ks},
            "quantileMeansAnn": {str(k): ann(means[k], h) for k in ks},
            "top": top, "bottom": bot, "universe": uni,
            "topMinusBottom": (top - bot) if (top is not None and bot is not None) else None,
            "topMinusUniverse": (top - uni) if (top is not None and uni is not None) else None,
            "topMinusBottomAnn": ann(top - bot, h) if (top is not None and bot is not None) else None,
            "topMinusUniverseAnn": ann(top - uni, h) if (top is not None and uni is not None) else None,
            "gradientCorr": grad,
            "obsOverlapping": n_obs,
            "obsNonOverlapping": (n_obs // h) if n_obs else 0,
            "topPositiveRate": _pos_rate(qd.get(1, [])),
            "spreadPositiveRate": _spread_pos_rate(qd),
            "spreadStdev": _stdev([a - b for a, b in zip(qd.get(1, []), qd.get(N_QUANTILES, []))]),
        }
    return out


def _spearman(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    rx = _ranks(xs)
    ry = _ranks(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return (num / den) if den else None


def _ranks(v):
    s = sorted(range(len(v)), key=lambda i: v[i])
    r = [0] * len(v)
    for i, idx in enumerate(s):
        r[idx] = i + 1
    return r


def _pos_rate(a):
    return (sum(1 for x in a if x > 0) / len(a)) if a else None


def _spread_pos_rate(qd):
    t, b = qd.get(1, []), qd.get(N_QUANTILES, [])
    n = min(len(t), len(b))
    if not n:
        return None
    return sum(1 for i in range(n) if t[i] - b[i] > 0) / n


def _stdev(a):
    if len(a) < 2:
        return None
    m = sum(a) / len(a)
    return math.sqrt(sum((x - m) ** 2 for x in a) / (len(a) - 1))


# ──────────────────────────── 대조군 ────────────────────────────
def control_returns(snapshots, names, dates, *, kind="RANDOM", n_pick=50,
                    min_market_cap=300, haircut=0.0, seed=RANDOM_SEED):
    """같은 universe·같은 horizon 에서 정보 없는 선택이 어떤 수익을 내는지."""
    px = build_price_index(snapshots, dates)
    rng = random.Random(seed)
    out = {}
    for i, d in enumerate(dates):
        uni = investable_universe(snapshots[d], names, min_market_cap=min_market_cap)
        if not uni:
            continue
        tick = sorted(uni)
        if kind == "RANDOM":
            pick = rng.sample(tick, min(n_pick, len(tick)))
        elif kind == "MCAP_LARGE":
            pick = [t for _, t in sorted(((-(uni[t]["marketCap"] or 0), t) for t in tick))][:n_pick]
        else:                                     # EQUAL_WEIGHT_UNIVERSE
            pick = tick
        for h in HORIZONS:
            j = i + h
            if j >= len(dates):
                continue
            rs = [fwd_return(px, i, j, t, haircut) for t in pick]
            rs = [r for r in rs if r is not None]
            if rs:
                out.setdefault(h, []).append(sum(rs) / len(rs))
    return {h: {"mean": sum(v) / len(v), "meanAnn": ann(sum(v) / len(v), h), "n": len(v)}
            for h, v in out.items()}


def verdict(full, subs, size_results, fname):
    """사전 확정 규칙으로만 판정한다(결과를 보고 기준을 바꾸지 않는다)."""
    f12 = full.get(12)
    if not f12:
        return "UNRELIABLE", {"reason": "12M 결과 없음"}
    if (f12.get("obsNonOverlapping") or 0) < 5:
        return "UNRELIABLE", {"reason": "비중첩 관측 부족"}
    spread = f12.get("topMinusBottomAnn")
    grad = f12.get("gradientCorr")
    top_gt_uni = (f12.get("topMinusUniverseAnn") or 0) > 0
    sub_signs = [s.get(12, {}).get("topMinusBottomAnn") for s in subs]
    sub_pos = sum(1 for x in sub_signs if x is not None and x > 0)
    sub_neg = sum(1 for x in sub_signs if x is not None and x < 0)
    size_pos = sum(1 for s in size_results if (s.get(12, {}).get("topMinusBottomAnn") or 0) > 0)
    ev = {"spread12Ann": spread, "gradientCorr": grad, "topBeatsUniverse": top_gt_uni,
          "subperiodsPositive": sub_pos, "subperiodsNegative": sub_neg,
          "sizeBucketsPositive": size_pos}
    if spread is None:
        return "UNRELIABLE", ev
    if spread > STRONG_SPREAD and sub_pos >= 3 and grad is not None and grad <= GRADIENT_CORR \
       and top_gt_uni and size_pos >= 2:
        return "STRONG_SIGNAL", ev
    if spread > 0 and sub_pos >= 3:
        return "PROMISING_SIGNAL", ev
    if spread < -NOISE_BAND and sub_neg >= 3:
        return "INVERTED_SIGNAL", ev
    if abs(spread) < NOISE_BAND:
        return "NO_SIGNAL", ev
    if spread > 0:
        return "WEAK_SIGNAL", ev
    return "NO_SIGNAL", ev
