#!/usr/bin/env python3
"""R26 회귀 — BM×SIZE 결합 incremental invariant. 네트워크 0.

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26

계층:
  L1 engine  — R24 엔진 동치 · 삼성 anchor · 자본행위 · 상폐 · 공짜 wealth 금지
  L2 def     — precommit · 결합 정의 하나 · 방향 동결 · 동일 eligibility/horizon
  L3 method  — 백분위 · 짝지음 incremental · 분위 · 부트스트랩 · 결정성
  L4 real    — 실제 산출물 · R25 정확 재현 · 판정 재현 · audit 전부
  L5 regress — R25~R16 산출물·판정 보존

사용: python scripts/research/test_r26_validation.py
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r26_analysis as R  # noqa: E402
import r26_verdict as V  # noqa: E402
from r25_engine import R25Engine  # noqa: E402
from r26_precommit import (BOOTSTRAP, COST, ELIGIBILITY, FORBIDDEN,  # noqa: E402
                           HORIZONS, LIQUIDITY, PRICE_BUCKETS, QUALIFICATION,
                           SECTOR, SUBPERIODS)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PASS = FAIL = 0
_K = None
PH = HORIZONS["primary"]


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(n):
    p = RD / f"r26-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def K():
    global _K
    if _K is None:
        _K = R.Combo()
    return _K


DS = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def eng_of(rows, dps=None, ov=None):
    cap = {d: {"X": {"close": c, "shares": s,
                     "dps": (dps[i] if dps else None)}}
           for i, (d, (c, s)) in enumerate(zip(DS[:len(rows)], rows))}
    return R25Engine(DS[:len(rows)], cap=cap, overrides=ov or {})


# ═══════════════════ L1 engine ═══════════════════
def t_l1():
    print("[L1] canonical TSR 엔진")
    ck("10:1 분할 wealth 불변",
       abs(eng_of([(100000, 100)] + [(10000, 1000)] * 4).tsr(0, 2, "X")) < 1e-12)
    ck("역분할 wealth 불변",
       abs(eng_of([(10000, 1000)] + [(100000, 100)] * 4).tsr(0, 2, "X")) < 1e-12)
    ck("무상증자 wealth 불변",
       abs(eng_of([(10000, 100)] + [(5000, 200)] * 4).tsr(0, 2, "X")) < 1e-12)
    e = eng_of([(1000, 100)] * 5, dps=[None, 120.0, None, None, None])
    ck("배당 반영", abs(e.tsr(0, 1, "X") - 0.01) < 1e-12)
    ov = {("X", DS[1]): {"kind": "RIGHTS", "ratio": 0.5, "issuePrice": 1000.0,
                         "label": "CONFIRMED_RIGHTS"}}
    e2 = eng_of([(3000, 100)] + [(2000, 150)] * 4, ov=ov)
    ck("유상증자 외부납입 중립화",
       abs(e2.tsr(0, 1, "X") - ((1.5 * 2000 - 500) / 3000 - 1)) < 1e-12)
    ck("공짜 wealth 없음", e2.tsr(0, 1, "X") < 0)
    ov2 = {("X", DS[1]): {"kind": "RIGHTS", "ratio": 0.5, "issuePrice": 5000.0,
                          "label": "CONFIRMED_RIGHTS"}}
    ck("합리적 실권",
       abs(eng_of([(3000, 100)] + [(2000, 150)] * 4, ov=ov2).tsr(0, 1, "X")
           - (2000 / 3000 - 1)) < 1e-12)
    cap = {DS[0]: {"X": {"close": 1000.0, "shares": 100.0, "dps": None}},
           DS[1]: {"X": {"close": 500.0, "shares": 100.0, "dps": None}},
           DS[2]: {}, DS[3]: {}}
    e3 = R25Engine(DS[:4], cap=cap, overrides={})
    ck("상폐 제외 금지", e3.tsr(0, 3, "X") is not None)
    ck("상폐 마지막 관측가 청산", abs(e3.tsr(0, 3, "X") + 0.5) < 1e-12)

    k = K()
    rw = W.RightsWealth(k.eng.eng, [])
    rw.ov = k.eng.ov
    tick = sorted({t for d in k.dates for t in k.eng.cap.get(d, {})})
    n = ok = 0
    worst = 0.0
    for t in tick[::53]:
        idx = [i for i in range(len(k.dates)) if k.eng.price(i, t)]
        if len(idx) < 14:
            continue
        i, j = idx[0], idx[0] + 12
        if j >= len(k.dates) or k.eng.price(j, t) is None:
            continue
        a, b = k.eng.tsr(i, j, t), rw.run(t, i, j, reinvest=True)
        if a is None or b is None:
            continue
        n += 1
        worst = max(worst, abs(a - b["twr"]))
        ok += abs(a - b["twr"]) < 1e-9
    ck(f"R24 엔진 동치 {n}건", n > 15 and ok == n, (ok, n, worst))
    ck("동치 최대오차 < 1e-9", worst < 1e-9, worst)
    i, j = k.pos.get("2010-01-04"), k.pos.get("2021-08-02")
    ck("삼성 anchor 478.52% 재현",
       abs(k.eng.tsr(i, j, "005930") * 100 - 478.52) < 0.05,
       k.eng.tsr(i, j, "005930") * 100)


# ═══════════════════ L2 정의 ═══════════════════
def t_l2():
    print("[L2] precommit · 결합 정의")
    pc, cd = L("precommit"), L("combination-definition")
    ck("precommit 존재", pc is not None)
    ck("결합정의 산출물 존재", cd is not None)
    if pc:
        ck("§3 결과 보기 전 작성", pc["writtenBeforeResults"] is True)
        ck("§4 BM = 1/PBR", pc["bmDefinition"]["formula"] == "1 / PBR")
        ck("§4 BM 방향 +1", pc["bmDefinition"]["direction"] == +1)
        ck("§5 SIZE = -ln(marketCap)",
           pc["sizeDefinition"]["formula"] == "-ln(marketCap)")
        ck("§5 SIZE 방향 +1", pc["sizeDefinition"]["direction"] == +1)
        ck("§6 결합 id", pc["combination"]["id"] == "DOUBLE_RANK_EQUAL_WEIGHT")
        ck("§6 가중치 0.5/0.5",
           pc["combination"]["weights"] == {"BM": 0.5, "SIZE_SMALL": 0.5})
        ck("§9 primary horizon 사전 고정", pc["horizons"]["primary"] == 36)
        ck("§9 primary 근거 기록", len(pc["horizons"]["primaryWhy"]) > 30)
        ck("§8 세 팔", pc["comparison"]["arms"] == ["BM_ALONE", "SIZE_ALONE", "COMBO"])
        ck("§8 공유 universe", "동시에" in pc["comparison"]["sharedUniverse"])
        ck("§12 부분기간 4개", len(pc["subperiods"]) == 4)
        ck("§14 부트스트랩 2방법", len(pc["bootstrap"]["methods"]) == 2)
        ck("§14 seed 고정", pc["bootstrap"]["seed"] == 20260822)
        ck("§19 유동성 데이터 불완전 선언",
           pc["liquidity"]["status"] == "LIQUIDITY_DATA_INCOMPLETE")
        ck("§25 섹터 데이터 없음 선언",
           pc["sector"]["status"] == "SECTOR_DATA_UNAVAILABLE")
        ck("§21 가격 구간 4개", len(pc["priceBuckets"]) == 4)
        ck("§28 자격기준 4종",
           all(k in pc["qualification"] for k in
               ("INCREMENTAL_COMBINATION_STRONG",
                "INCREMENTAL_COMBINATION_PROMISING",
                "COMBINATION_FRAGILE", "NO_INCREMENTAL_COMBINATION")))
        ck("§2 weight search 금지 명시",
           any("weight search" in x for x in pc["forbidden"]))
        ck("§7 intersection 동시경쟁 금지",
           any("intersection" in x for x in pc["forbidden"]))
        ck("§33 EY 확장 금지", any("EY" in x for x in pc["forbidden"]))
        ck("§34 Quality 재추가 금지", any("Quality" in x for x in pc["forbidden"]))
        ck("§31 포트폴리오 설계 금지",
           any("포트폴리오" in x for x in pc["forbidden"]))
        ck("§41 실패조건 정의", len(pc["failureConditions"]) >= 5)
    if cd:
        ck("결합은 하나뿐", cd["onlyOneCombination"] is True)
        ck("rescue 금지 문구", "사후 조정하지 않는다" in cd["noParameterRescue"])

    ck("§ELIGIBILITY 최소 100", ELIGIBILITY["minUniversePerMonth"] == 100)
    ck("§COST 고비용 100bp", COST["high"]["roundTripBps"] == 100)
    ck("§부분기간 이름 R25 와 동일",
       [s["name"] for s in SUBPERIODS] ==
       ["2007-2011", "2012-2016", "2017-2021", "2022-2026"])


# ═══════════════════ L3 방법론 ═══════════════════
def t_l3():
    print("[L3] 방법론")
    # 백분위 방향
    r = R.pct_rank({"A": 1.0, "B": 2.0, "C": 3.0})
    ck("백분위 최대값이 1", r["C"] == 1.0)
    ck("백분위 최소값이 0", r["A"] == 0.0)
    ck("백분위 단조", r["A"] < r["B"] < r["C"])
    tie = R.pct_rank({"B": 1.0, "A": 1.0})
    ck("동점은 ticker 오름차순 결정적", tie["A"] < tie["B"])
    ck("단일 원소 안전", R.pct_rank({"A": 1.0}) == {"A": 0.5})

    # 결합 공식
    k = K()
    d = sorted(k.scores)[100]
    s = k.scores[d]
    t0 = next(iter(s))
    ck("COMBO = 0.5·BM랭크 + 0.5·SIZE랭크",
       abs(s[t0]["COMBO"] - (0.5 * s[t0]["bmRank"] + 0.5 * s[t0]["sizeRank"]))
       < 1e-12)
    ck("COMBO 범위 0~1", all(0.0 <= x["COMBO"] <= 1.0 for x in s.values()))
    ck("공유 universe: 모든 종목이 BM·SIZE 둘 다 보유",
       all("BM" in (k.fv[d].get(t) or {}) and "SIZE_SMALL" in (k.fv[d].get(t) or {})
           for t in s))

    # 세 팔 동일 eligibility (§8)
    i = k.pos[d]
    cs = {a: k.cohort(a, i, PH) for a in R.ARMS}
    if all(cs.values()):
        ck("§8 세 팔 eligible 동일",
           len({c["eligible"] for c in cs.values()}) == 1,
           {a: c["eligible"] for a, c in cs.items()})
        ck("§8 세 팔 분위수 동일",
           len({c["quantiles"] for c in cs.values()}) == 1)
        ck("§8 세 팔 universe 수익 동일",
           len({round(c["universeAnn"], 12) for c in cs.values()}) == 1)
        ck("§8 세 팔 horizon 동일",
           len({c["months"] for c in cs.values()}) == 1)

    # 짝지음
    rows = k.paired(PH)
    ck("짝지음 코호트 생성", len(rows) > 100)
    ck("짝지음은 같은 시작월",
       all(r["BM_ALONE"]["startDate"] == r["COMBO"]["startDate"] for r in rows))
    man = statistics.fmean([r["COMBO"]["topAnn"] - r["BM_ALONE"]["topAnn"]
                            for r in rows]) * 100
    ck("incremental = 코호트별 차이의 평균",
       abs(R.incr(rows, "COMBO", "BM_ALONE")["meanPct"] - round(man, 3)) < 0.001)

    # 부트스트랩 결정성
    xs = [0.01 * ((i * 7) % 11 - 5) for i in range(120)]
    b1 = R.boot_summary(R.moving_block(xs, BOOTSTRAP["seed"]), xs)
    b2 = R.boot_summary(R.moving_block(xs, BOOTSTRAP["seed"]), xs)
    ck("moving-block 결정적", b1 == b2)
    ck("CI 하한 <= 평균 <= 상한",
       b1["ci95LowPct"] <= b1["meanPct"] <= b1["ci95HighPct"])
    ck("P(>0)/P(>1)/P(>2) 단조",
       b1["pExcessAbove0"] >= b1["pExcessAbove1pp"] >= b1["pExcessAbove2pp"])
    ck("표본 부족 시 None", R.moving_block([0.1] * 5, 1) is None)
    rws = [{"startDate": f"{2007 + i // 12}-{i % 12 + 1:02d}-01"}
           for i in range(120)]
    y1 = R.year_level(rws, xs, BOOTSTRAP["seed"])
    y2 = R.year_level(rws, xs, BOOTSTRAP["seed"])
    ck("year-level 결정적", y1 == y2)

    ck("비겹침 코호트 수 < 겹침", len(R.non_overlapping(rows, PH)) < len(rows))


# ═══════════════════ L4 실제 산출물 ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    base, q, h = L("base-reproduction"), L("quantile-results"), L("horizon-results")
    inc, sub, roll = L("incremental-alpha"), L("subperiod-results"), L("rolling-cohorts")
    boot, cond = L("bootstrap"), L("conditional-attribution")
    lq, dist = L("liquidity"), L("distress-delisting")
    conc, lim = L("concentration"), L("tsr-limitation")
    ec, v = L("exchange-and-cost"), L("verdict")
    for n, o in (("base-reproduction", base), ("quantile", q), ("horizon", h),
                 ("incremental", inc), ("subperiod", sub), ("rolling", roll),
                 ("bootstrap", boot), ("conditional", cond), ("liquidity", lq),
                 ("distress", dist), ("concentration", conc),
                 ("tsr-limitation", lim), ("exchange/cost", ec), ("verdict", v)):
        ck(f"{n} 산출물 존재", o is not None)
    if not all((base, q, h, inc, sub, roll, boot, cond, lq, dist, conc, lim, ec, v)):
        return

    # R25 정확 재현
    ck("§37 R25 전부 정확 재현", base["allExactMatch"] is True)
    for fac, hh in base["r25ExactReproduction"].items():
        for kk, x in hh.items():
            ck(f"R25 재현 {fac} {kk}", x["exactMatch"] is True,
               (x["r25SpreadAnnPct"], x["r26SpreadAnnPct"]))

    ck("엔진 기록", h["engine"] == "CANONICAL_TSR_R24")
    ck("primary horizon 36M", h["primaryHorizon"] == 36)
    for kk in ("12M", "36M", "60M"):
        a = h["byHorizon"].get(kk)
        ck(f"{kk} 세 팔 결과", a is not None and all(a[x] for x in R.ARMS))
        if a:
            ck(f"{kk} 코호트 >= 100", a["COMBO"]["cohorts"] >= 100)
        c = inc["byHorizon"].get(kk)
        ck(f"{kk} incremental vs BM", c and c["incremental_vs_BM"] is not None)
        ck(f"{kk} incremental vs SIZE", c and c["incremental_vs_SIZE"] is not None)
        ck(f"{kk} incremental vs universe",
           c and c["incremental_vs_UNIVERSE"] is not None)
        ck(f"{kk} spread 보조지표", c and c["spreadIncremental_vs_BM"] is not None)

    for a, x in q["byArm"].items():
        ck(f"{a} 분위 사다리", x["monotonicity"] is not None
           and len(x["quantileAnnPct"]) == x["quantiles"])
    ck("§11 최상위만 튐 판정 기록",
       all("topOnlySpike" in x for x in q["byArm"].values()))

    for kk in ("vsBM", "vsSIZE"):
        ck(f"§12 부분기간 {kk} 4구간", sub[kk][f"{PH}M"]["total"] == 4)
        ck(f"§13 롤링 {kk} 겹침",
           roll["byHorizon"][f"{PH}M"]["overlapping"][kk] is not None)
        ck(f"§13 롤링 {kk} 비겹침",
           roll["byHorizon"][f"{PH}M"]["nonOverlapping"][kk] is not None)
        x = roll["byHorizon"][f"{PH}M"]["overlapping"][kk]
        ck(f"§13 {kk} p10/p25/p75/p90/worst/best",
           all(x.get(y) is not None for y in
               ("p10Pct", "p25Pct", "p75Pct", "p90Pct", "worstPct", "bestPct")))

    for tgt in ("COMBO_minus_BM", "COMBO_minus_SIZE"):
        for meth in ("movingBlock", "yearLevel"):
            b = boot["byHorizon"][f"{PH}M"][tgt][meth]
            ck(f"§14 {tgt} {meth}", b is not None and b["ci95LowPct"] is not None)
            if b:
                ck(f"§14 {tgt} {meth} P(>0)/P(>1)/P(>2)",
                   all(b.get(y) is not None for y in
                       ("pExcessAbove0", "pExcessAbove1pp", "pExcessAbove2pp")))

    ck("§15 독립성 상관 기록",
       cond["independence"]["corrComboBmRank"] is not None
       and cond["independence"]["corrComboSizeRank"] is not None)
    ck("§15 한쪽 지배 판정", "dominatedByOneFactor" in cond["independence"])
    ck("§17 크기층 3개", len(cond["conditional"]["bmWithinSize"]) == 3)
    ck("§18 BM층 3개", len(cond["conditional"]["sizeWithinBm"]) == 3)
    ck("§17 크기층 전부 결과 있음",
       all(x["bmSpreadAnnPct"] is not None
           for x in cond["conditional"]["bmWithinSize"].values()))
    ck("§18 BM층 전부 결과 있음",
       all(x["sizeSpreadAnnPct"] is not None
           for x in cond["conditional"]["sizeWithinBm"].values()))

    ck("§16 KOSPI/KOSDAQ 둘 다",
       ec["exchange"]["KOSPI"]["vsBM"] is not None
       and ec["exchange"]["KOSDAQ"]["vsBM"] is not None)
    ck("§16 KOSDAQ 의존 판정", "kosdaqDependent" in ec["exchange"])
    ck("§27 세 팔 회전율", all(ec["cost"][a]["annualTurnover"] is not None
                          for a in R.ARMS))
    ck("§27 고비용 후 incremental", ec["cost"].get("incrementalAfterHighCost"))

    ck("§19 유동성 상태 선언", lq["status"] == "LIQUIDITY_DATA_INCOMPLETE")
    ck("§19 없는 데이터 명시", len(lq["unavailable"]) >= 3)
    ck("§25 섹터 없음 선언", lq["sector"]["status"] == "SECTOR_DATA_UNAVAILABLE")
    ck("§20 5천만원 기준", lq["notionalCapitalKrw"] == 50_000_000)
    for a in R.ARMS:
        x = lq["byArm"][a]
        ck(f"§19 {a} 중앙주가·중앙시총",
           x["medianClose"] is not None and x["medianMarketCapKrw"] is not None)
        ck(f"§21 {a} 가격구간 4개", len(x["priceBucketPct"]) == 4)
        ck(f"§21 {a} 가격구간 합 100",
           abs(sum(x["priceBucketPct"].values()) - 100.0) < 1.0,
           sum(x["priceBucketPct"].values()))

    for a in R.ARMS + ["UNIVERSE"]:
        x = dist["byArm"][a]
        ck(f"§22 {a} 상폐율", x["delistingRatePct"] is not None)
        ck(f"§23 {a} 부실 지표",
           all(x.get(y) is not None for y in
               ("lowPriceRatePct", "extremeBmRatePct", "persistentLossRatePct",
                "negativeEpsRatePct")))
    ck("§22 상폐 semantics 명시", "UNKNOWN_RECOVERY" in dist["delistingSemantics"])
    ck("§22 universe 대비 비교 가능",
       dist["byArm"]["COMBO"]["delistingRatePct"]
       > dist["byArm"]["UNIVERSE"]["delistingRatePct"])

    for a in R.ARMS:
        x = conc["byArm"][a]
        ck(f"§24 {a} top1/3/5/10",
           all(x.get(y) is not None for y in
               ("top1Pct", "top3Pct", "top5Pct", "top10Pct")))
    for kk in ("removeTop1", "removeTop3", "removeTop5", "removeTop10"):
        ck(f"§24 {kk} 제거 후 재계산",
           conc["removal"][kk]["vsBM"] is not None
           and conc["removal"][kk]["vsSIZE"] is not None)
    ck("§24 부호반전 판정 기록", "signFlipsWhenTop3Removed" in conc)

    ck("§26 네 arm 미해결률",
       all(lim["byArm"][a]["unresolvedRatePct"] is not None
           for a in R.ARMS + ["UNIVERSE"]))
    ck("§26 노출 판정", "exposed" in lim)

    # 판정 재현 + 기준 불변
    ck("§28 기준 불변", v["thresholdsUnchanged"] is True)
    ck("§28 기준이 precommit 그대로",
       v["qualificationFromPrecommit"]["primaryHorizon"]
       == QUALIFICATION["primaryHorizon"])
    ck("§28 판정이 4종 중 하나",
       v["verdict"] in ("INCREMENTAL_COMBINATION_STRONG",
                        "INCREMENTAL_COMBINATION_PROMISING",
                        "COMBINATION_FRAGILE", "NO_INCREMENTAL_COMBINATION"))
    p = inc["byHorizon"][f"{PH}M"]
    ck("판정 근거가 실측과 일치 (vsBM)",
       v["incrementalPrimary"]["vsBM"] == p["incremental_vs_BM"]["meanPct"])
    ck("판정 근거가 실측과 일치 (vsSIZE)",
       v["incrementalPrimary"]["vsSIZE"] == p["incremental_vs_SIZE"]["meanPct"])
    ck("betterThanBmAlone 일치",
       v["betterThanBmAlone"] == (v["incrementalPrimary"]["vsBM"] > 0))
    ck("betterThanSizeAlone 일치",
       v["betterThanSizeAlone"] == (v["incrementalPrimary"]["vsSIZE"] > 0))
    if v["verdict"] == "NO_INCREMENTAL_COMBINATION":
        ck("§29·§30 두 단독 중 하나라도 못 이김",
           v["incrementalPrimary"]["vsBM"] <= 0
           or v["incrementalPrimary"]["vsSIZE"] <= 0)
        ck("§29 단독 factor 추천", v["recommendedSingleFactor"] in ("BM", "SIZE_SMALL"))
        ck("§31 포트폴리오 연구 가치 False",
           v["portfolioResearchWorthwhile"] is False)
    ck("§2 가중치 탐색 안 함", v["weightSearchDone"] is False)
    ck("§7 intersection 시험 안 함", v["intersectionTested"] is False)
    ck("§31 포트폴리오 설계 안 함", v["portfolioSearchDone"] is False)
    ck("§30 억지 결합 금지 문구", "억지" not in v["noForcedCombination"]
       or "evidence" in v["noForcedCombination"])

    # 결정적 재현
    k = K()
    rows = k.paired(PH)
    ck("결정적 재현 — COMBO vs BM",
       abs(R.incr(rows, "COMBO", "BM_ALONE")["meanPct"]
           - p["incremental_vs_BM"]["meanPct"]) < 1e-9)
    ck("결정적 재현 — COMBO vs SIZE",
       abs(R.incr(rows, "COMBO", "SIZE_ALONE")["meanPct"]
           - p["incremental_vs_SIZE"]["meanPct"]) < 1e-9)
    ck("결정적 재현 — 코호트 수",
       len(rows) == h["byHorizon"][f"{PH}M"]["COMBO"]["cohorts"])
    xs = R.series(rows, "COMBO", "BM_ALONE")
    ck("결정적 재현 — 부트스트랩",
       R.boot_summary(R.moving_block(xs, BOOTSTRAP["seed"]), xs)["ci95LowPct"]
       == boot["byHorizon"][f"{PH}M"]["COMBO_minus_BM"]["movingBlock"]["ci95LowPct"])


# ═══════════════════ L5 회귀 ═══════════════════
def t_l5():
    print("[L5] R25~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]

    def syn(rows):
        return {d: {"X": {"close": c, "shares": s, "dps": None}}
                for d, (c, s) in zip(ds, rows)}
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=syn(rows))
        ck(f"R16 {nm} wealth 불변",
           abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    for f in ("r16-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r23-foundation-verdict-latest.json",
              "r24-foundation-verdict-latest.json",
              "r24-full-reconciliation-latest.json",
              "r25-verdict-latest.json",
              "r25-factor-scorecard-latest.json",
              "r25-horizon-results-latest.json",
              "r15-samsung-tsr-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())

    r24 = json.loads((RD / "r24-foundation-verdict-latest.json")
                     .read_text(encoding="utf-8"))
    ck("R24 판정 보존",
       r24["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")
    ck("R24 편향 보존", r24["expectedBiasPct"] == 1.734)
    r25 = json.loads((RD / "r25-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R25 PRIMARY 보존", set(r25["PRIMARY_FACTOR"]) == {"BM", "SIZE_SMALL"})
    ck("R25 SECONDARY 보존", r25["SECONDARY_FACTOR"] == ["EY"])
    r25h = json.loads((RD / "r25-horizon-results-latest.json")
                      .read_text(encoding="utf-8"))["byFactor"]
    ck("R25 BM 36M 보존", r25h["BM"]["36M"]["meanSpreadAnnPct"] == 14.141)
    ck("R25 SIZE 36M 보존", r25h["SIZE_SMALL"]["36M"]["meanSpreadAnnPct"] == 13.32)

    for f in ("wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-final-27-holder-rights-terms-recovery-r22-latest.md",
              "wababa-final-11-holder-rights-allocation-recovery-r23-latest.md",
              "wababa-final-unknown-50-direct-classification-r24-latest.md",
              "wababa-canonical-factor-rediscovery-r25-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-bm-size-incremental-combination-r26-latest.json"
    ck("R26 보고서 JSON 존재", j.exists())
    if j.exists():
        d = json.loads(j.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure"):
            ck(f"{k} = 0", p[k] == 0)
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish", "canonicalProduction"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED",
           p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 scheduler 0", d["hotg"]["newScheduler"] == 0)
        ck("HOTG 신규 orchestration 0", d["hotg"]["newOrchestration"] == 0)
    m = WD / "wababa-bm-size-incremental-combination-r26-latest.md"
    ck("R26 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§42)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§42)", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("R25 정확 재현", "결합 정의", "incremental alpha",
                     "조건부 귀속", "부트스트랩", "유동성", "상폐",
                     "집중도", "비용 스트레스", "TSR 한계",
                     "LIQUIDITY_DATA_INCOMPLETE", "SECTOR_DATA_UNAVAILABLE",
                     "REAL_MONEY_NOT_APPROVED", "Founder 행동",
                     "다음 단일 작업", "상품성"):
            ck(f"§43 보고 항목: {need}", need in body)
        ck("수익보장 표현 없음",
           "보장" not in body or "수익을 보장하지 않는다" in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_prod):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
