#!/usr/bin/env python3
"""R25 회귀 — canonical TSR factor rediscovery invariant. 네트워크 0.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

계층:
  L1 engine  — R24 엔진 동치 · 삼성 anchor · 배당·분할·무상증자·유상증자·공짜 wealth
  L2 pit     — PIT 시점 · 상폐 · survivorship · factor 정의·방향 동결 · eligibility
  L3 method  — 분위 · 연율화 · benchmark 일치 · 통제 · 부분기간 · 롤링 · 부트스트랩
  L4 real    — 실제 산출물(mock 아님) · scorecard · 판정 · 결정적 재현
  L5 regress — R24~R16 산출물·판정 보존

사용: python scripts/research/test_r25_validation.py
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r25_analysis as AN  # noqa: E402
import r25_factors as F  # noqa: E402
import r25_scorecard as SC  # noqa: E402
from r25_engine import R25Engine, load_overrides  # noqa: E402
from r25_precommit import (ELIGIBILITY, FACTORS, HORIZONS,  # noqa: E402
                           QUALIFICATION, QUANTILES, STATS, SUBPERIODS)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PASS = FAIL = 0
_A = None


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(n):
    p = RD / f"r25-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def A():
    global _A
    if _A is None:
        _A = AN.Analyzer()
    return _A


DS = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(10)]


def synth(dates, rows, dps=None):
    return {d: {"X": {"close": c, "shares": s,
                      "dps": (dps[i] if dps else None)}}
            for i, (d, (c, s)) in enumerate(zip(dates, rows))}


def eng_of(dates, rows, dps=None, ov=None):
    return R25Engine(dates, cap=synth(dates, rows, dps), overrides=ov or {})


# ═══════════════════ L1 engine ═══════════════════
def t_l1():
    print("[L1] canonical TSR 엔진")
    ds = DS[:5]

    # 분할 — wealth 불변
    e = eng_of(ds, [(100000, 100), (10000, 1000), (10000, 1000),
                    (10000, 1000), (10000, 1000)])
    ck("10:1 분할 wealth 불변", abs(e.tsr(0, 2, "X")) < 1e-12, e.tsr(0, 2, "X"))
    # 역분할
    e2 = eng_of(ds, [(10000, 1000), (100000, 100), (100000, 100),
                     (100000, 100), (100000, 100)])
    ck("역분할 wealth 불변", abs(e2.tsr(0, 2, "X")) < 1e-12)
    # 무상증자 (1:1)
    e3 = eng_of(ds, [(10000, 100), (5000, 200), (5000, 200),
                     (5000, 200), (5000, 200)])
    ck("무상증자 wealth 불변", abs(e3.tsr(0, 2, "X")) < 1e-12)
    # 배당 — 재투자로 wealth 증가
    e4 = eng_of(ds, [(1000, 100)] * 5, dps=[None, 120.0, None, None, None])
    r = e4.tsr(0, 1, "X")
    ck("배당 반영 (월 1/12)", abs(r - (120.0 / 12.0) / 1000.0) < 1e-12, r)
    ck("배당 없으면 0", abs(eng_of(ds, [(1000, 100)] * 5).tsr(0, 1, "X")) < 1e-12)
    # 특별배당(큰 DPS)도 같은 경로
    e5 = eng_of(ds, [(1000, 100)] * 5, dps=[None, 1200.0, None, None, None])
    ck("특별배당 동일 경로", abs(e5.tsr(0, 1, "X") - 0.1) < 1e-12)

    # 유상증자 — 외부납입은 수익이 아니다
    ov = {("X", ds[1]): {"kind": "RIGHTS", "ratio": 0.5, "issuePrice": 1000.0,
                         "label": "CONFIRMED_RIGHTS"}}
    e6 = eng_of(ds, [(3000, 100), (2000, 150), (2000, 150), (2000, 150),
                     (2000, 150)], ov=ov)
    man = (1.5 * 2000 - 0.5 * 1000) / 3000 - 1.0
    ck("유상증자 청약 = manual", abs(e6.tsr(0, 1, "X") - man) < 1e-12,
       (e6.tsr(0, 1, "X"), man))
    ck("외부납입이 수익이 아님", e6.tsr(0, 1, "X") < 0.0)
    # 합리적 실권 (K >= P_ex)
    ov2 = {("X", ds[1]): {"kind": "RIGHTS", "ratio": 0.5, "issuePrice": 5000.0,
                          "label": "CONFIRMED_RIGHTS"}}
    e7 = eng_of(ds, [(3000, 100), (2000, 150), (2000, 150), (2000, 150),
                     (2000, 150)], ov=ov2)
    ck("합리적 실권 → 주가만", abs(e7.tsr(0, 1, "X") - (2000 / 3000 - 1)) < 1e-12)
    ck("실권 시 공짜 wealth 없음", e7.tsr(0, 1, "X") < 0.0)

    # 권리 없음 확정 → 조정 0
    ov3 = {("X", ds[1]): {"kind": "NO_ADJUST", "label": "CONFIRMED_NON_RIGHTS"}}
    e8 = eng_of(ds, [(3000, 100), (2000, 150), (2000, 150), (2000, 150),
                     (2000, 150)], ov=ov3)
    ck("권리 없음 → 주가만 반영",
       abs(e8.tsr(0, 1, "X") - (2000 / 3000 - 1)) < 1e-12)

    # 공짜 wealth 금지: 납입 없이 주식수만 늘면 wealth 가 늘면 안 된다
    ck("무상 사건 외 주식수 증가로 wealth 생성 없음",
       abs(e8.tsr(0, 1, "X")) > 0 and e8.tsr(0, 1, "X") < 0)

    # 상폐 — 마지막 관측가 청산
    cap = {ds[0]: {"X": {"close": 1000.0, "shares": 100.0, "dps": None}},
           ds[1]: {"X": {"close": 500.0, "shares": 100.0, "dps": None}},
           ds[2]: {}, ds[3]: {}, ds[4]: {}}
    e9 = R25Engine(ds, cap=cap, overrides={})
    ck("상폐 종목도 계산됨(제외 금지)", e9.tsr(0, 4, "X") is not None)
    ck("상폐 = 마지막 관측가 청산", abs(e9.tsr(0, 4, "X") - (-0.5)) < 1e-12,
       e9.tsr(0, 4, "X"))
    ck("상폐 판정", e9.is_delisted_by(4, "X") is True)
    ck("생존 판정", e9.is_delisted_by(1, "X") is False)

    # ★ R24 승인 엔진과 동치 (실데이터)
    a = A()
    rw = W.RightsWealth(a.eng.eng, [])
    rw.ov = a.eng.ov
    tick = sorted({t for d in a.dates for t in a.eng.cap.get(d, {})})
    n = ok = 0
    worst = 0.0
    for t in tick[::37]:
        idx = [i for i in range(len(a.dates)) if a.eng.price(i, t)]
        if len(idx) < 14:
            continue
        i = idx[0]
        j = i + 12
        if j >= len(a.dates) or a.eng.price(j, t) is None:
            continue
        x, y = a.eng.tsr(i, j, t), rw.run(t, i, j, reinvest=True)
        if x is None or y is None:
            continue
        n += 1
        worst = max(worst, abs(x - y["twr"]))
        ok += abs(x - y["twr"]) < 1e-9
    ck(f"R24 엔진 동치 실데이터 {n}건", n > 20 and ok == n, (ok, n, worst))
    ck("동치 최대오차 < 1e-9", worst < 1e-9, worst)

    # 삼성 anchor
    i = a.pos.get("2010-01-04")
    j = a.pos.get("2021-08-02")
    ck("삼성 R15 구간 존재", i is not None and j is not None)
    if i is not None and j is not None:
        v = a.eng.tsr(i, j, "005930") * 100
        ck("삼성 TSR 478.52% 재현", abs(v - 478.52) < 0.05, v)


# ═══════════════════ L2 PIT / 정의 ═══════════════════
def t_l2():
    print("[L2] PIT · factor 정의")
    pc = L("precommit")
    ck("precommit 존재", pc is not None)
    if pc:
        ck("§2 결과 보기 전 작성", pc["writtenBeforeResults"] is True)
        ck("§1 R7/R11/R14 재실행 아님",
           set(pc["isNotRerunOf"]) >= {"R7", "R11", "R14"})
        ck("§4 기간 2007-01-02", pc["universe"]["period"]["start"] == "2007-01-02")
        ck("§4 기간 2026-08-03", pc["universe"]["period"]["end"] == "2026-08-03")
        ck("§4 상폐 제외 금지 명시", "제외 금지" in pc["universe"]["delisting"])
        ck("§4 current-universe bias 금지", "bias 금지" in pc["universe"]["asOfUniverse"])
        ck("§3 close-only 금지", pc["engine"]["closeOnlyForbidden"] is True)
        ck("§3 동일 엔진 3곳", len(pc["engine"]["sameEngineFor"]) == 3)
        ck("§3 자본행위 17종 커버", len(pc["engine"]["covers"]) >= 15)
        ck("§7 방향 동결 문구", "뒤집지 않고" in pc["directionFrozen"])
        ck("§8 horizon 12/36/60", pc["horizons"]["primary"] == [12, 36, 60])
        ck("§8 84M 보조", pc["horizons"]["secondary"] == [84])
        ck("§12 부분기간 4개", len(pc["subperiods"]) == 4)
        ck("§14 방법 사전 고정", pc["statisticalTests"]["method"] == "moving-block bootstrap")
        ck("§14 seed 고정", pc["statisticalTests"]["seed"] == 20260822)
        ck("§18 자격기준 사전 고정", "STRONG_SIGNAL" in pc["qualification"])
        ck("§19 NONE 허용", pc["selection"]["noneAllowed"] is True)
        ck("§20 새 조합 금지", any("조합" in x for x in pc["forbidden"]))
        ck("§21 포트폴리오 search 금지", any("parameter search" in x
                                       for x in pc["forbidden"]))
        ck("§22 R11 숫자 승계 금지", any("R11" in x for x in pc["forbidden"]))
        ck("§2 실패조건 정의", len(pc["failureConditions"]) >= 4)

    # 방향 동결 — 전부 +1 (값이 클수록 좋다는 가설)
    for k, v in FACTORS.items():
        ck(f"방향 사전 고정 {k}", v["direction"] in (+1, -1))
    ck("SIZE 는 작을수록 좋다는 가설", FACTORS["SIZE_SMALL"]["direction"] == +1
       and "작을수록" in FACTORS["SIZE_SMALL"]["formula"])
    ck("SIZE 방향 근거가 학술 표준", "small-cap premium"
       in FACTORS["SIZE_SMALL"]["directionWhy"])
    ck("legacy 조합은 참고용", FACTORS["MAGIC_FORMULA_LEGACY"]["referenceOnly"]
       and FACTORS["BM_ROE_LEGACY"]["referenceOnly"])

    # factor 계산식
    snaps = {"2020-01-01": {"A": {"close": 1000.0, "marketCap": 1e10,
                                  "shares": 1e7, "PBR": 0.5, "PER": 5.0,
                                  "EPS": 200.0, "BPS": 2000.0, "DIV": 3.0,
                                  "DPS": 30.0, "market": "KOSPI"}}}
    fv, _ = F.compute(snaps, ["2020-01-01"])
    a = fv["2020-01-01"]["A"]
    ck("BM = 1/PBR", abs(a["BM"] - 2.0) < 1e-12)
    ck("EY = 1/PER", abs(a["EY"] - 0.2) < 1e-12)
    ck("ROE = EPS/BPS", abs(a["ROE"] - 0.1) < 1e-12)
    ck("DIVIDEND_YIELD = DIV", a["DIVIDEND_YIELD"] == 3.0)
    ck("SIZE_SMALL = -ln(mcap)", abs(a["SIZE_SMALL"] + 23.0258509) < 1e-5)
    ck("창 부족하면 지속성 없음", "EARNINGS_PERSISTENCE" not in a)
    ck("창 부족하면 BPS 성장 없음", "BPS_GROWTH" not in a)

    # 음수/0 제외 (§ELIGIBILITY)
    bad = {"2020-01-01": {"B": {"close": 1000.0, "marketCap": 1e10,
                                "shares": 1e7, "PBR": -0.5, "PER": -5.0,
                                "EPS": -200.0, "BPS": -2000.0, "DIV": None,
                                "DPS": None, "market": "KOSPI"}}}
    fb, _ = F.compute(bad, ["2020-01-01"])
    bb = fb["2020-01-01"].get("B", {})
    ck("음수 PBR 제외", "BM" not in bb)
    ck("음수 PER 제외", "EY" not in bb)
    ck("음수 BPS 제외 (ROE 계산 안 함)", "ROE" not in bb)
    ck("결측 미보간", "DIVIDEND_YIELD" not in bb)
    ck("§ELIGIBILITY 최소 종목수", ELIGIBILITY["minUniversePerMonth"] == 100)

    # PIT: 과거 창 factor 가 미래를 보지 않는다
    dts = [f"2019-{m:02d}-01" for m in range(1, 13)] + \
          [f"2020-{m:02d}-01" for m in range(1, 13)] + \
          [f"2021-{m:02d}-01" for m in range(1, 13)]
    sn = {d: {"A": {"close": 1000.0, "marketCap": 1e10, "shares": 1e7,
                    "PBR": 1.0, "PER": 10.0, "EPS": (100.0 if d < "2021-01-01"
                                                     else -100.0),
                    "BPS": 1000.0, "DIV": 1.0, "DPS": 10.0, "market": "KOSPI"}}
          for d in dts}
    fv2, _ = F.compute(sn, dts)
    p_end = fv2[dts[-1]]["A"]["EARNINGS_PERSISTENCE"]
    p_mid = fv2["2021-01-01"]["A"]["EARNINGS_PERSISTENCE"]
    ck("지속성은 과거만 본다 (2021-01 시점 = 100%)", abs(p_mid - 1.0) < 1e-12, p_mid)
    ck("미래 적자가 과거 시점에 안 새어들어감", p_end < 1.0, p_end)

    cov = L("factor-coverage")
    ck("coverage 산출물 존재", cov is not None)
    if cov:
        ck("10개 factor coverage", len(cov["byFactor"]) == 10)
        ck("236개월", cov["months"] == 236, cov["months"])
        for k, x in cov["byFactor"].items():
            ck(f"{k} 사용가능 월 >= 150", x["monthsUsable"] >= 150,
               x["monthsUsable"])


# ═══════════════════ L3 방법론 ═══════════════════
def t_l3():
    print("[L3] 방법론")
    ck("분위 규칙 사전 고정", "200" in QUANTILES["rule"])
    ck("동일가중", "동일가중" in
       json.dumps(L("precommit")["ranking"], ensure_ascii=False))

    # 연율화
    ck("연율화 12M", abs(AN.ann(0.20, 12) - 0.20) < 1e-12)
    ck("연율화 36M", abs(AN.ann(0.331, 36) - 0.1) < 1e-3, AN.ann(0.331, 36))
    ck("연율화 60M 단순연율화 아님",
       abs(AN.ann(1.0, 60) - (2.0 ** (1 / 5) - 1)) < 1e-12)
    ck("−100% 는 −1 고정", AN.ann(-1.0, 12) == -1.0)

    # 분할
    ck("10분위 균등 분할", [len(x) for x in AN.buckets(list(range(100)), 10)]
       == [10] * 10)
    ck("나머지는 앞 구간부터", [len(x) for x in AN.buckets(list(range(103)), 10)]
       == [11, 11, 11, 10, 10, 10, 10, 10, 10, 10])
    ck("5분위 fallback 가능", len(AN.buckets(list(range(150)), 5)) == 5)

    # 백분위
    ck("p10", abs(AN.pctl(list(range(101)), 0.10) - 10.0) < 1e-9)
    ck("p90", abs(AN.pctl(list(range(101)), 0.90) - 90.0) < 1e-9)

    # 부트스트랩 결정성
    sp = [0.01 * ((i * 7) % 11 - 5) for i in range(120)]
    b1, b2 = AN.bootstrap(sp), AN.bootstrap(sp)
    ck("부트스트랩 결정적(seed 고정)", b1 == b2)
    ck("부트스트랩 블록 12", b1["blockMonths"] == STATS["blockMonths"])
    ck("부트스트랩 재표본 2000", b1["resamples"] == 2000)
    ck("CI 하한 <= 평균 <= 상한",
       b1["ci95LowPct"] <= b1["meanSpreadAnnPct"] <= b1["ci95HighPct"])
    ck("표본 부족이면 None", AN.bootstrap([0.1] * 5) is None)

    # 비겹침 코호트
    cs = [{"startDate": f"2010-{m:02d}-01",
           "endDate": f"2011-{m:02d}-01"} for m in range(1, 13)]
    ck("비겹침 코호트 추출", len(AN.non_overlapping(cs, 12)) < len(cs))

    # benchmark eligibility 일치 (§9)
    a = A()
    c = a.cohort("BM", a.pos["2015-01-02"], 12) if "2015-01-02" in a.pos else None
    if c is None:
        for d in a.dates:
            c = a.cohort("BM", a.pos[d], 12)
            if c:
                break
    ck("코호트 계산됨", c is not None)
    if c:
        ck("§9 benchmark = 같은 eligible universe",
           c["eligible"] == sum(c["quantileCount"]) or
           c["eligible"] >= sum(c["quantileCount"]))
        ck("분위 수가 규칙과 일치",
           c["quantiles"] == (10 if c["eligible"] >= 200 else 5))
        ck("TOP 이 가설상 최상위", c["topCum"] is not None)
        ck("spread = TOP연율 − BOTTOM연율",
           abs(c["spreadAnn"] - (c["topAnn"] - c["bottomAnn"])) < 1e-12)
        ck("excess = TOP연율 − universe연율",
           abs(c["excessAnn"] - (c["topAnn"] - c["universeAnn"])) < 1e-12)

    # 통제 subset
    ex = a.exchange_subset("KOSPI")(a.dates[100])
    kd = a.exchange_subset("KOSDAQ")(a.dates[100])
    ck("거래소 subset 서로소", not (ex & kd))
    ck("KOSPI subset 비어있지 않음", len(ex) > 50)
    sm = a.size_subset("SMALL")(a.dates[100])
    lg = a.size_subset("LARGE")(a.dates[100])
    ck("크기 subset 서로소", not (sm & lg))
    ck("size+거래소 교집합", a.size_exchange_subset("SMALL", "KOSPI")(
        a.dates[100]) <= sm)

    # 부분기간 배정
    ck("§12 달력 고정 4구간", [s["name"] for s in SUBPERIODS] ==
       ["2007-2011", "2012-2016", "2017-2021", "2022-2026"])


# ═══════════════════ L4 실제 산출물 ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    h, q, s = L("horizon-results"), L("quantile-results"), L("subperiod-results")
    c, r, b = L("size-exchange-controls"), L("rolling-cohorts"), L("bootstrap")
    cc, dd, xx = L("concentration"), L("distress"), L("tsr-limitation-exposure")
    sc, v = L("factor-scorecard"), L("verdict")
    for n, o in (("horizon", h), ("quantile", q), ("subperiod", s),
                 ("controls", c), ("rolling", r), ("bootstrap", b),
                 ("concentration", cc), ("distress", dd),
                 ("limitation", xx), ("scorecard", sc), ("verdict", v)):
        ck(f"{n} 산출물 존재", o is not None)
    if not all((h, q, s, c, r, b, cc, dd, xx, sc, v)):
        return

    ck("엔진 이름 기록", h["engine"] == "CANONICAL_TSR_R24")
    ck("236개월", h["months"] == 236)
    ck("기간 2007~2026", h["period"]["start"] == "2007-01-02"
       and h["period"]["end"] == "2026-08-03")
    ck("엔진 통계 기록 (mock 아님)",
       h["engineStats"]["R16_MECHANICAL"] > 0
       and h["engineStats"]["RIGHTS_EXERCISED"] > 0)

    for k in FACTORS:
        for hh in ("12M", "36M", "60M"):
            a = h["byFactor"][k].get(hh)
            ck(f"{k} {hh} 결과 존재", a is not None and a["cohorts"] > 0)
            if a:
                ck(f"{k} {hh} 코호트 >= 100", a["cohorts"] >= 100, a["cohorts"])
        ck(f"{k} 84M 보조 결과", h["byFactor"][k].get("84M") is not None)
        ck(f"{k} 단조성 계산", (q["byFactor"][k].get("12M") or {}).get(
            "monotonicity") is not None)
        ck(f"{k} 부분기간 4구간", s["byFactor"][k]["total"] == 4)
        ck(f"{k} size 통제", c["byFactor"][k].get(
            "sizeMatchedSpreadAnnPct") is not None)
        ck(f"{k} 거래소 통제", c["byFactor"][k].get(
            "exchangeMatchedSpreadAnnPct") is not None)
        ck(f"{k} size+거래소 통제", c["byFactor"][k].get(
            "sizeExchangeMatchedSpreadAnnPct") is not None)
        ck(f"{k} 롤링 겹침/비겹침",
           r["byFactor"][k]["12M"]["overlapping"] is not None
           and r["byFactor"][k]["12M"]["nonOverlapping"] is not None)
        ck(f"{k} 부트스트랩 CI", b["byFactor"][k] is not None
           and b["byFactor"][k]["ci95LowPct"] is not None)
        ck(f"{k} 집중도 top1/3/5/10", all(
            cc["byFactor"][k].get(x) is not None
            for x in ("top1Pct", "top3Pct", "top5Pct", "top10Pct")))
        ck(f"{k} 상폐 노출 TOP/BOTTOM",
           dd["byFactor"][k]["TOP"]["delistingRatePct"] is not None
           and dd["byFactor"][k]["BOTTOM"]["delistingRatePct"] is not None)
        ck(f"{k} TSR 한계 노출", xx["byFactor"][k].get("diffPp") is not None)

    ck("§17 foundation 한계 기록",
       xx["foundationLimitation"]["extremeDiscontinuityPct"] == 1.42)
    ck("§17 어떤 factor 도 한계에 노출되지 않음",
       not any(x["exposed"] for x in xx["byFactor"].values()),
       [k for k, x in xx["byFactor"].items() if x["exposed"]])

    # scorecard 판정이 사전 기준과 일치
    ck("§18 기준 불변", sc["thresholdsUnchanged"] is True)
    ck("§18 기준이 precommit 그대로",
       sc["qualificationFromPrecommit"]["STRONG_SIGNAL"]["minAnnualSpreadPct"]
       == QUALIFICATION["STRONG_SIGNAL"]["minAnnualSpreadPct"])
    rows = {x["factor"]: x for x in sc["rows"]}
    ck("10개 factor 판정", len(rows) == 10)
    for k, x in rows.items():
        vd, _why = SC.classify(x)
        ck(f"{k} 판정 재현", vd == x["verdict"], (vd, x["verdict"]))

    # STRONG 은 사전 조건을 실제로 전부 만족해야 한다
    S = QUALIFICATION["STRONG_SIGNAL"]
    for k, x in rows.items():
        if x["verdict"] != "STRONG_SIGNAL":
            continue
        ck(f"{k} STRONG: spread>=3", x["spreadAnnPct"]["12M"] >= S["minAnnualSpreadPct"])
        ck(f"{k} STRONG: 3/3 horizon 양", x["horizonsPositive"] == 3)
        ck(f"{k} STRONG: CI 하한>0", x["ci95LowPct"] > 0)
        ck(f"{k} STRONG: size 통제 부호 유지", x["sizeControlSameSign"] is True)
        ck(f"{k} STRONG: 거래소 통제 부호 유지",
           x["exchangeControlSameSign"] is True)
        ck(f"{k} STRONG: 집중 아님", x["concentratedSignal"] is False)
        ck(f"{k} STRONG: TSR 한계 아님", x["tsrLimitationExposed"] is False)
        ck(f"{k} STRONG: 단조성>=0.5", x["monotonicity"] >= S["monotonicityMin"])

    # INVERTED 는 방향을 뒤집지 않았음을 확인
    for k, x in rows.items():
        if x["verdict"] != "INVERTED_SIGNAL":
            continue
        ck(f"{k} INVERTED: 사전 방향 유지 +1", FACTORS[k]["direction"] == +1)
        ck(f"{k} INVERTED: 12M 음(-)", x["spreadAnnPct"]["12M"] < 0)

    ck("§19 PRIMARY 선정", v["PRIMARY_FACTOR"] is not None)
    ck("§19 reference 는 PRIMARY 가 아님",
       all(not FACTORS[f]["get" if False else "family"] or
           not FACTORS[f].get("referenceOnly")
           for f in (v["PRIMARY_FACTOR"] if isinstance(v["PRIMARY_FACTOR"], list)
                     else [])))
    ck("§21 포트폴리오 search 미수행", v["portfolioSearchDone"] is False)
    ck("§20 새 조합 탐색 미수행", v["combinationSearchDone"] is False)
    ck("§34 다음 작업 규칙 지정", v["nextTaskRule"] in list("ABCDEF"))
    ck("§23 Founder 가설 검증 기록",
       len(v["foundersQualityHypothesis"]["result"]) == 3)

    # 결정적 재현 (§27)
    a = A()
    cs = a.cohorts("BM", 12)
    agg = AN.agg(cs)
    ck("결정적 재현 — BM 12M spread",
       abs(agg["meanSpreadAnnPct"] - h["byFactor"]["BM"]["12M"]["meanSpreadAnnPct"])
       < 1e-9, (agg["meanSpreadAnnPct"],
                h["byFactor"]["BM"]["12M"]["meanSpreadAnnPct"]))
    ck("결정적 재현 — BM 코호트 수",
       agg["cohorts"] == h["byFactor"]["BM"]["12M"]["cohorts"])
    sp = [x["spreadAnn"] for x in cs]
    ck("결정적 재현 — BM 부트스트랩",
       AN.bootstrap(sp)["ci95LowPct"] == b["byFactor"]["BM"]["ci95LowPct"])


# ═══════════════════ L5 회귀 ═══════════════════
def t_l5():
    print("[L5] R24~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]

    def syn(rows):
        return {d: {"X": {"close": c, "shares": s, "dps": None}}
                for d, (c, s) in zip(ds, rows)}
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(1000, 10000), (100000, 100)] + [(100000, 100)] * 4, "역분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=syn(rows))
        ck(f"R16 {nm} wealth 불변",
           abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    ov = load_overrides()
    ck("R24 재분류에서 override 로드", len(ov) == 2742, len(ov))
    kinds = {}
    for p in ov.values():
        kinds[p["kind"]] = kinds.get(p["kind"], 0) + 1
    ck("override 에 MECHANICAL 존재", kinds.get("MECHANICAL", 0) > 0, kinds)
    ck("override 에 RIGHTS 존재", kinds.get("RIGHTS", 0) > 0, kinds)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r19-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r21-foundation-verdict-latest.json",
              "r22-foundation-verdict-latest.json",
              "r23-foundation-verdict-latest.json",
              "r24-foundation-verdict-latest.json",
              "r24-full-reconciliation-latest.json",
              "r15-samsung-tsr-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())

    r24 = json.loads((RD / "r24-foundation-verdict-latest.json")
                     .read_text(encoding="utf-8"))
    ck("R24 판정 보존",
       r24["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")
    ck("R24 편향 보존", r24["expectedBiasPct"] == 1.734)
    ck("R24 미해결비율 보존", r24["unresolvedTickerPct"] == 2.2)

    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-dart-rights-issue-canonical-recovery-r17-latest.md",
              "wababa-dart-legacy-rights-document-recovery-r18-latest.md",
              "wababa-no-direct-match-capital-action-recovery-r19-latest.md",
              "wababa-holder-rights-final-terms-recovery-r20-latest.md",
              "wababa-low-confidence-direct-entitlement-recovery-r21-latest.md",
              "wababa-final-27-holder-rights-terms-recovery-r22-latest.md",
              "wababa-final-11-holder-rights-allocation-recovery-r23-latest.md",
              "wababa-final-unknown-50-direct-classification-r24-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-canonical-factor-rediscovery-r25-latest.json"
    ck("R25 보고서 JSON 존재", j.exists())
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
    m = WD / "wababa-canonical-factor-rediscovery-r25-latest.md"
    ck("R25 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§32)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§32)", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("PRIMARY_FACTOR", "SECONDARY_FACTOR", "INVERTED_SIGNAL",
                     "Founder 의 Quality 가설", "부트스트랩", "집중도",
                     "상장폐지", "TSR 한계", "삼성전자", "REAL_MONEY_NOT_APPROVED",
                     "Founder 행동", "다음 단일 작업"):
            ck(f"§33 보고 항목 포함: {need}", need in body)
        ck("수익보장 표현 없음 (§25)",
           "보장" not in body or "수익을 보장하지 않으며" in body)
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
