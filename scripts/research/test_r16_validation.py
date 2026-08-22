#!/usr/bin/env python3
"""R16 회귀 — canonical wealth engine invariant. 네트워크 0 · production write 0.

WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16

§23·§24 의 4계층:
  L1 synthetic unit tests      — 합성 사건으로 wealth 보존/이중계상 검사
  L2 real anchor reconciliation — 삼성전자 등 실제 종목 manual 대조
  L3 universe anomaly scan      — 전 구간 이상치 분류
  L4 deterministic full rerun   — 재실행 동일성

지원 데이터가 없는 event(합병·인적분할·유상증자)는 mock 으로 통과시키지 않고
UNSUPPORTED 임을 검증한다(§23).

사용: python scripts/research/test_r16_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r16_precommit as PC  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
TK = "005930"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


# ═══════ L1 synthetic ═══════
def synth(dates, rows):
    """rows: [(close, shares, dps)] → cap dict"""
    return {d: {"X": {"close": c, "shares": s, "dps": dv}}
            for d, (c, s, dv) in zip(dates, rows)}


def t_l1():
    print("[L1] synthetic unit tests")
    ds = [f"20{10+i//12:02d}-{i%12+1:02d}-01" for i in range(6)]

    # 1) 50:1 분할 — wealth 불변
    cap = synth(ds, [(100000, 100, None), (2000, 5000, None), (2000, 5000, None),
                     (2000, 5000, None), (2000, 5000, None), (2000, 5000, None)])
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("50:1 분할 wealth 불변 (0%)", abs(r["cumulativeReturn"]) < 1e-9,
       r["cumulativeReturn"])
    ck("50:1 분할 보유주식 ×50", abs(r["endingShares"] - 50) < 1e-9, r["endingShares"])
    ck("분할이 MECHANICAL 로 분류", r["eventCounts"][C.MECHANICAL] == 1)

    # 2) 100:1 역분할 — wealth 불변 (가짜 +9900% 금지)
    cap = synth(ds, [(1000, 10000, None), (100000, 100, None)] + [(100000, 100, None)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("100:1 역분할 wealth 불변", abs(r["cumulativeReturn"]) < 1e-9,
       r["cumulativeReturn"])
    ck("역분할 가짜 +9900% 미발생", r["cumulativeReturn"] < 1.0)
    raw = e.get_total_return("X", ds[0], ds[2], mode="PRICE_ONLY_UNSAFE")
    ck("PRICE_ONLY 는 가짜 +9900% 를 낸다(버그 재현)",
       raw["cumulativeReturn"] > 90, raw["cumulativeReturn"])

    # 3) 무상증자 2:1
    cap = synth(ds, [(10000, 100, None), (5000, 200, None)] + [(5000, 200, None)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("무상증자 2:1 wealth 불변", abs(r["cumulativeReturn"]) < 1e-9)

    # 4) 유상증자 — 시총 급증 → 조정 금지
    cap = synth(ds, [(10000, 100, None), (8000, 200, None)] + [(8000, 200, None)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("유상증자 미조정(보유주식 1주 유지)", abs(r["endingShares"] - 1.0) < 1e-9,
       r["endingShares"])
    ck("유상증자로 가짜 wealth 미발생 (가격대로 -20%)",
       abs(r["cumulativeReturn"] - (-0.20)) < 1e-9, r["cumulativeReturn"])
    ck("SUSPECTED_RIGHTS 로 분류", r["eventCounts"][C.RIGHTS] == 1)
    ck("unsupported flag 노출",
       r["unsupportedEventFlags"]["SUSPECTED_RIGHTS_NOT_ADJUSTED"] == 1)

    # 5) 자사주 소각 — 주식수만 감소, 조정 금지
    cap = synth(ds, [(10000, 100, None), (10000, 80, None)] + [(10000, 80, None)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("자사주 소각 미조정(보유주식 1주)", abs(r["endingShares"] - 1.0) < 1e-9)
    ck("자사주 소각으로 공짜 현금 미발생", abs(r["cumulativeReturn"]) < 1e-9)

    # 6) 배당 — 이중계상 금지
    cap = synth(ds, [(10000, 100, 1200)] * 6)
    e = C.CanonicalWealth(ds, cap=cap)
    r12 = e.get_total_return("X", ds[0], ds[5])
    exp = (1 + (1200 / 12) / 10000) ** 5 - 1
    ck("배당 재투자 월 1/12 발생", abs(r12["cumulativeReturn"] - exp) < 1e-12,
       (r12["cumulativeReturn"], exp))
    ck("배당이 연 12배로 계상되지 않음", r12["cumulativeReturn"] < 0.10)
    nr = e.get_total_return("X", ds[0], ds[5], mode="NO_REINVEST")
    ck("무재투자 < 재투자", nr["cumulativeReturn"] < r12["cumulativeReturn"])
    ck("무재투자 현금 적립됨", nr["endingCash"] > 0)
    po = e.get_total_return("X", ds[0], ds[5], mode="PRICE_ONLY_UNSAFE")
    ck("PRICE_ONLY 는 배당 0", abs(po["cumulativeReturn"]) < 1e-12)

    # 7) DPS 결측 vs 0 구분
    cap = synth(ds, [(10000, 100, None)] * 3 + [(10000, 100, 0.0)] * 3)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[5])
    f = r["dataQualityFlags"]
    ck("DPS 결측과 0 을 구분해 기록", f["DPS_MISSING"] > 0 and f["DPS_ZERO"] > 0, f)
    ck("둘 다 수익률 0", abs(r["cumulativeReturn"]) < 1e-12)

    # 8) 분할 + 배당 동시 — 이중계상 없음
    cap = synth(ds, [(100000, 100, 1200), (2000, 5000, 24)] + [(2000, 5000, 24)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("분할+배당 동시에도 wealth 폭발 없음", -0.05 < r["cumulativeReturn"] < 0.05,
       r["cumulativeReturn"])

    # 9) wealth 보존 (ledger)
    cap = synth(ds, [(100000, 100, None), (2000, 5000, None)] + [(2000, 5000, None)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e._run(0, 3, "X", ledger=True)
    bad = [x for x in r["ledger"]
           if x["wealthBefore"] and x["wealthAfter"]
           and abs(x["wealthAfter"] / x["wealthBefore"] - 1) > 0.01]
    ck("사건 전후 wealth 보존(외부 현금흐름 0)", not bad, bad[:1])
    ck("ledger 필수 필드 존재",
       all(k in r["ledger"][0] for k in
           ("date", "ticker", "sharesHeld", "cash", "price", "marketValue",
            "totalWealth", "dividendCash", "rightsValue", "capitalContribution",
            "corporateActionEvent", "splitFactor", "shareCountBefore",
            "shareCountAfter", "source", "provenance", "wealthBefore",
            "externalCashFlow", "wealthAfter")))
    ck("external cash flow 0 (유상증자 미조정이므로)",
       all(x["externalCashFlow"] == 0 for x in r["ledger"]))
    ck("자본 창조 없음 (capitalContribution 0)",
       all(x["capitalContribution"] == 0 for x in r["ledger"]))


# ═══════ L2 anchor ═══════
def t_l2(eng, ds):
    print("[L2] real anchor reconciliation")
    ref = PC.ANCHOR["samsungReproduction"]
    tol = PC.ANCHOR["tolerancePctPoints"]
    a = eng.get_total_return(TK, "2010-01-04", "2021-08-02")
    b = eng.get_total_return(TK, "2010-01-04", "2021-08-02", mode="NO_REINVEST")
    c = eng.get_total_return(TK, "2010-01-04", "2021-08-02", mode="PRICE_ONLY_UNSAFE")
    ck(f"삼성 TSR 재투자 {100*a['cagr']:.2f}% == R15 {ref['tsrReinvestCagrPct']}%",
       abs(100 * a["cagr"] - ref["tsrReinvestCagrPct"]) <= tol)
    ck(f"삼성 TSR 무재투자 {100*b['cagr']:.2f}% == R15 {ref['tsrNoReinvestCagrPct']}%",
       abs(100 * b["cagr"] - ref["tsrNoReinvestCagrPct"]) <= tol)
    ck(f"삼성 PRICE_ONLY {100*c['cagr']:.2f}% == 구 엔진 -18.17%",
       abs(100 * c["cagr"] + 18.17) <= tol)
    ck("삼성 보유 1주 → 약 59주 (분할50 + 배당재투자)",
       58 < a["endingShares"] < 61, a["endingShares"])
    # ★ raw-close guard (§29)
    ck("CANONICAL 과 PRICE_ONLY 차이가 30%p/년 이상 (guard)",
       100 * (a["cagr"] - c["cagr"]) > 30, 100 * (a["cagr"] - c["cagr"]))
    # 2018-06 분할 달 wealth 보존
    i = eng.pos["2018-05-02"]
    j = eng.pos["2018-06-01"]
    r = eng._run(i, j, TK, mode="NO_REINVEST")
    p0, s0 = eng.cap[ds[i]][TK]["close"], eng.cap[ds[i]][TK]["shares"]
    p1, s1 = eng.cap[ds[j]][TK]["close"], eng.cap[ds[j]][TK]["shares"]
    mc = (p1 * s1) / (p0 * s0) - 1
    ck("분할 달 wealth 변화 == 시총 변화 (±0.5%p)",
       abs(r["cumulativeReturn"] - mc) < 0.005, (r["cumulativeReturn"], mc))


# ═══════ L3 anomaly ═══════
def t_l3():
    print("[L3] universe anomaly scan")
    p = RD / "r16-anomaly-audit-latest.json"
    ck("anomaly 산출물 존재", p.exists())
    if not p.exists():
        return
    d = json.loads(p.read_text(encoding="utf-8"))
    ck("anomaly 분류가 3종 안에 있음",
       set(d["byClassification"]) <= set(PC.ANOMALY["classes"]))
    ck("anomaly 가 전수 대비 소수 (<0.5%)",
       d["anomalies"] < 0.005 * 537715, d["anomalies"])
    ck("UNRESOLVED 를 숨기지 않음", "UNRESOLVED" in d["byClassification"])


# ═══════ L4 determinism ═══════
def t_l4(eng, ds):
    print("[L4] deterministic full rerun")
    e2 = C.CanonicalWealth(ds, cap=eng.cap)
    ck("사건 분류 재현 동일", e2.counts == eng.counts, (e2.counts, eng.counts))
    n = 0
    for t in list(eng.cap[ds[0]])[:300]:
        a = eng.get_total_return(t, ds[0], ds[120])
        b = e2.get_total_return(t, ds[0], ds[120])
        if a is None and b is None:
            continue
        if a is None or b is None or abs(a["cumulativeReturn"] - b["cumulativeReturn"]) > 1e-12:
            ck(f"재실행 불일치 {t}", False)
            return
        n += 1
    ck(f"재실행 동일 ({n}건)", n > 50, n)
    # O(1) 경로 == ledger 경로
    m = 0
    for t in list(eng.cap[ds[0]])[:300]:
        a = eng.get_total_return(t, ds[0], ds[120])
        b = eng.wealth_index(0, 120, t)
        if a is None or b is None:
            continue
        if abs(a["cumulativeReturn"] - b) > 1e-9:
            ck(f"O(1) 경로 불일치 {t}", False, (a["cumulativeReturn"], b))
            return
        m += 1
    ck(f"O(1) wealth_index == ledger 경로 ({m}건)", m > 50, m)
    ck("fwd_return adapter 가 wealth_index 와 동일",
       abs(eng.fwd_return(None, 0, 120, TK) - eng.wealth_index(0, 120, TK)) < 1e-15)


# ═══════ unsupported 를 지원한다고 선언하지 않았는가 (§23) ═══════
def t_unsupported():
    print("[U] unsupported event 정직성")
    inv = json.loads((RD / "r16-corporate-action-inventory-latest.json")
                     .read_text(encoding="utf-8"))
    by = {r["event"]: r["support"] for r in inv["rows"]}
    for ev in ("RIGHTS_ISSUE", "MERGER", "SPINOFF (인적분할)", "CODE_CHANGE"):
        ck(f"{ev} = UNSUPPORTED_DATA", by.get(ev) == "UNSUPPORTED_DATA", by.get(ev))
    ck("TREASURY_CANCELLATION = SUPPORTED_COMPLETE (조정 안 하는 게 정답)",
       by.get("TREASURY_CANCELLATION") == "SUPPORTED_COMPLETE")
    anc = json.loads((RD / "r16-anchor-cases-latest.json").read_text(encoding="utf-8"))
    ck("합병·인적분할 anchor 를 BLOCKED_DATA 로 표시(mock 통과 없음)",
       set(anc["blockedTypes"]) == {"G_MERGER", "H_SPINOFF"}, anc["blockedTypes"])
    ck("삼성 anchor PASS", anc["samsungReproductionPass"] is True)


# ═══════ 산출물 · legacy · production ═══════
def t_outputs():
    print("[O] 산출물 · legacy status · production")
    for n in ("precommit", "corporate-action-inventory", "anchor-cases",
              "anomaly-audit", "tsr-coverage", "legacy-research-status",
              "foundation-verdict"):
        ck(f"근거 JSON r16-{n}", (RD / f"r16-{n}-latest.json").exists())
    md = WD / "wababa-canonical-tsr-research-foundation-reset-r16-latest.md"
    js = WD / "wababa-canonical-tsr-research-foundation-reset-r16-latest.json"
    ck("R16 MD 존재", md.exists())
    ck("R16 JSON 존재", js.exists())
    if md.exists():
        ck("MD 첫 줄 전체 판정",
           md.read_text(encoding="utf-8").splitlines()[0].startswith("전체 판정:"))
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        ck("foundation verdict 가 4종 안에 있음",
           d["foundationVerdict"]["verdict"] in PC.FOUNDATION_VERDICTS)
        ck("factor 재실행 0 (§16)", d["factorResearchPerformed"] == 0)
        ck("SIZE 연구 미시작", d["sizeResearchStarted"] is False)
        ck("REAL_MONEY_NOT_APPROVED",
           d["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        pc = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "publicDisclosure"):
            ck(f"production {k} == 0", pc.get(k) == 0, pc.get(k))
        ck("LEGACY_50D unchanged", pc.get("legacy50d") == "unchanged")
        ck("kr-stock-agent untouched", pc.get("krStockAgentRepo") == "untouched")
        ck("기존 산출물 보존 선언", d["priorArtifactsPreserved"] is True)
    leg = json.loads((RD / "r16-legacy-research-status-latest.json")
                     .read_text(encoding="utf-8"))
    ck("R11 결과가 잠정으로 강등", "PROVISIONAL" in leg["statuses"]["R11_BM_INCREMENTAL_ALPHA_6_90PP"])
    ck("R14 결과가 잠정으로 강등", "PROVISIONAL" in leg["statuses"]["R14_QUALITY_INVERTED"])
    ck("R15 도 잠정으로 강등", "PROVISIONAL" in leg["statuses"]["R15_TSR_CORRECTED_Q3_PROMISING"])
    ck("canonical 성과 사용 불가 명시", leg["canonicalPerformanceUsable"] is False)
    ck("대외 사용 불가 명시", leg["publicUseAllowed"] is False)
    ck("LEGACY_50D 는 production 실험으로 유지",
       leg["statuses"]["LEGACY_50D"] == "UNTOUCHED_PRODUCTION_EXPERIMENT")
    # 기존 보고서 보존
    for n in ("wababa-factor-signal-discovery-r7-latest",
              "wababa-robust-factor-portfolio-r8-latest",
              "wababa-frozen-candidate-independent-validation-r9-latest",
              "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest",
              "wababa-bm-incremental-alpha-validation-r11-latest",
              "wababa-value-profitability-factor-discovery-r13-latest",
              "wababa-quality-compounder-long-horizon-r14-latest",
              "wababa-tsr-corporate-action-forensic-r15-latest"):
        ck(f"기존 보고서 보존 {n}.md", (WD / f"{n}.md").exists())


def t_precommit():
    print("[P] precommit immutability")
    d = json.loads((RD / "r16-precommit-latest.json").read_text(encoding="utf-8"))
    ck("엔진 결과 이전 저장", d["writtenBeforeEngineResults"] is True)
    ck("method = RAW_PRICE + EXPLICIT_EVENT_LEDGER",
       d["canonical"]["method"] == "RAW_PRICE + EXPLICIT_EVENT_LEDGER")
    ck("PRIMARY = 배당재투자",
       d["canonical"]["primary"] == "DIVIDEND_REINVESTED_TOTAL_RETURN")
    ck("minShareRatio 1.2 == 코드", d["classifier"]["minShareRatio"] == C.MIN_SR == 1.2)
    ck("tolerance 0.35 == 코드", d["classifier"]["proportionalTolerance"] == C.TOL == 0.35)
    ck("로그 상대오차 기준 명시", "ln(" in d["classifier"]["proportionalTest"])
    sc2 = d["classifier"].get("scaleCorrection") or {}
    ck("스케일 자체수정 기록", sc2.get("found", "").startswith("SELF_CORRECTION 2"))
    ms = sc2.get("measuredSeparation", {})
    ck("분리력 실측 근거 보존", ms.get("chosenThreshold") == 0.35
       and ms.get("logRelativeMedian", [0, 0])[1] > ms.get("logRelativeMedian", [0, 0])[0])
    ck("잔여 한계 은폐 안함", "신뢰 불가" in sc2.get("residualLimitation", ""))
    ck("대조군 한계 명시", "하한" in sc2.get("controlCaveat", ""))
    # rel_err 동작 — anchor 산수
    ck("삼성 50:1 분할 통과", C.rel_err(50.0, 51.66) < C.TOL)
    ck("1:5 역분할 통과", C.rel_err(0.2, 0.2) < C.TOL)
    ck("자사주 소각(가격 무변동) 배제", C.rel_err(0.8, 1.0) >= C.TOL)
    ck("주식수 무변동은 무한대", C.rel_err(1.0, 1.0) == float("inf"))
    sc = d["classifier"].get("signalIndependenceCorrection") or {}
    ck("시총 파생 자체수정 기록", sc.get("found") == "SELF_CORRECTION during L1 unit test")
    ck("허용오차 0.30->0.15 기록", sc.get("toleranceTightened", {}).get("from") == 0.30
       and sc.get("toleranceTightened", {}).get("to") == 0.15)
    ck("독립신호 2개로 정정", len(d["classifier"]["signals"]) == 2)
    ck("시총 밴드 == 코드",
       d["classifier"]["mcapContinuityBand"] == [C.MC_LO, C.MC_HI])
    ck("유상증자 무조정 선언", "RIGHTS_ISSUE" in d["noAdjustment"])
    ck("factor 연구 금지 선언", "factorResearch" in d["forbidden"])
    ck("benchmark 동일엔진 불변 선언", "benchmarkInvariant" in d)


def main():
    print("R16 canonical wealth engine 회귀\n")
    t_precommit()
    print()
    t_l1()
    print()
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월, 사건 {eng.counts})\n")
    t_l2(eng, ds)
    print()
    t_l3()
    print()
    t_l4(eng, ds)
    print()
    t_unsupported()
    print()
    t_outputs()
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
