#!/usr/bin/env python3
"""R21 회귀 — 저신뢰군 직접권리 실측 invariant. 네트워크 0 · production write 0.

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21

계층:
  L1 classifier — R19 분류 정본 재사용 확인 · 유형 · 종속회사 배제
  L2 entitlement— 권리 YES/NO/UNKNOWN · 합병 방향성 · 기계적 조정
  L3 wealth     — 권리 없으면 조정 0 · 권리 있으면 조정 · 공짜 wealth 금지
  L4 real data  — 실제 산출물(mock 아님) · 추정→실측 대체 · anchor · 판정
  L5 regression — R20/R19/R18/R17/R16 · 삼성 · 분할 · 무상증자 · 유상증자

사용: python scripts/research/test_r21_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r19_classify as K  # noqa: E402
import r21_classify as R  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(n):
    p = RD / f"r21-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


DS = [f"20{20 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


def ev(date="2020-06-01", sr=2.0, before=1_000_000):
    return {"ticker": "X", "date": date, "shareRatio": sr, "sharesBefore": before}


def fil(nm, dt="20200501"):
    return {"rcept_no": "1", "rcept_dt": dt, "report_nm": nm}


# ═══════ L1 classifier ═══════
def t_l1():
    print("[L1] 분류 — R19 정본 재사용")
    ck("R21 분류기가 R19 를 재사용", "r19" in R.CLASSIFIER_VERSION)
    for nm, want in [("전환청구권행사", "CONVERTIBLE_BOND_CONVERSION"),
                     ("신주인수권행사", "BW_WARRANT_EXERCISE"),
                     ("주식매수선택권행사", "STOCK_OPTION_EXERCISE"),
                     ("회사합병결정", "MERGER_NEW_SHARES"),
                     ("주식교환·이전 결정", "SHARE_SWAP"),
                     ("회사분할결정", "SPINOFF_RELATED_SHARES"),
                     ("주식분할결정", "STOCK_SPLIT"),
                     ("무상증자결정", "BONUS_ISSUE"),
                     ("유상증자결정", "TRUE_RIGHTS_ISSUE")]:
        ck(f"'{nm}' → {want}", K.classify_filing(nm)[0] == want,
           K.classify_filing(nm)[0])
    ck("종속회사 공시 배제",
       K.classify_filing("유상증자결정(종속회사의주요경영사항)")[0] is None)
    ck("무관 공시는 None", K.classify_filing("정기주주총회 소집결의")[0] is None)


# ═══════ L2 entitlement ═══════
def t_l2():
    print("[L2] 직접권리 판정")
    for kind, want in [("CONVERTIBLE_BOND_CONVERSION", "NO"),
                       ("BW_WARRANT_EXERCISE", "NO"),
                       ("STOCK_OPTION_EXERCISE", "NO"),
                       ("THIRD_PARTY_ISSUANCE", "NO"),
                       ("PUBLIC_OFFERING", "NO"),
                       ("MERGER_NEW_SHARES", "NO"),
                       ("SHARE_SWAP", "NO"),
                       ("SPINOFF_RELATED_SHARES", "NO"),
                       ("STOCK_SPLIT", "YES"),
                       ("BONUS_ISSUE", "YES"),
                       ("TRUE_RIGHTS_ISSUE", "YES")]:
        got, why = K.entitlement_of(kind)
        ck(f"{kind} → {want}", got == want, got)
        ck(f"{kind} 근거 기록", bool(why))

    r = K.classify_event(ev(), [fil("전환청구권행사")])
    ck("CB 전환 권리 없음", r["entitlement"] == "NO")
    ck("CB 전환 조정 불필요", r["wealthAdjustment"] == "NOT_REQUIRED")
    r = K.classify_event(ev(), [fil("신주인수권행사")])
    ck("BW 행사 권리 없음", r["entitlement"] == "NO")
    r = K.classify_event(ev(), [fil("회사합병결정")])
    ck("합병(주식수 증가측) 권리 없음", r["entitlement"] == "NO")
    ck("합병 방향 근거 기록", "발행한" in r["entitlementWhy"])
    r = K.classify_event(ev(), [fil("주식분할결정")])
    ck("주식분할 권리 있음", r["entitlement"] == "YES")
    ck("주식분할 기계적 조정",
       r["wealthAdjustment"] == "REQUIRED_MECHANICAL")
    r = K.classify_event(ev(), [fil("자기주식취득결정")])
    ck("자기주식은 주식수 증가 설명 불가", r["primaryEvent"] == "UNRESOLVED")
    r = K.classify_event(ev(), [])
    ck("공시 없으면 UNKNOWN", r["entitlement"] == "UNKNOWN")
    ck("공시 없으면 조정 UNKNOWN", r["wealthAdjustment"] == "UNKNOWN")
    r = K.classify_event(ev(), [fil("전환청구권행사", "20180101")])
    ck("창 밖은 매칭 금지", r["primaryEvent"] == "UNRESOLVED")
    r = K.classify_event(ev(sr=1.5, before=1_000_000),
                         [fil("신주인수권행사(500,000주)")])
    ck("수량 일치하면 HIGH", r["confidence"] == "HIGH", r["confidence"])
    r = K.classify_event(ev(sr=1.5, before=1_000_000),
                         [fil("신주인수권행사(9,000,000주)")])
    ck("수량 크게 다르면 HIGH 금지", r["confidence"] != "HIGH")


# ═══════ L3 wealth ═══════
def t_l3():
    print("[L3] wealth — 권리 유무가 결과를 가른다")
    cap = synth(DS, [(10000, 100)] + [(5000, 200)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)

    m = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0,
          "label": "CONFIRMED_NON_RIGHTS", "holderRight": False}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("권리 없으면 보유주식 불변", abs(r["endShares"] - 1.0) < 1e-9)
    ck("권리 없으면 조정 정확히 0",
       abs(r["twr"] - (5000 / 10000 - 1)) < 1e-9)
    ck("권리 없으면 납입 0", r["externalContribution"] == 0.0)
    ck("공짜 wealth 없음", r["endShares"] <= 1.0 + 1e-9)

    m2 = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0,
           "label": "CONFIRMED_BONUS_ISSUE", "holderRight": False}]
    r2 = W.RightsWealth(eng, m2).run("X", 0, 1, reinvest=False)
    ck("기계적 권리는 배율만큼 조정", abs(r2["endShares"] - 2.0) < 1e-9)
    ck("기계적 조정 wealth 중립", abs(r2["twr"]) < 1e-9)
    ck("기계적 조정 납입 0", r2["externalContribution"] == 0.0)
    ck("권리 유무가 실제로 결과를 가른다",
       abs(r["twr"] - r2["twr"]) > 0.4)

    cap3 = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng3 = C.CanonicalWealth(DS, cap=cap3)
    m3 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
           "label": "CONFIRMED_RIGHTS", "holderRight": True,
           "dartRatio": 0.5, "issuePriceDerived": 6000.0}]
    r3 = W.RightsWealth(eng3, m3).run("X", 0, 1, reinvest=False)
    ck("공정 유상증자 wealth 중립", abs(r3["twr"]) < 1e-9)
    ck("외부납입 분리", abs(r3["externalContribution"] - 3000.0) < 1e-6)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 산출물 — mock 이 아니다")
    tg = L("targets-precommit")
    ck("대상 precommit 존재", tg is not None)
    if tg:
        ck("결과 이전 작성", tg["writtenBeforeDirectSourceResults"] is True)
        ck("대상 96건", tg["targetTotal"] == 96, tg["targetTotal"])
        ck("threshold 변경 없음", tg["verdict"]["unchanged"] is True)
        ck("예단 금지 명시", "상승" in tg["noPrejudgement"]
           and "그대로 보고" in tg["noPrejudgement"])
        ck("새 확률 신설 금지", "만들어 넣지 않는다" in
           tg["estimateReplacement"]["hardRule"])
        ck("R19 정본 재사용 선언", "R19" in tg["typeRules"]["inheritedFrom"])
        ck("범위 고정", "target 변경 금지" in tg["scopeGuard"])
        ck("현재 P 기록", tg["targets"][0]["currentBiasP"] == 0.296)

    ds = L("direct-source-events")
    ck("직접소스 수집 존재", ds is not None)
    if ds:
        ck("수집기 재사용 명시", "r19_collect" in ds["collectorReused"])
        ck("96건 전부 커버", ds["eventsCovered"] == 96, ds["eventsCovered"])
        ck("공시 확보", ds["filingsAvailable"] > 0)
        ck("수집 실패 0", ds["hardFailures"] == 0)

    cl = L("event-classification")
    ck("분류 산출물 존재", cl is not None)
    if cl:
        ck("96건 전수 분류", cl["targets"] == 96)
        ck("R19 분류기 재사용", "r19_classify" in cl["reusedFrom"])
        ck("세 축 분리 명시", "다른 축" in cl["layerNote"])
        ck("자기주식이 PRIMARY 아님",
           cl["byPrimaryEvent"].get("TREASURY_ACTION", 0) == 0)

    en = L("entitlement")
    ck("권리 판정 존재", en is not None)
    if en:
        ck("YES/NO/UNKNOWN 3분류", len(en["counts"]) == 3)
        ck("실측 YES 비율 산출", en["measuredYesRateAmongKnown"] is not None)
        ck("추정치 병기", en["priorPopulationEstimate"] == 0.296)
        ck("예단 금지 문구", "예단하지 않고" in en["comparisonNote"])

    fp = L("false-classification-audit")
    ck("오분류 감사 존재", fp is not None)
    if fp:
        ck("원문 공시명 대조", "원문 공시명" in fp["method"])
        ck("오분류율 산출", fp["falsePositiveRate"] is not None)

    an = L("anchor-cases")
    ck("anchor 존재", an is not None)
    if an:
        ck("anchor 6건 이상", len(an["cases"]) >= 6, len(an["cases"]))
        ck("anchor 전부 manual 일치", an["allPass"])
        ck("권리 없으면 조정 0",
           an["noEntitlementAdjustmentExactlyZero"] is not False)
        ck("양쪽 필요 이유 기록", "구분되지 않는다" in an["whyBothSidesNeeded"])

    ct = L("ex-rights-continuity")
    ck("연속성 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0", ct["noFreeWealth"] is True)
        ck("차분 = 이론 권리가치", ct["diffMatchesTheory"] is True)

    b = L("expected-bias")
    ck("기대편향 존재", b is not None)
    if b:
        ck("공식 R20 유지", b["formulaUnchangedFromR20"] is True)
        ck("R18~R21 progression",
           set(b["progression"]) == {"R18", "R19", "R20", "R21"})
        ck("R20 값 보존", b["progression"]["R20"] == 3.442)
        er = b["estimateReplacement"]
        ck("추정→실측 기록", er["priorP"] == 0.296)
        ck("기존 기여 보존", er["priorContributionPp"] == 1.453)
        ck("잔여 기여 산출", er["residualContributionPp"] is not None)
        ck("실측이 추정보다 낮음",
           er["measuredYesRateAmongKnown"] < er["priorP"])
        ck("지표 구조 설명", "농축" in b["metricStructureNote"])
        gs = b["gateSensitivity"]
        ck("게이트 감도 분석 존재", gs is not None)
        ck("감도표에 통과 여부", all("passesGate" in r
                                for r in gs["ifGroupFullyResolved"]))
        ck("p=0 군 해결로는 안 내려감 명시", "내려가지 않는다" in gs["interpretation"])
        ck("잔여 추정의존 감소",
           b["remainingEstimateDependency"]["events"] < 143)

    mt = L("unresolved-materiality")
    ck("materiality 존재", mt is not None)
    if mt:
        ck("R16~R21 progression",
           set(mt["progression"]) == {"R16", "R17", "R18", "R19", "R20", "R21"})
        ck("R20 값 보존", mt["progression"]["R20"] == 5.43)
        ck("미해결 감소", mt["progression"]["R21"] < mt["progression"]["R20"])
        ck("편향 방향 과소평가",
           mt["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")

    rec = L("full-reconciliation")
    ck("전체 재조정 존재", rec is not None)
    if rec:
        ck("2,742건 전수", rec["total"] == 2742, rec["total"])
        ck("R21 merge 96건", rec["r21Merged"] == 96, rec["r21Merged"])

    v = L("foundation-verdict")
    ck("판정 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 변경 없음 (§19)", v["thresholdsUnchanged"] is True)
        ck("bias 공식 변경 없음 (§17)", v["biasFormulaUnchanged"] is True)
        ck("2% 기준 그대로",
           v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("20% 기준 그대로",
           v["thresholdsFromPrecommit"]["passWithLimitsMaxTickerPct"] == 20.0)
        for k in ("R11_BM", "R14_QUALITY", "SIZE"):
            ck(f"{k} PROVISIONAL 유지",
               "PROVISIONAL" in v["legacyResearchStatus"][k])
        ck("legacy 유지 (§25)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        ck("자동 승격 금지", "자동 canonical" in v["legacyResearchStatus"]["note"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))


# ═══════ L5 regression ═══════
def t_l5():
    print("[L5] R20~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(1000, 10000), (100000, 100)] + [(100000, 100)] * 4, "역분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=synth(ds, rows))
        r = e.get_total_return("X", ds[0], ds[2])
        ck(f"{nm} wealth 불변", abs(r["cumulativeReturn"]) < 1e-9)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r19-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r20-full-reconciliation-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-dart-rights-issue-canonical-recovery-r17-latest.md",
              "wababa-dart-legacy-rights-document-recovery-r18-latest.md",
              "wababa-no-direct-match-capital-action-recovery-r19-latest.md",
              "wababa-holder-rights-final-terms-recovery-r20-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════ production ═══════
def t_prod():
    print("[prod] production 보호 (§29)")
    rep = WD / "wababa-low-confidence-direct-entitlement-recovery-r21-latest.json"
    ck("R21 보고서 JSON 존재", rep.exists())
    if rep.exists():
        d = json.loads(rep.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure"):
            ck(f"production {k} == 0", p[k] == 0, p.get(k))
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED", p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 orchestration 0", d["hotg"]["newOrchestration"] == 0)
    m = WD / "wababa-low-confidence-direct-entitlement-recovery-r21-latest.md"
    ck("R21 보고서 MD 존재", m.exists())
    if m.exists():
        head = m.read_text(encoding="utf-8").splitlines()[0]
        ck("첫 줄 전체 판정 (§33)", head.startswith("전체 판정:"), head)


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
