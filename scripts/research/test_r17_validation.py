#!/usr/bin/env python3
"""R17 회귀 — DART 유상증자 복원 invariant. 네트워크 0 · production write 0.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

§22 의 계층:
  L1 parser/normalizer  — DART 필드 파싱·증자방식 분류·정정 chain
  L2 matching           — 비율/날짜 매칭, 가짜 direct match 금지
  L3 wealth semantics   — 권리 배정·외부납입 분리·공짜 wealth 금지·희석
  L4 real data          — 실제 anchor·materiality·판정 (mock 아님)
  L5 R16 regression     — R16 anchor·역분할·무상증자가 깨지지 않았는지

§22 경고: mock-only test 를 real data coverage PASS 로 오해하지 않는다.
L1~L3 는 합성이고, 실제 커버리지는 L4 가 판정한다.

사용: python scripts/research/test_r17_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_match as M  # noqa: E402
import r17_normalize as N  # noqa: E402
import r17_precommit as PC  # noqa: E402
import r17_wealth as W  # noqa: E402

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
    p = RD / f"r17-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


DS = [f"20{20 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


# ═══════ L1 parser / normalizer ═══════
def t_l1():
    print("[L1] DART parser · 증자방식 분류 · 정정 chain")
    ck("숫자 콤마 파싱", N._num("15,690,000") == 15690000.0)
    ck("'-' 는 결측(0 아님)", N._num("-") is None)
    ck("'0' 은 0 으로 보존", N._num("0") == 0)
    ck("빈값 결측", N._num("") is None)

    ck("주주배정 분류", N.classify_method("주주배정증자") == "SHAREHOLDER_ALLOCATION")
    ck("주주배정후 실권주 분류",
       N.classify_method("주주배정후 실권주 일반공모") == "SHAREHOLDER_THEN_PUBLIC")
    ck("실권주가 주주배정보다 우선 매칭",
       N.classify_method("주주배정후실권주일반공모") == "SHAREHOLDER_THEN_PUBLIC")
    ck("제3자배정 분류", N.classify_method("제3자배정증자") == "THIRD_PARTY")
    ck("일반공모 분류", N.classify_method("일반공모증자") == "PUBLIC_OFFERING")
    ck("미상은 추정하지 않고 UNKNOWN", N.classify_method("") == "UNKNOWN")
    ck("None 도 UNKNOWN", N.classify_method(None) == "UNKNOWN")

    row = {"rcept_no": "20191205000055", "nstk_ostk_cnt": "15,690,000",
           "nstk_estk_cnt": "-", "fv_ps": "5,000",
           "bfic_tisstk_ostk": "20,357,135", "fdpp_fclt": "19,000,000,000",
           "fdpp_op": "67,323,200,000", "fdpp_etc": "20,996,400,000",
           "ic_mthn": "주주배정후 실권주 일반공모"}
    e = N.normalize_structured("267260", "테스트", "piicDecsn", row)
    ck("접수일 = 접수번호 앞 8자리", e["filing_date"] == "20191205")
    ck("신주비율 = 신주/증자전주식수", abs(e["rights_ratio"] - 15690000 / 20357135) < 1e-9)
    ck("조달금액 합산", e["total_proceeds"] == 107319600000.0)
    ck("발행가는 파생값으로 표기",
       e["issuePriceProvenance"] == "DERIVED_FROM_PROCEEDS_DIV_SHARES")
    ck("발행가 = 조달금액/신주수",
       abs(e["issue_price_derived"] - 107319600000.0 / 15690000) < 1e-6)
    ck("직접출처 표기", e["provenance"] == "DIRECT_DART")
    ck("구조화 증거 수준", e["evidenceLevel"] == "STRUCTURED")

    b = N.normalize_structured("X", "t", "fricDecsn", dict(row, ic_mthn=None))
    ck("무상증자는 납입 없음 방식", b["issue_method"] == "BONUS_NO_PAYMENT")
    ck("무상증자 kind", b["kind"] == "BONUS")

    f = N.normalize_filing("X", "t", {"rcept_no": "20101231000001",
                                      "rcept_dt": "20101231",
                                      "report_nm": "[기재정정]주요사항보고서(유상증자결정)"})
    ck("공시명 정정 인식", f["isCorrection"] is True)
    ck("공시명 유상 인식", f["kind"] == "RIGHTS")
    ck("공시명만은 방식 미상", f["issue_method"] == "UNKNOWN")
    ck("공시명 증거수준 분리", f["evidenceLevel"] == "FILING_TITLE_ONLY")
    f2 = N.normalize_filing("X", "t", {"rcept_no": "1", "rcept_dt": "20101231",
                                       "report_nm": "증권발행결과(자율공시)(제3자배정 유상증자)"})
    ck("공시명에 드러난 제3자배정은 인식", f2["issue_method"] == "THIRD_PARTY")
    f3 = N.normalize_filing("X", "t", {"rcept_no": "1", "rcept_dt": "20101231",
                                       "report_nm": "정기주주총회 결과"})
    ck("무관 공시는 버림", f3 is None)

    ch = N.correction_chains([
        {"ticker": "X", "kind": "RIGHTS", "rcept_no": "a", "filing_date": "20200101",
         "isCorrection": False, "report_nm": "주요사항보고서(유상증자결정)"},
        {"ticker": "X", "kind": "RIGHTS", "rcept_no": "b", "filing_date": "20200120",
         "isCorrection": True, "report_nm": "[기재정정]주요사항보고서(유상증자결정)"},
    ])
    ck("정정 chain 복원", len(ch) == 1 and len(ch[0]["corrections"]) == 1)
    ck("chain 은 추론으로 표기", ch[0]["provenance"] == "DERIVED_INFERENCE")


# ═══════ L2 matching ═══════
def t_l2():
    print("[L2] matching — 가짜 direct match 금지")
    sus = [{"ticker": "X", "date": "2020-06-01", "shareRatio": 1.50,
            "priceRatio": 1.0, "mcapRatio": 1.5}]
    base = {"ticker": "X", "corpName": "t", "endpoint": "piicDecsn", "kind": "RIGHTS",
            "evidenceLevel": "STRUCTURED", "issue_method": "SHAREHOLDER_ALLOCATION",
            "issue_price_derived": 1000.0}

    def run(cands):
        return M.match_all(sus, {"events": cands, "filings": []})[0]

    r = run([dict(base, filing_date="20200401", rights_ratio=0.50)])
    ck("비율+날짜 일치 → HIGH", r["confidence"] == "HIGH", r["confidence"])
    ck("HIGH 는 직접출처", r["provenance"] == "DIRECT_DART")
    ck("주주배정은 holderRight", r["holderRight"] is True)
    ck("주주배정 + 조건있음 → wealth 확정", r["resolvedWealth"] is True)

    r = run([dict(base, filing_date="20200401", rights_ratio=2.00)])
    ck("비율 크게 다르면 HIGH 아님", r["confidence"] != "HIGH", r["confidence"])

    r = run([dict(base, filing_date="20190601", rights_ratio=0.50)])
    ck("12개월 전은 LOW → UNRESOLVED", r["label"] == "UNRESOLVED", r["confidence"])
    r = run([dict(base, filing_date="20170101", rights_ratio=0.50)])
    ck("창 밖은 매칭 없음", r["confidence"] == "NONE")
    r = run([dict(base, filing_date="20201201", rights_ratio=0.50)])
    ck("사건보다 한참 뒤 공시는 매칭 금지", r["confidence"] == "NONE", r["confidence"])

    r = run([dict(base, filing_date="20200401", rights_ratio=None,
                  evidenceLevel="FILING_TITLE_ONLY", issue_method="UNKNOWN",
                  endpoint="list", issue_price_derived=None)])
    ck("공시명만 = 날짜만으로 CONFIRMED_RIGHTS 선언 금지",
       r["label"] == "CONFIRMED_RIGHTS_METHOD_UNKNOWN", r["label"])
    ck("방식 미상은 wealth 미해결", r["resolvedWealth"] is False)
    ck("방식 미상 사유 기록", r["wealthReason"] == "ISSUE_METHOD_UNKNOWN")
    ck("방식 미상은 holderRight 주장 안함", r["holderRight"] is False)

    r = run([dict(base, filing_date="20200401", rights_ratio=0.50,
                  issue_method="THIRD_PARTY")])
    ck("제3자배정 라벨", r["label"] == "CONFIRMED_THIRD_PARTY_ISSUE")
    ck("제3자배정은 권리 없음", r["holderRight"] is False)
    ck("제3자배정은 wealth 확정(미조정이 정답)", r["resolvedWealth"] is True)

    r = run([dict(base, filing_date="20200401", rights_ratio=0.50,
                  kind="BONUS", issue_method="BONUS_NO_PAYMENT")])
    ck("무상증자 라벨", r["label"] == "CONFIRMED_BONUS_ISSUE")
    ck("무상증자 wealth 확정", r["resolvedWealth"] is True)

    r = run([dict(base, filing_date="20200401", rights_ratio=0.50,
                  issue_price_derived=None)])
    ck("발행가 없으면 wealth 미해결", r["resolvedWealth"] is False)
    ck("조건 결측 사유 기록", r["wealthReason"] == "RIGHTS_TERMS_INCOMPLETE")

    r = M.match_all(sus, {"events": [], "filings": []})[0]
    ck("증거 없으면 UNRESOLVED", r["label"] == "UNRESOLVED")
    ck("증거 없으면 NO_DIRECT_MATCH", r["provenance"] == "NO_DIRECT_MATCH")

    # 더 좋은 후보가 이기는가
    r = run([dict(base, filing_date="20190601", rights_ratio=0.50),
             dict(base, filing_date="20200401", rights_ratio=0.50)])
    ck("가장 강한 증거를 채택", r["confidence"] == "HIGH")


# ═══════ L3 wealth semantics ═══════
def t_l3():
    print("[L3] wealth semantics — 공짜 wealth·외부납입 금지")
    # 공정 유상증자: P_cum 10000, r 0.5, K 6000 → TERP = (10000+3000)/1.5 = 8666.67
    cap = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)
    m = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": "CONFIRMED_RIGHTS",
          "holderRight": True, "dartRatio": 0.5, "issuePriceDerived": 6000.0,
          "resolvedWealth": True}]
    rw = W.RightsWealth(eng, m)
    r = rw.run("X", 0, 1, reinvest=False)
    ck("공정 유상증자는 wealth 중립(TWR≈0)", abs(r["twr"]) < 1e-9, r["twr"])
    ck("외부납입 기록", abs(r["externalContribution"] - 0.5 * 6000) < 1e-6)
    ck("보유주식 1.5배", abs(r["endShares"] - 1.5) < 1e-9)

    # 외부납입을 gain 으로 세면 안 된다
    ck("납입금이 수익으로 계상되지 않음",
       abs(r["twr"]) < 1e-9 and r["externalContribution"] > 0)

    # 발행가 > 권리락가 → 합리적 실권
    cap = synth(DS, [(10000, 100)] + [(5000, 150)] * 7)
    eng2 = C.CanonicalWealth(DS, cap=cap)
    m2 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": "CONFIRMED_RIGHTS",
           "holderRight": True, "dartRatio": 0.5, "issuePriceDerived": 9000.0,
           "resolvedWealth": True}]
    r2 = W.RightsWealth(eng2, m2).run("X", 0, 1, reinvest=False)
    ck("K >= 권리락가면 실권(신주 0)", abs(r2["endShares"] - 1.0) < 1e-9, r2["endShares"])
    ck("실권이면 납입 0", r2["externalContribution"] == 0.0)
    ck("실권 수익 = 주가 수익", abs(r2["twr"] - (5000 / 10000 - 1)) < 1e-9)

    # 제3자배정 = 희석. 공짜 신주 금지
    m3 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
           "label": "CONFIRMED_THIRD_PARTY_ISSUE", "holderRight": False,
           "resolvedWealth": True}]
    r3 = W.RightsWealth(eng2, m3).run("X", 0, 1, reinvest=False)
    ck("제3자배정은 보유주식 불변", abs(r3["endShares"] - 1.0) < 1e-9)
    ck("제3자배정 희석은 주가에 반영", abs(r3["twr"] - (5000 / 10000 - 1)) < 1e-9)
    ck("제3자배정 납입 0", r3["externalContribution"] == 0.0)

    # 일반공모도 동일
    m4 = [dict(m3[0], label="CONFIRMED_PUBLIC_OFFERING")]
    r4 = W.RightsWealth(eng2, m4).run("X", 0, 1, reinvest=False)
    ck("일반공모도 보유주식 불변", abs(r4["endShares"] - 1.0) < 1e-9)

    # 무상증자 = 기계적, 납입 없음
    cap = synth(DS, [(10000, 100)] + [(5000, 200)] * 7)
    eng3 = C.CanonicalWealth(DS, cap=cap)
    m5 = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0,
           "label": "CONFIRMED_BONUS_ISSUE", "holderRight": False,
           "resolvedWealth": True}]
    r5 = W.RightsWealth(eng3, m5).run("X", 0, 1, reinvest=False)
    ck("무상증자 보유주식 2배", abs(r5["endShares"] - 2.0) < 1e-9)
    ck("무상증자 wealth 중립", abs(r5["twr"]) < 1e-9, r5["twr"])
    ck("무상증자 납입 0", r5["externalContribution"] == 0.0)

    # UNRESOLVED = 미조정(R16 동작) = 보수적
    m6 = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0, "label": "UNRESOLVED",
           "holderRight": False, "resolvedWealth": False}]
    r6 = W.RightsWealth(eng3, m6).run("X", 0, 1, reinvest=False)
    ck("미해결은 미조정 유지", abs(r6["endShares"] - 1.0) < 1e-9)
    ck("미해결은 wealth 를 부풀리지 않음", r6["twr"] < 0)

    # understatement 정의
    u = W.understatement({"ticker": "X", "date": DS[1], "dartRatio": 0.5,
                          "issuePriceDerived": 3000.0}, eng3)
    ck("과소평가 = r·(P_ex-K)/P_cum", abs(u - 0.5 * (5000 - 3000) / 10000) < 1e-9, u)
    u2 = W.understatement({"ticker": "X", "date": DS[1], "dartRatio": 0.5,
                           "issuePriceDerived": 9000.0}, eng3)
    ck("무가치 권리는 과소평가 0", u2 == 0.0)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 데이터 — mock 이 아니다")
    pc = L("precommit")
    ck("precommit 존재", pc is not None)
    ck("결과 이전 작성 표기", pc["writtenBeforeDartResults"] is True)
    ck("정책 = POLICY A", pc["wealthPolicy"]["chosen"] == "POLICY_A_ASSUME_FULL_EXERCISE")
    ck("시간가중 기준", "TIME_WEIGHTED" in pc["wealthPolicy"]["returnBasis"])
    ck("R16 40% 기준 승계",
       pc["verdictRule"]["r16ThresholdPreserved"]["thresholdPct"] == 40.0)
    ck("R16 인수값 보존", pc["inherits"]["suspectedRights"] == 2742)

    raw = L("dart-rights-raw")
    ck("원자료 산출물 존재", raw is not None)
    if raw:
        ck("구조화 API 시작연도 기록", raw["structuredEarliestYearObserved"] == 2015)
        ck("corp_code 매핑 기록", raw["corpCodeMapped"] > 1000)

    nm = L("dart-rights-normalized")
    ck("정규화 산출물 존재", nm is not None)
    if nm:
        ck("구조화 이벤트 수집됨", nm["structuredEvents"] > 0)
        ck("증거수준 두 종류 구분", set(nm["evidenceLevels"]) ==
           {"STRUCTURED", "FILING_TITLE_ONLY"})
        ck("발행가 파생 표기", "DERIVED" in nm["issuePriceNote"])

    cr = L("dart-rights-corrections")
    ck("정정 chain 산출물 존재", cr is not None)
    if cr:
        ck("정정 chain 발견", cr["chains"] > 0)
        ck("chain 은 추론 표기", cr["chainProvenance"] == "DERIVED_INFERENCE")

    mt = L("rights-matching")
    ck("매칭 산출물 존재", mt is not None)
    if mt:
        ck("2,742건 전수 처리", mt["suspectedTotal"] == 2742)
        ck("resolved 는 HIGH+MEDIUM 만", "LOW 는 UNRESOLVED" in mt["resolvedDefinition"])
        ck("두 층위 분리 기록", "resolvedWealth" in mt["twoLayerNote"])
        ck("창 조정 자체수정 공개",
           mt["windowAdaptation"]["found"].startswith("SELF_CORRECTION"))
        ck("엄격도 유지 명시", "비율 일치" in mt["windowAdaptation"]["strictnessPreserved"])

    an = L("rights-anchor-cases")
    ck("anchor 산출물 존재", an is not None)
    if an:
        ck("anchor 3종 이상 (§14)", len(an["cases"]) >= 3, len(an["cases"]))
        ck("anchor 전부 manual 일치", all(c["pass"] for c in an["cases"]))
        ck("정정 anchor 존재 (§15)", an["correctionCase"] is not None)
        if an["correctionCase"]:
            ck("정정건에 최종조건 사용", an["correctionCase"]["pass"] is True)
        methods = {c["case"][:1] for c in an["cases"]}
        ck("주주배정 계열 anchor 있음", "A" in methods or "B" in methods)
        ck("제3자/공모 anchor 있음", "C" in methods or "D" in methods)

    ct = L("ex-rights-continuity")
    ck("연속성 산출물 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0건 (§12)", ct["noFreeWealth"] is True)
        ck("R16 대비 차분으로 격리 (§13)", "차분" in ct["method"])
        ck("차분 = 이론 권리가치", ct["diffMatchesTheory"] is True)
        ck("당월 차분 음수 0", ct["eventMonthDiffNeverNegative"] is True)
        ck("차분이 누적 증폭되지 않음", ct["diffDoesNotCompound"] is True)
        ck("연속성 종합 PASS", ct["pass"] is True)
        ck("모든 창 공개(유리한 창만 고르지 않음)", len(ct["windows"]) >= 5)
        ck("절대 TWR 교란을 명시", "교란" in ct["absoluteTwrIsConfounded"]
           or "지배" in ct["absoluteTwrIsConfounded"])
        ck("넓은 창 음수의 정당성 설명", "항상 이득은 아니" in
           ct["wideWindowNegativesAreLegitimate"])
        ck("공짜검사 배당 제외 명시", "배당" in ct["freeWealthCheckNote"])
        ck("R17 이 R16 보다 낮지 않음(당월)",
           ct["windows"][0]["medianDiff"] >= 0)

    ma = L("unresolved-materiality")
    ck("materiality 산출물 존재", ma is not None)
    if ma:
        ck("미해결 사유 분해", len(ma["unresolvedReasons"]) > 0)
        ck("편향 방향 = 과소평가",
           ma["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")
        us = ma["understatementSample"]
        ck("대리치 한계 명시", "DERIVED_INFERENCE" in us["caveat"])
        ck("조건부와 기대편향 분리", us["conditionalMedian"] is not None
           and us["expectedUnderstatement"] is not None)
        ck("기대편향 <= 조건부(확률 가중)",
           us["expectedUnderstatement"] <= us["conditionalMedian"] + 1e-9)
        ck("보수적 추정 선택 공개", "보수적" in us["pHolderRightBiasNote"])
        ck("판정은 기대편향 기준", "기대편향" in
           (L("foundation-verdict") or {}).get("understatementBasis", ""))

    v = L("foundation-verdict")
    ck("판정 산출물 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 사후 수정 없음 (§17)", v["thresholdsUnchanged"] is True)
        ck("R16 40% 기준 그대로",
           v["thresholdsFromPrecommit"]["r16Preserved40"] == 40.0)
        ck("legacy 상태 유지 (§21)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        ck("R11 잠정 유지", "PROVISIONAL" in v["legacyResearchStatus"]["R11_BM"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))


# ═══════ L5 R16 regression ═══════
def t_l5():
    print("[L5] R16 회귀 — 기존 보정이 깨지지 않았는가")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]
    cap = synth(ds, [(100000, 100), (2000, 5000), (2000, 5000),
                     (2000, 5000), (2000, 5000), (2000, 5000)])
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("R16 50:1 분할 여전히 wealth 불변", abs(r["cumulativeReturn"]) < 1e-9)

    cap = synth(ds, [(1000, 10000), (100000, 100)] + [(100000, 100)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("R16 역분할 가짜수익 없음", abs(r["cumulativeReturn"]) < 1e-9)

    cap = synth(ds, [(10000, 100), (5000, 200)] + [(5000, 200)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    r = e.get_total_return("X", ds[0], ds[2])
    ck("R16 무상증자 가짜손실 없음", abs(r["cumulativeReturn"]) < 1e-9)

    ck("R16 산출물 보존", (RD / "r16-foundation-verdict-latest.json").exists())
    ck("R16 보고서 보존",
       (WD / "wababa-canonical-tsr-research-foundation-reset-r16-latest.md").exists())
    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-robust-factor-portfolio-r8-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════ production 보호 (§26) ═══════
def t_prod():
    print("[prod] production 보호")
    rep = WD / "wababa-dart-rights-issue-canonical-recovery-r17-latest.json"
    ck("R17 보고서 JSON 존재", rep.exists())
    if rep.exists():
        d = json.loads(rep.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "publicDisclosure"):
            ck(f"production {k} == 0", p[k] == 0, p.get(k))
        ck("LEGACY_50D 무변경", p["legacy50d"] == "untouched")
        ck("NEW_BM forward 무변경", p["newBmForward"] == "untouched")
        ck("kr-stock-agent 무변경", p["publicRepo"] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED", p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("R16 산출물 미덮어쓰기 선언", d["priorArtifactsPreserved"] is True)
        ck("판정 라인", d["verdictLine"] in ("PASS", "WARNING", "BLOCKED", "WAIT"))
        ck("reason_class 존재", bool(d.get("reasonClass")))
    md = WD / "wababa-dart-rights-issue-canonical-recovery-r17-latest.md"
    ck("R17 보고서 MD 존재", md.exists())
    if md.exists():
        head = md.read_text(encoding="utf-8").splitlines()[0]
        ck("첫 줄 전체 판정 (§30)", head.startswith("전체 판정:"), head)


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
