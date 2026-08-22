#!/usr/bin/env python3
"""R23 회귀 — 증권신고서 배정근거 복원 invariant. 네트워크 0 · production write 0.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

계층:
  L1 parser  — 신고서 3양식 · 무상증자 비율 오독 방지 · 자기주식 제외
  L2 resolve — 네 수량 분리 · 가격 우선순위 · 1.0 무근거 폐기 · 충돌 보존
  L3 wealth  — ENTITLEMENT 기준 · 외부납입 분리 · 실권 정책 · 공짜 wealth 금지
  L4 real    — 실제 산출물(mock 아님) · anchor · bias 식 불변 · 판정
  L5 regress — R22~R16 파서·엔진·산출물 보존

사용: python scripts/research/test_r23_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r20_parser as P20  # noqa: E402
import r22_parser as P22  # noqa: E402
import r23_parser as P  # noqa: E402
import r23_resolve as R  # noqa: E402

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
    p = RD / f"r23-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


DS = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


# 실제 신고서에서 확인된 산식형 배정근거
DERIV = ("※ 구주주 1주당 신주배정비율 산출 근거 "
         "모집주식총수(11,000,000주) - 우리사주조합 우선배정분(2,200,000주) "
         "= 기발행보통주식수(32,531,794주) - 자기주식(2,902,135주) "
         "= 0.2969997056 주")


# ═══════════════════ L1 parser ═══════════════════
def t_l1():
    print("[L1] 신고서 배정근거 파서")
    d = P.parse(f"<DOC><P>{DERIV}</P></DOC>")
    ck("배정근거 블록 인식", d["hasRightsScope"] is True)
    ck("모집주식총수", d["offerTotalShares"] == 11000000.0)
    ck("우리사주 우선배정", d["esopShares"] == 2200000.0)
    ck("기발행 보통주식수", d["issuedCommonShares"] == 32531794.0)
    ck("자기주식", d["treasuryShares"] == 2902135.0)
    ck("구주주 배정주식수 = 모집총수 - 우리사주",
       d["shareholderAllocatedShares"] == 8800000.0)
    ck("§7 eligible = 기발행보통주 - 자기주식",
       d["eligibleOldShares"] == 32531794.0 - 2902135.0)
    ck("파생비율 = 배정량 / eligible",
       abs(d["derivedRatio"] - 8800000.0 / (32531794.0 - 2902135.0)) < 1e-12)
    ck("명시비율 추출", abs(d["ratioPerOldShare"] - 0.2969997056) < 1e-9)
    ck("명시 vs 파생 일치 → 충돌 없음",
       "RATIO_DATA_CONFLICT" not in d["flags"], d["flags"])

    # ★ 자체수정 2·3 — 같은 문서의 무상증자 1.00000 을 집으면 안 된다
    bonus = "무상증자 1주당 신주배정비율 1.00000 주 " + DERIV
    b = P.parse(f"<DOC>{bonus}</DOC>")
    ck("무상증자 비율 오독 방지 (§31 SC2)",
       b["ratioPerOldShare"] != 1.0, b.get("ratioPerOldShare"))
    ck("무상증자 혼재해도 유상 배정근거는 복원",
       abs((b.get("derivedRatio") or 0) - 0.29699970) < 1e-5)
    ck("무상 구간 배제 후에도 후보 존재",
       len(P.rights_scopes(P.flatten(f"<DOC>{bonus}</DOC>"))) >= 1)

    # 배제가 과하면 안 된다 (SC3)
    far = "무상증자 관련 안내는 별도 공시. " + "가" * 400 + DERIV
    ck("멀리 있는 무상증자 언급은 배제하지 않음 (§31 SC3)",
       P.parse(f"<DOC>{far}</DOC>")["hasRightsScope"] is True)

    ck("자기주식 미기재 시 eligible None",
       P.parse("<DOC>구주주 1주당 신주배정비율 산출 근거 "
               "모집주식총수(1,000주)</DOC>").get("eligibleOldShares") is None)
    ck("배정근거 없으면 플래그",
       "NO_RIGHTS_SCOPE" in P.parse("<DOC>사업보고서</DOC>")["flags"])
    ck("빈 문서 예외 없음", P.parse("")["hasRightsScope"] is False)
    ck("파서 버전 기록", P.PARSER_VERSION.startswith("r23-parse"))
    ck("천단위 콤마 파싱", P._num("32,531,794") == 32531794.0)
    ck("소수 파싱", abs(P._num("0.2969997056") - 0.2969997056) < 1e-12)
    ck("숫자 아님 → None", P._num("해당사항없음") is None)
    ck("flatten 이 태그 제거", "<" not in P.flatten("<td>1,000</td>"))

    # §8 — 우선주를 보통주 배정에 섞지 않는다
    pf = P.parse("<DOC>구주주 1주당 신주배정비율 산출 근거 "
                 "모집주식총수(1,000주) 기발행보통주식수(10,000주) "
                 "자기주식(0주) B. 우선주식수 5,000 C. 발행주식총수 15,000</DOC>")
    ck("§8 우선주 분리 인식", pf.get("preferredShares") == 5000.0)
    ck("§8 eligible 에 우선주 미포함", pf["eligibleOldShares"] == 10000.0)


# ═══════════════════ L2 resolve ═══════════════════
def t_l2():
    print("[L2] 확정 로직")
    pc = L("targets-precommit")
    ck("precommit 존재", pc is not None)
    if pc:
        ck("§11 네 수량 분리 정의",
           len([k for k in pc["fourQuantities"]
                if k not in ("hardRule", "whichForWealth")]) == 4,
           list(pc["fourQuantities"]))
        for k in ("PLANNED_NEW_SHARES", "ENTITLED_TO_EXISTING_SHAREHOLDERS",
                  "ACTUALLY_SUBSCRIBED_BY_EXISTING_SHAREHOLDERS",
                  "ACTUALLY_ISSUED_TOTAL"):
            ck(f"§11 {k} 정의됨", k in pc["fourQuantities"])
        ck("§9 증권신고서가 1순위",
           pc["sourcePriority"][0]["rank"] == 1
           and "증권신고서" in pc["sourcePriority"][0]["source"])
        ck("§10 R17 실권정책 승계", "R17" in pc["wealth"]["inheritedFrom"])
        ck("§10 POLICY_A 유지",
           pc["wealth"]["policy"] == "POLICY_A_ASSUME_FULL_EXERCISE")
        ck("§10 새 청약정책 금지 명시", "새 청약 policy" in pc["wealth"]["lapseRule"])
        ck("§10 공짜 wealth 금지", "공짜 wealth" in pc["wealth"]["hardRule"])
        ck("§13 허용오차 사전고정", pc["reconciliation"]["tolerance"] == 0.15)
        ck("§19 판정기준 불변", pc["verdict"]["unchanged"] is True)
        ck("§17 결과공학 금지 문구", len(pc["noResultEngineering"]) > 10)
        ck("대상 11건", pc["targetTotal"] == 11, pc["targetTotal"])
        ck("등급 A~E 정의", len(pc["allocationEvidence"]) == 5)

    # §31 SC4 — 자릿수 검사는 **연도를 일반적으로 걸러내지 못한다**(2009 는
    # 권리락가 8,000원 대비 자릿수가 그럴듯하다). 그래서 실제 방어는 검사가
    # 아니라 **확정신고서 모집금액÷모집주식수를 1순위로 두는 것**이다.
    # 아래 두 줄이 그 사실을 고정한다.
    ck("자릿수 검사 — 큰 자릿수 차이는 배제", R.price_sane(2009.0, 500000) is False)
    ck("자릿수 검사만으로는 연도를 못 거른다 (SC4 근거)",
       R.price_sane(2009.0, 8000) is True)
    ck("자릿수 검사 — 정상 통과", R.price_sane(6140.0, 8000) is True)
    ck("자릿수 검사 — None 안전", R.price_sane(None, 8000) is False)
    ck("자릿수 검사 — dict 허용", R.price_sane({"price": 6140.0}, 8000) is True)

    # 확정본 우선 + 모집금액/모집주식수
    c = [{"meta": {"kind": "REG_ORIGINAL", "rcept_no": "1"},
          "data": {"offerAmount": 1000.0, "offerSharesHeader": 1.0}},
         {"meta": {"kind": "REG_CONFIRMED", "rcept_no": "2"},
          "data": {"offerAmount": 12280000.0, "offerSharesHeader": 2000.0}}]
    rp = R._registration_price(c)
    ck("확정신고서 우선 (§9)", rp["kind"] == "REG_CONFIRMED", rp)
    ck("모집금액 / 모집주식수", rp["price"] == 6140.0, rp)
    ck("근거 기록", rp["basis"] == "OFFER_AMOUNT_DIV_SHARES")
    ck("근거 없으면 None", R._registration_price([]) is None)
    ck("kind 우선순위 정의", R.KIND_RANK["REG_CONFIRMED"] == 0)
    ft0 = L("final-terms")
    if ft0:
        bad = [e for e in ft0["events"]
               if "R22_PRICE_DISCARDED_DISAGREES_REGISTRATION" in e["flags"]]
        ck("SC4 실제 방어 — 신고서 가격이 R22 값을 대체", len(bad) >= 1)
        for e in bad:
            ck(f"폐기된 R22 가격 기록 {e['ticker']}", e.get("discardedR22Price"))
            ck(f"채택가격 출처가 신고서 {e['ticker']}",
               str(e.get("priceSource", "")).startswith("R23_REGISTRATION"))

    al, ft = L("allocation-tables"), L("final-terms")
    ck("배정표 산출물 존재", al is not None)
    ck("최종조건 산출물 존재", ft is not None)
    if al:
        ck("§13 test1/test2 대조 수행", "flags" in al)
        ck("1.0 무근거 폐기 발동 (§31 SC5)",
           al["flags"].get("RATIO_EXACTLY_ONE_UNCORROBORATED", 0) > 0, al["flags"])
        ck("자기주식 제외 반영 (§7)", al["treasuryExcludedCount"] >= 1)
        ck("필드 복원률 기록", "finalRightsRatio" in al["fieldRecovery"])
    if ft:
        ck("11건 유지", ft["targets"] == 11)
        ck("확정 + 부분 = 11",
           sum(ft["byWealthStatus"].values()) == 11, ft["byWealthStatus"])
        ck("§36 미복원은 PARTIAL 로 남김",
           ft["byWealthStatus"].get("WEALTH_PARTIAL", 0) > 0)
        ck("§15 신뢰도 3단계 사용",
           set(ft["byConfidence"]) <= {"HIGH", "MEDIUM", "LOW"})
        for e in ft["events"]:
            if e["wealthStatus"] != "WEALTH_CONFIRMED":
                continue
            ck(f"확정건 비율 존재 {e['ticker']}", bool(e.get("finalRightsRatio")))
            ck(f"확정건 발행가 존재 {e['ticker']}", bool(e.get("finalIssuePrice")))
            ck(f"확정건 비율 1.0 아님 {e['ticker']}", e["finalRightsRatio"] != 1.0)

    sr = L("share-reconciliation")
    ck("수량 대조 산출물 존재", sr is not None)
    if sr:
        ck("§13 충돌은 보존", sr["conflicts"] >= 0 and "onConflict" in sr)


# ═══════════════════ L3 wealth ═══════════════════
def t_l3():
    print("[L3] wealth invariant")
    ds = DS[:4]
    # 1주 보유자: 0.5주 배정, 발행가 1,000, 권리락 2,000
    eng = C.CanonicalWealth(ds, cap=synth(ds, [(3000, 1000), (2000, 1500),
                                               (2000, 1500), (2000, 1500)]))
    m = [{"ticker": "X", "date": ds[1], "label": "CONFIRMED_RIGHTS",
          "holderRight": True, "shareRatio": 1.5, "dartRatio": 0.5,
          "issuePriceDerived": 1000.0, "resolvedWealth": True}]
    rw = W.RightsWealth(eng, m)
    r = rw.run("X", 0, 1, reinvest=False)
    ck("청약 → 주식 1.5", abs(r["endShares"] - 1.5) < 1e-12, r["endShares"])
    ck("외부납입 = 0.5 x 1,000", abs(r["externalContribution"] - 500.0) < 1e-9)
    man = (1.5 * 2000 - 500) / 3000 - 1
    ck("manual 과 엔진 일치", abs(r["twr"] - man) < 1e-12, (r["twr"], man))
    ck("외부납입은 수익이 아님", r["twr"] < 0.0)

    # 실권: K >= 권리락가
    m2 = [dict(m[0], issuePriceDerived=5000.0)]
    r2 = W.RightsWealth(eng, m2).run("X", 0, 1, reinvest=False)
    ck("§10 합리적 실권 → 주식 불변", abs(r2["endShares"] - 1.0) < 1e-12)
    ck("실권 시 외부납입 0", r2["externalContribution"] == 0.0)
    ck("§12 ENTITLEMENT 기준 — 청약률 미반영",
       abs(rw.run("X", 0, 1, reinvest=False)["endShares"] - 1.5) < 1e-12)
    ck("공짜 wealth 없음", not (r["externalContribution"] == 0.0
                             and r["endShares"] > 1.0 + 1e-9))
    ck("과소평가 크기 = r x max(0, P_ex - K) / P_cum",
       abs(W.understatement(m[0], eng) - 0.5 * 1000.0 / 3000.0) < 1e-12)
    ck("실권건 과소평가 0", W.understatement(m2[0], eng) == 0.0)

    an = L("anchor-cases")
    ck("anchor 산출물 존재", an is not None)
    if an:
        ck("복원된 사건 전부 anchor 화 (§16)", an["allResolvedCovered"] is True)
        ck("anchor 전부 manual 일치", an["allPass"] is True)
        ck("anchor 1건 이상", an["obtained"] >= 1)
        ck("정책 출처가 R17", "R17" in an["policySource"])
        for c in an["cases"]:
            ck(f"anchor 오차 0 {c['ticker']}", abs(c["diffPp"]) < 0.05, c["diffPp"])
            ck(f"anchor 수기항목 완비 {c['ticker']}",
               all(k in c["manual"] for k in
                   ("oldShares", "rightsRatio", "finalIssuePrice",
                    "externalContribution", "resultingShares",
                    "manualTsrAdjustment")))
    ct = L("ex-rights-continuity")
    ck("연속성 산출물 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0건", ct["freeWealthEvents"] == 0, ct["freeWealthEvents"])
        ck("이론값과 일치", ct["diffMatchesTheory"] is True)
        ck("연속성 PASS", ct["pass"] is True)


# ═══════════════════ L4 real ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    sf, rec = L("securities-filings"), L("full-reconciliation")
    bias, mat = L("expected-bias"), L("unresolved-materiality")
    v = L("foundation-verdict")
    ck("신고서 수집 산출물 존재", sf is not None)
    if sf:
        ck("실제 원문 확보 (mock 아님)",
           sf["documentsNew"] + sf["documentsCacheHits"] > 0)
        ck("수집 실패 0", sf["documentsFailed"] == 0, sf["documentsFailed"])
        ck("채무증권 제외 (§4)", "채무증권" in sf["debtExcluded"])
        ck("§22 창 제한 준수", "무한" not in str(sf["window"]))
    ck("전체 재분류 존재", rec is not None)
    if rec:
        ck("2,742건 유지", rec["total"] == 2742, rec["total"])
        ck("R23 merge 반영", rec["r23Merged"] == 11, rec["r23Merged"])

    ck("기대편향 산출물 존재", bias is not None)
    if bias:
        ck("§18 공식 R22 동일", bias["formulaUnchangedFromR22"] is True)
        ck("§18 progression 6단계 기록",
           all(k in bias["progression"] for k in
               ("R18", "R19", "R20", "R21", "R22", "R23")))
        ck("R22 값 보존", bias["progression"]["R22"] == 2.416)
        ck("R23 값 기록", isinstance(bias["progression"]["R23"], float))
        ck("§18 기준 2% 불변", bias["thresholdPct"] == 2.0, bias["thresholdPct"])
        ck("모집단 추정 P 유지 (사후 조작 금지)",
           abs(bias["populationP"] - 0.296) < 0.002, bias["populationP"])
        ck("권리 없음 확정군 P=0",
           bias["decomposition"].get(
               "NO_ENTITLEMENT_CONFIRMED", {}).get("avgP") == 0.0)
        ck("주주배정 조건미확보군 P=1 (보수적)",
           bias["decomposition"].get(
               "HOLDER_RIGHT_TERMS_MISSING", {}).get("avgP") == 1.0)
        ck("§21 누적 감도 제시", len(bias["cumulativeSensitivity"]) >= 2)
        ck("§21 군별 감도 제시",
           len(bias["gateSensitivity"]["ifGroupFullyResolved"]) >= 2)
        ck("R22 감도 예측치 명시", bias["r22SensitivityProjection"] == 1.520)

    ck("materiality 산출물 존재", mat is not None)
    if mat:
        ck("미해결 종목비율 progression 8단계", len(mat["progression"]) == 8)
        ck("R22 미해결비율 보존", mat["progression"]["R22"] == 3.04)
        ck("편향 방향 보수적", mat["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")
        ck("§20 20% 기준 통과", mat["unresolvedTickerPct"] < 20.0)

    ck("판정 산출물 존재", v is not None)
    if v:
        ck("§19 기준 불변", v["thresholdsUnchanged"] is True)
        ck("§18 공식 불변", v["biasFormulaUnchanged"] is True)
        ck("R16 40% 기준 보존", v["thresholdsFromPrecommit"]["r16Preserved40"] == 40.0)
        ck("2% 기준 보존", v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("anchor PASS 반영", v["checks"]["anchorsPass"] is True)
        ck("공짜 wealth 없음 반영", v["checks"]["noFakeWealth"] is True)
        ck("§24 기존 연구 자동승격 금지",
           "자동 승격하지 않는다" in v["legacyResearchStatus"]["note"])
        ck("§23 factor 게이트 = 판정 연동",
           v["factorResearchAllowed"] == v["verdict"].startswith(
               "CANONICAL_TSR_FOUNDATION_PASS"))
        gate = (v["checks"]["expectedBiasUnder2pct"]
                and v["checks"]["unresolvedTickerPctUnder20"])
        ck("게이트 미통과면 BLOCKED (§17 기준 이동 금지)",
           gate or v["verdict"].endswith("BLOCKED_RIGHTS_REMAINS"), v["verdict"])

    wr = L("wealth-resolution")
    ck("wealth 확정 산출물 존재", wr is not None)
    if wr:
        ck("§14 배정 상태 분류 기록", "byAllocationStatus" in wr)
        ck("§17 결과공학 금지 명시", len(wr["noResultEngineering"]) > 10)


# ═══════════════════ L5 regression ═══════════════════
def t_l5():
    print("[L5] R22~R16 회귀")
    ds = DS[:6]
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(1000, 10000), (100000, 100)] + [(100000, 100)] * 4, "역분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=synth(ds, rows))
        ck(f"{nm} wealth 불변",
           abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    d20 = P20.parse('<DOC><TABLE><TU ACODE="DST_CD">구주주</TU>'
                    '<TE ACODE="FST_DV_CNT">1,000</TE>'
                    '<TE ACODE="DV_ST_CNT">800</TE>'
                    '<TE ACODE="DV_AMT">800,000</TE></TABLE></DOC>')
    ck("R20 실적보고서 파서 회귀", d20["shareholderEntitledShares"] == 1000.0)
    ck("R20 확정발행가 회귀", abs(d20["finalIssuePrice"] - 1000.0) < 1e-9)

    d22 = P22.parse_final_price(P22.flatten(
        "<table><tr><td>확정발행가액</td><td>5,000원</td></tr></table>"))
    ck("R22 소문자 HTML 파서 회귀", d22.get("finalIssuePrice") == 5000.0, d22)
    ck("R22 절번호 오독 방지 회귀",
       P22.parse_final_price(P22.flatten(
           "<table><tr><td>확정발행가액 산정</td></tr>"
           "<tr><td>1. 발행예정내역</td></tr></table>")).get("finalIssuePrice")
       is None)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r19-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r21-foundation-verdict-latest.json",
              "r22-foundation-verdict-latest.json",
              "r22-full-reconciliation-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
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
              "wababa-final-27-holder-rights-terms-recovery-r22-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-final-11-holder-rights-allocation-recovery-r23-latest.json"
    ck("R23 보고서 JSON 존재", j.exists())
    if j.exists():
        d = json.loads(j.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure"):
            ck(f"{k} = 0", p[k] == 0)
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED",
           p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 scheduler 0", d["hotg"]["newScheduler"] == 0)
        ck("HOTG 신규 orchestration 0", d["hotg"]["newOrchestration"] == 0)
    m = WD / "wababa-final-11-holder-rights-allocation-recovery-r23-latest.md"
    ck("R23 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§32)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§32)", lines[1].startswith("reason_class:"), lines[1])
        body = "\n".join(lines)
        ck("§31 자체수정 공개", "자체수정" in body)
        ck("§16 manual ledger 포함", "manual ledger" in body)
        ck("§21 누적 감도 포함", "누적 감도" in body)
        ck("Founder 행동 명시", "Founder 행동" in body)
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
