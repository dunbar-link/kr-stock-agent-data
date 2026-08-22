#!/usr/bin/env python3
"""R20 회귀 — 실적보고서 파서 + 최종조건 invariant. 네트워크 0 · production write 0.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

계층:
  L1 parser    — 배정대상 분류 · 실발행/확정가 · 일정 · 실권 감지
  L2 resolve   — planned vs final 분리 · 일부발행 · 철회 · 수량 대조
  L3 wealth    — 외부납입 분리 · 가짜 gain 금지 · 실권 정책
  L4 real data — 실제 산출물(mock 아님) · anchor · bias · 판정
  L5 regression— R19/R18/R17/R16 · 삼성 · 분할 · 무상증자 · 유상증자

사용: python scripts/research/test_r20_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r20_parser as P  # noqa: E402
import r20_resolve as R  # noqa: E402

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
    p = RD / f"r20-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


DS = [f"20{20 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


def cell(code, val, tag="TE"):
    return f'<{tag} ACODE="{code}">{val}</{tag}>'


def alloc(label, fst, sub, dv, amt):
    """배정대상 1행."""
    return (cell("DST_CD", label, "TU") + cell("FST_DV_CNT", fst)
            + cell("SB_ST_CNT", sub) + cell("DV_ST_CNT", dv)
            + cell("DV_AMT", amt))


def report(rows, sb="2020년 05월 01일", se="2020년 05월 02일",
           pay="2020년 05월 10일"):
    return ("<DOC><TABLE>" + cell("SB_BGN_DT", sb, "TU")
            + cell("SB_END_DT", se, "TU") + cell("PYM_DT", pay, "TU")
            + "".join(rows) + "</TABLE></DOC>")


# ═══════ L1 parser ═══════
def t_l1():
    print("[L1] 실적보고서 파서")
    for lab, want in [("구주주", "SHAREHOLDER"), ("기존주주", "SHAREHOLDER"),
                      ("주주배정", "SHAREHOLDER"),
                      ("우리사주조합", "ESOP"),
                      ("기타 제3자 배정", "THIRD_PARTY"),
                      ("제삼자배정", "THIRD_PARTY"),
                      ("일반공모", "PUBLIC"), ("일반청약", "PUBLIC")]:
        ck(f"배정대상 '{lab}' → {want}", P.group_of(lab) == want, P.group_of(lab))
    ck("빈 라벨은 None", P.group_of("") is None)
    ck("우리사주가 주주보다 먼저 매칭",
       P.group_of("우리사주조합") == "ESOP")

    ck("숫자 파싱", P._num("5,308,124") == 5308124.0)
    ck("'-' 는 결측", P._num("-") is None)
    ck("한글 날짜", P._date("2007년 01월 12일") == "20070112")

    d = P.parse(report([
        alloc("우리사주조합", "2,000,000", "482,775", "482,775", "2,452,497,000"),
        alloc("구주주", "8,000,000", "5,308,124", "5,308,124", "26,965,269,920"),
        alloc("기타 제3자 배정", "-", "4,209,101", "4,209,101", "21,382,233,080"),
    ]))
    ck("구주주 그룹 감지", d["hasShareholderGroup"] is True)
    ck("구주주 권리(배정)주식수", d["shareholderEntitledShares"] == 8000000.0)
    ck("구주주 실배정주식수", d["shareholderAllocatedShares"] == 5308124.0)
    ck("제3자 배정분 분리", d["thirdPartyAllocatedShares"] == 4209101.0)
    ck("우리사주 분리", d["esopAllocatedShares"] == 482775.0)
    ck("총 발행주식수", d["finalNewSharesIssued"] == 10000000.0)
    ck("확정발행가 = 배정금액/배정주식수",
       abs(d["finalIssuePrice"] - 26965269920 / 5308124) < 1e-6)
    ck("확정발행가 출처 표기",
       d["finalIssuePriceSource"] == "SHAREHOLDER_DV_AMT_DIV_SHARES")
    ck("실권 감지", "PARTIAL_SUBSCRIPTION" in d["flags"])
    ck("청약률 계산", abs(d["shareholderTakeUpRate"] - 5308124 / 8000000) < 1e-9)
    ck("청약 시작일", d["subscriptionStart"] == "20200501")
    ck("납입일", d["paymentDate"] == "20200510")

    d2 = P.parse(report([
        alloc("구주주", "1,000,000", "1,000,000", "1,000,000", "5,000,000,000")]))
    ck("전량 청약이면 실권 flag 없음",
       "PARTIAL_SUBSCRIPTION" not in d2["flags"])
    ck("전량 청약 청약률 1.0", abs(d2["shareholderTakeUpRate"] - 1.0) < 1e-9)

    d3 = P.parse(report([
        alloc("기타 제3자 배정", "-", "500,000", "500,000", "1,000,000,000")]))
    ck("구주주 없으면 hasShareholderGroup False",
       d3["hasShareholderGroup"] is False)
    ck("구주주 없으면 권리주식수 None",
       d3["shareholderEntitledShares"] is None)
    ck("구주주 없어도 총 발행수는 나옴", d3["finalNewSharesIssued"] == 500000.0)

    d4 = P.parse("<DOC><TABLE></TABLE></DOC>")
    ck("배정표 없으면 flag", "NO_ALLOCATION_TABLE" in d4["flags"])
    ck("배정표 없으면 추정하지 않음", d4.get("finalIssuePrice") is None)


# ═══════ L2 resolve ═══════
def t_l2():
    print("[L2] 최종조건 확정 · planned vs final")
    docs = {"R1": P.parse(report([
        alloc("구주주", "500,000", "400,000", "400,000", "400,000,000")]))}
    t = {"ticker": "X", "date": "2020-06-01",
         "observedSharesBefore": 1_000_000, "observedSharesAfter": 1_400_000,
         "plannedRightsRatio": 0.5, "plannedIssuePrice": 1000.0,
         "shareRatio": 1.4}
    fl = [{"rcept_no": "R1", "rcept_dt": "20200610",
           "kind": "ISSUE_RESULT_REPORT", "report_nm": "증권발행실적보고서[주식]"}]
    r = R.resolve_one(t, fl, docs)
    ck("실적보고서 매칭", r["finalSource"] is not None)
    ck("실제 발행수 사용", r["finalNewSharesIssued"] == 400000.0)
    ck("권리비율 = 배정/기존", abs(r["entitlementRightsRatio"] - 0.5) < 1e-9)
    ck("청약비율 별도 기록", abs(r["takeUpRightsRatio"] - 0.4) < 1e-9)
    ck("권리 기준은 ENTITLEMENT", r["ratioBasisUsed"] == "ENTITLEMENT")
    ck("확정발행가 사용", abs(r["finalIssuePrice"] - 1000.0) < 1e-9)
    ck("실권 반영", r["eventStatus"] == "PARTIALLY_COMPLETED")
    ck("wealth 확정", r["wealthStatus"] == "WEALTH_CONFIRMED")
    ck("직접 권리 있음", r["holderRightFinal"] is True)
    ck("planned 값 보존", r["plannedRightsRatio"] == 0.5)
    ck("final 값 별도 필드", "finalShareholderEntitledShares" in r)
    ck("수량 대조 오차 기록", r.get("reportedVsObservedRelErr") is not None)
    ck("수량 일치하면 HIGH", r["confidence"] == "HIGH", r["confidence"])

    # 계획과 최종이 다르면 flag
    t2 = dict(t, plannedRightsRatio=0.9)
    r2 = R.resolve_one(t2, fl, docs)
    ck("계획 비율 차이 감지", "PLANNED_RATIO_DIFFERS" in r2["flags"])
    ck("계획이 아니라 최종을 씀",
       abs(r2["entitlementRightsRatio"] - 0.5) < 1e-9)
    t3 = dict(t, plannedIssuePrice=5000.0)
    r3 = R.resolve_one(t3, fl, docs)
    ck("계획 발행가 차이 감지", "PLANNED_PRICE_DIFFERS" in r3["flags"])

    # 실적보고서 없음 → 계획치로 승격 금지
    r4 = R.resolve_one(t, [], {})
    ck("실적보고서 없으면 미확정", r4["eventStatus"] == "NO_RESULT_REPORT")
    ck("계획치로 승격 안함", r4["wealthStatus"] == "WEALTH_PARTIAL")
    ck("미확정은 권리 주장 안함", r4.get("holderRightFinal") is False)

    # 구주주 배정이 없고 수량이 맞으면 → 실제로는 제3자·공모
    docs5 = {"R5": P.parse(report([
        alloc("기타 제3자 배정", "-", "400,000", "400,000", "400,000,000")]))}
    fl5 = [{"rcept_no": "R5", "rcept_dt": "20200610",
            "kind": "ISSUE_RESULT_REPORT", "report_nm": "증권발행실적보고서[주식]"}]
    r5 = R.resolve_one(t, fl5, docs5)
    ck("구주주 없고 수량 일치 → 제3자 확정",
       r5["eventStatus"] == "COMPLETED_WITHOUT_SHAREHOLDER_ALLOCATION")
    ck("제3자 확정은 wealth 확정", r5["wealthStatus"] == "WEALTH_CONFIRMED")
    ck("제3자 확정은 권리 없음", r5["holderRightFinal"] is False)
    ck("제3자 확정 비율 0", r5["entitlementRightsRatio"] == 0.0)
    ck("계획-최종 방식 변경 flag",
       "PLANNED_SHAREHOLDER_BUT_FINAL_NOT" in r5["flags"])

    # 수량이 전혀 안 맞으면 승격 금지
    t6 = dict(t, observedSharesAfter=9_000_000)
    r6 = R.resolve_one(t6, fl5, docs5)
    ck("수량 불일치면 제3자 확정 안함",
       r6["eventStatus"] == "NO_RESULT_REPORT", r6["eventStatus"])


# ═══════ L3 wealth ═══════
def t_l3():
    print("[L3] wealth — 외부납입 분리 · 실권 정책")
    cap = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)
    m = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
          "label": "CONFIRMED_RIGHTS", "holderRight": True,
          "dartRatio": 0.5, "issuePriceDerived": 6000.0}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("공정 유상증자 wealth 중립", abs(r["twr"]) < 1e-9, r["twr"])
    ck("외부납입 분리", abs(r["externalContribution"] - 3000.0) < 1e-6)
    ck("납입금이 수익이 아님", abs(r["twr"]) < 1e-9 and r["externalContribution"] > 0)

    cap2 = synth(DS, [(10000, 100)] + [(5000, 150)] * 7)
    eng2 = C.CanonicalWealth(DS, cap=cap2)
    m2 = [dict(m[0], issuePriceDerived=9000.0)]
    r2 = W.RightsWealth(eng2, m2).run("X", 0, 1, reinvest=False)
    ck("K >= 권리락가 → 실권(R17 정본)", abs(r2["endShares"] - 1.0) < 1e-9)
    ck("실권이면 납입 0", r2["externalContribution"] == 0.0)

    m3 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
           "label": "CONFIRMED_NON_RIGHTS", "holderRight": False}]
    r3 = W.RightsWealth(eng2, m3).run("X", 0, 1, reinvest=False)
    ck("권리 없으면 보유주식 불변", abs(r3["endShares"] - 1.0) < 1e-9)
    ck("권리 없으면 공짜 신주 없음", r3["externalContribution"] == 0.0)

    # 계획치(과대)로 계산하면 가짜 wealth 가 생긴다는 것을 보인다
    big = [dict(m[0], dartRatio=1.5)]
    rb = W.RightsWealth(eng, big).run("X", 0, 1, reinvest=False)
    ck("과대 비율은 다른 결과를 낸다(계획치 위험)",
       abs(rb["twr"] - r["twr"]) > 0.05)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 산출물 — mock 이 아니다")
    tg = L("targets-precommit")
    ck("대상 precommit 존재", tg is not None)
    if tg:
        ck("결과 이전 작성", tg["writtenBeforePostIssuanceResults"] is True)
        ck("대상 92건", tg["targetTotal"] == 92, tg["targetTotal"])
        ck("threshold 변경 없음", tg["verdict"]["unchanged"] is True)
        ck("계획치 승격 금지 명시", "승격 금지" in tg["issuePrice"]["ifOnlyPlanned"])
        ck("실권 정책 재사용 명시", "R17 정본" in tg["wealth"]["lapseRule"])
        ck("bias 계산법 유지", "새 계산식" in tg["bias"]["notAllowed"])
        ck("R19 투영 산수 경고", "분모" in tg["bias"]["arithmeticNote"])
        ck("범위 확대 금지", "확장 금지" in tg["scopeGuard"])

    pf = L("post-issuance-filings")
    ck("사후공시 수집 존재", pf is not None)
    if pf:
        ck("실적보고서 수집됨",
           pf["filingKinds"].get("ISSUE_RESULT_REPORT", 0) > 0)
        ck("파생결합증권 제외 명시", "ELS" in pf["derivativeIgnoredNote"])
        ck("원문 확보됨", pf["documentsNew"] + pf["documentsCacheHits"] > 0)

    ft = L("final-terms-normalized")
    ck("최종조건 산출물 존재", ft is not None)
    if ft:
        ck("92건 전수", ft["targets"] == 92)
        ck("wealth 확정 존재", ft["byWealthStatus"].get("WEALTH_CONFIRMED", 0) > 0)
        ck("권리 기준 ENTITLEMENT", ft["ratioBasis"]["used"] == "ENTITLEMENT")
        ck("청약률도 기록", "takeUp" in ft["ratioBasis"]["alsoRecorded"])
        ck("layer 분리 기록", len(ft["layerSeparation"]) >= 3)
        ck("계획-최종 차이 관측",
           ft["flags"].get("PLANNED_RATIO_DIFFERS", 0) > 0)
        ck("실권 관측", ft["flags"].get("PARTIAL_SUBSCRIPTION", 0) > 0)
        ck("확정발행가 복원률 기록",
           ft["fieldRecovery"]["finalIssuePrice"]["n"] > 0)

    ch = L("correction-chains")
    ck("정정 chain 존재", ch is not None)
    if ch:
        ck("실적보고서 우선 명시", "실적보고서" in ch["policy"])

    sc = L("share-count-reconciliation")
    ck("주식수 대조 존재", sc is not None)
    if sc:
        ck("강제 금지 명시", "강제" in sc["hardRule"])
        ck("다른 자본행위 분리 명시", "R19" in sc["method"])
        ck("불일치 건수 집계", sc["unexplainedConflict"] is not None)

    an = L("anchor-cases")
    ck("anchor 존재", an is not None)
    if an:
        ck("anchor 10건 이상 (§15)", an["atLeastTen"], an["obtained"])
        ck("anchor 전부 manual 일치", an["allPass"])
        ck("정책 출처 R17", "R17" in an["policySource"])
        labels = {c["case"] for c in an["cases"]}
        for want in ("대형", "중형", "소형"):
            ck(f"{want} anchor 있음", want in labels)
        ck("실권 다수 anchor 있음", "실권 다수" in labels)
        ck("정정 다수 anchor 있음", "정정 다수" in labels)
        ck("anchor 에 계획비율 병기",
           all("plannedRightsRatio" in c["manual"] for c in an["cases"]))
        ck("anchor 에 실발행량 기록",
           all("actualIssuedShares" in c["manual"] for c in an["cases"]))

    ct = L("ex-rights-continuity")
    ck("연속성 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0 (§14)", ct["noFreeWealth"] is True)
        ck("차분 = 이론 권리가치", ct["diffMatchesTheory"] is True)
        ck("당월 차분 음수 0", ct["eventMonthDiffNeverNegative"] is True)

    b = L("expected-bias")
    ck("기대편향 존재", b is not None)
    if b:
        ck("공식 R19 유지", b["formulaUnchangedFromR19"] is True)
        ck("R18~R20 progression",
           set(b["progression"]) == {"R18", "R19", "R20"})
        ck("R19 값 보존", b["progression"]["R19"] == 5.316)
        ck("분모 효과 설명", "분모" in b["arithmeticNote"])
        ck("기여도 분해", len(b["decomposition"]) >= 3)
        ck("P 근거 표기",
           all("pSource" in c for c in b["decomposition"].values()))

    mt = L("unresolved-materiality")
    ck("materiality 존재", mt is not None)
    if mt:
        ck("R16~R20 progression",
           set(mt["progression"]) == {"R16", "R17", "R18", "R19", "R20"})
        ck("R19 값 보존", mt["progression"]["R19"] == 6.74)
        ck("편향 방향 과소평가",
           mt["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")

    rec = L("full-reconciliation")
    ck("전체 재조정 존재", rec is not None)
    if rec:
        ck("2,742건 전수", rec["total"] == 2742, rec["total"])
        ck("R20 merge 92건", rec["r20Merged"] == 92, rec["r20Merged"])

    v = L("foundation-verdict")
    ck("판정 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 변경 없음 (§20)", v["thresholdsUnchanged"] is True)
        ck("bias 공식 변경 없음 (§18)", v["biasFormulaUnchanged"] is True)
        ck("2% 기준 그대로",
           v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("20% 기준 그대로",
           v["thresholdsFromPrecommit"]["passWithLimitsMaxTickerPct"] == 20.0)
        for k in ("R11_BM", "R14_QUALITY", "SIZE"):
            ck(f"{k} PROVISIONAL 유지",
               "PROVISIONAL" in v["legacyResearchStatus"][k])
        ck("legacy 유지 (§23)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        ck("자동 승격 금지 명시",
           "canonical 로 올리지" in v["legacyResearchStatus"]["note"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))


# ═══════ L5 regression ═══════
def t_l5():
    print("[L5] R19~R16 회귀")
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
              "r19-full-reconciliation-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-dart-rights-issue-canonical-recovery-r17-latest.md",
              "wababa-dart-legacy-rights-document-recovery-r18-latest.md",
              "wababa-no-direct-match-capital-action-recovery-r19-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════ production ═══════
def t_prod():
    print("[prod] production 보호 (§27)")
    rep = WD / "wababa-holder-rights-final-terms-recovery-r20-latest.json"
    ck("R20 보고서 JSON 존재", rep.exists())
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
    m = WD / "wababa-holder-rights-final-terms-recovery-r20-latest.md"
    ck("R20 보고서 MD 존재", m.exists())
    if m.exists():
        head = m.read_text(encoding="utf-8").splitlines()[0]
        ck("첫 줄 전체 판정 (§31)", head.startswith("전체 판정:"), head)


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
