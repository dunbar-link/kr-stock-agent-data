#!/usr/bin/env python3
"""R22 회귀 — 확대창 사후공시 복원 invariant. 네트워크 0 · production write 0.

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22

계층:
  L1 parser  — 자율공시(소문자 HTML) 라벨 추출 · 절번호 오독 방지
  L2 resolve — 소스 우선순위 · planned vs final · 일부발행 · 수량 대조
  L3 wealth  — 외부납입 분리 · 실권 정책 · 공짜 wealth 금지
  L4 real    — 실제 산출물(mock 아님) · anchor · bias · 판정
  L5 regress — R21/R20/R19/R18/R17/R16 · 삼성 · 분할 · 무상증자 · 유상증자

사용: python scripts/research/test_r22_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r20_parser as P20  # noqa: E402
import r22_parser as P  # noqa: E402
import r22_resolve as R  # noqa: E402

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
    p = RD / f"r22-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


DS = [f"20{20 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


def html(*rows):
    """소문자 HTML 표 — 거래소 자율공시 형식."""
    body = "".join(f"<tr><td><span>{a}</span></td>"
                   f"<td><span>{b}</span></td></tr>" for a, b in rows)
    return ("<html><head><style>.x{color:red}</style></head>"
            f"<body><table>{body}</table></body></html>")


# ═══════ L1 parser ═══════
def t_l1():
    print("[L1] 자율공시 파서")
    d = P.parse("SUBSCRIPTION_RESULT", html(
        ("1. 제목", "주주배정 유상증자 청약결과"),
        ("2. 주요내용",
         "(3) 모집주식수 : 12,000,000주<br/>(4) 청약주식수 : 5,757,450주 (47.98%)"
         "<br/>(5) 단수주 및 실권주식수 : 6,242,550주 (52.02%)")))
    ck("청약결과 모집주식수", d["offeredShares"] == 12000000.0, d["offeredShares"])
    ck("청약결과 청약주식수", d["subscribedShares"] == 5757450.0)
    ck("청약결과 실권주식수", d["unsubscribedShares"] == 6242550.0)
    ck("청약결과 주주배정 인식", d["isShareholderAllocation"] is True)
    ck("청약률 계산", abs(d["takeUpRate"] - 5757450 / 12000000) < 1e-9)
    ck("실권 flag", "PARTIAL_SUBSCRIPTION" in d["flags"])

    d = P.parse("FINAL_PRICE", html(
        ("유상증자 최종발행가액 확정", ""), ("1. 발행예정내역", ""),
        ("나. 주식수(주)", "20,000,000"), ("2. 확정발행가액(1주당)", ""),
        ("가. 확정가액(원)", "500원"), ("3. 액면가(원)", "500원")))
    ck("확정발행가 추출", d["finalIssuePrice"] == 500.0, d["finalIssuePrice"])
    ck("★ 절 번호를 값으로 읽지 않음", d["finalIssuePrice"] not in (1.0, 2.0, 3.0))
    ck("발행예정 주식수는 별도 필드",
       d["plannedSharesInNotice"] == 20000000.0)

    d = P.parse("ISSUE_COMPLETION", html(
        ("1. 증권의 종류", "기명식 보통주"), ("2. 발행방법", "주주배정 유상증자"),
        ("발행예정주식수(주)", "12,000,000"),
        ("발행예정금액(원)", "12,000,000,000원"),
        ("실제발행주식수(주)", "10,000,000"),
        ("실제발행금액(원)", "10,000,000,000원"), ("납입일", "2009-07-29")))
    ck("발행결과 실제발행주식수", d["issuedShares"] == 10000000.0)
    ck("발행결과 발행예정주식수", d["plannedShares"] == 12000000.0)
    ck("발행결과 주주배정 인식", d["isShareholderAllocation"] is True)
    ck("발행결과 사채 아님", d["isBondNotEquity"] is False)
    ck("발행결과 납입일", d["paymentDate"] == "20090729")
    ck("발행가 파생", abs(d["derivedIssuePrice"] - 1000.0) < 1e-9)
    ck("부분발행 flag", "PARTIAL_ISSUANCE" in d["flags"])

    d = P.parse("ISSUE_COMPLETION", html(
        ("1. 증권의 종류", "무기명 무보증 전환사채"),
        ("2. 발행방법", "국내공모 전환사채 발행결정"),
        ("실제발행주식수(주)", "-"), ("실제발행금액(원)", "895,000,000원")))
    ck("사채 발행결과는 주식 아님", d["isBondNotEquity"] is True)
    ck("사채는 유상증자 아님", d["isRights"] is False)

    d = P.parse("NEW_LISTING", html(
        ("2.추가주식의 종류와 수", ""), ("추가주식수(주)", "16,151,794"),
        ("4.추가상장후 총발행주식수", ""), ("주식수(주)", "32,531,794"),
        ("5.상장일", "2007년 08월 20일")))
    ck("추가상장 신주수", d["addedShares"] == 16151794.0, d["addedShares"])
    ck("추가상장 상장일", d["listingDate"] == "20070820")

    ck("빈 문서는 추정하지 않음",
       P.parse("FINAL_PRICE", html(("무관", "내용")))["finalIssuePrice"] is None)


# ═══════ L2 resolve ═══════
def t_l2():
    print("[L2] 소스 우선순위 · planned vs final")
    t = {"ticker": "X", "date": "2020-06-01",
         "observedSharesBefore": 1_000_000, "observedSharesAfter": 1_400_000,
         "observedShareDelta": 400_000, "shareRatio": 1.4,
         "plannedRightsRatio": 0.5, "plannedIssuePrice": 1500.0}

    def fil(kind, rn, dt, nm="x"):
        return {"rcept_no": rn, "rcept_dt": dt, "kind": kind, "report_nm": nm}

    # 실적보고서가 있으면 1순위
    import r22_resolve as RR
    real = RR._read

    def fake(rn):
        return DOCS.get(rn)
    DOCS = {
        "IRR": ('<DOC><TABLE>'
                '<TU ACODE="DST_CD">구주주</TU>'
                '<TE ACODE="FST_DV_CNT">500,000</TE>'
                '<TE ACODE="SB_ST_CNT">400,000</TE>'
                '<TE ACODE="DV_ST_CNT">400,000</TE>'
                '<TE ACODE="DV_AMT">400,000,000</TE>'
                '</TABLE></DOC>'),
        "IC": html(("1. 증권의 종류", "기명식 보통주"),
                   ("2. 발행방법", "주주배정 유상증자"),
                   ("발행예정주식수(주)", "500,000"),
                   ("실제발행주식수(주)", "400,000"),
                   ("실제발행금액(원)", "400,000,000원"),
                   ("납입일", "2020-06-10")),
        "FP": html(("유상증자 최종발행가액 확정", ""), ("1. 발행예정내역", ""),
                   ("가. 확정가액(원)", "1,000원")),
    }
    RR._read = fake
    try:
        r = R.resolve_one(t, [fil("ISSUE_RESULT_REPORT", "IRR", "20200610")])
        ck("1순위 실적보고서 사용",
           r["sharesSource"] == "ISSUE_RESULT_REPORT", r["sharesSource"])
        ck("배정량 = FST_DV_CNT", r["finalShareholderEntitledShares"] == 500000.0)
        ck("권리비율 = 배정/기존", abs(r["finalRightsRatio"] - 0.5) < 1e-9)
        ck("확정발행가", abs(r["finalIssuePrice"] - 1000.0) < 1e-9)
        ck("wealth 확정", r["wealthStatus"] == "WEALTH_CONFIRMED")
        ck("권리 있음", r["holderRightFinal"] is True)
        ck("실권 반영", "PARTIAL_SUBSCRIPTION" in r["flags"])

        # 실적보고서가 없으면 발행결과로 우회
        r = R.resolve_one(t, [fil("ISSUE_COMPLETION", "IC", "20200620")])
        ck("2순위 발행결과 우회", r["sharesSource"] == "ISSUE_COMPLETION")
        ck("★ 주주배정이면 발행예정 = 배정량",
           r["finalShareholderEntitledShares"] == 500000.0)
        ck("실제발행 = 배정 결과", r["finalShareholderAllocatedShares"] == 400000.0)
        ck("발행가 파생 사용", r["priceSource"] == "ISSUE_COMPLETION_DERIVED")
        ck("우회로도 wealth 확정", r["wealthStatus"] == "WEALTH_CONFIRMED")

        # 발행가만 있으면 확정 불가 — 계획치로 승격 금지
        r = R.resolve_one(t, [fil("FINAL_PRICE", "FP", "20200605")])
        ck("발행가만으로는 미확정", r["wealthStatus"] == "WEALTH_PARTIAL")
        ck("계획치로 승격 안함", r.get("holderRightFinal") is False)
        ck("배정량 없으면 비율 None", r["finalRightsRatio"] is None)

        # 증거 없음
        r = R.resolve_one(t, [])
        ck("증거 없으면 미확정", r["completionStatus"] == "NO_ACTUAL_EVIDENCE")
        ck("증거 없으면 WEALTH_PARTIAL", r["wealthStatus"] == "WEALTH_PARTIAL")

        # 계획 대비 차이 감지
        r = R.resolve_one(dict(t, plannedRightsRatio=0.9, plannedIssuePrice=5000.0),
                          [fil("ISSUE_RESULT_REPORT", "IRR", "20200610")])
        ck("계획 비율 차이 감지", "PLANNED_RATIO_DIFFERS" in r["flags"])
        ck("계획 발행가 차이 감지", "PLANNED_PRICE_DIFFERS" in r["flags"])
        ck("계획이 아니라 최종을 씀",
           abs(r["finalRightsRatio"] - 0.5) < 1e-9)

        # 창 밖은 무시
        r = R.resolve_one(t, [fil("ISSUE_RESULT_REPORT", "IRR", "20250610")])
        ck("창 밖 공시 무시", r["completionStatus"] == "NO_ACTUAL_EVIDENCE")
    finally:
        RR._read = real


# ═══════ L3 wealth ═══════
def t_l3():
    print("[L3] wealth — 외부납입·실권·공짜 금지")
    cap = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)
    m = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
          "label": "CONFIRMED_RIGHTS", "holderRight": True,
          "dartRatio": 0.5, "issuePriceDerived": 6000.0}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("공정 유상증자 wealth 중립", abs(r["twr"]) < 1e-9)
    ck("외부납입 분리", abs(r["externalContribution"] - 3000.0) < 1e-6)
    ck("납입금이 수익 아님", abs(r["twr"]) < 1e-9 and r["externalContribution"] > 0)

    cap2 = synth(DS, [(10000, 100)] + [(5000, 150)] * 7)
    eng2 = C.CanonicalWealth(DS, cap=cap2)
    r2 = W.RightsWealth(eng2, [dict(m[0], issuePriceDerived=9000.0)]).run(
        "X", 0, 1, reinvest=False)
    ck("K >= 권리락가 → 실권(R17 정본)", abs(r2["endShares"] - 1.0) < 1e-9)
    ck("실권이면 납입 0", r2["externalContribution"] == 0.0)

    r3 = W.RightsWealth(eng2, [{"ticker": "X", "date": DS[1], "shareRatio": 1.5,
                                "label": "CONFIRMED_NON_RIGHTS",
                                "holderRight": False}]).run(
        "X", 0, 1, reinvest=False)
    ck("권리 없으면 보유주식 불변", abs(r3["endShares"] - 1.0) < 1e-9)
    ck("권리 없으면 공짜 신주 없음", r3["externalContribution"] == 0.0)

    big = W.RightsWealth(eng, [dict(m[0], dartRatio=1.5)]).run(
        "X", 0, 1, reinvest=False)
    ck("과대 비율은 다른 결과(계획치 위험)", abs(big["twr"] - r["twr"]) > 0.05)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 산출물 — mock 이 아니다")
    tg = L("targets-precommit")
    ck("대상 precommit 존재", tg is not None)
    if tg:
        ck("결과 이전 작성", tg["writtenBeforeExpandedSearchResults"] is True)
        ck("대상 27건", tg["targetTotal"] == 27, tg["targetTotal"])
        ck("threshold 변경 없음", tg["verdict"]["unchanged"] is True)
        ck("창 확대 기록", tg["window"]["months"] == [-3, 18])
        ck("R20 창 보존", tg["window"]["r20Window"] == [-6, 9])
        ck("R20 대비 차이 기록", "단순 반복하지 않는다" in
           tg["howThisDiffersFromR20"]["why"])
        ck("계획치 승격 금지", "승격 금지" in tg["issuePrice"]["ifOnlyPlanned"])
        ck("실권 정책 재사용", "R17 정본" in tg["wealth"]["lapseRule"])
        ck("bias 식 유지", "새 계산식" in tg["bias"]["notAllowed"])
        ck("R21 투영 경고", "전부" in tg["bias"]["projectionCaveat"])
        ck("범위 고정", "추가·삭제 금지" in tg["scopeGuard"])

    ex = L("expanded-window-filings")
    ck("확대창 수집 존재", ex is not None)
    if ex:
        ck("확장 유형 검색", len(ex["kindsSearched"]) >= 6)
        ck("원문 확보", ex["documentsNew"] + ex["documentsCacheHits"] > 0)
        ck("helper 재사용 명시", "r18_doc_collect" in ex["helpersReused"])
        ck("ELS 제외 명시", "파생결합" in ex["derivativeExcluded"])

    ft = L("final-terms-normalized")
    ck("최종조건 존재", ft is not None)
    if ft:
        ck("27건 전수", ft["targets"] == 27)
        ck("wealth 확정 존재",
           ft["byWealthStatus"].get("WEALTH_CONFIRMED", 0) > 0)
        ck("권리 기준 ENTITLEMENT", ft["ratioBasis"]["used"] == "ENTITLEMENT")
        ck("여러 소스 사용", len(ft["sourceUsage"]) >= 3)
        ck("layer 분리 기록", len(ft["layerSeparation"]) >= 3)
        ck("계획-최종 차이 관측",
           ft["flags"].get("PLANNED_RATIO_DIFFERS", 0) > 0)

    ch = L("correction-chains")
    ck("정정 chain 존재", ch is not None)
    if ch:
        ck("사후 실제 우선 명시", "사후 실제 결과" in ch["policy"])

    nl = L("new-listing-reconciliation")
    ck("주식수 대조 존재", nl is not None)
    if nl:
        ck("강제 귀속 금지 명시", "전부 귀속하지 않는다" in nl["hardRule"])
        ck("불일치 집계", nl["unexplainedConflict"] is not None)

    cc = L("capital-change-reconciliation")
    ck("자본금 변동 대조 존재", cc is not None)
    if cc:
        ck("단독 확정 금지 명시", "단독으로" in cc["note"])

    an = L("anchor-cases")
    ck("anchor 존재", an is not None)
    if an:
        ck("anchor 10건 이상 (§18)", an["atLeastTen"], an["obtained"])
        ck("anchor 전부 manual 일치", an["allPass"])
        ck("정책 출처 R17", "R17" in an["policySource"])
        labels = {c["case"] for c in an["cases"]}
        for want in ("대형", "중형", "소형"):
            ck(f"{want} anchor 있음", want in labels)
        ck("anchor 에 계획비율 병기",
           all("plannedRightsRatio" in c["manual"] for c in an["cases"]))

    ct = L("ex-rights-continuity")
    ck("연속성 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0 (§19)", ct["noFreeWealth"] is True)
        ck("차분 = 이론 권리가치", ct["diffMatchesTheory"] is True)

    b = L("expected-bias")
    ck("기대편향 존재", b is not None)
    if b:
        ck("공식 R21 유지", b["formulaUnchangedFromR21"] is True)
        ck("R18~R22 progression",
           set(b["progression"]) == {"R18", "R19", "R20", "R21", "R22"})
        ck("R21 값 보존", b["progression"]["R21"] == 3.470)
        ck("R21 투영 대비 설명", "전부" in b["projectionVsActual"])
        ck("감도표 존재", len(b["gateSensitivity"]["ifGroupFullyResolved"]) >= 3)
        ck("기여도 분해", len(b["decomposition"]) >= 3)

    mt = L("unresolved-materiality")
    ck("materiality 존재", mt is not None)
    if mt:
        ck("R16~R22 progression", len(mt["progression"]) == 7)
        ck("R21 값 보존", mt["progression"]["R21"] == 3.36)
        ck("미해결 감소", mt["progression"]["R22"] <= mt["progression"]["R21"])
        ck("편향 방향 과소평가",
           mt["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")

    rec = L("full-reconciliation")
    ck("전체 재조정 존재", rec is not None)
    if rec:
        ck("2,742건 전수", rec["total"] == 2742, rec["total"])
        ck("R22 merge 27건", rec["r22Merged"] == 27, rec["r22Merged"])

    v = L("foundation-verdict")
    ck("판정 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 변경 없음 (§21)", v["thresholdsUnchanged"] is True)
        ck("bias 공식 변경 없음 (§20)", v["biasFormulaUnchanged"] is True)
        ck("2% 기준 그대로",
           v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("20% 기준 그대로",
           v["thresholdsFromPrecommit"]["passWithLimitsMaxTickerPct"] == 20.0)
        for k in ("R11_BM", "R14_QUALITY", "SIZE"):
            ck(f"{k} PROVISIONAL 유지",
               "PROVISIONAL" in v["legacyResearchStatus"][k])
        ck("legacy 유지 (§24)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        ck("자동 승격 금지", "자동 canonical" in v["legacyResearchStatus"]["note"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))


# ═══════ L5 regression ═══════
def t_l5():
    print("[L5] R21~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(1000, 10000), (100000, 100)] + [(100000, 100)] * 4, "역분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=synth(ds, rows))
        r = e.get_total_return("X", ds[0], ds[2])
        ck(f"{nm} wealth 불변", abs(r["cumulativeReturn"]) < 1e-9)

    # R20 파서(대문자 ACODE)는 그대로 동작해야 한다
    d = P20.parse('<DOC><TABLE><TU ACODE="DST_CD">구주주</TU>'
                  '<TE ACODE="FST_DV_CNT">1,000</TE>'
                  '<TE ACODE="DV_ST_CNT">800</TE>'
                  '<TE ACODE="DV_AMT">800,000</TE></TABLE></DOC>')
    ck("R20 실적보고서 파서 회귀", d["shareholderEntitledShares"] == 1000.0)
    ck("R20 확정발행가 회귀", abs(d["finalIssuePrice"] - 1000.0) < 1e-9)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r19-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r21-foundation-verdict-latest.json",
              "r21-full-reconciliation-latest.json"):
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
              "wababa-low-confidence-direct-entitlement-recovery-r21-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════ production ═══════
def t_prod():
    print("[prod] production 보호 (§28)")
    rep = WD / "wababa-final-27-holder-rights-terms-recovery-r22-latest.json"
    ck("R22 보고서 JSON 존재", rep.exists())
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
    m = WD / "wababa-final-27-holder-rights-terms-recovery-r22-latest.md"
    ck("R22 보고서 MD 존재", m.exists())
    if m.exists():
        head = m.read_text(encoding="utf-8").splitlines()[0]
        ck("첫 줄 전체 판정 (§32)", head.startswith("전체 판정:"), head)


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
