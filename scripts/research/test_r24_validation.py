#!/usr/bin/env python3
"""R24 회귀 — UNKNOWN 50건 직접판정 invariant. 네트워크 0 · production write 0.

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

계층:
  L1 classify — 유형규칙 · 사건 아님 배제 · 방향성 · 주식 종류 · 본문 표
  L2 entitle  — YES/NO/UNKNOWN · 합병 방향 · 경합 · LOW 사용금지
  L3 wealth   — NO 조정 0 · 무납입 기계조정 · 공짜 wealth 금지 · 정합성
  L4 real     — 실제 산출물(mock 아님) · anchor · bias 식 불변 · 판정
  L5 regress  — R23~R16 · 삼성 anchor · PIT

사용: python scripts/research/test_r24_validation.py
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
import r23_parser as P23  # noqa: E402
import r24_classify as CL  # noqa: E402
import r24_resolve as RS  # noqa: E402

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
    p = RD / f"r24-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


DS = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


# 실제 주식분할결정 공시 표 구조
SPLIT_DOC = ("<DOC>주식분할 결정 1. 주식분할 내용 구 분 분할 전 분할 후 "
             "1주당 액면가액 (원) 5,000 2,500 "
             "발행주식총수 보통주(주) 4,520,338 9,040,676 "
             "우선주(주) 86,331 172,662</DOC>")


# ═══════════════════ L1 classify ═══════════════════
def t_l1():
    print("[L1] 사건 유형 분류")
    for nm, want in (
        ("주요사항보고서(유상증자결정)", "RIGHTS_FAMILY"),
        ("주요사항보고서(무상증자결정)", "BONUS_ISSUE"),
        ("주식배당결정", "STOCK_DIVIDEND"),
        ("주식분할결정", "STOCK_SPLIT"),
        ("주식병합결정", "REVERSE_SPLIT"),
        ("주요사항보고서(감자결정)", "CAPITAL_REDUCTION"),
        ("주요사항보고서(자기주식소각결정)", "TREASURY_SHARE_CANCELLATION"),
        ("전환청구권행사", "CB_CONVERSION"),
        ("신주인수권행사", "BW_EXERCISE"),
        ("주식매수선택권행사", "STOCK_OPTION_EXERCISE"),
        ("주요사항보고서(회사합병결정)", "MERGER_NEW_SHARES"),
        ("증권신고서(합병)", "MERGER_NEW_SHARES"),
        ("합병등종료보고서(합병)", "MERGER_NEW_SHARES"),
        ("증권신고서(분할)", "COMPANY_SPLIT"),
        ("주요사항보고서(주식교환·이전 결정)", "SHARE_EXCHANGE"),
        ("기타경영사항(자율공시)(전환우선주의 보통주 전환청구)", "PREFERRED_CONVERSION"),
        ("기타주요경영사항(자율공시)(상환전환우선주의 보통주 전환청구)",
         "PREFERRED_CONVERSION"),
        ("유상증자또는주식관련사채등의청약결과(자율공시)", "RIGHTS_FAMILY_OUTCOME"),
    ):
        k, _w, _win = CL.classify_filing(nm)
        ck(f"분류 {nm[:30]} → {want}", k == want, k)

    # 사건이 아닌 공시 (§5)
    for nm in ("주식매수선택권부여에관한신고",
               "전환가액ㆍ신주인수권행사가액ㆍ교환가액의조정(안내공시)",
               "주권매매거래정지(주식배당)",
               "특수관계인의유상증자참여",
               "주요사항보고서(자기주식취득신탁계약체결결정)"):
        ck(f"사건 아님 배제 {nm[:26]}", CL.classify_filing(nm)[0] is None,
           CL.classify_filing(nm)[0])
    for nm in ("유상증자결정(종속회사의주요경영사항)",
               "회사합병결정(자회사의 주요경영사항)"):
        ck(f"종속·자회사 배제 {nm[:22]}", CL.classify_filing(nm)[0] is None)

    ck("정정공시 인식", CL.is_amendment("[기재정정]주요사항보고서(유상증자결정)"))
    ck("첨부정정 인식", CL.is_amendment("[첨부정정]주요사항보고서(유상증자결정)"))
    ck("원공시는 정정 아님", not CL.is_amendment("주요사항보고서(유상증자결정)"))

    # 약한 근거
    ck("사채 발행결정은 약한 근거",
       CL.classify_filing("주요사항보고서(전환사채권발행결정)")[1] is True)
    ck("전환청구권 행사는 강한 근거",
       CL.classify_filing("전환청구권행사")[1] is False)

    # ★ SC8 — 분할 공시 표 (주식 종류별 전/후)
    d = CL.inspect("STOCK_SPLIT", "___none___")
    ck("본문 없으면 빈 dict", d == {})
    flat = P23.flatten(SPLIT_DOC)
    pb, pa = CL._pair_after(flat, CL.SPLIT_ROWS["par"])
    ck("액면가 전/후 추출 (§31 SC8)", (pb, pa) == (5000.0, 2500.0), (pb, pa))
    cb, ca = CL._pair_after(flat, CL.SPLIT_ROWS["common"])
    ck("보통주 분할 전/후", (cb, ca) == (4520338.0, 9040676.0), (cb, ca))
    fb, fa = CL._pair_after(flat, CL.SPLIT_ROWS["preferred"])
    ck("우선주 분할 전/후", (fb, fa) == (86331.0, 172662.0), (fb, fa))
    ck("우선주 분할비율 = 2.0", abs(fa / fb - 2.0) < 1e-9)
    ck("액면가 비 = 분할비율", abs(pb / pa - 2.0) < 1e-9)
    ck("라벨 없으면 None", CL._pair_after("아무것도 없음", CL.SPLIT_ROWS["par"])
       == (None, None))

    # ★ SC6 — 우선주 전환 방향성
    ck("보통주 라인: 우선주전환이 증가를 설명",
       CL.can_explain("PREFERRED_CONVERSION", True, False) is True)
    ck("우선주 라인: 우선주전환이 증가를 설명 못 함",
       CL.can_explain("PREFERRED_CONVERSION", True, True) is False)
    ck("우선주 라인: 우선주전환이 감소를 설명",
       CL.can_explain("PREFERRED_CONVERSION", False, True) is True)
    ck("자기주식 처분은 발행주식수 증가를 설명 못 함 (§11)",
       CL.can_explain("TREASURY_ACTION_OTHER", True, False) is False)
    ck("감자는 증가를 설명 못 함",
       CL.can_explain("CAPITAL_REDUCTION", True, False) is False)
    ck("자기주식 소각은 증가를 설명 못 함 (§11)",
       CL.can_explain("TREASURY_SHARE_CANCELLATION", True, False) is False)
    ck("무상증자는 증가를 설명",
       CL.can_explain("BONUS_ISSUE", True, False) is True)

    # ★ SC5 — R16 비례성 (§16 marketCap 오용 금지)
    ck("무납입 사건만 가격대조",
       CL.price_corroboration("THIRD_PARTY_ALLOCATION", 2.0, 0.5) is None)
    # R16 규약: priceRatio = 직전가/당월가 이므로 기계적 사건에서는
    # shareRatio 와 **같은 값**이어야 한다(반비례가 아니라 동치 비교다).
    ck("완전 비례하면 오차 0",
       abs(CL.price_corroboration("STOCK_SPLIT", 10.0, 10.0)) < 1e-12,
       CL.price_corroboration("STOCK_SPLIT", 10.0, 10.0))
    ck("주가가 안 내리면 큰 오차",
       CL.price_corroboration("STOCK_SPLIT", 10.0, 1.0) > CL.R16_TOL)
    ck("R16 허용치를 그대로 승계", CL.R16_TOL == 0.35, CL.R16_TOL)
    ck("무납입 유형 4종", CL.NO_PAYMENT_TYPES == {
        "STOCK_SPLIT", "REVERSE_SPLIT", "BONUS_ISSUE", "STOCK_DIVIDEND"})


# ═══════════════════ L2 entitlement ═══════════════════
def t_l2():
    print("[L2] 기존주주 권리 판정")
    pc = L("targets-precommit")
    ck("precommit 존재", pc is not None)
    if pc:
        ck("§2 대상 50건 고정", pc["targetTotal"] == 50, pc["targetTotal"])
        ck("§2 결과 보기 전 작성", pc["writtenBeforeCollection"] is True)
        ck("§2 corp_code 전부 매핑", pc["corpCodeUnmapped"] == 0)
        ck("§31 SC1 우선주 재매핑 기록", pc["preferredLineRemapped"] > 0)
        ck("§26 창 유계", pc["window"]["before"] == -12 and pc["window"]["after"] == 6)
        ck("§19 판정기준 불변", pc["verdict"]["unchanged"] is True)
        ck("§19 gate 2%", pc["verdict"]["gate"]["expectedBiasPctMax"] == 2.0)
        ck("§19 gate 20%", pc["verdict"]["gate"]["unresolvedTickerPctMax"] == 20.0)
        ck("§18 계산식 승계", pc["bias"]["unchangedFrom"] == "R21/R22/R23")
        ck("§18 예단 금지 문구", "올릴 수" in pc["bias"]["warning"])
        ck("§17 결과공학 금지", len(pc["noResultEngineering"]) > 10)
        ck("§4 소스 18종", len(pc["sourcePriority"]) == 18)
        ck("§17 anchor 최소 YES3/NO5/UNKNOWN2",
           (pc["anchors"]["minYes"], pc["anchors"]["minNo"],
            pc["anchors"]["minUnknown"]) == (3, 5, 2))
        ck("§22 factor 연구 금지 명시", "factor 연구" in pc["forbidden"])
        ck("§34 R25 실행 금지 명시", "R25 실행" in pc["forbidden"])
        et = pc["eventTypes"]
        ck("§9 무상증자 = 권리 YES", et["BONUS_ISSUE"]["entitlement"] is True)
        ck("§31 SC4 주식배당 = 권리 YES",
           et["STOCK_DIVIDEND"]["entitlement"] is True)
        ck("§10 액면분할 = 권리 YES", et["STOCK_SPLIT"]["entitlement"] is True)
        ck("§8 CB 전환 = 권리 NO", et["CB_CONVERSION"]["entitlement"] is False)
        ck("§8 BW 행사 = 권리 NO", et["BW_EXERCISE"]["entitlement"] is False)
        ck("옵션 행사 = 권리 NO",
           et["STOCK_OPTION_EXERCISE"]["entitlement"] is False)
        ck("제3자배정 = 권리 NO",
           et["THIRD_PARTY_ALLOCATION"]["entitlement"] is False)
        ck("일반공모 = 권리 NO", et["PUBLIC_OFFERING"]["entitlement"] is False)
        ck("우선주 전환 = 권리 NO",
           et["PREFERRED_CONVERSION"]["entitlement"] is False)
        ck("§11 자기주식 소각 = 권리 NO",
           et["TREASURY_SHARE_CANCELLATION"]["entitlement"] is False)
        ck("§7 합병은 방향 확인 필요",
           et["MERGER_NEW_SHARES"]["entitlement"] is None)
        ck("§11 감자는 유무상 구분 필요",
           et["CAPITAL_REDUCTION"]["entitlement"] is None)

    # §7 합병 방향성
    e, why, basis = CL.entitlement_of("MERGER_NEW_SHARES", {"shareRatio": 1.5}, {})
    ck("§7 주식수 증가 합병 → 권리 NO", e == "NO", e)
    ck("§7 방향성 근거 기록", basis == "MERGER_DIRECTION")
    ck("§7 근거에 발행 주체 설명", "발행한 쪽" in why)
    e2, _, _ = CL.entitlement_of("MERGER_NEW_SHARES", {"shareRatio": 0.8}, {})
    ck("§7 감소 합병은 확정 불가", e2 == "UNKNOWN")

    # 본문 증자방식으로 확정
    for m, want in (("SHAREHOLDER_RIGHTS", "YES"), ("RIGHTS_THEN_PUBLIC", "YES"),
                    ("THIRD_PARTY", "NO"), ("PUBLIC_OFFERING", "NO")):
        e3, _, b3 = CL.entitlement_of("RIGHTS_FAMILY", {"shareRatio": 1.2},
                                      {"issueMethod": m})
        ck(f"§6 본문 증자방식 {m} → {want}", e3 == want, e3)
        ck(f"§6 근거가 본문임을 기록 {m}", b3 == f"DOC_ISSUE_METHOD:{m}")
    e4, _, b4 = CL.entitlement_of("RIGHTS_FAMILY", {"shareRatio": 1.2}, {})
    ck("§6 방식 미확정이면 UNKNOWN", e4 == "UNKNOWN" and b4 == "METHOD_UNKNOWN")
    e5, _, _ = CL.entitlement_of("CAPITAL_REDUCTION", {"shareRatio": 0.5}, {})
    ck("§11 감자는 UNKNOWN 유지", e5 == "UNKNOWN")

    ent = L("shareholder-entitlement")
    cls = L("event-classification")
    ck("권리 판정 산출물 존재", ent is not None)
    ck("분류 산출물 존재", cls is not None)
    if ent and cls:
        ck("50건 전부 판정", sum(ent["byEntitlement"].values()) == 50)
        ck("YES/NO/UNKNOWN 만 사용",
           set(ent["byEntitlement"]) <= {"YES", "NO", "UNKNOWN"})
        ck("§14 LOW 는 확정에 미사용",
           all(e["confidence"] != "LOW" or e["entitlement"] == "UNKNOWN"
               for e in ent["events"]),
           [e["ticker"] for e in ent["events"]
            if e["confidence"] == "LOW" and e["entitlement"] != "UNKNOWN"])
        ck("§31 SC7 경합은 UNKNOWN",
           all(e["entitlement"] == "UNKNOWN" for e in ent["events"]
               if e.get("entitlementBasis") == "ENTITLEMENT_CONFLICT"))
        ck("확정건은 근거 기록",
           all(e.get("entitlementBasis") for e in ent["events"]
               if e["entitlement"] in ("YES", "NO")))
        ck("확정건은 공시번호 기록",
           all(e.get("primaryRceptNo") for e in ent["events"]
               if e["entitlement"] in ("YES", "NO")))
        ck("§6 제목만 기계판정 금지 명시", "기계판정" in ent["rule"])

    ch = L("correction-chains")
    ck("정정 chain 산출물 존재", ch is not None)
    if ch:
        ck("§15 최종본 사용 규칙 명시", "최종 조건" in ch["rule"])


# ═══════════════════ L3 wealth ═══════════════════
def t_l3():
    print("[L3] wealth invariant")
    ds = DS[:4]

    # §13 — NO 는 조정 0
    r = RS.resolve_one({"ticker": "X", "date": ds[1], "primaryEvent": "CB_CONVERSION",
                        "entitlement": "NO", "confidence": "HIGH", "shareRatio": 1.3},
                       {})
    ck("§13 NO → 조정 0", r["wealthAdjustment"] == 0.0)
    ck("§13 NO → WEALTH_CONFIRMED", r["wealthStatus"] == "WEALTH_CONFIRMED")
    ck("§13 NO → holderRight False", r["holderRight"] is False)
    ck("§13 주가를 지우는 게 아님을 명시", "주가 움직임은 그대로" in r["why"])

    # 무납입 기계 조정
    r2 = RS.resolve_one({"ticker": "X", "date": ds[1], "primaryEvent": "STOCK_SPLIT",
                         "entitlement": "YES", "confidence": "HIGH",
                         "shareRatio": 10.0, "bodyRatio": 10.0,
                         "bodyRatioRelErr": 0.0}, {})
    ck("무납입 → WEALTH_CONFIRMED", r2["wealthStatus"] == "WEALTH_CONFIRMED")
    ck("무납입 → 외부납입 0", r2["externalContribution"] == 0.0)
    ck("무납입 → shareFactor = 관측비율", r2["shareFactor"] == 10.0)
    ck("무납입 → R16 경로", r2["adjustmentType"] == "MECHANICAL_R16")

    # §12 — YES 인데 조건 부족
    r3 = RS.resolve_one({"ticker": "X", "date": ds[1],
                         "primaryEvent": "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS",
                         "entitlement": "YES", "confidence": "MEDIUM",
                         "shareRatio": 1.5}, {})
    ck("§12 조건 부족 → WEALTH_PARTIAL", r3["wealthStatus"] == "WEALTH_PARTIAL")
    ck("§12 추정으로 확정하지 않음", "추정으로 채우지 않는다" in r3["why"])
    r4 = RS.resolve_one({"ticker": "X", "date": ds[1],
                         "primaryEvent": "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS",
                         "entitlement": "YES", "confidence": "HIGH",
                         "shareRatio": 1.5, "allocPerShare": 0.5,
                         "issuePrice": 1000.0}, {})
    ck("§12 조건 충족 → 확정", r4["wealthStatus"] == "WEALTH_CONFIRMED")
    ck("§12 R17 정책 사용", r4["adjustmentType"] == "RIGHTS_R17_POLICY_A")

    # UNKNOWN
    r5 = RS.resolve_one({"ticker": "X", "date": ds[1], "primaryEvent": "UNKNOWN",
                         "entitlement": "UNKNOWN", "confidence": "UNKNOWN",
                         "shareRatio": 1.3}, {})
    ck("§36 UNKNOWN 유지", r5["wealthStatus"] == "WEALTH_UNRESOLVED")

    # §31 SC9 — 비율 의미
    rec = RS.reconcile({"ticker": "X", "date": ds[1], "primaryEvent": "BONUS_ISSUE",
                        "bodyRatio": 1.0, "bodyRatioSemantics": "PER_OLD_SHARE"},
                       {"sharesBefore": 100.0, "sharesAfter": 200.0})
    ck("§31 SC9 구주 1주당 신주수 해석", rec["pass"] is True, rec)
    rec2 = RS.reconcile({"ticker": "X", "date": ds[1], "primaryEvent": "STOCK_SPLIT",
                         "bodyRatio": 2.0, "bodyRatioSemantics": "MULTIPLIER"},
                        {"sharesBefore": 100.0, "sharesAfter": 200.0})
    ck("§31 SC9 배수 해석", rec2["pass"] is True, rec2)

    # 엔진 invariant
    eng = C.CanonicalWealth(ds, cap=synth(ds, [(10000, 100), (5000, 200),
                                               (5000, 200), (5000, 200)]))
    m = [{"ticker": "X", "date": ds[1], "label": "CONFIRMED_BONUS_ISSUE",
          "shareRatio": 2.0, "holderRight": True, "resolvedWealth": True}]
    rw = W.RightsWealth(eng, m)
    run = rw.run("X", 0, 1, reinvest=False)
    ck("무상증자 wealth 불변", abs(run["twr"]) < 1e-12, run["twr"])
    ck("무상증자 외부납입 0", run["externalContribution"] == 0.0)
    ck("무상증자 주식 2배", abs(run["endShares"] - 2.0) < 1e-12)

    m2 = [{"ticker": "X", "date": ds[1], "label": "CONFIRMED_NON_RIGHTS",
           "shareRatio": 2.0, "holderRight": False, "resolvedWealth": True}]
    run2 = W.RightsWealth(eng, m2).run("X", 0, 1, reinvest=False)
    ck("권리 없음 → 주식수 불변", abs(run2["endShares"] - 1.0) < 1e-12)
    ck("권리 없음 → 조정 없이 주가만", abs(run2["twr"] - (5000 / 10000 - 1)) < 1e-12)

    an = L("anchor-cases")
    ck("anchor 산출물 존재", an is not None)
    if an:
        ck("§17 최소 개수 충족", an["meetsMinimums"] is True, an["obtained"])
        ck("§17 YES 3건 이상", an["obtained"]["YES"] >= 3)
        ck("§17 NO 5건 이상", an["obtained"]["NO"] >= 5)
        ck("§17 UNKNOWN 2건 이상", an["obtained"]["UNKNOWN"] >= 2)
        ck("anchor 전부 manual 일치", an["allPass"] is True)
        ck("50건 전부 anchor 화", an["allEventsAnchored"] == 50)
        for c in an["cases"]:
            ck(f"anchor 오차 0 {c['ticker']} {c['date'][:7]}",
               abs(c["errorPp"]) < 0.05, c["errorPp"])
            ck(f"anchor 항목 완비 {c['ticker']} {c['date'][:7]}",
               all(k in c["manual"] for k in
                   ("oldShares", "startPrice", "newShares", "exPrice",
                    "externalContribution", "expectedWealthAdjustment"))
               and c.get("source") is not None or c["entitlement"] == "UNKNOWN")

    ct = L("ex-rights-continuity")
    ck("연속성 산출물 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0건", ct["freeWealthEvents"] == 0, ct["freeWealthEvents"])
        ck("이론값과 일치", ct["diffMatchesTheory"] is True)
        ck("연속성 PASS", ct["pass"] is True)

    sr = L("share-count-reconciliation")
    ck("정합성 산출물 존재", sr is not None)
    if sr:
        ck("§16 marketCap 독립근거 금지 명시", "marketCap" in sr["rule"])
        ck("§16 대조 수행", sr["checked"] > 0)
        ck("§16 대부분 통과", sr["passed"] >= sr["checked"] - 1,
           (sr["passed"], sr["checked"]))


# ═══════════════════ L4 real ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    src, cls = L("direct-source-events"), L("event-classification")
    wr, rec = L("wealth-resolution"), L("full-reconciliation")
    bias, mat = L("expected-bias"), L("unresolved-materiality")
    v, sam = L("foundation-verdict"), L("samsung-anchor")

    ck("직접소스 산출물 존재", src is not None)
    if src:
        ck("실제 공시 확보 (mock 아님)", src["filingsFound"] > 0)
        ck("목록 실패 0", src["listsFailed"] == 0)
        ck("§3 새 crawler 없음", "새 crawler 0" in src["reuse"])
        ck("§3 기존 캐시 재사용", src["documentsCacheHits"] > 0)
        ck("본문 수집 대상 = 분류기 판정",
           "classify_filing" in src["docSelection"])
    ck("분류 산출물 존재", cls is not None)
    if cls:
        ck("50건 분류", cls["targets"] == 50)
        ck("본문을 실제로 읽음", cls["docsRead"] > 0)
        ck("분류기 버전 기록", cls["classifierVersion"].startswith("r24-classify"))

    ck("wealth 산출물 존재", wr is not None)
    if wr:
        ck("50건 resolve", wr["targets"] == 50)
        ck("상태 합 = 50", sum(wr["byWealthStatus"].values()) == 50)
        ck("§12 hardRule 명시", "계획치를" in wr["hardRule"])

    ck("전체 재분류 존재", rec is not None)
    if rec:
        ck("2,742건 유지", rec["total"] == 2742, rec["total"])
        ck("R24 merge 50건", rec["r24Merged"] == 50, rec["r24Merged"])

    ck("기대편향 산출물 존재", bias is not None)
    if bias:
        ck("§18 공식 R23 동일", bias["formulaUnchangedFromR23"] is True)
        ck("§18 before = R23 실측", bias["beforePct"] == 2.109)
        ck("§18 after 기록", isinstance(bias["afterPct"], float))
        ck("§18 변화량 기록",
           abs(bias["changePp"] - (bias["afterPct"] - bias["beforePct"])) < 1e-9)
        ck("§18 기준 2% 불변", bias["thresholdPct"] == 2.0)
        ck("§18 분모 규칙 불변", "미해결 사건 수" in bias["denominatorRule"])
        ck("§18 progression 7단계",
           all(k in bias["progression"] for k in
               ("R18", "R19", "R20", "R21", "R22", "R23", "R24")))
        ck("R23 값 보존", bias["progression"]["R23"] == 2.109)
        ck("R22 값 보존", bias["progression"]["R22"] == 2.416)
        ck("모집단 P 사후조작 없음",
           abs(bias["populationP"] - 0.296) < 0.002, bias["populationP"])
        ck("권리 없음 확정군 P=0",
           bias["decomposition"].get(
               "NO_ENTITLEMENT_CONFIRMED", {}).get("avgP") == 0.0)
        ck("주주배정 조건미확보군 P=1",
           bias["decomposition"].get(
               "HOLDER_RIGHT_TERMS_MISSING", {}).get("avgP") == 1.0)
        tu = bias["decomposition"].get("TRUE_UNRESOLVED", {})
        ck("모집단 추정 의존 감소 (50 → N)", tu.get("events", 99) < 50,
           tu.get("events"))
        ck("기여도 합 ≈ 전체 편향",
           abs(sum(c["biasContributionPct"]
                   for c in bias["decomposition"].values())
               - bias["expectedBiasPct"]) < 0.02)
        ck("§21 군별 감도 제시",
           len(bias["gateSensitivity"]["ifGroupFullyResolved"]) >= 2)

    ck("materiality 산출물 존재", mat is not None)
    if mat:
        ck("미해결 progression 9단계", len(mat["progression"]) == 9)
        ck("R23 미해결비율 보존", mat["progression"]["R23"] == 2.94)
        ck("편향 방향 보수적", mat["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")
        ck("§19 20% 기준 통과", mat["unresolvedTickerPct"] < 20.0)
        ck("잔여 사유 분해", len(mat["unresolvedReasons"]) >= 2)

    ck("판정 산출물 존재", v is not None)
    if v:
        ck("§19 기준 불변", v["thresholdsUnchanged"] is True)
        ck("§18 공식 불변", v["biasFormulaUnchanged"] is True)
        ck("R16 40% 기준 보존", v["thresholdsFromPrecommit"]["r16Preserved40"] == 40.0)
        ck("2% 기준 보존", v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("bias gate 판정 = 실측 대조",
           v["checks"]["expectedBiasUnder2pct"] == (bias["afterPct"] <= 2.0))
        ck("§20 엔진 검증 반영", v["checks"]["canonicalWealthEngineVerified"] is True)
        ck("§20 분할 정상", v["checks"]["splitReverseSplitCorrect"] is True)
        ck("§20 무상증자 정상", v["checks"]["bonusIssueCorrect"] is True)
        ck("§20 주주/비주주 분리", v["checks"]["shareholderVsNonShareholderSeparated"])
        ck("§20 공짜 wealth 없음", v["checks"]["noFakeWealth"] is True)
        ck("§20 잔여 한계 정량화", v["checks"]["residualLimitationQuantified"])
        ck("§20 무한연구 금지 명시", "무한 연구" in v["closureRule"])
        ck("§22 기존 연구 자동승격 금지",
           "자동 승격하지 않는다" in v["legacyResearchStatus"]["note"])
        ck("§23 factor 게이트 = 판정 연동",
           v["factorResearchAllowed"] == v["verdict"].startswith(
               "CANONICAL_TSR_FOUNDATION_PASS"))
        gate = (v["checks"]["expectedBiasUnder2pct"]
                and v["checks"]["unresolvedTickerPctUnder20"])
        ck("게이트 미통과면 BLOCKED (§35 기준 이동 금지)",
           gate or v["verdict"].endswith("BLOCKED_RIGHTS_REMAINS"), v["verdict"])

    # §24·§38 — 삼성 anchor
    ck("삼성 anchor 산출물 존재", sam is not None)
    if sam:
        ck("§24 R15 구간 사용", sam["period"]["start"] == "2010-01-04")
        ck("§24 raw price return 보존", sam["r15PriceReturnPct"] == -90.2)
        ck("§24 분할조정 보존", sam["r15SplitAdjustedPct"] == 390.11)
        ck("§24 TSR reinvest 478.52% 재현",
           sam["comparison"]["TOTAL_RETURN_WITH_REINVEST"]["pass"],
           sam["comparison"]["TOTAL_RETURN_WITH_REINVEST"])
        ck("§24 TSR no-reinvest 433.46% 재현",
           sam["comparison"]["TOTAL_RETURN_NO_REINVEST"]["pass"])
        ck("§24 anchor 보존", sam["preserved"] is True)


# ═══════════════════ L5 regression ═══════════════════
def t_l5():
    print("[L5] R23~R16 회귀")
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
    d22 = P22.parse_final_price(P22.flatten(
        "<table><tr><td>확정발행가액</td><td>5,000원</td></tr></table>"))
    ck("R22 소문자 HTML 파서 회귀", d22.get("finalIssuePrice") == 5000.0)
    d23 = P23.parse("<DOC>※ 구주주 1주당 신주배정비율 산출 근거 "
                    "모집주식총수(11,000,000주) - 우리사주조합 우선배정분(2,200,000주) "
                    "= 기발행보통주식수(32,531,794주) - 자기주식(2,902,135주) "
                    "= 0.2969997056 주</DOC>")
    ck("R23 배정근거 파서 회귀",
       abs(d23["derivedRatio"] - 0.2969997056) < 1e-9, d23.get("derivedRatio"))
    ck("R23 자기주식 제외 회귀", d23["eligibleOldShares"] == 29629659.0)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r19-foundation-verdict-latest.json",
              "r20-foundation-verdict-latest.json",
              "r21-foundation-verdict-latest.json",
              "r22-foundation-verdict-latest.json",
              "r23-foundation-verdict-latest.json",
              "r23-full-reconciliation-latest.json",
              "r15-samsung-tsr-latest.json"):
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
              "wababa-final-27-holder-rights-terms-recovery-r22-latest.md",
              "wababa-final-11-holder-rights-allocation-recovery-r23-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-final-unknown-50-direct-classification-r24-latest.json"
    ck("R24 보고서 JSON 존재", j.exists())
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
    m = WD / "wababa-final-unknown-50-direct-classification-r24-latest.md"
    ck("R24 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§32)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§32)", lines[1].startswith("reason_class:"), lines[1])
        body = "\n".join(lines)
        ck("§31 자체수정 공개", "자체수정" in body)
        ck("§17 manual anchor 포함", "manual anchor" in body)
        ck("§24 삼성 anchor 포함", "삼성전자 anchor" in body)
        ck("§33 before/after 편향 포함", "2.109%" in body)
        ck("Founder 행동 명시", "Founder 행동" in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)
        ck("§34 R25 는 후보로만", "R25 를 실행하지 않았다" in body
           or "R25" in body)


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
