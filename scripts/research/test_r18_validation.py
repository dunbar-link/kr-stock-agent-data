#!/usr/bin/env python3
"""R18 회귀 — legacy 원문 파서 + wealth invariant. 네트워크 0 · production write 0.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

계층:
  L1 parser        — 한국어 증자방식 변형 · 표 라벨 추출 · 숫자/날짜 파싱
  L2 consistency   — 숫자 정합성 · confidence 배정 · 오분류 금지
  L3 wealth        — 권리 배정 · 외부납입 분리 · 공짜 wealth 금지 · 희석
  L4 real data     — 실제 산출물(mock 아님) · anchor · materiality · 판정
  L5 regression    — R17/R16/삼성/역분할/무상증자가 깨지지 않았는지

사용: python scripts/research/test_r18_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r18_parser as P  # noqa: E402
import r18_reconcile as R  # noqa: E402

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
    p = RD / f"r18-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def cell(code, val, tag="TE"):
    return f'<{tag} ACODE="{code}">{val}</{tag}>'


def doc(**kw):
    """합성 DART 원문 — 실제 구조(ACODE 표 셀)를 그대로 흉내낸다."""
    parts = ["<DOCUMENT><TABLE>"]
    for k, v in kw.items():
        parts.append(cell(k, v, "TU" if k == "CI_MTH" else "TE"))
    parts.append("</TABLE></DOCUMENT>")
    return "".join(parts)


DS = [f"20{20 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(8)]


def synth(dates, rows):
    return {d: {"X": {"close": c, "shares": s, "dps": None}}
            for d, (c, s) in zip(dates, rows)}


# ═══════ L1 parser ═══════
def t_l1():
    print("[L1] 원문 파서 · 한국어 증자방식 변형")
    for raw, want in [
        ("주주배정증자", "SHAREHOLDER_RIGHTS"),
        ("주주배정", "SHAREHOLDER_RIGHTS"),
        ("구주주배정", "SHAREHOLDER_RIGHTS"),
        ("주주배정후 실권주 일반공모", "RIGHTS_THEN_PUBLIC"),
        ("주주배정후실권주일반공모", "RIGHTS_THEN_PUBLIC"),
        ("주주배정후 일반공모", "RIGHTS_THEN_PUBLIC"),
        ("제3자배정증자", "THIRD_PARTY"),
        ("제3자배정", "THIRD_PARTY"),
        ("제삼자배정", "THIRD_PARTY"),
        ("일반공모증자", "PUBLIC_OFFERING"),
        ("주주우선공모", "PUBLIC_OFFERING"),
    ]:
        ck(f"방식 '{raw}' → {want}", P.classify_method(raw) == want,
           P.classify_method(raw))
    ck("실권주가 주주배정보다 우선(오분류 방지)",
       P.classify_method("주주배정후 실권주 일반공모") == "RIGHTS_THEN_PUBLIC")
    ck("빈값은 추측하지 않음", P.classify_method("") is None)
    ck("None 도 추측 없음", P.classify_method(None) is None)
    ck("모르는 문구는 None", P.classify_method("신주인수권부사채발행") is None)

    ck("숫자 콤마 파싱", P._num("10,000,000") == 10000000.0)
    ck("'-' 는 결측", P._num("-") is None)
    ck("소수 배정비율", P._num("0.3737135") == 0.3737135)
    ck("단위 접미 제거", P._num("5,000 원") == 5000.0)
    ck("문자 섞이면 결측", P._num("미정") is None)
    ck("한글 날짜 파싱", P._date("2006년 12월 15일") == "20061215")
    ck("한자리 월일 zero-pad", P._date("2007년 1월 5일") == "20070105")
    ck("숫자 날짜 파싱", P._date("20061215") == "20061215")
    ck("빈 날짜", P._date("-") is None)

    d = doc(CST_CNT="10,000,000", PST_CNT="-", FVAL="5,000",
            BFR_CST_CNT="20,485,173", BFR_PST_CNT="921,600",
            FND_USE2="51,400,000,000", CI_MTH="주주배정증자",
            CST_ISS_VAL="-", ALL_BS_DT="2006년 12월 15일",
            NEW_ASN_CNT="0.4881", DRC_DT="2006년 11월 27일",
            PYM_DT="2007년 01월 12일", LST_PLN_DT="2007년 01월 24일")
    ev = P.parse(d, 0.4881)
    ck("표 필드코드 경로 사용", ev["evidencePath"] == "FIELD_CODE")
    ck("증자방식 추출", ev["issueMethod"] == "SHAREHOLDER_RIGHTS")
    ck("신주수 추출", ev["newSharesCommon"] == 10000000.0)
    ck("증자전 주식수 추출", ev["sharesBeforeCommon"] == 20485173.0)
    ck("액면가 추출", ev["parValue"] == 5000.0)
    ck("배정비율 계산", abs(ev["rightsRatio"] - 10000000 / 20485173) < 1e-12)
    ck("1주당 배정주식수 추출", ev["allocPerShare"] == 0.4881)
    ck("기준일 추출", ev["recordDate"] == "20061215")
    ck("납입일 추출", ev["paymentDate"] == "20070112")
    ck("상장예정일 추출", ev["listingDate"] == "20070124")
    ck("이사회결의일 추출", ev["boardDate"] == "20061127")
    ck("발행가 '-' 는 파생으로 보완",
       ev["issuePriceProvenance"] == "DERIVED_FROM_PROCEEDS_DIV_SHARES")
    ck("조달금액 합산", ev["totalProceeds"] == 51400000000.0)
    ck("주주배정 전용 필드 감지", ev["hasShareholderOnlyFields"] is True)
    ck("직접출처 표기", ev["provenance"] == "DIRECT_DART_DOCUMENT")

    d2 = doc(CI_MTH="제3자배정증자", CST_CNT="1,000,000",
             BFR_CST_CNT="10,000,000", CST_ISS_VAL="2,000",
             PART="홍길동", RLT="최대주주")
    e2 = P.parse(d2, 0.10)
    ck("제3자배정 추출", e2["issueMethod"] == "THIRD_PARTY")
    ck("제3자 전용 필드 감지", e2["hasThirdPartyOnlyFields"] is True)
    ck("제3자에 주주배정 필드 없음", e2["hasShareholderOnlyFields"] is False)
    ck("발행가 직접필드", e2["issuePriceProvenance"] == "DIRECT_FIELD")
    ck("배정대상자 보존", e2["thirdPartyTarget"] == "홍길동")

    d3 = "<DOCUMENT><P>본 건은 제3자배정 방식으로 진행합니다</P></DOCUMENT>"
    e3 = P.parse(d3)
    ck("필드 없으면 본문 keyword 경로", e3["evidencePath"] == "BODY_KEYWORD")
    ck("본문 추론은 LOW 고정", e3["confidence"] == "LOW")

    e4 = P.parse("<DOCUMENT><P>정기주주총회 결과입니다</P></DOCUMENT>")
    ck("증자 근거 없으면 UNRESOLVED", e4["confidence"] == "UNRESOLVED")
    ck("증자 근거 없으면 방식 None", e4["issueMethod"] is None)


# ═══════ L2 consistency · confidence ═══════
def t_l2():
    print("[L2] 숫자 정합성 · confidence")
    base = dict(CI_MTH="주주배정증자", CST_CNT="5,000,000",
                BFR_CST_CNT="10,000,000", CST_ISS_VAL="1,000",
                NEW_ASN_CNT="0.5", ALL_BS_DT="2010년 01월 01일")
    ev = P.parse(doc(**base), 0.5)
    ck("정합성 통과 → HIGH", ev["confidence"] == "HIGH", ev["confidence"])
    ck("정합성 flag 없음", "DATA_CONFLICT" not in ev["flags"])
    ck("allocVsRatio 계산", "allocVsRatio" in ev["consistency"])
    ck("observedVsRatio 계산", "observedVsRatio" in ev["consistency"])

    ev = P.parse(doc(**base), 2.0)
    ck("관측과 크게 다르면 DATA_CONFLICT", "DATA_CONFLICT" in ev["flags"])
    ck("불일치는 LOW 아니라 MEDIUM(§8 의미)", ev["confidence"] == "MEDIUM",
       ev["confidence"])
    ck("불일치여도 방식은 유지", ev["issueMethod"] == "SHAREHOLDER_RIGHTS")

    ev = P.parse(doc(**dict(base, NEW_ASN_CNT="0.9")), 0.5)
    ck("공시 내부 불일치도 DATA_CONFLICT", "DATA_CONFLICT" in ev["flags"])

    b2 = dict(CI_MTH="주주배정증자", CST_CNT="5,000,000")
    ev = P.parse(doc(**b2))
    ck("숫자 부족은 MEDIUM", ev["confidence"] == "MEDIUM", ev["confidence"])
    ck("비율 없으면 None(추측 금지)", ev["rightsRatio"] is None)

    ev = P.parse(doc(CI_MTH="제3자배정증자", CST_CNT="1,000",
                     BFR_CST_CNT="10,000"), None)
    ck("제3자는 비율 없이도 HIGH 가능", ev["confidence"] == "HIGH", ev["confidence"])


# ═══════ L3 wealth ═══════
def t_l3():
    print("[L3] wealth — 권리·외부납입·희석")
    cap = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)
    m = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": "CONFIRMED_RIGHTS",
          "holderRight": True, "dartRatio": 0.5, "issuePriceDerived": 6000.0}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("공정 유상증자 wealth 중립", abs(r["twr"]) < 1e-9, r["twr"])
    ck("외부납입 분리 기록", abs(r["externalContribution"] - 3000.0) < 1e-6)
    ck("납입금이 수익이 아님", abs(r["twr"]) < 1e-9 and r["externalContribution"] > 0)
    ck("보유주식 1.5배", abs(r["endShares"] - 1.5) < 1e-9)

    cap2 = synth(DS, [(10000, 100)] + [(5000, 150)] * 7)
    eng2 = C.CanonicalWealth(DS, cap=cap2)
    m2 = [dict(m[0], issuePriceDerived=9000.0)]
    r2 = W.RightsWealth(eng2, m2).run("X", 0, 1, reinvest=False)
    ck("K >= 권리락가 → 실권", abs(r2["endShares"] - 1.0) < 1e-9)
    ck("실권이면 납입 0", r2["externalContribution"] == 0.0)

    for lbl in ("CONFIRMED_THIRD_PARTY_ISSUE", "CONFIRMED_PUBLIC_OFFERING"):
        m3 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": lbl,
               "holderRight": False}]
        r3 = W.RightsWealth(eng2, m3).run("X", 0, 1, reinvest=False)
        ck(f"{lbl} 보유주식 불변", abs(r3["endShares"] - 1.0) < 1e-9)
        ck(f"{lbl} 공짜 신주 없음", r3["externalContribution"] == 0.0)
        ck(f"{lbl} 희석은 주가에 반영", abs(r3["twr"] - (5000 / 10000 - 1)) < 1e-9)

    m4 = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": "CONFIRMED_RIGHTS",
           "holderRight": False}]
    r4 = W.RightsWealth(eng2, m4).run("X", 0, 1, reinvest=False)
    ck("WEALTH_PARTIAL 은 보수적 미조정", abs(r4["endShares"] - 1.0) < 1e-9)
    ck("보수적 미조정은 wealth 를 부풀리지 않음", r4["twr"] < 0)

    u = W.understatement({"ticker": "X", "date": DS[1], "dartRatio": 0.5,
                          "issuePriceDerived": 3000.0}, eng2)
    ck("과소평가 = r·(P_ex-K)/P_cum", abs(u - 0.5 * 2000 / 10000) < 1e-9)
    ck("무가치 권리는 0",
       W.understatement({"ticker": "X", "date": DS[1], "dartRatio": 0.5,
                         "issuePriceDerived": 9000.0}, eng2) == 0.0)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 산출물 — mock 이 아니다")
    tg = L("targets-precommit")
    ck("대상 precommit 존재", tg is not None)
    if tg:
        ck("결과 이전 작성 표기", tg["writtenBeforeDocumentResults"] is True)
        ck("대상 1,081건", tg["targetTotal"] == 1081, tg["targetTotal"])
        ck("2007~2014 903건", tg["targetLegacy2007to2014"] == 903)
        ck("threshold 20% 승계", tg["verdict"]["rule"][
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
            "unresolvedTickerPctMax"] == 20.0)
        ck("threshold 변경 없음 선언", tg["verdict"]["unchanged"] is True)
        ck("범위 확대 금지 기록", "넓히지 않는다" in tg["scopeGuard"])

    dl = L("document-download-status")
    ck("다운로드 상태 존재", dl is not None)
    if dl:
        ck("원문 확보 > 0", dl["retrieved"] > 0)
        ck("provenance 보존 선언", "변형하지 않는다" in dl["provenanceNote"])
        ck("실패 사유 기록", bool(dl["statusMix"]))
        ck("문서마다 sha256", all(
            d.get("sha256") for d in dl["documents"] if d["status"] == "OK"))

    pr = L("document-parser-results")
    ck("파서 결과 존재", pr is not None)
    if pr:
        ck("문서 파싱됨", pr["documentsParsed"] > 0)
        ck("구조 경로 우세(§7)", pr["structuralPathPct"] >= 90,
           pr["structuralPathPct"])
        ck("방식 분포 존재", bool(pr["byMethod"]))
        ck("필드 복원률 기록", bool(pr["fieldRecovery"]))

    ch = L("correction-chains")
    ck("정정 chain 존재", ch is not None)
    if ch:
        ck("최신본을 final 로 사용 선언", "최신본" in ch["policy"])
        ck("PIT 소급사용 금지 명시", "소급" in ch["policy"])

    rec = L("full-reconciliation")
    ck("전체 재조정 존재", rec is not None)
    if rec:
        ck("2,742건 전수", rec["total"] == 2742, rec["total"])
        ck("유형/wealth 구분 명시", "다르다" in rec["layerVsTypeNote"])
        ck("wealth 층위 집계", bool(rec["byWealthLayer"]))

    fp = L("false-positive-audit")
    ck("오분류 감사 존재", fp is not None)
    if fp:
        ck("독립 구조 증거로 교차검증", "독립" in fp["method"])
        ck("방식별 표본 존재", len(fp["byMethod"]) >= 3)
        ck("오분류율 산출", fp["falsePositiveRate"] is not None)
        ck("false negative 단서 기록", bool(fp["falseNegativeClue"]))

    an = L("anchor-cases")
    ck("anchor 존재", an is not None)
    if an:
        ck("anchor 5건 이상 (§16)", an["atLeastFive"], len(an["cases"]))
        ck("anchor 전부 manual 일치", an["allPass"])
        ck("정정 chain case 존재", an["longestCorrectionChain"] is not None)
        ck("anchor 에 권리가치 기록",
           all("rightsValue" in c["manual"] for c in an["cases"]))

    ct = L("ex-rights-continuity")
    ck("연속성 존재", ct is not None)
    if ct:
        ck("공짜 wealth 0 (§15)", ct["noFreeWealth"] is True)
        ck("차분 = 이론 권리가치", ct["diffMatchesTheory"] is True)
        ck("당월 차분 음수 0", ct["eventMonthDiffNeverNegative"] is True)
        ck("R17 방법 재사용", "R17" in ct["method"])

    ma = L("unresolved-materiality")
    ck("materiality 존재", ma is not None)
    if ma:
        ck("R16→R17→R18 progression", set(ma["progression"]) == {"R16", "R17", "R18"})
        ck("R16 값 보존", ma["progression"]["R16"] == 40.8)
        ck("R17 값 보존", ma["progression"]["R17"] == 25.57)
        ck("월 비율 산출 (§19)", ma["unresolvedMonthPct"] is not None)
        ck("편향 방향 = 과소평가",
           ma["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")
        ck("새 threshold 발명 금지 명시",
           "발명하지 않는다" in ma["understatementSample"]["methodologyNote"])
        ck("P(주주배정) 원문 직접관측",
           "직접관측" in ma["understatementSample"]["pHolderRightSource"])

    v = L("foundation-verdict")
    ck("판정 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 사후 변경 없음 (§20)", v["thresholdsUnchanged"] is True)
        ck("20% 기준 그대로",
           v["thresholdsFromPrecommit"]["passWithLimitsMaxTickerPct"] == 20.0)
        ck("40% 기준 그대로", v["thresholdsFromPrecommit"]["r16Preserved40"] == 40.0)
        ck("legacy 유지 (§23)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        ck("R11 PROVISIONAL 유지",
           "PROVISIONAL" in v["legacyResearchStatus"]["R11_BM"])
        ck("R14 PROVISIONAL 유지",
           "PROVISIONAL" in v["legacyResearchStatus"]["R14_QUALITY"])
        ck("SIZE PROVISIONAL 유지",
           "PROVISIONAL" in v["legacyResearchStatus"]["SIZE"])
        ck("자동 부활 금지 명시", "자동 부활" in v["legacyResearchStatus"]["note"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))


# ═══════ L5 regression ═══════
def t_l5():
    print("[L5] R17/R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]
    cap = synth(ds, [(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    ck("50:1 분할 wealth 불변",
       abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    cap = synth(ds, [(1000, 10000), (100000, 100)] + [(100000, 100)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    ck("역분할 가짜수익 없음",
       abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    cap = synth(ds, [(10000, 100), (5000, 200)] + [(5000, 200)] * 4)
    e = C.CanonicalWealth(ds, cap=cap)
    ck("무상증자 가짜손실 없음",
       abs(e.get_total_return("X", ds[0], ds[2])["cumulativeReturn"]) < 1e-9)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r17-rights-matching-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-robust-factor-portfolio-r8-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-dart-rights-issue-canonical-recovery-r17-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())

    # reconcile 결정론
    ck("reconcile 창 상수 R17 과 동일", R.WINDOW == (-6, 1))


# ═══════ production ═══════
def t_prod():
    print("[prod] production 보호 (§30)")
    rep = WD / "wababa-dart-legacy-rights-document-recovery-r18-latest.json"
    ck("R18 보고서 JSON 존재", rep.exists())
    if rep.exists():
        d = json.loads(rep.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure"):
            ck(f"production {k} == 0", p[k] == 0, p.get(k))
        ck("LEGACY_50D 무변경", p["legacy50d"] == "untouched")
        ck("NEW_BM forward 무변경", p["newBmForward"] == "untouched")
        ck("kr-stock-agent 무변경", p["publicRepo"] == "untouched")
        ck("scheduler 무변경", p["scheduler"] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED", p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 scheduler 0", d["hotg"]["newScheduler"] == 0)
    md = WD / "wababa-dart-legacy-rights-document-recovery-r18-latest.md"
    ck("R18 보고서 MD 존재", md.exists())
    if md.exists():
        head = md.read_text(encoding="utf-8").splitlines()[0]
        ck("첫 줄 전체 판정 (§34)", head.startswith("전체 판정:"), head)


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
