#!/usr/bin/env python3
"""R19 회귀 — 자본행위 분류 + 권리 판정 invariant. 네트워크 0 · production write 0.

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19

계층:
  L1 classifier  — 공시명 → 유형, 한국어 변형, 종속회사 배제, 설명력 게이트
  L2 entitlement — 직접권리 판정, 합병 방향성, 기계적 조정
  L3 wealth      — 희석에 가짜 wealth 금지, 권리 있으면 정확히 조정
  L4 real data   — 실제 산출물(mock 아님) · anchor · bias · 판정
  L5 regression  — R18/R17/R16 · 삼성 · 역분할 · 무상증자 · 유상증자

사용: python scripts/research/test_r19_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
import r19_classify as K  # noqa: E402
import r19_targets as T  # noqa: E402

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
    p = RD / f"r19-{n}-latest.json"
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
    print("[L1] 공시명 → 자본행위 유형")
    for nm, want in [
        ("전환청구권행사", "CONVERTIBLE_BOND_CONVERSION"),
        ("전환권행사", "CONVERTIBLE_BOND_CONVERSION"),
        ("신주인수권행사", "BW_WARRANT_EXERCISE"),
        ("교환청구권행사", "EXCHANGEABLE_BOND_EXCHANGE"),
        ("주식매수선택권행사", "STOCK_OPTION_EXERCISE"),
        ("회사합병결정", "MERGER_NEW_SHARES"),
        ("[기재정정]회사합병결정", "MERGER_NEW_SHARES"),
        ("소규모합병결정", "MERGER_NEW_SHARES"),
        ("주식교환·이전 결정", "SHARE_SWAP"),
        ("회사분할결정", "SPINOFF_RELATED_SHARES"),
        ("주식분할결정", "STOCK_SPLIT"),
        ("액면분할 주권 변경상장", "STOCK_SPLIT"),
        ("무상증자결정", "BONUS_ISSUE"),
        ("유상증자결정", "TRUE_RIGHTS_ISSUE"),
        ("감자결정", "CAPITAL_REDUCTION"),
        ("자기주식취득결정", "TREASURY_ACTION"),
    ]:
        got = K.classify_filing(nm)[0]
        ck(f"'{nm}' → {want}", got == want, got)

    ck("주식분할이 회사분할보다 우선(오분류 방지)",
       K.classify_filing("주식분할결정")[0] == "STOCK_SPLIT")
    ck("매칭된 규칙의 창을 함께 반환", K.classify_filing("전환청구권행사")[2] == (-3, 3))
    ck("약한 규칙은 자기 창을 가짐",
       K.classify_filing("전환사채권 발행결정")[2] == (-12, 0))
    ck("무관 공시는 None", K.classify_filing("정기주주총회 소집결의")[0] is None)
    ck("종속회사 공시 배제",
       K.classify_filing("유상증자결정(종속회사의주요경영사항)")[0] is None)
    ck("자회사 공시 배제", K.classify_filing("자회사의 합병결정")[0] is None)
    got, weak, _w = K.classify_filing("전환사채권 발행결정")
    ck("사채 발행결정은 약한 근거", weak is True and got ==
       "CONVERTIBLE_BOND_CONVERSION")
    ck("행사 공시는 강한 근거", K.classify_filing("전환청구권행사")[1] is False)

    ck("설명불가 유형 목록", set(T.CANNOT_EXPLAIN_SHARE_INCREASE) ==
       {"TREASURY_ACTION", "CAPITAL_REDUCTION", "CODE_CHANGE"})
    ck("기계적 유형 목록", set(T.MECHANICAL_TYPES) == {"STOCK_SPLIT", "BONUS_ISSUE"})


# ═══════ L2 entitlement ═══════
def t_l2():
    print("[L2] 직접권리 판정 · 설명력 게이트")
    for kind, want in [("CONVERTIBLE_BOND_CONVERSION", "NO"),
                       ("BW_WARRANT_EXERCISE", "NO"),
                       ("EXCHANGEABLE_BOND_EXCHANGE", "NO"),
                       ("STOCK_OPTION_EXERCISE", "NO"),
                       ("THIRD_PARTY_ISSUANCE", "NO"),
                       ("PUBLIC_OFFERING", "NO"),
                       ("MERGER_NEW_SHARES", "NO"),
                       ("SHARE_SWAP", "NO"),
                       ("SPINOFF_RELATED_SHARES", "NO"),
                       ("STOCK_SPLIT", "YES"),
                       ("BONUS_ISSUE", "YES"),
                       ("TRUE_RIGHTS_ISSUE", "YES"),
                       ("CAPITAL_REDUCTION", "UNKNOWN")]:
        got, why = K.entitlement_of(kind)
        ck(f"{kind} 직접권리 {want}", got == want, got)
        ck(f"{kind} 근거 기록", bool(why))

    r = K.classify_event(ev(), [fil("전환청구권행사")])
    ck("CB 전환 → 권리 없음", r["entitlement"] == "NO")
    ck("CB 전환 → 조정 불필요", r["wealthAdjustment"] == "NOT_REQUIRED")
    ck("CB 전환 직접출처", r["provenance"] == "DIRECT_DART")

    r = K.classify_event(ev(), [fil("주식분할결정")])
    ck("주식분할 → 권리 있음", r["entitlement"] == "YES")
    ck("주식분할 → 기계적 조정", r["wealthAdjustment"] == "REQUIRED_MECHANICAL")
    ck("주식분할 mechanical 표시", r["mechanical"] is True)

    # ★ 자체수정 1 검증: 자기주식은 주식수를 늘릴 수 없다 → PRIMARY 금지
    r = K.classify_event(ev(), [fil("자기주식취득결정")])
    ck("자기주식만 있으면 PRIMARY 안됨", r["primaryEvent"] == "UNRESOLVED",
       r["primaryEvent"])
    ck("자기주식은 secondary 로 보존", "TREASURY_ACTION" in r["secondaryEvents"])
    ck("설명 못하면 근거 표시", r["provenance"] == "NO_EXPLANATORY_FILING")
    r = K.classify_event(ev(), [fil("자기주식취득결정"), fil("전환청구권행사")])
    ck("설명 가능한 공시가 PRIMARY 를 가져감",
       r["primaryEvent"] == "CONVERTIBLE_BOND_CONVERSION", r["primaryEvent"])
    ck("자기주식은 secondary 에 남음", "TREASURY_ACTION" in r["secondaryEvents"])

    r = K.classify_event(ev(), [fil("감자결정")])
    ck("감자만 있어도 PRIMARY 안됨", r["primaryEvent"] == "UNRESOLVED")

    r = K.classify_event(ev(), [])
    ck("공시 없으면 UNRESOLVED", r["primaryEvent"] == "UNRESOLVED")
    ck("공시 없으면 권리 UNKNOWN", r["entitlement"] == "UNKNOWN")
    ck("공시 없으면 조정 UNKNOWN", r["wealthAdjustment"] == "UNKNOWN")

    # 창 밖은 매칭 금지
    r = K.classify_event(ev(), [fil("전환청구권행사", "20180101")])
    ck("창 밖 공시는 매칭 안됨", r["primaryEvent"] == "UNRESOLVED")

    # 약한 근거만 있으면 LOW
    r = K.classify_event(ev(), [fil("전환사채권 발행결정", "20200101")])
    ck("사채 발행결정만이면 LOW", r["confidence"] == "LOW", r["confidence"])
    ck("LOW 는 DERIVED_MATCH", r["provenance"] == "DERIVED_MATCH")

    # 수량 일치 시 HIGH
    r = K.classify_event(ev(sr=1.5, before=1_000_000),
                         [fil("신주인수권행사(500,000주)")])
    ck("수량 일치하면 HIGH", r["confidence"] == "HIGH", r["confidence"])
    r = K.classify_event(ev(sr=1.5, before=1_000_000),
                         [fil("신주인수권행사(9,000,000주)")])
    ck("수량 크게 다르면 HIGH 금지", r["confidence"] != "HIGH", r["confidence"])


# ═══════ L3 wealth ═══════
def t_l3():
    print("[L3] wealth — 희석에 가짜 wealth 금지")
    cap = synth(DS, [(10000, 100)] + [(5000, 200)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)

    m = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0,
          "label": "CONFIRMED_NON_RIGHTS", "holderRight": False}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("권리 없으면 보유주식 불변", abs(r["endShares"] - 1.0) < 1e-9)
    ck("권리 없으면 납입 0", r["externalContribution"] == 0.0)
    ck("희석은 주가에 반영", abs(r["twr"] - (5000 / 10000 - 1)) < 1e-9)
    ck("공짜 wealth 없음", r["endShares"] <= 1.0 + 1e-9)

    m2 = [{"ticker": "X", "date": DS[1], "shareRatio": 2.0,
           "label": "CONFIRMED_BONUS_ISSUE", "holderRight": False}]
    r2 = W.RightsWealth(eng, m2).run("X", 0, 1, reinvest=False)
    ck("기계적 권리는 보유주식 2배", abs(r2["endShares"] - 2.0) < 1e-9)
    ck("기계적 조정 wealth 중립", abs(r2["twr"]) < 1e-9, r2["twr"])
    ck("기계적 조정도 납입 0", r2["externalContribution"] == 0.0)

    ck("권리 유무가 결과를 실제로 가른다", abs(r["twr"] - r2["twr"]) > 0.4)


# ═══════ L4 real data ═══════
def t_l4():
    print("[L4] 실제 산출물 — mock 이 아니다")
    tg = L("targets-precommit")
    ck("대상 precommit 존재", tg is not None)
    if tg:
        ck("결과 이전 작성 표기", tg["writtenBeforeDirectSourceResults"] is True)
        ck("대상 342건", tg["targetTotal"] == 342, tg["targetTotal"])
        ck("threshold 변경 없음", tg["verdict"]["unchanged"] is True)
        ck("bias 공식 유지 선언", "새 bias 계산법 도입" in tg["bias"]["notAllowed"])
        ck("자체수정 기록", len(tg.get("selfCorrections", [])) >= 2)
        ck("자체수정 방향 공개",
           all("방향" in sc.get("direction", "") or sc.get("direction")
               for sc in tg["selfCorrections"]))
        ck("범위 확대 금지", "확장 금지" in tg["scopeGuard"])

    ds = L("direct-source-events")
    ck("직접소스 수집 존재", ds is not None)
    if ds:
        ck("전 공시유형 사용 이유 기록", "거래소" in ds["whyAllTypes"])
        ck("공시 수집됨", ds["filingsKept"] > 0)

    cl = L("event-classification")
    ck("분류 산출물 존재", cl is not None)
    if cl:
        ck("342건 전수 분류", cl["targets"] == 342)
        ck("세 축 분리 기록", "다른 축" in cl["layerNote"])
        ck("유형 분포 존재", bool(cl["byPrimaryEvent"]))
        ck("자기주식이 PRIMARY 아님",
           cl["byPrimaryEvent"].get("TREASURY_ACTION", 0) == 0,
           cl["byPrimaryEvent"].get("TREASURY_ACTION"))
        ck("주식분할이 식별됨", cl["byPrimaryEvent"].get("STOCK_SPLIT", 0) > 0)

    en = L("shareholder-entitlement")
    ck("권리 판정 산출물 존재", en is not None)
    if en:
        ck("YES/NO/UNKNOWN 3분류", set(en["counts"]) == {"YES", "NO", "UNKNOWN"})
        ck("희석과 권리 구분 명시", "다른 개념" in en["coreDistinction"])

    md = L("merger-direction")
    ck("합병 방향성 검증 존재", md is not None)
    if md:
        ck("생존율 측정", md["survivalPct"] is not None)
        ck("반대 방향 설명", "상장폐지" in md["counterDirectionNote"])
        ck("방향성 주장이 데이터로 뒷받침", md["survivalPct"] >= 90,
           md["survivalPct"])

    fp = L("false-classification-audit")
    ck("오분류 감사 존재", fp is not None)
    if fp:
        ck("원문 공시명 대조", "원문 공시명" in fp["method"])
        ck("오분류율 산출", fp["falsePositiveRate"] is not None)
        ck("하네스 결함 자체수정 기록", "자체수정" in (fp.get("harnessNote") or ""))

    an = L("anchor-cases")
    ck("anchor 존재", an is not None)
    if an:
        ck("anchor 6건 이상 (§19)", len(an["cases"]) >= 6, len(an["cases"]))
        ck("anchor 전부 manual 일치", an["allPass"])
        ck("권리 없으면 조정 정확히 0",
           an["noEntitlementAdjustmentExactlyZero"] is True)
        ck("권리 있으면 비영 조정", an["entitlementCaseAdjustsNonZero"] is True)
        ck("양쪽 필요 이유 기록", "구분되지 않는다" in an["whyBothSidesNeeded"])
        kinds = {c["primaryEvent"] for c in an["cases"]}
        ck("CB anchor 있음", "CONVERTIBLE_BOND_CONVERSION" in kinds)
        ck("BW anchor 있음", "BW_WARRANT_EXERCISE" in kinds)
        ck("합병 anchor 있음", "MERGER_NEW_SHARES" in kinds)

    b = L("expected-bias")
    ck("기대편향 산출물 존재", b is not None)
    if b:
        ck("공식 R18 유지", b["formulaUnchangedFromR18"] is True)
        ck("R18 과소평가 지적", "과소평가" in b["progressionCaveat"])
        ck("지표 둔감성 지적", "둔감" in b["metricInsensitivityFinding"])
        ck("like-for-like 병기", b["likeForLike"]["r18Convention"] is not None)
        ck("like-for-like 는 판정에 미사용", "판정에는 쓰지 않는다" in
           b["likeForLike"]["note"])
        ck("기여도 분해 존재", len(b["decomposition"]) >= 3)
        ck("P 근거를 사건별로 표기",
           all("pSource" in c for c in b["decomposition"].values()))
        ck("확정 권리없음은 P=0",
           all(c["avgP"] == 0.0 for k, c in b["decomposition"].items()
               if c["pSource"].startswith("DIRECT") and k != "TRUE_RIGHTS"
               and "HOLDER_RIGHT" not in k))
        xc = b.get("directMeasurementCrossCheck") or {}
        ck("최대군 직접 교차검증", xc.get("n", 0) > 0)

    mt = L("unresolved-materiality")
    ck("materiality 존재", mt is not None)
    if mt:
        ck("R16~R19 progression",
           set(mt["progression"]) == {"R16", "R17", "R18", "R19"})
        ck("R18 값 보존", mt["progression"]["R18"] == 13.31)
        ck("규모 비중 산출 (§23)", mt["unresolvedMagnitudePct"] is not None)
        ck("월 비율 산출", mt["unresolvedMonthPct"] is not None)
        ck("편향 방향 = 과소평가",
           mt["biasDirection"] == "CONSERVATIVE_UNDERSTATEMENT")

    v = L("foundation-verdict")
    ck("판정 존재", v is not None)
    if v:
        ck("판정값 유효", v["verdict"] in (
            "CANONICAL_TSR_FOUNDATION_PASS",
            "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "CANONICAL_TSR_FOUNDATION_FAIL"), v["verdict"])
        ck("threshold 변경 없음 (§17)", v["thresholdsUnchanged"] is True)
        ck("bias 공식 변경 없음 (§15)", v["biasFormulaUnchanged"] is True)
        ck("2% 기준 그대로",
           v["thresholdsFromPrecommit"]["maxExpectedBiasPct"] == 2.0)
        ck("20% 기준 그대로",
           v["thresholdsFromPrecommit"]["passWithLimitsMaxTickerPct"] == 20.0)
        ck("legacy 유지 (§25)",
           "PRE_TSR_LEGACY_RESEARCH" in v["legacyResearchStatus"]["R5~R14"])
        for k in ("R11_BM", "R14_QUALITY", "SIZE"):
            ck(f"{k} PROVISIONAL 유지",
               "PROVISIONAL" in v["legacyResearchStatus"][k])
        ck("자동 복원 금지 명시", "자동 복원" in v["legacyResearchStatus"]["note"])
        ck("factor 허용은 판정과 일치",
           v["factorResearchAllowed"] == (v["verdict"] in (
               "CANONICAL_TSR_FOUNDATION_PASS",
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")))

    rec = L("full-reconciliation")
    ck("전체 재조정 존재", rec is not None)
    if rec:
        ck("2,742건 전수", rec["total"] == 2742, rec["total"])
        ck("R19 merge 건수 기록", rec["r19Merged"] == 342, rec["r19Merged"])


# ═══════ L5 regression ═══════
def t_l5():
    print("[L5] R18/R17/R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]
    for rows, nm in (
        ([(100000, 100), (2000, 5000)] + [(2000, 5000)] * 4, "50:1 분할"),
        ([(1000, 10000), (100000, 100)] + [(100000, 100)] * 4, "역분할"),
        ([(10000, 100), (5000, 200)] + [(5000, 200)] * 4, "무상증자"),
    ):
        e = C.CanonicalWealth(ds, cap=synth(ds, rows))
        r = e.get_total_return("X", ds[0], ds[2])
        ck(f"{nm} wealth 불변", abs(r["cumulativeReturn"]) < 1e-9)

    # 공정 유상증자 wealth 중립
    cap = synth(DS, [(10000, 100)] + [(8666.666666666666, 150)] * 7)
    eng = C.CanonicalWealth(DS, cap=cap)
    m = [{"ticker": "X", "date": DS[1], "shareRatio": 1.5, "label": "CONFIRMED_RIGHTS",
          "holderRight": True, "dartRatio": 0.5, "issuePriceDerived": 6000.0}]
    r = W.RightsWealth(eng, m).run("X", 0, 1, reinvest=False)
    ck("공정 유상증자 wealth 중립", abs(r["twr"]) < 1e-9)
    ck("외부납입 분리", abs(r["externalContribution"] - 3000.0) < 1e-6)

    for f in ("r16-foundation-verdict-latest.json",
              "r17-foundation-verdict-latest.json",
              "r18-foundation-verdict-latest.json",
              "r18-full-reconciliation-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
    for f in ("wababa-factor-signal-discovery-r7-latest.md",
              "wababa-bm-incremental-alpha-validation-r11-latest.md",
              "wababa-quality-compounder-long-horizon-r14-latest.md",
              "wababa-tsr-corporate-action-forensic-r15-latest.md",
              "wababa-canonical-tsr-research-foundation-reset-r16-latest.md",
              "wababa-dart-rights-issue-canonical-recovery-r17-latest.md",
              "wababa-dart-legacy-rights-document-recovery-r18-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════ production ═══════
def t_prod():
    print("[prod] production 보호 (§28)")
    rep = WD / "wababa-no-direct-match-capital-action-recovery-r19-latest.json"
    ck("R19 보고서 JSON 존재", rep.exists())
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
    m = WD / "wababa-no-direct-match-capital-action-recovery-r19-latest.md"
    ck("R19 보고서 MD 존재", m.exists())
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
