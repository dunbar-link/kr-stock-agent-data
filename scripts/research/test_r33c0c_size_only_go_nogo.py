#!/usr/bin/env python3
"""R33C0C 회귀 — SIZE 계약 복원 · universe 독립성 · 결정 enum · 경계.

WABABA-SIZE-ONLY-PROSPECTIVE-OOS-GO-NOGO-R33C0C

계층:
  L1 frozen  — 상위 동결 hash · source commit · remote 동기
  L2 size    — SIZE 정의·score·시총 source·universe 3종 분리
  L3 dep     — BM/PBR 의존성 실측 (개수만)
  L4 pre     — precommit hash 결정성 · 측정 이전 동결
  L5 src     — 공식 prospective source · calendar · PIT/DART 불요
  L6 ops     — 07:40 scheduler · 마감 여유 · 신규자원 0
  L7 time    — OOS timeline · review time gate
  L8 dec     — 결정 enum · gate fixture(결과독립) · 중간값 거부
  L9 perf    — 성과 접근 0 (PROHIBITED_AND_NOT_CALLED)
  L10 bound  — engine/ledger 미생성 · public/automation 무변경 · 보고서

사용: python scripts/research/test_r33c0c_size_only_go_nogo.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r33c0c_size_only_go_nogo as D      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")
AUTOMATION_REPO = Path(r"C:\work\ai-operating-system")

PASS = FAIL = NOTRUN = PROHIB = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" - {extra}" if extra else ""))


def notrun(name, why):
    global NOTRUN
    NOTRUN += 1
    print(f"  NOT_RUN  {name} - {why}")


def prohibited(name, why):
    global PROHIB
    PROHIB += 1
    print(f"  PROHIBITED_AND_NOT_CALLED  {name} - {why}")


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def jload(p):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


PRE = RD / "r33c0c-decision-precommit-latest.json"
INV = RD / "r33c0c-size-contract-inventory-latest.json"
DEP = RD / "r33c0c-universe-dependency-latest.json"
INC = RD / "r33c0c-universe-dependency-incidence-latest.json"
SO = RD / "r33c0c-source-operability-latest.json"
TL = RD / "r33c0c-oos-timeline-roi-latest.json"
AR = RD / "r33c0c-architecture-audit-latest.json"
DEC = RD / "r33c0c-decision-latest.json"


def t_l1():
    print("\n[L1] frozen 정본 · remote")
    import r33a_translation_precommit as A
    import r33b_precommit as P
    import r33c0_research_data_producer as C0
    ck("r27/r31/r32 hash", P.upstream_hashes()["allMatch"])
    st = jload(RD / "r31-stitch-latest.json")
    ck("R31 manifest 3439dec9", st["manifestSha256"].startswith("3439dec9"))
    ck("R33A d869f445", A.contract_hash().startswith("d869f445"))
    ck("R33A == R33B source", A.contract_hash() == P.SOURCE_CONTRACT_HASH)
    ck("R33B spec fea25545", P.spec_hash().startswith("fea25545"))
    ck("R33C0 a9f47e1a", C0.contract_hash().startswith("a9f47e1a"))
    ck("source commit 836b03d ancestor", subprocess.run(
        ["git", "merge-base", "--is-ancestor", "836b03d", "origin/main"],
        cwd=ROOT, capture_output=True).returncode == 0)
    ck("local HEAD == origin/main",
       git(["rev-parse", "HEAD"]).strip() == git(["rev-parse", "origin/main"]).strip())
    import r33c0b_bm_definition_decision as B
    bd = jload(RD / "r33c0b-decision-latest.json")
    ck("BM prospective = RETIRED",
       bd["decision"]["decision"] == "BM_PROSPECTIVE_RETIRED")
    ck("R33C0B 계약 hash 유지", B.contract_hash().startswith("03ed8dc3"))


def t_l2():
    print("\n[L2] SIZE 계약 복원")
    i = jload(INV)
    if not i:
        notrun("inventory", "artifact 없음")
        return
    C = D.SIZE_CONTRACT
    ck("factor 이름 SIZE_SMALL", C["factorLogicalName"] == "SIZE_SMALL")
    ck("score formula -ln(marketCap)", C["scoreFormula"] == "-ln(marketCap)")
    ck("소스에 실제로 존재", i["evidence"]["sizeFormulaInR25Factors"])
    ck("시총 = direct field", i["evidence"]["pitMarketCapIsDirectField"])
    ck("close × shares 를 정본으로 쓰지 않음",
       C["closeTimesSharesAsCanonical"] is False
       and not i["evidence"]["pitMarketCapIsCloseTimesShares"])
    ck("liquidity 125,000,000", "125,000,000" in C["liquidityRule"])
    ck("horizon 36M", C["horizonMonths"] == 36)
    ck("entry = next-session open", "next-session official open" in C["entryRule"])
    ck("tie-break ticker DESC", "ticker DESC" in C["tieBreak"])
    ck("universe 3종 분리", set(D.UNIVERSE_VARIANTS) == {
        "A_SIZE_FACTOR_SPECIFIC", "B_R32_BM_SIZE_MATCHED_COMMON",
        "C_R33B_TRANSLATED_SIZE"})
    ck("A 는 BM 불요",
       D.UNIVERSE_VARIANTS["A_SIZE_FACTOR_SPECIFIC"]["bmOrPbrRequired"] is False)
    ck("B 는 BM 필요",
       D.UNIVERSE_VARIANTS["B_R32_BM_SIZE_MATCHED_COMMON"]["bmOrPbrRequired"] is True)
    ck("C 는 BM 필요",
       D.UNIVERSE_VARIANTS["C_R33B_TRANSLATED_SIZE"]["bmOrPbrRequired"] is True)
    for k in ("r25FactorSpecificUniverse", "r26SharedUniverse", "r27SharedUniverse",
              "r27CohortUsesBmKeyedSet", "r27PairedUsesBmKeyedSet",
              "r32UsesR27BmKeyedSet", "r33bInheritsR27", "r33bOverridesOnlyTsr",
              "bmOnlyWhenPbrPositive"):
        ck(f"소스 실측: {k}", i["evidence"][k] is True)
    ck("실측 CASE = CASE_3", i["case"] == "CASE_3", i["case"])
    ck("universe 가 BM/PBR 독립이 아님",
       i["sizeUniverseIndependentOfBmPbr"] is False)


def t_l3():
    print("\n[L3] BM/PBR 의존성 크기")
    d, n = jload(DEP), jload(INC)
    if not d:
        notrun("dependency", "artifact 없음")
        return
    ck("성과 호출 0", d["performanceFunctionCalls"] == 0)
    ck("동결 이후 실행", d["ranAfterPrecommitFreeze"] is True)
    ck("월 단위 측정 존재", d["datesMeasured"] > 0)
    ck("PBR 요구가 종목을 실제로 뺀다", d["droppedMin"] > 0,
       str(d["droppedMin"]))
    ck("유지율 100% 아님", d["retainedPctMax"] < 100.0, str(d["retainedPctMax"]))
    if n:
        rows = n["rows"]
        ck("incidence 성과 0", n["performanceFunctionCalls"] == 0)
        ck("PBR 결측이 SIZE top bucket 에 집중",
           all(r["noPbrInSizeTopBucketPct"] > r["noPbrTotalPct"] * 3 for r in rows),
           str([r["noPbrInSizeTopBucketPct"] for r in rows]))
        ck("최소 시총 분위 결측률이 가장 높다",
           all(r["noPbrPctByMcapQuintile_smallToLarge"][0]
               == max(r["noPbrPctByMcapQuintile_smallToLarge"]) for r in rows))
        ck("top bucket 대체율 50% 초과(정의 변경 수준)",
           all(r["noPbrInSizeTopBucketPct"] > 50.0 for r in rows))


def t_l4():
    print("\n[L4] precommit 동결")
    h = D.contract_hash()
    r = subprocess.run([sys.executable, "-c",
                        "import sys;sys.path.insert(0,r'%s');"
                        "import r33c0c_size_only_go_nogo as D;"
                        "print(D.contract_hash())" % SRC],
                       capture_output=True, text=True)
    ck("contract hash 별도 프로세스 동일", r.stdout.strip() == h)
    p = jload(PRE)
    ck("precommit artifact 존재", p is not None)
    if p:
        ck("저장 hash == 재계산", p["contractHash"] == h)
        ck("측정 이전 동결 선언", p["frozenBeforeAnyMeasurement"] is True)
        ck("gate 를 결과에 맞춰 낮추지 않음",
           p["priorExposure"]["gatesLoweredAfterSeeingResults"] is False)
        ck("수익률로 gate 고르지 않음",
           p["priorExposure"]["returnMetricsUsedToChooseGates"] is False)
        ck("사전노출 공개", p["priorExposure"]["priorCanonicalEvidenceSeen"] is True)
        ck("성과 호출 0", p["performanceFunctionCalls"] == 0)
    for art in (INV, DEP, SO, TL, AR, DEC):
        if art.exists() and PRE.exists():
            ck(f"precommit 이 {art.name[:24]} 보다 먼저",
               PRE.stat().st_mtime <= art.stat().st_mtime)
    ck("mandatory gate 26개", len(D.MANDATORY_GATES) == 26)
    ck("gate 이름 중복 0", len(set(D.MANDATORY_GATES)) == 26)
    ck("auto NO-GO 부담 9종", len(D.AUTO_NOGO_BURDENS) == 9)


def t_l5():
    print("\n[L5] 공식 prospective source")
    s = jload(SO)
    if not s:
        notrun("source", "artifact 없음")
        return
    ck("성과 호출 0", s["performanceFunctionCalls"] == 0)
    ck("DART/KRX/network 호출 0",
       s["dartApiCalls"] == 0 and s["krxApiCalls"] == 0 and s["networkCalls"] == 0)
    for k in ("marketCap", "open", "close", "volume", "tradeValue",
              "market", "ticker", "shares"):
        ck(f"공식 field 존재: {k}", s["requiredFieldPresence"][k] is True)
    ck("필수 field 전부 존재", s["allRequiredFieldsPresent"] is True)
    ck("미래 날짜 입력 0", s["futureDatedFileCount"] == 0)
    ck("기존 snapshot 변조 0", s["oldSnapshotMutationCount"] == 0)
    ck("calendar 중복 0", s["calendarDuplicateCount"] == 0)
    ck("휴일표 불일치 0", s["holidayTableMismatch"] == 0)
    ck("official daily 진행", bool(s["officialDailyThrough"]))
    ck("next signal 2026-10-01", s["nextScheduledSignalDate"] == "2026-10-01")
    ck("next entry 2026-10-02", s["nextEntrySession"] == "2026-10-02")
    ck("PIT(PBR/BPS) producer 는 여전히 막혀 있다",
       s["pitProducerStatus"] == "BLOCKED_UPSTREAM_CREDENTIAL")
    ck("status artifact 존재(실패 표면)", s["producerStatusArtifactPresent"] is True)


def t_l6():
    print("\n[L6] 기존 07:40 운영")
    d = jload(DEC)
    if not d:
        notrun("decision", "artifact 없음")
        return
    sc, pd = d["schedulerIdentity"], d["preOpenDeadline"]
    ck("task 이름이 아니라 action 으로 확인",
       "magic_morning_combined_report.py" in sc["exec"])
    ck("07:40 trigger", sc["startBoundary"].startswith("07:40"))
    ck("state Ready", sc["state"] == "Ready")
    ck("직전 실행 결과 0", sc["lastTaskResult"] == 0)
    ck("working dir = 데이터 저장소",
       "kr-stock-agent-data-new" in sc["workingDirectory"])
    ck("개장전 여유 >= 10분", pd["marginMinutes"] >= 10, str(pd["marginMinutes"]))
    g = d["decision"]["gateMatrix"]
    for k in ("G10_new_scheduler_required_zero", "G11_new_credential_required_zero",
              "G12_new_vendor_required_zero", "G13_paid_recurring_data_cost_zero",
              "G14_manual_company_security_mapping_zero",
              "G15_normal_monthly_founder_action_zero",
              "G16_normal_daily_founder_action_zero",
              "G20_new_db_storage_service_required_zero",
              "G21_rowlevel_remote_upload_required_zero"):
        ck(f"운영 gate PASS: {k[:34]}", g[k] is True)
    ck("자동 NO-GO 부담 0", d["decision"]["autoNoGoBurdensHit"] == [])
    a = jload(AR)
    if a:
        ck("BM pair 구성요소 제거됨", a["removedCount"] >= 8)
        ck("thin engine 가능", a["thinEngineFeasible"] is True)
        ck("신규 scheduler 0", a["newSchedulerCount"] == 0)


def t_l7():
    print("\n[L7] timeline · review gate")
    t = jload(TL)
    if not t:
        notrun("timeline", "artifact 없음")
        return
    ck("성과 호출 0", t["performanceFunctionCalls"] == 0)
    ck("first signal 2026-10-01", t["firstSignal"] == "2026-10-01")
    ck("first maturity = +36M", t["firstMaturity"] == "2029-10-01")
    ck("12th maturity", t["maturity12th"] == "2030-09-01")
    ck("24th maturity", t["maturity24th"] == "2031-09-01")
    ck("36th maturity", t["maturity36th"] == "2032-09-01")
    ck("2nd non-overlapping anchor",
       t["secondNonOverlappingAnchorMaturity"] == "2032-10-01")
    ck("정식 review 는 36th·anchor 중 나중",
       t["earliestFormalReview"] == max(t["maturity36th"],
                                        t["secondNonOverlappingAnchorMaturity"]))
    g = t["reviewTimeGate"]
    ck("maturity 전 정식 판정 금지", g["noFormalDecisionBeforeMaturity"] is True)
    ck("12·24 는 descriptive 만", g["descriptiveCheckpointsOnly"] == [12, 24])
    ck("matured cohort >= 36", g["minMaturedMonthlyCohorts"] == 36)
    ck("non-overlapping anchor >= 2", g["minNonOverlappingAnchorCohorts"] == 2)
    ck("중간결과로 승격·중단 금지",
       g["interimResultMayStopOrPromoteFactor"] is False)
    ck("historical threshold 변경 0", g["historicalThresholdChangeAllowed"] == 0)


def t_l8():
    print("\n[L8] 결정 엔진")
    d = jload(DEC)
    if not d:
        notrun("decision", "artifact 없음")
        return
    ck("결정 enum 2개", d["decisionEnum"] == D.DECISION_ENUM)
    ck("enum 길이 정확히 2(중간값 불가)", len(D.DECISION_ENUM) == 2)
    ck("결정이 enum 안에", d["decision"]["decision"] in D.DECISION_ENUM)
    ck("실측 결정 = NO_GO",
       d["decision"]["decision"] == "PROSPECTIVE_OOS_NO_GO",
       d["decision"]["decision"])
    fg = d["decision"]["failedMandatoryGates"]
    ck("G2 실패 기록", "G2_size_universe_independent_of_bm_pbr" in fg)
    ck("G3 실패 기록", "G3_market_data_only_selection_exact_reproduction" in fg)
    ck("G18 실패 기록", "G18_bm_pbr_dart_dependency_zero" in fg)
    ck("passed + failed == 26",
       len(fg) + len(d["decision"]["passedMandatoryGates"]) == 26)
    ck("reason code 존재", len(d["decision"]["decisionReasonCodes"]) > 0)

    # ── 결과독립 fixture ──────────────────────────────────────────────
    allp = {g: True for g in D.MANDATORY_GATES}
    zero = {b: False for b in D.AUTO_NOGO_BURDENS}
    ck("전 gate PASS + 부담 0 → GO",
       D.decide(allp, zero)["decision"] == "SIZE_ONLY_OOS_GO")
    for g, label in (("G4_official_direct_market_cap_available", "source"),
                     ("G2_size_universe_independent_of_bm_pbr", "universe"),
                     ("G15_normal_monthly_founder_action_zero", "Founder action"),
                     ("G13_paid_recurring_data_cost_zero", "paid source"),
                     ("G10_new_scheduler_required_zero", "scheduler")):
        one = dict(allp)
        one[g] = False
        ck(f"{label} gate 하나 실패 → NO_GO",
           D.decide(one, zero)["decision"] == "PROSPECTIVE_OOS_NO_GO")
    # 장기 horizon 보정
    ck("긴 horizon + 한계부담 0 → GO 허용",
       D.decide(allp, zero)["decision"] == "SIZE_ONLY_OOS_GO")
    for b in ("recurringFounderAction", "recurringPaidData", "newScheduler",
              "monthlyManualMapping", "browserSessionDependence"):
        bb = dict(zero)
        bb[b] = True
        ck(f"긴 horizon + {b[:22]} → NO_GO",
           D.decide(allp, bb)["decision"] == "PROSPECTIVE_OOS_NO_GO")
    # 미측정 gate 를 PASS 로 승격하지 않는다
    miss = {g: True for g in D.MANDATORY_GATES[:-1]}
    r = D.decide(miss, zero)
    ck("미측정 gate 는 PASS 가 아니다", r["decision"] == "PROSPECTIVE_OOS_NO_GO")
    ck("미측정은 NOT_MEASURED 코드로 표면화",
       any(c.startswith("GATE_NOT_MEASURED::") for c in r["decisionReasonCodes"]))
    # 중간 결론 거부
    for bad in ("CONDITIONAL_GO", "MORE_RESEARCH_REQUIRED", "PROVISIONAL_GO",
                "SIZE_PRIMARY"):
        ck(f"중간 결론 미존재: {bad}", bad not in D.DECISION_ENUM)
    ck("계약이 conditional GO 를 금지",
       D.CONTRACT["conditionalGoAllowed"] is False
       and D.CONTRACT["moreResearchAllowed"] is False)


def t_l9():
    print("\n[L9] 성과 미접근")
    d = jload(DEC)
    if d:
        for k in ("newPerformanceFunctionCalls", "forwardReturnRowsRead",
                  "newHistoricalReturnCalculations", "r32DecisionEngineCalls",
                  "alternativeRulePerformanceRuns"):
            ck(f"{k} == 0", d[k] == 0)
    ck("계약: historical 성과 재계산 금지",
       D.CONTRACT["historicalPerformanceRecomputation"] is False)
    ck("계약: selection 재현은 수익률 없이만",
       D.CONTRACT["historicalSelectionReproduction"] == "ALLOWED_WITHOUT_RETURNS")
    ck("계약: OOS 구현 불가", D.CONTRACT["oosImplementationAllowed"] is False)
    ck("계약: BM 재도입 불가", D.CONTRACT["bmReintroductionAllowed"] is False)
    ck("계약: SIZE 정의 변경 불가", D.CONTRACT["sizeDefinitionChangeAllowed"] is False)
    ck("계약: universe 번역 불가", D.CONTRACT["universeTranslationAllowed"] is False)
    # guard 실증
    try:
        import r27_analysis as A
        with D.PerfGuard():
            try:
                A.R27.tsr(None, 0, 1, "000000")
                ck("guard 가 R27.tsr 차단", False, "예외 없음")
            except D.PerformanceAccessViolation:
                ck("guard 가 R27.tsr 차단", True)
            try:
                A.R27.paired(None, 36)
                ck("guard 가 R27.paired 차단", False, "예외 없음")
            except D.PerformanceAccessViolation:
                ck("guard 가 R27.paired 차단", True)
    except Exception as e:
        ck("guard 실증", False, f"{type(e).__name__}")
    src = (SRC / "r33c0c_size_only_go_nogo.py").read_text(encoding="utf-8")
    for bad in ("19.147", "15.009", "13.697", "12.298", "14.090"):
        ck(f"published 수익률 상수 없음: {bad}", bad not in src)
    # 정규식은 docstring 의 경로 인용("r25_analysis.cohort()")과 소스 확인용 문자열
    # 리터럴("def tsr(self, i, j, t):")까지 잡는다. 실제 위험은 **호출**뿐이므로
    # AST 로 Call 노드만 본다 — 문자열·주석에 속지 않는다.
    import ast
    banned = {"tsr", "paired", "cohort", "cohorts", "decide_r32"}
    called = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Call):
            f = node.func
            nm = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            if nm in banned:
                called.add(nm)
    ck("성과 함수 호출 0 (AST)", not called, str(sorted(called)))
    prohibited("SIZE forward return 재계산", "§4 금지")
    prohibited("CAGR/Sharpe/drawdown 재계산", "§4 금지")
    prohibited("R32 decision engine 실행", "§4 금지")
    prohibited("BM 과 SIZE 성과 비교", "§4 금지")
    prohibited("2020+ subperiod 성과 재계산", "§4 금지")


def t_l10():
    print("\n[L10] 경계")
    ck("R33C engine 미생성", not (SRC / "r33c_oos_paper.py").exists())
    ck("SIZE-only engine 미생성", not (SRC / "r33d_size_oos.py").exists())
    ck("ledger 미생성", not (ROOT / "_cache" / "oos-r33c").exists()
       and not (ROOT / "_cache" / "oos-size").exists())
    ck("activation manifest 미생성",
       not (RD / "r33c-activation-manifest-latest.json").exists()
       and not (RD / "r33c0c-activation-manifest-latest.json").exists())
    ck("observer 미등록", not (RD / "r33c0c-observer-latest.json").exists())
    ck("2026-09-01 signal 미생성",
       not (RD / "r33c0c-signal-2026-09-01.json").exists())
    src = (SRC / "r33c0c_size_only_go_nogo.py").read_text(encoding="utf-8")
    for bad in ("DART_API_KEY", "crtfc_key", "AUTH_KEY", "os.environ", "getenv",
                "requests.", "urlopen", "pykrx"):
        ck(f"모듈에 {bad} 없음", bad not in src)
    toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", src)
            if not re.fullmatch(r"[0-9a-f]{64}", w) and re.search(r"[0-9]", w)]
    ck("key 형태 토큰 0", not toks, str(toks[:2]))
    ck("실주문/broker 호출 0",
       not re.findall(r"\b(place_order|submit_order|send_order|broker)\s*\(", src))
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 _cache/reports 없음",
       not any(x.startswith(("_cache", "reports")) for x in staged), str(staged))
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    if PUBLIC_REPO.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
        ck("public repo 이번 작업 staged 0",
           git(["diff", "--cached", "--name-only"], cwd=PUBLIC_REPO).strip() == "")
    if AUTOMATION_REPO.exists():
        ck("automation repo staged 0",
           git(["diff", "--cached", "--name-only"], cwd=AUTOMATION_REPO).strip() == "")
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    for art in (PRE, INV, DEP, INC, SO, TL, AR, DEC):
        ck(f"JSON parse: {art.name[:34]}", jload(art) is not None)
    blob = " ".join(json.dumps(jload(a), ensure_ascii=False)
                    for a in (INV, DEP, INC, SO, TL, AR, DEC) if a.exists())
    # "ticker": true 는 공식 field 의 **존재 여부 불리언**이다(row-level 아님).
    # row-level 유출은 값·배열로 판정한다.
    ck("artifact 에 holdings/topTickers 배열 없음",
       not re.findall(r'"(?:topTickers|topKept|holdings|selectedTickers)"\s*:\s*\[',
                      blob))
    ck("artifact 에 6자리 종목코드 나열 없음",
       len(re.findall(r'"\d{6}"', blob)) == 0)
    md = WD / "wababa-size-only-prospective-oos-go-nogo-r33c0c-latest.md"
    if not md.exists():
        notrun("R33C0C canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED|WAIT)$",
                    body.splitlines()[0].strip()) is not None)
        for need in ("PROSPECTIVE_OOS_NO_GO", "REAL_MONEY_NOT_APPROVED",
                     "836b03d", "CASE_3"):
            ck(f"보고 항목: {need}", need in body)
        for banned in ("SIZE factor 무효", "SIZE 가짜", "BM 이 옳았다"):
            ck(f"금지 표현 없음: {banned}", banned not in body)
        # 중간 결론은 '결정으로 선언' 되면 안 된다. 금지 대상으로 **나열**하는
        # 설명 문장은 정상이므로, 결정을 주장하는 줄만 검사한다.
        decl = [ln for ln in body.splitlines()
                if "FINAL_SIZE_ONLY_OOS_DECISION" in ln or "DECISION:" in ln]
        ck("결정 선언 줄 존재", bool(decl))
        for banned in ("CONDITIONAL_GO", "MORE_RESEARCH_REQUIRED",
                       "PROVISIONAL_GO", "SIZE_PRIMARY", "SIZE_ONLY_OOS_GO"):
            ck(f"결정으로 선언되지 않음: {banned}",
               not any(banned in ln for ln in decl))
        ck("결정 선언은 NO_GO",
           any("PROSPECTIVE_OOS_NO_GO" in ln for ln in decl))
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8, t_l9, t_l10):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN} / "
          f"PROHIBITED_AND_NOT_CALLED {PROHIB}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
