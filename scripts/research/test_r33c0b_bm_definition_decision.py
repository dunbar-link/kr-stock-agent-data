#!/usr/bin/env python3
"""R33C0B 회귀 — 단일 후보·정의 동결·coverage 게이트·결정 enum·경계.

WABABA-BM-PROSPECTIVE-DEFINITION-DECISION-PRECOMMIT-R33C0B

계층:
  L1 frozen  — 상위 동결 hash · source commit
  L2 cand    — 후보 정확히 1개 · 정의 항목 전부 고정 · 수동 override 0
  L3 pre     — precommit hash 결정성 · coverage 계산 이전 동결 · 사전노출 공개
  L4 cover   — coverage gate 사전동결값 · 월 전체 판정
  L5 bias    — missingness 편향 · historical feasibility
  L6 dec     — 결정 enum · gate fixture(결과독립) · 두 번째 후보 미시도
  L7 perf    — 성과 접근 0 (PROHIBITED_AND_NOT_CALLED)
  L8 bound   — R33C 미생성 · SIZE 자동승격 0 · public/실주문 경계

사용: python scripts/research/test_r33c0b_bm_definition_decision.py
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
import r33c0b_bm_definition_decision as D      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")

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


DECL = RD / "r33c0b-definition-declaration-latest.json"
PRE = RD / "r33c0b-definition-precommit-latest.json"
COV = RD / "r33c0b-coverage-feasibility-latest.json"
OPS = RD / "r33c0b-operational-feasibility-latest.json"
DEC = RD / "r33c0b-decision-latest.json"


def t_l1():
    print("\n[L1] frozen 정본")
    import r33a_translation_precommit as A
    import r33b_precommit as P
    import r33c0_research_data_producer as C0
    ck("r27/r31/r32", P.upstream_hashes()["allMatch"])
    st = jload(RD / "r31-stitch-latest.json")
    ck("r31 manifest 3439dec9", st["manifestSha256"].startswith("3439dec9"))
    ck("R33A d869f445", A.contract_hash() == P.SOURCE_CONTRACT_HASH)
    ck("R33B fea25545", P.spec_hash().startswith("fea25545"))
    ck("R33C0 a9f47e1a", C0.contract_hash().startswith("a9f47e1a"))
    ck("source commit 4cba54d ancestor", subprocess.run(
        ["git", "merge-base", "--is-ancestor", "4cba54d", "origin/main"],
        cwd=ROOT, capture_output=True).returncode == 0)


def t_l2():
    print("\n[L2] 후보 선언")
    C = D.CANDIDATE
    ck("candidate count 정확히 1", D.CONTRACT["candidateCount"] == 1)
    ck("두 번째 후보 금지", D.CONTRACT["secondCandidateAllowed"] is False)
    ck("기존 BM 과 비동등 명시", D.CONTRACT["equivalentToHistoricalBM"] is False)
    ck("새 factor version 1개", D.CONTRACT["newFactorVersionCount"] == 1)
    ck("historical factor 상태 HISTORICAL_ONLY",
       D.CONTRACT["historicalFactorStatus"] == "HISTORICAL_ONLY")
    for k in ("numeratorAccountId", "denominator", "fsDivPriority",
              "reportCodePriority", "disclosureCutoff", "restatementPolicy",
              "multiClassPolicy", "representativeSecurityPolicy",
              "negativeBookPolicy", "zeroBookPolicy", "missingPolicy",
              "rankingDirection", "tieBreak", "formula"):
        ck(f"정의 항목 고정: {k}", bool(C.get(k)))
    ck("numerator = 지배주주지분",
       C["numeratorAccountId"] == "ifrs-full_EquityAttributableToOwnersOfParent")
    ck("수동 계정 override 금지", C["manualAccountOverride"] == "FORBIDDEN")
    ck("수동 종목 override 금지", C["manualSecurityOverride"] == "FORBIDDEN")
    ck("cutoff 보수적 D-1", C["disclosureCutoff"] == "rcept_dt <= D-1")
    ck("대표종목 규칙이 기존 저장소 정본 인용",
       "backtest_engine" in C["representativeSecurityPolicy"])
    ck("다중클래스 휴리스틱 미생성", "휴리스틱 매핑은 만들지 않는다"
       in C["multiClassPolicy"])
    ck("다중클래스 편향 공개", bool(C["multiClassKnownBias"]))
    ck("결측 미보간(R25 계승)", "채우지 않는다" in C["missingPolicy"])
    ck("랭킹 방향 higher B/M", "higher" in C["rankingDirection"])
    ck("entry/liquidity 는 frozen 계승",
       "R33A" in C["entryRule"] and "125,000,000" in C["liquidityRule"])


def t_l3():
    print("\n[L3] precommit 동결")
    h = D.contract_hash()
    r = subprocess.run([sys.executable, "-c",
                        "import sys;sys.path.insert(0,r'%s');"
                        "import r33c0b_bm_definition_decision as D;"
                        "print(D.contract_hash())" % SRC],
                       capture_output=True, text=True)
    ck("contract hash 별도 프로세스 동일", r.stdout.strip() == h)
    d = jload(PRE)
    ck("precommit artifact 존재", d is not None)
    if d:
        ck("저장 hash == 재계산", d["contractHash"] == h)
        pe = d["priorExposure"]
        ck("사전노출 공개(비성과)",
           pe["nonPerformanceEquivalenceMetricsSeen"] is True)
        ck("수익률 기반 후보선택 아님",
           pe["returnMetricsSeenForCandidateSelection"] is False)
        ck("old BM overlap 을 acceptance 로 안 씀",
           pe["oldBmOverlapUsedAsAcceptance"] is False)
        ck("gate 를 사전노출에 맞춰 낮추지 않음 기록",
           "낮추지 않았다" in pe["note"])
    decl = jload(DECL)
    ck("후보 선언이 coverage 계산 전",
       bool(decl) and decl["declaredBeforeCoverageComputation"] is True)
    c = jload(COV)
    ck("coverage 는 동결 이후 실행", bool(c) and c["ranAfterPrecommitFreeze"] is True)
    if c and decl and PRE.exists() and COV.exists():
        ck("precommit 파일이 coverage 파일보다 먼저",
           PRE.stat().st_mtime <= COV.stat().st_mtime)


def t_l4():
    print("\n[L4] coverage gate")
    g = D.COVERAGE_GATE
    ck("gate 가 R27 정본과 정합", "R27" in g["source"])
    ck("종목 coverage 90%", g["minSecurityCountCoverageEachMonth"] == 90.0)
    ck("시총 coverage 95%", g["minIssuerMarketCapCoverageEachMonth"] == 95.0)
    ck("KOSPI/KOSDAQ 85%",
       g["minKospiSecurityCoverageEachMonth"] == 85.0
       and g["minKosdaqSecurityCoverageEachMonth"] == 85.0)
    ck("전월 통과 요구", g["allMonthsMustPass"] is True)
    ck("평균으로 실패월 못 덮음", g["averageCannotCoverFailingMonth"] is True)
    for k in ("unresolvedAccountMappingAllowed", "manualCompanyOverrideAllowed",
              "futureFilingUseAllowed", "currentListingLeakageAllowed",
              "duplicateIssuerScoreAllowed"):
        ck(f"gate {k} == 0", g[k] == 0)
    c = jload(COV)
    if not c:
        notrun("coverage 결과", "artifact 없음")
        return
    ck("12개월 감사", c["result"]["monthsAudited"] == 12,
       str(c["result"]["monthsAudited"]))
    ck("성과 함수 호출 0", c["performanceFunctionCalls"] == 0)
    ck("실측: 전월 통과 실패", c["result"]["allMonthsPass"] is False)
    ck("실측: gate 실패 다수", c["result"]["gateFailureCount"] > 0,
       str(c["result"]["gateFailureCount"]))
    lo = min(m["securityCoveragePct"] for m in c["result"]["months"])
    hi = max(m["securityCoveragePct"] for m in c["result"]["months"])
    ck("종목 coverage 가 90% 미만", hi < 90.0, f"{lo}~{hi}")
    mc = min(m["marketCapCoveragePct"] for m in c["result"]["months"])
    ck("시총 coverage 는 통과 수준(대형 편중 증거)", mc >= 95.0, str(mc))


BIAS = RD / "r33c0b-missingness-bias-latest.json"


def t_l5():
    print("\n[L5] 편향 · historical feasibility")
    b = jload(BIAS)
    if not b:
        notrun("bias artifact", "없음")
    else:
        ck("bias 성과 호출 0", b["performanceFunctionCalls"] == 0)
        ck("bias 는 동결 이후 실행", b["ranAfterPrecommitFreeze"] is True)
        ck("bias contract hash 일치", b["contractHash"] == D.contract_hash())
        ck("수익률 미연결 명시", "수익률과 연결하지 않음" in b["note"])
        ck("size 5분위 측정", len(b["bySize"]) == 5)
        ck("실측: 크기 단조 감소", b["monotoneDecreasingWithSize"] is True)
        ck("실측: 편향 없음 아님", b["biasFree"] is False)
        ck("실측: 분위 격차 10%p 초과", b["sizeCoverageSpreadPp"] > 10.0,
           str(b["sizeCoverageSpreadPp"]))
        sm = {x["market"]: x["coveragePct"] for x in b["byMarket"]}
        ck("실측: KOSPI > KOSDAQ coverage", sm["KOSPI"] > sm["KOSDAQ"],
           f"{sm['KOSPI']} vs {sm['KOSDAQ']}")
        ck("실측: UNKNOWN market 0%", sm.get("UNKNOWN") == 0.0)
    o = jload(OPS)
    if not o:
        notrun("operational artifact", "없음")
        return
    h = o["historical"]
    ck("DART 연간 연도 수 기록", h["yearCount"] > 0)
    ck("15개 연도 요건 미달(실측)", h["minimum15Years"] is False,
       str(h["yearCount"]))
    ck("R33B 는 163 cohort 2010~2023 였다(대조 가능)", h["yearCount"] < 15)
    op = o["operational"]
    ck("기존 07:40 재사용 가능", op["existing0740Reusable"] is True)
    ck("신규 scheduler 불필요", op["newSchedulerRequired"] is False)
    ck("credential 변경 불필요", op["credentialChangeNeeded"] is False)
    mt = o["maintenance"]
    for k in ("manualAccountMapping", "manualSecurityMapping",
              "monthlyFounderCheck", "webSessionLogin", "newVendor",
              "newScheduler"):
        ck(f"유지보수 {k} False", mt[k] is False)
    ck("병렬 BM 정의 1개", mt["parallelBmDefinitions"] == 1)


def t_l6():
    print("\n[L6] 결정 엔진")
    d = jload(DEC)
    if not d:
        notrun("decision", "artifact 없음")
        return
    ck("결정 enum 2개", d["decisionEnum"] == D.DECISION_ENUM)
    ck("결정이 enum 안에", d["decision"]["decision"] in D.DECISION_ENUM)
    ck("실측 결정 = RETIRED",
       d["decision"]["decision"] == "BM_PROSPECTIVE_RETIRED",
       d["decision"]["decision"])
    ck("두 번째 후보 미시도", d["secondCandidateAttempted"] is False)
    ck("실패 gate 기록", len(d["decision"]["failedGates"]) > 0)
    ck("NO_SECOND_CANDIDATE 코드",
       "NO_SECOND_CANDIDATE_ATTEMPTED" in d["decision"]["reasonCodes"])
    # 결과독립 fixture
    allpass = {k: True for k in d["gateMatrix"]}
    ck("전부 PASS → FROZEN",
       D.decide(allpass)["decision"] == "BM_DART_V2_DEFINITION_FROZEN")
    for k in list(d["gateMatrix"])[:4]:
        one = dict(allpass)
        one[k] = False
        ck(f"'{k[:18]}' 하나만 실패 → RETIRED",
           D.decide(one)["decision"] == "BM_PROSPECTIVE_RETIRED")
    ck("중간값 없음(enum 2개뿐)", len(D.DECISION_ENUM) == 2)


def t_l7():
    print("\n[L7] 성과 미접근")
    d = jload(DEC)
    if d:
        for k in ("performanceFunctionCalls", "historicalReturnFilesRead",
                  "r32DecisionEngineCalls", "alternativeDefinitionPerformanceRuns"):
            ck(f"{k} == 0", d[k] == 0)
    ck("성과로 정의하지 않음", D.CONTRACT["performanceUsedToDefine"] is False)
    ck("historical return validation NOT_RUN",
       D.CONTRACT["historicalReturnValidation"] == "NOT_RUN")
    # guard 실증
    try:
        import r27_analysis as A
        with D.PerfGuard():
            try:
                A.R27.tsr(None, 0, 1, "000000")
                ck("guard 가 tsr 차단", False, "예외 없음")
            except D.PerformanceAccessViolation:
                ck("guard 가 tsr 차단", True)
    except Exception as e:
        ck("guard 실증", False, f"{type(e).__name__}")
    src = (SRC / "r33c0b_bm_definition_decision.py").read_text(encoding="utf-8")
    for bad in ("19.147", "15.009", "13.697", "12.298", "14.090"):
        ck(f"published 수익률 상수 없음: {bad}", bad not in src)
    prohibited("BM_DART_V2 historical return 계산", "§3 금지 — R33C0B1 소관")
    prohibited("기존 BM 과 새 BM 수익률 비교", "§3 금지")
    prohibited("SIZE 와 BM 수익률 비교", "§3 금지")
    prohibited("R32 decision engine 실행", "§3 금지")


def t_l8():
    print("\n[L8] 경계")
    ck("R33C engine 미생성", not (SRC / "r33c_oos_paper.py").exists())
    ck("R33C ledger 미생성", not (ROOT / "_cache" / "oos-r33c").exists())
    ck("R33C activation 미생성",
       not (RD / "r33c-activation-manifest-latest.json").exists())
    ck("BM producer 미구현", not (SRC / "r33c0b_bm_producer.py").exists())
    ck("SIZE 단독 자동활성화 0",
       not (RD / "r33c0c-size-activation-latest.json").exists())
    src = (SRC / "r33c0b_bm_definition_decision.py").read_text(encoding="utf-8")
    for bad in ("DART_API_KEY", "crtfc_key", "os.environ", "getenv", "requests."):
        ck(f"모듈에 {bad} 없음", bad not in src)
    # key 는 hex/base64 라 반드시 숫자를 포함한다. 순수 알파벳 camelCase 식별자
    # (priorNonPerformanceEquivalenceMetricsSeen 등)는 40자를 넘어도 key 가 아니다.
    # SHA-256(64-hex)은 기록이 정본이므로 제외한다.
    toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", src)
            if not re.fullmatch(r"[0-9a-f]{64}", w) and re.search(r"[0-9]", w)]
    ck("key 형태 토큰 0", not toks, str(toks[:2]))
    ck("40-hex(DART key 형태) 토큰 0",
       not [w for w in re.findall(r"[0-9a-f]{40}", src)
            if not re.search(r"[0-9a-f]{64}", src[max(0, src.find(w) - 24):
                                                 src.find(w) + 88])])
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
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    ck("실주문 호출 0",
       not re.findall(r"\b(place_order|submit_order|send_order)\s*\(", src))
    md = WD / "wababa-bm-prospective-definition-decision-precommit-r33c0b-latest.md"
    if not md.exists():
        notrun("R33C0B canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED|WAIT)$", first)
           is not None, first)
        for need in ("BM_PROSPECTIVE_RETIRED", "REAL_MONEY_NOT_APPROVED",
                     "4cba54d"):
            ck(f"보고 항목: {need}", need in body)
        for banned in ("BM 실패", "historical BM 무효", "SIZE 자동 승리"):
            ck(f"금지 표현 없음: {banned}", banned not in body)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN} / "
          f"PROHIBITED_AND_NOT_CALLED {PROHIB}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
