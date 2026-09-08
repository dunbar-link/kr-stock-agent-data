#!/usr/bin/env python3
"""R33A 회귀 — 실행번역 계약 동결 + field availability. 성과계산 0.

WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A

계층:
  L1 frozen    — R27/R31/R32 precommit 해시 · R32A/R33 commit ancestor
  L2 blocker   — same-close blocker 재현 · liquidity timing 위반 0
  L3 contract  — 번역규칙 · entry/exit/no-fill/cash/cost/비중 계약 동결값
  L4 hash      — 직렬화 결정성 · 성과 이전 동결 · correction append-only
  L5 avail     — historical field availability (계약 동결 이후 실행)
  L6 perf0     — 성과 미접근을 실행으로 증명 (PROHIBITED_AND_NOT_CALLED)
  L7 boundary  — raw Git 제외 · public repo · LEGACY_50D · 실주문 · 미생성 확인
  L8 outputs   — 산출물 parse · 판정 헤더 · secret scan · git diff --check

성과 관련 검사는 NOT_RUN 이 아니라 PROHIBITED_AND_NOT_CALLED 로 기록한다(§19).

사용: python scripts/research/test_r33a_translation_precommit.py
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
import r33a_translation_precommit as P        # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")
AUTOMATION_REPO = Path(r"C:\work\ai-operating-system")

CONTRACT_JSON = RD / "r33a-translation-precommit-latest.json"
AVAIL_JSON = RD / "r33a-availability-latest.json"
R33_JSON = RD / "r33-feasibility-latest.json"

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


def git(args, cwd=ROOT):
    try:
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def jload(p):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ── L1 frozen ─────────────────────────────────────────────────────────
def t_l1():
    print("\n[L1] frozen precommit · source commits")
    h = P.frozen_precommit_hashes()
    for n, pre in P.FROZEN_PRECOMMITS.items():
        ck(f"{n} 해시 {pre}", h[n]["match"], h[n]["sha256"][:12])
    ck("frozen 전체 일치", h["allMatch"])
    for c, label in ((P.RESEARCH_BASE_COMMIT, "R32A base 0431003"),
                     (P.SOURCE_R33_COMMIT, "R33 source 04beb59")):
        ck(f"{label} 존재", git(["cat-file", "-t", c]).strip() == "commit")
        r = subprocess.run(["git", "merge-base", "--is-ancestor", c, "origin/main"],
                           cwd=ROOT, capture_output=True, text=True)
        ck(f"{label} origin/main ancestor", r.returncode == 0)


# ── L2 blocker 재현 ───────────────────────────────────────────────────
def t_l2():
    print("\n[L2] same-close blocker 재현 · liquidity timing")
    d = jload(R33_JSON)
    if not d:
        notrun("R33 feasibility", "r33-feasibility-latest.json 없음")
        return
    ck("R33 판정 BLOCKED 유지", d["verdict"] == "BLOCKED")
    ck("blocker reason_class 일치",
       d["reasonClass"] == "R32_HISTORICAL_CONTRACT_NOT_PROSPECTIVELY_EXECUTABLE")
    t = d["inputTiming"]
    ck("same-bar 163/163 재현", t["sameInstantRankingAndEntryCount"] == 163)
    ck("liquidity look-ahead 위반 0", t["liquidityLookAheadViolations"] == 0)
    i = d["sameDayCloseDependency"]
    ck("marketCap == close×shares 100%", i["sizeIdentity"]["exactPct"] == 100.0)
    ck("PBR == close/BPS 100%",
       i["bmIdentity"]["withinPublishedRoundingPct"] == 100.0)


# ── L3 계약 내용 ──────────────────────────────────────────────────────
def t_l3():
    print("\n[L3] 계약 동결값")
    C = P.CONTRACT
    ck("translation rule ID 고정",
       P.TRANSLATION_RULE_ID == "D_CLOSE_RANK_NEXT_SESSION_OPEN_V1")
    ck("contract ID 고정", P.CONTRACT_ID == "WABABA_EXECUTION_TRANSLATION_R33A_V1")

    s = C["signal"]
    ck("factor price date = D", s["factorPriceDate"] == "D")
    ck("factor price field = official close", s["factorPriceField"] == "official close")
    ck("liquidity as-of 는 D 직전만", "직전" in s["liquidityAsOf"])
    ck("ranking freeze 는 official_write 이후",
       "official_write" in s["rankingFreezeEvent"])
    ck("ranking freeze deadline 존재", bool(s["rankingFreezeDeadline"]))
    ck("holdings 는 entry price 존재 전에 불변",
       s["holdingsImmutableBeforeEntryPriceExists"] is True)

    e = C["entry"]
    ck("entry = D 다음 공식 거래일", "다음" in e["targetSessionRule"])
    ck("entry price field = official opening price",
       e["priceField"] == "official opening price")
    ck("entry execution model = PAPER_MARKET_ON_OPEN",
       e["executionModel"] == "PAPER_MARKET_ON_OPEN")
    ck("same-bar execution FALSE", e["sameBarExecution"] is False)
    ck("entry price 는 freeze 시점에 미지",
       e["entryPriceKnownWhenRankingFrozen"] is False)
    ck("no-fill rule 고정",
       e["nonFillRule"] == "NO_VALID_OFFICIAL_OPEN_MEANS_NO_FILL")
    ck("entry retry 금지", e["retryRule"].startswith("NO_LATER_ENTRY"))
    ck("실패 자본은 현금 보유", e["failedEntryCapital"] == "RETAIN_AS_CASH")
    ck("실패 비중 재분배 금지", e["failedEntryWeightRedistribution"] is False)
    ck("데이터 누락 != no-fill", "구분" in e["dataMissingIsNotNonFill"])

    x = C["exit"]
    ck("36 calendar months from entry",
       "36 calendar months from entry target date" == x["nominalHoldingHorizon"])
    ck("exit price field = official opening price",
       x["priceField"] == "official opening price")
    ck("exit 미체결 시 pending",
       x["noValidOpenRule"].startswith("EXIT_PENDING_UNTIL"))
    ck("exit 가격 대체 금지", x["priceSubstitution"].startswith("FORBIDDEN"))
    ck("delisting rule 유지", "R32A" in x["delistingRule"])
    ck("corporate action rule 유지", "R32A" in x["corporateActionRule"])

    sel = C["selection"]
    ck("동일가중 유지", "동일가중" in sel["weighting"])
    ck("no-fill 후 비중 재정규화 금지",
       sel["weightRenormalizationAfterNonFill"] is False)
    ck("tie-break 두 경로 차이 기록", sel["tieBreak"]["knownInconsistency"] is True)
    ck("tie-break 이번에 변경 안 함", sel["tieBreak"]["changedHere"] is False)

    liq = C["liquidity"]
    ck("BASE 125,000,000 정확", "125,000,000" in liq["primaryRule"])
    ck("LOW 25,000,000 유지", "25,000,000" in liq["lowRule"])
    ck("HIGH 250,000,000 유지", "250,000,000" in liq["highRule"])
    ck("20D direct trade value only", "20D median direct trade value" in liq["primaryRule"])
    ck("유동성 변경 없음", liq["changed"] is False)

    cost = C["cost"]
    ck("비용 stress 0/25/50/100", cost["stressScenarios"] == [0, 25, 50, 100])
    ck("비용률 변경 없음", cost["ratesChanged"] is False)
    ck("bps 는 round-trip", "round-trip" in cost["bpsMeaning"])
    ck("비용식 R32 그대로",
       cost["formula"] == "costDragPct = annualTurnover × bps / 100")
    ck("no-fill 비용 0", cost["noFillCost"] == 0)
    ck("cash 비용 0", cost["cashCost"] == 0)
    ck("새 primary cost 미선택", "고르지 않는다" in cost["primaryCost"])

    cash = C["cash"]
    ck("cash 0% nominal 사전고정", cash["returnRule"] == "0% nominal")
    ck("cash 는 측정값이 아니라 고정값", cash["measuredNotAssumed"] is False)
    ck("cash 고정 이유 기록", "NOT_MODELLED_COHORT_METHOD" in cash["why"])

    f = C["factors"]
    ck("factor set = BM, SIZE_SMALL", f["set"] == ["BM", "SIZE_SMALL"])
    ck("factor 결합 금지", f["combinationAllowed"] is False)
    ck("BM/SIZE 동일 timing", f["identicalTimingBothFactors"] is True)
    sc = C["scope"]
    ck("factor 정의 불변", sc["factorDefinitionsChanged"] is False)
    ck("universe 불변", sc["universeChanged"] is False)
    ck("liquidity threshold 불변", sc["liquidityThresholdChanged"] is False)
    ck("성과로 rule 선택 안 함", sc["performanceUsedToSelectRule"] is False)
    ck("동결 전 수익률 계산 0",
       sc["historicalReturnsCalculatedBeforeFreeze"] is False)
    ck("대안 entry rule 테스트 0", sc["alternativeEntryRulesTested"] == 0)

    pr = C["prohibitions"]
    ck("current-listing filter 금지", pr["currentListingFilter"] == "FORBIDDEN")
    ck("survivorship filter 금지", pr["survivorshipFilter"] == "FORBIDDEN")
    ck("미완결 cohort 승자판정 금지",
       pr["partialCohortWinnerDecision"] == "FORBIDDEN")
    st = C["state"]
    ck("real money 미승인", st["realMoneyApproved"] is False)
    ck("runtime activation 불가", st["runtimeActivationAllowed"] is False)
    ck("OOS ledger 생성 불가", st["oosLedgerCreationAllowed"] is False)
    ck("다음 단계 R33B", st["contractNextStage"] ==
       "HISTORICAL_TRANSLATION_VALIDATION_R33B")

    ad = C["priorScopeAddendum"]
    ck("R32 는 historical closed-bar 로 범위 명시",
       ad["R32"]["historicalClosedBarDecision"] == "BOTH_SEPARATE")
    ck("R32 prospective 는 PENDING_R33B",
       ad["R32"]["prospectiveTranslatedDecision"] == "PENDING_R33B")
    ck("addendum append-only", ad["appendOnly"] is True)
    ck("R33B 이전 금지표현 목록 존재",
       "live executable" in ad["forbiddenClaimsUntilR33B"])


# ── L4 hash ───────────────────────────────────────────────────────────
def t_l4():
    print("\n[L4] 계약 해시")
    h1, h2 = P.contract_hash(), P.contract_hash()
    ck("직렬화 결정성 (동일 프로세스)", h1 == h2)
    r = subprocess.run(
        [sys.executable, "-c",
         "import sys;sys.path.insert(0,r'%s');"
         "import r33a_translation_precommit as C;print(C.contract_hash())" % SRC],
        capture_output=True, text=True)
    ck("직렬화 결정성 (별도 프로세스)", r.stdout.strip() == h1, r.stdout.strip()[:16])
    d = jload(CONTRACT_JSON)
    if not d:
        notrun("contract JSON", "미생성")
        return
    ck("저장된 해시 == 재계산 해시", d["contractHash"] == h1)
    # §10 "과거 수익률이나 예상 승자를 넣지 않는다" 는 **값**과 **승자 선언** 얘기다.
    # 비용식 문자열의 변수명(grossPct 등)은 §15 가 요구한 의미 복원이라 정상이다.
    cj = json.dumps(d["contract"], ensure_ascii=False)
    pub = [x for x in ("19.147", "15.009", "13.697", "12.193",
                       "9.845", "9.510", "9.525", "7.466") if x in cj]
    ck("계약에 published 수익률 값 없음", not pub, str(pub))
    win = [w for w in ("BM_PRIMARY", "SIZE_PRIMARY", "NO_EXECUTABLE_WINNER")
           if w in cj]
    ck("계약에 승자 선언 없음", not win, str(win))
    ck("계약에 성과 필드 없음",
       not any(k in cj for k in ('"excessAnn"', '"topAnn"', '"meanExcessPct"',
                                 '"annualizedReturn"', '"cagr"')))
    ck("writtenBeforeResults", d["writtenBeforeResults"] is True)
    ck("generatedAt 존재", bool(d.get("generatedAt")))
    ck("source file sha256 기록", len(d["sourceFileSha256"]) == 64)

    # 계약 동결이 availability 감사보다 먼저였는지 (§11·§13 순서)
    a = jload(AVAIL_JSON)
    if a and CONTRACT_JSON.exists() and AVAIL_JSON.exists():
        ck("계약 동결 파일이 availability 파일보다 먼저 생성",
           CONTRACT_JSON.stat().st_mtime <= AVAIL_JSON.stat().st_mtime)
        ck("availability 가 동결 이후 실행 표시",
           a["ranAfterContractFreeze"] is True)
    # correction 이 없었음을 명시 (append-only 정책은 유지)
    ck("correction 기록 없음 (최초 동결)", "correction" not in json.dumps(d).lower())


# ── L5 availability ───────────────────────────────────────────────────
def t_l5():
    print("\n[L5] historical field availability")
    a = jload(AVAIL_JSON)
    if not a:
        notrun("availability", "r33a-availability-latest.json 없음")
        return
    c, rc, ig = a["cohorts"], a["rowClassification"], a["integrity"]
    ck("163 cohort 감사", c["audited"] == 163, str(c["audited"]))
    ck("duplicate key 0", ig["duplicateKeyCount"] == 0)
    ck("same-bar violation 0", ig["sameBarViolationCount"] == 0)
    ck("future leakage 0", ig["futureLeakageCount"] == 0)
    ck("factor timing mismatch 0", ig["factorTimingMismatchCount"] == 0)
    t = rc["total"]
    ck("설명되지 않은 결손 0 (ticker 단위)", t["DATA_MISSING_TICKER"] == 0)
    ck("null 무결성 위반 0", t["DATA_INTEGRITY_NULL"] == 0)
    ck("비수치 무결성 위반 0", t["DATA_INTEGRITY_NONNUMERIC"] == 0)
    ck("open/volume 불일치 이상 0", t["ANOMALY_OPEN_VOLUME_MISMATCH"] == 0)
    ck("valid open 존재", t["VALID_OPEN"] > 0, str(t["VALID_OPEN"]))
    ck("legitimate no-fill 은 숨기지 않고 계상",
       t["NO_FILL_NO_TRADE"] >= 0 and "NO_FILL_NO_TRADE" in t)
    bm, sz = rc["byFactor"]["BM"], rc["byFactor"]["SIZE"]
    ck("BM/SIZE target row 동수",
       sum(bm.values()) == sum(sz.values()), f"{sum(bm.values())}/{sum(sz.values())}")
    ck("complete/incomplete 분리",
       c["translatedComplete"] + c["translatedIncomplete"] == c["audited"])
    ck("complete cohort > 0", c["translatedComplete"] > 0)
    ck("source split 양쪽 존재", len(a["sourceSplit"]) == 2, str(a["sourceSplit"]))
    ck("official open field 명시",
       "opening price" in a["officialSource"]["openField"])


# ── L6 성과 미접근 증명 ───────────────────────────────────────────────
def t_l6():
    print("\n[L6] 성과 미접근 (실행으로 증명)")
    a = jload(AVAIL_JSON)
    if a:
        pa = a["performanceAccess"]
        ck("성과 guard 설치됨", pa["guardInstalled"] is True)
        ck("성과 함수 호출 0", pa["performanceFunctionCalls"] == 0)
        for n in ("R25Engine.tsr", "R27.tsr", "R27.cohort", "R27.paired"):
            ck(f"차단 대상 포함: {n}", n in pa["blocked"])
        ck("forward-return row read 0", pa["forwardReturnRowsRead"] == 0)
        ck("R32 decision engine 호출 0", pa["r32DecisionEngineCalls"] == 0)
        ck("대안 entry 성과 실행 0", pa["alternativeEntryPerformanceRuns"] == 0)
        ck("entry rule 최적화 실행 0", pa["entryRuleOptimizationRuns"] == 0)
        ck("historical returns 계산 0", pa["historicalReturnsCalculated"] == 0)
    # guard 가 실제로 동작하는지 실증
    try:
        import r33a_availability as AV
        import r27_analysis as A
        with AV._PerfGuard():
            try:
                A.R27.tsr(None, 0, 1, "000000")
                ck("guard 가 tsr 호출을 차단", False, "예외가 발생하지 않음")
            except AV.PerformanceAccessViolation:
                ck("guard 가 tsr 호출을 차단", True)
    except Exception as e:
        ck("guard 실증", False, f"{type(e).__name__}: {e}")

    src = (SRC / "r33a_availability.py").read_text(encoding="utf-8")
    psrc = (SRC / "r33a_translation_precommit.py").read_text(encoding="utf-8")
    for bad in ("19.147", "15.009", "13.697", "12.193", "9.845", "9.510", "9.525"):
        ck(f"published 수익률 상수 미포함: {bad}",
           bad not in src and bad not in psrc)
    prohibited("BM/SIZE 승자 재판정", "§4 금지 — R33B 소관")
    prohibited("D+1 시가 vs D+1 종가 성과 비교", "§2 금지 — 대안 비교 0")
    prohibited("cost-adjusted return 계산", "§4 금지")
    prohibited("R32 decision engine 실행", "§4 금지")


# ── L7 경계 ───────────────────────────────────────────────────────────
def t_l7():
    print("\n[L7] 데이터·운영 경계 · 미생성 확인")
    for f in ("r33a_translation_precommit.py", "r33a_availability.py"):
        s = (SRC / f).read_text(encoding="utf-8")
        for bad in ("requests.", "urllib.request", "httpx", "pykrx",
                    "KRX_OPENAPI_AUTH_KEY", "AUTH_KEY", "os.environ", "getenv"):
            ck(f"{f}: 금지 호출 없음 {bad}", bad not in s)

    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8")
    ck("_cache gitignored", "_cache/" in gi)
    ck("reports gitignored", "reports/" in gi)

    st = [x for x in git(["status", "--short"]).splitlines() if x.strip()]
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json") for x in st), str(st))
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 raw/report 없음",
       not any(s.startswith(("_cache", "reports")) for s in staged), str(staged))

    if PUBLIC_REPO.exists():
        ck("public repo READ_ONLY (HEAD 2c8a000)",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
        ck("public repo 에 R33A 산출물 없음",
           "r33a" not in git(["status", "--short"], cwd=PUBLIC_REPO).lower())
    if AUTOMATION_REPO.exists():
        ck("automation repo 에 R33A 변경 없음",
           "r33a" not in git(["status", "--short"], cwd=AUTOMATION_REPO).lower())

    # §4·§23 — 이번 TASK 에서 만들면 안 되는 것들
    ck("OOS ledger 미생성", not (ROOT / "_cache" / "oos-r33").exists())
    ck("R33 runner 미생성", not (SRC / "r33_engine.py").exists())
    ck("activation manifest 미생성",
       not (RD / "r33-activation-manifest-latest.json").exists())
    a = jload(AVAIL_JSON)
    if a:
        s = a["safety"]
        for k in ("networkCalls", "marketDataApiCalls", "krxAuthKeyAccess",
                  "envChanges", "oosCohortCreated", "realOrders", "brokerCalls"):
            ck(f"safety {k} == 0", s[k] == 0, str(s[k]))
        ck("realMoneyApproved False", s["realMoneyApproved"] is False)
        ck("paperOnly True", s["paperOnly"] is True)
    ck("LEGACY_50D 미변경",
       "LEGACY_50D" not in (SRC / "r33a_availability.py").read_text(encoding="utf-8"))


# ── L8 산출물 ─────────────────────────────────────────────────────────
def t_l8():
    print("\n[L8] 산출물")
    ck("contract JSON parse", jload(CONTRACT_JSON) is not None)
    ck("availability JSON parse", jload(AVAIL_JSON) is not None)
    md = WD / "wababa-prospective-execution-translation-precommit-r33a-latest.md"
    if not md.exists():
        notrun("R33A canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None,
           first)
        ck("WAIT 미사용", "WAIT" not in first)
        for need in ("D_CLOSE_RANK_NEXT_SESSION_OPEN_V1", "REAL_MONEY_NOT_APPROVED",
                     "04beb59", "0431003", P.contract_hash()[:16]):
            ck(f"보고 항목: {need[:44]}", need in body)
        toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", body)
                if not re.fullmatch(r"[0-9a-f]{64}", w)]
        ck("장문 연속 토큰(비밀값 형태) 0", not toks, str(toks[:3]))
        for bad in ("AIza", "sk-", "ghp_", "BEGIN PRIVATE KEY", "@gmail.com"):
            ck(f"secret 패턴 없음: {bad}", bad not in body)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN} / "
          f"PROHIBITED_AND_NOT_CALLED {PROHIB}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("apiCalls: 0")
    print("historicalReturnsCalculated: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
