#!/usr/bin/env python3
"""R33B1 회귀 — plan 계보 · call 대조 · no-fill 계층 · 현금 · turnover/비용 · 결정.

WABABA-R33B-SUPPLEMENT-AND-NOFILL-PROVENANCE-AUDIT-R33B1

계층:
  L1 frozen    — 상위 동결 hash 전부 (contract/spec/plan/manifest/precommit)
  L2 lineage   — PLAN_V0/V1 · 집합연산 · 계획 밖 call 0
  L3 calls     — provider/market 산술 · 103 대조 · stale artifact 식별
  L4 nofill    — 6,680 분해 · level A/B/C 분모 분리
  L5 absent    — TICKER_ABSENT 8건 provenance · 선택 미해소 0
  L6 cash      — 현금비중 재현 · weight 항등식 · 재분배 0
  L7 cost      — target vs filled book · 비용 반영 · pre/post delta
  L8 decision  — 동결 정책 · POST_AUDIT_DECISION · enum
  L9 boundary  — network/API/credential 0 · raw Git 0 · public/automation 0

사용: python scripts/research/test_r33b1_provenance_audit.py
"""
from __future__ import annotations

import hashlib
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
import r33b_precommit as P                     # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")
AUTOMATION_REPO = Path(r"C:\work\ai-operating-system")

LIN = RD / "r33b1-plan-lineage-latest.json"
CAL = RD / "r33b1-call-reconciliation-latest.json"
NOF = RD / "r33b1-nofill-accounting-latest.json"
COS = RD / "r33b1-cost-reconciliation-latest.json"
DEC = RD / "r33b1-postaudit-decision-latest.json"

PASS = FAIL = NOTRUN = 0


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


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def jload(p):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def t_l1():
    print("\n[L1] 동결 hash")
    c = P.contract_valid()
    ck("R33A contract hash", c["match"], c["contractHash"][:16])
    ck("R33B spec hash", P.spec_hash() ==
       "fea25545698f79c00a5715165447e83b503620b35466aac369147d83b8b050a3")
    h = P.upstream_hashes()
    for n, pre in P.UPSTREAM.items():
        ck(f"{n} {pre}", h[n]["match"])
    st = jload(RD / "r31-stitch-latest.json")
    ck("R31 manifest 3439dec9", st["manifestSha256"].startswith("3439dec9"))
    pl = jload(RD / "r33b-missing-date-plan-latest.json")
    ck("corrected plan hash 0dfd9b44", pl["planHash"].startswith("0dfd9b44"))
    ck("initial plan hash ab291296",
       pl["correction"]["previousPlanHash"].startswith("ab291296"))
    for cm in ("5f8e7ba", "8f8ea81", "04beb59", "0431003"):
        ck(f"commit {cm} ancestor", subprocess.run(
            ["git", "merge-base", "--is-ancestor", cm, "origin/main"],
            cwd=ROOT, capture_output=True).returncode == 0)


def t_l2():
    print("\n[L2] plan 계보")
    d = jload(LIN)
    if not d:
        notrun("lineage", "artifact 없음")
        return
    v0, v1, so = d["PLAN_V0"], d["PLAN_V1"], d["setOps"]
    ck("V0 hash 를 R33A artifact 에서 재현", v0["hashReproducedFromR33AArtifact"])
    ck("V0 hash prefix ab291296", v0["prefixMatch"])
    ck("V0 distinct 50", v0["distinctDates"] == 50, str(v0["distinctDates"]))
    ck("V0 = entry 47 + exit 3 + both 0",
       v0["entryOnly"] == 47 and v0["exitOnly"] == 3 and v0["both"] == 0)
    ck("V0 status SUPERSEDED", v0["status"] == "SUPERSEDED")
    ck("V1 status ACTIVE", v1["status"] == "ACTIVE")
    ck("V1 parent = V0", v1["parentHash"] == v0["hash"])
    ck("V1 active 요구 60", v1["activeRequiredDistinctDates"] == 60,
       str(v1["activeRequiredDistinctDates"]))
    ck("V0_intersect_V1 = 48", so["V0_intersect_V1"] == 48, str(so["V0_intersect_V1"]))
    ck("V0_minus_V1 = 2", len(so["V0_minus_V1"]) == 2, str(so["V0_minus_V1"]))
    ck("V1_minus_V0 = 12", so["V1_minus_V0"] == 12, str(so["V1_minus_V0"]))
    ck("fetched distinct 62", so["fetchedDistinct"] == 62)
    ck("fetched_intersect_V1 = 60", so["fetched_intersect_V1"] == 60)
    ck("V1_minus_fetched = 0 (누락 없음)", so["V1_minus_fetched"] == 0)
    ck("fetched_minus_V1 = 2 (superseded 미사용)",
       len(so["fetched_minus_V1"]) == 2, str(so["fetched_minus_V1"]))
    ck("항등식 V0_union_V1 == fetched", so["identity_V0_union_V1_equals_fetched"])
    cl = d["classification"]
    ck("어떤 plan 에도 속하지 않는 call 0",
       cl["CALL_NOT_VALID_UNDER_ANY_PLAN"] == 0)
    ck("superseded plan call 2", cl["CALL_VALID_UNDER_SUPERSEDED_PLAN"] == 2)
    co = d["consumption"]
    ck("obsolete 날짜 entry 사용 0", co["obsoleteDatesUsedAsEntry"] == 0)
    ck("obsolete 날짜 exit 사용 0", co["obsoleteDatesUsedAsExit"] == 0)
    ck("obsolete 날짜 diagnostic 사용 0",
       co["obsoleteDatesUsedAsDiagnosticExit"] == 0)
    ck("primary 에 obsolete row 0", co["PRE_FIX_OBSOLETE_ROWS_USED_IN_PRIMARY"] == 0)
    ck("primary 에 diagnostic row 0", co["DIAGNOSTIC_ONLY_ROWS_USED_IN_PRIMARY"] == 0)
    ck("unknown provenance 0", co["UNKNOWN_PROVENANCE_ROWS"] == 0)


def t_l3():
    print("\n[L3] call 대조")
    d = jload(CAL)
    if not d:
        notrun("calls", "artifact 없음")
        return
    bp, tot = d["byProvider"], d["totals"]
    ck("KRX 41일", bp["KRX_OPENAPI"]["dates"] == 41, str(bp["KRX_OPENAPI"]["dates"]))
    ck("KRX 시장분리 ×2", bp["KRX_OPENAPI"]["marketsPerDate"] == 2)
    ck("KRX 82 calls", bp["KRX_OPENAPI"]["calls"] == 82)
    ck("FSC 21일", bp["PUBLIC_DATA_PORTAL_FSC"]["dates"] == 21)
    ck("FSC 시장통합 ×1", bp["PUBLIC_DATA_PORTAL_FSC"]["marketsPerDate"] == 1)
    ck("FSC 21 calls", bp["PUBLIC_DATA_PORTAL_FSC"]["calls"] == 21)
    ck("재구성 103", tot["reconstructedLogicalFetchCalls"] == 103)
    ck("pass 합계 103", tot["passSum"] == 103)
    ck("보고값 103 일치", tot["reportedInR33BReport"] == 103)
    ck("예산 250 이내", tot["reconstructedLogicalFetchCalls"] <= tot["budget"])
    for k in ("retries", "failures", "rateLimited", "duplicateCalls", "unplannedCalls"):
        ck(f"{k} 0", tot[k] == 0)
    ck("attempt == logical fetch (retry 0)", tot["attemptsEqualLogicalFetches"])
    pb = d["passBreakdown"]
    ck("PASS1 85", pb["PASS1_V0_plan"]["calls"] == 85)
    ck("PASS2 18", pb["PASS2_add_months_correction"]["calls"] == 18)
    sa = d["staleArtifact"]
    ck("stale manifest 식별", sa["storedValue"] == 85 and sa["correctValue"] == 103)
    ck("보고값은 정확했음 기록", sa["reportedValueWasCorrect"] is True)
    ck("ledger 재구성 근거 명시", "raw/ 미보존" in d["ledgerBasis"])


def t_l4():
    print("\n[L4] no-fill 계층")
    d = jload(NOF)
    if not d:
        notrun("nofill", "artifact 없음")
        return
    ld = d["levelDecomposition"]
    r = ld["reportedCounter_6680"]
    ck("6,680 은 포지션 수가 아님", r["notPositionCount"] is True)
    ck("6,680 = 6569 + 111", r["components"]["total"] == 6680
       and r["components"]["36M_full_universe_beforeFilterPath"]
       + r["components"]["60M_tradable"] == 6680)
    a = ld["LEVEL_A_supplementSourceUniverse"]
    ck("LEVEL_A 행 합계 정합",
       a["validOpen"] + a["noTrade"] + a["noOpeningPrice"] + a["invalid"] == a["rows"])
    b = ld["LEVEL_B_tradableAtEntry_163"]
    ck("LEVEL_B tradable ticker-absent 0", b["tickerAbsent"] == 0)
    c = ld["LEVEL_C_selectedPositions"]
    ck("LEVEL_C 합계 208",
       c["SIZE"] + c["BM"] + c["CONTROL"] == c["total"] == 208)
    ck("SIZE 선택 no-fill 48", c["SIZE"] == 48)
    ck("BM 선택 no-fill 27", c["BM"] == 27)
    ck("CONTROL 선택 no-fill 133", c["CONTROL"] == 133)
    ck("분모 혼동 정정 기록", d["r33bLabelDefect"]["cashWeightsWereCorrect"] is True)


def t_l5():
    print("\n[L5] TICKER_ABSENT provenance")
    d = jload(NOF)
    if not d:
        notrun("absent", "artifact 없음")
        return
    tp = d["tickerAbsentProvenance"]
    ck("총 8건", tp["total"] == 8)
    ck("선택 포지션 absence 0", tp["selectedPositionAbsences"] == 0)
    ck("UNRESOLVED 0", tp["classification"]["UNRESOLVED"] == 0)
    ck("DATA_INTEGRITY_MISSING 0", tp["classification"]["DATA_INTEGRITY_MISSING"] == 0)
    ck("8건 전부 상폐/종료 분류",
       tp["classification"]["DELISTED_OR_TERMINATED_BEFORE_ENTRY"] == 8)
    ck("현재 상장목록 미사용 명시", "현재 상장목록 미사용" in tp["evidenceRule"])
    ck("건별 표 8행", len(tp["perCase"]) == 8)
    ck("건별 전부 tradable set 밖",
       all(not x["inTradableSet"] for x in tp["perCase"]))


def t_l6():
    print("\n[L6] 현금비중")
    d = jload(NOF)
    if not d:
        notrun("cash", "artifact 없음")
        return
    cd, rep = d["levelCD"], d["reportedByR33B"]
    for arm, v in rep.items():
        got = cd[arm]["meanCohortCashWeightPct"]
        ck(f"{arm} 현금비중 재현 {v}%", abs(got - v) < 5e-5, f"{got:.6f}")
        ck(f"{arm} weight 항등식", cd[arm]["weightIdentityHolds"] is True)
        ck(f"{arm} pooled 비율 별도 제시",
           cd[arm]["pooledPositionNoFillPct"] is not None)
    ck("집계 정의 명시(cohort 단순평균)",
       "단순평균" in d["definition"]["meanCohortCashWeightPct"])
    ck("SIZE 선택 포지션 24,963", cd["SIZE"]["selectedTargetPositions"] == 24963)
    ck("BM 선택 포지션 24,963", cd["BM"]["selectedTargetPositions"] == 24963)
    ck("BM/SIZE target 동수",
       cd["SIZE"]["selectedTargetPositions"] == cd["BM"]["selectedTargetPositions"])


def t_l7():
    print("\n[L7] turnover / 비용")
    d = jload(COS)
    if not d:
        notrun("cost", "artifact 없음")
        return
    ck("계약 요구 명시", "실제 체결된 holdings" in d["contractRequirement"])
    ck("구현 결함 식별", "TARGET_BOOK_TURNOVER" in d["implementationFound"])
    ck("결함 분류", d["classification"] == "FILLED_BOOK_ACCOUNTING_BUG")
    ck("동결파일 미수정 명시", "동결파일 미수정" in d["fixScope"])
    tb = d["turnoverBooks"]
    for arm in ("SIZE", "BM"):
        t = tb[arm]
        ck(f"{arm} target/filled 분리 계산",
           t["targetBookAnnual"] is not None and t["filledBookAnnual"] is not None)
        ck(f"{arm} filled >= target (미체결이 회전 증가)",
           t["filledBookAnnual"] >= t["targetBookAnnual"])
    pp = d["prePost"]
    for arm in ("SIZE", "BM"):
        ck(f"{arm} gross 불변",
           pp[arm]["grossPct"]["pre"] == pp[arm]["grossPct"]["post"])
        ck(f"{arm} 0bp 불변",
           pp[arm]["net"]["0bp"]["pre"] == pp[arm]["net"]["0bp"]["post"])
        ck(f"{arm} turnover 증가",
           pp[arm]["annualTurnover"]["post"] >= pp[arm]["annualTurnover"]["pre"])
        ck(f"{arm} 100bp 순성과 하락(비용 증가)",
           pp[arm]["net"]["100bp"]["post"] <= pp[arm]["net"]["100bp"]["pre"])
    ck("no-fill 비용 0", d["noFillCost"] == 0)
    ck("cash 비용 0", d["cashCost"] == 0)
    ck("비중 재분배 0", d["weightRedistribution"] == 0)
    ck("fallback 횟수 기록", "fallbackDates" in d["fallback"])
    ck("materiality 기록", "결정 무영향" in d["materiality"])


def t_l8():
    print("\n[L8] 결정")
    import r32_decide as D
    d = jload(DEC)
    if not d:
        notrun("decision", "artifact 없음")
        return
    ck("PRE_AUDIT_DECISION BOTH_SEPARATE", d["preAuditDecision"] == "BOTH_SEPARATE")
    ck("POST_AUDIT_DECISION enum", d["postAuditDecision"] in D.DECISION_ENUM)
    ck("POST_AUDIT_DECISION BOTH_SEPARATE",
       d["postAuditDecision"] == "BOTH_SEPARATE", d["postAuditDecision"])
    live = hashlib.sha256((SRC / "r32_decide.py").read_bytes()).hexdigest()
    ck("결정정책 코드 무변경", d["decisionPolicySha256"] == live)
    ck("163 cohort 유지", d["cohorts"] == 163)
    ck("turnover basis FILLED_BOOK", d["turnoverBasis"] == "FILLED_BOOK")
    ck("결정 결정적 재현",
       D.decide(json.loads(json.dumps(d["decisionInputs"])))["decision"]
       == d["postAuditDecision"])


def t_l9():
    print("\n[L9] 경계")
    # 금지토큰 스캔 대상은 **실제 작업하는 모듈**이다. 이 테스트 파일 자신은
    # 그 토큰들을 '검사 목록'으로 담고 있어 자기 자신을 스캔하면 반드시 오탐한다.
    for f in ("r33b1_provenance_audit.py", "r33b1_run.py"):
        s = (SRC / f).read_text(encoding="utf-8")
        for bad in ("requests.", "urllib.request", "httpx", "pykrx",
                    "KRX_OPENAPI_AUTH_KEY", "AUTH_KEY", "DATA_GO_KR",
                    "os.environ", "getenv"):
            ck(f"{f}: {bad} 없음", bad not in s)
        toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", s)
                if not re.fullmatch(r"[0-9a-f]{64}", w)]
        ck(f"{f}: 장문 연속토큰 0", not toks, str(toks[:2]))
    # 테스트 파일은 '호출 형태'로만 검사한다(문자열 목록 보유는 정상).
    ts = (SRC / "test_r33b1_provenance_audit.py").read_text(encoding="utf-8")
    net_calls = re.findall(
        r"\b(requests\.(get|post)|urlopen|httpx\.(get|post))\s*\(", ts)
    ck("테스트 파일: 네트워크 호출 0", not net_calls, str(net_calls[:2]))
    env_calls = re.findall(r"\b(os\.environ\[|os\.getenv\s*\()", ts)
    ck("테스트 파일: env 접근 0", not env_calls, str(env_calls[:2]))
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 _cache/reports 없음",
       not any(s.startswith(("_cache", "reports")) for s in staged), str(staged))
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    if PUBLIC_REPO.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
        ck("public repo 에 r33b1 없음",
           "r33b1" not in git(["status", "--short"], cwd=PUBLIC_REPO).lower())
    if AUTOMATION_REPO.exists():
        ck("automation repo 에 r33b1 없음",
           "r33b1" not in git(["status", "--short"], cwd=AUTOMATION_REPO).lower())
    ck("R33 ledger 미생성", not (ROOT / "_cache" / "oos-r33").exists())
    ck("runtime hook/observer 미생성",
       not (SRC / "r33_engine.py").exists()
       and not (RD / "r33-activation-manifest-latest.json").exists())
    src = (SRC / "r33b1_provenance_audit.py").read_text(encoding="utf-8")
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    ck("실주문 호출 0",
       not re.findall(r"\b(place_order|submit_order|send_order)\s*\(", src))
    md = WD / "wababa-r33b-supplement-and-nofill-provenance-audit-r33b1-latest.md"
    if not md.exists():
        notrun("R33B1 canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None,
           first)
        for need in ("BOTH_SEPARATE", "REAL_MONEY_NOT_APPROVED", "5f8e7ba", "6,680"):
            ck(f"보고 항목: {need}", need in body)
        toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", body)
                if not re.fullmatch(r"[0-9a-f]{64}", w)]
        ck("보고서 장문 토큰 0", not toks, str(toks[:2]))
    for p in (LIN, CAL, NOF, COS, DEC):
        ck(f"JSON parse: {p.name}", jload(p) is not None)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8, t_l9):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("apiCalls: 0")
    print("credentialAccess: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
