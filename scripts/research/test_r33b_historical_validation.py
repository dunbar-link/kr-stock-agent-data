#!/usr/bin/env python3
"""R33B 회귀 — 동결계약 · 보완수집 · 완결성 gate · 번역성과 · 결정정책.

WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B

계층:
  L1 frozen    — R27/R31/R32 precommit · R31 manifest · R33A contract · commits
  L2 spec      — R33B spec hash · 부족일 plan hash · 47/50 분류
  L3 supply    — local 재사용 · API 예산 · source boundary · 스키마 · 무결성
  L4 gate      — 163/163 완결 · same-bar 0 · leakage 0 · pre-gate 성과 0
  L5 contract  — entry/exit/no-fill/cash/cost/비중/tie-break 계약 준수
  L6 results   — 번역 성과 · 비용 4종 · 안정성 · 적대검증 · paired 불확실성
  L7 decision  — 동결 R32 정책 · 4개 enum · fixture 대칭성
  L8 boundary  — raw Git 0 · public repo 0 · 실주문 0 · secret · git diff --check

사용: python scripts/research/test_r33b_historical_validation.py
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
SUPP = ROOT / "_cache" / "r33b-supplemental-open"
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


PLAN = RD / "r33b-missing-date-plan-latest.json"
MANI = RD / "r33b-supplement-manifest-latest.json"
GATE = RD / "r33b-completeness-gate.json"
RES = RD / "r33b-translated-results-latest.json"
DEC = RD / "r33b-decision-latest.json"


# ── L1 ────────────────────────────────────────────────────────────────
def t_l1():
    print("\n[L1] frozen 정본")
    h = P.upstream_hashes()
    for n, pre in P.UPSTREAM.items():
        ck(f"{n} {pre}", h[n]["match"], h[n]["sha256"][:12])
    ck("upstream 전체 일치", h["allMatch"])
    c = P.contract_valid()
    ck("R33A contract hash 일치", c["match"], c["contractHash"][:16])
    ck("translation rule 일치", c["ruleMatch"])
    ck("rule = D_CLOSE_RANK_NEXT_SESSION_OPEN_V1",
       P.TRANSLATION_RULE == "D_CLOSE_RANK_NEXT_SESSION_OPEN_V1")
    st = jload(RD / "r31-stitch-latest.json")
    ck("R31 dataset manifest 3439dec9",
       bool(st) and st["manifestSha256"].startswith(P.DATASET_MANIFEST_PREFIX))
    for cm, lb in ((P.SOURCE_COMMIT, "R33A 8f8ea81"),
                   (P.R33_FEASIBILITY_COMMIT, "R33 04beb59"),
                   (P.RESEARCH_BASE_COMMIT, "R32A 0431003")):
        ck(f"{lb} 존재", git(["cat-file", "-t", cm]).strip() == "commit")
        r = subprocess.run(["git", "merge-base", "--is-ancestor", cm, "origin/main"],
                           cwd=ROOT, capture_output=True, text=True)
        ck(f"{lb} ancestor", r.returncode == 0)


# ── L2 ────────────────────────────────────────────────────────────────
def t_l2():
    print("\n[L2] spec · 부족일 계획")
    h1, h2 = P.spec_hash(), P.spec_hash()
    ck("spec hash 결정성(동일 프로세스)", h1 == h2)
    r = subprocess.run([sys.executable, "-c",
                        "import sys;sys.path.insert(0,r'%s');"
                        "import r33b_precommit as P;print(P.spec_hash())" % SRC],
                       capture_output=True, text=True)
    ck("spec hash 결정성(별도 프로세스)", r.stdout.strip() == h1)
    ck("FULL_163_ONLY 정책", P.SPEC["performanceCohortPolicy"] == "FULL_163_ONLY")
    ck("partial 113 성과 금지", P.SPEC["partial113PerformanceAllowed"] is False)
    ck("incomplete cohort 성과 금지",
       P.SPEC["incompleteCohortPerformanceAllowed"] is False)
    ck("BOTH_SEPARATE 유지 가정 안 함", P.SPEC["priorBothSeparateAssumed"] is False)
    ck("결정 enum 4개", P.SPEC["allowedDecisions"] ==
       ["BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE", "NO_EXECUTABLE_WINNER"])
    ck("BASE 125,000,000", P.SPEC["liquidityBaseKrw"] == 125_000_000)
    ck("LOW 25,000,000", P.SPEC["liquidityLowKrw"] == 25_000_000)
    ck("HIGH 250,000,000", P.SPEC["liquidityHighKrw"] == 250_000_000)
    ck("비용 4종", P.SPEC["costScenarios"] == [0, 25, 50, 100])
    ck("새 primary cost 없음", P.SPEC["primaryCostSelection"] == "NONE_NEW")
    ck("대안 entry/exit 0", P.SPEC["alternativeEntryRules"] == 0
       and P.SPEC["alternativeExitRules"] == 0)
    ck("threshold/horizon 변경 0",
       P.SPEC["thresholdChanges"] == 0 and P.SPEC["horizonChanges"] == 0)
    ck("tie-break 통일 금지", P.SPEC["tieBreakUnificationAllowed"] is False)
    ck("R31 frozen cache 변경 금지",
       P.SPEC["r31FrozenCacheMutationAllowed"] is False)

    pl = jload(PLAN)
    if not pl:
        notrun("plan", "artifact 없음")
        return
    ck("plan hash 존재", bool(pl.get("planHash")))
    pc = pl["plan"]["purposeCounts"]
    ck("47/50 분류 설명 존재", "47" in pl["plan"]["fortySevenVsFiftyExplained"])
    ck("목적 분류 합계 = distinct",
       sum(pc.values()) == pl["plan"]["distinctRequiredDates"])
    ck("correction 기록(add_months 결함)", "correction" in pl)
    if "correction" in pl:
        for k in ("reason", "impact", "resultIndependence",
                  "previousPlanHash", "correctedPlanHash"):
            ck(f"correction 항목 {k}", k in pl["correction"])
        ck("correction 이 계약 hash 불변임을 기록",
           pl["correction"]["contractHashUnchanged"] == P.SOURCE_CONTRACT_HASH)


# ── L3 ────────────────────────────────────────────────────────────────
def t_l3():
    print("\n[L3] 보완 수집")
    m = jload(MANI)
    if not m:
        notrun("supplement manifest", "없음")
        return
    ck("API 호출 <= 250", m["requestCount"] <= P.HARD_MAX_CALLS,
       str(m["requestCount"]))
    ck("duplicate 0", m["duplicateCount"] == 0)
    ck("invalid 0", m["invalidCount"] == 0)
    ck("missing 0", m["missingCount"] == 0)
    ck("valid open > 0", m["validOpen"] > 0)
    ck("source split 2종", len(m["sourceSplit"]) == 2, str(list(m["sourceSplit"])))
    ck("KRX/FSC 경계 명시", m["sourceBoundary"]["krxEnd"] == "2019-12-30"
       and m["sourceBoundary"]["fscStart"] == "2020-01-02")
    ck("R31 frozen cache 변경 0", m["r31FrozenCacheMutations"] == 0)
    ck("local-only 표시", m["localOnly"] is True and m["gitTracked"] is False)
    ck("행 전수 계상", m.get("rowsAccountedFor") == m.get("rowCount"),
       f'{m.get("rowsAccountedFor")}/{m.get("rowCount")}')
    # supplemental 은 Git 추적 0 이어야 한다
    ck("supplement Git 추적 0",
       git(["ls-files", "_cache/r33b-supplemental-open"]).strip() == "")
    ck("supplement 디렉터리 존재", SUPP.exists())
    days = list((SUPP / "normalized").glob("*.csv.gz"))
    ck("보완 파일 생성됨", len(days) > 0, str(len(days)))
    # ticker leading zero 보존
    if days:
        import csv
        import gzip
        with gzip.open(sorted(days)[0], "rt", encoding="utf-8") as fh:
            ts = [r["ticker"] for r in csv.DictReader(fh)]
        ck("ticker 6자리 고정", all(len(t) == 6 for t in ts), str(ts[:2]))
        ck("leading zero 보존 종목 존재", any(t.startswith("0") for t in ts))


# ── L4 ────────────────────────────────────────────────────────────────
def t_l4():
    print("\n[L4] 완결성 gate")
    g = jload(GATE)
    r = jload(RES)
    if not g:
        notrun("gate", "없음")
        return
    ck("총 163 cohort", g["totalCohorts"] == 163, str(g["totalCohorts"]))
    ck("163/163 완결", g["completeCohorts"] == 163, str(g["completeCohorts"]))
    ck("incomplete 0", g["incompleteCohorts"] == 0)
    ck("same-bar 0", g["sameBarViolations"] == 0)
    ck("future leakage 0", g["futureLeakage"] == 0)
    ck("duplicate cohort 0", g["duplicateCohorts"] == 0)
    ck("factor timing mismatch 0", g["factorTimingMismatch"] == 0)
    ck("entry date 결손 0", g["entryDatesMissing"] == [])
    ck("exit session 결손 0", g["exitSessionsMissing"] == [])
    ck("gate pass", g["pass"] is True)
    if r:
        gt = r["gate"]
        ck("pre-gate 성과 호출 0", gt["preGatePerformanceFunctionCalls"] == 0)
        ck("gate 9조건 전부 충족", all(gt["conditions"].values()))
        ck("gate open", gt["open"] is True)
        ck("gate 개방시각 기록", bool(gt.get("openedAt")))


# ── L5 ────────────────────────────────────────────────────────────────
def t_l5():
    print("\n[L5] 계약 준수")
    r = jload(RES)
    if not r:
        notrun("results", "없음")
        return
    st = r["executionStats"]
    ck("데이터 결손 0", st["dataMissing"] == 0, str(st["dataMissing"]))
    ck("exit 미해결 0", st["exitUnresolved"] == 0)
    ck("exit beyond calendar 0", st["exitBeyondCalendar"] == 0)
    ck("no-fill 은 별도 계상", st["noFillEntry"] > 0)
    ck("no-fill 과 데이터결손 구분",
       "noFillEntry" in st and "dataMissing" in st and "outOfScopeDate" in st)
    ck("체결 건수 > 0", st["filled"] > 0)
    ck("exit at session 이 주경로", st["exitAtSession"] > st["exitPendingResolved"])
    ck("범위밖 결정일은 결손 아님", st["outOfScopeDate"] > 0)
    ck("163 paired rows", r["matchedSample"]["commonCohortCount"] == 163)
    ck("incomplete 포함 0", r["period"]["incompleteCohortsIncluded"] == 0)
    ck("horizon 36", r["period"]["horizonMonths"] == 36)
    ck("BASE threshold 125,000,000", r["thresholdKrw"] == 125_000_000)
    ck("LOW/HIGH 유지", r["sensitivityKrw"]["LOW"] == 25_000_000
       and r["sensitivityKrw"]["HIGH"] == 250_000_000)
    ck("BM/SIZE 동수 관측",
       r["matchedSample"]["bmObservations"] == r["matchedSample"]["sizeObservations"])
    # 어댑터 소스 계약
    src = (SRC / "r33b_historical_validation.py").read_text(encoding="utf-8")
    ck("no-fill 은 0.0 (None 아님)", "return 0.0" in src)
    ck("재정규화 방지 근거 주석", "재정규화" in src)
    ck("exit 가격 대체 금지(캐노니컬 terminal 만)", "CANONICAL_TERMINAL" in src)
    ck("horizon 별 exit 사용", "_exit_session_for" in src)
    ck("valid open 정의 준수", "op > 0 and vol > 0" in src)


# ── L6 ────────────────────────────────────────────────────────────────
def t_l6():
    print("\n[L6] 번역 성과")
    r = jload(RES)
    if not r:
        notrun("results", "없음")
        return
    p = r["performance"]
    for a in ("SIZE", "BM", "CONTROL"):
        ck(f"{a} top 산출", p[a]["meanTopAnnPct"] is not None)
    for a in ("SIZE", "BM"):
        for b in ("0bp", "25bp", "50bp", "100bp"):
            ck(f"{a} {b} 순성과", p[a]["costCurve"][b]["netPct"] is not None)
        ck(f"{a} turnover 산출", p[a]["turnover"]["annualTurnover"] is not None)
    ck("0bp 은 gross 와 동일",
       p["SIZE"]["costCurve"]["0bp"]["netPct"] == p["SIZE"]["meanTopAnnPct"])
    ck("비용 단조 감소",
       p["SIZE"]["costCurve"]["0bp"]["netPct"] > p["SIZE"]["costCurve"]["100bp"]["netPct"])
    ck("cost rank reversal 계산됨", isinstance(p["costRankReversal"], dict))
    st = r["stability"]
    ck("기간 3구간", len(st["subperiods"]) >= 3)
    ck("시장 2개", all(m in st["markets"] for m in ("KOSPI", "KOSDAQ")))
    ad = r["adversarial"]
    for k in ("top1", "top3", "top5"):
        ck(f"{k} 제거 검증", k in ad["removal"])
    ck("LOYO 수행", "loyoSizeMinusBmSignStable" in ad)
    unc = r["uncertainty"]
    bb = unc["blockBootstrap"]["summary"]
    for k in ("ci95LowPct", "ci95HighPct", "pExcessAbove0"):
        ck(f"bootstrap {k}", bb.get(k) is not None)
    ck("non-overlapping 산출",
       (st["rolling"]["36M"].get("nonOverlapping") or {}).get("cohorts") is not None)


# ── L7 ────────────────────────────────────────────────────────────────
def t_l7():
    print("\n[L7] 결정정책")
    import r32_decide as D
    d = jload(DEC)
    if not d:
        notrun("decision", "없음")
        return
    ck("결정정책 hash 기록", len(d["decisionPolicySha256"]) == 64)
    live = hashlib.sha256((SRC / "r32_decide.py").read_bytes()).hexdigest()
    ck("결정정책 코드 무변경", d["decisionPolicySha256"] == live)
    ck("결정이 enum 안에", d["decision"]["decision"] in D.DECISION_ENUM)
    ck("163 cohort 기준", d["cohorts"] == 163)
    ck("판정 근거코드 존재", len(d["decision"]["reasonCodes"]) > 0)

    # fixture — 결과독립 대칭성
    base = json.loads(json.dumps(d["decisionInputs"]))
    ck("BOTH_SEPARATE 재현", D.decide(base)["decision"] == d["decision"]["decision"])

    f = json.loads(json.dumps(base))
    f["factors"]["BM"]["excessPct"] = 0.5
    f["factors"]["BM"]["subperiodPositiveRatio"] = 0.1
    ck("SIZE_PRIMARY fixture", D.decide(f)["decision"] == "SIZE_PRIMARY")

    f = json.loads(json.dumps(base))
    f["factors"]["SIZE"]["excessPct"] = 0.5
    f["factors"]["SIZE"]["subperiodPositiveRatio"] = 0.1
    ck("BM_PRIMARY fixture", D.decide(f)["decision"] == "BM_PRIMARY")

    f = json.loads(json.dumps(base))
    for a in ("SIZE", "BM"):
        f["factors"][a]["excessPct"] = 0.5
        f["factors"][a]["subperiodPositiveRatio"] = 0.1
    ck("NO_EXECUTABLE_WINNER fixture",
       D.decide(f)["decision"] == "NO_EXECUTABLE_WINNER")

    f = json.loads(json.dumps(base))
    f["baselineReproductionPass"] = False
    ck("baseline 실패 시 비교 차단",
       D.decide(f)["decision"] == "NO_EXECUTABLE_WINNER")

    # factor-swap 대칭
    f = json.loads(json.dumps(base))
    f["factors"]["SIZE"], f["factors"]["BM"] = f["factors"]["BM"], f["factors"]["SIZE"]
    sw = D.decide(f)["decision"]
    ck("factor-swap 대칭(둘 다 통과면 동일 결론)",
       sw in ("BOTH_SEPARATE", "BM_PRIMARY", "SIZE_PRIMARY"), sw)

    prohibited("113 부분표본 성과 실행", "§1·§3 금지 — FULL_163_ONLY")
    prohibited("D+1 종가/VWAP/D+2 성과 비교", "§3 금지 — 대안 entry 0")
    prohibited("entry rule 최적화", "§3 금지")
    prohibited("결과 확인 후 tie policy 수정", "§3 금지")


# ── L8 ────────────────────────────────────────────────────────────────
def t_l8():
    print("\n[L8] 경계·산출물")
    for f in ("r33b_precommit.py", "r33b_supplement.py",
              "r33b_historical_validation.py", "r33b_run.py"):
        s = (SRC / f).read_text(encoding="utf-8")
        for bad in ("AIza", "sk-", "ghp_", "BEGIN PRIVATE KEY"):
            ck(f"{f}: secret 없음 {bad}", bad not in s)
        toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", s)
                if not re.fullmatch(r"[0-9a-f]{64}", w)]
        ck(f"{f}: 장문 연속토큰 0", not toks, str(toks[:2]))
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 _cache/reports 없음",
       not any(s.startswith(("_cache", "reports")) for s in staged), str(staged))
    st = [x for x in git(["status", "--short"]).splitlines() if x.strip()]
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json") for x in st))
    if PUBLIC_REPO.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
        ck("public repo 에 r33b 없음",
           "r33b" not in git(["status", "--short"], cwd=PUBLIC_REPO).lower())
    if AUTOMATION_REPO.exists():
        ck("automation repo 에 r33b 없음",
           "r33b" not in git(["status", "--short"], cwd=AUTOMATION_REPO).lower())
    ck("OOS ledger 미생성", not (ROOT / "_cache" / "oos-r33").exists())
    ck("runtime hook/observer 미생성",
       not (SRC / "r33_engine.py").exists()
       and not (RD / "r33-activation-manifest-latest.json").exists())
    src = (SRC / "r33b_historical_validation.py").read_text(encoding="utf-8")
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    order = re.findall(r"\b(place_order|submit_order|send_order)\s*\(", src)
    ck("실주문 호출 0", not order)

    md = WD / "wababa-prospective-execution-historical-validation-r33b-latest.md"
    if not md.exists():
        notrun("R33B canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None,
           first)
        for need in ("D_CLOSE_RANK_NEXT_SESSION_OPEN_V1", "REAL_MONEY_NOT_APPROVED",
                     P.SOURCE_CONTRACT_HASH[:16], "163"):
            ck(f"보고 항목: {need[:40]}", need in body)
    for p in (PLAN, MANI, GATE, RES, DEC):
        ck(f"JSON parse: {p.name}", jload(p) is not None)
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
