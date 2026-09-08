#!/usr/bin/env python3
"""R32A 회귀 — 경계 감사 · row-level 재현 · 결정 재실행. 네트워크 0 · API 0.

WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A

계층:
  L1 frozen    — R27/R31/R32 precommit + dataset manifest 해시
  L2 source    — source 별 min/max date 분리 (liquidity vs 가격 vs 캘린더)
  L3 boundary  — 163 cohort 전수 · 미완결 0 · 경계초과 0 · leakage 0
  L4 repro     — 보고서 입력 의존 0 · row-level 재계산 · 불일치 처분
  L5 reconcile — R31 ↔ R32 표본 차이 설명
  L6 rerun     — 동일 precommit R32 재실행 · 결정 enum · fixture
  L7 boundary2 — raw Git 제외 · secret 0 · public repo · LEGACY_50D
  L8 outputs   — 산출물 parse · 판정 헤더 · git diff --check

사용: python scripts/research/test_r32a_audit.py
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r32_decide as D                       # noqa: E402
import r32a_audit as AU                      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")

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


def art(n):
    p = RD / n
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except ValueError:
        return None


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=str(cwd), capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


A = art("r32a-audit-latest.json")
COH = art("r32a-cohort-table-latest.json")
H2H = art("r32-headtohead-latest.json")
DEC = art("r32-decision-latest.json")


# ══════════════ L1 frozen ══════════════
def t_l1():
    print("\n[L1] frozen 해시")
    exp = {"r27_precommit": "acee4288", "r31_precommit": "d7fe3929",
           "r32_precommit": "07edbe72"}
    for n, p in exp.items():
        h = hashlib.sha256((SRC / f"{n}.py").read_bytes()).hexdigest()
        ck(f"{n} hash {p}", h.startswith(p), h[:12])
    st = art("r31-stitch-latest.json") or {}
    ck("dataset manifest 3439dec9",
       str(st.get("manifestSha256", "")).startswith("3439dec9"))
    if A:
        ck("감사가 frozen 해시 일치를 기록", A["frozenHashesOk"] is True)


# ══════════════ L2 source inventory ══════════════
def t_l2():
    print("\n[L2] source 별 date 경계 분리")
    if not A:
        notrun("source inventory", "감사 미실행")
        return
    inv = {s["source"]: s for s in A["inputSourceInventory"]["sources"]}
    smd = A["cohortBoundaryAudit"]["sourceMaxDates"]
    ck("liquidity(official) max 2026-07-31",
       inv["official-liquidity"]["maxDate"] == "2026-07-31")
    ck("liquidity(krx 투영) max 2026-07-31",
       inv["krx-liquidity"]["maxDate"] == "2026-07-31")
    ck("PIT 가격 max 2026-08-03", inv["pit-snapshots"]["maxDate"] == "2026-08-03")
    ck("거래일 캘린더 max 2026-08-03",
       inv["_trading-calendar.json"]["maxDate"] == "2026-08-03")
    ck("결정일 max 2026-08-03",
       inv["capital series (r16_canonical)"]["maxDate"] == "2026-08-03")
    ck("source 종류 6개 이상 분리 기록", len(inv) >= 6, str(len(inv)))
    ck("liquidity != 가격 source 종료일 (축이 다름)",
       smd["liquidity"] != smd["pitPrice"])
    ck("R31 manifest 는 liquidity 만 기술한다고 명시",
       "liquidity" in A["inputSourceInventory"]["r31ManifestDescribes"])
    ck("모든 source local-only · Git 미추적",
       all(s["localOnly"] and not s["gitTracked"]
           for s in A["inputSourceInventory"]["sources"]))


# ══════════════ L3 cohort 경계 ══════════════
def t_l3():
    print("\n[L3] cohort 경계 전수")
    if not A or not COH:
        notrun("cohort audit", "감사 미실행")
        return
    ag = A["cohortBoundaryAudit"]["aggregate"]
    ck("163 cohort 전수 감사", ag["audited"] == 163, str(ag["audited"]))
    ck("cohort table 행수 == 감사 수", len(COH["cohorts"]) == ag["audited"])
    ck("incomplete cohort 0", ag["incomplete"] == 0)
    ck("post-boundary cohort 0", ag["postBoundaryCohorts"] == 0)
    ck("post-boundary price row 0", ag["postBoundaryPriceRows"] == 0)
    ck("future leakage 0", ag["futureLeakage"] == 0)
    ck("liquidity as-of >= signal 인 cohort 0", ag["liquidityAfterSignal"] == 0)
    ck("BM/SIZE date mismatch 0", ag["factorDateMismatch"] == 0)
    ck("label vs actual mismatch 0", ag["labelActualMismatch"] == 0)

    rows = COH["cohorts"]
    sig = [r["signal_date"] for r in rows]
    ck("signal date 단조증가", sig == sorted(sig))
    ck("signal date 중복 0", len(sig) == len(set(sig)))
    ck("모든 cohort complete_flag True", all(r["complete_flag"] for r in rows))
    ck("모든 liquidity as-of < signal date",
       all(r["liquidity_as_of_date"] < r["signal_date"] for r in rows))
    ck("모든 exit date <= PIT 가격 max",
       all(r["actual_exit_price_date"] <= r["price_source_max_date"] for r in rows))
    ck("label == actual exit date (전 cohort)",
       all(r["cohort_label_end_date"] == r["actual_exit_price_date"] for r in rows))
    ck("BM/SIZE 같은 날짜 (전 cohort)", all(r["factor_same_dates"] for r in rows))
    last = A["cohortBoundaryAudit"]["lastCohortTrace"]
    ck("마지막 cohort signal 2023-08-01", last["signal_date"] == "2023-08-01")
    ck("마지막 cohort exit 2026-08-03", last["actual_exit_price_date"] == "2026-08-03")
    ck("마지막 cohort 가격 커버리지 > 0", last["price_covered_count"] > 0,
       str(last["price_covered_count"]))
    ck("raw row 미공개", COH["rawRowsDisclosed"] == 0)
    # 날짜 용어 복원
    ck("날짜 용어 12종 복원", len(A["dateSemantics"]) >= 12)
    ck("exit fallback 이 과거 방향임을 기록",
       "과거 방향" in A["dateSemantics"]["actual_exit_price_date"])


# ══════════════ L4 재현 ══════════════
def t_l4():
    print("\n[L4] row-level 재현")
    if not A:
        notrun("baseline repro", "감사 미실행")
        return
    rec, cmp_ = A["baselineRecomputation"], A["baselineComparison"]
    ck("canonical evidence 접근 차단 활성", rec["evidenceAccessBlocked"] is True)
    ck("재계산 중 보고서 접근 0건", len(rec["evidenceAccessHits"]) == 0,
       str(rec["evidenceAccessHits"])[:120])
    ck("published 값은 계산 입력 아님",
       A["baselineDisposition"]["publishedValueUsedAsInput"] is False)
    ck("tolerance 확대 없음",
       A["baselineDisposition"]["toleranceWidened"] is False
       and cmp_["tolerance"] == 5e-4)
    for k in ("R25 raw 36M SIZE", "R25 raw 36M BM", "R25 raw 36M CONTROL",
              "R31 BASE 36M SIZE", "R31 BASE 36M BM", "R31 BASE 36M CONTROL",
              "R31 100bp SIZE"):
        ck(f"재현: {k}", cmp_["checks"].get(k) is True)
    # 유일한 불일치가 설명됐는가
    disp = A["baselineDisposition"]
    ck("실패 체크는 1건 이하", len(disp["failedChecks"]) <= 1,
       str(disp["failedChecks"]))
    if disp["failedChecks"]:
        ck("불일치가 전부 설명됨", disp["allExplained"] is True)
        it = disp["items"][0]
        ck("불일치 분류 = 순서의존 동점", it["classification"]
           == "EXPLAINED_ORDER_DEPENDENT_TIE_BREAK")
        ck("동점 근거 존재(경계 동점 > 0)", it["boundaryTieDates"] > 0,
           str(it["boundaryTieDates"]))
        ck("불일치 크기 0.02%p 미만", abs(it["diffPct"]) < 0.02, str(it["diffPct"]))
    tf = A["turnoverForensic"]
    ck("SIZE 는 경계 동점 0 (그래서 정확 재현)",
       tf["SIZE"]["boundaryTies"]["datesWithBoundaryTie"] == 0)
    ck("BM 은 경계 동점 존재 (그래서 순서 의존)",
       tf["BM"]["boundaryTies"]["datesWithBoundaryTie"] > 0)
    ck("결정적 정렬키는 안정적", tf["SIZE"]["deterministicStable"]
       and tf["BM"]["deterministicStable"])
    ck("결정적 정렬키가 r32_engine 에 반영",
       "(-kv[1], kv[0])" in (SRC / "r32_engine.py").read_text(encoding="utf-8"))


# ══════════════ L5 reconciliation ══════════════
def t_l5():
    print("\n[L5] R31 ↔ R32 reconciliation")
    if not A:
        notrun("reconcile", "감사 미실행")
        return
    rc = A["r31r32Reconciliation"]
    ck("차이 설명됨", rc["explained"] is True)
    ck("R32-only cohort 0", rc["r32OnlyCohorts"] == 0)
    ck("R31-only cohort == 2007~2009 제외분",
       rc["r31OnlyCohorts"] == len(rc["r31OnlyDates"])
       and all(d < "2010-01-01" for d in rc["r31OnlyDates"]))
    ck("common == R32 cohort 수", rc["commonCohorts"] == rc["r32Scope"]["cohorts"])
    ck("R31 + R31only == R32 (표본 관계 정합)",
       rc["r31Scope"]["cohorts"] == rc["commonCohorts"] + rc["r31OnlyCohorts"])
    ck("arm 별 delta 기록", set(rc["byArm"]) == {"SIZE", "BM", "CONTROL"})


# ══════════════ L6 재실행 · 결정 ══════════════
def t_l6():
    print("\n[L6] R32 재실행 · 결정")
    if not H2H or not DEC:
        notrun("R32 rerun", "산출물 없음")
        return
    ck("cohort 163 유지", H2H["matchedSample"]["commonCohortCount"] == 163)
    ck("BM-only/SIZE-only 0",
       not H2H["matchedSample"]["bmOnly"] and not H2H["matchedSample"]["sizeOnly"])
    ck("horizon 36M", H2H["period"]["horizonMonths"] == 36)
    ck("threshold 125,000,000 유지", H2H["thresholdKrw"] == 125_000_000)
    ck("LOW/HIGH 불변", H2H["sensitivityKrw"] == {"LOW": 25_000_000,
                                                  "BASE": 125_000_000,
                                                  "HIGH": 250_000_000})
    ck("cost stress 4구간", set(H2H["performance"]["SIZE"]["costCurve"])
       == {"0bp", "25bp", "50bp", "100bp"})
    ck("비용 순위반전 0", not any(H2H["performance"]["costRankReversal"].values()))
    enum = {"BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE", "NO_EXECUTABLE_WINNER"}
    ck("결정 enum 유효", DEC["FINAL_CANDIDATE_DECISION"] in enum)
    ck("결정 = BOTH_SEPARATE (재실행 동일)",
       DEC["FINAL_CANDIDATE_DECISION"] == "BOTH_SEPARATE",
       DEC["FINAL_CANDIDATE_DECISION"])
    ck("결과 독립 로직 선언", DEC["resultIndependentLogic"] is True)
    ck("등급 STRONG/PROMISING 기록",
       DEC["grades"] == {"SIZE": "STRONG", "BM": "PROMISING"}, str(DEC.get("grades")))

    # fixture — 결정 로직이 결과와 무관한지
    def f(ok, ex=8.0):
        return ({"excessPct": ex, "retentionRatio": 0.8, "includedSpreadPositive": True,
                 "subperiodPositiveRatio": 1.0, "rollingPositiveRatio": 1.0,
                 "bothExchangesSameSign": True, "survivesTop3Removal": True,
                 "nontradableAlpha": False, "net100bpPct": ex, "grossPct": ex,
                 "attritionRate": 0.5, "thresholdHeadroom": 1.2} if ok else
                {"excessPct": 0.5, "retentionRatio": 0.1, "includedSpreadPositive": False,
                 "subperiodPositiveRatio": 0.0, "rollingPositiveRatio": 0.0,
                 "bothExchangesSameSign": False, "survivesTop3Removal": False,
                 "nontradableAlpha": True, "net100bpPct": 0.5, "grossPct": 0.5,
                 "attritionRate": 0.9, "thresholdHeadroom": 0.5})

    def pr(lead, solo=True, rev=False):
        return {"leader": lead, "ciExcludesZero": solo,
                "ciDirection": lead if solo else None, "rankReversalAt100bp": rev,
                "stabilityChecksPass": solo, "fragilityPass": True}

    base = {"baselineReproductionPass": True}
    ck("fixture SIZE_PRIMARY", D.decide({**base, "factors": {"SIZE": f(True), "BM": f(False)},
                                         "paired": pr("SIZE")})["decision"] == "SIZE_PRIMARY")
    ck("fixture BM_PRIMARY", D.decide({**base, "factors": {"SIZE": f(False), "BM": f(True)},
                                       "paired": pr("BM")})["decision"] == "BM_PRIMARY")
    ck("fixture BOTH_SEPARATE",
       D.decide({**base, "factors": {"SIZE": f(True, 9.0), "BM": f(True, 8.0)},
                 "paired": pr("SIZE", solo=False)})["decision"] == "BOTH_SEPARATE")
    ck("fixture NO_EXECUTABLE_WINNER",
       D.decide({**base, "factors": {"SIZE": f(False), "BM": f(False)},
                 "paired": pr(None, solo=False)})["decision"] == "NO_EXECUTABLE_WINNER")
    ck("factor-swap 대칭",
       D.decide({**base, "factors": {"SIZE": f(True, 8.0), "BM": f(True, 9.0)},
                 "paired": pr("BM")})["decision"] == "BM_PRIMARY")
    # STRONG/PROMISING 이 모두 executable family 인가
    strong = f(True, 9.0)
    promising = {**f(True, 3.0), "retentionRatio": 0.1,
                 "rollingPositiveRatio": 0.0, "bothExchangesSameSign": False}
    g_s, m_s, _ = D.execution_gate(strong)
    g_p, m_p, _ = D.execution_gate(promising)
    ck("STRONG executable", g_s and m_s["_grade"] == "STRONG")
    ck("PROMISING executable (숨은 hard-fail 없음)",
       g_p and m_p["_grade"] == "PROMISING", m_p["_grade"])
    ck("PROMISING 을 실패시키는 hard-code 없음",
       "PROMISING" in (SRC / "r32_decide.py").read_text(encoding="utf-8")
       and g_p is True)
    ck("baseline 실패 시 비교 전 차단",
       D.decide({"baselineReproductionPass": False,
                 "factors": {"SIZE": f(True), "BM": f(False)},
                 "paired": pr("SIZE")}).get("blockedBeforeComparison") is True)


# ══════════════ L7 경계 ══════════════
def t_l7():
    print("\n[L7] 데이터·저장소 경계")
    tracked = git(["ls-files"]).splitlines()
    ck("_cache Git 추적 0", not [f for f in tracked if f.startswith("_cache/")])
    ck("reports Git 추적 0", not [f for f in tracked if f.startswith("reports/")])
    src = (SRC / "r32a_audit.py").read_text(encoding="utf-8")
    ck("감사 코드에 네트워크 호출 0",
       not re.search(r"(?m)^\s*(import|from)\s+(requests|urllib|httpx|pykrx)", src))
    ck("감사 코드에 인증키 접근 0",
       "KRX_OPENAPI_AUTH_KEY" not in src and "auth_key" not in src)
    ck("감사 코드에 os.environ 접근 0", "os.environ" not in src)
    if A:
        ck("감사가 network/API 0 기록",
           A["networkCalls"] == 0 and A["apiCalls"] == 0
           and A["credentialAccessed"] == 0)
    if PUBLIC_REPO.exists():
        st = git(["status", "--porcelain"], PUBLIC_REPO)
        ck("public repo 에 R32A 변경 0",
           not [ln for ln in st.splitlines() if re.search(r"r32", ln, re.I)])
    else:
        notrun("public repo", "경로 없음")
    ck("known dirty 미스테이지",
       "financial-universe-real.json" not in git(["diff", "--cached", "--name-only"]))
    ck("LEGACY_50D 변경 0",
       not re.search(r"legacy.?50", git(["diff", "--name-only", "HEAD"]), re.I))


# ══════════════ L8 산출물 ══════════════
def t_l8():
    print("\n[L8] 산출물")
    for n in ("r32a-audit-latest.json", "r32a-cohort-table-latest.json",
              "r32-headtohead-latest.json", "r32-decision-latest.json",
              "r32-precommit-latest.json"):
        ck(f"JSON parse: {n}", art(n) is not None)
    md = WD / "wababa-r32-frozen-boundary-and-baseline-repro-audit-r32a-latest.md"
    if not md.exists():
        notrun("R32A canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("R32A 첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None, first)
        ck("WAIT 미사용", not re.match(r"^(전체 판정:\s*)?WAIT$", first))
        for need in ("PRE_AUDIT_DECISION", "POST_AUDIT_DECISION", "R32_STATUS",
                     "REAL_MONEY_NOT_APPROVED", "2026-08-03", "2026-07-31"):
            ck(f"R32A 보고 항목: {need}", need in body)
    r32md = WD / "wababa-bm-vs-size-frozen-head-to-head-r32-latest.md"
    if r32md.exists():
        b = r32md.read_text(encoding="utf-8")
        ck("R32 보고서에 audit disposition 반영", "R32A" in b)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("apiCalls: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
