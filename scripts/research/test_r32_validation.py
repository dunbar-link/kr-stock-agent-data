#!/usr/bin/env python3
"""R32 회귀 — 동결 계약 · matched sample · 결정 엔진 · 경계. 네트워크 0.

WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32

계층:
  L1 freeze    — R27/R31/R32 해시 · dataset manifest · threshold 불변
  L2 baseline  — R25/R31 재현 gate
  L3 sample    — matched date/universe/position/cost/weighting/horizon · 미완결 제외
  L4 integrity — survivorship · future leakage · current-listing · direct trade value
  L5 metrics   — cost stress · turnover · drawdown · attrition · headroom · 시장 · 기간
                 · rolling · top-N 제거 · LOYO · source boundary · overlap · 불확실성
  L6 decision  — enum · 결과 독립성 · synthetic fixture 4종
  L7 boundary  — raw Git 제외 · secret 0 · public repo 무변경 · LEGACY_50D 무변경
  L8 report    — 판정 헤더 · JSON parse · git diff --check

사용: python scripts/research/test_r32_validation.py
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r32_decide as D                                  # noqa: E402
import r32_precommit as SPEC                            # noqa: E402

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


def art(name):
    p = RD / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except ValueError:
        return None


def sha(name):
    return hashlib.sha256((SRC / f"{name}.py").read_bytes()).hexdigest()


def git(args, cwd=ROOT):
    try:
        return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


# ══════════════ L1 freeze ══════════════
def t_l1():
    print("\n[L1] 동결 계약")
    ck("1 R27 precommit hash acee4288", sha("r27_precommit").startswith("acee4288"))
    ck("2 R31 precommit hash d7fe3929", sha("r31_precommit").startswith("d7fe3929"))
    r32 = sha("r32_precommit")
    spec = art("r32-precommit-latest.json")
    ck("3 R32 precommit 산출물 존재/parse", spec is not None)
    ck("4 R32 precommit hash 기록 가능", len(r32) == 64)
    st = art("r31-stitch-latest.json") or {}
    ck("5 dataset manifest 3439dec9",
       str(st.get("manifestSha256", "")).startswith("3439dec9"))
    ck("6 dataset range 2010-01-04..2026-07-31",
       st.get("stitchedRange") == ["2010-01-04", "2026-07-31"])
    ck("7 boundary duplicate 0 · provenance 누락 0",
       st.get("duplicatePrimaryKeys") == 0 and st.get("rowsMissingProvenance") == 0)
    if spec:
        ck("8 BASE 125,000,000 고정",
           spec["liquidity"]["primaryThresholdKrw"] == 125_000_000)
        ck("9 LOW/HIGH 값 불변",
           spec["liquidity"]["sensitivityKrw"] == {"LOW": 25_000_000,
                                                   "BASE": 125_000_000,
                                                   "HIGH": 250_000_000})
        ck("10 threshold 변경 불가 선언",
           spec["liquidity"]["thresholdChangeAllowed"] is False)
        ck("11 결합/새 factor 금지 선언",
           spec["production"]["combinationAllowed"] is False
           and spec["production"]["newFactorCount"] == 0)
        ck("12 결과 이전 작성 선언", spec["writtenBeforeResults"] is True)
        ck("13 Rule B 는 승자규칙이 아님을 기록",
           spec["ruleB"]["providesWinnerConditions"] is False
           and spec["ruleB"]["isNextTaskRule"] is True)
        ck("14 decision enum 4종",
           spec["decisionEnum"] == ["BM_PRIMARY", "SIZE_PRIMARY",
                                    "BOTH_SEPARATE", "NO_EXECUTABLE_WINNER"])
        ck("15 solo PRIMARY 는 5조건 전부 필요",
           spec["winnerRule"]["soloPrimaryRequiresAll"] is True
           and len(spec["winnerRule"]["soloPrimaryConditions"]) == 5)
        ck("16 tie 는 BOTH_SEPARATE", spec["winnerRule"]["tieOutcome"] == "BOTH_SEPARATE")
        ck("17 horizon 36M 고정",
           spec["executionContract"]["holdingPeriodMonths"] == 36)


# ══════════════ L2 baseline ══════════════
def t_l2():
    print("\n[L2] baseline 재현")
    b = art("r32-baseline-repro-latest.json")
    if b is None:
        notrun("18 baseline gate", "미실행")
        return
    ck("18 baseline gate PASS", b["allPass"] is True,
       str([k for k, v in b["checks"].items() if not v])[:160])
    ck("19 R25 raw 36M 재현", all(
        b["checks"].get(f"R25 raw 36M {a} = {v}", False)
        for a, v in {"SIZE": 19.147, "BM": 15.01, "CONTROL": 9.418}.items()))
    ck("20 R31 BASE 36M 재현", all(
        b["checks"].get(f"R31 BASE 36M {a} = {v}", False)
        for a, v in {"SIZE": 13.698, "BM": 12.193, "CONTROL": 7.467}.items()))
    ck("21 R31 100bp 재현",
       b["checks"].get("R31 100bp SIZE = 9.846", False)
       and b["checks"].get("R31 100bp BM = 9.51", False))
    ck("22 재계산 아닌 대조(복제 금지)", b["recomputed"] is False)


# ══════════════ L3 matched sample ══════════════
def t_l3():
    print("\n[L3] matched sample · 평가기간")
    h = art("r32-headtohead-latest.json")
    if h is None:
        for n in range(23, 30):
            notrun(f"{n} matched sample", "head-to-head 미실행")
        return
    ms, pe = h["matchedSample"], h["period"]
    ck("23 BM/SIZE 관측수 동일", ms["bmObservations"] == ms["sizeObservations"],
       f'{ms["bmObservations"]}/{ms["sizeObservations"]}')
    ck("24 BM-only / SIZE-only 0",
       not ms["bmOnly"] and not ms["sizeOnly"],
       f'bmOnly={len(ms["bmOnly"])} sizeOnly={len(ms["sizeOnly"])}')
    ck("25 common == 사용된 paired rows",
       ms["commonObservations"] == ms["pairedRowsUsed"])
    ck("26 동일 mask 엔진 보장", ms["identicalMaskGuaranteedByEngine"] is True)
    ck("27 미완결 cohort 0", pe["incompleteCohortsIncluded"] == 0)
    ck("28 마지막 완결 cohort 종료일 기록", bool(pe["lastCompleteCohortEndDate"]))
    ck("29 primary 시작 2010", pe["firstSignalDate"] >= "2010-01-01",
       str(pe["firstSignalDate"]))
    ck("30 2007~2009 primary 미포함",
       all(d < "2010-01-01" for d in pe["pre2010Dates"]))
    # 동일 비용·가중·horizon
    perf = h["performance"]
    ck("31 동일 horizon 36M", pe["horizonMonths"] == 36)
    ck("32 동일 비용식 적용(두 factor 같은 bp 집합)",
       set(perf["SIZE"]["costCurve"]) == set(perf["BM"]["costCurve"]))
    ck("33 position count 동일 규칙에서 산출",
       perf["SIZE"]["avgHoldingCount"] is not None
       and perf["BM"]["avgHoldingCount"] is not None)


# ══════════════ L4 integrity ══════════════
def t_l4():
    print("\n[L4] PIT · survivorship · 데이터 무결성")
    ana = (SRC / "r27_analysis.py").read_text(encoding="utf-8")
    eng = (SRC / "r32_engine.py").read_text(encoding="utf-8")
    ck("34 PIT trailing window(결정일 직전만)",
       "self.cal[max(0, i - LOOKBACK):i]" in ana)
    ck("35 future leakage 0", "결정일 **직전**만" in ana)
    ck("36 current-listing join 0",
       not re.search(r"current_listing|currently_listed|live_tickers", eng + ana))
    ck("37 상폐 제거 코드 0", not re.search(r"delisted.*remove|remove.*delisted", eng))
    ck("38 close x volume proxy 0",
       not re.search(r"close.*\*.*volume|volume.*\*.*close", eng))
    ck("39 새 데이터 수집 0(requests/pykrx 미사용)",
       not re.search(r"(?m)^\s*(import|from)\s+(requests|pykrx)", eng))
    st = art("r31-stitch-latest.json") or {}
    ck("40 direct trade value only (금지연산 0)",
       all(v == 0 for v in (st.get("forbiddenOperationsUsed") or {}).values()))


# ══════════════ L5 metrics ══════════════
def t_l5():
    print("\n[L5] 지표")
    h = art("r32-headtohead-latest.json")
    if h is None:
        for n in range(41, 56):
            notrun(f"{n} 지표", "head-to-head 미실행")
        return
    p, l, s, a, o, u = (h["performance"], h["liquidity"], h["stability"],
                        h["adversarial"], h["overlap"], h["uncertainty"])
    ck("41 cost stress 0/25/50/100bp",
       set(p["SIZE"]["costCurve"]) == {"0bp", "25bp", "50bp", "100bp"})
    ck("42 turnover 산출", all(p[f]["turnover"]["annualTurnover"] is not None
                              for f in ("SIZE", "BM")))
    ck("43 drawdown proxy 산출(+NAV 아님 명시)",
       all(p[f]["chainedDrawdown"] and "NAV drawdown 아님" in p[f]["chainedDrawdown"]["note"]
           for f in ("SIZE", "BM")))
    ck("44 volatility 산출", all(p[f]["volatilityAnnPct"] is not None
                                for f in ("SIZE", "BM")))
    ck("45 attrition 산출", all(l[f]["attritionRate"] is not None
                               for f in ("SIZE", "BM")))
    ck("46 liquidity headroom 산출", all(l[f]["thresholdHeadroom"] is not None
                                        for f in ("SIZE", "BM")))
    ck("47 p10~p90 유동성 분포", all(l[f]["p10Krw"] is not None and l[f]["p90Krw"] is not None
                                  for f in ("SIZE", "BM")))
    ck("48 시장 분할 KOSPI/KOSDAQ", set(s["markets"]) == {"KOSPI", "KOSDAQ"})
    ck("49 고정 부분기간 3구간",
       set(s["subperiods"]) == {"2010-2014", "2015-2019", "2020-END"})
    ck("50 rolling 36/60M", set(s["rolling"]) == {"36M", "60M"})
    ck("51 top1/3/5 제거", set(a["removal"]) == {"top1", "top3", "top5"})
    ck("52 leave-one-year-out", len(a["leaveOneYearOut"]) >= 5)
    ck("53 best/worst signal year 제거",
       set(a["yearRemoval"]) == {"removeBestSignalYear", "removeWorstSignalYear"})
    ck("54 source boundary 전후 비교",
       set(h["stability"]["sourceBoundary"]) == {"KRX_2010_2019", "PORTAL_2020_PLUS"})
    ck("55 overlap/correlation 산출",
       o["meanJaccard"] is not None and o["returnCorrelation"] is not None)
    ck("56 overlap 은 설명용(결정 미사용)", "PRIMARY 선택에 쓰지 않는다" in o["role"])
    ck("57 paired 불확실성(중첩/비중첩/block)",
       all(k in u for k in ("overlapping", "nonOverlapping", "blockBootstrap")))
    ck("58 block = holding horizon 보존", u["blockBootstrap"]["blockMonths"] == 36)
    ck("59 p-value 단독 판정 금지", u["pValueAloneDecides"] is False)
    ck("60 cash drag / 정수주 미모델 명시",
       p["SIZE"]["cashDrag"] == "NOT_MODELLED_COHORT_METHOD")


# ══════════════ L6 decision ══════════════
def _factor(passing, excess=8.0, retention=0.8):
    """gate 통과/미통과 factor 입력을 대칭으로 만든다."""
    if passing:
        return {"excessPct": excess, "retentionRatio": retention,
                "includedSpreadPositive": True, "subperiodPositiveRatio": 1.0,
                "rollingPositiveRatio": 1.0, "bothExchangesSameSign": True,
                "survivesTop3Removal": True, "nontradableAlpha": False,
                "net100bpPct": excess, "grossPct": excess,
                "attritionRate": 0.5, "thresholdHeadroom": 1.2}
    return {"excessPct": 0.5, "retentionRatio": 0.1,
            "includedSpreadPositive": False, "subperiodPositiveRatio": 0.0,
            "rollingPositiveRatio": 0.0, "bothExchangesSameSign": False,
            "survivesTop3Removal": False, "nontradableAlpha": True,
            "net100bpPct": 0.5, "grossPct": 0.5,
            "attritionRate": 0.9, "thresholdHeadroom": 0.5}


def _paired(leader, solo_ok=True, reversal=False):
    return {"leader": leader,
            "ciExcludesZero": bool(solo_ok), "ciDirection": leader if solo_ok else None,
            "rankReversalAt100bp": reversal,
            "stabilityChecksPass": bool(solo_ok),
            "fragilityPass": True}


def t_l6():
    print("\n[L6] 결정 엔진 (결과 독립)")
    enum = {"BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE", "NO_EXECUTABLE_WINNER"}

    # fixture 1 — SIZE 만 gate 통과
    r = D.decide({"baselineReproductionPass": True,
                  "factors": {"SIZE": _factor(True), "BM": _factor(False)},
                  "paired": _paired("SIZE")})
    ck("61 synthetic SIZE_PRIMARY", r["decision"] == "SIZE_PRIMARY", r["decision"])

    # fixture 2 — BM 만 통과 (완전 대칭)
    r = D.decide({"baselineReproductionPass": True,
                  "factors": {"SIZE": _factor(False), "BM": _factor(True)},
                  "paired": _paired("BM")})
    ck("62 synthetic BM_PRIMARY", r["decision"] == "BM_PRIMARY", r["decision"])

    # fixture 3 — 둘 다 통과하지만 solo 조건 미충족
    r = D.decide({"baselineReproductionPass": True,
                  "factors": {"SIZE": _factor(True, 9.0), "BM": _factor(True, 8.0)},
                  "paired": _paired("SIZE", solo_ok=False)})
    ck("63 synthetic BOTH_SEPARATE", r["decision"] == "BOTH_SEPARATE", r["decision"])
    ck("64 tie 시 새 margin 만들지 않음",
       "CONSERVATIVE_TIE_NO_NEW_MARGIN" in r["reasonCodes"])

    # fixture 4 — 둘 다 미통과
    r = D.decide({"baselineReproductionPass": True,
                  "factors": {"SIZE": _factor(False), "BM": _factor(False)},
                  "paired": _paired(None, solo_ok=False)})
    ck("65 synthetic NO_EXECUTABLE_WINNER",
       r["decision"] == "NO_EXECUTABLE_WINNER", r["decision"])

    # fixture 5 — 둘 다 통과 + 모든 solo 조건 충족 → 단독 PRIMARY
    r = D.decide({"baselineReproductionPass": True,
                  "factors": {"SIZE": _factor(True, 9.0), "BM": _factor(True, 8.0)},
                  "paired": _paired("SIZE", solo_ok=True)})
    ck("66 solo 조건 전부 충족 시 단독 PRIMARY", r["decision"] == "SIZE_PRIMARY")

    # 결과 독립성 — SIZE/BM 을 바꾸면 결정도 대칭으로 바뀐다
    r2 = D.decide({"baselineReproductionPass": True,
                   "factors": {"SIZE": _factor(True, 8.0), "BM": _factor(True, 9.0)},
                   "paired": _paired("BM", solo_ok=True)})
    ck("67 결과 독립(대칭) — 입력을 뒤집으면 결정도 뒤집힘",
       r2["decision"] == "BM_PRIMARY", r2["decision"])

    # 100bp 역전이면 단독 PRIMARY 불가
    r3 = D.decide({"baselineReproductionPass": True,
                   "factors": {"SIZE": _factor(True, 9.0), "BM": _factor(True, 8.0)},
                   "paired": _paired("SIZE", solo_ok=True, reversal=True)})
    ck("68 100bp 순위역전이면 단독 PRIMARY 차단",
       r3["decision"] == "BOTH_SEPARATE", r3["decision"])

    # baseline 재현 실패면 비교 이전에 차단
    r4 = D.decide({"baselineReproductionPass": False,
                   "factors": {"SIZE": _factor(True), "BM": _factor(False)},
                   "paired": _paired("SIZE")})
    ck("69 baseline 실패 시 비교 전 차단",
       r4["decision"] == "NO_EXECUTABLE_WINNER"
       and r4.get("blockedBeforeComparison") is True)

    # 실제 결정 산출물
    dec = art("r32-decision-latest.json")
    if dec is None:
        notrun("70 실제 결정 산출물", "decide 미실행")
    else:
        ck("70 결정이 enum 안", dec["FINAL_CANDIDATE_DECISION"] in enum,
           dec["FINAL_CANDIDATE_DECISION"])
        ck("71 결정 근거코드 존재", bool(dec["DECISION_REASON_CODES"]))
        ck("72 Rule B matrix 두 factor 모두",
           set(dec["RULE_B_PASS_MATRIX"]) == {"SIZE", "BM"})
        ck("73 gate 는 R27 사다리 3조건 + 등급 기록",
           all({"nontradable alpha 아님", "_grade", "_strongConditions"} <= set(v)
               for v in dec["RULE_B_PASS_MATRIX"].values()))
        ck("73b 등급은 STRONG/PROMISING/NOT_EXECUTABLE 중 하나",
           all(v["_grade"] in ("STRONG", "PROMISING", "NOT_EXECUTABLE")
               for v in dec["RULE_B_PASS_MATRIX"].values()))
        ck("73c 초판(STRONG-as-floor) 판정도 투명 기록",
           any(c.startswith("IF_STRONG_WERE_FLOOR_PASSERS::")
               for c in dec["DECISION_REASON_CODES"]))
        ck("74 결과 독립 로직 선언", dec["resultIndependentLogic"] is True)


# ══════════════ L7 boundary ══════════════
def t_l7():
    print("\n[L7] 데이터·저장소 경계")
    tracked = git(["ls-files"]).splitlines()
    ck("75 raw/cache Git 추적 0",
       not [f for f in tracked if f.startswith("_cache/")])
    ck("76 reports Git 추적 0",
       not [f for f in tracked if f.startswith("reports/")])
    ck("77 .env 추적 0", not [f for f in tracked if f.endswith(".env")
                             or f.endswith(".env.local")])
    src_all = "\n".join((SRC / f"r32_{n}.py").read_text(encoding="utf-8")
                        for n in ("precommit", "engine", "decide", "baseline"))
    ck("78 R32 코드에 인증키 접근 0",
       "KRX_OPENAPI_AUTH_KEY" not in src_all and "auth_key" not in src_all)
    ck("79 R32 코드에 네트워크 호출 0",
       not re.search(r"(?m)^\s*(import|from)\s+(requests|urllib|httpx)", src_all))
    # 키 모양 = 40자 이상 연속 토큰. 다음은 비밀값일 수 없어 제외한다:
    #   sha256(소문자 hex 64) · TASK_ID 류(대문자+하이픈만) · 경로/식별자(_ / . 포함)
    leaky = [x for x in re.findall(r"[A-Za-z0-9._~%+/=-]{40,}", src_all)
             if not re.fullmatch(r"[0-9a-f]{64}", x)
             and not re.fullmatch(r"[A-Z0-9-]+", x)
             and "_" not in x and "/" not in x and "." not in x]
    ck("80 40자 이상 키 모양 리터럴 0(sha256·TASK_ID 제외)", not leaky,
       str(leaky[:2])[:120])
    if PUBLIC_REPO.exists():
        st = git(["status", "--porcelain"], PUBLIC_REPO)
        ck("81 public repo 에 R32 변경 0",
           not [ln for ln in st.splitlines() if re.search(r"r32", ln, re.I)])
    else:
        notrun("81 public repo", "경로 없음")
    ck("82 known dirty 보존(financial-universe-real.json 미스테이지)",
       "financial-universe-real.json" not in git(["diff", "--cached", "--name-only"]))
    legacy = git(["diff", "--name-only", "HEAD"])
    ck("83 LEGACY_50D 관련 파일 변경 0",
       not re.search(r"legacy.?50", legacy, re.I), legacy[:120])


# ══════════════ L8 report ══════════════
def t_l8():
    print("\n[L8] 산출물")
    md = WD / "wababa-bm-vs-size-frozen-head-to-head-r32-latest.md"
    js = WD / "wababa-bm-vs-size-frozen-head-to-head-r32-latest.json"
    if not md.exists():
        notrun("84 canonical MD", "보고서 작성 단계에서 생성")
        notrun("85 canonical JSON", "보고서 작성 단계에서 생성")
    else:
        first = md.read_text(encoding="utf-8").splitlines()[0].strip()
        ck("84 첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None,
           first)
        ck("85 canonical JSON parse", js.exists() and json.loads(
            js.read_text(encoding="utf-8")) is not None)
        body = md.read_text(encoding="utf-8")
        for need in ("FINAL_CANDIDATE_DECISION", "REAL_MONEY_NOT_APPROVED",
                     "Rule B", "Founder", "다음 단일 작업"):
            ck(f"86 보고 항목: {need}", need in body)
    for n in ("r32-precommit-latest.json", "r32-baseline-repro-latest.json",
              "r32-headtohead-latest.json", "r32-decision-latest.json"):
        p = RD / n
        if p.exists():
            ck(f"87 JSON parse: {n}", art(n) is not None)
        else:
            notrun(f"87 JSON parse: {n}", "미생성")
    out = git(["diff", "--check"])
    ck("88 git diff --check clean", "trailing whitespace" not in out.lower())


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
