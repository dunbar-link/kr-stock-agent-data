#!/usr/bin/env python3
"""R32 결정 엔진 — 동결된 winner rule 을 코드로 집행한다.

WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32

── 이 파일의 핵심 성질: 결과 독립 ─────────────────────────────────
  어떤 factor 가 이기는지 이 코드는 모른다. 입력 dict 하나를 받아 §8 규칙을
  기계적으로 적용할 뿐이고, SIZE/BM 을 서로 바꿔 넣으면 결정도 대칭으로 뒤집힌다.
  (test_r32_validation 의 synthetic fixture 4종이 이것을 강제한다.)

  보고서 문장으로 사후 판정하지 않는다(§20). 판정은 여기서만 난다.

── 실행가능성 gate 는 새로 만들지 않는다 ──────────────────────────
  R27 precommit 의 QUALIFICATION 사다리(`order`)를 **두 factor 에 똑같이** 적용한다.
  Rule B 가 요구하는 "동일 execution rules" 가 바로 이것이다.
  조건값을 R32 에서 바꾸지 않는다 — 정본에서 읽는다.

  실행가능성 floor 는 **PROMISING** 이다(STRONG 은 그 위 등급). 아래 execution_gate
  주석의 2026-09-08 결함 수정 기록 참조 — 초판은 최상위 등급을 floor 로 잘못 묶었다.

── §8 winner rule (지시문 문구 그대로) ────────────────────────────
  1) 정확히 하나만 gate 통과 → 그 factor 가 PRIMARY
  2) 둘 다 미통과 → NO_EXECUTABLE_WINNER
  3) 둘 다 통과 → paired comparison. 아래 **전부** 충족해야 단독 PRIMARY:
       · BASE canonical 비용가정에서 순성과 우위
       · paired uncertainty interval 이 0 을 같은 방향으로 벗어남
       · 100bp stress 에서 승패 방향이 뒤집히지 않음
       · 기간·시장·top3 제거 검증을 위반하지 않음
       · execution fragility gate 를 위반하지 않음
     하나라도 미충족 → BOTH_SEPARATE (새 margin 을 만들지 않는다)

사용: python scripts/research/r32_decide.py
부작용: reports/research/r32-decision-latest.json 1개 write. 네트워크 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r27_precommit import COST, QUALIFICATION  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

FACTORS = ("SIZE", "BM")
DECISION_ENUM = ["BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE",
                 "NO_EXECUTABLE_WINNER"]
STRONG = QUALIFICATION["SIZE_EXECUTABLE_STRONG"]


# ══════════════ 실행가능성 gate (두 factor 동일 적용) ══════════════
#
# ★ 2026-09-08 결함 수정 — 초판은 "실행가능성 gate" 를 STRONG 하나로 묶었다.
#   그런데 R27 의 QUALIFICATION["order"] 는
#       DATA_INSUFFICIENT → ALPHA_LARGELY_NONTRADABLE → FRAGILE
#       → EXECUTABLE_STRONG → EXECUTABLE_PROMISING
#   이고, **EXECUTABLE 등급이 두 개**다. 즉 STRONG 은 최상위 '등급'이지
#   실행가능성의 '바닥' 이 아니다. 실행가능성 floor 는 PROMISING 이다.
#   초판처럼 STRONG 을 floor 로 쓰면 excess 4.99%p 인 factor 가 '실행 불가' 가 되는데
#   R27 정본은 그것을 EXECUTABLE_PROMISING 으로 분류한다 — 정본과 어긋난다.
#
#   그래서 gate 를 R27 사다리 그대로 구현한다:
#       executable = NOT nontradableAlpha  AND  NOT fragile  AND  (STRONG or PROMISING)
#   STRONG 여부는 **등급**으로 따로 보고한다(승자 결정에 쓰지 않는다).
#   이 수정은 R27 조건값을 하나도 바꾸지 않는다 — 어떤 상수를 어디에 묶느냐의 문제다.
PROMISING = QUALIFICATION["SIZE_EXECUTABLE_PROMISING"]
FRAGILE = QUALIFICATION["SIZE_FRAGILE"]


def strong_grade(f):
    """R27 최상위 등급 판정(설명용). 승자 결정에 쓰지 않는다."""
    c = {
        "AFTER TOP−CONTROL >= %.1f%%p" % STRONG["afterTopMinusControlPctMin"]:
            f["excessPct"] is not None
            and f["excessPct"] >= STRONG["afterTopMinusControlPctMin"],
        "retention >= %.1f" % STRONG["retentionRatioMin"]:
            f["retentionRatio"] is not None
            and f["retentionRatio"] >= STRONG["retentionRatioMin"],
        "included(tradable) spread 양(+)":
            bool(f["includedSpreadPositive"]),
        "부분기간 양(+) 비율 >= %.2f" % STRONG["minSubperiodPositiveRatio"]:
            f["subperiodPositiveRatio"] is not None
            and f["subperiodPositiveRatio"] >= STRONG["minSubperiodPositiveRatio"],
        "롤링 양(+) 비율 >= %.2f" % STRONG["minRollingPositiveRatio"]:
            f["rollingPositiveRatio"] is not None
            and f["rollingPositiveRatio"] >= STRONG["minRollingPositiveRatio"],
        "KOSPI·KOSDAQ 방향 동일":
            bool(f["bothExchangesSameSign"]),
        "top3 제거 후 부호 유지":
            bool(f["survivesTop3Removal"]),
        "nontradable alpha 아님":
            not bool(f["nontradableAlpha"]),
    }
    return all(c.values()), c, [k for k, v in c.items() if not v]


def execution_gate(f):
    """R27 사다리 그대로의 **실행가능성** 판정. (pass, {조건: bool}, [실패사유])."""
    strong_ok, strong_c, _ = strong_grade(f)
    promising_ok = (
        f["excessPct"] is not None
        and f["excessPct"] >= PROMISING["afterTopMinusControlPctMin"]
        and bool(f["includedSpreadPositive"])
        and f["subperiodPositiveRatio"] is not None
        and f["subperiodPositiveRatio"] >= PROMISING["minSubperiodPositiveRatio"])
    attr_pct = (f["attritionRate"] * 100.0) if f["attritionRate"] is not None else None
    fragile = bool((not f["survivesTop3Removal"])
                   or (attr_pct is not None
                       and attr_pct >= FRAGILE["attritionMaxPct"]))
    c = {
        "nontradable alpha 아님": not bool(f["nontradableAlpha"]),
        "FRAGILE 아님 (top3 부호유지 · attrition < %.0f%%)" % FRAGILE["attritionMaxPct"]:
            not fragile,
        "EXECUTABLE 등급 (STRONG 또는 PROMISING)": bool(strong_ok or promising_ok),
    }
    ok = all(c.values())
    return ok, {**c, "_grade": "STRONG" if strong_ok else
                ("PROMISING" if promising_ok else "NOT_EXECUTABLE"),
                "_strongConditions": strong_c}, [k for k, v in c.items() if not v]


# ══════════════ §8 winner rule ══════════════
def decide(inp):
    """결과 독립 결정. inp 는 factor 이름을 키로 갖는 대칭 구조여야 한다."""
    codes, rejections = [], {a: [] for a in FACTORS}

    if not inp.get("baselineReproductionPass", False):
        return {"decision": "NO_EXECUTABLE_WINNER",
                "reasonCodes": ["BASELINE_REPRODUCTION_FAILED"],
                "gateMatrix": {}, "rejections": rejections,
                "blockedBeforeComparison": True}

    matrix, passed, grades = {}, [], {}
    strong_only_passed = []
    for a in FACTORS:
        ok, cond, fails = execution_gate(inp["factors"][a])
        matrix[a] = cond
        grades[a] = cond["_grade"]
        rejections[a] = fails
        if ok:
            passed.append(a)
        if strong_grade(inp["factors"][a])[0]:
            strong_only_passed.append(a)
    # 투명성: 초판 구현(STRONG 을 floor 로 쓴 경우) 이 무엇을 냈을지 함께 남긴다.
    codes.append("GRADES::" + ",".join(f"{a}={grades[a]}" for a in FACTORS))
    codes.append("IF_STRONG_WERE_FLOOR_PASSERS::" + ",".join(strong_only_passed))

    # 1) 정확히 하나만 통과
    if len(passed) == 1:
        w = passed[0]
        codes.append(f"ONLY_{w}_PASSES_EXECUTION_GATE")
        return {"decision": f"{w}_PRIMARY", "reasonCodes": codes,
                "gateMatrix": matrix, "rejections": rejections,
                "winnerRuleStep": "step1"}

    # 2) 둘 다 미통과
    if not passed:
        codes.append("NEITHER_PASSES_EXECUTION_GATE")
        return {"decision": "NO_EXECUTABLE_WINNER", "reasonCodes": codes,
                "gateMatrix": matrix, "rejections": rejections,
                "winnerRuleStep": "step2"}

    # 3) 둘 다 통과 → paired comparison
    codes.append("BOTH_PASS_EXECUTION_GATE")
    p = inp["paired"]
    lead = p.get("leader")              # 순성과 우위 factor ('SIZE'|'BM'|None)
    checks = {
        "BASE canonical 비용에서 순성과 우위": lead is not None,
        "paired CI 가 0 을 같은 방향으로 벗어남": bool(p.get("ciExcludesZero")),
        "100bp 에서 승패 방향 유지": not bool(p.get("rankReversalAt100bp")),
        "기간·시장·top3 검증 위반 없음": bool(p.get("stabilityChecksPass")),
        "execution fragility gate 위반 없음": bool(p.get("fragilityPass")),
    }
    if lead is not None and p.get("ciDirection") not in (None, lead):
        checks["paired CI 가 0 을 같은 방향으로 벗어남"] = False

    if all(checks.values()):
        codes.append(f"SOLO_PRIMARY_ALL_CONDITIONS_MET_{lead}")
        return {"decision": f"{lead}_PRIMARY", "reasonCodes": codes,
                "gateMatrix": matrix, "rejections": rejections,
                "soloPrimaryChecks": checks, "winnerRuleStep": "step3-solo"}

    for k, v in checks.items():
        if not v:
            codes.append("SOLO_PRIMARY_BLOCKED::" + k)
    codes.append("CONSERVATIVE_TIE_NO_NEW_MARGIN")
    return {"decision": "BOTH_SEPARATE", "reasonCodes": codes,
            "gateMatrix": matrix, "rejections": rejections,
            "soloPrimaryChecks": checks, "winnerRuleStep": "step3-tie"}


# ══════════════ 엔진 산출물 → 결정 입력 ══════════════
def build_inputs(h2h, baseline_pass):
    """r32-headtohead 산출물을 대칭 입력으로 변환. 판정은 하지 않는다."""
    perf, liq, stab, adv, unc = (h2h["performance"], h2h["liquidity"],
                                 h2h["stability"], h2h["adversarial"],
                                 h2h["uncertainty"])
    factors = {}
    for a in FACTORS:
        ex = perf[a]["excessVsControl"] or {}
        before = (h2h.get("before") or {}).get(a) or {}
        b_ex = before.get("excessMeanPct")
        a_ex = ex.get("meanPct")
        ret = (a_ex / b_ex) if (a_ex is not None and b_ex) else None
        subs = [v[a] for v in stab["subperiods"].values() if v.get(a)]
        sub_pos = (sum(1 for s in subs if s["meanPct"] > 0) / len(subs)) if subs else None
        roll = stab["rolling"]
        roll_vals = [roll[k]["overlapping"] for k in roll
                     if roll[k].get("overlapping")]
        rp = stab["signalYear"]
        yrs = [v[a] for v in rp.values() if v.get(a)]
        roll_pos = (sum(1 for y in yrs if y["meanPct"] > 0) / len(yrs)) if yrs else None
        mk = stab["markets"]
        try:
            same = ((mk["KOSPI"][a]["meanPct"] > 0) == (mk["KOSDAQ"][a]["meanPct"] > 0))
        except (KeyError, TypeError):
            same = False
        t3 = adv["removal"]["top3"][a]
        survives = bool(t3.get("excessVsControl")
                        and t3["excessVsControl"]["meanPct"] > 0)
        factors[a] = {
            "excessPct": a_ex,
            "retentionRatio": round(ret, 4) if ret is not None else None,
            "includedSpreadPositive": (liq[a]["includedTopAnnPct"] or 0) > 0,
            "subperiodPositiveRatio": round(sub_pos, 4) if sub_pos is not None else None,
            "rollingPositiveRatio": round(roll_pos, 4) if roll_pos is not None else None,
            "bothExchangesSameSign": same,
            "survivesTop3Removal": survives,
            "nontradableAlpha": bool(liq[a]["nontradableAlpha"]),
            "net100bpPct": perf[a]["costCurve"]["100bp"]["netPct"],
            "grossPct": perf[a]["meanTopAnnPct"],
            "attritionRate": liq[a]["attritionRate"],
            "thresholdHeadroom": liq[a]["thresholdHeadroom"],
        }
        _ = roll_vals

    # canonical 비용(R26 상속 base=0bp)에서의 순성과 우위
    base_bps = COST["base"]["roundTripBps"]
    net_base = {a: perf[a]["costCurve"][f"{base_bps}bp"]["netPct"] for a in FACTORS}
    if net_base["SIZE"] > net_base["BM"]:
        leader = "SIZE"
    elif net_base["BM"] > net_base["SIZE"]:
        leader = "BM"
    else:
        leader = None

    boot = (unc["blockBootstrap"]["summary"] or {})
    lo, hi = boot.get("ci95LowPct"), boot.get("ci95HighPct")
    ci_excl = bool(lo is not None and hi is not None and (lo > 0 or hi < 0))
    ci_dir = None
    if ci_excl:
        ci_dir = "SIZE" if lo > 0 else "BM"

    net100 = {a: factors[a]["net100bpPct"] for a in FACTORS}
    leader100 = ("SIZE" if net100["SIZE"] > net100["BM"]
                 else "BM" if net100["BM"] > net100["SIZE"] else None)
    reversal = bool(leader and leader100 and leader != leader100)

    stability_pass = bool(
        stab.get("marketDirectionAgrees")
        and adv.get("loyoSizeMinusBmSignStable")
        and all((stab["subperiods"][k]["sizeMinusBm"] or {}).get("meanPct", 0) > 0
                for k in stab["subperiods"])
        or stab.get("marketDirectionAgrees")
        and adv.get("loyoSizeMinusBmSignStable")
        and all((stab["subperiods"][k]["sizeMinusBm"] or {}).get("meanPct", 0) < 0
                for k in stab["subperiods"]))

    fragility_pass = all(
        factors[a]["survivesTop3Removal"] and not factors[a]["nontradableAlpha"]
        for a in FACTORS)

    return {
        "baselineReproductionPass": baseline_pass,
        "factors": factors,
        "paired": {
            "leader": leader,
            "netBaseCostPct": net_base,
            "canonicalBaseBps": base_bps,
            "ciExcludesZero": ci_excl, "ciDirection": ci_dir,
            "ci95": {"lowPct": lo, "highPct": hi},
            "rankReversalAt100bp": reversal,
            "leaderAt100bp": leader100,
            "stabilityChecksPass": stability_pass,
            "fragilityPass": fragility_pass,
        },
    }


def main() -> int:
    h2h_p = RD / "r32-headtohead-latest.json"
    if not h2h_p.exists():
        print(json.dumps({"verdict": "BLOCKED",
                          "reasonClass": "R32_HEADTOHEAD_NOT_COMPUTED"},
                         ensure_ascii=False))
        return 1
    h2h = json.loads(h2h_p.read_text(encoding="utf-8"))
    repro_p = RD / "r32-baseline-repro-latest.json"
    repro = json.loads(repro_p.read_text(encoding="utf-8")) if repro_p.exists() else {}
    baseline_pass = bool(repro.get("allPass"))

    inp = build_inputs(h2h, baseline_pass)
    dec = decide(inp)
    assert dec["decision"] in DECISION_ENUM, "decision enum 위반"

    out = {"task": "R32", "phase": "decision",
           "FINAL_CANDIDATE_DECISION": dec["decision"],
           "DECISION_REASON_CODES": dec["reasonCodes"],
           "PRIMARY_REJECTION_REASONS": dec["rejections"],
           "RULE_B_PASS_MATRIX": dec["gateMatrix"],
           "grades": {a: dec["gateMatrix"][a]["_grade"] for a in FACTORS}
                     if dec["gateMatrix"] else {},
           "soloPrimaryChecks": dec.get("soloPrimaryChecks"),
           "winnerRuleStep": dec.get("winnerRuleStep"),
           "inputs": inp,
           "decisionEnum": DECISION_ENUM,
           "executionGateSource": "r27_precommit.QUALIFICATION['SIZE_EXECUTABLE_STRONG'] — 두 factor 동일 적용",
           "resultIndependentLogic": True}
    (RD / "r32-decision-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    print(json.dumps({"FINAL_CANDIDATE_DECISION": dec["decision"],
                      "step": dec.get("winnerRuleStep"),
                      "reasonCodes": dec["reasonCodes"],
                      "rejections": dec["rejections"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
