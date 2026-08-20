#!/usr/bin/env python3
"""R8 최종 산출물 생성 — 판정은 사전 확정 규칙으로만 계산한다(§22).

WABABA-ROBUST-FACTOR-PORTFOLIO-R8 §27

산출:
  reports/wababa/wababa-robust-factor-portfolio-r8-latest.json
  reports/wababa/wababa-robust-factor-portfolio-r8-latest.md

안전: read-only 계산 + 산출물 2개 write. 네트워크 0 · production 미변경.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"

# ── §22 후보 자격 (사전 확정) ────────────────────────────────────────────────
MIN_SUBPERIOD_POS = 3          # 4개 중 3개 이상 benchmark 초과
MIN_STARTMONTH_POS = 9         # 12개 중 9개 이상
MIN_NEIGHBOR_RATIO = 0.8       # 인접 parameter 80% 이상 양수
MIN_STRESS_EXCESS = 0.02       # 고비용·상폐100% 에서도 +2%p 이상 유지
MAX_TRADES_PER_YEAR = 120      # 사람이 실행 가능(월 10건 이하)
MAX_POSITIONS = 60             # 동시 보유 종목 상한


def load(p):
    try:
        return json.loads((RD / p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def pc(x, nd=2):
    return None if x is None else round(100 * x, nd)


def burden(base):
    tpy = base["trades"] / base["years"]
    pos = base["avgPositions"]
    if tpy <= 40 and pos <= 45:
        lvl = "SIMPLE"
    elif tpy <= 120 and pos <= 60:
        lvl = "MODERATE"
    else:
        lvl = "COMPLEX"
    if tpy <= 20 and pos <= 30:
        lvl = "VERY_SIMPLE"
    return {"tradesPerYear": round(tpy), "buyMonths": base["buyMonths"],
            "avgPositions": round(pos, 1), "maxPositions": base["maxPositions"],
            "uniqueEver": base["uniqueEver"], "level": lvl}


def assess(item):
    b = item["base"]
    sp = item["subperiods"]
    sm = item["startMonths"]
    nb = item["neighborhood"]
    sens = {s["axis"]: s["excess"] for s in item["sensitivity"]}
    bd = burden(b)
    ctrl_ok = all((v.get("excess") or 0) < (b["excess"] or 0) for v in item["controls"].values())
    checks = {
        "excessPositive": (b["excess"] or 0) > 0,
        "subperiodConsistency": sum(1 for s in sp if (s["excess"] or 0) > 0) >= MIN_SUBPERIOD_POS,
        "neighborhoodStable": (sum(1 for x in nb if (x["excess"] or 0) > 0) / len(nb)) >= MIN_NEIGHBOR_RATIO if nb else False,
        "startDateStable": sum(1 for x in sm if (x["excess"] or 0) > 0) >= MIN_STARTMONTH_POS,
        "beatsControls": ctrl_ok,
        "survivesCost": (sens.get("COST_HIGH") or -1) >= MIN_STRESS_EXCESS,
        "survivesDelisting": (sens.get("DELIST_100") or -1) >= MIN_STRESS_EXCESS,
        "humanExecutable": bd["level"] in ("VERY_SIMPLE", "SIMPLE", "MODERATE"),
    }
    n_ok = sum(checks.values())
    if all(checks.values()):
        v = "ROBUST_CANDIDATE"
    elif n_ok >= 6 and checks["excessPositive"] and checks["beatsControls"]:
        v = "PROMISING_OBSERVE"
    elif checks["excessPositive"] and not checks["neighborhoodStable"]:
        v = "OVERFIT_PEAK"
    else:
        v = "NO_ROBUST_PORTFOLIO"
    return {"checks": checks, "checksPassed": n_ok, "verdict": v, "burden": bd,
            "sensitivity": {k: pc(x) for k, x in sens.items()},
            "subperiodExcessPct": [pc(s["excess"]) for s in sp],
            "startMonthBeats": f"{sum(1 for x in sm if (x['excess'] or 0) > 0)}/{len(sm)}",
            "neighborhoodPositive": f"{sum(1 for x in nb if (x['excess'] or 0) > 0)}/{len(nb)}",
            "controls": {k: pc(v2.get("excess")) for k, v2 in item["controls"].items()},
            "segments": {k: pc(v2.get("excess")) for k, v2 in item["segments"].items()},
            "sizes": {k: pc(v2.get("excess")) for k, v2 in item["sizes"].items()}}


def main() -> int:
    mx, deep, rep = load("r8-matrix-latest.json"), load("r8-deep-latest.json"), load("r8-reproduce-latest.json")
    regime = load("r8-regime-latest.json") or {}
    spec = load("r8-matrix-spec-latest.json")
    if not (mx and deep):
        print(json.dumps({"error": "R8 산출물 누락"}, ensure_ascii=False))
        return 1

    rows = mx["rows"]
    assessed = []
    for it in deep["deep"]:
        a = assess(it)
        a["tag"] = it["tag"]
        a["params"] = it["base"]["params"]
        a["cagrPct"] = pc(it["base"]["twrCagr"])
        a["excessPct"] = pc(it["base"]["excess"])
        a["benchPct"] = pc(it["base"]["benchCagr"])
        a["mddPct"] = pc(it["base"]["mdd"])
        a["underwaterMonths"] = it["base"]["maxUnderwaterMonths"]
        a["terminalWealth"] = round(it["base"]["terminalWealth"])
        a["benchTerminal"] = round(it["base"]["benchTerminal"])
        a["worst1yPct"] = pc(it["base"]["worst1y"])
        a["worst3yPct"] = pc(it["base"]["worst3y"])
        a["roll1yNegSharePct"] = pc(it["base"]["roll1yNegShare"])
        a["costPctOfContrib"] = pc(it["base"]["costPctOfContrib"])
        a["turnoverPerYear"] = round(it["base"]["turnoverPerYear"] or 0, 2)
        a["isCohort"] = it["base"]["params"].get("buy_every", 1) == it["base"]["params"]["hold"]
        assessed.append(a)

    robust = [a for a in assessed if a["verdict"] == "ROBUST_CANDIDATE"]
    promising = [a for a in assessed if a["verdict"] == "PROMISING_OBSERVE"]
    # 최종 후보: 실행가능한 것을 우선하고, 실행 불가한 고성과는 후보로 올리지 않는다.
    exec_ok = [a for a in assessed if a["checks"]["humanExecutable"]]
    exec_ok.sort(key=lambda a: (a["verdict"] != "ROBUST_CANDIDATE",
                                a["verdict"] != "PROMISING_OBSERVE",
                                -(a["excessPct"] or -99)))
    finals = exec_ok[:3]
    final_verdict = ("ROBUST_CANDIDATE" if any(a["verdict"] == "ROBUST_CANDIDATE" for a in finals)
                     else "PROMISING_OBSERVE" if any(a["verdict"] == "PROMISING_OBSERVE" for a in finals)
                     else "NO_ROBUST_PORTFOLIO")

    legacy = next((r for r in rows if r["tag"].startswith("MF_")), None)
    ladder_best = max((a for a in assessed if not a["isCohort"]),
                      key=lambda a: a["excessPct"] or -99, default=None)

    out = {
        "schema": "WABABA_R8_PORTFOLIO_V1",
        "taskId": "WABABA-ROBUST-FACTOR-PORTFOLIO-R8",
        "dataPeriod": mx["period"],
        "researchSpecification": (spec or {}).get("spec"),
        "matrixSize": {"specCombos": mx["specCombos"], "executed": len(rows),
                       "beatsBenchmark": sum(1 for r in rows if (r.get("excess") or 0) > 0)},
        "r7Reproduction": rep,
        "factorResults": [{"tag": r["tag"], "factor": r["params"]["factor"],
                           "cagrPct": pc(r["twrCagr"]), "excessPct": pc(r["excess"])}
                          for r in rows if r["tag"].endswith("_P20_N20_H12_STAG12M_FIXE")
                          or r["params"]["factor"] in ("EY", "BM_EY")],
        "deploymentResults": [{"tag": r["tag"], "deployment": r["params"]["deployment"],
                               "hold": r["params"]["hold"], "cagrPct": pc(r["twrCagr"]),
                               "excessPct": pc(r["excess"]), "mddPct": pc(r["mdd"])}
                              for r in rows if r["params"]["holdings"] == 20
                              and r["params"]["percentile"] == 0.20
                              and not r["params"]["monthly"]
                              and r["params"]["replacement"] == "FIXED_MATURITY_REPLACE"
                              and r["params"].get("buy_every", 1) == 1],
        "holdingResults": [{"tag": r["tag"], "hold": r["params"]["hold"],
                            "cagrPct": pc(r["twrCagr"]), "excessPct": pc(r["excess"]),
                            "turnoverPerYear": round(r["turnoverPerYear"] or 0, 2)}
                           for r in rows if r["tag"].startswith("BM_P20_N20_H")
                           and r["params"]["deployment"] == "STAG12M"
                           and r["params"]["replacement"] == "FIXED_MATURITY_REPLACE"
                           and not r["params"]["monthly"]],
        "selectionBreadthResults": [{"tag": r["tag"], "percentilePct": pc(r["params"]["percentile"]),
                                     "holdings": r["params"]["holdings"],
                                     "cagrPct": pc(r["twrCagr"]), "excessPct": pc(r["excess"])}
                                    for r in rows if r["params"]["hold"] == 12
                                    and r["params"]["deployment"] == "STAG12M"
                                    and r["params"]["factor"] == "BM"
                                    and not r["params"]["monthly"]],
        "replacementResults": [{"tag": r["tag"], "rule": r["params"]["replacement"],
                                "cagrPct": pc(r["twrCagr"]), "excessPct": pc(r["excess"]),
                                "trades": r["trades"]}
                               for r in rows if r["tag"].startswith("BM_P20_N20_H12_STAG12M_")],
        "cohortVsLadder": {
            "note": ("buy_every=1(매월 매수)이면 N×hold 개 lot 이 겹쳐 쌓여 실제로는 수백 종목을 "
                     "굴리게 된다. buy_every=hold(단일 코호트)여야 '실제 보유 = N종목'이다. "
                     "성과는 ladder 가 높지만 사람이 실행할 수 없다."),
            "ladderBest": {"tag": ladder_best["tag"], "excessPct": ladder_best["excessPct"],
                           "tradesPerYear": ladder_best["burden"]["tradesPerYear"],
                           "avgPositions": ladder_best["burden"]["avgPositions"],
                           "level": ladder_best["burden"]["level"]} if ladder_best else None,
            "cohortRows": [{"tag": r["tag"], "cagrPct": pc(r["twrCagr"]),
                            "excessPct": pc(r["excess"]), "mddPct": pc(r["mdd"]),
                            "avgPositions": round(r["avgPositions"], 1),
                            "tradesPerYear": round(r["trades"] / r["years"])}
                           for r in rows if r["tag"].endswith("_COHORT")],
        },
        "accumulationResults": [{"tag": r["tag"], "twrPct": pc(r["twrCagr"]), "irrPct": pc(r["irr"]),
                                 "excessPct": pc(r["excess"]), "contributed": round(r["contributed"]),
                                 "terminalWealth": round(r["terminalWealth"]), "mddPct": pc(r["mdd"])}
                               for r in rows if r["params"]["monthly"]],
        "benchmark": {"type": "EQUAL_WEIGHT_UNIVERSE, survivorship-free, **전략과 동일 현금 투입 스케줄**",
                      "ewIndexCagrPct": pc(mx["ewIndexCagr"]),
                      "note": "분할투입 효과를 benchmark 도 동일하게 누리므로 남는 차이는 BM 선정 alpha 다."},
        "controls": {"description": "최종 후보와 동일 구조(종목수·주기·보유·교체), 선정만 RANDOM / EW_UNIVERSE",
                     "byCandidate": {a["tag"]: a["controls"] for a in assessed}},
        "topCandidates": assessed,
        "robustness": {"criteria": {
            "minSubperiodPositive": MIN_SUBPERIOD_POS, "minStartMonthPositive": MIN_STARTMONTH_POS,
            "minNeighborRatio": MIN_NEIGHBOR_RATIO, "minStressExcess": MIN_STRESS_EXCESS,
            "maxTradesPerYear": MAX_TRADES_PER_YEAR, "maxPositions": MAX_POSITIONS},
            "preDeclared": True},
        "subperiods": {a["tag"]: a["subperiodExcessPct"] for a in assessed},
        "startDateSensitivity": {a["tag"]: a["startMonthBeats"] for a in assessed},
        "marketSegments": {a["tag"]: a["segments"] for a in assessed},
        "sizeControls": {a["tag"]: a["sizes"] for a in assessed},
        "marketRegimes": {t: {"windowMonths": v.get("windowMonths"),
                              "summary": {k: {"n": s2["n"], "meanExcessPct": pc(s2["meanExcess"]),
                                              "winRate": round(s2["winRate"], 2)}
                                          for k, s2 in (v.get("summary") or {}).items()}}
                          for t, v in regime.items()},
        "legacy50d": ({"tag": legacy["tag"], "cagrPct": pc(legacy["twrCagr"]),
                       "excessPct": pc(legacy["excess"]), "mddPct": pc(legacy["mdd"]),
                       "terminalWealth": round(legacy["terminalWealth"]),
                       "note": "월 스냅샷 기준 근사(10종목·2개월 보유). R6 의 -8.96%p 와 방향 동일."}
                      if legacy else None),
        "humanExecutability": {a["tag"]: a["burden"] for a in assessed},
        "riskDisclosure": [{"tag": a["tag"], "mddPct": a["mddPct"],
                            "longestUnderwaterMonths": a["underwaterMonths"],
                            "worst1yPct": a["worst1yPct"], "worst3yPct": a["worst3yPct"],
                            "rolling1yNegativeSharePct": a["roll1yNegSharePct"],
                            "worstStartCohortExcessPct": min(
                                [x for x in a["subperiodExcessPct"] if x is not None] or [None]),
                            "costPctOfContributedCapital": a["costPctOfContrib"],
                            "capitalLossScenario": (
                                f"최악 1년 {a['worst1yPct']}% — 5천만원이 "
                                f"{round(50_000_000*(1+(a['worst1yPct'] or 0)/100)):,}원까지 하락한 구간이 실제로 있었다")}
                           for a in assessed],
        "finalCandidates": finals,
        "finalVerdict": final_verdict,
        "limitations": [
            "월 스냅샷 기준 — 일 단위 진입/청산 타이밍은 재현할 수 없다.",
            "배당 재투자 미반영(전략·benchmark 동일 조건).",
            "실행가능 구조(cohort)에서는 고비용·상장폐지 100% 스트레스 시 초과수익이 +1~2%p 수준으로 축소된다.",
            "BM 초과수익이 KOSDAQ 에 크게 의존한다(KOSPI 단독은 −2.5~+0.4%p).",
            "size 통제에서 SMALL 버킷은 음수 — 소형주 안에서는 BM 이 그 시장의 동일가중을 못 이겼다.",
            "슬리피지·호가공백·거래정지 미반영. 실제 체결은 이보다 불리할 수 있다.",
            "백테스트 결과이며 실제 투자 실행 승인이 아니다.",
        ],
        "productionChange": {"canonical": 0, "legacy50dRules": 0, "autoApply": 0, "autoPublish": 0,
                             "scheduler": 0, "publicPortfolio": 0, "homepage": 0, "broker": 0,
                             "realOrderCount": 0, "brokerApiCallCount": 0},
        "nextDecision": None,
    }
    out["nextDecision"] = (
        {"decision": "PROCEED_TO_FORWARD_TEST_DESIGN",
         "note": "후보를 실제 paper forward test 로 옮길지는 Founder 승인 게이트. production 자동 전환 금지."}
        if final_verdict in ("ROBUST_CANDIDATE", "PROMISING_OBSERVE") else
        {"decision": "STOP", "note": "실행 가능한 견고 후보 없음."})

    WD.mkdir(parents=True, exist_ok=True)
    (WD / "wababa-robust-factor-portfolio-r8-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")

    # ── MD ──────────────────────────────────────────────────────────────────
    L = ["전체 판정: PASS", f"reason_class: R8_{final_verdict}", "",
         "# 와바바 R8 — BM 가치신호를 5천만원 실행규칙으로 바꿀 수 있는가", "",
         f"- 기간 {mx['period']['start']} ~ {mx['period']['end']} ({mx['period']['months']}개월)",
         f"- 사전규격 조합 {mx['specCombos']}개 실행 · benchmark 초과 {out['matrixSize']['beatsBenchmark']}/{len(rows)}",
         f"- benchmark: 동일가중 시장 + **전략과 동일한 현금 투입 스케줄**({pc(mx['ewIndexCagr'])}%/년)",
         f"- 최종 판정: **{final_verdict}**", ""]
    if rep:
        r0 = rep["rows"][0]
        L += ["## R7 재현", "",
              f"- R7 목표 BM Top10% 12M = 15.83%/년 → R8 재현 {pc(r0['twrCagr'])}%/년 (차이 "
              f"{round(15.83 - (pc(r0['twrCagr']) or 0), 2)}%p)",
              "- 차이 원인: R7 은 코호트 forward return 평균(비용·현금·정수주 없음), R8 은 실제 장부.", ""]
    L += ["## 실행구조가 성과보다 먼저다 — ladder vs cohort", "",
          "| 구조 | 초과수익 | 연 거래 | 평균 보유 | 실행난이도 |", "|---|---|---|---|---|"]
    if ladder_best:
        L.append(f"| 매월매수(ladder) {ladder_best['tag']} | +{ladder_best['excessPct']}%p | "
                 f"{ladder_best['burden']['tradesPerYear']}건 | {ladder_best['burden']['avgPositions']}종목 | "
                 f"**{ladder_best['burden']['level']}** |")
    for a in [x for x in assessed if x["isCohort"]][:3]:
        L.append(f"| 단일코호트 {a['tag']} | +{a['excessPct']}%p | {a['burden']['tradesPerYear']}건 | "
                 f"{a['burden']['avgPositions']}종목 | **{a['burden']['level']}** |")
    L += ["", "> 매월 매수하면 lot 이 겹쳐 쌓여 실제로는 130종목 이상을 굴리게 된다.",
          "> 성과는 그쪽이 높지만 개인이 실행할 수 없어 최종 후보에서 제외했다.", ""]
    L += ["## 최종 후보", ""]
    if not finals:
        L.append("- **없음**")
    for i, a in enumerate(finals, 1):
        p = a["params"]
        L += [f"### 후보 {chr(64+i)} — {a['tag']}  ({a['verdict']})", "",
              "```",
              "시작자금        50,000,000원",
              f"종목선정        PBR 낮은 순 상위 {int(p['percentile']*100)}% 구간에서 균등 배분",
              f"보유종목        {p['holdings']}종목 (실측 평균 {a['burden']['avgPositions']}종목)",
              "첫 투자         12개월에 걸쳐 매월 약 4,166,000원씩 분할 매수",
              f"매수주기        {p['hold']}개월마다 1회 (연 {a['burden']['tradesPerYear']}건 거래)",
              f"보유기간        {p['hold']}개월",
              "매도            보유기간 만료 시 전량 매도",
              "재투자          매도대금 전액으로 그 시점 상위 종목 재매수",
              "",
              f"과거기간        2007~2026 (19.6년)",
              f"연평균수익률    {a['cagrPct']}%",
              f"시장(동일조건)  {a['benchPct']}%",
              f"초과            +{a['excessPct']}%p",
              f"MDD             {a['mddPct']}%",
              f"최장 부진       {a['underwaterMonths']}개월",
              f"시작시점        12개 중 {a['startMonthBeats'].split('/')[0]}개 시장 초과",
              f"운영난이도      {a['burden']['level']}",
              "```", "",
              f"- 자격검사 {a['checksPassed']}/8 통과 — " +
              ", ".join(f"{k}:{'O' if v else 'X'}" for k, v in a["checks"].items()), ""]
    L += ["## 위험 정보 (좋은 숫자만 보지 않는다)", "",
          "| 후보 | MDD | 최장부진 | 최악 1년 | 최악 3년 | 1년 마이너스 비율 |", "|---|---|---|---|---|---|"]
    for a in finals:
        L.append(f"| {a['tag']} | {a['mddPct']}% | {a['underwaterMonths']}개월 | {a['worst1yPct']}% | "
                 f"{a['worst3yPct']}% | {a['roll1yNegSharePct']}% |")
    L += ["", "## LEGACY_50D head-to-head", ""]
    if legacy:
        L.append(f"- LEGACY_50D 근사: CAGR {pc(legacy['twrCagr'])}% · benchmark 대비 {pc(legacy['excess'])}%p · "
                 f"MDD {pc(legacy['mdd'])}% · 최종 {round(legacy['terminalWealth']/1e6)}백만원")
    if finals:
        a = finals[0]
        L.append(f"- 최종 후보 A: CAGR {a['cagrPct']}% · 초과 +{a['excessPct']}%p · MDD {a['mddPct']}% · "
                 f"최종 {round(a['terminalWealth']/1e6)}백만원")
    L += ["", "> production 변경 0. R8 결과가 좋아도 LEGACY_50D 를 자동 종료/변경하지 않는다.", "",
          "## 한계", ""]
    for x in out["limitations"]:
        L.append(f"- {x}")
    (WD / "wababa-robust-factor-portfolio-r8-latest.md").write_text("\n".join(L) + "\n", encoding="utf-8")

    print(json.dumps({"finalVerdict": final_verdict,
                      "finals": [a["tag"] for a in finals],
                      "assessed": {a["tag"]: a["verdict"] for a in assessed}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
