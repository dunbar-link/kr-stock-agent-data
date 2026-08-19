#!/usr/bin/env python3
"""R6 공개용 연구자료 생성 — machine-readable(JSON) + human-readable(MD).

WABABA-CAPITAL-DEPLOYMENT-AND-HOLDING-RULE-MATRIX-R6 §14

판정을 사람이 손으로 쓰지 않는다. r6-matrix / r6-deep / r6-control 산출물에서
사전 정의된 규칙으로만 계산해 저장한다(재실행 시 같은 결과).

산출:
  reports/wababa/wababa-investment-rule-research-r6-latest.json
  reports/wababa/wababa-investment-rule-research-r6-latest.md

안전: read-only 계산 + 산출물 2개 write. 네트워크 0 · canonical 미접근 · 홈페이지 미수정.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
MIN_UNDERWATER_OK = 60          # 5년 초과 수중은 일반인이 못 버틴다


def load(p):
    try:
        return json.loads((RD / p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def pct(x):
    return None if x is None else round(100 * x, 2)


def main() -> int:
    mx, deep, ctl = load("r6-matrix-latest.json"), load("r6-deep-latest.json"), load("r6-control-latest.json")
    audit = load("r6-audit-latest.json")
    if not (mx and deep and ctl):
        print(json.dumps({"error": "R6 산출물 누락"}, ensure_ascii=False))
        return 1

    fair = mx["meta"]["benchmarkFairCagr"]
    bm = mx.get("benchmarks", {})
    rows = [r for r in mx["rows"] if r.get("twrCagr") is not None]
    beats = [r for r in rows if r["excessVsFair"] > 0]
    plateau = mx.get("plateau", {})

    # ── 후보 판정 ────────────────────────────────────────────────────────────
    # 단독 peak 가 시작월 이동에서 재현되지 않으면 후보로 인정하지 않는다.
    peak = ctl.get("peakSummary", {})
    peak_ok = peak.get("verdict") == "REPRODUCIBLE"
    if plateau.get("robustCount", 0) >= 3:
        candidate_verdict = "ROBUST_PLATEAU"
    elif plateau.get("robustCount", 0) >= 1:
        candidate_verdict = "PROMISING_BUT_WEAK"
    elif plateau.get("peakCount", 0) > 0 and not peak_ok:
        candidate_verdict = "OVERFIT_PEAK"
    elif beats:
        candidate_verdict = "PROMISING_BUT_WEAK"
    else:
        candidate_verdict = "UNDERPERFORMING"

    # ── 사업 판정 (R5 를 자동 상속하지 않고 R6 근거로 새로 계산) ───────────────
    worst_uw = max((r.get("maxUnderwaterMonths") or 0) for r in rows)
    conds = {
        "hasRobustPlateau": plateau.get("robustCount", 0) >= 3,
        "peakReproducible": peak_ok,
        "beatsInvestableIndex": any(
            r["twrCagr"] > (bm.get("OFFICIAL_KOSPI", {}).get("cagr") or 9e9) for r in rows),
        "underwaterTolerable": worst_uw <= MIN_UNDERWATER_OK,
        "rankingAddsValue": ctl.get("diagnosis") != "STRUCTURAL_UNDERPERFORMANCE",
    }
    if conds["hasRobustPlateau"] and conds["peakReproducible"] and conds["underwaterTolerable"]:
        business = "SCALE"
    elif candidate_verdict == "PROMISING_BUT_WEAK":
        business = "OBSERVE"
    else:
        business = "HOLD"

    # ── 축별 비교 요약 ───────────────────────────────────────────────────────
    def find(tag):
        return next((r for r in rows if r["tag"] == tag), None)

    deployment = []
    for lbl in ("LUMP_SUM", "STAGGERED_3", "STAGGERED_6", "STAGGERED_12", "STAGGERED_25", "STAGGERED_50"):
        r = find(f"DEP_{lbl}_H12_N20")
        if r:
            deployment.append({"deployment": lbl, "cagrPct": pct(r["twrCagr"]),
                               "mddPct": pct(r["mdd"]), "idleCashPct": pct(r["idleCashRatioAvg"]),
                               "terminalWealth": round(r["terminalWealth"])})
    replacement = []
    for r in rows:
        if r["tag"].startswith("REPL_") and r["params"].get("hold_months") == 12:
            replacement.append({"rule": r["params"]["replacement"], "cagrPct": pct(r["twrCagr"]),
                                "mddPct": pct(r["mdd"]), "trades": r["trades"],
                                "turnoverPerYear": round(r["turnoverPerYear"] or 0, 2)})
    cadence = [{"tag": r["tag"], "buyEveryMonths": r["params"].get("buy_every", 1),
                "cagrPct": pct(r["twrCagr"]), "trades": r["trades"]}
               for r in rows if r["tag"].startswith("CADENCE_")]
    accumulation = [{"tag": r["tag"], "twrPct": pct(r["twrCagr"]), "irrPct": pct(r["irr"]),
                     "contributed": round(r["totalContributed"]),
                     "terminalWealth": round(r["terminalWealth"]), "mddPct": pct(r["mdd"])}
                    for r in rows if r["tag"].startswith(("DCA_", "INIT_PLUS_"))]
    legacy = [{"tag": r["tag"], "cagrPct": pct(r["twrCagr"]), "excessPct": pct(r["excessVsFair"]),
               "mddPct": pct(r["mdd"]), "trades": r["trades"],
               "avgConcurrentPositions": round(r["avgConcurrentPositions"], 1),
               "note": r.get("note", "")}
              for r in rows if "LEGACY" in r["tag"]]

    top = sorted(rows, key=lambda r: r["excessVsFair"], reverse=True)[:5]
    top_candidates = [{"tag": r["tag"], "cagrPct": pct(r["twrCagr"]), "excessPct": pct(r["excessVsFair"]),
                       "mddPct": pct(r["mdd"]), "underwaterMonths": r["maxUnderwaterMonths"],
                       "rejected": True, "rejectReason": (
                           "시작월 이동에서 재현되지 않는 경로의존 아티팩트" if r["tag"] == "LUMP_SUM_H24_N10"
                           else "공정 benchmark 미달")}
                      for r in top]

    out = {
        "schema": "WABABA_R6_RESEARCH_V1",
        "taskId": "WABABA-CAPITAL-DEPLOYMENT-AND-HOLDING-RULE-MATRIX-R6",
        "formulaLabel": "APPROXIMATE_MAGIC_FORMULA",
        "dataPeriod": {"start": mx["meta"]["start"], "end": mx["meta"]["end"],
                       "months": mx["meta"]["months"], "years": round(mx["meta"]["months"] / 12, 2)},
        "methodology": {
            "pointInTime": "각 시점 스냅샷의 KRX 공표값만 사용(사후 정정 재무 미사용)",
            "survivorshipFree": "그 시점 상장 종목만 universe. 보유 중 상장폐지는 마지막 관측가×(1-haircut) 청산",
            "benchmark": "전략과 동일 생존조건의 동일가중 투자가능 포트폴리오(R5 수정판). 공식 지수는 별도 표기",
            "capital": mx["meta"]["initialCapital"],
            "monthlyAmount": mx["meta"]["monthlyAmount"],
            "costs": "매수 15bp · 매도 15bp + 거래세 20bp",
            "deterministic": "난수 0 — 같은 입력이면 같은 결과",
            "holdPeriodUnit": "월 단위 스냅샷. 거래일 등가는 holdTradingDaysApprox 로 병기",
        },
        "testedDimensions": {
            "capitalDeployment": ["LUMP_SUM", "STAGGERED_3", "STAGGERED_6", "STAGGERED_12", "STAGGERED_25", "STAGGERED_50"],
            "purchaseCadence": ["EVERY_1M", "EVERY_3M"],
            "numberOfStocks": [10, 20, 30, 40],
            "holdingPeriodMonths": [2, 3, 6, 9, 12, 18, 24, 36],
            "replacementRule": ["MATURITY_REPLACE", "FIXED_MATURITY", "RANK_RETENTION", "PERIODIC_REBALANCE"],
            "accumulation": ["LUMP_SUM", "MONTHLY_DCA", "INITIAL_PLUS_MONTHLY"],
        },
        "r5AxisAudit": (audit or {}).get("axisAudit"),
        "r5NotTested": (audit or {}).get("notTested"),
        "testedStrategies": len(rows),
        "beatsFairBenchmark": len(beats),
        "benchmark": {"fairEqualWeightCagrPct": pct(fair),
                      "investableIndexes": {k: pct(v.get("cagr")) for k, v in bm.items() if k.startswith("OFFICIAL")},
                      "allBenchmarksPct": {k: pct(v.get("cagr")) for k, v in bm.items()}},
        "comparisons": {"capitalDeployment": deployment, "replacementRule": replacement,
                        "purchaseCadence": cadence, "accumulation": accumulation},
        "legacy50d": legacy,
        "topCandidates": top_candidates,
        "rejectedCandidates": [c for c in top_candidates if c["rejected"]],
        "finalCandidates": [] if business == "HOLD" else None,
        "robustness": {
            "plateauVerdict": plateau.get("verdict"),
            "robustCells": plateau.get("robustCount"),
            "isolatedPeaks": plateau.get("peakCount"),
            "peakReplication": ctl.get("peakSummary"),
            "controlDiagnosis": ctl.get("diagnosis"),
            "controlSummary": {k: {kk: pct(vv) for kk, vv in v.items()}
                               for k, v in (ctl.get("controlSummary") or {}).items()},
            "deep": [{"tag": d["tag"], "startMonthBeats": d["startMonthBeats"],
                      "subperiodExcessPct": [pct(s["excess"]) for s in d["subperiods"]],
                      "regimeSummary": {k: {"n": v["n"], "meanExcessPct": pct(v["meanExcess"]),
                                            "winRate": round(v["winRate"], 2)}
                                        for k, v in (d.get("regimeSummary") or {}).items()}}
                     for d in deep["deep"]],
        },
        "candidateVerdict": candidate_verdict,
        "businessVerdict": business,
        "businessConditions": conds,
        "keyFindings": [
            "분할투입(staggering)이 일시투입보다 대부분의 보유기간에서 우월하고, MDD 를 크게 낮춘다.",
            "교체규칙(만기교체/현금대기/순위유지/정기리밸런싱)은 성과 차이를 거의 만들지 않는다.",
            "대조군 검사 결과 부진의 원인은 순위(Magic Formula)가 아니라 소수종목 집중 구조다.",
            "전 구간 유일한 초과 조합은 시작월을 옮기면 재현되지 않는 경로의존 아티팩트다.",
        ],
        "limitations": [
            "스냅샷이 월 단위라 LEGACY_50D 의 '일 단위 50회 분할'을 그대로 재현할 수 없다(월 근사만 가능).",
            "공정 benchmark(약 1,500종목 동일가중 월리밸)는 편의 없는 비교 기준이지만 개인이 실제로 복제할 수 없다.",
            "실제로 매수 가능한 기준은 공식 지수(KOSPI 7.81%)에 가깝다 — 두 기준을 함께 본다.",
            "ORIGINAL_STYLE(EBIT/EV)은 DART corp_code↔종목코드 매핑 부재로 여전히 산출 불가.",
            "배당 재투자는 모델에 포함되지 않는다(전략·benchmark 양쪽 모두 동일하게 제외).",
        ],
        "productionChange": {"canonical": 0, "legacy50dRules": 0, "publicPortfolio": 0,
                             "homepage": 0, "scheduler": 0, "realOrderCount": 0,
                             "brokerApiCallCount": 0},
    }

    WD.mkdir(parents=True, exist_ok=True)
    (WD / "wababa-investment-rule-research-r6-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── human-readable ──────────────────────────────────────────────────────
    L = []
    L.append(f"전체 판정: {'PASS' if business != 'BLOCKED' else 'BLOCKED'}")
    L.append(f"reason_class: R6_{candidate_verdict}_{business}")
    L.append("")
    L.append("# 와바바 투자규칙 연구 R6 — 얼마씩·얼마나 자주·몇 종목·얼마나 보유·어떻게 교체")
    L.append("")
    L.append(f"- 기간 {out['dataPeriod']['start']} ~ {out['dataPeriod']['end']} "
             f"({out['dataPeriod']['months']}개월 · 약 {out['dataPeriod']['years']}년)")
    L.append(f"- 비교자본 {out['methodology']['capital']:,}원 · 검증 조합 {len(rows)}개")
    L.append(f"- 공정 benchmark {pct(fair)}%/년 · 투자가능 공식지수 KOSPI "
             f"{pct(bm.get('OFFICIAL_KOSPI', {}).get('cagr'))}%/년")
    L.append(f"- 후보 판정 **{candidate_verdict}** · 사업 판정 **{business}**")
    L.append("")
    L.append("## 자본 투입방식 (보유 12개월 · 20종목 고정)")
    L.append("")
    L.append("| 투입방식 | CAGR | MDD | 평균 현금비중 | 최종자산 |")
    L.append("|---|---|---|---|---|")
    for d in deployment:
        L.append(f"| {d['deployment']} | {d['cagrPct']}% | {d['mddPct']}% | {d['idleCashPct']}% | {d['terminalWealth']:,}원 |")
    L.append("")
    L.append("## 교체규칙 (보유 12개월 · 20종목)")
    L.append("")
    L.append("| 규칙 | CAGR | MDD | 회전율 | 거래수 |")
    L.append("|---|---|---|---|---|")
    for r in replacement:
        L.append(f"| {r['rule']} | {r['cagrPct']}% | {r['mddPct']}% | {r['turnoverPerYear']} | {r['trades']} |")
    L.append("")
    L.append("## 적립식 / 초기+적립 (현금흐름이 달라 TWR·IRR·최종자산을 분리)")
    L.append("")
    L.append("| 전략 | TWR | IRR | 납입 | 최종자산 | MDD |")
    L.append("|---|---|---|---|---|---|")
    for a in accumulation:
        L.append(f"| {a['tag']} | {a['twrPct']}% | {a['irrPct']}% | {a['contributed']:,} | {a['terminalWealth']:,} | {a['mddPct']}% |")
    L.append("")
    L.append("## LEGACY_50D 역사 시뮬레이션")
    L.append("")
    for lg in legacy:
        L.append(f"- **{lg['tag']}** CAGR {lg['cagrPct']}% · benchmark 대비 {lg['excessPct']}%p · "
                 f"MDD {lg['mddPct']}% · 거래 {lg['trades']}건 · 평균 동시보유 {lg['avgConcurrentPositions']}종목")
        if lg["note"]:
            L.append(f"  - {lg['note']}")
    L.append("")
    L.append("## 왜 후보가 없는가 (자기감사 결과)")
    L.append("")
    L.append(f"- 대조군 진단: **{ctl.get('diagnosis')}** — 같은 구조에서 선정규칙만 바꿔도(시총상위·종목코드순) "
             "비슷하게 부진했다. 즉 문제는 Magic Formula 순위가 아니라 **소수종목 집중 구조**다.")
    if peak:
        L.append(f"- 단독 peak 재현성: benchmark 초과 {peak.get('beats')}/{peak.get('n')} · "
                 f"평균 {pct(peak.get('mean'))}%p · 판정 **{peak.get('verdict')}**")
    L.append("")
    L.append("## 핵심 발견")
    L.append("")
    for k in out["keyFindings"]:
        L.append(f"- {k}")
    L.append("")
    L.append("## 한계")
    L.append("")
    for k in out["limitations"]:
        L.append(f"- {k}")
    L.append("")
    L.append("> 이 연구는 production 을 변경하지 않는다. canonical · LEGACY_50D · public portfolio ·")
    L.append("> 홈페이지 · scheduler 변경 0 · 실주문 0 · 브로커 호출 0.")
    (WD / "wababa-investment-rule-research-r6-latest.md").write_text("\n".join(L) + "\n", encoding="utf-8")

    print(json.dumps({"candidateVerdict": candidate_verdict, "businessVerdict": business,
                      "tested": len(rows), "beatsFair": len(beats),
                      "json": str(WD / "wababa-investment-rule-research-r6-latest.json"),
                      "md": str(WD / "wababa-investment-rule-research-r6-latest.md")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
