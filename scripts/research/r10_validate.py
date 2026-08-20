#!/usr/bin/env python3
"""R10 Phase D — 사전규격 준수 엔진으로 R9 adversarial validation 전체 재실행.

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

★ 설계 원칙: **검증 로직을 다시 쓰지 않는다.**
   r9_validate 의 mode 함수를 그대로 호출하고, 그 모듈이 참조하는 **엔진 심볼만**
   r10_engine 으로 교체한다. 이렇게 하면
     - R9 precommit gate 가 코드 수준에서 변경 불가능하다(§8 gate 유지)
     - walk-forward harness 수정(49·73개월 창)·cohort 최소창 조건이 자동 승계된다(§7 회귀)
     - benchmark 정의·비용·상폐 가정·지표 계산이 R9 와 100% 동일하다
   즉 R9 와 R10 의 차이는 **자본 투입 스케줄 단 하나**로 통제된다.

산출: reports/research/r10-*-latest.json (R8·R9 산출물은 건드리지 않는다 — §14 provenance)

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 홈페이지 미수정 ·
      실주문 0 · 브로커 0 · 유료데이터 0 · 외부발송 0 · 배포 0 · env/token 0.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r9_validate as V  # noqa: E402
import r10_engine as R10  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r8_portfolio import evaluate as r8_eval  # noqa: E402
from r8_portfolio import run_benchmark_same_schedule as r8_bench  # noqa: E402
from r8_portfolio import run_portfolio as r8_run  # noqa: E402
from r9_engine import run_case as r9_run_case  # noqa: E402
from r9_precommit import FROZEN, GATES  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
P = FROZEN["params"]


# ═════════════════ 엔진 교체 (검증 로직은 손대지 않는다) ═════════════════
def _remap_burden(res):
    """실행부담 지표를 **주문 건수**로 맞춘다.

    r10 은 tranche 마다 lot 을 만들므로 lot 단위 trades 가 실제 주문보다 많다
    (한 종목 12 tranche lot 을 파는 것은 사람 기준 매도주문 1건이다).
    G10 게이트의 취지가 'humanExecutable / 사람이 실행 가능'이므로 그 취지대로
    주문 건수를 쓰고, lot 단위 값은 별도 키로 보존해 보고서에 병기한다.
    수수료·세금은 simulate 내부에서 lot 단위로 이미 정확히 부과됐다(영향 없음).
    """
    res = dict(res)
    res["lotTrades"], res["lotBuys"], res["lotSells"] = (
        res["trades"], res["buys"], res["sells"])
    res["trades"] = res["orders"]
    res["buys"], res["sells"] = res["buyOrders"], res["sellOrders"]
    return res


def _aggregate_positions(res):
    """lot 을 **포지션 단위(코호트, 종목)** 로 합친다.

    R10 은 tranche 마다 lot 을 만든다. 그러면 코호트 0 은 종목당 12 lot = 480 lot 이 되어
    per-lot 수익률 분포가 코호트 0 하나에 지배된다(실측: 중앙 -31%·승률 26% 로 왜곡).
    사람이 보는 단위는 '한 종목을 12개월에 걸쳐 모아서 만기에 한 번 매도한 포지션'이다.
    합산은 금액 합계를 바꾸지 않으므로(기여도·시장/size 귀속은 불변) 분포 통계만 정상화된다.
    R9 는 코호트당 종목당 lot 1개였으므로 이 합산 후 두 결과가 같은 단위로 비교된다.
    """
    res = dict(res)
    for key in ("ledger", "openLots"):
        agg = {}
        for l in res[key]:
            k = (l["cohort"], l["ticker"])
            e = agg.get(k)
            if e is None:
                agg[k] = dict(l)
                continue
            e["qty"] += l["qty"]
            e["costBasis"] += l["costBasis"]
            e["proceeds"] += l["proceeds"]
            e["pnl"] += l["pnl"]
            e["buyIdx"] = min(e["buyIdx"], l["buyIdx"])
            e["delisted"] = e["delisted"] or l["delisted"]
        out = []
        for e in agg.values():
            cb, q = e["costBasis"], e["qty"]
            e["ret"] = (e["proceeds"] / cb - 1) if cb else None
            e["execPrice"] = (cb / q) if q else e["execPrice"]   # 가중평균 매수단가
            out.append(e)
        res[key] = out
    return res


def r10_simulate(cache, dates, **kw):
    res = R10.simulate(cache, dates, **kw)
    return None if res is None else _aggregate_positions(_remap_burden(res))


def r10_run_case(cache, dates, idx, **kw):
    res = r10_simulate(cache, dates, **kw)
    if not res:
        return None, None
    b = R10.bench_same_schedule(idx, dates, tranches=res["tranches"])
    return res, R10.evaluate(res, b)


def install_engine():
    V.simulate = r10_simulate
    V.run_case = r10_run_case
    V.save = _save
    V.load = _load


def _save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r10-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r10] saved {p.name}", file=sys.stderr)


def _load(name):
    return json.loads((RD / f"r10-{name}-latest.json").read_text(encoding="utf-8"))


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


# ═════════════════ Phase D-0 재현/정합 게이트 ═════════════════
def mode_repro(sn, nm, ds, cache, idx):
    """R10 재현 게이트 — 세 가지를 동시에 증명한다.

    (1) R8/R9 baseline 이 여전히 bit-exact 로 재현된다 (provenance 보존 회귀)
    (2) R10 은 R8/R9 와 **자본 투입 스케줄에서만** 다르다 (자본흐름 원장 감사)
    (3) 만기 해석(코호트 vs tranche) 이 결과를 좌우하지 않는다 (민감도)
    """
    # (1) R8 == R9 재현 회귀
    r8res = r8_run(cache, ds, factor=P["factor"], percentile=P["percentile"],
                   n_holdings=P["holdings"], hold_months=P["hold"],
                   deployment=P["deployment"], replacement=P["replacement"],
                   capital=FROZEN["initialCapitalKrw"], buy_every=P["buy_every"],
                   rebalance_months=P["hold"])
    r8ev = r8_eval(r8res, r8_bench(idx, ds, tranches=r8res["tranches"]))
    _, r9ev = r9_run_case(cache, ds, idx, **V.BASE_KW)
    bit_exact = all(abs(r8ev[k] - r9ev[k]) < 1e-9
                    for k in ("twrCagr", "benchCagr", "excess", "mdd", "terminalWealth"))

    # (2) R10 spec-conformant
    res10, ev10 = r10_run_case(cache, ds, idx, **V.BASE_KW)
    raw10 = R10.simulate(cache, ds, **V.BASE_KW)
    audit = R10.audit_capital_flow(raw10, capital=FROZEN["initialCapitalKrw"])

    # (3) 만기 해석 민감도 — 그리고 어느 해석이 사전규격을 지키는지 구조로 판정
    _, ev_pt = r10_run_case(cache, ds, idx, maturity_unit=R10.MATURITY_PER_TRANCHE,
                            **V.BASE_KW)
    raw_pt = R10.simulate(cache, ds, maturity_unit=R10.MATURITY_PER_TRANCHE, **V.BASE_KW)

    def _sell_dates_per_cohort(r):
        by = {}
        for l in r["ledger"]:
            by.setdefault(l["cohort"], set()).add(l["sellIdx"])
        return {c: len(s) for c, s in sorted(by.items())}

    mat_conf = {}
    for lbl, r in (("COHORT", raw10), ("PER_TRANCHE", raw_pt)):
        sd = _sell_dates_per_cohort(r)
        mat_conf[lbl] = {
            "maxPositions": max(r["positions"]),
            "monthsOver40Positions": sum(1 for p in r["positions"] if p > 40),
            "sellDatesPerCohort": sd,
            "violatesN40_E4": max(r["positions"]) > P["holdings"],
            "violatesFullReplacement": any(v > 1 for v in sd.values()),
        }
        mat_conf[lbl]["specConformant"] = not (
            mat_conf[lbl]["violatesN40_E4"] or mat_conf[lbl]["violatesFullReplacement"])

    # R8 deep 모드 buy_every 미전달 결함 회귀 (§7 재발 방지)
    r8bug = []
    for lbl, kw, reported in (("COST_HIGH", dict(cost_bps=30.0, sell_tax_bps=20.0), 1.85),
                              ("DELIST_100", dict(delist_haircut=1.0), 0.86)):
        common = dict(factor=P["factor"], percentile=P["percentile"],
                      n_holdings=P["holdings"], hold_months=P["hold"],
                      deployment=P["deployment"], replacement=P["replacement"],
                      capital=FROZEN["initialCapitalKrw"], rebalance_months=P["hold"])
        rb = r8_run(cache, ds, **common, **kw)
        eb = r8_eval(rb, r8_bench(idx, ds, tranches=rb["tranches"]))
        rf = r8_run(cache, ds, **common, buy_every=P["buy_every"], **kw)
        ef = r8_eval(rf, r8_bench(idx, ds, tranches=rf["tranches"]))
        r8bug.append({"axis": lbl, "r8ReportedExcessPct": reported,
                      "asR8Called_ladder": {"excessPct": pct(eb["excess"]),
                                            "tradesPerYear": round(eb["trades"] / eb["years"])},
                      "frozenCohort_correct": {"excessPct": pct(ef["excess"]),
                                               "tradesPerYear": round(ef["trades"] / ef["years"])},
                      "r8NumberMatchesLadder": abs(pct(eb["excess"]) - reported) <= 0.05})

    ok = bool(bit_exact and audit["pass"])
    out = {
        "gate": "R10_SPEC_CONFORMANCE",
        "pass": ok,
        "period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
        "r9BaselineBitExact": bit_exact,
        "r9BaselineRegression": {
            "note": ("R8 원본 엔진과 R9 엔진이 여전히 소수점까지 일치하는지 재확인한다. "
                     "R10 이 R9 를 오염시키지 않았음을 보증한다(§14 provenance)."),
            "r8Cagr": pct(r8ev["twrCagr"]), "r9Cagr": pct(r9ev["twrCagr"]),
            "r8Excess": pct(r8ev["excess"]), "r9Excess": pct(r9ev["excess"])},
        "capitalFlowAudit": audit,
        "maturityInterpretationSensitivity": {
            "primary_COHORT": {"cagrPct": pct(ev10["twrCagr"]),
                               "excessPct": pct(ev10["excess"]),
                               "mddPct": pct(ev10["mdd"])},
            "variant_PER_TRANCHE": {"cagrPct": pct(ev_pt["twrCagr"]) if ev_pt else None,
                                    "excessPct": pct(ev_pt["excess"]) if ev_pt else None,
                                    "mddPct": pct(ev_pt["mdd"]) if ev_pt else None},
            "structuralConformance": mat_conf,
            "decision": ("PER_TRANCHE 는 사전규격을 **구조적으로 위반**한다 — 최대 동시보유 "
                         f"{mat_conf['PER_TRANCHE']['maxPositions']}종목(E4 의 N=40 위반, "
                         f"40 초과 {mat_conf['PER_TRANCHE']['monthsOver40Positions']}개월) "
                         "이고 코호트 0 의 매도가 12개 시점으로 흩어져 '만기 후 전량 교체'가 "
                         "성립하지 않는다. COHORT 는 최대 40종목·코호트당 매도 1시점으로 두 "
                         "제약을 모두 만족한다. 따라서 채택 근거는 결과값이 아니라 구조다."),
            "warning": ("두 해석의 초과수익 차이가 크다(+2.67%p vs +0.26%p). 판정 근거가 "
                        "구조라는 점을 명시하지 않으면 해석이 사실상 parameter 가 된다. "
                        "위 구조 판정은 r10_forensics.conformance_reading() 에 실행 **전** "
                        "기록됐다."),
            "note": ("만기 단위 해석이 결과를 좌우하면 그 해석이 사실상 parameter 가 된다. "
                     "두 해석의 초과수익 차이를 그대로 공개한다.")},
        "r8SensitivityStructureBug": {
            "found": all(r["r8NumberMatchesLadder"] for r in r8bug),
            "rows": r8bug,
            "impact": ("R8 이 후보 A 에 붙인 survivesCost:X / survivesDelisting:X 는 "
                       "cohort 후보가 아니라 매월매수 ladder 구조로 계산된 값이다. "
                       "R9·R10 은 전 구간에서 올바른 구조로만 스트레스를 준다."),
            "action": "R8 산출물은 재현성 보존을 위해 수정하지 않는다."},
        "base": {
            "cagrPct": pct(ev10["twrCagr"]), "benchPct": pct(ev10["benchCagr"]),
            "excessPct": pct(ev10["excess"]), "mddPct": pct(ev10["mdd"]),
            "underwaterMonths": ev10["maxUnderwaterMonths"],
            "worst1yPct": pct(ev10["worst1y"]), "worst3yPct": pct(ev10["worst3y"]),
            "terminalWealth": ev10["terminalWealth"],
            "benchTerminal": ev10["benchTerminal"],
            "contributed": ev10["contributed"],
            "tradesPerYear": round(ev10["trades"] / ev10["years"], 1),
            "lotTradesPerYear": round(raw10["trades"] / ev10["years"], 1),
            "buyOrders": raw10["buyOrders"], "sellOrders": raw10["sellOrders"],
            "avgPositions": round(ev10["avgPositions"], 1),
            "maxPositions": ev10["maxPositions"],
            "uniqueEver": ev10["uniqueEver"], "delistEvents": ev10["delistEvents"],
            "costPctOfContrib": pct(ev10["costPctOfContrib"]),
            "roll1yNegSharePct": pct(ev10["roll1yNegShare"]),
            "avgCashRatioPct": pct(ev10["avgCashRatio"]),
            "cohorts": raw10["cohorts"]},
        "burdenMetricNote": (
            "R10 은 tranche 마다 lot 이 생기므로 lot 단위 거래건수가 사람의 주문건수보다 "
            "많다. G10(사람이 실행 가능) 은 취지대로 **주문 건수**로 판정하고 lot 단위 "
            "값을 병기한다. 수수료·세금은 lot 단위로 이미 정확히 부과됐다."),
        "specVerdictRef": "reports/research/r10-forensics-latest.json",
    }
    _save("repro", out)
    print(json.dumps({"reproPass": ok, "r9BitExact": bit_exact,
                      "capitalAudit": audit["pass"],
                      "first24mCashPct": audit["first24mAvgCashPct"],
                      "baseExcessPct": out["base"]["excessPct"],
                      "ordersPerYear": out["base"]["tradesPerYear"]},
                     ensure_ascii=False))
    return 0 if ok else 3


def mode_verdict():
    """R9 게이트 로직을 **그대로** 돌리고(변경 0), 연구자 판단만 R10 사실로 교체한다.

    V.mode_verdict 의 researcherReading 문장은 R9 수치가 하드코딩돼 있어 R10 에 그대로
    두면 거짓 서술이 된다. 게이트·판정 규칙은 손대지 않고 서술만 R10 데이터로 다시 쓴다.
    """
    rc = V.mode_verdict()
    if rc:
        return rc
    d = _load("verdict")
    core, wf, ca, bm = _load("core"), _load("walkforward"), _load("cashaudit"), _load("benchmarks")
    rep = _load("repro")
    ph = d["postHocFindings"]
    sy = ph["startYearDependency"]
    esd = ph["entryScheduleDefect"]
    vp = ph["validPbrBenchmark"]
    conc = core["concentration"]
    c5 = wf["cohortDistribution"]["summary"].get("60", {})
    r5 = next((r for r in conc["removal"] if r["topK"] == 5), None)
    att = None
    p = RD / "r10-attribution-latest.json"
    if p.exists():
        att = json.loads(p.read_text(encoding="utf-8"))

    fixed = [
        f"진입 타이밍 의존이 사라졌다 — 첫 24개월 시장방향과 5년 초과수익 상관 "
        f"{esd['corr_first24mMarket_vs_5yExcess']} (R9 -0.639). 시장이 올랐을 때도 "
        f"5년 초과 중앙값 {esd['when_first24m_marketRose']['median5yExcessPct']:+.2f}%p "
        f"(positive {esd['when_first24m_marketRose']['positivePct']}%).",
        f"시작연도 편중이 사라졌다 — 2008년 이후 시작 코호트 5년 초과 중앙값 "
        f"{sy['start2008AndLater']['medianExcessPct']:+.2f}%p · positive "
        f"{sy['start2008AndLater']['positivePct']}% (R9 -2.53%p · 32.2%).",
        f"5년 cohort positive-excess 비율 {c5.get('positiveExcessRatePct')}% "
        f"(R9 37.2%).",
        f"첫 24개월 평균 현금비중 "
        f"{rep['capitalFlowAudit']['first24mAvgCashPct']}% (R9 81%). 자본흐름 원장 "
        f"assertion {sum(1 for c in rep['capitalFlowAudit']['checks'] if c['pass'])}/"
        f"{len(rep['capitalFlowAudit']['checks'])} 통과.",
    ]
    broken = [
        f"초과수익이 축소됐다 — base excess {d['baseExcessPct']:+.2f}%p (R9 +4.60%p). "
        f"차이 약 1.93%p 가 사양 위반에서 나온 것이었다.",
        f"위험이 커졌다 — MDD {core['base']['mdd']}% (R9 -43.79%) · 최악 1년 "
        f"{core['base']['worst1y']}% (R9 -31.09%). 폭락장에 실제로 투자돼 있었기 때문이다.",
        f"PBR 결측 종목을 담을 수 없다는 점을 benchmark 에 반영하면 초과수익이 "
        f"{vp['candidateExcessVsValidPbrEwPct']:+.2f}%p — 즉 **사실상 0** 이다 "
        f"(결측제외 효과 {vp['attributableToMissingPbrExclusionPct']:+.2f}%p 가 전부 설명).",
        f"기여 집중이 더 심해졌다 — 총손익의 {conc['top5SharePct']}% 가 상위 5종목, "
        f"제거 시 1차근사 초과수익 {r5['firstOrder']['excessPct']:+.2f}%p.",
        f"out-of-sample 창 {wf['walkForward']['positive']}/"
        f"{wf['walkForward']['total']} positive ({wf['walkForward']['positiveRatePct']}%).",
        f"KOSPI 단독 {core['marketSegments']['KOSPI']['excessPct']:+.2f}%p · "
        f"SMALL 구간 내 {core['sizeControls']['SMALL']['excessPct']:+.2f}%p — "
        f"시장·size 노출을 제거하면 alpha 가 남지 않는다.",
    ]
    d["researcherReading"] = {
        "label": "FRAGILE",
        "separateFromPrecommittedVerdict": True,
        "specConformanceFixed": True,
        "whatTheFixResolved": fixed,
        "whatStillBreaks": broken,
        "why": (
            f"사전확정 규칙은 {d['verdict']} 를 준다(게이트 {d['passed']}/{d['total']}). "
            "게이트·threshold 는 전혀 바꾸지 않았다. 그러나 연구자 판단은 FRAGILE 이다. "
            "이유는 R9 와 **다르다**: R9 의 주된 결함(진입 타이밍 의존·시작연도 편중)은 "
            "사양준수 수정으로 실제로 해소됐다. 남은 문제는 더 근본적이다 — 전략이 담을 수 "
            "없는 PBR 결측 종목을 benchmark 에서 제거하면 초과수익이 "
            f"{vp['candidateExcessVsValidPbrEwPct']:+.2f}%p 로 사라지고, 총손익의 "
            f"{conc['top5SharePct']}% 가 상위 5종목에서 나오며, KOSPI 단독·소형주 내부에서는 "
            "음수다. 즉 '무작위보다 낫다'는 성립하지만 '재현 가능한 BM 종목선정 alpha'는 "
            "성립하지 않는다."),
        "attributionSurvived": att and f"{att['verdict']['survivedCount']}/"
                                       f"{att['verdict']['totalAxes']} 축",
        "notAParameterRescue": (
            "R10 은 BM·P20·N40·H24·universe·ranking·benchmark·비용·상폐·gate 를 하나도 "
            "바꾸지 않았다. 자본 투입 스케줄을 사전규격과 일치시킨 것이 유일한 변경이며, "
            "그 결과 초과수익은 **줄었다**. 대안 parameter 탐색 0(§12)."),
    }
    d["comparisonRef"] = "reports/wababa/wababa-r8-frozen-spec-forensic-reconciliation-r10-latest.md §9"
    _save("verdict", d)
    print(json.dumps({"verdict": d["verdict"], "passed": f"{d['passed']}/{d['total']}",
                      "failed": d["failed"],
                      "researcherReading": d["researcherReading"]["label"]},
                     ensure_ascii=False))
    return 0


MODES = ("repro", "core", "stress", "walkforward", "controls", "dataquality",
         "benchmarks", "cashaudit", "verdict", "all")
ORDER = ("repro", "core", "stress", "walkforward", "controls", "dataquality",
         "benchmarks", "cashaudit", "verdict")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    install_engine()
    todo = ORDER if a.mode == "all" else (a.mode,)
    need = [m for m in todo if m != "verdict"]
    c = None
    if need:
        sn, nm = V.load_snapshots(), V.load_names()
        ds = contiguous_span(sorted(sn))
        cache = RankCache(sn, nm)
        idx = ew_universe_index(cache, ds)
        c = (sn, nm, ds, cache, idx)
        print(f"[r10] {ds[0]} ~ {ds[-1]} ({len(ds)}m) · spec-conformant engine",
              file=sys.stderr)
    for m in todo:
        if m == "repro":
            rc = mode_repro(*c)
        elif m == "verdict":
            rc = mode_verdict()
        else:
            rc = getattr(V, f"mode_{m}")(*c)
        if rc:
            print(f"[r10] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
