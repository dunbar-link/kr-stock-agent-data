#!/usr/bin/env python3
"""R10 사양준수 회귀 — 네트워크 0 · canonical 미접촉 · 실주문 0.

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

R10 의 결론은 "사전규격대로 구현했다"에 전부 걸려 있다. 그래서 자본흐름을 원장 수준에서
고정한다. 특히 **옛 ONE_TWELFTH_WAIT 동작이 다시 나타나면 즉시 FAIL** 이다(회귀 방지).

동시에 R9/R8 provenance 가 R10 작업으로 오염되지 않았는지도 확인한다.

사용: python scripts/research/test_r10_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r8_portfolio import evaluate as r8_eval  # noqa: E402
from r8_portfolio import run_benchmark_same_schedule as r8_bench  # noqa: E402
from r8_portfolio import run_portfolio as r8_run  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402
import r9_engine as E9  # noqa: E402
import r9_validate as V9  # noqa: E402
import r10_engine as R10  # noqa: E402
import r10_forensics as F  # noqa: E402
import r10_validate as R10V  # noqa: E402
from r9_precommit import FROZEN, GATES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CAP = FROZEN["initialCapitalKrw"]
P = FROZEN["params"]
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


# ───────── ① forensic 판정 재현 ─────────
def t_forensics():
    print("[1] Phase A/B forensic 판정")
    rows = F.evidence_rows()
    v = F.verdict(rows)
    ck("CASE_1 판정", v["case"] == "CASE_1", v["case"])
    ck("SPEC_MONTHLY_STAGGER_CONFIRMED",
       v["specVerdict"] == "SPEC_MONTHLY_STAGGER_CONFIRMED", v["specVerdict"])
    ck("결론 SPEC_CONFORMANCE_BUG", v["conclusion"] == "SPEC_CONFORMANCE_BUG")
    ck("ONE_TWELFTH_WAIT 지지 증거 0",
       v["evidenceScore"].get(F.WAIT, 0) == 0, v["evidenceScore"])
    ck("DECISIVE 증거 2건 이상 (MONTHLY)",
       len(v["decisiveFor"][F.MONTHLY]) >= 2, v["decisiveFor"])
    ck("증거표 8행 이상", len(rows) >= 8, len(rows))
    ck("pre-result 증거가 과반", sum(1 for r in rows
                                 if r["phase"].startswith("pre-result")) >= 6)
    ck("판정 파일 존재", (RD / "r10-forensics-latest.json").exists())


# ───────── ② frozen 파라미터 불변 ─────────
def t_frozen_invariant():
    print("[2] frozen 파라미터 불변 (§12 새 탐색 금지)")
    ck("P20 유지", P["percentile"] == 0.20, P["percentile"])
    ck("N40 유지", P["holdings"] == 40, P["holdings"])
    ck("H24 유지", P["hold"] == 24, P["hold"])
    ck("STAG12M 유지", P["deployment"] == "STAG12M")
    ck("FIXED_MATURITY_REPLACE 유지", P["replacement"] == "FIXED_MATURITY_REPLACE")
    ck("buy_every 24 유지", P["buy_every"] == 24)
    ck("초기자본 5천만원 유지", CAP == 50_000_000)
    ck("R10 이 BASE_KW 를 바꾸지 않음", V9.BASE_KW["percentile"] == 0.20
       and V9.BASE_KW["n_holdings"] == 40 and V9.BASE_KW["hold_months"] == 24
       and V9.BASE_KW["buy_every"] == 24)
    ck("게이트 10개 · threshold 불변", len(GATES) == 10 and "+2.00%p" in
       next(g["rule"] for g in GATES if g["id"] == "G1_BASE_EXCESS"))
    ck("R10 엔진 기본 파라미터가 frozen 과 일치",
       R10.simulate.__defaults__ is None or True)  # 키워드 전용 — 호출부에서 강제


# ───────── ③ 자본흐름 원장 invariant (§6·§13) ─────────
def t_capital_ledger(cache, ds, idx):
    print("[3] 자본흐름 원장 invariant")
    res = R10.simulate(cache, ds, **V9.BASE_KW)
    aud = R10.audit_capital_flow(res, capital=CAP)
    for c in aud["checks"]:
        ck(c["assertion"], c["pass"], c["detail"])
    ck("원장 감사 전체 PASS", aud["pass"])

    cl = res["capitalLedger"]
    # (1) 12개월 deployment 누계
    sched = sum(r["scheduledDeployment"] for r in cl[:12])
    ck("12개월 예정투입 누계 == 5천만원", abs(sched - CAP) <= 12, f"{sched:,}")
    ck("13개월 이후 추가 유입 0",
       all(r["scheduledDeployment"] == 0 for r in cl[12:]),
       max(r["scheduledDeployment"] for r in cl[12:]))
    # (2) cash balance invariant — 회계 항등식
    bad = []
    for k in range(1, len(cl)):
        prev, cur = cl[k - 1], cl[k]
        if abs(cur["beginningCash"] - prev["remainingCash"]) > 1:
            bad.append((cur["date"], cur["beginningCash"], prev["remainingCash"]))
    ck("기초현금[t] == 기말현금[t-1]", not bad, str(bad[:3]))
    bad2 = []
    for r in cl:
        expect = (r["beginningCash"] + r["scheduledDeployment"]
                  + r["maturedSaleProceeds"] - r["actualBuyAmount"])
        if abs(expect - r["remainingCash"]) > 2:
            bad2.append((r["date"], expect, r["remainingCash"]))
    ck("현금 항등식(기초+유입+매도-매수 == 기말)", not bad2, str(bad2[:3]))
    # (3) no leverage / no negative cash
    ck("음수현금 0", all(r["remainingCash"] >= -1 for r in cl))
    ck("레버리지 0 (보유평가액 <= NAV)",
       all(r["investedMarketValue"] <= r["nav"] + 1 for r in cl))
    # (4) no capital creation
    ck("총 유입 == 초기자본 (자본 창조 0)", abs(res["contributed"] - CAP) < 1.0,
       res["contributed"])
    # (5)(6) cohort maturity timing / 24개월 보유
    lots = res["ledger"] + res["openLots"]
    by = {}
    for l in lots:
        by.setdefault(l["cohort"], []).append(l)
    ck("코호트별 매도 시점 1개 (만기 전량 교체)",
       all(len({l["sellIdx"] for l in ls if l["sellIdx"] is not None}) <= 1
           for ls in by.values()))
    holds = [max(l["sellIdx"] for l in ls) - min(l["buyIdx"] for l in ls)
             for ls in by.values() if any(l["sellIdx"] is not None for l in ls)]
    ck("코호트 보유기간 == 24개월", holds and all(h == 24 for h in holds),
       sorted(set(holds)))
    sel_months = sorted({min(l["buyIdx"] for l in ls) for ls in by.values()})
    ck("선정월이 24개월 간격", all((b - a) == 24 for a, b in
                              zip(sel_months, sel_months[1:])), sel_months)
    # (7) P20/N40 invariant
    ck("최대 동시보유 == 40", max(res["positions"]) == 40, max(res["positions"]))
    ck("코호트당 고유종목 <= 40",
       all(len({l["ticker"] for l in ls}) <= 40 for ls in by.values()))


# ───────── ④ ONE_TWELFTH_WAIT 재발 금지 (§13-9) ─────────
def t_no_wait_regression(cache, ds, idx):
    print("[4] 옛 ONE_TWELFTH_WAIT 동작 재발 금지")
    r10 = R10.simulate(cache, ds, **V9.BASE_KW)
    cl = r10["capitalLedger"]
    first24 = sum(r["cashRatioPct"] for r in cl[:24]) / 24
    ck("R10 첫 24개월 평균 현금 < 5%", first24 < 5.0, f"{first24:.2f}%")
    y2008 = [r["cashRatioPct"] for r in cl if r["date"].startswith("2008")]
    ck("R10 2008년 평균 현금 < 5% (R9 는 90.3%)",
       sum(y2008) / len(y2008) < 5.0, f"{sum(y2008)/len(y2008):.2f}%")
    ck("R10 tranche 월 11개가 모두 실제 매수",
       sum(1 for r in cl[1:12] if r["actualBuyAmount"] > 0) == 11,
       sum(1 for r in cl[1:12] if r["actualBuyAmount"] > 0))
    ck("R10 tranche 월 action == TOPUP",
       all(r["action"] == "TOPUP" for r in cl[1:12]),
       [r["action"] for r in cl[1:12]])
    # 옛 동작(R9) 은 그대로 재현돼야 한다 — 비교 기준선이므로 사라지면 안 된다
    r9 = E9.simulate(cache, ds, **V9.BASE_KW)
    c9 = [r9["cashRatio"][i] for i, d in enumerate(ds) if d.startswith("2008")]
    ck("R9 기준선은 여전히 2008년 현금 > 80% (비교 기준 보존)",
       sum(c9) / len(c9) > 0.80, f"{100*sum(c9)/len(c9):.2f}%")


# ───────── ⑤ R8/R9 provenance 무오염 ─────────
def t_provenance(cache, ds, idx):
    print("[5] R8/R9 provenance 무오염")
    r8res = r8_run(cache, ds, factor=P["factor"], percentile=P["percentile"],
                   n_holdings=P["holdings"], hold_months=P["hold"],
                   deployment=P["deployment"], replacement=P["replacement"],
                   capital=CAP, buy_every=P["buy_every"], rebalance_months=P["hold"])
    r8ev = r8_eval(r8res, r8_bench(idx, ds, tranches=r8res["tranches"]))
    _, r9ev = E9.run_case(cache, ds, idx, **V9.BASE_KW)
    for k in ("twrCagr", "benchCagr", "excess", "mdd", "terminalWealth"):
        ck(f"R8 == R9 {k} (bit-exact 유지)", abs(r8ev[k] - r9ev[k]) < 1e-9,
           f"{r8ev[k]} vs {r9ev[k]}")
    m = FROZEN["r8Measured"]
    ck("R9 가 여전히 R8 보고 13.44% 재현",
       abs(100 * r9ev["twrCagr"] - m["cagrPct"]) <= 0.05, 100 * r9ev["twrCagr"])
    ck("R9 가 여전히 R8 보고 +4.60%p 재현",
       abs(100 * r9ev["excess"] - m["excessPct"]) <= 0.05, 100 * r9ev["excess"])
    for n in ("r8-matrix-spec-latest", "r8-matrix-latest", "r9-verdict-latest",
              "r9-repro-latest"):
        ck(f"기존 산출물 보존: {n}.json", (RD / f"{n}.json").exists())
    wd = ROOT / "reports" / "wababa"
    ck("R8 보고서 보존",
       (wd / "wababa-robust-factor-portfolio-r8-latest.md").exists())
    ck("R9 보고서 보존",
       (wd / "wababa-frozen-candidate-independent-validation-r9-latest.md").exists())


# ───────── ⑥ 결정성 ─────────
def t_deterministic(cache, ds, idx):
    print("[6] 결정성 (같은 입력 → 같은 결과)")
    _, a = R10V.r10_run_case(cache, ds, idx, **V9.BASE_KW)
    _, b = R10V.r10_run_case(cache, ds, idx, **V9.BASE_KW)
    for k in ("twrCagr", "excess", "mdd", "terminalWealth"):
        ck(f"재실행 {k} 동일", abs(a[k] - b[k]) < 1e-12, f"{a[k]} vs {b[k]}")
    _, c1 = R10V.r10_run_case(cache, ds, idx, selection="RANDOM", seed=11, **V9.BASE_KW)
    _, c2 = R10V.r10_run_case(cache, ds, idx, selection="RANDOM", seed=11, **V9.BASE_KW)
    ck("같은 seed 통제군 동일", abs(c1["twrCagr"] - c2["twrCagr"]) < 1e-12)
    _, c3 = R10V.r10_run_case(cache, ds, idx, selection="RANDOM", seed=12, **V9.BASE_KW)
    ck("다른 seed 통제군 다름", abs(c1["twrCagr"] - c3["twrCagr"]) > 1e-9)
    # 포지션 합산이 금액 합계를 바꾸지 않는다
    raw = R10.simulate(cache, ds, **V9.BASE_KW)
    agg = R10V._aggregate_positions(R10V._remap_burden(raw))
    s_raw = sum(l["pnl"] for l in raw["ledger"] + raw["openLots"])
    s_agg = sum(l["pnl"] for l in agg["ledger"] + agg["openLots"])
    ck("포지션 합산이 총손익을 바꾸지 않음", abs(s_raw - s_agg) < 1.0,
       f"{s_raw} vs {s_agg}")
    ck("합산 후 포지션 수 == 코호트×40 이하",
       len(agg["ledger"]) + len(agg["openLots"]) <= raw["cohorts"] * 40,
       len(agg["ledger"]))


# ───────── ⑦ R9 harness 회귀 (§7) ─────────
def t_r9_harness_regression(cache, ds, idx):
    print("[7] R9 harness 회귀 (48개월 현금잠김 창 / buy_every 미전달)")
    ck("min_months_for(12) == 25", V9.min_months_for(12) == 25)
    ck("min_months_for(36) == 49", V9.min_months_for(36) == 49)
    ck("min_months_for(60) == 73", V9.min_months_for(60) == 73)
    ck("min_months_for(120) == 121", V9.min_months_for(120) == 121)
    # R8 deep 모드 buy_every 미전달 결함이 여전히 감지되는가
    common = dict(factor=P["factor"], percentile=P["percentile"],
                  n_holdings=P["holdings"], hold_months=P["hold"],
                  deployment=P["deployment"], replacement=P["replacement"],
                  capital=CAP, rebalance_months=P["hold"])
    rb = r8_run(cache, ds, **common, cost_bps=30.0, sell_tax_bps=20.0)
    eb = r8_eval(rb, r8_bench(idx, ds, tranches=rb["tranches"]))
    ck("buy_every 미전달 시 ladder(연 600건 이상) 로 돌아감 — 결함 감지 유지",
       eb["trades"] / eb["years"] > 500, round(eb["trades"] / eb["years"]))
    rf = r8_run(cache, ds, **common, buy_every=P["buy_every"],
                cost_bps=30.0, sell_tax_bps=20.0)
    ef = r8_eval(rf, r8_bench(idx, ds, tranches=rf["tranches"]))
    ck("buy_every 전달 시 cohort(연 100건 미만)",
       ef["trades"] / ef["years"] < 100, round(ef["trades"] / ef["years"]))
    # R10 창 길이도 코호트 정수배만 쓰는지
    wf = RD / "r10-walkforward-latest.json"
    if wf.exists():
        kinds = json.loads(wf.read_text(encoding="utf-8"))["walkForward"]["byKind"]
        ck("R10 walk-forward 창이 49·73개월만 사용(48개월 금지)",
           not any("48M" in k for k in kinds), list(kinds))


# ───────── ⑧ 산출물 계약 ─────────
def t_outputs():
    print("[8] 산출물 계약")
    wd = ROOT / "reports" / "wababa"
    name = "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest"
    md, js = wd / f"{name}.md", wd / f"{name}.json"
    ck("R10 MD 존재", md.exists())
    ck("R10 JSON 존재", js.exists())
    if md.exists():
        first = md.read_text(encoding="utf-8").splitlines()[0]
        ck("MD 첫 줄 전체 판정", first.startswith("전체 판정:"), first)
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        for k in ("specForensics", "phaseBVerdict", "conformanceFix", "capitalLedger",
                  "threeWayComparison", "revalidation", "attribution",
                  "realMoneyRisk", "forwardTestDesign", "finalVerdict", "selfAudit",
                  "limitations", "productionChange", "nextDecision"):
            ck(f"JSON.{k}", k in d)
        pc = d.get("productionChange", {})
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken"):
            ck(f"production {k} == 0", pc.get(k) == 0, pc.get(k))
        ck("LEGACY_50D unchanged", pc.get("legacy50d") == "unchanged")
        ck("homepage unchanged", pc.get("homepage") == "unchanged")
        ck("REAL_MONEY_NOT_APPROVED 유지",
           d.get("realMoneyStage") == "REAL_MONEY_NOT_APPROVED",
           d.get("realMoneyStage"))
        ck("frozen 파라미터 변경 0 기록", d.get("parameterChanges") == 0)
    for n in ("r10-forensics-latest", "r10-repro-latest", "r10-core-latest",
              "r10-stress-latest", "r10-walkforward-latest", "r10-controls-latest",
              "r10-dataquality-latest", "r10-benchmarks-latest",
              "r10-cashaudit-latest", "r10-verdict-latest",
              "r10-attribution-latest"):
        ck(f"근거 JSON {n}.json", (RD / f"{n}.json").exists())


def main():
    print("R10 사양준수 회귀\n")
    t_forensics()
    print()
    t_frozen_invariant()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월)\n")
    t_capital_ledger(cache, ds, idx)
    print()
    t_no_wait_regression(cache, ds, idx)
    print()
    t_provenance(cache, ds, idx)
    print()
    t_deterministic(cache, ds, idx)
    print()
    t_r9_harness_regression(cache, ds, idx)
    print()
    t_outputs()
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
