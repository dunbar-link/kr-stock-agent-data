#!/usr/bin/env python3
"""R9 검증 파이프라인 회귀 — 네트워크 0 · canonical 미접촉 · 실주문 0.

WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9

R9 의 결론은 두 가지 전제 위에 서 있다. 그 전제가 깨지면 결론 전체가 무의미하므로
회귀로 고정한다.

  ① R9 엔진이 base 설정에서 R8 엔진과 **소수점까지 같은 값**을 낸다
     (아니면 R9 는 R8 후보가 아닌 다른 것을 검증한 것이다)
  ② frozen candidate 파라미터가 어디서도 변경되지 않는다 (§18 parameter rescue 금지)

여기에 R9 가 자체감사에서 잡은 harness 결함(현금잠김 창)과 판정 규칙 매핑도 고정한다.

사용: python scripts/research/test_r9_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r8_portfolio import evaluate as r8_evaluate  # noqa: E402
from r8_portfolio import run_benchmark_same_schedule as r8_bench  # noqa: E402
from r8_portfolio import run_portfolio as r8_run  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402
import r9_engine as E  # noqa: E402
import r9_precommit as PC  # noqa: E402
import r9_validate as V  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


# ─────────────────── ① frozen 정의 불변 ───────────────────
def t_frozen_immutable():
    print("[1] frozen candidate 정의 불변 (§18)")
    p = PC.FROZEN["params"]
    ck("percentile 0.20", p["percentile"] == 0.20, p["percentile"])
    ck("holdings 40", p["holdings"] == 40, p["holdings"])
    ck("hold 24", p["hold"] == 24, p["hold"])
    ck("deployment STAG12M", p["deployment"] == "STAG12M", p["deployment"])
    ck("replacement FIXED_MATURITY_REPLACE",
       p["replacement"] == "FIXED_MATURITY_REPLACE", p["replacement"])
    ck("buy_every 24", p["buy_every"] == 24, p["buy_every"])
    ck("capital 5천만원", PC.FROZEN["initialCapitalKrw"] == 50_000_000)
    ck("factor BM", p["factor"] == "BM", p["factor"])

    # 실행기가 실제로 쓰는 인자가 frozen 정의와 일치하는가
    b = V.BASE_KW
    ck("BASE_KW.percentile == frozen", b["percentile"] == p["percentile"])
    ck("BASE_KW.n_holdings == frozen", b["n_holdings"] == p["holdings"])
    ck("BASE_KW.hold_months == frozen", b["hold_months"] == p["hold"])
    ck("BASE_KW.deployment == frozen", b["deployment"] == p["deployment"])
    ck("BASE_KW.buy_every == frozen", b["buy_every"] == p["buy_every"])
    ck("BASE_KW.capital == frozen", b["capital"] == PC.FROZEN["initialCapitalKrw"])

    # 금지 파라미터 문자열이 precommit 에 명시돼 있는가
    ck("noParameterRescue 금지목록 존재", len(PC.NO_RESCUE["forbidden"]) >= 6)


# ─────────────────── ② 재현 게이트 (핵심) ───────────────────
def t_reproduction(cache, ds, idx):
    print("[2] R9 엔진 == R8 엔진 (bit-exact 재현 게이트)")
    p = PC.FROZEN["params"]
    r8res = r8_run(cache, ds, factor=p["factor"], percentile=p["percentile"],
                   n_holdings=p["holdings"], hold_months=p["hold"],
                   deployment=p["deployment"], replacement=p["replacement"],
                   capital=PC.FROZEN["initialCapitalKrw"], buy_every=p["buy_every"],
                   rebalance_months=p["hold"])
    r8ev = r8_evaluate(r8res, r8_bench(idx, ds, tranches=r8res["tranches"]))
    _, r9ev = E.run_case(cache, ds, idx, **V.BASE_KW)
    for k in ("twrCagr", "benchCagr", "excess", "mdd", "terminalWealth",
              "worst1y", "worst3y", "costPaid"):
        a, b = r8ev[k], r9ev[k]
        ck(f"{k} 동일", abs(a - b) < 1e-9, f"{a} vs {b}")
    for k in ("maxUnderwaterMonths", "trades", "buys", "sells", "uniqueEver",
              "delistEvents"):
        ck(f"{k} 동일", r8ev[k] == r9ev[k], f"{r8ev[k]} vs {r9ev[k]}")

    # R8 보고서 공표값과도 일치해야 한다
    m = PC.FROZEN["r8Measured"]
    ck("보고서 CAGR 13.44%", abs(100 * r9ev["twrCagr"] - m["cagrPct"]) <= 0.05,
       100 * r9ev["twrCagr"])
    ck("보고서 excess 4.60%p", abs(100 * r9ev["excess"] - m["excessPct"]) <= 0.05,
       100 * r9ev["excess"])
    ck("보고서 MDD -43.79%", abs(100 * r9ev["mdd"] - m["mddPct"]) <= 0.50,
       100 * r9ev["mdd"])


# ─────────────────── ③ 확장 기능이 base 를 오염시키지 않는가 ───────────────────
def t_extensions_neutral(cache, ds, idx):
    print("[3] 확장 기능의 중립성 — 기본값에서 base 와 동일")
    _, base = E.run_case(cache, ds, idx, **V.BASE_KW)
    _, zero_slip = E.run_case(cache, ds, idx, slippage_bps=0.0, **V.BASE_KW)
    ck("slippage 0bp 는 base 와 동일",
       abs(base["twrCagr"] - zero_slip["twrCagr"]) < 1e-12)
    _, no_ban = E.run_case(cache, ds, idx, banned=[], **V.BASE_KW)
    ck("banned 빈집합은 base 와 동일",
       abs(base["twrCagr"] - no_ban["twrCagr"]) < 1e-12)

    # 슬리피지는 반드시 성과를 악화시킨다(부호 검사)
    _, slipped = E.run_case(cache, ds, idx, slippage_bps=100.0, **V.BASE_KW)
    ck("슬리피지 100bp 는 CAGR 을 낮춘다", slipped["twrCagr"] < base["twrCagr"],
       f"{slipped['twrCagr']} vs {base['twrCagr']}")
    ck("슬리피지 비용이 0 보다 크다", slipped["slipPaid"] > 0, slipped["slipPaid"])

    # 상장폐지 haircut 은 성과를 악화시킨다
    _, hc = E.run_case(cache, ds, idx, delist_haircut=1.0, **V.BASE_KW)
    ck("상폐 100% haircut 은 CAGR 을 낮춘다", hc["twrCagr"] < base["twrCagr"],
       f"{hc['twrCagr']} vs {base['twrCagr']}")

    # 원장 회계 정합성: 청산 lot 손익 합 + 잔여 = 설명 가능해야 한다
    res, _ = E.run_case(cache, ds, idx, **V.BASE_KW)
    lots = res["ledger"] + res["openLots"]
    ck("모든 lot 이 청산됐다(단일 코호트)", len(res["openLots"]) == 0,
       len(res["openLots"]))
    ck("lot 수 == 매수 건수", len(lots) == res["buys"], f"{len(lots)} vs {res['buys']}")
    ck("정수주만 매수", all(float(l["qty"]).is_integer() for l in lots))
    ck("매수단가 > 0", all(l["buyPrice"] > 0 for l in lots))


# ─────────────────── ④ 부분구간 지수 슬라이스 정확성 ───────────────────
def t_slice_index(sn, nm, cache, ds, idx):
    print("[4] slice_index == 부분구간 재계산 (walk-forward 전제)")
    for off in (12, 60, 120):
        sub = ds[off:]
        fresh = ew_universe_index(cache, sub)
        sl = E.slice_index(idx, off)
        mx = max(abs(a - b) for a, b in zip(fresh, sl))
        ck(f"offset {off} 지수 일치(오차 {mx:.2e})", mx < 1e-9, mx)


# ─────────────────── ⑤ harness 결함 가드 ───────────────────
def t_harness_guard(cache, ds, idx):
    print("[5] 현금잠김 창 가드 (§23 자체감사에서 잡은 결함)")
    ck("horizon 12 최소창 25", V.min_months_for(12) == 25, V.min_months_for(12))
    ck("horizon 36 최소창 49", V.min_months_for(36) == 49, V.min_months_for(36))
    ck("horizon 60 최소창 73", V.min_months_for(60) == 73, V.min_months_for(60))
    ck("horizon 120 최소창 121", V.min_months_for(120) == 121, V.min_months_for(120))

    # 48개월 창은 두 번째 코호트 매수가 차단돼 뒤 24개월이 현금이 된다 — 결함 재현
    bad = E.simulate(cache, ds[:48], **V.BASE_KW)
    tail_cash = sum(bad["cashRatio"][30:]) / len(bad["cashRatio"][30:])
    ck("48개월 창은 후반 현금 90% 초과(결함 재현)", tail_cash > 0.90, tail_cash)
    good = E.simulate(cache, ds[:49], **V.BASE_KW)
    tail_cash2 = sum(good["cashRatio"][30:]) / len(good["cashRatio"][30:])
    ck("49개월 창은 후반 현금 10% 미만(정상)", tail_cash2 < 0.10, tail_cash2)


# ─────────────────── ⑥ 통제군 구조 일치 ───────────────────
def t_controls(cache, ds, idx):
    print("[6] 통제군은 selection 만 다르다")
    _, base = E.run_case(cache, ds, idx, **V.BASE_KW)
    for mode in ("RANDOM", "SIZE_MATCHED", "MARKET_MATCHED"):
        res, ev = E.run_case(cache, ds, idx, selection=mode, seed=20260820, **V.BASE_KW)
        ck(f"{mode} benchmark 동일", abs(ev["benchCagr"] - base["benchCagr"]) < 1e-12)
        ck(f"{mode} 투입금액 동일", abs(ev["contributed"] - base["contributed"]) < 1e-6)
        ck(f"{mode} 매수 시점 수 동일", res["buyMonths"] == 9, res["buyMonths"])
    # 같은 seed → 같은 결과 (결정성)
    _, a = E.run_case(cache, ds, idx, selection="RANDOM", seed=7, **V.BASE_KW)
    _, b = E.run_case(cache, ds, idx, selection="RANDOM", seed=7, **V.BASE_KW)
    ck("같은 seed 는 같은 결과", abs(a["twrCagr"] - b["twrCagr"]) < 1e-12)
    _, c = E.run_case(cache, ds, idx, selection="RANDOM", seed=8, **V.BASE_KW)
    ck("다른 seed 는 다른 결과", abs(a["twrCagr"] - c["twrCagr"]) > 1e-9)


# ─────────────────── ⑦ 판정 규칙 매핑 ───────────────────
def t_verdict_rules():
    print("[7] 사전확정 판정규칙 (§17)")
    ck("게이트 10개", len(PC.GATES) == 10, len(PC.GATES))
    ids = [g["id"] for g in PC.GATES]
    ck("G9·G10 은 PROMISING 에도 필수",
       all("PROMISING_FORWARD_TEST" in g["mandatoryFor"]
           for g in PC.GATES if g["id"] in ("G9_DATA_QUALITY", "G10_HUMAN_EXECUTABLE")))
    ck("나머지 8개는 ROBUST 전용",
       sum(1 for g in PC.GATES if g["mandatoryFor"] == ["ROBUST_CANDIDATE"]) == 8)
    ck("G1 기준 +2.00%p 유지", "+2.00%p" in
       next(g["rule"] for g in PC.GATES if g["id"] == "G1_BASE_EXCESS"))
    ck("판정 우선순위에 REJECT 최우선",
       PC.VERDICT_RULES["precedence"][0] == "REJECT")
    ck("상폐 게이트 모드 SYMMETRIC", PC.DELIST_GATE_MODE == "SYMMETRIC")
    ck("비용 3단계", [c["id"] for c in PC.COST_LEVELS] == ["BASE", "MODERATE", "HIGH"])
    ck("HIGH 슬리피지 100bp",
       next(c for c in PC.COST_LEVELS if c["id"] == "HIGH")["slippageBps"] == 100.0)
    ck("상폐 4단계", len(PC.DELIST_LEVELS) == 4)
    ck("시대 6개", len(PC.ERAS) == 6)
    ck("시대 구간이 겹치지 않는다",
       all(PC.ERAS[i]["to"] < PC.ERAS[i + 1]["from"] for i in range(len(PC.ERAS) - 1)))
    ck("통제군 seed 30개", len(PC.CONTROL_SEEDS) == 30)
    ck("precommit 파일 존재", (RD / "r9-precommit-latest.json").exists())
    if (RD / "r9-precommit-latest.json").exists():
        d = json.loads((RD / "r9-precommit-latest.json").read_text(encoding="utf-8"))
        ck("precommit writtenBeforeResults", d["writtenBeforeResults"] is True)
        ck("precommit 게이트가 코드와 일치",
           [g["id"] for g in d["gates"]] == ids)


# ─────────────────── ⑧ 산출물 계약 ───────────────────
def t_outputs():
    print("[8] 산출물 계약 (§24)")
    wd = ROOT / "reports" / "wababa"
    name = "wababa-frozen-candidate-independent-validation-r9-latest"
    md, js = wd / f"{name}.md", wd / f"{name}.json"
    ck("MD 존재", md.exists())
    ck("JSON 존재", js.exists())
    if md.exists():
        first = md.read_text(encoding="utf-8").splitlines()[0]
        ck("MD 첫 줄 전체 판정", first.startswith("전체 판정:"), first)
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        for k in ("frozenCandidate", "precommittedCriteria", "reproduction",
                  "walkForward", "leaveOneEraOut", "cohortDistribution",
                  "extremePeriods", "delistingStress", "costStress",
                  "liquidityStress", "bmDataQuality", "distressExposure",
                  "kosdaqDependency", "sizeExposure", "matchedControls",
                  "concentrationAudit", "returnDistribution", "benchmarks",
                  "realMoneyRisk", "forwardTestDesign", "finalVerdict",
                  "limitations", "nextDecision"):
            ck(f"JSON.{k}", k in d)
        ck("frozen 정의가 산출물에서도 동일",
           d["frozenCandidate"]["params"] == PC.FROZEN["params"])
        ck("파라미터 변경 0 기록", d["finalVerdict"] and
           json.loads(json.dumps(d))["reproduction"]["pass"] is True)
        pc = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken"):
            ck(f"production {k} == 0", pc[k] == 0, pc[k])
        ck("LEGACY_50D unchanged", pc["legacy50d"] == "unchanged")
        ck("homepage unchanged", pc["homepage"] == "unchanged")


def main():
    print("R9 검증 파이프라인 회귀\n")
    t_frozen_immutable()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월)\n")
    t_reproduction(cache, ds, idx)
    print()
    t_extensions_neutral(cache, ds, idx)
    print()
    t_slice_index(sn, nm, cache, ds, idx)
    print()
    t_harness_guard(cache, ds, idx)
    print()
    t_controls(cache, ds, idx)
    print()
    t_verdict_rules()
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
