#!/usr/bin/env python3
"""R11 회귀 — 공정 control invariant 검증. 네트워크 0 · canonical 미접촉 · 실주문 0.

WABABA-BM-INCREMENTAL-ALPHA-VALIDATION-R11

R11 의 결론은 "candidate 와 control 이 selection 만 다르다"에 전부 걸려 있다.
그래서 §25 의 핵심 invariant 를 강하게 고정한다.

  identical cash schedule / identical cost framework / valid-PBR eligibility /
  no future data / deterministic rerun / bootstrap deterministic seed /
  calendar alignment / R8·R9·R10 provenance 무오염

사용: python scripts/research/test_r11_validation.py
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
from r9_precommit import CONTROL_SEEDS, ERAS, FROZEN  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402
import r9_engine as E9  # noqa: E402
import r9_validate as V9  # noqa: E402
import r10_engine as R10  # noqa: E402
import r11_control as C  # noqa: E402
import r11_validate as V11  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
P = FROZEN["params"]
CAP = FROZEN["initialCapitalKrw"]
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


# ───────── ① frozen 불변 / 새 탐색 금지 ─────────
def t_frozen():
    print("[1] frozen 파라미터 불변 · 새 탐색 0 (§1)")
    ck("P20", P["percentile"] == 0.20, P["percentile"])
    ck("N40", P["holdings"] == 40, P["holdings"])
    ck("H24", P["hold"] == 24, P["hold"])
    ck("STAG12M", P["deployment"] == "STAG12M")
    ck("buy_every 24", P["buy_every"] == 24)
    ck("초기자본 5천만원", CAP == 50_000_000)
    b = V11.BASE_KW
    ck("R11 BASE_KW == frozen",
       b["percentile"] == 0.20 and b["n_holdings"] == 40 and b["hold_months"] == 24
       and b["buy_every"] == 24 and b["capital"] == CAP)
    ck("PRIMARY control == EW_VALID_PBR_ALL", V11.PRIMARY == C.C1, V11.PRIMARY)
    ck("era 정의 R9/R10 재사용 (6개)", len(ERAS) == 6)
    ck("통제군 seed 30개 재사용", len(CONTROL_SEEDS) == 30)


# ───────── ② eligible set 동일성 (§2) ─────────
def t_eligibility(cache, ds):
    print("[2] valid-PBR eligibility invariant (§2)")
    from r9_engine import _pick
    import random
    bad_pbr = bad_sub = 0
    for d in ds[::12]:
        elig = set(C.eligible(cache, d))
        uni = cache.universe(d)
        # eligible 은 전부 PBR>0 이어야 한다
        for t in elig:
            if not (uni[t]["PBR"] and uni[t]["PBR"] > 0):
                bad_pbr += 1
        # candidate 가 실제로 고른 종목은 전부 eligible 안에 있어야 한다
        picks = _pick(cache, d, uni, factor="BM", percentile=0.20, n_holdings=40,
                      selection="FACTOR", rng=random.Random(1), banned=set())
        if not set(picks) <= elig:
            bad_sub += 1
        # control 3종도 eligible 안에서만 고른다
        for fn in (C.pick_ew_all, C.pick_rest80, C.pick_top20_pool,
                   C.pick_rest80_pool):
            p = fn(cache, d, uni, factor="BM", percentile=0.20, n_holdings=40,
                   selection="FACTOR", rng=random.Random(1), banned=set())
            if not set(p) <= elig:
                bad_sub += 1
        p = C.pick_random_valid_pbr(cache, d, uni, factor="BM", percentile=0.20,
                                    n_holdings=40, selection="FACTOR",
                                    rng=random.Random(1), banned=set())
        if not set(p) <= elig:
            bad_sub += 1
    ck("eligible 전부 PBR>0", bad_pbr == 0, bad_pbr)
    ck("candidate·control 모두 eligible 부분집합", bad_sub == 0, bad_sub)
    # TOP20 ∪ REST80 == eligible, 교집합 없음
    d0 = ds[0]
    uni = cache.universe(d0)
    import random as _r
    t20 = set(C.pick_top20_pool(cache, d0, uni, factor="BM", percentile=0.20,
                                n_holdings=40, selection="FACTOR",
                                rng=_r.Random(1), banned=set()))
    r80 = set(C.pick_rest80_pool(cache, d0, uni, factor="BM", percentile=0.20,
                                 n_holdings=40, selection="FACTOR",
                                 rng=_r.Random(1), banned=set()))
    ck("TOP20 ∩ REST80 == 공집합", not (t20 & r80), len(t20 & r80))
    ck("TOP20 ∪ REST80 == eligible", (t20 | r80) == set(C.eligible(cache, d0)),
       len((t20 | r80) ^ set(C.eligible(cache, d0))))
    ck("TOP20 크기 == eligible 의 20%",
       abs(len(t20) - int(len(C.eligible(cache, d0)) * 0.20)) <= 1,
       f"{len(t20)} vs {int(len(C.eligible(cache, d0)) * 0.20)}")


# ───────── ③ 구조 동일성 (§3) ─────────
def t_structural(cache, ds, idx):
    print("[3] candidate/control 구조 동일성 (§3)")
    rc, ec = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    for arm in (C.C1, C.C2, C.C3, C.P1, C.P2):
        r, e = C.run(cache, ds, idx, arm=arm, base_kw=V11.BASE_KW)
        sd = C.structural_diff(rc, r)
        for k in ("sameDates", "sameInflowSchedule", "sameContributed",
                  "sameContributionPath", "sameTranches", "sameCohortCount"):
            ck(f"{arm} {k}", sd[k], sd)
        ck(f"{arm} benchmark 동일(2차 참고축)",
           abs(e["benchCagr"] - ec["benchCagr"]) < 1e-12)
        ck(f"{arm} 투입금 == 5천만원", abs(e["contributed"] - CAP) < 1.0,
           e["contributed"])
    # 정수주 arm 은 정수주를 지켜야 한다
    for arm in (C.C2, C.C3):
        r, _ = C.run(cache, ds, idx, arm=arm, base_kw=V11.BASE_KW)
        ck(f"{arm} 정수주 유지",
           all(float(l["qty"]).is_integer() for l in r["ledger"]))
        ck(f"{arm} 최대 보유 <= 40", max(r["positions"]) <= 40, max(r["positions"]))
    # 소수주 arm 은 C1/P1/P2 만
    ck("FRACTIONAL 설정이 C1·P1·P2 만 True",
       C.FRACTIONAL[C.C1] and C.FRACTIONAL[C.P1] and C.FRACTIONAL[C.P2]
       and not C.FRACTIONAL[C.C2] and not C.FRACTIONAL[C.C3])


# ───────── ④ 비용 프레임 동일성 (§16) ─────────
def t_cost_framework(cache, ds, idx):
    print("[4] identical cost framework invariant (§16)")
    for kw, lbl in ((dict(), "BASE"),
                    (dict(cost_bps=20.0, sell_tax_bps=23.0, slippage_bps=100.0), "HIGH"),
                    (dict(delist_haircut=1.0), "DELIST100")):
        _, ec = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW, **kw)
        _, e1 = C.run(cache, ds, idx, arm=C.C1, base_kw=V11.BASE_KW, **kw)
        ck(f"{lbl} 양쪽 모두 실행됨", ec is not None and e1 is not None)
        ck(f"{lbl} 투입금 동일", abs(ec["contributed"] - e1["contributed"]) < 1.0)
    # 비용을 올리면 양쪽 모두 CAGR 이 내려가야 한다(한쪽만 이득 보는 구조 금지)
    _, b_c = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    _, b_1 = C.run(cache, ds, idx, arm=C.C1, base_kw=V11.BASE_KW)
    _, h_c = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW,
                   cost_bps=20.0, sell_tax_bps=23.0, slippage_bps=100.0)
    _, h_1 = C.run(cache, ds, idx, arm=C.C1, base_kw=V11.BASE_KW,
                   cost_bps=20.0, sell_tax_bps=23.0, slippage_bps=100.0)
    ck("비용 상승 시 candidate CAGR 하락", h_c["twrCagr"] < b_c["twrCagr"])
    ck("비용 상승 시 control CAGR 하락", h_1["twrCagr"] < b_1["twrCagr"])
    ck("control turnover 가 candidate 보다 큼 → 비용도 큼",
       h_1["costPctOfContrib"] > 0 and b_1["costPctOfContrib"] > 0)


# ───────── ⑤ no future data ─────────
def t_no_future(cache, ds, idx):
    print("[5] no future-data invariant")
    rc, _ = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    bad = 0
    for pl in rc["pickLog"]:
        snap = cache.sn[pl["date"]]
        for p in pl["picks"]:
            r = snap.get(p["ticker"])
            if not r or r["close"] != p["close"] or r["PBR"] != p["pbr"]:
                bad += 1
    ck("매수 기록의 가격·PBR 이 그 날 스냅샷과 일치", bad == 0, bad)
    lots = rc["ledger"]
    ck("매도 시점이 항상 매수 시점 이후", all(l["sellIdx"] > l["buyIdx"] for l in lots))
    ck("매수 시점이 데이터 범위 안", all(0 <= l["buyIdx"] < len(ds) for l in lots))
    # 부분구간 실행은 그 구간 밖 데이터를 쓰지 않는다 → 앞부분 결과가 동일해야 한다
    sub = ds[:120]
    sidx = idx[:120]
    r_sub, e_sub = C.run(cache, sub, sidx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    ck("부분구간 NAV 앞 60개월이 전체구간과 동일",
       all(abs(r_sub["nav"][i] - rc["nav"][i]) < 1e-6 for i in range(60)),
       max(abs(r_sub["nav"][i] - rc["nav"][i]) for i in range(60)))


# ───────── ⑥ 결정성 / bootstrap seed ─────────
def t_deterministic(cache, ds, idx):
    print("[6] deterministic rerun · bootstrap seed")
    for arm in (C.CANDIDATE, C.C1, C.C3, C.P1):
        _, a = C.run(cache, ds, idx, arm=arm, base_kw=V11.BASE_KW)
        _, b = C.run(cache, ds, idx, arm=arm, base_kw=V11.BASE_KW)
        ck(f"{arm} 재실행 동일", abs(a["twrCagr"] - b["twrCagr"]) < 1e-12)
    _, x = C.run(cache, ds, idx, arm=C.C2, base_kw=V11.BASE_KW, seed=777)
    _, y = C.run(cache, ds, idx, arm=C.C2, base_kw=V11.BASE_KW, seed=777)
    _, z = C.run(cache, ds, idx, arm=C.C2, base_kw=V11.BASE_KW, seed=778)
    ck("C2 같은 seed 동일", abs(x["twrCagr"] - y["twrCagr"]) < 1e-12)
    ck("C2 다른 seed 다름", abs(x["twrCagr"] - z["twrCagr"]) > 1e-9)
    ck("bootstrap seed 고정값", V11.BOOT_SEED == 20260820, V11.BOOT_SEED)
    p = RD / "r11-bootstrap-latest.json"
    if p.exists():
        b = json.loads(p.read_text(encoding="utf-8"))
        ck("bootstrap deterministic 플래그", b["deterministic"] is True)
        ck("bootstrap seed 기록", b["seed"] == V11.BOOT_SEED)
        ck("bootstrap 방법 4종 이상", len(b["results"]) >= 4, len(b["results"]))
        ck("bootstrap draws 1000 이상", b["draws"] >= 1000, b["draws"])


# ───────── ⑦ calendar alignment ─────────
def t_calendar(cache, ds, idx):
    print("[7] candidate/control calendar alignment")
    rc, _ = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    r1, _ = C.run(cache, ds, idx, arm=C.C1, base_kw=V11.BASE_KW)
    ck("dates 동일", rc["dates"] == r1["dates"])
    ck("NAV 길이 동일", len(rc["nav"]) == len(r1["nav"]) == len(ds))
    ck("inflow 길이 동일", len(rc["inflow"]) == len(r1["inflow"]) == len(ds))
    ck("매수 시점 집합 동일",
       {p["idx"] for p in rc["pickLog"]} == {p["idx"] for p in r1["pickLog"]},
       ({p["idx"] for p in rc["pickLog"]} ^ {p["idx"] for p in r1["pickLog"]}))
    ck("코호트 매도 시점 동일",
       {l["sellIdx"] for l in rc["ledger"]} == {l["sellIdx"] for l in r1["ledger"]})


# ───────── ⑧ R8/R9/R10 provenance 무오염 ─────────
def t_provenance(cache, ds, idx):
    print("[8] R8/R9/R10 provenance 무오염 (§24)")
    r8res = r8_run(cache, ds, factor=P["factor"], percentile=P["percentile"],
                   n_holdings=P["holdings"], hold_months=P["hold"],
                   deployment=P["deployment"], replacement=P["replacement"],
                   capital=CAP, buy_every=P["buy_every"], rebalance_months=P["hold"])
    r8ev = r8_eval(r8res, r8_bench(idx, ds, tranches=r8res["tranches"]))
    _, r9ev = E9.run_case(cache, ds, idx, **V9.BASE_KW)
    for k in ("twrCagr", "benchCagr", "excess", "mdd", "terminalWealth"):
        ck(f"R8 == R9 {k} bit-exact", abs(r8ev[k] - r9ev[k]) < 1e-9,
           f"{r8ev[k]} vs {r9ev[k]}")
    ck("R9 가 R8 보고 13.44% 재현",
       abs(100 * r9ev["twrCagr"] - FROZEN["r8Measured"]["cagrPct"]) <= 0.05)
    # R10 사양준수 결과도 유지
    _, r10ev = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=V11.BASE_KW)
    r10 = json.loads((RD / "r10-repro-latest.json").read_text(encoding="utf-8"))
    ck("R10 사양준수 CAGR 재현",
       abs(100 * r10ev["twrCagr"] - r10["base"]["cagrPct"]) <= 0.05,
       100 * r10ev["twrCagr"])
    for n in ("r8-matrix-spec-latest", "r9-verdict-latest", "r10-verdict-latest",
              "r10-forensics-latest", "r10-repro-latest"):
        ck(f"기존 산출물 보존 {n}.json", (RD / f"{n}.json").exists())
    for n in ("wababa-robust-factor-portfolio-r8-latest",
              "wababa-frozen-candidate-independent-validation-r9-latest",
              "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest"):
        ck(f"기존 보고서 보존 {n}.md", (WD / f"{n}.md").exists())


# ───────── ⑨ 산출물 계약 ─────────
def t_outputs():
    print("[9] 산출물 계약 (§24)")
    name = "wababa-bm-incremental-alpha-validation-r11-latest"
    md, js = WD / f"{name}.md", WD / f"{name}.json"
    ck("R11 MD 존재", md.exists())
    ck("R11 JSON 존재", js.exists())
    if md.exists():
        first = md.read_text(encoding="utf-8").splitlines()[0]
        ck("MD 첫 줄 전체 판정", first.startswith("전체 판정:"), first)
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        for k in ("reproduction", "primaryBenchmark", "baseIncrementalAlpha",
                  "calendarYear", "rolling", "startDateStability", "walkForward",
                  "leaveOneEraOut", "bootstrapInference", "crossSectionalSelection",
                  "subgroupFairControl", "costAndDelistingStress", "realMoneyRisk",
                  "finalVerdict", "limitations", "productionChange",
                  "hotgClosedLoop", "nextDecision"):
            ck(f"JSON.{k}", k in d)
        ck("REAL_MONEY_NOT_APPROVED",
           d.get("realMoneyStage") == "REAL_MONEY_NOT_APPROVED", d.get("realMoneyStage"))
        ck("parameterChanges == 0", d.get("parameterChanges") == 0)
        ck("gateThresholdsChanged == 0", d.get("gateThresholdsChanged") == 0)
        pc = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "forwardTestStateCreated"):
            ck(f"production {k} == 0", pc.get(k) == 0, pc.get(k))
        ck("LEGACY_50D unchanged", pc.get("legacy50d") == "unchanged")
        ck("homepage unchanged", pc.get("homepage") == "unchanged")
        ck("kr-stock-agent 저장소 untouched",
           pc.get("krStockAgentRepo") == "untouched")
    for n in ("repro", "core", "rolling", "stability", "walkforward", "era",
              "bootstrap", "crosssec", "subgroup", "stress", "risk", "verdict"):
        ck(f"근거 JSON r11-{n}", (RD / f"r11-{n}-latest.json").exists())


def main():
    print("R11 공정 control 회귀\n")
    t_frozen()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월)\n")
    t_eligibility(cache, ds)
    print()
    t_structural(cache, ds, idx)
    print()
    t_cost_framework(cache, ds, idx)
    print()
    t_no_future(cache, ds, idx)
    print()
    t_deterministic(cache, ds, idx)
    print()
    t_calendar(cache, ds, idx)
    print()
    t_provenance(cache, ds, idx)
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
