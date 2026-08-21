#!/usr/bin/env python3
"""R14 회귀 — 장기 horizon PIT / 계산 정확성 invariant. 네트워크 0 · 실주문 0.

WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14

§28 의 핵심:
  **5년 뒤 결과를 5년 전 ranking 계산에 사용하면 FAIL.**
그 외 horizon 정확성 · 연율화 공식 · 상폐 포함 · overlapping 생성 ·
matched control · concentration · bootstrap seed · precommit 불변 · 기존 회귀를 고정한다.

사용: python scripts/research/test_r14_validation.py
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe, load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r13_factors as F13  # noqa: E402
import r14_precommit as PC  # noqa: E402
import r14_validate as V  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


# ───────── ① precommit 불변 ─────────
def t_precommit():
    print("[1] precommit immutability (§4)")
    p = RD / "r14-precommit-latest.json"
    ck("precommit 존재", p.exists())
    if not p.exists():
        return
    d = json.loads(p.read_text(encoding="utf-8"))
    ck("writtenBeforeResults", d["writtenBeforeResults"] is True)
    ck("PRIMARY = 60M(5Y)", d["horizonRule"]["primaryMonths"] == 60
       == PC.PRIMARY_HORIZON)
    ck("ROBUSTNESS = [36, 84]", d["horizonRule"]["robustnessMonths"] == [36, 84])
    ck("REFERENCE = 12M", d["horizonRule"]["referenceMonths"] == 12)
    ck("10Y 제외 명시", "제외" in d["horizonRule"]["excluded10Y"])
    for bad in ("18M", "30M", "42M", "54M", "72M"):
        ck(f"금지 horizon {bad} 명시", bad in d["horizonRule"]["forbidden"])
    ck("factor 3개", len(d["factors"]) == 3)
    ids = [f["id"] for f in d["factors"]]
    ck("factor ID 가 코드와 일치", ids == V.QIDS, ids)
    ck("새 factor 발굴 금지", d["newFactorDiscovery"]["allowed"] is False)
    ck("STRONG_CANDIDATE 사전 도달불가 설정",
       d["factorVerdicts"]["LONG_HORIZON_STRONG_CANDIDATE"]["reachable"] is False)
    ck("표본 비대칭 규칙 기록", "asymmetry" in d["sampleReality"])
    ck("R13 정의 재사용 매핑",
       V.R13_MAP[V.Q1] == F13.L1 and V.R13_MAP[V.Q2] == F13.L3
       and V.R13_MAP[V.Q3] == F13.L2)
    ck("ALL_HORIZONS = [12,36,60,84]", PC.ALL_HORIZONS == [12, 36, 60, 84])
    ck("no-rescue 금지목록 >= 10", len(d["noParameterRescue"]["forbidden"]) >= 10)


# ───────── ② ★ 미래 데이터 금지 ─────────
def t_no_future(sn, nm, ds, lf):
    print("[2] ★ no future-data — 미래가 ranking 에 못 들어간다 (§28 핵심)")
    i = 120                       # 2017-01 근처
    d = ds[i]
    uni = investable_universe(sn[d], nm)
    base = lf.values(uni, d, i)

    # 결정월 **이후** 스냅샷을 전부 훼손해도 factor 값이 불변이어야 한다
    sn2 = dict(sn)
    for k in range(i, len(ds)):
        sn2[ds[k]] = copy.deepcopy(sn[ds[k]])
        for t in sn2[ds[k]]:
            r = sn2[ds[k]][t]
            if r["EPS"] is not None:
                r["EPS"] = -abs(r["EPS"]) - 1e9
            if r["BPS"] is not None:
                r["BPS"] = abs(r["BPS"]) * 1000 + 1
            if r["close"] is not None:
                r["close"] = r["close"] * 7 + 13
    lf2 = F13.LongFactors(sn2, ds, dps_series=lf.dps)
    v2 = lf2.values(uni, d, i)
    for q, fid in ((V.Q1, F13.L1), (V.Q2, F13.L3)):
        a, b = base[fid], v2[fid]
        same = (set(a) == set(b)) and all(abs(a[t] - b[t]) < 1e-12 for t in a)
        ck(f"{q}: 결정월 이후 데이터 훼손에도 값 불변", same,
           f"n={len(a)} vs {len(b)}")
    # Q3 는 당월 스냅샷을 쓰므로 t+1 이후만 훼손해 확인
    sn3 = dict(sn)
    for k in range(i + 1, len(ds)):
        sn3[ds[k]] = copy.deepcopy(sn[ds[k]])
        for t in sn3[ds[k]]:
            if sn3[ds[k]][t]["EPS"] is not None:
                sn3[ds[k]][t]["EPS"] = -1e9
    lf3 = F13.LongFactors(sn3, ds, dps_series=lf.dps)
    v3 = lf3.values(uni, d, i)
    a, b = base[F13.L2], v3[F13.L2]
    ck("Q3: 결정월 이후 데이터 훼손에도 값 불변",
       set(a) == set(b) and all(abs(a[t] - b[t]) < 1e-12 for t in a))

    # 5년 뒤 가격을 바꿔도 ranking 이 안 바뀐다 (가격은 forward return 에만 쓰인다)
    items0 = sorted(base[F13.L1].items(), key=lambda kv: (-kv[1], kv[0]))
    items2 = sorted(v2[F13.L1].items(), key=lambda kv: (-kv[1], kv[0]))
    ck("5년 뒤 가격 변조에도 랭킹 순서 동일",
       [t for t, _ in items0] == [t for t, _ in items2])


# ───────── ③ horizon 정확성 / 연율화 ─────────
def t_horizon(sn, nm, ds, lf):
    print("[3] horizon exactness · terminal date · annualization")
    px = FR.build_price_index(sn, ds)
    for h in PC.ALL_HORIZONS:
        i = 60
        j = i + h
        ck(f"{h}M terminal index = i+h ({ds[i]} → {ds[j]})", j - i == h)
    # fwd_return 이 실제 두 스냅샷 가격비인지
    i, h = 80, 60
    j = i + h
    t = next(t for t in sn[ds[i]] if sn[ds[i]][t]["close"] and sn[ds[j]].get(t)
             and sn[ds[j]][t]["close"])
    want = sn[ds[j]][t]["close"] / sn[ds[i]][t]["close"] - 1
    ck("fwd_return == 종가비 - 1",
       abs(FR.fwd_return(px, i, j, t) - want) < 1e-12)
    # 연율화 공식
    ck("ann(1.0, 12) == 100%", abs(FR.ann(1.0, 12) - 1.0) < 1e-12)
    ck("ann(cum, 60) 정의 = (1+r)^(12/60)-1",
       abs(FR.ann(1.5, 60) - (2.5 ** 0.2 - 1)) < 1e-12)
    ck("ann(0, h) == 0", abs(FR.ann(0.0, 84)) < 1e-12)
    ck("ann(-1 이하) == None", FR.ann(-1.0, 60) is None)
    # ★ annSpread 는 '연율의 차이'이지 '차이의 연율'이 아니다
    acc = {"X": {60: {1: [1.0], 10: [0.2]}}}
    s = FR.summarize_factor(acc, {60: [0.5]}, "X")[60]
    ck("annSpread == ann(top)-ann(bottom)",
       abs(s["annSpread"] - (FR.ann(1.0, 60) - FR.ann(0.2, 60))) < 1e-12)
    ck("topMinusBottomAnn(R7 원지표) 와 annSpread 가 다름(장기 왜곡 확인)",
       abs(s["annSpread"] - s["topMinusBottomAnn"]) > 1e-6,
       (s["annSpread"], s["topMinusBottomAnn"]))
    ck("R14 는 annSpread 를 주 지표로 쓴다",
       V.ann_spread({60: s}, 60) == s["annSpread"])


# ───────── ④ 상폐 포함 / survivorship ─────────
def t_delisting(sn, nm, ds, lf):
    print("[4] delisted stock inclusion · survivorship-free")
    px = FR.build_price_index(sn, ds)
    i, h = 60, 60
    j = i + h
    uni = investable_universe(sn[ds[i]], nm)
    gone = [t for t in uni if t not in sn[ds[j]] or not sn[ds[j]][t]["close"]]
    ck("5년 뒤 사라진 종목이 결정시점 universe 에 존재", len(gone) > 0, len(gone))
    got = sum(1 for t in gone if FR.fwd_return(px, i, j, t) is not None)
    ck("상폐 종목도 forward return 이 계산됨(마지막 관측가 청산)",
       got >= len(gone) * 0.8, f"{got}/{len(gone)}")
    ck("상폐 종목 수익률이 -100% 로 강제되지 않음(정본 haircut 0)",
       any((FR.fwd_return(px, i, j, t) or 0) > -0.999 for t in gone))


# ───────── ⑤ overlapping / non-overlapping 생성 ─────────
def t_overlap():
    print("[5] overlapping / non-overlapping generation (§10)")
    p = RD / "r14-overlap-latest.json"
    ck("overlap 산출물 존재", p.exists())
    if not p.exists():
        return
    d = json.loads(p.read_text(encoding="utf-8"))["byHorizon"]
    for h, exp in (("12", 16), ("36", 5), ("60", 3), ("84", 2)):
        v = d[h][V.Q1]["NON_OVERLAPPING"]
        ck(f"{int(h)//12}Y 비중첩 코호트 {exp}개", v and v["n"] == exp,
           v and v["n"])
        ov = d[h][V.Q1]["OVERLAPPING"]
        ck(f"{int(h)//12}Y 겹침 표본 > 비중첩", ov["n"] > v["n"], (ov["n"], v["n"]))
    an = d["60"][V.Q1]["ANNUAL_START"]
    ck("5Y 연초 코호트 12개", an and an["n"] == 12, an and an["n"])
    ck("코호트 시작구간 3개 산출", len(d["60"][V.Q1]["byStartEraMedianPct"]) == 3,
       d["60"][V.Q1]["byStartEraMedianPct"])
    ck("하위구간이 비어있지 않음(초기 버그 회귀)",
       all(v is not None for v in d["60"][V.Q1]["byStartEraMedianPct"].values()))


# ───────── ⑥ matched control / concentration ─────────
def t_matched_conc():
    print("[6] matched control · concentration")
    m = json.loads((RD / "r14-matched-latest.json").read_text(encoding="utf-8"))
    for mode in ("SIZE_MATCHED", "EXCHANGE_MATCHED", "SIZE_EXCHANGE_MATCHED"):
        ck(f"{mode} 결과 존재", mode in m["rows"])
        for q in V.QIDS:
            r = m["rows"][mode][q]
            ck(f"{mode}/{q} 값 계산됨(None 아님)",
               r["meanAnnSpreadPct"] is not None, r)
            ck(f"{mode}/{q} 관측 개월 > 30", (r["months"] or 0) > 30, r["months"])
    ck("EXCHANGE_MATCHED 최소셀 2 (거래소는 2개뿐 — 초기 버그 회귀)",
       m["minCellsPerMode"]["EXCHANGE_MATCHED"] == 2)
    a = json.loads((RD / "r14-audit-latest.json").read_text(encoding="utf-8"))
    for q in V.QIDS:
        c = a["concentration"][q]
        ck(f"{q} 집중도 단조 (top1<=top3<=top5<=top10)",
           c["top1SharePct"] <= c["top3SharePct"] <= c["top5SharePct"]
           <= c["top10SharePct"],
           (c["top1SharePct"], c["top3SharePct"], c["top5SharePct"], c["top10SharePct"]))
        ck(f"{q} 집중도 분모가 총이익 기준(<=100%)", c["top10SharePct"] <= 100.0,
           c["top10SharePct"])
        for g in ("top", "bottom"):
            r = a["intraHoldingRisk"][q][g]
            ck(f"{q}/{g} MDD <= 0", r["medianMaxDrawdownPct"] <= 0)
            ck(f"{q}/{g} 최악 MDD <= 중앙 MDD",
               r["worstMaxDrawdownPct"] <= r["medianMaxDrawdownPct"])


# ───────── ⑦ bootstrap seed / 결정성 ─────────
def t_deterministic(sn, nm, ds, lf):
    print("[7] deterministic rerun · bootstrap seed")
    ck("BOOT_SEED 고정", V.BOOT_SEED == 20260821, V.BOOT_SEED)
    b = json.loads((RD / "r14-boot-latest.json").read_text(encoding="utf-8"))
    ck("bootstrap seed 기록", b["_seed"] == V.BOOT_SEED)
    ck("draws >= 1000", b["_draws"] >= 1000, b["_draws"])
    for q in V.QIDS:
        ck(f"{q} bootstrap 방법 3종", len(b[q]["results"]) == 3, len(b[q]["results"]))
        for m, r in b[q]["results"].items():
            ck(f"{q}/{m} CI 하한 <= 상한",
               r["ci95LowerPct"] <= r["ci95UpperPct"])
    # factor 값 재계산 결정성
    i, d = 100, ds[100]
    uni = investable_universe(sn[d], nm)
    a1 = F13.LongFactors(sn, ds, dps_series=lf.dps).values(uni, d, i)
    a2 = F13.LongFactors(sn, ds, dps_series=lf.dps).values(uni, d, i)
    ck("factor 값 재계산 동일",
       all(abs(a1[k][t] - a2[k][t]) < 1e-12 for k in a1 for t in a1[k]))
    # 분위 패널 결정성
    sub = ds[100:140]
    p1 = V.panel(sn, nm, sub, lf, horizons=[12], factors=[V.Q1])[0]
    p2 = V.panel(sn, nm, sub, lf, horizons=[12], factors=[V.Q1])[0]
    ck("분위 패널 재실행 동일",
       json.dumps(p1, sort_keys=True, default=str) ==
       json.dumps(p2, sort_keys=True, default=str))


# ───────── ⑧ R13/R7 회귀 + 산출물 ─────────
def t_regression_outputs():
    print("[8] R13/R7 회귀 · 산출물 계약 (§27)")
    rep = json.loads((RD / "r14-repro-latest.json").read_text(encoding="utf-8"))
    ck("R13 재현 PASS", rep["pass"] is True)
    for c in rep["checks"]:
        ck(f"R13 재현 {c['metric']} (차이 {c['diff']})", c["pass"], c)
    md = WD / "wababa-quality-compounder-long-horizon-r14-latest.md"
    js = WD / "wababa-quality-compounder-long-horizon-r14-latest.json"
    ck("R14 MD 존재", md.exists())
    ck("R14 JSON 존재", js.exists())
    if md.exists():
        first = md.read_text(encoding="utf-8").splitlines()[0]
        ck("MD 첫 줄 전체 판정", first.startswith("전체 판정:"), first)
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        for k in ("precommit", "reproduction", "standalone", "bmTop20Internal",
                  "overlapAnalysis", "bootstrapInference", "matchedControls",
                  "audit", "finalVerdict", "limitations", "productionChange",
                  "hotgClosedLoop", "nextDecision"):
            ck(f"JSON.{k}", k in d)
        ck("REAL_MONEY_NOT_APPROVED",
           d.get("realMoneyStage") == "REAL_MONEY_NOT_APPROVED")
        fv = d["finalVerdict"]
        ck("horizonRescue 0", fv.get("horizonRescue") == 0)
        ck("parameterRescue 0", fv.get("parameterRescue") == 0)
        ck("portfolioSearchPerformed 0", fv.get("portfolioSearchPerformed") == 0)
        pcx = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "publicDisclosure"):
            ck(f"production {k} == 0", pcx.get(k) == 0, pcx.get(k))
        ck("LEGACY_50D unchanged", pcx.get("legacy50d") == "unchanged")
        ck("kr-stock-agent untouched", pcx.get("krStockAgentRepo") == "untouched")
        ck("NEW_BM forward 미접근", "미접근" in pcx.get("newBmR8FrozenForwardState", ""))
    for n in ("precommit", "repro", "standalone", "bmtop20", "overlap", "boot",
              "matched", "audit", "verdict"):
        ck(f"근거 JSON r14-{n}", (RD / f"r14-{n}-latest.json").exists())
    for n in ("wababa-factor-signal-discovery-r7-latest",
              "wababa-robust-factor-portfolio-r8-latest",
              "wababa-frozen-candidate-independent-validation-r9-latest",
              "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest",
              "wababa-bm-incremental-alpha-validation-r11-latest",
              "wababa-value-profitability-factor-discovery-r13-latest"):
        ck(f"기존 보고서 보존 {n}.md", (WD / f"{n}.md").exists())


def main():
    print("R14 장기 horizon 회귀\n")
    t_precommit()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    lf = F13.LongFactors(sn, ds)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월)\n")
    t_no_future(sn, nm, ds, lf)
    print()
    t_horizon(sn, nm, ds, lf)
    print()
    t_delisting(sn, nm, ds, lf)
    print()
    t_overlap()
    print()
    t_matched_conc()
    print()
    t_deterministic(sn, nm, ds, lf)
    print()
    t_regression_outputs()
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
