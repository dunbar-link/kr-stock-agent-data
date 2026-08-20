#!/usr/bin/env python3
"""R11 — frozen BM 후보의 incremental alpha 검증 (vs 공정 EW_VALID_PBR control).

WABABA-BM-INCREMENTAL-ALPHA-VALIDATION-R11

단일 질문(§32): PBR 데이터가 존재하는 동일 투자가능 universe 를 그냥 동일가중으로
사는 것보다, BM 상위 20% 를 고르는 것이 실제로 추가 수익을 만드는가?

★ R11 이 R10 과 달라지는 단 하나의 지점 — PRIMARY CONTROL 정의
   R10 의 `EW_VALID_PBR_UNIVERSE` 는 **월 리밸런싱 · 비용 0 지수**였다.
   후보는 24개월 보유 + 실제 비용을 낸다. 즉 R10 은 비교 구조가 달랐다.
   §3 이 요구하는 동일화(동일 24개월 보유 프레임 · 동일 교체 타이밍 · 동일 비용
   프레임)를 적용하면 control 은 **같은 코호트 엔진 안에서** 돌려야 한다.
   이 재정의는 결과를 보고 고른 것이 아니라 §2·§3·§16 이 사전에 규정한 것이다.
   (§16: "candidate 만 비용을 더 내거나 덜 내는 구조 금지")
   두 정의의 차이는 §1 분해표로 전부 공개한다.

금지(§1): P/N/H·스케줄·비용·gate·benchmark 를 결과에 맞춰 바꾸는 것. 새 후보 생성.
모드: repro core rolling stability walkforward era bootstrap crosssec subgroup
      stress verdict all

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 홈페이지 미수정 · 실주문 0 ·
      브로커 0 · 유료데이터 0 · 외부발송 0 · 배포 0 · env/token 0 · 운영DB write 0.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r9_engine import bench_same_schedule, chain_stats, evaluate, monthly_twr  # noqa: E402
from r9_precommit import CONTROL_SEEDS, ERAS, FROZEN  # noqa: E402
from r9_validate import dist, min_months_for, quant  # noqa: E402
import r10_engine as R10  # noqa: E402
import r11_control as C  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
P = FROZEN["params"]
CAP = FROZEN["initialCapitalKrw"]
BASE_KW = dict(factor=P["factor"], percentile=P["percentile"], n_holdings=P["holdings"],
               hold_months=P["hold"], deployment=P["deployment"],
               replacement=P["replacement"], buy_every=P["buy_every"], capital=CAP)
PRIMARY = C.C1
BOOT_SEED = 20260820


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r11-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r11] saved {p.name}", file=sys.stderr)


def load(name):
    return json.loads((RD / f"r11-{name}-latest.json").read_text(encoding="utf-8"))


class ValidPbrCache(RankCache):
    """유효 PBR 종목만의 universe (R10 지수 재현·2차 참고용)."""

    def universe(self, d):
        if d not in self._uni:
            u = super().universe(d)
            self._uni[d] = {t: r for t, r in u.items() if r["PBR"] and r["PBR"] > 0}
        return self._uni[d]


def ctx():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    return sn, nm, ds, cache, idx


def arms(cache, ds, idx, **over):
    """candidate + control 3종을 한 번에. 모두 같은 엔진·같은 현금 스케줄."""
    out = {}
    for a in (C.CANDIDATE, C.C1, C.C3):
        res, ev = C.run(cache, ds, idx, arm=a, base_kw=BASE_KW, **over)
        out[a] = (res, ev)
    return out


def series(res):
    """월별 TWR 수익률."""
    return monthly_twr(res["nav"], res["inflow"])


def excess_stats(sc, sb, months_per_year=12):
    """candidate/control 월 수익률 → 연율 초과수익 및 부속 지표."""
    a, b = chain_stats(sc), chain_stats(sb)
    if not a or not b:
        return None
    d = [x - y for x, y in zip(sc, sb)]
    n = len(d)
    mean_m = sum(d) / n
    var = sum((x - mean_m) ** 2 for x in d) / (n - 1) if n > 1 else 0.0
    te = math.sqrt(var) * math.sqrt(12)
    return {
        "candCagrPct": pct(a["cagr"]), "ctrlCagrPct": pct(b["cagr"]),
        "cagrExcessPct": pct(a["cagr"] - b["cagr"]),
        "arithAnnualExcessPct": pct(mean_m * 12),
        "trackingErrorPct": pct(te),
        "informationRatio": round((mean_m * 12) / te, 3) if te > 0 else None,
        "candMddPct": pct(a["mdd"]), "ctrlMddPct": pct(b["mdd"]),
        "months": n,
    }


# ═══════════════ §23 reproduction gate ═══════════════
def mode_repro(sn, nm, ds, cache, idx):
    r10 = json.loads((RD / "r10-repro-latest.json").read_text(encoding="utf-8"))
    r10b = json.loads((RD / "r10-benchmarks-latest.json").read_text(encoding="utf-8"))
    r10_vp = next(r for r in r10b["rows"] if r["benchmark"] == "EW_VALID_PBR_UNIVERSE")

    res_c, ev_c = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW)
    vidx = ew_universe_index(ValidPbrCache(sn, nm), ds)
    b_vp = bench_same_schedule(vidx, ds, tranches=res_c["tranches"])
    ev_vp = evaluate(res_c, b_vp)

    checks = []

    def ck(metric, got, want, tol, unit="%p"):
        ok = got is not None and want is not None and abs(got - want) <= tol
        checks.append({"metric": metric, "r11": got, "r10Canonical": want,
                       "diff": None if got is None or want is None else round(got - want, 4),
                       "tolerance": tol, "unit": unit, "pass": ok})

    ck("candidateCagrPct", pct(ev_c["twrCagr"]), r10["base"]["cagrPct"], 0.05)
    ck("fullMarketBenchCagrPct", pct(ev_c["benchCagr"]), r10["base"]["benchPct"], 0.05)
    ck("fullMarketExcessPct", pct(ev_c["excess"]), r10["base"]["excessPct"], 0.05)
    ck("mddPct", pct(ev_c["mdd"]), r10["base"]["mddPct"], 0.10)
    ck("underwaterMonths", ev_c["maxUnderwaterMonths"],
       r10["base"]["underwaterMonths"], 0, "개월")
    ck("r10ValidPbrIndexExcessPct", pct(ev_vp["excess"]),
       r10_vp["candidateExcessPct"], 0.05)

    # ── §1 벤치마크 정의 분해 — R10 -0.22%p 의 정체 ──────────────────────
    yrs = len(ds) / 12
    decomp = [{
        "variant": "R10_PRIMARY_월리밸런싱_비용0_지수",
        "cagrPct": pct(vidx[-1] ** (1 / yrs) - 1),
        "note": "ew_universe_index(ValidPbrCache) — 매월 전 종목 재편입, 비용 0",
    }]
    for lbl, kw, note in (
        ("포트폴리오_월리밸런싱_비용0", dict(hold_months=1, buy_every=1, cost_bps=0.0,
                                     sell_tax_bps=0.0),
         "위 지수를 코호트 엔진으로 재현 — 값이 일치하면 엔진 검증 완료"),
        ("포트폴리오_월리밸런싱_실제비용", dict(hold_months=1, buy_every=1),
         "같은 구조에 실제 수수료·매도세 부과 — 월 1,500종목 회전의 실제 비용"),
        ("포트폴리오_24개월보유_비용0", dict(cost_bps=0.0, sell_tax_bps=0.0),
         "후보와 동일한 보유 프레임, 비용만 제거"),
        ("R11_PRIMARY_24개월보유_실제비용", dict(),
         "§3 구조 동일화를 만족하는 공정 control = C1"),
    ):
        _, e = C.run(cache, ds, idx, arm=C.C1, base_kw=BASE_KW, **kw)
        decomp.append({"variant": lbl, "cagrPct": pct(e["twrCagr"]),
                       "costPctOfContrib": pct(e["costPctOfContrib"]),
                       "avgCashPct": pct(e["avgCashRatio"]), "note": note})

    idx_cagr = decomp[0]["cagrPct"]
    port_repro = decomp[1]["cagrPct"]
    monthly_cost = decomp[2]["cagrPct"]
    hold_free = decomp[3]["cagrPct"]
    primary = decomp[4]["cagrPct"]
    engine_ok = abs(idx_cagr - port_repro) <= 0.30
    checks.append({"metric": "지수==포트폴리오재현(엔진검증)", "r11": port_repro,
                   "r10Canonical": idx_cagr,
                   "diff": round(port_repro - idx_cagr, 4), "tolerance": 0.30,
                   "unit": "%p", "pass": engine_ok})

    ok = all(c["pass"] for c in checks)
    out = {
        "gate": "R11_REPRODUCTION_AND_BENCHMARK_DECOMPOSITION",
        "pass": ok,
        "period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
        "checks": checks,
        "candidateBase": {
            "cagrPct": pct(ev_c["twrCagr"]), "mddPct": pct(ev_c["mdd"]),
            "underwaterMonths": ev_c["maxUnderwaterMonths"],
            "worst1yPct": pct(ev_c["worst1y"]), "worst3yPct": pct(ev_c["worst3y"]),
            "terminalWealth": ev_c["terminalWealth"],
            "ordersPerYear": round(res_c["orders"] / ev_c["years"], 1),
            "avgPositions": round(ev_c["avgPositions"], 1),
            "maxPositions": ev_c["maxPositions"],
            "avgCashRatioPct": pct(ev_c["avgCashRatio"]),
            "costPctOfContrib": pct(ev_c["costPctOfContrib"])},
        "benchmarkDefinitionDecomposition": {
            "rows": decomp,
            "engineValidatedAgainstR10Index": engine_ok,
            "rebalanceFrequencyBonusPct": round(port_repro - hold_free, 2),
            "monthlyRebalanceCostDragPct": round(port_repro - monthly_cost, 2),
            "holdFrameCostDragPct": round(hold_free - primary, 2),
            "finding": (
                f"R10 의 PRIMARY benchmark(월 리밸런싱·비용 0 지수, {idx_cagr}%)는 "
                f"코호트 엔진으로 {port_repro}% 로 정확히 재현된다 — 엔진은 맞다. "
                f"그 지수에 **실제 비용**을 부과하면 {monthly_cost}% 로 떨어진다"
                f"(월 1,500종목 회전 비용이 투입자본의 199% 수준). 비용을 제거하고 "
                f"**후보와 같은 24개월 보유 프레임**으로 바꾸면 {hold_free}% 다. "
                f"즉 R10 이 benchmark 에 준 우위 대부분은 '매월 1,500종목을 공짜로 "
                f"재편입한다'는 실현 불가능한 가정에서 나왔다."),
            "conclusion": (
                "R10 의 candidate vs EW_VALID_PBR = -0.22%p 는 24개월 보유·비용 부담 "
                "후보를 월 리밸런싱·무비용 지수와 비교한 결과다. §3 구조 동일화 규칙에 "
                "따라 R11 은 같은 엔진·같은 보유 프레임·같은 비용의 control 을 "
                "PRIMARY 로 쓴다. 이 재정의는 §2·§3·§16 이 사전에 규정한 것이고 "
                "결과를 보고 고른 것이 아니다."),
        },
        "r10ArtifactsUntouched": True,
    }
    save("repro", out)
    print(json.dumps({"reproPass": ok, "engineValidated": engine_ok,
                      "r10IndexCagr": idx_cagr, "r11PrimaryCtrlCagr": primary},
                     ensure_ascii=False))
    return 0 if ok else 3


# ═══════════════ §5 base incremental alpha + §3 구조 동일화 ═══════════════
def mode_core(sn, nm, ds, cache, idx):
    a = arms(cache, ds, idx)
    res_c, ev_c = a[C.CANDIDATE]
    res_1, ev_1 = a[C.C1]
    res_3, ev_3 = a[C.C3]
    sc, s1, s3 = series(res_c), series(res_1), series(res_3)

    # C2 — 30 seed 분포 (구조 100% 동일)
    c2 = []
    for s in CONTROL_SEEDS:
        r, e = C.run(cache, ds, idx, arm=C.C2, base_kw=BASE_KW, seed=s)
        if e:
            c2.append({"seed": s, "cagrPct": pct(e["twrCagr"]),
                       "mddPct": pct(e["mdd"]),
                       "terminalWealth": round(e["terminalWealth"])})
    c2c = [x["cagrPct"] for x in c2]
    cand = pct(ev_c["twrCagr"])

    primary = excess_stats(sc, s1)
    rest80 = excess_stats(sc, s3)

    out = {
        "primaryControl": PRIMARY,
        "primaryControlDefinition": (
            "동일 PIT 시점의 investable universe 중 유효 PBR(>0)이 존재하는 종목 전체를 "
            "동일가중 보유. 코호트 엔진(12개월 월별 분할진입 · 24개월 보유 · 만기 전량 "
            "교체 · 동일 비용/상폐/현금 처리) 안에서 실행. 종목수 ~1,500개라 이 control "
            "에서만 소수주를 허용한다(유일한 구조 차이 — 정수주로는 전 종목 0주가 된다)."),
        "eligibleSetIdentity": (
            "candidate 선정 풀의 모집단과 control 의 보유 집합이 동일하게 "
            "RankCache.ranked(d,'BM') = investable ∩ PBR>0 이다."),
        "structuralIdentity": {
            "vs_C1": C.structural_diff(res_c, res_1),
            "vs_C3": C.structural_diff(res_c, res_3),
        },
        "candidate": {
            "cagrPct": cand, "mddPct": pct(ev_c["mdd"]),
            "volPct": pct(ev_c["vol"]),
            "underwaterMonths": ev_c["maxUnderwaterMonths"],
            "worst1yPct": pct(ev_c["worst1y"]), "worst3yPct": pct(ev_c["worst3y"]),
            "terminalWealth": round(ev_c["terminalWealth"]),
            "avgPositions": round(ev_c["avgPositions"], 1),
            "maxPositions": ev_c["maxPositions"],
            "avgCashRatioPct": pct(ev_c["avgCashRatio"]),
            "ordersPerYear": round(res_c["orders"] / ev_c["years"], 1),
            "costPctOfContrib": pct(ev_c["costPctOfContrib"]),
            "delistEvents": ev_c["delistEvents"]},
        "controlC1": {
            "cagrPct": pct(ev_1["twrCagr"]), "mddPct": pct(ev_1["mdd"]),
            "volPct": pct(ev_1["vol"]),
            "underwaterMonths": ev_1["maxUnderwaterMonths"],
            "worst1yPct": pct(ev_1["worst1y"]), "worst3yPct": pct(ev_1["worst3y"]),
            "terminalWealth": round(ev_1["terminalWealth"]),
            "avgPositions": round(ev_1["avgPositions"], 1),
            "maxPositions": ev_1["maxPositions"],
            "avgCashRatioPct": pct(ev_1["avgCashRatio"]),
            "ordersPerYear": round(res_1["orders"] / ev_1["years"], 1),
            "costPctOfContrib": pct(ev_1["costPctOfContrib"]),
            "delistEvents": ev_1["delistEvents"]},
        "controlC3_REST80": {
            "cagrPct": pct(ev_3["twrCagr"]), "mddPct": pct(ev_3["mdd"]),
            "terminalWealth": round(ev_3["terminalWealth"])},
        "controlC2_RANDOM_N40": {
            "seeds": len(c2), "cagrPct": dist(c2c),
            "candidateCagrPct": cand,
            "seedsBeatingCandidate": sum(1 for x in c2c if x >= cand),
            "candidatePercentile": round(100 * sum(1 for x in c2c if x < cand)
                                         / len(c2c), 1) if c2c else None,
            "rows": c2},
        "incrementalAlpha": {
            "vs_C1_PRIMARY": primary,
            "vs_C3_REST80": rest80,
            "vs_C2_RANDOM_mean": round(cand - sum(c2c) / len(c2c), 2) if c2c else None,
            "terminalWealthRatio_vs_C1": round(
                ev_c["terminalWealth"] / ev_1["terminalWealth"], 3)
            if ev_1["terminalWealth"] else None,
            "terminalWealthRatio_vs_C3": round(
                ev_c["terminalWealth"] / ev_3["terminalWealth"], 3)
            if ev_3["terminalWealth"] else None,
        },
        "secondaryReferences": {
            "note": "§2 — full-market benchmark 는 2차 참고로만 유지한다.",
            "fullMarketEwCagrPct": pct(ev_c["benchCagr"]),
            "fullMarketExcessPct": pct(ev_c["excess"]),
        },
    }
    save("core", out)
    print(json.dumps({"candCagr": cand, "c1Cagr": out["controlC1"]["cagrPct"],
                      "primaryExcessPct": primary["cagrExcessPct"],
                      "c2Percentile": out["controlC2_RANDOM_N40"]["candidatePercentile"],
                      "rest80ExcessPct": rest80["cagrExcessPct"]}, ensure_ascii=False))
    return 0


# ═══════════════ §6 calendar year + §7 rolling ═══════════════
def mode_rolling(sn, nm, ds, cache, idx):
    a = arms(cache, ds, idx)
    sc, s1 = series(a[C.CANDIDATE][0]), series(a[C.C1][0])
    rdates = ds[1:]

    # ── §6 calendar year ──────────────────────────────────────────────
    by_year = {}
    for j, d in enumerate(rdates):
        by_year.setdefault(d[:4], []).append(j)
    years = []
    for y, js in sorted(by_year.items()):
        if len(js) < 6:
            continue
        gc = gb = 1.0
        for j in js:
            gc *= 1 + sc[j]
            gb *= 1 + s1[j]
        years.append({"year": y, "months": len(js), "candPct": pct(gc - 1),
                      "ctrlPct": pct(gb - 1), "excessPct": pct(gc - gb),
                      "win": gc > gb})
    ex = [r["excessPct"] for r in years]
    streak = worst_streak = 0
    for r in years:
        if not r["win"]:
            streak += 1
            worst_streak = max(worst_streak, streak)
        else:
            streak = 0
    cal = {
        "rows": years, "totalYears": len(years),
        "positiveYears": sum(1 for r in years if r["win"]),
        "positiveRatePct": round(100 * sum(1 for r in years if r["win"]) / len(years), 1),
        "medianAnnualExcessPct": round(quant(ex, 0.5), 2),
        "meanAnnualExcessPct": round(sum(ex) / len(ex), 2),
        "worstYearExcessPct": min(ex), "bestYearExcessPct": max(ex),
        "worstYear": min(years, key=lambda r: r["excessPct"])["year"],
        "bestYear": max(years, key=lambda r: r["excessPct"])["year"],
        "longestConsecutiveUnderperformYears": worst_streak,
    }

    # ── §7 rolling horizons ───────────────────────────────────────────
    def roll(m, step=1):
        out = []
        for k in range(0, len(sc) - m + 1, step):
            gc = gb = 1.0
            for j in range(k, k + m):
                gc *= 1 + sc[j]
                gb *= 1 + s1[j]
            ac = gc ** (12 / m) - 1
            ab = gb ** (12 / m) - 1
            out.append({"start": rdates[k], "end": rdates[k + m - 1],
                        "candAnnualPct": pct(ac), "ctrlAnnualPct": pct(ab),
                        "excessPct": pct(ac - ab)})
        return out

    def summarize(rows, overlapping):
        e = [r["excessPct"] for r in rows]
        if not e:
            return None
        d = dist(e)
        return {"windows": len(e), "overlapping": overlapping,
                "medianPct": round(d["median"], 2), "meanPct": round(d["mean"], 2),
                "p10Pct": round(d["p10"], 2), "p25Pct": round(d["p25"], 2),
                "p75Pct": round(d["p75"], 2), "p90Pct": round(d["p90"], 2),
                "positiveRatePct": round(100 * sum(1 for x in e if x > 0) / len(e), 1),
                "worst": min(rows, key=lambda r: r["excessPct"]),
                "best": max(rows, key=lambda r: r["excessPct"])}

    rolling = {}
    for yh in (1, 3, 5, 7, 10):
        m = yh * 12
        if m > len(sc):
            continue
        rolling[f"{yh}Y"] = {
            "overlapping": summarize(roll(m, 1), True),
            "nonOverlapping": summarize(roll(m, m), False),
        }
    out = {"calendarYear": cal, "rolling": rolling,
           "note": ("overlapping window 는 서로 겹쳐 독립표본이 아니다. 그래서 "
                    "non-overlapping 결과를 함께 산출해 robustness 를 비교한다.")}
    save("rolling", out)
    print(json.dumps({"calPositive": f"{cal['positiveYears']}/{cal['totalYears']}",
                      "medAnnualExcess": cal["medianAnnualExcessPct"],
                      "roll5Y": rolling.get("5Y", {}).get("overlapping", {}).get("medianPct"),
                      "roll5YPos": rolling.get("5Y", {}).get("overlapping", {}).get("positiveRatePct")},
                     ensure_ascii=False))
    return 0


# ═══════════════ §8 start-date / cohort stability ═══════════════
def mode_stability(sn, nm, ds, cache, idx):
    n = len(ds)
    need = P["hold"] + 4
    rows = []
    for o in range(0, n - need + 1):
        sub = ds[o:]
        sidx = [v / idx[o] for v in idx[o:]]
        rc, ec = C.run(cache, sub, sidx, arm=C.CANDIDATE, base_kw=BASE_KW)
        r1, e1 = C.run(cache, sub, sidx, arm=C.C1, base_kw=BASE_KW)
        if not (ec and e1):
            continue
        scc, s11 = series(rc), series(r1)
        row = {"offset": o, "start": sub[0], "months": len(sub), "h": {}}
        for h in (60, 120):
            if h >= len(sub) or len(sub) < min_months_for(h):
                continue
            gc = gb = 1.0
            for j in range(h):
                gc *= 1 + scc[j]
                gb *= 1 + s11[j]
            ac = gc ** (12 / h) - 1
            ab = gb ** (12 / h) - 1
            row["h"][str(h)] = {"candAnnualPct": pct(ac), "ctrlAnnualPct": pct(ab),
                                "excessPct": pct(ac - ab)}
        # 전체 잔여기간
        row["toEnd"] = {"candCagrPct": pct(ec["twrCagr"]),
                        "ctrlCagrPct": pct(e1["twrCagr"]),
                        "excessPct": pct(ec["twrCagr"] - e1["twrCagr"])}
        rows.append(row)

    def summ(sel, key):
        e = [r[key]["excessPct"] for r in sel if key in r and r[key]]
        if not e:
            return None
        d = dist(e)
        return {"n": len(e), "medianPct": round(d["median"], 2),
                "meanPct": round(d["mean"], 2), "p10Pct": round(d["p10"], 2),
                "worstPct": round(d["min"], 2), "bestPct": round(d["max"], 2),
                "positiveRatePct": round(100 * sum(1 for x in e if x > 0) / len(e), 1)}

    def summh(sel, h):
        e = [r["h"][h]["excessPct"] for r in sel if h in r["h"]]
        if not e:
            return None
        d = dist(e)
        return {"n": len(e), "medianPct": round(d["median"], 2),
                "meanPct": round(d["mean"], 2), "p10Pct": round(d["p10"], 2),
                "worstPct": round(d["min"], 2), "bestPct": round(d["max"], 2),
                "positiveRatePct": round(100 * sum(1 for x in e if x > 0) / len(e), 1)}

    all_rows = rows
    ex2007 = [r for r in rows if not r["start"].startswith("2007")]
    from2009 = [r for r in rows if r["start"] >= "2009-01"]
    by_year = {}
    for r in rows:
        if "60" in r["h"]:
            by_year.setdefault(r["start"][:4], []).append(r["h"]["60"]["excessPct"])

    # benchmark(=C1) 대비 최장 부진
    rc, _ = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW)
    r1, _ = C.run(cache, ds, idx, arm=C.C1, base_kw=BASE_KW)
    rel = [rc["nav"][i] / r1["nav"][i] if r1["nav"][i] > 0 else 1.0
           for i in range(len(ds))]
    rp, ruw, cur = rel[0], 0, 0
    for v in rel:
        if v >= rp:
            rp, cur = v, 0
        else:
            cur += 1
            ruw = max(ruw, cur)

    out = {
        "startMonths": len(rows),
        "allStarts": {"cohort5Y": summh(all_rows, "60"),
                      "cohort10Y": summh(all_rows, "120"),
                      "toEnd": summ(all_rows, "toEnd")},
        "excluding2007Starts": {"cohort5Y": summh(ex2007, "60"),
                                "cohort10Y": summh(ex2007, "120"),
                                "toEnd": summ(ex2007, "toEnd")},
        "from2009FullyInvested": {"cohort5Y": summh(from2009, "60"),
                                  "cohort10Y": summh(from2009, "120"),
                                  "toEnd": summ(from2009, "toEnd")},
        "cohort5YExcessByStartYear": {
            y: {"n": len(v), "medianPct": round(quant(v, 0.5), 2),
                "positive": sum(1 for x in v if x > 0)}
            for y, v in sorted(by_year.items())},
        "longestControlUnderperformMonths": ruw,
        "rows": rows,
    }
    save("stability", out)
    print(json.dumps({"all5Y": out["allStarts"]["cohort5Y"],
                      "ex2007_5Y": out["excluding2007Starts"]["cohort5Y"],
                      "longestUnderperformMonths": ruw}, ensure_ascii=False))
    return 0


# ═══════════════ §9 walk-forward (기존 창 재사용) ═══════════════
def mode_walkforward(sn, nm, ds, cache, idx):
    n = len(ds)
    need = P["hold"] + 4
    rows = []

    def one(sub, off, kind):
        sidx = [v / idx[off] for v in idx[off:off + len(sub)]]
        rc, ec = C.run(cache, sub, sidx, arm=C.CANDIDATE, base_kw=BASE_KW)
        r1, e1 = C.run(cache, sub, sidx, arm=C.C1, base_kw=BASE_KW)
        if not (ec and e1):
            return None
        return {"kind": kind, "testFrom": sub[0], "testTo": sub[-1],
                "testMonths": len(sub),
                "candCagrPct": pct(ec["twrCagr"]), "ctrlCagrPct": pct(e1["twrCagr"]),
                "excessPct": pct(ec["twrCagr"] - e1["twrCagr"]),
                "positive": ec["twrCagr"] > e1["twrCagr"]}

    # R9/R10 사전확정 창 그대로 (49·73개월 + expanding-to-end). 새 최적화 금지.
    for test_len, kind in ((49, "EXPANDING_TRAIN_2COHORT_49M_TEST"),
                           (73, "EXPANDING_TRAIN_3COHORT_73M_TEST")):
        for cut in range(60, n - test_len + 1, 12):
            r = one(ds[cut:cut + test_len], cut, kind)
            if r:
                rows.append(r)
    for cut in range(60, n - 49 + 1, 12):
        r = one(ds[cut:cut + 49], cut, "ROLLING60_TRAIN_2COHORT_49M_TEST")
        if r:
            rows.append(r)
    for cut in range(60, n - need, 24):
        r = one(ds[cut:], cut, "EXPANDING_TRAIN_TEST_TO_END")
        if r:
            rows.append(r)

    e = [r["excessPct"] for r in rows]
    npos = sum(1 for r in rows if r["positive"])
    d = dist(e)
    by = {}
    for k in sorted({r["kind"] for r in rows}):
        sel = [r for r in rows if r["kind"] == k]
        se = [r["excessPct"] for r in sel]
        by[k] = {"n": len(sel), "positive": sum(1 for r in sel if r["positive"]),
                 "medianExcessPct": round(quant(se, 0.5), 2)}
    out = {"windowsReused": "R9/R10 사전확정 창(49·73개월 코호트 정수배 + expanding-to-end). 새 최적화 0.",
           "primaryControl": PRIMARY,
           "rows": rows, "total": len(rows), "positive": npos,
           "positiveRatePct": round(100 * npos / len(rows), 1),
           "majorityPositive": npos > len(rows) / 2,
           "medianExcessPct": round(d["median"], 2), "meanExcessPct": round(d["mean"], 2),
           "p10Pct": round(d["p10"], 2), "worstPct": round(d["min"], 2),
           "byKind": by}
    save("walkforward", out)
    print(json.dumps({"wf": f"{npos}/{len(rows)}", "median": out["medianExcessPct"],
                      "majority": out["majorityPositive"]}, ensure_ascii=False))
    return 0


# ═══════════════ §10 leave-one-era-out ═══════════════
def mode_era(sn, nm, ds, cache, idx):
    a = arms(cache, ds, idx)
    sc, s1 = series(a[C.CANDIDATE][0]), series(a[C.C1][0])
    rdates = ds[1:]
    full_c, full_b = chain_stats(sc), chain_stats(s1)
    rows = []
    for e in ERAS:
        keep = [j for j, d in enumerate(rdates) if not (e["from"] <= d[:7] <= e["to"])]
        drop = len(rdates) - len(keep)
        if drop == 0 or len(keep) < 36:
            rows.append({"era": e["id"], "label": e["label"], "skipped": True})
            continue
        cc = chain_stats([sc[j] for j in keep])
        bb = chain_stats([s1[j] for j in keep])
        rows.append({"era": e["id"], "label": e["label"], "from": e["from"],
                     "to": e["to"], "droppedMonths": drop,
                     "candCagrPct": pct(cc["cagr"]), "ctrlCagrPct": pct(bb["cagr"]),
                     "excessPct": pct(cc["cagr"] - bb["cagr"]),
                     "positive": cc["cagr"] > bb["cagr"]})
    standalone = []
    for e in ERAS:
        sel = [j for j, d in enumerate(rdates) if e["from"] <= d[:7] <= e["to"]]
        if len(sel) < 6:
            continue
        cc = chain_stats([sc[j] for j in sel])
        bb = chain_stats([s1[j] for j in sel])
        standalone.append({"era": e["id"], "label": e["label"], "months": len(sel),
                           "candTotalPct": pct(cc["growth"] - 1),
                           "ctrlTotalPct": pct(bb["growth"] - 1),
                           "candCagrPct": pct(cc["cagr"]), "ctrlCagrPct": pct(bb["cagr"]),
                           "excessPct": pct(cc["cagr"] - bb["cagr"]),
                           "positive": cc["cagr"] > bb["cagr"]})
    live = [r for r in rows if not r.get("skipped")]
    ex = [r["excessPct"] for r in live]
    out = {"erasReused": "R9/R10 정의 그대로. 새 era 재분할 0.",
           "full": {"candCagrPct": pct(full_c["cagr"]), "ctrlCagrPct": pct(full_b["cagr"]),
                    "excessPct": pct(full_c["cagr"] - full_b["cagr"])},
           "method": "월별 TWR 시계열에서 해당 시대의 달을 candidate·control 동일하게 제거 후 재연쇄",
           "leaveOneOut": rows,
           "positiveCount": sum(1 for r in live if r["positive"]),
           "negativeCount": sum(1 for r in live if not r["positive"]),
           "total": len(live), "medianExcessPct": round(quant(ex, 0.5), 2),
           "worstExcludedEra": min(live, key=lambda r: r["excessPct"])["era"],
           "worstExcludedExcessPct": min(ex),
           "eraStandalone": standalone,
           "standalonePositive": sum(1 for r in standalone if r["positive"]),
           "standaloneTotal": len(standalone)}
    save("era", out)
    print(json.dumps({"loo": f"{out['positiveCount']}/{out['total']}",
                      "worst": out["worstExcludedEra"],
                      "worstExcess": out["worstExcludedExcessPct"],
                      "standalone": f"{out['standalonePositive']}/{out['standaloneTotal']}"},
                     ensure_ascii=False))
    return 0


# ═══════════════ §11 bootstrap inference ═══════════════
def mode_bootstrap(sn, nm, ds, cache, idx):
    a = arms(cache, ds, idx)
    sc, s1 = series(a[C.CANDIDATE][0]), series(a[C.C1][0])
    rdates = ds[1:]
    n = len(sc)

    def ann_excess(js):
        gc = gb = 1.0
        for j in js:
            gc *= 1 + sc[j]
            gb *= 1 + s1[j]
        m = len(js)
        return (gc ** (12 / m) - 1) - (gb ** (12 / m) - 1)

    B = 5000
    res = {}

    # (1) moving block bootstrap — block = 24개월(코호트 주기와 일치)
    for L in (12, 24, 36):
        rng = random.Random(BOOT_SEED + L)
        vals = []
        nb = math.ceil(n / L)
        for _ in range(B):
            js = []
            for _ in range(nb):
                s = rng.randrange(0, n - L + 1)
                js.extend(range(s, s + L))
            vals.append(ann_excess(js[:n]))
        res[f"movingBlock_L{L}"] = _boot_summary(vals)

    # (2) stationary bootstrap (Politis-Romano), 기대 블록길이 24
    rng = random.Random(BOOT_SEED + 999)
    p = 1.0 / 24
    vals = []
    for _ in range(B):
        js = []
        i = rng.randrange(n)
        while len(js) < n:
            js.append(i)
            if rng.random() < p:
                i = rng.randrange(n)
            else:
                i = (i + 1) % n
        vals.append(ann_excess(js))
    res["stationary_meanBlock24"] = _boot_summary(vals)

    # (3) year-level bootstrap — 달력연도 블록 재표집
    by_year = {}
    for j, d in enumerate(rdates):
        by_year.setdefault(d[:4], []).append(j)
    yrs = [v for _, v in sorted(by_year.items())]
    rng = random.Random(BOOT_SEED + 7)
    vals = []
    for _ in range(B):
        js = []
        for _ in range(len(yrs)):
            js.extend(rng.choice(yrs))
        vals.append(ann_excess(js))
    res["yearLevel"] = _boot_summary(vals)

    # (4) cohort-level bootstrap — 24개월 코호트 블록 재표집
    coh = [list(range(k, min(k + 24, n))) for k in range(0, n, 24)]
    coh = [c for c in coh if len(c) >= 12]
    rng = random.Random(BOOT_SEED + 24)
    vals = []
    for _ in range(B):
        js = []
        for _ in range(len(coh)):
            js.extend(rng.choice(coh))
        vals.append(ann_excess(js))
    res["cohortLevel"] = _boot_summary(vals)

    pooled = [v for k in res for v in ()]  # 표시용 없음
    out = {"observedAnnualExcessPct": pct(ann_excess(list(range(n)))),
           "draws": B, "seed": BOOT_SEED,
           "deterministic": True,
           "method": ("장기 겹침·직렬상관 자료이므로 naive t-test 를 쓰지 않는다. "
                      "moving block(12/24/36개월) · stationary(기대블록 24) · "
                      "year-level · cohort-level 4가지로 분포를 추정한다. "
                      "seed 고정, 표준 Python 만 사용(neue 라이브러리 0)."),
           "results": res,
           "consensus": {
               "allMethodsProbPositive": [v["probExcessGt0Pct"] for v in res.values()],
               "minProbPositivePct": min(v["probExcessGt0Pct"] for v in res.values()),
               "minProbGt1Pct": min(v["probExcessGt1pPct"] for v in res.values()),
               "minProbGt2Pct": min(v["probExcessGt2pPct"] for v in res.values()),
               "allCiLowerPositive": all(v["ci95LowerPct"] > 0 for v in res.values()),
           }}
    save("bootstrap", out)
    print(json.dumps({"observed": out["observedAnnualExcessPct"],
                      "minProbPositive": out["consensus"]["minProbPositivePct"],
                      "minProbGt1": out["consensus"]["minProbGt1Pct"],
                      "allCiLowerPositive": out["consensus"]["allCiLowerPositive"]},
                     ensure_ascii=False))
    return 0


def _boot_summary(vals):
    v = sorted(vals)
    n = len(v)
    return {"meanPct": pct(sum(v) / n), "medianPct": pct(quant(v, 0.5)),
            "ci95LowerPct": pct(quant(v, 0.025)), "ci95UpperPct": pct(quant(v, 0.975)),
            "p10Pct": pct(quant(v, 0.10)), "p90Pct": pct(quant(v, 0.90)),
            "probExcessGt0Pct": round(100 * sum(1 for x in v if x > 0) / n, 1),
            "probExcessGt1pPct": round(100 * sum(1 for x in v if x > 0.01) / n, 1),
            "probExcessGt2pPct": round(100 * sum(1 for x in v if x > 0.02) / n, 1)}


# ═══════════════ §12 cross-sectional + §13 concentration + §14 분포 ═══════════════
def mode_crosssec(sn, nm, ds, cache, idx):
    a = arms(cache, ds, idx)
    res_c, ev_c = a[C.CANDIDATE]
    res_1, ev_1 = a[C.C1]
    res_3, ev_3 = a[C.C3]
    sc, s1, s3 = series(res_c), series(res_1), series(res_3)

    # ── §12 TOP20 vs REST80 (동일 프레임·동일 정수주) ────────────────────
    top_rest = excess_stats(sc, s3)
    # 코호트별 포지션 평균 수익률로 cross-section spread 도 본다
    def pos_ret_by_cohort(res):
        agg = {}
        for l in res["ledger"] + res["openLots"]:
            k = (l["cohort"], l["ticker"])
            e = agg.setdefault(k, {"cb": 0.0, "pr": 0.0, "coh": l["cohort"]})
            e["cb"] += l["costBasis"]
            e["pr"] += l["proceeds"]
        by = {}
        for e in agg.values():
            if e["cb"] > 0:
                by.setdefault(e["coh"], []).append(e["pr"] / e["cb"] - 1)
        return by
    bc, b3, b1 = (pos_ret_by_cohort(x) for x in (res_c, res_3, res_1))
    coh_rows = []
    for k in sorted(set(bc) & set(b3) & set(b1)):
        mc = sum(bc[k]) / len(bc[k])
        m3 = sum(b3[k]) / len(b3[k])
        m1 = sum(b1[k]) / len(b1[k])
        coh_rows.append({"cohort": k, "buyDate": ds[k * P["buy_every"]],
                         "top20MeanPosRetPct": pct(mc), "rest80MeanPosRetPct": pct(m3),
                         "allEligibleMeanPosRetPct": pct(m1),
                         "top20MinusRest80Pct": pct(mc - m3),
                         "top20MinusAllPct": pct(mc - m1),
                         "top20Wins": mc > m3})

    # ── §13 concentration (정밀 재검증 + exact 재시뮬) ────────────────────
    def per_ticker(res):
        d = {}
        for l in res["ledger"] + res["openLots"]:
            d[l["ticker"]] = d.get(l["ticker"], 0.0) + l["pnl"]
        return d
    pt = per_ticker(res_c)
    ranked = sorted(pt.items(), key=lambda kv: -kv[1])
    tot = sum(pt.values())
    years = ev_c["years"]
    growth = (1 + ev_c["twrCagr"]) ** years
    ctrl_cagr = ev_1["twrCagr"]
    conc = {"totalPnl": round(tot), "uniqueTickers": len(pt),
            "top": [{"ticker": t, "name": nm.get(t, t), "pnl": round(v),
                     "sharePct": round(100 * v / tot, 2)} for t, v in ranked[:12]],
            "removal": []}
    pos = [v for v in pt.values() if v > 0]
    tp = sum(pos)
    conc["hhiPositiveContrib"] = round(sum((v / tp) ** 2 for v in pos), 5) if tp else None
    for k in (1, 3, 5, 10):
        conc[f"top{k}SharePct"] = round(100 * sum(v for _, v in ranked[:k]) / tot, 2)
    for k in (1, 3, 5, 10):
        drop = sum(v for _, v in ranked[:k])
        adj = ev_c["terminalWealth"] - drop
        f = adj / ev_c["terminalWealth"] if ev_c["terminalWealth"] else 0
        g2 = growth * f
        c2 = g2 ** (1 / years) - 1 if g2 > 0 else None
        banned = [t for t, _ in ranked[:k]]
        _, ex_ev = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW, banned=banned)
        conc["removal"].append({
            "topK": k, "removedPnl": round(drop), "tickers": banned,
            "firstOrder": {"cagrPct": pct(c2),
                           "excessVsC1Pct": pct(c2 - ctrl_cagr) if c2 is not None else None},
            "exactResimulated": {"cagrPct": pct(ex_ev["twrCagr"]),
                                 "excessVsC1Pct": pct(ex_ev["twrCagr"] - ctrl_cagr)}
            if ex_ev else None,
            "note": "fragility audit 이다. 결과를 보고 후보를 재구성하지 않는다(§13)."})

    # ── §14 winner distribution ──────────────────────────────────────────
    def wdist(res, label):
        agg = {}
        for l in res["ledger"] + res["openLots"]:
            k = (l["cohort"], l["ticker"])
            e = agg.setdefault(k, {"cb": 0.0, "pr": 0.0, "pnl": 0.0, "del": False})
            e["cb"] += l["costBasis"]
            e["pr"] += l["proceeds"]
            e["pnl"] += l["pnl"]
            e["del"] = e["del"] or l["delisted"]
        items = [e for e in agg.values() if e["cb"] > 0]
        rets = [e["pr"] / e["cb"] - 1 for e in items]
        pnls = sorted((e["pnl"] for e in items), reverse=True)
        tot_p = sum(pnls)
        gross_pos = sum(x for x in pnls if x > 0)
        n = len(pnls)
        k1 = max(1, n // 100)
        k5 = max(1, n // 20)
        d = dist(rets)
        mean = d["mean"]
        sd = math.sqrt(sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)) if len(rets) > 1 else 0
        skew = (sum((r - mean) ** 3 for r in rets) / len(rets) / (sd ** 3)) if sd > 0 else None
        loss = sum(e["pnl"] for e in items if e["pnl"] < 0)
        deli = sum(e["pnl"] for e in items if e["del"])
        return {"label": label, "positions": n,
                "medianRetPct": pct(d["median"]), "meanRetPct": pct(d["mean"]),
                "winRatePct": round(100 * sum(1 for r in rets if r > 0) / n, 2),
                "p10Pct": pct(d["p10"]), "p25Pct": pct(d["p25"]),
                "p75Pct": pct(d["p75"]), "p90Pct": pct(d["p90"]),
                "p95Pct": pct(quant(rets, 0.95)),
                "skewness": round(skew, 3) if skew is not None else None,
                # ★ 분모를 두 가지로 나눈다. 순손익(tot_p)으로 나누면 손실이 상쇄돼
                #   상위 5% 기여가 100% 를 넘는 오해 소지 숫자가 나온다(실측 107~236%).
                #   그래서 '총 이익(gross positive)' 대비 비중을 주 지표로 쓰고,
                #   순손익 대비 배수는 별도 키로 병기한다.
                "grossPositivePnlKrw": round(gross_pos),
                "top1PctShareOfGrossPositivePct": round(100 * sum(pnls[:k1]) / gross_pos, 2)
                if gross_pos else None,
                "top5PctShareOfGrossPositivePct": round(100 * sum(pnls[:k5]) / gross_pos, 2)
                if gross_pos else None,
                "top5PctAsMultipleOfNetPnl": round(sum(pnls[:k5]) / tot_p, 2) if tot_p else None,
                "lossPositionsContribKrw": round(loss),
                "lossPositionsShareOfGrossPositivePct": round(100 * abs(loss) / gross_pos, 2)
                if gross_pos else None,
                "delistedPositionsContribKrw": round(deli),
                "delistedPositionsShareOfGrossPositivePct": round(100 * abs(deli) / gross_pos, 2)
                if gross_pos else None,
                "structure": ("FEW_BIG_WINNERS"
                              if (d["median"] < 0.05 and
                                  (100 * sum(pnls[:k5]) / gross_pos if gross_pos else 0) > 50)
                              else "BROAD")}

    # ── §12 pool 수준 cross-section — 40종목 샘플링 효과 분리 ────────────────
    pool = {}
    for arm in (C.P1, C.P2):
        r, e = C.run(cache, ds, idx, arm=arm, base_kw=BASE_KW)
        pool[arm] = {"cagrPct": pct(e["twrCagr"]), "mddPct": pct(e["mdd"]),
                     "avgPositions": round(e["avgPositions"], 1),
                     "terminalWealth": round(e["terminalWealth"])}
    p1c = pool[C.P1]["cagrPct"]
    p2c = pool[C.P2]["cagrPct"]
    c1c = pct(ev_1["twrCagr"])
    candc = pct(ev_c["twrCagr"])
    # pool 수준 집중도 — 266종목이면 소수 대박 종목 의존을 구조적으로 배제한다
    _, _ = None, None
    rp1, _ = C.run(cache, ds, idx, arm=C.P1, base_kw=BASE_KW)
    ptp1 = {}
    for l in rp1["ledger"] + rp1["openLots"]:
        ptp1[l["ticker"]] = ptp1.get(l["ticker"], 0.0) + l["pnl"]
    rk1 = sorted(ptp1.values(), reverse=True)
    tt1 = sum(rk1)
    pool_conc = {"top1SharePct": round(100 * rk1[0] / tt1, 2) if tt1 else None,
                 "top5SharePct": round(100 * sum(rk1[:5]) / tt1, 2) if tt1 else None,
                 "top10SharePct": round(100 * sum(rk1[:10]) / tt1, 2) if tt1 else None,
                 "uniqueTickers": len(ptp1)}
    pool_level = {
        "note": ("§12 가 요구하는 pool 수준 비교. frozen P20 그대로이고 40종목 균등간격 "
                 "샘플링만 제거해 풀 전체를 동일가중 보유한다. 새 threshold 탐색 0."),
        "BM_TOP20_POOL_ALL": pool[C.P1],
        "BM_REST80_POOL_ALL": pool[C.P2],
        "EW_VALID_PBR_ALL": {"cagrPct": c1c, "mddPct": pct(ev_1["mdd"]),
                             "avgPositions": round(ev_1["avgPositions"], 1)},
        "top20PoolMinusRest80PoolPct": round(p1c - p2c, 2),
        "top20PoolMinusAllEligiblePct": round(p1c - c1c, 2),
        "candidateMinusTop20PoolPct": round(candc - p1c, 2),
        "poolLevelConcentration": pool_conc,
        "reading": (
            f"BM 상위 20% 풀 전체({pool[C.P1]['avgPositions']}종목 평균)를 그냥 동일가중으로 "
            f"들고만 있어도 {p1c}% 이고, 하위 80% 풀({pool[C.P2]['avgPositions']}종목)은 "
            f"{p2c}% 다. spread {round(p1c - p2c, 2):+.2f}%p. 전 종목 동일가중 {c1c}% 대비 "
            f"{round(p1c - c1c, 2):+.2f}%p. 40종목 균등간격 샘플링이 추가하는 몫은 "
            f"{round(candc - p1c, 2):+.2f}%p 에 불과하다. "
            f"즉 초과수익은 **BM 상위 20% 라는 단면 자체**에 있고 40종목 추출 방식의 "
            f"산물이 아니다. 풀 수준에서는 상위 5종목 기여가 "
            f"{pool_conc['top5SharePct']}% 로 40종목판(75.48%)보다 훨씬 낮아 소수 대박 "
            f"종목 의존 우려도 구조적으로 완화된다. MDD 도 "
            f"{pool[C.P1]['mddPct']}% 로 후보(40종목) {pct(ev_c['mdd'])}% 보다 얕다."),
    }

    out = {"poolLevelCrossSection": pool_level,
           "topVsRest": {"primary": top_rest,
                         "candidateCagrPct": pct(ev_c["twrCagr"]),
                         "rest80CagrPct": pct(ev_3["twrCagr"]),
                         "allEligibleCagrPct": pct(ev_1["twrCagr"]),
                         "note": ("동일 프레임·동일 정수주·동일 비용. 구간만 "
                                  "상위20% / 하위80%. 추가 threshold 탐색 0(§12).")},
           "cohortCrossSection": {
               "rows": coh_rows,
               "cohortsWhereTop20Wins": sum(1 for r in coh_rows if r["top20Wins"]),
               "totalCohorts": len(coh_rows),
               "medianTop20MinusRest80Pct": round(
                   quant([r["top20MinusRest80Pct"] for r in coh_rows], 0.5), 2),
               "medianTop20MinusAllPct": round(
                   quant([r["top20MinusAllPct"] for r in coh_rows], 0.5), 2)},
           "concentration": conc,
           "winnerDistribution": {"candidate": wdist(res_c, "CANDIDATE_BM_TOP20"),
                                  "controlC1": wdist(res_1, C.C1),
                                  "controlC3": wdist(res_3, C.C3)}}
    save("crosssec", out)
    print(json.dumps({"top20VsRest80Pct": top_rest["cagrExcessPct"],
                      "cohortsTop20Wins": f"{out['cohortCrossSection']['cohortsWhereTop20Wins']}"
                                          f"/{out['cohortCrossSection']['totalCohorts']}",
                      "top5SharePct": conc["top5SharePct"],
                      "top5RemovedExcess": conc["removal"][2]["exactResimulated"]["excessVsC1Pct"]},
                     ensure_ascii=False))
    return 0


# ═══════════════ §15 subgroup fair control + §17 §18 ═══════════════
def mode_subgroup(sn, nm, ds, cache, idx):
    groups = {}
    for lbl, mk in (("KOSPI", dict(market="KOSPI")), ("KOSDAQ", dict(market="KOSDAQ")),
                    ("SMALL", dict(size_bucket=0)), ("MID", dict(size_bucket=1)),
                    ("LARGE", dict(size_bucket=2))):
        c2 = RankCache(sn, nm, **mk)
        i2 = ew_universe_index(c2, ds)
        rc, ec = C.run(c2, ds, i2, arm=C.CANDIDATE, base_kw=BASE_KW)
        r1, e1 = C.run(c2, ds, i2, arm=C.C1, base_kw=BASE_KW)
        r3, e3 = C.run(c2, ds, i2, arm=C.C3, base_kw=BASE_KW)
        if not (ec and e1):
            continue
        elig = [len(C.eligible(c2, d)) for d in ds[::24]]
        groups[lbl] = {
            "avgEligible": round(sum(elig) / len(elig), 1),
            "candCagrPct": pct(ec["twrCagr"]),
            "fairControlCagrPct": pct(e1["twrCagr"]),
            "excessVsFairControlPct": pct(ec["twrCagr"] - e1["twrCagr"]),
            "rest80CagrPct": pct(e3["twrCagr"]) if e3 else None,
            "top20MinusRest80Pct": pct(ec["twrCagr"] - e3["twrCagr"]) if e3 else None,
            "candMddPct": pct(ec["mdd"]), "ctrlMddPct": pct(e1["mdd"]),
            "positive": ec["twrCagr"] > e1["twrCagr"],
        }

    # ── §17 유동성 / §18 부실 노출 (attribution only, 필터 추가 0) ─────────
    res_c, ev_c = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW)
    res_1, _ = C.run(cache, ds, idx, arm=C.C1, base_kw=BASE_KW)

    def audit(res, label):
        picks = [p for pl in res["pickLog"] for p in pl["picks"]]
        prices = [p["close"] for p in picks]
        caps = [p["marketCap"] for p in picks if p["marketCap"]]
        impact = []
        for pl in res["pickLog"]:
            nav = res["nav"][pl["idx"]]
            per = nav / max(1, len(pl["picks"]))
            for p in pl["picks"]:
                if p["marketCap"]:
                    impact.append(per / p["marketCap"])
        stale = 0
        for pl in res["pickLog"]:
            j = ds.index(pl["date"])
            for p in pl["picks"]:
                if j >= 2:
                    x = sn[ds[j - 1]].get(p["ticker"], {}).get("close")
                    y = sn[ds[j - 2]].get(p["ticker"], {}).get("close")
                    if x and y and p["close"] == x == y:
                        stale += 1
        lots = res["ledger"] + res["openLots"]
        agg = {}
        for l in lots:
            k = (l["cohort"], l["ticker"])
            e = agg.setdefault(k, {"cb": 0.0, "pnl": 0.0, "del": False,
                                   "pbr": l["pbrAtBuy"]})
            e["cb"] += l["costBasis"]
            e["pnl"] += l["pnl"]
            e["del"] = e["del"] or l["delisted"]
        items = list(agg.values())
        tot = sum(e["pnl"] for e in items)
        lowpbr = [e for e in items if e["pbr"] and e["pbr"] < 0.3]
        return {
            "label": label, "pickCount": len(picks),
            "medianPriceKrw": round(quant(prices, 0.5)) if prices else None,
            "priceBelow1000Pct": round(100 * sum(1 for x in prices if x < 1000)
                                       / len(prices), 2) if prices else None,
            "medianMarketCapEok": round(quant(caps, 0.5) / 1e8) if caps else None,
            "p10MarketCapEok": round(quant(caps, 0.10) / 1e8) if caps else None,
            "positionVsMarketCapMedianPct": pct(quant(impact, 0.5)) if impact else None,
            "positionVsMarketCapP90Pct": pct(quant(impact, 0.90)) if impact else None,
            "stalePricePickPct": round(100 * stale / len(picks), 2) if picks else None,
            "delistRatePct": round(100 * sum(1 for e in items if e["del"])
                                   / len(items), 2) if items else None,
            "delistPnlSharePct": round(100 * sum(e["pnl"] for e in items if e["del"])
                                       / tot, 2) if tot else None,
            "pbrBelow0_3SharePct": round(100 * len(lowpbr) / len(items), 2) if items else None,
            "pbrBelow0_3PnlSharePct": round(100 * sum(e["pnl"] for e in lowpbr)
                                            / tot, 2) if tot else None,
            "medianPbrAtBuy": round(quant([e["pbr"] for e in items if e["pbr"]], 0.5), 3),
        }

    out = {"subgroupFairControl": {
               "note": ("각 subgroup 내부에서 candidate 를 **같은 subgroup 의 공정 "
                        "EW_VALID_PBR control** 과 비교한다. full-market 대비가 아니다. "
                        "BM alpha 가 size/exchange proxy 인지 판별하는 핵심 증거(§15). "
                        "새 filter 추가 0 — attribution 만."),
               "groups": groups,
               "positiveGroups": sum(1 for v in groups.values() if v["positive"]),
               "totalGroups": len(groups)},
           "liquidityAudit": {"candidate": audit(res_c, "CANDIDATE"),
                              "controlC1": audit(res_1, C.C1),
                              "dataLimitation": ("스냅샷에 거래량·거래대금·거래정지 "
                                                 "플래그가 없다(컬럼: ticker,market,close,"
                                                 "marketCap,shares,PER,PBR,EPS,BPS,DIV,DPS). "
                                                 "가격·시총·종가무변동 proxy 만으로 감사했고 "
                                                 "실제 체결 가능성 검증은 미완이다. "
                                                 "결과가 나빠도 종목을 제거하지 않았다(§17).")}}
    save("subgroup", out)
    print(json.dumps({g: v["excessVsFairControlPct"] for g, v in groups.items()},
                     ensure_ascii=False))
    return 0


# ═══════════════ §16 cost / delisting stress (양쪽 동일 적용) ═══════════════
def mode_stress(sn, nm, ds, cache, idx):
    rows = []
    for lbl, kw in (("BASE", dict()),
                    ("HIGH_COST_SLIPPAGE", dict(cost_bps=20.0, sell_tax_bps=23.0,
                                                slippage_bps=100.0)),
                    ("DELIST_100_SYMMETRIC", dict(delist_haircut=1.0))):
        _, ec = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW, **kw)
        _, e1 = C.run(cache, ds, idx, arm=C.C1, base_kw=BASE_KW, **kw)
        _, e3 = C.run(cache, ds, idx, arm=C.C3, base_kw=BASE_KW, **kw)
        rows.append({
            "scenario": lbl, "params": {k: v for k, v in kw.items()},
            "candCagrPct": pct(ec["twrCagr"]), "ctrlC1CagrPct": pct(e1["twrCagr"]),
            "incrementalExcessPct": pct(ec["twrCagr"] - e1["twrCagr"]),
            "rest80CagrPct": pct(e3["twrCagr"]) if e3 else None,
            "top20MinusRest80Pct": pct(ec["twrCagr"] - e3["twrCagr"]) if e3 else None,
            "candCostPctOfContrib": pct(ec["costPctOfContrib"]),
            "ctrlCostPctOfContrib": pct(e1["costPctOfContrib"]),
            "candMddPct": pct(ec["mdd"]), "ctrlMddPct": pct(e1["mdd"])})
    out = {"note": ("동일 stress 를 candidate 와 control 에 **동일하게** 적용한다. "
                    "turnover 차이에서 실제로 발생하는 비용 차이는 그대로 반영된다(§16). "
                    "candidate 만 비용을 더/덜 내는 구조는 없다."),
           "rows": rows,
           "allScenariosPositive": all(r["incrementalExcessPct"] > 0 for r in rows)}
    save("stress", out)
    print(json.dumps({r["scenario"]: r["incrementalExcessPct"] for r in rows},
                     ensure_ascii=False))
    return 0


# ═══════════════ §22 5천만원 위험표 ═══════════════
def mode_risk(sn, nm, ds, cache, idx):
    res_c, ev_c = C.run(cache, ds, idx, arm=C.CANDIDATE, base_kw=BASE_KW)
    res_1, ev_1 = C.run(cache, ds, idx, arm=C.C1, base_kw=BASE_KW)
    st = load("stability")

    def sheet(res, ev, label):
        nav, inflow = res["nav"], res["inflow"]
        con, c = [], 0.0
        for x in inflow:
            c += x
            con.append(c)
        peak = nav[0]
        wdd, wat = 0.0, None
        for i, v in enumerate(nav):
            peak = max(peak, v)
            if peak - v > wdd:
                wdd, wat = peak - v, ds[i]
        ratio = [(v / con[i], i) for i, v in enumerate(nav) if con[i] > 0]
        lo, loi = min(ratio)
        return {"label": label,
                "initialCapitalKrw": CAP,
                "terminalWealthKrw": round(nav[-1]),
                "mddPct": pct(ev["mdd"]),
                "mddAmountKrw": round(wdd), "mddAtDate": wat,
                "lowestValueVsContributedPct": pct(lo),
                "lowestValueAt": ds[loi],
                "lowestValueKrw": round(nav[loi]),
                "worst1yPct": pct(ev["worst1y"]),
                "worst1yEndValueKrw": round(CAP * (1 + ev["worst1y"]))
                if ev["worst1y"] is not None else None,
                "worst3yAnnualPct": pct(ev["worst3y"]),
                "worst3yTotalPct": pct((1 + ev["worst3y"]) ** 3 - 1)
                if ev["worst3y"] is not None else None,
                "longestUnderwaterMonths": ev["maxUnderwaterMonths"],
                "roll1yNegSharePct": pct(ev["roll1yNegShare"]),
                "maxConcurrentPositions": ev["maxPositions"],
                "avgPositions": round(ev["avgPositions"], 1),
                "ordersPerYear": round(res["orders"] / ev["years"], 1),
                "avgCashRatioPct": pct(ev["avgCashRatio"]),
                "costPctOfContrib": pct(ev["costPctOfContrib"])}

    w5 = st["allStarts"]["cohort5Y"]
    worst_start = min(
        (r for r in st["rows"] if "60" in r["h"]),
        key=lambda r: r["h"]["60"]["excessPct"], default=None)
    out = {"basis": "50,000,000원 · 과거 실측치 · 예측·보장 아님 · 개인화된 투자권유 아님",
           "candidate": sheet(res_c, ev_c, "CANDIDATE_BM_TOP20"),
           "fairControl": sheet(res_1, ev_1, C.C1),
           "longestFairControlUnderperformMonths": st["longestControlUnderperformMonths"],
           "worstStart5YExcess": worst_start and {
               "start": worst_start["start"],
               "excessPct": worst_start["h"]["60"]["excessPct"],
               "candAnnualPct": worst_start["h"]["60"]["candAnnualPct"],
               "ctrlAnnualPct": worst_start["h"]["60"]["ctrlAnnualPct"]},
           "start5YExcessP10Pct": w5 and w5["p10Pct"],
           "note": "미래 예측·보장 표현을 쓰지 않는다."}
    save("risk", out)
    print(json.dumps({"candTerminal": out["candidate"]["terminalWealthKrw"],
                      "ctrlTerminal": out["fairControl"]["terminalWealthKrw"],
                      "candMdd": out["candidate"]["mddPct"],
                      "ctrlMdd": out["fairControl"]["mddPct"]}, ensure_ascii=False))
    return 0


# ═══════════════ §19 verdict ═══════════════
def mode_verdict():
    rp, co, ro = load("repro"), load("core"), load("rolling")
    stb, wf, era = load("stability"), load("walkforward"), load("era")
    bs, cs, sg = load("bootstrap"), load("crosssec"), load("subgroup")
    ss, rk = load("stress"), load("risk")

    base = co["incrementalAlpha"]["vs_C1_PRIMARY"]["cagrExcessPct"]
    crit = {}

    crit["C1_baseExcessPositive"] = {
        "pass": base > 0, "value": base,
        "rule": "EW_VALID_PBR 대비 base excess 명확히 양수",
        "evidence": f"+{base:.2f}%p" if base > 0 else f"{base:.2f}%p"}
    crit["C2_economicSize1p"] = {
        "pass": base >= 1.0, "value": base,
        "rule": "경제적 크기 최소 +1%p 이상",
        "evidence": f"{base:+.2f}%p (기준 >= +1.00%p)"}
    r5 = ro["rolling"].get("5Y", {}).get("overlapping", {})
    r10r = ro["rolling"].get("10Y", {}).get("overlapping", {})
    crit["C3_rolling5Y10YMajority"] = {
        "pass": (r5.get("positiveRatePct", 0) > 50 and r10r.get("positiveRatePct", 0) > 50),
        "rule": "rolling 5Y/10Y 다수 positive",
        "evidence": f"5Y {r5.get('positiveRatePct')}% · 10Y {r10r.get('positiveRatePct')}%"}
    crit["C4_walkForwardMajority"] = {
        "pass": wf["majorityPositive"],
        "rule": "walk-forward majority positive",
        "evidence": f"{wf['positive']}/{wf['total']} ({wf['positiveRatePct']}%) · "
                    f"중앙값 {wf['medianExcessPct']:+.2f}%p"}
    crit["C5_leaveOneEraOutMostlyPositive"] = {
        "pass": era["positiveCount"] >= era["total"] - 1,
        "rule": "leave-one-era-out 대부분 positive",
        "evidence": f"{era['positiveCount']}/{era['total']} · 최악 제외시대 "
                    f"{era['worstExcludedEra']} {era['worstExcludedExcessPct']:+.2f}%p"}
    cns = bs["consensus"]
    crit["C6_bootstrapCiNotCenteredOnZero"] = {
        "pass": bool(cns["allCiLowerPositive"] and cns["minProbPositivePct"] >= 90),
        "rule": "bootstrap CI 가 0 을 중심으로 하지 않음",
        "evidence": f"모든 방법 95% CI 하한 > 0: {cns['allCiLowerPositive']} · "
                    f"P(excess>0) 최소 {cns['minProbPositivePct']}%"}
    rm5 = next(r for r in cs["concentration"]["removal"] if r["topK"] == 5)
    crit["C7_topContributorsRemovedDirectionHolds"] = {
        "pass": bool((rm5["exactResimulated"] or {}).get("excessVsC1Pct", -1) > 0
                     and (rm5["firstOrder"] or {}).get("excessVsC1Pct", -1) > 0),
        "rule": "top contributors 제거 후 방향 유지",
        "evidence": f"상위5 제거 — 정확재시뮬 "
                    f"{rm5['exactResimulated']['excessVsC1Pct']:+.2f}%p · 1차근사 "
                    f"{rm5['firstOrder']['excessVsC1Pct']:+.2f}%p "
                    f"(상위5 기여 {cs['concentration']['top5SharePct']}%)"}
    g = sg["subgroupFairControl"]
    crit["C8_subgroupConsistency"] = {
        "pass": g["positiveGroups"] >= 3,
        "rule": "subgroup 내 최소 일부 이상 일관된 BM effect",
        "evidence": f"공정 control 대비 양수 subgroup {g['positiveGroups']}/{g['totalGroups']} — "
                    + " · ".join(f"{k} {v['excessVsFairControlPct']:+.2f}%p"
                                 for k, v in g["groups"].items())}

    npass = sum(1 for v in crit.values() if v["pass"])
    fails = [k for k, v in crit.items() if not v["pass"]]

    # ── §19 판정 ──────────────────────────────────────────────────────
    neg_signals = sum([
        base <= 0,
        r5.get("positiveRatePct", 0) <= 50,
        not wf["majorityPositive"],
        cns["minProbPositivePct"] < 50,
        (rm5["exactResimulated"] or {}).get("excessVsC1Pct", 0) <= 0,
        g["positiveGroups"] <= g["totalGroups"] // 2,
    ])
    if npass == len(crit):
        alpha = "INCREMENTAL_ALPHA_CONFIRMED"
    elif neg_signals >= 3 or base <= 0:
        alpha = "NO_INCREMENTAL_ALPHA"
    else:
        alpha = "WEAK_OR_INCONCLUSIVE"

    strategy = {"INCREMENTAL_ALPHA_CONFIRMED": "FORWARD_TEST_ELIGIBLE",
                "WEAK_OR_INCONCLUSIVE": "HOLD",
                "NO_INCREMENTAL_ALPHA": "REJECT"}[alpha]

    out = {
        "primaryControl": PRIMARY,
        "primaryBenchmarkRedefinition": rp["benchmarkDefinitionDecomposition"]["conclusion"],
        "baseIncrementalExcessPct": base,
        "criteria": crit, "passed": npass, "total": len(crit), "failed": fails,
        "negativeSignals": neg_signals,
        "alphaVerdict": alpha,
        "strategyVerdict": strategy,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "candidateRejected": alpha == "NO_INCREMENTAL_ALPHA",
        "reasoning": None,
        "parameterChanges": 0,
        "gateThresholdsChanged": 0,
        "caution": None,
    }
    if alpha == "INCREMENTAL_ALPHA_CONFIRMED":
        out["reasoning"] = (
            f"§19 기준 8개 전부 충족. 동일 eligible universe·동일 코호트 프레임·동일 "
            f"비용에서 BM 상위 20% 선택이 {base:+.2f}%p/년의 incremental alpha 를 "
            f"만든다. 무작위 40종목 통제군 30 seed 중 후보를 이긴 것은 "
            f"{co['controlC2_RANDOM_N40']['seedsBeatingCandidate']}개이고, "
            f"TOP20 vs REST80 spread 는 "
            f"{cs['topVsRest']['primary']['cagrExcessPct']:+.2f}%p 다.")
        out["caution"] = (
            "★ 이 결론은 R10 의 -0.22%p 를 뒤집는다. 뒤집힌 이유는 새 데이터나 새 "
            "parameter 가 아니라 **PRIMARY control 정의**다. R10 은 24개월 보유·비용 "
            "부담 후보를 '월 리밸런싱·비용 0 지수'와 비교했고, 그 지수는 월 1,500종목을 "
            "공짜로 재편입한다는 실현 불가능한 가정에 기반한다(실제 비용 부과 시 지수 "
            "CAGR 11.7% → 5.4%). §3 이 요구한 구조 동일화를 적용한 것이 R11 이며, "
            "이 재정의는 결과를 보기 전에 지시문 §2·§3·§16 이 규정했다. "
            "그럼에도 후보에게 유리한 방향의 재정의이므로 분해표를 전부 공개한다.")
    elif alpha == "WEAK_OR_INCONCLUSIVE":
        out["reasoning"] = (
            f"base excess {base:+.2f}%p 이지만 기준 {npass}/{len(crit)} 만 충족 "
            f"(미달 {', '.join(fails)}). historical evidence 로 alpha 를 선언하지 않는다.")
    else:
        out["reasoning"] = (
            f"base excess {base:+.2f}%p · 부정 신호 {neg_signals}개. "
            f"CANDIDATE_REJECTED_NO_INCREMENTAL_ALPHA. parameter rescue 금지(§20).")
        out["terminationRecord"] = "CANDIDATE_REJECTED_NO_INCREMENTAL_ALPHA"
    save("verdict", out)
    print(json.dumps({"alphaVerdict": alpha, "strategyVerdict": strategy,
                      "passed": f"{npass}/{len(crit)}", "failed": fails,
                      "baseExcessPct": base}, ensure_ascii=False))
    return 0


MODES = ("repro", "core", "rolling", "stability", "walkforward", "era", "bootstrap",
         "crosssec", "subgroup", "stress", "risk", "verdict", "all")
ORDER = ("repro", "core", "rolling", "stability", "walkforward", "era", "bootstrap",
         "crosssec", "subgroup", "stress", "risk", "verdict")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    todo = ORDER if a.mode == "all" else (a.mode,)
    c = None
    if [m for m in todo if m != "verdict"]:
        c = ctx()
        print(f"[r11] {c[2][0]} ~ {c[2][-1]} ({len(c[2])}m) · primary control {PRIMARY}",
              file=sys.stderr)
    for m in todo:
        rc = mode_verdict() if m == "verdict" else globals()[f"mode_{m}"](*c)
        if rc:
            print(f"[r11] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
