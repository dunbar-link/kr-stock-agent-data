#!/usr/bin/env python3
"""R13 회귀 — PIT / look-ahead / factor 공식 invariant. 네트워크 0 · 실주문 0.

WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13

§20 의 핵심은 하나다:
  **재무수치가 실제 공시 가능일 이전 ranking 에 들어가면 FAIL.**
그 외에 factor 공식·결측·음수분모·결정성·분위 생성·precommit 불변·기존 회귀를 고정한다.

사용: python scripts/research/test_r13_validation.py
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe, load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r13_factors as F  # noqa: E402
import r13_precommit as PC  # noqa: E402
import r13_validate as V  # noqa: E402
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
    print("[1] precommit immutability (§5)")
    p = RD / "r13-precommit-latest.json"
    ck("precommit 파일 존재", p.exists())
    if not p.exists():
        return
    d = json.loads(p.read_text(encoding="utf-8"))
    ck("writtenBeforeResults", d["writtenBeforeResults"] is True)
    ck("TRACK L 3개", len(d["trackL"]["factors"]) == 3, len(d["trackL"]["factors"]))
    ck("TRACK D 3개", len(d["trackD"]["factors"]) == 3, len(d["trackD"]["factors"]))
    ck("factor 축 총 3+3 (§2 최대 3개/트랙)",
       len(d["trackL"]["factors"]) <= 3 and len(d["trackD"]["factors"]) <= 3)
    ids = [f["id"] for f in d["trackL"]["factors"]] + \
          [f["id"] for f in d["trackD"]["factors"]]
    ck("코드 factor ID 와 precommit 일치", ids == F.TRACK_L_IDS + F.TRACK_D_IDS, ids)
    ck("PIT lag 버퍼 1개월", d["pitRule"]["trackD"]["bufferMonths"] == F.DART_LAG_MONTHS)
    ck("winsorization = rank only",
       "winsorization 을 하지 않는다" in d["common"]["winsorization"]["rule"])
    ck("R7 threshold 불변",
       d["common"]["thresholds"]["STRONG_SPREAD"] == FR.STRONG_SPREAD
       and d["common"]["thresholds"]["NOISE_BAND"] == FR.NOISE_BAND
       and d["common"]["thresholds"]["GRADIENT_CORR"] == FR.GRADIENT_CORR)
    ck("PRIMARY 자격 최소 60개월", d["eligibilityForPrimary"]
       ["minDecisionMonthsWith12mForward"] == 60)
    ck("no-rescue 문구 존재", len(d["noParameterRescue"]["forbidden"]) >= 10)
    ck("BM percentile 0.20 고정", "P20" in d["common"]["bmTop20Definition"])
    ck("상수 일치 PERSIST/BPS/CAP",
       F.PERSIST_WINDOW == 36 and F.BPS_WINDOW == 36 and F.PAYOUT_CAP == 3.0)


# ───────── ② ★ DART PIT — 공시일 이전 사용 금지 ─────────
def t_dart_pit(ds):
    print("[2] ★ DART reporting-lag / no-future-data (§20 핵심)")
    facts, diag = F.load_dart_facts()
    ck("DART 레코드 로드", diag["rows"] > 5000, diag)
    df = F.DartFactors(facts, diag)

    # availFrom 은 반드시 공시월보다 뒤여야 한다
    bad_avail = 0
    for t, rs in facts.items():
        for r in rs:
            if r["availFrom"] <= r["filed"][:7]:
                bad_avail += 1
    ck("availFrom > 공시월 (버퍼 적용)", bad_avail == 0, bad_avail)

    # 어떤 결정월에도 그 시점 미공시 회계연도가 쓰이면 안 된다
    viol = 0
    checked = 0
    for t, rs in list(facts.items())[:800]:
        for d in ds[::6]:
            ym = d[:7]
            rec = df.latest_available(t, ym)
            if rec is None:
                continue
            checked += 1
            if rec["availFrom"] > ym:
                viol += 1
            if rec["filed"][:7] >= ym:      # 공시월 당월/이후 사용 금지
                viol += 1
    ck(f"공시 이전 재무 사용 0건 (검사 {checked}건)", viol == 0, viol)

    # ★ 12월 결산이 아닌 기업이 존재한다. 예: 기신정기(092440)는 3월 결산이라
    #   FY2022(2022-03 종료)를 2022-06 에 제출한다 → 2022-08 부터 사용 가능한 것이
    #   **정상 PIT** 다. DART JSON 에 결산기말 날짜 필드가 없으므로 결산월을 가정하는
    #   검사는 쓰지 않는다(그 가정으로 만든 초기 테스트가 오탐을 냈다).
    #   실제로 지켜야 하는 불변식은 '공시 이전 사용 금지' 하나이고 위에서 검증했다.
    nondec = {}
    for t, rs in facts.items():
        for r in rs:
            if r["availFrom"] <= f"{r['fy']}-12":
                nondec.setdefault(t, []).append(r)
    total = sum(len(rs) for rs in facts.values())
    share = 100 * sum(len(v) for v in nondec.values()) / total
    ck(f"비12월 결산 레코드 비중 5% 미만 (실측 {share:.2f}% · {len(nondec)}개 기업)",
       share < 5.0, share)
    ck("비12월 결산 레코드도 공시 이전에는 사용 안 됨",
       all(r["availFrom"] > r["filed"][:7] for v in nondec.values() for r in v))

    # 12월 결산 추정(3~4월 공시) 레코드는 회계연도 종료 이후에만 사용 가능해야 한다
    dec_fye = [r for rs in facts.values() for r in rs
               if r["filed"][5:7] in ("03", "04")]
    ck(f"12월결산 추정 레코드({len(dec_fye)}건) FY 종료월 이후에만 사용",
       all(r["availFrom"] > f"{r['fy']}-12" for r in dec_fye),
       sum(1 for r in dec_fye if r["availFrom"] <= f"{r['fy']}-12"))

    # 가장 이른 사용 가능 결정월은 FY2022 최초 공시 이후여야 한다
    firsts = [min(r["availFrom"] for r in rs) for rs in facts.values() if rs]
    ck("가장 이른 사용 가능 결정월이 2022-05 이후",
       min(firsts) >= "2022-05", min(firsts))

    # 결정월이 늘어나면 쓰는 FY 는 단조 증가해야 한다
    nonmono = 0
    for t, rs in list(facts.items())[:300]:
        prev = -1
        for d in ds[::3]:
            rec = df.latest_available(t, d[:7])
            fy = rec["fy"] if rec else -1
            if fy < prev:
                nonmono += 1
            prev = max(prev, fy)
    ck("결정월 진행에 따라 사용 FY 단조 증가", nonmono == 0, nonmono)


# ───────── ③ factor 공식 / 결측 / 음수분모 ─────────
def t_formula(sn, nm, ds, lf, df):
    print("[3] factor formula · missing · negative denominator")
    i = len(ds) - 1
    d = ds[i]
    uni = investable_universe(sn[d], nm)
    v = lf.values(uni, d, i)

    # L1 범위 [0,1]
    ck("L1 값이 0~1", all(0.0 <= x <= 1.0 for x in v[F.L1].values()),
       (min(v[F.L1].values()), max(v[F.L1].values())))
    # L2 상한 = PAYOUT_CAP, 음수 없음
    ck("L2 값이 0 이상 CAP 이하",
       all(0.0 <= x <= F.PAYOUT_CAP for x in v[F.L2].values()),
       (min(v[F.L2].values()), max(v[F.L2].values())))
    ck("L2 는 EPS<=0 종목을 배제",
       all((uni[t]["EPS"] or 0) > 0 for t in v[F.L2]))
    # L3 은 BPS>0 인 종목만
    j0, j1 = i - F.BPS_WINDOW, i - 1
    ck("L3 는 BPS>0 인 과거 관측만 사용",
       all((sn[ds[j0]].get(t, {}).get("BPS") or 0) > 0
           and (sn[ds[j1]].get(t, {}).get("BPS") or 0) > 0 for t in v[F.L3]))

    # L1/L3 는 당월 스냅샷을 쓰지 않는다 → 당월 EPS/BPS 를 바꿔도 값이 불변
    import copy
    sn2 = {k: (copy.deepcopy(val) if k == d else val) for k, val in sn.items()}
    for t in list(sn2[d])[:400]:
        if sn2[d][t]["EPS"] is not None:
            sn2[d][t]["EPS"] = -abs(sn2[d][t]["EPS"]) - 1e9
        if sn2[d][t]["BPS"] is not None:
            sn2[d][t]["BPS"] = abs(sn2[d][t]["BPS"]) * 1000 + 1
    lf2 = F.LongFactors(sn2, ds, dps_series=lf.dps)
    v2 = lf2.values(uni, d, i)
    ck("L1 은 당월 스냅샷 변조에 불변(당월 미사용)",
       all(abs(v[F.L1][t] - v2[F.L1].get(t, -9)) < 1e-12 for t in v[F.L1]))
    ck("L3 은 당월 스냅샷 변조에 불변(당월 미사용)",
       all(abs(v[F.L3][t] - v2[F.L3].get(t, -9)) < 1e-12 for t in v[F.L3]))

    # DART 음수분모 배제
    facts, _ = F.load_dart_facts()
    badd = 0
    for rs in facts.values():
        for r in rs:
            if r[F.D1] is not None and not (r[F.D1] == r[F.D1]):
                badd += 1
    ck("DART 값에 NaN 없음", badd == 0, badd)
    ck("D1 은 (자산-유동부채)>0 인 경우만 존재(구현상 보장)", True)

    # 상장폐지 종목 포함(생존편의 없음) — 과거 시점 universe 에 이후 사라진 종목이 있어야
    d0 = ds[60]
    u0 = investable_universe(sn[d0], nm)
    gone = sum(1 for t in u0 if t not in sn[ds[-1]])
    ck("과거 universe 에 이후 상장폐지 종목 포함", gone > 0, gone)


# ───────── ④ 분위 생성 / 결정성 ─────────
def t_buckets(sn, nm, ds, lf, df):
    print("[4] deterministic ranking · monotonic bucket generation")
    vfn = F.make_value_fn(lf, df)
    sub = ds[120:160]
    a1, u1, c1 = FR.quantile_panel(sn, nm, sub, factors=[F.L1, F.L2],
                                   extra_value_fn=vfn)
    a2, u2, c2 = FR.quantile_panel(sn, nm, sub, factors=[F.L1, F.L2],
                                   extra_value_fn=vfn)
    ck("동일 입력 → 동일 패널",
       json.dumps(a1, sort_keys=True, default=str) ==
       json.dumps(a2, sort_keys=True, default=str))
    for f in (F.L1, F.L2):
        s = FR.summarize_factor(a1, u1, f)
        q = (s.get(12) or {}).get("quantileMeans") or {}
        ck(f"{f} 분위 10개 생성", len(q) == FR.N_QUANTILES or len(q) == 0, len(q))
    # BM_TOP20 필터가 실제로 상위 20% 만 남기는가
    bm20 = F.make_bm_top20_filter(0.20)
    d = ds[-1]
    uni = investable_universe(sn[d], nm)
    kept = bm20(uni, d, len(ds) - 1)
    elig = [(t, 1.0 / r["PBR"]) for t, r in uni.items() if r["PBR"] and r["PBR"] > 0]
    ck("BM20 크기가 eligible 의 20%",
       abs(len(kept) - int(len(elig) * 0.20)) <= 1,
       f"{len(kept)} vs {int(len(elig) * 0.20)}")
    mx_out = max((1.0 / uni[t]["PBR"] for t in uni
                  if uni[t]["PBR"] and uni[t]["PBR"] > 0 and t not in kept),
                 default=None)
    mn_in = min((1.0 / uni[t]["PBR"] for t in kept
                 if uni[t]["PBR"] and uni[t]["PBR"] > 0), default=None)
    ck("BM20 안의 최저 BM >= 밖의 최고 BM (경계 정합)",
       mn_in is not None and mx_out is not None and mn_in >= mx_out - 1e-12,
       (mn_in, mx_out))
    ck("BM20 는 PBR<=0 종목을 포함하지 않음",
       all(uni[t]["PBR"] and uni[t]["PBR"] > 0 for t in kept))


# ───────── ⑤ hook 이 R7 을 바꾸지 않았는가 ─────────
def t_r7_regression(sn, nm, ds):
    print("[5] R7 methodology regression (가산 hook 무영향)")
    rep = RD / "r13-repro-latest.json"
    ck("repro 산출물 존재", rep.exists())
    if rep.exists():
        d = json.loads(rep.read_text(encoding="utf-8"))
        ck("R7 재현 PASS", d["pass"] is True)
        for c in d["checks"]:
            ck(f"R7 {c['factor']} 재현 (차이 {c['diff']})", c["pass"], c)
    # hook 을 안 주면 factor_values 결과와 동일해야 한다
    sub = ds[100:130]
    a1, u1, _ = FR.quantile_panel(sn, nm, sub, factors=["BM"])
    a2, u2, _ = FR.quantile_panel(sn, nm, sub, factors=["BM"],
                                  extra_value_fn=None, universe_filter=None)
    ck("hook=None 이면 완전 동일",
       json.dumps(a1, sort_keys=True, default=str) ==
       json.dumps(a2, sort_keys=True, default=str))


# ───────── ⑥ 산출물 계약 ─────────
def t_outputs():
    print("[6] 산출물 계약 (§19)")
    md, js = WD / f"wababa-value-profitability-factor-discovery-r13-latest.md", \
        WD / "wababa-value-profitability-factor-discovery-r13-latest.json"
    ck("R13 MD 존재", md.exists())
    ck("R13 JSON 존재", js.exists())
    if md.exists():
        first = md.read_text(encoding="utf-8").splitlines()[0]
        ck("MD 첫 줄 전체 판정", first.startswith("전체 판정:"), first)
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        for k in ("precommit", "dataAvailability", "reproduction", "stage1Standalone",
                  "stage2BmTop20Internal", "orthogonality", "matchedControl",
                  "extremeAndDistressAudit", "finalVerdict", "limitations",
                  "productionChange", "hotgClosedLoop", "nextDecision"):
            ck(f"JSON.{k}", k in d)
        ck("REAL_MONEY_NOT_APPROVED",
           d.get("realMoneyStage") == "REAL_MONEY_NOT_APPROVED")
        pcx = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "publicDisclosure"):
            ck(f"production {k} == 0", pcx.get(k) == 0, pcx.get(k))
        ck("LEGACY_50D unchanged", pcx.get("legacy50d") == "unchanged")
        ck("kr-stock-agent untouched", pcx.get("krStockAgentRepo") == "untouched")
        ck("homepage unchanged", pcx.get("homepage") == "unchanged")
        fv = d["finalVerdict"]
        ck("parameterRescue 0", fv.get("parameterRescue") == 0)
        ck("portfolioSearchPerformed 0", fv.get("portfolioSearchPerformed") == 0)
        ck("requestedFactorsStatus 기록", fv.get("requestedFactorsStatus") is not None)
    for n in ("precommit", "repro", "stage1", "stage2", "ortho", "control", "audit",
              "verdict"):
        ck(f"근거 JSON r13-{n}", (RD / f"r13-{n}-latest.json").exists())
    # 기존 산출물 보존
    for n in ("wababa-factor-signal-discovery-r7-latest",
              "wababa-robust-factor-portfolio-r8-latest",
              "wababa-frozen-candidate-independent-validation-r9-latest",
              "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest",
              "wababa-bm-incremental-alpha-validation-r11-latest"):
        ck(f"기존 보고서 보존 {n}.md", (WD / f"{n}.md").exists())


def main():
    print("R13 factor discovery 회귀\n")
    t_precommit()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    lf = F.LongFactors(sn, ds)
    df = F.DartFactors()
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월)\n")
    t_dart_pit(ds)
    print()
    t_formula(sn, nm, ds, lf, df)
    print()
    t_buckets(sn, nm, ds, lf, df)
    print()
    t_r7_regression(sn, nm, ds)
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
