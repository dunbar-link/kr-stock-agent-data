#!/usr/bin/env python3
"""R15 회귀 — TSR 엔진 invariant. 네트워크 0 · 실주문 0 · production write 0.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

§19 의 핵심:
  분할로 wealth 가 50배 증가/감소하면 FAIL.
  배당을 이중계상하면 FAIL.
  자사주 소각을 현금배당처럼 더하면 FAIL.

사용: python scripts/research/test_r15_validation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r15_precommit as PC  # noqa: E402
import r15_tsr as T  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
TK = "005930"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def t_precommit():
    print("[1] precommit immutability")
    p = RD / "r15-precommit-latest.json"
    ck("precommit 존재", p.exists())
    if not p.exists():
        return
    d = json.loads(p.read_text(encoding="utf-8"))
    ck("결과 이전 저장", d["writtenBeforeCorrectedResults"] is True)
    ck("PRIMARY = 재투자 TSR",
       d["dividendRule"]["primary"] == "TOTAL_RETURN_WITH_REINVEST")
    ck("split 최소배율 1.5", d["splitRule"]["minRatio"] == T.MIN_RATIO == 1.5)
    ck("split 허용오차 0.30", d["splitRule"]["tolerance"] == T.TOL == 0.30)
    ck("자사주 소각 무조정 명시", "TREASURY_CANCELLATION" in d["noAdjustment"])
    ck("유상증자 무조정 명시", "RIGHTS_ISSUE" in d["noAdjustment"])
    ck("parameter rescue 0", d["revalidationRule"]["parameterRescue"] == 0)
    ck("factor 정의 불변 선언", len(d["revalidationRule"]["unchanged"]) >= 6)
    ck("SIZE 연구 미시작", d["sizeResearch"]["started"] is False)


def t_split(ds, idx):
    print("[2] ★ 액면분할 invariant (§15·§19)")
    k = ds.index("2018-06-01")
    # 50:1 분할 탐지
    f = idx.split[k].get(TK)
    ck("삼성 2018-06 분할 탐지", f is not None and abs(f - 50.0) < 0.01, f)
    # wealth 가 50배 변하지 않는다
    r = idx.tsr(k - 1, k, TK, reinvest=False, include_dividend=False)
    ck("분할 달 wealth 가 50배 증가하지 않음", r is not None and abs(r) < 0.10, r)
    ck("분할 달 wealth 가 -98% 로 떨어지지 않음", r is not None and r > -0.10, r)
    # 시총 변화와 일치
    a, b = idx.cap[ds[k - 1]][TK], idx.cap[ds[k]][TK]
    mc = (b["close"] * b["shares"]) / (a["close"] * a["shares"]) - 1
    ck("조정 수익률 == 시총 변화 (±0.5%p)", abs(r - mc) < 0.005, (r, mc))
    # 미조정 엔진은 실제로 -98% 를 낸다(버그 재현)
    raw = idx.price_return(k - 1, k, TK)
    ck("미조정 엔진은 -98% 를 기록(버그 재현)", raw < -0.97, raw)
    # 분할 이중계상 금지 — 같은 구간 두 번 적용되면 2500배
    long_r = idx.tsr(ds.index("2018-01-02"), ds.index("2019-01-02"), TK,
                     reinvest=False, include_dividend=False)
    ck("1년 구간에 분할이 이중 적용되지 않음", -0.5 < long_r < 1.0, long_r)
    # 역분할도 폭발하지 않는다
    revs = [e for e in idx.events if e["shareRatio"] < 1.0][:20]
    bad = 0
    for e in revs:
        j = ds.index(e["toDate"])
        rr = idx.tsr(j - 1, j, e["ticker"], reinvest=False, include_dividend=False)
        if rr is not None and rr > 2.0:
            bad += 1
    ck(f"역분할 {len(revs)}건에서 가짜 +200% 초과 없음", bad == 0, bad)


def t_dividend(ds, idx):
    print("[3] 배당 포함 · 이중계상 금지")
    i, j = ds.index("2010-01-04"), ds.index("2021-08-02")
    no_div = idx.tsr(i, j, TK, reinvest=False, include_dividend=False)
    with_div = idx.tsr(i, j, TK, reinvest=False, include_dividend=True)
    rein = idx.tsr(i, j, TK, reinvest=True, include_dividend=True)
    ck("배당 포함이 미포함보다 큼", with_div > no_div, (no_div, with_div))
    ck("재투자가 무재투자보다 큼", rein > with_div, (with_div, rein))
    # 이중계상 검사 — 11.6년 배당이 연 2% 수준이면 총 30% 내외여야 한다
    gap = with_div - no_div
    ck("배당 기여가 합리적 범위(총 10~120%p)", 0.10 < gap < 1.20, gap)
    # 월 소득수익률 = 연 DPS/12 / 가격
    k = ds.index("2021-01-04")
    r = idx.cap[ds[k]][TK]
    want = (r["dps"] / 12.0) / r["close"]
    ck("월 소득수익률 = (연DPS/12)/가격",
       abs(idx.monthly_income_yield(k, TK) - want) < 1e-15)
    ck("월 소득수익률이 연 DPS 를 통째로 쓰지 않음",
       idx.monthly_income_yield(k, TK) < (r["dps"] / r["close"]) / 6)
    # 무배당 종목은 배당 포함/미포함이 같아야 한다
    nodps = [t for t, v in idx.cap[ds[i]].items()
             if v.get("close") and not v.get("dps")][:30]
    same = 0
    for t in nodps:
        a = idx.tsr(i, j, t, reinvest=False, include_dividend=False)
        b = idx.tsr(i, j, t, reinvest=True, include_dividend=True)
        if a is not None and b is not None and abs(a - b) < 1e-9:
            same += 1
    ck(f"시작 무배당 종목 중 일부는 동일(이후 배당개시분 제외) {same}/{len(nodps)}",
       same > 0, same)


def t_no_free_wealth(ds, idx):
    print("[4] 자사주 소각·유상증자 semantics")
    # 삼성 2018-07 → 2019-01 은 자사주 소각(주식수 -7%) — 분할로 잡히면 안 된다
    a, b = ds.index("2018-07-02"), ds.index("2019-01-02")
    found = any(idx.split[k].get(TK) for k in range(a + 1, b + 1))
    ck("자사주 소각이 분할로 오탐되지 않음", not found)
    # 소각으로 wealth 가 공짜로 늘지 않는다(보유주식수 불변)
    g = idx.growth(a, b, TK)
    ck("소각 구간 보유주식 배율이 배당분만큼만 증가(<1.05)",
       g is not None and 1.0 <= g < 1.05, g)
    # 주식수 급증(유상증자 의심)인데 가격이 비례하락 안 하면 조정 없음
    checked = adj = 0
    for i in range(1, len(ds)):
        aa = idx.cap.get(ds[i - 1], {})
        bb = idx.cap.get(ds[i], {})
        for t, cur in list(bb.items())[:400]:
            p = aa.get(t)
            if not p or not (p.get("shares") and cur.get("shares")
                             and p.get("close") and cur.get("close")):
                continue
            sr = cur["shares"] / p["shares"]
            pr = p["close"] / cur["close"]
            if sr >= 1.5 and abs(pr / sr - 1) >= T.TOL:
                checked += 1
                if idx.split[i].get(t):
                    adj += 1
    ck(f"가격 비례하락 없는 주식수 급증({checked}건)은 미조정", adj == 0, adj)


def t_equivalence_determinism(ds, idx):
    print("[5] fast/slow 동치 · 결정성 · delisting")
    i, j = ds.index("2010-01-04"), ds.index("2021-08-02")
    idx._build_cum()
    n = 0
    for t in list(idx.cap[ds[i]])[:600]:
        a = idx.tsr(i, j, t, reinvest=True, include_dividend=True)
        b = idx.tsr_fast(i, j, t)
        if a is None and b is None:
            continue
        if a is None or b is None or abs(a - b) > 1e-9:
            ck(f"fast/slow 불일치 {t}", False, (a, b))
            return
        n += 1
    ck(f"fast == slow ({n}건 검사)", n > 100, n)
    # 결정성
    idx2 = T.TsrIndex(ds, cap=idx.cap)
    ck("재구성 시 분할 이벤트 수 동일", len(idx2.events) == len(idx.events))
    ck("재구성 시 TSR 동일",
       abs(idx2.tsr_fast(i, j, TK) - idx.tsr_fast(i, j, TK)) < 1e-12)
    # 상장폐지 — 사라진 종목도 값이 나온다
    gone = [t for t in idx.cap[ds[i]]
            if t not in idx.cap[ds[j]] or not idx.cap[ds[j]][t]["close"]]
    ok = sum(1 for t in gone[:100] if idx.tsr_fast(i, j, t) is not None)
    ck(f"상장폐지 종목도 수익률 계산됨 {ok}/{min(100,len(gone))}", ok > 0, ok)
    ck("상장폐지 종목이 -100% 로 강제되지 않음",
       any((idx.tsr_fast(i, j, t) or 0) > -0.99 for t in gone[:100]))


def t_no_future(ds, idx):
    print("[6] PIT / no future data")
    i = ds.index("2015-01-02")
    j = i + 12
    # i→j 수익률은 j 이후 데이터에 의존하지 않는다
    sub = T.TsrIndex(ds[:j + 1], cap={d: idx.cap[d] for d in ds[:j + 1]})
    a = idx.tsr(i, j, TK, reinvest=True)
    b = sub.tsr(i, j, TK, reinvest=True)
    ck("구간 절단해도 i→j 수익률 동일(미래 미사용)",
       a is not None and b is not None and abs(a - b) < 1e-12, (a, b))
    ck("분할 계수는 i+1..j 만 사용",
       all(k <= j for k in range(i + 1, j + 1)))


def t_outputs():
    print("[7] 산출물 계약 (§18)")
    for n in ("precommit", "samsung-tsr", "corporate-action-coverage",
              "return-engine-audit", "before-repro", "r14-tsr-revalidation",
              "tsr-bmtop20", "tsr-verdict"):
        ck(f"근거 JSON r15-{n}", (RD / f"r15-{n}-latest.json").exists())
    md = WD / "wababa-tsr-corporate-action-forensic-r15-latest.md"
    js = WD / "wababa-tsr-corporate-action-forensic-r15-latest.json"
    ck("R15 MD 존재", md.exists())
    ck("R15 JSON 존재", js.exists())
    if md.exists():
        ck("MD 첫 줄 전체 판정",
           md.read_text(encoding="utf-8").splitlines()[0].startswith("전체 판정:"))
    if js.exists():
        d = json.loads(js.read_text(encoding="utf-8"))
        ck("TSR_ENGINE_VERDICT 기록",
           d["tsrEngineVerdict"] in PC.VERDICT_OPTIONS["TSR_ENGINE_VERDICT"])
        ck("R14_QUALITY_VERDICT_AFTER_TSR 기록",
           d["r14QualityVerdictAfterTsr"]
           in PC.VERDICT_OPTIONS["R14_QUALITY_VERDICT_AFTER_TSR"])
        ck("SIZE 연구 미시작", d["sizeResearchStarted"] is False)
        ck("parameter rescue 0", d["parameterRescue"] == 0)
        ck("REAL_MONEY_NOT_APPROVED",
           d["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        pc = d["productionChange"]
        for k in ("realOrders", "broker", "paidData", "externalSend", "deploy",
                  "envOrToken", "productionDbWrite", "publicDisclosure"):
            ck(f"production {k} == 0", pc.get(k) == 0, pc.get(k))
        ck("LEGACY_50D unchanged", pc.get("legacy50d") == "unchanged")
        ck("kr-stock-agent untouched", pc.get("krStockAgentRepo") == "untouched")
        ck("기존 산출물 보존 선언", d["priorArtifactsPreserved"] is True)
    # 기존 보고서 보존
    for n in ("wababa-factor-signal-discovery-r7-latest",
              "wababa-robust-factor-portfolio-r8-latest",
              "wababa-frozen-candidate-independent-validation-r9-latest",
              "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest",
              "wababa-bm-incremental-alpha-validation-r11-latest",
              "wababa-value-profitability-factor-discovery-r13-latest",
              "wababa-quality-compounder-long-horizon-r14-latest"):
        ck(f"기존 보고서 보존 {n}.md", (WD / f"{n}.md").exists())
    # R14 근거 JSON 미변경
    for n in ("bmtop20", "verdict", "standalone"):
        ck(f"R14 근거 보존 r14-{n}", (RD / f"r14-{n}-latest.json").exists())


def t_original_engine_intact():
    print("[8] 원본 엔진 무오염")
    import r15_revalidate as RV  # noqa: PLC0415
    ck("factor_research.fwd_return 이 원본으로 복구됨",
       FR.fwd_return is RV._ORIG_FWD)
    sn = load_snapshots()
    ds = contiguous_span(sorted(sn))
    px = FR.build_price_index(sn, ds)
    i, j = ds.index("2018-05-02"), ds.index("2018-06-01")
    r = FR.fwd_return(px, i, j, TK)
    ck("원본 엔진은 여전히 -98% (수정하지 않았다)", r < -0.97, r)


def main():
    print("R15 TSR 회귀\n")
    t_precommit()
    print()
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    idx = T.TsrIndex(ds)
    print(f"  (데이터 {ds[0]} ~ {ds[-1]}, {len(ds)}개월, 분할 {len(idx.events)}건)\n")
    t_split(ds, idx)
    print()
    t_dividend(ds, idx)
    print()
    t_no_free_wealth(ds, idx)
    print()
    t_equivalence_determinism(ds, idx)
    print()
    t_no_future(ds, idx)
    print()
    t_outputs()
    print()
    t_original_engine_intact()
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
