#!/usr/bin/env python3
"""R14 실행기 — quality/compounder 신호가 장기 horizon 에서 나타나는가.

WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14

모드
  repro      §34-5 R13 12M 재현 (methodology·정의가 동일한지)
  standalone §7  전체 universe 에서 1Y/3Y/5Y/7Y spread
  bmtop20    §8  BM 상위 20% **내부** 장기 incremental  ← 핵심
  overlap    §10 overlapping / annual-start / non-overlapping
  boot       §11 dependency-aware bootstrap (표본이 얇다는 것을 보여주는 용도)
  matched    §15 size / exchange / size+exchange matched
  subgroup   §16 KOSPI·KOSDAQ · SMALL·MID·LARGE
  audit      §17·§18·§19·§20 distress / concentration / 분포 / 보유중 위험
  verdict    §22·§23 factor 판정 → PRIMARY 1개 또는 NONE

정의·horizon·판정기준은 전부 r14_precommit.py 에 결과 이전에 고정됐다.
horizon 최적화 0 · 새 factor 0 · 포트폴리오 최적화 0(§24).

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
from backtest_engine import investable_universe, load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r13_factors as F13  # noqa: E402
from r14_precommit import (  # noqa: E402
    ALL_HORIZONS, PRIMARY_HORIZON, REFERENCE_R13, ROBUSTNESS_HORIZONS,
)
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
BOOT_SEED = 20260821

Q1, Q2, Q3 = "Q1_PROFIT_PERSISTENCE", "Q2_CAPITAL_COMPOUNDING", "Q3_DIVIDEND_DISCIPLINE"
QIDS = [Q1, Q2, Q3]
# R13 정의를 그대로 재사용한다 — 이름만 R14 표기로 바꾼다(§3).
R13_MAP = {Q1: F13.L1, Q2: F13.L3, Q3: F13.L2}
REF = ["ROE", "SIZE"]                       # negative control / attribution 전용
ERAS = [("2010-2013", "2010-01", "2013-12"), ("2014-2017", "2014-01", "2017-12"),
        ("2018-2021", "2018-01", "2021-12"), ("2022-현재", "2022-01", "2099-12")]


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r14-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r14] saved {p.name}", file=sys.stderr)


def load(name):
    return json.loads((RD / f"r14-{name}-latest.json").read_text(encoding="utf-8"))


def ctx():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    lf = F13.LongFactors(sn, ds)
    return sn, nm, ds, lf


def qvalues(lf):
    """R13 LongFactors 값을 R14 이름으로 노출한다. 정의 변경 0."""
    def fn(uni, d, i):
        v = lf.values(uni, d, i)
        return {q: v[R13_MAP[q]] for q in QIDS}
    return fn


def panel(sn, nm, ds, lf, *, ufilter=None, horizons=None, factors=None, **kw):
    return FR.quantile_panel(sn, nm, ds, factors=factors or (QIDS + REF),
                             extra_value_fn=qvalues(lf), universe_filter=ufilter,
                             horizons=horizons or ALL_HORIZONS, **kw)


def summ(acc, uni_acc, names):
    return {f: FR.summarize_factor(acc, uni_acc, f) for f in names if f in acc}


def ann_spread(d, h):
    """R14 주 지표 — **연율수익률의 차이**(§6). 장기 horizon 에서 올바른 정의."""
    return (d.get(h) or {}).get("annSpread") if d else None


def r7_spread(d, h):
    """R7 원 정의(누적차이 연율화). R13 12M 재현 호환용."""
    return (d.get(h) or {}).get("topMinusBottomAnn") if d else None


def sub_dates(dates, lo, hi):
    return [d for d in dates if lo <= d[:7] <= hi]


def bm20():
    return F13.make_bm_top20_filter(0.20)


# ═══════════════ repro — R13 12M 재현 ═══════════════
def mode_repro(sn, nm, ds, lf):
    acc, ua, _ = panel(sn, nm, ds, lf, horizons=[12], factors=QIDS + REF)
    stand = summ(acc, ua, QIDS + REF)
    acc2, ua2, _ = panel(sn, nm, ds, lf, ufilter=bm20(), horizons=[12],
                         factors=QIDS + REF)
    inner = summ(acc2, ua2, QIDS + REF)

    checks = []

    def ck(metric, got, want, tol=0.05):
        ok = got is not None and abs(got - want) <= tol
        checks.append({"metric": metric, "r14": got, "r13Canonical": want,
                       "diff": None if got is None else round(got - want, 3),
                       "tolerance": tol, "pass": ok})

    for q in QIDS:
        ck(f"standalone12M/{q}", pct(r7_spread(stand.get(q), 12)),
           REFERENCE_R13["standalone12M"][q])
        ck(f"bmTop20Internal12M/{q}", pct(r7_spread(inner.get(q), 12)),
           REFERENCE_R13["bmTop20Internal12M"][q])
    ck("sizeInsideBmTop20_12M", pct(r7_spread(inner.get("SIZE"), 12)),
       REFERENCE_R13["sizeInsideBmTop20_12M"])

    ok = all(c["pass"] for c in checks)
    out = {"gate": "R13_REPRODUCTION", "pass": ok,
           "note": ("R14 는 R13 의 L1/L3/L2 정의를 그대로 쓴다(이름만 Q1/Q2/Q3). "
                    "12M 값이 R13 정본과 같아야 같은 factor 를 장기로 확장한 것이 된다. "
                    "재현은 R7 원 지표(누적차이 연율화)로 비교한다."),
           "period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
           "checks": checks,
           "annSpreadNote": ("R14 본검증은 R7 지표가 아니라 **연율수익률 차이**"
                             "(annSpread)를 쓴다. 60/84개월에서 누적차이 연율화는 "
                             "왜곡이 크기 때문이다. 12M 에서는 두 지표가 거의 같다."),
           "annSpread12M": {q: pct(ann_spread(inner.get(q), 12)) for q in QIDS}}
    save("repro", out)
    print(json.dumps({"reproPass": ok,
                      "diffs": {c["metric"]: c["diff"] for c in checks}},
                     ensure_ascii=False))
    return 0 if ok else 3


# ═══════════════ standalone (§7) ═══════════════
def mode_standalone(sn, nm, ds, lf):
    acc, ua, cnt = panel(sn, nm, ds, lf)
    full = summ(acc, ua, QIDS + REF)
    subs = {}
    for lab, lo, hi in ERAS:
        sub = sub_dates(ds, lo, hi)
        if len(sub) < 96:
            continue
        a, u, _ = panel(sn, nm, sub, lf)
        subs[lab] = summ(a, u, QIDS + REF)
    segs, sizes = {}, {}
    for mkt in ("KOSPI", "KOSDAQ"):
        a, u, _ = panel(sn, nm, ds, lf, market=mkt)
        segs[mkt] = summ(a, u, QIDS + REF)
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        a, u, _ = panel(sn, nm, ds, lf, size_bucket=b)
        sizes[lbl] = summ(a, u, QIDS + REF)

    rows = {}
    for f in QIDS + REF:
        rows[f] = {
            "annSpreadPct": {str(h): pct(ann_spread(full.get(f), h)) for h in ALL_HORIZONS},
            "cumSpreadPct": {str(h): pct((full.get(f, {}).get(h) or {}).get("topMinusBottom"))
                             for h in ALL_HORIZONS},
            "topAnnPct": {str(h): pct((full.get(f, {}).get(h) or {}).get("topAnn"))
                          for h in ALL_HORIZONS},
            "bottomAnnPct": {str(h): pct((full.get(f, {}).get(h) or {}).get("bottomAnn"))
                             for h in ALL_HORIZONS},
            "annTopMinusUniversePct": {str(h): pct((full.get(f, {}).get(h) or {})
                                                   .get("annTopMinusUniverse"))
                                       for h in ALL_HORIZONS},
            "gradientCorr": {str(h): (full.get(f, {}).get(h) or {}).get("gradientCorr")
                             for h in ALL_HORIZONS},
            "spreadPositiveRate": {str(h): (full.get(f, {}).get(h) or {})
                                   .get("spreadPositiveRate") for h in ALL_HORIZONS},
            "obsOverlapping": {str(h): (full.get(f, {}).get(h) or {}).get("obsOverlapping")
                               for h in ALL_HORIZONS},
            "obsNonOverlapping": {str(h): (full.get(f, {}).get(h) or {})
                                  .get("obsNonOverlapping") for h in ALL_HORIZONS},
            "quantileAnnPct": {str(h): {k: pct(v) for k, v in
                                        ((full.get(f, {}).get(h) or {})
                                         .get("quantileMeansAnn") or {}).items()}
                               for h in ALL_HORIZONS},
            "subperiods": {lab: {str(h): pct(ann_spread(subs[lab].get(f), h))
                                 for h in ALL_HORIZONS} for lab in subs},
            "marketSegments": {m: {str(h): pct(ann_spread(segs[m].get(f), h))
                                   for h in ALL_HORIZONS} for m in segs},
            "sizeControlled": {s: {str(h): pct(ann_spread(sizes[s].get(f), h))
                                   for h in ALL_HORIZONS} for s in sizes},
            "avgUniverseCount": (cnt.get(f) and round(sum(cnt[f]) / len(cnt[f]), 1)),
        }
    out = {"note": ("전체 PIT universe · 10분위 · TOP−BOTTOM. 주 지표는 **연율수익률 "
                    "차이**(annSpread)다. 누적 spread 도 병기한다(§6)."),
           "horizons": ALL_HORIZONS, "rows": rows}
    save("standalone", out)
    print(json.dumps({q: rows[q]["annSpreadPct"] for q in QIDS}, ensure_ascii=False))
    return 0


# ═══════════════ bmtop20 (§8) — 핵심 ═══════════════
def mode_bmtop20(sn, nm, ds, lf):
    f = bm20()
    acc, ua, cnt = panel(sn, nm, ds, lf, ufilter=f)
    full = summ(acc, ua, QIDS + REF)
    subs = {}
    for lab, lo, hi in ERAS:
        sub = sub_dates(ds, lo, hi)
        if len(sub) < 96:
            continue
        a, u, _ = panel(sn, nm, sub, lf, ufilter=f)
        subs[lab] = summ(a, u, QIDS + REF)
    segs, sizes = {}, {}
    for mkt in ("KOSPI", "KOSDAQ"):
        a, u, _ = panel(sn, nm, ds, lf, ufilter=f, market=mkt)
        segs[mkt] = summ(a, u, QIDS + REF)
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        a, u, _ = panel(sn, nm, ds, lf, ufilter=f, size_bucket=b)
        sizes[lbl] = summ(a, u, QIDS + REF)

    rows = {}
    for fid in QIDS + REF:
        sp = {str(h): pct(ann_spread(full.get(fid), h)) for h in ALL_HORIZONS}
        subv = {lab: {str(h): pct(ann_spread(subs[lab].get(fid), h))
                      for h in ALL_HORIZONS} for lab in subs}
        p5 = [v[str(PRIMARY_HORIZON)] for v in subv.values()
              if v.get(str(PRIMARY_HORIZON)) is not None]
        rows[fid] = {
            "annSpreadPct": sp,
            "cumSpreadPct": {str(h): pct((full.get(fid, {}).get(h) or {})
                                         .get("topMinusBottom")) for h in ALL_HORIZONS},
            "topAnnPct": {str(h): pct((full.get(fid, {}).get(h) or {}).get("topAnn"))
                          for h in ALL_HORIZONS},
            "bottomAnnPct": {str(h): pct((full.get(fid, {}).get(h) or {}).get("bottomAnn"))
                             for h in ALL_HORIZONS},
            "annTopMinusUniversePct": {str(h): pct((full.get(fid, {}).get(h) or {})
                                                   .get("annTopMinusUniverse"))
                                       for h in ALL_HORIZONS},
            "gradientCorr": {str(h): (full.get(fid, {}).get(h) or {}).get("gradientCorr")
                             for h in ALL_HORIZONS},
            "spreadPositiveRate": {str(h): (full.get(fid, {}).get(h) or {})
                                   .get("spreadPositiveRate") for h in ALL_HORIZONS},
            "obsOverlapping": {str(h): (full.get(fid, {}).get(h) or {}).get("obsOverlapping")
                               for h in ALL_HORIZONS},
            "obsNonOverlapping": {str(h): (full.get(fid, {}).get(h) or {})
                                  .get("obsNonOverlapping") for h in ALL_HORIZONS},
            "quantileAnnPct": {str(h): {k: pct(v) for k, v in
                                        ((full.get(fid, {}).get(h) or {})
                                         .get("quantileMeansAnn") or {}).items()}
                               for h in ALL_HORIZONS},
            "subperiods": subv,
            "subperiods5YPositive": f"{sum(1 for x in p5 if x > 0)}/{len(p5)}",
            "marketSegments": {m: {str(h): pct(ann_spread(segs[m].get(fid), h))
                                   for h in ALL_HORIZONS} for m in segs},
            "sizeControlled": {s: {str(h): pct(ann_spread(sizes[s].get(fid), h))
                                   for h in ALL_HORIZONS} for s in sizes},
            "avgUniverseCount": (cnt.get(fid) and round(sum(cnt[fid]) / len(cnt[fid]), 1)),
        }
        rows[fid]["horizonShape"] = _shape(sp)
    out = {"note": ("BM 상위 20%(R11 frozen P20) 안에서 10분위. 질문: 싼 주식 중에서도 "
                    "장기간 이익·자본을 복리성장시킨 기업이 더 좋은가?"),
           "bmPercentile": 0.20, "primaryHorizon": PRIMARY_HORIZON, "rows": rows}
    save("bmtop20", out)
    print(json.dumps({q: {"5Y": rows[q]["annSpreadPct"]["60"],
                          "shape": rows[q]["horizonShape"]} for q in QIDS},
                     ensure_ascii=False))
    return 0


def _shape(sp):
    """§9 horizon shape 분류. 사전확정 규칙."""
    v = [sp.get(str(h)) for h in ALL_HORIZONS]
    if any(x is None for x in v):
        return "UNDETERMINED"
    pos = [x > 0 for x in v]
    if all(x <= 0 for x in v):
        return "D_INVERTED"
    if all(abs(x) < 1.0 for x in v):
        return "C_NO_HORIZON_SIGNAL"
    long_avg = (v[2] + v[3]) / 2      # 5Y·7Y
    short_avg = (v[0] + v[1]) / 2     # 1Y·3Y
    if long_avg > short_avg + 1.0 and v[2] > 0:
        return "A_LONG_HORIZON_STRENGTHENING"
    if short_avg > 0 and long_avg < short_avg - 1.0:
        return "B_SHORT_TERM_ONLY"
    if len(set(pos)) > 1:
        return "E_UNSTABLE"
    return "C_NO_HORIZON_SIGNAL"


# ═══════════════ overlap (§10) ═══════════════
def _cohort_spreads(sn, nm, ds, lf, h, *, ufilter=None, starts=None):
    """시작월별 (TOP분위 연율, BOTTOM분위 연율, 차이) 를 직접 계산한다."""
    px = FR.build_price_index(sn, ds)
    vfn = qvalues(lf)
    out = {q: [] for q in QIDS + REF}
    for i in (starts if starts is not None else range(len(ds))):
        j = i + h
        if j >= len(ds):
            continue
        uni = investable_universe(sn[ds[i]], nm)
        if ufilter is not None:
            uni = ufilter(uni, ds[i], i)
        if len(uni) < FR.N_QUANTILES * FR.MIN_PER_QUANTILE:
            continue
        vals = dict(vfn(uni, ds[i], i))
        base = FR.factor_values(uni)
        for k in REF:
            if k in base:
                vals[k] = base[k]
        for q in QIDS + REF:
            v = {t: x for t, x in (vals.get(q) or {}).items() if t in uni}
            if len(v) < FR.N_QUANTILES * FR.MIN_PER_QUANTILE:
                continue
            items = sorted(v.items(), key=lambda kv: (-kv[1], kv[0]))
            n = len(items)
            k1 = n // FR.N_QUANTILES
            top = [t for t, _ in items[:k1]]
            bot = [t for t, _ in items[n - k1:]]

            def grp(sel):
                rs = [FR.fwd_return(px, i, j, t) for t in sel]
                rs = [x for x in rs if x is not None]
                return sum(rs) / len(rs) if rs else None
            a, b = grp(top), grp(bot)
            if a is None or b is None:
                continue
            aa, ba = FR.ann(a, h), FR.ann(b, h)
            if aa is None or ba is None:
                continue
            out[q].append({"idx": i, "start": ds[i], "end": ds[j],
                           "topAnnPct": pct(aa), "bottomAnnPct": pct(ba),
                           "spreadPct": pct(aa - ba)})
    return out


def _dist(xs):
    if not xs:
        return None
    s = sorted(xs)
    def q(p):
        if len(s) == 1:
            return s[0]
        k = p * (len(s) - 1)
        lo = int(math.floor(k))
        hi = min(lo + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)
    return {"n": len(s), "mean": round(sum(s) / len(s), 2), "median": round(q(0.5), 2),
            "p10": round(q(0.10), 2), "p25": round(q(0.25), 2),
            "p75": round(q(0.75), 2), "p90": round(q(0.90), 2),
            "min": round(s[0], 2), "max": round(s[-1], 2),
            "positiveRatePct": round(100 * sum(1 for x in s if x > 0) / len(s), 1)}


def mode_overlap(sn, nm, ds, lf):
    FIRST = F13.PERSIST_WINDOW
    res = {}
    for h in ALL_HORIZONS:
        all_starts = [i for i in range(FIRST, len(ds) - h)]
        annual = [i for i in all_starts if ds[i][5:7] == "01"]
        nonov = list(range(FIRST, len(ds) - h, h))
        inner = _cohort_spreads(sn, nm, ds, lf, h, ufilter=bm20(), starts=all_starts)
        idx_ann, idx_non = set(annual), set(nonov)
        res[str(h)] = {}
        for q in QIDS + REF:
            rows = inner[q]
            ov = [r["spreadPct"] for r in rows]
            an = [r["spreadPct"] for r in rows if r["idx"] in idx_ann]
            no = [r["spreadPct"] for r in rows if r["idx"] in idx_non]
            # ★ 하위구간은 **코호트 시작일** 기준으로 나눈다.
            #   결정월 창을 4년 era 로 잘라 그 안에서 60개월 forward 를 요구하면
            #   관측이 0 이 된다(실측: subperiods 가 전부 빈 dict 였다).
            #   시작일 기준이면 forward 는 era 밖으로 나가도 되므로 정상 계산된다.
            by_era = {}
            for r in rows:
                for lab, lo, hi in ERAS:
                    if lo <= r["start"][:7] <= hi:
                        by_era.setdefault(lab, []).append(r["spreadPct"])
                        break
            era_d = {lab: _dist(v) for lab, v in by_era.items()}
            era_med = {lab: (era_d[lab] or {}).get("median") for lab in era_d}
            npos = sum(1 for v in era_med.values() if v is not None and v > 0)
            res[str(h)][q] = {
                "OVERLAPPING": _dist(ov), "ANNUAL_START": _dist(an),
                "NON_OVERLAPPING": _dist(no),
                "byStartEra": era_d,
                "byStartEraMedianPct": era_med,
                "startErasPositive": f"{npos}/{len(era_med)}",
                "worstCohort": min(rows, key=lambda r: r["spreadPct"]) if rows else None,
                "bestCohort": max(rows, key=lambda r: r["spreadPct"]) if rows else None,
                "annualRows": [r for r in rows if r["idx"] in idx_ann],
            }
    out = {"note": ("BM 상위20% 내부. 같은 데이터를 세 가지 표본추출로 본다. "
                    "비중첩 관측이 5Y 3개·7Y 2개뿐이라는 사실을 숨기지 않는다(§10)."),
           "byHorizon": res}
    save("overlap", out)
    print(json.dumps({q: {h: (res[h][q]["NON_OVERLAPPING"] or {}).get("n")
                          for h in res} for q in QIDS}, ensure_ascii=False))
    return 0


# ═══════════════ boot (§11) ═══════════════
def mode_boot(sn, nm, ds, lf):
    ov = load("overlap")["byHorizon"][str(PRIMARY_HORIZON)]
    B = 5000
    out = {}
    for q in QIDS + REF:
        rows = ov[q]["annualRows"]
        xs = [r["spreadPct"] for r in rows]
        if len(xs) < 4:
            out[q] = {"skipped": True, "reason": f"연초 코호트 {len(xs)}개 — 부족"}
            continue
        res = {}
        # (1) 연초 코호트 IID bootstrap
        rng = random.Random(BOOT_SEED)
        vals = [sum(rng.choice(xs) for _ in range(len(xs))) / len(xs) for _ in range(B)]
        res["annualCohortBootstrap"] = _boot(vals)
        # (2) moving block bootstrap (연초 코호트 시계열, block=3)
        for Lb in (2, 3):
            rng = random.Random(BOOT_SEED + Lb)
            nb = math.ceil(len(xs) / Lb)
            v2 = []
            for _ in range(B):
                seq = []
                for _ in range(nb):
                    s = rng.randrange(0, len(xs) - Lb + 1)
                    seq.extend(xs[s:s + Lb])
                seq = seq[:len(xs)]
                v2.append(sum(seq) / len(seq))
            res[f"movingBlock_L{Lb}"] = _boot(v2)
        out[q] = {"cohorts": len(xs), "observed": round(sum(xs) / len(xs), 2),
                  "results": res,
                  "minProbPositivePct": min(v["probGt0Pct"] for v in res.values()),
                  "allCiLowerPositive": all(v["ci95LowerPct"] > 0 for v in res.values()),
                  "anyCiIncludesZero": any(v["ci95LowerPct"] <= 0 <= v["ci95UpperPct"]
                                           for v in res.values())}
    out["_note"] = ("연초 코호트 12개(5Y)를 재표집한다. 독립 관측이 사실상 3개이므로 "
                    "이 CI 는 **실제보다 좁다**. 유의성 근거가 아니라 '표본이 얇다'는 "
                    "사실을 수치로 보여주는 용도다(§11 · precommit sampleReality).")
    out["_seed"] = BOOT_SEED
    out["_draws"] = B
    save("boot", out)
    print(json.dumps({q: (out[q].get("observed"), out[q].get("minProbPositivePct"))
                      for q in QIDS if isinstance(out.get(q), dict)}, ensure_ascii=False))
    return 0


def _boot(vals):
    s = sorted(vals)
    n = len(s)
    def q(p):
        k = p * (n - 1)
        lo = int(math.floor(k))
        hi = min(lo + 1, n - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)
    return {"meanPct": round(sum(s) / n, 2), "medianPct": round(q(0.5), 2),
            "ci95LowerPct": round(q(0.025), 2), "ci95UpperPct": round(q(0.975), 2),
            "probGt0Pct": round(100 * sum(1 for x in s if x > 0) / n, 1),
            "probGt1pPct": round(100 * sum(1 for x in s if x > 1.0) / n, 1),
            "probGt2pPct": round(100 * sum(1 for x in s if x > 2.0) / n, 1)}


# ═══════════════ matched (§15) ═══════════════
def mode_matched(sn, nm, ds, lf):
    px = FR.build_price_index(sn, ds)
    vfn = qvalues(lf)
    f20 = bm20()
    H = PRIMARY_HORIZON
    modes = {"SIZE_MATCHED": ("size",), "EXCHANGE_MATCHED": ("mkt",),
             "SIZE_EXCHANGE_MATCHED": ("mkt", "size")}
    # 셀 개수가 모드마다 다르다. 거래소 통제는 셀이 KOSPI/KOSDAQ 2개뿐이라
    # 일괄 '3개 이상' 조건을 쓰면 결과가 통째로 None 이 된다(실측 버그).
    MIN_CELLS = {"SIZE_MATCHED": 3, "EXCHANGE_MATCHED": 2,
                 "SIZE_EXCHANGE_MATCHED": 3}
    acc = {m: {q: [] for q in QIDS + REF} for m in modes}
    cells = {m: {q: [] for q in QIDS + REF} for m in modes}
    for i in range(F13.PERSIST_WINDOW, len(ds) - H):
        j = i + H
        uni = f20(investable_universe(sn[ds[i]], nm), ds[i], i)
        if len(uni) < 60:
            continue
        vals = dict(vfn(uni, ds[i], i))
        base = FR.factor_values(uni)
        for k in REF:
            if k in base:
                vals[k] = base[k]
        order = sorted(uni, key=lambda t: (uni[t]["marketCap"] or 0))
        n = len(order)
        skey = {t: min(4, (k * 5) // n) for k, t in enumerate(order)}
        for mname, dims in modes.items():
            def cell(t):
                c = []
                if "mkt" in dims:
                    c.append(uni[t]["market"])
                if "size" in dims:
                    c.append(skey[t])
                return tuple(c)
            for q in QIDS + REF:
                v = {t: x for t, x in (vals.get(q) or {}).items() if t in uni}
                if len(v) < 40:
                    continue
                groups = {}
                for t, x in v.items():
                    groups.setdefault(cell(t), []).append((t, x))
                hi_a, lo_a, used = [], [], 0
                for _, items in groups.items():
                    if len(items) < 6:
                        continue
                    items.sort(key=lambda kv: (-kv[1], kv[0]))
                    k = max(1, len(items) // 3)
                    def grp(sel):
                        rs = [FR.fwd_return(px, i, j, t) for t, _ in sel]
                        rs = [x for x in rs if x is not None]
                        return sum(rs) / len(rs) if rs else None
                    a, b = grp(items[:k]), grp(items[-k:])
                    if a is None or b is None:
                        continue
                    aa, ba = FR.ann(a, H), FR.ann(b, H)
                    if aa is None or ba is None:
                        continue
                    hi_a.append(aa)
                    lo_a.append(ba)
                    used += 1
                if used >= MIN_CELLS[mname]:
                    acc[mname][q].append(sum(hi_a) / len(hi_a) - sum(lo_a) / len(lo_a))
                    cells[mname][q].append(used)
    out = {"note": ("BM 상위20% 안에서 (거래소) / (시총5분위) / (거래소×시총5분위) 셀별로 "
                    "quality 상위1/3 vs 하위1/3 의 5년 연율수익률 차이를 구하고 셀 평균. "
                    "R13 matched-control 논리 재사용 — 새 matching 설계 0(§15)."),
           "horizonMonths": H,
           "minCellsPerMode": {"SIZE_MATCHED": 3, "EXCHANGE_MATCHED": 2,
                               "SIZE_EXCHANGE_MATCHED": 3},
           "rows": {m: {q: {"months": len(acc[m][q]),
                            "meanAnnSpreadPct": pct(sum(acc[m][q]) / len(acc[m][q]))
                            if acc[m][q] else None,
                            "medianAnnSpreadPct": pct(sorted(acc[m][q])[len(acc[m][q]) // 2])
                            if acc[m][q] else None,
                            "positiveRatePct": round(100 * sum(1 for x in acc[m][q] if x > 0)
                                                     / len(acc[m][q]), 1)
                            if acc[m][q] else None,
                            "avgCellsUsed": round(sum(cells[m][q]) / len(cells[m][q]), 1)
                            if cells[m][q] else None}
                        for q in QIDS + REF} for m in modes}}
    save("matched", out)
    print(json.dumps({m: {q: out["rows"][m][q]["meanAnnSpreadPct"] for q in QIDS}
                      for m in modes}, ensure_ascii=False))
    return 0


# ═══════════════ audit (§17·18·19·20) ═══════════════
def mode_audit(sn, nm, ds, lf):
    px = FR.build_price_index(sn, ds)
    vfn = qvalues(lf)
    f20 = bm20()
    H = PRIMARY_HORIZON
    contrib = {q: {} for q in QIDS}
    posret = {q: {"top": [], "bottom": []} for q in QIDS}
    dis = {q: {"top": _dz(), "bottom": _dz()} for q in QIDS}
    risk = {q: {"top": [], "bottom": []} for q in QIDS}
    bpsaudit = {"n": 0, "shareGrowthDominant": 0, "lowBaseBps": 0, "negPrevEps": 0}
    q3audit = {"n": 0, "epsNearZero": 0, "payoutAtCap": 0, "zeroPayout": 0}
    starts = [i for i in range(F13.PERSIST_WINDOW, len(ds) - H) if ds[i][5:7] == "01"]
    for i in starts:
        j = i + H
        uni = f20(investable_universe(sn[ds[i]], nm), ds[i], i)
        if len(uni) < 60:
            continue
        vals = vfn(uni, ds[i], i)
        fut = sn[ds[j]]
        for q in QIDS:
            v = {t: x for t, x in (vals.get(q) or {}).items() if t in uni}
            if len(v) < FR.N_QUANTILES * FR.MIN_PER_QUANTILE:
                continue
            items = sorted(v.items(), key=lambda kv: (-kv[1], kv[0]))
            n = len(items)
            k1 = n // FR.N_QUANTILES
            for label, sel in (("top", items[:k1]), ("bottom", items[n - k1:])):
                path = []
                for t, _ in sel:
                    r = FR.fwd_return(px, i, j, t)
                    if r is None:
                        continue
                    posret[q][label].append(r)
                    if label == "top":
                        e = contrib[q].setdefault(t, {"n": 0, "sum": 0.0,
                                                      "name": nm.get(t, t)})
                        e["n"] += 1
                        e["sum"] += r
                    _distress(dis[q][label], uni[t], t, fut, lf, ds[i], i)
                # 보유 중 위험(§20) — 동일가중 buy&hold 경로
                path = _hold_path(px, i, j, [t for t, _ in sel])
                if path:
                    risk[q][label].append(path)
        # §13 Q2 audit
        a = lf.all_values(ds[i], i)
        for t in uni:
            g = a[F13.L3].get(t)
            if g is None:
                continue
            bpsaudit["n"] += 1
            b0 = sn[ds[i - F13.BPS_WINDOW]].get(t, {}).get("BPS")
            b1 = sn[ds[i - 1]].get(t, {}).get("BPS")
            s0 = sn[ds[i - F13.BPS_WINDOW]].get(t, {}).get("marketCap")
            s1 = sn[ds[i - 1]].get(t, {}).get("marketCap")
            if b0 and b0 < 1000:
                bpsaudit["lowBaseBps"] += 1
            e0 = sn[ds[i - F13.BPS_WINDOW]].get(t, {}).get("EPS")
            if e0 is not None and e0 <= 0:
                bpsaudit["negPrevEps"] += 1
            # 발행주식수 증가가 BPS 상승을 압도하는지(유상증자 proxy)
            sh0 = (s0 / b0) if (s0 and b0) else None
            sh1 = (s1 / b1) if (s1 and b1) else None
            if sh0 and sh1 and sh1 > sh0 * 1.5:
                bpsaudit["shareGrowthDominant"] += 1
        # §14 Q3 audit
        for t, r in uni.items():
            e = r["EPS"]
            if e is None or e <= 0:
                continue
            q3audit["n"] += 1
            if 0 < e < 100:
                q3audit["epsNearZero"] += 1
            val = a[F13.L2].get(t)
            if val is not None:
                if val >= F13.PAYOUT_CAP - 1e-9:
                    q3audit["payoutAtCap"] += 1
                if val == 0:
                    q3audit["zeroPayout"] += 1

    out = {"horizonMonths": H, "cohortStarts": len(starts),
           "concentration": {}, "returnDistribution": {}, "distressExposure": {},
           "intraHoldingRisk": {},
           "q2CapitalCompoundingAudit": _rate(bpsaudit),
           "q3DividendDisciplineAudit": _rate(q3audit)}
    for q in QIDS:
        items = sorted(contrib[q].items(), key=lambda kv: -kv[1]["sum"])
        pos = sum(e["sum"] for _, e in items if e["sum"] > 0)
        out["concentration"][q] = {
            "tickers": len(items),
            **{f"top{k}SharePct": (round(100 * sum(e["sum"] for _, e in items[:k]) / pos, 2)
                                   if pos else None) for k in (1, 3, 5, 10)},
            "top1PctSharePct": (round(100 * sum(e["sum"] for _, e in
                                                items[:max(1, len(items) // 100)]) / pos, 2)
                                if pos else None),
            "top5PctSharePct": (round(100 * sum(e["sum"] for _, e in
                                                items[:max(1, len(items) // 20)]) / pos, 2)
                                if pos else None),
            "denominatorNote": "분모는 총 이익(gross positive) — 순합은 100% 초과 오해 유발.",
            "top": [{"ticker": t, "name": e["name"], "cohorts": e["n"],
                     "sumFwd5YPct": pct(e["sum"])} for t, e in items[:8]]}
        for label in ("top", "bottom"):
            rs = posret[q][label]
            d = _dist([100 * x for x in rs])
            mean = sum(rs) / len(rs) if rs else 0
            sd = math.sqrt(sum((x - mean) ** 2 for x in rs) / (len(rs) - 1)) if len(rs) > 1 else 0
            out["returnDistribution"].setdefault(q, {})[label] = {
                **(d or {}),
                "winRatePct": round(100 * sum(1 for x in rs if x > 0) / len(rs), 2)
                if rs else None,
                "skewness": round(sum((x - mean) ** 3 for x in rs) / len(rs) / sd ** 3, 3)
                if sd > 0 else None,
                "annualizedMedianPct": pct(FR.ann((d or {}).get("median", 0) / 100, H))
                if d else None}
            out["distressExposure"].setdefault(q, {})[label] = _rate(dis[q][label])
            paths = risk[q][label]
            if paths:
                out["intraHoldingRisk"].setdefault(q, {})[label] = {
                    "cohorts": len(paths),
                    "medianMaxDrawdownPct": round(
                        sorted(p["mdd"] for p in paths)[len(paths) // 2], 2),
                    "worstMaxDrawdownPct": round(min(p["mdd"] for p in paths), 2),
                    "medianWorst1YInsidePct": round(
                        sorted(p["worst1y"] for p in paths)[len(paths) // 2], 2),
                    "worstWorst1YInsidePct": round(min(p["worst1y"] for p in paths), 2),
                    "medianWorst3YInsidePct": round(
                        sorted(p["worst3y"] for p in paths)[len(paths) // 2], 2),
                    "medianMonthsToRecoverPeak": sorted(p["rec"] for p in paths)[len(paths) // 2]}
    save("audit", out)
    print(json.dumps({q: {"top5": out["concentration"][q]["top5SharePct"],
                          "topMDD": (out["intraHoldingRisk"].get(q, {})
                                     .get("top", {}).get("medianMaxDrawdownPct"))}
                      for q in QIDS}, ensure_ascii=False))
    return 0


def _dz():
    return {"n": 0, "delisted": 0, "negEquity": 0, "persistentLoss": 0,
            "lowPrice": 0, "microCap": 0, "extremeBm": 0}


def _distress(acc, r, t, fut, lf, d, i):
    acc["n"] += 1
    if t not in fut or not fut[t]["close"]:
        acc["delisted"] += 1
    if r["BPS"] is not None and r["BPS"] <= 0:
        acc["negEquity"] += 1
    p = lf.all_values(d, i)[F13.L1].get(t)
    if p is not None and p < 0.5:
        acc["persistentLoss"] += 1
    if r["close"] and r["close"] < 1000:
        acc["lowPrice"] += 1
    if (r["marketCap"] or 0) < 500 * 1e8:
        acc["microCap"] += 1
    if r["PBR"] and 0 < r["PBR"] < 0.2:
        acc["extremeBm"] += 1


def _rate(d):
    n = d.get("n") or 0
    return {**{k: (round(100 * v / n, 2) if n else None)
               for k, v in d.items() if k != "n"}, "observations": n}


def _hold_path(px, i, j, tickers):
    """동일가중 buy&hold 월별 경로 → MDD / 보유중 최악 1Y·3Y / 회복개월."""
    base = {}
    for t in tickers:
        p0 = px[i][0].get(t)
        if p0 and p0 > 0:
            base[t] = p0
    if len(base) < 5:
        return None
    nav = []
    for k in range(i, j + 1):
        vals = []
        for t, p0 in base.items():
            p = px[k][0].get(t)
            if p is None:
                p = px[k][1].get(t)
            if p is None:
                continue
            vals.append(p / p0)
        if not vals:
            return None
        nav.append(sum(vals) / len(vals))
    peak, mdd, cur, rec = nav[0], 0.0, 0, 0
    for v in nav:
        if v >= peak:
            peak, cur = v, 0
        else:
            cur += 1
            rec = max(rec, cur)
            mdd = min(mdd, v / peak - 1)
    def worst(m):
        if len(nav) <= m:
            return 0.0
        return min(nav[k + m] / nav[k] - 1 for k in range(len(nav) - m))
    return {"mdd": 100 * mdd, "worst1y": 100 * worst(12), "worst3y": 100 * worst(36),
            "rec": rec}


# ═══════════════ verdict (§22·§23) ═══════════════
def mode_verdict():
    rep, st, bm = load("repro"), load("standalone"), load("bmtop20")
    ov, bo, mt, au = load("overlap"), load("boot"), load("matched"), load("audit")
    H = str(PRIMARY_HORIZON)
    grades = {}
    for q in QIDS:
        r = bm["rows"][q]
        sp = r["annSpreadPct"]
        s5 = sp[H]
        s3, s7, s1 = sp["36"], sp["84"], sp["12"]
        # 하위구간은 overlap 의 **코호트 시작일 기준** 결과를 쓴다(위 주석 참조).
        era_med = ov["byHorizon"][H][q].get("byStartEraMedianPct") or {}
        subs = era_med
        p5 = [v for v in era_med.values() if v is not None]
        segs = [v[H] for v in r["marketSegments"].values() if v.get(H) is not None]
        sizes = [v[H] for v in r["sizeControlled"].values() if v.get(H) is not None]
        mm = {m: mt["rows"][m][q]["meanAnnSpreadPct"] for m in mt["rows"]}
        annual = (ov["byHorizon"][H][q]["ANNUAL_START"] or {})
        nonov = (ov["byHorizon"][H][q]["NON_OVERLAPPING"] or {})
        b = bo.get(q, {})
        top5 = au["concentration"][q]["top5SharePct"]
        conds = {
            "primary5YPositive": bool(s5 is not None and s5 > 0),
            "robustness3YPositive": bool(s3 is not None and s3 > 0),
            "robustness7YPositive": bool(s7 is not None and s7 > 0),
            "annualCohortsMajorityPositive": bool(
                annual.get("positiveRatePct") is not None
                and annual["positiveRatePct"] > 50),
            "subperiodsMostlyPositive": bool(p5 and sum(1 for x in p5 if x > 0)
                                             >= max(1, len(p5) - 1)),
            "matchedControlsHold": all((v or -1) > 0 for v in mm.values()),
            "notConcentrationDriven": bool(top5 is not None and top5 < 50),
            "bootstrapMeaningfullyPositive": bool(b.get("allCiLowerPositive")),
        }
        npass = sum(1 for v in conds.values() if v)
        shape = r["horizonShape"]
        if all(x is not None and x <= 0 for x in (s1, s3, s5, s7)) and \
                all((v or 0) <= 0 for v in mm.values()):
            grade = "INVERTED_LONG_HORIZON"
        elif s5 is None:
            grade = "DATA_INSUFFICIENT"
        elif s5 <= 0:
            grade = "NO_LONG_HORIZON_SIGNAL"
        elif conds["primary5YPositive"] and npass >= 6:
            grade = "LONG_HORIZON_PROMISING"
        else:
            grade = "NO_LONG_HORIZON_SIGNAL"
        grades[q] = {
            "annSpreadPct": sp, "horizonShape": shape,
            "topAnnPct": r["topAnnPct"], "bottomAnnPct": r["bottomAnnPct"],
            "subperiods5YPositive": ov["byHorizon"][H][q].get("startErasPositive"),
            "subperiodsByStartEra": subs, "marketSegments": r["marketSegments"],
            "sizeControlled": r["sizeControlled"],
            "matchedControls": mm,
            "annualStart5Y": annual, "nonOverlapping5Y": nonov,
            "bootstrap": {"observed": b.get("observed"),
                          "minProbPositivePct": b.get("minProbPositivePct"),
                          "allCiLowerPositive": b.get("allCiLowerPositive"),
                          "anyCiIncludesZero": b.get("anyCiIncludesZero")},
            "top5ContribSharePct": top5,
            "standalone": st["rows"][q]["annSpreadPct"],
            "conditions": conds, "conditionsPassed": f"{npass}/{len(conds)}",
            "grade": grade,
        }
    cands = [q for q in QIDS if grades[q]["grade"] in
             ("LONG_HORIZON_STRONG_CANDIDATE", "LONG_HORIZON_PROMISING")]

    def key(q):
        g = grades[q]
        return (-(g["annSpreadPct"][H] or -999),
                -sum(1 for h in ("36", "84") if (g["annSpreadPct"][h] or -1) > 0),
                -sum(1 for v in g["matchedControls"].values() if (v or -1) > 0),
                (g["top5ContribSharePct"] or 999))
    cands.sort(key=key)
    primary = cands[0] if cands else None

    inverted = [q for q in QIDS if grades[q]["grade"] == "INVERTED_LONG_HORIZON"]
    if primary:
        final = ("LONG_HORIZON_PROMISING" if grades[primary]["grade"] ==
                 "LONG_HORIZON_PROMISING" else grades[primary]["grade"])
    elif len(inverted) == len(QIDS):
        final = "INVERTED_LONG_HORIZON"
    else:
        final = "NO_LONG_HORIZON_SIGNAL"

    out = {
        "primaryHorizon": PRIMARY_HORIZON,
        "reproPass": rep["pass"],
        "grades": grades,
        "primaryQualityFactor": primary,
        "primarySelectionRule": ("§23 — CAGR 최고가 아니라 BM 내부 5Y strength → "
                                 "3Y/7Y consistency → matched-control → concentration "
                                 "순. 없으면 NONE."),
        "finalVerdict": final,
        "longHoldHypothesis": (
            "REJECTED — '보유기간이 짧아서 수익성 factor 가 실패했다'는 가설은 "
            "기각된다. horizon 을 1Y→3Y→5Y→7Y 로 늘려도 신호가 나타나지 않는다."
            if final in ("NO_LONG_HORIZON_SIGNAL", "INVERTED_LONG_HORIZON")
            else "SURVIVED — 장기 horizon 에서 신호가 관찰된다."),
        "r15Eligible": final in ("LONG_HORIZON_STRONG_CANDIDATE",
                                 "LONG_HORIZON_PROMISING"),
        "horizonRescue": 0, "parameterRescue": 0, "portfolioSearchPerformed": 0,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "sampleCaveat": (
            "5Y 비중첩 관측 3개 · 7Y 2개. 이 표본으로 양수 결과를 확증할 수 없으므로 "
            "precommit 이 STRONG_CANDIDATE 를 사전에 도달 불가로 설정했다. 반대로 "
            "**모든 horizon·모든 통제에서 일관되게 음수**인 경우의 기각은 표본이 얇아도 "
            "타당하다 — 방향이 한 방향으로만 나오기 때문이다."),
    }
    save("verdict", out)
    print(json.dumps({"final": final, "primary": primary,
                      "grades": {q: grades[q]["grade"] for q in QIDS},
                      "shapes": {q: grades[q]["horizonShape"] for q in QIDS}},
                     ensure_ascii=False))
    return 0


MODES = ("repro", "standalone", "bmtop20", "overlap", "boot", "matched", "audit",
         "verdict", "all")
ORDER = ("repro", "standalone", "bmtop20", "overlap", "boot", "matched", "audit",
         "verdict")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    todo = ORDER if a.mode == "all" else (a.mode,)
    c = None
    if [m for m in todo if m not in ("verdict", "boot")]:
        c = ctx()
        print(f"[r14] {c[2][0]} ~ {c[2][-1]} ({len(c[2])}m) · primary {PRIMARY_HORIZON}M",
              file=sys.stderr)
    for m in todo:
        if m == "verdict":
            rc = mode_verdict()
        elif m == "boot":
            rc = mode_boot(*(c or ctx()))
        else:
            rc = globals()[f"mode_{m}"](*c)
        if rc:
            print(f"[r14] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
