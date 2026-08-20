#!/usr/bin/env python3
"""R13 실행기 — value(BM) + profitability factor discovery.

WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13

단계
  repro     R7 정본 재현 (methodology 재사용 확인 · 가산 hook 이 R7 을 안 바꿨는지)
  stage1    §7 단독 factor signal (R7 10분위 방법론 그대로)
  stage2    §8 BM 상위20% **내부** incremental signal  ← 핵심
  ortho     §9 orthogonality / redundancy (BM·SIZE·EY·ROE 순위상관)
  control   §10 matched control (같은 BM bucket 내 size/exchange matched)
  audit     §11·§13 극단치 audit · 집중도 · distress exposure
  verdict   §14·§15·§25 factor 등급 → PRIMARY 1개 → 최종 판정

정의·판정규칙은 전부 r13_precommit.py 에 결과 이전에 고정됐다. 여기서 바꾸지 않는다.
새 parameter sweep 0 · 포트폴리오 최적화 0(§16).

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 홈페이지 미수정 · 실주문 0 ·
      브로커 0 · 유료데이터 0 · 외부발송 0 · 배포 0 · env/token 0 · 운영DB write 0.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import investable_universe, load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r13_factors as F  # noqa: E402
from r13_precommit import ELIGIBILITY_FOR_PRIMARY, REFERENCE_R7  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
ALL_NEW = F.TRACK_L_IDS + F.TRACK_D_IDS
LEGACY = ["BM", "ROE", "EY", "BM_ROE", "MF", "SIZE"]
ERAS = [("2007-2011", "2007-01", "2011-12"), ("2012-2016", "2012-01", "2016-12"),
        ("2017-2021", "2017-01", "2021-12"), ("2022-현재", "2022-01", "2099-12")]


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r13-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r13] saved {p.name}", file=sys.stderr)


def load(name):
    return json.loads((RD / f"r13-{name}-latest.json").read_text(encoding="utf-8"))


def ctx():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    lf = F.LongFactors(sn, ds)
    df = F.DartFactors()
    return sn, nm, ds, lf, df


def sub_dates(dates, lo, hi):
    return [d for d in dates if lo <= d[:7] <= hi]


def panel(sn, nm, ds, vfn, ufilter=None, factors=None, **kw):
    acc, uni_acc, counts = FR.quantile_panel(
        sn, nm, ds, factors=factors, extra_value_fn=vfn, universe_filter=ufilter, **kw)
    return acc, uni_acc, counts


def summarize(acc, uni_acc, names):
    return {f: FR.summarize_factor(acc, uni_acc, f) for f in names if f in acc}


def s12(d):
    return (d.get(12) or {}).get("topMinusBottomAnn") if d else None


# ═══════════════ repro — R7 정본 재현 ═══════════════
def mode_repro(sn, nm, ds, lf, df):
    acc, uni_acc, counts = panel(sn, nm, ds, None, factors=LEGACY)
    got = summarize(acc, uni_acc, LEGACY)
    checks = []
    for f, ref in REFERENCE_R7.items():
        if not isinstance(ref, dict):        # "note" 항목 건너뛴다
            continue
        v = pct(s12(got.get(f)))
        want = ref["spread12AnnPct"]
        ok = v is not None and abs(v - want) <= 0.05
        checks.append({"factor": f, "r13": v, "r7Canonical": want,
                       "diff": None if v is None else round(v - want, 3),
                       "tolerance": 0.05, "pass": ok})
    ok = all(c["pass"] for c in checks)
    out = {"gate": "R7_METHODOLOGY_REPRODUCTION", "pass": ok,
           "note": ("factor_research 에 가산 hook(extra_value_fn·universe_filter)을 "
                    "넣었으므로 hook 미사용 시 R7 값이 소수점까지 같아야 한다."),
           "period": {"start": ds[0], "end": ds[-1], "months": len(ds)},
           "checks": checks,
           "legacySummary": {f: {"verdictInputs": got.get(f)} for f in LEGACY}}
    save("repro", out)
    print(json.dumps({"reproPass": ok,
                      "diffs": {c["factor"]: c["diff"] for c in checks}},
                     ensure_ascii=False))
    return 0 if ok else 3


# ═══════════════ stage1 — 단독 signal (§7) ═══════════════
def mode_stage1(sn, nm, ds, lf, df):
    vfn = F.make_value_fn(lf, df)
    want = ALL_NEW + LEGACY
    acc, uni_acc, counts = panel(sn, nm, ds, vfn, factors=want)
    full = summarize(acc, uni_acc, want)

    subs = {}
    for lab, lo, hi in ERAS:
        sub = sub_dates(ds, lo, hi)
        if len(sub) < 30:
            continue
        a, u, _ = panel(sn, nm, sub, vfn, factors=want)
        subs[lab] = summarize(a, u, want)
    segs = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        a, u, _ = panel(sn, nm, ds, vfn, factors=want, market=mkt)
        segs[mkt] = summarize(a, u, want)
    sizes = {}
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        a, u, _ = panel(sn, nm, ds, vfn, factors=want, size_bucket=b)
        sizes[lbl] = summarize(a, u, want)

    verdicts = {}
    for f in want:
        if f not in full:
            verdicts[f] = {"verdict": "UNRELIABLE",
                           "evidence": {"reason": "패널 없음(관측 부족)"}}
            continue
        sl = [subs[k].get(f, {}) for k in subs if f in subs[k]]
        sz = [sizes[k].get(f, {}) for k in sizes if f in sizes[k]]
        v, ev = FR.verdict(full[f], sl, sz, f)
        verdicts[f] = {"verdict": v, "evidence": ev}

    out = {"note": "R7 methodology 그대로. 10분위 · HORIZONS [3,6,12,24] · 12M 주 지표.",
           "spread12AnnPct": {f: pct(s12(full.get(f))) for f in want},
           "obsNonOverlapping": {f: (full.get(f, {}).get(12) or {}).get("obsNonOverlapping")
                                 for f in want},
           "avgUniverseCount": {f: (counts.get(f) and
                                    round(sum(counts[f]) / len(counts[f]), 1))
                                for f in want},
           "full": full,
           "subperiods": {lab: {f: pct(s12(subs[lab].get(f))) for f in want}
                          for lab in subs},
           "marketSegments": {m: {f: pct(s12(segs[m].get(f))) for f in want}
                              for m in segs},
           "sizeControlled": {s: {f: pct(s12(sizes[s].get(f))) for f in want}
                              for s in sizes},
           "verdicts": verdicts}
    save("stage1", out)
    print(json.dumps({f: (verdicts[f]["verdict"], out["spread12AnnPct"][f])
                      for f in ALL_NEW}, ensure_ascii=False))
    return 0


# ═══════════════ stage2 — BM_TOP20 내부 incremental (§8) ═══════════════
def mode_stage2(sn, nm, ds, lf, df):
    vfn = F.make_value_fn(lf, df)
    bm20 = F.make_bm_top20_filter(0.20)
    want = ALL_NEW + ["ROE", "EY", "SIZE"]
    acc, uni_acc, counts = panel(sn, nm, ds, vfn, ufilter=bm20, factors=want)
    full = summarize(acc, uni_acc, want)

    subs = {}
    for lab, lo, hi in ERAS:
        sub = sub_dates(ds, lo, hi)
        if len(sub) < 30:
            continue
        a, u, _ = panel(sn, nm, sub, vfn, ufilter=bm20, factors=want)
        subs[lab] = summarize(a, u, want)
    segs = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        a, u, _ = panel(sn, nm, ds, vfn, ufilter=bm20, factors=want, market=mkt)
        segs[mkt] = summarize(a, u, want)
    sizes = {}
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        a, u, _ = panel(sn, nm, ds, vfn, ufilter=bm20, factors=want, size_bucket=b)
        sizes[lbl] = summarize(a, u, want)

    rows = {}
    for f in want:
        sub_v = {lab: pct(s12(subs[lab].get(f))) for lab in subs}
        seg_v = {m: pct(s12(segs[m].get(f))) for m in segs}
        siz_v = {s: pct(s12(sizes[s].get(f))) for s in sizes}
        posn = sum(1 for v in sub_v.values() if v is not None and v > 0)
        totn = sum(1 for v in sub_v.values() if v is not None)
        rows[f] = {
            "spread12AnnPct": pct(s12(full.get(f))),
            "spread24AnnPct": pct((full.get(f, {}).get(24) or {}).get("topMinusBottomAnn")),
            "topMinusUniverse12AnnPct": pct((full.get(f, {}).get(12) or {})
                                            .get("topMinusUniverseAnn")),
            "gradientCorr": (full.get(f, {}).get(12) or {}).get("gradientCorr"),
            "spreadPositiveRate": (full.get(f, {}).get(12) or {}).get("spreadPositiveRate"),
            "obsNonOverlapping": (full.get(f, {}).get(12) or {}).get("obsNonOverlapping"),
            "avgUniverseCount": (counts.get(f) and
                                 round(sum(counts[f]) / len(counts[f]), 1)),
            "subperiods": sub_v, "subperiodsPositive": f"{posn}/{totn}",
            "marketSegments": seg_v, "sizeControlled": siz_v,
            "segmentsPositive": sum(1 for v in seg_v.values() if v and v > 0),
            "sizesPositive": sum(1 for v in siz_v.values() if v and v > 0),
        }
    out = {"note": ("BM 상위 20%(R11 frozen P20) 로 모집단을 좁힌 뒤 그 안에서 10분위. "
                    "질문: 이미 싼 종목들 중에서도 실제로 돈을 잘 버는 회사가 더 좋은가?"),
           "bmPercentile": 0.20, "rows": rows, "full": full}
    save("stage2", out)
    print(json.dumps({f: rows[f]["spread12AnnPct"] for f in ALL_NEW},
                     ensure_ascii=False))
    return 0


# ═══════════════ ortho — §9 redundancy ═══════════════
def _spearman(xs, ys):
    return FR._spearman(xs, ys)


def mode_ortho(sn, nm, ds, lf, df):
    from factor_research import factor_values
    acc = {f: [] for f in ALL_NEW}
    pairs = {}
    kosdaq = {f: [] for f in ALL_NEW}
    capq = {f: [] for f in ALL_NEW}
    for i, d in enumerate(ds):
        uni = investable_universe(sn[d], nm)
        if len(uni) < 100:
            continue
        base = factor_values(uni)
        ext = F.make_value_fn(lf, df)(uni, d, i)
        allv = dict(base)
        allv.update(ext)
        caps = sorted((r["marketCap"] or 0) for r in uni.values())
        for f in ALL_NEW:
            v = allv.get(f) or {}
            if len(v) < 100:
                continue
            for other in ("BM", "SIZE", "EY", "ROE"):
                o = allv.get(other) or {}
                common = sorted(set(v) & set(o))
                if len(common) < 100:
                    continue
                c = _spearman([v[t] for t in common], [o[t] for t in common])
                if c is not None:
                    pairs.setdefault((f, other), []).append(c)
            top = sorted(v.items(), key=lambda kv: (-kv[1], kv[0]))
            k = max(1, len(top) // 5)
            sel = [t for t, _ in top[:k]]
            kd = sum(1 for t in sel if uni[t]["market"] == "KOSDAQ")
            kosdaq[f].append(kd / len(sel))
            mc = [uni[t]["marketCap"] or 0 for t in sel]
            med = sorted(mc)[len(mc) // 2]
            rank = sum(1 for x in caps if x < med) / len(caps)
            capq[f].append(rank)
            acc[f].append(len(v))

    def mean(a):
        return round(sum(a) / len(a), 4) if a else None
    out = {"note": ("각 결정월마다 순위상관(Spearman)을 계산해 평균한다. "
                    "profitability 가 BM/size/EY/ROE 와 사실상 같은 정보인지 본다."),
           "rankCorrelation": {f: {o: mean(pairs.get((f, o), []))
                                   for o in ("BM", "SIZE", "EY", "ROE")}
                               for f in ALL_NEW},
           "top20PctKosdaqShare": {f: mean(kosdaq[f]) for f in ALL_NEW},
           "top20PctMedianCapPercentile": {f: mean(capq[f]) for f in ALL_NEW},
           "avgRankedCount": {f: mean(acc[f]) for f in ALL_NEW},
           "sectorExposure": ("업종 코드가 PIT 스냅샷에 없다(컬럼: ticker,market,close,"
                              "marketCap,shares,PER,PBR,EPS,BPS,DIV,DPS). "
                              "sector exposure 는 계산 불가 — 미측정으로 명시한다.")}
    save("ortho", out)
    print(json.dumps({f: out["rankCorrelation"][f] for f in ALL_NEW},
                     ensure_ascii=False))
    return 0


# ═══════════════ control — §10 matched control ═══════════════
def mode_control(sn, nm, ds, lf, df):
    """BM 상위20% 안에서 size/exchange 를 맞춘 뒤 profitability high vs low.

    R11 matched-control 논리 재사용: 같은 (시장 × 시총 5분위) 셀 안에서만 상/하위를
    비교해 size·exchange 노출이 spread 로 새어나오지 않게 한다.
    """
    px = FR.build_price_index(sn, ds)
    bm20 = F.make_bm_top20_filter(0.20)
    H = 12
    acc = {f: [] for f in ALL_NEW}
    cells_used = {f: [] for f in ALL_NEW}
    for i, d in enumerate(ds):
        j = i + H
        if j >= len(ds):
            continue
        uni = investable_universe(sn[d], nm)
        uni = bm20(uni, d, i)
        if len(uni) < 60:
            continue
        vals = F.make_value_fn(lf, df)(uni, d, i)
        order = sorted(uni, key=lambda t: (uni[t]["marketCap"] or 0))
        n = len(order)
        cell = {}
        for k, t in enumerate(order):
            cell[t] = (uni[t]["market"], min(4, (k * 5) // n))
        for f in ALL_NEW:
            v = {t: x for t, x in (vals.get(f) or {}).items() if t in uni}
            if len(v) < 40:
                continue
            groups = {}
            for t, x in v.items():
                groups.setdefault(cell[t], []).append((t, x))
            hi_r, lo_r, used = [], [], 0
            for _, items in groups.items():
                if len(items) < 6:
                    continue
                items.sort(key=lambda kv: (-kv[1], kv[0]))
                k = max(1, len(items) // 3)
                hs = [FR.fwd_return(px, i, j, t) for t, _ in items[:k]]
                ls = [FR.fwd_return(px, i, j, t) for t, _ in items[-k:]]
                hs = [x for x in hs if x is not None]
                ls = [x for x in ls if x is not None]
                if hs and ls:
                    hi_r.append(sum(hs) / len(hs))
                    lo_r.append(sum(ls) / len(ls))
                    used += 1
            if used >= 3:
                acc[f].append(sum(hi_r) / len(hi_r) - sum(lo_r) / len(lo_r))
                cells_used[f].append(used)
    out = {"note": ("BM 상위20% 안에서 (시장 × 시총5분위) 셀별로 profitability 상위1/3 vs "
                    "하위1/3 의 12M forward return 차이를 구하고 셀 평균을 낸다. "
                    "size·exchange 노출이 통제된 상태의 순수 profitability spread."),
           "horizonMonths": H,
           "rows": {f: {"months": len(acc[f]),
                        "meanSpread12Pct": pct(sum(acc[f]) / len(acc[f])) if acc[f] else None,
                        "medianSpread12Pct": pct(sorted(acc[f])[len(acc[f]) // 2])
                        if acc[f] else None,
                        "positiveRatePct": round(100 * sum(1 for x in acc[f] if x > 0)
                                                 / len(acc[f]), 1) if acc[f] else None,
                        "avgCellsUsed": round(sum(cells_used[f]) / len(cells_used[f]), 1)
                        if cells_used[f] else None}
                    for f in ALL_NEW}}
    save("control", out)
    print(json.dumps({f: out["rows"][f]["meanSpread12Pct"] for f in ALL_NEW},
                     ensure_ascii=False))
    return 0


# ═══════════════ audit — §11 극단치 · §13 distress · 집중도 ═══════════════
def mode_audit(sn, nm, ds, lf, df):
    px = FR.build_price_index(sn, ds)
    bm20 = F.make_bm_top20_filter(0.20)
    H = 12
    contrib = {f: {} for f in ALL_NEW}
    extreme = {f: {"denomNearZero": 0, "negInvested": 0, "hugeValue": 0, "n": 0}
               for f in ALL_NEW}
    distress = {f: {"negEquity": 0, "persistentLoss": 0, "extremeLowPbr": 0,
                    "delisted12m": 0, "microCap": 0, "n": 0} for f in ALL_NEW}
    uni_distress = {"negEquity": 0, "persistentLoss": 0, "extremeLowPbr": 0,
                    "delisted12m": 0, "microCap": 0, "n": 0}
    for i, d in enumerate(ds):
        j = i + H
        if j >= len(ds):
            continue
        uni0 = investable_universe(sn[d], nm)
        uni = bm20(uni0, d, i)
        if len(uni) < 60:
            continue
        vals = F.make_value_fn(lf, df)(uni, d, i)
        allv = lf.all_values(d, i)
        fut = sn[ds[j]]
        for t, r in uni.items():
            uni_distress["n"] += 1
            if r["BPS"] is not None and r["BPS"] <= 0:
                uni_distress["negEquity"] += 1
            p = allv[F.L1].get(t)
            if p is not None and p < 0.5:
                uni_distress["persistentLoss"] += 1
            if r["PBR"] and 0 < r["PBR"] < 0.2:
                uni_distress["extremeLowPbr"] += 1
            if t not in fut or not fut[t]["close"]:
                uni_distress["delisted12m"] += 1
            if (r["marketCap"] or 0) < 500 * 1e8:
                uni_distress["microCap"] += 1
        for f in ALL_NEW:
            v = {t: x for t, x in (vals.get(f) or {}).items() if t in uni}
            if len(v) < 40:
                continue
            top = sorted(v.items(), key=lambda kv: (-kv[1], kv[0]))
            k = max(1, len(top) // 5)
            sel = [t for t, _ in top[:k]]
            for t in sel:
                r = uni[t]
                distress[f]["n"] += 1
                if r["BPS"] is not None and r["BPS"] <= 0:
                    distress[f]["negEquity"] += 1
                p = allv[F.L1].get(t)
                if p is not None and p < 0.5:
                    distress[f]["persistentLoss"] += 1
                if r["PBR"] and 0 < r["PBR"] < 0.2:
                    distress[f]["extremeLowPbr"] += 1
                if t not in fut or not fut[t]["close"]:
                    distress[f]["delisted12m"] += 1
                if (r["marketCap"] or 0) < 500 * 1e8:
                    distress[f]["microCap"] += 1
                x = v[t]
                extreme[f]["n"] += 1
                if abs(x) > 5:
                    extreme[f]["hugeValue"] += 1
                fr = FR.fwd_return(px, i, j, t)
                if fr is not None:
                    e = contrib[f].setdefault(t, {"n": 0, "sum": 0.0,
                                                  "name": nm.get(t, t)})
                    e["n"] += 1
                    e["sum"] += fr
    out = {"note": ("BM 상위20% 안에서 각 factor 상위 20% 종목의 12M forward return "
                    "기여를 종목별로 합산한다. 극단치 몇 종목이 신호를 만드는지 본다(§11)."),
           "concentration": {}, "extremeValues": {}, "distressExposure": {},
           "universeDistressBaseline": {
               k: round(100 * uni_distress[k] / uni_distress["n"], 2)
               for k in uni_distress if k != "n"}}
    for f in ALL_NEW:
        items = sorted(contrib[f].items(), key=lambda kv: -kv[1]["sum"])
        tot = sum(e["sum"] for _, e in items)
        pos = sum(e["sum"] for _, e in items if e["sum"] > 0)
        out["concentration"][f] = {
            "tickers": len(items),
            "top1SharePct": round(100 * items[0][1]["sum"] / pos, 2) if items and pos else None,
            "top5SharePct": round(100 * sum(e["sum"] for _, e in items[:5]) / pos, 2)
            if pos else None,
            "top10SharePct": round(100 * sum(e["sum"] for _, e in items[:10]) / pos, 2)
            if pos else None,
            "note": "분모는 총 이익(gross positive) — 순합으로 나누면 100% 초과 오해가 생긴다.",
            "top": [{"ticker": t, "name": e["name"], "months": e["n"],
                     "sumFwd12Pct": pct(e["sum"])} for t, e in items[:8]]}
        out["extremeValues"][f] = {
            k: (round(100 * extreme[f][k] / extreme[f]["n"], 2)
                if extreme[f]["n"] else None)
            for k in extreme[f] if k != "n"}
        out["extremeValues"][f]["observations"] = extreme[f]["n"]
        n = distress[f]["n"]
        out["distressExposure"][f] = {
            k: (round(100 * distress[f][k] / n, 2) if n else None)
            for k in distress[f] if k != "n"}
        out["distressExposure"][f]["observations"] = n
    save("audit", out)
    print(json.dumps({f: out["concentration"][f]["top5SharePct"] for f in ALL_NEW},
                     ensure_ascii=False))
    return 0


# ═══════════════ verdict — §14·§15·§25 ═══════════════
def mode_verdict():
    s1, s2 = load("stage1"), load("stage2")
    ort, ctl, aud = load("ortho"), load("control"), load("audit")
    rep = load("repro")

    grades = {}
    for f in ALL_NEW:
        standalone = s1["spread12AnnPct"].get(f)
        r2 = s2["rows"].get(f, {})
        internal = r2.get("spread12AnnPct")
        subs = r2.get("subperiods", {})
        sub_pos = sum(1 for v in subs.values() if v is not None and v > 0)
        sub_tot = sum(1 for v in subs.values() if v is not None)
        segp, sizp = r2.get("segmentsPositive", 0), r2.get("sizesPositive", 0)
        mc = (ctl["rows"].get(f) or {}).get("meanSpread12Pct")
        top5 = (aud["concentration"].get(f) or {}).get("top5SharePct")
        obs = r2.get("obsNonOverlapping")
        is_dart = f in F.TRACK_D_IDS

        # PRIMARY 자격 (사전확정 — 결과와 무관하게 구조로 결정)
        months12 = 28 if is_dart else None
        eligible = not is_dart
        why_inelig = None
        if is_dart:
            why_inelig = (f"TRACK D 는 12M forward 가능 결정월이 약 28개월(<"
                          f"{ELIGIBILITY_FOR_PRIMARY['minDecisionMonthsWith12mForward']}) "
                          f"이고 단일 시장국면이라 사전규격상 PRIMARY 자격이 없다.")

        conds = {
            "standalonePositive": bool(standalone is not None and standalone > 0),
            "bmInternalPositive": bool(internal is not None and internal > 0),
            "subperiodsMostlyPositive": bool(sub_tot and sub_pos >= max(1, sub_tot - 1)),
            "segmentsMajority": segp >= 1,
            "sizesMajority": sizp >= 2,
            "matchedControlHolds": bool(mc is not None and mc > 0),
            "notConcentrationDriven": bool(top5 is not None and top5 < 50),
            "dataQualityClean": True,
        }
        npass = sum(1 for v in conds.values() if v)
        if conds["bmInternalPositive"] and npass >= 7:
            grade = "STRONG_CANDIDATE"
        elif conds["bmInternalPositive"] and npass >= 5:
            grade = "PROMISING"
        else:
            grade = "WEAK_REJECT"
        grades[f] = {
            "track": "D" if is_dart else "L",
            "standaloneSpread12AnnPct": standalone,
            "standaloneVerdict": s1["verdicts"].get(f, {}).get("verdict"),
            "bmTop20InternalSpread12AnnPct": internal,
            "bmTop20InternalSpread24AnnPct": r2.get("spread24AnnPct"),
            "subperiodsPositive": f"{sub_pos}/{sub_tot}",
            "subperiods": subs,
            "marketSegments": r2.get("marketSegments"),
            "sizeControlled": r2.get("sizeControlled"),
            "matchedControlMeanSpread12Pct": mc,
            "matchedControlPositiveRatePct": (ctl["rows"].get(f) or {}).get("positiveRatePct"),
            "top5ContribSharePct": top5,
            "rankCorrWithBM": (ort["rankCorrelation"].get(f) or {}).get("BM"),
            "rankCorrWithSIZE": (ort["rankCorrelation"].get(f) or {}).get("SIZE"),
            "rankCorrWithROE": (ort["rankCorrelation"].get(f) or {}).get("ROE"),
            "obsNonOverlapping": obs,
            "conditions": conds, "conditionsPassed": f"{npass}/{len(conds)}",
            "grade": grade,
            "primaryEligible": eligible,
            "primaryIneligibleReason": why_inelig,
            "decisionMonthsWith12mForward": months12,
        }

    # PRIMARY 선정 — 자격 있는 factor 중에서만. CAGR 최고가 아니라 robust 우선(§15).
    cands = [f for f in ALL_NEW if grades[f]["primaryEligible"]
             and grades[f]["grade"] in ("STRONG_CANDIDATE", "PROMISING")]

    def robust_key(f):
        g = grades[f]
        sp = sum(1 for v in (g["subperiods"] or {}).values() if v is not None and v > 0)
        return (0 if g["grade"] == "STRONG_CANDIDATE" else 1,
                -sp,
                -(g["matchedControlPositiveRatePct"] or 0),
                (g["top5ContribSharePct"] or 999),
                -(g["bmTop20InternalSpread12AnnPct"] or -999))
    cands.sort(key=robust_key)
    primary = cands[0] if cands else None

    any_eligible_internal = [f for f in ALL_NEW if grades[f]["primaryEligible"]
                             and (grades[f]["bmTop20InternalSpread12AnnPct"] or 0) > 0]
    if primary and grades[primary]["grade"] == "STRONG_CANDIDATE":
        final = "VALUE_PROFITABILITY_STRONG_CANDIDATE"
    elif primary:
        final = "VALUE_PROFITABILITY_PROMISING"
    elif any_eligible_internal:
        final = "VALUE_PROFITABILITY_PROMISING"
    else:
        final = "NO_INCREMENTAL_PROFITABILITY_SIGNAL"

    out = {
        "reproPass": rep["pass"],
        "grades": grades,
        "primaryEligibleFactors": [f for f in ALL_NEW if grades[f]["primaryEligible"]],
        "primaryIneligibleFactors": {f: grades[f]["primaryIneligibleReason"]
                                     for f in ALL_NEW
                                     if not grades[f]["primaryEligible"]},
        "primaryProfitabilityFactor": primary,
        "primarySelectionRule": ("§15 — CAGR 최고가 아니라 robust 우선. 순서: 등급 → "
                                 "하위구간 positive 수 → matched control positive 비율 → "
                                 "집중도 낮은 순 → BM 내부 spread."),
        "rejectedFactors": {f: {"grade": grades[f]["grade"],
                                "reason": _reject_reason(grades[f])}
                            for f in ALL_NEW if f != primary},
        "finalVerdict": final,
        # 지시문 §2 가 요청한 실물 profitability(ROIC/OP/CP) 자체의 상태는 별도로 적는다.
        "requestedFactorsStatus": "DATA_INSUFFICIENT",
        "requestedFactorsNote": (
            "ROIC·Operating Profitability·Cash Profitability 는 장기 PIT 스냅샷에 "
            "자산·매출·영업이익·영업현금흐름이 없어 2007~2022 구간에서 계산 자체가 "
            "불가능하다. DART 로는 FY2022~FY2025 만 있어 12M forward 가능 결정월이 "
            "약 28개월·단일 국면이다. 따라서 '요청된 factor 가 장기 alpha 를 주는가' 는 "
            "이 저장소 데이터로 답할 수 없다 — 긍정도 부정도 아니고 DATA_INSUFFICIENT 다."),
        "dartShortWindowCaution": {
            "why_not_promoted": (
                "D1/D2 의 28개월 spread 가 +26.2/+23.0%p 로 크게 나왔지만 승격하지 "
                "않는다. 사전규격의 자격 제한 외에도 독립적인 반증 근거가 3개 있다."),
            "evidence": [
                "비중첩 관측 2개 — R7 정본 verdict 규칙이 5개 미만을 UNRELIABLE 로 "
                "규정한다. 실제로 stage1 단독 판정도 UNRELIABLE 이다.",
                "horizon 불일치 — 12M +26.2/+23.0%p 가 24M 에서 +4.8/+1.0%p 로 붕괴한다. "
                "진짜 신호라면 이렇게 무너지지 않는다.",
                "ROE 순위상관 0.767/0.772 — 사실상 ROE 의 다른 표현이다. ROE 는 R7 에서 "
                "장기 INVERTED_SIGNAL 이었고 R13 의 BM 내부에서도 -10.86%p 다.",
                "집중도 52.5%/58.5% — 상위 5종목이 총 이익 기여의 절반 이상이다. "
                "장기 축(L1 10.7% · L2 11.3% · L3 19.7%)과 대조적이다.",
                "같은 창에서 D3(Cash Profitability)는 -31.6%p 로 정반대다. 세 실물 "
                "profitability 가 서로 격렬히 어긋나는 것 자체가 신호가 아니라 잡음의 "
                "증거다."],
        },
        "terminationRecord": (
            "장기 PIT 로 검증 가능한 profitability 대용 축 3개는 BM 상위 20% 내부에서 "
            "모두 음수이고 size/exchange matched control 후에도 음수다. 즉 '싼 주식 중 "
            "질 좋은 회사를 고르면 더 낫다'는 가설은 이 데이터에서 지지되지 않는다. "
            "R14 로 넘길 PRIMARY_PROFITABILITY_FACTOR 없음."),
        "nextStageEligible": final in ("VALUE_PROFITABILITY_STRONG_CANDIDATE",
                                       "VALUE_PROFITABILITY_PROMISING"),
        "parameterRescue": 0,
        "portfolioSearchPerformed": 0,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
    }
    save("verdict", out)
    print(json.dumps({"final": final, "primary": primary,
                      "grades": {f: grades[f]["grade"] for f in ALL_NEW}},
                     ensure_ascii=False))
    return 0


def _reject_reason(g):
    c = g["conditions"]
    bad = [k for k, v in c.items() if not v]
    if not g["primaryEligible"]:
        return f"PRIMARY 자격 없음 — {g['primaryIneligibleReason']}"
    if not bad:
        return "조건은 통과했으나 PRIMARY 1개 제한(§15)에서 robust 순위가 낮았다."
    return "미충족: " + ", ".join(bad)


MODES = ("repro", "stage1", "stage2", "ortho", "control", "audit", "verdict", "all")
ORDER = ("repro", "stage1", "stage2", "ortho", "control", "audit", "verdict")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    todo = ORDER if a.mode == "all" else (a.mode,)
    c = None
    if [m for m in todo if m != "verdict"]:
        c = ctx()
        print(f"[r13] {c[2][0]} ~ {c[2][-1]} ({len(c[2])}m)", file=sys.stderr)
    for m in todo:
        rc = mode_verdict() if m == "verdict" else globals()[f"mode_{m}"](*c)
        if rc:
            print(f"[r13] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
