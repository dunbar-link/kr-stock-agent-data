#!/usr/bin/env python3
"""R32 head-to-head 계산 — BM vs SIZE, 동일 표본·동일 규칙.

WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32

── 새로 만들지 않은 것 (§22) ──────────────────────────────────────
  factor engine·데이터 프레임워크·backtester 를 다시 쓰지 않는다.
  이 파일은 **thin orchestration** 이다:
    R27.paired()          matched cohort 생성 (세 팔을 같은 결정일·같은 universe 로)
    R27.Tradability       BASE gate · attrition · liquidity headroom
    r27_analysis          arm_stats / diff_series / summarize_diff / ann
    r26_analysis          non_overlapping / moving_block / boot_summary
    r27_run 의 비용식      drag = annualTurnover × bps / 100  (그대로 재사용)

── §9 matched sample 이 구조적으로 보장되는 이유 ──────────────────
  R27.paired() 는 한 결정일에서 SIZE·BM·CONTROL **세 팔이 모두 성립할 때만**
  그 행을 표본에 넣는다. 같은 universe(BM·SIZE 동시 유효), 같은 tradable subset,
  같은 i·h 를 쓴다. 그래서 "factor 마다 다른 기간" 이 발생할 수 없다.
  그래도 가정하지 않고 per-arm 독립 시도로 BM-only / SIZE-only 를 **측정**한다.

── 정직하게 측정하지 않는 것 ──────────────────────────────────────
  NAV-level maximum drawdown: 포트폴리오 NAV 시계열이 이 pipeline 에 없다.
  새 backtester 를 만들지 않는다(§22). 대신 **비중첩 코호트를 연결한** 자산곡선의
  낙폭을 proxy 로 내고 NAV drawdown 이라고 부르지 않는다(관측점이 적다).
  cash drag / 정수주 효과: 코호트 방식이라 모델에 없다. 0 으로 측정된 것이 아니다.

안전: 읽기·계산 전용. 네트워크 0. 새 데이터 수집 0. production write 0.
"""
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r27_analysis as A                                        # noqa: E402
from r26_analysis import boot_summary, moving_block, non_overlapping  # noqa: E402
from r27_analysis import (ARMS, PH, R27, arm_stats, diff_series,  # noqa: E402
                          summarize_diff)
from r27_precommit import COST, PRIMARY_GATE, SENSITIVITY        # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

FACTORS = ("SIZE", "BM")
PRIMARY_START = "2010-01-01"
COST_BPS = [0, 25, 50, 100]
SUBPERIODS = [("2010-2014", "2010-01-01", "2014-12-31"),
              ("2015-2019", "2015-01-01", "2019-12-31"),
              ("2020-END", "2020-01-01", "2099-12-31")]
BOOT_SEED = 20320908          # 고정 seed — 결과를 보고 바꾸지 않는다
BOOT_RESAMPLES = 2000


# ══════════════════════ helpers ══════════════════════
def _ann_pct(x):
    return round(x * 100, 3) if x is not None else None


def _std(xs):
    return statistics.pstdev(xs) if len(xs) > 1 else 0.0


def chained_drawdown(rows, arm):
    """비중첩 코호트를 이어 만든 자산곡선의 최대 낙폭(proxy).

    NAV 시계열이 아니다 — 관측점이 적고(36M 비중첩이면 5~6개) 코호트 평균수익을
    연결한 것이다. 그대로 표기한다.
    """
    no = non_overlapping(rows, PH, ARMS)
    if len(no) < 2:
        return None
    nav, peak, mdd = 1.0, 1.0, 0.0
    for r in no:
        # topAnn 은 연율. 코호트 총수익으로 환산해 연결한다.
        nav *= (1.0 + r[arm]["topAnn"]) ** (PH / 12.0)
        peak = max(peak, nav)
        mdd = min(mdd, nav / peak - 1.0)
    return {"nonOverlappingCohorts": len(no),
            "chainedFinalNav": round(nav, 4),
            "maxDrawdownPct": round(mdd * 100, 3),
            "note": "NAV drawdown 아님 — 비중첩 코호트 연결 proxy"}


def turnover_of(K, arm, dates):
    """r27_run 과 **같은 정의**의 회전율. 새 정의를 만들지 않는다.

    ★ 2026-09-08 재현성 수정: 원본은 `key=lambda kv: -kv[1]` 로 동점 처리가 없어
      상위 n 선택이 set 순회 순서(문자열 해시 랜덤화)에 의존했다. 실측으로 같은
      입력 재실행에서 BM 100bp 순성과가 9.742 ↔ 9.736 으로 흔들렸다(≤0.01%p).
      정렬 키에 ticker 를 2차 키로 추가해 결정적으로 만든다 — R27 의 cohort() 가
      이미 쓰는 tie-break 방식과 같다(`key=(kv[1], kv[0])`). 회전율 **정의**는
      그대로이고 동점일 때 누구를 담을지만 고정된다.
      r27_run 원본은 R31 의 동결 evidence 라 건드리지 않는다(같은 잠복 결함 있음).
    """
    turns, prev = [], None
    for d in dates:
        sub = K.tr.tradable_set(d, set(K.base[d]["BM"]))
        vals = {t: K.base[d][arm][t] for t in sub}
        if len(vals) < 100:
            prev = None
            continue
        n = max(1, len(vals) // (10 if len(vals) >= 200 else 5))
        cur = {t for t, _ in sorted(vals.items(),
                                    key=lambda kv: (-kv[1], kv[0]))[:n]}
        if prev:
            turns.append(1.0 - len(cur & prev) / len(prev))
        prev = cur
    mt = statistics.fmean(turns) if turns else None
    return {"observations": len(turns),
            "monthlyTurnover": round(mt, 4) if mt is not None else None,
            "annualTurnover": round(mt * 12, 3) if mt is not None else None}


def cost_curve(gross_pct, annual_turnover):
    """drag = annualTurnover × bps / 100  (r27_run 식 그대로)."""
    out = {}
    for bps in COST_BPS:
        drag = (annual_turnover * bps / 100.0) if annual_turnover else 0.0
        out[f"{bps}bp"] = {"grossPct": gross_pct,
                           "costDragPct": round(drag, 3),
                           "netPct": round(gross_pct - drag, 3)}
    return out


def break_even_bps(gross_a, turn_a, gross_b, turn_b):
    """두 factor 의 순성과가 같아지는 비용(bp). turnover 차이가 없으면 None."""
    if turn_a is None or turn_b is None:
        return None
    dt = (turn_a - turn_b) / 100.0
    if abs(dt) < 1e-12:
        return None
    return round((gross_a - gross_b) / dt, 1)


# ══════════════════════ 본체 ══════════════════════
class R32:
    def __init__(self):
        self.K = R27()
        self.thr = PRIMARY_GATE["thresholdKrw"]
        self.all_rows = self.K.paired(PH, tradable=True, threshold=self.thr)
        self.rows = [r for r in self.all_rows if r["startDate"] >= PRIMARY_START]
        self.pre2010 = [r for r in self.all_rows if r["startDate"] < PRIMARY_START]
        self.dates = [r["startDate"] for r in self.rows]

    # ── §9 matched sample 실측 ───────────────────────────────────────
    def matched_sample(self):
        per = {a: set() for a in FACTORS}
        attempted = 0
        for i, d in enumerate(self.K.dates):
            if d < PRIMARY_START or d not in self.K.base:
                continue
            if i + PH >= len(self.K.dates):
                continue
            attempted += 1
            sub = self.K.tr.tradable_set(d, set(self.K.base[d]["BM"]), self.thr)
            for a in FACTORS:
                if self.K.cohort(a, i, PH, subset=sub) is not None:
                    per[a].add(d)
        common = per["SIZE"] & per["BM"]
        return {
            "signalDatesAttempted": attempted,
            "bmObservations": len(per["BM"]),
            "sizeObservations": len(per["SIZE"]),
            "commonObservations": len(common),
            "bmOnly": sorted(per["BM"] - per["SIZE"]),
            "sizeOnly": sorted(per["SIZE"] - per["BM"]),
            "pairedRowsUsed": len(self.rows),
            "commonDateCount": len(set(self.dates)),
            "commonCohortCount": len(self.rows),
            "commonSecurityCount": len({t for r in self.rows
                                        for a in FACTORS
                                        for t in r[a]["topTickers"]}),
            "identicalMaskGuaranteedByEngine": True,
            "engineContract": "R27.paired — 세 팔이 모두 성립할 때만 표본에 포함",
        }

    def period(self):
        end_idx = len(self.K.dates) - 1
        return {
            "primaryStartRule": PRIMARY_START,
            "firstSignalDate": self.dates[0] if self.dates else None,
            "lastSignalDate": self.dates[-1] if self.dates else None,
            # endDate 는 arm 단위 필드다(세 팔이 같은 기간을 공유한다).
            "lastCompleteCohortEndDate": (self.rows[-1]["SIZE"]["endDate"]
                                          if self.rows else None),
            "snapshotLastDecisionDate": self.K.dates[end_idx],
            "horizonMonths": PH,
            "incompleteCohortsIncluded": 0,
            "incompleteCohortRule": "paired() 는 i+h >= len(dates) 인 코호트를 만들지 않는다",
            "pre2010CohortsExcludedFromPrimary": len(self.pre2010),
            "pre2010Dates": [r["startDate"] for r in self.pre2010],
        }

    # ── §12 핵심 성과 ────────────────────────────────────────────────
    def performance(self):
        out = {}
        ds_sorted = sorted(d for d in self.K.base if d >= PRIMARY_START)
        for a in ARMS:
            st = arm_stats(self.rows, a)
            tops = [r[a]["topAnn"] for r in self.rows]
            out[a] = {**(st or {}),
                      "volatilityAnnPct": round(_std(tops) * 100, 3),
                      "worstCohortAnnPct": _ann_pct(min(tops)) if tops else None,
                      "bestCohortAnnPct": _ann_pct(max(tops)) if tops else None,
                      "negativeCohortRatio": round(
                          sum(1 for x in tops if x < 0) / len(tops), 4) if tops else None,
                      "returnToVolRatio": round(
                          statistics.fmean(tops) / _std(tops), 3)
                      if tops and _std(tops) > 0 else None,
                      "chainedDrawdown": chained_drawdown(self.rows, a)}
        for a in FACTORS:
            out[a]["turnover"] = turnover_of(self.K, a, ds_sorted)
            out[a]["excessVsControl"] = summarize_diff(
                diff_series(self.rows, a, "CONTROL"))
            out[a]["costCurve"] = cost_curve(
                out[a]["meanTopAnnPct"], out[a]["turnover"]["annualTurnover"])
            out[a]["avgHoldingCount"] = round(statistics.fmean(
                [len(r[a]["topTickers"]) for r in self.rows]), 1)
            out[a]["cashDrag"] = "NOT_MODELLED_COHORT_METHOD"
            out[a]["integerShare"] = "NOT_MODELLED_COHORT_METHOD"
        out["directDifference"] = summarize_diff(
            diff_series(self.rows, "SIZE", "BM"))
        out["breakEvenBps_SIZE_minus_BM"] = break_even_bps(
            out["SIZE"]["meanTopAnnPct"], out["SIZE"]["turnover"]["annualTurnover"],
            out["BM"]["meanTopAnnPct"], out["BM"]["turnover"]["annualTurnover"])
        out["costRankReversal"] = {
            f"{b}bp": (out["SIZE"]["costCurve"][f"{b}bp"]["netPct"]
                       < out["BM"]["costCurve"][f"{b}bp"]["netPct"])
            for b in COST_BPS}
        out["costAssumptionsCanonical"] = COST
        return out

    # ── §17 유동성 / attrition / headroom ────────────────────────────
    def liquidity(self):
        out = {}
        for a in FACTORS:
            inc, exc = [], []
            reasons = defaultdict(int)
            med_tv, near_above, near_below, total = [], 0, 0, 0
            # ★ 2026-09-08 결함 수정: 초판은 self.rows(필터 **후** 코호트)의 top 종목을
            #   평가했다. 그 종목들은 이미 BASE 를 통과한 것들이라 attrition 이 구조적으로
            #   0 이 되고 excluded 표본이 비었다(실측 attr=0.0 / excluded=None).
            #   R27 이 쓰는 방식대로 **필터 전** 코호트의 top 후보를 평가한다.
            for r in self._rows_before():
                d = r["startDate"]
                i, j = self.K.pos[d], self.K.pos[d] + PH
                for t in r[a]["topTickers"]:
                    ok, why = self.K.tr.evaluate(d, t, self.thr)
                    total += 1
                    if not ok:
                        reasons[why] += 1
                    m = (self.K.tr.metrics.get(d) or {}).get(t)
                    if m:
                        tv = m["median20TradedValue"]
                        med_tv.append(tv)
                        if self.thr <= tv < self.thr * 1.25:
                            near_above += 1
                        elif self.thr * 0.75 <= tv < self.thr:
                            near_below += 1
                    v = self.K.tsr(i, j, t)
                    if v is not None:
                        (inc if ok else exc).append(v)
            inc_ann = A.ann(statistics.fmean(inc), PH) if inc else None
            exc_ann = A.ann(statistics.fmean(exc), PH) if exc else None
            q = sorted(med_tv)

            def pc(p):
                return round(q[min(len(q) - 1, int(p * (len(q) - 1)))], 0) if q else None
            med = statistics.median(q) if q else None
            out[a] = {
                "candidateNames": total,
                "includedNames": len(inc), "excludedNames": len(exc),
                "attritionRate": round(sum(reasons.values()) / total, 4) if total else None,
                "attritionReasons": dict(reasons),
                "medianLiquidityKrw": round(med, 0) if med else None,
                "p10Krw": pc(0.10), "p25Krw": pc(0.25), "p50Krw": pc(0.50),
                "p75Krw": pc(0.75), "p90Krw": pc(0.90),
                "thresholdHeadroom": round(med / self.thr, 4) if med else None,
                "nearAboveThresholdRatio": round(near_above / len(med_tv), 4) if med_tv else None,
                "nearBelowThresholdRatio": round(near_below / len(med_tv), 4) if med_tv else None,
                "includedTopAnnPct": _ann_pct(inc_ann),
                "excludedTopAnnPct": _ann_pct(exc_ann),
                "nontradableAlpha": bool(inc_ann is not None and exc_ann is not None
                                         and (exc_ann >= inc_ann * 2 or inc_ann <= 0)),
            }
        # 연도별 · 시장별 attrition
        for a in FACTORS:
            by_year, by_mkt = defaultdict(lambda: [0, 0]), defaultdict(lambda: [0, 0])
            for r in self._rows_before():
                d = r["startDate"]
                snap = self.K.snaps.get(d) or {}
                for t in r[a]["topTickers"]:
                    ok, _ = self.K.tr.evaluate(d, t, self.thr)
                    by_year[d[:4]][0] += 1
                    by_year[d[:4]][1] += (0 if ok else 1)
                    mk = (snap.get(t) or {}).get("market") or "UNKNOWN"
                    by_mkt[mk][0] += 1
                    by_mkt[mk][1] += (0 if ok else 1)
            out[a]["attritionByYear"] = {y: round(v[1] / v[0], 4)
                                         for y, v in sorted(by_year.items()) if v[0]}
            out[a]["attritionByMarket"] = {m: round(v[1] / v[0], 4)
                                           for m, v in sorted(by_mkt.items()) if v[0]}
        out["thresholdKrw"] = self.thr
        return out

    # ── §14 기간 / §15 시장 ──────────────────────────────────────────
    def stability(self):
        out = {"subperiods": {}, "signalYear": {}, "markets": {}, "rolling": {}}
        for name, lo, hi in SUBPERIODS:
            sel = [r for r in self.rows if lo <= r["startDate"] <= hi]
            out["subperiods"][name] = {
                "cohorts": len(sel),
                "spanNote": ("마지막 블록은 5년 미만일 수 있다"
                             if name == "2020-END" else None),
                **{a: summarize_diff(diff_series(sel, a, "CONTROL")) for a in FACTORS},
                "sizeMinusBm": summarize_diff(diff_series(sel, "SIZE", "BM")),
            }
        years = sorted({r["startDate"][:4] for r in self.rows})
        wins = {"SIZE": 0, "BM": 0, "tie": 0}
        for y in years:
            sel = [r for r in self.rows if r["startDate"][:4] == y]
            d = summarize_diff(diff_series(sel, "SIZE", "BM"))
            out["signalYear"][y] = {
                "cohorts": len(sel),
                **{a: summarize_diff(diff_series(sel, a, "CONTROL")) for a in FACTORS},
                "sizeMinusBm": d}
            if d:
                if d["meanPct"] > 0:
                    wins["SIZE"] += 1
                elif d["meanPct"] < 0:
                    wins["BM"] += 1
                else:
                    wins["tie"] += 1
        out["signalYearWinCount"] = wins
        out["positiveExcessYearCount"] = {
            a: sum(1 for y in years
                   if (out["signalYear"][y][a] or {}).get("meanPct", 0) > 0)
            for a in FACTORS}
        out["yearCount"] = len(years)

        # 시장 분할은 universe 를 다시 잘라 코호트를 재계산한다(동일 규칙).
        for ex in ("KOSPI", "KOSDAQ"):
            rows_ex = []
            for i, d in enumerate(self.K.dates):
                if d < PRIMARY_START or d not in self.K.base:
                    continue
                if i + PH >= len(self.K.dates):
                    continue
                sub = self.K.tr.tradable_set(d, set(self.K.base[d]["BM"]), self.thr)
                sub &= self.K.exchange_subset(d, ex)
                if len(sub) < 100:
                    continue
                row, ok = {}, True
                for a in ARMS:
                    c = self.K.cohort(a, i, PH, subset=sub)
                    if c is None:
                        ok = False
                        break
                    row[a] = c
                if ok:
                    row["startDate"] = d
                    rows_ex.append(row)
            out["markets"][ex] = {
                "cohorts": len(rows_ex),
                **{a: summarize_diff(diff_series(rows_ex, a, "CONTROL"))
                   for a in FACTORS},
                "sizeMinusBm": summarize_diff(diff_series(rows_ex, "SIZE", "BM"))}
        out["marketDirectionAgrees"] = None
        try:
            ko = out["markets"]["KOSPI"]["sizeMinusBm"]["meanPct"]
            kq = out["markets"]["KOSDAQ"]["sizeMinusBm"]["meanPct"]
            out["marketDirectionAgrees"] = bool((ko > 0) == (kq > 0))
        except (KeyError, TypeError):
            pass

        for h in (36, 60):
            rr = ([r for r in self.K.paired(h, tradable=True, threshold=self.thr)
                   if r["startDate"] >= PRIMARY_START] if h != PH else self.rows)
            out["rolling"][f"{h}M"] = {
                "cohorts": len(rr),
                "overlapping": summarize_diff(diff_series(rr, "SIZE", "BM")),
                "nonOverlapping": summarize_diff(
                    diff_series(non_overlapping(rr, h, ARMS), "SIZE", "BM")),
                "note": "return horizon 과 같은 개념이면 중복 근거로 세지 않는다"}

        # source boundary 전후 (2010~2019 KRX vs 2020+ 공공데이터)
        for tag, lo, hi in (("KRX_2010_2019", "2010-01-01", "2019-12-31"),
                            ("PORTAL_2020_PLUS", "2020-01-01", "2099-12-31")):
            sel = [r for r in self.rows if lo <= r["startDate"] <= hi]
            out.setdefault("sourceBoundary", {})[tag] = {
                "cohorts": len(sel),
                **{a: summarize_diff(diff_series(sel, a, "CONTROL")) for a in FACTORS},
                "sizeMinusBm": summarize_diff(diff_series(sel, "SIZE", "BM"))}
        return out

    # ── §16 극단값 적대검증 ──────────────────────────────────────────
    def adversarial(self):
        out = {"contributors": {}, "removal": {}, "leaveOneYearOut": {},
               "yearRemoval": {}, "perMarketTopRemoval": {}}
        base = {a: summarize_diff(diff_series(self.rows, a, "CONTROL"))
                for a in FACTORS}
        base_sb = summarize_diff(diff_series(self.rows, "SIZE", "BM"))
        out["baseline"] = {**base, "sizeMinusBm": base_sb}

        contrib = {}
        for a in FACTORS:
            c, tot = {}, 0.0
            for r in self.rows:
                d = r["startDate"]
                i, j = self.K.pos[d], self.K.pos[d] + PH
                rs = [(t, self.K.tsr(i, j, t)) for t in r[a]["topTickers"]]
                rs = [(t, x) for t, x in rs if x is not None]
                if not rs:
                    continue
                w = 1.0 / len(rs)
                for t, x in rs:
                    c[t] = c.get(t, 0.0) + w * x
                    tot += w * x
            top = sorted(c.items(), key=lambda kv: -kv[1])
            contrib[a] = top
            sh = (lambda k: round(sum(v for _, v in top[:k]) / tot * 100, 2)) \
                if tot else (lambda k: None)
            out["contributors"][a] = {"contributors": len(c), "top1Pct": sh(1),
                                      "top3Pct": sh(3), "top5Pct": sh(5),
                                      "topNames": [t for t, _ in top[:5]]}

        for n in (1, 3, 5):
            out["removal"][f"top{n}"] = {}
            for a in FACTORS:
                names = {t for t, _ in contrib[a][:n]}
                rr = [r for r in self.K.paired(PH, tradable=True,
                                               threshold=self.thr, exclude=names)
                      if r["startDate"] >= PRIMARY_START]
                out["removal"][f"top{n}"][a] = {
                    "removed": sorted(names),
                    "excessVsControl": summarize_diff(diff_series(rr, a, "CONTROL")),
                    "signFlipped": bool(
                        base[a] and summarize_diff(diff_series(rr, a, "CONTROL"))
                        and (base[a]["meanPct"] > 0)
                        != (summarize_diff(diff_series(rr, a, "CONTROL"))["meanPct"] > 0))}

        years = sorted({r["startDate"][:4] for r in self.rows})
        for y in years:
            sel = [r for r in self.rows if r["startDate"][:4] != y]
            out["leaveOneYearOut"][y] = {
                **{a: summarize_diff(diff_series(sel, a, "CONTROL")) for a in FACTORS},
                "sizeMinusBm": summarize_diff(diff_series(sel, "SIZE", "BM"))}
        sb = {y: (v["sizeMinusBm"] or {}).get("meanPct")
              for y, v in out["leaveOneYearOut"].items()}
        vals = [v for v in sb.values() if v is not None]
        out["loyoSizeMinusBmSignStable"] = bool(
            vals and (all(v > 0 for v in vals) or all(v < 0 for v in vals)))
        out["loyoRange"] = {"min": min(vals), "max": max(vals)} if vals else None

        per_year = {y: summarize_diff(diff_series(
            [r for r in self.rows if r["startDate"][:4] == y], "SIZE", "BM"))
            for y in years}
        ranked = [(y, (v or {}).get("meanPct", 0.0)) for y, v in per_year.items()]
        ranked.sort(key=lambda kv: kv[1])
        worst_y, best_y = ranked[0][0], ranked[-1][0]
        for tag, y in (("removeBestSignalYear", best_y), ("removeWorstSignalYear", worst_y)):
            sel = [r for r in self.rows if r["startDate"][:4] != y]
            out["yearRemoval"][tag] = {
                "year": y,
                **{a: summarize_diff(diff_series(sel, a, "CONTROL")) for a in FACTORS},
                "sizeMinusBm": summarize_diff(diff_series(sel, "SIZE", "BM"))}
        return out

    # ── §18 overlap / correlation ────────────────────────────────────
    def overlap(self):
        jac, common_n, only = [], [], {"SIZE": [], "BM": []}
        for r in self.rows:
            s, b = set(r["SIZE"]["topTickers"]), set(r["BM"]["topTickers"])
            u = s | b
            jac.append(len(s & b) / len(u) if u else 0.0)
            common_n.append(len(s & b))
            only["SIZE"].append(len(s - b))
            only["BM"].append(len(b - s))
        st = [r["SIZE"]["topAnn"] for r in self.rows]
        bt = [r["BM"]["topAnn"] for r in self.rows]
        se = [r["SIZE"]["excessAnn"] for r in self.rows]
        be = [r["BM"]["excessAnn"] for r in self.rows]

        def corr(x, y):
            try:
                return round(statistics.correlation(x, y), 4)
            except (statistics.StatisticsError, ValueError):
                return None
        return {"meanJaccard": round(statistics.fmean(jac), 4) if jac else None,
                "meanCommonNames": round(statistics.fmean(common_n), 1) if common_n else None,
                "meanSizeOnlyNames": round(statistics.fmean(only["SIZE"]), 1),
                "meanBmOnlyNames": round(statistics.fmean(only["BM"]), 1),
                "returnCorrelation": corr(st, bt),
                "excessReturnCorrelation": corr(se, be),
                "role": "설명용. overlap threshold 를 만들어 PRIMARY 선택에 쓰지 않는다."}

    # ── §19 paired uncertainty ───────────────────────────────────────
    def uncertainty(self):
        xs = diff_series(self.rows, "SIZE", "BM")
        no_rows = non_overlapping(self.rows, PH, ARMS)
        xs_no = diff_series(no_rows, "SIZE", "BM")
        means = moving_block(xs, BOOT_SEED, block=PH, resamples=BOOT_RESAMPLES)
        # boot_summary(means, obs) 의 obs 는 **관측 리스트**다(길이가 아니다).
        bs = boot_summary(means, xs) if means else None
        se = (_std(xs) / (len(xs) ** 0.5)) if len(xs) > 1 else None
        se_no = (_std(xs_no) / (len(xs_no) ** 0.5)) if len(xs_no) > 1 else None
        return {
            "metric": "SIZE topAnn − BM topAnn (동일 결정일 paired)",
            "overlapping": {"cohorts": len(xs), **(summarize_diff(xs) or {}),
                            "standardErrorPct": round(se * 100, 4) if se else None,
                            "caveat": "겹치는 36M forward return — 독립 표본 아님"},
            "nonOverlapping": {"cohorts": len(xs_no), **(summarize_diff(xs_no) or {}),
                               "standardErrorPct": round(se_no * 100, 4) if se_no else None},
            "blockBootstrap": {"blockMonths": PH, "resamples": BOOT_RESAMPLES,
                               "seed": BOOT_SEED, "summary": bs,
                               "blockRule": "holding horizon(36M) 보존 · 결과 보고 변경 0"},
            "pValueAloneDecides": False,
        }

    def _rows_before(self):
        """유동성 필터 **전** 코호트를 primary 기간·동일 날짜로 맞춰 캐시한다."""
        if getattr(self, "_rb", None) is None:
            rows_b = [r for r in self.K.paired(PH, tradable=False)
                      if r["startDate"] >= PRIMARY_START]
            keep = {r["startDate"] for r in self.rows}
            self._rb = [r for r in rows_b if r["startDate"] in keep]
        return self._rb

    def before_filter(self):
        """유동성 필터 **전** 성과 — retention ratio 의 분모(R27 STRONG 조건).

        같은 primary 기간·같은 공유 universe 를 쓰고 tradable subset 만 적용하지
        않는다. 기간을 바꿔 분모를 유리하게 만들지 않기 위해 날짜를 맞춘다.
        """
        rows_m = self._rows_before()
        out = {"cohortsDateMatched": len(rows_m), "dateMatchedToAfter": True}
        for a in FACTORS:
            ex = summarize_diff(diff_series(rows_m, a, "CONTROL"))
            st = arm_stats(rows_m, a)
            out[a] = {"topAnnPct": (st or {}).get("meanTopAnnPct"),
                      "excessMeanPct": (ex or {}).get("meanPct"),
                      "excess": ex}
        out["CONTROL"] = {"topAnnPct": (arm_stats(rows_m, "CONTROL") or {}).get("meanTopAnnPct")}
        return out

    def run(self):
        return {"task": "R32",
                "thresholdKrw": self.thr,
                "sensitivityKrw": {k: v["thresholdKrw"] for k, v in SENSITIVITY.items()
                                   if isinstance(v, dict) and "thresholdKrw" in v},
                "matchedSample": self.matched_sample(),
                "period": self.period(),
                "before": self.before_filter(),
                "performance": self.performance(),
                "liquidity": self.liquidity(),
                "stability": self.stability(),
                "adversarial": self.adversarial(),
                "overlap": self.overlap(),
                "uncertainty": self.uncertainty()}


def main() -> int:
    e = R32()
    out = e.run()
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r32-headtohead-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    p = out["performance"]
    print(json.dumps({
        "cohorts": out["matchedSample"]["commonCohortCount"],
        "period": [out["period"]["firstSignalDate"], out["period"]["lastSignalDate"]],
        "SIZE": p["SIZE"]["meanTopAnnPct"], "BM": p["BM"]["meanTopAnnPct"],
        "CONTROL": p["CONTROL"]["meanTopAnnPct"],
        "sizeMinusBm": (p["directDifference"] or {}).get("meanPct"),
        "net100bp": {a: p[a]["costCurve"]["100bp"]["netPct"] for a in FACTORS},
        "costRankReversal": p["costRankReversal"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
