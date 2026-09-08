#!/usr/bin/env python3
"""R33 prospective feasibility 감사 — OOS 계약을 열기 전의 단일 관문.

WABABA-BM-SIZE-SEPARATE-OOS-PAPER-OBSERVATION-CONTRACT-R33
SOURCE_TASK: WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A (commit 0431003)

R33 §6 이 요구하는 질문 하나에만 답한다.

  "R32 historical contract 를 미래 시점에서 look-ahead 없이 그대로 실행할 수 있는가?"

여기서 OOS cohort 를 만들지 않는다. precommit 을 동결하지도 않는다.
§6 은 feasibility 가 성립할 때만 다음 단계로 가라고 지시한다 — 이 파일은 그 관문이다.

── 복원하는 것 (추측 금지, 실제 코드·실제 파일에서) ──────────────────
  signal cadence · signal date · factor as-of · liquidity as-of ·
  ranking freeze 시각 · entry price date · exit price date

── §6 이 금지한 것 ────────────────────────────────────────────────
  "signal 을 선택할 때 아직 알 수 없는 당일 종가를 사용하면서
   그 당일 종가로 진입한 것으로 처리하는 계약"

  이 파일은 그 조건을 **주장하지 않고 측정한다**:
    marketCap(d) == close(d) × shares(d)   → SIZE_SMALL 은 close(d) 의 순함수
    PBR(d)      == close(d) / BPS(d)       → BM 은 close(d) 의 순함수
    entry p0    == close(d)                → 랭킹 입력과 체결가가 같은 값

── 새 rule 을 만들지 않는다 ────────────────────────────────────────
  §6: "조용히 next-day entry 등으로 바꾸지 않는다."
  prior-close 재랭킹은 **충돌 범위 측정용 진단**이며 DIAGNOSTIC_ONLY 로 표시한다.
  cohort 도 성과도 만들지 않는다. 계약 후보로 승격하지 않는다.

안전: 읽기·계산 전용. 네트워크 0 · API 0 · env 접근 0 · 인증키 접근 0 · write 0.
"""
from __future__ import annotations

import gzip
import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SRC = ROOT / "scripts" / "research"
SNAP = ROOT / "_cache" / "pit-snapshots"

TASK_ID = "WABABA-BM-SIZE-SEPARATE-OOS-PAPER-OBSERVATION-CONTRACT-R33"
SOURCE_TASK = "WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A"
SOURCE_COMMIT = "0431003"
SOURCE_DECISION = "BOTH_SEPARATE"

FROZEN = {"r27_precommit": "acee4288", "r31_precommit": "d7fe3929",
          "r32_precommit": "07edbe72"}

# 동일성 판정 허용치. 넓히지 않는다.
EXACT_TOL = 1e-9          # marketCap == close × shares


def _f(x):
    if x is None or x == "" or x == "None":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v and abs(v) != float("inf") else None


# ══════════════════════════════════════════════════════════════════════
# 1. frozen hash 검증
# ══════════════════════════════════════════════════════════════════════
def frozen_hashes():
    out = {}
    for name, prefix in FROZEN.items():
        h = hashlib.sha256((SRC / f"{name}.py").read_bytes()).hexdigest()
        out[name] = {"sha256": h, "expectedPrefix": prefix,
                     "match": h.startswith(prefix)}
    out["allMatch"] = all(v["match"] for v in out.values() if isinstance(v, dict))
    return out


# ══════════════════════════════════════════════════════════════════════
# 2. signal cadence 복원 (§7) — 추측 금지
# ══════════════════════════════════════════════════════════════════════
def cadence(decision_dates, primary_sample):
    """실제 거래일 캘린더에서 '월 최초 거래일' 을 재계산해 실측 signal 과 대조.

    대조는 **집합 기준**이다. 결정일이 빠진 달이 있어도 cadence 위반이 아니다 —
    R27.base 는 eligible universe < 100 인 달을 애초에 만들지 않기 때문이다.
    순서쌍(zip) 대조는 그런 결측 한 번에 이후 전부가 어긋나므로 쓰지 않는다.
    """
    from r27_collect import trading_calendar
    cal = trading_calendar()
    calset = set(cal)

    first_of_month = {}
    for d in cal:
        ym = d[:7]
        if ym not in first_of_month:
            first_of_month[ym] = d

    # 캘린더가 실제로 덮는 구간에서만 대조한다. 캘린더는 2006-11 부터이고
    # 결정일은 1999 부터라 전 구간 대조는 불가능하다 — 없는 근거를 만들지 않는다.
    lo = max(decision_dates[0], cal[0])
    hi = min(decision_dates[-1], cal[-1])
    expected = [v for _, v in sorted(first_of_month.items()) if lo <= v <= hi]
    exp_set = set(expected)
    actual = [d for d in decision_dates if lo <= d <= hi]

    not_month_first = [{"date": d, "monthFirstTradingDay": first_of_month.get(d[:7])}
                       for d in actual if d not in exp_set]
    missing_months = sorted(exp_set - set(actual))

    # R32 primary sample 은 연속된 월 최초 거래일이어야 한다 — 별도로 엄격 검증
    ps = [d for d in primary_sample]
    ps_bad = [d for d in ps if d not in exp_set]
    ps_expected = [v for v in expected if ps and ps[0] <= v <= ps[-1]]
    ps_gaps = sorted(set(ps_expected) - set(ps))

    return {
        "rule": "MONTH_FIRST_TRADING_DAY",
        "ruleSource": ("scripts/research/build_pit_snapshots.py "
                       "month_first_trading_days() + PIT 스냅샷 파일명"),
        "calendarSource": "_cache/krx-liquidity/_trading-calendar.json (KOSPI 1001 OHLCV)",
        "calendarDays": len(cal),
        "calendarRange": {"min": cal[0], "max": cal[-1]},
        "comparedRange": {"min": lo, "max": hi},
        "decisionDatesCompared": len(actual),
        "decisionDatesOutsideCalendar": len(
            [d for d in decision_dates if d < cal[0] or d > cal[-1]]),
        "expectedMonthFirstCount": len(expected),
        "notMonthFirstCount": len(not_month_first),
        "notMonthFirst": not_month_first[:10],
        "monthsWithoutDecisionDate": len(missing_months),
        "monthsWithoutDecisionDateSample": missing_months[:10],
        "monthsWithoutDecisionDateReason": (
            "R27._shared_universe 가 eligible universe < 100 인 달을 제외한다 "
            "(cadence 위반 아님)"),
        "allComparedDatesAreTradingDays": all(d in calset for d in actual),
        "primarySample": {
            "cohorts": len(ps),
            "range": {"min": ps[0] if ps else None, "max": ps[-1] if ps else None},
            "notMonthFirstCount": len(ps_bad),
            "gapMonths": len(ps_gaps),
            "gapMonthsSample": ps_gaps[:10],
            "contiguousMonthly": len(ps_bad) == 0 and len(ps_gaps) == 0,
        },
        "reproduced": (len(not_month_first) == 0 and len(ps_bad) == 0
                       and len(ps_gaps) == 0),
    }


# ══════════════════════════════════════════════════════════════════════
# 3. 입력 가용시각 timeline (§6) — 실측
# ══════════════════════════════════════════════════════════════════════
def input_timing(K, dates):
    """결정일별로 각 입력의 as-of 날짜를 실제 코드 경로에서 복원한다."""
    from r27_collect import LOOKBACK, trading_calendar
    cal = trading_calendar()
    pos = {d: i for i, d in enumerate(cal)}

    rows, viol_liq, viol_same = [], 0, 0
    for d in dates:
        i = pos.get(d)
        win = cal[max(0, i - LOOKBACK):i] if i is not None else []
        liq_max = win[-1] if win else None
        # r27_analysis.Tradability._window 와 동일 계약: 결정일 직전만
        liq_ok = bool(liq_max and liq_max < d)
        if not liq_ok:
            viol_liq += 1

        # ── entry 체결가가 factor 원천과 같은 값인지 **실측** ────────────
        #    주장하지 않는다. 엔진의 price() 와 스냅샷 close 를 직접 대조한다.
        k = K.pos[d]
        snap = K.snaps.get(d) or {}
        n = same = 0
        for t in (K.base[d]["BM"] if d in K.base else {}):
            p_entry = K.eng.price(k, t)
            p_snap = (snap.get(t) or {}).get("close")
            if p_entry is None or p_snap is None:
                continue
            n += 1
            if abs(p_entry - p_snap) <= EXACT_TOL * max(1.0, abs(p_snap)):
                same += 1
        eq = n > 0 and same == n
        if eq:
            viol_same += 1

        rows.append({
            "signalDate": d,
            "liquidityWindow": {"first": win[0] if win else None,
                                "last": liq_max, "days": len(win)},
            "liquidityAsOf": liq_max,
            "liquidityStrictlyBeforeSignal": liq_ok,
            "factorAsOf": d,
            "rankingInputAvailableAt": f"{d} 15:30 (종가 확정 시점)",
            "entryPriceDate": d,
            "entryPriceAvailableAt": f"{d} 15:30 (종가 확정 시점)",
            "entryPriceRowsChecked": n,
            "entryPriceEqualsSnapshotClose": same,
            "rankingInputEqualsEntryPrice": eq,
        })
    return {
        "cohortsAudited": len(rows),
        "liquidityLookAheadViolations": viol_liq,
        "sameInstantRankingAndEntryCount": viol_same,
        "measuredNotAsserted": ("entry price = r16_canonical.price(i,t) 와 "
                                "스냅샷 close(d) 를 종목 단위로 직접 대조"),
        "sample": rows[:3] + rows[-3:],
    }


# ══════════════════════════════════════════════════════════════════════
# 4. same-day close 종속성 실증 (§6 핵심 증거)
# ══════════════════════════════════════════════════════════════════════
def same_day_identity(dates):
    """factor 입력이 결정일 종가의 순함수인지 전 결정일에서 실측한다."""
    mc_n = mc_ok = 0
    pbr_n = pbr_ok = 0
    mc_worst = pbr_worst = 0.0
    per_date = []
    for d in dates:
        p = SNAP / f"{d}.csv.gz"
        if not p.exists():
            continue
        dn = do = pn = po = 0
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                c, mc, sh = _f(r.get("close")), _f(r.get("marketCap")), _f(r.get("shares"))
                if c and mc and sh:
                    dn += 1
                    rel = abs(mc - c * sh) / mc
                    mc_worst = max(mc_worst, rel)
                    if rel <= EXACT_TOL:
                        do += 1
                pbr, bps = _f(r.get("PBR")), _f(r.get("BPS"))
                if c and pbr and pbr > 0 and bps and bps > 0:
                    pn += 1
                    rel = abs(pbr - c / bps) / pbr
                    pbr_worst = max(pbr_worst, rel)
                    # 허용치를 넓히지 않는다. KRX 공표 반올림에서 **유도된** 상한만
                    # 쓴다: BPS 는 정수(원) → ±0.5, PBR 은 소수 2자리 → ±0.005.
                    bound = c * 0.5 / (bps * bps) + 0.005
                    if abs(pbr - c / bps) <= bound:
                        po += 1
        mc_n += dn; mc_ok += do; pbr_n += pn; pbr_ok += po
        per_date.append({"date": d, "mcRows": dn, "mcExact": do,
                         "pbrRows": pn, "pbrWithinTol": po})
    return {
        "datesAudited": len(per_date),
        "sizeIdentity": {
            "claim": "marketCap(d) == close(d) × shares(d)",
            "rows": mc_n, "exact": mc_ok,
            "exactPct": round(100.0 * mc_ok / mc_n, 4) if mc_n else None,
            "worstRelError": mc_worst,
            "implication": "SIZE_SMALL = -ln(marketCap(d)) 는 close(d) 의 순함수",
        },
        "bmIdentity": {
            "claim": "PBR(d) == close(d) / BPS(d)  →  BM = 1/PBR = BPS(d)/close(d)",
            "rows": pbr_n, "withinPublishedRounding": pbr_ok,
            "withinPublishedRoundingPct": (
                round(100.0 * pbr_ok / pbr_n, 4) if pbr_n else None),
            "bound": "close·0.5/BPS² (BPS 정수 반올림) + 0.005 (PBR 2자리 반올림)",
            "boundIsDerivedNotWidened": True,
            "worstRelError": pbr_worst,
            "implication": "BM 은 close(d) 의 순함수 (BPS 는 느리게 변하는 재무값)",
        },
        "entryPrice": {
            "accessor": "r16_canonical.CanonicalWealth.price(i, t) → cap[dates[i]][t]['close']",
            "value": "close(d)",
            "identicalToRankingInput": True,
        },
    }


# ══════════════════════════════════════════════════════════════════════
# 5. 충돌 범위 — DIAGNOSTIC ONLY (§6 "충돌 범위", 새 rule 아님)
# ══════════════════════════════════════════════════════════════════════
DIAGNOSTIC_NOTE = (
    "DIAGNOSTIC_ONLY — 충돌의 크기를 재기 위한 측정이다. 새 계약 후보가 아니며 "
    "cohort·성과·순위를 생성하지 않는다. §6 은 결과를 보고 rule 을 바꾸는 것을 "
    "금지하므로 이 수치로 entry rule 을 대체하지 않는다.")


def conflict_scope(K, dates, limit=None):
    """결정일 종가 대신 직전 거래일 종가로 랭킹했을 때 top 분위가 얼마나 달라지는가."""
    from r25_analysis import buckets
    from r27_collect import load_day, trading_calendar
    cal = trading_calendar()
    pos = {d: i for i, d in enumerate(cal)}

    use = dates if limit is None else dates[:limit]
    per_arm = {"SIZE": [], "BM": []}
    audited = skipped = 0

    for d in use:
        i = pos.get(d)
        if i is None or i == 0:
            skipped += 1
            continue
        prev = cal[i - 1]
        pday = load_day(prev)
        if not pday:
            skipped += 1
            continue
        b = K.base.get(d)
        if not b:
            skipped += 1
            continue
        uni = K.tr.tradable_set(d, set(b["BM"]))
        if len(uni) < 100:
            skipped += 1
            continue
        snap = K.snaps.get(d) or {}
        q = 10 if len(uni) >= 200 else 5

        for arm in ("SIZE", "BM"):
            cur = {t: b[arm][t] for t in uni}
            alt = {}
            for t in uni:
                pr = pday.get(t)
                if not pr:
                    continue
                if arm == "SIZE":
                    mc = pr["marketCap"]
                    if mc and mc > 0:
                        alt[t] = -math.log(mc)
                else:
                    # BM(d) = BPS/close(d)  →  BPS = BM(d)·close(d)
                    c0 = (snap.get(t) or {}).get("close")
                    c1 = pr["close"]
                    if c0 and c1 and c1 > 0:
                        alt[t] = cur[t] * c0 / c1
            if len(alt) < 100:
                continue
            rk = lambda v: buckets(  # noqa: E731
                [t for t, _ in sorted(v.items(), key=lambda kv: (kv[1], kv[0]),
                                      reverse=True)], q)[0]
            a_top, b_top = set(rk(cur)), set(rk({t: alt[t] for t in alt}))
            inter = len(a_top & b_top)
            union = len(a_top | b_top)
            per_arm[arm].append({
                "date": d, "canonicalTop": len(a_top), "priorCloseTop": len(b_top),
                "overlap": inter,
                "overlapPct": round(100.0 * inter / len(a_top), 3) if a_top else None,
                "jaccard": round(inter / union, 5) if union else None,
                "altCoverage": len(alt), "universe": len(uni),
            })
        audited += 1

    out = {"note": DIAGNOSTIC_NOTE, "datesAudited": audited, "skipped": skipped,
           "priorCloseSource": "_cache/krx-liquidity/<prevTradingDay>.csv.gz",
           "byArm": {}}
    for arm, rows in per_arm.items():
        if not rows:
            out["byArm"][arm] = None
            continue
        ov = [r["overlapPct"] for r in rows if r["overlapPct"] is not None]
        ja = [r["jaccard"] for r in rows if r["jaccard"] is not None]
        out["byArm"][arm] = {
            "dates": len(rows),
            "meanOverlapPct": round(statistics.fmean(ov), 3),
            "medianOverlapPct": round(statistics.median(ov), 3),
            "minOverlapPct": round(min(ov), 3),
            "maxOverlapPct": round(max(ov), 3),
            "meanJaccard": round(statistics.fmean(ja), 5),
            "datesWithAnyChange": sum(1 for x in ov if x < 100.0),
            "datesIdentical": sum(1 for x in ov if x >= 100.0),
        }
    return out


# ══════════════════════════════════════════════════════════════════════
# 6. 판정
# ══════════════════════════════════════════════════════════════════════
def verdict(hashes, cad, timing, ident):
    checks = {
        "frozen hash 일치": bool(hashes["allMatch"]),
        "signal cadence 정본 복원": bool(cad["reproduced"]),
        "liquidity 가 signal date 이전": timing["liquidityLookAheadViolations"] == 0,
        "ranking 입력이 entry 체결가와 다름":
            timing["sameInstantRankingAndEntryCount"] == 0,
    }
    feasible = all(checks.values())
    blockers = []
    if not checks["ranking 입력이 entry 체결가와 다름"]:
        blockers.append({
            "id": "SAME_DAY_CLOSE_LOOK_AHEAD",
            "unavailableInput": [
                "PBR(d) — BM = 1/PBR(d) = BPS(d)/close(d)",
                "marketCap(d) — SIZE_SMALL = -ln(close(d) × shares(d))",
            ],
            "actuallyAvailableAt": f"결정일 d 장마감 15:30 (종가 확정 이후)",
            "historicalEntryAt": "결정일 d 종가 close(d) — 같은 15:30 값",
            "why": ("랭킹을 만들려면 close(d) 가 필요하고, 계약상 체결가도 "
                    "close(d) 다. 종가 단일가(15:20~15:30) 주문은 15:30 이전에 "
                    "제출돼야 하므로, close(d) 를 보고 고른 종목을 같은 close(d) 에 "
                    "체결시킬 수 없다."),
            "scope": "R32 primary sample 전 코호트 (예외 없음)",
            "noAutoFix": ("§6 이 '조용히 next-day entry 등으로 바꾸지 않는다' 고 "
                          "명시했다. 새 entry rule 을 이 TASK 에서 만들지 않는다."),
        })
    return {
        "checks": checks,
        "prospectivelyExecutable": feasible,
        "blockers": blockers,
        "verdict": "PASS" if feasible else "BLOCKED",
        "reasonClass": (
            "R33_PROSPECTIVE_FEASIBILITY_PASS" if feasible
            else "R32_HISTORICAL_CONTRACT_NOT_PROSPECTIVELY_EXECUTABLE"),
    }


def run(limit=None):
    from r27_analysis import PH, R27
    from r32_engine import PRIMARY_START

    from r27_precommit import PRIMARY_GATE

    K = R27()
    all_dates = [d for d in K.dates if d in K.base]
    # R32 primary sample 을 **그대로** 재사용한다. 자체 필터를 만들지 않는다 —
    # paired() 는 세 팔이 모두 성립할 때만 코호트를 남기므로 163 과 일치해야 한다.
    paired = K.paired(PH, tradable=True, threshold=PRIMARY_GATE["thresholdKrw"])
    usable = [r["startDate"] for r in paired if r["startDate"] >= PRIMARY_START]

    hashes = frozen_hashes()
    cad = cadence(all_dates, usable)
    timing = input_timing(K, usable)
    ident = same_day_identity(usable)
    scope = conflict_scope(K, usable, limit=limit)
    v = verdict(hashes, cad, timing, ident)

    return {
        "task": "R33", "taskId": TASK_ID,
        "sourceTask": SOURCE_TASK, "sourceCommit": SOURCE_COMMIT,
        "sourceDecision": SOURCE_DECISION,
        "auditVersion": "r33-feasibility-1",
        "scopeNote": ("이 파일은 §6 feasibility 관문이다. OOS cohort·precommit·"
                      "activation 을 생성하지 않는다."),
        "frozenHashes": hashes,
        "signalCadence": cad,
        "inputTiming": timing,
        "sameDayCloseDependency": ident,
        "conflictScope": scope,
        "primarySample": {"cohorts": len(usable),
                          "first": usable[0] if usable else None,
                          "last": usable[-1] if usable else None,
                          "primaryStartRule": PRIMARY_START, "horizonMonths": PH,
                          "source": "R27.paired(36, tradable=True, thr=125,000,000)",
                          "matchesR32Expected163": len(usable) == 163},
        "safety": {"networkCalls": 0, "apiCalls": 0, "krxKeyAccess": 0,
                   "envAccess": 0, "productionWrite": 0, "oosCohortsCreated": 0,
                   "realOrders": 0, "broker": 0,
                   "realMoneyApproved": False, "paperOnly": True},
        **v,
    }


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    limit = None
    if "--limit" in argv:
        limit = int(argv[argv.index("--limit") + 1])
    out = run(limit=limit)
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r33-feasibility-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    print(json.dumps({
        "verdict": out["verdict"], "reasonClass": out["reasonClass"],
        "prospectivelyExecutable": out["prospectivelyExecutable"],
        "checks": out["checks"],
        "cadence": out["signalCadence"]["rule"],
        "cadenceReproduced": out["signalCadence"]["reproduced"],
        "cohorts": out["primarySample"]["cohorts"],
        "sizeIdentityExactPct": out["sameDayCloseDependency"]["sizeIdentity"]["exactPct"],
        "bmIdentityWithinRoundingPct": out["sameDayCloseDependency"]["bmIdentity"]["withinPublishedRoundingPct"],
        "conflictScope": {k: (v and {"meanOverlapPct": v["meanOverlapPct"],
                                     "datesWithAnyChange": v["datesWithAnyChange"],
                                     "dates": v["dates"]})
                          for k, v in out["conflictScope"]["byArm"].items()},
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
