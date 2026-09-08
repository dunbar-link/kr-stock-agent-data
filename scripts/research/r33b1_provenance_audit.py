#!/usr/bin/env python3
"""R33B1 — 보완수집 provenance + no-fill/현금/turnover 회계 감사.

WABABA-R33B-SUPPLEMENT-AND-NOFILL-PROVENANCE-AUDIT-R33B1
SOURCE: WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B (5f8e7ba)
CONTRACT: WABABA_EXECUTION_TRANSLATION_R33A_V1 (d869f445…) — 변경하지 않는다

읽기 전용 감사다. 계약·factor·universe·threshold·horizon 을 바꾸지 않는다.
네트워크 0 · API 0 · 인증키 접근 0.

── 답해야 할 것 ──────────────────────────────────────────────────
  A. plan 계보 50 → 62 · 103 calls 의 정확한 구성
  B. legitimate no-fill 6,680 이 어느 level 의 분모인가
  C. 선택 포지션 기준 no-fill 과 현금비중
  D. 비용에 들어간 turnover 가 target book 인가 filled book 인가
  E. TICKER_ABSENT 8건의 provenance

── D 가 이 감사의 핵심이다 ────────────────────────────────────────
  R33A 계약: turnoverComputedOn = "실제 체결된 holdings 계열"
  r32_engine.turnover_of(): base[d][arm] + tradable_set 만 본다 — 체결 여부 미참조
  → 계약과 구현이 어긋난다. 계약이 아니라 **구현**을 고친다.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

TASK_ID = "WABABA-R33B-SUPPLEMENT-AND-NOFILL-PROVENANCE-AUDIT-R33B1"
SOURCE_COMMIT = "5f8e7ba"
CONTRACT_HASH = (
    "d869f445af1b8d8449c454e58ab2c0bb93bb5e6fcd61c147157c7305c0259b99")


# ══════════════════════════════════════════════════════════════════════
# 선택 포지션 단위 no-fill / 현금 (LEVEL B·C·D)
# ══════════════════════════════════════════════════════════════════════
def selected_positions(K, book, cohorts, thr):
    """163 cohort × arm 의 **실제 선택 포지션**에서 체결/미체결을 센다.

    universe 전체가 아니라 계약이 실제로 담는 종목만 본다.
    """
    from r25_analysis import buckets
    rows = []
    for c in cohorts:
        d, ent = c["signalDate"], c["entryDate"]
        i = K.pos.get(d)
        b = K.base.get(d)
        if i is None or not b:
            continue
        uni = K.tr.tradable_set(d, set(b["BM"]), thr)
        if len(uni) < 100:
            continue
        q = 10 if len(uni) >= 200 else 5
        sel = {}
        for arm in ("SIZE", "BM"):
            vals = {t: b[arm][t] for t in uni}
            ranked = [t for t, _ in sorted(vals.items(),
                                           key=lambda kv: (kv[1], kv[0]),
                                           reverse=True)]
            sel[arm] = buckets(ranked, q)[0]
        sel["CONTROL"] = sorted(uni)
        for arm, names in sel.items():
            cnt = {"VALID": 0, "NO_TRADE": 0, "NO_OPENING_PRICE": 0,
                   "TICKER_ABSENT": 0, "DATE_MISSING": 0, "FIELD_NULL": 0}
            absent = []
            for t in names:
                px, why = book.valid_open(ent, t)
                cnt["VALID" if px is not None else why] += 1
                if px is None and why == "TICKER_ABSENT":
                    absent.append(t)
            n = len(names)
            fills = cnt["VALID"]
            rows.append({
                "signalDate": d, "entryDate": ent, "arm": arm,
                "targetCount": n, "filled": fills, "noFill": n - fills,
                "reasons": cnt, "absentTickers": absent,
                "targetWeightSum": 1.0,
                "filledWeightSum": fills / n if n else 0.0,
                "cashWeight": (n - fills) / n if n else 0.0,
            })
    return rows


def cash_summary(rows):
    out = {}
    for arm in ("SIZE", "BM", "CONTROL"):
        rs = [r for r in rows if r["arm"] == arm]
        if not rs:
            out[arm] = None
            continue
        cw = [r["cashWeight"] for r in rs]
        tgt = sum(r["targetCount"] for r in rs)
        nf = sum(r["noFill"] for r in rs)
        reasons = {}
        for r in rs:
            for k, v in r["reasons"].items():
                if k != "VALID":
                    reasons[k] = reasons.get(k, 0) + v
        out[arm] = {
            "cohorts": len(rs),
            "selectedTargetPositions": tgt,
            "selectedFilled": sum(r["filled"] for r in rs),
            "selectedNoFill": nf,
            "noFillReasons": reasons,
            # cohort 단순평균 — R33B 가 보고한 정의
            "meanCohortCashWeightPct": statistics.fmean(cw) * 100.0,
            "medianCohortCashWeightPct": statistics.median(cw) * 100.0,
            "maxCohortCashWeightPct": max(cw) * 100.0,
            # 전체 포지션 비율 (동일가중 대상 수가 cohort 마다 달라 값이 다르다)
            "pooledPositionNoFillPct": (100.0 * nf / tgt) if tgt else None,
            "cohortsWithZeroCash": sum(1 for x in cw if x == 0.0),
            "cohortsWithPositiveCash": sum(1 for x in cw if x > 0.0),
            "weightIdentityHolds": all(
                abs(r["targetWeightSum"] - (r["filledWeightSum"] + r["cashWeight"]))
                < 1e-12 for r in rs),
        }
    return out


# ══════════════════════════════════════════════════════════════════════
# §13 turnover — target book vs filled book
# ══════════════════════════════════════════════════════════════════════
def turnover_books(K, book, cohorts, thr):
    """같은 날짜열에서 target 기준과 filled 기준 회전율을 각각 계산한다.

    정의는 r32_engine.turnover_of 와 동일하다(1 − |cur∩prev|/|prev|).
    바뀌는 것은 cur 의 구성원이 '목표 선택' 이냐 '실제 체결' 이냐 뿐이다.
    """
    from r25_analysis import buckets
    cmap = {c["signalDate"]: c for c in cohorts}
    dates = [c["signalDate"] for c in cohorts]
    out = {}
    for arm in ("SIZE", "BM"):
        tgt_turns, fil_turns = [], []
        prev_t = prev_f = None
        for d in dates:
            b = K.base.get(d)
            if not b:
                prev_t = prev_f = None
                continue
            uni = K.tr.tradable_set(d, set(b["BM"]), thr)
            if len(uni) < 100:
                prev_t = prev_f = None
                continue
            q = 10 if len(uni) >= 200 else 5
            vals = {t: b[arm][t] for t in uni}
            ranked = [t for t, _ in sorted(vals.items(),
                                           key=lambda kv: (kv[1], kv[0]),
                                           reverse=True)]
            cur_t = set(buckets(ranked, q)[0])
            ent = cmap[d]["entryDate"]
            cur_f = {t for t in cur_t if book.valid_open(ent, t)[0] is not None}
            if prev_t:
                tgt_turns.append(1.0 - len(cur_t & prev_t) / len(prev_t))
            if prev_f:
                fil_turns.append(1.0 - len(cur_f & prev_f) / len(prev_f))
            prev_t, prev_f = cur_t, cur_f
        out[arm] = {
            "observations": len(tgt_turns),
            "targetBookMonthly": statistics.fmean(tgt_turns) if tgt_turns else None,
            "targetBookAnnual": (statistics.fmean(tgt_turns) * 12
                                 if tgt_turns else None),
            "filledBookMonthly": statistics.fmean(fil_turns) if fil_turns else None,
            "filledBookAnnual": (statistics.fmean(fil_turns) * 12
                                 if fil_turns else None),
        }
        a, bq = out[arm]["targetBookAnnual"], out[arm]["filledBookAnnual"]
        out[arm]["annualDelta"] = (bq - a) if (a is not None and bq is not None) else None
    return out


def cost_curve(gross_pct, annual_turnover, bps_list=(0, 25, 50, 100)):
    """R33A/R32 동결 비용식 그대로: drag = annualTurnover × bps / 100."""
    return {f"{b}bp": {"grossPct": gross_pct,
                       "costDragPct": (annual_turnover * b / 100.0)
                       if annual_turnover else 0.0,
                       "netPct": gross_pct - ((annual_turnover * b / 100.0)
                                              if annual_turnover else 0.0)}
            for b in bps_list}


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r33b1-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    return p


# ══════════════════════════════════════════════════════════════════════
# §13 최소 수정 — filled-book turnover 주입 (R32 동결파일은 건드리지 않는다)
# ══════════════════════════════════════════════════════════════════════
def make_filled_turnover_of(book, cohort_map, cal):
    """r32_engine.turnover_of 와 **정의가 같고** 구성원만 실제 체결로 좁힌다.

    R33A 계약: turnoverComputedOn = "실제 체결된 holdings 계열".
    원본은 base[d][arm] + tradable_set 만 보고 체결여부를 참조하지 않는다.
    정의(1 − |cur∩prev|/|prev|)·날짜열·tie-break 는 그대로 두고
    cur 에서 미체결 종목만 제외한다.

    체결증거가 없는 날짜(entry 일자 파일 부재)는 target membership 으로
    fallback 하고 그 횟수를 센다 — 없는 근거를 만들지 않는다.
    """
    stats = {"fallbackDates": 0, "evidenceDates": 0}
    cpos = {d: i for i, d in enumerate(cal)}

    def filled_turnover_of(K, arm, dates):
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
            cm = cohort_map.get(d)
            ent = cm["entryDate"] if cm else None
            if ent is None:
                i = cpos.get(d)
                ent = cal[i + 1] if i is not None and i + 1 < len(cal) else None
            if ent is not None and book.day(ent) is not None:
                stats["evidenceDates"] += 1
                cur = {t for t in cur
                       if book.valid_open(ent, t)[0] is not None}
            else:
                stats["fallbackDates"] += 1
            if prev and cur:
                turns.append(1.0 - len(cur & prev) / len(prev))
            prev = cur
        mt = statistics.fmean(turns) if turns else None
        return {"observations": len(turns),
                "monthlyTurnover": round(mt, 4) if mt is not None else None,
                "annualTurnover": round(mt * 12, 3) if mt is not None else None,
                "monthlyTurnoverFull": mt,
                "annualTurnoverFull": mt * 12 if mt is not None else None,
                "basis": "FILLED_BOOK"}

    return filled_turnover_of, stats
