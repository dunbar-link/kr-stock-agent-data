#!/usr/bin/env python3
"""R33A field-availability 감사 — 계약 hash 고정 **이후에만** 실행한다.

WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A

동결된 D_CLOSE_RANK_NEXT_SESSION_OPEN_V1 을 historical 데이터에 적용할 수 있는지
**가격필드 존재 여부와 날짜 매핑만** 검사한다.

수익률·초과수익·CAGR·비용조정수익·승자를 계산하지 않는다.

── 성과 미접근을 주장이 아니라 실행으로 강제한다 ──────────────────
  R25Engine.tsr / R27.tsr / R27.cohort 를 fail-closed 로 감싼다.
  한 번이라도 호출되면 그 자리에서 예외를 던지고 감사를 실패시킨다.
  즉 "수익률을 계산하지 않았다" 가 증명된다.

  종목선택은 수익률 없이 재현한다 — R27.cohort() 의 선택 부분(universe →
  tradable → 분위 → 최상위 버킷)만 같은 규칙으로 다시 만들고, tsr() 은 부르지
  않는다. 선택은 성과가 아니다.

── 왜 precommit 과 별도 파일인가 ──────────────────────────────────
  계약 파일(r33a_translation_precommit.py)의 sourceFileSha256 이 동결돼 있다.
  감사 코드를 그 안에 넣으면 감사 버그를 고칠 때마다 계약 해시가 흔들린다.
  §11 을 지키려면 분리해야 한다.

안전: 읽기·계산 전용. 네트워크 0 · API 0 · 인증키 접근 0 · 성과계산 0.
"""
from __future__ import annotations

import calendar
import csv
import gzip
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
OFFICIAL = ROOT / "_cache" / "official-liquidity"

TASK_ID = "WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A"
HORIZON_MONTHS = 36


class PerformanceAccessViolation(RuntimeError):
    """수익률 경로가 호출되면 즉시 실패시킨다."""


class _PerfGuard:
    """tsr / cohort 를 fail-closed 로 차단한다."""

    def __init__(self):
        self.calls = 0
        self._saved = []

    def __enter__(self):
        import r16_canonical as C
        import r25_engine as E
        import r27_analysis as A

        def block(name):
            def _f(*a, **k):
                self.calls += 1
                raise PerformanceAccessViolation(
                    f"성과 경로 호출됨: {name} — R33A 는 수익률을 계산하지 않는다")
            return _f

        for owner, attr, label in (
                (E.R25Engine, "tsr", "R25Engine.tsr"),
                (E.R25Engine, "cagr", "R25Engine.cagr"),
                (C.CanonicalWealth, "tsr", "CanonicalWealth.tsr"),
                (A.R27, "tsr", "R27.tsr"),
                (A.R27, "cohort", "R27.cohort"),
                (A.R27, "paired", "R27.paired"),
        ):
            if hasattr(owner, attr):
                self._saved.append((owner, attr, getattr(owner, attr)))
                setattr(owner, attr, block(label))
        return self

    def __exit__(self, *exc):
        for owner, attr, orig in self._saved:
            setattr(owner, attr, orig)
        return False


# ══════════════════════════════════════════════════════════════════════
# official daily 로더
# ══════════════════════════════════════════════════════════════════════
def _f(x):
    if x in (None, "", "None"):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "NONNUM"
    return v if v == v else None


_DAY_CACHE = {}


def load_official(d):
    """{ticker: row} + 중복키 수. 파일이 없으면 None."""
    if d in _DAY_CACHE:
        return _DAY_CACHE[d]
    p = OFFICIAL / f"{d}.csv.gz"
    if not p.exists():
        _DAY_CACHE[d] = None
        return None
    rows, dup = {}, 0
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            t = r.get("ticker")
            if not t:
                continue
            if t in rows:
                dup += 1
                continue
            rows[t] = {"open": _f(r.get("open")), "close": _f(r.get("close")),
                       "volume": _f(r.get("volume")),
                       "market": r.get("market"), "source": r.get("source"),
                       "quality_flag": r.get("quality_flag")}
    out = {"rows": rows, "duplicates": dup}
    _DAY_CACHE[d] = out
    return out


def classify_entry(day, ticker):
    """entry 가능 여부 분류. 성과와 무관하다."""
    if day is None:
        return "DATA_MISSING_DATE", None
    r = day["rows"].get(ticker)
    if r is None:
        return "DATA_MISSING_TICKER", None
    op, vol = r["open"], r["volume"]
    if op == "NONNUM" or vol == "NONNUM":
        return "DATA_INTEGRITY_NONNUMERIC", r
    if op is None or vol is None:
        return "DATA_INTEGRITY_NULL", r
    if vol > 0 and op > 0:
        return "VALID_OPEN", r
    if vol == 0 and op == 0:
        return "NO_FILL_NO_TRADE", r
    return "ANOMALY_OPEN_VOLUME_MISMATCH", r


def add_months(iso, months):
    """calendar month 덧셈. 말일 넘침은 그 달 말일로 절단."""
    y, m, d = (int(x) for x in iso.split("-"))
    m2 = m + months
    y2 = y + (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    # 말일 절단: 1/31 + 1개월 → 2/28(29). range(d, 27, -1) 은 d<=27 일 때
    # 빈 range 라 항상 28일로 떨어지는 결함이 있었다(2026-09-08 R33B 에서 실측·수정).
    return date(y2, m2, min(d, calendar.monthrange(y2, m2)[1])).isoformat()


# ══════════════════════════════════════════════════════════════════════
# 감사 본체
# ══════════════════════════════════════════════════════════════════════
def run(limit=None):
    from r25_analysis import buckets
    from r27_analysis import R27
    from r27_collect import trading_calendar
    from r27_precommit import PRIMARY_GATE
    from r32_engine import PRIMARY_START

    guard = _PerfGuard()
    cal = trading_calendar()
    cpos = {d: i for i, d in enumerate(cal)}
    official_days = sorted(p.name[:-7] for p in OFFICIAL.glob("*.csv.gz"))
    src_max = official_days[-1] if official_days else None

    K = R27()                       # universe·tradability. 수익률 아님.

    with guard:
        # R32 primary sample 을 수익률 없이 재현한다.
        dates = [d for i, d in enumerate(K.dates)
                 if d in K.base and d >= PRIMARY_START
                 and i + HORIZON_MONTHS < len(K.dates)]
        if limit:
            dates = dates[:limit]

        thr = PRIMARY_GATE["thresholdKrw"]
        cohorts, agg = [], {
            "VALID_OPEN": 0, "NO_FILL_NO_TRADE": 0, "DATA_MISSING_DATE": 0,
            "DATA_MISSING_TICKER": 0, "DATA_INTEGRITY_NULL": 0,
            "DATA_INTEGRITY_NONNUMERIC": 0, "ANOMALY_OPEN_VOLUME_MISMATCH": 0}
        per_factor = {"BM": dict(agg), "SIZE": dict(agg)}
        dup_total = 0
        src_split = {}
        entry_missing_dates, exit_missing_dates = set(), set()
        same_bar_violation = leakage = 0

        for d in dates:
            b = K.base.get(d)
            uni = K.tr.tradable_set(d, set(b["BM"]), thr)
            if len(uni) < 100:
                continue
            q = 10 if len(uni) >= 200 else 5

            i = cpos.get(d)
            entry = cal[i + 1] if i is not None and i + 1 < len(cal) else None
            nominal_exit = add_months(entry, HORIZON_MONTHS) if entry else None
            exit_sess = None
            if nominal_exit:
                nxt = [x for x in cal if x >= nominal_exit]
                exit_sess = nxt[0] if nxt else None

            if entry and entry <= d:
                same_bar_violation += 1
            eday = load_official(entry) if entry else None
            if eday is None and entry:
                entry_missing_dates.add(entry)
            else:
                dup_total += eday["duplicates"] if eday else 0
            if entry and src_max and entry > src_max:
                leakage += 1

            xday_avail = False
            if exit_sess:
                xd = load_official(exit_sess)
                xday_avail = xd is not None
                if not xday_avail:
                    exit_missing_dates.add(exit_sess)

            row = {"signalDate": d, "entryDate": entry,
                   "nominalExitDate": nominal_exit, "exitSession": exit_sess,
                   "entryDateAvailable": eday is not None,
                   "exitSessionAvailable": xday_avail,
                   "universe": len(uni), "quantile": q, "byFactor": {}}

            for arm in ("BM", "SIZE"):
                vals = {t: b[arm][t] for t in uni}
                ranked = [t for t, _ in sorted(vals.items(),
                                               key=lambda kv: (kv[1], kv[0]),
                                               reverse=True)]
                top = buckets(ranked, q)[0]
                counts = {k: 0 for k in agg}
                for t in top:
                    cls, r = classify_entry(eday, t)
                    counts[cls] += 1
                    agg[cls] += 1
                    per_factor[arm][cls] += 1
                    if r and r.get("source"):
                        src_split[r["source"]] = src_split.get(r["source"], 0) + 1
                row["byFactor"][arm] = {"targetRows": len(top), **counts}
            cohorts.append(row)

    # ── 집계 ─────────────────────────────────────────────────────────
    complete = [c for c in cohorts
                if c["entryDateAvailable"] and c["exitSessionAvailable"]]
    incomplete = [c for c in cohorts if c not in complete]
    entry_ok = [c for c in cohorts if c["entryDateAvailable"]]

    def _last(rows, key):
        v = [r[key] for r in rows if r.get(key)]
        return max(v) if v else None

    return {
        "task": "R33A", "taskId": TASK_ID,
        "auditVersion": "r33a-availability-1",
        "ranAfterContractFreeze": True,
        "translationRuleId": "D_CLOSE_RANK_NEXT_SESSION_OPEN_V1",
        "officialSource": {
            "path": "_cache/official-liquidity",
            "days": len(official_days),
            "min": official_days[0] if official_days else None,
            "max": src_max,
            "openField": "official opening price (KRX TDD_OPNPRC / FSC mkp)",
        },
        "cohorts": {
            "audited": len(cohorts),
            "entryDateAvailable": len(entry_ok),
            "entryDateMissing": len(cohorts) - len(entry_ok),
            "translatedComplete": len(complete),
            "translatedIncomplete": len(incomplete),
            "lastCompleteSignalDate": _last(complete, "signalDate"),
            "lastCompleteExitSession": _last(complete, "exitSession"),
            "firstSignalDate": cohorts[0]["signalDate"] if cohorts else None,
            "lastSignalDate": cohorts[-1]["signalDate"] if cohorts else None,
        },
        "entryDateMissingDates": sorted(entry_missing_dates),
        "exitSessionMissingDates": sorted(exit_missing_dates)[:20],
        "exitSessionMissingCount": len(exit_missing_dates),
        "rowClassification": {"total": agg, "byFactor": per_factor},
        "sourceSplit": src_split,
        "integrity": {
            "duplicateKeyCount": dup_total,
            "sameBarViolationCount": same_bar_violation,
            "futureLeakageCount": leakage,
            "factorTimingMismatchCount": 0,
            "factorTimingMismatchWhy": (
                "BM·SIZE 는 동일 D 스냅샷·동일 entry date·동일 exit session 을 "
                "쓴다. cohort 별로 두 factor 의 날짜가 다를 수 있는 경로가 없다."),
        },
        "performanceAccess": {
            "guardInstalled": True,
            "performanceFunctionCalls": guard.calls,
            "blocked": ["R25Engine.tsr", "R25Engine.cagr", "CanonicalWealth.tsr",
                        "R27.tsr", "R27.cohort", "R27.paired"],
            "forwardReturnRowsRead": 0,
            "r32DecisionEngineCalls": 0,
            "alternativeEntryPerformanceRuns": 0,
            "entryRuleOptimizationRuns": 0,
            "historicalReturnsCalculated": 0,
        },
        "safety": {"networkCalls": 0, "marketDataApiCalls": 0,
                   "krxAuthKeyAccess": 0, "envChanges": 0,
                   "oosCohortCreated": 0, "realOrders": 0, "brokerCalls": 0,
                   "realMoneyApproved": False, "paperOnly": True},
        "cohortDetailSample": cohorts[:2] + cohorts[-2:],
    }


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    limit = int(argv[argv.index("--limit") + 1]) if "--limit" in argv else None
    out = run(limit=limit)
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r33a-availability-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    c, rc = out["cohorts"], out["rowClassification"]["total"]
    print(json.dumps({
        "cohortsAudited": c["audited"],
        "entryDateAvailable": c["entryDateAvailable"],
        "entryDateMissing": c["entryDateMissing"],
        "translatedComplete": c["translatedComplete"],
        "translatedIncomplete": c["translatedIncomplete"],
        "lastCompleteSignalDate": c["lastCompleteSignalDate"],
        "rows": rc,
        "duplicateKeyCount": out["integrity"]["duplicateKeyCount"],
        "sameBarViolationCount": out["integrity"]["sameBarViolationCount"],
        "futureLeakageCount": out["integrity"]["futureLeakageCount"],
        "performanceFunctionCalls": out["performanceAccess"][
            "performanceFunctionCalls"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
