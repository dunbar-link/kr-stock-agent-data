#!/usr/bin/env python3
"""R30 전체 수집 — 공식 소스로 historical 유동성 4,640일을 resumable 하게 받는다.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

Probe PASS 후에만 실행된다(§12). probe 가 통과하지 않았으면 시작하지 않는다.

필요일 정의는 R27 과 **동일**하다 — 결정일 직전 20거래일의 합집합. 새 정의를
만들지 않는다. r27_collect.needed_days 를 그대로 재사용한다(look-ahead 0, §8).

중단돼도 다음 실행이 pending 만 이어받는다(§15). 하루 성공할 때마다 저장한다.

안전: 공식 무료 API · 일일 quota 준수 · 무리한 병렬 0 · 캐시만 write ·
production 원장 write 0 · 실주문 0.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C                                  # noqa: E402
import r30_cache as K                                      # noqa: E402
import r30_source as S                                     # noqa: E402
from r16_audit import contiguous_span                      # noqa: E402
from r27_collect import needed_days, trading_calendar      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

CONSECUTIVE_FAIL_ABORT = 25       # 소스가 막힌 것이다. 목록을 태우지 않는다.
PROGRESS_EVERY = 100


def required_days():
    cal = trading_calendar()
    ds = contiguous_span(sorted(C.load_capital_series()))
    return needed_days(cal, ds), len(cal), len(ds)


def save_progress(obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r30-acquisition-progress-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    return obj


def _source_verdict():
    p = RD / "r30-source-verdict-latest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def probe_allows():
    v = _source_verdict()
    if v is None:
        return False, "PROBE_NOT_RUN"
    return bool(v.get("fullAcquisitionAllowed")), v.get("verdict", "UNKNOWN")


def partial_window():
    """소스가 실제로 덮는 구간만 받는 부분수집 창(§8).

    probe 가 PARTIAL_PERIOD_ONLY 일 때만 열린다. 요구 구간 전체를 못 덮는다는
    사실을 숨기지 않으면서, **덮는 부분은 실제로 확보**하기 위한 것이다.
    시작일은 추정하지 않고 probe 가 이분탐색으로 실측한 값을 그대로 쓴다.

    R27 분석을 여는 것과는 무관하다 — coverage gate 는 그대로 적용된다.
    """
    v = _source_verdict() or {}
    if v.get("verdict") != "PROBE_PASS_PARTIAL_PERIOD_ONLY":
        return None, "NOT_PARTIAL_VERDICT"
    hs = v.get("historyStart") or {}
    start = hs.get("historyStartDate")
    if not start or not hs.get("verified"):
        return None, "HISTORY_START_UNVERIFIED"
    return start, "OK"


def inherit_reuse_allowed():
    """§12 — 기존 R27 111일을 재사용할지. 교차검증 결과가 결정한다."""
    p = RD / "r30-r27-cache-crosscheck-latest.json"
    if not p.exists():
        return False, "CROSSCHECK_NOT_RUN"
    try:
        cc = json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return False, "CROSSCHECK_UNREADABLE"
    return bool(cc.get("inheritReuseAllowed")), cc.get("status", "UNKNOWN")


def main() -> int:
    # 부분수집은 **명시적으로 켤 때만** 열린다. 기본값은 종전대로 probe PASS
    # 게이트다 — 요구 구간을 못 덮는 소스로 조용히 채워 넣지 않기 위해서다.
    partial_opt_in = "--partial" in sys.argv
    allowed, pverdict = probe_allows()
    need, cal_n, dec_n = required_days()
    partial_start = None
    if not allowed and partial_opt_in:
        partial_start, pstat = partial_window()
        if partial_start:
            need = [d for d in need if d >= partial_start]
            allowed = True
        else:
            print(f"[r30] 부분수집 불가: {pstat}", file=sys.stderr)
    inherited = sorted(d for d in need if d in K.r27_have_days())
    reuse, cc_status = inherit_reuse_allowed()

    base = {"task": "R30", **S.source_meta(),
            "tradingCalendarDays": cal_n, "decisionDates": dec_n,
            "requiredDays": len(need),
            "lookbackTradingDays": 20,
            "lookAheadRule": "결정일 직전 20거래일만(§8) — R27 정의 재사용",
            "inheritedR27Days": len(inherited),
            "inheritReuseAllowed": reuse, "crosscheckStatus": cc_status,
            "probeVerdict": pverdict,
            "acquisitionMode": ("PARTIAL_SOURCE_WINDOW" if partial_start
                                else "FULL_REQUIRED_WINDOW"),
            "partialWindowStart": partial_start,
            "partialWindowDays": len(need) if partial_start else None,
            "requiredDaysFullWindow": len(required_days()[0]),
            "partialWindowWhy": (
                "소스가 요구 구간 전체를 못 덮는다. 덮는 구간만 받되 coverage "
                "gate 는 전체 구간 기준 그대로 적용한다(§8·§9)."
                if partial_start else None)}

    if not allowed:
        out = {**base, "status": "NOT_STARTED_PROBE_GATE",
               "why": "Probe PASS 전에는 전체수집을 시작하지 않는다(§3·§10·§12).",
               "completedDays": len(K.have_days() & set(need)),
               "newlyAcquiredDays": 0, "failedDays": 0,
               "pendingDays": len(need), "calls": 0}
        save_progress(out)
        print(json.dumps({k: out[k] for k in
                          ("status", "requiredDays", "pendingDays",
                           "probeVerdict")}, ensure_ascii=False))
        return 4

    # 재사용이 허용되지 않으면 기존 111일도 공식 소스로 다시 받아 단일 소스로 통일
    ck = K.load_checkpoint(need)
    ck = K.sync_checkpoint_from_disk(ck, need)
    todo = list(ck["pending_days"])
    K.save_checkpoint(ck)

    print(f"[r30] 필요 {len(need)} · 보유 {len(ck['completed_days'])} · "
          f"수집 {len(todo)} · R27 상속 {len(inherited)} (재사용 {reuse})",
          file=sys.stderr)

    cli = S.Client()
    new = fail = empty = 0
    streak = 0
    aborted = None
    projected = {"WROTE": 0, "KEPT_EXISTING": 0, "NO_USABLE_ROWS": 0}
    fails = []

    for i, d in enumerate(todo, 1):
        if cli.calls >= cli.quota - 5:
            aborted = f"DAILY_QUOTA_REACHED_{cli.calls}"
            break
        try:
            rows, st, meta = cli.fetch_day(d)
        except S.SchemaMismatch as e:
            rows, st, meta = None, f"SCHEMA:{e}", {}
        if st == "OK" and rows:
            K.write_day(d, rows)
            # R27 이 그대로 읽도록 투영. 기존 R27 파일은 **교차검증이 실제로
            # 불일치를 증명했을 때만** 덮는다. 겹치는 날짜가 없어 검증 자체가
            # 불가능했던 경우(INSUFFICIENT_OVERLAP)에 검증된 데이터를 미검증
            # 데이터로 갈아끼우면 근거 없이 품질을 낮추는 것이다.
            r = K.project_to_r27(d, rows,
                                 overwrite=(cc_status == "MATERIAL_MISMATCH"))
            projected[r] = projected.get(r, 0) + 1
            ck["completed_days"].append(d)
            ck["last_success"] = d
            new += 1
            streak = 0
        elif st == "EMPTY":
            empty += 1
            streak = 0
            fails.append({"date": d, "status": st})
            ck["failed_days"][d] = st
        else:
            fail += 1
            streak += 1
            fails.append({"date": d, "status": st})
            ck["failed_days"][d] = st
            ck["last_error_class"] = st
            print(f"[r30] {d} {st} (streak {streak})", file=sys.stderr)
            if st.startswith("CREDENTIAL_INVALID"):
                aborted = "CREDENTIAL_INVALID"
                break
            if st.startswith("QUOTA_EXHAUSTED"):
                aborted = st
                break
            if streak >= CONSECUTIVE_FAIL_ABORT:
                aborted = f"CONSECUTIVE_FAILURES_{streak}_AT_{d}"
                break
        if i % PROGRESS_EVERY == 0:
            ck["retry_count"] = cli.retries
            ck["pending_days"] = [x for x in need
                                  if x not in set(ck["completed_days"])]
            K.save_checkpoint(ck)
            print(f"[r30] {i}/{len(todo)} new={new} fail={fail} "
                  f"calls={cli.calls}", file=sys.stderr)
        time.sleep(S.SLEEP)

    ck["retry_count"] = cli.retries
    ck = K.sync_checkpoint_from_disk(ck, need)
    K.save_checkpoint(ck)

    have = K.have_days()
    completed = [d for d in need if d in have]
    out = {**base,
           "status": "ABORTED" if aborted else "COMPLETE",
           "aborted": aborted,
           "completedDays": len(completed),
           "newlyAcquiredDays": new,
           "emptyDays": empty,
           "failedDays": fail,
           "pendingDays": len(need) - len(completed),
           "dayCoveragePct": round(100.0 * len(completed) / len(need), 3)
           if need else 0.0,
           "projection": projected,
           "calls": cli.calls, "retries": cli.retries,
           "rateLimitHits": cli.rateLimitHits, "quota": cli.quota,
           "retryPolicy": {"maxRetry": S.MAX_RETRY,
                           "backoffSec": list(S.RETRY_BACKOFF),
                           "sleepBetweenCallsSec": S.SLEEP,
                           "consecutiveFailAbort": CONSECUTIVE_FAIL_ABORT},
           "checkpointPath": str(K.CKPT),
           "resumable": True,
           "cacheDir": str(K.CACHE),
           "failures": fails[:50],
           "paidData": 0, "newCredential": 0, "krxWebAutomation": 0}
    save_progress(out)
    print(json.dumps({k: out[k] for k in
                      ("status", "requiredDays", "completedDays",
                       "newlyAcquiredDays", "failedDays", "dayCoveragePct")},
                     ensure_ascii=False))
    return 0 if not aborted else 6


if __name__ == "__main__":
    raise SystemExit(main())
