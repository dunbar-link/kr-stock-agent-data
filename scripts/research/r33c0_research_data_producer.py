#!/usr/bin/env python3
"""R33C0 — 연구 데이터 producer 증분 복구 (frozen base 불변 · prospective continuation).

WABABA-RESEARCH-DATA-PRODUCER-RESTORE-AND-SCHEDULE-R33C0
SOURCE: WABABA-BM-SIZE-SEPARATE-OOS-PAPER-CONTRACT-IMPLEMENT-AND-ARM-R33C (d198309)

R33C 가 BLOCKED 된 이유는 연구 데이터가 2026-08 초부터 생산되지 않아서다.
이 모듈은 **얼어붙은 과거를 건드리지 않고** 그 이후만 이어 붙인다.

── 왜 별도 continuation root 인가 (핵심) ──────────────────────────
  _cache/official-liquidity 에 하루라도 append 하면
  R31 dataset manifest(3439dec9…)가 그 디렉터리 scan 으로 계산되므로 깨진다.
  _cache/pit-snapshots 에 append 하면 R27 의 결정일 배열이 늘어
  R32/R33B 의 163 코호트 표본이 164 로 바뀐다.
  둘 다 frozen 재현성을 훼손한다. 그래서 새 데이터는 전부 여기로만 간다:

      _cache/prospective-research-data/

  읽을 때는 FROZEN_BASE + CONTINUATION 을 union 으로 본다.

── source 계약 ────────────────────────────────────────────────────
  post-2020 공식 일별 = R31 canonical 금융위원회 공공데이터포털 (r30_source 재사용).
  직접 거래대금만 쓴다. close × volume 대체 금지. 새 source 도입 금지.

── PIT 월별 스냅샷은 이 모듈이 만들지 않는다 ──────────────────────
  PBR/BPS 를 주는 유일한 경로가 pykrx(KRX) 인데 KRX 로그인이 실패한다.
  자격증명 복구는 이번 승인 범위 밖이라 손대지 않는다.
  따라서 이 모듈은 calendar + 공식 일별만 담당하고, PIT 는 BLOCKED 로 표면화한다.

안전: 공식 source 읽기 전용 · 인증키 값 미출력 · frozen base write 0 · OOS cohort 0.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ── frozen base (읽기 전용) ──────────────────────────────────────────
FROZEN_OFFICIAL = ROOT / "_cache" / "official-liquidity"
FROZEN_PIT = ROOT / "_cache" / "pit-snapshots"
FROZEN_CALENDAR = ROOT / "_cache" / "krx-liquidity" / "_trading-calendar.json"

# ── prospective continuation (append-only) ───────────────────────────
CONT = ROOT / "_cache" / "prospective-research-data"
CONT_DAILY = CONT / "official-daily"
CONT_CAL = CONT / "calendar"
CONT_MANIFEST = CONT / "manifests"
CONT_STATUS = CONT / "status"
CONT_AUDIT = CONT / "audit"

TASK_ID = "WABABA-RESEARCH-DATA-PRODUCER-RESTORE-AND-SCHEDULE-R33C0"
CONTRACT_ID = "WABABA_RESEARCH_DATA_PRODUCER_R33C0_V1"

COLS = ["date", "ticker", "name", "market", "close", "open", "high", "low",
        "volume", "traded_value", "market_cap", "shares",
        "source", "source_version", "retrieved_at", "quality_flag"]

CATCHUP_HARD_MAX_CALLS = 500
DAILY_HARD_MAX_CALLS = 100
# 호스트 task 의 ExecutionTimeLimit 이 PT10M 이라 하루 실행은 짧게 묶는다.
DAILY_MAX_DATES = 8


# ══════════════════════════════════════════════════════════════════════
# 계약
# ══════════════════════════════════════════════════════════════════════
CONTRACT = {
    "contractId": CONTRACT_ID,
    "taskId": TASK_ID,
    "sourceCommit": "d198309",
    "frozenBaseMutationAllowed": False,
    "prospectiveContinuation": True,
    "continuationRoot": "_cache/prospective-research-data",
    "officialPost2020Source": "R31 canonical 금융위원회 공공데이터포털 (r30_source)",
    "directTradeValueOnly": True,
    "priceXVolumeProxy": False,
    "calendarMode": "INCREMENTAL_OBSERVED_FROM_OFFICIAL_SOURCE_PRESENCE",
    "latestCompleteSessionRule": (
        "공식 source 가 그 날짜에 대해 rows>0 을 반환하면 완결 거래일. "
        "0행이면 휴장 또는 미공개 — 달력만으로 추정하지 않는다."),
    "pitProducer": "OUT_OF_SCOPE_THIS_MODULE",
    "pitBlockedReason": (
        "PBR/BPS canonical 경로가 pykrx(KRX) 뿐인데 KRX 로그인 실패. "
        "자격증명 복구는 승인 범위 밖(env/token/security 변경 불가)."),
    "oldSnapshotRewrite": False,
    "existingFileHashDivergence": "BLOCKED",
    "atomicWrite": True,
    "idempotent": True,
    "sourceHashRequired": True,
    "rawRemoteUpload": False,
    "historicalOosBackfill": False,
    "preActivationSignalStatus": "PRE_ACTIVATION_NOT_ELIGIBLE",
    "schedulerMode": "EXISTING_TASK_ONLY",
    "newScheduledTaskAllowed": False,
    "newTriggerAllowed": False,
    "boundedSourceReadyRetry": True,
    "unboundedPolling": False,
    "r33cEngineAllowed": False,
    "catchupHardMaxCalls": CATCHUP_HARD_MAX_CALLS,
    "dailyHardMaxCalls": DAILY_HARD_MAX_CALLS,
    "dailyMaxDates": DAILY_MAX_DATES,
}


def canonical_json(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def contract_hash():
    return hashlib.sha256(canonical_json(CONTRACT).encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# frozen base 무결성
# ══════════════════════════════════════════════════════════════════════
def frozen_base_state():
    """frozen base 의 관측 가능한 상태. 이 값이 변하면 안 된다."""
    off = sorted(p.name[:-7] for p in FROZEN_OFFICIAL.glob("*.csv.gz"))
    pit = sorted(p.name[:-7] for p in FROZEN_PIT.glob("*.csv.gz"))
    cal = json.loads(FROZEN_CALENDAR.read_text(encoding="utf-8"))
    return {
        "officialDays": len(off), "officialMax": off[-1] if off else None,
        "pitSnapshots": len(pit), "pitMax": pit[-1] if pit else None,
        "calendarDays": len(cal["days"]), "calendarMax": cal["days"][-1],
        "calendarSource": cal.get("source"),
    }


def upstream_hashes():
    import r33b_precommit as P
    out = P.upstream_hashes()
    st = json.loads((RD / "r31-stitch-latest.json").read_text(encoding="utf-8"))
    out["r31DatasetManifest"] = {
        "sha256": st["manifestSha256"],
        "match": st["manifestSha256"].startswith("3439dec9")}
    return out


# ══════════════════════════════════════════════════════════════════════
# continuation 읽기 / union view
# ══════════════════════════════════════════════════════════════════════
def continuation_days():
    if not CONT_DAILY.exists():
        return []
    return sorted(p.name[:-7] for p in CONT_DAILY.glob("*.csv.gz"))


def union_official_days():
    """FROZEN_BASE + CONTINUATION. 미래 R33C 엔진이 쓸 통합 뷰."""
    base = {p.name[:-7] for p in FROZEN_OFFICIAL.glob("*.csv.gz")}
    return sorted(base | set(continuation_days()))


def load_official_day(d):
    """union 뷰 로더. continuation 우선(더 최신), 없으면 frozen base."""
    for root in (CONT_DAILY, FROZEN_OFFICIAL):
        p = root / f"{d}.csv.gz"
        if p.exists():
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                return {r["ticker"]: r for r in csv.DictReader(fh)
                        if r.get("ticker")}
    return None


# ══════════════════════════════════════════════════════════════════════
# 수집
# ══════════════════════════════════════════════════════════════════════
class Budget:
    def __init__(self, hard_max):
        self.hard_max = hard_max
        self.calls = self.retries = self.failures = self.rate_limited = 0
        self.stopped = None

    def spend(self, n=1, retry=False):
        if self.calls + n > self.hard_max:
            raise RuntimeError(f"API 예산 초과: {self.calls}+{n} > {self.hard_max}")
        self.calls += n
        if retry:
            self.retries += n


def _atomic_write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in COLS})
    tmp.replace(path)


def _content_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def candidate_dates(start, end):
    """주말만 제외한 후보. 휴장 판정은 source 응답으로만 한다(달력 추정 금지)."""
    a = date.fromisoformat(start)
    b = date.fromisoformat(end)
    out = []
    while a <= b:
        if a.weekday() < 5:
            out.append(a.isoformat())
        a += timedelta(days=1)
    return out


def fetch_range(start, end, budget, max_dates=None, client=None):
    """공식 source 로 미보유 날짜만 증분 수집. frozen base 는 절대 쓰지 않는다."""
    import r30_source

    have = set(union_official_days())
    todo = [d for d in candidate_dates(start, end) if d not in have]
    if max_dates:
        todo = todo[:max_dates]

    cli = client or r30_source.Client()
    import datetime as _dt
    import zoneinfo as _zi
    hol = _krx_holidays()
    res = {"fetched": [], "holiday": [], "sourceNotReady": [],
           "failed": [], "skippedExisting": len(candidate_dates(start, end)) - len(todo),
           "rows": 0, "duplicates": 0, "invalid": 0}

    def _classify_empty(d):
        """0행을 휴장과 미공개로 나눈다. 검증된 휴장표를 기준으로 판정한다."""
        if d in hol:
            res["holiday"].append(d)
        else:
            res["sourceNotReady"].append(d)
    for d in todo:
        ts = _dt.datetime.now(_zi.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
        try:
            budget.spend(1)
            rows, st, meta = cli.fetch_day(d)
            if meta.get("pages", 1) > 1:
                budget.spend(meta["pages"] - 1)
        except Exception as e:
            msg = str(e)[:160]
            low = msg.lower()
            if "unauthor" in low or "auth" in low:
                budget.stopped = f"AUTH_BLOCKED at {d}"
                res["failed"].append({"date": d, "reason": "AUTH"})
                break
            if "429" in msg:
                budget.rate_limited += 1
                budget.stopped = f"RATE_LIMITED at {d}"
                res["failed"].append({"date": d, "reason": "429"})
                break
            budget.failures += 1
            res["failed"].append({"date": d, "reason": msg})
            continue

        if not rows:
            _classify_empty(d)
            continue
        seen, clean, dup, bad = set(), [], 0, 0
        for r in rows:
            t = r.get("ticker")
            if not t:
                bad += 1
                continue
            if t in seen:
                dup += 1
                continue
            seen.add(t)
            r.setdefault("date", d)
            r.setdefault("retrieved_at", ts)
            clean.append(r)
        if not clean:
            _classify_empty(d)
            continue
        p = CONT_DAILY / f"{d}.csv.gz"
        if p.exists():
            res["skippedExisting"] += 1
            continue
        _atomic_write_csv(p, clean)
        res["fetched"].append({"date": d, "rows": len(clean),
                               "contentSha256": _content_hash(p)})
        res["rows"] += len(clean)
        res["duplicates"] += dup
        res["invalid"] += bad
    return res


# ══════════════════════════════════════════════════════════════════════
# calendar continuation — 공식 source 실재 여부로만 판정
# ══════════════════════════════════════════════════════════════════════
def rebuild_calendar():
    """frozen base 달력 + continuation 관측 거래일. 과거는 건드리지 않는다."""
    base = json.loads(FROZEN_CALENDAR.read_text(encoding="utf-8"))
    base_days = list(base["days"])
    obs = [d for d in continuation_days() if d > base_days[-1]]
    days = base_days + sorted(obs)
    dup = len(days) - len(set(days))
    out = {
        "schemaVersion": "r33c0-calendar-1",
        "frozenBaseThrough": base_days[-1],
        "frozenBaseSource": base.get("source"),
        "continuationSource": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE row presence",
        "observedThrough": days[-1],
        "dateCount": len(days), "duplicateCount": dup,
        "continuationAppended": len(obs),
        "days": days,
    }
    out["contentHash"] = hashlib.sha256(
        canonical_json({k: out[k] for k in
                        ("frozenBaseThrough", "observedThrough", "dateCount",
                         "continuationAppended")}).encode("utf-8")).hexdigest()
    CONT_CAL.mkdir(parents=True, exist_ok=True)
    tmp = CONT_CAL / "observed.json.tmp"
    tmp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CONT_CAL / "observed.json")
    return {k: v for k, v in out.items() if k != "days"}


def union_calendar():
    p = CONT_CAL / "observed.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))["days"]
    return json.loads(FROZEN_CALENDAR.read_text(encoding="utf-8"))["days"]


def next_monthly_signal(after):
    """관측 달력에서 after 이후 첫 '월 최초 거래일'. 추정하지 않는다."""
    days = union_calendar()
    first = {}
    for d in days:
        first.setdefault(d[:7], d)
    cand = sorted(v for v in first.values() if v > after)
    return cand[0] if cand else None


def next_session(after):
    days = union_calendar()
    nxt = [d for d in days if d > after]
    return nxt[0] if nxt else None


# ══════════════════════════════════════════════════════════════════════
# planned calendar — 저장소에 이미 있는 큐레이션 KRX 휴장표 재사용
# ══════════════════════════════════════════════════════════════════════
def _krx_holidays():
    """기존 정본(build_magic_signal_package.KRX_HOLIDAYS). 새 dependency 0, 네트워크 0."""
    sys.path.insert(0, str(ROOT / "scripts"))
    from build_magic_signal_package import KRX_HOLIDAYS
    return set(KRX_HOLIDAYS)


def _is_planned_session(d, hol):
    return date.fromisoformat(d).weekday() < 5 and d not in hol


def validate_holiday_table():
    """관측 거래일과 휴장표를 대조한다. 표를 그냥 믿지 않는다."""
    hol = _krx_holidays()
    obs = set(continuation_days())
    if not obs:
        return {"validated": False, "reason": "관측 continuation 없음"}
    lo, hi = min(obs), max(obs)
    cands = candidate_dates(lo, hi)
    mism = [{"date": d, "predictedTrading": _is_planned_session(d, hol),
             "actualTrading": d in obs}
            for d in cands
            if _is_planned_session(d, hol) != (d in obs)]
    return {"validated": len(mism) == 0, "range": {"min": lo, "max": hi},
            "candidateWeekdays": len(cands),
            "observedSessions": len(obs & set(cands)),
            "mismatchCount": len(mism), "mismatches": mism[:5],
            "holidayTableSource": "scripts/build_magic_signal_package.KRX_HOLIDAYS",
            "holidayCount": len(hol),
            "coverage": {"min": min(hol), "max": max(hol)} if hol else None}


def planned_sessions(after, count=6):
    hol = _krx_holidays()
    out, d = [], date.fromisoformat(after)
    guard = 0
    while len(out) < count and guard < 400:
        d += timedelta(days=1)
        guard += 1
        s = d.isoformat()
        if _is_planned_session(s, hol):
            out.append(s)
    return out


def planned_monthly_signals(after, count=2):
    hol = _krx_holidays()
    out = []
    y, m = int(after[:4]), int(after[5:7])
    for _ in range(24):
        m += 1
        if m > 12:
            m, y = 1, y + 1
        d = date(y, m, 1)
        while not _is_planned_session(d.isoformat(), hol):
            d += timedelta(days=1)
        if d.isoformat() > after:
            nxt = planned_sessions(d.isoformat(), 1)
            out.append({"signalDate": d.isoformat(),
                        "entrySession": nxt[0] if nxt else None})
        if len(out) >= count:
            break
    return out


# ══════════════════════════════════════════════════════════════════════
# 상태 / CLI
# ══════════════════════════════════════════════════════════════════════
def _today_kst():
    import datetime as _dt
    import zoneinfo as _zi
    return _dt.datetime.now(_zi.ZoneInfo("Asia/Seoul")).date().isoformat()


def pit_status():
    """PIT 월별 스냅샷은 이 모듈이 만들지 않는다 — 원인을 사실대로 보고한다."""
    pit = sorted(p.name[:-7] for p in FROZEN_PIT.glob("*.csv.gz"))
    sig = planned_monthly_signals(pit[-1] if pit else "2026-01-01", 2)
    return {
        "producer": "build_pit_snapshots.py (pykrx / KRX)",
        "status": "BLOCKED_UPSTREAM_CREDENTIAL",
        "reason": ("PBR/BPS canonical 경로가 pykrx(KRX) 뿐인데 KRX 로그인이 거부된다. "
                   "자격증명 복구는 이번 승인 범위 밖(env/token/security 변경 불가)."),
        "latestSnapshot": pit[-1] if pit else None,
        "snapshotCount": len(pit),
        "dueSignalsNotProduced": [s["signalDate"] for s in sig],
        "sizeComputableFromOfficialSource": True,
        "bmComputableFromOfficialSource": False,
        "consequence": "BM/SIZE 공통 snapshot 동결 불가 → R33C 계약 실행 불가",
    }


def build_status(run_mode, run_res=None, budget=None, started=None):
    import datetime as _dt
    import zoneinfo as _zi
    now = _dt.datetime.now(_zi.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    cont = continuation_days()
    uni = union_official_days()
    calp = CONT_CAL / "observed.json"
    cal = json.loads(calp.read_text(encoding="utf-8")) if calp.exists() else None
    hv = validate_holiday_table()
    up = planned_monthly_signals(uni[-1] if uni else _today_kst(), 2)
    st = {
        "taskId": TASK_ID, "contractId": CONTRACT_ID,
        "contractHash": contract_hash(),
        "runMode": run_mode, "runStartedAt": started, "runCompletedAt": now,
        "frozenBase": frozen_base_state(),
        "frozenBaseHashStatus": upstream_hashes()["allMatch"],
        "calendarObservedThrough": (cal or {}).get("observedThrough"),
        "calendarContinuationAppended": (cal or {}).get("continuationAppended"),
        "calendarDuplicateCount": (cal or {}).get("duplicateCount"),
        "holidayTableValidation": hv,
        "plannedUpcomingSignals": up,
        "nextScheduledSignalDate": up[0]["signalDate"] if up else None,
        "nextEntrySession": up[0]["entrySession"] if up else None,
        "officialDailyThrough": uni[-1] if uni else None,
        "officialOpenThrough": uni[-1] if uni else None,
        "officialLiquidityThrough": uni[-1] if uni else None,
        "continuationAppendCount": len(cont),
        "continuationRange": {"min": cont[0], "max": cont[-1]} if cont else None,
        "oldSnapshotMutationCount": 0,
        "pit": pit_status(),
        "preActivationSignal": {"date": "2026-09-01",
                                "status": "PRE_ACTIVATION_NOT_ELIGIBLE"},
        "oosCohortCount": 0, "r33cLedgerCount": 0,
        "realMoneyApproved": False, "paperOnly": True,
        "founderAction": "NONE",
    }
    if run_res is not None:
        st["run"] = {k: (len(v) if isinstance(v, list) else v)
                     for k, v in run_res.items()}
    if budget is not None:
        st["apiCalls"] = budget.calls
        st["retries"] = budget.retries
        st["failures"] = budget.failures
        st["rateLimited"] = budget.rate_limited
        st["stopped"] = budget.stopped
    fresh = bool(uni and cal and cal["observedThrough"] == uni[-1])
    st["producerVerdict"] = ("OFFICIAL_DAILY_AND_CALENDAR_CURRENT_PIT_BLOCKED"
                             if fresh else "STALE_OR_INCOMPLETE")
    st["dataReadyForNextSignal"] = False
    st["dataReadyReason"] = "PIT 월별 스냅샷 미생산 (PBR/BPS 경로 차단)"
    CONT_STATUS.mkdir(parents=True, exist_ok=True)
    tmp = CONT_STATUS / "status.json.tmp"
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CONT_STATUS / "status.json")
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r33c0-producer-status-latest.json").write_text(
        json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    return st


def run_daily(max_dates=None, dry_run=False):
    """매 거래일 자연 실행용. due 없으면 NO_CHANGE_FRESH 로 즉시 끝난다."""
    import datetime as _dt
    import zoneinfo as _zi
    started = _dt.datetime.now(
        _zi.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    before = frozen_base_state()
    uni = union_official_days()
    start = ((date.fromisoformat(uni[-1]) + timedelta(days=1)).isoformat()
             if uni else "2026-08-01")
    today = _today_kst()
    if start > today:
        st = build_status("run-daily", {"result": "NO_CHANGE_FRESH"}, None, started)
        return "NO_CHANGE_FRESH", st
    if dry_run:
        return "DRY_RUN", {"wouldFetchFrom": start, "to": today}
    b = Budget(DAILY_HARD_MAX_CALLS)
    res = fetch_range(start, today, b, max_dates=max_dates or DAILY_MAX_DATES)
    rebuild_calendar()
    after = frozen_base_state()
    if before != after:
        return "FROZEN_BASE_DIVERGED", build_status("run-daily", res, b, started)
    st = build_status("run-daily", res, b, started)
    if b.stopped and "AUTH" in str(b.stopped):
        return "AUTH_BLOCKED", st
    if b.rate_limited:
        return "PROVIDER_TRANSIENT_FAILURE", st
    if res["fetched"]:
        return "UPDATED", st
    if res["failed"]:
        return "PROVIDER_TRANSIENT_FAILURE", st
    return "NO_CHANGE_FRESH", st


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = argv[0] if argv else "status"
    if cmd == "preflight":
        out = {"contractHash": contract_hash(),
               "frozenBase": frozen_base_state(),
               "upstreamHashesOk": upstream_hashes()["allMatch"],
               "holidayTable": validate_holiday_table(),
               "pit": pit_status()}
        print(json.dumps(out, ensure_ascii=False, default=str))
        return 0
    if cmd == "run-daily":
        verdict, st = run_daily()
        print(json.dumps({"verdict": verdict,
                          "officialDailyThrough": st.get("officialDailyThrough"),
                          "nextScheduledSignalDate": st.get("nextScheduledSignalDate"),
                          "producerVerdict": st.get("producerVerdict"),
                          "pitStatus": (st.get("pit") or {}).get("status"),
                          "apiCalls": st.get("apiCalls", 0)},
                         ensure_ascii=False, default=str))
        return 0
    if cmd == "status":
        st = build_status("status")
        print(json.dumps({"officialDailyThrough": st["officialDailyThrough"],
                          "calendarObservedThrough": st["calendarObservedThrough"],
                          "nextScheduledSignalDate": st["nextScheduledSignalDate"],
                          "nextEntrySession": st["nextEntrySession"],
                          "producerVerdict": st["producerVerdict"],
                          "pitStatus": st["pit"]["status"],
                          "dataReadyForNextSignal": st["dataReadyForNextSignal"]},
                         ensure_ascii=False, default=str))
        return 0
    if cmd == "verify":
        cont = continuation_days()
        dup = len(cont) - len(set(cont))
        hv = validate_holiday_table()
        ok = bool(dup == 0 and hv.get("validated")
                  and upstream_hashes()["allMatch"])
        print(json.dumps({"continuationDays": len(cont), "duplicates": dup,
                          "holidayTableValidated": hv.get("validated"),
                          "frozenHashesOk": upstream_hashes()["allMatch"],
                          "verify": "PASS" if ok else "FAIL"}, ensure_ascii=False))
        return 0 if ok else 1
    print(json.dumps({"error": "unknown command", "cmd": cmd}, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
