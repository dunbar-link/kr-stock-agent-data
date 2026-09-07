#!/usr/bin/env python3
"""R31 수집 — anchor 검증 후 2010~2019 백필과 2020 overlap 을 받는다.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

── 새로 만들지 않은 것 ────────────────────────────────────────────
  필요일 정의     r27_collect.needed_days (결정일 직전 20거래일 합집합, look-ahead 0)
  거래일 캘린더    r27_collect.trading_calendar (기존 캐시)
  저장 스키마·원자적 write·R27 투영   r30_cache (write_day / project_to_r27)
  즉 R27 은 코드 한 줄 바꾸지 않고 이 캐시를 그대로 읽는다.

── 두 목적지를 분리한다 (§10 stitch 계약) ─────────────────────────
  2010-01-04 ~ 2019 마지막 거래일 → _cache/official-liquidity/  (canonical, source=KRX)
  2020 전체 거래일                → _cache/krx-overlap-2020/     (비교 근거 전용)
  2020 canonical 은 기존 R30 공공데이터포털 행을 그대로 둔다 — KRX 2020 을
  canonical 에 덮어쓰거나 중복 삽입하지 않는다.

── 안전 ───────────────────────────────────────────────────────────
  최대 2 req/s · 429 즉시 중단 · auth/승인 오류 재시도 0 · 5xx bounded retry ·
  retry 도 예산 포함 · 원자적 write · resume(완료일 재호출 0) · 키 미기록.

사용:
  python scripts/research/r31_acquire.py --anchors
  python scripts/research/r31_acquire.py --backfill [--max-seconds 540]
  python scripts/research/r31_acquire.py --overlap  [--max-seconds 540]
  python scripts/research/r31_acquire.py --status
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C                                   # noqa: E402
import r30_cache as K                                       # noqa: E402
import r31_credential as CRED                               # noqa: E402
import r31_source as S                                      # noqa: E402
from r16_audit import contiguous_span                       # noqa: E402
from r27_collect import needed_days, trading_calendar       # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
OVERLAP_CACHE = ROOT / "_cache" / "krx-overlap-2020"
CKPT = ROOT / "_cache" / "official-liquidity" / "_checkpoint-r31.json"

BACKFILL_START = "2010-01-04"
BACKFILL_END = "2019-12-31"       # 실제 마지막 거래일은 캘린더가 정한다
MARKETS = ["KOSPI", "KOSDAQ"]
HARD_DAILY_CAP = 7500
PROGRESS_EVERY = 50


class Budget:
    """probe·retry·bulk 를 한 계정에 모아 센다(§6). checkpoint 에 영속된다."""

    def __init__(self, used=0, retries=0, cap=HARD_DAILY_CAP):
        self.calls = used
        self.retries = retries
        self.cap = cap

    def spend(self, retry=False):
        self.calls += 1
        if retry:
            self.retries += 1
        if self.calls > self.cap:
            raise RuntimeError("BUDGET_EXCEEDED")

    def left(self):
        return self.cap - self.calls


# ══════════════════════ 대상일 ══════════════════════
def scopes():
    cal = trading_calendar()
    ds = contiguous_span(sorted(C.load_capital_series()))
    need = needed_days(cal, ds)
    back = [d for d in need if BACKFILL_START <= d <= BACKFILL_END]
    ovl = [d for d in need if d.startswith("2020")]
    return {"calendarDays": len(cal), "decisionDates": len(ds),
            "neededDays": len(need), "backfill": back, "overlap": ovl}


# ══════════════════════ checkpoint ══════════════════════
def load_ckpt():
    if CKPT.exists():
        try:
            return json.loads(CKPT.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            pass
    return {"task": "R31", "source": S.SOURCE,
            "callsUsed": 0, "retries": 0, "rateLimitHits": 0,
            "backfillDone": [], "overlapDone": [],
            "failed": {}, "aborted": None,
            "budgetDate": datetime.now().strftime("%Y-%m-%d"),
            "updated_at": None}


def save_ckpt(ck):
    ck["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    K._atomic_write(CKPT, lambda tmp: tmp.write_text(
        json.dumps(ck, ensure_ascii=False, indent=2), encoding="utf-8"))


def _sync_from_disk(ck, back, ovl):
    """실제 파일을 정본으로 checkpoint 를 맞춘다(수동 삭제·중단 복구)."""
    have = K.have_days()
    ck["backfillDone"] = [d for d in back if d in have]
    ovl_have = ({p.name[:-7] for p in OVERLAP_CACHE.glob("*.csv.gz")}
                if OVERLAP_CACHE.exists() else set())
    ck["overlapDone"] = [d for d in ovl if d in ovl_have]
    return ck


# ══════════════════════ overlap 전용 writer ══════════════════════
def write_overlap_day(iso_date, rows, retrieved_at):
    """canonical 을 오염시키지 않는 별도 목적지. 스키마는 R30 것을 그대로 쓴다."""
    def _w(tmp):
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=K.COLS, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                r = dict(r)
                r["retrieved_at"] = retrieved_at
                for k in K.COLS:
                    r.setdefault(k, None)
                    if r[k] is None:
                        r[k] = ""
                w.writerow(r)
    K._atomic_write(OVERLAP_CACHE / f"{iso_date}.csv.gz", _w)


def load_overlap_day(iso_date):
    p = OVERLAP_CACHE / f"{iso_date}.csv.gz"
    if not p.exists():
        return None
    out = {}
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            row = {}
            for k, v in r.items():
                if k in ("date", "ticker", "name", "market", "source",
                         "source_version", "retrieved_at", "quality_flag"):
                    row[k] = v
                else:
                    row[k] = None if (v is None or v == "") else float(v)
            out[str(r.get("ticker") or "")] = row
    return out


# ══════════════════════ 하루 수집 ══════════════════════
def fetch_both_markets(iso_date, key, budget):
    """KOSPI+KOSDAQ 을 받아 한 날짜의 행 목록으로 합친다. duplicate ticker 검사 포함."""
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows, metas, seen = [], [], {}
    dup = 0
    for m in MARKETS:
        raw, meta = S.fetch_day(m, iso_date, key, budget)
        metas.append(meta)
        for rr in raw:
            row = S.normalize(rr, iso_date, m, ts)
            t = row["ticker"]
            if not t:
                continue
            if t in seen:
                dup += 1
                continue
            seen[t] = True
            rows.append(row)
    return rows, metas, dup, ts


# ══════════════════════ §5 anchor 검증 ══════════════════════
def anchors():
    sc = scopes()
    cal = trading_calendar()
    y2014 = [d for d in cal if d.startswith("2014")]
    y2019 = [d for d in cal if d.startswith("2019")]
    dates = [BACKFILL_START, y2014[0], y2019[-1], "2020-01-02"]

    key = CRED.auth_key()
    budget = Budget()
    out = {"task": "R31", "phase": "anchor", "dates": dates, "checks": [],
           "verdict": "PASS", "failures": []}

    for d in dates:
        for m in MARKETS:
            try:
                raw, meta = S.fetch_day(m, d, key, budget)
            except Exception as e:
                out["failures"].append(f"{m} {d} {type(e).__name__}")
                continue
            rows = [S.normalize(r, d, m, "anchor") for r in raw]
            codes = [r["ticker"] for r in rows]
            lead0 = [c for c in codes if c.startswith("0")]
            tv = [r for r in rows if r["traded_value"] is not None]
            zero_vol = [r for r in rows if r["volume"] == 0]
            zero_tv = [r for r in rows if r["traded_value"] == 0]
            # 직접 거래대금이 close×volume 과 다른가 (proxy 가 아님을 실증)
            diff = 0
            for r in rows[:400]:
                if (r["close"] is not None and r["volume"] and r["traded_value"]):
                    if abs(r["close"] * r["volume"] - r["traded_value"]) > 1:
                        diff += 1
            chk = {
                "market": m, "date": d, "http": meta["http"],
                "rowCount": meta["rowCount"],
                "basDdMatches": all(str(r.get("BAS_DD")) == d.replace("-", "")
                                    for r in raw[:50]),
                "marketFieldConsistent": len({r["market"] for r in rows}) == 1,
                "codeLen6": all(len(c) == 6 for c in codes),
                "leadingZeroCodes": len(lead0),
                "duplicateCodes": len(codes) - len(set(codes)),
                "closeNonNull": sum(1 for r in rows if r["close"] is not None),
                "volumeNonNull": sum(1 for r in rows if r["volume"] is not None),
                "directTradedValueNonNull": len(tv),
                "zeroVolumeRows": len(zero_vol),
                "zeroTradedValueRows": len(zero_tv),
                "tradedValueDiffersFromCloseXVolume": diff,
                "sharesNonNull": sum(1 for r in rows if r["shares"] is not None),
                "marketCapNonNull": sum(1 for r in rows if r["market_cap"] is not None),
            }
            out["checks"].append(chk)
            if chk["rowCount"] == 0:
                out["failures"].append(f"{m} {d} ZERO_ROWS")
            if not chk["codeLen6"]:
                out["failures"].append(f"{m} {d} CODE_LEN")
            if chk["duplicateCodes"]:
                out["failures"].append(f"{m} {d} DUPLICATE_CODE")
            if chk["directTradedValueNonNull"] == 0:
                out["failures"].append(f"{m} {d} NO_DIRECT_TRADED_VALUE")
            if chk["tradedValueDiffersFromCloseXVolume"] == 0:
                out["failures"].append(f"{m} {d} TRADED_VALUE_LOOKS_LIKE_PROXY")

    # 휴장일 응답 형태 — 캘린더에 없는 날 1회(주말)
    try:
        raw, meta = S.fetch_day("KOSPI", "2010-01-02", key, budget)
        out["holidayResponse"] = {"date": "2010-01-02", "http": meta["http"],
                                  "rowCount": meta["rowCount"]}
    except Exception as e:
        out["holidayResponse"] = {"date": "2010-01-02", "error": type(e).__name__}

    # 상장·상폐 경계: 2010 에 있고 최신 PIT 에 없는 종목이 실제로 반환되는가
    try:
        pit = ROOT / "_cache" / "pit-snapshots"
        days = sorted(p.name[:-7] for p in pit.glob("*.csv.gz"))
        live = set()
        if days:
            with gzip.open(pit / f"{days[-1]}.csv.gz", "rt", encoding="utf-8") as fh:
                live = {r["ticker"] for r in csv.DictReader(fh)}
        raw, _ = S.fetch_day("KOSPI", BACKFILL_START, key, budget)
        codes2010 = {str(r.get("ISU_CD") or "").strip() for r in raw}
        gone = codes2010 - live if live else set()
        out["survivorship"] = {
            "codes2010": len(codes2010), "currentlyListed": len(live),
            "returnedButNotCurrentlyListed": len(gone),
            "historicalRowsIndependentOfCurrentListing": len(gone) > 0}
        if live and len(gone) == 0:
            out["failures"].append("SURVIVORSHIP_SUSPECT_NO_DELISTED_ROWS")
    except Exception as e:
        out["survivorship"] = {"error": type(e).__name__}

    key = None
    out["calls"] = budget.calls
    out["scopes"] = {k: (len(v) if isinstance(v, list) else v)
                     for k, v in sc.items()}
    if out["failures"]:
        out["verdict"] = "BLOCKED"
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r31-anchor-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ══════════════════════ bulk ══════════════════════
def collect(kind, max_seconds):
    sc = scopes()
    back, ovl = sc["backfill"], sc["overlap"]
    ck = _sync_from_disk(load_ckpt(), back, ovl)
    budget = Budget(ck.get("callsUsed", 0), ck.get("retries", 0))

    targets = back if kind == "backfill" else ovl
    done = set(ck["backfillDone"] if kind == "backfill" else ck["overlapDone"])
    todo = [d for d in targets if d not in done]

    key = CRED.auth_key()
    started = time.time()
    new_days = 0
    stop = None
    for d in todo:
        if time.time() - started > max_seconds:
            stop = "TIME_SLICE_END"
            break
        if budget.left() < len(MARKETS):
            stop = "BUDGET_EXHAUSTED"
            break
        try:
            rows, metas, dup, ts = fetch_both_markets(d, key, budget)
        except S.RateLimited:
            ck["rateLimitHits"] = ck.get("rateLimitHits", 0) + 1
            stop = "RATE_LIMITED"
            break
        except S.AuthRejected:
            stop = "AUTH_REJECTED"
            break
        except S.ServiceNotApproved:
            stop = "SERVICE_NOT_APPROVED"
            break
        except S.FetchFailed as e:
            ck["failed"][d] = str(e)
            continue
        if not rows:
            ck["failed"][d] = "ZERO_ROWS"
            continue

        if kind == "backfill":
            K.write_day(d, rows, retrieved_at=ts)
            K.project_to_r27(d, rows)          # R27 이 읽는 형식으로 투영
            ck["backfillDone"].append(d)
        else:
            write_overlap_day(d, rows, ts)
            ck["overlapDone"].append(d)

        ck["failed"].pop(d, None)
        new_days += 1
        ck["callsUsed"] = budget.calls
        ck["retries"] = budget.retries
        if new_days % PROGRESS_EVERY == 0:
            save_ckpt(ck)
            print(f"[r31] {kind} {new_days} days · calls={budget.calls}",
                  file=sys.stderr)

    key = None
    ck["callsUsed"] = budget.calls
    ck["retries"] = budget.retries
    ck["aborted"] = stop
    save_ckpt(ck)

    doneset = set(ck["backfillDone"] if kind == "backfill" else ck["overlapDone"])
    return {"kind": kind, "target": len(targets), "done": len(doneset),
            "pending": len([d for d in targets if d not in doneset]),
            "newThisRun": new_days, "calls": budget.calls,
            "retries": budget.retries, "failed": len(ck["failed"]),
            "stop": stop, "elapsedSec": round(time.time() - started, 1)}


def status():
    """읽기 전용이다. 절대 checkpoint 를 쓰지 않는다.

    이유(2026-09-07 실측): 수집이 도는 중에 status 가 checkpoint 를 저장하면
    수집 프로세스의 호출 카운터를 오래된 값으로 덮어쓴다(두 writer 경합).
    완료일 집계는 어차피 디스크가 정본이므로 여기서 저장할 이유가 없다.
    """
    sc = scopes()
    ck = _sync_from_disk(load_ckpt(), sc["backfill"], sc["overlap"])
    return {"backfillTarget": len(sc["backfill"]),
            "backfillDone": len(ck["backfillDone"]),
            "overlapTarget": len(sc["overlap"]),
            "overlapDone": len(ck["overlapDone"]),
            "callsUsed": ck.get("callsUsed", 0), "cap": HARD_DAILY_CAP,
            "failed": len(ck.get("failed") or {}),
            "aborted": ck.get("aborted")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--anchors", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--overlap", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--max-seconds", type=int, default=540)
    a = ap.parse_args()

    if CRED.credential_status()["status"] != "PRESENT":
        print(json.dumps({"verdict": "BLOCKED",
                          "reasonClass": "KRX_AUTH_KEY_DISAPPEARED_FROM_WINDOWS_USER_ENV"},
                         ensure_ascii=False))
        return 1

    if a.anchors:
        r = anchors()
        print(json.dumps({k: r[k] for k in
                          ("verdict", "calls", "failures")}, ensure_ascii=False))
        for c in r["checks"]:
            print(f"  [{c['market']}] {c['date']} rows={c['rowCount']} "
                  f"code6={c['codeLen6']} lead0={c['leadingZeroCodes']} "
                  f"dup={c['duplicateCodes']} directTV={c['directTradedValueNonNull']} "
                  f"tv!=c*v={c['tradedValueDiffersFromCloseXVolume']} "
                  f"zeroVol={c['zeroVolumeRows']}")
        print(f"  holiday: {r.get('holidayResponse')}")
        print(f"  survivorship: {r.get('survivorship')}")
        return 0 if r["verdict"] == "PASS" else 1

    if a.backfill or a.overlap:
        r = collect("backfill" if a.backfill else "overlap", a.max_seconds)
        print(json.dumps(r, ensure_ascii=False))
        return 0

    print(json.dumps(status(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
