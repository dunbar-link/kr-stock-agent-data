#!/usr/bin/env python3
"""R27 historical 유동성 수집 — 거래량·거래대금 (결정일 직전 20거래일).

WABABA-SIZE-TRADABILITY-VALIDATION-R27

새 crawler·새 framework 를 만들지 않는다(§3). 이 repo 의 PIT 스냅샷 빌더가 이미
쓰는 `pykrx stock.get_market_cap_by_ticker(ymd, market='ALL')` 를 그대로 쓴다.
한 콜로 전 종목의 종가·시가총액·**거래량·거래대금**·상장주식수를 준다.

수집 범위는 연구에 실제로 필요한 날만이다 — 결정일(월별 PIT 스냅샷) **직전**
20거래일의 합집합. 결정일 당일과 그 이후는 look-ahead 라 쓰지 않는다(§8).

resumable: 이미 받은 날은 건너뛴다. 중단해도 다시 이어받는다.

안전: 무료 공개 KRX · 기존 세션 재사용 · 신규 key 0 · 캐시만 write.
"""
from __future__ import annotations

import csv
import gzip
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "krx-liquidity"
CAL_PATH = CACHE / "_trading-calendar.json"
LOOKBACK = 20
SLEEP = 0.05
COLS = ["ticker", "close", "marketCap", "volume", "tradedValue", "shares"]


def trading_calendar():
    """KOSPI 지수 시계열 = 실제 거래일 캘린더. 평일 추정 금지."""
    if CAL_PATH.exists():
        return json.loads(CAL_PATH.read_text(encoding="utf-8"))["days"]
    from pykrx import stock
    idx = stock.get_index_ohlcv("20061101", "20260803", "1001")
    days = [d.strftime("%Y-%m-%d") for d in idx.index]
    CACHE.mkdir(parents=True, exist_ok=True)
    CAL_PATH.write_text(json.dumps({"source": "pykrx KOSPI(1001) OHLCV index",
                                    "days": days}, ensure_ascii=False),
                        encoding="utf-8")
    return days


def needed_days(cal, decision_dates):
    """결정일 **직전** 20거래일의 합집합. 결정일 당일·이후 제외(look-ahead 0)."""
    pos = {d: i for i, d in enumerate(cal)}
    need = set()
    for d in decision_dates:
        i = pos.get(d)
        if i is None:
            prev = [k for k, x in enumerate(cal) if x <= d]
            if not prev:
                continue
            i = prev[-1] + 1
        need.update(cal[max(0, i - LOOKBACK):i])
    return sorted(need)


# ★ 자체수정 1 (§41): 초판은 단발 호출이었다. KRX 세션이 한 번 끊기자
#   이후 전 요청이 KeyError 로 실패했는데(실측 new=52 고정, fail 548 누적)
#   재시도도 세션 복구도 없어 목록만 소진했다. 이 repo 에 이미 있는
#   krx_fetch_guard(재시도·백오프 규약)를 쓰고, 연속 실패가 길어지면 세션을
#   새로 만든 뒤 그래도 안 되면 **멈춘다**(무의미한 소진 금지).
MAX_RETRY = 3
RETRY_BACKOFF = (1.0, 3.0, 8.0)
CONSECUTIVE_FAIL_ABORT = 25


def _reset_krx_session():
    """pykrx 세션을 새로 만든다(만료·차단 복구)."""
    try:
        from pykrx.website.comm import webio
        from pykrx.website.comm.auth import build_krx_session
        webio._session = build_krx_session()
        return True
    except Exception:  # noqa: BLE001
        return False


def fetch_day(ymd_iso):
    """하루치 전 종목 유동성. (rows, status). 재시도·세션복구 포함."""
    # ★ 자체수정 2 (§41): pykrx 는 **import 시점에** KRX 세션을 만든다. 소스가
    #   막히면 import 자체가 예외를 던져 수집기가 죽고, 그러면 inventory 조차
    #   기록되지 않아 "얼마나 못 받았는지"를 잃는다. import 를 감싸서 소스
    #   불가도 하나의 status 로 기록한다.
    try:
        from pykrx import stock
    except Exception as e:  # noqa: BLE001
        return None, f"SOURCE_UNAVAILABLE:{type(e).__name__}"
    ymd = ymd_iso.replace("-", "")
    last = "UNKNOWN"
    for attempt in range(MAX_RETRY):
        try:
            df = stock.get_market_cap_by_ticker(ymd, market="ALL")
            break
        except Exception as e:  # noqa: BLE001
            last = f"FETCH_FAIL:{type(e).__name__}"
            if attempt == MAX_RETRY - 1:
                return None, last
            time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
            _reset_krx_session()
    else:
        return None, last
    if df is None or len(df) == 0:
        return None, "EMPTY"
    need = ("종가", "시가총액", "거래량", "거래대금", "상장주식수")
    if not all(c in df.columns for c in need):
        return None, f"SCHEMA:{list(df.columns)}"
    rows = []
    for t, r in df.iterrows():
        rows.append({"ticker": str(t), "close": r["종가"],
                     "marketCap": r["시가총액"], "volume": r["거래량"],
                     "tradedValue": r["거래대금"], "shares": r["상장주식수"]})
    return rows, "OK"


def write_day(ymd_iso, rows):
    CACHE.mkdir(parents=True, exist_ok=True)
    p = CACHE / f"{ymd_iso}.csv.gz"
    tmp = p.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(p)


def load_day(ymd_iso):
    p = CACHE / f"{ymd_iso}.csv.gz"
    if not p.exists():
        return None
    out = {}
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                out[r["ticker"]] = {
                    "close": float(r["close"]), "marketCap": float(r["marketCap"]),
                    "volume": float(r["volume"]),
                    "tradedValue": float(r["tradedValue"]),
                    "shares": float(r["shares"])}
            except (TypeError, ValueError):
                continue
    return out


def main() -> int:
    cal = trading_calendar()
    ds = contiguous_span(sorted(C.load_capital_series()))
    need = needed_days(cal, ds)
    have = {p.name[:-7] for p in CACHE.glob("*.csv.gz")}
    todo = [d for d in need if d not in have]
    print(f"[r27] 거래일 캘린더 {len(cal)} · 결정일 {len(ds)} · "
          f"필요 {len(need)} · 보유 {len(need) - len(todo)} · 수집 {len(todo)}",
          file=sys.stderr)

    new = fail = empty = 0
    fails = []
    streak = 0
    aborted = None
    for i, d in enumerate(todo, 1):
        rows, st = fetch_day(d)
        if st == "OK":
            write_day(d, rows)
            new += 1
            streak = 0
        elif st == "EMPTY":
            empty += 1
            streak = 0
            fails.append({"date": d, "status": st})
        else:
            fail += 1
            streak += 1
            fails.append({"date": d, "status": st})
            print(f"[r27] {d} {st} (streak {streak})", file=sys.stderr)
            # 연속 실패가 길면 소스가 막힌 것이다. 목록을 태우지 않고 멈춘다.
            if streak >= CONSECUTIVE_FAIL_ABORT:
                aborted = f"CONSECUTIVE_FAILURES_{streak}_AT_{d}"
                print(f"[r27] ABORT {aborted}", file=sys.stderr)
                break
        if i % 200 == 0:
            print(f"[r27] {i}/{len(todo)} new={new} fail={fail}", file=sys.stderr)
        time.sleep(SLEEP)

    have2 = {p.name[:-7] for p in CACHE.glob("*.csv.gz")}
    out = {"task": "R27",
           "source": "pykrx stock.get_market_cap_by_ticker(ymd, market='ALL')",
           "sourceReuse": ("PIT 스냅샷 빌더가 이미 쓰는 함수. 새 crawler 0(§3)."),
           "columns": ["거래량", "거래대금", "종가", "시가총액", "상장주식수"],
           "lookbackTradingDays": LOOKBACK,
           "lookAheadRule": "결정일 직전 20거래일만. 결정일 당일·이후 미사용(§8).",
           "tradingCalendarDays": len(cal),
           "decisionDates": len(ds),
           "daysRequired": len(need),
           "daysCollectedTotal": len([d for d in need if d in have2]),
           "daysNewThisRun": new, "daysEmpty": empty, "daysFailed": fail,
           "coveragePct": round(100.0 * len([d for d in need if d in have2])
                                / len(need), 3) if need else 0.0,
           "failures": fails[:50],
           "aborted": aborted,
           "retryPolicy": {"maxRetry": MAX_RETRY, "backoffSec": list(RETRY_BACKOFF),
                           "sessionResetOnFail": True,
                           "consecutiveFailAbort": CONSECUTIVE_FAIL_ABORT},
           "cacheDir": str(CACHE), "paidData": 0, "newCredential": 0}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r27-data-inventory-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: out[k] for k in
                      ("daysRequired", "daysCollectedTotal", "daysNewThisRun",
                       "daysFailed", "coveragePct")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
