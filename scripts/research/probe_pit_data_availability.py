#!/usr/bin/env python3
"""WABABA-KOREA-MAGIC-FORMULA-LONG-HORIZON-MASTER-R1 / PHASE 1 — DATA FEASIBILITY probe.

목적: "장기 point-in-time 백테스트가 실제로 어디까지 가능한가"를 추측이 아니라 실측한다.

검사 항목
---------
1. pykrx 과거 시점 universe : get_market_ticker_list(date) 가 *그 시점* 상장목록을 주는가
                              (= survivorship bias 를 피할 수 있는가)
2. 상장폐지 종목 가격       : 지금은 없는 종목의 과거 OHLCV 를 아직 받을 수 있는가
3. 과거 시가총액            : get_market_cap_by_ticker(date) 가 과거 시점 값을 주는가
4. 과거 fundamental         : get_market_fundamental_by_ticker(date) (PER/PBR/EPS/BPS) 가용 범위
5. 지수 benchmark           : KOSPI(1001)/KOSDAQ(2001) 히스토리 시작점

안전: 전부 read-only 조회. 파일 write 는 --out 으로 지정한 리포트 1개뿐.
      실주문 0 · 브로커 API 0 · canonical 미접근 · 운영 데이터 미변경.

사용: python scripts/research/probe_pit_data_availability.py [--out <json>]
"""
from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import datetime

PROBE_DATES = ["20100104", "20130102", "20160104", "20200102", "20230102", "20260807"]


def _safe(fn, *a, **kw):
    """조회 실패를 예외로 죽이지 않고 (ok, value|error) 로 돌려준다."""
    try:
        return True, fn(*a, **kw)
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def probe_universe(stock):
    """과거 시점 상장 종목 수 — 시점마다 달라야 point-in-time universe 가 가능한 것이다."""
    out = []
    for d in PROBE_DATES:
        row = {"date": d}
        for mkt in ("KOSPI", "KOSDAQ"):
            ok, v = _safe(stock.get_market_ticker_list, d, market=mkt)
            row[mkt] = len(v) if ok and v is not None else f"ERR({v})" if not ok else 0
        out.append(row)
    return out


def probe_delisted(stock, universe_now: set, past_date: str, sample: int = 5):
    """과거 시점에는 있었지만 지금은 없는 종목 = 그 사이 상장폐지/이전.
    그 종목의 과거 가격을 아직 받을 수 있으면 survivorship bias 없는 백테스트가 가능하다."""
    res = {"pastDate": past_date, "gone": 0, "sampleTested": 0, "priceOk": 0, "samples": []}
    past = set()
    for mkt in ("KOSPI", "KOSDAQ"):
        ok, v = _safe(stock.get_market_ticker_list, past_date, market=mkt)
        if ok and v:
            past |= set(v)
    if not past:
        res["error"] = "past universe empty"
        return res
    gone = sorted(past - universe_now)
    res["pastUniverse"] = len(past)
    res["gone"] = len(gone)
    for t in gone[:sample]:
        res["sampleTested"] += 1
        ok, df = _safe(stock.get_market_ohlcv_by_date, past_date, "20991231", t)
        rows = 0 if not ok or df is None else len(df)
        last = None
        if ok and df is not None and len(df):
            try:
                last = str(df.index[-1])[:10]
            except Exception:  # noqa: BLE001
                last = None
        if rows > 0:
            res["priceOk"] += 1
        res["samples"].append({"ticker": t, "rows": rows, "lastDate": last,
                               "error": None if ok else df})
    return res


def probe_marketcap(stock):
    out = []
    for d in PROBE_DATES:
        ok, df = _safe(stock.get_market_cap_by_ticker, d, market="ALL")
        out.append({"date": d, "rows": (0 if not ok or df is None else len(df)),
                    "cols": (None if not ok or df is None else list(df.columns)),
                    "error": None if ok else df})
    return out


def probe_fundamental(stock):
    out = []
    for d in PROBE_DATES:
        ok, df = _safe(stock.get_market_fundamental_by_ticker, d, market="ALL")
        nonzero = None
        if ok and df is not None and len(df):
            try:
                nonzero = int((df.get("PER", 0) > 0).sum())
            except Exception:  # noqa: BLE001
                nonzero = None
        out.append({"date": d, "rows": (0 if not ok or df is None else len(df)),
                    "perPositive": nonzero, "error": None if ok else df})
    return out


def probe_index(stock):
    out = []
    for code, name in (("1001", "KOSPI"), ("2001", "KOSDAQ")):
        ok, df = _safe(stock.get_index_ohlcv_by_date, "19900101", "20260807", code)
        first = last = None
        if ok and df is not None and len(df):
            first, last = str(df.index[0])[:10], str(df.index[-1])[:10]
        out.append({"code": code, "name": name,
                    "rows": (0 if not ok or df is None else len(df)),
                    "first": first, "last": last, "error": None if ok else df})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)

    from pykrx import stock  # noqa: PLC0415  (로그인 부작용을 main 안으로)

    report = {"stage": "PIT_DATA_FEASIBILITY_PROBE", "project": "wababa",
              "taskId": "WABABA-KOREA-MAGIC-FORMULA-LONG-HORIZON-MASTER-R1",
              "createdAt": datetime.now().isoformat(timespec="seconds"),
              "probeDates": PROBE_DATES,
              "realOrderCount": 0, "brokerApiCallCount": 0}

    steps = [
        ("universe", lambda: probe_universe(stock)),
        ("marketcap", lambda: probe_marketcap(stock)),
        ("fundamental", lambda: probe_fundamental(stock)),
        ("index", lambda: probe_index(stock)),
    ]
    for key, fn in steps:
        try:
            report[key] = fn()
        except Exception:  # noqa: BLE001
            report[key] = {"fatal": traceback.format_exc(limit=2)}
        print(f"[probe] {key} done", file=sys.stderr)

    # survivorship 검사는 현재 universe 를 먼저 확보한 뒤에 한다.
    now = set()
    for mkt in ("KOSPI", "KOSDAQ"):
        ok, v = _safe(stock.get_market_ticker_list, PROBE_DATES[-1], market=mkt)
        if ok and v:
            now |= set(v)
    report["universeNowCount"] = len(now)
    report["survivorship"] = []
    for d in ("20160104", "20200102", "20230102"):
        try:
            report["survivorship"].append(probe_delisted(stock, now, d))
        except Exception:  # noqa: BLE001
            report["survivorship"].append({"pastDate": d, "fatal": traceback.format_exc(limit=2)})
        print(f"[probe] survivorship {d} done", file=sys.stderr)

    txt = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(txt)
        print(f"[probe] written {args.out}", file=sys.stderr)
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
