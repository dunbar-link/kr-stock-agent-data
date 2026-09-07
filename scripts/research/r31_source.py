#!/usr/bin/env python3
"""R31 KRX OPEN API 클라이언트 — 일별매매정보(유가증권·코스닥).

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

── 새로 만들지 않은 것 ────────────────────────────────────────────
  저장 스키마·원자적 write·R27 투영·checkpoint 는 **R30 것을 그대로 쓴다**
  (`r30_cache.py`). 이 모듈은 KRX 응답을 그 스키마로 정규화하는 어댑터일 뿐이다.
  새 data framework·새 캐시 레이아웃·새 R27 리더를 만들지 않는다.

── endpoint (2026-09-07 실호출 검증 완료) ─────────────────────────
  유가증권  /svc/apis/sto/stk_bydd_trd   HTTP 200 · 2010-01-04 925행
  코스닥    /svc/apis/sto/ksq_bydd_trd   HTTP 200 · 2010-01-04 1,036행
  둘 다 basDd=YYYYMMDD · 응답 배열 키 OutBlock_1.
  R31 이전에는 코스닥이 DOCUMENTED_NOT_VERIFIED 였고, 이제 실측으로 승격됐다.

── 필드 (실측) ────────────────────────────────────────────────────
  BAS_DD ISU_CD ISU_NM MKT_NM SECT_TP_NM TDD_CLSPRC CMPPREVDD_PRC FLUC_RT
  TDD_OPNPRC TDD_HGPRC TDD_LWPRC ACC_TRDVOL ACC_TRDVAL MKTCAP LIST_SHRS

  ISU_CD 는 6자리 단축코드이고 leading zero 가 보존된다(실측 '004560').
  ACC_TRDVAL 은 **직접 거래대금**이다 — close×volume 과 다르다
  (실측 8910×138442=1,233,518,220 vs ACC_TRDVAL 1,214,497,520).
  그래서 proxy 를 쓰지 않는다(R27 계약).

── 비밀값 ─────────────────────────────────────────────────────────
  AUTH_KEY 는 Request Header 로만. 값·전체 헤더·전체 URL 을 출력하지 않는다.
  예외 메시지에도 키를 넣지 않는다.

안전: 공식 API · 승인된 2개 서비스만 · 웹 스크래핑 0 · pykrx 0 · 캐시만 write.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

SOURCE = "KRX_OFFICIAL_OPEN_API"
SOURCE_VERSION = "krx/sto/bydd_trd/v1"
BASE = "https://data-dbg.krx.co.kr/svc/apis/sto"
ENDPOINTS = {
    "KOSPI": f"{BASE}/stk_bydd_trd",
    "KOSDAQ": f"{BASE}/ksq_bydd_trd",
}
ROWS_KEY = "OutBlock_1"
DATE_PARAM = "basDd"

TIMEOUT = 30
MIN_INTERVAL_SEC = 0.5      # §6 최대 2 requests/second
MAX_RETRY_5XX = 3
RETRY_BACKOFF = (1.0, 3.0, 8.0)

_last_call_at = [0.0]


class AuthRejected(RuntimeError):
    """키 자체가 거부됐다. 재시도하지 않는다(fail-closed)."""


class ServiceNotApproved(RuntimeError):
    """키는 인식되나 이 API 활용승인이 없다. 재시도하지 않는다."""


class RateLimited(RuntimeError):
    """429. 추가 호출을 멈춘다."""


class FetchFailed(RuntimeError):
    """그 밖의 실패. 호출자가 날짜 단위로 기록한다."""


def _throttle():
    gap = time.time() - _last_call_at[0]
    if gap < MIN_INTERVAL_SEC:
        time.sleep(MIN_INTERVAL_SEC - gap)
    _last_call_at[0] = time.time()


def _to_num(v):
    """빈칸·'-'·콤마를 0 으로 만들지 않는다. 결측은 None 이다(R27 계약)."""
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize(raw_row, iso_date, market, retrieved_at):
    """KRX row -> R30 canonical 스키마(r30_cache.COLS). 값을 추정하지 않는다."""
    code = str(raw_row.get("ISU_CD") or "").strip()
    return {
        "date": iso_date,
        "ticker": code,                      # 6자리, leading zero 보존
        "name": str(raw_row.get("ISU_NM") or "").strip(),
        "market": str(raw_row.get("MKT_NM") or market).strip(),
        "close": _to_num(raw_row.get("TDD_CLSPRC")),
        "open": _to_num(raw_row.get("TDD_OPNPRC")),
        "high": _to_num(raw_row.get("TDD_HGPRC")),
        "low": _to_num(raw_row.get("TDD_LWPRC")),
        "volume": _to_num(raw_row.get("ACC_TRDVOL")),
        "traded_value": _to_num(raw_row.get("ACC_TRDVAL")),   # 직접 거래대금
        "market_cap": _to_num(raw_row.get("MKTCAP")),
        "shares": _to_num(raw_row.get("LIST_SHRS")),
        "source": SOURCE,
        "source_version": SOURCE_VERSION,
        "retrieved_at": retrieved_at,
        "quality_flag": "OK",
    }


def fetch_day(market, iso_date, key, budget=None):
    """하루·한 시장. 성공하면 (rows, meta). 실패는 예외로 올린다.

    5xx 만 bounded retry 한다. auth/승인/429 는 재시도하지 않는다(§6).
    """
    ep = ENDPOINTS[market]
    ymd = iso_date.replace("-", "")
    last_err = None
    for attempt in range(MAX_RETRY_5XX):
        _throttle()
        if budget is not None:
            budget.spend(retry=(attempt > 0))
        try:
            r = requests.get(ep, headers={"AUTH_KEY": key},
                             params={DATE_PARAM: ymd}, timeout=TIMEOUT)
        except requests.RequestException as e:
            last_err = f"NETWORK_{type(e).__name__}"
            if attempt < MAX_RETRY_5XX - 1:
                time.sleep(RETRY_BACKOFF[attempt])
                continue
            raise FetchFailed(last_err)

        if r.status_code == 429:
            raise RateLimited("HTTP_429")
        if r.status_code in (401, 403):
            b = (r.text or "").lower()
            if "unauthorized key" in b:
                raise AuthRejected("UNAUTHORIZED_KEY")
            raise ServiceNotApproved("UNAUTHORIZED_API_CALL")
        if 500 <= r.status_code < 600:
            last_err = f"HTTP_{r.status_code}"
            if attempt < MAX_RETRY_5XX - 1:
                time.sleep(RETRY_BACKOFF[attempt])
                continue
            raise FetchFailed(last_err)
        if r.status_code != 200:
            raise FetchFailed(f"HTTP_{r.status_code}")

        try:
            payload = r.json()
        except ValueError:
            raise FetchFailed("MALFORMED_JSON")
        rows = payload.get(ROWS_KEY)
        if rows is None:
            raise FetchFailed("ROWS_KEY_MISSING")

        import hashlib
        body = r.content or b""
        meta = {
            "market": market, "date": iso_date, "http": r.status_code,
            "rowCount": len(rows), "retryCount": attempt,
            "responseSha256": hashlib.sha256(body).hexdigest(),
            "responseBytes": len(body),
            "source": SOURCE, "sourceVersion": SOURCE_VERSION,
            "endpointId": ep.rsplit("/", 1)[-1],
        }
        return rows, meta
    raise FetchFailed(last_err or "UNKNOWN")
