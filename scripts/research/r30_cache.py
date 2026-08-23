#!/usr/bin/env python3
"""R30 공식 유동성 캐시 — 스키마 · checkpoint · R27 투영.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

두 개의 캐시를 명확히 분리한다.

  _cache/official-liquidity/<YYYY-MM-DD>.csv.gz
      R30 canonical. §14 전체 스키마(source·source_version·retrieved_at·
      quality_flag 포함). provenance 를 잃지 않는다.

  _cache/krx-liquidity/<YYYY-MM-DD>.csv.gz
      R27 이 이미 읽는 형식. **R27 코드를 한 줄도 바꾸지 않고** 재실행하기
      위한 투영본이다. R27 이 남긴 기존 111일은 건드리지 않는다(§12).

checkpoint 는 _cache/official-liquidity/_checkpoint.json 하나다(§15).
중단되면 다음 실행이 pending 만 이어받는다. 처음부터 다시 받지 않는다.

안전: 캐시만 write. production 원장·운영 파일 write 0. 네트워크 없음(이 모듈).
"""
from __future__ import annotations

import csv
import gzip
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "_cache" / "official-liquidity"
R27_CACHE = ROOT / "_cache" / "krx-liquidity"
CKPT = CACHE / "_checkpoint.json"

# §14 canonical 스키마 — missing 과 0 을 구분한다(빈칸=missing, "0"=관측된 0)
COLS = ["date", "ticker", "name", "market", "close", "open", "high", "low",
        "volume", "traded_value", "market_cap", "shares",
        "source", "source_version", "retrieved_at", "quality_flag"]

# R27 이 읽는 형식(r27_collect.COLS 와 동일해야 한다)
R27_COLS = ["ticker", "close", "marketCap", "volume", "tradedValue", "shares"]


def _atomic_write(path, write_fn):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    os.close(fd)
    tmp = Path(tmp)
    try:
        write_fn(tmp)
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def write_day(iso_date, rows, retrieved_at=None):
    """R30 canonical 하루치 저장. 원자적 교체 — 중단돼도 반쪽 파일이 남지 않는다."""
    ts = retrieved_at or datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _w(tmp):
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=COLS, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                r = dict(r)
                r["retrieved_at"] = ts
                for k in COLS:
                    r.setdefault(k, None)
                    if r[k] is None:
                        r[k] = ""          # 빈칸 = missing. 0 과 구분(§14)
                w.writerow(r)

    _atomic_write(CACHE / f"{iso_date}.csv.gz", _w)


def load_day(iso_date):
    """{ticker: row}. 빈칸은 None 으로 되살린다 — 0 으로 만들지 않는다."""
    p = CACHE / f"{iso_date}.csv.gz"
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


def have_days():
    if not CACHE.exists():
        return set()
    return {p.name[:-7] for p in CACHE.glob("*.csv.gz")
            if not p.name.startswith("_")}


def r27_have_days():
    if not R27_CACHE.exists():
        return set()
    return {p.name[:-7] for p in R27_CACHE.glob("*.csv.gz")
            if not p.name.startswith("_")}


def project_to_r27(iso_date, rows, overwrite=False):
    """R27 이 읽는 형식으로 투영. 기존 R27 파일은 기본적으로 보존한다(§12).

    반환: "WROTE" | "KEPT_EXISTING" | "NO_USABLE_ROWS"
    """
    p = R27_CACHE / f"{iso_date}.csv.gz"
    if p.exists() and not overwrite:
        return "KEPT_EXISTING"
    usable = [r for r in rows
              if r.get("volume") is not None and r.get("traded_value") is not None
              and r.get("close") is not None]
    if not usable:
        return "NO_USABLE_ROWS"

    def _w(tmp):
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=R27_COLS)
            w.writeheader()
            for r in usable:
                w.writerow({
                    "ticker": r["ticker"],
                    "close": r["close"],
                    # 시가총액이 없으면 종가×상장주식수. 둘 다 없으면 빈칸.
                    "marketCap": (r.get("market_cap")
                                  if r.get("market_cap") is not None
                                  else (r["close"] * r["shares"]
                                        if r.get("shares") is not None else "")),
                    "volume": r["volume"],
                    "tradedValue": r["traded_value"],
                    "shares": (r.get("shares")
                               if r.get("shares") is not None else ""),
                })

    _atomic_write(p, _w)
    return "WROTE"


# ══════════════════════ checkpoint (§15) ══════════════════════
def new_checkpoint(required_days):
    return {"task": "R30",
            "source": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE",
            "required_days": len(required_days),
            "completed_days": [],
            "pending_days": list(required_days),
            "failed_days": {},
            "retry_count": 0,
            "last_success": None,
            "last_error_class": None,
            "updated_at": None}


def load_checkpoint(required_days=None):
    if CKPT.exists():
        try:
            ck = json.loads(CKPT.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            ck = None
        if ck and required_days is not None:
            # 필요일 목록이 바뀌었으면 완료분은 유지하고 pending 만 재계산한다.
            done = set(ck.get("completed_days") or [])
            ck["required_days"] = len(required_days)
            ck["pending_days"] = [d for d in required_days if d not in done]
        if ck:
            return ck
    return new_checkpoint(required_days or [])


def save_checkpoint(ck):
    ck["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _w(tmp):
        tmp.write_text(json.dumps(ck, ensure_ascii=False, indent=2),
                       encoding="utf-8")

    _atomic_write(CKPT, _w)


def sync_checkpoint_from_disk(ck, required_days):
    """실제 캐시 파일을 정본으로 checkpoint 를 맞춘다(수동 삭제·중단 복구)."""
    have = have_days()
    done = [d for d in required_days if d in have]
    ck["completed_days"] = done
    ck["pending_days"] = [d for d in required_days if d not in have]
    ck["required_days"] = len(required_days)
    return ck
