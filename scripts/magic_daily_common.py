#!/usr/bin/env python3
"""마법공식 A티어 일일 자동화 공통 헬퍼 (Phase 45-AUTO2).

read-only / TEMP-only 자동화 스크립트(signal/dry-run/status)가 공유하는 경로·거래일·리포트·BLOCKED
표준 구조. canonical/public/REPO1 write 0. import 전용(직접 실행 안 함).
"""
from __future__ import annotations

import json
import os
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "magic-formula-official-state.json"
TEMP_ROOT = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-signal"))
REPORTS_DIR = TEMP_ROOT / "reports"
LOGS_DIR = TEMP_ROOT / "logs"
KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(KST)


def _krx_holidays() -> frozenset:
    try:
        import build_magic_signal_package as B
        return B.KRX_HOLIDAYS
    except Exception:  # noqa: BLE001
        return frozenset()


def is_krx_trading_day(date_iso: str) -> bool:
    """평일 + KRX 휴장표 제외(큐레이션 캘린더, 네트워크 0). 평일 추정만으로 거래 생성하지 않음(판정용)."""
    try:
        d = _date.fromisoformat(str(date_iso)[:10])
    except ValueError:
        return False
    return d.weekday() < 5 and str(date_iso)[:10] not in _krx_holidays()


def today_kst_iso() -> str:
    return now_kst().date().isoformat()


def market_close_dt(date_iso: str) -> datetime:
    y, m, d = (int(x) for x in str(date_iso)[:10].split("-"))
    return datetime(y, m, d, 15, 30, tzinfo=KST)


def market_open_dt(date_iso: str) -> datetime:
    y, m, d = (int(x) for x in str(date_iso)[:10].split("-"))
    return datetime(y, m, d, 9, 0, tzinfo=KST)


def read_log_text(path) -> str:
    raw = Path(path).read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    for enc in ("utf-8-sig", "utf-8", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def write_json_report(path, obj) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return str(p)


def blocked_report(phase: str, code: str, date_iso: str, reason: str, *,
                   execution_date: str | None = None, signal_as_of: str | None = None,
                   evidence=None, recommended_fix: str | None = None, now: str | None = None) -> dict:
    """A티어 공통 BLOCKED 구조. 항상 가짜거래·write 0임을 명시."""
    return {
        "status": "BLOCKED", "phase": phase, "blockedCode": code, "date": date_iso,
        "signalAsOfDate": signal_as_of, "executionDate": execution_date,
        "autoStopped": True, "noFakeTrade": True,
        "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
        "reason": reason, "evidence": evidence or [],
        "recommendedManualFix": recommended_fix,
        "createdAt": now or now_kst().isoformat(),
    }


def load_canonical_summary() -> dict:
    """canonical read-only 요약(없으면 {})."""
    try:
        c = json.loads(CANONICAL_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    import hashlib
    return {
        "officialSequence": c.get("officialSequence"),
        "officialTradingDayIndex": c.get("officialTradingDayIndex"),
        "batchCount": len(c.get("batches") or []),
        "buyCount": len(c.get("buyLedger") or []),
        "sellCount": len(c.get("sellLedger") or []),
        "missedRunCount": len(c.get("missedRuns") or []),
        "officialAvailableCash": c.get("officialAvailableCash"),
        "officialExecutionCalendar": c.get("officialExecutionCalendar"),
        "officialKrxTradingCalendar": c.get("officialKrxTradingCalendar"),
        "canonicalSha256": hashlib.sha256(CANONICAL_PATH.read_bytes()).hexdigest()[:16]
                           if CANONICAL_PATH.exists() else None,
    }


def latest_report(prefix: str):
    """REPORTS_DIR에서 prefix-*.json 중 최신 1개(없으면 None)."""
    if not REPORTS_DIR.exists():
        return None
    cands = sorted(REPORTS_DIR.glob(f"{prefix}-*.json"), key=lambda p: p.stat().st_mtime)
    if not cands:
        return None
    try:
        return {"path": str(cands[-1]), "data": json.loads(cands[-1].read_text(encoding="utf-8"))}
    except (OSError, ValueError):
        return {"path": str(cands[-1]), "data": None}
