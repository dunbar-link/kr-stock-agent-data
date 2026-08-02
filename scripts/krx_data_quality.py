#!/usr/bin/env python3
"""거래일별 KRX 데이터 품질 판정 기록·조회 (WABABA-KRX-FUNDAMENTAL-RECOVERY-R1).

계약 A: 펀더멘털이 INVALID/BLOCKED 로 판정되면 그 실제 거래일에 대해
  공식 ranking 확정 · Signal 정상완료 · Dry Run 승인가능 · Fund Plan Preview 확정 ·
  Auto Apply · canonical write · sequence 증가 · 신규 lot · FIFO 매도 ·
  public write · commit · push · deploy 를 전부 0 으로 유지한다.

이 모듈은 "그 거래일 데이터 품질이 PASS 였다"는 증거를 남기고, 후속 gate 가 그 증거를
요구하도록 하는 단일 지점이다. 증거가 없으면(=수집이 아예 안 돌았으면) PASS 로 간주하지 않는다.

기록 위치(REPO2 reports/ — .gitignore 대상이라 Git stage 되지 않는다):
  reports/wababa/krx-data-quality-latest.json
  reports/wababa/krx-data-quality-<date>.json
비밀값(로그인 ID/PW/쿠키/세션)은 기록하지 않는다.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUALITY_DIR = ROOT / "reports" / "wababa"
LATEST_JSON = QUALITY_DIR / "krx-data-quality-latest.json"

REQUIRED_MARKETS = ("KOSPI", "KOSDAQ")   # 공식 산출물에는 두 시장 모두 PASS 필요


def path_for(date_iso: str) -> Path:
    return QUALITY_DIR / f"krx-data-quality-{str(date_iso)[:10]}.json"


def build_status(*, date_iso: str, verdict: str, evidence: list | None = None,
                 markets: dict | None = None, reason: str = "",
                 universe_count: int | None = None, now: str | None = None) -> dict:
    return {
        "stage": "KRX_DATA_QUALITY",
        "project": "wababa",
        "date": str(date_iso)[:10],
        "verdict": verdict,                      # PASS | INVALID
        "markets": markets or {},                # {'KOSPI':'PASS','KOSDAQ':'INVALID'}
        "requiredMarkets": list(REQUIRED_MARKETS),
        "universeCount": universe_count,
        "reason": reason,
        "evidence": evidence or [],              # redacted only
        "realOrderCount": 0,
        "brokerApiCallCount": 0,
        "createdAt": now or "",
    }


def write_status(status: dict) -> Path:
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(status, ensure_ascii=False, indent=2)
    LATEST_JSON.write_text(payload, encoding="utf-8")
    dated = path_for(status.get("date") or "")
    if status.get("date"):
        dated.write_text(payload, encoding="utf-8")
    return LATEST_JSON


def read_status(date_iso: str) -> dict | None:
    """해당 거래일의 품질 기록. 날짜본 우선, 없으면 latest 가 그 날짜일 때만 인정."""
    p = path_for(date_iso)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            return None
    if LATEST_JSON.exists():
        try:
            latest = json.loads(LATEST_JSON.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            return None
        if str(latest.get("date")) == str(date_iso)[:10]:
            return latest
    return None


def evaluate(status: dict | None, date_iso: str) -> tuple[bool, str, str]:
    """(ok, code, detail). 증거가 없으면 PASS 로 간주하지 않는다(fail-closed)."""
    if not isinstance(status, dict) or not status:
        return False, "KRX_QUALITY_EVIDENCE_MISSING", f"{date_iso} KRX 데이터 품질 증거 없음"
    if str(status.get("date"))[:10] != str(date_iso)[:10]:
        return False, "KRX_QUALITY_DATE_MISMATCH", \
            f"품질 증거 날짜 {status.get('date')} != 대상 거래일 {date_iso}"
    if str(status.get("verdict")) != "PASS":
        return False, "KRX_DATA_QUALITY_INVALID", \
            f"{date_iso} KRX 데이터 품질 {status.get('verdict')}: {status.get('reason') or ''}"
    markets = status.get("markets") or {}
    bad = [m for m in REQUIRED_MARKETS if str(markets.get(m)) != "PASS"]
    if bad:
        return False, "KRX_MARKET_NOT_PASS", f"{date_iso} 필수 시장 미통과: {bad}"
    return True, "", ""


def gate(date_iso: str) -> tuple[bool, str, str]:
    """후속 단계(Auto Apply·publish)가 호출하는 단일 진입점."""
    return evaluate(read_status(date_iso), date_iso)
