#!/usr/bin/env python3
"""감사 격리(quarantine) — 무결성 감사 중 신규 sequence 누적을 fail-closed 로 차단.

WABABA-KRX-SEQ20-22-INTEGRITY-AUDIT-QUARANTINE-R1.

배경: 2026-07-29 KRX 수집 실패 당시 fail-open 결함으로 품질 증거 없이 seq20·21·22 가
누적됐다. 그 무결성이 검증되기 전에 seq23 이상이 그 위에 쌓이면 오염 범위가 커진다.

이 모듈은 활성 quarantine marker 가 있으면 Auto Apply·Auto Publish 가 진입 자체를
못 하게 하는 단일 지점이다. 예약작업을 삭제하거나 Disabled 로 바꾸지 않는다 —
기존 게이트 경로를 재사용해 write 0 을 보장한다(Signal 등 read-only 실행은 영향 없음).

marker: reports/wababa/audit-quarantine.json  (reports/ 는 .gitignore 대상 = 운영 상태)
해제는 감사가 완전 일치를 증명했을 때만. 자동 만료 없음(시간 경과로 풀리지 않는다).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER_PATH = ROOT / "reports" / "wababa" / "audit-quarantine.json"

DEFAULT_CODE = "BLOCKED_KRX_SEQ20_22_AUDIT_QUARANTINE"


def build_marker(*, code: str = DEFAULT_CODE, reason: str = "",
                 scope: list | None = None, task_id: str = "",
                 blocked_sequences_from: int | None = None, now: str = "") -> dict:
    return {
        "stage": "AUDIT_QUARANTINE",
        "project": "wababa",
        "active": True,
        "code": code,
        "taskId": task_id,
        "reason": reason,
        "scope": scope or ["AUTO_APPLY", "AUTO_PUBLISH", "CANONICAL_WRITE", "PUBLIC_WRITE"],
        "blockedSequencesFrom": blocked_sequences_from,
        "readOnlyAllowed": ["SIGNAL", "DRY_RUN", "STATUS", "REPORTING"],
        "autoReleaseAllowed": False,
        "releaseCondition": "seq20·21·22 결정론적 재계산이 완전 일치로 검증된 경우에만 해제",
        "realOrderCount": 0,
        "brokerApiCallCount": 0,
        "createdAt": now,
    }


def activate(marker: dict) -> Path:
    MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKER_PATH.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    return MARKER_PATH


def read_marker() -> dict | None:
    if not MARKER_PATH.exists():
        return None
    try:
        return json.loads(MARKER_PATH.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        # 읽을 수 없는 marker 는 "격리 없음"이 아니라 격리로 본다(fail-closed).
        return {"active": True, "code": DEFAULT_CODE,
                "reason": "quarantine marker 판독 불가 — 안전을 위해 격리 유지"}


def release(*, released_by: str = "", evidence: str = "", now: str = "") -> Path | None:
    """해제는 감사 통과 증명이 있을 때만. marker 를 지우지 않고 active=False 로 이력을 남긴다."""
    cur = read_marker()
    if cur is None:
        return None
    cur.update({"active": False, "releasedBy": released_by,
                "releaseEvidence": evidence, "releasedAt": now})
    MARKER_PATH.write_text(json.dumps(cur, ensure_ascii=False, indent=2), encoding="utf-8")
    return MARKER_PATH


def gate() -> tuple[bool, str, str]:
    """(ok, code, detail). ok=False 면 신규 누적·publish 금지."""
    m = read_marker()
    if not isinstance(m, dict) or not m:
        return True, "", ""
    if m.get("active") is not True:
        return True, "", ""
    code = str(m.get("code") or DEFAULT_CODE)
    reason = str(m.get("reason") or "무결성 감사 중")
    return False, code, f"감사 격리 활성: {reason}"
