#!/usr/bin/env python3
"""PIT 수집 안전장치 — 차단 감지 · checkpoint/resume · pacing · backoff.

WABABA-PIT-DATA-ACQUISITION-RESUME-R2

왜 필요한가
-----------
2026-08-08 실사고: 0.15초 간격으로 약 250콜을 몰아쳐 **KRX 가 IP 차단**을 걸었다
(data.krx.co.kr 응답이 JSON 대신 `.ip-block-page` HTML 로 바뀌고 pykrx import 자체가 실패).
그 사이 와바바 일일 파이프라인의 KRX 조회도 함께 막힌다.

이 모듈은 그 재발을 구조적으로 막는다.
  1. **선차단검사(preflight)** — 대량 요청 *전에* read-only 1콜로 차단 여부를 먼저 본다.
  2. **차단 시 즉시 중단** — 차단 상태에서 반복 probe/수집 루프를 돌리지 않는다.
  3. **checkpoint/resume** — 이미 받은 월은 다시 요청하지 않는다(원자적 저장).
  4. **pacing + jitter** — 고정간격은 봇 패턴으로 보인다. 무작위 지터를 섞어 완만하게 보낸다.
  5. **bounded retry + exponential backoff** — 무한 재시도 금지. 상한 도달 시 정지.
  6. **보수적 batch** — 실제 endpoint 콜 수로 batch 크기를 계산한다(고정 60개월 가정 금지).

금지(이 모듈이 하지 않는 것): 프록시 · VPN · 헤더 위장 · User-Agent 회전 · 계정 변경 · 차단 우회.
차단은 우회 대상이 아니라 **정지 신호**다.

전부 순수 함수 + 주입 가능한 fetch 라서 네트워크 없이 테스트된다.
"""
from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path

BLOCK_PROBE_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
BLOCK_MARKERS = ("ip-block-page", "ip-block-login-header", "ipBlockPage")

# 월 1건을 만들 때 실제로 나가는 endpoint 호출 수 (build_pit_snapshots.build_snapshot 기준)
#   ① get_market_cap_by_ticker(ALL)      1
#   ② get_market_ticker_list(KOSPI)      1
#   ③ get_market_ticker_list(KOSDAQ)     1
#   ④ get_market_fundamental_by_ticker(ALL) 1
CALLS_PER_MONTH = 4
# 연 1회 종목명 갱신(get_market_price_change_by_ticker ALL) 1콜
CALLS_PER_YEAR_EXTRA = 1

# 차단을 유발한 실측 속도: 약 250콜 / 4분 ≈ 60콜/분.
# 그 1/10 이하로 낮춘다. 안전 목표 = 분당 6콜 이하.
SAFE_CALLS_PER_MIN = 6.0


def block_status(fetch, url: str = BLOCK_PROBE_URL) -> dict:
    """read-only 1콜로 차단 여부 판정. fetch(url) -> (status:int, text:str).

    반환 state: 'CLEAR' | 'BLOCKED' | 'UNKNOWN'
    판정 불가(네트워크 오류 등)를 CLEAR 로 낙관하지 않는다(fail-closed).
    """
    try:
        status, text = fetch(url)
    except Exception as e:  # noqa: BLE001
        return {"state": "UNKNOWN", "reason": f"{type(e).__name__}: {e}",
                "httpStatus": None, "blocked": None}
    body = text or ""
    hit = next((m for m in BLOCK_MARKERS if m in body), None)
    if hit:
        return {"state": "BLOCKED", "reason": f"block marker '{hit}' in response",
                "httpStatus": status, "blocked": True, "bytes": len(body)}
    if status != 200:
        return {"state": "UNKNOWN", "reason": f"http {status}", "httpStatus": status,
                "blocked": None, "bytes": len(body)}
    return {"state": "CLEAR", "reason": "no block marker", "httpStatus": status,
            "blocked": False, "bytes": len(body)}


def default_fetch(url: str, timeout: int = 20):
    """실제 조회용 기본 fetch. 헤더 위장 없음 — 평범한 User-Agent 하나만 쓴다."""
    import requests  # noqa: PLC0415
    r = requests.get(url, timeout=timeout,
                     headers={"User-Agent": "wababa-research-pit/1.0 (+read-only)"})
    return r.status_code, r.content.decode("utf-8", errors="replace")


def recommended_batch_months(minutes_budget: float = 20.0,
                             calls_per_min: float = SAFE_CALLS_PER_MIN) -> int:
    """'60개월이 안전하다'는 가정을 쓰지 않는다 — 실제 콜 수로 역산한다.

    분당 안전 콜수 × 예산(분) ÷ 월당 콜수 = 한 번에 받을 개월 수.
    기본값(20분 예산, 분당 6콜, 월 4콜) → 30개월.
    """
    budget_calls = max(1.0, minutes_budget * calls_per_min)
    return max(1, int(budget_calls // CALLS_PER_MONTH))


def pacing_seconds(calls_per_min: float = SAFE_CALLS_PER_MIN) -> float:
    """월 1건(=CALLS_PER_MONTH 콜) 처리 후 쉬어야 할 시간."""
    return (60.0 / max(0.1, calls_per_min)) * CALLS_PER_MONTH


def sleep_with_jitter(base: float, jitter_ratio: float = 0.35, sleeper=time.sleep,
                      rand=random.random) -> float:
    """고정 간격은 봇 패턴이다. ±jitter_ratio 범위로 흔든다. 실제 대기시간을 반환."""
    if base <= 0:
        return 0.0
    lo = base * (1.0 - jitter_ratio)
    hi = base * (1.0 + jitter_ratio)
    wait = lo + (hi - lo) * rand()
    sleeper(wait)
    return wait


def backoff_seconds(attempt: int, base: float = 30.0, cap: float = 900.0,
                    rand=random.random) -> float:
    """지수 백오프(+지터). attempt 는 1부터. 상한 cap 으로 제한한다."""
    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    return raw * (0.7 + 0.6 * rand())


class Checkpoint:
    """수집 진행상태를 원자적으로 저장한다(부분 데이터 재사용 · 동일 데이터 재호출 방지).

    스냅샷 파일 자체가 1차 진실이고, 이 체크포인트는 '시도했지만 비어 있던 달'과
    운영 메타(마지막 실행·누적 콜수·차단 시각)를 기억하는 2차 기록이다.
    파일이 손상돼도 스냅샷만 있으면 수집은 정확히 이어진다(fail-safe).
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.data = {"schema": "WABABA_PIT_CHECKPOINT_V1", "done": [], "empty": [],
                     "failed": {}, "totalCalls": 0, "runs": 0,
                     "lastRunAt": None, "lastBlockedAt": None, "lastState": None}
        self.load()

    def load(self):
        try:
            d = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(d, dict) and d.get("schema") == "WABABA_PIT_CHECKPOINT_V1":
                self.data.update(d)
        except (OSError, ValueError):
            pass
        return self.data

    def save(self):
        """tmp → os.replace 원자적 교체. 중간에 죽어도 이전 상태가 깨지지 않는다."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    def mark_done(self, iso, calls=CALLS_PER_MONTH):
        if iso not in self.data["done"]:
            self.data["done"].append(iso)
        self.data["failed"].pop(iso, None)
        self.data["totalCalls"] += calls

    def mark_empty(self, iso):
        if iso not in self.data["empty"]:
            self.data["empty"].append(iso)

    def mark_failed(self, iso, reason):
        self.data["failed"][iso] = str(reason)[:200]

    def mark_blocked(self, when=None):
        self.data["lastBlockedAt"] = when or time.strftime("%Y-%m-%dT%H:%M:%S")
        self.data["lastState"] = "BLOCKED"

    def start_run(self, when=None):
        self.data["runs"] += 1
        self.data["lastRunAt"] = when or time.strftime("%Y-%m-%dT%H:%M:%S")


def pending_months(targets, snap_dir: Path, checkpoint: Checkpoint):
    """아직 안 받은 달만 남긴다.

    - 스냅샷 파일이 있으면 skip (동일 데이터 재호출 0)
    - 이전 실행에서 '데이터 없음'으로 확인된 달도 skip (재요청 낭비 방지)
    """
    empty = set(checkpoint.data.get("empty", []))
    out = []
    for iso in targets:
        if (snap_dir / f"{iso}.csv.gz").exists():
            continue
        if iso in empty:
            continue
        out.append(iso)
    return out


def plan(targets, snap_dir: Path, checkpoint: Checkpoint, *, minutes_budget: float = 20.0):
    """이번 실행에서 받을 목록과 예상 소요를 미리 계산해 보여준다(과도 수집 방지)."""
    pend = pending_months(targets, snap_dir, checkpoint)
    batch = recommended_batch_months(minutes_budget)
    take = pend[:batch]
    pace = pacing_seconds()
    years = len({t[:4] for t in take})
    calls = len(take) * CALLS_PER_MONTH + years * CALLS_PER_YEAR_EXTRA
    return {"pendingTotal": len(pend), "batchSize": batch, "take": take,
            "estimatedCalls": calls, "pacingSeconds": round(pace, 1),
            "estimatedMinutes": round(len(take) * pace / 60.0, 1),
            "remainingAfter": max(0, len(pend) - len(take))}
