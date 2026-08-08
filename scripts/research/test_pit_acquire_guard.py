#!/usr/bin/env python3
"""pit_acquire_guard 오프라인 회귀 테스트 — 네트워크 호출 0.

차단 상태에서 코드를 검증해야 하므로 fetch/sleep/random 을 전부 주입한다.
KRX 에 단 1콜도 나가지 않는다.

사용: python scripts/research/test_pit_acquire_guard.py
"""
from __future__ import annotations

import gzip
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pit_acquire_guard as G  # noqa: E402

PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


BLOCK_HTML = '<html><head><style>.ip-block-page { min-width: 1200px; }</style></head></html>'
OK_HTML = '<html><head><title>KRX</title></head><body>login form</body></html>'


def t_block_detection():
    print("[1] 차단 감지 (fail-closed)")
    ck("차단 페이지 → BLOCKED",
       G.block_status(lambda u: (200, BLOCK_HTML))["state"] == "BLOCKED")
    ck("정상 페이지 → CLEAR",
       G.block_status(lambda u: (200, OK_HTML))["state"] == "CLEAR")
    ck("HTTP 403 → UNKNOWN (CLEAR 로 낙관 금지)",
       G.block_status(lambda u: (403, ""))["state"] == "UNKNOWN")

    def boom(u):
        raise ConnectionError("network down")
    ck("네트워크 예외 → UNKNOWN (예외 전파 안 함)",
       G.block_status(boom)["state"] == "UNKNOWN")
    ck("차단 시 blocked=True 플래그",
       G.block_status(lambda u: (200, BLOCK_HTML))["blocked"] is True)
    # 대문자/변형 마커
    ck("변형 마커(ipBlockPage) 도 감지",
       G.block_status(lambda u: (200, "<div class=ipBlockPage>"))["state"] == "BLOCKED")


def t_pacing():
    print("[2] pacing · jitter · backoff")
    pace = G.pacing_seconds()
    ck("월당 대기 = 분당안전콜 기준 역산 (4콜/월, 6콜/분 → 40s)", abs(pace - 40.0) < 0.01, pace)
    ck("차단 유발 속도(0.15s)보다 훨씬 느림", pace > 30)

    waits = [G.sleep_with_jitter(10.0, sleeper=lambda s: None, rand=lambda: r)
             for r in (0.0, 0.5, 1.0)]
    ck("지터 하한 = base*(1-0.35)", abs(waits[0] - 6.5) < 1e-6, waits[0])
    ck("지터 중앙 = base", abs(waits[1] - 10.0) < 1e-6, waits[1])
    ck("지터 상한 = base*(1+0.35)", abs(waits[2] - 13.5) < 1e-6, waits[2])
    ck("고정간격 아님(값이 서로 다름)", len(set(waits)) == 3)
    ck("base<=0 이면 대기 0", G.sleep_with_jitter(0, sleeper=lambda s: None) == 0.0)

    b1 = G.backoff_seconds(1, rand=lambda: 0.5)
    b2 = G.backoff_seconds(2, rand=lambda: 0.5)
    b3 = G.backoff_seconds(3, rand=lambda: 0.5)
    ck("백오프가 지수적으로 증가", b1 < b2 < b3, (b1, b2, b3))
    ck("백오프 상한(cap) 적용", G.backoff_seconds(50, rand=lambda: 1.0) <= 900 * 1.3)


def t_batch_sizing():
    print("[3] 보수적 batch 산정 (60개월 고정 가정 금지)")
    b = G.recommended_batch_months(minutes_budget=20.0)
    ck("20분 예산 → 30개월 (6콜/분 ÷ 4콜/월)", b == 30, b)
    ck("기존 60개월 가정보다 보수적", b < 60, b)
    ck("예산이 작으면 batch 도 작아짐",
       G.recommended_batch_months(minutes_budget=5.0) < b)
    ck("최소 1개월은 보장", G.recommended_batch_months(minutes_budget=0.0) >= 1)
    ck("월당 콜수 상수가 실제 구현과 일치(4)", G.CALLS_PER_MONTH == 4)


def t_checkpoint(tmp: Path):
    print("[4] checkpoint 원자성 · resume")
    cp_path = tmp / "_checkpoint.json"
    cp = G.Checkpoint(cp_path)
    cp.start_run(when="2026-08-08T15:00:00")
    cp.mark_done("2007-01-02")
    cp.mark_empty("2007-02-01")
    cp.mark_failed("2007-03-02", "timeout")
    cp.save()
    ck("체크포인트 파일 생성", cp_path.exists())
    ck("tmp 파일이 남지 않음(원자적 교체)", not cp_path.with_suffix(".tmp").exists())

    cp2 = G.Checkpoint(cp_path)
    ck("재기동 시 done 복원", cp2.data["done"] == ["2007-01-02"])
    ck("재기동 시 empty 복원", cp2.data["empty"] == ["2007-02-01"])
    ck("재기동 시 failed 복원", "2007-03-02" in cp2.data["failed"])
    ck("누적 콜수 기록", cp2.data["totalCalls"] == G.CALLS_PER_MONTH)

    cp2.mark_blocked(when="2026-08-08T15:05:00")
    cp2.save()
    ck("차단 시각 기록", G.Checkpoint(cp_path).data["lastBlockedAt"] == "2026-08-08T15:05:00")

    (cp_path).write_text("{ this is not json", encoding="utf-8")
    cp3 = G.Checkpoint(cp_path)
    ck("손상된 체크포인트에도 죽지 않음(빈 상태로 시작)", cp3.data["done"] == [])


def t_resume(tmp: Path):
    print("[5] resume — 이미 받은 달 재호출 0")
    snap = tmp / "snaps"
    snap.mkdir(parents=True, exist_ok=True)
    for iso in ("2007-01-02", "2007-02-01"):
        with gzip.open(snap / f"{iso}.csv.gz", "wt", encoding="utf-8") as fh:
            fh.write("ticker\n005930\n")
    cp = G.Checkpoint(tmp / "_cp2.json")
    cp.mark_empty("2007-04-02")
    targets = ["2007-01-02", "2007-02-01", "2007-03-02", "2007-04-02", "2007-05-02"]
    pend = G.pending_months(targets, snap, cp)
    ck("이미 있는 스냅샷 2건 skip", "2007-01-02" not in pend and "2007-02-01" not in pend)
    ck("빈 것으로 확인된 달도 skip", "2007-04-02" not in pend)
    ck("남은 것만 대상", pend == ["2007-03-02", "2007-05-02"], pend)

    p = G.plan(targets, snap, cp, minutes_budget=20.0)
    ck("plan 이 남은 개수 보고", p["pendingTotal"] == 2)
    ck("plan 이 예상 콜수 계산", p["estimatedCalls"] == 2 * 4 + 1 * 1, p["estimatedCalls"])
    ck("plan 이 예상 소요 계산", p["estimatedMinutes"] > 0)
    ck("batch 초과분은 다음 실행으로", p["remainingAfter"] == 0)

    big = [f"20{y:02d}-01-02" for y in range(7, 27)] * 3
    p2 = G.plan(big, snap, cp, minutes_budget=20.0)
    ck("대량 대상이면 batch 로 잘림", len(p2["take"]) == p2["batchSize"])
    ck("나머지는 remainingAfter 로 이월", p2["remainingAfter"] > 0)


def t_no_evasion():
    print("[6] 우회 금지 확인 (정적 검사)")
    src = Path(__file__).with_name("pit_acquire_guard.py").read_text(encoding="utf-8")
    for bad in ("proxies", "socks", "rotate", "randint(0, 255)"):
        ck(f"우회 코드 없음: {bad}", bad not in src)
    # 문자열 전체를 세면 "User-Agent 회전 금지" 라는 *금지 문구* 자체가 잡힌다.
    # 실제로 확인해야 할 것은 "헤더로 보내는 UA 가 하나뿐인가" 이므로 dict 키 형태만 센다.
    ua_headers = src.count('"User-Agent":')
    ck("헤더 UA 는 정확히 1개(회전 없음)", ua_headers == 1, f"count={ua_headers}")
    ck("UA 목록/배열 없음", "USER_AGENTS" not in src and "user_agents" not in src)
    ck("차단은 정지 신호로 문서화", "정지 신호" in src)


def main():
    tmp = Path(tempfile.mkdtemp(prefix="pit-guard-test-"))
    try:
        t_block_detection()
        t_pacing()
        t_batch_sizing()
        t_checkpoint(tmp)
        t_resume(tmp)
        t_no_evasion()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
