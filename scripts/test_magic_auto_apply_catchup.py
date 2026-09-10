#!/usr/bin/env python3
# test_magic_auto_apply_catchup.py
# WABABA-AUTO-APPLY-BACKLOG-CATCHUP-R1 회귀
#
# 고정하려는 것(2026-08-12 실측 결함):
#   run_auto_apply 는 "1회 = 가장 오래된 미반영 1건"이라, 백로그가 1일 생기면 매일 16:25 가
#   어제를 소진하는 동안 오늘이 새로 쌓여 **백로그가 영원히 1로 고정**된다.
#   → 17:00 publish gate 의 canonicalLatestIsToday 가 매일 실패(BLOCKED_CANONICAL_LEDGER_BEHIND).
#   실측: 08-12 16:25 실행이 target=2026-08-11 성공 + remainingPendingDates=["2026-08-12"].
#
# 방식: run_once_fn 주입 fake 만 쓴다(기존 rederive_fn 주입과 같은 패턴).
#   실 canonical·네트워크·apply 체인·파일 write 전부 미접근. 실주문/브로커/publish 0.
# 사용: python scripts\test_magic_auto_apply_catchup.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import magic_daily_auto_apply as AA  # noqa: E402

_pass = 0
_fail = 0


def check(name, actual, expected):
    global _pass, _fail
    if actual == expected:
        _pass += 1
        print(f"  PASS  {name}")
    else:
        _fail += 1
        print(f"  FAIL  {name}  expected={expected!r} actual={actual!r}")


TODAY = "2026-08-12"


def applied(target, seq, remaining):
    """run_auto_apply 의 APPLIED 결과와 같은 모양(필요 필드만)."""
    return {"status": AA.APPLIED_AUTOMATICALLY, "verdict": "PASS", "date": TODAY,
            "targetExecutionDate": target, "canonicalChanged": True,
            "officialSequence": seq, "officialTradingDayIndex": seq + 2,
            "remainingPendingDates": list(remaining),
            "realOrderCount": 0, "brokerApiCallCount": 0,
            "reason": "전체 게이트 PASS — 가상 장부 자동 반영 완료(실주문 없음)"}


def terminal(status, verdict, **extra):
    r = {"status": status, "verdict": verdict, "date": TODAY, "canonicalChanged": False,
         "realOrderCount": 0, "brokerApiCallCount": 0}
    r.update(extra)
    return r


class Fake:
    """호출될 때마다 미리 정한 결과를 순서대로 낸다. 호출 인자도 기록한다."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)
        if not self.results:
            raise AssertionError("fake runner 가 예상보다 많이 호출됐다(무한루프 의심)")
        return self.results.pop(0)


# ── A) backlog 0 — 아무 것도 쓰지 않고 정상 종료(기존 동작 비회귀) ──────────────
print("[A] backlog 0")
f = Fake([terminal(AA.NO_ACTION_ALREADY_CURRENT, "PASS", remainingPendingDates=[],
                   reason="미반영 거래일 없음 — 장부가 최신", pendingDates=[])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 1회만", len(f.calls), 1)
check("status 보존", r["status"], AA.NO_ACTION_ALREADY_CURRENT)
check("verdict PASS", r["verdict"], "PASS")
check("appliedDates 빈 배열", r["appliedDates"], [])
check("canonicalChanged False", r["canonicalChanged"], False)
check("catchupStopped 없음", "catchupStopped" in r, False)

print("[A-2] 비거래일 self-skip 비회귀")
f = Fake([terminal(AA.SKIPPED_NON_TRADING_DAY, "PASS", reason="비거래일 — 자동 반영 대상 없음")])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso="2026-08-15", do_apply=True)
check("호출 1회만", len(f.calls), 1)
check("status 보존", r["status"], AA.SKIPPED_NON_TRADING_DAY)
check("verdict PASS", r["verdict"], "PASS")

# ── B) backlog 1 — 기존과 동일하게 1건 적용 ────────────────────────────────────
print("[B] backlog 1")
f = Fake([applied("2026-08-12", 30, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 1회", len(f.calls), 1)
check("status APPLIED", r["status"], AA.APPLIED_AUTOMATICALLY)
check("verdict PASS", r["verdict"], "PASS")
check("appliedDates 1건", r["appliedDates"], ["2026-08-12"])
check("remaining 0", r["remainingPendingDates"], [])
check("iterations 1", r["catchupIterations"], 1)
check("seq 유지", r["officialSequence"], 30)

# ── C) backlog 2 — 한 번의 실행으로 2건 순차 적용 ──────────────────────────────
print("[C] backlog 2 (이번 결함의 핵심 시나리오)")
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          applied("2026-08-12", 30, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 2회", len(f.calls), 2)
check("status APPLIED", r["status"], AA.APPLIED_AUTOMATICALLY)
check("appliedDates 오래된 순", r["appliedDates"], ["2026-08-11", "2026-08-12"])
check("remainingPendingDates = []", r["remainingPendingDates"], [])
check("마지막 target 이 당일", r["targetExecutionDate"], "2026-08-12")
check("seq 최종값", r["officialSequence"], 30)
check("iterations 2", r["catchupIterations"], 2)

# ── D) backlog 3+ — 순서 유지 · 중복 0 · 무한루프 0 ────────────────────────────
print("[D] backlog 4")
days = ["2026-08-06", "2026-08-07", "2026-08-10", "2026-08-11"]
res = [applied(d, 26 + i, days[i + 1:]) for i, d in enumerate(days)]
f = Fake(res)
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso="2026-08-11", do_apply=True)
check("호출 4회", len(f.calls), 4)
check("오래된 순서 유지", r["appliedDates"], days)
check("중복 적용 0", len(set(r["appliedDates"])), 4)
check("오름차순", r["appliedDates"] == sorted(r["appliedDates"]), True)
check("remaining 0", r["remainingPendingDates"], [])
check("iterations 4", r["catchupIterations"], 4)

# ── E) 중간 실패 — 첫 성공 보존 · 이후 중단 · reason/action 보존 ───────────────
print("[E] 중간 실패(2번째 날짜 게이트 실패)")
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          terminal(AA.BLOCKED_GATE_FAILED, "BLOCKED",
                   targetExecutionDate="2026-08-12",
                   blockedCodes=[AA.B_KRX_DATA_QUALITY],
                   remainingPendingDates=["2026-08-12"],
                   reason="게이트 실패 — KRX 품질 증거 없음",
                   founderAction="해당 거래일 KRX 수집 재검증 후 재실행")])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 2회 후 중단", len(f.calls), 2)
check("실패를 숨기지 않음", r["status"], AA.BLOCKED_GATE_FAILED)
check("verdict BLOCKED", r["verdict"], "BLOCKED")
check("첫 성공 보존", r["appliedDates"], ["2026-08-11"])
check("blockedCodes 보존", r["blockedCodes"], [AA.B_KRX_DATA_QUALITY])
check("reason 보존", r["reason"], "게이트 실패 — KRX 품질 증거 없음")
check("founderAction 보존", r["founderAction"], "해당 거래일 KRX 수집 재검증 후 재실행")
check("remaining 정확", r["remainingPendingDates"], ["2026-08-12"])
check("catchupStopped 기록", r["catchupStopped"]["status"], AA.BLOCKED_GATE_FAILED)

print("[E-2] 첫 건부터 실패 → 단일 실행과 동일(비회귀)")
f = Fake([terminal(AA.BLOCKED_LOCK_BUSY, "BLOCKED", reason="lock 보유 중",
                   founderAction="잔존 lock 확인")])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 1회", len(f.calls), 1)
check("status 그대로", r["status"], AA.BLOCKED_LOCK_BUSY)
check("appliedDates 빈 배열", r["appliedDates"], [])
check("catchupStopped 없음(단일과 동일)", "catchupStopped" in r, False)

# ── F) 당일 데이터 미준비 — 처리 가능한 날짜까지만, 강제 적용 없음 ─────────────
print("[F] 당일 데이터 미준비")
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          terminal(AA.SKIPPED_NOT_READY, "BLOCKED", targetExecutionDate="2026-08-12",
                   remainingPendingDates=["2026-08-12"],
                   reason="dry-run 재도출 실패 또는 시퀀스 불일치",
                   founderAction="signal/dry-run 파이프라인 상태 확인")])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("호출 2회 후 중단", len(f.calls), 2)
check("강제 적용 안 함(적용 1건)", r["appliedDates"], ["2026-08-11"])
check("성공분을 BLOCKED 로 덮지 않음", r["verdict"], "PASS")
check("status 는 마지막 성공", r["status"], AA.APPLIED_AUTOMATICALLY)
check("미준비 사실 보존", r["catchupStopped"]["status"], AA.SKIPPED_NOT_READY)
check("미준비 사유 보존", r["catchupStopped"]["reason"], "dry-run 재도출 실패 또는 시퀀스 불일치")
check("잔여 노출", r["remainingPendingDates"], ["2026-08-12"])

# ── loop safety ────────────────────────────────────────────────────────────────
print("[S] loop safety")
# S1 동일 target 반복 → 즉시 BLOCKED(무한루프 차단)
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          applied("2026-08-11", 29, ["2026-08-12"])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("S1 동일 target 반복 감지", r["status"], AA.BLOCKED_CATCHUP_NO_PROGRESS)
check("S1 verdict BLOCKED", r["verdict"], "BLOCKED")
check("S1 중복 적용 0", r["appliedDates"], ["2026-08-11"])
check("S1 2회에서 멈춤", len(f.calls), 2)

# S2 target 이 과거로 역행 → BLOCKED
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          applied("2026-08-10", 30, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("S2 날짜 역행 차단", r["status"], AA.BLOCKED_CATCHUP_NO_PROGRESS)
check("S2 적용은 1건뿐", r["appliedDates"], ["2026-08-11"])

# S3 seq 가 전진하지 않음 → BLOCKED
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]),
          applied("2026-08-12", 29, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
check("S3 seq 미전진 차단", r["status"], AA.BLOCKED_CATCHUP_NO_PROGRESS)
check("S3 적용은 1건뿐", r["appliedDates"], ["2026-08-11"])

# S4 데이터 기반 상한 — remaining 이 줄지 않아도 pending 수 이상 돌지 않는다
f = Fake([applied("2026-08-10", 28, ["2026-08-11"]),
          applied("2026-08-11", 29, ["2026-08-12"]),
          applied("2026-08-12", 30, ["2026-08-13"]),
          applied("2026-08-13", 31, ["2026-08-14"]),
          applied("2026-08-14", 32, ["2026-08-17"])])
r = AA.run_auto_apply_catchup(run_once_fn=f, max_iterations=3, today_iso=TODAY, do_apply=True)
check("S4 상한 3회 준수", len(f.calls), 3)
check("S4 상한 도달 기록", r["catchupStopped"]["status"], AA.BLOCKED_CATCHUP_MAX_ITERATIONS)
check("S4 적용분 보존", r["appliedDates"], ["2026-08-10", "2026-08-11", "2026-08-12"])
check("S4 잔여 노출", r["remainingPendingDates"], ["2026-08-13"])

# S5 하드 캡을 넘는 max_iterations 요청은 캡으로 잘린다
check("S5 하드 캡 상수", AA.CATCHUP_MAX_ITERATIONS, 40)
f = Fake([applied("2026-08-11", 29, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, max_iterations=99999, today_iso=TODAY, do_apply=True)
check("S5 정상 종료", r["appliedDates"], ["2026-08-11"])

# S6 인자는 그대로 단일 runner 로 전달된다(계약 유지)
f = Fake([applied("2026-08-11", 29, [])])
AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True, canonical_path="X")
check("S6 kwargs 전달", f.calls[0], {"today_iso": TODAY, "do_apply": True, "canonical_path": "X"})

# ── 계약 비회귀 ────────────────────────────────────────────────────────────────
print("[G] 스키마·계약 비회귀")
f = Fake([applied("2026-08-11", 29, ["2026-08-12"]), applied("2026-08-12", 30, [])])
r = AA.run_auto_apply_catchup(run_once_fn=f, today_iso=TODAY, do_apply=True)
base_keys = set(applied("2026-08-12", 30, []).keys())
check("기존 필드 전부 유지", base_keys.issubset(set(r.keys())), True)
check("추가 필드는 2개뿐", sorted(set(r.keys()) - base_keys), ["appliedDates", "catchupIterations"])
check("publish gate 요구 status", r["status"] in (AA.APPLIED_AUTOMATICALLY,
                                                  AA.NO_ACTION_ALREADY_CURRENT), True)
check("실주문 0", r["realOrderCount"], 0)
check("브로커 호출 0", r["brokerApiCallCount"], 0)

# durable status MD 가 catch-up 을 드러내는지(파일 write 는 임시 경로로만)
import tempfile  # noqa: E402
tmp = Path(tempfile.mkdtemp())
AA.write_durable_status(r, json_path=tmp / "s.json", md_path=tmp / "s.md")
md = (tmp / "s.md").read_text(encoding="utf-8")
check("MD 에 반영 건수 노출", "이번 실행 반영: 2건" in md, True)
check("MD 판정 헤더 유지", md.splitlines()[0], "전체 판정: PASS")

# 단일 실행 계약(run_auto_apply)은 손대지 않았다 — 시그니처·docstring 고정
import inspect  # noqa: E402
sig = inspect.signature(AA.run_auto_apply)
check("run_auto_apply 시그니처 불변", "max_iterations" in sig.parameters, False)
check("run_auto_apply 단일 계약 유지", "1회 실행 = 최대 1거래일" in (AA.run_auto_apply.__doc__ or ""), True)

# 보안 정적: catch-up 코드가 주문·브로커·publish 경로를 새로 들이지 않았다
src = Path(AA.__file__).read_text(encoding="utf-8")
for bad in ("smtplib", "place_order", "send_order", "broker_api", "requests.post"):
    check(f"미사용(코드): {bad}", bad in src, False)

# ── H) CLI 배선 — main() 이 실제로 catch-up 을 타는가(회귀 방지) ───────────────
print("[H] CLI 배선")
_seen = {}


def _fake_catchup(**kw):
    _seen["catchup"] = kw
    return applied("2026-08-12", 30, [])


def _fake_single(**kw):
    _seen["single"] = kw
    return applied("2026-08-12", 30, [])


_orig = (AA.run_auto_apply_catchup, AA.run_auto_apply, AA.write_durable_status,
         AA.C.write_json_report, AA._magic_hold_gate)
try:
    # R4: HOLD 게이트 동작은 test_magic_paper_lane_hold.py 가 고정한다. 여기선 기존 CLI 배선만 본다.
    AA._magic_hold_gate = lambda: {"decision": "UNHELD"}
    AA.run_auto_apply_catchup = _fake_catchup
    AA.run_auto_apply = _fake_single
    AA.write_durable_status = lambda *a, **k: None
    AA.C.write_json_report = lambda *a, **k: ""      # 운영 reports/ 미접촉

    _seen.clear()
    check("기본 실행 exit 0", AA.main(["--date", TODAY]), 0)
    check("기본은 catch-up 경로", "catchup" in _seen, True)
    check("기본은 단일 경로 미사용", "single" in _seen, False)
    check("catch-up 에 do_apply=True", _seen["catchup"].get("do_apply"), True)

    _seen.clear()
    AA.main(["--date", TODAY, "--no-catchup"])
    check("--no-catchup 은 단일 경로", "single" in _seen, True)
    check("--no-catchup 은 catch-up 미사용", "catchup" in _seen, False)

    _seen.clear()
    AA.main(["--date", TODAY, "--dry-run"])
    check("--dry-run 은 단일 경로(무한 재평가 방지)", "single" in _seen, True)
    check("--dry-run 은 catch-up 미사용", "catchup" in _seen, False)
    check("--dry-run 은 do_apply=False", _seen["single"].get("do_apply"), False)

    _seen.clear()
    AA.main(["--date", TODAY, "--max-catchup", "3"])
    check("--max-catchup 전달", _seen["catchup"].get("max_iterations"), 3)
finally:
    (AA.run_auto_apply_catchup, AA.run_auto_apply, AA.write_durable_status,
     AA.C.write_json_report, AA._magic_hold_gate) = _orig

print()
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
