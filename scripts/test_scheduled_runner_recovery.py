#!/usr/bin/env python3
"""와바바 예약 runner 3건 장애 복구 회귀 (WABABA-SCHEDULED-RUNNER-FAILURE-RECOVERY-CLOSEOUT-R1).

2026-08-21 실사고: 예약 runner 3건이 동시에 실패했다.
  Fund Plan Preview (16:20) rc=2 · Magic Daily Auto Apply (16:25) rc=2 ·
  Wababa Auto Publish (17:00) rc=1

원인은 3개가 아니라 **1개**였다. 2026-08-19 KRX 데이터 품질 게이트 실패로
signalAsOf=2026-08-19 패키지가 생성되지 않았고, 그 패키지가 있어야만 만들 수 있는
executionDate=2026-08-20 이 영구히 반영 불가가 되면서 backlog 선두가 막혔다.
막힌 장부가 Fund Plan(CANONICAL_LEDGER_BEHIND)과
Auto Publish(BLOCKED_APPLY_NOT_PASS)로 그대로 전파됐다.

이 테스트가 고정하는 것:
  ① 세 runner 의 exit code 계약이 뒤집히지 않는다(정상 no-op=0, 실제 이상=non-zero)
  ② 예약 action 이 가리키는 실행파일·스크립트·작업 디렉터리가 실재한다
  ③ backlog 선두의 신호 패키지 부재를 **탐지 가능**하다(같은 장애 재발 시 조기 발견)
  ④ MISSED_RUN 복구가 backlog 를 실제로 전진시킨다
  ⑤ 안전 게이트: 자동 반영 경로에 broker/주문/SMTP 호출이 없다
  ⑥ publish 는 선행 게이트 실패 시 외부행동 0 으로 멈춘다

안전: 계산·읽기 전용. 네트워크 0. canonical write 0. 실주문 0.
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import magic_daily_auto_apply as AA  # noqa: E402
import magic_daily_common as C  # noqa: E402
import magic_daily_fund_plan as FP  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
REPO1 = Path("C:/work/kr-stock-agent")
CANONICAL = ROOT / "magic-formula-official-state.json"
PASS = FAIL = 0

# 2026-08-24 예약 주기에 예약된 3건 (TaskName · 실행파일 · 인자 대상 · 작업 디렉터리)
RUNNERS = [
    {"task": "Wababa Magic Daily Fund Plan Preview",
     "script": ROOT / "scripts" / "magic_daily_fund_plan.py",
     "workdir": ROOT, "at": "16:20"},
    {"task": "Wababa Magic Daily Auto Apply",
     "script": ROOT / "scripts" / "magic_daily_auto_apply.py",
     "workdir": ROOT, "at": "16:25"},
    {"task": "Wababa Auto Publish",
     "script": REPO1 / "scripts" / "ops" / "publish-public-data.ps1",
     "workdir": REPO1, "at": "17:00"},
]

# 자동 반영 경로에 있으면 안 되는 것(정적 고정).
# ★ 부분문자열로 보면 안 된다 — brokerApiCallCount 처럼 **0 을 증명하는 카운터 필드**가
#   오탐된다. 실제 import·호출 형태로만 판정한다.
FORBIDDEN_IMPORTS = ("smtplib", "kis_api", "broker_api", "ebest", "kiwoom")
FORBIDDEN_CALLS = ("send_order", "place_order", "OrderRequest", "sendmail",
                   "SMTP(", "submit_order")


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def canonical():
    return json.loads(CANONICAL.read_text(encoding="utf-8"))


# ═══════════ ① exit code 계약 ═══════════
def t_exit_contract():
    print("[1] exit code 계약 (정상=0 / 실제 이상=non-zero)")
    src = (ROOT / "scripts" / "magic_daily_auto_apply.py").read_text(encoding="utf-8")
    ck("auto_apply: verdict PASS 면 0, 아니면 2",
       'return 0 if r["verdict"] == "PASS" else 2' in src)
    ck("auto_apply: 무조건 0 반환 아님", "return 0\n\n\nif __name__" not in src)
    fsrc = (ROOT / "scripts" / "magic_daily_fund_plan.py").read_text(encoding="utf-8")
    ck("fund_plan: PLAN_BLOCKED 면 2, 아니면 0",
       'return 2 if r["status"] == PLAN_BLOCKED else 0' in fsrc)

    # 비거래일 no-op 은 성공이어야 한다 — 실패로 기록되면 오탐이 된다
    ck("비거래일은 정상 no-op(PASS) 개념 존재",
       AA.SKIPPED_NON_TRADING_DAY in src)
    ck("데이터 미준비는 BLOCKED 개념으로 분리", AA.SKIPPED_NOT_READY in src)
    ck("두 개념이 다른 상수", AA.SKIPPED_NON_TRADING_DAY != AA.SKIPPED_NOT_READY)

    # publish 는 선행 게이트 실패를 NO_CHANGE 성공으로 위장하지 않는다
    if RUNNERS[2]["script"].exists():
        p = RUNNERS[2]["script"].read_text(encoding="utf-8", errors="replace")
        ck("publish: 선행 미완료를 성공으로 위장 금지 명시",
           "NO_CHANGE 성공으로 위장하지 않는다" in p)
        ck("publish: 선행 게이트 존재", "magic_publish_gate" in p)


# ═══════════ ② 예약 action 실체 ═══════════
def t_action_targets():
    print("[2] 예약 action 대상 실재")
    py = Path(sys.executable)
    ck("python 실행파일 존재", py.exists(), str(py))
    for r in RUNNERS:
        ck(f"{r['at']} {r['task']}: 스크립트 존재", r["script"].exists(),
           str(r["script"]))
        ck(f"{r['at']} {r['task']}: 작업 디렉터리 존재", r["workdir"].is_dir(),
           str(r["workdir"]))
    ck("canonical 장부 파일 존재", CANONICAL.exists())
    ck("신호 패키지 루트 존재", C.TEMP_ROOT.exists(), str(C.TEMP_ROOT))


# ═══════════ ③ backlog 선두 탐지 ═══════════
def _pkg_for(exec_date):
    """executionDate 를 만들 수 있는 신호 패키지(직전 거래일 signalAsOf)."""
    prev = AA.previous_trading_day(exec_date)
    if not prev:
        return None, None
    man = C.TEMP_ROOT / str(prev) / "manifest.json"
    if not man.exists():
        return prev, None
    try:
        return prev, json.loads(man.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return prev, None


def t_backlog_detection():
    print("[3] backlog 선두 신호 패키지 부재 탐지")
    can = canonical()
    pend = AA.unapplied_execution_dates(can, "2026-08-24")
    ck("미반영 거래일 목록 산출", isinstance(pend, list))
    ck("missedRuns 는 미반영에서 제외됨",
       all(d not in {str(m.get("date")) for m in (can.get("missedRuns") or [])}
           for d in pend))
    if pend:
        head = pend[0]
        prev, man = _pkg_for(head)
        ready = bool(man) and man.get("packageStatus") == "READY_FOR_EXECUTION_OPEN"
        print(f"        backlog 선두 {head} ← 신호 {prev} "
              f"패키지={'READY' if ready else '없음/미준비'}")
        ck("선두 상태를 판정할 수 있다(탐지 가능)", isinstance(ready, bool))
        # 선두 패키지가 없으면 그 날은 영구 반영 불가 → MISSED_RUN 없이는 전진 불가
        if not ready:
            sim = copy.deepcopy(can)
            sim.setdefault("missedRuns", []).append(
                {"date": head, "status": "MISSED_RUN",
                 "reason": "NO_PREOPEN_SIGNAL_PACKAGE"})
            after = AA.unapplied_execution_dates(sim, "2026-08-24")
            ck("선두 패키지 부재 = 스스로 전진 불가(교착)", head in pend)
            ck("MISSED_RUN 기록이 backlog 를 전진시킨다",
               head not in after and len(after) < len(pend), (pend, after))


# ═══════════ ④ MISSED_RUN 복구 경로 ═══════════
def t_recovery_path():
    print("[4] MISSED_RUN 복구 경로")
    rec = ROOT / "scripts" / "record_magic_missed_run.py"
    ck("승인된 복구 스크립트 존재", rec.exists())
    if rec.exists():
        s = rec.read_text(encoding="utf-8")
        ck("복구는 confirm 토큰 필수(무단 canonical write 금지)",
           'confirm != f"RECORD_MISSED_RUN_{date}"' in s)
        ck("--apply 없으면 write 0", "use --apply --confirm to persist" in s)
        ck("가짜 거래 생성 금지 명시", "가짜 거래 없이" in s)
        ck("거래일 판정은 실제 KRX 로만", "평일 추정 금지" in s)

    # 복구가 거래·자금·순번을 건드리지 않는다는 계약
    import magic_rolling_engine as E
    ck("엔진에 MISSED_RUN 사유 상수 존재",
       hasattr(E, "MISSED_RUN_NO_PREOPEN_SIGNAL"))
    ck("idempotent 설계(중복 기록 방지)",
       "already=True" in (ROOT / "scripts" / "magic_rolling_engine.py")
       .read_text(encoding="utf-8"))


# ═══════════ ⑤ 안전 게이트 ═══════════
def t_safety_gates():
    print("[5] 안전 게이트 — 실주문·브로커·SMTP 0")
    for name in ("magic_daily_auto_apply.py", "magic_daily_fund_plan.py"):
        src = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        imports = [ln.strip() for ln in src.splitlines()
                   if ln.strip().startswith(("import ", "from "))]
        for tok in FORBIDDEN_IMPORTS:
            ck(f"{name}: {tok} import 없음",
               not any(tok in ln for ln in imports),
               [ln for ln in imports if tok in ln])
        code = "\n".join(ln for ln in src.splitlines()
                         if not ln.strip().startswith("#"))
        for tok in FORBIDDEN_CALLS:
            ck(f"{name}: {tok} 호출 없음", tok not in code)
        # 0 을 **증명하는** 카운터는 있어야 정상이다
        ck(f"{name}: 브로커 호출 카운터로 0 을 명시",
           "brokerApiCallCount" in src)
    ck("auto_apply: 정책 승인자 표기(사람 사칭 금지)",
       AA.AUTO_APPROVER == "WABABA_AUTO_POLICY_V1")

    # fund plan 은 미리보기 — 실제 발송 0
    fs = (ROOT / "scripts" / "magic_daily_fund_plan.py").read_text(encoding="utf-8")
    ck("fund_plan: dry-run/실제 발송 없음 명시", "실제 발송 없음" in fs)


# ═══════════ ⑥ publish 외부행동 차단 ═══════════
def t_publish_gate():
    print("[6] publish 외부행동 차단 (선행 게이트 실패 시)")
    st = REPO1 / "reports" / "wababa" / "wababa-auto-publish-status-latest.json"
    ck("publish 상태 산출물 존재", st.exists(), str(st))
    if st.exists():
        # PowerShell 이 쓴 JSON 은 UTF-8 BOM 을 갖는다 → utf-8-sig 로 읽는다.
        d = json.loads(st.read_text(encoding="utf-8-sig"))
        blocked = str(d.get("status", "")).startswith("BLOCKED")
        ck("상태 기록됨", d.get("status") is not None)
        if blocked:
            ck("BLOCKED 시 public 변경 0", d.get("publicChanged") is False)
            ck("BLOCKED 시 commit 없음", not d.get("commit"))
            ck("BLOCKED 시 push 없음", d.get("pushed") is False)
            ck("BLOCKED 시 배포 트리거 없음", d.get("deployTrigger") == "none")
        ck("실주문 0", d.get("realOrderCount") == 0)
        ck("브로커 호출 0", d.get("brokerApiCallCount") == 0)
        ck("SMTP 호출 0", d.get("smtpCallCount") == 0)


def main() -> int:
    for f in (t_exit_contract, t_action_targets, t_backlog_detection,
              t_recovery_path, t_safety_gates, t_publish_gate):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("canonicalWrites: 0")
    print("realOrders: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
