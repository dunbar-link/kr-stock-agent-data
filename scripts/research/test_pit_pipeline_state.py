#!/usr/bin/env python3
"""pit_pipeline_state 오프라인 회귀 — 네트워크 0 · 예약작업 미접촉.

차단이 풀렸을 때 Founder 개입 없이 baseline→matrix→robust 가 이어지는지,
그리고 차단이 유지되는 동안 작은 WAIT 보고가 반복 생성되지 않는지 검증한다.

사용: python scripts/research/test_pit_pipeline_state.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pit_pipeline_state as P  # noqa: E402

PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def t_transitions():
    print("[1] 상태 전이 · 반복보고 억제")
    st = {"state": None, "reportedStates": [], "history": [], "runs": 0}

    # 차단 유지 구간: 몇 번을 돌려도 보고 대상이 아니어야 한다
    reports = [P.transition(st, "WAIT_AUTO_RESUME", {"i": i}) for i in range(5)]
    ck("WAIT_AUTO_RESUME 은 보고 대상 아님", not any(reports))
    ck("WAIT 반복해도 history 1건", len(st["history"]) == 1, len(st["history"]))
    ck("runs 는 매번 증가(상태파일은 갱신됨)", st["runs"] == 5, st["runs"])

    ck("차단 해제 전이는 보고 대상",
       P.transition(st, "BLOCK_CLEARED_ACQUISITION_STARTED", {}) is True)
    ck("같은 의미상태 재전이는 보고 안 함",
       P.transition(st, "BLOCK_CLEARED_ACQUISITION_STARTED", {}) is False)

    for s in ("DATA_ACQUISITION_COMPLETE", "BASELINE_COMPLETE",
              "MATRIX_COMPLETE", "ROBUSTNESS_COMPLETE"):
        ck(f"{s} 최초 1회만 보고", P.transition(st, s, {}) is True)
        ck(f"{s} 재전이 억제", P.transition(st, s, {}) is False)

    ck("의미상태 집합에 7종 정의", len(P.MEANINGFUL) == 7, len(P.MEANINGFUL))
    ck("HARD_FAILURE 도 의미상태", "HARD_FAILURE" in P.MEANINGFUL)
    ck("HUMAN_APPROVAL_REQUIRED 도 의미상태", "HUMAN_APPROVAL_REQUIRED" in P.MEANINGFUL)


def t_report(tmp: Path):
    print("[2] 전이 보고서 형식 (Report Bridge 호환)")
    orig = P.RESEARCH_DIR
    P.RESEARCH_DIR = tmp
    try:
        p = P.write_transition_report("BASELINE_COMPLETE", {"mode": "baseline"})
        txt = p.read_text(encoding="utf-8")
        first = txt.splitlines()[0]
        ck("첫 줄이 판정 헤더", first.startswith("전체 판정: "), first)
        ck("reason_class 포함", "reason_class: BASELINE_COMPLETE" in txt)
        ck("안전 고지 포함", "실주문 0" in txt and "브로커 API 0" in txt)
        p2 = P.write_transition_report("HARD_FAILURE", {"rc": 1}, verdict="BLOCKED")
        ck("실패는 BLOCKED 판정", p2.read_text(encoding="utf-8").startswith("전체 판정: BLOCKED"))
    finally:
        P.RESEARCH_DIR = orig


def t_adequacy_gate():
    print("[3] 데이터 충분성 게이트")
    adq = P.data_adequacy()
    ck("실측 스냅샷 수 보고", isinstance(adq["snapshots"], int) and adq["snapshots"] > 0, adq["snapshots"])
    ck("유효 연속구간 계산됨", isinstance(adq["usableContiguousMonths"], int))
    ck("최소요건 24개월", adq["minRequired"] == P.MIN_USABLE_MONTHS == 24)
    # 게이트의 '현재 값'이 아니라 '규칙'을 검사한다.
    #   R3 당시엔 유효 5개월이라 adequate=False 였지만, 수집이 끝난 지금은 True 가 정상이다.
    #   전이적 데이터 상태를 기대값으로 굳히면 정상 진행이 테스트 실패로 보인다(2026-08-19 실측).
    ck("충분성 판정이 최소요건 규칙과 일치",
       adq["adequate"] == (adq["usableContiguousMonths"] >= adq["minRequired"]),
       f"usable={adq['usableContiguousMonths']} min={adq['minRequired']} adequate={adq['adequate']}")
    ck("유효구간 경계가 산출됨", adq["usableFirst"] is not None and adq["usableLast"] is not None)


def t_status_io(tmp: Path):
    print("[4] 상태파일 원자적 저장 · 복원")
    orig_dir, orig_path = P.RESEARCH_DIR, P.STATUS_PATH
    P.RESEARCH_DIR = tmp
    P.STATUS_PATH = tmp / "status.json"
    try:
        st = P.load_status()
        ck("없으면 초기 상태 반환", st["state"] is None)
        P.transition(st, "DATA_ACQUISITION_COMPLETE", {"x": 1})
        P.save_status(st)
        ck("파일 생성", P.STATUS_PATH.exists())
        ck("tmp 잔여 없음(원자적)", not P.STATUS_PATH.with_suffix(".tmp").exists())
        again = P.load_status()
        ck("재기동 시 상태 복원", again["state"] == "DATA_ACQUISITION_COMPLETE")
        ck("보고이력 복원", "DATA_ACQUISITION_COMPLETE" in again["reportedStates"])
        P.STATUS_PATH.write_text("{broken", encoding="utf-8")
        ck("손상돼도 죽지 않음", P.load_status()["state"] is None)
    finally:
        P.RESEARCH_DIR, P.STATUS_PATH = orig_dir, orig_path


def t_research_skip(tmp: Path):
    print("[5] 이미 끝난 연구 단계는 재실행 안 함")
    orig = P.RESEARCH_DIR
    P.RESEARCH_DIR = tmp
    try:
        for f in ("baseline-latest.json", "matrix-latest.json", "robustness-latest.json"):
            (tmp / f).write_text("{}", encoding="utf-8")
        st = {"state": "ROBUSTNESS_COMPLETE", "history": [], "runs": 0,
              "reportedStates": ["BASELINE_COMPLETE", "MATRIX_COMPLETE", "ROBUSTNESS_COMPLETE"]}
        res = P.run_research(st)          # 서브프로세스가 한 번도 안 떠야 한다
        ck("전 단계 skip", res["ok"] and all(r.get("skipped") for r in res["reports"]),
           json.dumps(res, ensure_ascii=False)[:120])
        ck("3단계 모두 확인", len(res["reports"]) == 3)
    finally:
        P.RESEARCH_DIR = orig


def main():
    tmp = Path(tempfile.mkdtemp(prefix="pit-pipeline-test-"))
    try:
        t_transitions()
        t_report(tmp)
        t_adequacy_gate()
        t_status_io(tmp)
        t_research_skip(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    print(f"결과: PASS {PASS} / FAIL {FAIL}")
    print("verdict: " + ("PASS" if FAIL == 0 else "FAIL"))
    print("networkCalls: 0")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
