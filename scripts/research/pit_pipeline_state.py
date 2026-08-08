#!/usr/bin/env python3
"""PIT 파이프라인 상태 전이 + 자동 연구 진행 (WABABA-PIT-AUTO-RESUME-AND-RESEARCH-R3).

역할
----
예약작업이 30분마다 수집 스크립트를 부른다. 그 스크립트가 매번 이 모듈로
"지금 어떤 상태인가"를 판단하고, **의미 있는 상태 전이일 때만** 보고서를 남긴다.

의미 있는 상태(= Founder 에게 알릴 가치가 있는 것)
    BLOCK_CLEARED_ACQUISITION_STARTED
    DATA_ACQUISITION_COMPLETE
    BASELINE_COMPLETE
    MATRIX_COMPLETE
    ROBUSTNESS_COMPLETE
    HUMAN_APPROVAL_REQUIRED
    HARD_FAILURE

차단이 유지되는 동안(WAIT_AUTO_RESUME)은 상태파일만 조용히 갱신하고
**보고서를 새로 만들지 않는다** — 30분마다 작은 WAIT 보고가 쌓이는 것을 막는다.

새 scheduler framework 가 아니다. 예약작업 1개가 부르는 순차 함수일 뿐이다.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESEARCH_DIR = ROOT / "reports" / "research"
STATUS_PATH = RESEARCH_DIR / "pit-pipeline-status-latest.json"

MEANINGFUL = {
    "BLOCK_CLEARED_ACQUISITION_STARTED",
    "DATA_ACQUISITION_COMPLETE",
    "BASELINE_COMPLETE",
    "MATRIX_COMPLETE",
    "ROBUSTNESS_COMPLETE",
    "HUMAN_APPROVAL_REQUIRED",
    "HARD_FAILURE",
}
# 연구를 실제로 돌리기 위한 최소 유효 연속구간(개월)
MIN_USABLE_MONTHS = 24


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def load_status():
    try:
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"schema": "WABABA_PIT_PIPELINE_STATUS_V1", "state": None,
                "reportedStates": [], "history": [], "runs": 0}


def save_status(st):
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_PATH)


def transition(st, state, detail=None):
    """상태 전이 기록. 같은 의미상태를 두 번 보고하지 않는다."""
    changed = st.get("state") != state
    st["state"] = state
    st["updatedAt"] = _now()
    st["runs"] = st.get("runs", 0) + 1
    if changed:
        st.setdefault("history", []).append({"at": _now(), "state": state,
                                             "detail": (detail or {})})
        st["history"] = st["history"][-50:]
    should_report = (state in MEANINGFUL) and (state not in st.get("reportedStates", []))
    if should_report:
        st.setdefault("reportedStates", []).append(state)
    st["lastDetail"] = detail or {}
    return should_report


def write_transition_report(state, detail, verdict="PASS"):
    """Report Bridge 가 읽는 판정 헤더를 첫 줄에 둔다(파일=상세 원칙)."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    p = RESEARCH_DIR / f"pit-pipeline-{state.lower().replace('_', '-')}-latest.md"
    lines = [
        f"전체 판정: {verdict}",
        f"reason_class: {state}",
        "",
        f"# WABABA PIT 파이프라인 — {state}",
        "",
        f"- 전이 시각: {_now()}",
        "- task_id: WABABA-PIT-AUTO-RESUME-AND-RESEARCH-R3",
        "- 실주문 0 · 브로커 API 0 · 유료 API 0 · canonical 미변경 · public 미변경",
        "",
        "## 상세",
        "",
        "```json",
        json.dumps(detail or {}, ensure_ascii=False, indent=2),
        "```",
    ]
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def data_adequacy():
    """수집 데이터가 연구에 충분한지 — 유효(랭킹 산출 가능) 연속구간 기준."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from backtest_engine import load_names, load_snapshots, rank_universe  # noqa: PLC0415
    from run_backtest_matrix import contiguous_span  # noqa: PLC0415

    snaps = load_snapshots()
    names = load_names()
    usable = [d for d in sorted(snaps) if len(rank_universe(snaps[d], names)) > 0]
    span = contiguous_span(usable)
    return {"snapshots": len(snaps), "usable": len(usable),
            "usableFirst": usable[0] if usable else None,
            "usableLast": usable[-1] if usable else None,
            "usableContiguousMonths": len(span),
            "spanStart": span[0] if span else None,
            "spanEnd": span[-1] if span else None,
            "adequate": len(span) >= MIN_USABLE_MONTHS,
            "minRequired": MIN_USABLE_MONTHS}


def _run_matrix(mode, out_path, timeout=3600):
    """run_backtest_matrix 를 별도 프로세스로 돌린다(네트워크 0 · 결정적)."""
    script = Path(__file__).with_name("run_backtest_matrix.py")
    cmd = [sys.executable, str(script), "--mode", mode, "--out", str(out_path)]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       timeout=timeout, cwd=str(ROOT))
    return p.returncode, (p.stderr or "")[-400:]


def run_research(st):
    """수집이 끝났고 데이터가 충분하면 baseline → matrix → robust 를 연속 실행한다.

    각 단계는 완료 시 상태 전이를 남기고, 이미 끝난 단계는 다시 돌리지 않는다
    (결정적이라 재실행해도 같은 결과지만 불필요한 CPU 를 쓰지 않는다).
    """
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    reports = []
    for mode, state, fname in (
        ("baseline", "BASELINE_COMPLETE", "baseline-latest.json"),
        ("matrix", "MATRIX_COMPLETE", "matrix-latest.json"),
        ("robust", "ROBUSTNESS_COMPLETE", "robustness-latest.json"),
    ):
        out = RESEARCH_DIR / fname
        if state in st.get("reportedStates", []) and out.exists():
            reports.append({"mode": mode, "state": state, "skipped": "already complete"})
            continue
        rc, err = _run_matrix(mode, out)
        if rc != 0:
            detail = {"mode": mode, "returncode": rc, "stderr": err}
            if transition(st, "HARD_FAILURE", detail):
                write_transition_report("HARD_FAILURE", detail, verdict="BLOCKED")
            save_status(st)
            return {"ok": False, "failedMode": mode, "reports": reports}
        detail = {"mode": mode, "out": str(out), "bytes": out.stat().st_size if out.exists() else 0}
        if transition(st, state, detail):
            write_transition_report(state, detail)
        save_status(st)
        reports.append({"mode": mode, "state": state, "out": str(out)})
    return {"ok": True, "reports": reports}
