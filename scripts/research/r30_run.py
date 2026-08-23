#!/usr/bin/env python3
"""R30 전체 실행 — probe → gate → 전체수집 → coverage → R27 재실행 → 판정.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

한 번의 실행으로 끝까지 간다. 각 단계는 앞 단계가 통과했을 때만 열린다.

  1. probe        r30_probe      — 공식 소스가 R27 에 쓸 수 있는지 증명(§3~§11)
  2. reproduce    r30_reproduce  — R25/R26 신호 재현 확인(§20, 읽기 전용)
  3. acquire      r30_acquire    — probe PASS 시에만 4,640일 resumable 수집(§12~§15)
  4. coverage     r30_coverage   — R27 precommit threshold 그대로 재측정(§16·§17)
  5. r27 재실행    r27_run → r27_verdict — coverage PASS 시 즉시(§18)
  6. verdict      r30-final-verdict — reason_class 포함

중간에 막히면 그 지점에서 **정확한 evidence 를 남기고** 멈춘다. 다음 단계를
억지로 진행하지 않는다. Founder 가 기억해서 다시 지시해야 하는 구조를 만들지
않는다 — checkpoint 와 canonical status 가 남는다(§40).

안전: 공식 무료 API · 캐시/보고서만 write · production 원장 write 0 ·
실주문 0 · 외부발송 0 · deploy 0.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RD = ROOT / "reports" / "research"


def run_step(module):
    """같은 인터프리터로 하위 단계를 돌린다. 반환코드와 마지막 줄을 남긴다."""
    p = subprocess.run([sys.executable, str(HERE / f"{module}.py")],
                       capture_output=True, text=True, encoding="utf-8",
                       errors="replace", cwd=str(ROOT))
    tail = [ln for ln in (p.stdout or "").strip().splitlines() if ln.strip()]
    return {"module": module, "returncode": p.returncode,
            "lastLine": tail[-1] if tail else "",
            "stderrTail": "\n".join((p.stderr or "").strip().splitlines()[-6:])}


def read(name):
    p = RD / f"{name}-latest.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {}


def r27_precommit_unchanged():
    """§18·§37 — R27 precommit 이 한 글자도 안 바뀌었는지 확인한다."""
    import hashlib
    from r27_precommit import COVERAGE, PRIMARY_GATE, SENSITIVITY, CAPITAL
    src = (HERE / "r27_precommit.py").read_bytes()
    ok = (PRIMARY_GATE["thresholdKrw"] == 125_000_000
          and SENSITIVITY["LOW"]["thresholdKrw"] == 25_000_000
          and SENSITIVITY["BASE"]["thresholdKrw"] == 125_000_000
          and SENSITIVITY["HIGH"]["thresholdKrw"] == 250_000_000
          and CAPITAL["totalKrw"] == 50_000_000
          and CAPITAL["namesForSanity"] == 40
          and CAPITAL["orderPerNameKrw"] == 1_250_000
          and COVERAGE["minCoveragePctPerYear"] == 90.0
          and COVERAGE["minYearsCovered"] == 15)
    return {"unchanged": bool(ok),
            "sha256": hashlib.sha256(src).hexdigest(),
            "thresholdKrw": PRIMARY_GATE["thresholdKrw"],
            "sensitivity": {k: v["thresholdKrw"] for k, v in SENSITIVITY.items()
                            if isinstance(v, dict) and "thresholdKrw" in v},
            "capital": CAPITAL["totalKrw"],
            "orderPerNameKrw": CAPITAL["orderPerNameKrw"],
            "minCoveragePctPerYear": COVERAGE["minCoveragePctPerYear"],
            "minYearsCovered": COVERAGE["minYearsCovered"]}


REASONS = {
    "CREDENTIAL_ABSENT": ("WAIT", "R30_CREDENTIAL_ABSENT_PENDING_FOUNDER_REGISTRATION"),
    "CREDENTIAL_INVALID": ("WAIT", "R30_CREDENTIAL_INVALID"),
    "PROBE_PASS_2010_PLUS_ONLY": ("BLOCKED", "R30_PRIMARY_FULL_PERIOD_FAIL_2007_2009_MISSING"),
    "PROBE_FAIL_NO_HISTORY": ("BLOCKED", "R30_PRIMARY_NO_HISTORICAL_COVERAGE"),
    "NO_TRADED_VALUE": ("BLOCKED", "R30_TRADED_VALUE_FIELD_MISSING"),
    "NO_DELISTED_HISTORY": ("BLOCKED", "R30_DELISTED_HISTORY_NOT_SUPPORTED"),
    "PROBE_FAIL": ("BLOCKED", "R30_PROBE_FAIL"),
}


def main() -> int:
    steps = []
    pre = r27_precommit_unchanged()

    # ── 1. probe ────────────────────────────────────────────────
    steps.append(run_step("r30_probe"))
    sv = read("r30-source-verdict")
    cred = read("r30-credential-status")
    pverdict = sv.get("verdict", "UNKNOWN")

    # ── 2. 재현 확인 (credential 과 무관 — 항상 돌린다) ──────────
    steps.append(run_step("r30_reproduce"))
    repro = read("r30-reproduction")

    # ── 3·4. 수집 · coverage ────────────────────────────────────
    #   probe 가 막혔어도 두 단계를 **호출은 한다.** 각자 스스로 게이트를
    #   확인하고 NOT_STARTED 를 기록한다. 그래야 "얼마나 못 받았는지"가
    #   evidence 로 남는다 — 파일을 아예 안 만들면 그 사실을 잃는다.
    steps.append(run_step("r30_acquire"))
    acquired = read("r30-acquisition-progress")
    steps.append(run_step("r30_coverage"))
    coverage = read("r30-final-coverage")

    reexec = {}
    if sv.get("fullAcquisitionAllowed"):
        if coverage.get("r27AnalysisAllowed") and repro.get("reproductionPass"):
            # §18 — Founder 승인 기다리지 않고 같은 세션에서 즉시 재실행
            steps.append(run_step("r27_run"))
            steps.append(run_step("r27_verdict"))
            reexec = {"r27Verdict": read("r27-verdict"),
                      "r27Coverage": read("r27-liquidity-coverage"),
                      "r27SizeReproduction": read("r27-size-reproduction"),
                      "r27BmReference": read("r27-bm-reference"),
                      "r27BmVsSize": read("r27-bm-vs-size"),
                      "executed": True}
        else:
            reexec = {"executed": False,
                      "why": ("coverage gate 미달 — R27 분석 실행 금지(§17)"
                              if not coverage.get("r27AnalysisAllowed")
                              else "R25/R26 재현 실패 — 선행 해결 필요(§20)")}
    else:
        reexec = {"executed": False,
                  "why": f"probe gate 미통과({pverdict}) — 전체수집 시작 금지(§10)"}

    (RD / "r30-r27-reexecution-latest.json").write_text(
        json.dumps({"task": "R30", "r27PrecommitUnchanged": pre,
                    "reproduction": repro, **reexec},
                   ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    # ── 6. 최종 판정 ────────────────────────────────────────────
    r27v = (reexec.get("r27Verdict") or {}).get("verdict")
    if reexec.get("executed") and r27v:
        judgement = "PASS" if r27v != "SIZE_DATA_INSUFFICIENT" else "BLOCKED"
        reason = f"R30_R27_{r27v}"
    elif pverdict in REASONS:
        # 근본 원인이 credential·probe 면 그것을 말한다. coverage 미달은
        # 그 결과일 뿐이라 그쪽을 사유로 삼으면 원인을 흐린다(§17).
        judgement, reason = REASONS[pverdict]
    elif not coverage.get("r27AnalysisAllowed"):
        judgement, reason = "BLOCKED", "R30_COVERAGE_GATE_FAIL"
    else:
        judgement, reason = "BLOCKED", "R30_UNKNOWN"

    final = {
        "task": "R30",
        "taskId": "WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30",
        "judgement": judgement,
        "reasonClass": reason,
        "credentialStatus": cred.get("status"),
        "secretExposure": 0,
        "probeVerdict": pverdict,
        "probeFailedChecks": sv.get("failedChecks", []),
        "fullAcquisitionExecuted": bool(acquired.get("newlyAcquiredDays")),
        "requiredDays": coverage.get("requiredDays") or acquired.get("requiredDays"),
        "inheritedDays": coverage.get("daysInheritedFromR27")
        or acquired.get("inheritedR27Days"),
        "newlyAcquiredDays": acquired.get("newlyAcquiredDays", 0),
        "failedDays": acquired.get("failedDays", 0),
        "finalDayCoveragePct": coverage.get("dayCoveragePct"),
        "yearsMeetingThresholdCount": coverage.get("yearsMeetingThresholdCount"),
        "r27PrecommitUnchanged": pre["unchanged"],
        "r27PrecommitSha256": pre["sha256"],
        "reproductionPass": repro.get("reproductionPass"),
        "r27Reexecuted": reexec.get("executed", False),
        "r27Verdict": r27v,
        "executionCandidate": (reexec.get("r27Verdict") or {}).get(
            "executionCandidate", "NONE_PENDING_DATA"),
        "portfolioResearchEntryAllowed": bool(
            (reexec.get("r27Verdict") or {}).get("portfolioResearchEntryAllowed")),
        "steps": steps,
        "production": {"realMoneyStage": "REAL_MONEY_NOT_APPROVED",
                       "realOrders": 0, "broker": 0, "realAccount": 0,
                       "paidData": 0, "externalSend": 0, "deploy": 0,
                       "envOrToken": 0, "productionDbWrite": 0,
                       "legacy50d": "untouched", "newBmForward": "untouched",
                       "publicRepo": "untouched", "homepage": "untouched",
                       "scheduler": "untouched", "autoApply": "untouched",
                       "autoPublish": "untouched",
                       "krxWebAutomation": 0, "pykrxBulk": 0},
        "forbidden": {"portfolioOptimization": False, "newFactorResearch": False,
                      "newTaxonomy": False, "thresholdChanged": False},
    }
    (RD / "r30-final-verdict-latest.json").write_text(
        json.dumps(final, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    print(json.dumps({"judgement": judgement, "reasonClass": reason,
                      "probeVerdict": pverdict,
                      "credential": cred.get("status")}, ensure_ascii=False))
    return 0 if judgement == "PASS" else (9 if judgement == "BLOCKED" else 10)


if __name__ == "__main__":
    raise SystemExit(main())
