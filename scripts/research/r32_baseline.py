#!/usr/bin/env python3
"""R32 baseline 재현 gate — R25/R26/R31 수치가 그대로 나오는지 먼저 확인한다.

WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32

§11: 새 head-to-head 결과를 만들기 전에 기존 결과를 재현한다. 재현되지 않은
상태에서 R32 승자를 선언하지 않는다(BLOCKED / R25_R31_BASELINE_REPRODUCTION_FAILED).

── 무엇을 재현으로 볼 것인가 ──────────────────────────────────────
  새 계산기를 만들어 같은 숫자를 두 번 만드는 것은 재현이 아니라 복제다.
  R31 이 **정식 재실행**(r27_run → r27_verdict)으로 생성한 canonical evidence 를
  읽어 기대값과 대조한다. 그 evidence 는 R31 frozen dataset 위에서 나온 것이고
  hash 로 고정돼 있다.

── tolerance ──────────────────────────────────────────────────────
  기존 산출물은 소수 3자리로 반올림해 저장된다(round(x*100, 3)).
  그래서 표시 정밀도에 맞춘 절대오차 5e-4 를 쓴다. 반올림 표시값을 억지로
  맞추지 않으며, 통과시키려고 tolerance 를 키우지 않는다.

안전: 읽기·비교 전용. 네트워크 0. 재계산 0.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SRC = ROOT / "scripts" / "research"

TOL = 5e-4

EXPECTED = {
    "r25_36m": {"SIZE": 19.147, "BM": 15.010, "CONTROL": 9.418},
    "r31_base_36m": {"SIZE": 13.698, "BM": 12.193, "CONTROL": 7.467},
    "r31_cost100bp_36m": {"SIZE": 9.846, "BM": 9.510},
}


def _load(name):
    p = RD / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except ValueError:
        return None


def _close(a, b):
    return a is not None and b is not None and abs(float(a) - float(b)) <= TOL


def main() -> int:
    checks, details = {}, {}

    # ── 1. 동결 해시 ────────────────────────────────────────────────
    hashes = {n: hashlib.sha256((SRC / f"{n}.py").read_bytes()).hexdigest()
              for n in ("r27_precommit", "r31_precommit", "r32_precommit")}
    details["frozenHashes"] = hashes
    stitch = _load("r31-stitch-latest.json") or {}
    details["datasetManifestSha256"] = stitch.get("manifestSha256")
    checks["dataset manifest 3439dec9"] = str(
        stitch.get("manifestSha256", "")).startswith("3439dec9")
    checks["r27_precommit acee4288"] = hashes["r27_precommit"].startswith("acee4288")
    checks["r31_precommit d7fe3929"] = hashes["r31_precommit"].startswith("d7fe3929")

    # ── 2. R25/R26 정확 재현 (R27 이 이미 대조한 결과) ───────────────
    repro = _load("r27-size-reproduction-latest.json")
    if repro is None:
        checks["R25/R26 재현 산출물 존재"] = False
    else:
        blob = json.dumps(repro, ensure_ascii=False)
        checks["R25/R26 재현 산출물 존재"] = True
        checks["R25/R26 exactMatch=false 없음"] = '"exactMatch": false' not in blob
        details["reproArtifact"] = "r27-size-reproduction-latest.json"

    # ── 3. R25 raw 36M (필터 전) ────────────────────────────────────
    ba = _load("r27-before-after-latest.json")
    if ba is None:
        checks["before-after 산출물 존재"] = False
    else:
        checks["before-after 산출물 존재"] = True
        b36 = (ba.get("byHorizon") or ba).get("36M") or {}
        before = b36.get("BEFORE") or {}
        after = b36.get("AFTER") or {}
        got_before = {a: (before.get(a) or {}).get("meanTopAnnPct")
                      for a in ("SIZE", "BM", "CONTROL")}
        got_after = {a: (after.get(a) or {}).get("meanTopAnnPct")
                     for a in ("SIZE", "BM", "CONTROL")}
        details["r25RawObserved"] = got_before
        details["r31BaseObserved"] = got_after
        for a, exp in EXPECTED["r25_36m"].items():
            checks[f"R25 raw 36M {a} = {exp}"] = _close(got_before.get(a), exp)
        for a, exp in EXPECTED["r31_base_36m"].items():
            checks[f"R31 BASE 36M {a} = {exp}"] = _close(got_after.get(a), exp)

    # ── 4. R31 100bp ────────────────────────────────────────────────
    sc = _load("r27-sensitivity-cost-latest.json")
    if sc is None:
        checks["sensitivity-cost 산출물 존재"] = False
    else:
        checks["sensitivity-cost 산출물 존재"] = True
        cost = sc.get("cost") or {}
        got = {a: (cost.get(a) or {}).get("highCostTopAnnPct")
               for a in ("SIZE", "BM")}
        details["r31Cost100bpObserved"] = got
        for a, exp in EXPECTED["r31_cost100bp_36m"].items():
            checks[f"R31 100bp {a} = {exp}"] = _close(got.get(a), exp)
        sens = sc.get("sensitivity") or {}
        details["thresholds"] = {k: (sens.get(k) or {}).get("thresholdKrw")
                                 for k in ("LOW", "BASE", "HIGH")}
        checks["LOW/BASE/HIGH 정확"] = (
            details["thresholds"] == {"LOW": 25_000_000, "BASE": 125_000_000,
                                      "HIGH": 250_000_000})

    # ── 5. coverage gate ────────────────────────────────────────────
    cov = _load("r27-liquidity-coverage-latest.json") or {}
    details["coverage"] = {"yearsMeetingThreshold": cov.get("yearsMeetingThreshold"),
                           "minYearsCovered": cov.get("minYearsCovered"),
                           "gatePass": cov.get("coverageGatePass")}
    checks["R27 coverage gate PASS"] = bool(cov.get("coverageGatePass"))

    all_pass = all(checks.values())
    out = {"task": "R32", "phase": "baseline-reproduction",
           "allPass": all_pass,
           "verdict": "PASS" if all_pass else "BLOCKED",
           "reasonClass": None if all_pass else "R25_R31_BASELINE_REPRODUCTION_FAILED",
           "tolerance": TOL,
           "toleranceRule": "기존 산출물 표시 정밀도(소수 3자리)에 맞춘 절대오차",
           "expected": EXPECTED, "checks": checks, "details": details,
           "recomputed": False,
           "method": ("R31 정식 재실행이 만든 canonical evidence 를 읽어 대조한다. "
                      "새 계산기로 같은 숫자를 다시 만들지 않는다(복제 아님).")}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r32-baseline-repro-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"verdict: {out['verdict']} · allPass={all_pass}")
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
