#!/usr/bin/env python3
"""R30 §20 재현 검증 — R25/R26 신호가 그대로인지 **읽기 전용**으로 확인한다.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

§20 은 R27 tradability 분석 전에 R25/R26 재현을 다시 확인하라고 한다. 그런데
§38 은 기존 R7~R29 산출물 변경을 금지한다. r27_run.py 를 그냥 돌리면 R27 의
evidence 파일들이 덮어쓰인다 — coverage gate 가 아직 통과하지도 않았는데.

그래서 여기서는 **같은 계산을 다시 하되 R30 파일에만 쓴다.** R27 산출물은
읽기만 한다. coverage PASS 후의 정식 재실행은 §18 대로 r27_run → r27_verdict
가 담당한다.

기대값은 지시문에 고정된 숫자다(결과를 보고 정하지 않는다).

안전: 계산·읽기 전용. 네트워크 0. write 는 r30 evidence 하나.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

RD = Path(__file__).resolve().parents[2] / "reports" / "research"

# §20 — 사전 고정 기대값 (지시문 정본)
EXPECTED = {
    "SIZE_R25_SPREAD": {"12M": 35.336, "36M": 13.320, "60M": 10.752,
                        "84M": 9.063},
    "SIZE_R26_TOP": {"12M": 48.424, "36M": 19.147, "60M": 14.864},
    "BM_R26_TOP": {"12M": 28.082, "36M": 15.010, "60M": 12.799},
}
TOL = 1e-3          # 소수 셋째 자리까지 일치


def recompute():
    """R25/R26 분석기를 직접 돌려 앵커를 다시 만든다."""
    from r25_analysis import Analyzer as R25Analyzer
    from r25_analysis import agg as r25_agg
    from r26_analysis import Combo, arm_stats as r26_arm_stats

    out = {"SIZE_R25_SPREAD": {}, "SIZE_R26_TOP": {}, "BM_R26_TOP": {}}
    a25 = R25Analyzer()
    for h in (12, 36, 60, 84):
        st = r25_agg(a25.cohorts("SIZE_SMALL", h))
        out["SIZE_R25_SPREAD"][f"{h}M"] = (round(st["meanSpreadAnnPct"], 3)
                                           if st else None)
    del a25

    kc = Combo()
    for h in (12, 36, 60):
        for arm, key in (("SIZE_ALONE", "SIZE_R26_TOP"),
                         ("BM_ALONE", "BM_R26_TOP")):
            s = r26_arm_stats(kc.paired(h), arm)
            out[key][f"{h}M"] = round(s["meanTopAnnPct"], 3) if s else None
    del kc
    return out


def compare(got):
    rows, all_ok = {}, True
    for group, exp in EXPECTED.items():
        rows[group] = {}
        for h, want in exp.items():
            have = (got.get(group) or {}).get(h)
            ok = have is not None and abs(have - want) <= TOL
            all_ok = all_ok and ok
            rows[group][h] = {"expected": want, "recomputed": have,
                              "match": ok}
    return rows, all_ok


def stored_r27_claim():
    """R27 이 남긴 재현 판정도 함께 본다(읽기만)."""
    out = {}
    for name, key in (("size-reproduction", "SIZE"), ("bm-reference", "BM")):
        p = RD / f"r27-{name}-latest.json"
        if p.exists():
            try:
                d = json.loads(p.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                continue
            out[key] = {"r25": d.get("r25"), "r26": d.get("r26"),
                        "allExactMatch": d.get("allExactMatch")}
    return out


def main() -> int:
    try:
        got = recompute()
        err = None
    except Exception as e:                                   # noqa: BLE001
        got, err = {}, f"{type(e).__name__}: {e}"
    rows, ok = compare(got)
    out = {"task": "R30", "check": "R25/R26 reproduction (§20)",
           "readOnly": True,
           "r27ArtifactsMutated": False,
           "expectedFixedBeforeRun": True,
           "tolerance": TOL,
           "byGroup": rows,
           "recomputed": got,
           "storedR27Claim": stored_r27_claim(),
           "reproductionPass": ok,
           "error": err,
           "rule": ("재현 실패 시 R27 tradability 분석 전에 원인을 해결한다(§20).")}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r30-reproduction-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    print(json.dumps({"reproductionPass": ok, "error": err},
                     ensure_ascii=False))
    return 0 if ok else 8


if __name__ == "__main__":
    raise SystemExit(main())
