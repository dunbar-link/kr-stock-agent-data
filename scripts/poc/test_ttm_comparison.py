"""test_ttm_comparison — TTM vs 연간 비교 데이터셋 계약/정책 검증 (조회 대시보드 PoC).

산출물(_cache/ttm-poc-output/ttm-comparison-full.json, fixture20.json)을 검증한다.
선행: python scripts/poc/build_ttm_comparison.py
실행: python scripts/poc/test_ttm_comparison.py  (exit 0 = 전부 PASS)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parents[2] / "_cache" / "ttm-poc-output"
PASS_LIKE = {"PASS", "PASS_WITH_TRANSITION_NOTE", "PASS_OFFICIAL_IR_CONFIRMED"}
_P = _F = 0


def check(name, cond, detail=""):
    global _P, _F
    if cond:
        _P += 1; print(f"  PASS  {name}")
    else:
        _F += 1; print(f"  FAIL  {name}  {detail}")


def main():
    full = json.loads((OUT / "ttm-comparison-full.json").read_text(encoding="utf-8"))
    fx = json.loads((OUT / "ttm-comparison-fixture20.json").read_text(encoding="utf-8"))
    print("=== test_ttm_comparison ===")

    # schema 필수 필드
    need = ["schemaVersion", "codeSetHash", "formulaVersionAnnual", "experimentVersion",
            "annualFinancialYear", "ttmAsOfQuarter", "includedCount", "rows"]
    check("schema 필수 필드", all(k in full for k in need), str([k for k in need if k not in full]))

    rows = full["rows"]
    # 포함 정책: rows 전부 PASS 계열
    check("포함정책: rows 전부 PASS 계열", all(r["qualityStatus"] in PASS_LIKE for r in rows))
    # 제외 정책: WARNING/BLOCKED 미포함
    check("제외정책: WARNING/BLOCKED 미포함", not any(r["qualityStatus"] in
          ("WARNING_EXTERNAL_CONFIRMATION", "BLOCKED_DATA_INCONSISTENCY") for r in rows))
    check("includedCount == len(rows)", full["includedCount"] == len(rows))

    # 필드 분리: annual(공식) vs experimental(접두어)
    r0 = rows[0]
    check("annual 공식필드", all(k in r0["annual"] for k in ("combinedRank", "returnOnCapital", "ebitEv")))
    check("experimental 접두어 필드", all(k in r0["ttmExperiment"] for k in
          ("experimentalCombinedRank", "experimentalReturnOnCapital", "experimentalEbitEv")))
    check("공식/실험 필드명 혼동 없음(experimental 접두어)",
          all(k.startswith("experimental") or k.startswith("latest") or k in
              ("revenue", "operatingIncome", "netIncome", "excludeReason", "quarterlyOperatingIncome", "quarterlyRevenue")
              for k in r0["ttmExperiment"]))

    # experimental 순위 유효성: 산출된 종목은 양수 rank, 결정론(정렬 규칙)
    ranked = [r for r in rows if r["ttmExperiment"]["experimentalCombinedRank"]]
    check("experimental rank 양수", all(r["ttmExperiment"]["experimentalCombinedRank"] > 0 for r in ranked))
    check("experimentalRanked <= included", full["experimentalRankedCount"] <= full["includedCount"])

    # EBIT 정의 대응: experiment EBIT = TTM operatingIncome (동일 계정)
    check("EBIT 정의 대응(주석)", "영업이익" in full["experimentVersion"] or "EBIT" in full["experimentVersion"])

    # fixture 20 + 제외예시(WARNING+BLOCKED)
    check("fixture rows 20", len(fx["rows"]) == 20)
    check("fixture 제외예시 WARNING+BLOCKED 포함",
          any(e["qualityStatus"] == "WARNING_EXTERNAL_CONFIRMATION" for e in fx["excludedExamples"]) and
          any(e["qualityStatus"] == "BLOCKED_DATA_INCONSISTENCY" for e in fx["excludedExamples"]))
    check("disclaimer 존재(투자추천 아님)", "투자 추천 아님" in full["disclaimer"])

    print(f"\n=== {_P} passed, {_F} failed ===")
    return 0 if _F == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
