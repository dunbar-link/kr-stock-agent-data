#!/usr/bin/env python3
"""R33B validation spec — 성과를 보기 전에 동결한다.

WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B
SOURCE: WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A (8f8ea81)
CONTRACT: WABABA_EXECUTION_TRANSLATION_R33A_V1
          d869f445af1b8d8449c454e58ab2c0bb93bb5e6fcd61c147157c7305c0259b99

R33A 가 동결한 D_CLOSE_RANK_NEXT_SESSION_OPEN_V1 을
**누락 없는 163개 전체 cohort** 에 적용해 BM·SIZE_SMALL·CONTROL 을 재계산하고,
기존 R32 보수적 tie policy 로 미래 OOS 연구후보를 결정한다.

이 파일은 **선언만** 한다. 계산하지 않는다.

── 왜 engine 과 분리했는가 ─────────────────────────────────────────
  이 spec 의 hash 가 동결값이다. engine 버그를 고칠 때마다 hash 가 흔들리면
  "결과를 보고 기준을 바꾸지 않았다" 를 증명할 수 없다. R33A 와 같은 이유다.

안전: 선언·읽기 전용. 네트워크 0 · 성과계산 0.
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

TASK_ID = "WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B"
SOURCE_COMMIT = "8f8ea81"
SOURCE_CONTRACT_ID = "WABABA_EXECUTION_TRANSLATION_R33A_V1"
SOURCE_CONTRACT_HASH = (
    "d869f445af1b8d8449c454e58ab2c0bb93bb5e6fcd61c147157c7305c0259b99")
TRANSLATION_RULE = "D_CLOSE_RANK_NEXT_SESSION_OPEN_V1"

UPSTREAM = {
    "r27_precommit": "acee428846d358d8",
    "r31_precommit": "d7fe3929262fe8bd",
    "r32_precommit": "07edbe72b8ac75c2",
}
DATASET_MANIFEST_PREFIX = "3439dec9"
RESEARCH_BASE_COMMIT = "0431003"
R33_FEASIBILITY_COMMIT = "04beb59"

EXPECTED_DISTINCT_MISSING_DATES = 50
EXPECTED_TOTAL_COHORTS = 163

# ── API 예산 (§11) ────────────────────────────────────────────────────
HARD_MAX_CALLS = 250
MAX_REQUEST_RATE_PER_SEC = 2
MAX_RETRY_TRANSIENT = 3

SPEC = {
    "taskId": TASK_ID,
    "sourceCommit": SOURCE_COMMIT,
    "sourceContractId": SOURCE_CONTRACT_ID,
    "sourceContractHash": SOURCE_CONTRACT_HASH,
    "translationRule": TRANSLATION_RULE,
    "upstreamPrecommits": UPSTREAM,
    "datasetManifestPrefix": DATASET_MANIFEST_PREFIX,
    "researchBaseCommit": RESEARCH_BASE_COMMIT,
    "r33FeasibilityCommit": R33_FEASIBILITY_COMMIT,

    # ── 표본 정책 ────────────────────────────────────────────────────
    "performanceCohortPolicy": "FULL_163_ONLY",
    "expectedTotalCohorts": EXPECTED_TOTAL_COHORTS,
    "partial113PerformanceAllowed": False,
    "incompleteCohortPerformanceAllowed": False,
    "cohortExclusionAfterResults": "FORBIDDEN",

    # ── factor ──────────────────────────────────────────────────────
    "factorSet": ["BM", "SIZE_SMALL", "CONTROL"],
    "factorDefinitions": "R32 frozen — 변경 0",
    "rankingDirection": "R32 frozen — 변경 0",
    "quantileRule": "R32/R27 frozen — eligible>=200 decile, else quintile, 최상위 버킷",
    "selectionTieBreak": "ticker DESC (r27_analysis.cohort reverse=True) — 그대로 유지",
    "turnoverTieBreak": "ticker ASC (r32_engine.turnover_of) — 그대로 유지",
    "tieBreakUnificationAllowed": False,
    "newFactorCount": 0,
    "factorCombinationCount": 0,

    # ── 실행규칙 (R33A frozen 참조) ─────────────────────────────────
    "entryRule": "R33A frozen — D 다음 첫 공식 거래일 official open",
    "exitRule": "R33A frozen — entry+36 calendar months 이후 첫 공식 거래일 official open",
    "noFillRule": "R33A frozen — NO_VALID_OFFICIAL_OPEN_MEANS_NO_FILL",
    "cashRule": "R33A frozen — 0% nominal, 재분배 없음",
    "costRule": "R33A frozen — TURNOVER_BASED_ANNUALIZED_DRAG, round-trip",
    "costScenarios": [0, 25, 50, 100],
    "primaryCostSelection": "NONE_NEW",
    "alternativeEntryRules": 0,
    "alternativeExitRules": 0,
    "thresholdChanges": 0,
    "horizonChanges": 0,

    # ── 유동성 (변경 0) ─────────────────────────────────────────────
    "liquidityBaseKrw": 125_000_000,
    "liquidityLowKrw": 25_000_000,
    "liquidityHighKrw": 250_000_000,
    "liquidityWindow": "결정일 D 직전 20거래일",

    # ── 결정 ────────────────────────────────────────────────────────
    "decisionPolicy": "R32 frozen conservative tie policy (r32_decide)",
    "allowedDecisions": ["BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE",
                         "NO_EXECUTABLE_WINNER"],
    "priorBothSeparateAssumed": False,
    "postResultRuleChanges": 0,

    # ── 데이터 보완 ─────────────────────────────────────────────────
    "expectedDistinctMissingDates": EXPECTED_DISTINCT_MISSING_DATES,
    "localReuseFirst": True,
    "supplementalRoot": "_cache/r33b-supplemental-open",
    "r31FrozenCacheMutationAllowed": False,
    "r31ManifestMutationAllowed": False,
    "hardMaxCalls": HARD_MAX_CALLS,
    "maxRequestRatePerSec": MAX_REQUEST_RATE_PER_SEC,
    "maxRetryTransient": MAX_RETRY_TRANSIENT,
    "sourceBoundary": {
        "2010-01-04..2019-12-30": "KRX_OPENAPI",
        "2020-01-02..": "PUBLIC_DATA_PORTAL_FSC",
    },
    "marketsAllowed": ["KOSPI", "KOSDAQ"],
    "fullPeriodBackfillAllowed": False,
    "offPlanDateFetchAllowed": False,

    # ── gate ────────────────────────────────────────────────────────
    "performanceGateConditions": [
        "R33A_CONTRACT_HASH_VALID",
        "R33B_SPEC_HASH_FROZEN",
        "MISSING_DATE_PLAN_HASH_FROZEN",
        "SUPPLEMENTAL_DATE_COMPLETENESS_PASS",
        "COHORT_163_OF_163_COMPLETE",
        "DATA_INTEGRITY_MISSING_ZERO",
        "SAME_BAR_ZERO",
        "FUTURE_LEAKAGE_ZERO",
    ],
    "preGatePerformanceAccessAllowed": False,

    # ── 상태 ────────────────────────────────────────────────────────
    "runtimeActivationAllowed": False,
    "oosLedgerAllowed": False,
    "publicationAllowed": False,
    "realMoneyApproved": False,
    "paperOnly": True,
}


def canonical_json(obj=None) -> str:
    return json.dumps(obj if obj is not None else SPEC,
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def spec_hash(obj=None) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def upstream_hashes() -> dict:
    out = {}
    for n, pre in UPSTREAM.items():
        h = hashlib.sha256((SRC / f"{n}.py").read_bytes()).hexdigest()
        out[n] = {"sha256": h, "expectedPrefix": pre, "match": h.startswith(pre)}
    out["allMatch"] = all(v["match"] for v in out.values() if isinstance(v, dict))
    return out


def contract_valid() -> dict:
    import r33a_translation_precommit as C
    h = C.contract_hash()
    return {"contractHash": h, "expected": SOURCE_CONTRACT_HASH,
            "match": h == SOURCE_CONTRACT_HASH,
            "translationRule": C.TRANSLATION_RULE_ID,
            "ruleMatch": C.TRANSLATION_RULE_ID == TRANSLATION_RULE}


def main(argv=None) -> int:
    import datetime
    import zoneinfo
    now = datetime.datetime.now(
        zoneinfo.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    out = {"task": "R33B", "taskId": TASK_ID, "writtenBeforeResults": True,
           "generatedAt": now, "spec": SPEC, "specHash": spec_hash(),
           "specSerialization": "json sort_keys=True separators=(',',':') utf-8",
           "upstreamHashes": upstream_hashes(),
           "sourceContract": contract_valid(),
           "performanceAccess": {"performanceFunctionCalls": 0,
                                 "partial113PerformanceRuns": 0,
                                 "alternativeEntryPerformanceRuns": 0}}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r33b-validation-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"specHash": out["specHash"],
                      "upstreamAllMatch": out["upstreamHashes"]["allMatch"],
                      "contractMatch": out["sourceContract"]["match"],
                      "generatedAt": now}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
