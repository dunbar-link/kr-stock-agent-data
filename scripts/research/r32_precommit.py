#!/usr/bin/env python3
"""R32 사전 고정 (precommit) — BM vs SIZE head-to-head. 결과를 보기 전에 전부 확정한다.

WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32
SOURCE_TASK: WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

── 이 파일을 지금 쓰는 이유 ────────────────────────────────────────
  R32 의 factor 결과는 아직 **한 줄도 계산되지 않았다**. head-to-head 지표
  (paired 차이·turnover·drawdown·attrition·overlap·불확실성)를 만들기 전에
  승자 규칙을 박아야 "결과를 보고 기준을 고쳤다" 는 의심이 원천적으로 불가능하다.

  R31 이 이미 보고한 BASE 수치(SIZE 13.698 / BM 12.193 / CONTROL 7.467)는
  **이 파일이 만드는 규칙의 입력이 아니다** — 아래 winner rule 은 그 숫자가
  어느 쪽이든 동일하게 작동하는 조건문이고, 지시문이 제공한 문구를 옮긴 것이다.

── ★ Rule B 복원 결과 (§6) ────────────────────────────────────────
  R27 precommit 의 Rule B 는 `NEXT_TASK_RULE["B"]` 이고 원문은
    "SIZE 생존 + BM 도 강함 → BM vs SIZE frozen head-to-head (동일 execution rules)"
  이다. 이것은 **다음 작업을 지정하는 규칙**이지 승자를 고르는 규칙이 아니다.
  `EXECUTION_CANDIDATE` 가 비교 criteria 11개와 "notReturnOnly" 를 주지만
  각 criterion 의 판정 threshold·결합 규칙·tie 처리·실패 조건이 없다.

  → 즉 Rule B 는 machine-readable 승자 결정조건을 제공하지 않는다.
    §6 단서에 따라 **결과 계산 전에만** 허용되는 §8 보수적 tie policy 를
    여기서 동결한다. R27 의 조건은 하나도 바꾸지 않고 그대로 인용한다.

── 하지 않는 것 ───────────────────────────────────────────────────
  · R27/R31 threshold·gate·기간·시장 변경 0 (정본에서 읽기만 한다)
  · 새 factor·EY/Quality 재탐색·BM×SIZE 결합·blend·composite·weight 최적화 0
  · top-N·보유기간·리밸런싱·signal lag·return horizon 변경 0
  · 결과 확인 후 이 파일 수정 0 (구현 버그 수정은 코드에서 한다)

사용: python scripts/research/r32_precommit.py
부작용: reports/research/r32-precommit-latest.json 1개 write. 네트워크 0.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
HERE = Path(__file__).resolve().parent

TASK_ID = "WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32"
SOURCE_TASK = "WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31"

# ── 정본에서 읽는다(복제 금지) ───────────────────────────────────────
from r27_precommit import (  # noqa: E402
    BM as BM_DEFINITION,
    SIZE as SIZE_DEFINITION,
    CAPITAL, COST, EXECUTION_CANDIDATE, HORIZONS,
    NEXT_TASK_RULE, PRIMARY_GATE, QUALIFICATION, SENSITIVITY, SUSPENSION,
)

R27_SHA256 = hashlib.sha256((HERE / "r27_precommit.py").read_bytes()).hexdigest()
R31_SHA256 = hashlib.sha256((HERE / "r31_precommit.py").read_bytes()).hexdigest()

# ── §6 Rule B 복원 기록 ─────────────────────────────────────────────
RULE_B = {
    "source": "scripts/research/r27_precommit.py :: NEXT_TASK_RULE['B']",
    "verbatim": NEXT_TASK_RULE["B"],
    "isNextTaskRule": True,
    "isWinnerDecisionRule": False,
    "providesWinnerConditions": False,
    "whatItProvides": ["다음 작업 지정", "동일 execution rules 요구"],
    "whatItDoesNotProvide": [
        "primary metric 의 승자 threshold", "tie 처리", "BOTH_SEPARATE 조건",
        "NO_EXECUTABLE_WINNER 조건", "criterion 간 결합 규칙",
    ],
    "executionCandidateCriteria": EXECUTION_CANDIDATE["criteria"],
    "executionCandidateOptions": EXECUTION_CANDIDATE["options"],
    "notReturnOnly": EXECUTION_CANDIDATE["notReturnOnly"],
    "bothSeparateMeaning": EXECUTION_CANDIDATE["bothSeparateMeaning"],
    "consequence": ("Rule B 가 승자 조건을 주지 않으므로 §6 단서에 따라 §8 보수적 "
                    "tie policy 를 결과 계산 전에 추가한다. R27 조건 변경 0."),
}

# ── 동일 실행조건 (전부 R25/R26/R27 정본 그대로) ─────────────────────
EXECUTION_CONTRACT = {
    "factors": ["BM", "SIZE_SMALL"],
    "bmDefinition": BM_DEFINITION,
    "sizeDefinition": SIZE_DEFINITION,
    "factorDefinitionsChangedByR32": False,
    "rankingDirection": {"BM": 1, "SIZE_SMALL": 1},
    "rankingUniverse": ("R26/R27 공유 universe — BM·SIZE 가 동시에 유효한 종목만. "
                        "eligible < 100 인 결정일은 제외(R27 r27_analysis._shared_universe)."),
    "rebalanceDates": "PIT 스냅샷 월별 결정일 (R25/R26/R27 과 동일)",
    "signalLag": ("factor 는 결정일 스냅샷. 유동성은 결정일 **직전** 20거래일만 "
                  "(결정일 당일·이후 미사용 → look-ahead 0)."),
    "holdingPeriodMonths": HORIZONS["primary"],
    "allHorizons": HORIZONS["all"],
    "topSelection": ("분위 top bucket. eligible >= 200 이면 10분위, 아니면 5분위 "
                     "(r27_analysis.cohort — 변경 0)."),
    "weighting": "동일가중 (R25/R26/R27 동일)",
    "costAssumptions": {"canonicalPrimary": COST, "stressBps": [0, 25, 50, 100]},
    "cashTreatment": ("코호트 방식이라 현금 잔고 모델이 없다. cash drag 는 "
                      "구조적으로 0 이며 '측정했더니 0' 이 아니다 — 그대로 표기한다."),
    "integerShareTreatment": ("코호트 방식이라 정수주 제약이 없다. 실제 체결 단위 "
                              "효과는 이번 비교의 대상이 아니며 없는 것을 있다고 하지 않는다."),
    "delistingTreatment": "상폐 제외 금지. canonical UNKNOWN_RECOVERY 유지(R25 엔진).",
    "missingDataTreatment": ("MISSING ≠ 0. 유동성 결측은 tradable 판정을 받을 수 없고 "
                             "0 으로 채우지 않는다."),
    "positionCountSource": "동일 universe·동일 분위 규칙에서 자동 결정(임의 top-N 지정 0)",
}

# ── 유동성 (BASE 만 primary) ─────────────────────────────────────────
LIQUIDITY = {
    "primary": "BASE_20D_MEDIAN_TRADE_VALUE_125M",
    "primaryThresholdKrw": PRIMARY_GATE["thresholdKrw"],
    "primaryGateConditions": PRIMARY_GATE["conditions"],
    "sensitivityKrw": {k: v["thresholdKrw"] for k, v in SENSITIVITY.items()
                       if isinstance(v, dict) and "thresholdKrw" in v},
    "sensitivityRole": ("LOW/HIGH 는 robustness 표시 전용이다. 승자 선택 기준을 "
                        "바꾸는 데 쓰지 않는다(R27 §26 그대로)."),
    "thresholdChangeAllowed": False,
    "capitalKrw": CAPITAL["totalKrw"],
    "orderPerNameKrw": CAPITAL["orderPerNameKrw"],
    "newAumAssumption": 0,
    "suspension": SUSPENSION["status"],
}

# ── 데이터셋 ────────────────────────────────────────────────────────
DATASET = {
    "name": "R31_FROZEN_STITCHED_SNAPSHOT",
    "manifestShaPrefixExpected": "3439dec9",
    "range": ["2010-01-04", "2026-07-31"],
    "sourceBoundary": {"2010-01-04..2019-12-30": "KRX_OFFICIAL_OPEN_API",
                       "2020-01-02..": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"},
    "dataEnd": "2026-07-31",
    "newCollection": 0, "newApiCalls": 0, "webScraping": 0, "paidData": 0,
    "storageClass": "LOCAL_ONLY",
}

# ── §10 평가기간 ────────────────────────────────────────────────────
EVALUATION_PERIOD = {
    "rule": ("primary period = 2010년 첫 eligible signal date ~ 2026-07-31 스냅샷에서 "
             "**완결된** 마지막 forward-return cohort. 미완결 cohort 포함 금지."),
    "startBasis": "2010 — R31 공식 liquidity coverage 가 90%+ 로 확보된 첫 해",
    "incompleteCohortRule": "endDate 가 스냅샷 마지막 결정일을 넘는 cohort 는 제외",
    "pre2010": ("2007~2009 는 공식 liquidity coverage 불완전(49.49% / 0% / 0%). "
                "primary 승자 판정에 사용 금지. descriptive sensitivity 로만 표시하고 "
                "missing 을 0 으로 채우지 않으며 유리한 factor 에만 포함하지 않는다."),
    "commonMaskRequired": True,
    "commonMaskRule": ("BM·SIZE·CONTROL 세 팔이 **같은 결정일·같은 universe** 에서 "
                       "동시에 성립할 때만 표본에 넣는다(R27 R27.paired 계약). "
                       "factor 별로 다른 기간을 쓰지 않는다."),
}

# ── §14 기간분할 (결과 확인 전 고정) ─────────────────────────────────
SUBPERIODS = [
    {"name": "2010-2014", "start": "2010-01-01", "end": "2014-12-31"},
    {"name": "2015-2019", "start": "2015-01-01", "end": "2019-12-31"},
    {"name": "2020-END", "start": "2020-01-01", "end": "2099-12-31"},
]
ROLLING = {"windowsMonths": [36, 60],
           "note": ("return horizon 과 rolling window 가 같은 개념이면 중복 통계를 "
                    "새 근거처럼 부풀리지 않는다.")}

# ── §16 극단값 적대검증 ──────────────────────────────────────────────
CONTRIBUTOR_REMOVAL = {
    "topN": [1, 3, 5],
    "removeBestSignalYear": True,
    "removeWorstSignalYear": True,
    "leaveOneYearOut": True,
    "perMarketTopRemoval": True,
    "delistedSensitivityOnly": ("상폐 종목을 primary 데이터에서 제거하지 않는다. "
                                "제거는 sensitivity 표시 전용이다."),
}

MARKET_SPLIT = {"markets": ["KOSPI", "KOSDAQ"],
                "regimeDefinition": ("기존 정본에 regime 정의가 없다. 결과를 보고 regime "
                                     "경계를 만들지 않는다. 새 regime threshold 는 승자 "
                                     "결정에 사용하지 않고 descriptive 로만 표시한다.")}

# ── §19 불확실성 ────────────────────────────────────────────────────
UNCERTAINTY = {
    "method": "paired difference on matched cohorts",
    "overlapHandling": ("겹치는 장기 forward return 을 독립 표본으로 오인하지 않는다. "
                        "우선순위 ① 기존 R26 non_overlapping cohort "
                        "② 기존 R26 moving_block/boot_summary(block bootstrap)."),
    "blockRule": "block 단위는 기존 holding/return 계약(H개월)을 보존한다.",
    "blockSizeChangeAfterResults": False,
    "reported": ["pairedMeanDiff", "pairedMedianDiff", "ci95", "winRate",
                 "cohortCount", "nonOverlappingCount", "blockDefinition",
                 "standardError"],
    "pValueAloneDecides": False,
}

# ══════════════ §8 보수적 tie policy (Rule B 결손 보완) ══════════════
#   지시문 §8 문구를 그대로 옮긴다. 새 margin·새 cut-off 를 만들지 않는다.
WINNER_RULE = {
    "appliesBecause": "Rule B 가 tie/winner 결정을 제공하지 않음(§6 단서)",
    "frozenBeforeAnyFactorResult": True,
    "step1": ("BM 과 SIZE 중 정확히 하나만 기존 실행가능성 gate 를 통과하면 "
              "그 factor 를 PRIMARY 로 선택한다."),
    "step2": "둘 다 기존 실행가능성 gate 를 통과하지 못하면 NO_EXECUTABLE_WINNER.",
    "step3": ("둘 다 통과하면 동일 날짜·동일 eligible universe 의 paired comparison 을 "
              "수행한다."),
    "soloPrimaryConditions": [
        "BASE canonical 비용가정에서 순성과 우위",
        "paired uncertainty interval 이 0 을 같은 방향으로 벗어남",
        "100bp stress 에서 승패 방향이 뒤집히지 않음",
        "기존 Rule B 의 기간·시장·top3 제거 검증을 위반하지 않음",
        "기존 execution fragility gate 를 위반하지 않음",
    ],
    "soloPrimaryRequiresAll": True,
    "tieOutcome": "BOTH_SEPARATE",
    "tieMeaning": ("혼합 ranking 아님 · 결합 portfolio 아님 · 자본배분 승인 아님 · "
                   "실주문 승인 아님. 두 factor 를 별도 연구 후보로 유지한다는 뜻."),
    "noNewMargin": ("새로운 임의 우위 margin 을 만들지 않는다. 운영지표가 엇갈리고 "
                    "기존 Rule B 에 판정 threshold 가 없으면 새 cut-off 를 만들지 말고 "
                    "BOTH_SEPARATE 로 보수 판정한다."),
    "executionGateSourceKeys": sorted(QUALIFICATION.keys()),
    "executionGateSource": ("R27 QUALIFICATION 의 실행가능성 조건 — 기존 gate 를 그대로 "
                            "쓰고 R32 에서 새로 만들지 않는다."),
}

DECISION_ENUM = ["BM_PRIMARY", "SIZE_PRIMARY", "BOTH_SEPARATE",
                 "NO_EXECUTABLE_WINNER"]

# ── §21 거짓 승자 방지 ──────────────────────────────────────────────
FALSE_WINNER_BLOCKERS = [
    "서로 다른 평가기간", "서로 다른 universe", "서로 다른 liquidity mask",
    "서로 다른 비용", "서로 다른 position count", "baseline 재현 실패",
    "incomplete forward cohort 포함", "overlapping sample 을 독립 표본으로 오인",
    "특정 시장에만 성과 의존", "top3 제거 후 우위 소멸", "100bp 에서 순위 반전",
    "frozen spec 변경", "source boundary 이상", "survivorship leakage",
    "future leakage", "현재 상장목록 사용", "raw data divergence",
    "결과 확인 후 rule 변경",
]

FORBIDDEN = [
    "새 factor", "EY 재검토", "QUALITY/PROFITABILITY 재탐색", "BM×SIZE 결합",
    "factor blend", "composite score", "factor weight 최적화", "새 universe",
    "새 market", "새 threshold", "BASE 125,000,000원 변경", "LOW/HIGH 값 변경",
    "top-N 변경", "holding period 변경", "rebalance frequency 변경",
    "signal lag 변경", "return horizon 변경", "비용을 결과에 맞춰 선택",
    "유리한 시작일·종료일 선택", "결과 확인 후 subperiod 재구성",
    "결과 확인 후 승자 조건 수정", "현재 상장기업으로 과거 universe 필터",
    "상폐기업 제거", "close × volume 거래대금 대체", "KRX 재수집",
    "공공데이터포털 재수집", "웹 scraping", "pykrx 우회", "외부 데이터 구매",
]

BASELINE_REPRODUCTION = {
    "r25_36m": {"SIZE": 19.147, "BM": 15.010, "CONTROL": 9.418},
    "r31_base_36m": {"SIZE": 13.698, "BM": 12.193, "CONTROL": 7.467},
    "r31_cost100bp_36m": {"SIZE": 9.846, "BM": 9.510},
    "toleranceRule": ("기존 test/report 계약의 정밀도를 따른다. 반올림 표시값을 "
                      "억지로 맞추지 않는다."),
    "onFailure": "BLOCKED / R25_R31_BASELINE_REPRODUCTION_FAILED",
}

PRODUCTION = {
    "realMoneyStage": "REAL_MONEY_NOT_APPROVED", "paperOnly": True,
    "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
    "externalSend": 0, "deploy": 0, "envOrToken": 0, "productionDbWrite": 0,
    "publicRepo": "READ_ONLY", "legacy50d": "untouched", "homepage": "untouched",
    "scheduler": "untouched", "newFactorCount": 0, "combinationAllowed": False,
}


def build():
    return {
        "task": "R32", "taskId": TASK_ID, "sourceTask": SOURCE_TASK,
        "writtenBeforeResults": True,
        "writtenBeforeResultsEvidence": (
            "이 파일 작성 시점에 R32 head-to-head 지표(paired 차이·turnover·drawdown·"
            "attrition·overlap·불확실성)는 한 건도 계산되지 않았다. winner rule 은 "
            "지시문 §8 문구를 그대로 옮긴 것이며 어느 factor 가 이기든 동일하게 작동한다."),
        "researchQuestion": (
            "R31 이 확정한 동일 tradable universe·동일 BASE 유동성·동일 보유/리밸런싱/"
            "비용 규칙에서 BM 과 SIZE 중 실행 후보로 우선할 factor 가 명확히 존재하는가?"),
        "ruleB": RULE_B,
        "frozenHashes": {"r27_precommit": R27_SHA256, "r31_precommit": R31_SHA256},
        "executionContract": EXECUTION_CONTRACT,
        "liquidity": LIQUIDITY,
        "dataset": DATASET,
        "evaluationPeriod": EVALUATION_PERIOD,
        "subperiods": SUBPERIODS,
        "rolling": ROLLING,
        "contributorRemoval": CONTRIBUTOR_REMOVAL,
        "marketSplit": MARKET_SPLIT,
        "uncertainty": UNCERTAINTY,
        "winnerRule": WINNER_RULE,
        "decisionEnum": DECISION_ENUM,
        "falseWinnerBlockers": FALSE_WINNER_BLOCKERS,
        "baselineReproduction": BASELINE_REPRODUCTION,
        "forbidden": FORBIDDEN,
        "production": PRODUCTION,
        "notMeasuredHonestly": [
            "NAV-level maximum drawdown — 기존 pipeline 에 포트폴리오 NAV 시계열이 "
            "없다. 새 backtester 를 만들지 않는다(§22). cohort-level 수익 시계열의 "
            "낙폭만 proxy 로 산출하고 NAV drawdown 이라고 부르지 않는다.",
            "cash drag / 정수주 효과 — 코호트 방식이라 구조적으로 부재. 0 으로 "
            "측정된 것이 아니라 모델에 없는 것이다.",
        ],
        "immutability": ("결과 확인 후 이 spec 을 수정하지 않는다. 구현 버그 수정은 "
                         "허용하되 threshold·기간·시장·winner rule 변경은 금지한다."),
    }


def main() -> int:
    spec = build()
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r32-precommit-latest.json"
    p.write_text(json.dumps(spec, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    src_sha = hashlib.sha256((HERE / "r32_precommit.py").read_bytes()).hexdigest()
    print(f"[r32] saved {p.name}")
    print(f"[r32] r32_precommit.py sha256 = {src_sha}")
    print(f"[r32] r27_precommit.py sha256 = {R27_SHA256}")
    print(f"[r32] r31_precommit.py sha256 = {R31_SHA256}")
    print(f"[r32] Rule B provides winner conditions = "
          f"{RULE_B['providesWinnerConditions']} → §8 tie policy frozen")
    print(f"[r32] BASE {PRIMARY_GATE['thresholdKrw']} · horizon "
          f"{HORIZONS['primary']}M · decision enum {DECISION_ENUM}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
