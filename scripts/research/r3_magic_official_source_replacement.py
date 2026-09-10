#!/usr/bin/env python3
"""R3 — 마법공식 시장 snapshot 의 공식 source 대체 GO/NO-GO.

WABABA-MAGIC-FORMULA-PYKRX-WEB-DEPENDENCY-AND-OFFICIAL-SOURCE-GO-NOGO-R3
SOURCE_COMMIT 813df94 (R2 · CD007 · DIRECT_CAUSAL)

질문 하나: Magic Signal 이 pykrx **웹 로그인**으로 얻던 시장 snapshot 을,
이미 승인된 공식 read-only source 로 **전략·universe·ranking·거래규칙을
하나도 바꾸지 않고** 정확히 대체할 수 있는가.

이 모듈은 결과를 보기 전에 계약을 동결한다. 성과·수익률을 입력으로 쓰지 않는다.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[2]
SCR = ROOT / "scripts"
RD = ROOT / "reports" / "research"
sys.path.insert(0, str(SCR))

TASK = "WABABA-MAGIC-FORMULA-PYKRX-WEB-DEPENDENCY-AND-OFFICIAL-SOURCE-GO-NOGO-R3"
SOURCE_COMMIT = "813df94"
CONTRACT_ID = "WABABA_MAGIC_OFFICIAL_MARKET_SOURCE_R3_V1"


# ══════════════════════════════════════════════════════════════════════
# §7 Active dependency — Magic Signal 이 KRX 에서 **실제로** 소비하는 필드
#     근거는 전부 저장소 소스 실측이다(추측 0).
# ══════════════════════════════════════════════════════════════════════
FIELD_MATRIX = [
    {"normalized": "symbol", "sourceCall": "stock.get_market_ticker_list",
     "dtype": "str(6)", "unit": "-", "nullable": False,
     "usedInRanking": True, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["universe key", "build_item", "ranking join"],
     "evidence": "build_market_snapshot.get_market_frame base frame"},
    {"normalized": "price", "sourceCall": "stock.get_market_ohlcv_by_ticker (종가)",
     "dtype": "int", "unit": "KRW", "nullable": False,
     "usedInRanking": True, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["EV", "체결가·평가", "marketCapBelow/price gate"],
     "evidence": "safe_get_ohlcv docstring: 공식 마법공식 체결가·평가에 필수"},
    {"normalized": "marketCap", "sourceCall": "stock.get_market_cap_by_ticker (시가총액)",
     "dtype": "int", "unit": "억원(×1e8) 로 정규화", "nullable": False,
     "usedInRanking": True, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["EV = 시총 + 총부채 - 현금"],
     "evidence": "safe_get_market_cap docstring + build_magic_formula_fund evMethod"},
    {"normalized": "corpName", "sourceCall": "stock.get_market_ticker_name",
     "dtype": "str", "unit": "-", "nullable": False,
     "usedInRanking": False, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["financeNameKeywords 제외(증권·은행·지주·홀딩스·캐피탈·보험·파이낸셜·카드)"],
     "evidence": "build_magic_formula_fund CONFIG['financeNameKeywords']"},
    {"normalized": "marketName", "sourceCall": "literal KOSPI/KOSDAQ",
     "dtype": "str", "unit": "-", "nullable": False,
     "usedInRanking": False, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["universe 시장 구분"],
     "evidence": "get_market_frame merged['marketName'] = market"},
    {"normalized": "industryName",
     "sourceCall": "stock.get_market_sector_classifications",
     "dtype": "str", "unit": "-", "nullable": True,
     "usedInRanking": False, "usedInExclusion": True, "statusOnly": False,
     "required": True,
     "consumers": ["financeIndustries 제외(금융·기타금융·증권·은행·보험)", "utilities 제외"],
     "evidence": "build_magic_formula_fund L288 업종 제외 · L326/L328 excluded 카운터"},
    # ── 아래는 랭킹 입력이 아니다. 카나리아(응답 정상성 신호)로만 쓰인다.
    {"normalized": "PER", "sourceCall": "stock.get_market_fundamental_by_ticker",
     "dtype": "float", "unit": "배", "nullable": True,
     "usedInRanking": False, "usedInExclusion": False, "statusOnly": True,
     "required": False,
     "consumers": ["universe row 표시", "데이터품질 카나리아"],
     "evidence": "safe_get_fundamental docstring: 공식 마법공식은 이 값을 쓰지 않는다"},
    {"normalized": "PBR", "sourceCall": "stock.get_market_fundamental_by_ticker",
     "dtype": "float", "unit": "배", "nullable": True,
     "usedInRanking": False, "usedInExclusion": False, "statusOnly": True,
     "required": False, "consumers": ["표시", "collapse guard"],
     "evidence": "동일 docstring"},
    {"normalized": "divYield", "sourceCall": "stock.get_market_fundamental_by_ticker (DIV)",
     "dtype": "float", "unit": "%", "nullable": True,
     "usedInRanking": False, "usedInExclusion": False, "statusOnly": True,
     "required": False, "consumers": ["표시"], "evidence": "동일 docstring"},
]

REQUIRED_FIELDS = tuple(f["normalized"] for f in FIELD_MATRIX if f["required"])
CANARY_FIELDS = tuple(f["normalized"] for f in FIELD_MATRIX if f["statusOnly"])

# DART 에서 오는 값(EBIT·총부채·현금·유동자산/부채·ppe 등)은 KRX 교체 대상이 아니다.
NON_KRX_INPUTS = ("ebit", "totalLiabilities", "cash", "currentAssets",
                  "currentLiabilities", "ppe", "ROE", "opMargin")


# ══════════════════════════════════════════════════════════════════════
# §8 Source replacement precommit — 공식 API 결과를 보기 전에 동결
# ══════════════════════════════════════════════════════════════════════
SOURCE_PRIORITY = [
    {"rank": 1, "name": "KRX_OPEN_API_DAILY",
     "detail": "기존 승인 KRX OPEN API 유가증권·코스닥 일별매매정보",
     "credential": "KRX_OPENAPI_AUTH_KEY (기존 header 경로)",
     "condition": "same-day publication + frozen field parity 통과 시"},
    {"rank": 2, "name": "DATA_GO_KR_DAILY",
     "detail": "기존 공공데이터포털 공식 일별 source",
     "credential": "DATA_GO_KR_SERVICE_KEY (기존 경로)",
     "condition": "15:40 Signal 시각에 당일 데이터 가용 + parity 통과 시"},
    {"rank": 3, "name": "NONE", "detail": "fallback 없음"},
]

CONTRACT = {
    "contractId": CONTRACT_ID, "task": TASK, "sourceCommit": SOURCE_COMMIT,
    "fieldMatrix": FIELD_MATRIX,
    "requiredFields": list(REQUIRED_FIELDS),
    "canaryFields": list(CANARY_FIELDS),
    "nonKrxInputs": list(NON_KRX_INPUTS),
    "sourcePriority": SOURCE_PRIORITY,
    "PYKRX_WEB_ALLOWED": False,
    "MULTI_SOURCE_ROW_MIXING": False,
    "SOURCE_SELECTION_BY_RETURN": False,
    "STRATEGY_CHANGE_ALLOWED": False,
    "UNIVERSE_CHANGE_ALLOWED": False,
    "FINANCIAL_INPUT_CHANGE_ALLOWED": False,
    "RANKING_CHANGE_ALLOWED": False,
    "SIGNAL_DATE_CHANGE_ALLOWED": False,
    "REQUIRED_DATE": "current actual KRX completed session",
    "SOURCE_READY_DEADLINE": "기존 15:40 Signal 과 15:45 Dry Run 사이 안전 완료시각",
    "BOUNDED_READINESS_ATTEMPTS": 4,
    "BOUNDED_READINESS_WINDOW_SEC": 180,
    "UNBOUNDED_POLLING": False,
    "AUTH_FAILURE_RETRY": False,
    "RETRY_429": False,
    "RETRY_5XX_MAX": 2,
    "KRX_OPEN_API_CALLS_MAX": 30,
    "DATA_GO_KR_CALLS_MAX": 30,
    "TOTAL_NETWORK_CALLS_MAX": 60,
    "rawResponse": "LOCAL_ONLY_GITIGNORED_NO_REMOTE_UPLOAD",
    # §10 검증일 — 결과 보기 전 고정
    "PRIMARY_PARITY_DATE": "2026-09-04",
    "ADDITIONAL_FIXED_DATES": ["2026-08-03", "2026-08-31", "2026-09-01",
                               "2026-09-02", "2026-09-03"],
    "CURRENT_AVAILABILITY_DATE": "2026-09-10",
    "dateSubstitutionAllowed": False,
}

# §11 row-level 필수 gate (필수필드 한정)
ROW_PARITY_GATES = {
    "signalEligibleTickerCoveragePct": 100.0,
    "closeExactMatchPct": 100.0,
    "marketCapExactMatchPct": 100.0,
    "marketClassificationExactPct": 100.0,
    "corpNameResolvablePct": 100.0,
    "industryNameResolvablePct": 100.0,
    "duplicateKeyCount": 0,
    "invalidNumericCount": 0,
    "currentListingFilterUsed": 0,
    "survivorshipFilterUsed": 0,
    "priceTimesVolumeProxyUsed": 0,
    "closeTimesSharesAsMarketCapUsed": 0,
}

# §13 end-to-end signal package 필수 gate
PACKAGE_PARITY_GATES = {
    "top10ExactCount": 10,
    "top10OrderExact": True,
    "fullRankingEligibleSetExactPct": 100.0,
    "fullRankingOrderExactPct": 100.0,
    "actionSetExactPct": 100.0,
    "exclusionReasonTotalsExact": True,
    "semanticHashExact": True,
    "unexplainedDifferenceCount": 0,
}

TOLERANCE_WIDENING_AFTER_RESULTS = False

DECISION_ENUM = ["OFFICIAL_SOURCE_REPLACEMENT_GO",
                 "OFFICIAL_SOURCE_REPLACEMENT_NO_GO"]

MANDATORY_GATES = [
    "G1_required_field_set_recoverable",
    "G2_official_source_provides_all_required_fields",
    "G3_row_level_parity_all_gates",
    "G4_security_universe_parity",
    "G5_end_to_end_package_parity",
    "G6_same_day_structural_availability",
    "G7_no_pykrx_web_login_in_active_path",
    "G8_no_strategy_universe_ranking_change",
    "G9_bounded_readiness_implementable",
    "G10_minimal_adapter_feasible",
]

READINESS_STATUS_ENUM = [
    "OFFICIAL_SOURCE_READY",
    "OFFICIAL_SOURCE_NOT_READY_BOUNDED",
    "OFFICIAL_SOURCE_AUTH_BLOCKED",
    "OFFICIAL_SOURCE_RATE_LIMITED",
    "OFFICIAL_SOURCE_SCHEMA_BLOCKED",
    "OFFICIAL_SOURCE_DATA_INTEGRITY_BLOCKED",
]

# §17 누락일 판정 enum
MISSED_DAY_ENUM = [
    "DELAYED_REPLAY_ELIGIBLE_FROM_PREEXISTING_IMMUTABLE_PIT",
    "MISSED_RUN_NO_VALID_PIT_EVIDENCE",
    "NOT_A_TRADING_DAY",
    "DATA_INTEGRITY_UNRESOLVED",
]
MISSED_DAYS = ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10"]

FULL_CONTRACT = {
    **CONTRACT,
    "rowParityGates": ROW_PARITY_GATES,
    "packageParityGates": PACKAGE_PARITY_GATES,
    "toleranceWideningAfterResults": TOLERANCE_WIDENING_AFTER_RESULTS,
    "mandatoryGates": MANDATORY_GATES,
    "decisionEnum": DECISION_ENUM,
    "readinessStatusEnum": READINESS_STATUS_ENUM,
    "missedDayEnum": MISSED_DAY_ENUM,
    "missedDays": MISSED_DAYS,
    "intermediateConclusionsAllowed": False,
    "catchUpTradesAllowed": False,
    "manualPublishAllowed": False,
}


def canonical_json(o):
    return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def contract_hash():
    return hashlib.sha256(canonical_json(FULL_CONTRACT).encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# §24 결정 엔진 — 동결 gate 만 사용. 출력은 enum 2개 중 하나.
# ══════════════════════════════════════════════════════════════════════
def decide(gates):
    unknown = [g for g in MANDATORY_GATES if g not in gates]
    failed = [g for g in MANDATORY_GATES if gates.get(g) is not True]
    codes = [f"GATE_NOT_MEASURED::{g}" for g in unknown]
    codes += [f"GATE_FAILED::{g}" for g in failed if g not in unknown]
    if not failed:
        return {"decision": "OFFICIAL_SOURCE_REPLACEMENT_GO", "gateMatrix": gates,
                "failedGates": [], "passedGates": MANDATORY_GATES,
                "reasonCodes": ["ALL_MANDATORY_PARITY_GATES_PASS"]}
    return {"decision": "OFFICIAL_SOURCE_REPLACEMENT_NO_GO", "gateMatrix": gates,
            "failedGates": failed,
            "passedGates": [g for g in MANDATORY_GATES if g not in failed],
            "reasonCodes": codes}


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r3-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    return p


if __name__ == "__main__":
    print("contract hash:", contract_hash())
