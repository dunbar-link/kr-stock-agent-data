#!/usr/bin/env python3
"""R31 사전 고정 (precommit) — 결과를 보기 전에 전부 확정한다.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

이 파일은 **데이터를 한 줄도 보지 않은 상태**에서 작성됐다. 작성 시점에
KRX OPEN API 인증키가 승인된 secret 경로 어디에도 없어 실호출이 0 이었고,
따라서 어떤 backfill/overlap 통계도 이 계약에 영향을 줄 수 없었다.
그것이 이 파일을 지금 쓰는 이유다 — 나중에 쓰면 결과가 기준을 흔든다.

── 이 파일이 하지 않는 것 ─────────────────────────────────────────
  · R27 threshold·window·coverage gate 를 바꾸지 않는다. 그대로 인용만 한다.
    (정본은 r27_precommit.py 하나이며 sha256 으로 불변을 확인한다.)
  · 새 factor·새 liquidity metric·새 universe 규칙을 만들지 않는다.
  · 거래대금을 close × volume 으로 대체하지 않는다.
  · 결과가 나온 뒤 이 파일을 수정하지 않는다. 구현 버그 수정은 코드에서 한다.

── 고정 범위 ──────────────────────────────────────────────────────
  §7  backfill/overlap/stitch 계약
  §8  2020 overlap acceptance gate
  §9  sample anchor 검증 항목
  §10 공식 거래일 집합 확정 규칙
  §11 request budget · circuit breaker
  §15 provenance-preserving stitching 규칙

사용: python scripts/research/r31_precommit.py
부작용: reports/research/r31-precommit-latest.json 1개 write. 네트워크 0.
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

TASK_ID = "WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31"

# ══════════════ R27 계약 인용 (변경 0 — 정본은 r27_precommit.py) ══════════════
#   여기서 값을 다시 타이핑하지 않고 정본 모듈에서 읽는다. 복제본이 생기면
#   두 곳이 갈라져 "어느 쪽이 기준인가" 를 잃는다.
from r27_precommit import (  # noqa: E402
    CAPITAL, COVERAGE, PRIMARY_GATE, SENSITIVITY,
)

R27_PRECOMMIT_SHA256 = hashlib.sha256(
    (HERE / "r27_precommit.py").read_bytes()).hexdigest()

# ══════════════ §7 backfill / overlap / stitch 범위 ══════════════
BACKFILL = {
    "range": {"start": "2010-01-04", "end": "2019-12-31"},
    "endNote": ("달력상 2019-12-31 을 거래일로 가정하지 않는다. 실제 마지막 "
                "거래일은 canonical calendar 로 확정한다(2026-09-06 실측: 2019-12-30)."),
    "markets": ["KOSPI", "KOSDAQ"],
    "marketsExcluded": ["KONEX", "ETF", "ETN", "ELW", "신주인수권"],
    "marketsExcludedWhy": "이번 TASK 범위 밖. 활용신청도 호출도 하지 않는다.",
    "source": "KRX_OFFICIAL_OPEN_API",
    "sourceOperator": "한국거래소 (openapi.krx.co.kr)",
    "endpointStatus": ("DOCUMENTED_NOT_VERIFIED — repo 가 기록해 둔 "
                       "stk_bydd_trd(유가증권)만 있고 코스닥 endpoint 는 "
                       "기록이 없다. 실제 API ID·URL·요청인자는 공식 "
                       "개발명세서로 확인한 뒤 확정한다(추정 endpoint 금지)."),
    "documentedStartDate": "2010-01-04",
    "dayBasis": ("R27 needed_days — 결정일 직전 20거래일의 합집합. "
                 "r27_collect.needed_days 를 그대로 재사용하고 새 정의를 만들지 않는다."),
}

OVERLAP = {
    "range": {"start": "2020-01-02", "end": "2020년 마지막 실제 거래일"},
    "endResolved": "canonical calendar 로 확정 (2026-09-06 실측: 2020-12-30)",
    "post2020Source": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE",
    "post2020SourceVersion": "getStockPriceInfo/1160100",
    "why": ("두 소스가 같은 시장사실을 말하는지 2020년 **전체 공통구간**에서 "
            "확인한다. 표본 몇 건으로 PASS 하지 않는다."),
}

TRADE_VALUE = {
    "directTradeValueOnly": True,
    "priceTimesVolumeProxy": "FORBIDDEN",
    "why": ("실제 거래대금은 체결가 가중이라 종가×거래량과 다르다. "
            "직접 거래대금 필드가 없으면 TRADED_VALUE_FIELD_MISSING 으로 "
            "fail-closed 한다(R27·R30 규약 동일)."),
    "fieldDiscovery": ("응답 필드명을 추정 하드코딩하지 않는다. 별칭표로 탐색하고 "
                       "못 찾으면 0·근사로 대체하지 않는다(R30 §5 재사용)."),
}

# ══════════════ R27 gate 인용 (변경 0) ══════════════
R27_CONTRACT = {
    "sourceOfTruth": "scripts/research/r27_precommit.py",
    "sha256": R27_PRECOMMIT_SHA256,
    "primaryGateName": PRIMARY_GATE["name"],
    "primaryGateConditions": PRIMARY_GATE["conditions"],
    "thresholdKrw": PRIMARY_GATE["thresholdKrw"],
    "sensitivityKrw": {k: v["thresholdKrw"] for k, v in SENSITIVITY.items()
                       if isinstance(v, dict) and "thresholdKrw" in v},
    "sensitivityRule": SENSITIVITY.get("rule"),
    "window": 20,
    "windowUnit": "거래일",
    "windowRule": "결정일 **직전** 20거래일 (결정일 포함 안 함 → look-ahead 0)",
    "minCoveragePctPerYear": COVERAGE["minCoveragePctPerYear"],
    "minYearsCovered": COVERAGE["minYearsCovered"],
    "capitalKrw": CAPITAL["totalKrw"],
    "orderPerNameKrw": CAPITAL["orderPerNameKrw"],
    "missingIsNotZero": True,
    "delistingRule": "상폐 제외 금지 (survivorship 방지). 현재 상장목록으로 과거를 필터링하지 않는다.",
    "suspension": "SUSPENSION_DATA_UNAVAILABLE — zero-volume 은 하한 proxy 로만 사용.",
    "changedByR31": False,
}

# ══════════════ §8 2020 overlap acceptance gate ══════════════
#   결과를 보기 전에 숫자를 박는다. 미달이면 stitching 하지 않는다.
OVERLAP_GATE = {
    "primaryKey": ["trade_date", "market", "canonical_short_code"],
    "normalization": ("문자열 비교 금지. 종목코드는 6자리 leading-zero 보존 후 "
                      "정규화하고, 수치는 numeric 으로 캐스팅해 비교한다."),
    "reportedStages": ["RAW", "R27_ELIGIBLE"],
    "reportedStagesWhy": "raw universe 차이와 R27 판정에 실제로 쓰이는 구간을 섞지 않는다.",
    "gates": {
        "officialTradingDayCoverage": {
            "min": 1.0,
            "note": "양쪽 모두 공식적으로 데이터가 없는 휴장일은 분모에서 제외."},
        "r27EligibleCommonKeyCoverage": {
            "min": 0.99, "basis": "작은 쪽 source 의 key 수"},
        "directTradeValueExactMatch": {"min": 0.995},
        "volumeExactMatch": {"min": 0.995},
        "closeExactMatch": {"min": 0.995},
        "systematicScaleMismatch": {"max": 0},
    },
    "forbiddenSystematicRatios": [10, 100, 1000, 0.1, 0.01, 0.001],
    "residualMismatchMustClassifyAs": [
        "MARKET_DIFFERENCE", "SECURITY_TYPE_DIFFERENCE", "CODE_NORMALIZATION",
        "CORRECTED_DATA", "LISTING_DELISTING_BOUNDARY", "TRADING_SUSPENSION",
        "SOURCE_OMISSION",
    ],
    "onFail": {
        "verdict": "BLOCKED",
        "reasonClass": "KRX_DATA_GO_KR_OVERLAP_VALIDATION_FAILED",
        "forbidden": ["stitched dataset 생성", "R27 최종판정",
                      "유리한 source 임의 선택", "threshold 인하",
                      "source conflict 은닉"],
    },
}

# ══════════════ §9 sample anchor (bulk 이전 검증) ══════════════
SAMPLE_ANCHORS = {
    "dates": ["2010-01-04", "2014년 첫 실제 거래일",
              "2019년 마지막 실제 거래일", "2020-01-02"],
    "datesResolvedBy": "canonical trading calendar (평일 추정 금지)",
    "marketsEach": ["KOSPI", "KOSDAQ"],
    "checks": [
        "과거 시점 당시 상장 종목 반환 여부",
        "현재 상장 여부와 무관한 historical row 존재",
        "종목코드 leading zero 보존",
        "시장 코드 일치",
        "직접 거래대금 필드 존재",
        "거래대금이 close × volume 과 별도 값인가",
        "0 거래량·0 거래대금 row 처리",
        "휴장일 response 형태",
        "상장·상폐 경계 row",
        "duplicate key",
        "null/blank/comma 처리",
        "integer overflow",
        "encoding",
    ],
    "onMismatch": "bulk 수집 전에 BLOCKED 로 중단한다.",
}

# ══════════════ §10 공식 거래일 집합 ══════════════
CALENDAR = {
    "priority": [
        "1. 기존 canonical Korean trading calendar (_cache/krx-liquidity/_trading-calendar.json)",
        "2. 기존 PIT/TSR foundation 의 검증된 calendar",
        "3. 기존 공식 일별 데이터에서 확인된 날짜 집합",
    ],
    "noWeekendBruteForce": "주말·공휴일을 무작정 전수 호출하지 않는다.",
    "crossCheck": "calendar 가 불완전하면 KRX 응답의 실제 row date 와 교차검증한다.",
    "warmupNote": "2010년 초기 20D warm-up 구간은 별도 표시한다.",
    "noRetroactiveCalendar": "현재 calendar 로 과거를 소급 추정하지 않는다.",
}

# ══════════════ §11 request budget / circuit breaker ══════════════
BUDGET = {
    "hardKstDailyRequestCap": 7500,
    "countsTowardCap": ["probe", "retry", "bulk"],
    "circuitBreaker": {
        "authError": "1회 → 즉시 중단",
        "serviceApprovalError": "1회 → 즉시 중단",
        "http429": "1회 → 추가 호출 중단",
        "same4xx": "3회 연속 → 중단",
        "same5xx": "3회 연속 → bounded WAIT",
        "malformedJson": "3회 연속 → 중단",
    },
    "retry": {"only": "transient network / 5xx", "max": 3,
              "backoff": "exponential", "countedInBudget": True},
    "onBudgetOrRateLimit": {
        "verdict": "WAIT",
        "reasonClass": "KRX_DAILY_REQUEST_BUDGET_OR_RATE_LIMIT_REACHED",
        "mustReport": ["completed range", "remaining range", "requests used",
                       "failed dates", "resume_condition", "owner", "due",
                       "founder_action: NONE"],
        "newScheduler": 0,
    },
}

# ══════════════ §15 stitching ══════════════
STITCH = {
    "sourceBoundary": {
        "2010-01-04..2019-lastTradingDay": "KRX_OFFICIAL_OPEN_API",
        "2020-01-02..": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE (기존 R30 canonical)",
    },
    "krx2020Role": "overlap 검증 근거로만 보관. canonical post-2020 row 로 중복 삽입 금지.",
    "requiredProvenanceFields": [
        "trade_date", "market", "canonical_security_code", "direct_trade_value",
        "source", "source_field", "source_version",
        "collected_at_or_snapshot_ref", "raw_evidence_hash_ref",
    ],
    "forbidden": [
        "평균값 병합", "충돌 시 큰 값 선택", "충돌 시 작은 값 선택",
        "forward fill", "backward fill", "거래대금 추정",
        "close × volume 대체", "current listing master 로 과거 row 삭제",
        "상폐기업 제거", "source field 덮어쓰기",
    ],
    "dedupeKey": "R27 정본을 따른다(새 key 정의 금지).",
    "storage": "row-level 은 local-only. Git remote 업로드 금지.",
}

# ══════════════ 데이터 취급 경계 ══════════════
DATA_BOUNDARY = {
    "rawKrxData": "LOCAL_ONLY",
    "publication": "FORBIDDEN",
    "gitTrackedAllowed": ["request manifest 의 hash/count/status", "aggregate coverage",
                          "aggregate mismatch statistics", "code", "tests",
                          "schema", "spec", "canonical report"],
    "gitTrackedForbidden": ["AUTH_KEY", ".env", "credential file",
                            "KRX raw response", "row-level dataset",
                            "API full response sample", "absolute local secret path"],
    "credentialRule": ("값은 코드·로그·보고서·Git·채팅 어디에도 기록하지 않는다. "
                       "PRESENT/ABSENT · source class · length · sha256[:8] 만 남긴다."),
    "repoNote": ("이 repo 의 .gitignore 는 reports/ 전체를 제외한다. "
                 "따라서 canonical 보고서는 로컬 산출물이며 remote 로 나가지 않는다."),
}

VERDICT_RULES = {
    "PASS": ["두 KRX API 접근 승인 확인", "2010~2019 KOSPI/KOSDAQ 수집 완료",
             "설명되지 않은 trading-day missing 0", "2020 overlap gate 통과",
             "provenance-preserving stitch 완료", "R27 coverage 재계산 완료",
             "SIZE_SMALL 최종판정 완료", "threshold 변경 0", "raw remote upload 0",
             "secret 유출 0", "public/production 변경 0", "테스트 PASS"],
    "researchVerdicts": ["SIZE_SMALL_LIQUIDITY_TRADABILITY_PASS",
                         "SIZE_SMALL_LIQUIDITY_TRADABILITY_FAIL",
                         "SIZE_SMALL_LIQUIDITY_TRADABILITY_INCONCLUSIVE"],
    "executionVsResearch": ("SIZE_SMALL 이 FAIL 로 판정돼도 정상 실행이면 "
                            "TASK 자체는 PASS 다. 둘을 분리해 보고한다."),
    "noThresholdRescue": ("LOW 가 좋고 BASE 가 나쁘다고 BASE 를 LOW 로 바꾸지 않는다. "
                          "수익률이 좋아 보인다는 이유로 유동성 부족을 통과시키지 않는다."),
}

FORBIDDEN = [
    "새 factor", "새 factor combination", "BM/SIZE/EY 정의 변경",
    "R27 threshold 변경", "결과 확인 후 threshold 보정", "결과 확인 후 universe 보정",
    "새로운 liquidity metric 탐색", "거래대금 대신 close × volume 사용",
    "현재 상장종목 목록으로 과거 종목 필터링", "상폐 종목 제거",
    "KRX 웹페이지 bulk scraping", "비공식 scraping endpoint",
    "pykrx 등 비공식 우회 수집을 정본으로 사용", "KONEX 임의 추가",
    "ETF/ETN/ELW 임의 추가", "새 백테스트 확장", "새 portfolio construction",
    "새 holding period", "새 top-N", "새 rebalance frequency",
    "API 활용신청 자동 제출", "신규 인증키 신청·재발급·연장", "env/token/security 변경",
]

PRODUCTION = {
    "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
    "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
    "externalSend": 0, "deploy": 0, "envOrToken": 0, "productionDbWrite": 0,
    "publicRepo": "READ_ONLY", "legacy50d": "untouched", "homepage": "untouched",
    "scheduler": "untouched", "autoApply": "untouched", "autoPublish": "untouched",
}


def build():
    return {
        "task": "R31",
        "taskId": TASK_ID,
        "writtenBeforeResults": True,
        "writtenBeforeResultsEvidence": (
            "작성 시점 KRX OPEN API 인증키가 승인된 secret 경로 전부에서 ABSENT "
            "→ KRX 실호출 0 → backfill/overlap 통계 0. 결과가 이 계약에 개입할 수 없다."),
        "researchQuestion": (
            "공식 KRX API 로 2010~2019 일별 거래대금을 point-in-time·survivorship-safe "
            "하게 확보하고, 2020년 공식 공공데이터와 교차검증한 뒤, 사전에 고정된 R27 "
            "기준으로 SIZE_SMALL 의 유동성·거래가능성을 최종 판정할 수 있는가?"),
        "r27Contract": R27_CONTRACT,
        "backfill": BACKFILL,
        "overlap": OVERLAP,
        "tradeValue": TRADE_VALUE,
        "overlapGate": OVERLAP_GATE,
        "sampleAnchors": SAMPLE_ANCHORS,
        "calendar": CALENDAR,
        "budget": BUDGET,
        "stitch": STITCH,
        "dataBoundary": DATA_BOUNDARY,
        "verdictRules": VERDICT_RULES,
        "forbidden": FORBIDDEN,
        "production": PRODUCTION,
        "immutability": (
            "결과 확인 후 이 spec 을 수정하지 않는다. 구현 버그 수정은 허용하되 "
            "연구 threshold·기간·시장·acceptance gate 변경은 금지한다."),
    }


def main() -> int:
    spec = build()
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r31-precommit-latest.json"
    p.write_text(json.dumps(spec, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    src_sha = hashlib.sha256((HERE / "r31_precommit.py").read_bytes()).hexdigest()
    print(f"[r31] saved {p.name}")
    print(f"[r31] r31_precommit.py sha256 = {src_sha}")
    print(f"[r31] r27_precommit.py sha256 = {R27_PRECOMMIT_SHA256}")
    print(f"[r31] R27 thresholds unchanged = "
          f"BASE {PRIMARY_GATE['thresholdKrw']} / "
          f"LOW {SENSITIVITY['LOW']['thresholdKrw']} / "
          f"HIGH {SENSITIVITY['HIGH']['thresholdKrw']} · "
          f"coverage {COVERAGE['minCoveragePctPerYear']}% x {COVERAGE['minYearsCovered']}y")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
