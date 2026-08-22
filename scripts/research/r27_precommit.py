#!/usr/bin/env python3
"""R27 사전 고정 (precommit) — 결과를 보기 전에 전부 확정한다.

WABABA-SIZE-TRADABILITY-VALIDATION-R27

질문은 하나다: R25/R26 에서 발견된 SIZE_SMALL 초과수익이 **당시 실제로 거래
가능한 종목과 현실적인 개인투자 규모**에서도 유지되는가?

여기 적힌 gate·threshold·bucket·horizon 은 결과를 본 뒤 바꾸지 않는다(§2·§5·§26).

안전: 읽기 전용 + reports/research write 만. 네트워크 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

TASK = "WABABA-SIZE-TRADABILITY-VALIDATION-R27"

QUESTION = (
    "R25/R26 에서 발견된 SIZE_SMALL 초과수익은 당시 실제로 거래 가능한 종목과 "
    "현실적인 개인투자 규모에서도 유지되는가?")
REFERENCE_QUESTION = (
    "동일한 거래가능성 기준을 적용했을 때 BM 과 SIZE 중 어느 factor 가 실행 "
    "가능한 전략 후보로 더 강한가?")
SUCCESS = (
    "SIZE 19.15% 를 지키는 것이 성공이 아니다. '이 수익률을 당시 실제 개인투자자가 "
    "살 수 있었고 나중에 팔 수 있었는가' 에 답하는 것이 성공이다. SIZE 가 무너져도 "
    "성공이며 그 경우 BM 으로 간다(§43).")
NO_RESCUE = (
    "threshold·종목수·보유기간·리밸런싱을 사후 조정하지 않는다. 민감도 3개는 "
    "robustness 확인용이며 성과가 가장 높은 것을 채택하지 않는다. PRIMARY 판정은 "
    "BASE gate 하나만 쓴다(§2·§15·§26).")

# ── §3 데이터 소스 ────────────────────────────────────────────────────
DATA_SOURCES = {
    "priority": [
        "1. 기존 repo/cache (_cache/pit-snapshots — 종가·시총·주식수·PER/PBR/EPS/BPS/DIV/DPS)",
        "2. KRX 계열 기존 수집 구조 (pykrx — 이 repo 가 이미 쓰는 라이브러리)",
        "3. DART 등 공식 데이터",
        "4. 신뢰 가능한 무료 소스",
    ],
    "liquidityFetcher": "pykrx stock.get_market_cap_by_ticker(ymd, market='ALL')",
    "liquidityFetcherWhy": (
        "이 repo 의 PIT 스냅샷 빌더(scripts/research/build_pit_snapshots.py)가 이미 "
        "쓰는 바로 그 함수다. 한 콜로 전 종목의 종가·시가총액·**거래량·거래대금**·"
        "상장주식수를 준다. 새 crawler·새 framework 를 만들지 않는다(§3)."),
    "columnsUsed": ["거래량", "거래대금", "종가", "시가총액", "상장주식수"],
    "paidData": 0, "newCredential": 0,
    "existingCredentialReuse": "기존 KRX 세션만 재사용. 신규 key/token 발급 0.",
}

UNIVERSE = {
    "period": {"start": "2007-01-02", "end": "2026-08-03"},
    "decisionDates": "PIT 스냅샷 월별 시점 (R25/R26 과 동일)",
    "inheritedFrom": "R25/R26 (변경 없음)",
    "delisting": "상폐 제외 금지. canonical UNKNOWN_RECOVERY 유지.",
    "engine": "CANONICAL_TSR_R24 (r25_engine.R25Engine)",
}

# ── §6·§7 factor 정의 — R25/R26 정본 그대로 ───────────────────────────
BM = {"formula": "1 / PBR", "direction": +1, "frozen": "R25 정의 그대로(§7)"}
SIZE = {"formula": "-ln(marketCap)", "direction": +1,
        "frozen": "R25 정의 그대로(§6)"}
SAME_GATE_BOTH = (
    "BM 과 SIZE 에 서로 다른 거래가능성 기준을 적용하지 않는다. 완전히 같은 "
    "TRADABLE universe 안에서 비교한다(§7·§18).")

# ── §8·§9 유동성 변수 ─────────────────────────────────────────────────
LIQUIDITY_VARS = {
    "window": 20,
    "windowUnit": "거래일",
    "windowRule": "결정일 **직전** 20거래일 (결정일 포함 안 함 → look-ahead 0)",
    "measures": [
        "median20TradedValue  — 20거래일 거래대금 중앙값(원)",
        "mean20TradedValue    — 20거래일 거래대금 평균(원)",
        "median20Volume       — 20거래일 거래량 중앙값(주)",
        "zeroVolumeDays20     — 20거래일 중 거래량 0 인 날",
        "observedDays20       — 20거래일 중 실제 관측된 날",
        "tradedOnDecisionDate — 결정일 당일 거래량 > 0",
    ],
    "noLookAhead": (
        "결정일 **이후** 거래량으로 매수 가능성을 판정하면 look-ahead 다. "
        "직전 20거래일만 쓴다(§8)."),
    "tradedValueSource": "KRX 공식 거래대금 컬럼 (price × volume 추정 아님)",
}

# ── §10 거래정지 ──────────────────────────────────────────────────────
SUSPENSION = {
    "status": "SUSPENSION_DATA_UNAVAILABLE",
    "why": ("거래정지 이력 자체를 주는 무료 공식 시계열 소스를 이 repo 가 갖고 "
            "있지 않다. 없는 데이터를 만들어내지 않는다(§4·§10)."),
    "proxy": "거래량 0 = 사실상 거래 불가로 간주(정지·단일가·유동성 고갈 미구분)",
    "proxyLimitation": ("zero-volume 은 거래정지의 **하한 proxy** 다. 실제 정지일을 "
                        "모두 잡지 못하며, 반대로 정지가 아닌 무거래도 포함한다."),
    "sellSideRule": ("매도일 정지를 가정하지 않는다. canonical 엔진의 상폐/최종가 "
                     "semantics 를 그대로 쓰고 별도 매도 지연 모델을 만들지 않는다."),
}

# ── §11 zero volume / §30 결측 ────────────────────────────────────────
MISSING_RULE = {
    "missingIsNotZero": (
        "유동성 데이터가 없는 종목-시점을 **0 으로 취급하지 않는다**. "
        "tradable 로도 nontradable 로도 강제하지 않고 MISSING_DATA 로 분류한다(§30)."),
    "missingExcludedFromTradable": (
        "MISSING_DATA 는 TRADABLE 판정을 받을 수 없다(근거 없이 통과시키지 않음). "
        "탈락 사유로 별도 집계해 coverage 문제를 숨기지 않는다."),
    "zeroVolumeIsObserved": "거래량 0 은 관측된 값이다. 결측과 구분한다.",
}

# ── §14 개인투자 규모 (sanity 하나만) ─────────────────────────────────
CAPITAL = {
    "totalKrw": 50_000_000,
    "namesForSanity": 40,
    "orderPerNameKrw": 1_250_000,
    "why": ("과거 후보와 비교 가능한 현실성 참고값 하나일 뿐이다. R27 에서 40 이 "
            "최적이라고 주장하지 않으며 종목수를 탐색하지 않는다(§14·§35)."),
}

# ── §15·§16 참여율 ────────────────────────────────────────────────────
PARTICIPATION = {
    "formula": "orderPerNameKrw / median20TradedValue",
    "buckets": [0.001, 0.005, 0.01, 0.05, 0.10],
    "bucketNames": ["<=0.1%", "0.1~0.5%", "0.5~1%", "1~5%", "5~10%", ">10%"],
    "auditOnly": ("이 bucket 들을 서로 경쟁시켜 성과가 가장 좋은 것을 고르지 않는다. "
                  "목적은 sensitivity/audit 이다(§15)."),
}

# ── §17 PRIMARY tradability gate — 하나만, 보수적으로 ─────────────────
PRIMARY_GATE = {
    "name": "TRADABLE_AT_DECISION",
    "conditions": [
        "tradedOnDecisionDate = True            (결정일 당일 거래 성립)",
        "observedDays20 >= 15                   (20거래일 중 15일 이상 관측)",
        "zeroVolumeDays20 <= 5                  (무거래일 25% 이하)",
        "median20TradedValue >= 125,000,000원   (참여율 1% 이하)",
    ],
    "participationImplied": 0.01,
    "thresholdKrw": 125_000_000,
    "why": ("1종목 주문액 1,250,000원이 20거래일 median 거래대금의 **1% 이하**가 "
            "되도록 잡았다. §15 가 나열한 1/5/10/20% 중 가장 보수적인 쪽이며, "
            "결과를 보고 고른 것이 아니라 지금 고정한다."),
    "onlyOne": "PRIMARY gate 는 이것 하나다. 판정은 이 gate 로만 한다(§17·§26).",
}

# ── §26 민감도 — 3개, 결과 전 고정 ────────────────────────────────────
SENSITIVITY = {
    "LOW":  {"participation": 0.05, "thresholdKrw": 25_000_000},
    "BASE": {"participation": 0.01, "thresholdKrw": 125_000_000},
    "HIGH": {"participation": 0.005, "thresholdKrw": 250_000_000},
    "rule": ("PRIMARY verdict 는 BASE 만 쓴다. LOW/HIGH 는 결론 robustness 확인용이며 "
             "성과가 가장 높은 threshold 를 채택하는 것은 금지다(§26)."),
}

# ── §12 가격 bucket ───────────────────────────────────────────────────
PRICE_BUCKETS = [
    {"name": "<500", "lo": None, "hi": 500},
    {"name": "500~999", "lo": 500, "hi": 1000},
    {"name": "1,000~4,999", "lo": 1000, "hi": 5000},
    {"name": ">=5,000", "lo": 5000, "hi": None},
]
PRICE_RULE = {
    "basis": "결정일 당시 **실제 거래가격**(스냅샷 종가). 분할 조정가 아님.",
    "why": "분할 때문에 가짜 penny 가 생기지 않게 한다(§12).",
    "notAFilter": ("저가주는 PRIMARY gate 의 제외 조건이 **아니다**. 노출을 "
                   "측정만 하고 제거하지 않는다(§12·§21)."),
}

# ── §13 시총 분포 ─────────────────────────────────────────────────────
MARKETCAP_STATS = ["min", "p10", "p25", "median", "p75", "p90"]

# ── §19 horizon ───────────────────────────────────────────────────────
HORIZONS = {"all": [12, 36, 60], "secondary": [84], "primary": 36,
            "primaryWhy": "R26 과 직접 비교하기 위해 36M 고정. 결과 보고 변경 금지(§19).",
            "annualization": "(1+cum)^(12/months) − 1"}

# ── §18 비교 구조 ─────────────────────────────────────────────────────
COMPARISON = {
    "arms": ["TRADABLE_SIZE", "TRADABLE_BM", "TRADABLE_CONTROL"],
    "fairness": SAME_GATE_BOTH,
    "control": "TRADABLE universe 전체 동일가중 (같은 분모)",
    "beforeAfter": "각 factor 의 필터 전/후를 TOP TSR·spread·coverage·종목수로 비교(§20)",
}

# ── §30 coverage gate ─────────────────────────────────────────────────
COVERAGE = {
    "reportBy": ["year", "exchange", "sizeBucket"],
    "minCoveragePctPerYear": 90.0,
    "minYearsCovered": 15,
    "rule": ("연도별 eligible 종목 중 유동성 데이터가 있는 비율이 90% 미만인 해는 "
             "DATA_INSUFFICIENT 로 표시한다. 기준 미달 연도가 많아 minYearsCovered 를 "
             "못 채우면 전체를 SIZE_DATA_INSUFFICIENT 로 종료한다."),
    "noSecretWindowShift": ("coverage 가 좋은 최근 기간만 몰래 primary 로 바꾸지 "
                            "않는다. primary 기간은 전체 연구기간이다(§30)."),
}

# ── §25 비용 ──────────────────────────────────────────────────────────
COST = {"base": {"roundTripBps": 0}, "high": {"roundTripBps": 100},
        "inheritedFrom": "R26 (동일 가정 재사용)",
        "noBidAskEstimate": ("historical bid-ask spread 데이터가 없으므로 추정하지 "
                             "않는다. 유동성 bucket 별 한계를 병기한다(§25)."),
        "noParameterSearch": "비용 parameter 탐색 금지."}

# ── §22 included / excluded ───────────────────────────────────────────
INCLUDED_EXCLUDED = {
    "definition": ("SIZE TOP 분위를 PRIMARY gate 로 둘로 나눈다 — "
                   "included(=tradable) vs excluded(=nontradable). 같은 분위 안에서 "
                   "나누므로 factor 신호는 동일하고 거래가능성만 다르다."),
    "flagRule": ("excluded 의 forward TSR 이 included 의 2배 이상이거나 included 가 "
                 "0 이하이면 SIZE_NONTRADABLE_ALPHA 경고(§22)."),
}

# ── §31 SIZE 최종 판정 기준 (결과 전 고정) ────────────────────────────
QUALIFICATION = {
    "primaryHorizon": 36,
    "SIZE_EXECUTABLE_STRONG": {
        "afterTopMinusControlPctMin": 5.0,
        "retentionRatioMin": 0.6,
        "includedSpreadPositive": True,
        "minSubperiodPositiveRatio": 0.5,
        "minRollingPositiveRatio": 0.55,
        "bothExchangesSameSign": True,
        "survivesTop3Removal": True,
        "notNontradableAlpha": True,
    },
    "SIZE_EXECUTABLE_PROMISING": {
        "afterTopMinusControlPctMin": 2.0,
        "includedSpreadPositive": True,
        "minSubperiodPositiveRatio": 0.5,
    },
    "SIZE_ALPHA_LARGELY_NONTRADABLE": {
        "note": "excluded TSR >= included TSR × 2 이거나 included spread <= 0",
    },
    "SIZE_FRAGILE": {
        "note": "AFTER 양(+)이지만 top3 제거 시 부호 반전 또는 attrition >= 80%",
        "attritionMaxPct": 80.0,
    },
    "SIZE_DATA_INSUFFICIENT": {"note": "coverage gate 미달(§30)"},
    "order": ["SIZE_DATA_INSUFFICIENT", "SIZE_ALPHA_LARGELY_NONTRADABLE",
              "SIZE_FRAGILE", "SIZE_EXECUTABLE_STRONG",
              "SIZE_EXECUTABLE_PROMISING"],
    "retentionRatio": "AFTER (TOP−CONTROL) / BEFORE (TOP−CONTROL) at 36M",
}

# ── §33 실행후보 선정 ─────────────────────────────────────────────────
EXECUTION_CANDIDATE = {
    "options": ["BM", "SIZE", "BOTH_SEPARATE", "NONE"],
    "bothSeparateMeaning": ("두 전략을 섞으라는 뜻이 아니다. 둘 다 독립적으로 실행 "
                            "포트폴리오 연구 가치가 있다는 뜻이다(§33)."),
    "criteria": ["36M TOP TSR", "36M spread", "60M spread", "tradability coverage",
                 "median traded value", "order participation", "low-price exposure",
                 "delisting", "concentration", "KOSPI/KOSDAQ robustness",
                 "cost stress"],
    "notReturnOnly": "단순 최고수익률만으로 결정하지 않는다(§32).",
}

NEXT_TASK_RULE = {
    "A": "SIZE_EXECUTABLE_STRONG + SIZE 가 BM 보다 명확 우위 → SIZE 단독 portfolio construction",
    "B": "SIZE 생존 + BM 도 강함 → BM vs SIZE frozen head-to-head (동일 execution rules)",
    "C": "SIZE alpha 가 nontradable 에 집중 → BM 단독 portfolio construction",
    "D": "SIZE_DATA_INSUFFICIENT → 최소 liquidity data gap 만 해결. 새 factor 연구 금지",
    "E": "둘 다 실행성 약함 → portfolio 연구 중단 후 원인 재평가",
}

FORBIDDEN = [
    "SIZE/BM threshold optimization", "종목수 optimization",
    "보유기간 optimization", "rebalance optimization",
    "liquidity threshold sweep 후 최고값 선택", "penny-stock filter rescue",
    "BM×SIZE 재조합", "EY 추가", "Quality 추가", "새로운 factor",
    "결과를 보고 유리한 거래가능성 기준 선택",
    "포트폴리오 최적화(종목수·보유기간·매수주기·분할투입·리밸런싱)",
    "유료데이터 구매", "신규 API key/token 발급",
]

FAILURE_CONDITIONS = {
    "SIZE_REPRODUCTION_MISMATCH": "R25/R26 SIZE 재현 실패 시 본 연구 진행 금지(§6)",
    "BM_REPRODUCTION_MISMATCH": "BM reference 재현 실패 시 중단(§7)",
    "LOOK_AHEAD": "결정일 이후 거래량 사용이 발견되면 중단",
    "MISSING_AS_ZERO": "결측을 0 으로 취급한 경로가 발견되면 중단",
    "UNIVERSE_MISMATCH": "BM/SIZE/control 의 tradable universe 가 다르면 중단",
    "NONDETERMINISTIC": "동일 입력 재실행이 다르면 중단",
    "onFailure": ("§41 대로 원인분리 → 최소수정 → targeted test → R27 전체 재실행 "
                  "→ 전체 regression. 결과를 유리하게 만드는 rescue 금지."),
}


def main() -> int:
    out = {
        "task": "R27", "taskId": TASK,
        "writtenBeforeResults": True,
        "researchQuestion": QUESTION,
        "referenceQuestion": REFERENCE_QUESTION,
        "successDefinition": SUCCESS,
        "noParameterRescue": NO_RESCUE,
        "priorState": {
            "r24": "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "r25Primary": ["BM", "SIZE_SMALL"], "r25Secondary": ["EY"],
            "r26": "NO_INCREMENTAL_COMBINATION",
            "r26Limitation": "LIQUIDITY_DATA_INCOMPLETE — 이번에 해소 시도",
            "r26Size36mTop": 19.15, "r26Bm36mTop": 15.01},
        "dataSources": DATA_SOURCES, "universe": UNIVERSE,
        "bmDefinition": BM, "sizeDefinition": SIZE, "sameGateBoth": SAME_GATE_BOTH,
        "liquidityVariables": LIQUIDITY_VARS,
        "suspension": SUSPENSION, "missingDataRule": MISSING_RULE,
        "capital": CAPITAL, "participation": PARTICIPATION,
        "primaryGate": PRIMARY_GATE, "sensitivity": SENSITIVITY,
        "priceBuckets": PRICE_BUCKETS, "priceRule": PRICE_RULE,
        "marketCapStats": MARKETCAP_STATS,
        "horizons": HORIZONS, "comparison": COMPARISON,
        "coverage": COVERAGE, "cost": COST,
        "includedExcluded": INCLUDED_EXCLUDED,
        "qualification": QUALIFICATION,
        "executionCandidate": EXECUTION_CANDIDATE,
        "nextTaskRule": NEXT_TASK_RULE,
        "forbidden": FORBIDDEN, "failureConditions": FAILURE_CONDITIONS,
        "production": {"realMoneyStage": "REAL_MONEY_NOT_APPROVED",
                       "realOrders": 0, "broker": 0, "paidData": 0},
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r27-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (RD / "r27-tradability-definition-latest.json").write_text(
        json.dumps({"task": "R27", "primaryGate": PRIMARY_GATE,
                    "sensitivity": SENSITIVITY,
                    "liquidityVariables": LIQUIDITY_VARS,
                    "missingDataRule": MISSING_RULE,
                    "suspension": SUSPENSION,
                    "sameGateBoth": SAME_GATE_BOTH,
                    "capital": CAPITAL, "participation": PARTICIPATION,
                    "onlyOnePrimaryGate": True},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"primaryGate": PRIMARY_GATE["name"],
                      "thresholdKrw": PRIMARY_GATE["thresholdKrw"],
                      "participation": PRIMARY_GATE["participationImplied"],
                      "primaryHorizon": HORIZONS["primary"],
                      "orderPerName": CAPITAL["orderPerNameKrw"],
                      "suspension": SUSPENSION["status"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
