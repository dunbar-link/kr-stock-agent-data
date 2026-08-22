#!/usr/bin/env python3
"""R26 사전 고정 (precommit) — 결과를 보기 전에 전부 확정한다.

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26

질문은 하나다: 싸고(BM) 작은(SIZE) 회사를 동시에 선호하는 **단순 규칙 하나**가
그냥 싼 회사만 사는 것보다도, 그냥 작은 회사만 사는 것보다도 더 나은
shareholder TSR 을 반복적으로 만드는가?

여기 적힌 정의·가중치·horizon·기준은 결과를 본 뒤 바꾸지 않는다(§2·§3·§44).

안전: 읽기 전용 + reports/research write 만. 네트워크 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

TASK = "WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26"

QUESTION = (
    "싸고 작은 회사를 동시에 선호하는 단순 규칙이, 그냥 싼 회사를 사는 것보다도, "
    "그냥 작은 회사를 사는 것보다도 실제로 더 나은 shareholder TSR 을 반복적으로 "
    "만드는가?")

SECONDARY_QUESTION = (
    "그 추가수익이 상폐·저가주·비유동성·KOSDAQ·소수 대박주 노출로 설명되는가?")

SUCCESS = (
    "CAGR 숫자를 크게 만드는 것이 성공이 아니다. 위 질문에 정확히 답하는 것이 "
    "성공이다. YES 면 실행 포트폴리오를 연구하고, NO 면 두 factor 를 억지로 "
    "합치지 않는다(§29·§30·§44).")

NO_RESCUE = (
    "threshold·weight·보유기간·종목수를 사후 조정하지 않는다. 결합 정의는 하나뿐이고 "
    "0.5/0.5 외 가중치 탐색·intersection 동시탐색·EY 추가·Quality 재투입을 "
    "하지 않는다(§2·§6·§7·§33·§34).")

# ── 상속: 엔진·universe·PIT (§3·§4·§5) ───────────────────────────────
ENGINE = {
    "name": "CANONICAL_TSR_R24",
    "adapter": "r25_engine.R25Engine (R24 승인 엔진의 O(1) 누적계수 형태)",
    "policy": "R17 POLICY_A_ASSUME_FULL_EXERCISE + 합리적 실권(K >= P_ex)",
    "returnBasis": "TIME_WEIGHTED — 외부 납입 중립화",
    "equivalenceProof": ("r17_wealth.RightsWealth.run 과 실측 동치. R25 에서 "
                         "최대오차 3.6e-15 확인. R26 회귀가 다시 강제한다."),
    "delisting": "UNKNOWN_RECOVERY (마지막 관측가 청산) — R16 정책 그대로. 상한 추정.",
    "sameEngineFor": ["BM ALONE", "SIZE ALONE", "BM×SIZE COMBO", "benchmark"],
    "closeOnlyForbidden": True,
}

UNIVERSE = {
    "source": "_cache/pit-snapshots/*.csv.gz",
    "period": {"start": "2007-01-02", "end": "2026-08-03"},
    "inheritedFrom": "R25 (변경 없음)",
    "delisting": "상폐 종목 제외 금지. 편입 시점 universe 전체.",
    "survivorship": "current-universe bias 금지.",
    "shareClass": "보통주·우선주 라인 모두 유지 (R25 와 동일).",
}

PIT_RULE = {
    "factorAsOf": "스냅샷 date 의 KRX 공표값",
    "forwardReturnStart": "같은 스냅샷 date",
    "noFutureData": "미래 스냅샷·미래 재무·미래 가격 사용 금지",
    "inheritedFrom": "R25 (변경 없음)",
}

# ── §4·§5 factor 정의 — R25 정본 그대로 ───────────────────────────────
BM = {"name": "BM", "formula": "1 / PBR", "direction": +1,
      "meaning": "높을수록 저평가", "requires": ["PBR"], "positiveOnly": ["PBR"],
      "frozen": "R25 canonical 정의 그대로. PER·PER+PBR 등 변형 금지(§4)."}
SIZE = {"name": "SIZE_SMALL", "formula": "-ln(marketCap)", "direction": +1,
        "meaning": "높을수록 소형", "requires": ["marketCap"],
        "positiveOnly": ["marketCap"],
        "frozen": "R25 canonical 정의 그대로. 시총 정의·날짜·PIT 기준 변경 금지(§5)."}

# ── §6 결합 규칙 — 단 하나 ────────────────────────────────────────────
COMBINATION = {
    "id": "DOUBLE_RANK_EQUAL_WEIGHT",
    "formula": "COMBINED_SCORE = 0.5 × pctRank(BM) + 0.5 × pctRank(SIZE_SMALL)",
    "pctRank": ("각 결정일 eligible 종목 안에서 0~1 백분위. 1 이 가설상 가장 "
                "유리한 쪽(BM 높음 / 시총 작음). 동점은 ticker 오름차순."),
    "weights": {"BM": 0.5, "SIZE_SMALL": 0.5},
    "direction": +1,
    "onlyOne": ("결합은 이 하나뿐이다. 가중치 탐색·intersection 방식 동시 경쟁·"
                "BM×SIZE×EY 확장 금지(§2·§6·§7·§33)."),
    "why": ("가장 단순하고 해석 가능한 형태. 두 신호를 같은 척도(백분위)로 바꾼 뒤 "
            "동일가중 평균한다 — 어느 한쪽 원값의 스케일이 결과를 지배하지 않는다."),
}

# ── §8 비교 구조 — 세 팔이 완전히 같은 조건 ───────────────────────────
ARMS = ["BM_ALONE", "SIZE_ALONE", "COMBO"]
COMPARISON = {
    "arms": ARMS,
    "sharedUniverse": ("세 팔 모두 **BM 과 SIZE 가 동시에 계산 가능한** 종목만 쓴다. "
                       "비교구조가 달라서 결합이 유리해지는 것을 막는다(§8)."),
    "sharedEverything": ["canonical TSR", "universe timing", "quantile 규칙",
                         "horizon", "PIT", "missing-data rule", "동일가중"],
    "r25Reproduction": ("별도로 R25 의 factor 별 universe 로 BM·SIZE 를 **정확 재현**해 "
                        "drift 가 없음을 먼저 증명한다(§37). 재현과 비교는 다른 표다."),
}

ELIGIBILITY = {
    "common": ["close > 0", "shares > 0", "marketCap > 0", "PBR > 0"],
    "intersection": "BM 과 SIZE_SMALL 이 **둘 다** 유효한 종목만",
    "noImputation": "결측 보간 금지",
    "minUniversePerMonth": 100,
    "inheritedFrom": "R25 ELIGIBILITY",
}

QUANTILES = {
    "rule": "eligible >= 200 이면 decile, 100~199 면 quintile, 100 미만이면 그 달 제외",
    "inheritedFrom": "R25 QUANTILES (변경 없음)",
    "weighting": "분위 내 동일가중",
}

# ── §9 horizon — 결과 보기 전 primary 고정 ────────────────────────────
HORIZONS = {
    "all": [12, 36, 60],
    "secondary": [84],
    "primary": 36,
    "primaryWhy": (
        "결과를 보고 고르지 않기 위해 지금 고정한다. 12M 은 R25 에서 spread 가 "
        "가장 컸지만 극단 우편향에 가장 민감했고, 60M 은 코호트가 가장 적다. "
        "36M 은 표본 수와 장기 robustness 사이에서 중간이며 R25 primary 집합에 "
        "이미 포함돼 있다. 84M 은 표본이 충분할 때만 robustness reference."),
    "unit": "개월", "metric": "연율 TSR",
    "annualization": "(1+cum)^(12/months) − 1",
}

# ── §9·§10 incremental 지표 ───────────────────────────────────────────
INCREMENTAL = {
    "primaryMetric": "TOP 분위 연율 TSR",
    "primaryWhy": ("실제 투자자가 보유하는 것은 TOP 분위다. long-short spread 는 "
                   "보조로 함께 본다."),
    "secondaryMetric": "TOP − BOTTOM 연율 spread",
    "pairing": ("코호트 단위 짝지음 — 같은 시작월의 COMBO 와 BM 을 직접 뺀다. "
                "서로 다른 평균을 나중에 빼지 않는다(짝지음이 검정력이 높고 정확하다)."),
    "required": ["incremental_vs_BM", "incremental_vs_SIZE",
                 "incremental_vs_UNIVERSE"],
    "notEnough": "COMBO vs MARKET 만 보고 좋아졌다고 선언 금지(§10).",
}

SUBPERIODS = [
    {"name": "2007-2011", "start": "2007-01-02", "end": "2011-12-31"},
    {"name": "2012-2016", "start": "2012-01-01", "end": "2016-12-31"},
    {"name": "2017-2021", "start": "2017-01-01", "end": "2021-12-31"},
    {"name": "2022-2026", "start": "2022-01-01", "end": "2026-08-03"},
]
SUBPERIOD_WHY = "R25 와 동일한 달력 고정 구간 재사용(§12). 결과에 맞춰 자르지 않는다."

ROLLING = {
    "overlapping": "매월 시작 코호트 전부",
    "nonOverlapping": "horizon 간격으로 겹치지 않게",
    "outputs": ["median", "positiveRatio", "p10", "p25", "p75", "p90",
                "worst", "best"],
}

BOOTSTRAP = {
    "methods": ["moving-block", "year-level"],
    "blockMonths": 12,
    "resamples": 2000,
    "seed": 20260822,
    "seedWhy": "R25 와 같은 시드. 결정적 재현을 위해 고정.",
    "targets": ["COMBO_minus_BM", "COMBO_minus_SIZE"],
    "outputs": ["mean", "median", "ci95Low", "ci95High", "pExcessAbove0",
                "pExcessAbove1pp", "pExcessAbove2pp"],
}

INDEPENDENCE = {
    "measures": ["corr(COMBO_score, BM_rank)", "corr(COMBO_score, SIZE_rank)",
                 "rank variance contribution", "TOP 분위 이름 겹침률"],
    "flagRule": ("결합 score 가 사실상 한쪽 rank 에 지배되면(상관 >= 0.95 이거나 "
                 "TOP 겹침 >= 90%) 결합 의미가 약하다고 보고한다(§15)."),
}

CONDITIONAL = {
    "bmWithinSize": ("SMALL/MID/LARGE 각 층 안에서 BM 상위 vs 하위 — '작은 주식이면 "
                     "아무거나 좋은가, 작은 주식 안에서도 싼 게 더 좋은가'(§17)"),
    "sizeWithinBm": ("BM 상위/중위/하위 각 층 안에서 소형 vs 대형 — '싼 주식이면 "
                     "아무거나 좋은가, 싼 주식 중에도 작은 게 더 좋은가'(§18)"),
    "note": "새 parameter 탐색이 아니라 independence attribution 이다.",
}

# ── §19 liquidity — 데이터 한계를 먼저 고정 ───────────────────────────
LIQUIDITY = {
    "available": ["close", "marketCap", "shares"],
    "unavailable": ["trading volume", "traded value", "zero-volume frequency",
                    "suspension"],
    "status": "LIQUIDITY_DATA_INCOMPLETE",
    "why": ("PIT 스냅샷에 거래량·거래대금 컬럼이 없다. 실측으로 확인했다. "
            "없는 데이터를 추정해 가짜 정밀도를 만들지 않는다(§19)."),
    "auditable": ["median close", "median marketCap", "marketCap 분포",
                  "저가주 노출", "5천만원 기준 1종목당 투자금 / 시총 비율"],
    "notionalCapitalKrw": 50_000_000,
    "notionalWhy": "개인투자자 규모 sanity 참고용일 뿐 포트폴리오 설계가 아니다(§20).",
}

PRICE_BUCKETS = [
    {"name": "<500", "lo": None, "hi": 500},
    {"name": "500~1,000", "lo": 500, "hi": 1000},
    {"name": "1,000~5,000", "lo": 1000, "hi": 5000},
    {"name": ">5,000", "lo": 5000, "hi": None},
]
PRICE_BUCKET_WHY = ("§21 이 제시한 구간 그대로. 저가주 filter 를 새로 추가하지 "
                    "않는다 — 노출을 **측정만** 한다.")

DISTRESS = {
    "measures": ["delistingRate", "lowPriceRate", "extremeBmRate",
                 "persistentLossRate", "negativeEpsRate"],
    "extremeBm": "BM > 5 (PBR < 0.2)",
    "lowPrice": "close < 1000원",
    "persistentLoss": "직전 36개월 EPS>0 비율 <= 0.5",
    "noNewFilter": "결과가 좋다고 distress filter 를 추가하지 않는다(§23).",
}

CONCENTRATION = {
    "measures": ["top1", "top3", "top5", "top10"],
    "removal": ["remove top1", "remove top3", "remove top5", "remove top10"],
    "check": "제거 후에도 COMBO vs BM / COMBO vs SIZE 방향이 유지되는가",
    "fragileRule": "top3 제거 시 incremental 부호가 뒤집히면 FRAGILE",
}

SECTOR = {
    "status": "SECTOR_DATA_UNAVAILABLE",
    "why": "PIT 스냅샷·DART 캐시에 업종 분류가 없다. 실측 확인했다.",
    "noNeutralization": "새 sector neutralization 금지(§25).",
}

COST = {
    "base": {"roundTripBps": 0},
    "high": {"roundTripBps": 100},
    "highWhy": ("소형주 슬리피지를 포함한 스트레스 값. 세 팔에 **동일하게** "
                "적용하며 비용 parameter 탐색이 아니다(§27)."),
    "turnoverProxy": ("각 팔의 TOP 분위 월간 이름 교체율. COMBO 가 SIZE 때문에 "
                      "교체가 잦아지는지 본다."),
    "application": "연율 TSR − (연간 교체율 × roundTripBps)",
}

TSR_LIMITATION = {
    "foundation": {"expectedBiasPct": 1.734, "extremeDiscontinuityPct": 1.42},
    "compare": ["COMBO TOP", "BM TOP", "SIZE TOP", "UNIVERSE"],
    "flagRule": ("COMBO TOP 의 미해결 자본행위 노출률이 universe 대비 3%p 이상 "
                 "높으면 TSR_LIMITATION_EXPOSED"),
}

# ── §28 자격 기준 (결과 보기 전 고정) ─────────────────────────────────
QUALIFICATION = {
    "primaryHorizon": 36,
    "INCREMENTAL_COMBINATION_STRONG": {
        "incrementalVsBmPrimaryPositive": True,
        "incrementalVsSizePrimaryPositive": True,
        "minHorizonsPositiveBoth": 2,
        "horizonsTested": 3,
        "minSubperiodPositiveRatio": 0.5,
        "minRollingPositiveRatio": 0.55,
        "bootstrapCi95LowAboveZeroBoth": True,
        "exchangeDirectionHolds": "KOSPI·KOSDAQ 중 최소 다수에서 방향 유지",
        "survivesTop3Removal": True,
        "notTsrLimitationExposed": True,
        "maxComboTopDelistingRatePct": 20.0,
        "delistingNotWorseThanParents": True,
    },
    "INCREMENTAL_COMBINATION_PROMISING": {
        "incrementalVsBmPrimaryPositive": True,
        "incrementalVsSizePrimaryPositive": True,
        "minHorizonsPositiveBoth": 1,
        "minSubperiodPositiveRatio": 0.5,
        "note": "STRONG 조건 일부 미충족이지만 두 단독 대비 모두 양(+)",
    },
    "COMBINATION_FRAGILE": {
        "note": ("primary 에서 양(+)이지만 top3 제거 시 부호가 뒤집히거나, "
                 "TSR_LIMITATION_EXPOSED 이거나, 상폐/유동성 노출이 한계치를 넘음"),
    },
    "NO_INCREMENTAL_COMBINATION": {
        "note": ("primary horizon 에서 BM 단독 또는 SIZE 단독 대비 incremental 이 "
                 "0 이하. 억지로 살리지 않는다(§29·§30)."),
    },
    "order": ["NO_INCREMENTAL_COMBINATION", "COMBINATION_FRAGILE",
              "INCREMENTAL_COMBINATION_STRONG",
              "INCREMENTAL_COMBINATION_PROMISING"],
}

NEXT_TASK_RULE = {
    "onStrongOrPromising": ("WABABA-BM-SIZE-FROZEN-PORTFOLIO-CONSTRUCTION-R27 "
                            "— 종목수·자본투입·보유기간·교체주기·거래비용·"
                            "liquidity 를 사전규격 기반으로 설계(§32)."),
    "onNoIncremental": ("BM 과 SIZE 중 실행가능성과 경제적 우위가 더 좋은 **단독** "
                        "factor 의 portfolio 연구 하나(§29)."),
    "onFragile": "결합의 취약 원인 하나를 직접 규명하는 연구 하나.",
    "r26Forbidden": "R26 안에서 포트폴리오 구성·종목수·보유기간 설계 금지(§31).",
}

FORBIDDEN = [
    "BM/SIZE threshold sweep", "P10/P20/P30 비교", "size decile 다중 조합",
    "20/30/40/50종목 비교", "12/24/36개월 보유 최적화",
    "monthly/quarterly rebalance 비교", "score weight optimization",
    "BM 70% + SIZE 30% 류 weight search", "intersection 방식 동시 경쟁",
    "BM×SIZE×EY 확장", "Quality 재추가", "ROE rescue",
    "결과 보고 결합정의·horizon·기준 변경", "CAGR 최대 조합 찾기",
    "저가주/distress filter 신규 추가", "sector neutralization 신규 추가",
    "포트폴리오 parameter search", "실주문·실계좌·외부발송",
]

FAILURE_CONDITIONS = {
    "ENGINE_MISMATCH": "R25 엔진 동치가 깨지면 중단",
    "R25_REPRODUCTION_MISMATCH": "BM/SIZE 재현이 R25 수치와 다르면 중단",
    "RANK_DIRECTION_BUG": "백분위 방향이 뒤집히면 중단",
    "ELIGIBILITY_MISMATCH": "세 팔의 universe 가 다르면 중단",
    "NONDETERMINISTIC": "동일 입력 재실행이 다르면 중단",
    "onFailure": ("§41 대로 원인분리 → 최소수정 → targeted regression → R26 "
                  "전체 재실행 → 전체 regression. 결과를 좋게 만드는 변경 금지."),
}


def main() -> int:
    out = {
        "task": "R26", "taskId": TASK,
        "writtenBeforeResults": True,
        "researchQuestion": QUESTION,
        "secondaryQuestion": SECONDARY_QUESTION,
        "successDefinition": SUCCESS,
        "noParameterRescue": NO_RESCUE,
        "foundation": {
            "r24Verdict": "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "expectedBiasPct": 1.734, "unresolvedTickerPct": 2.20,
            "extremeDiscontinuityPct": 1.42,
            "r25Verdict": "R25_CANONICAL_FACTOR_REDISCOVERY_PRIMARY_FOUND",
            "r25Primary": ["BM", "SIZE_SMALL"], "r25Secondary": ["EY"],
            "r25Rule": "§34 규칙 B → BM×SIZE 단일 결합 연구"},
        "engine": ENGINE, "universe": UNIVERSE, "pitRule": PIT_RULE,
        "bmDefinition": BM, "sizeDefinition": SIZE,
        "combination": COMBINATION, "comparison": COMPARISON,
        "eligibility": ELIGIBILITY, "quantiles": QUANTILES,
        "horizons": HORIZONS, "incremental": INCREMENTAL,
        "subperiods": SUBPERIODS, "subperiodWhy": SUBPERIOD_WHY,
        "rolling": ROLLING, "bootstrap": BOOTSTRAP,
        "independence": INDEPENDENCE, "conditional": CONDITIONAL,
        "liquidity": LIQUIDITY, "priceBuckets": PRICE_BUCKETS,
        "priceBucketWhy": PRICE_BUCKET_WHY,
        "distress": DISTRESS, "concentration": CONCENTRATION,
        "sector": SECTOR, "cost": COST, "tsrLimitation": TSR_LIMITATION,
        "qualification": QUALIFICATION, "nextTaskRule": NEXT_TASK_RULE,
        "forbidden": FORBIDDEN, "failureConditions": FAILURE_CONDITIONS,
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r26-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (RD / "r26-combination-definition-latest.json").write_text(
        json.dumps({"task": "R26", "combination": COMBINATION,
                    "bmDefinition": BM, "sizeDefinition": SIZE,
                    "arms": ARMS, "comparison": COMPARISON,
                    "eligibility": ELIGIBILITY, "quantiles": QUANTILES,
                    "noParameterRescue": NO_RESCUE,
                    "onlyOneCombination": True},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"combination": COMBINATION["id"],
                      "formula": COMBINATION["formula"],
                      "primaryHorizon": HORIZONS["primary"],
                      "arms": ARMS,
                      "liquidityStatus": LIQUIDITY["status"],
                      "sectorStatus": SECTOR["status"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
