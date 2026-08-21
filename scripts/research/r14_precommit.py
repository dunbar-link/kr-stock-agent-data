#!/usr/bin/env python3
"""R14 §4 — 사전규격. factor 결과를 보기 **전에** 저장한다.

WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14

연구질문(§1):
  과거에 지속적으로 이익을 내고 자본가치를 성장시킨 기업은 1년이 아니라
  3년·5년·7년 장기 보유에서 더 높은 복리수익률을 보이는가?
  그리고 그 효과가 BM 상위 20% 저평가 universe 안에서도 추가로 존재하는가?

이 파일은 **데이터 가용성 조사 결과만** 보고 작성했고, 어떤 factor 의 수익률도
계산하기 전에 저장한다. 저장 이후 horizon·정의·판정기준을 바꾸지 않는다(§4·§24).

────────────────────────────────────────────────────────────────────────
조사로 확정된 표본 현실 (§10 — 이것이 판정 방식을 결정했다)
────────────────────────────────────────────────────────────────────────
스냅샷 2007-01 ~ 2026-08 (236개월). Q1·Q2 는 직전 36개월이 필요하므로
결정월은 2010-01 부터 시작한다.

  horizon   결정월수   첫 결정월     마지막 결정월    비중첩 코호트   연초 코호트
  1Y          188     2010-01      2025-08            16          16
  3Y          164     2010-01      2023-08             5          14
  5Y          140     2010-01      2021-08             3          12
  7Y          116     2010-01      2019-08             2          10
  10Y          80     2010-01      2016-08             1           -

→ **5Y 독립 관측이 3개, 7Y 가 2개다.** 이 표본으로는 어떤 방법을 써도
  통계적 유의성을 주장할 수 없다. 그래서 R14 의 판정은 p-value 가 아니라
  **방향 일관성**(horizon × 하위구간 × 거래소 × size × matched control)에 둔다.
  bootstrap 은 계산하되 '신뢰구간이 넓다'는 사실을 보여주는 용도로만 쓴다.
  10Y 는 독립 관측 1개라 핵심 판정에서 제외한다(§2 지침 그대로).

안전: 계산 0 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ Horizon (§2 — 결과 이전 고정) ══════════════
PRIMARY_HORIZON = 60
ROBUSTNESS_HORIZONS = [36, 84]
REFERENCE_HORIZON = 12
ALL_HORIZONS = [12, 36, 60, 84]

HORIZON_RULE = {
    "primaryMonths": PRIMARY_HORIZON,
    "robustnessMonths": ROBUSTNESS_HORIZONS,
    "referenceMonths": REFERENCE_HORIZON,
    "referencePurpose": "R13 12M 결과 재현 전용. 판정에 쓰지 않는다.",
    "whyPrimary5Y": [
        "기업의 재투자·복리 효과가 주가에 반영될 충분한 시간",
        "24개월보다 structural earnings compounding 관찰에 적합",
        "7Y 보다 역사 데이터 내 독립 표본 수 확보 가능(3 vs 2)",
    ],
    "excluded10Y": "독립 관측 1개 — 핵심 판정에서 제외(참고 계산도 하지 않는다).",
    "forbidden": ["18M", "30M", "42M", "54M", "72M", "결과를 보고 horizon 추가",
                  "horizon 별 최고값만 골라 보고", "보유기간 최적화"],
    "statement": ("horizon 은 이 파일에 고정됐다. 결과가 나쁘다고 다른 horizon 을 "
                  "찾지 않는다. 결과가 좋다고 horizon 을 늘리지도 않는다."),
}

# ══════════════ Factor (§3 — R13 정의 그대로 재사용) ══════════════
FACTORS = [
    {
        "id": "Q1_PROFIT_PERSISTENCE",
        "r13Id": "L1_EARNINGS_PERSISTENCE",
        "concept": "꾸준히 흑자를 내는가",
        "formula": "직전 36개월 스냅샷 중 EPS > 0 인 달의 비율",
        "pitRule": "결정월 t 의 **이전** 스냅샷(t-36 ~ t-1)만 사용. t 자신 미포함.",
        "rankDirection": "높을수록 매력적",
        "missingRule": "직전 36개월 유효 관측 12개 미만이면 랭킹 제외",
        "knownLimitation": ("'EPS 가 양수냐'만 보는 proxy 다. 이익의 크기·질을 "
                            "반영하지 못한다. R14 에서 정의를 바꾸지 않는다(§12)."),
    },
    {
        "id": "Q2_CAPITAL_COMPOUNDING",
        "r13Id": "L3_CAPITAL_COMPOUNDING",
        "concept": "주주자본을 실제로 불려왔는가",
        "formula": "BPS 36개월 CAGR = (BPS_t-1 / BPS_t-36)^(12/35) - 1",
        "pitRule": "결정월 t 의 **이전** 스냅샷만 사용(t-36, t-1). t 자신 미포함.",
        "rankDirection": "높을수록 매력적",
        "missingRule": "t-36 또는 t-1 에 BPS 없거나 <= 0 이면 제외",
        "knownLimitation": ("유상증자로 인한 BPS 증가와 이익 유보로 인한 증가를 "
                            "구분하지 못한다. §13 에서 audit 만 하고 filter 는 "
                            "추가하지 않는다."),
    },
    {
        "id": "Q3_DIVIDEND_DISCIPLINE",
        "r13Id": "L2_CASH_PAYOUT_QUALITY",
        "concept": "회계이익이 실제 현금으로 나오는가",
        "formula": "DPS / EPS (EPS > 0 인 경우만 정의) · 상한 3.0",
        "pitRule": "결정월 t 스냅샷의 KRX 공표 DPS·EPS",
        "rankDirection": "높을수록 매력적",
        "missingRule": "DPS 결측/0 → 배당성향 0. EPS <= 0 이면 랭킹 제외",
        "cap": 3.0,
        "knownLimitation": ("저PBR universe 에서 성숙·저성장 기업 노출일 수 있다. "
                            "§14 에서 exposure audit 만 한다."),
    },
]
NEW_FACTOR_DISCOVERY = {
    "allowed": False,
    "statement": ("새 factor 발굴 금지(§3). ROIC·영업이익률·FCF·영업CF 는 장기 PIT "
                  "데이터가 없어 억지 backfill 하지 않는다. ROE 는 negative control, "
                  "SIZE 는 attribution/control 목적으로만 쓴다."),
}

# ══════════════ 공통 규격 ══════════════
COMMON = {
    "universe": ("R7/R13 과 동일. investable_universe(시총 300억 이상 · 우선주/SPAC/"
                 "리츠/금융·지주 제외) · survivorship-free."),
    "bmTop20": ("R11 frozen P20 그대로. eligible = investable ∩ PBR>0 를 BM 내림차순 "
                "정렬 후 상위 20%. P10/P30 탐색 금지(§8)."),
    "bucket": "R7 정본 10분위. 분위당 최소 10종목. TOP=D1, BOTTOM=D10.",
    "spreadMetric": ("TOP−BOTTOM 의 forward total return 차이. 누적수익과 연율화 "
                     "CAGR 을 모두 보고한다(§6)."),
    "annualization": "(1+r)^(12/h) - 1",
    "delisting": ("R7 정본 fwd_return 재사용 — j 시점에 종목이 없으면 마지막 관측가로 "
                  "청산. survivorship-free. haircut 0(R7·R13 과 동일 가정)."),
    "costRelevance": ("R14 는 factor spread 검증이지 포트폴리오가 아니다. 거래비용을 "
                      "부과하지 않는다. 다만 장기 보유(3~7년)는 회전율이 낮아 비용 "
                      "민감도가 12M 보다 **낮다**는 점을 보고서에 명시한다."),
    "overlapMethod": ("§10 — OVERLAPPING(월별 전체) · ANNUAL_START(매년 1월 시작) · "
                      "NON_OVERLAPPING(horizon 간격) 세 가지를 모두 산출한다. "
                      "겹치는 월별 표본 수가 많다는 이유로 강한 통계결론을 내지 않는다."),
    "benchmarkControl": ("장기 spread 의 기준은 같은 시점 universe 평균(TOP−UNIVERSE)과 "
                         "BOTTOM 분위다. matched control 은 R13 방식 재사용 — "
                         "(시장 × 시총5분위) 셀 안에서만 상/하위를 비교한다(§15)."),
    "winsorization": "값 winsorization 없음. 순위 기반 분위만 사용(R7·R13 동일).",
    "methodologyReuse": ("factor_research.quantile_panel / summarize_factor 재사용. "
                         "horizon 만 가산 파라미터로 확장한다. 새 methodology 0."),
}

# ══════════════ 표본 현실 (§10·§11) ══════════════
SAMPLE_REALITY = {
    "snapshotMonths": 236, "period": "2007-01 ~ 2026-08",
    "firstDecisionMonth": "2010-01 (Q1·Q2 가 직전 36개월을 요구)",
    "byHorizon": {
        "12": {"decisionMonths": 188, "nonOverlapping": 16, "annualStarts": 16,
               "lastDecision": "2025-08"},
        "36": {"decisionMonths": 164, "nonOverlapping": 5, "annualStarts": 14,
               "lastDecision": "2023-08"},
        "60": {"decisionMonths": 140, "nonOverlapping": 3, "annualStarts": 12,
               "lastDecision": "2021-08"},
        "84": {"decisionMonths": 116, "nonOverlapping": 2, "annualStarts": 10,
               "lastDecision": "2019-08"},
    },
    "consequence": (
        "5Y 독립 관측 3개 · 7Y 2개. 이 표본으로는 통계적 유의성을 주장할 수 없다. "
        "따라서 판정은 **방향 일관성**(horizon × 하위구간 × 거래소 × size × matched "
        "control)에 둔다. bootstrap 은 신뢰구간의 **넓이를 보여주는 용도**이지 "
        "유의성 근거가 아니다."),
    "asymmetry": (
        "이 표본 한계는 대칭이 아니다. 효과가 **강하고 일관되게 음수**면 표본이 적어도 "
        "기각은 타당하다(방향이 모든 축에서 같으므로). 반대로 **양수**가 나오면 독립 "
        "관측 3개로는 확증할 수 없으므로 최대 PROMISING 까지만 가능하고 "
        "STRONG_CANDIDATE 는 주지 않는다. 이 비대칭을 결과 이전에 고정한다."),
}

# ══════════════ Horizon shape 분류 (§9) ══════════════
HORIZON_SHAPE = {
    "A_LONG_HORIZON_STRENGTHENING": "horizon 이 길수록 spread 가 개선(1Y<3Y<5Y<7Y 경향)",
    "B_SHORT_TERM_ONLY": "1Y/3Y 만 양수이고 5Y/7Y 에서 약화·소멸",
    "C_NO_HORIZON_SIGNAL": "모든 horizon 에서 |spread| 가 작아 무의미",
    "D_INVERTED": "장기에서도 지속적으로 음수",
    "E_UNSTABLE": "horizon 별 부호가 급변",
}

# ══════════════ 판정 기준 (§22 — 결과 이전 고정) ══════════════
FACTOR_VERDICTS = {
    "LONG_HORIZON_STRONG_CANDIDATE": {
        "conditions": [
            "BM_TOP20 5Y 연율 spread 명확히 양수(>= +2%p)",
            "3Y·7Y 도 방향 일관(양수)",
            "5Y 연초 코호트 12개 중 8개 이상 양수",
            "size+exchange matched 후에도 양수",
            "하위구간 대부분 양수",
            "상위 5 기여종목 제거 후에도 방향 유지",
            "PIT/데이터 clean",
            "경제적으로 설명 가능",
        ],
        "note": ("표본 비대칭(§SAMPLE_REALITY.asymmetry)에 따라 양수 결과에는 이 등급을 "
                 "부여하지 않는다. 독립 5Y 관측 3개로는 STRONG 을 주장할 수 없다. "
                 "이 등급은 사실상 도달 불가로 사전 설정됐고, 그것이 의도다."),
        "reachable": False,
    },
    "LONG_HORIZON_PROMISING": {
        "conditions": ["BM_TOP20 5Y 양수", "3Y·7Y 대체로 양수",
                       "matched control 후 방향 유지", "후속 portfolio 검증 가치 있음"],
        "reachable": True,
    },
    "NO_LONG_HORIZON_SIGNAL": {
        "conditions": ["BM_TOP20 5Y <= 0", "3Y·7Y 도 약함",
                       "matched control 에서 소멸"],
        "reachable": True,
    },
    "INVERTED_LONG_HORIZON": {
        "conditions": ["모든 horizon(1Y·3Y·5Y·7Y)에서 지속적으로 음수",
                       "matched control 후에도 음수"],
        "reachable": True,
    },
    "DATA_INSUFFICIENT": {
        "conditions": ["5Y/7Y 표본이 판정 자체를 불가능하게 하는 경우",
                       "예: 분위 구성 실패 · 관측 부족으로 spread 계산 불가"],
        "reachable": True,
    },
}

PRIMARY_SELECTION = {
    "maxFactors": 1,
    "order": ["BM 내부 5Y incremental strength", "3Y/7Y consistency",
              "matched-control robustness", "concentration robustness",
              "PIT reliability", "economic interpretation"],
    "forbidden": "CAGR 최고 factor 를 고르지 않는다.",
    "noneAllowed": "조건을 만족하는 factor 가 없으면 PRIMARY_QUALITY_FACTOR = NONE.",
}

NO_RESCUE = {
    "statement": ("결과가 나빠도 horizon·factor 정의·BM percentile·분위수·판정기준을 "
                  "바꾸지 않는다. 유리한 변형을 고르지 않는다."),
    "forbidden": ["horizon 최적화", "보유기간 최적화", "새 factor 추가",
                  "정의 변경", "threshold 조정", "20/30/40종목 비교",
                  "BM/quality weight 최적화", "intersection percentile 최적화",
                  "rebalance frequency 탐색", "CAGR 최고 조합 찾기",
                  "결과를 보고 filter 추가"],
    "portfolioSearch": ("§24 — R14 는 '장기 quality signal 존재 여부'만 확정한다. "
                        "포트폴리오 최적화는 전면 금지. 살아남은 factor 가 있으면 "
                        "R15 별도 task 에서 사전규격 기반으로 검증한다."),
    "r13Interpretation": ("R13 을 '잘 버는 회사 효과 없음'으로 일반화하지 않는다. "
                          "정확한 해석은 '현재 장기 PIT 로 쓸 수 있는 quality proxy 는 "
                          "12M forward 에서 BM 내부 추가 alpha 를 보이지 않았다' 이다."),
}

REFERENCE_R13 = {
    "note": "§34-5 재현 대상. R13 정본 값이며 R14 에서 재현만 하고 수정하지 않는다.",
    "bmTop20Internal12M": {"Q1_PROFIT_PERSISTENCE": -26.47,
                           "Q2_CAPITAL_COMPOUNDING": -31.73,
                           "Q3_DIVIDEND_DISCIPLINE": -7.48},
    "standalone12M": {"Q1_PROFIT_PERSISTENCE": -13.81,
                      "Q2_CAPITAL_COMPOUNDING": -24.69,
                      "Q3_DIVIDEND_DISCIPLINE": 7.49},
    "sizeInsideBmTop20_12M": 15.08,
    "r11BmReference": {"tag": "BM_P20_N40_H24_MONTHLY_STAGGER", "cagrPct": 11.50,
                       "mddPct": -53.38,
                       "note": ("§21 — R14 는 최종 portfolio 가 아니므로 이 값과 "
                                "factor spread 를 동일 전략처럼 직접 비교하지 않는다.")},
}


def build():
    return {
        "schema": "wababa-quality-compounder-long-horizon-r14/precommit@1",
        "taskId": "WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14",
        "writtenBeforeResults": True,
        "researchQuestion": (
            "과거에 지속적으로 이익을 내고 자본가치를 성장시킨 기업은 1년이 아니라 "
            "3년·5년·7년 장기 보유에서 더 높은 복리수익률을 보이는가? 그리고 그 효과가 "
            "BM 상위 20% 저평가 universe 안에서도 추가로 존재하는가?"),
        "stage": "LONG_HORIZON_SIGNAL_VALIDATION (포트폴리오 최적화 아님 — §24)",
        "horizonRule": HORIZON_RULE,
        "factors": FACTORS,
        "newFactorDiscovery": NEW_FACTOR_DISCOVERY,
        "common": COMMON,
        "sampleReality": SAMPLE_REALITY,
        "horizonShape": HORIZON_SHAPE,
        "factorVerdicts": FACTOR_VERDICTS,
        "primarySelection": PRIMARY_SELECTION,
        "noParameterRescue": NO_RESCUE,
        "referenceR13": REFERENCE_R13,
        "successCriterion": (
            "성공은 높은 CAGR 을 억지로 만드는 것이 아니다. '잘 버는·복리성장하는 "
            "기업이라는 특성이 한국 시장에서 1년이 아니라 3~7년 장기 보유에서 비로소 "
            "주가수익률로 나타나는가', 그리고 '그 효과가 BM 상위 20% 안에서도 "
            "추가로 존재하는가' 에 정확히 답하는 것이다. NO 이면 '보유기간이 짧아서 "
            "수익성 factor 가 실패했다'는 가설도 함께 폐기한다."),
    }


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    out = RD / "r14-precommit-latest.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out), "primaryHorizon": PRIMARY_HORIZON,
                      "robustness": ROBUSTNESS_HORIZONS, "factors": len(FACTORS),
                      "nonOverlapping5Y": SAMPLE_REALITY["byHorizon"]["60"]["nonOverlapping"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
