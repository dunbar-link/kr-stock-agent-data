#!/usr/bin/env python3
"""R13 §5 — 사전규격. factor 결과를 보기 **전에** 저장한다.

WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13

연구질문(§1):
  BM 이 높은 저평가주 중 영업·자본·현금창출 능력이 좋은 기업을 구분하는 factor 가
  BM 단독보다 추가적인 장기 alpha 를 제공하는가?

이 파일은 데이터 가용성 **조사 결과만** 보고 작성했고, 어떤 factor 의 수익률도
계산하기 전에 저장한다. 저장 이후 definition 을 성과에 맞춰 바꾸지 않는다(§5).

────────────────────────────────────────────────────────────────────────
조사로 확정된 데이터 현실 (§4 — 이것이 규격을 결정했다)
────────────────────────────────────────────────────────────────────────
A. 장기 PIT 스냅샷 (_cache/pit-snapshots, 2007-01~2026-08, 236개월)
   컬럼: ticker, market, close, marketCap, shares, PER, PBR, EPS, BPS, DIV, DPS
   → 제공되는 것은 **순이익(EPS×주식수)·자본(BPS×주식수)·배당(DPS)** 뿐이다.
   → 자산총계·매출액·영업이익·영업활동현금흐름이 **없다**.
   → 따라서 자산기준/매출기준 profitability(ROIC·OP·CP)는 이 데이터로
     **구조적으로 계산 불가능**하다. 이것은 구현 난이도가 아니라 정보 부재다.

B. DART 재무제표 (_cache/dart-statements, FY2022~FY2025 4개 회계연도만)
   IFRS 표준태그 커버리지 97~100% (Assets/CurrentLiabilities/OperatingIncome/
   OperatingCashFlow/Revenue/Equity), 연도당 1,800~2,100개 기업.
   rcept_no 앞 8자리 = **실제 공시일**이라 진짜 PIT lag 적용 가능.
   실측 공시일 중앙값: FY2022 2023-03-21 · FY2023 2024-03-20 ·
                      FY2024 2025-03-20 · FY2025 2026-03-19
   → 투자결정 가능일은 대략 2023-04 ~ 2026-08.
   → 12M forward return 이 존재하는 결정월은 약 29개월(2023-04~2025-08),
     24M 은 약 17개월. **단일 시장국면**이라 시대별/시작시점 안정성 검증 불가.

C. financial-universe-real.json 은 **현재 시점 스냅샷**(오늘의 ROE·opMargin 등)이며
   시계열이 아니다. 백테스트에 쓰면 전면적 look-ahead 다. → 사용 금지.

→ 결론적으로 지시문 §2 가 요청한 P1/P2/P3(ROIC·Operating·Cash Profitability)는
  **DART 에서만** 계산 가능하고 그 구간은 통계적으로 얇다. 이 사실을 결과 이전에
  규격으로 고정하고, 장기 축과 DART 축을 분리해 둘 다 정직하게 보고한다.

안전: 계산 0 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════════ TRACK L — 장기 (2007~2026, 236개월) ══════════════════
# KRX PIT 필드만으로 계산 가능한 profitability/quality 축. 각 개념당 정의 1개.
TRACK_L = [
    {
        "id": "L1_EARNINGS_PERSISTENCE",
        "concept": "꾸준히 흑자를 내는가 (수익성의 지속성)",
        "formula": "직전 36개월 스냅샷 중 EPS > 0 인 달의 비율",
        "pitRule": "결정월 t 의 **이전** 스냅샷(t-36 ~ t-1)만 사용. t 자신 미포함.",
        "rankDirection": "높을수록 매력적",
        "missingRule": "직전 36개월 유효 관측이 12개 미만이면 그 달 랭킹에서 제외",
        "negativeDenominator": "해당 없음(비율)",
        "why": ("KRX 장기 데이터로 표현 가능한 '수익성' 중 ROE 수준(R7 INVERTED)과 "
                "가장 독립적인 축이다. 자산·매출이 없으므로 지속성으로 대체한다."),
    },
    {
        "id": "L2_CASH_PAYOUT_QUALITY",
        "concept": "회계이익이 실제 현금으로 나오는가 (현금창출의 대용)",
        "formula": "DPS / EPS  (EPS > 0 인 경우만 정의)",
        "pitRule": "결정월 t 스냅샷의 DPS·EPS (KRX 가 그 날 공표한 값)",
        "rankDirection": "높을수록 매력적",
        "missingRule": ("DPS 결측 또는 0 → 배당성향 0 으로 취급(경제적으로 의미 있는 "
                        "값이지 결측이 아니다). EPS <= 0 이면 정의 불가 → 랭킹 제외"),
        "negativeDenominator": "EPS <= 0 제외",
        "cap": "배당성향 3.0 초과는 3.0 으로 절단(사전확정, 성과 보고 조정 금지)",
        "why": ("영업현금흐름이 없으므로 '이익의 현금 전환'을 배당 실지급으로 근사한다. "
                "배당은 조작 불가능한 실제 현금 유출이다."),
    },
    {
        "id": "L3_CAPITAL_COMPOUNDING",
        "concept": "주주자본을 실제로 불려왔는가 (자본 복리성장)",
        "formula": "BPS 36개월 CAGR = (BPS_t-1 / BPS_t-36)^(12/35) - 1",
        "pitRule": "결정월 t 의 **이전** 스냅샷만 사용(t-36, t-1). t 자신 미포함.",
        "rankDirection": "높을수록 매력적",
        "missingRule": "t-36 또는 t-1 스냅샷에 BPS 없으면 랭킹 제외",
        "negativeDenominator": "BPS_t-36 <= 0 또는 BPS_t-1 <= 0 이면 제외",
        "why": ("ROE 수준이 아니라 **실현된 자본 복리**를 본다. 배당·증자를 거친 뒤 "
                "실제로 남은 성과다."),
    },
]

# ══════════════════ TRACK D — DART 실물 (FY2022~FY2025) ══════════════════
# 지시문 §2 가 요청한 P1/P2/P3 그 자체. 실제 공시일 PIT.
TRACK_D = [
    {
        "id": "D1_ROIC_PROXY",
        "requestedAs": "P1 ROIC 또는 재현 가능한 proxy",
        "concept": "투입한 영업자본 대비 영업이익",
        "formula": ("dart_OperatingIncomeLoss / (ifrs-full_Assets - "
                    "ifrs-full_CurrentLiabilities)"),
        "rankDirection": "높을수록 매력적",
        "negativeDenominator": "(자산총계 - 유동부채) <= 0 이면 제외",
        "missingRule": "세 항목 중 하나라도 결측이면 제외(0 으로 채우지 않는다)",
    },
    {
        "id": "D2_OPERATING_PROFITABILITY",
        "requestedAs": "P2 Operating Profitability",
        "concept": "보유 자산 대비 영업이익",
        "formula": "dart_OperatingIncomeLoss / ifrs-full_Assets",
        "rankDirection": "높을수록 매력적",
        "negativeDenominator": "자산총계 <= 0 이면 제외",
        "missingRule": "결측 제외",
        "variantChoiceRationale": ("영업이익/매출 대신 영업이익/자산을 택했다. 매출액 "
                                   "커버리지 96~97% < 자산 99.9% 이고, 자산기준이 "
                                   "자본투입 대비 수익성이라는 개념에 더 가깝다. "
                                   "결과를 보고 고른 것이 아니라 커버리지 조사 결과다."),
    },
    {
        "id": "D3_CASH_PROFITABILITY",
        "requestedAs": "P3 Cash Profitability",
        "concept": "자산 대비 실제 영업현금창출",
        "formula": ("ifrs-full_CashFlowsFromUsedInOperatingActivities / "
                    "ifrs-full_Assets"),
        "rankDirection": "높을수록 매력적",
        "negativeDenominator": "자산총계 <= 0 이면 제외",
        "missingRule": "결측 제외",
        "variantChoiceRationale": ("FCF Yield 대신 영업CF/자산을 택했다. FCF 는 "
                                   "CAPEX 계정명이 표준화돼 있지 않아(유형자산의 취득 "
                                   "75% 수준) PIT 품질이 낮다. 영업활동현금흐름 "
                                   "표준태그는 99% 다."),
    },
]

# ══════════════════ PIT lag (§4 — 최우선) ══════════════════
PIT_RULE = {
    "trackL": ("장기 축은 KRX 가 그 스냅샷 날짜에 실제 공표한 값만 쓴다. "
               "L1·L3 은 결정월 이전 스냅샷만 사용해 당월 정보조차 쓰지 않는다."),
    "trackD": {
        "rule": "각 (기업, 회계연도) 재무는 **실제 공시일(rcept_no 앞 8자리) + 1개월** "
                "이후의 결정월부터만 사용한다.",
        "bufferMonths": 1,
        "why": ("공시 당일 반영은 비현실적이라 1개월 버퍼를 더한다. 기존 와바바 "
                "저장소에 명시된 lag convention 이 없으므로 §4 의 '필요하면 더 "
                "보수적인 lag' 지침에 따라 보수적으로 잡았다."),
        "hardFail": "공시일 이전 결정월에 그 회계연도 재무가 들어가면 테스트 FAIL.",
        "latestWins": "결정월 시점에 공시된 것들 중 **가장 최근 회계연도**를 쓴다.",
    },
    "forbidden": ("연말 재무수치를 그 연말 이전 결정에 사용 금지. "
                  "financial-universe-real.json(현재 스냅샷) 사용 금지."),
}

# ══════════════════ 공통 규격 ══════════════════
COMMON = {
    "eligibility": ("R11 과 동일. investable_universe(시총 300억 이상 · 우선주/SPAC/"
                    "리츠/금융·지주 제외) ∩ 해당 factor 값 계산 가능."),
    "bmTop20Definition": ("R11 frozen P20 그대로. eligible = investable ∩ PBR>0 를 "
                          "BM 내림차순 정렬 후 상위 20%. P10/P30 금지(§8)."),
    "winsorization": {
        "rule": "값 winsorization 을 하지 않는다. **순위(rank) 기반 분위**만 쓴다.",
        "why": ("저장소에 정본 winsorization rule 이 없다. R7 이 이미 순위 분위 "
                "방식이므로 그것을 재사용한다. 순위는 극단치에 자동으로 둔감하고 "
                "percentile 을 조절해 성과를 올릴 여지가 없다(§12)."),
        "robustVariant": "없음. 사전규격으로 raw rank 단일 방식만 쓴다.",
    },
    "methodology": ("R7 정본 재사용 — factor_research.quantile_panel / summarize_factor / "
                    "verdict. 10분위, 분위당 최소 10종목, HORIZONS [3,6,12,24], "
                    "12M 연율 spread 가 주 지표. 새 methodology 를 만들지 않는다."),
    "horizons": [3, 6, 12, 24],
    "primaryHorizon": 12,
    "controls": ("R7 control_returns + R11 matched-control 논리 재사용. 새 최적화용 "
                 "matching 을 만들지 않는다(§10)."),
    "thresholds": {
        "STRONG_SPREAD": 0.03, "NOISE_BAND": 0.01, "GRADIENT_CORR": -0.6,
        "source": "R7 factor_research 상수 그대로. R13 에서 변경 0.",
    },
}

# ══════════════════ 판정 규칙 (§7·§14·§15·§25) ══════════════════
SIGNAL_VERDICTS = ["STRONG_SIGNAL", "PROMISING_SIGNAL", "WEAK_SIGNAL", "NO_SIGNAL",
                   "INVERTED_SIGNAL", "UNRELIABLE"]

FACTOR_GRADE = {
    "STRONG_CANDIDATE": ("단독 signal 양수 AND BM_TOP20 내부 spread 양수 AND "
                         "하위구간 대부분 양수 AND exchange/size 다수 양수 AND "
                         "matched control 방향 유지 AND top contributors 제거 후 "
                         "방향 유지 AND PIT/데이터품질 clean"),
    "PROMISING": "방향은 양수이나 일부 구간 불안정. 추가 portfolio 검증 가치 있음.",
    "WEAK_REJECT": ("BM 내부 incremental signal 거의 없음 / 특정 시대·시장·size 의존 / "
                    "극단치 의존 / look-ahead·데이터품질 위험 / ROE 처럼 역방향"),
}

# ★ 결과 이전에 고정하는 자격 제한 — 이것이 R13 판정의 핵심 사전규격이다.
ELIGIBILITY_FOR_PRIMARY = {
    "rule": ("PRIMARY_PROFITABILITY_FACTOR 는 **12M forward 관측이 존재하는 결정월이 "
             "60개월 이상**이고 **서로 다른 시장국면(R9/R10/R11 의 era 정의 기준) 2개 "
             "이상**을 포함하는 factor 에서만 선정한다."),
    "consequence": ("TRACK D 는 12M forward 결정월이 약 29개월·단일 국면이므로 이 "
                    "기준을 구조적으로 통과할 수 없다 → **PRIMARY 자격 없음**. "
                    "TRACK D 결과는 실물 profitability 의 방향성 참고(feasibility)로만 "
                    "보고한다."),
    "why": ("§7·§8 이 요구하는 subperiod consistency·start-date stability 를 29개월 "
            "단일 국면으로는 검증할 수 없다. 짧은 구간의 높은 spread 를 PRIMARY 로 "
            "승격시키는 것이 이 연구에서 가장 위험한 실수다. 그래서 결과를 보기 전에 "
            "자격 자체를 막아둔다."),
    "minDecisionMonthsWith12mForward": 60,
    "minDistinctEras": 2,
}

FINAL_VERDICTS = {
    "VALUE_PROFITABILITY_STRONG_CANDIDATE":
        "PRIMARY 자격 있는 factor 1개가 STRONG_CANDIDATE 등급",
    "VALUE_PROFITABILITY_PROMISING":
        "PRIMARY 자격 있는 factor 1개가 PROMISING 등급",
    "NO_INCREMENTAL_PROFITABILITY_SIGNAL":
        "PRIMARY 자격 있는 factor 중 BM_TOP20 내부 incremental signal 이 없음",
    "DATA_INSUFFICIENT":
        ("요청된 실물 profitability(ROIC/OP/CP)를 장기 PIT 로 검증할 데이터가 없고, "
         "장기 대용 축에서도 결론을 내릴 근거가 부족한 경우"),
}

NO_RESCUE = {
    "statement": ("결과가 나빠도 factor definition·PIT lag·winsorization·threshold·"
                  "BM percentile 을 바꾸지 않는다. 유리한 변형을 고르지 않는다. "
                  "각 개념당 정의는 이 파일에 1개씩 고정됐다."),
    "forbidden": ["PBR threshold sweep", "종목수 sweep", "보유기간 sweep",
                  "매수주기 sweep", "rebalance sweep", "3개 이상 factor 조합",
                  "ratio 무차별 탐색", "결과 보고 factor 추가", "정의 변경",
                  "CAGR 최고 조합 찾기", "parameter rescue",
                  "winsorization percentile 조절", "distress filter 를 투자규칙으로 추가"],
    "portfolioSearch": ("§16 — BM + profitability 40종목 포트폴리오 최종 CAGR 최적화 "
                        "금지. fixed-form sanity 검증만 허용하고 그 결과로 threshold/"
                        "weight 를 조절하지 않는다. 본격 조합은 R14 별도 task."),
    "roeRule": ("§3 — ROE 는 R7 에서 INVERTED_SIGNAL 이었다. R13 에서 ROE 를 주력 "
                "profitability 로 재탐색하지 않는다. historical reference · "
                "negative control · sanity comparison 으로만 쓴다."),
}

REFERENCE_R7 = {
    "note": "§18 비교표의 기준선. R7 정본 값이며 R13 에서 재현만 하고 수정하지 않는다.",
    "BM": {"verdict": "STRONG_SIGNAL", "spread12AnnPct": 16.58},
    "ROE": {"verdict": "INVERTED_SIGNAL", "spread12AnnPct": -6.70},
    "EY": {"verdict": "PROMISING_SIGNAL", "spread12AnnPct": 3.29},
    "BM_ROE": {"verdict": "WEAK_SIGNAL", "spread12AnnPct": 5.48},
    "MF": {"verdict": "NO_SIGNAL", "spread12AnnPct": -3.08},
    "SIZE": {"verdict": "WEAK_SIGNAL", "spread12AnnPct": 16.14},
}


def build():
    return {
        "schema": "wababa-value-profitability-factor-discovery-r13/precommit@1",
        "taskId": "WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13",
        "writtenBeforeResults": True,
        "researchQuestion": (
            "BM 이 높은 저평가주 중 영업·자본·현금창출 능력이 좋은 기업을 구분하는 "
            "factor 가 BM 단독보다 추가적인 장기 alpha 를 제공하는가?"),
        "stage": "FACTOR_DISCOVERY (포트폴리오 최적화 아님 — §1)",
        "dataAvailabilityFindings": {
            "longPitSnapshots": {
                "path": "_cache/pit-snapshots", "period": "2007-01 ~ 2026-08 (236개월)",
                "columns": ["ticker", "market", "close", "marketCap", "shares",
                            "PER", "PBR", "EPS", "BPS", "DIV", "DPS"],
                "implication": ("자산·매출·영업이익·영업CF 부재 → ROIC/OP/CP "
                                "구조적 계산 불가")},
            "dartStatements": {
                "path": "_cache/dart-statements",
                "fiscalYears": [2022, 2023, 2024, 2025],
                "companiesPerYear": "약 1,800~2,100",
                "tagCoveragePct": {"Assets": 99.9, "CurrentLiabilities": 97.5,
                                   "OperatingIncome": 97.4, "OperatingCashFlow": 99.2,
                                   "Revenue": 96.3, "Equity": 99.9},
                "filingDateSource": "rcept_no 앞 8자리 (실제 공시일)",
                "medianFilingDates": {"FY2022": "2023-03-21", "FY2023": "2024-03-20",
                                      "FY2024": "2025-03-20", "FY2025": "2026-03-19"},
                "usableDecisionWindow": "약 2023-04 ~ 2026-08",
                "decisionMonthsWith12mForward": "약 29개월 (2023-04 ~ 2025-08)",
                "implication": "단일 시장국면 · 시대별/시작시점 안정성 검증 불가"},
            "financialUniverseRealJson": {
                "path": "financial-universe-real.json",
                "nature": "현재 시점 스냅샷(시계열 아님)",
                "decision": "백테스트 사용 금지 — 전면적 look-ahead"},
        },
        "trackL": {"label": "장기 KRX PIT 대용 축 (2007~2026)", "factors": TRACK_L},
        "trackD": {"label": "DART 실물 profitability (FY2022~FY2025)",
                   "factors": TRACK_D,
                   "note": "지시문 §2 가 요청한 P1/P2/P3 그 자체"},
        "pitRule": PIT_RULE,
        "common": COMMON,
        "signalVerdicts": SIGNAL_VERDICTS,
        "factorGrade": FACTOR_GRADE,
        "eligibilityForPrimary": ELIGIBILITY_FOR_PRIMARY,
        "finalVerdicts": FINAL_VERDICTS,
        "noParameterRescue": NO_RESCUE,
        "referenceR7": REFERENCE_R7,
        "successCriterion": (
            "성공은 CAGR 20% 전략을 찾는 것이 아니다. '싼 주식 중에서 실제로 돈도 잘 "
            "버는 회사를 구분하는 추가 factor 가 한국 장기 PIT 데이터에 존재하는가' 를 "
            "객관적으로 끝내는 것이다. 없으면 없다고 결론낸다."),
    }


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    out = RD / "r13-precommit-latest.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out), "trackL": len(TRACK_L), "trackD": len(TRACK_D),
                      "primaryEligibilityMinMonths":
                          ELIGIBILITY_FOR_PRIMARY["minDecisionMonthsWith12mForward"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
