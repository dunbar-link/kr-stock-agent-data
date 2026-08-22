#!/usr/bin/env python3
"""R16 사전규격 — canonical TSR / wealth engine 정의를 결과 이전에 고정한다.

WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16

목적(§1): "주주가 실제로 얼마를 벌거나 잃었는가?" 에 신뢰할 수 있게 답하는 engine.
전략 연구(BM/SIZE/QUALITY/포트폴리오)는 이번 작업에서 하지 않는다(§16).

────────────────────────────────────────────────────────────────────────
조사로 확정된 데이터 사실 (엔진 구현 이전)
────────────────────────────────────────────────────────────────────────
A. PIT 스냅샷(월) 컬럼: ticker, market, close, marketCap, shares, PER, PBR,
   EPS, BPS, DIV, DPS.  → **raw close 중심**. adjusted price 시리즈가 아니다.
   따라서 §22 에 따라 `RAW_PRICE + EXPLICIT_EVENTS` 방식을 택한다(혼합 금지).

B. pykrx `get_stock_major_changes(ticker)` 가 **액면변경 이력(날짜·변경전·변경후)**
   을 준다 = DIRECT_SOURCE. 삼성전자 2018-05-04 액면 5000 → 100 (50:1) 확인.
   단 종목당 1콜이라 전 종목(약 3,000)은 이번 범위 밖 → anchor 검증에만 사용.

C. 파생탐지 22건 표본을 액면변경과 대조: **7건만 액면변경 확인, 15건은 액면 무변동**.
   무변동 15건의 배율은 1.50~2.13(주식수↑) 또는 0.10~0.50(주식수↓) 로
   **무상증자 / 무상감자**로 해석된다. 둘 다 보유주식수가 같은 배율로 변하므로
   조정 대상이 맞다. 즉 '액면변경 아님'이 '오탐'을 뜻하지 않는다.

D. 시총 연속성 전수 스캔(종목-월 533,478건, 주식수 1.2배 이상 변동 6,680건):
     MECHANICAL(가격 반비례 + 시총 연속)          3,153건
     SHARES_UP_MCAP_UP (유상증자 유력)            2,403건
     SHARES_DOWN_NONPROPORTIONAL (자사주 소각 등)   556건
     MECH_BUT_MCAP_JUMP (유상증자 의심)             359건
     SHARES_UP_NONPROPORTIONAL                    209건
   → **주식수 신호만으로 판단하면 유상증자를 무상증자로 오인해 가짜 wealth 를
     만든다.**
E. ★ 자체수정(L1 단위테스트에서 검출): 스냅샷 marketCap 은 close × shares 와 정확히
   일치한다(불일치 0/2,035). 따라서 mcapRatio 는 비례성 검정과 동치이며 **독립 신호가
   아니다**. 독립 신호는 주식수·가격 2개뿐이고, 유상증자를 무상증자와 분리할 독립
   근거가 없다. 비례성 허용오차를 0.30 → 0.15 로 조여 오분류를 줄인다(보수적 방향).

안전: 계산 0 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ canonical return 정의 (§2) ══════════════
CANONICAL = {
    "measure": "TOTAL_SHAREHOLDER_WEALTH",
    "method": "RAW_PRICE + EXPLICIT_EVENT_LEDGER",
    "whyNotAdjustedSeries": (
        "저장소가 raw close 중심이고 adjusted 시리즈가 없다. 두 방식을 섞으면 "
        "배당·분할 이중계상이 생긴다(§22). 명시적 event ledger 하나만 쓴다."),
    "primary": "DIVIDEND_REINVESTED_TOTAL_RETURN",
    "secondary": "NO_REINVEST_CASH_RETURN (audit 병기)",
    "unit": "보유 1주에서 출발하는 wealth ledger. shares_held·cash 를 추적한다.",
}

# ══════════════ 배당 convention (§12) ══════════════
DIVIDEND = {
    "source": "KRX DPS (직전 회계연도 연간 주당배당금, 특별배당 포함)",
    "accrual": "월별 소득수익률 = (DPS_t / 12) / close_t",
    "reinvestPrice": "같은 달 종가(close_t). 모든 종목·benchmark 에 동일 적용.",
    "transactionCost": "0 (factor 비교용 canonical. 포트폴리오 비용은 별도 단계)",
    "whyMonthlyAccrual": [
        "DPS 는 연간값이라 매월 통째로 더하면 12배 중복계상.",
        "배당기준일 데이터가 없어 배당락월 특정이 불가능.",
        "균등 발생은 보유기간이 길수록 정확히 연 1회분씩 누적하고 universe 전체에 "
        "일관 적용 가능하며 결과를 보고 조정할 자유도가 없다.",
    ],
    "missingVsZero": {
        "rule": ("DPS 컬럼이 **결측(빈칸/nan)** 이면 UNKNOWN, **0** 이면 무배당으로 "
                 "구분한다. 둘 다 소득수익률 0 으로 계산하되 ledger 에 flag 를 남긴다."),
        "why": "0 과 missing 을 같은 값으로 저장하면 커버리지 audit 이 불가능해진다.",
    },
    "specialDividend": "DPS 에 이미 합산돼 있어 별도 가산 금지(중복계상 방지).",
}

# ══════════════ 이벤트 분류기 (§3·§19) ══════════════
CLASSIFIER = {
    "signals": ["shareRatio = shares_t / shares_{t-1}",
                "priceRatio = close_{t-1} / close_t"],
    "signalIndependenceCorrection": {
        "found": "SELF_CORRECTION during L1 unit test",
        "what": ("초안은 mcapRatio 를 '독립 3번째 신호'로 규정했다. 실측 결과 스냅샷의 "
                 "marketCap 은 close × shares 와 **정확히 일치**한다(2,035건 검사, "
                 "불일치 0건). 즉 mcapRatio = shareRatio / priceRatio 이므로 "
                 "비례성 검정과 **수학적으로 동치**이고 독립 정보가 아니다."),
        "consequence": ("독립 신호는 주식수·가격 2개뿐이다. 유상증자를 무상증자와 "
                        "분리할 **독립 근거가 없다**. 이 사실이 BLOCKED 판정을 강화한다."),
        "toleranceTightened": {
            "from": 0.30, "to": 0.15,
            "why": ("0.30 은 느슨해서 '가격 무변동 + 주식수 감소'(자사주 소각)를 "
                    "기계적 사건으로 오분류했다(L1 합성테스트가 검출). 예: shareRatio "
                    "0.80 · priceRatio 1.00 이면 |1.00/0.80-1| = 0.25 < 0.30 으로 "
                    "통과해 보유주식을 잘못 줄였다. 0.15 로 조이면 배제된다."),
            "direction": "더 엄격 = 조정 건수 감소 = 보수적. 결과를 좋게 만드는 방향 아님.",
            "timing": "factor 결과를 만들기 전(§16 은 R16 에서 factor 연구를 금지한다).",
        },
    },
    "minShareRatio": 1.2,
    "proportionalTest": "relErr = |ln(priceRatio) - ln(shareRatio)| / |ln(shareRatio)|",
    "proportionalTolerance": 0.35,
    "mcapContinuityBand": [0.75, 1.35],
    "mcapBandNote": "비례성 검정과 동치. 독립 검증이 아니며 방어적으로만 남긴다.",
    "scaleCorrection": {
        "found": "SELF_CORRECTION 2 — 임계값 실측 후 기준 자체를 교체",
        "problem": ("절대오차 |priceRatio/shareRatio - 1| 의 전수 분포(9,032건)는 "
                    "0~0.30 이 거의 균일해 **골짜기가 없다**. 어떤 임계값도 자의적이다."),
        "cause": ("월간 스냅샷은 자본거래와 그 달의 주가변동을 분리하지 못한다. "
                  "자본거래 전후 소형주는 월 25%+ 움직이는 일이 흔하다."),
        "fix": "로그공간 상대오차로 교체 — 사건이 클수록 허용폭이 비례해 커진다.",
        "measuredSeparation": {
            "control": "정수비 주식수변동(2,856건) vs 비정수비(6,176건)",
            "absoluteMedian": [0.254, 0.324],
            "logRelativeMedian": [0.245, 0.834],
            "chosenThreshold": 0.35,
            "why": "분리력 최대 평탄구간 0.30~0.40(+35.8%p/+35.5%p)의 중점.",
        },
        "residualLimitation": ("최적 임계에서도 비정수비의 18.3% 가 통과하고 정수비의 "
                               "46% 가 탈락한다. 개별 사건 분류는 신뢰 불가."),
        "controlCaveat": ("정수비/비정수비는 완전한 정답이 아니다(무상증자 1.5배와 "
                          "유상증자 1.5배가 같은 비율을 낸다). 양쪽이 오염돼 있으므로 "
                          "측정된 분리력은 **하한**이다."),
    },
    "classes": {
        "MECHANICAL_SHARE_CHANGE": {
            "test": "relErr < 0.35 AND 0.75 <= mcapRatio <= 1.35",
            "covers": "액면분할 · 액면병합 · 무상증자 · 무상감자 · 주식배당",
            "action": "보유주식수 × shareRatio (wealth 보존)",
            "why": ("보유자의 주식수가 같은 배율로 변하고 기업가치는 그대로다. "
                    "액면변경 여부와 무관하게 경제적 처리가 동일하다."),
        },
        "SUSPECTED_RIGHTS_ISSUE": {
            "test": "shareRatio > 1 AND mcapRatio > 1.35 (기계적 서명 여부 무관)",
            "action": "**조정하지 않는다.** 보유주식수 불변.",
            "why": ("유상증자는 주주가 돈을 내야 신주를 받는다. 무상으로 주식을 "
                    "늘리면 가짜 wealth 가 생긴다. 신주배정비율·발행가 데이터가 없어 "
                    "권리가치를 계산할 수 없다 → RIGHTS_DATA_INCOMPLETE."),
        },
        "SHARES_DOWN_NONPROPORTIONAL": {
            "test": "shareRatio < 1 AND 기계적 서명 아님",
            "action": "조정하지 않는다.",
            "why": ("자사주 소각이 대표적이다. 총 주식수는 줄지만 **보유자의 주식수는 "
                    "그대로**이고 현금도 받지 않는다. 지분율 상승은 이미 주가에 반영."),
        },
        "SHARES_UP_NONPROPORTIONAL": {
            "test": "shareRatio > 1 AND 기계적 아님 AND mcapRatio <= 1.35",
            "action": "조정하지 않는다 → UNRESOLVED 로 표시.",
            "why": "유상증자·전환사채 전환·합병 신주 등이 섞여 있어 단정할 수 없다.",
        },
    },
    "directSourceUpgrade": (
        "pykrx get_stock_major_changes 의 액면변경 이력이 DIRECT_SOURCE 다. "
        "종목당 1콜이라 전 종목 적용은 R16 범위 밖. anchor 검증에만 쓰고, "
        "universe 적용은 다음 단계 과제로 남긴다."),
}

# ══════════════ 조정하지 않는 것 (§3) ══════════════
NO_ADJUSTMENT = {
    "TREASURY_BUYBACK": "회사가 자사주를 사도 주주에게 현금이 오지 않는다. 가산 금지.",
    "TREASURY_CANCELLATION": "보유주식수 불변 · 현금 없음. 지분율 효과는 주가에 반영.",
    "RIGHTS_ISSUE": "추가 납입이 필요하다. 무상 조정 금지. RIGHTS_DATA_INCOMPLETE.",
    "PHYSICAL_SPINOFF": "물적분할은 기존 주주에게 신주가 직접 지급되지 않는다. 조정 금지.",
    "MERGER": "교환비율 데이터 없음 → UNSUPPORTED_DATA.",
    "HUMAN_SPINOFF": "인적분할 배정비율 데이터 없음 → UNSUPPORTED_DATA.",
}

# ══════════════ 상장폐지 (§21) ══════════════
DELISTING = {
    "classes": ["ZERO_RECOVERY", "CASH_SETTLEMENT", "MERGER_EXCHANGE",
                "UNKNOWN_RECOVERY", "LAST_PRICE_ONLY_UNSAFE"],
    "canonicalDefault": "UNKNOWN_RECOVERY",
    "implementation": (
        "실제 정산금액 데이터가 없다. 기본값은 마지막 관측가 청산이되 ledger 에 "
        "**UNKNOWN_RECOVERY** flag 를 남기고, 보고서에서 이것이 상한 추정임을 명시한다."),
    "why": ("상폐를 0원으로 강제하면 실제 정산받은 주주를 과소평가하고, 마지막 종가로 "
            "종료하면 과대평가한다. 어느 쪽도 '정확'이 아니므로 UNKNOWN 을 숨기지 않는다."),
}

# ══════════════ external cash flow (§8) ══════════════
CASHFLOW = {
    "separation": ("EXTERNAL_CONTRIBUTION(주주가 넣은 돈)과 INVESTMENT_GAIN(투자 성과)을 "
                   "ledger 에서 분리한다."),
    "canonicalReturnBasis": "TIME_WEIGHTED (외부 현금흐름 중립화)",
    "why": ("factor 비교는 종목 선택의 성과를 봐야 하므로 외부 납입 규모에 좌우되면 "
            "안 된다. money-weighted 는 포트폴리오 단계에서 별도로 본다."),
    "currentState": ("R16 canonical 엔진은 유상증자를 조정하지 않으므로 external "
                     "contribution 이 0 이다. 필드는 ledger 에 두되 항상 0 이고, "
                     "rights 데이터가 확보되면 그때 채운다."),
}

# ══════════════ event 지원 상태 분류 (§6) ══════════════
SUPPORT_LEVELS = ["SUPPORTED_COMPLETE", "SUPPORTED_PARTIAL", "UNSUPPORTED_DATA",
                  "UNSUPPORTED_ENGINE", "NOT_APPLICABLE"]

# ══════════════ anchor tolerance (§10·§11) ══════════════
ANCHOR = {
    "tolerancePctPoints": 0.50,
    "samsungReproduction": {
        "period": "2010-01-04 → 2021-08-02",
        "splitAdjustedCagrPct": 14.71,
        "tsrNoReinvestCagrPct": 15.55,
        "tsrReinvestCagrPct": 16.36,
        "source": "R15 정본 (reports/research/r15-samsung-tsr-latest.json)",
    },
    "requiredTypes": ["SPLIT", "REVERSE_SPLIT_OR_REDUCTION", "BONUS_ISSUE",
                      "SUSPECTED_RIGHTS", "TREASURY_CANCELLATION", "DELISTING"],
    "note": ("합병·인적분할은 데이터가 없어 anchor 를 만들 수 없다 → BLOCKED_DATA 로 "
             "명시한다. mock 으로 통과시키고 지원한다고 선언하지 않는다(§23)."),
}

# ══════════════ anomaly 기준 (§25) ══════════════
ANOMALY = {
    "monthlyReturnOver": 5.00,
    "monthlyReturnUnder": -0.95,
    "shareRatioOver": 10.0,
    "shareRatioUnder": 0.1,
    "mcapRatioOver": 3.0,
    "mcapRatioUnder": 0.33,
    "dividendYieldOver": 0.50,
    "classes": ["TRUE_EVENT", "DATA_ERROR", "UNRESOLVED"],
}

# ══════════════ foundation 판정 (§17) ══════════════
FOUNDATION_VERDICTS = {
    "CANONICAL_TSR_FOUNDATION_PASS": (
        "핵심 corporate action 이 충분히 지원되고 anchor 가 manual ledger 와 일치하며 "
        "**중대 event 중 UNSUPPORTED_DATA 가 없음**"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS": (
        "split/dividend/delisting 등 핵심은 신뢰 가능하고 anchor 통과. 단 rights/"
        "merger/spinoff 등 일부가 데이터 부족이며 **영향 범위를 정량적으로 한정**할 수 있음"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED": (
        "중대 corporate action coverage 부족으로 장기 factor research 를 신뢰할 수 없음"),
    "CANONICAL_TSR_FOUNDATION_FAIL": "engine 자체 오류 미해결",
}

# ══════════════ 금지 (§1·§16) ══════════════
FORBIDDEN = {
    "factorResearch": ("BM · SIZE · QUALITY · ROE · Magic Formula · 보유기간 · 종목수 · "
                       "분할매수 · 포트폴리오 를 R16 에서 계산하지 않는다."),
    "why": "foundation PASS 전에 새 연구결과를 만들면 같은 문제를 반복한다.",
    "paidData": "유료 데이터 구매·신규 유료 API 가입 금지.",
    "newCrawler": "새 crawler framework 금지. 기존 pacing/backoff 재사용.",
    "overwritePrior": "R5~R15 산출물 삭제·수정 금지.",
}


def build():
    return {
        "schema": "wababa-canonical-tsr-research-foundation-reset-r16/precommit@1",
        "taskId": "WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16",
        "writtenBeforeEngineResults": True,
        "question": "주주가 실제로 얼마를 벌거나 잃었는가?",
        "dataFactsBeforePrecommit": {
            "snapshotColumns": ["ticker", "market", "close", "marketCap", "shares",
                                "PER", "PBR", "EPS", "BPS", "DIV", "DPS"],
            "priceSeriesType": "RAW_CLOSE (adjusted 아님)",
            "parChangeDirectSource": "pykrx get_stock_major_changes (종목당 1콜)",
            "derivedVsDirectSample": {"checked": 22, "parConfirmed": 7,
                                      "noParChange": 15,
                                      "interpretation": "무변동 15건은 무상증자/무상감자"},
            "shareEventScan": {"tickerMonths": 533478, "shareJumps1_2x": 6680,
                               "MECHANICAL": 3153, "SHARES_UP_MCAP_UP": 2403,
                               "SHARES_DOWN_NONPROPORTIONAL": 556,
                               "MECH_BUT_MCAP_JUMP": 359,
                               "SHARES_UP_NONPROPORTIONAL": 209},
        },
        "canonical": CANONICAL,
        "dividend": DIVIDEND,
        "classifier": CLASSIFIER,
        "noAdjustment": NO_ADJUSTMENT,
        "delisting": DELISTING,
        "cashflow": CASHFLOW,
        "supportLevels": SUPPORT_LEVELS,
        "anchor": ANCHOR,
        "anomaly": ANOMALY,
        "foundationVerdicts": FOUNDATION_VERDICTS,
        "forbidden": FORBIDDEN,
        "benchmarkInvariant": (
            "strategy · market benchmark · EW_VALID_PBR · KOSPI/KOSDAQ subset · size "
            "control 이 **모두 동일 canonical engine** 을 쓴다. 회귀 테스트로 고정한다(§13)."),
        "successCriterion": (
            "높은 CAGR 을 찾는 것이 아니다. '2007년 당시 어떤 종목을 산 주주가 배당·분할·"
            "증자·감자·자사주·합병·분할·상폐까지 겪은 뒤 실제로 얼마의 wealth 를 가졌는가' "
            "에 답할 수 있는 engine 을 만드는 것이다. 기존 연구가 전부 뒤집혀도 괜찮다."),
    }


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    out = RD / "r16-precommit-latest.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out), "method": CANONICAL["method"],
                      "primary": CANONICAL["primary"],
                      "minShareRatio": CLASSIFIER["minShareRatio"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
