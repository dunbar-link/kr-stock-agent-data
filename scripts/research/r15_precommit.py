#!/usr/bin/env python3
"""R15 사전규격 — TSR 측정 방식을 **수정 결과를 보기 전에** 고정한다.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

§10 요구: "기본 연구 정본은 경제적으로 타당하고 dataset 전체에서 일관되게 구현 가능한
한 방식을 precommit. 결과가 좋아지는 방식을 사후 선택하지 마라."

이 파일은 **조사 단계에서 확인한 데이터 사실만** 보고 작성했고, 수정된 R14 결과를
계산하기 전에 저장한다.

────────────────────────────────────────────────────────────────────────
조사로 확인된 사실 (수정 이전)
────────────────────────────────────────────────────────────────────────
1. 스냅샷 close 는 **실제 거래가**이고 액면분할이 반영돼 있지 않다.
   삼성전자 2018-05 2,650,000원 → 2018-06 51,300원 (주식수 128,386,494 → 6,419,324,700).
   현행 fwd_return 은 p1/p0-1 = **-98.06%** 로 기록한다. 실제 시총 변화는 -3.2%.
2. 전 구간 종목-월 585,412건 중 주식수 1.5배 이상 변동 5,680건(0.970%),
   그중 split/무상 서명(가격이 주식수에 반비례)이 2,207건(0.377%).
   역분할은 **가짜 +3,000~4,900% 수익**을 만든다(더 치명적).
3. fwd_return 은 close 만 쓴다 → **PRICE RETURN**. DIV/DPS 컬럼은 존재하지만
   수익률 계산에 전혀 쓰이지 않는다.
4. DPS 는 **직전 연간 주당배당금**이다(DIV = DPS/close × 100 으로 검증).
   삼성전자 2021-07 DPS 2,994 = FY2020 정규 1,416 + 특별 1,578. 즉 특별배당이
   DPS 에 이미 포함돼 있다.
5. 자사주 소각은 주식수만 줄고 가격은 비례해서 오르지 않는다
   (삼성전자 2018-07 6,419M → 2019-01 5,970M, 비율 0.93). split 서명에 안 걸린다.

안전: 계산 0 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ 분할/무상증자 조정 (기계적 사건) ══════════════
SPLIT_RULE = {
    "detection": (
        "연속 두 스냅샷에서 주식수 배율 sr = shares_j/shares_i 가 sr >= 1.5 또는 "
        "sr <= 1/1.5 이고, 동시에 가격 역배율 pr = P_i/P_j 가 sr 과 일치"
        "(|pr/sr - 1| < 0.30)하면 SPLIT/BONUS 로 판정한다."),
    "adjustmentFactor": "sr (주식수 배율). 보유 1주가 sr 주가 된다.",
    "economicMeaning": (
        "액면분할·무상증자는 기업가치 증가가 아니다. 주식수와 주당가격을 기계적으로 "
        "바꾼다. 따라서 보유주식수를 같은 배율로 늘려 wealth 를 불변으로 만든다."),
    "whyBothConditions": (
        "주식수만 보면 유상증자(주식수↑, 가격은 비례하락 안 함)와 자사주 소각"
        "(주식수↓, 가격 비례상승 안 함)이 섞인다. 가격 역배율 일치 조건이 그 둘을 "
        "걸러낸다. 실측: 주식수 1.5배 이상 변동 5,680건 중 서명 일치는 2,207건뿐."),
    "tolerance": 0.30,
    "minRatio": 1.5,
}

# ══════════════ 배당 (실제 현금 이전) ══════════════
DIVIDEND_RULE = {
    "dataSemantics": (
        "DPS = 직전 회계연도 **연간** 주당배당금(특별배당 포함). DIV = DPS/close×100. "
        "월별 배당금이 아니다."),
    "accrual": (
        "월별 소득수익률 = (DPS_t / 12) / P_t. 즉 직전 연간 DPS 를 12개월에 걸쳐 "
        "균등 발생시킨다."),
    "whyThisWay": [
        "DPS 를 매월 통째로 더하면 연 12배 중복계상이 된다(치명적 과대).",
        "배당락월에만 한 번 더하려면 각 기업의 배당기준일이 필요한데 그 데이터가 없다.",
        "균등 발생은 5년 보유 시 약 5회분 연간배당을 정확히 누적하고, universe 전체에 "
        "일관되게 적용 가능하며, 결과를 보고 조정할 자유도가 없다.",
        "특별배당은 DPS 에 이미 포함돼 있어 별도 처리가 불필요하다(삼성 FY2020 검증).",
    ],
    "primary": "TOTAL_RETURN_WITH_REINVEST",
    "primaryDefinition": (
        "매월 보유주식수 × (1 + 월 소득수익률). 배당을 같은 종목에 재투자한다. "
        "표준 TSR 정의이고 분할 조정과 자연스럽게 합성된다."),
    "secondary": "TOTAL_RETURN_NO_REINVEST",
    "secondaryDefinition": "배당을 현금으로 적립(재투자 없음). 비교 표시용.",
    "precommitStatement": (
        "PRIMARY 를 재투자 방식으로 **결과를 보기 전에** 고정한다. 수정된 R14 결과가 "
        "어느 쪽에서 더 좋게 나오든 방식을 바꾸지 않는다."),
    "missingRule": "DPS 결측 또는 <= 0 이면 그 달 소득수익률 0(무배당으로 취급).",
}

# ══════════════ 조정하지 않는 것 (경제적 근거) ══════════════
NO_ADJUSTMENT = {
    "TREASURY_CANCELLATION": (
        "자사주 소각은 주식수가 줄지만 **기존 주주의 보유주식수는 그대로**다. "
        "현금이 주주에게 들어오지도 않는다. 잔존주주 지분율 상승 효과는 이미 주가에 "
        "반영되므로 수익률에 별도 가산하면 이중계상이다. → 조정 없음."),
    "RIGHTS_ISSUE": (
        "유상증자는 주주가 **돈을 추가로 내야** 새 주식을 받는다. 신주배정비율·발행가·"
        "청약 여부 데이터가 이 저장소에 없다. 조정을 흉내내면 가짜 정밀도가 된다. "
        "→ 조정 없음. 그 결과 권리행사 주주의 실제 수익을 과소평가할 수 있고, "
        "권리를 포기한 주주에게는 오히려 정확하다. 한계로 명시한다."),
    "MERGER_SPINOFF": (
        "합병·인적분할의 배정비율 데이터가 없다. 분할 서명(가격 역배율 일치)에 걸리면 "
        "분할로 처리되고, 아니면 미조정이다. 한계로 명시한다."),
    "DELISTING": "R7 정본 그대로 — 마지막 관측가로 청산(haircut 0). 변경 없음.",
}

# ══════════════ R14 재실행 규칙 (§13) ══════════════
REVALIDATION_RULE = {
    "trigger": "split 오류 또는 배당 누락이 material 이면 R14 Q1/Q2/Q3 전체 재실행.",
    "unchanged": [
        "factor 정의(Q1 흑자지속 · Q2 BPS 36M CAGR · Q3 배당성향)",
        "BM TOP20 (R11 frozen P20)",
        "horizon 1Y/3Y/5Y/7Y (PRIMARY 5Y)",
        "10분위 bucket · 분위당 최소 10종목",
        "matched control 방식", "bootstrap seed/방법", "판정 gate",
    ],
    "changedOnly": "RETURN MEASUREMENT (분할 조정 + 배당 포함) 하나뿐.",
    "parameterRescue": 0,
    "statement": (
        "수정 후 결과가 R14 를 뒤집든 유지하든 factor 정의·horizon·gate 를 "
        "건드리지 않는다. 기존 R7~R14 산출물은 삭제·수정하지 않고 provenance 로 "
        "보존한다(§24)."),
}

VERDICT_OPTIONS = {
    "TSR_ENGINE_VERDICT": ["TSR_CORRECT", "TSR_MINOR_GAPS",
                           "TSR_MATERIAL_BUG_FIXED", "TSR_MATERIAL_BUG_BLOCKED_DATA"],
    "R14_QUALITY_VERDICT_AFTER_TSR": ["UNCHANGED_INVERTED", "WEAK_OR_INCONCLUSIVE",
                                      "LONG_HORIZON_PROMISING", "LONG_HORIZON_STRONG"],
}

SIZE_RESEARCH = {
    "started": False,
    "statement": ("§17 — R15 에서 SIZE 전략 연구를 시작하지 않는다. return measurement "
                  "판정이 먼저다."),
}


def build():
    return {
        "schema": "wababa-tsr-corporate-action-forensic-r15/precommit@1",
        "taskId": "WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15",
        "writtenBeforeCorrectedResults": True,
        "purpose": ("R7~R14 의 장기 forward-return engine 이 실제 주주의 Total "
                    "Shareholder Return 을 정확히 측정했는가를 forensic 확인한다. "
                    "새 factor 탐색이 아니다."),
        "findingsBeforePrecommit": {
            "splitUnadjusted": True,
            "samsung2018": {"prevClose": 2650000, "nextClose": 51300,
                            "sharesBefore": 128386494, "sharesAfter": 6419324700,
                            "engineReturnPct": -98.06, "marketCapChangePct": -3.2},
            "splitLikeEvents": 2207, "shareJumpEvents": 5680,
            "tickerMonths": 585412,
            "returnIsPriceOnly": True,
            "dpsIsTrailingAnnual": True,
        },
        "splitRule": SPLIT_RULE,
        "dividendRule": DIVIDEND_RULE,
        "noAdjustment": NO_ADJUSTMENT,
        "revalidationRule": REVALIDATION_RULE,
        "verdictOptions": VERDICT_OPTIONS,
        "sizeResearch": SIZE_RESEARCH,
        "successCriterion": (
            "지난 약 20년의 수익률을 실제 주주가 경험한 경제적 수익으로 계산하고 "
            "있었는지 확정한다. 틀렸으면 원인을 전체 연구에 반영하고, R14 를 뒤집더라도 "
            "기존 보고서는 보존한 채 corrected evidence 를 새 정본으로 추가한다."),
    }


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    out = RD / "r15-precommit-latest.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out),
                      "primaryDividendMethod": DIVIDEND_RULE["primary"],
                      "splitTolerance": SPLIT_RULE["tolerance"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
