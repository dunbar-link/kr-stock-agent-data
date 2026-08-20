#!/usr/bin/env python3
"""R9 §17 — 판정규칙을 **결과를 보기 전에** 파일로 고정한다.

WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9

이 파일이 R9 의 계약서다. 실행 순서상 **가장 먼저** 실행해 산출물을 남기고,
이후 어떤 검증 결과가 나와도 이 규칙을 수정하지 않는다.
(R8 에서 stress +2%p 기준을 못 넘겼다는 이유로 기준을 낮추지 않는다 — §17)

frozen candidate 정의도 여기에 있고, R9 는 이 정의를 단 하나도 바꾸지 않는다.
parameter search 금지(§18): percentile·holdings·hold·deployment·replacement 를
"결과가 나쁘니 조정" 하는 코드는 R9 어디에도 없다.

안전: 계산 0 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ─────────────────────── FROZEN CANDIDATE (변경 금지) ───────────────────────
FROZEN = {
    "id": "BM_P20_N40_H24_COHORT",
    "r8Tag": "BM_P20_N40_H24_STAG12M_FIXE_COHORT",
    "initialCapitalKrw": 50_000_000,
    "factor": "BM",
    "factorDefinition": "BM = 1 / PBR (KRX 공표 PBR, PIT)",
    "selectionPool": "당시 PIT investable universe 의 BM 상위 20% 구간",
    "holdings": 40,
    "weighting": "균등(정수주)",
    "entry": "12개월 분할 진입 (STAG12M)",
    "holdMonths": 24,
    "exit": "만기 후 전량 교체 (FIXED_MATURITY_REPLACE)",
    "buyEveryMonths": 24,
    "reinvest": "매도대금 전액 재투자",
    "params": {"factor": "BM", "percentile": 0.20, "holdings": 40, "hold": 24,
               "deployment": "STAG12M", "replacement": "FIXED_MATURITY_REPLACE",
               "buy_every": 24, "monthly": 0},
    "r8Measured": {"cagrPct": 13.44, "benchPct": 8.83, "excessPct": 4.60,
                   "mddPct": -43.79, "underwaterMonths": 61,
                   "startMonthBeats": "10/12", "tradesPerYear": 37,
                   "avgPositions": 36.6, "burden": "SIMPLE"},
    "r8Verdict": "PROMISING_OBSERVE",
}

# ─────────────────────── 재현 허용오차 (§1 — 사전 정의) ───────────────────────
REPRO_TOLERANCE = {
    "cagrPctAbs": 0.05, "benchPctAbs": 0.05, "excessPctAbs": 0.05,
    "mddPctAbs": 0.50, "tradesAbs": 5, "avgPositionsAbs": 0.5,
    "underwaterMonthsAbs": 0,
    "note": "이 오차를 설명 없이 넘으면 R9 본검증 중단 → BLOCKED (§1)",
}

# ─────────────────────── 시장시대 구간 (§3 — 결과 전 확정) ───────────────────────
ERAS = [
    {"id": "E1_GFC", "label": "글로벌 금융위기", "from": "2007-01", "to": "2009-06"},
    {"id": "E2_RECOVERY", "label": "위기 후 회복·차화정", "from": "2009-07", "to": "2011-12"},
    {"id": "E3_BOX", "label": "박스피 장기 횡보", "from": "2012-01", "to": "2016-12"},
    {"id": "E4_CYCLE", "label": "반도체 사이클·미중분쟁", "from": "2017-01", "to": "2019-12"},
    {"id": "E5_COVID", "label": "COVID 급락·유동성 급등", "from": "2020-01", "to": "2021-12"},
    {"id": "E6_TIGHTEN", "label": "금리인상·최근", "from": "2022-01", "to": "2026-12"},
]

# ─────────────────────── stress 단계 (§6·§7·§8 — 결과 전 확정) ───────────────────────
COST_LEVELS = [
    {"id": "BASE", "feeBps": 15.0, "sellTaxBps": 20.0, "slippageBps": 0.0,
     "note": "R8 가정. 종가 체결·슬리피지 0 (가장 유리)"},
    {"id": "MODERATE", "feeBps": 15.0, "sellTaxBps": 23.0, "slippageBps": 30.0,
     "note": "실제 증권거래세 0.23% + 편도 30bp 슬리피지"},
    {"id": "HIGH", "feeBps": 20.0, "sellTaxBps": 23.0, "slippageBps": 100.0,
     "note": "소형·코스닥 현실: 편도 100bp 슬리피지 + 높은 수수료"},
]

DELIST_LEVELS = [
    {"id": "BASE", "haircut": 0.00, "note": "R8 가정 — 마지막 관측가 전액 회수"},
    {"id": "LOSS_75", "haircut": 0.75, "note": "상장폐지 시 75% 손실"},
    {"id": "LOSS_90", "haircut": 0.90, "note": "상장폐지 시 90% 손실"},
    {"id": "LOSS_100", "haircut": 1.00, "note": "상장폐지 = 전액 손실"},
]
DELIST_GATE_MODE = "SYMMETRIC"
DELIST_MODE_NOTE = (
    "게이트는 SYMMETRIC(전략·benchmark 에 동일 haircut) 으로 판정한다. "
    "benchmark 도 같은 상장폐지 종목을 들고 있으므로 전략만 haircut 을 먹이는 것은 "
    "방법론적으로 틀렸다. 단 ASYMMETRIC(전략만 haircut = R8 방식) 도 함께 보고해 "
    "최악 귀속을 숨기지 않는다.")

LIQUIDITY_STRESS = [
    {"id": "BASE", "minMarketCapEok": 300, "excludeStalePrice": False},
    {"id": "MCAP_1000", "minMarketCapEok": 1000, "excludeStalePrice": False,
     "note": "시총 하한 1,000억 — 실제 체결 가능성 확보"},
    {"id": "MCAP_1000_NOSTALE", "minMarketCapEok": 1000, "excludeStalePrice": True,
     "note": "+ 직전 2개월 종가 무변동(거래부진 proxy) 종목 제외"},
]

CONTROL_SEEDS = list(range(20260820, 20260820 + 30))

# ─────────────────────── 판정 게이트 (§17 — 결과 전 확정) ───────────────────────
GATES = [
    {"id": "G1_BASE_EXCESS",
     "rule": "base excess >= +2.00%p",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G2_WALK_FORWARD",
     "rule": "retrospective walk-forward out-of-sample 창의 과반(>50%)에서 excess > 0",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G3_LEAVE_ONE_ERA_OUT",
     "rule": "시대 1개 제거 시 excess > 0 인 경우가 6개 중 5개 이상",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G4_COHORT_STABILITY",
     "rule": "5년 horizon cohort positive-excess 비율 >= 60% AND 5년 excess 10th pct > -5.00%p",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G5_COST_HIGH",
     "rule": "HIGH 비용·슬리피지에서 excess > 0",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G6_DELIST_100",
     "rule": "상장폐지 100% 손실(SYMMETRIC)에서 excess > 0",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G7_MATCHED_CONTROLS",
     "rule": "candidate excess 가 RANDOM/SIZE/MARKET_MATCHED 세 분포 모두에서 percentile >= 90",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G8_CONCENTRATION",
     "rule": "기여 상위 5종목 제거 후에도 excess > 0",
     "mandatoryFor": ["ROBUST_CANDIDATE"]},
    {"id": "G9_DATA_QUALITY",
     "rule": "치명적 데이터 결함 없음 — 확인된 look-ahead/PBR 미래정보 유입 0, 결측 제외 중대 선택편향 없음",
     "mandatoryFor": ["ROBUST_CANDIDATE", "PROMISING_FORWARD_TEST"]},
    {"id": "G10_HUMAN_EXECUTABLE",
     "rule": "연 거래 <= 60건 AND 평균 보유 <= 50종목",
     "mandatoryFor": ["ROBUST_CANDIDATE", "PROMISING_FORWARD_TEST"]},
]

VERDICT_RULES = {
    "ROBUST_CANDIDATE": "G1~G10 전부 PASS",
    "PROMISING_FORWARD_TEST": (
        "base excess > 0 AND G9 PASS AND G10 PASS AND (G5 또는 G6 중 1개 이상 PASS) "
        "AND G1~G10 중 PASS 개수 >= 6"),
    "FRAGILE": "base excess > 0 이지만 위 두 조건 모두 미달",
    "REJECT": ("base excess <= 0 OR G9 FAIL(치명적 데이터 결함 확인) OR "
               "leave-one-era-out 과반에서 excess <= 0"),
    "precedence": ["REJECT", "ROBUST_CANDIDATE", "PROMISING_FORWARD_TEST", "FRAGILE"],
}

NO_RESCUE = {
    "rule": "결과가 나빠도 frozen candidate 의 어떤 파라미터도 바꾸지 않는다(§18).",
    "forbidden": ["percentile != 0.20", "holdings != 40", "hold != 24",
                  "deployment != STAG12M", "replacement != FIXED_MATURITY_REPLACE",
                  "buy_every != 24", "다른 entry schedule", "새 factor 조합"],
    "note": "새 후보 탐색이 필요하면 R9 결과에 '후속 연구 질문'으로만 적는다.",
}


def build():
    return {
        "schema": "wababa-frozen-candidate-independent-validation-r9/precommit@1",
        "taskId": "WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9",
        "writtenBeforeResults": True,
        "frozenCandidate": FROZEN,
        "reproTolerance": REPRO_TOLERANCE,
        "eras": ERAS,
        "costLevels": COST_LEVELS,
        "delistLevels": DELIST_LEVELS,
        "delistGateMode": DELIST_GATE_MODE,
        "delistModeNote": DELIST_MODE_NOTE,
        "liquidityStress": LIQUIDITY_STRESS,
        "controlSeeds": {"n": len(CONTROL_SEEDS), "first": CONTROL_SEEDS[0],
                         "last": CONTROL_SEEDS[-1]},
        "gates": GATES,
        "verdictRules": VERDICT_RULES,
        "noParameterRescue": NO_RESCUE,
        "objective": ("후보가 얼마나 좋은지 재기 위한 것이 아니다. 고치지 않고 공격했을 때도 "
                      "forward-test 할 이유가 남는지 정직하게 판정한다."),
    }


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    out = RD / "r9-precommit-latest.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out), "gates": len(GATES), "eras": len(ERAS),
                      "costLevels": len(COST_LEVELS), "delistLevels": len(DELIST_LEVELS)},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
