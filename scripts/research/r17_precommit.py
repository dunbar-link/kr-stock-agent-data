#!/usr/bin/env python3
"""R17 사전규격 — DART 유상증자 직접소스 복원 정책을 **결과 이전에** 고정한다.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

R16 인수: CANONICAL_TSR_FOUNDATION_BLOCKED
  SUSPECTED_RIGHTS 2,742건 / 1,652 종목 / universe 40.8% (사전기준 40% 초과)

R17 단일 목적(§1): DART '유상증자결정' 공시를 직접소스로 수집해 2,742건을
실제 유상증자와 대조하고, 기존 주주 wealth 를 계산 가능한 수준까지 gap 을 줄인다.
factor 연구는 하지 않는다(§20).

안전: 계산·조회 전용 · 무료 공개 DART 만 · env/token 변경 0 · production write 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ 수집 범위 (§7·§19) ══════════════
SCOPE = {
    "target": "R16 SUSPECTED_RIGHTS_ISSUE 2,742건이 발생한 종목",
    "endpoints": {
        "piicDecsn": "유상증자 결정 — 이번 작업의 직접소스",
        "fricDecsn": "무상증자 결정 — suspected 를 무상으로 확정하는 직접 판별자",
        "pifricDecsn": "유무상증자 결정 — 혼합 공시 누락 방지",
    },
    "whyThreeEndpoints": (
        "질문이 '이 주식수 증가가 유상이냐 무상이냐'이므로 반대편 증거(무상증자 "
        "결정)가 없으면 CONFIRMED_NON_RIGHTS 를 만들 수 없다. 세 endpoint 모두 "
        "**suspected 종목에만** 조회한다 — §19 가 금지한 '해당 event type 의 전체 "
        "historical collector 신설'이 아니다."),
    "notCollected": ["감자", "합병", "분할", "자기주식", "전환사채",
                     "suspected 아닌 종목의 어떤 공시도"],
    "accessPath": "기존 DART_API_KEY 환경변수 재사용. 신규 키·토큰 생성 0.",
    "pacing": "기존 build_market_snapshot.py 규약 재사용(DART_REQUEST_SLEEP_SEC, 백오프).",
    "resume": "종목 단위 checkpoint. 재실행 시 캐시 적중분은 재호출하지 않는다.",
}

# ══════════════ 유상증자 유형 분리 (§4) ══════════════
# 핵심: 기존 주주에게 **신주인수권이 실제로 배정되는가**로 갈린다.
ISSUE_METHODS = {
    "SHAREHOLDER_ALLOCATION": {
        "label": "A. 주주배정",
        "existingHolderRight": True,
        "action": "RIGHTS_ENTITLEMENT — 배정비율만큼 신주 + EXTERNAL_CONTRIBUTION 기록",
    },
    "SHAREHOLDER_THEN_PUBLIC": {
        "label": "B. 주주배정 후 실권주 일반공모",
        "existingHolderRight": True,
        "action": ("배정분은 A 와 동일. 실권주 공모분은 기존 주주 권리가 아니므로 "
                   "미조정(희석은 주가에 반영)."),
    },
    "PUBLIC_OFFERING": {
        "label": "C. 일반공모",
        "existingHolderRight": False,
        "action": "NO_ADJUSTMENT — 기존 주주는 신주를 받지 않는다.",
    },
    "THIRD_PARTY": {
        "label": "D. 제3자배정",
        "existingHolderRight": False,
        "action": "NO_ADJUSTMENT — 기존 주주는 신주를 받지 않는다.",
    },
    "MIXED": {
        "label": "E. 혼합형",
        "existingHolderRight": True,
        "action": "주주배정 비율만 A 처리. 나머지는 미조정.",
    },
}

# ★ R16 대비 무엇이 달라지는가 — 결과 이전에 명시한다.
IMPACT_DIRECTION = {
    "C_D_types": (
        "일반공모·제3자배정으로 확정되면 **R16 의 현재 동작(미조정)이 이미 정답**이다. "
        "엔진 변경 없이 UNRESOLVED → CORRECTLY_HANDLED 로 바뀐다."),
    "A_B_E_types": (
        "주주배정으로 확정되면 R16 은 기존 주주의 신주인수권 가치를 **누락**하고 "
        "있었다. 미조정은 가짜 wealth 를 만들지 않지만 수익을 **과소평가**한다."),
    "whyThisMatters": (
        "따라서 R16 의 미조정은 어느 유형에서도 wealth 를 과대평가하지 않는다 = "
        "보수적이다. R17 은 그 보수성의 **크기**를 정량화한다."),
}

# ══════════════ 주주 wealth 정책 (§10·§12) ══════════════
WEALTH_POLICY = {
    "chosen": "POLICY_A_ASSUME_FULL_EXERCISE",
    "definition": (
        "주주배정형에서 보유자가 배정분을 **전량 청약**했다고 가정한다. "
        "추가 납입금은 EXTERNAL_CONTRIBUTION 으로 기록하고 투자수익에 넣지 않는다."),
    "whyNotPolicyB": (
        "권리 시장가격(신주인수권증서) 데이터가 저장소에 없다. TERP 로 이론값을 "
        "만들 수는 있으나 그것은 DERIVED_INFERENCE 이고, R16 이 이미 "
        "EXTERNAL_CONTRIBUTION 필드를 만들어 두고 '권리 데이터가 확보되면 채운다'고 "
        "명시했다. POLICY A 가 기존 설계와 일관된다."),
    "equivalenceNote": (
        "공정가격(TERP) 기준에서 POLICY A(청약)와 POLICY B(권리매도)의 "
        "**시간가중수익률은 동일**하다. 차이는 금액가중 관점에서만 생긴다. "
        "canonical 은 TIME_WEIGHTED 이므로 정책 선택이 factor 비교를 왜곡하지 않는다."),
    "returnBasis": "TIME_WEIGHTED — 외부 납입을 중립화한다(R16 CASHFLOW 승계).",
    "ledgerFields": ["wealth_before", "rights_entitlement", "external_contribution",
                     "shares_received", "wealth_after"],
    "hardRule": "추가 납입금을 gain 으로 계산하면 FAIL. 공짜 신주를 만들면 FAIL.",
}

# ══════════════ 정정공시 (§5) ══════════════
CORRECTIONS = {
    "chain": "original receipt_no -> correction_of_receipt_no -> ... -> final",
    "useForAccounting": "FINAL_EFFECTIVE_TERMS (실제 발생한 사건)",
    "useForForwardDecision": "당시 알려진 정보만 (이번 작업에는 forward decision 없음)",
    "preserve": "최초 공시와 정정 history 를 모두 보존한다(PIT provenance).",
    "knownRepoCaveat": (
        "docs/WABABA-PIT-DATA-FEASIBILITY-R1.md: DART 는 조회 시 마지막 정정본을 "
        "준다. rcept_no 앞 8자리(접수일)로 시점을 판별해야 한다."),
}

# ══════════════ matching 규칙 (§9) ══════════════
MATCHING = {
    "keys": ["stock_code", "event date proximity", "shares outstanding jump ratio"],
    "hardRule": "날짜 근접만으로 DIRECT match 선언 금지.",
    "confidence": {
        "HIGH": ("stock_code 일치 AND 상장일(또는 납입일)이 관측 사건월 ±1개월 이내 "
                 "AND 신주비율이 관측 shareRatio-1 과 상대오차 0.15 이내"),
        "MEDIUM": ("stock_code 일치 AND 날짜 ±2개월 이내 AND 비율 검증 불가 또는 "
                   "상대오차 0.15~0.50"),
        "LOW": "stock_code 일치 AND 날짜 ±3개월. 비율 불일치.",
        "NONE": "직접 공시 없음",
    },
    "resolvedDefinition": (
        "HIGH 또는 MEDIUM 만 resolved 로 센다. **LOW 는 UNRESOLVED 로 센다** — "
        "§9 '날짜 근접만으로 direct match 선언 금지'를 계수에도 적용한다."),
    "ratioTolerance": 0.15,
    "mediumRatioMax": 0.50,
    "monthsHigh": 1,
    "monthsMedium": 2,
    "monthsLow": 3,
}

# ══════════════ 분류 라벨 (§8) ══════════════
LABELS = ["CONFIRMED_RIGHTS", "CONFIRMED_NON_RIGHTS", "CONFIRMED_OTHER_CAPITAL_ACTION",
          "CONFIRMED_THIRD_PARTY_ISSUE", "CONFIRMED_PUBLIC_OFFERING",
          "CONFIRMED_BONUS_ISSUE", "UNRESOLVED"]
PROVENANCE = ["DIRECT_DART", "DIRECT_KRX", "DERIVED_MATCH", "NO_DIRECT_MATCH"]

# ══════════════ foundation 재판정 기준 (§17·§18) — 결과 이전 고정 ══════════════
VERDICT_RULE = {
    "r16ThresholdPreserved": {
        "rule": "unresolved universe ratio >= 40% 이면 BLOCKED_RIGHTS_REMAINS",
        "note": "R16 사전기준을 그대로 승계한다. 사후 완화 금지(§17).",
        "thresholdPct": 40.0,
    },
    "notMechanical": (
        "40% 미만이어도 자동 PASS 아니다(§17 명시). 아래 경제적 materiality 를 "
        "모두 충족해야 한다."),
    "CANONICAL_TSR_FOUNDATION_PASS": {
        "unresolvedTickerPctMax": 5.0,
        "noFakeWealth": True,
        "extremeDiscontinuityPctMax": 1.0,
    },
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS": {
        "unresolvedTickerPctMax": 20.0,
        "noFakeWealth": True,
        "biasDirectionMustBe": "CONSERVATIVE_UNDERSTATEMENT",
        "medianUnderstatementPctMax": 2.0,
        "why": ("미해결이 남아도 (a) 가짜 wealth 를 만들지 않고 (b) 편향이 과소평가 "
                "방향이며 (c) 그 크기가 작으면, factor 비교 기반으로 사용 가능하다."),
    },
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS": "위 조건 미충족",
    "CANONICAL_TSR_FOUNDATION_FAIL": (
        "엔진이 가짜 wealth 를 만들거나 anchor 재현에 실패하면 FAIL"),
    "materialityMetrics": ["unresolvedTickerPct", "unresolvedEventPct",
                           "unresolvedMedianShareRatio", "extremeDiscontinuityPct",
                           "estimatedUnderstatementPct"],
}

# ══════════════ 금지 (§20·§26) ══════════════
FORBIDDEN = [
    "BM/SIZE/QUALITY/ROE/Magic Formula 재계산", "portfolio search", "CAGR ranking",
    "factor rediscovery", "R16 산출물 덮어쓰기", "R5~R14 결론의 정본 승격",
    "유료 데이터 구매", "신규 유료 API 가입", "새 crawler framework",
    "env/token 변경", "production write", "실주문", "외부발송", "deploy",
    "결과를 본 뒤 threshold 완화",
]

PAYLOAD = {
    "task": "R17", "taskId": "WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17",
    "writtenBeforeDartResults": True,
    "inherits": {"from": "R16", "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED",
                 "suspectedRights": 2742, "affectedTickers": 1652,
                 "affectedUniversePct": 40.8, "precommitThresholdPct": 40.0},
    "question": "유상증자 때문에 확정하지 못한 기존 주주 wealth 를 직접증거로 복원할 수 있는가?",
    "scope": SCOPE, "issueMethods": ISSUE_METHODS, "impactDirection": IMPACT_DIRECTION,
    "wealthPolicy": WEALTH_POLICY, "corrections": CORRECTIONS, "matching": MATCHING,
    "labels": LABELS, "provenance": PROVENANCE, "verdictRule": VERDICT_RULE,
    "forbidden": FORBIDDEN,
}


def main() -> int:
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r17-precommit-latest.json"
    p.write_text(json.dumps(PAYLOAD, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(p), "policy": WEALTH_POLICY["chosen"],
                      "resolvedDef": "HIGH+MEDIUM only",
                      "passWithLimitsMaxUnresolvedTickerPct":
                      VERDICT_RULE["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
                      ["unresolvedTickerPctMax"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
