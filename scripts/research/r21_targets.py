#!/usr/bin/env python3
"""R21 대상 확정 + 판정 정책 — **결과 이전에** 고정한다.

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21

R20 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 5.43% (20% 게이트 통과)
  **기대편향 3.442% > 2.00%** 이 유일한 미통과 조건
  최대 기여군 = LOW_CONFIDENCE_NOT_COUNTED 96건 (1.453%p)

R21 단일 목적(§1): 그 96건의 P 는 지금 **모집단 추정치 0.296** 이다. DART 직접증거로
기존 보통주 주주의 직접권리 유무를 전수 판정해 **추정을 실측으로 대체**한다.

★ 예단 금지(§18·§36): 실측 결과 권리 YES 비율이 29.6% 보다 높으면 편향은
  **올라간다**. 그래도 그대로 보고한다. 성공은 편향을 낮추는 것이 아니라
  추정을 측정으로 바꾸는 것이다.

threshold 와 bias 계산식은 R16~R20 것을 그대로 승계한다. 변경 금지(§17·§19).

안전: 무료 공개 DART 만 · env/token 변경 0 · production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r17_precommit import VERDICT_RULE  # noqa: E402  정본 승계
from r19_targets import EVENT_TYPES, MERGER_DIRECTION  # noqa: E402  분류 정본 재사용

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ 핵심 경제 질문 (§5) ══════════════
CORE_QUESTION = (
    "기존 보통주 주주가 이 사건으로 직접 신주인수권·현금·추가주식·교환대가 등 "
    "경제적 consideration 을 받았는가?")
CORE_DISTINCTION = (
    "주식수 증가와 기존 주주 권리는 같은 개념이 아니다. CB 전환·BW 행사·"
    "주식매수선택권 행사·존속회사의 합병신주 발행은 주식수를 늘리지만 기존 "
    "일반주주에게 직접 entitlement 를 주지 않는다. 이 경우 미조정이 정답이다.")

# ══════════════ 유형·권리 규칙 — R19 정본 재사용 (§4·§9~§13) ══════════════
TYPE_RULES = {
    "inheritedFrom": "R19 (r19_targets.EVENT_TYPES · r19_classify)",
    "why": ("§26 이 새 crawler·새 framework 를 금지한다. R19 가 342건에서 검증한 "
            "분류기와 권리 규칙을 그대로 쓴다. 새 규칙을 만들지 않는다."),
    "noEntitlement": ["CONVERTIBLE_BOND_CONVERSION", "BW_WARRANT_EXERCISE",
                      "EXCHANGEABLE_BOND_EXCHANGE", "STOCK_OPTION_EXERCISE",
                      "THIRD_PARTY_ISSUANCE", "PUBLIC_OFFERING",
                      "TREASURY_ACTION", "CODE_CHANGE"],
    "entitlement": ["SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC",
                    "BONUS_ISSUE", "STOCK_SPLIT", "TRUE_RIGHTS_ISSUE"],
    "directional": ["MERGER_NEW_SHARES", "SHARE_SWAP",
                    "SPINOFF_RELATED_SHARES", "CAPITAL_REDUCTION"],
    "mergerDirection": MERGER_DIRECTION,
    "cannotExplainShareIncrease": ["TREASURY_ACTION", "CAPITAL_REDUCTION",
                                   "CODE_CHANGE"],
}

# ══════════════ 매칭·confidence (§7·§8) ══════════════
MATCHING = {
    "keys": ["stock_code", "corp_code", "filing date", "effective/listing date",
             "new shares", "share-count change", "event ratio"],
    "hardRule": "제목만 비슷하다고 HIGH confidence 금지. 숫자까지 맞아야 한다.",
    "confidence": {
        "HIGH": "직접공시 + 유형 + 수량/날짜 일치",
        "MEDIUM": "직접공시 + 유형 일치, 일부 numeric gap",
        "LOW": "간접 추론 중심",
        "UNRESOLVED": "결론 불가",
    },
    "goal": ("LOW 를 억지로 HIGH 로 올리는 것이 목적이 아니다(§8). 모르는 건 "
             "그대로 UNRESOLVED 로 남긴다."),
    "ratioTolerance": 0.15,
}

# ══════════════ 같은 달 복수 사건 (§14) ══════════════
SAME_MONTH = {
    "rule": "주식수 변화를 단일 사건 하나에 전부 귀속하지 않는다.",
    "method": "R19/R20 의 same-month separation 로직 재사용",
}

# ══════════════ 추정확률 제거 (§16) ══════════════
ESTIMATE_REPLACEMENT = {
    "current": "LOW_CONFIDENCE_NOT_COUNTED 96건에 모집단 추정 P=0.296 적용 중",
    "target": "직접 판정된 사건은 추정분포에서 빼고 실제 YES/NO 를 반영",
    "hardRule": ("새로운 임의 모집단확률을 사후로 만들어 넣지 않는다. 남은 "
                 "unresolved 에만 기존 R20 methodology 를 그대로 적용한다."),
}

# ══════════════ bias (§17·§18) ══════════════
BIAS = {
    "formulaUnchanged": "R20 계산식 그대로. expected = P × 조건부 과소평가 크기",
    "notAllowed": "계산식 변경 · 조건부 크기 재정의 · threshold 완화",
    "r20ExpectedBiasPct": 3.442,
    "groupEstimatedContributionPp": 1.453,
    "mayIncrease": ("직접 조사 결과 권리 YES 비율이 29.6% 보다 높으면 편향이 "
                    "**상승**한다. 그 경우도 그대로 보고한다(§18)."),
    "arithmeticNote": ("지표는 미해결 1건당 기대값이다. 확정 사건은 분자와 분모에서 "
                       "모두 빠지므로 단순 뺄셈으로 예측되지 않는다(R20 에서 확인)."),
}

# ══════════════ 판정 (§19·§20) — 승계, 변경 금지 ══════════════
VERDICT = {
    "inheritedFrom": "R16/R17/R18/R19/R20 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "minimumForPass": "unresolved universe ratio < 20% AND expected bias <= 2%",
    "r20Status": {"unresolvedTickerPct": 5.43, "expectedBiasPct": 3.442,
                  "firstConditionMet": True, "secondConditionMet": False},
    "stopWhenGateMet": ("사전 gate 를 충족하면 완벽주의로 foundation 연구를 "
                        "무한 연장하지 않는다(§20)."),
}

FORBIDDEN = [
    "BM/SIZE/QUALITY/ROE/EY/Magic Formula 성과 재계산", "portfolio",
    "보유기간 연구", "factor 재발견",
    "새 universe 확장", "새 crawler framework", "새 분류 규칙 신설",
    "기존 R16~R20 산출물 덮어쓰기", "R5~R14 자동 승격",
    "threshold 변경", "bias 계산식 변경", "사후 임의 모집단확률 신설",
    "LOW 를 근거 없이 HIGH 로 승격", "편향이 내려간다는 예단",
    "유료 API·데이터", "신규 key/token", "env 변경",
    "production write", "실주문", "외부발송", "deploy",
]


def build_targets():
    rows = json.loads((RD / "r20-full-reconciliation-latest.json")
                      .read_text(encoding="utf-8"))["rows"]
    cc = json.loads((ROOT / "_cache" / "dart-corp-codes.json")
                    .read_text(encoding="utf-8"))
    import r16_canonical as C
    from r16_audit import contiguous_span
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    pos = {d: i for i, d in enumerate(ds)}

    out = []
    for r in rows:
        if r.get("wealthReason") != "LOW_CONFIDENCE_NOT_COUNTED":
            continue
        t = r["ticker"]
        i = pos.get(r["date"])
        sb = sa = None
        if i is not None and i > 0:
            sb = (cap.get(ds[i - 1], {}).get(t) or {}).get("shares")
            sa = (cap.get(ds[i], {}).get(t) or {}).get("shares")
        out.append({
            "ticker": t,
            "corpCode": (cc.get(t) or {}).get("corp_code"),
            "corpName": (cc.get(t) or {}).get("corp_name"),
            "date": r["date"],
            "shareRatio": r["shareRatio"],
            "observedSharesBefore": sb, "observedSharesAfter": sa,
            "shareCountDelta": (sa - sb) if (sb and sa) else None,
            "priceRatio": r.get("priceRatio"),
            "mcapRatio": r.get("mcapRatio"),
            "currentEventClassification": r.get("label"),
            "currentConfidence": r.get("confidence"),
            "currentEntitlementState": "UNKNOWN",
            "currentBiasP": 0.296,
            "currentBiasSource": "POPULATION_ESTIMATE",
            "relatedReceipts": [x for x in [r.get("rcept_no")] if x],
            "r17EvidenceLevel": r.get("evidenceLevel"),
            "r17IssueMethodRaw": r.get("issueMethodRaw"),
            "r17FilingGapMonths": r.get("filingGapMonths"),
            "r18Label": r.get("r18Label"),
            "r19PrimaryEvent": r.get("r19PrimaryEvent"),
            "r20EventStatus": r.get("r20EventStatus"),
            "lowConfidenceReason": (
                "R17 매칭에서 공시는 찾았으나 비율 corroboration 이 없거나 "
                "날짜가 멀어 LOW 로 강등됐다. §9 '날짜 근접만으로 direct match "
                "선언 금지' 를 계수에도 적용한 결과다."),
        })
    return out


def main() -> int:
    tg = build_targets()
    payload = {
        "task": "R21",
        "taskId": "WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21",
        "writtenBeforeDirectSourceResults": True,
        "inherits": {
            "from": "R20",
            "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 5.43, "expectedBiasPct": 3.442,
            "biasThresholdPct": 2.0, "unresolvedThresholdPct": 20.0,
            "groupContributionPp": 1.453,
            "onlyFailingCondition": "expectedBias",
        },
        "purpose": ("이 96건의 P=0.296 은 추정치다. 직접증거로 전수 판정해 "
                    "추정을 실측으로 바꾼다."),
        "noPrejudgement": BIAS["mayIncrease"],
        "targetDefinition": "R20 wealthReason == LOW_CONFIDENCE_NOT_COUNTED",
        "targetTotal": len(tg),
        "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": sum(1 for t in tg if t["corpCode"]),
        "scopeGuard": "96건만. target 변경 금지(§2).",
        "coreQuestion": CORE_QUESTION, "coreDistinction": CORE_DISTINCTION,
        "typeRules": TYPE_RULES,
        "eventTypeCatalog": sorted(EVENT_TYPES),
        "matching": MATCHING, "sameMonth": SAME_MONTH,
        "estimateReplacement": ESTIMATE_REPLACEMENT,
        "bias": BIAS, "verdict": VERDICT, "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r21-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                 encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "tickers": len({t["ticker"] for t in tg}),
                      "corpCodeMapped": payload["corpCodeMapped"],
                      "biasThresholdPct": 2.0, "thresholdChanged": False,
                      "currentP": 0.296}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
