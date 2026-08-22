#!/usr/bin/env python3
"""R23 대상 확정 + 복원 정책 — **결과 이전에** 고정한다.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

R22 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 3.04% (20% 게이트 통과)
  **기대편향 2.416% > 2.00%** 이 유일한 미통과 조건
  R22 감도: 이 11건 해결 시 1.520% (통과)

R23 단일 목적(§1): 이 11건은 주주배정이라는 사실도, 발행가도 대부분 확보됐다.
없는 것은 **기존 주주가 몇 주를 배정받을 권리가 있었는가**(구주주 배정량)뿐이다.
증권신고서·정정신고서의 배정표를 직접 파싱해 복원한다. factor 연구 금지(§23).

★ 결과조작 금지(§17·§36): 감도상 1.520% 가 예상되지만 그 값에 맞추려고 LOW 를
  CONFIRMED 로 올리거나 계획치를 actual 로 쓰지 않는다. 성공은 게이트 통과가
  아니라 11건의 사실을 정확히 복원하는 것이다. 복원 안 되면 UNKNOWN 유지.

threshold 와 bias 계산식은 R16~R22 것을 그대로 승계한다. 변경 금지(§18·§19).

안전: 무료 공개 DART 만 · env/token 변경 0 · production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r17_precommit import VERDICT_RULE, WEALTH_POLICY  # noqa: E402  정본 승계

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ 최우선 소스 (§3) ══════════════
SOURCE_PRIORITY = [
    {"rank": 1, "kind": "SECURITIES_REGISTRATION", "source": "증권신고서",
     "why": "구주주 배정 표·1주당 신주배정주식수·배정비율이 본문 표에 있다."},
    {"rank": 2, "kind": "SECURITIES_REGISTRATION_AMENDED", "source": "정정증권신고서",
     "why": "최종 확정 조건으로 정정된 배정표."},
    {"rank": 3, "kind": "PROSPECTUS", "source": "투자설명서·정정투자설명서",
     "why": "신고서와 같은 배정표를 담는다."},
    {"rank": 4, "kind": "SUBSCRIPTION_RESULT", "source": "청약결과 공시",
     "why": "구주주 실제 청약/배정량."},
    {"rank": 5, "kind": "ISSUE_COMPLETION", "source": "증권발행결과",
     "why": "실제 발행주식수·발행방법."},
    {"rank": 6, "kind": "NEW_LISTING", "source": "추가상장",
     "why": "실제 상장 신주수."},
]
SOURCE_RULE = ("R22 에서 못 찾은 것을 같은 방식으로 반복하지 않는다(§3). "
               "이번 1순위는 **증권신고서 원문의 배정표**다.")

# ══════════════ 배정량 복원 우선순위 (§5) ══════════════
ALLOCATION_EVIDENCE = [
    {"grade": "A", "rule": "사후 직접공시에 실제 구주주 배정주식수 명시"},
    {"grade": "B", "rule": "청약결과에 구주주 실제 청약/배정량 명시"},
    {"grade": "C", "rule": "증권신고서 확정 배정량 + 이후 발행완료 수량 일치"},
    {"grade": "D", "rule": "배정비율 × 실제 eligible old shares 로 재현"},
    {"grade": "E", "rule": "여러 직접소스 수량의 일관된 reconciliation"},
]
ALLOCATION_HARD_RULE = ("계획 신주수 하나만으로 WEALTH_CONFIRMED 금지(§5). "
                        "예정발행가 사용 금지(§9).")

# ══════════════ eligible old shares (§6·§7·§8) ══════════════
ELIGIBLE = {
    "why": ("배정비율이 있어도 분모(eligible old shares)를 잘못 쓰면 effective "
            "rights ratio 가 왜곡된다."),
    "checks": ["기준일 발행주식수", "자기주식 제외 여부", "의결권 없는 주식",
               "우선주/보통주 구분", "구주주 배정 제외주식"],
    "treasuryRule": ("주주배정에서 회사 보유 자기주식은 배정 대상에서 제외될 수 "
                     "있다. total issued shares != eligible shareholder shares "
                     "가 가능하다(§7)."),
    "preferredRule": ("보통주 연구다. 공시가 보통주 신주와 우선주 신주를 별도 "
                      "표기하면 분리하고 보통주만 쓴다(§8)."),
    "preferDirect": "공시가 eligible denominator 를 직접 주면 그것을 쓴다.",
}

# ══════════════ 네 수량 분리 (§11) ══════════════
FOUR_QUANTITIES = {
    "PLANNED_NEW_SHARES": "계획 신주수",
    "ENTITLED_TO_EXISTING_SHAREHOLDERS": "기존 주주 배정량 ← wealth 계산의 권리",
    "ACTUALLY_SUBSCRIBED_BY_EXISTING_SHAREHOLDERS": "기존 주주 실제 청약",
    "ACTUALLY_ISSUED_TOTAL": "최종 총 발행주식수",
    "hardRule": "네 값을 같은 것으로 취급하지 않는다.",
    "whichForWealth": ("ENTITLED — canonical 은 1주 보유자 관점이고 그 주주의 "
                       "권리는 배정량이다(R20~R22 정본)."),
}

# ══════════════ 숫자 정합성 (§13) ══════════════
RECONCILIATION = {
    "test1": "eligible old shares × rights ratio ≈ shareholder allocated shares",
    "test2": ("shareholder allocation + public/unsubscribed + third-party "
              "≈ final issued shares"),
    "tolerance": 0.15,
    "toleranceWhy": "R17~R22 와 동일. 새 값을 발명하지 않는다.",
    "onConflict": "DATA_CONFLICT flag. 억지로 하나를 고르지 않는다.",
}

# ══════════════ wealth·실권 정책 — R17~R22 정본 (§10·§15) ══════════════
WEALTH = {
    "inheritedFrom": "R17 (R18~R22 에서 변경 없이 사용)",
    "policy": WEALTH_POLICY["chosen"],
    "returnBasis": WEALTH_POLICY["returnBasis"],
    "lapseRule": ("발행가 K >= 권리락가 P_ex 이면 합리적 실권. R17 정본 그대로. "
                  "새 청약 policy 를 만들지 않는다(§10)."),
    "externalContribution": "추가 납입금은 EXTERNAL_CONTRIBUTION. 수익 아님.",
    "hardRule": "공짜 wealth 생성 금지.",
}

WEALTH_STATUS = {
    "WEALTH_CONFIRMED": ("배정량 확보 AND 확정발행가 확보 AND eligible/ratio "
                         "정합 AND same-month 분리 AND 외부납입 계산 가능"),
    "WEALTH_PARTIAL": "핵심값 일부 결측",
    "WEALTH_UNRESOLVED": "wealth 계산 불가",
    "noPromotionFromLow": "LOW confidence 는 WEALTH_CONFIRMED 금지(§14).",
}
CONFIDENCE = {
    "HIGH": "직접 배정표 + 확정발행가 + 발행수 reconciliation",
    "MEDIUM": "직접 비율 + eligible shares + 발행 cross-check",
    "LOW": "계획치·간접추론",
    "ratioTolerance": 0.15,
}

# ══════════════ bias (§18) ══════════════
BIAS = {
    "methodologyUnchanged": "R21/R22 그대로. expected = P × 조건부 과소평가 크기",
    "notAllowed": "새 formula · 조건부 크기 재정의 · threshold 완화",
    "r22ExpectedBiasPct": 2.416,
    "r22SensitivityProjection": 1.520,
    "projectionCaveat": ("감도값은 11건이 **전부** 해결될 때다. 일부만 해결되면 "
                         "그 값에 도달하지 않는다. 투영이 아니라 실측값을 쓴다."),
    "cumulativeSensitivityRequired": ("§21 — 일부만 해결돼도 게이트를 넘을 수 "
                                      "있으므로 누적 감도를 보고한다."),
}

# ══════════════ 판정 (§19·§20) — 승계, 변경 금지 ══════════════
VERDICT = {
    "inheritedFrom": "R16~R22 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "minimumForPass": "unresolved universe ratio < 20% AND expected bias <= 2%",
    "r22Status": {"unresolvedTickerPct": 3.04, "expectedBiasPct": 2.416,
                  "firstConditionMet": True, "secondConditionMet": False},
    "closeWhenMet": ("게이트를 통과하면 완벽한 corporate-action history 를 위해 "
                     "무한정 foundation 연구를 계속하지 않는다(§20)."),
}

FORBIDDEN = [
    "BM/EY/ROE/SIZE/QUALITY/Magic Formula 재계산", "portfolio", "보유기간 연구",
    "target 추가·삭제", "50건까지 범위 확대", "새 범용 crawler/framework",
    "기존 R16~R22 산출물 덮어쓰기", "R5~R14 자동 승격",
    "threshold 변경", "bias formula 변경", "실권 policy 신설",
    "계획 신주수를 배정량으로 사용", "예정발행가 사용",
    "게이트 통과를 위해 LOW 를 CONFIRMED 로 승격", "추정으로 정본 생성",
    "유료 API·데이터", "신규 key/token", "env 변경",
    "production write", "실주문", "외부발송", "deploy",
]


def build_targets():
    res = json.loads((RD / "r22-final-terms-normalized-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    rec = json.loads((RD / "r22-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    by = {(r["ticker"], r["date"]): r for r in rec}
    out = []
    for e in res:
        if e["wealthStatus"] == "WEALTH_CONFIRMED":
            continue
        m = by.get((e["ticker"], e["date"])) or {}
        out.append({
            "ticker": e["ticker"], "corpCode": e.get("corpCode"),
            "corpName": e.get("corpName"), "date": e["date"],
            "originalRceptNo": e.get("originalRceptNo"),
            "originalFilingDate": e.get("originalFilingDate"),
            "confirmedRightsSource": e.get("rightsConfirmationSource"),
            "plannedIssueMethod": e.get("plannedIssueMethod"),
            "plannedRightsRatio": e.get("plannedRightsRatio"),
            "plannedIssuePrice": e.get("plannedIssuePrice"),
            "knownFinalIssuePrice": e.get("finalIssuePrice"),
            "knownFinalIssuePriceSource": e.get("priceSource"),
            "knownFinalShares": e.get("finalActualNewShares"),
            "knownFinalSharesSource": e.get("sharesSource"),
            "observedSharesBefore": e.get("observedSharesBefore"),
            "observedSharesAfter": e.get("observedSharesAfter"),
            "observedShareDelta": e.get("observedShareDelta"),
            "observedShareRatio": e.get("observedShareRatio"),
            "priceRatio": e.get("priceRatio"),
            "currentMissingField": "shareholderEntitledShares",
            "currentWealthStatus": e["wealthStatus"],
            "currentConfidence": e["confidence"],
            "currentCompletionStatus": e["completionStatus"],
            "currentBiasP": 1.0,
            "r22Sources": list((e.get("sources") or {}).keys()),
            "r22Flags": e.get("flags", []),
            "r20EventStatus": m.get("r20EventStatus"),
            "r21WealthStatus": m.get("r21WealthStatus"),
            "otherEventsSameMonth": e.get("otherEventsSameMonth", []),
        })
    return out


def main() -> int:
    tg = build_targets()
    payload = {
        "task": "R23",
        "taskId": "WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23",
        "writtenBeforeRegistrationResults": True,
        "inherits": {
            "from": "R22",
            "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 3.04, "expectedBiasPct": 2.416,
            "biasThresholdPct": 2.0, "unresolvedThresholdPct": 20.0,
            "sensitivityIfFullyResolved": 1.520,
            "onlyFailingCondition": "expectedBias",
        },
        "purpose": ("주주배정도 발행가도 대부분 확보됐다. 없는 것은 구주주 "
                    "배정량뿐이다. 증권신고서 배정표로 복원한다."),
        "noResultEngineering": (
            "감도상 1.520% 가 예상되지만 그 값에 맞추려고 LOW 를 CONFIRMED 로 "
            "올리거나 계획치를 actual 로 쓰지 않는다(§17·§36)."),
        "targetDefinition": "R22 wealthStatus != WEALTH_CONFIRMED",
        "targetTotal": len(tg),
        "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": sum(1 for t in tg if t["corpCode"]),
        "scopeGuard": "11건만. target 추가·삭제 금지. 50건 확대 금지(§2·§22).",
        "sourcePriority": SOURCE_PRIORITY, "sourceRule": SOURCE_RULE,
        "allocationEvidence": ALLOCATION_EVIDENCE,
        "allocationHardRule": ALLOCATION_HARD_RULE,
        "eligible": ELIGIBLE, "fourQuantities": FOUR_QUANTITIES,
        "reconciliation": RECONCILIATION,
        "wealth": WEALTH, "wealthStatus": WEALTH_STATUS,
        "confidence": CONFIDENCE, "bias": BIAS, "verdict": VERDICT,
        "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r23-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                 encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "tickers": len({t["ticker"] for t in tg}),
                      "withKnownPrice": sum(1 for t in tg
                                            if t["knownFinalIssuePrice"]),
                      "withKnownShares": sum(1 for t in tg
                                             if t["knownFinalShares"]),
                      "biasThresholdPct": 2.0, "thresholdChanged": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
