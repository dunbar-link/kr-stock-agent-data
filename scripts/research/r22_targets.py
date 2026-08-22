#!/usr/bin/env python3
"""R22 대상 확정 + 복원 정책 — **결과 이전에** 고정한다.

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22

R21 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 3.36% (20% 게이트 통과)
  **기대편향 3.470% > 2.00%** 이 유일한 미통과 조건
  R21 감도분석: 이 27건만이 게이트를 연다(해결 시 1.520%)

R22 단일 목적(§1): 주주배정이라는 사실은 이미 확정됐다. 실제로 몇 주를 얼마에
발행했는지를 **사후 직접공시**로 복원한다. factor 연구 금지(§23).

★ R20 과 무엇이 다른가(§3): R20 은 기존 창(-6~+9) 안에서 증권발행실적보고서만
  찾았고 25건이 '창 안에 구주주 배정이 있는 실적보고서 없음' 으로 남았다.
  R22 는 같은 시도를 반복하지 않는다 — **창을 넓히고(-3~+18) 공시 유형을 확장**해
  발행가확정·청약결과·실권처리·신주상장·자본금변동으로 우회 복원한다.

threshold 와 bias 계산식은 R16~R21 것을 그대로 승계한다. 변경 금지(§20·§21).

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

# ══════════════ 직접소스 우선순위 (§3) ══════════════
SOURCE_PRIORITY = [
    {"rank": 1, "kind": "ISSUE_RESULT_REPORT", "source": "증권발행실적보고서",
     "why": "실청약·실배정·확정발행가·납입총액이 사후 확정치로 들어 있다."},
    {"rank": 2, "kind": "RIGHTS_DECISION_CORRECTED", "source": "정정 유상증자결정",
     "why": "확정발행가·최종 신주수로 정정된 조건."},
    {"rank": 3, "kind": "FINAL_PRICE", "source": "발행가액확정 공시",
     "why": "예정가 → 확정가."},
    {"rank": 4, "kind": "SUBSCRIPTION_RESULT", "source": "청약결과 공시",
     "why": "구주주 실청약 규모."},
    {"rank": 5, "kind": "UNSUBSCRIBED_RESULT", "source": "실권주 처리결과",
     "why": "실권분의 일반공모·제3자 전환."},
    {"rank": 6, "kind": "NEW_LISTING", "source": "신주상장·추가상장 공시",
     "why": "실제 상장된 신주 수 = 실발행량의 직접 증거(§9)."},
    {"rank": 7, "kind": "ISSUE_COMPLETION", "source": "증자완료·납입완료 공시",
     "why": "발행 완료 사실과 수량."},
    {"rank": 8, "kind": "PERIODIC_REPORT", "source": "사업·분기보고서 자본금 변동",
     "why": ("보조 직접증거(§10). 단독으로 rights shares 를 확정하지 않는다 — "
             "같은 기간 CB·BW·합병·옵션이 섞일 수 있다.")},
]
SOURCE_RULE = ("planned terms 보다 post-event actual terms 를 우선한다(§6). "
               "사후 확정치가 없으면 계획치로 승격하지 않는다(§35).")

# ══════════════ 검색 창 (§4) ══════════════
WINDOW = {
    "months": [-3, 18],
    "why": ("원 계획공시 → 정정 → 가격확정 → 청약 → 납입 → 신주상장 → 발행완료 "
            "chain 을 끝까지 연결하려면 사후를 길게 봐야 한다. R20 의 +9 는 "
            "짧았다."),
    "bounded": "무한 기간 검색 금지. -3~+18 개월로 유계.",
    "r20Window": [-6, 9],
}

# ══════════════ PLANNED vs FINAL_ACTUAL (§6) ══════════════
LAYER_RULE = {
    "mustSeparate": ["planned_new_shares / final_actual_new_shares",
                     "planned_issue_price / final_issue_price",
                     "planned_rights_ratio / final_rights_ratio",
                     "planned_listing_date / final_listing_date"],
    "hardRule": "실제 shareholder wealth 에는 FINAL_ACTUAL 만 쓴다.",
}

# ══════════════ 발행가 (§7) ══════════════
ISSUE_PRICE = {
    "distinguish": ["1차 발행가", "2차 발행가", "확정발행가", "할인율", "기준주가"],
    "use": "최종 실제 청약·배정에 사용된 가격만",
    "ifOnlyPlanned": "WEALTH_PARTIAL 유지. 예정가로 확정 승격 금지.",
}

# ══════════════ 청약결과·실권 (§8) ══════════════
SUBSCRIPTION = {
    "fields": ["구주주 배정주식", "실제 구주주 청약주식", "실권주",
               "일반공모 전환", "제3자 배정 전환", "미발행", "최종 실제 발행주식"],
    "hardRule": "계획 배정량을 실제 realized entitlement 로 그대로 쓰지 않는다.",
}

# ══════════════ 같은 달 다른 자본행위 (§11) ══════════════
SAME_MONTH = {
    "rule": "관측 주식수 delta 를 rights issue 하나에 전부 귀속하지 않는다.",
    "method": ("observed total delta − 알려진 non-rights 신주 = rights 관련 delta. "
               "다만 **직접공시가 있으면 직접값 우선**."),
    "evidenceSource": "R19/R20 same-month separation 증거 재사용",
}

# ══════════════ 최종 배정비율 (§12) ══════════════
EFFECTIVE_RATIO = {
    "formula": "actual shareholder allocated shares / eligible old shares",
    "compareTo": "공시 명시 ratio",
    "onMismatch": "DATA_CONFLICT flag + confidence 하향",
}

# ══════════════ 취소·일부발행 (§13) ══════════════
COMPLETION = {
    "cancelled": "CANCELLED_NO_WEALTH_EVENT — 조정하지 않는다.",
    "partial": "실제 완료분만 wealth 에 반영. 계획 전체분으로 계산 금지.",
}

# ══════════════ wealth·실권 정책 — R17~R21 정본 재사용 (§14·§15) ══════════════
WEALTH = {
    "inheritedFrom": "R17 (R18~R21 에서 변경 없이 사용)",
    "policy": WEALTH_POLICY["chosen"],
    "returnBasis": WEALTH_POLICY["returnBasis"],
    "lapseRule": ("발행가 K >= 권리락가 P_ex 이면 합리적 실권. R17 정본 그대로. "
                  "결과를 보고 전량청약·전량미청약·권리매도를 새로 고르지 "
                  "않는다(§15)."),
    "externalContribution": "추가 납입금은 EXTERNAL_CONTRIBUTION. investment gain 아님.",
    "hardRule": "공짜 wealth 생성 금지.",
    "ratioBasis": {
        "used": "ENTITLEMENT (배정 기준)",
        "why": ("R20 정본. canonical 은 1주 보유자 관점이고 그 주주의 권리는 "
                "배정주식수이지 다른 주주들의 청약률이 아니다."),
        "alsoRecord": "실제 청약률(take-up) 을 함께 기록한다.",
    },
}

# ══════════════ wealth 확정 기준 (§16·§17) ══════════════
WEALTH_STATUS = {
    "WEALTH_CONFIRMED": ("rights event 확정 AND 실발행수 충분 AND 확정발행가 "
                         "충분 AND 배정/유효비율 확보 AND 외부납입 계산 가능 "
                         "AND same-month 분리 가능"),
    "WEALTH_PARTIAL": "핵심값 일부 결측",
    "WEALTH_UNRESOLVED": "wealth 계산 불가",
    "noPromotionFromLow": "LOW confidence 를 WEALTH_CONFIRMED 로 과장하지 않는다.",
}
CONFIDENCE = {
    "HIGH": "사후 직접공시 + 최종 숫자 + 주식수 대조 통과",
    "MEDIUM": "직접공시 + 일부 파생값",
    "LOW": "계획치·간접추론 중심",
    "ratioTolerance": 0.15,
}

# ══════════════ bias (§20) ══════════════
BIAS = {
    "methodologyUnchanged": "R21 그대로. expected = P × 조건부 과소평가 크기",
    "notAllowed": "새 계산식 · 조건부 크기 재정의 · threshold 완화",
    "r21ExpectedBiasPct": 3.470,
    "groupContributionPp": 2.242,
    "r21SensitivityProjection": 1.520,
    "projectionCaveat": ("R21 감도는 '이 군이 **전부** 해결될 때' 값이다. 일부만 "
                         "해결되면 그 값에 도달하지 않는다. 투영을 결과로 쓰지 "
                         "않고 실측값을 보고한다."),
}

# ══════════════ 판정 (§21·§22) — 승계, 변경 금지 ══════════════
VERDICT = {
    "inheritedFrom": "R16~R21 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "minimumForPass": "unresolved universe ratio < 20% AND expected bias <= 2%",
    "r21Status": {"unresolvedTickerPct": 3.36, "expectedBiasPct": 3.470,
                  "firstConditionMet": True, "secondConditionMet": False},
    "stopWhenGateMet": ("조건을 충족하면 완벽주의로 추가 foundation research 를 "
                        "계속하지 않는다(§22)."),
}

FORBIDDEN = [
    "BM/EY/ROE/SIZE/QUALITY/Magic Formula 재계산", "portfolio", "보유기간 연구",
    "target 추가·삭제", "새 crawler framework", "무한 기간 검색",
    "기존 R16~R21 산출물 덮어쓰기", "R5~R14 자동 승격",
    "threshold 변경", "bias 계산식 변경", "실권 policy 신설",
    "계획치를 확정치로 승격", "추정으로 채우기",
    "유료 API·데이터", "신규 key/token", "env 변경",
    "production write", "실주문", "외부발송", "deploy",
]


def build_targets():
    rows = json.loads((RD / "r21-full-reconciliation-latest.json")
                      .read_text(encoding="utf-8"))["rows"]
    nm = json.loads((RD / "r20-final-terms-normalized-latest.json")
                    .read_text(encoding="utf-8"))["events"]
    by20 = {(e["ticker"], e["date"]): e for e in nm}
    cc = json.loads((ROOT / "_cache" / "dart-corp-codes.json")
                    .read_text(encoding="utf-8"))
    import r16_canonical as C
    from r16_audit import contiguous_span
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    pos = {d: i for i, d in enumerate(ds)}

    out = []
    for r in rows:
        if r.get("wealthReason") not in ("R20_FINAL_TERMS_INCOMPLETE",
                                         "R21_ENTITLEMENT_TERMS_MISSING"):
            continue
        t = r["ticker"]
        e = by20.get((t, r["date"])) or {}
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
            "originalRceptNo": e.get("originalRceptNo") or r.get("rcept_no"),
            "originalFilingDate": e.get("originalFilingDate"),
            "rightsConfirmationSource": r.get("wealthReason"),
            "plannedIssueMethod": e.get("plannedIssueMethod"),
            "plannedNewShares": e.get("plannedNewShares"),
            "plannedIssuePrice": e.get("plannedIssuePrice"),
            "plannedRightsRatio": e.get("plannedRightsRatio"),
            "knownCorrections": e.get("knownCorrectionReceipts") or [],
            "observedSharesBefore": sb, "observedSharesAfter": sa,
            "observedShareDelta": (sa - sb) if (sb and sa) else None,
            "observedShareRatio": r["shareRatio"],
            "priceRatio": r.get("priceRatio"),
            "r20WealthStatus": r.get("r20WealthStatus"),
            "r20EventStatus": r.get("r20EventStatus"),
            "r20ResultReportCandidates": e.get("resultReportCandidates"),
            "r21WealthStatus": r.get("wealthLayer"),
            "unresolvedFields": ["finalActualNewShares", "finalIssuePrice",
                                 "finalRightsRatio",
                                 "finalShareholderAllocatedShares"],
            "currentBiasP": 1.0,
            "currentBiasSource": "DIRECT (주주배정 확정, 조건만 미확보)",
        })
    return out


def main() -> int:
    tg = build_targets()
    payload = {
        "task": "R22",
        "taskId": "WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22",
        "writtenBeforeExpandedSearchResults": True,
        "inherits": {
            "from": "R21",
            "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 3.36, "expectedBiasPct": 3.470,
            "biasThresholdPct": 2.0, "unresolvedThresholdPct": 20.0,
            "groupContributionPp": 2.242,
            "sensitivityIfFullyResolved": 1.520,
            "onlyFailingCondition": "expectedBias",
        },
        "purpose": ("주주배정은 이미 확정됐다. 실제 발행수량·확정발행가·배정비율을 "
                    "사후 직접공시로 복원한다."),
        "targetDefinition": ("R21 wealthReason in "
                            "{R20_FINAL_TERMS_INCOMPLETE, "
                            "R21_ENTITLEMENT_TERMS_MISSING}"),
        "targetTotal": len(tg),
        "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": sum(1 for t in tg if t["corpCode"]),
        "scopeGuard": "27건만. target 추가·삭제 금지(§2).",
        "howThisDiffersFromR20": {
            "r20Approach": "기존 창(-6~+9)에서 증권발행실적보고서만 검색",
            "r20Failure": "25건이 '창 안에 구주주 배정 실적보고서 없음' 으로 남음",
            "r22Approach": ("창 확대(-3~+18) + 공시유형 확장(발행가확정·청약결과·"
                            "실권처리·신주상장·증자완료·자본금변동)"),
            "why": "같은 시도를 단순 반복하지 않는다(§3).",
        },
        "sourcePriority": SOURCE_PRIORITY, "sourceRule": SOURCE_RULE,
        "window": WINDOW, "layerRule": LAYER_RULE, "issuePrice": ISSUE_PRICE,
        "subscription": SUBSCRIPTION, "sameMonth": SAME_MONTH,
        "effectiveRatio": EFFECTIVE_RATIO, "completion": COMPLETION,
        "wealth": WEALTH, "wealthStatus": WEALTH_STATUS,
        "confidence": CONFIDENCE, "bias": BIAS, "verdict": VERDICT,
        "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r22-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                 encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "tickers": len({t["ticker"] for t in tg}),
                      "corpCodeMapped": payload["corpCodeMapped"],
                      "window": WINDOW["months"],
                      "biasThresholdPct": 2.0, "thresholdChanged": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
