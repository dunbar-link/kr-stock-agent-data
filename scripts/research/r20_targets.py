#!/usr/bin/env python3
"""R20 대상 확정 + 최종조건 복원 정책 — **결과 이전에** 고정한다.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

R19 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 6.74% (20% 게이트 통과)
  **기대편향 5.316% > 2.00%** 이 유일한 미통과 조건
  최대 기여군 = R18_CONFIRMED_HOLDER_RIGHT_TERMS_MISSING 92건 (3.641%p)

R20 단일 목적(§1): 그 92건은 **주주배정이라는 사실은 이미 확정**됐다. 막고 있는 것은
실제로 몇 주를 얼마에 발행했는지다. 계획공시가 아니라 **사후 확정공시**로
실발행량·확정발행가·최종배정비율을 복원한다. factor 연구 금지(§22).

threshold 와 bias 계산법은 R16~R19 것을 그대로 승계한다. 변경 금지(§18·§20).

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

# ══════════════ 직접소스 우선순위 (§3·§6) ══════════════
SOURCE_PRIORITY = [
    {"rank": 1, "source": "증권발행실적보고서",
     "why": "실제 발행수·확정발행가·납입총액·청약결과가 사후 확정치로 들어 있다.",
     "titles": ["증권발행실적보고서"]},
    {"rank": 2, "source": "유상증자 정정공시(최종본)",
     "why": "확정발행가·최종 신주수로 정정된 조건.",
     "titles": ["[기재정정]주요사항보고서(유상증자결정)", "유상증자결정"]},
    {"rank": 3, "source": "신주상장 / 추가상장 공시",
     "why": "실제 상장된 신주 수 = 실발행량의 직접 증거.",
     "titles": ["추가상장", "신주상장", "변경상장"]},
    {"rank": 4, "source": "청약결과 · 실권주 처리결과 공시",
     "why": "주주배정분과 실권주 공모분의 분리.",
     "titles": ["청약", "실권주", "발행결과"]},
    {"rank": 5, "source": "발행가액 확정 공시",
     "why": "예정발행가 → 확정발행가.",
     "titles": ["발행가액", "확정발행가"]},
]
SOURCE_RULE = ("planned terms 보다 **post-event actual terms** 를 우선한다(§3). "
               "사후 확정치가 없으면 계획치로 승격하지 않는다.")

# ══════════════ PLANNED vs FINAL_ACTUAL 분리 (§5) ══════════════
LAYER_RULE = {
    "mustSeparate": ["planned_new_shares / final_new_shares",
                     "planned_issue_price / final_issue_price",
                     "planned_rights_ratio / effective_rights_ratio",
                     "planned_listing_date / actual_listing_date"],
    "hardRule": "planned 값과 actual 값을 같은 필드에 섞어 저장하지 않는다.",
    "why": "섞으면 어느 것이 근거였는지 사후에 복원할 수 없다.",
}

# ══════════════ 일부발행·철회 (§8) ══════════════
PARTIAL_RULE = {
    "rule": ("계획 100만주인데 실제 60만주가 발행됐으면 **60만주 기준**으로만 "
             "rights wealth 를 계산한다. 계획치로 계산 금지."),
    "cancelled": "발행이 철회됐으면 NO_EVENT / CANCELLED 로 처리하고 조정하지 않는다.",
    "why": "실권·부분청약이 흔하고, 계획치를 쓰면 존재하지 않은 wealth 를 만든다.",
}

# ══════════════ 주식수 대조 (§9) ══════════════
SHARE_RECONCILIATION = {
    "hardRule": ("관측 주식수 변화(after - before)를 rights 신주로 **강제하지 "
                 "않는다**. 같은 달에 CB 전환·BW 행사·옵션 행사·자기주식 소각·"
                 "분할·합병이 섞일 수 있다."),
    "method": ("R19 가 만든 직접증거(자본행위 분류)로 같은 기간 다른 사건을 "
               "분리한 뒤 잔여분과 대조한다."),
    "tolerance": 0.15,
    "toleranceWhy": "R17~R19 와 동일. 새 값을 발명하지 않는다.",
    "onConflict": "DATA_CONFLICT flag. 억지로 한쪽 숫자를 고르지 않는다.",
}

# ══════════════ 최종 배정비율 (§10) ══════════════
EFFECTIVE_RATIO = {
    "formula": "effective_rights_ratio = 주주배정 실배정주식수 / 배정기준일 기존주식수",
    "compareTo": "공시 명시비율",
    "onMismatch": "DATA_CONFLICT flag + 사후 확정치 우선(§3)",
}

# ══════════════ 발행가 (§11) ══════════════
ISSUE_PRICE = {
    "use": "최종 확정발행가",
    "ifOnlyPlanned": "WEALTH_PARTIAL 유지. 예정가로 확정 승격 금지.",
    "hardRule": "결과가 좋아지는 가격을 고르지 않는다.",
}

# ══════════════ wealth·실권 정책 — R17~R19 정본 재사용 (§12·§13) ══════════════
WEALTH = {
    "inheritedFrom": "R17 (R18·R19 에서 변경 없이 사용)",
    "policy": WEALTH_POLICY["chosen"],
    "returnBasis": WEALTH_POLICY["returnBasis"],
    "lapseRule": ("발행가 K >= 권리락가 P_ex 이면 합리적 실권. R17 정본 그대로. "
                  "새 policy 를 만들지 않는다(§13)."),
    "externalContribution": "추가 납입금은 EXTERNAL_CONTRIBUTION. 투자수익 아님.",
    "unsubscribedSplit": ("실권주 일반공모가 실제 있었으면 기존 주주 귀속분과 "
                          "외부청약자분을 분리한다. 외부분은 기존 주주 권리가 아니다."),
    "hardRule": "공짜 신주·공짜 wealth 금지.",
}

# ══════════════ wealth 확정 기준 (§16) ══════════════
WEALTH_STATUS = {
    "WEALTH_CONFIRMED": ("final issue method 확인 AND 실발행량 충분히 확인 AND "
                         "확정발행가 확인 AND 주주배정분 확인·도출 가능 "
                         "→ canonical wealth 계산 가능"),
    "WEALTH_PARTIAL": "핵심값 일부 결측",
    "WEALTH_UNRESOLVED": "wealth 계산 불가",
    "noPromotionFromLow": "LOW confidence 는 WEALTH_CONFIRMED 로 승격하지 않는다(§17).",
}

# ══════════════ confidence (§17) ══════════════
CONFIDENCE = {
    "HIGH": "사후 직접공시 + 수량 대조 통과",
    "MEDIUM": "직접공시 있으나 일부 파생 필드",
    "LOW": "계획치·간접추론 위주",
    "rule": "LOW → WEALTH_CONFIRMED 승격 금지",
}

# ══════════════ bias 재계산 (§18·§19) ══════════════
BIAS = {
    "methodologyUnchanged": "R19 그대로. expected = P(직접권리) × 조건부 과소평가 크기",
    "conditionalSource": "확정된 주주배정 사건의 실측 중앙값 (R18~R19 와 동일)",
    "whatChanges": ("이번 작업으로 확정되는 사건은 미해결 pool 에서 빠진다. "
                    "P 배정 규칙 자체는 R19 것을 그대로 쓴다."),
    "notAllowed": "새 계산식 · 조건부 크기 재정의 · threshold 완화",
    "decomposition": ["HOLDER_RIGHT_TERMS_MISSING", "OTHER_RIGHTS_PARTIAL",
                      "TRUE_UNRESOLVED", "OTHER_CAPITAL_ACTION", "DATA_GAP"],
    "arithmeticNote": (
        "★ 사전 확인: R19 보고서는 '이 군이 해결되면 5.316 - 3.641 = 1.675%' 라고 "
        "투영했다. 그 뺄셈은 **분모가 고정일 때만** 성립한다. 지표는 미해결 1건당 "
        "기대값(sumP / N_unresolved × cond)이므로 92건이 pool 에서 빠지면 분모도 "
        "함께 준다. 정확한 상한은 실행 후 계산해 보고한다 — 투영을 결과로 쓰지 "
        "않는다."),
}

# ══════════════ 판정 (§20·§21) — 승계, 변경 금지 ══════════════
VERDICT = {
    "inheritedFrom": "R16/R17/R18/R19 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "minimumForPass": "unresolved universe ratio < 20% AND expected bias <= 2%",
    "r19Status": {"unresolvedTickerPct": 6.74, "expectedBiasPct": 5.316,
                  "firstConditionMet": True, "secondConditionMet": False},
    "doNotOverRun": ("사전 기준을 충족하면 완벽주의로 foundation 연구를 계속하지 "
                     "않는다(§21)."),
}

FORBIDDEN = [
    "BM/SIZE/QUALITY/ROE/EY/Magic Formula 재계산", "portfolio", "보유기간 연구",
    "새 universe 확장", "새 crawler framework",
    "기존 R16~R19 산출물 덮어쓰기", "R5~R14 자동 복원",
    "threshold 변경", "bias 계산법 변경", "실권 policy 신설",
    "계획치를 확정치로 승격", "추정으로 foundation 통과",
    "유료 API·데이터", "신규 key/token", "env 변경",
    "production write", "실주문", "외부발송", "deploy",
]


def build_targets():
    rows = json.loads((RD / "r19-full-reconciliation-latest.json")
                      .read_text(encoding="utf-8"))["rows"]
    nm = json.loads((RD / "r18-legacy-rights-normalized-latest.json")
                    .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in nm}
    cc = json.loads((ROOT / "_cache" / "dart-corp-codes.json")
                    .read_text(encoding="utf-8"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import r16_canonical as C
    from r16_audit import contiguous_span
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    pos = {d: i for i, d in enumerate(ds)}

    out = []
    for r in rows:
        if r.get("wealthReason") != "R18_TERMS_UNRELIABLE":
            continue
        t = r["ticker"]
        e = by.get((t, r["date"])) or {}
        ev = e.get("evidence") or {}
        chain = ev.get("chain") or []
        hr = [c for c in chain
              if c.get("issueMethod") in ("SHAREHOLDER_RIGHTS",
                                          "RIGHTS_THEN_PUBLIC", "MIXED")]
        pick = hr[-1] if hr else {}
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
            "originalRceptNo": pick.get("rcept_no") or r.get("rcept_no"),
            "originalFilingDate": pick.get("filingDate"),
            "plannedIssueMethod": pick.get("issueMethod"),
            "plannedNewShares": None,
            "plannedIssuePrice": pick.get("issuePrice"),
            "plannedRightsRatio": pick.get("rightsRatio"),
            "observedSharesBefore": sb, "observedSharesAfter": sa,
            "observedShareRatio": r["shareRatio"],
            "priceRatio": r.get("priceRatio"),
            "currentWealthStatus": "WEALTH_PARTIAL",
            "currentWealthReason": r.get("wealthReason"),
            "currentBiasP": 1.0,
            "knownCorrectionReceipts": [c.get("rcept_no") for c in chain],
            "windowFilings": len(chain),
            "unresolvedFields": [k for k, v in
                                 (("finalNewShares", None),
                                  ("finalIssuePrice", pick.get("issuePrice")),
                                  ("effectiveRightsRatio", None),
                                  ("shareholderAllocationShares", None))
                                 if not v],
        })
    return out


def main() -> int:
    tg = build_targets()
    payload = {
        "task": "R20",
        "taskId": "WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20",
        "writtenBeforePostIssuanceResults": True,
        "inherits": {
            "from": "R19",
            "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 6.74, "expectedBiasPct": 5.316,
            "biasThresholdPct": 2.0, "unresolvedThresholdPct": 20.0,
            "groupContributionPp": 3.641,
            "onlyFailingCondition": "expectedBias",
        },
        "purpose": ("주주배정이라는 사실은 이미 안다. 실제로 몇 주를 얼마에 "
                    "발행했는지를 사후 확정공시로 복원한다."),
        "targetDefinition": "R19 wealthReason == R18_TERMS_UNRELIABLE",
        "targetTotal": len(tg),
        "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": sum(1 for t in tg if t["corpCode"]),
        "scopeGuard": "92건만. 새 universe 확장 금지(§2). precommit 이후 target 변경 금지.",
        "sourcePriority": SOURCE_PRIORITY, "sourceRule": SOURCE_RULE,
        "layerRule": LAYER_RULE, "partialRule": PARTIAL_RULE,
        "shareReconciliation": SHARE_RECONCILIATION,
        "effectiveRatio": EFFECTIVE_RATIO, "issuePrice": ISSUE_PRICE,
        "wealth": WEALTH, "wealthStatus": WEALTH_STATUS,
        "confidence": CONFIDENCE, "bias": BIAS, "verdict": VERDICT,
        "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r20-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "tickers": len({t["ticker"] for t in tg}),
                      "corpCodeMapped": payload["corpCodeMapped"],
                      "withPlannedPrice": sum(1 for t in tg
                                              if t["plannedIssuePrice"]),
                      "biasThresholdPct": 2.0, "thresholdChanged": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
