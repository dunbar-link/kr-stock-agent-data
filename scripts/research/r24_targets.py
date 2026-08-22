#!/usr/bin/env python3
"""R24 대상 50건 사전 고정 (precommit).

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

결과를 보기 **전에** 대상·분류체계·판정규칙·기준을 고정한다(§2).
R16~R23 의 기준·bias 계산식은 그대로 승계한다. 새 지표·새 threshold 금지(§18·§19).

안전: 읽기 전용 + reports/research write 만. 네트워크 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CORP = ROOT / "_cache" / "dart-corp-codes.json"

TASK = "WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24"

PURPOSE = (
    "FULLY_UNKNOWN 50건의 주식수 변동이 기존 주주에게 경제적 권리가 발생한 "
    "사건인지 직접 evidence 로 판정한다. 목적은 2% 를 맞추는 것이 아니라 "
    "모집단 추정 P=0.296 을 실제 관측으로 대체하는 것이다(§1·§36).")

NO_RESULT_ENGINEERING = (
    "숫자를 좋게 만들기 위한 판정 변경 금지. LOW 를 확정으로 승격 금지. "
    "제목만 보고 기계판정 금지. 조건이 없으면 UNKNOWN 유지. "
    "gate·threshold·bias 계산식 이동 금지(§18·§19·§35).")

# ── §5 사건 유형 ──────────────────────────────────────────────────────
# entitlement: True=기존주주 권리 발생 / False=발생 안 함 / None=직접확인 필요
EVENT_TYPES = {
    "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS": {
        "entitlement": True, "why": "구주주 배정. 기존주주가 청약권을 받는다."},
    "RIGHTS_THEN_PUBLIC_UNSUBSCRIBED": {
        "entitlement": True, "why": "구주주 배정 후 실권주 일반공모. 배정 자체는 기존주주."},
    "PUBLIC_OFFERING": {
        "entitlement": False, "why": "일반공모. 기존주주 우선권 없음."},
    "THIRD_PARTY_ALLOCATION": {
        "entitlement": False, "why": "제3자 배정. 기존주주는 받지 않는다."},
    "CB_CONVERSION": {
        "entitlement": False, "why": "사채권자의 전환. 보통주 주주의 새 권리가 아니다."},
    "BW_EXERCISE": {
        "entitlement": False, "why": "신주인수권 보유자의 행사. 보통주 주주 권리 아님."},
    "STOCK_OPTION_EXERCISE": {
        "entitlement": False, "why": "임직원 옵션 행사. 주주 권리 아님."},
    "PREFERRED_CONVERSION": {
        "entitlement": False,
        "why": ("전환우선주·상환전환우선주 보유자의 전환청구. 보통주 주주에게 "
                "새 권리가 생기는 사건이 아니다(CB 전환과 같은 성격).")},
    "MERGER_NEW_SHARES": {
        "entitlement": None, "why": "방향성 확인 필요(§7). 신주 수령 주체를 본다."},
    "SHARE_EXCHANGE": {
        "entitlement": None, "why": "교환 상대방이 받는지 확인 필요(§7)."},
    "COMPANY_SPLIT": {
        "entitlement": None, "why": "인적/물적 분할 구분 필요."},
    "STOCK_SPLIT": {
        "entitlement": True, "why": "액면분할. 기존주주 보유주식수가 기계적으로 증가."},
    "REVERSE_SPLIT": {
        "entitlement": True, "why": "액면병합. 기존주주 보유주식수가 기계적으로 감소."},
    "BONUS_ISSUE": {
        "entitlement": True, "why": "무상증자. 기존주주가 신주를 직접 받는다."},
    # ★ 자체수정 4 (§31): 초판 분류규칙에는 '주식배당' 이 있었는데 유형표에는
    #   없어서, 실제로 잡힌 4건이 전부 entitlement UNKNOWN 으로 떨어졌다.
    #   주식배당은 무상증자와 같이 기존주주가 신주를 직접 받는 사건이다.
    "STOCK_DIVIDEND": {
        "entitlement": True,
        "why": "주식배당. 배당을 현금 대신 신주로 받는다. 기존주주가 직접 수령."},
    "CAPITAL_REDUCTION": {
        "entitlement": None, "why": "유상감자/무상감자 구분 필요(§11)."},
    "TREASURY_SHARE_CANCELLATION": {
        "entitlement": False,
        "why": "자기주식 소각. 발행주식총수는 줄지만 개별주주 보유주식수는 그대로(§11)."},
    "OTHER_CAPITAL_ACTION": {"entitlement": None, "why": "직접 확인 필요."},
    "NO_MATERIAL_CAPITAL_ACTION": {
        "entitlement": False, "why": "창 안에 주주 wealth 를 바꿀 자본행위가 없다."},
    "UNKNOWN": {"entitlement": None, "why": "직접 evidence 없음."},
}

MECHANICAL_TYPES = ["STOCK_SPLIT", "REVERSE_SPLIT", "BONUS_ISSUE",
                    "STOCK_DIVIDEND"]
CANNOT_EXPLAIN_SHARE_INCREASE = [
    "TREASURY_SHARE_CANCELLATION", "CAPITAL_REDUCTION"]

# ── §4 직접소스 우선순위 ──────────────────────────────────────────────
SOURCE_PRIORITY = [
    (1, "주요사항보고서"), (2, "증권신고서"), (3, "정정증권신고서"),
    (4, "증권발행결과"), (5, "증권발행실적보고서"), (6, "청약결과"),
    (7, "발행가액확정"), (8, "추가상장/신주상장"),
    (9, "합병/회사분할/주식교환 공시"), (10, "전환청구권행사"),
    (11, "신주인수권행사"), (12, "주식매수선택권행사"), (13, "무상증자"),
    (14, "유상증자"), (15, "주식분할/병합"), (16, "감자"),
    (17, "자기주식 소각"), (18, "사업보고서 자본금 변동표"),
]

CONFIDENCE = {
    "HIGH": "직접 공시 구조필드/표 + 수량 reconciliation",
    "MEDIUM": "직접 공시 본문 + 복수 source cross-check",
    "LOW": "제목/간접추론/불완전 keyword — 최종 직접확정으로 사용 금지(§14)",
    "ratioTolerance": 0.15,
}

# ── §26 수집 창 ───────────────────────────────────────────────────────
# R19 는 유형별 -6~+3 을 썼고 이 50건은 거기서 아무것도 못 찾은 잔여다.
# 결정공시가 훨씬 앞서거나 행사·상장이 뒤로 밀리는 경우를 덮되 유계로 둔다.
WINDOW = {"months": "-12 ~ +6", "before": -12, "after": 6,
          "why": "R19(-6~+3)에서 못 찾은 잔여다. 창을 넓히되 무한 확장하지 않는다(§26)."}


def _tsr_matches():
    p = RD / "r23-full-reconciliation-latest.json"
    return json.loads(p.read_text(encoding="utf-8"))["rows"]


def bias_group(m):
    """R21~R23 이 쓰는 편향 조건화 군. 계산식 변경 없이 그대로 옮긴다(§18)."""
    reason = m.get("wealthReason")
    if m.get("r21Entitlement") == "NO":
        return "NO_ENTITLEMENT_CONFIRMED", 0.0
    if reason in ("R23_ALLOCATION_STILL_MISSING",
                  "R22_FINAL_TERMS_STILL_INCOMPLETE",
                  "R21_ENTITLEMENT_TERMS_MISSING",
                  "R20_FINAL_TERMS_INCOMPLETE"):
        return "HOLDER_RIGHT_TERMS_MISSING", 1.0
    if m.get("r19Entitlement") == "NO":
        return "NO_ENTITLEMENT_CONFIRMED", 0.0
    if m.get("r19Entitlement") == "YES":
        return "TRUE_RIGHTS", 1.0
    if reason == "REDUCTION_PAID_OR_FREE_UNKNOWN":
        return "OTHER_CAPITAL_ACTION", 0.0
    return "TRUE_UNRESOLVED", None          # 모집단 추정에 의존


def main() -> int:
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    pos = {d: i for i, d in enumerate(ds)}
    cc = json.loads(CORP.read_text(encoding="utf-8"))

    tg = []
    for m in _tsr_matches():
        if m.get("resolvedWealth"):
            continue
        grp, p = bias_group(m)
        if grp != "TRUE_UNRESOLVED":
            continue
        t, dt = m["ticker"], m["date"]
        i = pos.get(dt)
        prev = ds[i - 1] if i else None
        a = (cap.get(prev) or {}).get(t) or {}
        b = (cap.get(dt) or {}).get(t) or {}

        # ★ corp_code 매핑. 우선주 라인은 DART corpCode 에 자기 코드가 없다.
        #   같은 회사의 보통주 코드로 조회해야 공시가 나온다. 추정이 아니라
        #   실제 매핑표에 존재하는지 확인하고, 근거를 기록한다(§31 SC1).
        ent, src, base = cc.get(t), "DIRECT", None
        if not ent:
            base = t[:5] + "0"
            ent = cc.get(base)
            src = "PREFERRED_LINE_TO_COMMON_BASE" if ent else "UNMAPPED"

        tg.append({
            "ticker": t, "baseTicker": base,
            "corpCode": (ent or {}).get("corp_code"),
            "companyName": (ent or {}).get("corp_name"),
            "corpCodeSource": src,
            "shareClass": "COMMON" if t.endswith("0") else "PREFERRED_OR_OTHER",
            "date": dt, "eventMonth": dt[:7], "prevDate": prev,
            "sharesBefore": a.get("shares"), "sharesAfter": b.get("shares"),
            "shareRatio": m.get("shareRatio"),
            "shareDelta": (None if not (a.get("shares") and b.get("shares"))
                           else b["shares"] - a["shares"]),
            "priceBefore": a.get("close"), "priceAfter": b.get("close"),
            "priceRatio": m.get("priceRatio"),
            "knownFilingIds": [x for x in [m.get("r19RceptNo")] if x],
            "r16Label": m.get("label"), "r16Confidence": m.get("confidence"),
            "r16Provenance": m.get("provenance"),
            "r17Source": m.get("source"),
            "r19PrimaryEvent": m.get("r19PrimaryEvent"),
            "r19Entitlement": m.get("r19Entitlement"),
            "r19Provenance": m.get("r19Provenance"),
            "r20Source": m.get("r20Source"), "r21Source": m.get("r21Source"),
            "r22Source": m.get("r22Source"), "r23Source": m.get("r23Source"),
            "currentEntitlement": "UNKNOWN",
            "currentWealthStatus": m.get("wealthLayer") or "WEALTH_UNRESOLVED",
            "currentBiasClass": "TRUE_UNRESOLVED",
            "currentBiasP": "POPULATION_ESTIMATE_0.296",
            "whyUnknown": m.get("reason") or m.get("wealthReason"),
        })

    tg.sort(key=lambda x: (x["date"], x["ticker"]))
    mapped = [t for t in tg if t["corpCode"]]
    pref = [t for t in tg if t["corpCodeSource"] == "PREFERRED_LINE_TO_COMMON_BASE"]

    bias = json.loads((RD / "r23-expected-bias-latest.json")
                      .read_text(encoding="utf-8"))
    out = {
        "task": "R24", "taskId": TASK,
        "writtenBeforeCollection": True,
        "inherits": "R16~R23 precommit (기준·계산식 변경 없음)",
        "purpose": PURPOSE, "noResultEngineering": NO_RESULT_ENGINEERING,
        "targetDefinition": ("R23 canonical reconciliation 에서 편향 조건화군이 "
                             "TRUE_UNRESOLVED 인 사건 전부. 모집단 추정 P 에 "
                             "의존하는 유일한 군이다."),
        "targetTotal": len(tg), "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": len(mapped), "corpCodeUnmapped": len(tg) - len(mapped),
        "preferredLineRemapped": len(pref),
        "preferredRemapRule": (
            "우선주 라인은 DART corpCode 에 자기 종목코드가 없다. 같은 회사의 "
            "보통주 코드(앞 5자리 + '0')가 매핑표에 실제로 존재할 때만 그 "
            "corp_code 로 조회한다. 추정 매핑을 만들지 않는다."),
        "scopeGuard": ("이 50건 밖으로 확장 금지. 새 universe·새 crawler·"
                       "새 framework 금지(§3·§26)."),
        "sourcePriority": [{"rank": r, "source": s} for r, s in SOURCE_PRIORITY],
        "window": WINDOW,
        "eventTypes": EVENT_TYPES,
        "mechanicalTypes": MECHANICAL_TYPES,
        "cannotExplainShareIncrease": CANNOT_EXPLAIN_SHARE_INCREASE,
        "entitlementRule": (
            "사건명만으로 기계판정 금지(§6). 누가 신주를 받았는지 직접 evidence "
            "로 확인한다. 합병·주식교환·분할은 방향성을 먼저 본다(§7)."),
        "confidence": CONFIDENCE,
        "correctionChain": ("정정공시가 있으면 최종 조건 사용. initial·correction·"
                            "final chain 보존하고 결과를 바꾼 정정인지 명시(§15)."),
        "reconciliation": {
            "rule": "observed share delta ↔ 분류된 자본행위 주식수 대조(§16)",
            "tolerance": 0.15,
            "marketCapRule": ("marketCap = close × shares 이므로 독립 evidence 로 "
                              "쓰지 않는다. R16 에서 확인한 오류를 반복하지 않는다."),
            "onConflict": "DATA_CONFLICT flag. 억지로 하나를 고르지 않는다."},
        "wealth": {
            "inheritedFrom": "R16(기계적 조정) + R17(유상증자 정책)",
            "policy": "POLICY_A_ASSUME_FULL_EXERCISE",
            "returnBasis": "TIME_WEIGHTED — 외부 납입을 중립화한다.",
            "lapseRule": "발행가 K >= 권리락가 P_ex 이면 합리적 실권. R17 정본 그대로.",
            "noEntitlementRule": ("NO 로 확정되면 wealth adjustment = 0. 주가 움직임을 "
                                  "지우는 것이 아니라 없는 권리를 만들지 않는 것이다(§13)."),
            "hardRule": "공짜 wealth 생성 금지. 조건 부족하면 PARTIAL 유지(§12)."},
        "anchors": {"minYes": 3, "minNo": 5, "minUnknown": 2,
                    "tolerancePp": 0.05},
        "bias": {
            "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
            "unchangedFrom": "R21/R22/R23",
            "baselinePct": bias["expectedBiasPct"],
            "thresholdPct": 2.0,
            "denominatorRule": "미해결 사건 수. 분모·조건화 방식 변경 금지(§18).",
            "warning": ("R21 실측: NO 확정은 분모를 줄여 지표를 **올릴 수** 있다. "
                        "내려갈 것이라고 예단하지 않는다.")},
        "verdict": {
            "inheritedFrom": "R16~R23 precommit", "unchanged": True,
            "gate": {"unresolvedTickerPctMax": 20.0, "expectedBiasPctMax": 2.0},
            "onPass": "CANONICAL_TSR_FOUNDATION_PASS(_WITH_LIMITATIONS) 로 종료(§20)",
            "onFail": ("BLOCKED 유지. 잔여 편향을 기여도순 분해하고 가장 큰 단일 "
                       "material group 하나만 다음 작업으로 지정(§35).")},
        "forbidden": ["factor 연구", "portfolio", "보유기간", "CAGR 전략비교",
                      "gate/threshold 변경", "새 모집단 추정치 발명",
                      "R25 실행", "50건 밖 확장"],
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r24-targets-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    print(json.dumps({k: out[k] for k in
                      ("targetTotal", "targetTickers", "corpCodeMapped",
                       "corpCodeUnmapped", "preferredLineRemapped")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
