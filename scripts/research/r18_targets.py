#!/usr/bin/env python3
"""R18 대상 확정 + parser 정책 — **결과 이전에** 고정한다.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

R17 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 25.57% (사전기준 20% 미달)
  최대 gap = ISSUE_METHOD_UNKNOWN 1,081건 (그 중 2007~2014 가 903건)

R18 단일 목적(§1): 그 1,081건의 **공시 원문**을 직접 읽어 증자방식과 기존 주주의
경제적 권리를 복원한다. factor 연구는 하지 않는다(§24).

threshold 는 R16/R17 것을 그대로 승계한다. 사후 변경 금지(§20).

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

# ══════════════ 원문 스키마 (착수 전 실측한 사실) ══════════════
# DART document.xml 은 ZIP(EUC-KR XML) 이고 표 셀에 ACODE/AUNIT 필드코드가 있다.
# 2007~2023 표본 10건에서 아래 핵심 코드가 10/10 출현했다 = 양식이 안정적이다.
DOC_SCHEMA = {
    "transport": "document.xml → ZIP → {rcept_no}.xml, EUC-KR",
    "structureNote": ("표 셀에 ACODE/AUNIT 기계판독 필드코드가 있다. "
                      "§7 우선순위대로 **구조 파싱을 1순위**로 쓰고 "
                      "본문 keyword 는 마지막 수단으로만 쓴다."),
    "coreFields": {
        "CI_MTH": "증자방식 (핵심 판별자)",
        "CST_CNT": "신주 보통주 수", "PST_CNT": "신주 우선주 수",
        "BFR_CST_CNT": "증자전 발행주식총수 보통주",
        "BFR_PST_CNT": "증자전 우선주",
        "CST_ISS_VAL": "신주 발행가액 보통주", "PST_ISS_VAL": "우선주 발행가액",
        "FVAL": "1주당 액면가",
        "FND_USE1": "시설자금", "FND_USE2": "운영자금", "FND_USE3": "기타자금",
        "ANC_ACQ_AMT": "타법인유가증권취득자금",
        "DRC_DT": "이사회결의일", "NST_GV_DT": "신주 교부일",
        "PYM_DT": "납입일", "LST_PLN_DT": "신주상장예정일",
    },
    "shareholderOnlyFields": {
        "ALL_BS_DT": "신주배정기준일", "NEW_ASN_CNT": "1주당 신주배정주식수",
        "SH_BGN_DT": "청약 시작", "SH_END_DT": "청약 종료",
        "DC_RATE": "이론권리락주가 대비 할인율",
    },
    "thirdPartyOnlyFields": {"PART": "배정대상자", "RLT": "회사와의 관계",
                             "PIN_*": "보호예수"},
    "whyThisMatters": ("배정기준일·1주당 배정주식수·청약기간은 **주주배정 계열에만** "
                       "존재하고, 배정대상자·보호예수는 **제3자배정에만** 존재한다. "
                       "즉 CI_MTH 문자열과 독립적인 구조적 교차검증이 가능하다."),
}

# ══════════════ 증자방식 분류 (§6) ══════════════
# CI_MTH 원문 문자열을 그대로 받는다. 매칭 실패는 추측하지 않고 UNRESOLVED.
METHOD_RULES = [
    ("RIGHTS_THEN_PUBLIC", ["주주배정후실권주일반공모", "주주배정후일반공모",
                            "주주배정후실권주공모"]),
    ("THIRD_PARTY", ["제3자배정", "제삼자배정", "3자배정"]),
    ("PUBLIC_OFFERING", ["일반공모", "주주우선공모", "주주우선"]),
    ("SHAREHOLDER_RIGHTS", ["주주배정", "구주주배정"]),
]
METHOD_ORDER_WHY = (
    "긴 문구가 먼저다. '주주배정후 실권주 일반공모'는 '주주배정'과 '일반공모'를 "
    "모두 포함하므로 순서를 뒤집으면 오분류된다.")

HOLDER_RIGHT_METHODS = {"SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED"}
NO_HOLDER_RIGHT_METHODS = {"THIRD_PARTY", "PUBLIC_OFFERING"}

# ══════════════ confidence (§8) ══════════════
CONFIDENCE = {
    "HIGH": ("CI_MTH 구조 필드에서 방식이 명시되고, 주주배정 계열이면 배정비율 "
             "(NEW_ASN_CNT) 또는 신주수/증자전주식수 가 있으며, 숫자 정합성 "
             "검사를 통과한 경우."),
    "MEDIUM": "CI_MTH 는 명시되나 일부 numeric field 가 결측인 경우.",
    "LOW": "CI_MTH 구조 필드가 없어 본문 keyword 로 추론한 경우.",
    "UNRESOLVED": "원문 미확보 · 방식 미확인 · 숫자 충돌(DATA_CONFLICT).",
    "resolvedPolicy": ("foundation resolved 로 인정하는 것은 HIGH/MEDIUM 뿐이다. "
                       "LOW 를 direct certainty 처럼 취급하지 않는다(§8)."),
}

# ══════════════ 숫자 정합성 (§14) ══════════════
CONSISTENCY = {
    "test1": "NEW_ASN_CNT (1주당 배정) ≈ CST_CNT / BFR_CST_CNT",
    "test2": "관측 주식수 점프(shareRatio-1) ≈ CST_CNT / BFR_CST_CNT",
    "tolerance": 0.15,
    "toleranceWhy": ("R17 매칭 허용오차와 동일하게 맞춘다. 새 값을 발명하지 않는다."),
    "onConflict": "DATA_CONFLICT flag. 억지로 한쪽 숫자를 고르지 않는다.",
}

# ══════════════ wealth 정책 — R17 정본 재사용 (§11·§12·§13) ══════════════
WEALTH = {
    "inheritedFrom": "R17",
    "policy": WEALTH_POLICY["chosen"],
    "returnBasis": WEALTH_POLICY["returnBasis"],
    "lapseRule": ("발행가 K >= 권리락가 P_ex 이면 합리적 실권. R17 정본 그대로 쓰고 "
                  "결과가 좋아지는 방향으로 바꾸지 않는다(§12)."),
    "thirdPartyRule": ("제3자배정·일반공모는 기존 주주에게 rights value 0. "
                       "희석은 주가에 반영돼 있으므로 미조정이 정답(§13)."),
    "hardRule": "공짜 신주·공짜 wealth 생성 금지. 외부 납입을 수익으로 계산 금지.",
}

# ══════════════ 판정 기준 — R16/R17 승계, 변경 금지 (§20·§22) ══════════════
VERDICT = {
    "inheritedFrom": "R16/R17 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "note": ("25.57% → 19.99% 가 됐다고 자동 PASS 하지 않는다(§20). "
             "남은 unresolved 의 경제적 materiality 도 R17 방법론 그대로 본다(§21)."),
}

FORBIDDEN = [
    "BM/EY/ROE/SIZE/QUALITY/Magic Formula 수익률 계산", "factor table 생성",
    "portfolio CAGR", "factor rediscovery",
    "R16/R17 산출물 덮어쓰기", "R5~R14 결론 부활",
    "threshold 사후 변경", "유료 API·데이터", "신규 key/token", "env 변경",
    "production write", "실주문", "외부발송", "deploy",
    "전종목 대량탐색으로 범위 확대",
]


def build_targets():
    rows = json.loads((RD / "r17-rights-matching-latest.json")
                      .read_text(encoding="utf-8"))["rows"]
    tg = [r for r in rows if r.get("wealthReason") == "ISSUE_METHOD_UNKNOWN"]
    out = []
    for r in tg:
        out.append({
            "ticker": r["ticker"], "date": r["date"],
            "rcept_no": r.get("rcept_no"),
            "filingDate": r.get("filingDate"),
            "reportName": r.get("issueMethodRaw"),
            "shareRatio": r["shareRatio"],
            "observedNewPerOld": r.get("observedRatio"),
            "priceRatio": r.get("priceRatio"),
            "mcapRatio": r.get("mcapRatio"),
            "r17Confidence": r.get("confidence"),
            "r17Label": r.get("label"),
            "legacy": r["date"] < "2015",
        })
    return out


def main() -> int:
    tg = build_targets()
    legacy = [t for t in tg if t["legacy"]]
    payload = {
        "task": "R18",
        "taskId": "WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18",
        "writtenBeforeDocumentResults": True,
        "inherits": {
            "from": "R17", "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 25.57, "r16UnresolvedTickerPct": 40.8,
            "precommitThresholdPct": 20.0,
        },
        "targetDefinition": "R17 wealthReason == ISSUE_METHOD_UNKNOWN",
        "targetTotal": len(tg),
        "targetLegacy2007to2014": len(legacy),
        "targetTickers": len({t["ticker"] for t in tg}),
        "targetLegacyTickers": len({t["ticker"] for t in legacy}),
        "scopeGuard": ("R17 에서 이미 특정된 목록만 쓴다. 전종목 대량탐색으로 "
                       "범위를 넓히지 않는다(§2)."),
        "docSchema": DOC_SCHEMA,
        "methodRules": [{"type": k, "patterns": v} for k, v in METHOD_RULES],
        "methodOrderWhy": METHOD_ORDER_WHY,
        "holderRightMethods": sorted(HOLDER_RIGHT_METHODS),
        "noHolderRightMethods": sorted(NO_HOLDER_RIGHT_METHODS),
        "confidence": CONFIDENCE,
        "consistency": CONSISTENCY,
        "wealth": WEALTH,
        "verdict": VERDICT,
        "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r18-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "legacy2007to2014": len(legacy),
                      "tickers": len({t["ticker"] for t in tg}),
                      "thresholdPct": 20.0, "thresholdChanged": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
