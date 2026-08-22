#!/usr/bin/env python3
"""R19 대상 확정 + 분류·판정 정책 — **결과 이전에** 고정한다.

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19

R18 인수: CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS
  미해결 종목비율 13.31% (20% 게이트 통과)
  **기대편향 3.47% > 2.00%** 이 유일한 미통과 조건

R19 단일 목적(§1): 미해결 591건 중 최대군인 NO_DIRECT_MATCH **342건**이 실제로
어떤 자본행위였는지 DART 직접소스로 확정하고, 기존 보통주 주주에게 직접 경제적
권리가 발생했는지 판정한 뒤 기대편향을 재계산한다.

threshold 는 R16/R17/R18 것을 그대로 승계한다. 변경 금지(§17).
bias 계산법도 R18 것을 그대로 쓴다. 새 계산법 금지(§15).

안전: 무료 공개 DART 만 · env/token 변경 0 · production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r17_precommit import VERDICT_RULE  # noqa: E402  정본 승계

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# ══════════════ 핵심 경제 질문 (§4) ══════════════
CORE_QUESTION = (
    "이 사건으로 **기존 보통주 주주가 직접** 신주인수권·현금·추가주식·교환권 등 "
    "경제적 consideration 을 받았는가?")
CORE_DISTINCTION = (
    "주식수 증가에 따른 **희석**과 기존 주주에게 **직접 권리가 생기는 것**은 다른 "
    "개념이다. 희석은 이미 주가에 반영돼 있으므로 별도 조정 대상이 아니다. "
    "직접 권리가 없으면 아무것도 더하지 않는 것이 정답이다(§4·§14).")

# ══════════════ 자본행위 유형 (§3) ══════════════
# entitlement: 기존 보통주 주주의 직접 권리 여부
EVENT_TYPES = {
    "CONVERTIBLE_BOND_CONVERSION": {
        "ko": "전환사채 전환", "entitlement": False,
        "why": ("사채권자가 전환권을 행사해 신주를 받는다. 기존 보통주 주주는 "
                "신주를 받지 않는다. 희석은 주가에 반영된다(§5)."),
        "titles": ["전환청구권행사", "전환권행사", "전환청구"],
    },
    "BW_WARRANT_EXERCISE": {
        "ko": "신주인수권(BW) 행사", "entitlement": False,
        "why": ("신주인수권 보유자에게 신주가 간다. 기존 일반주주 전체에게 배정되는 "
                "것이 아니다(§6)."),
        "titles": ["신주인수권행사", "신주인수권부사채", "워런트행사"],
    },
    "EXCHANGEABLE_BOND_EXCHANGE": {
        "ko": "교환사채 교환", "entitlement": False,
        "why": "교환사채권자에게 기존 주식이 교부된다. 신주 배정이 아니다.",
        "titles": ["교환청구권행사", "교환사채"],
    },
    "STOCK_OPTION_EXERCISE": {
        "ko": "주식매수선택권 행사", "entitlement": False,
        "why": "임직원에게 신주가 간다. 기존 일반주주 직접 권리 0(§8).",
        "titles": ["주식매수선택권행사", "주식매수선택권"],
    },
    "MERGER_NEW_SHARES": {
        "ko": "합병 신주", "entitlement": "DIRECTIONAL",
        "why": ("방향을 봐야 한다(§7·§20). 분석대상이 **존속·발행** 회사면 상대회사 "
                "주주가 신주를 받으므로 기존 주주 직접 권리 0. 분석대상 주주가 "
                "대가를 받는 경우면 조정 필요."),
        "titles": ["합병결정", "회사합병", "합병"],
    },
    "SHARE_SWAP": {
        "ko": "주식교환·이전", "entitlement": "DIRECTIONAL",
        "why": ("완전모회사가 되는 쪽은 신주를 발행하고 기존 주주는 받지 않는다. "
                "완전자회사가 되는 쪽 주주가 모회사 주식을 받는다."),
        "titles": ["주식교환", "주식이전"],
    },
    "SPINOFF_RELATED_SHARES": {
        "ko": "분할 관련 신주", "entitlement": "DIRECTIONAL",
        "why": "인적분할은 기존 주주에게 배정, 물적분할은 아니다.",
        "titles": ["분할결정", "분할합병"],
    },
    "THIRD_PARTY_ISSUANCE": {
        "ko": "제3자배정", "entitlement": False,
        "why": "기존 주주는 신주를 받지 않는다(R17/R18 정본).",
        "titles": ["제3자배정", "제삼자배정"],
    },
    "PUBLIC_OFFERING": {
        "ko": "일반공모", "entitlement": False,
        "why": "기존 주주 우선권 없음.",
        "titles": ["일반공모"],
    },
    "STOCK_SPLIT": {
        "ko": "주식분할(액면분할)", "entitlement": True,
        "why": ("기존 주주의 보유주식수가 같은 배율로 늘어난다. 납입 없음. "
                "기계적 조정 대상이며 관측 shareRatio 만으로 조정이 확정된다."),
        "titles": ["주식분할결정", "액면분할"],
    },
    "BONUS_ISSUE": {
        "ko": "무상증자", "entitlement": True,
        "why": ("기존 주주에게 무상으로 신주가 배정된다. 기계적 조정 대상이며 "
                "관측 shareRatio 만으로 조정이 확정된다."),
        "titles": ["무상증자"],
    },
    "TRUE_RIGHTS_ISSUE": {
        "ko": "주주배정 유상증자", "entitlement": True,
        "why": "기존 주주에게 신주인수권이 배정된다. 조정 필요.",
        "titles": ["유상증자결정", "주주배정"],
    },
    "CAPITAL_REDUCTION": {
        "ko": "감자", "entitlement": "DIRECTIONAL",
        "why": "유상감자는 현금 수령, 무상감자는 주식수 감소. 구분 필요.",
        "titles": ["감자결정", "자본감소"],
    },
    "TREASURY_ACTION": {
        "ko": "자기주식", "entitlement": False,
        "why": "보유주식수 불변. R16 정본 그대로.",
        "titles": ["자기주식"],
    },
    "CODE_CHANGE": {"ko": "종목코드 변경", "entitlement": False,
                    "why": "실질 자본행위 아님.", "titles": ["종목코드"]},
    "OTHER_CAPITAL_ACTION": {"ko": "기타 자본행위", "entitlement": "UNKNOWN",
                             "why": "유형은 있으나 권리 판정 불가.", "titles": []},
    "UNRESOLVED": {"ko": "미확인", "entitlement": "UNKNOWN",
                   "why": "직접 증거 없음. 모르는 것은 모른다고 둔다.", "titles": []},
}

# ★ 자체수정 1 (§31): 주식수 **증가** 사건을 설명할 수 없는 유형이 있다.
#   자기주식 취득·처분과 감자는 발행주식총수를 늘리지 못한다. 그런데 이 공시들은
#   매우 흔해서 '사건월에 가장 가까운 공시'를 고르면 PRIMARY 를 가로챈다(실측 60건).
#   설명력 없는 유형은 PRIMARY 에서 배제하고 secondary 로만 남긴다.
CANNOT_EXPLAIN_SHARE_INCREASE = ["TREASURY_ACTION", "CAPITAL_REDUCTION",
                                 "CODE_CHANGE"]

# ★ 자체수정 2 (§31): '주식분할결정'(액면분할)이 '회사분할'(SPINOFF) 패턴에
#   걸려 오분류됐다(실측: shareRatio 10.0 사건들). 주식분할은 기존 주주의 주식이
#   그대로 쪼개지는 것이라 **직접 권리 YES** 이며 조정이 필요하다. 두 수정 모두
#   조정 필요 건수를 **늘리는** 방향 = 판정을 어렵게 만드는 방향이다.
MECHANICAL_TYPES = ["STOCK_SPLIT", "BONUS_ISSUE"]
MECHANICAL_WHY = ("주식분할·무상증자는 납입 없이 보유주식수가 같은 배율로 늘어난다. "
                  "발행가·배정비율 같은 추가 조건이 필요 없고 관측 shareRatio 만으로 "
                  "조정이 확정되므로 wealth 확정이 가능하다.")

NO_ENTITLEMENT_TYPES = sorted(
    k for k, v in EVENT_TYPES.items() if v["entitlement"] is False)
ENTITLEMENT_TYPES = sorted(
    k for k, v in EVENT_TYPES.items() if v["entitlement"] is True)
DIRECTIONAL_TYPES = sorted(
    k for k, v in EVENT_TYPES.items() if v["entitlement"] == "DIRECTIONAL")

# ══════════════ 합병 방향성 판정 (§7·§20) ══════════════
MERGER_DIRECTION = {
    "rule": ("분석대상 종목의 **주식수가 증가**한 사건이다. 주식수가 늘었다는 것은 "
             "그 회사가 **신주를 발행한 쪽**(존속회사·완전모회사)이라는 뜻이다. "
             "흡수당한 회사는 상장폐지되어 주식수 증가로 관측되지 않는다."),
    "consequence": ("따라서 이 표본에서 관측되는 합병 신주는 기본적으로 "
                    "**상대회사 주주**에게 가는 것이고 기존 주주 직접 권리는 0 이다."),
    "counterCase": ("분석대상 주주가 대가를 받는 경우(피합병·주식교환의 완전자회사)는 "
                    "주식수 증가가 아니라 상장폐지로 나타난다. 그 경로는 R16 의 "
                    "delisting 처리이며 이번 범위 밖이다."),
    "doNotAssume": ("'합병 신주 발행' 문구만 보고 기존 주주 권리로 오분류하지 "
                    "않는다(§7). 방향 근거를 사건마다 기록한다."),
}

# ══════════════ 수집 창 (§10·§11) ══════════════
# ★ R17/R18 은 (-6, +1) 을 썼다. 그것은 '결정' 공시가 주식수 변동보다 **앞서기**
#   때문이다. 그러나 전환청구권행사·신주인수권행사는 **사후 보고**라 주식수 변동과
#   같거나 뒤에 접수된다. 방향이 반대이므로 뒤쪽을 넓힌다.
#   이는 결과를 보고 고른 값이 아니라 공시 종류의 구조적 성질이다.
WINDOW = {
    "decision": [-6, 1],
    "exercise": [-3, 3],
    "why": ("'결정' 공시는 사건에 선행하고 '행사·청구' 공시는 사건에 후행한다. "
            "창을 유형별로 다르게 두되 R17/R18 의 결정형 창은 그대로 유지한다."),
}

# ══════════════ confidence (§12) ══════════════
CONFIDENCE = {
    "HIGH": "직접공시 + 신주수/주식수 변화가 관측치와 일치(상대오차 <= 0.15)",
    "MEDIUM": "직접공시 유형 일치 + 수량 근거 부족",
    "LOW": "간접 추론(공시명 없이 정황만)",
    "UNRESOLVED": "직접 증거 없음",
    "gate": ("공시 제목만 맞고 주식수 변화가 전혀 안 맞으면 HIGH 금지(§11). "
             "bias 계산에서 LOW 를 resolved 로 과도하게 인정하지 않는다(§12)."),
    "ratioTolerance": 0.15,
}

# ══════════════ bias 재계산 (§15·§16) ══════════════
BIAS = {
    "formulaUnchanged": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
    "conditionalSource": "R18 과 동일 — 확정된 주주배정 사건의 실측 중앙값",
    "whatChanges": (
        "계산법이 아니라 **P 의 조건화**가 바뀐다. R18 은 미해결 전체에 문서 방식 "
        "분포(29.6%)를 일괄 적용했다. R19 는 직접증거로 권리 없음이 확정된 사건에 "
        "**P=0** 을 넣는다 — §16 의 '이미 NOT_REQUIRED 로 확정된 사건은 bias 에서 "
        "제거되어야 함' 을 그대로 구현한 것이다."),
    "notAllowed": "새 bias 계산법 도입 · 조건부 크기 재정의 · threshold 완화",
    "decomposition": ["TRUE_RIGHTS", "CB_CONVERSION", "BW_EXERCISE", "MERGER",
                      "STOCK_OPTION", "OTHER", "UNRESOLVED"],
}

# ══════════════ 판정 (§17·§18) — 승계, 변경 금지 ══════════════
VERDICT = {
    "inheritedFrom": "R16/R17/R18 precommit",
    "unchanged": True,
    "rule": VERDICT_RULE,
    "minimumForPass": "unresolved universe ratio < 20% AND expected bias <= 2%",
    "r18Status": {"unresolvedTickerPct": 13.31, "expectedBiasPct": 3.47,
                  "firstConditionMet": True, "secondConditionMet": False},
    "doNotOverRun": ("사전 기준을 충족하면 다음 단계로 넘어간다. 남은 미세 gap 때문에 "
                     "무한정 foundation 연구를 계속하지 않는다(§18)."),
}

FORBIDDEN = [
    "BM/SIZE/QUALITY/ROE/EY/Magic Formula 성과 계산", "portfolio", "factor 재발견",
    "새 universe 확장", "새 crawler framework",
    "기존 산출물 덮어쓰기", "R5~R14 자동 복원",
    "threshold 변경", "bias 계산법 변경",
    "표본 추정을 전체로 확대", "유료 API·데이터", "env/token 변경",
    "production write", "실주문", "외부발송", "deploy",
]


def build_targets():
    rows = json.loads((RD / "r18-full-reconciliation-latest.json")
                      .read_text(encoding="utf-8"))["rows"]
    cc = json.loads((ROOT / "_cache" / "dart-corp-codes.json")
                    .read_text(encoding="utf-8"))
    out = []
    for r in rows:
        if r.get("wealthReason") != "NO_DIRECT_MATCH":
            continue
        t = r["ticker"]
        out.append({
            "ticker": t,
            "corpCode": (cc.get(t) or {}).get("corp_code"),
            "corpName": (cc.get(t) or {}).get("corp_name"),
            "date": r["date"],
            "shareRatio": r["shareRatio"],
            "shareCountDelta": r["shareRatio"] - 1.0,
            "priceRatio": r.get("priceRatio"),
            "mcapRatio": r.get("mcapRatio"),
            "r16Class": "SUSPECTED_RIGHTS_ISSUE",
            "r17Label": r.get("label"), "r17Confidence": r.get("confidence"),
            "r18Label": r.get("r18Label"), "r18WealthLayer": r.get("wealthLayer"),
            "currentWealthStatus": "WEALTH_UNRESOLVED",
            "currentWealthReason": r.get("wealthReason"),
            "receiptClue": r.get("rcept_no"),
            "expectedBiasContribution": "P(권리)×조건부크기 — R18 방식",
        })
    return out


def main() -> int:
    tg = build_targets()
    mapped = [t for t in tg if t["corpCode"]]
    payload = {
        "task": "R19",
        "taskId": "WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19",
        "writtenBeforeDirectSourceResults": True,
        "inherits": {
            "from": "R18",
            "verdict": "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS",
            "unresolvedTickerPct": 13.31, "expectedBiasPct": 3.47,
            "biasThresholdPct": 2.0, "unresolvedThresholdPct": 20.0,
            "onlyFailingCondition": "expectedBias",
        },
        "coreQuestion": CORE_QUESTION, "coreDistinction": CORE_DISTINCTION,
        "targetDefinition": "R18 wealthReason == NO_DIRECT_MATCH",
        "targetTotal": len(tg),
        "targetTickers": len({t["ticker"] for t in tg}),
        "corpCodeMapped": len(mapped),
        "scopeGuard": "342건만. 새 universe 확장 금지(§2).",
        "eventTypes": EVENT_TYPES,
        "noEntitlementTypes": NO_ENTITLEMENT_TYPES,
        "entitlementTypes": ENTITLEMENT_TYPES,
        "directionalTypes": DIRECTIONAL_TYPES,
        "mergerDirection": MERGER_DIRECTION,
        "cannotExplainShareIncrease": CANNOT_EXPLAIN_SHARE_INCREASE,
    "mechanicalTypes": MECHANICAL_TYPES, "mechanicalWhy": MECHANICAL_WHY,
    "selfCorrections": [
        {"id": "R19-SC1", "found": "분류 결과 육안 검수",
         "what": "자기주식·감자가 주식수 증가 사건의 PRIMARY 로 60건 선정됐다.",
         "why": "그 행위들은 발행주식총수를 늘릴 수 없다. 구조적으로 불가능하다.",
         "fix": "설명력 없는 유형을 PRIMARY 에서 배제하고 secondary 로만 남긴다.",
         "direction": "NOT_REQUIRED 건수를 줄인다 = 판정이 어려워지는 방향"},
        {"id": "R19-SC2", "found": "분류 결과 육안 검수",
         "what": "'주식분할결정'(액면분할)이 '회사분할' 패턴에 걸려 SPINOFF 로 분류됐다.",
         "why": "주식분할은 기존 주주 보유주식이 그대로 쪼개지는 것 = 직접 권리 YES.",
         "fix": "STOCK_SPLIT 유형 신설 + 회사분할보다 먼저 매칭.",
         "direction": "REQUIRED 건수를 늘린다 = 판정이 어려워지는 방향"},
    ],
    "window": WINDOW, "confidence": CONFIDENCE, "bias": BIAS,
        "verdict": VERDICT, "forbidden": FORBIDDEN,
        "targets": tg,
    }
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / "r19-targets-precommit-latest.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"saved": str(p), "targets": len(tg),
                      "tickers": len({t["ticker"] for t in tg}),
                      "corpCodeMapped": len(mapped),
                      "biasThresholdPct": 2.0, "thresholdChanged": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
