#!/usr/bin/env python3
"""R24 사건 유형 직접판정 + 기존주주 권리 판정.

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

R19 분류기를 뼈대로 재사용하되(§3), 이 50건 모집단에서 실제로 나온 공시유형을
덮도록 최소 확장한다. 확장한 것만 적는다:
  · 주식배당      — 기존주주가 신주를 직접 받는다. R19 규칙에 없었다.
  · 우선주 전환   — 전환우선주·전환상환우선주의 보통주 전환청구.
  · 합병/분할 신고서·실적보고서·종료보고서 — R19 는 '결정' 공시만 봤다.
  · 유상증자 결과계열(발행결과·청약결과·발행가액) — 방식이 제목에 없다.
  · 주식매수선택권 **부여** 는 사건이 아니다(행사와 구분).

그리고 §6 의 핵심 요구 — 제목만으로 기계판정하지 않기 — 를 위해 유상증자·
무상증자·주식배당·분할은 **본문(document.xml)** 을 열어 증자방식·배정비율·
신주수를 확인한다. 본문 파서는 R18 것을 그대로 쓴다.

안전: 계산·읽기 전용. 네트워크 0(캐시만 읽는다). production write 0.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C16  # noqa: E402
import r18_parser as P18  # noqa: E402
import r23_parser as P23  # noqa: E402
from r16_precommit import CLASSIFIER as R16_CLASSIFIER  # noqa: E402
from r24_targets import (CANNOT_EXPLAIN_SHARE_INCREASE, CONFIDENCE,  # noqa: E402
                         EVENT_TYPES, MECHANICAL_TYPES)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
LIST_CACHE = ROOT / "_cache" / "dart-r24-lists"
DOC_CACHE = ROOT / "_cache" / "dart-documents"

CLASSIFIER_VERSION = "r24-classify-7"
TOL = CONFIDENCE["ratioTolerance"]

# 사건이 아닌 공시. 매칭 전에 먼저 버린다.
NOT_AN_EVENT = re.compile(
    r"주식매수선택권\s*부여"          # 부여는 신주 발행이 아니다
    r"|가액의?\s*조정"                # 전환가액 조정 안내
    r"|매매거래\s*정지"
    r"|특수관계인의\s*유상증자\s*참여"  # 참여 사실일 뿐 발행조건이 아니다
    r"|신탁계약")                     # 자기주식 신탁

# 모회사가 아니라 종속·자회사 사건.
SUBSIDIARY = re.compile(r"종속회사|자회사의|피출자회사")

# (유형, 제목패턴, 창) — 순서가 의미를 가진다. 구체적인 것이 먼저다.
RULES = [
    ("STOCK_OPTION_EXERCISE", r"주식매수선택권\s*행사", (-3, 3)),
    ("CB_CONVERSION", r"전환청구권\s*행사|전환권\s*행사", (-3, 3)),
    ("BW_EXERCISE", r"신주인수권\s*행사|신주인수권증권\s*행사", (-3, 3)),
    ("PREFERRED_CONVERSION",
     r"(?:전환우선주|전환상환우선주|상환전환우선주|우선주)[^)]{0,12}보통주[^)]{0,12}전환"
     r"|보통주\s*자동전환", (-3, 3)),
    ("SHARE_EXCHANGE", r"주식교환|주식이전", (-9, 3)),
    ("STOCK_SPLIT", r"주식분할|액면분할", (-9, 3)),
    ("REVERSE_SPLIT", r"주식병합|액면병합", (-9, 3)),
    ("COMPANY_SPLIT",
     r"분할합병|회사분할|증권신고서\(분할\)|분할종료보고서|분할\s*결정", (-12, 6)),
    ("MERGER_NEW_SHARES",
     r"회사합병|합병\s*결정|소규모합병|간이합병|증권신고서\(합병\)"
     r"|증권발행실적보고서\(합병등\)|합병등종료보고서", (-12, 6)),
    ("STOCK_DIVIDEND", r"주식배당", (-6, 3)),
    ("BONUS_ISSUE", r"무상증자", (-9, 3)),
    ("CAPITAL_REDUCTION", r"감자\s*결정|자본감소", (-9, 3)),
    ("TREASURY_SHARE_CANCELLATION", r"자기주식\s*소각", (-9, 3)),
    ("TREASURY_ACTION_OTHER", r"자기주식", (-9, 3)),
    # 유상증자 계열 — 방식은 제목에 없다. 본문에서 확정한다.
    ("RIGHTS_FAMILY", r"유상증자\s*결정|증권신고서\(지분증권\)"
                      r"|증권발행실적보고서(?!\(합병)", (-12, 3)),
    ("RIGHTS_FAMILY_OUTCOME",
     r"유상증자또는주식관련사채등의(?:발행결과|청약결과)"
     r"|증권발행결과|유상증자\s*신주\s*발행가액|발행가액\s*확정", (-12, 6)),
    # 사채 발행결정은 전환·행사 자체가 아니다. 약한 근거로만.
    ("CB_CONVERSION", r"전환사채권?\s*발행", (-18, 0)),
    ("BW_EXERCISE", r"신주인수권부사채권?\s*발행", (-18, 0)),
]
WEAK_PATTERNS = (r"전환사채권?\s*발행", r"신주인수권부사채권?\s*발행",
                 r"증권발행결과", r"발행가액")

# 유상증자 방식 → §5 유형
METHOD_TO_KIND = {
    "SHAREHOLDER_RIGHTS": "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS",
    "RIGHTS_THEN_PUBLIC": "RIGHTS_THEN_PUBLIC_UNSUBSCRIBED",
    "PUBLIC_OFFERING": "PUBLIC_OFFERING",
    "THIRD_PARTY": "THIRD_PARTY_ALLOCATION",
}

# ★ §7·§8 — 주식수를 늘릴 수 없는 행위는 PRIMARY 가 될 수 없다.
#   자기주식 처분은 유통주식만 늘리고 **발행주식총수**는 늘리지 않는다.
#   이 연구의 shares 계열은 발행주식수이므로 증가를 설명할 수 없다(§11).
CANNOT_INCREASE = set(CANNOT_EXPLAIN_SHARE_INCREASE) | {"TREASURY_ACTION_OTHER"}
# 감소를 설명할 수 있는 행위
CAN_DECREASE = {"CAPITAL_REDUCTION", "REVERSE_SPLIT",
                "TREASURY_SHARE_CANCELLATION", "PREFERRED_CONVERSION"}

# ★ 자체수정 6 (§31): 우선주 전환은 **방향이 종목마다 반대**다. 초판은
#   PREFERRED_CONVERSION 을 무조건 '증가를 설명 못 함' 으로 넣었는데, 그건
#   우선주 라인 기준이다. 보통주 라인에서는 전환우선주가 보통주로 바뀌면
#   보통주 발행주식수가 **늘어난다**. 그래서 실제로 전환청구 공시가 창 안에
#   있는 보통주 사건 4건이 근거를 눈앞에 두고 UNRESOLVED 로 떨어졌다.
#   종목의 주식 종류에 따라 증가/감소 어느 쪽을 설명하는지 나눈다.
def can_explain(kind, increased, pref):
    if kind == "PREFERRED_CONVERSION":
        # 보통주 라인이면 증가를, 우선주 라인이면 감소를 설명한다.
        return (not pref) if increased else pref
    return kind not in CANNOT_INCREASE if increased else kind in CAN_DECREASE

# ★ 주식 종류별 적용 가능성 (§8 우선주 혼입 금지의 반대 방향 적용).
#   우선주 라인의 주식수는 보통주에게 발행한 신주로 늘지 않는다.
COMMON_ONLY_ISSUANCE = {
    "CB_CONVERSION", "BW_EXERCISE", "STOCK_OPTION_EXERCISE",
    "MERGER_NEW_SHARES", "SHARE_EXCHANGE", "PUBLIC_OFFERING",
    "THIRD_PARTY_ALLOCATION", "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS",
    "RIGHTS_THEN_PUBLIC_UNSUBSCRIBED", "RIGHTS_FAMILY",
    "RIGHTS_FAMILY_OUTCOME"}
ALL_CLASS_ACTIONS = {"STOCK_SPLIT", "REVERSE_SPLIT", "BONUS_ISSUE",
                     "STOCK_DIVIDEND", "CAPITAL_REDUCTION", "COMPANY_SPLIT"}

# ★ 자체수정 5 (§31): 분할·무상증자·주식배당은 **납입이 없다**. 그래서 주가가
#   주식수에 반비례해 움직인다 — R16 이 이미 고정한 비례성 검정이다. 초판은
#   본문 배정비율만 대조해서, 본문을 못 읽은 액면분할 6건이 근거 없이 MEDIUM
#   에 머물렀다. R16 의 로그상대오차와 **사전 고정된 허용치**를 그대로 쓴다.
#   (§16 주의: marketCap = close × shares 라 독립근거가 아니다. 여기서 쓰는 것은
#    marketCap 이 아니라 price 와 shares 라는 **서로 다른 두 관측 계열**이다.)
NO_PAYMENT_TYPES = {"STOCK_SPLIT", "REVERSE_SPLIT", "BONUS_ISSUE", "STOCK_DIVIDEND"}
R16_TOL = R16_CLASSIFIER["proportionalTolerance"]


def price_corroboration(kind, share_ratio, price_ratio):
    """무납입 사건의 가격-주식수 비례성. R16 규칙·허용치를 그대로 재사용한다."""
    if kind not in NO_PAYMENT_TYPES or not share_ratio or not price_ratio:
        return None
    return C16.rel_err(share_ratio, price_ratio)


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def _shares_in(nm):
    m = re.search(r"([\d,]{4,})\s*주", nm)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def classify_filing(nm):
    """공시명 → (유형, 약한근거, 창). 사건이 아니면 (None, False, None)."""
    if NOT_AN_EVENT.search(nm) or SUBSIDIARY.search(nm):
        return None, False, None
    for kind, pat, win in RULES:
        if re.search(pat, nm):
            weak = any(re.search(w, nm) for w in WEAK_PATTERNS)
            return kind, weak, win
    return None, False, None


def is_amendment(nm):
    return bool(re.match(r"^\s*\[?\s*(?:기재정정|첨부정정|정정)", nm))


# ── 본문 확인 (§4 제목만 보고 판정 금지) ──────────────────────────────
RATIO_PAT = [r"신주\s*배정\s*비율[^\d]{0,20}([\d.]+)",
             r"1주당\s*신주배정\s*주식수[^\d]{0,20}([\d.]+)",
             r"주식배당\s*비율[^\d]{0,20}([\d.]+)",
             r"분할\s*비율[^\d]{0,20}([\d.]+)"]
NEWSH_PAT = [r"신주의?\s*종류와\s*수[^\d]{0,40}([\d,]{4,})",
             r"발행\s*주식\s*수[^\d]{0,20}([\d,]{4,})\s*주",
             r"배정\s*주식수[^\d]{0,20}([\d,]{4,})\s*주"]
# ★ 자체수정 8 (§31): 주식분할·병합 공시 본문은 '전/후' 라는 단어를 값 옆에
#   두지 않는다. 표 머리글이 "분할 전 | 분할 후" 이고 각 행은 라벨 뒤에 값이
#   **두 개 연달아** 온다:
#       1주당 액면가액 (원)  5,000   2,500
#       발행주식총수 보통주(주)  4,520,338   9,040,676
#                    우선주(주)     86,331     172,662
#   초판의 '전/후' 패턴은 하나도 매칭되지 않았고, 그래서 액면분할 7건이 전부
#   수량근거 없이 MEDIUM 에 머물렀다. 라벨 뒤 연속 두 값을 읽는다.
#   우선주 행이 있으면 **그 종류의 분할 전/후 주식수**를 직접 얻는다 — 이
#   연구가 필요한 바로 그 값이다(§8).
SPLIT_ROWS = {
    "par": r"1\s*주당\s*액면\s*가(?:액)?",
    "common": r"보\s*통\s*주\s*\(?\s*주",
    "preferred": r"우\s*선\s*주\s*\(?\s*주",
}
PAIR = r"[^\d]{0,40}([\d,]{1,15})[^\d]{1,40}([\d,]{1,15})"


def _pair_after(flat, label):
    m = re.search(label + PAIR, flat)
    if not m:
        return None, None
    try:
        a, b = (float(x.replace(",", "")) for x in m.groups())
    except ValueError:
        return None, None
    return (a, b) if a > 0 and b > 0 else (None, None)


def _first(flat, pats):
    for p in pats:
        m = re.search(p, flat)
        if m:
            try:
                v = float(m.group(1).replace(",", ""))
            except ValueError:
                continue
            if v > 0:
                return v
    return None


def read_doc(rcept_no):
    p = DOC_CACHE / f"{rcept_no}.txt"
    if not p.exists():
        return None
    return p.read_text(encoding="utf-8", errors="replace")


def inspect(kind, rcept_no):
    """본문에서 방식·비율·신주수를 읽는다. 없으면 빈 dict."""
    txt = read_doc(rcept_no)
    if not txt:
        return {}
    out = {"docRead": True}
    if kind in ("RIGHTS_FAMILY", "RIGHTS_FAMILY_OUTCOME"):
        d = P18.parse(txt)
        out.update({"issueMethod": d.get("issueMethod"),
                    "issueMethodPath": d.get("evidencePath"),
                    "newSharesCommon": d.get("newSharesCommon"),
                    "newSharesPreferred": d.get("newSharesPreferred"),
                    "allocPerShare": d.get("allocPerShare"),
                    "issuePrice": d.get("issuePriceCommon"),
                    "recordDate": d.get("recordDate"),
                    "thirdPartyTarget": d.get("thirdPartyTarget"),
                    "hasShareholderOnlyFields": d.get("hasShareholderOnlyFields"),
                    "hasThirdPartyOnlyFields": d.get("hasThirdPartyOnlyFields")})
        if not out.get("issueMethod"):
            r = P23.parse(txt)
            if r.get("ratioPerOldShare") or r.get("derivedRatio"):
                out["registrationRatio"] = (r.get("ratioPerOldShare")
                                            or r.get("derivedRatio"))
    else:
        flat = P23.flatten(txt)
        out["bodyRatio"] = _first(flat, RATIO_PAT)
        out["bodyNewShares"] = _first(flat, NEWSH_PAT)
        if kind in ("STOCK_SPLIT", "REVERSE_SPLIT"):
            pb, pa = _pair_after(flat, SPLIT_ROWS["par"])
            cb, ca = _pair_after(flat, SPLIT_ROWS["common"])
            fb, fa = _pair_after(flat, SPLIT_ROWS["preferred"])
            out.update({"parBefore": pb, "parAfter": pa,
                        "splitCommonBefore": cb, "splitCommonAfter": ca,
                        "splitPreferredBefore": fb, "splitPreferredAfter": fa})
            if pb and pa:
                # 분할비율 = 액면가 비. 관측 shareRatio 와 직접 비교한다.
                out["parImpliedShareRatio"] = pb / pa
            if cb and ca:
                out["commonSplitRatio"] = ca / cb
            if fb and fa:
                out["preferredSplitRatio"] = fa / fb
    return out


# ── 사건 판정 ─────────────────────────────────────────────────────────
def entitlement_of(kind, ev, doc):
    spec = EVENT_TYPES.get(kind) or {}
    e = spec.get("entitlement")
    if e is True:
        return "YES", spec.get("why", ""), "DIRECT_EVENT_TYPE"
    if e is False:
        return "NO", spec.get("why", ""), "DIRECT_EVENT_TYPE"
    if kind in ("MERGER_NEW_SHARES", "SHARE_EXCHANGE", "COMPANY_SPLIT"):
        # §7 방향성. 이 표본은 주식수가 **증가**한 사건이다. 신주를 발행한 쪽이
        # 분석대상이고, 신주는 상대회사 주주에게 간다. R19 논리 그대로 재사용.
        if (ev.get("shareRatio") or 1.0) > 1.0:
            return "NO", ("주식수 증가 = 분석대상이 신주를 발행한 쪽(존속·완전모회사). "
                          "신주는 상대회사 주주에게 간다. 분석대상 주주가 대가를 "
                          "받는 경우는 주식수 증가가 아니라 상장폐지로 나타난다."), \
                "MERGER_DIRECTION"
        return "UNKNOWN", "주식수가 늘지 않았다. 방향성을 확정할 수 없다.", "UNRESOLVED"
    if kind == "CAPITAL_REDUCTION":
        return "UNKNOWN", "유상감자(현금)와 무상감자를 공시명만으로 구분할 수 없다(§11).", \
            "UNRESOLVED"
    if kind in ("RIGHTS_FAMILY", "RIGHTS_FAMILY_OUTCOME"):
        m = (doc or {}).get("issueMethod")
        k2 = METHOD_TO_KIND.get(m)
        if k2:
            s2 = EVENT_TYPES[k2]
            return ("YES" if s2["entitlement"] else "NO"), s2["why"], \
                f"DOC_ISSUE_METHOD:{m}"
        return "UNKNOWN", "유상증자는 맞지만 본문에서 증자방식을 확정하지 못했다.", \
            "METHOD_UNKNOWN"
    return "UNKNOWN", spec.get("why", ""), "UNRESOLVED"


def classify_event(ev, filings):
    em = _mon(ev["date"])
    sr = ev.get("shareRatio") or 1.0
    increased = sr > 1.0
    obs_new = sr - 1.0
    pref = ev.get("shareClass") != "COMMON"

    cands, dropped_class = [], 0
    for f in filings:
        nm = f.get("report_nm") or ""
        fd = f.get("rcept_dt")
        if not fd:
            continue
        kind, weak, win = classify_filing(nm)
        if not kind:
            continue
        lo, hi = win
        gap = _mon(fd) - em
        if not (lo <= gap <= hi):
            continue
        doc = inspect(kind, f.get("rcept_no"))
        # 유상증자 계열은 본문 방식으로 유형을 확정한다.
        eff = METHOD_TO_KIND.get((doc or {}).get("issueMethod"), kind)
        # ★ 주식 종류 적용성 (§8): 우선주 라인의 주식수는 보통주에게 발행한
        #   신주로 늘지 않는다. 후보에서 제외하되 개수를 기록한다.
        # ★ 자체수정 3 (§31): 초판은 우선주 라인 사건에서 보통주 대상 발행을
        #   **무조건 버렸다**. 그런데 본문을 못 읽은 후보까지 버려서, 근거가
        #   없다는 이유로 후보 자체가 사라졌다(실측 98건 탈락, 삼양홀딩스
        #   우선주는 19건 전부 탈락해 UNRESOLVED). 본문에서 우선주 신주가
        #   0 임을 **확인한 경우에만** 버리고, 못 읽었으면 남기되 불확실로
        #   표시해 HIGH 가 되지 못하게 한다.
        class_uncertain = False
        if pref and increased and eff in COMMON_ONLY_ISSUANCE:
            d = doc or {}
            if d.get("docRead"):
                if not d.get("newSharesPreferred"):
                    dropped_class += 1
                    continue
            else:
                class_uncertain = True
        sh = (_shares_in(nm) or (doc or {}).get("bodyNewShares")
              or (doc or {}).get("newSharesPreferred" if pref else "newSharesCommon"))
        rel = None
        if sh and ev.get("sharesBefore") and obs_new:
            rel = abs(sh / ev["sharesBefore"] - obs_new) / abs(obs_new)
        bodyr = (doc or {}).get("bodyRatio") or (doc or {}).get("allocPerShare") \
            or (doc or {}).get("registrationRatio")
        relr = None
        if bodyr and obs_new:
            relr = abs(bodyr - obs_new) / abs(obs_new)
        # 액면분할·병합은 배정비율이 없다. **그 종목의 주식 종류** 분할 전/후
        # 주식수가 있으면 그것을, 없으면 액면가 비를 쓴다.
        d0 = doc or {}
        # ★ 자체수정 9 (§31): 비율에는 **두 가지 의미**가 섞여 있다.
        #   분할 표의 값은 배수(after/before), 무상증자·주식배당·유상증자의
        #   값은 구주 1주당 신주수다. 정합성 대조가 이 둘을 같게 다뤄서
        #   무상증자 1.00 을 배수 1.00 으로 읽고 4건이 전부 불일치로 나왔다.
        #   어느 의미인지 함께 들고 다닌다.
        sem = "PER_OLD_SHARE"
        par = (d0.get("preferredSplitRatio") if pref
               else d0.get("commonSplitRatio")) or d0.get("parImpliedShareRatio")
        if par and sr:
            pr = abs(par - sr) / sr
            if relr is None or pr < relr:
                relr, bodyr, sem = pr, par, "MULTIPLIER"
        prop = price_corroboration(eff, sr, ev.get("priceRatio"))
        cands.append({"kind": eff, "ruleKind": kind, "weak": weak,
                      "rcept_no": f.get("rcept_no"), "filingDate": fd,
                      "reportName": nm, "gapMonths": gap,
                      "isAmendment": is_amendment(nm),
                      "sharesEvidence": sh, "ratioRelErr": rel,
                      "bodyRatio": bodyr, "bodyRatioRelErr": relr,
                      "bodyRatioSemantics": sem if bodyr else None,
                      "priceProportionRelErr": prop,
                      "priceProportional": prop is not None and prop < R16_TOL,
                      "classUncertain": class_uncertain, "doc": doc or {}})

    base = {"classifierVersion": CLASSIFIER_VERSION,
            "candidates": len(cands), "droppedByShareClass": dropped_class,
            "secondaryEvents": sorted({c["kind"] for c in cands})}
    if not cands:
        why = ("창 안에서 자본행위 공시를 찾지 못했다."
               if not dropped_class else
               "창 안 공시가 전부 보통주 대상 발행이어서 이 우선주 라인의 "
               "주식수 증가를 설명할 수 없다.")
        return {**base, "eventIdentity": "UNRESOLVED", "primaryEvent": "UNKNOWN",
                "entitlement": "UNKNOWN", "entitlementWhy": why,
                "entitlementBasis": "UNRESOLVED",
                "wealthAdjustment": "UNKNOWN", "confidence": "UNKNOWN",
                "provenance": "NO_DIRECT_MATCH"}

    explains = [c for c in cands if can_explain(c["kind"], increased, pref)]
    if not increased and not explains:
        explains = list(cands)
    if not explains:
        return {**base, "eventIdentity": "UNRESOLVED", "primaryEvent": "UNKNOWN",
                "entitlement": "UNKNOWN",
                "entitlementWhy": ("창 안 공시가 전부 주식수를 늘릴 수 없는 "
                                   "행위(자기주식·감자·우선주전환)뿐이다."),
                "entitlementBasis": "UNRESOLVED",
                "wealthAdjustment": "UNKNOWN", "confidence": "UNKNOWN",
                "provenance": "NO_EXPLANATORY_FILING"}

    def rank(c):
        r = c["bodyRatioRelErr"] if c["bodyRatioRelErr"] is not None \
            else c["ratioRelErr"]
        return (0 if ((r is not None and r <= TOL) or c["priceProportional"])
                else 1,
                0 if not c["weak"] else 1,
                abs(c["gapMonths"]),
                1e9 if r is None else r)

    explains.sort(key=rank)
    primary = explains[0]
    ent, why, basis = entitlement_of(primary["kind"], ev, primary["doc"])

    corroborated = ((primary["bodyRatioRelErr"] is not None
                     and primary["bodyRatioRelErr"] <= TOL)
                    or (primary["ratioRelErr"] is not None
                        and primary["ratioRelErr"] <= TOL)
                    or primary["priceProportional"])
    if primary["weak"]:
        conf = "LOW"
    elif primary.get("classUncertain"):
        conf = "MEDIUM"          # 우선주 라인 적용 여부 미확인 — HIGH 금지
    elif corroborated and (primary["doc"].get("docRead")
                           or primary["priceProportional"]):
        conf = "HIGH"
    elif primary["doc"].get("docRead") or basis.startswith("DOC_"):
        conf = "MEDIUM"
    else:
        conf = "LOW"                 # 제목만 — §14 최종확정 불가

    identity = "CONFIRMED" if conf in ("HIGH", "MEDIUM") else "PARTIAL"
    mech = primary["kind"] in MECHANICAL_TYPES

    # ★ 자체수정 7 (§31·§14): 같은 창에 서로 **권리 판정이 반대인** 후보가 함께
    #   있는데 수량 근거가 없으면, 가장 가까운 공시가 이기는 규칙이 사실상
    #   동전던지기가 된다(실측: 무상증자 YES 와 우선주전환 NO 가 한 달 차이로
    #   경합해 판정이 뒤집혔다). 수량으로 확정되지 않은 경합은 UNKNOWN 으로
    #   남긴다. 어느 쪽으로도 유리하게 고르지 않는다(§17).
    rival = None
    if not corroborated:
        for c in explains:
            if c is primary or c["kind"] == primary["kind"]:
                continue
            e2, _w2, _b2 = entitlement_of(c["kind"], ev, c["doc"])
            if e2 in ("YES", "NO") and ent in ("YES", "NO") and e2 != ent:
                rival = c
                break
    if rival is not None:
        return {**base, "eventIdentity": "AMBIGUOUS",
                "primaryEvent": primary["kind"],
                "rivalEvent": rival["kind"],
                "rivalRceptNo": rival["rcept_no"],
                "rivalReportName": rival["reportName"],
                "entitlement": "UNKNOWN",
                "entitlementWhy": (f"{primary['kind']} 와 {rival['kind']} 가 같은 창에 "
                                   "있고 권리 판정이 서로 반대다. 수량 근거가 없어 "
                                   "어느 쪽인지 확정할 수 없다(§14·§17)."),
                "entitlementBasis": "ENTITLEMENT_CONFLICT",
                "wealthAdjustment": "UNKNOWN", "confidence": "LOW",
                "provenance": "AMBIGUOUS_MULTIPLE_ACTIONS",
                "primaryRceptNo": primary["rcept_no"],
                "primaryReportName": primary["reportName"],
                "primaryFilingDate": primary["filingDate"],
                "primaryGapMonths": primary["gapMonths"],
                "docRead": bool(primary["doc"].get("docRead"))}

    if conf == "LOW":
        ent, basis = "UNKNOWN", "LOW_CONFIDENCE_NOT_USABLE"
        why = "제목 근거뿐이다. §14 에 따라 최종 직접확정으로 쓰지 않는다."
    if ent == "NO":
        adj = "NOT_REQUIRED"
    elif ent == "YES":
        adj = "REQUIRED_MECHANICAL" if mech else "REQUIRED"
    else:
        adj = "UNKNOWN"

    return {**base,
            "eventIdentity": identity, "primaryEvent": primary["kind"],
            "primaryRuleKind": primary["ruleKind"],
            "secondaryEvents": [k for k in base["secondaryEvents"]
                                if k != primary["kind"]],
            "entitlement": ent, "entitlementWhy": why, "entitlementBasis": basis,
            "wealthAdjustment": adj, "confidence": conf, "mechanical": mech,
            "provenance": "DIRECT_DART" if not primary["weak"] else "DERIVED_MATCH",
            "primaryRceptNo": primary["rcept_no"],
            "primaryFilingDate": primary["filingDate"],
            "primaryReportName": primary["reportName"],
            "primaryGapMonths": primary["gapMonths"],
            "sharesEvidence": primary["sharesEvidence"],
            "ratioRelErr": primary["ratioRelErr"],
            "bodyRatio": primary["bodyRatio"],
            "bodyRatioRelErr": primary["bodyRatioRelErr"],
            "bodyRatioSemantics": primary["bodyRatioSemantics"],
            "priceProportionRelErr": primary["priceProportionRelErr"],
            "priceProportional": primary["priceProportional"],
            "issueMethod": primary["doc"].get("issueMethod"),
            "issueMethodPath": primary["doc"].get("issueMethodPath"),
            "issuePrice": primary["doc"].get("issuePrice"),
            "allocPerShare": primary["doc"].get("allocPerShare"),
            "docRead": bool(primary["doc"].get("docRead")),
            "classUncertain": bool(primary.get("classUncertain")),
            "parBefore": primary["doc"].get("parBefore"),
            "parAfter": primary["doc"].get("parAfter"),
            "amendments": [c["rcept_no"] for c in cands
                           if c["isAmendment"] and c["kind"] == primary["kind"]]}


def load_lists():
    out = {}
    for p in LIST_CACHE.glob("*.json"):
        out[p.stem] = json.loads(p.read_text(encoding="utf-8"))
    return out


def main() -> int:
    tg = json.loads((RD / "r24-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    lists = load_lists()
    res, chains = [], []
    for t in tg:
        rec = lists.get(t["corpCode"]) or {}
        r = classify_event(t, rec.get("filings") or [])
        res.append({**{k: t[k] for k in
                       ("ticker", "date", "companyName", "shareClass",
                        "shareRatio", "sharesBefore", "sharesAfter",
                        "corpCodeSource")}, **r})
        if r.get("amendments"):
            chains.append({"ticker": t["ticker"], "date": t["date"],
                           "primaryEvent": r["primaryEvent"],
                           "initial": r.get("primaryRceptNo"),
                           "amendments": r["amendments"],
                           "finalUsed": r.get("primaryRceptNo"),
                           "changedOutcome": None,
                           "note": ("정정본이 존재한다. 분류에는 창 안 최우선 "
                                    "근거를 썼고 정정 목록을 함께 보존한다(§15).")})

    def cnt(key):
        d = {}
        for r in res:
            d[r.get(key)] = d.get(r.get(key), 0) + 1
        return dict(sorted(d.items(), key=lambda x: -x[1]))

    out = {"task": "R24", "classifierVersion": CLASSIFIER_VERSION,
           "targets": len(res),
           "byPrimaryEvent": cnt("primaryEvent"),
           "byEntitlement": cnt("entitlement"),
           "byConfidence": cnt("confidence"),
           "byIdentity": cnt("eventIdentity"),
           "byProvenance": cnt("provenance"),
           "byEntitlementBasis": cnt("entitlementBasis"),
           "docsRead": sum(1 for r in res if r.get("docRead")),
           "droppedByShareClass": sum(r.get("droppedByShareClass", 0) for r in res),
           "eventsWithNoCandidate": sum(1 for r in res if not r["candidates"]),
           "titleOnlyDowngraded": sum(
               1 for r in res if r.get("entitlementBasis") == "LOW_CONFIDENCE_NOT_USABLE"),
           "events": res}
    (RD / "r24-event-classification-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    ent = {"task": "R24", "rule": ("사건명만으로 기계판정하지 않는다. 유상증자 "
                                   "계열은 본문 증자방식(CI_MTH)으로 확정한다(§6)."),
           "byEntitlement": cnt("entitlement"),
           "byBasis": cnt("entitlementBasis"),
           "events": [{k: r.get(k) for k in
                       ("ticker", "date", "companyName", "shareClass",
                        "primaryEvent", "entitlement", "entitlementWhy",
                        "entitlementBasis", "confidence", "issueMethod",
                        "primaryRceptNo", "primaryReportName")} for r in res]}
    (RD / "r24-shareholder-entitlement-latest.json").write_text(
        json.dumps(ent, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    (RD / "r24-correction-chains-latest.json").write_text(
        json.dumps({"task": "R24", "chains": chains, "count": len(chains),
                    "rule": ("정정공시가 있으면 최종 조건을 쓴다. initial·"
                             "correction·final 을 보존한다(§15).")},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({k: out[k] for k in
                      ("targets", "byPrimaryEvent", "byEntitlement",
                       "byConfidence", "docsRead", "droppedByShareClass",
                       "eventsWithNoCandidate")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
