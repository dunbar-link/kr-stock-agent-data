#!/usr/bin/env python3
"""R23 증권신고서 배정표 파서 — 구주주 배정량·eligible shares·자기주식.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

증권신고서 본문에는 배정비율의 **산출 근거**가 통째로 실려 있다. 실측 형태:

    ※ 구주주 1주당 신주배정비율 산출 근거
      모집주식총수(11,000,000주) - 우리사주조합 우선배정분(2,200,000주)
      = ------------------------------------------------------------
      기발행보통주식수(32,531,794주) - 자기주식(2,902,135주)
      = 0.2969997056 주

여기서 §5~§8 이 요구하는 값이 전부 나온다:
  구주주 배정주식수 = 모집주식총수 − 우리사주 우선배정분
  eligible old shares = 기발행보통주식수 − 자기주식   ← §6·§7 자기주식 제외
  1주당 신주배정주식수 = 위 비율                        ← §12
  보통주 기준                                           ← §8

문서는 오래된 DART XML(대문자 TD/TABLE, `&cr;` 줄바꿈)과 HTML 이 섞여 있다(§4).
없는 값은 추정하지 않는다(§36).

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import re

PARSER_VERSION = "r23-parse-5"

TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"[ \t ]+")
NUM = r"([\d,]+(?:\.\d+)?)"


def flatten(txt: str) -> str:
    """DART 구양식의 `&cr;` 과 태그를 걷어 평문으로 만든다."""
    t = txt.replace("&cr;", "\n").replace("&nbsp;", " ").replace("&#13;", "\n")
    t = re.sub(r"</(?:td|th|tr|p)>", "\n", t, flags=re.I)
    t = TAG.sub(" ", t)
    return "\n".join(WS.sub(" ", ln).strip() for ln in t.split("\n"))


def _num(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("", "-"):
        return None
    return float(s) if re.fullmatch(r"\d+(\.\d+)?", s) else None


def _first(flat, patterns):
    for p in patterns:
        m = re.search(p, flat)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return v
    return None


# ── 배정비율 산출근거 (§5 grade C·D) ─────────────────────────────
# ★ 자체수정 1 (§4·§31): 신고서 양식이 최소 세 가지다. 하나만 지원하면 1/11 만
#   읽힌다(실측). 실제로 관측된 형태:
#     A 산식형  모집주식총수(N주) - 우리사주조합 우선배정분(N주)
#               / 기발행보통주식수(N주) - 자기주식(N주)
#     B 항목형  A. 보통주식수 N / B. 우선주식수 N / C. 발행주식총수 N
#               / E. 자기주식+자기주식신탁 N
#     C 배정표형 주 주 배 정 N주(95.00%) / 우리사주배정 N주(5.00%)
#               ▶ 구주 1주당 신주 배정비율:0.626553330주당
P_OFFER_TOTAL = [r"모집주식\s*총수\s*\(\s*" + NUM + r"\s*주",
                 r"모집\s*주식수\s*\(\s*" + NUM + r"\s*주",
                 r"합\s*계\s*" + NUM + r"\s*주\s*\(\s*100",
                 r"모집주식\s*총수[^\d]{0,12}" + NUM]
P_ESOP = [r"우리사주조합\s*우선\s*배정분?\s*\(\s*" + NUM + r"\s*주",
          r"우리사주\s*배정\s*" + NUM + r"\s*주",
          r"우리사주조합\s*배정\s*\(?\s*" + NUM + r"\s*주",
          r"우리사주조합에\s*우선\s*배정[^\d]{0,20}" + NUM + r"\s*주"]
# §8 — 보통주만 쓴다. '우선주식수' 는 분모에 넣지 않는다.
P_ISSUED_COMMON = [r"기발행\s*보통주식수\s*\(\s*" + NUM + r"\s*주",
                   r"A\.\s*보통주식수\s*" + NUM,
                   r"보통주식수\s*[:：]?\s*" + NUM,
                   r"발행\s*보통주식\s*총수\s*\(\s*" + NUM + r"\s*주"]
P_PREFERRED = [r"B\.\s*우선주식수\s*" + NUM,
               r"우선주식수\s*[:：]?\s*" + NUM]
P_TOTAL_ISSUED = [r"C\.\s*발행주식총수\s*" + NUM,
                  r"발행주식\s*총수\s*[:：]?\s*" + NUM]
P_TREASURY = [r"자기주식\s*\+\s*자기주식신탁\s*" + NUM,
              r"E\.\s*자기주식[^\d]{0,20}" + NUM,
              r"자기주식\s*\(\s*" + NUM + r"\s*주",
              r"자기주식수?\s*[:：]\s*" + NUM + r"\s*주"]
P_RATIO = [r"구주주?\s*1주당\s*신주\s*배정\s*비율\s*[:：]?\s*" + NUM,
           r"1주당\s*신주배정비율인?\s*" + NUM + r"\s*주",
           r"소유주식\s*1주당\s*" + NUM + r"\s*주의?\s*비율로\s*배정",
           # ★ 자체수정 6 (§31): 이 gap 이 원래 [^\d]{0,40} 이었는데, 라벨이
           #   "구주주 1주당 신주배정비율 **산출 근거**" 라는 제목줄이면 그 뒤
           #   첫 숫자인 **모집주식총수**(11,000,000)를 비율로 집었다. 회귀
           #   테스트가 잡았다. gap 에서 근거·총수·괄호를 배제하고 좁힌다.
           r"구주주\s*1주당\s*신주배정\s*비율(?![^\d]{0,40}(?:근거|총수|\())"
           r"[^\d]{0,20}" + NUM,
           r"1주당\s*신주배정\s*주식수\s*[:：]?\s*" + NUM,
           r"1주당\s*신주\s*배정비율\s*[:：]?\s*" + NUM,
           # 산식형 배정근거의 **계산 결과** 줄: "= 0.2969997056 주".
           # 소수만 받는다 — 산식 안의 "= 기발행보통주식수(32,531,794주)" 같은
           # 정수 수량을 비율로 오인하지 않기 위해서다.
           r"=\s*(\d*\.\d+)\s*주"]
P_SH_ALLOC = [r"주\s*주\s*배\s*정\s*" + NUM + r"\s*주",
              r"주주배정\s*" + NUM + r"\s*주",
              r"구주주\s*배정\s*주식수\s*[:：]?\s*" + NUM,
              r"구주주에게\s*배정[^\d]{0,20}" + NUM + r"\s*주"]
P_OFFER_AMT = [r"모집\s*또는\s*매출\s*금액\s*[:：]?\s*" + NUM + r"\s*원",
               r"모집총액\s*[:：]?\s*" + NUM + r"\s*원"]
P_OFFER_SHARES_HDR = [
    r"모집\s*또는\s*매출\s*증권의\s*종류\s*[:：]?[^\d]{0,20}" + NUM + r"\s*주"]
P_PRICE = [r"확정\s*발행가액\s*[:：]?\s*" + NUM,
           r"최종\s*발행가액\s*[:：]?\s*" + NUM]


# ★ 자체수정 2 (§31): 신고서 한 건에 유상증자와 **무상증자**가 함께 실린다.
#   문서 전체에서 '1주당 신주배정비율' 을 찾으면 무상증자의 1.00000 을 집는다
#   (실측: statedRatio 가 1.0 으로 나온 4건). 또 '보통주식수' 도 수권주식수나
#   다른 회차 표에서 잘못 잡혀 eligible 이 관측치의 44배가 되기도 했다.
#   → 구주주 배정 산출근거 **블록 안에서만** 찾는다. 블록을 못 찾으면 값을
#     비운다(추정 금지, §36).
BONUS = re.compile(r"무상증자")
SCOPE_START = re.compile(
    r"(구주주\s*1주당\s*(?:신주)?\s*배정\s*비율\s*산출\s*근거"
    r"|구주주\s*1주당\s*신주배정비율"
    r"|소유주식\s*1주당"
    r"|1주당\s*신주배정비율인"
    r"|주\s*주\s*배\s*정)")


def rights_scopes(flat, width=1400):
    """구주주 배정 근거가 실린 구간들을 앞에서부터 돌려준다.

    ★ 자체수정 3 (§31): 초판은 앞 250자·구간 앞 200자에 '무상증자' 가 하나라도
      있으면 구간을 버렸다. 그 배제가 너무 넓어 정상 건까지 떨어뜨렸다(실측:
      4건 복원 → 2건으로 감소). 배제 조건을 좁힌다 — 바로 앞 120자에서
      **무상증자가 유상증자·구주주보다 가까울 때만** 무상 구간으로 본다.
      그리고 한 구간만 쓰지 않고 후보를 순서대로 시도한다.
    """
    out = []
    for m in SCOPE_START.finditer(flat):
        # ★ 자체수정 7 (§31): 스코프 자체가 '구주주' 로 시작하면 유상 배정
        #   근거가 확실하다. 그런데 바로 앞 문단이 무상증자 안내면 SC3 의
        #   문맥검사가 그 확실한 스코프까지 버렸다(회귀 테스트가 잡았다).
        #   문맥검사는 라벨만으로는 유·무상을 구분할 수 없는 스코프에만 쓴다.
        if not m.group(1).startswith("구주주"):
            head = flat[max(0, m.start() - 120):m.start()]
            bi = head.rfind("무상증자")
            pi = max(head.rfind("유상증자"), head.rfind("구주주"))
            if bi >= 0 and bi > pi:
                continue                  # 무상증자 설명 안에 있는 비율이다
        out.append(flat[m.start():m.start() + width])
    return out


def parse(txt):
    """신고서 원문 → 배정 관련 수량. 없는 값은 None 으로 둔다."""
    f = flatten(txt)
    out = {"parserVersion": PARSER_VERSION, "flags": []}

    scopes = rights_scopes(f)
    out["hasRightsScope"] = bool(scopes)
    out["rightsScopeCount"] = len(scopes)
    if not scopes:
        out["flags"].append("NO_RIGHTS_SCOPE")
    # 비율이 잡히는 첫 구간을 쓴다. 없으면 첫 구간을 그대로 쓴다.
    sc = ""
    for cand in scopes:
        if _first(cand, P_RATIO) is not None or _first(cand, P_ISSUED_COMMON):
            sc = cand
            break
    if not sc and scopes:
        sc = scopes[0]

    # 문서 전체에서 찾아도 안전한 것(모집 규모·금액·가격)
    out["offerTotalShares"] = _first(f, P_OFFER_TOTAL)
    out["esopShares"] = _first(f, P_ESOP)
    out["offerAmount"] = _first(f, P_OFFER_AMT)
    out["offerSharesHeader"] = _first(f, P_OFFER_SHARES_HDR)
    out["confirmedPrice"] = _first(f, P_PRICE)

    # 구주주 배정 근거 블록 안에서만 찾아야 하는 것(§31 자체수정 2)
    out["issuedCommonShares"] = _first(sc, P_ISSUED_COMMON)
    out["treasuryShares"] = _first(sc, P_TREASURY)
    # ★ 자체수정 6 (§31): 구주주 1주당 배정비율은 현실적으로 0 < r <= 10 이다.
    #   그보다 크면 라벨 뒤에서 다른 수량(모집주식총수 등)을 집은 것이다.
    #   비율로 쓰지 않고 비운다 — 추정으로 채우지 않는다(§36).
    _r = _first(sc, P_RATIO)
    if _r is not None and not (0.0 < _r <= 10.0):
        out["flags"].append("RATIO_IMPLAUSIBLE_DISCARDED")
        out["discardedRatio"] = _r
        _r = None
    out["ratioPerOldShare"] = _r
    out["shareholderAllocatedDirect"] = _first(sc, P_SH_ALLOC)
    out["preferredShares"] = _first(sc, P_PREFERRED)
    out["totalIssuedShares"] = _first(sc, P_TOTAL_ISSUED)

    # §6·§7·§8 — eligible old shares 는 **보통주** 기발행 − 자기주식
    ic, tr = out["issuedCommonShares"], out["treasuryShares"]
    if ic is None and out["totalIssuedShares"] and out["preferredShares"] is not None:
        # 항목형에서 보통주식수가 안 잡히면 총발행 − 우선주로 복원한다.
        ic = out["totalIssuedShares"] - out["preferredShares"]
        out["issuedCommonShares"] = ic
        out["flags"].append("COMMON_DERIVED_FROM_TOTAL_MINUS_PREFERRED")
    if ic:
        out["eligibleOldShares"] = ic - (tr or 0.0)
        out["treasuryExcluded"] = tr is not None
        out["eligibleSource"] = ("ISSUED_COMMON_MINUS_TREASURY" if tr
                                 else "ISSUED_COMMON_ONLY")
        if tr is None:
            out["flags"].append("TREASURY_NOT_STATED")
    else:
        out["eligibleOldShares"] = None
        out["treasuryExcluded"] = False
        out["eligibleSource"] = None

    # §5 — 구주주 배정주식수 = 모집총수 − 우리사주
    if out["shareholderAllocatedDirect"]:
        out["shareholderAllocatedShares"] = out["shareholderAllocatedDirect"]
        out["allocationSource"] = "DIRECT_STATED"
    elif out["offerTotalShares"] is not None:
        out["shareholderAllocatedShares"] = (out["offerTotalShares"]
                                             - (out["esopShares"] or 0.0))
        out["allocationSource"] = ("OFFER_MINUS_ESOP" if out["esopShares"]
                                   else "OFFER_TOTAL")
    else:
        out["shareholderAllocatedShares"] = None
        out["allocationSource"] = None

    # 확정발행가 — 모집금액/모집주식수 파생 가능
    if out["confirmedPrice"] is None and out["offerAmount"]:
        base = out["offerSharesHeader"] or out["offerTotalShares"]
        if base:
            out["confirmedPrice"] = out["offerAmount"] / base
            out["priceSource"] = "OFFER_AMOUNT_DIV_SHARES"
    elif out["confirmedPrice"] is not None:
        out["priceSource"] = "STATED"
    else:
        out["priceSource"] = None

    # §13 — 배정비율 정합성: 배정주식수 / eligible ≈ 명시 비율
    a, e, r = (out["shareholderAllocatedShares"], out["eligibleOldShares"],
               out["ratioPerOldShare"])
    if a and e and e > 0:
        out["derivedRatio"] = a / e
        if r:
            out["ratioRelErr"] = abs(out["derivedRatio"] - r) / r
            if out["ratioRelErr"] > 0.15:
                out["flags"].append("RATIO_DATA_CONFLICT")
    else:
        out["derivedRatio"] = None

    # §5 grade D — 배정량이 없어도 (비율 × eligible) 로 재현 가능하면 인정한다.
    if (out["shareholderAllocatedShares"] is None
            and out["ratioPerOldShare"] and out["eligibleOldShares"]):
        out["shareholderAllocatedShares"] = (out["ratioPerOldShare"]
                                             * out["eligibleOldShares"])
        out["allocationSource"] = "RATIO_TIMES_ELIGIBLE"

    out["hasAllocationEvidence"] = bool(
        out["shareholderAllocatedShares"] and out["eligibleOldShares"])
    # 비율만 있어도 wealth 계산에는 충분하다(1주 보유자 관점).
    out["hasRatioEvidence"] = bool(out["ratioPerOldShare"])
    return out
