#!/usr/bin/env python3
"""R22 확장 공시 파서 — 거래소 자율공시(HTML 자유서술) + 신주상장.

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22

★ 자체수정 (§31, 실측으로 발견): 증권발행실적보고서는 대문자 태그 + ACODE
  필드코드를 쓰지만, **거래소 자율공시(청약결과·증권발행결과·발행가액확정·
  추가상장)는 소문자 HTML** 이고 ACODE 가 없다. R20 파서의 정규식은
  `<(TD|TE|TU)` 로 대소문자를 구분해 이 문서들을 전부 빈 문서로 읽었다.
  대소문자 무시 + 자유서술 라벨 추출을 추가한다.

이 문서들이 담고 있는 것(§8·§9 가 요구하는 값):
  청약결과      모집주식수 · 청약주식수 · 단수주/실권주식수
  발행가액확정  확정발행가액 · 1차/2차 발행가액
  증권발행결과  실제 발행주식수 · 발행가액 · 납입일
  추가상장      추가주식수 · 추가상장후 총발행주식수 · 상장일

없는 값은 추정하지 않는다(§5).

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import re

PARSER_VERSION = "r22-parse-4"

# ★ 대소문자 무시 — 자율공시는 소문자 HTML 이다.
CELL = re.compile(r"<(td|te|tu)\b[^>]*>(.*?)</\1>", re.S | re.I)
STYLE = re.compile(r"<style.*?</style>", re.S | re.I)
BR = re.compile(r"<br[^>]*>", re.I)
# 셀·행 종료 태그도 줄바꿈으로 본다.
# ★ 자체수정 4 (§31): 초판 flatten 은 **소스 파일의 개행**에 의존해
#   셀을 나눴다. 공백 없이 이어붙인 HTML 이면 라벨과 값이 한 줄로
#   합쳐져 추출이 실패한다(unit test 가 검출).
CELLEND = re.compile(r"</(?:td|th|tr)>", re.I)
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")

NUM = r"([\d,]+(?:\.\d+)?)"


def flatten(txt: str) -> str:
    """STYLE 을 걷어내고 <br> 를 줄바꿈으로 바꾼 뒤 평문화한다."""
    t = STYLE.sub(" ", txt)
    t = BR.sub("\n", t)
    t = CELLEND.sub("\n", t)
    t = TAG.sub(" ", t).replace("&nbsp;", " ")
    return "\n".join(WS.sub(" ", ln).strip() for ln in t.split("\n"))


def _num(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("", "-"):
        return None
    return float(s) if re.fullmatch(r"\d+(\.\d+)?", s) else None


def _find(flat, patterns):
    """라벨 뒤의 첫 숫자. 여러 패턴을 순서대로 시도한다."""
    for p in patterns:
        m = re.search(p, flat)
        if m:
            v = _num(m.group(1))
            if v is not None:
                return v
    return None


def _date(flat, patterns):
    for p in patterns:
        m = re.search(p, flat)
        if m:
            s = m.group(1)
            d = re.search(r"(\d{4})\D*(\d{1,2})\D*(\d{1,2})", s)
            if d:
                return f"{d.group(1)}{int(d.group(2)):02d}{int(d.group(3)):02d}"
    return None


def lines(txt):
    return [ln for ln in flatten(txt).split("\n") if ln.strip()]


# 문서의 절 번호 줄. 값이 아니라 목차다.
HEADING = re.compile(r"^\s*(?:\d+|[가-힣])\s*[.．]\s*\S")


def _cellnum(lns, labels, lookahead=3):
    """★ 자체수정 2 (§31): 거래소 자율공시는 표라서 라벨과 값이 다른 줄에 있다.

    초판은 라벨 뒤 첫 숫자를 그대로 값으로 썼는데, 그 다음 줄이 '1. 발행예정내역'
    같은 **절 번호**면 1 을 발행가로 읽었다(실측: 확정발행가 1·2·4 원).
    절 번호 줄은 건너뛴다.
    """
    # ★ 자체수정 5 (§31): 초판은 **줄 순서**로 순회해서, 문서 제목
    #   ('유상증자 최종발행가액 확정')이 먼저 매치되면 그 뒤의 엉뚱한 숫자를
    #   집었다(실측: 확정발행가 대신 발행예정 주식수 20,000,000). labels 는
    #   구체적인 것부터 정렬돼 있으므로 **라벨 우선**으로 순회한다.
    for lab in labels:
        for i, ln in enumerate(lns):
            if not re.search(lab, ln):
                continue
            for j in range(i + 1, min(i + 1 + lookahead, len(lns))):
                if HEADING.match(lns[j]):
                    continue
                m = re.search(NUM, lns[j])
                if m:
                    v = _num(m.group(1))
                    if v is not None:
                        return v
    return None


def _celldate(lns, labels, lookahead=3):
    for i, ln in enumerate(lns):
        for lab in labels:
            if re.search(lab, ln):
                for j in range(i, min(i + 1 + lookahead, len(lns))):
                    d = re.search(r"(\d{4})\D{1,3}(\d{1,2})\D{1,3}(\d{1,2})",
                                  lns[j])
                    if d:
                        return (f"{d.group(1)}{int(d.group(2)):02d}"
                                f"{int(d.group(3)):02d}")
    return None


def _celltext(lns, labels, lookahead=2):
    for i, ln in enumerate(lns):
        for lab in labels:
            if re.search(lab, ln):
                for j in range(i + 1, min(i + 1 + lookahead, len(lns))):
                    if lns[j].strip():
                        return lns[j].strip()
    return None

def parse_subscription_result(txt):
    """청약결과 자율공시 — 모집·청약·실권 주식수(§8)."""
    f = flatten(txt)
    out = {"parserVersion": PARSER_VERSION, "kind": "SUBSCRIPTION_RESULT",
           "flags": []}
    ls = lines(txt)
    out["offeredShares"] = (_find(f, [r"모집주식수\s*[:：]?\s*" + NUM,
                                      r"발행주식수\s*[:：]?\s*" + NUM,
                                      r"배정주식수\s*[:：]?\s*" + NUM])
                            or _cellnum(ls, [r"모집주식수", r"배정주식수"]))
    out["subscribedShares"] = (_find(f, [r"청약주식수\s*[:：]?\s*" + NUM,
                                         r"총\s*청약주식수\s*[:：]?\s*" + NUM])
                               or _cellnum(ls, [r"청약주식수"]))
    out["unsubscribedShares"] = _find(
        f, [r"실권주식수\s*[:：]?\s*" + NUM,
            r"단수주\s*및\s*실권주식수\s*[:：]?\s*" + NUM,
            r"실권주\s*[:：]?\s*" + NUM])
    out["subscriptionRatePct"] = _find(
        f, [r"청약주식수[^%\n]*?\(\s*" + NUM + r"\s*%"])
    out["isShareholderAllocation"] = bool(
        re.search(r"구주주\s*청약|주주배정", f))
    if out["offeredShares"] and out["subscribedShares"] is not None:
        out["takeUpRate"] = out["subscribedShares"] / out["offeredShares"]
        if out["subscribedShares"] < out["offeredShares"] * 0.999:
            out["flags"].append("PARTIAL_SUBSCRIPTION")
    return out


def parse_final_price(txt):
    """발행가액확정 공시 — 확정발행가(§7).

    실측 형식: '가. 확정가액(원)' 다음 줄에 '500원'. 라벨과 값이 다른 줄이다.
    '안내공시' 는 1차 발행가일 수 있으므로 확정가로 승격하지 않는다.
    """
    f = flatten(txt)
    ls = lines(txt)
    out = {"parserVersion": PARSER_VERSION, "kind": "FINAL_PRICE", "flags": []}
    out["finalIssuePrice"] = _cellnum(
        ls, [r"확정가액", r"확정\s*발행가액", r"최종\s*발행가액"])
    out["firstPrice"] = _cellnum(ls, [r"1\s*차\s*발행가"])
    out["secondPrice"] = _cellnum(ls, [r"2\s*차\s*발행가"])
    out["perSharePrice"] = _cellnum(ls, [r"주당\s*발행가액", r"보통주식"])
    out["plannedSharesInNotice"] = _cellnum(ls, [r"주식수\s*\(주\)"])
    out["parValue"] = _cellnum(ls, [r"액면가"])
    out["discountRatePct"] = _find(f, [r"할인율\s*[:：]?\s*" + NUM])
    out["stage"] = _celltext(ls, [r"^1\.\s*구분"])
    out["confirmedDate"] = _celldate(ls, [r"확정일"])
    if out["finalIssuePrice"] is None and out["secondPrice"]:
        out["finalIssuePrice"] = out["secondPrice"]
        out["flags"].append("USED_SECOND_PRICE_AS_FINAL")
    if out["finalIssuePrice"] is None and out.get("perSharePrice"):
        out["noticePrice"] = out["perSharePrice"]
        out["flags"].append("NOTICE_PRICE_NOT_FINAL")
    return out


def parse_issue_completion(txt):
    """증권발행결과 자율공시 — 실제 발행주식수·발행방법(§3 rank 7).

    실측 형식: '실제발행주식수(주)' 다음 줄에 값. '2. 발행방법' 다음 줄에
    '주주배정 유상증자' 같은 방식이 온다. 사채 발행결과도 같은 양식이므로
    증권 종류로 걸러야 한다.
    """
    ls = lines(txt)
    out = {"parserVersion": PARSER_VERSION, "kind": "ISSUE_COMPLETION",
           "flags": []}
    out["issuedShares"] = _cellnum(ls, [r"실제발행주식수"])
    out["plannedShares"] = _cellnum(ls, [r"발행예정주식수"])
    out["issuedAmount"] = _cellnum(ls, [r"실제발행금액"])
    out["plannedAmount"] = _cellnum(ls, [r"발행예정금액"])
    out["paymentDate"] = _celldate(ls, [r"납입일"])
    out["boardDate"] = _celldate(ls, [r"발행결정\s*최초\s*이사회"])
    out["securityType"] = _celltext(ls, [r"^1\.\s*증권의\s*종류"])
    out["issueMethod"] = _celltext(ls, [r"^2\.\s*발행방법"])
    meth = out["issueMethod"] or ""
    out["isRights"] = bool(re.search(r"유상증자", meth))
    out["isShareholderAllocation"] = bool(re.search(r"주주배정|구주주", meth))
    out["isThirdParty"] = bool(re.search(r"제\s*3\s*자\s*배정", meth))
    # 사채 발행결과는 주식 발행이 아니다. 혼동 방지.
    out["isBondNotEquity"] = bool(
        re.search(r"사채", (out["securityType"] or "") + meth))
    if out["issuedShares"] and out["issuedAmount"]:
        out["derivedIssuePrice"] = out["issuedAmount"] / out["issuedShares"]
    if out["plannedShares"] and out["issuedShares"] is not None:
        if out["issuedShares"] < out["plannedShares"] * 0.999:
            out["flags"].append("PARTIAL_ISSUANCE")
    return out


def parse_new_listing(txt):
    """추가상장 공시 — 실제 상장된 신주 수(§9)."""
    f = flatten(txt)
    ls = lines(txt)
    out = {"parserVersion": PARSER_VERSION, "kind": "NEW_LISTING", "flags": []}
    out["addedShares"] = _cellnum(ls, [r"추가주식수", r"추가\s*상장\s*주식수"],
                                  lookahead=6)
    out["totalSharesAfter"] = _cellnum(ls, [r"추가상장후\s*총발행주식수"],
                                       lookahead=6)
    out["parValue"] = _cellnum(ls, [r"1주당\s*액면가"])
    out["listingDate"] = _celldate(ls, [r"상장일"])
    out["reason"] = ("RIGHTS" if re.search(r"유상증자", f)
                     else ("BONUS" if re.search(r"무상증자", f) else None))
    return out


PARSERS = {"SUBSCRIPTION_RESULT": parse_subscription_result,
           "FINAL_PRICE": parse_final_price,
           "ISSUE_COMPLETION": parse_issue_completion,
           "NEW_LISTING": parse_new_listing}


def parse(kind, txt):
    fn = PARSERS.get(kind)
    return fn(txt) if fn else None
