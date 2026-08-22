#!/usr/bin/env python3
"""R20 증권발행실적보고서 파서 — 계획치가 아닌 **실제 발행 결과**.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

실적보고서 표는 배정대상(DST_CD)별로 아래 필드를 준다:
  FST_DV_CNT   최초 배정주식수   ← 기존 주주의 **권리(entitlement)**
  FST_DV_RT    최초 배정비율(%)
  SB_ST_CNT    청약 주식수       ← 실제 청약분
  SB_AMT       청약 금액
  DV_ST_CNT    배정 주식수       ← 실제 배정(발행)분
  DV_AMT       배정 금액         ← 확정발행가 = DV_AMT / DV_ST_CNT
  SB_BGN_DT / SB_END_DT / PYM_DT  청약기간·납입일

배정대상 구분(DST_CD) 예: '구주주', '우리사주조합', '기타 제3자 배정', '일반공모'.

★ 기존 주주의 권리는 '구주주' 행이다. 우리사주조합·제3자·일반공모 행은
  기존 보통주 주주의 권리가 아니다(R17~R19 정본). 반드시 분리한다(§12·§13).

없는 값은 추정하지 않는다(§4). PLANNED 와 FINAL_ACTUAL 을 섞지 않는다(§5).

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import re

PARSER_VERSION = "r20-parse-2"

CELL = re.compile(r"<(TD|TE|TU)\b([^>]*)>(.*?)</\1>", re.S)
CODE = re.compile(r'A(?:CODE|UNIT)="([^"]+)"')
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")

# 기존 보통주 주주 = '구주주'. 표기 변형을 흡수한다.
SHAREHOLDER = re.compile(r"구주주|기존주주|주주배정")
ESOP = re.compile(r"우리사주")
THIRD = re.compile(r"제\s*3\s*자|제삼자")
PUBLIC = re.compile(r"일반공모|일반청약|공모|청약자")


def _txt(x):
    return WS.sub(" ", TAG.sub("", x).replace("&nbsp;", " ")).strip()


def _num(v):
    """'5,308,124' → 5308124.0 · '-'/빈값 → None. 추정하지 않는다."""
    if v is None:
        return None
    s = str(v).replace(",", "").replace("원", "").replace("주", "").strip()
    if s in ("", "-", "–", "—"):
        return None
    return float(s) if re.fullmatch(r"-?\d+(\.\d+)?", s) else None


def _date(v):
    if not v:
        return None
    m = re.search(r"(\d{4})\D*(\d{1,2})\D*(\d{1,2})", str(v))
    return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}" if m else None


def group_of(dst):
    """배정대상 문자열 → 그룹. 순서가 중요하다(제3자·우리사주가 먼저)."""
    if not dst:
        return None
    if ESOP.search(dst):
        return "ESOP"
    if THIRD.search(dst):
        return "THIRD_PARTY"
    if SHAREHOLDER.search(dst):
        return "SHAREHOLDER"
    if PUBLIC.search(dst):
        return "PUBLIC"
    return "OTHER"


# ★ 자체수정 1 (§30): 초판은 '일반청약' 라벨을 PUBLIC 으로 잡지 못해 OTHER 로
#   흘렸다. 실적보고서 배정표의 실제 표기를 실측해 보완했다
#   (관측 라벨: 구주주 · 우리사주조합 · 기타 제3자 배정 · 일반청약).


def parse(txt):
    """실적보고서 원문 → 배정대상별 실제 발행 결과."""
    cells = []
    for m in CELL.finditer(txt):
        c = CODE.search(m.group(2))
        cells.append((c.group(1) if c else None, _txt(m.group(3))))

    out = {"parserVersion": PARSER_VERSION, "groups": {}, "flags": [],
           "subscriptionStart": None, "subscriptionEnd": None,
           "paymentDate": None}

    # 일정 — 문서 상단에 한 번 나온다.
    for code, val in cells:
        if code == "SB_BGN_DT" and not out["subscriptionStart"]:
            out["subscriptionStart"] = _date(val)
        elif code == "SB_END_DT" and not out["subscriptionEnd"]:
            out["subscriptionEnd"] = _date(val)
        elif code == "PYM_DT" and not out["paymentDate"]:
            out["paymentDate"] = _date(val)

    # 배정대상별 블록 — DST_CD 를 만나면 새 그룹을 열고 이후 필드를 채운다.
    cur = None
    for code, val in cells:
        if code == "DST_CD":
            g = group_of(val)
            cur = {"rawLabel": val, "group": g}
            out["groups"].setdefault(g or "OTHER", []).append(cur)
            continue
        if cur is None or not code:
            continue
        if code in ("FST_DV_CNT", "FST_DV_RT", "SB_CNT", "SB_ST_CNT",
                    "SB_AMT", "SB_RT", "DV_CNT", "DV_ST_CNT", "DV_AMT",
                    "DV_RT"):
            cur.setdefault(code, _num(val))

    if not out["groups"]:
        out["flags"].append("NO_ALLOCATION_TABLE")
        return out

    def agg(gname, field):
        rows = out["groups"].get(gname) or []
        vals = [r.get(field) for r in rows if r.get(field) is not None]
        return sum(vals) if vals else None

    sh_alloc = agg("SHAREHOLDER", "FST_DV_CNT")     # 기존 주주 권리(배정)
    sh_sub = agg("SHAREHOLDER", "SB_ST_CNT")        # 실제 청약
    sh_dv = agg("SHAREHOLDER", "DV_ST_CNT")         # 실제 배정(발행)
    sh_amt = agg("SHAREHOLDER", "DV_AMT")
    total_dv = sum(v for g in out["groups"]
                   for r in out["groups"][g]
                   if (v := r.get("DV_ST_CNT")) is not None) or None

    out.update({
        "shareholderEntitledShares": sh_alloc,
        "shareholderSubscribedShares": sh_sub,
        "shareholderAllocatedShares": sh_dv,
        "shareholderPaidAmount": sh_amt,
        "esopAllocatedShares": agg("ESOP", "DV_ST_CNT"),
        "thirdPartyAllocatedShares": agg("THIRD_PARTY", "DV_ST_CNT"),
        "publicAllocatedShares": agg("PUBLIC", "DV_ST_CNT"),
        "finalNewSharesIssued": total_dv,
        "hasShareholderGroup": "SHAREHOLDER" in out["groups"],
    })

    # 확정발행가 — 구주주 배정금액 / 배정주식수 가 가장 직접적이다.
    price = None
    src = "MISSING"
    if sh_amt and sh_dv:
        price, src = sh_amt / sh_dv, "SHAREHOLDER_DV_AMT_DIV_SHARES"
    else:
        t_amt = sum(v for g in out["groups"] for r in out["groups"][g]
                    if (v := r.get("DV_AMT")) is not None) or None
        if t_amt and total_dv:
            price, src = t_amt / total_dv, "TOTAL_DV_AMT_DIV_SHARES"
    out["finalIssuePrice"] = price
    out["finalIssuePriceSource"] = src

    # 실권 규모 — 권리 대비 실제 청약
    if sh_alloc and sh_dv is not None:
        out["shareholderTakeUpRate"] = sh_dv / sh_alloc
        if sh_dv < sh_alloc * 0.999:
            out["flags"].append("PARTIAL_SUBSCRIPTION")
    return out
