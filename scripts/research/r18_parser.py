#!/usr/bin/env python3
"""R18 DART 공시원문 파서 — 구조 우선(§7).

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

DART 원문의 표 셀에는 ACODE/AUNIT 기계판독 필드코드가 있다. 2007~2023 표본에서
핵심 코드가 안정적으로 출현했다. 따라서:

  1순위  ACODE/AUNIT 필드코드 (label-value 관계가 명시적)  → HIGH 가능
  2순위  표 라벨 텍스트('5. 증자방식')와 인접 값             → HIGH 가능
  3순위  본문 keyword 추론                                   → LOW 고정

전체문서 regex 한 번으로 결정하지 않는다(§7). 없는 필드는 추측하지 않는다(§5).

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r18_targets import CONSISTENCY, METHOD_RULES  # noqa: E402

PARSER_VERSION = "r18-parse-4"
TOL = CONSISTENCY["tolerance"]

CELL = re.compile(r"<(TD|TE|TU)\b([^>]*)>(.*?)</\1>", re.S)
CODE = re.compile(r'A(?:CODE|UNIT)="([^"]+)"')
UVAL = re.compile(r'AUNITVALUE="([^"]*)"')
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")

# 라벨 텍스트 → 필드 (2순위 경로. 필드코드가 없는 구식 문서 대비)
LABEL_FIELDS = [
    ("CI_MTH", ("증자방식",)),
    ("NEW_ASN_CNT", ("1주당 신주배정주식수", "주당신주배정주식수")),
    ("ALL_BS_DT", ("신주배정기준일",)),
    ("CST_ISS_VAL", ("신주 발행가액", "신주발행가액")),
    ("FVAL", ("1주당 액면가액", "주당액면가액")),
]

# 본문 keyword (3순위. LOW 고정)
KW = [(t, p) for t, p in METHOD_RULES]


def _txt(x):
    return WS.sub(" ", TAG.sub("", x).replace("&nbsp;", " ")).strip()


def _num(v):
    """'10,000,000' → 10000000.0 · '-'/빈값 → None. 추측하지 않는다."""
    if v is None:
        return None
    s = str(v).replace(",", "").replace("원", "").replace("주", "").strip()
    if s in ("", "-", "–", "—"):
        return None
    m = re.match(r"^-?\d+(\.\d+)?$", s)
    return float(s) if m else None


def _date(v):
    """'2006년 12월 15일' 또는 '20061215' → 'YYYYMMDD'."""
    if not v:
        return None
    s = str(v)
    m = re.search(r"(\d{4})\D*(\d{1,2})\D*(\d{1,2})", s)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    m = re.fullmatch(r"\s*(\d{8})\s*", s)
    return m.group(1) if m else None


def classify_method(raw):
    """CI_MTH 원문 문자열 → 유형. 긴 문구 우선(§6 오분류 방지)."""
    if not raw:
        return None
    s = str(raw).replace(" ", "")
    for kind, pats in METHOD_RULES:
        for p in pats:
            if p.replace(" ", "") in s:
                return kind
    return None


def extract_cells(txt):
    """(필드코드 → 값) 과 (라벨, 값) 순열을 함께 뽑는다."""
    fields, seq = {}, []
    for m in CELL.finditer(txt):
        attr, body = m.group(2), m.group(3)
        val = _txt(body)
        c = CODE.search(attr)
        if c:
            uv = UVAL.search(attr)
            # 같은 코드가 반복되면 첫 값을 쓴다(정정표 하단 중복 방지).
            fields.setdefault(c.group(1), {"text": val,
                                           "unitValue": uv.group(1) if uv else None})
        seq.append(val)
    return fields, seq


def by_label(seq):
    """필드코드가 없을 때: 라벨 셀 바로 뒤의 비어있지 않은 셀을 값으로 본다."""
    out = {}
    for i, v in enumerate(seq):
        for field, labels in LABEL_FIELDS:
            if field in out:
                continue
            if any(lb in v for lb in labels):
                for j in range(i + 1, min(i + 5, len(seq))):
                    if seq[j] and not any(lb in seq[j] for lb, in ()) and seq[j] != v:
                        out[field] = seq[j]
                        break
    return out


def parse(txt, observed_new_per_old=None):
    """원문 1건 → 정규화 event. 근거 경로와 confidence 를 함께 남긴다."""
    fields, seq = extract_cells(txt)
    lab = by_label(seq)
    ev = {"parserVersion": PARSER_VERSION,
          "fieldCodesFound": len(fields), "evidencePath": None,
          "flags": []}

    # ── 증자방식 ──────────────────────────────────────────────────
    raw = path = None
    if "CI_MTH" in fields:
        raw, path = fields["CI_MTH"]["text"], "FIELD_CODE"
        ev["ciMthUnitValue"] = fields["CI_MTH"]["unitValue"]
    elif "CI_MTH" in lab:
        raw, path = lab["CI_MTH"], "TABLE_LABEL"
    method = classify_method(raw)
    if method is None:
        # 3순위 — 본문 keyword. LOW 로만 인정한다.
        flat = _txt(txt)
        for kind, pats in KW:
            if any(p in flat.replace(" ", "") for p in
                   [x.replace(" ", "") for x in pats]):
                method, raw, path = kind, f"KEYWORD:{pats[0]}", "BODY_KEYWORD"
                break
    ev["issueMethodRaw"] = raw
    ev["issueMethod"] = method
    ev["evidencePath"] = path

    # ── 수량·가격·일정 ────────────────────────────────────────────
    g = (lambda c: fields.get(c, {}).get("text"))
    new_c = _num(g("CST_CNT"))
    new_p = _num(g("PST_CNT"))
    bef_c = _num(g("BFR_CST_CNT"))
    bef_p = _num(g("BFR_PST_CNT"))
    ev.update({
        "newSharesCommon": new_c, "newSharesPreferred": new_p,
        "sharesBeforeCommon": bef_c, "sharesBeforePreferred": bef_p,
        "parValue": _num(g("FVAL")),
        "issuePriceCommon": _num(g("CST_ISS_VAL")) or _num(lab.get("CST_ISS_VAL")),
        "issuePricePreferred": _num(g("PST_ISS_VAL")),
        "discountRate": _num(g("DC_RATE")),
        "allocPerShare": _num(g("NEW_ASN_CNT")) or _num(lab.get("NEW_ASN_CNT")),
        "boardDate": _date(g("DRC_DT")),
        "recordDate": _date(g("ALL_BS_DT")) or _date(lab.get("ALL_BS_DT")),
        "subscriptionStart": _date(g("SH_BGN_DT")),
        "subscriptionEnd": _date(g("SH_END_DT")),
        "paymentDate": _date(g("PYM_DT")),
        "listingDate": _date(g("LST_PLN_DT")),
        "thirdPartyTarget": g("PART"), "thirdPartyRelation": g("RLT"),
    })
    funds = [_num(g(c)) for c in ("FND_USE1", "FND_USE2", "FND_USE3", "ANC_ACQ_AMT")]
    ev["totalProceeds"] = sum(x for x in funds if x) if any(funds) else None

    # 구조적 교차검증 — 주주배정 계열에만 있는 필드 / 제3자에만 있는 필드
    ev["hasShareholderOnlyFields"] = any(
        c in fields for c in ("ALL_BS_DT", "NEW_ASN_CNT", "SH_BGN_DT"))
    ev["hasThirdPartyOnlyFields"] = any(c in fields for c in ("PART", "RLT"))

    # ── 비율 ──────────────────────────────────────────────────────
    ratio = None
    if new_c and bef_c:
        ratio = new_c / bef_c
    ev["rightsRatio"] = ratio
    ev["rightsRatioSource"] = "CST_CNT/BFR_CST_CNT" if ratio else None

    # 발행가 파생 (직접값이 없을 때만)
    if not ev["issuePriceCommon"] and ev["totalProceeds"] and new_c:
        ev["issuePriceCommon"] = ev["totalProceeds"] / new_c
        ev["issuePriceProvenance"] = "DERIVED_FROM_PROCEEDS_DIV_SHARES"
    elif ev["issuePriceCommon"]:
        ev["issuePriceProvenance"] = "DIRECT_FIELD"
    else:
        ev["issuePriceProvenance"] = "MISSING"

    # ── 숫자 정합성 (§14) ─────────────────────────────────────────
    checks = {}
    if ratio and ev["allocPerShare"]:
        checks["allocVsRatio"] = abs(ev["allocPerShare"] - ratio) / ratio
    if ratio and observed_new_per_old and observed_new_per_old > 0:
        checks["observedVsRatio"] = abs(ratio - observed_new_per_old) / observed_new_per_old
    ev["consistency"] = {k: round(v, 4) for k, v in checks.items()}
    conflict = any(v > TOL for v in checks.values())
    if conflict:
        ev["flags"].append("DATA_CONFLICT")

    # ── confidence (§8) ───────────────────────────────────────────
    # ★ 자체수정(§33, 공개 기록): 초안은 숫자 불일치를 LOW 로 떨어뜨렸다. 그건
    #   §8 의 정의와 어긋난다 — LOW 는 'keyword inference 위주'를 위한 등급이다.
    #   CI_MTH 는 구조 필드에 **명시된 직접 사실**이고, 수량 불일치는 방식 판정을
    #   무너뜨리지 않는다(공시된 신주수는 계획치라 실권·부분청약으로 실제 발행량과
    #   갈리는 일이 흔하다). 그래서 불일치는 MEDIUM + DATA_CONFLICT 로 둔다.
    #   대신 **wealth 확정 여부**는 reconcile 단계에서 따로 판단한다 — 주주배정
    #   계열은 비율을 믿을 수 없으면 wealth 미해결이고, 제3자·일반공모는 애초에
    #   비율이 필요 없으므로(미조정이 정답) 영향이 없다.
    if method is None:
        conf = "UNRESOLVED"
    elif path == "BODY_KEYWORD":
        conf = "LOW"
    else:
        holder = method in ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED")
        has_num = bool(ratio or ev["allocPerShare"])
        passes = bool(checks) and all(v <= TOL for v in checks.values())
        if conflict:
            conf = "MEDIUM"
        elif holder:
            conf = "HIGH" if (has_num and passes) else "MEDIUM"
        else:
            # 제3자·일반공모는 배정비율 자체가 없다. 방식 명시 + 신주수면 충분하다.
            conf = "HIGH" if (new_c and bef_c) else "MEDIUM"
    ev["confidence"] = conf
    ev["provenance"] = "DIRECT_DART_DOCUMENT"
    return ev
