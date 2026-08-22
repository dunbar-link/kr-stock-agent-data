#!/usr/bin/env python3
"""R17 DART 원자료 정규화 + 정정공시 chain 해소.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

두 종류의 직접증거를 하나의 event 스키마로 모은다:
  STRUCTURED  piicDecsn/fricDecsn/pifricDecsn — 증자방식·신주수·증자전주식수 (2015+)
  FILING_ONLY list.json report_nm — 유형·정정표시만 (1999+)

정정 처리(§5): DART 주요정보 API 는 조회 시 **최종 정정본**을 준다
(docs/WABABA-PIT-DATA-FEASIBILITY-R1.md 의 기존 저장소 지식). 따라서
structured 결과는 이미 final effective terms 다. 그럼에도 정정 이력을 버리지 않고
list.json 의 '[기재정정]/[첨부정정]/[첨부추가]' 표시로 chain 을 복원해 보존한다
(historical accounting 은 final, PIT provenance 는 원본 — 두 목적을 섞지 않는다).

안전: 네트워크 0(캐시만 읽는다) · 파일 write 는 reports/research 만.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-rights"

# ── 증자방식(ic_mthn) → precommit ISSUE_METHODS 매핑 ──────────────────────
# 실제 DART 문자열을 그대로 받는다. 추정하지 않고 매칭 실패는 UNKNOWN 으로 남긴다.
METHOD_RULES = [
    ("SHAREHOLDER_THEN_PUBLIC", ("주주배정후 실권주 일반공모", "주주배정후실권주일반공모",
                                 "주주배정 후 실권주 일반공모")),
    ("THIRD_PARTY", ("제3자배정", "제3자 배정", "제삼자배정")),
    ("PUBLIC_OFFERING", ("일반공모", "주주우선공모")),
    ("SHAREHOLDER_ALLOCATION", ("주주배정",)),
]

RIGHTS_TITLE = re.compile(r"유상증자")
BONUS_TITLE = re.compile(r"무상증자")
BOTH_TITLE = re.compile(r"유무상증자")
REDUCTION_TITLE = re.compile(r"감자")
CORRECTION_TITLE = re.compile(r"^\[(기재정정|첨부정정|첨부추가|정정)\]")
THIRD_PARTY_TITLE = re.compile(r"제3자배정|제삼자배정")


def _num(v):
    """'15,690,000' → 15690000. '-'/빈값 → None. 추정하지 않는다."""
    if v is None:
        return None
    s = str(v).replace(",", "").strip()
    if s in ("", "-", "0"):
        return 0 if s == "0" else None
    try:
        return float(s)
    except ValueError:
        return None


def classify_method(ic_mthn: str | None) -> str:
    if not ic_mthn:
        return "UNKNOWN"
    s = str(ic_mthn).replace(" ", "")
    for key, pats in METHOD_RULES:
        for p in pats:
            if p.replace(" ", "") in s:
                return key
    return "UNKNOWN"


def _proceeds(row) -> float | None:
    """자금조달 목적별 금액 합계 = 총 조달금액. 하나도 없으면 None."""
    tot, seen = 0.0, False
    for k in ("fdpp_fclt", "fdpp_bsninh", "fdpp_op", "fdpp_dtrp",
              "fdpp_ocsa", "fdpp_etc"):
        v = _num(row.get(k))
        if v:
            tot += v
            seen = True
    return tot if seen else None


def normalize_structured(ticker, corp_name, ep, row) -> dict:
    new_o = _num(row.get("nstk_ostk_cnt"))
    new_e = _num(row.get("nstk_estk_cnt"))
    before = _num(row.get("bfic_tisstk_ostk"))
    rcept = str(row.get("rcept_no") or "")
    proceeds = _proceeds(row)
    new_total = (new_o or 0) + (new_e or 0)
    ratio = (new_total / before) if (before and new_total) else None
    # 발행가는 API 가 직접 주지 않는다. 조달금액/신주수 는 **파생**이며 그렇게 표기한다.
    issue_price = (proceeds / new_total) if (proceeds and new_total) else None
    if ep == "fricDecsn":
        kind = "BONUS"
    elif ep == "pifricDecsn":
        kind = "BOTH"
    else:
        kind = "RIGHTS"
    return {
        "ticker": ticker, "corpName": corp_name, "endpoint": ep, "kind": kind,
        "rcept_no": rcept, "filing_date": rcept[:8] or None,
        "issue_method_raw": row.get("ic_mthn"),
        "issue_method": classify_method(row.get("ic_mthn")) if kind != "BONUS"
        else "BONUS_NO_PAYMENT",
        "new_shares_common": new_o, "new_shares_other": new_e,
        "shares_before": before, "rights_ratio": ratio,
        "par_value": _num(row.get("fv_ps")),
        "total_proceeds": proceeds,
        "issue_price_derived": issue_price,
        "issuePriceProvenance": "DERIVED_FROM_PROCEEDS_DIV_SHARES"
        if issue_price else "MISSING",
        "provenance": "DIRECT_DART",
        "evidenceLevel": "STRUCTURED",
        "missingFields": [k for k, v in
                          (("new_shares", new_total or None), ("shares_before", before),
                           ("proceeds", proceeds), ("issue_method", row.get("ic_mthn")))
                          if not v],
    }


def normalize_filing(ticker, corp_name, f) -> dict | None:
    nm = f.get("report_nm") or ""
    if BOTH_TITLE.search(nm):
        kind = "BOTH"
    elif RIGHTS_TITLE.search(nm):
        kind = "RIGHTS"
    elif BONUS_TITLE.search(nm):
        kind = "BONUS"
    elif REDUCTION_TITLE.search(nm):
        kind = "REDUCTION"
    else:
        return None
    return {
        "ticker": ticker, "corpName": corp_name, "endpoint": "list", "kind": kind,
        "rcept_no": f.get("rcept_no"), "filing_date": f.get("rcept_dt"),
        "report_nm": nm,
        "isCorrection": bool(CORRECTION_TITLE.match(nm)),
        # 공시명에 방식이 드러나는 경우만 채운다. 없으면 UNKNOWN 을 유지한다.
        "issue_method": "THIRD_PARTY" if THIRD_PARTY_TITLE.search(nm)
        else ("BONUS_NO_PAYMENT" if kind == "BONUS" else "UNKNOWN"),
        "rights_ratio": None, "issue_price_derived": None,
        "provenance": "DIRECT_DART",
        "evidenceLevel": "FILING_TITLE_ONLY",
    }


def correction_chains(filings) -> list[dict]:
    """같은 종목·같은 kind 안에서 원본과 정정을 시간순으로 묶는다.

    DART list.json 은 correction_of_receipt_no 를 주지 않는다. 따라서 chain 은
    '원본 공시 이후 60일 이내의 정정 공시'로 **추론**한다 → DERIVED_INFERENCE.
    최종 조건 자체는 structured API 가 이미 최종본을 주므로 이 추론에 의존하지 않는다.
    """
    out = []
    by = {}
    for f in filings:
        by.setdefault((f["ticker"], f["kind"]), []).append(f)
    for (t, kind), rows in by.items():
        rows.sort(key=lambda r: r["filing_date"] or "")
        cur = None
        for r in rows:
            if not r["isCorrection"]:
                if cur:
                    out.append(cur)
                cur = {"ticker": t, "kind": kind, "original": r["rcept_no"],
                       "originalDate": r["filing_date"], "corrections": [],
                       "provenance": "DERIVED_INFERENCE",
                       "rule": "원본 이후 60일 이내 정정 공시를 같은 chain 으로 본다"}
            elif cur and r["filing_date"] and cur["originalDate"]:
                gap = (int(r["filing_date"]) - int(cur["originalDate"]))
                if gap < 6000:            # 대략 60일 내 (YYYYMMDD 산술 근사)
                    cur["corrections"].append(
                        {"rcept_no": r["rcept_no"], "date": r["filing_date"],
                         "report_nm": r["report_nm"]})
        if cur:
            out.append(cur)
    return [c for c in out if c["corrections"]]


def main() -> int:
    if not CACHE.exists():
        print(json.dumps({"error": "캐시 없음. r17_dart_collect.py 를 먼저 실행."},
                         ensure_ascii=False))
        return 2
    events, filings = [], []
    files = sorted(CACHE.glob("*.json"))
    for p in files:
        rec = json.loads(p.read_text(encoding="utf-8"))
        t, nm = rec["ticker"], rec.get("corpName")
        for ep, rows in (rec.get("structured") or {}).items():
            for row in rows:
                events.append(normalize_structured(t, nm, ep, row))
        for f in rec.get("filings") or []:
            n = normalize_filing(t, nm, f)
            if n:
                filings.append(n)

    chains = correction_chains(filings)
    kinds = {}
    methods = {}
    for e in events:
        kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
        methods[e["issue_method"]] = methods.get(e["issue_method"], 0) + 1
    fk = {}
    for f in filings:
        fk[f["kind"]] = fk.get(f["kind"], 0) + 1

    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r17-dart-rights-normalized-latest.json").write_text(json.dumps({
        "task": "R17", "tickersWithCache": len(files),
        "structuredEvents": len(events), "structuredByKind": kinds,
        "structuredByMethod": methods,
        "filingTitleEvents": len(filings), "filingByKind": fk,
        "evidenceLevels": {
            "STRUCTURED": "증자방식·신주수·증자전주식수 있음 (2015+)",
            "FILING_TITLE_ONLY": "공시명만. 비율·방식 상세 없음 (1999+)"},
        "issuePriceNote": ("DART 주요정보 API 는 발행가를 직접 주지 않는다. "
                           "조달금액/신주수 파생값이며 DERIVED 로 표기했다."),
        "events": events, "filings": filings,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    (RD / "r17-dart-rights-corrections-latest.json").write_text(json.dumps({
        "task": "R17", "chains": len(chains),
        "totalCorrectionFilings": sum(len(c["corrections"]) for c in chains),
        "howFinalTermsAreObtained": (
            "structured API 가 조회 시점의 **최종 정정본**을 반환한다(저장소 기존 "
            "지식: docs/WABABA-PIT-DATA-FEASIBILITY-R1.md). 따라서 회계용 final "
            "effective terms 는 이 chain 추론에 의존하지 않는다. chain 은 PIT "
            "provenance 보존용이다."),
        "chainProvenance": "DERIVED_INFERENCE",
        "list": chains,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    print(json.dumps({"tickers": len(files), "structured": len(events),
                      "byKind": kinds, "byMethod": methods,
                      "filings": len(filings), "filingByKind": fk,
                      "correctionChains": len(chains)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
