#!/usr/bin/env python3
"""R20 사후 확정공시 수집기 — 증권발행실적보고서 중심.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

대상은 R20 targets precommit 의 92건뿐이다(§2 범위 확대 금지).

수집 대상 공시(§3 우선순위):
  증권발행실적보고서[주식]   실청약·실배정·실납입액 (1순위)
  유상증자 최종발행가액확정   확정발행가
  추가상장 / 변경상장         실제 상장된 신주 수
  단수주및실권주처리          실권주 처리 결과
  유상증자결정(정정 포함)     정정 chain

주의: '증권발행실적보고서[파생결합증권...]' 는 ELS 라 무관하다. [주식]만 쓴다.

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
재현: 접수번호 단위 checkpoint. 캐시 적중분은 재호출하지 않는다.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from krx_fetch_guard import BACKOFF_SEC, MAX_ATTEMPTS  # noqa: E402  기존 규약 재사용
from r18_doc_collect import fetch_doc  # noqa: E402  원문 수집 helper 재사용

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
LIST_CACHE = ROOT / "_cache" / "dart-r20-lists"
DOC_CACHE = ROOT / "_cache" / "dart-documents"      # R18 캐시 공유(중복 다운로드 방지)
API = "https://opendart.fss.or.kr/api/list.json"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))

_key = os.environ.get("DART_API_KEY", "").strip()

# 사후 확정공시 분류 — 우선순위 순서대로 본다.
KINDS = [
    ("ISSUE_RESULT_REPORT", re.compile(r"증권발행실적보고서")),
    ("FINAL_PRICE", re.compile(r"발행가액\s*확정|최종발행가액|발행가액.*결정")),
    ("ADDITIONAL_LISTING", re.compile(r"추가상장|신주상장|변경상장")),
    ("UNSUBSCRIBED_RESULT", re.compile(r"실권주|단수주")),
    ("RIGHTS_DECISION", re.compile(r"유상증자\s*결정")),
    ("PRICE_NOTICE", re.compile(r"신주발행가액|발행가액.*안내")),
]
# ELS·파생결합증권 실적보고서는 유상증자와 무관하다.
DERIVATIVE = re.compile(r"파생결합|주가연계|ELS|DLS|사채")


def classify(nm: str):
    if DERIVATIVE.search(nm) and "증권발행실적보고서" in nm:
        return "ISSUE_RESULT_DERIVATIVE_IGNORED"
    for kind, pat in KINDS:
        if pat.search(nm):
            return kind
    return None


def _get(params):
    url = f"{API}?{urllib.parse.urlencode(dict(params, crtfc_key=_key))}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt >= MAX_ATTEMPTS:
                return {"status": "NETFAIL", "message": type(e).__name__}
            time.sleep(BACKOFF_SEC[min(attempt - 1, len(BACKOFF_SEC) - 1)])
    return {"status": "NETFAIL"}


def window(date, back=6, fwd=6):
    """사후 확정공시는 사건 **뒤**에 나온다. 앞뒤를 모두 덮는다."""
    y, m = int(date[:4]), int(date[5:7])
    a = y * 12 + (m - 1) - back
    b = y * 12 + (m - 1) + fwd
    return (f"{a // 12:04d}{a % 12 + 1:02d}01",
            f"{b // 12:04d}{b % 12 + 1:02d}28")


def fetch_list(corp, bgn, end):
    rows, page = [], 1
    while page <= 12:
        d = _get({"corp_code": corp, "bgn_de": bgn, "end_de": end,
                  "page_no": page, "page_count": 100})
        st = d.get("status")
        if st == "NETFAIL":
            return rows, True
        if st == "013":
            break
        for r in (d.get("list") or []):
            nm = r.get("report_nm") or ""
            kind = classify(nm)
            if kind:
                rows.append({"rcept_no": r.get("rcept_no"),
                             "rcept_dt": r.get("rcept_dt"),
                             "report_nm": nm, "kind": kind})
        if page >= int(d.get("total_page") or 1):
            break
        page += 1
        time.sleep(SLEEP)
    return rows, False


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r20-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    LIST_CACHE.mkdir(parents=True, exist_ok=True)
    DOC_CACHE.mkdir(parents=True, exist_ok=True)

    # ── 1) 공시목록 (종목 단위, 창 병합) ──────────────────────────
    jobs = {}
    for t in tg:
        b, e = window(t["date"])
        j = jobs.setdefault(t["ticker"], {"corpCode": t["corpCode"],
                                          "bgn": b, "end": e})
        j["bgn"], j["end"] = min(j["bgn"], b), max(j["end"], e)

    lnew = lhit = lfail = 0
    for i, (tk, j) in enumerate(sorted(jobs.items()), 1):
        p = LIST_CACHE / f"{tk}.json"
        if p.exists():
            lhit += 1
            continue
        rows, netfail = fetch_list(j["corpCode"], j["bgn"], j["end"])
        if netfail and not rows:
            lfail += 1
        else:
            p.write_text(json.dumps({"ticker": tk, "bgn": j["bgn"],
                                     "end": j["end"], "filings": rows},
                                    ensure_ascii=False), encoding="utf-8")
            lnew += 1
        if i % 25 == 0:
            print(f"[r20] list {i}/{len(jobs)} new={lnew} cached={lhit}",
                  file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    # ── 2) 원문 (실적보고서·확정발행가·실권주 처리) ────────────────
    want = {"ISSUE_RESULT_REPORT", "FINAL_PRICE", "UNSUBSCRIBED_RESULT"}
    receipts = {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        for f in d["filings"]:
            if f["kind"] in want:
                receipts[f["rcept_no"]] = {**f, "ticker": d["ticker"]}

    dnew = dhit = dfail = 0
    status = {}
    for i, (rn, meta) in enumerate(sorted(receipts.items()), 1):
        f = DOC_CACHE / f"{rn}.xml"
        if f.exists():
            dhit += 1
            status[rn] = {**meta, "status": "OK", "cachedFile": f.name,
                          "source": "CACHE"}
            continue
        txt, m = fetch_doc(rn)
        if txt is not None:
            f.write_text(txt, encoding="utf-8")
            dnew += 1
            status[rn] = {**meta, **m, "cachedFile": f.name, "source": "FETCH"}
        else:
            dfail += 1
            status[rn] = {**meta, **m}
        if i % 50 == 0:
            print(f"[r20] doc {i}/{len(receipts)} new={dnew} cached={dhit} "
                  f"fail={dfail}", file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    kinds = {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        for f in json.loads(p.read_text(encoding="utf-8"))["filings"]:
            kinds[f["kind"]] = kinds.get(f["kind"], 0) + 1

    (RD / "r20-post-issuance-filings-latest.json").write_text(json.dumps({
        "task": "R20", "endpoint": "opendart list.json + document.xml",
        "targetEvents": len(tg), "targetTickers": len(jobs),
        "windowMonths": "-6 ~ +6 (사후 확정공시는 사건 뒤에 나온다)",
        "listCached": len(list(LIST_CACHE.glob('*.json'))),
        "listNew": lnew, "listCacheHits": lhit, "listFailures": lfail,
        "filingKinds": kinds,
        "documentsWanted": len(receipts), "documentsNew": dnew,
        "documentsCacheHits": dhit, "documentsFailed": dfail,
        "derivativeIgnoredNote": ("'증권발행실적보고서[파생결합증권]' 은 ELS 라 "
                                  "유상증자와 무관하므로 분류 단계에서 제외한다."),
        "docCacheShared": "R18 문서 캐시를 공유해 중복 다운로드를 피한다.",
        "documents": sorted(status.values(), key=lambda x: x["rcept_no"]),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"tickers": len(jobs), "filingKinds": kinds,
                      "docsWanted": len(receipts), "docNew": dnew,
                      "docCached": dhit, "docFail": dfail,
                      "listFail": lfail}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
