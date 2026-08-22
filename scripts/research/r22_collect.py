#!/usr/bin/env python3
"""R22 확대창 사후공시 수집기 — 27건 유계.

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22

R20 은 창 -6~+9 에서 증권발행실적보고서만 봤고 25건이 남았다. R22 는 창을
-3~+18 로 넓히고 유형을 확장한다(§3·§4): 발행가확정·청약결과·실권처리·
신주상장·증자완료·정기보고서 자본금변동.

기존 helper 를 재사용한다(§25 새 framework 금지):
  목록  opendart list.json (r20_collect._get 규약과 동일한 백오프)
  원문  r18_doc_collect.fetch_doc
  캐시  _cache/dart-documents 공유

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
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
from krx_fetch_guard import BACKOFF_SEC, MAX_ATTEMPTS  # noqa: E402
from r18_doc_collect import fetch_doc  # noqa: E402  원문 helper 재사용

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
LIST_CACHE = ROOT / "_cache" / "dart-r22-lists"
DOC_CACHE = ROOT / "_cache" / "dart-documents"
API = "https://opendart.fss.or.kr/api/list.json"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))

_key = os.environ.get("DART_API_KEY", "").strip()

# 파생결합증권(ELS) 실적보고서는 유상증자와 무관하다.
DERIVATIVE = re.compile(r"파생결합|주가연계|ELS|DLS")

# §3 우선순위 순서. 앞선 규칙이 이긴다.
KINDS = [
    ("ISSUE_RESULT_REPORT", re.compile(r"증권발행실적보고서")),
    ("FINAL_PRICE", re.compile(r"발행가액\s*확정|최종발행가액|발행가액\s*결정"
                               r"|신주발행가액")),
    ("SUBSCRIPTION_RESULT", re.compile(r"청약\s*결과|청약결과")),
    ("UNSUBSCRIBED_RESULT", re.compile(r"실권주|단수주")),
    ("NEW_LISTING", re.compile(r"추가상장|신주상장|변경상장")),
    ("ISSUE_COMPLETION", re.compile(r"증자\s*완료|납입\s*완료|발행결과")),
    ("RIGHTS_DECISION", re.compile(r"유상증자\s*결정")),
    ("PERIODIC_REPORT", re.compile(r"사업보고서|분기보고서|반기보고서")),
]
# 원문까지 받는 유형 — 숫자가 필요한 것들
WANT_DOC = {"ISSUE_RESULT_REPORT", "FINAL_PRICE", "SUBSCRIPTION_RESULT",
            "UNSUBSCRIBED_RESULT", "NEW_LISTING", "ISSUE_COMPLETION",
            "RIGHTS_DECISION"}


def classify(nm: str):
    if DERIVATIVE.search(nm):
        return None
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


def window(date, back=3, fwd=18):
    """§4 — 사후 chain 을 끝까지 연결하려면 뒤를 길게 본다. 유계."""
    y, m = int(date[:4]), int(date[5:7])
    a = y * 12 + (m - 1) - back
    b = y * 12 + (m - 1) + fwd
    return (f"{a // 12:04d}{a % 12 + 1:02d}01",
            f"{b // 12:04d}{b % 12 + 1:02d}28")


def fetch_list(corp, bgn, end):
    rows, page = [], 1
    while page <= 20:
        d = _get({"corp_code": corp, "bgn_de": bgn, "end_de": end,
                  "page_no": page, "page_count": 100})
        st = d.get("status")
        if st == "NETFAIL":
            return rows, True
        if st == "013":
            break
        for r in (d.get("list") or []):
            nm = r.get("report_nm") or ""
            k = classify(nm)
            if k:
                rows.append({"rcept_no": r.get("rcept_no"),
                             "rcept_dt": r.get("rcept_dt"),
                             "report_nm": nm, "kind": k})
        if page >= int(d.get("total_page") or 1):
            break
        page += 1
        time.sleep(SLEEP)
    return rows, False


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r22-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    LIST_CACHE.mkdir(parents=True, exist_ok=True)
    DOC_CACHE.mkdir(parents=True, exist_ok=True)

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
        time.sleep(SLEEP)

    receipts = {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        for f in d["filings"]:
            if f["kind"] in WANT_DOC:
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
        if i % 100 == 0:
            print(f"[r22] doc {i}/{len(receipts)} new={dnew} cached={dhit} "
                  f"fail={dfail}", file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    kinds = {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        for f in json.loads(p.read_text(encoding="utf-8"))["filings"]:
            kinds[f["kind"]] = kinds.get(f["kind"], 0) + 1

    (RD / "r22-expanded-window-filings-latest.json").write_text(json.dumps({
        "task": "R22",
        "window": {"months": [-3, 18], "r20Window": [-6, 9],
                   "why": "사후 chain 을 끝까지 연결하려면 뒤를 길게 본다(§4)."},
        "kindsSearched": [k for k, _ in KINDS],
        "documentKinds": sorted(WANT_DOC),
        "targetEvents": len(tg), "targetTickers": len(jobs),
        "filingKinds": kinds,
        "listNew": lnew, "listCacheHits": lhit, "listFailures": lfail,
        "documentsWanted": len(receipts), "documentsNew": dnew,
        "documentsCacheHits": dhit, "documentsFailed": dfail,
        "derivativeExcluded": "파생결합증권(ELS) 실적보고서는 제외한다.",
        "helpersReused": ("r18_doc_collect.fetch_doc · krx_fetch_guard 백오프 · "
                          "dart-documents 캐시 공유 (§25 새 framework 금지)"),
        "documents": sorted(status.values(), key=lambda x: x["rcept_no"]),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"tickers": len(jobs), "filingKinds": kinds,
                      "docsWanted": len(receipts), "docNew": dnew,
                      "docCached": dhit, "docFail": dfail,
                      "listFail": lfail}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
