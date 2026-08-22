#!/usr/bin/env python3
"""R23 증권신고서·투자설명서 수집기 — 11건 유계.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

R22 는 사후 자율공시를 봤고 11건이 남았다. R23 은 **증권신고서 원문의
구주주 배정표**를 1순위로 본다(§3). 같은 시도를 반복하지 않는다.

수집 유형(지분증권만 — 채무증권/사채는 유상증자가 아니다):
  [발행조건확정]증권신고서(지분증권)   최종 확정 조건  ← 최우선
  [기재정정]증권신고서(지분증권)       정정본
  증권신고서(지분증권)                  최초본
  증권신고서[주식...]                   구양식
  투자설명서 / 예비투자설명서[주식]     같은 배정표

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
helper 재사용: r18_doc_collect.fetch_doc · krx_fetch_guard 백오프 (§25).
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
from r18_doc_collect import fetch_doc  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
LIST_CACHE = ROOT / "_cache" / "dart-r23-lists"
DOC_CACHE = ROOT / "_cache" / "dart-documents"
API = "https://opendart.fss.or.kr/api/list.json"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))
_key = os.environ.get("DART_API_KEY", "").strip()

# 채무증권·사채는 유상증자가 아니다. 제외한다.
DEBT = re.compile(r"채무증권|사채|파생결합|주가연계")
EQUITY_REG = re.compile(r"증권신고서")
PROSPECTUS = re.compile(r"투자설명서")
CONFIRMED = re.compile(r"발행조건확정")
AMENDED = re.compile(r"기재정정|첨부정정|첨부추가|정정")


def classify(nm: str):
    if DEBT.search(nm):
        return None
    if EQUITY_REG.search(nm):
        if CONFIRMED.search(nm):
            return "REG_CONFIRMED"
        if AMENDED.search(nm):
            return "REG_AMENDED"
        return "REG_ORIGINAL"
    if PROSPECTUS.search(nm):
        return "PROSPECTUS_AMENDED" if AMENDED.search(nm) else "PROSPECTUS"
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


def window(date, back=6, fwd=12):
    """신고서는 사건보다 앞서고 정정·확정은 뒤따른다. 양쪽을 덮되 유계."""
    y, m = int(date[:4]), int(date[5:7])
    a, b = y * 12 + (m - 1) - back, y * 12 + (m - 1) + fwd
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
    tg = json.loads((RD / "r23-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    LIST_CACHE.mkdir(parents=True, exist_ok=True)
    DOC_CACHE.mkdir(parents=True, exist_ok=True)

    lnew = lhit = lfail = 0
    for t in tg:
        p = LIST_CACHE / f"{t['ticker']}.json"
        if p.exists():
            lhit += 1
            continue
        b, e = window(t["date"])
        rows, netfail = fetch_list(t["corpCode"], b, e)
        if netfail and not rows:
            lfail += 1
        else:
            p.write_text(json.dumps({"ticker": t["ticker"], "date": t["date"],
                                     "bgn": b, "end": e, "filings": rows},
                                    ensure_ascii=False), encoding="utf-8")
            lnew += 1
        time.sleep(SLEEP)

    receipts = {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        for f in d["filings"]:
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
        if i % 20 == 0:
            print(f"[r23] doc {i}/{len(receipts)} new={dnew} cached={dhit} "
                  f"fail={dfail}", file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    kinds, per = {}, {}
    for p in sorted(LIST_CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        per[d["ticker"]] = len(d["filings"])
        for f in d["filings"]:
            kinds[f["kind"]] = kinds.get(f["kind"], 0) + 1

    (RD / "r23-securities-filings-latest.json").write_text(json.dumps({
        "task": "R23", "endpoint": "opendart list.json + document.xml",
        "window": {"months": [-6, 12],
                   "why": "신고서는 선행, 정정·확정은 후행. 양쪽을 덮되 유계."},
        "kindsSearched": ["REG_CONFIRMED", "REG_AMENDED", "REG_ORIGINAL",
                          "PROSPECTUS", "PROSPECTUS_AMENDED"],
        "debtExcluded": "채무증권·사채·파생결합은 유상증자가 아니라 제외한다.",
        "targetEvents": len(tg),
        "filingKinds": kinds, "perTicker": per,
        "tickersWithNoFiling": sorted(t["ticker"] for t in tg
                                      if per.get(t["ticker"], 0) == 0),
        "listNew": lnew, "listCacheHits": lhit, "listFailures": lfail,
        "documentsWanted": len(receipts), "documentsNew": dnew,
        "documentsCacheHits": dhit, "documentsFailed": dfail,
        "helpersReused": ("r18_doc_collect.fetch_doc · krx_fetch_guard 백오프 · "
                          "dart-documents 캐시 공유 (§25 새 framework 금지)"),
        "documents": sorted(status.values(), key=lambda x: x["rcept_no"]),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"filingKinds": kinds, "perTicker": per,
                      "docsWanted": len(receipts), "docNew": dnew,
                      "docCached": dhit, "docFail": dfail,
                      "listFail": lfail}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
