#!/usr/bin/env python3
"""R24 직접소스 수집 — UNKNOWN 50건 주변 전 공시유형 + 필요한 본문.

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

새 crawler 를 만들지 않는다(§3). 기존 것을 그대로 쓴다:
  · 목록  = r19_collect.fetch_ticker (pacing·backoff·페이징 규약 포함)
  · 본문  = r18_doc_collect.fetch_doc (ZIP·EUC-KR 처리 포함)
  · 캐시  = 기존 dart-documents 재사용, 목록만 R24 전용 폴더

R19 와 다른 점은 **창(-12~+6)과 대상(50건)** 뿐이다. R19 는 -6~+3 을 썼고
이 50건은 거기서 아무것도 못 찾은 잔여다. 창을 넓히되 유계로 둔다(§26).

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r18_doc_collect import fetch_doc  # noqa: E402
from r19_collect import SLEEP, _key, fetch_ticker  # noqa: E402
from r24_classify import classify_filing  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
LIST_CACHE = ROOT / "_cache" / "dart-r24-lists"
DOC_CACHE = ROOT / "_cache" / "dart-documents"

# ★ 자체수정 2 (§31): 초판은 자체 키워드 목록(DOC_WORTH)으로 받을 본문을
#   골랐다. 그 목록에 '주식배당' 이 빠져 있었고, 사건당 6건 상한 때문에 정작
#   **분류기가 PRIMARY 로 고른 공시**가 안 받아진 경우가 많았다(실측 50건 중
#   본문 확보 21건). 키워드 목록을 따로 두지 말고 **분류기가 실제로 사건으로
#   인정하는 공시**를 그대로 받는다. 판정 근거와 수집 대상이 같아진다.
MAX_DOCS_PER_EVENT = 14


def window(date, back=12, fwd=6):
    y, m = int(date[:4]), int(date[5:7])
    a = y * 12 + (m - 1) - back
    b = y * 12 + (m - 1) + fwd
    return (f"{a // 12:04d}{a % 12 + 1:02d}01",
            f"{b // 12:04d}{b % 12 + 1:02d}28")


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r24-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    LIST_CACHE.mkdir(parents=True, exist_ok=True)
    DOC_CACHE.mkdir(parents=True, exist_ok=True)

    # 같은 corp_code 를 여러 사건이 공유하면 창을 합쳐 한 번만 받는다.
    jobs = {}
    for t in tg:
        if not t["corpCode"]:
            continue
        b, e = window(t["date"])
        j = jobs.setdefault(t["corpCode"], {"bgn": b, "end": e, "tickers": set()})
        j["bgn"] = min(j["bgn"], b)
        j["end"] = max(j["end"], e)
        j["tickers"].add(t["ticker"])

    lnew = lhit = lfail = 0
    lists = {}
    for i, (corp, j) in enumerate(sorted(jobs.items()), 1):
        p = LIST_CACHE / f"{corp}.json"
        if p.exists():
            lists[corp] = json.loads(p.read_text(encoding="utf-8"))
            lhit += 1
            continue
        rows, netfail = fetch_ticker(corp, j["bgn"], j["end"])
        if netfail:
            lfail += 1
            print(f"[r24] list NETFAIL {corp}", file=sys.stderr)
            continue
        rec = {"corpCode": corp, "bgn": j["bgn"], "end": j["end"],
               "tickers": sorted(j["tickers"]), "filings": rows}
        p.write_text(json.dumps(rec, ensure_ascii=False, indent=1),
                     encoding="utf-8")
        lists[corp] = rec
        lnew += 1
        if i % 10 == 0:
            print(f"[r24] lists {i}/{len(jobs)}", file=sys.stderr)
        time.sleep(SLEEP)

    # ── 본문: 사건월에 가까운 관련 공시부터 최대 N건 ──────────────────
    want = {}
    for t in tg:
        rec = lists.get(t["corpCode"])
        if not rec:
            continue
        ev = _mon(t["date"])
        cand = []
        for f in rec["filings"]:
            kind, _weak, win = classify_filing(f.get("report_nm") or "")
            if not kind:
                continue
            gap = _mon(f["rcept_dt"]) - ev
            if win[0] <= gap <= win[1]:
                cand.append(f)
        cand.sort(key=lambda f: abs(_mon(f["rcept_dt"]) - ev))
        for f in cand[:MAX_DOCS_PER_EVENT]:
            want.setdefault(f["rcept_no"], f)

    dnew = dhit = dfail = 0
    for i, (rc, f) in enumerate(sorted(want.items()), 1):
        p = DOC_CACHE / f"{rc}.txt"
        if p.exists():
            dhit += 1
            continue
        txt, meta = fetch_doc(rc)
        if txt is None:
            dfail += 1
            print(f"[r24] doc {meta.get('status')} {rc}", file=sys.stderr)
        else:
            p.write_text(txt, encoding="utf-8")
            dnew += 1
        if i % 25 == 0:
            print(f"[r24] docs {i}/{len(want)}", file=sys.stderr)
        time.sleep(SLEEP)

    filings = sum(len(r["filings"]) for r in lists.values())
    out = {"task": "R24", "targets": len(tg),
           "corpCodes": len(jobs), "window": window("2020-01-01"),
           "windowRule": "-12 ~ +6 개월 (§26 유계)",
           "listsNew": lnew, "listsCacheHits": lhit, "listsFailed": lfail,
           "filingsFound": filings,
           "eventsWithFilings": sum(
               1 for t in tg if lists.get(t["corpCode"], {}).get("filings")),
           "documentsRequested": len(want),
           "documentsNew": dnew, "documentsCacheHits": dhit,
           "documentsFailed": dfail,
           "docSelection": "분류기(classify_filing)가 사건으로 인정한 공시만",
           "reuse": ("목록=r19_collect.fetch_ticker · 본문=r18_doc_collect.fetch_doc · "
                     "본문캐시=dart-documents 재사용. 새 crawler 0(§3).")}
    (RD / "r24-direct-source-events-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
