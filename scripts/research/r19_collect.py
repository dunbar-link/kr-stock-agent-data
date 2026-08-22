#!/usr/bin/env python3
"""R19 직접공시 수집기 — NO_DIRECT_MATCH 342건 주변의 **모든 공시유형**.

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19

왜 전 유형인가: R17/R18 은 주요사항보고서(pblntf_ty=B)만 봤다. 그런데 전환청구권
행사·신주인수권행사·주식매수선택권행사는 **거래소 공시**로 나가고 B 에 없다.
그래서 이 342건에서 아무 유상증자 공시도 찾지 못한 것이다 — 애초에 유상증자가
아니었을 가능성이 크다는 뜻이다(§10).

범위는 342건 대상 종목·기간에 한정한다. 새 universe 확장 금지(§2).
새 crawler framework 를 만들지 않고 기존 DART helper 규약을 재사용한다.

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from krx_fetch_guard import BACKOFF_SEC, MAX_ATTEMPTS  # noqa: E402  기존 규약 재사용

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-capital-actions"
API = "https://opendart.fss.or.kr/api/list.json"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))
MAX_PAGES = 12

_key = os.environ.get("DART_API_KEY", "").strip()

# 자본행위 관련 공시만 보관한다(저장량 축소 · 목적 외 수집 방지).
KEEP = ("증자", "감자", "전환", "신주인수권", "교환사채", "합병", "분할",
        "주식교환", "주식이전", "매수선택권", "자기주식", "주식배당",
        "발행결과", "종목코드")


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


def fetch_ticker(corp_code, bgn, end):
    """한 종목의 창 안 자본행위 공시 전부(유형 필터 없음)."""
    rows, page, netfail = [], 1, False
    while page <= MAX_PAGES:
        d = _get({"corp_code": corp_code, "bgn_de": bgn, "end_de": end,
                  "page_no": page, "page_count": 100})
        st = d.get("status")
        if st == "NETFAIL":
            netfail = True
            break
        if st == "013":                      # 공시 없음 — 정상 결과
            break
        for r in (d.get("list") or []):
            nm = r.get("report_nm") or ""
            if any(k in nm for k in KEEP):
                rows.append({"rcept_no": r.get("rcept_no"),
                             "rcept_dt": r.get("rcept_dt"),
                             "report_nm": nm,
                             "corp_name": r.get("corp_name"),
                             "stock_code": r.get("stock_code")})
        if page >= int(d.get("total_page") or 1):
            break
        page += 1
        time.sleep(SLEEP)
    return rows, netfail


def window(date):
    """사건월 기준 -6 ~ +3 개월. 결정형은 선행, 행사형은 후행하므로 양쪽을 덮는다."""
    y, m = int(date[:4]), int(date[5:7])
    a = y * 12 + (m - 1) - 6
    b = y * 12 + (m - 1) + 3
    return (f"{a // 12:04d}{a % 12 + 1:02d}01",
            f"{b // 12:04d}{b % 12 + 1:02d}28")


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r19-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    CACHE.mkdir(parents=True, exist_ok=True)

    # 같은 종목이 여러 사건을 가지면 창을 합쳐 한 번만 받는다.
    jobs = {}
    for t in tg:
        if not t["corpCode"]:
            continue
        b, e = window(t["date"])
        j = jobs.setdefault(t["ticker"], {"corpCode": t["corpCode"],
                                          "bgn": b, "end": e, "events": []})
        j["bgn"] = min(j["bgn"], b)
        j["end"] = max(j["end"], e)
        j["events"].append(t["date"])

    new = hit = fail = 0
    for i, (tk, j) in enumerate(sorted(jobs.items()), 1):
        p = CACHE / f"{tk}.json"
        if p.exists():
            hit += 1
            continue
        rows, netfail = fetch_ticker(j["corpCode"], j["bgn"], j["end"])
        if netfail and not rows:
            fail += 1
        else:
            p.write_text(json.dumps({"ticker": tk, "corpCode": j["corpCode"],
                                     "bgn": j["bgn"], "end": j["end"],
                                     "events": j["events"],
                                     "filings": rows}, ensure_ascii=False),
                         encoding="utf-8")
            new += 1
        if i % 50 == 0:
            print(f"[r19] {i}/{len(jobs)} new={new} cached={hit} fail={fail}",
                  file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    files = sorted(CACHE.glob("*.json"))
    total = sum(len(json.loads(f.read_text(encoding="utf-8"))["filings"])
                for f in files)
    (RD / "r19-direct-source-events-latest.json").write_text(json.dumps({
        "task": "R19", "endpoint": "opendart list.json (전 공시유형)",
        "whyAllTypes": ("전환청구권행사·신주인수권행사·주식매수선택권행사는 거래소 "
                        "공시라 주요사항보고서(B)에 없다. R17/R18 이 B 만 봤기 때문에 "
                        "이 342건에서 유상증자 공시를 못 찾은 것이다."),
        "windowMonths": "-6 ~ +3 (결정형 선행 · 행사형 후행 모두 포함)",
        "keepFilter": list(KEEP),
        "targetEvents": len(tg), "targetTickers": len(jobs),
        "tickersCached": len(files), "filingsKept": total,
        "newThisRun": new, "cacheHits": hit, "hardFailures": fail,
        "cacheDir": str(CACHE.relative_to(ROOT)),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"tickers": len(jobs), "cached": len(files),
                      "filings": total, "new": new, "hit": hit, "fail": fail},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
