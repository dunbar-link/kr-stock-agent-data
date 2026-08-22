#!/usr/bin/env python3
"""R17 DART 유상증자 직접소스 수집기.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

수집 대상은 **R16 SUSPECTED_RIGHTS 가 발생한 종목만**이다(§19 범위 확장 금지).

두 경로를 함께 쓴다 — 실측으로 확인한 커버리지 경계 때문이다:
  1) 주요사항보고서 주요정보 API (piicDecsn/fricDecsn/pifricDecsn)
     구조화 필드(증자방식·신주수·증자전주식수)를 준다. **2015년부터만 존재한다**
     (표본 8종목 39건 전부 2015+). 2007~2014 는 여기서 나오지 않는다.
  2) 공시목록 API (list.json, pblntf_ty=B 주요사항보고서)
     1999년부터 존재하고 report_nm 에 유형('유상증자결정'/'무상증자결정')과
     정정표시('[기재정정]')가 직접 들어 있다. 구조화 필드는 없다.

두 경로 모두 DIRECT_DART 다. 다만 (2)는 비율·방식 상세가 없으므로 confidence 가
낮게 잡힌다 — 이 구분을 매칭 단계에서 유지한다.

안전: 무료 공개 DART 조회만 · env/token 변경 0 · 파일은 캐시만 write ·
      실주문 0 · 브로커 0 · production write 0.
재현: 종목 단위 checkpoint. 재실행 시 캐시 적중분은 재호출하지 않는다.
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
CACHE = ROOT / "_cache" / "dart-rights"
CORP_CODES = ROOT / "_cache" / "dart-corp-codes.json"

API = "https://opendart.fss.or.kr/api"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))
STRUCTURED = ("piicDecsn", "fricDecsn", "pifricDecsn")
BGN, END = "19990101", "20261231"

_key = os.environ.get("DART_API_KEY", "").strip()


def _get(path: str, params: dict) -> dict:
    """DART 호출 — 기존 백오프 규약(MAX_ATTEMPTS/BACKOFF_SEC) 재사용. 키는 절대 로그하지 않는다."""
    q = dict(params)
    q["crtfc_key"] = _key
    url = f"{API}/{path}.json?{urllib.parse.urlencode(q)}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt >= MAX_ATTEMPTS:
                return {"status": "NETFAIL", "message": type(e).__name__}
            time.sleep(BACKOFF_SEC[min(attempt - 1, len(BACKOFF_SEC) - 1)])
    return {"status": "NETFAIL", "message": "exhausted"}


def fetch_ticker(corp_code: str) -> dict:
    """한 종목의 증자 관련 직접증거 전부. 상태코드 013(무데이터)은 정상 결과다."""
    out = {"corpCode": corp_code, "structured": {}, "filings": [],
           "netfail": [], "fetchedParts": 0}
    for ep in STRUCTURED:
        d = _get(ep, {"corp_code": corp_code, "bgn_de": BGN, "end_de": END})
        st = d.get("status")
        if st == "NETFAIL":
            out["netfail"].append(ep)
        else:
            out["structured"][ep] = d.get("list") or []
            out["fetchedParts"] += 1
        time.sleep(SLEEP)

    page = 1
    while page <= 20:                     # 안전 상한. 초과분은 truncated 로 표시.
        d = _get("list", {"corp_code": corp_code, "bgn_de": BGN, "end_de": END,
                          "pblntf_ty": "B", "page_no": page, "page_count": 100})
        st = d.get("status")
        if st == "NETFAIL":
            out["netfail"].append(f"list:p{page}")
            break
        if st == "013":                   # 공시 없음
            out["fetchedParts"] += 1
            break
        rows = d.get("list") or []
        # 증자·감자 관련만 보관한다(저장량 축소, 목적 외 수집 방지).
        out["filings"] += [
            {"rcept_no": r.get("rcept_no"), "rcept_dt": r.get("rcept_dt"),
             "report_nm": r.get("report_nm"), "stock_code": r.get("stock_code"),
             "corp_name": r.get("corp_name")}
            for r in rows if ("증자" in (r.get("report_nm") or "")
                              or "감자" in (r.get("report_nm") or ""))]
        out["fetchedParts"] += 1
        if page >= int(d.get("total_page") or 1):
            break
        page += 1
        time.sleep(SLEEP)
    else:
        out["truncated"] = True
    return out


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS",
                          "reason": "DART_API_KEY 환경변수 없음"}, ensure_ascii=False))
        return 2
    cc = json.loads(CORP_CODES.read_text(encoding="utf-8"))
    sus = json.loads((RD / "r17-suspected-events-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    tickers = sorted({e["ticker"] for e in sus})
    mapped = [t for t in tickers if t in cc]
    unmapped = [t for t in tickers if t not in cc]

    CACHE.mkdir(parents=True, exist_ok=True)
    done = fail = hit = 0
    for i, t in enumerate(mapped, 1):
        p = CACHE / f"{t}.json"
        if p.exists():                    # resume — 재호출하지 않는다
            hit += 1
            continue
        rec = fetch_ticker(cc[t]["corp_code"])
        rec["ticker"] = t
        rec["corpName"] = cc[t].get("corp_name")
        if rec["netfail"] and rec["fetchedParts"] == 0:
            fail += 1                     # 전량 실패는 저장하지 않는다(다음 실행에서 재시도)
        else:
            p.write_text(json.dumps(rec, ensure_ascii=False), encoding="utf-8")
            done += 1
        if i % 100 == 0:
            print(f"[r17] {i}/{len(mapped)} new={done} cached={hit} fail={fail}",
                  file=sys.stderr, flush=True)

    raw = {"task": "R17", "endpointsStructured": list(STRUCTURED),
           "endpointFilings": "list.json pblntf_ty=B",
           "structuredEarliestYearObserved": 2015,
           "suspectedTickers": len(tickers), "corpCodeMapped": len(mapped),
           "corpCodeUnmapped": unmapped,
           "cacheDir": str(CACHE.relative_to(ROOT)),
           "fetchedThisRun": done, "cacheHits": hit, "hardFailures": fail}
    (RD / "r17-dart-rights-raw-latest.json").write_text(
        json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"mapped": len(mapped), "unmapped": len(unmapped),
                      "new": done, "cached": hit, "fail": fail}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
