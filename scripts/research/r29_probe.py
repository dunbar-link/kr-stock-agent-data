#!/usr/bin/env python3
"""R29 유동성 데이터 소스 검증 — 소규모 실호출 probe.

WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29

이번 작업은 **수집이 아니라 소스 선정**이다(§24). 후보마다 아주 작은 표본만
실제로 호출해 coverage·schema·약관 적합성을 확인한다. 전체 4,640일 수집 금지.

금지(§3): KRX 차단 우회 · IP/proxy rotation · UA 위장 · undocumented KRX bulk
endpoint 재사용 · pykrx 대량수집 재개 · 신규 key 임의발급 · env 변경 · HTML 대량 scraping.

안전: 읽기 전용 + reports/research write. 비밀값 출력 0. env 변경 0. 결제 0.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "krx-liquidity"

# §6 표본 — 총 호출량을 작게 유지한다.
SAMPLE_TICKERS = {
    "005930": "삼성전자(대형)",
    "046940": "우원개발(소형)",
    "005935": "삼성전자우(우선주 — R24 매핑 이슈 검증)",
}
SAMPLE_YEARS = [2007, 2009, 2010, 2015, 2020, 2026]

# §4 credential 은 존재여부만 본다. 값 출력 금지.
CRED_PATTERNS = {
    "DATA_GO_KR_SERVICE_KEY": r"DATA_GO_KR|DATAGOKR|PUBLIC_DATA|GOKR|SERVICE_KEY",
    "KRX_OPENAPI_AUTH_KEY": r"KRX_OPEN|KRX_API|KRX_AUTH|AUTH_KEY",
    "KRX_WEBSITE_LOGIN": r"KRX_ID|KRX_PW|KRX_LOGIN",
    "DART_API_KEY": r"DART_API_KEY|DART_KEY",
}

DATA_GO_KR_BASE = ("https://apis.data.go.kr/1160100/service/"
                   "GetStockSecuritiesInfoService/getStockPriceInfo")
KRX_OPENAPI_BASE = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
NAVER_SISE = "https://api.finance.naver.com/siseJson.naver"

UA = "Mozilla/5.0 (research; contact=wababa)"
TIMEOUT = 20


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r29-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    print(f"[r29] saved {p.name}", file=sys.stderr)


# ══════════════ §4 credential inventory ══════════════
def credential_inventory():
    env = list(os.environ)
    out = {}
    for name, pat in CRED_PATTERNS.items():
        hits = [k for k in env if re.search(pat, k, re.I)]
        usable = [k for k in hits if (os.environ.get(k) or "").strip()]
        out[name] = {
            "status": ("PRESENT" if usable else
                       ("UNUSABLE" if hits else "ABSENT")),
            "envVarCount": len(hits),
            "envVarNames": hits,          # 이름만. 값은 절대 기록하지 않는다.
        }
    return out


# ══════════════ §7·§8 공공데이터포털 ══════════════
def probe_data_go_kr(service_key):
    """key 가 있으면 연도별 실호출. 없으면 인증요구만 확인(1콜)."""
    res = {"candidate": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE",
           "official": True, "operator": "금융위원회 / 공공데이터포털",
           "endpoint": DATA_GO_KR_BASE,
           "automationAllowed": "YES (공공데이터 개방 — 이용허락범위 제한 없음 표기)",
           "cost": 0, "credentialRequired": True,
           "credentialPresent": bool(service_key),
           "calls": 0, "byYear": {}, "notes": []}
    if not service_key:
        # key 없이 1콜 — 엔드포인트 생존과 인증 요구를 확인한다(데이터 수집 아님).
        try:
            r = requests.get(DATA_GO_KR_BASE, params={
                "resultType": "json", "numOfRows": 1, "pageNo": 1,
                "basDt": "20200102"}, timeout=TIMEOUT,
                headers={"User-Agent": UA})
            res["calls"] += 1
            body = r.text[:400]
            res["unauthenticatedProbe"] = {
                "httpStatus": r.status_code,
                "authErrorDetected": bool(
                    re.search(r"SERVICE.?KEY|인증|IS NOT REGISTERED|"
                              r"NO_OPENAPI_SERVICE_ERROR|30|31", body, re.I)),
                "bodyHead": body[:220]}
            res["endpointReachable"] = r.status_code < 500
        except Exception as e:  # noqa: BLE001
            res["unauthenticatedProbe"] = {"error": type(e).__name__,
                                           "detail": str(e)[:160]}
            res["endpointReachable"] = False
        res["notes"].append(
            "serviceKey 부재 — 연도별 coverage 실측 불가. §21 에 따라 신규 키를 "
            "임의 발급하지 않았다.")
        return res

    for y in SAMPLE_YEARS:
        d = f"{y}0102" if y != 2026 else "20260803"
        try:
            r = requests.get(DATA_GO_KR_BASE, params={
                "serviceKey": service_key, "resultType": "json",
                "numOfRows": 5, "pageNo": 1, "basDt": d,
                "likeSrtnCd": "005930"}, timeout=TIMEOUT,
                headers={"User-Agent": UA})
            res["calls"] += 1
            j = r.json()
            items = (((j.get("response") or {}).get("body") or {})
                     .get("items") or {}).get("item") or []
            if isinstance(items, dict):
                items = [items]
            row = items[0] if items else None
            res["byYear"][str(y)] = {
                "date": d, "httpStatus": r.status_code,
                "rows": len(items),
                "hasData": bool(items),
                "fields": sorted(row) if row else [],
                "volumeField": next((k for k in (row or {})
                                     if k.lower() in ("trqu", "volume")), None),
                "tradedValueField": next((k for k in (row or {})
                                          if k.lower() in ("trprc", "amount")), None)}
        except Exception as e:  # noqa: BLE001
            res["byYear"][str(y)] = {"date": d, "error": type(e).__name__,
                                     "detail": str(e)[:160]}
        time.sleep(0.5)
    got = [y for y, v in res["byYear"].items() if v.get("hasData")]
    res["earliestConfirmedYear"] = min(got) if got else None
    res["latestConfirmedYear"] = max(got) if got else None
    return res


# ══════════════ §9 KRX 공식 Open API ══════════════
def probe_krx_openapi(auth_key):
    res = {"candidate": "KRX_OFFICIAL_OPEN_API",
           "official": True, "operator": "한국거래소 (openapi.krx.co.kr)",
           "endpoint": KRX_OPENAPI_BASE,
           "automationAllowed": "YES (자동화 활용 전제 공식 서비스)",
           "cost": 0, "credentialRequired": True,
           "credentialPresent": bool(auth_key),
           "documentedStartDate": "2010-01-04 (유가증권 일별매매정보)",
           "calls": 0, "notes": []}
    if not auth_key:
        res["status"] = "CREDENTIAL_REQUIRED"
        res["notes"].append(
            "AUTH_KEY 부재 — 실호출 미수행. §9·§21 에 따라 신규 신청을 하지 않았다. "
            "회원가입·인증키 신청·서비스 활용신청·관리자 승인 절차가 필요하다.")
        res["gap2007to2009"] = (
            "공식 문서상 시작이 2010-01-04 이면 R27 필요구간 2007-01-02~2009-12-31 은 "
            "이 소스 단독으로 덮이지 않는다(§9).")
        return res
    res["status"] = "CREDENTIAL_PRESENT_BUT_NOT_PROBED"
    return res


# ══════════════ §10 무료 대체 (NAVER) ══════════════
def probe_naver():
    """FinanceDataReader 의 NAVER 경로가 쓰는 엔드포인트를 1콜로 특성만 확인."""
    res = {"candidate": "NAVER_FINANCE_SISE_JSON",
           "official": False, "operator": "네이버 금융(비공식 내부 endpoint)",
           "endpoint": NAVER_SISE,
           "viaLibrary": "FinanceDataReader (NAVER mode) — 이 repo 미설치",
           "libraryInstalled": False,
           "cost": 0, "credentialRequired": False,
           "calls": 0, "notes": []}
    try:
        r = requests.get(NAVER_SISE, params={
            "symbol": "005930", "requestType": "1",
            "startTime": "20070102", "endTime": "20070110",
            "timeframe": "day"}, timeout=TIMEOUT,
            headers={"User-Agent": UA})
        res["calls"] += 1
        res["httpStatus"] = r.status_code
        txt = (r.text or "").strip()
        res["responseHead"] = txt[:200]
        # siseJson 은 JS 배열 리터럴을 돌려준다(정식 JSON 아님).
        res["isStrictJson"] = txt.startswith("[") and '"' in txt[:80]
        rows = re.findall(r"\[([^\[\]]+)\]", txt)
        res["rowCount"] = max(0, len(rows) - 1)
        res["headerRow"] = rows[0][:200] if rows else None
        res["hasVolumeColumn"] = bool(rows and "거래량" in rows[0])
        res["hasTradedValueColumn"] = bool(rows and "거래대금" in rows[0])
        res["reached2007"] = res["rowCount"] > 0
    except Exception as e:  # noqa: BLE001
        res["error"] = type(e).__name__
        res["detail"] = str(e)[:160]
    res["notes"] = [
        "공개 문서화된 API 가 아니라 웹서비스 내부 endpoint 다.",
        "자동화 대량 이용을 명시적으로 허용한 약관 근거를 확인하지 못했다.",
        "R28 에서 KRX 비공식 경로 대량수집이 약관 위반으로 차단된 전례가 있다.",
        "§13 '약관이 모호하면 PRIMARY 선정 금지' 에 해당한다.",
        "라이브러리 신규 설치는 의존성 변경이므로 하지 않았다(§3).",
    ]
    res["termsRisk"] = "HIGH_AMBIGUOUS"
    return res


# ══════════════ §16 R27 111일 cross-check 준비 ══════════════
def r27_cache_summary():
    days = sorted(p.name[:-7] for p in CACHE.glob("*.csv.gz"))
    return {"cachedDays": len(days),
            "range": [days[0], days[-1]] if days else None,
            "purpose": ("reference evidence 전용. 대량수집 재개 근거로 쓰지 "
                        "않는다(§16)."),
            "deleted": False}


def main() -> int:
    cred = credential_inventory()
    save("credential-inventory", {
        "task": "R29", "byCredential": cred,
        "secretsPrinted": 0,
        "envModified": 0,
        "rule": ("PRESENT/ABSENT/UNUSABLE 과 환경변수 **이름**만 기록한다. "
                 "값·토큰·키는 출력하지 않는다(§4).")})

    dgk_key = None
    for k in cred["DATA_GO_KR_SERVICE_KEY"]["envVarNames"]:
        if (os.environ.get(k) or "").strip():
            dgk_key = os.environ[k]
            break
    krx_key = None
    for k in cred["KRX_OPENAPI_AUTH_KEY"]["envVarNames"]:
        if (os.environ.get(k) or "").strip():
            krx_key = os.environ[k]
            break

    dgk = probe_data_go_kr(dgk_key)
    save("data-go-kr-probe", {"task": "R29", **dgk})
    krx = probe_krx_openapi(krx_key)
    save("krx-openapi-probe", {"task": "R29", **krx})
    nav = probe_naver()
    save("free-alternative-probe", {"task": "R29", "candidates": [nav],
                                    "maxCandidatesRule":
                                    "공식 2 + 무료 2 로 제한(§11)"})

    save("source-inventory", {
        "task": "R29",
        "localLiquidityData": {
            "pitSnapshots": {"hasVolume": False, "hasTradedValue": False,
                             "columns": ["ticker", "market", "close",
                                         "marketCap", "shares", "PER", "PBR",
                                         "EPS", "BPS", "DIV", "DPS"]},
            "krxLiquidityCache": r27_cache_summary(),
            "parquetOrSqlite": "없음",
            "otherPipelineWithVolume": "없음 (repo 전수 검색)"},
        "searchedFields": ["volume", "tradingVolume", "traded_value",
                           "tradingValue", "accTrdvol", "accTrdval",
                           "거래량", "거래대금"],
        "conclusion": ("로컬에 2007~2026 거래량·거래대금을 가진 pipeline 은 없다. "
                       "R27 이 남긴 111일 캐시가 유일하다.")})

    print(json.dumps({
        "dataGoKrCredential": cred["DATA_GO_KR_SERVICE_KEY"]["status"],
        "krxOpenApiCredential": cred["KRX_OPENAPI_AUTH_KEY"]["status"],
        "dataGoKrReachable": dgk.get("endpointReachable"),
        "krxOpenApiStatus": krx.get("status"),
        "naverHttp": nav.get("httpStatus"),
        "naverVolume": nav.get("hasVolumeColumn"),
        "naverTradedValue": nav.get("hasTradedValueColumn"),
        "naverReached2007": nav.get("reached2007"),
        "totalCalls": dgk["calls"] + krx["calls"] + nav["calls"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
