#!/usr/bin/env python3
"""R30 공식 유동성 소스 클라이언트 — 공공데이터포털 금융위원회 주식시세정보.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

R29 가 PRIMARY 로 확정한 소스 하나만 쓴다.

    PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE
    https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

R28 에서 약관 위반으로 차단된 KRX 웹 자동화(pykrx 대량수집)는 이 모듈이
**전혀 건드리지 않는다.** 거래일 캘린더는 R27 이 이미 캐시해 둔 파일만 읽는다.

── 비밀값 규칙(§1) ────────────────────────────────────────────────
serviceKey 는 이 모듈 밖으로 나가지 않는다. 로그·evidence·예외 메시지에
값을 넣지 않고, 존재여부(PRESENT/ABSENT)와 **환경변수 이름**만 기록한다.
URL 을 통째로 출력하지 않는다 — serviceKey 가 쿼리스트링에 들어가기 때문이다.

── 필드 매핑 규칙(§5) ─────────────────────────────────────────────
응답 필드명을 추정해서 하드코딩하지 않는다. 별칭표로 **탐색**하고, 찾지
못하면 조용히 0 이나 근사로 대체하지 않고 실패로 표면화한다(fail-closed).
특히 거래대금이 없으면 close×volume 을 canonical 거래대금으로 쓰지 않는다 —
실제 거래대금은 체결가 가중이라 종가×거래량과 다르다(§5).

안전: 무료 공식 공개 API · 기존 credential 규약 재사용 · 신규 키 발급 0 ·
env 변경 0 · 캐시만 write · 실주문 0.
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import requests

ROOT = Path(__file__).resolve().parents[2]

# ── 소스 정본 (R29 §8 에서 확정. 추정 endpoint 금지 §4) ──────────────
SOURCE = "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"
SOURCE_VERSION = "getStockPriceInfo/1160100"
ENDPOINT = ("https://apis.data.go.kr/1160100/service/"
            "GetStockSecuritiesInfoService/getStockPriceInfo")
OPERATOR = "금융위원회 / 공공데이터포털"
LICENSE = "이용허락범위 제한 없음 (공공데이터 개방 · 무료 · 자동승인)"

# ── 호출 예산과 페이지 크기는 **다른 것**이다 (2026-08-23 교정) ──────
#   초판은 둘을 같은 10000 으로 묶어 두어 한쪽을 고치면 다른 쪽이 따라
#   움직였다. 의미가 다르므로 분리한다.
#     DAILY_CALL_QUOTA  = 하루에 몇 번 부를 수 있나 (계정 한도)
#     MAX_ROWS_PER_PAGE = 한 번에 몇 행을 받을 수 있나 (요청 파라미터 상한)
DAILY_CALL_QUOTA = 10000        # 공식 문서: 개발계정 일 10,000 호출
MAX_ROWS_PER_PAGE = 10000       # 2026-08-23 실측: 10/100/1000/2000/10000 모두 OK
#   실측 근거 — basDt=20260803 전종목 2,872행이 numOfRows=10000 한 콜에
#   전부 들어왔다(반환 2872 = totalCount). 즉 하루치 = 1콜이다.
#   그래도 상한을 신뢰하지 않고, 페이지 크기 오류가 나면 절반으로 낮춰
#   재시도한다(adaptive fallback). 추정값을 고정하지 않기 위해서다.
PAGE_SIZE_FALLBACKS = (10000, 5000, 2000, 1000, 500, 100)

# ── User-Agent (2026-08-23 근본원인 교정) ────────────────────────────
#   ★ 초판의 UA `Mozilla/5.0 (research; contact=wababa)` 를 붙이면
#     data.go.kr 게이트웨이가 **INVALID_REQUEST_PARAMETER_ERROR(코드 10)** 로
#     거부한다. 파라미터·인증키는 멀쩡한데도 그렇다. 같은 요청에서 UA 만
#     빼거나 평범한 값으로 바꾸면 즉시 `NORMAL SERVICE` 다(실측 대조 3종).
#
#     이 UA 는 R29 probe 에서 물려받은 것이고, R29 가 "미인증이라 코드 10"
#     이라고 읽었던 응답도 실은 이 UA 때문이었을 수 있다. 코드 10 을 인증
#     문제로 해석하면 안 되는 이유가 여기 있다.
#
#     그래서 커스텀 UA 를 쓰지 않는다. requests 기본 UA 로 보낸다.
UA = None                       # None = requests 기본 UA 사용
TIMEOUT = 30

# ── credential (§1·§2) — 기존 규약 재사용. 새 env 구조를 만들지 않는다 ──
#   이 repo 의 research 스크립트 규약은 `os.environ.get("<NAME>")` 이고
#   (r17~r20 의 DART_API_KEY 와 동일), .env.local 이 그 mirror 다.
#   둘 다 **이미 존재하는 경로**이므로 새로 만드는 것이 아니다.
CRED_ENV_PRIMARY = "DATA_GO_KR_SERVICE_KEY"
CRED_ENV_ALIASES = (CRED_ENV_PRIMARY, "DATA_GO_KR_API_KEY", "DATA_GO_KR_KEY",
                    "PUBLIC_DATA_SERVICE_KEY", "DATAGOKR_SERVICE_KEY")
ENV_LOCAL = ROOT / ".env.local"

# ── 응답 필드 별칭 (§5) — 탐색용. 못 찾으면 fail-closed ───────────────
FIELD_ALIASES = {
    "basDt":      ("basDt", "bas_dt", "기준일자"),
    "ticker":     ("srtnCd", "srtn_cd", "종목코드"),
    "isin":       ("isinCd", "isin_cd", "isinCdNm"),
    "name":       ("itmsNm", "itms_nm", "종목명"),
    "market":     ("mrktCtg", "mrkt_ctg", "시장구분"),
    "open":       ("mkp", "시가"),
    "high":       ("hipr", "고가"),
    "low":        ("lopr", "저가"),
    "close":      ("clpr", "종가"),
    "volume":     ("trqu", "trQu", "거래량"),
    "tradedValue": ("trPrc", "trprc", "거래대금"),
    "shares":     ("lstgStCnt", "lstg_st_cnt", "상장주식수"),
    "marketCap":  ("mrktTotAmt", "mrkt_tot_amt", "시가총액"),
}
# R27 이 실제로 쓰는 최소 필드 — 하나라도 없으면 소스 부적합(§10)
REQUIRED_FIELDS = ("basDt", "ticker", "close", "volume", "tradedValue")

MAX_RETRY = 3
RETRY_BACKOFF = (1.0, 3.0, 8.0)
SLEEP = 0.12          # 초당 과호출 금지(§13)


class CredentialAbsent(RuntimeError):
    """serviceKey 미등록. 값이 아니라 사실만 담는다."""


class SchemaMismatch(RuntimeError):
    """응답에 R27 이 요구하는 필드가 없다. 근사로 대체하지 않는다(§5)."""


# ══════════════════════ credential (값 출력 0) ══════════════════════
def _read_windows_user_env():
    """Windows 사용자 환경변수를 **읽기만** 한다. 쓰지 않는다.

    왜 필요한가: 대장이 `setx` 로 키를 넣어도 **이미 열려 있던 터미널의
    프로세스 환경에는 반영되지 않는다.** 그 상태에서 `os.environ` 만 보면
    등록된 키를 ABSENT 로 오판한다(2026-08-23 실제로 그랬다). 새 저장소를
    만드는 게 아니라 대장이 이미 쓴 그 자리를 그대로 읽는 것이다.
    """
    if sys.platform != "win32":
        return {}
    try:
        import winreg
    except ImportError:
        return {}
    out = {}
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
            for name in CRED_ENV_ALIASES:
                try:
                    val, _ = winreg.QueryValueEx(k, name)
                except OSError:
                    continue
                if isinstance(val, str) and val.strip():
                    out[name] = val.strip()
    except OSError:
        return {}
    return out


def _read_env_local():
    """.env.local 에서 이름→값. 값은 반환만 하고 절대 로그로 내보내지 않는다."""
    if not ENV_LOCAL.exists():
        return {}
    out = {}
    try:
        text = ENV_LOCAL.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$", line)
        if m:
            out[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return out


def credential_status():
    """PRESENT / ABSENT + 어디서 왔는지. **값·길이 이외 어떤 단서도 남기지 않는다.**

    길이를 남기는 이유: 키가 잘렸는지(붙여넣기 사고) 를 값 없이 판별하려면
    길이 하나는 필요하다. 길이만으로 키를 복원할 수 없다.
    """
    envmap = _read_env_local()
    winmap = _read_windows_user_env()
    checked = []
    for name in CRED_ENV_ALIASES:
        for origin, val in (("process-env", os.environ.get(name)),
                            (".env.local", envmap.get(name)),
                            ("windows-user-env", winmap.get(name))):
            checked.append({"envVarName": name, "origin": origin,
                            "present": bool((val or "").strip())})
            if (val or "").strip():
                return {"status": "PRESENT", "envVarName": name,
                        "origin": origin, "valueLength": len((val or "").strip()),
                        "checked": checked, "secretPrinted": False}
    return {"status": "ABSENT", "envVarName": None, "origin": None,
            "valueLength": 0, "checked": checked, "secretPrinted": False,
            "expectedEnvVarName": CRED_ENV_PRIMARY,
            "expectedPaths": ["Windows 사용자 환경변수 (DART_API_KEY 와 동일 규약)",
                              str(ENV_LOCAL) + " (기존 gitignore 대상 mirror)"]}


def service_key():
    """실제 키. 반환값은 호출자가 절대 출력하지 않는다."""
    envmap = _read_env_local()
    winmap = _read_windows_user_env()
    for name in CRED_ENV_ALIASES:
        for val in (os.environ.get(name), envmap.get(name),
                    winmap.get(name)):
            if (val or "").strip():
                # 포털은 Encoding/Decoding 두 형태를 준다. requests 가 다시
                # 인코딩하므로 미리 unquote 해 두면 두 형태 모두 정상 동작한다.
                return unquote((val or "").strip())
    raise CredentialAbsent(
        f"{CRED_ENV_PRIMARY} 미등록 — 값이 아니라 부재 사실만 기록한다.")


# ══════════════════════ 호출 ══════════════════════
def _scrub(text):
    """혹시라도 응답/오류에 섞인 serviceKey 흔적을 지운다."""
    return re.sub(r"(serviceKey|ServiceKey)=[^&\s\"']+", r"\1=<redacted>",
                  str(text))


class Client:
    """공식 API 클라이언트. 호출 수를 세고 일일 quota 를 넘지 않는다(§13)."""

    def __init__(self, key=None, quota=DAILY_CALL_QUOTA):
        self._key = key if key is not None else service_key()
        self.calls = 0
        self.quota = quota
        self.retries = 0
        self.rateLimitHits = 0
        self.pageSizeDowngrades = 0
        self.effectivePageSize = MAX_ROWS_PER_PAGE
        self.session = requests.Session()
        if UA:                      # None 이면 requests 기본 UA 를 그대로 쓴다
            self.session.headers.update({"User-Agent": UA})

    # ── 저수준 1콜 ─────────────────────────────────────────────
    def _get(self, params):
        if self.calls >= self.quota:
            return None, f"QUOTA_EXHAUSTED:{self.quota}"
        p = dict(params)
        p["serviceKey"] = self._key
        p.setdefault("resultType", "json")
        last = "UNKNOWN"
        for attempt in range(MAX_RETRY):
            try:
                r = self.session.get(ENDPOINT, params=p, timeout=TIMEOUT)
                self.calls += 1
                # 공공데이터포털 표준 오류 규약 — 본문이 JSON 이 아닐 수 있다.
                try:
                    j = r.json()
                except ValueError:
                    last = f"NON_JSON:{r.status_code}"
                    j = None
                if j is not None and "OpenAPI_ServiceResponse" in j:
                    hdr = ((j["OpenAPI_ServiceResponse"] or {})
                           .get("cmmMsgHeader") or {})
                    code = str(hdr.get("returnReasonCode", "?"))
                    msg = str(hdr.get("errMsg", "?"))
                    # 22/336 = 트래픽 초과 계열 → 재시도 가치 있음
                    if code in ("22", "336"):
                        self.rateLimitHits += 1
                        last = f"RATE_LIMIT:{code}"
                    elif code in ("30", "31", "32"):
                        return None, f"CREDENTIAL_INVALID:{code}:{msg}"
                    else:
                        return None, f"API_ERROR:{code}:{msg}"
                elif j is not None:
                    hdr = ((j.get("response") or {}).get("header") or {})
                    rc = str(hdr.get("resultCode", "00"))
                    if rc not in ("00", "0"):
                        rm = str(hdr.get("resultMsg", "?"))
                        if rc in ("22", "336"):
                            self.rateLimitHits += 1
                            last = f"RATE_LIMIT:{rc}"
                        else:
                            return None, f"API_ERROR:{rc}:{rm}"
                    else:
                        return j, "OK"
            except Exception as e:                       # noqa: BLE001
                last = f"FETCH_FAIL:{type(e).__name__}"
            if attempt < MAX_RETRY - 1:
                self.retries += 1
                time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
        return None, _scrub(last)

    # ── 페이지 하나 ────────────────────────────────────────────
    def page(self, *, bas_dt=None, ticker=None, rows=None,
             page_no=1, begin=None, end=None):
        """한 페이지. 페이지 크기 상한을 추정으로 고정하지 않고 낮춰가며 맞춘다.

        `rows` 를 주지 않으면 현재 유효 페이지 크기를 쓴다. 파라미터 오류
        (코드 10) 가 나면 **한 단계 작은 크기로 낮춰 재시도**한다. 이때
        낮아진 크기는 이 클라이언트에 기억돼 다음 호출부터 그대로 쓰인다 —
        같은 실패를 4,640번 반복하지 않기 위해서다.
        """
        base = {}
        if bas_dt:
            base["basDt"] = bas_dt.replace("-", "")
        if begin:
            base["beginBasDt"] = begin.replace("-", "")
        if end:
            base["endBasDt"] = end.replace("-", "")
        if ticker:
            base["likeSrtnCd"] = ticker

        explicit = rows is not None
        size = rows if explicit else self.effectivePageSize
        ladder = ([size] if explicit else
                  [s for s in PAGE_SIZE_FALLBACKS if s <= size] or [size])
        st = "UNKNOWN"
        j = None
        for attempt_size in ladder:
            j, st = self._get({**base, "numOfRows": attempt_size,
                               "pageNo": page_no})
            if st == "OK":
                if not explicit and attempt_size != self.effectivePageSize:
                    self.effectivePageSize = attempt_size
                    self.pageSizeDowngrades += 1
                size = attempt_size
                break
            # 파라미터 오류만 페이지 크기 문제일 수 있다. 인증·트래픽 오류는
            # 크기를 줄여도 달라지지 않으므로 즉시 포기한다.
            if not st.startswith("API_ERROR:10"):
                break
        if st != "OK":
            return None, st, {"triedPageSizes": ladder}
        rows = size
        body = ((j.get("response") or {}).get("body") or {})
        items = (body.get("items") or {}).get("item") or []
        if isinstance(items, dict):
            items = [items]
        meta = {"totalCount": int(body.get("totalCount") or 0),
                "numOfRows": int(body.get("numOfRows") or rows),
                "pageNo": int(body.get("pageNo") or page_no)}
        return items, "OK", meta

    # ── 하루치 전 종목 (페이지네이션 포함) ─────────────────────
    def fetch_day(self, iso_date):
        """(rows, status, meta). rows 는 canonical 스키마로 정규화된 dict 목록."""
        out, page_no, total = [], 1, None
        pages = 0
        while True:
            items, st, meta = self.page(bas_dt=iso_date, page_no=page_no)
            if st != "OK":
                return None, st, {"pages": pages}
            pages += 1
            if total is None:
                total = meta["totalCount"]
            if not items:
                break
            out.extend(items)
            if total and len(out) >= total:
                break
            if len(items) < meta["numOfRows"]:
                break
            page_no += 1
            time.sleep(SLEEP)
            if page_no > 50:                     # 폭주 방지
                return None, "PAGINATION_RUNAWAY", {"pages": pages}
        if not out:
            return [], "EMPTY", {"pages": pages, "totalCount": total or 0}
        norm = [normalize(r, iso_date) for r in out]
        return norm, "OK", {"pages": pages, "totalCount": total or len(out)}


# ══════════════════════ 정규화 (§5·§14) ══════════════════════
def resolve_fields(sample):
    """실제 응답 필드 → canonical 이름. 추정하지 않고 탐색한다."""
    got = {}
    lower = {k.lower(): k for k in sample}
    for canon, aliases in FIELD_ALIASES.items():
        for a in aliases:
            if a in sample:
                got[canon] = a
                break
            if a.lower() in lower:
                got[canon] = lower[a.lower()]
                break
    return got


def _num(v):
    """숫자 또는 None. 빈문자·'-' 는 결측이지 0 이 아니다(§14)."""
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "null", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize(item, iso_date):
    """공식 응답 1행 → canonical 캐시 행(§14). 결측과 0 을 구분한다."""
    f = resolve_fields(item)
    missing = [k for k in REQUIRED_FIELDS if k not in f]
    if missing:
        raise SchemaMismatch("REQUIRED_FIELD_MISSING:" + ",".join(missing))
    tk = str(item.get(f["ticker"], "")).strip()
    tk = re.sub(r"^A", "", tk)              # 일부 응답의 'A005930' 접두 제거
    tk = tk.zfill(6) if tk.isdigit() else tk   # leading zero 보존(§8)
    vol = _num(item.get(f["volume"]))
    tv = _num(item.get(f.get("tradedValue")))
    flags = []
    if vol is None:
        flags.append("VOLUME_MISSING")
    if tv is None:
        flags.append("TRADED_VALUE_MISSING")
    if vol == 0:
        flags.append("ZERO_VOLUME")            # 관측된 0 — 결측 아님
    return {
        "date": iso_date,
        "ticker": tk,
        "name": str(item.get(f.get("name"), "") or ""),
        "market": str(item.get(f.get("market"), "") or ""),
        "close": _num(item.get(f["close"])),
        "open": _num(item.get(f.get("open"))),
        "high": _num(item.get(f.get("high"))),
        "low": _num(item.get(f.get("low"))),
        "volume": vol,
        "traded_value": tv,
        "market_cap": _num(item.get(f.get("marketCap"))),
        "shares": _num(item.get(f.get("shares"))),
        "source": SOURCE,
        "source_version": SOURCE_VERSION,
        "quality_flag": "|".join(flags) if flags else "OK",
    }


def source_meta():
    return {"source": SOURCE, "sourceVersion": SOURCE_VERSION,
            "endpoint": ENDPOINT, "operator": OPERATOR,
            "automationAllowed": "YES — " + LICENSE,
            "cost": 0, "dailyCallQuota": DAILY_CALL_QUOTA,
            "maxRowsPerPage": MAX_ROWS_PER_PAGE,
            "credentialEnvVarName": CRED_ENV_PRIMARY,
            "krxWebAutomationUsed": False,
            "pykrxBulkUsed": False}
