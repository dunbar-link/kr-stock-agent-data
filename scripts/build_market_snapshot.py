import io
import json
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET

import pandas as pd
from pykrx import stock


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "financial-universe-real.json"
NEWS_SAMPLE_PATH = ROOT / "news-momentum-sample.json"

CACHE_DIR = ROOT / "_cache"
CORP_CODE_CACHE_PATH = CACHE_DIR / "dart-corp-codes.json"
STATEMENT_CACHE_DIR = CACHE_DIR / "dart-statements"

DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
DART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_FINANCIAL_STATEMENT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

DART_REQUEST_SLEEP_SEC = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))
DART_MAX_TICKERS = int(os.environ.get("DART_MAX_TICKERS", "0") or "0")
DART_LOOKBACK_YEARS = 4
ANNUAL_REPORT_CODE = "11011"

# universe 종목 수 급감 감지 하한(정상 KOSPI+KOSDAQ ≈ 2,700+). 부분 산출물 공개 금지용.
MIN_UNIVERSE_COUNT = int(os.environ.get("WABABA_MIN_UNIVERSE_COUNT", "2000") or "2000")


def ensure_cache_dirs():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    STATEMENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


def safe_number(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace(",", "")
            .replace(" ", "")
            .replace("\u3000", "")
        )
        if cleaned == "":
            return None

        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = f"-{cleaned[1:-1]}"

        value = cleaned

    try:
        numeric = float(value)
        if pd.isna(numeric):
            return None
        return numeric
    except Exception:
        return None


def safe_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_string_list(value):
    if not isinstance(value, list):
        return []

    result = []
    for item in value:
        text = safe_text(item)
        if text:
            result.append(text)
    return result


def format_date_yyyymmdd(value: datetime) -> str:
    return value.strftime("%Y%m%d")


def find_latest_business_day() -> str:
    current = now_kst()

    for days_back in range(0, 14):
        candidate = format_date_yyyymmdd(current - timedelta(days=days_back))

        try:
            df = stock.get_market_ohlcv_by_ticker(candidate, market="KOSPI")
            if df is None or df.empty:
                continue

            # pykrx 1.2.7은 비영업일(토/일/공휴일)에도 empty=False인 DataFrame을
            # 반환하지만 모든 값이 0 — 종가 합계로 실제 거래 여부를 검증한다.
            if "종가" in df.columns:
                close_series = df["종가"]
            else:
                close_series = df.iloc[:, 3]
            close_sum = pd.to_numeric(close_series, errors="coerce").fillna(0).sum()

            if close_sum > 0:
                print(f"[INFO] 사용 거래일: {candidate}")
                return candidate

            print(f"[INFO] 비영업일(종가 합계=0) 건너뜀: {candidate}")
        except Exception as error:
            print(f"[WARN] 거래일 확인 실패: {candidate} / {error}")

    raise RuntimeError("최근 영업일을 찾지 못했습니다. (14일 범위 탐색 실패)")


def normalize_market_cap_to_billion_krw(value):
    numeric = safe_number(value)
    if numeric is None:
        return None
    return round(numeric / 100_000_000, 2)


# ── KRX 수집 fail-closed (WABABA-KRX-FUNDAMENTAL-RECOVERY-R1) ────────────────
#  이전 구현은 어떤 예외든 삼키고 *빈 DataFrame* 을 돌려줘, 일시적 upstream 장애가
#  조용한 데이터 결측으로 바뀌어 ranking·signal·apply 까지 그대로 흘렀다.
#  이제 krx_fetch_guard 로 응답을 검증하고(구조만; 개별 종목의 정상 결측은 허용)
#  일시적 장애만 제한 재시도(최초 1 + 재시도 2 = 총 3회)한 뒤, 끝내 실패하면 예외를 올린다.
#  과거 값·0·전일 데이터로 위장하지 않는다.
_FETCH_EVIDENCE: list[dict] = []


def _run_guarded(fetcher, spec):
    """검증 통과 프레임 반환. 실패 시 KrxDataInvalid 전파(빈 프레임 반환 금지)."""
    from krx_fetch_guard import fetch_validated
    try:
        result = fetch_validated(fetcher, spec)
    except Exception as err:  # KrxDataInvalid 포함 — 증거만 남기고 그대로 올린다
        ev = getattr(err, "evidence", None) or {}
        ev.setdefault("kind", spec.kind)
        ev.setdefault("market", spec.market)
        ev["verdict"] = "INVALID"
        ev["code"] = getattr(err, "code", "UNKNOWN")
        _FETCH_EVIDENCE.append(ev)
        raise
    _FETCH_EVIDENCE.append({**result.evidence, "verdict": "PASS"})
    return result.frame


def fetch_evidence() -> list[dict]:
    """이번 실행에서 수집된 redacted 진단 증거(원문·인증정보 없음)."""
    return list(_FETCH_EVIDENCE)


def reset_fetch_evidence() -> None:
    _FETCH_EVIDENCE.clear()


def safe_get_ohlcv(base_date: str, market: str) -> pd.DataFrame:
    """종가 — 공식 마법공식 체결가·평가에 필수. 실패 시 fail-closed."""
    from krx_fetch_guard import FetchSpec
    spec = FetchSpec(kind="ohlcv", market=market, required_columns=("티커", "종가"),
                     min_rows=50, numeric_columns=("종가",), collapse_guard_columns=("종가",),
                     required=True)
    frame = _run_guarded(
        lambda: stock.get_market_ohlcv_by_ticker(base_date, market=market).reset_index(), spec)
    return frame.rename(columns={"티커": "symbol", "종가": "price"})[["symbol", "price"]]


def safe_get_fundamental(base_date: str, market: str) -> pd.DataFrame:
    """PER/PBR/DIV — 구조는 필수(응답 자체가 유효해야 함). 개별 종목 결측은 정상(적자기업 PER 등).

    공식 마법공식(EBIT/EV·EBIT/투입자본)은 이 값을 쓰지 않지만, 이 호출이 깨졌다는 것은
    KRX 응답 자체가 비정상이라는 신호이므로 해당 거래일 데이터 품질을 INVALID 로 본다.
    """
    from krx_fetch_guard import FetchSpec
    spec = FetchSpec(kind="fundamental", market=market,
                     required_columns=("티커", "PER", "PBR", "DIV"),
                     optional_columns=("BPS", "EPS", "DPS"),
                     min_rows=50, numeric_columns=("PER", "PBR", "DIV"),
                     # PER 은 적자기업이 많아 전량 NaN 검사에서 제외. PBR 은 사실상 전 종목 존재.
                     collapse_guard_columns=("PBR",), required=True)
    frame = _run_guarded(
        lambda: stock.get_market_fundamental_by_ticker(base_date, market=market).reset_index(), spec)
    return frame.rename(columns={"티커": "symbol"})[["symbol", "PER", "PBR", "DIV"]]


def safe_get_market_cap(base_date: str, market: str) -> pd.DataFrame:
    """시가총액 — EV(=시총+총부채-현금) 계산에 필수. 실패 시 fail-closed."""
    from krx_fetch_guard import FetchSpec
    spec = FetchSpec(kind="marketcap", market=market, required_columns=("티커", "시가총액"),
                     min_rows=50, numeric_columns=("시가총액",),
                     collapse_guard_columns=("시가총액",), required=True)
    frame = _run_guarded(
        lambda: stock.get_market_cap_by_ticker(base_date, market=market).reset_index(), spec)
    frame = frame.rename(columns={"티커": "symbol", "시가총액": "marketCap"})[["symbol", "marketCap"]]
    frame["marketCap"] = frame["marketCap"].apply(normalize_market_cap_to_billion_krw)
    return frame


def safe_get_ticker_list(base_date: str, market: str) -> list[str]:
    """종목 목록 — 비면 그 시장 전체가 결측이므로 fail-closed."""
    from krx_fetch_guard import KrxDataInvalid, classify_error, redact, BACKOFF_SEC, MAX_ATTEMPTS
    trail = []
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            tickers = stock.get_market_ticker_list(base_date, market=market)
        except BaseException as error:  # noqa: BLE001
            kind = classify_error(error)
            trail.append({"attempt": attempt, "outcome": "EXCEPTION", "errorKind": kind,
                          "errorFingerprint": redact(error)})
            if kind == "RETRYABLE_TRANSIENT" and attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SEC[min(attempt - 1, len(BACKOFF_SEC) - 1)])
                continue
            _FETCH_EVIDENCE.append({"kind": "tickers", "market": market, "verdict": "INVALID",
                                    "code": "UPSTREAM_EXCEPTION", "attempts": attempt, "trail": trail})
            raise KrxDataInvalid("UPSTREAM_EXCEPTION",
                                 f"[tickers/{market}] {redact(error)} (시도 {attempt}회)")
        if tickers:
            trail.append({"attempt": attempt, "outcome": "OK", "count": len(tickers)})
            _FETCH_EVIDENCE.append({"kind": "tickers", "market": market, "verdict": "PASS",
                                    "attempts": attempt, "count": len(tickers), "trail": trail})
            return tickers
        trail.append({"attempt": attempt, "outcome": "INVALID", "invalidCode": "EMPTY_TICKER_LIST"})
        if attempt < MAX_ATTEMPTS:
            time.sleep(BACKOFF_SEC[min(attempt - 1, len(BACKOFF_SEC) - 1)])
    _FETCH_EVIDENCE.append({"kind": "tickers", "market": market, "verdict": "INVALID",
                            "code": "EMPTY_TICKER_LIST", "attempts": MAX_ATTEMPTS, "trail": trail})
    raise KrxDataInvalid("EMPTY_TICKER_LIST", f"[tickers/{market}] 종목 목록이 비어 있음 (시도 {MAX_ATTEMPTS}회)")


def collect_markets_with_quality(base_date: str, iso_date: str):
    """KOSPI·KOSDAQ 을 수집하고 그 거래일 품질 증거를 기록한다. (frames, markets, iso_date) 반환.

    ★ 이 함수는 build_market_snapshot.build_payload 와 build_market_snapshot_fast.build_payload
      **양쪽 모두** 호출해야 한다. 한쪽에만 넣으면 실제 일일 파이프라인(=fast 경로)에서
      품질 증거가 생성되지 않아, fail-closed gate 가 매 거래일 Auto Apply 를 막는다.
      (2026-08-03~08-07 운영 중단 실사고 — 증거 생성 경로와 소비 경로 불일치)

    두 시장 중 하나라도 검증 실패면 품질 INVALID 를 기록하고 예외를 올려 universe 생성을 중단한다.
    과거값·0·전일값 대체 없음(fail-closed).
    """
    import krx_data_quality as Q
    reset_fetch_evidence()
    markets: dict[str, str] = {}
    frames: dict = {}
    try:
        for name in Q.REQUIRED_MARKETS:
            frames[name] = get_market_frame(base_date, name)
            markets[name] = "PASS"
    except Exception as err:  # noqa: BLE001 — KrxDataInvalid 포함
        failed = next((m for m in Q.REQUIRED_MARKETS if m not in markets), "UNKNOWN")
        markets[failed] = "INVALID"
        Q.write_status(Q.build_status(
            date_iso=iso_date, verdict="INVALID", markets=markets,
            evidence=fetch_evidence(),
            reason=f"{type(err).__name__}: {getattr(err, 'message', str(err))[:300]}",
            now=now_kst().isoformat()))
        print(f"[BLOCKED] KRX 데이터 품질 INVALID ({iso_date}) — {failed} 실패. "
              f"universe 생성 중단(과거값·0 대체 없음).")
        raise
    return frames, markets, iso_date


def record_universe_quality(iso_date: str, markets: dict, universe_count: int) -> None:
    """universe 최종 산출 직후 품질 PASS 를 확정 기록한다(종목 수 급감이면 INVALID + 예외)."""
    import krx_data_quality as Q
    if universe_count < MIN_UNIVERSE_COUNT:
        Q.write_status(Q.build_status(
            date_iso=iso_date, verdict="INVALID", markets=markets, evidence=fetch_evidence(),
            universe_count=universe_count,
            reason=f"universe 종목 수 {universe_count} < 최소 {MIN_UNIVERSE_COUNT}(급감)",
            now=now_kst().isoformat()))
        raise RuntimeError(
            f"[BLOCKED] universe 종목 수 급감: {universe_count} < {MIN_UNIVERSE_COUNT} — 산출물 생성 중단")
    Q.write_status(Q.build_status(
        date_iso=iso_date, verdict="PASS", markets=markets, evidence=fetch_evidence(),
        universe_count=universe_count, reason="KOSPI·KOSDAQ 응답 검증 통과",
        now=now_kst().isoformat()))


def get_market_frame(base_date: str, market: str) -> pd.DataFrame:
    # 아래 4개 수집은 전부 검증·제한재시도를 거치며, 끝내 실패하면 KrxDataInvalid 를 올린다.
    # 여기서 잡아 빈 프레임으로 되돌리지 않는다(fail-closed) — 상위에서 그 거래일을 INVALID 로 처리한다.
    tickers = safe_get_ticker_list(base_date, market)

    base = pd.DataFrame({"symbol": [str(ticker).zfill(6) for ticker in tickers]})

    ohlcv = safe_get_ohlcv(base_date, market)
    ohlcv["symbol"] = ohlcv["symbol"].astype(str).str.zfill(6)

    fundamental = safe_get_fundamental(base_date, market)
    fundamental["symbol"] = fundamental["symbol"].astype(str).str.zfill(6)

    market_cap = safe_get_market_cap(base_date, market)
    market_cap["symbol"] = market_cap["symbol"].astype(str).str.zfill(6)

    merged = (
        base.merge(ohlcv, on="symbol", how="left")
        .merge(fundamental, on="symbol", how="left")
        .merge(market_cap, on="symbol", how="left")
    )

    names = []
    for symbol in merged["symbol"].tolist():
        try:
            names.append(stock.get_market_ticker_name(symbol))
        except Exception:
            names.append(symbol)
        time.sleep(0.005)

    merged["corpName"] = names
    merged["marketName"] = market

    # 업종명 조회 (pykrx get_market_sector_classifications)
    try:
        sector_df = stock.get_market_sector_classifications(base_date, market)
        # index = 종목코드(6자리), 컬럼 "업종명" 포함
        if sector_df is not None and not sector_df.empty and "업종명" in sector_df.columns:
            sector_map: dict[str, str] = sector_df["업종명"].to_dict()
            merged["industryName"] = merged["symbol"].map(lambda s: sector_map.get(str(s).zfill(6), ""))
            print(f"[INFO] 업종명 매핑 완료 ({market}): {merged['industryName'].ne('').sum()}건")
        else:
            merged["industryName"] = ""
            print(f"[WARN] 업종명 데이터 없음 ({market})")
    except Exception as error:
        merged["industryName"] = ""
        print(f"[WARN] 업종명 조회 실패 ({market}): {error}")

    return merged[
        [
            "symbol",
            "corpName",
            "marketName",
            "industryName",
            "price",
            "marketCap",
            "PER",
            "PBR",
            "DIV",
        ]
    ]


def load_news_sample_map(sample_path: Path) -> dict[str, dict]:
    if not sample_path.exists():
        print(f"[WARN] 뉴스 샘플 파일이 없습니다: {sample_path}")
        return {}

    try:
        payload = json.loads(sample_path.read_text(encoding="utf-8"))
    except Exception as error:
        print(f"[WARN] 뉴스 샘플 파일 읽기 실패: {error}")
        return {}

    items = payload.get("data", [])
    result: dict[str, dict] = {}

    for item in items:
        symbol = str(item.get("symbol", "")).zfill(6)
        if symbol:
            result[symbol] = item

    print(f"[INFO] 뉴스 샘플 로드 완료: {len(result)}건")
    return result


def http_get_json(base_url: str, params: dict[str, Any]) -> dict:
    query = urlencode(params)
    url = f"{base_url}?{query}"

    with urlopen(url, timeout=30) as response:
        raw = response.read().decode("utf-8")

    return json.loads(raw)


def try_parse_dart_xml_error(payload: bytes) -> str:
    try:
        text = payload.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""

    if not text:
        return ""

    try:
        root = ET.fromstring(text)
    except Exception:
        return text[:500]

    status = safe_text(root.findtext("status"))
    message = safe_text(root.findtext("message"))

    if status or message:
        return f"status={status}, message={message}"

    return text[:500]


def fetch_corp_code_map_from_dart(api_key: str) -> dict[str, dict]:
    if not api_key:
        raise RuntimeError(
            "DART_API_KEY 환경변수가 비어 있습니다. "
            "PowerShell에서 실제 OpenDART 인증키를 먼저 설정하세요."
        )

    url = f"{DART_CORP_CODE_URL}?{urlencode({'crtfc_key': api_key})}"

    with urlopen(url, timeout=60) as response:
        payload = response.read()
        content_type = response.headers.get_content_type()

    print(f"[INFO] OpenDART corpCode 응답 content-type: {content_type}")
    print(f"[INFO] OpenDART corpCode 응답 크기: {len(payload)} bytes")

    if not payload.startswith(b"PK"):
        error_text = try_parse_dart_xml_error(payload)
        raise RuntimeError(
            "OpenDART corpCode 응답이 zip 파일이 아닙니다.\n"
            f"응답 요약: {error_text}\n"
            "보통 원인은 다음 중 하나입니다.\n"
            "1) DART_API_KEY 오입력\n"
            "2) 사용 불가 키\n"
            "3) 허용되지 않은 IP\n"
            "4) OpenDART 측 에러 응답"
        )

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        xml_name = archive.namelist()[0]
        xml_bytes = archive.read(xml_name)

    root = ET.fromstring(xml_bytes)
    result: dict[str, dict] = {}

    for item in root.findall("list"):
        stock_code = safe_text(item.findtext("stock_code"))
        corp_code = safe_text(item.findtext("corp_code"))
        corp_name = safe_text(item.findtext("corp_name"))
        modify_date = safe_text(item.findtext("modify_date"))

        if len(stock_code) != 6 or not corp_code:
            continue

        result[stock_code] = {
            "corp_code": corp_code,
            "corp_name": corp_name,
            "modify_date": modify_date,
        }

    return result


def load_or_fetch_corp_code_map(api_key: str) -> dict[str, dict]:
    ensure_cache_dirs()

    if CORP_CODE_CACHE_PATH.exists():
        try:
            cached = json.loads(CORP_CODE_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached:
                print(f"[INFO] corp code cache 사용: {CORP_CODE_CACHE_PATH}")
                return cached
        except Exception:
            pass

    print("[INFO] OpenDART corp code 다운로드 시작")
    corp_map = fetch_corp_code_map_from_dart(api_key)
    CORP_CODE_CACHE_PATH.write_text(
        json.dumps(corp_map, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] corp code cache 저장 완료: {len(corp_map)}건")
    return corp_map


def statement_cache_path(corp_code: str, year: int, fs_div: str) -> Path:
    return STATEMENT_CACHE_DIR / f"{corp_code}_{year}_{fs_div}.json"


def fetch_statement_rows(
    api_key: str,
    corp_code: str,
    year: int,
    fs_div: str,
) -> list[dict]:
    ensure_cache_dirs()
    cache_path = statement_cache_path(corp_code, year, fs_div)

    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, list):
                return cached
        except Exception:
            pass

    payload = http_get_json(
        DART_FINANCIAL_STATEMENT_URL,
        {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": ANNUAL_REPORT_CODE,
            "fs_div": fs_div,
        },
    )

    status = safe_text(payload.get("status"))
    message = safe_text(payload.get("message"))

    if status == "000":
        rows = payload.get("list", [])
        if not isinstance(rows, list):
            rows = []
        cache_path.write_text(
            json.dumps(rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return rows

    if status in {"013", "014"}:
        cache_path.write_text("[]", encoding="utf-8")
        return []

    if status == "020":
        raise RuntimeError("OpenDART 요청 제한을 초과했습니다. 잠시 후 다시 실행하세요.")

    if status == "901":
        raise RuntimeError("OpenDART API 키 보유기간이 만료되었습니다. 새 키를 발급받아야 합니다.")

    raise RuntimeError(
        f"OpenDART 재무제표 조회 실패: corp_code={corp_code}, year={year}, fs_div={fs_div}, "
        f"status={status}, message={message}"
    )


def choose_statement_rows(api_key: str, corp_code: str, year: int) -> tuple[list[dict], str]:
    for fs_div in ("CFS", "OFS"):
        rows = fetch_statement_rows(api_key, corp_code, year, fs_div)
        if rows:
            return rows, fs_div

        time.sleep(DART_REQUEST_SLEEP_SEC)

    return [], ""


def normalize_account_text(value: str) -> str:
    text = safe_text(value).lower()
    text = text.replace(" ", "")
    text = text.replace("-", "")
    text = text.replace("_", "")
    text = text.replace("(", "")
    text = text.replace(")", "")
    return text


def pick_amount(row: dict) -> float | None:
    for key in (
        "thstrm_amount",
        "thstrm_add_amount",
        "frmtrm_amount",
        "frmtrm_add_amount",
    ):
        value = safe_number(row.get(key))
        if value is not None:
            return value
    return None


def build_statement_value_map(rows: list[dict]) -> dict[str, float]:
    result: dict[str, float] = {}

    for row in rows:
        account_nm = normalize_account_text(row.get("account_nm"))
        account_id = normalize_account_text(row.get("account_id"))
        amount = pick_amount(row)

        if amount is None:
            continue

        for key in (account_id, account_nm):
            if key and key not in result:
                result[key] = amount

    return result


def find_first_value(value_map: dict[str, float], candidates: list[str]) -> float | None:
    for candidate in candidates:
        key = normalize_account_text(candidate)
        if key in value_map:
            return value_map[key]
    return None


def extract_financial_metrics_from_rows(rows: list[dict]) -> dict[str, float | None]:
    value_map = build_statement_value_map(rows)

    revenue = find_first_value(
        value_map,
        [
            "ifrs-full_Revenue",
            "ifrs-full_GrossProfit",
            "매출액",
            "수익(매출액)",
            "영업수익",
        ],
    )

    operating_income = find_first_value(
        value_map,
        [
            "dart_OperatingIncomeLoss",
            "ifrs-full_ProfitLossFromOperatingActivities",
            "영업이익",
            "영업이익(손실)",
        ],
    )

    net_income = find_first_value(
        value_map,
        [
            "ifrs-full_ProfitLoss",
            "당기순이익",
            "당기순이익(손실)",
            "연결당기순이익",
            "분기순이익",
        ],
    )

    equity = find_first_value(
        value_map,
        [
            "ifrs-full_Equity",
            "자본총계",
        ],
    )

    liabilities = find_first_value(
        value_map,
        [
            "ifrs-full_Liabilities",
            "부채총계",
        ],
    )

    basic_eps = find_first_value(
        value_map,
        [
            "ifrs-full_BasicEarningsLossPerShare",
            "dart_BasicEarningsLossPerShare",
            "기본주당이익",
            "기본주당순이익",
        ],
    )

    return {
        "revenue": revenue,
        "operatingIncome": operating_income,
        "netIncome": net_income,
        "equity": equity,
        "liabilities": liabilities,
        "basicEps": basic_eps,
    }


def compute_growth_percent(current_value: float | None, previous_value: float | None) -> float | None:
    current = safe_number(current_value)
    previous = safe_number(previous_value)

    if current is None or previous is None:
        return None

    if previous == 0:
        return None

    return round(((current - previous) / abs(previous)) * 100, 2)


def compute_margin_percent(numerator: float | None, denominator: float | None) -> float | None:
    num = safe_number(numerator)
    den = safe_number(denominator)

    if num is None or den is None or den == 0:
        return None

    return round((num / den) * 100, 2)


def compute_cagr_percent(current_value: float | None, past_value: float | None, years: int) -> float | None:
    current = safe_number(current_value)
    past = safe_number(past_value)

    if current is None or past is None:
        return None

    if current <= 0 or past <= 0 or years <= 0:
        return None

    return round((((current / past) ** (1 / years)) - 1) * 100, 2)


def build_dart_financial_map(symbols: list[str], base_date: str) -> dict[str, dict]:
    if not DART_API_KEY:
        raise RuntimeError(
            "DART_API_KEY 환경변수가 없습니다. "
            "PowerShell에서 먼저 설정한 뒤 실행하세요."
        )

    corp_code_map = load_or_fetch_corp_code_map(DART_API_KEY)
    latest_year_candidate = int(base_date[:4]) - 1

    if DART_MAX_TICKERS > 0:
        symbols = symbols[:DART_MAX_TICKERS]
        print(f"[INFO] DART_MAX_TICKERS 적용: {len(symbols)}건")

    result: dict[str, dict] = {}
    processed = 0

    for symbol in symbols:
        corp_meta = corp_code_map.get(symbol)

        if not corp_meta:
            result[symbol] = {}
            continue

        corp_code = safe_text(corp_meta.get("corp_code"))
        yearly_metrics: dict[int, dict[str, float | None]] = {}

        for year in range(latest_year_candidate, latest_year_candidate - DART_LOOKBACK_YEARS, -1):
            rows, fs_div = choose_statement_rows(DART_API_KEY, corp_code, year)

            if rows:
                yearly_metrics[year] = extract_financial_metrics_from_rows(rows)
                yearly_metrics[year]["fsDiv"] = fs_div

            time.sleep(DART_REQUEST_SLEEP_SEC)

        latest_year = None
        for year in sorted(yearly_metrics.keys(), reverse=True):
            metrics = yearly_metrics[year]
            if (
                metrics.get("revenue") is not None
                and metrics.get("equity") is not None
                and metrics.get("netIncome") is not None
            ):
                latest_year = year
                break

        if latest_year is None:
            result[symbol] = {
                "dartYears": sorted(yearly_metrics.keys(), reverse=True),
                "dartFsDiv": "",
            }
            processed += 1
            if processed % 50 == 0:
                print(f"[INFO] DART 처리 진행: {processed}/{len(symbols)}")
            continue

        current = yearly_metrics.get(latest_year, {})
        prev_1 = yearly_metrics.get(latest_year - 1, {})
        prev_3 = yearly_metrics.get(latest_year - 3, {})

        result[symbol] = {
            "ROE": compute_margin_percent(current.get("netIncome"), current.get("equity")),
            "salesGrowth": compute_growth_percent(current.get("revenue"), prev_1.get("revenue")),
            "opIncomeGrowth": compute_growth_percent(
                current.get("operatingIncome"),
                prev_1.get("operatingIncome"),
            ),
            "debtRatio": compute_margin_percent(current.get("liabilities"), current.get("equity")),
            "opMargin": compute_margin_percent(current.get("operatingIncome"), current.get("revenue")),
            "netMargin": compute_margin_percent(current.get("netIncome"), current.get("revenue")),
            "salesCagr3Y": compute_cagr_percent(current.get("revenue"), prev_3.get("revenue"), 3),
            "EPSGrowth3Y": compute_cagr_percent(current.get("basicEps"), prev_3.get("basicEps"), 3),
            "dartLatestYear": latest_year,
            "dartYears": sorted(yearly_metrics.keys(), reverse=True),
            "dartFsDiv": current.get("fsDiv", ""),
        }

        processed += 1
        if processed % 50 == 0:
            print(f"[INFO] DART 처리 진행: {processed}/{len(symbols)}")

    return result


def build_item(
    row,
    dart_financial_map: dict[str, dict],
    news_sample_map: dict[str, dict],
    updated_at: str,
) -> dict:
    symbol = str(row["symbol"]).zfill(6)
    financial_real = dart_financial_map.get(symbol, {})
    news_sample = news_sample_map.get(symbol, {})

    corp_name = safe_text(row["corpName"])
    market_name = safe_text(row["marketName"])
    industry_name = safe_text(row["industryName"])

    return {
        "symbol": symbol,
        "corpName": corp_name,
        "marketName": market_name,
        "industryName": industry_name,
        "price": safe_number(row["price"]),
        "marketCap": safe_number(row["marketCap"]),
        "PER": safe_number(row["PER"]),
        "PBR": safe_number(row["PBR"]),
        "ROE": safe_number(financial_real.get("ROE")),
        "salesGrowth": safe_number(financial_real.get("salesGrowth")),
        "opIncomeGrowth": safe_number(financial_real.get("opIncomeGrowth")),
        "debtRatio": safe_number(financial_real.get("debtRatio")),
        "divYield": safe_number(row["DIV"]),
        "opMargin": safe_number(financial_real.get("opMargin")),
        "netMargin": safe_number(financial_real.get("netMargin")),
        "salesCagr3Y": safe_number(financial_real.get("salesCagr3Y")),
        "EPSGrowth3Y": safe_number(financial_real.get("EPSGrowth3Y")),
        "dartLatestYear": financial_real.get("dartLatestYear"),
        "dartYears": financial_real.get("dartYears", []),
        "dartFsDiv": financial_real.get("dartFsDiv", ""),
        "newsMomentumScore": safe_number(news_sample.get("newsMomentumScore")) or 0,
        "hypothesis": safe_text(news_sample.get("hypothesis")),
        "evidence": safe_string_list(news_sample.get("evidence")),
        "risk": safe_string_list(news_sample.get("risk")),
        "updatedAt": updated_at,
    }


def build_payload() -> dict:
    base_date = find_latest_business_day()
    updated_at = f"{base_date[:4]}-{base_date[4:6]}-{base_date[6:8]}"

    news_sample_map = load_news_sample_map(NEWS_SAMPLE_PATH)

    # KOSPI·KOSDAQ 을 독립 판정하되, 공식 산출물에는 두 시장 모두 PASS 가 필요하다(계약 B).
    # 하나라도 INVALID 면 그 거래일 품질을 INVALID 로 기록하고 예외를 올려
    # ranking·signal·apply·public 이 전부 진행되지 않게 한다(계약 A, fail-closed).
    frames, markets, iso_date = collect_markets_with_quality(base_date, updated_at)

    kospi, kosdaq = frames["KOSPI"], frames["KOSDAQ"]
    merged = pd.concat([kospi, kosdaq], ignore_index=True)

    merged["marketCap"] = merged["marketCap"].apply(safe_number)
    merged = merged.sort_values(by="marketCap", ascending=False, na_position="last").reset_index(drop=True)

    symbols = merged["symbol"].astype(str).str.zfill(6).tolist()
    dart_financial_map = build_dart_financial_map(symbols, base_date)

    items = []
    enriched_count = 0

    for _, row in merged.iterrows():
        item = build_item(row, dart_financial_map, news_sample_map, updated_at)

        if (
            item["ROE"] is not None
            or item["salesGrowth"] is not None
            or item["opMargin"] is not None
        ):
            enriched_count += 1

        items.append(item)

    # 종목 수 급감도 품질 결함으로 본다(부분 산출물 공개 금지).
    record_universe_quality(iso_date, markets, len(items))

    return {
        "data": items,
        "meta": {
            "provider": "pykrx-market-snapshot+opendart-financials+news-sample-merge",
            "version": 9,
            "count": len(items),
            "baseDate": updated_at,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "enrichedCount": enriched_count,
            "newsSampleCount": len(news_sample_map),
            "dartApiConfigured": bool(DART_API_KEY),
            "dartMaxTickers": DART_MAX_TICKERS,
            "dartRequestSleepSec": DART_REQUEST_SLEEP_SEC,
        },
    }


def main():
    ensure_cache_dirs()
    payload = build_payload()

    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"written: {OUTPUT_PATH}")
    print(f"count: {payload['meta']['count']}")
    print(f"baseDate: {payload['meta']['baseDate']}")
    print(f"enrichedCount: {payload['meta']['enrichedCount']}")
    print(f"newsSampleCount: {payload['meta']['newsSampleCount']}")
    print(f"dartApiConfigured: {payload['meta']['dartApiConfigured']}")
    print(f"dartMaxTickers: {payload['meta']['dartMaxTickers']}")


if __name__ == "__main__":
    main()