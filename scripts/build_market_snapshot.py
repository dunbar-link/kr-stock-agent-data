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


def safe_get_ohlcv(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_ohlcv_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "price"])

        return frame.rename(
            columns={
                "티커": "symbol",
                "종가": "price",
            }
        )[["symbol", "price"]]
    except Exception as error:
        print(f"[WARN] OHLCV 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "price"])


def safe_get_fundamental(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_fundamental_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "PER", "PBR", "DIV"])

        return frame.rename(
            columns={
                "티커": "symbol",
                "PER": "PER",
                "PBR": "PBR",
                "DIV": "DIV",
            }
        )[["symbol", "PER", "PBR", "DIV"]]
    except Exception as error:
        print(f"[WARN] 펀더멘털 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "PER", "PBR", "DIV"])


def safe_get_market_cap(base_date: str, market: str) -> pd.DataFrame:
    try:
        frame = stock.get_market_cap_by_ticker(base_date, market=market).reset_index()
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["symbol", "marketCap"])

        frame = frame.rename(
            columns={
                "티커": "symbol",
                "시가총액": "marketCap",
            }
        )[["symbol", "marketCap"]]

        frame["marketCap"] = frame["marketCap"].apply(normalize_market_cap_to_billion_krw)
        return frame
    except Exception as error:
        print(f"[WARN] 시가총액 조회 실패 ({market}): {error}")
        return pd.DataFrame(columns=["symbol", "marketCap"])


def safe_get_ticker_list(base_date: str, market: str) -> list[str]:
    try:
        tickers = stock.get_market_ticker_list(base_date, market=market)
        if tickers:
            return tickers
        return []
    except Exception as error:
        print(f"[WARN] 티커 목록 조회 실패 ({market}): {error}")
        return []


def get_market_frame(base_date: str, market: str) -> pd.DataFrame:
    tickers = safe_get_ticker_list(base_date, market)

    if not tickers:
        return pd.DataFrame(
            columns=[
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
        )

    base = pd.DataFrame({"symbol": [str(ticker).zfill(6) for ticker in tickers]})

    ohlcv = safe_get_ohlcv(base_date, market)
    if not ohlcv.empty:
        ohlcv["symbol"] = ohlcv["symbol"].astype(str).str.zfill(6)

    fundamental = safe_get_fundamental(base_date, market)
    if not fundamental.empty:
        fundamental["symbol"] = fundamental["symbol"].astype(str).str.zfill(6)

    market_cap = safe_get_market_cap(base_date, market)
    if not market_cap.empty:
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

    kospi = get_market_frame(base_date, "KOSPI")
    kosdaq = get_market_frame(base_date, "KOSDAQ")
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