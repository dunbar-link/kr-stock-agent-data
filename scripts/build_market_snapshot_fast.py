import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from build_market_snapshot import (
    OUTPUT_PATH,
    NEWS_SAMPLE_PATH,
    build_dart_financial_map,
    build_item,
    find_latest_business_day,
    get_market_frame,
    load_news_sample_map,
    safe_number,
)


ROOT = Path(__file__).resolve().parents[1]

MAX_DART_REFRESH_PER_RUN = int(os.environ.get("MAX_DART_REFRESH_PER_RUN", "80"))
DART_STALE_DAYS = int(os.environ.get("DART_STALE_DAYS", "100"))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_kst_date() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


def load_existing_payload() -> dict:
    if not OUTPUT_PATH.exists():
        print("[WARN] 기존 financial-universe-real.json 이 없습니다.")
        return {"data": [], "meta": {}}

    try:
        return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception as error:
        print(f"[WARN] 기존 JSON 읽기 실패: {error}")
        return {"data": [], "meta": {}}


def build_existing_financial_map(existing_payload: dict) -> dict[str, dict]:
    result: dict[str, dict] = {}

    for item in existing_payload.get("data", []):
        symbol = str(item.get("symbol", "")).zfill(6)
        if not symbol:
            continue

        result[symbol] = {
            "ROE": safe_number(item.get("ROE")),
            "salesGrowth": safe_number(item.get("salesGrowth")),
            "opIncomeGrowth": safe_number(item.get("opIncomeGrowth")),
            "debtRatio": safe_number(item.get("debtRatio")),
            "opMargin": safe_number(item.get("opMargin")),
            "netMargin": safe_number(item.get("netMargin")),
            "salesCagr3Y": safe_number(item.get("salesCagr3Y")),
            "EPSGrowth3Y": safe_number(item.get("EPSGrowth3Y")),
            "dartLatestYear": item.get("dartLatestYear"),
            "dartYears": item.get("dartYears", []),
            "dartFsDiv": item.get("dartFsDiv", ""),
            "dartUpdatedAt": item.get("dartUpdatedAt") or item.get("updatedAt"),
        }

    return result


def parse_date(value) -> datetime | None:
    if not value:
        return None

    text = str(value)

    try:
        if "T" in text:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        return datetime.fromisoformat(text)
    except Exception:
        return None


def is_missing_core_financials(financial: dict) -> bool:
    return (
        financial.get("ROE") is None
        or financial.get("salesGrowth") is None
        or financial.get("opMargin") is None
        or financial.get("debtRatio") is None
    )


def is_stale_financials(financial: dict) -> bool:
    updated_at = parse_date(financial.get("dartUpdatedAt"))

    if updated_at is None:
        return True

    age_days = (now_kst_date().date() - updated_at.date()).days
    return age_days >= DART_STALE_DAYS


def select_dart_refresh_symbols(
    symbols: list[str],
    existing_financial_map: dict[str, dict],
) -> list[str]:
    candidates: list[str] = []

    for symbol in symbols:
        financial = existing_financial_map.get(symbol)

        if financial is None:
            candidates.append(symbol)
            continue

        if is_missing_core_financials(financial):
            candidates.append(symbol)
            continue

        if is_stale_financials(financial):
            candidates.append(symbol)
            continue

    return candidates[:MAX_DART_REFRESH_PER_RUN]


def merge_financial_maps(
    existing_financial_map: dict[str, dict],
    refreshed_financial_map: dict[str, dict],
) -> dict[str, dict]:
    merged = dict(existing_financial_map)
    refreshed_at = now_utc_iso()

    for symbol, financial in refreshed_financial_map.items():
        next_financial = dict(financial)
        next_financial["dartUpdatedAt"] = refreshed_at
        merged[symbol] = next_financial

    return merged


def build_payload() -> dict:
    existing_payload = load_existing_payload()
    existing_financial_map = build_existing_financial_map(existing_payload)

    base_date = find_latest_business_day()
    updated_at = f"{base_date[:4]}-{base_date[4:6]}-{base_date[6:8]}"

    news_sample_map = load_news_sample_map(NEWS_SAMPLE_PATH)

    kospi = get_market_frame(base_date, "KOSPI")
    kosdaq = get_market_frame(base_date, "KOSDAQ")
    merged = pd.concat([kospi, kosdaq], ignore_index=True)

    merged["marketCap"] = merged["marketCap"].apply(safe_number)
    merged = merged.sort_values(
        by="marketCap",
        ascending=False,
        na_position="last",
    ).reset_index(drop=True)

    symbols = merged["symbol"].astype(str).str.zfill(6).tolist()

    refresh_symbols = select_dart_refresh_symbols(
        symbols=symbols,
        existing_financial_map=existing_financial_map,
    )

    print(f"[INFO] 기존 DART 재무 데이터: {len(existing_financial_map)}건")
    print(f"[INFO] 이번 실행 DART 선택 업데이트 대상: {len(refresh_symbols)}건")
    print(f"[INFO] MAX_DART_REFRESH_PER_RUN: {MAX_DART_REFRESH_PER_RUN}")
    print(f"[INFO] DART_STALE_DAYS: {DART_STALE_DAYS}")

    if refresh_symbols:
        refreshed_financial_map = build_dart_financial_map(refresh_symbols, base_date)
    else:
        refreshed_financial_map = {}

    dart_financial_map = merge_financial_maps(
        existing_financial_map=existing_financial_map,
        refreshed_financial_map=refreshed_financial_map,
    )

    items = []
    enriched_count = 0

    for _, row in merged.iterrows():
        item = build_item(
            row=row,
            dart_financial_map=dart_financial_map,
            news_sample_map=news_sample_map,
            updated_at=updated_at,
        )

        financial = dart_financial_map.get(item["symbol"], {})
        if financial.get("dartUpdatedAt"):
            item["dartUpdatedAt"] = financial.get("dartUpdatedAt")

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
            "provider": "pykrx-daily-fast+cached-opendart-financials+selective-dart-refresh",
            "version": 1,
            "count": len(items),
            "baseDate": updated_at,
            "updatedAt": now_utc_iso(),
            "enrichedCount": enriched_count,
            "newsSampleCount": len(news_sample_map),
            "existingFinancialCount": len(existing_financial_map),
            "dartRefreshCount": len(refresh_symbols),
            "maxDartRefreshPerRun": MAX_DART_REFRESH_PER_RUN,
            "dartStaleDays": DART_STALE_DAYS,
        },
    }


def main():
    payload = build_payload()

    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"written: {OUTPUT_PATH}")
    print(f"count: {payload['meta']['count']}")
    print(f"baseDate: {payload['meta']['baseDate']}")
    print(f"enrichedCount: {payload['meta']['enrichedCount']}")
    print(f"existingFinancialCount: {payload['meta']['existingFinancialCount']}")
    print(f"dartRefreshCount: {payload['meta']['dartRefreshCount']}")
    print(f"maxDartRefreshPerRun: {payload['meta']['maxDartRefreshPerRun']}")
    print(f"dartStaleDays: {payload['meta']['dartStaleDays']}")


if __name__ == "__main__":
    main()