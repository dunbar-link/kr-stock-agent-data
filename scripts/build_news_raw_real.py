import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]

UNIVERSE_PATH = ROOT / "financial-universe-real.json"
OUTPUT_PATH = ROOT / "news-raw-real.json"

MAX_COMPANIES = 80
MAX_ARTICLES_PER_COMPANY = 5
REQUEST_SLEEP_SEC = 0.4


def normalize_text(value) -> str:
    if value is None:
        return ""

    text = str(value)
    text = unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def read_json(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"파일이 없습니다: {path}")

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict):
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_google_news_rss_url(query: str) -> str:
    encoded = quote(query)
    return (
        "https://news.google.com/rss/search"
        f"?q={encoded}"
        "&hl=ko"
        "&gl=KR"
        "&ceid=KR:ko"
    )


def fetch_rss_items(query: str) -> list[dict]:
    url = build_google_news_rss_url(query)

    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    )

    try:
        with urlopen(request, timeout=20) as response:
            raw = response.read()
    except Exception as error:
        print(f"[WARN] 뉴스 RSS 조회 실패: {query} / {error}")
        return []

    try:
        root = ET.fromstring(raw)
    except Exception as error:
        print(f"[WARN] 뉴스 RSS XML 파싱 실패: {query} / {error}")
        return []

    items = []

    for item in root.findall("./channel/item"):
        title = normalize_text(item.findtext("title"))
        link = normalize_text(item.findtext("link"))
        pub_date = normalize_text(item.findtext("pubDate"))
        description = normalize_text(item.findtext("description"))
        source_node = item.find("source")
        source = normalize_text(source_node.text if source_node is not None else "Google News")

        if not title:
            continue

        items.append(
            {
                "title": title,
                "summary": description,
                "source": source,
                "publishedAt": pub_date,
                "url": link,
                "sentiment": "neutral",
            }
        )

    return items


def select_target_companies(universe_payload: dict) -> list[dict]:
    data = universe_payload.get("data", [])

    valid_items = []

    for item in data:
        symbol = str(item.get("symbol", "")).zfill(6)
        corp_name = normalize_text(item.get("corpName"))
        market_cap = item.get("marketCap")

        if not symbol or not corp_name:
            continue

        try:
            market_cap_number = float(market_cap)
        except Exception:
            market_cap_number = 0

        valid_items.append(
            {
                "symbol": symbol,
                "corpName": corp_name,
                "marketCap": market_cap_number,
            }
        )

    valid_items.sort(key=lambda item: item["marketCap"], reverse=True)

    return valid_items[:MAX_COMPANIES]


def build_company_news(company: dict) -> dict:
    symbol = company["symbol"]
    corp_name = company["corpName"]

    query = f'"{corp_name}" 주식 실적 매출 영업이익 수주 투자'

    articles = fetch_rss_items(query)
    articles = articles[:MAX_ARTICLES_PER_COMPANY]

    return {
        "symbol": symbol,
        "corpName": corp_name,
        "articles": articles,
    }


def build_payload() -> dict:
    universe_payload = read_json(UNIVERSE_PATH)
    companies = select_target_companies(universe_payload)

    print(f"[INFO] 뉴스 수집 대상 기업: {len(companies)}건")

    items = []

    for index, company in enumerate(companies, start=1):
        print(f"[INFO] 뉴스 수집: {index}/{len(companies)} {company['corpName']}")
        item = build_company_news(company)
        items.append(item)
        time.sleep(REQUEST_SLEEP_SEC)

    return {
        "data": items,
        "meta": {
            "provider": "google-news-rss-real",
            "version": 1,
            "count": len(items),
            "maxCompanies": MAX_COMPANIES,
            "maxArticlesPerCompany": MAX_ARTICLES_PER_COMPANY,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        },
    }


def main():
    payload = build_payload()
    write_json(OUTPUT_PATH, payload)

    print(f"written: {OUTPUT_PATH}")
    print(f"count: {payload['meta']['count']}")
    print(f"provider: {payload['meta']['provider']}")


if __name__ == "__main__":
    main()