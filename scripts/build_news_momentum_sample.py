import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

RAW_NEWS_REAL_PATH = ROOT / "news-raw-real.json"
RAW_NEWS_SAMPLE_PATH = ROOT / "news-raw-sample.json"
OUTPUT_PATH = ROOT / "news-momentum-sample.json"


THEME_RULES = [
    {
        "keywords": ["AI", "인공지능", "HBM", "서버", "데이터센터", "반도체", "메모리"],
        "theme": "AI·반도체 수요",
        "sales_effect": "AI 서버와 데이터센터 투자 흐름이 이어지면 관련 제품 수요가 유지될 가능성이 있습니다.",
        "profit_effect": "고부가 제품 비중이 확대되면 6개월 안에 영업이익 개선 기대가 생길 수 있습니다.",
    },
    {
        "keywords": ["수주", "계약", "납품", "공급", "방산", "조선", "플랜트"],
        "theme": "수주·계약",
        "sales_effect": "수주와 공급 계약은 향후 매출 인식으로 이어질 수 있습니다.",
        "profit_effect": "수익성이 확보된 계약이라면 6개월 뒤 실적 기대를 높일 수 있습니다.",
    },
    {
        "keywords": ["수출", "해외", "미국", "유럽", "중국", "동남아", "글로벌"],
        "theme": "수출 확대",
        "sales_effect": "해외 판매 확대가 이어지면 매출 성장 가능성이 있습니다.",
        "profit_effect": "환율과 원가 부담이 크지 않다면 이익 증가로 연결될 수 있습니다.",
    },
    {
        "keywords": ["신제품", "출시", "신기술", "개발", "상용화", "인증"],
        "theme": "신제품·신기술",
        "sales_effect": "신제품과 기술 상용화가 실제 판매로 이어지는지 확인할 필요가 있습니다.",
        "profit_effect": "추가 설비투자 부담이 작다면 이익률 개선 가능성이 있습니다.",
    },
    {
        "keywords": ["소비", "판매", "주문", "예약", "고객", "브랜드", "관심"],
        "theme": "소비자 관심",
        "sales_effect": "소비자와 구매자 관심이 실제 판매량 증가로 이어지는지가 핵심입니다.",
        "profit_effect": "마케팅 비용보다 매출 증가 폭이 크면 이익 개선 가능성이 있습니다.",
    },
    {
        "keywords": ["주주환원", "배당", "자사주"],
        "theme": "주주환원",
        "sales_effect": "주주환원 이슈는 직접적인 매출 증가 요인은 아닙니다.",
        "profit_effect": "현금흐름과 이익 체력이 유지되는지 함께 봐야 합니다.",
    },
]

RISK_KEYWORDS = {
    "관세": "관세 부담",
    "둔화": "수요 둔화",
    "하락": "가격 하락",
    "적자": "적자 가능성",
    "비용": "비용 증가",
    "소송": "소송 리스크",
    "제재": "제재 리스크",
    "부진": "실적 부진",
    "경쟁": "경쟁 심화",
    "감소": "매출 또는 이익 감소",
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict):
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def select_raw_news_path() -> Path:
    if RAW_NEWS_REAL_PATH.exists():
        print(f"[INFO] 실제 뉴스 원문 사용: {RAW_NEWS_REAL_PATH}")
        return RAW_NEWS_REAL_PATH

    print(f"[INFO] 샘플 뉴스 원문 사용: {RAW_NEWS_SAMPLE_PATH}")
    return RAW_NEWS_SAMPLE_PATH


def detect_themes(text: str) -> list[dict]:
    matched = []

    for rule in THEME_RULES:
        if any(keyword in text for keyword in rule["keywords"]):
            matched.append(rule)

    return matched


def detect_risks(text: str) -> list[str]:
    risks = []

    for keyword, label in RISK_KEYWORDS.items():
        if keyword in text:
            risks.append(label)

    return list(dict.fromkeys(risks))


def build_hypothesis(corp_name: str, themes: list[dict]) -> str:
    if not themes:
        return f"{corp_name} 관련 6개월 실적 전망을 판단할 만한 뉴스 흐름이 아직 충분하지 않습니다."

    primary = themes[0]

    return (
        f"{primary['theme']} 이슈가 확인됩니다. "
        f"{primary['sales_effect']} "
        f"{primary['profit_effect']}"
    )


def build_outlook(themes: list[dict]) -> str:
    if not themes:
        return "6개월 전망을 판단할 만한 뉴스 흐름이 아직 충분하지 않습니다."

    primary = themes[0]

    return (
        f"6개월 관점에서는 {primary['theme']} 흐름이 실제 매출과 영업이익으로 연결되는지 확인해야 합니다."
    )


def build_entry_signal(themes: list[dict], evidence: list[str], risks: list[str]) -> str:
    if not themes:
        return "관찰 필요"

    if len(evidence) >= 2 and len(risks) == 0:
        return "진입 검토 가능"

    if len(evidence) >= 1:
        return "조건 확인 중"

    return "관찰 필요"


def build_profit_scenario_rate(themes: list[dict], risks: list[str]) -> int:
    if not themes:
        return 0

    base = 8

    if len(themes) >= 2:
        base += 4

    if any(theme["theme"] in ["AI·반도체 수요", "수주·계약", "수출 확대"] for theme in themes):
        base += 3

    if risks:
        base -= min(6, len(risks) * 2)

    return max(0, min(20, base))


def build_payload() -> dict:
    raw_news_path = select_raw_news_path()
    raw_payload = read_json(raw_news_path)
    raw_items = raw_payload.get("data", [])

    items = []

    for raw in raw_items:
        symbol = str(raw.get("symbol", "")).zfill(6)
        corp_name = normalize_text(raw.get("corpName"))
        articles = raw.get("articles", [])

        evidence: list[str] = []
        risk: list[str] = []
        article_summaries: list[dict] = []
        all_text_parts: list[str] = []

        for article in articles:
            title = normalize_text(article.get("title"))
            summary = normalize_text(article.get("summary"))
            source = normalize_text(article.get("source"))
            published_at = normalize_text(article.get("publishedAt"))
            url = normalize_text(article.get("url"))
            sentiment = normalize_text(article.get("sentiment")) or "neutral"

            if title:
                evidence.append(title)

            all_text_parts.append(title)
            all_text_parts.append(summary)

            article_summaries.append(
                {
                    "title": title,
                    "summary": summary,
                    "source": source,
                    "publishedAt": published_at,
                    "url": url,
                    "sentiment": sentiment,
                }
            )

        combined_text = " ".join(all_text_parts)
        themes = detect_themes(combined_text)
        detected_risks = detect_risks(combined_text)
        profit_scenario_rate = build_profit_scenario_rate(themes, detected_risks)

        news_score = min(15, len(themes) * 4 + min(len(evidence), 3))

        item = {
            "symbol": symbol,
            "newsMomentumScore": news_score,
            "hypothesis": build_hypothesis(corp_name, themes),
            "outlook6M": build_outlook(themes),
            "entrySignal": build_entry_signal(themes, evidence, detected_risks),
            "profitScenarioRate6M": profit_scenario_rate,
            "themes": [theme["theme"] for theme in themes],
            "evidence": evidence[:3],
            "risk": detected_risks[:3],
            "articleSummaries": article_summaries,
        }

        items.append(item)

    return {
        "data": items,
        "meta": {
            "provider": "rule-based-news-outlook-entry-builder",
            "version": 5,
            "count": len(items),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "sourceFile": str(raw_news_path),
        },
    }


def main():
    payload = build_payload()
    write_json(OUTPUT_PATH, payload)

    print(f"written: {OUTPUT_PATH}")
    print(f"count: {payload['meta']['count']}")
    print(f"provider: {payload['meta']['provider']}")
    print(f"sourceFile: {payload['meta']['sourceFile']}")


if __name__ == "__main__":
    main()