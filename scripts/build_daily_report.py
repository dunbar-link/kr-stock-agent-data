import json
from pathlib import Path
from datetime import datetime


ROOT_DIR = Path(__file__).resolve().parents[1]

RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
DAILY_REPORT_TXT_PATH = ROOT_DIR / "daily-report.txt"
DAILY_REPORT_JSON_PATH = ROOT_DIR / "daily-report.json"


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_json(path):
    if not path.exists():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_text(path, text):
    with path.open("w", encoding="utf-8") as f:
        f.write(text)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def safe_number(value, default=0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def get_name(item):
    return str(
        item.get("name")
        or item.get("stockName")
        or item.get("companyName")
        or item.get("corpName")
        or item.get("code")
        or ""
    ).strip()


def get_code(item):
    return str(
        item.get("code")
        or item.get("stockCode")
        or item.get("ticker")
        or item.get("symbol")
        or ""
    ).strip()


def get_reason(item):
    return str(item.get("reason") or "").strip()


def format_score(value):
    number = safe_number(value)

    if number == int(number):
        return str(int(number))

    return f"{number:.1f}"


def format_item_line(item, show_reason=True):
    name = get_name(item)
    code = get_code(item)

    score = format_score(item.get("score"))
    financial_score = format_score(item.get("financialScore"))
    news_score = format_score(item.get("newsScore"))

    label = name

    if code:
        label = f"{name} ({code})"

    line = f"- {label} | 총점 {score} / 재무 {financial_score} / 뉴스 {news_score}"

    reason = get_reason(item)

    if show_reason and reason:
        line += f"\n  이유: {reason}"

    return line


def build_section(title, items, show_reason=True):
    lines = []

    lines.append(f"▶ {title}")

    if not items:
        lines.append("- 해당 없음")
        return "\n".join(lines)

    for item in items:
        lines.append(format_item_line(item, show_reason=show_reason))

    return "\n".join(lines)


def sort_buy_items(items):
    return sorted(
        items,
        key=lambda item: safe_number(item.get("score")),
        reverse=True,
    )


def sort_hold_items(items):
    return sorted(
        items,
        key=lambda item: safe_number(item.get("score")),
        reverse=True,
    )


def sort_sell_items(items):
    return sorted(
        items,
        key=lambda item: safe_number(item.get("score")),
    )


def build_report(history):
    base_date = history.get("baseDate") or datetime.now().strftime("%Y-%m-%d")

    buy_candidates = sort_buy_items(history.get("buyCandidates") or [])
    hold_candidates = sort_hold_items(history.get("holdCandidates") or [])
    sell_candidates = sort_sell_items(history.get("sellCandidates") or [])

    # 시세 최신성(WABABA-PRICE-ASOF-STALE-GATE-20260720) — recommendation-history 에서 그대로 전달.
    # 없으면(과거 fixture) 표시를 생략해 기존 호환을 유지한다.
    price_as_of = history.get("priceAsOf")
    price_status = history.get("priceFreshnessStatus")
    price_stale_days = history.get("priceStaleTradingDays")
    price_reason = history.get("priceFreshnessReason")

    lines = []

    lines.append(f"[{base_date} 투자 판단]")
    if price_as_of or price_status:
        lines.append(
            "시세 기준일: {0} / {1}{2}".format(
                price_as_of or "판정 불가",
                price_status or "UNKNOWN",
                "" if price_status == "PASS" else f" — {price_reason or '최신성 확인 필요'}",
            )
        )
    lines.append("")
    lines.append(build_section("BUY 후보", buy_candidates, show_reason=True))
    lines.append("")
    lines.append(build_section("HOLD 유지", hold_candidates, show_reason=False))
    lines.append("")
    lines.append(build_section("SELL 제외 후보", sell_candidates, show_reason=True))
    lines.append("")
    lines.append(f"생성 시간: {now_text()}")

    report_text = "\n".join(lines)

    report_json = {
        "baseDate": base_date,
        "generatedAt": now_text(),
        # 시세 최신성(additive) — 값이 없으면 None 으로 남겨 기존 consumer 호환.
        "priceAsOf": price_as_of,
        "priceStaleTradingDays": price_stale_days,
        "priceFreshnessStatus": price_status,
        "priceFreshnessReason": price_reason,
        "buyCandidates": buy_candidates,
        "holdCandidates": hold_candidates,
        "sellCandidates": sell_candidates,
        "counts": {
            "buy": len(buy_candidates),
            "hold": len(hold_candidates),
            "sell": len(sell_candidates),
        },
    }

    return report_text, report_json


def main():
    print("일일 투자 판단 리포트 생성 시작")
    print(f"시작 시간: {now_text()}")

    history = read_json(RECOMMENDATION_HISTORY_PATH)

    report_text, report_json = build_report(history)

    write_text(DAILY_REPORT_TXT_PATH, report_text)
    write_json(DAILY_REPORT_JSON_PATH, report_json)

    print(f"written: {DAILY_REPORT_TXT_PATH}")
    print(f"written: {DAILY_REPORT_JSON_PATH}")
    print(f"buyCandidates: {report_json['counts']['buy']}")
    print(f"holdCandidates: {report_json['counts']['hold']}")
    print(f"sellCandidates: {report_json['counts']['sell']}")


if __name__ == "__main__":
    main()