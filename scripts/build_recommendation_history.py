import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta


ROOT_DIR = Path(__file__).resolve().parents[1]

MARKET_SNAPSHOT_PATH = ROOT_DIR / "financial-universe-real.json"
NEWS_MOMENTUM_PATH = ROOT_DIR / "news-momentum-sample.json"
RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
FILTER_CONFIG_PATH = ROOT_DIR / "strategy-filter-config.json"
REVIEWED_CANDIDATES_PATH = ROOT_DIR / "reviewed-candidates.json"

WABABA_FUND_VERSION = "2026-05-06-50m-full-stock-v1"
WABABA_FUND_START_DATE = "2026-05-06"
WABABA_FUND_INITIAL_CAPITAL = 50000000

WABABA_AI_FUND_VERSION = "2026-05-06-50m-ai-fund-v1"
WABABA_AI_FUND_START_DATE = "2026-05-06"
WABABA_AI_FUND_INITIAL_CAPITAL = 50000000

PORTFOLIO_PATH = ROOT_DIR / "portfolio.json"
TRADE_HISTORY_PATH = ROOT_DIR / "trade-history.json"
AUTO_TRADE_LOG_PATH = ROOT_DIR / "wababa-auto-trade-log.json"

AI_PORTFOLIO_PATH = ROOT_DIR / "wababa-ai-portfolio.json"
AI_TRADE_HISTORY_PATH = ROOT_DIR / "wababa-ai-trade-history.json"
AI_AUTO_TRADE_LOG_PATH = ROOT_DIR / "wababa-ai-auto-trade-log.json"


DEFAULT_FILTER_CONFIG = {
    "maxBuyCandidates": 3,
    "maxWababaPicks": 3,

    "minFinancialScoreForBuy": 65,
    "minTotalScoreForBuy": 75,
    "minNewsScoreForBuy": 0,

    "minRoeForWababa": 8,
    "maxPerForWababa": 25,
    "minOperatingProfitGrowthForWababa": 0,
    "minMarketCapBillionKrwForWababa": 500,

    "maxSellScore": 45,
    "minScoreDropForSell": 20,
    "badNewsScore": -30,
    "minNewsScoreDropForSell": 40,
}


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_json(path):
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)



def normalize_trade_history_record(entry):
    if not isinstance(entry, dict):
        return None

    action = str(entry.get("action") or entry.get("type") or "").upper().strip()
    if action not in ["BUY", "SELL"]:
        return None

    code = str(entry.get("code") or entry.get("stockCode") or entry.get("symbol") or "").strip()
    name = str(entry.get("name") or entry.get("corpName") or entry.get("companyName") or "").strip()
    date_text = str(entry.get("date") or "")[:10]
    price = safe_number(entry.get("price"))
    quantity = safe_number(entry.get("quantity") or entry.get("shares") or entry.get("qty"))

    if not date_text or (not code and not name):
        return None

    amount = safe_number(entry.get("amount"))
    if amount <= 0 and price > 0 and quantity > 0:
        amount = price * quantity

    result = {
        "id": str(entry.get("id") or make_trade_id(date_text, action, code or name)).strip(),
        "date": date_text,
        "type": action,
        "action": action,
        "stockCode": code,
        "code": code,
        "name": name,
        "price": round(price, 0),
        "quantity": round(quantity, 4),
        "amount": round(amount, 0),
        "reason": str(entry.get("reason") or "").strip(),
    }

    if action == "SELL":
        result["profitAmount"] = round(safe_number(entry.get("profitAmount")), 0)
        profit_rate = entry.get("profitRate")
        result["profitRate"] = round(safe_number(profit_rate), 2) if profit_rate is not None else None

    return result


def build_trade_history_from_portfolio(portfolio):
    source = portfolio if isinstance(portfolio, dict) else {}
    ledger = source.get("tradeLedger")
    if not isinstance(ledger, list):
        ledger = []

    normalized = []
    seen_ids = set()
    for entry in ledger:
        record = normalize_trade_history_record(entry)
        if not record:
            continue
        trade_id = str(record.get("id") or "").strip()
        if trade_id and trade_id in seen_ids:
            continue
        if trade_id:
            seen_ids.add(trade_id)
        normalized.append(record)

    return sorted(normalized, key=lambda item: (str(item.get("date") or ""), str(item.get("id") or "")))


def write_trade_history_from_portfolio(portfolio):
    trade_history = build_trade_history_from_portfolio(portfolio)
    write_json(TRADE_HISTORY_PATH, trade_history)
    return trade_history


def load_auto_trade_log():
    loaded = read_json(AUTO_TRADE_LOG_PATH)
    if isinstance(loaded, dict):
        entries = loaded.get("entries")
        if isinstance(entries, list):
            return loaded
    if isinstance(loaded, list):
        return {"entries": loaded}
    return {"entries": []}


def write_auto_trade_log(data):
    source = data if isinstance(data, dict) else {"entries": []}
    entries = source.get("entries")
    if not isinstance(entries, list):
        entries = []
    payload = {
        "fundVersion": WABABA_FUND_VERSION,
        "updatedAt": now_text(),
        "entries": entries[-200:],
    }
    write_json(AUTO_TRADE_LOG_PATH, payload)
    return payload


def has_auto_trade_log_for_date(base_date):
    base_date_text = str(base_date or "")[:10]
    if not base_date_text:
        return False

    loaded = load_auto_trade_log()
    entries = loaded.get("entries") if isinstance(loaded, dict) else []
    if not isinstance(entries, list):
        return False

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_date = str(entry.get("date") or "")[:10]
        status = str(entry.get("status") or "").upper().strip()
        if entry_date == base_date_text and status in [
            "TRADED",
            "NO_TRADE",
            "ALREADY_EXECUTED",
            "MARKET_CLOSED",
            "WAIT_START",
            "OFF",
        ]:
            return True
    return False


def append_auto_trade_log(base_date, status, message, orders=None, skipped=None, auto_trade_pick=None):
    loaded = load_auto_trade_log()
    entries = loaded.get("entries") if isinstance(loaded, dict) else []
    if not isinstance(entries, list):
        entries = []

    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    status_text = str(status or "UNKNOWN").upper().strip()
    entry_id = f"{base_date_text}::{status_text}"

    # 같은 날짜/상태 로그는 1개만 유지해서 버튼 반복 클릭 시 로그가 무한히 늘지 않게 한다.
    entries = [
        entry for entry in entries
        if not (
            isinstance(entry, dict)
            and str(entry.get("id") or "") == entry_id
        )
    ]

    pick_payload = None
    if isinstance(auto_trade_pick, dict):
        pick_payload = {
            "code": get_code(auto_trade_pick),
            "name": get_name(auto_trade_pick),
            "valueBuyPassed": bool(auto_trade_pick.get("valueBuyPassed")),
            "growthStory": auto_trade_pick.get("growthStory"),
        }

    entries.append({
        "id": entry_id,
        "date": base_date_text,
        "status": status_text,
        "message": str(message or ""),
        "createdAt": now_text(),
        "orderCount": len(orders) if isinstance(orders, list) else 0,
        "skippedCount": len(skipped) if isinstance(skipped, list) else 0,
        "orders": orders if isinstance(orders, list) else [],
        "skipped": skipped if isinstance(skipped, list) else [],
        "autoTradePick": pick_payload,
    })

    write_auto_trade_log({"entries": entries})




def load_ai_auto_trade_log():
    loaded = read_json(AI_AUTO_TRADE_LOG_PATH)
    if isinstance(loaded, dict):
        entries = loaded.get("entries")
        if isinstance(entries, list):
            return loaded
    if isinstance(loaded, list):
        return {"entries": loaded}
    return {"entries": []}


def write_ai_auto_trade_log(data):
    source = data if isinstance(data, dict) else {"entries": []}
    entries = source.get("entries")
    if not isinstance(entries, list):
        entries = []
    payload = {
        "fundVersion": WABABA_AI_FUND_VERSION,
        "updatedAt": now_text(),
        "entries": entries[-200:],
    }
    write_json(AI_AUTO_TRADE_LOG_PATH, payload)
    return payload


def has_ai_auto_trade_log_for_date(base_date):
    base_date_text = str(base_date or "")[:10]
    if not base_date_text:
        return False
    loaded = load_ai_auto_trade_log()
    entries = loaded.get("entries") if isinstance(loaded, dict) else []
    if not isinstance(entries, list):
        return False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_date = str(entry.get("date") or "")[:10]
        status = str(entry.get("status") or "").upper().strip()
        if entry_date == base_date_text and status in [
            "TRADED",
            "NO_TRADE",
            "ALREADY_EXECUTED",
            "MARKET_CLOSED",
            "WAIT_START",
            "OFF",
        ]:
            return True
    return False


def append_ai_auto_trade_log(base_date, status, message, orders=None, skipped=None, ai_trade_picks=None):
    loaded = load_ai_auto_trade_log()
    entries = loaded.get("entries") if isinstance(loaded, dict) else []
    if not isinstance(entries, list):
        entries = []

    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    status_text = str(status or "UNKNOWN").upper().strip()
    entry_id = f"{base_date_text}::{status_text}"

    entries = [
        entry for entry in entries
        if not (
            isinstance(entry, dict)
            and str(entry.get("id") or "") == entry_id
        )
    ]

    pick_payload = []
    if isinstance(ai_trade_picks, list):
        for item in ai_trade_picks:
            if isinstance(item, dict):
                pick_payload.append({
                    "code": get_code(item),
                    "name": get_name(item),
                    "aiScore": safe_number(item.get("aiFundScore")),
                    "reason": str(item.get("aiFundReason") or ""),
                })

    entries.append({
        "id": entry_id,
        "date": base_date_text,
        "status": status_text,
        "message": str(message or ""),
        "createdAt": now_text(),
        "orderCount": len(orders) if isinstance(orders, list) else 0,
        "skippedCount": len(skipped) if isinstance(skipped, list) else 0,
        "orders": orders if isinstance(orders, list) else [],
        "skipped": skipped if isinstance(skipped, list) else [],
        "aiTradePicks": pick_payload,
    })

    write_ai_auto_trade_log({"entries": entries})

def build_default_portfolio():
    return {
        "fundVersion": WABABA_FUND_VERSION,
        "initialCapital": WABABA_FUND_INITIAL_CAPITAL,
        "cash": WABABA_FUND_INITIAL_CAPITAL,
        "currency": "KRW",
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
        "fundStartDate": WABABA_FUND_START_DATE,
        "autoTradingEnabled": True,
        "marketHolidays": [],
        "realizedProfit": 0,
        "maxPositions": 12,
        "maxPositionWeight": 10,
        "minCashRate": 0,
        "minBuyConfidence": 70,
        "dailyNewBuyLimit": 1,
        "tradeLedger": [],
        "positions": [],
        "memo": "와바바펀드는 2026-05-06부터 시작금 50,000,000원 기준, 개장일에만 매매하고 현금 보유 제한 없이 주식 100%까지 가상 운용합니다.",
    }


def load_portfolio():
    loaded = read_json(PORTFOLIO_PATH)

    if isinstance(loaded, dict):
        current_version = str(loaded.get("fundVersion") or "").strip()
        initial_capital = safe_number(loaded.get("initialCapital") or loaded.get("initialCash"))

        if current_version == WABABA_FUND_VERSION and initial_capital == WABABA_FUND_INITIAL_CAPITAL:
            positions = loaded.get("positions")
            if isinstance(positions, list):
                return loaded

    default_portfolio = build_default_portfolio()
    write_json(PORTFOLIO_PATH, default_portfolio)
    return default_portfolio



def build_default_ai_portfolio():
    return {
        "fundVersion": WABABA_AI_FUND_VERSION,
        "fundName": "와바바AI펀드",
        "initialCapital": WABABA_AI_FUND_INITIAL_CAPITAL,
        "cash": WABABA_AI_FUND_INITIAL_CAPITAL,
        "currency": "KRW",
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
        "fundStartDate": WABABA_AI_FUND_START_DATE,
        "autoTradingEnabled": True,
        "marketHolidays": [],
        "realizedProfit": 0,
        "maxPositions": 10,
        "maxPositionWeight": 12,
        "minCashRate": 5,
        "minBuyConfidence": 65,
        "dailyNewBuyLimit": 2,
        "tradeLedger": [],
        "positions": [],
        "memo": "와바바AI펀드는 시작금 50,000,000원 기준으로 AI가 자유롭게 종목·비중·매수·매도 판단을 비교 운용하는 별도 펀드입니다.",
    }


def load_ai_portfolio():
    loaded = read_json(AI_PORTFOLIO_PATH)

    if isinstance(loaded, dict):
        current_version = str(loaded.get("fundVersion") or "").strip()
        initial_capital = safe_number(loaded.get("initialCapital") or loaded.get("initialCash"))

        if current_version == WABABA_AI_FUND_VERSION and initial_capital == WABABA_AI_FUND_INITIAL_CAPITAL:
            positions = loaded.get("positions")
            if isinstance(positions, list):
                return loaded

    default_portfolio = build_default_ai_portfolio()
    write_json(AI_PORTFOLIO_PATH, default_portfolio)
    return default_portfolio


def write_ai_trade_history_from_portfolio(portfolio):
    trade_history = build_trade_history_from_portfolio(portfolio)
    write_json(AI_TRADE_HISTORY_PATH, trade_history)
    return trade_history


def get_ai_fund_policy(portfolio):
    source = portfolio if isinstance(portfolio, dict) else {}
    holidays = source.get("marketHolidays")
    if not isinstance(holidays, list):
        holidays = []

    return {
        "fundName": "와바바AI펀드",
        "startDate": str(source.get("fundStartDate") or WABABA_AI_FUND_START_DATE),
        "initialCapital": safe_number(source.get("initialCapital") or source.get("initialCash") or WABABA_AI_FUND_INITIAL_CAPITAL),
        "autoTradingEnabled": bool(source.get("autoTradingEnabled", True)),
        "maxPositions": safe_int(source.get("maxPositions"), 10),
        "maxPositionWeight": safe_number(source.get("maxPositionWeight"), 12),
        "minCashRate": safe_number(source.get("minCashRate"), 5),
        "minBuyConfidence": safe_int(source.get("minBuyConfidence"), 65),
        "dailyNewBuyLimit": safe_int(source.get("dailyNewBuyLimit"), 2),
        "marketHolidays": [str(value).strip() for value in holidays if str(value).strip()],
        "rules": [
            "와바바AI펀드는 AI 자율운용 비교 펀드",
            "가치·성장·뉴스·품질·성장 지속성을 종합해 자유롭게 매수",
            "하루 신규 매수는 최대 2종목",
            "보유 종목 최대 10개",
            "성장 훼손, 품질 악화, 더 좋은 기회 발생 시 매도 가능",
            "와바바펀드와 별도 포트폴리오로 성과 비교",
        ],
    }

def parse_date_value(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in ["%Y-%m-%d", "%Y.%m.%d", "%Y. %m. %d.", "%y.%m.%d"]:
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%y.%m.%d":
                return parsed.date()
            return parsed.date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def get_fund_policy(portfolio):
    source = portfolio if isinstance(portfolio, dict) else {}
    holidays = source.get("marketHolidays")
    if not isinstance(holidays, list):
        holidays = []

    return {
        "fundName": "와바바펀드",
        "startDate": str(source.get("fundStartDate") or WABABA_FUND_START_DATE),
        "initialCapital": safe_number(source.get("initialCapital") or source.get("initialCash") or WABABA_FUND_INITIAL_CAPITAL),
        "autoTradingEnabled": bool(source.get("autoTradingEnabled", True)),
        "maxPositions": safe_int(source.get("maxPositions"), 12),
        "maxPositionWeight": safe_number(source.get("maxPositionWeight"), 10),
        "minCashRate": safe_number(source.get("minCashRate"), 0),
        "minBuyConfidence": safe_int(source.get("minBuyConfidence"), 70),
        "dailyNewBuyLimit": safe_int(source.get("dailyNewBuyLimit"), 1),
        "marketHolidays": [str(value).strip() for value in holidays if str(value).strip()],
        "rules": [
            "와바바펀드는 고성장 저평가 가치투자 펀드",
            "매수 기준: ROE 15% 이상, 영업이익 성장 15% 이상, PER 15 이하, 성장 힌트 1개 이상",
            "좋은 종목이 없으면 하루 0건 매수 가능하며 억지 매수하지 않음",
            "보유 기준: 돈을 계속 잘 벌고 성장 가설이 유지되면 3년 이상 또는 무기한 보유",
            "PER 상승만으로 매도하지 않고, 성장 가설 붕괴 또는 더 좋은 고성장 저평가 기회 발견 시 매도 점검",
            "배당수익도 와바바펀드 총성과에 포함",
            "한국 주식 개장일에만 매수·매도 반영",
            "보유 종목 최대 12개",
        ],
    }


def is_market_open_day(base_date, policy):
    parsed = parse_date_value(base_date)
    if parsed is None:
        return False

    holidays = set(policy.get("marketHolidays") or [])
    if parsed.weekday() >= 5:
        return False
    if parsed.strftime("%Y-%m-%d") in holidays:
        return False
    return True


def get_existing_trade_ids(portfolio):
    ledger = portfolio.get("tradeLedger") if isinstance(portfolio, dict) else []
    if not isinstance(ledger, list):
        return set()

    result = set()
    for entry in ledger:
        if isinstance(entry, dict):
            trade_id = str(entry.get("id") or "").strip()
            if trade_id:
                result.add(trade_id)
    return result


def make_trade_id(base_date, action, code):
    return f"{str(base_date)[:10]}::{action}::{code}"


def count_trades_on_date(ledger, base_date, action=None):
    if not isinstance(ledger, list):
        return 0

    base_date_text = str(base_date or "")[:10]
    action_text = str(action or "").upper().strip()
    count = 0

    for entry in ledger:
        if not isinstance(entry, dict):
            continue
        entry_date = str(entry.get("date") or "")[:10]
        entry_action = str(entry.get("action") or entry.get("type") or "").upper().strip()

        if entry_date != base_date_text:
            continue
        if action_text and entry_action != action_text:
            continue
        count += 1

    return count


def has_buy_trade_on_date(ledger, base_date):
    return count_trades_on_date(ledger, base_date, "BUY") > 0


def get_portfolio_cash(portfolio, positions=None):
    if not isinstance(portfolio, dict):
        return WABABA_FUND_INITIAL_CAPITAL

    direct_cash = portfolio.get("cash")
    if direct_cash is not None:
        return safe_number(direct_cash)

    initial_capital = safe_number(portfolio.get("initialCapital") or portfolio.get("initialCash") or WABABA_FUND_INITIAL_CAPITAL)
    normalized_positions = positions if positions is not None else normalize_portfolio_positions(portfolio)
    total_buy_amount = sum(
        safe_number(position.get("buyPrice")) * safe_number(position.get("quantity"))
        for position in normalized_positions
    )
    return max(0, initial_capital - total_buy_amount)


def upsert_auto_position(positions, code, name, buy_price, quantity, buy_date):
    if not code or buy_price <= 0 or quantity <= 0:
        return positions

    for index, position in enumerate(positions):
        existing_code = str(position.get("code") or "").strip()
        if existing_code != code:
            continue

        previous_quantity = safe_number(position.get("quantity"))
        previous_buy_price = safe_number(position.get("buyPrice"))
        previous_amount = previous_quantity * previous_buy_price
        new_amount = quantity * buy_price
        merged_quantity = previous_quantity + quantity
        merged_price = round((previous_amount + new_amount) / merged_quantity, 2) if merged_quantity > 0 else buy_price

        positions[index] = {
            **position,
            "code": code,
            "name": str(position.get("name") or name),
            "buyPrice": merged_price,
            "quantity": round(merged_quantity, 4),
            "buyDate": str(position.get("buyDate") or buy_date),
            "lastBuyDate": buy_date,
            "source": "WABABA_AUTO_FUND",
        }
        return positions

    positions.insert(0, {
        "code": code,
        "name": name,
        "buyPrice": round(buy_price, 2),
        "quantity": round(quantity, 4),
        "buyDate": buy_date,
        "source": "WABABA_AUTO_FUND",
    })
    return positions


def apply_wababa_fund_auto_trading(portfolio, final_best_pick, today_map, base_date):
    if not isinstance(portfolio, dict):
        portfolio = load_portfolio()

    policy = get_fund_policy(portfolio)
    positions = normalize_portfolio_positions(portfolio)
    ledger = portfolio.get("tradeLedger")
    if not isinstance(ledger, list):
        ledger = []

    existing_trade_ids = get_existing_trade_ids({**portfolio, "tradeLedger": ledger})
    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    fund_start = parse_date_value(policy.get("startDate"))
    current_date = parse_date_value(base_date_text)

    orders = []
    skipped = []

    if has_auto_trade_log_for_date(base_date_text):
        result = {
            "status": "ALREADY_EXECUTED",
            "message": "오늘 자동운용은 이미 실행되어 추가 매매를 차단했습니다.",
            "orders": [],
            "skipped": [{"action": "AUTO", "reason": "오늘 자동운용 이미 실행됨"}],
        }
        append_auto_trade_log(base_date_text, result.get("status"), result.get("message"), result.get("orders"), result.get("skipped"), None)
        return portfolio, result

    if not policy.get("autoTradingEnabled"):
        result = {"status": "OFF", "message": "자동 운용 꺼짐", "orders": [], "skipped": []}
        append_auto_trade_log(base_date_text, result.get("status"), result.get("message"), result.get("orders"), result.get("skipped"), None)
        return portfolio, result

    if fund_start and current_date and current_date < fund_start:
        result = {
            "status": "WAIT_START",
            "message": f"와바바펀드 시작일 {policy.get('startDate')} 전입니다.",
            "orders": [],
            "skipped": [],
        }
        append_auto_trade_log(base_date_text, result.get("status"), result.get("message"), result.get("orders"), result.get("skipped"), None)
        return portfolio, result

    if not is_market_open_day(base_date_text, policy):
        result = {
            "status": "MARKET_CLOSED",
            "message": "주식 개장일이 아니므로 매매를 반영하지 않았습니다.",
            "orders": [],
            "skipped": [],
        }
        append_auto_trade_log(base_date_text, result.get("status"), result.get("message"), result.get("orders"), result.get("skipped"), None)
        return portfolio, result

    initial_capital = safe_number(policy.get("initialCapital"))
    cash = get_portfolio_cash(portfolio, positions)
    realized_profit = safe_number(portfolio.get("realizedProfit"))

    # 1) 매도 우선: HIGH 신호 또는 급격한 손실 점검 대상은 전량 매도, 큰 수익은 절반 차익 실현.
    remaining_positions = []
    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        current_price = get_current_price(item)
        quantity = safe_number(position.get("quantity"))
        buy_price = safe_number(position.get("buyPrice"))
        decision = build_portfolio_decision(item, position)
        signal = decision.get("sellSignal") if isinstance(decision, dict) else {}
        urgency = str(signal.get("urgency") or "LOW") if isinstance(signal, dict) else "LOW"
        action = str(decision.get("action") or "") if isinstance(decision, dict) else ""
        profit_rate = decision.get("profitRate") if isinstance(decision, dict) else None
        profit_rate_number = safe_number(profit_rate) if profit_rate is not None else 0

        sell_quantity = 0
        sell_reason = ""

        if current_price > 0 and quantity > 0 and urgency == "HIGH":
            sell_quantity = quantity
            sell_reason = "매도 엔진 HIGH"
        elif current_price > 0 and quantity > 0 and action == "SELL_CHECK" and profit_rate_number <= -8:
            sell_quantity = quantity
            sell_reason = "손실 확대와 매도 점검 동시 발생"
        elif current_price > 0 and quantity > 1 and action == "TAKE_PROFIT_CHECK" and profit_rate_number >= 20:
            sell_quantity = max(1, int(quantity * 0.5))
            sell_reason = "수익률 20% 이상 일부 차익"

        if sell_quantity > 0:
            trade_id = make_trade_id(base_date_text, "SELL", code)
            if trade_id not in existing_trade_ids:
                sell_amount = current_price * sell_quantity
                buy_amount = buy_price * sell_quantity
                realized = sell_amount - buy_amount
                cash += sell_amount
                realized_profit += realized
                remaining_quantity = quantity - sell_quantity
                order = {
                    "id": trade_id,
                    "date": base_date_text,
                    "action": "SELL",
                    "code": code,
                    "name": name or get_name(item),
                    "price": round(current_price, 0),
                    "quantity": round(sell_quantity, 4),
                    "amount": round(sell_amount, 0),
                    "reason": sell_reason,
                    "profitAmount": round(realized, 0),
                    "profitRate": profit_rate,
                }
                orders.append(order)
                ledger.append(order)
                existing_trade_ids.add(trade_id)

                if remaining_quantity > 0:
                    remaining_positions.append({**position, "quantity": round(remaining_quantity, 4)})
            else:
                remaining_positions.append(position)
                skipped.append({"code": code, "action": "SELL", "reason": "오늘 이미 반영됨"})
        else:
            remaining_positions.append(position)

    positions = remaining_positions

    # 2) 신규 매수: 하루 신규 매수 한도와 오늘 실행 락을 먼저 확인한다.
    today_buy_count = count_trades_on_date(ledger, base_date_text, "BUY")
    daily_new_buy_limit = max(0, safe_int(policy.get("dailyNewBuyLimit"), 1))

    if daily_new_buy_limit <= 0:
        skipped.append({"action": "BUY", "reason": "하루 신규 매수 한도 0"})
    elif today_buy_count >= daily_new_buy_limit:
        skipped.append({
            "action": "BUY",
            "reason": f"오늘 이미 신규 매수 {today_buy_count}건 반영됨",
        })

    # 3) 신규 매수: 오늘 한도 내에서만, 보유 한도/현금 한도/신뢰도 통과 시 1차 금액만 반영.
    if isinstance(final_best_pick, dict) and daily_new_buy_limit > 0 and today_buy_count < daily_new_buy_limit:
        code = get_code(final_best_pick)
        name = get_name(final_best_pick)
        already_held = any(str(position.get("code") or "").strip() == code for position in positions)
        engine = final_best_pick.get("decisionEngine") if isinstance(final_best_pick.get("decisionEngine"), dict) else build_decision_engine(final_best_pick)
        sizing = final_best_pick.get("positionSizing") if isinstance(final_best_pick.get("positionSizing"), dict) else build_position_sizing(final_best_pick, engine)
        confidence = safe_int(engine.get("confidence"), 0) if isinstance(engine, dict) else 0
        action = str(engine.get("action") or "") if isinstance(engine, dict) else ""
        current_price = get_current_price(final_best_pick)
        first_buy_amount = safe_number(sizing.get("firstBuyAmount")) if isinstance(sizing, dict) else 0
        target_amount = safe_number(sizing.get("targetAmount")) if isinstance(sizing, dict) else 0
        buy_budget = first_buy_amount if first_buy_amount > 0 else min(target_amount, initial_capital * 0.05)
        min_cash_after_buy = initial_capital * safe_number(policy.get("minCashRate")) / 100
        max_positions = safe_int(policy.get("maxPositions"), 12)
        trade_id = make_trade_id(base_date_text, "BUY", code)

        if not code:
            skipped.append({"action": "BUY", "reason": "오늘 1종목 코드 없음"})
        elif already_held:
            skipped.append({"code": code, "action": "BUY", "reason": "이미 보유 중"})
        elif trade_id in existing_trade_ids:
            skipped.append({"code": code, "action": "BUY", "reason": "오늘 이미 반영됨"})
        elif len(positions) >= max_positions:
            skipped.append({"code": code, "action": "BUY", "reason": "보유 종목 한도 도달"})
        elif confidence < safe_int(policy.get("minBuyConfidence"), 70):
            skipped.append({"code": code, "action": "BUY", "reason": f"신뢰도 {confidence}%로 기준 미달"})
        elif action != "BUY_NOW":
            skipped.append({"code": code, "action": "BUY", "reason": "와바바 가치매수 기준 미달"})
        elif current_price <= 0:
            skipped.append({"code": code, "action": "BUY", "reason": "현재가 없음"})
        elif buy_budget <= 0:
            skipped.append({"code": code, "action": "BUY", "reason": "매수 예산 없음"})
        elif cash - buy_budget < min_cash_after_buy:
            skipped.append({"code": code, "action": "BUY", "reason": "매수 가능 현금 부족"})
        else:
            quantity = int(buy_budget // current_price)
            if quantity <= 0:
                skipped.append({"code": code, "action": "BUY", "reason": "1주 매수 예산 미달"})
            else:
                amount = quantity * current_price
                cash -= amount
                positions = upsert_auto_position(positions, code, name, current_price, quantity, base_date_text)
                order = {
                    "id": trade_id,
                    "date": base_date_text,
                    "action": "BUY",
                    "code": code,
                    "name": name,
                    "price": round(current_price, 0),
                    "quantity": quantity,
                    "amount": round(amount, 0),
                    "reason": f"오늘 1종목 · 신뢰도 {confidence}% · {sizing.get('label') if isinstance(sizing, dict) else ''}",
                }
                orders.append(order)
                ledger.append(order)
                existing_trade_ids.add(trade_id)

    portfolio = {
        **portfolio,
        "fundVersion": WABABA_FUND_VERSION,
        "initialCapital": initial_capital,
        "cash": round(cash, 0),
        "currency": "KRW",
        "fundStartDate": policy.get("startDate"),
        "autoTradingEnabled": policy.get("autoTradingEnabled"),
        "maxPositions": policy.get("maxPositions"),
        "maxPositionWeight": policy.get("maxPositionWeight"),
        "minCashRate": policy.get("minCashRate"),
        "minBuyConfidence": policy.get("minBuyConfidence"),
        "realizedProfit": round(realized_profit, 0),
        "updatedAt": base_date_text,
        "positions": positions,
        "tradeLedger": ledger,
    }
    write_json(PORTFOLIO_PATH, portfolio)
    write_trade_history_from_portfolio(portfolio)

    status = "TRADED" if orders else "NO_TRADE"
    message = "개장일 기준 자동 운용 반영" if orders else "개장일이지만 신규 체결 없음"
    append_auto_trade_log(base_date_text, status, message, orders, skipped, final_best_pick)
    return portfolio, {"status": status, "message": message, "orders": orders, "skipped": skipped}


def normalize_portfolio_positions(portfolio):
    positions = portfolio.get("positions") if isinstance(portfolio, dict) else []
    if not isinstance(positions, list):
        return []

    normalized = []
    for raw in positions:
        if not isinstance(raw, dict):
            continue

        code = str(raw.get("code") or raw.get("symbol") or raw.get("stockCode") or "").strip()
        name = str(raw.get("name") or raw.get("corpName") or raw.get("companyName") or "").strip()
        buy_price = safe_number(raw.get("buyPrice") or raw.get("averagePrice") or raw.get("avgBuyPrice"))
        quantity = safe_number(raw.get("quantity") or raw.get("shares") or raw.get("qty"))

        if not code and not name:
            continue

        normalized.append({
            **raw,
            "code": code,
            "name": name,
            "buyPrice": buy_price,
            "quantity": quantity,
            "buyDate": str(raw.get("buyDate") or raw.get("date") or "").strip(),
        })

    return normalized


def build_portfolio_position_map(portfolio):
    result = {}

    for position in normalize_portfolio_positions(portfolio):
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()

        if code:
            result[code] = position
        if name:
            result[name] = position

    return result


def get_current_price(item):
    return safe_number(
        item.get("price")
        or item.get("close")
        or item.get("currentPrice")
        or item.get("lastPrice")
    )


def calculate_profit_rate(item, position):
    buy_price = safe_number(position.get("buyPrice"))
    current_price = get_current_price(item)

    if buy_price <= 0 or current_price <= 0:
        return None

    return round(((current_price - buy_price) / buy_price) * 100, 2)


def read_decision_action(item):
    engine = item.get("decisionEngine") if isinstance(item, dict) else {}
    if isinstance(engine, dict):
        action = str(engine.get("action") or "").strip()
        if action:
            return action

    generated = build_decision_engine(item) if isinstance(item, dict) else {}
    if isinstance(generated, dict):
        return str(generated.get("action") or "SKIP").strip()

    return "SKIP"



def build_sell_signal(item, position=None, profit_rate=None):
    action = read_decision_action(item)
    confidence = safe_int((item.get("decisionEngine") or {}).get("confidence"), 0) if isinstance(item.get("decisionEngine"), dict) else 0
    quality_level = str(item.get("qualityLevel") or classify_quality_level(item))
    quality_warnings = item.get("qualityWarnings") if isinstance(item.get("qualityWarnings"), list) else build_quality_warnings(item)

    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    margin = safe_number(item.get("ebitMargin") or item.get("opMargin"))
    news_score = safe_number(item.get("newsScore") or item.get("newsMomentumScore"))
    quality_penalty = calculate_quality_penalty(item)

    reasons = []
    trigger = "none"
    urgency = "LOW"
    action_label = "HOLD"

    if profit_rate is not None and profit_rate <= -15:
        trigger = "loss_cut"
        urgency = "HIGH"
        action_label = "SELL_CHECK"
        reasons.append(f"손실률 {profit_rate:.1f}%로 방어선 점검 필요")
    elif profit_rate is not None and profit_rate <= -8:
        trigger = "loss_watch"
        urgency = "MID"
        action_label = "REDUCE_OR_HOLD_CHECK"
        reasons.append(f"손실률 {profit_rate:.1f}%로 매수 가설 재확인")

    if quality_level == "주의" or quality_penalty >= 45:
        trigger = "quality_break"
        urgency = "HIGH"
        action_label = "SELL_CHECK"
        reasons.append("품질 점검 단계가 주의 구간")
    elif quality_penalty >= 30 and urgency != "HIGH":
        trigger = "quality_warning"
        urgency = "MID"
        action_label = "HOLD_CHECK"
        reasons.append("품질 경고가 누적되어 보유 근거 확인 필요")

    if sales_growth < 0 and op_growth < 0:
        trigger = "growth_break"
        urgency = "HIGH"
        action_label = "SELL_CHECK"
        reasons.append("매출과 영업이익이 동시에 감소")
    elif sales_growth < 0 and urgency != "HIGH":
        trigger = "sales_break"
        urgency = "MID"
        action_label = "HOLD_CHECK"
        reasons.append("매출 감소가 동반되어 성장 지속성 확인 필요")

    if per >= 30 and roe < 10:
        trigger = "valuation_expansion"
        urgency = "MID" if urgency != "HIGH" else urgency
        action_label = "REDUCE_OR_HOLD_CHECK" if action_label == "HOLD" else action_label
        reasons.append("PER 부담이 커졌고 ROE 방어력이 약함")
    elif pbr >= 4 and roe < 12 and urgency != "HIGH":
        trigger = "pbr_burden"
        urgency = "MID"
        action_label = "HOLD_CHECK"
        reasons.append("PBR 부담 대비 ROE 방어력 확인 필요")

    if news_score <= -30:
        trigger = "bad_news"
        urgency = "HIGH"
        action_label = "SELL_CHECK"
        reasons.append("부정 뉴스 점수가 매도 점검 구간")

    if action == "SKIP" and position:
        if urgency == "LOW":
            trigger = "buy_thesis_broken"
            urgency = "MID"
            action_label = "HOLD_CHECK"
        reasons.append("현재 판단엔진이 신규 매수 제외로 변경")

    if profit_rate is not None and profit_rate >= 30 and confidence < 70 and urgency != "HIGH":
        trigger = "take_profit"
        urgency = "MID"
        action_label = "TAKE_PROFIT_CHECK"
        reasons.append(f"수익률 {profit_rate:.1f}% 도달, 신뢰도 {confidence}%라 일부 차익 검토")
    elif profit_rate is not None and profit_rate >= 45 and urgency != "HIGH":
        trigger = "strong_take_profit"
        urgency = "MID"
        action_label = "TAKE_PROFIT_CHECK"
        reasons.append(f"수익률 {profit_rate:.1f}%로 과열·차익 구간 점검")

    if not reasons:
        if action == "BUY_NOW" and confidence >= 70 and quality_level != "주의":
            trigger = "thesis_alive"
            urgency = "LOW"
            action_label = "HOLD_OR_ADD" if position else "BUY_CHECK"
            reasons.append("성장·밸류·품질 조합이 유지")
        else:
            trigger = "monitor"
            urgency = "LOW"
            action_label = "HOLD" if position else "WATCH"
            reasons.append("매도 급한 신호 없음")

    for warning in quality_warnings[:2]:
        warning_text = str(warning).strip()
        if warning_text and warning_text not in reasons:
            reasons.append(warning_text)

    return {
        "action": action_label,
        "trigger": trigger,
        "urgency": urgency,
        "reasons": reasons[:3],
        "summary": " / ".join(reasons[:2]),
        "profitRate": profit_rate,
        "confidence": confidence,
        "qualityPenalty": quality_penalty,
    }


def format_krw_amount_text(amount):
    number = safe_number(amount)
    if number <= 0:
        return "0원"

    eok = int(number // 100000000)
    man = int((number % 100000000) // 10000)

    if eok > 0 and man > 0:
        return f"{eok}억 {man:,}만원"
    if eok > 0:
        return f"{eok}억원"
    return f"{man:,}만원"


def build_position_sizing(item, decision_engine=None):
    engine = decision_engine if isinstance(decision_engine, dict) else build_decision_engine(item)
    action = str(engine.get("action") or "SKIP")
    confidence = safe_int(engine.get("confidence"), 0)

    quality_level = str(item.get("qualityLevel") or classify_quality_level(item))
    quality_penalty = calculate_quality_penalty(item)
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin") or item.get("opMargin"))

    total_capital = WABABA_FUND_INITIAL_CAPITAL
    target_weight = 0
    split_count = 0
    reason_parts = []

    if action == "BUY_NOW":
        target_weight = 8
        split_count = 3
        reason_parts.append("매수권 진입")
    elif action == "WATCH":
        target_weight = 4
        split_count = 2
        reason_parts.append("조건부 관찰권")
    else:
        target_weight = 0
        split_count = 0
        reason_parts.append("신규 매수 보류")

    if confidence >= 85:
        target_weight += 2
        reason_parts.append(f"신뢰도 {confidence}%")
    elif confidence >= 75:
        target_weight += 1
        reason_parts.append(f"신뢰도 {confidence}%")
    elif confidence < 60 and target_weight > 0:
        target_weight -= 2
        reason_parts.append(f"신뢰도 {confidence}%로 축소")

    if sales_growth >= 10 and op_growth >= 20:
        target_weight += 1
        reason_parts.append("매출·영업이익 동반 성장")
    elif op_growth >= 50:
        target_weight += 1
        reason_parts.append("영업이익 개선 강함")

    if roe >= 15 and 0 < per <= 10:
        target_weight += 1
        reason_parts.append("ROE 대비 PER 낮음")
    elif pbr <= 1 and roe >= 12:
        target_weight += 1
        reason_parts.append("PBR 대비 ROE 양호")

    if margin < 5 or quality_level == "주의":
        target_weight -= 3
        reason_parts.append("품질 리스크로 축소")
    elif quality_penalty >= 30:
        target_weight -= 2
        reason_parts.append("확인 항목 존재")
    elif quality_level == "양호" and target_weight > 0:
        reason_parts.append("품질 점검 양호")

    if op_growth >= 150:
        target_weight -= 1
        reason_parts.append("이익 급증 일회성 확인 필요")

    target_weight = max(0, min(12, int(round(target_weight))))

    if target_weight == 0:
        target_amount = 0
        first_buy_amount = 0
        split_count = 0
        label = "매수 금액 없음"
        summary = "신규 매수보다 관찰 우선"
    else:
        if split_count <= 0:
            split_count = 2
        if target_weight >= 9 and split_count < 3:
            split_count = 3
        target_amount = int(total_capital * target_weight / 100)
        first_buy_amount = int(target_amount / split_count)
        label = f"목표 비중 {target_weight}%"
        summary = f"추천 금액 {format_krw_amount_text(target_amount)}, {split_count}회 분할"

    return {
        "targetWeight": target_weight,
        "targetAmount": target_amount,
        "firstBuyAmount": first_buy_amount,
        "splitCount": split_count,
        "label": label,
        "summary": summary,
        "reason": " / ".join(reason_parts[:4]),
    }

def build_portfolio_decision(item, position=None):
    action = read_decision_action(item)
    confidence = safe_int((item.get("decisionEngine") or {}).get("confidence"), 0) if isinstance(item.get("decisionEngine"), dict) else 0
    quality_level = str(item.get("qualityLevel") or classify_quality_level(item))
    quality_warnings = item.get("qualityWarnings") if isinstance(item.get("qualityWarnings"), list) else build_quality_warnings(item)
    profit_rate = calculate_profit_rate(item, position) if position else None
    code = get_code(item)
    name = get_name(item)

    if not position:
        if action == "BUY_NOW":
            return {
                "status": "NOT_HELD",
                "action": "BUY",
                "label": "신규 매수 검토",
                "profitRate": None,
                "reason": "보유 전 종목. 성장·밸류·품질 조건이 매수권에 들어왔습니다.",
                "position": None,
                "sellSignal": build_sell_signal(item, None, None),
            }
        if action == "WATCH":
            return {
                "status": "NOT_HELD",
                "action": "WATCH",
                "label": "보유 전 관찰",
                "profitRate": None,
                "reason": "보유 전 종목. 조건은 일부 맞지만 확인 항목이 남아 있습니다.",
                "position": None,
                "sellSignal": build_sell_signal(item, None, None),
            }
        return {
            "status": "NOT_HELD",
            "action": "SKIP",
            "label": "신규 매수 보류",
            "profitRate": None,
            "reason": "보유 전 종목. 현재 조합은 신규 매수 우선순위가 낮습니다.",
            "position": None,
        }

    blocker = ", ".join(str(value) for value in quality_warnings[:2]) if quality_warnings else "핵심 리스크 급증 없음"
    sell_signal = build_sell_signal(item, position, profit_rate)

    if sell_signal.get("action") == "SELL_CHECK" and sell_signal.get("urgency") == "HIGH":
        portfolio_action = "SELL_CHECK"
        label = "강한 매도 점검"
        reason = sell_signal.get("summary") or f"보유 종목이지만 매도 점검 신호가 강합니다. 확인: {blocker}"
    elif sell_signal.get("action") == "TAKE_PROFIT_CHECK":
        portfolio_action = "TAKE_PROFIT_CHECK"
        label = "일부 차익 검토"
        reason = sell_signal.get("summary") or "수익 구간에 진입해 일부 차익 실현을 검토합니다."
    elif profit_rate is not None and profit_rate <= -12:
        portfolio_action = "SELL_CHECK"
        label = "손실 방어 점검"
        reason = f"현재 수익률 {profit_rate:.1f}%. 손실 폭이 커져 보유 근거 재확인이 필요합니다."
    elif quality_level == "주의" or action == "SKIP":
        portfolio_action = "SELL_CHECK"
        label = "매도 점검"
        reason = f"보유 종목이지만 현재 품질·판단 조건이 약합니다. 확인: {blocker}"
    elif profit_rate is not None and profit_rate >= 25 and confidence < 70:
        portfolio_action = "TAKE_PROFIT_CHECK"
        label = "일부 차익 검토"
        reason = f"현재 수익률 {profit_rate:.1f}%. 신뢰도가 강하지 않으면 일부 차익 실현을 검토합니다."
    elif action == "BUY_NOW" and confidence >= 70 and quality_level != "주의":
        portfolio_action = "ADD_OR_HOLD"
        label = "보유 유지 / 추가 검토"
        reason = "보유 종목의 매수 논리가 유지됩니다. 가격 부담과 비중만 확인합니다."
    else:
        portfolio_action = "HOLD"
        label = "보유 유지"
        reason = f"보유 종목. 현재는 매도보다 유지 판단이 우선입니다. 확인: {blocker}"

    quantity = safe_number(position.get("quantity"))
    buy_price = safe_number(position.get("buyPrice"))
    current_price = get_current_price(item)
    evaluation_amount = current_price * quantity if current_price > 0 and quantity > 0 else 0

    return {
        "status": "HELD",
        "action": portfolio_action,
        "label": label,
        "profitRate": profit_rate,
        "reason": reason,
        "sellSignal": sell_signal,
        "position": {
            "code": code or str(position.get("code") or ""),
            "name": name or str(position.get("name") or ""),
            "buyPrice": buy_price,
            "quantity": quantity,
            "buyDate": position.get("buyDate") or "",
            "currentPrice": current_price,
            "evaluationAmount": round(evaluation_amount, 0),
        },
    }


def attach_portfolio_decision(item, portfolio_map):
    if not isinstance(item, dict):
        return item

    code = get_code(item)
    name = get_name(item)
    position = portfolio_map.get(code) or portfolio_map.get(name)

    return {
        **item,
        "portfolioDecision": build_portfolio_decision(item, position),
    }


def attach_portfolio_to_list(items, portfolio_map):
    if not isinstance(items, list):
        return []
    return [attach_portfolio_decision(item, portfolio_map) for item in items]


def build_portfolio_summary(portfolio, portfolio_map, today_map, prev_summary=None):
    positions = normalize_portfolio_positions(portfolio)
    position_items = []
    total_buy_amount = 0
    total_eval_amount = 0

    # 직전 currentPrice fallback 맵: code → currentPrice
    prev_price_map: dict = {}
    if isinstance(prev_summary, dict):
        for prev_pos in (prev_summary.get("positions") or []):
            if not isinstance(prev_pos, dict):
                continue
            prev_code = str(prev_pos.get("code") or "").strip()
            prev_price = safe_number(prev_pos.get("currentPrice"))
            if prev_code and prev_price > 0:
                prev_price_map[prev_code] = prev_price

    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        buy_price = safe_number(position.get("buyPrice"))
        quantity = safe_number(position.get("quantity"))
        current_price = get_current_price(item)

        # fallback 1: today_map 가격이 없으면 직전 currentPrice 사용
        if current_price <= 0 and code in prev_price_map:
            current_price = prev_price_map[code]
        # fallback 2: today_map 가격이 buyPrice와 동일하고 직전 currentPrice가 다르면 직전 값 유지
        elif current_price > 0 and current_price == buy_price and code in prev_price_map and prev_price_map[code] != buy_price:
            current_price = prev_price_map[code]

        buy_amount = buy_price * quantity if buy_price > 0 and quantity > 0 else 0
        eval_amount = current_price * quantity if current_price > 0 and quantity > 0 else 0
        total_buy_amount += buy_amount
        total_eval_amount += eval_amount
        decision = build_portfolio_decision(item, position)
        position_items.append({
            "code": code,
            "name": name or get_name(item),
            "buyPrice": buy_price,
            "quantity": quantity,
            "currentPrice": current_price,
            "buyAmount": round(buy_amount, 0),
            "evaluationAmount": round(eval_amount, 0),
            "profitAmount": round(eval_amount - buy_amount, 0),
            "profitRate": decision.get("profitRate"),
            "action": decision.get("action"),
            "label": decision.get("label"),
            "reason": decision.get("reason"),
            "sellSignal": decision.get("sellSignal"),
        })

    total_profit_amount = total_eval_amount - total_buy_amount
    initial_capital = safe_number(
        portfolio.get("initialCapital")
        or portfolio.get("initialCash")
        or WABABA_FUND_INITIAL_CAPITAL
        if isinstance(portfolio, dict)
        else WABABA_FUND_INITIAL_CAPITAL
    )
    cash = get_portfolio_cash(portfolio, positions)
    total_asset_amount = cash + total_eval_amount

    total_profit_rate = None
    if initial_capital > 0 and total_asset_amount > 0:
        total_profit_rate = round(((total_asset_amount - initial_capital) / initial_capital) * 100, 2)

    invested_rate = None
    cash_rate = None
    if total_asset_amount > 0:
        invested_rate = round((total_eval_amount / total_asset_amount) * 100, 2)
        cash_rate = round((cash / total_asset_amount) * 100, 2)

    if total_profit_rate is None:
        performance_label = "운용 전"
        performanceTone = "neutral"
    elif total_profit_rate >= 10:
        performance_label = "수익 우수"
        performanceTone = "strong"
    elif total_profit_rate >= 3:
        performance_label = "수익 구간"
        performanceTone = "positive"
    elif total_profit_rate <= -8:
        performance_label = "손실 점검"
        performanceTone = "danger"
    elif total_profit_rate < 0:
        performance_label = "약손실"
        performanceTone = "warning"
    else:
        performance_label = "보합권"
        performanceTone = "neutral"

    return {
        "initialCapital": round(initial_capital, 0),
        "cash": round(cash, 0),
        "positionCount": len(position_items),
        "totalBuyAmount": round(total_buy_amount, 0),
        "totalEvaluationAmount": round(total_eval_amount, 0),
        "totalProfitAmount": round(total_profit_amount, 0),
        "realizedProfit": round(safe_number(portfolio.get("realizedProfit")) if isinstance(portfolio, dict) else 0, 0),
        "totalAssetAmount": round(total_asset_amount, 0),
        "totalProfitRate": total_profit_rate,
        "investedRate": invested_rate,
        "cashRate": cash_rate,
        "performanceLabel": performance_label,
        "performanceTone": performanceTone,
        "positions": position_items,
    }


def calculate_days_between(start_value, end_value):
    start_date = parse_date_value(start_value)
    end_date = parse_date_value(end_value)

    if start_date is None or end_date is None:
        return None

    return max(0, (end_date - start_date).days)


def build_performance_tone(rate):
    if rate is None:
        return "neutral"
    if rate >= 10:
        return "strong"
    if rate >= 3:
        return "positive"
    if rate < -8:
        return "danger"
    if rate < 0:
        return "warning"
    return "neutral"


def build_performance_label(rate):
    if rate is None:
        return "운용 전"
    if rate >= 10:
        return "성과 우수"
    if rate >= 3:
        return "수익 구간"
    if rate < -8:
        return "손실 점검"
    if rate < 0:
        return "약손실"
    return "보합권"


def build_recommendation_performance_items(portfolio, today_map, base_date):
    if not isinstance(portfolio, dict):
        return []

    positions = normalize_portfolio_positions(portfolio)
    trade_ledger = portfolio.get("tradeLedger")
    if not isinstance(trade_ledger, list):
        trade_ledger = []

    buy_entries = {}
    items = []

    for trade in trade_ledger:
        if not isinstance(trade, dict):
            continue

        action = str(trade.get("action") or "").upper().strip()
        code = str(trade.get("code") or "").strip()
        name = str(trade.get("name") or "").strip()
        trade_date = str(trade.get("date") or "").strip()

        if not code and not name:
            continue

        key = code or name

        if action == "BUY":
            buy_entries.setdefault(key, []).append(trade)
            continue

        if action == "SELL":
            profit_rate = trade.get("profitRate")
            profit_rate_number = safe_number(profit_rate) if profit_rate is not None else None
            if profit_rate_number is None:
                sell_price = safe_number(trade.get("price"))
                matched_buys = buy_entries.get(key) or []
                first_buy = matched_buys[0] if matched_buys else {}
                buy_price = safe_number(first_buy.get("price")) if isinstance(first_buy, dict) else 0
                profit_rate_number = round(((sell_price - buy_price) / buy_price) * 100, 2) if buy_price > 0 and sell_price > 0 else None

            matched_buys = buy_entries.get(key) or []
            first_buy_date = ""
            if matched_buys and isinstance(matched_buys[0], dict):
                first_buy_date = str(matched_buys[0].get("date") or "")

            items.append({
                "code": code,
                "name": name,
                "status": "CLOSED",
                "buyDate": first_buy_date,
                "sellDate": trade_date,
                "holdingDays": calculate_days_between(first_buy_date, trade_date),
                "profitRate": profit_rate_number,
                "profitAmount": round(safe_number(trade.get("profitAmount")), 0),
                "reason": str(trade.get("reason") or ""),
            })

    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        buy_price = safe_number(position.get("buyPrice"))
        quantity = safe_number(position.get("quantity"))
        current_price = get_current_price(item)

        if buy_price <= 0 or quantity <= 0 or current_price <= 0:
            continue

        profit_rate = round(((current_price - buy_price) / buy_price) * 100, 2)
        profit_amount = round((current_price - buy_price) * quantity, 0)
        buy_date = str(position.get("buyDate") or position.get("lastBuyDate") or "").strip()

        items.append({
            "code": code,
            "name": name or get_name(item),
            "status": "OPEN",
            "buyDate": buy_date,
            "sellDate": None,
            "holdingDays": calculate_days_between(buy_date, base_date),
            "buyPrice": round(buy_price, 2),
            "currentPrice": round(current_price, 0),
            "quantity": quantity,
            "profitRate": profit_rate,
            "profitAmount": profit_amount,
            "reason": "현재 보유 중",
        })

    return items


def build_recommendation_performance(portfolio, today_map, base_date):
    items = build_recommendation_performance_items(portfolio, today_map, base_date)
    evaluated_items = [item for item in items if item.get("profitRate") is not None]
    rates = [safe_number(item.get("profitRate")) for item in evaluated_items]
    holding_days = [safe_number(item.get("holdingDays")) for item in evaluated_items if item.get("holdingDays") is not None]

    win_count = sum(1 for rate in rates if rate > 0)
    loss_count = sum(1 for rate in rates if rate < 0)
    evaluated_count = len(rates)
    average_return_rate = round(sum(rates) / evaluated_count, 2) if evaluated_count > 0 else None
    win_rate = round((win_count / evaluated_count) * 100, 2) if evaluated_count > 0 else None
    max_profit_rate = round(max(rates), 2) if rates else None
    max_loss_rate = round(min(rates), 2) if rates else None
    average_holding_days = round(sum(holding_days) / len(holding_days), 1) if holding_days else None

    best_item = None
    worst_item = None
    if evaluated_items:
        best_item = max(evaluated_items, key=lambda item: safe_number(item.get("profitRate")))
        worst_item = min(evaluated_items, key=lambda item: safe_number(item.get("profitRate")))

    return {
        "recommendedCount": len(items),
        "evaluatedCount": evaluated_count,
        "winCount": win_count,
        "lossCount": loss_count,
        "winRate": win_rate,
        "averageReturnRate": average_return_rate,
        "maxProfitRate": max_profit_rate,
        "maxLossRate": max_loss_rate,
        "averageHoldingDays": average_holding_days,
        "bestItem": best_item,
        "worstItem": worst_item,
        "items": sorted(evaluated_items, key=lambda item: safe_number(item.get("profitRate")), reverse=True),
    }



def build_performance_event_label(action, status):
    action_text = str(action or "").upper().strip()
    status_text = str(status or "").upper().strip()

    if action_text == "BUY":
        return "매수"
    if action_text == "SELL":
        return "매도"
    if status_text == "OPEN":
        return "보유중"
    if status_text == "CLOSED":
        return "매도완료"
    return "기록"


def build_performance_event_tone(action, profit_rate=None):
    action_text = str(action or "").upper().strip()
    if action_text == "BUY":
        return "buy"
    if action_text == "SELL":
        return "sell"
    if profit_rate is None:
        return "neutral"
    rate = safe_number(profit_rate)
    if rate > 0:
        return "positive"
    if rate < 0:
        return "negative"
    return "neutral"


def build_recent_performance_events(portfolio, recommendation_performance, base_date):
    events = []
    trade_ledger = portfolio.get("tradeLedger") if isinstance(portfolio, dict) else []
    if not isinstance(trade_ledger, list):
        trade_ledger = []

    for trade in trade_ledger:
        if not isinstance(trade, dict):
            continue

        action = str(trade.get("action") or "").upper().strip()
        code = str(trade.get("code") or "").strip()
        name = str(trade.get("name") or "").strip()
        trade_date = str(trade.get("date") or "").strip()
        profit_rate = trade.get("profitRate") if action == "SELL" else None

        events.append({
            "date": trade_date,
            "type": action or "TRADE",
            "label": build_performance_event_label(action, None),
            "tone": build_performance_event_tone(action, profit_rate),
            "code": code,
            "name": name,
            "amount": round(safe_number(trade.get("amount")), 0),
            "profitRate": safe_number(profit_rate) if profit_rate is not None else None,
            "profitAmount": round(safe_number(trade.get("profitAmount")), 0) if action == "SELL" else None,
            "reason": str(trade.get("reason") or "").strip(),
        })

    performance_items = recommendation_performance.get("items") if isinstance(recommendation_performance, dict) else []
    if isinstance(performance_items, list):
        for item in performance_items:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "").upper().strip() != "OPEN":
                continue

            rate = item.get("profitRate")
            events.append({
                "date": str(base_date or "")[:10],
                "type": "OPEN_POSITION",
                "label": build_performance_event_label(None, "OPEN"),
                "tone": build_performance_event_tone(None, rate),
                "code": str(item.get("code") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "amount": round(safe_number(item.get("profitAmount")), 0),
                "profitRate": safe_number(rate) if rate is not None else None,
                "profitAmount": round(safe_number(item.get("profitAmount")), 0),
                "holdingDays": item.get("holdingDays"),
                "reason": "현재 보유 수익률 추적",
            })

    return sorted(events, key=lambda item: str(item.get("date") or ""), reverse=True)[:12]



def build_actual_trade_performance(portfolio):
    source = portfolio if isinstance(portfolio, dict) else {}
    trade_history = build_trade_history_from_portfolio(source)

    buy_queues = {}
    closed_items = []

    for trade in trade_history:
        action = str(trade.get("action") or trade.get("type") or "").upper().strip()
        code = str(trade.get("code") or trade.get("stockCode") or "").strip()
        name = str(trade.get("name") or "").strip()
        key = code or name
        if not key:
            continue

        if action == "BUY":
            buy_queues.setdefault(key, []).append({**trade, "remainingQuantity": safe_number(trade.get("quantity"))})
            continue

        if action != "SELL":
            continue

        sell_quantity = safe_number(trade.get("quantity"))
        sell_price = safe_number(trade.get("price"))
        remaining_sell_quantity = sell_quantity
        matched_cost = 0
        matched_quantity = 0
        first_buy_date = ""

        queue = buy_queues.get(key) or []
        for buy in queue:
            if remaining_sell_quantity <= 0:
                break
            available = safe_number(buy.get("remainingQuantity"))
            if available <= 0:
                continue
            matched = min(available, remaining_sell_quantity)
            buy_price = safe_number(buy.get("price"))
            matched_cost += buy_price * matched
            matched_quantity += matched
            remaining_sell_quantity -= matched
            buy["remainingQuantity"] = available - matched
            if not first_buy_date:
                first_buy_date = str(buy.get("date") or "")[:10]

        profit_amount = safe_number(trade.get("profitAmount"))
        profit_rate = trade.get("profitRate")
        profit_rate_number = safe_number(profit_rate) if profit_rate is not None else None

        if profit_rate_number is None and matched_cost > 0 and sell_price > 0 and matched_quantity > 0:
            sell_amount = sell_price * matched_quantity
            profit_amount = sell_amount - matched_cost
            profit_rate_number = round((profit_amount / matched_cost) * 100, 2)

        closed_items.append({
            "id": str(trade.get("id") or "").strip(),
            "code": code,
            "name": name,
            "buyDate": first_buy_date,
            "sellDate": str(trade.get("date") or "")[:10],
            "holdingDays": calculate_days_between(first_buy_date, trade.get("date")),
            "sellPrice": round(sell_price, 0),
            "quantity": round(sell_quantity, 4),
            "profitAmount": round(profit_amount, 0),
            "profitRate": round(profit_rate_number, 2) if profit_rate_number is not None else None,
            "reason": str(trade.get("reason") or "").strip(),
        })

    evaluated_items = [item for item in closed_items if item.get("profitRate") is not None]
    rates = [safe_number(item.get("profitRate")) for item in evaluated_items]
    holding_days = [safe_number(item.get("holdingDays")) for item in evaluated_items if item.get("holdingDays") is not None]

    trade_count = len(evaluated_items)
    win_count = sum(1 for rate in rates if rate > 0)
    loss_count = sum(1 for rate in rates if rate < 0)
    average_return_rate = round(sum(rates) / trade_count, 2) if trade_count > 0 else None
    win_rate = round((win_count / trade_count) * 100, 2) if trade_count > 0 else None
    max_profit_rate = round(max(rates), 2) if rates else None
    max_loss_rate = round(min(rates), 2) if rates else None
    average_holding_days = round(sum(holding_days) / len(holding_days), 1) if holding_days else None
    realized_profit = round(sum(safe_number(item.get("profitAmount")) for item in evaluated_items), 0)

    best_item = max(evaluated_items, key=lambda item: safe_number(item.get("profitRate"))) if evaluated_items else None
    worst_item = min(evaluated_items, key=lambda item: safe_number(item.get("profitRate"))) if evaluated_items else None

    return {
        "source": "portfolio.tradeLedger",
        "tradeHistoryPath": str(TRADE_HISTORY_PATH),
        "totalTradeCount": len(trade_history),
        "closedTradeCount": trade_count,
        "winCount": win_count,
        "lossCount": loss_count,
        "winRate": win_rate,
        "averageReturnRate": average_return_rate,
        "maxProfitRate": max_profit_rate,
        "maxLossRate": max_loss_rate,
        "averageHoldingDays": average_holding_days,
        "realizedProfit": realized_profit,
        "bestItem": best_item,
        "worstItem": worst_item,
        "items": sorted(evaluated_items, key=lambda item: safe_number(item.get("profitRate")), reverse=True),
    }

def build_performance_insights(portfolio_performance, recommendation_performance, trade_performance=None):
    insights = []
    total_rate = portfolio_performance.get("totalProfitRate") if isinstance(portfolio_performance, dict) else None
    total_profit = portfolio_performance.get("totalProfitAmount") if isinstance(portfolio_performance, dict) else None
    trade_source = trade_performance if isinstance(trade_performance, dict) else {}
    evaluated_count = trade_source.get("closedTradeCount") if trade_source else recommendation_performance.get("evaluatedCount") if isinstance(recommendation_performance, dict) else 0
    win_rate = trade_source.get("winRate") if trade_source else recommendation_performance.get("winRate") if isinstance(recommendation_performance, dict) else None
    best_item = recommendation_performance.get("bestItem") if isinstance(recommendation_performance, dict) else None
    worst_item = recommendation_performance.get("worstItem") if isinstance(recommendation_performance, dict) else None
    if isinstance(trade_source, dict) and trade_source.get("closedTradeCount"):
        best_item = trade_source.get("bestItem") or best_item
        worst_item = trade_source.get("worstItem") or worst_item

    if total_rate is None:
        insights.append("아직 수익률을 판단할 운용 기간이 부족합니다.")
    elif total_rate >= 0:
        insights.append(f"포트폴리오 누적 수익률은 {total_rate:.2f}%이며 평가 손익은 {round(safe_number(total_profit), 0):,.0f}원입니다.")
    else:
        insights.append(f"포트폴리오 누적 수익률은 {total_rate:.2f}%입니다. 손실 확대 여부를 매일 점검합니다.")

    if safe_number(evaluated_count) > 0 and win_rate is not None:
        insights.append(f"실현 매도 거래 {int(safe_number(evaluated_count))}건 기준 승률은 {win_rate:.2f}%입니다.")
    else:
        insights.append("실제 운용 성과는 자동 매수·매도 체결이 쌓이면 계산됩니다.")

    if isinstance(best_item, dict) and best_item.get("name"):
        insights.append(f"현재 최고 성과 종목은 {best_item.get('name')}입니다.")
    if isinstance(worst_item, dict) and worst_item.get("name"):
        insights.append(f"현재 최저 성과 종목은 {worst_item.get('name')}입니다.")

    return insights[:4]


def build_portfolio_performance_timeline(portfolio, portfolio_summary, base_date):
    """Build a lightweight daily performance timeline without changing the trading engine.

    The engine currently stores trade ledger and current portfolio summary, not full historical
    mark-to-market snapshots. So this timeline records:
    - fund start baseline
    - realized-profit changes on SELL dates
    - current total asset on base_date

    This keeps the existing portfolio logic intact and gives the UI a durable structure that
    can later be upgraded when daily snapshots are accumulated.
    """
    summary = portfolio_summary if isinstance(portfolio_summary, dict) else {}
    source = portfolio if isinstance(portfolio, dict) else {}
    initial_capital = safe_number(summary.get("initialCapital") or source.get("initialCapital") or WABABA_FUND_INITIAL_CAPITAL)
    if initial_capital <= 0:
        initial_capital = WABABA_FUND_INITIAL_CAPITAL

    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    start_date_text = str(source.get("fundStartDate") or WABABA_FUND_START_DATE)[:10]
    current_total_asset = safe_number(summary.get("totalAssetAmount"))
    if current_total_asset <= 0:
        current_total_asset = initial_capital

    ledger = source.get("tradeLedger")
    if not isinstance(ledger, list):
        ledger = []

    realized_by_date = {}
    trade_count_by_date = {}
    for trade in ledger:
        if not isinstance(trade, dict):
            continue
        date_text = str(trade.get("date") or "")[:10]
        if not date_text:
            continue
        action = str(trade.get("action") or "").upper().strip()
        trade_count_by_date[date_text] = trade_count_by_date.get(date_text, 0) + 1
        if action == "SELL":
            realized_by_date[date_text] = realized_by_date.get(date_text, 0) + safe_number(trade.get("profitAmount"))

    dates = {start_date_text, base_date_text}
    dates.update(realized_by_date.keys())
    dates.update(trade_count_by_date.keys())
    ordered_dates = sorted(date for date in dates if date)

    rows = []
    previous_asset = initial_capital
    running_realized = 0

    for date_text in ordered_dates:
        running_realized += realized_by_date.get(date_text, 0)
        if date_text == base_date_text:
            total_asset = current_total_asset
        else:
            total_asset = initial_capital + running_realized

        profit_amount = total_asset - initial_capital
        cumulative_return_rate = round((profit_amount / initial_capital) * 100, 2) if initial_capital > 0 else None
        daily_return_rate = None
        if previous_asset > 0:
            daily_return_rate = round(((total_asset - previous_asset) / previous_asset) * 100, 2)

        rows.append({
            "date": date_text,
            "totalAssetAmount": round(total_asset, 0),
            "profitAmount": round(profit_amount, 0),
            "dailyReturnRate": daily_return_rate,
            "cumulativeReturnRate": cumulative_return_rate,
            "tradeCount": trade_count_by_date.get(date_text, 0),
            "realizedProfitChange": round(realized_by_date.get(date_text, 0), 0),
        })
        previous_asset = total_asset

    return rows[-30:]

def build_wababa_performance_analysis(portfolio, portfolio_summary, today_map, base_date):
    summary = portfolio_summary if isinstance(portfolio_summary, dict) else {}
    initial_capital = safe_number(summary.get("initialCapital") or WABABA_FUND_INITIAL_CAPITAL)
    total_asset_amount = safe_number(summary.get("totalAssetAmount"))
    cash = safe_number(summary.get("cash"))
    total_evaluation_amount = safe_number(summary.get("totalEvaluationAmount"))
    unrealized_profit = safe_number(summary.get("totalProfitAmount"))
    realized_profit = safe_number(summary.get("realizedProfit"))
    total_profit_amount = round((total_asset_amount - initial_capital), 0) if total_asset_amount > 0 and initial_capital > 0 else round(realized_profit + unrealized_profit, 0)

    total_profit_rate = None
    if initial_capital > 0 and total_asset_amount > 0:
        total_profit_rate = round(((total_asset_amount - initial_capital) / initial_capital) * 100, 2)

    daily_return_rate = total_profit_rate
    cumulative_return_rate = total_profit_rate
    daily_returns = build_portfolio_performance_timeline(portfolio, summary, base_date)
    if daily_returns:
        latest_return = daily_returns[-1]
        daily_return_rate = latest_return.get("dailyReturnRate")
        cumulative_return_rate = latest_return.get("cumulativeReturnRate")

    dividend_income = estimate_dividend_income(portfolio, today_map, base_date)
    estimated_dividend_income = safe_number(dividend_income.get("estimatedDividendIncome")) if isinstance(dividend_income, dict) else 0
    total_profit_amount = round(total_profit_amount + estimated_dividend_income, 0)
    if initial_capital > 0 and total_asset_amount > 0:
        total_profit_rate = round(((total_asset_amount + estimated_dividend_income - initial_capital) / initial_capital) * 100, 2)
    daily_return_rate = total_profit_rate if daily_return_rate is None else daily_return_rate
    cumulative_return_rate = total_profit_rate if cumulative_return_rate is None else cumulative_return_rate

    recommendation_performance = build_recommendation_performance(portfolio, today_map, base_date)
    trade_performance = build_actual_trade_performance(portfolio)
    trade_history = build_trade_history_from_portfolio(portfolio)
    strategy_insight = build_wababa_strategy_insight(portfolio, today_map, base_date)

    portfolio_performance = {
        "initialCapital": round(initial_capital, 0),
        "cash": round(cash, 0),
        "totalEvaluationAmount": round(total_evaluation_amount, 0),
        "totalAssetAmount": round(total_asset_amount, 0),
        "realizedProfit": round(realized_profit, 0),
        "unrealizedProfit": round(unrealized_profit, 0),
        "totalProfitAmount": total_profit_amount,
        "estimatedDividendIncome": estimated_dividend_income,
        "totalProfitRate": total_profit_rate,
        "dailyReturnRate": daily_return_rate,
        "cumulativeReturnRate": cumulative_return_rate,
        "label": build_performance_label(total_profit_rate),
        "tone": build_performance_tone(total_profit_rate),
        "dailyReturns": daily_returns,
        "dailyReturnCount": len(daily_returns),
    }

    recent_events = build_recent_performance_events(portfolio, recommendation_performance, base_date)
    insights = build_performance_insights(portfolio_performance, recommendation_performance, trade_performance)

    return {
        "baseDate": str(base_date or "")[:10],
        "generatedAt": now_text(),
        "portfolioPerformance": portfolio_performance,
        "recommendationPerformance": recommendation_performance,
        "tradePerformance": trade_performance,
        "tradeHistory": trade_history,
        "dividendIncome": dividend_income,
        "strategyInsight": strategy_insight,
        "recentEvents": recent_events,
        "insights": insights,
        "summary": {
            "totalReturnRate": total_profit_rate,
            "totalProfitAmount": total_profit_amount,
            "winRate": trade_performance.get("winRate") if trade_performance.get("winRate") is not None else recommendation_performance.get("winRate"),
            "averageReturnRate": trade_performance.get("averageReturnRate") if trade_performance.get("averageReturnRate") is not None else recommendation_performance.get("averageReturnRate"),
            "maxLossRate": trade_performance.get("maxLossRate") if trade_performance.get("maxLossRate") is not None else recommendation_performance.get("maxLossRate"),
            "evaluatedCount": trade_performance.get("closedTradeCount") if trade_performance.get("closedTradeCount") is not None else recommendation_performance.get("evaluatedCount"),
            "averageHoldingDays": trade_performance.get("averageHoldingDays") if trade_performance.get("averageHoldingDays") is not None else recommendation_performance.get("averageHoldingDays"),
            "totalTradeCount": trade_performance.get("totalTradeCount"),
            "closedTradeCount": trade_performance.get("closedTradeCount"),
            "recentEventCount": len(recent_events),
            "dailyReturnCount": len(daily_returns),
        },
    }


def load_filter_config():
    loaded = read_json(FILTER_CONFIG_PATH)

    if not isinstance(loaded, dict):
        return DEFAULT_FILTER_CONFIG

    config = dict(DEFAULT_FILTER_CONFIG)

    for key, value in loaded.items():
        config[key] = value

    return config


def safe_number(value, default=0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def get_items(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ["items", "universe", "data", "stocks", "recommendations"]:
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def get_base_date(data):
    if isinstance(data, dict):
        return data.get("baseDate") or data.get("date") or datetime.now().strftime("%Y-%m-%d")

    return datetime.now().strftime("%Y-%m-%d")


def get_code(item):
    return str(
        item.get("code")
        or item.get("stockCode")
        or item.get("ticker")
        or item.get("symbol")
        or item.get("isin")
        or item.get("name")
        or ""
    ).strip()


def get_name(item):
    return str(
        item.get("name")
        or item.get("stockName")
        or item.get("companyName")
        or item.get("corpName")
        or get_code(item)
    ).strip()


def get_metric(item, keys):
    metrics = item.get("metrics")

    if isinstance(metrics, dict):
        for key in keys:
            if key in metrics:
                return safe_number(metrics.get(key))

    for key in keys:
        if key in item:
            return safe_number(item.get(key))

    return 0


def short_text(value, max_len=64):
    text = str(value or "").strip()
    text = text.replace("\n", " ").replace("\r", " ")
    text = " ".join(text.split())

    if len(text) <= max_len:
        return text

    return text[:max_len].rstrip() + "..."


# 산업군 정밀화 매핑 (Phase 18)
# valuation/decision/businessImpact 분기에서 재사용
_INDUSTRY_GROUP_KEYWORDS = {
    "project": ("기계·장비", "운송장비·부품", "건설"),
    "tech": ("전기·전자", "IT 서비스"),
    "bio": ("제약", "의료·정밀기기"),
    "finance": ("금융", "기타금융", "증권", "은행"),
    "defensive": ("음식료·담배", "유통", "통신", "전기·가스", "전기·가스·수도"),
}


def classify_industry_group(industry_name):
    """industryName 문자열을 5개 그룹("project"/"tech"/"bio"/"finance"/"defensive") 중 하나로 분류.

    매칭 실패 시 빈 문자열 반환.
    키워드 충돌 위험 ("전기" → 전기·전자 vs 전기·가스, "서비스" → IT 서비스 vs 일반서비스)을
    피하기 위해 부분 매칭이 아닌 정확 매칭만 사용.
    """
    if not isinstance(industry_name, str) or not industry_name:
        return ""
    for group, names in _INDUSTRY_GROUP_KEYWORDS.items():
        if industry_name in names:
            return group
    return ""


def build_news_map(news_data):
    news_items = get_items(news_data)
    result = {}

    for item in news_items:
        code = get_code(item)
        name = get_name(item)

        if code:
            result[code] = item

        if name:
            result[name] = item

    return result


def get_news_item(item, news_map):
    code = get_code(item)
    name = get_name(item)

    return news_map.get(code) or news_map.get(name) or {}


def calculate_financial_score(item):
    roe = get_metric(item, ["roe", "ROE"])
    per = get_metric(item, ["per", "PER"])
    pbr = get_metric(item, ["pbr", "PBR"])
    dividend_yield = get_metric(item, ["dividendYield", "divYield", "dividend_yield"])
    operating_margin = get_metric(item, ["operatingMargin", "opMargin"])
    operating_profit_growth = get_metric(
        item,
        ["operatingProfitGrowth", "operatingIncomeGrowth", "opIncomeGrowth"],
    )
    debt_ratio = get_metric(item, ["debtRatio"])
    market_cap = get_metric(item, ["marketCapBillionKrw", "marketCap"])

    score = 0

    if roe >= 15:
        score += 25
    elif roe >= 10:
        score += 18
    elif roe >= 7:
        score += 10

    if 0 < per <= 10:
        score += 18
    elif 10 < per <= 15:
        score += 12
    elif 15 < per <= 25:
        score += 6

    if 0 < pbr <= 1:
        score += 15
    elif 1 < pbr <= 2:
        score += 10
    elif 2 < pbr <= 3:
        score += 4

    if dividend_yield >= 3:
        score += 8
    elif dividend_yield >= 1:
        score += 4

    if operating_margin >= 10:
        score += 12
    elif operating_margin >= 5:
        score += 6

    if operating_profit_growth >= 20:
        score += 12
    elif operating_profit_growth >= 10:
        score += 8
    elif operating_profit_growth >= 0:
        score += 3

    if 0 < debt_ratio <= 100:
        score += 8
    elif 100 < debt_ratio <= 200:
        score += 4

    if market_cap >= 500:
        score += 2

    return score


def calculate_news_score(news_item):
    if not news_item:
        return 0

    for key in ["newsScore", "momentumScore", "issueScore", "score", "newsMomentumScore"]:
        if key in news_item:
            return safe_number(news_item.get(key))

    outlook = str(news_item.get("outlook") or news_item.get("sentiment") or "").lower()

    if outlook in ["positive", "bullish", "good", "긍정"]:
        return 20

    if outlook in ["negative", "bearish", "bad", "부정"]:
        return -30

    return 0


def normalize_item(item, news_map):
    news_item = get_news_item(item, news_map)

    # news_map 매칭 실패 시 item 자체에 내장된 뉴스 필드를 fallback으로 활용한다.
    # financial-universe-real.json은 이미 hypothesis / evidence / newsMomentumScore를
    # 직접 포함하고 있으므로 버리지 않는다.
    if not news_item or not news_item.get("hypothesis"):
        item_hypothesis = str(item.get("hypothesis") or "").strip()
        item_evidence = item.get("evidence") if isinstance(item.get("evidence"), list) else []
        item_news_score = safe_number(item.get("newsMomentumScore"))
        item_themes = item.get("themes") if isinstance(item.get("themes"), list) else []
        item_risk = str(item.get("risk") or "").strip()
        if item_hypothesis or item_evidence or item_news_score:
            news_item = {
                **(news_item or {}),
                "hypothesis": item_hypothesis or (news_item or {}).get("hypothesis", ""),
                "evidence": item_evidence if item_evidence else (news_item or {}).get("evidence", []),
                "newsMomentumScore": item_news_score if item_news_score else (news_item or {}).get("newsMomentumScore", 0),
                "themes": item_themes if item_themes else (news_item or {}).get("themes", []),
                "risk": item_risk or (news_item or {}).get("risk", ""),
            }

    roe = get_metric(item, ["roe", "ROE"])
    per = get_metric(item, ["per", "PER"])
    pbr = get_metric(item, ["pbr", "PBR"])
    dividend_yield = get_metric(item, ["dividendYield", "divYield", "dividend_yield"])
    ebit_margin = get_metric(item, ["ebitMargin", "operatingMargin", "opMargin"])
    operating_profit_growth = get_metric(
        item,
        ["operatingProfitGrowth", "operatingIncomeGrowth", "opIncomeGrowth"],
    )
    market_cap = get_metric(item, ["marketCapBillionKrw", "marketCap"])

    financial_score = calculate_financial_score(item)
    news_score = calculate_news_score(news_item)
    # item에 직접 내장된 newsMomentumScore가 있는데 news_score가 0이면 대체 사용
    if news_score == 0:
        item_momentum = safe_number(item.get("newsMomentumScore"))
        if item_momentum != 0:
            news_score = item_momentum
    total_score = financial_score + news_score

    return {
        **item,
        "code": get_code(item),
        "name": get_name(item),
        "roe": roe,
        "per": per,
        "pbr": pbr,
        "dividendYield": dividend_yield,
        "ebitMargin": ebit_margin,
        "operatingProfitGrowth": operating_profit_growth,
        "marketCapBillionKrw": market_cap,
        "financialScore": financial_score,
        "newsScore": news_score,
        "score": total_score,
        "news": news_item,
    }


def is_buy_candidate(item, config):
    score = safe_number(item.get("score"))
    financial_score = safe_number(item.get("financialScore"))
    news_score = safe_number(item.get("newsScore"))

    min_financial_score = safe_number(config.get("minFinancialScoreForBuy"))
    min_total_score = safe_number(config.get("minTotalScoreForBuy"))
    min_news_score = safe_number(config.get("minNewsScoreForBuy"))

    if financial_score >= min_financial_score and news_score >= min_news_score:
        return True

    if score >= min_total_score and news_score >= min_news_score:
        return True

    return False


def is_wababa_candidate(item, config):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    operating_profit_growth = safe_number(item.get("operatingProfitGrowth"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    min_roe = safe_number(config.get("minRoeForWababa"))
    max_per = safe_number(config.get("maxPerForWababa"))
    min_growth = safe_number(config.get("minOperatingProfitGrowthForWababa"))
    min_market_cap = safe_number(config.get("minMarketCapBillionKrwForWababa"))

    if roe < min_roe:
        return False

    if per <= 0 or per > max_per:
        return False

    if operating_profit_growth < min_growth:
        return False

    if market_cap < min_market_cap:
        return False

    return True


def calculate_wababa_score(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    ebit_margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    news_score = safe_number(item.get("newsScore"))
    financial_score = safe_number(item.get("financialScore"))

    per_bonus = 0

    if 0 < per <= 8:
        per_bonus = 25
    elif per <= 12:
        per_bonus = 18
    elif per <= 18:
        per_bonus = 10
    elif per <= 25:
        per_bonus = 5

    return (
        roe * 2.0
        + ebit_margin * 1.5
        + growth * 1.2
        + per_bonus
        + news_score * 0.8
        + financial_score * 0.5
    )


def calculate_stable_score(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    dividend = safe_number(item.get("dividendYield"))
    financial = safe_number(item.get("financialScore"))

    per_bonus = 0
    if 0 < per <= 8:
        per_bonus = 25
    elif per <= 12:
        per_bonus = 18
    elif per <= 15:
        per_bonus = 10

    pbr_bonus = 0
    if 0 < pbr <= 1:
        pbr_bonus = 15
    elif pbr <= 2:
        pbr_bonus = 8

    return (
        roe * 2.2
        + margin * 1.4
        + dividend * 1.0
        + financial * 0.6
        + per_bonus
        + pbr_bonus
    )


def calculate_growth_score(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    news = safe_number(item.get("newsScore"))

    per_penalty = 0
    if per > 30:
        per_penalty = 20
    elif per > 25:
        per_penalty = 10

    return (
        growth * 2.0
        + roe * 1.2
        + margin * 0.8
        + news * 0.7
        - per_penalty
    )


def calculate_opportunity_score(item):
    news = safe_number(item.get("newsScore"))
    financial = safe_number(item.get("financialScore"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))

    valuation_bonus = 0
    if 0 < per <= 12:
        valuation_bonus += 15
    elif per <= 20:
        valuation_bonus += 8

    if 0 < pbr <= 1:
        valuation_bonus += 10
    elif pbr <= 2:
        valuation_bonus += 5

    return (
        news * 2.0
        + financial * 0.8
        + growth * 0.6
        + valuation_bonus
    )



def calculate_quality_penalty(item):
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    debt_ratio = safe_number(item.get("debtRatio"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))
    news = safe_number(item.get("newsScore"))

    penalty = 0

    if per <= 0:
        penalty += 50
    if pbr <= 0:
        penalty += 15
    if market_cap < 500:
        penalty += 30

    if margin < 5:
        penalty += 18
    elif margin < 8:
        penalty += 8

    if growth < 0:
        penalty += 25
    if sales_growth < 0:
        penalty += 12

    if debt_ratio >= 300:
        penalty += 20
    elif debt_ratio >= 200:
        penalty += 12

    if pbr >= 4:
        penalty += 22
    elif pbr >= 3:
        penalty += 16
    elif pbr >= 2.5:
        penalty += 8

    if growth >= 300 and sales_growth < 20:
        penalty += 35
    elif growth >= 150 and sales_growth < 10:
        penalty += 22

    if growth >= 150 and margin < 7:
        penalty += 18

    if news < 0:
        penalty += 25

    return penalty


def build_quality_warnings(item):
    warnings = []

    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    debt_ratio = safe_number(item.get("debtRatio"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))
    news = safe_number(item.get("newsScore"))

    if market_cap < 500:
        warnings.append("시가총액이 작아 변동성 확인")
    if sales_growth < 0:
        warnings.append("매출 감소 동반 여부 확인")
    if growth >= 300 and sales_growth < 20:
        warnings.append("이익 급증의 일회성 여부 확인")
    elif growth >= 150 and sales_growth < 10:
        warnings.append("이익 증가 지속성 확인")
    if margin < 5:
        warnings.append("영업이익률 낮음")
    if debt_ratio >= 300:
        warnings.append("부채비율 부담 큼")
    elif debt_ratio >= 200:
        warnings.append("부채비율 확인 필요")
    if pbr >= 3:
        warnings.append("PBR 부담 확인")
    if per <= 0 or pbr <= 0:
        warnings.append("PER/PBR 데이터 확인")
    if news < 0:
        warnings.append("부정 뉴스 흐름 확인")

    unique = []
    for warning in warnings:
        if warning and warning not in unique:
            unique.append(warning)

    return unique[:3]


def classify_quality_level(item):
    penalty = calculate_quality_penalty(item)

    if penalty >= 45:
        return "주의"
    if penalty >= 20:
        return "보통"
    return "양호"


def calculate_final_best_score(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    dividend = safe_number(item.get("dividendYield"))
    news = safe_number(item.get("newsScore"))
    financial = safe_number(item.get("financialScore"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    valuation_bonus = 0
    if 0 < per <= 8:
        valuation_bonus += 22
    elif per <= 12:
        valuation_bonus += 16
    elif per <= 18:
        valuation_bonus += 8
    elif per <= 25:
        valuation_bonus += 2

    if 0 < pbr <= 1:
        valuation_bonus += 14
    elif pbr <= 2:
        valuation_bonus += 7

    size_stability_bonus = 0
    if market_cap >= 10000:
        size_stability_bonus = 8
    elif market_cap >= 3000:
        size_stability_bonus = 5
    elif market_cap >= 1000:
        size_stability_bonus = 2

    quality_penalty = calculate_quality_penalty(item)

    return (
        financial * 0.9
        + roe * 1.6
        + margin * 1.2
        + growth * 0.7
        + sales_growth * 0.25
        + dividend * 1.0
        + news * 1.2
        + valuation_bonus
        + size_stability_bonus
        + calculate_growth_consistency_score(item) * 0.35
        - quality_penalty
    )


def is_relaxed_growth_candidate(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    if market_cap < 500:
        return False
    if roe < 7:
        return False
    if per <= 0 or per > 35:
        return False
    if growth >= 10:
        return True
    if sales_growth >= 10 and margin >= 7 and growth >= 0:
        return True
    return False


def pick_final_best(items):
    if not items:
        return None
    return sorted(
        items,
        key=lambda item: safe_number(item.get("todayPickScore") or item.get("finalBestScore")),
        reverse=True,
    )[0]

def get_news_title(item):
    news = item.get("news") or {}

    if not isinstance(news, dict):
        return ""

    return short_text(
        news.get("title")
        or news.get("headline")
        or news.get("summary")
        or news.get("description")
        or "",
        70,
    )


def get_news_dict(item):
    news = item.get("news") or {}
    if isinstance(news, dict):
        return news
    return {}


def get_news_themes(item):
    themes = []

    direct_themes = item.get("themes")
    if isinstance(direct_themes, list):
        themes.extend([str(value).strip() for value in direct_themes if str(value).strip()])

    news = get_news_dict(item)
    news_themes = news.get("themes")
    if isinstance(news_themes, list):
        themes.extend([str(value).strip() for value in news_themes if str(value).strip()])

    unique = []
    for theme in themes:
        if theme not in unique:
            unique.append(theme)

    return unique


def get_news_title(item):
    news = get_news_dict(item)

    title = short_text(
        news.get("title")
        or news.get("headline")
        or news.get("summary")
        or news.get("description")
        or "",
        70,
    )

    if title:
        return title

    evidence = news.get("evidence") or item.get("evidence")
    if isinstance(evidence, list) and evidence:
        return short_text(evidence[0], 70)

    article_summaries = news.get("articleSummaries")
    if isinstance(article_summaries, list):
        for article in article_summaries:
            if isinstance(article, dict):
                article_title = article.get("title") or article.get("summary")
                if article_title:
                    return short_text(article_title, 70)

    return ""


def get_theme_text(item):
    themes = get_news_themes(item)

    if themes:
        return " ".join(themes[:3])

    news_title = get_news_title(item)

    if news_title:
        return news_title

    return ""


def contains_any(text, keywords):
    return any(keyword in text for keyword in keywords)


def build_name_based_story(item):
    name = get_name(item)
    joined = f"{name} {get_theme_text(item)}"

    if contains_any(joined, ["한국전력", "한전", "지역난방", "금화피에스시", "전력", "전기", "변압", "송전", "발전"]):
        return {
            "summary": "전력·에너지 인프라 실적 회복",
            "point": "전력 수요와 설비 투자 확대 흐름에 실적이 연결될 수 있음",
        }

    if contains_any(joined, ["아이스크림미디어", "디지털대성", "밀리의서재", "교육", "콘텐츠", "미디어", "게임", "SOOP"]):
        return {
            "summary": "디지털 콘텐츠·교육 수요 회복",
            "point": "콘텐츠 소비와 교육 플랫폼 수요가 매출 성장으로 이어질 수 있음",
        }

    if contains_any(joined, ["NICE", "나이스", "평가정보", "디앤비", "신용", "데이터"]):
        return {
            "summary": "데이터·신용평가 기반 안정 수익",
            "point": "기업·개인 데이터 수요와 신용평가 서비스 기반 매출이 강점",
        }

    if contains_any(joined, ["코웨이", "쿠쿠", "홈시스", "렌탈", "생활가전"]):
        return {
            "summary": "생활가전·렌탈 현금흐름",
            "point": "반복 매출 성격의 렌탈·생활가전 수요가 실적 방어에 기여",
        }

    if contains_any(joined, ["기아", "현대", "자동차", "오토", "일지테크", "코리아에프티", "THN", "티에이치엔"]):
        return {
            "summary": "자동차 밸류체인 실적 개선",
            "point": "완성차 판매와 부품 공급 흐름이 매출·이익 개선에 연결될 수 있음",
        }

    if contains_any(joined, ["HMM", "해운", "선박", "조선", "동방선기", "대창단조"]):
        return {
            "summary": "해운·조선 밸류체인 회복",
            "point": "운임·수주·기자재 수요 변화가 실적 개선 변수로 작용",
        }

    if contains_any(joined, ["화장품", "뷰티", "미용", "F&F", "영원무역", "의류", "패션"]):
        return {
            "summary": "소비재·수출 회복 관심",
            "point": "브랜드·수출·소비 회복 흐름이 매출 개선으로 이어질 수 있음",
        }

    if contains_any(joined, ["헬스", "바이오", "제약", "의료", "엠아이텍", "원바이오젠", "JW"]):
        return {
            "summary": "헬스케어 실적 성장 관심",
            "point": "의료기기·헬스케어 수요가 매출 성장으로 이어질 수 있음",
        }

    if contains_any(joined, ["반도체", "AI", "데이터센터", "첨단소재", "아바코", "오성첨단소재"]):
        return {
            "summary": "첨단산업 설비투자 회복",
            "point": "반도체·AI·소재 투자 흐름이 장비·부품 수요로 연결될 수 있음",
        }

    return None


def infer_industry_story(item):
    name_story = build_name_based_story(item)

    if name_story:
        return name_story

    joined = f"{get_name(item)} {get_theme_text(item)}"

    if contains_any(joined, ["수주", "계약", "공급", "증설"]):
        return {
            "summary": "수주·계약 모멘텀이 있는 실적 흐름",
            "point": "수주와 공급계약 흐름이 향후 매출 인식으로 이어질 수 있음",
        }

    if contains_any(joined, ["수출", "해외", "북미", "유럽", "중국", "일본"]):
        return {
            "summary": "수출 확대 기대 관심",
            "point": "해외 매출 확대 가능성이 6개월 실적 전망을 높일 수 있음",
        }

    if contains_any(joined, ["신기술", "AI", "로봇", "자동화", "데이터"]):
        return {
            "summary": "신기술 투자 흐름과 연결된 성장",
            "point": "기술 투자와 자동화 수요가 성장성 프리미엄으로 연결될 수 있음",
        }

    if contains_any(joined, ["실적", "영업익", "매출", "흑자", "개선"]):
        return {
            "summary": "실적 개선 확인",
            "point": "매출과 영업이익 개선 흐름이 투자 가설의 핵심",
        }

    return {
        "summary": "실적과 밸류에이션을 함께 통과",
        "point": "재무 점수와 밸류에이션 기준을 통과",
    }


def strip_news_source(title):
    """뉴스 제목 끝의 ' - 출처명' 패턴 제거"""
    text = str(title or "").strip()
    # ' - 출처' 또는 '|출처' 패턴을 제거 (끝부분)
    import re
    text = re.sub(r'\s*[-|]\s*[^\[\]]{1,30}$', '', text).strip()
    return text


def pick_relevant_evidence(item):
    name = get_name(item)
    news = get_news_dict(item)
    candidates = []

    evidence = news.get("evidence") or item.get("evidence")
    if isinstance(evidence, list):
        candidates.extend([str(value).strip() for value in evidence if str(value).strip()])

    # articleSummaries: news_map에서 매칭된 종목의 기사 목록 (해당 종목 관련 기사)
    article_summaries = news.get("articleSummaries")
    article_titles = []
    if isinstance(article_summaries, list):
        for article in article_summaries:
            if isinstance(article, dict):
                title = str(article.get("title") or article.get("summary") or "").strip()
                if title:
                    candidates.append(title)
                    article_titles.append(title)

    if not candidates:
        return ""

    # 1순위: 종목명이 제목에 포함된 기사 (가장 확실한 근거)
    for title in candidates:
        if name and name in title:
            return short_text(strip_news_source(title), 80)

    # 2순위: compact 이름 앞 4글자 매칭
    compact_name = name.replace(" ", "")
    for title in candidates:
        compact_title = title.replace(" ", "")
        if len(compact_name) >= 4 and compact_name[:4] in compact_title:
            return short_text(strip_news_source(title), 80)

    # 3순위: news_map에서 매칭된 기사는 해당 종목 큐레이션 — 첫 번째 기사 반환
    # (evidence 리스트의 직접 문자열 항목은 제외, articleSummaries 항목만 허용)
    if article_titles:
        return short_text(strip_news_source(article_titles[0]), 80)

    return ""


def format_ratio(value, digits=1):
    number = safe_number(value)
    return f"{number:.{digits}f}"


def get_primary_theme_phrase(item):
    themes = get_news_themes(item)
    joined = f"{get_name(item)} {get_theme_text(item)}"

    if themes:
        joined = f"{joined} {' '.join(themes)}"

    if contains_any(joined, ["수주", "계약", "공급", "납품", "증설"]):
        return "수주·계약"
    if contains_any(joined, ["수출", "해외", "북미", "유럽", "일본", "중국", "글로벌"]):
        return "수출·해외 매출"
    if contains_any(joined, ["전력", "전기", "변압", "송전", "발전", "에너지", "인프라", "원전"]):
        return "전력·인프라"
    if contains_any(joined, ["반도체", "AI", "데이터센터", "첨단소재", "자동화", "로봇"]):
        return "첨단산업 투자"
    if contains_any(joined, ["자동차", "부품", "전장", "완성차"]):
        return "자동차 밸류체인"
    if contains_any(joined, ["화장품", "뷰티", "의류", "패션", "소비재"]):
        return "소비재·수출"
    if contains_any(joined, ["의료", "헬스", "바이오", "제약", "의료기기"]):
        return "헬스케어 수요"
    if contains_any(joined, ["교육", "콘텐츠", "미디어", "플랫폼", "게임"]):
        return "디지털 콘텐츠 수요"
    if contains_any(joined, ["실적", "영업익", "매출", "흑자", "개선"]):
        return "실적 개선"

    return "재무·실적 기준"


def build_fact_phrase(item):
    evidence = pick_relevant_evidence(item)
    theme = get_primary_theme_phrase(item)
    sales_growth = safe_number(item.get("salesGrowth"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    roe = safe_number(item.get("roe"))
    news_score = safe_number(item.get("newsScore"))
    industry = str(item.get("industryName") or "").strip()
    # 업종명이 너무 길면 축약해 문장 앞에 붙이기 어렵다
    industry_prefix = f"{industry} 업종: " if industry and len(industry) <= 10 else ""

    # 1순위: 뉴스 기사 제목(evidence) — 가장 구체적이고 종목별로 다름
    if evidence:
        return short_text(evidence, 96)

    # 2순위: hypothesis — 뉴스 기반 투자 가설 텍스트
    hypothesis = str(item.get("hypothesis") or "").strip()
    if not hypothesis:
        news = item.get("news") or {}
        if isinstance(news, dict):
            hypothesis = str(news.get("hypothesis") or "").strip()
    if hypothesis and len(hypothesis) > 10:
        return short_text(hypothesis, 96)

    # 3순위: 재무수치 + 업종 조합 (수치가 종목마다 달라 차별화 기여)
    if sales_growth >= 10 and growth >= 20:
        return f"{industry_prefix}매출 {sales_growth:.1f}%·영업이익 {growth:.1f}% 함께 증가."
    if sales_growth >= 10 and growth >= 0:
        return f"{industry_prefix}매출 {sales_growth:.1f}% 증가, 수익성 훼손은 제한적."
    if growth >= 80:
        return f"{industry_prefix}영업이익 {growth:.1f}% 증가로 이익 개선 폭이 매우 큼."
    if growth >= 30:
        return f"{industry_prefix}영업이익 {growth:.1f}% 증가하며 분기 대비 실적 개선."
    if growth >= 10:
        return f"{industry_prefix}영업이익 {growth:.1f}% 증가로 이익 개선 흐름 확인."
    if margin >= 15:
        return f"{industry_prefix}영업이익률 {margin:.1f}%로 업계 대비 수익성 우수."
    if margin >= 8 and roe >= 10:
        return f"영업이익률 {margin:.1f}%, ROE {roe:.1f}%로 수익성과 자본효율 양호."
    if news_score > 0:
        return f"{theme} 관련 긍정 뉴스 흐름이 지속 중."

    return f"{theme} 기준과 재무 필터를 통과."


def build_business_impact_phrase(item):
    theme = get_primary_theme_phrase(item)
    sales_growth = safe_number(item.get("salesGrowth"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    eps_growth = safe_number(item.get("EPSGrowth3Y"))
    roe = safe_number(item.get("roe"))
    div_yield = safe_number(item.get("dividendYield"))
    industry_name_bi = item.get("industryName") if isinstance(item.get("industryName"), str) else ""
    ig_bi = classify_industry_group(industry_name_bi)
    warnings = build_quality_warnings(item)

    # 종목명 기반 업종 추정 (industryName이 비어있는 환경 보완)
    # SK / 산업 단독 키워드는 너무 광범위해 제외
    _name = get_name(item)
    is_finance_name = any(k in _name for k in [
        "증권", "금융", "은행", "지주", "홀딩스", "캐피탈", "보험", "파이낸셜"
    ])
    is_semi_mfg_name = any(k in _name for k in [
        "반도체", "하이텍", "전자", "세미콘", "마이크로", "테크", "소재"
    ])
    is_project_name = any(k in _name for k in [
        "건설", "중공업", "조선", "엔지니어링", "이엔지", "오션플랜트", "플랜트"
    ])

    # 3년 지속성 보조 문구 (종목별 수치가 달라 차별화에 기여)
    def cagr_note():
        if sales_cagr >= 15:
            return f" 3년 매출 CAGR {sales_cagr:.1f}%로 성장 지속성 확인."
        if sales_cagr >= 8:
            return f" 3년 매출 CAGR {sales_cagr:.1f}%로 중기 성장 흐름 유지."
        if eps_growth >= 20:
            return f" 3년 EPS 성장률 {eps_growth:.1f}%로 이익 체력 지속."
        if eps_growth >= 10:
            return f" 3년 EPS 성장률 {eps_growth:.1f}%로 이익 개선 흐름 유지."
        if sales_cagr > 0 and eps_growth > 0:
            return f" 매출 CAGR {sales_cagr:.1f}%·EPS 성장 {eps_growth:.1f}%로 기본 성장성 확인."
        return ""

    c_note = cagr_note()

    # 지속 가능성 보조 문구 — 현재 실적이 다음 분기에도 유지될 수 있는지 판단
    def sustainability_note():
        industry = str(item.get("industryName") or "")
        cyclical = any(k in industry for k in ["건설", "조선", "철강", "화학", "운송"])

        # 수주 테마에서 이익이 매출 대비 크게 높으면 인식 시점 리스크 존재
        if contains_any(theme, ["수주", "계약"]) and sales_growth > 0 and growth > sales_growth * 1.5:
            return " 다만 수주 인식 시점에 따라 분기 실적 변동성이 있을 수 있습니다."

        # 이익이 매출 증가의 2배 이상 → 고정비 레버리지 효과, 반감 가능성 병기
        if sales_growth > 5 and growth > sales_growth * 2 and growth >= 40:
            return " 고정비 효과의 지속성은 판매량 안정 여부에 달려 있습니다."

        # 경기민감 업종에서 단기 성장이 중기 추세보다 크게 높으면 사이클 주의
        if cyclical and sales_growth >= 20 and sales_cagr > 0 and sales_growth > sales_cagr * 1.5:
            return " 업황 사이클 상단 가능성이 있어 전환 시점을 지속 점검할 필요가 있습니다."

        # 마진·EPS·CAGR 모두 양호하면 지속성 긍정 표현
        if margin >= 12 and eps_growth >= 10 and sales_cagr >= 8:
            return " 이익률과 중기 성장 추세가 함께 뒷받침되어 지속성이 상대적으로 양호합니다."

        return ""

    s_note = sustainability_note()

    if "일회성" in " ".join(warnings):
        if sales_growth >= 10:
            return f"매출 {sales_growth:.1f}% 증가가 동반돼 이익 개선의 질은 확인 가능하나, 급증분의 반복성 점검 필요."
        return "이익 급증은 강하지만 매출 동반 여부가 약해 일회성 개선 가능성 점검 필요."

    if contains_any(theme, ["수주", "계약"]):
        if margin >= 8:
            return f"수주·계약 흐름이 향후 매출 인식으로 이어질 수 있고, 영업이익률 {margin:.1f}% 유지 시 수익성 확보 가능." + c_note + s_note
        # Phase 25 — 수주/계약 margin<8 구간을 업종 그룹과 이익 흐름으로 분기
        if ig_bi == "project":
            if growth < 0:
                return "수주 잔고는 확인되나 단기 이익이 둔화된 구간으로, 원가율 정상화 시점이 다음 분기 관건입니다." + c_note + s_note
            return "수주 잔고가 실적으로 전환되는 속도와 원가율 흐름이 매출 인식의 핵심 점검 변수입니다." + c_note + s_note
        if ig_bi == "tech":
            return "수주·계약 흐름은 매출 인식 기반이 되며, 가동률과 제품 믹스 개선이 이익률 회복의 추가 변수입니다." + c_note + s_note
        if sales_growth >= 5:
            return "수주·계약과 외형 성장이 함께 확인되는 흐름으로, 원가율 관리가 이익 전환의 핵심 변수입니다." + c_note + s_note
        return "수주·계약은 매출 인식 기반이 되나, 마진 유지와 비용 구조 정상화가 이익 전환의 추가 변수입니다." + c_note + s_note
    if contains_any(theme, ["수출", "해외"]):
        if margin >= 8:
            return f"해외 매출 확대는 외형 성장에 직접 연결되고, 영업이익률 {margin:.1f}% 수준의 고마진 품목 비중이 높아지면 이익률 추가 개선 가능." + c_note + s_note
        return "해외 매출 확대는 외형 성장에 직접 연결되고, 고마진 품목 비중이 높아지면 영업이익률 개선 가능." + c_note + s_note
    if contains_any(theme, ["전력", "인프라"]):
        if margin >= 8:
            return f"인프라 투자 확대는 장비·서비스 수요를 키우고, 영업이익률 {margin:.1f}% 수준의 고정비 흡수로 수익성 개선 기여 가능." + c_note + s_note
        return "인프라 투자 확대는 장비·서비스 수요를 키우고, 고정비 흡수로 수익성 개선에 기여 가능." + c_note + s_note
    if contains_any(theme, ["콘텐츠", "교육", "플랫폼"]):
        if margin >= 10:
            return f"콘텐츠·플랫폼 특성상 영업이익률 {margin:.1f}%에서 매출 증가분이 대부분 이익으로 전환될 여지 큼." + c_note + s_note
        return "콘텐츠·플랫폼 매출 증가는 추가 비용 부담이 낮을수록 영업이익률 개선으로 이어질 수 있음." + c_note + s_note
    if contains_any(theme, ["헬스", "의료", "바이오"]):
        if margin >= 8:
            return f"의료·헬스케어 수요 증가는 반복 매출 확대로 이어지며, 영업이익률 {margin:.1f}% 유지 시 안정 수익 구조 강화." + c_note + s_note
        return "의료·헬스케어 수요 증가는 제품 판매와 반복 매출 확대로 이어질 수 있음." + c_note + s_note
    if contains_any(theme, ["자동차"]):
        if margin >= 6:
            return f"완성차 생산·판매 회복은 부품 공급 증가로 연결되고, 가동률 상승 시 영업이익률 {margin:.1f}%에서 레버리지 발생 가능." + c_note + s_note
        return "완성차 생산·판매 회복은 부품 공급 증가로 연결되고, 가동률 상승 시 이익률 개선 가능." + c_note + s_note
    if contains_any(theme, ["반도체", "첨단"]):
        if margin >= 8:
            return f"반도체·첨단소재 투자 흐름이 장비·부품 수요로 연결될 수 있고, 영업이익률 {margin:.1f}% 수준에서 수주 증가 시 이익 레버리지 기대." + c_note + s_note
        return "반도체·첨단소재 투자 흐름이 장비·부품 수요로 연결될 수 있음." + c_note + s_note
    if contains_any(theme, ["방산"]):
        if margin >= 6:
            return f"방산 수주 증가는 장기 매출 파이프라인 확보로 이어지며, 영업이익률 {margin:.1f}% 유지 시 안정적 이익 기반 강화." + c_note + s_note
        return "방산·수주 증가는 장기 매출 파이프라인 확보로 실적 안정성을 높임." + c_note + s_note

    if sales_growth >= 10 and growth >= 20:
        if growth >= sales_growth * 2:
            return "매출 증가분이 대부분 영업이익으로 전환되는 구간입니다. 고정비 흡수와 원가율 개선이 동시에 작동 중입니다." + c_note + s_note
        # 균형 성장 구간 — 종목명/마진별로 세분화해 동일 문장 반복 방지
        if is_finance_name:
            return "이익 회복 속도가 외형보다 빠른 흐름으로, 수익성 사이클이 함께 돌아오는 구간입니다. 금융 성격 업종에서는 이익의 반복성과 자본 효율이 핵심 점검 변수입니다." + c_note + s_note
        if is_semi_mfg_name:
            return "매출과 이익이 동반 회복되며 업황 사이클 진입 가능성이 확인되는 구간입니다. 반도체·제조 성격 종목은 업황 회복과 가동률 흐름이 이익 레버리지 지속성의 핵심 점검 변수입니다." + c_note + s_note
        if is_project_name:
            return "외형 성장이 이익으로 전환되는 흐름이 확인되는 구간입니다. 프로젝트성 사업은 매출 인식 시점과 원가율 흐름이 분기 실적 변동성을 결정할 수 있어 함께 점검할 필요가 있습니다." + c_note + s_note
        if margin >= 8:
            return "매출과 이익이 함께 회복되며 성장의 질이 개선되는 구간입니다. 수익성 지표가 함께 받쳐주고 있어 단순 외형 성장보다 질적인 개선에 가깝습니다." + c_note + s_note
        return "외형 성장에 이익이 따라붙는 형태로, 고정비 흡수 여부가 이익률 회복의 핵심 점검 변수로 작용하는 구간입니다." + c_note + s_note
    if sales_growth >= 10 and margin >= 8:
        return f"매출 확대 시에도 이익률 {margin:.1f}%가 안정적으로 유지되는 구조입니다. 비용 효율이 확보된 상태에서의 성장입니다." + c_note + s_note
    if growth >= 30 and margin >= 8:
        return f"이익 개선 강도가 높고, 이익률 {margin:.1f}% 수준에서 레버리지 효과가 지속될 수 있습니다." + c_note + s_note
    if margin >= 15:
        if sales_growth < 0:
            return "수익성 기반이 두터운 구조가 유지되어 매출 감소 국면에서도 이익 체력 방어가 확인되는 구간입니다." + c_note + s_note
        if roe >= 20:
            return "수익성 기반이 두텁고 자본 효율도 함께 유지되어, 매출 증가가 이익으로 빠르게 전환되는 특성이 있습니다." + c_note + s_note
        if roe >= 10:
            return "수익성 기반이 두터운 구조에서 매출 증가가 이익으로 빠르게 전환되며, 자본 효율 유지 여부가 추세 점검 변수입니다." + c_note + s_note
        return "수익성 기반이 두터운 구조에서 매출 증가가 이익으로 빠르게 전환되나, 자본 효율 회복 여부가 다음 점검 변수입니다." + c_note + s_note

    # 단기 실적 지표가 약해도 중기 성장성으로 보완
    if sales_cagr >= 8:
        return f"3년 매출 CAGR {sales_cagr:.1f}%로 중기 성장 흐름이 유지되고 있음. 단기 실적 변동은 추세 점검 필요."
    if eps_growth >= 10:
        return f"3년 EPS 성장률 {eps_growth:.1f}%로 이익 개선 흐름이 이어지고 있음. 매출 동반 여부 확인 필요."

    # 기본 fallback — 종목명/마진/단기 흐름으로 세분화해 동일 문장 반복 방지
    if margin >= 10:
        return "수익성 자체는 일정 수준을 유지하고 있어, 외형 성장 촉매가 다시 확인되면 이익 체력 회복이 빠를 수 있는 구간입니다." + c_note + s_note
    if is_finance_name:
        return "지주·금융 성격 업종에서는 이익의 반복성과 자산 대비 수익성이 핵심 점검 변수로, 추가 모멘텀보다 체력 유지 여부가 더 중요합니다." + c_note + s_note
    if sales_growth < 0 and growth < 0:
        # Phase 25 — 외형·이익 동반 둔화 구간을 업종과 배당으로 분기
        if ig_bi == "defensive":
            if div_yield >= 1.5:
                return "외형 둔화에도 배당 수준의 방어력은 유지되며, 비용 구조 정상화 시점이 다음 분기 관건입니다." + c_note + s_note
            return "방어 업종 특성상 외형 둔화에도 비용 통제와 가격 전가력이 실적 회복의 핵심 변수입니다." + c_note + s_note
        if ig_bi == "tech":
            return "외형이 축소된 구간으로, 가동률 회복과 제품 믹스 개선 속도가 다음 분기 이익 회복의 핵심 변수입니다." + c_note + s_note
        if ig_bi == "project":
            return "외형·이익 둔화 구간으로, 수주 잔고와 원가율 흐름이 다음 분기 실적 회복 여부를 좌우합니다." + c_note + s_note
        if div_yield >= 1.5:
            return "외형 둔화 구간이나 배당이 일정 부분 방어 요인으로 작동하며, 비용 구조 정상화 시점 확인이 다음 점검 변수입니다." + c_note + s_note
        return "단기 외형이 정체된 상태로, 사업 부문별 회복 시점과 비용 구조 정상화 여부가 다음 분기 관건입니다." + c_note + s_note

    # Phase 25 — 기본 fallback을 업종 그룹·배당·ROE로 추가 분산
    if ig_bi == "tech":
        return "단기 모멘텀이 약한 구간으로, 매출 CAGR과 가동률 회복이 이익 레버리지 재가동의 핵심 점검 변수입니다." + c_note + s_note
    if ig_bi == "project":
        return "단기 모멘텀보다 수주 잔고와 원가율 흐름이 다음 분기 실적 변동성을 좌우할 핵심 점검 변수입니다." + c_note + s_note
    if ig_bi == "bio":
        return "단기 모멘텀보다 파이프라인 진행과 실적 반복성 확인이 우선 점검 변수로 작용하는 구간입니다." + c_note + s_note
    if ig_bi == "finance":
        return "자본 효율과 충당금 부담 흐름이 안정되면 이익 체력 회복으로 연결되며, 추가 모멘텀보다 ROE 유지가 우선 점검 변수입니다." + c_note + s_note
    if ig_bi == "defensive":
        if div_yield >= 1.5:
            return "방어 업종 특성상 배당 방어력은 유지되나, 가격 전가력과 매출 회복 신호가 추가 확인되어야 이익 체력으로 연결됩니다." + c_note + s_note
        return "방어 업종 특성상 비용 구조 안정성은 확보되어 있으나, 외형 회복 신호가 추가 확인되어야 추세 점검이 가능합니다." + c_note + s_note
    if div_yield >= 2:
        return "배당이 일정 부분 방어 요인으로 작동하고 있으나, 추가 성장 신호 없이는 보조 후보 유지 가능성이 큽니다." + c_note + s_note
    if roe >= 10:
        return "자본 효율은 유지되고 있으나, 매출과 이익이 동반 개선되는 신호가 추가 확인되어야 본진 진입 신뢰도가 올라갑니다." + c_note + s_note
    return "현재는 강한 단기 모멘텀보다 재무 체력과 가격 매력을 우선 점검할 단계입니다. 추가 실적 촉매 확인 전까지는 보조 후보로 유지될 가능성이 큽니다." + c_note + s_note


def build_valuation_phrase(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    eps_growth = safe_number(item.get("EPSGrowth3Y"))
    div_yield = safe_number(item.get("dividendYield"))

    # per/pbr 정상 데이터 구간
    if 0 < per <= 8 and roe >= 15:
        return f"ROE {roe:.1f}%인데 PER {per:.1f}배. 이익 체력 대비 가격 부담 낮음."
    if 0 < per <= 10 and (growth >= 20 or sales_growth >= 10):
        return f"PER {per:.1f}배로 실적 증가 속도 대비 밸류 부담 낮음."
    if 0 < per <= 12 and roe >= 12:
        return f"ROE {roe:.1f}% 대비 PER {per:.1f}배. 성장 후보 중 저평가 구간."
    if 0 < per <= 12 and pbr > 1.2:
        return f"PER {per:.1f}배로 이익 대비 가격 부담은 낮으나 PBR {pbr:.1f}배는 자산가치 부담."
    if 0 < pbr <= 1 and roe >= 10:
        return f"PBR {pbr:.1f}배, ROE {roe:.1f}%. 자산가치와 수익성 조합 양호."
    if 0 < pbr <= 1 and sales_growth < 5 and growth < 10:
        return f"PBR {pbr:.1f}배로 가격 부담은 낮으나 성장 신호가 약해 자산가치 방어 성격."
    if 0 < pbr <= 1:
        return f"PBR {pbr:.1f}배로 자산가치 대비 가격 부담 낮음."
    if roe >= 15 and 0 < per <= 18:
        return f"ROE {roe:.1f}%에 PER {per:.1f}배. 고ROE 대비 과열 밸류는 아님."
    if per > 18 and roe >= 15:
        return f"PER {per:.1f}배 부담은 있으나 ROE {roe:.1f}%로 이익 체력이 가격을 일부 보완."
    if 0 < per <= 15:
        return f"PER {per:.1f}배로 절대 가격 부담은 과도하지 않음."
    if per > 15 and pbr > 2 and (sales_cagr >= 8 or eps_growth >= 10):
        # Phase 18 P1a — industry_name으로 D 분기 세분화
        industry_name_d = item.get("industryName") if isinstance(item.get("industryName"), str) else ""
        ig_d = classify_industry_group(industry_name_d)
        if ig_d == "project":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배는 수주 사이클 기대가 선반영된 가격대. 수주 인식 속도와 원가율이 정당화 변수."
        if ig_d == "tech":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배는 성장 프리미엄이 반영된 가격대. 매출 CAGR과 가동률이 정당화 변수."
        if ig_d == "bio":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배는 파이프라인 가치가 선반영된 가격대. 임상·허가 진행과 실적 반복성이 핵심."
        if ig_d == "finance":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배는 자본 효율 대비 부담. ROE 유지와 이익 반복성이 가격 정당화 변수."
        if ig_d == "defensive":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배는 안정 캐시 흐름 프리미엄. 마진 안정성과 배당 방어력이 가격 정당화 변수."
        return f"PER {per:.1f}배·PBR {pbr:.1f}배는 부담이나 성장 지표가 받쳐 밸류 부담 흡수 여지."
    if per > 0 and div_yield >= 3:
        return f"가격 부담은 크지 않으나 성장보다 배당수익률 {div_yield:.1f}%가 방어 요인에 가까움."
    if per > 0 and roe >= 12 and margin >= 10:
        return f"밸류 부담은 낮지 않으나 ROE {roe:.1f}%·이익률 {margin:.1f}%가 수익성 기반 정당화 여지."

    # per=0 / pbr=0: 시장 가격 데이터 미수집 — ROE·margin 기반 내재가치 판단
    if per <= 0 and pbr <= 0:
        if roe >= 25 and margin >= 20:
            return (
                f"ROE {roe:.1f}%·영업이익률 {margin:.1f}%로 수익성 기반 탄탄."
                f" 시장 가격 재확인 후 밸류 판단 가능."
            )
        if roe >= 20 and margin >= 12:
            return (
                f"ROE {roe:.1f}%·영업이익률 {margin:.1f}%로 이익 체력 양호."
                f" PER·PBR 데이터 갱신 후 가격 부담 확인 권장."
            )
        if roe >= 15:
            if eps_growth >= 15:
                return (
                    f"ROE {roe:.1f}%에 3년 EPS 성장률 {eps_growth:.1f}%."
                    f" 이익 지속성 확인 후 밸류 판단 필요."
                )
            return (
                f"ROE {roe:.1f}% 수준으로 수익성 기반 유지."
                f" 시장 가격 확인 후 저평가 여부 판단."
            )
        if roe >= 10:
            if sales_cagr >= 10:
                return (
                    f"ROE {roe:.1f}%·3년 매출 CAGR {sales_cagr:.1f}%."
                    f" 기본 성장성 확인. 가격 데이터 재수집 필요."
                )
            return (
                f"ROE {roe:.1f}%로 기본 수익성 유지."
                f" PER·PBR 데이터 확인 후 가격 부담 판단 필요."
            )
        if roe > 0:
            # ROE 한 자릿수 + 가격 데이터 미수집 구간 — margin/growth/CAGR 강도로 세분화
            if margin >= 12:
                return (
                    f"ROE는 {roe:.1f}%지만 영업이익률 {margin:.1f}% 유지."
                    f" 가격 데이터 보강 후 수익성 기반 재평가 필요."
                )
            if eps_growth >= 15:
                return (
                    f"ROE는 {roe:.1f}%지만 3년 EPS 성장률 {eps_growth:.1f}%."
                    f" 이익 체력 보강 흐름 확인하며 가격 데이터 재수집."
                )
            if sales_cagr >= 10:
                return (
                    f"ROE는 {roe:.1f}%지만 3년 매출 CAGR {sales_cagr:.1f}%."
                    f" 중기 성장 흐름 확인하며 가격 데이터 재수집 필요."
                )
            if growth >= 30:
                return (
                    f"ROE는 {roe:.1f}%지만 영업이익 {growth:.1f}% 증가 흐름."
                    f" 이익 회복 지속 여부와 가격 매력 함께 확인 필요."
                )
            if sales_growth >= 10:
                return (
                    f"ROE는 {roe:.1f}%지만 매출 {sales_growth:.1f}% 증가."
                    f" 수익성 회복 신호 확인 후 가격 매력 재평가."
                )
            if margin < 5 and growth <= 0 and sales_growth <= 0:
                return (
                    f"ROE {roe:.1f}%·이익률 {margin:.1f}% 모두 약해"
                    f" 현재 가격 매력 판단 우선순위는 낮음."
                )
            return (
                f"ROE {roe:.1f}% 수준이며 가격 데이터도 미수집."
                f" 실적 회복 신호와 가격 매력 추가 확인 필요."
            )
        return "수익성 지표 확인 필요. 시장 가격 데이터 재수집 후 밸류 판단."

    # Phase 18 P1b — 최종 fallback 분산 (industry_group + roe + div_yield 조합)
    industry_name_fb = item.get("industryName") if isinstance(item.get("industryName"), str) else ""
    ig_fb = classify_industry_group(industry_name_fb)
    if roe >= 10 and ig_fb == "tech":
        return f"가격 지표만으로 매력 판단은 어렵지만 ROE {roe:.1f}% 수준이 점검 단서. 매출 회복 속도가 다음 변수."
    if roe >= 10 and ig_fb == "project":
        return "가격 지표만으로 매력 판단은 어렵고, 수주 잔고와 마진 회복이 다음 점검 변수."
    if roe >= 10 and ig_fb == "bio":
        return "가격 지표만으로 매력 판단은 어렵고, 파이프라인 진행과 실적 반복성이 다음 점검 변수."
    if roe >= 10 and ig_fb == "finance":
        return f"PER {per:.1f}배·PBR {pbr:.1f}배 구간으로 자본 효율 유지 여부가 가격 정당화 핵심 변수."
    if roe >= 10 and ig_fb == "defensive":
        return "가격 지표만으로 매력 판단은 어렵고, 마진 안정과 배당 흐름이 다음 점검 변수."
    if div_yield >= 3:
        return f"가격 지표만으로 매력 판단은 어렵지만 배당수익률 {div_yield:.1f}%가 부분적 방어. 실적 흐름 점검 필요."

    # Phase 24 — ROE<10 산업×지표 조합으로 final fallback 분산
    # (P24-A) PER 데이터 결손 (per<=0, pbr>0)
    if per <= 0 and pbr > 0:
        if ig_fb == "tech":
            return f"PER 데이터 결손, PBR {pbr:.1f}배 부담만 노출. 이익 지표 갱신 후 가격 매력 재평가 필요."
        if ig_fb == "finance":
            return f"PER 미수집·PBR {pbr:.1f}배 구간으로 자본 효율 데이터 보강 후 가격 판단 필요."
        return f"PER 데이터 결손, PBR {pbr:.1f}배 부담 확인. 이익 지표 갱신 후 재평가 필요."

    # (P24-B) 영업이익 큰 폭 감소
    if growth <= -15:
        if ig_fb == "tech":
            return f"PER {per:.1f}배 부담에 영업이익 {growth:.1f}% 감소가 겹쳐, 출하·마진 회복이 정당화 변수."
        if ig_fb == "project":
            return f"PER {per:.1f}배 가격대에서 영업이익 {growth:.1f}% 감소세, 수주 인식 회복이 선행 변수."
        if ig_fb == "defensive":
            return f"영업이익 {growth:.1f}% 감소 구간으로 안정성보다 비용 구조 정상화가 우선 점검 변수."
        if ig_fb == "finance":
            return f"PER {per:.1f}배 부담 속 영업이익 {growth:.1f}% 감소, 자본 효율 회복 시점이 다음 변수."
        return f"영업이익 {growth:.1f}% 감소가 가격 매력 판단을 가리며, 회복 시점 확인이 필요합니다."

    # (P24-C) 이익 급증 + 가격 프리미엄 동반
    if growth >= 50 and per > 30:
        if ig_fb == "tech":
            return f"PER {per:.1f}배 프리미엄을 영업이익 {growth:.0f}% 증가가 일부 흡수, 반복성과 출하 회복이 정당화 변수."
        if ig_fb == "finance":
            return f"PER {per:.1f}배 부담 속 이익 {growth:.0f}% 증가, 반복성 확인이 가격 정당화 핵심."
        return f"PER {per:.1f}배 부담을 이익 {growth:.0f}% 증가가 일부 흡수, 일회성 여부 점검이 우선."

    # (P24-D) 고 PER 일반 (per > 30, 이익 정체)
    if per > 30:
        if ig_fb == "tech":
            return f"PER {per:.1f}배·PBR {pbr:.1f}배 프리미엄은 출하·마진 개선 확인 시에만 정당화 여지."
        if ig_fb == "defensive":
            return f"PER {per:.1f}배 부담은 안정 캐시 흐름만으로는 정당화 제한적, 마진 회복이 변수."
        if ig_fb == "finance":
            return f"PER {per:.1f}배는 자본 효율 대비 부담, ROE 회복 신호 확인 후 비교 적절."
        if ig_fb == "project":
            return f"PER {per:.1f}배 부담은 수주 인식 속도가 정당화하지 못하면 매력 제한적."
        return f"PER {per:.1f}배 프리미엄은 실적 반등 확인 전까지 정당화 제한적."

    # (P24-E) 마진 양호 + ROE 낮음
    if margin >= 15:
        if ig_fb == "tech":
            return f"영업이익률 {margin:.1f}%는 양호하나 ROE {roe:.1f}%로 자기자본 재투자 효율이 다음 변수."
        if ig_fb == "finance":
            return f"이익률 {margin:.1f}%는 방어 요인이지만 ROE {roe:.1f}%로 자본 효율 회복 시점이 핵심."
        return f"영업이익률 {margin:.1f}%는 양호하지만 ROE {roe:.1f}%로 가격 매력 정당화는 제한적."

    # (P24-F) 매출 감소
    if sales_growth <= -3:
        if ig_fb == "defensive":
            return f"매출 {sales_growth:.1f}% 감소 흐름이 가격 매력을 가리며, 비용 구조 정상화가 다음 변수."
        if ig_fb == "tech":
            return f"매출 {sales_growth:.1f}% 감소 구간으로 출하 회복 시점이 가격 매력 정당화 선행 변수."
        return f"매출 {sales_growth:.1f}% 감소 구간으로 외형 회복 신호가 가격 매력 정당화 선행 변수."

    # (P24-G) 매출 성장 양호 + ROE 낮음
    if sales_growth >= 5 and per > 0:
        if ig_fb == "tech":
            return f"매출 {sales_growth:.1f}% 성장은 긍정적이나 PER {per:.1f}배 부담 흡수에 이익률 회복이 필요."
        return f"매출 {sales_growth:.1f}% 성장은 받쳐주나 ROE {roe:.1f}%로 가격 매력 정당화에 시간 필요."

    # (P24-H) 산업별 일반 fallback (ROE 낮음 + 위 조건 모두 비해당)
    if ig_fb == "tech":
        return f"PER {per:.1f}배·PBR {pbr:.1f}배 부담 속 ROE {roe:.1f}%로 가격 매력은 출하·마진 회복 후 재평가."
    if ig_fb == "project":
        return f"PER {per:.1f}배 가격대에서 수주 잔고 회복과 마진 정상화가 가격 매력 판단의 선행 변수."
    if ig_fb == "bio":
        return f"PER {per:.1f}배 부담은 파이프라인 진행과 매출 전환 확인 시점에 정당화 여부 결정."
    if ig_fb == "finance":
        return f"PER {per:.1f}배·PBR {pbr:.1f}배는 자본 효율 회복 시점에 따라 가격 매력 판단이 달라집니다."
    if ig_fb == "defensive":
        return f"PER {per:.1f}배 부담은 마진 안정과 비용 구조 정상화 확인 시 정당화 여지가 생깁니다."

    return "가격 지표만으로 저평가를 말하기는 어렵고, 실적 지속성과 추가 성장 신호 확인이 필요합니다."


def build_decision_phrase(item):
    entry = build_entry_view(item)
    quality_level = classify_quality_level(item)
    warnings = build_quality_warnings(item)
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    roe = safe_number(item.get("roe"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    eps_growth = safe_number(item.get("EPSGrowth3Y"))
    news_score = safe_number(item.get("newsScore"))
    div_yield = safe_number(item.get("dividendYield"))
    industry_name_dp = item.get("industryName") if isinstance(item.get("industryName"), str) else ""
    ig = classify_industry_group(industry_name_dp)

    undervalued = (0 < per <= 12) or (0 < pbr <= 1)
    improving = growth >= 20 or sales_growth >= 10

    if entry == "매수 가능" and improving and undervalued and quality_level == "양호":
        return "실적 상승 초입과 저평가가 겹친 구간. 오늘 우선 매수 검토."
    if entry == "매수 가능" and improving and undervalued:
        caution = f" 단, {warnings[0]}." if warnings else ""
        return f"실적 개선과 낮은 밸류가 함께 남은 구간. 분할 매수 우선.{caution}"

    if entry == "분할 매수":
        # growth 강도별 분기
        if growth >= 60 and margin >= 20:
            return (
                f"영업이익 {growth:.0f}% 증가에 이익률 {margin:.1f}% 수준."
                f" 이익 레버리지 구간 — 분할 진입 후 지속성 확인."
            )
        if growth >= 60:
            return (
                f"영업이익 {growth:.0f}% 증가 구간."
                f" 급증의 반복성 점검하며 분할 접근."
            )
        if growth >= 30 and margin >= 15:
            return (
                f"영업이익 {growth:.0f}% 증가·이익률 {margin:.1f}%로 수익성 개선 강도 높음."
                f" 분할 매수 우선."
            )
        if growth >= 30:
            return (
                f"영업이익 {growth:.0f}% 증가 중."
                f" 실적 지속성 확인하며 분할 접근 가능."
            )
        if sales_cagr >= 20:
            return (
                f"3년 매출 CAGR {sales_cagr:.1f}% 흐름 유지 중."
                f" 중기 성장 지속 여부 확인하며 분할 진입."
            )
        if eps_growth >= 20:
            return (
                f"3년 EPS 성장률 {eps_growth:.1f}%로 이익 체력 확인."
                f" 분할 접근으로 리스크 관리."
            )
        if sales_cagr >= 10:
            return (
                f"3년 매출 CAGR {sales_cagr:.1f}% 기반 성장 흐름."
                f" 단기 실적 모멘텀 병행 확인하며 분할 접근."
            )
        if roe >= 20 and margin >= 15:
            return (
                f"ROE {roe:.1f}%·이익률 {margin:.1f}%로 수익성 기반 양호."
                f" 가격 부담 확인 후 분할 진입 권장."
            )
        if improving:
            return (
                f"실적 개선({growth:.0f}%) 흐름 유지 중."
                f" 수익성 지속 여부 점검하며 분할 접근."
            )
        return "수익성 기반 확인. 추가 모멘텀 확인 후 분할 접근."

    if improving:
        # 가격 데이터 미수집(per=0) 등으로 entry가 "매수 보류"여도
        # 실적 강도에 따라 narrative 차별화 — entry/scoring 로직 변경 없음
        if growth >= 60 and margin >= 20:
            return (
                f"영업이익 {growth:.0f}% 증가·이익률 {margin:.1f}%로 실적 강도 높음."
                f" 가격 데이터 확인 후 재검토 권장."
            )
        if growth >= 60:
            if ig == "project":
                return (
                    f"영업이익 {growth:.0f}% 급증."
                    f" 수주 인식 집중 여부 점검하며 보조 후보 유지."
                )
            if ig == "finance":
                return (
                    f"영업이익 {growth:.0f}% 급증."
                    f" 비이자이익·충당금 구조 확인 후 보조 후보 유지."
                )
            if ig == "defensive":
                return (
                    f"영업이익 {growth:.0f}% 급증."
                    f" 비용 구조 개선 여부 확인하며 보조 후보 유지."
                )
            if margin >= 10:
                return (
                    f"영업이익 {growth:.0f}% 급증·이익률 {margin:.1f}% 유지."
                    f" 일회성 여부 점검 후 보조 후보 유지."
                )
            return (
                f"영업이익 {growth:.0f}% 급증 구간."
                f" 일회성 여부 점검 후 보조 후보로 유지."
            )
        if growth >= 30 and margin >= 12:
            return (
                f"영업이익 {growth:.0f}% 증가·이익률 {margin:.1f}% 유지."
                f" 실적 개선 흐름 확인 중. 가격 확인 후 재평가."
            )
        if growth >= 30:
            return (
                f"영업이익 {growth:.0f}% 증가 중."
                f" 리스크 요인 확인 후 보조 후보 유지."
            )
        if sales_cagr >= 15:
            return (
                f"3년 매출 CAGR {sales_cagr:.1f}% 기반 성장 흐름."
                f" 단기 실적 모멘텀과 함께 가격 확인 필요."
            )
        if eps_growth >= 15:
            return (
                f"3년 EPS 성장률 {eps_growth:.1f}%로 이익 체력 유지."
                f" 리스크 확인 후 보조 후보 유지."
            )
        if sales_growth >= 10 and margin >= 15:
            return (
                f"매출 {sales_growth:.1f}% 증가·이익률 {margin:.1f}% 유지."
                f" 수익성 지속 확인하며 보조 후보 유지."
            )
        return (
            f"실적 개선({growth:.0f}%) 흐름은 유지 중."
            f" 가격 데이터 재확인 후 재검토."
        )
    if undervalued:
        if ig == "project":
            if 0 < per <= 12:
                return f"PER {per:.1f}배로 가격 부담은 낮으나, 수주 잔고 증가 확인 전까지 보조 후보."
            return "수주 사이클 대비 가격 부담은 낮으나, 수주 잔고 증가 확인 전까지 보조 후보."
        if ig == "finance":
            return "자산 대비 가격 매력은 있으나 이익 성장 신호 확인 전까지 보조 후보."
        if ig == "defensive":
            if div_yield >= 3:
                return f"배당수익률 {div_yield:.1f}%가 방어 요인. 성장 촉매 확인 전까지 관찰 후보."
            return "방어 업종으로 가격 부담은 낮으나 외형 성장 신호 확인이 필요합니다."
        if ig == "tech":
            return "가격 지표로는 매력적이나 제품 사이클 회복 확인 전까지 보조 후보."
        if ig == "bio":
            return "파이프라인 대비 가격 부담은 낮은 편이나 임상 진전 확인 후 재평가."
        if 0 < per <= 8:
            return f"PER {per:.1f}배로 저평가 구간이나 성장 촉매 확인 전까지 보조 후보."
        if 0 < pbr <= 0.7:
            return f"PBR {pbr:.1f}배로 자산 대비 저평가. 이익 회복 신호 확인 필요."
        # Phase 26-A — general undervalued fallback을 배당·자본효율·마진으로 분기
        if div_yield >= 3:
            return f"배당수익률 {div_yield:.1f}%가 방어 요인이나, 이익 회복 신호 확인 전까지 관찰 후보 유지."
        if roe >= 12 and growth >= 10:
            return f"영업이익 {growth:.0f}% 증가와 자본 효율 유지가 확인되나, 추가 촉매 확인 후 재평가 단계."
        if margin >= 10:
            return f"이익률 {margin:.1f}% 수준의 수익성은 유지되나, 가격 매력만으로는 진입 결정에 추가 점검 필요."
        return "가격 매력은 있으나 성장 촉매 확인 전까지 보조 후보."

    # 일반 fallback — 단기 실적이 약하면서 가격 매력도 약한 구간.
    # 와바바 가치성장 철학에 따라 중기 성장 흐름을 margin/ROE보다 먼저 반영.
    if sales_growth < 0 and growth < 0:
        if ig == "project":
            return f"수주 인식 지연 가능성으로 매출·이익 동반 약화. 다음 재무 업데이트 후 재검토."
        if ig == "defensive":
            return "방어 업종임에도 단기 실적 동반 하락 중. 비용 구조 정상화 여부 점검 필요."
        return f"매출·영업이익 동반 약화 구간. 다음 재무 업데이트 후 재검토가 필요합니다."
    if sales_cagr >= 8 or eps_growth >= 10:
        if sales_cagr >= 8 and eps_growth >= 10:
            return (
                f"단기 실적은 약하지만 3년 매출 CAGR {sales_cagr:.1f}%·EPS 성장률 {eps_growth:.1f}% 유지."
                f" 다음 분기 추이 점검."
            )
        if sales_cagr >= 8:
            return (
                f"단기 실적은 약하지만 3년 매출 CAGR {sales_cagr:.1f}%로 중기 성장 흐름 확인."
                f" 다음 분기 점검 필요."
            )
        return (
            f"단기 실적은 약하지만 EPS 성장률 {eps_growth:.1f}%로 이익 체력 유지."
            f" 다음 분기 추이 점검."
        )
    if margin >= 12:
        return f"이익률 {margin:.1f}%로 양호하지만, 성장 촉매 확인 전까지는 비교 후보로 두는 편이 적절합니다."
    if roe >= 10:
        return "자본 효율은 유지되고 있어, 외형 회복 신호가 더 확인되면 매수 강도를 다시 점검할 수 있습니다."
    if news_score > 0:
        if ig == "project":
            return "수주 관련 뉴스 모멘텀은 있으나 재무 수치 확인 전까지 관찰 후보에 가깝습니다."
        if ig == "bio":
            return "임상·파이프라인 뉴스 모멘텀은 있으나 매출 전환 확인 전까지 관찰 후보."
        if ig == "tech":
            # Phase 26-A — tech 뉴스 분기를 이익 흐름·외형 흐름으로 분기
            if growth <= -10:
                return f"업황 회복 기대 뉴스는 있으나 영업이익 {growth:.0f}% 감소 구간으로, 출하 회복 확인이 선행 변수입니다."
            if sales_growth >= 5:
                return "업황 회복 기대 뉴스와 외형 성장이 함께 확인되나, 마진 회복 신호가 다음 점검 변수입니다."
            return "업황 회복 기대 뉴스는 있으나 출하·수주 데이터로 재무 확인이 필요합니다."
        if ig == "finance":
            return "섹터 뉴스 모멘텀은 있으나 이익 체력 확인 전까지 관찰 후보에 가깝습니다."
        return "뉴스 기반 모멘텀은 있으나 재무 확증이 더 필요해 관찰 후보에 가깝습니다."
    return "단기 모멘텀과 수익성 강도가 약해 비교 후보로 유지하고, 다음 업데이트에서 다시 점검하는 편이 적절합니다."


def build_investment_report(item):
    report = {
        "fact": build_fact_phrase(item),
        "businessImpact": build_business_impact_phrase(item),
        "valuation": build_valuation_phrase(item),
        "decision": build_decision_phrase(item),
    }

    cleaned = {}
    banned = [
        "최근 이슈와 실적 흐름",
        "먼저 확인했습니다",
        "가능성을 함께 보는",
        "관찰 구간",
        "AI",
    ]

    for key, value in report.items():
        text = short_text(value, 130).strip()
        for word in banned:
            text = text.replace(word, "")
        text = " ".join(text.split()).strip()
        cleaned[key] = text

    return cleaned


def calculate_decision_score_breakdown(item):
    sales_growth = safe_number(item.get("salesGrowth"))
    profit_growth = safe_number(item.get("operatingProfitGrowth"))
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    quality_penalty = calculate_quality_penalty(item)
    news_score = safe_number(item.get("newsScore"))

    growth = 35
    if sales_growth >= 20:
        growth += 25
    elif sales_growth >= 10:
        growth += 18
    elif sales_growth >= 5:
        growth += 8

    if profit_growth >= 80:
        growth += 30
    elif profit_growth >= 30:
        growth += 24
    elif profit_growth >= 10:
        growth += 14
    elif profit_growth < 0:
        growth -= 20

    profitability = 35
    if roe >= 20:
        profitability += 30
    elif roe >= 15:
        profitability += 24
    elif roe >= 10:
        profitability += 15
    elif roe < 5:
        profitability -= 15

    if margin >= 20:
        profitability += 25
    elif margin >= 12:
        profitability += 18
    elif margin >= 8:
        profitability += 10
    elif margin < 5:
        profitability -= 15

    valuation = 35
    if 0 < per <= 8:
        valuation += 30
    elif per <= 12:
        valuation += 23
    elif per <= 18:
        valuation += 12
    elif per <= 25:
        valuation += 5
    else:
        valuation -= 10

    if 0 < pbr <= 1:
        valuation += 20
    elif pbr <= 2:
        valuation += 10
    elif pbr >= 3:
        valuation -= 15

    quality = 90 - quality_penalty
    if news_score > 0:
        quality += min(news_score, 10)
    if news_score < 0:
        quality -= 20

    return {
        "growth": max(0, min(100, round(growth))),
        "profitability": max(0, min(100, round(profitability))),
        "valuation": max(0, min(100, round(valuation))),
        "quality": max(0, min(100, round(quality))),
    }


def build_buy_trigger(item, breakdown):
    sales_growth = safe_number(item.get("salesGrowth"))
    profit_growth = safe_number(item.get("operatingProfitGrowth"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    roe = safe_number(item.get("roe"))

    if sales_growth >= 10 and profit_growth >= 20 and 0 < per <= 12:
        return "매출·영업이익 동반 증가 + 낮은 PER"
    if profit_growth >= 50 and 0 < pbr <= 1:
        return "이익 급증 + 낮은 PBR"
    if roe >= 15 and 0 < per <= 10:
        return "고ROE + 저PER"
    if sales_growth >= 10 and breakdown.get("quality", 0) >= 75:
        return "매출 성장 + 재무 품질 양호"
    if 0 < per <= 12 or 0 < pbr <= 1:
        return "밸류 부담 낮음"
    return "재무 기준 통과"


def build_decision_blocker(item):
    warnings = build_quality_warnings(item)
    if warnings:
        return warnings[0]

    sales_growth = safe_number(item.get("salesGrowth"))
    profit_growth = safe_number(item.get("operatingProfitGrowth"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    debt_ratio = safe_number(item.get("debtRatio"))

    if profit_growth >= 120 and sales_growth < 10:
        return "이익 급증의 일회성 여부"
    if sales_growth < 0:
        return "매출 감소 동반 여부"
    if margin < 8:
        return "수익성 유지 여부"
    if debt_ratio >= 200:
        return "부채비율 부담"
    if pbr >= 3:
        return "PBR 부담"
    if per >= 20:
        return "PER 부담"
    return "특별한 차단 요인은 작음"


def calculate_decision_confidence(breakdown, item):
    confidence = (
        breakdown.get("growth", 0) * 0.28
        + breakdown.get("profitability", 0) * 0.24
        + breakdown.get("valuation", 0) * 0.24
        + breakdown.get("quality", 0) * 0.24
    )

    news_score = safe_number(item.get("newsScore"))
    if news_score > 0:
        confidence += min(news_score * 0.4, 5)
    if news_score < 0:
        confidence -= 8

    return max(0, min(95, round(confidence)))



# =============================================================================
# 와바바 가치투자 펀드 운용 규칙 v1.0
# 핵심: 고성장 + 저평가 + 성장 힌트. 성장이 유지되는 한 장기 보유.
# =============================================================================

WABABA_VALUE_RULES = {
    "minRoe": 15,
    "minOperatingProfitGrowth": 15,
    "maxBuyPer": 15,
    "holdMinRoe": 15,
    "growthSignalMinScore": 1,
    "longTermHoldingYears": 3,
}


def detect_growth_signal_tags(item):
    """뉴스/사업/업황 힌트를 간단 점수화한다. 없으면 재무 성장 자체를 보조 힌트로 인정한다."""
    tags = []
    text_parts = []

    for key in ["hypothesis", "companySummary", "investmentThesis", "industryMomentum", "growthCatalyst", "agentReport", "naturalReport"]:
        value = item.get(key) if isinstance(item, dict) else ""
        if isinstance(value, str) and value.strip():
            text_parts.append(value)

    news = item.get("news") if isinstance(item, dict) else None
    if isinstance(news, dict):
        for key in ["hypothesis", "outlook6M", "entrySignal"]:
            value = news.get(key)
            if isinstance(value, str) and value.strip():
                text_parts.append(value)
        raw_themes = news.get("themes")
        if isinstance(raw_themes, list):
            for value in raw_themes:
                if isinstance(value, str) and value.strip():
                    text_parts.append(value)

    evidence = item.get("evidence") if isinstance(item, dict) else None
    if isinstance(evidence, list):
        text_parts.extend(str(value) for value in evidence[:5])

    combined = " ".join(text_parts)

    keyword_groups = [
        ("수출", ["수출", "해외", "글로벌", "북미", "미국", "유럽", "중국", "인도"]),
        ("수주", ["수주", "계약", "공급", "납품", "증설", "투자 확대"]),
        ("신사업", ["신사업", "신제품", "신규", "플랫폼", "AI", "반도체", "전장", "로봇", "배터리"]),
        ("업황", ["턴어라운드", "업황", "수요", "가격 상승", "회복", "호조"]),
    ]

    for label, keywords in keyword_groups:
        if any(keyword in combined for keyword in keywords):
            tags.append(label)

    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    if sales_growth >= 10 and op_growth >= 15 and "실적" not in tags:
        tags.append("실적")

    return tags[:4]


def calculate_growth_signal_score(item):
    return len(detect_growth_signal_tags(item))


def is_wababa_value_buy_candidate(item):
    roe = safe_number(item.get("roe") or item.get("ROE"))
    per = safe_number(item.get("per") or item.get("PER"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    quality_penalty = calculate_quality_penalty(item)
    growth_signal_score = calculate_growth_signal_score(item)

    return (
        roe >= WABABA_VALUE_RULES["minRoe"]
        and op_growth >= WABABA_VALUE_RULES["minOperatingProfitGrowth"]
        and per > 0
        and per <= WABABA_VALUE_RULES["maxBuyPer"]
        and growth_signal_score >= WABABA_VALUE_RULES["growthSignalMinScore"]
        and quality_penalty < 35
    )


def build_buy_reason(item):
    roe = safe_number(item.get("roe") or item.get("ROE"))
    per = safe_number(item.get("per") or item.get("PER"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    tags = detect_growth_signal_tags(item)

    reasons = []
    if roe > 0:
        reasons.append(f"ROE {roe:.1f}%")
    if op_growth != 0:
        reasons.append(f"영업이익 증가율 {op_growth:.1f}%")
    if per > 0:
        reasons.append(f"PER {per:.1f}배")
    if tags:
        reasons.append("성장 힌트 " + "/".join(tags[:3]))

    if is_wababa_value_buy_candidate(item):
        headline = "고성장 + 저평가 + 성장 힌트 충족"
    else:
        headline = "매수 기준 일부 미달, 관찰 우선"

    return [headline] + reasons[:4]


def build_hold_reason(item, position=None):
    roe = safe_number(item.get("roe") or item.get("ROE"))
    per = safe_number(item.get("per") or item.get("PER"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    tags = detect_growth_signal_tags(item)
    profit_rate = calculate_profit_rate(item, position) if position else None

    reasons = []
    if roe >= WABABA_VALUE_RULES["holdMinRoe"] and op_growth >= 0:
        reasons.append("성장 가설 유지")
    elif op_growth >= 0:
        reasons.append("이익 성장 유지, ROE 추가 확인")
    else:
        reasons.append("성장 가설 약화, 점검 필요")

    if roe > 0:
        reasons.append(f"ROE {roe:.1f}%")
    if op_growth != 0:
        reasons.append(f"영업이익 증가율 {op_growth:.1f}%")
    if per > 0:
        reasons.append(f"PER {per:.1f}배는 단독 매도 사유 아님")
    if profit_rate is not None:
        reasons.append(f"평가수익률 {profit_rate:.1f}%")
    if tags:
        reasons.append("힌트 유지 " + "/".join(tags[:2]))

    return reasons[:5]


def build_sell_reason(item, position=None, better_opportunity=None):
    roe = safe_number(item.get("roe") or item.get("ROE"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    news_score = safe_number(item.get("newsScore") or item.get("newsMomentumScore"))
    reasons = []

    if op_growth < 0:
        reasons.append("영업이익 감소 → 성장 가설 붕괴 점검")
    if sales_growth < 0 and op_growth < 10:
        reasons.append("매출 감소 동반 → 성장 지속성 약화")
    if roe > 0 and roe < 10:
        reasons.append(f"ROE {roe:.1f}%로 고성장 기준 이탈")
    if news_score <= -30:
        reasons.append("부정 뉴스 모멘텀 확대")
    if better_opportunity:
        reasons.append("더 높은 성장 + 더 낮은 PER 후보 발견")

    if not reasons:
        reasons.append("성장 가설 유지, 매도 사유 없음")

    return reasons[:4]


def infer_industry_tailwind(item):
    text = " ".join(
        str(item.get(key) or "")
        for key in ["industryName", "hypothesis", "companySummary", "investmentThesis", "industryMomentum", "growthCatalyst", "agentReport", "naturalReport"]
    )
    news = item.get("news") if isinstance(item, dict) else None
    if isinstance(news, dict):
        text += " " + " ".join(str(news.get(key) or "") for key in ["hypothesis", "outlook6M", "entrySignal"])
        themes = news.get("themes")
        if isinstance(themes, list):
            text += " " + " ".join(str(value) for value in themes)

    rules = [
        ("전력·인프라 투자 확대 수혜 가능성", ["전력", "전기", "인프라", "변압", "송전", "배전", "설비"]),
        ("AI·반도체 투자 확대 수혜 가능성", ["AI", "반도체", "데이터센터", "서버", "HBM", "장비"]),
        ("조선·해양 업황 회복 수혜 가능성", ["조선", "선박", "해양", "LNG", "수주"]),
        ("헬스케어·의료 수요 확대 수혜 가능성", ["헬스케어", "의료", "병원", "바이오", "제약"]),
        ("콘텐츠·플랫폼 매출 확대 수혜 가능성", ["콘텐츠", "게임", "플랫폼", "구독", "미디어"]),
        ("자동차·전장 수요 회복 수혜 가능성", ["자동차", "전장", "부품", "모빌리티", "EV"]),
        ("방산·수주 산업 성장 수혜 가능성", ["방산", "방위", "국방", "로템"]),
        ("소비 회복과 브랜드 수요 확대 가능성", ["소비", "화장품", "식품", "유통", "브랜드"]),
        ("기계·정밀 투자 사이클 수혜 가능성", ["기계", "정밀", "공작기계", "금형", "설비투자"]),
        ("소재·화학 업황 회복 가능성", ["소재", "철강", "화학", "금속", "비철"]),
        ("금융·보험 수익성 개선 가능성", ["금융", "보험", "증권", "은행", "캐피탈", "저축"]),
        ("물류·운송 수요 회복 수혜 가능성", ["물류", "운송", "항공", "항만", "배송"]),
        ("건설·부동산 수주 회복 가능성", ["건설", "시공", "분양", "도급", "인테리어"]),
        ("소프트웨어·IT서비스 성장 가능성", ["소프트웨어", "시스템", "SI", "ERP", "클라우드"]),
        ("식품·농업 안정 수요 기반 성장", ["식품", "농업", "사료", "가공", "음료"]),
    ]

    for label, keywords in rules:
        if any(keyword in text for keyword in keywords):
            return label

    tags = detect_growth_signal_tags(item)
    if "수출" in tags:
        return "해외 매출 확대 수혜 가능성"
    if "수주" in tags:
        return "수주 증가에 따른 실적 개선 가능성"
    if "신사업" in tags:
        return "신사업 매출 확대 가능성"
    if "업황" in tags:
        return "업황 회복에 따른 이익 개선 가능성"

    # industryName 직접 활용 — 키워드 매칭 실패 시 업종명으로 차별화
    industry = str(item.get("industryName") or "").strip()
    if industry:
        growth = safe_number(item.get("operatingProfitGrowth"))
        sales_growth = safe_number(item.get("salesGrowth"))
        if growth >= 20 or sales_growth >= 10:
            return f"{industry} 업종 내 실적 개선으로 재평가 가능성"
        return f"{industry} 업종 기반 안정 수익 가능성"

    return "실적 개선이 이어질 경우 재평가 가능성"


def build_core_catalyst(item):
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    roe = safe_number(item.get("roe") or item.get("ROE"))
    per = safe_number(item.get("per") or item.get("PER"))
    tags = detect_growth_signal_tags(item)

    parts = []
    if sales_growth >= 10 and op_growth >= 15:
        parts.append(f"매출 {sales_growth:.1f}% + 영업이익 {op_growth:.1f}% 동반 성장")
    elif op_growth >= 15:
        parts.append(f"영업이익 {op_growth:.1f}% 증가")
    elif sales_growth >= 10:
        parts.append(f"매출 {sales_growth:.1f}% 증가")

    if roe >= 15 and 0 < per <= 15:
        parts.append(f"ROE {roe:.1f}% 대비 PER {per:.1f}배")
    elif 0 < per <= 15:
        parts.append(f"PER {per:.1f}배로 저평가 구간")

    if tags:
        parts.append("성장 힌트 " + "/".join(tags[:2]))

    if not parts:
        parts.append("성장성과 저평가 조건 추가 확인")

    return parts[:3]



def calculate_growth_consistency_score(item):
    """와바바 가치투자용 성장 지속성 점수.

    단기 급등보다 오래 돈을 벌 수 있는지를 보기 위한 보조 점수다.
    0~100 범위로 계산하며, 최종 추천 우선순위와 자동매수 후보 선정에 반영한다.
    """
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    roe = safe_number(item.get("roe") or item.get("ROE"))
    per = safe_number(item.get("per") or item.get("PER"))
    margin = safe_number(item.get("ebitMargin") or item.get("opMargin"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    eps_growth = safe_number(item.get("EPSGrowth3Y") or item.get("epsGrowth3Y"))
    debt_ratio = safe_number(item.get("debtRatio"))
    quality_penalty = calculate_quality_penalty(item)
    growth_signal_score = calculate_growth_signal_score(item)

    score = 40

    if sales_growth >= 20:
        score += 14
    elif sales_growth >= 10:
        score += 10
    elif sales_growth >= 0:
        score += 4
    else:
        score -= 12

    if op_growth >= 80 and sales_growth >= 10:
        score += 14
    elif op_growth >= 30:
        score += 12
    elif op_growth >= 15:
        score += 8
    elif op_growth >= 0:
        score += 3
    else:
        score -= 16

    if roe >= 20:
        score += 12
    elif roe >= 15:
        score += 9
    elif roe >= 10:
        score += 4
    else:
        score -= 8

    if margin >= 20:
        score += 8
    elif margin >= 10:
        score += 6
    elif margin >= 7:
        score += 3
    elif margin > 0:
        score -= 6

    if sales_cagr >= 10:
        score += 8
    elif sales_cagr >= 3:
        score += 5
    elif sales_cagr < -10:
        score -= 8

    if eps_growth >= 10:
        score += 5
    elif eps_growth < -10:
        score -= 5

    if 0 < per <= 15:
        score += 5
    elif per > 25:
        score -= 6

    if debt_ratio > 0 and debt_ratio <= 100:
        score += 4
    elif debt_ratio >= 250:
        score -= 8

    if growth_signal_score >= 2:
        score += 6
    elif growth_signal_score >= 1:
        score += 3

    if quality_penalty >= 45:
        score -= 18
    elif quality_penalty >= 30:
        score -= 10
    elif quality_penalty <= 10:
        score += 4

    return max(0, min(100, round(score)))


def classify_growth_consistency(score):
    number = safe_number(score)
    if number >= 75:
        return "장기보유 후보"
    if number >= 60:
        return "지속성 양호"
    if number >= 45:
        return "지속성 점검"
    return "일회성 주의"


def build_growth_consistency_reasons(item):
    score = calculate_growth_consistency_score(item)
    label = classify_growth_consistency(score)
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    roe = safe_number(item.get("roe") or item.get("ROE"))
    margin = safe_number(item.get("ebitMargin") or item.get("opMargin"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    tags = detect_growth_signal_tags(item)
    warnings = build_quality_warnings(item)

    reasons = [f"{label} · {score}점"]

    if sales_growth >= 10 and op_growth >= 15:
        reasons.append("매출과 영업이익이 함께 증가")
    elif op_growth >= 15:
        reasons.append("영업이익은 증가, 매출 지속성 확인")
    elif op_growth < 0:
        reasons.append("영업이익 감소로 성장 가설 점검")

    if roe >= 15:
        reasons.append(f"ROE {roe:.1f}%로 고성장 기준 통과")
    elif roe > 0:
        reasons.append(f"ROE {roe:.1f}%로 고성장 기준 미달")

    if margin >= 10:
        reasons.append(f"영업이익률 {margin:.1f}%로 수익성 방어")
    elif margin > 0:
        reasons.append(f"영업이익률 {margin:.1f}%로 수익성 추가 확인")

    if sales_cagr >= 3:
        reasons.append(f"3년 매출 CAGR {sales_cagr:.1f}%")
    elif sales_cagr < -10:
        reasons.append("3년 매출 흐름 약화")

    if tags:
        reasons.append("성장 힌트 " + "/".join(tags[:2]))

    for warning in warnings[:1]:
        if warning and warning not in reasons:
            reasons.append(warning)

    return reasons[:5]


def build_long_term_hold_view(item):
    score = calculate_growth_consistency_score(item)
    label = classify_growth_consistency(score)
    if score >= 75:
        return f"{label}: 성장이 유지되면 3년 이상 보유 가능"
    if score >= 60:
        return f"{label}: 분기 실적 유지 여부를 보며 보유"
    if score >= 45:
        return f"{label}: 이익 증가의 반복성 확인 필요"
    return f"{label}: 일회성 성장 가능성 우선 점검"

def build_growth_story(item):
    tailwind = infer_industry_tailwind(item)
    catalysts = build_core_catalyst(item)
    per = safe_number(item.get("per") or item.get("PER"))
    roe = safe_number(item.get("roe") or item.get("ROE"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    consistency_score = calculate_growth_consistency_score(item)
    consistency_label = classify_growth_consistency(consistency_score)

    headline_parts = [tailwind]
    if consistency_score >= 75:
        headline_parts.append("장기보유 후보")
    elif op_growth >= 15 and 0 < per <= 15:
        headline_parts.append("실적 성장 대비 낮은 PER")
    elif roe >= 15 and 0 < per <= 15:
        headline_parts.append("고ROE 대비 낮은 PER")

    headline = " · ".join(headline_parts[:2])
    thesis = " / ".join(catalysts)

    return {
        "headline": headline,
        "tailwind": tailwind,
        "catalysts": catalysts,
        "thesis": thesis,
        "consistencyScore": consistency_score,
        "consistencyLabel": consistency_label,
    }


def attach_wababa_value_reasons(item, position=None):
    growth_story = build_growth_story(item)
    return {
        **item,
        "growthSignalTags": detect_growth_signal_tags(item),
        "growthSignalScore": calculate_growth_signal_score(item),
        "growthStory": growth_story.get("headline"),
        "industryTailwind": growth_story.get("tailwind"),
        "coreCatalyst": growth_story.get("catalysts"),
        "growthThesis": growth_story.get("thesis"),
        "growthConsistencyScore": calculate_growth_consistency_score(item),
        "growthConsistencyLabel": classify_growth_consistency(calculate_growth_consistency_score(item)),
        "growthConsistencyReasons": build_growth_consistency_reasons(item),
        "longTermHoldView": build_long_term_hold_view(item),
        "valueBuyPassed": is_wababa_value_buy_candidate(item),
        "buyReason": build_buy_reason(item),
        "holdReason": build_hold_reason(item, position),
        "sellReason": build_sell_reason(item, position),
        "fundRuleSummary": "Buy: ROE 15%+ · 영업이익 성장 15%+ · PER 15 이하 · 성장 힌트 / Hold: 성장 유지 / Bye: 성장 가설 붕괴 또는 더 좋은 기회",
    }


def estimate_dividend_income(portfolio, today_map, base_date):
    """배당수익 추정치. 실제 배당 입금 파일이 생기기 전까지 보유기간 안분 추정으로 성과에 포함한다."""
    positions = normalize_portfolio_positions(portfolio)
    base = parse_date_value(base_date) or datetime.now().date()
    total = 0
    details = []

    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {}
        current_price = get_current_price(item)
        quantity = safe_number(position.get("quantity"))
        div_yield = safe_number(item.get("dividendYield") or item.get("divYield"))
        buy_date = parse_date_value(position.get("buyDate")) or base
        holding_days = max(0, (base - buy_date).days)
        evaluation_amount = current_price * quantity
        estimated = evaluation_amount * div_yield / 100 * holding_days / 365 if evaluation_amount > 0 and div_yield > 0 else 0

        if estimated > 0:
            total += estimated
            details.append({
                "code": code,
                "name": name,
                "dividendYield": round(div_yield, 2),
                "holdingDays": holding_days,
                "estimatedDividendIncome": round(estimated, 0),
            })

    return {"estimatedDividendIncome": round(total, 0), "items": details[:20]}


def build_wababa_strategy_insight(portfolio, today_map, base_date):
    positions = normalize_portfolio_positions(portfolio)
    held = []
    sell_watch = []

    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        enriched = attach_wababa_value_reasons(item, position)
        hold_reasons = enriched.get("holdReason") if isinstance(enriched.get("holdReason"), list) else []
        sell_reasons = enriched.get("sellReason") if isinstance(enriched.get("sellReason"), list) else []
        sell_needed = any("붕괴" in reason or "감소" in reason or "이탈" in reason for reason in sell_reasons)
        row = {
            "code": code,
            "name": name or get_name(item),
            "holdReason": hold_reasons,
            "sellReason": sell_reasons,
            "profitRate": calculate_profit_rate(item, position),
        }
        if sell_needed:
            sell_watch.append(row)
        else:
            held.append(row)

    summary = "성장 가설 유지 종목은 장기 보유, 성장 붕괴 종목만 매도 점검"
    if not positions:
        summary = "보유 종목 없음. 기준 충족 종목이 나올 때까지 현금 대기 가능"

    return {
        "strategyName": "와바바 가치투자 펀드",
        "summary": summary,
        "rules": [
            "매수: ROE 15% 이상 + 영업이익 성장 + PER 15 이하 중심",
            "성장 힌트: 수출, 수주, 신사업, 업황 턴어라운드 확인",
            "보유: 성장이 유지되면 3년 이상 또는 무기한 보유 가능",
            "매도: 성장 둔화, 영업이익 감소, 고성장 가설 붕괴",
            "원칙: 단기 변동으로 매도하지 않고 배당수익도 성과에 포함",
            "기준 미달이면 하루 0건 매수 가능",
        ],
        "holdChecks": held[:10],
        "sellChecks": sell_watch[:10],
    }

def build_decision_engine(item):
    breakdown = calculate_decision_score_breakdown(item)
    confidence = calculate_decision_confidence(breakdown, item)
    quality_penalty = calculate_quality_penalty(item)
    entry = build_entry_view(item)

    growth_ok = breakdown.get("growth", 0) >= 70
    profitability_ok = breakdown.get("profitability", 0) >= 65
    valuation_ok = breakdown.get("valuation", 0) >= 65
    quality_ok = breakdown.get("quality", 0) >= 65 and quality_penalty < 35
    value_buy_ok = is_wababa_value_buy_candidate(item)

    if value_buy_ok and growth_ok and profitability_ok and valuation_ok and quality_ok and confidence >= 72:
        action = "BUY_NOW"
        action_label = "가치 매수"
    elif entry in ["매수 가능", "분할 매수"] and confidence >= 62 and valuation_ok:
        action = "WATCH"
        action_label = "기준 확인 대기"
    elif confidence >= 55:
        action = "HOLD"
        action_label = "보조 후보 유지"
    else:
        action = "SKIP"
        action_label = "이번 차수 제외"

    blocker = build_decision_blocker(item)
    trigger = build_buy_trigger(item, breakdown)

    return {
        "scoreBreakdown": breakdown,
        "buyTrigger": trigger,
        "blocker": blocker,
        "action": action,
        "actionLabel": action_label,
        "confidence": confidence,
        "summary": f"{action_label} · 신뢰도 {confidence}%",
    }


def _strip_trailing_confirm(text):
    """blocker 텍스트가 '확인'으로 끝나는 경우 후행 어구('확인 후 순위 비교', '확인 필요')와의
    중복을 막기 위해 꼬리의 '확인'과 인접 구분자(·, ., 공백, 콤마)를 제거.

    '확인'이 전혀 없으면 원문 그대로 반환. 모든 텍스트가 '확인'으로만 이루어진 경우 원문 보존.
    """
    if not isinstance(text, str):
        return text
    original = text
    stripped = text.rstrip()
    while stripped.endswith("확인"):
        stripped = stripped[:-2].rstrip(" ·,.")
    cleaned = stripped.rstrip()
    return cleaned if cleaned else original


def build_decision_engine_sentence(item):
    engine = build_decision_engine(item)
    blocker = engine.get("blocker") or ""
    trigger = engine.get("buyTrigger") or ""
    label = engine.get("actionLabel") or "판단 보류"
    confidence = engine.get("confidence") or 0
    action = engine.get("action") or ""

    blocker_clean = _strip_trailing_confirm(blocker)

    industry_name = item.get("industryName") if isinstance(item.get("industryName"), str) else ""
    ig = classify_industry_group(industry_name)

    # ── BUY_NOW ──────────────────────────────────────────────────────────────
    if action == "BUY_NOW":
        if "PER" in trigger or "밸류" in trigger:
            body = f"신뢰도 {confidence}%. 가격 매력과 성장 지속성이 동시에 확인됨."
        elif "동반 증가" in trigger or "성장" in trigger:
            body = f"신뢰도 {confidence}%. 매출·이익 동반 성장이 지금 진입 근거."
        else:
            body = f"신뢰도 {confidence}%. {trigger}가 핵심 근거."
        return f"{label}. {body}"

    # ── WATCH ─────────────────────────────────────────────────────────────────
    if action == "WATCH":
        if blocker == "특별한 차단 요인은 작음":
            if ig == "project":
                detail = "수주 흐름 확인 시 진입 타이밍 포착 가능"
            elif ig == "tech":
                detail = "출하 지표 개선 확인 시 비중 확대 검토"
            elif ig == "bio":
                detail = "임상 결과 공개 시 모멘텀 확인 필요"
            elif ig == "finance":
                detail = "분기 실적과 배당 안정성 확인 후 진입 검토"
            elif ig == "defensive":
                detail = "가격 조정 시 방어적 진입 검토 가능"
            else:
                detail = "단기 트리거 확인 후 진입 타이밍 검토"
            return f"{label}. {detail}."
        return f"{label}. {trigger}는 긍정적이나 {blocker_clean} 점검 필요."

    # ── HOLD ──────────────────────────────────────────────────────────────────
    if action == "HOLD":
        if blocker == "특별한 차단 요인은 작음":
            if ig == "project":
                detail = "수주 잔고와 원가율 점검 후 우선순위 비교"
            elif ig == "tech":
                detail = "출하 회복과 가동률 확인 후 비교"
            elif ig == "bio":
                detail = "파이프라인 일정과 매출 전환 여부 확인"
            elif ig == "finance":
                detail = "자본 효율과 충당금 부담 점검 후 비교"
            elif ig == "defensive":
                detail = "배당 방어력 확인 후 성장 촉매 점검"
            else:
                detail = "실적 지속성과 밸류 부담 함께 점검"
            return f"{label}. {detail}."
        if "PBR 부담" in blocker:
            if ig == "tech":
                detail = "밸류 프리미엄은 실적 모멘텀으로 정당화 여부 확인"
            elif ig == "bio":
                detail = "파이프라인 기대가 현 프리미엄을 정당화하는지 점검"
            else:
                detail = "밸류 프리미엄 정당화 여부 확인 후 비중 조정"
            return f"{label}. {detail}."
        if "매출 감소" in blocker:
            if ig == "project":
                detail = "수주 감소세와 실적 연결 여부 점검 선행"
            else:
                detail = "매출 감소 여부 점검 후 재진입 검토"
            return f"{label}. {detail}."
        if "수익성" in blocker or "마진" in blocker:
            if ig == "project":
                detail = "원가율 안정화와 이익 반등 여부 확인"
            else:
                detail = "마진 유지 여부 확인 후 보유 비중 판단"
            return f"{label}. {detail}."
        if "이익 급증" in blocker or "일회성" in blocker:
            if ig == "bio":
                detail = "임상 기술료 등 일회성 이익 반복성 점검"
            else:
                detail = "이익 급증의 반복성 확인 후 순위 비교"
            return f"{label}. {detail}."
        if "부채" in blocker or "레버리지" in blocker:
            if ig == "finance":
                detail = "부채비율과 자본 건전성 재확인 필요"
            else:
                detail = "재무 레버리지 점검 후 보유 비중 조정"
            return f"{label}. {detail}."
        return f"{label}. 신뢰도 {confidence}%. {blocker_clean} 점검 후 우선순위 비교."

    # ── SKIP (기본) ───────────────────────────────────────────────────────────
    if "부채" in blocker:
        return f"{label}. 재무 부담 해소 확인 전 매수 강도 낮춤."
    if "적자" in blocker or "이익 감소" in blocker:
        return f"{label}. 수익성 회복 확인 전까지 관망 유지."
    return f"{label}. {blocker_clean}으로 매수 강도 낮춤."



def build_growth_phrase(item):
    return build_investment_report(item).get("businessImpact", "")


def build_six_month_thesis(item):
    return build_investment_report(item).get("decision", "")


def build_agent_report_lines(item):
    report = build_investment_report(item)
    return [
        report.get("fact", ""),
        report.get("businessImpact", ""),
        report.get("valuation", ""),
        report.get("decision", ""),
    ]


def build_agent_natural_report(item):
    return " ".join([line for line in build_agent_report_lines(item) if line])


def build_valuation_short(item):
    """단문 밸류에이션 — build_company_summary 내 문장 결합용"""
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    if 0 < per <= 8 and roe >= 15:
        return f"ROE {roe:.1f}%인데 PER {per:.1f}배로 이익 체력 대비 부담 낮음."
    if 0 < per <= 10 and roe >= 12:
        return f"PER {per:.1f}배 수준으로 이익 대비 밸류 부담 낮음."
    if 0 < per <= 12 and roe >= 12:
        return f"ROE {roe:.1f}% 대비 PER {per:.1f}배로 저평가 구간."
    if 0 < pbr <= 1 and roe >= 10:
        return f"PBR {pbr:.1f}배·ROE {roe:.1f}%로 자산·수익성 조합 양호."
    if 0 < per <= 15:
        return f"PER {per:.1f}배로 가격 부담 낮은 편."
    return ""


def build_company_summary(item):
    news = item.get("news") or {}
    val_phrase = build_valuation_short(item)

    # 1순위: 종목명 매칭 뉴스 기사 제목 — 가장 구체적
    evidence = pick_relevant_evidence(item)
    if evidence:
        if val_phrase:
            return short_text(f"{evidence} {val_phrase}", 160)
        return short_text(evidence, 120)

    # 2순위: articleSummaries 첫 번째 타이틀 + 밸류에이션 결합
    # news_map 매칭 종목의 큐레이션 기사 — 해당 종목 관련 흐름 반영
    article_summaries = news.get("articleSummaries") if isinstance(news, dict) else []
    if isinstance(article_summaries, list):
        for article in article_summaries:
            if isinstance(article, dict):
                title = str(article.get("title") or article.get("summary") or "").strip()
                title = strip_news_source(title)
                if title and len(title) > 10:
                    if val_phrase:
                        return short_text(f"{title} {val_phrase}", 160)
                    return short_text(title, 120)

    # 3순위: hypothesis — 뉴스 기반 투자 가설
    hypothesis = str(item.get("hypothesis") or "").strip()
    if not hypothesis and isinstance(news, dict):
        hypothesis = str(news.get("hypothesis") or "").strip()
    if hypothesis and len(hypothesis) > 15:
        # hypothesis 뒤에 성장 수치 보강
        sales_cagr = safe_number(item.get("salesCagr3Y"))
        eps_growth = safe_number(item.get("EPSGrowth3Y"))
        suffix = ""
        if sales_cagr >= 10:
            suffix = f" 3년 매출 CAGR {sales_cagr:.1f}%."
        elif eps_growth >= 10:
            suffix = f" EPS 3년 성장률 {eps_growth:.1f}%."
        return short_text(f"{hypothesis}{suffix}", 140)

    # 4순위: 성장 수치 + 밸류 포인트 조합
    sales_growth = safe_number(item.get("salesGrowth"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    margin = safe_number(item.get("ebitMargin"))
    sales_cagr = safe_number(item.get("salesCagr3Y"))
    eps_growth = safe_number(item.get("EPSGrowth3Y"))
    industry = str(item.get("industryName") or "").strip()
    tailwind = infer_industry_tailwind(item)

    # 업종명이 있으면 앞에 붙여 차별화
    industry_prefix = f"{industry} 업종, " if industry and len(industry) <= 12 else ""

    if sales_growth >= 10 and growth >= 20:
        growth_part = f"{industry_prefix}매출 {sales_growth:.1f}%·영업이익 {growth:.1f}% 동반 성장"
    elif growth >= 30:
        growth_part = f"{industry_prefix}영업이익 {growth:.1f}% 증가"
    elif growth >= 10:
        growth_part = f"{industry_prefix}영업이익 {growth:.1f}% 증가"
    elif sales_cagr >= 10:
        growth_part = f"{industry_prefix}3년 매출 CAGR {sales_cagr:.1f}%로 성장 흐름 지속"
    elif eps_growth >= 10:
        growth_part = f"{industry_prefix}3년 EPS 성장률 {eps_growth:.1f}%로 이익 체력 유지"
    elif margin >= 15:
        growth_part = f"{industry_prefix}영업이익률 {margin:.1f}%로 수익성 우수"
    elif sales_growth >= 5:
        growth_part = f"{industry_prefix}매출 {sales_growth:.1f}% 증가"
    else:
        growth_part = ""

    # tailwind가 generic fallback이면 CAGR/EPS로 대체
    generic_tailwind = "재평가 가능성" in tailwind or tailwind == "재무·밸류 기준 통과"

    if growth_part and val_phrase:
        return short_text(f"{growth_part}. {val_phrase}", 160)
    if growth_part:
        if not generic_tailwind:
            return short_text(f"{growth_part}. {tailwind}.", 120)
        if sales_cagr >= 8:
            return short_text(f"{growth_part}. 3년 매출 CAGR {sales_cagr:.1f}%로 중기 성장 흐름 유지.", 160)
        if eps_growth >= 10:
            return short_text(f"{growth_part}. 3년 EPS 성장률 {eps_growth:.1f}%로 이익 체력 확인.", 160)
        return short_text(f"{growth_part}.", 120)
    if val_phrase:
        if not generic_tailwind:
            return short_text(f"{tailwind}. {val_phrase}", 120)
        return short_text(val_phrase, 120)
    if not generic_tailwind:
        return short_text(tailwind, 90)

    # 최후 fallback: CAGR/EPS가 있으면 그걸로, 없으면 간단한 재무 요약
    if sales_cagr >= 8:
        return f"3년 매출 CAGR {sales_cagr:.1f}%로 중기 성장 흐름 유지."
    if eps_growth >= 10:
        return f"3년 EPS 성장률 {eps_growth:.1f}%로 이익 개선 흐름 유지."
    if margin >= 15:
        return f"영업이익률 {margin:.1f}%로 수익성 기반 안정."
    return "재무·밸류 기준 통과"


def build_investment_points(item):
    lines = build_agent_report_lines(item)

    unique_lines = []
    for line in lines:
        clean = short_text(line, 120).strip()
        if clean and clean not in unique_lines:
            unique_lines.append(clean)

    return unique_lines[:4]


def build_risk_summary(item):
    risk = str(item.get("riskSummary") or "").strip()

    if risk:
        return risk

    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    dividend = safe_number(item.get("dividendYield"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    news_score = safe_number(item.get("newsScore"))

    if news_score < 0:
        return "부정 뉴스 흐름이 있어 매수 전 추가 확인 필요"

    if growth >= 80:
        return "이익 증가율이 높아도 일회성 개선 여부 확인 필요"

    if dividend < 1:
        return "배당 방어력은 낮아 단기 변동성 관리 필요"

    if margin < 8:
        return "수익성은 아직 개선 확인이 더 필요"

    if per >= 20:
        return "PER 부담이 있어 추격매수는 주의"

    if pbr >= 2:
        return "PBR 기준으로는 저평가 매력이 약할 수 있음"

    return "단기 주가 변동성은 분할 접근으로 관리 필요"


def build_entry_view(item):
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    roe = safe_number(item.get("roe"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    quality_level = classify_quality_level(item)
    debt_ratio = safe_number(item.get("debtRatio"))

    undervalued = (0 < per <= 12) or (0 < pbr <= 1)
    profitable = roe >= 10 and margin >= 8
    improving = growth >= 10 or sales_growth >= 10
    high_risk = quality_level == "주의" or debt_ratio >= 250 or per >= 25 or pbr >= 3

    if high_risk:
        return "매수 보류"

    if undervalued and profitable and improving:
        return "매수 가능"

    if undervalued and profitable:
        return "분할 매수"

    if improving and profitable:
        return "분할 매수"

    return "매수 보류"


def build_wababa_reason(item):
    return build_agent_natural_report(item)


def build_legacy_buy_reason(item):
    financial = safe_number(item.get("financialScore"))
    news = safe_number(item.get("newsScore"))
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))

    parts = []

    if financial >= 80:
        parts.append("재무 점수 상위")
    elif financial >= 65:
        parts.append("재무 기준 통과")

    if 0 < per <= 10:
        parts.append("저PER")
    elif per <= 25:
        parts.append("PER 부담 낮음")

    if 0 < pbr <= 1:
        parts.append("저PBR")
    elif pbr <= 2:
        parts.append("PBR 안정")

    if roe >= 15:
        parts.append("고ROE")
    elif roe >= 8:
        parts.append("ROE 안정")

    if growth > 0:
        parts.append("이익 증가 흐름")

    if news == 0:
        parts.append("뉴스 공백")
    elif news > 0:
        parts.append("긍정 뉴스")

    if not parts:
        parts.append("기본 조건 충족")

    return ", ".join(parts[:4])


def build_legacy_hold_reason(item):
    score = safe_number(item.get("score"))
    financial = safe_number(item.get("financialScore"))

    if score >= 80 and financial >= 65:
        return "기존 가설 유지, 점수 상위권, 보유 관찰"

    if financial >= 65:
        return "재무 기준 유지, 추가 악재 없음, 보유 관찰"

    return "기존 후보 유지, 변화 관찰"


def build_legacy_sell_reason(item):
    return "점수 하락 가능성, 뉴스 악화 가능성, 제외 검토"


def build_wababa_rank_reason(item, rank):
    category = str(item.get("wababaType") or "")
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    news = safe_number(item.get("newsScore"))

    parts = []

    if category == "안정형":
        if roe >= 12:
            parts.append("ROE 안정")
        if 0 < per <= 15:
            parts.append("PER 부담 낮음")
        if margin >= 8:
            parts.append("영업이익률 안정")
        if pbr <= 2:
            parts.append("PBR 안정")
        return f"안정형, {', '.join(parts[:3]) if parts else '재무 안정성 우선'}"

    if category == "성장형":
        if growth >= 20:
            parts.append("이익 성장 강함")
        if roe >= 8:
            parts.append("ROE 기준 통과")
        if per <= 30:
            parts.append("성장 대비 PER 허용")
        return f"성장형, {', '.join(parts[:3]) if parts else '성장성 우선'}"

    if category == "기회형":
        if news > 0:
            parts.append("긍정 뉴스")
        if per <= 20:
            parts.append("PER 부담 낮음")
        if pbr <= 2:
            parts.append("PBR 안정")
        return f"기회형, {', '.join(parts[:3]) if parts else '뉴스/가격 기회'}"

    base = []
    if roe >= 15:
        base.append("고ROE")
    elif roe >= 8:
        base.append("ROE 안정")

    if 0 < per <= 10:
        base.append("저PER")
    elif per <= 25:
        base.append("PER 부담 낮음")

    if margin >= 10:
        base.append("영업이익률 양호")

    return f"{', '.join(base[:3]) if base else '재무/성장 균형'}, 비교 종목"


def is_stable_candidate(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    return (
        roe >= 12
        and 0 < per <= 15
        and margin >= 8
        and growth >= 0
        and market_cap >= 1000
    )


def is_growth_candidate(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    return (
        growth >= 20
        and roe >= 8
        and 0 < per <= 30
        and market_cap >= 500
    )


def is_opportunity_candidate(item):
    news = safe_number(item.get("newsScore"))
    per = safe_number(item.get("per"))
    financial = safe_number(item.get("financialScore"))
    market_cap = safe_number(item.get("marketCapBillionKrw"))

    return (
        news > 0
        and 0 < per <= 20
        and financial >= 60
        and market_cap >= 500
    )


def pick_best_unique(items, score_key, used_codes):
    candidates = []

    for item in items:
        code = get_code(item)

        if not code or code in used_codes:
            continue

        candidates.append(item)

    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda item: safe_number(item.get(score_key)),
        reverse=True,
    )[0]


def is_sell_candidate(previous_item, today_map, config):
    code = get_code(previous_item)

    if not code:
        return False

    today_item = today_map.get(code)

    if today_item is None:
        return True

    previous_score = safe_number(previous_item.get("score"))
    today_score = safe_number(today_item.get("score"))

    previous_news_score = safe_number(previous_item.get("newsScore"))
    today_news_score = safe_number(today_item.get("newsScore"))

    max_sell_score = safe_number(config.get("maxSellScore"))
    min_score_drop = safe_number(config.get("minScoreDropForSell"))
    bad_news_score = safe_number(config.get("badNewsScore"))
    min_news_drop = safe_number(config.get("minNewsScoreDropForSell"))

    if today_score <= max_sell_score:
        return True

    if previous_score - today_score >= min_score_drop:
        return True

    if today_news_score <= bad_news_score:
        return True

    if previous_news_score - today_news_score >= min_news_drop:
        return True

    return False


def attach_decision(item, decision, reason):
    return {
        **item,
        "decision": decision,
        "reason": reason,
    }


def build_outlook_6m(item):
    news = get_news_dict(item)
    direct = str(news.get("outlook6M") or item.get("outlook6M") or "").strip()
    if direct:
        return direct

    return build_six_month_thesis(item)


def build_evidence_summary(item):
    evidence = pick_relevant_evidence(item)
    if evidence:
        return evidence

    return build_agent_natural_report(item)


def build_final_best_reason(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    margin = safe_number(item.get("ebitMargin"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    quality_level = classify_quality_level(item)

    parts = []

    if roe >= 15:
        parts.append("ROE")
    if margin >= 10:
        parts.append("영업이익률")
    if growth >= 20:
        parts.append("이익 성장")
    if sales_growth >= 10:
        parts.append("매출 성장")
    if 0 < per <= 12 or 0 < pbr <= 1:
        parts.append("저평가")

    prefix = " / ".join(parts[:4]) if parts else "재무 안정성과 가격 매력"

    if quality_level == "양호":
        return f"{prefix} 균형이 가장 좋고, 주요 위험 점검도 양호"

    return f"{prefix} 균형이 좋지만 품질 점검은 {quality_level} 단계"



def build_today_pick_headline(item):
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    roe = safe_number(item.get("roe"))
    margin = safe_number(item.get("ebitMargin"))

    if (growth >= 30 or sales_growth >= 10) and 0 < per <= 12:
        return "실적 상승 + 낮은 PER → 오늘 우선 검토"

    if (growth >= 30 or sales_growth >= 10) and 0 < pbr <= 1:
        return "성장 확인 + 낮은 PBR → 재평가 관찰"

    if roe >= 15 and margin >= 10 and 0 < per <= 15:
        return "수익성 우수 + 가격 부담 낮음"

    if growth >= 20 or sales_growth >= 10:
        return "실적 개선 시작 → 오늘 집중 확인"

    return "오늘 가장 먼저 볼 종목"


def build_buy_timing_summary(item):
    entry = build_entry_view(item)
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    growth = safe_number(item.get("operatingProfitGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))

    undervalued = (0 < per <= 12) or (0 < pbr <= 1)
    improving = growth >= 20 or sales_growth >= 10

    if entry == "매수 가능" and improving and undervalued:
        return "오늘 우선 검토"

    if entry == "매수 가능":
        return "매수 가능"

    if entry == "분할 매수":
        return "분할 접근"

    return "관찰 우선"


def build_one_line_recommendation(item):
    headline = build_today_pick_headline(item)
    timing = build_buy_timing_summary(item)
    return f"{headline} · {timing}"

def attach_wababa_pick(item, pick_type, score_key):
    investment_report = build_investment_report(item)
    decision_engine = build_decision_engine(item)
    if decision_engine:
        investment_report["decision"] = build_decision_engine_sentence(item)
    company_summary = build_company_summary(item)
    investment_points = build_investment_points(item)
    risk_summary = build_risk_summary(item)
    entry_view = build_entry_view(item)
    evidence_summary = build_evidence_summary(item)
    outlook_6m = build_outlook_6m(item)
    final_best_reason = build_final_best_reason(item)
    today_pick_headline = build_today_pick_headline(item)
    buy_timing_summary = build_buy_timing_summary(item)
    one_line_recommendation = build_one_line_recommendation(item)
    agent_report = build_agent_natural_report(item)
    quality_penalty = calculate_quality_penalty(item)
    quality_warnings = build_quality_warnings(item)
    quality_level = classify_quality_level(item)

    position_sizing = build_position_sizing(item, decision_engine)

    enriched_item = attach_wababa_value_reasons({
            **item,
            "wababaType": pick_type,
            "wababaScore": safe_number(item.get(score_key)),
            "companySummary": company_summary,
            "investmentThesis": company_summary,
            "industryMomentum": investment_points[0] if investment_points else "",
            "growthCatalyst": investment_points[1] if len(investment_points) > 1 else "",
            "investmentPoints": investment_points,
            "investmentReport": investment_report,
            "decisionEngine": decision_engine,
            "positionSizing": position_sizing,
            "riskSummary": risk_summary,
            "entryView": entry_view,
            "evidenceSummary": evidence_summary,
            "outlook6M": outlook_6m,
            "finalBestReason": final_best_reason,
            "todayPickHeadline": today_pick_headline,
            "buyTimingSummary": buy_timing_summary,
            "oneLineRecommendation": one_line_recommendation,
            "agentReport": agent_report,
            "naturalReport": agent_report,
            "qualityPenalty": quality_penalty,
            "qualityLevel": quality_level,
            "qualityWarnings": quality_warnings,
        })

    return attach_decision(
        enriched_item,
        "WABABA_PICK",
        build_wababa_reason(item),
    )




def infer_explore_type(item):
    stable = calculate_stable_score(item)
    growth = calculate_growth_score(item)
    opportunity = calculate_opportunity_score(item)

    scores = [
        ("안정형", stable),
        ("성장형", growth),
        ("기회형", opportunity),
    ]

    return sorted(scores, key=lambda pair: pair[1], reverse=True)[0][0]


def build_explore_group(
    all_candidates,
    group_key,
    group_label,
    score_key,
    main_pick_codes,
    preferred_filter=None,
    max_count=10,
):
    result = []
    used_codes = set()

    preferred = []

    if preferred_filter:
        preferred = [item for item in all_candidates if preferred_filter(item)]

    preferred_codes = {get_code(item) for item in preferred if get_code(item)}
    fallback = [item for item in all_candidates if get_code(item) not in preferred_codes]

    ordered_candidates = sorted(
        preferred,
        key=lambda item: (
            safe_number(item.get(score_key)),
            safe_number(item.get("finalBestScore")),
            safe_number(item.get("generalWababaScore")),
            safe_number(item.get("score")),
        ),
        reverse=True,
    ) + sorted(
        fallback,
        key=lambda item: (
            safe_number(item.get(score_key)),
            safe_number(item.get("finalBestScore")),
            safe_number(item.get("generalWababaScore")),
            safe_number(item.get("score")),
        ),
        reverse=True,
    )

    for item in ordered_candidates:
        if len(result) >= max_count:
            break

        code = get_code(item)

        if not code or code in used_codes:
            continue

        used_codes.add(code)
        attached = attach_wababa_pick(item, group_label, score_key)

        result.append(
            {
                **attached,
                "rank": len(result) + 1,
                "exploreGroup": group_key,
                "exploreType": group_label,
                "exploreScore": safe_number(item.get(score_key)),
                "isMainPick": code in main_pick_codes,
                "rankReason": build_wababa_rank_reason(
                    {**item, "wababaType": group_label},
                    len(result) + 1,
                ),
            }
        )

    return result


def build_explore_groups(all_candidates, main_pick_codes):
    groups = {
        "total": build_explore_group(
            all_candidates,
            "total",
            "종합",
            "finalBestScore",
            main_pick_codes,
            None,
            10,
        ),
        "stable": build_explore_group(
            all_candidates,
            "stable",
            "안정형",
            "stableScore",
            main_pick_codes,
            is_stable_candidate,
            10,
        ),
        "growth": build_explore_group(
            all_candidates,
            "growth",
            "성장형",
            "growthScore",
            main_pick_codes,
            lambda item: is_growth_candidate(item) or is_relaxed_growth_candidate(item),
            10,
        ),
        "opportunity": build_explore_group(
            all_candidates,
            "opportunity",
            "기회형",
            "opportunityScore",
            main_pick_codes,
            is_opportunity_candidate,
            10,
        ),
    }

    flat = []

    for key in ["total", "stable", "growth", "opportunity"]:
        flat.extend(groups[key])

    return groups, flat



def load_reviewed_candidate_codes():
    loaded = read_json(REVIEWED_CANDIDATES_PATH)

    if not isinstance(loaded, dict):
        return set()

    values = loaded.get("reviewedCodes")

    if not isinstance(values, list):
        return set()

    return {
        str(value).strip()
        for value in values
        if str(value or "").strip()
    }


def filter_unreviewed_candidates(items, reviewed_codes):
    if not reviewed_codes:
        return items

    filtered = [
        item for item in items
        if get_code(item) not in reviewed_codes
    ]

    if filtered:
        return filtered

    return items



def pick_auto_trade_candidate(final_best_pick, wababa_picks, explore_groups, portfolio, policy):
    """Select the actual fund buy candidate.

    The screen's finalBestPick can be a good watch candidate but fail the strict
    Wababa value-fund buy rules. For actual fund buying, scan the visible BEST
    picks and explore groups and choose the first candidate that passes:
    - Wababa value buy rule
    - decisionEngine.action == BUY_NOW
    - confidence >= minBuyConfidence
    - not already held
    """
    held_codes = set()
    for position in normalize_portfolio_positions(portfolio if isinstance(portfolio, dict) else {}):
        code = str(position.get("code") or "").strip()
        if code:
            held_codes.add(code)

    min_confidence = safe_int((policy or {}).get("minBuyConfidence"), 70) if isinstance(policy, dict) else 70

    candidates = []

    def add_candidate(source, source_label):
        if isinstance(source, dict):
            candidates.append({**source, "autoTradeSource": source_label})
        elif isinstance(source, list):
            for item in source:
                if isinstance(item, dict):
                    candidates.append({**item, "autoTradeSource": source_label})

    add_candidate(final_best_pick, "todayOnePick")
    add_candidate(wababa_picks, "wababaPicks")

    if isinstance(explore_groups, dict):
        for key in ["total", "stable", "growth", "opportunity"]:
            add_candidate(explore_groups.get(key), f"exploreGroups.{key}")

    seen = set()
    qualified = []

    for item in candidates:
        code = get_code(item)
        if not code or code in seen or code in held_codes:
            continue
        seen.add(code)

        engine = item.get("decisionEngine") if isinstance(item.get("decisionEngine"), dict) else build_decision_engine(item)
        action = str(engine.get("action") or "").strip() if isinstance(engine, dict) else ""
        confidence = safe_int(engine.get("confidence"), 0) if isinstance(engine, dict) else 0
        value_buy_passed = bool(item.get("valueBuyPassed")) or is_wababa_value_buy_candidate(item)

        if not value_buy_passed:
            continue
        if action != "BUY_NOW":
            continue
        if confidence < min_confidence:
            continue

        score = (
            safe_number(item.get("todayPickScore") or item.get("finalBestScore")) * 1.0
            + safe_number(item.get("growthSignalScore")) * 8
            + safe_number(item.get("growthConsistencyScore") or calculate_growth_consistency_score(item)) * 1.4
            + confidence * 1.2
            + safe_number(item.get("roe")) * 0.8
            - max(0, safe_number(item.get("per")) - 10) * 1.5
        )
        qualified.append({
            **item,
            "decisionEngine": engine,
            "autoTradeScore": round(score, 4),
            "autoTradeReason": "와바바 가치매수 기준 통과",
        })

    if not qualified:
        return None

    return sorted(qualified, key=lambda item: safe_number(item.get("autoTradeScore")), reverse=True)[0]






def calculate_ai_fund_score(item):
    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    pbr = safe_number(item.get("pbr"))
    sales_growth = safe_number(item.get("salesGrowth"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    margin = safe_number(item.get("ebitMargin") or item.get("opMargin"))
    news = safe_number(item.get("newsScore") or item.get("newsMomentumScore"))
    financial = safe_number(item.get("financialScore"))
    quality_penalty = calculate_quality_penalty(item)
    consistency = safe_number(item.get("growthConsistencyScore") or calculate_growth_consistency_score(item))
    growth_signal = safe_number(item.get("growthSignalScore") or calculate_growth_signal_score(item))

    valuation_bonus = 0
    if 0 < per <= 8:
        valuation_bonus += 28
    elif per <= 12:
        valuation_bonus += 20
    elif per <= 18:
        valuation_bonus += 10
    elif per <= 25:
        valuation_bonus += 3

    if 0 < pbr <= 1:
        valuation_bonus += 12
    elif pbr <= 2:
        valuation_bonus += 5

    return round(
        financial * 0.7
        + roe * 1.5
        + margin * 1.0
        + op_growth * 0.55
        + sales_growth * 0.45
        + news * 2.2
        + consistency * 1.15
        + growth_signal * 9
        + valuation_bonus
        - quality_penalty * 1.8,
        4,
    )


def build_ai_fund_reason(item):
    reasons = []
    story = str(item.get("growthStory") or item.get("growthThesis") or "").strip()
    if story:
        reasons.append(story)

    roe = safe_number(item.get("roe"))
    per = safe_number(item.get("per"))
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    consistency = safe_number(item.get("growthConsistencyScore") or calculate_growth_consistency_score(item))
    news = safe_number(item.get("newsScore") or item.get("newsMomentumScore"))

    if roe > 0 and per > 0:
        reasons.append(f"ROE {roe:.1f}% · PER {per:.1f}배")
    if op_growth != 0:
        reasons.append(f"영업이익 증가율 {op_growth:.1f}%")
    if consistency > 0:
        reasons.append(f"성장 지속성 {consistency:.0f}점")
    if news > 0:
        reasons.append(f"뉴스 모멘텀 +{news:.0f}")

    return " / ".join(reasons[:4]) if reasons else "AI 종합점수 기준 매수"


def pick_wababa_ai_trade_candidates(all_candidates, ai_portfolio, policy):
    held_codes = set()
    for position in normalize_portfolio_positions(ai_portfolio if isinstance(ai_portfolio, dict) else {}):
        code = str(position.get("code") or "").strip()
        if code:
            held_codes.add(code)

    max_positions = safe_int((policy or {}).get("maxPositions"), 10) if isinstance(policy, dict) else 10
    daily_limit = safe_int((policy or {}).get("dailyNewBuyLimit"), 2) if isinstance(policy, dict) else 2
    remaining_slots = max(0, max_positions - len(held_codes))
    pick_limit = max(0, min(daily_limit, remaining_slots))
    if pick_limit <= 0:
        return []

    qualified = []
    seen = set()
    for item in all_candidates if isinstance(all_candidates, list) else []:
        if not isinstance(item, dict):
            continue
        code = get_code(item)
        if not code or code in seen or code in held_codes:
            continue
        seen.add(code)

        price = get_current_price(item)
        if price <= 0:
            continue

        quality_level = classify_quality_level(item)
        quality_penalty = calculate_quality_penalty(item)
        if quality_level == "주의" or quality_penalty >= 45:
            continue

        ai_score = calculate_ai_fund_score(item)
        consistency = safe_number(item.get("growthConsistencyScore") or calculate_growth_consistency_score(item))
        per = safe_number(item.get("per"))
        roe = safe_number(item.get("roe"))
        op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
        growth_signal = safe_number(item.get("growthSignalScore") or calculate_growth_signal_score(item))

        if ai_score < 170:
            continue
        if consistency < 50 and growth_signal <= 0:
            continue
        if per <= 0 or per > 30:
            continue
        if roe < 8 and op_growth < 20:
            continue

        qualified.append({
            **item,
            "aiFundScore": ai_score,
            "aiFundReason": build_ai_fund_reason(item),
        })

    return sorted(qualified, key=lambda item: safe_number(item.get("aiFundScore")), reverse=True)[:pick_limit]


def should_ai_sell_position(item, position):
    profit_rate = calculate_profit_rate(item, position)
    quality_level = classify_quality_level(item)
    quality_penalty = calculate_quality_penalty(item)
    op_growth = safe_number(item.get("operatingProfitGrowth") or item.get("opIncomeGrowth"))
    sales_growth = safe_number(item.get("salesGrowth"))
    news_score = safe_number(item.get("newsScore") or item.get("newsMomentumScore"))
    consistency = safe_number(item.get("growthConsistencyScore") or calculate_growth_consistency_score(item))

    if quality_level == "주의" or quality_penalty >= 50:
        return True, "AI 품질 점수 악화"
    if op_growth < 0 and sales_growth < 0:
        return True, "매출·영업이익 동반 감소"
    if news_score <= -30:
        return True, "부정 뉴스 모멘텀 확대"
    if consistency < 35 and op_growth < 5:
        return True, "성장 지속성 약화"
    if profit_rate is not None and profit_rate <= -18 and consistency < 55:
        return True, "손실 확대와 성장 지속성 약화"
    return False, "매도 사유 없음"


def apply_wababa_ai_fund_auto_trading(ai_portfolio, ai_trade_picks, today_map, base_date):
    if not isinstance(ai_portfolio, dict):
        ai_portfolio = load_ai_portfolio()

    policy = get_ai_fund_policy(ai_portfolio)
    positions = normalize_portfolio_positions(ai_portfolio)
    ledger = ai_portfolio.get("tradeLedger")
    if not isinstance(ledger, list):
        ledger = []

    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    fund_start = parse_date_value(policy.get("startDate"))
    current_date = parse_date_value(base_date_text)
    orders = []
    skipped = []

    if not policy.get("autoTradingEnabled"):
        result = {"status": "OFF", "message": "와바바AI펀드 자동 운용 꺼짐", "orders": [], "skipped": []}
        append_ai_auto_trade_log(base_date_text, result["status"], result["message"], [], [], ai_trade_picks)
        return ai_portfolio, result

    if fund_start and current_date and current_date < fund_start:
        result = {"status": "WAIT_START", "message": f"와바바AI펀드 시작일 {policy.get('startDate')} 전입니다.", "orders": [], "skipped": []}
        append_ai_auto_trade_log(base_date_text, result["status"], result["message"], [], [], ai_trade_picks)
        return ai_portfolio, result

    if not is_market_open_day(base_date_text, policy):
        result = {"status": "MARKET_CLOSED", "message": "주식 개장일이 아니므로 AI펀드 매매를 반영하지 않았습니다.", "orders": [], "skipped": []}
        append_ai_auto_trade_log(base_date_text, result["status"], result["message"], [], [], ai_trade_picks)
        return ai_portfolio, result

    if has_ai_auto_trade_log_for_date(base_date_text):
        result = {"status": "ALREADY_EXECUTED", "message": "오늘 와바바AI펀드 자동운용은 이미 실행되어 추가 매매를 차단했습니다.", "orders": [], "skipped": []}
        return ai_portfolio, result

    initial_capital = safe_number(policy.get("initialCapital"))
    cash = get_portfolio_cash(ai_portfolio, positions)
    realized_profit = safe_number(ai_portfolio.get("realizedProfit"))
    existing_trade_ids = get_existing_trade_ids({**ai_portfolio, "tradeLedger": ledger})

    remaining_positions = []
    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        current_price = get_current_price(item)
        quantity = safe_number(position.get("quantity"))
        buy_price = safe_number(position.get("buyPrice"))
        should_sell, sell_reason = should_ai_sell_position(item, position)

        if should_sell and current_price > 0 and quantity > 0:
            trade_id = make_trade_id(base_date_text, "AI_SELL", code)
            if trade_id not in existing_trade_ids:
                sell_amount = current_price * quantity
                buy_amount = buy_price * quantity
                realized = sell_amount - buy_amount
                profit_rate = round((realized / buy_amount) * 100, 2) if buy_amount > 0 else None
                cash += sell_amount
                realized_profit += realized
                order = {
                    "id": trade_id,
                    "date": base_date_text,
                    "action": "SELL",
                    "code": code,
                    "name": name or get_name(item),
                    "price": round(current_price, 0),
                    "quantity": round(quantity, 4),
                    "amount": round(sell_amount, 0),
                    "reason": sell_reason,
                    "profitAmount": round(realized, 0),
                    "profitRate": profit_rate,
                }
                orders.append(order)
                ledger.append(order)
                existing_trade_ids.add(trade_id)
            else:
                remaining_positions.append(position)
                skipped.append({"code": code, "action": "SELL", "reason": "오늘 이미 AI 매도 반영됨"})
        else:
            remaining_positions.append(position)

    positions = remaining_positions
    held_codes = {str(position.get("code") or "").strip() for position in positions if str(position.get("code") or "").strip()}
    max_position_weight = safe_number(policy.get("maxPositionWeight"), 12)
    min_cash_rate = safe_number(policy.get("minCashRate"), 5)
    min_cash_after_buy = initial_capital * min_cash_rate / 100
    max_positions = safe_int(policy.get("maxPositions"), 10)

    for item in ai_trade_picks if isinstance(ai_trade_picks, list) else []:
        if len(positions) >= max_positions:
            skipped.append({"action": "BUY", "reason": "AI펀드 보유 종목 한도 도달"})
            break

        code = get_code(item)
        name = get_name(item)
        current_price = get_current_price(item)
        if not code:
            skipped.append({"action": "BUY", "reason": "AI 후보 코드 없음"})
            continue
        if code in held_codes:
            skipped.append({"code": code, "action": "BUY", "reason": "AI펀드 이미 보유 중"})
            continue
        if current_price <= 0:
            skipped.append({"code": code, "action": "BUY", "reason": "현재가 없음"})
            continue

        ai_score = safe_number(item.get("aiFundScore"))
        target_weight = 4
        if ai_score >= 260:
            target_weight = 10
        elif ai_score >= 230:
            target_weight = 8
        elif ai_score >= 200:
            target_weight = 6
        target_weight = min(target_weight, max_position_weight)
        buy_budget = int(initial_capital * target_weight / 100 / 2)
        buy_budget = min(buy_budget, max(0, cash - min_cash_after_buy))
        trade_id = make_trade_id(base_date_text, "AI_BUY", code)

        if trade_id in existing_trade_ids:
            skipped.append({"code": code, "action": "BUY", "reason": "오늘 이미 AI 매수 반영됨"})
            continue
        if buy_budget <= 0:
            skipped.append({"code": code, "action": "BUY", "reason": "AI 매수 가능 현금 부족"})
            continue

        quantity = int(buy_budget // current_price)
        if quantity <= 0:
            skipped.append({"code": code, "action": "BUY", "reason": "1주 매수 예산 미달"})
            continue

        amount = quantity * current_price
        cash -= amount
        positions = upsert_auto_position(positions, code, name, current_price, quantity, base_date_text)
        held_codes.add(code)
        order = {
            "id": trade_id,
            "date": base_date_text,
            "action": "BUY",
            "code": code,
            "name": name,
            "price": round(current_price, 0),
            "quantity": quantity,
            "amount": round(amount, 0),
            "reason": f"AI 자율매수 · 점수 {ai_score:.1f} · {item.get('aiFundReason')}",
        }
        orders.append(order)
        ledger.append(order)
        existing_trade_ids.add(trade_id)

    ai_portfolio = {
        **ai_portfolio,
        "fundVersion": WABABA_AI_FUND_VERSION,
        "fundName": "와바바AI펀드",
        "initialCapital": initial_capital,
        "cash": round(cash, 0),
        "currency": "KRW",
        "fundStartDate": policy.get("startDate"),
        "autoTradingEnabled": policy.get("autoTradingEnabled"),
        "maxPositions": policy.get("maxPositions"),
        "maxPositionWeight": policy.get("maxPositionWeight"),
        "minCashRate": policy.get("minCashRate"),
        "minBuyConfidence": policy.get("minBuyConfidence"),
        "realizedProfit": round(realized_profit, 0),
        "updatedAt": base_date_text,
        "positions": positions,
        "tradeLedger": ledger,
    }
    write_json(AI_PORTFOLIO_PATH, ai_portfolio)
    write_ai_trade_history_from_portfolio(ai_portfolio)

    status = "TRADED" if orders else "NO_TRADE"
    message = "와바바AI펀드 자동운용 반영" if orders else "AI펀드 조건에 맞는 신규 체결 없음"
    append_ai_auto_trade_log(base_date_text, status, message, orders, skipped, ai_trade_picks)
    return ai_portfolio, {"status": status, "message": message, "orders": orders, "skipped": skipped, "aiTradePicks": ai_trade_picks}


def build_ai_daily_fund_memo(ai_portfolio, ai_trade_result, ai_trade_picks, ai_portfolio_summary, ai_performance_analysis, base_date):
    memo = build_daily_fund_memo(ai_portfolio, ai_trade_result, ai_trade_picks[0] if isinstance(ai_trade_picks, list) and ai_trade_picks else None, ai_portfolio_summary, ai_performance_analysis, base_date)
    if not isinstance(memo, dict):
        return {}
    lines = memo.get("lines") if isinstance(memo.get("lines"), list) else []
    return {
        **memo,
        "title": "와바바AI펀드 운용 메모",
        "lines": [str(line).replace("와바바", "와바바AI") for line in lines],
    }

def build_holding_review(portfolio, today_map, base_date):
    """Build a compact daily review for current holdings.

    Value-fund priority: hold as long as the company keeps earning well;
    sell only when the growth thesis breaks or a clearly better opportunity appears.
    """
    positions = normalize_portfolio_positions(portfolio)
    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    items = []
    summary_lines = []
    sell_watch_count = 0

    for position in positions:
        code = str(position.get("code") or "").strip()
        name = str(position.get("name") or "").strip()
        item = today_map.get(code) or today_map.get(name) or {"code": code, "name": name}
        enriched = attach_wababa_value_reasons(item, position)
        consistency_score = safe_number(enriched.get("growthConsistencyScore") or calculate_growth_consistency_score(enriched))
        consistency_label = str(enriched.get("growthConsistencyLabel") or classify_growth_consistency(consistency_score) or "점검")
        hold_reasons = enriched.get("holdReason") if isinstance(enriched.get("holdReason"), list) else build_hold_reason(enriched, position)
        sell_reasons = enriched.get("sellReason") if isinstance(enriched.get("sellReason"), list) else build_sell_reason(enriched, position)
        dividend_yield = safe_number(enriched.get("dividendYield") or enriched.get("divYield"))
        buy_date = str(position.get("buyDate") or position.get("lastBuyDate") or "").strip()
        holding_days = calculate_days_between(buy_date, base_date_text)
        profit_rate = calculate_profit_rate(enriched, position)
        thesis_broken = any(
            keyword in str(reason)
            for reason in sell_reasons
            for keyword in ["붕괴", "감소", "이탈", "부정"]
        )
        action = "SELL_CHECK" if thesis_broken else "HOLD"
        if thesis_broken:
            sell_watch_count += 1

        next_check = "성장 지속 여부"
        if consistency_score >= 80:
            next_check = "장기 보유 가능성 유지 여부"
        elif consistency_score >= 60:
            next_check = "다음 실적에서도 성장 지속 확인"
        else:
            next_check = "일회성 성장인지 재확인"

        if dividend_yield > 0:
            next_check += f" · 배당수익률 {dividend_yield:.1f}% 반영"

        items.append({
            "code": code,
            "name": name or get_name(enriched),
            "action": action,
            "actionLabel": "보유 유지" if action == "HOLD" else "매도 점검",
            "buyDate": buy_date,
            "holdingDays": holding_days,
            "profitRate": profit_rate,
            "growthConsistencyScore": round(consistency_score, 1),
            "growthConsistencyLabel": consistency_label,
            "holdReason": hold_reasons[:4],
            "sellReason": sell_reasons[:4],
            "dividendYield": round(dividend_yield, 2),
            "nextCheck": next_check,
        })

    if not positions:
        summary_lines.append("보유 종목 없음. 기준 충족 종목이 나올 때까지 현금 대기.")
    else:
        summary_lines.append(f"보유 종목 {len(positions)}개 정기 점검 완료.")
        if sell_watch_count > 0:
            summary_lines.append(f"매도 점검 {sell_watch_count}개. 성장 가설 훼손 여부 우선 확인.")
        else:
            summary_lines.append("현재 매도 점검 종목 없음. 성장 유지 여부 중심으로 보유.")

    return {
        "date": base_date_text,
        "summary": " ".join(summary_lines[:2]),
        "holdingCount": len(items),
        "sellWatchCount": sell_watch_count,
        "items": items,
    }

def build_daily_fund_memo(portfolio, fund_trade_result, auto_trade_pick, portfolio_summary, performance_analysis, base_date):
    """Create a short operator-style daily memo for the Wababa value fund.

    This memo is intentionally concise. It records what the fund did today,
    why it did or did not buy, and what the fund should keep checking while
    holding positions.
    """
    base_date_text = str(base_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    result = fund_trade_result if isinstance(fund_trade_result, dict) else {}
    status = str(result.get("status") or "").strip()
    message = str(result.get("message") or "").strip()
    orders = result.get("orders") if isinstance(result.get("orders"), list) else []
    skipped = result.get("skipped") if isinstance(result.get("skipped"), list) else []
    positions = normalize_portfolio_positions(portfolio)
    summary = portfolio_summary if isinstance(portfolio_summary, dict) else {}
    analysis = performance_analysis if isinstance(performance_analysis, dict) else {}
    portfolio_perf = analysis.get("portfolioPerformance") if isinstance(analysis.get("portfolioPerformance"), dict) else {}

    lines = []
    title = f"{base_date_text} 와바바 운용일지"

    if status == "TRADED" and orders:
        buy_orders = [order for order in orders if isinstance(order, dict) and str(order.get("action") or "").upper() == "BUY"]
        sell_orders = [order for order in orders if isinstance(order, dict) and str(order.get("action") or "").upper() == "SELL"]
        if buy_orders:
            order = buy_orders[0]
            lines.append(f"{order.get('name') or order.get('code')} 신규 매수.")
            reason = str(order.get("reason") or "").strip()
            if reason:
                lines.append(reason)
        if sell_orders:
            names = [str(order.get("name") or order.get("code") or "").strip() for order in sell_orders if isinstance(order, dict)]
            lines.append("매도 반영: " + ", ".join([name for name in names if name]) + ".")
    elif status == "ALREADY_EXECUTED":
        lines.append("오늘 자동운용은 이미 실행되어 추가 매수하지 않음.")
    elif status == "MARKET_CLOSED":
        lines.append("주식 개장일이 아니어서 매매하지 않음.")
    elif status == "WAIT_START":
        lines.append(message or "펀드 시작일 전이라 매매하지 않음.")
    elif status == "NO_TRADE":
        lines.append("오늘은 기준을 만족하는 신규 매수 체결 없음.")
    elif status == "OFF":
        lines.append("자동운용이 꺼져 있어 매매하지 않음.")
    else:
        lines.append(message or "오늘 운용 결과를 기록함.")

    if isinstance(auto_trade_pick, dict):
        pick_name = get_name(auto_trade_pick)
        if pick_name:
            if orders:
                lines.append(f"선정 근거: {pick_name}이 가치매수 후보로 통과.")
            elif status in ["NO_TRADE", "ALREADY_EXECUTED"]:
                pick_reason = str(auto_trade_pick.get("autoTradeReason") or "").strip()
                if pick_reason:
                    lines.append(f"검토 후보: {pick_name} · {pick_reason}")
                else:
                    lines.append(f"검토 후보: {pick_name}")

    position_count = len(positions)
    cash = safe_number(summary.get("cash"))
    total_asset = safe_number(summary.get("totalAssetAmount"))
    total_return_rate = portfolio_perf.get("totalProfitRate")
    total_return_text = "-"
    if total_return_rate is not None:
        total_return_text = f"{safe_number(total_return_rate):.2f}%"

    if position_count > 0:
        held_names = [str(position.get("name") or position.get("code") or "").strip() for position in positions[:3]]
        held_text = ", ".join([name for name in held_names if name])
        if held_text:
            lines.append(f"보유: {held_text} 중심으로 성장 유지 여부 점검.")
        else:
            lines.append(f"보유 종목 {position_count}개. 성장 유지 여부 점검.")
    else:
        lines.append("보유 종목 없음. 좋은 주식이 나올 때까지 현금 대기.")

    if total_asset > 0:
        lines.append(f"총자산 {format_krw_amount_text(total_asset)}, 누적수익률 {total_return_text}.")
    elif cash > 0:
        lines.append(f"현금 {format_krw_amount_text(cash)} 대기.")

    if skipped and status not in ["ALREADY_EXECUTED"]:
        first_skip = skipped[0] if isinstance(skipped[0], dict) else {}
        skip_reason = str(first_skip.get("reason") or "").strip()
        skip_name = str(first_skip.get("name") or first_skip.get("code") or "").strip()
        if skip_reason:
            if skip_name:
                lines.append(f"미체결: {skip_name} · {skip_reason}")
            else:
                lines.append(f"미체결: {skip_reason}")

    # Deduplicate while preserving order and keep UI compact.
    unique_lines = []
    for line in lines:
        clean = " ".join(str(line or "").split()).strip()
        if clean and clean not in unique_lines:
            unique_lines.append(clean)

    return {
        "date": base_date_text,
        "title": title,
        "status": status or "UNKNOWN",
        "summary": unique_lines[0] if unique_lines else "오늘 운용 메모 없음.",
        "lines": unique_lines[:5],
        "holdingCount": position_count,
        "orderCount": len(orders),
        "cash": round(cash, 0),
        "totalAssetAmount": round(total_asset, 0),
        "totalReturnRate": safe_number(total_return_rate) if total_return_rate is not None else None,
    }

def main():
    # --no-trade 또는 WABABA_DISABLE_AUTO_TRADE=1 이면 자동매매 스킵
    # 문장 QA / 테스트 재생성 시 반드시 이 옵션으로 실행할 것:
    #   python scripts\build_recommendation_history.py --no-trade
    #   $env:WABABA_DISABLE_AUTO_TRADE="1"; python scripts\build_recommendation_history.py
    no_trade_mode = (
        "--no-trade" in sys.argv
        or os.environ.get("WABABA_DISABLE_AUTO_TRADE", "").strip() == "1"
    )

    print("추천 히스토리 생성 시작")
    print(f"시작 시간: {now_text()}")
    if no_trade_mode:
        print("[no-trade 모드] 자동매매 스킵 — portfolio/trade 파일 변경 없음")

    config = load_filter_config()

    market_snapshot = read_json(MARKET_SNAPSHOT_PATH)

    if market_snapshot is None:
        raise FileNotFoundError(f"시장 스냅샷 파일을 찾을 수 없습니다: {MARKET_SNAPSHOT_PATH}")

    news_momentum = read_json(NEWS_MOMENTUM_PATH) or {}
    previous_history = read_json(RECOMMENDATION_HISTORY_PATH) or {}
    portfolio = load_portfolio()
    ai_portfolio = load_ai_portfolio()

    base_date = get_base_date(market_snapshot)

    news_map = build_news_map(news_momentum)

    raw_items = get_items(market_snapshot)
    today_items = [normalize_item(item, news_map) for item in raw_items]
    today_map = {item["code"]: item for item in today_items if item.get("code")}
    portfolio_map = build_portfolio_position_map(portfolio)
    portfolio_summary = {}
    fund_policy = get_fund_policy(portfolio)
    fund_trade_result = {"status": "PENDING", "message": "자동 운용 판단 전", "orders": [], "skipped": []}
    ai_fund_policy = get_ai_fund_policy(ai_portfolio)
    ai_fund_trade_result = {"status": "PENDING", "message": "AI펀드 자동 운용 판단 전", "orders": [], "skipped": []}

    previous_active_items = []

    for key in ["wababaPicks", "buyCandidates", "holdCandidates", "recommendations", "continuedItems"]:
        value = previous_history.get(key)

        if isinstance(value, list):
            previous_active_items.extend(value)

    previous_active_map = {}

    for item in previous_active_items:
        code = get_code(item)

        if code:
            previous_active_map[code] = item

    all_wababa_candidates = []

    for item in today_items:
        if is_wababa_candidate(item, config):
            all_wababa_candidates.append(
                {
                    **item,
                    "generalWababaScore": calculate_wababa_score(item),
                    "stableScore": calculate_stable_score(item),
                    "growthScore": calculate_growth_score(item),
                    "opportunityScore": calculate_opportunity_score(item),
                    "qualityPenalty": calculate_quality_penalty(item),
                    "qualityLevel": classify_quality_level(item),
                    "qualityWarnings": build_quality_warnings(item),
                    "finalBestScore": calculate_final_best_score(item),
                    "todayPickScore": calculate_final_best_score(item),
                }
            )

    reviewed_codes = load_reviewed_candidate_codes()
    main_wababa_candidates = filter_unreviewed_candidates(
        all_wababa_candidates,
        reviewed_codes,
    )

    stable_candidates = [
        item for item in main_wababa_candidates if is_stable_candidate(item)
    ]

    if not stable_candidates:
        stable_candidates = sorted(
            main_wababa_candidates,
            key=lambda item: safe_number(item.get("stableScore")),
            reverse=True,
        )[:10]

    growth_candidates = [
        item for item in main_wababa_candidates if is_growth_candidate(item)
    ]

    if not growth_candidates:
        growth_candidates = [
            item for item in main_wababa_candidates if is_relaxed_growth_candidate(item)
        ]

    if not growth_candidates:
        growth_candidates = sorted(
            main_wababa_candidates,
            key=lambda item: safe_number(item.get("growthScore")),
            reverse=True,
        )[:10]

    opportunity_candidates = [
        item for item in main_wababa_candidates if is_opportunity_candidate(item)
    ]

    if not opportunity_candidates:
        opportunity_candidates = sorted(
            main_wababa_candidates,
            key=lambda item: safe_number(item.get("opportunityScore")),
            reverse=True,
        )[:10]

    wababa_picks = []
    used_wababa_codes = set()

    stable_pick = pick_best_unique(stable_candidates, "stableScore", used_wababa_codes)
    if stable_pick:
        wababa_picks.append(attach_wababa_pick(stable_pick, "안정형", "stableScore"))
        used_wababa_codes.add(get_code(stable_pick))

    growth_pick = pick_best_unique(growth_candidates, "growthScore", used_wababa_codes)
    if growth_pick:
        wababa_picks.append(attach_wababa_pick(growth_pick, "성장형", "growthScore"))
        used_wababa_codes.add(get_code(growth_pick))

    opportunity_pick = pick_best_unique(
        opportunity_candidates,
        "opportunityScore",
        used_wababa_codes,
    )
    if opportunity_pick:
        wababa_picks.append(
            attach_wababa_pick(opportunity_pick, "기회형", "opportunityScore")
        )
        used_wababa_codes.add(get_code(opportunity_pick))

    max_wababa_picks = safe_int(config.get("maxWababaPicks"), 3)

    if len(wababa_picks) < max_wababa_picks:
        fallback_candidates = sorted(
            main_wababa_candidates,
            key=lambda item: safe_number(item.get("generalWababaScore")),
            reverse=True,
        )

        for item in fallback_candidates:
            if len(wababa_picks) >= max_wababa_picks:
                break

            code = get_code(item)

            if not code or code in used_wababa_codes:
                continue

            wababa_picks.append(
                attach_wababa_pick(item, "보완형", "generalWababaScore")
            )
            used_wababa_codes.add(code)

    wababa_picks = [
        {
            **item,
            "rank": index + 1,
            "rankReason": build_wababa_rank_reason(item, index + 1),
        }
        for index, item in enumerate(wababa_picks[:max_wababa_picks])
    ]

    final_best_pool = [
        item for item in main_wababa_candidates
        if classify_quality_level(item) != "주의"
    ]

    if not final_best_pool:
        final_best_pool = main_wababa_candidates

    final_best_source = pick_final_best(final_best_pool)
    final_best_pick = None

    if final_best_source:
        final_best_pick = {
            **attach_wababa_pick(final_best_source, "오늘 1종목", "finalBestScore"),
            "rank": 1,
            "rankReason": build_wababa_rank_reason(final_best_source, 1),
            "finalBestScore": safe_number(final_best_source.get("finalBestScore")),
        }

    previous_wababa = previous_history.get("wababaPicks", [])
    if not isinstance(previous_wababa, list):
        previous_wababa = []

    previous_wababa_codes = {
        get_code(item) for item in previous_wababa if get_code(item)
    }

    current_wababa_codes = {
        get_code(item) for item in wababa_picks if get_code(item)
    }

    new_wababa_codes = current_wababa_codes - previous_wababa_codes
    removed_wababa_codes = previous_wababa_codes - current_wababa_codes
    continued_wababa_codes = current_wababa_codes & previous_wababa_codes

    new_wababa_picks = [
        item for item in wababa_picks if get_code(item) in new_wababa_codes
    ]

    removed_wababa_picks = [
        item for item in previous_wababa if get_code(item) in removed_wababa_codes
    ]

    continued_wababa_picks = [
        item for item in wababa_picks if get_code(item) in continued_wababa_codes
    ]

    wababa_pick_codes = {
        get_code(item) for item in wababa_picks if get_code(item)
    }

    main_pick_codes = set(wababa_pick_codes)

    if final_best_pick:
        final_best_code = get_code(final_best_pick)
        if final_best_code:
            main_pick_codes.add(final_best_code)

    explore_groups, explore_candidates = build_explore_groups(
        all_wababa_candidates,
        main_pick_codes,
    )

    buy_candidates = []

    for item in today_items:
        code = get_code(item)

        if not code:
            continue

        if code in previous_active_map:
            continue

        if code in wababa_pick_codes:
            continue

        if is_buy_candidate(item, config):
            buy_candidates.append(
                attach_decision(
                    item,
                    "BUY",
                    build_legacy_buy_reason(item),
                )
            )

    max_buy_candidates = safe_int(config.get("maxBuyCandidates"), 3)

    buy_candidates = sorted(
        buy_candidates,
        key=lambda item: safe_number(item.get("score")),
        reverse=True,
    )[:max_buy_candidates]

    sell_candidates = []

    for previous_item in previous_active_map.values():
        if is_sell_candidate(previous_item, today_map, config):
            sell_candidates.append(
                attach_decision(
                    previous_item,
                    "SELL",
                    build_legacy_sell_reason(previous_item),
                )
            )

    sell_candidates = sorted(
        sell_candidates,
        key=lambda item: safe_number(item.get("score")),
        reverse=True,
    )

    sell_codes = {get_code(item) for item in sell_candidates}

    hold_candidates = []

    for previous_item in previous_active_map.values():
        code = get_code(previous_item)

        if not code:
            continue

        if code in sell_codes:
            continue

        latest_item = today_map.get(code, previous_item)

        hold_candidates.append(
            attach_decision(
                latest_item,
                "HOLD",
                build_legacy_hold_reason(latest_item),
            )
        )

    hold_candidates = sorted(
        hold_candidates,
        key=lambda item: safe_number(item.get("score")),
        reverse=True,
    )

    quality_warning_count = sum(
        1 for item in wababa_picks
        if build_quality_warnings(item)
    )

    no_action = (
        len(wababa_picks) == 0
        and len(buy_candidates) == 0
        and len(sell_candidates) == 0
    )

    auto_trade_pick = pick_auto_trade_candidate(
        final_best_pick,
        wababa_picks,
        explore_groups,
        portfolio,
        fund_policy,
    )

    if no_trade_mode:
        fund_trade_result = {
            "status": "NO_TRADE_MODE",
            "message": "--no-trade 모드: 자동매매 스킵. portfolio 변경 없음.",
            "orders": [],
            "skipped": [{"action": "AUTO", "reason": "no-trade 모드"}],
        }
    else:
        portfolio, fund_trade_result = apply_wababa_fund_auto_trading(
            portfolio,
            auto_trade_pick,
            today_map,
            base_date,
        )

    if isinstance(fund_trade_result, dict):
        fund_trade_result["autoTradePick"] = {
            "code": get_code(auto_trade_pick),
            "name": get_name(auto_trade_pick),
            "source": auto_trade_pick.get("autoTradeSource") if isinstance(auto_trade_pick, dict) else "",
            "score": safe_number(auto_trade_pick.get("autoTradeScore")) if isinstance(auto_trade_pick, dict) else 0,
            "reason": auto_trade_pick.get("autoTradeReason") if isinstance(auto_trade_pick, dict) else "",
        } if isinstance(auto_trade_pick, dict) else None
    portfolio_map = build_portfolio_position_map(portfolio)
    portfolio_summary = build_portfolio_summary(portfolio, portfolio_map, today_map, previous_history.get("portfolioSummary"))
    performance_analysis = build_wababa_performance_analysis(portfolio, portfolio_summary, today_map, base_date)
    daily_fund_memo = build_daily_fund_memo(
        portfolio,
        fund_trade_result,
        auto_trade_pick,
        portfolio_summary,
        performance_analysis,
        base_date,
    )
    holding_review = build_holding_review(portfolio, today_map, base_date)
    fund_policy = get_fund_policy(portfolio)

    ai_trade_picks = pick_wababa_ai_trade_candidates(all_wababa_candidates, ai_portfolio, ai_fund_policy)
    if no_trade_mode:
        ai_fund_trade_result = {
            "status": "NO_TRADE_MODE",
            "message": "--no-trade 모드: AI펀드 자동매매 스킵. portfolio 변경 없음.",
            "orders": [],
            "skipped": [{"action": "AUTO", "reason": "no-trade 모드"}],
        }
    else:
        ai_portfolio, ai_fund_trade_result = apply_wababa_ai_fund_auto_trading(
            ai_portfolio,
            ai_trade_picks,
            today_map,
            base_date,
        )
    ai_portfolio_map = build_portfolio_position_map(ai_portfolio)
    ai_portfolio_summary = build_portfolio_summary(ai_portfolio, ai_portfolio_map, today_map, previous_history.get("aiPortfolioSummary"))
    ai_performance_analysis = build_wababa_performance_analysis(ai_portfolio, ai_portfolio_summary, today_map, base_date)
    ai_daily_fund_memo = build_ai_daily_fund_memo(
        ai_portfolio,
        ai_fund_trade_result,
        ai_trade_picks,
        ai_portfolio_summary,
        ai_performance_analysis,
        base_date,
    )
    ai_holding_review = build_holding_review(ai_portfolio, today_map, base_date)
    ai_fund_policy = get_ai_fund_policy(ai_portfolio)

    wababa_picks = attach_portfolio_to_list(wababa_picks, portfolio_map)
    buy_candidates = attach_portfolio_to_list(buy_candidates, portfolio_map)
    hold_candidates = attach_portfolio_to_list(hold_candidates, portfolio_map)
    sell_candidates = attach_portfolio_to_list(sell_candidates, portfolio_map)
    new_wababa_picks = attach_portfolio_to_list(new_wababa_picks, portfolio_map)
    continued_wababa_picks = attach_portfolio_to_list(continued_wababa_picks, portfolio_map)
    removed_wababa_picks = attach_portfolio_to_list(removed_wababa_picks, portfolio_map)
    explore_candidates = attach_portfolio_to_list(explore_candidates, portfolio_map)
    explore_groups = {
        key: attach_portfolio_to_list(value, portfolio_map)
        for key, value in explore_groups.items()
    }

    if final_best_pick:
        final_best_pick = attach_portfolio_decision(final_best_pick, portfolio_map)

    result = {
        "agentName": "와바바",
        "agentFullName": "와바바 (Why Buy & Bye?)",
        "baseDate": base_date,
        "generatedAt": now_text(),
        "filterConfig": config,
        "fundPolicy": fund_policy,
        "fundTradeResult": fund_trade_result,
        "portfolioSummary": portfolio_summary,
        "performanceAnalysis": performance_analysis,
        "dailyFundMemo": daily_fund_memo,
        "holdingReview": holding_review,
        "portfolio": portfolio,
        "aiFundPolicy": ai_fund_policy,
        "aiFundTradeResult": ai_fund_trade_result,
        "aiPortfolioSummary": ai_portfolio_summary,
        "aiPerformanceAnalysis": ai_performance_analysis,
        "aiDailyFundMemo": ai_daily_fund_memo,
        "aiHoldingReview": ai_holding_review,
        "aiPortfolio": ai_portfolio,
        "summary": {
            "wababaPickCount": len(wababa_picks),
            "newWababaPickCount": len(new_wababa_picks),
            "continuedWababaPickCount": len(continued_wababa_picks),
            "removedWababaPickCount": len(removed_wababa_picks),
            "buyCount": len(buy_candidates),
            "holdCount": len(hold_candidates),
            "sellCount": len(sell_candidates),
            "qualityWarningCount": quality_warning_count,
            "reviewedCandidateCount": len(reviewed_codes),
            "exploreCandidateCount": len(explore_candidates),
            "totalExploreCandidateCount": len(explore_groups.get("total", [])),
            "stableExploreCandidateCount": len(explore_groups.get("stable", [])),
            "growthExploreCandidateCount": len(explore_groups.get("growth", [])),
            "opportunityExploreCandidateCount": len(explore_groups.get("opportunity", [])),
            "noAction": no_action,
        },
        "reviewedCandidateCodes": sorted(reviewed_codes),
        "finalBestPick": final_best_pick,
        "todayPick": final_best_pick,
        "todayOnePick": final_best_pick,
        "wababaPicks": wababa_picks,
        "exploreGroups": explore_groups,
        "exploreCandidates": explore_candidates,
        "newWababaPicks": new_wababa_picks,
        "continuedWababaPicks": continued_wababa_picks,
        "removedWababaPicks": removed_wababa_picks,
        "buyCandidates": buy_candidates,
        "holdCandidates": hold_candidates,
        "sellCandidates": sell_candidates,
        "noAction": no_action,
    }

    write_json(RECOMMENDATION_HISTORY_PATH, result)

    print(f"written: {RECOMMENDATION_HISTORY_PATH}")
    print(f"baseDate: {base_date}")
    print(f"wababaPicks: {len(wababa_picks)}")
    print(f"finalBestPick: {get_name(final_best_pick) if final_best_pick else '-'}")
    print(f"newWababaPicks: {len(new_wababa_picks)}")
    print(f"continuedWababaPicks: {len(continued_wababa_picks)}")
    print(f"removedWababaPicks: {len(removed_wababa_picks)}")
    print(f"buyCandidates: {len(buy_candidates)}")
    print(f"holdCandidates: {len(hold_candidates)}")
    print(f"sellCandidates: {len(sell_candidates)}")
    print(f"portfolioPositions: {portfolio_summary.get('positionCount', 0)}")
    print(f"fundTradeStatus: {fund_trade_result.get('status')}")
    print(f"fundOrders: {len(fund_trade_result.get('orders', []))}")
    print(f"aiPortfolioPositions: {ai_portfolio_summary.get('positionCount', 0)}")
    print(f"aiFundTradeStatus: {ai_fund_trade_result.get('status')}")
    print(f"aiFundOrders: {len(ai_fund_trade_result.get('orders', []))}")
    print(f"qualityWarningCount: {quality_warning_count}")
    print(f"reviewedCandidateCount: {len(reviewed_codes)}")
    print(f"exploreCandidates: {len(explore_candidates)}")
    print(f"noAction: {no_action}")
    print(f"filterConfig: {FILTER_CONFIG_PATH}")


if __name__ == "__main__":
    main()