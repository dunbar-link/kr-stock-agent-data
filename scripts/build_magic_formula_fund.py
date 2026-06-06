#!/usr/bin/env python3
"""와바바 마법공식 펀드 - 내부 데이터/운용 (Phase 43-B ~ 43-B6).

독립 실행 모듈. 기존 build_recommendation_history.py(2펀드)와 daily_run 흐름을
일절 건드리지 않고, 마법공식 펀드의 "내부" 데이터만 생성한다.
public JSON(REPO1)에는 아무것도 쓰지 않는다(43-C에서 분리 처리).

공식 모드(formulaMode):
  - approx           : 수익성=ROE, 저평가=PER (43-B, 비교/fallback용)
  - book_faithful_v1 : 수익성=ROC=EBIT/(순운전자본+유형자산), 저평가=EarningsYield=EBIT/EV
                       (43-B4~B5, DART 재무제표 기반. 기본값)
  EV v1 = 시가총액(억원×1e8) + 부채총계 - 현금  (evMethod=marketCap_plus_totalLiabilities_minus_cash)
  ※ marketCap 단위 = 억원(×1e8), 43-B5에서 PER×NI/mc·PBR×Eq/mc 양쪽 median≈1e8로 확정.

43-B6 운영기록 안정화(공식 무변경):
  - formulaVersion / tradingRuleVersion 고정 + 전 파일에 version metadata 기록
  - magic-formula-operation-log.json : 매일 운용 로그(운영자용, append, 날짜 dedupe)
  - magic-formula-change-log.json     : 공식/운용 변경 로그(seed-if-missing)
  - 공식수정 중독 방지 가드: 현재 version이 change-log에 없으면 sanityWarning

운용부(대장 기준, 절대 불변):
  매일 top10 매수 / 종목당 약 10만 / 5천만÷50거래일=하루 100만 / 시작가 / lot 50거래일 보유 후 시가 매도 /
  같은 종목 재진입은 매도 lot + 신규 lot 분리 / 주말·휴일 매매 SKIP.

CLI:
  python scripts/build_magic_formula_fund.py [--reset] [--asof YYYY-MM-DD] [--fetch-open] [--mode MODE]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "financial-universe-real.json"
RECO_HISTORY_PATH = ROOT / "recommendation-history.json"
OPEN_CACHE_PATH = ROOT / "magic-formula-open-cache.json"
HOLIDAYS_PATH = ROOT / "magic-formula-holidays.json"
BLACKLIST_PATH = ROOT / "magic-formula-blacklist.json"
CORP_CODES_PATH = ROOT / "_cache" / "dart-corp-codes.json"
DART_DIR = ROOT / "_cache" / "dart-statements"

PORTFOLIO_PATH = ROOT / "magic-formula-portfolio.json"
LOTS_PATH = ROOT / "magic-formula-trade-lots.json"
ACTIONS_PATH = ROOT / "magic-formula-daily-actions.json"
RANKINGS_PATH = ROOT / "magic-formula-rankings.json"
REALIZED_PATH = ROOT / "magic-formula-realized-trades.json"
OPERATION_LOG_PATH = ROOT / "magic-formula-operation-log.json"
CHANGE_LOG_PATH = ROOT / "magic-formula-change-log.json"

CONFIG = {
    "fundName": "와바바 마법공식 펀드",
    "initialCapital": 50000000,
    "dailyBudget": 1000000,
    "topN": 10,
    "perStockTarget": 100000,
    "holdingTradingDaysTarget": 50,
    "minMarketCap": 300,
    "rankTop100": 100,
    "financeIndustries": ["금융", "기타금융", "증권", "은행", "보험"],
    "financeNameKeywords": ["증권", "은행", "지주", "홀딩스", "캐피탈", "보험", "파이낸셜", "카드"],
    "utilityIndustries": ["전기·가스", "전기·가스·수도"],
    "utilityNameKeywords": ["한국전력", "한국가스공사", "지역난방", "수자원"],
    "priceField": "price",
    # 공식 version lock (43-B6)
    "formulaMode": "book_faithful_v1",
    "formulaVersion": "book-faithful-v1-2026-43B5",
    "formulaVersionLockedAt": "2026-06-06",
    "tradingRuleVersion": "magic-50td-start-open-top10-v1",
    "liveOperationStarted": False,
    "formulaChangeRequiresVersionBump": True,
    "allowFormulaTuningWithoutVersionBump": False,
    "evMethod": "marketCap_plus_totalLiabilities_minus_cash",
    "marketCapUnitAssumption": "억원(KRW 100M) -> ×100,000,000 = 원. 43-B5 검증: PER×순이익/mc, PBR×자본/mc 양쪽 median≈1.0e8(log10=8.00)로 확정",
    "marketCapToWon": 100000000,
    "formulaInputs": {
        "profitability": "ReturnOnCapital = EBIT / (순운전자본 + 유형자산)  [순운전자본=유동자산-유동부채]",
        "value": "EarningsYield = EBIT / EnterpriseValue  [EV=시총+부채총계-현금]",
        "combined": "valueRank + profitabilityRank (낮을수록 우수)",
        "ebit": "DART dart_OperatingIncomeLoss(영업이익), IS->CIS",
    },
    "dartAccounts": {
        "ebit": ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        "cash": ["ifrs-full_CashAndCashEquivalents"],
        "currentAssets": ["ifrs-full_CurrentAssets"],
        "currentLiabilities": ["ifrs-full_CurrentLiabilities"],
        "ppe": ["ifrs-full_PropertyPlantAndEquipment"],
        "totalLiabilities": ["ifrs-full_Liabilities"],
    },
}

# 공식/운용 변경 로그 seed (파일이 없을 때만 기록. 이후 수동/버전업 시 append)
CHANGE_LOG_SEED = [
    {"date": "2026-06-06", "changeType": "formula_metadata", "formulaMode": "approx",
     "formulaVersion": "approx-roe-per-2026-43B3", "tradingRuleVersion": "magic-50td-start-open-top10-v1",
     "description": "approx 공식(ROE/PER) 명시 + formulaMode/version 메타 도입",
     "reason": "근사 공식임을 내부 데이터에 박제", "expectedImpact": "표기만, 계산 동일",
     "backwardCompatibility": "유지", "publicImpact": "없음", "operatorNote": "Phase 43-B3"},
    {"date": "2026-06-06", "changeType": "formula_upgrade", "formulaMode": "book_faithful_v1",
     "formulaVersion": "book-faithful-v1-2026-43B4", "tradingRuleVersion": "magic-50td-start-open-top10-v1",
     "description": "DART 기반 ROC/EarningsYield 원전 근사 공식 도입(기본 모드 전환)",
     "reason": "그린블라트 원전에 근접", "expectedImpact": "top10 대폭 변동(approx 대비 9/10 상이)",
     "backwardCompatibility": "approx는 비교용 유지", "publicImpact": "없음", "operatorNote": "Phase 43-B4"},
    {"date": "2026-06-06", "changeType": "data_unit_fix", "formulaMode": "book_faithful_v1",
     "formulaVersion": "book-faithful-v1-2026-43B5", "tradingRuleVersion": "magic-50td-start-open-top10-v1",
     "description": "marketCap 단위를 억원(×1e8)로 확정(이전 ×1e6 오류 정정)",
     "reason": "EV 100배 과소 → EarningsYield 100배 과대(EY>1) 해소",
     "expectedImpact": "EY median 3.1%로 정상화, EY>1=0, top10 재편(EV 큰 종목 하락)",
     "backwardCompatibility": "공식 정의 동일, 입력 단위만 정정", "publicImpact": "없음", "operatorNote": "Phase 43-B5"},
    {"date": "2026-06-06", "changeType": "trading_rule_lock", "formulaMode": "book_faithful_v1",
     "formulaVersion": "book-faithful-v1-2026-43B5", "tradingRuleVersion": "magic-50td-start-open-top10-v1",
     "description": "매일 top10·시작가 매수/매도·50거래일 보유·lot 분리 규칙 고정",
     "reason": "운용 규칙 동결로 성과 비교 기준 확립", "expectedImpact": "운용 동일",
     "backwardCompatibility": "유지", "publicImpact": "없음", "operatorNote": "대장 기준"},
    {"date": "2026-06-06", "changeType": "operational_stabilization", "formulaMode": "book_faithful_v1",
     "formulaVersion": "book-faithful-v1-2026-43B5", "tradingRuleVersion": "magic-50td-start-open-top10-v1",
     "description": "version lock + operation-log + change-log + 공식수정 가드(sanityWarning) 도입",
     "reason": "공식 수정과 운용 성과 분리, 재현성/회귀분석 확보",
     "expectedImpact": "계산 무변경, 운영기록만 안정화", "backwardCompatibility": "유지",
     "publicImpact": "없음(public 미반영)", "operatorNote": "Phase 43-B6"},
]


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_json(path, default=None):
    if not Path(path).exists():
        return default
    with Path(path).open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, data):
    with Path(path).open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def num(value):
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ----- 실거래일 -----

def parse_date(text):
    try:
        return datetime.fromisoformat(str(text)[:10]).date()
    except ValueError:
        return None


def is_weekend(date_text):
    d = parse_date(date_text)
    return d is not None and d.weekday() >= 5


def is_market_open_day(date_text, holidays):
    d = parse_date(date_text)
    if d is None or d.weekday() >= 5:
        return False
    return d.strftime("%Y-%m-%d") not in set(holidays or [])


def add_trading_days(start_text, n, holidays):
    d = parse_date(start_text)
    if d is None:
        return None
    holiday_set, count, cur = set(holidays or []), 0, d
    while count < n:
        cur += timedelta(days=1)
        if cur.weekday() >= 5 or cur.strftime("%Y-%m-%d") in holiday_set:
            continue
        count += 1
    return cur.strftime("%Y-%m-%d")


# ----- 시가 resolver -----

def fetch_open_map_pykrx(base_date):
    try:
        from pykrx import stock  # type: ignore
        frame = stock.get_market_ohlcv_by_ticker(str(base_date).replace("-", ""), market="ALL")
        out = {}
        for ticker, row in frame.iterrows():
            v = num(row.get("시가"))
            if v and v > 0:
                out[str(ticker).zfill(6)] = v
        return out
    except Exception as error:
        print(f"  [WARN] pykrx 시가 조회 실패 -> fallback: {error}")
        return {}


def make_price_resolver(base_date, open_cache, open_map):
    cache_day = open_cache.get(base_date) if isinstance(open_cache, dict) else None
    cache_day = cache_day if isinstance(cache_day, dict) else {}

    def resolve(code, universe_price):
        if code in cache_day and num(cache_day[code]):
            return num(cache_day[code]), "ohlc_cache"
        if code in open_map and num(open_map[code]):
            return num(open_map[code]), "pykrx_open"
        return universe_price, "fallback_price"

    return resolve


# ----- DART 로더 (book_faithful) -----

_CORP_MAP = None


def load_dart_corp_code_map():
    global _CORP_MAP
    if _CORP_MAP is None:
        raw = read_json(CORP_CODES_PATH, {}) or {}
        _CORP_MAP = {}
        if isinstance(raw, dict):
            for sym, v in raw.items():
                if isinstance(v, dict) and v.get("corp_code"):
                    _CORP_MAP[str(sym).zfill(6)] = v["corp_code"]
    return _CORP_MAP


def load_dart_statement_for_symbol(symbol, year, fs):
    corp_code = load_dart_corp_code_map().get(str(symbol).zfill(6))
    if not corp_code:
        return None, None
    years = [year, (year - 1) if isinstance(year, int) else None]
    fs_order = [fs, "CFS", "OFS"] if fs else ["CFS", "OFS"]
    for y in [yr for yr in years if yr]:
        for f in fs_order:
            p = DART_DIR / f"{corp_code}_{y}_{f}.json"
            if p.exists():
                doc = read_json(p, None)
                if isinstance(doc, list) and doc:
                    return doc, f"DART {y} {f}"
    return None, None


def extract_dart_amount(stmt, account_ids, sj_priority, nm_contains=None, nm_exclude=None):
    ids = set(account_ids)
    for sj in sj_priority:
        for it in stmt:
            if it.get("sj_div") != sj:
                continue
            nm = (it.get("account_nm") or "").strip()
            id_hit = it.get("account_id") in ids
            nm_hit = bool(nm_contains) and nm_contains in nm and not (nm_exclude and nm_exclude in nm)
            if id_hit or nm_hit:
                v = num(it.get("thstrm_amount"))
                if v is not None:
                    return v
    return None


def build_book_faithful_metrics(stock):
    symbol = str(stock.get("symbol") or stock.get("code") or "")
    stmt, source = load_dart_statement_for_symbol(symbol, stock.get("dartLatestYear"), stock.get("dartFsDiv"))
    if not stmt:
        return None
    acc = CONFIG["dartAccounts"]
    return {
        "ebit": extract_dart_amount(stmt, acc["ebit"], ["IS", "CIS"], nm_contains="영업이익", nm_exclude="률"),
        "cash": extract_dart_amount(stmt, acc["cash"], ["BS"], nm_contains="현금및현금성자산"),
        "currentAssets": extract_dart_amount(stmt, acc["currentAssets"], ["BS"], nm_contains="유동자산", nm_exclude="비유동"),
        "currentLiabilities": extract_dart_amount(stmt, acc["currentLiabilities"], ["BS"], nm_contains="유동부채", nm_exclude="비유동"),
        "ppe": extract_dart_amount(stmt, acc["ppe"], ["BS"], nm_contains="유형자산"),
        "totalLiabilities": extract_dart_amount(stmt, acc["totalLiabilities"], ["BS"], nm_contains="부채총계"),
        "dataSource": source,
    }


# ----- 업종 제외 -----

def is_finance(stock):
    industry = str(stock.get("industryName") or "").strip()
    if industry in CONFIG["financeIndustries"]:
        return True
    blob = industry + " " + str(stock.get("corpName") or stock.get("name") or "")
    return any(kw in blob for kw in CONFIG["financeNameKeywords"])


def is_utility(stock):
    industry = str(stock.get("industryName") or "").strip()
    if industry in CONFIG["utilityIndustries"]:
        return True
    name = str(stock.get("corpName") or stock.get("name") or "")
    return any(kw in name for kw in CONFIG["utilityNameKeywords"])


def load_universe():
    data = read_json(UNIVERSE_PATH, {})
    if isinstance(data, dict):
        return (data.get("data") if isinstance(data.get("data"), list) else []), (data.get("meta") or {})
    return (data if isinstance(data, list) else []), {}


# ----- 랭킹: approx -----

def calculate_approx_magic_ranking(rows, blacklist):
    excluded = {"financial": 0, "utilities": 0, "perInvalid": 0, "roeInvalid": 0,
                "marketCapBelow": 0, "blacklisted": 0, "missing": 0}
    eligible = []
    for r in rows:
        code = str(r.get("symbol") or r.get("code") or "").strip()
        if not code:
            excluded["missing"] += 1; continue
        if code in blacklist:
            excluded["blacklisted"] += 1; continue
        if is_finance(r):
            excluded["financial"] += 1; continue
        if is_utility(r):
            excluded["utilities"] += 1; continue
        per, roe = num(r.get("PER", r.get("per"))), num(r.get("ROE", r.get("roe")))
        mc, price = num(r.get("marketCap")), num(r.get(CONFIG["priceField"], r.get("price")))
        if per is None or roe is None or mc is None or price is None or price <= 0:
            excluded["missing"] += 1; continue
        if per <= 0:
            excluded["perInvalid"] += 1; continue
        if roe <= 0:
            excluded["roeInvalid"] += 1; continue
        if mc < CONFIG["minMarketCap"]:
            excluded["marketCapBelow"] += 1; continue
        eligible.append({"code": code, "name": str(r.get("corpName") or code),
                         "industryName": str(r.get("industryName") or ""), "marketCap": mc,
                         "price": price, "ROE": roe, "opMargin": num(r.get("opMargin")) or 0.0,
                         "PER": per, "PBR": num(r.get("PBR", r.get("pbr"))) or 0.0})
    for i, s in enumerate(sorted(eligible, key=lambda s: (-s["ROE"], -s["opMargin"], s["code"])), 1):
        s["profitabilityRank"] = i
    for i, s in enumerate(sorted(eligible, key=lambda s: (s["PER"], s["PBR"], s["code"])), 1):
        s["valueRank"] = i
    for s in eligible:
        s["combinedRank"] = s["profitabilityRank"] + s["valueRank"]
    final = sorted(eligible, key=lambda s: (s["combinedRank"], s["PER"], s["code"]))
    for i, s in enumerate(final, 1):
        s["rank"] = i
    return final, excluded


# ----- 랭킹: book_faithful_v1 -----

def calculate_book_faithful_magic_ranking(rows, blacklist):
    excluded = {"financial": 0, "utilities": 0, "ebitInvalid": 0, "evInvalid": 0,
                "capitalBaseInvalid": 0, "dartMissing": 0, "marketCapBelow": 0,
                "blacklisted": 0, "missing": 0, "approxFallback": 0}
    mc_to_won = CONFIG["marketCapToWon"]
    dart_attempted, dart_ok, eligible = 0, 0, []
    for r in rows:
        code = str(r.get("symbol") or r.get("code") or "").strip()
        if not code:
            excluded["missing"] += 1; continue
        if code in blacklist:
            excluded["blacklisted"] += 1; continue
        if is_finance(r):
            excluded["financial"] += 1; continue
        if is_utility(r):
            excluded["utilities"] += 1; continue
        mc = num(r.get("marketCap"))
        price = num(r.get(CONFIG["priceField"], r.get("price")))
        if mc is None or price is None or price <= 0:
            excluded["missing"] += 1; continue
        if mc < CONFIG["minMarketCap"]:
            excluded["marketCapBelow"] += 1; continue
        dart_attempted += 1
        m = build_book_faithful_metrics(r)
        if not m or any(m.get(k) is None for k in ("ebit", "cash", "currentAssets",
                                                   "currentLiabilities", "ppe", "totalLiabilities")):
            excluded["dartMissing"] += 1; continue
        dart_ok += 1
        ebit = m["ebit"]
        if ebit <= 0:
            excluded["ebitInvalid"] += 1; continue
        capital_base = m["currentAssets"] - m["currentLiabilities"] + m["ppe"]
        if capital_base <= 0:
            excluded["capitalBaseInvalid"] += 1; continue
        ev = mc * mc_to_won + m["totalLiabilities"] - m["cash"]
        if ev <= 0:
            excluded["evInvalid"] += 1; continue
        eligible.append({
            "code": code, "name": str(r.get("corpName") or code),
            "industryName": str(r.get("industryName") or ""), "marketCap": mc, "price": price,
            "EBIT": ebit, "cashAndCashEquivalents": m["cash"], "currentAssets": m["currentAssets"],
            "currentLiabilities": m["currentLiabilities"], "propertyPlantAndEquipment": m["ppe"],
            "totalLiabilities": m["totalLiabilities"], "capitalBase": capital_base,
            "enterpriseValue": ev, "returnOnCapital": round(ebit / capital_base, 6),
            "earningsYield": round(ebit / ev, 6), "evMethod": CONFIG["evMethod"],
            "dataSource": m["dataSource"],
        })
    for i, s in enumerate(sorted(eligible, key=lambda s: (-s["returnOnCapital"], s["code"])), 1):
        s["profitabilityRank"] = i
    for i, s in enumerate(sorted(eligible, key=lambda s: (-s["earningsYield"], s["code"])), 1):
        s["valueRank"] = i
    for s in eligible:
        s["combinedRank"] = s["valueRank"] + s["profitabilityRank"]
    final = sorted(eligible, key=lambda s: (s["combinedRank"], -s["earningsYield"], s["code"]))
    for i, s in enumerate(final, 1):
        s["rank"] = i
    coverage = round(dart_ok / dart_attempted, 4) if dart_attempted else 0.0
    return final, excluded, {"dartAttempted": dart_attempted, "dartOk": dart_ok, "dartCoverage": coverage}


def calculate_magic_formula_ranking(rows, mode, blacklist):
    if mode == "approx":
        final, excluded = calculate_approx_magic_ranking(rows, blacklist)
        return final, excluded, {"dartCoverage": None}
    return calculate_book_faithful_magic_ranking(rows, blacklist)


def book_rank_row(s, resolver=None):
    row = {k: s.get(k) for k in (
        "rank", "code", "name", "industryName", "profitabilityRank", "valueRank", "combinedRank",
        "EBIT", "enterpriseValue", "earningsYield", "returnOnCapital", "marketCap",
        "cashAndCashEquivalents", "totalLiabilities", "currentAssets", "currentLiabilities",
        "propertyPlantAndEquipment", "evMethod", "dataSource")}
    row["formulaVersion"] = CONFIG["formulaVersion"]
    if resolver is not None:
        bp, src = resolver(s["code"], s["price"])
        row["buyOpenPrice"] = bp
        row["priceSource"] = src
    return row


def approx_rank_row(s):
    return {k: s.get(k) for k in ("rank", "code", "name", "ROE", "PER", "opMargin", "PBR",
                                  "profitabilityRank", "valueRank", "combinedRank")}


def empty_portfolio():
    return {"fundName": CONFIG["fundName"], "initialCapital": CONFIG["initialCapital"],
            "cash": float(CONFIG["initialCapital"]), "tradingCalendar": [], "tradingDayIndex": -1,
            "openLots": [], "positionsByCode": [], "totalRealizedProfit": 0.0}


# ----- 43-B6 version lock / 로그 -----

def version_block(mode):
    fv = CONFIG["formulaVersion"] if mode != "approx" else "approx-roe-per-2026-43B3"
    return {"formulaMode": mode, "formulaVersion": fv,
            "formulaVersionLockedAt": CONFIG["formulaVersionLockedAt"],
            "tradingRuleVersion": CONFIG["tradingRuleVersion"], "evMethod": CONFIG["evMethod"],
            "marketCapUnitAssumption": CONFIG["marketCapUnitAssumption"]}


def load_or_seed_change_log():
    doc = read_json(CHANGE_LOG_PATH, None)
    if isinstance(doc, dict) and isinstance(doc.get("entries"), list):
        return doc
    seeded = {"fundName": CONFIG["fundName"], "updatedAt": now_text(), "entries": CHANGE_LOG_SEED}
    write_json(CHANGE_LOG_PATH, seeded)
    return seeded


def version_guard_warnings(change_log, mode):
    warnings = []
    fv = version_block(mode)["formulaVersion"]
    versions = {e.get("formulaVersion") for e in (change_log.get("entries") or [])}
    if CONFIG["formulaChangeRequiresVersionBump"] and fv not in versions:
        warnings.append(f"formulaVersion '{fv}'에 대한 change-log entry가 없음(공식수정 가드)")
    if CONFIG["liveOperationStarted"] and CONFIG["allowFormulaTuningWithoutVersionBump"]:
        warnings.append("liveOperationStarted=true인데 allowFormulaTuningWithoutVersionBump=true (버전업 없는 튜닝 위험)")
    return warnings


def append_operation_log(entry):
    doc = read_json(OPERATION_LOG_PATH, None)
    entries = doc.get("entries") if isinstance(doc, dict) else None
    entries = [e for e in (entries or []) if e.get("date") != entry["date"]]
    entries.append(entry)
    entries = entries[-800:]
    write_json(OPERATION_LOG_PATH, {"fundName": CONFIG["fundName"], "updatedAt": now_text(),
                                    "entryCount": len(entries), "entries": entries})
    return len(entries)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--asof", default=None)
    parser.add_argument("--fetch-open", action="store_true")
    parser.add_argument("--mode", default=CONFIG["formulaMode"], choices=["approx", "book_faithful_v1"])
    args = parser.parse_args()
    mode = args.mode

    reco = read_json(RECO_HISTORY_PATH, {}) or {}
    base_date = str(args.asof or reco.get("baseDate") or datetime.now().strftime("%Y-%m-%d"))[:10]
    holidays = read_json(HOLIDAYS_PATH, []) or []
    holidays = holidays if isinstance(holidays, list) else []
    blacklist_doc = read_json(BLACKLIST_PATH, []) or []
    blacklist = set(str(x).strip() for x in blacklist_doc) if isinstance(blacklist_doc, list) else set()
    market_open = is_market_open_day(base_date, holidays)
    weekend = is_weekend(base_date)

    change_log = load_or_seed_change_log()
    sanity_warnings = version_guard_warnings(change_log, mode)

    rows, meta = load_universe()
    universe_base = str((meta or {}).get("baseDate") or "")
    final, excluded, cov = calculate_magic_formula_ranking(rows, mode, blacklist)
    top10 = final[:CONFIG["topN"]]
    top100 = final[:CONFIG["rankTop100"]]
    universe_price = {str(r.get("symbol") or r.get("code")): num(r.get("price")) for r in rows}

    # sanity: EY/ROC 이상치
    if mode != "approx":
        ey_gt1 = sum(1 for s in final if s.get("earningsYield", 0) > 1.0)
        roc_out = sum(1 for s in final if s.get("returnOnCapital", 0) > 5.0)
        if ey_gt1:
            sanity_warnings.append(f"EarningsYield>1.0 종목 {ey_gt1}개(단위/데이터 점검)")
        if roc_out:
            sanity_warnings.append(f"ReturnOnCapital>5.0 자산경량 outlier {roc_out}개")
        if cov.get("dartCoverage") is not None and cov["dartCoverage"] < 0.9:
            sanity_warnings.append(f"dartCoverage 낮음({cov['dartCoverage']})")

    approx_top10 = []
    if mode != "approx":
        a_final, _ = calculate_approx_magic_ranking(rows, blacklist)
        approx_top10 = [approx_rank_row(s) for s in a_final[:CONFIG["topN"]]]

    open_cache = read_json(OPEN_CACHE_PATH, {}) or {}
    open_map = fetch_open_map_pykrx(base_date) if (args.fetch_open and market_open) else {}
    resolve_price = make_price_resolver(base_date, open_cache, open_map)

    if args.reset:
        portfolio, lots, realized, history = empty_portfolio(), [], [], []
    else:
        portfolio = read_json(PORTFOLIO_PATH, None) or empty_portfolio()
        lots = (read_json(LOTS_PATH, {}) or {}).get("lots") or []
        realized = (read_json(REALIZED_PATH, {}) or {}).get("realized") or []
        history = (read_json(ACTIONS_PATH, {}) or {}).get("history") or []

    calendar = portfolio.get("tradingCalendar") or []
    already = bool(calendar) and calendar[-1] == base_date
    sells_today, buys_today, realized_today, skipped_reason = [], [], [], None

    if not market_open:
        skipped_reason = "weekend" if weekend else "holiday"
    elif already:
        skipped_reason = "already_processed"
    else:
        calendar.append(base_date)
        portfolio["tradingCalendar"] = calendar
        cur_idx = len(calendar) - 1
        portfolio["tradingDayIndex"] = cur_idx
        for lot in lots:
            if lot.get("status") != "OPEN":
                continue
            if cur_idx - int(lot.get("buyTradingDayIndex", cur_idx)) < CONFIG["holdingTradingDaysTarget"]:
                continue
            code = lot["code"]
            sell_price, src = resolve_price(code, universe_price.get(code) or lot.get("buyOpenPrice"))
            qty = lot["quantity"]
            proceeds = round(sell_price * qty, 2)
            pnl = round(proceeds - lot["investedAmount"], 2)
            ret = round((pnl / lot["investedAmount"] * 100) if lot["investedAmount"] else 0.0, 2)
            held = cur_idx - int(lot.get("buyTradingDayIndex", cur_idx))
            lot.update({"status": "CLOSED", "sellDate": base_date, "sellTradingDayIndex": cur_idx,
                        "sellOpenPrice": sell_price, "sellPriceSource": src,
                        "realizedProfit": pnl, "realizedReturnRate": ret})
            portfolio["cash"] = round(portfolio["cash"] + proceeds, 2)
            portfolio["totalRealizedProfit"] = round(portfolio.get("totalRealizedProfit", 0.0) + pnl, 2)
            rec = {"date": base_date, "lotId": lot["lotId"], "code": code, "name": lot["name"],
                   "buyDate": lot["buyDate"], "sellDate": base_date, "buyOpenPrice": lot.get("buyOpenPrice"),
                   "sellOpenPrice": sell_price, "priceSource": src, "quantity": qty,
                   "investedAmount": lot["investedAmount"], "proceeds": proceeds, "realizedProfit": pnl,
                   "realizedReturnRate": ret, "holdingTradingDays": held, "rebuySameDay": False,
                   "formulaMode": lot.get("formulaMode"), "formulaVersion": lot.get("formulaVersion"),
                   "tradingRuleVersion": CONFIG["tradingRuleVersion"]}
            realized.append(rec)
            realized_today.append(rec)
            sells_today.append({k: rec[k] for k in ("date", "lotId", "code", "name", "buyDate",
                                                    "sellOpenPrice", "priceSource", "quantity",
                                                    "realizedProfit", "realizedReturnRate", "rebuySameDay")})
        planned_sell = add_trading_days(base_date, CONFIG["holdingTradingDaysTarget"], holidays)
        seq = 1
        for s in top10:
            buy_price, src = resolve_price(s["code"], s["price"])
            if not buy_price or buy_price <= 0:
                continue
            qty = int(math.floor(CONFIG["perStockTarget"] / buy_price)) or 0
            if qty < 1:
                qty = 1
            invested = round(buy_price * qty, 2)
            if portfolio["cash"] < invested:
                qty = int(math.floor(portfolio["cash"] / buy_price))
                if qty < 1:
                    continue
                invested = round(buy_price * qty, 2)
            lot = {"lotId": f"MF-{base_date}-{s['code']}-{seq:02d}", "status": "OPEN", "code": s["code"],
                   "name": s["name"], "rank": s["rank"], "buyDate": base_date, "buyTradingDayIndex": cur_idx,
                   "plannedSellTradingDayIndex": cur_idx + CONFIG["holdingTradingDaysTarget"],
                   "plannedSellDateEstimate": planned_sell, "holdingTradingDaysTarget": CONFIG["holdingTradingDaysTarget"],
                   "buyOpenPrice": buy_price, "priceSource": src, "quantity": qty, "investedAmount": invested,
                   "formulaMode": mode, "formulaVersion": version_block(mode)["formulaVersion"],
                   "tradingRuleVersion": CONFIG["tradingRuleVersion"]}
            seq += 1
            lots.append(lot)
            portfolio["cash"] = round(portfolio["cash"] - invested, 2)
            buys_today.append({k: lot[k] for k in ("lotId", "code", "name", "rank", "buyOpenPrice",
                                                   "priceSource", "quantity", "investedAmount",
                                                   "plannedSellTradingDayIndex", "plannedSellDateEstimate")})
            buys_today[-1]["date"] = base_date
        # rebuySameDay 표시(매도 종목이 당일 신규 매수에도 있으면)
        buy_codes = {b["code"] for b in buys_today}
        for rec in realized_today:
            if rec["code"] in buy_codes:
                rec["rebuySameDay"] = True
        for s in sells_today:
            if s["code"] in buy_codes:
                s["rebuySameDay"] = True

    # 집계
    open_lots = [l for l in lots if l.get("status") == "OPEN"]
    by_code = {}
    for l in open_lots:
        agg = by_code.setdefault(l["code"], {"code": l["code"], "name": l["name"], "lotCount": 0,
                                             "totalQuantity": 0, "totalInvested": 0.0})
        agg["lotCount"] += 1
        agg["totalQuantity"] += l["quantity"]
        agg["totalInvested"] = round(agg["totalInvested"] + l["investedAmount"], 2)
    positions, total_eval = [], 0.0
    for code, agg in by_code.items():
        agg["avgBuyPrice"] = round(agg["totalInvested"] / agg["totalQuantity"], 2) if agg["totalQuantity"] else 0
        cur = universe_price.get(code)
        if cur is not None:
            ev = round(cur * agg["totalQuantity"], 2)
            agg.update({"currentPrice": cur, "evaluationAmount": ev,
                        "evalProfit": round(ev - agg["totalInvested"], 2)})
            total_eval += ev
        positions.append(agg)
    invested_amount = round(sum(a["totalInvested"] for a in by_code.values()), 2)
    unrealized = round(total_eval - invested_amount, 2)
    portfolio["openLots"] = [l["lotId"] for l in open_lots]
    portfolio["openLotCount"] = len(open_lots)
    portfolio["positionsByCode"] = sorted(positions, key=lambda p: -p["totalInvested"])
    portfolio["investedAmount"] = invested_amount
    portfolio["evaluationAmount"] = round(total_eval, 2)
    portfolio["unrealizedProfit"] = unrealized
    portfolio["totalAssetApprox"] = round(portfolio["cash"] + total_eval, 2)
    portfolio["baseDate"] = base_date
    portfolio["marketOpen"] = market_open
    portfolio["updatedAt"] = now_text()
    portfolio["formula"] = version_block(mode)
    portfolio["config"] = {k: CONFIG[k] for k in ("initialCapital", "dailyBudget", "topN",
                                                  "perStockTarget", "holdingTradingDaysTarget", "minMarketCap")}
    portfolio.setdefault("tradingCalendar", calendar)
    portfolio["tradingDayIndex"] = len(portfolio.get("tradingCalendar") or []) - 1

    executed_buy = round(sum(b["investedAmount"] for b in buys_today), 2)
    executed_sell = round(sum(r["proceeds"] for r in realized_today), 2)
    if executed_buy > CONFIG["dailyBudget"]:
        sanity_warnings.append(f"당일 매수 {executed_buy} > dailyBudget {CONFIG['dailyBudget']}(고가주 1주 허용, 규칙상 정상)")

    today_action = {"baseDate": base_date, "marketOpen": market_open, "marketClosed": not market_open,
                    "skippedReason": skipped_reason, **version_block(mode),
                    "tradingDayIndex": portfolio["tradingDayIndex"], "dailyBudget": CONFIG["dailyBudget"],
                    "perStockTarget": CONFIG["perStockTarget"], "buyCount": len(buys_today),
                    "sellCount": len(sells_today), "buyInvestedTotal": executed_buy,
                    "todayMagicBuyList": buys_today, "todaySellList": sells_today}
    if market_open and not already:
        history = (history + [today_action])[-90:]

    # 출력 파일
    write_json(RANKINGS_PATH, {
        "fundName": CONFIG["fundName"], "baseDate": base_date, "marketOpen": market_open,
        "universeBaseDate": universe_base, "generatedAt": now_text(), **version_block(mode),
        "formulaInputs": CONFIG["formulaInputs"], "universeCount": len(rows), "eligibleCount": len(final),
        "dartCoverage": cov.get("dartCoverage"), "dartStats": cov, "excludedCounts": excluded,
        "sanityWarnings": sanity_warnings,
        "todayMagicRankingTop10": [book_rank_row(s, resolve_price) if mode != "approx" else approx_rank_row(s) for s in top10],
        "top100": [book_rank_row(s) if mode != "approx" else approx_rank_row(s) for s in top100],
        "approxComparisonTop10": approx_top10})
    write_json(LOTS_PATH, {"fundName": CONFIG["fundName"], "baseDate": base_date,
                           "updatedAt": now_text(), **version_block(mode), "lots": lots})
    write_json(REALIZED_PATH, {"fundName": CONFIG["fundName"], "updatedAt": now_text(), "realized": realized})
    write_json(ACTIONS_PATH, {"fundName": CONFIG["fundName"], "updatedAt": now_text(),
                              "today": today_action, "history": history})
    write_json(PORTFOLIO_PATH, portfolio)

    # operation log (운영자용, 날짜 dedupe append)
    op_entry = {
        "date": base_date, **version_block(mode), "rankingGeneratedAt": now_text(),
        "marketOpen": market_open, "skippedReason": skipped_reason, "universeCount": len(rows),
        "eligibleCount": len(final), "excludedCounts": excluded,
        "top10Ranking": [{"rank": s.get("rank"), "code": s.get("code"), "name": s.get("name"),
                          "formulaVersion": version_block(mode)["formulaVersion"],
                          "profitabilityRank": s.get("profitabilityRank"), "valueRank": s.get("valueRank"),
                          "combinedRank": s.get("combinedRank"), "returnOnCapital": s.get("returnOnCapital"),
                          "earningsYield": s.get("earningsYield"), "EBIT": s.get("EBIT"),
                          "enterpriseValue": s.get("enterpriseValue")} for s in top10],
        "actualBuyList": buys_today, "actualSellList": sells_today, "cash": portfolio["cash"],
        "executedBuyAmount": executed_buy, "executedSellAmount": executed_sell,
        "portfolioSnapshot": {"totalAsset": portfolio["totalAssetApprox"], "cash": portfolio["cash"],
                              "investedAmount": invested_amount, "evaluationAmount": portfolio["evaluationAmount"],
                              "unrealizedProfit": unrealized, "realizedProfit": portfolio.get("totalRealizedProfit", 0.0),
                              "holdingCount": len(by_code), "openLotCount": portfolio["openLotCount"]},
        "openLotCount": portfolio["openLotCount"], "realizedTradeCount": len(realized),
        "dataSourceSummary": {"dartCoverage": cov.get("dartCoverage"),
                              "buyPriceSources": sorted({b["priceSource"] for b in buys_today})},
        "sanityWarnings": sanity_warnings}
    op_count = append_operation_log(op_entry)

    # 콘솔
    print(f"[마법공식] mode={mode} ver={version_block(mode)['formulaVersion']} baseDate={base_date} "
          f"marketOpen={market_open} skipped={skipped_reason} reset={args.reset}")
    print(f"  universe={len(rows)} eligible={len(final)} dartCoverage={cov.get('dartCoverage')}")
    print(f"  excluded={excluded}")
    print(f"  sanityWarnings={sanity_warnings}")
    print("  todayMagicRankingTop10:")
    for s in top10:
        if mode == "approx":
            print(f"    #{s['rank']:>2} {s['code']} {s['name']} | ROE {s['ROE']} PER {s['PER']} comb {s['combinedRank']}")
        else:
            print(f"    #{s['rank']:>2} {s['code']} {s['name']} | ROC {s['returnOnCapital']} EY {s['earningsYield']} comb {s['combinedRank']}")
    print(f"  buys={len(buys_today)} sells={len(sells_today)} cash={portfolio['cash']} "
          f"openLots={portfolio['openLotCount']}")
    print(f"  operationLog entries={op_count} changeLog entries={len(change_log.get('entries') or [])}")


if __name__ == "__main__":
    main()
