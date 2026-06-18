#!/usr/bin/env python3
"""와바바 마법공식 — 50개 매수배치 롤링 엔진 (Phase 45-E2 / 45-E2.1).

목적
----
공식 운용 규칙을 *순수 로직*으로 구현한다.
- 매 개장일 상위10을 1개 매수배치(batch)로 매수(종목별 itemLot 10개).
- 초기 1~50배치: 배치당 allocatedCapital = 1,000,000원(officialAvailableCash에서 차감).
- 51번째 거래일부터: 가장 오래된 open batch FIFO 전량 매도 → 매도대금 + 그 배치 cashReserve를
  rolloverBudget로 신규 교체배치에 *독립 복리* 배정(다른 배치/전역 현금 미혼합).

안전 설계
---------
- 운영 STEP 2.5(build_magic_formula_fund.py)를 *대체/수정하지 않는다*(별도 모듈).
- 코어 함수는 파일 I/O / pykrx / 네트워크를 *전혀* 쓰지 않는다(전부 인자 주입).
- 매수/매도 executionPrice는 pykrx_open만 허용. 시가 누락 → 전체 BLOCKED(부분매수·fallback 금지).
- 평가가(eval price)는 거래가와 source 분리. 평가 실패가 매수 원장을 바꾸지 않는다.
- 51번째 교체는 *원자적*: 신규 매수가 BLOCKED될 수 있으면 기존 배치 매도도 0(state 불변).
- append-only / 결정적 batchId·tradeId·수량 → 재실행 idempotent.
- PILOT(2026-06-08) 데이터는 official 자금/시퀀스/카운트에서 분리.
- 수수료·세금 미반영(fee/tax = 0 가정). 존재하지 않는 수수료 계산을 임의 생성하지 않는다.
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Optional

# ----- 상수 -----
OFFICIAL = "OFFICIAL"
PILOT = "PILOT"

HOLD_TRADING_DAYS = 50
MAX_OPEN_BATCHES = 50
TOP_N = 10
INITIAL_CAPITAL = 50_000_000
INITIAL_BATCH_CAPITAL = 1_000_000          # 5천만 ÷ 50 = 배치당 100만원
PRICE_SOURCE_TRADE = "pykrx_open"
EPS = 1e-6
FEE_TAX_MODELED = False                     # 수수료/세금 미반영(0 가정)

# runStatus
COMPLETED = "COMPLETED"
NON_TRADING_DAY = "NON_TRADING_DAY"
ALREADY_PROCESSED = "ALREADY_PROCESSED"
BLOCKED_MISSING_OPEN_PRICE = "BLOCKED_MISSING_OPEN_PRICE"
BLOCKED_MISSING_RANKING = "BLOCKED_MISSING_RANKING"
BLOCKED_NO_TRADING_CALENDAR = "BLOCKED_NO_TRADING_CALENDAR"
BLOCKED_INSUFFICIENT_AVAILABLE_CASH = "BLOCKED_INSUFFICIENT_AVAILABLE_CASH"  # 초기배치: officialAvailableCash < 100만
BLOCKED_INSUFFICIENT_BATCH_BUDGET = "BLOCKED_INSUFFICIENT_BATCH_BUDGET"      # 10종목 최소1주 > allocatedCapital
BLOCKED_MULTIPLE_OVERDUE_BATCHES = "BLOCKED_MULTIPLE_OVERDUE_BATCHES"        # 45-E10: due batch 2개 이상 → 자동 일괄매도 금지
MISSED_RUN = "MISSED_RUN"
MISSED_RUN_NO_PREOPEN_SIGNAL = "NO_PREOPEN_SIGNAL_PACKAGE"
PILOT_RUN = "PILOT"

SELL_REASON_ROLLOVER = "FIFTY_BATCH_FIFO_ROLLOVER"
RANK_FIELDS = ("rank", "combinedRank", "profitabilityRank", "valueRank", "returnOnCapital", "earningsYield")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ----- 거래일 캘린더 (신뢰 가능한 KRX 거래일 set 주입; 평일 추정 금지) -----

def make_calendar(trading_days, range_start: Optional[str] = None, range_end: Optional[str] = None):
    if trading_days is None:
        return None
    tds = sorted(set(str(d)[:10] for d in trading_days))
    return {"tradingDays": frozenset(tds),
            "rangeStart": range_start or (tds[0] if tds else None),
            "rangeEnd": range_end or (tds[-1] if tds else None)}


def classify_trading_day(date: str, calendar: Optional[dict]) -> str:
    """'TRADING' / 'NON_TRADING' / 'NO_CALENDAR'."""
    if not calendar or not calendar.get("tradingDays"):
        return "NO_CALENDAR"
    rs, re = calendar.get("rangeStart"), calendar.get("rangeEnd")
    if rs and re and not (rs <= date <= re):
        return "NO_CALENDAR"
    return "TRADING" if date in calendar["tradingDays"] else "NON_TRADING"


# ----- 상태 -----

def empty_official_state(initial_capital: int = INITIAL_CAPITAL,
                         initial_batch_capital: int = INITIAL_BATCH_CAPITAL,
                         official_start_date: Optional[str] = None) -> dict:
    return {
        "operationMode": OFFICIAL,
        "officialStartDate": official_start_date,
        "initialCapital": int(initial_capital),
        "initialBatchCapital": int(initial_batch_capital),
        "officialAvailableCash": float(initial_capital),   # 아직 생성 안 된 초기배치용 자본
        "officialTradingCalendar": [],                      # [legacy] 성공 batch 날짜(=officialExecutionCalendar와 동일). 보존만.
        # 45-E10: 성공 batch 순번(officialSequence)과 실제 KRX 거래일 순번(officialTradingDayIndex) 분리.
        "officialTradingDayIndex": 0,                       # officialStartDate 이후 실제 KRX 거래일 순번(COMPLETED+MISSED_RUN 증가)
        "officialKrxTradingCalendar": [],                   # 실제 KRX 거래일 전체(누락일 포함)
        "officialExecutionCalendar": [],                    # 성공적으로 batch 생성한 날짜만
        "officialSequence": 0,                              # 성공 batch 순번(COMPLETED만 증가)
        "batches": [],
        "itemLots": [],
        "buyLedger": [],        # = 거래원장 BUY
        "sellLedger": [],       # = 거래원장 SELL
        "dailyLedger": [],
        "evalSnapshots": [],    # = evaluationSnapshots
        "missedRuns": [],
        "pilot": None,
        "prevTotalAsset": float(initial_capital),
    }


def _open_lots(state: dict, batch_id: Optional[str] = None) -> list:
    return [l for l in state["itemLots"]
            if l.get("status") == "OPEN" and (batch_id is None or l.get("batchId") == batch_id)]


def _official_open_batches(state: dict) -> list:
    return sorted([b for b in state["batches"]
                   if b.get("operationMode") == OFFICIAL and b.get("status") == "OPEN"],
                  key=lambda b: b.get("sequence", 0))


def due_official_batches(state: dict, current_trading_day_index: int) -> list:
    """45-E10: 매도 예정 거래일 index가 현재 거래일 index 이하인 open 공식 batch(FIFO=오래된 순).
    plannedSellTradingDayIndex 누락 batch는 buyTradingDayIndex(또는 sequence) + HOLD_TRADING_DAYS로 보정."""
    out = []
    for b in _official_open_batches(state):
        psi = b.get("plannedSellTradingDayIndex")
        if psi is None:
            base = b.get("buyTradingDayIndex", b.get("sequence", 0))
            psi = base + HOLD_TRADING_DAYS
        if psi <= current_trading_day_index:
            out.append(b)
    return sorted(out, key=lambda b: (b.get("buyTradingDayIndex", b.get("sequence", 0)), b.get("sequence", 0)))


def fund_cash(state: dict) -> float:
    """미배정 초기자본 + open 공식배치들의 cashReserve 합. (PILOT/CLOSED 제외)"""
    reserves = sum(b.get("cashReserve", 0.0) for b in state["batches"]
                   if b.get("operationMode") == OFFICIAL and b.get("status") == "OPEN")
    return round(state["officialAvailableCash"] + reserves, 2)


# ----- 정수 수량 배분(결정적) -----

def allocate_quantities(top10: list, open_prices: dict, allocated_capital: float):
    """rule 4: targetPerStock=allocated/10, qty=max(1,floor(target/open)).
    총액이 allocated 초과 시 qty>1 종목 중 '목표 대비 초과액 큰 종목'부터 결정적으로 1주씩 감소(최소 1주 유지).
    모두 최소 1주인데도 초과면 (None, total) 반환 → BLOCKED_INSUFFICIENT_BATCH_BUDGET.
    동일 입력 → 동일 결과(결정성)."""
    target = allocated_capital / TOP_N
    qty = {}
    for r in top10:
        op = float(open_prices[r["code"]])
        qty[r["code"]] = max(1, int(target // op))

    def total():
        return round(sum(qty[c] * float(open_prices[c]) for c in qty), 2)

    t = total()
    guard = 0
    while t > allocated_capital + EPS:
        cands = [c for c in qty if qty[c] > 1]
        if not cands:
            return None, t  # 최소 1주씩도 초과
        cands.sort(key=lambda c: (-(qty[c] * float(open_prices[c]) - target), c))
        qty[cands[0]] -= 1
        t = total()
        guard += 1
        if guard > 100000:  # 안전 가드(이론상 도달 불가)
            return None, t
    return qty, t


# ----- 평가(거래가와 분리) -----

def evaluate(state: dict, date: str, eval_prices: dict, eval_source: str = "official_close") -> dict:
    eval_prices = eval_prices or {}
    market_value, missing, per_lot = 0.0, [], []
    for l in _open_lots(state):
        cur = eval_prices.get(l["code"])
        if cur is None or cur <= 0:
            missing.append(l["code"])
            mv, cur_out = 0.0, None
        else:
            mv = round(cur * l["quantity"], 2)
            cur_out = cur
            market_value += mv
        per_lot.append({
            "lotId": l["lotId"], "batchId": l["batchId"], "code": l["code"],
            "buyPrice": l["buyOpenPrice"], "currentPrice": cur_out, "quantity": l["quantity"],
            "marketValue": mv,
            "unrealizedPnL": round(mv - l["investedAmount"], 2) if cur_out is not None else None,
            "returnRate": round((mv - l["investedAmount"]) / l["investedAmount"] * 100, 2)
                          if (cur_out is not None and l["investedAmount"]) else None,
            "holdingTradingDays": state["officialSequence"] - l["buySequence"],
        })
    cash = fund_cash(state)
    total_asset = round(cash + market_value, 2)
    init = state["initialCapital"] or 1
    return {
        "date": date, "evalPriceSource": eval_source,
        "holdingsMarketValue": round(market_value, 2), "cash": cash, "totalAsset": total_asset,
        "cumulativeReturn": round((total_asset - state["initialCapital"]) / init * 100, 2),
        "missingEvalCodes": sorted(set(missing)), "perLot": per_lot,
    }


def _daily_ledger(state, date, market_open, run_status, run_reason, *, eval_info=None,
                  buy_batch_id=None, sell_batch_id=None, buy_count=0, sell_count=0, now=None) -> dict:
    eval_info = eval_info or evaluate(state, date, {})
    total_asset = eval_info["totalAsset"]
    prev = state.get("prevTotalAsset") or state["initialCapital"]
    daily_ret = round((total_asset - prev) / prev * 100, 2) if prev else 0.0
    return {
        "date": date, "marketOpen": market_open, "operationMode": OFFICIAL,
        "runStatus": run_status, "runReason": run_reason,
        "officialSequence": state["officialSequence"],
        "openBatchCount": len(_official_open_batches(state)),
        "openItemLotCount": len(_open_lots(state)),
        "closedBatchCount": len([b for b in state["batches"] if b.get("status") == "CLOSED"]),
        "buyBatchId": buy_batch_id, "sellBatchId": sell_batch_id,
        "buyCount": buy_count, "sellCount": sell_count,
        "cash": eval_info["cash"], "holdingsMarketValue": eval_info["holdingsMarketValue"],
        "totalAsset": total_asset, "dailyReturn": daily_ret,
        "cumulativeReturn": eval_info["cumulativeReturn"], "createdAt": now or _now(),
    }


def _blocked(date, status, reason, now, extra=None) -> dict:
    res = {"date": date, "marketOpen": True, "operationMode": OFFICIAL, "runStatus": status,
           "runReason": reason, "buyBatchId": None, "sellBatchId": None, "buyCount": 0,
           "sellCount": 0, "createdAt": now or _now()}
    if extra:
        res.update(extra)
    return res


def plan_official_day(state: dict, date: str, ranking, open_prices: dict, eval_prices: dict,
                      calendar: Optional[dict], now: Optional[str] = None,
                      timing: Optional[dict] = None, trading_day_index: Optional[int] = None):
    """순수 함수. (새 state, day_result) 반환. 입력 state 불변(deepcopy). 파일 I/O 0.

    timing(선택): 신호일·체결일 분리 메타데이터를 batch/buyLedger/dailyLedger에 *추가 기록만* 한다.
      look-ahead 검증은 wrapper가 끝낸 뒤 통과한 값만 주입한다(코어는 검증/계산하지 않음).
      None이면 기존 동작과 100% 동일(하위호환). 기대 키:
        signalAsOfDate, rankingGeneratedAt, executionDate, executionMarketOpenAt,
        executionPriceSource, lookAheadValidationPassed
    date 인자는 *체결일(executionDate)* 이다."""
    st = copy.deepcopy(state)
    now = now or _now()
    open_prices = open_prices or {}
    eval_prices = eval_prices or {}

    # 1) idempotency
    existing = next((d for d in st["dailyLedger"] if d.get("date") == date), None)
    if existing is not None:
        res = dict(existing)
        res["runStatus"] = ALREADY_PROCESSED
        res["runReason"] = "duplicate run for date (idempotent)"
        return st, res

    # 2) 거래일(신뢰 캘린더 필수)
    cls = classify_trading_day(date, calendar)
    if cls == "NO_CALENDAR":
        return st, _blocked(date, BLOCKED_NO_TRADING_CALENDAR, "no reliable KRX trading-day calendar", now)
    if cls == "NON_TRADING":
        ev = evaluate(st, date, eval_prices)
        led = _daily_ledger(st, date, False, NON_TRADING_DAY, "weekend/holiday", eval_info=ev, now=now)
        st["dailyLedger"].append(led)
        st["evalSnapshots"].append(ev)
        st["prevTotalAsset"] = ev["totalAsset"]
        return st, led

    # 3) ranking
    if not ranking or len(ranking) < TOP_N:
        return st, _blocked(date, BLOCKED_MISSING_RANKING, "ranking missing or <10", now)
    top10 = ranking[:TOP_N]

    # 4) 교체 여부 — 45-E10: 실제 KRX 거래일 index 기준(open batch 수/officialSequence 기준 아님).
    #    현재 거래일 index = 주입값 또는 (직전 index+1). 누락일은 외부 recorder가 index를 올려둠.
    current_index = trading_day_index if trading_day_index is not None else st.get("officialTradingDayIndex", 0) + 1
    due = due_official_batches(st, current_index)
    if len(due) >= 2:   # 2개 이상 overdue → 자동 일괄매도 금지(별도 복구 승인)
        return st, _blocked(date, BLOCKED_MULTIPLE_OVERDUE_BATCHES,
                            f"{len(due)} batches overdue at tradingDayIndex {current_index}: "
                            f"{[b['batchId'] for b in due]}", now)
    is_rollover = len(due) == 1
    sell_batch = due[0] if is_rollover else None
    sell_lots = _open_lots(st, sell_batch["batchId"]) if sell_batch else []

    # 5) 실제 시가 완전성(신규 top10 + 매도배치 종목 전부). 하나라도 없으면 전체 BLOCKED(매도도 0).
    need_codes = [r["code"] for r in top10] + [l["code"] for l in sell_lots]
    missing = [c for c in need_codes if not open_prices.get(c) or open_prices[c] <= 0]
    if missing:
        return st, _blocked(date, BLOCKED_MISSING_OPEN_PRICE, f"missing pykrx_open: {sorted(set(missing))}", now)

    # 6) allocatedCapital 결정
    proceeds = 0.0
    rollover_budget = None
    if is_rollover:
        proceeds = round(sum(float(open_prices[l["code"]]) * l["quantity"] for l in sell_lots), 2)
        rollover_budget = round(proceeds + float(sell_batch.get("cashReserve", 0.0)), 2)
        allocated = rollover_budget
    else:
        if st["officialAvailableCash"] + EPS < st["initialBatchCapital"]:
            return st, _blocked(date, BLOCKED_INSUFFICIENT_AVAILABLE_CASH,
                                f"officialAvailableCash {st['officialAvailableCash']} < {st['initialBatchCapital']}", now)
        allocated = float(st["initialBatchCapital"])

    # 7) 정수 수량 배분(allocated 내). 최소1주 합도 초과면 BLOCKED(원자성: 매도 0).
    qty_map, total_invested = allocate_quantities(top10, open_prices, allocated)
    if qty_map is None:
        return st, _blocked(date, BLOCKED_INSUFFICIENT_BATCH_BUDGET,
                            f"min 1-share total {total_invested} > allocatedCapital {round(allocated,2)}", now,
                            extra={"allocatedCapital": round(allocated, 2),
                                   "rolloverBudget": rollover_budget})

    cash_reserve = round(allocated - total_invested, 2)

    # === COMPLETED: 원자적 적용 (매도+매수 한 번에) ===
    st["officialTradingCalendar"].append(date)                     # [legacy] 보존
    if date not in st.setdefault("officialKrxTradingCalendar", []):
        st["officialKrxTradingCalendar"].append(date)              # 실제 거래일(성공)
    st.setdefault("officialExecutionCalendar", []).append(date)    # 성공 batch 날짜
    st["officialTradingDayIndex"] = current_index                  # 실제 거래일 index 갱신
    seq = st["officialSequence"] + 1
    st["officialSequence"] = seq
    if st["officialStartDate"] is None:
        st["officialStartDate"] = date

    sell_count, sell_batch_id = 0, None
    if is_rollover and sell_batch and sell_lots:
        sell_batch_id = sell_batch["batchId"]
        for l in sell_lots:
            sp = float(open_prices[l["code"]])
            amt = round(sp * l["quantity"], 2)
            pnl = round(amt - l["investedAmount"], 2)
            ret = round(pnl / l["investedAmount"] * 100, 2) if l["investedAmount"] else 0.0
            held = seq - l["buySequence"]
            l["status"] = "CLOSED"
            l["sellDate"] = date
            l["sellOpenPrice"] = sp
            st["sellLedger"].append({
                "tradeId": f"SELL-{date}-{l['code']}-{l['lotId']}", "date": date,
                "batchId": sell_batch_id, "lotId": l["lotId"], "code": l["code"], "name": l.get("name"),
                "side": "SELL", "executionPrice": sp, "quantity": l["quantity"], "amount": amt,
                "realizedProfit": pnl, "realizedReturn": ret, "holdingTradingDays": held,
                "sellReason": SELL_REASON_ROLLOVER, "priceSource": PRICE_SOURCE_TRADE,
                "feeTaxModeled": FEE_TAX_MODELED,
            })
            sell_count += 1
        # 매도배치 cashReserve는 rolloverBudget로 소진됨 → open 집계에서 제외(상태 CLOSED)
        sell_batch["status"] = "CLOSED"
        sell_batch["closedDate"] = date
        sell_batch["rolledIntoSequence"] = seq
    elif not is_rollover:
        st["officialAvailableCash"] = round(st["officialAvailableCash"] - st["initialBatchCapital"], 2)

    # 매수(신규 배치 1개 + lot 10개)
    batch_id = f"MF-BATCH-{date}"
    lot_ids, buy_count = [], 0
    plan = []
    for i, r in enumerate(top10, 1):
        op = float(open_prices[r["code"]])
        q = qty_map[r["code"]]
        inv = round(op * q, 2)
        lot_id = f"{batch_id}-{r['code']}-{i:02d}"
        rank_snap = {k: r.get(k) for k in RANK_FIELDS}
        st["itemLots"].append({
            "lotId": lot_id, "batchId": batch_id, "code": r["code"], "name": r.get("name"),
            "buyDate": date, "buyOpenPrice": op, "quantity": q, "investedAmount": inv,
            "rankSnapshot": rank_snap, "buySequence": seq, "status": "OPEN", "priceSource": PRICE_SOURCE_TRADE,
            "buyTradingDayIndex": current_index,
            "plannedSellTradingDayIndex": current_index + HOLD_TRADING_DAYS,
        })
        buy_entry = {
            "tradeId": f"BUY-{date}-{r['code']}-{i:02d}", "date": date, "batchId": batch_id,
            "lotId": lot_id, "code": r["code"], "name": r.get("name"), "side": "BUY",
            "executionPrice": op, "quantity": q, "amount": inv, "rankSnapshot": rank_snap,
            "priceSource": PRICE_SOURCE_TRADE,
        }
        if timing:
            buy_entry.update({
                "signalAsOfDate": timing.get("signalAsOfDate"),
                "rankingGeneratedAt": timing.get("rankingGeneratedAt"),
                "executionDate": timing.get("executionDate", date),
                "executionPriceSource": timing.get("executionPriceSource", PRICE_SOURCE_TRADE),
            })
        st["buyLedger"].append(buy_entry)
        plan.append({"code": r["code"], "name": r.get("name"), "openPrice": op, "quantity": q, "amount": inv})
        lot_ids.append(lot_id)
        buy_count += 1

    new_batch = {
        "batchId": batch_id, "operationMode": OFFICIAL, "sequence": seq, "buyDate": date,
        "status": "OPEN", "itemLotIds": lot_ids, "buyCount": buy_count,
        "allocatedCapital": round(allocated, 2), "totalInvested": round(total_invested, 2),
        "cashReserve": cash_reserve,
        "rolloverSourceBatchId": sell_batch_id, "rolloverSaleProceeds": (proceeds if is_rollover else None),
        "rolloverBudget": rollover_budget,
        "plannedSellSequence": seq + HOLD_TRADING_DAYS,   # [legacy] 보존(매도 판정엔 미사용)
        "buyTradingDayIndex": current_index,
        "plannedSellTradingDayIndex": current_index + HOLD_TRADING_DAYS,
        "closedDate": None, "createdAt": now,
    }
    if timing:
        new_batch.update({
            "signalAsOfDate": timing.get("signalAsOfDate"),
            "rankingGeneratedAt": timing.get("rankingGeneratedAt"),
            "executionDate": timing.get("executionDate", date),
            "executionMarketOpenAt": timing.get("executionMarketOpenAt"),
            "executionPriceSource": timing.get("executionPriceSource", PRICE_SOURCE_TRADE),
        })
    st["batches"].append(new_batch)

    ev = evaluate(st, date, eval_prices)
    led = _daily_ledger(st, date, True, COMPLETED, "official rolling executed", eval_info=ev,
                        buy_batch_id=batch_id, sell_batch_id=sell_batch_id,
                        buy_count=buy_count, sell_count=sell_count, now=now)
    led.update({"allocatedCapital": round(allocated, 2), "totalInvested": round(total_invested, 2),
                "cashReserve": cash_reserve, "rolloverBudget": rollover_budget,
                "rolloverSaleProceeds": (proceeds if is_rollover else None), "plan": plan})
    if timing:
        led.update({
            "signalAsOfDate": timing.get("signalAsOfDate"),
            "executionDate": timing.get("executionDate", date),
            "rankingGeneratedAt": timing.get("rankingGeneratedAt"),
            "lookAheadValidationPassed": timing.get("lookAheadValidationPassed"),
        })
    st["dailyLedger"].append(led)
    st["evalSnapshots"].append(ev)
    st["prevTotalAsset"] = ev["totalAsset"]
    return st, led


# ----- PILOT(2026-06-08) 마이그레이션 계획 (입력 불변) -----

def build_pilot_batch(existing_item_lots: list, buy_date: str = "2026-06-08") -> dict:
    lots = [l for l in existing_item_lots if str(l.get("buyDate")) == buy_date]
    total = round(sum(float(l.get("investedAmount") or 0) for l in lots), 2)
    return {
        "batchId": f"MF-PILOT-{buy_date}", "operationMode": PILOT, "sequence": None, "buyDate": buy_date,
        "status": "OPEN", "itemLotIds": [l.get("lotId") for l in lots], "buyCount": len(lots),
        "allocatedCapital": None, "totalInvested": total, "cashReserve": None,
        "note": "pilot run; excluded from official capital/sequence/availableCash/rolling",
    }


def record_missed_run(date: str, reason: str = "DAILY_PIPELINE_NOT_EXECUTED") -> dict:
    return {"date": date, "status": MISSED_RUN, "reason": reason, "syntheticTradesCreated": False}


# ----- 45-E10: 거래일 index 마이그레이션 + MISSED_RUN 적용(순수 함수, 입력 불변) -----

def migrate_official_state_indices(state: dict) -> dict:
    """기존 canonical을 거래일 index 모델로 *additive* 마이그레이션(idempotent). 입력 불변(deepcopy 반환).
    - officialTradingDayIndex / officialKrxTradingCalendar / officialExecutionCalendar 보장.
    - 누락 시에만 officialTradingCalendar(=성공일)에서 backfill(누락일 없던 과거이므로 index==sequence).
    - batch/itemLot에 buyTradingDayIndex / plannedSellTradingDayIndex 보강(없을 때만)."""
    st = copy.deepcopy(state)
    exec_cal = list(st.get("officialExecutionCalendar") or [])
    if not exec_cal:
        exec_cal = list(st.get("officialTradingCalendar") or [])
        st["officialExecutionCalendar"] = exec_cal
    krx_cal = list(st.get("officialKrxTradingCalendar") or [])
    if not krx_cal:
        # 마이그레이션 시점엔 누락일이 아직 없으므로 KRX 거래일 = 성공일.
        krx_cal = list(st.get("officialTradingCalendar") or [])
        st["officialKrxTradingCalendar"] = krx_cal
    if st.get("officialTradingDayIndex") in (None, 0) and krx_cal:
        st["officialTradingDayIndex"] = len(krx_cal)
    elif "officialTradingDayIndex" not in st:
        st["officialTradingDayIndex"] = len(krx_cal)

    for b in st.get("batches", []):
        if b.get("operationMode") != OFFICIAL:
            continue
        if b.get("buyTradingDayIndex") is None:
            b["buyTradingDayIndex"] = b.get("sequence", 0)
        if b.get("plannedSellTradingDayIndex") is None:
            b["plannedSellTradingDayIndex"] = b["buyTradingDayIndex"] + HOLD_TRADING_DAYS
    for l in st.get("itemLots", []):
        if l.get("buyTradingDayIndex") is None:
            l["buyTradingDayIndex"] = l.get("buySequence", 0)
        if l.get("plannedSellTradingDayIndex") is None:
            l["plannedSellTradingDayIndex"] = l["buyTradingDayIndex"] + HOLD_TRADING_DAYS
    return st


def apply_missed_run(state: dict, date: str, reason: str = MISSED_RUN_NO_PREOPEN_SIGNAL,
                     now: Optional[str] = None):
    """MISSED_RUN을 append-only로 기록. 거래/배치/자금/원장/평가 변경 0. (새 state, entry, already) 반환.
    officialTradingDayIndex만 +1(실제 거래일 경과), officialSequence·officialExecutionCalendar 불변.
    idempotent: 같은 날짜가 이미 missedRuns에 있으면 변경 없이 already=True."""
    st = migrate_official_state_indices(state)
    now = now or _now()
    existing = next((m for m in st.get("missedRuns", []) if m.get("date") == date), None)
    if existing is not None:
        return st, existing, True
    seq_before = st["officialSequence"]
    idx_before = st["officialTradingDayIndex"]
    if date not in st["officialKrxTradingCalendar"]:
        st["officialKrxTradingCalendar"].append(date)
    st["officialTradingDayIndex"] = idx_before + 1
    entry = {
        "date": date, "status": MISSED_RUN, "reason": reason,
        "officialTradingDayIndex": st["officialTradingDayIndex"],
        "officialSequence": seq_before,
        "syntheticTradesCreated": False, "buyCount": 0, "sellCount": 0,
        "batchCreated": False, "executionPriceUsed": False,
        "signalPackagePresentBeforeOpen": False, "lookAheadTradePrevented": True,
        "createdAt": now,
    }
    st["missedRuns"].append(entry)
    return st, entry, False


# ----- dry-run 리포트(파일 쓰기 0) -----

def dry_run_report(result: dict, state_after: dict) -> str:
    lines = [
        f"[DRY-RUN] operationDate={result.get('date')} officialSequence={result.get('officialSequence')} "
        f"runStatus={result.get('runStatus')} reason={result.get('runReason')}",
        f"  allocatedCapital={result.get('allocatedCapital')} totalInvested={result.get('totalInvested')} "
        f"cashReserve={result.get('cashReserve')}",
        f"  FIFO 매도배치={result.get('sellBatchId')} 예상매도대금={result.get('rolloverSaleProceeds')} "
        f"rolloverBudget={result.get('rolloverBudget')}",
        f"  openBatch={result.get('openBatchCount')} openLots={result.get('openItemLotCount')} "
        f"officialAvailableCash={state_after.get('officialAvailableCash')}",
        f"  cash={result.get('cash')} totalAsset={result.get('totalAsset')} "
        f"cumulativeReturn={result.get('cumulativeReturn')}  (fee/tax modeled={FEE_TAX_MODELED})",
    ]
    for p in (result.get("plan") or []):
        lines.append(f"    - {p['code']} {p.get('name')}: open={p['openPrice']} qty={p['quantity']} amount={p['amount']}")
    lines.append("  (production 파일 쓰기 0건 · 기존 JSON 변경 0건 · REPO1 public 복사 0건)")
    return "\n".join(lines)


if __name__ == "__main__":
    # 합성 자가 데모 — production 파일/pykrx 미접근.
    cal = make_calendar(["2030-01-02", "2030-01-03", "2030-01-04"])
    rk = [{"code": f"{i:06d}", "name": f"S{i}", "rank": i, "combinedRank": i,
           "profitabilityRank": i, "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2}
          for i in range(1, 11)]
    op = {f"{i:06d}": 1000 + i * 10 for i in range(1, 11)}
    st = empty_official_state()
    st, res = plan_official_day(st, "2030-01-02", rk, op, op, cal, now="2030-01-02T00:00:00")
    print(dry_run_report(res, st))
