#!/usr/bin/env python3
"""magic_rolling_engine 테스트 (Phase 45-E2).

전부 in-memory fixture. production JSON/pykrx/네트워크/REPO1 public 미접근.
실행:  python scripts/test_magic_rolling_engine.py
"""
from __future__ import annotations

import copy
from datetime import date, timedelta

import magic_rolling_engine as E

NOW = "2030-01-01T00:00:00"


# ----- fixtures -----

def weekdays(start_iso: str, n: int) -> list:
    """start 이후 평일 n개(합성 거래일 fixture용)."""
    y, m, d = (int(x) for x in start_iso.split("-"))
    cur = date(y, m, d)
    out = []
    while len(out) < n:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def mk_ranking(codes):
    return [{"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
             "profitabilityRank": i + 1, "valueRank": i + 1,
             "returnOnCapital": round(0.8 - i * 0.05, 4), "earningsYield": round(0.3 - i * 0.01, 4)}
            for i, c in enumerate(codes)]


def flat_prices(codes, price=1000.0):
    return {c: float(price) for c in codes}


CODES10 = [f"{i:06d}" for i in range(1, 11)]      # 000001..000010
CODES_ALT = [f"{i:06d}" for i in range(11, 21)]   # 000011..000020


def run_days(state, dates, codes_fn, price=1000.0, cal=None):
    results = []
    for d in dates:
        codes = codes_fn(d)
        rk = mk_ranking(codes)
        px = flat_prices(set(CODES10) | set(CODES_ALT), price)
        state, res = E.plan_official_day(state, d, rk, px, px, cal, now=NOW)
        results.append(res)
    return state, results


# ----- 테스트들 -----

def t1_first_official_day():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    st, res = E.plan_official_day(st, weekdays("2030-01-02", 1)[0], mk_ranking(CODES10),
                                  flat_prices(CODES10), flat_prices(CODES10), cal, now=NOW)
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert len([b for b in st["batches"] if b["operationMode"] == E.OFFICIAL]) == 1
    assert len(E._open_lots(st)) == 10
    assert len(st["buyLedger"]) == 10
    assert res["buyCount"] == 10 and res["sellCount"] == 0
    assert st["officialSequence"] == 1
    assert all(l["priceSource"] == "pykrx_open" for l in st["itemLots"])
    assert all("rankSnapshot" in l and l["rankSnapshot"]["combinedRank"] is not None for l in st["itemLots"])


def t2_fifty_open_batches():
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 50), lambda d: CODES10, cal=cal)
    open_batches = [b for b in st["batches"] if b["status"] == "OPEN"]
    assert st["officialSequence"] == 50, st["officialSequence"]
    assert len(open_batches) == 50, len(open_batches)
    assert len(E._open_lots(st)) == 500, len(E._open_lots(st))
    assert len(st["sellLedger"]) == 0, "no sells before 51st day"


def t3_51st_fifo_rollover():
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 51), lambda d: CODES10, cal=cal)
    open_batches = [b for b in st["batches"] if b["status"] == "OPEN"]
    closed_batches = [b for b in st["batches"] if b["status"] == "CLOSED"]
    assert st["officialSequence"] == 51
    assert len(open_batches) == 50, f"open batches stay 50, got {len(open_batches)}"
    assert len(closed_batches) == 1, "oldest batch closed"
    # 51일차 SELL 10 + 신규 BUY 10
    day51 = weekdays("2030-01-02", 51)[-1]
    sells_51 = [s for s in st["sellLedger"] if s["date"] == day51]
    buys_51 = [b for b in st["buyLedger"] if b["date"] == day51]
    assert len(sells_51) == 10 and len(buys_51) == 10
    assert all(s["sellReason"] == "FIFTY_BATCH_FIFO_ROLLOVER" for s in sells_51)
    assert all(s["holdingTradingDays"] == 50 for s in sells_51), [s["holdingTradingDays"] for s in sells_51]
    # 가장 오래된(sequence 1) 배치가 닫혔는지
    assert closed_batches[0]["sequence"] == 1


def t4_same_stock_separate_lots():
    """동일 종목이 연속 배치에 포함돼도 합치지 않고 별도 lot."""
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    ds = weekdays("2030-01-02", 2)
    st, _ = run_days(st, ds, lambda d: CODES10, cal=cal)  # 동일 10종목 2일
    code = CODES10[0]
    lots = [l for l in st["itemLots"] if l["code"] == code and l["status"] == "OPEN"]
    assert len(lots) == 2, f"two separate open lots for same code, got {len(lots)}"
    assert lots[0]["batchId"] != lots[1]["batchId"]
    assert lots[0]["lotId"] != lots[1]["lotId"]
    # 원가/거래기록 분리(합산 아님)
    assert all(l["investedAmount"] == lots[0]["investedAmount"] for l in lots)
    assert len([b for b in st["buyLedger"] if b["code"] == code]) == 2


def t5_idempotent_rerun():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    d = weekdays("2030-01-02", 1)[0]
    st, _ = E.plan_official_day(st, d, mk_ranking(CODES10), flat_prices(CODES10),
                                flat_prices(CODES10), cal, now=NOW)
    snap_batches = copy.deepcopy(st["batches"])
    snap_buys = copy.deepcopy(st["buyLedger"])
    st2, res2 = E.plan_official_day(st, d, mk_ranking(CODES10), flat_prices(CODES10),
                                    flat_prices(CODES10), cal, now=NOW)
    assert res2["runStatus"] == E.ALREADY_PROCESSED, res2["runStatus"]
    assert st2["batches"] == snap_batches, "no duplicate batch"
    assert st2["buyLedger"] == snap_buys, "no duplicate BUY"
    assert len(st2["batches"]) == 1 and len(st2["buyLedger"]) == 10


def t6_missing_open_price_blocks_whole_batch():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    d = weekdays("2030-01-02", 1)[0]
    px = flat_prices(CODES10)
    del px[CODES10[3]]  # 한 종목 시가 누락
    st2, res = E.plan_official_day(st, d, mk_ranking(CODES10), px, px, cal, now=NOW)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]
    assert len(st2["itemLots"]) == 0, "no partial buy"
    assert len(st2["buyLedger"]) == 0
    assert st2["officialAvailableCash"] == st["officialAvailableCash"], "현금 불변 (fallback 매수 0)"
    assert st2["officialSequence"] == 0, "sequence not advanced"


def t7_non_trading_day():
    # 거래일 캘린더 범위 안의 '주말'(거래일 set 미포함) 날짜 → NON_TRADING
    tds = weekdays("2030-01-02", 10)            # 평일 10개(주말을 사이에 둠)
    cal = E.make_calendar(tds)                  # range=[tds[0], tds[-1]]
    y, m, dd = (int(x) for x in tds[0].split("-"))
    cur = date(y, m, dd)
    weekend = None
    while cur.isoformat() <= tds[-1]:
        if cur.weekday() >= 5 and cur.isoformat() not in tds:
            weekend = cur.isoformat()
            break
        cur += timedelta(days=1)
    assert weekend is not None, "range 내 주말 날짜 확보 실패"
    st = E.empty_official_state()
    st2, res = E.plan_official_day(st, weekend, mk_ranking(CODES10), flat_prices(CODES10),
                                   flat_prices(CODES10), cal, now=NOW)
    assert res["runStatus"] == E.NON_TRADING_DAY, res["runStatus"]
    assert len([b for b in st2["batches"]]) == 0, "no batch"
    assert st2["officialSequence"] == 0, "sequence not increased"


def t8_insufficient_available_cash_blocks():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state(initial_capital=500_000)  # officialAvailableCash 500,000 < 배치자본 1,000,000
    d = weekdays("2030-01-02", 1)[0]
    st2, res = E.plan_official_day(st, d, mk_ranking(CODES10), flat_prices(CODES10),
                                   flat_prices(CODES10), cal, now=NOW)
    assert res["runStatus"] == E.BLOCKED_INSUFFICIENT_AVAILABLE_CASH, res["runStatus"]
    assert len(st2["itemLots"]) == 0, "부분매수 0"
    assert st2["officialAvailableCash"] == 500_000.0


def t9_pilot_separation():
    # 합성 06-08 pilot lot 10개(별도 보존, official itemLots에 미포함)
    pilot_lots = [{"lotId": f"MF-2026-06-08-{c}-{i:02d}", "code": c, "name": f"P{c}",
                   "buyDate": "2026-06-08", "buyOpenPrice": 3100.0, "quantity": 32,
                   "investedAmount": 99200.0, "rank": i, "status": "OPEN", "priceSource": "pykrx_open"}
                  for i, c in enumerate(CODES10, 1)]
    pilot_lots_before = copy.deepcopy(pilot_lots)
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    st["pilot"] = E.build_pilot_batch(pilot_lots, "2026-06-08")
    pilot_batch_before = copy.deepcopy(st["pilot"])
    # official 1일 실행
    st, results = run_days(st, weekdays("2030-01-02", 1), lambda d: CODES10, cal=cal)
    res = results[-1]
    assert res["openBatchCount"] == 1, "official open batch만 카운트(파일럿 제외)"
    assert st["officialSequence"] == 1
    assert all(b["operationMode"] == E.OFFICIAL for b in st["batches"]), "official 배치에 pilot 미혼입"
    assert st["pilot"]["operationMode"] == E.PILOT and st["pilot"]["sequence"] is None
    assert st["pilot"] == pilot_batch_before, "pilot 배치 불변"
    assert pilot_lots == pilot_lots_before, "pilot lot core 불변"
    assert st["pilot"]["totalInvested"] == round(99200.0 * 10, 2)


def t10_append_only_past_records_immutable():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    ds = weekdays("2030-01-02", 3)
    st, _ = run_days(st, ds[:2], lambda d: CODES10, cal=cal)
    buys_before = copy.deepcopy(st["buyLedger"])
    lots_before = copy.deepcopy(st["itemLots"])
    # 신규 날짜 1개 실행
    st, _ = run_days(st, ds[2:3], lambda d: CODES10, cal=cal)
    # 과거 원장 불변(앞부분 그대로) + 신규만 append
    assert st["buyLedger"][:len(buys_before)] == buys_before, "과거 BUY 원장 불변"
    assert st["itemLots"][:len(lots_before)] == lots_before, "과거 lot 불변"
    assert len(st["buyLedger"]) == len(buys_before) + 10, "신규 날짜만 +10"
    # 과거 날짜 재실행 → ALREADY_PROCESSED, 변경 0
    snap = copy.deepcopy(st["buyLedger"])
    st2, res = E.plan_official_day(st, ds[0], mk_ranking(CODES10), flat_prices(CODES10),
                                   flat_prices(CODES10), cal, now=NOW)
    assert res["runStatus"] == E.ALREADY_PROCESSED
    assert st2["buyLedger"] == snap


def t11_no_calendar_blocks():
    st = E.empty_official_state()
    st2, res = E.plan_official_day(st, "2030-01-02", mk_ranking(CODES10), flat_prices(CODES10),
                                   flat_prices(CODES10), None, now=NOW)  # 캘린더 없음
    assert res["runStatus"] == E.BLOCKED_NO_TRADING_CALENDAR, res["runStatus"]
    assert st2["officialSequence"] == 0 and len(st2["batches"]) == 0


def t12_eval_vs_trade_price_separation():
    """평가가가 거래가와 달라도 매수원장 불변, 평가만 별도 반영."""
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    d = weekdays("2030-01-02", 1)[0]
    buy_px = flat_prices(CODES10, 1000.0)
    eval_px = flat_prices(CODES10, 1100.0)  # 평가가 ↑
    st, res = E.plan_official_day(st, d, mk_ranking(CODES10), buy_px, eval_px, cal, now=NOW)
    assert all(l["buyOpenPrice"] == 1000.0 for l in st["itemLots"]), "매수원장은 거래가(1000) 그대로"
    ev = st["evalSnapshots"][-1]
    assert ev["holdingsMarketValue"] == round(1100.0 * 100 * 10, 2), "평가는 평가가(1100)로"
    assert res["holdingsMarketValue"] == ev["holdingsMarketValue"]
    # 평가가 일부 누락 → 매수원장 불변, missingEval만 기록
    st2, _ = E.plan_official_day(st, weekdays("2030-01-02", 2)[1], mk_ranking(CODES10),
                                 buy_px, {c: 1100.0 for c in CODES10[:9]}, cal, now=NOW)
    assert all(l["buyOpenPrice"] == 1000.0 for l in st2["itemLots"]), "평가 실패가 매수원장 변경 안 함"
    assert st2["evalSnapshots"][-1]["missingEvalCodes"] == [CODES10[9]]


def t13_pilot_migration_plan_no_mutation():
    """build_pilot_batch는 계획만 — 입력 lot core 필드 불변."""
    lots = [{"lotId": "MF-2026-06-08-046940-01", "code": "046940", "buyDate": "2026-06-08",
             "buyOpenPrice": 3100.0, "quantity": 32, "investedAmount": 99200.0, "rank": 1}]
    before = copy.deepcopy(lots)
    batch = E.build_pilot_batch(lots, "2026-06-08")
    assert lots == before, "입력 lot 불변(마이그레이션은 래퍼만 생성)"
    assert batch["batchId"] == "MF-PILOT-2026-06-08" and batch["operationMode"] == E.PILOT
    assert batch["sequence"] is None
    # missed-run 기록은 가짜 거래 0
    mr = E.record_missed_run("2026-06-09")
    assert mr["status"] == E.MISSED_RUN and mr["syntheticTradesCreated"] is False


# ===== 45-E2.1 추가: 배치 자금 규칙 =====

def t14_initial_batch_capital():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 1), lambda d: CODES10, cal=cal)
    b = st["batches"][0]
    assert b["allocatedCapital"] == 1_000_000
    assert b["totalInvested"] <= 1_000_000
    assert b["cashReserve"] == round(b["allocatedCapital"] - b["totalInvested"], 2)
    lots = [l for l in st["itemLots"] if l["batchId"] == b["batchId"]]
    assert len(lots) == 10 and all(l["quantity"] >= 1 for l in lots)
    assert st["officialAvailableCash"] == 49_000_000.0


def t15_fifty_initial_batches_capital_conservation():
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 50), lambda d: CODES10, cal=cal)
    initial_batches = [b for b in st["batches"] if b["rolloverBudget"] is None]
    assert len(initial_batches) == 50
    assert round(sum(b["allocatedCapital"] for b in initial_batches), 2) == 50_000_000.0
    assert st["officialAvailableCash"] == 0.0
    for b in initial_batches:
        assert round(b["totalInvested"] + b["cashReserve"], 2) == b["allocatedCapital"]
    open_b = [b for b in st["batches"] if b["status"] == "OPEN"]
    cons = round(st["officialAvailableCash"]
                 + sum(b["cashReserve"] for b in open_b)
                 + sum(b["totalInvested"] for b in open_b), 2)
    assert cons == 50_000_000.0, cons   # 현금 소실/이중계산 없음


def t16_high_price_min_one_share():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    d = weekdays("2030-01-02", 1)[0]
    codes = CODES10
    px = {c: 1000.0 for c in codes[:9]}
    px[codes[9]] = 150_000.0   # floor(100000/150000)=0 → 최소 1주 시도
    st2, res = E.plan_official_day(st, d, mk_ranking(codes), px, px, cal, now=NOW)
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    lots = st2["itemLots"]
    assert len(lots) == 10 and all(l["quantity"] >= 1 for l in lots)
    assert next(l for l in lots if l["code"] == codes[9])["quantity"] == 1
    assert st2["batches"][0]["totalInvested"] <= 1_000_000


def t17_min_one_share_exceeds_budget_blocked():
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    d = weekdays("2030-01-02", 1)[0]
    px = {c: 200_000.0 for c in CODES10}  # 10종목 ×1주 = 2,000,000 > 1,000,000
    st2, res = E.plan_official_day(st, d, mk_ranking(CODES10), px, px, cal, now=NOW)
    assert res["runStatus"] == E.BLOCKED_INSUFFICIENT_BATCH_BUDGET, res["runStatus"]
    assert len(st2["itemLots"]) == 0 and len(st2["batches"]) == 0
    assert st2["officialAvailableCash"] == 50_000_000.0 and st2["officialSequence"] == 0


def t18_quantity_determinism():
    codes = CODES10
    px = {codes[i]: (1000.0 if i < 9 else 150_000.0) for i in range(10)}  # 감량 유발
    rk = mk_ranking(codes)
    q1, t1 = E.allocate_quantities(rk, px, 1_000_000)
    q2, t2 = E.allocate_quantities(rk, px, 1_000_000)
    assert q1 == q2 and t1 == t2
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    d = weekdays("2030-01-02", 1)[0]
    a, _ = E.plan_official_day(E.empty_official_state(), d, rk, px, px, cal, now=NOW)
    b, _ = E.plan_official_day(E.empty_official_state(), d, rk, px, px, cal, now=NOW)
    assert a["batches"][0]["totalInvested"] == b["batches"][0]["totalInvested"]
    assert [l["quantity"] for l in a["itemLots"]] == [l["quantity"] for l in b["itemLots"]]


def t19_rollover_budget_from_sale_plus_reserve():
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    price = 1100.0  # qty=90, invested 990,000, cashReserve 10,000
    st, _ = run_days(st, weekdays("2030-01-02", 50), lambda d: CODES10, price=price, cal=cal)
    b1 = [b for b in st["batches"] if b["sequence"] == 1][0]
    assert b1["cashReserve"] == round(1_000_000 - b1["totalInvested"], 2)
    cash_before = st["officialAvailableCash"]
    st, _ = run_days(st, weekdays("2030-01-02", 51)[-1:], lambda d: CODES10, price=price, cal=cal)
    nb = [b for b in st["batches"] if b["sequence"] == 51][0]
    assert nb["rolloverBudget"] == round(nb["rolloverSaleProceeds"] + b1["cashReserve"], 2)
    assert nb["allocatedCapital"] == nb["rolloverBudget"]
    assert st["officialAvailableCash"] == cash_before   # 전역/타배치 현금 미사용
    assert [b for b in st["batches"] if b["sequence"] == 1][0]["status"] == "CLOSED"


def t20_rollover_atomic_block_no_sell():
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 50), lambda d: CODES10, cal=cal)  # 50 open batches
    sells_before = len(st["sellLedger"])
    d51 = weekdays("2030-01-02", 51)[-1]
    prices = {c: 1000.0 for c in (set(CODES10) | set(CODES_ALT))}
    del prices[CODES_ALT[0]]  # 신규 top10(ALT) 중 1종목 시가 누락
    st2, res = E.plan_official_day(st, d51, mk_ranking(CODES_ALT), prices, prices, cal, now=NOW)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]
    assert len(st2["sellLedger"]) == sells_before, "매도 0(원자성)"
    assert len([b for b in st2["batches"] if b["status"] == "OPEN"]) == 50
    assert len(st2["batches"]) == 50 and st2["officialSequence"] == 50


def _rollover_at(price_day51):
    cal = E.make_calendar(weekdays("2030-01-02", 60))
    st = E.empty_official_state()
    st, _ = run_days(st, weekdays("2030-01-02", 50), lambda d: CODES10, price=1000.0, cal=cal)
    d51 = weekdays("2030-01-02", 51)[-1]
    prices = {c: float(price_day51) for c in (set(CODES10) | set(CODES_ALT))}
    st, _ = E.plan_official_day(st, d51, mk_ranking(CODES10), prices, prices, cal, now=NOW)
    return st


def t21_pnl_reflected_in_rollover_budget():
    st = _rollover_at(1200.0)   # 이익
    nb = [b for b in st["batches"] if b["sequence"] == 51][0]
    assert nb["allocatedCapital"] > 1_000_000 and nb["allocatedCapital"] == round(1200.0 * 100 * 10, 2)
    st2 = _rollover_at(800.0)   # 손실
    nb2 = [b for b in st2["batches"] if b["sequence"] == 51][0]
    assert nb2["allocatedCapital"] < 1_000_000 and nb2["allocatedCapital"] == round(800.0 * 100 * 10, 2)


def t22_pilot_capital_isolation():
    pilot_lots = [{"lotId": f"MF-2026-06-08-{c}-{i:02d}", "code": c, "buyDate": "2026-06-08",
                   "buyOpenPrice": 3100.0, "quantity": 32, "investedAmount": 88984.0}
                  for i, c in enumerate(CODES10, 1)]  # 합 889,840
    cal = E.make_calendar(weekdays("2030-01-02", 5))
    st = E.empty_official_state()
    st["pilot"] = E.build_pilot_batch(pilot_lots, "2026-06-08")
    assert st["initialCapital"] == 50_000_000
    assert st["officialAvailableCash"] == 50_000_000.0   # pilot 미포함
    st, _ = run_days(st, weekdays("2030-01-02", 1), lambda d: CODES10, cal=cal)
    assert st["officialAvailableCash"] == 49_000_000.0   # 공식 1배치만 차감
    assert st["pilot"]["totalInvested"] == 889_840.0
    assert st["pilot"]["allocatedCapital"] is None and st["pilot"]["operationMode"] == E.PILOT


TESTS = [
    ("1  첫 official 거래일(batch1/lot10/BUY10)", t1_first_official_day),
    ("2  50거래일 누적(open batch50/lot500/매도0)", t2_fifty_open_batches),
    ("3  51거래일 FIFO 교체(SELL10/BUY10/batch50유지)", t3_51st_fifo_rollover),
    ("4  동일 종목 별도 lot(합산 금지)", t4_same_stock_separate_lots),
    ("5  동일 날짜 재실행 idempotent", t5_idempotent_rerun),
    ("6  시가 한 종목 누락→전체 BLOCKED(부분매수0)", t6_missing_open_price_blocks_whole_batch),
    ("7  비개장일→배치0/시퀀스0", t7_non_trading_day),
    ("8  officialAvailableCash 부족→BLOCKED", t8_insufficient_available_cash_blocks),
    ("9  PILOT 분리(official 카운트 제외/불변)", t9_pilot_separation),
    ("10 append-only(과거 원장 불변)", t10_append_only_past_records_immutable),
    ("11 신뢰 캘린더 없음→BLOCKED", t11_no_calendar_blocks),
    ("12 평가가/거래가 분리", t12_eval_vs_trade_price_separation),
    ("13 PILOT 마이그레이션 계획=무변경/missed-run 가짜0", t13_pilot_migration_plan_no_mutation),
    ("14 초기 배치 자금(allocated 1M/cashReserve/qty>=1)", t14_initial_batch_capital),
    ("15 50배치 자금 보존(Σallocated=50M/availableCash=0)", t15_fifty_initial_batches_capital_conservation),
    ("16 고가 종목 최소 1주", t16_high_price_min_one_share),
    ("17 최소1주합>예산→BLOCKED_INSUFFICIENT_BATCH_BUDGET", t17_min_one_share_exceeds_budget_blocked),
    ("18 정수 수량 결정성", t18_quantity_determinism),
    ("19 51번째 rolloverBudget=매도대금+cashReserve", t19_rollover_budget_from_sale_plus_reserve),
    ("20 rollover 원자성(BLOCKED시 매도0)", t20_rollover_atomic_block_no_sell),
    ("21 손익 반영(이익↑/손실↓)", t21_pnl_reflected_in_rollover_budget),
    ("22 PILOT 자금 격리", t22_pilot_capital_isolation),
]


def main():
    passed, failed = 0, 0
    for name, fn in TESTS:
        try:
            fn()
            print(f"[PASS] {name}")
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {name}  -> {e}")
            failed += 1
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {name}  -> {type(e).__name__}: {e}")
            failed += 1
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(TESTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
