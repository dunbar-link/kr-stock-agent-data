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
from datetime import date as _date, datetime, timedelta
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
BLOCKED_EVALUATION_SNAPSHOT_ALIAS_CONFLICT = "BLOCKED_EVALUATION_SNAPSHOT_ALIAS_CONFLICT"  # 45-E12.1: 두 키 내용 불일치
MISSED_RUN = "MISSED_RUN"
MISSED_RUN_NO_PREOPEN_SIGNAL = "NO_PREOPEN_SIGNAL_PACKAGE"
PILOT_RUN = "PILOT"

SELL_REASON_ROLLOVER = "FIFTY_BATCH_FIFO_ROLLOVER"
RANK_FIELDS = ("rank", "combinedRank", "profitabilityRank", "valueRank", "returnOnCapital", "earningsYield")

# ----- 실행 프로파일 (WABABA-MAGIC-FORMULA-EXECUTION-PROFILE-R1) -----
# 왜: 같은 마법공식 종목선정을 유지한 채 *실행 방식만* 다른 펀드(CORE/EASY)를 굴리기 위해서다.
#     엔진을 복제하면 두 벌의 매매 로직이 생겨 검증이 갈라진다 → 복제 금지, 프로파일 주입으로 푼다.
# 계약(동결 보호):
#   - profile 인자를 주지 않으면 아래 CORE_PROFILE(=기존 모듈 상수)로 해석된다.
#     즉 **기존 호출부는 한 곳도 바꾸지 않아도 동작·결과가 완전히 동일**하다.
#   - CORE 값은 상수를 재입력하지 않고 그대로 참조한다(정본 1곳 유지).
CADENCE_DAILY = "DAILY"                                   # 매 거래일 신규 batch (기존 Core)
CADENCE_WEEKLY_FIRST = "WEEKLY_FIRST_TRADING_DAY"         # 매주 첫 실제 거래일에만 신규 batch
CADENCES = (CADENCE_DAILY, CADENCE_WEEKLY_FIRST)

# cadence 상 신규 batch 를 시작하지 않는 거래일의 runStatus.
# 휴장(NON_TRADING_DAY)과 구분한다 — 개장은 했고 보유는 유지되며 거래일 index 는 전진한다.
CADENCE_NON_BUY_DAY = "CADENCE_NON_BUY_DAY"

CORE_PROFILE = {
    "profileId": "CORE",
    "cadence": CADENCE_DAILY,
    "topN": TOP_N,
    "holdTradingDays": HOLD_TRADING_DAYS,
    "maxOpenBatches": MAX_OPEN_BATCHES,
    "batchCapital": INITIAL_BATCH_CAPITAL,
}


def resolve_execution_profile(profile: Optional[dict] = None) -> dict:
    """profile(부분 dict) → 완전한 프로파일. None/빈 값이면 CORE(기존 상수)와 동일.

    None 값 키는 무시한다(부분 override 만 하고 나머지는 Core 기본값 유지).
    """
    p = dict(CORE_PROFILE)
    if profile:
        for k, v in dict(profile).items():
            if v is not None:
                p[k] = v
    if p["cadence"] not in CADENCES:
        raise ValueError(f"unknown cadence: {p['cadence']!r} (allowed: {CADENCES})")
    for k in ("topN", "holdTradingDays"):
        if int(p[k]) < 1:
            raise ValueError(f"{k} must be >= 1 (got {p[k]!r})")
    return p


def is_batch_start_day(date: str, calendar: Optional[dict], profile: Optional[dict] = None) -> bool:
    """이 거래일에 *신규 batch 를 시작하는가*. 순수 함수(파일 I/O·네트워크 0).

    DAILY  : 항상 True → 기존 Core 동작 불변(분기 자체가 발생하지 않는다).
    WEEKLY_FIRST_TRADING_DAY : 같은 ISO 주(월~일)에서 **가장 이른 실제 거래일**만 True.
      → 월요일이 휴장이면 그 주의 첫 실제 거래일(화/수…)이 자동으로 선택된다.
        거래일 판정은 기존 calendar/classify_trading_day 를 그대로 재사용한다(새 캘린더 로직 0).
        미래 데이터를 보지 않는다 — 같은 주의 *이전* 날짜만 조회한다.
    """
    prof = resolve_execution_profile(profile)
    if prof["cadence"] == CADENCE_DAILY:
        return True
    if classify_trading_day(date, calendar) != "TRADING":
        return False
    if prof["cadence"] == CADENCE_WEEKLY_FIRST:
        d = _date.fromisoformat(str(date)[:10])
        trading_days = calendar["tradingDays"]
        monday = d - timedelta(days=d.weekday())
        for i in range(d.weekday()):          # 같은 주의 '이전' 날짜만 (look-ahead 0)
            if (monday + timedelta(days=i)).isoformat() in trading_days:
                return False
        return True
    raise ValueError(f"unknown cadence: {prof['cadence']!r}")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


class EvalSnapshotAliasConflict(Exception):
    """45-E12.1: evaluationSnapshots와 legacy evalSnapshots 내용이 다를 때."""


def normalize_eval_snapshots(st: dict) -> dict:
    """평가 스냅샷 키를 표준 evaluationSnapshots로 *in-memory* 정규화한다(canonical 파일 미수정).
    - evaluationSnapshots 있으면 우선. evalSnapshots만 있으면 그것을 evaluationSnapshots로 alias.
    - 둘 다 있으면 동일 객체/내용이면 OK, 다르면 EvalSnapshotAliasConflict.
    - 둘 다 없으면 빈 배열. 중복 evalSnapshots 키는 제거(별도 저장 안 함). 입력 dict를 제자리 수정."""
    std = st.get("evaluationSnapshots")
    leg = st.get("evalSnapshots")
    if std is not None and leg is not None:
        if std is not leg and std != leg:
            raise EvalSnapshotAliasConflict(
                "evaluationSnapshots != evalSnapshots (content mismatch)")
        st["evaluationSnapshots"] = std
    elif std is None and leg is not None:
        st["evaluationSnapshots"] = leg
    elif std is None and leg is None:
        st["evaluationSnapshots"] = []
    st.pop("evalSnapshots", None)
    return st


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
        "evaluationSnapshots": [],   # 표준 키(45-E12.1). legacy alias=evalSnapshots(읽기만 호환).
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


def due_official_batches(state: dict, current_trading_day_index: int,
                         profile: Optional[dict] = None) -> list:
    """45-E10: 매도 예정 거래일 index가 현재 거래일 index 이하인 open 공식 batch(FIFO=오래된 순).
    plannedSellTradingDayIndex 누락 batch는 buyTradingDayIndex(또는 sequence) + holdTradingDays로 보정.
    profile 미지정 → HOLD_TRADING_DAYS(기존과 동일)."""
    hold = resolve_execution_profile(profile)["holdTradingDays"]
    out = []
    for b in _official_open_batches(state):
        psi = b.get("plannedSellTradingDayIndex")
        if psi is None:
            base = b.get("buyTradingDayIndex", b.get("sequence", 0))
            psi = base + hold
        if psi <= current_trading_day_index:
            out.append(b)
    return sorted(out, key=lambda b: (b.get("buyTradingDayIndex", b.get("sequence", 0)), b.get("sequence", 0)))


def fund_cash(state: dict) -> float:
    """미배정 초기자본 + open 공식배치들의 cashReserve 합. (PILOT/CLOSED 제외)"""
    reserves = sum(b.get("cashReserve", 0.0) for b in state["batches"]
                   if b.get("operationMode") == OFFICIAL and b.get("status") == "OPEN")
    return round(state["officialAvailableCash"] + reserves, 2)


# ----- 정수 수량 배분(결정적) -----

def allocate_quantities(top10: list, open_prices: dict, allocated_capital: float,
                        profile: Optional[dict] = None):
    """rule 4: targetPerStock=allocated/topN, qty=max(1,floor(target/open)). profile 미지정 → TOP_N.
    총액이 allocated 초과 시 qty>1 종목 중 '목표 대비 초과액 큰 종목'부터 결정적으로 1주씩 감소(최소 1주 유지).
    모두 최소 1주인데도 초과면 (None, total) 반환 → BLOCKED_INSUFFICIENT_BATCH_BUDGET.
    동일 입력 → 동일 결과(결정성)."""
    target = allocated_capital / resolve_execution_profile(profile)["topN"]
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
                      timing: Optional[dict] = None, trading_day_index: Optional[int] = None,
                      profile: Optional[dict] = None):
    """순수 함수. (새 state, day_result) 반환. 입력 state 불변(deepcopy). 파일 I/O 0.

    profile(선택, WABABA-MAGIC-FORMULA-EXECUTION-PROFILE-R1): 실행 프로파일.
      **미지정이면 CORE(기존 모듈 상수)** — 기존 호출부는 결과가 완전히 동일하다.
      cadence 가 DAILY 가 아니면 '신규 batch 를 시작하지 않는 거래일'이 생긴다(CADENCE_NON_BUY_DAY).
      그 날에도 개장은 했으므로 평가·거래일 index 는 정상 전진하고 보유는 유지된다.

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
    prof = resolve_execution_profile(profile)

    # 0) 평가 스냅샷 키 정규화(evaluationSnapshots 표준). canonical 직접입력 호환(KeyError 방지).
    try:
        st = normalize_eval_snapshots(st)
    except EvalSnapshotAliasConflict as e:
        return st, _blocked(date, BLOCKED_EVALUATION_SNAPSHOT_ALIAS_CONFLICT, str(e), now)

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
        st["evaluationSnapshots"].append(ev)
        st["prevTotalAsset"] = ev["totalAsset"]
        return st, led

    # 2-B) cadence — 이 거래일에 신규 batch 를 시작하는가(프로파일).
    #      DAILY 는 항상 True 이므로 이 분기는 **Core 에서 절대 발생하지 않는다**(동작 불변).
    #      비매수 거래일: 개장했으므로 평가·KRX 캘린더·거래일 index 는 전진시키고,
    #      officialSequence / officialExecutionCalendar / 매매 원장은 손대지 않는다.
    #      (만기 도래 batch 의 매도는 Core 와 동일하게 '매수와 원자적으로' 처리되므로
    #       다음 매수일로 이월된다 — 보유기간이 hold 이상으로 늘 수는 있어도 미기록 매도는 없다.)
    if not is_batch_start_day(date, calendar, prof):
        cur_idx = trading_day_index if trading_day_index is not None else st.get("officialTradingDayIndex", 0) + 1
        ev = evaluate(st, date, eval_prices)
        if date not in st.setdefault("officialKrxTradingCalendar", []):
            st["officialKrxTradingCalendar"].append(date)
        st["officialTradingDayIndex"] = cur_idx
        led = _daily_ledger(st, date, True, CADENCE_NON_BUY_DAY,
                            f"cadence={prof['cadence']} — 신규 batch 미개시(보유 유지)",
                            eval_info=ev, now=now)
        st["dailyLedger"].append(led)
        st["evaluationSnapshots"].append(ev)
        st["prevTotalAsset"] = ev["totalAsset"]
        return st, led

    # 3) ranking
    if not ranking or len(ranking) < prof["topN"]:
        return st, _blocked(date, BLOCKED_MISSING_RANKING, "ranking missing or <10", now)
    top10 = ranking[:prof["topN"]]

    # 4) 교체 여부 — 45-E10: 실제 KRX 거래일 index 기준(open batch 수/officialSequence 기준 아님).
    #    현재 거래일 index = 주입값 또는 (직전 index+1). 누락일은 외부 recorder가 index를 올려둠.
    current_index = trading_day_index if trading_day_index is not None else st.get("officialTradingDayIndex", 0) + 1
    due = due_official_batches(st, current_index, profile=prof)
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
    qty_map, total_invested = allocate_quantities(top10, open_prices, allocated, profile=prof)
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
            "plannedSellTradingDayIndex": current_index + prof["holdTradingDays"],
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
        "plannedSellSequence": seq + prof["holdTradingDays"],   # [legacy] 보존(매도 판정엔 미사용)
        "buyTradingDayIndex": current_index,
        "plannedSellTradingDayIndex": current_index + prof["holdTradingDays"],
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
    st["evaluationSnapshots"].append(ev)
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

def migrate_official_state_indices(state: dict, profile: Optional[dict] = None) -> dict:
    """기존 canonical을 거래일 index 모델로 *additive* 마이그레이션(idempotent). 입력 불변(deepcopy 반환).
    profile 미지정 → HOLD_TRADING_DAYS(기존과 동일).
    - officialTradingDayIndex / officialKrxTradingCalendar / officialExecutionCalendar 보장.
    - 누락 시에만 officialTradingCalendar(=성공일)에서 backfill(누락일 없던 과거이므로 index==sequence).
    - batch/itemLot에 buyTradingDayIndex / plannedSellTradingDayIndex 보강(없을 때만)."""
    hold = resolve_execution_profile(profile)["holdTradingDays"]
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
            b["plannedSellTradingDayIndex"] = b["buyTradingDayIndex"] + hold
    for l in st.get("itemLots", []):
        if l.get("buyTradingDayIndex") is None:
            l["buyTradingDayIndex"] = l.get("buySequence", 0)
        if l.get("plannedSellTradingDayIndex") is None:
            l["plannedSellTradingDayIndex"] = l["buyTradingDayIndex"] + hold
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
