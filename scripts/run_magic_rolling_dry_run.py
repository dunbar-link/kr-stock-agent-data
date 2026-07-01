#!/usr/bin/env python3
"""와바바 마법공식 — PILOT 마이그레이션 미리보기 + OFFICIAL DRY-RUN thin wrapper (Phase 45-E3).

원칙
----
- *thin wrapper*: 수량배분/FIFO/원장/평가 등 코어 규칙을 *복제하지 않는다*. 전부 magic_rolling_engine 호출.
- 읽기 전용: production JSON 쓰기 0, REPO1 public 반영 0, officialStartDate 저장 0, daily_run 연결 0.
- 거래일 판정은 평일 추정이 아니라 pykrx 실거래일(get_business_days)로 한다(실패 시 BLOCKED_NO_TRADING_CALENDAR).
- 매수/매도 거래가는 pykrx_open만. 시가 없으면 BLOCKED(종가/universe/fallback 대체 금지).
- pykrx는 실제 dry-run에서 읽기 전용 조회만. 테스트는 pykrx를 mock한다(network 0).
"""
from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import magic_rolling_engine as E

ROOT = Path(__file__).resolve().parents[1]
LOTS_PATH = ROOT / "magic-formula-trade-lots.json"
RANKINGS_PATH = ROOT / "magic-formula-rankings.json"

# PILOT 불변 기준(2026-06-08 첫 lot)
PILOT_DATE = "2026-06-08"
PILOT_EXPECTED_LOTS = 10
PILOT_EXPECTED_INVESTED = 889_840.0
PILOT_CORE_FIELDS = ("code", "name", "buyDate", "buyOpenPrice", "priceSource",
                     "quantity", "investedAmount", "rank", "lotId")

# wrapper 전용 BLOCKED (engine BLOCKED는 그대로 재사용)
BLOCKED_PILOT_SOURCE_MISMATCH = "BLOCKED_PILOT_SOURCE_MISMATCH"
PILOT_PREVIEW_OK = "PILOT_PREVIEW_OK"

# 신호일·체결일 분리 / look-ahead 차단 BLOCKED (Phase 45-E4.1)
BLOCKED_MISSING_SIGNAL_METADATA = "BLOCKED_MISSING_SIGNAL_METADATA"
BLOCKED_SIGNAL_DATE_MISMATCH = "BLOCKED_SIGNAL_DATE_MISMATCH"
BLOCKED_SIGNAL_NOT_PREVIOUS_TRADING_DAY = "BLOCKED_SIGNAL_NOT_PREVIOUS_TRADING_DAY"
BLOCKED_RANKING_GENERATED_AFTER_OPEN = "BLOCKED_RANKING_GENERATED_AFTER_OPEN"
BLOCKED_UNIVERSE_DATE_MISMATCH = "BLOCKED_UNIVERSE_DATE_MISMATCH"
BLOCKED_EXECUTION_DATE_MISMATCH = "BLOCKED_EXECUTION_DATE_MISMATCH"

# 신호 메타데이터 필수 필드(legacy baseDate-only 문서는 BLOCKED_MISSING_SIGNAL_METADATA)
REQUIRED_SIGNAL_FIELDS = ("signalAsOfDate", "rankingGeneratedAt", "universeBaseDate", "formulaVersion")

# PILOT(2026-06-08) 타이밍 감사(45-E4 실증). 실제 PILOT 파일은 수정하지 않는다.
PILOT_LEGACY_RANKING_BASE_DATE = "2026-06-08"   # rankings.json이 찍어둔 라벨(실제 신호일 아님)
PILOT_AUDITED_SIGNAL_AS_OF_DATE = "2026-05-29"  # marketCap이 일치한 실제 신호(종가) 기준일
PILOT_AUDIT_EXECUTION_DATE = "2026-06-08"        # 시가 매수 체결일
PILOT_TIMING_AUDIT_PASS = "PASS_NO_LOOKAHEAD"


def _read_json(path):
    p = Path(path)
    if not p.exists():
        return None
    with p.open(encoding="utf-8-sig") as f:
        return json.load(f)


# ===== PILOT 마이그레이션 미리보기 (read-only, write 0) =====

def pilot_preview(lots_doc=None, lots_path=LOTS_PATH) -> dict:
    doc = lots_doc if lots_doc is not None else _read_json(lots_path)
    lots = (doc or {}).get("lots") or []
    src = [l for l in lots if str(l.get("buyDate")) == PILOT_DATE]
    total = round(sum(float(l.get("investedAmount") or 0) for l in src), 2)

    if len(src) != PILOT_EXPECTED_LOTS:
        return {"runStatus": BLOCKED_PILOT_SOURCE_MISMATCH,
                "runReason": f"expected {PILOT_EXPECTED_LOTS} lots, got {len(src)}",
                "writeCount": 0, "sourceDate": PILOT_DATE}
    if round(total, 2) != PILOT_EXPECTED_INVESTED:
        return {"runStatus": BLOCKED_PILOT_SOURCE_MISMATCH,
                "runReason": f"totalInvested {total} != {PILOT_EXPECTED_INVESTED}",
                "writeCount": 0, "sourceDate": PILOT_DATE}
    for l in src:
        if l.get("priceSource") != "pykrx_open":
            return {"runStatus": BLOCKED_PILOT_SOURCE_MISMATCH,
                    "runReason": f"{l.get('lotId')} priceSource={l.get('priceSource')} != pykrx_open",
                    "writeCount": 0, "sourceDate": PILOT_DATE}
        for k in ("code", "buyDate", "buyOpenPrice", "quantity", "investedAmount", "lotId", "rank"):
            if l.get(k) in (None, ""):
                return {"runStatus": BLOCKED_PILOT_SOURCE_MISMATCH,
                        "runReason": f"{l.get('lotId')} missing core field {k}",
                        "writeCount": 0, "sourceDate": PILOT_DATE}

    snapshot = copy.deepcopy(src)
    batch = E.build_pilot_batch(src, PILOT_DATE)       # 코어 호출(입력 불변)
    core_unchanged = (src == snapshot)
    return {
        "runStatus": PILOT_PREVIEW_OK, "sourceDate": PILOT_DATE,
        "pilotBatchId": batch["batchId"], "operationMode": batch["operationMode"],
        "itemLotCount": len(src), "totalInvested": total,
        "coreFieldsUnchanged": core_unchanged, "officialCapitalImpact": 0,
        "officialAvailableCashImpact": 0, "writeCount": 0, "preview": batch,
    }


# ===== ranking 입력 정규화/검증 =====

def _ranking_from_doc(rankings_doc: dict) -> list:
    top = (rankings_doc or {}).get("todayMagicRankingTop10") or []
    out = []
    for it in top:
        out.append({k: it.get(k) for k in
                    ("code", "name", "rank", "combinedRank", "profitabilityRank",
                     "valueRank", "returnOnCapital", "earningsYield")})
    return out


# ===== KRX 거래일 캘린더 (pykrx 실거래일; 평일 추정 금지) =====

def build_krx_calendar(operation_date: str, pykrx_stock=None):
    """pykrx.stock.get_business_days로 실거래일을 받아 engine.make_calendar 생성.
    월 첫 거래일의 직전 거래일(전월 마지막 거래일) 탐색을 위해 operation_date의 당월과 전월을 함께 로드한다.
    실패/빈 결과 → None(=engine이 BLOCKED_NO_TRADING_CALENDAR 처리). pykrx_stock 주입 시 테스트 mock."""
    try:
        if pykrx_stock is None:
            from pykrx import stock as pykrx_stock  # lazy: import 시 network 회피
        y, m = int(operation_date[:4]), int(operation_date[5:7])
        prev_y, prev_m = (y - 1, 12) if m == 1 else (y, m - 1)
        days = []
        for (yy, mm) in ((prev_y, prev_m), (y, m)):  # 전월 → 당월(월경계 직전거래일 확보)
            bdays = pykrx_stock.get_business_days(yy, mm)
            for d in (bdays or []):
                s = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
                days.append(s)
        if not days:
            return None
        return E.make_calendar(days)  # make_calendar가 set/sort로 중복 제거·정렬
    except Exception:
        return None


# ===== 시가 조회 (pykrx_open; 실패/누락은 dict에서 빠져 engine이 BLOCKED) =====

def fetch_open_prices_pykrx(date: str, codes, pykrx_stock=None) -> dict:
    """pykrx OHLCV에서 '시가'만. read-only. pykrx_stock 주입 시 테스트 mock."""
    try:
        if pykrx_stock is None:
            from pykrx import stock as pykrx_stock
        ymd = date.replace("-", "")
        frame = pykrx_stock.get_market_ohlcv_by_ticker(ymd, market="ALL")
        out = {}
        for code in codes:
            try:
                v = float(frame.loc[str(code).zfill(6)]["시가"])
            except Exception:
                continue
            if v and v > 0:
                out[str(code).zfill(6)] = v
        return out
    except Exception:
        return {}


# ===== 신호일·체결일 / 시점 검증 유틸 (look-ahead 차단; Phase 45-E4.1) =====

def previous_krx_trading_day(execution_date: str, calendar) -> Optional[str]:
    """체결일 직전 KRX 거래일(신뢰 캘린더 기준). 평일/단순 -1일 추정 금지.
    금→월, 휴장전→휴장후 모두 '직전 거래일'로 올바르게 잡는다. 없으면 None."""
    if not calendar or not calendar.get("tradingDays"):
        return None
    ed = str(execution_date)[:10]
    earlier = sorted(d for d in calendar["tradingDays"] if d < ed)
    return earlier[-1] if earlier else None


def _parse_aware(ts):
    """tz-aware ISO-8601만 허용. naive(offset 없음)/parse 실패 → None."""
    if not ts or not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt


def default_market_open_at(execution_date: str, tz_offset: str = "+09:00") -> str:
    """운영 입력용 기본 개장시각(KST 09:00). official_dry_run 내부엔 하드코딩하지 않고,
    호출자가 주입(override 가능)한다."""
    return f"{str(execution_date)[:10]}T09:00:00{tz_offset}"


def _dr_base(execution_date, *, signal_as_of=None, universe_base_date=None,
             ranking_generated_at=None, execution_market_open_at=None, prev_td=None) -> dict:
    """DRY-RUN 공통 출력 골격(시점 필드 포함). productionWrite/persist 전부 0."""
    return {
        "validationOnly": True,
        "signalAsOfDate": signal_as_of,
        "universeBaseDate": universe_base_date,
        "rankingGeneratedAt": ranking_generated_at,
        "executionDate": execution_date,
        "executionMarketOpenAt": execution_market_open_at,
        "previousKrxTradingDay": prev_td,
        "operationDate": execution_date,   # deprecated 별칭: executionDate를 그대로 비춤
        "officialStartDatePersisted": False,
        "productionWriteCount": 0,
        "publicCopyCount": 0,
        "dailyRunConnected": False,
    }


def _blocked_dr(execution_date, status, reason, *, signal_as_of=None, universe_base_date=None,
                ranking_generated_at=None, execution_market_open_at=None, prev_td=None,
                timing_ok=False) -> dict:
    out = _dr_base(execution_date, signal_as_of=signal_as_of, universe_base_date=universe_base_date,
                   ranking_generated_at=ranking_generated_at,
                   execution_market_open_at=execution_market_open_at, prev_td=prev_td)
    out.update({
        "blocked": True, "runStatus": status, "runReason": reason,
        "timingValidationPassed": timing_ok, "lookAheadValidationPassed": False,
        "proposedSequence": 0, "proposedBatchId": None, "selectedTop10": None,
        "allocatedCapital": None, "totalInvested": None, "cashReserve": None,
    })
    return out


# ===== PILOT(2026-06-08) 타이밍 감사 (read-only; write 0; 실제 PILOT 파일 미수정) =====

def pilot_timing_audit() -> dict:
    """45-E4 실증 결과를 분리 감사 규칙으로만 기록한다.
    rankings.json은 baseDate=2026-06-08 라벨만 갖지만, marketCap이 2026-05-29 universe와
    정확히 일치 → 실제 신호일은 2026-05-29(종가), 체결은 2026-06-08(시가). look-ahead 아님.
    legacy 문서(baseDate-only)는 OFFICIAL 경로에선 BLOCKED_MISSING_SIGNAL_METADATA로 차단되고,
    오직 이 분리 감사로만 읽는다. 실제 PILOT 파일은 수정/저장하지 않는다."""
    return {
        "auditScope": "PILOT_2026-06-08",
        "legacyRankingBaseDate": PILOT_LEGACY_RANKING_BASE_DATE,
        "auditedSignalAsOfDate": PILOT_AUDITED_SIGNAL_AS_OF_DATE,
        "executionDate": PILOT_AUDIT_EXECUTION_DATE,
        "executionPriceSource": "pykrx_open",
        "timingAuditStatus": PILOT_TIMING_AUDIT_PASS,
        "officialPathReadable": False,      # OFFICIAL 경로에선 legacy 문서 차단
        "auditOnly": True,
        "productionWriteCount": 0, "publicCopyCount": 0, "officialStartDatePersisted": False,
    }


# ===== OFFICIAL DRY-RUN (read-only; 코어 engine 호출; persist 0) =====

def official_dry_run(execution_date: str = None, rankings_doc: dict = None, open_prices: dict = None,
                     calendar=None, *, execution_market_open_at: str = None,
                     initial_capital: int = E.INITIAL_CAPITAL,
                     operation_date: str = None) -> dict:
    """신호일(signalAsOfDate, 전일 종가 기준)과 체결일(executionDate, 익일 시가 매수)을 분리하고
    look-ahead를 구조적으로 차단한다. 검증·입력 조립만 wrapper가 담당하고, 수량배분/FIFO/자금/원장은
    전부 engine.plan_official_day 호출(로직 복제 금지). production 쓰기·persist 0."""
    open_prices = open_prices or {}
    doc = rankings_doc or {}

    # 0) deprecated operationDate: executionDate 별칭으로만 허용. 충돌 시 명시적 에러(조용한 오작동 차단).
    if operation_date is not None:
        if execution_date is None:
            execution_date = operation_date
        elif str(operation_date) != str(execution_date):
            return _blocked_dr(str(execution_date), BLOCKED_EXECUTION_DATE_MISMATCH,
                               f"deprecated operationDate {operation_date} != executionDate {execution_date}")
    execution_date = str(execution_date)[:10] if execution_date is not None else None

    signal_as_of = doc.get("signalAsOfDate")
    ranking_generated_at = doc.get("rankingGeneratedAt")
    universe_base_date = doc.get("universeBaseDate")
    formula_version = doc.get("formulaVersion")
    doc_execution_date = doc.get("executionDate")

    def blk(status, reason, *, prev_td=None, timing_ok=False):
        return _blocked_dr(execution_date, status, reason, signal_as_of=signal_as_of,
                           universe_base_date=universe_base_date,
                           ranking_generated_at=ranking_generated_at,
                           execution_market_open_at=execution_market_open_at,
                           prev_td=prev_td, timing_ok=timing_ok)

    # 1) ranking 문서가 선언한 executionDate가 인자와 다르면 차단(라벨 불일치).
    if doc_execution_date not in (None, "") and str(doc_execution_date)[:10] != execution_date:
        return blk(BLOCKED_EXECUTION_DATE_MISMATCH,
                   f"ranking.executionDate {doc_execution_date} != executionDate {execution_date}")

    # 2) 신호 메타데이터 필수. legacy(baseDate-only) 문서는 여기서 차단(baseDate를 executionDate로 자동등치 금지).
    missing = [k for k in REQUIRED_SIGNAL_FIELDS if doc.get(k) in (None, "")]
    if missing:
        return blk(BLOCKED_MISSING_SIGNAL_METADATA, f"missing signal metadata: {missing}")

    # 2b) ranking이 자체 stamping한 signalAsOfDate가 있으면 문서 signalAsOfDate와 일치해야 함.
    ranking_signal = doc.get("rankingSignalAsOfDate")
    if ranking_signal not in (None, "") and str(ranking_signal) != str(signal_as_of):
        return blk(BLOCKED_SIGNAL_DATE_MISMATCH,
                   f"rankingSignalAsOfDate {ranking_signal} != signalAsOfDate {signal_as_of}")

    # 3) rankingGeneratedAt: tz-aware ISO-8601 필수(naive/파싱실패 차단).
    gen_dt = _parse_aware(ranking_generated_at)
    if gen_dt is None:
        return blk(BLOCKED_MISSING_SIGNAL_METADATA,
                   f"rankingGeneratedAt must be tz-aware ISO-8601: {ranking_generated_at!r}")

    # 4) executionMarketOpenAt: 주입 필수(하드코딩 09:00 금지) + tz-aware.
    open_dt = _parse_aware(execution_market_open_at)
    if open_dt is None:
        return blk(BLOCKED_MISSING_SIGNAL_METADATA,
                   f"executionMarketOpenAt must be injected tz-aware ISO-8601: {execution_market_open_at!r}")

    # 5) rankingGeneratedAt < executionMarketOpenAt (== 또는 이후면 차단 = look-ahead).
    if gen_dt >= open_dt:
        return blk(BLOCKED_RANKING_GENERATED_AFTER_OPEN,
                   f"rankingGeneratedAt {ranking_generated_at} >= executionMarketOpenAt {execution_market_open_at}",
                   timing_ok=False)

    # 여기까지 시점 메타데이터/시각 검증 통과
    timing_ok = True

    # 6) 거래일 분류: 체결일이 거래일일 때만 신호일↔체결일 관계 검증.
    #    NON_TRADING / NO_CALENDAR 는 engine이 판정하도록 위임.
    cls = E.classify_trading_day(execution_date, calendar) if calendar else "NO_CALENDAR"
    prev_td = previous_krx_trading_day(execution_date, calendar)
    if cls == "TRADING":
        # 6a) signalAsOfDate < executionDate (동일/미래 신호 차단)
        if str(signal_as_of) >= execution_date:
            return blk(BLOCKED_SIGNAL_DATE_MISMATCH,
                       f"signalAsOfDate {signal_as_of} must be < executionDate {execution_date}",
                       prev_td=prev_td, timing_ok=timing_ok)
        # 6b) universeBaseDate == signalAsOfDate
        if str(universe_base_date) != str(signal_as_of):
            return blk(BLOCKED_UNIVERSE_DATE_MISMATCH,
                       f"universeBaseDate {universe_base_date} != signalAsOfDate {signal_as_of}",
                       prev_td=prev_td, timing_ok=timing_ok)
        # 6c) signalAsOfDate == previousKrxTradingDay(executionDate) (사이에 거래일 없음)
        if str(signal_as_of) != str(prev_td):
            return blk(BLOCKED_SIGNAL_NOT_PREVIOUS_TRADING_DAY,
                       f"signalAsOfDate {signal_as_of} != previousKrxTradingDay {prev_td} of {execution_date}",
                       prev_td=prev_td, timing_ok=timing_ok)

    # 7) ranking 코드 정규화 + 중복/공백 검사(engine은 <10만 검사)
    ranking = _ranking_from_doc(doc)
    codes = [r.get("code") for r in ranking]
    if len(set(codes)) != len(codes) or any(c in (None, "") for c in codes):
        return blk(E.BLOCKED_MISSING_RANKING, "duplicate/empty codes in top10",
                   prev_td=prev_td, timing_ok=timing_ok)

    # 8) 통과한 시점 메타데이터만 engine에 주입(코어는 추가 기록만; 검증/계산 안 함).
    timing = {
        "signalAsOfDate": signal_as_of,
        "rankingGeneratedAt": ranking_generated_at,
        "executionDate": execution_date,
        "executionMarketOpenAt": execution_market_open_at,
        "executionPriceSource": E.PRICE_SOURCE_TRADE,
        "lookAheadValidationPassed": True,
    }

    # === 코어 호출 (수량배분/FIFO/원장/BLOCKED_MISSING_OPEN_PRICE/RANKING/CALENDAR/NON_TRADING 전부 engine) ===
    state = E.empty_official_state(initial_capital=initial_capital)
    cash_before = state["officialAvailableCash"]
    new_state, result = E.plan_official_day(state, execution_date, ranking,
                                            open_prices, open_prices, calendar, timing=timing)
    # new_state는 *버리고* 저장하지 않는다(officialStartDate 등 in-memory만).

    completed = result["runStatus"] in (E.COMPLETED, E.ALREADY_PROCESSED)
    blocked = not completed
    look_ahead_ok = completed and (cls == "TRADING")

    new_batch = new_state["batches"][-1] if (completed and new_state["batches"]) else None
    buy_first = new_state["buyLedger"][-result.get("buyCount", 0)] if (new_batch and result.get("buyCount")) else None

    out = _dr_base(execution_date, signal_as_of=signal_as_of, universe_base_date=universe_base_date,
                   ranking_generated_at=ranking_generated_at,
                   execution_market_open_at=execution_market_open_at, prev_td=prev_td)
    out.update({
        "marketOpen": result.get("marketOpen"),
        "blocked": blocked, "runStatus": result["runStatus"], "runReason": result.get("runReason"),
        "timingValidationPassed": timing_ok, "lookAheadValidationPassed": look_ahead_ok,
        "proposedSequence": new_state["officialSequence"] if completed else 0,
        "proposedBatchId": result.get("buyBatchId"),
        "selectedTop10": result.get("plan"),
        "allocatedCapital": result.get("allocatedCapital"),
        "totalInvested": result.get("totalInvested"),
        "cashReserve": result.get("cashReserve"),
        "officialAvailableCashBefore": cash_before,
        "officialAvailableCashAfterPreview": new_state["officialAvailableCash"],
    })
    if new_batch:
        out["batchPreview"] = {k: new_batch.get(k) for k in
                               ("batchId", "sequence", "allocatedCapital", "totalInvested",
                                "cashReserve", "buyCount", "signalAsOfDate", "rankingGeneratedAt",
                                "executionDate", "executionMarketOpenAt", "executionPriceSource")}
    if buy_first:
        out["buyLedgerPreview"] = {k: buy_first.get(k) for k in
                                   ("tradeId", "code", "executionPrice", "quantity", "signalAsOfDate",
                                    "rankingGeneratedAt", "executionDate", "executionPriceSource")}
    if completed:
        out["dailyLedgerPreview"] = {k: result.get(k) for k in
                                     ("signalAsOfDate", "executionDate", "rankingGeneratedAt",
                                      "lookAheadValidationPassed")}
    return out


def render(preview: dict, official: dict) -> str:
    lines = ["[PILOT PREVIEW]"]
    for k in ("sourceDate", "pilotBatchId", "itemLotCount", "totalInvested",
              "coreFieldsUnchanged", "officialCapitalImpact", "writeCount", "runStatus", "runReason"):
        if k in preview:
            lines.append(f"  {k} = {preview[k]}")
    lines.append("[OFFICIAL DRY-RUN]")
    for k in ("validationOnly", "signalAsOfDate", "universeBaseDate", "rankingGeneratedAt",
              "executionDate", "executionMarketOpenAt", "previousKrxTradingDay", "marketOpen",
              "officialStartDatePersisted", "blocked", "runStatus", "runReason",
              "timingValidationPassed", "lookAheadValidationPassed",
              "proposedSequence", "proposedBatchId", "allocatedCapital", "totalInvested",
              "cashReserve", "officialAvailableCashBefore", "officialAvailableCashAfterPreview",
              "productionWriteCount", "publicCopyCount", "dailyRunConnected"):
        if k in official:
            lines.append(f"  {k} = {official[k]}")
    for p in (official.get("selectedTop10") or []):
        lines.append(f"    - rank? {p.get('code')} {p.get('name')} open={p.get('openPrice')} "
                     f"qty={p.get('quantity')} amount={p.get('amount')}")
    return "\n".join(lines)


def run_real_dry_run(execution_date: str = "2026-06-16") -> dict:
    """실제 read-only dry-run 1회. production 쓰기 0. (pykrx 읽기 전용).
    체결일 개장시각은 주입(default_market_open_at, KST 09:00)하되 하드코딩하지 않는다."""
    preview = pilot_preview()  # 실제 06-08 lots
    rankings = _read_json(RANKINGS_PATH) or {}
    cal = build_krx_calendar(execution_date)
    market_open = (E.classify_trading_day(execution_date, cal) == "TRADING") if cal else None
    # legacy ranking(신호 메타데이터 없음) → BLOCKED_MISSING_SIGNAL_METADATA (시가/추가조회 불필요)
    official = official_dry_run(execution_date, rankings, open_prices={}, calendar=cal,
                                execution_market_open_at=default_market_open_at(execution_date))
    official["krxTradingDay"] = market_open
    return {"preview": preview, "official": official, "pilotTimingAudit": pilot_timing_audit()}


def run_engine_validation_dry_run() -> dict:
    """legacy 06-08 rankings(baseDate-only)는 신호 메타데이터가 없어 OFFICIAL 경로에서 차단됨을 보인다.
    타이밍 진실은 pilot_timing_audit()(분리 감사)로만 읽는다. persist 0·공식 운용 아님."""
    rankings = _read_json(RANKINGS_PATH) or {}
    top = rankings.get("todayMagicRankingTop10") or []
    open_prices = {str(t.get("code")): float(t.get("buyOpenPrice"))
                   for t in top if t.get("buyOpenPrice")}
    cal = build_krx_calendar("2026-06-08")
    official = official_dry_run("2026-06-08", rankings, open_prices, cal,
                                execution_market_open_at=default_market_open_at("2026-06-08"))
    official["note"] = ("legacy baseDate-only ranking → BLOCKED_MISSING_SIGNAL_METADATA; "
                        "timing truth via pilot_timing_audit() only — not official, not persisted")
    return official


if __name__ == "__main__":
    res = run_real_dry_run("2026-06-16")
    print(render(res["preview"], res["official"]))
    print(f"  krxTradingDay(2026-06-16) = {res['official'].get('krxTradingDay')}")
    print("\n[PILOT TIMING AUDIT (read-only, separate legacy audit)]")
    for k, v in res["pilotTimingAudit"].items():
        print(f"  {k} = {v}")
    print("\n[ENGINE VALIDATION DRY-RUN @2026-06-08 (legacy → blocked)]")
    ev = run_engine_validation_dry_run()
    print(render({}, ev))
    print(f"  note = {ev.get('note')}")
