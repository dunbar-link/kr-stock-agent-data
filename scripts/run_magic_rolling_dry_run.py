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
BLOCKED_STALE_RANKING = "BLOCKED_STALE_RANKING"
PILOT_PREVIEW_OK = "PILOT_PREVIEW_OK"


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
    """pykrx.stock.get_business_days로 해당 월 실거래일을 받아 engine.make_calendar 생성.
    실패/빈 결과 → None(=engine이 BLOCKED_NO_TRADING_CALENDAR 처리). pykrx_stock 주입 시 테스트 mock."""
    try:
        if pykrx_stock is None:
            from pykrx import stock as pykrx_stock  # lazy: import 시 network 회피
        y, m = int(operation_date[:4]), int(operation_date[5:7])
        bdays = pykrx_stock.get_business_days(y, m)
        days = []
        for d in (bdays or []):
            s = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
            days.append(s)
        if not days:
            return None
        return E.make_calendar(days)
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


# ===== OFFICIAL DRY-RUN (read-only; 코어 engine 호출; persist 0) =====

def official_dry_run(operation_date: str, rankings_doc: dict, open_prices: dict,
                     calendar, *, initial_capital: int = E.INITIAL_CAPITAL) -> dict:
    open_prices = open_prices or {}
    ranking_base = str((rankings_doc or {}).get("baseDate") or "")

    # (wrapper) ranking 기준일 == operationDate
    if ranking_base != operation_date:
        return {"validationOnly": True, "operationDate": operation_date,
                "rankingBaseDate": ranking_base, "blocked": True,
                "runStatus": BLOCKED_STALE_RANKING,
                "runReason": f"rankingBaseDate {ranking_base} != operationDate {operation_date}",
                "officialStartDatePersisted": False, "productionWriteCount": 0,
                "publicCopyCount": 0, "dailyRunConnected": False}

    ranking = _ranking_from_doc(rankings_doc)
    # (wrapper) 중복 코드 검사(engine은 <10만 검사)
    codes = [r.get("code") for r in ranking]
    if len(set(codes)) != len(codes) or any(c in (None, "") for c in codes):
        return {"validationOnly": True, "operationDate": operation_date, "blocked": True,
                "runStatus": E.BLOCKED_MISSING_RANKING, "runReason": "duplicate/empty codes in top10",
                "officialStartDatePersisted": False, "productionWriteCount": 0,
                "publicCopyCount": 0, "dailyRunConnected": False}

    # === 코어 호출 (수량배분/FIFO/원장/BLOCKED_MISSING_OPEN_PRICE/RANKING/CALENDAR 전부 engine) ===
    state = E.empty_official_state(initial_capital=initial_capital)
    cash_before = state["officialAvailableCash"]
    new_state, result = E.plan_official_day(state, operation_date, ranking,
                                            open_prices, open_prices, calendar)
    # new_state는 *버리고* 저장하지 않는다(officialStartDate 등 in-memory만).

    blocked = result["runStatus"] not in (E.COMPLETED, E.ALREADY_PROCESSED)
    new_batch = new_state["batches"][-1] if (not blocked and new_state["batches"]) else None
    out = {
        "validationOnly": True, "operationDate": operation_date,
        "rankingBaseDate": ranking_base, "marketOpen": (result.get("marketOpen")),
        "blocked": blocked, "runStatus": result["runStatus"], "runReason": result.get("runReason"),
        "officialStartDatePersisted": False,
        "proposedSequence": new_state["officialSequence"] if not blocked else 0,
        "proposedBatchId": result.get("buyBatchId"),
        "selectedTop10": result.get("plan"),
        "allocatedCapital": result.get("allocatedCapital"),
        "totalInvested": result.get("totalInvested"),
        "cashReserve": result.get("cashReserve"),
        "officialAvailableCashBefore": cash_before,
        "officialAvailableCashAfterPreview": new_state["officialAvailableCash"],
        "productionWriteCount": 0, "publicCopyCount": 0, "dailyRunConnected": False,
    }
    if new_batch:
        out["batchPreview"] = {k: new_batch.get(k) for k in
                               ("batchId", "sequence", "allocatedCapital", "totalInvested",
                                "cashReserve", "buyCount")}
    return out


def render(preview: dict, official: dict) -> str:
    lines = ["[PILOT PREVIEW]"]
    for k in ("sourceDate", "pilotBatchId", "itemLotCount", "totalInvested",
              "coreFieldsUnchanged", "officialCapitalImpact", "writeCount", "runStatus", "runReason"):
        if k in preview:
            lines.append(f"  {k} = {preview[k]}")
    lines.append("[OFFICIAL DRY-RUN]")
    for k in ("validationOnly", "operationDate", "rankingBaseDate", "marketOpen",
              "officialStartDatePersisted", "blocked", "runStatus", "runReason",
              "proposedSequence", "proposedBatchId", "allocatedCapital", "totalInvested",
              "cashReserve", "officialAvailableCashBefore", "officialAvailableCashAfterPreview",
              "productionWriteCount", "publicCopyCount", "dailyRunConnected"):
        if k in official:
            lines.append(f"  {k} = {official[k]}")
    for p in (official.get("selectedTop10") or []):
        lines.append(f"    - rank? {p.get('code')} {p.get('name')} open={p.get('openPrice')} "
                     f"qty={p.get('quantity')} amount={p.get('amount')}")
    return "\n".join(lines)


def run_real_dry_run(operation_date: str = "2026-06-15") -> dict:
    """실제 read-only dry-run 1회. production 쓰기 0. (pykrx 읽기 전용)."""
    preview = pilot_preview()  # 실제 06-08 lots
    rankings = _read_json(RANKINGS_PATH) or {}
    cal = build_krx_calendar(operation_date)
    market_open = (E.classify_trading_day(operation_date, cal) == "TRADING") if cal else None
    # 06-15는 ranking(06-08)과 기준일 불일치 → STALE에서 BLOCK(시가/추가조회 불필요)
    official = official_dry_run(operation_date, rankings,
                                open_prices={}, calendar=cal)
    official["krxTradingDay"] = market_open
    return {"preview": preview, "official": official}


def run_engine_validation_dry_run() -> dict:
    """엔진 검증용: 06-08 실데이터(랭킹·저장된 pykrx_open 시가)로 COMPLETED 미리보기.
    공식 운용 아님·persist 0·소급 매수 아님."""
    rankings = _read_json(RANKINGS_PATH) or {}
    top = rankings.get("todayMagicRankingTop10") or []
    open_prices = {str(t.get("code")): float(t.get("buyOpenPrice"))
                   for t in top if t.get("buyOpenPrice")}
    cal = build_krx_calendar("2026-06-08")
    official = official_dry_run("2026-06-08", rankings, open_prices, cal)
    official["note"] = "engine validation only — NOT official day1, NOT persisted, NOT a backdated buy"
    return official


if __name__ == "__main__":
    res = run_real_dry_run("2026-06-15")
    print(render(res["preview"], res["official"]))
    print(f"  krxTradingDay(2026-06-15) = {res['official'].get('krxTradingDay')}")
    print("\n[ENGINE VALIDATION DRY-RUN @2026-06-08 (read-only, not official)]")
    ev = run_engine_validation_dry_run()
    print(render({}, ev))
    print(f"  note = {ev.get('note')}")
