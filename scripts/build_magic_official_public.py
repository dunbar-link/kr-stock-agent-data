#!/usr/bin/env python3
"""와바바 마법공식 — OFFICIAL canonical 장부 → public 표시 모델 *순수 매핑* (Phase 45-E8.1).

- canonical(magic-formula-official-state.json)을 *read-only*로 읽어 신규 public 키 3개를 파생한다.
  magicOfficialSummary · magicOfficialPortfolio · magicOfficialTradeDays
- 어떤 파일도 쓰지 않는다(productionWriteCount=0). PILOT은 절대 혼합하지 않는다(state.pilot 제외).
- 거래 lot은 합치지 않는다(원장 보존). 종목별 보유 집계는 화면용 *파생*일 뿐 canonical 원장이 아니다.
- 매핑 전 무결성 검증(MappingValidationError). 부분 결과 반환 금지.

금액 표기 정책(결정적):
- 원화 금액/가격/수량/평가액은 정수(원). KRX 가격·수량이 정수라 가치도 정수.
- averageBuyPrice·returnRate·realizedReturn·cumulativeReturn 은 소수 2자리 반올림.
- 보존식 허용오차 _EPS = 0.5원(정수 원화의 float 반올림 잔차 흡수용; 실제 차이는 0).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
OFFICIAL_STATE_PATH = ROOT / "magic-formula-official-state.json"

PUBLIC_SCHEMA_VERSION = "magic-official-public-v1"
OFFICIAL_PUBLIC_KEYS = ("magicOfficialSummary", "magicOfficialPortfolio", "magicOfficialTradeDays")
OFFICIAL = "OFFICIAL"
_EPS = 0.5


class MappingValidationError(Exception):
    pass


def _won(v) -> int:
    return int(round(float(v)))


def _r2(v) -> float:
    return round(float(v), 2)


def _close(a, b) -> bool:
    return abs(float(a) - float(b)) <= _EPS


def _by_date_buys(st, date):
    return [e for e in st["buyLedger"] if str(e.get("date")) == date]


def _by_date_sells(st, date):
    return [e for e in st["sellLedger"] if str(e.get("date")) == date]


# ===== 무결성 검증 =====

def validate_canonical(st: dict) -> None:
    def err(m):
        raise MappingValidationError(m)

    # --- 기본 ---
    if st.get("operationMode") != OFFICIAL:
        err(f"operationMode != OFFICIAL ({st.get('operationMode')})")
    if not st.get("officialStartDate"):
        err("officialStartDate missing")
    seq = st.get("officialSequence")
    if not isinstance(seq, int) or seq < 1:
        err(f"officialSequence must be int >= 1 ({seq})")
    cal = st.get("officialTradingCalendar") or []
    if sorted(cal) != list(cal):
        err("officialTradingCalendar not ascending")
    if len(set(cal)) != len(cal):
        err("officialTradingCalendar has duplicate dates")
    if len(cal) != seq:
        err(f"officialTradingCalendar len {len(cal)} != officialSequence {seq}")

    batches = st.get("batches") or []
    item_lots = st.get("itemLots") or []
    buy_ledger = st.get("buyLedger") or []
    sell_ledger = st.get("sellLedger") or []
    daily = st.get("dailyLedger") or []

    batch_ids = [b.get("batchId") for b in batches]
    if len(set(batch_ids)) != len(batch_ids):
        err("duplicate batchId")
    lot_ids = [l.get("lotId") for l in item_lots]
    if len(set(lot_ids)) != len(lot_ids):
        err("duplicate lotId")
    lot_set = set(lot_ids)
    bt = [e.get("tradeId") for e in buy_ledger]
    if len(set(bt)) != len(bt):
        err("duplicate BUY tradeId")
    stids = [e.get("tradeId") for e in sell_ledger]
    if len(set(stids)) != len(stids):
        err("duplicate SELL tradeId")
    for e in buy_ledger:
        if e.get("lotId") not in lot_set:
            err(f"buyLedger lotId not in itemLots: {e.get('lotId')}")
    for e in sell_ledger:
        if e.get("lotId") not in lot_set:
            err(f"sellLedger lotId not in itemLots: {e.get('lotId')}")
    # batch.itemLotIds ↔ itemLots 일치
    lots_by_batch = {}
    for l in item_lots:
        lots_by_batch.setdefault(l.get("batchId"), set()).add(l.get("lotId"))
    for b in batches:
        ids = b.get("itemLotIds") or []
        if len(set(ids)) != len(ids):
            err(f"batch {b.get('batchId')} duplicate itemLotIds")
        if set(ids) != lots_by_batch.get(b.get("batchId"), set()):
            err(f"batch {b.get('batchId')} itemLotIds != itemLots membership")
        if b.get("operationMode") != OFFICIAL:
            err(f"batch {b.get('batchId')} operationMode != OFFICIAL (PILOT 혼입?)")

    # --- PILOT 격리: pilot lotId가 공식 itemLots/ledger에 없어야 함 ---
    pilot = st.get("pilot") or {}
    pilot_lot_ids = {l.get("lotId") for l in (pilot.get("itemLots") or [])}
    if pilot_lot_ids & lot_set:
        err(f"PILOT lotId leaked into official itemLots: {sorted(pilot_lot_ids & lot_set)}")
    if pilot.get("pilotBatchId") in set(batch_ids):
        err("PILOT batchId leaked into official batches")

    # --- 배치 검증 ---
    for b in batches:
        bid = b.get("batchId")
        bbuys = [e for e in buy_ledger if e.get("batchId") == bid]
        if b.get("buyCount") != 10 or len(b.get("itemLotIds") or []) != 10 or len(bbuys) != 10:
            err(f"batch {bid} BUY != 10 (buyCount {b.get('buyCount')}, lots {len(b.get('itemLotIds') or [])}, ledger {len(bbuys)})")
        if not _close(b.get("totalInvested"), sum(float(e["amount"]) for e in bbuys)):
            err(f"batch {bid} totalInvested != sum BUY amount")
        if not _close(float(b.get("allocatedCapital")), float(b.get("totalInvested")) + float(b.get("cashReserve"))):
            err(f"batch {bid} allocatedCapital != totalInvested + cashReserve")
        if b.get("status") == "CLOSED":
            bsells = [e for e in sell_ledger if e.get("batchId") == bid]
            if len(bsells) != 10:
                err(f"CLOSED batch {bid} SELL != 10 ({len(bsells)})")
        src = b.get("rolloverSourceBatchId")
        if src:
            srcb = next((x for x in batches if x.get("batchId") == src), None)
            if srcb is None or srcb.get("status") != "CLOSED":
                err(f"batch {bid} rolloverSourceBatchId {src} not a CLOSED batch")

    # --- 거래일별 검증 ---
    dates = [d.get("date") for d in daily]
    if len(set(dates)) != len(dates):
        err("dailyLedger duplicate dates")
    completed = sorted([d for d in daily if d.get("runStatus") == "COMPLETED"], key=lambda d: d["date"])
    for i, d in enumerate(completed, 1):
        if d.get("officialSequence") != i:
            err(f"officialSequence discontinuity at {d.get('date')}: got {d.get('officialSequence')} expect {i}")
    if completed and completed[-1].get("officialSequence") != seq:
        err(f"latest COMPLETED sequence {completed[-1].get('officialSequence')} != officialSequence {seq}")
    for d in daily:
        date = d.get("date")
        buys, sells = _by_date_buys(st, date), _by_date_sells(st, date)
        if d.get("buyCount") != len(buys):
            err(f"{date} dailyLedger.buyCount {d.get('buyCount')} != BUY rows {len(buys)}")
        if d.get("sellCount") != len(sells):
            err(f"{date} dailyLedger.sellCount {d.get('sellCount')} != SELL rows {len(sells)}")
        if not _close(d.get("totalBuyAmount", 0), sum(float(e["amount"]) for e in buys)):
            err(f"{date} totalBuyAmount != sum BUY amount")
        if not _close(d.get("totalSellAmount", 0), sum(float(e["amount"]) for e in sells)):
            err(f"{date} totalSellAmount != sum SELL amount")
        if not _close(d.get("realizedProfit", 0), sum(float(e.get("realizedProfit") or 0) for e in sells)):
            err(f"{date} realizedProfit != sum SELL realizedProfit")
        if d.get("buyBatchId") and d.get("buyBatchId") not in set(batch_ids):
            err(f"{date} buyBatchId not in batches")
        if d.get("sellBatchId") and d.get("sellBatchId") not in set(batch_ids):
            err(f"{date} sellBatchId not in batches")
        # 자산 보존(거래일별)
        if not _close(float(d.get("officialAvailableCash", 0)) + float(d.get("batchCashReserveTotal", 0)),
                      float(d.get("totalCash", 0))):
            err(f"{date} officialAvailableCash + batchCashReserveTotal != totalCash")
        if not _close(float(d.get("totalCash", 0)) + float(d.get("holdingsMarketValue", 0)),
                      float(d.get("totalAsset", 0))):
            err(f"{date} totalCash + holdingsMarketValue != totalAsset")

    # --- 최신 dailyLedger ↔ 최신 evaluationSnapshot 자산 일치 ---
    evals = st.get("evaluationSnapshots") or []
    if completed and evals:
        latest_d = completed[-1]
        latest_e = sorted(evals, key=lambda e: e["date"])[-1]
        if not (_close(latest_d.get("holdingsMarketValue"), latest_e.get("holdingsMarketValue"))
                and _close(latest_d.get("totalAsset"), latest_e.get("totalAsset"))
                and _close(latest_d.get("totalCash"), latest_e.get("cash"))):
            err("latest dailyLedger asset values != latest evaluationSnapshot")


# ===== 매핑 =====

def _latest_completed(st):
    completed = [d for d in (st.get("dailyLedger") or []) if d.get("runStatus") == "COMPLETED"]
    return sorted(completed, key=lambda d: d["date"])[-1] if completed else None


def build_summary(st: dict, sha: str) -> dict:
    batches = st.get("batches") or []
    item_lots = st.get("itemLots") or []
    latest = _latest_completed(st)
    open_batches = [b for b in batches if b.get("operationMode") == OFFICIAL and b.get("status") == "OPEN"]
    reserve_total = round(sum(float(b.get("cashReserve") or 0) for b in open_batches), 2)
    avail = float(st.get("officialAvailableCash") or 0)
    return {
        "schemaVersion": PUBLIC_SCHEMA_VERSION,
        "formulaVersion": st.get("formulaVersion"),
        "officialStartDate": st.get("officialStartDate"),
        "officialSequence": st.get("officialSequence"),
        "dataDate": latest["date"] if latest else None,
        "latestTradingDate": latest["date"] if latest else None,
        "openBatchCount": len(open_batches),
        "openItemLotCount": len([l for l in item_lots if l.get("status") == "OPEN"]),
        "closedBatchCount": len([b for b in batches if b.get("status") == "CLOSED"]),
        "totalBuyCount": len(st.get("buyLedger") or []),
        "totalSellCount": len(st.get("sellLedger") or []),
        "officialAvailableCash": _won(avail),
        "batchCashReserveTotal": _won(reserve_total),
        "totalCash": _won(avail + reserve_total),
        "holdingsMarketValue": _won(latest["holdingsMarketValue"]) if latest else 0,
        "totalAsset": _won(latest["totalAsset"]) if latest else 0,
        "cumulativeReturn": _r2(latest["cumulativeReturn"]) if latest else 0.0,
        "pilotExcluded": True,
        "sourceStateSha256": sha,
    }


def build_portfolio(st: dict) -> dict:
    """OPEN lot을 code로 화면용 집계(원장 lot은 보존). 최신 evaluationSnapshot.perLot과 lotId JOIN."""
    evals = st.get("evaluationSnapshots") or []
    per_lot = {}
    if evals:
        latest_e = sorted(evals, key=lambda e: e["date"])[-1]
        per_lot = {p.get("lotId"): p for p in (latest_e.get("perLot") or [])}
    groups = {}
    for l in st.get("itemLots") or []:
        if l.get("status") != "OPEN":
            continue
        g = groups.setdefault(l["code"], {"code": l["code"], "name": l.get("name"),
                                          "lots": [], "totalQuantity": 0, "totalInvested": 0.0,
                                          "marketValue": 0.0, "currentPrices": set()})
        g["lots"].append(l)
        g["totalQuantity"] += int(l["quantity"])
        g["totalInvested"] += float(l["investedAmount"])
        pl = per_lot.get(l["lotId"])
        if pl is not None:
            g["marketValue"] += float(pl.get("marketValue") or 0)
            if pl.get("currentPrice") is not None:
                g["currentPrices"].add(float(pl["currentPrice"]))
    holdings = []
    for code in sorted(groups):
        g = groups[code]
        if len(g["currentPrices"]) > 1:
            raise MappingValidationError(f"code {code} has differing currentPrice across lots: {sorted(g['currentPrices'])}")
        inv = round(g["totalInvested"], 2)
        mv = round(g["marketValue"], 2)
        cur = next(iter(g["currentPrices"])) if g["currentPrices"] else None
        unreal = round(mv - inv, 2)
        holdings.append({
            "code": code, "name": g["name"], "openLotCount": len(g["lots"]),
            "totalQuantity": g["totalQuantity"], "totalInvested": _won(inv),
            "averageBuyPrice": _r2(inv / g["totalQuantity"]) if g["totalQuantity"] else 0.0,
            "currentPrice": _won(cur) if cur is not None else None,
            "marketValue": _won(mv), "unrealizedProfit": _won(unreal),
            "returnRate": _r2(unreal / inv * 100) if inv else 0.0,
        })
    return {"holdings": holdings}


def build_trade_days(st: dict) -> list:
    lot_by_id = {l.get("lotId"): l for l in (st.get("itemLots") or [])}
    days = []
    for d in sorted(st.get("dailyLedger") or [], key=lambda x: x["date"], reverse=True):
        date = d["date"]
        buys_raw = _by_date_buys(st, date)
        buys = []
        for e in buys_raw:
            rs = e.get("rankSnapshot") or {}
            buys.append({
                "tradeId": e.get("tradeId"), "batchId": e.get("batchId"), "lotId": e.get("lotId"),
                "rank": rs.get("rank"), "code": e.get("code"), "name": e.get("name"),
                "executionPrice": _won(e["executionPrice"]), "quantity": int(e["quantity"]),
                "amount": _won(e["amount"]), "signalAsOfDate": e.get("signalAsOfDate"),
                "executionDate": e.get("executionDate"), "priceSource": e.get("priceSource"),
            })
        buys.sort(key=lambda b: (b["rank"] if b["rank"] is not None else 1e9, b["code"]))
        sells_raw = _by_date_sells(st, date)
        sells = []
        for e in sells_raw:
            lot = lot_by_id.get(e.get("lotId"))
            if lot is None:
                raise MappingValidationError(f"SELL lotId not joinable: {e.get('lotId')}")
            sells.append({
                "tradeId": e.get("tradeId"), "batchId": e.get("batchId"), "lotId": e.get("lotId"),
                "code": e.get("code"), "name": e.get("name"),
                "originalBuyDate": lot.get("buyDate"), "originalBuyPrice": _won(lot["buyOpenPrice"]),
                "executionPrice": _won(e["executionPrice"]), "quantity": int(e["quantity"]),
                "amount": _won(e["amount"]), "realizedProfit": _won(e.get("realizedProfit") or 0),
                "realizedReturn": _r2(e.get("realizedReturn") or 0), "holdingTradingDays": e.get("holdingTradingDays"),
                "sellReason": e.get("sellReason"), "executionDate": e.get("executionDate", date),
                "priceSource": e.get("priceSource"),
            })
        sells.sort(key=lambda s: (s["code"], s["lotId"]))
        days.append({
            "date": date, "officialSequence": d.get("officialSequence"), "runStatus": d.get("runStatus"),
            "buyCount": d.get("buyCount"), "sellCount": d.get("sellCount"),
            "totalBuyAmount": _won(d.get("totalBuyAmount") or 0), "totalSellAmount": _won(d.get("totalSellAmount") or 0),
            "realizedProfit": _won(d.get("realizedProfit") or 0),
            "officialAvailableCash": _won(d.get("officialAvailableCash") or 0),
            "batchCashReserveTotal": _won(d.get("batchCashReserveTotal") or 0),
            "totalCash": _won(d.get("totalCash") or 0), "holdingsMarketValue": _won(d.get("holdingsMarketValue") or 0),
            "totalAsset": _won(d.get("totalAsset") or 0), "cumulativeReturn": _r2(d.get("cumulativeReturn") or 0),
            "buyBatchId": d.get("buyBatchId"), "sellBatchId": d.get("sellBatchId"),
            "buys": buys, "sells": sells,
        })
    return days


def build_magic_official_public(state_path=OFFICIAL_STATE_PATH) -> dict:
    """canonical을 read-only로 읽어 public 3키 dict 반환. 파일 쓰기 0. 검증 실패 시 MappingValidationError."""
    raw = Path(state_path).read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    st = json.loads(raw.decode("utf-8"))
    validate_canonical(st)
    summary = build_summary(st, sha)
    portfolio = build_portfolio(st)
    # 보유 집계 합 == 최신 holdingsMarketValue 검증(파생 일관성)
    agg = sum(h["marketValue"] for h in portfolio["holdings"])
    if not _close(agg, summary["holdingsMarketValue"]):
        raise MappingValidationError(f"portfolio marketValue sum {agg} != holdingsMarketValue {summary['holdingsMarketValue']}")
    trade_days = build_trade_days(st)
    return {"magicOfficialSummary": summary, "magicOfficialPortfolio": portfolio,
            "magicOfficialTradeDays": trade_days}


# ===== CLI preview (stdout only; 파일 쓰기 0) =====

def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="OFFICIAL canonical → public 매핑 미리보기 (read-only, write 0)")
    ap.add_argument("--state-path", default=str(OFFICIAL_STATE_PATH))
    args = ap.parse_args(argv)
    try:
        out = build_magic_official_public(args.state_path)
    except (FileNotFoundError, MappingValidationError) as e:
        print(json.dumps({"status": "BLOCKED", "reason": f"{type(e).__name__}: {e}",
                          "productionWriteCount": 0}, ensure_ascii=False, indent=2))
        return 2
    out["_meta"] = {"status": "PREVIEW_OK", "productionWriteCount": 0, "filesWritten": 0}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
