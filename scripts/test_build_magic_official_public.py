#!/usr/bin/env python3
"""build_magic_official_public 테스트 + build_recommendation_history additive 연결 테스트 (Phase 45-E8.1).

real day-1 canonical(read-only) + 엔진 합성 51일차 state로 검증. public/REPO1/canonical 쓰기 0.
실행:  python scripts/test_build_magic_official_public.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
from datetime import date as _date, timedelta
from pathlib import Path

import magic_rolling_engine as E
import build_magic_official_public as M
import build_recommendation_history as H

CODES = [f"{i:06d}" for i in range(1, 11)]


def gen_days(n, start=(2026, 1, 2)):
    out, d = [], _date(*start)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def mk_ranking(codes):
    return [{"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
             "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.5, "earningsYield": 0.2}
            for i, c in enumerate(codes)]


def mk_pilot_section():
    lots = [{"lotId": f"MF-2026-06-08-{c}-{i:02d}", "code": c, "name": f"P{c}", "buyDate": "2026-06-08",
             "buyOpenPrice": 3100.0, "priceSource": "pykrx_open", "quantity": 32,
             "investedAmount": 88984.0, "rank": i} for i, c in enumerate(CODES, 1)]
    return {"pilotBatchId": "MF-PILOT-2026-06-08", "operationMode": "PILOT", "buyDate": "2026-06-08",
            "itemLotCount": 10, "totalInvested": 889840.0, "officialCapitalImpact": 0, "officialSequenceImpact": 0,
            "timingAudit": {"legacyRankingBaseDate": "2026-06-08", "auditedSignalAsOfDate": "2026-05-29",
                            "executionDate": "2026-06-08", "timingAuditStatus": "PASS_NO_LOOKAHEAD"},
            "itemLots": lots}


def build_engine_state(n):
    days = gen_days(n + 10)
    cal = E.make_calendar(days)
    st = E.empty_official_state()
    rk, px = mk_ranking(CODES), {c: 1000.0 for c in CODES}
    for i in range(n):
        d = days[i]
        timing = {"signalAsOfDate": days[i - 1] if i > 0 else "2026-01-01",
                  "rankingGeneratedAt": f"{days[i]}T08:00:00+09:00", "executionDate": d,
                  "executionMarketOpenAt": f"{d}T09:00:00+09:00", "executionPriceSource": "pykrx_open",
                  "lookAheadValidationPassed": True}
        st, _ = E.plan_official_day(st, d, rk, px, px, cal, now=f"{d}T18:00:00+09:00", timing=timing)
    return st


def to_canonical(st):
    daily = []
    for dl in st["dailyLedger"]:
        d = dict(dl); date = d["date"]; s = d["officialSequence"]
        buys = [e for e in st["buyLedger"] if e["date"] == date]
        sells = [e for e in st["sellLedger"] if e["date"] == date]
        avail = max(0, st["initialCapital"] - min(s, 50) * st["initialBatchCapital"])
        d["totalBuyAmount"] = round(sum(float(e["amount"]) for e in buys), 2)
        d["totalSellAmount"] = round(sum(float(e["amount"]) for e in sells), 2)
        d["realizedProfit"] = round(sum(float(e.get("realizedProfit") or 0) for e in sells), 2)
        d["totalCash"] = d["cash"]
        d["officialAvailableCash"] = float(avail)
        d["batchCashReserveTotal"] = round(d["cash"] - avail, 2)
        daily.append(d)
    return {"schemaVersion": "magic-official-state-v1", "formulaVersion": "test-fv", "operationMode": "OFFICIAL",
            "officialStartDate": st["officialStartDate"], "initialCapital": st["initialCapital"],
            "initialBatchCapital": st["initialBatchCapital"], "officialSequence": st["officialSequence"],
            "officialTradingCalendar": st["officialTradingCalendar"], "officialAvailableCash": st["officialAvailableCash"],
            "batches": st["batches"], "itemLots": st["itemLots"], "buyLedger": st["buyLedger"],
            "sellLedger": st["sellLedger"], "dailyLedger": daily,
            "evaluationSnapshots": st.get("evaluationSnapshots") or st.get("evalSnapshots") or [],
            "missedRuns": st["missedRuns"], "pilot": mk_pilot_section(), "prevTotalAsset": st["prevTotalAsset"],
            "updatedAt": "2026-06-17T00:00:00+09:00"}


STATE3 = to_canonical(build_engine_state(3))
STATE51 = to_canonical(build_engine_state(51))

# 45-E14: 실제 가격 기반 day-1/day-2 고정 fixture(거래일이 늘어도 불변). live canonical 검증은 dynamic.
RANK_CODES = ["046940", "461300", "088130", "184230", "124500",
              "053580", "171090", "018290", "052400", "215200"]
REAL_NAMES = {"046940": "우원개발", "461300": "아이스크림미디어", "088130": "동아엘텍",
              "184230": "SGA솔루션즈", "124500": "아이티센글로벌", "053580": "웹케시",
              "171090": "선익시스템", "018290": "브이티", "052400": "코나아이", "215200": "메가스터디교육"}
REAL_D1_OPENS = {"046940": 3465.0, "461300": 17650.0, "088130": 6900.0, "184230": 2110.0, "124500": 34200.0,
                 "053580": 6780.0, "171090": 75700.0, "018290": 12780.0, "052400": 47600.0, "215200": 41950.0}
REAL_D2_OPENS = {"046940": 3400.0, "461300": 16800.0, "088130": 6410.0, "184230": 2045.0, "124500": 32200.0,
                 "053580": 6490.0, "171090": 71600.0, "018290": 12210.0, "052400": 46850.0, "215200": 39100.0}
# day-2 보유 집계 기대값(batch1 qty + batch2 qty)
EXPECT_TOTAL_QTY = {"046940": 57, "461300": 10, "088130": 29, "184230": 95, "124500": 5,
                    "053580": 29, "171090": 2, "018290": 15, "052400": 4, "215200": 4}


def _real_ranking():
    return [{"code": c, "name": REAL_NAMES[c], "rank": i, "combinedRank": i * 2, "profitabilityRank": i,
             "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2} for i, c in enumerate(RANK_CODES, 1)]


def _timing(signal_as_of, exec_date):
    return {"signalAsOfDate": signal_as_of, "rankingGeneratedAt": f"{signal_as_of}T18:00:00+09:00",
            "executionDate": exec_date, "executionMarketOpenAt": f"{exec_date}T09:00:00+09:00",
            "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}


def build_day1_fixture():
    cal = E.make_calendar(["2026-06-16", "2026-06-17"])
    st = E.empty_official_state()
    st, _ = E.plan_official_day(st, "2026-06-17", _real_ranking(), REAL_D1_OPENS, REAL_D1_OPENS, cal,
                                now="2026-06-17T18:00:00+09:00", timing=_timing("2026-06-16", "2026-06-17"),
                                trading_day_index=1)
    return to_canonical(st)


def build_day2_fixture():
    cal = E.make_calendar(["2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"])
    st = E.empty_official_state()
    st, _ = E.plan_official_day(st, "2026-06-17", _real_ranking(), REAL_D1_OPENS, REAL_D1_OPENS, cal,
                                now="2026-06-17T18:00:00+09:00", timing=_timing("2026-06-16", "2026-06-17"),
                                trading_day_index=1)
    st, _, _ = E.apply_missed_run(st, "2026-06-18", now="2026-06-18T08:00:00+09:00")
    st, _ = E.plan_official_day(st, "2026-06-19", _real_ranking(), REAL_D2_OPENS, REAL_D2_OPENS, cal,
                                now="2026-06-19T18:00:00+09:00", timing=_timing("2026-06-18", "2026-06-19"),
                                trading_day_index=3)
    return to_canonical(st)


DAY1_FIX = build_day1_fixture()
DAY2_FIX = build_day2_fixture()


def build_from(state_dict):
    d = tempfile.mkdtemp()
    p = Path(d) / "state.json"
    p.write_text(json.dumps(state_dict, ensure_ascii=False), encoding="utf-8")
    try:
        return M.build_magic_official_public(p)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def load_real():
    return json.loads(Path(M.OFFICIAL_STATE_PATH).read_text(encoding="utf-8"))


def expect_blocked(state_dict):
    try:
        build_from(state_dict)
        return False
    except M.MappingValidationError:
        return True


# ----- 테스트 -----

def t1_real_day1_maps_ok():
    out = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)
    assert set(out) == set(M.OFFICIAL_PUBLIC_KEYS), out.keys()


def t2_summary_live_consistency():
    """live canonical: 거래일 수와 무관하게 summary가 입력 canonical과 일관(동적). seq 고정 단언 없음."""
    sha_live = hashlib.sha256(Path(M.OFFICIAL_STATE_PATH).read_bytes()).hexdigest()
    st = load_real()
    s = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)["magicOfficialSummary"]
    completed = [d for d in st["dailyLedger"] if d.get("runStatus") == "COMPLETED"]
    latest = max(completed, key=lambda d: d["date"])
    open_batches = [b for b in st["batches"] if b.get("operationMode") == "OFFICIAL" and b.get("status") == "OPEN"]
    reserves = round(sum(float(b.get("cashReserve") or 0) for b in open_batches), 2)
    assert s["officialSequence"] == st["officialSequence"]
    assert s["officialStartDate"] == st["officialStartDate"]
    assert s["dataDate"] == latest["date"] and s["latestTradingDate"] == latest["date"]
    assert s["openBatchCount"] == len(open_batches)
    assert s["openItemLotCount"] == len([l for l in st["itemLots"] if l.get("status") == "OPEN"])
    assert s["totalBuyCount"] == len(st["buyLedger"]) and s["totalSellCount"] == len(st["sellLedger"])
    assert s["officialAvailableCash"] == M._won(st["officialAvailableCash"])
    assert s["totalCash"] == M._won(float(st["officialAvailableCash"]) + reserves)
    assert s["holdingsMarketValue"] == M._won(latest["holdingsMarketValue"])
    assert s["totalAsset"] == M._won(latest["totalAsset"])
    assert s["pilotExcluded"] is True
    assert s["sourceStateSha256"] == sha_live


def t2b_summary_day1_fixture():
    """고정 day-1 fixture: 1일차 정확값(거래일 증가에 불변)."""
    s = build_from(DAY1_FIX)["magicOfficialSummary"]
    assert s["officialSequence"] == 1 and s["officialStartDate"] == "2026-06-17"
    assert s["latestTradingDate"] == "2026-06-17"
    assert s["openBatchCount"] == 1 and s["openItemLotCount"] == 10 and s["closedBatchCount"] == 0
    assert s["totalBuyCount"] == 10 and s["totalSellCount"] == 0
    assert s["officialAvailableCash"] == 49000000 and s["batchCashReserveTotal"] == 111380
    assert s["totalCash"] == 49111380 and s["holdingsMarketValue"] == 888620 and s["totalAsset"] == 50000000
    assert s["cumulativeReturn"] == 0.0 and s["pilotExcluded"] is True


def t2c_summary_day2_fixture():
    """고정 day-2 fixture: 2일차 정확값(batch2 반영)."""
    s = build_from(DAY2_FIX)["magicOfficialSummary"]
    assert s["officialSequence"] == 2 and s["officialStartDate"] == "2026-06-17"
    assert s["latestTradingDate"] == "2026-06-19"
    assert s["openBatchCount"] == 2 and s["openItemLotCount"] == 20 and s["closedBatchCount"] == 0
    assert s["totalBuyCount"] == 20 and s["totalSellCount"] == 0
    assert s["officialAvailableCash"] == 48000000 and s["batchCashReserveTotal"] == 199340
    assert s["totalCash"] == 48199340 and s["holdingsMarketValue"] == 1761325 and s["totalAsset"] == 49960665


def t3_portfolio_live_consistency():
    """live: OPEN lot이 code별로 집계되고 합계가 summary와 일치(동적)."""
    out = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)
    st = load_real()
    p, s = out["magicOfficialPortfolio"], out["magicOfficialSummary"]
    open_lots = [l for l in st["itemLots"] if l.get("status") == "OPEN"]
    codes = sorted({l["code"] for l in open_lots})
    assert [h["code"] for h in p["holdings"]] == codes
    for h in p["holdings"]:
        clots = [l for l in open_lots if l["code"] == h["code"]]
        assert h["openLotCount"] == len(clots)
        assert h["totalQuantity"] == sum(int(l["quantity"]) for l in clots)
    assert sum(h["marketValue"] for h in p["holdings"]) == s["holdingsMarketValue"]


def t3b_portfolio_day2_aggregation():
    """고정 day-2 fixture: 20 lot → 10종목, 종목별 openLotCount=2 및 합산수량/평가/손익 정확."""
    p = build_from(DAY2_FIX)["magicOfficialPortfolio"]
    assert len(p["holdings"]) == 10
    assert all(h["openLotCount"] == 2 for h in p["holdings"])
    by = {h["code"]: h for h in p["holdings"]}
    for c, q in EXPECT_TOTAL_QTY.items():
        assert by[c]["totalQuantity"] == q, (c, by[c]["totalQuantity"])
    assert sum(h["marketValue"] for h in p["holdings"]) == 1761325
    assert sum(h["totalInvested"] for h in p["holdings"]) == 1800660
    assert sum(h["unrealizedProfit"] for h in p["holdings"]) == -39335


def t4_tradedays_live_consistency():
    """live: tradeDays=dailyLedger(최신순), MISSED_RUN 미포함, 일자별 카운트 일치(동적)."""
    td = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)["magicOfficialTradeDays"]
    st = load_real()
    daily_dates = sorted([d["date"] for d in st["dailyLedger"]], reverse=True)
    assert [t["date"] for t in td] == daily_dates
    missed = {m["date"] for m in (st.get("missedRuns") or [])}
    assert all(t["date"] not in missed for t in td), "MISSED_RUN이 거래일에 섞이면 안 됨"
    for t in td:
        dl = next(d for d in st["dailyLedger"] if d["date"] == t["date"])
        assert t["officialSequence"] == dl["officialSequence"]
        assert t["buyCount"] == dl["buyCount"] and t["sellCount"] == dl["sellCount"]
        assert len(t["buys"]) == dl["buyCount"] and len(t["sells"]) == dl["sellCount"]


def t4b_tradedays_day2_fixture():
    """고정 day-2 fixture: 거래일 2개 최신순, 06-18 MISSED_RUN 미포함."""
    td = build_from(DAY2_FIX)["magicOfficialTradeDays"]
    assert len(td) == 2
    assert td[0]["date"] == "2026-06-19" and td[0]["officialSequence"] == 2
    assert td[0]["buyCount"] == 10 and td[0]["sellCount"] == 0 and td[0]["totalBuyAmount"] == 912040
    assert td[1]["date"] == "2026-06-17" and td[1]["officialSequence"] == 1
    assert td[1]["buyCount"] == 10 and td[1]["sellCount"] == 0 and td[1]["totalBuyAmount"] == 888620
    assert "2026-06-18" not in [t["date"] for t in td]


def t5_buy_detail_fields():
    buys = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)["magicOfficialTradeDays"][0]["buys"]
    req = {"tradeId", "batchId", "lotId", "rank", "code", "name", "executionPrice", "quantity",
           "amount", "signalAsOfDate", "executionDate", "priceSource"}
    for b in buys:
        assert req <= set(b), set(b)
        assert b["rank"] is not None and b["priceSource"] == "pykrx_open"
    assert [b["rank"] for b in buys] == sorted(b["rank"] for b in buys)  # rank 오름차순


def t6_synthetic_day51_sell10_buy10():
    td = build_from(STATE51)["magicOfficialTradeDays"]
    assert td[0]["officialSequence"] == 51, td[0]["officialSequence"]
    assert td[0]["buyCount"] == 10 and td[0]["sellCount"] == 10
    assert len(td[0]["buys"]) == 10 and len(td[0]["sells"]) == 10


def t7_sell_join_original_buy():
    sells = build_from(STATE51)["magicOfficialTradeDays"][0]["sells"]
    for s in sells:
        assert s["originalBuyDate"] is not None and s["originalBuyPrice"] == 1000
        assert "realizedReturn" in s and "holdingTradingDays" in s and s["lotId"]


def t8_same_code_lots_not_merged_in_tradedays():
    td = build_from(STATE51)["magicOfficialTradeDays"][0]
    buy_lots = {b["lotId"] for b in td["buys"]}
    sell_lots = {s["lotId"] for s in td["sells"]}
    assert buy_lots.isdisjoint(sell_lots), "같은 날 매수/매도 lot은 서로 다른 lotId"
    # 동일 code가 buys와 sells 양쪽에 등장(별도 lot으로 분리됨)
    buy_codes = {b["code"] for b in td["buys"]}
    sell_codes = {s["code"] for s in td["sells"]}
    assert buy_codes & sell_codes, "동일 종목이 매수·매도 양쪽에 별도 lot으로 존재"


def t9_holdings_aggregate_same_code():
    p = build_from(STATE51)["magicOfficialPortfolio"]
    assert len(p["holdings"]) == 10                      # code별 1개로 집계
    assert all(h["openLotCount"] == 50 for h in p["holdings"])  # 50개 open lot 합산
    assert all(h["totalQuantity"] == 5000 for h in p["holdings"])


def t10_pilot_excluded():
    out = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)
    st = load_real()
    assert out["magicOfficialSummary"]["pilotExcluded"] is True
    # PILOT lotId가 공식 itemLots/보유에 없음(동적)
    pilot_lots = {l.get("lotId") for l in (st.get("pilot") or {}).get("itemLots") or []}
    official_lots = {l["lotId"] for l in st["itemLots"]}
    assert not (pilot_lots & official_lots), "PILOT lot이 공식 lot에 누출"
    # 공식 거래일/매수에 PILOT(2026-06-08) 흔적 없음
    for d in out["magicOfficialTradeDays"]:
        assert d["date"] != "2026-06-08"
        for b in d["buys"]:
            assert b["signalAsOfDate"] != "2026-06-08" or b["executionDate"] != "2026-06-08"


def t11_source_sha_correct():
    s = M.build_magic_official_public(M.OFFICIAL_STATE_PATH)["magicOfficialSummary"]
    assert s["sourceStateSha256"] == hashlib.sha256(Path(M.OFFICIAL_STATE_PATH).read_bytes()).hexdigest()


def t12_duplicate_lot_blocked():
    st = load_real()
    st["itemLots"].append(copy.deepcopy(st["itemLots"][0]))  # lotId 중복
    assert expect_blocked(st)


def t13_sequence_discontinuity_blocked():
    st = copy.deepcopy(STATE3)
    st["dailyLedger"][1]["officialSequence"] = 5  # 연속성 깨기
    assert expect_blocked(st)


def t14_buy_count_mismatch_blocked():
    st = copy.deepcopy(STATE3)
    st["dailyLedger"][0]["buyCount"] = 9
    assert expect_blocked(st)


def t15_sell_amount_mismatch_blocked():
    st = copy.deepcopy(STATE51)
    # 최신(매도)일의 totalSellAmount 변조
    latest = max(st["dailyLedger"], key=lambda d: d["date"])
    latest["totalSellAmount"] = latest["totalSellAmount"] + 1.0
    assert expect_blocked(st)


def t16_cash_asset_conservation_blocked():
    st = copy.deepcopy(STATE3)
    st["dailyLedger"][0]["totalAsset"] = 49999999.0  # 보존식 깨기
    assert expect_blocked(st)


def t17_canonical_input_file_unchanged():
    before = hashlib.sha256(Path(M.OFFICIAL_STATE_PATH).read_bytes()).hexdigest()
    M.build_magic_official_public(M.OFFICIAL_STATE_PATH)
    after = hashlib.sha256(Path(M.OFFICIAL_STATE_PATH).read_bytes()).hexdigest()
    assert before == after


def t18_integration_additive_merge():
    enriched = {"baseDate": "2026-06-17"}
    out = H.apply_magic_official_public(enriched, state_path=M.OFFICIAL_STATE_PATH, warn=None)
    assert all(k in out for k in M.OFFICIAL_PUBLIC_KEYS)
    st = load_real()
    assert out["magicOfficialSummary"]["officialSequence"] == st["officialSequence"]
    assert len(out["magicOfficialTradeDays"]) == len(st["dailyLedger"])


def t19_existing_magic5_keys_unchanged():
    legacy = {"magicPortfolioSummary": {"dataDate": "2026-06-08"}, "magicPortfolio": {"holdings": [1, 2]},
              "magicRecentActions": {"date": "2026-06-08"}, "magicFormula": {"v": 1}, "magicFundPolicy": {"t": 1}}
    enriched = copy.deepcopy(legacy)
    H.apply_magic_official_public(enriched, state_path=M.OFFICIAL_STATE_PATH, warn=None)
    for k, v in legacy.items():
        assert enriched[k] == v, f"legacy magic key changed: {k}"


def t20_existing_fund_keys_unchanged():
    base = {"portfolioSummary": {"x": 1}, "aiPortfolioSummary": {"y": 2}, "wababaPicks": [1, 2, 3]}
    enriched = copy.deepcopy(base)
    H.apply_magic_official_public(enriched, state_path=M.OFFICIAL_STATE_PATH, warn=None)
    for k, v in base.items():
        assert enriched[k] == v


def t21_failure_preserves_last_known_good():
    last_good = {"magicOfficialSummary": {"officialSequence": 99}, "magicOfficialPortfolio": {"holdings": []},
                 "magicOfficialTradeDays": [{"date": "old"}]}
    enriched = {"baseDate": "x"}
    out = H.apply_magic_official_public(enriched, state_path="C:/nope/missing-state.json",
                                       existing_public=last_good, warn=None)
    for k in M.OFFICIAL_PUBLIC_KEYS:
        assert out[k] == last_good[k], f"last-known-good not preserved: {k}"


def t22_missing_canonical_keeps_others():
    enriched = {"baseDate": "x", "portfolioSummary": {"a": 1}}
    out = H.apply_magic_official_public(enriched, state_path="C:/nope/missing-state.json",
                                       existing_public=None, warn=None)
    assert out["portfolioSummary"] == {"a": 1}
    assert not any(k in out for k in M.OFFICIAL_PUBLIC_KEYS)  # 신규 키 없음(스킵), 기존 생성 정상


def t23_allowlist_has_official_keys():
    assert set(M.OFFICIAL_PUBLIC_KEYS) <= H._PUBLIC_TOP_ALLOW


def t24_integration_no_file_write():
    paths = [H.PUBLIC_DATA_PATH, H.RECOMMENDATION_HISTORY_PATH]
    before = {str(p): (hashlib.sha256(Path(p).read_bytes()).hexdigest() if Path(p).exists() else None) for p in paths}
    H.apply_magic_official_public({"baseDate": "x"}, state_path=M.OFFICIAL_STATE_PATH, warn=None)
    after = {str(p): (hashlib.sha256(Path(p).read_bytes()).hexdigest() if Path(p).exists() else None) for p in paths}
    assert before == after, "integration 함수가 어떤 public 파일도 쓰지 않아야 함"


# ----- MF-PUBLIC-1B: buys[] evidence 노출(C안) -----

_EV_META = {"evMethod": "marketCap_plus_totalLiabilities_minus_cash", "closePrice": 1000.0,
            "priceAsOfDate": "2026-01-05", "financialStatementYear": 2025, "dartFsDiv": "CFS",
            "evidenceCompleteness": True}


def _with_evidence(state_dict):
    """최신 거래일 buy lot rankSnapshot에 evidence 전용 메타를 주입한 canonical 사본(테스트용)."""
    st = copy.deepcopy(state_dict)
    latest = max(d["date"] for d in st["dailyLedger"])
    for coll in (st["itemLots"], st["buyLedger"]):
        for row in coll:
            if row.get("date") == latest or row.get("buyDate") == latest:
                snap = row.get("rankSnapshot")
                if isinstance(snap, dict):
                    snap.update(_EV_META)
    return st


def t25_buys_evidence_exposed_and_mapped():
    """evidence 있는 canonical → 최신 거래일 buys에 evidence 필드가 노출되고 명칭 매핑이 정확하다."""
    td = build_from(_with_evidence(STATE3))["magicOfficialTradeDays"]
    buys = td[0]["buys"]
    assert buys, "buys 비어있음"
    for b in buys:
        # 명칭 매핑(mk_ranking: valueRank=profitabilityRank=rank, combinedRank=rank*2)
        assert b["cheapRank"] == b["rank"] and b["qualityRank"] == b["rank"]
        assert b["magicScore"] == b["rank"] * 2 and b["finalRank"] == b["rank"]
        assert b["earningsYield"] == 0.2 and b["returnOnCapital"] == 0.5
        # evidence 전용 메타
        assert b["evMethod"] == "marketCap_plus_totalLiabilities_minus_cash"
        assert b["evidenceCompleteness"] is True
        assert b["financialStatementYear"] == 2025 and b["dartFsDiv"] == "CFS"
        assert b["priceAsOfDate"] == "2026-01-05" and b["closePrice"] == 1000.0


def t26_past_seq_buys_evidence_null_backcompat():
    """evidence 없는 과거 canonical → buys의 evidence 전용 메타는 null, 순위/점수/비율은 값 유지(하위호환)."""
    buys = build_from(STATE3)["magicOfficialTradeDays"][0]["buys"]
    assert buys, "buys 비어있음"
    for b in buys:
        # 순위/점수/비율은 랭크필드라 과거에도 값 존재
        assert b["cheapRank"] is not None and b["qualityRank"] is not None
        assert b["magicScore"] is not None and b["earningsYield"] is not None
        # evidence 전용 메타는 과거 seq에서 null(키는 항상 존재)
        for k in ("evMethod", "closePrice", "priceAsOfDate", "financialStatementYear",
                  "dartFsDiv", "evidenceCompleteness"):
            assert k in b and b[k] is None, (k, b.get(k))


def t27_three_public_keys_unchanged_with_evidence():
    """evidence 확장 후에도 public top-level key는 정확히 3키(구조 계약 불변)."""
    out = build_from(_with_evidence(STATE3))
    assert set(out) == set(M.OFFICIAL_PUBLIC_KEYS), out.keys()


TESTS = [
    ("1  real canonical 정상 매핑(키 존재)", t1_real_day1_maps_ok),
    ("2  summary live 일관성(동적)", t2_summary_live_consistency),
    ("2b summary day-1 fixture 정확값", t2b_summary_day1_fixture),
    ("2c summary day-2 fixture 정확값", t2c_summary_day2_fixture),
    ("3  portfolio live 일관성(동적)", t3_portfolio_live_consistency),
    ("3b portfolio day-2 집계(20→10/lot2)", t3b_portfolio_day2_aggregation),
    ("4  tradeDays live 일관성(MISSED 제외)", t4_tradedays_live_consistency),
    ("4b tradeDays day-2 fixture(거래일 2)", t4b_tradedays_day2_fixture),
    ("5  BUY 상세 필드 완전성", t5_buy_detail_fields),
    ("6  synthetic 51일차 SELL10/BUY10", t6_synthetic_day51_sell10_buy10),
    ("7  SELL originalBuyDate/Price JOIN", t7_sell_join_original_buy),
    ("8  동일종목 복수 lot tradeDays 미합산", t8_same_code_lots_not_merged_in_tradedays),
    ("9  holdings 동일 code 파생 집계", t9_holdings_aggregate_same_code),
    ("10 PILOT 완전 제외", t10_pilot_excluded),
    ("11 sourceStateSha256 정확성", t11_source_sha_correct),
    ("12 duplicate lot 차단", t12_duplicate_lot_blocked),
    ("13 officialSequence 불연속 차단", t13_sequence_discontinuity_blocked),
    ("14 BUY count 불일치 차단", t14_buy_count_mismatch_blocked),
    ("15 SELL amount 불일치 차단", t15_sell_amount_mismatch_blocked),
    ("16 cash/asset 보존식 불일치 차단", t16_cash_asset_conservation_blocked),
    ("17 canonical 입력 파일 불변", t17_canonical_input_file_unchanged),
    ("18 build_recommendation_history additive merge", t18_integration_additive_merge),
    ("19 기존 magic* 5키 불변", t19_existing_magic5_keys_unchanged),
    ("20 기존 와바바·AI 키 불변", t20_existing_fund_keys_unchanged),
    ("21 실패 시 last-known-good 보존", t21_failure_preserves_last_known_good),
    ("22 canonical 없음 시 기존 생성 정상", t22_missing_canonical_keeps_others),
    ("23 allowlist 신규 3키 포함", t23_allowlist_has_official_keys),
    ("24 integration public 파일 쓰기 0", t24_integration_no_file_write),
    ("25 buys evidence 노출+명칭매핑 정확", t25_buys_evidence_exposed_and_mapped),
    ("26 과거 seq buys evidence null 하위호환", t26_past_seq_buys_evidence_null_backcompat),
    ("27 evidence 확장 후에도 public 3키 불변", t27_three_public_keys_unchanged_with_evidence),
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
