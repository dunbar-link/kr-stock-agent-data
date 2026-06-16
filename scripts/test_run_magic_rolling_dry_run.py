#!/usr/bin/env python3
"""run_magic_rolling_dry_run 테스트 (Phase 45-E4.1: 신호일·체결일 분리 / look-ahead 차단).

전부 in-memory fixture + pykrx mock. production JSON은 읽기만(쓰기 0) — 해시 불변 검증 포함.
실행:  python scripts/test_run_magic_rolling_dry_run.py
"""
from __future__ import annotations

import copy
import hashlib

import magic_rolling_engine as E
import run_magic_rolling_dry_run as W

CODES10 = [f"{i:06d}" for i in range(1, 11)]
KST = "+09:00"


def mk_rankings_doc(signal_as_of, codes, *, universe_base_date=None, generated_at=None,
                    formula_version="magic-v1", execution_date=None, ranking_signal=None):
    """신호일·체결일 분리 스키마. universe/generated는 기본값을 signalAsOfDate에서 파생."""
    doc = {
        "signalAsOfDate": signal_as_of,
        "universeBaseDate": signal_as_of if universe_base_date is None else universe_base_date,
        "rankingGeneratedAt": f"{signal_as_of}T18:00:00{KST}" if generated_at is None else generated_at,
        "formulaVersion": formula_version,
        "todayMagicRankingTop10": [
            {"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
             "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.5,
             "earningsYield": 0.2, "buyOpenPrice": 1000.0, "priceSource": "pykrx_open"}
            for i, c in enumerate(codes)],
    }
    if execution_date is not None:
        doc["executionDate"] = execution_date
    if ranking_signal is not None:
        doc["rankingSignalAsOfDate"] = ranking_signal
    return doc


def flat_prices(codes, price=1000.0):
    return {c: float(price) for c in codes}


def open_at(execution_date, hhmmss="09:00:00", tz=KST):
    return f"{execution_date}T{hhmmss}{tz}"


def call(execution, signal, *, codes=None, cal=None, prices=None, generated_at=None,
         universe_base_date=None, market_open_at=None, execution_date_field=None,
         ranking_signal=None, **kw):
    """공통 호출 헬퍼. 기본 캘린더 = [signal, execution] 두 거래일."""
    codes = CODES10 if codes is None else codes
    cal = E.make_calendar([signal, execution]) if cal is None else cal
    doc = mk_rankings_doc(signal, codes, universe_base_date=universe_base_date,
                          generated_at=generated_at, execution_date=execution_date_field,
                          ranking_signal=ranking_signal)
    prices = flat_prices(CODES10) if prices is None else prices
    moa = open_at(execution) if market_open_at is None else market_open_at
    return W.official_dry_run(execution, doc, prices, cal, execution_market_open_at=moa, **kw)


def mk_pilot_lots(n=10, invested=88_984.0):
    return {"lots": [
        {"lotId": f"MF-2026-06-08-{c}-{i:02d}", "code": c, "name": f"P{c}", "buyDate": "2026-06-08",
         "buyOpenPrice": 3100.0, "priceSource": "pykrx_open", "quantity": 32,
         "investedAmount": invested, "rank": i}
        for i, c in enumerate(CODES10[:n], 1)]}


def _hash(path):
    p = str(path)
    try:
        with open(p, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except FileNotFoundError:
        return None


# ----- 1~3 PILOT preview (불변) -----

def t1_pilot_preview_ok():
    doc = mk_pilot_lots()                      # 10 lots × 88,984 = 889,840
    before = copy.deepcopy(doc)
    res = W.pilot_preview(lots_doc=doc)
    assert res["runStatus"] == W.PILOT_PREVIEW_OK, res["runStatus"]
    assert res["itemLotCount"] == 10 and res["totalInvested"] == 889_840.0
    assert res["pilotBatchId"] == "MF-PILOT-2026-06-08" and res["operationMode"] == E.PILOT
    assert res["coreFieldsUnchanged"] is True
    assert res["officialCapitalImpact"] == 0 and res["writeCount"] == 0
    assert doc == before, "입력 불변"


def t2_pilot_lot_count_mismatch():
    res = W.pilot_preview(lots_doc=mk_pilot_lots(n=9))
    assert res["runStatus"] == W.BLOCKED_PILOT_SOURCE_MISMATCH, res["runStatus"]
    assert res["writeCount"] == 0


def t3_pilot_total_mismatch():
    res = W.pilot_preview(lots_doc=mk_pilot_lots(invested=99_999.0))  # 합 ≠ 889,840
    assert res["runStatus"] == W.BLOCKED_PILOT_SOURCE_MISMATCH, res["runStatus"]


# ----- 4~13 OFFICIAL dry-run (신모델) -----

def t4_official_dry_run_ok():
    res = call("2030-01-03", "2030-01-02")
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert res["blocked"] is False
    assert res["proposedSequence"] == 1
    assert res["batchPreview"]["buyCount"] == 10
    assert len(res["selectedTop10"]) == 10
    assert res["allocatedCapital"] == 1_000_000
    assert res["officialStartDatePersisted"] is False
    assert res["productionWriteCount"] == 0 and res["publicCopyCount"] == 0
    assert res["dailyRunConnected"] is False


def t5_legacy_baseonly_blocked():
    """legacy baseDate-only 문서(신호 메타데이터 없음) → BLOCKED_MISSING_SIGNAL_METADATA.
    (구 rankingBaseDate==operationDate 규칙은 제거됨; baseDate를 executionDate로 자동등치 금지)"""
    cal = E.make_calendar(["2030-01-02", "2030-01-03"])
    legacy = {"baseDate": "2030-01-02",
              "todayMagicRankingTop10": mk_rankings_doc("2030-01-02", CODES10)["todayMagicRankingTop10"]}
    res = W.official_dry_run("2030-01-03", legacy, flat_prices(CODES10), cal,
                             execution_market_open_at=open_at("2030-01-03"))
    assert res["runStatus"] == W.BLOCKED_MISSING_SIGNAL_METADATA, res["runStatus"]
    assert res["productionWriteCount"] == 0 and res["blocked"] is True


def t6_ranking_incomplete_or_dup():
    # 9개 → engine BLOCKED_MISSING_RANKING
    res = call("2030-01-03", "2030-01-02", codes=CODES10[:9])
    assert res["runStatus"] == E.BLOCKED_MISSING_RANKING, res["runStatus"]
    # 중복 코드 → wrapper BLOCKED_MISSING_RANKING
    res2 = call("2030-01-03", "2030-01-02", codes=CODES10[:9] + [CODES10[0]])
    assert res2["runStatus"] == E.BLOCKED_MISSING_RANKING, res2["runStatus"]


def t7_non_trading_and_no_calendar():
    # 비개장일(범위 내, 거래일 set 미포함). signal=금(01-04), exec=토(01-05)
    cal = E.make_calendar(["2030-01-04", "2030-01-07"])
    res = call("2030-01-05", "2030-01-04", cal=cal,
               market_open_at=open_at("2030-01-05"))
    assert res["runStatus"] == E.NON_TRADING_DAY, res["runStatus"]
    # 캘린더 없음 → engine BLOCKED_NO_TRADING_CALENDAR
    res2 = call("2030-01-02", "2030-01-01", cal=False)  # cal=False → None 전달
    assert res2["runStatus"] == E.BLOCKED_NO_TRADING_CALENDAR, res2["runStatus"]


def t8_missing_open_price_blocked():
    px = flat_prices(CODES10)
    del px[CODES10[5]]  # 한 종목 시가 누락
    res = call("2030-01-03", "2030-01-02", prices=px)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]
    assert res.get("batchPreview") is None, "부분 매수 0"


def t9_no_fallback_substitution():
    # open price 0/누락 → BLOCKED (universe/종가 대체 안 함)
    px = flat_prices(CODES10)
    px[CODES10[2]] = 0.0
    res = call("2030-01-03", "2030-01-02", prices=px)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]


def t10_deterministic_and_no_write():
    a = call("2030-01-03", "2030-01-02")
    b = call("2030-01-03", "2030-01-02")
    for k in ("runStatus", "allocatedCapital", "totalInvested", "cashReserve",
              "selectedTop10", "proposedSequence", "signalAsOfDate", "executionDate"):
        assert a[k] == b[k], f"비결정적: {k}"
    assert a["productionWriteCount"] == 0


def t11_wrapper_calls_core_engine():
    calls = []
    orig = E.plan_official_day
    def spy(*a, **k):
        calls.append(1)
        return orig(*a, **k)
    E.plan_official_day = spy
    try:
        call("2030-01-03", "2030-01-02")
    finally:
        E.plan_official_day = orig
    assert calls, "wrapper가 engine.plan_official_day(코어)를 호출해야 함(로직 복제 금지)"


def t12_production_files_unchanged():
    files = [W.LOTS_PATH, W.RANKINGS_PATH, W.ROOT / "financial-universe-real.json",
             W.ROOT / "magic-formula-portfolio.json", W.ROOT / "recommendation-history.json"]
    before = {str(f): _hash(f) for f in files}
    W.pilot_preview()
    call("2030-01-03", "2030-01-02")
    W.pilot_timing_audit()
    after = {str(f): _hash(f) for f in files}
    assert before == after, "production 파일 해시 불변(쓰기 0)"


def t13_build_krx_calendar_uses_pykrx_mock():
    class FakeStock:
        def get_business_days(self, y, m):
            return ["2030-01-02", "2030-01-03", "2030-01-04"]
    cal = W.build_krx_calendar("2030-01-02", pykrx_stock=FakeStock())
    assert cal is not None and E.classify_trading_day("2030-01-02", cal) == "TRADING"
    assert E.classify_trading_day("2030-01-05", cal) in ("NON_TRADING", "NO_CALENDAR")

    class EmptyStock:
        def get_business_days(self, y, m):
            return []
    assert W.build_krx_calendar("2030-01-02", pykrx_stock=EmptyStock()) is None


# ----- 14~27 신호일·체결일 분리 / look-ahead -----

def t14_signal_d_minus_1_execution_d():
    res = call("2030-01-03", "2030-01-02")
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert res["signalAsOfDate"] == "2030-01-02"
    assert res["executionDate"] == "2030-01-03"
    assert res["previousKrxTradingDay"] == "2030-01-02"
    assert res["timingValidationPassed"] is True
    assert res["lookAheadValidationPassed"] is True


def t15_same_day_signal_execution_mismatch():
    # 동일일 신호==체결 → SIGNAL_DATE_MISMATCH (generated_at은 같은날 개장 이전으로 둬 시각검증 통과)
    res = call("2030-01-02", "2030-01-02", cal=E.make_calendar(["2030-01-02"]),
               generated_at="2030-01-02T08:00:00+09:00")
    assert res["runStatus"] == W.BLOCKED_SIGNAL_DATE_MISMATCH, res["runStatus"]
    assert res["blocked"] is True and res.get("batchPreview") is None
    assert res["lookAheadValidationPassed"] is False


def t16_signal_not_previous_trading_day():
    # 2거래일 이상 stale: exec=01-06, prev_td=01-03, signal=01-02
    cal = E.make_calendar(["2030-01-02", "2030-01-03", "2030-01-06"])
    res = call("2030-01-06", "2030-01-02", cal=cal)
    assert res["runStatus"] == W.BLOCKED_SIGNAL_NOT_PREVIOUS_TRADING_DAY, res["runStatus"]
    assert res["previousKrxTradingDay"] == "2030-01-03"


def t17_ranking_generated_after_open():
    res = call("2030-01-03", "2030-01-02",
               generated_at="2030-01-03T10:00:00+09:00",      # 개장(09:00) 이후
               market_open_at=open_at("2030-01-03"))
    assert res["runStatus"] == W.BLOCKED_RANKING_GENERATED_AFTER_OPEN, res["runStatus"]
    assert res["blocked"] is True and res.get("batchPreview") is None


def t18_ranking_generated_equals_open():
    res = call("2030-01-03", "2030-01-02",
               generated_at="2030-01-03T09:00:00+09:00",      # == 개장
               market_open_at=open_at("2030-01-03"))
    assert res["runStatus"] == W.BLOCKED_RANKING_GENERATED_AFTER_OPEN, res["runStatus"]


def t19_tz_naive_generated_at():
    res = call("2030-01-03", "2030-01-02",
               generated_at="2030-01-02T18:00:00")            # offset 없음(naive)
    assert res["runStatus"] == W.BLOCKED_MISSING_SIGNAL_METADATA, res["runStatus"]


def t20_universe_base_date_mismatch():
    res = call("2030-01-03", "2030-01-02", universe_base_date="2030-01-01")
    assert res["runStatus"] == W.BLOCKED_UNIVERSE_DATE_MISMATCH, res["runStatus"]


def t21_ranking_signal_mismatch():
    # ranking이 stamp한 signalAsOfDate가 문서 signalAsOfDate와 불일치
    res = call("2030-01-03", "2030-01-02", ranking_signal="2030-01-01")
    assert res["runStatus"] == W.BLOCKED_SIGNAL_DATE_MISMATCH, res["runStatus"]


def t22_friday_signal_monday_execution():
    # 금→월: 주말 사이지만 직전 거래일이므로 PASS
    cal = E.make_calendar(["2030-01-04", "2030-01-07"])
    res = call("2030-01-07", "2030-01-04", cal=cal)
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert res["lookAheadValidationPassed"] is True


def t23_pre_holiday_signal_post_holiday_execution():
    # 06-03 휴장: 06-02 신호 → 06-04 체결(휴장 다음 첫 거래일) PASS
    cal = E.make_calendar(["2026-06-02", "2026-06-04"])
    res = call("2026-06-04", "2026-06-02", cal=cal)
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert res["previousKrxTradingDay"] == "2026-06-02"


def t24_stale_signal_with_intervening_trading_day():
    # 신호와 체결 사이에 거래일(01-08)이 끼어 있음 → NOT_PREVIOUS_TRADING_DAY
    cal = E.make_calendar(["2030-01-07", "2030-01-08", "2030-01-10"])
    res = call("2030-01-10", "2030-01-07", cal=cal)
    assert res["runStatus"] == W.BLOCKED_SIGNAL_NOT_PREVIOUS_TRADING_DAY, res["runStatus"]
    assert res["previousKrxTradingDay"] == "2030-01-08"


def t25_same_input_deterministic_no_state_change():
    files = [W.LOTS_PATH, W.RANKINGS_PATH]
    before = {str(f): _hash(f) for f in files}
    a = call("2030-01-03", "2030-01-02")
    b = call("2030-01-03", "2030-01-02")
    after = {str(f): _hash(f) for f in files}
    for k in ("runStatus", "proposedSequence", "totalInvested", "cashReserve",
              "signalAsOfDate", "rankingGeneratedAt", "executionDate", "lookAheadValidationPassed"):
        assert a[k] == b[k], f"비결정적: {k}"
    assert before == after, "state/파일 0 변경"
    assert a["productionWriteCount"] == 0 and a["officialStartDatePersisted"] is False


def t26_preview_contains_signal_execution_fields():
    res = call("2030-01-03", "2030-01-02")
    bp = res["batchPreview"]
    for k in ("signalAsOfDate", "rankingGeneratedAt", "executionDate", "executionMarketOpenAt",
              "executionPriceSource"):
        assert k in bp, f"batchPreview 누락: {k}"
    assert bp["signalAsOfDate"] == "2030-01-02" and bp["executionDate"] == "2030-01-03"
    assert bp["executionPriceSource"] == "pykrx_open"
    bl = res["buyLedgerPreview"]
    for k in ("signalAsOfDate", "rankingGeneratedAt", "executionDate", "executionPriceSource"):
        assert k in bl, f"buyLedgerPreview 누락: {k}"
    dl = res["dailyLedgerPreview"]
    assert dl["signalAsOfDate"] == "2030-01-02" and dl["executionDate"] == "2030-01-03"
    assert dl["lookAheadValidationPassed"] is True


def t27_pilot_timing_audit():
    files = [W.LOTS_PATH, W.RANKINGS_PATH]
    before = {str(f): _hash(f) for f in files}
    a = W.pilot_timing_audit()
    assert a["legacyRankingBaseDate"] == "2026-06-08", a["legacyRankingBaseDate"]
    assert a["auditedSignalAsOfDate"] == "2026-05-29", a["auditedSignalAsOfDate"]
    assert a["executionDate"] == "2026-06-08", a["executionDate"]
    assert a["timingAuditStatus"] == "PASS_NO_LOOKAHEAD", a["timingAuditStatus"]
    assert a["officialPathReadable"] is False
    assert a["productionWriteCount"] == 0 and a["officialStartDatePersisted"] is False
    after = {str(f): _hash(f) for f in files}
    assert before == after, "PILOT production 파일 불변"


TESTS = [
    ("1  정상 PILOT preview(10/889,840/불변/write0)", t1_pilot_preview_ok),
    ("2  PILOT lot 개수 불일치→MISMATCH", t2_pilot_lot_count_mismatch),
    ("3  PILOT 총액 불일치→MISMATCH", t3_pilot_total_mismatch),
    ("4  정상 OFFICIAL dry-run(batch1/BUY10/persist0)", t4_official_dry_run_ok),
    ("5  legacy baseDate-only→MISSING_SIGNAL_METADATA", t5_legacy_baseonly_blocked),
    ("6  ranking 9개/중복→MISSING_RANKING", t6_ranking_incomplete_or_dup),
    ("7  비개장일/캘린더없음", t7_non_trading_and_no_calendar),
    ("8  시가 한 종목 누락→MISSING_OPEN_PRICE(부분0)", t8_missing_open_price_blocked),
    ("9  fallback 대체 차단(open없으면 BLOCKED)", t9_no_fallback_substitution),
    ("10 결정적 + write0", t10_deterministic_and_no_write),
    ("11 wrapper가 core engine 호출(복제 금지)", t11_wrapper_calls_core_engine),
    ("12 production 파일 해시 불변", t12_production_files_unchanged),
    ("13 build_krx_calendar pykrx mock", t13_build_krx_calendar_uses_pykrx_mock),
    ("14 D-1 신호→D 체결 COMPLETED(lookAhead=True)", t14_signal_d_minus_1_execution_d),
    ("15 동일일 신호==체결→SIGNAL_DATE_MISMATCH(BUY0)", t15_same_day_signal_execution_mismatch),
    ("16 신호≠직전거래일(2+stale)→NOT_PREVIOUS", t16_signal_not_previous_trading_day),
    ("17 rankingGeneratedAt>개장→AFTER_OPEN(BUY0)", t17_ranking_generated_after_open),
    ("18 rankingGeneratedAt==개장→AFTER_OPEN", t18_ranking_generated_equals_open),
    ("19 tz-naive rankingGeneratedAt→MISSING_METADATA", t19_tz_naive_generated_at),
    ("20 universeBaseDate 불일치→UNIVERSE_MISMATCH", t20_universe_base_date_mismatch),
    ("21 ranking signalAsOfDate 불일치→SIGNAL_MISMATCH", t21_ranking_signal_mismatch),
    ("22 금 신호→월 체결 PASS", t22_friday_signal_monday_execution),
    ("23 휴장전 신호→휴장후 첫거래일 PASS", t23_pre_holiday_signal_post_holiday_execution),
    ("24 사이 거래일 존재 stale→NOT_PREVIOUS", t24_stale_signal_with_intervening_trading_day),
    ("25 동일입력 결정적+state/파일0변경", t25_same_input_deterministic_no_state_change),
    ("26 Batch/BUY/daily preview 시점필드 포함", t26_preview_contains_signal_execution_fields),
    ("27 PILOT 타이밍 감사(05-29/06-08/PASS/불변)", t27_pilot_timing_audit),
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
