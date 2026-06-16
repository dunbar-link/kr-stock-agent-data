#!/usr/bin/env python3
"""run_magic_rolling_dry_run 테스트 (Phase 45-E3).

전부 in-memory fixture + pykrx mock. production JSON은 읽기만(쓰기 0) — 해시 불변 검증 포함.
실행:  python scripts/test_run_magic_rolling_dry_run.py
"""
from __future__ import annotations

import copy
import hashlib

import magic_rolling_engine as E
import run_magic_rolling_dry_run as W

CODES10 = [f"{i:06d}" for i in range(1, 11)]


def mk_rankings_doc(base_date, codes):
    return {"baseDate": base_date,
            "todayMagicRankingTop10": [
                {"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
                 "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.5,
                 "earningsYield": 0.2, "buyOpenPrice": 1000.0, "priceSource": "pykrx_open"}
                for i, c in enumerate(codes)]}


def flat_prices(codes, price=1000.0):
    return {c: float(price) for c in codes}


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


# ----- 테스트 -----

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


def t4_official_dry_run_ok():
    cal = E.make_calendar(["2030-01-02"])
    res = W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10),
                             flat_prices(CODES10), cal)
    assert res["runStatus"] == E.COMPLETED, res["runStatus"]
    assert res["blocked"] is False
    assert res["proposedSequence"] == 1
    assert res["batchPreview"]["buyCount"] == 10
    assert len(res["selectedTop10"]) == 10
    assert res["allocatedCapital"] == 1_000_000
    assert res["officialStartDatePersisted"] is False
    assert res["productionWriteCount"] == 0 and res["publicCopyCount"] == 0
    assert res["dailyRunConnected"] is False


def t5_stale_ranking_blocked():
    cal = E.make_calendar(["2030-01-02"])
    res = W.official_dry_run("2030-01-02", mk_rankings_doc("2029-12-31", CODES10),
                             flat_prices(CODES10), cal)
    assert res["runStatus"] == W.BLOCKED_STALE_RANKING, res["runStatus"]
    assert res["productionWriteCount"] == 0


def t6_ranking_incomplete_or_dup():
    cal = E.make_calendar(["2030-01-02"])
    # 9개
    r9 = mk_rankings_doc("2030-01-02", CODES10[:9])
    res = W.official_dry_run("2030-01-02", r9, flat_prices(CODES10), cal)
    assert res["runStatus"] == E.BLOCKED_MISSING_RANKING, res["runStatus"]
    # 중복 코드
    rdup = mk_rankings_doc("2030-01-02", CODES10[:9] + [CODES10[0]])
    res2 = W.official_dry_run("2030-01-02", rdup, flat_prices(CODES10), cal)
    assert res2["runStatus"] == E.BLOCKED_MISSING_RANKING, res2["runStatus"]


def t7_non_trading_and_no_calendar():
    # 비개장일(범위 내, 거래일 set 미포함)
    cal = E.make_calendar(["2030-01-04", "2030-01-07"])  # 금/월 → 토(05) 비개장
    res = W.official_dry_run("2030-01-05", mk_rankings_doc("2030-01-05", CODES10),
                             flat_prices(CODES10), cal)
    assert res["runStatus"] == E.NON_TRADING_DAY, res["runStatus"]
    # 캘린더 없음
    res2 = W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10),
                              flat_prices(CODES10), None)
    assert res2["runStatus"] == E.BLOCKED_NO_TRADING_CALENDAR, res2["runStatus"]


def t8_missing_open_price_blocked():
    cal = E.make_calendar(["2030-01-02"])
    px = flat_prices(CODES10)
    del px[CODES10[5]]  # 한 종목 시가 누락
    res = W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10), px, cal)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]
    assert res.get("batchPreview") is None, "부분 매수 0"


def t9_no_fallback_substitution():
    # universe price가 있어도(여기선 일부러 0/누락) open price 없으면 BLOCKED — 대체 안 함
    cal = E.make_calendar(["2030-01-02"])
    px = flat_prices(CODES10)
    px[CODES10[2]] = 0.0   # 0/null/음수 → 시가 없음 취급
    res = W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10), px, cal)
    assert res["runStatus"] == E.BLOCKED_MISSING_OPEN_PRICE, res["runStatus"]


def t10_deterministic_and_no_write():
    cal = E.make_calendar(["2030-01-02"])
    args = ("2030-01-02", mk_rankings_doc("2030-01-02", CODES10), flat_prices(CODES10), cal)
    a = W.official_dry_run(*args)
    b = W.official_dry_run(*args)
    # 결정적(타임스탬프 제외 핵심값 동일)
    for k in ("runStatus", "allocatedCapital", "totalInvested", "cashReserve", "selectedTop10", "proposedSequence"):
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
        W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10),
                           flat_prices(CODES10), E.make_calendar(["2030-01-02"]))
    finally:
        E.plan_official_day = orig
    assert calls, "wrapper가 engine.plan_official_day(코어)를 호출해야 함(로직 복제 금지)"


def t12_production_files_unchanged():
    files = [W.LOTS_PATH, W.RANKINGS_PATH, W.ROOT / "financial-universe-real.json",
             W.ROOT / "magic-formula-portfolio.json", W.ROOT / "recommendation-history.json"]
    before = {str(f): _hash(f) for f in files}
    # 실제 파일을 읽는 pilot_preview() + 합성 official_dry_run 호출
    W.pilot_preview()
    W.official_dry_run("2030-01-02", mk_rankings_doc("2030-01-02", CODES10),
                       flat_prices(CODES10), E.make_calendar(["2030-01-02"]))
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


TESTS = [
    ("1  정상 PILOT preview(10/889,840/불변/write0)", t1_pilot_preview_ok),
    ("2  PILOT lot 개수 불일치→MISMATCH", t2_pilot_lot_count_mismatch),
    ("3  PILOT 총액 불일치→MISMATCH", t3_pilot_total_mismatch),
    ("4  정상 OFFICIAL dry-run(batch1/BUY10/persist0)", t4_official_dry_run_ok),
    ("5  ranking 날짜 불일치→STALE_RANKING", t5_stale_ranking_blocked),
    ("6  ranking 9개/중복→MISSING_RANKING", t6_ranking_incomplete_or_dup),
    ("7  비개장일/캘린더없음", t7_non_trading_and_no_calendar),
    ("8  시가 한 종목 누락→MISSING_OPEN_PRICE(부분0)", t8_missing_open_price_blocked),
    ("9  fallback 대체 차단(open없으면 BLOCKED)", t9_no_fallback_substitution),
    ("10 결정적 + write0", t10_deterministic_and_no_write),
    ("11 wrapper가 core engine 호출(복제 금지)", t11_wrapper_calls_core_engine),
    ("12 production 파일 해시 불변", t12_production_files_unchanged),
    ("13 build_krx_calendar pykrx mock", t13_build_krx_calendar_uses_pykrx_mock),
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
