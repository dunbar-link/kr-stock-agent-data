#!/usr/bin/env python3
"""build_magic_signal_package 테스트 (Phase 45-E5).

전부 in-memory fixture + 주입(build_payload/ranking/calendar/now) — 네트워크 0.
TEMP 출력은 tempfile.mkdtemp() 아래에만(저장소 밖). production JSON·재무 캐시는 read-only(해시 불변 검증 포함).
실행:  python scripts/test_build_magic_signal_package.py
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import magic_rolling_engine as E
import build_magic_signal_package as P

KST = "+09:00"
SIGNAL = "2026-06-16"
NEXT = "2026-06-17"
CLOSE = "2026-06-16T15:30:00+09:00"
NOW_OK = "2026-06-16T16:00:00+09:00"          # 장 마감 이후
CAL = E.make_calendar([SIGNAL, NEXT])         # 신호일 + 다음 거래일


def fake_universe(signal=SIGNAL, n=12, price=1000.0):
    return {"data": [{"symbol": f"{i:06d}", "corpName": f"S{i}", "industryName": "제조",
                      "marketCap": 1000 + i, "price": price, "dartLatestYear": 2025, "dartFsDiv": "CFS"}
                     for i in range(1, n + 1)],
            "meta": {"baseDate": signal, "count": n}}


def fake_ranking(rows, mode, blacklist, k=11):
    out = []
    for i, r in enumerate(rows[:k]):
        out.append({"rank": i + 1, "code": r["symbol"], "name": r["corpName"], "combinedRank": (i + 1) * 2,
                    "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.3, "earningsYield": 0.1,
                    "marketCap": r["marketCap"], "price": r["price"], "EBIT": 100, "enterpriseValue": 1000,
                    "capitalBase": 500, "cashAndCashEquivalents": 50, "totalLiabilities": 200,
                    "currentAssets": 300, "currentLiabilities": 100, "propertyPlantAndEquipment": 300,
                    "evMethod": "x", "dataSource": "test"})
    return out, {}, {"dartCoverage": 1.0}


def call(signal_date=SIGNAL, **kw):
    base = dict(now=NOW_OK, market_close_at=CLOSE, calendar=CAL, code_commit="testcommit",
                formula_version="test-fv", formula_mode="book_faithful_v1",
                build_payload_fn=lambda: fake_universe(), ranking_fn=fake_ranking)
    base.update(kw)
    return P.build_signal_package(signal_date, **base)


def _hash(p):
    try:
        with open(p, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except (FileNotFoundError, OSError):
        return None


def _mtime(p):
    try:
        return os.path.getmtime(p)
    except OSError:
        return None


# ----- 테스트 -----

def t1_market_not_closed():
    r = call(now="2026-06-16T14:00:00+09:00")     # 마감 전
    assert r["packageStatus"] == P.BLOCKED_SIGNAL_MARKET_NOT_CLOSED, r["packageStatus"]
    assert r["productionWriteCount"] == 0 and r["blocked"] is True


def t2_tz_naive_blocked():
    r1 = call(now="2026-06-16T16:00:00")          # naive now
    assert r1["packageStatus"] == P.BLOCKED_SIGNAL_MARKET_NOT_CLOSED, r1["packageStatus"]
    r2 = call(market_close_at="2026-06-16T15:30:00")   # naive close
    assert r2["packageStatus"] == P.BLOCKED_SIGNAL_MARKET_NOT_CLOSED, r2["packageStatus"]


def t3_ready_basic():
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r = call(output_dir=str(out))
        assert r["packageStatus"] == P.READY, r["packageStatus"]
        assert (out / "universe.json").exists()
        assert (out / "rankings.json").exists()
        assert (out / "manifest.json").exists()
        assert r["executionPriceAvailable"] is False and r["officialTradeCreated"] is False
        assert r["officialStartDatePersisted"] is False and r["productionWriteCount"] == 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t4_universe_prev_day():
    r = call(build_payload_fn=lambda: fake_universe(signal="2026-06-15"))
    assert r["packageStatus"] == P.BLOCKED_SIGNAL_UNIVERSE_NOT_READY, r["packageStatus"]


def t5_invalid_prices_date_mismatch():
    r = call(build_payload_fn=lambda: fake_universe(price=0.0))
    assert r["packageStatus"] == P.BLOCKED_SIGNAL_DATE_MISMATCH, r["packageStatus"]


def t6_duplicate_codes():
    u = fake_universe()
    u["data"][1]["symbol"] = u["data"][0]["symbol"]
    r = call(build_payload_fn=lambda: u)
    assert r["packageStatus"] == P.BLOCKED_SIGNAL_UNIVERSE_NOT_READY, r["packageStatus"]


def t7_missing_ranking():
    r = call(ranking_fn=lambda rows, m, b: fake_ranking(rows, m, b, k=5))
    assert r["packageStatus"] == P.BLOCKED_MISSING_RANKING, r["packageStatus"]


def t8_unsafe_output_path():
    r = call(output_dir=str(P.ROOT / "temp-sig"))
    assert r["packageStatus"] == P.BLOCKED_UNSAFE_OUTPUT_PATH, r["packageStatus"]
    r2 = call(output_dir=str(P.REPO1_ROOT / "public" / "x"))
    assert r2["packageStatus"] == P.BLOCKED_UNSAFE_OUTPUT_PATH, r2["packageStatus"]


def t9_full_package():
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r = call(output_dir=str(out))
        assert r["packageStatus"] == P.READY, r["packageStatus"]
        ub = (out / "universe.json").read_bytes()
        rb = (out / "rankings.json").read_bytes()
        assert hashlib.sha256(ub).hexdigest() == r["universeSha256"]
        assert hashlib.sha256(rb).hexdigest() == r["rankingsSha256"]
        man = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        assert man["universeSha256"] == r["universeSha256"]
        assert man["rankingsSha256"] == r["rankingsSha256"]
        assert man["executionPriceAvailable"] is False and man["officialTradeCreated"] is False
        assert man["productionWriteCount"] == 0 and man["publicCopyCount"] == 0
        assert man["nextExecutionDateCandidate"] == NEXT
        assert "financialInputManifest" in man and man["financialInputManifest"]["dartRefreshCount"] == 0
        rk = json.loads((out / "rankings.json").read_text(encoding="utf-8"))
        assert len(rk["top10"]) == 10
        for t in rk["top10"]:
            assert "signalClosePrice" in t and "marketCap" in t
            assert "buyOpenPrice" not in t and "executionPrice" not in t and "openPrice" not in t
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t10_already_prepared():
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r1 = call(output_dir=str(out))
        assert r1["packageStatus"] == P.READY, r1["packageStatus"]
        names = ("universe.json", "rankings.json", "manifest.json")
        h1 = {f: _hash(out / f) for f in names}
        m1 = {f: _mtime(out / f) for f in names}
        r2 = call(output_dir=str(out))
        assert r2["packageStatus"] == P.ALREADY_PREPARED, r2["packageStatus"]
        h2 = {f: _hash(out / f) for f in names}
        m2 = {f: _mtime(out / f) for f in names}
        assert h1 == h2 and m1 == m2, "기존 패키지 파일 불변"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t11_package_conflict():
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r1 = call(output_dir=str(out))
        assert r1["packageStatus"] == P.READY, r1["packageStatus"]
        h1 = _hash(out / "rankings.json")
        r2 = call(output_dir=str(out), build_payload_fn=lambda: fake_universe(price=2000.0))
        assert r2["packageStatus"] == P.BLOCKED_PACKAGE_CONFLICT, r2["packageStatus"]
        assert _hash(out / "rankings.json") == h1, "기존 READY 패키지 불변"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t12_mid_failure_cleanup():
    tmp = tempfile.mkdtemp()
    orig = P.os.replace
    def boom(a, b):  # work_dir 작성 후 교체 단계에서 실패 유발
        raise OSError("injected replace failure")
    P.os.replace = boom
    try:
        out = Path(tmp) / SIGNAL
        r = call(output_dir=str(out))
        assert r["packageStatus"] == P.BLOCKED_GENERATION_ERROR, r["packageStatus"]
        assert not out.exists(), "불완전한 최종 패키지 금지"
        assert not (out.parent / (out.name + ".partial")).exists(), "임시 작업 디렉터리 정리"
    finally:
        P.os.replace = orig
        shutil.rmtree(tmp, ignore_errors=True)


def t13_next_trading_day():
    cal_fri = E.make_calendar(["2030-01-04", "2030-01-07"])     # 금→월(주말 사이)
    assert P.next_krx_trading_day("2030-01-04", cal_fri) == "2030-01-07"
    assert P.next_krx_trading_day("2030-01-04", cal_fri) != "2030-01-05", "+1일 추정 금지"
    cal_hol = E.make_calendar(["2026-06-02", "2026-06-04"])     # 06-03 휴장
    assert P.next_krx_trading_day("2026-06-02", cal_hol) == "2026-06-04"
    assert P.next_krx_trading_day("2030-01-07", cal_fri) is None


def t14_financial_cache_unchanged():
    paths = [P.FINANCIAL_UNIVERSE_PATH, P.DART_CORP_CODES_PATH]
    before = {str(p): (_hash(p), _mtime(p)) for p in paths}
    listing_before = P._dart_cache_listing_hash(P.DART_CACHE_DIR)
    tmp = tempfile.mkdtemp()
    try:
        call(output_dir=str(Path(tmp) / SIGNAL))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    after = {str(p): (_hash(p), _mtime(p)) for p in paths}
    assert before == after, "재무 캐시 해시·mtime 불변"
    assert P._dart_cache_listing_hash(P.DART_CACHE_DIR) == listing_before, "DART 캐시 목록 불변"


def t15_production_json_unchanged():
    paths = [P.FINANCIAL_UNIVERSE_PATH,
             P.ROOT / "magic-formula-portfolio.json",
             P.ROOT / "magic-formula-rankings.json",
             P.ROOT / "recommendation-history.json",
             P.REPO1_ROOT / "public" / "data" / "recommendation-history.json"]
    before = {str(p): _hash(p) for p in paths}
    tmp = tempfile.mkdtemp()
    try:
        call(output_dir=str(Path(tmp) / SIGNAL))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    after = {str(p): _hash(p) for p in paths}
    assert before == after, "production JSON(REPO2/REPO1 public) 불변"


def t16_ranking_reuse_spy():
    import build_magic_formula_fund as mff
    calls = []
    orig = mff.calculate_magic_formula_ranking
    def spy(rows, mode, blacklist):
        calls.append((mode, len(rows)))
        return fake_ranking(rows, mode, blacklist)
    mff.calculate_magic_formula_ranking = spy
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        # ranking_fn 미주입 → 기본 경로가 mff.calculate_magic_formula_ranking 호출해야 함
        r = P.build_signal_package(SIGNAL, now=NOW_OK, market_close_at=CLOSE, calendar=CAL,
                                   code_commit="testcommit", formula_version="test-fv",
                                   formula_mode="book_faithful_v1",
                                   build_payload_fn=lambda: fake_universe(), output_dir=str(out))
        assert calls, "기본 ranking 경로가 calculate_magic_formula_ranking을 호출해야 함(산식 복제 0)"
        assert r["packageStatus"] == P.READY, r["packageStatus"]
    finally:
        mff.calculate_magic_formula_ranking = orig
        shutil.rmtree(tmp, ignore_errors=True)


# ----- 45-E5.2: 다음 체결일 계산 보강 / READY 게이트 -----

def t17_forward_normal_next_day():
    # 평일 다음 거래일(캘린더 미주입 → 전진 계산)
    assert P.next_krx_trading_day("2026-06-16") == "2026-06-17"
    ok, _ = P.validate_next_execution_date("2026-06-16", "2026-06-17")
    assert ok is True


def t18_forward_friday_to_monday():
    # 금(06-19)→월(06-22): 주말 건너뜀, +1일 아님
    assert P.next_krx_trading_day("2026-06-19") == "2026-06-22"
    assert P.next_krx_trading_day("2026-06-19") != "2026-06-20"


def t19_forward_pre_holiday_to_post_holiday():
    # 광복절(08-15 토)+대체(08-17 월) 건너뛰고 08-18(화). 내장 KRX_HOLIDAYS 사용
    assert P.next_krx_trading_day("2026-08-14") == "2026-08-18"


def t20_forward_month_end_rollover():
    # 월말(01-30 금)→다음 달(02-02 월)
    assert P.next_krx_trading_day("2026-01-30") == "2026-02-02"


def t21_forward_year_end_rollover():
    # 연말(12-30)→다음 연도(2027-01-04): 12-31 폐장·01-01 신정·주말 건너뜀
    assert P.next_krx_trading_day("2026-12-30") == "2027-01-04"


def t22_calendar_no_later_day_returns_none():
    cal_only = E.make_calendar([SIGNAL])          # signal만, 이후 거래일 없음
    assert P.next_krx_trading_day(SIGNAL, cal_only) is None
    assert P.next_krx_trading_day("nope") is None  # 잘못된 입력도 None(전진 경로)


def t23_next_calc_exception_blocked():
    orig = P.next_krx_trading_day
    def boom(*a, **k):
        raise RuntimeError("injected trading-day calc failure")
    P.next_krx_trading_day = boom
    tmp = tempfile.mkdtemp()
    try:
        r = call(output_dir=str(Path(tmp) / SIGNAL))
        assert r["packageStatus"] == P.BLOCKED_GENERATION_ERROR, r["packageStatus"]
        assert not (Path(tmp) / SIGNAL).exists()
    finally:
        P.next_krx_trading_day = orig
        shutil.rmtree(tmp, ignore_errors=True)


def t24_null_candidate_blocks_ready():
    cal_only = E.make_calendar([SIGNAL])          # 다음 거래일 없음 → candidate null
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r = call(output_dir=str(out), calendar=cal_only)
        assert r["packageStatus"] == P.BLOCKED_NEXT_EXECUTION_DATE_UNAVAILABLE, r["packageStatus"]
        assert r["blocked"] is True
        assert not out.exists(), "READY 아님 → 파일 생성 0"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t25_candidate_not_after_signal_blocked():
    ok_eq, _ = P.validate_next_execution_date("2026-06-16", "2026-06-16")
    ok_before, _ = P.validate_next_execution_date("2026-06-16", "2026-06-15")
    assert ok_eq is False and ok_before is False


def t26_candidate_skips_trading_day_blocked():
    cal = E.make_calendar(["2026-06-16", "2026-06-17", "2026-06-18"])
    ok_cal, _ = P.validate_next_execution_date("2026-06-16", "2026-06-18", cal)   # 06-17 스킵
    assert ok_cal is False
    ok_fwd, _ = P.validate_next_execution_date("2026-06-16", "2026-06-18")        # 전진 경로도 06-17 스킵
    assert ok_fwd is False


def t27_existing_package_readonly_next_exec():
    tmp = tempfile.mkdtemp()
    try:
        out = Path(tmp) / SIGNAL
        r = call(output_dir=str(out))
        assert r["packageStatus"] == P.READY, r["packageStatus"]
        names = ("universe.json", "rankings.json", "manifest.json")
        before = {f: (_hash(out / f), _mtime(out / f)) for f in names}
        info = P.read_only_next_execution_for_package(out)
        assert info["signalAsOfDate"] == SIGNAL
        assert info["nextExecutionDateCandidate"] == "2026-06-17"
        assert info["valid"] is True and info["writeCount"] == 0 and info["filesModified"] is False
        after = {f: (_hash(out / f), _mtime(out / f)) for f in names}
        assert before == after, "기존 TEMP 패키지 해시·mtime 불변(read-only)"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t28_forward_path_production_unchanged():
    paths = [P.FINANCIAL_UNIVERSE_PATH,
             P.ROOT / "magic-formula-portfolio.json",
             P.ROOT / "recommendation-history.json",
             P.REPO1_ROOT / "public" / "data" / "recommendation-history.json"]
    before = {str(p): _hash(p) for p in paths}
    tmp = tempfile.mkdtemp()
    try:
        r = call(output_dir=str(Path(tmp) / SIGNAL), calendar=None)   # 전진(휴장표) 경로
        assert r["packageStatus"] == P.READY, r["packageStatus"]
        assert r["nextExecutionDateCandidate"] == "2026-06-17"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    after = {str(p): _hash(p) for p in paths}
    assert before == after, "production JSON 불변(전진 경로)"


TESTS = [
    ("1  장마감 전→MARKET_NOT_CLOSED(생성0)", t1_market_not_closed),
    ("2  tz-naive now/close→BLOCKED", t2_tz_naive_blocked),
    ("3  정상 baseDate==signal→READY", t3_ready_basic),
    ("4  이전 거래일 universe→UNIVERSE_NOT_READY", t4_universe_prev_day),
    ("5  가격 0/null/음수→DATE_MISMATCH(ranking0)", t5_invalid_prices_date_mismatch),
    ("6  종목코드 중복→BLOCKED", t6_duplicate_codes),
    ("7  ranking top10 미완성→MISSING_RANKING", t7_missing_ranking),
    ("8  production 경로 output→UNSAFE_OUTPUT_PATH", t8_unsafe_output_path),
    ("9  정상 패키지(3파일/SHA/persist0)", t9_full_package),
    ("10 동일 입력 재실행→ALREADY_PREPARED(불변)", t10_already_prepared),
    ("11 동일 날짜 다른 해시→PACKAGE_CONFLICT(불변)", t11_package_conflict),
    ("12 중간 실패→불완전0+임시정리", t12_mid_failure_cleanup),
    ("13 다음 KRX 거래일(금→월/휴장)", t13_next_trading_day),
    ("14 재무 캐시 해시·mtime 불변", t14_financial_cache_unchanged),
    ("15 production JSON 불변(REPO2/REPO1)", t15_production_json_unchanged),
    ("16 ranking 함수 재사용 spy(복제0)", t16_ranking_reuse_spy),
    ("17 전진 정상 다음 거래일(06-16→17)", t17_forward_normal_next_day),
    ("18 전진 금→월(주말 건너뜀)", t18_forward_friday_to_monday),
    ("19 전진 휴장전→휴장후(광복절)", t19_forward_pre_holiday_to_post_holiday),
    ("20 전진 월말→다음 달", t20_forward_month_end_rollover),
    ("21 전진 연말→다음 연도", t21_forward_year_end_rollover),
    ("22 거래일 set 빈 결과/잘못된 입력→None", t22_calendar_no_later_day_returns_none),
    ("23 다음거래일 계산 예외→GENERATION_ERROR", t23_next_calc_exception_blocked),
    ("24 candidate null→READY 금지(생성0)", t24_null_candidate_blocks_ready),
    ("25 candidate≤signal→BLOCKED", t25_candidate_not_after_signal_blocked),
    ("26 중간 거래일 스킵 candidate→BLOCKED", t26_candidate_skips_trading_day_blocked),
    ("27 기존 TEMP 패키지 read-only 다음거래일+불변", t27_existing_package_readonly_next_exec),
    ("28 전진 경로 production 불변", t28_forward_path_production_unchanged),
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
