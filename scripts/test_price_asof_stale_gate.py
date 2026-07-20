# test_price_asof_stale_gate.py
# 시세 최신성 게이트(WABABA-PRICE-ASOF-STALE-GATE-20260720) 회귀 테스트.
#   목적: 실제 시세 거래일(priceAsOf)이 산출물에 전파되고, 거래일 기준 stale 판정이
#         PASS / WARNING_CACHED / WAIT_EXTERNAL / BLOCKED_NO_DATA 로 정직하게 갈리는 계약을 고정한다.
#   방식: 순수 함수만 호출. 운영 파일(financial-universe-real.json / recommendation-history.json 등)
#         읽기·쓰기 0. 실주문·외부 호출 0.
# 사용: python scripts\test_price_asof_stale_gate.py

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_recommendation_history import (  # noqa: E402
    evaluate_price_freshness,
    get_price_as_of,
    get_price_source,
    latest_trading_day_on_or_before,
    count_trading_days_between,
)

# 2026-07 기준 달력
#   07-17(금) 거래일 / 07-18(토) / 07-19(일) / 07-20(월) 거래일 / 07-21(화) 거래일
#   공휴일 시나리오용: 07-21 을 임시 공휴일로 지정한 정책을 별도로 쓴다.
POLICY = {"marketHolidays": []}
POLICY_HOLIDAY_0721 = {"marketHolidays": ["2026-07-21"]}

_pass = 0
_fail = 0


def check(name, actual, expected):
    global _pass, _fail
    if str(actual) == str(expected):
        _pass += 1
        print(f"  PASS  {name}  (= {actual})")
    else:
        _fail += 1
        print(f"  FAIL  {name}  expected={expected} actual={actual}")


def status_of(price_as_of, ref, policy=POLICY):
    return evaluate_price_freshness(price_as_of, ref, policy)


print("=== 시세 최신성 게이트 회귀 테스트 (fixture 전용, 운영파일 미접근) ===")

# 1) 최신 거래일 시세 → PASS
r = status_of("2026-07-20", date(2026, 7, 20))
print("[1] 최신 거래일 시세(월) → PASS")
check("status", r["priceFreshnessStatus"], "PASS")
check("staleTradingDays", r["priceStaleTradingDays"], 0)
check("priceAsOf", r["priceAsOf"], "2026-07-20")

# 2) 주말(일요일)에 직전 금요일 시세 → PASS (달력 2일 경과지만 거래일 0)
r = status_of("2026-07-17", date(2026, 7, 19))
print("[2] 일요일 실행 + 금요일 시세 → PASS")
check("status", r["priceFreshnessStatus"], "PASS")
check("staleTradingDays", r["priceStaleTradingDays"], 0)

# 3) 공휴일(07-21 휴장) 실행 + 직전 거래일(07-20) 시세 → PASS
r = evaluate_price_freshness("2026-07-20", date(2026, 7, 21), POLICY_HOLIDAY_0721)
print("[3] 공휴일 실행 + 직전 거래일 시세 → PASS")
check("status", r["priceFreshnessStatus"], "PASS")
check("staleTradingDays", r["priceStaleTradingDays"], 0)

# 4) 1거래일 stale → WARNING_CACHED
r = status_of("2026-07-17", date(2026, 7, 20))
print("[4] 1거래일 stale(금 시세 / 월 실행) → WARNING_CACHED")
check("status", r["priceFreshnessStatus"], "WARNING_CACHED")
check("staleTradingDays", r["priceStaleTradingDays"], 1)

# 5) 2거래일 stale → WARNING_CACHED
r = status_of("2026-07-16", date(2026, 7, 20))
print("[5] 2거래일 stale → WARNING_CACHED")
check("status", r["priceFreshnessStatus"], "WARNING_CACHED")
check("staleTradingDays", r["priceStaleTradingDays"], 2)

# 6) 3거래일 이상 stale → WAIT_EXTERNAL (최초 사고 재현: 07-16 시세를 07-21 에 사용)
r = status_of("2026-07-15", date(2026, 7, 20))
print("[6] 3거래일 stale → WAIT_EXTERNAL")
check("status", r["priceFreshnessStatus"], "WAIT_EXTERNAL")
check("staleTradingDays", r["priceStaleTradingDays"], 3)

# 7) 시세 데이터 없음 → BLOCKED_NO_DATA
r = status_of(None, date(2026, 7, 20))
print("[7] 시세 기준일 없음 → BLOCKED_NO_DATA")
check("status", r["priceFreshnessStatus"], "BLOCKED_NO_DATA")
check("staleTradingDays(None)", r["priceStaleTradingDays"] is None, True)

# 8) 손상된 날짜 → BLOCKED_NO_DATA
r = status_of("20260720ABC", date(2026, 7, 20))
print("[8] 손상된 날짜 → BLOCKED_NO_DATA")
check("status", r["priceFreshnessStatus"], "BLOCKED_NO_DATA")

# 9) 미래 날짜 → BLOCKED_NO_DATA (명시적 사유)
r = status_of("2026-07-25", date(2026, 7, 20))
print("[9] 미래 날짜 → BLOCKED_NO_DATA")
check("status", r["priceFreshnessStatus"], "BLOCKED_NO_DATA")
check("reason 에 '미래' 포함", "미래" in (r["priceFreshnessReason"] or ""), True)

# 10) NO_SIGNAL + 최신 시세 → 데이터 장애 아님(PASS 유지)
#     NO_SIGNAL 은 매매신호 없음이지 데이터 상태가 아니므로 freshness 는 그대로 PASS 여야 한다.
r = status_of("2026-07-20", date(2026, 7, 20))
print("[10] NO_SIGNAL + 최신 시세 → PASS(신호없음은 장애 아님)")
check("status", r["priceFreshnessStatus"], "PASS")

# 11) 과거 fixture(신규 필드 없는 메타) 호환 — 최상위 baseDate 만 있는 구형 스냅샷
legacy_snapshot = {"baseDate": "2026-07-20", "data": []}
print("[11] 구형 스냅샷(최상위 baseDate) 호환")
check("get_price_as_of", get_price_as_of(legacy_snapshot), "2026-07-20")
check("priceSource fallback", get_price_source(legacy_snapshot), "unknown")

# 12) 현행 스냅샷 구조(meta.baseDate) 추출 — 원 결함 회귀 고정
#     최상위에는 baseDate 가 없고 meta 에만 있다. 예전 로직은 여기서 today 로 폴백했다.
current_snapshot = {
    "data": [],
    "meta": {"baseDate": "2026-07-16", "provider": "pykrx-daily-fast+cached-opendart-financials"},
}
print("[12] 현행 스냅샷(meta.baseDate) 추출 — today 폴백 금지")
check("get_price_as_of", get_price_as_of(current_snapshot), "2026-07-16")
check("priceSource", get_price_source(current_snapshot).startswith("pykrx"), True)
r = status_of(get_price_as_of(current_snapshot), date(2026, 7, 20))
check("stale 판정", r["priceFreshnessStatus"], "WARNING_CACHED")

# 13) 스냅샷 자체가 없음/비정상 → None → BLOCKED_NO_DATA 로 이어짐
print("[13] 스냅샷 None/형식이상")
check("get_price_as_of(None)", get_price_as_of(None) is None, True)
check("get_price_as_of({})", get_price_as_of({}) is None, True)
check("이어지는 판정", status_of(get_price_as_of({}), date(2026, 7, 20))["priceFreshnessStatus"], "BLOCKED_NO_DATA")

# 14) 거래일 헬퍼 단위 검증(주말 건너뜀)
print("[14] 거래일 헬퍼")
check("최근거래일(일요일 기준)", latest_trading_day_on_or_before(date(2026, 7, 19), POLICY).strftime("%Y-%m-%d"), "2026-07-17")
check("거래일수 금→월", count_trading_days_between(date(2026, 7, 17), date(2026, 7, 20), POLICY), 1)
check("거래일수 동일일", count_trading_days_between(date(2026, 7, 20), date(2026, 7, 20), POLICY), 0)
check("역전(start>end)", count_trading_days_between(date(2026, 7, 21), date(2026, 7, 20), POLICY) is None, True)

print("")
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
