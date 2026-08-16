#!/usr/bin/env python3
# test_magic_execution_profile.py
# WABABA-MAGIC-FORMULA-EXECUTION-PROFILE-R1 회귀
#
# 고정하려는 것:
#   같은 마법공식 엔진을 CORE(매 거래일) / EASY(주 1회) 두 실행 프로파일로 굴리되,
#   ① profile 을 주지 않으면 기존 Core 와 **결과가 완전히 동일**해야 하고(동결 보호)
#   ② WEEKLY_FIRST_TRADING_DAY cadence 가 '그 주 첫 실제 거래일'을 정확히 고르며
#   ③ 비매수 거래일에도 평가·거래일 index 는 전진하고 보유는 유지되고
#   ④ state schema 가 깨지지 않아야 한다.
#
# 방식: 전부 순수 함수 + fixture. 실 canonical·네트워크·파일 write·apply 체인 미접근.
#       실주문 0 · 브로커 0 · publish 0.
# 사용: python scripts\test_magic_execution_profile.py

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import magic_rolling_engine as E  # noqa: E402

_pass = 0
_fail = 0


def check(name, actual, expected):
    global _pass, _fail
    if actual == expected:
        _pass += 1
        print(f"  PASS  {name}")
    else:
        _fail += 1
        print(f"  FAIL  {name}  expected={expected!r} actual={actual!r}")


CODES = ["046940", "461300", "088130", "124500", "184230",
         "171090", "053580", "052400", "018290", "484870"]
PX = {c: 10_000 + i * 137 for i, c in enumerate(CODES)}
RANKING = [{"code": c, "name": f"N{i}", "rank": i + 1, "combinedRank": i + 1,
            "profitabilityRank": i + 1, "valueRank": i + 1,
            "returnOnCapital": 0.3, "earningsYield": 0.2} for i, c in enumerate(CODES)]

# 2026-06 fixture. ★ 2주차 월요일(06-08)을 휴장으로 뺐다 → 그 주 첫 거래일은 화요일(06-09).
WEEK1 = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]
WEEK2 = ["2026-06-09", "2026-06-10", "2026-06-11", "2026-06-12"]          # 06-08(월) 휴장
WEEK3 = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"]
DAYS = WEEK1 + WEEK2 + WEEK3
CAL = E.make_calendar(DAYS)
NOW = "2026-06-30T18:00:00+09:00"

EASY = {"profileId": "EASY", "cadence": E.CADENCE_WEEKLY_FIRST}


def run_days(days, profile=None, state=None):
    """고정 now 로 결정적으로 돌린다(createdAt 비교 가능)."""
    st = state if state is not None else E.empty_official_state()
    results = []
    for d in days:
        st, res = E.plan_official_day(st, d, RANKING, PX, PX, CAL, now=NOW, profile=profile)
        results.append(res)
    return st, results


def sha(obj):
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


# ── A) Core baseline — profile 미지정 == 명시 CORE == 부분 override ──────────────
print("[A] Core 기본 동작 동일성 (동결 보호)")
st_none, res_none = run_days(DAYS)
st_core, res_core = run_days(DAYS, profile=E.CORE_PROFILE)
st_part, _ = run_days(DAYS, profile={"cadence": E.CADENCE_DAILY})
st_empty, _ = run_days(DAYS, profile={})
st_nonekeys, _ = run_days(DAYS, profile={"topN": None, "holdTradingDays": None})

check("A1 profile 미지정 == 명시 CORE (SHA)", sha(st_none), sha(st_core))
check("A2 부분 override(cadence=DAILY) 동일", sha(st_part), sha(st_none))
check("A3 빈 dict 동일", sha(st_empty), sha(st_none))
check("A4 None 값 키는 무시 → 동일", sha(st_nonekeys), sha(st_none))
check("A5 일별 결과도 동일", sha(res_none), sha(res_core))
check("A6 Core 는 매 거래일 COMPLETED", [r["runStatus"] for r in res_none],
      [E.COMPLETED] * len(DAYS))
check("A7 Core seq == 거래일 수", st_none["officialSequence"], len(DAYS))
check("A8 Core lot == 거래일 수 * 10", len(st_none["itemLots"]), len(DAYS) * 10)
check("A9 Core 는 CADENCE_NON_BUY_DAY 를 만들지 않는다",
      any(r["runStatus"] == E.CADENCE_NON_BUY_DAY for r in res_none), False)

# 프로파일 상수가 정본(모듈 상수)을 그대로 참조하는지 — 값 재입력 금지 계약
check("A10 CORE_PROFILE.topN == TOP_N", E.CORE_PROFILE["topN"], E.TOP_N)
check("A11 CORE_PROFILE.holdTradingDays == HOLD_TRADING_DAYS",
      E.CORE_PROFILE["holdTradingDays"], E.HOLD_TRADING_DAYS)
check("A12 CORE_PROFILE.batchCapital == INITIAL_BATCH_CAPITAL",
      E.CORE_PROFILE["batchCapital"], E.INITIAL_BATCH_CAPITAL)
check("A13 CORE_PROFILE.maxOpenBatches == MAX_OPEN_BATCHES",
      E.CORE_PROFILE["maxOpenBatches"], E.MAX_OPEN_BATCHES)

# 하위 함수도 profile 미지정 시 기존 동작
check("A14 allocate_quantities 기본 == 명시 CORE",
      E.allocate_quantities(RANKING, PX, 1_000_000.0),
      E.allocate_quantities(RANKING, PX, 1_000_000.0, profile=E.CORE_PROFILE))
check("A15 due_official_batches 기본 == 명시 CORE",
      sha(E.due_official_batches(st_none, 999)),
      sha(E.due_official_batches(st_none, 999, profile=E.CORE_PROFILE)))
check("A16 migrate_official_state_indices 기본 == 명시 CORE",
      sha(E.migrate_official_state_indices(st_none)),
      sha(E.migrate_official_state_indices(st_none, profile=E.CORE_PROFILE)))

# ── B) 프로파일 해석/검증 ───────────────────────────────────────────────────────
print("[B] 프로파일 해석·검증")
check("B1 None → CORE", E.resolve_execution_profile(None), E.CORE_PROFILE)
check("B2 부분 override 는 나머지 유지",
      E.resolve_execution_profile({"cadence": E.CADENCE_WEEKLY_FIRST})["holdTradingDays"],
      E.HOLD_TRADING_DAYS)
check("B3 CORE_PROFILE 원본 불변(반환 dict 수정해도)",
      (lambda p: (p.update({"topN": 99}), E.CORE_PROFILE["topN"])[1])(E.resolve_execution_profile()),
      E.TOP_N)
try:
    E.resolve_execution_profile({"cadence": "MONTHLY"})
    check("B4 알 수 없는 cadence → ValueError", "no-raise", "ValueError")
except ValueError:
    check("B4 알 수 없는 cadence → ValueError", "ValueError", "ValueError")
try:
    E.resolve_execution_profile({"topN": 0})
    check("B5 topN<1 → ValueError", "no-raise", "ValueError")
except ValueError:
    check("B5 topN<1 → ValueError", "ValueError", "ValueError")

# ── C) WEEKLY_FIRST_TRADING_DAY cadence ────────────────────────────────────────
print("[C] WEEKLY_FIRST_TRADING_DAY")
check("C1 1주차 월요일 = 시작일", E.is_batch_start_day("2026-06-01", CAL, EASY), True)
check("C2 1주차 화요일 = 아님", E.is_batch_start_day("2026-06-02", CAL, EASY), False)
check("C3 1주차 금요일 = 아님", E.is_batch_start_day("2026-06-05", CAL, EASY), False)
check("C4 ★월요일 휴장 → 그 주 첫 실제 거래일(화)", E.is_batch_start_day("2026-06-09", CAL, EASY), True)
check("C5 그 주 수요일 = 아님", E.is_batch_start_day("2026-06-10", CAL, EASY), False)
check("C6 휴장일 자체 = 아님", E.is_batch_start_day("2026-06-08", CAL, EASY), False)
check("C7 3주차 월요일 = 시작일", E.is_batch_start_day("2026-06-15", CAL, EASY), True)
check("C8 DAILY 는 항상 True", [E.is_batch_start_day(d, CAL) for d in DAYS], [True] * len(DAYS))

# 미래 데이터 사용 0 — 같은 주의 '뒤' 거래일 유무가 판정을 바꾸면 안 된다.
cal_trunc = E.make_calendar(WEEK1[:2] + WEEK2 + WEEK3)      # 06-03~05 제거(06-01 이후 날짜)
check("C9 look-ahead 0 (뒤 거래일 제거해도 동일)",
      E.is_batch_start_day("2026-06-01", cal_trunc, EASY),
      E.is_batch_start_day("2026-06-01", CAL, EASY))

st_e, res_e = run_days(DAYS, profile=EASY)
completed = [d for d, r in zip(DAYS, res_e) if r["runStatus"] == E.COMPLETED]
nonbuy = [d for d, r in zip(DAYS, res_e) if r["runStatus"] == E.CADENCE_NON_BUY_DAY]
check("C10 신규 batch 는 주당 1회", completed, ["2026-06-01", "2026-06-09", "2026-06-15"])
check("C11 나머지 거래일은 비매수", len(nonbuy), len(DAYS) - 3)
check("C12 seq == 주 수", st_e["officialSequence"], 3)
check("C13 executionCalendar == 매수일만", st_e["officialExecutionCalendar"], completed)
check("C14 KRX 캘린더는 전 거래일", st_e["officialKrxTradingCalendar"], DAYS)
check("C15 거래일 index 는 매 거래일 전진", st_e["officialTradingDayIndex"], len(DAYS))
check("C16 lot == 3배치 * 10", len(st_e["itemLots"]), 30)
check("C17 현금은 3회분만 차감",
      st_e["officialAvailableCash"], E.INITIAL_CAPITAL - 3 * E.INITIAL_BATCH_CAPITAL)
check("C18 비매수일엔 원장 증가 0", len(st_e["buyLedger"]), 30)
check("C19 평가 스냅샷은 매 거래일", len(st_e["evaluationSnapshots"]), len(DAYS))

# 종목선정은 Core 와 동일해야 한다(같은 날짜 = 같은 top10)
core_d1 = [l["code"] for l in st_none["itemLots"] if l["buyDate"] == "2026-06-01"]
easy_d1 = [l["code"] for l in st_e["itemLots"] if l["buyDate"] == "2026-06-01"]
check("C20 selection 동일(같은 날 같은 top10)", easy_d1, core_d1)
check("C21 selection 은 ranking 상위 10", easy_d1, CODES)

# 비매수일 ledger 가 소비처를 깨지 않는지 — COMPLETED ledger 와 키 집합 비교
comp_led = next(r for r in res_e if r["runStatus"] == E.COMPLETED)
nb_led = next(r for r in res_e if r["runStatus"] == E.CADENCE_NON_BUY_DAY)
check("C22 비매수 ledger 키 ⊆ COMPLETED 키", set(nb_led).issubset(set(comp_led)), True)
check("C23 비매수일 marketOpen=True(휴장과 구분)", nb_led.get("marketOpen"), True)

# ── C-2) 보유 lifecycle / 만기 매도 ────────────────────────────────────────────
print("[C-2] 비매수일에도 보유 유지 · 만기 매도는 다음 매수일에")
SHORT = {"profileId": "EASY_SHORT", "cadence": E.CADENCE_WEEKLY_FIRST, "holdTradingDays": 5}
st_s, res_s = run_days(DAYS, profile=SHORT)
# 06-01(idx1) 매수 → plannedSell = 1+5 = 6 → idx6 = 06-09(그 주 첫 거래일)
b1 = st_s["batches"][0]
check("C24 만기 index 가 profile hold 반영", b1["plannedSellTradingDayIndex"], 1 + 5)
sell_dates = sorted({s["date"] for s in st_s["sellLedger"]})
# idx 매핑: 06-01=1 … 06-09=6 … 06-15=10, 06-16=11 … 06-19=14
#   batch1 buyIdx1 → 만기 idx6 = 06-09(매수일) → 그 날 롤오버 매도 ✔
#   batch2 buyIdx6 → 만기 idx11 = 06-16(비매수일) → **다음 매수일로 이월**되어 창 안에서는 매도 없음
check("C25 매도는 매수일에만 발생(비매수일 매도 0)", sell_dates, ["2026-06-09"])
check("C26 만기 batch 는 CLOSED", st_s["batches"][0]["status"], "CLOSED")
check("C27 매도일은 전부 매수일 집합에 포함", all(d in completed for d in sell_dates), True)
# ★ 이월 계약 고정: 비매수일에 만기가 와도 조용히 매도하지 않고 OPEN 으로 남긴다(미기록 매도 0).
b2 = st_s["batches"][1]
check("C27b 비매수일 만기 batch 는 OPEN 유지(이월)", b2["status"], "OPEN")
check("C27c 그 batch 는 실제로 만기 도래 상태",
      b2["plannedSellTradingDayIndex"] <= st_s["officialTradingDayIndex"], True)
check("C27d 이월돼도 due 는 1건(2건 이상 = BLOCKED 조건 미발생)",
      len(E.due_official_batches(st_s, st_s["officialTradingDayIndex"], profile=SHORT)), 1)
# 비매수일에 보유가 유지되는지 — 06-02(비매수일) 시점 평가에 lot 이 살아있다
ev_0602 = next(e for e in st_s["evaluationSnapshots"] if e["date"] == "2026-06-02")
check("C28 비매수일에도 보유 평가 유지", ev_0602["totalAsset"] > 0, True)

# ── D) state / schema 호환 ─────────────────────────────────────────────────────
print("[D] state·schema 비파괴")
check("D1 EASY state 키 == CORE state 키", sorted(st_e.keys()), sorted(st_none.keys()))
check("D2 EASY state 키 == empty_official_state 키",
      sorted(st_e.keys()), sorted(E.empty_official_state().keys()))
check("D3 batch 키 동일", sorted(st_e["batches"][0].keys()), sorted(st_none["batches"][0].keys()))
check("D4 itemLot 키 동일", sorted(st_e["itemLots"][0].keys()), sorted(st_none["itemLots"][0].keys()))
check("D5 profile 은 state 에 새 키를 심지 않는다",
      set(st_e.keys()) - set(E.empty_official_state().keys()), set())
check("D6 EASY state 도 마이그레이션 idempotent",
      sha(E.migrate_official_state_indices(st_e, profile=EASY)),
      sha(E.migrate_official_state_indices(
          E.migrate_official_state_indices(st_e, profile=EASY), profile=EASY)))

# 입력 state 불변(순수함수 계약)
before = sha(st_none)
_ = E.plan_official_day(st_none, "2026-06-22", RANKING, PX, PX,
                        E.make_calendar(DAYS + ["2026-06-22"]), now=NOW, profile=EASY)
check("D7 입력 state 불변(deepcopy 계약)", sha(st_none), before)

# ── E) 안전 정적 ───────────────────────────────────────────────────────────────
print("[E] 안전 정적")
src = Path(E.__file__).read_text(encoding="utf-8")
for bad in ("smtplib", "place_order", "send_order", "broker_api", "kiwoom", "주문번호"):
    check(f"E 미사용(코드): {bad}", bad in src, False)
# 엔진은 순수 로직 — 네트워크/시세조회 라이브러리를 import 하지 않는다.
# ("pykrx" 문자열 자체는 PRICE_SOURCE_TRADE="pykrx_open" 라벨로만 존재 → import 여부로 검사한다.)
for mod in ("pykrx", "requests", "urllib"):
    check(f"E import 없음: {mod}",
          (f"import {mod}" in src) or (f"from {mod}" in src), False)

print()
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
