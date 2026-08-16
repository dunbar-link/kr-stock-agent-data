#!/usr/bin/env python3
# test_kospi_benchmark.py
# WABABA-KOSPI-BENCHMARK-R1 회귀
#
# 고정하려는 것:
#   A 정규화  — 각 펀드 시작일 D0 를 0% 로 맞추고 excess 를 %p 로 계산
#   B 캘린더  — 주말/휴장/벤치마크 결측 처리, **미래 벤치마크 값 사용 0**
#   C 공급자  — 기존 krx_fetch_guard 재사용(재시도·fail-closed), 캐시 재사용
#   D Core    — 실제 canonical 로 생성해도 canonical/state 를 건드리지 않음
#   E Easy-ready — 서로 다른 시작일 펀드가 각각 독립적으로 0% 정규화
#
# 방식: fetcher 주입 fixture. **네트워크 0**, 운영 캐시 미접근(임시 폴더만).
# 사용: python scripts\test_kospi_benchmark.py

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import kospi_benchmark as B  # noqa: E402
import krx_fetch_guard as G  # noqa: E402

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


def close_to(a, b, eps=1e-9):
    return a is not None and abs(a - b) < eps


# 펀드 NAV fixture: 5 거래일. D0=5천만 → +10% → -5% → +20% → 0%
FUND = [
    {"date": "2026-06-01", "nav": 50_000_000.0},
    {"date": "2026-06-02", "nav": 55_000_000.0},
    {"date": "2026-06-03", "nav": 47_500_000.0},
    {"date": "2026-06-04", "nav": 60_000_000.0},
    {"date": "2026-06-05", "nav": 50_000_000.0},
]
# KOSPI fixture: 2000 기준 → +5% → -10% → +5% → 0%
KOSPI = {
    "2026-06-01": 2000.0,
    "2026-06-02": 2100.0,
    "2026-06-03": 1800.0,
    "2026-06-04": 2100.0,
    "2026-06-05": 2000.0,
}

# ── A) 정규화 ──────────────────────────────────────────────────────────────────
print("[A] 정규화")
r = B.build_benchmark_series(FUND, KOSPI)
check("A1 status OK", r["status"], B.STATUS_OK)
check("A2 baseDate == 펀드 시작일", r["baseDate"], "2026-06-01")
check("A3 baseDate 이동 없음", r["baseDateShifted"], False)
check("A4 시계열 길이", len(r["series"]), 5)

s = {x["date"]: x for x in r["series"]}
check("A5 D0 fund = 0%", close_to(s["2026-06-01"]["fundReturnPct"], 0.0), True)
check("A6 D0 KOSPI = 0%", close_to(s["2026-06-01"]["benchmarkReturnPct"], 0.0), True)
check("A7 D0 excess = 0%p", close_to(s["2026-06-01"]["excessReturnPctPoint"], 0.0), True)

check("A8 상승 fund +10%", close_to(s["2026-06-02"]["fundReturnPct"], 10.0), True)
check("A9 상승 KOSPI +5%", close_to(s["2026-06-02"]["benchmarkReturnPct"], 5.0), True)
check("A10 excess +5%p", close_to(s["2026-06-02"]["excessReturnPctPoint"], 5.0), True)

check("A11 하락 fund -5%", close_to(s["2026-06-03"]["fundReturnPct"], -5.0), True)
check("A12 하락 KOSPI -10%", close_to(s["2026-06-03"]["benchmarkReturnPct"], -10.0), True)
check("A13 하락 excess +5%p", close_to(s["2026-06-03"]["excessReturnPctPoint"], 5.0), True)

check("A14 fund +20% / KOSPI +5% → excess +15%p",
      close_to(s["2026-06-04"]["excessReturnPctPoint"], 15.0), True)
check("A15 원위치 fund 0% / KOSPI 0% → excess 0%p",
      close_to(s["2026-06-05"]["excessReturnPctPoint"], 0.0), True)

# 절대 지수값을 시계열에 섞지 않는다(% 축 오염 방지)
check("A16 series 는 % 필드만(지수 절대값 없음)",
      sorted(r["series"][0].keys()),
      ["benchmarkReturnPct", "date", "excessReturnPctPoint", "fundReturnPct"])
check("A17 기준값은 메타에만 보관", (r["benchmarkBaseClose"], r["fundBaseNav"]), (2000.0, 50_000_000.0))
# 반올림하지 않는다(표시 단계 책임)
odd = B.build_benchmark_series([{"date": "d0", "nav": 3.0}, {"date": "d1", "nav": 10.0}],
                               {"d0": 3.0, "d1": 10.0})
check("A18 내부 원정밀도 유지(반올림 0)",
      odd["series"][1]["fundReturnPct"], (10.0 / 3.0 - 1.0) * 100.0)

# ── B) 캘린더/결측 ─────────────────────────────────────────────────────────────
print("[B] 캘린더·결측")
kospi_gap = dict(KOSPI)
del kospi_gap["2026-06-03"]                      # 벤치마크만 결측인 날
r2 = B.build_benchmark_series(FUND, kospi_gap)
check("B1 결측일은 시계열에서 제외(OMIT)", [x["date"] for x in r2["series"]],
      ["2026-06-01", "2026-06-02", "2026-06-04", "2026-06-05"])
check("B2 결측일을 숨기지 않고 기록", r2["missingBenchmarkDates"], ["2026-06-03"])
check("B3 결측이 있어도 나머지 계산은 정상",
      close_to(r2["series"][-1]["excessReturnPctPoint"], 0.0), True)

# 벤치마크에만 있는 날짜(펀드 휴장)는 축에 들어오지 않는다
kospi_extra = dict(KOSPI, **{"2026-06-06": 2500.0, "2026-05-29": 1900.0})
r3 = B.build_benchmark_series(FUND, kospi_extra)
check("B4 펀드 거래일만 축으로 사용", [x["date"] for x in r3["series"]],
      [f["date"] for f in FUND])

# ★ look-ahead 0 — 미래 벤치마크 값을 추가해도 기존 행이 바뀌면 안 된다
r_base = B.build_benchmark_series(FUND[:3], KOSPI)
r_future = B.build_benchmark_series(FUND[:3], kospi_extra)
check("B5 미래 벤치마크 추가해도 기존 행 불변",
      json.dumps(r_base["series"], sort_keys=True),
      json.dumps(r_future["series"], sort_keys=True))
# carry-forward 를 하지 않는다 = 결측일 다음 행이 직전 종가를 쓰지 않는다
check("B6 carry-forward 0(결측 다음 행은 자기 날짜 종가 사용)",
      close_to([x for x in r2["series"] if x["date"] == "2026-06-04"][0]["benchmarkReturnPct"], 5.0),
      True)

# BLOCK 정책
r4 = B.build_benchmark_series(FUND, kospi_gap, missing_policy=B.MISSING_BLOCK)
check("B7 policy=BLOCK 이면 결측 시 실패", r4["status"], B.STATUS_MISSING_BLOCKED)
check("B8 BLOCK 이어도 결측 목록 보존", r4["missingBenchmarkDates"], ["2026-06-03"])

# 시작일에 벤치마크가 없으면 첫 공통 거래일로 baseline 이동(조용히 옮기지 않는다)
kospi_late = {d: v for d, v in KOSPI.items() if d >= "2026-06-03"}
r5 = B.build_benchmark_series(FUND, kospi_late)
check("B9 D0 결측 → 첫 공통일로 이동", r5["baseDate"], "2026-06-03")
check("B10 이동 사실을 기록", r5["baseDateShifted"], True)
check("B11 요청 시작일도 보존", r5["requestedBaseDate"], "2026-06-01")
check("B12 이동 후 D0 는 0%", close_to(r5["series"][0]["fundReturnPct"], 0.0), True)
check("B13 이동 전 날짜는 시계열에 없음",
      any(x["date"] < "2026-06-03" for x in r5["series"]), False)

check("B14 공통 거래일 0 → BLOCKED",
      B.build_benchmark_series(FUND, {"2020-01-02": 100.0})["status"], B.STATUS_NO_COMMON_DATE)
check("B15 빈 펀드 시계열 → BLOCKED",
      B.build_benchmark_series([], KOSPI)["status"], B.STATUS_NO_COMMON_DATE)
try:
    B.build_benchmark_series(FUND, KOSPI, missing_policy="INTERPOLATE")
    check("B16 임의 보간 정책 거부", "no-raise", "ValueError")
except ValueError:
    check("B16 임의 보간 정책 거부", "ValueError", "ValueError")

# ── C) 공급자·캐시 ─────────────────────────────────────────────────────────────
print("[C] 공급자·캐시 (기존 krx_fetch_guard 재사용)")


import pandas as pd  # noqa: E402  (pykrx 의존성으로 이미 설치돼 있다)


def make_frame(mapping):
    """pykrx get_index_ohlcv(...).reset_index() 와 같은 모양의 진짜 DataFrame.
    guard 가 DataFrame 타입을 검사하므로 가짜 객체를 쓰지 않는다."""
    rows = sorted(mapping.items())
    return pd.DataFrame({B.DATE_COL: [pd.Timestamp(d) for d, _ in rows],
                         B.CLOSE_COL: [v for _, v in rows]})


tmp = Path(tempfile.mkdtemp(prefix="kospi-bench-"))
cache_file = tmp / "cache.json"

calls = {"n": 0}


def fake_fetcher(s_ymd, e_ymd):
    calls["n"] += 1
    return make_frame(KOSPI)


got = B.fetch_kospi_closes("2026-06-01", "2026-06-05", fetcher=fake_fetcher, cache_path=cache_file)
check("C1 1회차 수집 성공", got, KOSPI)
check("C2 fetcher 1회 호출", calls["n"], 1)
check("C3 캐시 파일 생성", cache_file.exists(), True)

got2 = B.fetch_kospi_closes("2026-06-01", "2026-06-05", fetcher=fake_fetcher, cache_path=cache_file)
check("C4 동일 구간 재조회는 캐시 재사용", calls["n"], 1)
check("C5 캐시 결과 동일", got2, KOSPI)

got3 = B.fetch_kospi_closes("2026-06-02", "2026-06-04", fetcher=fake_fetcher, cache_path=cache_file)
check("C6 부분 구간도 캐시로 처리", calls["n"], 1)
check("C7 부분 구간 범위 정확", sorted(got3), ["2026-06-02", "2026-06-03", "2026-06-04"])

calls2 = {"n": 0}


def failing_fetcher(s, e):
    calls2["n"] += 1
    raise RuntimeError("Expecting value: line 1 column 1 (char 0)")


try:
    B.fetch_kospi_closes("2020-01-01", "2020-01-05", fetcher=failing_fetcher,
                         cache_path=tmp / "c2.json", sleep=lambda _s: None)
    check("C8 공급자 실패 → KrxDataInvalid(빈 값 둔갑 0)", "no-raise", "KrxDataInvalid")
except G.KrxDataInvalid:
    check("C8 공급자 실패 → KrxDataInvalid(빈 값 둔갑 0)", "KrxDataInvalid", "KrxDataInvalid")
check("C9 기존 guard 재시도 계약 유지(3회)", calls2["n"], G.MAX_ATTEMPTS)
check("C10 실패 시 캐시 파일 생성 0", (tmp / "c2.json").exists(), False)

# 종가가 전부 0 이면 guard 의 collapse 가드가 잡는다(정상 데이터로 둔갑 0)
try:
    B.fetch_kospi_closes("2026-06-01", "2026-06-05",
                         fetcher=lambda s, e: make_frame({d: 0.0 for d in KOSPI}),
                         cache_path=tmp / "c3.json", sleep=lambda _s: None)
    check("C11 전 종가 0 → 붕괴로 차단", "no-raise", "KrxDataInvalid")
except G.KrxDataInvalid:
    check("C11 전 종가 0 → 붕괴로 차단", "KrxDataInvalid", "KrxDataInvalid")

check("C12 KOSPI 지수코드 고정", B.BENCHMARK_CODE, "1001")
check("C13 캐시는 기존 _cache 관례 사용", "_cache" in str(B.CACHE_PATH).replace("\\", "/"), True)

# ── D) Core 실제 canonical (read-only) ─────────────────────────────────────────
print("[D] Core canonical 영향 0")
state_path = B.ROOT / "magic-formula-official-state.json"
if not state_path.exists():
    print("  SKIP  운영 canonical 없음")
else:
    raw_before = state_path.read_bytes()
    st = json.loads(raw_before.decode("utf-8"))
    fund = B.load_fund_nav_series(st)
    check("D1 NAV 시계열 추출됨", len(fund) > 0, True)
    check("D2 시작일 == officialStartDate", fund[0]["date"], st.get("officialStartDate"))
    check("D3 시작 NAV == initialCapital", fund[0]["nav"], float(st.get("initialCapital")))
    check("D4 날짜 오름차순", [f["date"] for f in fund], sorted(f["date"] for f in fund))

    # 실제 canonical + fixture 종가로 생성(네트워크 0)
    synth = {f["date"]: 2000.0 + i for i, f in enumerate(fund)}
    rd = B.build_benchmark_series(fund, synth)
    check("D5 생성 OK", rd["status"], B.STATUS_OK)
    check("D6 D0 fund 0%", close_to(rd["series"][0]["fundReturnPct"], 0.0), True)
    # 기존 공개 수익률 정의(cumulativeReturn = NAV/NAV0-1)와 일치해야 한다
    last_ev = [e for e in st["evaluationSnapshots"] if e.get("cumulativeReturn") is not None][-1]
    expect = round(rd["series"][-1]["fundReturnPct"], 2)
    check("D7 fundReturnPct == 기존 cumulativeReturn 정의",
          expect, round(float(last_ev["cumulativeReturn"]), 2))
    check("D8 canonical 파일 무변경", state_path.read_bytes(), raw_before)

# ── E) Easy-ready: 서로 다른 시작일 독립 정규화 ────────────────────────────────
print("[E] 서로 다른 시작일 독립 정규화")
rA = B.build_benchmark_series(FUND, KOSPI, base_date="2026-06-01")
rB = B.build_benchmark_series(FUND, KOSPI, base_date="2026-06-03")
check("E1 A 기준일", rA["baseDate"], "2026-06-01")
check("E2 B 기준일", rB["baseDate"], "2026-06-03")
check("E3 각자 D0 는 0%",
      (close_to(rA["series"][0]["fundReturnPct"], 0.0),
       close_to(rB["series"][0]["fundReturnPct"], 0.0)), (True, True))
check("E4 B 는 시작일 이전 데이터를 쓰지 않음", rB["series"][0]["date"], "2026-06-03")
# 같은 날짜라도 기준일이 다르면 값이 다르다(독립 정규화 증명)
a_last = [x for x in rA["series"] if x["date"] == "2026-06-05"][0]
b_last = [x for x in rB["series"] if x["date"] == "2026-06-05"][0]
check("E5 A 최종 fund 0%", close_to(a_last["fundReturnPct"], 0.0), True)
check("E6 B 최종 fund +5.263%", close_to(b_last["fundReturnPct"], (50 / 47.5 - 1) * 100), True)
check("E7 B 최종 KOSPI +11.11%", close_to(b_last["benchmarkReturnPct"], (2000 / 1800 - 1) * 100), True)
check("E8 두 펀드 excess 는 서로 독립",
      a_last["excessReturnPctPoint"] != b_last["excessReturnPctPoint"], True)

import shutil  # noqa: E402
shutil.rmtree(tmp, ignore_errors=True)

print()
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
