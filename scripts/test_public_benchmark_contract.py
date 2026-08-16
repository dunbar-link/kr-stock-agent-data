#!/usr/bin/env python3
# test_public_benchmark_contract.py
# WABABA-PUBLIC-FUND-KOSPI-WEBSITE-R1 회귀 — public 계약에 KOSPI 벤치마크를 additive 로 붙인다.
#
# 고정하려는 것:
#   ① 기존 3키 계약 비회귀 — benchmark 미지정이면 반환이 기존과 100% 동일(publish gate 경로 불변)
#   ② additive — 신규는 4번째 키 하나뿐. 기존 필드 삭제·rename 0
#   ③ 파생 일관성 — benchmark 의 fundReturnPct 최신값 == 기존 magicOfficialSummary.cumulativeReturn
#   ④ 지수 절대값이 series 에 섞이지 않음(% 축 오염 방지)
#   ⑤ 실패/결측을 0% 로 위장하지 않음
#   ⑥ 기본 OFF — 지수 피드 확인 전 자동 공개 금지(WABABA_PUBLIC_BENCHMARK 로만 켠다)
#
# 방식: 순수 함수 + fixture. 네트워크 0, 파일 write 0.
# 사용: python scripts\test_public_benchmark_contract.py

import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_magic_official_public as M  # noqa: E402
import build_recommendation_history as H  # noqa: E402
import kospi_benchmark as KB  # noqa: E402

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


def sha(o):
    return hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


STATE = M.OFFICIAL_STATE_PATH
HAVE_STATE = Path(STATE).exists()

# ── ① 기존 3키 비회귀 ──────────────────────────────────────────────────────────
print("[1] 기존 public 3키 비회귀")
if not HAVE_STATE:
    print("  SKIP  운영 canonical 없음")
else:
    raw_before = Path(STATE).read_bytes()
    base = M.build_magic_official_public(STATE)
    check("1-1 키는 정확히 기존 3개", sorted(base.keys()), sorted(M.OFFICIAL_PUBLIC_KEYS))
    check("1-2 benchmark 키 없음", M.OFFICIAL_BENCHMARK_KEY in base, False)
    check("1-3 canonical 무변경", Path(STATE).read_bytes(), raw_before)

    st = json.loads(raw_before.decode("utf-8"))
    fund = KB.load_fund_nav_series(st)
    # fixture 지수(네트워크 0): 펀드 거래일에 임의 종가 부여
    closes = {f["date"]: 2000.0 + i * 3 for i, f in enumerate(fund)}
    bench = KB.build_benchmark_series(fund, closes)
    with_b = M.build_magic_official_public(STATE, benchmark=bench)

    check("1-4 기존 3키 값이 완전히 동일",
          sha({k: with_b[k] for k in M.OFFICIAL_PUBLIC_KEYS}),
          sha({k: base[k] for k in M.OFFICIAL_PUBLIC_KEYS}))
    check("1-5 추가된 키는 정확히 1개",
          sorted(set(with_b) - set(base)), [M.OFFICIAL_BENCHMARK_KEY])
    check("1-6 기존 summary 필드 삭제·rename 0",
          sorted(with_b["magicOfficialSummary"].keys()),
          sorted(base["magicOfficialSummary"].keys()))

# ── ② benchmark public 모델 ────────────────────────────────────────────────────
print("[2] benchmark public 모델")
FUND = [{"date": "2026-06-01", "nav": 50_000_000.0},
        {"date": "2026-06-02", "nav": 55_000_000.0},
        {"date": "2026-06-03", "nav": 52_500_000.0}]
KOSPI = {"2026-06-01": 2000.0, "2026-06-02": 2100.0, "2026-06-03": 1900.0}
b = KB.build_benchmark_series(FUND, KOSPI)
summary = {"cumulativeReturn": 5.0}
pub = M.build_benchmark_public(b, summary)

check("2-1 status OK", pub["status"], "OK")
check("2-2 schemaVersion 존재", pub["schemaVersion"], "magic-official-benchmark-public-v1")
check("2-3 benchmark 식별", (pub["benchmark"], pub["benchmarkCode"]), ("KOSPI", "1001"))
check("2-4 series 필드는 % 3종 + date 뿐(지수 절대값 없음)",
      sorted(pub["series"][0].keys()),
      ["benchmarkReturnPct", "date", "excessReturnPctPoint", "fundReturnPct"])
check("2-5 D0 = 0%", (pub["series"][0]["fundReturnPct"], pub["series"][0]["benchmarkReturnPct"]),
      (0.0, 0.0))
check("2-6 표시용 2자리 반올림", pub["series"][2]["fundReturnPct"], 5.0)
check("2-7 excess %p 계산", pub["series"][1]["excessReturnPctPoint"], 5.0)
check("2-8 latest 제공", pub["latest"]["date"], "2026-06-03")
check("2-9 결측 건수 노출", pub["missingBenchmarkDateCount"], 0)
check("2-10 baseDate 메타", pub["baseDate"], "2026-06-01")

# ③ 파생 일관성 — 펀드 수익률 정의가 두 개가 되면 즉시 실패해야 한다
try:
    M.build_benchmark_public(b, {"cumulativeReturn": 99.0})
    check("2-11 summary 와 불일치 시 MappingValidationError", "no-raise", "raise")
except M.MappingValidationError:
    check("2-11 summary 와 불일치 시 MappingValidationError", "raise", "raise")

# ⑤ 실패/결측을 0% 로 위장하지 않는다
bad = KB.build_benchmark_series(FUND, {"2020-01-02": 100.0})
pub_bad = M.build_benchmark_public(bad, summary)
check("2-12 실패는 status 로 드러남", pub_bad["status"], KB.STATUS_NO_COMMON_DATE)
check("2-13 실패 시 series 비움(0% 위장 0)", pub_bad["series"], [])
check("2-14 실패 시 latest 없음", pub_bad["latest"], None)
check("2-15 실패 사유 보존", bool(pub_bad.get("reason")), True)

gap = KB.build_benchmark_series(FUND, {"2026-06-01": 2000.0, "2026-06-03": 1900.0})
pub_gap = M.build_benchmark_public(gap, summary)
check("2-16 결측일은 series 에서 빠지고 건수로 노출",
      (len(pub_gap["series"]), pub_gap["missingBenchmarkDateCount"]), (2, 1))

# ── ③ writer 배선 · allowlist ──────────────────────────────────────────────────
print("[3] writer 배선 · allowlist")
check("3-1 allowlist 기존 3키 유지", set(M.OFFICIAL_PUBLIC_KEYS) <= H._PUBLIC_TOP_ALLOW, True)
check("3-2 allowlist 에 benchmark 키 추가", M.OFFICIAL_BENCHMARK_KEY in H._PUBLIC_TOP_ALLOW, True)

# 기본 OFF (지수 피드 확인 전 자동 공개 금지)
_env_before = os.environ.pop("WABABA_PUBLIC_BENCHMARK", None)
try:
    check("3-3 기본은 OFF", H._benchmark_enabled(), False)
    os.environ["WABABA_PUBLIC_BENCHMARK"] = "1"
    check("3-4 env=1 이면 ON", H._benchmark_enabled(), True)
    os.environ["WABABA_PUBLIC_BENCHMARK"] = "0"
    check("3-5 env=0 이면 OFF", H._benchmark_enabled(), False)
    check("3-6 명시 인자가 env 를 이긴다", H._benchmark_enabled(True), True)
finally:
    os.environ.pop("WABABA_PUBLIC_BENCHMARK", None)
    if _env_before is not None:
        os.environ["WABABA_PUBLIC_BENCHMARK"] = _env_before

if HAVE_STATE:
    paths = [H.PUBLIC_DATA_PATH, H.RECOMMENDATION_HISTORY_PATH]
    before = {str(p): (hashlib.sha256(Path(p).read_bytes()).hexdigest() if Path(p).exists() else None)
              for p in paths}
    canon_before = Path(STATE).read_bytes()

    off = H.apply_magic_official_public({"baseDate": "x"}, state_path=STATE, warn=None,
                                        include_benchmark=False)
    check("3-7 OFF 면 public 에 benchmark 키 없음", M.OFFICIAL_BENCHMARK_KEY in off, False)
    check("3-8 OFF 여도 기존 3키는 병합", all(k in off for k in M.OFFICIAL_PUBLIC_KEYS), True)

    on = H.apply_magic_official_public({"baseDate": "x"}, state_path=STATE, warn=None,
                                       include_benchmark=True)
    check("3-9 ON 이면 benchmark 키 병합", M.OFFICIAL_BENCHMARK_KEY in on, True)
    check("3-10 ON 이어도 기존 3키 유지", all(k in on for k in M.OFFICIAL_PUBLIC_KEYS), True)

    after = {str(p): (hashlib.sha256(Path(p).read_bytes()).hexdigest() if Path(p).exists() else None)
             for p in paths}
    check("3-11 public/내부 원장 파일 쓰기 0", after, before)
    check("3-12 canonical 무변경", Path(STATE).read_bytes(), canon_before)

    # fail-open: 벤치마크 계산이 죽어도 public 3키 생성은 계속돼야 한다
    _orig = H._build_official_benchmark
    try:
        H._build_official_benchmark = lambda *a, **k: None
        fo = H.apply_magic_official_public({"baseDate": "x"}, state_path=STATE, warn=None,
                                           include_benchmark=True)
        check("3-13 benchmark 실패해도 3키 생성 계속(fail-open)",
              all(k in fo for k in M.OFFICIAL_PUBLIC_KEYS), True)
        check("3-14 실패 시 benchmark 키는 붙지 않음", M.OFFICIAL_BENCHMARK_KEY in fo, False)
    finally:
        H._build_official_benchmark = _orig

# ── ④ 실제 Core 값 일치 (fixture 지수, 네트워크 0) ─────────────────────────────
print("[4] Core 최신값 일치")
if HAVE_STATE:
    st = json.loads(Path(STATE).read_bytes().decode("utf-8"))
    fund = KB.load_fund_nav_series(st)
    closes = {f["date"]: 2000.0 for f in fund}          # 지수 무변동 → excess == fund%
    bench = KB.build_benchmark_series(fund, closes)
    model = M.build_magic_official_public(STATE, benchmark=bench)
    bp = model[M.OFFICIAL_BENCHMARK_KEY]
    summ = model["magicOfficialSummary"]
    check("4-1 benchmark latest == summary cumulativeReturn",
          bp["latest"]["fundReturnPct"], summ["cumulativeReturn"])
    check("4-2 지수 무변동이면 KOSPI 0%", bp["latest"]["benchmarkReturnPct"], 0.0)
    check("4-3 그때 excess == fund%", bp["latest"]["excessReturnPctPoint"], summ["cumulativeReturn"])
    check("4-4 series 마지막 날짜 == latestTradingDate",
          bp["series"][-1]["date"], summ["latestTradingDate"])
    check("4-5 series 길이 == tradeDays 수",
          len(bp["series"]), len(model["magicOfficialTradeDays"]))

print()
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
