#!/usr/bin/env python3
"""와바바 공개 freshness observer 회귀 — 네트워크 0 (decide() 순수 판정만 검증).

WABABA-HOTG-PUBLIC-FRESHNESS-OBSERVER-HARDENING-R2

핵심은 두 방향을 동시에 지키는 것이다.
  ① stale 을 PASS 로 흘려보내지 않는다 (R1 blind spot 재발 방지)
  ② 주말·휴장일·장중·publish 전·benchmark base date·과거 MISSED_RUN 을
     stale 로 오탐하지 않는다 (false positive 방지)

실행: python scripts/test_wababa_public_freshness.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import magic_daily_common as C  # noqa: E402
import wababa_public_freshness as F  # noqa: E402

PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}")
    else:
        FAIL += 1
        print(f"[FAIL] {name}  {extra}")


def kst(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=C.KST)


def canon(latest, missed=(), ok=True, err=None):
    return {"ok": ok, "error": err, "latestTradeDate": latest,
            "missedRunDates": list(missed), "officialSequence": 38}


def pub(fund_date, ok=True, err=None, **over):
    if not ok:
        return {"ok": False, "error": err, "source": "http"}
    base = {"ok": True, "error": None, "source": "http", "httpStatus": 200,
            "fundDataDate": fund_date, "dashboardBaseDate": fund_date,
            "rankingsDate": fund_date, "tradeDaysMaxDate": fund_date,
            "benchmarkLatestDate": fund_date,
            "benchmarkBaseDate": "2026-06-17"}
    base.update(over)
    return base


def run(now, canonical, public):
    return F.decide(now, canonical, public, {"ok": True})


# ══════════════ CASE 1 — R1 이 놓쳤던 바로 그 상태 ══════════════
def case1():
    print("\n[CASE 1] canonical 08-28 / public 08-18 / 토요일 08-29")
    r = run(kst(2026, 8, 29, 18, 44),
            canon("2026-08-28", missed=("2026-08-20", "2026-08-21", "2026-08-24")),
            pub("2026-08-18"))
    ck("PASS 아님 (blind spot 재발 방지)", r["verdict"] != "PASS", r["verdict"])
    ck("BLOCKED 판정", r["verdict"] == "BLOCKED", r["verdict"])
    ck("expected = 2026-08-28", r["expectedPublicTradeDate"] == "2026-08-28")
    ck("canonical stale 0", r["canonicalStaleTradingDays"] == 0)
    ck("public stale 5거래일 (MISSED_RUN 3건 제외)",
       r["publicStaleTradingDays"] == 5, str(r["publicStaleDates"]))
    ck("publish 만 stale 로 분리 표기", r["publishOnlyStale"] is True)
    ck("예약 Ready 여부는 판정 근거 아님", "scheduler" not in str(r["checks"]).lower())


# ══════════════ CASE 2 — 정상 ══════════════
def case2():
    print("\n[CASE 2] canonical/public 모두 최신")
    r = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"), pub("2026-08-28"))
    ck("PASS", r["verdict"] == "PASS", str(r["reasons"]))
    ck("stale 0/0", r["canonicalStaleTradingDays"] == 0
       and r["publicStaleTradingDays"] == 0)


# ══════════════ CASE 3 — 월요일 deadline 전 ══════════════
def case3():
    print("\n[CASE 3] 월요일 08-31 10:00 (publish deadline 전)")
    r = run(kst(2026, 8, 31, 10, 0), canon("2026-08-28"), pub("2026-08-28"))
    ck("expected = 직전 거래일 08-28 (오늘 요구 안 함)",
       r["expectedPublicTradeDate"] == "2026-08-28")
    ck("deadline 미도래", r["publishDeadlinePassed"] is False)
    ck("오류 판정 아님", r["verdict"] in ("PASS", "WAIT"), r["verdict"])
    ck("BLOCKED 아님", r["verdict"] != "BLOCKED")


# ══════════════ CASE 4 — 월요일 deadline 후 금요일에 머묾 ══════════════
def case4():
    print("\n[CASE 4] 월요일 08-31 17:40 (deadline 후) / public 08-28 정체")
    r = run(kst(2026, 8, 31, 17, 40), canon("2026-08-31"), pub("2026-08-28"))
    ck("expected = 오늘 08-31", r["expectedPublicTradeDate"] == "2026-08-31")
    ck("deadline 지남", r["publishDeadlinePassed"] is True)
    ck("PASS 아님", r["verdict"] != "PASS", r["verdict"])
    ck("public stale 1거래일", r["publicStaleTradingDays"] == 1)
    ck("1거래일은 WARNING", r["verdict"] == "WARNING", r["verdict"])


# ══════════════ CASE 5 — 주말 ══════════════
def case5():
    print("\n[CASE 5] 주말 — 오늘 날짜를 요구해 false stale 내지 않음")
    for day, label in ((29, "토"), (30, "일")):
        r = run(kst(2026, 8, day, 9, 0), canon("2026-08-28"), pub("2026-08-28"))
        ck(f"08-{day}({label}) expected=08-28",
           r["expectedPublicTradeDate"] == "2026-08-28")
        ck(f"08-{day}({label}) PASS", r["verdict"] == "PASS", str(r["reasons"]))
        ck(f"08-{day}({label}) 오늘은 거래일 아님", r["todayIsTradingDay"] is False)


# ══════════════ CASE 6 — 공휴일 ══════════════
def case6():
    print("\n[CASE 6] 공휴일 2026-09-24 — 휴장일을 거래일로 요구하지 않음")
    ck("2026-09-24 는 휴장일", C.is_krx_trading_day("2026-09-24") is False)
    r = run(kst(2026, 9, 24, 18, 0), canon("2026-09-23"), pub("2026-09-23"))
    ck("expected = 직전 거래일 09-23",
       r["expectedPublicTradeDate"] == "2026-09-23", r["expectedPublicTradeDate"])
    ck("PASS", r["verdict"] == "PASS", str(r["reasons"]))
    # 광복절 대체(08-17) 도 같은 규칙
    ck("2026-08-17 휴장일", C.is_krx_trading_day("2026-08-17") is False)
    r2 = run(kst(2026, 8, 17, 18, 0), canon("2026-08-14"), pub("2026-08-14"))
    ck("08-17 expected=08-14", r2["expectedPublicTradeDate"] == "2026-08-14")
    ck("08-17 PASS", r2["verdict"] == "PASS", str(r2["reasons"]))


# ══════════════ CASE 7 — benchmark base vs latest ══════════════
def case7():
    print("\n[CASE 7] benchmark base date 는 과거지만 series latest 는 최신")
    r = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
            pub("2026-08-28", benchmarkBaseDate="2026-06-17",
                benchmarkLatestDate="2026-08-28"))
    ck("PASS (base date 로 stale 판정 안 함)", r["verdict"] == "PASS",
       str(r["reasons"]))
    ck("benchmark base 무시 선언", r["checks"]["benchmarkBaseDateIgnored"] is True)
    ck("표면 정렬 OK", r["checks"]["publicSurfacesAligned"] is True)
    # 반대로 benchmark latest 가 실제로 뒤처지면 잡아야 한다
    r2 = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
             pub("2026-08-28", benchmarkLatestDate="2026-08-14"))
    ck("benchmark latest 가 뒤처지면 WARNING", r2["verdict"] == "WARNING",
       r2["verdict"])


# ══════════════ CASE 8 — 과거 MISSED_RUN ══════════════
def case8():
    print("\n[CASE 8] 과거 MISSED_RUN 이 있어도 현재가 정상이면 영구 BLOCKED 아님")
    missed = ("2026-06-18", "2026-06-22", "2026-08-20", "2026-08-21", "2026-08-24")
    r = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28", missed=missed),
            pub("2026-08-28"))
    ck("PASS", r["verdict"] == "PASS", str(r["reasons"]))
    ck("MISSED_RUN 5건 기록돼 있음", len(r["missedRunDatesExcluded"]) == 5)
    # MISSED_RUN 이 gap 계산에서 실제로 제외되는지
    r2 = run(kst(2026, 8, 25, 18, 0),
             canon("2026-08-19", missed=("2026-08-20", "2026-08-21", "2026-08-24")),
             pub("2026-08-19"))
    ck("08-19 -> 08-25 사이 MISSED_RUN 3건 제외 후 stale 1거래일",
       r2["publicStaleTradingDays"] == 1, str(r2["publicStaleDates"]))


# ══════════════ fail-closed ══════════════
def case_fail_closed():
    print("\n[CASE 9] 검사 실패를 정상으로 위장하지 않는다 (fail-open 금지)")
    r = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
            pub(None, ok=False, err="URLError: timed out"))
    ck("fetch 실패 -> PASS 아님", r["verdict"] != "PASS", r["verdict"])
    ck("fetch 실패 -> 최소 WARNING",
       r["verdict"] in ("WARNING", "BLOCKED"), r["verdict"])
    ck("publicFetchable=False 기록", r["checks"]["publicFetchable"] is False)

    r2 = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
             pub(None, ok=False, err="JSONDecodeError"))
    ck("parse 실패 -> PASS 아님", r2["verdict"] != "PASS", r2["verdict"])

    r3 = run(kst(2026, 8, 28, 18, 0),
             canon(None, ok=False, err="JSONDecodeError"), pub("2026-08-28"))
    ck("canonical 판독 실패 -> PASS 아님", r3["verdict"] != "PASS", r3["verdict"])
    ck("canonicalReadable=False", r3["checks"]["canonicalReadable"] is False)

    r4 = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
             pub("2026-08-28", fundDataDate=None))
    ck("fundDataDate 누락 -> PASS 아님", r4["verdict"] != "PASS", r4["verdict"])


# ══════════════ canonical 자체가 뒤처진 경우 ══════════════
def case_canonical_behind():
    print("\n[CASE 10] canonical 장부까지 뒤처지면 더 무겁게")
    r = run(kst(2026, 8, 29, 18, 0), canon("2026-08-19"), pub("2026-08-18"))
    ck("BLOCKED", r["verdict"] == "BLOCKED", r["verdict"])
    ck("canonical stale > 0", r["canonicalStaleTradingDays"] > 0)
    ck("publishOnlyStale=False (원인 분리)", r["publishOnlyStale"] is False)


# ══════════════ 표면 불일치 ══════════════
def case_surface_mismatch():
    print("\n[CASE 11] 공개 표면끼리 기준일이 어긋나면 잡는다")
    r = run(kst(2026, 8, 28, 18, 0), canon("2026-08-28"),
            pub("2026-08-28", rankingsDate="2026-08-14"))
    ck("PASS 아님", r["verdict"] != "PASS", r["verdict"])
    ck("불일치 표면 기록", any(m["surface"] == "rankingsDate"
                          for m in r["publicSurfaceMismatch"]))


# ══════════════ 결정성 ══════════════
def case_deterministic():
    print("\n[CASE 12] 동일 입력 재실행 동일 결과")
    args = (kst(2026, 8, 29, 18, 44),
            canon("2026-08-28", missed=("2026-08-20",)), pub("2026-08-18"))
    a, b = run(*args), run(*args)
    ck("deterministic", a == b)


def case_calendar_reuse():
    print("\n[CASE 13] 새 calendar 만들지 않고 기존 helper 재사용")
    src = (Path(__file__).resolve().parent / "wababa_public_freshness.py").read_text(
        encoding="utf-8")
    ck("magic_daily_common 재사용", "import magic_daily_common as C" in src)
    ck("is_krx_trading_day 사용", "C.is_krx_trading_day" in src)
    ck("휴장표 하드코딩 안 함", "KRX_HOLIDAYS" not in src)
    ck("mtime 을 freshness 근거로 쓰지 않음",
       "st_mtime" not in src and "LastWriteTime" not in src)
    ck("deadline 은 기존 예약 17:00 기준", F.PUBLISH_DEADLINE_HOUR == 17)


def main() -> int:
    for f in (case1, case2, case3, case4, case5, case6, case7, case8,
              case_fail_closed, case_canonical_behind, case_surface_mismatch,
              case_deterministic, case_calendar_reuse):
        f()
    print(f"\n결과: {PASS} passed, {FAIL} failed (총 {PASS + FAIL})")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
