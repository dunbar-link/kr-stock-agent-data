#!/usr/bin/env python3
"""다중 벤치마크 회귀 — 네트워크 0 (종가는 전부 주입).

WABABA-LEGACY50D-KOSPI200-BENCHMARK-ADD-R1  (KOSPI200 추가)
WABABA-LEGACY50D-KOSDAQ-BENCHMARK-SWAP-R1   (KOSDAQ 추가 · 메인 표시 교체)

파일명은 최초 도입 과제명을 유지한다(히스토리 보존). 실제 범위는
KOSPI / KOSDAQ / KOSPI200 세 지수 전체다.

지켜야 할 두 가지:
  ① 공정 비교 — Fund·KOSPI·KOSPI200 이 같은 시작일·같은 끝일·같은 거래일 축·
     같은 정규화를 쓴다. 유리한 쪽을 고르거나 결측을 채우지 않는다.
  ② 무회귀 — 이미 공개 중인 KOSPI 숫자·필드·기준일이 한 글자도 바뀌지 않는다.

실행: python scripts/test_kospi200_benchmark.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_magic_official_public as P  # noqa: E402
import kospi_benchmark as K  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_JSON = Path("C:/work/kr-stock-agent/public/data/recommendation-history.json")
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}")
    else:
        FAIL += 1
        print(f"[FAIL] {name}  {extra}")


DATES = ["2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
FUND = [{"date": d, "nav": nav} for d, nav in
        zip(DATES, [50_000_000, 50_500_000, 51_000_000, 50_750_000, 53_419_623])]
KOSPI = dict(zip(DATES, [8864.24, 8800.0, 8700.0, 8650.0, 6788.88]))
K200 = dict(zip(DATES, [1416.97, 1400.0, 1380.0, 1370.0, 1065.70]))
KQ = dict(zip(DATES, [900.0, 895.0, 880.0, 905.0, 731.16]))
META = {"kospi": {"name": "KOSPI", "code": "1001"},
        "kosdaq": {"name": "KOSDAQ", "code": "2001"},
        "kospi200": {"name": "KOSPI200", "code": "1028"}}


def build(closes=None, base=None, fund=None):
    return K.build_multi_series(FUND if fund is None else fund,
                                closes or {"kospi": KOSPI, "kosdaq": KQ,
                                           "kospi200": K200},
                                base_date=base, meta_by_key=META)


def code_only(path):
    """주석을 걷어낸 실행 코드만. 계약을 설명한 주석이 결함으로 오탐되지 않게 한다."""
    keep = [ln for ln in path.read_text(encoding="utf-8").splitlines()
            if not ln.lstrip().startswith("#")]
    return chr(10).join(keep)


# ══════════════ 1. source / 코드 계약 ══════════════
def t_source():
    print("\n[1] source · 지수 계약")
    ck("KOSPI 코드 1001 불변", K.BENCHMARK_CODE == "1001")
    ck("KOSPI200 코드 1028", K.SECONDARY_BENCHMARK_CODE == "1028")
    ck("KOSPI200 이름", K.SECONDARY_BENCHMARK_NAME == "KOSPI200")
    ck("두 지수 캐시 파일 분리",
       K.SECONDARY_CACHE_PATH != K.CACHE_PATH
       and K.SECONDARY_CACHE_PATH.parent == K.CACHE_PATH.parent)
    src = code_only(Path(__file__).resolve().parent / "kospi_benchmark.py")
    ck("두 지수 모두 같은 get_index_ohlcv 종가(price index) 사용",
       src.count("get_index_ohlcv") == 1 and 'CLOSE_COL = "종가"' in src)
    ck("새 공급자·HTML 파싱 없음",
       not any(w in src for w in ("BeautifulSoup", "html.parser", "requests.get",
                                  "selenium", "webdriver")))
    ck("공개 key 매핑 정본",
       K.BENCHMARK_KEYS == {"1001": "kospi", "1028": "kospi200", "2001": "kosdaq"})
    ck("KOSDAQ 코드 2001 (시장 대표지수, 150 아님)",
       K.KOSDAQ_BENCHMARK_CODE == "2001" and K.KOSDAQ_BENCHMARK_NAME == "KOSDAQ")
    ck("메인 표시 = KOSPI + KOSDAQ", K.DISPLAY_BENCHMARK_KEYS == ("kospi", "kosdaq"))
    ck("KOSPI200 은 표시만 끄고 데이터는 유지",
       "kospi200" not in K.DISPLAY_BENCHMARK_KEYS
       and "kospi200" in K.BENCHMARK_KEYS.values())

    # fetcher 파라미터화 — 코드별로 다른 지수를 받는다
    calls = []

    def fake(s, e, code=K.BENCHMARK_CODE):
        calls.append(code)
        import pandas as pd
        return pd.DataFrame({"날짜": [], "종가": []})
    try:
        K.fetch_kospi_closes("2026-06-17", "2026-06-23", fetcher=fake,
                             use_cache=False, benchmark_code="1028",
                             benchmark_name="KOSPI200")
    except Exception:      # min_rows 로 guard 가 막는 건 정상
        pass
    ck("fetcher 에 지수코드가 전달됨", calls and calls[0] == "1028", str(calls))


# ══════════════ 2. 동일 축 정렬 ══════════════
def t_alignment():
    print("\n[2] 동일 기간 · 동일 축 정렬")
    m = build()
    ck("status OK", m["status"] == "OK", str(m.get("reason")))
    ck("base = 첫 공통 거래일", m["baseDate"] == "2026-06-17")
    ck("base 이동 없음", m["baseDateShifted"] is False)
    ck("alignment 선언", m["alignment"] == "INNER_JOIN_FUND_AND_ALL_BENCHMARKS")
    ck("points = fund 거래일 수", len(m["series"]) == len(DATES))
    dates = [r["date"] for r in m["series"]]
    ck("start 동일", dates[0] == DATES[0])
    ck("end 동일", dates[-1] == DATES[-1])
    ck("날짜 오름차순", dates == sorted(dates))
    ck("중복 날짜 0", len(dates) == len(set(dates)))
    ck("dropped 0", m["droppedDates"] == [])
    ck("모든 행에 세 지수 값이 다 있다",
       all(all(f"{k}ReturnPct" in r for k in ("kospi", "kosdaq", "kospi200"))
           for r in m["series"]))
    ck("한 축만 쓴다(지수 절대값 미포함)",
       all(not any(k.endswith("Close") or k == "value" for k in r) for r in m["series"]))


# ══════════════ 3. 정규화 ══════════════
def t_normalization():
    print("\n[3] 정규화 — 기존 계약 재사용")
    m = build()
    b = m["series"][0]
    ck("기준일 fund 0%", abs(b["fundReturnPct"]) < 1e-9)
    ck("기준일 KOSPI 0%", abs(b["kospiReturnPct"]) < 1e-9)
    ck("기준일 KOSPI200 0%", abs(b["kospi200ReturnPct"]) < 1e-9)
    last = m["series"][-1]
    exp_k = (KOSPI[DATES[-1]] / KOSPI[DATES[0]] - 1) * 100
    exp_2 = (K200[DATES[-1]] / K200[DATES[0]] - 1) * 100
    exp_f = (FUND[-1]["nav"] / FUND[0]["nav"] - 1) * 100
    ck("KOSPI 수식 (v/base-1)*100", abs(last["kospiReturnPct"] - exp_k) < 1e-9)
    ck("KOSPI200 동일 수식", abs(last["kospi200ReturnPct"] - exp_2) < 1e-9)
    exp_q = (KQ[DATES[-1]] / KQ[DATES[0]] - 1) * 100
    ck("KOSDAQ 동일 수식", abs(last["kosdaqReturnPct"] - exp_q) < 1e-9)
    ck("fund 수식 동일", abs(last["fundReturnPct"] - exp_f) < 1e-9)
    # 단일 벤치마크 경로와 KOSPI 값이 정확히 같아야 한다(계약 하나만 존재)
    single = K.build_benchmark_series(FUND, KOSPI, base_date=DATES[0])
    ck("단일/다중 경로의 KOSPI 값 동일",
       abs(single["series"][-1]["benchmarkReturnPct"] - last["kospiReturnPct"]) < 1e-9)
    ck("단일/다중 base 동일", single["baseDate"] == m["baseDate"])


# ══════════════ 4. 초과수익 ══════════════
def t_excess():
    print("\n[4] 초과수익 (vs KOSPI / vs KOSPI200)")
    m = build()
    last = m["series"][-1]
    by = {b["key"]: b for b in m["benchmarks"]}
    ck("벤치마크 3종", set(by) == {"kospi", "kosdaq", "kospi200"})
    ck("excess vs KOSPI = fund - kospi",
       abs(by["kospi"]["excessPctPoint"] - (last["fundReturnPct"] - last["kospiReturnPct"])) < 1e-9)
    ck("excess vs KOSPI200 = fund - kospi200",
       abs(by["kospi200"]["excessPctPoint"] - (last["fundReturnPct"] - last["kospi200ReturnPct"])) < 1e-9)
    ck("latestReturnPct == series[-1]",
       abs(by["kospi"]["latestReturnPct"] - last["kospiReturnPct"]) < 1e-9
       and abs(by["kospi200"]["latestReturnPct"] - last["kospi200ReturnPct"]) < 1e-9)
    ck("이름·코드 기록", by["kospi"]["code"] == "1001"
       and by["kospi200"]["code"] == "1028" and by["kosdaq"]["code"] == "2001")
    ck("excess vs KOSDAQ = fund - kosdaq",
       abs(by["kosdaq"]["excessPctPoint"] - (last["fundReturnPct"] - last["kosdaqReturnPct"])) < 1e-9)


# ══════════════ 5. 결측 / 중복 ══════════════
def t_missing():
    print("\n[5] 결측 · 중복 — 채우지 않는다")
    k2 = {d: v for d, v in K200.items() if d != "2026-06-19"}
    m = build(closes={"kospi": KOSPI, "kosdaq": KQ, "kospi200": k2})
    ck("한 지수 결측일은 축에서 제외", "2026-06-19" not in [r["date"] for r in m["series"]])
    ck("제외된 날짜를 droppedDates 에 기록", m["droppedDates"] == ["2026-06-19"])
    ck("결측을 지수별로 기록", m["missingByKey"]["kospi200"] == ["2026-06-19"]
       and m["missingByKey"]["kospi"] == [])
    ck("세 지수가 여전히 같은 축", len(m["series"]) == len(DATES) - 1)
    ck("carry-forward 안 함(값 재사용 없음)",
       all(r["kospi200ReturnPct"] == (k2[r["date"]] / k2[DATES[0]] - 1) * 100
           for r in m["series"]))
    src = code_only(Path(__file__).resolve().parent / "kospi_benchmark.py")
    ck("보간/ffill 구현 자체가 없음",
       not any(w in src for w in ("ffill", "fillna", "interpolate", "bfill")))
    # 중복 날짜 입력 방어
    dup = FUND + [FUND[-1]]
    m2 = build(fund=dup)
    ds = [r["date"] for r in m2["series"]]
    ck("입력 중복이 있어도 출력 중복 0", len(ds) == len(set(ds)))


# ══════════════ 6. 실패/경계 ══════════════
def t_edges():
    print("\n[6] 경계 — 0% 위장 금지")
    m = build(closes={"kospi": {}, "kosdaq": {}, "kospi200": {}})
    ck("공통일 0 → BLOCKED", m["status"] == K.STATUS_NO_COMMON_DATE, m["status"])
    ck("series 비움(0% 로 위장 안 함)", m["series"] == [])
    m2 = build(fund=[])
    ck("펀드 비면 BLOCKED", m2["status"] == K.STATUS_NO_COMMON_DATE)
    zero = dict(K200); zero[DATES[0]] = 0.0
    m3 = build(closes={"kospi": KOSPI, "kosdaq": KQ, "kospi200": zero})
    ck("기준일 종가 0 → BLOCKED", m3["status"] == K.STATUS_NO_COMMON_DATE, m3["status"])


# ══════════════ 7. public schema (additive · KOSPI 불변) ══════════════
def t_public_schema():
    print("\n[7] public schema — additive · 기존 KOSPI 불변")
    single = K.build_benchmark_series(FUND, KOSPI, base_date=DATES[0])
    summary = {"cumulativeReturn": round(single["series"][-1]["fundReturnPct"], 2)}

    before = P.build_benchmark_public(dict(single), summary)
    after = P.build_benchmark_public(dict(single, multi=build()), summary)

    ck("multi 없으면 multi 키도 없다(구 payload 하위호환)", "multi" not in before)
    ck("multi 주면 multi 키가 붙는다", "multi" in after)
    common = {k: v for k, v in after.items() if k != "multi"}
    ck("★ 기존 KOSPI 필드 완전 동일", common == before,
       str({k: (before.get(k), common.get(k)) for k in before if before.get(k) != common.get(k)}))
    ck("기존 schemaVersion 불변",
       after["schemaVersion"] == "magic-official-benchmark-public-v1")

    mp = after["multi"]
    ck("multi schemaVersion", mp["schemaVersion"] == "magic-official-benchmark-multi-public-v1")
    ck("multi status OK", mp["status"] == "OK")
    ck("multi base == 기존 base", mp["baseDate"] == after["baseDate"])
    ck("multi points == 기존 points", len(mp["series"]) == len(after["series"]))
    ck("multi latest 초과수익 키", set(["excessVsKospiPctPoint", "excessVsKospi200PctPoint"])
       <= set(mp["latest"]))
    ck("표시용 2자리 반올림",
       all(round(v, 2) == v for r in mp["series"]
           for v in (r["fundReturnPct"], r["kospiReturnPct"], r["kospi200ReturnPct"])))
    ck("multi 도 지수 절대값 미포함",
       all(not any("Close" in k for k in r) for r in mp["series"]))

    # multi 계산이 실패해도 KOSPI 는 살아남는다(fail-open)
    broken = P.build_benchmark_public(dict(single, multi={"status": "BLOCKED_X",
                                                          "reason": "boom"}), summary)
    ck("multi 실패해도 KOSPI series 정상", broken["series"] == before["series"])
    ck("multi 실패는 status 로 드러남", broken["multi"]["status"] == "BLOCKED_X")


# ══════════════ 8. 실제 공개 산출물 ══════════════
def t_production_artifact():
    print("\n[8] 실제 public 산출물 정합")
    if not PUBLIC_JSON.exists():
        ck("public 산출물 존재", False, str(PUBLIC_JSON))
        return
    d = json.loads(PUBLIC_JSON.read_text(encoding="utf-8"))
    bm = d.get("magicOfficialBenchmark") or {}
    ck("KOSPI benchmark 유지", bm.get("benchmark") == "KOSPI" and bm.get("benchmarkCode") == "1001")
    ck("base date 2026-06-17 불변", bm.get("baseDate") == "2026-06-17")
    lt = bm.get("latest") or {}
    ck("KOSPI latest date 2026-08-28", lt.get("date") == "2026-08-28")
    ck("KOSPI 누적 -23.41 불변", lt.get("benchmarkReturnPct") == -23.41, str(lt))
    ck("Fund 누적 6.84 불변", lt.get("fundReturnPct") == 6.84)
    ck("excess vs KOSPI 30.25 불변", lt.get("excessReturnPctPoint") == 30.25)

    m = bm.get("multi") or {}
    ck("multi 존재", bool(m))
    ck("multi status OK", m.get("status") == "OK")
    ck("multi base 동일", m.get("baseDate") == bm.get("baseDate"))
    ck("multi points 동일", len(m.get("series") or []) == len(bm.get("series") or []))
    ck("dropped 0", m.get("droppedDateCount") == 0)
    by = {b["key"]: b for b in (m.get("benchmarks") or [])}
    ck("KOSPI200 포함", "kospi200" in by and by["kospi200"]["code"] == "1028")
    ck("KOSPI200 누적 -24.79", by.get("kospi200", {}).get("latestReturnPct") == -24.79, str(by))
    ck("excess vs KOSPI200 31.63", by.get("kospi200", {}).get("excessPctPoint") == 31.63)
    ck("multi 안 KOSPI 값이 기존과 동일",
       by.get("kospi", {}).get("latestReturnPct") == lt.get("benchmarkReturnPct"))
    ck("KOSDAQ 포함 · 코드 2001", "kosdaq" in by and by["kosdaq"]["code"] == "2001")
    ck("KOSDAQ 누적 -18.76", by.get("kosdaq", {}).get("latestReturnPct") == -18.76, str(by.get("kosdaq")))
    ck("excess vs KOSDAQ 25.59", by.get("kosdaq", {}).get("excessPctPoint") == 25.59)
    ck("KOSDAQ 결측 0", by.get("kosdaq", {}).get("missingDateCount") == 0)
    ck("메인 표시 2종(KOSPI·KOSDAQ)", m.get("displayKeys") == ["kospi", "kosdaq"], str(m.get("displayKeys")))
    ml = m.get("latest") or {}
    ck("multi latest date 일치", ml.get("date") == lt.get("date"))
    ck("multi latest fund 일치", ml.get("fundReturnPct") == lt.get("fundReturnPct"))
    ck("summary cumulativeReturn 과 일치",
       (d.get("magicOfficialSummary") or {}).get("cumulativeReturn") == ml.get("fundReturnPct"))
    # series[-1] == latest
    s = m.get("series") or []
    ck("series[-1] == latest", bool(s) and s[-1]["date"] == ml.get("date")
       and s[-1]["kospi200ReturnPct"] == ml.get("kospi200ReturnPct"))
    ck("public 최상위 키 수 불변(31)", len(d) == 31, str(len(d)))
    ck("새 top-level 키 없음(multi 는 benchmark 하위)", "magicOfficialBenchmarkMulti" not in d)


# ══════════════ 8-B. 메인 표시 지수 교체 (KOSDAQ swap) ══════════════
def t_display_swap():
    print("\n[8-B] 메인 표시 = KOSPI + KOSDAQ · KOSPI200 은 데이터 유지 표시만 OFF")
    single = K.build_benchmark_series(FUND, KOSPI, base_date=DATES[0])
    summary = {"cumulativeReturn": round(single["series"][-1]["fundReturnPct"], 2)}
    pubm = P.build_benchmark_public(dict(single, multi=build()), summary)["multi"]

    ck("displayKeys = kospi, kosdaq", pubm["displayKeys"] == ["kospi", "kosdaq"],
       str(pubm["displayKeys"]))
    ck("benchmarkKeys 에는 3종 모두 남아 있다",
       set(pubm["benchmarkKeys"]) == {"kospi", "kosdaq", "kospi200"})
    by = {b["key"]: b for b in pubm["benchmarks"]}
    ck("KOSPI display=True", by["kospi"]["display"] is True)
    ck("KOSDAQ display=True", by["kosdaq"]["display"] is True)
    ck("KOSPI200 display=False", by["kospi200"]["display"] is False)
    ck("★ KOSPI200 값은 삭제되지 않았다(하위호환)",
       by["kospi200"]["latestReturnPct"] is not None
       and all("kospi200ReturnPct" in r for r in pubm["series"]))
    ck("latest 에 세 지수 초과수익 모두 존재",
       {"excessVsKospiPctPoint", "excessVsKosdaqPctPoint",
        "excessVsKospi200PctPoint"} <= set(pubm["latest"]))

    # 초과수익 반올림이 두 곳에서 일치해야 한다(같은 화면에 다른 숫자 금지)
    for k in ("kospi", "kosdaq", "kospi200"):
        key = f"excessVs{k[:1].upper()}{k[1:]}PctPoint"
        ck(f"{k} 초과수익 benchmarks == latest",
           by[k]["excessPctPoint"] == pubm["latest"][key],
           f'{by[k]["excessPctPoint"]} vs {pubm["latest"][key]}')


# ══════════════ 8-C. 정보 차별성 지표 ══════════════
def t_differentiation():
    print("\n[8-C] 차별성 — 결과를 미리 정하지 않고 수식만 검증")

    def corr(a, b):
        n = len(a); ma, mb = sum(a) / n, sum(b) / n
        cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
        va = sum((x - ma) ** 2 for x in a); vb = sum((y - mb) ** 2 for y in b)
        return cov / ((va * vb) ** 0.5) if va and vb else float("nan")

    # 자기 자신과의 상관은 1 — 지표 자체가 옳은지 먼저 확인한다
    xs = [0.0, 1.0, -2.0, 3.5, 2.0]
    ck("corr 자기상관 = 1", abs(corr(xs, xs) - 1.0) < 1e-9)
    ck("corr 완전역상관 = -1", abs(corr(xs, [-x for x in xs]) + 1.0) < 1e-9)

    m = build()
    lv = {k: [r[f"{k}ReturnPct"] for r in m["series"]]
          for k in ("kospi", "kosdaq", "kospi200")}
    ck("세 시계열 길이 동일",
       len(lv["kospi"]) == len(lv["kosdaq"]) == len(lv["kospi200"]))
    mad = lambda k: sum(abs(x - y) for x, y in zip(lv["kospi"], lv[k])) / len(lv[k])
    ck("평균절대차 계산 가능", mad("kosdaq") >= 0 and mad("kospi200") >= 0)
    ck("동일 지수끼리 평균절대차 0", mad("kospi") == 0)

    # 실제 production evidence 로 판정 근거를 고정한다(2026-08-30 실측)
    if PUBLIC_JSON.exists():
        d = json.loads(PUBLIC_JSON.read_text(encoding="utf-8"))
        by = {b["key"]: b for b in
              ((d.get("magicOfficialBenchmark") or {}).get("multi") or {}).get("benchmarks", [])}
        if "kosdaq" in by and "kospi200" in by and "kospi" in by:
            gap_kq = abs(by["kosdaq"]["latestReturnPct"] - by["kospi"]["latestReturnPct"])
            gap_k2 = abs(by["kospi200"]["latestReturnPct"] - by["kospi"]["latestReturnPct"])
            ck("KOSDAQ 이 KOSPI200 보다 KOSPI 와 더 벌어진다",
               gap_kq > gap_k2, f"KOSDAQ {gap_kq:.2f}%p vs KOSPI200 {gap_k2:.2f}%p")
            # 유리해서 고른 게 아니라는 사실도 고정해 둔다
            ck("교체가 펀드에 유리하지 않다(초과성과가 오히려 감소)",
               by["kosdaq"]["excessPctPoint"] < by["kospi200"]["excessPctPoint"],
               f'vs KOSDAQ {by["kosdaq"]["excessPctPoint"]} < '
               f'vs KOSPI200 {by["kospi200"]["excessPctPoint"]}')


# ══════════════ 9. 전략 불변 ══════════════
def t_protection():
    print("\n[9] LEGACY_50D 전략 불변")
    st = json.loads((ROOT / "magic-formula-official-state.json").read_text(encoding="utf-8"))
    ck("초기자본 5천만", st.get("initialCapital") == 50_000_000)
    lots = st.get("itemLots") or []
    hold = {l["plannedSellTradingDayIndex"] - l["buyTradingDayIndex"] for l in lots
            if l.get("plannedSellTradingDayIndex") is not None
            and l.get("buyTradingDayIndex") is not None}
    ck("50거래일 FIFO 불변", hold == {50}, str(sorted(hold)))
    ck("최신 장부일 2026-08-28", (st["dailyLedger"][-1]["date"]) == "2026-08-28")
    src = code_only(Path(__file__).resolve().parent / "kospi_benchmark.py")
    ck("실주문·브로커 API 호출 경로 없음",
       not any(w in src for w in ("place_order", "send_order", "order_cash",
                                  "TradeAPI", "kiwoom", "ebest", "creon")))
    ck("write 는 캐시 경로뿐", "_cache" in src and "public/data" not in src)


def main() -> int:
    for f in (t_source, t_alignment, t_normalization, t_excess, t_missing,
              t_edges, t_public_schema, t_production_artifact, t_display_swap,
              t_differentiation, t_protection):
        f()
    print(f"\n결과: {PASS} passed, {FAIL} failed (총 {PASS + FAIL})")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
