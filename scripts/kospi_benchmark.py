#!/usr/bin/env python3
"""KOSPI 벤치마크 시계열 (WABABA-KOSPI-BENCHMARK-R1).

무엇을 하나
-----------
펀드의 일별 NAV(canonical evaluationSnapshots)와 같은 날짜 축에서 KOSPI 를 비교할 수 있게
**각 펀드의 시작일을 0% 로 normalize** 한 시계열을 만든다.
KOSPI 절대 지수값은 시계열에 넣지 않는다(같은 y축에 % 와 index 를 섞지 않기 위해).

왜 이렇게
---------
- 공급자는 **기존 pykrx 재사용**. 새 공급자·유료 API 0. KOSPI = index code 1001.
- 수집 방어도 **기존 krx_fetch_guard.fetch_validated 재사용**(재시도 3회·schema 검증·redact).
  실패가 빈 값/0/과거값으로 둔갑하지 않는다(fail-closed) — guard 계약 그대로.
- 캐시는 기존 `_cache/` 관례 재사용. 같은 구간 재조회 시 네트워크를 타지 않는다.
- fundReturnPct 정의는 이미 공개 중인 `magicOfficialTradeDays[].cumulativeReturn` 과 동일하다
  (NAV(D)/NAV(D0)-1). 새 수익률 정의를 만들지 않는다.

안전
----
파일 write 는 캐시(`_cache/kospi-index/`)뿐. canonical·public 미접근. 실주문/브로커/발송 0.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

import krx_fetch_guard as G

ROOT = Path(__file__).resolve().parents[1]

BENCHMARK_NAME = "KOSPI"
BENCHMARK_CODE = "1001"                       # pykrx 지수코드: 코스피
CACHE_PATH = ROOT / "_cache" / "kospi-index" / f"{BENCHMARK_CODE}-daily-close.json"

# ── 보조 벤치마크 (WABABA-LEGACY50D-KOSPI200-BENCHMARK-ADD-R1) ─────────────────
#   KOSPI 는 그대로 두고 KOSPI200 을 **추가**한다. 교체가 아니다.
#   코드 1028 은 추측이 아니라 pykrx get_index_ticker_list() 실측으로 확인했다
#   (2026-08-30: 1001 코스피 / 1028 코스피 200).
#   두 지수 모두 같은 `get_index_ohlcv` 의 **종가(price index)** 를 쓴다 —
#   한쪽만 total-return 계열을 섞으면 공정 비교가 깨지므로 계열을 일치시킨다.
SECONDARY_BENCHMARK_NAME = "KOSPI200"
SECONDARY_BENCHMARK_CODE = "1028"
SECONDARY_CACHE_PATH = ROOT / "_cache" / "kospi-index" / f"{SECONDARY_BENCHMARK_CODE}-daily-close.json"

# ── KOSDAQ (WABABA-LEGACY50D-KOSDAQ-BENCHMARK-SWAP-R1) ────────────────────────
#   코드 2001 은 추측이 아니라 pykrx get_index_ticker_list(market="KOSDAQ") 실측이다
#   (2026-08-30: 2001 코스닥 = 시장 대표지수 / 2203 코스닥 150 은 별개).
#   KOSPI(1001) 와 같은 위상의 **시장 대표지수**를 골랐다.
KOSDAQ_BENCHMARK_NAME = "KOSDAQ"
KOSDAQ_BENCHMARK_CODE = "2001"

# 공개 표시용 key (public schema·UI 가 함께 쓰는 정본 이름)
BENCHMARK_KEYS = {BENCHMARK_CODE: "kospi", SECONDARY_BENCHMARK_CODE: "kospi200",
                  KOSDAQ_BENCHMARK_CODE: "kosdaq"}

# ── 메인 그래프에 그릴 지수 (WABABA-LEGACY50D-KOSDAQ-BENCHMARK-SWAP-R1) ────────
#   2026-08-30 실측: 같은 38점 축에서 KOSPI 대비
#     KOSPI200  일간수익률 상관 0.9984 · 평균 |차| 1.02%p · 최대 2.06%p
#     KOSDAQ    일간수익률 상관 0.7094 · 평균 |차| 5.16%p · 최대 14.71%p
#   KOSPI200 은 사실상 KOSPI 와 같은 선이라 그래프에서 정보를 더하지 못했다.
#   그래서 **메인 표시는 KOSPI + KOSDAQ**, KOSPI200 은 데이터로만 남긴다
#   (하위호환 유지 — series·benchmarks 에서 삭제하지 않는다).
#   주의: 이 선택은 펀드에 유리해서가 아니다. 초과성과는 오히려 줄어든다
#   (vs KOSPI200 +31.63%p → vs KOSDAQ +25.59%p). 정보량 기준으로 골랐다.
DISPLAY_BENCHMARK_KEYS = ("kospi", "kosdaq")

CLOSE_COL = "종가"
DATE_COL = "날짜"

MISSING_OMIT = "OMIT"                         # 결측 거래일은 시계열에서 제외(기본)
MISSING_BLOCK = "BLOCK"                       # 결측이 하나라도 있으면 실패로 본다

STATUS_OK = "OK"
STATUS_NO_COMMON_DATE = "BLOCKED_NO_COMMON_TRADING_DAY"
STATUS_MISSING_BLOCKED = "BLOCKED_BENCHMARK_MISSING"


# ── 수집(기존 guard 재사용) ─────────────────────────────────────────────────────

def _pykrx_index_fetcher(start_ymd: str, end_ymd: str, code: str = BENCHMARK_CODE):
    """기본 fetcher. lazy import 로 모듈 import 시 네트워크/의존성을 타지 않는다.

    KOSPI(1001)·KOSPI200(1028) 둘 다 **같은 함수·같은 종가 컬럼**을 쓴다.
    새 공급자를 붙이지 않는다(§데이터 소스 원칙 1순위 — 기존 사용 소스 재사용).
    """
    from pykrx import stock  # noqa: PLC0415
    return stock.get_index_ohlcv(start_ymd, end_ymd, code).reset_index()


def _spec(name: str = BENCHMARK_NAME) -> G.FetchSpec:
    return G.FetchSpec(
        kind="index_ohlcv", market=name,
        required_columns=(DATE_COL, CLOSE_COL),
        min_rows=1,
        numeric_columns=(CLOSE_COL,),
        collapse_guard_columns=(CLOSE_COL,),   # 종가가 전부 0/NaN 이면 붕괴로 본다
        required=True,
    )


def _frame_to_closes(frame) -> dict:
    """DataFrame → {ISO date: close}. pykrx 날짜는 Timestamp."""
    out = {}
    for _, row in frame.iterrows():
        d = row[DATE_COL]
        iso = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        close = float(row[CLOSE_COL])
        if close > 0:
            out[iso] = close
    return out


def read_cache(cache_path: Optional[Path] = None) -> dict:
    p = Path(cache_path or CACHE_PATH)
    if not p.exists():
        return {"benchmarkCode": BENCHMARK_CODE, "closes": {}, "coveredRanges": []}
    try:
        c = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # 손상 캐시를 조용히 빈 값으로 덮어쓰지 않는다 — 빈 캐시로 취급하되 기록은 남기지 않는다.
        return {"benchmarkCode": BENCHMARK_CODE, "closes": {}, "coveredRanges": []}
    c.setdefault("closes", {})
    c.setdefault("coveredRanges", [])
    return c


def write_cache(cache: dict, cache_path: Optional[Path] = None) -> Path:
    p = Path(cache_path or CACHE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return p


def _covered(cache: dict, start_iso: str, end_iso: str) -> bool:
    return any(r[0] <= start_iso and end_iso <= r[1] for r in cache.get("coveredRanges", []) if len(r) == 2)


def _merge_range(ranges: list, start_iso: str, end_iso: str) -> list:
    out = sorted([list(r) for r in ranges] + [[start_iso, end_iso]])
    merged = [out[0]]
    for s, e in out[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


def fetch_kospi_closes(start_iso: str, end_iso: str, *,
                       fetcher: Optional[Callable] = None,
                       cache_path: Optional[Path] = None,
                       use_cache: bool = True,
                       sleep: Optional[Callable] = None,
                       benchmark_code: str = BENCHMARK_CODE,
                       benchmark_name: str = BENCHMARK_NAME) -> dict:
    """[start, end] 지수 종가 {ISO: close}. 캐시가 구간을 덮으면 네트워크 0.

    기본값은 KOSPI 라 기존 호출부 동작이 100% 그대로다. benchmark_code 를 주면
    같은 경로로 보조 지수(KOSPI200 등)를 받는다 — 지수별 캐시 파일이 분리된다.

    실패 시 krx_fetch_guard 계약대로 KrxDataInvalid 를 올린다(빈 dict 반환 금지).
    """
    if cache_path is None:
        cache_path = (CACHE_PATH if benchmark_code == BENCHMARK_CODE
                      else CACHE_PATH.parent / f"{benchmark_code}-daily-close.json")
    cache = read_cache(cache_path) if use_cache else {"closes": {}, "coveredRanges": []}
    if use_cache and _covered(cache, start_iso, end_iso):
        return {d: c for d, c in cache["closes"].items() if start_iso <= d <= end_iso}

    fn = fetcher or _pykrx_index_fetcher
    s_ymd, e_ymd = start_iso.replace("-", ""), end_iso.replace("-", "")
    kw = {"sleep": sleep} if sleep is not None else {}
    # fetcher 가 code 를 받으면 넘기고, 기존 2-인자 테스트 fetcher 면 그대로 호출한다.
    def _call():
        try:
            return fn(s_ymd, e_ymd, benchmark_code)
        except TypeError:
            return fn(s_ymd, e_ymd)
    res = G.fetch_validated(_call, _spec(benchmark_name), **kw)
    closes = _frame_to_closes(res.frame)

    if use_cache:
        cache["benchmarkCode"] = benchmark_code
        cache["closes"].update(closes)
        cache["coveredRanges"] = _merge_range(cache.get("coveredRanges", []), start_iso, end_iso)
        write_cache(cache, cache_path)
    return {d: c for d, c in closes.items() if start_iso <= d <= end_iso}


# ── 펀드 NAV 시계열 ─────────────────────────────────────────────────────────────

def load_fund_nav_series(state: dict) -> list:
    """canonical → [{date, nav}] 오름차순. evaluationSnapshots 를 그대로 쓴다(새 정의 0)."""
    rows = []
    for ev in (state.get("evaluationSnapshots") or []):
        d = str(ev.get("date") or "")[:10]
        nav = ev.get("totalAsset")
        if d and nav is not None:
            rows.append({"date": d, "nav": float(nav)})
    rows.sort(key=lambda r: r["date"])
    # 같은 날짜가 두 번 들어있으면 마지막 값을 쓴다(append-only 재실행 방어).
    dedup = {}
    for r in rows:
        dedup[r["date"]] = r
    return [dedup[d] for d in sorted(dedup)]


# ── 정규화(순수 함수) ───────────────────────────────────────────────────────────

def build_benchmark_series(fund_series: list, benchmark_closes: dict, *,
                           base_date: Optional[str] = None,
                           missing_policy: str = MISSING_OMIT,
                           benchmark: str = BENCHMARK_NAME,
                           benchmark_code: str = BENCHMARK_CODE) -> dict:
    """펀드 NAV + 벤치마크 종가 → D0=0% 정규화 비교 시계열. 순수 함수(네트워크·파일 0).

    - 축은 **펀드 거래일**이다. 벤치마크에만 있는 날짜는 쓰지 않는다.
    - 결측(펀드엔 있는데 벤치마크에 없는 날): OMIT(기본) = 그 날짜를 시계열에서 빼고 목록에 남긴다.
      carry-forward·보간·미래값 사용 **전부 금지**(정책적으로 구현하지 않는다).
    - D0 에 벤치마크가 없으면 **첫 공통 거래일로 baseline 을 이동**하고 그 사실을 기록한다
      (조용히 옮기지 않는다). 공통 거래일이 하나도 없으면 BLOCKED.
    - 반올림하지 않는다(원정밀도 유지). 반올림은 표시 계층 책임.
    """
    if missing_policy not in (MISSING_OMIT, MISSING_BLOCK):
        raise ValueError(f"unknown missing_policy: {missing_policy!r}")

    fund = [{"date": str(r["date"])[:10], "nav": float(r["nav"])} for r in (fund_series or [])]
    fund.sort(key=lambda r: r["date"])
    requested = str(base_date)[:10] if base_date else (fund[0]["date"] if fund else None)

    base = {"benchmark": benchmark, "benchmarkCode": benchmark_code,
            "requestedBaseDate": requested, "baseDate": None, "baseDateShifted": False,
            "fundBaseNav": None, "benchmarkBaseClose": None,
            "missingPolicy": missing_policy, "missingBenchmarkDates": [],
            "series": []}

    if not fund or requested is None:
        return dict(base, status=STATUS_NO_COMMON_DATE,
                    reason="펀드 NAV 시계열이 비어 있음")

    window = [r for r in fund if r["date"] >= requested]
    missing = [r["date"] for r in window if r["date"] not in benchmark_closes]

    # D0 결정 — 요청 시작일에 벤치마크가 없으면 첫 공통 거래일로 이동(사실을 기록).
    d0 = next((r["date"] for r in window if r["date"] in benchmark_closes), None)
    if d0 is None:
        return dict(base, status=STATUS_NO_COMMON_DATE, missingBenchmarkDates=missing,
                    reason=f"{requested} 이후 펀드·벤치마크 공통 거래일 0")

    if missing_policy == MISSING_BLOCK and missing:
        return dict(base, status=STATUS_MISSING_BLOCKED, missingBenchmarkDates=missing,
                    reason=f"벤치마크 결측 거래일 {len(missing)}건 (policy=BLOCK)")

    fund_nav0 = next(r["nav"] for r in window if r["date"] == d0)
    bench0 = float(benchmark_closes[d0])
    if fund_nav0 == 0 or bench0 == 0:
        return dict(base, status=STATUS_NO_COMMON_DATE, missingBenchmarkDates=missing,
                    reason="기준일 NAV 또는 벤치마크 종가가 0")

    series = []
    for r in window:
        if r["date"] < d0:
            continue
        close = benchmark_closes.get(r["date"])
        if close is None:              # OMIT — 미래값·직전값으로 채우지 않는다
            continue
        fund_pct = (r["nav"] / fund_nav0 - 1.0) * 100.0
        bench_pct = (float(close) / bench0 - 1.0) * 100.0
        series.append({"date": r["date"],
                       "fundReturnPct": fund_pct,
                       "benchmarkReturnPct": bench_pct,
                       "excessReturnPctPoint": fund_pct - bench_pct})

    return dict(base, status=STATUS_OK, baseDate=d0, baseDateShifted=(d0 != requested),
                fundBaseNav=fund_nav0, benchmarkBaseClose=bench0,
                missingBenchmarkDates=[d for d in missing if d >= d0], series=series)


def build_fund_benchmark(state: dict, *, base_date: Optional[str] = None,
                         fetcher: Optional[Callable] = None,
                         cache_path: Optional[Path] = None,
                         use_cache: bool = True,
                         missing_policy: str = MISSING_OMIT) -> dict:
    """canonical state → 벤치마크 비교 시계열. 수집 구간은 펀드 기간으로 한정한다."""
    fund = load_fund_nav_series(state)
    if not fund:
        return build_benchmark_series([], {}, base_date=base_date, missing_policy=missing_policy)
    start = str(base_date)[:10] if base_date else fund[0]["date"]
    closes = fetch_kospi_closes(start, fund[-1]["date"], fetcher=fetcher,
                                cache_path=cache_path, use_cache=use_cache)
    return build_benchmark_series(fund, closes, base_date=start, missing_policy=missing_policy)


# ── 다중 벤치마크 정렬 (WABABA-LEGACY50D-KOSPI200-BENCHMARK-ADD-R1) ────────────

def build_multi_series(fund_series: list, closes_by_key: dict, *,
                       base_date: Optional[str] = None,
                       meta_by_key: Optional[dict] = None) -> dict:
    """펀드 + 여러 벤치마크를 **하나의 동일한 날짜 축**으로 정렬한다. 순수 함수.

    공정 비교의 전부가 여기에 달려 있다. 그래서:
      - 축 = 펀드 거래일 ∩ **모든** 벤치마크 보유일 (inner join). 어느 한 지수에만
        있는 날짜로 다른 지수를 유리하게 만들지 않는다.
      - 기준일 = 그 교집합의 **첫 날** 하나. 지수마다 다른 base 를 쓰지 않는다.
      - 정규화는 기존 계약 그대로 (value/base - 1) * 100. 새 수식 0.
      - 결측은 채우지 않는다. carry-forward·보간·미래값 전부 금지 —
        빠진 날짜는 droppedDates 에 사실대로 남긴다.
    """
    meta_by_key = meta_by_key or {}
    rows = [{"date": str(r["date"])[:10], "nav": float(r["nav"])}
            for r in (fund_series or [])]
    rows.sort(key=lambda r: r["date"])
    # 같은 날짜가 두 번 들어오면 마지막 값만 쓴다 — 축에 중복 점이 생기면 그래프와
    # 거래일 수가 함께 어긋난다(load_fund_nav_series 와 동일 규칙).
    dedup = {}
    for r in rows:
        dedup[r["date"]] = r
    fund = [dedup[d] for d in sorted(dedup)]
    requested = str(base_date)[:10] if base_date else (fund[0]["date"] if fund else None)

    keys = list(closes_by_key)
    out = {"schemaVersion": "magic-official-benchmark-multi-v1",
           "requestedBaseDate": requested, "baseDate": None, "baseDateShifted": False,
           "alignment": "INNER_JOIN_FUND_AND_ALL_BENCHMARKS",
           "benchmarkKeys": keys, "droppedDates": [], "series": [],
           "missingByKey": {k: [] for k in keys}}

    if not fund or requested is None or not keys:
        return dict(out, status=STATUS_NO_COMMON_DATE,
                    reason="펀드 NAV 시계열 또는 벤치마크가 비어 있음")

    window = [r for r in fund if r["date"] >= requested]
    for k in keys:
        out["missingByKey"][k] = [r["date"] for r in window
                                  if r["date"] not in closes_by_key[k]]

    common = [r for r in window if all(r["date"] in closes_by_key[k] for k in keys)]
    out["droppedDates"] = [r["date"] for r in window
                           if r["date"] not in {c["date"] for c in common}]
    if not common:
        return dict(out, status=STATUS_NO_COMMON_DATE,
                    reason=f"{requested} 이후 펀드·전체 벤치마크 공통 거래일 0")

    d0 = common[0]["date"]
    nav0 = common[0]["nav"]
    base_closes = {k: float(closes_by_key[k][d0]) for k in keys}
    if nav0 == 0 or any(v == 0 for v in base_closes.values()):
        return dict(out, status=STATUS_NO_COMMON_DATE,
                    reason="기준일 NAV 또는 지수 종가가 0")

    series = []
    for r in common:
        row = {"date": r["date"],
               "fundReturnPct": (r["nav"] / nav0 - 1.0) * 100.0}
        for k in keys:
            row[f"{k}ReturnPct"] = (float(closes_by_key[k][r["date"]])
                                    / base_closes[k] - 1.0) * 100.0
        series.append(row)

    last = series[-1]
    benchmarks = []
    for k in keys:
        m = meta_by_key.get(k) or {}
        benchmarks.append({
            "key": k, "name": m.get("name"), "code": m.get("code"),
            "latestReturnPct": last[f"{k}ReturnPct"],
            "excessPctPoint": last["fundReturnPct"] - last[f"{k}ReturnPct"],
            "baseClose": base_closes[k],
            "missingDateCount": len(out["missingByKey"][k]),
            # 메인 그래프 표시 여부. 값은 계속 내려보내되 표시만 끌 수 있다.
            "display": k in DISPLAY_BENCHMARK_KEYS,
        })

    return dict(out, status=STATUS_OK, baseDate=d0,
                baseDateShifted=(d0 != requested), fundBaseNav=nav0,
                benchmarks=benchmarks, series=series,
                displayKeys=[k for k in keys if k in DISPLAY_BENCHMARK_KEYS])


def build_fund_multi_benchmark(state: dict, *, base_date: Optional[str] = None,
                               fetcher: Optional[Callable] = None,
                               use_cache: bool = True,
                               closes_by_key: Optional[dict] = None) -> dict:
    """canonical state → Fund vs KOSPI vs KOSDAQ (+KOSPI200 데이터) 동일축 시계열.

    세 지수를 모두 **같은 축**으로 계산하되, 메인 그래프에 그릴 대상은
    DISPLAY_BENCHMARK_KEYS 로만 표시한다. KOSPI200 은 계산·저장은 계속하고
    표시만 끈다 — 기존 consumer 가 값을 잃지 않게 하기 위해서다.
    """
    specs = [
        {"key": "kospi", "name": BENCHMARK_NAME, "code": BENCHMARK_CODE},
        {"key": "kosdaq", "name": KOSDAQ_BENCHMARK_NAME,
         "code": KOSDAQ_BENCHMARK_CODE},
        {"key": "kospi200", "name": SECONDARY_BENCHMARK_NAME,
         "code": SECONDARY_BENCHMARK_CODE},
    ]
    fund = load_fund_nav_series(state)
    if not fund:
        return build_multi_series([], {}, base_date=base_date)
    start = str(base_date)[:10] if base_date else fund[0]["date"]
    end = fund[-1]["date"]

    if closes_by_key is None:
        closes_by_key = {}
        for sp in specs:
            closes_by_key[sp["key"]] = fetch_kospi_closes(
                start, end, fetcher=fetcher, use_cache=use_cache,
                benchmark_code=sp["code"], benchmark_name=sp["name"])
    meta = {sp["key"]: {"name": sp["name"], "code": sp["code"]} for sp in specs}
    return build_multi_series(fund, closes_by_key, base_date=start, meta_by_key=meta)


# ── CLI (read-only preview; canonical·public write 0) ───────────────────────────

def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(
        description="KOSPI 벤치마크 비교 시계열 미리보기 (canonical/public write 0)")
    ap.add_argument("--state-path", default=str(ROOT / "magic-formula-official-state.json"))
    ap.add_argument("--base-date", default=None, help="펀드 시작일 override(YYYY-MM-DD)")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--points", type=int, default=3, help="요약 출력 점 개수(처음/중간/마지막)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    state = json.loads(Path(args.state_path).read_text(encoding="utf-8"))
    out = build_fund_benchmark(state, base_date=args.base_date, use_cache=not args.no_cache)

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("status") == STATUS_OK else 2

    print(f"[{out['benchmark']} {out['benchmarkCode']}] status={out['status']}")
    if out.get("status") != STATUS_OK:
        print("  reason:", out.get("reason"))
        return 2
    s = out["series"]
    print(f"  기준일 {out['baseDate']} (요청 {out['requestedBaseDate']}, 이동 {out['baseDateShifted']})"
          f" · 구간 {s[0]['date']}~{s[-1]['date']} · {len(s)}점"
          f" · 결측 {len(out['missingBenchmarkDates'])}일")
    picks = [0, len(s) // 2, len(s) - 1][:max(1, args.points)]
    print("  date         fund%     KOSPI%    excess%p")
    for i in sorted(set(picks)):
        r = s[i]
        print(f"  {r['date']}  {r['fundReturnPct']:+8.2f}  {r['benchmarkReturnPct']:+8.2f}  "
              f"{r['excessReturnPctPoint']:+8.2f}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
