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

CLOSE_COL = "종가"
DATE_COL = "날짜"

MISSING_OMIT = "OMIT"                         # 결측 거래일은 시계열에서 제외(기본)
MISSING_BLOCK = "BLOCK"                       # 결측이 하나라도 있으면 실패로 본다

STATUS_OK = "OK"
STATUS_NO_COMMON_DATE = "BLOCKED_NO_COMMON_TRADING_DAY"
STATUS_MISSING_BLOCKED = "BLOCKED_BENCHMARK_MISSING"


# ── 수집(기존 guard 재사용) ─────────────────────────────────────────────────────

def _pykrx_index_fetcher(start_ymd: str, end_ymd: str):
    """기본 fetcher. lazy import 로 모듈 import 시 네트워크/의존성을 타지 않는다."""
    from pykrx import stock  # noqa: PLC0415
    return stock.get_index_ohlcv(start_ymd, end_ymd, BENCHMARK_CODE).reset_index()


def _spec() -> G.FetchSpec:
    return G.FetchSpec(
        kind="index_ohlcv", market=BENCHMARK_NAME,
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
                       sleep: Optional[Callable] = None) -> dict:
    """[start, end] KOSPI 종가 {ISO: close}. 캐시가 구간을 덮으면 네트워크 0.

    실패 시 krx_fetch_guard 계약대로 KrxDataInvalid 를 올린다(빈 dict 반환 금지).
    """
    cache = read_cache(cache_path) if use_cache else {"closes": {}, "coveredRanges": []}
    if use_cache and _covered(cache, start_iso, end_iso):
        return {d: c for d, c in cache["closes"].items() if start_iso <= d <= end_iso}

    fn = fetcher or _pykrx_index_fetcher
    s_ymd, e_ymd = start_iso.replace("-", ""), end_iso.replace("-", "")
    kw = {"sleep": sleep} if sleep is not None else {}
    res = G.fetch_validated(lambda: fn(s_ymd, e_ymd), _spec(), **kw)
    closes = _frame_to_closes(res.frame)

    if use_cache:
        cache["benchmarkCode"] = BENCHMARK_CODE
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
