"""quarterly_ttm_poc — DART 분기보고서 기반 20종목 TTM 복원 PoC 오케스트레이터.

Phase MF-TTM-DART-20-STOCK-POC.

동작
1) scripts/poc/ttm_poc_config.json 의 20종목을 읽는다.
2) 종목별로 fs_div(연결 CFS / 별도 OFS)를 lock 하고, TTM(2026Q1 종료)에 필요한
   5개 보고서를 DART fnlttSinglAcntAll 로 조회한다.
     (2026,11013) (2025,11011) (2025,11014) (2025,11012) (2025,11013)
3) 응답을 분기 전용 캐시(_cache/dart-statements-quarterly/)에 저장한다.
   * 기존 연간 캐시(_cache/dart-statements/{corp}_{year}_{fs_div}.json)는 절대 건드리지 않는다.
4) 손익(IS/CIS) 누적 → 단일분기 누적차분 → 최근 4개 단일분기 합(TTM).
   재무상태표(BS)는 최신 분기 말 시점값(합산 금지).
5) 결과/아이투자 비교 fixture 를 PoC 전용 경로(_cache/ttm-poc-output/)에 쓴다.
   * 운영 파일(financial-universe-real.json 등)은 읽기만 하고 쓰지 않는다.

안전
- reprt_code 는 분기 3종 + 연간 1종만. 20종목만. 저속·보수적 요청.
- status 020(요청제한) 발생 시 즉시 중단, 반복 호출하지 않는다.
- 비밀키/요청헤더/인증정보는 캐시에 저장하지 않는다(응답 body 만 저장).
- 캐시 hit 시 네트워크 호출 없음(재실행 결정론).

실행
  python scripts/poc/quarterly_ttm_poc.py
환경변수(선택)
  POC_SLEEP=0.5     요청 간 대기(초, 기본 0.5)
  POC_OFFLINE=1     네트워크 호출 금지, 캐시만 사용(결정론 재검증용)
  POC_LIMIT=3       앞에서 N종목만(스모크용)
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import ttm_core as C

ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT / "_cache"
ANNUAL_CACHE_DIR = CACHE_DIR / "dart-statements"            # 기존 연간(수정 금지)
QUARTERLY_CACHE_DIR = CACHE_DIR / "dart-statements-quarterly"  # 분기 전용(신규)
CORP_CODE_CACHE_PATH = CACHE_DIR / "dart-corp-codes.json"
OUTPUT_DIR = CACHE_DIR / "ttm-poc-output"                   # PoC 결과(gitignore: _cache/)
CONFIG_PATH = Path(__file__).resolve().parent / "ttm_poc_config.json"

DART_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
SLEEP = float(os.environ.get("POC_SLEEP", "0.5"))
OFFLINE = os.environ.get("POC_OFFLINE", "0") == "1"
LIMIT = int(os.environ.get("POC_LIMIT", "0") or "0")

# TTM(2026Q1 종료)에 필요한 보고서
REQUIRED_REPORTS = [
    (2026, C.Q1_REPORT_CODE),
    (2025, C.ANNUAL_REPORT_CODE),
    (2025, C.Q3_REPORT_CODE),
    (2025, C.HALF_REPORT_CODE),
    (2025, C.Q1_REPORT_CODE),
]
# BS 스냅샷 우선순위(최신 분기 말)
BS_SNAPSHOT_ORDER = [(2026, C.Q1_REPORT_CODE), (2025, C.ANNUAL_REPORT_CODE)]


class RateLimited(Exception):
    pass


def ensure_dirs():
    QUARTERLY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_corp_code_map() -> dict:
    if not CORP_CODE_CACHE_PATH.exists():
        raise RuntimeError(f"corp code 캐시 없음: {CORP_CODE_CACHE_PATH} (build_market_snapshot 선행 필요)")
    return json.loads(CORP_CODE_CACHE_PATH.read_text(encoding="utf-8"))


def quarterly_cache_path(corp_code: str, year: int, reprt_code: str, fs_div: str) -> Path:
    return QUARTERLY_CACHE_DIR / C.quarterly_cache_filename(corp_code, year, reprt_code, fs_div)


def fetch_report(corp_code: str, year: int, reprt_code: str, fs_div: str, stats: dict) -> list[dict]:
    """분기/연간 보고서 조회(캐시 우선). 인증정보는 저장하지 않는다."""
    path = quarterly_cache_path(corp_code, year, reprt_code, fs_div)
    if path.exists():
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(cached, list):
                stats["cacheHit"] += 1
                return cached
        except Exception:
            pass

    if OFFLINE:
        stats["cacheMiss"] += 1
        return []

    if not DART_API_KEY:
        raise RuntimeError("DART_API_KEY 없음. .env.local 로드 후 실행하세요.")

    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    url = f"{DART_URL}?{urlencode(params)}"
    stats["dartCalls"] += 1
    with urlopen(url, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    status = C.safe_text(payload.get("status"))
    if status == "000":
        rows = payload.get("list", [])
        rows = rows if isinstance(rows, list) else []
        # 응답 body(list)만 저장. crtfc_key/헤더 등 요청정보는 저장하지 않음.
        path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        stats["cacheMiss"] += 1
        return rows
    if status in {"013", "014"}:  # 데이터 없음
        path.write_text("[]", encoding="utf-8")
        stats["cacheMiss"] += 1
        stats["noData"] += 1
        return []
    if status == "020":
        raise RateLimited("OpenDART status=020 요청제한 — 즉시 중단(반복 호출 안 함)")
    if status == "901":
        raise RuntimeError("OpenDART status=901 API 키 만료")
    raise RuntimeError(f"OpenDART 조회 실패 status={status} msg={C.safe_text(payload.get('message'))}")


def lock_fs_div(corp_code: str, preferred: str, stats: dict) -> tuple[str, list[dict], bool]:
    """앵커(2025 사업보고서)로 fs_div 확정. preferred 없으면 반대 fs_div fallback(혼용 아님: 이후 전 보고서 동일 적용)."""
    order = [preferred] + [d for d in ("CFS", "OFS") if d != preferred]
    for fs in order:
        rows = fetch_report(corp_code, 2025, C.ANNUAL_REPORT_CODE, fs, stats)
        if SLEEP and not OFFLINE:
            time.sleep(SLEEP)
        if rows:
            return fs, rows, (fs != preferred)
    return preferred, [], False


def collect_cumulatives(reports: dict, metric: str) -> dict:
    """metric(revenue/operatingIncome/netIncome)에 대해 (year,reprt)->누적값 dict + 추적."""
    cands = C.IS_ACCOUNTS[metric]
    cum = {}
    trace = {}
    for (year, reprt), rows in reports.items():
        info = C.cumulative_is_value(rows, cands, reprt)
        cum[(year, reprt)] = info["value"]
        trace[f"{year}_{reprt}"] = info
    return cum, trace


def latest_bs_snapshot(reports: dict, metric: str):
    cands = C.BS_ACCOUNTS[metric]
    for (year, reprt) in BS_SNAPSHOT_ORDER:
        rows = reports.get((year, reprt))
        if rows:
            info = C.snapshot_bs_value(rows, cands, reprt)
            if info["found"]:
                info["snapshotFrom"] = f"{year}_{reprt}"
                return info
    return {"value": None, "found": False, "snapshotFrom": None}


def process_stock(stock: dict, corp_map: dict, stats: dict) -> dict:
    code = stock["code"]
    corp_meta = corp_map.get(code) or {}
    corp_code = C.safe_text(corp_meta.get("corp_code"))
    warnings: list[str] = []

    if not corp_code:
        return {
            "stockCode": code, "companyName": stock["name"], "corpCode": "",
            "qualityStatus": "BLOCKED", "warningReasons": ["corp_code 매핑 실패"],
        }

    fs_div, anchor_rows, fs_fallback = lock_fs_div(corp_code, stock.get("fsDiv", "CFS"), stats)
    if fs_fallback:
        warnings.append(f"fsDiv fallback: config={stock.get('fsDiv')} → 실제={fs_div}")
    if not anchor_rows:
        return {
            "stockCode": code, "companyName": stock["name"], "corpCode": corp_code,
            "fsDiv": fs_div, "qualityStatus": "BLOCKED",
            "warningReasons": ["2025 사업보고서(11011) 조회 실패 — 앵커 없음"],
        }

    # 모든 보고서 동일 fs_div 로만 조회(연결/별도 혼용 금지)
    reports: dict = {(2025, C.ANNUAL_REPORT_CODE): anchor_rows}
    source_reports = []
    for (year, reprt) in REQUIRED_REPORTS:
        if (year, reprt) in reports:
            source_reports.append({"year": year, "reprtCode": reprt, "fsDiv": fs_div, "obtained": True})
            continue
        rows = fetch_report(corp_code, year, reprt, fs_div, stats)
        if SLEEP and not OFFLINE:
            time.sleep(SLEEP)
        reports[(year, reprt)] = rows
        source_reports.append({"year": year, "reprtCode": reprt, "fsDiv": fs_div, "obtained": bool(rows)})

    missing = [f"{y}_{r}" for (y, r), rows in reports.items() if not rows]
    if missing:
        warnings.append("보고서 누락: " + ",".join(sorted(missing)))

    # 손익 누적 → 단일분기 → TTM
    metric_out = {}
    ambiguous_hits = []
    for metric in ("revenue", "operatingIncome", "netIncome"):
        cum, trace = collect_cumulatives(reports, metric)
        for k, info in trace.items():
            if info.get("ambiguous"):
                ambiguous_hits.append(f"{metric}:{k}")
        singles = C.reconstruct_single_quarters(cum)
        ttm = C.assemble_ttm(singles)
        mono = C.check_monotonic_cumulative(cum) if metric == "revenue" else {"monotonic": True, "violations": []}
        metric_out[metric] = {
            "ttm": ttm["ttm"],
            "ttmComplete": ttm["complete"],
            "quarterValues": {q: singles[q]["value"] for q in singles},
            "formulaTrace": {q: singles[q]["formulaTrace"] for q in singles},
            "cumulative": {f"{y}_{r}": v for (y, r), v in cum.items()},
            "monotonic": mono["monotonic"],
            "monotonicViolations": mono["violations"],
        }
        if not mono["monotonic"]:
            warnings.append(f"{metric} 누적 비단조(정정공시/이상 의심): {mono['violations']}")

    if ambiguous_hits:
        warnings.append("누적필드 모호(반기/3분기 add_amount 부재→3개월값 사용): " + ",".join(ambiguous_hits))

    # --- 물리적 타당성 검증(단일분기 ≤ 연간 / YoY 급변) ---
    single_by_metric = {m: metric_out[m]["quarterValues"] for m in ("revenue", "operatingIncome", "netIncome")}
    fy_by_metric = {m: metric_out[m]["cumulative"].get(f"2025_{C.ANNUAL_REPORT_CODE}")
                    for m in ("revenue", "operatingIncome", "netIncome")}
    q1_2026_rows = reports.get((2026, C.Q1_REPORT_CODE), [])
    yoy_by_metric = {m: C.prior_year_quarter_value(q1_2026_rows, C.IS_ACCOUNTS[m])
                     for m in ("revenue", "operatingIncome", "netIncome")}
    sanity = C.sanity_flags_single_quarter(single_by_metric, fy_by_metric, yoy_by_metric)
    if sanity:
        warnings.extend("이상치: " + s for s in sanity)

    # 연간 역산 검증(revenue 기준 대표 + 각 metric 확보여부)
    rev_cum = {(y, r): metric_out["revenue"]["cumulative"].get(f"{y}_{r}")
               for (y, r) in reports.keys()}
    ann = C.validate_annual_reconstruction(rev_cum)

    # BS 최신 분기말 스냅샷
    bs = {}
    for metric in ("currentAssets", "currentLiabilities", "ppe", "cash", "totalDebt"):
        snap = latest_bs_snapshot(reports, metric)
        bs[metric] = snap["value"]
        if not snap["found"]:
            warnings.append(f"BS 결측: {metric}")

    # 필수 IS 계정 확보율 & TTM 완성도로 품질 분류
    is_ttm_ok = all(metric_out[m]["ttmComplete"] for m in ("revenue", "operatingIncome", "netIncome"))
    is_partial = any(metric_out[m]["ttm"] is not None for m in ("revenue", "operatingIncome", "netIncome"))

    hard_flags = [s for s in sanity if s.startswith("[불가]")]
    review_flags = [s for s in sanity if s.startswith("[검산]")]

    if not corp_code or not anchor_rows:
        quality = "BLOCKED"
    elif hard_flags:
        # 매출 음수/분기>연간 = 데이터 무결성 실패
        quality = "BLOCKED"
    elif is_ttm_ok and not missing and ann.get("reconstructable"):
        quality = "PASS"
    elif is_partial:
        quality = "WARNING"
    else:
        quality = "BLOCKED"

    # WARNING 승격: fallback/모호/비단조/역산불일치/YoY검산
    if quality == "PASS" and (
        fs_fallback or ambiguous_hits or review_flags
        or any("비단조" in w for w in warnings) or not ann.get("match", True)
    ):
        quality = "WARNING"
    has_sanity = bool(sanity)

    return {
        "stockCode": code,
        "corpCode": corp_code,
        "companyName": stock["name"],
        "industry": stock.get("industry"),
        "category": stock.get("category"),
        "fsDiv": fs_div,
        "fsDivFallback": fs_fallback,
        "fiscalYearEnd": "12(assumed)",
        "sourceReports": source_reports,
        "quarterValues": {
            "revenue": metric_out["revenue"]["quarterValues"],
            "operatingIncome": metric_out["operatingIncome"]["quarterValues"],
            "netIncome": metric_out["netIncome"]["quarterValues"],
        },
        "ttmRevenue": metric_out["revenue"]["ttm"],
        "ttmOperatingIncome": metric_out["operatingIncome"]["ttm"],
        "ttmNetIncome": metric_out["netIncome"]["ttm"],
        "latestCurrentAssets": bs["currentAssets"],
        "latestCurrentLiabilities": bs["currentLiabilities"],
        "latestPpe": bs["ppe"],
        "latestCash": bs["cash"],
        "latestTotalDebt": bs["totalDebt"],
        "annualReconstruction": ann,
        "anomalyFlagged": has_sanity,
        "sanityFlags": sanity,
        "formulaTrace": {
            "revenue": metric_out["revenue"]["formulaTrace"],
            "operatingIncome": metric_out["operatingIncome"]["formulaTrace"],
            "netIncome": metric_out["netIncome"]["formulaTrace"],
        },
        "cumulativeRaw": {m: metric_out[m]["cumulative"] for m in ("revenue", "operatingIncome", "netIncome")},
        "qualityStatus": quality,
        "warningReasons": warnings,
    }


def write_itooza_fixture(results: list[dict]):
    """아이투자 검산 Phase 에서 비교할 fixture(자동 크롤링/로그인 없음, 값 비교용)."""
    rows = []
    for r in results:
        rows.append({
            "stockCode": r.get("stockCode"),
            "companyName": r.get("companyName"),
            "baseQuarter": "2026Q1",
            "fsDiv": r.get("fsDiv"),
            "ttmRevenue": r.get("ttmRevenue"),
            "ttmOperatingIncome": r.get("ttmOperatingIncome"),
            "ttmNetIncome": r.get("ttmNetIncome"),
            "latestCurrentAssets": r.get("latestCurrentAssets"),
            "latestCurrentLiabilities": r.get("latestCurrentLiabilities"),
            "latestPpe": r.get("latestPpe"),
            "latestCash": r.get("latestCash"),
            "latestTotalDebt": r.get("latestTotalDebt"),
            "qualityStatus": r.get("qualityStatus"),
        })
    (OUTPUT_DIR / "itooza-compare-fixture.json").write_text(
        json.dumps({"note": "아이투자 검산 비교용. 이 값으로 공식 데이터를 덮어쓰지 않는다. 차이 탐지·원인분류 전용.",
                    "asOf": "2026Q1", "stocks": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    headers = list(rows[0].keys()) if rows else []
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join("" if row[h] is None else str(row[h]) for h in headers))
    (OUTPUT_DIR / "itooza-compare-fixture.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ensure_dirs()
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    stocks = config["stocks"]
    if LIMIT > 0:
        stocks = stocks[:LIMIT]
    corp_map = load_corp_code_map()

    stats = {"dartCalls": 0, "cacheHit": 0, "cacheMiss": 0, "noData": 0}
    results = []
    started = time.time()
    rate_limited = False

    for i, stock in enumerate(stocks, 1):
        try:
            res = process_stock(stock, corp_map, stats)
        except RateLimited as e:
            print(f"[STOP] {e}")
            rate_limited = True
            break
        results.append(res)
        print(f"[{i}/{len(stocks)}] {stock['code']} {stock['name']:8} "
              f"{res.get('fsDiv','?')} → {res['qualityStatus']} "
              f"(TTM매출={_fmt(res.get('ttmRevenue'))})")

    elapsed = time.time() - started
    counts = {"PASS": 0, "WARNING": 0, "BLOCKED": 0}
    for r in results:
        counts[r["qualityStatus"]] = counts.get(r["qualityStatus"], 0) + 1

    # 계정별 확보율(TTM 완성 기준)
    def coverage(field):
        vals = [r for r in results if r.get(field) is not None]
        return round(len(vals) / len(results), 4) if results else 0.0
    cov = {
        "ttmRevenue": coverage("ttmRevenue"),
        "ttmOperatingIncome": coverage("ttmOperatingIncome"),
        "ttmNetIncome": coverage("ttmNetIncome"),
        "latestCurrentAssets": coverage("latestCurrentAssets"),
        "latestCurrentLiabilities": coverage("latestCurrentLiabilities"),
        "latestPpe": coverage("latestPpe"),
        "latestCash": coverage("latestCash"),
        "latestTotalDebt": coverage("latestTotalDebt"),
    }
    fs_dist = {}
    for r in results:
        fs_dist[r.get("fsDiv", "?")] = fs_dist.get(r.get("fsDiv", "?"), 0) + 1

    summary = {
        "phase": config["phase"],
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "asOf": "2026-07-09",
        "stockCount": len(results),
        "rateLimited": rate_limited,
        "counts": counts,
        "accountCoverage": cov,
        "fsDivDistribution": fs_dist,
        "dart": {
            "calls": stats["dartCalls"],
            "cacheHit": stats["cacheHit"],
            "cacheMiss": stats["cacheMiss"],
            "noData": stats["noData"],
        },
        "avgSecPerStock": round(elapsed / len(results), 3) if results else None,
        "fullUniverseProjection": _project_full_universe(stats, results, elapsed),
    }

    (OUTPUT_DIR / "ttm-poc-result.json").write_text(
        json.dumps({"summary": summary, "results": results}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    if results:
        write_itooza_fixture(results)

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\nwritten: {OUTPUT_DIR / 'ttm-poc-result.json'}")
    return 0


def _project_full_universe(stats, results, elapsed):
    if not results:
        return {}
    calls_per_stock = stats["dartCalls"] / len(results) if stats["dartCalls"] else 5.0
    # 캐시 hit 이면 dartCalls 가 작으므로 최악(5~6 calls/stock)으로 보수 추정
    est_calls_per_stock = max(calls_per_stock, 5.0)
    eligible = 1316
    total_calls = int(est_calls_per_stock * eligible)
    # DART 분당 제한 통상 1000회/분 가정 → 저속(0.5s sleep)시 실효 ~120회/분
    per_min = 100
    est_minutes = total_calls / per_min
    return {
        "eligibleUniverse": eligible,
        "estCallsPerStock": round(est_calls_per_stock, 2),
        "estTotalDartCalls": total_calls,
        "assumedThrottlePerMin": per_min,
        "estMinutes": round(est_minutes, 1),
        "estHours": round(est_minutes / 60, 2),
        "note": "저속·보수적 가정(분당 100콜). 캐시 재사용 시 재실행 비용 급감. DART 일일 한도 확인 필요.",
    }


def _fmt(v):
    if v is None:
        return "None"
    try:
        return f"{v:,.0f}"
    except Exception:
        return str(v)


if __name__ == "__main__":
    raise SystemExit(main())
