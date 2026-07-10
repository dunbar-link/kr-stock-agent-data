#!/usr/bin/env python3
"""와바바 마법공식 — 격리된 TEMP 신호 패키지 생성기 (Phase 45-E5).

목적
----
지정한 signalAsOfDate(종가 기준)의 universe·마법공식 ranking을 *저장소 밖 TEMP 경로*에만 고정한다.
- production 파일(financial-universe-real.json, magic-formula-*.json, recommendation-history.json,
  REPO1 public)은 *읽기 전용*. 쓰기 0.
- 마법공식 산식/수익성·가치 순위는 직접 구현하지 않고 build_magic_formula_fund를 호출한다(복제 0).
- universe는 build_market_snapshot_fast.build_payload를 호출하되 DART 네트워크 갱신을 0으로 강제한다
  (MAX_DART_REFRESH_PER_RUN=0). 재무 캐시는 read-only.
- 신호일 ≠ 체결일. 이번 단계는 신호 패키지만 만든다. executionPrice·BUY·batch·officialStartDate 없음.
- 다음 KRX 거래일을 executionDate 후보로 계산하되 시가는 확인하지 않는다(executionPriceAvailable=false).

CLI
---
  --signal-date YYYY-MM-DD
  --output-dir "절대경로"            (생략 시 %TEMP%\\wababa-magic-signal\\<signal>\\)
  --market-close-at "tz-aware ISO"   (호출자 주입; 종가 확정 검증용)
  --now "tz-aware ISO"               (테스트·재현용)
  --prepare-only                     (검증·미리계산만; 파일 생성 0)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ----- 경로 -----
ROOT = Path(__file__).resolve().parents[1]                 # REPO2: kr-stock-agent-data-new
REPO1_ROOT = Path("C:/work/kr-stock-agent")                # REPO1 (public)
FINANCIAL_UNIVERSE_PATH = ROOT / "financial-universe-real.json"
DART_CORP_CODES_PATH = ROOT / "_cache" / "dart-corp-codes.json"
DART_CACHE_DIR = ROOT / "_cache" / "dart-statements"
BLOCKED_OUTPUT_ROOTS = (ROOT, REPO1_ROOT)

_KST = timezone(timedelta(hours=9))

SCHEMA_VERSION = "signal-package-v1"
GENERATED_BY = "build_magic_signal_package.py"

# ----- packageStatus -----
READY = "READY_FOR_EXECUTION_OPEN"
ALREADY_PREPARED = "ALREADY_PREPARED"
BLOCKED_SIGNAL_MARKET_NOT_CLOSED = "BLOCKED_SIGNAL_MARKET_NOT_CLOSED"
BLOCKED_SIGNAL_UNIVERSE_NOT_READY = "BLOCKED_SIGNAL_UNIVERSE_NOT_READY"
BLOCKED_SIGNAL_DATE_MISMATCH = "BLOCKED_SIGNAL_DATE_MISMATCH"
BLOCKED_MISSING_RANKING = "BLOCKED_MISSING_RANKING"
BLOCKED_UNSAFE_OUTPUT_PATH = "BLOCKED_UNSAFE_OUTPUT_PATH"
BLOCKED_GENERATION_ERROR = "BLOCKED_GENERATION_ERROR"
BLOCKED_PACKAGE_CONFLICT = "BLOCKED_PACKAGE_CONFLICT"
BLOCKED_NEXT_EXECUTION_DATE_UNAVAILABLE = "BLOCKED_NEXT_EXECUTION_DATE_UNAVAILABLE"

# 검증 임계값(테스트에서 주입 가능)
DEFAULT_MIN_VALID_PRICE_FRACTION = 0.5
DEFAULT_TOP10_N = 10
DEFAULT_TOP_AUDIT_N = 100
DEFAULT_GLOBAL_TOP_N = 100                 # 전체 유효 universe 기준 독립 순위 top100(Case B 해소)
GLOBAL_RANKING_SCHEMA_VERSION = "global-rankings-v1"

# KRX 휴장일(주말 외). 다음 *체결일*은 미래라 pykrx(OHLCV 기반 get_business_days/OHLCV)로는 알 수 없다
# — 미래 날짜는 시세가 없어 빈 결과/예외가 난다(45-E5.2 실증). 따라서 주말 + 아래 휴장표로 전진 계산한다.
# ※ KRX 공시 기준 *매년 갱신 필요*. 현재 2026 전체 + 2027 신정(연말 롤오버용)까지 정비됨.
KRX_HOLIDAYS = frozenset({
    "2026-01-01",                                  # 신정
    "2026-02-16", "2026-02-17", "2026-02-18",      # 설날 연휴
    "2026-03-01", "2026-03-02",                    # 삼일절(일)→대체(월)
    "2026-05-01",                                  # 근로자의날(KRX 휴장)
    "2026-05-05",                                  # 어린이날
    "2026-05-24", "2026-05-25",                    # 부처님오신날(일)→대체(월)
    "2026-06-06",                                  # 현충일(토)
    "2026-08-15", "2026-08-17",                    # 광복절(토)→대체(월)
    "2026-09-24", "2026-09-25", "2026-09-26", "2026-09-28",  # 추석 연휴(토)→대체(월)
    "2026-10-03", "2026-10-05",                    # 개천절(토)→대체(월)
    "2026-10-09",                                  # 한글날
    "2026-12-25",                                  # 성탄절
    "2026-12-31",                                  # KRX 폐장일
    "2027-01-01",                                  # 신정(연말 롤오버 경계)
})
KRX_HOLIDAY_CALENDAR_THROUGH = "2026-12-31"        # 이 날짜 이후는 휴장표 미정비(경고만)
MAX_FORWARD_LOOKAHEAD_DAYS = 15

# top10/top100 직렬화 필드(산식이 실제 사용/생성한 감사 필드만; buyOpenPrice 등 체결가는 절대 미포함)
TOP_FIELDS = ("rank", "combinedRank", "profitabilityRank", "valueRank", "returnOnCapital",
              "earningsYield", "code", "name", "marketCap", "EBIT", "enterpriseValue",
              "capitalBase", "cashAndCashEquivalents", "totalLiabilities", "currentAssets",
              "currentLiabilities", "propertyPlantAndEquipment", "evMethod", "dataSource")


# ===== 유틸 =====

def _canonical_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p) -> Optional[str]:
    try:
        with open(p, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except (FileNotFoundError, OSError):
        return None


def _mtime(p) -> Optional[float]:
    try:
        return os.path.getmtime(p)
    except OSError:
        return None


def _parse_aware_dt(v) -> Optional[datetime]:
    """tz-aware datetime만 허용(str 또는 datetime). naive/parse 실패 → None. 문자열 비교 안 함."""
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (ValueError, OSError):
        return False


def _is_unsafe_output(path) -> bool:
    rp = Path(path).resolve()
    for root in BLOCKED_OUTPUT_ROOTS:
        rr = root.resolve()
        if rp == rr or _is_within(rp, rr):
            return True
    return False


def _default_output_dir(signal_date: str) -> Path:
    return Path(tempfile.gettempdir()) / "wababa-magic-signal" / signal_date


# ===== KRX 거래일 (전진 계산; 단순 +1일/월~금 추정 금지, 휴장일 제외) =====

def next_krx_trading_day(signal_date: str, calendar=None, *, holidays=None,
                         max_lookahead_days: int = MAX_FORWARD_LOOKAHEAD_DAYS) -> Optional[str]:
    """signalAsOfDate *다음 첫* KRX 거래일.
    - calendar(신뢰 거래일 set) 주입 시: 그 set에서 signal보다 뒤 첫 날(없으면 None) — 테스트/명시 캘린더용.
    - 미주입(실운영): 주말 제외 + KRX 휴장표 제외로 *전진* 계산. 월말→다음달, 연말→다음연도 자동 처리.
    pykrx는 미래 거래일을 모르므로(시세 없음) 휴장표가 미래 판정의 근거다. 단순 +1일/평일 추정 아님."""
    sd = str(signal_date)[:10]
    if calendar is not None and calendar.get("tradingDays"):
        later = sorted(d for d in calendar["tradingDays"] if d > sd)
        return later[0] if later else None
    hol = set(KRX_HOLIDAYS) | set(holidays or [])
    try:
        d = date.fromisoformat(sd)
    except ValueError:
        return None
    for _ in range(max_lookahead_days):
        d = d + timedelta(days=1)
        iso = d.isoformat()
        if d.weekday() >= 5 or iso in hol:      # 주말/휴장 제외
            continue
        return iso
    return None


def _forward_trading_days_between(signal_iso: str, candidate_iso: str, holidays: set) -> list:
    out = []
    d = date.fromisoformat(signal_iso) + timedelta(days=1)
    end = date.fromisoformat(candidate_iso)
    while d < end:
        iso = d.isoformat()
        if d.weekday() < 5 and iso not in holidays:
            out.append(iso)
        d = d + timedelta(days=1)
    return out


def validate_next_execution_date(signal_date: str, candidate, calendar=None, *, holidays=None):
    """READY 게이트. candidate가 signalAsOfDate '다음 첫' KRX 거래일인지 검증.
    null / 이전·동일 / 비거래일(주말·휴장) / 사이 거래일 스킵 → (False, 사유). 통과 → (True, 'ok')."""
    sd = str(signal_date)[:10]
    if candidate in (None, ""):
        return False, "nextExecutionDateCandidate is null"
    cand = str(candidate)[:10]
    if cand <= sd:
        return False, f"candidate {cand} not after signalAsOfDate {sd}"
    hol = set(KRX_HOLIDAYS) | set(holidays or [])
    if calendar is not None and calendar.get("tradingDays"):
        tdays = calendar["tradingDays"]
        if cand not in tdays:
            return False, f"candidate {cand} not a KRX trading day"
        between = sorted(d for d in tdays if sd < d < cand)
        if between:
            return False, f"candidate {cand} skips KRX trading day(s) {between}"
        return True, "ok"
    try:
        cd = date.fromisoformat(cand)
    except ValueError:
        return False, f"candidate {cand} not a valid date"
    if cd.weekday() >= 5 or cand in hol:
        return False, f"candidate {cand} not a KRX trading day (weekend/holiday)"
    between = _forward_trading_days_between(sd, cand, hol)
    if between:
        return False, f"candidate {cand} skips KRX trading day(s) {between}"
    return True, "ok"


def read_only_next_execution_for_package(package_dir, *, holidays=None) -> dict:
    """기존 TEMP 패키지를 *수정하지 않고*(read-only) signalAsOfDate의 다음 KRX 거래일만 계산해 보고.
    manifest.json만 읽고 어떤 파일도 쓰지 않는다."""
    pkg = Path(package_dir)
    man = json.loads((pkg / "manifest.json").read_text(encoding="utf-8"))
    sig = man.get("signalAsOfDate")
    cand = next_krx_trading_day(sig, None, holidays=holidays)
    ok, reason = validate_next_execution_date(sig, cand, None, holidays=holidays)
    return {"signalAsOfDate": sig, "nextExecutionDateCandidate": cand, "valid": ok, "reason": reason,
            "packageStatusOnDisk": man.get("packageStatus"),
            "writeCount": 0, "filesModified": False}


# ===== 기본 universe / ranking (네트워크는 실제 실행 때만; DART 갱신 0 강제) =====

def _default_build_payload() -> dict:
    """build_market_snapshot_fast.build_payload 호출. DART 네트워크 갱신 0 강제(read-only 캐시)."""
    import build_market_snapshot_fast as bmsf
    bmsf.MAX_DART_REFRESH_PER_RUN = 0          # 재무 갱신 차단(캐시 불변)
    return bmsf.build_payload()


def _default_ranking(rows, mode, blacklist):
    """기존 마법공식 ranking 함수 재사용(산식 복제 0)."""
    import build_magic_formula_fund as mff
    return mff.calculate_magic_formula_ranking(rows, mode, blacklist)


def _resolve_formula_meta(formula_version, formula_mode):
    if formula_version is not None and formula_mode is not None:
        return formula_version, formula_mode
    import build_magic_formula_fund as mff
    return (formula_version or mff.CONFIG["formulaVersion"],
            formula_mode or mff.CONFIG["formulaMode"])


def _git_head() -> str:
    try:
        import subprocess
        out = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ===== 재무 입력 감사 (read-only) =====

def _dart_cache_listing_hash(d):
    try:
        entries = []
        for name in sorted(os.listdir(d)):
            fp = os.path.join(d, name)
            try:
                st = os.stat(fp)
                entries.append(f"{name}:{st.st_size}:{int(st.st_mtime)}")
            except OSError:
                continue
        return _sha256_bytes("\n".join(entries).encode("utf-8")), len(entries)
    except (FileNotFoundError, OSError, NotADirectoryError):
        return None, 0


def financial_input_manifest(universe_rows) -> dict:
    dist = {}
    for r in (universe_rows or []):
        y = str(r.get("dartLatestYear"))
        dist[y] = dist.get(y, 0) + 1
    listing_hash, count = _dart_cache_listing_hash(DART_CACHE_DIR)
    fu_count = None
    try:
        doc = json.loads(Path(FINANCIAL_UNIVERSE_PATH).read_text(encoding="utf-8"))
        fu_count = len(doc.get("data") or []) if isinstance(doc, dict) else None
    except (FileNotFoundError, OSError, ValueError):
        pass
    return {
        "financialUniverseFile": str(FINANCIAL_UNIVERSE_PATH),
        "financialUniverseSha256": _sha256_file(FINANCIAL_UNIVERSE_PATH),
        "financialUniverseMtime": _mtime(FINANCIAL_UNIVERSE_PATH),
        "financialUniverseCount": fu_count,
        "dartCorpCodesFile": str(DART_CORP_CODES_PATH),
        "dartCorpCodesSha256": _sha256_file(DART_CORP_CODES_PATH),
        "dartCacheDir": str(DART_CACHE_DIR),
        "dartCacheFileCount": count,
        "dartCacheListingSha256": listing_hash,
        "dartLatestYearDistribution": dist,
        "dartNetworkRefreshThisRun": False,
        "dartRefreshCount": 0,
        "maxDartRefreshPerRun": 0,
        "financialCacheReadOnly": True,
    }


# ===== 결과 골격 =====

def _result(status, signal_date, *, now=None, market_close_at=None, output_dir=None,
            next_exec=None, run_reason=None, prepare_only=False, warnings=None, **extra) -> dict:
    base = {
        "schemaVersion": SCHEMA_VERSION,
        "packageStatus": status,
        "signalAsOfDate": signal_date,
        "rankingGeneratedAt": now.isoformat() if isinstance(now, datetime) else now,
        "marketCloseAt": market_close_at.isoformat() if isinstance(market_close_at, datetime) else market_close_at,
        "nextExecutionDateCandidate": next_exec,
        "executionPriceAvailable": False,
        "officialTradeCreated": False,
        "officialStartDatePersisted": False,
        "productionWriteCount": 0,
        "publicCopyCount": 0,
        "prepareOnly": prepare_only,
        "outputDir": str(output_dir) if output_dir is not None else None,
        "runReason": run_reason,
        "warnings": warnings or [],
        "blocked": status not in (READY, ALREADY_PREPARED),
    }
    base.update(extra)
    return base


# ===== 메인: TEMP 신호 패키지 생성 =====

def build_signal_package(signal_date: str, *, now=None, market_close_at=None, output_dir=None,
                         prepare_only: bool = False, build_payload_fn=None, ranking_fn=None,
                         calendar=None, blacklist=None, code_commit=None,
                         formula_version=None, formula_mode=None,
                         min_valid_price_fraction: float = DEFAULT_MIN_VALID_PRICE_FRACTION,
                         baseline_universe_count: Optional[int] = None,
                         min_universe_count: int = 0, extra_holidays=None) -> dict:
    """신호 패키지를 TEMP에만 원자적으로 생성한다. production 쓰기 0. 항상 manifest dict를 반환한다."""
    signal_date = str(signal_date)[:10]
    blacklist = set(blacklist or [])

    # 0) now / marketCloseAt 기본값 + tz-aware 검증 (PC 날짜만으로 종가 확정 판단 금지)
    now_dt = _parse_aware_dt(now) if now is not None else datetime.now(_KST)
    if market_close_at is not None:
        close_dt = _parse_aware_dt(market_close_at)
    else:
        y, m, d = int(signal_date[:4]), int(signal_date[5:7]), int(signal_date[8:10])
        close_dt = datetime(y, m, d, 15, 30, tzinfo=_KST)
    if now_dt is None or close_dt is None:
        return _result(BLOCKED_SIGNAL_MARKET_NOT_CLOSED, signal_date,
                       now=now if isinstance(now, str) else None,
                       market_close_at=market_close_at if isinstance(market_close_at, str) else None,
                       run_reason="now/marketCloseAt must be tz-aware ISO-8601 (naive/parse fail)")

    # 1) 장 마감 검증: now <= marketCloseAt 이면 종가 미확정
    if now_dt <= close_dt:
        return _result(BLOCKED_SIGNAL_MARKET_NOT_CLOSED, signal_date, now=now_dt, market_close_at=close_dt,
                       run_reason=f"now {now_dt.isoformat()} <= marketCloseAt {close_dt.isoformat()}")

    # 2) 출력 경로 안전성(저장소 내부 금지)
    out_dir = Path(output_dir) if output_dir is not None else _default_output_dir(signal_date)
    if _is_unsafe_output(out_dir):
        return _result(BLOCKED_UNSAFE_OUTPUT_PATH, signal_date, now=now_dt, market_close_at=close_dt,
                       output_dir=out_dir, run_reason=f"output-dir inside repo: {out_dir}")

    formula_version, formula_mode = _resolve_formula_meta(formula_version, formula_mode)
    code_commit = code_commit if code_commit is not None else _git_head()

    work_dir = None
    next_exec = None
    try:
        # 3) 다음 KRX 거래일(체결일 후보) 계산 + READY 게이트 (network 전에 차단)
        next_exec = next_krx_trading_day(signal_date, calendar, holidays=extra_holidays)
        ok_next, next_reason = validate_next_execution_date(signal_date, next_exec, calendar,
                                                            holidays=extra_holidays)
        if not ok_next:
            return _result(BLOCKED_NEXT_EXECUTION_DATE_UNAVAILABLE, signal_date, now=now_dt,
                           market_close_at=close_dt, output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"next execution trading day invalid: {next_reason}")

        # 4) universe (build_payload; DART 갱신 0)
        bpf = build_payload_fn or _default_build_payload
        payload = bpf()
        meta = (payload or {}).get("meta") or {}
        rows = (payload or {}).get("data") or []
        base_date = str(meta.get("baseDate") or "")

        # 4) baseDate == signalAsOfDate (이전 거래일 데이터면 NOT_READY; 라벨만 임의변경 금지)
        if base_date != signal_date:
            return _result(BLOCKED_SIGNAL_UNIVERSE_NOT_READY, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"universe baseDate {base_date!r} != signalAsOfDate {signal_date}",
                           universeBaseDate=base_date, universeCount=len(rows))

        # 5) universe 종목 수 검증(비정상 감소 차단)
        if len(rows) < min_universe_count:
            return _result(BLOCKED_SIGNAL_UNIVERSE_NOT_READY, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"universe count {len(rows)} < min {min_universe_count}",
                           universeBaseDate=base_date, universeCount=len(rows))
        if baseline_universe_count and len(rows) < baseline_universe_count * 0.5:
            return _result(BLOCKED_SIGNAL_UNIVERSE_NOT_READY, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"universe count {len(rows)} dropped >50% vs baseline {baseline_universe_count}",
                           universeBaseDate=base_date, universeCount=len(rows))

        # 6) 종목코드 중복 0
        codes = [str(r.get("symbol") or r.get("code") or "").strip() for r in rows]
        nonempty = [c for c in codes if c]
        if len(set(nonempty)) != len(nonempty):
            return _result(BLOCKED_SIGNAL_UNIVERSE_NOT_READY, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason="duplicate codes in universe",
                           universeBaseDate=base_date, universeCount=len(rows))

        # 7) 가격 데이터 확정 검증(0/null/음수 비율). 라벨만 signal이고 실제 가격이 비정상이면 DATE_MISMATCH.
        def _valid_price(r):
            p = r.get("price")
            try:
                return p is not None and float(p) > 0
            except (TypeError, ValueError):
                return False
        valid = sum(1 for r in rows if _valid_price(r))
        frac = (valid / len(rows)) if rows else 0.0
        if frac < min_valid_price_fraction:
            return _result(BLOCKED_SIGNAL_DATE_MISMATCH, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"valid signalClosePrice fraction {round(frac,4)} < {min_valid_price_fraction}",
                           universeBaseDate=base_date, universeCount=len(rows), validPriceFraction=round(frac, 4))

        # 8) ranking (기존 함수 재사용; DART 캐시 read-only)
        rfn = ranking_fn or _default_ranking
        ranking_out = rfn(rows, formula_mode, blacklist)
        final = ranking_out[0] if isinstance(ranking_out, (list, tuple)) else ranking_out
        final = final or []
        if len(final) < DEFAULT_TOP10_N:
            return _result(BLOCKED_MISSING_RANKING, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason=f"ranking produced {len(final)} (<{DEFAULT_TOP10_N})",
                           universeBaseDate=base_date, universeCount=len(rows))

        # 9) 패키지 콘텐츠 구성
        def _top_row(s):
            row = {k: s.get(k) for k in TOP_FIELDS}
            row["signalClosePrice"] = s.get("price")
            return row
        top10 = [_top_row(s) for s in final[:DEFAULT_TOP10_N]]
        top_audit = [_top_row(s) for s in final[:DEFAULT_TOP_AUDIT_N]]

        # 전체 유효 universe 기준 *독립* 순위 top100. final은 valueRank/profitabilityRank가
        # eligible 전체에서 dense 1..N(코드 tiebreak로 unique)이라, 각 rank로 정렬해 자르면
        # 표시 순위가 1~100 연속이 된다. combined top100/top10과 달리 후보 subset이 아니다.
        value_top100 = [_top_row(s) for s in
                        sorted(final, key=lambda s: (s.get("valueRank") if s.get("valueRank") is not None else 1 << 30,
                                                     str(s.get("code") or "")))[:DEFAULT_GLOBAL_TOP_N]]
        profitability_top100 = [_top_row(s) for s in
                                sorted(final, key=lambda s: (s.get("profitabilityRank") if s.get("profitabilityRank") is not None else 1 << 30,
                                                             str(s.get("code") or "")))[:DEFAULT_GLOBAL_TOP_N]]

        universe_bytes = _canonical_bytes(payload)
        universe_sha = _sha256_bytes(universe_bytes)

        rankings_doc = {
            "schemaVersion": SCHEMA_VERSION,
            "formulaVersion": formula_version,
            "formulaMode": formula_mode,
            "signalAsOfDate": signal_date,
            "universeBaseDate": base_date,
            "rankingGeneratedAt": now_dt.isoformat(),
            "generatedBy": GENERATED_BY,
            "codeCommit": code_commit,
            "universeSha256": universe_sha,
            "rankingCount": len(final),
            "top10": top10,
            "top100": top_audit,
            # --- MF-GLOBAL-RANKINGS-PIPELINE: additive global 순위 원천 ---
            "globalRankingSchemaVersion": GLOBAL_RANKING_SCHEMA_VERSION,
            "rankingScope": "global-eligible-universe",
            "eligibleCount": len(final),
            "valueTop100": value_top100,
            "profitabilityTop100": profitability_top100,
        }
        rankings_bytes = _canonical_bytes(rankings_doc)
        rankings_sha = _sha256_bytes(rankings_bytes)

        fin_manifest = financial_input_manifest(rows)
        warnings = []
        if not blacklist:
            warnings.append("blacklist empty: signal package applies formula exclusions only")
        if next_exec and next_exec > KRX_HOLIDAY_CALENDAR_THROUGH:
            warnings.append(f"nextExecutionDateCandidate {next_exec} beyond curated KRX holiday horizon "
                            f"{KRX_HOLIDAY_CALENDAR_THROUGH}; verify KRX holidays before execution")

        manifest = {
            "schemaVersion": SCHEMA_VERSION,
            "packageStatus": READY,
            "signalAsOfDate": signal_date,
            "universeBaseDate": base_date,
            "rankingGeneratedAt": now_dt.isoformat(),
            "marketCloseAt": close_dt.isoformat(),
            "now": now_dt.isoformat(),
            "nextExecutionDateCandidate": next_exec,
            "executionPriceAvailable": False,
            "officialTradeCreated": False,
            "officialStartDatePersisted": False,
            "productionWriteCount": 0,
            "publicCopyCount": 0,
            "sourceCodeCommit": code_commit,
            "formulaVersion": formula_version,
            "formulaMode": formula_mode,
            "universeFile": "universe.json",
            "universeSha256": universe_sha,
            "universeCount": len(rows),
            "rankingsFile": "rankings.json",
            "rankingsSha256": rankings_sha,
            "rankingCount": len(final),
            "top10Count": len(top10),
            "valueTop100Count": len(value_top100),
            "profitabilityTop100Count": len(profitability_top100),
            "rankingScope": "global-eligible-universe",
            "financialInputManifest": fin_manifest,
            "warnings": warnings,
            "createdAt": now_dt.isoformat(),
            "outputDir": str(out_dir),
        }

        # 10) prepare-only: 파일 생성 없이 검증·해시만 반환
        if prepare_only:
            res = dict(manifest)
            res.update({"packageStatus": READY, "prepareOnly": True, "filesWritten": False,
                        "blocked": False, "runReason": "prepare-only: validated, no files written"})
            return res

        # 11) 동일 signalAsOfDate 기존 패키지 처리(자동 덮어쓰기 금지)
        if out_dir.exists():
            existing = None
            try:
                existing = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, ValueError):
                existing = None
            same = (existing and existing.get("packageStatus") == READY
                    and existing.get("universeSha256") == universe_sha
                    and existing.get("rankingsSha256") == rankings_sha)
            if same:
                res = dict(existing)
                res.update({"packageStatus": ALREADY_PREPARED, "blocked": False,
                            "runReason": "identical package already prepared (hash match)",
                            "outputDir": str(out_dir), "filesWritten": False})
                return res
            return _result(BLOCKED_PACKAGE_CONFLICT, signal_date, now=now_dt, market_close_at=close_dt,
                           output_dir=out_dir, next_exec=next_exec,
                           run_reason="existing package for signalAsOfDate has different hash (no auto-overwrite)",
                           universeSha256=universe_sha, rankingsSha256=rankings_sha,
                           existingUniverseSha256=(existing or {}).get("universeSha256"),
                           existingRankingsSha256=(existing or {}).get("rankingsSha256"))

        # 12) 원자적 생성: work_dir에 작성 후 atomic rename
        work_dir = out_dir.parent / (out_dir.name + ".partial")
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        work_dir.mkdir(parents=True, exist_ok=True)
        (work_dir / "universe.json").write_bytes(universe_bytes)
        (work_dir / "rankings.json").write_bytes(rankings_bytes)
        (work_dir / "manifest.json").write_bytes(_canonical_bytes(manifest))
        out_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(work_dir, out_dir)          # 대상 미존재 → 원자적 디렉터리 교체
        work_dir = None

        res = dict(manifest)
        res.update({"blocked": False, "filesWritten": True,
                    "universePath": str(out_dir / "universe.json"),
                    "rankingsPath": str(out_dir / "rankings.json"),
                    "manifestPath": str(out_dir / "manifest.json"),
                    "runReason": "signal package prepared (TEMP only, production untouched)"})
        return res

    except Exception as err:  # noqa: BLE001
        if work_dir is not None:
            shutil.rmtree(work_dir, ignore_errors=True)   # 불완전 작업물 정리
        return _result(BLOCKED_GENERATION_ERROR, signal_date, now=now_dt, market_close_at=close_dt,
                       output_dir=out_dir, next_exec=next_exec,
                       run_reason=f"{type(err).__name__}: {err}")


# ===== CLI =====

def _parse_args(argv=None):
    ap = argparse.ArgumentParser(description="와바바 마법공식 TEMP 신호 패키지 생성기 (production 쓰기 0)")
    ap.add_argument("--signal-date", required=True, help="YYYY-MM-DD (종가 기준 신호일)")
    ap.add_argument("--output-dir", default=None, help="절대경로(생략 시 시스템 TEMP)")
    ap.add_argument("--market-close-at", default=None, help="tz-aware ISO-8601 (종가 확정시각)")
    ap.add_argument("--now", default=None, help="tz-aware ISO-8601 (테스트·재현용)")
    ap.add_argument("--prepare-only", action="store_true", help="검증·미리계산만, 파일 생성 0")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    res = build_signal_package(args.signal_date, now=args.now, market_close_at=args.market_close_at,
                               output_dir=args.output_dir, prepare_only=args.prepare_only)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res.get("packageStatus") in (READY, ALREADY_PREPARED) else 2


if __name__ == "__main__":
    import sys
    sys.exit(main())
