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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import magic_rolling_engine as E  # make_calendar (순수 함수; 네트워크 0)

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

# 검증 임계값(테스트에서 주입 가능)
DEFAULT_MIN_VALID_PRICE_FRACTION = 0.5
DEFAULT_TOP10_N = 10
DEFAULT_TOP_AUDIT_N = 100

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


# ===== KRX 거래일 (신뢰 캘린더; 평일/+1일 추정 금지) =====

def next_krx_trading_day(signal_date: str, calendar) -> Optional[str]:
    """signalAsOfDate 다음 실제 KRX 거래일. 캘린더 set 기반(금→월, 휴장전→휴장후 정상). 없으면 None."""
    if not calendar or not calendar.get("tradingDays"):
        return None
    sd = str(signal_date)[:10]
    later = sorted(d for d in calendar["tradingDays"] if d > sd)
    return later[0] if later else None


def _default_calendar(signal_date: str, pykrx_stock=None):
    """signal 월 + 다음 월 실거래일을 합쳐 캘린더 생성(월말 신호의 다음달 첫 거래일 대비). 실패→None."""
    try:
        if pykrx_stock is None:
            from pykrx import stock as pykrx_stock  # lazy: import 시 network 회피
        y, m = int(signal_date[:4]), int(signal_date[5:7])
        months = [(y, m), (y + (1 if m == 12 else 0), 1 if m == 12 else m + 1)]
        days = []
        for (yy, mm) in months:
            for d in (pykrx_stock.get_business_days(yy, mm) or []):
                s = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
                days.append(s)
        return E.make_calendar(days) if days else None
    except Exception:
        return None


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
                         min_universe_count: int = 0) -> dict:
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
    cal = calendar if calendar is not None else _default_calendar(signal_date)
    next_exec = next_krx_trading_day(signal_date, cal)

    work_dir = None
    try:
        # 3) universe (build_payload; DART 갱신 0)
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
        }
        rankings_bytes = _canonical_bytes(rankings_doc)
        rankings_sha = _sha256_bytes(rankings_bytes)

        fin_manifest = financial_input_manifest(rows)
        warnings = []
        if not blacklist:
            warnings.append("blacklist empty: signal package applies formula exclusions only")

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
