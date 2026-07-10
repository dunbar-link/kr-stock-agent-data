"""batch_runner — TTM 분기수집 배치 실행기 (Phase MF-TTM-ELIGIBLE-BATCH-RUNNER-PREP).

기존 quarterly_ttm_poc 코드/캐시를 재사용하는 얇은 오케스트레이션 래퍼(새 프레임워크 아님).
배치 1개 단위로 dry-run / offline / (승인 시) real-fetch 를 실행하고, resume·020 backoff·상태파일을 관리한다.

기본은 안전모드(dry-run). real DART 호출은 --real-fetch + 유효 --confirm 토큰이 있을 때만.
민감정보(API 키·요청 URL 키·쿠키·토큰)는 상태파일/로그에 저장하지 않는다.

실행 예:
  python batch_runner.py --batch-id batch-01 --dry-run
  python batch_runner.py --batch-id batch-01 --offline
  python batch_runner.py --batch-id batch-01 --offline --resume
  python batch_runner.py --batch-id batch-01 --real-fetch --confirm RUN_TTM_BACKFILL_2026-07-09_batch-01   # 승인 필요(이번 Phase 미실행)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quarterly_ttm_poc as Q  # 기존 PoC 재사용
import ttm_core as C

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "_cache" / "ttm-poc-output"
MANIFEST = OUT_DIR / "ttm-batch-manifest.json"
CONFIG = Path(__file__).resolve().parent / "ttm_poc_config.json"

# 상태 값
ST_PENDING = "PENDING"
ST_COMPLETE = "COMPLETE"
ST_PARTIAL = "PARTIAL"
ST_RATE_LIMITED = "RATE_LIMITED"
ST_FAILED = "FAILED"
ST_SKIPPED_CACHE = "SKIPPED_CACHE_HIT"

REQ_REPORTS = [(2026, C.Q1_REPORT_CODE), (2025, C.ANNUAL_REPORT_CODE),
               (2025, C.Q3_REPORT_CODE), (2025, C.HALF_REPORT_CODE), (2025, C.Q1_REPORT_CODE)]


def load_manifest():
    if not MANIFEST.exists():
        raise SystemExit("manifest 없음 — 먼저 `node scripts/poc/build_batch_manifest.mjs` 실행")
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def state_path(batch_id: str, output_dir: Path) -> Path:
    return output_dir / f"batch-state-{batch_id}.json"


def load_state(batch_id: str, output_dir: Path) -> dict:
    p = state_path(batch_id, output_dir)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"batchId": batch_id, "stocks": {}, "apiCalls": 0, "rateLimited": False, "resumeAfterSelectionIndex": None}


def save_state(state: dict, output_dir: Path):
    # 민감정보 저장 금지: state 에는 상태·카운트만. (키/URL/쿠키/토큰 없음)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path(state["batchId"], output_dir).write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def cache_complete(corp_code: str, fs_div: str) -> bool:
    if not corp_code:
        return False
    for (y, r) in REQ_REPORTS:
        if not (Q.QUARTERLY_CACHE_DIR / C.quarterly_cache_filename(corp_code, y, r, fs_div)).exists():
            return False
    return True


def valid_confirm_token(token: str, base_date: str, batch_id: str) -> bool:
    # 패턴: RUN_TTM_BACKFILL_<baseDate>_<batchId>
    if not token:
        return False
    return token.strip() == f"RUN_TTM_BACKFILL_{base_date}_{batch_id}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="TTM 분기수집 배치 실행기(기본 안전모드=dry-run)")
    ap.add_argument("--batch-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--real-fetch", action="store_true")
    ap.add_argument("--confirm", default="")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--max-api-calls", type=int, default=0)
    ap.add_argument("--sleep-seconds", type=float, default=None)
    ap.add_argument("--stop-on-020", dest="stop_on_020", action="store_true", default=True)
    ap.add_argument("--output-dir", default=str(OUT_DIR))
    args = ap.parse_args(argv)

    output_dir = Path(args.output_dir)
    manifest = load_manifest()
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    bc = cfg.get("batchConfig", {})
    confirmations = cfg.get("officialIrConfirmations", [])
    base_date = manifest.get("universeBaseDate", "unknown")
    sleep = args.sleep_seconds if args.sleep_seconds is not None else bc.get("requestSleepSeconds", 0.4)
    max_calls = args.max_api_calls or bc.get("maxApiCallsPerBatch", 1200)

    stocks = [s for s in manifest["stocks"] if s["batchId"] == args.batch_id]
    if not stocks:
        raise SystemExit(f"batch-id 종목 없음: {args.batch_id}")

    # 모드 결정: real-fetch 는 토큰 필수. 아니면 offline. 둘 다 아니면 dry-run(기본).
    mode = "dry-run"
    if args.real_fetch:
        if not valid_confirm_token(args.confirm, base_date, args.batch_id):
            raise SystemExit(
                f"real-fetch 차단: 유효 confirm 토큰 필요 → RUN_TTM_BACKFILL_{base_date}_{args.batch_id}")
        mode = "real-fetch"
    elif args.offline:
        mode = "offline"
    if args.dry_run:
        mode = "dry-run"

    # quarterly_ttm_poc 전역 제어(재사용)
    Q.OFFLINE = (mode != "real-fetch")     # real-fetch 아니면 네트워크 금지
    Q.SLEEP = sleep

    state = load_state(args.batch_id, output_dir) if args.resume else \
        {"batchId": args.batch_id, "stocks": {}, "apiCalls": 0, "rateLimited": False, "resumeAfterSelectionIndex": None}
    state["mode"] = mode
    corp_map = Q.load_corp_code_map()
    stats = {"dartCalls": 0, "cacheHit": 0, "cacheMiss": 0, "noData": 0}

    plan_calls = 0
    processed = 0
    rate_limited = False
    fatal_stop = False
    counts = {}
    results = []

    for s in stocks:
        code = s["stockCode"]
        prev = state["stocks"].get(code, {}).get("status")
        if args.resume and prev in (ST_COMPLETE, ST_SKIPPED_CACHE):
            continue  # 완료/캐시 종목 재호출 금지
        has_cache = cache_complete(s["corpCode"], s["fsDiv"])
        missing = sum(1 for (y, r) in REQ_REPORTS
                      if not (Q.QUARTERLY_CACHE_DIR / C.quarterly_cache_filename(s["corpCode"], y, r, s["fsDiv"])).exists())

        # dry-run: 계획만(호출 0). 캐시 완비면 SKIP, 아니면 필요 콜 수 집계.
        if mode == "dry-run":
            if has_cache:
                state["stocks"][code] = {"status": ST_SKIPPED_CACHE}
            else:
                plan_calls += missing
                state["stocks"][code] = {"status": ST_PENDING, "plannedCalls": missing}
            continue

        # real-fetch: 캐시 완비면 신규호출 없이 SKIP(재호출 금지)
        if mode == "real-fetch" and has_cache:
            state["stocks"][code] = {"status": ST_SKIPPED_CACHE}
            continue
        if mode == "real-fetch" and (state["apiCalls"] + stats["dartCalls"]) >= max_calls:
            state["resumeAfterSelectionIndex"] = s["selectionIndex"] - 1
            break

        # offline: 캐시 있는 종목만 실제 복원(없으면 PARTIAL, 호출 0). real-fetch: 신규 수집.
        if mode == "offline" and not has_cache:
            state["stocks"][code] = {"status": ST_PARTIAL, "reason": "offline_no_cache"}
            continue

        stock_in = {"code": code, "name": s["companyName"], "fsDiv": s["fsDiv"], "industry": s.get("industry")}
        try:
            res = Q.process_stock(stock_in, corp_map, stats, confirmations)
        except Q.RateLimited:
            state["stocks"][code] = {"status": ST_RATE_LIMITED}
            state["rateLimited"] = True
            state["resumeAfterSelectionIndex"] = s["selectionIndex"] - 1
            rate_limited = True
            break  # 020 즉시 중단, 자동 재시도 금지
        except Q.FatalDartError as e:
            # 012 접근불가 IP / 901 키만료 등 권한·영구 오류 → 즉시 중단(재시도 무의미). 이 종목은 미완료로 남김.
            state["stocks"][code] = {"status": ST_RATE_LIMITED, "fatal": str(e)}
            state["fatalDartError"] = str(e)
            state["resumeAfterSelectionIndex"] = s["selectionIndex"] - 1
            fatal_stop = True
            break
        except Exception as e:  # noqa: BLE001
            state["stocks"][code] = {"status": ST_FAILED, "error": type(e).__name__}
            continue
        st = ST_COMPLETE if (res.get("ttmRevenue") is not None) else ST_PARTIAL
        state["stocks"][code] = {"status": st, "qualityStatus": res.get("qualityStatus")}
        counts[res.get("qualityStatus")] = counts.get(res.get("qualityStatus"), 0) + 1
        results.append(res)
        processed += 1

    state["apiCalls"] += stats["dartCalls"]
    save_state(state, output_dir)

    summary = {
        "batchId": args.batch_id, "mode": mode, "universeBaseDate": base_date,
        "batchStockCount": len(stocks),
        "plannedNewApiCalls_dryRun": plan_calls if mode == "dry-run" else None,
        "actualApiCalls": stats["dartCalls"],
        "cacheHitSkipped": sum(1 for v in state["stocks"].values() if v.get("status") == ST_SKIPPED_CACHE),
        "processed": processed, "rateLimited": rate_limited,
        "fatalDartStop": fatal_stop, "fatalDartError": state.get("fatalDartError"),
        "gateCounts": counts if counts else None,
        "resumeAfterSelectionIndex": state.get("resumeAfterSelectionIndex"),
        "statePath": str(state_path(args.batch_id, output_dir)),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    # 안전 단언: 이번 Phase 는 real-fetch 미실행 → dry-run/offline 은 actualApiCalls==0
    if mode in ("dry-run", "offline") and stats["dartCalls"] != 0:
        print("[ERROR] 안전모드인데 API 호출 발생!", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
