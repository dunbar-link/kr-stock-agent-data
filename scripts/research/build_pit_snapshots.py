#!/usr/bin/env python3
"""PHASE 1/2 — point-in-time 월별 스냅샷 구축 (장기 백테스트 입력).

한 스냅샷 = 특정 거래일 t 시점에 **실제로 공개돼 있던** 정보만 담는다.
  - 그 날 상장돼 있던 종목만(get_market_cap_by_ticker(t, market=...) 의 index)
    → 상장폐지 종목도 그 날 살아있었으면 포함된다(survivorship bias 제거)
  - 그 날의 종가·시가총액·상장주식수
  - 그 날 KRX 가 공표한 PER/PBR/EPS/BPS/DIV/DPS
    → KRX 는 "그 시점 최신 공시 재무"로 계산해 매일 공표하므로 구조적으로 point-in-time 이다
      (미래에 정정된 재무로 과거를 다시 계산하지 않는다 = look-ahead 없음)

저장: _cache/pit-snapshots/<YYYY-MM-DD>.csv.gz  (gitignore 대상 — 커밋하지 않는다)
      _cache/pit-snapshots/_ticker-names.json   (연 1회 갱신되는 누적 종목명 맵)
      _cache/pit-snapshots/_trading-days.json   (KOSPI 지수 기준 실제 거래일 목록)

안전: 전부 read-only 조회(KRX 공개 데이터). 실주문 0 · 브로커 API 0 · 유료 API 0.
      canonical 장부/운영 파일 미접근. 재실행 가능(이미 있는 스냅샷은 건너뛴다).

사용:
  python scripts/research/build_pit_snapshots.py --start 2000-01 --end 2026-08
  python scripts/research/build_pit_snapshots.py --status
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pit_acquire_guard as G  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "_cache" / "pit-snapshots"
TD_PATH = OUT_DIR / "_trading-days.json"
NAME_PATH = OUT_DIR / "_ticker-names.json"
CP_PATH = OUT_DIR / "_checkpoint.json"

COLS = ["ticker", "market", "close", "marketCap", "shares", "PER", "PBR", "EPS", "BPS", "DIV", "DPS"]


def log(msg):
    print(f"[pit] {msg}", file=sys.stderr, flush=True)


def load_trading_days(stock, start_ymd="19990101", end_ymd=None):
    """KOSPI 지수 시계열 = 실제 개장일. 평일 추정 금지(휴장일 오탐 방지)."""
    if TD_PATH.exists():
        try:
            return json.loads(TD_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
    end_ymd = end_ymd or datetime.now().strftime("%Y%m%d")
    df = stock.get_index_ohlcv_by_date(start_ymd, end_ymd, "1001")
    days = [str(d)[:10] for d in df.index]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    TD_PATH.write_text(json.dumps(days), encoding="utf-8")
    log(f"trading days {len(days)}: {days[0]} ~ {days[-1]}")
    return days


def month_first_trading_days(days, start_ym, end_ym):
    """각 월의 *첫 실제 거래일*. 달력 1일이 휴장이어도 자연히 다음 개장일이 잡힌다."""
    seen, out = set(), []
    for d in days:
        ym = d[:7]
        if ym < start_ym or ym > end_ym or ym in seen:
            continue
        seen.add(ym)
        out.append(d)
    return out


def fetch_names(stock, ymd, cache):
    """종목명 누적 맵. 이름은 거의 안 바뀌므로 연 1회만 갱신한다(호출 절약).

    get_market_ticker_name 은 종목당 1콜이라 4천 종목에 쓸 수 없다.
    get_market_price_change_by_ticker 는 한 콜로 전 종목 '종목명'을 준다(그 시점 기준 = PIT).
    금융/지주 등 이름 기반 제외 규칙에 쓰인다.
    """
    changed = False
    for mkt in ("ALL",):
        try:
            df = stock.get_market_price_change_by_ticker(ymd, ymd, market=mkt)
        except Exception as e:  # noqa: BLE001
            log(f"names {ymd} {mkt} skip: {type(e).__name__}: {e}")
            continue
        if df is None or not len(df) or "종목명" not in df.columns:
            continue
        for t, n in df["종목명"].items():
            t = str(t)
            if cache.get(t) != n:
                cache[t] = n
                changed = True
    return changed


def build_snapshot(stock, iso):
    """t 시점 스냅샷 1건. 실패하면 None(그 달은 비운다 — 추정으로 채우지 않는다)."""
    ymd = iso.replace("-", "")
    rows = {}
    # market="KOSPI"/"KOSDAQ" 로 직접 부르면 구간(2002~2009 등)에 따라 pykrx 내부에서 KeyError 가 난다(실측).
    #   → market="ALL" 한 콜로 전 종목을 받고, 시장 구분은 ticker_list 로 따로 붙인다.
    #     ticker_list 마저 실패하면 그 종목은 UNKNOWN 으로 두되 데이터는 버리지 않는다
    #     (COMBINED 분석은 그대로 가능하고, 시장별 분석에서만 제외된다).
    cap = stock.get_market_cap_by_ticker(ymd, market="ALL")
    if cap is None or not len(cap):
        return None
    member = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            for t in (stock.get_market_ticker_list(ymd, market=mkt) or []):
                member[str(t)] = mkt
        except Exception as e:  # noqa: BLE001
            log(f"{iso} ticker_list {mkt} unavailable: {type(e).__name__}")
    for t, r in cap.iterrows():
        t = str(t)
        rows[t] = {
            "ticker": t, "market": member.get(t, "UNKNOWN"),
            "close": r.get("종가"), "marketCap": r.get("시가총액"), "shares": r.get("상장주식수"),
            "PER": None, "PBR": None, "EPS": None, "BPS": None, "DIV": None, "DPS": None,
        }
    if not rows:
        return None
    fun = stock.get_market_fundamental_by_ticker(ymd, market="ALL")
    if fun is not None and len(fun):
        for t, r in fun.iterrows():
            tt = str(t)
            if tt in rows:
                for c in ("PER", "PBR", "EPS", "BPS", "DIV", "DPS"):
                    if c in fun.columns:
                        rows[tt][c] = r.get(c)
    return list(rows.values())


def write_snapshot(iso, rows):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p = OUT_DIR / f"{iso}.csv.gz"
    tmp = p.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        fh.write(",".join(COLS) + "\n")
        for r in rows:
            fh.write(",".join("" if r.get(c) is None else str(r.get(c)) for c in COLS) + "\n")
    os.replace(tmp, p)
    return p


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2000-01")
    ap.add_argument("--end", default=datetime.now().strftime("%Y-%m"))
    ap.add_argument("--status", action="store_true")
    # ★ 2026-08-08 실사고: sleep 0.15s 로 약 250콜을 몰아치자 KRX 가 **IP 차단**을 걸었다
    #   (data.krx.co.kr 로그인 응답이 JSON 대신 `.ip-block-page` HTML 로 바뀌고 pykrx import 자체가 실패).
    #   그 사이 와바바 일일 파이프라인(16:25 Auto Apply 등)도 같이 막힌다.
    #   → 기본 간격을 1.0s 로 올리고, 한 번에 받는 개월 수를 제한한다. 나눠서 여러 번 돌리는 것이 정상 사용법이다.
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="0이면 pit_acquire_guard 의 pacing(분당 안전콜 역산)을 쓴다. 수동 지정은 권장하지 않는다.")
    ap.add_argument("--max-per-run", type=int, default=0,
                    help="0이면 guard 가 --minutes-budget 으로 batch 를 역산한다(고정 60개월 가정 금지).")
    ap.add_argument("--minutes-budget", type=float, default=20.0,
                    help="이번 실행에 쓸 시간 예산(분). batch 크기를 이걸로 역산한다.")
    ap.add_argument("--preflight-only", action="store_true",
                    help="KRX 차단 여부만 read-only 1콜로 확인하고 종료(수집 안 함).")
    ap.add_argument("--plan-only", action="store_true",
                    help="네트워크 없이 '무엇을 몇 콜로 받을지'만 계산하고 종료.")
    args = ap.parse_args(argv)

    if args.status:
        n = len(list(OUT_DIR.glob("[12]*.csv.gz"))) if OUT_DIR.exists() else 0
        got = sorted(p.name.split(".")[0] for p in OUT_DIR.glob("[12]*.csv.gz")) if n else []
        cp = G.Checkpoint(CP_PATH)
        print(json.dumps({"snapshots": n, "first": got[0] if got else None,
                          "last": got[-1] if got else None,
                          "checkpoint": {k: cp.data.get(k) for k in
                                         ("runs", "totalCalls", "lastRunAt", "lastBlockedAt", "lastState")},
                          "emptyMonths": len(cp.data.get("empty", [])),
                          "failedMonths": len(cp.data.get("failed", {}))}, ensure_ascii=False))
        return 0

    # ── ① 선차단검사(preflight) — 대량 요청 *전에* read-only 1콜만 ────────────────
    #   차단 상태에서 수집 루프를 절대 시작하지 않는다. 반복 probe 도 하지 않는다(1회로 끝).
    cp = G.Checkpoint(CP_PATH)
    st = G.block_status(G.default_fetch)
    log(f"preflight: state={st['state']} http={st.get('httpStatus')} reason={st['reason']}")
    if st["state"] != "CLEAR":
        cp.mark_blocked()
        cp.data["lastState"] = st["state"]
        cp.save()
        print(json.dumps({"preflight": st, "acquired": 0,
                          "stopped": "BLOCKED_OR_UNKNOWN_NO_FURTHER_CALLS",
                          "note": "차단은 우회 대상이 아니라 정지 신호다. 재시도 루프를 돌리지 않는다."},
                         ensure_ascii=False))
        return 3
    if args.preflight_only:
        print(json.dumps({"preflight": st}, ensure_ascii=False))
        return 0

    from pykrx import stock  # noqa: PLC0415

    days = load_trading_days(stock)
    targets = month_first_trading_days(days, args.start, args.end)
    log(f"target months: {len(targets)}  {targets[0]} ~ {targets[-1]}")

    # ── ② 이번 실행 계획 — 남은 달만, 보수적 batch 로 ────────────────────────────
    pl = G.plan(targets, OUT_DIR, cp, minutes_budget=args.minutes_budget)
    batch_take = set(pl["take"])
    pace = args.sleep if args.sleep > 0 else G.pacing_seconds()
    log(f"plan: pending={pl['pendingTotal']} take={len(pl['take'])} "
        f"calls≈{pl['estimatedCalls']} pace={pace:.0f}s eta≈{pl['estimatedMinutes']}min "
        f"remainingAfter={pl['remainingAfter']}")
    if args.plan_only:
        print(json.dumps({"preflight": st, "plan": pl}, ensure_ascii=False))
        return 0
    cp.start_run()
    cp.save()

    names = {}
    if NAME_PATH.exists():
        try:
            names = json.loads(NAME_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            names = {}

    done = skipped = failed = 0
    last_name_year = None
    t0 = time.time()
    stop_reason = "COMPLETED_BATCH"
    hard_cap = args.max_per_run if args.max_per_run else len(pl["take"])
    for iso in targets:
        if iso not in batch_take:          # 이번 batch 대상만 (이미 받은 달·이월분은 건드리지 않는다)
            skipped += 1
            continue
        year = iso[:4]
        if year != last_name_year:
            if fetch_names(stock, iso.replace("-", ""), names):
                NAME_PATH.parent.mkdir(parents=True, exist_ok=True)
                NAME_PATH.write_text(json.dumps(names, ensure_ascii=False), encoding="utf-8")
            last_name_year = year

        # bounded retry + exponential backoff. 차단이 확인되면 재시도하지 않고 즉시 정지한다.
        rows, err = None, None
        for attempt in range(1, 4):
            try:
                rows = build_snapshot(stock, iso)
                err = None
                break
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {e}"
                bs = G.block_status(G.default_fetch)
                if bs["state"] == "BLOCKED":
                    log(f"{iso} 차단 감지 — 즉시 중단(재시도 안 함)")
                    cp.mark_blocked()
                    stop_reason = "BLOCKED_MID_RUN"
                    err = "BLOCKED"
                    break
                wait = G.backoff_seconds(attempt)
                log(f"{iso} 실패({err}) — {wait:.0f}s 후 재시도 {attempt}/3")
                time.sleep(wait)
        if err == "BLOCKED":
            break
        if err:
            log(f"{iso} FAILED(재시도 소진) {err}")
            cp.mark_failed(iso, err)
            failed += 1
            cp.save()
            continue
        if not rows:
            log(f"{iso} empty — 이 달은 데이터 없음으로 기록(재요청 안 함)")
            cp.mark_empty(iso)
            failed += 1
            cp.save()
            continue

        write_snapshot(iso, rows)
        cp.mark_done(iso)
        cp.save()                          # 매 건 원자적 저장 — 중간에 죽어도 진행분이 보존된다
        done += 1
        if done % 6 == 0:
            log(f"{iso}  done={done} failed={failed}  {time.time()-t0:.0f}s")
        if done >= hard_cap:
            stop_reason = "BATCH_LIMIT_REACHED"
            log(f"batch 상한 {hard_cap} 도달 — 정상 종료. 다시 실행하면 이어서 받는다.")
            break
        G.sleep_with_jitter(pace)          # 고정간격 대신 지터를 섞는다

    cp.data["lastState"] = stop_reason
    cp.save()
    remaining = len(G.pending_months(targets, OUT_DIR, cp))
    log(f"COMPLETE done={done} failed={failed} remaining={remaining} "
        f"stop={stop_reason} {time.time()-t0:.0f}s")
    print(json.dumps({"done": done, "failed": failed, "skipped": skipped,
                      "remaining": remaining, "stopReason": stop_reason,
                      "names": len(names), "totalCalls": cp.data["totalCalls"],
                      "outDir": str(OUT_DIR)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
