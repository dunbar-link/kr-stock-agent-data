"""shadow_portfolio — Shadow Portfolio 스냅샷·성과 계산기 (Phase MF-TTM-SHADOW-PORTFOLIO-POC).

실주문 없음. 브로커 API 없음. 거래일 종가만. 미래 소급 없음. 리밸런싱 없음(buy-and-hold 관찰).
기본 안전모드(dry-run). 실제 가격조회(--snapshot-date + --real)는 이번 Phase 미실행.

모드:
  --initialize                 freeze fixture 검증(호출 0)
  --snapshot-date YYYY-MM-DD   해당일 스냅샷 계산
  --offline                    캐시 가격만 사용(없으면 누락 기록, 호출 0)
  --real                       pykrx 종가 조회(승인/명시 필요, 이번 미사용)
  --dry-run                    계획만(기본)
  --strategy annual|ttm|both

핵심 순수 함수(테스트 대상): compute_snapshot / performance / is_duplicate_snapshot
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
FREEZE = HERE / "fixtures" / "shadow-freeze-latest.json"
SNAP_DIR = HERE.parents[1] / "_cache" / "ttm-poc-output" / "shadow-snapshots"   # runtime(gitignore)
KOSPI_INDEX = "1001"


def _now_kst_date():
    return (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d")


def resolve_trading_date(requested_yyyymmdd: str):
    """요청일부터 역순으로 실제 거래일(종가합>0) 판정. pykrx read-only. 미래·임의 날짜 금지."""
    from pykrx import stock
    import pandas as pd
    base = datetime.strptime(requested_yyyymmdd, "%Y%m%d")
    for back in range(0, 14):
        cand = (base - timedelta(days=back)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_ticker(cand, market="KOSPI")
            if df is None or df.empty:
                continue
            close = df["종가"] if "종가" in df.columns else df.iloc[:, 3]
            if pd.to_numeric(close, errors="coerce").fillna(0).sum() > 0:
                return cand
        except Exception:
            continue
    return None


def fetch_closes(trading_date_yyyymmdd: str) -> dict:
    """거래일 전체 종목 종가(KOSPI+KOSDAQ). read-only."""
    from pykrx import stock
    prices = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(trading_date_yyyymmdd, market=mkt)
            if df is None or df.empty:
                continue
            col = "종가" if "종가" in df.columns else df.columns[3]
            for tk, val in df[col].items():
                try:
                    prices[str(tk).zfill(6)] = float(val)
                except Exception:
                    pass
        except Exception:
            pass
    return prices


def fetch_kospi_close(trading_date_yyyymmdd: str):
    from pykrx import stock
    try:
        df = stock.get_index_ohlcv_by_date(trading_date_yyyymmdd, trading_date_yyyymmdd, KOSPI_INDEX)
        if df is not None and not df.empty:
            col = "종가" if "종가" in df.columns else df.columns[-2]
            return float(df[col].iloc[0])
    except Exception:
        pass
    return None


# ---------- 순수 로직 ----------

def compute_snapshot(strategy: dict, price_map: dict, snapshot_date: str,
                     dividends: float = 0.0, transaction_costs: float = 0.0,
                     halted_codes: set | None = None) -> dict:
    """holdings 시장가치 + 현금 → 총자산·수익률. 가격 누락은 임의 보정 없이 기록."""
    halted_codes = halted_codes or set()
    holdings = strategy["holdings"]
    initial = strategy["initialCapital"]
    cash = strategy["cash"]
    market_value = 0.0
    missing, halted, contrib = [], [], []
    for h in holdings:
        code = h["stockCode"]
        shares = h.get("shares", 0)
        price = price_map.get(code)
        if code in halted_codes:
            halted.append(code)
        if price is None:
            missing.append(code)
            price = h.get("entryPrice")   # 마지막 유효가격 표시(누락 표기), 임의 보정 아님
            if price is None:
                continue
        mv = shares * price
        market_value += mv
        entry_mv = shares * (h.get("entryPrice") or 0)
        contrib.append({"stockCode": code, "companyName": h.get("companyName"),
                        "marketValue": mv, "pnl": mv - entry_mv,
                        "returnPct": round((price - h["entryPrice"]) / h["entryPrice"] * 100, 2)
                        if h.get("entryPrice") else None})
    total = market_value + cash + dividends - transaction_costs
    return {
        "snapshotDate": snapshot_date, "strategyType": strategy["strategyType"],
        "holdingsMarketValue": round(market_value, 2), "cash": cash, "dividends": dividends,
        "transactionCosts": transaction_costs, "totalAsset": round(total, 2),
        "cumulativeReturn": round((total - initial) / initial, 6),
        "cashWeight": round(cash / total, 4) if total else None,
        "missingPriceCount": len(missing), "missingPriceCodes": missing,
        "haltedCount": len(halted), "haltedCodes": halted,
        "contributions": sorted(contrib, key=lambda c: c["pnl"], reverse=True),
        "sourceStatus": "partial" if missing else "ok",
    }


def performance(snapshots: list, benchmark: list | None = None) -> dict:
    """스냅샷 이력 → 누적·주간·MDD·변동성. turnover=0(리밸런싱 없음)."""
    if not snapshots:
        return {"snapshots": 0}
    snaps = sorted(snapshots, key=lambda s: s["snapshotDate"])
    totals = [s["totalAsset"] for s in snaps]
    cum = snaps[-1]["cumulativeReturn"]
    weekly = []
    for i in range(1, len(totals)):
        prev = totals[i - 1]
        weekly.append((totals[i] - prev) / prev if prev else 0.0)
    # MDD
    peak = totals[0]; mdd = 0.0
    for t in totals:
        peak = max(peak, t)
        mdd = min(mdd, (t - peak) / peak if peak else 0.0)
    # 변동성(주간수익 표준편차, 표본)
    vol = None
    if len(weekly) >= 2:
        m = sum(weekly) / len(weekly)
        var = sum((w - m) ** 2 for w in weekly) / (len(weekly) - 1)
        vol = var ** 0.5
    wins = sum(1 for w in weekly if w > 0)
    return {
        "snapshots": len(snaps), "cumulativeReturn": round(cum, 6),
        "maxDrawdown": round(mdd, 6), "weeklyVolatility": round(vol, 6) if vol is not None else None,
        "weeklyWinRate": round(wins / len(weekly), 4) if weekly else None,
        "turnover": 0.0, "cashWeightLatest": snaps[-1].get("cashWeight"),
        "note": "리밸런싱 없음(buy-and-hold). 초기 몇 주로 우열 결론 금지. 최소 6개월 관찰.",
    }


def is_duplicate_snapshot(existing: list, snapshot_date: str, strategy_type: str) -> bool:
    return any(s.get("snapshotDate") == snapshot_date and s.get("strategyType") == strategy_type for s in existing)


# ---------- CLI ----------

def load_freeze():
    if not FREEZE.exists():
        raise SystemExit("freeze 없음 — 먼저 python scripts/poc/build_shadow_freeze.py 실행")
    return json.loads(FREEZE.read_text(encoding="utf-8"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Shadow Portfolio 계산기(기본 dry-run, 실주문 0)")
    ap.add_argument("--initialize", action="store_true")
    ap.add_argument("--snapshot-date", default=None)
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--real", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--strategy", choices=["annual", "ttm", "both"], default="both")
    args = ap.parse_args(argv)

    freeze = load_freeze()
    strat_keys = ["annual", "ttm"] if args.strategy == "both" else [args.strategy]

    if args.initialize or not args.snapshot_date:
        # freeze 검증만(호출 0)
        out = {"mode": "initialize/dry-run", "freezeDate": freeze["sourceMetadata"]["freezeDate"],
               "strategies": {k: {"holdings": len(freeze["strategies"][k]["holdings"]),
                                  "cash": freeze["strategies"][k]["cash"],
                                  "invested": freeze["strategies"][k]["investedAmount"]} for k in strat_keys},
               "overlap": freeze["overlap"]["commonCount"],
               "note": "dry-run: 가격조회 0. 실제 스냅샷은 --snapshot-date + (--offline|--real)."}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    # 스냅샷 모드
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    state_path = SNAP_DIR / "snapshots.json"
    existing = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else []
    requested = (args.snapshot_date or _now_kst_date()).replace("-", "")

    price_map, kospi_close, resolved = {}, None, None
    if args.real:
        # read-only 종가 조회(승인). 요청일→직전 거래일 판정. 미래·임의 날짜 금지.
        resolved = resolve_trading_date(requested)
        if not resolved:
            print(json.dumps({"verdict": "WAIT", "reason": "거래일 판정 실패(휴장/조회 실패)"}, ensure_ascii=False))
            return 3
        price_map = fetch_closes(resolved)
        kospi_close = fetch_kospi_close(resolved)
    else:
        resolved = requested  # offline: 계산 로직 검증(무캐시가격→전종목 missing)

    resolved_iso = f"{resolved[:4]}-{resolved[4:6]}-{resolved[6:8]}"
    freeze_date = freeze["sourceMetadata"]["freezeDate"]
    results = []
    for k in strat_keys:
        stype = freeze["strategies"][k]["strategyType"]
        if is_duplicate_snapshot(existing, resolved_iso, stype):
            results.append({"strategy": k, "duplicate": True, "snapshotDate": resolved_iso}); continue
        snap = compute_snapshot(freeze["strategies"][k], price_map, resolved_iso)
        snap["requestedDate"] = f"{requested[:4]}-{requested[4:6]}-{requested[6:8]}"
        snap["resolvedTradingDate"] = resolved_iso
        snap["sameAsFreezeDate"] = (resolved_iso == freeze_date)
        snap["kospiClose"] = kospi_close
        if not args.dry_run and args.real:
            existing.append(snap)
        results.append({"strategy": k, "totalAsset": snap["totalAsset"],
                        "cumulativeReturn": snap["cumulativeReturn"],
                        "missingPriceCount": snap["missingPriceCount"],
                        "sourceStatus": snap["sourceStatus"], "duplicate": False})
    if not args.dry_run and args.real:
        state_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    verdict = "PASS"
    total_missing = max((r.get("missingPriceCount", 0) for r in results if "missingPriceCount" in r), default=0)
    if args.real and total_missing > 0:
        verdict = "WARNING"
    def _res(k):
        return next((r for r in results if r.get("strategy") == k), {})
    out = {"verdict": verdict, "mode": "real" if args.real else "offline",
           "requestedDate": f"{requested[:4]}-{requested[4:6]}-{requested[6:8]}",
           "resolvedTradingDate": resolved_iso, "sameAsFreezeDate": resolved_iso == freeze_date,
           "kospiClose": kospi_close, "results": results, "realOrderCount": 0,
           "statePath": str(state_path)}
    print(json.dumps(out, ensure_ascii=False, indent=2))

    # 상태 요약(runtime, 미stage). 민감정보 없음. duplicate 시 기존 스냅샷 값으로 채움.
    if args.real and not args.dry_run:
        def _saved(stype, field):
            s = next((x for x in existing if x.get("resolvedTradingDate") == resolved_iso
                      and x.get("strategyType") == stype), {})
            return s.get(field)
        status = {
            "verdict": verdict, "executedAt": datetime.now(timezone(timedelta(hours=9))).isoformat(),
            "requestedDate": out["requestedDate"], "resolvedTradingDate": resolved_iso,
            "annualTotalAsset": _saved("OFFICIAL_ANNUAL", "totalAsset"), "annualReturn": _saved("OFFICIAL_ANNUAL", "cumulativeReturn"),
            "ttmTotalAsset": _saved("TTM_EXPERIMENT", "totalAsset"), "ttmReturn": _saved("TTM_EXPERIMENT", "cumulativeReturn"),
            "benchmarkKospiClose": kospi_close, "missingPriceCount": total_missing,
            "duplicate": all(r.get("duplicate") for r in results) if results else False, "realOrderCount": 0,
        }
        (SNAP_DIR / "status-latest.json").write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    # exit code: 0=성공/중복, 2=가격누락(WARNING), 3=거래일 실패(WAIT는 위에서 처리)
    if args.real and total_missing > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
