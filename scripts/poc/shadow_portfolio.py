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
from pathlib import Path

HERE = Path(__file__).resolve().parent
FREEZE = HERE / "fixtures" / "shadow-freeze-latest.json"
SNAP_DIR = HERE.parents[1] / "_cache" / "ttm-poc-output" / "shadow-snapshots"   # runtime(gitignore)


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
    mode = "real" if args.real else "offline"
    if mode == "real":
        print("[정보] --real 종가조회는 이번 Phase 미실행(승인/별도 Phase). --offline 로 캐시 계산만 검증하세요.")
        return 0

    # offline: 캐시 가격 없으면 계산 불가(임의 가격 금지) — 이번 Phase 는 계산 로직 검증까지
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    state_path = SNAP_DIR / "snapshots.json"
    existing = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else []
    price_map = {}  # offline + 캐시 없음 → 빈 맵(전 종목 missing 으로 안전 기록)
    results = []
    for k in strat_keys:
        if is_duplicate_snapshot(existing, args.snapshot_date, freeze["strategies"][k]["strategyType"]):
            results.append({"strategy": k, "skipped": "이미 존재(멱등)"}); continue
        snap = compute_snapshot(freeze["strategies"][k], price_map, args.snapshot_date)
        if not args.dry_run:
            existing.append(snap)
        results.append({"strategy": k, "missingPriceCount": snap["missingPriceCount"],
                        "sourceStatus": snap["sourceStatus"]})
    if not args.dry_run:
        state_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"mode": "offline", "snapshotDate": args.snapshot_date, "results": results,
                      "note": "offline+무캐시가격 → 전 종목 missing 기록(임의 보정 없음). 실가격은 --real 승인 Phase."},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
