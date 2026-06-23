#!/usr/bin/env python3
"""magic_daily_signal 테스트 (Phase 45-AUTO2). 전부 주입형 fixture + tmp 경로. canonical/public write 0."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import magic_daily_signal as S
import magic_daily_common as C

NON_TRADING = "2026-06-20"   # 토요일
TRADING = "2026-06-24"       # 수요일(비휴장 가정)
AFTER = f"{TRADING}T16:00:00+09:00"
BEFORE = f"{TRADING}T14:00:00+09:00"


def fake_universe(signal=TRADING, n=12, price=1000.0):
    return {"data": [{"symbol": f"{i:06d}", "corpName": f"S{i}", "industryName": "제조",
                      "marketCap": 1000 + i, "price": price, "dartLatestYear": 2025, "dartFsDiv": "CFS"}
                     for i in range(1, n + 1)],
            "meta": {"baseDate": signal, "count": n}}


def fake_ranking(rows, mode, blacklist, k=11):
    out = []
    for i, r in enumerate(rows[:k]):
        out.append({"rank": i + 1, "code": r["symbol"], "name": r["corpName"], "combinedRank": (i + 1) * 2,
                    "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.3, "earningsYield": 0.1,
                    "marketCap": r["marketCap"], "price": r["price"], "EBIT": 100, "enterpriseValue": 1000,
                    "capitalBase": 500, "cashAndCashEquivalents": 50, "totalLiabilities": 200,
                    "currentAssets": 300, "currentLiabilities": 100, "propertyPlantAndEquipment": 300,
                    "evMethod": "x", "dataSource": "test"})
    return out, {}, {"dartCoverage": 1.0}


def _canon_sha():
    p = C.CANONICAL_PATH
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


def t1_non_trading_self_skip():
    r = S.run_signal(NON_TRADING, now=f"{NON_TRADING}T16:00:00+09:00")
    assert r["status"] == "SELF_SKIPPED_NON_TRADING_DAY", r
    assert r["noFakeTrade"] is True


def t2_market_not_closed_blocked():
    r = S.run_signal(TRADING, now=BEFORE, build_payload_fn=fake_universe, ranking_fn=fake_ranking)
    assert r["status"] == "BLOCKED", r
    assert r["blockedCode"] == "BLOCKED_MARKET_NOT_CLOSED"
    assert r["canonicalChanged"] is False and r["productionWriteCount"] == 0


def t3_ready_writes_temp_only():
    d = tempfile.mkdtemp()
    try:
        before = _canon_sha()
        r = S.run_signal(TRADING, now=AFTER, output_dir=str(Path(d) / TRADING),
                         build_payload_fn=fake_universe, ranking_fn=fake_ranking)
        assert r["status"] == "READY", r
        assert r["productionWriteCount"] == 0 and r["publicCopyCount"] == 0
        assert r["nextExecutionDateCandidate"] and r["nextExecutionDateCandidate"] > TRADING
        assert (Path(d) / TRADING / "manifest.json").exists()
        assert _canon_sha() == before, "canonical 불변"
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t4_already_prepared():
    d = tempfile.mkdtemp()
    try:
        out = str(Path(d) / TRADING)
        a = S.run_signal(TRADING, now=AFTER, output_dir=out, build_payload_fn=fake_universe, ranking_fn=fake_ranking)
        assert a["status"] == "READY", a
        b = S.run_signal(TRADING, now=AFTER, output_dir=out, build_payload_fn=fake_universe, ranking_fn=fake_ranking)
        assert b["status"] == "ALREADY_PREPARED", b
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t5_package_conflict_blocked():
    d = tempfile.mkdtemp()
    try:
        out = str(Path(d) / TRADING)
        S.run_signal(TRADING, now=AFTER, output_dir=out, build_payload_fn=fake_universe, ranking_fn=fake_ranking)
        r = S.run_signal(TRADING, now=AFTER, output_dir=out,
                         build_payload_fn=lambda: fake_universe(price=2000.0), ranking_fn=fake_ranking)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_PACKAGE_CONFLICT", r
    finally:
        shutil.rmtree(d, ignore_errors=True)


TESTS = [
    ("1  비거래일 self-skip", t1_non_trading_self_skip),
    ("2  장마감 전 BLOCKED_MARKET_NOT_CLOSED", t2_market_not_closed_blocked),
    ("3  READY는 TEMP만 생성·canonical 불변", t3_ready_writes_temp_only),
    ("4  동일 재실행 ALREADY_PREPARED", t4_already_prepared),
    ("5  해시 충돌 BLOCKED_PACKAGE_CONFLICT", t5_package_conflict_blocked),
]


def main():
    p = f = 0
    for name, fn in TESTS:
        try:
            fn(); print(f"[PASS] {name}"); p += 1
        except AssertionError as e:
            print(f"[FAIL] {name} -> {e}"); f += 1
        except Exception as e:  # noqa: BLE001
            import traceback; print(f"[ERROR] {name} -> {type(e).__name__}: {e}"); traceback.print_exc(); f += 1
    print(f"\n결과: {p} passed, {f} failed (총 {len(TESTS)})")
    return 0 if f == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
