"""test_shadow_portfolio — Shadow Portfolio freeze/계산기 검증 (Phase MF-TTM-SHADOW-PORTFOLIO-POC).

선행: python scripts/poc/build_shadow_freeze.py
실행: python scripts/poc/test_shadow_portfolio.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import shadow_portfolio as SP

FREEZE = HERE / "fixtures" / "shadow-freeze-latest.json"
_P = _F = 0


def check(name, cond, detail=""):
    global _P, _F
    if cond:
        _P += 1; print(f"  PASS  {name}")
    else:
        _F += 1; print(f"  FAIL  {name}  {detail}")


def main():
    fz = json.loads(FREEZE.read_text(encoding="utf-8"))
    print("=== test_shadow_portfolio ===")

    # freeze 계약
    check("전략 A/B 각 top30", len(fz["strategies"]["annual"]["holdings"]) == 30 and len(fz["strategies"]["ttm"]["holdings"]) == 30)
    check("동일가중: 각 종목 invested <= 100만", all(h["investedAmount"] <= 1_000_000 for h in fz["strategies"]["ttm"]["holdings"]))
    check("정수 수량", all(isinstance(h["shares"], int) for h in fz["strategies"]["annual"]["holdings"]))
    # 잔여현금 = 초기자본 - 투자금액
    a = fz["strategies"]["annual"]
    inv = sum(h["investedAmount"] for h in a["holdings"])
    check("잔여현금 = 초기 - 투자", abs((a["initialCapital"] - inv) - a["cash"]) < 1)
    # WARNING/BLOCKED 미포함(TTM)
    check("TTM top30 WARNING/BLOCKED 미포함", fz["warningBlockedExcludedFromTtm"]["ttmTop30HasNonPass"] == 0)
    check("annual/TTM 완전 분리(strategyType)", a["strategyType"] == "OFFICIAL_ANNUAL" and fz["strategies"]["ttm"]["strategyType"] == "TTM_EXPERIMENT")
    check("교집합+전용 = 각 30 구성", fz["overlap"]["commonCount"] + len(fz["overlap"]["annualOnly"]) == 30)
    check("disclaimer 실주문 없음 명시", "실주문 없음" in fz["disclaimer"])

    # compute_snapshot: entryPrice 그대로면 수익률 0
    strat = fz["strategies"]["ttm"]
    pm0 = {h["stockCode"]: h["entryPrice"] for h in strat["holdings"]}
    s0 = SP.compute_snapshot(strat, pm0, "2026-07-10")
    check("entryPrice 스냅샷 → 총자산≈초기자본", abs(s0["totalAsset"] - strat["initialCapital"]) < 1, str(s0["totalAsset"]))
    check("entryPrice 스냅샷 → 누적수익 0", abs(s0["cumulativeReturn"]) < 1e-6)

    # +10% 가격 → holdings 시장가치 상승
    pm1 = {c: p * 1.1 for c, p in pm0.items()}
    s1 = SP.compute_snapshot(strat, pm1, "2026-07-17")
    check("가격 +10% → 총자산 증가", s1["totalAsset"] > s0["totalAsset"])

    # 가격 누락 → 임의 보정 없이 기록
    pm_miss = dict(list(pm0.items())[:-5])  # 5종 누락
    sm = SP.compute_snapshot(strat, pm_miss, "2026-07-24")
    check("가격 누락 → missing 기록", sm["missingPriceCount"] == 5 and sm["sourceStatus"] == "partial")

    # performance: 3 스냅샷 → MDD/vol
    perf = SP.performance([s0, s1, sm])
    check("performance MDD/vol 산출", perf["snapshots"] == 3 and perf["turnover"] == 0.0)

    # 멱등성
    ex = [{"snapshotDate": "2026-07-17", "strategyType": "TTM_EXPERIMENT"}]
    check("멱등: 동일일자 중복 감지", SP.is_duplicate_snapshot(ex, "2026-07-17", "TTM_EXPERIMENT"))
    check("멱등: 다른 일자는 신규", not SP.is_duplicate_snapshot(ex, "2026-07-24", "TTM_EXPERIMENT"))

    # 거래정지 flag
    sh = SP.compute_snapshot(strat, pm0, "2026-07-10", halted_codes={strat["holdings"][0]["stockCode"]})
    check("거래정지 종목 flag", sh["haltedCount"] == 1)

    print(f"\n=== {_P} passed, {_F} failed ===")
    return 0 if _F == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
