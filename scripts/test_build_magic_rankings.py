#!/usr/bin/env python3
"""build_magic_rankings 테스트 (Phase MF-RANKING-DATA-1).

fixture rankings.json으로 magicOfficialRankings 파생을 검증한다. 파일 write 0.
운영 signal(2026-07-08)로 read-only smoke도 수행(존재할 때만).
실행:  python scripts/test_build_magic_rankings.py
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

import build_magic_rankings as R


def _item(i, *, vr, pr, cr, ey, roc, full=True):
    """TOP_FIELDS + signalClosePrice 형태의 signal top100 원소 fixture."""
    d = {
        "rank": i, "combinedRank": cr, "profitabilityRank": pr, "valueRank": vr,
        "returnOnCapital": roc, "earningsYield": ey, "code": f"{i:06d}", "name": f"S{i}",
        "marketCap": 100.0 * i, "signalClosePrice": 1000.0 + i, "evMethod": "marketCap_plus_totalLiabilities_minus_cash",
        "dataSource": "DART 2025 CFS",
    }
    if full:
        d.update({"EBIT": 1_000_000.0 * i, "enterpriseValue": 5_000_000.0 * i,
                  "capitalBase": 2_000_000.0 * i, "cashAndCashEquivalents": 400_000.0 * i,
                  "totalLiabilities": 1_200_000.0 * i, "currentAssets": 3_000_000.0 * i,
                  "currentLiabilities": 1_500_000.0 * i, "propertyPlantAndEquipment": 800_000.0 * i})
    return d


def mk_rankings(top100):
    return {"schemaVersion": "signal-package-v1", "formulaVersion": "book-faithful-v1-test",
            "formulaMode": "book_faithful_v1", "signalAsOfDate": "2026-07-08",
            "rankingGeneratedAt": "2026-07-08T15:40:00+09:00", "rankingCount": len(top100),
            "top10": top100[:10], "top100": top100}


def _write(doc):
    d = tempfile.mkdtemp()
    p = Path(d) / "rankings.json"
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return p, d


# 5개 종목: valueRank/profitabilityRank/combinedRank가 서로 다르게 섞이도록 구성.
FIX = [
    _item(1, vr=1, pr=5, cr=6, ey=0.50, roc=0.30),
    _item(2, vr=4, pr=1, cr=5, ey=0.20, roc=0.90),
    _item(3, vr=2, pr=2, cr=4, ey=0.40, roc=0.60),
    _item(4, vr=5, pr=4, cr=9, ey=0.10, roc=0.35),
    _item(5, vr=3, pr=3, cr=6, ey=0.30, roc=0.50),
]


def t1_counts_and_limit():
    # 120개 → cheap/quality 100 제한, combined 10 제한
    big = [_item(i, vr=i, pr=121 - i, cr=i + 1, ey=1.0 / i, roc=1.0 / i) for i in range(1, 121)]
    p, d = _write(mk_rankings(big))
    try:
        out = R.build_magic_rankings(p)
        assert len(out["cheapTop100"]) == 100, len(out["cheapTop100"])
        assert len(out["qualityTop100"]) == 100, len(out["qualityTop100"])
        assert len(out["combinedTop10"]) == 10, len(out["combinedTop10"])
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t2_cheap_sorted_by_valuerank():
    p, d = _write(mk_rankings(FIX))
    try:
        cheap = R.build_magic_rankings(p)["cheapTop100"]
        assert [c["cheapRank"] for c in cheap] == [1, 2, 3, 4, 5], [c["cheapRank"] for c in cheap]
        assert cheap[0]["code"] == "000001"  # valueRank=1
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t3_quality_sorted_by_qualityrank():
    p, d = _write(mk_rankings(FIX))
    try:
        q = R.build_magic_rankings(p)["qualityTop100"]
        assert [x["qualityRank"] for x in q] == [1, 2, 3, 4, 5], [x["qualityRank"] for x in q]
        assert q[0]["code"] == "000002"  # profitabilityRank=1
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t4_combined_sorted_by_magicscore():
    p, d = _write(mk_rankings(FIX))
    try:
        c = R.build_magic_rankings(p)["combinedTop10"]
        scores = [x["magicScore"] for x in c]
        assert scores == sorted(scores), scores
        assert c[0]["magicScore"] == 4 and c[0]["code"] == "000003"  # combinedRank=4
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t5_name_mapping_and_derived():
    p, d = _write(mk_rankings(FIX))
    try:
        cheap = R.build_magic_rankings(p)["cheapTop100"]
        it = next(c for c in cheap if c["code"] == "000001")
        # 명칭 매핑
        assert it["cheapRank"] == 1 and it["qualityRank"] == 5 and it["magicScore"] == 6
        assert it["finalRank"] == 1
        assert it["investedCapital"] == 2_000_000.0 * 1  # capitalBase
        assert it["closePrice"] == 1001.0  # signalClosePrice
        assert it["ebit"] == 1_000_000.0 and it["enterpriseValue"] == 5_000_000.0
        # 파생값
        assert it["netWorkingCapital"] == 3_000_000.0 - 1_500_000.0
        assert it["netDebtApprox"] == 1_200_000.0 - 400_000.0
        # dataSource 파싱
        assert it["financialStatementYear"] == 2025 and it["dartFsDiv"] == "CFS"
        assert it["evMethod"] == "marketCap_plus_totalLiabilities_minus_cash"
        assert it["priceAsOfDate"] == "2026-07-08"
        assert it["evidenceCompleteness"] is True
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t6_incomplete_evidence_null_safe():
    # 원값 없는 종목 → evidenceCompleteness=false, 파생값 null, 크래시 없음
    partial = [_item(1, vr=1, pr=1, cr=2, ey=0.5, roc=0.5, full=False)]
    p, d = _write(mk_rankings(partial))
    try:
        it = R.build_magic_rankings(p)["cheapTop100"][0]
        assert it["evidenceCompleteness"] is False
        assert it["netWorkingCapital"] is None and it["netDebtApprox"] is None
        assert it["ebit"] is None and it["cheapRank"] == 1  # 순위는 유지
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t7_top_meta_fields():
    p, d = _write(mk_rankings(FIX))
    try:
        out = R.build_magic_rankings(p)
        assert out["schemaVersion"] == "magic-official-rankings-v1"
        assert out["dataDate"] == "2026-07-08" and out["signalAsOfDate"] == "2026-07-08"
        assert out["formulaVersion"] == "book-faithful-v1-test"
        assert out["evMethod"] == "marketCap_plus_totalLiabilities_minus_cash"
        assert out["rankingCount"] == 5
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t8_empty_top100_safe():
    p, d = _write(mk_rankings([]))
    try:
        out = R.build_magic_rankings(p)
        assert out["cheapTop100"] == [] and out["qualityTop100"] == [] and out["combinedTop10"] == []
        assert out["evMethod"] is None
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t9_real_signal_smoke():
    # 운영 최신 signal이 있으면 read-only smoke(있을 때만; 없으면 skip PASS)
    base = Path(os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-signal"))
    cands = sorted(base.glob("*/rankings.json")) if base.exists() else []
    if not cands:
        return
    out = R.build_magic_rankings(cands[-1])
    assert 0 < len(out["cheapTop100"]) <= 100
    assert len(out["combinedTop10"]) <= 10
    assert [c["cheapRank"] for c in out["cheapTop100"]] == sorted(c["cheapRank"] for c in out["cheapTop100"])


TESTS = [
    ("1  counts/limit(120→100/100/10)", t1_counts_and_limit),
    ("2  cheapTop100 valueRank 오름차순", t2_cheap_sorted_by_valuerank),
    ("3  qualityTop100 profitabilityRank 오름차순", t3_quality_sorted_by_qualityrank),
    ("4  combinedTop10 magicScore 오름차순", t4_combined_sorted_by_magicscore),
    ("5  명칭 매핑+파생값 정확", t5_name_mapping_and_derived),
    ("6  원값 불완전→completeness false·null 안전", t6_incomplete_evidence_null_safe),
    ("7  top-level 메타 필드", t7_top_meta_fields),
    ("8  빈 top100 안전", t8_empty_top100_safe),
    ("9  운영 signal read-only smoke", t9_real_signal_smoke),
]


def main():
    passed, failed = 0, 0
    for name, fn in TESTS:
        try:
            fn(); print(f"[PASS] {name}"); passed += 1
        except AssertionError as e:
            print(f"[FAIL] {name}  -> {e}"); failed += 1
        except Exception as e:  # noqa: BLE001
            import traceback
            print(f"[ERROR] {name}  -> {type(e).__name__}: {e}")
            traceback.print_exc(); failed += 1
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(TESTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
