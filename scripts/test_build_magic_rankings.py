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


def mk_rankings_global(top100, value_src, profit_src, *, eligible=1316):
    """신규 signal(전체 universe 기준 valueTop100/profitabilityTop100 포함) fixture."""
    d = mk_rankings(top100)
    d.update({"globalRankingSchemaVersion": "global-rankings-v1",
              "rankingScope": "global-eligible-universe", "eligibleCount": eligible,
              "valueTop100": value_src, "profitabilityTop100": profit_src})
    return d


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


# ===== MF-GLOBAL-RANKINGS-PIPELINE: 전체 universe 기준 global 순위 =====

# 전체 universe 기준 원천 fixture: universe 순위에 gap이 있어도 표시 순위는 1..N 연속이어야 함.
# value_src: valueRank(=cheapRank 매핑) 이 [1,5,8,10,12] (gappy) 이지만 이미 정렬됨(signal이 정렬해 저장).
GVAL = [
    _item(11, vr=1, pr=40, cr=41, ey=0.90, roc=0.10),
    _item(12, vr=5, pr=33, cr=38, ey=0.80, roc=0.12),
    _item(13, vr=8, pr=27, cr=35, ey=0.70, roc=0.15),
    _item(14, vr=10, pr=22, cr=32, ey=0.60, roc=0.18),
    _item(15, vr=12, pr=18, cr=30, ey=0.55, roc=0.20),
]
# profit_src: profitabilityRank(=qualityRank 매핑) 이 [1,3,6,9,11] (gappy), 종목 집합도 value와 다름.
GPROF = [
    _item(21, vr=60, pr=1, cr=61, ey=0.05, roc=9.9),
    _item(22, vr=52, pr=3, cr=55, ey=0.06, roc=7.7),
    _item(23, vr=44, pr=6, cr=50, ey=0.07, roc=5.5),
    _item(24, vr=39, pr=9, cr=48, ey=0.08, roc=4.4),
    _item(25, vr=31, pr=11, cr=42, ey=0.09, roc=3.3),
]


def t10_global_cheap_continuous():
    p, d = _write(mk_rankings_global(FIX, GVAL, GPROF))
    try:
        out = R.build_magic_rankings(p)
        assert out["rankingScope"] == "global-eligible-universe", out["rankingScope"]
        cheap = out["cheapTop100"]
        # 표시 순위는 gap 없이 1..5 연속
        assert [c["cheapRank"] for c in cheap] == [1, 2, 3, 4, 5], [c["cheapRank"] for c in cheap]
        # 원래 universe 순위는 별도 필드로 보존(gappy 원값)
        assert [c["universeCheapRank"] for c in cheap] == [1, 5, 8, 10, 12], [c["universeCheapRank"] for c in cheap]
        # value 원천의 종목 순서 유지
        assert cheap[0]["code"] == "000011" and cheap[-1]["code"] == "000015"
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t11_global_quality_continuous_and_independent():
    p, d = _write(mk_rankings_global(FIX, GVAL, GPROF))
    try:
        out = R.build_magic_rankings(p)
        q = out["qualityTop100"]
        assert [x["qualityRank"] for x in q] == [1, 2, 3, 4, 5], [x["qualityRank"] for x in q]
        assert [x["universeQualityRank"] for x in q] == [1, 3, 6, 9, 11], [x["universeQualityRank"] for x in q]
        # 두 목록은 독립: 종목 집합이 달라야 함(value=11~15, quality=21~25)
        cheap_codes = {c["code"] for c in out["cheapTop100"]}
        quality_codes = {x["code"] for x in q}
        assert cheap_codes.isdisjoint(quality_codes), (cheap_codes, quality_codes)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t12_global_combined_regression():
    # global 배열이 있어도 combinedTop10은 top100(combined 후보)에서 생성 — FIX 기준 회귀 없음
    p, d = _write(mk_rankings_global(FIX, GVAL, GPROF))
    try:
        out = R.build_magic_rankings(p)
        c = out["combinedTop10"]
        scores = [x["magicScore"] for x in c]
        assert scores == sorted(scores), scores
        assert c[0]["magicScore"] == 4 and c[0]["code"] == "000003"  # FIX combinedRank=4
        assert out["eligibleCount"] == 1316
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t13_subset_fallback_scope():
    # 구형 signal(valueTop100/profitabilityTop100 없음) → subset scope, 기존 동작 유지
    p, d = _write(mk_rankings(FIX))
    try:
        out = R.build_magic_rankings(p)
        assert out["rankingScope"] == "combined-subset", out["rankingScope"]
        assert [c["cheapRank"] for c in out["cheapTop100"]] == [1, 2, 3, 4, 5]
        # subset 경로에는 universe 보존 필드가 없다
        assert "universeCheapRank" not in out["cheapTop100"][0]
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t14_global_limit_100_continuous():
    # 150개 global 원천 → 100개로 제한, 표시 순위 1..100 정확히 연속
    big_val = [_item(i, vr=i * 2, pr=300 - i, cr=i, ey=1.0 / i, roc=1.0 / i) for i in range(1, 151)]
    big_prof = [_item(1000 + i, vr=300 - i, pr=i * 3, cr=i, ey=1.0 / i, roc=1.0 / i) for i in range(1, 151)]
    p, d = _write(mk_rankings_global(FIX, big_val, big_prof))
    try:
        out = R.build_magic_rankings(p)
        assert len(out["cheapTop100"]) == 100 and len(out["qualityTop100"]) == 100
        assert [c["cheapRank"] for c in out["cheapTop100"]] == list(range(1, 101))
        assert [x["qualityRank"] for x in out["qualityTop100"]] == list(range(1, 101))
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t15_global_deterministic_repeat():
    # 동일 입력 반복 실행 → 동일 결과(JSON 직렬화 동일)
    p, d = _write(mk_rankings_global(FIX, GVAL, GPROF))
    try:
        a = json.dumps(R.build_magic_rankings(p), ensure_ascii=False, sort_keys=True)
        b = json.dumps(R.build_magic_rankings(p), ensure_ascii=False, sort_keys=True)
        assert a == b
    finally:
        shutil.rmtree(d, ignore_errors=True)


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
    ("10 global cheapTop100 표시 1..N 연속+universe 보존", t10_global_cheap_continuous),
    ("11 global qualityTop100 연속+두 목록 독립", t11_global_quality_continuous_and_independent),
    ("12 global 있어도 combinedTop10 회귀 없음", t12_global_combined_regression),
    ("13 구형 signal→subset fallback scope", t13_subset_fallback_scope),
    ("14 global 150→100 제한 1..100 연속", t14_global_limit_100_continuous),
    ("15 global 동일 입력 반복 결정론", t15_global_deterministic_repeat),
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
