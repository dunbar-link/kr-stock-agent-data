#!/usr/bin/env python3
"""magic_publish_rankings 테스트 (Phase MF-RANKING-PUBLISH-BUILD).

fixture REPO1 doc(tmp) + fixture signal rankings로 plan/apply 게이트를 검증한다.
운영 REPO1 public 미접근(전부 tmp). git_status_fn 주입으로 clean/dirty 제어.
실행:  python scripts/test_magic_publish_rankings.py
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import magic_publish_rankings as PR
import build_magic_rankings as BR


def _rank_item(i, *, vr, pr, cr):
    return {
        "rank": i, "combinedRank": cr, "profitabilityRank": pr, "valueRank": vr,
        "returnOnCapital": 0.5, "earningsYield": 0.3, "code": f"{i:06d}", "name": f"S{i}",
        "marketCap": 100.0 * i, "signalClosePrice": 1000.0 + i,
        "evMethod": "marketCap_plus_totalLiabilities_minus_cash", "dataSource": "DART 2025 CFS",
        "EBIT": 1_000_000.0 * i, "enterpriseValue": 5_000_000.0 * i, "capitalBase": 2_000_000.0 * i,
        "cashAndCashEquivalents": 400_000.0 * i, "totalLiabilities": 1_200_000.0 * i,
        "currentAssets": 3_000_000.0 * i, "currentLiabilities": 1_500_000.0 * i,
        "propertyPlantAndEquipment": 800_000.0 * i,
    }


TOP = [_rank_item(1, vr=1, pr=2, cr=3), _rank_item(2, vr=2, pr=1, cr=3), _rank_item(3, vr=3, pr=3, cr=6)]


def mk_signal(d):
    doc = {"schemaVersion": "signal-package-v1", "formulaVersion": "book-faithful-v1-test",
           "formulaMode": "book_faithful_v1", "signalAsOfDate": "2026-07-08", "rankingCount": 3,
           "top10": TOP, "top100": TOP}
    p = Path(d) / "rankings.json"
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return p


# 기존 3키 + 비마법 키를 가진 REPO1 doc(불변 검증용).
def mk_repo1(d, *, with_rankings=None):
    doc = {
        "baseDate": "2026-07-08", "generatedAt": "2026-07-08 08:31:00",
        "magicOfficialSummary": {"officialSequence": 12, "dataDate": "2026-07-08"},
        "magicOfficialPortfolio": {"holdings": [{"code": "046940"}]},
        "magicOfficialTradeDays": [{"date": "2026-07-08", "buys": []}],
        "portfolioSummary": {"cash": 100}, "aiPortfolioSummary": {"cash": 200},
    }
    if with_rankings is not None:
        doc["magicOfficialRankings"] = with_rankings
    p = Path(d) / "recommendation-history.json"
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return p, doc


CONFIRM = "PUBLISH_MAGIC_RANKINGS_2026-07-08"
CLEAN = lambda: ""            # porcelain 빈 문자열 = clean
DIRTY = lambda: " M public/data/recommendation-history.json"


def _tmp():
    d = tempfile.mkdtemp()
    return Path(d), d


def t1_plan_write0():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, _ = mk_repo1(root)
        before = rp.read_bytes()
        r = PR.plan(sig, repo1_path=rp)
        assert r["status"] == "PLAN_OK", r
        assert r["updateNeeded"] is True and r["repo1HasKey"] is False
        assert r["cheapTop100Count"] == 3 and r["combinedTop10Count"] == 3
        assert r["confirmToken"] == CONFIRM
        assert r["productionWriteCount"] == 0 and rp.read_bytes() == before  # write 0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t2_apply_confirm_required():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, orig = mk_repo1(root)
        before = rp.read_bytes()
        r = PR.apply(sig, repo1_path=rp, confirm="", git_status_fn=CLEAN, do_write=True)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_CONFIRM_REQUIRED", r
        assert rp.read_bytes() == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t3_apply_confirm_mismatch():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, _ = mk_repo1(root)
        before = rp.read_bytes()
        r = PR.apply(sig, repo1_path=rp, confirm="WRONG", git_status_fn=CLEAN, do_write=True)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_CONFIRM_MISMATCH", r
        assert rp.read_bytes() == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t4_apply_repo1_dirty_blocked():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, _ = mk_repo1(root)
        before = rp.read_bytes()
        r = PR.apply(sig, repo1_path=rp, confirm=CONFIRM, git_status_fn=DIRTY, do_write=True)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_REPO1_NOT_CLEAN", r
        assert rp.read_bytes() == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t5_apply_verify_only_write0():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, _ = mk_repo1(root)
        before = rp.read_bytes()
        r = PR.apply(sig, repo1_path=rp, confirm=CONFIRM, git_status_fn=CLEAN, do_write=False)
        assert r["status"] == "PUBLISH_VERIFIED", r
        assert r["publicChanged"] is False and rp.read_bytes() == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t6_apply_write_adds_key_only():
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, orig = mk_repo1(root)
        r = PR.apply(sig, repo1_path=rp, confirm=CONFIRM, git_status_fn=CLEAN, do_write=True)
        assert r["status"] == "PUBLISHED" and r["publicChanged"] is True, r
        after = json.loads(rp.read_text(encoding="utf-8"))
        # magicOfficialRankings 추가됨
        assert "magicOfficialRankings" in after
        assert len(after["magicOfficialRankings"]["cheapTop100"]) == 3
        # 기존 3키 + 비마법 키 전부 불변(drift 0)
        for k in orig.keys():
            assert after[k] == orig[k], f"key {k} drifted"
        assert set(after.keys()) == set(orig.keys()) | {"magicOfficialRankings"}
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t7_already_current():
    root, d = _tmp()
    try:
        sig = mk_signal(root)
        rankings = BR.build_magic_rankings(sig)
        rp, _ = mk_repo1(root, with_rankings=rankings)
        before = rp.read_bytes()
        r = PR.apply(sig, repo1_path=rp, confirm=CONFIRM, git_status_fn=CLEAN, do_write=True)
        assert r["status"] == "ALREADY_CURRENT", r
        assert rp.read_bytes() == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t8_existing_3keys_unchanged_on_write():
    """핵심: publish 후 magicOfficial 3키가 byte-동일하게 보존되는지."""
    root, d = _tmp()
    try:
        sig = mk_signal(root); rp, orig = mk_repo1(root)
        PR.apply(sig, repo1_path=rp, confirm=CONFIRM, git_status_fn=CLEAN, do_write=True)
        after = json.loads(rp.read_text(encoding="utf-8"))
        for k in ("magicOfficialSummary", "magicOfficialPortfolio", "magicOfficialTradeDays"):
            assert after[k] == orig[k], f"{k} changed by rankings publish"
    finally:
        shutil.rmtree(d, ignore_errors=True)


TESTS = [
    ("1  plan write 0", t1_plan_write0),
    ("2  apply confirm 없음 → BLOCKED(불변)", t2_apply_confirm_required),
    ("3  apply confirm 오타 → BLOCKED(불변)", t3_apply_confirm_mismatch),
    ("4  apply REPO1 dirty → BLOCKED(불변)", t4_apply_repo1_dirty_blocked),
    ("5  apply verify-only → write 0", t5_apply_verify_only_write0),
    ("6  apply write: magicOfficialRankings만 추가·drift 0", t6_apply_write_adds_key_only),
    ("7  동일 재적용 → ALREADY_CURRENT(불변)", t7_already_current),
    ("8  기존 magic 3키 publish 후 불변", t8_existing_3keys_unchanged_on_write),
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
