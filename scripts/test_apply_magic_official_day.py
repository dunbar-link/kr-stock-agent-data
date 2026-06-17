#!/usr/bin/env python3
"""apply_magic_official_day 테스트 (Phase 45-E7).

전부 in-memory fixture + 임시경로. 실제 production JSON·REPO1 public은 읽기만(불변 검증 포함).
실행:  python scripts/test_apply_magic_official_day.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import apply_magic_official_day as A

CREATED_AT = "2026-06-17T15:40:00+09:00"
# (rank, code, name, open, qty, amount, comb, prof, val, roc, ey)
APPROVED = [
    (1, "046940", "우원개발", 3465.0, 28, 97020.0, 12, 11, 1, 0.8446, 0.4977),
    (2, "461300", "아이스크림미디어", 17650.0, 5, 88250.0, 23, 18, 5, 0.6073, 0.3109),
    (3, "088130", "동아엘텍", 6900.0, 14, 96600.0, 28, 22, 6, 0.5721, 0.2528),
    (4, "184230", "SGA솔루션즈", 2110.0, 47, 99170.0, 35, 3, 32, 1.6459, 0.1281),
    (5, "124500", "아이티센글로벌", 34200.0, 2, 68400.0, 41, 13, 28, 0.736, 0.1386),
    (6, "053580", "웹케시", 6780.0, 14, 94920.0, 56, 34, 22, 0.4989, 0.1496),
    (7, "171090", "선익시스템", 75700.0, 1, 75700.0, 63, 12, 51, 0.8163, 0.1144),
    (8, "018290", "브이티", 12780.0, 7, 89460.0, 78, 31, 47, 0.51, 0.1165),
    (9, "052400", "코나아이", 47600.0, 2, 95200.0, 83, 39, 44, 0.4566, 0.1185),
    (10, "215200", "메가스터디교육", 41950.0, 2, 83900.0, 98, 19, 79, 0.607, 0.0969),
]


def approved_top10():
    out = []
    for (rk, code, name, op, qty, amt, comb, prof, val, roc, ey) in APPROVED:
        out.append({"rank": rk, "code": code, "name": name, "openPrice": op, "quantity": qty,
                    "amount": amt, "combinedRank": comb, "profitabilityRank": prof, "valueRank": val,
                    "returnOnCapital": roc, "earningsYield": ey, "signalClosePrice": None, "marketCap": None})
    return out


def mk_signal_pkg(d):
    """가짜 신호 패키지(SHA 검증용). 내용은 고정, SHA만 의미."""
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    (d / "manifest.json").write_text(json.dumps({"signalAsOfDate": "2026-06-16",
        "rankingGeneratedAt": "2026-06-16T17:40:35.151708+09:00",
        "formulaVersion": "book-faithful-v1-2026-43B5"}, ensure_ascii=False), encoding="utf-8")
    (d / "rankings.json").write_text(json.dumps({"top10": "fixture"}, ensure_ascii=False), encoding="utf-8")
    return d


def mk_receipt(pkg_dir, *, created_at=CREATED_AT, source_log="E6.1.log"):
    r = {
        "schemaVersion": A.RECEIPT_SCHEMA_VERSION,
        "signalPackagePath": str(pkg_dir),
        "signalPackageManifestSha256": A._sha_file(Path(pkg_dir) / "manifest.json"),
        "rankingsSha256": A._sha_file(Path(pkg_dir) / "rankings.json"),
        "signalAsOfDate": "2026-06-16",
        "rankingGeneratedAt": "2026-06-16T17:40:35.151708+09:00",
        "executionDate": "2026-06-17",
        "executionMarketOpenAt": "2026-06-17T09:00:00+09:00",
        "executionPriceSource": "pykrx_open",
        "lookAheadValidationPassed": True,
        "formulaVersion": "book-faithful-v1-2026-43B5",
        "batchId": "MF-BATCH-2026-06-17",
        "sequence": 1,
        "allocatedCapital": 1000000.0,
        "totalInvested": 888620.0,
        "cashReserve": 111380.0,
        "approvedTop10": approved_top10(),
        "sourceDryRunLog": source_log,
        "sourceDryRunExitCode": 0,
        "createdAt": created_at,
    }
    r["receiptSha256"] = A.receipt_self_sha(r)
    return r


def mk_pilot_lots(n=10, invested=88984.0):
    return [{"lotId": f"MF-2026-06-08-{c}-{i:02d}", "code": c, "name": f"P{c}", "buyDate": "2026-06-08",
             "buyOpenPrice": 3100.0, "priceSource": "pykrx_open", "quantity": 32,
             "investedAmount": invested, "rank": i}
            for i, c in enumerate([f"{j:06d}" for j in range(1, n + 1)], 1)]


def _hash(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except (FileNotFoundError, OSError):
        return None


def _tmp():
    d = tempfile.mkdtemp()
    return Path(d), d


def _apply(receipt, *, do_apply=True, confirm="APPLY_OFFICIAL_DAY_2026-06-17", pkg=None,
           state=None, snaps=None, pilot=None):
    return A.apply_official_day(receipt, pilot if pilot is not None else mk_pilot_lots(),
                                do_apply=do_apply, confirm=confirm,
                                state_path=state, snapshot_dir=snaps, signal_pkg_dir=pkg)


# ----- 테스트 -----

def t1_first_apply_ok():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        r = mk_receipt(pkg)
        res = _apply(r, pkg=pkg, state=root / "state.json", snaps=root / "snaps")
        assert res["status"] == A.APPLIED, res
        assert res["officialStartDate"] == "2026-06-17" and res["officialSequence"] == 1
        assert res["buyCount"] == 10 and res["sellCount"] == 0 and res["itemLotCount"] == 10
        assert res["batchId"] == "MF-BATCH-2026-06-17"
        assert (root / "state.json").exists() and (root / "snaps" / "2026-06-17.json").exists()
        assert res["realOrderCount"] == 0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t2_amount_conservation():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        res = _apply(mk_receipt(pkg), pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["allocatedCapital"] == 1000000.0
        assert res["totalInvested"] == 888620.0 and res["cashReserve"] == 111380.0
        assert round(res["totalInvested"] + res["cashReserve"], 2) == 1000000.0
        assert res["officialAvailableCash"] == 49000000.0
        assert res["totalCash"] == 49111380.0
        assert res["holdingsMarketValue"] == 888620.0 and res["totalAsset"] == 50000000.0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t3_approved_prices_quantities_unchanged():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        _apply(mk_receipt(pkg), pkg=pkg, state=root / "s.json", snaps=root / "sn")
        st = json.loads((root / "s.json").read_text(encoding="utf-8"))
        bl = {e["code"]: e for e in st["buyLedger"]}
        for (rk, code, name, op, qty, amt, *_rest) in APPROVED:
            e = bl[code]
            assert e["executionPrice"] == op and e["quantity"] == qty and e["amount"] == amt, code
            assert e["priceSource"] == "pykrx_open" and e["executionDate"] == "2026-06-17"
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t4_receipt_sha_mismatch_blocked():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        r = mk_receipt(pkg)
        r["totalInvested"] = 999999.0           # 위변조(receiptSha256 재계산 안 함)
        res = _apply(r, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["status"] == A.BLOCKED_EXECUTION_RECEIPT_MISMATCH, res
        assert not (root / "s.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t5_signal_package_sha_mismatch_blocked():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        r = mk_receipt(pkg)
        (pkg / "manifest.json").write_text(json.dumps({"tampered": True}), encoding="utf-8")  # 패키지 변조
        res = _apply(r, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["status"] == A.BLOCKED_SIGNAL_PACKAGE_MISMATCH, res
        assert not (root / "s.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t6_confirm_required_blocked():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        res = _apply(mk_receipt(pkg), confirm="WRONG", pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["status"] == A.BLOCKED_APPLY_CONFIRMATION_REQUIRED, res
        assert not (root / "s.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t7_idempotent_reapply():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        r = mk_receipt(pkg)
        a = _apply(r, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert a["status"] == A.APPLIED
        h1, h2 = _hash(root / "s.json"), _hash(root / "sn" / "2026-06-17.json")
        b = _apply(r, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert b["status"] == A.ALREADY_PROCESSED, b
        assert _hash(root / "s.json") == h1 and _hash(root / "sn" / "2026-06-17.json") == h2
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t8_same_date_different_receipt_conflict():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        r1 = mk_receipt(pkg)
        _apply(r1, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        h1 = _hash(root / "sn" / "2026-06-17.json")
        # canonical 동일·snapshot만 다른 receipt(sourceDryRunLog만 변경) → SNAPSHOT_CONFLICT
        r2 = mk_receipt(pkg, source_log="OTHER.log")
        res = _apply(r2, pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["status"] in (A.BLOCKED_SNAPSHOT_CONFLICT, A.BLOCKED_OFFICIAL_STATE_CONFLICT), res
        assert _hash(root / "sn" / "2026-06-17.json") == h1, "기존 snapshot 불변"
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t9_pilot_isolated_from_official():
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        _apply(mk_receipt(pkg), pkg=pkg, state=root / "s.json", snaps=root / "sn")
        st = json.loads((root / "s.json").read_text(encoding="utf-8"))
        p = st["pilot"]
        assert p["itemLotCount"] == 10 and p["totalInvested"] == 889840.0
        assert p["officialCapitalImpact"] == 0 and p["officialSequenceImpact"] == 0
        assert p["timingAudit"]["auditedSignalAsOfDate"] == "2026-05-29"
        assert p["timingAudit"]["timingAuditStatus"] == "PASS_NO_LOOKAHEAD"
        # 공식 수치는 PILOT 미포함
        assert st["initialCapital"] == 50000000 and st["officialAvailableCash"] == 49000000.0
        assert st["officialSequence"] == 1 and len(st["itemLots"]) == 10   # 공식 lot만(PILOT 제외)
        assert st["dailyLedger"][0]["totalAsset"] == 50000000.0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t10_atomic_write_failure_preserves():
    root, d = _tmp()
    orig = A.os.replace
    def boom(a, b):
        raise OSError("injected replace failure")
    A.os.replace = boom
    try:
        pkg = mk_signal_pkg(root / "pkg")
        res = _apply(mk_receipt(pkg), pkg=pkg, state=root / "s.json", snaps=root / "sn")
        assert res["status"] == A.BLOCKED_ATOMIC_WRITE_FAILED, res
        assert not (root / "s.json").exists(), "반쪽 canonical 금지"
        assert not (root / "s.json.tmp").exists(), "임시파일 정리"
    finally:
        A.os.replace = orig
        shutil.rmtree(d, ignore_errors=True)


def _prod_invariance(paths):
    before = {str(p): _hash(p) for p in paths}
    root, d = _tmp()
    try:
        pkg = mk_signal_pkg(root / "pkg")
        _apply(mk_receipt(pkg), pkg=pkg, state=root / "s.json", snaps=root / "sn")
    finally:
        shutil.rmtree(d, ignore_errors=True)
    after = {str(p): _hash(p) for p in paths}
    return before == after


def t11_production_repo2_unchanged():
    assert _prod_invariance([A.ROOT / "magic-formula-portfolio.json",
                             A.ROOT / "magic-formula-rankings.json",
                             A.ROOT / "recommendation-history.json"])


def t12_financial_universe_unchanged():
    assert _prod_invariance([A.ROOT / "financial-universe-real.json"])


def t13_repo1_public_unchanged():
    assert _prod_invariance([Path("C:/work/kr-stock-agent") / "public" / "data" / "recommendation-history.json"])


TESTS = [
    ("1  정상 최초 적용(start/seq1/batch1/lot10/BUY10/SELL0)", t1_first_apply_ok),
    ("2  금액 보존(888620+111380=1M, 49M+...=50M)", t2_amount_conservation),
    ("3  승인 가격·수량 불변", t3_approved_prices_quantities_unchanged),
    ("4  receipt SHA 불일치→BLOCKED", t4_receipt_sha_mismatch_blocked),
    ("5  signal package SHA 불일치→BLOCKED", t5_signal_package_sha_mismatch_blocked),
    ("6  confirm 누락→BLOCKED", t6_confirm_required_blocked),
    ("7  동일 재실행→ALREADY_PROCESSED(불변)", t7_idempotent_reapply),
    ("8  같은 날짜 다른 receipt→CONFLICT(불변)", t8_same_date_different_receipt_conflict),
    ("9  PILOT 공식자금 격리", t9_pilot_isolated_from_official),
    ("10 atomic 중간 실패→기존 보존", t10_atomic_write_failure_preserves),
    ("11 production REPO2 JSON 불변", t11_production_repo2_unchanged),
    ("12 financial-universe-real.json 불변", t12_financial_universe_unchanged),
    ("13 REPO1 public 불변", t13_repo1_public_unchanged),
]


def main():
    passed, failed = 0, 0
    for name, fn in TESTS:
        try:
            fn()
            print(f"[PASS] {name}")
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {name}  -> {e}")
            failed += 1
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {name}  -> {type(e).__name__}: {e}")
            failed += 1
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(TESTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
