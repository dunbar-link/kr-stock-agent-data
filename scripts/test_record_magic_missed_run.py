#!/usr/bin/env python3
"""record_magic_missed_run + 거래일 index 모델 테스트 (Phase 45-E10).

in-memory + 임시경로 + pykrx mock. production/REPO1 read-only(불변 검증). 실행: python scripts/test_record_magic_missed_run.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
from datetime import date as _date, timedelta
from pathlib import Path

import magic_rolling_engine as E
import record_magic_missed_run as R

CODES = [f"{i:06d}" for i in range(1, 11)]
CODES_ALT = [f"{i:06d}" for i in range(11, 21)]
PX = {c: 1000.0 for c in CODES + CODES_ALT}


def mk_ranking(codes):
    return [{"code": c, "name": f"S{c}", "rank": i + 1, "combinedRank": (i + 1) * 2,
             "profitabilityRank": i + 1, "valueRank": i + 1, "returnOnCapital": 0.5, "earningsYield": 0.2}
            for i, c in enumerate(codes)]


def gen_days(n, start=(2026, 1, 2)):
    out, d = [], _date(*start)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


class FakeStock:
    def __init__(self, days):
        self._days = days
    def get_business_days(self, y, m):
        return [d for d in self._days if d.startswith(f"{y:04d}-{m:02d}")]


def build_day1_state(date="2026-06-17"):
    cal = E.make_calendar([date])
    st = E.empty_official_state()
    timing = {"signalAsOfDate": "2026-06-16", "rankingGeneratedAt": "2026-06-16T18:00:00+09:00",
              "executionDate": date, "executionMarketOpenAt": f"{date}T09:00:00+09:00",
              "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    st, _ = E.plan_official_day(st, date, mk_ranking(CODES), PX, PX, cal, now=f"{date}T18:00:00+09:00", timing=timing)
    return st


def legacy_canonical():
    """E10 이전 canonical 모사: 거래일 index 필드 제거."""
    st = build_day1_state()
    for k in ("officialTradingDayIndex", "officialKrxTradingCalendar", "officialExecutionCalendar"):
        st.pop(k, None)
    for it in st["batches"] + st["itemLots"]:
        for k in ("buyTradingDayIndex", "plannedSellTradingDayIndex"):
            it.pop(k, None)
    return st


def write_state(d, state):
    p = Path(d) / "magic-formula-official-state.json"
    p.write_text(R.A.canonical_bytes(state).decode("utf-8"), encoding="utf-8")
    return p


def _hash(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except (FileNotFoundError, OSError):
        return None


# ----- 거래일 index 모델 (engine) -----

def t1_migrate_additive():
    m = E.migrate_official_state_indices(legacy_canonical())
    assert m["officialKrxTradingCalendar"] == ["2026-06-17"]
    assert m["officialExecutionCalendar"] == ["2026-06-17"]
    assert m["officialTradingDayIndex"] == 1
    # 보호 필드 보존
    leg = legacy_canonical()
    assert m["officialSequence"] == leg["officialSequence"] == 1
    assert len(m["batches"]) == 1 and len(m["itemLots"]) == 10 and len(m["buyLedger"]) == 10


def t2_batch_buy_index_1():
    m = E.migrate_official_state_indices(legacy_canonical())
    assert m["batches"][0]["buyTradingDayIndex"] == 1
    assert all(l["buyTradingDayIndex"] == 1 for l in m["itemLots"])


def t3_planned_sell_index_51():
    m = E.migrate_official_state_indices(legacy_canonical())
    assert m["batches"][0]["plannedSellTradingDayIndex"] == 51
    assert all(l["plannedSellTradingDayIndex"] == 51 for l in m["itemLots"])


def t4_missed_increments_trading_day_index_only():
    st, entry, already = E.apply_missed_run(legacy_canonical(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    assert already is False
    assert st["officialTradingDayIndex"] == 2
    assert st["officialKrxTradingCalendar"] == ["2026-06-17", "2026-06-18"]
    assert st["officialExecutionCalendar"] == ["2026-06-17"]   # 성공일 불변


def t5_missed_does_not_increment_sequence():
    st, _, _ = E.apply_missed_run(legacy_canonical(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    assert st["officialSequence"] == 1


def t6_missed_creates_no_trades():
    before = E.migrate_official_state_indices(legacy_canonical())
    st, entry, _ = E.apply_missed_run(legacy_canonical(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    assert len(st["batches"]) == len(before["batches"]) == 1
    assert len(st["itemLots"]) == len(before["itemLots"]) == 10
    assert len(st["buyLedger"]) == len(before["buyLedger"]) == 10
    assert len(st["sellLedger"]) == 0
    assert st["officialAvailableCash"] == before["officialAvailableCash"]
    assert entry["syntheticTradesCreated"] is False and entry["batchCreated"] is False
    assert entry["lookAheadTradePrevented"] is True


def t7_next_normal_batch_indices():
    # 06-18 누락 후 다음 정상 거래일(index 3): sequence=2 / buyIndex=3 / sellIndex=53
    st, _, _ = E.apply_missed_run(legacy_canonical(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    cal = E.make_calendar(["2026-06-17", "2026-06-18", "2026-06-19"])
    st2, res = E.plan_official_day(st, "2026-06-19", mk_ranking(CODES_ALT), PX, PX, cal,
                                   now="2026-06-19T18:00:00+09:00", trading_day_index=3)
    assert res["runStatus"] == E.COMPLETED
    assert st2["officialSequence"] == 2
    nb = st2["batches"][-1]
    assert nb["buyTradingDayIndex"] == 3 and nb["plannedSellTradingDayIndex"] == 53


def t8_first_batch_due_at_index_51_after_gap():
    st = E.migrate_official_state_indices(legacy_canonical())   # batch1 buyIndex1 sellIndex51
    due = E.due_official_batches(st, 51)
    assert len(due) == 1 and due[0]["batchId"] == "MF-BATCH-2026-06-17"
    assert E.due_official_batches(st, 50) == []   # index50엔 미도래


def t9_due_by_index_not_batch_count():
    st = E.migrate_official_state_indices(legacy_canonical())   # open batch 1개뿐(50 미만)
    assert len(st["batches"]) == 1
    assert len(E.due_official_batches(st, 51)) == 1   # batch 수 무관, index51이면 due


def t10_same_code_lots_separate():
    st = build_day1_state()
    cal = E.make_calendar(["2026-06-17", "2026-06-18"])
    st2, _ = E.plan_official_day(st, "2026-06-18", mk_ranking(CODES), PX, PX, cal,
                                 now="2026-06-18T18:00:00+09:00", trading_day_index=2)
    lots_046940 = [l for l in st2["itemLots"] if l["code"] == "000001"]
    assert len(lots_046940) == 2 and lots_046940[0]["lotId"] != lots_046940[1]["lotId"]
    assert lots_046940[0]["buyTradingDayIndex"] != lots_046940[1]["buyTradingDayIndex"]


def t11_due_one_atomic_sell10_buy10():
    st = build_day1_state()   # batch1 sellIndex51
    cal = E.make_calendar(["2026-06-17", "2026-09-01"])
    st2, res = E.plan_official_day(st, "2026-09-01", mk_ranking(CODES_ALT), PX, PX, cal,
                                   now="2026-09-01T18:00:00+09:00", trading_day_index=51)
    assert res["runStatus"] == E.COMPLETED
    assert res["sellCount"] == 10 and res["buyCount"] == 10
    assert len([b for b in st2["batches"] if b["status"] == "OPEN"]) == 1   # 1 close + 1 open


def t12_due_two_blocked():
    # 2거래일 COMPLETED → open batch 2개(buyIndex1,2). index52면 둘 다 due → BLOCKED.
    st = build_day1_state()
    cal = E.make_calendar(["2026-06-17", "2026-06-18"])
    st, _ = E.plan_official_day(st, "2026-06-18", mk_ranking(CODES_ALT), PX, PX, cal,
                               now="2026-06-18T18:00:00+09:00", trading_day_index=2)
    assert len([b for b in st["batches"] if b["status"] == "OPEN"]) == 2
    cal2 = E.make_calendar(["2026-06-17", "2026-06-18", "2026-09-02"])
    st2, res = E.plan_official_day(st, "2026-09-02", mk_ranking(CODES), PX, PX, cal2,
                                   now="2026-09-02T18:00:00+09:00", trading_day_index=52)
    assert res["runStatus"] == E.BLOCKED_MULTIPLE_OVERDUE_BATCHES, res["runStatus"]
    assert res["buyCount"] == 0 and res["sellCount"] == 0
    assert st2["officialSequence"] == 2   # 변경 없음(2일까지만 성공)


# ----- recorder (파일/검증/idempotency/백업) -----

def t13_unverified_trading_day_no_change():
    root, d = Path(tempfile.mkdtemp()), None
    try:
        sp = write_state(root, legacy_canonical())
        before = _hash(sp)
        res = R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                                  state_path=sp, snapshot_dir=root / "snaps",
                                  pykrx_stock=FakeStock([]))   # 거래일 미검증
        assert res["status"] == R.BLOCKED_TRADING_DAY_UNVERIFIED, res["status"]
        assert _hash(sp) == before   # 변경 0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t14_idempotent_reapply():
    root = Path(tempfile.mkdtemp())
    fk = FakeStock(["2026-06-17", "2026-06-18"])
    try:
        sp = write_state(root, legacy_canonical())
        snaps = root / "snaps"
        a = R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                                state_path=sp, snapshot_dir=snaps, receipt_path=root / "r.json", pykrx_stock=fk)
        assert a["status"] == R.RECORDED, a["status"]
        h1, m1 = _hash(sp), _hash(snaps / "2026-06-18.json")
        b = R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                                state_path=sp, snapshot_dir=snaps, receipt_path=root / "r.json", pykrx_stock=fk)
        assert b["status"] == R.ALREADY_RECORDED, b["status"]
        assert _hash(sp) == h1 and _hash(snaps / "2026-06-18.json") == m1
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t15_invariants_preserved():
    before = E.migrate_official_state_indices(legacy_canonical())
    st, _, _ = E.apply_missed_run(legacy_canonical(), "2026-06-18", now="2026-06-18T08:00:00+09:00")
    ok, bad = R.check_invariants(before, st)
    assert ok, bad


def t16_existing_0617_snapshot_unchanged():
    root = Path(tempfile.mkdtemp())
    fk = FakeStock(["2026-06-17", "2026-06-18"])
    try:
        sp = write_state(root, legacy_canonical())
        snaps = root / "snaps"; snaps.mkdir(parents=True, exist_ok=True)
        (snaps / "2026-06-17.json").write_text('{"keep":"2026-06-17"}', encoding="utf-8")
        h0 = _hash(snaps / "2026-06-17.json")
        R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                            state_path=sp, snapshot_dir=snaps, receipt_path=root / "r.json", pykrx_stock=fk)
        assert _hash(snaps / "2026-06-17.json") == h0   # 06-17 snapshot 불변
        assert (snaps / "2026-06-18.json").exists()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t17_0618_snapshot_lastknown_separated():
    root = Path(tempfile.mkdtemp())
    fk = FakeStock(["2026-06-17", "2026-06-18"])
    try:
        sp = write_state(root, legacy_canonical())
        snaps = root / "snaps"
        R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                            state_path=sp, snapshot_dir=snaps, receipt_path=root / "r.json", pykrx_stock=fk)
        snap = json.loads((snaps / "2026-06-18.json").read_text(encoding="utf-8"))
        assert snap["status"] == "MISSED_RUN" and snap["BUY"] == 0 and snap["SELL"] == 0
        assert snap["officialTradingDayIndex"] == 2 and snap["officialSequence"] == 1
        assert snap["valuationUpdated"] is False
        assert snap["lastKnownTotalAsset"] == 50000000.0
        assert snap["lastKnownTotalAssetAsOf"] == "2026-06-17"
        assert snap["lastCompletedTradingDate"] == "2026-06-17"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t18_pilot_isolated():
    pilot = {"pilotBatchId": "MF-PILOT-2026-06-08", "operationMode": "PILOT", "itemLotCount": 10,
             "totalInvested": 889840.0, "officialCapitalImpact": 0}
    can = legacy_canonical(); can["pilot"] = copy.deepcopy(pilot)
    st, _, _ = E.apply_missed_run(can, "2026-06-18", now="2026-06-18T08:00:00+09:00")
    assert st["pilot"] == pilot   # PILOT 불변·격리


def t19_repo1_public_unchanged():
    pub = Path("C:/work/kr-stock-agent") / "public" / "data" / "recommendation-history.json"
    before = _hash(pub)
    root = Path(tempfile.mkdtemp())
    try:
        sp = write_state(root, legacy_canonical())
        R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                            state_path=sp, snapshot_dir=root / "snaps", receipt_path=root / "r.json",
                            pykrx_stock=FakeStock(["2026-06-17", "2026-06-18"]))  # backup_root None
    finally:
        shutil.rmtree(root, ignore_errors=True)
    assert _hash(pub) == before   # REPO1 public 불변


def t20_onedrive_backup_sha_match():
    root = Path(tempfile.mkdtemp())
    onedrive = Path(tempfile.mkdtemp())
    fk = FakeStock(["2026-06-17", "2026-06-18"])
    try:
        sp = write_state(root, legacy_canonical())
        res = R.record_missed_run("2026-06-18", do_apply=True, confirm="RECORD_MISSED_RUN_2026-06-18",
                                  state_path=sp, snapshot_dir=root / "snaps", receipt_path=root / "evid" / "receipt.json",
                                  backup_root=onedrive, pykrx_stock=fk)
        assert res["status"] == R.RECORDED, res["status"]
        bk = res["backup"]
        assert bk["status"] == "BACKED_UP" and bk["allShaMatch"] is True
        final = Path(bk["finalPath"])
        assert _hash(final / "canonical/magic-formula-official-state.json") == _hash(sp)
        assert (final / "backup-manifest.json").exists()
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(onedrive, ignore_errors=True)


TESTS = [
    ("1  migrate additive(index 필드 보강)", t1_migrate_additive),
    ("2  06-17 batch buyTradingDayIndex=1", t2_batch_buy_index_1),
    ("3  plannedSellTradingDayIndex=51", t3_planned_sell_index_51),
    ("4  MISSED_RUN이 거래일 index만 +1", t4_missed_increments_trading_day_index_only),
    ("5  MISSED_RUN이 officialSequence 미증가", t5_missed_does_not_increment_sequence),
    ("6  MISSED_RUN 거래·batch 생성 0", t6_missed_creates_no_trades),
    ("7  다음 정상 batch seq2/buyIdx3/sellIdx53", t7_next_normal_batch_indices),
    ("8  누락 후 첫 batch index51 due", t8_first_batch_due_at_index_51_after_gap),
    ("9  batch 수 무관 index51 due", t9_due_by_index_not_batch_count),
    ("10 동일 종목 lot 분리", t10_same_code_lots_separate),
    ("11 due 1개 atomic SELL10/BUY10", t11_due_one_atomic_sell10_buy10),
    ("12 due 2개 이상 BLOCKED", t12_due_two_blocked),
    ("13 거래일 미검증→변경0", t13_unverified_trading_day_no_change),
    ("14 동일 재실행 idempotent", t14_idempotent_reapply),
    ("15 canonical 불변 필드 보존", t15_invariants_preserved),
    ("16 2026-06-17 snapshot 불변", t16_existing_0617_snapshot_unchanged),
    ("17 2026-06-18 snapshot lastKnown 분리", t17_0618_snapshot_lastknown_separated),
    ("18 PILOT 격리", t18_pilot_isolated),
    ("19 REPO1 public 불변", t19_repo1_public_unchanged),
    ("20 OneDrive 백업 SHA 일치", t20_onedrive_backup_sha_match),
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
