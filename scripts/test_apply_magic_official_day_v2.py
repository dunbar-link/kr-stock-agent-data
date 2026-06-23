#!/usr/bin/env python3
"""apply_magic_official_day day-N(≥2) append 테스트 (Phase 45-E13).

전부 in-memory fixture + 임시경로. production JSON·REPO1 public 미접근.
직전 canonical(day1 + 06-18 MISSED_RUN)을 엔진으로 합성하고, day2(06-19) batch를
append 저장하는 경로를 검증한다. day-1 회귀는 test_apply_magic_official_day.py가 담당.
실행:  python scripts/test_apply_magic_official_day_v2.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import apply_magic_official_day as A
import build_magic_official_public as P
import magic_rolling_engine as E

CREATED_AT = "2026-06-21T16:30:00+09:00"
CODES = [f"{i:06d}" for i in range(1, 11)]
D1_OPENS = {c: 1000.0 + i * 10 for i, c in enumerate(CODES, 1)}   # day1 시가
D2_OPENS = {c: 1100.0 + i * 13 for i, c in enumerate(CODES, 1)}   # day2 시가(다름→batch1 재평가)
CONFIRM = "APPLY_OFFICIAL_DAY_2026-06-19"


def _ranking(codes):
    return [{"code": c, "name": f"S{c}", "rank": i, "combinedRank": i * 2,
             "profitabilityRank": i, "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2}
            for i, c in enumerate(codes, 1)]


def _enrich_all_daily(st):
    reserve = round(sum(b.get("cashReserve") or 0 for b in st["batches"]
                        if b.get("operationMode") == "OFFICIAL" and b.get("status") == "OPEN"), 2)
    out = []
    for d in st["dailyLedger"]:
        d = dict(d)
        ds = d["date"]
        dbuys = [e for e in st["buyLedger"] if e.get("date") == ds]
        dsells = [e for e in st["sellLedger"] if e.get("date") == ds]
        d.update({
            "totalBuyAmount": round(sum(float(e["amount"]) for e in dbuys), 2),
            "totalSellAmount": round(sum(float(e["amount"]) for e in dsells), 2),
            "realizedProfit": round(sum(float(e.get("realizedProfit") or 0) for e in dsells), 2),
            "officialAvailableCash": st["officialAvailableCash"],
            "batchCashReserveTotal": reserve,
            "totalCash": round(st["officialAvailableCash"] + reserve, 2),
        })
        out.append(d)
    return out


def build_prior_canonical():
    """day1(06-17 batch1) + 06-18 MISSED_RUN 상태의 canonical dict."""
    st = E.empty_official_state()
    cal1 = E.make_calendar(["2026-06-16", "2026-06-17"])
    timing1 = {"signalAsOfDate": "2026-06-16", "rankingGeneratedAt": "2026-06-16T18:00:00+09:00",
               "executionDate": "2026-06-17", "executionMarketOpenAt": "2026-06-17T09:00:00+09:00",
               "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True}
    st, _ = E.plan_official_day(st, "2026-06-17", _ranking(CODES), D1_OPENS, D1_OPENS, cal1,
                                now="2026-06-17T18:00:00+09:00", timing=timing1, trading_day_index=1)
    st, _, _ = E.apply_missed_run(st, "2026-06-18", now="2026-06-18T08:00:00+09:00")
    pss = {b["batchId"]: b["plannedSellSequence"] for b in st["batches"]}
    for l in st["itemLots"]:
        l["plannedSellSequence"] = pss[l["batchId"]]
    return {
        "schemaVersion": A.STATE_SCHEMA_VERSION, "formulaVersion": "book-faithful-v1-test",
        "operationMode": "OFFICIAL", "officialStartDate": st["officialStartDate"],
        "initialCapital": st["initialCapital"], "initialBatchCapital": st["initialBatchCapital"],
        "officialSequence": st["officialSequence"], "officialTradingDayIndex": st["officialTradingDayIndex"],
        "officialTradingCalendar": st["officialTradingCalendar"],
        "officialKrxTradingCalendar": st["officialKrxTradingCalendar"],
        "officialExecutionCalendar": st["officialExecutionCalendar"],
        "officialAvailableCash": st["officialAvailableCash"],
        "batches": st["batches"], "itemLots": st["itemLots"],
        "buyLedger": st["buyLedger"], "sellLedger": st["sellLedger"],
        "dailyLedger": _enrich_all_daily(st),
        "evaluationSnapshots": st["evaluationSnapshots"], "missedRuns": st["missedRuns"],
        "pilot": None, "prevTotalAsset": st["prevTotalAsset"], "updatedAt": "2026-06-18T08:00:00+09:00",
    }


def mk_v2_receipt(prior_bytes, *, pkg_dir=None, created_at=CREATED_AT):
    qty_map, total_invested = E.allocate_quantities([{"code": c} for c in CODES], D2_OPENS, 1_000_000.0)
    approved = []
    for i, c in enumerate(CODES, 1):
        op, q = D2_OPENS[c], qty_map[c]
        approved.append({"rank": i, "code": c, "name": f"S{c}", "openPrice": op, "quantity": int(q),
                         "amount": round(op * q, 2), "combinedRank": i * 2, "profitabilityRank": i,
                         "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2,
                         "signalClosePrice": None, "marketCap": None})
    cash_reserve = round(1_000_000.0 - total_invested, 2)
    r = {
        "schemaVersion": A.RECEIPT_V2_SCHEMA_VERSION, "receiptType": "OFFICIAL_EXECUTION_RECEIPT",
        "signalPackagePath": str(pkg_dir or "pkg"), "sourceSignalPackagePath": str(pkg_dir or "pkg"),
        "signalAsOfDate": "2026-06-18", "rankingGeneratedAt": "2026-06-18T18:00:00+09:00",
        "executionDate": "2026-06-19", "executionMarketOpenAt": "2026-06-19T09:00:00+09:00",
        "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True,
        "formulaVersion": "book-faithful-v1-test",
        "batchId": "MF-BATCH-2026-06-19", "proposedBatchId": "MF-BATCH-2026-06-19",
        "sequence": 2, "proposedSequence": 2, "buyTradingDayIndex": 3, "plannedSellTradingDayIndex": 53,
        "buyCount": 10, "sellCount": 0, "allocatedCapital": 1_000_000.0,
        "totalInvested": total_invested, "cashReserve": cash_reserve,
        "officialAvailableCashBefore": 49_000_000.0, "officialAvailableCashAfter": 48_000_000.0,
        "canonicalBeforeSha256": hashlib.sha256(prior_bytes).hexdigest(),
        "approvedTop10": approved, "executionPrices": approved,
        "dryRunLogPath": "test.log", "sourceDryRunLog": "test.log", "dryRunLogSha256": "0" * 64,
        "sourceDryRunExitCode": 0, "productionWriteCountAtDryRun": 0,
        "officialStartDatePersistedAtDryRun": False, "createdAt": created_at,
    }
    if pkg_dir:
        r["signalPackageManifestSha256"] = A._sha_file(Path(pkg_dir) / "manifest.json")
        r["rankingsSha256"] = A._sha_file(Path(pkg_dir) / "rankings.json")
        r["universeSha256"] = A._sha_file(Path(pkg_dir) / "universe.json")
    else:
        r["signalPackageManifestSha256"] = "0" * 64
        r["rankingsSha256"] = "0" * 64
        r["universeSha256"] = "0" * 64
    r["receiptSha256"] = A.receipt_self_sha(r)
    return r


def mk_pkg(d):
    d = Path(d); d.mkdir(parents=True, exist_ok=True)
    (d / "manifest.json").write_text(json.dumps({"signalAsOfDate": "2026-06-18"}), encoding="utf-8")
    (d / "rankings.json").write_text(json.dumps({"top10": "fix"}), encoding="utf-8")
    (d / "universe.json").write_text(json.dumps({"u": 1}), encoding="utf-8")
    return d


def _hash(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except (FileNotFoundError, OSError):
        return None


def _setup(root, *, prior=None):
    state = root / "magic-formula-official-state.json"
    snaps = root / "snaps"
    snaps.mkdir(parents=True, exist_ok=True)
    canon = prior if prior is not None else build_prior_canonical()
    data = A.canonical_bytes(canon)
    state.write_bytes(data)
    return state, snaps, data


def _tmp():
    d = tempfile.mkdtemp()
    return Path(d), d


# ===== 테스트 =====

def t1_append_apply_ok():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r = mk_v2_receipt(prior)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.APPLIED_APPEND, res
        assert res["realOrderCount"] == 0 and res["productionWriteCount"] == 0
        assert (snaps / "2026-06-19.json").exists()
        assert res["officialSequence"] == 2 and res["officialTradingDayIndex"] == 3
        assert res["batchId"] == "MF-BATCH-2026-06-19"
        assert res["totalInvested"] == r["totalInvested"] and res["cashReserve"] == r["cashReserve"]
        assert res["officialAvailableCash"] == 48_000_000.0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t2_seq_and_index_and_calendars():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        assert st["officialSequence"] == 2, st["officialSequence"]
        assert st["officialTradingDayIndex"] == 3, st["officialTradingDayIndex"]
        assert st["officialExecutionCalendar"][-1] == "2026-06-19"
        assert st["officialExecutionCalendar"] == ["2026-06-17", "2026-06-19"]
        assert st["officialKrxTradingCalendar"][-1] == "2026-06-19"
        assert st["officialKrxTradingCalendar"] == ["2026-06-17", "2026-06-18", "2026-06-19"]
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t3_batch_counts_and_buy20_sell0():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        assert len(st["batches"]) == 2 and len(st["itemLots"]) == 20
        assert len(st["buyLedger"]) == 20 and len(st["sellLedger"]) == 0
        b2 = next(b for b in st["batches"] if b["batchId"] == "MF-BATCH-2026-06-19")
        assert b2["buyTradingDayIndex"] == 3 and b2["plannedSellTradingDayIndex"] == 53
        assert b2["status"] == "OPEN" and b2["sequence"] == 2
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t4_batch1_open_preserved():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        prior_obj = json.loads(prior.decode("utf-8"))
        b1_before = next(b for b in prior_obj["batches"] if b["batchId"] == "MF-BATCH-2026-06-17")
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        b1_after = next(b for b in st["batches"] if b["batchId"] == "MF-BATCH-2026-06-17")
        assert b1_after == b1_before, "batch1 변경됨"
        assert b1_after["status"] == "OPEN" and b1_after["plannedSellTradingDayIndex"] == 51
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t5_missed_run_preserved():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        prior_obj = json.loads(prior.decode("utf-8"))
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        assert st["missedRuns"] == prior_obj["missedRuns"], "MISSED_RUN 변경됨"
        assert len(st["missedRuns"]) == 1 and st["missedRuns"][0]["date"] == "2026-06-18"
        assert st["missedRuns"][0]["status"] == "MISSED_RUN"
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t6_day1_ledger_snapshot_preserved():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        prior_obj = json.loads(prior.decode("utf-8"))
        d1_before = next(x for x in prior_obj["dailyLedger"] if x["date"] == "2026-06-17")
        bl1_before = [e for e in prior_obj["buyLedger"] if e["date"] == "2026-06-17"]
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        d1_after = next(x for x in st["dailyLedger"] if x["date"] == "2026-06-17")
        bl1_after = [e for e in st["buyLedger"] if e["date"] == "2026-06-17"]
        assert d1_after == d1_before, "06-17 dailyLedger 변경됨"
        assert bl1_after == bl1_before, "06-17 buyLedger 변경됨"
        assert len(st["dailyLedger"]) == 2
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t7_eval_snapshots_standard_key_no_dup():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        st = json.loads(state.read_text(encoding="utf-8"))
        assert "evaluationSnapshots" in st and "evalSnapshots" not in st
        assert len(st["evaluationSnapshots"]) == 2
        assert [e["date"] for e in st["evaluationSnapshots"]] == ["2026-06-17", "2026-06-19"]
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t8_snapshot_content():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r = mk_v2_receipt(prior)
        A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        snap = json.loads((snaps / "2026-06-19.json").read_text(encoding="utf-8"))
        assert snap["status"] == "COMPLETED" and snap["tradeCreated"] is True
        assert snap["buyCount"] == 10 and snap["sellCount"] == 0
        assert snap["officialSequence"] == 2 and snap["officialTradingDayIndex"] == 3
        assert snap["batchId"] == "MF-BATCH-2026-06-19"
        assert A._approx(snap["totalBuyAmount"], r["totalInvested"]) and snap["totalSellAmount"] == 0
        assert snap["realizedProfit"] == 0
        assert snap["executionReceiptSha256"] == r["receiptSha256"]
        assert snap["canonicalStateSha256"] == _hash(state)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t9_idempotent_reapply():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r = mk_v2_receipt(prior)
        a = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert a["status"] == A.APPLIED_APPEND
        h1, h2 = _hash(state), _hash(snaps / "2026-06-19.json")
        b = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert b["status"] == A.ALREADY_PROCESSED, b
        assert _hash(state) == h1 and _hash(snaps / "2026-06-19.json") == h2
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t10_canonical_before_mismatch_blocked():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r = mk_v2_receipt(prior)
        # receipt 생성 후 canonical을 변경 → before SHA 불일치
        tampered = json.loads(prior.decode("utf-8"))
        tampered["officialAvailableCash"] = 12345.0
        state.write_bytes(A.canonical_bytes(tampered))
        h0 = _hash(state)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.BLOCKED_CANONICAL_BEFORE_MISMATCH, res
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t11_receipt_plan_mismatch_blocked():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r = mk_v2_receipt(prior)
        # 승인값을 conservation 유지하며 위조: 한 종목 qty+1, amount/총계 동반 조정 → 엔진 재계산과 불일치
        a0 = r["approvedTop10"][0]
        op = a0["openPrice"]
        a0["quantity"] += 1
        a0["amount"] = round(op * a0["quantity"], 2)
        r["totalInvested"] = round(r["totalInvested"] + op, 2)
        r["cashReserve"] = round(1_000_000.0 - r["totalInvested"], 2)
        r["receiptSha256"] = A.receipt_self_sha(r)
        h0 = _hash(state)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.BLOCKED_RECEIPT_PLAN_MISMATCH, res
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t12_signal_package_sha_mismatch_blocked():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        pkg = mk_pkg(root / "pkg")
        r = mk_v2_receipt(prior, pkg_dir=pkg)
        (pkg / "manifest.json").write_text(json.dumps({"tampered": True}), encoding="utf-8")
        h0 = _hash(state)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state,
                                      snapshot_dir=snaps, signal_pkg_dir=pkg)
        assert res["status"] == A.BLOCKED_SIGNAL_PACKAGE_MISMATCH, res
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t13_confirm_required_blocked():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        h0 = _hash(state)
        res = A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm="WRONG",
                                      state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.BLOCKED_APPLY_CONFIRMATION_REQUIRED, res
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t14_dry_run_no_write():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        h0 = _hash(state)
        res = A.apply_official_append(mk_v2_receipt(prior), do_apply=False,
                                      state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.DRY_RUN_OK, res
        assert res["officialSequence"] == 2 and res["officialAvailableCash"] == 48_000_000.0
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t15_public_mapper_reads_day2():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        A.apply_official_append(mk_v2_receipt(prior), do_apply=True, confirm=CONFIRM,
                                state_path=state, snapshot_dir=snaps)
        pub = P.build_magic_official_public(state_path=state)   # validate_canonical 포함, read-only
        assert pub["magicOfficialSummary"]["officialSequence"] == 2, pub["magicOfficialSummary"]
        assert len(pub["magicOfficialTradeDays"]) >= 1
        assert len(pub["magicOfficialPortfolio"]["holdings"]) >= 1
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t16_input_canonical_not_mutated_on_block():
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        before = copy.deepcopy(json.loads(prior.decode("utf-8")))
        r = mk_v2_receipt(prior)
        r["sequence"] = 1   # v2 append은 seq>=2 요구 → BLOCKED
        r["receiptSha256"] = A.receipt_self_sha(r)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["blocked"] is True
        assert json.loads(state.read_text(encoding="utf-8")) == before
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _shifted_receipt(prior, *, with_eval=True):
    """day-N: TOP10=000002..000011(000001 탈락·000011 신규), 보유=000001..000010(batch1).
    union=000001..000011. evalPrices 포함 여부로 보유-비TOP10(000001) 평가 가능성 검증."""
    top = [f"{i:06d}" for i in range(2, 12)]
    union = [f"{i:06d}" for i in range(1, 12)]
    px = {f"{i:06d}": 1200.0 + i * 11 for i in range(1, 12)}
    qty_map, total_invested = E.allocate_quantities([{"code": c} for c in top], px, 1_000_000.0)
    approved = [{"rank": i, "code": c, "name": f"S{c}", "openPrice": px[c], "quantity": int(qty_map[c]),
                 "amount": round(px[c] * qty_map[c], 2), "combinedRank": i * 2, "profitabilityRank": i,
                 "valueRank": i, "returnOnCapital": 0.5, "earningsYield": 0.2,
                 "signalClosePrice": None, "marketCap": None} for i, c in enumerate(top, 1)]
    r = {
        "schemaVersion": A.RECEIPT_V2_SCHEMA_VERSION, "receiptType": "OFFICIAL_EXECUTION_RECEIPT",
        "signalAsOfDate": "2026-06-18", "rankingGeneratedAt": "2026-06-18T18:00:00+09:00",
        "executionDate": "2026-06-19", "executionMarketOpenAt": "2026-06-19T09:00:00+09:00",
        "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True,
        "formulaVersion": "book-faithful-v1-test",
        "batchId": "MF-BATCH-2026-06-19", "proposedBatchId": "MF-BATCH-2026-06-19",
        "sequence": 2, "proposedSequence": 2, "buyTradingDayIndex": 3, "plannedSellTradingDayIndex": 53,
        "buyCount": 10, "sellCount": 0, "allocatedCapital": 1_000_000.0,
        "totalInvested": total_invested, "cashReserve": round(1_000_000.0 - total_invested, 2),
        "officialAvailableCashBefore": 49_000_000.0, "officialAvailableCashAfter": 48_000_000.0,
        "canonicalBeforeSha256": hashlib.sha256(prior).hexdigest(),
        "approvedTop10": approved, "executionPrices": approved,
        "holdingsMarketValuePreview": None, "missingEvalCodes": [],
        "signalPackageManifestSha256": "0" * 64, "rankingsSha256": "0" * 64, "universeSha256": "0" * 64,
        "dryRunLogPath": "t.log", "sourceDryRunLog": "t.log", "dryRunLogSha256": "0" * 64,
        "sourceDryRunExitCode": 0, "productionWriteCountAtDryRun": 0,
        "officialStartDatePersistedAtDryRun": False, "createdAt": CREATED_AT,
    }
    if with_eval:
        r["evalPrices"] = [{"code": c, "openPrice": px[c]} for c in union]   # 보유∪TOP10
    r["receiptSha256"] = A.receipt_self_sha(r)
    return r, px


def t17_eval_union_values_held_non_top10():
    """evalPrices(union)로 보유-비TOP10(000001)이 평가됨 → missingEvalCodes=[], 신규 000011 매수."""
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        r, px = _shifted_receipt(prior, with_eval=True)
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.APPLIED_APPEND, res
        st = json.loads(state.read_text(encoding="utf-8"))
        b2 = {e["code"] for e in st["buyLedger"] if e["batchId"] == "MF-BATCH-2026-06-19"}
        assert "000011" in b2 and "000001" not in b2          # 신규 매수 O, 탈락종목 매수 X
        ev = st["evaluationSnapshots"][-1]
        assert ev["missingEvalCodes"] == []                    # 보유 전체 평가됨
        held = [p for p in ev["perLot"] if p["code"] == "000001"]
        assert held and held[0]["currentPrice"] == px["000001"] and held[0]["marketValue"] > 0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t18_no_eval_prices_held_non_top10_blocked():
    """evalPrices 없으면 보유-비TOP10(000001)이 평가 누락→eng missingEvalCodes≠receipt[] →BLOCKED(변경 0)."""
    root, d = _tmp()
    try:
        state, snaps, prior = _setup(root)
        h0 = _hash(state)
        r, _ = _shifted_receipt(prior, with_eval=False)        # evalPrices 누락 → TOP10만 평가
        res = A.apply_official_append(r, do_apply=True, confirm=CONFIRM, state_path=state, snapshot_dir=snaps)
        assert res["status"] == A.BLOCKED_RECEIPT_PLAN_MISMATCH, res
        assert _hash(state) == h0 and not (snaps / "2026-06-19.json").exists()
    finally:
        shutil.rmtree(d, ignore_errors=True)


TESTS = [
    ("1  day2 append 정상(seq2/batch2/idx3)", t1_append_apply_ok),
    ("2  officialSequence/index/달력 append", t2_seq_and_index_and_calendars),
    ("3  batch2/lot20/BUY20/SELL0/buyIdx3/sellIdx53", t3_batch_counts_and_buy20_sell0),
    ("4  batch1 OPEN 불변", t4_batch1_open_preserved),
    ("5  06-18 MISSED_RUN 보존", t5_missed_run_preserved),
    ("6  06-17 dailyLedger/buyLedger 불변", t6_day1_ledger_snapshot_preserved),
    ("7  evaluationSnapshots 표준키/중복 미생성", t7_eval_snapshots_standard_key_no_dup),
    ("8  2026-06-19 snapshot 내용", t8_snapshot_content),
    ("9  동일 재적용→ALREADY_PROCESSED(불변)", t9_idempotent_reapply),
    ("10 canonicalBeforeSha 불일치→BLOCKED(불변)", t10_canonical_before_mismatch_blocked),
    ("11 receipt≠plan 재계산→BLOCKED(불변)", t11_receipt_plan_mismatch_blocked),
    ("12 signal package SHA 불일치→BLOCKED(불변)", t12_signal_package_sha_mismatch_blocked),
    ("13 confirm 누락→BLOCKED(불변)", t13_confirm_required_blocked),
    ("14 dry-run(do_apply=False)→저장0", t14_dry_run_no_write),
    ("15 public mapper가 batch2 장부 읽음", t15_public_mapper_reads_day2),
    ("16 BLOCKED 시 canonical 불변", t16_input_canonical_not_mutated_on_block),
    ("17 evalPrices union→보유-비TOP10 평가", t17_eval_union_values_held_non_top10),
    ("18 evalPrices 누락+보유-비TOP10→BLOCKED", t18_no_eval_prices_held_non_top10_blocked),
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
            traceback.print_exc()
            failed += 1
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(TESTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
