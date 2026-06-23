#!/usr/bin/env python3
"""와바바 마법공식 — OFFICIAL 1일차 장부 원자적 저장 (Phase 45-E7).

검증 완료된 execution receipt(고정 입력)로 모의펀드 *내부 장부*를 최초 저장한다.
- 실제 증권 주문/계좌 연결 *절대 없음*. REPO1 public·UI 미반영. daily_run 미연결.
- 수량배분/FIFO/자금/원장 로직은 *복제하지 않고* magic_rolling_engine.plan_official_day를 호출한다.
  엔진이 만든 값이 receipt의 승인값과 다르면 BLOCKED(승인값 임의 변경·재계산 변경 금지).
- 적용은 receipt만으로 *결정적*(네트워크 재조회 없음). idempotent: 동일 receipt 재적용 → ALREADY_PROCESSED.
- PILOT(2026-06-08)은 state.pilot에 보존하되 공식 자금/시퀀스/평가에서 격리(core 불변).
- 원자적: 임시파일 → flush/fsync → atomic replace. 중간 실패 시 기존 파일 보존.
"""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import magic_rolling_engine as E
import run_magic_rolling_dry_run as W   # pilot_timing_audit 재사용

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_PATH = ROOT / "magic-formula-official-state.json"
DEFAULT_SNAPSHOT_DIR = ROOT / "data" / "magic-formula-official" / "snapshots"
LOTS_PATH = ROOT / "magic-formula-trade-lots.json"

PILOT_DATE = "2026-06-08"
PILOT_EXPECTED_LOTS = 10
PILOT_EXPECTED_INVESTED = 889_840.0
PILOT_CORE_FIELDS = ("code", "name", "lotId", "buyDate", "buyOpenPrice",
                     "priceSource", "quantity", "investedAmount", "rank")

STATE_SCHEMA_VERSION = "magic-official-state-v1"
RECEIPT_SCHEMA_VERSION = "execution-receipt-v1"
RECEIPT_V2_SCHEMA_VERSION = "execution-receipt-v2"   # 45-E13: day-N(≥2) append receipt
SNAPSHOT_SCHEMA_VERSION = "magic-official-snapshot-v1"

# 상태
APPLIED = "APPLIED_OFFICIAL_DAY"
APPLIED_APPEND = "APPLIED_OFFICIAL_APPEND"           # 45-E13: 다음 거래일 batch append 저장
DRY_RUN_OK = "DRY_RUN_OK"
ALREADY_PROCESSED = "ALREADY_PROCESSED"
BLOCKED_APPLY_CONFIRMATION_REQUIRED = "BLOCKED_APPLY_CONFIRMATION_REQUIRED"
BLOCKED_EXECUTION_RECEIPT_MISMATCH = "BLOCKED_EXECUTION_RECEIPT_MISMATCH"
BLOCKED_SIGNAL_PACKAGE_MISMATCH = "BLOCKED_SIGNAL_PACKAGE_MISMATCH"
BLOCKED_OFFICIAL_STATE_CONFLICT = "BLOCKED_OFFICIAL_STATE_CONFLICT"
BLOCKED_SNAPSHOT_CONFLICT = "BLOCKED_SNAPSHOT_CONFLICT"
BLOCKED_PILOT_SOURCE_MISMATCH = "BLOCKED_PILOT_SOURCE_MISMATCH"
BLOCKED_ATOMIC_WRITE_FAILED = "BLOCKED_ATOMIC_WRITE_FAILED"
BLOCKED_CANONICAL_BEFORE_MISMATCH = "BLOCKED_CANONICAL_BEFORE_MISMATCH"   # 45-E13: canonical 변경됨
BLOCKED_RECEIPT_PLAN_MISMATCH = "BLOCKED_RECEIPT_PLAN_MISMATCH"           # 45-E13: receipt≠재계산
BLOCKED_PRIOR_STATE_UNEXPECTED = "BLOCKED_PRIOR_STATE_UNEXPECTED"         # 45-E13: 직전 상태 불일치

_KST = timezone(timedelta(hours=9))


class ReceiptMismatch(Exception):
    pass


class CanonicalBeforeMismatch(Exception):   # 45-E13: 직전 canonical SHA 불일치
    pass


class PriorStateUnexpected(Exception):      # 45-E13: 직전 상태(seq/배치) 불일치
    pass


# ===== 직렬화/해시 유틸 =====

def canonical_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")


def _sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha_file(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _approx(a, b, eps=0.01) -> bool:
    try:
        return abs(float(a) - float(b)) <= eps
    except (TypeError, ValueError):
        return a == b


def _read_json(path, encoding="utf-8"):
    return json.loads(Path(path).read_text(encoding=encoding))


# ===== 실행 영수증 =====

def receipt_self_sha(receipt: dict) -> str:
    body = {k: v for k, v in receipt.items() if k != "receiptSha256"}
    return _sha_bytes(canonical_bytes(body))


def build_execution_receipt(signal_pkg_dir, dry_run_log_path, *, created_at: str) -> dict:
    """v2 신호 패키지 + E6.1 검증 로그에서 승인값을 고정한 execution receipt를 만든다(네트워크 0)."""
    pkg = Path(signal_pkg_dir)
    man = _read_json(pkg / "manifest.json")
    rk = _read_json(pkg / "rankings.json")
    txt = Path(dry_run_log_path).read_text(encoding="utf-16", errors="replace")
    a = txt.index("RESULT_JSON_BEGIN"); a = txt.index("{", a)
    b = txt.rindex("RESULT_JSON_END"); b = txt.rindex("}", a, b) + 1
    res = json.loads(txt[a:b])
    exit_code = 0 if "EXIT_CODE=0" in txt else -1

    rk_by_code = {str(t["code"]): t for t in rk["top10"]}
    approved = []
    for p in res["selectedTop10"]:
        t = rk_by_code[str(p["code"])]
        approved.append({
            "rank": t["rank"], "code": str(p["code"]), "name": p["name"],
            "openPrice": float(p["openPrice"]), "quantity": int(p["quantity"]),
            "amount": float(p["amount"]),
            "combinedRank": t["combinedRank"], "profitabilityRank": t["profitabilityRank"],
            "valueRank": t["valueRank"], "returnOnCapital": t["returnOnCapital"],
            "earningsYield": t["earningsYield"],
            "signalClosePrice": t.get("signalClosePrice"), "marketCap": t.get("marketCap"),
        })
    receipt = {
        "schemaVersion": RECEIPT_SCHEMA_VERSION,
        "signalPackagePath": str(pkg),
        "signalPackageManifestSha256": _sha_file(pkg / "manifest.json"),
        "rankingsSha256": _sha_file(pkg / "rankings.json"),
        "signalAsOfDate": man["signalAsOfDate"],
        "rankingGeneratedAt": man["rankingGeneratedAt"],
        "executionDate": res["executionDate"],
        "executionMarketOpenAt": res["executionMarketOpenAt"],
        "executionPriceSource": "pykrx_open",
        "lookAheadValidationPassed": bool(res["lookAheadValidationPassed"]),
        "formulaVersion": man["formulaVersion"],
        "batchId": res["proposedBatchId"],
        "sequence": res["proposedSequence"],
        "allocatedCapital": float(res["allocatedCapital"]),
        "totalInvested": float(res["totalInvested"]),
        "cashReserve": float(res["cashReserve"]),
        "approvedTop10": approved,
        "sourceDryRunLog": str(dry_run_log_path),
        "sourceDryRunExitCode": exit_code,
        "createdAt": created_at,
    }
    receipt["receiptSha256"] = receipt_self_sha(receipt)
    return receipt


def verify_receipt(receipt: dict):
    if receipt.get("schemaVersion") != RECEIPT_SCHEMA_VERSION:
        return False, f"receipt schemaVersion != {RECEIPT_SCHEMA_VERSION}"
    if receipt.get("sourceDryRunExitCode") != 0:
        return False, f"sourceDryRunExitCode != 0 ({receipt.get('sourceDryRunExitCode')})"
    if not receipt.get("lookAheadValidationPassed"):
        return False, "lookAheadValidationPassed != true"
    if receipt.get("receiptSha256") != receipt_self_sha(receipt):
        return False, "receiptSha256 mismatch (receipt tampered)"
    s = round(sum(float(r["amount"]) for r in receipt["approvedTop10"]), 2)
    if not _approx(s, receipt["totalInvested"]):
        return False, f"sum(approved amount) {s} != totalInvested {receipt['totalInvested']}"
    if not _approx(float(receipt["allocatedCapital"]) - float(receipt["totalInvested"]),
                   receipt["cashReserve"]):
        return False, "allocatedCapital - totalInvested != cashReserve"
    return True, "ok"


def verify_signal_package(receipt: dict, signal_pkg_dir):
    pkg = Path(signal_pkg_dir)
    if not (pkg / "manifest.json").exists() or not (pkg / "rankings.json").exists():
        return False, f"signal package files missing under {pkg}"
    if _sha_file(pkg / "manifest.json") != receipt.get("signalPackageManifestSha256"):
        return False, "signalPackageManifestSha256 mismatch"
    if _sha_file(pkg / "rankings.json") != receipt.get("rankingsSha256"):
        return False, "rankingsSha256 mismatch"
    return True, "ok"


# ===== PILOT 보존(격리) =====

def build_pilot_section(pilot_lots: list) -> dict:
    src = [l for l in (pilot_lots or []) if str(l.get("buyDate")) == PILOT_DATE]
    total = round(sum(float(l.get("investedAmount") or 0) for l in src), 2)
    if len(src) != PILOT_EXPECTED_LOTS or not _approx(total, PILOT_EXPECTED_INVESTED):
        raise ReceiptMismatch(
            f"PILOT source mismatch: lots={len(src)} (expect {PILOT_EXPECTED_LOTS}), "
            f"total={total} (expect {PILOT_EXPECTED_INVESTED})")
    batch = E.build_pilot_batch(src, PILOT_DATE)             # 코어 호출(입력 불변)
    audit = W.pilot_timing_audit()
    preserved = [{k: l.get(k) for k in PILOT_CORE_FIELDS} for l in src]
    return {
        "pilotBatchId": batch["batchId"], "operationMode": E.PILOT, "buyDate": PILOT_DATE,
        "itemLotCount": len(src), "totalInvested": total,
        "officialCapitalImpact": 0, "officialSequenceImpact": 0,
        "timingAudit": {
            "legacyRankingBaseDate": audit["legacyRankingBaseDate"],
            "auditedSignalAsOfDate": audit["auditedSignalAsOfDate"],
            "executionDate": audit["executionDate"],
            "timingAuditStatus": audit["timingAuditStatus"],
        },
        "itemLots": preserved,
    }


# ===== OFFICIAL state 빌드(엔진 호출 + 승인값 검증) =====

def build_official_state(receipt: dict, pilot_lots: list):
    appr = receipt["approvedTop10"]
    ranking = [{"code": str(r["code"]), "name": r["name"], "rank": r["rank"],
                "combinedRank": r["combinedRank"], "profitabilityRank": r["profitabilityRank"],
                "valueRank": r["valueRank"], "returnOnCapital": r["returnOnCapital"],
                "earningsYield": r["earningsYield"]} for r in appr]
    open_prices = {str(r["code"]): float(r["openPrice"]) for r in appr}
    cal = E.make_calendar([receipt["signalAsOfDate"], receipt["executionDate"]])
    timing = {
        "signalAsOfDate": receipt["signalAsOfDate"],
        "rankingGeneratedAt": receipt["rankingGeneratedAt"],
        "executionDate": receipt["executionDate"],
        "executionMarketOpenAt": receipt["executionMarketOpenAt"],
        "executionPriceSource": "pykrx_open",
        "lookAheadValidationPassed": True,
    }
    state = E.empty_official_state()
    state, result = E.plan_official_day(state, receipt["executionDate"], ranking,
                                        open_prices, open_prices, cal,
                                        now=receipt["createdAt"], timing=timing)

    # --- 승인값 == 엔진 결과 검증(불일치 → BLOCKED, 임의 변경 금지) ---
    if result["runStatus"] != E.COMPLETED:
        raise ReceiptMismatch(f"engine runStatus {result['runStatus']} != COMPLETED")
    if result["officialSequence"] != int(receipt["sequence"]) or result["officialSequence"] != 1:
        raise ReceiptMismatch(f"sequence {result['officialSequence']} != receipt {receipt['sequence']}")
    if result.get("buyBatchId") != receipt["batchId"]:
        raise ReceiptMismatch(f"batchId {result.get('buyBatchId')} != receipt {receipt['batchId']}")
    if result.get("buyCount") != 10 or result.get("sellCount") != 0:
        raise ReceiptMismatch(f"buy/sell {result.get('buyCount')}/{result.get('sellCount')} != 10/0")
    for key in ("allocatedCapital", "totalInvested", "cashReserve"):
        if not _approx(result.get(key), receipt[key]):
            raise ReceiptMismatch(f"{key} {result.get(key)} != receipt {receipt[key]}")
    if not _approx(state["officialAvailableCash"], 49_000_000):
        raise ReceiptMismatch(f"officialAvailableCash {state['officialAvailableCash']} != 49,000,000")
    if not (_approx(result.get("holdingsMarketValue"), 888_620)
            and _approx(result.get("cash"), 49_111_380)
            and _approx(result.get("totalAsset"), 50_000_000)):
        raise ReceiptMismatch("cash/holdings/totalAsset reconciliation failed")
    bl = {e["code"]: e for e in state["buyLedger"]}
    for r in appr:
        e = bl.get(str(r["code"]))
        if e is None:
            raise ReceiptMismatch(f"buyLedger missing {r['code']}")
        if not (_approx(e["executionPrice"], r["openPrice"]) and e["quantity"] == int(r["quantity"])
                and _approx(e["amount"], r["amount"])):
            raise ReceiptMismatch(
                f"{r['code']} engine(price/qty/amount)=({e['executionPrice']}/{e['quantity']}/{e['amount']}) "
                f"!= receipt({r['openPrice']}/{r['quantity']}/{r['amount']})")
        if e.get("priceSource") != "pykrx_open":
            raise ReceiptMismatch(f"{r['code']} priceSource != pykrx_open")

    state["pilot"] = build_pilot_section(pilot_lots)
    return state, result


# ===== canonical / snapshot 직렬화 =====

def serialize_canonical(state: dict, receipt: dict) -> dict:
    batches = copy.deepcopy(state["batches"])
    planned_sell = batches[0]["plannedSellSequence"] if batches else None
    item_lots = []
    for l in state["itemLots"]:
        lot = dict(l)
        lot["plannedSellSequence"] = planned_sell
        item_lots.append(lot)
    reserve_total = round(sum(b.get("cashReserve") or 0 for b in batches
                              if b.get("operationMode") == E.OFFICIAL and b.get("status") == "OPEN"), 2)
    daily = dict(state["dailyLedger"][-1])
    daily.update({
        "totalBuyAmount": round(sum(float(e["amount"]) for e in state["buyLedger"]), 2),
        "totalSellAmount": round(sum(float(e["amount"]) for e in state["sellLedger"]), 2),
        "realizedProfit": round(sum(float(e.get("realizedProfit") or 0) for e in state["sellLedger"]), 2),
        "officialAvailableCash": state["officialAvailableCash"],
        "batchCashReserveTotal": reserve_total,
        "totalCash": round(state["officialAvailableCash"] + reserve_total, 2),
    })
    return {
        "schemaVersion": STATE_SCHEMA_VERSION,
        "formulaVersion": receipt["formulaVersion"],
        "operationMode": E.OFFICIAL,
        "officialStartDate": state["officialStartDate"],
        "initialCapital": state["initialCapital"],
        "initialBatchCapital": state["initialBatchCapital"],
        "officialSequence": state["officialSequence"],
        "officialTradingCalendar": state["officialTradingCalendar"],
        "officialAvailableCash": state["officialAvailableCash"],
        "batches": batches,
        "itemLots": item_lots,
        "buyLedger": state["buyLedger"],
        "sellLedger": state["sellLedger"],
        "dailyLedger": [daily],
        "evaluationSnapshots": state.get("evaluationSnapshots") or state.get("evalSnapshots") or [],
        "missedRuns": state["missedRuns"],
        "pilot": state["pilot"],
        "prevTotalAsset": state["prevTotalAsset"],
        "updatedAt": receipt["createdAt"],
    }


def build_snapshot(canonical: dict, receipt: dict, canonical_sha: str) -> dict:
    daily = canonical["dailyLedger"][0]
    batch = canonical["batches"][0]
    return {
        "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
        "executionDate": receipt["executionDate"],
        "signalAsOfDate": receipt["signalAsOfDate"],
        "stateSummary": {
            "officialStartDate": canonical["officialStartDate"],
            "officialSequence": canonical["officialSequence"],
            "officialAvailableCash": canonical["officialAvailableCash"],
            "openBatchCount": 1, "openItemLotCount": len(canonical["itemLots"]),
            "totalCash": daily["totalCash"], "holdingsMarketValue": daily["holdingsMarketValue"],
            "totalAsset": daily["totalAsset"],
        },
        "batch": batch,
        "buyLedger": canonical["buyLedger"],
        "sellLedger": canonical["sellLedger"],
        "dailyLedger": daily,
        "executionReceiptSha256": receipt["receiptSha256"],
        "signalPackageManifestSha256": receipt["signalPackageManifestSha256"],
        "canonicalStateSha256": canonical_sha,
        "createdAt": receipt["createdAt"],
    }


# ===== 원자적 쓰기 =====

def atomic_write_bytes(path, data: bytes):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise


# ===== 적용(검증 → dry-run/apply → 원자적 저장 → idempotency) =====

def apply_official_day(receipt: dict, pilot_lots: list, *, do_apply: bool = False, confirm: str = "",
                       state_path=DEFAULT_STATE_PATH, snapshot_dir=DEFAULT_SNAPSHOT_DIR,
                       signal_pkg_dir=None) -> dict:
    state_path = Path(state_path)
    snapshot_dir = Path(snapshot_dir)
    base = {"executionDate": receipt.get("executionDate"), "productionWriteCount": 0,
            "publicCopyCount": 0, "realOrderCount": 0, "statePath": str(state_path)}

    # 1) receipt 무결성
    ok, why = verify_receipt(receipt)
    if not ok:
        return {**base, "status": BLOCKED_EXECUTION_RECEIPT_MISMATCH, "blocked": True, "reason": why}
    # 2) signal package SHA (경로 주어지면)
    if signal_pkg_dir is not None:
        ok, why = verify_signal_package(receipt, signal_pkg_dir)
        if not ok:
            return {**base, "status": BLOCKED_SIGNAL_PACKAGE_MISMATCH, "blocked": True, "reason": why}

    # 3) 엔진으로 state 빌드 + 승인값 검증
    try:
        state, result = build_official_state(receipt, pilot_lots)
    except ReceiptMismatch as e:
        status = BLOCKED_PILOT_SOURCE_MISMATCH if "PILOT" in str(e) else BLOCKED_EXECUTION_RECEIPT_MISMATCH
        return {**base, "status": status, "blocked": True, "reason": str(e)}

    canonical = serialize_canonical(state, receipt)
    canonical_data = canonical_bytes(canonical)
    canonical_sha = _sha_bytes(canonical_data)
    snapshot = build_snapshot(canonical, receipt, canonical_sha)
    snapshot_data = canonical_bytes(snapshot)
    snap_path = snapshot_dir / f"{receipt['executionDate']}.json"

    summary = {
        **base,
        "officialStartDate": canonical["officialStartDate"],
        "officialSequence": canonical["officialSequence"],
        "batchId": canonical["batches"][0]["batchId"],
        "buyCount": len(canonical["buyLedger"]), "sellCount": len(canonical["sellLedger"]),
        "itemLotCount": len(canonical["itemLots"]),
        "allocatedCapital": canonical["batches"][0]["allocatedCapital"],
        "totalInvested": canonical["batches"][0]["totalInvested"],
        "cashReserve": canonical["batches"][0]["cashReserve"],
        "officialAvailableCash": canonical["officialAvailableCash"],
        "totalCash": canonical["dailyLedger"][0]["totalCash"],
        "holdingsMarketValue": canonical["dailyLedger"][0]["holdingsMarketValue"],
        "totalAsset": canonical["dailyLedger"][0]["totalAsset"],
        "pilotItemLotCount": canonical["pilot"]["itemLotCount"],
        "pilotTotalInvested": canonical["pilot"]["totalInvested"],
        "canonicalStateSha256": canonical_sha,
        "snapshotPath": str(snap_path),
        "snapshotSha256": _sha_bytes(snapshot_data),
    }

    # 4) dry-run(기본): 저장 없이 검증 결과만
    if not do_apply:
        return {**summary, "status": DRY_RUN_OK, "blocked": False,
                "reason": "validated; no write (use --apply --confirm to persist)"}

    # 5) confirm 강제
    if confirm != f"APPLY_OFFICIAL_DAY_{receipt['executionDate']}":
        return {**summary, "status": BLOCKED_APPLY_CONFIRMATION_REQUIRED, "blocked": True,
                "reason": f"confirm must be 'APPLY_OFFICIAL_DAY_{receipt['executionDate']}'"}

    # 6) idempotency: 기존 canonical/snapshot 처리(자동 덮어쓰기 금지)
    state_exists = state_path.exists()
    if state_exists:
        if state_path.read_bytes() != canonical_data:
            return {**summary, "status": BLOCKED_OFFICIAL_STATE_CONFLICT, "blocked": True,
                    "reason": "existing official-state differs from proposed (no auto-overwrite)"}
    if snap_path.exists():
        if snap_path.read_bytes() != snapshot_data:
            return {**summary, "status": BLOCKED_SNAPSHOT_CONFLICT, "blocked": True,
                    "reason": f"existing snapshot {snap_path.name} differs (no auto-overwrite)"}
    if state_exists and snap_path.exists():
        return {**summary, "status": ALREADY_PROCESSED, "blocked": False,
                "reason": "identical canonical state & snapshot already persisted"}

    # 7) 원자적 저장(둘 다 tmp 작성 후 replace; 실패 시 기존 보존)
    try:
        atomic_write_bytes(state_path, canonical_data)
        atomic_write_bytes(snap_path, snapshot_data)
    except Exception as e:  # noqa: BLE001
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True, "reason": str(e)}

    # 8) 재읽기 해시 검증
    if _sha_file(state_path) != canonical_sha or _sha_file(snap_path) != summary["snapshotSha256"]:
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True,
                "reason": "post-write hash verification failed"}
    return {**summary, "status": APPLIED, "blocked": False,
            "reason": "official rolling ledger persisted (mock fund internal; no real order)"}


# =====================================================================
# 45-E13: day-N(≥2) append — 기존 canonical에 다음 거래일 batch를 원자적 append.
#   day-1 경로(apply_official_day)는 변경하지 않는다(회귀 보존).
#   엔진(plan_official_day)을 기존 canonical 복사본에 호출해 append를 *재계산*하고
#   receipt 승인값과 1원/1주라도 다르면 BLOCKED. snapshot.canonicalStateSha256를
#   idempotency witness로 사용(동일 receipt 재적용 → ALREADY_PROCESSED).
# =====================================================================

def _read_log_text(path) -> str:
    """dry-run 로그를 인코딩 자동 판별로 읽는다(PowerShell Tee=UTF-16)."""
    raw = Path(path).read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    for enc in ("utf-8-sig", "utf-8", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _extract_result_json(txt: str) -> dict:
    a = txt.index("RESULT_JSON_BEGIN"); a = txt.index("{", a)
    e = txt.index("RESULT_JSON_END", a); b = txt.rindex("}", a, e) + 1
    return json.loads(txt[a:b])


def _parse_pykrx_opens(txt: str) -> dict:
    # day-2: "pykrx_open 10/10: {...}" / day-3+: "pykrx_open buy(TOP10) 10/10 · eval(union) 11/11: {...}"
    m = re.search(r"pykrx_open[^\n]*?(\{[^{}]*\})", txt)
    if not m:
        raise ReceiptMismatch("dry-run log missing 'pykrx_open ...: {...}' line")
    raw = ast.literal_eval(m.group(1))
    return {str(k).zfill(6): float(v) for k, v in raw.items()}


def build_execution_receipt_v2(signal_pkg_dir, dry_run_log_path, canonical_path, *, created_at: str) -> dict:
    """COMPLETED day-N dry-run 로그 + 신호 패키지 + 직전 canonical에서 승인값을 고정(네트워크 0).
    qty/amount는 엔진 순수함수(allocate_quantities)로 재계산하고 dry-run RESULT_JSON 총계와 교차검증한다."""
    pkg = Path(signal_pkg_dir)
    man = _read_json(pkg / "manifest.json")
    rk = _read_json(pkg / "rankings.json")
    txt = _read_log_text(dry_run_log_path)
    if "EXIT_CODE=0" not in txt:
        raise ReceiptMismatch("dry-run log EXIT_CODE != 0")
    res = _extract_result_json(txt)
    for k, want in (("runStatus", "COMPLETED"), ("readOnlyUnchanged", True),
                    ("officialStartDatePersisted", False), ("productionWriteCount", 0)):
        if res.get(k) != want:
            raise ReceiptMismatch(f"dry-run {k}={res.get(k)!r} != {want!r}")
    if not res.get("lookAheadValidationPassed"):
        raise ReceiptMismatch("dry-run lookAheadValidationPassed != true")
    if int(res.get("proposedSequence") or 0) < 2:
        raise ReceiptMismatch(f"proposedSequence {res.get('proposedSequence')} < 2 (v2는 day-N append 전용)")

    opens = _parse_pykrx_opens(txt)
    top10 = rk["top10"]
    if len(top10) != 10:
        raise ReceiptMismatch(f"rankings top10 count {len(top10)} != 10")
    codes = [str(t["code"]).zfill(6) for t in top10]
    miss = [c for c in codes if not opens.get(c) or opens[c] <= 0]
    if miss:
        raise ReceiptMismatch(f"missing pykrx_open for {miss}")

    alloc = float(res["allocatedCapital"])
    qty_map, total_invested = E.allocate_quantities([{"code": c} for c in codes], opens, alloc)
    if qty_map is None:
        raise ReceiptMismatch("engine allocate_quantities BLOCKED (min 1-share > allocated)")
    if not _approx(total_invested, res["totalInvested"]):
        raise ReceiptMismatch(f"engine totalInvested {total_invested} != dry-run {res['totalInvested']}")
    cash_reserve = round(alloc - total_invested, 2)
    if not _approx(cash_reserve, res["cashReserve"]):
        raise ReceiptMismatch(f"cashReserve {cash_reserve} != dry-run {res['cashReserve']}")

    execution_prices = []
    for t in top10:
        c = str(t["code"]).zfill(6)
        op, q = opens[c], qty_map[c]
        execution_prices.append({
            "rank": t["rank"], "code": c, "name": t.get("name"),
            "openPrice": op, "quantity": int(q), "amount": round(op * q, 2),
            "combinedRank": t.get("combinedRank"), "profitabilityRank": t.get("profitabilityRank"),
            "valueRank": t.get("valueRank"), "returnOnCapital": t.get("returnOnCapital"),
            "earningsYield": t.get("earningsYield"),
            "signalClosePrice": t.get("signalClosePrice"), "marketCap": t.get("marketCap"),
        })

    # 평가가(eval) union: 보유∪TOP10. 로그 pykrx_open 라인의 dict가 곧 dry-run이 쓴 eval_prices다
    # (day-2는 TOP10과 동일, day-3+는 보유 비-TOP10 종목 포함). 매수는 TOP10(executionPrices)만.
    eval_prices = [{"code": c, "openPrice": float(op)} for c, op in sorted(opens.items())]

    exec_date = res["executionDate"]
    receipt = {
        "schemaVersion": RECEIPT_V2_SCHEMA_VERSION,
        "receiptType": "OFFICIAL_EXECUTION_RECEIPT",
        "signalPackagePath": str(pkg),
        "sourceSignalPackagePath": str(pkg),
        "signalPackageManifestSha256": _sha_file(pkg / "manifest.json"),
        "rankingsSha256": _sha_file(pkg / "rankings.json"),
        "universeSha256": _sha_file(pkg / "universe.json"),
        "signalAsOfDate": man["signalAsOfDate"],
        "rankingGeneratedAt": man["rankingGeneratedAt"],
        "executionDate": exec_date,
        "executionMarketOpenAt": f"{exec_date}T09:00:00+09:00",
        "executionPriceSource": "pykrx_open",
        "lookAheadValidationPassed": True,
        "formulaVersion": man["formulaVersion"],
        "batchId": res["proposedBatchId"],
        "proposedBatchId": res["proposedBatchId"],
        "sequence": int(res["proposedSequence"]),
        "proposedSequence": int(res["proposedSequence"]),
        "buyTradingDayIndex": int(res["buyTradingDayIndex"]),
        "plannedSellTradingDayIndex": int(res["plannedSellTradingDayIndex"]),
        "buyCount": int(res["buyCount"]),
        "sellCount": int(res["sellCount"]),
        "allocatedCapital": alloc,
        "totalInvested": float(res["totalInvested"]),
        "cashReserve": float(res["cashReserve"]),
        "officialAvailableCashBefore": float(res["officialAvailableCashBefore"]),
        "officialAvailableCashAfter": float(res["officialAvailableCashAfterPreview"]),
        "canonicalBeforeSha256": _sha_file(canonical_path),
        "approvedTop10": execution_prices,
        "executionPrices": execution_prices,
        "evalPrices": eval_prices,
        "holdingsMarketValuePreview": (float(res["holdingsMarketValuePreview"])
                                       if res.get("holdingsMarketValuePreview") is not None else None),
        "missingEvalCodes": [str(c).zfill(6) for c in (res.get("missingEvalCodes") or [])],
        "dryRunLogPath": str(dry_run_log_path),
        "sourceDryRunLog": str(dry_run_log_path),
        "dryRunLogSha256": _sha_file(dry_run_log_path),
        "sourceDryRunExitCode": 0,
        "productionWriteCountAtDryRun": 0,
        "officialStartDatePersistedAtDryRun": False,
        "createdAt": created_at,
    }
    receipt["receiptSha256"] = receipt_self_sha(receipt)
    return receipt


def verify_receipt_v2(receipt: dict):
    if receipt.get("schemaVersion") != RECEIPT_V2_SCHEMA_VERSION:
        return False, f"receipt schemaVersion != {RECEIPT_V2_SCHEMA_VERSION}"
    if receipt.get("sourceDryRunExitCode") != 0:
        return False, f"sourceDryRunExitCode != 0 ({receipt.get('sourceDryRunExitCode')})"
    if not receipt.get("lookAheadValidationPassed"):
        return False, "lookAheadValidationPassed != true"
    if int(receipt.get("sequence") or 0) < 2:
        return False, "sequence < 2 (v2 append 전용)"
    if not receipt.get("canonicalBeforeSha256"):
        return False, "canonicalBeforeSha256 missing"
    if receipt.get("receiptSha256") != receipt_self_sha(receipt):
        return False, "receiptSha256 mismatch (receipt tampered)"
    s = round(sum(float(r["amount"]) for r in receipt["approvedTop10"]), 2)
    if not _approx(s, receipt["totalInvested"]):
        return False, f"sum(approved amount) {s} != totalInvested {receipt['totalInvested']}"
    if not _approx(float(receipt["allocatedCapital"]) - float(receipt["totalInvested"]), receipt["cashReserve"]):
        return False, "allocatedCapital - totalInvested != cashReserve"
    if not _approx(float(receipt["officialAvailableCashBefore"]) - float(receipt["officialAvailableCashAfter"]),
                   receipt["allocatedCapital"]):
        return False, "availCashBefore - availCashAfter != allocatedCapital"
    return True, "ok"


def build_official_state_append(receipt: dict, canonical_bytes: bytes):
    """직전 canonical(bytes) + receipt로 다음 거래일 batch를 엔진 재계산해 append한 new_state 반환.
    receipt 승인값과 엔진 재계산이 다르면 ReceiptMismatch. 입력 canonical 불변(deepcopy)."""
    if _sha_bytes(canonical_bytes) != receipt["canonicalBeforeSha256"]:
        raise CanonicalBeforeMismatch("canonicalBeforeSha256 != current canonical (canonical changed)")
    canon = json.loads(canonical_bytes.decode("utf-8"))

    seq = int(receipt["sequence"])
    if int(canon.get("officialSequence") or 0) != seq - 1:
        raise PriorStateUnexpected(
            f"prior officialSequence {canon.get('officialSequence')} != {seq - 1}")
    prior_batches = canon.get("batches") or []
    if any(b.get("batchId") == receipt["batchId"] for b in prior_batches):
        raise PriorStateUnexpected(f"batch {receipt['batchId']} already in prior canonical")

    appr = receipt["approvedTop10"]
    ranking = [{"code": str(r["code"]), "name": r["name"], "rank": r["rank"],
                "combinedRank": r["combinedRank"], "profitabilityRank": r["profitabilityRank"],
                "valueRank": r["valueRank"], "returnOnCapital": r["returnOnCapital"],
                "earningsYield": r["earningsYield"]} for r in appr]
    opens = {str(r["code"]): float(r["openPrice"]) for r in appr}   # TOP10 (매수)
    # 평가가: receipt.evalPrices(보유∪TOP10) 우선. 없으면 TOP10 fallback(day-2 호환: 그땐 TOP10=보유).
    if receipt.get("evalPrices"):
        eval_opens = {str(e["code"]): float(e["openPrice"]) for e in receipt["evalPrices"]}
    else:
        eval_opens = dict(opens)
    cal = E.make_calendar([receipt["signalAsOfDate"], receipt["executionDate"]])
    timing = {
        "signalAsOfDate": receipt["signalAsOfDate"], "rankingGeneratedAt": receipt["rankingGeneratedAt"],
        "executionDate": receipt["executionDate"], "executionMarketOpenAt": receipt["executionMarketOpenAt"],
        "executionPriceSource": "pykrx_open", "lookAheadValidationPassed": True,
    }
    work = E.migrate_official_state_indices(canon)
    new_state, result = E.plan_official_day(
        work, receipt["executionDate"], ranking, opens, eval_opens, cal,
        now=receipt["createdAt"], timing=timing, trading_day_index=int(receipt["buyTradingDayIndex"]))

    # --- 승인값 == 엔진 재계산 검증(1원/1주 차이도 BLOCKED) ---
    if result["runStatus"] != E.COMPLETED:
        raise ReceiptMismatch(f"engine runStatus {result['runStatus']} != COMPLETED")
    if new_state["officialSequence"] != seq:
        raise ReceiptMismatch(f"officialSequence {new_state['officialSequence']} != receipt {seq}")
    if new_state.get("officialTradingDayIndex") != int(receipt["buyTradingDayIndex"]):
        raise ReceiptMismatch(f"officialTradingDayIndex {new_state.get('officialTradingDayIndex')} "
                              f"!= receipt {receipt['buyTradingDayIndex']}")
    if result.get("buyBatchId") != receipt["batchId"]:
        raise ReceiptMismatch(f"batchId {result.get('buyBatchId')} != receipt {receipt['batchId']}")
    if result.get("buyCount") != int(receipt["buyCount"]) or result.get("sellCount") != int(receipt["sellCount"]):
        raise ReceiptMismatch(f"buy/sell {result.get('buyCount')}/{result.get('sellCount')} "
                              f"!= {receipt['buyCount']}/{receipt['sellCount']}")
    for key in ("allocatedCapital", "totalInvested", "cashReserve"):
        if not _approx(result.get(key), receipt[key]):
            raise ReceiptMismatch(f"{key} {result.get(key)} != receipt {receipt[key]}")
    if not _approx(new_state["officialAvailableCash"], receipt["officialAvailableCashAfter"]):
        raise ReceiptMismatch(f"officialAvailableCash {new_state['officialAvailableCash']} "
                              f"!= receipt {receipt['officialAvailableCashAfter']}")
    nb = new_state["batches"][-1]
    if nb.get("batchId") != receipt["batchId"]:
        raise ReceiptMismatch("appended batch id mismatch")
    if nb.get("buyTradingDayIndex") != int(receipt["buyTradingDayIndex"]) \
            or nb.get("plannedSellTradingDayIndex") != int(receipt["plannedSellTradingDayIndex"]):
        raise ReceiptMismatch(f"batch buy/sell tradingDayIndex "
                              f"{nb.get('buyTradingDayIndex')}/{nb.get('plannedSellTradingDayIndex')} "
                              f"!= {receipt['buyTradingDayIndex']}/{receipt['plannedSellTradingDayIndex']}")
    new_buys = {e["code"]: e for e in new_state["buyLedger"] if e["batchId"] == receipt["batchId"]}
    if len(new_buys) != 10:
        raise ReceiptMismatch(f"appended buyLedger count {len(new_buys)} != 10")
    for r in appr:
        e = new_buys.get(str(r["code"]))
        if e is None:
            raise ReceiptMismatch(f"appended buyLedger missing {r['code']}")
        if not (_approx(e["executionPrice"], r["openPrice"]) and e["quantity"] == int(r["quantity"])
                and _approx(e["amount"], r["amount"])):
            raise ReceiptMismatch(
                f"{r['code']} engine(price/qty/amount)=({e['executionPrice']}/{e['quantity']}/{e['amount']}) "
                f"!= receipt({r['openPrice']}/{r['quantity']}/{r['amount']})")
        if e.get("priceSource") != "pykrx_open":
            raise ReceiptMismatch(f"{r['code']} priceSource != pykrx_open")

    # --- 평가(eval) 재계산이 dry-run preview와 일치하는지(보유 union 가격 적용 검증) ---
    es = (new_state.get("evaluationSnapshots") or [{}])[-1]
    if receipt.get("holdingsMarketValuePreview") is not None:
        if not _approx(es.get("holdingsMarketValue"), receipt["holdingsMarketValuePreview"]):
            raise ReceiptMismatch(f"holdingsMarketValue {es.get('holdingsMarketValue')} "
                                  f"!= receipt preview {receipt['holdingsMarketValuePreview']}")
    rec_missing = {str(c).zfill(6) for c in (receipt.get("missingEvalCodes") or [])}
    eng_missing = {str(c).zfill(6) for c in (es.get("missingEvalCodes") or [])}
    if eng_missing != rec_missing:
        raise ReceiptMismatch(f"missingEvalCodes engine {sorted(eng_missing)} != receipt {sorted(rec_missing)}")

    # --- 직전 batch/lot/missed-run/달력 보존 검증(append-only) ---
    for pb in prior_batches:
        nbatch = next((b for b in new_state["batches"] if b.get("batchId") == pb.get("batchId")), None)
        if nbatch != pb:
            raise ReceiptMismatch(f"prior batch {pb.get('batchId')} mutated by append")
    prior_missed = canon.get("missedRuns") or []
    if (new_state.get("missedRuns") or []) != prior_missed:
        raise ReceiptMismatch("missedRuns mutated by append")
    if new_state["officialExecutionCalendar"][:-1] != list(canon.get("officialExecutionCalendar") or []):
        raise ReceiptMismatch("officialExecutionCalendar prior entries mutated")
    if new_state["officialExecutionCalendar"][-1] != receipt["executionDate"]:
        raise ReceiptMismatch("officialExecutionCalendar last != executionDate")
    if new_state["officialKrxTradingCalendar"][-1] != receipt["executionDate"]:
        raise ReceiptMismatch("officialKrxTradingCalendar last != executionDate")
    return new_state, result, canon


def serialize_canonical_append(new_state: dict, receipt: dict, prior_canon: dict) -> dict:
    """new_state(엔진 append 결과)를 canonical 스키마로 직렬화. 전체 이력 보존, 신규 일자 ledger만 보강."""
    st = E.normalize_eval_snapshots(copy.deepcopy(new_state))
    exec_date = receipt["executionDate"]
    open_reserve = round(sum(b.get("cashReserve") or 0 for b in st["batches"]
                             if b.get("operationMode") == E.OFFICIAL and b.get("status") == "OPEN"), 2)
    pss_by_batch = {b.get("batchId"): b.get("plannedSellSequence") for b in st["batches"]}
    item_lots = []
    for l in st["itemLots"]:
        lot = dict(l)
        lot.setdefault("plannedSellSequence", pss_by_batch.get(lot.get("batchId")))
        item_lots.append(lot)
    daily = []
    for d in st["dailyLedger"]:
        d = dict(d)
        if d.get("date") == exec_date:   # 신규 일자만 집계필드 보강(기존 일자는 불변)
            day_buys = [e for e in st["buyLedger"] if e.get("date") == exec_date]
            day_sells = [e for e in st["sellLedger"] if e.get("date") == exec_date]
            d.update({
                "totalBuyAmount": round(sum(float(e["amount"]) for e in day_buys), 2),
                "totalSellAmount": round(sum(float(e["amount"]) for e in day_sells), 2),
                "realizedProfit": round(sum(float(e.get("realizedProfit") or 0) for e in day_sells), 2),
                "officialAvailableCash": st["officialAvailableCash"],
                "batchCashReserveTotal": open_reserve,
                "totalCash": round(st["officialAvailableCash"] + open_reserve, 2),
            })
        daily.append(d)
    return {
        "schemaVersion": STATE_SCHEMA_VERSION,
        "formulaVersion": prior_canon.get("formulaVersion") or receipt.get("formulaVersion"),
        "operationMode": E.OFFICIAL,
        "officialStartDate": st["officialStartDate"],
        "initialCapital": st["initialCapital"],
        "initialBatchCapital": st["initialBatchCapital"],
        "officialSequence": st["officialSequence"],
        "officialTradingDayIndex": st["officialTradingDayIndex"],
        "officialTradingCalendar": st["officialTradingCalendar"],
        "officialKrxTradingCalendar": st["officialKrxTradingCalendar"],
        "officialExecutionCalendar": st["officialExecutionCalendar"],
        "officialAvailableCash": st["officialAvailableCash"],
        "batches": st["batches"],
        "itemLots": item_lots,
        "buyLedger": st["buyLedger"],
        "sellLedger": st["sellLedger"],
        "dailyLedger": daily,
        "evaluationSnapshots": st["evaluationSnapshots"],
        "missedRuns": st["missedRuns"],
        "pilot": st.get("pilot"),
        "prevTotalAsset": st["prevTotalAsset"],
        "updatedAt": receipt["createdAt"],
    }


def build_snapshot_append(canonical: dict, receipt: dict, canonical_sha: str) -> dict:
    """신규 거래일(batch N)의 immutable snapshot. dailyLedger/batch/buyLedger는 해당 일자만."""
    exec_date = receipt["executionDate"]
    daily = next(d for d in canonical["dailyLedger"] if d.get("date") == exec_date)
    batch = next(b for b in canonical["batches"] if b.get("batchId") == receipt["batchId"])
    day_buys = [e for e in canonical["buyLedger"] if e.get("date") == exec_date]
    day_sells = [e for e in canonical["sellLedger"] if e.get("date") == exec_date]
    return {
        "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
        "executionDate": exec_date,
        "signalAsOfDate": receipt["signalAsOfDate"],
        "officialSequence": canonical["officialSequence"],
        "officialTradingDayIndex": canonical["officialTradingDayIndex"],
        "batchId": receipt["batchId"],
        "status": daily["runStatus"],
        "tradeCreated": True,
        "buyCount": len(day_buys),
        "sellCount": len(day_sells),
        "totalBuyAmount": round(sum(float(e["amount"]) for e in day_buys), 2),
        "totalSellAmount": round(sum(float(e["amount"]) for e in day_sells), 2),
        "realizedProfit": round(sum(float(e.get("realizedProfit") or 0) for e in day_sells), 2),
        "stateSummary": {
            "officialStartDate": canonical["officialStartDate"],
            "officialSequence": canonical["officialSequence"],
            "officialTradingDayIndex": canonical["officialTradingDayIndex"],
            "officialAvailableCash": canonical["officialAvailableCash"],
            "openBatchCount": len([b for b in canonical["batches"]
                                   if b.get("operationMode") == E.OFFICIAL and b.get("status") == "OPEN"]),
            "openItemLotCount": len([l for l in canonical["itemLots"] if l.get("status") == "OPEN"]),
            "totalCash": daily["totalCash"], "holdingsMarketValue": daily["holdingsMarketValue"],
            "totalAsset": daily["totalAsset"],
        },
        "batch": batch,
        "buyLedger": day_buys,
        "sellLedger": day_sells,
        "dailyLedger": daily,
        "executionReceiptSha256": receipt["receiptSha256"],
        "signalPackageManifestSha256": receipt["signalPackageManifestSha256"],
        "canonicalStateSha256": canonical_sha,
        "createdAt": receipt["createdAt"],
    }


def apply_official_append(receipt: dict, *, do_apply: bool = False, confirm: str = "",
                          state_path=DEFAULT_STATE_PATH, snapshot_dir=DEFAULT_SNAPSHOT_DIR,
                          signal_pkg_dir=None) -> dict:
    """day-N(≥2) append 저장. snapshot.canonicalStateSha256를 idempotency witness로 사용."""
    state_path = Path(state_path)
    snapshot_dir = Path(snapshot_dir)
    snap_path = snapshot_dir / f"{receipt.get('executionDate')}.json"
    base = {"executionDate": receipt.get("executionDate"), "productionWriteCount": 0,
            "publicCopyCount": 0, "realOrderCount": 0, "statePath": str(state_path),
            "canonicalBeforeSha256": receipt.get("canonicalBeforeSha256")}

    ok, why = verify_receipt_v2(receipt)
    if not ok:
        return {**base, "status": BLOCKED_EXECUTION_RECEIPT_MISMATCH, "blocked": True, "reason": why}
    if signal_pkg_dir is not None:
        ok, why = verify_signal_package(receipt, signal_pkg_dir)
        if not ok:
            return {**base, "status": BLOCKED_SIGNAL_PACKAGE_MISMATCH, "blocked": True, "reason": why}

    if not state_path.exists():
        return {**base, "status": BLOCKED_PRIOR_STATE_UNEXPECTED, "blocked": True,
                "reason": "prior canonical state missing (append needs day<N already persisted)"}
    cur_bytes = state_path.read_bytes()
    cur_sha = _sha_bytes(cur_bytes)

    # idempotency witness: 현재 canonical이 이미 적용본인지 snapshot으로 판정
    if snap_path.exists():
        try:
            snap = _read_json(snap_path)
        except (json.JSONDecodeError, OSError):
            snap = {}
        if snap.get("canonicalStateSha256") == cur_sha \
                and snap.get("executionReceiptSha256") == receipt.get("receiptSha256"):
            return {**base, "status": ALREADY_PROCESSED, "blocked": False, "canonicalStateSha256": cur_sha,
                    "snapshotPath": str(snap_path), "officialSequence": int(receipt["sequence"]),
                    "reason": "identical canonical state & snapshot already persisted (append)"}

    # 엔진 재계산 + 승인값 검증
    try:
        new_state, _result, prior_canon = build_official_state_append(receipt, cur_bytes)
    except CanonicalBeforeMismatch as e:
        return {**base, "status": BLOCKED_CANONICAL_BEFORE_MISMATCH, "blocked": True,
                "reason": str(e), "currentCanonicalSha256": cur_sha}
    except PriorStateUnexpected as e:
        return {**base, "status": BLOCKED_PRIOR_STATE_UNEXPECTED, "blocked": True, "reason": str(e)}
    except ReceiptMismatch as e:
        return {**base, "status": BLOCKED_RECEIPT_PLAN_MISMATCH, "blocked": True, "reason": str(e)}

    canonical = serialize_canonical_append(new_state, receipt, prior_canon)
    canonical_data = canonical_bytes(canonical)
    canonical_sha = _sha_bytes(canonical_data)
    snapshot = build_snapshot_append(canonical, receipt, canonical_sha)
    snapshot_data = canonical_bytes(snapshot)

    daily = next(d for d in canonical["dailyLedger"] if d.get("date") == receipt["executionDate"])
    summary = {
        **base,
        "officialStartDate": canonical["officialStartDate"],
        "officialSequence": canonical["officialSequence"],
        "officialTradingDayIndex": canonical["officialTradingDayIndex"],
        "batchId": receipt["batchId"],
        "batchCount": len(canonical["batches"]),
        "openBatchCount": len([b for b in canonical["batches"]
                               if b.get("operationMode") == E.OFFICIAL and b.get("status") == "OPEN"]),
        "buyCount": len([e for e in canonical["buyLedger"] if e.get("date") == receipt["executionDate"]]),
        "sellCount": len([e for e in canonical["sellLedger"] if e.get("date") == receipt["executionDate"]]),
        "itemLotCount": len(canonical["itemLots"]),
        "totalBuyCount": len(canonical["buyLedger"]), "totalSellCount": len(canonical["sellLedger"]),
        "allocatedCapital": next(b["allocatedCapital"] for b in canonical["batches"]
                                 if b["batchId"] == receipt["batchId"]),
        "totalInvested": next(b["totalInvested"] for b in canonical["batches"]
                              if b["batchId"] == receipt["batchId"]),
        "cashReserve": next(b["cashReserve"] for b in canonical["batches"]
                            if b["batchId"] == receipt["batchId"]),
        "officialAvailableCash": canonical["officialAvailableCash"],
        "totalCash": daily["totalCash"], "holdingsMarketValue": daily["holdingsMarketValue"],
        "totalAsset": daily["totalAsset"], "missedRunCount": len(canonical["missedRuns"]),
        "canonicalStateSha256": canonical_sha,
        "snapshotPath": str(snap_path), "snapshotSha256": _sha_bytes(snapshot_data),
    }

    if not do_apply:
        return {**summary, "status": DRY_RUN_OK, "blocked": False,
                "reason": "validated append; no write (use --apply --confirm to persist)"}
    if confirm != f"APPLY_OFFICIAL_DAY_{receipt['executionDate']}":
        return {**summary, "status": BLOCKED_APPLY_CONFIRMATION_REQUIRED, "blocked": True,
                "reason": f"confirm must be 'APPLY_OFFICIAL_DAY_{receipt['executionDate']}'"}

    # 쓰기 전 충돌/중복 가드(자동 덮어쓰기 금지)
    if cur_bytes == canonical_data:
        if snap_path.exists() and snap_path.read_bytes() == snapshot_data:
            return {**summary, "status": ALREADY_PROCESSED, "blocked": False,
                    "reason": "identical canonical state & snapshot already persisted (append)"}
        if snap_path.exists():
            return {**summary, "status": BLOCKED_SNAPSHOT_CONFLICT, "blocked": True,
                    "reason": "canonical already appended but snapshot differs (no auto-overwrite)"}
    if snap_path.exists() and snap_path.read_bytes() != snapshot_data:
        return {**summary, "status": BLOCKED_SNAPSHOT_CONFLICT, "blocked": True,
                "reason": f"existing snapshot {snap_path.name} differs (no auto-overwrite)"}

    try:
        atomic_write_bytes(state_path, canonical_data)
        atomic_write_bytes(snap_path, snapshot_data)
    except Exception as e:  # noqa: BLE001
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True, "reason": str(e)}
    if _sha_file(state_path) != canonical_sha or _sha_file(snap_path) != summary["snapshotSha256"]:
        return {**summary, "status": BLOCKED_ATOMIC_WRITE_FAILED, "blocked": True,
                "reason": "post-write hash verification failed"}
    return {**summary, "status": APPLIED_APPEND, "blocked": False,
            "reason": "official rolling ledger appended (mock fund internal; no real order)"}


# ===== CLI =====

def _now_kst_iso():
    return datetime.now(_KST).isoformat()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="OFFICIAL 1일차 장부 저장 (모의펀드 내부; 실제 주문 없음)")
    ap.add_argument("--receipt", default=None, help="execution receipt JSON 경로")
    ap.add_argument("--signal-package", default=None, help="신호 패키지 디렉터리(SHA 검증용)")
    ap.add_argument("--build-receipt", action="store_true", help="receipt 생성 모드(day-1)")
    ap.add_argument("--build-receipt-v2", action="store_true", help="receipt v2 생성 모드(day-N append)")
    ap.add_argument("--append", action="store_true", help="day-N append 저장(기존 canonical에 추가)")
    ap.add_argument("--canonical-path", default=None, help="직전 canonical 경로(build-receipt-v2 입력)")
    ap.add_argument("--dry-run-log", default=None, help="검증 로그 경로(build-receipt 입력)")
    ap.add_argument("--receipt-out", default=None, help="생성할 receipt 저장 경로(build-receipt)")
    ap.add_argument("--apply", action="store_true", help="실제 저장(기본은 dry-run)")
    ap.add_argument("--confirm", default="", help="APPLY_OFFICIAL_DAY_<executionDate>")
    ap.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    ap.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    ap.add_argument("--lots-path", default=str(LOTS_PATH))
    args = ap.parse_args(argv)

    if args.build_receipt or args.build_receipt_v2:
        if args.build_receipt_v2:
            receipt = build_execution_receipt_v2(args.signal_package, args.dry_run_log,
                                                 args.canonical_path or args.state_path,
                                                 created_at=_now_kst_iso())
        else:
            receipt = build_execution_receipt(args.signal_package, args.dry_run_log,
                                              created_at=_now_kst_iso())
        out = Path(args.receipt_out)
        if out.exists():
            existing = _read_json(out)
            same = ({k: v for k, v in existing.items() if k not in ("createdAt", "receiptSha256")}
                    == {k: v for k, v in receipt.items() if k not in ("createdAt", "receiptSha256")})
            print(json.dumps({"status": "ALREADY_PREPARED" if same else "RECEIPT_CONFLICT",
                              "receiptPath": str(out),
                              "receiptSha256": existing.get("receiptSha256")}, ensure_ascii=False, indent=2))
            return 0 if same else 2
        atomic_write_bytes(out, canonical_bytes(receipt))
        print(json.dumps({"status": "RECEIPT_CREATED", "receiptPath": str(out),
                          "receiptSha256": receipt["receiptSha256"]}, ensure_ascii=False, indent=2))
        return 0

    receipt = _read_json(args.receipt)
    if args.append or receipt.get("schemaVersion") == RECEIPT_V2_SCHEMA_VERSION:
        res = apply_official_append(receipt, do_apply=args.apply, confirm=args.confirm,
                                    state_path=args.state_path, snapshot_dir=args.snapshot_dir,
                                    signal_pkg_dir=args.signal_package)
    else:
        lots = (_read_json(args.lots_path, encoding="utf-8-sig") or {}).get("lots") or []
        res = apply_official_day(receipt, lots, do_apply=args.apply, confirm=args.confirm,
                                 state_path=args.state_path, snapshot_dir=args.snapshot_dir,
                                 signal_pkg_dir=args.signal_package)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] in (APPLIED, APPLIED_APPEND, ALREADY_PROCESSED, DRY_RUN_OK) else 2


if __name__ == "__main__":
    sys.exit(main())
