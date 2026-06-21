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
import copy
import hashlib
import json
import os
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
SNAPSHOT_SCHEMA_VERSION = "magic-official-snapshot-v1"

# 상태
APPLIED = "APPLIED_OFFICIAL_DAY"
DRY_RUN_OK = "DRY_RUN_OK"
ALREADY_PROCESSED = "ALREADY_PROCESSED"
BLOCKED_APPLY_CONFIRMATION_REQUIRED = "BLOCKED_APPLY_CONFIRMATION_REQUIRED"
BLOCKED_EXECUTION_RECEIPT_MISMATCH = "BLOCKED_EXECUTION_RECEIPT_MISMATCH"
BLOCKED_SIGNAL_PACKAGE_MISMATCH = "BLOCKED_SIGNAL_PACKAGE_MISMATCH"
BLOCKED_OFFICIAL_STATE_CONFLICT = "BLOCKED_OFFICIAL_STATE_CONFLICT"
BLOCKED_SNAPSHOT_CONFLICT = "BLOCKED_SNAPSHOT_CONFLICT"
BLOCKED_PILOT_SOURCE_MISMATCH = "BLOCKED_PILOT_SOURCE_MISMATCH"
BLOCKED_ATOMIC_WRITE_FAILED = "BLOCKED_ATOMIC_WRITE_FAILED"

_KST = timezone(timedelta(hours=9))


class ReceiptMismatch(Exception):
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


# ===== CLI =====

def _now_kst_iso():
    return datetime.now(_KST).isoformat()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="OFFICIAL 1일차 장부 저장 (모의펀드 내부; 실제 주문 없음)")
    ap.add_argument("--receipt", default=None, help="execution receipt JSON 경로")
    ap.add_argument("--signal-package", default=None, help="신호 패키지 디렉터리(SHA 검증용)")
    ap.add_argument("--build-receipt", action="store_true", help="receipt 생성 모드")
    ap.add_argument("--dry-run-log", default=None, help="E6.1 검증 로그 경로(build-receipt 입력)")
    ap.add_argument("--receipt-out", default=None, help="생성할 receipt 저장 경로(build-receipt)")
    ap.add_argument("--apply", action="store_true", help="실제 저장(기본은 dry-run)")
    ap.add_argument("--confirm", default="", help="APPLY_OFFICIAL_DAY_<executionDate>")
    ap.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    ap.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    ap.add_argument("--lots-path", default=str(LOTS_PATH))
    args = ap.parse_args(argv)

    if args.build_receipt:
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
    lots = (_read_json(args.lots_path, encoding="utf-8-sig") or {}).get("lots") or []
    res = apply_official_day(receipt, lots, do_apply=args.apply, confirm=args.confirm,
                             state_path=args.state_path, snapshot_dir=args.snapshot_dir,
                             signal_pkg_dir=args.signal_package)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] in (APPLIED, ALREADY_PROCESSED, DRY_RUN_OK) else 2


if __name__ == "__main__":
    sys.exit(main())
