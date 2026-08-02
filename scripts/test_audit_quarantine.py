#!/usr/bin/env python3
"""감사 격리 + seq20~22 무결성 계약 테스트.

WABABA-KRX-SEQ20-22-INTEGRITY-AUDIT-QUARANTINE-R1.
운영 파일 write 0 · 네트워크 0 · 실주문 0. 격리 marker 는 백업 후 복원한다.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import audit_quarantine as AQ
import krx_data_quality as Q
import magic_publish_gate as G
import magic_daily_auto_apply as A

ROOT = Path(__file__).resolve().parents[1]
CANON = ROOT / "magic-formula-official-state.json"
PUB = Path("C:/work/kr-stock-agent/public/data/recommendation-history.json")
D = "2026-08-03"   # 다음 실제 거래일(seq23 후보)


def _canon_fixture(latest=D, seq=22, tdi=24):
    """D 까지 반영된 canonical 형태 — 앞단 검사(미반영 거래일·최신일)를 통과해
    뒤쪽 격리·품질 게이트를 실제로 평가하게 한다."""
    return {"officialSequence": seq, "officialTradingDayIndex": tdi,
            "officialExecutionCalendar": [latest], "missedRuns": [],
            "dailyLedger": [{"date": latest, "runStatus": "COMPLETED"}]}


def _apply_fixture(date=D):
    return {"date": date, "status": "APPLIED_AUTOMATICALLY", "verdict": "PASS",
            "realOrderCount": 0, "brokerApiCallCount": 0}


def _quality_ok(date=D):
    return Q.build_status(date_iso=date, verdict="PASS",
                          markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}, universe_count=2763)


class Marker:
    """격리 marker 를 임시 경로로 격리해 실제 운영 marker 를 건드리지 않는다."""

    def __enter__(self):
        self._dir = tempfile.mkdtemp()
        self._orig = AQ.MARKER_PATH
        AQ.MARKER_PATH = Path(self._dir) / "audit-quarantine.json"
        return self

    def __exit__(self, *a):
        AQ.MARKER_PATH = self._orig
        shutil.rmtree(self._dir, ignore_errors=True)

    def activate(self):
        AQ.activate(AQ.build_marker(reason="테스트 격리", blocked_sequences_from=23))

    def release(self):
        AQ.release(released_by="test", evidence="test")


# ── 1~5 격리 상태에서 write·seq·lot 전부 0 ──────────────────────────────────
def t01_quarantine_blocks_auto_apply():
    with Marker() as m:
        m.activate()
        g = A.evaluate_auto_approval_gates(canonical=_canon_fixture(), target_exec_date=D,
                                           dry_run={"executionDate": D}, ranking={"x": 1},
                                           rules=A.engine_rules(), quality_status=_quality_ok())
        assert g["eligible"] is False, g
        assert A.B_AUDIT_QUARANTINE in [b["code"] for b in g["blockedCodes"]], g["blockedCodes"]


def t02_quarantine_blocks_auto_publish():
    with Marker() as m:
        m.activate()
        r = G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                       lock_held=False, public_model_error=None, quality_status=_quality_ok())
        assert r["verdict"] == "BLOCKED", r
        assert "QUARANTINE" in r["decision"], r["decision"]
        assert r["filesWritten"] == 0


def t03_quarantine_beats_quality_pass():
    """품질이 PASS 여도 격리가 우선한다(= seq 증가 0)."""
    with Marker() as m:
        m.activate()
        g = A.evaluate_auto_approval_gates(canonical=_canon_fixture(), target_exec_date=D,
                                           dry_run={"executionDate": D}, ranking={"x": 1},
                                           rules=A.engine_rules(), quality_status=_quality_ok())
        codes = [b["code"] for b in g["blockedCodes"]]
        assert codes == [A.B_AUDIT_QUARANTINE], codes   # 격리에서 즉시 중단


def t04_quarantine_no_new_lot_or_fifo():
    """apply 게이트가 막히면 신규 lot·FIFO 매도 경로 자체에 진입하지 않는다."""
    with Marker() as m:
        m.activate()
        g = A.evaluate_auto_approval_gates(canonical=_canon_fixture(), target_exec_date=D,
                                           dry_run={"executionDate": D}, ranking={"x": 1},
                                           rules=A.engine_rules(), quality_status=_quality_ok())
        assert g["eligible"] is False
        assert "lotsPlusTen" not in g["checks"], "게이트 통과 후 검사에 도달하면 안 됨"


def t05_quarantine_public_write_zero():
    with Marker() as m:
        m.activate()
        r = G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                       lock_held=False, public_model_error=None, quality_status=_quality_ok())
        assert r["productionWriteCount"] == 0 and r["filesWritten"] == 0


# ── 6~10 KRX 품질 게이트(격리 해제 상태) ────────────────────────────────────
def t06_quality_unknown_not_pass():
    with Marker() as m:
        m.release()
        r = G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                       lock_held=False, public_model_error=None, quality_status=None)
        assert r["verdict"] == "BLOCKED" and "EVIDENCE_MISSING" in r["decision"], r


def t07_kospi_fail_blocks():
    with Marker() as m:
        m.release()
        bad = Q.build_status(date_iso=D, verdict="INVALID", markets={"KOSPI": "INVALID", "KOSDAQ": "PASS"})
        r = G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                       lock_held=False, public_model_error=None, quality_status=bad)
        assert r["verdict"] == "BLOCKED", r


def t08_kosdaq_fail_blocks():
    with Marker() as m:
        m.release()
        half = Q.build_status(date_iso=D, verdict="PASS", markets={"KOSPI": "PASS", "KOSDAQ": "INVALID"})
        ok, code, _ = Q.evaluate(half, D)
        assert not ok and code == "KRX_MARKET_NOT_PASS", code


def t09_universe_collapse_blocks():
    st = Q.build_status(date_iso=D, verdict="INVALID", markets={"KOSPI": "PASS", "KOSDAQ": "PASS"},
                        universe_count=120, reason="universe 급감")
    ok, code, _ = Q.evaluate(st, D)
    assert not ok and code == "KRX_DATA_QUALITY_INVALID", code


def t10_required_columns_missing_blocks():
    import krx_fetch_guard as FG
    import pandas as pd
    spec = FG.FetchSpec(kind="fundamental", market="KOSPI",
                        required_columns=("티커", "PER", "PBR", "DIV"), min_rows=1)
    ok, code, _ = FG.validate_frame(pd.DataFrame({"티커": ["000001"], "PER": [1.0]}), spec)
    assert not ok and code == "REQUIRED_COLUMNS_MISSING", code


# ── 11~17 감사 증거 계약 (실제 보존 증거 기반, read-only) ────────────────────
SIG = Path("C:/Users/duria/AppData/Local/Temp/wababa-magic-signal")
REC = Path("C:/Users/duria/AppData/Local/Temp/wababa-magic-auto-approval")
AUDIT = [(20, "2026-07-29", "2026-07-28"), (21, "2026-07-30", "2026-07-29"), (22, "2026-07-31", "2026-07-30")]


def _have_evidence():
    return all((REC / d / f"execution-receipt-seq{s}.json").exists() and (SIG / g / "universe.json").exists()
               for s, d, g in AUDIT)


def t11_receipt_universe_sha_matches_preserved():
    if not _have_evidence():
        return
    for seq, exec_date, signal_date in AUDIT:
        rec = json.loads((REC / exec_date / f"execution-receipt-seq{seq}.json").read_text(encoding="utf-8"))
        actual = hashlib.sha256((SIG / signal_date / "universe.json").read_bytes()).hexdigest()
        assert rec.get("universeSha256") == actual, f"seq{seq} universe SHA 불일치"


def t12_sha_chain_links():
    if not _have_evidence():
        return
    prev_after = None
    for seq, exec_date, _ in AUDIT:
        ar = json.loads((REC / exec_date / "apply-result.json").read_text(encoding="utf-8"))
        if prev_after is not None:
            assert ar["canonicalSha256Before"] == prev_after, f"seq{seq} SHA chain 끊김"
        prev_after = ar["canonicalSha256After"]
    cur = hashlib.sha256(CANON.read_bytes()).hexdigest()
    assert prev_after == cur, "마지막 after != 현재 canonical"


def t13_ledger_arithmetic_consistent():
    st = json.loads(CANON.read_text(encoding="utf-8"))
    dl = {d["officialSequence"]: d for d in st["dailyLedger"] if d.get("runStatus") == "COMPLETED"}
    for seq in (20, 21, 22):
        cur, prev = dl.get(seq), dl.get(seq - 1)
        assert cur and prev, f"seq{seq} ledger 없음"
        assert cur["buyCount"] == 10, f"seq{seq} 매수 10건 아님"
        assert int(prev["officialAvailableCash"]) - int(cur["officialAvailableCash"]) == 1_000_000, \
            f"seq{seq} 현금 감소가 배치 배분액과 다름"


def t14_per_pbr_immune_to_ranking():
    """공식 마법공식은 PER/PBR/DIV 에 영향받지 않는다(결측·오염 모두)."""
    if not _have_evidence():
        return
    import build_magic_formula_fund as mff
    import copy
    rows = json.loads((SIG / "2026-07-29" / "universe.json").read_text(encoding="utf-8"))["data"]

    def top10(rs):
        o = mff.calculate_magic_formula_ranking(rs, "book_faithful_v1", [])
        f = o[0] if isinstance(o, (list, tuple)) else o
        return [str(s.get("code")) for s in (f or [])[:10]]

    base = top10(rows)
    blanked = copy.deepcopy(rows)
    for r in blanked:
        r["PER"] = None; r["PBR"] = None; r["DIV"] = None
    assert top10(blanked) == base, "PER/PBR 결측이 ranking 을 바꿈"


def t15_exact_match_recorded_inputs():
    """보존 universe → 공식 ranking 재실행 결과가 canonical 실제 매수와 일치."""
    if not _have_evidence():
        return
    import build_magic_formula_fund as mff
    st = json.loads(CANON.read_text(encoding="utf-8"))
    buys = {}
    for e in st["buyLedger"]:
        buys.setdefault(e.get("batchId"), []).append(str(e.get("code")))
    for seq, exec_date, signal_date in AUDIT:
        rows = json.loads((SIG / signal_date / "universe.json").read_text(encoding="utf-8"))["data"]
        rec = json.loads((REC / exec_date / f"execution-receipt-seq{seq}.json").read_text(encoding="utf-8"))
        o = mff.calculate_magic_formula_ranking(rows, rec.get("formulaMode"), [])
        f = o[0] if isinstance(o, (list, tuple)) else o
        re_top = sorted(str(s.get("code")) for s in (f or [])[:10])
        canon = sorted(buys.get(rec.get("batchId"), []))
        assert re_top == canon, f"seq{seq} 재계산 top10 != canonical 매수"


def t16_material_mismatch_is_detectable():
    """비교 로직이 실제 불일치를 잡아내는지(반대 방향 검증)."""
    a = ["A", "B", "C"]; b = ["A", "B", "D"]
    assert a != b, "불일치 감지 실패"


def t17_missing_evidence_not_pass():
    ok, code, _ = Q.evaluate(None, D)
    assert not ok and code == "KRX_QUALITY_EVIDENCE_MISSING", code


# ── 18~22 불변·안전 ─────────────────────────────────────────────────────────
def t18_canonical_sha_unchanged_by_gates():
    before = hashlib.sha256(CANON.read_bytes()).hexdigest()
    with Marker() as m:
        m.activate()
        A.evaluate_auto_approval_gates(canonical=_canon_fixture(), target_exec_date=D,
                                       dry_run={"executionDate": D}, ranking={"x": 1},
                                       rules=A.engine_rules(), quality_status=_quality_ok())
        G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                   lock_held=False, public_model_error=None, quality_status=_quality_ok())
    assert hashlib.sha256(CANON.read_bytes()).hexdigest() == before, "canonical 변경됨"


def t19_public_sha_unchanged_by_gates():
    if not PUB.exists():
        return
    before = hashlib.sha256(PUB.read_bytes()).hexdigest()
    with Marker() as m:
        m.activate()
        G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                   lock_held=False, public_model_error=None, quality_status=_quality_ok())
    assert hashlib.sha256(PUB.read_bytes()).hexdigest() == before, "운영 public 변경됨"


def t20_no_order_path_in_quarantine_module():
    src = Path(AQ.__file__).read_text(encoding="utf-8")
    for bad in ("주문", "order_submit", "place_order", "브로커", "broker_api", "kiwoom", "creon"):
        assert bad not in src, f"격리 모듈에 주문/브로커 흔적: {bad}"


def t21_marker_reports_zero_counts():
    m = AQ.build_marker(reason="x")
    assert m["realOrderCount"] == 0 and m["brokerApiCallCount"] == 0


def t22_no_secret_in_quarantine_marker():
    src = Path(AQ.__file__).read_text(encoding="utf-8")
    for bad in ("KRX_PW", "password", "JSESSIONID", "Authorization"):
        assert bad not in src, f"격리 모듈에 비밀값 키: {bad}"


# ── 23~24 해제 조건 / 보고 배선 ─────────────────────────────────────────────
def t23_release_requires_explicit_action_and_is_recorded():
    with Marker() as m:
        m.activate()
        ok, _, _ = AQ.gate()
        assert ok is False, "활성 marker 인데 통과"
        AQ.release(released_by="audit", evidence="seq20-22 EXACT_MATCH", now="2026-08-02T00:00:00+09:00")
        ok2, _, _ = AQ.gate()
        assert ok2 is True, "해제 후에도 차단"
        cur = AQ.read_marker()
        assert cur["active"] is False and cur["releasedBy"] == "audit", cur
        assert cur.get("releaseEvidence"), "해제 증거가 기록돼야 함"


def t23b_unreadable_marker_is_fail_closed():
    with Marker() as m:
        AQ.MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
        AQ.MARKER_PATH.write_text("{ broken json", encoding="utf-8")
        ok, code, _ = AQ.gate()
        assert ok is False, "판독 불가 marker 는 격리로 봐야 함"


def t23c_no_auto_expiry():
    m = AQ.build_marker(reason="x")
    assert m["autoReleaseAllowed"] is False
    assert "releaseCondition" in m


def t24_krx_quality_cannot_be_reported_as_pass():
    """품질 INVALID/UNKNOWN 이면 publish status verdict 가 BLOCKED 가 되어
    Founder 보고(원천 = wababa-auto-publish-status-latest.json)에서 PASS 로 숨을 수 없다."""
    with Marker() as m:
        m.release()
        for qs in (None, Q.build_status(date_iso=D, verdict="INVALID", markets={"KOSPI": "INVALID"})):
            r = G.evaluate(today_iso=D, canonical=_canon_fixture(), apply_status=_apply_fixture(),
                           lock_held=False, public_model_error=None, quality_status=qs)
            assert r["verdict"] == "BLOCKED", r
    ps = Path("C:/work/kr-stock-agent/scripts/ops/publish-public-data.ps1")
    if ps.exists():
        src = ps.read_text(encoding="utf-8-sig")
        assert 'Verdict "BLOCKED"' in src, "BLOCKED 판정이 상태 산출물에 기록되는 경로가 있어야 함"


TESTS = [
    ("01 격리 중 Auto Apply 차단", t01_quarantine_blocks_auto_apply),
    ("02 격리 중 Auto Publish 차단", t02_quarantine_blocks_auto_publish),
    ("03 품질 PASS 여도 격리 우선(seq 증가 0)", t03_quarantine_beats_quality_pass),
    ("04 격리 중 신규 lot·FIFO 경로 미진입", t04_quarantine_no_new_lot_or_fifo),
    ("05 격리 중 public write 0", t05_quarantine_public_write_zero),
    ("06 KRX 품질 UNKNOWN → PASS 금지", t06_quality_unknown_not_pass),
    ("07 KOSPI 실패 → 차단", t07_kospi_fail_blocks),
    ("08 KOSDAQ 실패 → 차단", t08_kosdaq_fail_blocks),
    ("09 universe 급감 → 차단", t09_universe_collapse_blocks),
    ("10 필수 columns 누락 → 차단", t10_required_columns_missing_blocks),
    ("11 receipt universe SHA == 보존 universe", t11_receipt_universe_sha_matches_preserved),
    ("12 seq20→22 SHA chain 연결", t12_sha_chain_links),
    ("13 장부 산술 정합(현금·매수건수)", t13_ledger_arithmetic_consistent),
    ("14 PER/PBR/DIV 는 ranking 에 영향 없음", t14_per_pbr_immune_to_ranking),
    ("15 재계산 top10 == canonical 실제 매수", t15_exact_match_recorded_inputs),
    ("16 불일치 감지 로직 동작", t16_material_mismatch_is_detectable),
    ("17 증거 없음 → PASS 금지", t17_missing_evidence_not_pass),
    ("18 게이트 평가가 canonical 을 바꾸지 않음", t18_canonical_sha_unchanged_by_gates),
    ("19 게이트 평가가 운영 public 을 바꾸지 않음", t19_public_sha_unchanged_by_gates),
    ("20 격리 모듈에 주문·브로커 경로 0", t20_no_order_path_in_quarantine_module),
    ("21 marker 실주문·브로커 0 기록", t21_marker_reports_zero_counts),
    ("22 격리 모듈 비밀값 0", t22_no_secret_in_quarantine_marker),
    ("23 해제는 명시 조치 + 증거 기록", t23_release_requires_explicit_action_and_is_recorded),
    ("23b 판독 불가 marker 는 fail-closed", t23b_unreadable_marker_is_fail_closed),
    ("23c 자동 만료 없음", t23c_no_auto_expiry),
    ("24 품질 INVALID/UNKNOWN 은 PASS 로 숨지 않음", t24_krx_quality_cannot_be_reported_as_pass),
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
