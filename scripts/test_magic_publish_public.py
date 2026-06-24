#!/usr/bin/env python3
"""magic_publish_public 테스트 (Phase 45-AUTO3). plan write 0. apply 게이트만 검증(실 REPO1 미수정).
build_magic_official_public 은 fixture model 로 주입(실 canonical 미접근)."""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import magic_publish_public as PUB

NOW = "2026-06-25T16:30:00+09:00"

MODEL = {
    "magicOfficialSummary": {"officialSequence": 4, "totalAsset": 50_000_000,
                             "dataDate": "2026-06-25", "sourceStateSha256": "a" * 64},
    "magicOfficialPortfolio": {"holdings": [{"code": "046940"}, {"code": "461300"}]},
    "magicOfficialTradeDays": [{"date": "2026-06-25"}, {"date": "2026-06-23"},
                               {"date": "2026-06-19"}, {"date": "2026-06-17"}],
}


def _repo1(seq=3):
    return {
        "magicOfficialSummary": {"officialSequence": seq, "totalAsset": 49_831_105},
        "magicOfficialPortfolio": {"holdings": []},
        "magicOfficialTradeDays": [{"date": "2026-06-23"}],
        # 나머지 키(와바바/AI/PILOT 등 모사) — 절대 건드리면 안 됨
        "magicPortfolioSummary": {"x": 1}, "wababaFund": {"y": 2}, "wababaAiFund": {"z": 3},
        "recommendations": [], "generatedAt": "2026-06-23T17:00:00+09:00",
    }


def _patch_model(model=MODEL):
    PUB.P.build_magic_official_public = lambda state_path=None: model


def _ticket(status="APPROVED", approved=True):
    return {"schemaVersion": "magic-approval-ticket-v1", "ticketId": "MF-APPROVAL-2026-06-25-PUBLIC_PUBLISH",
            "actionType": "PUBLIC_PUBLISH", "status": status, "executionDate": "2026-06-25",
            "approval": {"approved": approved, "approvalPhrase": "APPROVE_PUBLIC_PUBLISH_2026-06-25"}}


# ===== 테스트 =====

def t18_plan_detects_seq_diff():
    r = PUB.build_plan(MODEL, _repo1(seq=3), repo1_path="repo1.json", canonical_sha="d" * 64, now=NOW)
    assert r["status"] == "PLAN_OK", r
    assert r["currentCanonicalSeq"] == 4 and r["currentRepo1PublicSeq"] == 3
    assert r["updateNeeded"] is True
    assert "magicOfficialSummary" in r["changedKeys"]
    assert r["magicOfficialPreview"]["seqBefore"] == 3 and r["magicOfficialPreview"]["seqAfter"] == 4


def t19_plan_write0():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_model()
        repo1 = root / "recommendation-history.json"
        doc = _repo1(seq=3)
        repo1.write_text(json.dumps(doc), encoding="utf-8")
        before = repo1.read_bytes()
        r = PUB.plan(canonical_path=root / "canon.json", repo1_path=repo1, now=NOW)
        assert r["status"] == "PLAN_OK", r
        assert r["productionWriteCount"] == 0 and r["publicCopyCount"] == 0
        assert repo1.read_bytes() == before  # write 0
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t20_apply_no_approval_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_model()
        repo1 = root / "recommendation-history.json"
        repo1.write_text(json.dumps(_repo1()), encoding="utf-8")
        before = repo1.read_bytes()
        r = PUB.apply(_ticket(status="PENDING_APPROVAL", approved=False),
                      canonical_path=root / "canon.json", repo1_path=repo1,
                      git_status_fn=lambda: "", do_write=False, now=NOW)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_TICKET_NOT_APPROVED", r
        assert repo1.read_bytes() == before and r["publicChanged"] is False
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t21_apply_repo1_dirty_blocked():
    root = Path(tempfile.mkdtemp())
    try:
        _patch_model()
        repo1 = root / "recommendation-history.json"
        repo1.write_text(json.dumps(_repo1()), encoding="utf-8")
        before = repo1.read_bytes()
        dirty = lambda: " M recommendation-history.json\n"
        r = PUB.apply(_ticket(), canonical_path=root / "canon.json", repo1_path=repo1,
                      git_status_fn=dirty, do_write=False, now=NOW)
        assert r["status"] == "BLOCKED" and r["blockedCode"] == "BLOCKED_REPO1_NOT_CLEAN", r
        assert repo1.read_bytes() == before
    finally:
        shutil.rmtree(root, ignore_errors=True)


def t22_only_three_keys_targeted():
    repo1 = _repo1(seq=3)
    r = PUB.build_plan(MODEL, repo1, repo1_path="repo1.json", canonical_sha="d" * 64, now=NOW)
    assert r["keysToUpdate"] == ["magicOfficialSummary", "magicOfficialPortfolio", "magicOfficialTradeDays"]
    assert set(r["changedKeys"]) <= set(r["keysToUpdate"])
    assert r["untouchedKeyCount"] == len(repo1) - 3
    assert r["repo1TotalKeyCount"] == len(repo1)
    # apply 의 drift 계산도 3키 외 불변 확인
    new_doc, drift = PUB._compute_drift(repo1, MODEL)
    assert drift is None
    for k in repo1:
        if k not in r["keysToUpdate"]:
            assert new_doc[k] == repo1[k]


TESTS = [
    ("18 plan: canonical seq vs REPO1 seq 차이 감지", t18_plan_detects_seq_diff),
    ("19 plan write 0(REPO1 불변)", t19_plan_write0),
    ("20 apply 승인 없음 → BLOCKED(불변)", t20_apply_no_approval_blocked),
    ("21 apply REPO1 dirty → BLOCKED(불변)", t21_apply_repo1_dirty_blocked),
    ("22 magicOfficial 3키만 변경 대상", t22_only_three_keys_targeted),
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
