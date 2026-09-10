#!/usr/bin/env python3
"""와바바 마법공식 paper lane HOLD 정책 판정 (R4).

WABABA-MAGIC-FORMULA-PAPER-LANE-HOLD-ENFORCEMENT-AND-TRANSPARENCY-R4
SOURCE: R3 (21fdced) — OFFICIAL_SOURCE_REPLACEMENT_NO_GO

왜 필요한가
-----------
R3 에서 공식 source 로의 대체가 NO_GO 로 확정됐다(업종분류 부재 · 공식 일별 데이터 T+1).
그런데 15:40 Signal 은 매일 pykrx 웹 로그인을 시도하고(CD007) 실패해 Result 2 를 내고,
Dry Run / Status / Auto Apply 가 그 실패를 전파한다. 이건 운영 장애가 아니라 **의도된 중지**다.

이 모듈이 하는 일
-----------------
- config/wababa-magic-paper-lane-state.json 을 읽어 HOLD / UNHELD / INVALID 를 판정한다.
- 정책 본문 hash 를 **코드에 고정**한다. 정책 파일만 고쳐서 HOLD 를 풀 수 없다(무단 해제 거부).
- HOLD 일 때 각 단계가 쓰는 표준 보고 dict 를 만든다.

import 규칙 (중요)
------------------
stdlib 만 쓴다. pykrx · build_market_snapshot · build_market_snapshot_fast ·
build_magic_signal_package 를 **절대 import 하지 않는다**. 이 모듈은 그것들보다 먼저 불린다.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config" / "wababa-magic-paper-lane-state.json"
SCHEMA_VERSION = "wababa.magic-paper-lane-state/v1"

# 정책 본문(policySha256 제외)의 canonical JSON SHA-256.
# 정책을 바꾸려면(HOLD 해제 포함) 이 값도 **같은 커밋에서** 바꿔야 한다 → 리뷰를 거친다.
EXPECTED_POLICY_SHA256 = "bc2b94313368592e571baddbab0f04dd62919d325d033d708e82e8d4f2f3b2e5"

KST = timezone(timedelta(hours=9))

DECISION_HOLD = "HOLD"
DECISION_UNHELD = "UNHELD"
DECISION_INVALID = "INVALID"

LANE_STATE = "HOLD_NO_CANONICAL_SAME_DAY_SOURCE"
REASON_CLASS = "MAGIC_PAPER_LANE_HOLD_NO_CANONICAL_SAME_DAY_SOURCE"
DOWNSTREAM_REASON = "UPSTREAM_MAGIC_PAPER_LANE_HOLD"
HOLD_CLASSIFICATION = "KNOWN_INTENTIONAL_HOLD"
STATUS_EXPECTED_HOLD = "EXPECTED_HOLD_NO_ACTION"
APPLY_SKIPPED_EXPECTED_HOLD = "SKIPPED_EXPECTED_HOLD"
PUBLISH_SKIPPED_EXPECTED_HOLD = "SKIPPED_EXPECTED_HOLD"
BLOCKED_HOLD_POLICY_INVALID = "BLOCKED_HOLD_POLICY_INVALID"

EXIT_HOLD_OK = 0
EXIT_POLICY_INVALID = 3

LANE_TASKS = ("Wababa Magic Daily Signal", "Wababa Magic Daily Dry Run",
              "Wababa Magic Daily Status", "Wababa Magic Daily Auto Apply")


def now_kst_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def canonical_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def body_sha256(policy: dict) -> str:
    """정책 본문 hash. 줄바꿈·들여쓰기와 무관하다(파싱 후 canonical 직렬화)."""
    body = {k: v for k, v in policy.items() if k != "policySha256"}
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def _verdict(decision, code, reason, *, path, expected, policy=None, computed=None) -> dict:
    return {"decision": decision, "code": code, "reason": reason, "policyPath": str(path),
            "policy": policy, "computedSha256": computed, "expectedSha256": expected}


def evaluate(policy_path=None, expected_sha: str | None = None) -> dict:
    """HOLD 정책 판정. 어떤 실패도 HOLD 를 무시하고 기존 실행으로 넘어가게 하지 않는다(fail-closed)."""
    try:
        return _evaluate(policy_path, expected_sha)
    except Exception as e:  # noqa: BLE001 — 판정기 자체 오류도 INVALID(= 기존 로직 미실행)
        return _verdict(DECISION_INVALID, "POLICY_EVALUATION_ERROR", f"판정 오류: {type(e).__name__}",
                        path=Path(policy_path) if policy_path else POLICY_PATH,
                        expected=EXPECTED_POLICY_SHA256 if expected_sha is None else expected_sha)


def _evaluate(policy_path, expected_sha) -> dict:
    path = Path(policy_path) if policy_path else POLICY_PATH
    expected = EXPECTED_POLICY_SHA256 if expected_sha is None else expected_sha

    def inv(code, reason, **kw):
        return _verdict(DECISION_INVALID, code, reason, path=path, expected=expected, **kw)

    if not path.exists():
        return inv("POLICY_MISSING", "HOLD 정책파일 없음")
    try:
        policy = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as e:
        return inv("POLICY_MALFORMED", f"정책 JSON 파싱 실패: {type(e).__name__}")
    if not isinstance(policy, dict) or policy.get("schemaVersion") != SCHEMA_VERSION:
        return inv("POLICY_SCHEMA_MISMATCH", "schemaVersion 불일치",
                   policy=policy if isinstance(policy, dict) else None)
    computed = body_sha256(policy)
    if policy.get("policySha256") != computed:
        return inv("POLICY_SELF_HASH_MISMATCH",
                   "정책 본문과 policySha256 불일치 — 손상 또는 수기 편집",
                   policy=policy, computed=computed)
    if computed != expected:
        return inv("POLICY_UNAUTHORIZED_CHANGE",
                   "정책이 코드에 고정된 hash 와 다름 — 승인 없는 변경(HOLD 무단 해제 포함) 거부",
                   policy=policy, computed=computed)
    state = policy.get("state")
    if state == "HOLD":
        return _verdict(DECISION_HOLD, "HOLD_POLICY_IN_FORCE", "의도적 HOLD 정책 유효",
                        path=path, expected=expected, policy=policy, computed=computed)
    if state == "RESUMED":
        appr = policy.get("unholdApproval") or {}
        if appr.get("approvedBy") == "FOUNDER" and appr.get("approvedAt") and appr.get("reference"):
            return _verdict(DECISION_UNHELD, "RESUMED_WITH_FOUNDER_APPROVAL", "Founder 승인된 재개",
                            path=path, expected=expected, policy=policy, computed=computed)
        return inv("POLICY_UNHOLD_NOT_APPROVED", "HOLD 해제에 Founder 승인 근거 없음",
                   policy=policy, computed=computed)
    return inv("POLICY_STATE_INVALID", f"알 수 없는 state={state!r}",
               policy=policy, computed=computed)


def _policy(gate: dict) -> dict:
    return gate.get("policy") or {}


def hold_report(phase: str, date_iso: str, gate: dict, **extra) -> dict:
    """HOLD 중 각 단계의 표준 보고. 거래·네트워크·로그인·장부 변경이 전부 0 임을 명시한다."""
    p = _policy(gate)
    incident = list(p.get("incidentMissedSignalDates") or [])
    r = {
        "status": STATUS_EXPECTED_HOLD, "phase": phase,
        "date": date_iso, "scheduledSignalDate": date_iso,
        "reasonClass": REASON_CLASS, "magicPaperLaneState": LANE_STATE, "state": "HOLD",
        "holdClassification": HOLD_CLASSIFICATION,
        "lastNormalTradeDate": p.get("lastNormalTradeDate"),
        "lastNormalSequence": p.get("lastNormalSequence"),
        "incidentMissedSignalDates": incident,
        "incidentMissedSignalCount": len(incident),
        "holdEffectiveAt": p.get("effectiveAt"),
        "firstIntentionalHoldSignalDate": p.get("firstIntentionalHoldSignalDate"),
        "holdPolicySha256": gate.get("computedSha256"),
        "signalGenerated": False, "rankingCreated": False,
        "pykrxImported": False, "webLoginAttemptCount": 0,
        "credentialReadCount": 0, "networkRequestCount": 0,
        "canonicalAppendCount": 0, "newLotCount": 0, "newBuyCount": 0, "newSellCount": 0,
        "noFakeTrade": True, "productionWriteCount": 0, "canonicalChanged": False,
        "publicCopyCount": 0,
        # 의도적 HOLD 일은 missed-run 이 아니다(사고 누락일과 구분).
        "missedRunRecorded": False, "missedRunReason": "INTENTIONAL_HOLD_IS_NOT_A_MISSED_RUN",
        "founderAction": "NONE", "founderActionDisplay": "없음 — 의도적 HOLD",
        "nextSingleTask": "NONE", "realMoneyApproved": False, "paperOnly": True,
        "resumeCondition": list(p.get("resumeCondition") or []),
        "createdAt": now_kst_iso(),
    }
    r.update(extra)
    return r


def invalid_report(phase: str, date_iso: str, gate: dict, **extra) -> dict:
    """정책 손상·무단 변경 — fail-closed. 기존 실행으로 넘어가지 않았다는 사실을 남긴다."""
    r = {
        "status": "BLOCKED", "phase": phase, "blockedCode": BLOCKED_HOLD_POLICY_INVALID,
        "date": date_iso, "holdPolicyCode": gate.get("code"), "reason": gate.get("reason"),
        "holdPolicyPath": gate.get("policyPath"),
        "originalLogicExecuted": False, "noFakeTrade": True, "productionWriteCount": 0,
        "canonicalChanged": False, "publicCopyCount": 0, "autoStopped": True,
        "founderAction": "HOLD 정책파일 손상/무단 변경 원인 확인",
        "realMoneyApproved": False, "paperOnly": True, "createdAt": now_kst_iso(),
    }
    r.update(extra)
    return r


def emit(*, phase: str, date_iso: str, gate: dict, report_path, write_json,
         as_json: bool = False, extra: dict | None = None) -> int:
    """Signal / Dry Run 공용: 보고를 쓰고 exit code 를 돌려준다."""
    if gate.get("decision") == DECISION_HOLD:
        r, rc = hold_report(phase, date_iso, gate, **(extra or {})), EXIT_HOLD_OK
    else:
        r, rc = invalid_report(phase, date_iso, gate), EXIT_POLICY_INVALID
    write_json(report_path, r)
    if as_json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        tag = r.get("blockedCode") or r.get("reasonClass")
        print(f"[{phase} {date_iso}] {r['status']} {tag} "
              f"lastNormal={r.get('lastNormalTradeDate')} webLogin=0 signalGenerated=False")
    return rc


def emit_status(*, date_iso: str, gate: dict, reports_dir, write_json, canonical_summary: dict,
                as_json: bool = False) -> int:
    """Status 전용: 운영 실패(BLOCKED)가 아니라 의도된 HOLD(WAIT) 로 요약한다."""
    p = _policy(gate)
    if gate.get("decision") == DECISION_HOLD:
        s = hold_report("STATUS", date_iso, gate)
        s.update({
            "overall": "WAIT", "operationalState": "HOLD",
            "nextAction": "NO_ACTION_INTENTIONAL_HOLD",
            "nextManualHint": (f"의도적 HOLD — 마지막 정상 거래일 {p.get('lastNormalTradeDate')} · "
                               "신규 signal·거래 없음 · Founder 행동 없음"),
            "currentTradeDate": None, "newSignal": False, "newAction": False,
            "canonical": canonical_summary, "signal": None, "dryRun": None,
        })
        rc = EXIT_HOLD_OK
    else:
        s = invalid_report("STATUS", date_iso, gate)
        s.update({"overall": "BLOCKED", "operationalState": "HOLD_POLICY_INVALID",
                  "nextAction": BLOCKED_HOLD_POLICY_INVALID,
                  "nextManualHint": f"HOLD 정책 무효({gate.get('code')}) — 정책파일 원인 확인",
                  "canonical": canonical_summary})
        rc = EXIT_POLICY_INVALID
    write_json(Path(reports_dir) / f"daily-status-{date_iso}.json", s)
    md = [f"전체 판정: {s['overall']}", "",
          f"# 와바바 마법공식 일일 상태 — {date_iso}", "",
          f"- 운영 상태: {s['operationalState']}",
          f"- 다음 조치: {s['nextAction']}",
          f"- {s['nextManualHint']}",
          f"- 마지막 정상 거래일: {p.get('lastNormalTradeDate')} · seq {p.get('lastNormalSequence')}",
          f"- 사고 누락일: {', '.join(p.get('incidentMissedSignalDates') or []) or '없음'}",
          f"- canonical seq: {(canonical_summary or {}).get('officialSequence')}",
          "- 신규 signal 0 · 신규 거래 0 · 실주문 0 · 브로커 0", ""]
    (Path(reports_dir) / f"daily-status-{date_iso}.md").write_text("\n".join(md), encoding="utf-8")
    if as_json:
        print(json.dumps(s, ensure_ascii=False, indent=2))
    else:
        print(f"[STATUS {date_iso}] {s['overall']} → {s['nextAction']}")
        print(f"  {s['nextManualHint']}")
    return rc


def apply_hold_result(*, date_iso: str, gate: dict, canonical_path, policy_version: str,
                      archive_recipient: str, alert_recipient: str):
    """Auto Apply 전용. canonical 은 **읽기만** 한다(sha before == after 를 기록)."""
    c, sha = {}, None
    try:
        b = Path(canonical_path).read_bytes()
        c, sha = json.loads(b), hashlib.sha256(b).hexdigest()
    except (OSError, ValueError):
        pass
    counts = {
        "officialSequence": c.get("officialSequence"),
        "officialTradingDayIndex": c.get("officialTradingDayIndex"),
        "officialAvailableCash": c.get("officialAvailableCash"),
        "itemLots": len(c.get("itemLots") or []),
        "buyLedgerCount": len(c.get("buyLedger") or []),
        "sellLedgerCount": len(c.get("sellLedger") or []),
        "canonicalSha256Before": sha, "canonicalSha256After": sha,
    }
    common = {"policyVersion": policy_version, "targetExecutionDate": None,
              "appliedDates": [], "remainingPendingDates": [], "pendingDates": [],
              "blockedCodes": [], "realOrderCount": 0, "brokerApiCallCount": 0,
              "smtpCallCount": 0, "emailSent": False, "deployCount": 0}
    if gate.get("decision") == DECISION_HOLD:
        r = hold_report("AUTO_APPLY", date_iso, gate, **counts, **common)
        r.update({
            "status": APPLY_SKIPPED_EXPECTED_HOLD, "verdict": "WAIT",
            "applyStatus": APPLY_SKIPPED_EXPECTED_HOLD, "readiness": "NOT_APPLICABLE_HOLD",
            # 의도된 HOLD 는 founder 예외알림이 아니다 → archive 로만 기록한다.
            "routing": {"to": [archive_recipient], "purpose": "INTENTIONAL_HOLD_ARCHIVE",
                        "founderNotified": False, "founderRecipientCount": 0},
            "founderNotified": False, "founderRecipientCount": 0,
            "reason": ("의도적 HOLD — 공식 당일 source·업종분류 부재(R3 NO_GO). "
                       "장부 반영 0 · 실주문 0 · 과거 거래 소급 0"),
        })
        return r, EXIT_HOLD_OK
    r = invalid_report("AUTO_APPLY", date_iso, gate, **counts, **common)
    r.update({
        "status": BLOCKED_HOLD_POLICY_INVALID, "verdict": "BLOCKED",
        "routing": {"to": [alert_recipient], "purpose": "EXCEPTION_ALERT",
                    "founderNotified": True, "founderRecipientCount": 1},
        "founderNotified": True, "founderRecipientCount": 1,
    })
    return r, EXIT_POLICY_INVALID


def aggregate_status(gate: dict, *, current_signal_generated: bool = False,
                     current_auto_apply_status: str | None = None) -> dict:
    """내부 투명성 요약(§11). HOTG·Founder 보고가 KNOWN_INTENTIONAL_HOLD 로 분류할 수 있는 필드."""
    p = _policy(gate)
    hold = gate.get("decision") == DECISION_HOLD
    incident = list(p.get("incidentMissedSignalDates") or [])
    return {
        "magicPaperLaneState": LANE_STATE if hold else (
            "HOLD_POLICY_INVALID" if gate.get("decision") == DECISION_INVALID else "RESUMED"),
        "reasonClass": REASON_CLASS if hold else gate.get("code"),
        "holdClassification": HOLD_CLASSIFICATION if hold else None,
        "lastNormalTradeDate": p.get("lastNormalTradeDate"),
        "lastNormalSeq": p.get("lastNormalSequence"),
        "incidentMissedSignalDates": incident,
        "incidentMissedSignalCount": len(incident),
        "holdEffectiveAt": p.get("effectiveAt"),
        "firstIntentionalHoldSignalDate": p.get("firstIntentionalHoldSignalDate"),
        "currentSignalGenerated": current_signal_generated,
        "currentAutoApplyStatus": current_auto_apply_status,
        "webLoginAttemptCount": 0, "pykrxImported": False,
        "canonicalAppendCount": 0, "newLotCount": 0, "newBuyCount": 0, "newSellCount": 0,
        "publicDisclosureStatus": p.get("publicDisclosureStatus"),
        "founderAction": "NONE" if hold else "HOLD 정책 원인 확인",
        "nextSingleTask": "NONE",
        "realMoneyApproved": False, "paperOnly": True,
    }


if __name__ == "__main__":
    print(json.dumps({k: v for k, v in evaluate().items() if k != "policy"},
                     ensure_ascii=False, indent=2))
