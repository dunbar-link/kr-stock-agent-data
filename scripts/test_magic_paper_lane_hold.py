#!/usr/bin/env python3
"""R4 — 마법공식 paper lane 의도적 HOLD 게이트 회귀 테스트.

WABABA-MAGIC-FORMULA-PAPER-LANE-HOLD-ENFORCEMENT-AND-TRANSPARENCY-R4 (SOURCE: R3 21fdced · NO_GO)

고정하는 것
  [1] HOLD 정책 판정 — 정상 HOLD · 누락 · 손상 · schema · 자기 hash · 무단 변경 · 무승인 해제 ·
      Founder 승인 해제 · 알 수 없는 state · 판정기 예외 (전부 fail-closed 또는 명시 판정)
  [2] 정책 내용 — R3 근거 · 사고 누락일 09-07~09-10 · HOLD 일은 missed-run 아님 · 허용 플래그 false
  [3] 4개 진입점 HOLD 실행 — 자식 프로세스(socket 차단 + KRX_ID/KRX_PW 제거)에서 실제 main():
      exit 0 · EXPECTED_HOLD_NO_ACTION · pykrx/build_market_snapshot*/build_magic_signal_package 미적재 ·
      연결 시도 0 · signal 패키지 0 · canonical 불변 · 운영 reports 미접촉
  [4] 정책 무효 → BLOCKED_HOLD_POLICY_INVALID(exit 3) · 기존 로직 미실행
  [5] UNHELD(Founder 승인 재개) → 기존 로직 그대로 (fake 주입 비회귀)
  [6] Auto Publish 선행 gate — HOLD: SKIPPED_EXPECTED_HOLD(exit 10) · 무효: BLOCKED · 비거래일: 기존 SKIP
  [7] 오전 통합보고 — HOLD 는 KNOWN_INTENTIONAL_HOLD, 발효 후 이상만 BLOCKED, R4 자연 관찰 판정
  [8] 정적 배선   [9] HOLD 중 장부 불변
  [10] (--r4-closeout) 스케줄러 정의·전략 코드·pykrx·public repo 불변 + 결과 보고서 기록

사용
  python scripts/test_magic_paper_lane_hold.py                 # hermetic (전체 suite 포함)
  python scripts/test_magic_paper_lane_hold.py --r4-closeout    # + 운영 불변 확인 + 보고서 기록

네트워크 0 · 로그인 0 · 운영 reports/ 미접촉(임시 폴더) · canonical 은 읽기만 한다.
"""
from __future__ import annotations

import argparse
import ast
import contextlib
import copy
import hashlib
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── 이 프로세스도 네트워크 차단: 판정 로직이 몰래 연결하면 즉시 드러난다 ──
_CONNECTS: list = []


def _blocked(*_a, **_k):
    _CONNECTS.append(1)
    raise OSError("R4 test network guard: outbound connection blocked")


socket.socket.connect = lambda self, *a, **k: _blocked()
socket.create_connection = lambda *a, **k: _blocked()

SCR = Path(__file__).resolve().parent
ROOT = SCR.parent
sys.path.insert(0, str(SCR))

import magic_daily_common as C  # noqa: E402
import magic_paper_lane_hold as H  # noqa: E402

RISKY = ("pykrx", "build_market_snapshot", "build_market_snapshot_fast", "build_magic_signal_package")
NET_RISKY = RISKY[:3]
KST = timezone(timedelta(hours=9))
TRADING = "2026-09-11"          # 금 — 첫 의도적 HOLD 거래일(예상)
SATURDAY = "2026-09-12"
INCIDENT = ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10"]
BASE_COUNTS = {"itemLots": 430, "buyLedger": 430, "sellLedger": 0, "batches": 43, "missedRuns": 5}
PUBLIC_REPO = Path("C:/work/kr-stock-agent")
RESULTS: list = []

RUNS = (
    ("signal", "magic_daily_signal", ["--signal-date", TRADING], f"signal-{TRADING}.json"),
    ("dry_run", "magic_daily_dry_run", [], "dry-run-*.json"),
    ("status", "magic_daily_status", [], "daily-status-*.json"),
    ("auto_apply", "magic_daily_auto_apply", ["--date", TRADING], f"auto-apply-{TRADING}.json"),
)

# 자식 프로세스 드라이버: socket 차단 → 운영 경로를 임시 폴더로 격리 → 실제 main() 실행 → 결과 한 줄.
DRIVER = r'''
import json, os, pathlib, socket, sys
_att = []
def _blk(*a, **k):
    _att.append(1)
    raise OSError("R4 driver network guard")
socket.socket.connect = lambda self, *a, **k: _blk()
socket.create_connection = lambda *a, **k: _blk()
scripts, mode, entry, tmp = sys.argv[1:5]
argv = sys.argv[5:]
sys.path.insert(0, scripts)
tmp = pathlib.Path(tmp)
import magic_daily_common as C
C.REPORTS_DIR = tmp / "reports"
C.LOGS_DIR = tmp / "logs"
C.TEMP_ROOT = tmp / "signal-root"
mod = __import__(entry)
if entry == "magic_daily_auto_apply":
    mod.DURABLE_STATUS_JSON = tmp / "durable" / "magic-auto-apply-status-latest.json"
    mod.DURABLE_STATUS_MD = tmp / "durable" / "magic-auto-apply-status-latest.md"
if mode == "invalid":
    import magic_paper_lane_hold as H
    mod._magic_hold_gate = lambda: H.evaluate(tmp / "missing-policy.json")
try:
    rc = mod.main(argv)
except SystemExit as e:
    rc = e.code
risky = [m for m in ("pykrx", "build_market_snapshot", "build_market_snapshot_fast",
                     "build_magic_signal_package") if m in sys.modules]
print("R4DRIVER " + json.dumps({"rc": rc, "connect": len(_att), "risky": risky,
                                "krxIdPresent": bool(os.environ.get("KRX_ID")),
                                "krxPwPresent": bool(os.environ.get("KRX_PW"))}))
'''


def ck(name, cond, detail=""):
    ok = bool(cond)
    RESULTS.append({"name": name, "pass": ok, "detail": "" if ok else str(detail)[:300]})
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"  -> {str(detail)[:300]}"))


def _sha(p):
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()
    except (OSError, TypeError):
        return None


def _read(p):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, TypeError):
        return None


def _one(folder: Path, pattern: str):
    hits = sorted(folder.glob(pattern)) if folder.exists() else []
    return hits[0] if len(hits) == 1 else None


def _snap(folder: Path) -> dict:
    try:
        return {p.name: p.stat().st_mtime_ns for p in folder.glob("*")} if folder.exists() else {}
    except OSError:
        return {"error": True}


def _quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def _write_policy(path: Path, pol: dict, *, rehash=True, bom=False, indent=2) -> dict:
    pol = copy.deepcopy(pol)
    if rehash:
        pol["policySha256"] = H.body_sha256(pol)
    txt = json.dumps(pol, ensure_ascii=False, indent=indent)
    path.write_bytes((b"\xef\xbb\xbf" if bom else b"") + txt.encode("utf-8"))
    return pol


def _next_trading_day(d: str) -> str:
    x = datetime.fromisoformat(d).date() + timedelta(days=1)
    while not C.is_krx_trading_day(x.isoformat()):
        x += timedelta(days=1)
    return x.isoformat()


def _drive(mode: str, entry: str, tmp: Path, *argv: str) -> dict:
    drv = tmp.parent / "r4_hold_driver.py"
    if not drv.exists():
        drv.write_text(DRIVER, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k not in ("KRX_ID", "KRX_PW")}
    env.update(PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    p = subprocess.run([sys.executable, str(drv), str(SCR), mode, entry, str(tmp), *argv],
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       env=env, cwd=str(ROOT), timeout=180)
    line = next((x for x in reversed(p.stdout.splitlines()) if x.startswith("R4DRIVER ")), None)
    if not line:
        return {"rc": None, "error": ((p.stderr or "") + (p.stdout or ""))[-500:]}
    return json.loads(line[len("R4DRIVER "):])


# ═══════════════════════ [1] 정책 판정 ═══════════════════════
def t_policy_eval(base: Path):
    print("[1] HOLD 정책 판정 (fail-closed)")
    base.mkdir(parents=True, exist_ok=True)
    real = H.evaluate()
    pol = real.get("policy") or {}
    ck("실제 정책 = HOLD (HOLD_POLICY_IN_FORCE)",
       (real["decision"], real["code"]) == (H.DECISION_HOLD, "HOLD_POLICY_IN_FORCE"), real.get("code"))
    ck("코드 고정 hash == 정책 자기 hash == 계산 hash",
       H.EXPECTED_POLICY_SHA256 == real.get("computedSha256") == pol.get("policySha256"),
       (H.EXPECTED_POLICY_SHA256, real.get("computedSha256")))
    ck("고정 hash 가 placeholder 아님(64 hex)",
       re.fullmatch(r"[0-9a-f]{64}", H.EXPECTED_POLICY_SHA256 or "") is not None)

    r = H.evaluate(base / "nope.json")
    ck("정책파일 없음 → INVALID POLICY_MISSING", (r["decision"], r["code"]) == ("INVALID", "POLICY_MISSING"), r["code"])
    (base / "malformed.json").write_text("{ not json", encoding="utf-8")
    r = H.evaluate(base / "malformed.json")
    ck("JSON 손상 → POLICY_MALFORMED", r["code"] == "POLICY_MALFORMED", r["code"])
    (base / "list.json").write_text("[1, 2]", encoding="utf-8")
    r = H.evaluate(base / "list.json")
    ck("객체 아님 → POLICY_SCHEMA_MISMATCH", r["code"] == "POLICY_SCHEMA_MISMATCH", r["code"])

    q = copy.deepcopy(pol); q["schemaVersion"] = "other/v0"
    _write_policy(base / "schema.json", q)
    r = H.evaluate(base / "schema.json")
    ck("schemaVersion 불일치 → POLICY_SCHEMA_MISMATCH", r["code"] == "POLICY_SCHEMA_MISMATCH", r["code"])

    q = copy.deepcopy(pol); q["lastNormalSequence"] = 44
    _write_policy(base / "selfhash.json", q, rehash=False)
    r = H.evaluate(base / "selfhash.json")
    ck("본문 수기 편집(자기 hash 불일치) → POLICY_SELF_HASH_MISMATCH", r["code"] == "POLICY_SELF_HASH_MISMATCH", r["code"])

    q = copy.deepcopy(pol); q["state"] = "RESUMED"
    _write_policy(base / "unauth.json", q)
    r = H.evaluate(base / "unauth.json")
    ck("정책파일만 고친 무단 해제 → POLICY_UNAUTHORIZED_CHANGE", r["code"] == "POLICY_UNAUTHORIZED_CHANGE", r["code"])
    q = copy.deepcopy(pol); q["signalGenerationAllowed"] = True
    _write_policy(base / "unauth-hold.json", q)
    r = H.evaluate(base / "unauth-hold.json")
    ck("HOLD 유지여도 고정 hash 와 다르면 거부", r["code"] == "POLICY_UNAUTHORIZED_CHANGE", r["code"])

    q = copy.deepcopy(pol); q["state"] = "RESUMED"
    qq = _write_policy(base / "resumed-noappr.json", q)
    r = H.evaluate(base / "resumed-noappr.json", expected_sha=qq["policySha256"])
    ck("RESUMED + 승인 근거 없음 → POLICY_UNHOLD_NOT_APPROVED", r["code"] == "POLICY_UNHOLD_NOT_APPROVED", r["code"])
    q = copy.deepcopy(pol); q["state"] = "RESUMED"
    q["unholdApproval"] = {"approvedBy": "CLAUDE", "approvedAt": "2026-09-30T10:00:00+09:00", "reference": "X"}
    qq = _write_policy(base / "resumed-notfounder.json", q)
    r = H.evaluate(base / "resumed-notfounder.json", expected_sha=qq["policySha256"])
    ck("RESUMED + Founder 아닌 승인자 → 거부", r["code"] == "POLICY_UNHOLD_NOT_APPROVED", r["code"])
    q = copy.deepcopy(pol); q["state"] = "RESUMED"
    q["unholdApproval"] = {"approvedBy": "FOUNDER", "approvedAt": "2026-09-30T10:00:00+09:00", "reference": "TASK-X"}
    qq = _write_policy(base / "resumed-ok.json", q)
    r = H.evaluate(base / "resumed-ok.json", expected_sha=qq["policySha256"])
    ck("RESUMED + Founder 승인 + 고정 hash 동시 갱신 → UNHELD",
       (r["decision"], r["code"]) == ("UNHELD", "RESUMED_WITH_FOUNDER_APPROVAL"), r["code"])
    q = copy.deepcopy(pol); q["state"] = "PAUSED?"
    qq = _write_policy(base / "weird.json", q)
    r = H.evaluate(base / "weird.json", expected_sha=qq["policySha256"])
    ck("알 수 없는 state → POLICY_STATE_INVALID", r["code"] == "POLICY_STATE_INVALID", r["code"])

    _write_policy(base / "oneline-bom.json", pol, rehash=False, bom=True, indent=None)
    r = H.evaluate(base / "oneline-bom.json")
    ck("줄바꿈·들여쓰기·BOM 이 달라도 같은 본문이면 HOLD(canonical hash)", r["decision"] == "HOLD", r["code"])

    orig = H._evaluate
    try:
        def boom(*_a, **_k):
            raise RuntimeError("boom")
        H._evaluate = boom
        r = H.evaluate()
        ck("판정기 내부 예외 → INVALID POLICY_EVALUATION_ERROR",
           (r["decision"], r["code"]) == ("INVALID", "POLICY_EVALUATION_ERROR"), r)
    finally:
        H._evaluate = orig


# ═══════════════════════ [2] 정책 내용 ═══════════════════════
def t_policy_content():
    print("[2] 정책 내용 (R3 근거 · 사고 누락일 · HOLD 경계)")
    pol = H.evaluate().get("policy") or {}
    ck("state=HOLD · reasonClass=HOLD_NO_CANONICAL_SAME_DAY_SOURCE",
       (pol.get("state"), pol.get("reasonClass")) == ("HOLD", "HOLD_NO_CANONICAL_SAME_DAY_SOURCE"))
    ck("근거 = R3 21fdced OFFICIAL_SOURCE_REPLACEMENT_NO_GO",
       (pol.get("sourceCommit"), pol.get("sourceDecision")) == ("21fdced", "OFFICIAL_SOURCE_REPLACEMENT_NO_GO"))
    ck("마지막 정상 거래 2026-09-04 · seq 43",
       (pol.get("lastNormalTradeDate"), pol.get("lastNormalSequence")) == ("2026-09-04", 43))
    inc = pol.get("incidentMissedSignalDates") or []
    ck("사고 누락일 = 09-07~09-10 4건(순서 고정)", inc == INCIDENT, inc)
    ck("사고 누락일은 전부 실제 거래일", all(C.is_krx_trading_day(d) for d in inc))
    recs = pol.get("incidentMissedRecords") or []
    ck("누락 기록 4건 · 날짜 일치", [x.get("signalDate") for x in recs] == INCIDENT)
    keys = [x.get("idempotencyKey") for x in recs]
    ck("idempotencyKey 유일 · date+reason 형식",
       len(set(keys)) == 4 and all(k == f"{x.get('signalDate')}+{pol.get('incidentMissedReason')}"
                                   for k, x in zip(keys, recs)), keys)
    ck("누락 기록: 거래 생성 0 · canonical append 0 · PIT 증거 없음",
       all(x.get("tradeCreated") is False and x.get("canonicalAppend") is False
           and x.get("immutablePITEvidencePresent") is False for x in recs))
    ck("사유 = MISSED_RUN_NO_VALID_PIT_EVIDENCE", pol.get("incidentMissedReason") == "MISSED_RUN_NO_VALID_PIT_EVIDENCE")
    first = str(pol.get("firstIntentionalHoldSignalDate") or "")
    ck("첫 의도적 HOLD 일 > 마지막 사고 누락일 · 실제 거래일",
       first > INCIDENT[-1] and C.is_krx_trading_day(first), first)
    ck("HOLD 일은 사고 누락일 목록에 없음(missed-run 아님)", all(d < first for d in inc))
    try:
        eff = datetime.fromisoformat(str(pol.get("effectiveAt")))
        ok = eff.utcoffset() == timedelta(hours=9) and eff.date().isoformat() <= first
    except (TypeError, ValueError):
        ok = False
    ck("effectiveAt = KST ISO · 첫 HOLD 일 이전", ok, pol.get("effectiveAt"))
    flags = ("signalGenerationAllowed", "autoApplyAllowed", "historicalReplayAllowed", "webLoginAllowed",
             "credentialChangeRequired", "publicPublishAllowed", "realMoneyApproved")
    ck("허용 플래그 전부 false", all(pol.get(f) is False for f in flags), {f: pol.get(f) for f in flags})
    ck("paperOnly · Founder 승인 없이는 해제 불가",
       pol.get("paperOnly") is True and pol.get("unholdRequiresFounderApproval") is True)
    ck("공개 공지는 초안만", pol.get("publicDisclosureStatus") == "DRAFT_ONLY_NOT_APPROVED")
    ck("missed-run 기록 방식 = 정책+보고서(장부 무변경)",
       pol.get("missedRunRecordingMethod") == "HOLD_POLICY_AND_R4_REPORT_ONLY")
    ck("재개 조건·전제 명시", len(pol.get("resumeCondition") or []) >= 1 and len(pol.get("resumePreconditions") or []) >= 3)
    obs = pol.get("naturalObservation") or {}
    ck("자연 관찰 ID = R4:NATURAL-HOLD-CLOSEOUT:<첫 HOLD 일>",
       obs.get("id") == f"WABABA-MAGIC-FORMULA-PAPER-LANE-HOLD-ENFORCEMENT-R4:NATURAL-HOLD-CLOSEOUT:{first}", obs.get("id"))
    ck("자연 관찰: owner · due 17:10 · Founder 행동 NONE · 공유 registry 미기록 명시",
       obs.get("owner") == "WABABA PROJECT" and obs.get("due") == f"{first}T17:10:00+09:00"
       and obs.get("founderAction") == "NONE"
       and obs.get("sharedFutureResumeRegistry") == "NOT_WRITTEN_AUTOMATION_REPOSITORY_READ_ONLY", obs)


# ═══════════════════════ [3][4] 진입점 실행 ═══════════════════════
COMMON_ZERO = ("webLoginAttemptCount", "credentialReadCount", "networkRequestCount", "canonicalAppendCount",
               "newLotCount", "newBuyCount", "newSellCount", "productionWriteCount", "publicCopyCount")


def _hold_common(tag: str, rep: dict, pol: dict):
    ck(f"{tag}: 0 카운터 전부 0 (로그인·자격증명·네트워크·append·lot·매수·매도·write·public)",
       all(rep.get(k) == 0 for k in COMMON_ZERO), {k: rep.get(k) for k in COMMON_ZERO})
    ck(f"{tag}: signal·ranking 미생성 · pykrx 미적재 · canonical 불변",
       rep.get("signalGenerated") is False and rep.get("rankingCreated") is False
       and rep.get("pykrxImported") is False and rep.get("canonicalChanged") is False)
    ck(f"{tag}: KNOWN_INTENTIONAL_HOLD · lane 상태 · 마지막 정상 거래",
       rep.get("holdClassification") == H.HOLD_CLASSIFICATION and rep.get("magicPaperLaneState") == H.LANE_STATE
       and rep.get("lastNormalTradeDate") == pol.get("lastNormalTradeDate")
       and rep.get("lastNormalSequence") == pol.get("lastNormalSequence"))
    ck(f"{tag}: HOLD 일은 missed-run 아님 · 사고 누락일 4건 표기",
       rep.get("missedRunRecorded") is False and rep.get("incidentMissedSignalCount") == 4)
    ck(f"{tag}: Founder 행동 없음 · 다음 작업 없음 · 실계좌 미승인",
       rep.get("founderAction") == "NONE" and rep.get("nextSingleTask") == "NONE"
       and rep.get("realMoneyApproved") is False and rep.get("paperOnly") is True)


def t_entrypoints(base: Path):
    print("[3] 진입점 HOLD 실행 — 자식 프로세스 · socket 차단 · KRX 자격증명 제거")
    import magic_daily_auto_apply as AA
    base.mkdir(parents=True, exist_ok=True)
    pol = H.evaluate().get("policy") or {}
    canon_sha = _sha(C.CANONICAL_PATH)
    canon = _read(C.CANONICAL_PATH) or {}
    real_reports = _snap(C.REPORTS_DIR)
    durable = ROOT / "reports" / "magic-auto-apply-status-latest.json"
    durable_sha = _sha(durable)
    lock_before = AA.LOCK_PATH.exists()

    for tag, entry, argv, pat in RUNS:
        tmp = base / f"hold-{tag}"
        tmp.mkdir(parents=True, exist_ok=True)
        r = _drive("hold", entry, tmp, *argv)
        ck(f"{tag}: exit 0 (운영 실패 Result 2 아님)", r.get("rc") == H.EXIT_HOLD_OK, r)
        ck(f"{tag}: 위험 모듈 미적재 (pykrx·build_market_snapshot*·build_magic_signal_package)",
           r.get("risky") == [], r.get("risky"))
        ck(f"{tag}: 네트워크 연결 시도 0", r.get("connect") == 0, r.get("connect"))
        ck(f"{tag}: KRX_ID/KRX_PW 미전달", r.get("krxIdPresent") is False and r.get("krxPwPresent") is False, r)
        path = _one(tmp / "reports", pat)
        rep = _read(path) if path else None
        ck(f"{tag}: HOLD 보고 1건 기록", rep is not None,
           sorted(p.name for p in (tmp / "reports").glob("*")) if (tmp / "reports").exists() else "no reports")
        rep = rep or {}
        _hold_common(tag, rep, pol)
        ck(f"{tag}: signal 패키지·dry-run 로그 생성 0",
           not (tmp / "signal-root").exists() and not (tmp / "logs").exists())
        if tag == "signal":
            ck("signal: EXPECTED_HOLD_NO_ACTION · reasonClass · signalAsOfDate",
               (rep.get("status"), rep.get("reasonClass"), rep.get("signalAsOfDate"))
               == (H.STATUS_EXPECTED_HOLD, H.REASON_CLASS, TRADING), rep.get("status"))
        elif tag == "dry_run":
            ck("dry_run: EXPECTED_HOLD_NO_ACTION · 상류 HOLD 사유 · signal 패키지 불요",
               (rep.get("status"), rep.get("reasonClass"), rep.get("executionDate"), rep.get("signalPackageRequired"))
               == (H.STATUS_EXPECTED_HOLD, H.DOWNSTREAM_REASON, None, False), rep.get("status"))
        elif tag == "status":
            ck("status: WAIT · NO_ACTION_INTENTIONAL_HOLD (운영 실패 아님)",
               (rep.get("overall"), rep.get("nextAction"), rep.get("operationalState"), rep.get("status"))
               == ("WAIT", "NO_ACTION_INTENTIONAL_HOLD", "HOLD", H.STATUS_EXPECTED_HOLD),
               (rep.get("overall"), rep.get("nextAction")))
            md = _one(tmp / "reports", "daily-status-*.md")
            first_line = md.read_text(encoding="utf-8").splitlines()[0] if md else ""
            ck("status: MD 첫 줄 = 전체 판정: WAIT", first_line == "전체 판정: WAIT", first_line)
        else:
            ck("auto_apply: SKIPPED_EXPECTED_HOLD · verdict WAIT · readiness N/A",
               (rep.get("status"), rep.get("verdict"), rep.get("readiness"))
               == (H.APPLY_SKIPPED_EXPECTED_HOLD, "WAIT", "NOT_APPLICABLE_HOLD"), (rep.get("status"), rep.get("verdict")))
            ck("auto_apply: canonical sha before == after == 실제 장부",
               rep.get("canonicalSha256Before") == rep.get("canonicalSha256After") == canon_sha)
            ck("auto_apply: seq·lot 수 = 실제 장부(읽기만)",
               (rep.get("officialSequence"), rep.get("itemLots"))
               == (canon.get("officialSequence"), len(canon.get("itemLots") or [])),
               (rep.get("officialSequence"), rep.get("itemLots")))
            rt = rep.get("routing") or {}
            ck("auto_apply: Founder 알림 0 · archive 기록만",
               rep.get("founderNotified") is False and rt.get("purpose") == "INTENTIONAL_HOLD_ARCHIVE"
               and rt.get("to") == [AA.OPS_ARCHIVE_RECIPIENT], rt)
            ck("auto_apply: pending 0 · 실주문·브로커·SMTP·메일 0",
               rep.get("remainingPendingDates") == [] and rep.get("realOrderCount") == 0
               and rep.get("brokerApiCallCount") == 0 and rep.get("smtpCallCount") == 0
               and rep.get("emailSent") is False)
            dj = _read(tmp / "durable" / "magic-auto-apply-status-latest.json") or {}
            dm = tmp / "durable" / "magic-auto-apply-status-latest.md"
            ck("auto_apply: 08:40 입력(durable status) = SKIPPED_EXPECTED_HOLD / WAIT / KNOWN_INTENTIONAL_HOLD",
               (dj.get("status"), dj.get("verdict"), dj.get("holdClassification"))
               == (H.APPLY_SKIPPED_EXPECTED_HOLD, "WAIT", H.HOLD_CLASSIFICATION), (dj.get("status"), dj.get("verdict")))
            ck("auto_apply: durable MD 첫 줄 = 전체 판정: WAIT",
               dm.exists() and dm.read_text(encoding="utf-8").splitlines()[0] == "전체 판정: WAIT")

    print("[4] 정책 무효 → fail-closed (BLOCKED_HOLD_POLICY_INVALID · exit 3 · 기존 로직 미실행)")
    for tag, entry, argv, pat in RUNS:
        tmp = base / f"invalid-{tag}"
        tmp.mkdir(parents=True, exist_ok=True)
        r = _drive("invalid", entry, tmp, *argv)
        ck(f"{tag}: exit 3", r.get("rc") == H.EXIT_POLICY_INVALID, r)
        ck(f"{tag}: 무효여도 위험 모듈 미적재(HOLD 무시하고 기존 실행으로 넘어가지 않음)",
           r.get("risky") == [], r.get("risky"))
        ck(f"{tag}: 연결 시도 0", r.get("connect") == 0, r.get("connect"))
        path = _one(tmp / "reports", pat)
        rep = (_read(path) if path else None) or {}
        if tag == "status":
            ok = rep.get("overall") == "BLOCKED" and rep.get("blockedCode") == H.BLOCKED_HOLD_POLICY_INVALID
        elif tag == "auto_apply":
            ok = (rep.get("status") == H.BLOCKED_HOLD_POLICY_INVALID and rep.get("verdict") == "BLOCKED"
                  and (rep.get("routing") or {}).get("purpose") == "EXCEPTION_ALERT"
                  and rep.get("canonicalChanged") is False)
        else:
            ok = rep.get("status") == "BLOCKED" and rep.get("blockedCode") == H.BLOCKED_HOLD_POLICY_INVALID
        ck(f"{tag}: BLOCKED_HOLD_POLICY_INVALID 기록 · 기존 로직 미실행",
           ok and rep.get("originalLogicExecuted") is False and rep.get("holdPolicyCode") == "POLICY_MISSING",
           {k: rep.get(k) for k in ("status", "overall", "blockedCode", "holdPolicyCode", "originalLogicExecuted")})
        ck(f"{tag}: signal 패키지·로그 0", not (tmp / "signal-root").exists() and not (tmp / "logs").exists())

    ck("HOLD·무효 실행 전후 canonical sha 불변", _sha(C.CANONICAL_PATH) == canon_sha)
    ck("운영 durable status(08:40 입력) 미접촉", _sha(durable) == durable_sha)
    ck("운영 TEMP reports 미접촉", _snap(C.REPORTS_DIR) == real_reports)
    ck("운영 auto-apply lock 미생성", AA.LOCK_PATH.exists() == lock_before)


# ═══════════════════════ [5] UNHELD 비회귀 ═══════════════════════
def t_unheld(base: Path):
    print("[5] UNHELD(Founder 승인 재개) → 기존 로직 그대로 (fake 주입 · 네트워크 0)")
    import magic_daily_auto_apply as AA
    import magic_daily_dry_run as DR
    import magic_daily_signal as S
    import magic_daily_status as ST
    base.mkdir(parents=True, exist_ok=True)

    def unheld():
        return {"decision": "UNHELD"}

    saved = (C.REPORTS_DIR, S._magic_hold_gate, S.run_signal, DR._magic_hold_gate, DR.find_ready_package_for_today,
             ST._magic_hold_gate, ST.build_status, AA._magic_hold_gate, AA.run_auto_apply_catchup,
             AA.write_durable_status)
    seen: dict = {}
    try:
        C.REPORTS_DIR = base / "reports"
        S._magic_hold_gate = unheld
        S.run_signal = lambda d, now=None: seen.setdefault("signal", {"status": "READY", "phase": "SIGNAL", "date": d})
        ck("signal: UNHELD → run_signal 호출 · exit 0",
           _quiet(S.main, ["--signal-date", TRADING]) == 0 and "signal" in seen, seen)

        DR._magic_hold_gate = unheld
        DR.find_ready_package_for_today = lambda today_iso: seen.setdefault("dry", None)
        rc = _quiet(DR.main, [])
        ck("dry_run: UNHELD → 기존 패키지 탐색 경로(exit 2)", rc == 2 and "dry" in seen, rc)
        rep = _read(_one(C.REPORTS_DIR, "dry-run-*.json")) or {}
        ck("dry_run: 기존 BLOCKED_SIGNAL_PACKAGE_MISSING 유지", rep.get("blockedCode") == "BLOCKED_SIGNAL_PACKAGE_MISSING",
           rep.get("blockedCode"))

        ST._magic_hold_gate = unheld
        ST.build_status = lambda today: seen.setdefault("status", {
            "date": today, "overall": "PASS", "nextAction": "SIGNAL_READY", "nextManualHint": "fake",
            "canonical": {}, "signal": None, "dryRun": None, "createdAt": "fake"})
        ck("status: UNHELD → build_status 호출 · exit 0", _quiet(ST.main, []) == 0 and "status" in seen)

        AA._magic_hold_gate = unheld
        AA.run_auto_apply_catchup = lambda **kw: seen.setdefault("apply", {
            "status": "NO_ACTION_ALREADY_CURRENT", "verdict": "PASS", "date": TRADING, "targetExecutionDate": None,
            "canonicalChanged": False, "founderNotified": False, "remainingPendingDates": [], "appliedDates": []})
        AA.write_durable_status = lambda *a, **k: None
        ck("auto_apply: UNHELD → catch-up 경로 · exit 0",
           _quiet(AA.main, ["--date", TRADING]) == 0 and "apply" in seen)
    finally:
        (C.REPORTS_DIR, S._magic_hold_gate, S.run_signal, DR._magic_hold_gate, DR.find_ready_package_for_today,
         ST._magic_hold_gate, ST.build_status, AA._magic_hold_gate, AA.run_auto_apply_catchup,
         AA.write_durable_status) = saved


# ═══════════════════════ [6] publish gate ═══════════════════════
def t_publish_gate(base: Path):
    print("[6] Auto Publish 선행 gate")
    import audit_quarantine as AQ
    base.mkdir(parents=True, exist_ok=True)
    saved_marker = AQ.MARKER_PATH
    AQ.MARKER_PATH = base / "no-audit-quarantine.json"   # 운영 격리 marker 미접촉(기존 테스트와 같은 격리)
    import magic_publish_gate as G
    hold = H.evaluate()
    kw = dict(canonical=None, apply_status=None, lock_held=False, public_model_error=None)
    try:
        r = G.evaluate(today_iso=TRADING, hold_state=hold, **kw)
        ck("HOLD + 거래일 → SKIPPED_EXPECTED_HOLD (verdict PASS)",
           (r["decision"], r["verdict"]) == (G.SKIP_EXPECTED_HOLD, "PASS"), r["decision"])
        ck("HOLD skip: publish 대상일 없음 · write 0 · 실주문·브로커 0",
           r.get("publishTargetDate") is None and r["filesWritten"] == 0 and r["productionWriteCount"] == 0
           and r["realOrderCount"] == 0 and r["brokerApiCallCount"] == 0)
        ck("HOLD skip: lane 상태 · KNOWN_INTENTIONAL_HOLD 표기",
           r.get("magicPaperLaneState") == H.LANE_STATE and r.get("holdClassification") == H.HOLD_CLASSIFICATION)
        r = G.evaluate(today_iso=SATURDAY, hold_state=hold, **kw)
        ck("HOLD + 토요일 → 기존 SKIPPED_NON_TRADING_DAY 유지", r["decision"] == G.SKIP_NON_TRADING_DAY, r["decision"])
        bad = H.evaluate(base / "missing-policy.json")
        r = G.evaluate(today_iso=TRADING, hold_state=bad, **kw)
        ck("정책 무효 + 거래일 → BLOCKED_HOLD_POLICY_INVALID (fail-closed)",
           (r["decision"], r["verdict"]) == (H.BLOCKED_HOLD_POLICY_INVALID, "BLOCKED")
           and str(r.get("founderAction") or "").strip(), r["decision"])
        r = G.evaluate(today_iso=TRADING, hold_state={"decision": "UNHELD"},
                       canonical=None, apply_status=None, lock_held=True, public_model_error=None)
        ck("UNHELD → 기존 판정 계속(lock 보유 → BLOCKED_APPLY_IN_PROGRESS)",
           r["decision"] == "BLOCKED_APPLY_IN_PROGRESS", r["decision"])
        r = G.evaluate(today_iso=TRADING, **kw)
        ck("hold_state 미지정(순수 fixture) → 기존 판정 그대로", r["decision"] == "BLOCKED_APPLY_RECEIPT_MISSING",
           r["decision"])
        orig_run, orig_gate = G.run, G._magic_hold_gate
        try:
            G.run = lambda **_k: G._result(G.SKIP_EXPECTED_HOLD, verdict="PASS", reason="fixture", today=TRADING)
            ck("SKIPPED_EXPECTED_HOLD → exit 10 (ps1 정상 self-skip)", _quiet(G.main, []) == G.EXIT_SKIP)
            G.run = orig_run
            G._magic_hold_gate = lambda: hold
            r = G.run(today_iso=TRADING)
            ck("run() 이 실제 HOLD 판정을 gate 에 넘긴다", r["decision"] == G.SKIP_EXPECTED_HOLD, r["decision"])
        finally:
            G.run, G._magic_hold_gate = orig_run, orig_gate
    finally:
        AQ.MARKER_PATH = saved_marker


# ═══════════════════════ [7] 오전 통합보고 ═══════════════════════
def t_morning(base: Path):
    print("[7] 오전 통합보고 — HOLD 분류(KNOWN_INTENTIONAL_HOLD) · R4 자연 관찰 판정")
    import magic_morning_combined_report as M
    base.mkdir(parents=True, exist_ok=True)
    hold = H.evaluate()
    pol = hold.get("policy") or {}
    first = str(pol.get("firstIntentionalHoldSignalDate"))
    nxt = _next_trading_day(first)
    nxt2 = _next_trading_day(nxt)
    eff = str(pol.get("effectiveAt"))
    pre_run = "2026-09-10T15:40:00"          # 발효 전(마지막 사고 누락일 실행)
    post_run = f"{first}T15:40:00"           # 발효 후(첫 HOLD 일)
    lane = M.HOLD_LANE_TASKS

    def sched(result=0, last=post_run, drop=()):
        out = []
        for n in lane + (M.OBSERVE_TASK, M.MORNING_TASK):
            if n in drop:
                continue
            out.append({"name": n, "found": True, "state": "Ready", "lastRunTime": last,
                        "lastTaskResult": result if n in lane else 0, "nextRunTime": ""})
        return out

    live = {"jsonHttp": 200, "performanceHttp": 200, "rankingsHttp": 200, "deployedSequence": 43}
    clean = {"head": "abc", "branch": "main", "modified": [], "stagedCount": 0, "behind": 0}
    canon = {"officialSequence": pol.get("lastNormalSequence"),
             "canonicalSha256": str(pol.get("lastNormalCanonicalSha256"))[:16]}
    rep_ok = {"status": H.STATUS_EXPECTED_HOLD, "webLoginAttemptCount": 0, "signalGenerated": False}
    app_ok = {"date": first, "status": H.APPLY_SKIPPED_EXPECTED_HOLD, "canonicalChanged": False}
    wa_old = {"attempts": [{"observedAt": "2026-09-10T20:32:22+09:00", "entrypoint": "-"}]}

    def run(**kw):
        b = dict(today=nxt, yesterday=first, now_dt=datetime.fromisoformat(f"{nxt}T07:40:00+09:00"),
                 is_today_trading=True, is_yesterday_trading=True, canonical=canon, y_dryrun=dict(rep_ok),
                 evidence=None, sched=sched(), live=live, repo1=clean, repo2=clean, backup_done=False,
                 hold=hold, web_auth=wa_old, y_signal=dict(rep_ok), y_status=dict(rep_ok), apply_status=app_ok)
        b.update(kw)
        return M.classify_morning(**b)

    r = run()
    ck("정상 HOLD 일 → WAIT · closeoutState HOLD · blocker 0",
       (r["verdict"], r["closeoutState"], r["blocked"]) == ("WAIT", "HOLD", []),
       (r["verdict"], r["blocked"], r["actionWarns"]))
    ck("KNOWN_INTENTIONAL_HOLD · lane 상태 · Founder 행동 NONE",
       r.get("holdClassification") == H.HOLD_CLASSIFICATION and r.get("magicPaperLaneState") == H.LANE_STATE
       and r.get("founderAction") == "NONE")
    ck("HOLD 자체와 HOLD 일은 known-warn 로 분리",
       any("의도적 HOLD(" in w for w in r["knownWarns"]) and any("의도적 HOLD 일" in w for w in r["knownWarns"]),
       r["knownWarns"])
    ck("HOLD 이후 로그인: 마법공식 0 · 다른 lane 0",
       (r.get("webLoginAttemptCount"), r.get("otherLaneWebLoginSinceHold")) == (0, 0))
    ck("첫 HOLD 일 다음 아침 → R4 자연 관찰 PROVEN",
       r.get("naturalObservationId") == (pol.get("naturalObservation") or {}).get("id")
       and r.get("naturalObservationStatus") == "PROVEN", (r.get("naturalObservationId"), r.get("naturalObservationStatus")))

    r = run(today=first, yesterday=INCIDENT[-1], y_signal={"status": "BLOCKED"}, y_status={"status": "BLOCKED"},
            y_dryrun={"status": "BLOCKED", "blockedCode": "BLOCKED_SIGNAL_PACKAGE_MISSING"},
            sched=sched(2, pre_run), apply_status={"date": INCIDENT[-1], "status": "BLOCKED_GATE_FAILED"})
    ck("첫 아침(어제=사고 누락일): 발효 전 실패 5건 → known-warn · blocker 0",
       r["verdict"] == "WAIT" and r["blocked"] == [] and any("사고 누락일" in w for w in r["knownWarns"])
       and sum("발효 전" in w for w in r["knownWarns"]) == len(lane) and "naturalObservationId" not in r,
       (r["verdict"], r["blocked"], r["knownWarns"]))

    r = run(sched=sched(2, post_run))
    ck("발효 후 lane 작업 Result≠0 → BLOCKED · 자연 관찰 FAILED",
       r["verdict"] == "BLOCKED" and r.get("naturalObservationStatus") == "FAILED", r["blocked"])
    r = run(sched=sched(0, post_run, drop=(M.AUTO_APPLY_TASK,)))
    ck("HOLD lane 작업 조회 누락(Auto Apply) → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(sched=sched(0, "2026-09-10T16:25:00"))
    ck("HOLD 일에 lane 작업 자연 실행 기록 없음 → WARNING · 자연 관찰 INCOMPLETE",
       r["verdict"] == "WARNING" and r.get("naturalObservationStatus") == "INCOMPLETE", (r["verdict"], r["actionWarns"]))
    after_eff = (datetime.fromisoformat(eff) + timedelta(hours=1)).isoformat(timespec="seconds")
    r = run(web_auth={"attempts": [{"observedAt": after_eff, "entrypoint": "magic_daily_signal.py"}]})
    ck("발효 후 마법공식 경로 KRX 웹 로그인 → BLOCKED",
       r["verdict"] == "BLOCKED" and r.get("webLoginAttemptCount") == 1, r["blocked"])
    r = run(web_auth={"attempts": [{"observedAt": after_eff, "entrypoint": "shadow_weekly_snapshot.py"}]})
    ck("발효 후 다른 lane 로그인 → WARNING(마법공식과 분리, 숨기지 않음)",
       r["verdict"] == "WARNING" and r["blocked"] == [] and r.get("otherLaneWebLoginSinceHold") == 1,
       (r["verdict"], r["actionWarns"]))
    r = run(y_dryrun={"status": "BLOCKED", "blockedCode": "BLOCKED_SIGNAL_PACKAGE_MISSING"})
    ck("HOLD 일 dry-run 이 HOLD 아님 → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(y_signal=dict(rep_ok, webLoginAttemptCount=1))
    ck("HOLD 일 signal 이 로그인 흔적 보고 → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(y_status=None)
    ck("HOLD 일 status 산출물 없음 → WARNING · 자연 관찰 INCOMPLETE",
       r["verdict"] == "WARNING" and r["blocked"] == [] and r.get("naturalObservationStatus") == "INCOMPLETE",
       (r["verdict"], r["blocked"]))
    r = run(apply_status={"date": first, "status": "APPLIED_AUTOMATICALLY", "canonicalChanged": True})
    ck("HOLD 중 Auto Apply 가 장부 변경 → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(apply_status={"date": INCIDENT[-1], "status": H.APPLY_SKIPPED_EXPECTED_HOLD})
    ck("Auto Apply 기록이 어제보다 이전 → WARNING", r["verdict"] == "WARNING", (r["verdict"], r["actionWarns"]))
    r = run(live=dict(live, deployedSequence=44))
    ck("live seq ≠ canonical → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(canonical=dict(canon, officialSequence=44), live=dict(live, deployedSequence=44))
    ck("HOLD 중 canonical seq 변화 → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(canonical=dict(canon, canonicalSha256="0" * 16))
    ck("HOLD 중 canonical 내용 변경 → BLOCKED", r["verdict"] == "BLOCKED", r["blocked"])
    r = run(today=nxt2, yesterday=nxt, sched=sched(0, f"{nxt}T15:40:00"),
            apply_status=dict(app_ok, date=nxt))
    ck("두 번째 HOLD 일 이후 → WAIT · 자연 관찰 필드 없음",
       r["verdict"] == "WAIT" and "naturalObservationId" not in r, (r["verdict"], r["actionWarns"]))

    bad = H.evaluate(base / "missing-policy.json")
    r = run(hold=bad)
    ck("정책 무효 → BLOCKED · HOLD_POLICY_INVALID",
       (r["verdict"], r["closeoutState"]) == ("BLOCKED", "HOLD_POLICY_INVALID"), r["closeoutState"])
    ck("정책 무효 → 다음 작업 = HOLD 정책 원인분리", M.next_action_line(r).startswith("Phase MF-HOLD-POLICY-FIX"),
       M.next_action_line(r))
    r = run(hold=None, y_dryrun=None)
    ck("hold=None(기존 fixture) → 기존 판정 경로 그대로(WAIT · NA)",
       (r["verdict"], r["closeoutState"]) == ("WAIT", "NA"), r["closeoutState"])
    r = run(hold={"decision": "UNHELD"}, y_dryrun=None)
    ck("UNHELD → 기존 판정 경로", r["closeoutState"] == "NA", r["closeoutState"])

    r = run()
    o = dict(r, createdAt=f"{nxt}T07:40:00+09:00", canonical=canon, yesterdayDryRun=dict(rep_ok),
             yesterdaySignal=dict(rep_ok), yesterdayStatus=dict(rep_ok), evidence=None, scheduler=sched(),
             live=live, repo1=clean, repo2=clean)
    md = M.to_markdown(o)
    ck("보고서: HOLD 절이 요약보다 먼저 표시",
       "## 0. 마법공식 paper lane HOLD (KNOWN_INTENTIONAL_HOLD)" in md
       and md.index("## 0. 마법공식") < md.index("## 1. 요약"))
    ck("보고서: R4 자연 관찰 줄 표시", "R4 자연 관찰 WABABA-MAGIC-FORMULA-PAPER-LANE-HOLD-ENFORCEMENT-R4" in md)
    ck("보고서: 대장이 할 일 = 없음(의도적 HOLD)", "[대장이 할 일] 없음 (의도적 HOLD" in md)
    ck("보고서: '어제 신호 생성됨' 같은 정상 운영 문구 없음", "어제 신호 생성됨" not in md)
    ck("보고서: 파서 판정 줄 유지(WAIT→WARNING 매핑)", "\n전체 판정: WARNING\n" in md)
    ck("다음 작업 = 없음(HOLD 유지)", M.next_action_line(o).startswith("없음 — 마법공식 paper lane 의도적 HOLD"),
       M.next_action_line(o))
    ck("자동화 채팅 요청 = HOLD 유지 확인만", "HOLD 유지 확인만" in M.to_txt(o))
    rb = run(sched=sched(2, post_run))
    ck("HOLD 중 이상 → 다음 작업 = HOLD 이상 원인분리",
       M.next_action_line(rb).startswith("Phase MF-HOLD-ANOMALY-FIX"), M.next_action_line(rb))


# ═══════════════════════ [8] 정적 배선 ═══════════════════════
def t_static():
    print("[8] 정적 배선")
    sig = (SCR / "magic_daily_signal.py").read_text(encoding="utf-8")
    tree = ast.parse(sig)
    top = [a.name for n in tree.body if isinstance(n, ast.Import) for a in n.names]
    top += [n.module for n in tree.body if isinstance(n, ast.ImportFrom) and n.module]
    ck("signal: 모듈 최상위 import 에 위험 모듈·signal package 없음", not any(m in top for m in RISKY), top)
    main_src = sig[sig.index("def main("):]
    ck("signal: HOLD 게이트가 run_signal 호출보다 먼저",
       main_src.index("_magic_hold_gate()") < main_src.index("run_signal(signal_date"))
    ck("signal: 기존 exit 계약 문자열 유지(BLOCKED → 2)",
       'return 0 if r["status"] in ("READY", "ALREADY_PREPARED", "SELF_SKIPPED_NON_TRADING_DAY") else 2' in sig)
    for name, first_call in (("magic_daily_dry_run.py", "find_ready_package_for_today(today)"),
                             ("magic_daily_status.py", "build_status(today)"),
                             ("magic_daily_auto_apply.py", "run_auto_apply(today_iso=args.date")):
        s = (SCR / name).read_text(encoding="utf-8")
        m = s[s.index("def main("):]
        ck(f"{name}: HOLD 게이트가 기존 실행보다 먼저", m.index("_magic_hold_gate()") < m.index(first_call))
    aa = (SCR / "magic_daily_auto_apply.py").read_text(encoding="utf-8")
    ck("auto_apply: 기존 exit 계약 문자열 유지", 'return 0 if r["verdict"] == "PASS" else 2' in aa)
    gate = (SCR / "magic_publish_gate.py").read_text(encoding="utf-8")
    ck("publish gate: run() 이 hold_state=_magic_hold_gate() 전달", "hold_state=_magic_hold_gate()" in gate)
    mo = (SCR / "magic_morning_combined_report.py").read_text(encoding="utf-8")
    br = mo[mo.index("def build_report("):]
    ck("오전보고: HOLD 판정 후 스케줄러 조회", br.index("hold = _magic_hold_gate()") < br.index("query_scheduler_state("))
    for name in ("magic_daily_signal.py", "magic_daily_dry_run.py", "magic_daily_status.py",
                 "magic_daily_auto_apply.py", "magic_publish_gate.py", "magic_morning_combined_report.py"):
        s = (SCR / name).read_text(encoding="utf-8")
        ck(f"{name}: _magic_hold_gate 정의 · 신규 스케줄러 명령 없음",
           "def _magic_hold_gate(" in s and "Register-ScheduledTask" not in s and "schtasks" not in s)
    hsrc = (SCR / "magic_paper_lane_hold.py").read_text(encoding="utf-8")
    mods = set()
    for n in ast.walk(ast.parse(hsrc)):
        if isinstance(n, ast.Import):
            mods |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module:
            mods.add(n.module.split(".")[0])
    ck("helper: stdlib 만 import", mods <= {"__future__", "hashlib", "json", "datetime", "pathlib"}, sorted(mods))
    ck("helper: 환경변수·자격증명 미접근", "os.environ" not in hsrc and "KRX_PW" not in hsrc and "getenv" not in hsrc)


# ═══════════════════════ [9] HOLD 중 장부 불변 ═══════════════════════
def t_invariants():
    print("[9] HOLD 중 장부 불변 (canonical 읽기만)")
    pol = H.evaluate().get("policy") or {}
    c = _read(C.CANONICAL_PATH) or {}
    ck("canonical sha == 마지막 정상 sha", _sha(C.CANONICAL_PATH) == pol.get("lastNormalCanonicalSha256"))
    ck("canonical seq == 마지막 정상 seq", c.get("officialSequence") == pol.get("lastNormalSequence"),
       c.get("officialSequence"))
    ck("장부 최신 실행일 == 마지막 정상 거래일",
       max(c.get("officialExecutionCalendar") or [""]) == pol.get("lastNormalTradeDate"))
    counts = {k: len(c.get(k) or []) for k in BASE_COUNTS}
    ck("lot·BUY·SELL·batch·missedRuns 수 불변(사고 누락일·HOLD 일 장부 미기록 · 소급 0)", counts == BASE_COUNTS, counts)


# ═══════════════════════ [10] R4 closeout ═══════════════════════
def t_closeout():
    print("[10] R4 closeout — 운영 정의·전략 코드·외부 불변")
    base = _read(ROOT / "reports" / "research" / "r4-magic-paper-lane-scheduler-baseline.json")
    ck("스케줄러 baseline artifact 존재", base is not None)
    if base:
        names = [t["name"] for t in base.get("tasks") or []]
        ps = ("$ns=@(" + ",".join("'" + n.replace("'", "''") + "'" for n in names) + ");"
              "$r=foreach($n in $ns){$t=Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue;"
              "if($t){$a=$t.Actions|Select-Object -First 1;$g=$t.Triggers|Select-Object -First 1;"
              "[pscustomobject]@{name=$n;state=[string]$t.State;user=[string]$t.Principal.UserId;"
              "trigger=[string]$g.StartBoundary;days=[int]$g.DaysOfWeek;execute=[string]$a.Execute;"
              "arguments=[string]$a.Arguments;workingDirectory=[string]$a.WorkingDirectory;"
              "triggerCount=@($t.Triggers).Count;actionCount=@($t.Actions).Count}}};"
              "$r|ConvertTo-Json -Depth 3")
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True, timeout=120)
        try:
            cur = json.loads(out.stdout)
        except ValueError:
            cur = []
        cur = cur if isinstance(cur, list) else [cur]
        cmap = {t.get("name"): t for t in cur}
        for t in base.get("tasks") or []:
            c = cmap.get(t["name"]) or {}
            keys = ("user", "trigger", "execute", "arguments", "workingDirectory")
            diff = {k: (t.get(k), c.get(k)) for k in keys if t.get(k) != c.get(k)}
            ok = (not diff and c.get("state") in ("Ready", "Running") and c.get("days") == base.get("daysOfWeekMonFri")
                  and c.get("triggerCount") == 1 and c.get("actionCount") == 1)
            ck(f"스케줄러 정의 불변: {t['name']}", ok, diff or c)
        cnt = subprocess.run(["powershell", "-NoProfile", "-Command",
                              "@(Get-ScheduledTask | Where-Object { $_.TaskName -like 'Wababa*' }).Count"],
                             capture_output=True, text=True, timeout=120).stdout.strip()
        ck("Wababa 예약작업 수 불변(신규 task·trigger 0)", cnt == str(base.get("wababaTaskCount")), cnt)
    strat = ["scripts/build_magic_signal_package.py", "scripts/build_market_snapshot.py",
             "scripts/build_market_snapshot_fast.py", "scripts/magic_rolling_engine.py",
             "scripts/run_magic_rolling_dry_run.py", "scripts/build_magic_formula_fund.py",
             "scripts/apply_magic_official_day.py", "scripts/record_magic_missed_run.py",
             "scripts/build_magic_official_public.py", "scripts/magic_apply_from_approval.py",
             "scripts/magic_make_approval_ticket.py", "scripts/krx_auth_observer.py",
             "scripts/refresh_public_from_canonical.py", "magic-formula-portfolio.json"]
    p = subprocess.run(["git", "-C", str(ROOT), "diff", "--quiet", "21fdced", "--", *strat])
    ck("전략·ranking·universe·apply·publish 변환 코드 불변(vs 21fdced)", p.returncode == 0, p.returncode)
    import importlib.util
    spec = importlib.util.find_spec("pykrx")      # 최상위 패키지 find_spec 은 import 하지 않는다
    auth = (Path(list(spec.submodule_search_locations)[0]) / "website" / "comm" / "auth.py") if spec else None
    ck("site-packages pykrx auth.py 불변(sha 32defaee…)", (_sha(auth) or "").startswith("32defaee"), _sha(auth))
    ck("pykrx 는 위치만 확인 — import 안 함", "pykrx" not in sys.modules)
    head = subprocess.run(["git", "-C", str(PUBLIC_REPO), "rev-parse", "--short", "HEAD"],
                          capture_output=True, text=True).stdout.strip()
    ck("public repo HEAD 불변(2c8a000)", head == "2c8a000", head)
    st = subprocess.run(["git", "-C", str(PUBLIC_REPO), "status", "--porcelain"],
                        capture_output=True, text=True).stdout.splitlines()
    ck("public repo 작업트리 = 착수 전 known dirty 2건만",
       sorted(x[3:] for x in st) == ["next-env.d.ts", "public/data/recommendation-history.json"], st)
    changed = ["scripts/magic_paper_lane_hold.py", "config/wababa-magic-paper-lane-state.json",
               "scripts/magic_daily_signal.py", "scripts/magic_daily_dry_run.py", "scripts/magic_daily_status.py",
               "scripts/magic_daily_auto_apply.py", "scripts/magic_publish_gate.py",
               "scripts/magic_morning_combined_report.py", "scripts/test_magic_auto_apply_catchup.py",
               "scripts/test_magic_publish_gate.py", "scripts/run_magic_full_test_suite.py"]
    pat = re.compile(r"(?i)(krx_pw|krx_id|password|passwd|secret|api[_-]?key|token)\s*[=:]\s*['\"][^'\"\s]{4,}")
    hits = [f for f in changed if (ROOT / f).exists() and pat.search((ROOT / f).read_text(encoding="utf-8"))]
    ck("변경 파일 비밀값 패턴 0", not hits, hits)


def _write_report(started: str, passed: int, failed: int):
    p = ROOT / "reports" / "research" / "r4-magic-paper-lane-hold-test-latest.json"
    doc = {
        "task": "WABABA-MAGIC-FORMULA-PAPER-LANE-HOLD-ENFORCEMENT-AND-TRANSPARENCY-R4",
        "schema": "wababa.r4-hold-test/v1", "mode": "R4_CLOSEOUT",
        "startedAt": started, "finishedAt": datetime.now(KST).isoformat(timespec="seconds"),
        "verdict": "PASS" if failed == 0 else "FAIL", "passed": passed, "failed": failed,
        "testProcessConnectAttempts": len(_CONNECTS),
        "testProcessNetworkRiskyModulesLoaded": [m for m in NET_RISKY if m in sys.modules],
        "policySha256": H.EXPECTED_POLICY_SHA256,
        "naturalRunItems": "NOT_YET_DUE (첫 의도적 HOLD 자연 실행 = 정책 firstIntentionalHoldSignalDate)",
        "results": RESULTS,
    }
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"  report: {p}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="R4 마법공식 paper lane HOLD 게이트 회귀")
    ap.add_argument("--r4-closeout", action="store_true", help="운영 불변 확인 + 결과 보고서 기록")
    args = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    started = datetime.now(KST).isoformat(timespec="seconds")
    with tempfile.TemporaryDirectory(prefix="r4-hold-", ignore_cleanup_errors=True) as d:
        base = Path(d)
        steps = [("policy_eval", lambda: t_policy_eval(base / "policy")),
                 ("policy_content", t_policy_content),
                 ("entrypoints", lambda: t_entrypoints(base / "run")),
                 ("unheld", lambda: t_unheld(base / "unheld")),
                 ("publish_gate", lambda: t_publish_gate(base / "gate")),
                 ("morning", lambda: t_morning(base / "morning")),
                 ("static", t_static),
                 ("invariants", t_invariants)]
        if args.r4_closeout:
            steps.append(("closeout", t_closeout))
        for name, fn in steps:
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                import traceback
                traceback.print_exc()
                ck(f"단계 예외 없음: {name}", False, f"{type(e).__name__}: {e}")
    print("[11] 테스트 프로세스 격리")
    ck("테스트 프로세스 네트워크 연결 시도 0", len(_CONNECTS) == 0, len(_CONNECTS))
    ck("테스트 프로세스 pykrx/build_market_snapshot* 미적재",
       not [m for m in NET_RISKY if m in sys.modules], [m for m in NET_RISKY if m in sys.modules])
    passed = sum(1 for r in RESULTS if r["pass"])
    failed = len(RESULTS) - passed
    if args.r4_closeout:
        _write_report(started, passed, failed)
    print(f"\n결과: {passed} passed, {failed} failed (총 {len(RESULTS)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
