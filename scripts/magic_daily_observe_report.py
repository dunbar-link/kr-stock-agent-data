#!/usr/bin/env python3
"""마법공식 — 매일 장마감 후 read-only 관찰 보고 생성기 (Phase MF-DAILY-OBSERVE-AUTOMATION).

기존 스케줄러 3종(Signal 15:40 / Dry Run 15:45 / Status 16:05) 실행 결과를 16:10에 read-only로
종합해, 대장이 메모장으로 바로 보고 ChatGPT에 붙일 수 있는 보고서를 만든다.

**이 스크립트는 관찰만 한다.** canonical apply / backup / public publish / rankings publish /
git add·commit·push / deploy / 실주문 / 브로커 API — 전부 하지 않는다. 파일 write는
reports/magic-daily-observe-latest.{md,txt} 두 개뿐(장부·public·git 무접근).

입력(전부 read-only):
- Windows Task Scheduler 3종 상태(PowerShell Get-ScheduledTaskInfo)
- TEMP signal/dry-run/status 산출물 + signal 패키지 rankings.json(top10/top100 evidence)
- canonical 요약(magic-formula-official-state.json) — read only
- 운영 URL live 상태(magic_live_verify.run, deploy 호출 0)
- REPO1/REPO2 git HEAD·status(read only)

판정: PASS / WARNING / BLOCKED / WAIT (보고서 첫 줄 "전체 판정: ...").

예) python scripts/magic_daily_observe_report.py            # 보고서 생성 + notepad 열기 시도
    python scripts/magic_daily_observe_report.py --no-notepad --json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, time as _time
from pathlib import Path

import magic_daily_common as C

try:
    import magic_live_verify as LV
except Exception:  # noqa: BLE001 — live 모듈 없더라도 관찰은 진행
    LV = None

ROOT = Path(__file__).resolve().parents[1]
REPO1_ROOT = Path("C:/work/kr-stock-agent")
REPO2_ROOT = ROOT
REPORTS_DIR = ROOT / "reports"
CANONICAL_PATH = ROOT / "magic-formula-official-state.json"
BASE_URL = "https://kr-stock-agent.vercel.app"

SCHED_TASKS = ("Wababa Magic Daily Signal", "Wababa Magic Daily Dry Run", "Wababa Magic Daily Status")
STATUS_SCHED_TIME = _time(16, 5)   # 마지막 스케줄러(status) 실행 시각 — 이 전이면 WAIT

# 항상 존재할 수 있는 구조적 known-warn(있어도 closeout 가능 → PASS 유지, 보고만)
EXPECTED_WARN_REPO2 = {"financial-universe-real.json"}
EXPECTED_WARN_REPO1 = {"next-env.d.ts"}
# closeout 게이트에 영향을 주는 REPO1 public 파일(미커밋이면 정리 필요 → WARNING)
REPO1_PUBLIC_REL = "public/data/recommendation-history.json"


# ============================== read-only 수집 ==============================

def _run(cmd, *, timeout=60):
    """subprocess 실행(read-only 조회 전용). (stdout, ok)."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.stdout, p.returncode == 0
    except Exception:  # noqa: BLE001
        return "", False


def _powershell(ps: str, *, timeout=60) -> str:
    out, _ = _run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps], timeout=timeout)
    return out.strip()


def query_scheduler(names=SCHED_TASKS, *, runner=None) -> list[dict]:
    """3종 스케줄러 LastRunTime/LastTaskResult/NextRunTime(read-only). runner 주입 시 네트워크/OS 0(테스트)."""
    runner = runner or _powershell
    ps_names = ",".join("'" + n.replace("'", "''") + "'" for n in names)
    ps = (
        "$ns=@(" + ps_names + ");"
        "$r=foreach($n in $ns){"
        "$i=Get-ScheduledTaskInfo -TaskName $n -ErrorAction SilentlyContinue;"
        "[pscustomobject]@{name=$n;found=[bool]$i;"
        "lastRunTime=if($i -and $i.LastRunTime){$i.LastRunTime.ToString('yyyy-MM-ddTHH:mm:ss')}else{''};"
        "lastTaskResult=if($i){[int]$i.LastTaskResult}else{$null};"
        "nextRunTime=if($i -and $i.NextRunTime){$i.NextRunTime.ToString('yyyy-MM-ddTHH:mm:ss')}else{''}}};"
        "$r | ConvertTo-Json -Depth 4"
    )
    try:
        data = json.loads(runner(ps))
    except (ValueError, TypeError):
        return [{"name": n, "found": False, "lastRunTime": "", "lastTaskResult": None,
                 "nextRunTime": "", "queryError": True} for n in names]
    if isinstance(data, dict):
        data = [data]
    return data


def git_state(repo_path, *, runner=None) -> dict:
    """git HEAD·branch·modified(tracked)·staged 개수(read-only)."""
    runner = runner or (lambda args: _run(["git", "-C", str(repo_path)] + args)[0])
    head = runner(["rev-parse", "--short", "HEAD"]).strip()
    branch = runner(["branch", "--show-current"]).strip()
    porcelain = runner(["status", "--porcelain"])
    modified, staged = [], 0
    for line in porcelain.splitlines():
        if len(line) < 3 or line[:2] == "??":
            continue
        index_col = line[0]
        path = line[3:].strip().strip('"')
        if index_col not in (" ", "?"):
            staged += 1
        modified.append(path.replace("\\", "/"))
    return {"path": str(repo_path), "head": head, "branch": branch,
            "modified": modified, "stagedCount": staged}


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def collect_evidence(rankings_doc) -> dict | None:
    """signal 패키지 rankings.json → top10/top100 존재·evMethod·rank1 원값 요약(재계산 0)."""
    if not isinstance(rankings_doc, dict):
        return None
    top10 = rankings_doc.get("top10") or []
    top100 = rankings_doc.get("top100") or []
    r1 = (top100 or top10 or [{}])[0] if (top100 or top10) else {}
    return {
        "top10Count": len(top10), "top100Count": len(top100),
        "evMethod": r1.get("evMethod"),
        "rank1": {
            "code": r1.get("code"), "name": r1.get("name"), "rank": r1.get("rank"),
            "valueRank": r1.get("valueRank"), "profitabilityRank": r1.get("profitabilityRank"),
            "combinedRank": r1.get("combinedRank"), "earningsYield": r1.get("earningsYield"),
            "returnOnCapital": r1.get("returnOnCapital"), "signalClosePrice": r1.get("signalClosePrice"),
        } if r1 else None,
    }


def observe_live() -> dict:
    """운영 URL live 상태 read-only(deploy 호출 0). live 모듈/네트워크 실패는 error로만 담는다."""
    if LV is None:
        return {"error": "live module unavailable"}
    try:
        r = LV.run()
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}
    try:
        rank_status, _ = LV._fetch(f"{BASE_URL}/magic-formula/rankings")
    except Exception:  # noqa: BLE001
        rank_status = None
    r["rankingsHttp"] = rank_status
    return r


# ============================== 판정(순수 함수) ==============================

def classify(*, today, now_dt, is_trading_day, canonical, dryrun, evidence, sched, live,
             repo1, repo2) -> dict:
    """read-only 관찰 결과 → 판정 dict(순수 함수, 시간·OS·네트워크 주입). write 0."""
    canon_seq = (canonical or {}).get("officialSequence")
    expected_seq = (canon_seq + 1) if isinstance(canon_seq, int) else None
    actual_seq = (dryrun or {}).get("proposedSequence")
    batch_id = (dryrun or {}).get("proposedBatchId")
    signal_as_of = (dryrun or {}).get("signalAsOfDate")

    blocked, action_warns, known_warns = [], [], []

    # --- WAIT: 비거래일 / 장마감 전 / 스케줄러 미완료 ---
    if not is_trading_day:
        return _result("WAIT", "오늘은 KRX 비거래일 — 관찰 대상 없음", canon_seq, expected_seq,
                       actual_seq, batch_id, signal_as_of, blocked, action_warns, known_warns, "NA")
    if now_dt <= C.market_close_dt(today):
        return _result("WAIT", "장마감(15:30 KST) 전 — signal/dry-run 산출물 생성 전", canon_seq,
                       expected_seq, actual_seq, batch_id, signal_as_of, blocked, action_warns,
                       known_warns, "NA")
    if dryrun is None:
        if now_dt.timetz().replace(tzinfo=None) < STATUS_SCHED_TIME:
            return _result("WAIT", "16:05 status 스케줄러 실행 전 — dry-run 산출물 대기", canon_seq,
                           expected_seq, actual_seq, batch_id, signal_as_of, blocked, action_warns,
                           known_warns, "NA")
        # 스케줄러 시각 지났는데 산출물 없음 → 스케줄러 결과로 분기
        if any((s.get("lastTaskResult") not in (0, None)) for s in (sched or [])):
            blocked.append("스케줄러 실패(LastTaskResult != 0) 且 오늘 dry-run 산출물 없음")
            return _result("BLOCKED", "스케줄러 실패로 dry-run 산출물 미생성", canon_seq, expected_seq,
                           actual_seq, batch_id, signal_as_of, blocked, action_warns, known_warns, "NA")
        return _result("WAIT", "오늘 dry-run 산출물 아직 없음 — 스케줄러 재확인 필요", canon_seq,
                       expected_seq, actual_seq, batch_id, signal_as_of, blocked, action_warns,
                       known_warns, "NA")

    # --- 여기부터 오늘 dry-run 존재 ---
    status = dryrun.get("status") or dryrun.get("runStatus")
    if status != "COMPLETED":
        blocked.append(f"dry-run status={status!r} != COMPLETED (blockedCode={dryrun.get('blockedCode')})")
    if dryrun.get("blockedCode"):
        blocked.append(f"dry-run blockedCode={dryrun.get('blockedCode')} (BLOCKED_LOOKAHEAD 등)")
    if dryrun.get("lookAheadValidationPassed") is not True:
        blocked.append("lookAheadValidationPassed != true")
    if dryrun.get("missingEvalCodes"):
        blocked.append(f"missingEvalCodes 존재: {dryrun.get('missingEvalCodes')}")
    if dryrun.get("productionWriteCount") not in (0, None):
        blocked.append(f"productionWriteCount={dryrun.get('productionWriteCount')} != 0")
    if dryrun.get("readOnlyUnchanged") is not True:
        blocked.append("readOnlyUnchanged != true")
    if dryrun.get("buyCount") != 10:
        blocked.append(f"buyCount={dryrun.get('buyCount')} != 10 (buy 10/10 아님)")

    # evidence / rankings
    if not evidence:
        blocked.append("evidence/rankings 산출물 없음(signal 패키지 rankings.json 누락)")
    else:
        if (evidence.get("top10Count") or 0) < 10:
            blocked.append(f"top10 원값 부족(top10Count={evidence.get('top10Count')})")
        if (evidence.get("top100Count") or 0) < 100:
            blocked.append(f"top100 부족(top100Count={evidence.get('top100Count')})")

    # 스케줄러 결과 0 확인
    for s in (sched or []):
        if s.get("lastTaskResult") not in (0, None):
            blocked.append(f"스케줄러 '{s.get('name')}' LastTaskResult={s.get('lastTaskResult')} != 0")

    # proposedSequence 정합
    closeout_state = "NA"
    if isinstance(actual_seq, int) and isinstance(canon_seq, int):
        if actual_seq == canon_seq + 1:
            closeout_state = "NEEDED"
        elif actual_seq == canon_seq:
            closeout_state = "ALREADY_APPLIED"
        elif actual_seq < canon_seq:
            closeout_state = "STALE"
            action_warns.append(f"오늘 dry-run proposedSequence={actual_seq} < canonical={canon_seq} (직전 산출물?)")
        else:
            closeout_state = "AHEAD"
            blocked.append(f"proposedSequence={actual_seq} > 예상 {canon_seq + 1} (예상 밖 점프)")

    # live 상태
    if live:
        if live.get("error"):
            action_warns.append(f"live 조회 실패(네트워크): {live.get('error')}")
        else:
            for label, key in (("json", "jsonHttp"), ("performance", "performanceHttp"),
                               ("rankings", "rankingsHttp")):
                code = live.get(key)
                if code in (404, 500, 502, 503):
                    blocked.append(f"live {label} HTTP {code} (운영 장애)")
                elif code != 200:
                    action_warns.append(f"live {label} HTTP {code} != 200")
            live_seq = live.get("deployedSequence")
            if isinstance(live_seq, int) and isinstance(canon_seq, int) and live_seq != canon_seq:
                action_warns.append(f"live seq {live_seq} != canonical {canon_seq} (배포 지연 가능)")

    # known-warn / action-warn 분류(repo dirty)
    _classify_repo_warns(repo1, repo2, action_warns, known_warns)

    if blocked:
        verdict = "BLOCKED"
    elif action_warns:
        verdict = "WARNING"
    else:
        verdict = "PASS"
    hint = _verdict_hint(verdict, closeout_state, actual_seq, expected_seq)
    return _result(verdict, hint, canon_seq, expected_seq, actual_seq, batch_id, signal_as_of,
                   blocked, action_warns, known_warns, closeout_state)


def _classify_repo_warns(repo1, repo2, action_warns, known_warns):
    r1mod = set((repo1 or {}).get("modified") or [])
    r2mod = set((repo2 or {}).get("modified") or [])
    # REPO2: financial-universe-real.json = 구조적 known-warn; 그 외 tracked dirty = action
    for f in sorted(r2mod):
        (known_warns if f in EXPECTED_WARN_REPO2 else action_warns).append(
            f"REPO2 {f} M" + ("" if f in EXPECTED_WARN_REPO2 else " (예상 밖 dirty)"))
    # REPO1: next-env.d.ts = known-warn; recommendation-history.json 미커밋 = closeout 정리 필요(action)
    for f in sorted(r1mod):
        if f in EXPECTED_WARN_REPO1:
            known_warns.append(f"REPO1 {f} M")
        elif f == REPO1_PUBLIC_REL:
            action_warns.append(f"REPO1 {f} 미커밋(자동갱신/publish 잔여) — closeout 전 clean 정리 필요")
        else:
            action_warns.append(f"REPO1 {f} M (예상 밖 dirty)")
    if (repo1 or {}).get("stagedCount"):
        action_warns.append(f"REPO1 staged {repo1['stagedCount']}건 — 정리 필요")
    if (repo2 or {}).get("stagedCount"):
        action_warns.append(f"REPO2 staged {repo2['stagedCount']}건 — 정리 필요")


def _verdict_hint(verdict, closeout_state, actual_seq, expected_seq):
    if verdict == "BLOCKED":
        return "blocker 존재 — 원인분리 후 재관찰 전까지 closeout 금지"
    if closeout_state == "NEEDED":
        return f"seq={actual_seq} closeout 가능(dry-run COMPLETED·eligibility 정상)"
    if closeout_state == "ALREADY_APPLIED":
        return f"seq={actual_seq} 이미 canonical 반영 완료 — 다음 거래일 재관찰"
    return "read-only 관찰 완료"


def _result(verdict, hint, canon_seq, expected_seq, actual_seq, batch_id, signal_as_of,
            blocked, action_warns, known_warns, closeout_state):
    return {
        "verdict": verdict, "hint": hint, "closeoutState": closeout_state,
        "canonicalSequence": canon_seq, "expectedProposedSequence": expected_seq,
        "actualProposedSequence": actual_seq, "batchId": batch_id, "signalAsOfDate": signal_as_of,
        "blocked": blocked, "actionWarns": action_warns, "knownWarns": known_warns,
    }


# ============================== 오케스트레이션 ==============================

def build_observation(today, *, now_dt=None) -> dict:
    now_dt = now_dt or C.now_kst()
    canonical = C.load_canonical_summary()
    dryrun = read_json(C.REPORTS_DIR / f"dry-run-{today}.json")
    signal = read_json(C.REPORTS_DIR / f"signal-{today}.json")
    status_rep = read_json(C.REPORTS_DIR / f"daily-status-{today}.json")
    signal_as_of = (dryrun or {}).get("signalAsOfDate")
    rankings_doc = read_json(C.TEMP_ROOT / str(signal_as_of) / "rankings.json") if signal_as_of else None
    evidence = collect_evidence(rankings_doc)
    sched = query_scheduler()
    live = observe_live()
    repo1 = git_state(REPO1_ROOT)
    repo2 = git_state(REPO2_ROOT)
    verdict = classify(today=today, now_dt=now_dt, is_trading_day=C.is_krx_trading_day(today),
                       canonical=canonical, dryrun=dryrun, evidence=evidence, sched=sched,
                       live=live, repo1=repo1, repo2=repo2)
    return {
        "schemaVersion": "magic-daily-observe-v1", "date": today,
        "createdAt": now_dt.isoformat(), **verdict,
        "canonical": canonical, "dryRun": dryrun, "signal": signal, "statusReport": status_rep,
        "evidence": evidence, "scheduler": sched, "live": live,
        "repo1": repo1, "repo2": repo2, "signalAsOfDate": signal_as_of,
        "readOnly": True, "realOrderCount": 0, "noFakeTrade": True,
        "writeScope": "reports/magic-daily-observe-latest.{md,txt} only",
    }


# ============================== 보고서 렌더 ==============================

def _seq_for_task(o) -> int | None:
    if o["closeoutState"] == "NEEDED":
        return o["actualProposedSequence"]
    if o["closeoutState"] == "ALREADY_APPLIED":
        return (o["actualProposedSequence"] or 0) + 1
    return o["expectedProposedSequence"]


def next_task_line(o) -> str:
    n = _seq_for_task(o)
    v = o["verdict"]
    if v == "BLOCKED":
        return f"Phase MF-SEQ{n}-BLOCKER-FIX: blocker 원인분리 (closeout 금지)"
    if v == "WAIT":
        return f"Phase MF-SEQ{n}-RECHECK: read-only 재관찰"
    if o["closeoutState"] == "NEEDED":
        return (f"Phase MF-SEQ{n}-ONE-SHOT-CLOSEOUT: seq={n} ticket→승인→apply→backup→"
                "official publish→rankings publish→push→live→closure 원샷 진행")
    if o["closeoutState"] == "ALREADY_APPLIED":
        return f"Phase MF-SEQ{n}-RECHECK: 다음 거래일 read-only 재관찰 (seq{o['actualProposedSequence']} 이미 반영)"
    return f"Phase MF-SEQ{n}-RECHECK: read-only 재관찰"


def _sched_line(s) -> str:
    return (f"- {s.get('name')}: last={s.get('lastRunTime') or '-'} "
            f"result={s.get('lastTaskResult')} next={s.get('nextRunTime') or '-'}"
            + (" [조회실패]" if s.get("queryError") or not s.get("found") else ""))


def to_markdown(o) -> str:
    d, dr, ev, lv = o, (o["dryRun"] or {}), (o["evidence"] or {}), (o["live"] or {})
    c = o["canonical"] or {}
    L = [f"전체 판정: {o['verdict']}", "",
         f"# 와바바 마법공식 일일 관찰 보고 — {o['date']}", "",
         "## 1. 요약",
         f"- 관찰 날짜: {o['date']} (생성 {o['createdAt']})",
         f"- 예상 proposedSequence: {o['expectedProposedSequence']} / 실제: {o['actualProposedSequence']}",
         f"- batchId: {o['batchId']} / signalAsOfDate: {o['signalAsOfDate']}",
         f"- closeout 상태: {o['closeoutState']} — {o['hint']}", ""]
    L += ["## 2. scheduler 3종"] + [_sched_line(s) for s in (o["scheduler"] or [])] + [""]
    L += ["## 3. dry-run 게이트",
          f"- status/runStatus: {dr.get('status') or dr.get('runStatus')} / blockedCode: {dr.get('blockedCode')}",
          f"- lookAheadValidationPassed: {dr.get('lookAheadValidationPassed')}",
          f"- buyCount: {dr.get('buyCount')} (10/10 여부: {dr.get('buyCount') == 10}) / sellCount: {dr.get('sellCount')}",
          f"- eval(openPrices)={len(dr.get('openPrices') or {})} / missingEvalCodes: {dr.get('missingEvalCodes')}",
          f"- productionWriteCount: {dr.get('productionWriteCount')} / canonicalChanged: {dr.get('canonicalChanged', False)}",
          f"- readOnlyUnchanged: {dr.get('readOnlyUnchanged')} / noFakeTrade: {dr.get('noFakeTrade')} / realOrderCount: 0", ""]
    r1 = ev.get("rank1") or {}
    L += ["## 4. evidence / rankings",
          f"- top10 원값: {ev.get('top10Count')}개 / top100: {ev.get('top100Count')}개 / evMethod: {ev.get('evMethod')}",
          f"- rank1: {r1.get('code')} {r1.get('name')} (value {r1.get('valueRank')}/quality {r1.get('profitabilityRank')}/combined {r1.get('combinedRank')})",
          f"- earningsYield: {r1.get('earningsYield')} / returnOnCapital: {r1.get('returnOnCapital')} / signalClosePrice: {r1.get('signalClosePrice')}", ""]
    L += ["## 5. repo / live 상태",
          f"- REPO2 HEAD {o['repo2'].get('head')} ({o['repo2'].get('branch')}) modified={o['repo2'].get('modified')} staged={o['repo2'].get('stagedCount')}",
          f"- REPO1 HEAD {o['repo1'].get('head')} ({o['repo1'].get('branch')}) modified={o['repo1'].get('modified')} staged={o['repo1'].get('stagedCount')}",
          f"- canonical seq {c.get('officialSequence')} / cash {c.get('officialAvailableCash')}",
          f"- live seq {lv.get('deployedSequence')} / totalAsset {lv.get('deployedTotalAsset')} / tradeDays {lv.get('tradeDayCount')}",
          f"- live json {lv.get('jsonHttp')} / performance {lv.get('performanceHttp')} / rankings {lv.get('rankingsHttp')}"
          + (f" / error {lv.get('error')}" if lv.get('error') else ""), ""]
    L += ["## 6. known-warn"] + ([f"- {w}" for w in o["knownWarns"]] or ["- (없음)"])
    if o["actionWarns"]:
        L += ["", "### action-needed(WARNING 유발)"] + [f"- {w}" for w in o["actionWarns"]]
    if o["blocked"]:
        L += ["", "### BLOCKED 원인"] + [f"- {w}" for w in o["blocked"]]
    L += ["", "## 7. 다음 단일 작업 제안", f"- {next_task_line(o)}", "",
          "> read-only 관찰만 수행. apply/backup/publish/commit/push/deploy/실주문 0."]
    return "\n".join(L)


def _chatgpt_request(o) -> str:
    n = _seq_for_task(o)
    if o["verdict"] == "BLOCKED":
        body = (f"와바바 마법공식 {o['date']} 관찰 결과 BLOCKED다. proposedSequence={o['actualProposedSequence']}, "
                f"blocked 원인: {'; '.join(o['blocked']) or '상세 확인 필요'}. "
                f"Phase MF-SEQ{n}-BLOCKER-FIX 원인분리 지시문을 만들어줘. closeout은 금지.")
    elif o["verdict"] == "WAIT":
        body = (f"와바바 마법공식 {o['date']} 아직 WAIT다({o['hint']}). "
                f"Phase MF-SEQ{n}-RECHECK read-only 재관찰 지시문을 만들어줘.")
    elif o["closeoutState"] == "ALREADY_APPLIED":
        body = (f"와바바 마법공식 {o['date']} seq={o['actualProposedSequence']}는 이미 canonical 반영 완료다"
                f"(판정 {o['verdict']}). 다음 거래일 seq{n} read-only 재관찰 지시문을 만들어줘.")
    else:  # PASS/WARNING + NEEDED
        warns = ("; known-warn: " + "; ".join(o["knownWarns"] + o["actionWarns"])) if (o["knownWarns"] or o["actionWarns"]) else ""
        body = (f"와바바 마법공식 {o['date']} 관찰 {o['verdict']}, seq={n} closeout 가능하다. "
                f"batchId={o['batchId']}, signalAsOfDate={o['signalAsOfDate']}, dry-run COMPLETED, "
                f"eligibility 정상, realOrderCount=0{warns}. "
                f"Phase MF-SEQ{n}-ONE-SHOT-CLOSEOUT 원샷 지시문(ticket→승인→apply→backup→official publish→"
                f"rankings publish→push→live→closure)을 만들어줘.")
    return body


def to_txt(o) -> str:
    md = to_markdown(o)
    tail = ["", "════════════════════════════════════════", "[자동화 채팅창에 붙일 요청]",
            "아래 문단을 자동화 채팅창(ChatGPT)에 그대로 붙여넣으면 다음 단일 작업 지시문을 만들 수 있다.", "",
            _chatgpt_request(o), ""]
    return md + "\n" + "\n".join(tail)


# ============================== main ==============================

def _open_notepad(path):
    try:
        subprocess.Popen(["notepad.exe", str(path)])  # noqa: S603,S607 — 조회용 열기(실패 무시)
        return True
    except Exception:  # noqa: BLE001
        return False


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 일일 read-only 관찰 보고 생성(write=reports 2개만)")
    ap.add_argument("--no-notepad", action="store_true", help="notepad.exe 열기 생략")
    ap.add_argument("--date", default=None, help="관찰 날짜 YYYY-MM-DD(기본 오늘 KST)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    try:  # cp949 콘솔에서도 한글/em-dash 출력 안전
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    today = args.date or C.today_kst_iso()
    o = build_observation(today)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    md_path = REPORTS_DIR / "magic-daily-observe-latest.md"
    txt_path = REPORTS_DIR / "magic-daily-observe-latest.txt"
    md_path.write_text(to_markdown(o), encoding="utf-8")
    txt_path.write_text(to_txt(o), encoding="utf-8-sig")  # notepad 한글 안정(BOM)

    opened = False
    if not args.no_notepad:
        opened = _open_notepad(txt_path)

    if args.json:
        print(json.dumps({k: v for k, v in o.items()
                          if k not in ("canonical", "dryRun", "signal", "statusReport")},
                         ensure_ascii=False, indent=2))
    else:
        print(f"[OBSERVE {today}] 전체 판정: {o['verdict']} · closeout {o['closeoutState']} · "
              f"seq exp={o['expectedProposedSequence']}/act={o['actualProposedSequence']}")
        print(f"  md: {md_path}")
        print(f"  txt: {txt_path} (notepad opened={opened})")
        print(f"  다음: {next_task_line(o)}")
    return 0  # 관찰 스크립트 자체는 정상 생성 시 항상 0(판정은 파일 내용으로 전달)


if __name__ == "__main__":
    sys.exit(main())
