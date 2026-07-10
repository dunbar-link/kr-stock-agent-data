#!/usr/bin/env python3
"""마법공식 — 다음날 오전 통합 관찰 보고 생성기 (Phase MF-MORNING-COMBINED-REPORT).

평일 오전 07:40 KST에 실행. 두 시점을 한 보고서로 묶는다.
- **어제**(직전 거래일) 장마감 관찰: signal/dry-run/status 산출물 + closeout 완료/대기 상태
- **오늘** 운영 준비: canonical/live 정합, 스케줄러 4종 Ready·NextRunTime, 오늘 예상 seq/batchId

**read-only 전용.** ticket/approval/apply/backup/publish/commit/push/deploy/실주문 전부 안 한다.
파일 write는 reports/magic-morning-combined-latest.{md,txt} 두 개뿐. 기존 16:10 observe 스크립트/
작업은 건드리지 않고, 재사용 가능한 read-only 헬퍼만 magic_daily_observe_report에서 import한다.

예) python scripts/magic_morning_combined_report.py                 # 오늘=오늘 KST, 어제=직전 거래일
    python scripts/magic_morning_combined_report.py --date 2026-07-10 --no-notepad --json
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date as _date, time as _time, timedelta
from pathlib import Path

import magic_daily_common as C
import magic_daily_observe_report as OB   # read-only 헬퍼 재사용(그 모듈/작업 무변경)

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
BACKUP_ROOT = Path.home() / "OneDrive" / "WababaBackup" / "magic-formula-official"

CORE_TASKS = ("Wababa Magic Daily Signal", "Wababa Magic Daily Dry Run", "Wababa Magic Daily Status")
OBSERVE_TASK = "Wababa Magic Daily Observe Report"
MORNING_TASK = "Wababa Magic Morning Combined Report"
ALL_TASKS = CORE_TASKS + (OBSERVE_TASK, MORNING_TASK)
CORE_TIMES = {"Wababa Magic Daily Signal": "15:40", "Wababa Magic Daily Dry Run": "15:45",
              "Wababa Magic Daily Status": "16:05"}


# ============================== read-only 수집 ==============================

def previous_trading_day(today_iso: str) -> str | None:
    """today 이전의 가장 가까운 KRX 거래일(주말/휴장 skip). 없으면 None."""
    try:
        d = _date.fromisoformat(str(today_iso)[:10])
    except ValueError:
        return None
    for i in range(1, 11):
        cand = (d - timedelta(days=i)).isoformat()
        if C.is_krx_trading_day(cand):
            return cand
    return None


def query_scheduler_state(names=ALL_TASKS, *, runner=None) -> list[dict]:
    """스케줄러 State + LastRunTime/Result/NextRunTime(read-only). runner 주입 시 OS 접근 0(테스트)."""
    runner = runner or OB._powershell
    ps_names = ",".join("'" + n.replace("'", "''") + "'" for n in names)
    ps = (
        "$ns=@(" + ps_names + ");"
        "$r=foreach($n in $ns){"
        "$t=Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue;"
        "$i=if($t){Get-ScheduledTaskInfo -TaskName $n -ErrorAction SilentlyContinue}else{$null};"
        "[pscustomobject]@{name=$n;found=[bool]$t;state=if($t){[string]$t.State}else{''};"
        "lastRunTime=if($i -and $i.LastRunTime){$i.LastRunTime.ToString('yyyy-MM-ddTHH:mm:ss')}else{''};"
        "lastTaskResult=if($i){[int]$i.LastTaskResult}else{$null};"
        "nextRunTime=if($i -and $i.NextRunTime){$i.NextRunTime.ToString('yyyy-MM-ddTHH:mm:ss')}else{''}}};"
        "$r | ConvertTo-Json -Depth 4"
    )
    try:
        data = json.loads(runner(ps))
    except (ValueError, TypeError):
        return [{"name": n, "found": False, "state": "", "lastRunTime": "", "lastTaskResult": None,
                 "nextRunTime": "", "queryError": True} for n in names]
    if isinstance(data, dict):
        data = [data]
    return data


def git_state_ex(repo_path) -> dict:
    """OB.git_state + behind(원격 대비 뒤처짐) 추가."""
    st = OB.git_state(repo_path)
    out, _ = OB._run(["git", "-C", str(repo_path), "rev-list", "--count", "HEAD..@{u}"])
    try:
        st["behind"] = int(out.strip())
    except (ValueError, AttributeError):
        st["behind"] = 0
    return st


def observe_live_full() -> dict:
    """운영 URL JSON 1회 fetch로 summary/rankings/http 종합(deploy 호출 0)."""
    if OB.LV is None:
        return {"error": "live module unavailable"}
    base = OB.BASE_URL
    now = C.now_kst().isoformat()
    bust = now.replace(":", "").replace("-", "").replace(".", "")[:18]
    try:
        js_status, js_text = OB.LV._fetch(f"{base}/data/recommendation-history.json?_={bust}")
        perf_status, _ = OB.LV._fetch(f"{base}/performance")
        rank_status, _ = OB.LV._fetch(f"{base}/magic-formula/rankings")
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}
    try:
        doc = json.loads(js_text) if js_text else {}
    except ValueError:
        doc = {}
    summary = doc.get("magicOfficialSummary") or {}
    rankings = doc.get("magicOfficialRankings") or {}
    tdays = doc.get("magicOfficialTradeDays") or []
    return {
        "jsonHttp": js_status or 0, "performanceHttp": perf_status, "rankingsHttp": rank_status,
        "deployedSequence": summary.get("officialSequence"), "deployedTotalAsset": summary.get("totalAsset"),
        "deployedDataDate": summary.get("dataDate"), "tradeDayCount": len(tdays),
        "rankingsDataDate": rankings.get("dataDate"), "rankingsCheapCount": len(rankings.get("cheapTop100") or []),
    }


def backup_exists(date_iso: str) -> bool:
    try:
        return (BACKUP_ROOT / str(date_iso)).is_dir()
    except OSError:
        return False


def _find_task(sched, name):
    return next((s for s in (sched or []) if s.get("name") == name), None)


# ============================== 판정(순수 함수) ==============================

def classify_morning(*, today, yesterday, now_dt, is_today_trading, is_yesterday_trading,
                     canonical, y_dryrun, evidence, sched, live, repo1, repo2, backup_done) -> dict:
    """어제 관찰 + 오늘 준비 → 통합 판정(순수 함수). 시간/OS/네트워크 주입. write 0."""
    canon_seq = (canonical or {}).get("officialSequence")
    y_seq = (y_dryrun or {}).get("proposedSequence")
    y_batch = (y_dryrun or {}).get("proposedBatchId")
    today_expected_seq = (y_seq + 1) if isinstance(y_seq, int) else (
        (canon_seq + 1) if isinstance(canon_seq, int) else None)
    today_expected_batch = f"MF-BATCH-{today}"

    blocked, action_warns, known_warns = [], [], []

    # --- 어제 산출물 없음 → WAIT ---
    if y_dryrun is None:
        why = ("직전 거래일 dry-run 산출물 없음(휴장/스케줄러 미실행 가능)"
               if is_yesterday_trading else "직전 거래일 판단 불가/비거래일 — 관찰 대상 없음")
        return _result("WAIT", why, "NA", today, yesterday, y_seq, y_batch,
                       today_expected_seq, today_expected_batch, canon_seq, blocked, action_warns,
                       known_warns, backup_done)

    # --- 어제 dry-run 게이트 ---
    status = y_dryrun.get("status") or y_dryrun.get("runStatus")
    if status != "COMPLETED":
        blocked.append(f"어제 dry-run status={status!r} != COMPLETED (blockedCode={y_dryrun.get('blockedCode')})")
    if y_dryrun.get("blockedCode"):
        blocked.append(f"어제 dry-run blockedCode={y_dryrun.get('blockedCode')} (BLOCKED_LOOKAHEAD 등)")
    if y_dryrun.get("lookAheadValidationPassed") is not True:
        blocked.append("어제 lookAheadValidationPassed != true")
    if y_dryrun.get("missingEvalCodes"):
        blocked.append(f"어제 missingEvalCodes 존재: {y_dryrun.get('missingEvalCodes')}")
    if y_dryrun.get("productionWriteCount") not in (0, None):
        blocked.append(f"어제 productionWriteCount={y_dryrun.get('productionWriteCount')} != 0")
    if y_dryrun.get("readOnlyUnchanged") is not True:
        blocked.append("어제 readOnlyUnchanged != true")
    if y_dryrun.get("buyCount") != 10:
        blocked.append(f"어제 buyCount={y_dryrun.get('buyCount')} != 10")

    # evidence
    if not evidence:
        blocked.append("어제 evidence/rankings 산출물 없음(signal 패키지 rankings.json 누락)")
    else:
        if (evidence.get("top10Count") or 0) < 10:
            blocked.append(f"어제 top10 부족(top10Count={evidence.get('top10Count')})")
        if (evidence.get("top100Count") or 0) < 100:
            blocked.append(f"어제 top100 부족(top100Count={evidence.get('top100Count')})")

    # --- closeout 상태 ---
    closeout_state = "NA"
    if isinstance(y_seq, int) and isinstance(canon_seq, int):
        if canon_seq >= y_seq:
            closeout_state = "ALREADY_APPLIED"
        elif canon_seq == y_seq - 1:
            closeout_state = "READY_TO_CLOSEOUT"
        else:
            closeout_state = "STALE"
            action_warns.append(f"canonical={canon_seq} < 어제 seq-1({y_seq - 1}) — 예상 밖 지연")

    # --- canonical/live 정합 ---
    live_seq = (live or {}).get("deployedSequence")
    if live and live.get("error"):
        action_warns.append(f"live 조회 실패: {live.get('error')}")
    elif live:
        for label, key in (("home/json", "jsonHttp"), ("performance", "performanceHttp"),
                           ("rankings", "rankingsHttp")):
            code = live.get(key)
            if code in (404, 500, 502, 503):
                blocked.append(f"live {label} HTTP {code} (운영 장애)")
            elif code != 200:
                action_warns.append(f"live {label} HTTP {code} != 200")
        if isinstance(live_seq, int) and isinstance(canon_seq, int) and live_seq != canon_seq:
            blocked.append(f"canonical seq {canon_seq} != live seq {live_seq} (배포/publish 불일치)")

    # --- 스케줄러: 핵심 3종 필수, 리포트 2종은 경고 ---
    for name in CORE_TASKS:
        t = _find_task(sched, name)
        if not t or not t.get("found"):
            blocked.append(f"스케줄러 '{name}' 없음/조회실패")
        elif str(t.get("state")).lower() == "disabled":
            blocked.append(f"스케줄러 '{name}' Disabled")
        elif t.get("lastTaskResult") not in (0, None):
            blocked.append(f"스케줄러 '{name}' LastTaskResult={t.get('lastTaskResult')} != 0")
    for name in (OBSERVE_TASK, MORNING_TASK):
        t = _find_task(sched, name)
        if not t or not t.get("found"):
            action_warns.append(f"리포트 스케줄러 '{name}' 미등록(정보)")
        elif str(t.get("state")).lower() == "disabled":
            action_warns.append(f"리포트 스케줄러 '{name}' Disabled")

    # repo dirty / staged / behind
    _repo_warns(repo1, repo2, blocked, action_warns, known_warns)

    verdict = "BLOCKED" if blocked else ("WARNING" if action_warns else "PASS")
    # 오늘 비거래일 + 어제 이미 반영 + 문제없음 → WAIT(휴장, 오늘 할 일 없음; 실패 아님)
    if verdict == "PASS" and not is_today_trading and closeout_state == "ALREADY_APPLIED":
        return _result("WAIT", "오늘 KRX 비거래일 — 어제 closeout 완료, 오늘 할 일 없음", closeout_state,
                       today, yesterday, y_seq, y_batch, today_expected_seq, today_expected_batch,
                       canon_seq, blocked, action_warns, known_warns, backup_done)

    hint = _hint(verdict, closeout_state, y_seq, today_expected_seq)
    return _result(verdict, hint, closeout_state, today, yesterday, y_seq, y_batch,
                   today_expected_seq, today_expected_batch, canon_seq, blocked, action_warns,
                   known_warns, backup_done)


def _repo_warns(repo1, repo2, blocked, action_warns, known_warns):
    for f in sorted(set((repo2 or {}).get("modified") or [])):
        (known_warns if f in OB.EXPECTED_WARN_REPO2 else action_warns).append(
            f"REPO2 {f} M" + ("" if f in OB.EXPECTED_WARN_REPO2 else " (예상 밖 dirty)"))
    for f in sorted(set((repo1 or {}).get("modified") or [])):
        if f in OB.EXPECTED_WARN_REPO1:
            known_warns.append(f"REPO1 {f} M")
        elif f == OB.REPO1_PUBLIC_REL:
            action_warns.append(f"REPO1 {f} 미커밋 — closeout 전 clean 정리 필요")
        else:
            action_warns.append(f"REPO1 {f} M (예상 밖 dirty)")
    for label, r in (("REPO1", repo1), ("REPO2", repo2)):
        if (r or {}).get("stagedCount"):
            blocked.append(f"{label} staged {r['stagedCount']}건 (커밋 위험)")
        if (r or {}).get("behind"):
            blocked.append(f"{label} behind {r['behind']} (원격 뒤처짐)")


def _hint(verdict, closeout_state, y_seq, today_expected_seq):
    if verdict == "BLOCKED":
        return "blocker 존재 — 원인분리 전까지 운영 진행 금지"
    if closeout_state == "READY_TO_CLOSEOUT":
        return f"어제 seq={y_seq} closeout 대기 — 오늘 closeout 가능"
    if closeout_state == "ALREADY_APPLIED":
        return f"어제 seq={y_seq} 반영 완료 — 오늘 예상 seq={today_expected_seq} 관찰 대기"
    return "통합 관찰 완료"


def _result(verdict, hint, closeout_state, today, yesterday, y_seq, y_batch,
            today_expected_seq, today_expected_batch, canon_seq, blocked, action_warns,
            known_warns, backup_done):
    return {
        "verdict": verdict, "hint": hint, "closeoutState": closeout_state,
        "today": today, "yesterday": yesterday,
        "yesterdaySequence": y_seq, "yesterdayBatchId": y_batch,
        "todayExpectedSequence": today_expected_seq, "todayExpectedBatchId": today_expected_batch,
        "canonicalSequence": canon_seq, "backupDone": backup_done,
        "blocked": blocked, "actionWarns": action_warns, "knownWarns": known_warns,
    }


# ============================== 오케스트레이션 ==============================

def build_report(today, *, now_dt=None) -> dict:
    now_dt = now_dt or C.now_kst()
    yesterday = previous_trading_day(today)
    canonical = C.load_canonical_summary()
    y_dryrun = OB.read_json(C.REPORTS_DIR / f"dry-run-{yesterday}.json") if yesterday else None
    y_signal = OB.read_json(C.REPORTS_DIR / f"signal-{yesterday}.json") if yesterday else None
    y_status = OB.read_json(C.REPORTS_DIR / f"daily-status-{yesterday}.json") if yesterday else None
    signal_as_of = (y_dryrun or {}).get("signalAsOfDate")
    rankings_doc = OB.read_json(C.TEMP_ROOT / str(signal_as_of) / "rankings.json") if signal_as_of else None
    evidence = OB.collect_evidence(rankings_doc)
    sched = query_scheduler_state()
    live = observe_live_full()
    repo1 = git_state_ex(OB.REPO1_ROOT)
    repo2 = git_state_ex(OB.REPO2_ROOT)
    backup_done = backup_exists(yesterday) if yesterday else False
    verdict = classify_morning(
        today=today, yesterday=yesterday, now_dt=now_dt,
        is_today_trading=C.is_krx_trading_day(today),
        is_yesterday_trading=(C.is_krx_trading_day(yesterday) if yesterday else False),
        canonical=canonical, y_dryrun=y_dryrun, evidence=evidence, sched=sched, live=live,
        repo1=repo1, repo2=repo2, backup_done=backup_done)
    return {
        "schemaVersion": "magic-morning-combined-v1", "createdAt": now_dt.isoformat(), **verdict,
        "canonical": canonical, "yesterdayDryRun": y_dryrun, "yesterdaySignal": y_signal,
        "yesterdayStatus": y_status, "signalAsOfDate": signal_as_of, "evidence": evidence,
        "scheduler": sched, "live": live, "repo1": repo1, "repo2": repo2,
        "readOnly": True, "realOrderCount": 0, "noFakeTrade": True,
        "writeScope": "reports/magic-morning-combined-latest.{md,txt} only",
    }


# ============================== 렌더 ==============================

def _sched_line(s):
    tag = ""
    if s.get("queryError") or not s.get("found"):
        tag = " [미등록/조회실패]"
    elif str(s.get("state")).lower() == "disabled":
        tag = " [Disabled]"
    return (f"- {s.get('name')}: state={s.get('state') or '-'} last={s.get('lastRunTime') or '-'} "
            f"result={s.get('lastTaskResult')} next={s.get('nextRunTime') or '-'}{tag}")


def next_action_line(o):
    n = o["todayExpectedSequence"]
    cs = o["closeoutState"]
    v = o["verdict"]
    if v == "BLOCKED":
        return f"Phase MF-SEQ{o['yesterdaySequence']}-BLOCKER-FIX: blocker 원인분리 (운영 진행 금지)"
    if v == "WAIT":
        return "Phase MF-MORNING-RECHECK: read-only 재관찰 또는 휴장 대기"
    if cs == "READY_TO_CLOSEOUT":
        return (f"Phase MF-SEQ{o['yesterdaySequence']}-ONE-SHOT-CLOSEOUT: 어제 seq={o['yesterdaySequence']} "
                "ticket→승인→apply→backup→official publish→rankings publish→push→live→closure")
    if cs == "ALREADY_APPLIED":
        return (f"Phase MF-SEQ{n}-OBSERVE: 오늘 16:05 이후(또는 내일 오전 통합보고) seq={n} read-only 관찰 대기")
    return "Phase MF-MORNING-RECHECK: read-only 재관찰"


def to_markdown(o):
    dr, ev, lv, c = (o["yesterdayDryRun"] or {}), (o["evidence"] or {}), (o["live"] or {}), (o["canonical"] or {})
    r1 = ev.get("rank1") or {}
    L = [f"전체 판정: {o['verdict']}", "",
         f"# 와바바 마법공식 오전 통합 보고 — {o['today']} (어제 {o['yesterday']} 관찰)", "",
         "## 1. 요약",
         f"- 생성 시각: {o['createdAt']}",
         f"- 대상 거래일(어제): {o['yesterday']} / 오늘: {o['today']}",
         f"- 어제 proposedSequence: {o['yesterdaySequence']} / batchId: {o['yesterdayBatchId']}",
         f"- 어제 closeout 상태: {o['closeoutState']} — {o['hint']}",
         f"- 오늘 운영 준비: 예상 seq {o['todayExpectedSequence']} / {o['todayExpectedBatchId']}", ""]
    L += ["## 2. 어제 장마감 관찰 결과",
          f"- signal 존재: {o['yesterdaySignal'] is not None} / dry-run 존재: {o['yesterdayDryRun'] is not None} / status 존재: {o['yesterdayStatus'] is not None}",
          f"- status/runStatus: {dr.get('status') or dr.get('runStatus')} / blockedCode: {dr.get('blockedCode')}",
          f"- lookAheadValidationPassed: {dr.get('lookAheadValidationPassed')}",
          f"- buyCount: {dr.get('buyCount')} / eval(openPrices): {len(dr.get('openPrices') or {})} / missingEvalCodes: {dr.get('missingEvalCodes')}",
          f"- productionWriteCount: {dr.get('productionWriteCount')} / canonicalChanged: {dr.get('canonicalChanged', False)}",
          f"- readOnlyUnchanged: {dr.get('readOnlyUnchanged')} / noFakeTrade: {dr.get('noFakeTrade')} / realOrderCount: 0", ""]
    L += ["## 3. 어제 closeout 상태",
          f"- canonical officialSequence: {c.get('officialSequence')} / live deployedSequence: {lv.get('deployedSequence')}",
          f"- 어제 batchId 반영: {isinstance(o['canonicalSequence'], int) and isinstance(o['yesterdaySequence'], int) and o['canonicalSequence'] >= o['yesterdaySequence']}",
          f"- official publish(live seq==canonical): {lv.get('deployedSequence') == c.get('officialSequence')}",
          f"- rankings publish(live rankings dataDate): {lv.get('rankingsDataDate')} / cheapTop100 {lv.get('rankingsCheapCount')}",
          f"- OneDrive backup({o['yesterday']}) 존재: {o['backupDone']}",
          f"- closeoutState: {o['closeoutState']}", ""]
    L += ["## 4. 오늘 운영 준비 OK",
          f"- 오늘 날짜: {o['today']} / 현재 canonical seq: {c.get('officialSequence')} / 현재 live seq: {lv.get('deployedSequence')}",
          f"- 오늘 예상 proposedSequence: {o['todayExpectedSequence']} / 예상 batchId: {o['todayExpectedBatchId']}",
          "- 스케줄러:"] + ["  " + _sched_line(s) for s in (o["scheduler"] or [])]
    core_today = [f"{CORE_TIMES.get(s['name'])}={str((s.get('nextRunTime') or '')).startswith(o['today'])}"
                  for s in (o["scheduler"] or []) if s.get("name") in CORE_TIMES]
    L += [f"- 오늘 실행 예정(NextRunTime==오늘): {', '.join(core_today)}", ""]
    L += ["## 5. evidence / rankings (어제)",
          f"- top10: {ev.get('top10Count')}개 / top100: {ev.get('top100Count')}개 / evMethod: {ev.get('evMethod')}",
          f"- rank1: {r1.get('code')} {r1.get('name')} (value {r1.get('valueRank')}/quality {r1.get('profitabilityRank')}/combined {r1.get('combinedRank')})",
          f"- earningsYield: {r1.get('earningsYield')} / returnOnCapital: {r1.get('returnOnCapital')} / signalClosePrice: {r1.get('signalClosePrice')}",
          f"- live magicOfficialRankings dataDate: {lv.get('rankingsDataDate')}", ""]
    L += ["## 6. repo / live 상태",
          f"- REPO2 HEAD {o['repo2'].get('head')} ({o['repo2'].get('branch')}) behind={o['repo2'].get('behind')} modified={o['repo2'].get('modified')} staged={o['repo2'].get('stagedCount')}",
          f"- REPO1 HEAD {o['repo1'].get('head')} ({o['repo1'].get('branch')}) behind={o['repo1'].get('behind')} modified={o['repo1'].get('modified')} staged={o['repo1'].get('stagedCount')}",
          f"- live home/json {lv.get('jsonHttp')} / performance {lv.get('performanceHttp')} / rankings {lv.get('rankingsHttp')}"
          + (f" / error {lv.get('error')}" if lv.get('error') else ""),
          f"- live totalAsset {lv.get('deployedTotalAsset')} / tradeDays {lv.get('tradeDayCount')}", ""]
    L += ["### known-warn"] + ([f"- {w}" for w in o["knownWarns"]] or ["- (없음)"])
    if o["actionWarns"]:
        L += ["### action-needed(WARNING 유발)"] + [f"- {w}" for w in o["actionWarns"]]
    if o["blocked"]:
        L += ["### BLOCKED 원인"] + [f"- {w}" for w in o["blocked"]]
    L += ["", "## 다음 단일 작업", f"- {next_action_line(o)}", "",
          "> read-only 통합 관찰만 수행. apply/backup/publish/commit/push/deploy/실주문 0."]
    return "\n".join(L)


def _chatgpt_request(o):
    n_close = o["yesterdaySequence"]
    if o["verdict"] == "BLOCKED":
        return (f"와바바 마법공식 어제({o['yesterday']}) 관찰 BLOCKED다. 원인: {'; '.join(o['blocked']) or '상세 확인'}. "
                "이 보고를 기준으로 와바바 blocker 원인분리 지시문을 만들어줘.")
    if o["verdict"] == "WAIT":
        return (f"와바바 마법공식 {o['today']} 오전 통합보고 WAIT다({o['hint']}). "
                "이 보고를 기준으로 와바바 read-only 재관찰 또는 휴장 대기 안내를 만들어줘.")
    if o["closeoutState"] == "READY_TO_CLOSEOUT":
        return (f"와바바 마법공식 어제({o['yesterday']}) seq={n_close} closeout 대기다(판정 {o['verdict']}). "
                f"이 보고를 기준으로 와바바 seq{n_close} ONE-SHOT-CLOSEOUT 지시문을 만들어줘.")
    # ALREADY_APPLIED
    return (f"와바바 마법공식 어제({o['yesterday']}) seq={n_close} closeout 완료 확인 + 오늘 운영 준비 보고입니다"
            f"(판정 {o['verdict']}). closeout 지시문은 만들지 말고, 오늘 16:05 이후 또는 다음 오전 통합보고를 기다리라고 안내해줘.")


def to_txt(o):
    tail = ["", "════════════════════════════════════════", "[자동화 채팅창에 붙일 요청]",
            "아래 문단을 자동화 채팅창(ChatGPT)에 그대로 붙여넣으면 다음 지시를 받을 수 있다.", "",
            _chatgpt_request(o), ""]
    return to_markdown(o) + "\n" + "\n".join(tail)


# ============================== main ==============================

def open_in_notepad(txt_path, *, launcher=None) -> bool:
    """latest.txt를 notepad.exe로 연다(System32 전체경로 우선 + PATH fallback).

    실패해도 예외 없이 False 반환 — notepad 실패는 보고서 생성 실패가 아니다(비대화형 세션 등).
    launcher 주입 시 실제 프로세스 실행 0(테스트)."""
    launcher = launcher or (lambda exe, path: subprocess.Popen([exe, str(path)]))  # noqa: S603
    system_root = os.environ.get("SystemRoot") or r"C:\Windows"
    for exe in (os.path.join(system_root, "System32", "notepad.exe"), "notepad.exe"):
        try:
            launcher(exe, txt_path)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 오전 통합 관찰 보고(read-only, write=reports 2개만)")
    ap.add_argument("--no-notepad", action="store_true", help="notepad.exe 열기 생략")
    ap.add_argument("--date", default=None, help="기준(오늘) YYYY-MM-DD (기본 오늘 KST)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    today = args.date or C.today_kst_iso()
    o = build_report(today)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    md_path = REPORTS_DIR / "magic-morning-combined-latest.md"
    txt_path = REPORTS_DIR / "magic-morning-combined-latest.txt"
    md_path.write_text(to_markdown(o), encoding="utf-8")
    txt_path.write_text(to_txt(o), encoding="utf-8-sig")

    opened = False
    if not args.no_notepad:
        opened = open_in_notepad(txt_path)

    if args.json:
        print(json.dumps({k: v for k, v in o.items() if k not in
                          ("canonical", "yesterdayDryRun", "yesterdaySignal", "yesterdayStatus")},
                         ensure_ascii=False, indent=2))
    else:
        print(f"[MORNING {today}] 전체 판정: {o['verdict']} · 어제 {o['yesterday']} closeout {o['closeoutState']} · "
              f"오늘 예상 seq={o['todayExpectedSequence']}")
        print(f"  md: {md_path}")
        print(f"  txt: {txt_path} (notepad opened={opened})")
        print(f"  다음: {next_action_line(o)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
