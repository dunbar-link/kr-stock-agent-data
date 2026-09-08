#!/usr/bin/env python3
"""R33C0 회귀 — frozen base 불변 · continuation 증분 · 달력 · 스케줄러 · 경계.

WABABA-RESEARCH-DATA-PRODUCER-RESTORE-AND-SCHEDULE-R33C0

계층:
  L1 frozen   — 상위 동결 hash + frozen base 상태 불변
  L2 contract — producer contract hash·조항
  L3 cont     — continuation 분리·중복·원자성·union view
  L4 calendar — 휴장표 교차검증·관측 연장·향후 signal/entry
  L5 daily    — run-daily 결과 enum·idempotency·예산
  L6 pit      — PIT 차단 사유 표면화(거짓 green 금지)
  L7 sched    — 기존 task 재사용·신규 task/trigger 0·hook 격리
  L8 bound    — network/credential/raw Git/public/실주문 경계

사용: python scripts/research/test_r33c0_research_data_producer.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r33c0_research_data_producer as R      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")

PASS = FAIL = NOTRUN = 0

BASELINE = {"officialDays": 3911, "officialMax": "2026-07-31",
            "pitSnapshots": 273, "pitMax": "2026-08-03",
            "calendarDays": 4870, "calendarMax": "2026-08-03"}


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" - {extra}" if extra else ""))


def notrun(name, why):
    global NOTRUN
    NOTRUN += 1
    print(f"  NOT_RUN  {name} - {why}")


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def t_l1():
    print("\n[L1] frozen 정본·base 불변")
    h = R.upstream_hashes()
    ck("upstream precommit 전부 일치", h["allMatch"])
    ck("R31 dataset manifest 3439dec9", h["r31DatasetManifest"]["match"])
    import r33a_translation_precommit as C
    import r33b_precommit as P
    ck("R33A contract hash", C.contract_hash() == P.SOURCE_CONTRACT_HASH)
    ck("R33B spec hash", P.spec_hash().startswith("fea25545"))
    fb = R.frozen_base_state()
    for k, v in BASELINE.items():
        ck(f"frozen base 불변: {k}={v}", fb[k] == v, str(fb[k]))
    ck("frozen official 은 2026-07-31 에서 멈춰 있음",
       fb["officialMax"] == "2026-07-31")
    ck("frozen pit 은 2026-08-03 에서 멈춰 있음", fb["pitMax"] == "2026-08-03")


def t_l2():
    print("\n[L2] producer contract")
    c, C_ = R.contract_hash(), R.CONTRACT
    ck("contract hash 결정성", c == R.contract_hash())
    r = subprocess.run([sys.executable, "-c",
                        "import sys;sys.path.insert(0,r'%s');"
                        "import r33c0_research_data_producer as R;"
                        "print(R.contract_hash())" % SRC],
                       capture_output=True, text=True)
    ck("contract hash 별도 프로세스 동일", r.stdout.strip() == c)
    ck("frozen base 수정 금지 조항", C_["frozenBaseMutationAllowed"] is False)
    ck("continuation root 분리", C_["continuationRoot"].endswith("prospective-research-data"))
    ck("직접 거래대금만", C_["directTradeValueOnly"] is True)
    ck("close×volume 대체 금지", C_["priceXVolumeProxy"] is False)
    ck("신규 task 금지", C_["newScheduledTaskAllowed"] is False)
    ck("신규 trigger 금지", C_["newTriggerAllowed"] is False)
    ck("무한 polling 금지", C_["unboundedPolling"] is False)
    ck("historical OOS backfill 금지", C_["historicalOosBackfill"] is False)
    ck("R33C engine 금지", C_["r33cEngineAllowed"] is False)
    ck("2026-09-01 사전활성 미적격",
       C_["preActivationSignalStatus"] == "PRE_ACTIVATION_NOT_ELIGIBLE")


def t_l3():
    print("\n[L3] continuation 분리·무결성")
    cont = R.continuation_days()
    ck("continuation 존재", len(cont) > 0, str(len(cont)))
    ck("continuation 중복 0", len(cont) == len(set(cont)))
    ck("continuation 은 frozen base 이후만",
       all(d > BASELINE["officialMax"] for d in cont), cont[:2] if cont else "")
    ck("continuation root 는 frozen 과 다른 경로",
       R.CONT_DAILY != R.FROZEN_OFFICIAL)
    uni = R.union_official_days()
    ck("union = base + continuation",
       len(uni) == BASELINE["officialDays"] + len(cont), str(len(uni)))
    ck("union max 가 continuation max", uni[-1] == cont[-1])
    d = cont[0]
    rows = R.load_official_day(d)
    ck("union 로더 동작", rows is not None and len(rows) > 0)
    if rows:
        r0 = next(iter(rows.values()))
        for f in ("open", "close", "volume", "traded_value", "market_cap", "shares"):
            ck(f"필수 필드 {f}", f in r0)
        ck("source 가 공식 포털",
           "PUBLIC_DATA_PORTAL" in (r0.get("source") or ""), r0.get("source"))
        ck("ticker 6자리", all(len(t) == 6 for t in list(rows)[:50]))


def t_l4():
    print("\n[L4] calendar")
    hv = R.validate_holiday_table()
    ck("휴장표 관측 교차검증 통과", hv["validated"] is True, str(hv.get("mismatchCount")))
    ck("불일치 0", hv["mismatchCount"] == 0)
    ck("휴장표는 기존 저장소 정본",
       "build_magic_signal_package" in hv["holidayTableSource"])
    ck("휴장표가 향후까지 커버", hv["coverage"]["max"] >= "2026-12-31",
       str(hv["coverage"]))
    calp = R.CONT_CAL / "observed.json"
    ck("관측 달력 생성됨", calp.exists())
    if calp.exists():
        cal = json.loads(calp.read_text(encoding="utf-8"))
        ck("frozen base through 보존",
           cal["frozenBaseThrough"] == BASELINE["calendarMax"])
        ck("관측 연장됨", cal["observedThrough"] > BASELINE["calendarMax"])
        ck("달력 중복 0", cal["duplicateCount"] == 0)
        ck("append 수 > 0", cal["continuationAppended"] > 0)
    up = R.planned_monthly_signals(R.union_official_days()[-1], 2)
    ck("향후 signal 2개 계산", len(up) == 2, str(up))
    ck("첫 향후 signal 이 미래",
       up[0]["signalDate"] > R.union_official_days()[-1], str(up[0]))
    ck("각 signal 에 entry session", all(s["entrySession"] for s in up))
    ck("entry 는 signal 다음 세션",
       all(s["entrySession"] > s["signalDate"] for s in up))
    ck("2026-10-01 이 다음 signal", up[0]["signalDate"] == "2026-10-01", str(up[0]))


def t_l5():
    print("\n[L5] run-daily")
    v1, st1 = R.run_daily()
    ck("run-daily enum 유효",
       v1 in ("UPDATED", "NO_CHANGE_FRESH", "SOURCE_NOT_READY_BOUNDED",
              "DATA_INTEGRITY_BLOCKED", "AUTH_BLOCKED",
              "PROVIDER_TRANSIENT_FAILURE", "FROZEN_BASE_DIVERGED"), v1)
    ck("frozen base 미변경(run 후)", R.frozen_base_state()["officialMax"]
       == BASELINE["officialMax"])
    v2, st2 = R.run_daily()
    ck("idempotent 재실행", v1 == v2, f"{v1}/{v2}")
    ck("두 번째도 through 동일",
       st1["officialDailyThrough"] == st2["officialDailyThrough"])
    ck("일일 예산 준수", st2.get("apiCalls", 0) <= R.DAILY_HARD_MAX_CALLS)
    ck("하루 최대 날짜 제한 존재", R.DAILY_MAX_DATES <= 10)
    ck("status artifact 생성", (RD / "r33c0-producer-status-latest.json").exists())
    ck("oosCohortCount 0", st2["oosCohortCount"] == 0)
    ck("r33cLedgerCount 0", st2["r33cLedgerCount"] == 0)
    ck("paperOnly", st2["paperOnly"] is True)
    ck("realMoneyApproved False", st2["realMoneyApproved"] is False)
    d = R.run_daily(dry_run=True)
    ck("dry-run 은 write 안 함", d[0] in ("DRY_RUN", "NO_CHANGE_FRESH"))


def t_l6():
    print("\n[L6] PIT 차단 표면화")
    p = R.pit_status()
    ck("PIT 상태 BLOCKED", p["status"] == "BLOCKED_UPSTREAM_CREDENTIAL")
    ck("차단 사유 명시", "KRX 로그인" in p["reason"])
    ck("BM 계산 불가 명시", p["bmComputableFromOfficialSource"] is False)
    ck("SIZE 는 계산 가능 명시", p["sizeComputableFromOfficialSource"] is True)
    ck("미생산 signal 목록", len(p["dueSignalsNotProduced"]) > 0)
    ck("PIT 최신이 frozen 그대로", p["latestSnapshot"] == BASELINE["pitMax"])
    st = R.build_status("test")
    ck("dataReadyForNextSignal False (거짓 green 금지)",
       st["dataReadyForNextSignal"] is False)
    ck("producerVerdict 가 PIT 차단 명시",
       "PIT_BLOCKED" in st["producerVerdict"], st["producerVerdict"])


def t_l7():
    print("\n[L7] 스케줄러·hook")
    host = ROOT / "scripts" / "magic_morning_combined_report.py"
    s = host.read_text(encoding="utf-8")
    ck("host script 에 hook 추가됨", "_r33c0_producer_hook" in s)
    ck("hook 이 try/except 격리", "except Exception" in s
       and "HOOK_FAILED" in s)
    ck("silent failure 아님(표면화)", "HOOK_FAILED" in s)
    ck("hook 은 return 전 1회만", s.count("_r33c0_producer_hook()") == 2)
    ck("core return 0 유지", "    return 0" in s)
    ps = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "$t=Get-ScheduledTask | Where-Object {$_.TaskName -like '*R33C0*' -or "
         "$_.TaskName -like '*r33c0*'}; @($t).Count"],
        capture_output=True, text=True, timeout=90)
    ck("신규 Scheduled Task 0", ps.stdout.strip() in ("0", ""), ps.stdout.strip())
    ps2 = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "$x=Get-ScheduledTask -TaskName 'Wababa Magic Morning Combined Report';"
         "($x.Triggers | ForEach-Object {$_.StartBoundary}) -join ','"],
        capture_output=True, text=True, timeout=90)
    ck("host task trigger 07:40 유지", "07:40" in ps2.stdout, ps2.stdout.strip())


def t_l8():
    print("\n[L8] 경계")
    s = (SRC / "r33c0_research_data_producer.py").read_text(encoding="utf-8")
    for bad in ("KRX_OPENAPI_AUTH_KEY", "AUTH_KEY", "os.environ", "getenv",
                "serviceKey="):
        ck(f"금지 토큰 없음: {bad}", bad not in s)
    # pykrx 는 "왜 PIT 가 막혔는지" 설명에 등장한다. 단어가 아니라 **사용**을 본다.
    pykrx_use = re.findall(r"^\s*(?:import\s+pykrx|from\s+pykrx)", s, re.M)
    ck("pykrx 실제 import 0", not pykrx_use, str(pykrx_use[:2]))
    ck("pykrx 는 차단사유 설명으로만 등장", "pykrx" in s and not pykrx_use)
    toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", s)
            if not re.fullmatch(r"[0-9a-f]{64}", w)]
    ck("장문 연속토큰 0", not toks, str(toks[:2]))
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8")
    ck("continuation 은 gitignore 대상(_cache/)", "_cache/" in gi)
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 _cache/reports 없음",
       not any(x.startswith(("_cache", "reports")) for x in staged), str(staged))
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    if PUBLIC_REPO.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
        ck("public repo 에 r33c0 없음",
           "r33c0" not in git(["status", "--short"], cwd=PUBLIC_REPO).lower())
    ck("R33C ledger 미생성", not (ROOT / "_cache" / "oos-r33c").exists())
    ck("R33C engine 미생성", not (SRC / "r33c_oos_paper.py").exists())
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in s)
    ck("실주문 호출 0",
       not re.findall(r"\b(place_order|submit_order|send_order)\s*\(", s))
    md = WD / "wababa-research-data-producer-restore-and-schedule-r33c0-latest.md"
    if not md.exists():
        notrun("R33C0 canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED|WAIT)$", first)
           is not None, first)
        for need in ("2026-10-01", "BLOCKED_UPSTREAM_CREDENTIAL",
                     "REAL_MONEY_NOT_APPROVED"):
            ck(f"보고 항목: {need}", need in body)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7, t_l8):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
