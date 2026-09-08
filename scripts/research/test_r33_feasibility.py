#!/usr/bin/env python3
"""R33 회귀 — prospective feasibility 관문. 네트워크 0 · API 0 · 인증키 접근 0.

WABABA-BM-SIZE-SEPARATE-OOS-PAPER-OBSERVATION-CONTRACT-R33

계층:
  L1 frozen    — R27/R31/R32 precommit 해시 + R32A source commit
  L2 cadence   — signal cadence 정본 복원 (§7)
  L3 timing    — 입력 가용시각 · liquidity look-ahead · same-day close 종속 (§6)
  L4 gate      — feasibility 판정이 결과독립적으로 동작하는지 (fixture)
  L5 nocreate  — BLOCKED 상태에서 계약·ledger·activation 이 생기지 않았는지 (§6·§9)
  L6 boundary  — raw/row-level Git 제외 · public repo · LEGACY_50D · 실주문 0
  L7 outputs   — 산출물 parse · 판정 헤더 · secret scan · git diff --check

§26 의 계약·dry-run·state-machine·runtime hook 항목은 §6 관문에서 멈췄기 때문에
산출물 자체가 없다. PASS 로 위장하지 않고 NOT_RUN 으로 사유와 함께 남긴다.

사용: python scripts/research/test_r33_feasibility.py
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

# 콘솔 코드페이지(cp949)에서도 한글·기호가 깨지지 않게 고정한다.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r33_feasibility as F                   # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")
AUTOMATION_REPO = Path(r"C:\work\ai-operating-system")

RESULT = RD / "r33-feasibility-latest.json"

PASS = FAIL = NOTRUN = 0


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


def git(args, cwd=ROOT):
    try:
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def load():
    if not RESULT.exists():
        return None
    return json.loads(RESULT.read_text(encoding="utf-8"))


# ── L1 frozen ─────────────────────────────────────────────────────────
def t_l1():
    print("\n[L1] frozen hash · source commit")
    h = F.frozen_hashes()
    for name, prefix in F.FROZEN.items():
        ck(f"{name} 해시 {prefix}", h[name]["match"], h[name]["sha256"][:12])
    ck("frozen 전체 일치", h["allMatch"])
    ck("SOURCE_COMMIT 0431003 고정", F.SOURCE_COMMIT == "0431003")
    ck("SOURCE_DECISION BOTH_SEPARATE", F.SOURCE_DECISION == "BOTH_SEPARATE")
    out = git(["cat-file", "-t", F.SOURCE_COMMIT]).strip()
    ck("source commit 존재", out == "commit", out)
    anc = subprocess.run(["git", "merge-base", "--is-ancestor",
                          F.SOURCE_COMMIT, "origin/main"],
                         cwd=ROOT, capture_output=True, text=True)
    ck("source commit 이 origin/main ancestor", anc.returncode == 0)


# ── L2 cadence ────────────────────────────────────────────────────────
def t_l2():
    print("\n[L2] signal cadence")
    d = load()
    if not d:
        notrun("cadence", "r33-feasibility-latest.json 없음")
        return
    c = d["signalCadence"]
    ck("cadence = MONTH_FIRST_TRADING_DAY", c["rule"] == "MONTH_FIRST_TRADING_DAY")
    ck("cadence 정본 복원", c["reproduced"] is True)
    ck("월 최초 거래일이 아닌 결정일 0", c["notMonthFirstCount"] == 0)
    ck("결정일 전부 실제 거래일", c["allComparedDatesAreTradingDays"] is True)
    ps = c["primarySample"]
    ck("primary sample 163", ps["cohorts"] == 163, str(ps["cohorts"]))
    ck("primary sample 연속 월간", ps["contiguousMonthly"] is True)
    ck("primary sample gap 0", ps["gapMonths"] == 0)
    ck("primary 범위 2010-02-01..2023-08-01",
       ps["range"]["min"] == "2010-02-01" and ps["range"]["max"] == "2023-08-01")
    ck("캘린더 밖 결정일은 대조에서 제외 명시",
       "monthsWithoutDecisionDateReason" in c)


# ── L3 timing ─────────────────────────────────────────────────────────
def t_l3():
    print("\n[L3] 입력 가용시각 · look-ahead")
    d = load()
    if not d:
        notrun("timing", "r33-feasibility-latest.json 없음")
        return
    t = d["inputTiming"]
    ck("163 cohort 전수 감사", t["cohortsAudited"] == 163, str(t["cohortsAudited"]))
    ck("liquidity look-ahead 0", t["liquidityLookAheadViolations"] == 0)
    for row in t["sample"]:
        ck(f"liquidity as-of < signal ({row['signalDate']})",
           row["liquidityAsOf"] < row["signalDate"])
        ck(f"liquidity window 20거래일 ({row['signalDate']})",
           row["liquidityWindow"]["days"] == 20)
    ck("ranking 입력 == entry 체결가 (163 전건)",
       t["sameInstantRankingAndEntryCount"] == 163)
    ck("주장 아닌 실측", "measuredNotAsserted" in t)

    i = d["sameDayCloseDependency"]
    ck("identity 감사 163 날짜", i["datesAudited"] == 163)
    s = i["sizeIdentity"]
    ck("marketCap == close×shares 100%", s["exactPct"] == 100.0, str(s["exactPct"]))
    ck("SIZE identity worst error 0", s["worstRelError"] == 0.0)
    ck("SIZE identity 행수 > 300k", s["rows"] > 300_000, str(s["rows"]))
    b = i["bmIdentity"]
    ck("PBR == close/BPS 공표반올림 내 100%",
       b["withinPublishedRoundingPct"] == 100.0, str(b["withinPublishedRoundingPct"]))
    ck("BM 허용치는 유도된 상한(넓히지 않음)", b["boundIsDerivedNotWidened"] is True)
    ck("entry price accessor 명시", "price(i, t)" in i["entryPrice"]["accessor"])
    ck("entry price == ranking 입력", i["entryPrice"]["identicalToRankingInput"] is True)


# ── L4 gate ───────────────────────────────────────────────────────────
def t_l4():
    print("\n[L4] feasibility 판정기 — 결과독립")
    ok = {"allMatch": True}
    cad_ok = {"reproduced": True}
    cad_no = {"reproduced": False}
    tim_ok = {"liquidityLookAheadViolations": 0, "sameInstantRankingAndEntryCount": 0}
    tim_same = {"liquidityLookAheadViolations": 0,
                "sameInstantRankingAndEntryCount": 163}
    tim_liq = {"liquidityLookAheadViolations": 3,
               "sameInstantRankingAndEntryCount": 0}

    v = F.verdict(ok, cad_ok, tim_ok, {})
    ck("전부 통과 → PASS", v["verdict"] == "PASS" and v["prospectivelyExecutable"])
    ck("PASS 시 blocker 0", len(v["blockers"]) == 0)

    v = F.verdict(ok, cad_ok, tim_same, {})
    ck("same-day close → BLOCKED", v["verdict"] == "BLOCKED")
    ck("reason_class 정확",
       v["reasonClass"] == "R32_HISTORICAL_CONTRACT_NOT_PROSPECTIVELY_EXECUTABLE")
    ck("blocker id SAME_DAY_CLOSE_LOOK_AHEAD",
       v["blockers"][0]["id"] == "SAME_DAY_CLOSE_LOOK_AHEAD")
    for need in ("unavailableInput", "actuallyAvailableAt", "historicalEntryAt",
                 "scope", "noAutoFix"):
        ck(f"blocker 필수항목 {need}", need in v["blockers"][0])

    v = F.verdict(ok, cad_no, tim_ok, {})
    ck("cadence 미복원 → BLOCKED", v["verdict"] == "BLOCKED")
    v = F.verdict(ok, cad_ok, tim_liq, {})
    ck("liquidity look-ahead → BLOCKED", v["verdict"] == "BLOCKED")
    v = F.verdict({"allMatch": False}, cad_ok, tim_ok, {})
    ck("frozen hash 불일치 → BLOCKED", v["verdict"] == "BLOCKED")

    d = load()
    if d:
        ck("실측 판정 = BLOCKED", d["verdict"] == "BLOCKED")
        ck("실측 reason_class 정확",
           d["reasonClass"] == "R32_HISTORICAL_CONTRACT_NOT_PROSPECTIVELY_EXECUTABLE")
        ck("prospectivelyExecutable False", d["prospectivelyExecutable"] is False)


# ── L5 아무것도 만들지 않았는지 ───────────────────────────────────────
def t_l5():
    print("\n[L5] BLOCKED 상태에서 미생성 확인 (§6·§9)")
    d = load()
    ck("R33 precommit 파일 미생성", not (SRC / "r33_precommit.py").exists())
    ck("R33 engine/runner 미생성", not (SRC / "r33_engine.py").exists())
    ck("activation manifest 미생성",
       not (RD / "r33-activation-manifest-latest.json").exists())
    ck("OOS ledger 루트 미생성", not (ROOT / "_cache" / "oos-r33").exists())
    if d:
        ck("생성된 OOS cohort 0", d["safety"]["oosCohortsCreated"] == 0)
        ck("historical backfill 미수행", "conflictScope" in d
           and d["conflictScope"]["note"].startswith("DIAGNOSTIC_ONLY"))
        ck("prior-close 재랭킹은 진단 전용",
           "새 계약 후보가 아니" in d["conflictScope"]["note"])
    notrun("R33 precommit hash 안정성", "§6 관문 BLOCKED — precommit 미동결")
    notrun("paired cohort / common snapshot hash", "§6 관문 BLOCKED — ledger 미생성")
    notrun("state machine 전이 검증", "§6 관문 BLOCKED — state machine 미구현")
    notrun("append-only / correction event", "§6 관문 BLOCKED — event writer 미구현")
    notrun("missed-run · pair incomplete", "§6 관문 BLOCKED — ledger 미생성")
    notrun("runtime hook exact-once", "§6 관문 BLOCKED — hook 미연결")
    notrun("first-signal observer 등록", "§6 관문 BLOCKED — activation 미수행")
    notrun("dry-run idempotency", "§6 관문 BLOCKED — runner 미구현")


# ── L6 경계 ───────────────────────────────────────────────────────────
def t_l6():
    print("\n[L6] 데이터·운영 경계")
    src = (SRC / "r33_feasibility.py").read_text(encoding="utf-8")
    for bad in ("requests.", "urllib.request", "httpx", "pykrx",
                "KRX_OPENAPI_AUTH_KEY", "AUTH_KEY", "os.environ", "getenv"):
        ck(f"금지 호출 없음: {bad}", bad not in src)
    ck("network 선언 0", '"networkCalls": 0' in src)

    tracked = git(["ls-files", "_cache"]).strip()
    ck("_cache Git 추적 0", tracked == "", tracked[:120])
    ck("_cache gitignored",
       "_cache/" in (ROOT / ".gitignore").read_text(encoding="utf-8"))
    ck("reports gitignored",
       "reports/" in (ROOT / ".gitignore").read_text(encoding="utf-8"))

    st = git(["status", "--short"]).strip().splitlines()
    dirty = [x.strip() for x in st if x.strip()]
    ck("known dirty 보존 (financial-universe-real.json 미스테이지)",
       all(not x.startswith("M  financial-universe-real.json") for x in dirty),
       str(dirty))
    staged = git(["diff", "--cached", "--name-only"]).strip().splitlines()
    ck("staged 에 row-level/raw 없음",
       not any(s.startswith("_cache") or s.startswith("reports") for s in staged),
       str(staged))

    if PUBLIC_REPO.exists():
        head = git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip()
        ck("public repo READ_ONLY (HEAD 2c8a000)", head == "2c8a000", head)
        pst = git(["status", "--short"], cwd=PUBLIC_REPO)
        ck("public repo 에 R33 산출물 없음", "r33" not in pst.lower(), pst[:120])
    else:
        notrun("public repo", "경로 없음")

    if AUTOMATION_REPO.exists():
        ast = git(["status", "--short"], cwd=AUTOMATION_REPO)
        ck("automation repo 에 R33 변경 없음", "r33" not in ast.lower(), ast[:120])
    else:
        notrun("automation repo", "경로 없음")

    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    # 실주문 검사는 **호출 형태**로 본다. safety 선언의 "broker": 0 같은
    # 문자열을 잡으면 자기 자신을 오탐한다(이전 heuristic 오탐 재발 방지).
    order_calls = re.findall(
        r"(place_order|submit_order|send_order|create_order|cancel_order"
        r"|order_cash|order_credit)\s*\(", src)
    ck("실주문 호출 0", not order_calls, str(order_calls[:3]))
    broker_imports = re.findall(
        r"^\s*(?:import|from)\s+\S*(?:kis|ebest|kiwoom|creon|ls_sec|mojito|"
        r"broker|trading_api)\S*", src, re.M | re.I)
    ck("broker SDK import 0", not broker_imports, str(broker_imports[:3]))
    ck("safety 에 realOrders/broker 0 선언", '"realOrders": 0' in src
       and '"broker": 0' in src)
    d = load()
    if d:
        s = d["safety"]
        for k in ("networkCalls", "apiCalls", "krxKeyAccess", "envAccess",
                  "productionWrite", "oosCohortsCreated", "realOrders", "broker"):
            ck(f"safety {k} == 0", s[k] == 0, str(s[k]))
        ck("realMoneyApproved False", s["realMoneyApproved"] is False)
        ck("paperOnly True", s["paperOnly"] is True)


# ── L7 산출물 ─────────────────────────────────────────────────────────
def t_l7():
    print("\n[L7] 산출물")
    d = load()
    ck("feasibility JSON parse", d is not None)
    if d:
        for need in ("taskId", "sourceTask", "sourceCommit", "verdict",
                     "reasonClass", "signalCadence", "inputTiming",
                     "sameDayCloseDependency", "conflictScope", "blockers"):
            ck(f"JSON 필드: {need}", need in d)
        ck("taskId 정확", d["taskId"] == F.TASK_ID)

    md = WD / "wababa-bm-size-separate-oos-paper-observation-contract-r33-latest.md"
    if not md.exists():
        notrun("R33 canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED)$", first) is not None,
           first)
        ck("WAIT 미사용", not re.match(r"^(전체 판정:\s*)?WAIT$", first))
        for need in ("R32_HISTORICAL_CONTRACT_NOT_PROSPECTIVELY_EXECUTABLE",
                     "MONTH_FIRST_TRADING_DAY", "BOTH_SEPARATE",
                     "REAL_MONEY_NOT_APPROVED", "0431003"):
            ck(f"보고 항목: {need}", need in body)
        # secret 스캔. 실제 키/토큰은 **구분자 없는 연속 영숫자 덩어리**다.
        # 식별자(SCREAMING_SNAKE·kebab·파일명)는 _ 나 - 로 끊기므로 애초에
        # 후보가 아니다 — 이전 heuristic 오탐(해시·TASK_ID·파일명) 재발 방지.
        toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", body)
                if not re.fullmatch(r"[0-9a-f]{64}", w)]
        ck("장문 연속 토큰(비밀값 형태) 0", not toks, str(toks[:3]))
        for bad in ("AIza", "sk-", "ghp_", "BEGIN PRIVATE KEY", "@gmail.com"):
            ck(f"secret 패턴 없음: {bad}", bad not in body)

    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("apiCalls: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
