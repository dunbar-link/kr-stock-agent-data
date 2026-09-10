#!/usr/bin/env python3
"""R3 회귀 — 필드계약 · parity · 결정 · 누락일 · 경계.

WABABA-MAGIC-FORMULA-PYKRX-WEB-DEPENDENCY-AND-OFFICIAL-SOURCE-GO-NOGO-R3

★ 이 테스트는 **네트워크를 쓰지 않고 pykrx 를 import 하지 않는다.**
  `import build_market_snapshot` 한 줄이 KRX 웹 로그인 POST 를 유발하기 때문이다.

사용: python scripts/research/test_r3_magic_official_source_replacement.py
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[2]
SCR = ROOT / "scripts"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
sys.path.insert(0, str(SCR / "research"))
import r3_magic_official_source_replacement as R      # noqa: E402

PASS = FAIL = NOTDUE = 0


def ck(n, c, e=""):
    global PASS, FAIL
    if c:
        PASS += 1
        print(f"  PASS  {n}")
    else:
        FAIL += 1
        print(f"  FAIL  {n}" + (f" - {e}" if e else ""))


def notdue(n, w):
    global NOTDUE
    NOTDUE += 1
    print(f"  NOT_YET_DUE  {n} - {w}")


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def J(name):
    p = RD / f"r3-{name}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def t1():
    print("\n[L1] 상위 상태·보존")
    ck("source commit 813df94 ancestor", subprocess.run(
        ["git", "merge-base", "--is-ancestor", "813df94", "origin/main"],
        cwd=ROOT, capture_output=True).returncode == 0)
    p = RD / "krx-web-auth-safe-status-latest.json"
    ck("R2 safe artifact 보존", p.exists())
    if p.exists():
        d = json.loads(p.read_text(encoding="utf-8"))
        ck("R2 CD007 기록 보존", d["latest"]["errorCode"] == "CD007")
        ck("R2 success=false 보존", d["latest"]["success"] is False)
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    ck("credential 변경 0 (이 TASK 코드에 쓰기 없음)",
       not re.search(r"setx|SetEnvironmentVariable|os\.environ\[",
                     (SCR / "research" / "r3_magic_official_source_replacement.py")
                     .read_text(encoding="utf-8")))


def t2():
    print("\n[L2] precommit 동결")
    h = R.contract_hash()
    r = subprocess.run([sys.executable, "-c",
                        "import sys;sys.path.insert(0,r'%s');"
                        "import r3_magic_official_source_replacement as R;"
                        "print(R.contract_hash())" % (SCR / "research")],
                       capture_output=True, text=True)
    ck("contract hash 별도 프로세스 동일", r.stdout.strip() == h)
    pre = J("magic-source-replacement-precommit")
    ck("precommit artifact 존재", pre is not None)
    if pre:
        ck("저장 hash == 재계산", pre["contractHash"] == h)
        ck("공식 API 호출 전 동결", pre["frozenBeforeAnyOfficialApiCall"] is True)
        ck("동결 시점 API 호출 0", pre["officialApiCallsSoFar"] == 0)
        ck("결과 보고 gate 고르지 않음",
           pre["priorExposure"]["gatesChosenAfterSeeingOfficialData"] is False)
    C = R.FULL_CONTRACT
    ck("pykrx web 금지", C["PYKRX_WEB_ALLOWED"] is False)
    ck("source 혼합 금지", C["MULTI_SOURCE_ROW_MIXING"] is False)
    ck("수익률로 source 선택 금지", C["SOURCE_SELECTION_BY_RETURN"] is False)
    for k in ("STRATEGY_CHANGE_ALLOWED", "UNIVERSE_CHANGE_ALLOWED",
              "FINANCIAL_INPUT_CHANGE_ALLOWED", "RANKING_CHANGE_ALLOWED",
              "SIGNAL_DATE_CHANGE_ALLOWED"):
        ck(f"{k} False", C[k] is False)
    ck("source priority 3단 고정", len(C["sourcePriority"]) == 3
       and C["sourcePriority"][2]["name"] == "NONE")
    ck("bounded readiness 4회/180초",
       C["BOUNDED_READINESS_ATTEMPTS"] == 4
       and C["BOUNDED_READINESS_WINDOW_SEC"] == 180)
    ck("무한 polling 금지", C["UNBOUNDED_POLLING"] is False)
    ck("auth 재시도 금지", C["AUTH_FAILURE_RETRY"] is False)
    ck("429 재시도 금지", C["RETRY_429"] is False)
    ck("5xx 최대 2", C["RETRY_5XX_MAX"] == 2)
    ck("call budget 고정", C["KRX_OPEN_API_CALLS_MAX"] == 30
       and C["TOTAL_NETWORK_CALLS_MAX"] == 60)
    ck("검증일 사전고정", C["PRIMARY_PARITY_DATE"] == "2026-09-04"
       and C["dateSubstitutionAllowed"] is False)
    ck("tolerance 사후확대 금지", C["toleranceWideningAfterResults"] is False)
    ck("중간결론 금지", C["intermediateConclusionsAllowed"] is False)


def t3():
    print("\n[L3] 필드 의존성")
    d = J("magic-field-dependency")
    ck("required 6개", set(R.REQUIRED_FIELDS) == {
        "symbol", "price", "marketCap", "corpName", "marketName", "industryName"})
    ck("canary 3개 (랭킹 미사용)", set(R.CANARY_FIELDS) == {"PER", "PBR", "divYield"})
    for f in R.FIELD_MATRIX:
        ck(f"필드 근거 기록: {f['normalized']}", bool(f.get("evidence")))
    ck("PER/PBR 은 required 아님",
       all(not f["required"] for f in R.FIELD_MATRIX
           if f["normalized"] in ("PER", "PBR", "divYield")))
    ck("industryName 은 제외규칙 입력",
       next(f for f in R.FIELD_MATRIX
            if f["normalized"] == "industryName")["usedInExclusion"] is True)
    ck("DART 입력은 교체대상 아님", "ebit" in R.NON_KRX_INPUTS)
    # 소스 실측 재확인 (import 없이 텍스트로)
    fund = (SCR / "build_magic_formula_fund.py").read_text(encoding="utf-8")
    ck("financeIndustries 존재", '"financeIndustries"' in fund)
    ck("financeNameKeywords 존재", '"financeNameKeywords"' in fund)
    ck("업종 제외 분기 존재", 'excluded["financial"]' in fund
       and 'excluded["utilities"]' in fund)
    bms = (SCR / "build_market_snapshot.py").read_text(encoding="utf-8")
    ck("fundamental 은 랭킹 미사용 명시",
       "공식 마법공식(EBIT/EV·EBIT/투입자본)은 이 값을 쓰지 않지만" in bms)
    ck("업종은 pykrx sector endpoint 에서 옴",
       "get_market_sector_classifications" in bms)
    if d:
        ck("업종 전용 제외 162종목 기록",
           d["industryExclusionImpact"]["excludedByIndustryOnly"] == 162)
        ck("업종 부재 시 175종목 편입 기록",
           d["industryExclusionImpact"]["wouldEnterUniverseIfIndustryMissing"] == 175)


def t4():
    print("\n[L4] row-level parity 실측")
    d = J("magic-source-parity")
    if not d:
        notdue("parity artifact", "없음")
        return
    rp = d["rowParity"]
    ck("primary date 2026-09-04", d["primaryParityDate"] == "2026-09-04")
    ck("baseline universe 2765", d["baseline"]["universeCount"] == 2765)
    ck("공식 ticker 2765 동일", d["official"]["tickers"] == 2765)
    for k in ("signalEligibleTickerCoveragePct", "closeExactMatchPct",
              "marketCapExactMatchPct", "corpNameExactPct",
              "marketClassificationExactPct"):
        ck(f"{k} = 100%", rp[k] == 100.0, str(rp[k]))
    ck("★ industryName resolvable 0%", rp["industryNameResolvablePct"] == 0.0)
    for k in ("duplicateKeyCount", "invalidNumericCount", "missingTickerCount",
              "currentListingFilterUsed", "survivorshipFilterUsed",
              "priceTimesVolumeProxyUsed", "closeTimesSharesAsMarketCapUsed"):
        ck(f"{k} = 0", rp[k] == 0)
    ck("SECT_TP_NM 빈값 근거 기록", "빈값" in d["sectorFieldFinding"])
    ck("data.go.kr 도 업종 없음 기록", "업종 필드 없음" in d["dataGoKrFinding"])
    ck("package parity 미실행을 PASS 로 안 봄",
       d["packageParity"]["executed"] is False
       and d["packageParity"]["notTreatedAsPass"] is True)
    sd = d["sameDayAvailability"]
    ck("당일 0행 실측", sd["kospiRows"] == 0 and sd["kosdaqRows"] == 0)
    ck("대조일 정상 943행", sd["controlDates"]["2026-09-09"] == 943)
    ck("STRUCTURAL_SAME_DAY_AVAILABILITY FAIL",
       sd["STRUCTURAL_SAME_DAY_AVAILABILITY"] == "FAIL")


def t5():
    print("\n[L5] 결정 엔진")
    d = J("magic-source-decision")
    if not d:
        notdue("decision", "없음")
        return
    ck("결정 enum 2개", d["decisionEnum"] == R.DECISION_ENUM)
    ck("실측 결정 NO_GO",
       d["decision"]["decision"] == "OFFICIAL_SOURCE_REPLACEMENT_NO_GO")
    for g in ("G2_official_source_provides_all_required_fields",
              "G4_security_universe_parity",
              "G6_same_day_structural_availability"):
        ck(f"실패 gate 기록: {g[:34]}", g in d["decision"]["failedGates"])
    ck("passed+failed == 10",
       len(d["decision"]["failedGates"]) + len(d["decision"]["passedGates"]) == 10)
    ck("tolerance 사후확대 0", d["toleranceWidenedAfterResults"] is False)
    nc = d["networkCalls"]
    ck("KRX 호출 예산 내", nc["krxOpenApi"] <= nc["budget"]["krx"], str(nc))
    ck("총 호출 예산 내", nc["total"] <= nc["budget"]["total"])
    ck("우발 pykrx 웹 로그인 정직 기록",
       d["inadvertentPykrxWebLoginPosts"] >= 0)
    # 결과독립 fixture
    allp = {g: True for g in R.MANDATORY_GATES}
    ck("전 gate PASS → GO",
       R.decide(allp)["decision"] == "OFFICIAL_SOURCE_REPLACEMENT_GO")
    for g in ("G2_official_source_provides_all_required_fields",
              "G6_same_day_structural_availability",
              "G8_no_strategy_universe_ranking_change"):
        one = dict(allp)
        one[g] = False
        ck(f"{g[:30]} 하나만 실패 → NO_GO",
           R.decide(one)["decision"] == "OFFICIAL_SOURCE_REPLACEMENT_NO_GO")
    miss = {g: True for g in R.MANDATORY_GATES[:-1]}
    r = R.decide(miss)
    ck("미측정 gate 를 PASS 로 승격 안 함",
       r["decision"] == "OFFICIAL_SOURCE_REPLACEMENT_NO_GO"
       and any(c.startswith("GATE_NOT_MEASURED::") for c in r["reasonCodes"]))
    for bad in ("CONDITIONAL_GO", "PARTIAL_GO", "MORE_RESEARCH"):
        ck(f"중간 결론 미존재: {bad}", bad not in R.DECISION_ENUM)


def t6():
    print("\n[L6] 누락일 판정")
    d = J("magic-missed-days-adjudication")
    if not d:
        notdue("missed-day", "없음")
        return
    ck("4일 열거", len(d["missedDays"]) == 4)
    ck("날짜 정확", [m["date"] for m in d["missedDays"]] == R.MISSED_DAYS)
    for m in d["missedDays"]:
        ck(f"{m['date']} PIT 증거 없음",
           m["immutableSnapshotBeforeDeadline"] is False
           and m["signalInputHashes"] is False)
        ck(f"{m['date']} MISSED_RUN 판정",
           m["classification"] == "MISSED_RUN_NO_VALID_PIT_EVIDENCE")
        ck(f"{m['date']} enum 안", m["classification"] in R.MISSED_DAY_ENUM)
    ck("거래 생성 0", d["tradesCreated"] == 0 and d["lotsAppended"] == 0)
    ck("canonical append 0", d["canonicalAppend"] == 0)
    ck("현재데이터 과거복원 거부", d["currentDataHistoricalReplay"] == "REJECTED")
    ck("canonical 구조 미변경", d["canonicalLedgerStructureChanged"] is False)


def t7():
    print("\n[L7] 경계 — NO_GO 이므로 runtime 미변경")
    ck("공식 adapter 미생성",
       not (SCR / "official_market_source_adapter.py").exists()
       and not (SCR / "build_market_snapshot_official.py").exists())
    bms = (SCR / "build_market_snapshot.py").read_text(encoding="utf-8")
    ck("build_market_snapshot 에 공식 source 주입 없음",
       "r31_source" not in bms and "official_market_source" not in bms)
    ck("build_market_snapshot 원래 pykrx 경로 유지",
       "from pykrx import stock" in bms)
    ck("R2 관찰기 hook 보존", "_krx_obs.install()" in bms)
    bmsf = (SCR / "build_market_snapshot_fast.py").read_text(encoding="utf-8")
    ck("fast 미변경(공식 source 미주입)", "r31_source" not in bmsf)
    src = (SCR / "research" / "r3_magic_official_source_replacement.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    imps = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            imps += [a.name.split(".")[0] for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module:
            imps.append(n.module.split(".")[0])
    ck("R3 모듈이 pykrx 미import", "pykrx" not in imps)
    ck("R3 모듈이 build_market_snapshot 미import",
       "build_market_snapshot" not in imps)
    tsrc = Path(__file__).read_text(encoding="utf-8")
    timps = []
    for n in ast.walk(ast.parse(tsrc)):
        if isinstance(n, ast.Import):
            timps += [a.name.split(".")[0] for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module:
            timps.append(n.module.split(".")[0])
    ck("테스트가 pykrx 미import", "pykrx" not in timps)
    ck("테스트가 build_market_snapshot 미import",
       "build_market_snapshot" not in timps)
    # credential **이름**(KRX_OPENAPI_AUTH_KEY 등)은 계약 문서에 필요하다.
    # 금지 대상은 **값**이므로, 값을 읽거나 심는 형태만 잡는다.
    for bad in ("KRX_ID", "KRX_PW", "crtfc_key"):
        ck(f"R3 모듈에 {bad} 없음", bad not in src)
    ck("credential 값 읽기 코드 없음",
       not re.search(r"(os\.environ|getenv)\s*[\(\[]", src))
    ck("HTTP 호출 코드 없음 (계약·결정 전용 모듈)",
       not re.search(r"requests\.|urlopen\(|http[s]?://", src))
    ck("key 형태 토큰 0",
       not [w for w in re.findall(r"[A-Za-z0-9]{40,}", src)
            if not re.fullmatch(r"[0-9a-f]{64}", w) and re.search(r"[0-9]", w)])
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 reports/_cache 없음",
       not any(x.startswith(("reports", "_cache")) for x in staged), str(staged))
    pub = Path(r"C:\work\kr-stock-agent")
    if pub.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=pub).strip() == "2c8a000")
    nat = J("magic-natural-run")
    if nat:
        ck("scheduler 미변경", nat["schedulerUnchanged"] is True)
        ck("신규 scheduler/trigger 0",
           nat["newScheduler"] == 0 and nat["newTrigger"] == 0)
        ck("강제 실행 0", nat["forcedRun"] == 0)
        ck("자연 실행 관찰 대상 없음(NO_GO)",
           nat["naturalRunProof"] == "NOT_APPLICABLE")
    for a in ("magic-source-replacement-precommit", "magic-field-dependency",
              "magic-source-parity", "magic-missed-days-adjudication",
              "magic-natural-run", "magic-source-decision"):
        ck(f"JSON parse: r3-{a[:30]}", J(a) is not None)
    blob = " ".join(json.dumps(J(a), ensure_ascii=False)
                    for a in ("magic-source-parity", "magic-field-dependency",
                              "magic-missed-days-adjudication") if J(a))
    ck("artifact 에 6자리 종목코드 나열 없음", not re.findall(r'"\d{6}"', blob))
    md = WD / ("wababa-magic-formula-pykrx-web-dependency-and-official-source-"
               "go-nogo-r3-latest.md")
    if not md.exists():
        notdue("R3 canonical MD", "보고서 작성 단계")
    else:
        b = md.read_text(encoding="utf-8")
        ck("첫 줄 판정", re.match(r"^(PASS|WARNING|WAIT|BLOCKED)$",
                                 b.splitlines()[0].strip()) is not None)
        for need in ("OFFICIAL_SOURCE_REPLACEMENT_NO_GO", "MISSED_RUN_NO_VALID_PIT_EVIDENCE",
                     "REAL_MONEY_NOT_APPROVED", "813df94"):
            ck(f"보고 항목: {need}", need in b)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t1, t2, t3, t4, t5, t6, t7):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_YET_DUE {NOTDUE}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
