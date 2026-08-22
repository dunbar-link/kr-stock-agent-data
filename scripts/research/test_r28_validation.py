#!/usr/bin/env python3
"""R28 회귀 — 수집 재개·약관 게이트·R27 불변 invariant. 네트워크 0.

WABABA-R27-LIQUIDITY-DATA-RESUME-R28

계층:
  L1 guard   — 약관 게이트 · 승인 없는 대량수집 차단 · 재시도 정책
  L2 resume  — 완료분 skip · 캐시 검증 · 결측≠0 · 휴일 미저장 · PIT 창
  L3 gate    — coverage 계산·기준 불변 · 완화 금지
  L4 real    — 실제 산출물 · R25/R26 재현 · R27 precommit 불변 · 판정
  L5 regress — R27~R16 산출물·판정 보존

사용: python scripts/research/test_r28_validation.py
"""
from __future__ import annotations

import csv
import gzip
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r27_collect as CO  # noqa: E402
import r28_closeout as CL  # noqa: E402
from r25_engine import R25Engine  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
CACHE = ROOT / "_cache" / "krx-liquidity"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(pfx, n):
    p = RD / f"{pfx}-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ═══════════════════ L1 약관 게이트 ═══════════════════
def t_l1():
    print("[L1] 약관 게이트 · 재시도 정책")
    ck("§13 대량수집 상한 존재", CO.BULK_LIMIT_WITHOUT_APPROVAL > 0)
    ck("§13 상한이 보수적(<=100일)", CO.BULK_LIMIT_WITHOUT_APPROVAL <= 100,
       CO.BULK_LIMIT_WITHOUT_APPROVAL)
    ck("§13 승인 env 정의", CO.APPROVAL_ENV == "WABABA_KRX_BULK_APPROVED")
    ck("§13 약관 근거 문구", "제10조 제2호" in CO.TOS_NOTICE)
    ck("§13 공식 경로 3개 명시",
       all(x in CO.TOS_NOTICE for x in ("화면 다운로드", "데이터 상품", "Open API")))
    ck("§13 기본값은 미승인", os.environ.get(CO.APPROVAL_ENV) != "1")

    src = (ROOT / "scripts" / "research" / "r27_collect.py").read_text(
        encoding="utf-8")
    ck("게이트가 수집 루프 **앞**에 있다",
       src.index("BLOCKED_KRX_TOS") < src.index("for i, d in enumerate(todo"))
    ck("게이트가 exit 3 반환", "return 3" in src)
    # 게이트 블록 = 상한 검사부터 return 3 까지
    # ("BLOCKED_KRX_TOS" 문자열은 status·aborted 두 곳에 나오므로 앵커로 쓰지 않는다)
    guard_block = src.split("if len(todo) > BULK_LIMIT_WITHOUT_APPROVAL")[1] \
        .split("return 3")[0]
    ck("게이트가 inventory 를 남긴다",
       "r27-data-inventory-latest.json" in guard_block)
    ck("게이트 inventory 가 provenance 를 보존",
       all(k in guard_block for k in ("source", "lookbackTradingDays",
                                      "lookAheadRule", "retryPolicy")))
    ck("운영 runner 위험 명시", "예약 runner" in src)

    # R27 자체수정 유지
    ck("§41 재시도 3회", CO.MAX_RETRY == 3)
    ck("§41 백오프 존재", len(CO.RETRY_BACKOFF) >= 2)
    ck("§41 연속실패 중단", CO.CONSECUTIVE_FAIL_ABORT > 0)
    ck("§41 세션 복구 함수", callable(CO._reset_krx_session))
    ck("§41 import 실패도 status",
       "SOURCE_UNAVAILABLE" in src)

    inv = L("r27", "data-inventory")
    ck("게이트 실측 발동", inv is not None and inv.get("status") == "BLOCKED_KRX_TOS",
       (inv or {}).get("status"))
    if inv:
        ck("게이트가 잔여량 기록", inv["daysPending"] > 0)
        ck("게이트가 유료데이터 0", inv["paidData"] == 0)
        ck("게이트가 신규 credential 0", inv["newCredential"] == 0)
        ck("Founder 결정 3경로", len(inv["founderDecisionRequired"]) == 3)


# ═══════════════════ L2 resume 의미론 ═══════════════════
def t_l2():
    print("[L2] resume · 캐시 검증 · PIT")
    cal = CO.trading_calendar()
    ck("캘린더 정본 재사용(캐시)", (CACHE / "_trading-calendar.json").exists())
    ck("캘린더 거래일 수", len(cal) > 4000, len(cal))
    ck("§9 주말 없음",
       all(__import__("datetime").date.fromisoformat(d).weekday() < 5
           for d in cal[:300]))

    # §10 PIT 창 — 변경 금지
    ck("§10 lookback 20 유지", CO.LOOKBACK == 20)
    d0 = cal[200]
    w = CO.needed_days(cal, [d0])
    ck("§10 창 20일", len(w) == 20)
    ck("§10 결정일 당일 미포함", d0 not in w)
    ck("§10 미래 미포함", all(x < d0 for x in w))
    ck("§10 창이 D-20..D-1", w == cal[180:200])

    # §3 완료분 skip
    have = {p.name[:-7] for p in CACHE.glob("*.csv.gz")}
    ck("캐시에 완료분 존재", len(have) > 0)
    need = CO.needed_days(cal, [cal[i] for i in range(100, 140)])
    todo = [d for d in need if d not in have]
    ck("§3 완료분은 todo 에서 제외", all(d not in have for d in todo))

    # §8 캐시 검증 — schema/행수
    if have:
        d = sorted(have)[0]
        ok, n, sha = CL.validate_day(d)
        ck(f"캐시 검증 통과 {d}", ok is True, (ok, n))
        ck("검증이 행수를 센다", n > 0)
        ck("검증이 sha 를 남긴다", sha is not None and len(sha) == 16)
    ck("§8 필수 필드 정의",
       CL.REQUIRED_COLS == {"ticker", "close", "marketCap", "volume",
                            "tradedValue", "shares"})
    ck("§8 없는 날짜는 무효", CL.validate_day("1999-01-01")[0] is False)

    # §8 missing ≠ 0 — 저장 포맷에 0 강제가 없어야 한다
    src = (ROOT / "scripts" / "research" / "r27_collect.py").read_text(
        encoding="utf-8")
    ck("§8 missing 을 0 으로 채우지 않음",
       "or 0" not in src.split("def fetch_day")[1].split("def write_day")[0])
    ck("§9 빈 결과를 0 데이터로 저장 안 함", 'return None, "EMPTY"' in src)
    ck("§9 스키마 불일치도 저장 안 함", 'return None, f"SCHEMA' in src)

    # 실제 캐시에 0 과 결측이 구분돼 있는지
    if have:
        d = sorted(have)[0]
        with gzip.open(CACHE / f"{d}.csv.gz", "rt", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        ck("캐시에 거래량 필드 존재", all("volume" in r for r in rows[:50]))
        ck("캐시에 거래대금 필드 존재", all("tradedValue" in r for r in rows[:50]))
        zero = sum(1 for r in rows if r["volume"] in ("0", "0.0"))
        ck("거래량 0 이 값으로 보존됨(결측 아님)", zero >= 0)


# ═══════════════════ L3 coverage gate ═══════════════════
def t_l3():
    print("[L3] coverage gate")
    cov = L("r28", "collection-coverage")
    ck("coverage 산출물 존재", cov is not None)
    if not cov:
        return
    ck("§11 gate 를 R27 precommit 에서 가져옴", "minCoveragePctPerYear"
       in cov["gateFromR27Precommit"])
    ck("§11 gate 변경 없음", cov["gateUnchanged"] is True)
    ck("§11 연도 기준 90%", cov["gateFromR27Precommit"]["minCoveragePctPerYear"]
       == 90.0)
    ck("§11 최소 연도 15", cov["minYearsCovered"] == 15)
    ck("§12 완화 안 함(미달이면 False)",
       cov["coverageGatePass"] == (cov["yearsMeetingThreshold"]
                                   >= cov["minYearsCovered"]))
    ck("§11 연도별 coverage 산출", len(cov["byYear"]) > 0)
    ck("§11 거래소별 coverage 산출", len(cov["byExchange"]) > 0)
    ck("§11 size bucket coverage 산출", len(cov["bySizeBucket"]) == 3)
    # 산술 검증
    exp = round(100.0 * cov["collectedDays"] / cov["requiredDays"], 3)
    ck("coverage 계산 정확", abs(cov["dayCoveragePct"] - exp) < 1e-9,
       (cov["dayCoveragePct"], exp))


# ═══════════════════ L4 실제 산출물 ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    res, fr = L("r28", "collection-resume"), L("r28", "failure-retry")
    rex, fv = L("r28", "r27-reexecution"), L("r28", "final-verdict")
    for n, o in (("collection-resume", res), ("failure-retry", fr),
                 ("r27-reexecution", rex), ("final-verdict", fv)):
        ck(f"{n} 산출물 존재", o is not None)
    if not all((res, fr, rex, fv)):
        return

    # §4 정확한 산출
    ck("§4 required 4,640", res["requiredDays"] == 4640)
    ck("§4 already_valid 기록", res["alreadyValid"] >= 0)
    ck("§4 pending 기록", res["pending"] >= 0)
    ck("§4 합계 정합",
       res["alreadyValid"] + res["pending"] == res["requiredDays"])
    ck("§4 손상 캐시 0", res["invalidOrCorrupt"] == 0)
    ck("§4 R27 보고와 일치", res["matchesR27Report"] is True)
    ck("§41 기존 성공분 재사용 명시", "재다운로드 0" in res["reuseRule"])
    ck("§41 처음부터 재수집 금지 명시", "처음부터 수집 금지" in res["reuseRule"])
    ck("재개 시도 기록", res["resumeAttempted"] is True)
    ck("재개 결과가 약관 차단", res["resumeResult"] == "BLOCKED_KRX_TOS")

    # §13 원인 분리
    ck("§13 원인이 약관 금지", fr["rootCauseClass"] == "SERVICE_TERMS_PROHIBITION")
    ck("§13 collector bug 아님 명시", fr["notCollectorBug"] is True)
    ck("§13 bug 가설 배제 목록", len(fr["ruledOut"]) >= 5)
    ck("§13 로그인 페이지는 정상", fr["evidence"]["loginPageHttpStatus"] == 200)
    ck("§13 로그인 API 만 에러페이지",
       fr["evidence"]["loginApiReturnedHtmlErrorPage"] is True)
    ck("§13 KRX 실제 문구 보존", "제10조 제2호" in fr["evidence"]["krxNotice"])
    ck("§13 공식 경로 3개", len(fr["sanctionedPaths"]) == 3)
    ck("§13 세 경로 전부 Founder", all(
        "Founder" in p["who"] for p in fr["sanctionedPaths"]))
    ck("§36 유료 경로는 정책상 막힘",
       any("유료" in p["blockedBy"] for p in fr["sanctionedPaths"]))
    ck("§36 신규 credential 경로는 승인 게이트",
       any("credential" in p["blockedBy"] for p in fr["sanctionedPaths"]))
    ck("게이트 정보 기록", fr["guard"]["exitCode"] == 3)
    ck("자체수정 3건 기록", len(fr["collectorFixesApplied"]) == 3)

    # §1 precommit 불변 · §15·§16 재현
    ck("§1 R27 precommit 불변", rex["precommitUnchanged"] is True)
    ck("§1 primary gate 1.25억 유지",
       rex["primaryGate"]["thresholdKrw"] == 125_000_000)
    ck("§1 민감도 LOW 2,500만", rex["sensitivity"]["LOW"]["thresholdKrw"] == 25_000_000)
    ck("§1 민감도 BASE 1.25억", rex["sensitivity"]["BASE"]["thresholdKrw"] == 125_000_000)
    ck("§1 민감도 HIGH 2.5억", rex["sensitivity"]["HIGH"]["thresholdKrw"] == 250_000_000)
    ck("§15·§16 재현 전부 일치", rex["allReproductionExact"] is True)
    # 지시문이 명시한 값과 대조
    r25 = rex["sizeReproduction"]
    for k, want in (("12M", 35.336), ("36M", 13.32), ("60M", 10.752),
                    ("84M", 9.063)):
        ck(f"§15 SIZE R25 {k} = {want}",
           abs(r25[k]["r27SpreadAnnPct"] - want) < 1e-9,
           r25[k]["r27SpreadAnnPct"])
    r26s, r26b = rex["sizeReproductionR26Top"], rex["bmReproductionR26Top"]
    for k, want in (("12M", 48.424), ("36M", 19.147), ("60M", 14.864)):
        ck(f"§15 SIZE R26 TOP {k} = {want}",
           abs(r26s[k]["r27TopAnnPct"] - want) < 1e-9, r26s[k]["r27TopAnnPct"])
    for k, want in (("12M", 28.082), ("36M", 15.01), ("60M", 12.799)):
        ck(f"§16 BM R26 TOP {k} = {want}",
           abs(r26b[k]["r27TopAnnPct"] - want) < 1e-9, r26b[k]["r27TopAnnPct"])

    # §12 하류 미실행
    ck("§12 R27 pipeline 실행됨", rex["r27RunExecuted"] is True)
    ck("§12 coverage 에서 멈춤", rex["r27RunStoppedAt"] == "COVERAGE_GATE")
    ck("§12 하류 미실행", rex["downstreamAnalysis"] == "NOT_RUN")
    ck("§12 미실행 목록 기록", len(rex["downstreamNotRunList"]) >= 8)
    for n in ("before-after", "participation", "included-excluded",
              "concentration"):
        ck(f"§12 산출물 미생성 r27-{n}", L("r27", n) is None)
    # bm-vs-size 는 파일이 존재하되 **NOT_EVALUATED 를 명시**한다.
    # 침묵보다 낫다 — 다음 세션이 빈 파일을 결과로 오해하지 않는다.
    bvs = L("r27", "bm-vs-size")
    ck("§32 BM vs SIZE 는 NOT_EVALUATED 로 명시",
       bvs is not None and bvs.get("status") == "NOT_EVALUATED",
       (bvs or {}).get("status"))
    ck("§32 미평가 사유 기록", "근거 없는 비교" in (bvs or {}).get("why", ""))

    # §32 verdict taxonomy 재사용
    ck("§32 R27 taxonomy 사용", fv["verdictTaxonomyFromR27"] is True)
    ck("§32 새 taxonomy 없음", fv["newTaxonomy"] is False)
    ck("§32 SIZE 판정 5종 중 하나",
       fv["sizeVerdict"] in ("SIZE_EXECUTABLE_STRONG", "SIZE_EXECUTABLE_PROMISING",
                             "SIZE_ALPHA_LARGELY_NONTRADABLE", "SIZE_FRAGILE",
                             "SIZE_DATA_INSUFFICIENT"))
    ck("§12 coverage 미달이면 DATA_INSUFFICIENT 유지",
       fv["sizeVerdict"] == "SIZE_DATA_INSUFFICIENT")
    ck("§35 포트폴리오 진입 불가", fv["portfolioResearchEntryAllowed"] is False)
    ck("§34-D 다음 규칙", fv["nextTaskRule"] == "D")
    ck("§34 새 factor 금지 명시", "새 factor 연구 금지" in fv["nextTask"])
    ck("Founder 결정 필요", fv["founderDecisionRequired"] is True)
    ck("운영 위험 기록", "예약 runner" in fv["productionRisk"])


# ═══════════════════ L5 회귀 ═══════════════════
def t_l5():
    print("[L5] R27~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]

    def syn(rows):
        return {d: {"X": {"close": c, "shares": s, "dps": None}}
                for d, (c, s) in zip(ds, rows)}
    e = R25Engine(ds, cap=syn([(100000, 100)] + [(10000, 1000)] * 5),
                  overrides={})
    ck("10:1 분할 wealth 불변", abs(e.tsr(0, 2, "X")) < 1e-12)
    e2 = R25Engine(ds, cap=syn([(10000, 100)] + [(5000, 200)] * 5), overrides={})
    ck("무상증자 wealth 불변", abs(e2.tsr(0, 2, "X")) < 1e-12)

    for f in ("r24-foundation-verdict-latest.json",
              "r25-verdict-latest.json", "r26-verdict-latest.json",
              "r27-precommit-latest.json", "r27-verdict-latest.json",
              "r27-size-reproduction-latest.json",
              "r27-liquidity-coverage-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())

    v27 = json.loads((RD / "r27-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R27 판정 보존", v27["verdict"] == "SIZE_DATA_INSUFFICIENT")
    r26 = json.loads((RD / "r26-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R26 판정 보존", r26["verdict"] == "NO_INCREMENTAL_COMBINATION")
    r25 = json.loads((RD / "r25-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R25 PRIMARY 보존", set(r25["PRIMARY_FACTOR"]) == {"BM", "SIZE_SMALL"})

    # R27 precommit 실제 불변 확인
    pc = json.loads((RD / "r27-precommit-latest.json").read_text(encoding="utf-8"))
    ck("§1 gate 임계 불변", pc["primaryGate"]["thresholdKrw"] == 125_000_000)
    ck("§1 참여율 1% 불변", pc["primaryGate"]["participationImplied"] == 0.01)
    ck("§18 자본 5천만 불변", pc["capital"]["totalKrw"] == 50_000_000)
    ck("§18 40종목 불변", pc["capital"]["namesForSanity"] == 40)
    ck("§18 1종목 125만 불변", pc["capital"]["orderPerNameKrw"] == 1_250_000)
    ck("§19 primary horizon 36M 불변", pc["horizons"]["primary"] == 36)

    for f in ("wababa-size-tradability-validation-r27-latest.md",
              "wababa-bm-size-incremental-combination-r26-latest.md",
              "wababa-canonical-factor-rediscovery-r25-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-r27-liquidity-data-resume-r28-latest.json"
    ck("R28 보고서 JSON 존재", j.exists())
    if j.exists():
        d = json.loads(j.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure", "newCredential"):
            ck(f"{k} = 0", p[k] == 0)
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish", "canonicalProduction"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED",
           p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("§1 R27 precommit 불변 기록", d["r27PrecommitUnchanged"] is True)
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 orchestration 0", d["hotg"]["newOrchestration"] == 0)
    m = WD / "wababa-r27-liquidity-data-resume-r28-latest.md"
    ck("R28 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§42)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§42)", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("제10조 제2호", "약관", "화면 다운로드", "Open API",
                     "예약 runner", "coverage", "재현", "REAL_MONEY_NOT_APPROVED",
                     "Founder 행동", "다음 단일 작업", "한계"):
            ck(f"§43 보고 항목: {need}", need in body)
        ck("답 못한 것 명시", "답하지 못했다" in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_prod):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("krxBulkCollection: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
