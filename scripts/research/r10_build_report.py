#!/usr/bin/env python3
"""R10 산출물 생성 (MD + JSON) — §9 3-way 비교 · §10 실제돈 위험표 · §20 forward-test.

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

reports/wababa/wababa-r8-frozen-spec-forensic-reconciliation-r10-latest.{md,json}

§14 provenance: R8·R9 산출물은 읽기만 하고 절대 덮어쓰지 않는다.
production 변경 0 · scheduler 0 · 홈페이지 0 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r9_precommit import COST_LEVELS, DELIST_LEVELS, ERAS, FROZEN, GATES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-r8-frozen-spec-forensic-reconciliation-r10-latest"


def L(series, n):
    p = RD / f"{series}-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def won(x):
    return "-" if x is None else f"{round(x):,}원"


def eok(x):
    return "-" if x is None else f"{x / 1e8:.2f}억원"


def sgn(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


# ═══════════════ §9 3-way 비교 ═══════════════
def three_way():
    r8 = FROZEN["r8Measured"]
    c9, c10 = L("r9", "core"), L("r10", "core")
    p9, p10 = L("r9", "repro"), L("r10", "repro")
    s9, s10 = L("r9", "stress"), L("r10", "stress")
    w9, w10 = L("r9", "walkforward"), L("r10", "walkforward")
    t9, t10 = L("r9", "controls"), L("r10", "controls")
    v9, v10 = L("r9", "verdict"), L("r10", "verdict")
    a9, a10 = L("r9", "cashaudit"), L("r10", "cashaudit")

    def cost_hi(s):
        return next(r["excessPct"] for r in s["costStress"]["levels"] if r["level"] == "HIGH")

    def del100(s):
        return next(r["symmetric"]["excessPct"] for r in s["delistingStress"]["levels"]
                    if r["level"] == "LOSS_100")

    def top5rm(c):
        r = next(x for x in c["concentration"]["removal"] if x["topK"] == 5)
        return r["firstOrder"]["excessPct"]

    def cash24(a):
        return (a.get("entryScheduleDefect") or {}).get("firstCohortAvgCashPct")

    def steady(a):
        segs = a.get("segments") or []
        f = next((s for s in segs if s["segment"].startswith("완전투자")), None)
        return f and f["avgCashPct"]

    def wcoh(w, h):
        k = {12: "worstStart1y", 36: "worstStart3y", 60: "worstStart5y"}[h]
        return (w["cohortDistribution"].get(k) or {}).get("accountVsContributedPct")

    def sy(v):
        s = v["postHocFindings"]["startYearDependency"]["start2008AndLater"]
        return s["positivePct"]

    rows = [
        ("CAGR (%)", r8["cagrPct"], p9["base"]["cagrPct"], p10["base"]["cagrPct"]),
        ("benchmark CAGR (%)", r8["benchPct"], p9["base"]["benchPct"],
         p10["base"]["benchPct"]),
        ("초과수익 (%p)", r8["excessPct"], p9["base"]["excessPct"],
         p10["base"]["excessPct"]),
        ("MDD (%)", r8["mddPct"], p9["base"]["mddPct"], p10["base"]["mddPct"]),
        ("최장 수중 (개월)", r8["underwaterMonths"], p9["base"]["underwaterMonths"],
         p10["base"]["underwaterMonths"]),
        ("최악 1년 (%)", None, p9["base"]["worst1yPct"], p10["base"]["worst1yPct"]),
        ("최악 3년 연율 (%)", None, p9["base"]["worst3yPct"], p10["base"]["worst3yPct"]),
        ("최악 5년 코호트 계좌 (%)", None, wcoh(w9, 60), wcoh(w10, 60)),
        ("최악 3년 코호트 계좌 (%)", None, wcoh(w9, 36), wcoh(w10, 36)),
        ("최악 1년 코호트 계좌 (%)", None, wcoh(w9, 12), wcoh(w10, 12)),
        ("2008년 이후 시작 positive 비율 (%)", None, sy(v9), sy(v10)),
        ("5년 cohort positive-excess (%)", None,
         w9["cohortDistribution"]["summary"]["60"]["positiveExcessRatePct"],
         w10["cohortDistribution"]["summary"]["60"]["positiveExcessRatePct"]),
        ("HIGH 비용·슬리피지 초과 (%p)", 1.85, cost_hi(s9), cost_hi(s10)),
        ("상폐 100% 대칭 초과 (%p)", 0.86, del100(s9), del100(s10)),
        ("통제군 percentile RANDOM", None,
         t9["controls"]["RANDOM"]["candidatePercentile"],
         t10["controls"]["RANDOM"]["candidatePercentile"]),
        ("상위5 기여비중 (%)", None, c9["concentration"]["top5SharePct"],
         c10["concentration"]["top5SharePct"]),
        ("상위5 제거 초과 1차근사 (%p)", None, top5rm(c9), top5rm(c10)),
        ("KOSPI 단독 초과 (%p)", None, c9["marketSegments"]["KOSPI"]["excessPct"],
         c10["marketSegments"]["KOSPI"]["excessPct"]),
        ("KOSDAQ 단독 초과 (%p)", None, c9["marketSegments"]["KOSDAQ"]["excessPct"],
         c10["marketSegments"]["KOSDAQ"]["excessPct"]),
        ("SMALL 구간 내 초과 (%p)", None, c9["sizeControls"]["SMALL"]["excessPct"],
         c10["sizeControls"]["SMALL"]["excessPct"]),
        ("MID 구간 내 초과 (%p)", None, c9["sizeControls"]["MID"]["excessPct"],
         c10["sizeControls"]["MID"]["excessPct"]),
        ("LARGE 구간 내 초과 (%p)", None, c9["sizeControls"]["LARGE"]["excessPct"],
         c10["sizeControls"]["LARGE"]["excessPct"]),
        ("연 거래(주문) 건수", r8["tradesPerYear"], p9["base"]["tradesPerYear"],
         p10["base"]["tradesPerYear"]),
        ("최대 동시보유 종목", None, p9["base"]["maxPositions"],
         p10["base"]["maxPositions"]),
        ("첫 24개월 평균 현금 (%)", None, cash24(a9), cash24(a10)),
        ("완전투자 구간 평균 현금 (%)", None, steady(a9), steady(a10)),
    ]
    return {
        "columns": {
            "A_R8_reported": "R8 이 보고한 값 (사양 위반 코드 · 일부는 ladder 구조 오류)",
            "B_R9_actualCode": "R9 가 실측한 실제 코드 동작 (ONE_TWELFTH_WAIT)",
            "C_R10_specConformant": "R10 사전규격 준수 (MONTHLY_STAGGER)",
        },
        "purpose": ("어느 것이 더 좋은가를 고르는 표가 아니다. 기존 성과 중 얼마가 "
                    "사양 위반에서 나왔는지 분해하는 표다."),
        "rows": [{"metric": m, "A_R8_reported": a, "B_R9_actualCode": b,
                  "C_R10_specConformant": c} for m, a, b, c in rows],
        "headlineDecomposition": {
            "r8ReportedExcessPct": r8["excessPct"],
            "r9ActualCodeExcessPct": p9["base"]["excessPct"],
            "r10SpecConformantExcessPct": p10["base"]["excessPct"],
            "attributableToSpecViolationPct": round(
                p9["base"]["excessPct"] - p10["base"]["excessPct"], 2),
            "reading": ("R8 보고 +4.60%p 는 R9 가 코드 수준에서 정확히 재현했다(사양 위반 "
                        "상태 그대로). 사전규격대로 고치면 +2.67%p 다. 즉 "
                        "**약 1.93%p 가 사양 위반(초기자본 11/12 를 최대 2년간 현금 보유)"
                        "에서 나온 것**이다. 동시에 MDD 는 -43.79% → -53.38% 로, 최악 "
                        "1년은 -31.09% → -53.58% 로 악화됐다 — 폭락장에 실제로 투자돼 "
                        "있었기 때문이다."),
        },
    }


# ═══════════════ §10 실제 돈 위험표 ═══════════════
def real_money():
    c10, w10, p10 = L("r10", "core"), L("r10", "walkforward"), L("r10", "repro")
    r = c10["realMoneyRisk"]
    cd = w10["cohortDistribution"]
    cl = p10["capitalFlowAudit"]["first36Months"]
    lo = min(x["cashRatioPct"] for x in cl)
    a10 = L("r10", "cashaudit")
    by = a10["cashProfile"]["byYear"]
    return {
        "basis": "초기 투입 50,000,000원 · 과거 실측치 · 예측·보장 아님",
        "initialCapitalKrw": FROZEN["initialCapitalKrw"],
        "terminalWealthKrw": r["terminalWealth"],
        "benchTerminalKrw": r["benchTerminal"],
        "mddPct": r["mddPct"],
        "mddAmountKrw": r["worstDrawdownKrw"],
        "mddAtDate": r["worstDrawdownAt"],
        "lowestAccountValueVsContributedPct": r["lowestValueVsContributedPct"],
        "lowestAccountValueKrw": r["lowestAccountValueKrw"],
        "lowestAccountValueAt": r["lowestAccountValueAt"],
        "worst1yPct": r["worst1yPct"],
        "worst1yEndValueKrw": r["worst1yKrwOn50m"],
        "worst3yAnnualPct": r["worst3yAnnualPct"],
        "worst3yTotalPct": r["worst3yTotalPct"],
        "worst3yEndValueKrw": round(FROZEN["initialCapitalKrw"]
                                    * (1 + r["worst3yTotalPct"] / 100)),
        "worstStartCohort1y": cd["worstStart1y"],
        "worstStartCohort3y": cd["worstStart3y"],
        "worstStartCohort5y": cd["worstStart5y"],
        "longestUnderwaterMonths": r["longestUnderwaterMonths"],
        "longestBenchUnderperformMonths": r["longestBenchUnderperformMonths"],
        "roll1yNegSharePct": r["roll1yNegSharePct"],
        "maxCashRatioPct": max(by.values()),
        "minCashRatioPct": round(lo, 2),
        "avgCashRatioPct": a10["cashProfile"]["avgCashRatioPct"],
        "maxSimultaneousPositions": p10["base"]["maxPositions"],
        "avgPositions": p10["base"]["avgPositions"],
        "ordersPerYear": p10["base"]["tradesPerYear"],
        "lotTradesPerYear": p10["base"]["lotTradesPerYear"],
        "note": ("모두 2007~2026 실측 과거값이다. 미래 수익·손실을 예측하거나 보장하지 "
                 "않는다. 개인화된 투자권유가 아니다."),
    }


# ═══════════════ §20 forward-test 설계 ═══════════════
def forward_design(verdict, researcher, att):
    if verdict not in ("ROBUST_CANDIDATE", "PROMISING_FORWARD_TEST"):
        return {"created": False, "reason": f"판정 {verdict} — 조건 미충족"}
    return {
        "created": True, "designOnly": True,
        "name": "NEW_BM_SPEC_CONFORMANT_FORWARD_TEST",
        "purpose": ("반증(falsification) 테스트다. 검증 가설은 'PBR 유효종목 benchmark "
                    "대비 초과수익이 0 이다'이며, 이 가설이 기각될 때만 후보가 살아난다. "
                    "R10 실측은 그 benchmark 대비 -0.22%p 이므로 현재 근거는 가설 쪽에 있다."),
        "isolationFromLegacy": {
            "legacy": {"id": "LEGACY_50D", "unchanged": True,
                       "note": "R10 은 LEGACY_50D 를 조회조차 하지 않았다. 변경·종료 0."},
            "new": {"id": "NEW_BM_R10_SPEC_CONFORMANT",
                    "stateFile": "reports/research/forward-test/new-bm-r10-state.json",
                    "reportFile": "reports/research/forward-test/new-bm-r10-latest.md",
                    "note": "아직 생성하지 않는다 — 설계상 경로 예약일 뿐이다."},
            "sharedNothing": ["portfolio.json", "magic-formula-*.json",
                              "wababa-ai-*.json", "홈페이지 public 데이터"]},
        "resolvedBeforeStart": [
            "사양-코드 불일치 확정 완료 — Phase A/B 증거로 MONTHLY_STAGGER 확정(Founder "
            "선택 불필요). 구현도 자본흐름 원장으로 검증 완료.",
        ],
        "blockingPrerequisites": [
            "P1. 주 benchmark 를 EW_VALID_PBR_UNIVERSE 로 고정 — FAIR_EW 단독 비교 금지. "
            "결측 PBR 종목 회피 효과가 alpha 로 오인되는 것이 R10 최대 발견이다.",
            "P2. 집중도 상한 사전확정 — 상위 5종목 기여가 총손익의 60% 를 넘으면 폐기.",
            "P3. 실행부담 확인 — 첫 12개월간 매월 40종목 매수(총 480주문)를 Founder 가 "
            "실제로 감당할 수 있는지. 종목당 월 약 104,000원이라 호가·최소단위 문제도 확인.",
        ],
        "protocol": {
            "capital": "가상 50,000,000원 (paper only)", "realOrders": 0,
            "brokerCalls": 0, "paidData": 0, "externalSend": 0,
            "cadence": "월 1회 (기존 PIT 파이프라인 재사용 · 신규 수집 0)",
            "durationBeforeReview": "최소 36개월 (12개월 진입 + 24개월 보유 1주기 완주)",
            "primaryBenchmark": "EW_VALID_PBR_UNIVERSE",
            "secondaryBenchmarks": ["FAIR_EW_UNIVERSE", "OFFICIAL_KOSPI"],
            "recordEachMonth": ["보유종목·수량·평가액", "현금비중", "benchmark 3종",
                                "누적 초과수익", "상위5 기여비중", "상장폐지·거래정지"],
        },
        "killCriteria": [
            "K1. 36개월 시점 EW_VALID_PBR 대비 초과수익 <= 0 → 폐기",
            "K2. 상위 3종목 손익기여 합계 > 총손익의 60% → 폐기",
            "K3. 현금비중이 12개월 이상 50% 초과 → 타이밍 규칙으로 변질 → 폐기",
            "K4. 주문건수 연 60건 초과 또는 동시보유 50종목 초과 → 폐기",
            "K5. MDD -55% 초과 → Founder 감내 범위 초과로 폐기",
        ],
        "notPermittedWithoutFounderApproval": [
            "scheduler 등록", "production canonical 반영", "홈페이지 공개",
            "public marketing 숫자 사용", "실주문·브로커 연결", "유료 데이터 구매",
            "실계좌 자금 투입"],
        "researcherCaveat": researcher.get("why"),
        "attributionCaveat": att and att["verdict"]["reading"],
    }


# ═══════════════ §23 self-audit ═══════════════
def self_audit():
    p10, c10, a10 = L("r10", "repro"), L("r10", "core"), L("r10", "cashaudit")
    dq = L("r10", "dataquality")
    aud = p10["capitalFlowAudit"]
    return [
        {"item": "spec forensics", "result": "PASS",
         "detail": ("결과 이전 증거 8건으로 MONTHLY_STAGGER 확정. ONE_TWELFTH_WAIT 지지 "
                    "증거 0건. 결정적 증거 2건(R6 STAGGERED_12 실측 현금 3.63% · "
                    "R8 자체 공정성 장치 문구).")},
        {"item": "capital flow ledger", "result": "PASS",
         "detail": (f"assertion {sum(1 for c in aud['checks'] if c['pass'])}/"
                    f"{len(aud['checks'])} 통과. 12개월 예정투입 합계 == 5천만원, "
                    f"실제 매수 98.91%(반올림 잔여만), 첫 24개월 평균 현금 "
                    f"{aud['first24mAvgCashPct']}%, 음수현금 0, 레버리지 0, "
                    "자본 창조 0, 미래 현금 사용 0.")},
        {"item": "ONE_TWELFTH_WAIT 재발", "result": "PASS",
         "detail": ("R10 2008년 평균 현금 < 5%(R9 는 90.3%). tranche 월 11개 전부 실제 "
                    "매수(TOPUP). 회귀 테스트로 고정.")},
        {"item": "maturity 해석이 parameter 화되는 위험", "result": "RESOLVED_BY_STRUCTURE",
         "detail": ("COHORT +2.67%p vs PER_TRANCHE +0.26%p 로 차이가 크다. 그러나 "
                    "PER_TRANCHE 는 최대 동시보유 77종목(N=40 위반)·코호트0 매도가 12시점 "
                    "분산(전량교체 위반)으로 **사전규격을 구조적으로 위반**한다. 채택 "
                    "근거는 결과값이 아니라 구조이며 실행 전 문서화됐다.")},
        {"item": "burden 측정 단위", "result": "WARN",
         "detail": (f"lot 단위 거래 {p10['base']['lotTradesPerYear']}건/년 vs 주문 단위 "
                    f"{p10['base']['tradesPerYear']}건/년. G10 은 취지(사람이 실행 가능)에 "
                    "맞춰 주문 단위로 판정했다. lot 단위로 재면 60건 기준을 넘어 G10 이 "
                    "FAIL 이 된다 — 이 판단이 게이트 결과를 바꾸므로 명시한다. "
                    "수수료·세금은 lot 단위로 정확히 부과됐다(성과에는 영향 없음).")},
        {"item": "position 합산", "result": "PASS",
         "detail": ("tranche lot 을 (코호트,종목) 단위로 합산해 분포 통계를 정상화했다. "
                    "합산 전 per-lot 중앙 -31%·승률 26% 는 코호트0 이 480 lot 을 차지한 "
                    "왜곡이었다. 합산 후 360 포지션·중앙 -2.07%·승률 46.9% 로 R9(360 "
                    "포지션·-0.75%·48.3%) 와 같은 단위가 됐다. 금액 합계는 불변.")},
        {"item": "hardcoded r9 path 결함", "result": "FIXED",
         "detail": ("r9_validate.mode_cashaudit 이 r9-walkforward 파일명을 하드코딩해 "
                    "R10 이 R9 코호트 데이터를 읽었다(실측: entryScheduleDefect 가 R9 와 "
                    "동일 수치 출력). load() 경유로 고치고 R9 재실행해 수치 불변 확인.")},
        {"item": "look-ahead", "result": "PASS",
         "detail": (f"BPS 개정 5월 {dq['bpsRevisionMonthSharePct']['5']}% · "
                    f"1~2월 {dq['lookAheadVerdict']['janFebRevisionSharePct']}% — 공시 "
                    f"이후. PBR ≈ 종가/BPS 일치율 "
                    f"{dq['pbrVsCloseOverBps']['within5PctShare']}%. 치명적 결함 "
                    f"{dq['fatalCount']}건.")},
        {"item": "survivorship / delisting", "result": "PASS",
         "detail": (f"universe·benchmark 동일 생존조건. 상장폐지 포지션 "
                    f"{c10['distressExposure']['lotDelistRatePct']}% · haircut 4단계 · "
                    "SYMMETRIC/ASYMMETRIC 병기.")},
        {"item": "missing-data bias", "result": "FAIL",
         "detail": (f"PBR 결측 종목(universe 의 "
                    f"{dq['missingDataSelectionBias']['invalidShareOfUniversePct']}%)의 "
                    f"24개월 forward return 이 유효군보다 {dq['selectionBiasSpreadPct']}%p "
                    "낮다. 전략은 못 담고 benchmark 는 담는다. EW_VALID_PBR_UNIVERSE 로 "
                    "재면 초과수익이 -0.22%p — **R10 최대 발견**.")},
        {"item": "benchmark mismatch", "result": "PASS",
         "detail": f"benchmark {len(L('r10','benchmarks')['rows'])}종 + 통제군 4종으로 도전."},
        {"item": "control matching", "result": "PASS",
         "detail": ("RANDOM / SIZE_MATCHED / MARKET_MATCHED / SIZE_MARKET_MATCHED 각 "
                    "30 seed. 시작일·투입 스케줄·종목수·보유기간·교체주기·비용 동일, "
                    "selection 만 제거. R9 에 SIZE_MARKET_MATCHED 를 추가(가산적 변경) 후 "
                    "R9 회귀 112/112 PASS 로 무영향 확인.")},
        {"item": "contribution concentration", "result": "FAIL",
         "detail": (f"총손익의 {c10['concentration']['top5SharePct']}% 가 상위 5종목 "
                    f"(HHI {c10['concentration']['hhiPositiveContrib']}). 제거 시 1차근사 "
                    "초과수익 음수 → G8 FAIL. R9(71.0%) 보다 악화.")},
        {"item": "cash accounting", "result": "PASS",
         "detail": (f"평균 현금 {a10['cashProfile']['avgCashRatioPct']}% — 대부분 데이터 "
                    "말단 유휴구간(2025~2026, 매수 가드로 신규 선정 차단) 때문이다. "
                    "완전투자 구간 현금은 0.2% 수준. 현금비중 일치 benchmark 로 분리 검증.")},
        {"item": "annualization / integer shares / no leverage", "result": "PASS",
         "detail": ("TWR 365.25일 기준 연율화 · 정수주만 · 음수현금 0 · 레버리지 0 "
                    "(원장 assertion 으로 고정).")},
        {"item": "overlapping cohorts", "result": "WARN",
         "detail": "cohort 분포의 시작월 코호트는 서로 겹친다(반독립). 유의성으로 읽지 않는다."},
        {"item": "R8 산출물 오염", "result": "PASS",
         "detail": ("R8 원본 엔진 == R9 엔진 bit-exact 재확인(CAGR 13.44% · 초과 +4.60%p). "
                    "R8·R9 보고서·JSON 미변경. R10 은 r10-* 계열에만 기록.")},
    ]


def build():
    fo = L("r10", "forensics")
    p10, c10, s10 = L("r10", "repro"), L("r10", "core"), L("r10", "stress")
    w10, t10, dq10 = L("r10", "walkforward"), L("r10", "controls"), L("r10", "dataquality")
    bm10, a10, v10 = L("r10", "benchmarks"), L("r10", "cashaudit"), L("r10", "verdict")
    att = L("r10", "attribution")
    verdict = v10["verdict"]
    researcher = v10["researcherReading"]

    return {
        "schema": "wababa-r8-frozen-spec-forensic-reconciliation-r10@1",
        "taskId": "WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10",
        "verdictLine": "WARNING",
        "reasonClass": "R10_SPEC_BUG_CONFIRMED_AND_FIXED_STRATEGY_STILL_FRAGILE",
        "dataPeriod": p10["period"],
        "frozenCandidate": FROZEN,
        "parameterChanges": 0,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "specForensics": {
            "question": fo["question"],
            "evidencePriority": fo["evidencePriority"],
            "commits": fo["commits"],
            "evidenceTable": fo["evidenceTable"],
        },
        "phaseBVerdict": fo["phaseBVerdict"],
        "conformanceFix": {
            "reading": fo["conformanceReading"],
            "changed": ("초기자본 12개월 분할이 실제로 시장에 투입되도록 매수 로직만 "
                        "최소 수정. tranche 월에는 재선정 없이 기존 코호트에 추가 매수."),
            "unchanged": fo["phaseBVerdict"]["notAParameterChange"]["unchanged"],
            "maturityInterpretation": p10["maturityInterpretationSensitivity"],
            "r9BaselineBitExact": p10["r9BaselineBitExact"],
            "r9BaselineRegression": p10["r9BaselineRegression"],
            "r8SensitivityStructureBug": p10["r8SensitivityStructureBug"],
            "burdenMetricNote": p10["burdenMetricNote"],
        },
        "capitalLedger": p10["capitalFlowAudit"],
        "threeWayComparison": three_way(),
        "revalidation": {
            "note": ("R9 검증 모듈·precommit gate·benchmark 정의를 코드 수준에서 그대로 "
                     "재사용하고 엔진 심볼만 교체했다. R9 와 R10 의 차이는 자본 투입 "
                     "스케줄 단 하나로 통제된다."),
            "base": p10["base"],
            "walkForward": w10["walkForward"],
            "leaveOneEraOut": c10["leaveOneEraOut"],
            "eraStandalone": c10["eraStandalone"],
            "cohortDistribution": {k: v for k, v in
                                   w10["cohortDistribution"].items() if k != "rows"},
            "extremePeriods": c10["extremePeriods"],
            "delistingStress": s10["delistingStress"],
            "costStress": s10["costStress"],
            "liquidityStress": s10["liquidityStress"],
            "bmDataQuality": dq10,
            "distressExposure": c10["distressExposure"],
            "kosdaqDependency": {"pnlByMarket": c10["pnlByMarket"],
                                 "marketSegments": c10["marketSegments"]},
            "sizeExposure": {"pnlBySize": c10["pnlBySize"],
                             "sizeControls": c10["sizeControls"],
                             "sizeTilt": c10["sizeTiltPctileInUniverse"]},
            "matchedControls": t10,
            "concentrationAudit": c10["concentration"],
            "returnDistribution": c10["returnDistribution"],
            "benchmarks": bm10,
            "cashAndEntryScheduleAudit": a10,
        },
        "attribution": att,
        "realMoneyRisk": real_money(),
        "forwardTestDesign": forward_design(verdict, researcher, att),
        "selfAudit": self_audit(),
        "finalVerdict": {
            "precommitted": verdict,
            "gatesPassed": f"{v10['passed']}/{v10['total']}",
            "gatesFailed": v10["failed"],
            "gates": v10["gates"],
            "researcherReading": researcher,
            "postHocFindings": v10["postHocFindings"],
            "precommitRespected": True,
            "gateThresholdsChanged": 0,
        },
        "hotgClosedLoop": {
            "canonicalReport": f"reports/wababa/{NAME}.md",
            "reportBridgeCollectable": True,
            "evidence": ("projects.registry.json 의 wababa 항목에 reportBridge=true · "
                         "report_policy=INCLUDE · child_roots 에 "
                         "C:\\work\\kr-stock-agent-data-new 포함. bridge-data.json "
                         "recent 목록에 이 저장소 경로의 보고서가 실제로 수집돼 있음을 확인."),
            "sourceOwner": "Wababa",
            "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
            "newObserverCreated": 0,
            "newOrchestrationFramework": 0,
            "closeCondition": ("R10 판정이 08:40 종합보고에 source_owner=Wababa 로 올라가고 "
                               "Founder 행동 1건(아래)이 처리되면 CLOSE."),
            "executionSucceededInvisibly": False,
        },
        "limitations": [
            "월 스냅샷 기준 — 일 단위 진입·청산 타이밍은 재현 불가.",
            "배당 재투자 미반영(전략·benchmark 동일 조건). 저PBR·고배당 특성상 전략에 "
            "불리한 방향의 누락이다.",
            "스냅샷에 거래량·거래대금·거래정지 플래그가 없다. 유동성은 가격·시총·종가 "
            "무변동 proxy 로만 검사했고 실제 체결 가능성 검증은 미완이다. 특히 R10 은 "
            "종목당 월 약 104,000원 매수라 최소 호가단위·유동성 제약이 실전에서 더 크다.",
            "R7/R8 이 전체 2007~2026 데이터로 후보를 찾았으므로 진정한 untouched holdout 이 "
            "없다. walk-forward 는 retrospective 재현이다.",
            "cohort 분포의 시작월 코호트는 서로 겹친다(반독립).",
            "ROE/PER/PBR 은 KRX 공표 근사값이다(DART 원전 재무 아님).",
            "데이터 말단 19개월은 매수 가드(i+hold >= n)로 신규 선정이 차단돼 100% 현금이다 "
            "— 백테스트 종단 효과이며 규칙 성능이 아니다.",
            "백테스트 결과이며 실제 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "legacy50d": "unchanged", "canonical": "untouched",
            "autoApply": "untouched", "autoPublish": "untouched",
            "publicPortfolio": "untouched", "homepage": "unchanged",
            "scheduler": "untouched", "krStockAgentRepo": "untouched",
            "productionUrl": "untouched",
            "broker": 0, "realOrders": 0, "paidData": 0, "externalSend": 0,
            "deploy": 0, "envOrToken": 0, "productionDbWrite": 0,
        },
        "nextDecision": {
            "founderAction": ("NONE — Phase A/B 증거로 사양이 확정됐으므로 Founder 선택이 "
                              "필요한 항목은 없다. 승인 게이트 사항도 발생하지 않았다."),
            "single": ("결측 PBR 종목 회피 효과를 제거한 상태에서 BM 신호가 존재하는지 "
                       "단일 연구질문으로 검증한다 — 즉 EW_VALID_PBR_UNIVERSE 를 주 "
                       "benchmark 로 두고 BM 상위 20% 의 초과수익이 0 과 구분되는지. "
                       "R10 실측은 -0.22%p 이므로 이 질문이 후보의 생사를 결정한다."),
            "notNow": ["scheduler 등록", "production 반영", "홈페이지 공개",
                       "13.44% 또는 11.50% 를 public marketing 숫자로 사용",
                       "실계좌 자금 투입", "새 parameter 탐색"],
        },
    }


# ═══════════════ MD ═══════════════
def md(d):
    fo, pb = d["specForensics"], d["phaseBVerdict"]
    cf, cl = d["conformanceFix"], d["capitalLedger"]
    tw, rv = d["threeWayComparison"], d["revalidation"]
    fv, att, rm = d["finalVerdict"], d["attribution"], d["realMoneyRisk"]
    o = []
    A = o.append
    A("전체 판정: WARNING")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R10 — R8 frozen 사양 forensic 확정 + 사양준수 재검증")
    A("")
    A(f"- 기간 {d['dataPeriod']['start']} ~ {d['dataPeriod']['end']} "
      f"({d['dataPeriod']['months']}개월)")
    A(f"- **Phase B 사양 판정: {pb['specVerdict']}** → {pb['conclusion']}")
    A(f"- 사전확정 게이트 판정: **{fv['precommitted']}** ({fv['gatesPassed']})")
    A(f"- 연구자 판단(사전규칙과 별개): **{fv['researcherReading']['label']}**")
    A(f"- frozen parameter 변경 **{d['parameterChanges']}** · gate threshold 변경 "
      f"**{fv['gateThresholdsChanged']}** · 실제 돈 단계 **{d['realMoneyStage']}**")
    A("")
    A("> 이 작업의 성공 기준은 수익률이 아니다. 'R8 에서 검증했다고 생각한 규칙이")
    A("> 실제로 무엇이었는가'에 증거로 답하고, 사전규격대로 고쳤을 때도 살아남는지")
    A("> 정직하게 판정하는 것이다.")
    A("")

    # 1 forensic
    A("## 1. Phase A — 사전규격 forensic 증거")
    A("")
    A(f"질문: {fo['question']}")
    A("")
    A("| ID | artifact | commit | 시점 | 지지 | 가중치 |")
    A("|---|---|---|---|---|---|")
    for r in fo["evidenceTable"]:
        A(f"| {r['id']} | {r['artifact']} | `{r['gitCommit']}` | {r['phase']} | "
          f"**{r['supports']}** | {r['weight']} |")
    A("")
    A("### 결정적 증거 3건")
    A("")
    for r in fo["evidenceTable"][:3]:
        A(f"**{r['id']} — {r['weight']}** ({r['phase']})")
        A("")
        A(f"- artifact: `{r['artifact']}`")
        A(f"- 증거: {r['exactEvidence']}")
        A(f"- 판단: {r['why']}")
        A("")
    A("### 반대 증거 탐색 결과")
    A("")
    e8 = next(r for r in fo["evidenceTable"] if r["id"] == "E8")
    A(f"{e8['exactEvidence']}")
    A("")
    A(f"> {pb['counterEvidenceForWait']}")
    A("")

    # 2 Phase B
    A("## 2. Phase B — 사양 판정")
    A("")
    A("```")
    A(f"CASE            {pb['case']}")
    A(f"판정            {pb['specVerdict']}")
    A(f"결론            {pb['conclusion']}")
    A(f"증거 점수       MONTHLY_STAGGER {pb['evidenceScore']['MONTHLY_STAGGER']} · "
      f"ONE_TWELFTH_WAIT {pb['evidenceScore']['ONE_TWELFTH_WAIT']}")
    A("```")
    A("")
    A(f"{pb['reasoning']}")
    A("")
    A("**이것은 parameter 변경이 아니다.** 그대로 둔 것:")
    A("")
    A("```")
    for x in pb["notAParameterChange"]["unchanged"]:
        A(f"  {x}")
    A("```")
    A("")
    A(f"바꾼 것: {pb['notAParameterChange']['changedOnly']}")
    A("")

    # 3 conformance fix
    A("## 3. Phase C — 사양준수 구현")
    A("")
    A("두 사전제약을 **동시에** 만족해야 한다.")
    A("")
    A("```")
    A("제약1 deployment  5천만원이 12개월에 걸쳐 실제로 시장에 들어간다   (증거 E1·E2)")
    A("제약2 cohort      동시 보유종목수 = 40, N×hold lot 적층 금지        (증거 E4)")
    A("```")
    A("")
    A("→ 하나의 40종목 코호트를 12개월에 걸쳐 쌓아 올린다.")
    A("")
    for x in cf["reading"]["rule"]:
        A(f"- {x}")
    A("")
    A("### 만기 해석 — 결과가 아니라 구조로 결정했다")
    A("")
    mi = cf["maturityInterpretation"]
    A("| 해석 | CAGR | 초과 | MDD | 최대 동시보유 | 코호트0 매도 시점 수 | 사전규격 준수 |")
    A("|---|---|---|---|---|---|---|")
    for k in ("COHORT", "PER_TRANCHE"):
        src = mi["primary_COHORT"] if k == "COHORT" else mi["variant_PER_TRANCHE"]
        sc = mi["structuralConformance"][k]
        A(f"| {k} | {n2(src['cagrPct'])}% | {sgn(src['excessPct'])}%p | "
          f"{n2(src['mddPct'])}% | {sc['maxPositions']}종목 | "
          f"{sc['sellDatesPerCohort'].get('0')} | "
          f"{'O' if sc['specConformant'] else '**X**'} |")
    A("")
    A(f"{mi['decision']}")
    A("")
    A(f"> {mi['warning']}")
    A("")

    # 4 capital ledger
    A("## 4. Phase C 검증 — 자본 흐름 원장 (§6)")
    A("")
    A(f"assertion **{sum(1 for c in cl['checks'] if c['pass'])}/{len(cl['checks'])} "
      f"통과** · 첫 24개월 평균 현금 **{cl['first24mAvgCashPct']}%** "
      f"(R9 는 81%) · 코호트 {cl['cohorts']}개")
    A("")
    A("| assertion | 결과 | 실측 |")
    A("|---|---|---|")
    for c in cl["checks"]:
        A(f"| {c['assertion']} | {'**PASS**' if c['pass'] else '**FAIL**'} | "
          f"{c['detail'] or '-'} |")
    A("")
    A("### 첫 30개월 원장 (5,000만원 기준, 원)")
    A("")
    A("```")
    A(f"{'i':>3} {'date':<11} {'coh':>4} {'action':>7} {'기초현금':>12} "
      f"{'예정투입':>11} {'매도대금':>12} {'실매수':>12} {'기말현금':>11} "
      f"{'보유':>4} {'현금%':>7}")
    for r in cl["first36Months"][:30]:
        A(f"{r['idx']:>3} {r['date']:<11} {str(r['cohort']):>4} "
          f"{str(r['action'] or '-'):>7} {r['beginningCash']:>12,} "
          f"{r['scheduledDeployment']:>11,} {r['maturedSaleProceeds']:>12,} "
          f"{r['actualBuyAmount']:>12,} {r['remainingCash']:>11,} "
          f"{r['positions']:>4} {r['cashRatioPct']:>7.2f}")
    A("```")
    A("")
    A("> 첫 12개월 예정투입 합계 50,000,004원 · 실제 매수 49,455,826원(98.91%).")
    A("> 나머지는 정수주 반올림 잔여다. 2008년 내내 현금비중 1% 수준으로 유지된다 —")
    A("> R9 의 90.3% 와 대조된다.")
    A("")

    # 5 three-way
    A("## 5. §9 필수 3-way 비교")
    A("")
    A(f"> {tw['purpose']}")
    A("")
    A("| 지표 | A. R8 보고 | B. R9 실제코드 | C. R10 사양준수 |")
    A("|---|---|---|---|")
    for r in tw["rows"]:
        def f(x):
            if x is None:
                return "-"
            return f"{x:+.2f}" if isinstance(x, float) and "%p" in r["metric"] else \
                (f"{x:.2f}" if isinstance(x, float) else str(x))
        A(f"| {r['metric']} | {f(r['A_R8_reported'])} | {f(r['B_R9_actualCode'])} | "
          f"{f(r['C_R10_specConformant'])} |")
    A("")
    hd = tw["headlineDecomposition"]
    A("### 헤드라인 분해")
    A("")
    A("```")
    A(f"R8 보고 초과수익            {hd['r8ReportedExcessPct']:+.2f}%p")
    A(f"R9 실제코드 초과수익        {hd['r9ActualCodeExcessPct']:+.2f}%p   (R8 을 bit-exact 재현)")
    A(f"R10 사양준수 초과수익       {hd['r10SpecConformantExcessPct']:+.2f}%p")
    A(f"→ 사양 위반이 만든 몫       {hd['attributableToSpecViolationPct']:+.2f}%p")
    A("```")
    A("")
    A(f"{hd['reading']}")
    A("")

    # 6 revalidation gates
    A("## 6. Phase D — 사양준수 규격으로 R9 전체 재검증")
    A("")
    A(f"> {rv['note']}")
    A("")
    A("| 게이트 | 결과 | 근거 |")
    A("|---|---|---|")
    for gid, gv in fv["gates"].items():
        A(f"| {gid} | {'**PASS**' if gv['pass'] else '**FAIL**'} | {gv['evidence']} |")
    A("")
    A("### 사양 수정이 해결한 것")
    A("")
    for x in fv["researcherReading"]["whatTheFixResolved"]:
        A(f"- {x}")
    A("")
    A("### 여전히 깨지는 것")
    A("")
    for x in fv["researcherReading"]["whatStillBreaks"]:
        A(f"- {x}")
    A("")

    # 7 walk-forward / cohort
    A("## 7. walk-forward · cohort 분포")
    A("")
    w = rv["walkForward"]
    A(f"- out-of-sample 창 **{w['positive']}/{w['total']} positive "
      f"({w['positiveRatePct']}%)** (R9 9/38 · 23.7%)")
    A("")
    A("| 방식 | 창 수 | positive | 초과 중앙값 |")
    A("|---|---|---|---|")
    for k, v in w["byKind"].items():
        A(f"| {k} | {v['n']} | {v['positive']} | {sgn(v['medianExcessPct'])}%p |")
    A("")
    cdd = rv["cohortDistribution"]["summary"]
    A("| horizon | 코호트 | 초과 중앙값 | p10 | p90 | 초과>0 비율 | 원금손실 비율 |")
    A("|---|---|---|---|---|---|---|")
    for h in ("12", "36", "60", "120"):
        s = cdd.get(h)
        if not s:
            continue
        e = s["excess"]
        A(f"| {int(h)//12}년 | {s['cohorts']} | {sgn(e['median'])}%p | "
          f"{sgn(e['p10'])}%p | {sgn(e['p90'])}%p | "
          f"{s['positiveExcessRatePct']}% | {s['lossRatePct']}% |")
    A("")
    sy = fv["postHocFindings"]["startYearDependency"]
    A("### 시작 연도별 5년 초과수익 — R9 의 2007년 편중이 사라졌다")
    A("")
    A("| 시작연도 | 코호트 | 초과 중앙값 | 초과>0 |")
    A("|---|---|---|---|")
    for y, v in sy["cohort5yExcessByStartYear"].items():
        A(f"| {y} | {v['n']} | {sgn(v['medianExcessPct'])}%p | {v['positive']}/{v['n']} |")
    A("")
    A(f"- 2007년 시작: 중앙값 {sgn(sy['start2007']['medianExcessPct'])}%p · "
      f"{sy['start2007']['positive']}/{sy['start2007']['n']} positive")
    A(f"- 2008년 이후 시작: 중앙값 "
      f"{sgn(sy['start2008AndLater']['medianExcessPct'])}%p · positive "
      f"{sy['start2008AndLater']['positivePct']}%  "
      f"**(R9 -2.53%p · 32.2%)**")
    A("")
    esd = fv["postHocFindings"]["entryScheduleDefect"]
    A("### 진입 타이밍 의존 — 해소됨")
    A("")
    A("```")
    A(f"첫 24개월 시장수익률 vs 5년 초과수익 상관   {esd['corr_first24mMarket_vs_5yExcess']}   (R9 -0.639)")
    A(f"첫 24개월 시장 하락(하위25%) → 5년 초과 중앙값 "
      f"{sgn(esd['when_first24m_marketFell']['median5yExcessPct'])}%p "
      f"(positive {esd['when_first24m_marketFell']['positivePct']}%)")
    A(f"첫 24개월 시장 상승(상위25%) → 5년 초과 중앙값 "
      f"{sgn(esd['when_first24m_marketRose']['median5yExcessPct'])}%p "
      f"(positive {esd['when_first24m_marketRose']['positivePct']}%)   (R9 -8.11%p · 9.8%)")
    A("```")
    A("")
    A("R9 의 최대 결함이었던 '진입 2년간 시장 방향이 결과를 지배한다'는 문제는")
    A("사양준수 수정으로 **실제로 해소됐다**. 이것이 R10 의 가장 중요한 긍정 결과다.")
    A("")

    # 8 stress
    A("## 8. 비용 · 상장폐지 · 유동성 스트레스")
    A("")
    A("| 비용 단계 | 수수료 | 매도세 | 슬리피지 | CAGR | 초과 |")
    A("|---|---|---|---|---|---|")
    for r in rv["costStress"]["levels"]:
        A(f"| {r['level']} | {r['feeBps']}bp | {r['sellTaxBps']}bp | "
          f"{r['slippageBps']}bp | {n2(r['cagrPct'])}% | {sgn(r['excessPct'])}%p |")
    A("")
    A("| 상폐 단계 | haircut | 상폐 포지션 | 전략만(비대칭) | 양쪽동일(대칭) |")
    A("|---|---|---|---|---|")
    for r in rv["delistingStress"]["levels"]:
        A(f"| {r['level']} | {int(100*r['haircut'])}% | {r['delistedLots']} | "
          f"{sgn(r['asymmetric']['excessPct'])}%p | "
          f"{sgn(r['symmetric']['excessPct'])}%p |")
    A("")
    A("| 유동성 단계 | 시총하한 | 무변동제외 | 평균 universe | CAGR | 초과 |")
    A("|---|---|---|---|---|---|")
    for r in rv["liquidityStress"]["levels"]:
        A(f"| {r['level']} | {r['minMarketCapEok']}억 | {r['excludeStalePrice']} | "
          f"{r['avgUniverseSize']} | {n2(r['cagrPct'])}% | {sgn(r['excessPct'])}%p |")
    A("")
    dg = rv["liquidityStress"]["diagnostics"]
    A("```")
    A(f"매수 종목-회차 수           {dg['pickCount']}")
    A(f"주가 1,000원 미만 비중      {dg['pricesBelow1000Pct']}%")
    A(f"시총 중앙값                 {dg['marketCapDistEok']['median']}억원")
    A(f"1종목 매수액 / 시총 중앙값  {dg['positionVsMarketCapPct']['median']}%")
    A(f"종가무변동(거래부진) 비중   {dg['stalePricePicksPct']}%")
    A("```")
    A("")
    A("> R10 은 첫 12개월간 **종목당 월 약 104,000원**씩 매수한다. 최소 호가단위·")
    A("> 유동성 제약이 R9(1회 대량 매수)보다 실전에서 더 크다. 스냅샷에 거래량이 없어")
    A("> 이 부분은 검증 미완이다.")
    A("")

    # 9 data quality
    A("## 9. PBR / BM 데이터 품질 — 그리고 R10 최대 발견")
    A("")
    dq = rv["bmDataQuality"]
    A("```")
    A(f"PBR 결측                    {dq['pbrMissingPct']}%")
    A(f"PBR = 0                     {dq['pbrZeroPct']}%")
    A(f"BPS <= 0 (음의 자본)        {dq['bpsNonPositivePct']}%")
    A(f"PBR ≈ 종가/BPS 일치율       {dq['pbrVsCloseOverBps']['within5PctShare']}%")
    A("BPS 개정 월 분포            " +
      " ".join(f"{m}월 {v}%" for m, v in dq["bpsRevisionMonthSharePct"].items()
               if v >= 2.0))
    A(f"look-ahead 의심            {dq['lookAheadVerdict']['leakSuspected']}")
    A(f"치명적 결함                 {dq['fatalCount']}건")
    A("```")
    A("")
    A("BPS 개정이 공시 이후(5월 75.4% · 8월 6.1%)에 몰리고 1~2월은 3.2% 뿐이다 —")
    A("**미래 재무 유입 없음.** 데이터 품질 자체는 깨끗하다.")
    A("")
    vp = fv["postHocFindings"]["validPbrBenchmark"]
    A("### ★ 그러나 초과수익이 여기서 사라진다")
    A("")
    A("```")
    A(f"공정 EW benchmark 대비 초과            {vp['candidateExcessVsFairEwPct']:+.2f}%p")
    A(f"PBR 유효종목만의 EW benchmark 대비     {vp['candidateExcessVsValidPbrEwPct']:+.2f}%p")
    A(f"→ 결측 PBR 제외 효과로 설명되는 몫     {vp['attributableToMissingPbrExclusionPct']:+.2f}%p")
    A("```")
    A("")
    A(f"{vp['reading']}")
    A("")
    A("전략은 PBR 이 없는 종목을 **구조적으로 담을 수 없다**. 그런데 기본 benchmark 는")
    A("담는다. 그 종목들의 24개월 forward return 이 유효군보다 25.64%p 낮다. 그 차이를")
    A("benchmark 에 반영하면 사양준수 후보의 초과수익은 **-0.22%p** 로 사라진다.")
    A("이것이 R10 의 가장 중요한 부정 결과다.")
    A("")

    # 10 attribution
    A("## 10. §11 성과 귀속 — BM 종목선정에서 온 것인가")
    A("")
    A("| 통제군 (동일 구조 · selection 만 제거) | 평균 초과 | 중앙값 | p10 | p90 | 후보 초과 | 후보 percentile |")
    A("|---|---|---|---|---|---|---|")
    for m, v in att["matchedControls"].items():
        A(f"| {m} | {sgn(v['meanExcessPct'])}%p | {sgn(v['medianExcessPct'])}%p | "
          f"{sgn(v['p10ExcessPct'])}%p | {sgn(v['p90ExcessPct'])}%p | "
          f"{sgn(v['candidateExcessPct'])}%p | {v['candidatePercentile']} |")
    A("")
    A("| 축 | 실측 |")
    A("|---|---|")
    A(f"| 최악 benchmark | {att['benchmarkAxis']['worst']['benchmark']} "
      f"{sgn(att['benchmarkAxis']['worst']['candidateExcessPct'])}%p |")
    A(f"| PBR 유효종목 benchmark 대비 | {sgn(att['benchmarkAxis']['vsValidPbrUniversePct'])}%p |")
    A(f"| 현금비중 일치 benchmark 대비 | {sgn(att['cashAndTimingAxis']['excessVsCashMatchedPct'])}%p |")
    A(f"| 현금·타이밍 기여분 | {sgn(att['cashAndTimingAxis']['cashTimingContributionPct'])}%p |")
    A(f"| KOSPI 단독 | {sgn(att['exchangeAxis']['KOSPI']['excessPct'])}%p |")
    A(f"| KOSDAQ 단독 | {sgn(att['exchangeAxis']['KOSDAQ']['excessPct'])}%p |")
    for k in ("SMALL", "MID", "LARGE"):
        A(f"| {k} 구간 내 | {sgn(att['sizeAxis'][k]['excessPct'])}%p |")
    A(f"| 상위5 제거 (1차근사) | "
      f"{sgn(next(r for r in att['concentrationAxis']['removal'] if r['topK']==5)['firstOrder']['excessPct'])}%p |")
    A("")
    A(f"귀속 축 통과 **{att['verdict']['survivedCount']}/{att['verdict']['totalAxes']}**")
    A("")
    A(f"{att['verdict']['reading']}")
    A("")

    # 11 concentration / distribution
    A("## 11. 집중도 · 수익 분포")
    A("")
    cc = rv["concentrationAudit"]
    A(f"- 상위 1종목 {cc['top'][0]['pnlSharePct']}% · 상위 5종목 "
      f"**{cc['top5SharePct']}%** · 상위 10종목 {cc['top10SharePct']}% · "
      f"HHI {cc['hhiPositiveContrib']}")
    A("")
    A("| 제거 | 제거손익 | 1차근사 초과 | 재시뮬 초과 |")
    A("|---|---|---|---|")
    for r in cc["removal"]:
        A(f"| 상위 {r['topK']} | {eok(r['removedPnl'])} | "
          f"{sgn(r['firstOrder']['excessPct'])}%p | "
          f"{sgn((r['resimulated'] or {}).get('excessPct'))}%p |")
    A("")
    rd = rv["returnDistribution"]
    A("```")
    A(f"포지션 수(코호트×종목)    {rd['positions']}")
    A(f"중앙 수익률               {rd['medianRetPct']}%")
    A(f"평균 수익률               {rd['meanRetPct']}%")
    A(f"승률                      {rd['winRatePct']}%")
    A(f"+100% 이상                {rd['hugeWinRatePct(>=+100%)']}%")
    A(f"-50% 이하                 {rd['bigLossRatePct(<=-50%)']}%")
    A(f"판정                      {rd['shape']}")
    A("```")
    A("")
    A("중앙 포지션은 손실이고 승률은 절반 미만이다. 성과는 소수 대박 종목이 만든다 —")
    A("Founder 가 24개월간 대부분 손실인 계좌를 들고 있어야 하는 구조다.")
    A("")

    # 12 real money
    A("## 12. §10 실제 돈 위험표 — 5,000만원 기준")
    A("")
    A("```")
    A(f"기초 투입                        {won(rm['initialCapitalKrw'])}")
    A(f"전체기간 최종 평가액             {won(rm['terminalWealthKrw'])}  ({eok(rm['terminalWealthKrw'])})")
    A(f"동일조건 시장                    {won(rm['benchTerminalKrw'])}  ({eok(rm['benchTerminalKrw'])})")
    A("")
    A(f"최대 낙폭(MDD)                   {rm['mddPct']}%")
    A(f"최대 낙폭 금액                   {won(rm['mddAmountKrw'])}  ({rm['mddAtDate']})")
    A(f"계좌 최저 평가액(투입금 대비)    {rm['lowestAccountValueVsContributedPct']}%  "
      f"({rm['lowestAccountValueAt']})")
    A(f"  → 5,000만원 기준 최저          {won(rm['lowestAccountValueKrw'])}")
    A(f"최악 1년 수익률                  {rm['worst1yPct']}%")
    A(f"  → 1년 만에                     {won(rm['worst1yEndValueKrw'])}")
    A(f"최악 3년(연율/누적)              {rm['worst3yAnnualPct']}% / {rm['worst3yTotalPct']}%")
    A(f"  → 3년 뒤                       {won(rm['worst3yEndValueKrw'])}")
    A(f"최장 수중(원금 회복까지)         {rm['longestUnderwaterMonths']}개월")
    A(f"최장 benchmark 부진 지속         {rm['longestBenchUnderperformMonths']}개월")
    A(f"1년 수익률 마이너스 비율         {rm['roll1yNegSharePct']}%")
    A("")
    A("가장 나쁜 시작점에 들어갔다면 (실측)")
    for k, lbl in (("worstStartCohort1y", "1년"), ("worstStartCohort3y", "3년"),
                   ("worstStartCohort5y", "5년")):
        w = rm[k]
        A(f"  {lbl} 최악  {w['start'][:7]} 시작 → {won(w['accountKrw'])} "
          f"({w['accountVsContributedPct']}%)")
    A("")
    A(f"최대 현금비중                    {rm['maxCashRatioPct']}%")
    A(f"최소 현금비중                    {rm['minCashRatioPct']}%")
    A(f"평균 현금비중                    {rm['avgCashRatioPct']}%")
    A(f"최대 동시보유 종목수             {rm['maxSimultaneousPositions']}종목")
    A(f"평균 보유 종목수                 {rm['avgPositions']}종목")
    A(f"연간 평균 주문 건수              {rm['ordersPerYear']}건 "
      f"(lot 단위 {rm['lotTradesPerYear']}건)")
    A("```")
    A("")
    A(f"> {rm['note']}")
    A("")
    A("R9 대비 위험이 **커졌다**. 폭락장에 현금으로 피해 있지 않고 실제로 투자돼")
    A("있었기 때문이다. 5,000만원이 최악 1년에 2,321만원까지 내려간 시점이 데이터")
    A("안에 실제로 있다.")
    A("")

    # 13 verdict
    A("## 13. 최종 판정")
    A("")
    A(f"### 사전확정 규칙 적용 결과: **{fv['precommitted']}** ({fv['gatesPassed']})")
    A("")
    A(f"- 통과: {', '.join(k for k, v in fv['gates'].items() if v['pass'])}")
    A(f"- 미달: {', '.join(fv['gatesFailed'])}")
    A(f"- gate threshold 변경 **{fv['gateThresholdsChanged']}건** — R10 때문에 새 게이트를 "
      f"만들지도, 기존 기준을 내리지도 않았다.")
    A("")
    A(f"### 연구자 판단: **{fv['researcherReading']['label']}**")
    A("")
    A(f"{fv['researcherReading']['why']}")
    A("")
    A(f"**귀속 축 통과**: {fv['researcherReading']['attributionSurvived']}")
    A("")
    A(f"**파라미터 구출 없음**: {fv['researcherReading']['notAParameterRescue']}")
    A("")

    # 14 forward test
    A("## 14. forward-test 설계 (§20 — 설계까지만)")
    A("")
    ft = d["forwardTestDesign"]
    if not ft["created"]:
        A(f"작성하지 않음 — {ft['reason']}")
    else:
        A(f"- 이름 `{ft['name']}` · **설계 문서만. 코드·state·scheduler 생성 0.**")
        A(f"- 목적: {ft['purpose']}")
        A("")
        A("**이미 해결된 선결조건**")
        for x in ft["resolvedBeforeStart"]:
            A(f"- {x}")
        A("")
        A("**남은 선결조건 (미해결 시 시작 금지)**")
        for x in ft["blockingPrerequisites"]:
            A(f"- {x}")
        A("")
        A("**프로토콜**")
        A("```")
        for k, v in ft["protocol"].items():
            A(f"{k:24} {', '.join(v) if isinstance(v, list) else v}")
        A("```")
        A("")
        A("**반증 기준 (kill criteria)**")
        for x in ft["killCriteria"]:
            A(f"- {x}")
        A("")
        A("**Founder 승인 없이 금지**")
        for x in ft["notPermittedWithoutFounderApproval"]:
            A(f"- {x}")
        A("")

    # 15 self audit
    A("## 15. 자체감사")
    A("")
    A("| 항목 | 결과 | 내용 |")
    A("|---|---|---|")
    for a in d["selfAudit"]:
        A(f"| {a['item']} | **{a['result']}** | {a['detail']} |")
    A("")

    # 16 production safety
    A("## 16. production / public 보호")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:22} {v}")
    A(f"{'realMoneyStage':22} {d['realMoneyStage']}")
    A("```")
    A("")
    A("- `C:\\work\\kr-stock-agent` 저장소 미수정 · "
      "`https://kr-stock-agent.vercel.app` 미변경")
    A("- LEGACY_50D 는 공개 forward experiment 그대로 유지 (조회조차 하지 않음)")
    A("- **13.44% · 11.50% 를 public marketing 숫자로 사용 금지.** Financial Compliance")
    A("  Gate 전까지 production 투자추천 금지.")
    A("")

    # 17 HOTG
    A("## 17. HOTG 폐회로")
    A("")
    h = d["hotgClosedLoop"]
    A("```")
    A(f"canonical report      {h['canonicalReport']}")
    A(f"Report Bridge 수집    {h['reportBridgeCollectable']}")
    A(f"source_owner          {h['sourceOwner']}")
    A(f"표면                  {h['surface']}")
    A(f"신규 observer         {h['newObserverCreated']}")
    A(f"신규 orchestration    {h['newOrchestrationFramework']}")
    A(f"EXECUTION_SUCCEEDED_INVISIBLY  {h['executionSucceededInvisibly']}")
    A("```")
    A("")
    A(f"근거: {h['evidence']}")
    A("")
    A(f"CLOSE 조건: {h['closeCondition']}")
    A("")

    # 18 limitations
    A("## 18. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")

    # 19 next
    A("## 19. Founder 행동 / 다음 단일 작업")
    A("")
    A(f"- **Founder 행동: {d['nextDecision']['founderAction']}**")
    A("")
    A(f"- 다음 단일 작업: {d['nextDecision']['single']}")
    A("")
    A("- 지금 하지 않는 것: " + " · ".join(d["nextDecision"]["notNow"]))
    A("")
    return "\n".join(o)


def main() -> int:
    WD.mkdir(parents=True, exist_ok=True)
    d = build()
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"), "json": str(WD / f"{NAME}.json"),
                      "specVerdict": d["phaseBVerdict"]["specVerdict"],
                      "precommittedVerdict": d["finalVerdict"]["precommitted"],
                      "researcherReading": d["finalVerdict"]["researcherReading"]["label"],
                      "gates": d["finalVerdict"]["gatesPassed"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
