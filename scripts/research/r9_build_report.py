#!/usr/bin/env python3
"""R9 §24 — canonical 산출물 2개 생성 (MD + JSON).

WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9

reports/wababa/wababa-frozen-candidate-independent-validation-r9-latest.{md,json}

§20 forward-test 설계는 판정이 ROBUST_CANDIDATE / PROMISING_FORWARD_TEST 일 때만 쓴다.
production 변경 0 · scheduler 등록 0 · 홈페이지 0 · 실주문 0 · 브로커 0.
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
NAME = "wababa-frozen-candidate-independent-validation-r9-latest"


def L(n):
    return json.loads((RD / f"r9-{n}-latest.json").read_text(encoding="utf-8"))


def won(x, unit="원"):
    if x is None:
        return "-"
    return f"{round(x):,}{unit}"


def m_won(x):
    if x is None:
        return "-"
    return f"{x / 1e8:.2f}억원"


def sgn(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


# ───────────────────────── §20 forward-test 설계 ─────────────────────────
def forward_design(verdict, researcher):
    if verdict not in ("ROBUST_CANDIDATE", "PROMISING_FORWARD_TEST"):
        return {"created": False,
                "reason": f"판정이 {verdict} — §20 조건(ROBUST/PROMISING) 미충족"}
    return {
        "created": True,
        "designOnly": True,
        "name": "NEW_BM_CANDIDATE_FORWARD_TEST",
        "purpose": ("이 규칙이 좋다는 확인이 아니다. **반증(falsification) 테스트**다. "
                    "검증할 가설은 '2012년 이후 out-of-sample 부진이 계속된다'이며, "
                    "그 가설이 기각될 때만 후보가 살아난다."),
        "isolationFromLegacy": {
            "legacy": {"id": "LEGACY_50D", "unchanged": True,
                       "note": "R9 는 LEGACY_50D 를 조회조차 하지 않는다. 종료·변경 0."},
            "new": {"id": "NEW_BM_R8_FROZEN",
                    "canonical": "별도 state 파일 — LEGACY_50D 와 파일·상태·보고 완전 분리",
                    "stateFile": "reports/research/forward-test/new-bm-r8-frozen-state.json",
                    "reportFile": "reports/research/forward-test/new-bm-r8-frozen-latest.md",
                    "note": "아직 생성하지 않는다. 설계 문서상의 경로 예약일 뿐이다."},
            "sharedNothing": ["portfolio.json", "magic-formula-*.json",
                              "wababa-ai-*.json", "홈페이지 public 데이터"]},
        "blockingPrerequisites": [
            "P1. 사양-코드 불일치 확정 — '12개월 매월 분할 매수'인가, '1/12 매수 후 24개월 "
            "대기'인가. Founder 가 어느 쪽이 규칙인지 확정해야 한다. 확정 없이는 시작 금지.",
            "P2. 진입 스케줄 결함 처리방침 확정 — 첫 24개월 평균 80% 현금 상태를 규칙으로 "
            "인정할지, 결함으로 볼지. 인정한다면 그것은 '시장 타이밍 규칙'이므로 "
            "가치투자 규칙으로 설명하지 않아야 한다.",
            "P3. 반증 기준 사전확정 — 아래 killCriteria 를 시작 전에 파일로 고정."],
        "protocol": {
            "capital": "가상 5,000만원 (paper only)",
            "realOrders": 0, "brokerCalls": 0, "paidData": 0, "externalSend": 0,
            "cadence": "월 1회 스냅샷 기록 (기존 PIT 파이프라인 재사용, 신규 수집 0)",
            "durationBeforeReview": "최소 24개월 (보유기간 1주기 = 코호트 1개 완주)",
            "recordEachMonth": ["보유종목·수량·평가액", "현금비중", "benchmark 3종 "
                                "(FAIR_EW / EW_VALID_PBR / OFFICIAL_KOSPI) 동시 기록",
                                "누적 초과수익", "상장폐지·거래정지 발생"],
            "benchmarkRule": ("FAIR_EW 단독 비교 금지. EW_VALID_PBR_UNIVERSE 를 **주 "
                              "benchmark** 로 쓴다 — 결측 PBR 제외 효과가 alpha 로 "
                              "오인되는 것을 처음부터 막는다.")},
        "killCriteria": [
            "K1. 24개월 시점 EW_VALID_PBR 대비 초과수익 <= 0 → 후보 폐기",
            "K2. 어느 시점이든 상위 3종목 손익기여 합계 > 총손익의 60% → FRAGILE 재확인·폐기",
            "K3. 현금비중이 12개월 이상 50% 초과 → 가치투자 규칙이 아니라 타이밍 규칙 → 폐기",
            "K4. 실행부담이 연 60거래·50종목 초과 → 폐기"],
        "notPermittedWithoutFounderApproval": [
            "scheduler 등록", "production canonical 반영", "홈페이지 공개",
            "public marketing 숫자 사용", "실주문·브로커 연결", "유료 데이터 구매"],
        "researcherCaveat": researcher.get("why"),
    }


# ───────────────────────── §23 self-audit ─────────────────────────
def self_audit(rep, core, stress, dq, ca, wf):
    return [
        {"item": "look-ahead", "result": "PASS",
         "detail": (f"BPS 개정 시점 분포: 5월 {dq['bpsRevisionMonthSharePct']['5']}% · "
                    f"8월 {dq['bpsRevisionMonthSharePct']['8']}% · "
                    f"1~2월 {dq['lookAheadVerdict']['janFebRevisionSharePct']}%. "
                    "공시 이후에 바뀌므로 미래정보 유입 없음. 매수·매도는 항상 당일 종가.")},
        {"item": "survivorship", "result": "PASS",
         "detail": ("universe 는 그 날 상장돼 있던 종목이고 이후 상장폐지 종목도 포함된다. "
                    "benchmark 도 동일 생존조건(R5 수정 반영).")},
        {"item": "delisting", "result": "PASS",
         "detail": (f"상장폐지 lot {core['distressExposure']['lotDelistRatePct']}% · "
                    f"haircut 0/75/90/100% 4단계 검사 · SYMMETRIC/ASYMMETRIC 병기.")},
        {"item": "PBR timing", "result": "PASS",
         "detail": (f"PBR ≈ 종가/BPS 일치율 "
                    f"{dq['pbrVsCloseOverBps']['within5PctShare']}% (오차 5% 이내).")},
        {"item": "missing-data bias", "result": "WARN",
         "detail": (f"결측 PBR 종목은 universe 의 "
                    f"{dq['missingDataSelectionBias']['invalidShareOfUniversePct']}% 이고 "
                    f"24개월 forward return 이 유효군보다 {dq['selectionBiasSpreadPct']}%p "
                    "낮다. 전략은 못 담고 benchmark 는 담으므로 초과수익이 과대계상된다. "
                    "→ EW_VALID_PBR_UNIVERSE benchmark 로 분리 측정했다.")},
        {"item": "benchmark mismatch", "result": "PASS",
         "detail": (f"benchmark {len(L('benchmarks')['rows'])}종으로 도전(공정 동일가중 · "
                    "공식 KOSPI/KOSDAQ · 결합 proxy · 시장별 동일가중 · PBR 유효종목 "
                    "동일가중 · size-matched 통제군). 최악 benchmark 도 병기했다.")},
        {"item": "cash accounting", "result": "WARN",
         "detail": (f"평균 현금비중 {ca['cashProfile']['avgCashRatioPct']}% — 2007년 "
                    f"{ca['cashProfile']['byYear'].get('2007')}% · 2008년 "
                    f"{ca['cashProfile']['byYear'].get('2008')}% · 2025년 "
                    f"{ca['cashProfile']['byYear'].get('2025')}%. 회계 자체는 정확하다"
                    "(완전투자 구간에서 현금일치 benchmark 와 공정 benchmark 가 일치하는 "
                    "것으로 검증). 문제는 진입 2년·말단 19개월의 고현금 구간이 전체 "
                    "평균은 상쇄해도 **시작시점별 결과 분산을 지배**한다는 것이다.")},
        {"item": "integer shares", "result": "PASS",
         "detail": "정수주만 매수(int(budget // price)). 소수점 주식 없음. 잔여현금 보유."},
        {"item": "duplicate positions", "result": "PASS",
         "detail": ("단일 코호트 구조라 같은 종목 lot 중복 없음. 실측 평균 "
                    f"{core['burden']['avgPositions']}종목 / 최대 "
                    f"{core['burden']['maxPositions']}종목.")},
        {"item": "overlapping cohorts", "result": "WARN",
         "detail": ("cohort 분포의 시작월 코호트들은 서로 겹친다(반독립). 독립표본이 "
                    "아니므로 percentile 을 통계적 유의성으로 읽지 않는다.")},
        {"item": "annualization", "result": "PASS",
         "detail": ("TWR 를 실제 경과일수(365.25) 기준 연율화. 부분구간은 개월/12 사용. "
                    "현금유입은 TWR 에서 중립화(유입월 base 에 가산).")},
        {"item": "slippage", "result": "PASS",
         "detail": ("BASE 0bp / MODERATE 30bp / HIGH 100bp 편도. 매수 p*(1+s) · "
                    "매도 p*(1-s). benchmark 에는 비용 미부과(전략에 불리한 보수적 비교).")},
        {"item": "control matching", "result": "PASS",
         "detail": ("RANDOM / SIZE_MATCHED / MARKET_MATCHED 각 30 seed. 시작일·투입 "
                    "스케줄·종목수·보유기간·교체주기·비용 동일, selection 만 제거.")},
        {"item": "contribution concentration", "result": "FAIL",
         "detail": (f"총손익의 {core['concentration']['top5SharePct']}% 가 상위 5종목. "
                    f"HHI {core['concentration']['hhiPositiveContrib']}. 상위 5종목 제거 "
                    "1차근사 초과수익 음수 → G8 FAIL.")},
        {"item": "harness: 현금잠김 창", "result": "FIXED",
         "detail": ("최초 walk-forward 창을 48개월로 잡아 두 번째 코호트 매수가 차단되고 "
                    "뒤 24개월이 100% 현금이 됐다(첫 측정 3/29의 원인). 창을 49·73개월 "
                    "(코호트 정수배)로 고치고 cohort 분포에도 최소창 조건을 적용해 전체 "
                    "재실행했다. frozen 파라미터는 변경 0.")},
        {"item": "R8 sensitivity 구조 오류", "result": "FOUND",
         "detail": (rep["r8SensitivityStructureBug"]["impact"] + " " +
                    rep["r8SensitivityStructureBug"]["action"])},
    ]


def build():
    pre = L("precommit")
    rep, core, stress = L("repro"), L("core"), L("stress")
    wf, ctl, dq = L("walkforward"), L("controls"), L("dataquality")
    bm, ca, ver = L("benchmarks"), L("cashaudit"), L("verdict")

    verdict = ver["verdict"]
    researcher = ver["researcherReading"]
    base = rep["base"]
    risk = core["realMoneyRisk"]
    cd = wf["cohortDistribution"]
    fd = forward_design(verdict, researcher)
    audit = self_audit(rep, core, stress, dq, ca, wf)

    out = {
        "schema": "wababa-frozen-candidate-independent-validation-r9@1",
        "taskId": "WABABA-FROZEN-CANDIDATE-INDEPENDENT-VALIDATION-R9",
        "verdictLine": "WARNING",
        "dataPeriod": rep["period"],
        "frozenCandidate": FROZEN,
        "precommittedCriteria": {"gates": GATES, "verdictRules": pre["verdictRules"],
                                 "eras": ERAS, "costLevels": COST_LEVELS,
                                 "delistLevels": DELIST_LEVELS,
                                 "writtenBeforeResults": True,
                                 "file": "reports/research/r9-precommit-latest.json"},
        "reproduction": {"pass": rep["pass"], "bitExact": rep["engineBitExact"],
                         "checks": rep["checks"], "base": base,
                         "r8SensitivityStructureBug": rep["r8SensitivityStructureBug"]},
        "walkForward": wf["walkForward"],
        "leaveOneEraOut": core["leaveOneEraOut"],
        "eraStandalone": core["eraStandalone"],
        "cohortDistribution": {k: v for k, v in cd.items() if k != "rows"},
        "cohortDistributionRows": cd["rows"],
        "extremePeriods": core["extremePeriods"],
        "delistingStress": stress["delistingStress"],
        "costStress": stress["costStress"],
        "liquidityStress": stress["liquidityStress"],
        "bmDataQuality": dq,
        "distressExposure": core["distressExposure"],
        "kosdaqDependency": {"pnlByMarket": core["pnlByMarket"],
                             "marketSegments": core["marketSegments"],
                             "pnlByMarketEra": core["pnlByMarketEra"]},
        "sizeExposure": {"pnlBySize": core["pnlBySize"],
                         "sizeControls": core["sizeControls"],
                         "sizeTiltPctileInUniverse": core["sizeTiltPctileInUniverse"]},
        "matchedControls": ctl,
        "concentrationAudit": core["concentration"],
        "returnDistribution": core["returnDistribution"],
        "benchmarks": bm,
        "cashAndEntryScheduleAudit": ca,
        "realMoneyRisk": {**risk,
                          "worstStartCohort1y": cd["worstStart1y"],
                          "worstStartCohort3y": cd["worstStart3y"],
                          "worstStartCohort5y": cd["worstStart5y"]},
        "forwardTestDesign": fd,
        "selfAudit": audit,
        "finalVerdict": {"precommitted": verdict, "gatesPassed": f"{ver['passed']}/{ver['total']}",
                         "gatesFailed": ver["failed"], "gates": ver["gates"],
                         "labelWarning": ver.get("labelWarning"),
                         "researcherReading": researcher,
                         "reasoning": ver["reasoning"],
                         "postHocFindings": ver["postHocFindings"]},
        "limitations": [
            "월 스냅샷 기준 — 일 단위 진입·청산 타이밍은 재현 불가.",
            "배당 재투자 미반영(전략·benchmark 동일 조건). 배당수익률이 높은 저PBR 종목 "
            "특성상 전략에 불리한 방향의 누락이다.",
            "스냅샷에 거래량·거래대금·거래정지 플래그가 없다. 유동성 검증은 가격·시총·"
            "종가무변동 proxy 로만 했고 실제 체결 가능성 검증은 미완이다.",
            "R7/R8 이 전체 2007~2026 데이터로 후보를 찾았으므로 진정한 untouched holdout 이 "
            "존재하지 않는다. walk-forward 는 retrospective 재현이다.",
            "cohort 분포의 시작월 코호트는 서로 겹친다(반독립). 통계적 유의성으로 읽지 않는다.",
            "ROE/PER/PBR 은 KRX 공표 근사값이다(DART 원전 재무 아님).",
            "백테스트 결과이며 실제 투자 실행 승인이 아니다. 투자 판단은 Founder 책임이다.",
        ],
        "productionChange": {"legacy50d": "unchanged", "canonical": "untouched",
                             "autoApply": "untouched", "autoPublish": "untouched",
                             "publicPortfolio": "untouched", "homepage": "unchanged",
                             "scheduler": "untouched", "broker": 0, "realOrders": 0,
                             "paidData": 0, "externalSend": 0, "deploy": 0,
                             "envOrToken": 0},
        "nextDecision": {
            "single": ("Founder 결정 1건 — frozen candidate 의 사양-코드 불일치를 확정한다. "
                       "'12개월 매월 분할 매수'가 규칙인가, '1/12 매수 후 24개월 현금대기'가 "
                       "규칙인가. 이 결정 없이는 forward-test 를 시작할 수 없다."),
            "notNow": ["scheduler 등록", "production 반영", "홈페이지 공개",
                       "BM 13.44% 를 public marketing 숫자로 사용"],
            "followUpResearchQuestionOnly": (
                "R9 는 파라미터를 구출하지 않았다(§18). 만약 새 후보 탐색이 필요하다면 "
                "별도 연구 질문으로만 제안한다: '진입 스케줄을 현금대기 없이 정의했을 때 "
                "BM 신호가 2012~2026 구간에서 독립적으로 남는가.' 이번 R9 에서는 실행하지 "
                "않았다."),
        },
    }
    return out


# ───────────────────────── MD ─────────────────────────
def md(d):
    base, ver = d["reproduction"]["base"], d["finalVerdict"]
    risk = d["realMoneyRisk"]
    cd = d["cohortDistribution"]
    ca = d["cashAndEntryScheduleAudit"]
    esd = ver["postHocFindings"]["entryScheduleDefect"]
    o = []
    A = o.append
    A("전체 판정: WARNING")
    A("reason_class: R9_PRECOMMIT_PROMISING_BUT_STRUCTURALLY_FRAGILE")
    A("")
    A("# 와바바 R9 — frozen 후보 BM_P20_N40_H24_COHORT 독립 스트레스 검증")
    A("")
    A(f"- 기간 {d['dataPeriod']['start']} ~ {d['dataPeriod']['end']} "
      f"({d['dataPeriod']['months']}개월)")
    A(f"- 사전확정 판정: **{ver['precommitted']}** (게이트 {ver['gatesPassed']})")
    A(f"- 연구자 판단(사전규칙과 별개): **{ver['researcherReading']['label']}**")
    A("- frozen 파라미터 변경 **0** · 새 parameter search **0** · production 변경 **0**")
    A("")
    A("> 이번 작업의 성공 기준은 높은 CAGR 이 아니다. 규칙을 고치지 않고 공격했을 때도")
    A("> forward-test 를 할 이유가 남는지 정직하게 판정하는 것이다.")
    A("")

    A("## 1. FROZEN CANDIDATE (변경 없음)")
    A("")
    A("```")
    A(f"ID              {FROZEN['id']}")
    A(f"시작자금        {FROZEN['initialCapitalKrw']:,}원")
    A(f"팩터            {FROZEN['factorDefinition']}")
    A(f"선정풀          {FROZEN['selectionPool']}")
    A(f"보유종목        {FROZEN['holdings']}종목 균등(정수주)")
    A(f"진입            {FROZEN['entry']}")
    A(f"보유기간        {FROZEN['holdMonths']}개월")
    A(f"매도            {FROZEN['exit']}")
    A(f"재투자          {FROZEN['reinvest']}")
    A("```")
    A("")

    A("## 2. R8 재현 게이트")
    A("")
    A(f"- 결과: **{'PASS' if d['reproduction']['pass'] else 'FAIL'}** · "
      f"엔진 소수점까지 동일(bit-exact): {d['reproduction']['bitExact']}")
    A("")
    A("| 지표 | R8 엔진 | R9 엔진 | R8 보고서 | 허용오차 | 판정 |")
    A("|---|---|---|---|---|---|")
    for c in d["reproduction"]["checks"]:
        A(f"| {c['metric']} | {c.get('r8Engine')} | {c.get('r9Engine')} | "
          f"{c.get('r8Report')} | {c.get('tolerance', '-')} | "
          f"{'O' if c['pass'] else 'X'} |")
    A("")
    bug = d["reproduction"]["r8SensitivityStructureBug"]
    A(f"### 재현 중 발견한 R8 결함 (found: {bug['found']})")
    A("")
    A("| 축 | R8 보고값 | R8 실제 호출(ladder) | frozen cohort(정정) |")
    A("|---|---|---|---|")
    for r in bug["rows"]:
        A(f"| {r['axis']} | {sgn(r['r8ReportedExcessPct'])}%p | "
          f"{sgn(r['asR8Called_ladder']['excessPct'])}%p "
          f"(연 {r['asR8Called_ladder']['tradesPerYear']}건·"
          f"{r['asR8Called_ladder']['avgPositions']}종목) | "
          f"{sgn(r['frozenCohort_correct']['excessPct'])}%p "
          f"(연 {r['frozenCohort_correct']['tradesPerYear']}건·"
          f"{r['frozenCohort_correct']['avgPositions']}종목) |")
    A("")
    A(f"> {bug['impact']}")
    A(f"> {bug['action']}")
    A("")

    A("## 3. 사전확정 판정규칙 (결과 보기 전 저장)")
    A("")
    A(f"정본: `{d['precommittedCriteria']['file']}`")
    A("")
    A("| 게이트 | 규칙 | 결과 | 근거 |")
    A("|---|---|---|---|")
    for gid, gv in ver["gates"].items():
        A(f"| {gid} | {gv['rule']} | {'**PASS**' if gv['pass'] else '**FAIL**'} | "
          f"{gv['evidence']} |")
    A("")

    A("## 4. walk-forward (retrospective)")
    A("")
    w = d["walkForward"]
    A(f"- out-of-sample 창 **{w['positive']}/{w['total']} positive "
      f"({w['positiveRatePct']}%)** → G2 FAIL")
    A(f"- {w['honesty']}")
    A(f"- harness 정정: {w['harnessFix']}")
    A("")
    A("| 방식 | 창 수 | positive | 초과수익 중앙값 |")
    A("|---|---|---|---|")
    for k, v in w["byKind"].items():
        A(f"| {k} | {v['n']} | {v['positive']} | {sgn(v['medianExcessPct'])}%p |")
    A("")
    A("2012년 이후 시작하는 창은 대부분 음수다. 주요 창:")
    A("")
    A("| 테스트 구간 | 후보 CAGR | benchmark | 초과 |")
    A("|---|---|---|---|")
    for r in w["windows"]:
        if r["kind"].startswith("EXPANDING_TRAIN_TEST_TO_END"):
            A(f"| {r['testFrom'][:7]} ~ {r['testTo'][:7]} ({r['testMonths']}m) | "
              f"{r['cagrPct']}% | {r['benchPct']}% | {sgn(r['excessPct'])}%p |")
    A("")

    A("## 5. leave-one-era-out")
    A("")
    loo = d["leaveOneEraOut"]
    A(f"- 시대 1개 제거 시 초과수익 > 0: **{loo['positiveCount']}/{loo['total']}** → G3 PASS")
    A(f"- 방법: {loo['method']}")
    A("")
    A("| 제거 시대 | 제거 개월 | CAGR | benchmark | 초과 |")
    A("|---|---|---|---|---|")
    for r in loo["rows"]:
        if r.get("skipped"):
            continue
        A(f"| {r['era']} {r['label']} | {r['droppedMonths']} | {r['cagrPct']}% | "
          f"{r['benchPct']}% | {sgn(r['excessPct'])}%p |")
    A("")
    A("### 시대별 단독 성과 — 제거검사보다 이쪽이 더 많이 말해준다")
    A("")
    A("| 시대 | 개월 | 후보 총수익 | benchmark 총수익 | 연율 초과 |")
    A("|---|---|---|---|---|")
    for r in d["eraStandalone"]:
        A(f"| {r['era']} {r['label']} | {r['months']} | {r['totalReturnPct']}% | "
          f"{r['benchTotalPct']}% | {sgn(r['excessPct'])}%p |")
    A("")
    A("> 가장 최근 시대 E6_TIGHTEN(2022~2026, 56개월)은 단독 초과수익이 **음수**다.")
    A("")

    A("## 6. rolling cohort 분포 — Founder 가 나쁜 시작점에 들어갔을 때")
    A("")
    A(f"- 방법: {cd['method']}")
    A("")
    A("| horizon | 코호트 | 초과 중앙값 | p10 | p25 | p75 | p90 | 초과>0 비율 | 원금손실 비율 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for h in ("12", "36", "60", "120"):
        s = cd["summary"].get(h)
        if not s:
            continue
        e = s["excess"]
        A(f"| {int(h)//12}년 | {s['cohorts']} | {sgn(e['median'])}%p | {sgn(e['p10'])}%p | "
          f"{sgn(e['p25'])}%p | {sgn(e['p75'])}%p | {sgn(e['p90'])}%p | "
          f"{s['positiveExcessRatePct']}% | {s['lossRatePct']}% |")
    A("")
    A("**모든 horizon 에서 초과수익 중앙값이 음수다.** 전체기간 +4.60%p 는 특정 시작점의 결과다.")
    A("")
    A("### 5년 초과수익 — 시작 연도별")
    A("")
    A("| 시작연도 | 코호트 | 초과 중앙값 | 초과>0 |")
    A("|---|---|---|---|")
    for y, v in ver["postHocFindings"]["startYearDependency"][
            "cohort5yExcessByStartYear"].items():
        A(f"| {y} | {v['n']} | {sgn(v['medianExcessPct'])}%p | {v['positive']}/{v['n']} |")
    A("")
    sy = ver["postHocFindings"]["startYearDependency"]
    A(f"- 2007년 시작: 중앙값 {sgn(sy['start2007']['medianExcessPct'])}%p · "
      f"{sy['start2007']['positive']}/{sy['start2007']['n']} positive")
    A(f"- 2008년 이후 시작: 중앙값 "
      f"{sgn(sy['start2008AndLater']['medianExcessPct'])}%p · "
      f"positive {sy['start2008AndLater']['positivePct']}%")
    A(f"- {sy['reading']}")
    A("")

    A("## 7. ★ 초과수익의 정체 — 진입 스케줄 결함")
    A("")
    A(f"{ca['finding']}")
    A("")
    A("```")
    A("연도별 평균 현금비중")
    for y, v in ca["cashProfile"]["byYear"].items():
        if v is not None and v >= 1.0:
            A(f"  {y}   {v:5.1f}%")
    A("```")
    A("")
    A("| 구간 | 개월 | 평균현금 | 후보 | 공정 benchmark | 초과 | 현금비중일치 benchmark | 초과 |")
    A("|---|---|---|---|---|---|---|---|")
    for s in ca["segments"]:
        A(f"| {s['segment']} | {s['months']} | {s['avgCashPct']}% | "
          f"{s['candTotalPct']}% | {s['fairBenchTotalPct']}% | "
          f"{sgn(s['excessVsFairPct'])}%p | {s['cashMatchedBenchTotalPct']}% | "
          f"{sgn(s['excessVsCashMatchedPct'])}%p |")
    A("")
    sg_wait = ca["segments"][0]
    sg_full = ca["segments"][1]
    sg_idle = ca["segments"][2]
    A("읽는 법 — 세 구간이 방법론을 스스로 검증한다:")
    A("")
    A(f"1. **진입대기 구간**(평균 {sg_wait['avgCashPct']}% 현금): 공정 benchmark 대비 "
      f"{sgn(sg_wait['excessVsFairPct'])}%p 인데 현금비중을 맞추면 "
      f"{sgn(sg_wait['excessVsCashMatchedPct'])}%p 로 줄어든다.")
    A(f"   → 이 구간 초과수익의 대부분("
      f"{sg_wait['excessVsFairPct'] - sg_wait['excessVsCashMatchedPct']:.1f}%p)은 "
      f"종목선정이 아니라 **현금을 들고 폭락을 피한 것**이다.")
    A(f"2. **완전투자 구간**(현금 {sg_full['avgCashPct']}%): "
      f"{sgn(sg_full['excessVsFairPct'])}%p vs "
      f"{sgn(sg_full['excessVsCashMatchedPct'])}%p — 사실상 동일. 현금이 없으면 두 "
      f"benchmark 가 같아지므로 계산이 맞다는 검증이다. 이 16년의 초과수익은 진짜다.")
    A(f"3. **말단 유휴 구간**(현금 {sg_idle['avgCashPct']}%): "
      f"{sgn(sg_idle['excessVsFairPct'])}%p vs "
      f"{sgn(sg_idle['excessVsCashMatchedPct'])}%p — 100% 현금이라 현금일치 benchmark 도 "
      f"0. 규칙이 데이터 끝 20개월간 아무것도 안 했다는 뜻이다.")
    A("")
    A(f"진입대기의 {sgn(sg_wait['excessVsFairPct'])}%p 와 말단 유휴의 "
      f"{sgn(sg_idle['excessVsFairPct'])}%p 가 거의 정확히 상쇄되므로, **전체기간** "
      f"초과수익은 현금비중을 맞춰도 크게 변하지 않는다")
    A(f"(공정 대비 {sgn(ca['fullPeriod']['excessVsFairPct'])}%p → 현금일치 대비 "
      f"{sgn(ca['fullPeriod']['excessVsCashMatchedPct'])}%p).")
    A("")
    A("**그래서 결론은 '전체 초과수익이 전부 현금 덕분'이 아니다.** 그보다 나쁘다:")
    A(f"신규 시작 시 첫 24개월은 평균 {esd['avgCashFirst24mPct']}% 가 현금이므로, 결과가")
    A("'첫 2년간 시장이 떨어졌는가'라는 **통제할 수 없는 우연**에 묶인다:")
    A("")
    A("```")
    A(f"첫 24개월 시장수익률 vs 5년 초과수익 상관   {esd['corr_first24mMarket_vs_5yExcess']}")
    A(f"첫 24개월 시장 하락(하위25%)  → 5년 초과 중앙값 "
      f"{sgn(esd['when_first24m_marketFell']['median5yExcessPct'])}%p  "
      f"(positive {esd['when_first24m_marketFell']['positivePct']}%)")
    A(f"첫 24개월 시장 상승(상위25%)  → 5년 초과 중앙값 "
      f"{sgn(esd['when_first24m_marketRose']['median5yExcessPct'])}%p  "
      f"(positive {esd['when_first24m_marketRose']['positivePct']}%)")
    A("```")
    A("")
    A(f"> {esd['reading']}")
    A("")
    A(f"### 사양-코드 불일치")
    A("")
    A(f"{ca['specVsCode']}")
    A("")

    A("## 8. 극단구간")
    A("")
    A("| 유형 | 구간 | 개월 | benchmark | 후보 | 초과 | 회복 |")
    A("|---|---|---|---|---|---|---|")
    for r in d["extremePeriods"]:
        A(f"| {r['type']} | {r['from'][:7]}~{r['to'][:7]} | {r['months']} | "
          f"{r['benchTotalPct']}% | {r['candTotalPct']}% | "
          f"{sgn(r['excessTotalPct'])}%p | "
          f"{(str(r.get('recoveryMonths')) + '개월') if r.get('recoveryMonths') else '-'} |")
    A("")

    A("## 9. 상장폐지 스트레스")
    A("")
    A(f"- 게이트 판정 모드: **{d['delistingStress']['gateMode']}**")
    A("")
    A("| 단계 | haircut | 상폐 lot | 상폐손실 | 전략만(비대칭) 초과 | 양쪽동일(대칭) 초과 |")
    A("|---|---|---|---|---|---|")
    for r in d["delistingStress"]["levels"]:
        A(f"| {r['level']} | {int(100*r['haircut'])}% | {r['delistedLots']} | "
          f"{m_won(r['delistLossKrw'])} | {sgn(r['asymmetric']['excessPct'])}%p | "
          f"{sgn(r['symmetric']['excessPct'])}%p |")
    A("")
    A("> R8 이 보고한 상폐100% 초과 +0.86%p 는 ladder 구조 값이었다(2절 참조).")
    A("> frozen cohort 로 다시 계산하면 비대칭 +3.52%p · 대칭 +4.54%p 다.")
    A("")

    A("## 10. 비용 / 슬리피지 스트레스")
    A("")
    A("| 단계 | 수수료 | 매도세 | 슬리피지 | CAGR | 초과 | 비용drag | 총비용 | 슬리피지비용 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for r in d["costStress"]["levels"]:
        A(f"| {r['level']} | {r['feeBps']}bp | {r['sellTaxBps']}bp | {r['slippageBps']}bp | "
          f"{r['cagrPct']}% | {sgn(r['excessPct'])}%p | {sgn(r['costDragPct'])}%p | "
          f"{m_won(r['costPaidKrw'])} | {m_won(r['slipPaidKrw'])} |")
    A("")
    A(f"> {d['costStress']['note']}")
    A("")

    A("## 11. 유동성 / 체결가능성")
    A("")
    A("| 단계 | 시총하한 | 무변동제외 | 평균 universe | CAGR | 초과 |")
    A("|---|---|---|---|---|---|")
    for r in d["liquidityStress"]["levels"]:
        A(f"| {r['level']} | {r['minMarketCapEok']}억 | {r['excludeStalePrice']} | "
          f"{r['avgUniverseSize']} | {r['cagrPct']}% | {sgn(r['excessPct'])}%p |")
    A("")
    dg = d["liquidityStress"]["diagnostics"]
    A("```")
    A(f"매수 종목 수                {dg['pickCount']}")
    A(f"주가 1,000원 미만 비중      {dg['pricesBelow1000Pct']}%")
    A(f"주가 500원 미만 비중        {dg['pricesBelow500Pct']}%")
    A(f"시총 중앙값                 {dg['marketCapDistEok']['median']}억원")
    A(f"시총 p10                    {dg['marketCapDistEok']['p10']}억원")
    A(f"1종목 매수액 / 시총 중앙값  {dg['positionVsMarketCapPct']['median']}%")
    A(f"1종목 매수액 / 시총 p90     {dg['positionVsMarketCapPct']['p90']}%")
    A(f"종가무변동(거래부진) 비중   {dg['stalePricePicksPct']}%")
    A("```")
    A("")
    A(f"> {dg['dataLimitation']}")
    A("")

    A("## 12. PBR / BM 데이터 품질 · look-ahead 감사")
    A("")
    dq = d["bmDataQuality"]
    A("```")
    A(f"universe 종목-월 수         {dq['universeRowMonths']:,}")
    A(f"PBR 결측                    {dq['pbrMissingPct']}%")
    A(f"PBR = 0                     {dq['pbrZeroPct']}%")
    A(f"PBR < 0                     {dq['pbrNegativePct']}%")
    A(f"PBR > 100                   {dq['pbrExtremeHighPct(>100)']}%")
    A(f"BPS 결측                    {dq['bpsMissingPct']}%")
    A(f"BPS <= 0 (음의 자본)        {dq['bpsNonPositivePct']}%")
    A(f"PBR ≈ 종가/BPS 일치율       {dq['pbrVsCloseOverBps']['within5PctShare']}%")
    A(f"BPS 개정 중앙 간격          {dq['bpsRevisionMedianGapMonths']}개월")
    A("BPS 개정 월 분포            " +
      " ".join(f"{m}월 {v}%" for m, v in dq["bpsRevisionMonthSharePct"].items()
               if v >= 2.0))
    A(f"look-ahead 의심            {dq['lookAheadVerdict']['leakSuspected']}")
    A(f"치명적 결함                 {dq['fatalCount']}건")
    A("```")
    A("")
    A("BPS 개정이 5월(75.4%)·8월(6.1%)·6월(5.8%) 에 몰린다 — 사업보고서·분기보고서 제출")
    A("**이후**다. 결산기말 직후(1~2월)는 3.2% 뿐이다. **미래 재무 유입 없음.**")
    A("")
    vp = ver["postHocFindings"]["validPbrBenchmark"]
    A("### 그러나 결측 제외가 초과수익을 만든다")
    A("")
    A("```")
    A(f"공정 EW benchmark 대비 초과            {sgn(vp['candidateExcessVsFairEwPct'])}%p")
    A(f"PBR 유효종목만의 EW benchmark 대비     {sgn(vp['candidateExcessVsValidPbrEwPct'])}%p")
    A(f"→ 결측 PBR 제외 효과로 설명되는 몫     {sgn(vp['attributableToMissingPbrExclusionPct'])}%p")
    A("```")
    A("")
    A(f"> {vp['reading']}")
    A("")

    A("## 13. 부실기업 노출 — 위험 프리미엄인가 무료 alpha 인가")
    A("")
    de = d["distressExposure"]
    A("| 항목 | 매수종목 | universe |")
    A("|---|---|---|")
    A(f"| PBR 중앙값 | {de['pickPbr']['median']} | {de['universePbr']['median']} |")
    A(f"| PBR 0.2 미만 비중 | {de['pickExtremeLowPbrPct']}% | "
      f"{de['universeExtremeLowPbrPct']}% |")
    A(f"| 음의 자본(BPS<=0) | {de['pickNegBpsPct']}% | {de['universeNegBpsPct']}% |")
    A(f"| 보유 중 상장폐지 | {de['lotDelistRatePct']}% | - |")
    A(f"| -50% 이하 급락 | {de['crashRatePct(<=-50%)']}% | - |")
    A("")
    A("| 매수시점 | 매수 PBR 중앙 | universe PBR 중앙 | 매수 24개월 상폐율 | universe 상폐율 | 매수 KOSDAQ | universe KOSDAQ |")
    A("|---|---|---|---|---|---|---|")
    for r in de["byBuyDate"]:
        A(f"| {r['date'][:7]} | {r['pickMedianPbr']} | {r['universeMedianPbr']} | "
          f"{r['pickDelist24mPct']}% | {r['universeDelist24mPct']}% | "
          f"{r['pickKosdaqPct']}% | {r['universeKosdaqPct']}% |")
    A("")
    A("판정: **음의 자본 종목은 담지 않았고(0%) 상장폐지율도 universe 와 비슷하다.**")
    A("극단 저PBR 편중(4.17% vs 0.43%)은 있지만 노골적인 부실기업 포트폴리오는 아니다.")
    A("즉 초과수익을 순수 distress risk premium 으로 설명할 근거는 약하다 — 대신")
    A("소형주·KOSDAQ 편중과 진입 스케줄이 더 큰 설명력을 가진다.")
    A("")

    A("## 14. KOSDAQ 의존성")
    A("")
    kd = d["kosdaqDependency"]
    A("| 시장 | lot | 투입비중 | 손익비중 | 평균수익률 | 승률 | 상폐율 |")
    A("|---|---|---|---|---|---|---|")
    for m, v in kd["pnlByMarket"].items():
        A(f"| {m} | {v['lots']} | {v['basisSharePct']}% | {v['pnlSharePct']}% | "
          f"{v['avgRetPct']}% | {v['winRatePct']}% | {v['delistRatePct']}% |")
    A("")
    A("| 단독 실행 | CAGR | benchmark | 초과 |")
    A("|---|---|---|---|")
    for m, v in kd["marketSegments"].items():
        A(f"| {m} only | {v['cagrPct']}% | {v['benchPct']}% | {sgn(v['excessPct'])}%p |")
    A("")
    A("KOSDAQ 은 투입자본의 32.5% 로 총손익의 **49.3%** 를 만들었다(평균수익률 44.4% vs")
    A("KOSPI 22.0%). KOSPI 단독으로는 그 시장의 동일가중을 **못 이긴다(−2.52%p)**.")
    A("→ 전체 +4.60%p 중 KOSDAQ 기여가 절반이며, KOSPI 만으로는 alpha 가 없다.")
    A("")

    A("## 15. size 노출")
    A("")
    se = d["sizeExposure"]
    A("| size 구간 | lot | 투입비중 | 손익비중 | 평균수익률 |")
    A("|---|---|---|---|---|")
    for k in ("SMALL", "MID", "LARGE"):
        v = se["pnlBySize"].get(k)
        if v:
            A(f"| {k} | {v['lots']} | {v['basisSharePct']}% | {v['pnlSharePct']}% | "
              f"{v['avgRetPct']}% |")
    A("")
    A("| size 통제(해당 구간 내에서만 실행) | CAGR | benchmark | 초과 |")
    A("|---|---|---|---|")
    for k, v in se["sizeControls"].items():
        A(f"| {k} | {v['cagrPct']}% | {v['benchPct']}% | {sgn(v['excessPct'])}%p |")
    A("")
    t = se["sizeTiltPctileInUniverse"]
    A(f"매수종목의 universe 내 시총 percentile 중앙값 **{100*t['median']:.0f}%** "
      f"(p10 {100*t['p10']:.0f}% · p90 {100*t['p90']:.0f}%) — 소형주 편중.")
    A("")
    A("**핵심: SMALL 구간이 투입 40% 로 손익 83% 를 만들었지만, SMALL 구간 안에서만")
    A("실행하면 초과수익이 −3.28%p 다.** 즉 'BM 이 소형주 안에서 유효'한 것이 아니라")
    A("'소형주에 있었던 것'이 성과였다. size exposure 를 제거하면 alpha 가 남지 않는다.")
    A("")

    A("## 16. 통제군 대조 (동일 구조 · selection 만 제거)")
    A("")
    A(f"- {d['matchedControls']['matching']}")
    A("")
    A("| 통제군 | seed | 초과 중앙값 | p10 | p90 | 평균 | 후보 초과 | 후보 percentile | 후보 이긴 seed |")
    A("|---|---|---|---|---|---|---|---|---|")
    for m, v in d["matchedControls"]["controls"].items():
        e = v["excess"]
        A(f"| {m} | {v['seeds']} | {sgn(e['median'])}%p | {sgn(e['p10'])}%p | "
          f"{sgn(e['p90'])}%p | {sgn(v['meanExcessPct'])}%p | "
          f"{sgn(v['candidateExcessPct'])}%p | {v['candidatePercentile']} | "
          f"{v['controlsBeatingCandidate']} |")
    A("")
    A("후보는 세 통제군 분포 모두에서 상위권이다(G7 PASS). 무작위 선정이 아니라는 뜻이다.")
    A("단 이 결과는 '전체기간 단일 경로' 기준이며, 시작점을 바꾸면 6절처럼 무너진다.")
    A("")

    A("## 17. 기여 집중도")
    A("")
    cc = d["concentrationAudit"]
    A(f"- 총손익 {m_won(cc['totalPnl'])} · 거래종목 {cc['uniqueTickers']}개")
    A(f"- 상위 1종목 {cc['top'][0]['pnlSharePct']}% · 상위 5종목 **{cc['top5SharePct']}%** · "
      f"상위 10종목 {cc['top10SharePct']}%")
    A(f"- 기여 HHI {cc['hhiPositiveContrib']}")
    A("")
    A("| 상위 기여 종목 | 손익 | 비중 |")
    A("|---|---|---|")
    for r in cc["top"][:8]:
        A(f"| {r['name']} ({r['ticker']}) | {m_won(r['pnl'])} | {r['pnlSharePct']}% |")
    A("")
    A("| 제거 | 제거손익 | 1차근사 초과 | 재시뮬 초과 |")
    A("|---|---|---|---|")
    for r in cc["removal"]:
        A(f"| 상위 {r['topK']} | {m_won(r['removedPnl'])} | "
          f"{sgn(r['firstOrder']['excessPct'])}%p | "
          f"{sgn((r['resimulated'] or {}).get('excessPct'))}%p |")
    A("")
    A("**상위 5종목을 빼면 1차근사 초과수익이 −1.31%p 로 음수가 된다 → G8 FAIL.**")
    A("재시뮬(해당 종목 선정 금지 후 다음 순위로 대체)에서는 +3.94%p 로 남지만, 이는")
    A("사후에 승자를 알고 다른 종목을 채운 결과라 후보의 견고성 근거로 쓸 수 없다.")
    A("")

    A("## 18. 수익 분포 — 많은 작은 승리인가, 몇 개 초대형 승자인가")
    A("")
    rd = d["returnDistribution"]
    A("```")
    A(f"포지션 수                 {rd['positions']}")
    A(f"중앙 수익률               {rd['medianRetPct']}%")
    A(f"평균 수익률               {rd['meanRetPct']}%")
    A(f"승률                      {rd['winRatePct']}%")
    A(f"패률                      {rd['lossRatePct']}%")
    A(f"+50% 이상                 {rd['bigWinRatePct(>=+50%)']}%")
    A(f"+100% 이상                {rd['hugeWinRatePct(>=+100%)']}%")
    A(f"-50% 이하                 {rd['bigLossRatePct(<=-50%)']}%")
    A(f"-90% 이하                 {rd['wipeoutRatePct(<=-90%)']}%")
    A(f"최대                      {rd['percentiles']['max']}%")
    A(f"최소                      {rd['percentiles']['min']}%")
    A(f"판정                      {rd['shape']}")
    A("```")
    A("")
    A("**중앙 포지션은 손실(−0.75%)이고 승률은 절반 미만(48.3%)이다.** 성과는 +100% 이상")
    A("종목 11.4%(특히 최대 +3,390%)가 만든다. '많은 작은 승리'가 아니라 '몇 개 초대형")
    A("승자' 구조다. 이 구조는 Founder 가 실제로 견디기 어렵다 — 대부분의 종목이 손실인")
    A("계좌를 24개월 들고 있어야 한다.")
    A("")

    A("## 19. benchmark 도전")
    A("")
    A(f"후보 CAGR {d['benchmarks']['candidateCagrPct']}%")
    A("")
    A("| benchmark | CAGR | 후보 초과 | 비고 |")
    A("|---|---|---|---|")
    for r in d["benchmarks"]["rows"]:
        A(f"| {r['benchmark']} | {r['cagrPct']}% | {sgn(r['candidateExcessPct'])}%p | "
          f"{r.get('note') or '-'} |")
    A("")
    wb = d["benchmarks"]["worstCaseBenchmark"]
    A(f"최악 benchmark: **{wb['benchmark']}** → 초과 {sgn(wb['candidateExcessPct'])}%p")
    A(f"({d['benchmarks']['positiveAgainst']}/{d['benchmarks']['total']} benchmark 에서 양수)")
    A("")

    A("## 20. 실제 돈 위험 시트 — 5,000만원 기준")
    A("")
    A("```")
    A(f"기초 투입                     {won(risk['initialCapitalKrw'])}")
    A(f"전체기간 최종 평가액          {won(risk['terminalWealth'])}  ({m_won(risk['terminalWealth'])})")
    A(f"동일조건 시장                 {won(risk['benchTerminal'])}  ({m_won(risk['benchTerminal'])})")
    A("")
    A(f"최대 낙폭(MDD)                {risk['mddPct']}%")
    A(f"최대 낙폭 금액                {won(risk['worstDrawdownKrw'])}  ({risk['worstDrawdownAt']})")
    A(f"최악 1년 수익률               {risk['worst1yPct']}%")
    A(f"  → 5,000만원이 1년 만에      {won(risk['worst1yKrwOn50m'])}")
    A(f"최악 3년(연율)                {risk['worst3yAnnualPct']}%  (누적 {risk['worst3yTotalPct']}%)")
    A(f"최장 수중(원금 회복까지)      {risk['longestUnderwaterMonths']}개월")
    A(f"최장 benchmark 부진 지속      {risk['longestBenchUnderperformMonths']}개월")
    A(f"1년 수익률 마이너스 비율      {risk['roll1yNegSharePct']}%")
    A("")
    A("가장 나쁜 시작점에 들어갔다면 (실측)")
    w1, w3, w5 = (risk["worstStartCohort1y"], risk["worstStartCohort3y"],
                  risk["worstStartCohort5y"])
    A(f"  1년 최악  {w1['start'][:7]} 시작 → {won(w1['accountKrw'])} "
      f"({w1['accountVsContributedPct']}%)")
    A(f"  3년 최악  {w3['start'][:7]} 시작 → {won(w3['accountKrw'])} "
      f"({w3['accountVsContributedPct']}%)")
    A(f"  5년 최악  {w5['start'][:7]} 시작 → {won(w5['accountKrw'])} "
      f"({w5['accountVsContributedPct']}%) · 시장은 연 {w5['benchAnnualPct']}%")
    A("```")
    A("")
    A("> 모두 과거 실측치다. **미래 예측이 아니다.** 5,000만원이 3년 뒤 3,359만원,")
    A("> 5년 뒤 3,264만원이 된 시작점이 실제로 데이터 안에 있다.")
    A("")
    A("두 숫자가 왜 다른가 — '최악 1년 −31.09%' 는 전체 경로 안의 이동 1년 창(이미 "
      "완전투자 상태)이고, '1년 최악 코호트 −3.82%' 는 규칙대로 새로 시작한 계좌다. ")
    A("신규 시작 계좌는 첫 12개월 대부분이 현금이라 크게 잃지도 벌지도 못한다. "
      "**둘 다 실제 위험이다** — 진입 후 2년이 지나면 전자의 위험에 노출된다.")
    A("")

    A("## 21. 최종 판정")
    A("")
    A(f"### 사전확정 규칙 적용 결과: **{ver['precommitted']}** ({ver['gatesPassed']})")
    A("")
    A(f"- 통과: {', '.join(k for k, v in ver['gates'].items() if v['pass'])}")
    A(f"- 미달: {', '.join(ver['gatesFailed'])}")
    A("")
    A(f"{ver['reasoning']['why']}")
    A("")
    if ver.get("labelWarning"):
        A(f"> **판정 라벨 경고** — {ver['labelWarning']}")
        A("")
    A(f"### 연구자 판단(사전규칙과 별개): **{ver['researcherReading']['label']}**")
    A("")
    A(f"{ver['researcherReading']['why']}")
    A("")
    A(f"**forward-test 전 필수 확정**: {ver['researcherReading']['beforeAnyForwardTest']}")
    A("")
    A(f"**파라미터 구출 없음**: {ver['researcherReading']['notAParameterRescue']}")
    A("")

    A("## 22. forward-test 설계 (§20 — 설계까지만)")
    A("")
    if not d["forwardTestDesign"]["created"]:
        A(f"작성하지 않음 — {d['forwardTestDesign']['reason']}")
    else:
        f = d["forwardTestDesign"]
        A(f"- 이름 `{f['name']}` · **설계 문서만 작성. 코드·state·scheduler 생성 0.**")
        A(f"- 목적: {f['purpose']}")
        A("")
        A("```")
        A(f"LEGACY_50D        {f['isolationFromLegacy']['legacy']['note']}")
        A(f"NEW_BM_R8_FROZEN  {f['isolationFromLegacy']['new']['canonical']}")
        A(f"  state           {f['isolationFromLegacy']['new']['stateFile']}")
        A(f"  report          {f['isolationFromLegacy']['new']['reportFile']}")
        A(f"  (아직 생성하지 않음 — 경로 예약)")
        A("```")
        A("")
        A("**시작 전 선결조건 (미해결 시 시작 금지)**")
        for x in f["blockingPrerequisites"]:
            A(f"- {x}")
        A("")
        A("**프로토콜**")
        A("```")
        for k, v in f["protocol"].items():
            if isinstance(v, list):
                A(f"{k:24} {', '.join(v)}")
            else:
                A(f"{k:24} {v}")
        A("```")
        A("")
        A("**반증 기준 (kill criteria)**")
        for x in f["killCriteria"]:
            A(f"- {x}")
        A("")
        A("**Founder 승인 없이 금지**")
        for x in f["notPermittedWithoutFounderApproval"]:
            A(f"- {x}")
        A("")

    A("## 23. 자체감사 (§23)")
    A("")
    A("| 항목 | 결과 | 내용 |")
    A("|---|---|---|")
    for a in d["selfAudit"]:
        A(f"| {a['item']} | **{a['result']}** | {a['detail']} |")
    A("")

    A("## 24. production 안전 (§25)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:20} {v}")
    A("```")
    A("")

    A("## 25. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")

    A("## 26. 홈페이지 (§21)")
    A("")
    A("- 이번 작업 홈페이지 변경 **0**.")
    A("- **BM 13.44% 를 public marketing 숫자로 사용 금지.** R9 결과가 그 숫자를 뒷받침하지")
    A("  않는다(시작연도 의존·상위 5종목 71%·결측제외 효과 2.88%p).")
    A("")

    A("## 27. 다음 단일 작업")
    A("")
    A(f"{d['nextDecision']['single']}")
    A("")
    A("지금 하지 않는 것: " + " · ".join(d["nextDecision"]["notNow"]))
    A("")
    A(f"후속 연구 질문(제안만): {d['nextDecision']['followUpResearchQuestionOnly']}")
    A("")
    return "\n".join(o)


def main() -> int:
    WD.mkdir(parents=True, exist_ok=True)
    d = build()
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"), "json": str(WD / f"{NAME}.json"),
                      "precommittedVerdict": d["finalVerdict"]["precommitted"],
                      "researcherReading": d["finalVerdict"]["researcherReading"]["label"],
                      "gates": d["finalVerdict"]["gatesPassed"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
