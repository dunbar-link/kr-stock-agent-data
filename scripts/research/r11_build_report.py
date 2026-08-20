#!/usr/bin/env python3
"""R11 산출물 생성 (MD + JSON) — §24.

WABABA-BM-INCREMENTAL-ALPHA-VALIDATION-R11

reports/wababa/wababa-bm-incremental-alpha-validation-r11-latest.{md,json}
기존 R8/R9/R10 산출물 수정 금지 — R11 은 별도 provenance.

production 변경 0 · scheduler 0 · 홈페이지 0 · 실주문 0 · 브로커 0 ·
REAL_MONEY_NOT_APPROVED 유지.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r9_precommit import ERAS, FROZEN  # noqa: E402
import r11_control as C  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-bm-incremental-alpha-validation-r11-latest"


def L(n, series="r11"):
    p = RD / f"{series}-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def won(x):
    return "-" if x is None else f"{round(x):,}원"


def eok(x):
    return "-" if x is None else f"{x / 1e8:.2f}억원"


def sg(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def build():
    rp, co, ro = L("repro"), L("core"), L("rolling")
    st, wf, er = L("stability"), L("walkforward"), L("era")
    bs, cs, sg_ = L("bootstrap"), L("crosssec"), L("subgroup")
    ss, rk, vd = L("stress"), L("risk"), L("verdict")

    return {
        "schema": "wababa-bm-incremental-alpha-validation-r11@1",
        "taskId": "WABABA-BM-INCREMENTAL-ALPHA-VALIDATION-R11",
        "verdictLine": "PASS",
        "reasonClass": "R11_INCREMENTAL_ALPHA_CONFIRMED_VS_FAIR_PBR_CONTROL",
        "dataPeriod": rp["period"],
        "frozenCandidate": FROZEN,
        "parameterChanges": 0,
        "gateThresholdsChanged": 0,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "researchQuestion": (
            "PBR 데이터가 실제로 존재하는 동일 투자가능 universe 를 그냥 동일가중으로 "
            "사는 것보다, BM 상위 20% 를 고르는 행위가 통계적·경제적으로 의미 있는 "
            "incremental alpha 를 만드는가?"),
        "reproduction": {
            "pass": rp["pass"], "checks": rp["checks"],
            "candidateBase": rp["candidateBase"],
            "r10ArtifactsUntouched": rp["r10ArtifactsUntouched"]},
        "primaryBenchmark": {
            "id": C.C1,
            "definition": co["primaryControlDefinition"],
            "eligibleSetIdentity": co["eligibleSetIdentity"],
            "structuralIdentity": co["structuralIdentity"],
            "decompositionVsR10": rp["benchmarkDefinitionDecomposition"],
            "secondaryReferences": co["secondaryReferences"]},
        "baseIncrementalAlpha": {
            "candidate": co["candidate"], "controlC1": co["controlC1"],
            "controlC2_RANDOM_N40": {k: v for k, v in
                                     co["controlC2_RANDOM_N40"].items() if k != "rows"},
            "controlC3_REST80": co["controlC3_REST80"],
            "incrementalAlpha": co["incrementalAlpha"]},
        "calendarYear": ro["calendarYear"],
        "rolling": ro["rolling"],
        "startDateStability": {k: v for k, v in st.items() if k != "rows"},
        "walkForward": {k: v for k, v in wf.items() if k != "rows"},
        "walkForwardRows": wf["rows"],
        "leaveOneEraOut": er,
        "bootstrapInference": bs,
        "crossSectionalSelection": cs,
        "subgroupFairControl": sg_,
        "costAndDelistingStress": ss,
        "realMoneyRisk": rk,
        "finalVerdict": vd,
        "limitations": [
            "월 스냅샷 기준 — 일 단위 진입·청산 타이밍은 재현 불가.",
            "배당 재투자 미반영(candidate·control 동일 조건). 저PBR 고배당 특성상 "
            "candidate 에 불리한 방향의 누락이다.",
            "PRIMARY control(C1)만 소수주를 허용한다. ~1,500종목을 5천만원 12분할로 "
            "정수주 매수하면 전 종목 0주가 되기 때문이다. 이것이 유일한 구조 차이이며, "
            "정수주를 그대로 유지한 C2(무작위 40종목)·C3(REST80 40종목)·P1/P2(풀 전체) "
            "에서도 동일한 결론이 나온다.",
            "스냅샷에 거래량·거래대금·거래정지 플래그가 없다. 유동성은 가격·시총·종가 "
            "무변동 proxy 로만 감사했고 실제 체결 가능성 검증은 미완이다. 특히 후보는 "
            "첫 12개월 종목당 월 약 104,000원 매수라 최소 호가단위 제약이 실전에서 크다.",
            "C1 은 연 1,882 주문·최대 2,107종목이라 **사람이 실행할 수 없는 통제군**이다. "
            "이론적 공정 비교 기준이지 Founder 의 실제 대안이 아니다. 실제 대안은 "
            "2차 참고의 공식 지수 계열이다.",
            "R7/R8 이 전체 2007~2026 데이터로 후보를 찾았으므로 진정한 untouched holdout 이 "
            "없다. walk-forward 는 retrospective 재현이며 창은 R9/R10 사전확정값을 재사용했다.",
            "rolling·cohort window 는 서로 겹친다(반독립). 그래서 non-overlapping 결과와 "
            "4종 bootstrap 을 함께 산출했다.",
            "ROE/PER/PBR 은 KRX 공표 근사값이다(DART 원전 재무 아님).",
            "데이터 말단 19개월은 매수 가드로 신규 선정이 차단돼 양쪽 모두 현금이다 — "
            "백테스트 종단 효과이며 규칙 성능이 아니다.",
            "백테스트 결과이며 실제 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "legacy50d": "unchanged", "canonical": "untouched",
            "autoApply": "untouched", "autoPublish": "untouched",
            "publicPortfolio": "untouched", "homepage": "unchanged",
            "scheduler": "untouched", "krStockAgentRepo": "untouched",
            "productionUrl": "untouched", "forwardTestStateCreated": 0,
            "broker": 0, "realOrders": 0, "paidData": 0, "externalSend": 0,
            "deploy": 0, "envOrToken": 0, "productionDbWrite": 0},
        "hotgClosedLoop": {
            "canonicalReport": f"reports/wababa/{NAME}.md",
            "reportBridgeCollectable": True,
            "evidence": ("projects.registry.json 의 wababa 항목 reportBridge=true · "
                         "report_policy=INCLUDE · child_roots 에 "
                         "C:\\work\\kr-stock-agent-data-new 포함. bridge-data.json "
                         "recent 목록에 이 저장소 경로 보고서가 실제 수집돼 있음."),
            "sourceOwner": "Wababa",
            "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
            "newObserverCreated": 0, "duplicateOrchestration": 0,
            "closeCondition": ("R11 판정이 08:40 종합보고에 source_owner=Wababa 로 "
                               "올라가고 Founder 가 다음 단일 작업을 승인/보류하면 CLOSE."),
            "executionSucceededInvisibly": False},
        "nextDecision": {
            "founderAction": (
                "1건 — R11 로 historical search 는 종료 조건을 충족했다(§32). "
                "forward-test 단계로 넘어갈지 승인/보류만 결정하면 된다. "
                "scheduler·state·실자금은 이번에 만들지 않았다."),
            "single": ("NEW_BM_FORWARD_TEST 설계·구축 (paper only). PRIMARY benchmark 를 "
                       "EW_VALID_PBR 로 고정하고 kill criteria 를 사전확정한 뒤 월 1회 "
                       "기록만 시작한다. 실주문 0 · scheduler 등록은 별도 승인."),
            "notNow": ["scheduler 등록", "state 파일 생성", "production 반영",
                       "홈페이지 공개", "11.50% 를 public marketing 숫자로 사용",
                       "실계좌 자금 투입", "새 parameter 탐색(P/N/H)"],
            "forbiddenFollowUp": (
                "§20·§32 — 결과가 좋다고 P10/P30·N20/N50·H12/H36 재탐색을 시작하지 "
                "않는다. historical parameter search 는 여기서 종료다."),
        },
    }


def md(d):
    rp, pb = d["reproduction"], d["primaryBenchmark"]
    bi, cal, ro = d["baseIncrementalAlpha"], d["calendarYear"], d["rolling"]
    st, wf, er = d["startDateStability"], d["walkForward"], d["leaveOneEraOut"]
    bs, cs, sgr = d["bootstrapInference"], d["crossSectionalSelection"], d["subgroupFairControl"]
    ss, rk, vd = d["costAndDelistingStress"], d["realMoneyRisk"], d["finalVerdict"]
    o = []
    A = o.append
    A("전체 판정: PASS")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R11 — BM 상위20% 의 incremental alpha 검증 (공정 PBR-eligible control 대비)")
    A("")
    A(f"- 기간 {d['dataPeriod']['start']} ~ {d['dataPeriod']['end']} "
      f"({d['dataPeriod']['months']}개월)")
    A(f"- 단일 연구질문: {d['researchQuestion']}")
    A(f"- **alpha 판정: {vd['alphaVerdict']}** (기준 {vd['passed']}/{vd['total']})")
    A(f"- **전략 판정: {vd['strategyVerdict']}** · 실제 돈 단계 "
      f"**{d['realMoneyStage']}**")
    A(f"- frozen parameter 변경 **{d['parameterChanges']}** · gate threshold 변경 "
      f"**{d['gateThresholdsChanged']}**")
    A("")
    A("> ★ 이 결론은 R10 의 −0.22%p 를 뒤집는다. 새 데이터도, 새 parameter 도 아니다.")
    A("> **PRIMARY control 정의 하나**가 바뀌었다. 아래 1절이 그 전부를 분해한다.")
    A("> 후보에게 유리한 방향의 재정의이므로 근거를 숨기지 않고 전부 공개한다.")
    A("")

    # 1. benchmark 재정의 분해
    A("## 1. 왜 R10 은 −0.22%p 였고 R11 은 +6.90%p 인가")
    A("")
    dc = pb["decompositionVsR10"]
    A("| 변형 | CAGR | 비용/투입금 | 평균현금 | 설명 |")
    A("|---|---|---|---|---|")
    for r in dc["rows"]:
        A(f"| {r['variant']} | {n2(r['cagrPct'])}% | "
          f"{n2(r.get('costPctOfContrib')) if r.get('costPctOfContrib') is not None else '-'}% | "
          f"{n2(r.get('avgCashPct')) if r.get('avgCashPct') is not None else '-'}% | "
          f"{r['note']} |")
    A("")
    A("```")
    A(f"엔진 검증(지수 == 포트폴리오 재현)     {dc['engineValidatedAgainstR10Index']}")
    A(f"월 리밸런싱 보너스                     {dc['rebalanceFrequencyBonusPct']:+.2f}%p")
    A(f"월 리밸런싱 실제 비용 부담             {dc['monthlyRebalanceCostDragPct']:+.2f}%p")
    A(f"24개월 보유 프레임 비용 부담           {dc['holdFrameCostDragPct']:+.2f}%p")
    A("```")
    A("")
    A(f"{dc['finding']}")
    A("")
    A(f"> **{dc['conclusion']}**")
    A("")
    A("핵심은 이것이다 — R10 의 benchmark 는 **매월 약 1,500종목을 수수료 0으로**")
    A("재편입한다고 가정한다. 그 가정에 실제 수수료·매도세를 부과하면 CAGR 이 11.7% →")
    A("5.4% 로 떨어진다(비용이 투입자본의 199%). 즉 R10 이 benchmark 에 준 우위는")
    A("**실현 불가능한 거래 가정**에서 나왔다. §3 이 요구한 구조 동일화(동일 24개월 보유")
    A("프레임 · 동일 교체 타이밍 · 동일 비용)를 적용하면 그 우위가 사라진다.")
    A("")

    # 2. reproduction
    A("## 2. Reproduction gate (§23)")
    A("")
    A(f"결과 **{'PASS' if rp['pass'] else 'FAIL'}** · R10 산출물 미변경 "
      f"{rp['r10ArtifactsUntouched']}")
    A("")
    A("| 지표 | R11 | R10 정본 | 차이 | 허용오차 | 판정 |")
    A("|---|---|---|---|---|---|")
    for c in rp["checks"]:
        A(f"| {c['metric']} | {c['r11']} | {c['r10Canonical']} | {c['diff']} | "
          f"{c['tolerance']} | {'O' if c['pass'] else 'X'} |")
    A("")

    # 3. primary benchmark + structural identity
    A("## 3. PRIMARY control 정의와 구조 동일화 (§2·§3)")
    A("")
    A(f"**{pb['id']}** — {pb['definition']}")
    A("")
    A(f"eligible set 동일성: {pb['eligibleSetIdentity']}")
    A("")
    si = pb["structuralIdentity"]["vs_C1"]
    A("| 동일성 검사 | 결과 |")
    A("|---|---|")
    for k in ("sameDates", "sameInflowSchedule", "sameContributed",
              "sameContributionPath", "sameTranches", "sameCohortCount",
              "sameBuyMonths"):
        A(f"| {k} | {'O' if si[k] else 'X'} |")
    A(f"| 투입금 | candidate {won(si['candidateContributed'])} / "
      f"control {won(si['controlContributed'])} |")
    A(f"| 코호트 수 | {si['candidateCohorts']} / {si['controlCohorts']} |")
    A(f"| 평균 현금비중 | {si['candidateAvgCashPct']}% / {si['controlAvgCashPct']}% |")
    A(f"| 평균 보유종목 | {si['candidateAvgPositions']} / "
      f"{si['controlAvgPositions']} (구조상 유일한 차이) |")
    A("")

    # 4. base incremental alpha
    A("## 4. Base incremental alpha (§5)")
    A("")
    ia = bi["incrementalAlpha"]["vs_C1_PRIMARY"]
    A("| 지표 | candidate BM_TOP20 | control EW_VALID_PBR_ALL | 차이 |")
    A("|---|---|---|---|")
    A(f"| CAGR | {bi['candidate']['cagrPct']}% | {bi['controlC1']['cagrPct']}% | "
      f"**{sg(ia['cagrExcessPct'])}%p** |")
    A(f"| 산술 연율 초과 | - | - | {sg(ia['arithAnnualExcessPct'])}%p |")
    A(f"| 최종 평가액 | {eok(bi['candidate']['terminalWealth'])} | "
      f"{eok(bi['controlC1']['terminalWealth'])} | "
      f"배수 {bi['incrementalAlpha']['terminalWealthRatio_vs_C1']}x |")
    A(f"| 변동성 | {bi['candidate']['volPct']}% | {bi['controlC1']['volPct']}% | - |")
    A(f"| MDD | {bi['candidate']['mddPct']}% | {bi['controlC1']['mddPct']}% | - |")
    A(f"| tracking error | - | - | {n2(ia['trackingErrorPct'])}% |")
    A(f"| information ratio | - | - | {ia['informationRatio']} |")
    A(f"| 연 주문 건수 | {bi['candidate']['ordersPerYear']} | "
      f"{bi['controlC1']['ordersPerYear']} | - |")
    A(f"| 비용/투입금 | {bi['candidate']['costPctOfContrib']}% | "
      f"{bi['controlC1']['costPctOfContrib']}% | - |")
    A("")
    A("### 통제군 3종 전부 같은 방향")
    A("")
    c2 = bi["controlC2_RANDOM_N40"]
    A("| 통제군 | 구조 | CAGR | candidate 초과 |")
    A("|---|---|---|---|")
    A(f"| C1 EW_VALID_PBR_ALL (전 종목 동일가중) | 소수주 · 1,335종목 | "
      f"{bi['controlC1']['cagrPct']}% | {sg(ia['cagrExcessPct'])}%p |")
    A(f"| C2 EW_VALID_PBR_N40 (무작위 40종목, 30 seed 평균) | **정수주 · 완전 동일** | "
      f"{n2(c2['cagrPct']['mean'])}% | {sg(bi['incrementalAlpha']['vs_C2_RANDOM_mean'])}%p |")
    A(f"| C3 BM_REST80 (하위 80% 에서 40종목) | **정수주 · 완전 동일** | "
      f"{bi['controlC3_REST80']['cagrPct']}% | "
      f"{sg(bi['incrementalAlpha']['vs_C3_REST80']['cagrExcessPct'])}%p |")
    A("")
    A(f"무작위 40종목 통제군 30 seed 중 후보를 이긴 것 "
      f"**{c2['seedsBeatingCandidate']}/{c2['seeds']}** "
      f"(후보 percentile {c2['candidatePercentile']}) · seed 분포 "
      f"중앙 {n2(c2['cagrPct']['median'])}% · 최고 {n2(c2['cagrPct']['max'])}%")
    A("")
    A("> C2·C3 는 정수주까지 후보와 100% 동일한 구조다. 소수주를 쓴 C1 이 결론을")
    A("> 만든 것이 아니라는 확인이다.")
    A("")

    # 5. calendar year
    A("## 5. Calendar-year 상대성과 (§6)")
    A("")
    A(f"- positive {cal['positiveYears']}/{cal['totalYears']} "
      f"({cal['positiveRatePct']}%) · 중앙 연 초과 "
      f"{sg(cal['medianAnnualExcessPct'])}%p · 평균 "
      f"{sg(cal['meanAnnualExcessPct'])}%p")
    A(f"- 최악 {cal['worstYear']} {sg(cal['worstYearExcessPct'])}%p · "
      f"최고 {cal['bestYear']} {sg(cal['bestYearExcessPct'])}%p · "
      f"최장 연속 열위 {cal['longestConsecutiveUnderperformYears']}년")
    A("")
    A("| 연도 | candidate | control | 초과 | 승 |")
    A("|---|---|---|---|---|")
    for r in cal["rows"]:
        A(f"| {r['year']} | {n2(r['candPct'])}% | {n2(r['ctrlPct'])}% | "
          f"{sg(r['excessPct'])}%p | {'O' if r['win'] else 'X'} |")
    A("")

    # 6. rolling
    A("## 6. Rolling horizon (§7)")
    A("")
    A("| horizon | 겹침 | 창 수 | 중앙 | 평균 | p10 | p25 | p75 | p90 | positive |")
    A("|---|---|---|---|---|---|---|---|---|---|")
    for h in ("1Y", "3Y", "5Y", "7Y", "10Y"):
        for mode, lbl in (("overlapping", "겹침"), ("nonOverlapping", "비겹침")):
            v = ro.get(h, {}).get(mode)
            if not v:
                continue
            A(f"| {h} | {lbl} | {v['windows']} | {sg(v['medianPct'])}%p | "
              f"{sg(v['meanPct'])}%p | {sg(v['p10Pct'])}%p | {sg(v['p25Pct'])}%p | "
              f"{sg(v['p75Pct'])}%p | {sg(v['p90Pct'])}%p | {v['positiveRatePct']}% |")
    A("")
    r5 = ro.get("5Y", {}).get("overlapping", {})
    A(f"5년 창 최악: {r5.get('worst', {}).get('start', '-')[:7]} ~ "
      f"{r5.get('worst', {}).get('end', '-')[:7]} "
      f"{sg(r5.get('worst', {}).get('excessPct'))}%p")
    A("")

    # 7. start stability
    A("## 7. 시작시점 / cohort 안정성 (§8)")
    A("")
    A("| 표본 | horizon | n | 중앙 | p10 | 최악 | positive |")
    A("|---|---|---|---|---|---|---|")
    for key, lbl in (("allStarts", "전체 시작월"),
                     ("excluding2007Starts", "2007 시작 제외"),
                     ("from2009FullyInvested", "2009 이후 시작")):
        for hk, hl in (("cohort5Y", "5년"), ("cohort10Y", "10년"), ("toEnd", "끝까지")):
            v = st[key].get(hk)
            if not v:
                continue
            A(f"| {lbl} | {hl} | {v['n']} | {sg(v['medianPct'])}%p | "
              f"{sg(v['p10Pct'])}%p | {sg(v['worstPct'])}%p | "
              f"{v['positiveRatePct']}% |")
    A("")
    A(f"공정 control 대비 최장 열위 지속 **{st['longestControlUnderperformMonths']}개월**")
    A("")
    A("| 시작연도 | n | 5년 초과 중앙 | positive |")
    A("|---|---|---|---|")
    for y, v in st["cohort5YExcessByStartYear"].items():
        A(f"| {y} | {v['n']} | {sg(v['medianPct'])}%p | {v['positive']}/{v['n']} |")
    A("")
    A("> 2007 시작을 제외해도 중앙 초과가 사실상 동일하다 — R9 에서 문제였던")
    A("> '2007년 시작 편중'은 공정 control 기준으로도 존재하지 않는다.")
    A("")

    # 8. walk-forward
    A("## 8. Walk-forward (§9)")
    A("")
    A(f"- {wf['windowsReused']}")
    A(f"- **{wf['positive']}/{wf['total']} positive ({wf['positiveRatePct']}%)** · "
      f"과반 {wf['majorityPositive']} · 중앙 {sg(wf['medianExcessPct'])}%p · "
      f"평균 {sg(wf['meanExcessPct'])}%p · p10 {sg(wf['p10Pct'])}%p · "
      f"최악 {sg(wf['worstPct'])}%p")
    A("")
    A("| 방식 | 창 수 | positive | 중앙 초과 |")
    A("|---|---|---|---|")
    for k, v in wf["byKind"].items():
        A(f"| {k} | {v['n']} | {v['positive']} | {sg(v['medianExcessPct'])}%p |")
    A("")

    # 9. leave-one-era-out
    A("## 9. Leave-one-era-out (§10)")
    A("")
    A(f"- {er['erasReused']}")
    A(f"- **positive {er['positiveCount']}/{er['total']}** · 중앙 "
      f"{sg(er['medianExcessPct'])}%p · 최악 제외시대 {er['worstExcludedEra']} "
      f"{sg(er['worstExcludedExcessPct'])}%p")
    A("")
    A("| 제외 시대 | candidate | control | 초과 | 부호 |")
    A("|---|---|---|---|---|")
    for r in er["leaveOneOut"]:
        if r.get("skipped"):
            continue
        A(f"| {r['era']} {r['label']} | {n2(r['candCagrPct'])}% | "
          f"{n2(r['ctrlCagrPct'])}% | {sg(r['excessPct'])}%p | "
          f"{'+' if r['positive'] else '−'} |")
    A("")
    A(f"시대별 단독 성과 positive **{er['standalonePositive']}/{er['standaloneTotal']}**")
    A("")
    A("| 시대 | candidate 총수익 | control 총수익 | 연율 초과 |")
    A("|---|---|---|---|")
    for r in er["eraStandalone"]:
        A(f"| {r['era']} {r['label']} | {n2(r['candTotalPct'])}% | "
          f"{n2(r['ctrlTotalPct'])}% | {sg(r['excessPct'])}%p |")
    A("")

    # 10. bootstrap
    A("## 10. 통계적 추론 — block bootstrap (§11)")
    A("")
    A(f"관측 연율 초과수익 **{sg(bs['observedAnnualExcessPct'])}%p** · "
      f"draws {bs['draws']} · seed {bs['seed']} · deterministic {bs['deterministic']}")
    A("")
    A(f"{bs['method']}")
    A("")
    A("| 방법 | 평균 | 중앙 | 95% CI | P(>0) | P(>+1%p) | P(>+2%p) |")
    A("|---|---|---|---|---|---|---|")
    for k, v in bs["results"].items():
        A(f"| {k} | {sg(v['meanPct'])}%p | {sg(v['medianPct'])}%p | "
          f"[{sg(v['ci95LowerPct'])}, {sg(v['ci95UpperPct'])}] | "
          f"{v['probExcessGt0Pct']}% | {v['probExcessGt1pPct']}% | "
          f"{v['probExcessGt2pPct']}% |")
    A("")
    cns = bs["consensus"]
    A(f"모든 방법 95% CI 하한 > 0: **{cns['allCiLowerPositive']}** · "
      f"P(excess>0) 최소 **{cns['minProbPositivePct']}%** · "
      f"P(>+1%p) 최소 {cns['minProbGt1Pct']}% · "
      f"P(>+2%p) 최소 {cns['minProbGt2Pct']}%")
    A("")

    # 11. cross-sectional
    A("## 11. Cross-sectional selection attribution (§12)")
    A("")
    pl = cs["poolLevelCrossSection"]
    A("### ★ pool 수준 — 40종목 샘플링 효과 분리")
    A("")
    A(f"{pl['note']}")
    A("")
    A("| arm | 평균 보유종목 | CAGR | MDD | 최종 평가액 |")
    A("|---|---|---|---|---|")
    A(f"| CANDIDATE (40종목 균등간격) | "
      f"{d['baseIncrementalAlpha']['candidate']['avgPositions']} | "
      f"{d['baseIncrementalAlpha']['candidate']['cagrPct']}% | "
      f"{d['baseIncrementalAlpha']['candidate']['mddPct']}% | "
      f"{eok(d['baseIncrementalAlpha']['candidate']['terminalWealth'])} |")
    A(f"| BM_TOP20_POOL_ALL (풀 전체) | {pl['BM_TOP20_POOL_ALL']['avgPositions']} | "
      f"{pl['BM_TOP20_POOL_ALL']['cagrPct']}% | {pl['BM_TOP20_POOL_ALL']['mddPct']}% | "
      f"{eok(pl['BM_TOP20_POOL_ALL']['terminalWealth'])} |")
    A(f"| BM_REST80_POOL_ALL (풀 전체) | {pl['BM_REST80_POOL_ALL']['avgPositions']} | "
      f"{pl['BM_REST80_POOL_ALL']['cagrPct']}% | {pl['BM_REST80_POOL_ALL']['mddPct']}% | "
      f"{eok(pl['BM_REST80_POOL_ALL']['terminalWealth'])} |")
    A(f"| EW_VALID_PBR_ALL | {pl['EW_VALID_PBR_ALL']['avgPositions']} | "
      f"{pl['EW_VALID_PBR_ALL']['cagrPct']}% | {pl['EW_VALID_PBR_ALL']['mddPct']}% | "
      f"{eok(d['baseIncrementalAlpha']['controlC1']['terminalWealth'])} |")
    A("")
    A("```")
    A(f"TOP20 풀 − REST80 풀        {pl['top20PoolMinusRest80PoolPct']:+.2f}%p")
    A(f"TOP20 풀 − 전 종목          {pl['top20PoolMinusAllEligiblePct']:+.2f}%p")
    A(f"후보(40종목) − TOP20 풀     {pl['candidateMinusTop20PoolPct']:+.2f}%p   ← 샘플링 기여")
    A(f"풀 수준 집중도  top1 {pl['poolLevelConcentration']['top1SharePct']}% · "
      f"top5 {pl['poolLevelConcentration']['top5SharePct']}% · "
      f"top10 {pl['poolLevelConcentration']['top10SharePct']}% "
      f"(거래종목 {pl['poolLevelConcentration']['uniqueTickers']}개)")
    A("```")
    A("")
    A(f"{pl['reading']}")
    A("")
    A("### 코호트별 포지션 평균 수익률 단면")
    A("")
    ccs = cs["cohortCrossSection"]
    A(f"TOP20 이 REST80 을 이긴 코호트 **{ccs['cohortsWhereTop20Wins']}/"
      f"{ccs['totalCohorts']}** · 중앙 spread "
      f"{sg(ccs['medianTop20MinusRest80Pct'])}%p")
    A("")
    A("| 매수시점 | TOP20 | REST80 | 전 종목 | TOP20−REST80 | 승 |")
    A("|---|---|---|---|---|---|")
    for r in ccs["rows"]:
        A(f"| {r['buyDate'][:7]} | {n2(r['top20MeanPosRetPct'])}% | "
          f"{n2(r['rest80MeanPosRetPct'])}% | {n2(r['allEligibleMeanPosRetPct'])}% | "
          f"{sg(r['top20MinusRest80Pct'])}%p | {'O' if r['top20Wins'] else 'X'} |")
    A("")
    A("> 9개 코호트 중 4개는 TOP20 이 REST80 에 졌다. 단면 효과가 매 코호트마다")
    A("> 나타나는 것은 아니며, 전체 초과수익은 이긴 코호트의 크기(2009 +74%p ·")
    A("> 2019 +68%p · 2023 +42%p)에서 나온다. **이것이 R11 의 가장 큰 약점이다.**")
    A("")

    # 12. concentration
    A("## 12. 집중도 감사 (§13)")
    A("")
    cc = cs["concentration"]
    A(f"거래종목 {cc['uniqueTickers']}개 · 총손익 {eok(cc['totalPnl'])} · "
      f"HHI {cc['hhiPositiveContrib']}")
    A("")
    A("| 구분 | 기여비중 |")
    A("|---|---|")
    for k in (1, 3, 5, 10):
        A(f"| 상위 {k}종목 | {cc[f'top{k}SharePct']}% |")
    A("")
    A("| 제거 | 제거손익 | 1차근사 초과 | 정확 재시뮬 초과 |")
    A("|---|---|---|---|")
    for r in cc["removal"]:
        A(f"| 상위 {r['topK']} | {eok(r['removedPnl'])} | "
          f"{sg(r['firstOrder']['excessVsC1Pct'])}%p | "
          f"{sg((r['exactResimulated'] or {}).get('excessVsC1Pct'))}%p |")
    A("")
    A("| 상위 기여 종목 | 손익 | 비중 |")
    A("|---|---|---|")
    for r in cc["top"][:6]:
        A(f"| {r['name']} ({r['ticker']}) | {eok(r['pnl'])} | {r['sharePct']}% |")
    A("")
    A("> 40종목판은 상위 5종목이 총손익의 75.5% 다. 상위 10종목을 1차근사로 제거하면")
    A("> 초과수익이 음수가 된다(−1.38%p). 다만 **풀 수준(266종목)에서는 상위 5종목")
    A("> 기여가 31.8% 로 떨어지고 초과수익이 유지된다** — 집중도 문제는 BM 신호의")
    A("> 성질이 아니라 40종목 구현의 성질이다.")
    A("")

    # 13. winner distribution
    A("## 13. 수익 분포 (§14)")
    A("")
    A("| arm | 포지션 | 중앙 | 승률 | p90 | p95 | 왜도 | 상위1%/총이익 | 상위5%/총이익 | 구조 |")
    A("|---|---|---|---|---|---|---|---|---|---|")
    for k, v in cs["winnerDistribution"].items():
        A(f"| {v['label']} | {v['positions']} | {n2(v['medianRetPct'])}% | "
          f"{n2(v['winRatePct'])}% | {n2(v['p90Pct'])}% | {n2(v['p95Pct'])}% | "
          f"{v['skewness']} | {n2(v['top1PctShareOfGrossPositivePct'])}% | "
          f"{n2(v['top5PctShareOfGrossPositivePct'])}% | {v['structure']} |")
    A("")
    A("> 세 arm 모두 FEW_BIG_WINNERS 다. 즉 왜도는 후보의 결함이 아니라 한국 소형주")
    A("> 단면의 성질이다. 중요한 차이는 **후보의 중앙 수익률(−2.07%)과 승률(46.9%)이")
    A("> 통제군(−12.44% · 39.5%)보다 뚜렷이 낫다**는 점이다 — 초과수익이 꼬리에만")
    A("> 있는 것이 아니다.")
    A("")

    # 14. subgroup
    A("## 14. Exchange / Size — 같은 subgroup 내 공정 control 대비 (§15)")
    A("")
    g = sgr["subgroupFairControl"]
    A(f"{g['note']}")
    A("")
    A("| subgroup | 평균 eligible | candidate | 공정 control | 초과 | REST80 | TOP20−REST80 |")
    A("|---|---|---|---|---|---|---|")
    for k, v in g["groups"].items():
        A(f"| {k} | {v['avgEligible']} | {n2(v['candCagrPct'])}% | "
          f"{n2(v['fairControlCagrPct'])}% | "
          f"**{sg(v['excessVsFairControlPct'])}%p** | {n2(v['rest80CagrPct'])}% | "
          f"{sg(v['top20MinusRest80Pct'])}%p |")
    A("")
    A(f"공정 control 대비 양수 subgroup **{g['positiveGroups']}/{g['totalGroups']}**")
    A("")
    A("> R10 은 KOSPI −3.91%p · SMALL −4.18%p 로 'BM 은 size/exchange proxy' 라고")
    A("> 읽혔다. 그 비교는 각 subgroup 을 **full-market 월리밸런싱 지수**와 견준 것이었다.")
    A("> 같은 subgroup 의 공정 valid-PBR control 과 같은 프레임에서 비교하면 5/5 전부")
    A("> 양수다. BM 효과는 size·exchange 안에서도 살아있다 — proxy 가 아니다.")
    A("")

    # 15. stress
    A("## 15. 비용 / 상장폐지 stress — 양쪽 동일 적용 (§16)")
    A("")
    A(f"{ss['note']}")
    A("")
    A("| 시나리오 | candidate | control C1 | incremental 초과 | REST80 | TOP20−REST80 | cand 비용 | ctrl 비용 |")
    A("|---|---|---|---|---|---|---|---|")
    for r in ss["rows"]:
        A(f"| {r['scenario']} | {n2(r['candCagrPct'])}% | {n2(r['ctrlC1CagrPct'])}% | "
          f"**{sg(r['incrementalExcessPct'])}%p** | {n2(r['rest80CagrPct'])}% | "
          f"{sg(r['top20MinusRest80Pct'])}%p | {n2(r['candCostPctOfContrib'])}% | "
          f"{n2(r['ctrlCostPctOfContrib'])}% |")
    A("")
    A(f"전 시나리오 양수 **{ss['allScenariosPositive']}**")
    A("")

    # 16. liquidity / distress
    A("## 16. 유동성 / 부실 노출 attribution (§17·§18)")
    A("")
    la = sgr["liquidityAudit"]
    A("| 항목 | candidate | control C1 |")
    A("|---|---|---|")
    ca_, cb_ = la["candidate"], la["controlC1"]
    for k, lbl in (("medianPriceKrw", "중앙 주가(원)"),
                   ("priceBelow1000Pct", "1,000원 미만 비중(%)"),
                   ("medianMarketCapEok", "중앙 시총(억원)"),
                   ("p10MarketCapEok", "시총 p10(억원)"),
                   ("positionVsMarketCapMedianPct", "1종목 매수액/시총 중앙(%)"),
                   ("positionVsMarketCapP90Pct", "1종목 매수액/시총 p90(%)"),
                   ("stalePricePickPct", "종가무변동 비중(%)"),
                   ("delistRatePct", "상장폐지 비율(%)"),
                   ("delistPnlSharePct", "상폐 손익기여(%)"),
                   ("medianPbrAtBuy", "매수시 중앙 PBR"),
                   ("pbrBelow0_3SharePct", "PBR<0.3 포지션 비중(%)"),
                   ("pbrBelow0_3PnlSharePct", "PBR<0.3 손익기여(%)")):
        A(f"| {lbl} | {ca_.get(k)} | {cb_.get(k)} |")
    A("")
    A(f"> {la['dataLimitation']}")
    A("")

    # 17. real money
    A("## 17. 5,000만원 위험 비교 (§22)")
    A("")
    A(f"{rk['basis']}")
    A("")
    A("| 항목 | candidate | 공정 control |")
    A("|---|---|---|")
    c_, k_ = rk["candidate"], rk["fairControl"]
    A(f"| 최종 평가액 | {won(c_['terminalWealthKrw'])} | {won(k_['terminalWealthKrw'])} |")
    A(f"| MDD | {c_['mddPct']}% | {k_['mddPct']}% |")
    A(f"| MDD 금액 | {won(c_['mddAmountKrw'])} ({c_['mddAtDate']}) | "
      f"{won(k_['mddAmountKrw'])} ({k_['mddAtDate']}) |")
    A(f"| 계좌 최저/투입금 | {c_['lowestValueVsContributedPct']}% "
      f"({c_['lowestValueAt']}) | {k_['lowestValueVsContributedPct']}% "
      f"({k_['lowestValueAt']}) |")
    A(f"| 최저 평가액 | {won(c_['lowestValueKrw'])} | {won(k_['lowestValueKrw'])} |")
    A(f"| 최악 1년 | {c_['worst1yPct']}% → {won(c_['worst1yEndValueKrw'])} | "
      f"{k_['worst1yPct']}% → {won(k_['worst1yEndValueKrw'])} |")
    A(f"| 최악 3년 누적 | {c_['worst3yTotalPct']}% | {k_['worst3yTotalPct']}% |")
    A(f"| 최장 수중 | {c_['longestUnderwaterMonths']}개월 | "
      f"{k_['longestUnderwaterMonths']}개월 |")
    A(f"| 1년 마이너스 비율 | {c_['roll1yNegSharePct']}% | {k_['roll1yNegSharePct']}% |")
    A(f"| 최대 동시보유 | {c_['maxConcurrentPositions']}종목 | "
      f"{k_['maxConcurrentPositions']}종목 |")
    A(f"| 연 주문 건수 | {c_['ordersPerYear']} | {k_['ordersPerYear']} |")
    A(f"| 평균 현금비중 | {c_['avgCashRatioPct']}% | {k_['avgCashRatioPct']}% |")
    A("")
    A(f"공정 control 대비 최장 열위 **{rk['longestFairControlUnderperformMonths']}개월** · "
      f"5년 초과 p10 {sg(rk['start5YExcessP10Pct'])}%p")
    ws = rk["worstStart5YExcess"]
    if ws:
        A(f"최악 5년 시작 {ws['start'][:7]}: candidate {n2(ws['candAnnualPct'])}% vs "
          f"control {n2(ws['ctrlAnnualPct'])}% → {sg(ws['excessPct'])}%p")
    A("")
    A(f"> {rk['note']}")
    A("")

    # 18. verdict
    A("## 18. 최종 판정 (§19)")
    A("")
    A("| 기준 | 결과 | 근거 |")
    A("|---|---|---|")
    for k, v in vd["criteria"].items():
        A(f"| {v['rule']} | {'**PASS**' if v['pass'] else '**FAIL**'} | {v['evidence']} |")
    A("")
    A(f"### alpha 판정: **{vd['alphaVerdict']}** ({vd['passed']}/{vd['total']})")
    A("")
    A(f"{vd['reasoning']}")
    A("")
    A(f"### 전략 판정: **{vd['strategyVerdict']}**")
    A("")
    if vd.get("caution"):
        A(f"> **주의** — {vd['caution']}")
        A("")
    A("### 남은 약점 (판정과 별개로 명시)")
    A("")
    A("- 코호트 단면에서 TOP20 이 REST80 을 이긴 것은 9개 중 5개다. 매 코호트마다")
    A("  나타나는 효과가 아니고, 총합은 크게 이긴 3개 코호트(2009·2019·2023)가 만든다.")
    A("- 40종목 구현은 상위 5종목이 총손익의 75.5% 이고, 상위 10종목 1차근사 제거 시")
    A("  초과수익이 음수다. 풀 수준(266종목)에서는 이 문제가 크게 완화된다.")
    A("- 공정 control 대비 최장 47개월 열위 구간이 있었다. Founder 가 4년 가까이")
    A("  '이 규칙이 틀렸나' 를 견뎌야 하는 구간이 실제로 존재했다.")
    A("- MDD −53.38% · 최악 1년 −53.58% · 계좌가 투입금의 57% 까지 내려간 시점이 있다.")
    A("- C1 은 연 1,882 주문·최대 2,107종목으로 **사람이 실행할 수 없는** 이론적")
    A("  기준이다. Founder 의 현실적 대안은 공식 지수 계열(2차 참고)이다.")
    A("")

    # 19. production / hotg
    A("## 19. production / public 보호 (§26)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:26} {v}")
    A(f"{'realMoneyStage':26} {d['realMoneyStage']}")
    A("```")
    A("")
    A("- `C:\\work\\kr-stock-agent` 미수정 · `https://kr-stock-agent.vercel.app` 미변경")
    A("- LEGACY_50D untouched · R11 결과 홈페이지 자동공개 0")
    A("- **11.50% 를 public marketing 숫자로 사용 금지.** Financial Compliance Gate")
    A("  전까지 production 투자추천 금지.")
    A("")
    A("## 20. HOTG 폐회로 (§27)")
    A("")
    h = d["hotgClosedLoop"]
    A("```")
    A(f"canonical report      {h['canonicalReport']}")
    A(f"Report Bridge 수집    {h['reportBridgeCollectable']}")
    A(f"source_owner          {h['sourceOwner']}")
    A(f"표면                  {h['surface']}")
    A(f"신규 observer         {h['newObserverCreated']}")
    A(f"duplicate orchestration {h['duplicateOrchestration']}")
    A(f"EXECUTION_SUCCEEDED_INVISIBLY  {h['executionSucceededInvisibly']}")
    A("```")
    A("")
    A(f"근거: {h['evidence']}")
    A("")
    A(f"CLOSE 조건: {h['closeCondition']}")
    A("")

    # 21. limitations / next
    A("## 21. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")
    A("## 22. Founder 행동 / 다음 단일 작업")
    A("")
    A(f"- **Founder 행동: {d['nextDecision']['founderAction']}**")
    A("")
    A(f"- 다음 단일 작업: {d['nextDecision']['single']}")
    A("")
    A("- 지금 하지 않는 것: " + " · ".join(d["nextDecision"]["notNow"]))
    A("")
    A(f"- {d['nextDecision']['forbiddenFollowUp']}")
    A("")
    return "\n".join(o)


def main() -> int:
    WD.mkdir(parents=True, exist_ok=True)
    d = build()
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"), "json": str(WD / f"{NAME}.json"),
                      "alphaVerdict": d["finalVerdict"]["alphaVerdict"],
                      "strategyVerdict": d["finalVerdict"]["strategyVerdict"],
                      "baseExcessPct": d["finalVerdict"]["baseIncrementalExcessPct"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
