#!/usr/bin/env python3
"""R16 산출물 생성 (MD + JSON) — §30.

WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16
기존 R5~R15 산출물 덮어쓰기 금지.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-canonical-tsr-research-foundation-reset-r16-latest"


def L(n):
    p = RD / f"r16-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def sg(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def build():
    pc, inv = L("precommit"), L("corporate-action-inventory")
    anc, ano = L("anchor-cases"), L("anomaly-audit")
    cov, leg, vd = L("tsr-coverage"), L("legacy-research-status"), L("foundation-verdict")
    sep = L("classifier-separation")
    return {
        "schema": "wababa-canonical-tsr-research-foundation-reset-r16@1",
        "taskId": "WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16",
        "verdictLine": "BLOCKED",
        "reasonClass": "R16_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_ISSUE_DATA_GAP",
        "question": pc["question"],
        "precommit": {"created": True,
                      "writtenBeforeEngineResults": pc["writtenBeforeEngineResults"],
                      "file": "reports/research/r16-precommit-latest.json",
                      "canonical": pc["canonical"], "dividend": pc["dividend"],
                      "classifier": pc["classifier"], "noAdjustment": pc["noAdjustment"],
                      "delisting": pc["delisting"], "cashflow": pc["cashflow"],
                      "anchor": pc["anchor"], "forbidden": pc["forbidden"]},
        "corporateActionInventory": inv,
        "classifierSeparation": sep,
        "anchorCases": anc,
        "anomalyAudit": ano,
        "tsrCoverage": cov,
        "legacyResearchStatus": leg,
        "foundationVerdict": vd,
        "outputContract": {
            "api": "r16_canonical.CanonicalWealth.get_total_return(ticker, start_date, "
                   "end_date, mode='CANONICAL_TSR')",
            "returns": ["endingWealth", "cumulativeReturn", "cagr", "eventCounts",
                        "dataQualityFlags", "unsupportedEventFlags",
                        "endingShares", "endingCash", "delistingRecovery"],
            "modes": ["CANONICAL_TSR (배당재투자, PRIMARY)", "NO_REINVEST",
                      "PRICE_ONLY_UNSAFE (구 엔진 재현 전용 — 연구 사용 금지)"],
            "fwdReturnAdapter": ("CanonicalWealth.fwd_return(px, i, j, ticker, haircut) — "
                                 "factor_research.fwd_return 과 같은 시그니처. 향후 "
                                 "factor 연구는 이것만 쓴다(§28)."),
            "benchmarkInvariant": pc["benchmarkInvariant"],
            "rawCloseGuard": ("test_r16_validation 이 PRICE_ONLY_UNSAFE 와 CANONICAL_TSR "
                              "이 삼성전자에서 34%p 이상 달라짐을 고정한다. 향후 연구가 "
                              "raw close 로 직접 계산하면 이 회귀가 잡는다(§29)."),
        },
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "sizeResearchStarted": False,
        "factorResearchPerformed": 0,
        "limitations": [
            "RIGHTS_ISSUE(유상증자) 신주배정비율·발행가·청약일 데이터가 저장소에 없다. "
            "의심 2,742건이 1,652개 종목(universe 의 40.8%)에서 발생한다. 조정하지 않아 "
            "가짜 wealth 는 만들지 않지만, 권리를 행사한 주주의 수익은 과소평가된다.",
            "MERGER 교환비율 · SPINOFF 배정비율 데이터가 없다. 피합병 종목이 상폐로 "
            "처리되어 실제 교환대가가 wealth 에서 누락된다.",
            "CODE_CHANGE 매핑 이력이 없다. 종목코드 변경이 상폐+신규상장으로 보일 수 있다.",
            "이벤트 분류는 shares·close 두 신호만의 파생 추론이다(DERIVED_INFERENCE). "
            "marketCap 은 close × shares 파생값이라 독립 검증이 아니다(§1-1). "
            "최적 임계에서도 개별 사건 분류 정확도는 낮다(§1-2). "
            "액면변경 직접소스(pykrx get_stock_major_changes)로 22건 표본만 대조했고, "
            "전 종목 적용은 종목당 1콜이라 이번 범위 밖이다.",
            "월 스냅샷이라 정확한 ex-date/배당락일을 재현하지 못한다. DPS 는 연간값이라 "
            "월 1/12 균등 발생으로 근사했다.",
            "상장폐지 실제 정산금 데이터가 없다. 마지막 관측가 청산 + UNKNOWN_RECOVERY "
            "flag 이며 이는 상한 추정이다.",
            "유상감자(현금 수령)와 무상감자를 구분하지 못한다. 유상감자 현금이 누락된다.",
            "백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "krStockAgentRepo": "untouched", "productionUrl": "untouched",
            "legacy50d": "unchanged",
            "newBmForwardState": "존재하지 않음 — 미접근",
            "canonical": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "homepage": "unchanged",
            "scheduler": "untouched", "publicDisclosure": 0,
            "broker": 0, "realOrders": 0, "paidData": 0, "externalSend": 0,
            "deploy": 0, "envOrToken": 0, "productionDbWrite": 0},
        "priorArtifactsPreserved": True,
        "hotgClosedLoop": {
            "canonicalReport": f"reports/wababa/{NAME}.md",
            "reportBridgeCollectable": True,
            "evidence": ("projects.registry.json wababa reportBridge=true · "
                         "report_policy=INCLUDE · child_roots 에 data-new 포함."),
            "sourceOwner": "Wababa",
            "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
            "newObserverCreated": 0, "duplicateOrchestration": 0,
            "founderAttentionEscalation": (
                "BLOCKED 이므로 §33 에 따라 구체적 missing data 와 영향도를 Founder "
                "attention 으로 승격한다 — 유상증자 이력이 없어 universe 40.8% 종목의 "
                "장기 wealth 를 확정할 수 없다."),
            "closeCondition": "R16 판정이 08:40 종합보고에 올라가면 CLOSE."},
        "nextDecision": {
            "founderAction": "NONE — 승인 게이트 없음. 데이터 수집은 무료 공개소스 범위.",
            "single": ("유상증자(신주배정) 이력 1건만 확보한다. DART 공시 목록 API 에서 "
                       "'유상증자결정' 보고서를 종목·날짜·배정비율·발행가로 수집해 "
                       "reports/research 캐시에 적재하고, r16_canonical 의 "
                       "SUSPECTED_RIGHTS 2,742건을 직접소스로 대조한다. "
                       "이 gap 하나가 BLOCKED 를 PASS_WITH_LIMITATIONS 로 바꾸는 "
                       "유일한 병목이다."),
            "notNow": ["factor 연구(BM/SIZE/QUALITY/ROE/MF)", "R17 시작",
                       "포트폴리오 설계", "홈페이지 공개", "실계좌 자금",
                       "합병·인적분할 데이터(유상증자보다 영향 작음)"],
            "why": ("§38 — R16 이 BLOCKED 이면 factor 연구 금지. 가장 material 한 "
                    "corporate-action data gap 1개만 먼저 해결한다."),
        },
    }


def md(d):
    pc, inv = d["precommit"], d["corporateActionInventory"]
    anc, ano = d["anchorCases"], d["anomalyAudit"]
    sep = d["classifierSeparation"]
    cov, leg, vd = d["tsrCoverage"], d["legacyResearchStatus"], d["foundationVerdict"]
    o = []
    A = o.append
    A("전체 판정: BLOCKED")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R16 — canonical TSR 연구기반 재설정")
    A("")
    A(f"- **FOUNDATION_VERDICT: {vd['verdict']}**")
    A(f"- 삼성전자 anchor 재현: **{'PASS' if vd['anchorSamsungPass'] else 'FAIL'}** · "
      f"핵심 event 지원: {vd['coreEventsSupported']}")
    A(f"- 미지원 event: {', '.join(vd['unsupportedEvents'])}")
    A(f"- 유상증자 의심 영향 종목 비율: **{vd['rightsAffectedTickerPct']}%**")
    A(f"- 다음 factor 연구 허용: **{vd['nextFactorResearchAllowed']}** · "
      f"factor 재실행 {d['factorResearchPerformed']}건 · SIZE 연구 "
      f"{d['sizeResearchStarted']}")
    A("")
    A("> **엔진은 만들어졌고 검증도 통과했다. 그런데 데이터가 막았다.**")
    A("> 삼성전자 anchor 는 소수점까지 일치하고 역분할 가짜수익도 제거됐다. 하지만")
    A("> 유상증자 이력이 없어 universe 의 40.8% 종목에서 주주 wealth 를 확정할 수 없다.")
    A("> 사전에 정한 기준(40% 미만이면 PASS_WITH_LIMITATIONS)을 40.8% 로 **넘겼다**.")
    A("> 기준을 옮기지 않고 BLOCKED 로 판정한다.")
    A("")

    A("## 1. 기존 engine 문제 요약")
    A("")
    A("R15 에서 확인된 것: `fwd_return = close_j/close_i - 1` 은 액면분할 미조정 +")
    A("배당 누락이었다. 삼성전자 2010-01→2021-08 을 −18.17%/년으로 기록했지만 실제")
    A("주주수익은 +16.36%/년이다(오차 34.53%p).")
    A("")
    A("R16 이 추가로 발견한 것: **R15 의 수정도 불완전했다.** R15 는 주식수 배율과")
    A("가격 역배율만 보고 '분할'로 판정했는데, 그 서명은 **유상증자와 구분되지 않는다.**")
    A("시총이 함께 뛴 359건이 그 경계에 있었다. 유상증자를 무상으로 조정하면 주주가")
    A("내지 않은 돈으로 가짜 wealth 가 생긴다.")
    A("")
    A("### 1-1. ★ 자체수정 — '시총은 3번째 신호' 는 틀렸다 (§35)")
    A("")
    A("R16 초안은 시총 연속성을 **독립적인 3번째 신호**로 규정했다. L1 합성")
    A("단위테스트가 이것을 깼다: 가격 무변동(1.00) + 주식수 0.8배(자사주 소각)를")
    A("기계적 사건으로 오분류해 보유주식을 잘못 줄였다.")
    A("")
    A("원인을 추적한 결과 — 스냅샷의 `marketCap` 은 `close × shares` 와 **정확히**")
    A("일치한다(2,035건 검사, 불일치 0건). 즉 `mcapRatio = shareRatio / priceRatio`")
    A("이고, 시총 밴드 검정은 비례성 검정과 **수학적으로 동치**다. 독립 정보가 없다.")
    A("")
    A("| | 초안 주장 | 실측 정정 |")
    A("|---|---|---|")
    A("| 독립 신호 수 | 3 (주식수·가격·시총) | **2 (주식수·가격)** |")
    A("| 시총의 역할 | 유상증자 분리 근거 | 파생값. 독립 검증 아님 |")
    A("| 비례성 검정 | 절대오차 0.30 | §1-2 에서 기준식 자체를 교체 |")
    A("")
    A("**이 정정은 결론을 완화하지 않고 강화한다.** 유상증자를 무상증자와 분리할")
    A("독립 근거가 애초에 없었다는 뜻이므로, 아래 BLOCKED 판정의 근거가 더 단단해진다.")
    A("")
    A("### 1-2. ★ 자체수정 2 — 임계값이 아니라 **기준 자체**가 틀렸다")
    A("")
    A("허용오차를 조이려다 분포를 먼저 실측했다. 결과가 예상과 달랐다.")
    A("")
    A(f"주식수 1.2배 이상 변동 {sep['events']:,}건에서 절대오차")
    A("`|priceRatio/shareRatio - 1|` 의 분포는 **0~0.30 구간이 거의 균일했다.**")
    A("골짜기가 없다 = 기계적 사건과 나머지가 애초에 갈라지지 않는다 =")
    A("**어떤 임계값을 골라도 자의적이다.**")
    A("")
    A("원인은 데이터 손상이 아니라 **월간 해상도**다. 스냅샷은 자본거래와 그 달의")
    A("주가변동을 분리하지 못하는데, 자본거래 전후 소형주는 월 25% 넘게 움직이는")
    A("일이 흔하다. (정렬 자체는 맞다 — lag −1/0/+1 대조에서 lag 0 이 최선이었다.)")
    A("")
    A("그래서 임계값이 아니라 **기준식을 교체**했다. 사건 크기에 비례해 허용폭이")
    A("커지는 로그공간 상대오차를 쓴다:")
    A("")
    A("```")
    A(f"{sep['logRelativeError']['formula']}")
    A("```")
    A("")
    lr, ab = sep["logRelativeError"], sep["absoluteError"]
    A("| 기준 | 정수비 중앙값 | 비정수비 중앙값 | 격차 |")
    A("|---|---|---|---|")
    A(f"| 절대오차 (초안) | {ab['cleanMedian']} | {ab['messyMedian']} | "
      f"{ab['messyMedian']/ab['cleanMedian']:.1f}배 |")
    A(f"| **로그 상대오차 (채택)** | **{lr['cleanMedian']}** | **{lr['messyMedian']}** | "
      f"**{lr['messyMedian']/lr['cleanMedian']:.1f}배** |")
    A("")
    A("대조군은 정수·단순분수 주식수비율(분할·무상증자의 특징) vs 그 외다.")
    A(f"{sep['control']['caveat']}")
    A("")
    A("**임계값 민감도** — 결과를 보고 고르지 않았음을 보이기 위해 전 구간을 남긴다.")
    A("")
    A("| 임계 | 정수비 통과 | 비정수비 통과 | 분리력 |")
    A("|---|---|---|---|")
    for r in sep["thresholdSensitivity"]:
        mark = " ←채택" if r["threshold"] == sep["chosen"] else ""
        A(f"| {r['threshold']:.2f}{mark} | {r['cleanPassPct']}% | "
          f"{r['messyPassPct']}% | {r['separationPp']}%p |")
    A("")
    A(f"{sep['chosenWhy']}")
    A("")
    A("검증: 삼성 50:1 분할 0.008 통과 · 1:5 역분할 0.000 통과 ·")
    A("자사주 소각(주식수 0.8배·가격 무변동) 1.000 배제.")
    A("")
    A("**남는 한계를 숨기지 않는다.** " + sep["residualLimitation"])
    A("")
    A("두 자체수정 모두 factor 결과를 보기 **전**에 이뤄졌다 — §16 은 R16 에서")
    A("factor 연구 자체를 금지하므로 맞출 결과가 존재하지 않는다.")
    A("")

    A("## 2. canonical TSR 정의 (§2)")
    A("")
    c = pc["canonical"]
    A("```")
    A(f"측정              {c['measure']}")
    A(f"방식              {c['method']}")
    A(f"PRIMARY           {c['primary']}")
    A(f"보조              {c['secondary']}")
    A(f"단위              {c['unit']}")
    A("```")
    A("")
    A(f"{c['whyNotAdjustedSeries']}")
    A("")

    A("## 3. 배당 convention (§12·§20)")
    A("")
    dv = pc["dividend"]
    A("```")
    A(f"소스              {dv['source']}")
    A(f"발생              {dv['accrual']}")
    A(f"재투자 가격       {dv['reinvestPrice']}")
    A(f"거래비용          {dv['transactionCost']}")
    A(f"특별배당          {dv['specialDividend']}")
    A("```")
    A("")
    A(f"**결측 vs 0**: {dv['missingVsZero']['rule']}")
    A("")
    dc = inv["dividendCoverage"]
    A(f"실측 커버리지 — 유효 {dc['presentPct']}% · 0(무배당) {dc['zeroPct']}% · "
      f"결측 {dc['missingPct']}%")
    A("")

    A("## 4. corporate action 지원표 (§5·§6)")
    A("")
    A("| 이벤트 | 소스 | provenance | 커버리지 | 엔진 처리 | 지원상태 | 남은 gap |")
    A("|---|---|---|---|---|---|---|")
    for r in inv["rows"]:
        A(f"| {r['event']} | {r['source']} | {r['provenance']} | {r['coverage']} | "
          f"{r['engine']} | **{r['support']}** | {r['gap']} |")
    A("")
    ss = inv["supportSummary"]
    A(f"요약 — COMPLETE {ss['SUPPORTED_COMPLETE']} · PARTIAL {ss['SUPPORTED_PARTIAL']} · "
      f"**UNSUPPORTED_DATA {ss['UNSUPPORTED_DATA']}** · N/A {ss['NOT_APPLICABLE']}")
    A("")
    A("### 사건 분류 실측 (종목-월 537,715건)")
    A("")
    ec = inv["eventClassification"]
    A("```")
    for k, v in ec.items():
        A(f"{k:34} {v:6,}")
    A("```")
    A("")
    A("주식수만 보면 이 넷이 전부 '분할'처럼 보인다. 가격 비례성까지 봐야 갈라진다.")
    A("")
    A("다만 §1-1 대로 시총은 파생값이라 **독립 검증이 아니다.** 여기서 말하는")
    A("SUSPECTED_RIGHTS_ISSUE 는 '주식수가 늘었는데 가격이 비례해 떨어지지 않았다'")
    A("는 뜻일 뿐, 유상증자임을 **증명하지 못한다.** 무상증자 직후 주가가 오른")
    A("경우도 같은 서명을 낸다. 그래서 이 구간 전체를 미조정으로 남기고 BLOCKED 로")
    A("판정한다 — 조정해도 틀리고 안 해도 틀릴 수 있는 구간이기 때문이다.")
    A("")

    A("## 5. anchor case 검증 (§9·§10·§11)")
    A("")
    A(f"tolerance {anc['tolerancePctPoints']}%p")
    A("")
    sam = anc["cases"][0]
    A("### A. 삼성전자 — R15 manual ledger 재현")
    A("")
    A("| 지표 | engine | manual(R15) | 차이 | 판정 |")
    A("|---|---|---|---|---|")
    for k in sam["reproductionChecks"]:
        A(f"| {k['metric']} | {n2(k['engine'])}% | {n2(k['r15Manual'])}% | "
          f"{sg(k['diffPp'], 3)}%p | {'PASS' if k['pass'] else 'FAIL'} |")
    A("")
    A(f"보유 1주 → {n2(sam['endingShares'])}주 · 최종 wealth "
      f"{sam['endingWealth']:,}원 · 누적 {n2(sam['cumulativeReturnPct'])}%")
    A("")
    A("### B~F. 사건유형별")
    A("")
    A("| case | 유형 | 종목 | 배율 | 조정 | 사건월 wealth 변화 | 가격만이면 |")
    A("|---|---|---|---|---|---|---|")
    for cse in anc["cases"][1:]:
        if cse.get("status"):
            A(f"| {cse['case']} | {cse.get('kind','-')} | - | - | - | "
              f"**{cse['status']}** | - |")
            continue
        if cse["case"] == "F_DELISTING":
            A(f"| {cse['case']} | {cse['kind']} | {cse['name']}({cse['ticker']}) | - | - | "
              f"누적 {n2(cse['cumulativeReturnPct'])}% | {cse['delistingRecovery']} |")
            continue
        A(f"| {cse['case']} | {cse['kind']} | {cse['name']}({cse['ticker']}) | "
          f"×{n2(cse['shareRatio'], 3)} | {'O' if cse['adjusted'] else 'X'} | "
          f"{sg(cse['eventMonthWealthChangePct'])}% | "
          f"{sg(cse['priceOnlyWealthChangePct'])}% |")
    A("")
    A("읽는 법 —")
    A("")
    A("- **B 역분할/감자**: 가격만 보면 +231.0% 라는 가짜 수익이 나오는데, 보유주식수를")
    A("  ×0.250 으로 줄이면 −17.27% 다. R15 이전 엔진이 만들던 가짜 수천% 가 제거된다.")
    A("- **C 무상증자/분할**: 가격만 보면 −11.94% 인데 무상 신주 ×1.434 를 반영하면")
    A("  +26.47% 다. 주주가 실제로 받은 주식이 잡힌다.")
    A("- **D 유상증자 의심**: 조정하지 않으므로 wealth 변화 = 가격 변화(−4.15%).")
    A("  **가짜 wealth 를 만들지 않는다** — 이것이 R16 이 R15 보다 나아진 지점이다.")
    A("- **E 자사주 소각**: 조정하지 않는다. 보유주식수는 그대로이고 현금도 없다.")
    A("- **G 합병 · H 인적분할**: BLOCKED_DATA. mock 으로 통과시키지 않았다.")
    A("")

    A("## 6. universe coverage / materiality (§18·§27)")
    A("")
    A("```")
    A(f"universe 종목        {cov['universeTickers']:,}")
    A(f"종목-월              {cov['tickerMonths']:,}")
    A(f"MECHANICAL 영향 종목  {cov['affectedTickers']['MECHANICAL']:,} "
      f"({cov['affectedMonthRatio']['MECHANICAL']}% 월)")
    A(f"유상증자 의심 종목    {cov['affectedTickers']['SUSPECTED_RIGHTS']:,} "
      f"({cov['affectedTickers']['rightsPctOfUniverse']}% 종목 · "
      f"{cov['affectedMonthRatio']['SUSPECTED_RIGHTS']}% 월)")
    A(f"미해결 주식수 증가    {cov['affectedTickers']['SHARES_UP_UNRESOLVED']:,}")
    A(f"TSR_FULL_COVERAGE     {cov['TSR_FULL_COVERAGE_START']} 이후")
    A("```")
    A("")
    A(f"{cov['materiality']['interpretation']}")
    A("")
    A("> **두 숫자를 함께 봐야 한다.** 종목 기준으로는 40.8% 가 영향을 받지만,")
    A("> 종목-월 기준으로는 0.51% 다. 즉 대부분의 종목이 20년에 한두 번 유상증자를")
    A("> 겪는다. 그래도 장기 보유 수익률을 계산하려면 그 한두 번이 경로에 들어간다.")
    A("")

    A("## 7. anomaly scan (§25)")
    A("")
    A(f"canonical 적용 후 이상치 **{ano['anomalies']}건** — "
      f"TRUE_EVENT {ano['byClassification'].get('TRUE_EVENT',0)} · "
      f"UNRESOLVED {ano['byClassification'].get('UNRESOLVED',0)}")
    A("")
    A(f"사유별: {json.dumps(ano['byReason'], ensure_ascii=False)}")
    A("")
    A("| 날짜 | 종목 | 월수익률 | 탐지사건 | 분류 |")
    A("|---|---|---|---|---|")
    for w in ano["worst"][:8]:
        A(f"| {w['date'][:7]} | {w['name']}({w['ticker']}) | "
          f"{n2(w['monthlyReturnPct'])}% | {w['event'] or '-'} | {w['classification']} |")
    A("")
    A(f"UNRESOLVED {ano['byClassification'].get('UNRESOLVED',0)}건은 미탐지 corporate")
    A("action 이거나 데이터 오류다. 숨기지 않고 남긴다.")
    A("")

    A("## 8. wealth ledger / external cash flow (§7·§8)")
    A("")
    A("ledger 상태: date · ticker · sharesHeld · cash · price · marketValue ·")
    A("totalWealth · dividendCash · rightsValue · capitalContribution ·")
    A("corporateActionEvent · splitFactor · shareCountBefore/After · source ·")
    A("provenance · wealthBefore · externalCashFlow · wealthAfter")
    A("")
    cf = pc["cashflow"]
    A(f"- {cf['separation']}")
    A(f"- canonical 수익 기준: **{cf['canonicalReturnBasis']}** — {cf['why']}")
    A(f"- 현재 상태: {cf['currentState']}")
    A("")

    A("## 9. 기존 연구 상태 재설정 (§15)")
    A("")
    A(f"{leg['policy']}")
    A("")
    A("| 대상 | 상태 |")
    A("|---|---|")
    for k, v in leg["statuses"].items():
        A(f"| {k} | **{v}** |")
    A("")
    A(f"- canonical 성과로 사용 가능: **{leg['canonicalPerformanceUsable']}**")
    A(f"- 대외 사용 허용: **{leg['publicUseAllowed']}**")
    A(f"- {leg['note']}")
    A("")
    A(f"**R15 도 잠정으로 내린 이유** — {leg['whyR15Superseded']}")
    A("")
    A(f"기존 산출물 미변경: **{leg['priorArtifactsUntouched']}**")
    A("")

    A("## 10. 다음 연구용 output contract (§28·§29)")
    A("")
    oc = d["outputContract"]
    A("```")
    A(f"api      {oc['api']}")
    A(f"returns  {', '.join(oc['returns'])}")
    A(f"modes    {' | '.join(oc['modes'])}")
    A("```")
    A("")
    A(f"- adapter: {oc['fwdReturnAdapter']}")
    A(f"- benchmark invariant: {oc['benchmarkInvariant']}")
    A(f"- raw close guard: {oc['rawCloseGuard']}")
    A("")

    A("## 11. foundation 판정 (§17)")
    A("")
    A(f"### **{vd['verdict']}**")
    A("")
    A(f"{vd['reasoning']}")
    A("")
    A("판정 근거를 그대로 적는다:")
    A("")
    A("```")
    A(f"삼성전자 anchor 재현       {vd['anchorSamsungPass']}")
    A(f"핵심 event 지원            {vd['coreEventsSupported']}")
    A(f"UNSUPPORTED_DATA           {', '.join(vd['unsupportedEvents'])}")
    A(f"유상증자 영향 종목 비율     {vd['rightsAffectedTickerPct']}%")
    A(f"사전 기준(PASS_WITH_LIM)   40% 미만")
    A(f"→ 40.8% 로 기준 초과       BLOCKED")
    A("```")
    A("")
    A("> 40.8% 대 40% 는 아슬아슬하다. 그래서 더더욱 기준을 옮기지 않는다. 이 임계값은")
    A("> 감사 코드를 **실행하기 전에** 정해졌고, 결과를 본 뒤 조정하면 지금까지 R9~R15 에서")
    A("> 지켜온 규율이 무너진다. 다만 이 근접성은 그대로 보고한다 — 유상증자 데이터")
    A("> 하나만 확보하면 곧바로 PASS_WITH_LIMITATIONS 로 넘어갈 수 있다는 뜻이기도 하다.")
    A("")

    A("## 12. production 보호 (§31)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:26} {v}")
    A(f"{'realMoneyStage':26} {d['realMoneyStage']}")
    A(f"{'priorArtifactsPreserved':26} {d['priorArtifactsPreserved']}")
    A("```")
    A("")
    A("## 13. HOTG (§33)")
    A("")
    h = d["hotgClosedLoop"]
    A("```")
    A(f"canonical report      {h['canonicalReport']}")
    A(f"Report Bridge 수집    {h['reportBridgeCollectable']}")
    A(f"source_owner          {h['sourceOwner']}")
    A(f"표면                  {h['surface']}")
    A(f"신규 observer         {h['newObserverCreated']}")
    A("```")
    A("")
    A(f"**Founder attention 승격**: {h['founderAttentionEscalation']}")
    A("")
    A("## 14. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")
    A("## 15. Founder 행동 / 다음 단일 작업")
    A("")
    A(f"- **Founder 행동: {d['nextDecision']['founderAction']}**")
    A("")
    A(f"- 다음 단일 작업: {d['nextDecision']['single']}")
    A("")
    A("- 지금 하지 않는 것: " + " · ".join(d["nextDecision"]["notNow"]))
    A("")
    A(f"- {d['nextDecision']['why']}")
    A("")
    return "\n".join(o)


def main() -> int:
    WD.mkdir(parents=True, exist_ok=True)
    d = build()
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"),
                      "verdict": d["foundationVerdict"]["verdict"],
                      "nextFactorResearchAllowed":
                          d["foundationVerdict"]["nextFactorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
