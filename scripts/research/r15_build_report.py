#!/usr/bin/env python3
"""R15 산출물 생성 (MD + JSON) — §18.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

reports/wababa/wababa-tsr-corporate-action-forensic-r15-latest.{md,json}
기존 R7~R14 산출물 덮어쓰기 금지 — provenance 보존(§24).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-tsr-corporate-action-forensic-r15-latest"
QIDS = ["Q1_PROFIT_PERSISTENCE", "Q2_CAPITAL_COMPOUNDING", "Q3_DIVIDEND_DISCIPLINE"]
SHORT = {"Q1_PROFIT_PERSISTENCE": "Q1 흑자지속",
         "Q2_CAPITAL_COMPOUNDING": "Q2 자본복리",
         "Q3_DIVIDEND_DISCIPLINE": "Q3 배당성향",
         "ROE": "ROE(대조)", "SIZE": "SIZE(귀속)"}
HL = ["12", "36", "60", "84"]


def L(n):
    p = RD / f"r15-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def sg(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def won(x):
    return "-" if x is None else f"{round(x):,}원"


def build():
    pc, ss = L("precommit"), L("samsung-tsr")
    cov, ea = L("corporate-action-coverage"), L("return-engine-audit")
    rv, bef = L("r14-tsr-revalidation"), L("before-repro")
    newv = L("tsr-verdict")
    return {
        "schema": "wababa-tsr-corporate-action-forensic-r15@1",
        "taskId": "WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15",
        "verdictLine": "WARNING",
        "reasonClass": "R15_TSR_MATERIAL_BUG_FIXED_R14_CONCLUSION_CHANGED",
        "purpose": pc["purpose"],
        "precommit": {"created": True,
                      "writtenBeforeCorrectedResults": pc["writtenBeforeCorrectedResults"],
                      "file": "reports/research/r15-precommit-latest.json",
                      "splitRule": pc["splitRule"], "dividendRule": pc["dividendRule"],
                      "noAdjustment": pc["noAdjustment"],
                      "revalidationRule": pc["revalidationRule"]},
        "samsungAnchor": ss,
        "corporateActionCoverage": cov,
        "returnEngineAudit": ea,
        "reproductionBeforeCorrection": bef,
        "r14Revalidation": rv,
        "correctedVerdict": newv,
        "tsrEngineVerdict": "TSR_MATERIAL_BUG_FIXED",
        "r14QualityVerdictAfterTsr": "LONG_HORIZON_PROMISING",
        "sizeResearchStarted": False,
        "parameterRescue": 0,
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "limitations": [
            "유상증자·합병·인적분할은 신주배정비율·발행가 데이터가 저장소에 없어 "
            "조정하지 않았다. 권리를 행사한 주주의 실제 수익을 과소평가할 수 있다. "
            "가짜 정밀도를 만들지 않기 위한 선택이다(§11).",
            "DPS 는 직전 **연간** 값이라 배당락 시점을 정확히 재현하지 못한다. "
            "월 1/12 균등 발생으로 근사했다. 배당이 몰린 달의 경로는 실제와 다르다.",
            "무상감자와 액면병합은 서명이 같아 구분되지 않는다. 둘 다 REVERSE_SPLIT 로 "
            "처리된다.",
            "분할 탐지는 주식수·가격 서명 기반이다. 서명이 흐린 소수 사례는 놓칠 수 있다"
            "(주식수 1.5배 이상 변동 5,680건 중 서명 일치 1,633건, 나머지는 유상증자·"
            "자사주 소각 등으로 판단해 미조정).",
            "삼성전자 매출·영업이익·영업현금흐름은 장기 PIT 에 없다(DART FY2022~2025 뿐). "
            "총액은 EPS×주식수 / BPS×주식수 파생 근사치로만 보고했다.",
            "R15 는 지시문 범위대로 **R14 재검증까지만** 했다. R7·R8~R11·R13 은 같은 "
            "결함을 갖지만 이번에 재검증하지 않았다.",
            "백테스트 결과이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "krStockAgentRepo": "untouched", "productionUrl": "untouched",
            "legacy50d": "unchanged",
            "newBmForwardState": "존재하지 않음(R11 설계만) — 미접근",
            "canonical": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "homepage": "unchanged",
            "scheduler": "untouched", "publicDisclosure": 0,
            "broker": 0, "realOrders": 0, "paidData": 0, "externalSend": 0,
            "deploy": 0, "envOrToken": 0, "productionDbWrite": 0},
        "priorArtifactsPreserved": True,
        "hotgClosedLoop": {
            "canonicalReport": f"reports/wababa/{NAME}.md",
            "reportBridgeCollectable": True,
            "evidence": ("projects.registry.json 의 wababa 항목 reportBridge=true · "
                         "report_policy=INCLUDE · child_roots 에 "
                         "C:\\work\\kr-stock-agent-data-new 포함."),
            "sourceOwner": "Wababa",
            "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
            "newObserverCreated": 0, "duplicateOrchestration": 0,
            "closeCondition": "R15 판정이 08:40 종합보고에 올라가면 CLOSE.",
            "executionSucceededInvisibly": False},
        "nextDecision": {
            "founderAction": "NONE — 승인 게이트 없음. 판정은 데이터가 내렸다.",
            "single": ("R8~R11 을 수정된 TSR 엔진으로 재검증한다. R11 의 "
                       "INCREMENTAL_ALPHA_CONFIRMED(+6.90%p)와 R8~R10 의 포트폴리오 "
                       "CAGR 은 전부 `snap['close']` 직접 사용 = 같은 분할 미조정 + "
                       "배당 누락 위에 서 있다. R14 가 뒤집힌 이상 R11 도 재검증 "
                       "전까지 신뢰할 수 없다. 이것이 SIZE 연구보다 먼저다."),
            "notNow": ["SIZE 전략 연구", "R15 결과로 Q3 를 전략화",
                       "R16 포트폴리오 설계", "홈페이지 공개", "실계좌 자금"],
            "why": ("§17 — return measurement 가 틀렸다면 SIZE 포함 과거 factor 연구를 "
                    "모두 같은 기준으로 점검해야 한다. TSR engine 판정이 먼저다."),
        },
    }


def md(d):
    pc, ss = d["precommit"], d["samsungAnchor"]
    cov, ea = d["corporateActionCoverage"], d["returnEngineAudit"]
    rv, bef = d["r14Revalidation"], d["reproductionBeforeCorrection"]
    o = []
    A = o.append
    A("전체 판정: WARNING")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R15 — TSR / corporate-action forensic audit")
    A("")
    A(f"- **TSR_ENGINE_VERDICT: {d['tsrEngineVerdict']}**")
    A(f"- **R14_QUALITY_VERDICT_AFTER_TSR: {d['r14QualityVerdictAfterTsr']}**")
    A(f"- **R14 결론 변경: {rv['r14ConclusionChanged']}** "
      f"({rv['finalVerdict']['old']} → {rv['finalVerdict']['new']})")
    A(f"- parameter rescue {d['parameterRescue']} · SIZE 연구 시작 "
      f"{d['sizeResearchStarted']} · {d['realMoneyStage']}")
    A("")
    A("> **Founder 의 문제제기가 옳았다.** 우리는 지난 20년의 수익률을 실제 주주가")
    A("> 경험한 경제적 수익으로 계산하고 있지 **않았다**. 액면분할이 조정되지 않았고")
    A("> 배당이 통째로 빠져 있었다. 삼성전자를 2010-01부터 2021-08까지 보유했을 때")
    A("> 기존 엔진은 **−90.20%**로 기록했지만 실제 주주수익은 **+478.52%**다.")
    A("")

    # 1. 삼성전자
    A("## 1. 삼성전자 anchor case (§1~§4)")
    A("")
    p = ss["period"]
    A(f"기간 **{p['start']} → {p['end']}** ({p['months']}개월 · {p['years']}년)")
    A("")
    A("```")
    A(f"시작 종가        {won(ss['rawPrice']['start'])}")
    A(f"종료 종가        {won(ss['rawPrice']['end'])}")
    A(f"원시 가격변화    {ss['rawPrice']['rawPriceChangePct']}%   ← 분할 때문에 무의미")
    A("")
    A(f"주식수 시작      {ss['sharesOutstanding']['start']:,.0f}")
    A(f"주식수 종료      {ss['sharesOutstanding']['end']:,.0f}")
    A(f"전체 배율        {ss['sharesOutstanding']['totalRatio']}")
    A(f"  분할 배율      {ss['sharesOutstanding']['splitMultiplier']}   (2018-05→06 50:1)")
    A(f"  분할 제외 배율 {ss['sharesOutstanding']['nonSplitRatio']}   ← 자사주 소각으로 순감소")
    A("```")
    A("")
    A("### corporate action")
    A("")
    for e in ss["corporateActions"]["detectedSplitEvents"]:
        A(f"- **{e['fromDate'][:7]} → {e['toDate'][:7]} 액면분할** 주식수 ×{e['shareRatio']:.0f} · "
          f"가격 ×{1/e['priceRatio']:.4f} → 기존 엔진은 이 달을 "
          f"**{e['rawEngineReturnPct']:.2f}%** 로 기록했다")
    A(f"- 자사주 소각: {ss['corporateActions']['treasuryCancellation']}")
    A(f"- 특별배당: {ss['corporateActions']['specialDividend']}")
    A(f"- 유상증자: {ss['corporateActions']['rightsIssue']}")
    A("")
    A("### return bridge")
    A("")
    A("| 지표 | 총수익 | CAGR |")
    A("|---|---|---|")
    for k, v in ss["returnVariants"].items():
        A(f"| {k} | {n2(v['totalPct'])}% | {n2(v['cagrPct'])}% |")
    A("")
    hp = ss["holdingPath"]
    A("```")
    A(f"보유 1주 → {hp['finalSharesPerInitialShare']:.2f}주 (분할 반영)")
    A(f"누적 현금배당 {won(hp['cumulativeCashDividendKrw'])} "
      f"(시작 투자금 대비 {hp['dividendAsPctOfInitialPrice']}%)")
    A("```")
    A("")
    ec = ss["engineComparison"]
    A(f"**기존 엔진 {ec['currentEngineCagrPct']}%/년 vs 실제 TSR "
      f"{ec['correctTsrCagrPct']}%/년 → 오차 {sg(ec['errorPctPointsPerYear'])}%p/년**")
    A("")
    sm = ss["sanityChecks"]["splitMonthResult"]
    A("### sanity check (§15)")
    A("")
    A("```")
    A(f"{sm['month']}")
    A(f"  미조정 엔진      {sm['unadjustedEngineReturnPct']}%")
    A(f"  조정 후 보유수익 {sm['adjustedHoldingReturnPct']}%")
    A(f"  실제 시총 변화   {sm['marketCapChangePct']}%")
    A(f"  일치(±0.5%p)     {sm['pass']}")
    A("```")
    A("")
    A("분할로 wealth 가 50배 증가/감소하지 않는다. 조정 수익률이 시총 변화와 일치한다.")
    A("")

    # 2. fundamentals
    A("## 2. 삼성전자 per-share vs company-level (§7)")
    A("")
    fa = ss["fundamentalPerShareAudit"]
    A(f"{fa['note']}")
    A("")
    A("| 항목 | 시작 | 종료 | CAGR |")
    A("|---|---|---|---|")
    cl, ps = fa["companyLevel"], fa["perShare"]
    A(f"| 파생 순이익(총액) | {won(cl['derivedNetIncomeStart'])} | "
      f"{won(cl['derivedNetIncomeEnd'])} | {n2(cl['derivedNetIncomeCagrPct'])}% |")
    A(f"| 파생 자본(총액) | {won(cl['derivedEquityStart'])} | "
      f"{won(cl['derivedEquityEnd'])} | {n2(cl['derivedEquityCagrPct'])}% |")
    A(f"| EPS (분할조정) | {n2(ps['epsStartSplitAdj'])} | {n2(ps['epsEnd'])} | "
      f"{n2(ps['epsCagrPct'])}% |")
    A(f"| BPS (분할조정) | {n2(ps['bpsStartSplitAdj'])} | {n2(ps['bpsEnd'])} | "
      f"{n2(ps['bpsCagrPct'])}% |")
    A(f"| DPS | {n2(ps['dpsStart'])} | {n2(ps['dpsEnd'])} | - |")
    A("")
    A(f"{fa['companyVsPerShare']}")
    A("")
    A(f"미가용: {', '.join(fa['unavailable'])}")
    A("")

    # 3. coverage
    A("## 3. corporate-action coverage (§9)")
    A("")
    A(f"종목-월 {cov['tickerMonths']:,}건 · 주식수 1.5배 이상 변동 "
      f"{cov['shareCountJumps']:,}건({cov['shareCountJumpPct']}%) · "
      f"분할 서명 탐지 {cov['splitLikeDetected']:,}건({cov['splitLikePct']}%) "
      f"[상향 {cov['splitUp']} · 역분할 {cov['splitDown']}]")
    A("")
    A("| 이벤트 | 데이터 출처 | 커버리지 | 수정 전 엔진 | 수정 후 엔진 | 미해결 |")
    A("|---|---|---|---|---|---|")
    for r in cov["rows"]:
        A(f"| {r['event']} | {r['source']} | {r['coverage']} | "
          f"{r['engineSupportBefore']} | {r['engineSupportAfter']} | {r['unresolved']} |")
    A("")
    for x in cov["limitations"]:
        A(f"- {x}")
    A("")

    # 4. engine verdict
    A("## 4. return engine 판정 (§5·§10)")
    A("")
    A(f"현행 정의: `{ea['currentEngineDefinition']}`")
    A("")
    A(f"**PRICE 인가 TSR 인가: {ea['isPriceOrTsr']}**")
    A("")
    for b in ea["bugs"]:
        A(f"### {b['id']} — {b['severity']}")
        A("")
        A(f"{b['evidence']}")
        A("")
    ui = ea["universeImpact"]
    A(f"### universe 영향 ({ui['period']})")
    A("")
    A("```")
    A(f"검사 종목            {ui['tickers']:,}개")
    A(f"연율 오차 0.5%p 초과 {ui['errorOver0_5ppPerYear']:,}개 ({ui['errorOver0_5ppPct']}%)")
    A(f"연율 오차 5%p 초과   {ui['errorOver5ppPerYear']:,}개 ({ui['errorOver5ppPct']}%)")
    A("```")
    A("")
    A("| 최악 오차 종목 | 기존 엔진 CAGR | 실제 TSR CAGR | 오차 |")
    A("|---|---|---|---|")
    for w in ui["worstTickers"][:6]:
        A(f"| {w['name']} ({w['ticker']}) | {n2(w['priceEngineCagrPct'])}% | "
          f"{n2(w['tsrCagrPct'])}% | {sg(w['errorPpPerYear'])}%p |")
    A("")
    A(f"> **판정: {ea['verdict']}** — {ea['verdictReason']}")
    A("")
    A(f"> ★ **{ea['scopeBeyondR14']}**")
    A("")

    # 5. revalidation
    A("## 5. R14 재검증 (§13·§14)")
    A("")
    A(f"수정 전 재현 게이트: **{'PASS' if bef['pass'] else 'FAIL'}** — {bef['note']}")
    A("")
    A(f"{rv['whatChanged']} · parameter rescue {rv['parameterRescue']}")
    A("")
    A("### BM 상위 20% 내부 연율 spread — OLD(가격) → NEW(TSR)")
    A("")
    A("| factor | 1Y | 3Y | **5Y★** | 7Y |")
    A("|---|---|---|---|---|")
    for q in QIDS + ["ROE", "SIZE"]:
        cells = []
        for h in HL:
            c = rv["bmTop20AnnSpread"][q][h]
            cells.append(f"{n2(c['old'])} → **{n2(c['new'])}** ({sg(c['diff'])})")
        A(f"| {SHORT[q]} | " + " | ".join(cells) + " |")
    A("")
    A("### ★ 3단계 분해 — 양전이 종목선정인가 배당 수취인가")
    A("")
    ts = rv["threeStageDecomposition"]
    A(f"{ts['note']}")
    A("")
    A("| factor | horizon | ①원본 | ②분할수정만 | ③+배당(TSR) | 분할기여 | 배당기여 |")
    A("|---|---|---|---|---|---|---|")
    for q in QIDS:
        for h in HL:
            r = ts["rows"][q][h]
            A(f"| {SHORT[q] if h == '12' else ''} | {int(h)//12}Y | "
              f"{n2(r['stage1_original'])} | {n2(r['stage2_splitFixedOnly'])} | "
              f"**{n2(r['stage3_fullTsr'])}** | {sg(r['splitFixContribution'])} | "
              f"{sg(r['dividendContribution'])} |")
    A("")
    A(f"> **{ts['q3Reading']}**")
    A("")
    A(f"> {ts['q1q2Reading']}")
    A("")
    A("### matched control 5Y (OLD → NEW)")
    A("")
    A("| 통제 | Q1 | Q2 | Q3 |")
    A("|---|---|---|---|")
    for m, v in rv["matched5Y"].items():
        A(f"| {m} | {n2(v[QIDS[0]]['old'])} → {n2(v[QIDS[0]]['new'])} | "
          f"{n2(v[QIDS[1]]['old'])} → {n2(v[QIDS[1]]['new'])} | "
          f"{n2(v[QIDS[2]]['old'])} → **{n2(v[QIDS[2]]['new'])}** |")
    A("")
    A("### 코호트 시작구간 5Y 중앙값")
    A("")
    A("| factor | 2010-2013 | 2014-2017 | 2018-2021 | 양수 구간 |")
    A("|---|---|---|---|---|")
    for q in QIDS:
        c = rv["cohortEra5Y"][q]
        for lab, key, pk in (("OLD", "old", "oldPositive"), ("NEW", "new", "newPositive")):
            e = c[key] or {}
            A(f"| {SHORT[q]} {lab} | {sg(e.get('2010-2013'))} | "
              f"{sg(e.get('2014-2017'))} | {sg(e.get('2018-2021'))} | {c[pk]} |")
    A("")
    A("### 등급 변화")
    A("")
    A("| factor | OLD | NEW | 변경 |")
    A("|---|---|---|---|")
    for q in QIDS:
        g = rv["grades"][q]
        A(f"| {SHORT[q]} | {g['old']} | **{g['new']}** | "
          f"{'O' if g['changed'] else '-'} |")
    A("")
    A(f"**최종: {rv['finalVerdict']['old']} → {rv['finalVerdict']['new']}** · "
      f"PRIMARY_QUALITY_FACTOR {rv['primaryQualityFactor']['old']} → "
      f"**{rv['primaryQualityFactor']['new']}** · 장기보유 가설 "
      f"{rv['longHoldHypothesis']['old']} → {rv['longHoldHypothesis']['new']}")
    A("")

    # 6. 해석
    A("## 6. 정직한 해석 — 무엇이 바뀌었고 무엇은 안 바뀌었나")
    A("")
    A("**바뀐 것**")
    A("")
    A("- Q3 배당성향이 INVERTED → LONG_HORIZON_PROMISING. 5Y +0.77%p · 3Y +1.06 · 7Y +1.02.")
    A("  matched control 3종 전부 양수(+1.47 / +1.69 / +2.07), 코호트 시작구간 3/3 양수.")
    A("- R14 의 '장기보유 가설 REJECTED' 는 **부분적으로 철회**된다.")
    A("")
    A("**바뀌지 않은 것**")
    A("")
    A("- Q1 흑자지속 5Y −3.56%p · Q2 자본복리 5Y −5.53%p — 배당을 넣어도 여전히 음수.")
    A("  '싼 주식 중 잘 버는 회사가 낫다'는 가설은 이 두 축에서 여전히 기각된다.")
    A("- Q3 의 양전은 **배당 수취분(+1.97%p)** 이고 가격 성분은 −1.20%p 로 여전히 음수다.")
    A("  즉 고배당 기업의 **주가는 계속 열위**이고 배당이 그 열위를 메우고 남는 구조다.")
    A("  \"배당성향 높은 기업의 사업이 더 좋다\"는 증거가 아니다.")
    A("- 크기도 작다(5Y +0.77%p). 1Y 는 −0.31%p 로 여전히 음수라 shape 은 E_UNSTABLE 이다.")
    A("- 표본 한계는 그대로다 — 5Y 비중첩 코호트 3개 · 7Y 2개.")
    A("")
    A("**이번 작업이 하지 않은 것**")
    A("")
    A("- Q3 를 전략으로 만들지 않았다. R15 의 임무는 engine audit 이지 factor 승격이 아니다.")
    A("- SIZE 연구를 시작하지 않았다(§17).")
    A("- R7·R8~R11·R13 을 재검증하지 않았다 — 다음 작업이다.")
    A("")

    # 7. production / hotg
    A("## 7. production 보호 (§20)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:26} {v}")
    A(f"{'realMoneyStage':26} {d['realMoneyStage']}")
    A(f"{'priorArtifactsPreserved':26} {d['priorArtifactsPreserved']}")
    A("```")
    A("")
    A("## 8. HOTG (§21)")
    A("")
    h = d["hotgClosedLoop"]
    A("```")
    A(f"canonical report        {h['canonicalReport']}")
    A(f"Report Bridge 수집      {h['reportBridgeCollectable']}")
    A(f"source_owner            {h['sourceOwner']}")
    A(f"표면                    {h['surface']}")
    A(f"신규 observer           {h['newObserverCreated']}")
    A(f"중복 orchestration      {h['duplicateOrchestration']}")
    A("```")
    A("")
    A("## 9. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")
    A("## 10. Founder 행동 / 다음 단일 작업")
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
                      "tsrEngineVerdict": d["tsrEngineVerdict"],
                      "r14After": d["r14QualityVerdictAfterTsr"],
                      "changed": d["r14Revalidation"]["r14ConclusionChanged"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
