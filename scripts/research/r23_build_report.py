#!/usr/bin/env python3
"""R23 canonical report (MD + JSON).

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23
기존 R16~R22 산출물을 덮어쓰지 않는다(§27). HOTG 는 기존 Bridge 재사용(§29).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-final-11-holder-rights-allocation-recovery-r23-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R23_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R23_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R23_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R23_CANONICAL_TSR_FOUNDATION_FAIL"),
}
KO = {"HOLDER_RIGHT_TERMS_MISSING": "주주배정 확정·조건 미확보",
      "TRUE_UNRESOLVED": "완전 미확인",
      "OTHER_CAPITAL_ACTION": "감자 등 기타 자본행위",
      "NO_ENTITLEMENT_CONFIRMED": "권리 없음 확정"}
KA = {"ALLOCATION_RECOVERED": "배정근거 복원", "PARTIAL_EVIDENCE": "부분 근거",
      "NO_ALLOCATION_EVIDENCE": "배정근거 없음"}


def L(n):
    p = RD / f"r23-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, sf = L("targets-precommit"), L("securities-filings")
    al, ft = L("allocation-tables"), L("final-terms")
    sr, an = L("share-reconciliation"), L("anchor-cases")
    ct, wr = L("ex-rights-continuity"), L("wealth-resolution")
    rec, bias = L("full-reconciliation"), L("expected-bias")
    mat, v = L("unresolved-materiality"), L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-final-11-holder-rights-allocation-recovery-r23@1",
        "taskId": "WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "securitiesFilings": {k: sf[k] for k in sf if k != "documents"},
        "allocationTables": {k: al[k] for k in al if k != "rows"},
        "finalTerms": {k: ft[k] for k in ft if k != "events"},
        "shareReconciliation": {k: sr[k] for k in sr if k != "rows"},
        "anchors": an, "continuity": ct, "wealthResolution": wr,
        "reconciliation": {k: rec[k] for k in rec if k != "rows"},
        "expectedBias": bias, "materiality": mat, "foundationVerdict": v,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched",
            "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
            "externalSend": 0, "deploy": 0, "envOrToken": 0,
            "productionDbWrite": 0, "publicDisclosure": 0,
            "realMoneyStage": "REAL_MONEY_NOT_APPROVED"},
        "hotg": {"canonicalReport": f"reports/wababa/{NAME}.md",
                 "sourceOwner": "Wababa", "reportBridge": True,
                 "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
                 "newObservers": 0, "newScheduler": 0, "newOrchestration": 0},
    }


def md(d):
    tg, sf, al = d["targetsPrecommit"], d["securitiesFilings"], d["allocationTables"]
    ft, sr, an = d["finalTerms"], d["shareReconciliation"], d["anchors"]
    ct, rec = d["continuity"], d["reconciliation"]
    bias, mat, v = d["expectedBias"], d["materiality"], d["foundationVerdict"]
    gs, cum = bias["gateSensitivity"], bias["cumulativeSensitivity"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R23 — 마지막 11건 구주주 배정량 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = mat["progression"]
    A("- 미해결 종목비율 **" + " → ".join(f"{k} {pg[k]}%" for k in
                                     ("R16", "R17", "R18", "R19", "R20",
                                      "R21", "R22", "R23")) + "** (기준 20%)")
    bp = bias["progression"]
    A("- 기대편향 **" + " → ".join(f"{k} {bp[k]}%" for k in
                                ("R18", "R19", "R20", "R21", "R22", "R23"))
      + f"** (기준 {bias['thresholdPct']}%)")
    A(f"- 11건 중 배정근거 복원 "
      f"**{al['byAllocationStatus'].get('ALLOCATION_RECOVERED', 0)}** · "
      f"wealth 확정 {ft['byWealthStatus'].get('WEALTH_CONFIRMED', 0)}")
    A(f"- anchor {an['obtained']}건 전부 PASS · 공짜 wealth {ct['freeWealthEvents']}건")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 증권신고서 원문에서 구주주 배정비율 산출근거를 직접 읽어 4건을")
        A(f"> 복원했다. 편향은 {bp['R22']}% → {bp['R23']}% 로 내려왔고 기준 2% 에")
        A("> **0.109%p** 남았다. 남은 7건은 신고서가 없거나 배정근거가 실려 있지")
        A("> 않아 **추정으로 채우지 않았다**(§36). 기준을 옮기지 않고 BLOCKED 를")
        A("> 유지한다. 이 군에서 **2건만 더** 복원되면 게이트를 통과한다(§21 누적감도).")
    elif v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("> 배정량 복원으로 편향이 기준 이하로 내려왔다. 한계를 명시한 채")
        A("> canonical TSR foundation 을 닫고 연구 사용을 허용한다.")
    elif v["verdict"].endswith("_PASS"):
        A("> foundation 이 연구 사용 가능 수준에 도달했다.")
    else:
        A("> 엔진 검증에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 증권신고서 확보 (§2·§3·§4)")
    A("")
    A("```")
    A(f"대상(주주배정 확정·배정량 미확보)   {tg['targetTotal']:5,}")
    A(f"대상 종목                            {tg['targetTickers']:5,}")
    A(f"검색 창                              {sf['window']['months']}")
    A(f"신고서·투자설명서 원문 확보           "
      f"{sf['documentsNew'] + sf['documentsCacheHits']:5,}")
    A(f"원문 실패                            {sf['documentsFailed']:5,}")
    A("```")
    A("")
    A("```")
    KF = {"REG_CONFIRMED": "[발행조건확정]증권신고서(지분증권)",
          "REG_AMENDED": "[정정]증권신고서(지분증권)",
          "REG_ORIGINAL": "증권신고서(지분증권/주식)",
          "PROSPECTUS": "투자설명서", "PROSPECTUS_AMENDED": "[정정]투자설명서"}
    for k, n in sorted(sf["filingKinds"].items(), key=lambda x: -x[1]):
        A(f"{KF.get(k, k):34} {n:4,}")
    A("```")
    A("")
    A(f"{sf['debtExcluded']}")
    A("")
    if sf.get("tickersWithNoFiling"):
        A(f"신고서가 아예 없는 종목: {', '.join(sf['tickersWithNoFiling'])}")
        A("")

    A("## 3. ★ 신고서에서 무엇을 읽었나 (§5·§6·§7·§8)")
    A("")
    A("증권신고서 본문에는 배정비율의 **산출 근거**가 통째로 실려 있다. 실측 예:")
    A("")
    A("```")
    A("※ 구주주 1주당 신주배정비율 산출 근거")
    A("  모집주식총수(11,000,000주) - 우리사주조합 우선배정분(2,200,000주)")
    A("  = ----------------------------------------------------------")
    A("  기발행보통주식수(32,531,794주) - 자기주식(2,902,135주)")
    A("  = 0.2969997056 주")
    A("```")
    A("")
    A("여기서 §5~§8 이 요구하는 값이 모두 나온다 — 구주주 배정량, eligible old")
    A("shares(자기주식 제외), 1주당 배정비율, 보통주 기준.")
    A("")
    A("**필드 복원률** (11건 기준)")
    A("")
    A("| 필드 | 복원 | 비율 |")
    A("|---|---|---|")
    KOF = {"finalRightsRatio": "최종 배정비율",
           "finalIssuePrice": "확정발행가",
           "eligibleOldShares": "eligible 구주(자기주식 제외)",
           "ENTITLED_TO_EXISTING_SHAREHOLDERS": "구주주 배정주식수",
           "treasuryShares": "자기주식수",
           "issuedCommonShares": "기발행 보통주식수",
           "ACTUALLY_ISSUED_TOTAL": "실제 발행주식수"}
    for k, r in al["fieldRecovery"].items():
        A(f"| {KOF.get(k, k)} | {r['n']} | {r['pct']}% |")
    A("")
    A(f"자기주식 제외 반영: **{al['treasuryExcludedCount']}건** · "
      f"우선주 분리 확인: **{al['preferredSeparatedCount']}건**")
    A("")

    A("## 4. ★ 자체수정 7건 (§31)")
    A("")
    A("**SC1** 신고서 양식이 최소 세 가지다(산식형·항목형·배정표형). 하나만")
    A("지원하면 1/11 만 읽힌다. 세 양식을 모두 지원하도록 확장했다.")
    A("")
    A("**SC2** 신고서 한 건에 유상증자와 **무상증자**가 함께 실린다. 문서 전체에서")
    A("'1주당 신주배정비율' 을 찾으면 무상증자의 1.00000 을 집는다(실측 4건).")
    A("구주주 배정 근거 **블록 안에서만** 찾도록 문맥을 좁혔다.")
    A("")
    A("**SC3** 그 문맥 배제가 처음엔 너무 넓어 정상 건까지 떨어뜨렸다(4→2건).")
    A("바로 앞 120자에서 무상증자가 유상증자보다 가까울 때만 배제하도록 좁히고,")
    A("후보 구간을 순서대로 시도하게 했다.")
    A("")
    A("**SC4** R22 가 넘긴 확정발행가 중 **2009.0·2010.6** 처럼 연도가 섞여 있었다.")
    A("자릿수 검사로는 안 걸린다. 확정신고서의 **모집금액 ÷ 모집주식수**는 직접")
    A("명시된 두 값의 산술이라 신뢰도가 높으므로 1순위로 바꿨다(004550: 2009 →")
    A("실제 6,140원).")
    A("")
    A("**SC5** 비율이 정확히 1.0 인 건은 corroboration 없이 쓰지 않는다. 추정으로")
    A("정본을 만들지 않는다(§36).")
    A("")
    A("**SC6** (회귀 테스트가 잡음) 비율 패턴 하나가 라벨과 숫자 사이를 40자까지")
    A("건너뛰었다. 라벨이 '…배정비율 **산출 근거**' 라는 제목줄이면 그 뒤 첫 숫자인")
    A("**모집주식총수 11,000,000** 을 비율로 집었다. gap 에서 근거·총수·괄호를")
    A("배제하고, 1주당 배정비율이 0 < r ≤ 10 을 벗어나면 폐기하도록 했다. 대신")
    A("산식의 **결과줄**(`= 0.2969997056 주`)을 소수만 받도록 새로 인식해 명시값과")
    A("파생값이 서로를 검증하게 만들었다(상대오차 3e-12).")
    A("")
    A("**SC7** (회귀 테스트가 잡음) SC3 의 무상증자 문맥검사가, 바로 앞 문단이")
    A("무상증자 안내면 **'구주주' 로 시작하는 확실한 유상 배정근거 블록까지**")
    A("버렸다. 라벨만으로 유·무상이 구분되지 않는 블록에만 문맥검사를 적용한다.")
    A("")
    A("SC6·SC7 은 실데이터 11건의 판정 결과를 바꾸지 않았다(4건 복원 그대로).")
    A("결과를 바꾸려고 고친 것이 아니라, 합성 케이스로 드러난 오독 경로를 막은 것이다.")
    A("")

    A("## 5. 복원 결과 (§14·§15)")
    A("")
    A("```")
    for k, n in sorted(al["byAllocationStatus"].items(), key=lambda x: -x[1]):
        A(f"{KA.get(k, k):22} {n:4,}")
    A("```")
    A("")
    A("```")
    for k, n in sorted(ft["byWealthStatus"].items(), key=lambda x: -x[1]):
        A(f"{k:24} {n:4,}")
    A("```")
    A("")
    A("```")
    A("신뢰도    " + " · ".join(f"{k} {n}" for k, n in ft["byConfidence"].items()))
    A("비율 근거 " + " · ".join(f"{k} {n}" for k, n in al["byRatioBasis"].items()
                              if k))
    A("가격 출처 " + " · ".join(f"{k} {n}" for k, n in al["byPriceSource"].items()
                              if k))
    A("```")
    A("")
    A("```")
    FK = {"RATIO_EXACTLY_ONE_UNCORROBORATED": "비율 1.0 무근거 → 폐기",
          "R22_PRICE_DISCARDED_DISAGREES_REGISTRATION": "R22 발행가 폐기(신고서와 불일치)",
          "R22_PRICE_DISCARDED_IMPLAUSIBLE": "R22 발행가 폐기(자릿수 이상)",
          "RATIO_DATA_CONFLICT": "명시비율 vs 파생비율 불일치",
          "ALLOCATION_RATIO_MISMATCH": "배정량 vs 비율×eligible 불일치",
          "ELIGIBLE_OUT_OF_RANGE": "eligible 이 관측 주식수와 자릿수 불일치",
          "TREASURY_NOT_STATED": "자기주식 미기재",
          "NO_RIGHTS_SCOPE": "배정근거 블록 없음"}
    for k, n in sorted(al["flags"].items(), key=lambda x: -x[1]):
        A(f"{FK.get(k, k):36} {n:4,}")
    A("```")
    A("")

    A("## 6. 숫자 정합성 (§13)")
    A("")
    A("```")
    A(f"test1  {sr['test1']}")
    A(f"test2  {sr['test2']}")
    A(f"허용오차                 {sr['tolerance']}")
    A(f"test1 검사 / 통과        {sr['test1Checked']} / {sr['test1Pass']}")
    A(f"충돌                     {sr['conflicts']}")
    A("```")
    A("")
    A(f"{sr['onConflict']}")
    A("")

    A("## 7. manual ledger (§16)")
    A("")
    A("| 종목 | 시점 | eligible 구주 | 배정비율 | 배정주식 | 확정발행가 | "
      "외부납입 | 결과주식 | 권리가치 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]

        def f(x, fmt=",.0f"):
            return "-" if x is None else format(x, fmt)
        A(f"| {c['ticker']} | {c['date']} | {f(m['eligibleOldShares'])} | "
          f"{f(m['rightsRatio'], '.6f')} | {f(m['entitledShares'])} | "
          f"{f(m['finalIssuePrice'])} | {f(m['externalContribution'])} | "
          f"{f(m['resultingShares'], '.4f')} | {f(m['rightsEconomicValue'])} | "
          f"{m['manualTsrAdjustment'] * 100:.3f}% | "
          f"{c['engineTsrAdjustment'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    A(f"복원된 사건 전부 anchor 로 검증: **{an['allResolvedCovered']}** · "
      f"전부 manual 일치: **{an['allPass']}** · 정책 출처: {an['policySource']}")
    A("")
    if not an["atLeastFive"]:
        A(f"§16 은 최소 5건을 요구하지만 실제로 복원된 사건이 {an['obtained']}건뿐이다. "
          "복원되지 않은 건을 anchor 로 만들 수는 없으므로 **복원된 전부**를 검증했다.")
        A("")

    A("## 8. ex-rights 연속성 (§19 참조)")
    A("")
    A("```")
    A(f"검증 사건            {ct['rightsEventsTested']:5,}")
    A(f"당월 차분 중앙값      {ct['eventMonthMedianDiff'] * 100:6.2f}%")
    A(f"이론 권리가치         {ct['theoreticalMedian'] * 100:6.2f}%")
    A(f"이론과 일치           {ct['diffMatchesTheory']}")
    A(f"공짜 wealth           {ct['freeWealthEvents']:5,}건")
    A(f"연속성 판정           {'PASS' if ct['pass'] else 'FAIL'}")
    A("```")
    A("")

    A("## 9. 전체 2,742건 재분류 · materiality")
    A("")
    A("```")
    for k, n in sorted(rec["byWealthLayer"].items(), key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"미해결 고유종목      {mat['unresolvedTickers']:6,}")
    A(f"universe 종목        {mat['universeTickers']:6,}")
    A(f"미해결 종목비율      {mat['unresolvedTickerPct']:6.2f}%")
    A(f"미해결 사건비율      {mat['unresolvedEventPct']:6.2f}%")
    A(f"미해결 월 비율       {mat['unresolvedMonthPct']:6.2f}%")
    A(f"미해결 규모 비중     {mat['unresolvedMagnitudePct']:6.2f}%")
    A(f"극단 불연속          {mat['extremeDiscontinuityEvents']:6,} "
      f"({mat['extremeDiscontinuityPct']}%)")
    A("```")
    A("")
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 10. 기대편향 (§18·§21)")
    A("")
    A("```")
    A(f"공식              {bias['formula']}")
    A(f"공식 R22 동일     {bias['formulaUnchangedFromR22']}")
    A(f"조건부 크기       {bias['conditionalMedian'] * 100:.2f}%")
    A(f"미해결 사건       {bias['unresolvedEvents']:,}")
    A(f"유효 P            {bias['effectiveP'] * 100:.2f}%")
    A(f"기대편향          {bias['expectedBiasPct']}%   (기준 {bias['thresholdPct']}%)")
    A("```")
    A("")
    A(f"**R22 감도 대비**: {bias['projectionVsActual']}")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    A("**§21 누적 감도 — 주주배정 군에서 몇 건을 더 풀면 통과하는가**")
    A("")
    A("| 추가 해결 | 기대편향 | 2% 통과 |")
    A("|---|---|---|")
    for c in cum:
        A(f"| {c['resolvedFromGroup']}건 | {c['biasPct']}% | "
          f"{'○' if c['passesGate'] else '×'} |")
    A("")
    first = next((c for c in cum if c["passesGate"]), None)
    if first:
        A(f"**{first['resolvedFromGroup']}건만 더 복원되면 {first['biasPct']}% 로 "
          "게이트를 통과한다.**")
        A("")
    A("**군 전체 해결 감도**")
    A("")
    A("| 이 군을 전부 해결하면 | 건수 | 결과 편향 | 2% 통과 |")
    A("|---|---|---|---|")
    for r in gs["ifGroupFullyResolved"]:
        A(f"| {KO.get(r['resolveGroup'], r['resolveGroup'])} | {r['events']} | "
          f"{r['biasIfFullyResolvedPct']}% | {'○' if r['passesGate'] else '×'} |")
    A("")

    A("## 11. foundation 판정 (§19·§20)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16~R22 승계, 변경 없음):")
    A("")
    A("```")
    for k, val in v["thresholdsFromPrecommit"].items():
        A(f"{k:40} {val}")
    A(f"{'thresholdsUnchanged':40} {v['thresholdsUnchanged']}")
    A(f"{'biasFormulaUnchanged':40} {v['biasFormulaUnchanged']}")
    A("```")
    A("")
    A(f"**판정: {v['verdict']}**")
    A("")
    A(f"{tg['noResultEngineering']}")
    A("")

    A("## 12. 기존 연구 상태 (§24)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 13. production 보호 (§28)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 14. HOTG (§29)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 15. 한계")
    A("")
    A(f"- 11건 중 {al['byAllocationStatus'].get('NO_ALLOCATION_EVIDENCE', 0)}건은 "
      "증권신고서·투자설명서에서 배정량도 배정비율도 찾지 못했다. "
      f"{len(sf.get('tickersWithNoFiling') or [])}종목은 신고서 자체가 없다.")
    A(f"- {al['byAllocationStatus'].get('PARTIAL_EVIDENCE', 0)}건은 근거 일부만 "
      "있어 WEALTH_PARTIAL 로 남겼다. 계획치로 채우지 않았다(§36).")
    A("- 배정비율은 명시값 우선, 없으면 배정량÷eligible 파생이다. 둘이 충돌하면 "
      "DATA_CONFLICT 로 두고 억지로 고르지 않았다(§13).")
    A("- 유상감자·무상감자 구분(60건), 합병 교환비율, 인적분할 배정비율은 여전히 "
      "없다(R16~R22 한계 그대로).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 16. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R24**. 새 "
          "precommit 으로 Market · BM · EY · ROE · SIZE · Quality 를 canonical TSR "
          "로 처음부터 비교한다. R7~R14 의 승자·패자를 prior truth 로 쓰지 "
          "않는다(§34).")
    else:
        A("- 다음 단일 작업: **완전 미확인 50건 직접판정**(§35).")
        A("")
        A("  §22 대로 11건 경로는 여기서 닫는다 — 남은 7건은 증권신고서가 없거나")
        A("  배정근거가 실려 있지 않아 direct evidence 확보가 불가능했다. 추정으로")
        A("  채우지 않는다.")
        A("")
        tu = bias["decomposition"].get("TRUE_UNRESOLVED", {})
        A(f"  완전 미확인 군은 {tu.get('events', 0)}건이고 아직 **모집단 추정치**")
        A(f"  (P={tu.get('avgP', 0)})에 의존한다. 전체 해결 시 감도상 "
          f"{next((r['biasIfFullyResolvedPct'] for r in gs['ifGroupFullyResolved'] if r['resolveGroup'] == 'TRUE_UNRESOLVED'), '-')}% 로 "
          "게이트를 통과한다.")
        A("")
        A("  다만 R21 에서 확인했듯 '권리 없음' 확정은 분모를 줄여 지표를 되레")
        A("  올릴 수 있다. 예단하지 않는다.")
        A("")
        if first:
            A(f"  참고: 주주배정 군에서 **{first['resolvedFromGroup']}건만** 더")
            A(f"  복원돼도 {first['biasPct']}% 로 통과한다. 그러나 그 7건은 이미")
            A("  direct evidence 를 소진했다.")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · 포트폴리오 설계 · 감자 유무상 구분 · "
      "지표 재정의 · 홈페이지 공개 · 실계좌 자금")
    A("")
    A(f"- §23 — foundation 이 PASS 계열이 된 뒤에만 factor 연구를 재개한다. "
      f"현재: **{v['factorResearchAllowed']}**")
    return "\n".join(o) + "\n"


def main() -> int:
    d = build()
    WD.mkdir(parents=True, exist_ok=True)
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"),
                      "verdict": d["foundationVerdict"]["verdict"],
                      "verdictLine": d["verdictLine"],
                      "expectedBiasPct": d["expectedBias"]["expectedBiasPct"],
                      "unresolvedTickerPct": d["materiality"]["unresolvedTickerPct"],
                      "factorAllowed":
                      d["foundationVerdict"]["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
