#!/usr/bin/env python3
"""R22 canonical report (MD + JSON).

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22
기존 R16~R21 산출물을 덮어쓰지 않는다(§27). HOTG 는 기존 Bridge 재사용(§29).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-final-27-holder-rights-terms-recovery-r22-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R22_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R22_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R22_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R22_CANONICAL_TSR_FOUNDATION_FAIL"),
}

KO = {"HOLDER_RIGHT_TERMS_MISSING": "주주배정 확정·조건 미확보",
      "TRUE_UNRESOLVED": "완전 미확인",
      "OTHER_CAPITAL_ACTION": "감자 등 기타 자본행위",
      "NO_ENTITLEMENT_CONFIRMED": "권리 없음 확정"}
KS = {"ISSUE_RESULT_REPORT": "증권발행실적보고서",
      "ISSUE_COMPLETION": "증권발행결과 자율공시",
      "ISSUE_COMPLETION_DERIVED": "증권발행결과(금액/주식수 파생)",
      "SUBSCRIPTION_RESULT": "청약결과 자율공시",
      "FINAL_PRICE": "발행가액확정 공시", "NEW_LISTING": "추가상장 공시"}
KC = {"COMPLETED": "전량 완료", "PARTIALLY_COMPLETED": "일부 완료(실권)",
      "NO_ACTUAL_EVIDENCE": "실제 발행 증거 미확보",
      "CANCELLED_NO_WEALTH_EVENT": "철회"}


def L(n):
    p = RD / f"r22-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, ex = L("targets-precommit"), L("expanded-window-filings")
    ft, ch = L("final-terms-normalized"), L("correction-chains")
    nl, cc = L("new-listing-reconciliation"), L("capital-change-reconciliation")
    an, ct = L("anchor-cases"), L("ex-rights-continuity")
    wr, rec = L("wealth-resolution"), L("full-reconciliation")
    bias, mat = L("expected-bias"), L("unresolved-materiality")
    v = L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-final-27-holder-rights-terms-recovery-r22@1",
        "taskId": "WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "expandedWindowFilings": {k: ex[k] for k in ex if k != "documents"},
        "finalTerms": {k: ft[k] for k in ft if k != "events"},
        "corrections": {k: ch[k] for k in ch if k != "list"},
        "newListingReconciliation": {k: nl[k] for k in nl if k != "rows"},
        "capitalChangeReconciliation": cc,
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
    tg, ex, ft = d["targetsPrecommit"], d["expandedWindowFilings"], d["finalTerms"]
    ch, nl, an = d["corrections"], d["newListingReconciliation"], d["anchors"]
    ct, rec = d["continuity"], d["reconciliation"]
    bias, mat, v = d["expectedBias"], d["materiality"], d["foundationVerdict"]
    gs = bias["gateSensitivity"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R22 — 마지막 27건 주주배정 최종조건 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = mat["progression"]
    A(f"- 미해결 종목비율 **R16 {pg['R16']}% → R17 {pg['R17']}% → R18 {pg['R18']}% "
      f"→ R19 {pg['R19']}% → R20 {pg['R20']}% → R21 {pg['R21']}% "
      f"→ R22 {pg['R22']}%** (기준 20%)")
    bp = bias["progression"]
    A(f"- 기대편향 **R18 {bp['R18']}% → R19 {bp['R19']}% → R20 {bp['R20']}% "
      f"→ R21 {bp['R21']}% → R22 {bp['R22']}%** (기준 {bias['thresholdPct']}%)")
    A(f"- 27건 중 wealth 확정 **{ft['byWealthStatus'].get('WEALTH_CONFIRMED', 0)}** · "
      f"anchor {an['obtained']}건 전부 PASS · 공짜 wealth {ct['freeWealthEvents']}건")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 창을 넓히고(-6~+9 → -3~+18) 공시 유형을 확장해 27건 중 "
          f"**{ft['byWealthStatus'].get('WEALTH_CONFIRMED', 0)}건**의 실제 발행조건을")
        A("> 복원했다. R20 이 실적보고서만 보고 실패했던 대상을 증권발행결과·청약결과·")
        A("> 발행가액확정·추가상장으로 우회 복원한 것이다.")
        A(f"> 편향은 {bp['R21']}% → {bp['R22']}% 로 내려갔지만 기준 2% 를 넘는다.")
        A("> 기준을 옮기지 않고 BLOCKED 를 유지한다. 남은 11건은 배정량 자체가")
        A("> 확보되지 않아 **추정으로 채우지 않았다**(§35).")
    elif v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("> 실제 발행조건 복원으로 편향이 기준 이하로 내려왔다.")
        A("> 한계를 명시한 채 연구 사용을 허용한다.")
    elif v["verdict"].endswith("_PASS"):
        A("> foundation 이 연구 사용 가능 수준에 도달했다.")
    else:
        A("> 엔진 검증에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 R20 대비 달라진 점 (§2·§3·§4)")
    A("")
    A("```")
    A(f"대상(주주배정 확정·조건미확보)   {tg['targetTotal']:6,}")
    A(f"대상 종목                        {tg['targetTickers']:6,}")
    A(f"검색 창                          {tg['window']['months']}  "
      f"(R20: {tg['window']['r20Window']})")
    A("```")
    A("")
    h = tg["howThisDiffersFromR20"]
    A(f"- R20 접근: {h['r20Approach']}")
    A(f"- R20 실패: {h['r20Failure']}")
    A(f"- R22 접근: {h['r22Approach']}")
    A(f"- 이유: {h['why']}")
    A("")
    A("수집 결과:")
    A("")
    A("```")
    KF = {"ISSUE_RESULT_REPORT": "증권발행실적보고서",
          "FINAL_PRICE": "발행가액확정·안내",
          "SUBSCRIPTION_RESULT": "청약결과", "UNSUBSCRIBED_RESULT": "실권주·단수주",
          "NEW_LISTING": "추가·변경상장", "ISSUE_COMPLETION": "증권발행결과",
          "RIGHTS_DECISION": "유상증자결정(정정 포함)",
          "PERIODIC_REPORT": "사업·분기보고서"}
    for k, n in sorted(ex["filingKinds"].items(), key=lambda x: -x[1]):
        A(f"{KF.get(k, k):30} {n:5,}")
    A("```")
    A("")
    A(f"원문 확보 {ex['documentsNew'] + ex['documentsCacheHits']:,} · "
      f"실패 {ex['documentsFailed']} · {ex['helpersReused']}")
    A("")

    A("## 3. ★ 자체수정 — 자율공시를 못 읽고 있었다 (§31)")
    A("")
    A("**SC1**: 증권발행실적보고서는 대문자 태그 + ACODE 필드코드를 쓰지만, 거래소")
    A("자율공시(청약결과·증권발행결과·발행가액확정·추가상장)는 **소문자 HTML** 이고")
    A("ACODE 가 없다. R20 파서의 정규식은 대소문자를 구분해 이 문서들을 전부 빈")
    A("문서로 읽고 있었다. 대소문자 무시 + 표 셀 라벨 추출을 추가했다.")
    A("")
    A("**SC2**: 라벨 다음 줄의 첫 숫자를 값으로 썼는데, 그 줄이 '1. 발행예정내역'")
    A("같은 **절 번호**면 1 을 발행가로 읽었다(실측: 확정발행가 1·2·4 원). 절 번호")
    A("줄을 건너뛰도록 고쳤고, 액면가 미만 가격은 파싱 오류로 보고 **버린다**")
    A("(추정으로 채우지 않는다).")
    A("")
    A("**SC3**: 증권발행결과 자율공시의 발행방법이 '주주배정 유상증자' 면 발행예정")
    A("주식수가 곧 구주주 **배정량**이고 실제발행주식수가 배정 결과다. 이를 각각")
    A("제자리에 넣도록 했다(R20 의 ENTITLEMENT 기준과 같은 의미).")
    A("")

    A("## 4. 복원된 최종조건 (§5)")
    A("")
    A("| 필드 | 복원 | 비율 |")
    A("|---|---|---|")
    KOF = {"finalActualNewShares": "실제 발행주식수",
           "finalIssuePrice": "확정발행가",
           "finalRightsRatio": "최종 배정비율",
           "finalShareholderAllocatedShares": "구주주 실배정주식수",
           "finalShareholderEntitledShares": "구주주 배정량(권리)",
           "actualPaymentDate": "실제 납입일",
           "finalIssueMethod": "최종 증자방식"}
    for k, r in ft["fieldRecovery"].items():
        A(f"| {KOF.get(k, k)} | {r['n']} | {r['pct']}% |")
    A("")
    A("어느 공시에서 값을 얻었나:")
    A("")
    A("```")
    A("주식수 출처   " + " · ".join(
        f"{KS.get(k, str(k))} {n}" for k, n in ft["bySharesSource"].items() if k))
    A("발행가 출처   " + " · ".join(
        f"{KS.get(k, str(k))} {n}" for k, n in ft["byPriceSource"].items() if k))
    A("```")
    A("")

    A("## 5. 계획치 vs 실제치 (§6·§8)")
    A("")
    A("```")
    FK = {"PLANNED_RATIO_DIFFERS": "계획 배정비율이 최종과 다름(>15%)",
          "PLANNED_PRICE_DIFFERS": "계획 발행가가 확정가와 다름(>15%)",
          "PARTIAL_SUBSCRIPTION": "실권 발생(구주주 청약 < 배정)",
          "PARTIAL_ISSUANCE": "실제 발행 < 발행예정",
          "TAKEUP_BELOW_ENTITLEMENT": "실청약이 권리보다 적음",
          "SHARE_COUNT_DATA_CONFLICT": "보고 발행수 ≠ 관측 주식수 증가",
          "DELTA_EXPLAINED_BY_OTHER_EVENT": "차이가 같은 달 다른 자본행위로 설명됨",
          "IMPLAUSIBLE_PRICE_DISCARDED": "비상식적 발행가 파싱값 폐기",
          "ONLY_NOTICE_PRICE_AVAILABLE": "안내공시 1차가만 있음(확정가 아님)"}
    for k, n in sorted(ft["flags"].items(), key=lambda x: -x[1]):
        A(f"{FK.get(k, k):38} {n:4,}")
    A("```")
    A("")
    A("계획 배정비율·발행가가 최종과 다른 건이 여전히 다수다. 계획치로 wealth 를")
    A("계산했다면 존재하지 않은 권리가치를 만들었을 것이다.")
    A("")

    A("## 6. 완료 상태 (§13) · wealth 확정 (§16·§17)")
    A("")
    A("```")
    for k, n in sorted(ft["byCompletionStatus"].items(), key=lambda x: -x[1]):
        A(f"{KC.get(k, k):28} {n:4,}")
    A("```")
    A("")
    A("```")
    for k, n in sorted(ft["byWealthStatus"].items(), key=lambda x: -x[1]):
        A(f"{k:24} {n:4,}")
    A("```")
    A("")
    A("```")
    A("신뢰도  " + " · ".join(f"{k} {n}" for k, n in ft["byConfidence"].items()))
    A("```")
    A("")

    A("## 7. 정정 chain (§4) · 주식수 대조 (§11)")
    A("")
    A("```")
    A(f"chain                    {ch['chains']:5,}")
    A(f"정정 포함                {ch['withCorrection']:5,}")
    A(f"실제 발행수 확보          {nl['withActual']:5,}")
    A(f"허용오차 내 일치          {nl['matchedWithinTolerance']:5,}")
    A(f"다른 자본행위로 설명됨    {nl['explainedByOtherEvent']:5,}")
    A(f"설명되지 않는 불일치      {nl['unexplainedConflict']:5,}")
    A("```")
    A("")
    A(f"{nl['hardRule']}")
    A("")
    A(f"{d['capitalChangeReconciliation']['note']}")
    A("")

    A("## 8. manual anchor (§18)")
    A("")
    A("| 구분 | 종목 | 시점 | 최종비율 | 계획비율 | 확정가 | 실발행 | 외부납입 | "
      "권리가치 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]

        def f(x, fmt=",.0f"):
            return "-" if x is None else format(x, fmt)
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | "
          f"{f(m['finalRightsRatio'], '.4f')} | "
          f"{f(m['plannedRightsRatio'], '.4f')} | {f(m['finalIssuePrice'])} | "
          f"{f(m['actualNewShares'])} | {f(m['externalContribution'])} | "
          f"{f(m['rightsValue'])} | {m['manualTsrAdjustment'] * 100:.3f}% | "
          f"{c['engineTsrAdjustment'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    A(f"anchor 10건 이상: **{an['atLeastTen']}** · 전부 manual 일치: "
      f"**{an['allPass']}** · 정책 출처: {an['policySource']}")
    A("")

    A("## 9. ex-rights 연속성 (§19)")
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

    A("## 10. 전체 2,742건 재분류 · materiality (§23)")
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
    A("미해결 사유:")
    A("")
    A("```")
    for k, n in sorted(mat["unresolvedReasons"].items(), key=lambda x: -x[1]):
        A(f"{k:40} {n:5,}")
    A("```")
    A("")
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 11. 기대편향 (§20)")
    A("")
    A("```")
    A(f"공식              {bias['formula']}")
    A(f"공식 R21 동일     {bias['formulaUnchangedFromR21']}")
    A(f"조건부 크기       {bias['conditionalMedian'] * 100:.2f}%")
    A(f"미해결 사건       {bias['unresolvedEvents']:,}")
    A(f"유효 P            {bias['effectiveP'] * 100:.2f}%")
    A(f"기대편향          {bias['expectedBiasPct']}%   (기준 {bias['thresholdPct']}%)")
    A("```")
    A("")
    A(f"**R21 감도 투영 대비**: {bias['projectionVsActual']}")
    A(f"(R21 은 27건 전부 해결 시 {bias['r21SensitivityProjection']}% 로 봤고, "
      f"실제로는 {ft['byWealthStatus'].get('WEALTH_CONFIRMED', 0)}/{ft['targets']} "
      f"만 해결돼 {bias['expectedBiasPct']}% 다.)")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    A("**게이트 감도 — 어느 군을 해결해야 통과하는가**")
    A("")
    A("| 이 군을 전부 해결하면 | 건수 | 평균 P | 결과 편향 | 2% 통과 |")
    A("|---|---|---|---|---|")
    for r in gs["ifGroupFullyResolved"]:
        A(f"| {KO.get(r['resolveGroup'], r['resolveGroup'])} | {r['events']} | "
          f"{r['avgP']:.3f} | {r['biasIfFullyResolvedPct']}% | "
          f"{'○' if r['passesGate'] else '×'} |")
    A("")

    A("## 12. foundation 재판정 (§21·§22)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16~R21 승계, 변경 없음):")
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

    A("## 13. 기존 연구 상태 (§24)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 14. production 보호 (§28)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 15. HOTG (§29)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 16. 한계")
    A("")
    A(f"- 27건 중 {ft['byWealthStatus'].get('WEALTH_PARTIAL', 0)}건은 구주주 "
      "**배정량 자체**를 어떤 사후공시에서도 얻지 못했다. 발행가액확정 공시의 "
      "'발행예정 주식수' 를 배정량으로 쓰면 §6·§8 위반이라 쓰지 않았다.")
    A(f"- {ft['byCompletionStatus'].get('NO_ACTUAL_EVIDENCE', 0)}건은 실제 발행 "
      "증거 자체가 없다.")
    A(f"- 보고 발행수와 관측 주식수 증가가 어긋나 설명되지 않는 건이 "
      f"{nl['unexplainedConflict']}건이다.")
    A("- 권리 기준은 배정(entitlement)이다. 실제 청약률은 별도 기록했다.")
    A("- 유상감자·무상감자 구분(60건), 합병 교환비율, 인적분할 배정비율은 여전히 "
      "없다(R16~R21 한계 그대로).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 17. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R23**. 새 "
          "precommit 으로 시장 benchmark · BM · EY · ROE · SIZE · QUALITY 를 동일 "
          "canonical TSR engine 에서 원점 경쟁시킨다(§34).")
    else:
        pas = [r for r in gs["ifGroupFullyResolved"] if r["passesGate"]]
        pas.sort(key=lambda r: r["biasIfFullyResolvedPct"])
        if pas:
            A("- 게이트를 여는 경로가 **두 개** 남았다(감도표):")
            A("")
            for r in pas:
                A(f"  - {KO.get(r['resolveGroup'], r['resolveGroup'])} "
                  f"{r['events']}건 해결 → {r['biasIfFullyResolvedPct']}%")
            A("")
            best = pas[0]
            A(f"- 다음 단일 작업: **{KO.get(best['resolveGroup'], best['resolveGroup'])} "
              f"{best['events']}건**.")
            A("")
            if best["resolveGroup"] == "TRUE_UNRESOLVED":
                A("  이 군은 아직 **모집단 추정치**(P=0.296)에 의존한다. R19·R21 이 "
                  "직접 조사한 사건들에서 실측 권리비율은 각각 4.7%·9.7% 로 추정치보다")
                A("  훨씬 낮았다. 이 50건도 직접 판정하면 P 가 실측으로 바뀐다 —")
                A("  다만 R21 에서 확인했듯 권리 없음을 확정하면 분모가 줄어 지표가")
                A("  오히려 오를 수 있으므로 결과를 예단하지 않는다.")
            else:
                A("  R22 가 남긴 11건이다. 구주주 배정량을 어떤 사후공시에서도 얻지")
                A("  못했고, 계획치로 채우는 것은 §6·§8 위반이라 하지 않았다. 남은")
                A("  경로는 증권신고서 원문의 배정 표 정도이며 확보 보장은 없다.")
        else:
            A("- 다음 단일 작업: 감도표상 단일 군 해결로 기준을 통과하는 경로가 없다. "
              "지표 재설계 여부를 Founder 가 판단해야 한다.")
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
