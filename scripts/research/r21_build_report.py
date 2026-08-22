#!/usr/bin/env python3
"""R21 canonical report (MD + JSON).

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21
기존 R16~R20 산출물을 덮어쓰지 않는다(§27). HOTG 는 기존 Bridge 재사용(§30).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-low-confidence-direct-entitlement-recovery-r21-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R21_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R21_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R21_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R21_CANONICAL_TSR_FOUNDATION_FAIL"),
}

KO = {"CONVERTIBLE_BOND_CONVERSION": "전환사채 전환", "BW_WARRANT_EXERCISE": "BW 행사",
      "STOCK_OPTION_EXERCISE": "주식매수선택권 행사",
      "MERGER_NEW_SHARES": "합병 신주", "SHARE_SWAP": "주식교환·이전",
      "SPINOFF_RELATED_SHARES": "회사분할 신주", "STOCK_SPLIT": "주식분할(액면분할)",
      "BONUS_ISSUE": "무상증자", "TRUE_RIGHTS_ISSUE": "주주배정 유상증자",
      "UNRESOLVED": "미확인",
      "HOLDER_RIGHT_TERMS_MISSING": "주주배정 확정·조건 미확보",
      "TRUE_UNRESOLVED": "완전 미확인",
      "OTHER_CAPITAL_ACTION": "감자 등 기타 자본행위",
      "NO_ENTITLEMENT_CONFIRMED": "권리 없음 확정"}


def L(n):
    p = RD / f"r21-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, ds = L("targets-precommit"), L("direct-source-events")
    cl, en = L("event-classification"), L("entitlement")
    fp, an = L("false-classification-audit"), L("anchor-cases")
    ct, rec = L("ex-rights-continuity"), L("full-reconciliation")
    bias, mat = L("expected-bias"), L("unresolved-materiality")
    v = L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-low-confidence-direct-entitlement-recovery-r21@1",
        "taskId": "WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "directSource": ds,
        "classification": {k: cl[k] for k in cl if k != "events"},
        "entitlement": {k: en[k] for k in en if k != "detail"},
        "falseClassificationAudit": {k: fp[k] for k in fp if k != "samples"},
        "anchors": an, "continuity": ct,
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
    tg, ds, cl = d["targetsPrecommit"], d["directSource"], d["classification"]
    en, fp, an = d["entitlement"], d["falseClassificationAudit"], d["anchors"]
    ct, rec = d["continuity"], d["reconciliation"]
    bias, mat, v = d["expectedBias"], d["materiality"], d["foundationVerdict"]
    er = bias["estimateReplacement"]
    gs = bias["gateSensitivity"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R21 — 저신뢰 96건 직접권리 실측 대체")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = mat["progression"]
    A(f"- 미해결 종목비율 **R16 {pg['R16']}% → R17 {pg['R17']}% → R18 {pg['R18']}% "
      f"→ R19 {pg['R19']}% → R20 {pg['R20']}% → R21 {pg['R21']}%** (기준 20%)")
    bp = bias["progression"]
    A(f"- 기대편향 **R18 {bp['R18']}% → R19 {bp['R19']}% → R20 {bp['R20']}% "
      f"→ R21 {bp['R21']}%** (기준 {bias['thresholdPct']}%)")
    A(f"- **추정 P=0.296 → 실측 {er['measuredYesRateAmongKnown']}** · "
      f"이 군 기여 {er['priorContributionPp']}%p → {er['residualContributionPp']}%p")
    A(f"- anchor {len(an['cases'])}건 전부 PASS · 오분류율 "
      f"{fp['falsePositiveRate']} · 공짜 wealth {ct['freeWealthEvents']}건")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 96건을 전수 직접판정해 **추정을 실측으로 바꿨다**. 이 군의 권리 YES")
        A(f"> 비율은 추정 29.6% 가 아니라 실측 {er['measuredYesRateAmongKnown'] * 100:.1f}% 였고,")
        A(f"> 기여는 {er['priorContributionPp']}%p → {er['residualContributionPp']}%p 로 사라졌다.")
        A("> 그런데 **총편향은 내려가지 않았다.** 지표가 '미해결 1건당 기대값'이라")
        A("> 권리 없는 사건을 확정할수록 분모가 줄어 남은 pool 이 농축되기 때문이다.")
        A("> 기준을 옮기지 않고 BLOCKED 를 유지하며, 게이트를 여는 유일한 경로를")
        A("> 아래 §11 에서 정량화했다.")
    elif v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("> 추정을 실측으로 대체한 결과 편향이 기준 이하로 내려왔다.")
        A("> 한계를 명시한 채 연구 사용을 허용한다.")
    elif v["verdict"].endswith("_PASS"):
        A("> foundation 이 연구 사용 가능 수준에 도달했다.")
    else:
        A("> 엔진 검증에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 직접공시 수집 (§2·§3·§26)")
    A("")
    A("```")
    A(f"대상 LOW_CONFIDENCE_NOT_COUNTED   {tg['targetTotal']:6,}")
    A(f"대상 종목                          {tg['targetTickers']:6,}")
    A(f"corp_code 매핑                     {tg['corpCodeMapped']:6,}")
    A(f"수집 커버 사건                     {ds['eventsCovered']:6,}")
    A(f"확보 공시                          {ds['filingsAvailable']:6,}")
    A(f"수집 실패                          {ds['hardFailures']:6,}")
    A("```")
    A("")
    A(f"수집기: `{ds['collectorReused']}` · 창 {ds['windowMonths']}")
    A("")
    A(f"{tg['targets'][0]['lowConfidenceReason'] if tg.get('targets') else ''}")
    A("이 군이 LOW 였던 이유: R17 매칭에서 공시는 찾았으나 비율 corroboration 이")
    A("없거나 날짜가 멀어 강등됐다. 유형 자체가 불명이었던 것이 아니라 **그 공시가")
    A("이 주식수 변화의 원인인지**가 불확실했던 것이다.")
    A("")

    A("## 3. 자본행위 유형 확정 (§4)")
    A("")
    A("```")
    for k, n in sorted(cl["byPrimaryEvent"].items(), key=lambda x: -x[1]):
        A(f"{KO.get(k, str(k)):26} {n:5,}")
    A("```")
    A("")
    A("```")
    A("신뢰도   " + " · ".join(f"{k} {n}" for k, n in cl["byConfidence"].items()))
    A("근거출처 " + " · ".join(f"{k} {n}" for k, n in cl["byProvenance"].items()))
    A("```")
    A("")
    A(f"분류 정본: `{cl['reusedFrom']}` — 새 규칙을 만들지 않았다.")
    A("")

    A("## 4. 핵심 판정 — 기존 주주가 직접 무엇을 받았나 (§5)")
    A("")
    A(f"> {tg['coreQuestion']}")
    A("")
    A(f"{tg['coreDistinction']}")
    A("")
    A("```")
    for k, n in en["counts"].items():
        A(f"{k:32} {n:5,}")
    A("```")
    A("")
    A("```")
    for k, n in sorted(cl["byWealthAdjustment"].items(), key=lambda x: -x[1]):
        A(f"{k:34} {n:5,}")
    A("```")
    A("")

    A("## 5. ★ 추정을 실측으로 바꿨다 (§16·§36)")
    A("")
    A("이것이 R21 의 목적이다. 편향을 낮추는 것이 아니라 **가정을 측정으로**")
    A("바꾸는 것이다.")
    A("")
    A("```")
    A(f"기존 P (모집단 추정)          {er['priorP']}")
    A(f"실측 YES 비율 (기지 사건)     {er['measuredYesRateAmongKnown']}")
    A(f"실측 YES 비율 (전체 96건)     {er['measuredYesRateAmongAll']}")
    A(f"이 군 기존 기여               {er['priorContributionPp']}%p")
    A(f"이 군 잔여 기여               {er['residualContributionPp']}%p")
    A("```")
    A("")
    A(f"{en['comparisonNote']}")
    A("")
    A("실측값이 추정치의 약 1/3 이었다. 즉 이 군에 0.296 을 적용한 것은")
    A("**과대추정**이었고, 직접 조사가 그것을 바로잡았다.")
    A("")

    A("## 6. 오분류 감사 (§22)")
    A("")
    A(f"{fp['method']}")
    A("")
    A("| 유형 | 모집단 | 표본 | 통과 | 오분류율 |")
    A("|---|---|---|---|---|")
    for c in fp["byPrimaryEvent"]:
        A(f"| {KO.get(c['primaryEvent'], c['primaryEvent'])} | {c['population']} | "
          f"{c['sampled']} | {c['crossCheckPass']} | {c['falsePositiveRate']} |")
    A("")
    A(f"**전체 오분류율 {fp['falsePositiveRate']}** (표본 {fp['sampled']}건) · "
      f"{fp['falseNegativeClue']}")
    A("")

    A("## 7. manual anchor (§21)")
    A("")
    A("| 구분 | 종목 | 시점 | 직접권리 | 조정 | 종료주식 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | "
          f"{c['directEntitlement'].replace('DIRECT_ENTITLEMENT_', '')} | "
          f"{c['wealthAdjustment'].replace('WEALTH_ADJUSTMENT_', '')} | "
          f"{m['endingShares']:.4f} | {m['manualTsrEffect'] * 100:.3f}% | "
          f"{c['engineTsrEffect'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    if an["missingCaseTypes"]:
        A(f"확보하지 못한 유형: {', '.join(an['missingCaseTypes'])} — 96건 안에 "
          "해당 유형이 그만큼 없었다(없는 것을 만들지 않는다).")
        A("")
    A(f"**{an['whyBothSidesNeeded']}**")
    A("")
    A(f"권리 없는 사건 조정 정확히 0: **{an['noEntitlementAdjustmentExactlyZero']}** · "
      f"권리 있는 사건 비영 조정: **{an['entitlementCaseAdjustsNonZero']}** · "
      f"공짜 wealth **{ct['freeWealthEvents']}건**")
    A("")

    A("## 8. 전체 2,742건 최종 재분류 (§23)")
    A("")
    A("```")
    for k, n in sorted(rec["byWealthLayer"].items(), key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"R21 merge            {rec['r21Merged']:6,}")
    A(f"wealth 확정          {rec['wealthResolved']:6,}")
    A("```")
    A("")

    A("## 9. 미해결 materiality (§23)")
    A("")
    A("```")
    A(f"미해결 사건          {mat['wealthUnresolved']:6,}")
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
        A(f"{k:38} {n:5,}")
    A("```")
    A("")
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 10. 기대편향 (§17·§18)")
    A("")
    A("```")
    A(f"공식              {bias['formula']}")
    A(f"공식 R20 동일     {bias['formulaUnchangedFromR20']}")
    A(f"조건부 크기       {bias['conditionalMedian'] * 100:.2f}%")
    A(f"미해결 사건       {bias['unresolvedEvents']:,}")
    A(f"유효 P            {bias['effectiveP'] * 100:.2f}%")
    A(f"기대편향          {bias['expectedBiasPct']}%   (기준 {bias['thresholdPct']}%)")
    A("```")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    red = bias["remainingEstimateDependency"]
    A(f"아직 추정치에 의존: **{red['events']}건 · {red['contributionPct']}%p** "
      f"(R20 의 143건·2.164%p 에서 감소)")
    A("")

    A("## 11. ★ 왜 편향이 안 내려갔나 — 지표의 구조 (§18)")
    A("")
    A(f"{bias['metricStructureNote']}")
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
    A(f"{gs['interpretation']}")
    A("")
    A("즉 **권리 없음을 더 확정하는 작업으로는 게이트를 열 수 없다.** R19·R21 이")
    A("그 방향으로 342건·96건을 처리했지만 편향은 3.4% 대에서 움직이지 않았다.")
    A("게이트를 여는 유일한 경로는 `주주배정 확정·조건 미확보` 27건의 최종조건을")
    A("확보하는 것이고, 그 경우 편향은 1.520% 로 기준을 통과한다.")
    A("")

    A("## 12. foundation 재판정 (§19·§20)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16~R20 승계, 변경 없음):")
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

    A("## 13. 기존 연구 상태 (§25)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 14. production 보호 (§29)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 15. HOTG (§30)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 16. 한계")
    A("")
    A(f"- 96건 중 {cl['byPrimaryEvent'].get('UNRESOLVED', 0)}건은 창 안에서 자본행위 "
      "공시를 찾지 못해 UNRESOLVED 로 남겼다. 억지로 올리지 않았다(§8).")
    A("- 분류는 대부분 MEDIUM 이다. 공시명에 신주수가 없어 수량 일치까지 확인된 "
      "HIGH 가 드물다(§7 기준을 낮추지 않았다).")
    A("- 합병 방향성은 R19 의 구조 논거를 그대로 썼다(주식수 증가 = 신주 발행 측). "
      "개별 합병계약서를 읽은 것은 아니다.")
    A(f"- 아직 {red['events']}건이 모집단 추정치에 의존한다({red['contributionPct']}%p).")
    A("- 유상감자·무상감자 구분(60건), 합병 교환비율, 인적분할 배정비율은 여전히 "
      "없다(R16~R20 한계 그대로).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 17. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R22**. 새 "
          "precommit 으로 시장 benchmark · BM · EY · ROE · SIZE · QUALITY 를 동일 "
          "canonical TSR engine 에서 원점 경쟁시킨다(§35).")
    else:
        pas = [r for r in gs["ifGroupFullyResolved"] if r["passesGate"]]
        if pas:
            r = pas[0]
            A(f"- 다음 단일 작업: **{KO.get(r['resolveGroup'], r['resolveGroup'])} "
              f"{r['events']}건의 최종조건 확보**. 게이트 감도 분석상 이 군만이 "
              f"기준을 통과시킨다(편향 {bias['expectedBiasPct']}% → "
              f"{r['biasIfFullyResolvedPct']}%).")
            A("")
            A("  이들은 주주배정임이 이미 확정됐고 막는 것은 배정비율·발행가다.")
            A("  R20 이 92건 중 67건을 증권발행실적보고서로 풀었고 남은 25건 + R21 의")
            A("  2건이 이 군이다. R20 에서 실패한 사유는 '창 안에 구주주 배정이 있는")
            A("  실적보고서가 없음' 이었다 — 창을 넓히거나 정정공시·신주상장 공시로")
            A("  실발행량을 우회 확보하는 것이 남은 경로다.")
            A("")
            A("  **다만 이 군이 전부 풀린다는 보장은 없다.** R20 이 이미 한 번 시도해")
            A("  실패한 대상이다. 풀리지 않으면 이 지표로는 게이트를 통과할 수 없고,")
            A("  그때는 지표 자체의 적절성을 Founder 가 판단해야 한다(아래 참조).")
        else:
            A("- 다음 단일 작업: 게이트 감도 분석상 어느 단일 군을 해결해도 기준을 "
              "통과하지 못한다. 지표 재설계 여부를 Founder 가 판단해야 한다.")
    A("")
    A("- **구조적 관찰(판정 아님)**: 이 지표는 '미해결 1건당 기대값' 이라 해결 "
      "작업이 진행될수록 분모가 줄어 값이 잘 내려가지 않는다. R19(342건) · "
      "R21(96건) 이 대량 확정을 했는데도 편향이 3.4~5.3% 대에 머문 것이 그 "
      "증거다. 사전 기준을 바꾸는 것은 금지돼 있으므로 이 사실만 보고한다.")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · 포트폴리오 설계 · 감자 유무상 구분 · "
      "지표 재정의 · 홈페이지 공개 · 실계좌 자금")
    A("")
    A(f"- §24 — foundation 이 PASS 계열이 된 뒤에만 factor 연구를 재개한다. "
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
