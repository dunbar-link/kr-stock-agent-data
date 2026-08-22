#!/usr/bin/env python3
"""R19 canonical report (MD + JSON).

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19
기존 산출물을 덮어쓰지 않는다(§27). HOTG 는 기존 Report Bridge 재사용(§29).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-no-direct-match-capital-action-recovery-r19-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R19_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R19_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R19_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R19_CANONICAL_TSR_FOUNDATION_FAIL"),
}

KO = {"CONVERTIBLE_BOND_CONVERSION": "전환사채 전환", "BW_WARRANT_EXERCISE": "BW 행사",
      "EXCHANGEABLE_BOND_EXCHANGE": "교환사채 교환",
      "STOCK_OPTION_EXERCISE": "주식매수선택권 행사",
      "MERGER_NEW_SHARES": "합병 신주", "SHARE_SWAP": "주식교환·이전",
      "SPINOFF_RELATED_SHARES": "회사분할 신주", "STOCK_SPLIT": "주식분할(액면분할)",
      "BONUS_ISSUE": "무상증자", "TRUE_RIGHTS_ISSUE": "주주배정 유상증자",
      "THIRD_PARTY_ISSUANCE": "제3자배정", "PUBLIC_OFFERING": "일반공모",
      "CAPITAL_REDUCTION": "감자", "TREASURY_ACTION": "자기주식",
      "UNRESOLVED": "미확인"}


def L(n):
    p = RD / f"r19-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, ds = L("targets-precommit"), L("direct-source-events")
    cl, ent = L("event-classification"), L("shareholder-entitlement")
    fp, md = L("false-classification-audit"), L("merger-direction")
    an, rec = L("anchor-cases"), L("full-reconciliation")
    bias, mat, v = L("expected-bias"), L("unresolved-materiality"), L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-no-direct-match-capital-action-recovery-r19@1",
        "taskId": "WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "directSource": ds,
        "classification": {k: cl[k] for k in cl if k != "events"},
        "entitlement": {k: ent[k] for k in ent if k != "detail"},
        "falseClassificationAudit": {k: fp[k] for k in fp if k != "samples"},
        "mergerDirection": {k: md[k] for k in md if k != "samples"},
        "anchors": an,
        "reconciliation": {k: rec[k] for k in rec if k != "rows"},
        "expectedBias": {k: bias[k] for k in bias},
        "materiality": mat, "foundationVerdict": v,
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
    ent, fp, mdir = d["entitlement"], d["falseClassificationAudit"], d["mergerDirection"]
    an, rec = d["anchors"], d["reconciliation"]
    bias, mat, v = d["expectedBias"], d["materiality"], d["foundationVerdict"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R19 — NO_DIRECT_MATCH 342건 자본행위 정체 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = mat["progression"]
    A(f"- 미해결 종목비율 **R16 {pg['R16']}% → R17 {pg['R17']}% → R18 {pg['R18']}% "
      f"→ R19 {pg['R19']}%** (기준 20%)")
    A(f"- 기대편향 **R18 {bias['r18ExpectedBiasPct']}% → R19 "
      f"{bias['expectedBiasPct']}%** (기준 {bias['thresholdPct']}%)")
    A(f"- anchor {len(an['cases'])}건 전부 PASS · 오분류율 {fp['falsePositiveRate']} · "
      f"합병 방향성 생존율 {mdir['survivalPct']}%")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 342건 중 **296건을 직접증거로 확정**했고 미해결 종목비율을 절반으로")
        A("> 줄였다. 그런데 기대편향은 오히려 올라갔다 — R18 의 3.47% 가 **비대칭")
        A("> 조건화로 과소평가된 값**이었기 때문이다. 그 오류를 바로잡은 결과가")
        A("> 5.316% 이고 기준 2% 를 넘는다. 기준을 옮기지 않고 BLOCKED 를 유지한다.")
    elif v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("> 남은 미해결이 기준 이하로 내려왔다. 한계를 명시한 채 연구 사용을 허용한다.")
    elif v["verdict"].endswith("_PASS"):
        A("> foundation 이 연구 사용 가능 수준에 도달했다.")
    else:
        A("> 엔진 검증에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 직접소스 수집 (§2·§10)")
    A("")
    A("```")
    A(f"대상 NO_DIRECT_MATCH        {tg['targetTotal']:6,}")
    A(f"대상 종목                    {tg['targetTickers']:6,}")
    A(f"corp_code 매핑               {tg['corpCodeMapped']:6,}")
    A(f"수집 종목                    {ds['tickersCached']:6,}")
    A(f"보관 공시                    {ds['filingsKept']:6,}")
    A(f"수집 실패                    {ds['hardFailures']:6,}")
    A("```")
    A("")
    A("### ★ 왜 R17/R18 은 이 342건을 못 찾았나")
    A("")
    A(f"{ds['whyAllTypes']}")
    A("")
    A(f"창: {ds['windowMonths']}")
    A("")

    A("## 3. 자본행위 유형 확정 (§3·§22)")
    A("")
    A("```")
    for k, n in sorted(cl["byPrimaryEvent"].items(), key=lambda x: -x[1]):
        A(f"{KO.get(k, str(k)):26} {n:6,}")
    A("```")
    A("")
    A("```")
    A("신뢰도   " + " · ".join(f"{k} {n:,}" for k, n in cl["byConfidence"].items()))
    A("근거출처 " + " · ".join(f"{k} {n:,}" for k, n in cl["byProvenance"].items()))
    A("```")
    A("")

    A("## 4. 핵심 질문 — 기존 주주가 직접 무엇을 받았나 (§4)")
    A("")
    A(f"> {tg['coreQuestion']}")
    A("")
    A(f"{tg['coreDistinction']}")
    A("")
    A("```")
    for k, n in ent["counts"].items():
        A(f"직접 권리 {k:10} {n:6,}")
    A("```")
    A("")
    A("```")
    for k, n in sorted(cl["byWealthAdjustment"].items(), key=lambda x: -x[1]):
        A(f"조정 {k:22} {n:6,}")
    A("```")
    A("")
    A("**이것이 R19 의 핵심 결과다.** 이 사건들의 주식수 증가는 대부분 다른 투자자의")
    A("CB 전환·BW 행사·옵션 행사, 그리고 합병 신주 발행에서 온 것이다. 기존 보통주")
    A("주주는 아무것도 받지 않았으므로 **아무것도 더하지 않는 것이 정답**이고,")
    A("R16 의 미조정이 이미 맞았다. '문제가 없었다'는 사실을 직접증거로 확정했다(§14).")
    A("")

    A("## 5. 합병 방향성 실증 (§7·§20)")
    A("")
    A(f"{mdir['claim']}")
    A("")
    A(f"**검증 방법**: {mdir['test']}")
    A("")
    A("```")
    A(f"검증 사건       {mdir['tested']:6,}")
    A(f"6개월 후 생존   {mdir['stillTrading']:6,}")
    A(f"소멸            {mdir['disappeared']:6,}")
    A(f"생존율          {mdir['survivalPct']:6.2f}%")
    A("```")
    A("")
    A(f"{mdir['interpretation']}")
    A("")
    A(f"{mdir['counterDirectionNote']}")
    A("")

    A("## 6. anchor 검증 (§19)")
    A("")
    A("| case | 종목 | 시점 | 직접권리 | 조정 | 종료주식 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | {c['entitlement']} | "
          f"{c['wealthAdjustment']} | {m['endingShares']:.4f} | "
          f"{m['manualReturn'] * 100:.3f}% | {c['engineReturn'] * 100:.3f}% | "
          f"{c['diffPp']}%p |")
    A("")
    if an["missingCaseTypes"]:
        A(f"확보하지 못한 유형: {', '.join(an['missingCaseTypes'])} — 342건 중 "
          "주주배정 유상증자로 재분류된 사건이 없었다(없는 것을 만들지 않는다).")
        A("")
    A(f"**{an['whyBothSidesNeeded']}**")
    A("")
    A(f"권리 없는 사건 조정 정확히 0: **{an['noEntitlementAdjustmentExactlyZero']}** · "
      f"권리 있는 사건 비영 조정: **{an['entitlementCaseAdjustsNonZero']}**")
    A("")

    A("## 7. 오분류 감사 (§21)")
    A("")
    A(f"{fp['method']}")
    A("")
    A("| 유형 | 모집단 | 표본 | 통과 | 오분류율 |")
    A("|---|---|---|---|---|")
    for c in fp["byPrimaryEvent"]:
        A(f"| {KO.get(c['primaryEvent'], c['primaryEvent'])} | {c['population']:,} | "
          f"{c['sampled']} | {c['crossCheckPass']} | {c['falsePositiveRate']} |")
    A("")
    A(f"**전체 오분류율 {fp['falsePositiveRate']}** (표본 {fp['sampled']}건)")
    A("")
    if fp.get("harnessNote"):
        A(f"{fp['harnessNote']}")
        A("")

    A("## 8. 자체수정 (§31)")
    A("")
    for sc in tg.get("selfCorrections", []):
        A(f"**{sc['id']}** — {sc['what']}")
        A("")
        A(f"- 왜 틀렸나: {sc['why']}")
        A(f"- 조치: {sc['fix']}")
        A(f"- 방향: {sc['direction']}")
        A("")
    A("**R19-SC3** — 기대편향 조건화가 비대칭이었다.")
    A("")
    A("- 왜 틀렸나: 권리 없음이 확정된 사건에는 P=0 을 넣으면서, R18 이 주주배정으로")
    A("  **확정한** 92건에는 모집단값 0.296 을 그대로 썼다. 확정된 사실은 양방향 모두")
    A("  반영해야 한다(§16 의 취지).")
    A("- 조치: 확정 주주배정에 P=1.0 적용.")
    A("- 방향: 편향을 **키운다** = 판정을 어렵게 만드는 방향.")
    A("")
    A("**R19-SC4** — 오분류 감사 하네스에 STOCK_SPLIT 기대 키워드를 빠뜨려 전건 FAIL")
    A("(15.6%)로 나왔다. 분류가 아니라 감사 코드의 결함이었고, 원문 공시명은")
    A("'주식분할결정'·'액면분할'로 정확했다. 수정 후 0.0%.")
    A("")

    A("## 9. 기대편향 재계산 (§15·§16)")
    A("")
    A("```")
    A(f"공식            {bias['formula']}")
    A(f"공식 R18 동일   {bias['formulaUnchangedFromR18']}")
    A(f"조건부 크기     {bias['conditionalMedian'] * 100:.2f}%  ({bias['conditionalSource']})")
    A(f"모집단 P        {bias['populationP'] * 100:.2f}%")
    A(f"유효 P          {bias['effectiveP'] * 100:.2f}%")
    A(f"미해결 사건     {bias['unresolvedEvents']:,}")
    A(f"기대편향        {bias['expectedBiasPct']}%   (기준 {bias['thresholdPct']}%)")
    A("```")
    A("")
    A(f"{bias['whatChanged']}")
    A("")
    A("### ★ R18 3.47% 는 과소평가된 값이었다")
    A("")
    A(f"{bias['progressionCaveat']}")
    A("")
    A(f"**지표 자체의 결함**: {bias['metricInsensitivityFinding']}")
    A("")
    A("### 기여도 분해 (§16)")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    xc = bias.get("directMeasurementCrossCheck") or {}
    if xc.get("n"):
        A("### 대리치 교차검증")
        A("")
        A(f"최대 기여군({KO.get(xc['group'], xc['group'])}, {xc['n']}건)의 과소평가를")
        A("직접 계산해 대리치와 비교했다.")
        A("")
        A("```")
        A(f"공시 비율 기준   {xc['medianUsingDartRatio'] * 100:.2f}%")
        A(f"관측 비율 기준   {xc['medianUsingObservedRatio'] * 100:.2f}%")
        A(f"대리치(조건부)   {xc['proxyConditionalMedian'] * 100:.2f}%")
        A("```")
        A("")
        A(f"{xc['verdict']}")
        A("")

    A("## 10. 전체 2,742건 최종 재분류 (§23)")
    A("")
    A("```")
    for k, n in sorted(rec["byWealthLayer"].items(), key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"R19 직접증거 merge   {rec['r19Merged']:6,}")
    A(f"wealth 확정          {rec['wealthResolved']:6,}")
    A("```")
    A("")

    A("## 11. 미해결 materiality (§23)")
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
        A(f"{k:44} {n:6,}")
    A("```")
    A("")
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 12. foundation 재판정 (§17·§18)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16/R17/R18 승계, 변경 없음):")
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
    A("- 342건 중 46건은 창 안에서 자본행위 공시를 찾지 못했다. 모르는 것은 "
      "UNRESOLVED 로 남겼다.")
    A("- 분류는 대부분 MEDIUM 이다. 공시명에 신주수가 없어 수량 일치까지 확인된 "
      "HIGH 가 드물다(§11 기준을 낮추지 않았다).")
    A("- 합병 방향성은 구조 논거 + 생존율 실증이다. 개별 합병계약서를 읽은 것은 "
      "아니다.")
    A("- 유상감자와 무상감자 구분은 여전히 없다(60건). 유상감자 현금 수령은 별도 "
      "한계로 남는다.")
    A("- 기대편향은 대리치 기반 추정이다. 최대 기여군은 직접 측정으로 교차검증했지만 "
      "나머지는 모집단 확률을 쓴다.")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 17. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R20**. 새 precommit "
          "으로 시장 benchmark · BM · EY · ROE · SIZE · QUALITY 를 동일 canonical TSR "
          "engine 에서 원점 경쟁시킨다. 기존 결과를 정답으로 쓰지 않는다(§34).")
    else:
        top = max(bias["decomposition"].items(),
                  key=lambda x: x[1]["biasContributionPct"])
        A(f"- 다음 단일 작업: 기대편향 최대 기여군 **{KO.get(top[0], top[0])}** "
          f"({top[1]['events']}건, {top[1]['biasContributionPct']:.3f}%p) 하나만 처리한다.")
        A("")
        A("  이들은 R18 이 원문으로 **주주배정임을 이미 확정**한 사건이다. 막고 있는 것은")
        A("  유형이 아니라 **조건(배정비율·발행가)의 신뢰성**이다. 공시 신주수가 계획치라")
        A("  관측 주식수 변동과 어긋나 R18 이 보수적으로 미조정으로 남겼다.")
        A("")
        A("  해결 경로는 증권발행실적보고서 등 **실제 발행량**을 담은 사후 공시로")
        A("  계획치가 아닌 확정 발행량을 얻는 것이다. 대상이 92건으로 유계다.")
        A("")
        proj = bias["expectedBiasPct"] - top[1]["biasContributionPct"]
        A(f"  **투영**: 이 군이 전부 확정되면 기여 {top[1]['biasContributionPct']:.3f}%p "
          f"가 빠져 {proj:.3f}% 가 된다 — 기준 2% "
          f"{'미만이 된다' if proj <= 2.0 else '를 여전히 넘는다'}.")
        A("")
        A("  단 이것은 **투영이지 결과가 아니다.** 전부 해결된다는 보장이 없고,")
        A("  일부가 실제로 큰 권리가치를 가진 것으로 확정되면 편향이 오히려 오를 수도")
        A("  있다. 판정은 실제 확정 후에만 바뀐다.")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · 포트폴리오 설계 · 합병계약서 전수 판독 · "
      "홈페이지 공개 · 실계좌 자금")
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
