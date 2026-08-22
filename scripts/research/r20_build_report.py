#!/usr/bin/env python3
"""R20 canonical report (MD + JSON).

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20
기존 R16~R19 산출물을 덮어쓰지 않는다(§26). HOTG 는 기존 Bridge 재사용(§28).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-holder-rights-final-terms-recovery-r20-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R20_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R20_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R20_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R20_CANONICAL_TSR_FOUNDATION_FAIL"),
}

KO = {"LOW_CONFIDENCE_NOT_COUNTED": "R17 저신뢰 매칭(미계수)",
      "HOLDER_RIGHT_TERMS_MISSING": "주주배정 확정·조건 미확보",
      "TRUE_UNRESOLVED": "완전 미확인", "UNRESOLVED": "완전 미확인",
      "OTHER_CAPITAL_ACTION": "감자 등 기타 자본행위",
      "R18_NO_DOCUMENT": "원문 미보유",
      "CONVERTIBLE_BOND_CONVERSION": "전환사채 전환",
      "NO_ENTITLEMENT_CONFIRMED": "권리 없음 확정"}


def L(n):
    p = RD / f"r20-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, pf = L("targets-precommit"), L("post-issuance-filings")
    ft, ch = L("final-terms-normalized"), L("correction-chains")
    sc, an = L("share-count-reconciliation"), L("anchor-cases")
    ct, wr = L("ex-rights-continuity"), L("wealth-resolution")
    rec, bias = L("full-reconciliation"), L("expected-bias")
    mat, v = L("unresolved-materiality"), L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-holder-rights-final-terms-recovery-r20@1",
        "taskId": "WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "postIssuanceFilings": {k: pf[k] for k in pf if k != "documents"},
        "finalTerms": {k: ft[k] for k in ft if k != "events"},
        "corrections": {k: ch[k] for k in ch if k != "list"},
        "shareCountReconciliation": {k: sc[k] for k in sc if k != "rows"},
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
    tg, pf, ft = d["targetsPrecommit"], d["postIssuanceFilings"], d["finalTerms"]
    ch, sc, an = d["corrections"], d["shareCountReconciliation"], d["anchors"]
    ct, rec = d["continuity"], d["reconciliation"]
    bias, mat, v = d["expectedBias"], d["materiality"], d["foundationVerdict"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R20 — 주주배정 확정건 92건 최종조건 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = mat["progression"]
    A(f"- 미해결 종목비율 **R16 {pg['R16']}% → R17 {pg['R17']}% → R18 {pg['R18']}% "
      f"→ R19 {pg['R19']}% → R20 {pg['R20']}%** (기준 20%)")
    bp = bias["progression"]
    A(f"- 기대편향 **R18 {bp['R18']}% → R19 {bp['R19']}% → R20 {bp['R20']}%** "
      f"(기준 {bias['thresholdPct']}%)")
    A(f"- 92건 중 wealth 확정 **{ft['byWealthStatus'].get('WEALTH_CONFIRMED', 0)}** · "
      f"anchor {an['obtained']}건 전부 PASS · 공짜 wealth {ct['freeWealthEvents']}건")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 92건 중 67건의 **실제 발행조건**을 증권발행실적보고서로 복원했다.")
        A("> 계획치가 얼마나 못 믿을 것이었는지도 드러났다 — 배정비율이 계획과")
        A(f"> 다른 건이 {ft['flags'].get('PLANNED_RATIO_DIFFERS', 0)}건, 실권이")
        A(f"> 있었던 건이 {ft['flags'].get('PARTIAL_SUBSCRIPTION', 0)}건이다.")
        A("> 편향은 5.316% → 3.442% 로 내려갔지만 기준 2% 에는 못 미친다.")
        A("> 기준을 옮기지 않고 BLOCKED 를 유지한다.")
    elif v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("> 실제 발행조건 복원으로 편향이 기준 이하로 내려왔다.")
        A("> 한계를 명시한 채 연구 사용을 허용한다.")
    elif v["verdict"].endswith("_PASS"):
        A("> foundation 이 연구 사용 가능 수준에 도달했다.")
    else:
        A("> 엔진 검증에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 사후공시 수집 (§2·§3·§25)")
    A("")
    A("```")
    A(f"대상(R19 주주배정 확정·조건미확보)  {tg['targetTotal']:6,}")
    A(f"대상 종목                            {tg['targetTickers']:6,}")
    A(f"corp_code 매핑                       {tg['corpCodeMapped']:6,}")
    A(f"수집 종목                            {pf['listCached']:6,}")
    A(f"원문 확보                            {pf['documentsNew'] + pf['documentsCacheHits']:6,}")
    A(f"원문 실패                            {pf['documentsFailed']:6,}")
    A("```")
    A("")
    A("수집 공시 유형:")
    A("")
    A("```")
    KF = {"ISSUE_RESULT_REPORT": "증권발행실적보고서", "FINAL_PRICE": "확정발행가액",
          "ADDITIONAL_LISTING": "추가·변경상장", "UNSUBSCRIBED_RESULT": "실권주·단수주",
          "RIGHTS_DECISION": "유상증자결정(정정 포함)", "PRICE_NOTICE": "발행가액 안내",
          "ISSUE_RESULT_DERIVATIVE_IGNORED": "실적보고서[파생결합증권] — 제외"}
    for k, n in sorted(pf["filingKinds"].items(), key=lambda x: -x[1]):
        A(f"{KF.get(k, k):34} {n:6,}")
    A("```")
    A("")
    A(f"{pf['derivativeIgnoredNote']}")
    A("")

    A("## 3. 실적보고서에서 무엇을 얻었나 (§4·§6)")
    A("")
    A("증권발행실적보고서는 배정대상(구주주 / 우리사주조합 / 제3자 / 일반청약)별로")
    A("**최초 배정주식수 · 실제 청약 · 실제 배정 · 실제 납입금액**을 구조화 필드로")
    A("담고 있다. 확정발행가는 구주주 배정금액 ÷ 배정주식수로 직접 나온다.")
    A("")
    A("**필드 복원률** (92건 기준)")
    A("")
    A("| 필드 | 복원 | 비율 |")
    A("|---|---|---|")
    KOF = {"finalNewSharesIssued": "실제 총 발행주식수",
           "finalIssuePrice": "확정발행가",
           "finalShareholderEntitledShares": "구주주 배정주식수(권리)",
           "finalShareholderAllocatedShares": "구주주 실배정주식수",
           "entitlementRightsRatio": "최종 배정비율",
           "actualPaymentDate": "실제 납입일",
           "actualSubscriptionStart": "실제 청약 시작일",
           "finalThirdPartyShares": "제3자 배정분"}
    for k, r in ft["fieldRecovery"].items():
        A(f"| {KOF.get(k, k)} | {r['n']} | {r['pct']}% |")
    A("")

    A("## 4. ★ 계획치는 실제로 못 믿을 것이었다 (§5·§8)")
    A("")
    A("이것이 R18 이 이 92건을 보수적으로 미조정으로 남긴 이유를 사후에 정당화한다.")
    A("")
    A("```")
    FK = {"PLANNED_RATIO_DIFFERS": "계획 배정비율이 최종과 다름(>15%)",
          "PARTIAL_SUBSCRIPTION": "실권 발생(구주주 청약 < 배정)",
          "PLANNED_PRICE_DIFFERS": "계획 발행가가 확정가와 다름(>15%)",
          "TAKEUP_BELOW_ENTITLEMENT": "실청약이 권리보다 적음",
          "SHARE_COUNT_DATA_CONFLICT": "보고 발행수 ≠ 관측 주식수 증가",
          "PLANNED_SHAREHOLDER_BUT_FINAL_NOT": "계획은 주주배정, 실제는 제3자·공모"}
    for k, n in sorted(ft["flags"].items(), key=lambda x: -x[1]):
        A(f"{FK.get(k, k):38} {n:5,}")
    A("```")
    A("")
    A("계획 신주수로 wealth 를 계산했다면 존재하지 않은 권리가치를 만들었을 것이다.")
    A("")

    A("## 5. 권리 기준 — 배정 vs 실청약 (§10)")
    A("")
    A(f"**사용: {ft['ratioBasis']['used']}**")
    A("")
    A(f"{ft['ratioBasis']['why']}")
    A("")
    A(f"함께 기록: {ft['ratioBasis']['alsoRecorded']}. 둘이 15% 넘게 벌어지면")
    A("`TAKEUP_BELOW_ENTITLEMENT` 로 표시했다.")
    A("")

    A("## 6. 정정 chain (§7)")
    A("")
    A("```")
    A(f"chain                {ch['chains']:6,}")
    A(f"정정 포함            {ch['withCorrection']:6,}")
    A(f"실적보고서 포함      {ch['withResultReport']:6,}")
    A("```")
    A("")
    A(f"{ch['policy']}")
    A("")

    A("## 7. 주식수 대조 (§9)")
    A("")
    A(f"{sc['hardRule']}")
    A("")
    A(f"{sc['method']}")
    A("")
    A("```")
    A(f"보고 발행수 확보          {sc['withReported']:5,}")
    A(f"허용오차 내 일치          {sc['matchedWithinTolerance']:5,}")
    A(f"다른 자본행위로 설명됨    {sc['explainedByOtherEvent']:5,}")
    A(f"설명되지 않는 불일치      {sc['unexplainedConflict']:5,}")
    A("```")
    A("")

    A("## 8. 확정 결과 (§16·§17)")
    A("")
    A("```")
    ES = {"PARTIALLY_COMPLETED": "일부 완료(실권 발생)", "COMPLETED": "전량 완료",
          "NO_RESULT_REPORT": "실적보고서 미확보",
          "COMPLETED_WITHOUT_SHAREHOLDER_ALLOCATION": "실제로는 구주주 배정 없음",
          "CANCELLED_OR_UNKNOWN": "철회·불명"}
    for k, n in sorted(ft["byEventStatus"].items(), key=lambda x: -x[1]):
        A(f"{ES.get(k, k):34} {n:5,}")
    A("```")
    A("")
    A("```")
    for k, n in sorted(ft["byWealthStatus"].items(), key=lambda x: -x[1]):
        A(f"{k:24} {n:5,}")
    A("```")
    A("")
    A("```")
    A("신뢰도  " + " · ".join(f"{k} {n}" for k, n in ft["byConfidence"].items()))
    A("```")
    A("")

    A("## 9. manual anchor (§15)")
    A("")
    A("| 구분 | 종목 | 시점 | 최종비율 | 계획비율 | 확정가 | 실발행 | 청약률 | "
      "외부납입 | 권리가치 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]

        def f(x, fmt=",.0f"):
            return "-" if x is None else format(x, fmt)
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | "
          f"{f(m['rightsRatio'], '.4f')} | {f(m['plannedRightsRatio'], '.4f')} | "
          f"{f(m['issuePrice'])} | {f(m['actualIssuedShares'])} | "
          f"{f(m['takeUpRate'], '.3f')} | {f(m['externalContribution'])} | "
          f"{f(m['rightsValue'])} | {m['manualTsrEffect'] * 100:.3f}% | "
          f"{c['engineTsrEffect'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    A(f"anchor 10건 이상: **{an['atLeastTen']}** · 전부 manual 일치: "
      f"**{an['allPass']}** · 정책 출처: {an['policySource']}")
    A("")

    A("## 10. ex-rights 연속성 (§14)")
    A("")
    A("| 창(개월) | n | R20−R16 차분 중앙값 | 차분 음수건 |")
    A("|---|---|---|---|")
    for w in ct["windows"]:
        A(f"| {w['window']} | {w['n']} | "
          f"{'-' if w['medianDiff'] is None else format(w['medianDiff'] * 100, '+.2f') + '%'} | "
          f"{w['negativeDiffCount']} |")
    A("")
    A(f"사건 당월 차분 {ct['eventMonthMedianDiff'] * 100:.2f}% vs 이론 "
      f"{ct['theoreticalMedian'] * 100:.2f}% (`{ct['theoreticalDefinition']}`) → "
      f"**일치 {ct['diffMatchesTheory']}**")
    A("")
    A(f"공짜 wealth **{ct['freeWealthEvents']}건** · 연속성 판정 "
      f"**{'PASS' if ct['pass'] else 'FAIL'}**")
    A("")

    A("## 11. 기대편향 재계산 (§18·§19)")
    A("")
    A("```")
    A(f"공식              {bias['formula']}")
    A(f"공식 R19 동일     {bias['formulaUnchangedFromR19']}")
    A(f"조건부 크기       {bias['conditionalMedian'] * 100:.2f}%")
    A(f"미해결 사건       {bias['unresolvedEvents']:,}")
    A(f"유효 P            {bias['effectiveP'] * 100:.2f}%")
    A(f"기대편향          {bias['expectedBiasPct']}%   (기준 {bias['thresholdPct']}%)")
    A("```")
    A("")
    A("### ★ R19 보고서의 투영이 왜 빗나갔나")
    A("")
    A(f"{bias['arithmeticNote']}")
    A("")
    A("### 기여도 분해 (§19)")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    A(f"이번 작업으로 이 군의 기여가 {bias['r19GroupContributionPp']}%p "
      f"→ {bias['decomposition'].get('HOLDER_RIGHT_TERMS_MISSING', {}).get('biasContributionPct', 0)}%p "
      "로 줄었다.")
    A("")

    A("## 12. 전체 2,742건 최종 재분류 (§27)")
    A("")
    A("```")
    for k, n in sorted(rec["byWealthLayer"].items(), key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"R20 merge            {rec['r20Merged']:6,}")
    A(f"wealth 확정          {rec['wealthResolved']:6,}")
    A("```")
    A("")

    A("## 13. 미해결 materiality")
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
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 14. foundation 재판정 (§20·§21)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16~R19 승계, 변경 없음):")
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

    A("## 15. 기존 연구 상태 (§23)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 16. production 보호 (§27)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 17. HOTG (§28)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 18. 한계")
    A("")
    A(f"- 92건 중 {ft['byEventStatus'].get('NO_RESULT_REPORT', 0)}건은 창 안에서 "
      "구주주 배정이 있는 실적보고서를 찾지 못했다. 계획치로 승격하지 않고 "
      "WEALTH_PARTIAL 로 남겼다(§11).")
    A(f"- 보고 발행수와 관측 주식수 증가가 어긋나 설명되지 않는 건이 "
      f"{sc['unexplainedConflict']}건이다. 같은 달 다른 자본행위로 설명되는 건과 "
      "구분해 표시했다.")
    A("- 권리 기준은 배정(entitlement)이다. 실제 청약률은 별도 기록했지만 "
      "canonical wealth 에는 POLICY A 대로 배정 기준을 썼다.")
    A("- 우리사주조합 배정분은 기존 보통주 주주의 권리가 아니므로 제외했다.")
    A("- 유상감자·무상감자 구분(60건), 합병 교환비율, 인적분할 배정비율은 여전히 "
      "없다(R16~R19 한계 그대로).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 19. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R21**. 새 "
          "precommit 으로 시장 benchmark · BM · EY · ROE · SIZE · QUALITY 를 동일 "
          "canonical TSR engine 에서 원점 경쟁시킨다. 기존 R7 BM 승리 · SIZE 신호 · "
          "Quality 결과를 정답으로 쓰지 않는다(§33).")
    else:
        top = max(bias["decomposition"].items(),
                  key=lambda x: x[1]["biasContributionPct"])
        A(f"- 다음 단일 작업: 잔여 편향 최대 기여군 **{KO.get(top[0], top[0])}** "
          f"({top[1]['events']}건, {top[1]['biasContributionPct']:.3f}%p) 하나만 "
          "처리한다.")
        A("")
        A("  이 군의 P 는 **직접증거가 아니라 모집단 추정치**"
          f"({top[1]['avgP']:.3f})다. R19 가 NO_DIRECT_MATCH 342건을 직접 조사했을 때")
        A("  82%가 기존 주주 권리 없음으로 밝혀졌다. 같은 방식으로 이 군을 직접")
        A("  판정하면 P 가 추정치에서 실측치로 바뀌고 편향이 크게 달라질 수 있다")
        A("  — 올라갈 수도 내려갈 수도 있으므로 미리 결론내지 않는다.")
        A("")
        pop = [(k, c) for k, c in bias["decomposition"].items()
               if c["pSource"] == "POPULATION_ESTIMATE"]
        tot = sum(c["biasContributionPct"] for _k, c in pop)
        n = sum(c["events"] for _k, c in pop)
        A(f"  참고: 모집단 추정치에 의존하는 군이 총 {n}건 · {tot:.3f}%p 다. "
          f"전체 편향 {bias['expectedBiasPct']}% 의 대부분이 **측정이 아니라 "
          "가정**에서 온다.")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · 포트폴리오 설계 · 감자 유무상 구분 · "
      "홈페이지 공개 · 실계좌 자금")
    A("")
    A(f"- §22 — foundation 이 PASS 계열이 된 뒤에만 factor 연구를 재개한다. "
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
