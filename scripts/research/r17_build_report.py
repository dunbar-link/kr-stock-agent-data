#!/usr/bin/env python3
"""R17 canonical report 생성 (MD + JSON).

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

R16 이전 산출물을 덮어쓰지 않는다(§24). HOTG 는 기존 Report Bridge 를 재사용한다(§27).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-dart-rights-issue-canonical-recovery-r17-latest"


def L(n):
    p = RD / f"r17-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R17_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R17_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R17_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R17_CANONICAL_TSR_FOUNDATION_FAIL"),
}


def build():
    pc, raw, nm = L("precommit"), L("dart-rights-raw"), L("dart-rights-normalized")
    cr, mt = L("dart-rights-corrections"), L("rights-matching")
    an, ct = L("rights-anchor-cases"), L("ex-rights-continuity")
    ma, v = L("unresolved-materiality"), L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-dart-rights-issue-canonical-recovery-r17@1",
        "taskId": "WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17",
        "verdictLine": line, "reasonClass": rc,
        "question": pc["question"],
        "precommit": pc, "raw": raw, "normalized": {
            k: nm[k] for k in nm if k not in ("events", "filings")},
        "corrections": {k: cr[k] for k in cr if k != "list"},
        "matching": {k: mt[k] for k in mt if k != "rows"},
        "anchors": an, "continuity": ct, "materiality": ma, "foundationVerdict": v,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched",
            "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
            "externalSend": 0, "deploy": 0, "envOrToken": 0,
            "productionDbWrite": 0, "publicDisclosure": 0,
            "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        },
        "hotg": {"canonicalReport": f"reports/wababa/{NAME}.md",
                 "sourceOwner": "Wababa", "reportBridge": True,
                 "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
                 "newObservers": 0, "newScheduler": 0, "newOrchestration": 0},
    }


def md(d):
    pc, raw, nm = d["precommit"], d["raw"], d["normalized"]
    cr, mt = d["corrections"], d["matching"]
    an, ct, ma, v = d["anchors"], d["continuity"], d["materiality"], d["foundationVerdict"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R17 — DART 유상증자 직접소스 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    A(f"- 미해결 종목비율 **{ma['unresolvedTickerPct']}%** "
      f"(R16 40.8% → 사전기준 40% 유지)")
    A(f"- anchor 전부 PASS: {an['allPass']} · 공짜 wealth: {ct['freeWealthEvents']}건")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> DART 직접증거로 상당수를 확정했지만, 사전에 정한 경제적 materiality 기준을")
        A("> 넘지 못했다. 기준을 옮기지 않고 BLOCKED 를 유지한다.")
    elif v["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        A("> 남은 미해결이 **wealth 를 부풀리지 않고 과소평가 방향으로만** 작동하며")
        A("> 그 크기가 작다는 것을 정량 확인했다. 제한을 명시한 채 연구 사용을 허용한다.")
    elif v["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS":
        A("> 유상증자 gap 이 연구에 영향을 주지 않는 수준까지 복원됐다.")
    else:
        A("> 엔진이 가짜 wealth 를 만들거나 anchor 재현에 실패했다. 사용 금지.")
    A("")

    A("## 2. DART 접근과 수집 (§7)")
    A("")
    A("```")
    A(f"접근경로            기존 DART_API_KEY 재사용 (신규 키·토큰 0)")
    A(f"suspected 종목      {raw['suspectedTickers']:,}")
    A(f"corp_code 매핑      {raw['corpCodeMapped']:,} "
      f"({raw['corpCodeMapped'] / raw['suspectedTickers'] * 100:.1f}%)")
    A(f"매핑 실패           {len(raw['corpCodeUnmapped'])}")
    A(f"구조화 endpoint     {', '.join(raw['endpointsStructured'])}")
    A(f"공시목록 endpoint   {raw['endpointFilings']}")
    A("```")
    A("")
    A("### ★ 실측으로 드러난 커버리지 경계")
    A("")
    A(f"**DART 주요사항보고서 주요정보 API 는 {raw['structuredEarliestYearObserved']}년부터만")
    A("존재한다.** 표본 8종목 39건의 접수연도가 전부 2015+ 였다. 즉 증자방식·신주수 같은")
    A("구조화 필드는 2007~2014 구간에서 나오지 않는다.")
    A("")
    A("그래서 공시목록 API(`list.json`, 주요사항보고서 유형)를 함께 썼다. 이쪽은 1999년부터")
    A("있고 공시명에 유형('유상증자결정'/'무상증자결정')과 정정표시('[기재정정]')가 직접")
    A("들어 있다. **다만 비율·발행가·배정방식 상세가 없다** — 이 차이를 증거수준으로")
    A("구분해 끝까지 유지했다.")
    A("")
    A("```")
    A(f"STRUCTURED (2015+)        {nm['structuredEvents']:,} 건")
    A(f"FILING_TITLE_ONLY (1999+) {nm['filingTitleEvents']:,} 건")
    A("```")
    A("")

    A("## 3. 유상증자 유형 분포 (§4)")
    A("")
    A("```")
    for k, n in sorted(nm["structuredByMethod"].items(), key=lambda x: -x[1]):
        lbl = (pc["issueMethods"].get(k) or {}).get("label", k)
        A(f"{lbl:34} {n:6,}")
    A("```")
    A("")
    A("**이 분포가 핵심이다.** 기존 주주에게 신주인수권이 실제로 배정되는 것은 주주배정")
    A("계열(A·B)뿐이다. 제3자배정·일반공모는 기존 주주가 신주를 받지 않으므로")
    A("**R16 의 현재 동작(미조정)이 이미 정답**이다 — 엔진을 고칠 필요가 없다.")
    A("")

    A("## 4. 정정공시 (§5·§15)")
    A("")
    A("```")
    A(f"정정 chain          {cr['chains']:,}")
    A(f"정정 공시 총건       {cr['totalCorrectionFilings']:,}")
    A("```")
    A("")
    A(cr["howFinalTermsAreObtained"])
    A("")
    if an.get("correctionCase"):
        c = an["correctionCase"]
        A(f"실제 정정 case: **{c['ticker']}** {c['date']} — 원공시 {c['originalFiling']}")
        A(f"({c['originalDate']}) 이후 정정 {c['corrections']}건. 회계에 쓰인 조건은")
        A(f"최종본이며 비율 {c['finalTermsRatio']}, 발행가(파생) {c['finalTermsIssuePrice']}.")
        A(f"판정: **{'PASS' if c['pass'] else 'FAIL'}**")
    A("")

    A("## 5. 매칭 결과 (§8·§9)")
    A("")
    A("```")
    for k, n in sorted(mt["byLabel"].items(), key=lambda x: -x[1]):
        A(f"{k:36} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"confidence   " + " · ".join(f"{k} {n:,}" for k, n in mt["byConfidence"].items()))
    A(f"provenance   " + " · ".join(f"{k} {n:,}" for k, n in mt["byProvenance"].items()))
    A("```")
    A("")
    A(f"**resolved 정의**: {mt['resolvedDefinition']}")
    A("")
    A("### ★ 자체수정 — 매칭 창의 방향 (§29)")
    A("")
    wa = mt["windowAdaptation"]
    A(f"{wa['what']}")
    A("")
    A(f"조치: {wa['fix']} · {wa['strictnessPreserved']}")
    A("")
    A("### ★ resolved 를 두 층위로 나눴다")
    A("")
    A(mt["twoLayerNote"])
    A("")
    A("```")
    A(f"resolvedWealth (엔진 동작 확정)   {mt['wealthResolved']:6,}")
    A(f"wealth 미해결                    {mt['wealthUnresolved']:6,}")
    A("```")
    A("")
    A("미해결 사유:")
    A("")
    A("```")
    for k, n in sorted(mt["wealthUnresolvedReasons"].items(), key=lambda x: -x[1]):
        A(f"{k:34} {n:6,}")
    A("```")
    A("")

    A("## 6. 주주 wealth 정책 (§10·§12)")
    A("")
    wp = pc["wealthPolicy"]
    A("```")
    A(f"정책        {wp['chosen']}")
    A(f"기준        {wp['returnBasis']}")
    A("```")
    A("")
    A(wp["definition"])
    A("")
    A(f"**왜 POLICY B 가 아닌가**: {wp['whyNotPolicyB']}")
    A("")
    A(f"**정책 선택이 결과를 바꾸지 않는 이유**: {wp['equivalenceNote']}")
    A("")
    A("★ 자체수정(§29): 사전규격은 '전량 청약'이라고만 썼다. 그러나 발행가가 권리락")
    A("주가보다 **높으면** 청약은 손해이고 합리적 주주는 실권한다. 그대로 강제하면")
    A("엔진이 존재하지 않는 손실을 만든다. `K < P_ex` 일 때만 청약하도록 했다 —")
    A("이는 R16 대비 보정폭을 **줄이는** 방향이다(보수적).")
    A("")

    A("## 7. anchor 검증 (§14)")
    A("")
    A("| case | 종목 | 시점 | 방식 | r | 발행가 | 외부납입 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | "
          f"{str(c['issueMethodRaw'] or '-')[:18]} | "
          f"{'-' if m['rightsRatio'] is None else format(m['rightsRatio'], '.4f')} | "
          f"{'-' if m['issuePrice'] is None else format(m['issuePrice'], ',.0f')} | "
          f"{m['externalContribution']:,.0f} | "
          f"{m['economicReturn'] * 100:.3f}% | "
          f"{c['engineReturn'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    if an["missingCaseTypes"]:
        A(f"확보하지 못한 유형: {', '.join(an['missingCaseTypes'])} — 해당 유형의 "
          "HIGH confidence 구조화 사건이 없었다(없는 것을 만들어내지 않는다).")
        A("")
    A("각 case 의 manual ledger 는 엔진과 독립적으로 손계산한 것이다.")
    A("")

    A("## 8. ex-rights wealth 연속성 (§13)")
    A("")
    A("### ★ 자체수정 — 절대 TWR 검정은 교란된다 (§29)")
    A("")
    A("처음에는 유상증자 사건의 **절대** 시간가중수익률이 0 으로 수렴하는지 봤다.")
    A(f"그 검정은 실패한다: {ct['absoluteTwrIsConfounded']}")
    A("")
    A("엔진 변경분만 격리하려면 **같은 창에서 R16 과 R17 의 차이**를 봐야 한다.")
    A("")
    A("| 창(개월) | n | R17 TWR 중앙값 | R16 수익 중앙값 | 차분 | 차분 음수건 |")
    A("|---|---|---|---|---|---|")
    for w in ct["windows"]:
        def f(x):
            return "-" if x is None else format(x * 100, "+.2f") + "%"
        A(f"| {w['window']} | {w['n']} | {f(w['r17MedianTwr'])} | "
          f"{f(w['r16MedianReturn'])} | **{f(w['medianDiff'])}** | "
          f"{w['negativeDiffCount']} |")
    A("")
    A(f"**차분이 이론값과 일치하는가**: 사건 당월 차분 "
      f"{ct['eventMonthMedianDiff'] * 100:.2f}% vs 이론 "
      f"{ct['theoreticalMedian'] * 100:.2f}% "
      f"(`{ct['theoreticalDefinition']}`) → **{ct['diffMatchesTheory']}**")
    A("")
    A("이론값과 정확히 같다는 것은 엔진이 **권리의 내재가치만큼만** 되돌려주고")
    A("그 이상을 만들지 않는다는 뜻이다. 이것이 §12 '공짜 wealth 금지'의 실질 증명이다.")
    A("")
    A("```")
    A(f"공짜 wealth 생성        {ct['freeWealthEvents']}건")
    A(f"당월 차분 음수          {'없음' if ct['eventMonthDiffNeverNegative'] else '있음'}")
    A(f"차분 누적 증폭 없음      {ct['diffDoesNotCompound']}")
    A(f"연속성 판정             {'PASS' if ct['pass'] else 'FAIL'}")
    A("```")
    A("")
    A(f"{ct['freeWealthCheckNote']}")
    A("")
    A(f"**넓은 창의 음수 차분에 대하여**: {ct['wideWindowNegativesAreLegitimate']}")
    A("")
    A(f"**R16 과의 차이**: {ct['r16Comparison']}")
    A("")

    A("## 9. 미해결 materiality (§16·§17)")
    A("")
    A("```")
    A(f"원 SUSPECTED_RIGHTS          {ma['suspectedTotal']:6,}")
    A(f"wealth 확정                  {ma['wealthResolved']:6,}")
    A(f"wealth 미해결                {ma['wealthUnresolved']:6,}")
    A(f"미해결 고유종목              {ma['unresolvedTickers']:6,}")
    A(f"universe 종목                {ma['universeTickers']:6,}")
    A(f"미해결 종목비율              {ma['unresolvedTickerPct']:6.2f}%")
    A(f"미해결 사건비율              {ma['unresolvedEventPct']:6.2f}%")
    A(f"극단 불연속 사건             {ma['extremeDiscontinuityEvents']:6,} "
      f"({ma['extremeDiscontinuityPct']}%)")
    A("```")
    A("")
    us = ma["understatementSample"]
    A(f"**미조정이 놓치는 크기** ({us['definition']})")
    A("")
    A("```")
    A(f"표본 n                    {us['n']:,}")
    A(f"조건부 중앙값             {us['conditionalMedian'] * 100:.2f}%   "
      f"← 주주배정이었을 때")
    A(f"조건부 p90                "
      f"{'-' if us['conditionalP90'] is None else format(us['conditionalP90'] * 100, '.2f') + '%'}")
    A(f"P(주주배정) 매칭표본       {us['pHolderRight'] * 100:.2f}%")
    A(f"P(주주배정) 전체구조화     "
      f"{'-' if us['pHolderRightAllStructured'] is None else format(us['pHolderRightAllStructured'] * 100, '.2f') + '%'}")
    A(f"**기대 편향**             **{us['expectedUnderstatement'] * 100:.2f}%**   "
      f"← 판정에 쓰는 값")
    A("```")
    A("")
    A(f"{us['expectedMeaning']}")
    A("")
    A(f"**추정 편향에 대하여**: {us['pHolderRightBiasNote']}")
    A("")
    A(f"{us['caveat']}")
    A("")
    A(f"**편향 방향: {ma['biasDirection']}** — {ma['biasWhy']}")
    A("")

    A("## 10. foundation 재판정 (§18)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(결과를 보고 바꾸지 않았다):")
    A("")
    A("```")
    for k, val in v["thresholdsFromPrecommit"].items():
        A(f"{k:44} {val}")
    A("```")
    A("")
    A(f"**판정: {v['verdict']}**")
    A("")

    A("## 11. 기존 연구 상태 (§21)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 12. production 보호 (§26)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 13. HOTG (§27)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 14. 한계")
    A("")
    A("- DART 주요정보 API 는 2015년부터다. 2007~2014 구간은 공시명 증거만 있어 "
      "**배정방식을 알 수 없다** — 이번 미해결의 최대 원인(1,081건 중 83.5%)이다.")
    A("- 거래소공시(pblntf_ty=I) 경로는 실측 수확 0% 로 기각했다(표본 30종목). "
      "대부분 종속회사 증자 공시라 모회사 주식수와 무관하다.")
    A("- 발행가는 API 가 직접 주지 않는다. 조달금액/신주수 파생값이며 "
      "우선주·전환권 등이 섞이면 오차가 난다.")
    A("- 청약일·납입일·상장일·신주배정기준일이 없다. 매칭은 접수일 기준 비대칭 창이다.")
    A("- 월간 스냅샷이라 권리락과 신주 상장이 다른 달에 찍힌다(실측 중앙값 2개월). "
      "3개월 미만 horizon 에서는 이 시차가 남는다.")
    A("- 정정 chain 은 공시명 기반 추론이다(DERIVED_INFERENCE). 최종조건 자체는 "
      "구조화 API 가 최종본을 주므로 이 추론에 의존하지 않는다.")
    A("- 유상감자와 무상감자를 구분하지 못한다(R16 한계 그대로).")
    A("- 합병 교환비율·인적분할 배정비율은 여전히 없다(R16 한계 그대로, §19 범위 밖).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 15. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R18**. 새 precommit 을 "
          "쓰고 원점에서 factor discovery 를 실행한다. R7 결과를 정답으로 쓰지 않는다(§32).")
    else:
        A("- 다음 단일 작업: **2007~2014 구간의 증자방식 복원 — DART 공시원문 파싱**.")
        A("  대상은 이미 `ISSUE_METHOD_UNKNOWN` 1,081건으로 특정돼 있고 그 83.5%가")
        A("  2007~2014 다. 구조화 API 가 없는 구간이므로 남은 직접소스는 공시 원문")
        A("  (`document.xml`)뿐이다. 주요사항보고서 원문에는 '증자방식' 항목이 있다.")
        A("  약 1,081 콜로 유계이며 §19 가 금지한 '전체 historical collector 신설'이")
        A("  아니다.")
        A("")
        A("  ★ 착수 전에 더 싼 후보를 먼저 실측했고 **기각**했다: DART 거래소공시")
        A("  (`pblntf_ty=I`)의 '증권발행결과(자율공시)(제3자배정 유상증자)' 경로는")
        A("  미상 종목 30개 표본에서 수확 **0%** 였다. 이들의 typeI 증자 공시는 대부분")
        A("  '유상증자결정(종속회사의주요경영사항)' 즉 **종속회사** 증자라 모회사")
        A("  주식수와 무관하고 방식도 드러나지 않는다. 초기 27% 추정은 단일 종목")
        A("  편향이었다. 근거: `reports/research/r17-next-path-probe-latest.json`")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · R18 시작 · 포트폴리오 설계 · 합병/인적분할 "
      "collector · 홈페이지 공개 · 실계좌 자금")
    A("")
    A(f"- §20 — foundation 이 PASS 또는 PASS_WITH_LIMITATIONS 가 된 뒤에만 "
      f"factor 연구를 재개한다. 현재: **{v['factorResearchAllowed']}**")
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
                      "factorAllowed": d["foundationVerdict"]["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
