#!/usr/bin/env python3
"""R18 canonical report (MD + JSON).

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18
R16/R17 산출물을 덮어쓰지 않는다(§27). HOTG 는 기존 Report Bridge 재사용(§31).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-dart-legacy-rights-document-recovery-r18-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R18_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("WARNING", "R18_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R18_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R18_CANONICAL_TSR_FOUNDATION_FAIL"),
}


def L(n):
    p = RD / f"r18-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg = L("targets-precommit")
    dl, pr = L("document-download-status"), L("document-parser-results")
    ch, nm = L("correction-chains"), L("legacy-rights-normalized")
    rec, fp = L("full-reconciliation"), L("false-positive-audit")
    an, ct = L("anchor-cases"), L("ex-rights-continuity")
    ma, v = L("unresolved-materiality"), L("foundation-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-dart-legacy-rights-document-recovery-r18@1",
        "taskId": "WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "download": {k: dl[k] for k in dl if k != "documents"},
        "parser": {k: pr[k] for k in pr if k != "documents"},
        "corrections": {k: ch[k] for k in ch if k != "chains"},
        "legacyNormalized": {k: nm[k] for k in nm if k != "events"},
        "reconciliation": {k: rec[k] for k in rec if k != "rows"},
        "falsePositiveAudit": {k: fp[k] for k in fp if k != "samples"},
        "anchors": an, "continuity": ct, "materiality": ma,
        "foundationVerdict": v,
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
    tg, dl, pr = d["targetsPrecommit"], d["download"], d["parser"]
    ch, rec, fp = d["corrections"], d["reconciliation"], d["falsePositiveAudit"]
    an, ct, ma, v = d["anchors"], d["continuity"], d["materiality"], d["foundationVerdict"]
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R18 — 2007~2014 유상증자 공시원문 복원")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    pg = v["progression"]
    A(f"- 미해결 종목비율 **R16 {pg['R16']}% → R17 {pg['R17']}% → R18 {pg['R18']}%** "
      f"(사전기준 20%, 변경 없음)")
    A(f"- anchor {len(an['cases'])}건 전부 PASS: {an['allPass']} · "
      f"공짜 wealth {ct['freeWealthEvents']}건 · "
      f"오분류율 {fp['falsePositiveRate']}")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        A("> 2007~2014 공시원문에 기계판독 필드가 그대로 살아 있었다. 증자방식을")
        A("> 직접 읽어 남은 미해결을 사전기준 아래로 낮췄다. 한계를 명시한 채")
        A("> 연구 사용을 허용한다.")
    elif v["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS":
        A("> 유상증자 gap 이 연구에 영향을 주지 않는 수준까지 복원됐다.")
    elif v["verdict"] == "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        A("> 원문 복원으로 상당히 줄였지만 사전기준을 넘지 못했다.")
        A("> 기준을 옮기지 않고 BLOCKED 를 유지한다.")
    else:
        A("> 엔진이 가짜 wealth 를 만들거나 anchor 재현에 실패했다. 사용 금지.")
    A("")

    A("## 2. 대상과 원문 확보 (§2·§3·§25)")
    A("")
    A("```")
    A(f"대상 이벤트(R17 ISSUE_METHOD_UNKNOWN)   {tg['targetTotal']:6,}")
    A(f"  그 중 2007~2014                      {tg['targetLegacy2007to2014']:6,}")
    A(f"대상 종목                                {tg['targetTickers']:6,}")
    A(f"고유 접수번호(1차)                       {dl['uniqueReceipts']:6,}")
    A(f"원문 확보                                {dl['retrieved']:6,}")
    A(f"확보 실패                                {dl['failed']:6,}")
    A(f"이번 실행 신규 / 캐시적중               {dl.get('newThisRun', 0):,} / "
      f"{dl.get('cacheHits', 0):,}")
    A("```")
    A("")
    A(f"상태 분포: {dl['statusMix']}")
    A("")
    A("`NOT_ZIP` 은 DART 가 `status 014 파일이 존재하지 않습니다` 를 돌려준 건이다 —")
    A("네트워크 실패가 아니라 **DART 가 그 접수번호의 원문을 보유하지 않는다**.")
    A("재시도로 해결되지 않으므로 재시도하지 않았다.")
    A("")
    if dl.get("windowExpansion"):
        we = dl["windowExpansion"]
        A("### 2차 — 창 안 모든 유상증자 공시로 근거 확장")
        A("")
        A(f"{we['why']}")
        A("")
        A("```")
        A(f"창                    {we['window']}")
        A(f"창 내 후보 접수번호    {we['candidates']:6,}")
        A(f"이번 실행 신규        {we['downloadedThisRun']:6,}")
        A(f"이미 확보             {we['alreadyCached']:6,}")
        A(f"실패                  {we['failed']:6,}")
        A(f"최종 문서 총계        {dl.get('totalDocuments', 0):6,}")
        A("```")
        A("")
        A(f"**범위**: {we['scopeNote']}")
        A("")

    A("## 3. 원문 스키마 — 왜 복원이 가능했나 (§7)")
    A("")
    A("2007년 공시 원문에도 표 셀에 **기계판독 필드코드**(ACODE/AUNIT)가 들어 있다.")
    A("증자방식은 `CI_MTH`, 1주당 신주배정주식수는 `NEW_ASN_CNT`, 신주수는 `CST_CNT`,")
    A("증자전 주식수는 `BFR_CST_CNT` 로 라벨-값 관계가 명시돼 있다. 2007~2023 표본에서")
    A("핵심 코드가 안정적으로 출현했다 — 전체문서 regex 추측이 필요 없었다.")
    A("")
    A("```")
    A(f"파싱한 문서                {pr['documentsParsed']:6,}")
    A(f"구조 경로 비율             {pr['structuralPathPct']:6.1f}%")
    A("```")
    A("")
    A(f"근거 경로: {pr['byEvidencePath']}")
    A("")
    A("**필드 복원률**")
    A("")
    A("| 필드 | 복원 | 비율 |")
    A("|---|---|---|")
    KO = {"rightsRatio": "배정비율(신주/증자전)", "issuePriceCommon": "발행가",
          "recordDate": "신주배정기준일", "paymentDate": "납입일",
          "listingDate": "신주상장예정일", "allocPerShare": "1주당 신주배정주식수",
          "subscriptionStart": "청약 시작일", "boardDate": "이사회결의일"}
    for k, r in pr["fieldRecovery"].items():
        A(f"| {KO.get(k, k)} | {r['n']:,} | {r['pct']}% |")
    A("")

    A("## 4. 증자방식 분포 (§6)")
    A("")
    A("```")
    KM = {"SHAREHOLDER_RIGHTS": "주주배정", "RIGHTS_THEN_PUBLIC": "주주배정후 실권주 공모",
          "PUBLIC_OFFERING": "일반공모", "THIRD_PARTY": "제3자배정", "MIXED": "혼합형",
          None: "(미확인)"}
    for k, n in sorted(pr["byMethod"].items(), key=lambda x: -x[1]):
        A(f"{KM.get(k, str(k)):26} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"신뢰도  " + " · ".join(f"{k} {n:,}" for k, n in pr["byConfidence"].items()))
    A(f"flags   {pr['flags']}")
    A("```")
    A("")
    A("`DATA_CONFLICT` 는 공시된 신주수와 관측된 월간 주식수 변동이 어긋난 경우다.")
    A("공시 신주수는 **계획치**라 실권·부분청약으로 실제 발행량과 갈리는 일이 흔하다.")
    A("이것은 증자방식 판정을 무너뜨리지 않으므로 LOW 가 아니라 MEDIUM 으로 두고,")
    A("**wealth 확정 여부**만 따로 판단했다(아래 §6).")
    A("")

    A("## 5. 정정공시 chain (§9·§10)")
    A("")
    A("```")
    A(f"복수 공시 종목            {ch['tickersWithMultipleFilings']:6,}")
    A(f"조건이 바뀐 chain         {ch['chainsWithChangedTerms']:6,}")
    A("```")
    A("")
    A(f"{ch['policy']}")
    A("")
    if an.get("longestCorrectionChain"):
        c = an["longestCorrectionChain"]
        A(f"가장 긴 chain: **{c['ticker']}** 공시 {c['filings']}건 — "
          f"최초 {c['first']['rcept_no']}({c['first']['date']}) 비율 "
          f"{c['first']['rightsRatio']} → 최종 {c['final']['rcept_no']}"
          f"({c['final']['date']}) 비율 {c['final']['rightsRatio']} · "
          f"조건변경 {c['termsChanged']}")
        A("")

    A("## 6. wealth 확정 논리 — 유형과 wealth 는 다르다 (§18)")
    A("")
    A("창 안 **모든** 유상증자 원문을 근거로 쓴다:")
    A("")
    A("| 창 안 공시 구성 | 판정 | 이유 |")
    A("|---|---|---|")
    A("| 전부 제3자배정·일반공모 | **WEALTH_CONFIRMED** | 어느 것이 원인이든 기존 "
      "주주는 신주를 받지 않는다 → 미조정이 정답. 비율 불일치와 무관하다. |")
    A("| 주주배정 계열 포함 + 조건 확보 | **WEALTH_CONFIRMED** | 배정비율·발행가로 "
      "권리를 계산할 수 있다. |")
    A("| 주주배정 계열 포함 + 조건 불신 | **WEALTH_PARTIAL** | 억지 숫자를 쓰지 않고 "
      "미조정으로 남긴다. 과소평가는 되어도 가짜 wealth 는 만들지 않는다. |")
    A("| 원문 없음·방식 미확인 | **WEALTH_UNRESOLVED** | 모르는 것은 모른다고 둔다. |")
    A("")
    A("```")
    for k, n in sorted(rec["byWealthLayer"].items(),
                       key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {n:6,}")
    A("```")
    A("")

    A("## 7. 전체 2,742건 최종 재분류 (§18)")
    A("")
    A("```")
    for k, n in sorted(rec["byLabel"].items(), key=lambda x: -x[1]):
        A(f"{k:36} {n:6,}")
    A("```")
    A("")
    A("```")
    A(f"근거 출처   " + " · ".join(f"{k} {n:,}" for k, n in rec["bySource"].items()))
    A(f"wealth 확정 {rec['wealthResolved']:,}")
    A("```")
    A("")

    A("## 8. 오분류 감사 (§17)")
    A("")
    A(f"{fp['method']}")
    A("")
    A("| 증자방식 | 모집단 | 표본 | 교차검증 통과 | 오분류율 |")
    A("|---|---|---|---|---|")
    for c in fp["byMethod"]:
        A(f"| {KM.get(c['method'], c['method'])} | {c['population']:,} | "
          f"{c['sampled']} | {c['crossCheckPass']} | {c['falsePositiveRate']} |")
    A("")
    A(f"**전체 오분류율: {fp['falsePositiveRate']}** (표본 {fp['sampled']}건)")
    A("")
    A(f"false negative 단서: {fp['falseNegativeClue']}")
    A("")

    A("## 9. legacy anchor (§16)")
    A("")
    A("| case | 종목 | 시점 | 비율 | 발행가 | 외부납입 | 종료주식 | 권리가치 | "
      "manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|")
    for c in an["cases"]:
        m = c["manual"]
        def f(x, fmt=",.0f"):
            return "-" if x is None else format(x, fmt)
        A(f"| {c['case']} | {c['ticker']} | {c['date']} | "
          f"{f(m['rightsRatio'], '.4f')} | {f(m['issuePrice'])} | "
          f"{f(m['externalContribution'])} | {f(m['endingShares'], '.4f')} | "
          f"{f(m['rightsValue'])} | {m['manualReturn'] * 100:.3f}% | "
          f"{c['engineReturn'] * 100:.3f}% | {c['diffPp']}%p |")
    A("")
    if an["missingCaseTypes"]:
        A(f"확보하지 못한 유형: {', '.join(an['missingCaseTypes'])} — 해당 조건의 "
          "HIGH confidence legacy 사건이 없었다(없는 것을 만들지 않는다).")
        A("")
    A(f"anchor 5건 이상: **{an['atLeastFive']}** · 전부 manual 일치: "
      f"**{an['allPass']}**")
    A("")

    A("## 10. ex-rights 연속성 (§15)")
    A("")
    A(f"{ct['method']}")
    A("")
    A("| 창(개월) | n | R18−R16 차분 중앙값 | 차분 음수건 |")
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

    A("## 11. 미해결 materiality (§19·§21)")
    A("")
    A("```")
    A(f"원 SUSPECTED_RIGHTS          {ma['suspectedTotal']:6,}")
    A(f"wealth 확정                  {ma['wealthResolved']:6,}")
    A(f"wealth 미해결                {ma['wealthUnresolved']:6,}")
    A(f"미해결 고유종목              {ma['unresolvedTickers']:6,}")
    A(f"universe 종목                {ma['universeTickers']:6,}")
    A(f"미해결 종목비율              {ma['unresolvedTickerPct']:6.2f}%")
    A(f"미해결 사건비율              {ma['unresolvedEventPct']:6.2f}%")
    A(f"미해결 월 비율               {ma['unresolvedMonthPct']:6.2f}%")
    A(f"극단 불연속 사건             {ma['extremeDiscontinuityEvents']:6,} "
      f"({ma['extremeDiscontinuityPct']}%)")
    A("```")
    A("")
    A("미해결 사유:")
    A("")
    A("```")
    for k, n in sorted(ma["unresolvedReasons"].items(), key=lambda x: -x[1]):
        A(f"{k:36} {n:6,}")
    A("```")
    A("")
    us = ma["understatementSample"]
    A("```")
    A(f"조건부 중앙값(주주배정일 때)  "
      f"{'-' if us['conditionalMedian'] is None else format(us['conditionalMedian'] * 100, '.2f') + '%'}")
    A(f"P(주주배정)                   "
      f"{'-' if us['pHolderRight'] is None else format(us['pHolderRight'] * 100, '.2f') + '%'}   "
      f"← {us['pHolderRightSource']}")
    A(f"기대 편향                     "
      f"{'-' if us['expectedUnderstatement'] is None else format(us['expectedUnderstatement'] * 100, '.2f') + '%'}   "
      f"← 판정에 쓰는 값")
    A("```")
    A("")
    A(f"{us['methodologyNote']}")
    A("")
    A(f"**편향 방향: {ma['biasDirection']}** — {ma['biasWhy']}")
    A("")

    A("## 12. foundation 재판정 (§20·§22)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:38} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16/R17 승계, 변경 없음):")
    A("")
    A("```")
    for k, val in v["thresholdsFromPrecommit"].items():
        A(f"{k:44} {val}")
    A(f"{'thresholdsUnchanged':44} {v['thresholdsUnchanged']}")
    A("```")
    A("")
    A(f"**판정: {v['verdict']}**")
    A("")

    A("## 13. 기존 연구 상태 (§23)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 14. production 보호 (§30)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 15. HOTG (§31)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 16. 한계")
    A("")
    A(f"- DART 가 원문을 보유하지 않는 접수번호가 있다({dl['failed']:,}건, "
      "status 014). 대부분 2006~2007 구건이며 복구 수단이 없다.")
    A("- 공시 신주수는 계획치다. 실권·부분청약 때문에 실제 발행량과 갈리며, "
      "이 경우 조건을 신뢰하지 않고 보수적으로 미조정으로 남겼다.")
    A("- 발행가가 '-'(미정)인 공시가 있다. 조달금액/신주수 파생값을 쓰며 "
      "우선주·전환권이 섞이면 오차가 난다.")
    A("- 월간 스냅샷이라 권리락과 신주 상장이 다른 달에 찍힌다(R17 실측 중앙값 "
      "2개월). 3개월 미만 horizon 에서는 시차가 남는다.")
    A("- 유상감자·무상감자 구분, 합병 교환비율, 인적분할 배정비율은 여전히 없다 "
      "(R16/R17 한계 그대로, 이번 범위 밖).")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 17. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R19**.")
        A("  새 precommit 을 쓰고 canonical market return · BM · EY · ROE · SIZE ·")
        A("  Quality 를 **같은 조건에서 원점 경쟁**시킨다. R7 의 BM 승리, R13/R14 의")
        A("  Quality 실패, SIZE 강세를 **정답으로 쓰지 않는다**(§36).")
    else:
        A("- 다음 단일 작업: **NO_DIRECT_MATCH 342건의 정체 확정**"
          " — 전환사채 전환 · 신주인수권 행사 · 합병 신주 직접소스 대조.")
        A("")
        A("  판정을 막은 것은 미해결 비율이 아니라 **기대편향 하나**다"
          f"({v['expectedUnderstatementPct']}% > "
          f"{v['thresholdsFromPrecommit']['passWithLimitsMaxUnderstatementPct']}%)."
          " 종목비율 13.31% 는 20% 게이트를 이미 통과했다.")
        A("")
        A("  착수 전 표본 실측(24종목): **95.8%** 가 사건 직전 6개월에 합병·전환사채·")
        A("  신주인수권·주식매수선택권 공시를 갖고 있었다. 즉 이 주식수 증가는 "
          "유상증자가")
        A("  아니라 **전환·행사·합병 신주**로 보이며, 그 유형은 기존 주주에게 신주가")
        A("  지급되지 않으므로 **R16 의 미조정이 이미 정답**이다.")
        A("")
        A("  현재 기대편향은 이 342건에도 P(주주배정)=29.6% 를 적용한 값이라 "
          "**과대추정**")
        A("  일 가능성이 높다. 다만 24종목 표본으로 판정을 바꾸지 않았다 — threshold 도")
        A("  계산식도 그대로 뒀다(§20). 근거: "
          "`reports/research/r18-next-path-probe-latest.json`")
    A("")
    A("- 지금 하지 않는 것: factor 연구 · 포트폴리오 설계 · 합병/인적분할 collector · "
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
                      "progression": d["foundationVerdict"]["progression"],
                      "factorAllowed":
                      d["foundationVerdict"]["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
