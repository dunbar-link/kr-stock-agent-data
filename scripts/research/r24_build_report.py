#!/usr/bin/env python3
"""R24 canonical report (MD + JSON).

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24
기존 R16~R23 산출물을 덮어쓰지 않는다(§28). HOTG 는 기존 Bridge 재사용(§30).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-final-unknown-50-direct-classification-r24-latest"

VERDICT_LINE = {
    "CANONICAL_TSR_FOUNDATION_PASS": ("PASS", "R24_CANONICAL_TSR_FOUNDATION_PASS"),
    "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        ("PASS", "R24_CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
    "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS":
        ("BLOCKED", "R24_CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"),
    "CANONICAL_TSR_FOUNDATION_FAIL": ("BLOCKED", "R24_CANONICAL_TSR_FOUNDATION_FAIL"),
}
KO = {"HOLDER_RIGHT_TERMS_MISSING": "주주배정 확정·조건 미확보",
      "TRUE_UNRESOLVED": "완전 미확인",
      "OTHER_CAPITAL_ACTION": "감자 등 기타 자본행위",
      "NO_ENTITLEMENT_CONFIRMED": "권리 없음 확정"}
EV = {"STOCK_SPLIT": "액면분할", "REVERSE_SPLIT": "액면병합",
      "BONUS_ISSUE": "무상증자", "STOCK_DIVIDEND": "주식배당",
      "PREFERRED_CONVERSION": "우선주 → 보통주 전환",
      "CB_CONVERSION": "전환사채 전환", "BW_EXERCISE": "신주인수권 행사",
      "STOCK_OPTION_EXERCISE": "주식매수선택권 행사",
      "MERGER_NEW_SHARES": "합병 신주", "SHARE_EXCHANGE": "주식교환·이전",
      "COMPANY_SPLIT": "회사분할", "CAPITAL_REDUCTION": "감자",
      "TREASURY_SHARE_CANCELLATION": "자기주식 소각",
      "THIRD_PARTY_ALLOCATION": "제3자 배정", "PUBLIC_OFFERING": "일반공모",
      "RIGHTS_ISSUE_EXISTING_SHAREHOLDERS": "구주주 배정 유상증자",
      "RIGHTS_THEN_PUBLIC_UNSUBSCRIBED": "주주배정 후 실권주 일반공모",
      "RIGHTS_FAMILY": "유상증자(방식 미확정)",
      "RIGHTS_FAMILY_OUTCOME": "유상증자 결과공시(방식 미확정)",
      "UNKNOWN": "미확인"}


def L(n):
    p = RD / f"r24-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    tg, src = L("targets-precommit"), L("direct-source-events")
    cls, ent = L("event-classification"), L("shareholder-entitlement")
    ch, sr = L("correction-chains"), L("share-count-reconciliation")
    an, wr = L("anchor-cases"), L("wealth-resolution")
    ct, rec = L("ex-rights-continuity"), L("full-reconciliation")
    bias, mat = L("expected-bias"), L("unresolved-materiality")
    v, sam = L("foundation-verdict"), L("samsung-anchor")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-final-unknown-50-direct-classification-r24@1",
        "taskId": "WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24",
        "verdictLine": line, "reasonClass": rc,
        "targetsPrecommit": {k: tg[k] for k in tg if k != "targets"},
        "directSources": src,
        "eventClassification": {k: cls[k] for k in cls if k != "events"},
        "shareholderEntitlement": {k: ent[k] for k in ent if k != "events"},
        "correctionChains": {k: ch[k] for k in ch if k != "chains"},
        "shareCountReconciliation": {k: sr[k] for k in sr if k != "detail"},
        "anchors": {k: an[k] for k in an if k != "cases"},
        "anchorCases": an["cases"],
        "wealthResolution": {k: wr[k] for k in wr if k != "events"},
        "continuity": ct,
        "reconciliation": {k: rec[k] for k in rec if k != "rows"},
        "expectedBias": bias, "materiality": mat, "foundationVerdict": v,
        "samsungAnchor": sam,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "canonicalProduction": "untouched",
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
    tg, src, cls = d["targetsPrecommit"], d["directSources"], d["eventClassification"]
    ent, sr, an = d["shareholderEntitlement"], d["shareCountReconciliation"], d["anchors"]
    ch, wr, ct = d["correctionChains"], d["wealthResolution"], d["continuity"]
    rec, bias = d["reconciliation"], d["expectedBias"]
    mat, v, sam = d["materiality"], d["foundationVerdict"], d["samsungAnchor"]
    o = []
    A = o.append
    ev = cls["byPrimaryEvent"]

    def n(k):
        return ev.get(k, 0)

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R24 — 마지막 UNKNOWN 50건 직접판정")
    A("")
    A(f"- **FOUNDATION_VERDICT: {v['verdict']}**")
    A(f"- 기대편향 **{bias['beforePct']}% → {bias['afterPct']}%** "
      f"({bias['changePp']:+}%p) · 기준 {bias['thresholdPct']}% "
      f"→ **{'통과' if v['checks']['expectedBiasUnder2pct'] else '미통과'}**")
    A(f"- 미해결 종목비율 **{mat['progression']['R23']}% → "
      f"{mat['unresolvedTickerPct']}%** · 기준 20%")
    A(f"- 50건 중 권리 **YES {ent['byEntitlement'].get('YES', 0)} · "
      f"NO {ent['byEntitlement'].get('NO', 0)} · "
      f"UNKNOWN {ent['byEntitlement'].get('UNKNOWN', 0)}**")
    A(f"- 모집단 추정(P=0.296) 의존 사건 **50건 → "
      f"{bias['decomposition'].get('TRUE_UNRESOLVED', {}).get('events', 0)}건**")
    A(f"- factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["factorResearchAllowed"]:
        A("> 50건 중 32건을 직접 공시로 판정했다. 가장 큰 원인은 데이터가 없는 것이")
        A("> 아니라 **우선주 종목코드가 DART corp_code 에 없어 조회 자체가 안 된")
        A("> 것**이었다(18건). 보통주 코드로 다시 조회하니 공시가 그대로 나왔다.")
        A(f"> 편향은 {bias['beforePct']}% → {bias['afterPct']}% 로 내려와 사전 기준 2% 를")
        A("> 통과했고, 남은 18건은 근거가 없어 UNKNOWN 으로 두었다. §20 에 따라")
        A("> Foundation 을 닫는다 — 남은 자본행위를 100% 복원하는 무한 연구는")
        A("> 시작하지 않는다.")
    else:
        A(f"> 편향 {bias['afterPct']}% 로 기준 2% 를 넘는다. BLOCKED 를 유지한다.")
    A("")

    A("## 2. 대상 50건 사전 고정 (§2)")
    A("")
    A("```")
    A(f"대상                          {tg['targetTotal']:4,}")
    A(f"대상 종목                      {tg['targetTickers']:4,}")
    A(f"corp_code 매핑 성공             {tg['corpCodeMapped']:4,}")
    A(f"corp_code 미매핑                {tg['corpCodeUnmapped']:4,}")
    A(f"우선주 라인 재매핑                {tg['preferredLineRemapped']:4,}")
    A("```")
    A("")
    A(f"{tg['preferredRemapRule']}")
    A("")

    A("## 3. ★ 왜 이 50건이 미확인이었나 (§31 SC1)")
    A("")
    A("R19 는 이 50건에서 자본행위 공시를 **하나도** 찾지 못했다. 원인을 실측했다.")
    A("")
    A("```")
    A(f"우선주 라인이라 corp_code 매핑 실패     {tg['preferredLineRemapped']:4,} / 50")
    A(f"매핑은 됐지만 R19 창(-6~+3)에서 미발견   {50 - tg['preferredLineRemapped']:4,} / 50")
    A("```")
    A("")
    A("DART corpCode 매핑표에는 **보통주 종목코드만** 있다. 009815·000725·001067")
    A("같은 우선주 라인은 자기 코드로 조회하면 결과가 0건이다. 회사는 하나이므로")
    A("같은 회사의 보통주 코드(앞 5자리 + '0')로 조회해야 공시가 나온다.")
    A("추정 매핑을 만들지 않고, 매핑표에 그 코드가 **실제로 존재할 때만** 썼다.")
    A("")

    A("## 4. 직접소스 확보 (§4)")
    A("")
    A("```")
    A(f"조회 corp_code                 {src['corpCodes']:4,}")
    A(f"검색 창                        {src['windowRule']}")
    A(f"창 안 공시                      {src['filingsFound']:4,}")
    A(f"공시가 잡힌 사건                  {src['eventsWithFilings']:4,} / 50")
    A(f"본문 요청                       {src['documentsRequested']:4,}")
    A(f"본문 신규 확보                    {src['documentsNew']:4,}")
    A(f"본문 캐시 재사용                   {src['documentsCacheHits']:4,}")
    A(f"본문 실패(NOT_ZIP 등)            {src['documentsFailed']:4,}")
    A(f"판정에 본문을 실제로 읽은 사건          {cls['docsRead']:4,} / 50")
    A("```")
    A("")
    A(f"{src['reuse']}")
    A("")

    A("## 5. 사건 유형 직접판정 (§5)")
    A("")
    A("| 유형 | 건수 |")
    A("|---|---|")
    for k, c in sorted(ev.items(), key=lambda x: -x[1]):
        A(f"| {EV.get(k, k)} | {c} |")
    A("")
    A("```")
    A(f"CB 전환                       {n('CB_CONVERSION'):3,}")
    A(f"BW 행사                       {n('BW_EXERCISE'):3,}")
    A(f"주식매수선택권 행사                {n('STOCK_OPTION_EXERCISE'):3,}")
    A(f"합병 신주                      {n('MERGER_NEW_SHARES'):3,}")
    A(f"액면분할 / 병합                  {n('STOCK_SPLIT') + n('REVERSE_SPLIT'):3,}")
    A(f"무상증자                       {n('BONUS_ISSUE'):3,}")
    A(f"주식배당                       {n('STOCK_DIVIDEND'):3,}")
    A(f"구주주 배정 유상증자               "
      f"{n('RIGHTS_ISSUE_EXISTING_SHAREHOLDERS') + n('RIGHTS_THEN_PUBLIC_UNSUBSCRIBED'):3,}")
    A(f"제3자배정 / 일반공모               "
      f"{n('THIRD_PARTY_ALLOCATION') + n('PUBLIC_OFFERING'):3,}")
    A(f"우선주 전환                     {n('PREFERRED_CONVERSION'):3,}")
    A(f"회사분할                       {n('COMPANY_SPLIT'):3,}")
    A(f"유상증자(방식 미확정)              "
      f"{n('RIGHTS_FAMILY') + n('RIGHTS_FAMILY_OUTCOME'):3,}")
    A(f"미확인                        {n('UNKNOWN'):3,}")
    A("```")
    A("")

    A("## 6. 기존주주 권리 판정 (§6)")
    A("")
    A("```")
    for k in ("YES", "NO", "UNKNOWN"):
        A(f"{k:8} {ent['byEntitlement'].get(k, 0):3,}")
    A("```")
    A("")
    A("**판정 근거별**")
    A("")
    A("| 근거 | 건수 |")
    A("|---|---|")
    BK = {"DIRECT_EVENT_TYPE": "사건 유형이 곧 권리 여부(분할·무상증자·전환 등)",
          "MERGER_DIRECTION": "합병 방향성 판정(§7)",
          "DOC_ISSUE_METHOD:SHAREHOLDER_RIGHTS": "본문 증자방식 = 주주배정",
          "DOC_ISSUE_METHOD:THIRD_PARTY": "본문 증자방식 = 제3자배정",
          "DOC_ISSUE_METHOD:PUBLIC_OFFERING": "본문 증자방식 = 일반공모",
          "ENTITLEMENT_CONFLICT": "권리 판정이 반대인 후보 경합 → 확정 불가",
          "LOW_CONFIDENCE_NOT_USABLE": "제목 근거뿐 → §14 로 사용 불가",
          "METHOD_UNKNOWN": "유상증자는 맞지만 방식 미확정",
          "UNRESOLVED": "직접 evidence 없음"}
    for k, c in sorted(ent["byBasis"].items(), key=lambda x: -x[1]):
        A(f"| {BK.get(k, k)} | {c} |")
    A("")
    A(f"{ent['rule']}")
    A("")
    A("§7 합병 방향성 — 이 표본은 **주식수가 증가한** 사건이다. 신주를 발행한 쪽이")
    A("분석 대상이고 신주는 상대회사 주주에게 간다. 분석 대상 주주가 대가를 받는")
    A("경우는 주식수 증가가 아니라 상장폐지로 나타난다. R19 논리를 그대로 썼다.")
    A("")

    A("## 7. ★ 자체수정 9건 (§31)")
    A("")
    A("**SC1 우선주 corp_code 미매핑** — 위 §3. 18건이 조회 자체가 안 되고 있었다.")
    A("")
    A("**SC2 본문 수집 대상 불일치** — 초판은 자체 키워드 목록으로 받을 본문을")
    A("골랐는데 '주식배당' 이 빠져 있었고 사건당 6건 상한 때문에 정작 분류기가")
    A("PRIMARY 로 고른 공시가 안 받아졌다(본문 확보 21/50). **분류기가 사건으로")
    A("인정하는 공시**를 그대로 받도록 바꿨다 → 33/50.")
    A("")
    A("**SC3 우선주 종류 필터가 근거 없이 배제** — 우선주 라인 사건에서 보통주")
    A("대상 발행을 무조건 버렸다. 본문을 못 읽은 후보까지 버려서 삼양홀딩스")
    A("우선주는 19건이 전부 탈락해 UNRESOLVED 가 됐다. 본문에서 우선주 신주가")
    A("0 임을 **확인한 경우에만** 버리고, 못 읽었으면 남기되 HIGH 를 막았다.")
    A("")
    A("**SC4 주식배당 유형 미정의** — 분류규칙에는 있는데 유형표에 없어서 4건이")
    A("전부 entitlement UNKNOWN 으로 떨어졌다. 무상증자와 같은 성격이다.")
    A("")
    A("**SC5 무납입 사건의 가격 대조 미사용** — 분할·무상증자·주식배당은 납입이")
    A("없어 주가가 주식수에 반비례한다. R16 이 이미 고정한 비례성 검정을 쓰지")
    A("않고 있었다. R16 의 로그상대오차와 **사전 고정된 허용치**를 그대로 붙였다.")
    A("(§16: marketCap 은 close × shares 라 독립근거가 아니다. 여기서 쓴 것은")
    A("marketCap 이 아니라 price 와 shares 라는 서로 다른 두 관측 계열이다.)")
    A("")
    A("**SC6 우선주 전환의 방향성** — 우선주 전환은 종목에 따라 방향이 반대다.")
    A("보통주 라인에서는 보통주 발행주식수가 **늘어난다**. 초판은 일괄로 '증가를")
    A("설명 못 함' 처리해서, 전환청구 공시가 창 안에 있는 보통주 사건 4건이")
    A("근거를 눈앞에 두고 UNRESOLVED 였다.")
    A("")
    A("**SC7 경합 판정의 동전던지기** — 같은 창에 권리 판정이 **반대인** 후보가")
    A("함께 있고 수량 근거가 없으면, '가장 가까운 공시가 이긴다' 는 규칙이 사실상")
    A("우연이 된다(실측: 무상증자 YES 와 우선주전환 NO 가 한 달 차이로 경합해")
    A("판정이 뒤집혔다). 수량으로 확정되지 않은 경합은 UNKNOWN 으로 남긴다.")
    A("어느 쪽으로도 유리하게 고르지 않았다(§17).")
    A("")
    A("**SC8 분할 공시 표 오독** — 분할 공시는 '전/후' 를 값 옆에 두지 않고 표")
    A("머리글에 둔다. 초판 패턴은 하나도 매칭되지 않아 액면분할 7건이 전부 수량")
    A("근거 없이 MEDIUM 이었다. 라벨 뒤 연속 두 값을 읽도록 바꾸니 **주식 종류별**")
    A("분할 전/후 주식수가 그대로 나왔다 — 7건 전부 오차 0.0 으로 HIGH.")
    A("")
    A("**SC9 비율의 두 가지 의미** — 분할 표의 값은 배수(after/before), 무상증자·")
    A("주식배당·유상증자의 값은 구주 1주당 신주수다. 정합성 대조가 둘을 같게")
    A("다뤄 무상증자 1.00 을 배수로 읽고 4건이 전부 불일치로 나왔다. 의미를 함께")
    A("들고 다니게 했다 → 대조 통과 9/14 → 13/14.")
    A("")
    A("어느 수정도 기준·계산식·threshold 를 건드리지 않았다.")
    A("")

    A("## 8. 신뢰도 (§14)")
    A("")
    A("```")
    for k in ("HIGH", "MEDIUM", "LOW", "UNKNOWN"):
        if k in cls["byConfidence"]:
            A(f"{k:8} {cls['byConfidence'][k]:3,}")
    A("```")
    A("")
    A(f"LOW 는 최종 직접확정으로 쓰지 않았다 — "
      f"{cls['titleOnlyDowngraded']}건이 §14 로 UNKNOWN 강등됐다.")
    A("")

    A("## 9. 정정공시 chain (§15)")
    A("")
    A("```")
    A(f"정정 chain 보유 사건   {ch['count']:3,}")
    A("```")
    A("")
    A(f"{ch['rule']}")
    A("")

    A("## 10. 주식수 정합성 (§16)")
    A("")
    A("```")
    A(f"대조 가능           {sr['checked']:3,} / {sr['rows']}")
    A(f"통과               {sr['passed']:3,}")
    A(f"불일치              {sr['failed']:3,}")
    A(f"대조 불가            {sr['notCheckable']:3,}")
    A(f"허용오차             {sr['tolerance']}")
    A("```")
    A("")
    A(f"{sr['rule']}")
    A("")
    A(f"{sr['onConflict']}")
    A("")

    A("## 11. manual anchor (§17)")
    A("")
    A("```")
    A(f"요구 최소   YES {an['requested']['YES']} · NO {an['requested']['NO']} · "
      f"UNKNOWN {an['requested']['UNKNOWN']}")
    A(f"실제       YES {an['obtained']['YES']} · NO {an['obtained']['NO']} · "
      f"UNKNOWN {an['obtained']['UNKNOWN']}")
    A(f"최소 충족   {an['meetsMinimums']}")
    A(f"전체 검증   {an['allEventsAnchored']}건 (50건 전부)")
    A(f"전부 일치   {an['allPass']}")
    A(f"허용오차    {an['tolerancePp']}%p")
    A("```")
    A("")
    A("대표 사례 — 각 판정 유형 하나씩:")
    A("")
    A("| 종목 | 시점 | 사건 | 수령자 | 구주 | 신주 | 발행가 | 외부납입 | manual | engine | 오차 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|")
    seen = set()
    for c in d["anchorCases"]:
        key = c["entitlement"]
        if key in seen and len([x for x in seen]) >= 3:
            continue
        if key in seen:
            continue
        seen.add(key)
        m = c["manual"]
        A(f"| {c['ticker']} | {c['date']} | {EV.get(c['event'], c['event'])} | "
          f"{c['recipient']} | {m['oldShares']:.0f} | {m['newShares']:.4f} | "
          f"{m['issuePrice'] or '-'} | {m['externalContribution']:.0f} | "
          f"{m['expectedWealthAdjustment'] * 100:.3f}% | "
          f"{c['engineWealthAdjustment'] * 100:.3f}% | {c['errorPp']}%p |")
    A("")
    A(f"최대 오차: **{max(abs(c['errorPp']) for c in d['anchorCases']):.6f}%p** · "
      f"정책 출처: {an['policySource']}")
    A("")

    A("## 12. wealth resolution (§12·§13)")
    A("")
    A("```")
    for k, c in sorted(wr["byWealthStatus"].items(), key=lambda x: -x[1]):
        A(f"{k:22} {c:3,}")
    A("```")
    A("")
    A("```")
    AK = {"NONE_NO_ENTITLEMENT": "권리 없음 확정 → 조정 0",
          "MECHANICAL_R16": "무납입 → R16 기계적 조정",
          "RIGHTS_R17_POLICY_A": "구주주 배정 → R17 POLICY_A",
          "UNKNOWN": "미확정 → 조정 없음"}
    for k, c in sorted(wr["byAdjustmentType"].items(), key=lambda x: -x[1]):
        A(f"{AK.get(k, k):28} {c:3,}")
    A("```")
    A("")
    A(f"{wr['hardRule']}")
    A("")
    A("**§13 — NO 가 나왔다고 주가 움직임을 지우는 것이 아니다.** 존재하지 않는")
    A("주주 권리를 만들어내지 않는다는 뜻이고, 그때 조정 0 이 정답이다.")
    A("")

    A("## 13. 공짜 wealth · 연속성")
    A("")
    A("```")
    A(f"검증 유상증자 사건      {ct['rightsEventsTested']:5,}")
    A(f"당월 차분 중앙값        {ct['eventMonthMedianDiff'] * 100:6.2f}%")
    A(f"이론 권리가치           {ct['theoreticalMedian'] * 100:6.2f}%")
    A(f"이론과 일치             {ct['diffMatchesTheory']}")
    A(f"공짜 wealth            {ct['freeWealthEvents']:5,}건")
    A(f"연속성 판정             {'PASS' if ct['pass'] else 'FAIL'}")
    A("```")
    A("")
    A(f"{ct['freeWealthRule']}")
    A("")

    A("## 14. 전체 2,742건 재분류 · materiality")
    A("")
    A("```")
    for k, c in sorted(rec["byWealthLayer"].items(), key=lambda x: -(x[1] or 0)):
        A(f"{str(k):22} {c:6,}")
    A("```")
    A("")
    A("```")
    A(f"미해결 사건            {mat['wealthUnresolved']:6,}")
    A(f"미해결 고유종목          {mat['unresolvedTickers']:6,}")
    A(f"universe 종목          {mat['universeTickers']:6,}")
    A(f"미해결 종목비율          {mat['unresolvedTickerPct']:6.2f}%")
    A(f"미해결 사건비율          {mat['unresolvedEventPct']:6.2f}%")
    A(f"미해결 월 비율           {mat['unresolvedMonthPct']:6.2f}%")
    A(f"미해결 규모 비중         {mat['unresolvedMagnitudePct']:6.2f}%")
    A(f"극단 불연속             {mat['extremeDiscontinuityEvents']:6,} "
      f"({mat['extremeDiscontinuityPct']}%)")
    A("```")
    A("")
    A(f"**편향 방향: {mat['biasDirection']}** — {mat['biasWhy']}")
    A("")

    A("## 15. 기대편향 (§18)")
    A("")
    A("```")
    A(f"공식               {bias['formula']}")
    A(f"공식 R23 동일        {bias['formulaUnchangedFromR23']}")
    A(f"분모 규칙            {bias['denominatorRule']}")
    A(f"조건부 크기          {bias['conditionalMedian'] * 100:.2f}%")
    A(f"미해결 사건          {bias['unresolvedEvents']:,}")
    A(f"유효 P              {bias['effectiveP'] * 100:.2f}%")
    A("")
    A(f"before             {bias['beforePct']}%")
    A(f"after              {bias['afterPct']}%")
    A(f"변화                {bias['changePp']:+}%p")
    A(f"기준                {bias['thresholdPct']}%")
    A(f"bias gate          {'PASS' if bias['afterPct'] <= 2.0 else 'FAIL'}")
    A("```")
    A("")
    A(f"**R23 감도 대비**: {bias['projectionVsActual']}")
    A("")
    A("| 원인 | 건수 | 평균 P | P 근거 | 기여 |")
    A("|---|---|---|---|---|")
    for k, c in sorted(bias["decomposition"].items(),
                       key=lambda x: -x[1]["biasContributionPct"]):
        A(f"| {KO.get(k, k)} | {c['events']:,} | {c['avgP']:.3f} | "
          f"{c['pSource']} | {c['biasContributionPct']:.3f}%p |")
    A("")
    A("**진행**")
    A("")
    A("```")
    for k in ("R18", "R19", "R20", "R21", "R22", "R23", "R24"):
        A(f"{k}  {bias['progression'][k]:6.3f}%")
    A("```")
    A("")

    A("## 16. foundation 판정 (§19·§20)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:42} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A("사전 고정 기준(R16~R23 승계, 변경 없음):")
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
    if v["verdict"].endswith("PASS_WITH_LIMITATIONS"):
        A("완전 PASS 가 아닌 이유는 하나다 — 극단 불연속 사건 비율이")
        A(f"{mat['extremeDiscontinuityPct']}% 로 사전 기준 1% 를 넘는다. 나머지 조건은")
        A("전부 충족했다. 이것이 곧 아래 §18 의 잔여 한계다.")
        A("")
    A(f"{v['closureRule']}")
    A("")

    A("## 17. 삼성전자 anchor 보존 (§24)")
    A("")
    A("```")
    A(f"구간                {sam['period']['start']} ~ {sam['period']['end']} "
      f"({sam['period']['years']}년)")
    A(f"R15 raw price       {sam['r15PriceReturnPct']:8.2f}%")
    A(f"R15 분할조정          {sam['r15SplitAdjustedPct']:8.2f}%")
    for k, c in sam["comparison"].items():
        A(f"{k:20} R15 {c['r15Pct']:8.2f}%  R24 {c['r24Pct']:8.2f}%  "
          f"차이 {c['diffPp']:+.4f}%p")
    A(f"{'보존':20} {sam['preserved']}")
    A("```")
    A("")
    A(f"{sam['why']}")
    A("")

    A("## 18. 잔여 한계 (정량)")
    A("")
    A("```")
    RK = {"REDUCTION_PAID_OR_FREE_UNKNOWN": "감자 유상/무상 미구분",
          "R24_UNRESOLVED": "R24 직접 evidence 없음",
          "R23_ALLOCATION_STILL_MISSING": "주주배정 배정량 미확보",
          "R21_UNRESOLVED": "R21 미해결",
          "R24_HOLDER_RIGHT_TERMS_MISSING": "R24 주주배정 조건 미확보",
          "R19_UNRESOLVED": "R19 미해결"}
    for k, c in sorted(mat["unresolvedReasons"].items(), key=lambda x: -x[1]):
        A(f"{RK.get(k, k):30} {c:4,}")
    A("```")
    A("")
    A("| 이 군을 전부 해결하면 | 건수 | 결과 편향 |")
    A("|---|---|---|")
    for r in bias["gateSensitivity"]["ifGroupFullyResolved"]:
        A(f"| {KO.get(r['resolveGroup'], r['resolveGroup'])} | {r['events']} | "
          f"{r['biasIfFullyResolvedPct']}% |")
    A("")
    A("- 감자 60건은 유상감자(현금 환급)와 무상감자를 공시명만으로 구분할 수 없다.")
    A("  기여도는 0%p 다(권리 없음으로 조건화). 남은 최대 material 잔여지만 편향을")
    A("  올리지 않는다.")
    A("- 완전 미확인 18건은 여전히 모집단 추정 P=0.296 에 의존한다. 그중 10건은")
    A("  창 안에 자본행위 공시가 **하나도** 없다(합병 상대회사 corp_code 로 공시가")
    A("  나간 경우 포함).")
    A("- 극단 불연속 39건(1.42%)이 미조정으로 남는다. 완전 PASS 를 막는 유일한")
    A("  조건이다.")
    A("- 백테스트 인프라이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.")
    A("")

    A("## 19. 기존 연구 상태 (§22)")
    A("")
    A("```")
    for k, val in v["legacyResearchStatus"].items():
        A(f"{k:14} {val}")
    A("```")
    A("")

    A("## 20. production 보호 (§25)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 21. HOTG (§30)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 22. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 DART 조회 범위.**")
    A("")
    if v["factorResearchAllowed"]:
        A("- 다음 단일 작업: **WABABA-CANONICAL-FACTOR-REDISCOVERY-R25**")
        A("")
        A("  R7~R14 를 TSR 로 다시 계산하는 작업이 **아니다**. 새 precommit 으로")
        A("  원점에서 검증한다 — MARKET baseline · BM/value · EY/value · ROE ·")
        A("  SIZE · Quality · Dividend/주주환원 계열. R7~R14 의 승자·패자를")
        A("  prior truth 로 쓰지 않는다(§23).")
        A("")
        A("  R24 안에서 R25 를 실행하지 않았다(§34).")
    else:
        A("- 다음 단일 작업: 잔여 최대 material group 하나만 처리.")
    A("")
    A("- 지금 하지 않는 것: factor 성과 연구 · 포트폴리오 · 보유기간 · CAGR 전략비교 ·")
    A("  남은 자본행위 100% 복원 무한연구 · 홈페이지 공개 · 실계좌 자금")
    A("")
    A(f"- §23 — factor 연구 재개 가능: **{v['factorResearchAllowed']}**")
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
                      "biasBefore": d["expectedBias"]["beforePct"],
                      "biasAfter": d["expectedBias"]["afterPct"],
                      "unresolvedTickerPct": d["materiality"]["unresolvedTickerPct"],
                      "factorAllowed":
                      d["foundationVerdict"]["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
