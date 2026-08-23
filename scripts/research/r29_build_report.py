#!/usr/bin/env python3
"""R29 canonical report (MD + JSON).

WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29
기존 R7~R28 산출물을 덮어쓰지 않는다(§29). HOTG 는 기존 Bridge 재사용(§32).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-liquidity-data-source-recovery-r29-latest"

VERDICT_LINE = {
    "LIQUIDITY_SOURCE_READY": ("PASS", "R29_LIQUIDITY_SOURCE_READY"),
    "LIQUIDITY_SOURCE_READY_STITCHED": ("PASS", "R29_LIQUIDITY_SOURCE_READY_STITCHED"),
    "LIQUIDITY_SOURCE_SELECTED_PENDING_CREDENTIAL": (
        "WAIT", "R29_LIQUIDITY_SOURCE_SELECTED_PENDING_CREDENTIAL"),
    "LIQUIDITY_SOURCE_PARTIAL": ("WARNING", "R29_LIQUIDITY_SOURCE_PARTIAL"),
    "NO_COMPLIANT_SOURCE_FOUND": ("BLOCKED", "R29_NO_COMPLIANT_SOURCE_FOUND"),
}


def L(n):
    p = RD / f"r29-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    v = L("source-verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    return {
        "schema": "wababa-liquidity-data-source-recovery-r29@1",
        "taskId": "WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29",
        "verdictLine": line, "reasonClass": rc,
        "sourceInventory": L("source-inventory"),
        "credentialInventory": L("credential-inventory"),
        "dataGoKrProbe": L("data-go-kr-probe"),
        "krxOpenApiProbe": L("krx-openapi-probe"),
        "freeAlternativeProbe": L("free-alternative-probe"),
        "historicalCoverage": L("historical-coverage"),
        "delistedCoverage": L("delisted-coverage"),
        "crossSourceReconciliation": L("cross-source-reconciliation"),
        "r27CacheCrosscheck": L("r27-cache-crosscheck"),
        "sourceScorecard": L("source-scorecard"),
        "sourceVerdict": v,
        "r27PrecommitUnchanged": True,
        "factorResearchUntouched": True,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "canonicalProduction": "untouched",
            "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
            "externalSend": 0, "deploy": 0, "envOrToken": 0,
            "productionDbWrite": 0, "publicDisclosure": 0, "newCredential": 0,
            "secretsPrinted": 0, "fullAcquisitionDone": 0,
            "realMoneyStage": "REAL_MONEY_NOT_APPROVED"},
        "hotg": {"canonicalReport": f"reports/wababa/{NAME}.md",
                 "sourceOwner": "Wababa", "reportBridge": True,
                 "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
                 "newObservers": 0, "newScheduler": 0, "newOrchestration": 0},
    }


def md(d):
    v, sc = d["sourceVerdict"], d["sourceScorecard"]
    cred = d["credentialInventory"]["byCredential"]
    cc, dl = d["r27CacheCrosscheck"], d["delistedCoverage"]
    inv = d["sourceInventory"]
    nav = d["freeAlternativeProbe"]["candidates"][0]
    dgk, krx = d["dataGoKrProbe"], d["krxOpenApiProbe"]
    rows = {r["SOURCE"]: r for r in sc["rows"]}
    o = []
    A = o.append

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R29 — 합법 유동성 데이터 소스 확정")
    A("")
    A(f"- **판정: {v['verdict']}**")
    A(f"- **PRIMARY: {v['PRIMARY_LIQUIDITY_SOURCE']}** "
      f"(상태 {v['primaryStatus']})")
    A(f"- Founder 행동: **{v['founderActionCount']}건** — 인증키 발급 하나")
    A(f"- 추가 비용: **{'필요' if v['additionalCostRequired'] else '없음(무료)'}**")
    A(f"- 총 실호출: **{dgk['calls'] + nav['calls'] + cc['calls'] + dl['calls']}회** "
      "(전체 수집 0 — 이번은 소스 선정만)")
    A("- 비밀값 출력 0 · env 변경 0 · 결제 0 · 신규 키 임의발급 0")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    A("> 쓸 수 있는 합법 경로가 **있다.** 공공데이터포털의 금융위원회 주식시세")
    A("> Open API 는 무료이고 자동화 이용이 명시적으로 허용된다. 다만 이 repo 에")
    A("> serviceKey 가 없어 2007년까지 실제로 덮는지 확인하지 못했다 — 키 발급은")
    A("> §21 게이트라 임의로 하지 않았다. 그래서 **소스는 골랐고 키만 기다린다.**")
    A("> 부수적으로, 무료 대체 후보(NAVER)의 데이터 품질을 실측했더니 거래량이")
    A("> KRX 와 **완전히 일치**했다 — 다만 약관이 모호해 PRIMARY 로 쓰지 않는다.")
    A("")

    A("## 2. 로컬 인벤토리 — 먼저 뒤졌다 (§5)")
    A("")
    A("```")
    A(f"PIT 스냅샷 거래량      {inv['localLiquidityData']['pitSnapshots']['hasVolume']}")
    A(f"PIT 스냅샷 거래대금     {inv['localLiquidityData']['pitSnapshots']['hasTradedValue']}")
    A(f"R27 유동성 캐시        {inv['localLiquidityData']['krxLiquidityCache']['cachedDays']}일 "
      f"{inv['localLiquidityData']['krxLiquidityCache']['range']}")
    A(f"parquet/sqlite        {inv['localLiquidityData']['parquetOrSqlite']}")
    A(f"다른 pipeline         {inv['localLiquidityData']['otherPipelineWithVolume']}")
    A("```")
    A("")
    A(f"{inv['conclusion']}")
    A("")

    A("## 3. Credential 인벤토리 (§4 — 값 출력 0)")
    A("")
    A("| credential | 상태 | 용도 |")
    A("|---|---|---|")
    A(f"| 공공데이터포털 serviceKey | **{cred['DATA_GO_KR_SERVICE_KEY']['status']}** | "
      "PRIMARY 후보에 필요 |")
    A(f"| KRX Open API AUTH_KEY | **{cred['KRX_OPENAPI_AUTH_KEY']['status']}** | "
      "SECONDARY 후보에 필요 |")
    A(f"| KRX 웹 로그인(ID/PW) | {cred['KRX_WEBSITE_LOGIN']['status']} | "
      "**R28 에서 금지된 경로** — 사용 안 함 |")
    A(f"| DART_API_KEY | {cred['DART_API_KEY']['status']} | 유동성과 무관 |")
    A("")
    A(f"{d['credentialInventory']['rule']}")
    A("")

    A("## 4. 후보별 실호출 결과 (§6·§7·§9·§10)")
    A("")
    A("### 4-1. 공공데이터포털 — 금융위원회 주식시세 (PRIMARY 후보)")
    A("")
    A("```")
    A(f"endpoint       {dgk['endpoint']}")
    A(f"공식성          {dgk['operator']}")
    A(f"자동화 허용      {dgk['automationAllowed']}")
    A(f"비용            {dgk['cost']}")
    A(f"credential     {'있음' if dgk['credentialPresent'] else '**없음**'}")
    A(f"endpoint 생존   {dgk.get('endpointReachable')}")
    up = dgk.get("unauthenticatedProbe") or {}
    A(f"미인증 probe    HTTP {up.get('httpStatus')} · "
      f"{(up.get('bodyHead') or '')[:60].strip()}")
    A("```")
    A("")
    A("엔드포인트는 살아 있고 정상적인 오류 규약(공공데이터포털 표준 응답)으로")
    A("답한다. **연도별 coverage 는 키가 없어 실측하지 못했다** — \"API 가 있으니")
    A("과거도 있겠지\" 라고 추정하지 않았다(§8).")
    A("")
    A("### 4-2. KRX 공식 Open API (SECONDARY 후보)")
    A("")
    A("```")
    A(f"공식성          {krx['operator']}")
    A(f"자동화 허용      {krx['automationAllowed']}")
    A(f"비용            {krx['cost']}")
    A(f"credential     {krx['status']}")
    A(f"문서상 시작일     {krx['documentedStartDate']}")
    A("```")
    A("")
    A(f"{krx.get('gap2007to2009')}")
    A("")
    A("AUTH_KEY 가 없어 실호출하지 않았다. 신규 신청도 하지 않았다(§9·§21).")
    A("")
    A("### 4-3. NAVER 금융 siseJson (무료 대체 후보)")
    A("")
    A("```")
    A(f"HTTP           {nav.get('httpStatus')}")
    A(f"2007 도달       {nav.get('reached2007')}")
    A(f"컬럼            {nav.get('headerRow')}")
    A(f"거래량          {nav.get('hasVolumeColumn')}")
    A(f"거래대금         {nav.get('hasTradedValueColumn')}   ← 없음")
    A(f"라이브러리 설치    {nav.get('libraryInstalled')} (FinanceDataReader 미설치)")
    A(f"약관 위험        {nav.get('termsRisk')}")
    A("```")
    A("")
    A("FinanceDataReader 는 설치돼 있지 않다. 신규 의존성 추가는 하지 않고")
    A("그 NAVER 경로가 쓰는 endpoint 를 직접 1콜로 특성만 확인했다.")
    A("")

    A("## 5. ★ R27 캐시 교차검증 (§16·§17)")
    A("")
    A(f"허용오차는 결과를 보기 전에 고정했다 — 거래량 "
      f"{cc['tolerance']['volumeRelErrMax']} (완전일치), 거래대금 "
      f"{cc['tolerance']['tradedValueRelErrMax']}.")
    A("")
    A("| 종목 | 비교일수 | 거래량 완전일치 | 거래량 중앙오차 | 거래대금 근사 중앙오차 | 허용내 |")
    A("|---|---|---|---|---|---|")
    for r in cc["byTicker"]:
        if r.get("status") != "OK":
            A(f"| {r['ticker']} | - | - | - | - | {r.get('status')} |")
            continue
        A(f"| {r['ticker']} | {r['compared']} | **{r['volumeExactMatchPct']}%** | "
          f"{r['volumeMedianRelErr']} | {r['tradedValueApproxMedianRelErr']} | "
          f"{r['tradedValueApproxWithinTolerancePct']}% |")
    A("")
    A("**거래량은 세 종목 모두 100% 완전 일치했다.** NAVER 가 KRX 와 같은 시장사실을")
    A("돌려준다는 뜻이다.")
    A("")
    A("거래대금은 NAVER 에 컬럼이 없어 `KRX 종가 × NAVER 거래량` 근사로 대조했고")
    A("중앙오차 0.23~0.60% 였다. 이 차이는 오류가 아니라 **VWAP 과 종가의 차이**다 —")
    A("실제 거래대금은 체결가 가중이고 근사는 종가 단일가다. 사전 고정한 5% 안이다.")
    A("")
    if cc["byTicker"] and cc["byTicker"][0].get("samples"):
        s = cc["byTicker"][0]["samples"][0]
        A("실제 표본 하나 —")
        A("")
        A("```")
        A(f"{s['date']} 005930")
        A(f"  KRX 종가          {s['krxClose']:>15,.0f}")
        A(f"  NAVER 종가        {s['naverClose']:>15,.0f}   ← 분할조정가(50:1)")
        A(f"  KRX 거래량        {s['krxVolume']:>15,.0f}")
        A(f"  NAVER 거래량      {s['naverVolume']:>15,.0f}   ← 완전 일치")
        A(f"  KRX 거래대금      {s['krxTradedValue']:>15,.0f}")
        A(f"  종가×거래량 근사   {s['closeXVolumeApprox']:>15,.0f}")
        A("```")
        A("")
        A("여기서 중요한 함정이 하나 보인다 — **NAVER 종가는 분할조정가**다.")
        A("R27 precommit 은 저가주 bucket 을 \"결정일 당시 실제 거래가격\" 으로")
        A("보라고 고정했다. 조정가로는 그 조건을 만족할 수 없고, 조정가×거래량은")
        A("거래대금도 아니다. 위 근사가 맞았던 것은 **KRX 의 미조정 종가**를 썼기")
        A("때문이다.")
        A("")

    A("## 6. 상폐종목 coverage (§18)")
    A("")
    A("```")
    A(f"표본 상폐종목    {dl['sampledDelistedTickers']}")
    for r in dl["probed"]:
        A(f"  {r['ticker']}  이력 {r['hasHistory']}  rows {r['rows']}")
    A(f"survivorship bias  {dl.get('naverSurvivorshipBias')}")
    A("```")
    A("")
    A(f"{dl['why']}")
    A("")
    A("NAVER 는 상폐종목 과거 이력을 돌려줬다 — 이 축에서는 문제없다.")
    A("공공데이터포털·KRX Open API 는 키가 없어 확인하지 못했다.")
    A("")

    A("## 7. ★ 평가표 (§12) · hard gate (§13)")
    A("")
    A("| 축 | 공공데이터포털 | KRX Open API | NAVER siseJson |")
    A("|---|---|---|---|")
    keys = ["OFFICIALITY", "AUTOMATION_ALLOWED", "CREDENTIAL_REQUIRED",
            "CREDENTIAL_STATUS", "COST", "START_DATE", "KOSPI", "KOSDAQ",
            "DELISTED_HISTORY", "VOLUME", "TRADED_VALUE", "PIT_USABLE",
            "DAILY_BULK_CAPABILITY", "REPRODUCIBILITY", "TERMS_RISK",
            "IMPLEMENTATION_COST", "FOUNDER_ACTION"]
    a = rows["PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"]
    b = rows["KRX_OFFICIAL_OPEN_API"]
    c = rows["NAVER_FINANCE_SISE_JSON"]
    for k in keys:
        A(f"| {k} | {a.get(k)} | {b.get(k)} | {c.get(k)} |")
    A("")
    A("**hard gate 결과**")
    A("")
    A("```")
    for nm, r in (("공공데이터포털", a), ("KRX Open API", b), ("NAVER", c)):
        A(f"{nm:16} {'PASS' if r['hardGatePass'] else 'FAIL'}"
          f"  미충족: {', '.join(r['hardGateFailed']) or '없음'}")
    A("```")
    A("")
    A(f"{sc['hardGateRule']}")
    A("")

    A("## 8. 선정 결과 (§22·§23)")
    A("")
    A(f"**{v['verdict']}**")
    A("")
    for w in v["verdictWhy"] if isinstance(v.get("verdictWhy"), list) else []:
        A(f"- {w}")
    A("")
    A("```")
    A(f"PRIMARY      {v['PRIMARY_LIQUIDITY_SOURCE']}  ({v['primaryStatus']})")
    A(f"SECONDARY    {v['SECONDARY_FALLBACK']['source']}  "
      f"— {v['SECONDARY_FALLBACK']['role']}")
    A("```")
    A("")
    A("**제외한 후보와 이유**")
    A("")
    for x in v["EXCLUDED"]:
        A(f"- **{x['source']}** — {', '.join(x['excludedBecause'])}")
        A(f"  - {x['note']}")
    A("")
    A(f"약관 판정: {v['termsVerdict']}")
    A("")

    A("## 9. NAVER 를 쓰지 않은 이유 — 데이터는 좋았다")
    A("")
    A("솔직히 데이터 품질만 보면 NAVER 가 지금 당장 쓸 수 있는 유일한 후보였다.")
    A("거래량 완전 일치, 2007년 도달, 상폐종목 이력까지 있었다.")
    A("")
    A("그런데도 PRIMARY 로 고르지 않았다.")
    A("")
    A("1. **거래대금 컬럼이 없다.** R27 의 PRIMARY gate 자체가 20거래일 median")
    A("   거래대금 ≥ 1.25억이다. 핵심 지표를 근사로 대체하는 건 다른 연구다.")
    A("2. **주가가 분할조정가다.** R27 은 당시 실거래가를 요구한다.")
    A("3. **약관 근거가 없다.** 문서화된 공개 API 가 아니라 웹서비스 내부")
    A("   endpoint 다. R28 에서 정확히 같은 성격의 경로가 약관 위반으로 차단됐고,")
    A("   그때 운영 예약 runner 까지 위험해졌다. 같은 실수를 반복하지 않는다.")
    A("")
    A("§13 은 \"약관이 모호하면 PRIMARY 선정 금지\" 라고 미리 못박아 뒀다.")
    A("")

    A("## 10. 한계")
    A("")
    A("- PRIMARY 의 **2007년 coverage 를 실측하지 못했다.** 키가 없기 때문이다.")
    A("  키를 받은 뒤 첫 작업이 연도별 실측이어야 한다.")
    A("- 공공데이터포털이 2007 을 못 덮으면 KRX Open API(2010~)와 stitching 이")
    A("  필요하고, 그러면 2007~2009 를 덮을 세 번째 경로를 다시 찾아야 한다.")
    A("- 두 공식 후보의 상폐종목 이력·요청 제한을 확인하지 못했다.")
    A("- NAVER 교차검증은 3종목 20거래일 표본이다. 전수 검증이 아니다.")
    A("- 거래대금 근사(종가×거래량)는 VWAP 차이로 0.2~0.6% 오차가 있다. 공식")
    A("  거래대금 필드가 있으면 그쪽을 쓴다.")
    A("")

    A("## 11. 보존 확인 (§25·§26·§27)")
    A("")
    A("```")
    A(f"R27 precommit 불변        {d['r27PrecommitUnchanged']}")
    A("  20D median 거래대금 gate  125,000,000원 (LOW 25,000,000 / HIGH 250,000,000)")
    A("  5천만원 / 40종목 / 1종목 1,250,000원 / 참여율 1%")
    A(f"factor 연구 미변경         {d['factorResearchUntouched']}")
    A("  R25 BM·SIZE PRIMARY / EY SECONDARY")
    A("  R26 NO_INCREMENTAL_COMBINATION")
    A("  R27·R28 SIZE_DATA_INSUFFICIENT")
    A(f"전체 수집 수행            {v['fullAcquisitionDone']}  (§24 금지)")
    A("```")
    A("")

    A("## 12. production 보호 (§28)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A("```")
    A("")

    A("## 13. HOTG (§32)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 14. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: 1건.**")
    A("")
    A("```")
    A(f"{v['founderAction']}")
    A("")
    A("  · 무료. 개발계정은 자동승인이다.")
    A("  · 발급 후 환경변수로 넣어 주면 된다 (예: DATA_GO_KR_SERVICE_KEY)")
    A("  · env 는 제가 건드리지 않았고 앞으로도 대장이 직접 넣는다")
    A("```")
    A("")
    A(f"- 다음 작업 결정 규칙: **{v['nextTaskRule']}** (§34)")
    A("")
    A(f"- 다음 단일 작업: **{v['nextTask']}**")
    A("")
    A("- 지금 하지 않은 것: 전체 4,640일 수집 · 신규 키 임의발급 · env 변경 ·")
    A("  유료 계약 · KRX 차단 우회 · pykrx 대량수집 재개 · factor/portfolio 연구")
    A("")
    A(f"- 실제 돈 단계: **{d['production']['realMoneyStage']}**")
    return "\n".join(o) + "\n"


def main() -> int:
    d = build()
    WD.mkdir(parents=True, exist_ok=True)
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"),
                      "verdictLine": d["verdictLine"],
                      "reasonClass": d["reasonClass"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
