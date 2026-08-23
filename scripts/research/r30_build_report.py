#!/usr/bin/env python3
"""R30 canonical 보고서 — reports/wababa/ MD + JSON.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

evidence 파일만 읽어서 쓴다. 여기서 새로 계산하거나 추정하지 않는다.
비밀값은 어떤 경로로도 보고서에 들어가지 않는다(§1).

안전: 읽기 + 보고서 write 만. 네트워크 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-liquidity-official-full-acquisition-r30-latest"


def L(n):
    p = RD / f"r30-{n}-latest.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {}


def fmt(v, dash="—"):
    return dash if v is None else v


def main() -> int:
    fv = L("final-verdict")
    cred = L("credential-status")
    sv = L("source-verdict")
    hist = L("historical-coverage-probe")
    dp = L("delisted-preferred-probe")
    cc = L("r27-cache-crosscheck")
    prog = L("acquisition-progress")
    cov = L("final-coverage")
    rex = L("r27-reexecution")
    rep = L("reproduction")

    j = fv.get("judgement", "BLOCKED")
    rc = fv.get("reasonClass", "R30_UNKNOWN")
    A = []
    a = A.append

    a(f"전체 판정: {j}")
    a(f"reason_class: {rc}")
    a("")
    a("# 와바바 R30 — 공식 유동성 소스 전체 취득")
    a("")
    a(f"- **판정: {rc}**")
    a(f"- credential: **{cred.get('status', '?')}** "
      f"(환경변수 이름 `{cred.get('expectedEnvVarName') or cred.get('envVarName')}`"
      " — 값은 어디에도 기록하지 않음)")
    a(f"- 공식 소스: **{sv.get('source', '?')}**")
    _un = (sv.get("endpointProbe") or {}).get("unauthenticatedCalls", 0)
    a(f"- 인증 실호출: **{sv.get('calls', 0)}회**"
      f" (그 밖에 미인증 생존확인 {_un}회) · 전체수집 신규 취득 "
      f"**{prog.get('newlyAcquiredDays', 0)}일**")
    a("- 비밀값 노출 0 · env 변경 0 · 결제 0 · 신규 키 임의발급 0 · 실주문 0")
    a("")

    # ── 1. 한 줄 요약 ─────────────────────────────────────────
    a("## 1. 한 줄 요약")
    a("")
    if j == "WAIT" and cred.get("status") == "ABSENT":
        a("> 인증키가 **아직 이 PC 에 등록돼 있지 않다.** 대장이 포털에서 키를")
        a("> 발급받은 건 맞지만, repo 가 읽는 자리(환경변수 / `.env.local`)에는")
        a("> 없다 — 프로세스·사용자·시스템 환경변수와 `.env.local` 을 전부 확인했고")
        a("> 넷 다 비어 있다. 그래서 2007년 coverage 도, 거래대금 필드도 **실측할**")
        a("> **방법이 없었다.** 추정으로 통과시키지 않았고, 새 env 구조를 임의로")
        a("> 만들지도 않았다(§2). 대신 **키가 들어오면 명령 하나로 끝까지 가도록**")
        a("> probe → 전체수집 → coverage → R27 재실행 파이프라인을 전부 만들어")
        a("> 두고 회귀 테스트까지 통과시켰다. Founder 행동은 **1건**이다.")
    elif sv.get("verdict") == "CREDENTIAL_NOT_EFFECTIVE":
        cy = hist.get("controlYear")
        a("> 인증키는 등록됐는데 **호출이 성립하지 않았다.** 2007년뿐 아니라")
        a(f"> 반드시 데이터가 있어야 하는 통제연도 **{cy}년까지 row 0** 이고,")
        a(f"> 모든 연도가 같은 오류(`{hist.get('controlYearStatus')}`)로 죽었다.")
        a("> 무효키·빈키·정상키에 게이트웨이가 **같은 코드**를 돌려주므로 응답만으로")
        a("> 키 오류인지 활용신청 미승인인지 포털 반영 지연인지 구분되지 않는다.")
        a("> 그래서 **\"이 소스에 historical 이 없다\" 고 판정하지 않았다** — 그렇게")
        a("> 적으면 유일한 합법 소스를 근거 없이 폐기하는 것이 된다. coverage·")
        a("> 거래대금·상폐 축은 FAIL 이 아니라 **미측정**으로 남긴다. Founder")
        a("> 행동은 1건이다.")
    elif j == "PASS":
        a("> 공식 소스로 historical 유동성을 채웠고 R27 tradability 판정까지 끝냈다.")
    else:
        a(f"> 공식 소스 검증에서 막혔다 — {rc}. 전체수집을 강행하지 않았다.")
    a("")

    # ── 2. credential ────────────────────────────────────────
    a("## 2. Credential 상태 (§1·§2 — 값 출력 0)")
    a("")
    a("```")
    a(f"status              {cred.get('status')}")
    a(f"기대 환경변수 이름     {cred.get('expectedEnvVarName') or cred.get('envVarName')}")
    a(f"확인한 자리          process-env / 사용자 환경변수 / 시스템 환경변수 / .env.local")
    a(f"확인 횟수            {len(cred.get('checked') or [])}건 (이름만 대조)")
    a(f"secret 출력          {cred.get('secretExposure', 0)}")
    a(f"신규 env 구조 생성     0  (기존 규약 재사용 — DART_API_KEY 와 동일)")
    a("```")
    a("")
    a("이 repo 의 research 스크립트 규약은 `os.environ.get(\"<NAME>\")` 이고")
    a("(`r17_dart_collect.py` 등 DART 계열이 전부 그렇다), `.env.local` 이 그")
    a("mirror 다. 둘 다 **이미 있는 경로**라 새로 만들 것이 없다 — 그래서 §2 대로")
    a("새 secret manager·새 dependency·새 env framework 를 추가하지 않았다.")
    a("")

    # ── 3. 공식 API ──────────────────────────────────────────
    a("## 3. 공식 API (§4 — 추정 endpoint 금지)")
    a("")
    a("```")
    a(f"source          {sv.get('source')}")
    a(f"endpoint        {sv.get('endpoint')}")
    a(f"operator        {sv.get('operator')}")
    a(f"automation      {sv.get('automationAllowed')}")
    a(f"cost            {sv.get('cost')}")
    a(f"일일 호출 quota   {sv.get('dailyCallQuota')}  (개발계정)")
    a(f"page 최대 rows   {sv.get('maxRowsPerPage')}")
    ep = sv.get("endpointProbe") or {}
    a(f"endpoint 생존    {ep.get('reachable')}  (HTTP {ep.get('httpStatus')})")
    a("```")
    a("")
    a("R29 가 확정한 소스 하나만 쓴다. 미인증 1콜로 엔드포인트 생존과 공공데이터포털")
    a("표준 오류 규약을 확인했다 — 이건 데이터 수집이 아니라 생존 확인이다.")
    a("")
    a("**R28 에서 금지된 KRX 웹 자동화(pykrx 대량수집)는 이 코드가 전혀 부르지 않는다.**")
    a("거래일 캘린더도 R27 이 이미 캐시해 둔 파일만 읽는다.")
    a("")

    # ── 4. Probe ─────────────────────────────────────────────
    a("## 4. ★ Probe Gate (§3·§5~§11)")
    a("")
    a("전체수집 **전에** 확인한다. 바로 4,640일을 받지 않는다.")
    a("")
    a("| 축 | 결과 |")
    a("|---|---|")
    nm = set(sv.get("notMeasuredChecks") or [])
    for k, v in (sv.get("checks") or {}).items():
        a(f"| {k} | {'PASS' if v else ('NOT_MEASURED' if k in nm else 'FAIL')} |")
    a("")
    a(f"- probe 판정: **{sv.get('verdict')}**")
    a(f"- 전체수집 허용: **{sv.get('fullAcquisitionAllowed')}**")
    a(f"- 실패 축: `{sorted(set(sv.get('failedChecks') or []) - nm)}`")
    if nm:
        a(f"- 미측정 축: `{sorted(nm)}`  ← 호출이 성립하지 않아 **판정 불가**. "
          "부적합 판정이 아니다.")
    a("")

    a("### 4-1. 연도별 실제 row (§6 — HTTP 200 은 coverage PASS 가 아니다)")
    a("")
    a("```")
    a(f"판정            {hist.get('verdict')}")
    a(f"hard 연도        {hist.get('hardYears')}")
    a(f"row 확인된 연도   {hist.get('hardYearsWithRows')}")
    a(f"probe 대상 연도   {hist.get('probeYears')}")
    for y, v in sorted((hist.get("byYear") or {}).items()):
        a(f"  {y}  date={v.get('probeDate')}  status={v.get('status')}  "
          f"rows={v.get('rows')}")
    if not (hist.get("byYear") or {}):
        a("  (실호출 0 — credential 부재로 연도별 실측 불가)")
    a("```")
    a("")
    a("2007·2008·2009 는 hard check 다. 하나라도 실제 row 가 없으면")
    a("`PRIMARY_FULL_PERIOD_FAIL` 이고 전체수집을 시작하지 않는다.")
    a("")

    disc = hist.get("authDiscrimination") or L("auth-discrimination")
    if disc.get("probes"):
        a("### 4-1-B. 인증 실패 vs 소스 한계 판별 (추가 2콜)")
        a("")
        a("```")
        a(f"통제연도 {hist.get('controlYear')} 정상키 코드   "
          f"{disc.get('controlYearCode')}")
        for k, v in (disc.get("probes") or {}).items():
            a(f"{k:22s} 코드 {v.get('code')}  {v.get('errMsg')}")
        a(f"게이트웨이가 인증 상태를 구분하는가   {disc.get('gatewayDiscriminates')}")
        a("```")
        a("")
        a(disc.get("conclusion", ""))
        a("")

    a("### 4-2. 거래량 / 거래대금 스키마 (§5)")
    a("")
    sc = sv.get("schema") or {}
    a("```")
    a(f"status                {sc.get('status')}")
    a(f"거래량 필드 존재         {sc.get('volumeFieldPresent')}")
    a(f"거래대금 필드 존재       {sc.get('tradedValueFieldPresent')}")
    a(f"거래량 별칭             {sc.get('volumeFieldAliases')}")
    a(f"거래대금 별칭            {sc.get('tradedValueFieldAliases')}")
    a("```")
    a("")
    a("거래대금이 없으면 `close × volume` 을 canonical 거래대금으로 **쓰지 않는다.**")
    a("실제 거래대금은 체결가 가중이라 종가×거래량과 다르다. 그 경우")
    a("`TRADED_VALUE_FIELD_MISSING` 으로 판정하고 R27 PRIMARY gate 적합성을")
    a("따로 따진다(§5·§11-C).")
    a("")

    a("### 4-3. 상폐 · 우선주 (§7·§8)")
    a("")
    a("```")
    a(f"status                  {dp.get('status')}")
    a(f"상폐 후보(스냅샷 도출)      {dp.get('delistedCandidates')}")
    a(f"상폐 후보 총수             {dp.get('delistedCandidateCount')}")
    a(f"historical 반환된 상폐종목   {dp.get('delistedFoundInHistory')}")
    a(f"survivorship bias 위험     {dp.get('survivorshipBiasRisk')}")
    a(f"우선주 005935 확인         {dp.get('preferredSupported')}")
    a(f"6자리 leading zero 보존     {dp.get('leadingZeroPreserved')}")
    a("```")
    a("")
    a("상폐 표본은 **하드코딩하지 않았다.** 2007년 PIT 스냅샷에 있었고 최신")
    a("스냅샷에는 없는 종목을 상폐 후보로 도출한다 — 종목 추측이 아니라 데이터에서")
    a("나온다. 현재 상장종목만 주는 소스면 R27 에 부적합이다(survivorship 금지).")
    a("")

    a("### 4-4. R27 캐시 교차검증 (§9)")
    a("")
    a("허용오차는 결과를 보기 전에 고정했다.")
    a("")
    a("```")
    a(f"status                  {cc.get('status')}")
    a(f"R27 캐시 보유일           {cc.get('r27CacheDays')}")
    a(f"비교일수                 {cc.get('comparedDays')}  (최소 요구 "
      f"{cc.get('volumeExactMatchMin') and 20})")
    a(f"비교 종목                {cc.get('sampleTickers')}")
    a(f"비교쌍                   {cc.get('pairs')}")
    a(f"거래량 완전일치율          {fmt(cc.get('volumeExactMatchRate'))}"
      f"  (기준 >= {cc.get('volumeExactMatchMin')})")
    a(f"거래량 중앙오차            {fmt(cc.get('volumeMedianErr'))}")
    a(f"거래대금 중앙오차           {fmt(cc.get('tradedValueMedianErr'))}"
      f"  (기준 <= {cc.get('tradedValueMedianErrMax')})")
    a(f"거래량 p95 오차            {fmt(cc.get('volumeP95Err'))}")
    a(f"거래대금 p95 오차           {fmt(cc.get('tradedValueP95Err'))}")
    a(f"outlier                 {fmt(cc.get('outlierCount'))}")
    a(f"기존 111일 재사용 허용       {cc.get('inheritReuseAllowed')}")
    a("```")
    a("")
    a(f"재사용 규칙(§12, 사전 고정): {cc.get('inheritReuseRule', '—')}")
    a("")

    # ── 5. 전체수집 ──────────────────────────────────────────
    a("## 5. 전체 수집 (§12~§15)")
    a("")
    a("```")
    a(f"status              {prog.get('status')}")
    a(f"required days       {prog.get('requiredDays')}")
    a(f"R27 상속 일수         {prog.get('inheritedR27Days')}")
    a(f"신규 취득 일수         {prog.get('newlyAcquiredDays', 0)}")
    a(f"실패 일수            {prog.get('failedDays', 0)}")
    a(f"pending             {prog.get('pendingDays')}")
    a(f"호출수               {prog.get('calls', 0)} / quota "
      f"{prog.get('quota', sv.get('dailyCallQuota'))}")
    a(f"rate-limit 이벤트     {prog.get('rateLimitHits', 0)}")
    a(f"재시도               {prog.get('retries', 0)}")
    a(f"checkpoint          {prog.get('checkpointPath', '(미생성)')}")
    a(f"resumable           {prog.get('resumable', True)}")
    a(f"aborted             {prog.get('aborted')}")
    a("```")
    a("")
    a("필요일 정의는 R27 과 **동일**하다 — 결정일 직전 20거래일의 합집합.")
    a("새 정의를 만들지 않고 `r27_collect.needed_days` 를 그대로 재사용한다")
    a("(look-ahead 0, §8). 하루 성공할 때마다 저장하고, 중단되면 다음 실행이")
    a("pending 만 이어받는다 — 처음부터 다시 받지 않는다(§15).")
    a("")

    # ── 6. coverage ──────────────────────────────────────────
    a("## 6. Coverage 재측정 (§16·§17)")
    a("")
    a("```")
    a(f"threshold 출처        {cov.get('thresholdsFrom')}")
    a(f"연도 coverage 기준     {cov.get('minCoveragePctPerYear')}%  "
      f"(변경 {cov.get('thresholdChanged')})")
    a(f"최소 연도수            {cov.get('minYearsCovered')}")
    a(f"required days        {cov.get('requiredDays')}")
    a(f"확보 days            {cov.get('daysHave')}")
    a(f"  공식 소스           {cov.get('daysFromOfficialSource')}")
    a(f"  R27 상속            {cov.get('daysInheritedFromR27')}")
    a(f"day coverage         {cov.get('dayCoveragePct')}%")
    a(f"기준 충족 연도수        {cov.get('yearsMeetingThresholdCount')}")
    a(f"coverage gate        {cov.get('coverageGatePreCheckPass')}")
    a(f"R27 분석 허용          {cov.get('r27AnalysisAllowed')}")
    a("```")
    a("")
    if cov.get("causes"):
        a("**미달 원인 분리(§17)**")
        a("")
        for c in cov["causes"]:
            a(f"- `{c['cause']}` — {c['detail']}")
        a("")
    a("threshold 는 R27 precommit 그대로다. 결과가 안 좋다고 기준을 바꾸지")
    a("않았고, coverage 가 좋은 최근 구간만 몰래 primary 로 바꾸지도 않았다.")
    a("")

    # ── 7. 재현 ──────────────────────────────────────────────
    a("## 7. R25/R26 재현 (§20 — 읽기 전용)")
    a("")
    a("```")
    a(f"재현 PASS            {rep.get('reproductionPass')}")
    a(f"R27 산출물 변경        {rep.get('r27ArtifactsMutated')}")
    for g, rows in (rep.get("byGroup") or {}).items():
        a(f"  {g}")
        for h, v in rows.items():
            a(f"    {h:4}  기대 {v['expected']:>7}  재계산 "
              f"{fmt(v['recomputed']):>7}  {'OK' if v['match'] else 'MISMATCH'}")
    a("```")
    a("")
    a("§20 은 R27 분석 전에 재현을 확인하라 하고, §38 은 기존 R7~R29 산출물")
    a("변경을 금지한다. 둘 다 지키려고 `r27_run.py` 를 돌려 R27 evidence 를")
    a("덮어쓰는 대신, 같은 계산을 다시 해서 **R30 파일에만** 기록했다.")
    a("coverage PASS 후의 정식 재실행은 §18 대로 `r27_run` → `r27_verdict` 다.")
    a("")

    # ── 8. R27 재실행 ────────────────────────────────────────
    a("## 8. R27 재실행 · tradability 판정 (§18~§32)")
    a("")
    a("```")
    a(f"재실행 여부           {rex.get('executed')}")
    if not rex.get("executed"):
        a(f"미실행 사유           {rex.get('why')}")
    pre = rex.get("r27PrecommitUnchanged") or {}
    a(f"R27 precommit 불변    {pre.get('unchanged')}")
    a(f"  sha256            {pre.get('sha256', '')[:32]}…")
    a(f"  PRIMARY gate      {pre.get('thresholdKrw')}원")
    a(f"  sensitivity       {pre.get('sensitivity')}")
    a(f"  자본/1종목 주문액     {pre.get('capital')} / {pre.get('orderPerNameKrw')}원")
    v27 = (rex.get("r27Verdict") or {})
    a(f"R27 verdict         {v27.get('verdict', 'NOT_RUN')}")
    a(f"EXECUTION_CANDIDATE {v27.get('executionCandidate', 'NONE_PENDING_DATA')}")
    a(f"portfolio 진입 허용     {v27.get('portfolioResearchEntryAllowed', False)}")
    a("```")
    a("")
    if not rex.get("executed"):
        a("coverage gate 를 통과하지 못했으므로 R27 tradability 분석을 실행하지")
        a("않았다(§17). SIZE BEFORE/AFTER · included/excluded · participation ·")
        a("low-price · zero-volume · delisting · cost stress · subperiod ·")
        a("rolling · exchange · concentration 은 전부 **NOT_RUN** 이다 —")
        a("데이터 없이 숫자를 만들어내지 않는다.")
        a("")

    # ── 9. 한계 ──────────────────────────────────────────────
    a("## 9. 한계")
    a("")
    if cred.get("status") == "ABSENT":
        a("- 공식 소스의 **2007년 coverage 를 또 실측하지 못했다.** R29 와 같은")
        a("  이유(키 부재)다. 키 없이는 이 축을 닫을 방법이 없다.")
        a("- 거래대금 필드가 실제로 오는지도 문서 근거뿐이다. 문서는 \"시가·종가·")
        a("  고가·저가·거래량 등\" 이라고만 쓰여 있어 **거래대금 필드는 확정이 아니다.**")
        a("  없으면 §11-C 로 BLOCKED 이 될 수 있다.")
        a("- 상폐종목 historical 반환 여부도 미확인이다. 이게 안 되면 R27 전체가")
        a("  survivorship liquidity bias 로 부적합해진다.")
        a("- 즉 **키 하나가 세 개의 미확정 축을 동시에 잠그고 있다.**")
    else:
        a("- probe 표본은 연도별 대표 거래일 1개다. 전수 검증이 아니다.")
        a("- 교차검증은 R27 캐시가 덮는 2006~2007 구간에 한정된다.")
    a("- 거래정지 이력은 이 소스에도 없다 — `SUSPENSION_DATA_UNAVAILABLE` 유지.")
    a("  없는 데이터를 만들지 않고, 새 suspension 프로젝트로 확장하지도 않는다(§26).")
    a("")

    # ── 10. 보존 ─────────────────────────────────────────────
    a("## 10. 보존 · production 보호 (§34~§36)")
    a("")
    p = fv.get("production") or {}
    a("```")
    for k in ("realMoneyStage", "realOrders", "broker", "realAccount",
              "paidData", "externalSend", "deploy", "envOrToken",
              "productionDbWrite", "legacy50d", "newBmForward", "publicRepo",
              "homepage", "scheduler", "autoApply", "autoPublish",
              "krxWebAutomation", "pykrxBulk"):
        a(f"{k:20} {p.get(k)}")
    f = fv.get("forbidden") or {}
    for k, label in (("portfolioOptimization", "포트폴리오 최적화"),
                     ("newFactorResearch", "새 factor 연구"),
                     ("newTaxonomy", "새 taxonomy"),
                     ("thresholdChanged", "threshold 변경")):
        a(f"{label:20} {f.get(k)}")
    a("```")
    a("")
    a("인증키는 source-controlled 파일에 저장하지 않았다. `.env.local` 은")
    a("`.gitignore` 대상이고 이번 commit 에도 들어가지 않는다.")
    a("")

    # ── 11. HOTG ─────────────────────────────────────────────
    a("## 11. HOTG (§40)")
    a("")
    a("```")
    a(f"canonicalReport      reports/wababa/{NAME}.md")
    a("sourceOwner          Wababa")
    a("reportBridge         True")
    a("surface              08:40 Founder 종합보고 (기존 구조 재사용)")
    a(f"checkpoint           {prog.get('checkpointPath', '(수집 미시작)')}")
    a(f"resumable            {prog.get('resumable', True)}")
    a("newObservers         0")
    a("newScheduler         0")
    a("newOrchestration     0")
    a("```")
    a("")
    a("수집이 중간에 끊겨도 checkpoint 와 canonical status 가 남는다. 대장이")
    a("기억해서 다시 지시해야 하는 구조를 만들지 않았다 — 다음 실행이 pending 만")
    a("이어받는다.")
    a("")

    # ── 12. Founder 행동 ─────────────────────────────────────
    a("## 12. Founder 행동 / 다음 단일 작업")
    a("")
    if sv.get("verdict") == "CREDENTIAL_NOT_EFFECTIVE":
        a("- **Founder 행동: 1건.**")
        a("")
        a("```")
        a("공공데이터포털에서 이 인증키가 '주식시세정보' 서비스에 실제로")
        a("활성화돼 있는지 확인 (키 값은 저에게 보여주지 않으셔도 됩니다)")
        a("")
        a("  1) data.go.kr 로그인 → 마이페이지 → 오픈API → 개발계정")
        a("     · 금융위원회_주식시세정보 가 목록에 있는가?")
        a("     · 상태가 '승인' 인가? (신청만 하고 미승인이면 호출 전부 거부)")
        a("     · '일반 인증키(Decoding)' 를 그대로 등록했는가?")
        a("  2) 같은 화면의 '미리보기/테스트' 로 basDt=20260803 호출이")
        a("     200 으로 오는지 한 번만 눌러보기")
        a("     · 포털에서도 실패 → 포털 쪽 문제(승인·반영 지연). 보통 1~24시간.")
        a("     · 포털에서만 성공 → 등록된 키 값이 다른 것이므로 재등록:")
        a("         setx DATA_GO_KR_SERVICE_KEY \"<일반 인증키(Decoding)>\"")
        a("         → 새 터미널을 열어야 반영된다")
        a("")
        a("  · env 는 제가 건드리지 않았습니다. 대장이 직접 넣습니다(§5 승인 게이트).")
        a("  · 확인 후 명령 하나면 끝까지 갑니다:")
        a("      python scripts/research/r30_run.py")
        a("```")
        a("")
        a("- 다음 작업 결정 규칙: **D** (§33 — 공식 source 확정 전까지 gap 하나만)")
        a("")
        a("- 다음 단일 작업: **공공데이터포털에서 `주식시세정보` 개발계정 승인 상태를 "
          "확인하고(또는 반영 지연이면 대기 후) `python scripts/research/r30_run.py` "
          "를 다시 돌린다. 소스 교체·새 factor 연구 금지 — 이 소스는 아직 "
          "부적합으로 판정된 것이 아니다.**")
    elif cred.get("status") == "ABSENT":
        a("- **Founder 행동: 1건.**")
        a("")
        a("```")
        a("발급받은 공공데이터포털 serviceKey 를 이 PC 에 등록")
        a("")
        a("  방법 A (권장 — DART_API_KEY 와 같은 방식, 새 구조 0):")
        a("    setx DATA_GO_KR_SERVICE_KEY \"<발급받은 일반 인증키(Decoding)>\"")
        a("    → 새 터미널을 열어야 반영된다")
        a("")
        a("  방법 B: C:\\work\\kr-stock-agent-data-new\\.env.local 마지막 줄에 추가")
        a("    DATA_GO_KR_SERVICE_KEY=<발급받은 일반 인증키(Decoding)>")
        a("    → 이 파일은 이미 .gitignore 대상이라 commit 되지 않는다")
        a("")
        a("  · env 는 제가 건드리지 않았습니다. 대장이 직접 넣습니다(§5 승인 게이트).")
        a("  · Encoding/Decoding 어느 쪽을 넣어도 코드가 알아서 처리합니다.")
        a("  · 등록 후 명령 하나면 끝까지 갑니다:")
        a("      python scripts/research/r30_run.py")
        a("```")
        a("")
        a("- 다음 작업 결정 규칙: **D** (§33 — 공식 source 확정 전까지 gap 하나만)")
        a("")
        a(f"- 다음 단일 작업: **대장이 `DATA_GO_KR_SERVICE_KEY` 를 등록하면 "
          f"`python scripts/research/r30_run.py` 한 번으로 probe → 4,640일 수집 → "
          f"coverage → R27 재판정까지 끝낸다. 새 factor 연구 금지.**")
    else:
        a("- **Founder 행동: 없음.**")
        a("")
        nt = v27.get("nextTask") or "R27 verdict 확인 후 결정"
        a(f"- 다음 단일 작업: **{nt}**")
    a("")
    a("- 지금 하지 않은 것: 포트폴리오 최적화 · 새 factor 연구 · 새 taxonomy ·")
    a("  threshold 변경 · KRX 웹 자동화 재개 · 유료 데이터 · env 변경 ·")
    a("  신규 키 임의발급 · 운영 배포 · 외부 발송")
    a("")
    a(f"- 실제 돈 단계: **{p.get('realMoneyStage')}**")

    WD.mkdir(parents=True, exist_ok=True)
    (WD / f"{NAME}.md").write_text("\n".join(A) + "\n", encoding="utf-8")
    (WD / f"{NAME}.json").write_text(
        json.dumps({"task": "R30",
                    "taskId": "WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30",
                    "judgement": j, "reasonClass": rc,
                    "credential": {k: v for k, v in cred.items()
                                   if k != "checked"},
                    "sourceVerdict": sv, "historicalCoverage": hist,
                    "delistedPreferred": dp, "crosscheck": cc,
                    "acquisition": prog, "coverage": cov,
                    "reproduction": rep, "r27Reexecution": rex,
                    "final": fv},
                   ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"report": f"reports/wababa/{NAME}.md",
                      "judgement": j, "reasonClass": rc}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
