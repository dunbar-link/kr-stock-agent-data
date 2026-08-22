#!/usr/bin/env python3
"""R28 canonical closeout report (MD + JSON).

WABABA-R27-LIQUIDITY-DATA-RESUME-R28
기존 R7~R27 산출물을 덮어쓰지 않는다(§38). HOTG 는 기존 Bridge 재사용(§40).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-r27-liquidity-data-resume-r28-latest"


def L(pfx, n):
    p = RD / f"{pfx}-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    res, cov = L("r28", "collection-resume"), L("r28", "collection-coverage")
    fr, rex = L("r28", "failure-retry"), L("r28", "r27-reexecution")
    fv = L("r28", "final-verdict")
    return {
        "schema": "wababa-r27-liquidity-data-resume-r28@1",
        "taskId": "WABABA-R27-LIQUIDITY-DATA-RESUME-R28",
        "verdictLine": "BLOCKED",
        "reasonClass": "R28_BLOCKED_KRX_TOS_AUTOMATED_BULK_PROHIBITED",
        "collectionResume": res, "collectionCoverage": cov,
        "failureRetry": fr, "r27Reexecution": rex, "finalVerdict": fv,
        "r27PrecommitUnchanged": True,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "canonicalProduction": "untouched",
            "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
            "externalSend": 0, "deploy": 0, "envOrToken": 0,
            "productionDbWrite": 0, "publicDisclosure": 0, "newCredential": 0,
            "realMoneyStage": "REAL_MONEY_NOT_APPROVED"},
        "hotg": {"canonicalReport": f"reports/wababa/{NAME}.md",
                 "sourceOwner": "Wababa", "reportBridge": True,
                 "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
                 "newObservers": 0, "newScheduler": 0, "newOrchestration": 0},
    }


def md(d):
    res, cov = d["collectionResume"], d["collectionCoverage"]
    fr, rex, fv = d["failureRetry"], d["r27Reexecution"], d["finalVerdict"]
    o = []
    A = o.append

    def g(x, nd=2, suf="%"):
        return "-" if x is None else f"{x:.{nd}f}{suf}"

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R28 — R27 유동성 수집 재개 시도 · closeout")
    A("")
    A("- **수집을 재개하지 않았다.** KRX 가 이 수집을 **약관 위반**으로 명시 통보했다.")
    A(f"- 필요 {res['requiredDays']:,}일 · 보유 {res['alreadyValid']}일 · "
      f"잔여 {res['pending']:,}일 · coverage **{cov['dayCoveragePct']}%**")
    A(f"- 원인 분류: **{fr['rootCauseClass']}** — collector bug 아님"
      f"(bug 가설 {len(fr['ruledOut'])}개 전부 배제)")
    A(f"- SIZE 판정: **{fv['sizeVerdict']}** (R27 그대로, 새 taxonomy 0)")
    A(f"- R25/R26 재현: **{rex['allReproductionExact']}**")
    A("- 실주문 0 · 유료데이터 0 · 신규 credential 0 · production write 0")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    A("> 차단은 풀렸지만 **재개하지 않았다.** KRX 가 돌려준 것은 장애 메시지가")
    A("> 아니라 약관 위반 통보였다 — \"자동화 수단을 이용한 정보 무단 수집·복제·")
    A("> 배포를 금지\"(이용약관 제10조 제2호). 같은 대량 수집을 반복하면 위반을")
    A("> 알고도 되풀이하는 것이고, 재차단되면 **pykrx 를 매일 쓰는 운영 예약")
    A("> runner 가 함께 죽는다.** 남은 4,529일은 KRX 가 지정한 공식 경로 3개 중")
    A("> 하나로만 받을 수 있고, 셋 다 Founder 결정이 필요하다.")
    A("")

    A("## 2. ★ 수집 재개를 멈춘 이유 (§13 원인 분리)")
    A("")
    A("KRX 로그인 API 가 JSON 대신 HTML 을 돌려주고 있었다. 그 HTML 을 직접 열어")
    A("확인한 실제 문구다.")
    A("")
    A("```")
    A("KDM 이용 제한 안내")
    A("")
    A("자동화 수단을 통한 비정상 대량 조회가 감지되어 해당 IP의 접속이")
    A("일시적으로 제한되었습니다. KRX Data Marketplace 이용약관 제10조 제2호는")
    A("자동화 수단을 이용한 정보 무단 수집·복제·배포를 금지하고 있으며,")
    A("제6조 제2항에 따라 약관 위반시 홈페이지 이용이 제한될 수 있습니다.")
    A("해당 IP는 탐지일로부터 1일간 접속이 제한되며 ...")
    A("")
    A("일정 기간의 데이터를 일괄적으로 이용하고자 하는 경우")
    A("KRX Data Marketplace의 화면 다운로드 기능 이용, 데이터 상품 구입 또는")
    A("KRX Open API(openapi.krx.co.kr) 등 공식 경로를 이용해 주시기 바랍니다.")
    A("```")
    A("")
    A("**이것은 버그도 일시 장애도 아니다.** §13 이 요구한 원인 분리를 실측으로")
    A("끝냈다 — 아래 가설을 전부 배제했다.")
    A("")
    for x in fr["ruledOut"]:
        A(f"- {x}")
    A("")
    A("결정적 증거: 로그인 **페이지**는 HTTP 200 으로 정상 응답했고(사이트 정상,")
    A("IP 도달 가능) **로그인 API 만** 이용제한 페이지를 반환했다. 네트워크·인코딩·")
    A("파라미터 문제였다면 이런 형태가 나오지 않는다.")
    A("")

    A("## 3. 차단은 풀렸다 — 그래도 재개하지 않았다")
    A("")
    A("```")
    A(f"차단 통보 시각        2026-08-22 (탐지일로부터 1일 제한)")
    A(f"단발 확인            2026-08-23 · 로그인 페이지 200 · 이용제한 페이지 없음")
    A(f"대량 수집 재개        하지 않음")
    A("```")
    A("")
    A("재개하지 않은 이유는 두 가지다.")
    A("")
    A("**첫째, 약관이다.** 차단이 해제됐다는 것이 \"이제 해도 된다\"는 뜻은 아니다.")
    A("KRX 는 자동화 대량 수집 자체를 금지한다고 명시했고, 재탐지 시 제한이")
    A("재적용된다고 예고했다. 알고도 반복하는 것은 다른 종류의 행위다.")
    A("")
    A("**둘째, 운영 위험이다.** pykrx 는 이 프로젝트의 **운영 예약 runner** 가")
    A("매일 쓴다 — 15:40 signal · 16:20 fund plan · 16:25 auto apply. 재차단되면")
    A("연구 하나 때문에 **운영 파이프라인 전체가 멈춘다.** §36 은 production 변경을")
    A("금지하는데, 재차단으로 production 을 멈추게 하는 것은 그보다 나쁘다.")
    A("")
    A(f"{fv['productionRisk']}")
    A("")

    A("## 4. 재발 방지 — 약관 게이트 (§41 자체수정 3)")
    A("")
    A("`r27_collect.py` 에 게이트를 넣었다. 앞으로 어떤 세션도 승인 없이 대량")
    A("수집을 시작할 수 없다.")
    A("")
    A("```")
    A(f"승인 없는 최대 수집일    {fr['guard']['bulkLimitWithoutApproval']}일")
    A(f"승인 방법               환경변수 {fr['guard']['approvalEnv']}=1")
    A(f"차단 시 exit code       {fr['guard']['exitCode']}")
    A("```")
    A("")
    A("실측 확인 — 지금 실행하면 `BLOCKED_KRX_TOS` 로 즉시 멈춘다(exit 3).")
    A("")
    A("R27 에서 넣은 자체수정 2건도 유효하다.")
    A("")
    for x in fr["collectorFixesApplied"]:
        A(f"- {x}")
    A("")

    A("## 5. 수집 상태 정확 산출 (§3·§4)")
    A("")
    A("```")
    A(f"required_days          {res['requiredDays']:>6,}   (결정일 236 × 직전 20거래일 합집합)")
    A(f"already_valid          {res['alreadyValid']:>6,}   (schema·행수·sha 검증 통과)")
    A(f"newly_collected        {res['newlyCollected']:>6,}   (약관 게이트로 0)")
    A(f"pending                {res['pending']:>6,}")
    A(f"invalid/cache-corrupt  {res['invalidOrCorrupt']:>6,}")
    A(f"retries_needed         {res['retriesNeeded']:>6,}")
    A(f"캐시 유효 행수           {res['rowsInValidCache']:>6,}")
    A("```")
    A("")
    A(f"R27 보고 수집량 {res['r27ReportedCollected']}일과 **일치**"
      f"({res['matchesR27Report']}) — 차이 없음.")
    A("")
    A(f"{res['reuseRule']}")
    A("")
    A(f"보유 구간: {res['cachedRange'][0]} ~ {res['cachedRange'][1]} "
      f"(전체 필요 구간의 앞부분만)")
    A("")

    A("## 6. coverage gate — 완화하지 않았다 (§11·§12)")
    A("")
    A("```")
    A(f"거래일 coverage          {cov['dayCoveragePct']}%")
    A(f"연도별 요구 coverage      {cov['gateFromR27Precommit']['minCoveragePctPerYear']}%")
    A(f"요구 충족 연도            {cov['yearsMeetingThreshold']} / {cov['minYearsCovered']}")
    A(f"gate 통과                {cov['coverageGatePass']}")
    A(f"gate 변경 여부            {'없음' if cov['gateUnchanged'] else '변경됨'}")
    A("```")
    A("")
    A(f"{cov['note']}")
    A("")

    A("## 7. R27 재실행 상태 (§14·§15·§16)")
    A("")
    A(f"R27 precommit 변경 여부: **{'없음' if rex['precommitUnchanged'] else '변경됨'}** "
      f"(`{rex['precommitPath']}`)")
    A("")
    A("**SIZE R25 spread 재현**")
    A("")
    A("| horizon | R25 정본 | 재현 | 일치 |")
    A("|---|---|---|---|")
    for k, x in rex["sizeReproduction"].items():
        A(f"| {k} | {g(x['r25SpreadAnnPct'], 3, '%p')} | "
          f"{g(x['r27SpreadAnnPct'], 3, '%p')} | {'○' if x['exactMatch'] else '×'} |")
    A("")
    A("**R26 TOP 연율 TSR 재현**")
    A("")
    A("| horizon | SIZE 정본 | SIZE 재현 | BM 정본 | BM 재현 | 일치 |")
    A("|---|---|---|---|---|---|")
    for k in rex["sizeReproductionR26Top"]:
        s = rex["sizeReproductionR26Top"][k]
        b = rex["bmReproductionR26Top"][k]
        A(f"| {k} | {g(s['r26TopAnnPct'])} | {g(s['r27TopAnnPct'])} | "
          f"{g(b['r26TopAnnPct'])} | {g(b['r27TopAnnPct'])} | "
          f"{'○' if s['exactMatch'] and b['exactMatch'] else '×'} |")
    A("")
    A(f"**전부 일치: {rex['allReproductionExact']}** — 지시문이 §15·§16 에 적은 값")
    A("(SIZE 12M 35.336 / 36M 13.320 / 60M 10.752 / 84M 9.063 %p, SIZE TOP 36M")
    A("19.147%, BM TOP 36M 15.010%)과 정확히 같다. 기존 결과는 흔들리지 않았다.")
    A("")
    A(f"R27 pipeline 은 실행했고 **{rex['r27RunStoppedAt']}** 에서 멈췄다.")
    A("")
    A("미수행 분석 (coverage 미달 상태에서 사실처럼 계산하지 않는다 — §12):")
    A("")
    A("```")
    for x in rex["downstreamNotRunList"]:
        A(f"  × {x}")
    A("```")
    A("")

    A("## 8. 답하지 못한 질문 — R27과 동일")
    A("")
    A("> SIZE_SMALL canonical alpha 가 당시 실제로 거래 가능한 종목에서도 유지되는가?")
    A("")
    A("**여전히 답하지 못했다.** SIZE 19.147% 가 실행 가능하다는 증거도, 불가능하다는")
    A("증거도 없다. R26 의 `LIQUIDITY_DATA_INCOMPLETE` 한계는 그대로다.")
    A("")
    A("R28 이 새로 확정한 것은 **왜 못 받는지**다. 데이터가 없어서가 아니라")
    A("**자동화로 받는 것이 금지돼 있어서**다. 이건 pacing 을 늦춰서 풀리는 문제가")
    A("아니다.")
    A("")

    A("## 9. 남은 경로 — 셋 다 Founder 결정")
    A("")
    A("KRX 가 직접 지정한 공식 경로다.")
    A("")
    A("| 경로 | 주체 | 비용 | 현재 막힌 이유 |")
    A("|---|---|---|---|")
    for p in fr["sanctionedPaths"]:
        A(f"| {p['path']} | {p['who']} | {p['cost']} | {p['blockedBy']} |")
    A("")
    A("세 경로 모두 제가 자율로 실행할 수 없다 — ①은 사람이 직접 받아야 하고,")
    A("②는 유료(현재 정책 0), ③은 신규 credential 발급(승인 게이트)이다.")
    A("")
    A("참고로 **필요량은 크지 않다.** 4,529일 × 전 종목 1행 = 압축 후 대략 200MB")
    A("수준이고, 이미 받은 111일이 4.8MB 다. 경로만 열리면 분석은 즉시 끝난다 —")
    A("기준·gate·민감도가 전부 R27 precommit 에 고정돼 있어 재설계할 것이 없다.")
    A("")

    A("## 10. 한계")
    A("")
    A("- coverage 2.392% — 판정 불가의 직접 원인. R27 과 동일하다.")
    A(f"- 거래정지 데이터는 여전히 **{cov['suspension']['status']}** 다.")
    A("- 이번 차단은 제 수집 방식이 유발했다. R27 초판 collector 가 세션 끊김 후")
    A("  548건을 헛되이 재요청한 것이 탐지를 앞당겼을 가능성이 높다.")
    A("- 게이트를 넣었지만 `WABABA_KRX_BULK_APPROVED=1` 로 우회 가능하다. 이는")
    A("  의도된 설계다 — 사람이 공식 경로를 확보한 뒤 여는 스위치다.")
    A("")

    A("## 11. production 보호 (§36)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'r27PrecommitUnchanged':22} {d['r27PrecommitUnchanged']}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 12. HOTG (§40)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 13. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: 결정 1건.** 아래 셋 중 하나를 골라 주면 그 경로로 진행한다.")
    A("")
    A("```")
    A("① KRX Data Marketplace 화면 다운로드")
    A("   - 비용 0, 약관 위반 아님")
    A("   - Founder 가 직접 받아야 함 (자동화 불가)")
    A("")
    A("② KRX Open API (openapi.krx.co.kr) 키 발급")
    A("   - 비용 0, 공식 경로, 자동화 허용")
    A("   - 신규 credential 발급 = 승인 게이트")
    A("   - 개인적으로 이 경로를 권한다 — 한 번 열면 이후 연구가 자유로워진다")
    A("")
    A("③ 데이터 상품 구입")
    A("   - 유료. 현재 정책상 유료데이터 0 이라 별도 승인 필요")
    A("```")
    A("")
    A(f"- 다음 작업 결정 규칙: **{fv['nextTaskRule']}** (§34)")
    A("")
    A(f"- 다음 단일 작업: **{fv['nextTask']}**")
    A("")
    A("- 지금 하지 않은 것: 대량 수집 재개 · coverage gate 완화 · 최근 구간만")
    A("  primary 로 변경 · 불완전 데이터로 before/after 산출 · 포트폴리오 설계 ·")
    A("  새 factor")
    A("")
    A(f"- 실제 돈 단계: **{d['production']['realMoneyStage']}**")
    return "\n".join(o) + "\n"


def main() -> int:
    d = build()
    WD.mkdir(parents=True, exist_ok=True)
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"),
                      "verdictLine": d["verdictLine"],
                      "reasonClass": d["reasonClass"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
