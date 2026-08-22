#!/usr/bin/env python3
"""R27 canonical report (MD + JSON).

WABABA-SIZE-TRADABILITY-VALIDATION-R27
기존 R7~R26 산출물을 덮어쓰지 않는다(§38). HOTG 는 기존 Bridge 재사용(§40).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-size-tradability-validation-r27-latest"

VERDICT_LINE = {
    "SIZE_EXECUTABLE_STRONG": ("PASS", "R27_SIZE_EXECUTABLE_STRONG"),
    "SIZE_EXECUTABLE_PROMISING": ("PASS", "R27_SIZE_EXECUTABLE_PROMISING"),
    "SIZE_ALPHA_LARGELY_NONTRADABLE": ("PASS", "R27_SIZE_ALPHA_LARGELY_NONTRADABLE"),
    "SIZE_FRAGILE": ("WARNING", "R27_SIZE_FRAGILE"),
    "SIZE_DATA_INSUFFICIENT": ("BLOCKED", "R27_SIZE_DATA_INSUFFICIENT"),
}

# §38 요구 산출물 — 없는 것은 숨기지 않고 NOT_RUN 으로 표시한다.
REQUIRED = ["precommit", "data-inventory", "liquidity-coverage",
            "size-reproduction", "bm-reference", "tradability-definition",
            "before-after", "participation", "low-price", "included-excluded",
            "delisting-distress", "subperiod-rolling", "exchange",
            "concentration", "bm-vs-size", "verdict"]


def L(n):
    p = RD / f"r27-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    v = L("verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    art = {n: ("PRESENT" if (RD / f"r27-{n}-latest.json").exists() else "NOT_RUN")
           for n in REQUIRED}
    d = {
        "schema": "wababa-size-tradability-validation-r27@1",
        "taskId": "WABABA-SIZE-TRADABILITY-VALIDATION-R27",
        "verdictLine": line, "reasonClass": rc,
        "precommit": L("precommit"),
        "dataInventory": L("data-inventory"),
        "liquidityCoverage": L("liquidity-coverage"),
        "sizeReproduction": L("size-reproduction"),
        "bmReference": L("bm-reference"),
        "tradabilityDefinition": L("tradability-definition"),
        "beforeAfter": L("before-after"),
        "participation": L("participation"),
        "lowPrice": L("low-price"),
        "includedExcluded": L("included-excluded"),
        "delistingDistress": L("delisting-distress"),
        "subperiodRolling": L("subperiod-rolling"),
        "exchange": L("exchange"),
        "concentration": L("concentration"),
        "bmVsSize": L("bm-vs-size"),
        "verdict": v,
        "artifactStatus": art,
        "priorArtifactsPreserved": True,
        "production": {
            "publicRepo": "untouched", "legacy50d": "untouched",
            "newBmForward": "untouched", "homepage": "untouched",
            "scheduler": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "canonicalProduction": "untouched",
            "realOrders": 0, "broker": 0, "realAccount": 0, "paidData": 0,
            "externalSend": 0, "deploy": 0, "envOrToken": 0,
            "productionDbWrite": 0, "publicDisclosure": 0,
            "newCredential": 0,
            "realMoneyStage": "REAL_MONEY_NOT_APPROVED"},
        "hotg": {"canonicalReport": f"reports/wababa/{NAME}.md",
                 "sourceOwner": "Wababa", "reportBridge": True,
                 "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
                 "newObservers": 0, "newScheduler": 0, "newOrchestration": 0},
    }
    return d


def md(d):
    pc, v = d["precommit"], d["verdict"]
    inv, cov = d["dataInventory"], d["liquidityCoverage"]
    sz, bm = d["sizeReproduction"], d["bmReference"]
    gate = pc["primaryGate"]
    o = []
    A = o.append

    def g(x, nd=2, suf="%"):
        return "-" if x is None else f"{x:.{nd}f}{suf}"

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R27 — SIZE_SMALL 거래가능성 검증")
    A("")
    A(f"- **판정: {v['verdict']}**")
    A(f"- R25/R26 SIZE·BM 재현: **{sz['allExactMatch']}** (전 horizon 정확 일치)")
    A(f"- 유동성 데이터 확보: **{inv['daysCollectedTotal']:,} / "
      f"{inv['daysRequired']:,}일 = {inv['coveragePct']}%**")
    A(f"- coverage gate: **미달** (90%↑ 연도 {cov['yearsMeetingThreshold']}개 / "
      f"요구 {cov['minYearsCovered']}개)")
    A("- 거래가능성 필터 적용 분석은 **수행하지 않았다** — 근거 없는 숫자를 만들지 않는다.")
    A(f"- 실주문 0 · 브로커 0 · 유료데이터 0 · 신규 credential 0")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    A("> SIZE 초과수익이 실제로 살 수 있었던 것인지 확인하려면 **당시 거래대금**이")
    A("> 필요하다. 그 데이터를 이 repo 가 이미 쓰는 KRX 경로로 받을 수 있음은")
    A("> 확인했지만(컬럼·기간 실측 확인), 4,640 거래일 중 111일을 받은 시점에서")
    A("> **KRX 소스가 막혔다.** 2.39% coverage 로는 거래가능성을 판정할 수 없다.")
    A("> 사전에 고정한 coverage gate 대로 `SIZE_DATA_INSUFFICIENT` 로 종료하고,")
    A("> **SIZE 19.15% 는 아직 실행 가능하다고도 불가능하다고도 말하지 않는다.**")
    A("")

    A("## 2. 재현 게이트 — 통과 (§6·§7)")
    A("")
    A("새 조건을 적용하기 전에 R25/R26 을 먼저 재현했다. 전부 정확히 일치한다.")
    A("")
    A("**SIZE_SMALL — R25 spread (자체 universe)**")
    A("")
    A("| horizon | R25 | R27 재현 | 일치 |")
    A("|---|---|---|---|")
    for k, x in sz["r25"].items():
        A(f"| {k} | {g(x['r25SpreadAnnPct'], 3, '%p')} | "
          f"{g(x['r27SpreadAnnPct'], 3, '%p')} | {'○' if x['exactMatch'] else '×'} |")
    A("")
    A("**R26 TOP 연율 TSR (공유 universe)**")
    A("")
    A("| horizon | SIZE R26 | SIZE R27 | BM R26 | BM R27 | 일치 |")
    A("|---|---|---|---|---|---|")
    for k in sz["r26"]:
        s, b = sz["r26"][k], bm["r26"][k]
        A(f"| {k} | {g(s['r26TopAnnPct'])} | {g(s['r27TopAnnPct'])} | "
          f"{g(b['r26TopAnnPct'])} | {g(b['r27TopAnnPct'])} | "
          f"{'○' if s['exactMatch'] and b['exactMatch'] else '×'} |")
    A("")
    A(f"R26 anchor 36M — SIZE **{sz['r26Anchor36mTop']}%** · BM "
      f"**{bm['r26Anchor36mTop']}%** 모두 재현됐다. 즉 이번 BLOCKED 는 기존 결과가")
    A("흔들려서가 아니라 **새 데이터를 못 받아서**다.")
    A("")

    A("## 3. 유동성 데이터 — 소스는 맞았고 수집이 막혔다 (§3·§4)")
    A("")
    A("**소스 확인 (§3 우선순위대로)**")
    A("")
    A("```")
    A(f"1순위 기존 repo/cache   : PIT 스냅샷에 거래량·거래대금 컬럼 **없음** (실측)")
    A(f"2순위 KRX 기존 구조     : {inv['source']}")
    A(f"                         → {inv['sourceReuse']}")
    A(f"반환 컬럼               : {', '.join(inv['columns'])}")
    A("실측 확인               : 2007-01-02 / 2015-04-01 / 2026-08-21 모두 정상 반환")
    A("```")
    A("")
    A("**이 repo 의 PIT 스냅샷 빌더가 이미 쓰는 바로 그 함수다.** 새 crawler·새")
    A("framework 를 만들지 않았고, 유료 데이터 0 · 신규 key 0 이다.")
    A("")
    A("**수집 결과**")
    A("")
    A("```")
    A(f"필요 거래일        {inv['daysRequired']:>6,}   (결정일 236개 × 직전 20거래일 합집합)")
    A(f"확보               {inv['daysCollectedTotal']:>6,}")
    A(f"coverage           {inv['coveragePct']:>6}%")
    A(f"실패               {inv['daysFailed']:>6,}")
    A(f"중단 사유          {inv.get('aborted')}")
    A("```")
    A("")
    A(f"{pc['liquidityVariables']['noLookAhead']}")
    A("")

    A("## 4. ★ 자체수정 2건 (§41)")
    A("")
    A("**SC1 — 재시도·세션복구 없음.** 초판 수집기는 단발 호출이었다. KRX 세션이")
    A("한 번 끊기자 이후 **모든** 요청이 KeyError 로 실패했는데(실측: 성공 52건에서")
    A("고정된 채 실패만 548건 누적) 재시도도 세션 복구도 없어 목록만 태웠다.")
    A("이 repo 의 기존 재시도·백오프 규약을 붙이고, 연속 실패 25회면 **멈추도록**")
    A("했다. 무의미한 소진은 소스를 더 막을 뿐이다.")
    A("")
    A("**SC2 — 소스 불가 시 inventory 유실.** pykrx 는 **import 시점에** 세션을")
    A("만든다. 소스가 막히면 import 자체가 예외를 던져 수집기가 죽고, 그러면")
    A("'얼마나 못 받았는지' 기록조차 남지 않는다. import 를 감싸 소스 불가도")
    A("하나의 status 로 기록하게 했다. 이 수정 덕분에 위 2.39% 라는 숫자를")
    A("정확히 말할 수 있다.")
    A("")
    A("두 수정 모두 결과를 유리하게 만들지 않는다 — 오히려 실패를 **정확히**")
    A("드러내는 방향이다.")
    A("")

    A("## 5. coverage gate (§30)")
    A("")
    A("```")
    A(f"거래일 coverage          {cov['dayCoveragePct']}%")
    A(f"연도별 요구 coverage      {cov['minCoveragePctPerYear']}%")
    A(f"요구 충족 연도            {cov['yearsMeetingThreshold']} / {cov['minYearsCovered']}")
    A(f"gate 통과                {cov['coverageGatePass']}")
    A("```")
    A("")
    A("**연도별 coverage** (상위 몇 개만)")
    A("")
    A("| 연도 | 종목-시점 | 유동성 보유 | coverage |")
    A("|---|---|---|---|")
    ys = sorted(cov["byYear"].items())
    for y, x in ys[:6]:
        A(f"| {y} | {x['names']:,} | {x['withLiquidity']:,} | {x['coveragePct']}% |")
    if len(ys) > 6:
        A(f"| … | | | (이하 {len(ys) - 6}개 연도 전부 0% 대) |")
    A("")
    A(f"{cov['noSecretWindowShift']}")
    A("")
    A("**이것이 R27 의 결정적 게이트다.** coverage 가 좋은 최근 구간만 골라")
    A("primary 로 바꾸면 숫자는 나오지만 그건 답이 아니다. 하지 않았다.")
    A("")

    A("## 6. 적용하려 했던 기준 (§17 — 이미 고정됨)")
    A("")
    A("데이터가 없어 **적용하지 못했지만**, 기준은 결과를 보기 전에 고정해 뒀다.")
    A("다음 작업에서 그대로 쓴다.")
    A("")
    A("```")
    A(f"PRIMARY gate  {gate['name']}")
    for c in gate["conditions"]:
        A(f"  · {c}")
    A(f"함의 참여율    {gate['participationImplied'] * 100:.1f}%")
    A(f"1종목 주문액   {pc['capital']['orderPerNameKrw']:,}원 "
      f"({pc['capital']['totalKrw']:,}원 / {pc['capital']['namesForSanity']}종목)")
    A("```")
    A("")
    A(f"{gate['why']}")
    A("")
    A(f"민감도 3개도 고정돼 있다 — LOW {pc['sensitivity']['LOW']['thresholdKrw']:,}원 · "
      f"BASE {pc['sensitivity']['BASE']['thresholdKrw']:,}원 · "
      f"HIGH {pc['sensitivity']['HIGH']['thresholdKrw']:,}원.")
    A(f"{pc['sensitivity']['rule']}")
    A("")
    A(f"거래정지: **{pc['suspension']['status']}** — {pc['suspension']['why']}")
    A("")

    A("## 7. 수행하지 않은 분석 (§38 산출물 상태)")
    A("")
    A("coverage gate 미달로 아래는 **의도적으로 만들지 않았다.** 근거 없는 숫자를")
    A("산출물로 남기면 다음 작업이 그것을 사실로 인수인계한다.")
    A("")
    A("| 산출물 | 상태 |")
    A("|---|---|")
    for k, st in d["artifactStatus"].items():
        A(f"| r27-{k} | {'○ 생성' if st == 'PRESENT' else '× NOT_RUN'} |")
    A("")
    A("NOT_RUN 인 것: before/after · 참여율 분포 · 저가주 노출 · included/excluded ·")
    A("상폐/부실 · 부분기간/롤링 · 거래소 · 집중도 · BM vs SIZE 비교.")
    A("")

    A("## 8. 답하지 못한 질문 — 정직하게")
    A("")
    A("R27 의 단일 질문은 이것이었다:")
    A("")
    A("> SIZE_SMALL 초과수익은 당시 실제로 거래 가능한 종목과 현실적인 개인투자")
    A("> 규모에서도 유지되는가?")
    A("")
    A("**답하지 못했다.** SIZE 19.15% 가 실행 가능하다는 증거도, 불가능하다는")
    A("증거도 이번에 얻지 못했다. R26 의 `LIQUIDITY_DATA_INCOMPLETE` 한계는")
    A("**그대로 남아 있다.**")
    A("")
    A("다만 두 가지는 확정됐다.")
    A("")
    A("1. **데이터 경로는 존재한다.** 필요한 거래량·거래대금을 무료 공식 KRX 에서")
    A("   2007년까지 전 종목 일괄로 받을 수 있음을 실측했다. 유료 데이터도 새")
    A("   credential 도 필요 없다.")
    A("2. **기존 결과는 흔들리지 않았다.** SIZE·BM 의 R25/R26 수치가 전 horizon")
    A("   정확히 재현됐다.")
    A("")

    A("## 9. 한계")
    A("")
    A("- 유동성 coverage 2.39% — 판정 불가의 직접 원인.")
    A("- KRX 소스가 세션 로그인 단계에서 차단됐다. 일시적일 가능성이 높지만")
    A("  이번 세션 안에서는 회복되지 않았다.")
    A("- 거래정지 이력은 애초에 무료 공식 시계열 소스가 없다"
      f"(**{pc['suspension']['status']}**). 확보되더라도 zero-volume 을 하한")
    A("  proxy 로만 쓸 수 있다.")
    A("- 전체 기간 수집은 4,640 거래일이 필요하다. 완주하려면 KRX 를 자극하지")
    A("  않는 느린 pacing 으로 여러 세션에 나눠 받아야 한다.")
    A("")

    A("## 10. production 보호 (§36)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 11. HOTG (§40)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 12. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 무료 공개 KRX 조회 범위였다.**")
    A("")
    A(f"- 다음 작업 결정 규칙: **{v['nextTaskRule']}** (§34)")
    A("")
    A(f"- 다음 단일 작업: **{v['nextTask']}**")
    A("")
    A("  구체적으로: `scripts/research/r27_collect.py` 를 **느린 pacing 으로**")
    A("  이어받는다. resumable 이라 이미 받은 111일은 건너뛴다. 수집이 끝나면")
    A("  `r27_run.py` → `r27_verdict.py` 를 그대로 다시 돌리면 된다 — 기준·gate·")
    A("  민감도가 전부 precommit 에 고정돼 있어 재작성할 것이 없다.")
    A("")
    A("  **새 factor 연구는 하지 않는다(§34-D).**")
    A("")
    A("- 지금 하지 않은 것: 거래가능성 기준 완화 · 최근 구간만 primary 로 변경 ·")
    A("  포트폴리오 설계 · BM×SIZE 재조합 · EY/Quality 추가")
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
                      "reasonClass": d["reasonClass"],
                      "verdict": d["verdict"]["verdict"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
