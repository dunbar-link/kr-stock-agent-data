#!/usr/bin/env python3
"""R25 canonical report (MD + JSON).

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25
기존 R7~R24 산출물을 덮어쓰지 않는다(§28). HOTG 는 기존 Bridge 재사용(§30).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-canonical-factor-rediscovery-r25-latest"

KO = {"BM": "BM (장부가/시가 = 1/PBR)", "EY": "EY (이익수익률 = 1/PER)",
      "ROE": "ROE (EPS/BPS)",
      "EARNINGS_PERSISTENCE": "이익 지속성 (36M 흑자비율)",
      "BPS_GROWTH": "자기자본 성장 (36M BPS 증가율)",
      "SIZE_SMALL": "SIZE (작을수록 = −ln 시총)",
      "DIVIDEND_YIELD": "배당수익률", "DIVIDEND_DISCIPLINE": "배당 규율 (36M 배당비율)",
      "MAGIC_FORMULA_LEGACY": "Magic Formula (legacy 근사)",
      "BM_ROE_LEGACY": "BM + ROE (legacy 조합)"}
VK = {"STRONG_SIGNAL": "STRONG", "PROMISING_SIGNAL": "PROMISING",
      "WEAK_SIGNAL": "WEAK", "NO_SIGNAL": "NO_SIGNAL",
      "INVERTED_SIGNAL": "INVERTED", "UNRELIABLE": "UNRELIABLE"}


def L(n):
    p = RD / f"r25-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    pc, cov, sc = L("precommit"), L("factor-coverage"), L("factor-scorecard")
    v = L("verdict")
    h, q, s = L("horizon-results"), L("quantile-results"), L("subperiod-results")
    c, r, b = L("size-exchange-controls"), L("rolling-cohorts"), L("bootstrap")
    cc, dd, xx = L("concentration"), L("distress"), L("tsr-limitation-exposure")
    strong = v["byVerdict"].get("STRONG_SIGNAL", [])
    line = "PASS" if (strong or v["byVerdict"].get("PROMISING_SIGNAL")) else "WARNING"
    rc = ("R25_CANONICAL_FACTOR_REDISCOVERY_PRIMARY_FOUND" if strong
          else "R25_CANONICAL_FACTOR_REDISCOVERY_NO_PRIMARY")
    return {
        "schema": "wababa-canonical-factor-rediscovery-r25@1",
        "taskId": "WABABA-CANONICAL-FACTOR-REDISCOVERY-R25",
        "verdictLine": line, "reasonClass": rc,
        "precommit": {k: pc[k] for k in pc if k != "factorDefinitions"},
        "factorDefinitions": pc["factorDefinitions"],
        "coverage": cov, "horizonResults": h, "quantileResults": q,
        "subperiodResults": s, "controls": c, "rolling": r, "bootstrap": b,
        "concentration": cc, "distress": dd, "tsrLimitationExposure": xx,
        "scorecard": sc, "verdict": v,
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
    pc, cov, sc, v = d["precommit"], d["coverage"], d["scorecard"], d["verdict"]
    rows = {x["factor"]: x for x in sc["rows"]}
    o = []
    A = o.append

    def f(x, nd=2, suf="%"):
        return "-" if x is None else f"{x:.{nd}f}{suf}"

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R25 — canonical TSR 기반 factor 원점 재발견")
    A("")
    A(f"- **PRIMARY_FACTOR: {', '.join(v['PRIMARY_FACTOR']) if isinstance(v['PRIMARY_FACTOR'], list) else v['PRIMARY_FACTOR']}**")
    A(f"- **SECONDARY_FACTOR: {', '.join(v['SECONDARY_FACTOR']) if isinstance(v['SECONDARY_FACTOR'], list) else v['SECONDARY_FACTOR']}**")
    A(f"- **INVERTED_SIGNAL: {', '.join(v['byVerdict'].get('INVERTED_SIGNAL', [])) or '없음'}**")
    A(f"- Founder 의 Quality 가설(§23): **기각 — 반대 방향으로 유의미**")
    A(f"- 연구기간 {pc['universe']['period']['start']} ~ "
      f"{pc['universe']['period']['end']} · {cov['months']}개월")
    A(f"- Foundation: {pc['foundation']['verdict']} "
      f"(편향 {pc['foundation']['expectedBiasPct']}% ≤ 2%)")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    A("> 실제 주주가 받은 wealth 기준으로 다시 재면 **싼 주식(BM)과 작은 회사가**")
    A("> 이겼고, **잘 버는 회사(ROE·이익지속성·자기자본성장)는 반대로 졌다**.")
    A("> BM 은 크기·거래소를 통제해도 살아남았고 4개 부분기간 전부 양(+)이다.")
    A("> SIZE 는 더 크지만 상위분위의 상장폐지율이 12.7% 라 실행 가능성이 별개")
    A("> 문제로 남는다. 배당 factor 는 신호가 없었다.")
    A("")

    A("## 2. Foundation · 엔진 · 기간 (§3·§4)")
    A("")
    A("```")
    A(f"Foundation 판정        {pc['foundation']['verdict']}")
    A(f"기대편향               {pc['foundation']['expectedBiasPct']}%  (기준 2.0%)")
    A(f"미해결 종목비율          {pc['foundation']['unresolvedTickerPct']}%")
    A(f"알려진 한계             극단 불연속 {pc['foundation']['knownLimitationPct']}%")
    A(f"엔진                   {pc['engine']['name']}")
    A(f"엔진 base              {pc['engine']['base']}")
    A(f"정책                   {pc['engine']['policy']}")
    A(f"수익 기준               {pc['engine']['returnBasis']}")
    A(f"close-only 금지         {pc['engine']['closeOnlyForbidden']}")
    A(f"동일 엔진 적용           {' / '.join(pc['engine']['sameEngineFor'])}")
    A(f"연구기간                {pc['universe']['period']['start']} ~ "
      f"{pc['universe']['period']['end']}  ({cov['months']}개월)")
    A("```")
    A("")
    A(f"**PIT / survivorship**: {pc['universe']['asOfUniverse']} "
      f"{pc['universe']['delisting']} {pc['pitRule']['noRestatement']}")
    A("")
    A("엔진 동치는 추정이 아니라 실측으로 강제했다 — 무작위 384개 구간에서")
    A("`r17_wealth.RightsWealth.run` 과 최대오차 **3.6e-15**, 삼성전자 R15 anchor")
    A("**478.52%** 를 그대로 재현한다.")
    A("")

    A("## 3. factor coverage (§5)")
    A("")
    A("| factor | 가족 | 정의 | 사용가능 월 | 월중앙 종목수 |")
    A("|---|---|---|---|---|")
    for k, x in cov["byFactor"].items():
        A(f"| {KO.get(k, k)} | {x['family']} | `{x['formula']}` | "
          f"{x['monthsUsable']} / {cov['months']} | {x['medianEligible']:,} |")
    A("")

    A("## 4. ★ horizon 별 연율 spread (TOP − BOTTOM)")
    A("")
    A("| factor | 12M | 36M | 60M | 84M(보조) | 판정 |")
    A("|---|---|---|---|---|---|")
    for x in sc["rows"]:
        sp = x["spreadAnnPct"]
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(sp.get('12M'))} | "
          f"{f(sp.get('36M'))} | {f(sp.get('60M'))} | {f(sp.get('84M'))} | "
          f"**{VK[x['verdict']]}** |")
    A("")
    A(f"연율화: {pc['horizons']['annualization']}")
    A("")

    A("## 5. 분위 단조성 (§7)")
    A("")
    for k in ("BM", "SIZE_SMALL", "ROE"):
        m = d["quantileResults"]["byFactor"][k]["12M"]
        A(f"**{KO[k]}** — {m['quantiles']}분위 · 단조성 {m['monotonicity']}")
        A("")
        A("```")
        A("  " + " ".join(f"{x:6.1f}" if x is not None else "     -"
                          for x in m["quantileAnnPct"]))
        A("  " + " ".join(f"{'TOP' if i == 0 else ('BOT' if i == m['quantiles'] - 1 else str(i + 1)):>6}"
                          for i in range(m["quantiles"])))
        A("```")
        A("")
    A("전체 단조성:")
    A("")
    A("```")
    for x in sc["rows"]:
        A(f"{KO.get(x['factor'], x['factor']):34} {x['monotonicity']}")
    A("```")
    A("")

    A("## 6. 부분기간 (§12) — 달력 고정 4구간")
    A("")
    A("| factor | 2007-2011 | 2012-2016 | 2017-2021 | 2022-2026 | 양(+)/전체 |")
    A("|---|---|---|---|---|---|")
    for x in sc["rows"]:
        p = d["subperiodResults"]["byFactor"][x["factor"]]["byPeriod"]
        A(f"| {KO.get(x['factor'], x['factor'])} | "
          + " | ".join(f(p[k]["meanSpreadAnnPct"]) for k in
                       ("2007-2011", "2012-2016", "2017-2021", "2022-2026"))
          + f" | {x['subperiodPositive']}/{x['subperiodTotal']} |")
    A("")

    A("## 7. ★ SIZE / 거래소 통제 (§10·§11)")
    A("")
    A("| factor | raw | size 통제 | 거래소 통제 | size+거래소 통제 | KOSPI | KOSDAQ |")
    A("|---|---|---|---|---|---|---|")
    for x in sc["rows"]:
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(x['rawSpreadAnnPct'])} | "
          f"{f(x['sizeMatchedSpreadAnnPct'])} | {f(x['exchangeMatchedSpreadAnnPct'])} | "
          f"{f(x['sizeExchangeMatchedSpreadAnnPct'])} | {f(x['kospiSpreadAnnPct'])} | "
          f"{f(x['kosdaqSpreadAnnPct'])} |")
    A("")
    A("**크기 구간별 (12M)**")
    A("")
    A("| factor | SMALL | MID | LARGE |")
    A("|---|---|---|---|")
    for x in sc["rows"]:
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(x['smallSpreadAnnPct'])} | "
          f"{f(x['midSpreadAnnPct'])} | {f(x['largeSpreadAnnPct'])} |")
    A("")
    A("**이것이 R25 의 핵심 분리다.** BM 은 size+거래소를 동시에 통제해도")
    A(f"{f(rows['BM']['sizeExchangeMatchedSpreadAnnPct'])} 로 살아남고, SMALL·MID·LARGE")
    A("세 구간 **전부** 양(+)이다 — BM 은 small-cap premium 의 변장이 아니다.")
    A(f"반대로 SIZE 는 SMALL {f(rows['SIZE_SMALL']['smallSpreadAnnPct'])} 인데 "
      f"LARGE {f(rows['SIZE_SMALL']['largeSpreadAnnPct'])} 로, 효과가 소형주 구간")
    A("안에만 있다.")
    A("")

    A("## 8. 롤링 코호트 (§13)")
    A("")
    A("| factor | 겹침 중앙값 | 양(+)비율 | p10 | p90 | 비겹침 중앙값 | 비겹침 양(+)비율 |")
    A("|---|---|---|---|---|---|---|")
    for x in sc["rows"]:
        rr = d["rolling"]["byFactor"][x["factor"]]["12M"]
        ov, no = rr["overlapping"] or {}, rr["nonOverlapping"] or {}
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(ov.get('medianSpreadAnnPct'))} | "
          f"{ov.get('positiveRate')} | {f(ov.get('p10Pct'))} | {f(ov.get('p90Pct'))} | "
          f"{f(no.get('medianSpreadAnnPct'))} | {no.get('positiveRate')} |")
    A("")

    A("## 9. 부트스트랩 CI (§14)")
    A("")
    A("```")
    st = d["bootstrap"]["method"]
    A(f"방법        {st['method']} · 블록 {st['blockMonths']}개월 · "
      f"재표본 {st['resamples']} · seed {st['seed']}")
    A("```")
    A("")
    A("| factor | 평균 spread | CI95 하한 | CI95 상한 | P(spread>0) |")
    A("|---|---|---|---|---|")
    for x in sc["rows"]:
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(x['spreadAnnPct'].get('12M'))} | "
          f"{f(x['ci95LowPct'])} | {f(x['ci95HighPct'])} | {x['pSpreadPositive']} |")
    A("")

    A("## 10. 집중도 (§15)")
    A("")
    A("| factor | top1 | top3 | top5 | top10 | top3 제거 후 spread | 집중신호 |")
    A("|---|---|---|---|---|---|---|")
    for x in sc["rows"]:
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(x['top1Pct'])} | "
          f"{f(x['top3Pct'])} | {f(x['top5Pct'])} | {f(x['top10Pct'])} | "
          f"{f(x['spreadExcludingTop3AnnPct'])} | "
          f"{'○' if x['concentratedSignal'] else '×'} |")
    A("")
    A(f"BM 은 상위 3종목 제거 후에도 {f(rows['BM']['spreadExcludingTop3AnnPct'])} 로")
    A(f"살아남는다. SIZE 도 {f(rows['SIZE_SMALL']['spreadExcludingTop3AnnPct'])} 로 유지된다.")
    A(f"반면 EY 는 {f(rows['EY']['spreadExcludingTop3AnnPct'])} 로 부호가 뒤집힌다 —")
    A("소수 대박주 의존이 크다는 뜻이고, SECONDARY 로만 둔 이유 중 하나다.")
    A("")

    A("## 11. ★ 부실 · 상장폐지 노출 (§16)")
    A("")
    A("| factor | TOP 상폐율 | BOTTOM 상폐율 | TOP 저가주 | TOP 적자지속 | TOP 극단PBR |")
    A("|---|---|---|---|---|---|")
    for x in sc["rows"]:
        t = d["distress"]["byFactor"][x["factor"]]["TOP"]
        bo = d["distress"]["byFactor"][x["factor"]]["BOTTOM"]
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(t['delistingRatePct'])} | "
          f"{f(bo['delistingRatePct'])} | {f(t['lowPriceRatePct'])} | "
          f"{f(t['persistentLossRatePct'])} | {f(t['extremePbrRatePct'])} |")
    A("")
    A("**가장 중요한 한계다.** SIZE 상위분위는 상폐율 "
      f"{f(rows['SIZE_SMALL']['topDelistingRatePct'])} · 저가주 "
      f"{f(rows['SIZE_SMALL']['topLowPriceRatePct'])} · 적자지속 "
      f"{f(rows['SIZE_SMALL']['topPersistentLossRatePct'])} 다.")
    A("상폐는 canonical 정책상 **마지막 관측가 청산(UNKNOWN_RECOVERY)** 으로 처리되고,")
    A("이는 R16 이 명시한 대로 **상한 추정**이다. 즉 SIZE 의 수익은 상폐 회수율을")
    A("낙관적으로 가정한 값이다. BM 은 상폐율 "
      f"{f(rows['BM']['topDelistingRatePct'])} 로 훨씬 낮다.")
    A("")

    A("## 12. TSR 한계 전이 (§17)")
    A("")
    A("| factor | TOP 미해결율 | BOTTOM 미해결율 | 차이 | 노출 |")
    A("|---|---|---|---|---|")
    for x in sc["rows"]:
        e = d["tsrLimitationExposure"]["byFactor"][x["factor"]]
        A(f"| {KO.get(x['factor'], x['factor'])} | "
          f"{f(e['topUnresolvedRatePct'], 3)} | {f(e['bottomUnresolvedRatePct'], 3)} | "
          f"{f(e['diffPp'], 3, '%p')} | {'○' if e['exposed'] else '×'} |")
    A("")
    A("R24 잔여 한계(극단 불연속 1.42%)가 특정 분위에 몰려 있지 않다. 최대 차이가")
    A("0.35%p 로 기준 3%p 를 크게 밑돈다 — factor 결과가 미해결 자본행위로")
    A("설명되지 않는다.")
    A("")

    A("## 13. ★ factor scorecard (§19)")
    A("")
    A("| factor | 12M | 36M | 60M | 단조성 | 부분기간 | 롤링 | size통제 | 거래소 | CI하한 | 집중 | TSR한계 | 판정 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for x in sc["rows"]:
        sp = x["spreadAnnPct"]
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(sp.get('12M'))} | "
          f"{f(sp.get('36M'))} | {f(sp.get('60M'))} | {x['monotonicity']} | "
          f"{x['subperiodPositive']}/{x['subperiodTotal']} | "
          f"{x['rollingPositiveRate']} | {f(x['sizeMatchedSpreadAnnPct'])} | "
          f"{f(x['exchangeMatchedSpreadAnnPct'])} | {f(x['ci95LowPct'])} | "
          f"{'○' if x['concentratedSignal'] else '×'} | "
          f"{'○' if x['tsrLimitationExposed'] else '×'} | "
          f"**{VK[x['verdict']]}** |")
    A("")
    A("```")
    for k, names in sc["byVerdict"].items():
        A(f"{VK.get(k, k):12} {', '.join(names)}")
    A("```")
    A("")
    A("자격기준은 결과를 보기 전에 고정했고 바꾸지 않았다"
      f"(thresholdsUnchanged = {sc['thresholdsUnchanged']}).")
    A("")

    A("## 14. 최종 선정")
    A("")
    A("```")
    A(f"PRIMARY_FACTOR      {v['PRIMARY_FACTOR']}")
    A(f"SECONDARY_FACTOR    {v['SECONDARY_FACTOR']}")
    A(f"REJECTED_FACTOR     {v['REJECTED_FACTOR']}")
    A("```")
    A("")
    A(f"{v['noForcedWinner']}")
    A("")

    A("## 15. ★ Founder 의 Quality 가설 (§23)")
    A("")
    A(f"> {v['foundersQualityHypothesis']['statement']}")
    A("")
    A("canonical TSR 에서 12M·36M·60M 모두 확인했다. 결과:")
    A("")
    A("| quality factor | 12M | 36M | 60M | 판정 |")
    A("|---|---|---|---|---|")
    for x in v["foundersQualityHypothesis"]["result"]:
        sp = x["spread"]
        A(f"| {KO.get(x['factor'], x['factor'])} | {f(sp.get('12M'))} | "
          f"{f(sp.get('36M'))} | {f(sp.get('60M'))} | **{VK[x['verdict']]}** |")
    A("")
    A("**가설은 기각된다.** 세 정의 모두 사전에 고정한 방향과 **반대로** 유의미했다.")
    A("R15 의 close-only bug 를 걷어내고 canonical TSR 로 공정하게 다시 쟀는데도")
    A("결과가 바뀌지 않았다. 좋은 결과를 만들려고 quality 정의를 반복 변경하지")
    A("않았다 — 세 정의는 precommit 에 고정한 그대로다.")
    A("")
    A("다만 한 가지는 분명히 해둔다. 이것은 '수익성 좋은 회사가 나쁜 회사다' 라는")
    A("뜻이 아니다. **비싸게 사면 진다**는 뜻에 가깝다 — 고ROE 종목은 대체로")
    A("고PBR 이고, BM 이 이긴 것과 같은 현상의 뒷면이다.")
    A("")

    A("## 16. legacy 조합 재측정 (§20 — 참고용)")
    A("")
    A("| 조합 | 12M | 36M | 60M | 판정 |")
    A("|---|---|---|---|---|")
    for k in ("MAGIC_FORMULA_LEGACY", "BM_ROE_LEGACY"):
        x = rows[k]
        sp = x["spreadAnnPct"]
        A(f"| {KO[k]} | {f(sp.get('12M'))} | {f(sp.get('36M'))} | "
          f"{f(sp.get('60M'))} | **{VK[x['verdict']]}** |")
    A("")
    A("Magic Formula 는 신호가 없다. BM+ROE 는 raw 로는 양(+)이지만 size+거래소")
    A(f"동시 통제 시 {f(rows['BM_ROE_LEGACY']['sizeExchangeMatchedSpreadAnnPct'])} 로")
    A("부호가 뒤집히고 KOSPI 에서 "
      f"{f(rows['BM_ROE_LEGACY']['kospiSpreadAnnPct'])} 다 — BM 단독보다 나쁘다.")
    A("ROE 를 섞는 것이 BM 을 **약화**시킨다. 새 조합은 만들지 않았다(§20).")
    A("")

    A("## 17. R7~R14 대비 무엇이 바뀌었나")
    A("")
    A("| 항목 | 기존 (PRE_TSR_LEGACY) | R25 (canonical TSR) | 변화 |")
    A("|---|---|---|---|")
    A("| BM | strong | **STRONG** | 유지 — 단 숫자는 승계하지 않음 |")
    A("| SIZE | strong | **STRONG** | 유지 |")
    A("| EY | promising | **PROMISING** | 유지 |")
    A("| ROE | inverted | **INVERTED** | 유지 |")
    A("| Quality | inverted/promising 혼재 | **전부 INVERTED** | 명확해짐 |")
    A("| Magic Formula | no signal | **NO_SIGNAL** | 유지 |")
    A("| 배당 | 미검증 | **NO_SIGNAL** | 신규 |")
    A("")
    A("**왜 대부분 방향이 유지됐나** — R15 가 발견한 close-only bug 는 개별 종목의")
    A("장기 wealth 를 크게 왜곡했지만(삼성전자 −90.20% → +478.52%), 그 왜곡은")
    A("배당·분할을 많이 한 종목에 집중된다. 횡단면 **순위**는 그보다 덜 흔들렸다.")
    A("바뀐 것은 방향이 아니라 **신뢰도**다 — 이제 근거가 실제 주주 wealth 다.")
    A("")
    A("**R11 BM_P20_N40_H24 는 승계하지 않는다(§22).** R25 는 factor 존재만")
    A("확인했고 포트폴리오 파라미터는 하나도 재검증하지 않았다.")
    A("")

    A("## 18. 사람이 이해할 수 있는 해석 (§25)")
    A("")
    A("- **싼 주식이 이겼다.** 장부가 대비 싸게 거래되던 종목을 사서 1년 들고 있었을")
    A("  때, 비싸던 종목보다 연 25%p 앞섰다. 크기와 거래소를 동시에 맞춰도 연 8.6%p")
    A("  남는다. 20년 4개 구간에서 한 번도 지지 않았다.")
    A("- **작은 회사가 이겼다.** 다만 그 상위 분위의 12.7% 가 기간 중 상장폐지됐고,")
    A("  절반 이상이 적자를 지속하던 회사다. 수익의 상당 부분이 '망하지 않은 쪽'에서")
    A("  나온다는 뜻이라, 실제로 살 수 있었는지는 따로 확인해야 한다.")
    A("- **잘 버는 회사는 이기지 못했다.** 오히려 졌다. 좋은 회사와 좋은 주식은")
    A("  다르다는 고전적 결과가 한국 데이터에서도 그대로 나왔다.")
    A("- **배당은 신호가 아니었다.** 배당수익률도, 배당을 꾸준히 준 이력도 미래")
    A("  주주 wealth 를 예측하지 못했다.")
    A("")
    A("이것은 과거 데이터에서 관측된 통계적 규칙성이다. 수익을 보장하지 않으며")
    A("투자 권유가 아니다. 거래비용·세금·유동성은 반영하지 않았다(§21 로 금지).")
    A("")

    A("## 19. 한계")
    A("")
    A("- 거래비용·슬리피지·세금 0 가정. **총수익 기준 비교**이지 실현 가능 수익이")
    A("  아니다. 비용 최적화는 §21 로 이번 범위에서 금지됐다.")
    A("- 상장폐지 회수는 마지막 관측가(UNKNOWN_RECOVERY) — **상한 추정**이다.")
    A("  SIZE 처럼 상폐 노출이 큰 factor 는 실제보다 좋게 보일 수 있다.")
    A("- 코호트가 겹치므로 관측이 독립이 아니다. 블록 부트스트랩으로 완화했지만")
    A("  완전히 제거되지는 않는다.")
    A("- 분위 수익은 종목 누적수익의 산술평균(동일가중)이다. 우편향이 크며")
    A("  중앙값도 함께 보고했다(BM 25.0 vs 중앙값 24.4 — 거의 같다).")
    A("- R24 잔여 한계(극단 불연속 1.42%)는 분위 간 편중이 없음을 확인했으나")
    A("  0 은 아니다.")
    A("- 84M 은 보조 지표다. 코호트 수가 148~152 로 줄어든다.")
    A("")

    A("## 20. production 보호 (§26)")
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
    A("- **Founder 행동: NONE — 승인 게이트 없음. 로컬 계산 전용.**")
    A("")
    A(f"- 다음 작업 결정 규칙: **{v['nextTaskRule']}** (§34)")
    A("")
    A(f"- 다음 단일 작업: **{v['nextTask']}**")
    A("")
    A("  구체적으로: BM 과 SIZE 는 둘 다 STRONG 이지만 서로 독립인지 아직 모른다.")
    A("  BM 은 size 통제 후에도 살아남았으므로 독립일 가능성이 크지만, 그것을")
    A("  **측정**한 것은 아니다. 다음 연구 하나는 'BM 과 SIZE 를 결합하면")
    A("  incremental alpha 가 있는가' 이고, 그 안에서 SIZE 의 상폐·저가주·유동성")
    A("  노출을 실제 거래가능성 관점으로 함께 검증한다(§34 C 의 우려를 흡수).")
    A("")
    A("- 지금 하지 않은 것: 포트폴리오 parameter search(N/P/보유기간/리밸런싱/비용) ·")
    A("  새 조합 탐색 · R11 숫자 승계 · 실계좌 연결")
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
                      "PRIMARY": d["verdict"]["PRIMARY_FACTOR"],
                      "SECONDARY": d["verdict"]["SECONDARY_FACTOR"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
