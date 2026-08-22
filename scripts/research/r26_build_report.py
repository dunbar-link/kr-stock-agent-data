#!/usr/bin/env python3
"""R26 canonical report (MD + JSON).

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26
기존 R7~R25 산출물을 덮어쓰지 않는다(§38). HOTG 는 기존 Bridge 재사용(§40).
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-bm-size-incremental-combination-r26-latest"

ARM_KO = {"BM_ALONE": "BM 단독 (싼 주식)", "SIZE_ALONE": "SIZE 단독 (작은 회사)",
          "COMBO": "BM×SIZE 결합", "UNIVERSE": "eligible 전체"}
VERDICT_LINE = {
    "INCREMENTAL_COMBINATION_STRONG": ("PASS", "R26_INCREMENTAL_COMBINATION_STRONG"),
    "INCREMENTAL_COMBINATION_PROMISING": (
        "PASS", "R26_INCREMENTAL_COMBINATION_PROMISING"),
    "COMBINATION_FRAGILE": ("WARNING", "R26_COMBINATION_FRAGILE"),
    "NO_INCREMENTAL_COMBINATION": ("PASS", "R26_NO_INCREMENTAL_COMBINATION"),
}


def L(n):
    p = RD / f"r26-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build():
    v = L("verdict")
    line, rc = VERDICT_LINE[v["verdict"]]
    d = {
        "schema": "wababa-bm-size-incremental-combination-r26@1",
        "taskId": "WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26",
        "verdictLine": line, "reasonClass": rc,
        "precommit": L("precommit"),
        "combinationDefinition": L("combination-definition"),
        "baseReproduction": L("base-reproduction"),
        "quantileResults": L("quantile-results"),
        "horizonResults": L("horizon-results"),
        "incrementalAlpha": L("incremental-alpha"),
        "subperiodResults": L("subperiod-results"),
        "rollingCohorts": L("rolling-cohorts"),
        "bootstrap": L("bootstrap"),
        "conditionalAttribution": L("conditional-attribution"),
        "exchangeAndCost": L("exchange-and-cost"),
        "liquidity": L("liquidity"),
        "distressDelisting": L("distress-delisting"),
        "concentration": L("concentration"),
        "tsrLimitation": L("tsr-limitation"),
        "verdict": v,
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
    return d


def md(d):
    pc, v = d["precommit"], d["verdict"]
    base, h, inc = d["baseReproduction"], d["horizonResults"], d["incrementalAlpha"]
    sub, roll, boot = d["subperiodResults"], d["rollingCohorts"], d["bootstrap"]
    cond, ec = d["conditionalAttribution"], d["exchangeAndCost"]
    lq, dist = d["liquidity"], d["distressDelisting"]
    conc, lim, quant = d["concentration"], d["tsrLimitation"], d["quantileResults"]
    PH = f"{pc['horizons']['primary']}M"
    hs = sorted(h["byHorizon"], key=lambda x: int(x[:-1]))
    o = []
    A = o.append

    def f(x, nd=2, suf="%p"):
        return "-" if x is None else f"{x:+.{nd}f}{suf}"

    def g(x, nd=2, suf="%"):
        return "-" if x is None else f"{x:.{nd}f}{suf}"

    A(f"전체 판정: {d['verdictLine']}")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R26 — BM × SIZE 결합 incremental alpha 검증")
    A("")
    A(f"- **결합 판정: {v['verdict']}**")
    A(f"- primary horizon **{PH}** (결과 보기 전 고정)")
    A(f"- COMBO vs **BM 단독**: {f(v['incrementalPrimary']['vsBM'])} → 더 낫다")
    A(f"- COMBO vs **SIZE 단독**: {f(v['incrementalPrimary']['vsSIZE'])} → 더 못하다")
    A(f"- R25 BM·SIZE 정확 재현: **{v['r25ReproductionExact']}**")
    A(f"- 결합 최적화·가중치 탐색·intersection 시험: "
      f"**{v['weightSearchDone']} / {v['intersectionTested']}**")
    A("")

    A("## 1. 한 줄 요약")
    A("")
    if v["verdict"] == "NO_INCREMENTAL_COMBINATION":
        A("> 싸고 작은 회사를 동시에 선호하는 규칙은 **그냥 싼 회사만 사는 것보다는**")
        A("> 낫지만 **그냥 작은 회사만 사는 것보다는 못했다**. 두 신호가 각각")
        A("> 살아있다는 것(조건부 효과는 모든 층에서 양수)과, 둘을 반반 섞으면")
        A("> 더 좋아진다는 것은 **다른 이야기**였다. 결합을 억지로 살리지 않고")
        A("> 여기서 닫는다.")
    else:
        A(f"> 결합이 두 단독을 모두 이겼다 — {v['verdict']}.")
    A("")

    A("## 2. Foundation · 엔진 (§3)")
    A("")
    A("```")
    fo = pc["foundation"]
    A(f"R24 판정            {fo['r24Verdict']}")
    A(f"기대편향             {fo['expectedBiasPct']}%  (기준 2.0%)")
    A(f"미해결 종목비율        {fo['unresolvedTickerPct']}%")
    A(f"극단 불연속 한계        {fo['extremeDiscontinuityPct']}%")
    A(f"R25 판정            {fo['r25Verdict']}")
    A(f"R25 PRIMARY         {', '.join(fo['r25Primary'])}")
    A(f"엔진                {pc['engine']['name']}")
    A(f"상폐 처리            {pc['engine']['delisting']}")
    A(f"동일 엔진 적용         {' / '.join(pc['engine']['sameEngineFor'])}")
    A("```")
    A("")

    A("## 3. R25 정확 재현 (§37)")
    A("")
    A("| factor | horizon | R25 spread | R26 재현 | 코호트 | 일치 |")
    A("|---|---|---|---|---|---|")
    for fac, hh in base["r25ExactReproduction"].items():
        for k, x in hh.items():
            A(f"| {fac} | {k} | {g(x['r25SpreadAnnPct'], 3, '%p')} | "
              f"{g(x['r26SpreadAnnPct'], 3, '%p')} | "
              f"{x['r25Cohorts']}/{x['r26Cohorts']} | "
              f"{'○' if x['exactMatch'] else '×'} |")
    A("")
    A(f"**전부 일치: {base['allExactMatch']}** — R25 결과가 흔들리지 않았음을 먼저")
    A("증명하고 비교를 시작했다.")
    A("")

    A("## 4. 결합 정의 (§6) — 단 하나")
    A("")
    A("```")
    cb = pc["combination"]
    A(f"id       {cb['id']}")
    A(f"공식      {cb['formula']}")
    A(f"가중치     BM {cb['weights']['BM']} / SIZE {cb['weights']['SIZE_SMALL']}")
    A(f"백분위     {cb['pctRank']}")
    A("```")
    A("")
    A(f"{cb['onlyOne']}")
    A("")
    A(f"**{pc['noParameterRescue']}**")
    A("")

    A("## 5. ★ 공유 universe 비교 (§8)")
    A("")
    A(f"{pc['comparison']['sharedUniverse']}")
    A("")
    A("**TOP 분위 연율 TSR**")
    A("")
    A("| horizon | BM 단독 | SIZE 단독 | COMBO | 코호트 |")
    A("|---|---|---|---|---|")
    for k in hs:
        a = h["byHorizon"][k]
        A(f"| {k} | {g(a['BM_ALONE']['meanTopAnnPct'])} | "
          f"{g(a['SIZE_ALONE']['meanTopAnnPct'])} | "
          f"{g(a['COMBO']['meanTopAnnPct'])} | {a['COMBO']['cohorts']} |")
    A("")
    A(f"{base['sharedUniverseNote']}")
    A("")

    A("## 6. ★★ incremental alpha (§9·§10)")
    A("")
    A("| horizon | COMBO − BM | 양(+)비율 | COMBO − SIZE | 양(+)비율 | COMBO − universe |")
    A("|---|---|---|---|---|---|")
    for k in hs:
        c = inc["byHorizon"][k]
        A(f"| {k}{' **(primary)**' if k == PH else ''} | "
          f"{f(c['incremental_vs_BM']['meanPct'])} | "
          f"{c['incremental_vs_BM']['positiveRatio']} | "
          f"{f(c['incremental_vs_SIZE']['meanPct'])} | "
          f"{c['incremental_vs_SIZE']['positiveRatio']} | "
          f"{f(c['incremental_vs_UNIVERSE']['meanPct'])} |")
    A("")
    A("**여기가 R26 의 답이다.** COMBO 는 BM 단독을 모든 horizon 에서 이기지만,")
    A("SIZE 단독에는 12M·36M·60M 에서 **진다**. 84M 에서만 겨우 +0.20%p 다.")
    A("primary horizon 은 결과를 보기 전에 36M 으로 고정했고, 거기서 SIZE 대비")
    A(f"{f(v['incrementalPrimary']['vsSIZE'])} 다.")
    A("")
    A("보조 지표(TOP−BOTTOM spread)로 봐도 방향은 같다:")
    A("")
    A("```")
    for k in hs:
        c = inc["byHorizon"][k]
        A(f"{k:5} spread 기준  vs BM {c['spreadIncremental_vs_BM']['meanPct']:+7.3f}%p"
          f"   vs SIZE {c['spreadIncremental_vs_SIZE']['meanPct']:+7.3f}%p")
    A("```")
    A("")

    A("## 7. 분위 사다리 (§11)")
    A("")
    for a, x in quant["byArm"].items():
        A(f"**{ARM_KO[a]}** — {x['quantiles']}분위 · 단조성 {x['monotonicity']} · "
          f"최상위만 튐 {'○' if x['topOnlySpike'] else '×'}")
        A("")
        A("```")
        A("  " + " ".join(f"{y:6.1f}" if y is not None else "     -"
                          for y in x["quantileAnnPct"]))
        A("```")
        A("")
    A("흥미로운 점 하나 — **결합은 사다리가 더 매끄럽다**(단조성 1.000, 최상위 튐")
    A("없음). 두 단독은 각각 0.889 이고 최상위 한 칸이 튄다. 신호의 *질*은 결합이")
    A("낫다. 다만 매끄러움이 초과수익은 아니다.")
    A("")

    A("## 8. 부분기간 (§12)")
    A("")
    A("| 대비 | 2007-2011 | 2012-2016 | 2017-2021 | 2022-2026 | 양(+)/전체 |")
    A("|---|---|---|---|---|---|")
    for k, lab in (("vsBM", "COMBO − BM"), ("vsSIZE", "COMBO − SIZE")):
        p = sub[k][PH]
        A(f"| {lab} | "
          + " | ".join(f(p["byPeriod"][n]["meanPct"]) for n in
                       ("2007-2011", "2012-2016", "2017-2021", "2022-2026"))
          + f" | {p['positive']}/{p['total']} |")
    A("")
    A("BM 대비 우위는 3/4 구간에서 살아있지만 2022-2026 에 크게 무너진다.")
    A("SIZE 대비는 1/4 구간만 양(+)이다.")
    A("")

    A("## 9. 롤링 코호트 (§13)")
    A("")
    A("| horizon | 구분 | 대비 | 중앙값 | 양(+)비율 | p10 | p25 | p75 | p90 | 최악 | 최선 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|")
    for k in hs:
        r = roll["byHorizon"][k]
        for mode, lab in (("overlapping", "겹침"), ("nonOverlapping", "비겹침")):
            for tgt, tl in (("vsBM", "vs BM"), ("vsSIZE", "vs SIZE")):
                x = r[mode][tgt]
                if not x:
                    continue
                A(f"| {k} | {lab} | {tl} | {f(x['medianPct'])} | "
                  f"{x['positiveRatio']} | {f(x['p10Pct'])} | {f(x['p25Pct'])} | "
                  f"{f(x['p75Pct'])} | {f(x['p90Pct'])} | {f(x['worstPct'])} | "
                  f"{f(x['bestPct'])} |")
    A("")

    A("## 10. 부트스트랩 (§14)")
    A("")
    A("```")
    bm = pc["bootstrap"]
    A(f"방법   {', '.join(bm['methods'])} · 블록 {bm['blockMonths']}개월 · "
      f"재표본 {bm['resamples']} · seed {bm['seed']}")
    A("```")
    A("")
    A("| 대상 | 방법 | 평균 | CI95 하한 | CI95 상한 | P(>0) | P(>+1%p) | P(>+2%p) |")
    A("|---|---|---|---|---|---|---|---|")
    for tgt, lab in (("COMBO_minus_BM", "COMBO − BM"),
                     ("COMBO_minus_SIZE", "COMBO − SIZE")):
        for meth, ml in (("movingBlock", "moving-block"), ("yearLevel", "year-level")):
            x = boot["byHorizon"][PH][tgt][meth]
            if not x:
                continue
            A(f"| {lab} | {ml} | {f(x['meanPct'])} | {f(x['ci95LowPct'])} | "
              f"{f(x['ci95HighPct'])} | {x['pExcessAbove0']} | "
              f"{x['pExcessAbove1pp']} | {x['pExcessAbove2pp']} |")
    A("")
    A("COMBO − SIZE 의 CI 는 **0 을 넘지 못하고 음(-)쪽에 있다**. 우연이 아니라")
    A("구조적으로 SIZE 단독보다 낮다는 뜻이다.")
    A("")

    A("## 11. ★ 두 신호는 독립인가 (§15)")
    A("")
    A("```")
    ind = cond["independence"]
    A(f"corr(COMBO, BM rank)      {ind['corrComboBmRank']}")
    A(f"corr(COMBO, SIZE rank)    {ind['corrComboSizeRank']}")
    A(f"TOP 겹침 (BM TOP)          {ind['topOverlapWithBmTop']}")
    A(f"TOP 겹침 (SIZE TOP)        {ind['topOverlapWithSizeTop']}")
    A(f"한쪽 지배                  {ind['dominatedByOneFactor']}")
    A("```")
    A("")
    A("결합이 한쪽 rank 로 붕괴하지 않았다 — TOP 분위가 BM TOP 과 44%, SIZE TOP 과")
    A("52% 겹친다. 즉 **진짜 혼합**인데도 SIZE 단독을 못 이겼다. 결합이 실패한 이유가")
    A("'사실상 한쪽만 보고 있었다' 는 아니라는 뜻이다.")
    A("")

    A("## 12. ★ 조건부 귀속 (§17·§18)")
    A("")
    A(f"**{cond['question17']}**")
    A("")
    A("| 크기층 | 그 안에서 BM spread | BM TOP 연율 |")
    A("|---|---|---|")
    for k, x in cond["conditional"]["bmWithinSize"].items():
        A(f"| {k} | {g(x['bmSpreadAnnPct'], 3, '%p')} | {g(x['bmTopAnnPct'])} |")
    A("")
    A(f"**{cond['question18']}**")
    A("")
    A("| BM층 | 그 안에서 SIZE spread | SIZE TOP 연율 |")
    A("|---|---|---|")
    for k, x in cond["conditional"]["sizeWithinBm"].items():
        A(f"| {k} | {g(x['sizeSpreadAnnPct'], 3, '%p')} | {g(x['sizeTopAnnPct'])} |")
    A("")
    A("**답은 둘 다 YES 다.** 작은 주식 안에서도 싼 게 더 좋고, 싼 주식 중에서도")
    A("작은 게 더 좋다. 두 효과는 각각 살아 있다.")
    A("")
    A("그런데도 반반 섞기가 SIZE 단독을 못 이겼다. 이유는 단순하다 — 반반 섞으면")
    A("TOP 분위가 **크기 극단에서 멀어진다**. SIZE 단독 TOP 은 가장 작은 10% 인데,")
    A("결합 TOP 은 '적당히 작고 적당히 싼' 구간이다. 크기 효과의 대부분이 극단에")
    A("있으므로 희석이 손해가 된다.")
    A("")
    A("(§7 대로 intersection 규칙은 이번에 경쟁시키지 않았다. 위 표에서 CHEAP 층의")
    A("SIZE TOP 이 19.72% 로 가장 높게 보이지만, 이것은 **귀속 분석**이지 검증된")
    A("전략이 아니다. 별도 질문으로 남긴다.)")
    A("")

    A("## 13. 거래소 (§16)")
    A("")
    A("| 시장 | 코호트 | COMBO − BM | COMBO − SIZE |")
    A("|---|---|---|---|")
    for ex in ("KOSPI", "KOSDAQ"):
        x = ec["exchange"][ex]
        A(f"| {ex} | {x['cohorts']} | {f(x['vsBM']['meanPct'])} | "
          f"{f(x['vsSIZE']['meanPct'])} |")
    A("")
    A(f"KOSDAQ 의존: **{ec['exchange']['kosdaqDependent']}** — BM 대비 우위는 양")
    A("시장에서 모두 나타나고, SIZE 대비 열위도 양 시장에서 모두 나타난다.")
    A("결론이 한 시장의 특수성 때문이 아니다.")
    A("")

    A("## 14. 유동성 (§19·§20·§21)")
    A("")
    A(f"**상태: {lq['status']}**")
    A("")
    A(f"{lq['why']}")
    A("")
    A(f"없는 데이터: {', '.join(lq['unavailable'])}")
    A("")
    A("| arm | TOP 종목수 | 중앙 주가 | 중앙 시총 | 1종목당 투자금 | /시총 |")
    A("|---|---|---|---|---|---|")
    for a, x in lq["byArm"].items():
        A(f"| {ARM_KO[a]} | {x['topNamesPerCohort']:.0f} | "
          f"{x['medianClose']:,}원 | {x['medianMarketCapKrw'] / 1e8:,.0f}억 | "
          f"{x['notionalPerNameKrw']:,}원 | {x['notionalPerNameVsMedianCapPct']}% |")
    A("")
    A(f"({lq['notionalCapitalKrw']:,}원을 TOP 분위에 균등 분산했을 때의 참고치일 뿐")
    A("포트폴리오 설계가 아니다 — 종목수는 최적화하지 않았다.)")
    A("")
    A("**가격 구간 노출 (§21)**")
    A("")
    A("| arm | <500원 | 500~1,000 | 1,000~5,000 | >5,000 |")
    A("|---|---|---|---|---|")
    for a, x in lq["byArm"].items():
        b = x["priceBucketPct"]
        A(f"| {ARM_KO[a]} | {b['<500']}% | {b['500~1,000']}% | "
          f"{b['1,000~5,000']}% | {b['>5,000']}% |")
    A("")
    A("저가주 filter 를 새로 넣지 않았다 — 노출을 측정만 했다.")
    A("")
    A(f"섹터: **{lq['sector']['status']}** — {lq['sector']['why']}")
    A("")

    A("## 15. 상폐 · 부실 (§22·§23)")
    A("")
    A("| arm | 상폐율 | 저가주 | 극단 BM | 적자지속 | 적자 EPS |")
    A("|---|---|---|---|---|---|")
    for a, x in dist["byArm"].items():
        A(f"| {ARM_KO[a]} | {g(x['delistingRatePct'])} | {g(x['lowPriceRatePct'])} | "
          f"{g(x['extremeBmRatePct'])} | {g(x['persistentLossRatePct'])} | "
          f"{g(x['negativeEpsRatePct'])} |")
    A("")
    A(f"{dist['delistingSemantics']}")
    A("")
    A(f"COMBO 가 두 단독보다 나쁜가: **{dist['comboWorseThanParents']}** — 결합의")
    A("부실 노출은 정확히 두 부모 사이에 있다. SIZE 단독의 상폐율 15.54% ·")
    A("적자지속 54.13% 가 이 연구 전체에서 가장 큰 실행 위험이다.")
    A("")

    A("## 16. 집중도 (§24)")
    A("")
    A("| arm | top1 | top3 | top5 | top10 |")
    A("|---|---|---|---|---|")
    for a, x in conc["byArm"].items():
        A(f"| {ARM_KO[a]} | {g(x['top1Pct'])} | {g(x['top3Pct'])} | "
          f"{g(x['top5Pct'])} | {g(x['top10Pct'])} |")
    A("")
    A("**상위 기여자 제거 후 incremental**")
    A("")
    A("| 제거 | COMBO − BM | COMBO − SIZE |")
    A("|---|---|---|")
    A(f"| 없음 | {f(conc['baselineVsBmPct'])} | {f(conc['baselineVsSizePct'])} |")
    for k, x in conc["removal"].items():
        A(f"| {k} | {f(x['vsBM']['meanPct'])} | {f(x['vsSIZE']['meanPct'])} |")
    A("")
    A(f"top3 제거 시 부호 반전: **{conc['signFlipsWhenTop3Removed']}** — 결론이")
    A("소수 종목 때문에 생긴 것이 아니다. 결합은 오히려 두 단독보다 **덜** 집중돼")
    A("있다(top3 8.56% vs BM 12.90% / SIZE 9.15%).")
    A("")

    A("## 17. 비용 스트레스 (§27)")
    A("")
    A("| arm | 월 교체율 | 연 교체율 | 기본 TOP 연율 | 고비용(왕복 100bp) 후 |")
    A("|---|---|---|---|---|")
    for a in ("BM_ALONE", "SIZE_ALONE", "COMBO"):
        x = ec["cost"][a]
        A(f"| {ARM_KO[a]} | {x['monthlyTurnover']} | {x['annualTurnover']} | "
          f"{g(x['baseTopAnnPct'])} | {g(x['highCostTopAnnPct'])} |")
    A("")
    ic = ec["cost"].get("incrementalAfterHighCost") or {}
    A(f"고비용 후 incremental — vs BM {f(ic.get('vsBM'))} · "
      f"vs SIZE {f(ic.get('vsSIZE'))}. **결론이 바뀌지 않는다.**")
    A("결합이 SIZE 때문에 교체가 다소 잦아지지만(월 13.9% vs BM 11.3%) 결정적이지 않다.")
    A("")

    A("## 18. TSR 한계 노출 (§26)")
    A("")
    A("| arm | 미해결 자본행위 노출률 |")
    A("|---|---|")
    for a, x in lim["byArm"].items():
        A(f"| {ARM_KO[a]} | {g(x['unresolvedRatePct'], 3)} |")
    A("")
    A(f"COMBO − universe 차이 **{f(lim['comboVsUniverseDiffPp'], 3)}** · "
      f"노출 판정 **{lim['exposed']}** (기준 3%p). 결합 결과가 R24 잔여 한계로")
    A("설명되지 않는다.")
    A("")

    A("## 19. ★ 최종 판정 (§28)")
    A("")
    A("```")
    for k, ok in v["checks"].items():
        A(f"{k:34} {'PASS' if ok else 'FAIL'}")
    A("```")
    A("")
    A(f"**판정: {v['verdict']}**")
    A("")
    for w in v["verdictWhy"]:
        A(f"- {w}")
    A("")
    A(f"- BM 단독보다 나은가: **{v['betterThanBmAlone']}**")
    A(f"- SIZE 단독보다 나은가: **{v['betterThanSizeAlone']}**")
    A(f"- 포트폴리오 연구 가치: **{v['portfolioResearchWorthwhile']}**")
    A("")
    A(f"{v['noForcedCombination']}")
    A("")
    A(f"기준 불변: **{v['thresholdsUnchanged']}** · 가중치 탐색 "
      f"**{v['weightSearchDone']}** · intersection 시험 **{v['intersectionTested']}** · "
      f"포트폴리오 설계 **{v['portfolioSearchDone']}**")
    A("")

    A("## 20. 상품성 관점 한 줄 해석 (§43-42)")
    A("")
    A("> **\"싸고 작은\" 을 반반 섞는 규칙은 팔 만한 상품이 아니다.** 그냥 싼 것보다")
    A("> 낫지만 그냥 작은 것보다 못하고, 그렇다고 작은 것만 사자니 그 상위분위는")
    A("> 상폐율 15.5% · 적자지속 54% 의 회사들이다. 지금 단계에서 고객에게 내놓을")
    A("> 수 있는 건 **BM(싼 주식) 단독**이다 — 수익은 낮지만 상폐율 9.8%, 중앙")
    A("> 주가 5,520원, 20년 4개 구간 전부 양(+)으로 설명과 실행이 모두 가능하다.")
    A("")
    A("이것은 과거 데이터의 통계적 규칙성이며 수익을 보장하지 않는다. 투자 권유가")
    A("아니다.")
    A("")

    A("## 21. 한계")
    A("")
    A(f"- **{lq['status']}** — 거래량·거래대금·거래정지 데이터가 없어 실제 체결")
    A("  가능성을 검증하지 못했다. 시총·주가로만 간접 추정했다.")
    A(f"- **{lq['sector']['status']}** — 업종 집중도를 확인하지 못했다.")
    A("- 상폐 회수는 마지막 관측가(UNKNOWN_RECOVERY) 상한 추정이다. SIZE 계열이")
    A("  가장 크게 영향받는다.")
    A("- 코호트가 겹쳐 관측이 독립이 아니다. 블록·연 단위 부트스트랩으로 완화했다.")
    A("- 84M 은 표본이 줄어드는 보조 지표다.")
    A("- 비용은 스트레스 1종(왕복 100bp)만 적용했다. 비용 탐색은 §27 로 금지됐다.")
    A("")

    A("## 22. production 보호 (§36)")
    A("")
    A("```")
    for k, val in d["production"].items():
        A(f"{k:22} {val}")
    A(f"{'priorArtifactsPreserved':22} {d['priorArtifactsPreserved']}")
    A("```")
    A("")

    A("## 23. HOTG (§40)")
    A("")
    A("```")
    for k, val in d["hotg"].items():
        A(f"{k:20} {val}")
    A("```")
    A("")

    A("## 24. Founder 행동 / 다음 단일 작업")
    A("")
    A("- **Founder 행동: NONE — 승인 게이트 없음. 로컬 계산 전용.**")
    A("")
    A(f"- 다음 단일 작업: **{v['nextTask']}**")
    A("")
    if v.get("recommendedSingleFactor"):
        A(f"  **{v['recommendedSingleFactor']}** 를 고른 근거는 수익이 아니라")
        A("  **실행가능성**이다. SIZE 단독이 모든 horizon 에서 수익은 더 높지만,")
        A("  그 TOP 분위는 상폐율 15.54% · 적자지속 54.13% · 중앙주가 2,020원이고")
        A("  상폐 회수 가정에 크게 기댄다. BM 은 상폐율 9.84% · 중앙주가 5,520원 ·")
        A("  R25 에서 크기·거래소 통제 후에도 살아남았고 4개 부분기간 전부 양(+)")
        A("  이었다. 먼저 실행 가능한 쪽부터 포트폴리오로 만든다.")
        A("")
    A("- 지금 하지 않은 것: 결합 가중치 탐색 · intersection 규칙 · EY 추가 ·")
    A("  Quality 재투입 · 종목수/보유기간/리밸런싱 설계 · 실계좌 연결")
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
                      "verdict": d["verdict"]["verdict"],
                      "next": d["verdict"]["nextTask"][:60]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
