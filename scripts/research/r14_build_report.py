#!/usr/bin/env python3
"""R14 산출물 생성 (MD + JSON) — §27.

WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14

reports/wababa/wababa-quality-compounder-long-horizon-r14-latest.{md,json}
기존 R7~R13 산출물 수정 금지.

production 변경 0 · 홈페이지 0 · 실주문 0 · 브로커 0 · REAL_MONEY_NOT_APPROVED.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r14_precommit import ALL_HORIZONS, PRIMARY_HORIZON  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-quality-compounder-long-horizon-r14-latest"
Q1, Q2, Q3 = "Q1_PROFIT_PERSISTENCE", "Q2_CAPITAL_COMPOUNDING", "Q3_DIVIDEND_DISCIPLINE"
QIDS = [Q1, Q2, Q3]
SHORT = {Q1: "Q1 흑자지속", Q2: "Q2 자본복리", Q3: "Q3 배당성향",
         "ROE": "ROE(대조)", "SIZE": "SIZE(귀속)"}
H = str(PRIMARY_HORIZON)
HL = [str(h) for h in ALL_HORIZONS]


def L(n):
    p = RD / f"r14-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def sg(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def build():
    pc, rep = L("precommit"), L("repro")
    st, bm = L("standalone"), L("bmtop20")
    ov, bo, mt, au, vd = L("overlap"), L("boot"), L("matched"), L("audit"), L("verdict")
    return {
        "schema": "wababa-quality-compounder-long-horizon-r14@1",
        "taskId": "WABABA-QUALITY-COMPOUNDER-LONG-HORIZON-R14",
        "verdictLine": "PASS",
        "reasonClass": "R14_INVERTED_LONG_HORIZON_LONG_HOLD_HYPOTHESIS_REJECTED",
        "stage": "LONG_HORIZON_SIGNAL_VALIDATION (포트폴리오 최적화 아님 — §24)",
        "researchQuestion": pc["researchQuestion"],
        "dataPeriod": rep["period"],
        "precommit": {
            "created": True, "writtenBeforeResults": pc["writtenBeforeResults"],
            "file": "reports/research/r14-precommit-latest.json",
            "horizonRule": pc["horizonRule"], "factors": pc["factors"],
            "common": pc["common"], "sampleReality": pc["sampleReality"],
            "factorVerdicts": pc["factorVerdicts"],
            "noParameterRescue": pc["noParameterRescue"]},
        "reproduction": rep,
        "standalone": st,
        "bmTop20Internal": bm,
        "overlapAnalysis": ov,
        "bootstrapInference": bo,
        "matchedControls": mt,
        "audit": au,
        "finalVerdict": vd,
        "limitations": [
            "5Y 비중첩 관측 3개 · 7Y 2개. 통계적 유의성을 주장할 수 없는 표본이다. "
            "판정은 방향 일관성(horizon × 시작구간 × 거래소 × size × matched control)에 "
            "의존한다. bootstrap CI 는 연초 코호트 12개 재표집이라 실제보다 좁다.",
            "Q1 은 'EPS 가 양수냐'만 보는 proxy 다. 이익의 크기·질을 반영하지 못한다. "
            "R14 는 R13 정의를 그대로 재사용했고 변경하지 않았다(§12).",
            "Q2 는 유상증자로 인한 BPS 증가와 이익 유보를 구분하지 못한다. 실측 "
            "14.31% 가 주식수 1.5배 이상 증가(희석 proxy) · 15.70% 가 직전 EPS 음수"
            "(기저효과)다. filter 를 추가하지 않았다(§13).",
            "ROIC·영업이익률·영업현금흐름 등 실물 profitability 는 장기 PIT 데이터가 "
            "없어 R14 범위 밖이다(R13 DATA_INSUFFICIENT 그대로). 이 결과를 그 지표들에 "
            "대한 결론으로 확대해석하면 안 된다.",
            "SMALL 버킷 × BM 상위20% × Q3(EPS>0 요구)는 10분위 구성에 필요한 종목수를 "
            "채우지 못해 일부 셀이 미측정이다.",
            "업종 코드가 PIT 스냅샷에 없어 sector exposure 는 측정하지 못했다.",
            "거래비용 미부과(factor spread 검증이지 포트폴리오가 아니다). 다만 장기 "
            "보유는 회전율이 낮아 12M 보다 비용 민감도가 낮다.",
            "월 스냅샷 · 배당 재투자 미반영 · 상장폐지는 마지막 관측가 청산(R7 동일).",
            "백테스트 결과이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "krStockAgentRepo": "untouched", "productionUrl": "untouched",
            "legacy50d": "unchanged",
            "newBmR8FrozenForwardState": "존재하지 않음(R11 설계만, 미생성) — 미접근",
            "canonical": "untouched", "autoApply": "untouched",
            "autoPublish": "untouched", "homepage": "unchanged",
            "scheduler": "untouched", "publicDisclosure": 0,
            "broker": 0, "realOrders": 0, "paidData": 0, "externalSend": 0,
            "deploy": 0, "envOrToken": 0, "productionDbWrite": 0},
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
        "hotgClosedLoop": {
            "canonicalReport": f"reports/wababa/{NAME}.md",
            "reportBridgeCollectable": True,
            "evidence": ("projects.registry.json 의 wababa 항목 reportBridge=true · "
                         "report_policy=INCLUDE · child_roots 에 "
                         "C:\\work\\kr-stock-agent-data-new 포함."),
            "sourceOwner": "Wababa",
            "surface": "08:40 Founder 종합보고 (기존 구조 재사용)",
            "newObserverCreated": 0, "duplicateOrchestration": 0,
            "closeCondition": "R14 판정이 08:40 종합보고에 올라가면 CLOSE(승인 불필요).",
            "executionSucceededInvisibly": False},
        "nextDecision": {
            "founderAction": "NONE — 승인 게이트 없음. 판정은 데이터가 내렸다.",
            "single": ("R15(BM+quality 장기보유 포트폴리오)로 넘어가지 않는다. "
                       "R13·R14 를 통틀어 BM 상위 20% 내부에서 유일하게 살아남은 "
                       "신호는 SIZE 다(5Y +3.17%p · 시작구간 3/3 · 코호트 75.7% 양수). "
                       "다음 단일 연구질문: 'R11 후보(BM_P20_N40_H24)의 초과수익이 "
                       "이미 소형주 편중으로 설명되는가, 아니면 BM 이 SIZE 와 독립적인 "
                       "축인가?' — 새 전략을 만드는 것이 아니라 기존 후보의 귀속을 "
                       "확정하는 작업이다."),
            "notNow": ["R15 결합 포트폴리오", "장기보유로 수익성 조건 재시도",
                       "horizon 추가 탐색", "SIZE 를 곧바로 전략화",
                       "홈페이지 공개", "실계좌 자금"],
            "forbiddenFollowUp": ("§35 — R14 가 NO 이므로 '보유기간이 짧아서 실패했다'는 "
                                  "가설도 함께 폐기한다. 결과를 좋게 만들기 위한 추가 "
                                  "horizon·factor 탐색은 하지 않는다."),
        },
    }


def md(d):
    pc, rep = d["precommit"], d["reproduction"]
    st, bm = d["standalone"], d["bmTop20Internal"]
    ov, bo, mt, au, vd = (d["overlapAnalysis"], d["bootstrapInference"],
                          d["matchedControls"], d["audit"], d["finalVerdict"])
    o = []
    A = o.append
    A("전체 판정: PASS")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R14 — quality/compounder 신호는 장기 horizon 에서 나타나는가")
    A("")
    A(f"- 기간 {d['dataPeriod']['start']} ~ {d['dataPeriod']['end']} "
      f"({d['dataPeriod']['months']}개월) · 결정월 2010-01 부터")
    A(f"- 연구질문: {d['researchQuestion']}")
    A(f"- **연구 판정: {vd['finalVerdict']}**")
    A(f"- **PRIMARY_QUALITY_FACTOR: {vd['primaryQualityFactor'] or 'NONE'}**")
    A(f"- 장기보유 가설: **{vd['longHoldHypothesis'].split(' — ')[0]}**")
    A(f"- horizon rescue {vd['horizonRescue']} · parameter rescue "
      f"{vd['parameterRescue']} · 포트폴리오 탐색 {vd['portfolioSearchPerformed']} · "
      f"{d['realMoneyStage']}")
    A("")
    A("> 결론 한 줄: **보유기간을 1년에서 7년으로 늘려도 신호는 나타나지 않았다.**")
    A("> 다만 Founder 의 문제제기는 절반은 맞았다 — 음(−)의 크기가 horizon 이 길수록")
    A("> 일관되게 **줄어든다**(Q1 −26.5 → −7.7%p · Q2 −31.7 → −7.6%p). 방향은 맞았지만")
    A("> 0 을 넘지 못하고 −8%p 근처에서 평평해진다. 그래서 '보유기간이 짧아서 실패했다'는")
    A("> 가설도 함께 폐기한다.")
    A("")

    # 1. precommit
    A("## 1. 사전규격 (§4) — 결과 이전 저장")
    A("")
    A(f"정본: `{pc['file']}` · writtenBeforeResults **{pc['writtenBeforeResults']}**")
    A("")
    hr = pc["horizonRule"]
    A(f"- PRIMARY **{hr['primaryMonths']}M(5Y)** · ROBUSTNESS {hr['robustnessMonths']} · "
      f"REFERENCE {hr['referenceMonths']}M({hr['referencePurpose']})")
    A(f"- 10Y 제외: {hr['excluded10Y']}")
    A(f"- 금지: {', '.join(hr['forbidden'])}")
    A("")
    A("| ID | 개념 | 공식 | PIT 규칙 |")
    A("|---|---|---|---|")
    for f in pc["factors"]:
        A(f"| {f['id']} | {f['concept']} | `{f['formula']}` | {f['pitRule']} |")
    A("")
    A("### ★ 표본 현실 — 이것이 판정 방식을 결정했다")
    A("")
    sr = pc["sampleReality"]
    A("| horizon | 결정월수 | 비중첩 코호트 | 연초 코호트 | 마지막 결정월 |")
    A("|---|---|---|---|---|")
    for h, v in sr["byHorizon"].items():
        A(f"| {int(h)//12}Y | {v['decisionMonths']} | **{v['nonOverlapping']}** | "
          f"{v['annualStarts']} | {v['lastDecision']} |")
    A("")
    A(f"{sr['consequence']}")
    A("")
    A(f"> **비대칭 규칙(결과 이전 고정)** — {sr['asymmetry']}")
    A("")

    # 2. reproduction
    A("## 2. R13 재현 게이트")
    A("")
    A(f"결과 **{'PASS' if rep['pass'] else 'FAIL'}** — {rep['note']}")
    A("")
    A("| 지표 | R14 | R13 정본 | 차이 | 판정 |")
    A("|---|---|---|---|---|")
    for c in rep["checks"]:
        A(f"| {c['metric']} | {c['r14']} | {c['r13Canonical']} | {c['diff']} | "
          f"{'O' if c['pass'] else 'X'} |")
    A("")
    A(f"> {rep['annSpreadNote']}")
    A("")

    # 3. standalone
    A("## 3. 단독 signal — 전체 universe (§7)")
    A("")
    A("연율수익률 차이(TOP분위 − BOTTOM분위).")
    A("")
    A("| factor | 1Y | 3Y | 5Y★ | 7Y |")
    A("|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        s = st["rows"][f]["annSpreadPct"]
        A(f"| {SHORT[f]} | {sg(s['12'])}%p | {sg(s['36'])}%p | **{sg(s['60'])}%p** | "
          f"{sg(s['84'])}%p |")
    A("")
    A("전체 시장에서는 Q3(배당성향)만 모든 horizon 에서 양수다(+7.5 → +3.4%p).")
    A("Q1·Q2 는 전 horizon 음수이지만 horizon 이 길수록 음의 크기가 줄어든다.")
    A("")

    # 4. BM_TOP20 — 핵심
    A("## 4. BM 상위 20% **내부** 장기 incremental (§8) ★ 핵심")
    A("")
    A(f"{bm['note']}")
    A("")
    A("| factor | 1Y | 3Y | **5Y★** | 7Y | horizon shape |")
    A("|---|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        r = bm["rows"][f]
        s = r["annSpreadPct"]
        A(f"| {SHORT[f]} | {sg(s['12'])}%p | {sg(s['36'])}%p | **{sg(s['60'])}%p** | "
          f"{sg(s['84'])}%p | {r['horizonShape']} |")
    A("")
    A("### TOP분위 / BOTTOM분위의 연율수익률 자체")
    A("")
    A("| factor | 그룹 | 1Y | 3Y | 5Y | 7Y |")
    A("|---|---|---|---|---|---|")
    for f in QIDS:
        r = bm["rows"][f]
        A(f"| {SHORT[f]} | TOP(고품질) | {n2(r['topAnnPct']['12'])}% | "
          f"{n2(r['topAnnPct']['36'])}% | **{n2(r['topAnnPct']['60'])}%** | "
          f"{n2(r['topAnnPct']['84'])}% |")
        A(f"| {SHORT[f]} | BOTTOM(저품질) | {n2(r['bottomAnnPct']['12'])}% | "
          f"{n2(r['bottomAnnPct']['36'])}% | **{n2(r['bottomAnnPct']['60'])}%** | "
          f"{n2(r['bottomAnnPct']['84'])}% |")
    A("")
    A("**이것이 R14 의 답이다.** 싼 주식 안에서 5년을 들고 갔을 때,")
    A("꾸준히 흑자를 낸 기업은 연 4.67%, 그렇지 못한 기업은 연 12.75% 였다.")
    A("자본을 잘 불린 기업 4.62% vs 못 불린 기업 14.75%.")
    A("**보유기간을 늘려도 순서가 뒤집히지 않는다.**")
    A("")
    A("### 5Y 시장 / size 구간")
    A("")
    A("| factor | KOSPI | KOSDAQ | SMALL | MID | LARGE |")
    A("|---|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        r = bm["rows"][f]
        g = r["marketSegments"]
        z = r["sizeControlled"]
        A(f"| {SHORT[f]} | {sg(g.get('KOSPI', {}).get('60'))}%p | "
          f"{sg(g.get('KOSDAQ', {}).get('60'))}%p | "
          f"{sg(z.get('SMALL', {}).get('60'))}%p | "
          f"{sg(z.get('MID', {}).get('60'))}%p | "
          f"{sg(z.get('LARGE', {}).get('60'))}%p |")
    A("")

    # 5. horizon shape
    A("## 5. Horizon shape 판정 (§9)")
    A("")
    A("| factor | 1Y | 3Y | 5Y | 7Y | 판정 | 읽는 법 |")
    A("|---|---|---|---|---|---|---|")
    for f in QIDS:
        r = bm["rows"][f]
        s = r["annSpreadPct"]
        delta = (s["84"] - s["12"]) if (s["84"] is not None and s["12"] is not None) else None
        A(f"| {SHORT[f]} | {sg(s['12'])} | {sg(s['36'])} | {sg(s['60'])} | "
          f"{sg(s['84'])} | **{r['horizonShape']}** | 1Y→7Y 개선폭 {sg(delta)}%p |")
    A("")
    A("세 factor 모두 **D_INVERTED** — 모든 horizon 에서 음수다.")
    A("다만 개선폭이 Q1 +18.8%p · Q2 +24.2%p · Q3 +6.6%p 로 뚜렷하다.")
    A("즉 **'장기일수록 quality 가 덜 손해'는 사실이고, '장기면 quality 가 이긴다'는 거짓**이다.")
    A("")

    # 6. overlap
    A("## 6. Overlapping vs Non-overlapping (§10)")
    A("")
    A(f"{ov['note']}")
    A("")
    ovh = ov["byHorizon"][H]
    A("| factor | OVERLAPPING | ANNUAL_START | NON_OVERLAPPING |")
    A("|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        v = ovh[f]
        def cell(x):
            return ("-" if not x else
                    f"n={x['n']} 중앙 {sg(x['median'])}%p 양수 {x['positiveRatePct']}%")
        A(f"| {SHORT[f]} | {cell(v['OVERLAPPING'])} | {cell(v['ANNUAL_START'])} | "
          f"{cell(v['NON_OVERLAPPING'])} |")
    A("")
    A("Q2 는 **140개 겹침 코호트 전부(양수 0.0%)** 음수다. 표본이 얇아도 이 정도로")
    A("한 방향이면 기각은 타당하다.")
    A("")
    A("### 코호트 시작구간별 5Y spread 중앙값")
    A("")
    A("| factor | 2010-2013 | 2014-2017 | 2018-2021 | 양수 구간 |")
    A("|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        v = ovh[f]
        e = v.get("byStartEraMedianPct") or {}
        A(f"| {SHORT[f]} | {sg(e.get('2010-2013'))}%p | {sg(e.get('2014-2017'))}%p | "
          f"{sg(e.get('2018-2021'))}%p | {v.get('startErasPositive')} |")
    A("")
    A("> 하위구간은 **코호트 시작일** 기준으로 나눴다. 결정월 창을 4년 era 로 잘라")
    A("> 그 안에서 60개월 forward 를 요구하면 관측이 0 이 된다(초기 구현에서 실제로")
    A("> 빈 결과가 나왔고, 원인을 잡아 시작일 기준으로 고쳤다).")
    A("")
    A("| factor | 최악 코호트 | 최고 코호트 |")
    A("|---|---|---|")
    for f in QIDS:
        v = ovh[f]
        w, b = v["worstCohort"], v["bestCohort"]
        A(f"| {SHORT[f]} | {w['start'][:7]} {sg(w['spreadPct'])}%p | "
          f"{b['start'][:7]} {sg(b['spreadPct'])}%p |")
    A("")

    # 7. bootstrap
    A("## 7. 통계적 추론 (§11)")
    A("")
    A(f"{bo['_note']}")
    A("")
    A(f"draws {bo['_draws']} · seed {bo['_seed']} · deterministic")
    A("")
    A("| factor | 연초코호트 | 관측 | 방법 | 평균 | 95% CI | P(>0) |")
    A("|---|---|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        v = bo.get(f)
        if not isinstance(v, dict) or "results" not in v:
            continue
        first = True
        for m, r in v["results"].items():
            A(f"| {SHORT[f] if first else ''} | {v['cohorts'] if first else ''} | "
              f"{sg(v['observed']) if first else ''} | {m} | {sg(r['meanPct'])}%p | "
              f"[{sg(r['ci95LowerPct'])}, {sg(r['ci95UpperPct'])}] | {r['probGt0Pct']}% |")
            first = False
    A("")
    A("Q1·Q2 는 세 방법 모두 95% CI 상한이 0 아래다(P(>0) ≈ 0%).")
    A("Q3 는 CI 가 0 을 포함한다 — 사실상 신호 없음.")
    A("")

    # 8. matched
    A("## 8. size / exchange matched control (§15)")
    A("")
    A(f"{mt['note']}")
    A("")
    A("| factor | raw BM20 5Y | SIZE_MATCHED | EXCHANGE_MATCHED | SIZE+EXCHANGE |")
    A("|---|---|---|---|---|")
    for f in QIDS + ["ROE", "SIZE"]:
        raw = bm["rows"][f]["annSpreadPct"]["60"]
        A(f"| {SHORT[f]} | {sg(raw)}%p | "
          f"{sg(mt['rows']['SIZE_MATCHED'][f]['meanAnnSpreadPct'])}%p | "
          f"{sg(mt['rows']['EXCHANGE_MATCHED'][f]['meanAnnSpreadPct'])}%p | "
          f"{sg(mt['rows']['SIZE_EXCHANGE_MATCHED'][f]['meanAnnSpreadPct'])}%p |")
    A("")
    A("size·거래소를 통제하면 음의 크기가 줄어든다(Q1 −8.08 → −4.11 · Q2 −10.12 → −4.74).")
    A("즉 quality 의 손해 중 절반쯤은 '고품질 = 대형주' 노출 때문이다. 하지만 **통제 후에도**")
    A("**여전히 음수**다 — 순수 quality 축 자체가 저평가 구간에서 역방향이다.")
    A("")

    # 9. distress
    A("## 9. distress 회피 vs 복리효과 분리 (§17)")
    A("")
    A("| factor | 그룹 | 5Y내 상폐율 | 음의자본 | 지속손실 | 소형주 |")
    A("|---|---|---|---|---|---|")
    for f in QIDS:
        for g, lab in (("top", "TOP(고품질)"), ("bottom", "BOTTOM(저품질)")):
            r = au["distressExposure"][f][g]
            A(f"| {SHORT[f]} | {lab} | {n2(r['delisted'])}% | {n2(r['negEquity'])}% | "
              f"{n2(r['persistentLoss'])}% | {n2(r['microCap'])}% |")
    A("")
    A("**핵심 해석** — quality 는 부실을 확실히 피한다. Q1 고품질군 상폐율 3.97% vs")
    A("저품질군 15.01%. 그런데도 5년 수익률은 저품질군이 훨씬 높다(12.75% vs 4.67%).")
    A("즉 이 시장에서 **부실회피(DISTRESS_AVOIDANCE)는 작동하지만, 살아남은 부실주가**")
    A("**주는 보상이 파산 손실을 압도**한다. 저PBR 구간의 위험프리미엄이 실재한다는 뜻이고,")
    A("이것을 COMPOUNDING_EFFECT 로 오해하면 안 된다 — 복리효과는 관측되지 않았다.")
    A("")

    # 10. concentration
    A("## 10. 집중도 (§18)")
    A("")
    A("| factor | 거래종목 | top1 | top3 | top5 | top10 | top1% | top5% |")
    A("|---|---|---|---|---|---|---|---|")
    for f in QIDS:
        c = au["concentration"][f]
        A(f"| {SHORT[f]} | {c['tickers']} | {n2(c['top1SharePct'])}% | "
          f"{n2(c['top3SharePct'])}% | **{n2(c['top5SharePct'])}%** | "
          f"{n2(c['top10SharePct'])}% | {n2(c['top1PctSharePct'])}% | "
          f"{n2(c['top5PctSharePct'])}% |")
    A("")
    A(f"> {au['concentration'][Q1]['denominatorNote']}")
    A("")
    A("집중도는 상위 5종목 18~39% 로 극단적이지 않다. **음의 결과가 소수 종목 사고가**")
    A("**아니라 광범위한 현상**이라는 뜻이다(제거해도 방향이 바뀌지 않는다).")
    A("")

    # 11. return distribution
    A("## 11. 수익 분포 (§19)")
    A("")
    A("| factor | 그룹 | 중앙 | 평균 | 승률 | p10 | p25 | p75 | p90 | 왜도 |")
    A("|---|---|---|---|---|---|---|---|---|---|")
    for f in QIDS:
        for g, lab in (("top", "TOP(고품질)"), ("bottom", "BOTTOM(저품질)")):
            r = au["returnDistribution"][f][g]
            A(f"| {SHORT[f]} | {lab} | {n2(r['median'])}% | {n2(r['mean'])}% | "
              f"{n2(r['winRatePct'])}% | {n2(r['p10'])}% | {n2(r['p25'])}% | "
              f"{n2(r['p75'])}% | {n2(r['p90'])}% | {r.get('skewness')} |")
    A("")
    A("Q1·Q2 는 저품질군이 **중앙값·승률·평균 전부** 더 높다 — 꼬리효과가 아니다.")
    A("Q3 만 예외다: 고품질군이 중앙값(15.1% vs 10.9%)과 승률(61.1% vs 52.8%)에서")
    A("앞서지만 평균은 뒤진다(50.4% vs 77.6%). 배당 규율은 **오른쪽 꼬리를 포기하는**")
    A("**대신 안정성을 사는** 성격이고, 그래서 spread 는 음수다.")
    A("")

    # 12. intra-holding risk
    A("## 12. 5년 보유 중 위험 (§20)")
    A("")
    A("| factor | 그룹 | MDD 중앙 | MDD 최악 | 보유중 최악 1Y | 보유중 최악 3Y | 회복 개월 |")
    A("|---|---|---|---|---|---|---|")
    for f in QIDS:
        for g, lab in (("top", "TOP(고품질)"), ("bottom", "BOTTOM(저품질)")):
            r = au["intraHoldingRisk"][f][g]
            A(f"| {SHORT[f]} | {lab} | {n2(r['medianMaxDrawdownPct'])}% | "
              f"{n2(r['worstMaxDrawdownPct'])}% | {n2(r['medianWorst1YInsidePct'])}% | "
              f"{n2(r['medianWorst3YInsidePct'])}% | "
              f"{r['medianMonthsToRecoverPeak']} |")
    A("")
    A("**고품질군이 MDD 를 줄여주지도 않는다.** 중앙 MDD 가 양쪽 모두 −35~36% 수준이고,")
    A("Q1 은 고품질군 최악 MDD 가 오히려 더 깊다(−77.4% vs −61.8%).")
    A("R11 후보의 MDD −53.38% 를 quality 조건으로 개선하려는 기대는 이 데이터에서 근거가 없다.")
    A("")

    # 13. factor audits
    A("## 13. factor 정의 audit (§12·§13·§14)")
    A("")
    q2 = au["q2CapitalCompoundingAudit"]
    q3 = au["q3DividendDisciplineAudit"]
    A("```")
    A("Q2 자본복리 (관측 %d)" % q2["observations"])
    A("  주식수 1.5배 이상 증가(희석 proxy)   %.2f%%" % q2["shareGrowthDominant"])
    A("  직전 EPS 음수(기저효과)              %.2f%%" % q2["negPrevEps"])
    A("  기초 BPS 1,000원 미만(저베이스)      %.2f%%" % q2["lowBaseBps"])
    A("")
    A("Q3 배당성향 (관측 %d)" % q3["observations"])
    A("  EPS 100원 미만(분모 0 근접)          %.2f%%" % q3["epsNearZero"])
    A("  배당성향 상한 3.0 도달               %.2f%%" % q3["payoutAtCap"])
    A("  무배당(성향 0)                       %.2f%%" % q3["zeroPayout"])
    A("```")
    A("")
    A("Q2 의 14.31% 는 BPS 증가가 이익 유보가 아니라 **유상증자**일 가능성이 있다.")
    A("Q1 은 'EPS 양수 여부'만 보는 proxy 라는 한계가 있다. 둘 다 R13 정의 그대로")
    A("유지했고 **결과를 보고 filter 를 추가하지 않았다**(§12·§13).")
    A("")

    # 14. verdict
    A("## 14. 최종 판정 (§22·§23)")
    A("")
    A("| factor | 5Y spread | shape | 조건 통과 | 등급 |")
    A("|---|---|---|---|---|")
    for f in QIDS:
        g = vd["grades"][f]
        A(f"| {SHORT[f]} | {sg(g['annSpreadPct']['60'])}%p | {g['horizonShape']} | "
          f"{g['conditionsPassed']} | **{g['grade']}** |")
    A("")
    A(f"### 연구 판정: **{vd['finalVerdict']}**")
    A("")
    A(f"### PRIMARY_QUALITY_FACTOR: **{vd['primaryQualityFactor'] or 'NONE'}**")
    A("")
    A(f"{vd['primarySelectionRule']}")
    A("")
    A(f"### 장기보유 가설: {vd['longHoldHypothesis']}")
    A("")
    A(f"> {vd['sampleCaveat']}")
    A("")
    A(f"### R15 진행 가치: **{'있음' if vd['r15Eligible'] else '없음'}**")
    A("")
    A("R15(BM+quality 장기보유 포트폴리오)는 시작하지 않는다. 넘길 factor 가 없다.")
    A("")

    # 15. BM vs quality
    A("## 15. BM 과 quality 는 다른 horizon 에서 작동하는가 (§21)")
    A("")
    A("R14 는 최종 portfolio 가 아니므로 R11 의 BM_P20_N40_H24(CAGR 11.50% · MDD")
    A("−53.38%)와 factor spread 를 동일 전략처럼 비교하지 않는다. 연구 의미만 적는다.")
    A("")
    A("- **BM = VALUE REVERSION.** 싼 주식이 되돌아오는 힘이고, R11 에서 24개월 보유로")
    A("  작동했다. R13 에서 BM 상위20% pool 자체가 하위80% 대비 +7.37%p 였다.")
    A("- **Quality = LONG-TERM COMPOUNDING 가설.** R14 가 검증한 것이 이것인데,")
    A("  1Y~7Y 어느 horizon 에서도 저평가 구간 안에서 양수가 되지 않았다.")
    A("- 두 신호가 '다른 horizon 에서 작동한다'는 가설은 **지지되지 않았다.** BM 은")
    A("  중기(24M)에 작동하고, quality 는 장기에서도 작동하지 않는다.")
    A("- 한국 저PBR 구간에서 초과수익의 원천은 '좋은 회사'가 아니라 **'싸고 작고 위험한**")
    A("  **회사가 살아남았을 때의 보상'** 쪽에 가깝다.")
    A("")

    # 16. production / hotg
    A("## 16. production / forward 보호 (§29)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:30} {v}")
    A(f"{'realMoneyStage':30} {d['realMoneyStage']}")
    A("```")
    A("")
    A("## 17. HOTG 폐회로 (§30)")
    A("")
    h = d["hotgClosedLoop"]
    A("```")
    A(f"canonical report        {h['canonicalReport']}")
    A(f"Report Bridge 수집      {h['reportBridgeCollectable']}")
    A(f"source_owner            {h['sourceOwner']}")
    A(f"표면                    {h['surface']}")
    A(f"신규 observer           {h['newObserverCreated']}")
    A(f"중복 orchestration      {h['duplicateOrchestration']}")
    A(f"EXECUTION_SUCCEEDED_INVISIBLY  {h['executionSucceededInvisibly']}")
    A("```")
    A("")
    A(f"근거: {h['evidence']}")
    A("")
    A("## 18. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")
    A("## 19. Founder 행동 / 다음 단일 작업")
    A("")
    A(f"- **Founder 행동: {d['nextDecision']['founderAction']}**")
    A("")
    A(f"- 다음 단일 작업: {d['nextDecision']['single']}")
    A("")
    A("- 지금 하지 않는 것: " + " · ".join(d["nextDecision"]["notNow"]))
    A("")
    A(f"- {d['nextDecision']['forbiddenFollowUp']}")
    A("")
    return "\n".join(o)


def main() -> int:
    WD.mkdir(parents=True, exist_ok=True)
    d = build()
    (WD / f"{NAME}.json").write_text(
        json.dumps(d, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (WD / f"{NAME}.md").write_text(md(d), encoding="utf-8")
    print(json.dumps({"md": str(WD / f"{NAME}.md"), "json": str(WD / f"{NAME}.json"),
                      "final": d["finalVerdict"]["finalVerdict"],
                      "primary": d["finalVerdict"]["primaryQualityFactor"],
                      "longHold": d["finalVerdict"]["longHoldHypothesis"].split(" — ")[0]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
