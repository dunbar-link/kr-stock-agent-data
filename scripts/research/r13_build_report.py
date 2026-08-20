#!/usr/bin/env python3
"""R13 산출물 생성 (MD + JSON) — §19.

WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13

reports/wababa/wababa-value-profitability-factor-discovery-r13-latest.{md,json}
기존 R8~R12 산출물 수정 금지.

production 변경 0 · 홈페이지 0 · 실주문 0 · 브로커 0 · REAL_MONEY_NOT_APPROVED.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r13_factors as F  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
NAME = "wababa-value-profitability-factor-discovery-r13-latest"
ALL_NEW = F.TRACK_L_IDS + F.TRACK_D_IDS
SHORT = {F.L1: "L1 흑자지속", F.L2: "L2 배당성향", F.L3: "L3 자본복리",
         F.D1: "D1 ROIC", F.D2: "D2 영업이익/자산", F.D3: "D3 영업CF/자산"}


def L(n):
    p = RD / f"r13-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def sg(x, nd=2):
    return "-" if x is None else f"{x:+.{nd}f}"


def n2(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def build():
    pc, rep = L("precommit"), L("repro")
    s1, s2 = L("stage1"), L("stage2")
    ort, ctl, aud, vd = L("ortho"), L("control"), L("audit"), L("verdict")
    return {
        "schema": "wababa-value-profitability-factor-discovery-r13@1",
        "taskId": "WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13",
        "verdictLine": "PASS",
        "reasonClass": "R13_NO_INCREMENTAL_PROFITABILITY_SIGNAL_REQUESTED_FACTORS_DATA_INSUFFICIENT",
        "stage": "FACTOR_DISCOVERY (포트폴리오 최적화 아님)",
        "researchQuestion": pc["researchQuestion"],
        "dataPeriod": rep["period"],
        "precommit": {
            "created": True, "writtenBeforeResults": pc["writtenBeforeResults"],
            "file": "reports/research/r13-precommit-latest.json",
            "trackL": pc["trackL"], "trackD": pc["trackD"],
            "pitRule": pc["pitRule"], "common": pc["common"],
            "eligibilityForPrimary": pc["eligibilityForPrimary"],
            "noParameterRescue": pc["noParameterRescue"]},
        "dataAvailability": pc["dataAvailabilityFindings"],
        "reproduction": rep,
        "stage1Standalone": s1,
        "stage2BmTop20Internal": s2,
        "orthogonality": ort,
        "matchedControl": ctl,
        "extremeAndDistressAudit": aud,
        "finalVerdict": vd,
        "limitations": [
            "장기 PIT 스냅샷(2007~2026)에 자산·매출·영업이익·영업현금흐름이 없다. "
            "요청된 ROIC/OP/CP 는 이 구간에서 계산 자체가 불가능하다 — 구현 문제가 "
            "아니라 정보 부재다.",
            "DART 캐시는 FY2022~FY2025 4개 회계연도뿐이다. 12M forward 가능 결정월 "
            "약 28개월 · 비중첩 관측 2개 · 단일 시장국면이라 시대별/시작시점 안정성 "
            "검증이 불가능하다.",
            "TRACK L 3축은 '자본 대비 이익' 계열의 대용 지표다. 자산·매출 기반 "
            "profitability 와 경제적으로 동일하지 않다. 이 결과를 ROIC/OP/CP 에 대한 "
            "결론으로 확대해석하면 안 된다.",
            "업종 코드가 PIT 스냅샷에 없어 sector exposure 는 측정하지 못했다(§9 일부 미달).",
            "월 스냅샷 · 배당 재투자 미반영 · 상장폐지는 마지막 관측가 청산(R7 동일 가정).",
            "DART 레코드의 0.87%(22개 기업)는 12월 결산이 아니다(예: 기신정기 3월 결산 → "
            "FY2022 를 2022-06 제출). 공시일 기반 PIT 라 이런 기업도 정확히 처리되지만, "
            "'회계연도 + 3개월' 같은 고정 lag 규칙을 썼다면 오류가 났을 지점이다.",
            "10분위 12M forward 는 12개월 중첩 관측이다. 비중첩 관측수를 함께 보고했다.",
            "백테스트 결과이며 투자 실행 승인이 아니다. 개인화된 투자권유가 아니다.",
        ],
        "productionChange": {
            "krStockAgentRepo": "untouched", "productionUrl": "untouched",
            "legacy50d": "unchanged", "newBmR8FrozenForwardState": "존재하지 않음(미생성)",
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
            "closeCondition": "R13 판정이 08:40 종합보고에 올라가면 CLOSE(추가 승인 불필요).",
            "executionSucceededInvisibly": False},
        "nextDecision": {
            "founderAction": "NONE — 승인 게이트 사항 없음. 판정은 데이터가 내렸다.",
            "single": ("R14(BM+profitability 결합)로 넘어가지 않는다. 대신 R13 이 "
                       "실제로 찾아낸 단 하나의 강한 BM 내부 신호를 검증한다 — "
                       "BM 상위 20% 안에서 SIZE 가 4/4 하위구간 전부 양수(+15.08%p)다. "
                       "다음 단일 연구질문: 'BM 상위 20% 내부의 소형주 편중이 "
                       "R11 후보의 초과수익을 이미 설명하는가, 아니면 추가 축인가?'"),
            "notNow": ["R14 결합 포트폴리오", "수익성 조건 억지 부착",
                       "P/N/H 재탐색", "DART 28개월 결과로 factor 승격",
                       "홈페이지 공개", "실계좌 자금"],
            "forbiddenFollowUp": ("§27 — R13 이 NO_INCREMENTAL_PROFITABILITY_SIGNAL "
                                  "이므로 억지로 수익성 조건을 붙이지 않는다."),
        },
    }


def md(d):
    pc, rep = d["precommit"], d["reproduction"]
    s1, s2 = d["stage1Standalone"], d["stage2BmTop20Internal"]
    ort, ctl, aud, vd = (d["orthogonality"], d["matchedControl"],
                         d["extremeAndDistressAudit"], d["finalVerdict"])
    o = []
    A = o.append
    A("전체 판정: PASS")
    A(f"reason_class: {d['reasonClass']}")
    A("")
    A("# 와바바 R13 — 저평가(BM) + 수익성 factor discovery")
    A("")
    A(f"- 기간 {d['dataPeriod']['start']} ~ {d['dataPeriod']['end']} "
      f"({d['dataPeriod']['months']}개월)")
    A(f"- 연구질문: {d['researchQuestion']}")
    A(f"- **연구 판정: {vd['finalVerdict']}**")
    A(f"- **요청된 실물 factor(ROIC/OP/CP) 상태: {vd['requestedFactorsStatus']}**")
    A(f"- PRIMARY_PROFITABILITY_FACTOR: **{vd['primaryProfitabilityFactor'] or '없음'}**")
    A(f"- parameter rescue {vd['parameterRescue']} · 포트폴리오 탐색 "
      f"{vd['portfolioSearchPerformed']} · 실제 돈 {d['realMoneyStage']}")
    A("")
    A("> 결론 한 줄: **싼 주식 중에서 '질 좋은 회사'를 고르는 것은 한국 장기 데이터에서")
    A("> 오히려 손해였다.** 검증 가능한 3개 축 모두 BM 상위 20% 내부에서 음수이고,")
    A("> size·거래소를 통제한 뒤에도 음수다. 그리고 원래 요청된 ROIC/영업이익률/")
    A("> 영업현금흐름 지표는 이 저장소 데이터로는 **장기 검증 자체가 불가능**하다.")
    A("")

    # 1. 데이터 현실
    A("## 1. 왜 규격이 이렇게 됐는가 — 데이터 조사 결과")
    A("")
    da = d["dataAvailability"]
    A("```")
    A("장기 PIT 스냅샷  " + da["longPitSnapshots"]["period"])
    A("  컬럼           " + ", ".join(da["longPitSnapshots"]["columns"]))
    A("  → 자산·매출·영업이익·영업현금흐름 없음")
    A("  → ROIC / 영업이익률 / 영업CF 비율은 2007~2022 구간에서 계산 불가")
    A("")
    A("DART 재무제표    FY" + " · FY".join(str(y) for y in da["dartStatements"]["fiscalYears"]))
    A("  기업수         " + da["dartStatements"]["companiesPerYear"])
    A("  태그 커버리지   " + " · ".join(f"{k} {v}%" for k, v in
                                   da["dartStatements"]["tagCoveragePct"].items()))
    A("  공시일 출처     " + da["dartStatements"]["filingDateSource"])
    A("  중앙 공시일     " + " · ".join(f"{k} {v}" for k, v in
                                   da["dartStatements"]["medianFilingDates"].items()))
    A("  사용가능 결정월 " + da["dartStatements"]["usableDecisionWindow"])
    A("  12M forward     " + da["dartStatements"]["decisionMonthsWith12mForward"])
    A("")
    A("financial-universe-real.json  " + da["financialUniverseRealJson"]["nature"])
    A("  → " + da["financialUniverseRealJson"]["decision"])
    A("```")
    A("")
    A("이 조사 결과가 규격을 결정했다. 요청된 P1/P2/P3 는 **DART 에서만** 계산되고")
    A("그 구간은 통계적으로 얇다. 그래서 결과를 보기 전에 두 트랙으로 나누고,")
    A("DART 트랙에는 **PRIMARY 자격을 주지 않는다**고 사전에 못 박았다.")
    A("")

    # 2. precommit
    A("## 2. 사전규격 (§5) — 결과 이전 저장")
    A("")
    A(f"정본: `{pc['file']}` · writtenBeforeResults **{pc['writtenBeforeResults']}**")
    A("")
    A("### TRACK L — 장기 KRX PIT (2007~2026)")
    A("")
    A("| ID | 개념 | 공식 | PIT 규칙 |")
    A("|---|---|---|---|")
    for f in pc["trackL"]["factors"]:
        A(f"| {f['id']} | {f['concept']} | `{f['formula']}` | {f['pitRule']} |")
    A("")
    A("### TRACK D — DART 실물 (FY2022~FY2025) · 지시문이 요청한 P1/P2/P3")
    A("")
    A("| ID | 요청 항목 | 공식 |")
    A("|---|---|---|")
    for f in pc["trackD"]["factors"]:
        A(f"| {f['id']} | {f['requestedAs']} | `{f['formula']}` |")
    A("")
    A("### PIT lag (§4)")
    A("")
    A("```")
    A("TRACK L  " + pc["pitRule"]["trackL"])
    A("TRACK D  " + pc["pitRule"]["trackD"]["rule"])
    A("         버퍼 " + str(pc["pitRule"]["trackD"]["bufferMonths"]) + "개월 · "
      + pc["pitRule"]["trackD"]["latestWins"])
    A("         " + pc["pitRule"]["trackD"]["hardFail"])
    A("금지     " + pc["pitRule"]["forbidden"])
    A("```")
    A("")
    A("### PRIMARY 자격 제한 — 결과 이전에 고정한 핵심 장치")
    A("")
    el = pc["eligibilityForPrimary"]
    A(f"{el['rule']}")
    A("")
    A(f"→ {el['consequence']}")
    A("")
    A(f"> {el['why']}")
    A("")
    A("### winsorization")
    A("")
    A(f"{pc['common']['winsorization']['rule']} — {pc['common']['winsorization']['why']}")
    A("")

    # 3. reproduction
    A("## 3. R7 methodology 재현 게이트")
    A("")
    A(f"결과 **{'PASS' if rep['pass'] else 'FAIL'}** — {rep['note']}")
    A("")
    A("| factor | R13 | R7 정본 | 차이 | 판정 |")
    A("|---|---|---|---|---|")
    for c in rep["checks"]:
        A(f"| {c['factor']} | {c['r13']} | {c['r7Canonical']} | {c['diff']} | "
          f"{'O' if c['pass'] else 'X'} |")
    A("")

    # 4. stage1
    A("## 4. 1단계 — 단독 factor signal (§7)")
    A("")
    A("BM 과 섞지 않고 전체 universe 에서 10분위 TOP−BOTTOM 12M 연율 spread.")
    A("")
    A("| factor | 판정 | 12M spread | 비중첩 관측 | 평균 종목수 |")
    A("|---|---|---|---|---|")
    for f in ALL_NEW:
        A(f"| {SHORT[f]} | **{s1['verdicts'][f]['verdict']}** | "
          f"{sg(s1['spread12AnnPct'][f])}%p | {s1['obsNonOverlapping'][f]} | "
          f"{s1['avgUniverseCount'][f]} |")
    A("")
    A("| 하위구간 | " + " | ".join(SHORT[f] for f in ALL_NEW) + " |")
    A("|---|" + "---|" * len(ALL_NEW))
    for lab, v in s1["subperiods"].items():
        A(f"| {lab} | " + " | ".join(sg(v.get(f)) + "%p" if v.get(f) is not None else "-"
                                     for f in ALL_NEW) + " |")
    A("")
    A("전체 universe 기준으로는 L2(배당성향)만 양수(+7.49%p · 4개 구간 전부 양수)다.")
    A("L1·L3 은 ROE 와 같은 방향으로 **역신호**다. 한국 장기 데이터에서 '질 좋은 회사'는")
    A("비싸고, 싼 저질 회사가 더 올랐다는 뜻이다 — R7 의 ROE INVERTED 와 일관된다.")
    A("")

    # 5. stage2
    A("## 5. 2단계 — BM 상위 20% **내부** incremental signal (§8) ★ 핵심")
    A("")
    A(f"{s2['note']}")
    A("")
    A("| factor | 12M spread | 24M spread | 하위구간 + | 비중첩 | 평균 종목수 |")
    A("|---|---|---|---|---|---|")
    for f in ALL_NEW:
        r = s2["rows"][f]
        A(f"| {SHORT[f]} | **{sg(r['spread12AnnPct'])}%p** | "
          f"{sg(r['spread24AnnPct'])}%p | {r['subperiodsPositive']} | "
          f"{r['obsNonOverlapping']} | {r['avgUniverseCount']} |")
    for f in ("ROE", "EY", "SIZE"):
        r = s2["rows"].get(f)
        if r:
            A(f"| {f} (참고) | {sg(r['spread12AnnPct'])}%p | "
              f"{sg(r['spread24AnnPct'])}%p | {r['subperiodsPositive']} | "
              f"{r['obsNonOverlapping']} | {r['avgUniverseCount']} |")
    A("")
    A("| 하위구간 | " + " | ".join(list(SHORT.values()) + ["ROE", "SIZE"]) + " |")
    A("|---|" + "---|" * (len(ALL_NEW) + 2))
    labs = list(s2["rows"][F.L1]["subperiods"].keys())
    for lab in labs:
        cells = []
        for f in ALL_NEW + ["ROE", "SIZE"]:
            v = s2["rows"][f]["subperiods"].get(lab)
            cells.append(sg(v) + "%p" if v is not None else "-")
        A(f"| {lab} | " + " | ".join(cells) + " |")
    A("")
    A("**이것이 R13 의 답이다.**")
    A("")
    A("- 장기 검증 가능한 3축 전부 음수 — L1 −26.47%p(0/4) · L2 −7.48%p(1/4) ·")
    A("  L3 −31.73%p(0/4). L2 는 전체 universe 에서 +7.49%p 였는데 **싼 주식 안에서는")
    A("  −7.48%p 로 부호가 뒤집힌다**.")
    A("- 음성 대조군 ROE 도 −10.86%p 로 같은 방향이다(§3 예상대로).")
    A("- 반면 **SIZE 는 +15.08%p 이고 4/4 하위구간 전부 양수**다. 싼 주식 안에서")
    A("  실제로 작동한 축은 수익성이 아니라 **크기**였다.")
    A("")
    A("### DART 실물 factor 는 왜 승격하지 않았는가")
    A("")
    A(f"{vd['dartShortWindowCaution']['why_not_promoted']}")
    A("")
    for x in vd["dartShortWindowCaution"]["evidence"]:
        A(f"- {x}")
    A("")

    # 6. ortho
    A("## 6. 3단계 — orthogonality / redundancy (§9)")
    A("")
    A("| factor | vs BM | vs SIZE | vs EY | vs ROE | 상위20% KOSDAQ 비중 | 상위20% 시총 percentile |")
    A("|---|---|---|---|---|---|---|")
    for f in ALL_NEW:
        c = ort["rankCorrelation"][f]
        A(f"| {SHORT[f]} | {n2(c['BM'], 3)} | {n2(c['SIZE'], 3)} | {n2(c['EY'], 3)} | "
          f"**{n2(c['ROE'], 3)}** | {n2(ort['top20PctKosdaqShare'][f], 3)} | "
          f"{n2(ort['top20PctMedianCapPercentile'][f], 3)} |")
    A("")
    A("DART 실물 factor(D1·D2)의 ROE 순위상관이 **0.77** 이다 — 사실상 ROE 의 다른")
    A("표현이다. ROE 는 R7 에서 장기 INVERTED 였고 R13 의 BM 내부에서도 −10.86%p 다.")
    A("28개월 창의 양수 결과를 장기 신호로 믿을 수 없는 독립적 이유다.")
    A("")
    A(f"> {ort['sectorExposure']}")
    A("")

    # 7. matched control
    A("## 7. Matched control (§10) — size·거래소 통제 후에도 음수인가")
    A("")
    A(f"{ctl['note']}")
    A("")
    A("| factor | 평균 spread | 중앙 spread | 양수 비율 | 사용 셀 수 | 관측 개월 |")
    A("|---|---|---|---|---|---|")
    for f in ALL_NEW:
        r = ctl["rows"][f]
        A(f"| {SHORT[f]} | **{sg(r['meanSpread12Pct'])}%p** | "
          f"{sg(r['medianSpread12Pct'])}%p | {r['positiveRatePct']}% | "
          f"{r['avgCellsUsed']} | {r['months']} |")
    A("")
    A("size·거래소를 통제해도 L1 −11.91%p · L2 −8.53%p · L3 −16.05%p 로 여전히 음수다.")
    A("**즉 음의 결과는 size 효과의 부산물이 아니라 수익성 축 자체의 성질이다.**")
    A("")

    # 8. audit
    A("## 8. 극단치 · 집중도 · distress (§11·§13)")
    A("")
    A("| factor | 상위1 기여 | 상위5 기여 | 상위10 기여 | 극단값(|x|>5) |")
    A("|---|---|---|---|---|")
    for f in ALL_NEW:
        c = aud["concentration"][f]
        e = aud["extremeValues"][f]
        A(f"| {SHORT[f]} | {n2(c['top1SharePct'])}% | **{n2(c['top5SharePct'])}%** | "
          f"{n2(c['top10SharePct'])}% | {n2(e.get('hugeValue'))}% |")
    A("")
    A("### 비12월 결산 기업 처리")
    A("")
    A("DART 레코드의 **0.87%(22개 기업)**는 12월 결산이 아니다. 예를 들어 기신정기")
    A("(092440)는 3월 결산이라 FY2022(2022-03 종료)를 2022-06 에 제출한다 — 이 회사의")
    A("FY2022 재무를 2022-08 부터 쓰는 것은 정상 PIT 다. 실제 공시일(rcept_no) 기반")
    A("규칙이라 이런 기업도 자동으로 맞게 처리된다. '회계연도 + 고정 3개월' 같은 규칙을")
    A("썼다면 오류가 났을 지점이고, 회귀 테스트가 이를 고정한다(공시 이전 사용 0건 /")
    A("5,277건 검사 · 12월결산 추정 7,194건 전부 FY 종료월 이후 사용).")
    A("")
    A("장기 3축은 상위5 기여가 10~20% 로 낮다 — **음의 결과가 몇 종목의 사고가 아니라**")
    A("**광범위한 현상**이라는 뜻이다. 반대로 D1·D2 는 52~58% 로 극단적으로 집중돼 있어")
    A("28개월 양수 결과가 소수 종목에서 나왔음을 보여준다.")
    A("")
    A("### distress 노출 (BM 상위20% 안에서 각 factor 상위 20% 종목)")
    A("")
    base = aud["universeDistressBaseline"]
    A("| 구분 | 음의자본 | 지속손실 | 극단저PBR | 12M내 상폐 | 소형(<500억) |")
    A("|---|---|---|---|---|---|")
    A(f"| BM상위20% 전체(기준선) | {n2(base['negEquity'])}% | "
      f"{n2(base['persistentLoss'])}% | {n2(base['extremeLowPbr'])}% | "
      f"{n2(base['delisted12m'])}% | {n2(base['microCap'])}% |")
    for f in ALL_NEW:
        x = aud["distressExposure"][f]
        A(f"| {SHORT[f]} 상위20% | {n2(x['negEquity'])}% | {n2(x['persistentLoss'])}% | "
          f"{n2(x['extremeLowPbr'])}% | {n2(x['delisted12m'])}% | "
          f"{n2(x['microCap'])}% |")
    A("")
    A("수익성 상위 종목은 기준선보다 부실 노출이 **낮다**. 즉 이 factor 들은 실제로")
    A("'망할 회사'를 걸러낸다. 그런데도 수익률은 더 나빴다 — 부실 제거가 곧 수익은")
    A("아니라는 뜻이고, 한국 저PBR 구간에서는 오히려 그 위험프리미엄이 보상이었다.")
    A("**이번 R13 에서 distress filter 를 투자규칙으로 추가하지 않았다(§13).**")
    A("")

    # 9. 비교표
    A("## 9. ROE · 기존 Magic Formula 와의 비교 (§18)")
    A("")
    A("| factor | 단독 12M spread | 단독 판정 | BM상위20% 내부 spread | 하위구간 + | size 안정성 | 데이터 등급 |")
    A("|---|---|---|---|---|---|---|")
    ref = {"BM": "STRONG_SIGNAL", "ROE": "INVERTED_SIGNAL", "EY": "PROMISING_SIGNAL",
           "BM_ROE": "WEAK_SIGNAL", "MF": "NO_SIGNAL"}
    for f in ("BM", "ROE", "EY", "BM_ROE", "MF"):
        r2 = s2["rows"].get(f)
        A(f"| {f} | {sg(s1['spread12AnnPct'].get(f))}%p | {ref[f]} | "
          f"{sg(r2['spread12AnnPct']) + '%p' if r2 else '(미측정)'} | "
          f"{r2['subperiodsPositive'] if r2 else '-'} | "
          f"{str(r2['sizesPositive']) + '/3' if r2 else '-'} | LONG_OK |")
    for f in ALL_NEW:
        r2 = s2["rows"][f]
        grade = "LONG_OK" if f in F.TRACK_L_IDS else "SHORT_28M_THIN"
        A(f"| {SHORT[f]} | {sg(s1['spread12AnnPct'][f])}%p | "
          f"{s1['verdicts'][f]['verdict']} | **{sg(r2['spread12AnnPct'])}%p** | "
          f"{r2['subperiodsPositive']} | {r2['sizesPositive']}/3 | {grade} |")
    A("")
    A("**왜 기존 '싼 주식 + ROE'가 약했는지, 그리고 새 정의도 다르지 않은 이유**")
    A("")
    A("- ROE 는 R7 에서 단독 −6.70%p(INVERTED)였고, R13 에서 BM 상위20% 내부에서도")
    A("  −10.86%p 다. BM_ROE 가 WEAK(+5.48%p)였던 것은 ROE 가 기여해서가 아니라")
    A("  BM(+16.58%p)이 끌고 간 결과다.")
    A("- 이번에 새로 정의한 3축도 BM 내부에서 전부 음수다. 즉 문제는 'ROE 라는 특정")
    A("  지표'가 아니라 **한국 저PBR 구간에서 수익성 축 자체가 역방향**이라는 것이다.")
    A("- 요청된 실물 지표(ROIC·영업이익률·영업CF)는 장기 데이터가 없어 이 결론을")
    A("  확인도 반박도 할 수 없다.")
    A("")

    # 10. 판정
    A("## 10. 최종 판정 (§14·§15·§25)")
    A("")
    A("| factor | 등급 | 조건 통과 | PRIMARY 자격 | 탈락 사유 |")
    A("|---|---|---|---|---|")
    for f in ALL_NEW:
        g = vd["grades"][f]
        rej = vd["rejectedFactors"].get(f, {})
        A(f"| {SHORT[f]} | **{g['grade']}** | {g['conditionsPassed']} | "
          f"{'O' if g['primaryEligible'] else 'X'} | {rej.get('reason', '-')} |")
    A("")
    A(f"### 연구 판정: **{vd['finalVerdict']}**")
    A("")
    A(f"{vd['terminationRecord']}")
    A("")
    A(f"### 요청된 실물 factor 상태: **{vd['requestedFactorsStatus']}**")
    A("")
    A(f"{vd['requestedFactorsNote']}")
    A("")
    A(f"### PRIMARY_PROFITABILITY_FACTOR: **{vd['primaryProfitabilityFactor'] or '없음'}**")
    A("")
    A(f"선정 규칙(적용됐으나 후보 없음): {vd['primarySelectionRule']}")
    A("")
    A("**다음 단계로 넘길 factor 없음** — R14(BM+profitability 결합)는 시작하지 않는다.")
    A("§27 에 따라 억지로 수익성 조건을 붙이지 않는다.")
    A("")

    # 11. 상품성
    A("## 11. 상품성 관점 (§17 — 참고, 합격 기준 아님)")
    A("")
    A("Founder 의 원래 의도는 'BM 단독보다 더 높은 수익률과 더 좋은 위험조정 성과'였다.")
    A("R13 결과는 **그 경로가 수익성 factor 로는 열리지 않는다**는 것이다.")
    A("R11 후보의 MDD −53.38% · worst 1Y −53.58% 문제를 수익성 조건으로 개선하려는")
    A("시도는 이 데이터에서 근거가 없다. 숫자를 만들기 위해 parameter 를 조작하지 않았다.")
    A("")

    # 12. 테스트/안전
    A("## 12. production / public 보호 (§21)")
    A("")
    A("```")
    for k, v in d["productionChange"].items():
        A(f"{k:30} {v}")
    A(f"{'realMoneyStage':30} {d['realMoneyStage']}")
    A("```")
    A("")
    A("## 13. HOTG 폐회로 (§22)")
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
    A("## 14. 한계")
    A("")
    for x in d["limitations"]:
        A(f"- {x}")
    A("")
    A("## 15. Founder 행동 / 다음 단일 작업")
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
                      "requested": d["finalVerdict"]["requestedFactorsStatus"],
                      "primary": d["finalVerdict"]["primaryProfitabilityFactor"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
