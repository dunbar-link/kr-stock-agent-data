#!/usr/bin/env python3
"""R25 사전 고정 (precommit) — 결과를 보기 전에 전부 확정한다.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

R25 는 R7/R11/R14 재실행이 아니다. 기존 결과를 정답으로 쓰지 않는다(§1).
여기 적힌 정의·방향·기준은 결과를 본 뒤 바꾸지 않는다(§2·§7·§18·§35).

안전: 읽기 전용 + reports/research write 만. 네트워크 0.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

TASK = "WABABA-CANONICAL-FACTOR-REDISCOVERY-R25"

QUESTION = (
    "실제 shareholder wealth 를 측정하는 canonical TSR 기준에서, 한국 주식 장기 "
    "데이터에 존재하는 단순하고 독립적이며 재현 가능한 factor signal 은 무엇인가?")

SUCCESS = (
    "높은 CAGR 을 하나 찾는 것이 성공이 아니다. 어떤 기업 특성이 미래 shareholder "
    "wealth 와 반복적으로 연결되는지 정직하게 밝히는 것이 성공이다. BM 이 죽어도, "
    "Quality 가 죽어도, 아무것도 안 남아도 성공이다(§35).")

# ── §4 universe / PIT ─────────────────────────────────────────────────
UNIVERSE = {
    "source": "_cache/pit-snapshots/*.csv.gz (월별 PIT 스냅샷)",
    "period": {"start": "2007-01-02", "end": "2026-08-03"},
    "why": "R24 foundation 이 검증한 구간. 새 데이터 수집 금지(§24).",
    "asOfUniverse": "각 시점의 실제 상장 종목만. current-universe bias 금지.",
    "delisting": ("상폐 종목 제외 금지(§16). canonical 엔진의 DELISTING 정책"
                  "(UNKNOWN_RECOVERY = 마지막 관측가 청산)을 그대로 쓴다."),
    "survivorship": "생존 종목만 보지 않는다. 편입 시점 universe 전체를 본다.",
    "lookAhead": ("factor 값은 그 스냅샷 날짜에 실제 공표된 값만 사용한다. "
                  "미래 재무·미래 가격 사용 금지."),
    "shareClass": ("보통주·우선주 라인을 모두 그대로 둔다. R24 가 우선주 라인의 "
                   "자본행위를 직접 판정했으므로 인위적 제외를 하지 않는다."),
}

PIT_RULE = {
    "factorAsOf": "스냅샷 date 의 KRX 공표값(PER·PBR·EPS·BPS·DIV·DPS)",
    "why": ("KRX 가 그날 공표한 값이므로 그날 투자자가 실제로 볼 수 있었다. "
            "추가 lag 를 넣지 않는다 — 넣으면 오히려 실제보다 늦은 정보가 된다."),
    "forwardReturnStart": "같은 스냅샷 date (신호 관측 시점에 매수)",
    "noRestatement": "사후 정정 재무 사용 금지. 스냅샷 원본만.",
    "derivedFactors": ("과거 창을 쓰는 factor(지속성·BPS 성장·배당규율)는 "
                       "**과거 스냅샷만** 사용한다. 창은 아래 factorDefinitions 에 고정."),
}

# ── §3 return engine ──────────────────────────────────────────────────
ENGINE = {
    "name": "CANONICAL_TSR_R24",
    "base": "r16_canonical.CanonicalWealth",
    "overrides": ("R17~R24 가 직접 판정한 자본행위(무상증자·주식배당·분할·병합·"
                  "구주주 배정 유상증자·권리없음 확정)를 reconciliation 에서 읽어 "
                  "적용한다. r24-full-reconciliation-latest.json 이 정본."),
    "policy": "R17 POLICY_A_ASSUME_FULL_EXERCISE + 합리적 실권(K >= P_ex)",
    "returnBasis": "TIME_WEIGHTED — 외부 납입을 중립화한다.",
    "covers": ["cash dividend", "special dividend", "split", "reverse split",
               "bonus issue", "stock dividend", "rights issue",
               "shareholder contribution", "public offering",
               "third-party allocation", "CB conversion", "BW exercise",
               "merger shares", "share exchange", "company split",
               "capital reduction", "treasury-share cancellation"],
    "closeOnlyForbidden": True,
    "sameEngineFor": ["factor portfolio", "control", "benchmark"],
    "monthFactor": ("effective_m(k) = m(k)·(1+y(k)) − extPerShare(k)/p(k); "
                    "TSR(i→j) = (cum[j]/cum[i])·(p_j/p_i) − 1. "
                    "r17_wealth.RightsWealth.run 과 수학적으로 동치이며 "
                    "회귀 테스트로 동치를 강제한다."),
}

# ── §5·§7 factor 정의와 **방향** (결과 보고 뒤집기 금지) ──────────────
# direction: +1 = 값이 높을수록 좋다는 가설 / -1 = 낮을수록 좋다는 가설
FACTORS = {
    # A. VALUE
    "BM": {"family": "VALUE", "formula": "1 / PBR",
           "direction": +1, "hypothesis": "장부가 대비 싼 주식이 이긴다",
           "requires": ["PBR"], "positiveOnly": ["PBR"]},
    "EY": {"family": "VALUE", "formula": "1 / PER",
           "direction": +1, "hypothesis": "이익 대비 싼 주식이 이긴다",
           "requires": ["PER"], "positiveOnly": ["PER"]},
    # B. PROFITABILITY / QUALITY
    "ROE": {"family": "QUALITY", "formula": "EPS / BPS",
            "direction": +1, "hypothesis": "잘 버는 회사가 이긴다(§23 Founder 가설)",
            "requires": ["EPS", "BPS"], "positiveOnly": ["BPS"]},
    "EARNINGS_PERSISTENCE": {
        "family": "QUALITY",
        "formula": "직전 36개월 스냅샷 중 EPS > 0 인 비율",
        "direction": +1, "hypothesis": "꾸준히 흑자인 회사가 이긴다",
        "window": 36, "minObs": 12, "requires": ["EPS"]},
    "BPS_GROWTH": {
        "family": "QUALITY",
        "formula": "BPS(t) / BPS(t-36) − 1 (자기자본 복리 proxy)",
        "direction": +1, "hypothesis": "자기자본을 복리로 쌓는 회사가 이긴다",
        "window": 36, "requires": ["BPS"], "positiveOnly": ["BPS"]},
    # C. SIZE
    "SIZE_SMALL": {
        "family": "SIZE", "formula": "-ln(marketCap)  (작을수록 큰 값)",
        "direction": +1, "hypothesis": "작은 회사가 이긴다(small-cap premium)",
        "requires": ["marketCap"], "positiveOnly": ["marketCap"],
        "directionWhy": ("학술 표준 가설이 small-cap premium 이므로 '작을수록 좋다' "
                         "로 사전 고정한다. R13/R14 결과를 근거로 쓰지 않는다(§1).")},
    # D. SHAREHOLDER RETURN
    "DIVIDEND_YIELD": {
        "family": "SHAREHOLDER_RETURN", "formula": "DIV (KRX 공표 배당수익률 %)",
        "direction": +1, "hypothesis": "배당 많이 주는 회사가 이긴다",
        "requires": ["DIV"]},
    "DIVIDEND_DISCIPLINE": {
        "family": "SHAREHOLDER_RETURN",
        "formula": "직전 36개월 스냅샷 중 DPS > 0 인 비율",
        "direction": +1, "hypothesis": "배당을 꾸준히 주는 회사가 이긴다",
        "window": 36, "minObs": 12, "requires": ["DPS"]},
    # E. LEGACY COMBINATION (비교 reference 전용)
    "MAGIC_FORMULA_LEGACY": {
        "family": "LEGACY_COMBO",
        "formula": "rank(EY) + rank(ROE) 합산 (과거 근사 정의 그대로)",
        "direction": +1, "hypothesis": "싸고 잘 버는 회사가 이긴다",
        "requires": ["PER", "EPS", "BPS"], "referenceOnly": True},
    "BM_ROE_LEGACY": {
        "family": "LEGACY_COMBO", "formula": "rank(BM) + rank(ROE) 합산",
        "direction": +1, "hypothesis": "싸고 잘 버는 회사가 이긴다",
        "requires": ["PBR", "EPS", "BPS"], "referenceOnly": True},
}

DIRECTION_FROZEN = (
    "위 direction 은 결과를 보기 전에 고정했다. 결과가 반대로 나오면 방향을 "
    "뒤집지 않고 INVERTED_SIGNAL 로 정직하게 보고한다(§7).")

ELIGIBILITY = {
    "common": ["close > 0", "shares > 0", "marketCap > 0",
               "해당 factor 의 requires 필드가 전부 존재하고 유한"],
    "positiveOnly": ("PBR·PER·BPS·marketCap 은 0 이하면 제외한다. 음수 PER 은 "
                     "'싸다'가 아니라 적자다 — 역수를 취하면 의미가 뒤집힌다."),
    "noImputation": "결측을 채우지 않는다. 그 factor 의 universe 에서 제외한다.",
    "minUniversePerMonth": 100,
    "why": "표본이 너무 적은 달은 분위 자체가 무의미하다.",
}

RANKING = {
    "method": "각 rebalance 월에 eligible 종목만 대상으로 횡단면 순위",
    "tieBreak": "동점이면 ticker 오름차순 (결정적 재현)",
    "weighting": "분위 내 동일가중(equal weight)",
    "weightingWhy": ("시가총액 가중은 SIZE 를 factor 안으로 몰래 넣는다. "
                     "factor discovery 는 동일가중이 정본이다."),
}

QUANTILES = {
    "primary": "decile (10분위)",
    "fallback": "quintile (5분위)",
    "rule": "eligible >= 200 이면 decile, 100~199 면 quintile, 100 미만이면 그 달 제외",
    "top": "가설 방향 기준 최상위 분위", "bottom": "최하위 분위",
}

HORIZONS = {"primary": [12, 36, 60], "secondary": [84],
            "unit": "개월", "metric": "연율 TSR (CAGR)",
            "annualization": "(1+cum)^(12/months) − 1. 누적차이 단순연율화 금지(§8)."}

BENCHMARK = {
    "primary": ("VALID_<FACTOR>_UNIVERSE — 그 factor 를 계산할 수 있는 종목 "
                "전체의 동일가중 TSR"),
    "why": ("factor 를 계산할 수 없는 종목이 benchmark 에만 들어가면 "
            "selection-universe distortion 이 생긴다(§9)."),
    "secondary": "FULL_MARKET_UNIVERSE — 그 달 전체 상장 종목 동일가중",
}

TRANSACTION = {
    "cost": 0.0, "slippage": 0.0, "tax": 0.0,
    "why": ("R25 는 factor discovery 다. 비용 최적화는 §21 로 금지되어 있고, "
            "비용 0 은 **총수익 기준 비교**임을 명시하기 위한 것이지 실현 가능 "
            "수익 주장이 아니다."),
    "rebalance": "월별 코호트 형성. 포트폴리오 회전 최적화 금지(§21).",
}

SUBPERIODS = [
    {"name": "2007-2011", "start": "2007-01-02", "end": "2011-12-31"},
    {"name": "2012-2016", "start": "2012-01-01", "end": "2016-12-31"},
    {"name": "2017-2021", "start": "2017-01-01", "end": "2021-12-31"},
    {"name": "2022-2026", "start": "2022-01-01", "end": "2026-08-03"},
]
SUBPERIOD_WHY = ("달력 기준 고정. 시장 국면을 결과에 맞춰 자르지 않는다(§12). "
                 "코호트는 **시작월** 기준으로 구간에 배정한다.")

EXCHANGE_GROUPS = ["KOSPI", "KOSDAQ"]
SIZE_GROUPS = {"method": "각 월 eligible 내 marketCap 3분위",
               "names": ["SMALL", "MID", "LARGE"]}

CONTROLS = {
    "raw": "통제 없는 TOP − BOTTOM spread",
    "sizeMatched": ("SMALL/MID/LARGE 각 층 안에서 factor 분위를 다시 나눠 "
                    "spread 를 구하고 층 평균을 낸다"),
    "exchangeMatched": "KOSPI/KOSDAQ 각각에서 spread 를 구해 평균",
    "sizeExchangeMatched": "(거래소 × 크기) 6개 셀 각각에서 spread 를 구해 평균",
    "why": "VALUE alpha 가 실제로는 small-cap premium 인지 분리한다(§10).",
}

ROLLING = {
    "overlapping": "매월 시작하는 모든 코호트",
    "nonOverlapping": "horizon 간격으로 겹치지 않게 뽑은 코호트",
    "outputs": ["median spread", "positive rate", "p10", "p90"],
}

STATS = {
    "method": "moving-block bootstrap",
    "unit": "월별 코호트 spread 시계열",
    "blockMonths": 12,
    "blockWhy": "겹치는 코호트의 자기상관을 블록 단위로 보존한다.",
    "resamples": 2000,
    "seed": 20260822,
    "outputs": ["mean", "ci95Low", "ci95High", "pSpreadPositive"],
    "note": ("단순 평균 spread 만으로 PRIMARY 를 선정하지 않는다(§14). "
             "시드를 고정해 재현 가능하게 한다."),
}

CONCENTRATION = {
    "measure": "TOP 분위 내 종목별 wealth 기여도",
    "outputs": ["top1", "top3", "top5", "top10", "spreadExcludingTop3"],
    "flagRule": "top3 기여도 >= 50% 이고 top3 제거 시 spread 부호가 뒤집히면 "
                "CONCENTRATED_SIGNAL",
}

DISTRESS = {
    "outputs": ["delistingRate", "persistentLossRate", "lowPriceRate",
                "extremePbrRate", "extremePerRate"],
    "lowPrice": "close < 1000원",
    "extremePbr": "PBR < 0.2 또는 PBR > 10",
    "extremePer": "PER < 0 또는 PER > 100",
    "persistentLoss": "직전 36개월 중 EPS <= 0 비율 >= 0.5",
    "why": "저품질주 수익이 높게 보일 때 survivor 만 보고 판단하지 않는다(§16).",
}

TSR_LIMITATION = {
    "source": "r24-full-reconciliation-latest.json 의 미해결 사건",
    "measure": "TOP / BOTTOM 분위의 미해결 자본행위 노출률과 그 차이",
    "flagRule": "|TOP − BOTTOM| >= 3%p 이면 TSR_LIMITATION_EXPOSED",
    "why": "R24 잔여 한계가 특정 분위에 몰리면 factor 결과가 오염된다(§17).",
}

# ── §18 자격 기준 (결과 보기 전 고정) ─────────────────────────────────
QUALIFICATION = {
    "STRONG_SIGNAL": {
        "minAnnualSpreadPct": 3.0,
        "horizonsPositive": 3,
        "horizonsTested": 3,
        "minSubperiodPositiveRatio": 0.5,
        "minRollingPositiveRate": 0.55,
        "sizeControlSameSign": True,
        "exchangeControlSameSign": True,
        "ci95LowAboveZero": True,
        "notConcentrated": True,
        "notTsrLimitationExposed": True,
        "monotonicityMin": 0.5,
    },
    "PROMISING_SIGNAL": {
        "minAnnualSpreadPct": 2.0,
        "horizonsPositive": 2,
        "minSubperiodPositiveRatio": 0.5,
        "minRollingPositiveRate": 0.5,
        "note": "STRONG 조건 일부 미충족이지만 방향과 크기가 의미 있음",
    },
    "WEAK_SIGNAL": {
        "minAnnualSpreadPct": 0.5,
        "note": "방향은 있으나 통제·안정성 근거가 부족",
    },
    "NO_SIGNAL": {"note": "|spread| < 0.5%p 이거나 방향이 일관되지 않음"},
    "INVERTED_SIGNAL": {
        "maxAnnualSpreadPct": -2.0,
        "horizonsNegative": 2,
        "note": "사전 방향과 **반대로** 유의미. 방향을 뒤집지 않고 이렇게 보고한다(§7).",
    },
    "UNRELIABLE": {
        "note": ("표본 부족(유효 코호트 < 24) 또는 TSR_LIMITATION_EXPOSED 또는 "
                 "CONCENTRATED_SIGNAL 로 판단 근거가 오염된 경우"),
        "minCohorts": 24,
    },
    "order": ["UNRELIABLE", "INVERTED_SIGNAL", "STRONG_SIGNAL",
              "PROMISING_SIGNAL", "WEAK_SIGNAL", "NO_SIGNAL"],
    "monotonicity": ("분위 평균 연율 TSR 이 분위 순서와 같은 방향으로 움직이는 "
                     "인접쌍 비율. 1.0 이면 완전 단조."),
}

SELECTION = {
    "PRIMARY_FACTOR": "STRONG_SIGNAL 이고 referenceOnly 가 아닌 factor",
    "SECONDARY_FACTOR": "PROMISING_SIGNAL 이고 referenceOnly 가 아닌 factor",
    "REJECTED_FACTOR": "NO_SIGNAL · WEAK_SIGNAL · UNRELIABLE · INVERTED_SIGNAL",
    "noneAllowed": True,
    "noneRule": "조건을 만족하는 factor 가 없으면 PRIMARY = NONE 이라고 선언한다. "
                "억지로 승자를 만들지 않는다(§19).",
}

FAILURE_CONDITIONS = {
    "ENGINE_MISMATCH": "cum-factor 엔진이 RightsWealth.run 과 불일치하면 중단",
    "PIT_VIOLATION": "미래 스냅샷을 참조하면 중단",
    "BENCHMARK_MISMATCH": "benchmark universe 가 factor universe 와 다르면 중단",
    "NONDETERMINISTIC": "동일 입력 재실행이 다른 결과를 내면 중단",
    "onFailure": ("§31 대로 원인분리 → 최소수정 → targeted regression → R25 "
                  "전체 재실행 → 전체 regression. 결과를 유리하게 만드는 rescue 금지."),
}

FORBIDDEN = [
    "새 조합 탐색(BM+SIZE, BM+dividend 등) — §20",
    "포트폴리오 parameter search (N/P/holding/rebalance/cost) — §21",
    "결과 확인 후 factor 정의·방향·threshold 변경 — §2·§7·§18·§35",
    "R7~R14 결과를 정답으로 사용 — §1",
    "R11 BM_P20_N40_H24 숫자 승계 — §22",
    "close-only return — §3",
    "새 대규모 데이터 수집 — §24",
    "수익보장 표현 — §25",
]


def main() -> int:
    out = {
        "task": "R25", "taskId": TASK,
        "writtenBeforeResults": True,
        "researchQuestion": QUESTION,
        "successDefinition": SUCCESS,
        "isNotRerunOf": ["R7", "R11", "R13", "R14"],
        "legacyStatus": ("R5~R14 는 PRE_TSR_LEGACY_RESEARCH / PROVISIONAL 이다. "
                         "정답으로 사용하지 않는다(§1)."),
        "foundation": {
            "verdict": "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS",
            "from": "R24", "expectedBiasPct": 1.734, "thresholdPct": 2.0,
            "unresolvedTickerPct": 2.20,
            "knownLimitationPct": 1.42,
            "limitationName": "극단 불연속 미조정 사건 비율(사전 완전 PASS 기준 1%)"},
        "universe": UNIVERSE, "pitRule": PIT_RULE, "engine": ENGINE,
        "factorDefinitions": FACTORS, "directionFrozen": DIRECTION_FROZEN,
        "eligibility": ELIGIBILITY, "ranking": RANKING, "quantiles": QUANTILES,
        "horizons": HORIZONS, "benchmark": BENCHMARK,
        "transactionAssumptions": TRANSACTION,
        "subperiods": SUBPERIODS, "subperiodWhy": SUBPERIOD_WHY,
        "exchangeGroups": EXCHANGE_GROUPS, "sizeGroups": SIZE_GROUPS,
        "controls": CONTROLS, "rolling": ROLLING, "statisticalTests": STATS,
        "concentration": CONCENTRATION, "distress": DISTRESS,
        "tsrLimitation": TSR_LIMITATION,
        "qualification": QUALIFICATION, "selection": SELECTION,
        "failureConditions": FAILURE_CONDITIONS,
        "forbidden": FORBIDDEN,
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r25-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (RD / "r25-factor-definitions-latest.json").write_text(
        json.dumps({"task": "R25", "factors": FACTORS,
                    "directionFrozen": DIRECTION_FROZEN,
                    "eligibility": ELIGIBILITY, "ranking": RANKING,
                    "quantiles": QUANTILES}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    print(json.dumps({"factors": len(FACTORS),
                      "horizons": HORIZONS["primary"] + HORIZONS["secondary"],
                      "subperiods": len(SUBPERIODS),
                      "period": UNIVERSE["period"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
