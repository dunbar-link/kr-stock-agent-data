#!/usr/bin/env python3
"""R33A 실행번역 계약 — 결과를 보기 전에 동결한다.

WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A
SOURCE_R33: WABABA-BM-SIZE-SEPARATE-OOS-PAPER-OBSERVATION-CONTRACT-R33 (04beb59)
RESEARCH_BASE: WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A (0431003)

R33 이 163/163 cohort 전수로 확인한 문제 하나를 제거하기 위한 **시점 번역**이다.

    marketCap(D) == close(D) × shares(D)      → SIZE_SMALL 은 close(D) 의 순함수
    PBR(D)      == close(D) / BPS(D)          → BM 은 close(D) 의 순함수
    entry price == close(D)                   → 랭킹 입력 == 체결가 (same-bar)

번역 규칙은 **하나만** 사전선택한다(§2). 후보를 성과로 비교하지 않는다.

    D_CLOSE_RANK_NEXT_SESSION_OPEN_V1
    D 종가로 랭킹 확정 → D 다음 공식 거래일의 공식 시가로 paper entry

── 이 파일이 하지 않는 것 ─────────────────────────────────────────
  수익률·초과수익·CAGR·비용조정수익·승자 판정을 **계산하지 않는다**.
  R32 decision engine 을 호출하지 않는다. 대안 entry rule 을 비교하지 않는다.
  factor 정의·universe·유동성 threshold·비용률을 바꾸지 않는다.

── 비용·비중·현금은 R32 의 실제 의미를 복원한다(§15) ──────────────
  새 비용률을 만들지 않는다. R32 코드가 실제로 무엇을 하는지 읽어서 적는다.

안전: 선언·읽기 전용. 네트워크 0 · API 0 · 인증키 접근 0 · 성과계산 0.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SRC = ROOT / "scripts" / "research"

TASK_ID = "WABABA-PROSPECTIVE-EXECUTION-TRANSLATION-PRECOMMIT-R33A"
CONTRACT_ID = "WABABA_EXECUTION_TRANSLATION_R33A_V1"
TRANSLATION_RULE_ID = "D_CLOSE_RANK_NEXT_SESSION_OPEN_V1"

SOURCE_R33_COMMIT = "04beb59"
RESEARCH_BASE_COMMIT = "0431003"

FROZEN_PRECOMMITS = {"r27_precommit": "acee4288", "r31_precommit": "d7fe3929",
                     "r32_precommit": "07edbe72"}

# ══════════════════════════════════════════════════════════════════════
# 계약 본문. 결과를 본 뒤 수정하지 않는다.
# ══════════════════════════════════════════════════════════════════════
CONTRACT = {
    "contractId": CONTRACT_ID,
    "taskId": TASK_ID,
    "sourceR33Commit": SOURCE_R33_COMMIT,
    "researchBaseCommit": RESEARCH_BASE_COMMIT,
    "translationRuleId": TRANSLATION_RULE_ID,

    # ── 무엇을 바꾸고 무엇을 안 바꾸는가 ─────────────────────────────
    "scope": {
        "purpose": ("same-bar execution look-ahead 제거만을 위한 시점 번역. "
                    "factor 연구조건은 건드리지 않는다."),
        "factorDefinitionsChanged": False,
        "universeChanged": False,
        "liquidityThresholdChanged": False,
        "costRatesChanged": False,
        "horizonMethodChanged": True,
        "horizonMethodChangeScope": (
            "36M 의 기준점이 결정일 D 에서 translated entry date 로 이동한다. "
            "36 이라는 길이는 바뀌지 않는다."),
        "performanceUsedToSelectRule": False,
        "historicalReturnsCalculatedBeforeFreeze": False,
        "alternativeEntryRulesTested": 0,
    },

    # ── factor ──────────────────────────────────────────────────────
    "factors": {
        "set": ["BM", "SIZE_SMALL"],
        "BM": {"formula": "1 / PBR", "direction": +1,
               "source": "R25 정의 그대로 (r25_factors.compute)"},
        "SIZE_SMALL": {"formula": "-ln(marketCap)", "direction": +1,
                       "source": "R25 정의 그대로 (r25_factors.compute)"},
        "combinationAllowed": False,
        "separateBooks": True,
        "identicalTimingBothFactors": True,
        "identicalTimingWhy": ("BM 과 SIZE 는 같은 D 스냅샷 한 장에서 나온다. "
                               "따라서 freeze 시각이 구조적으로 동일하다."),
    },

    # ── signal / ranking ────────────────────────────────────────────
    "signal": {
        "signalDateRule": "MONTH_FIRST_TRADING_DAY (기존 canonical, 변경 없음)",
        "signalDate": "D",
        "factorPriceDate": "D",
        "factorPriceField": "official close",
        "financialAsOf": "기존 BM PIT 계약 (KRX 공표 PBR/BPS, D 스냅샷)",
        "sharesAsOf": "기존 SIZE PIT 계약 (D 스냅샷 상장주식수)",
        "liquidityAsOf": ("기존 R27 trailing 계약 — D **직전** 20거래일만. "
                          "D 당일·이후 미사용."),
        "rankingFreezeEvent": (
            "D 장마감 후 canonical snapshot 의 official_write 성공 이후"),
        "rankingFreezeDeadline": (
            "translated entry session 의 공식 시가 단일가매매 종료(09:00 KST) 이전"),
        "rankingFreezeDeadlineBasis": (
            "KRX 정규장 09:00~15:30 · 시가 단일가 08:30~09:00 · "
            "종가 단일가 15:20~15:30"),
        "holdingsImmutableBeforeEntryPriceExists": True,
    },

    # ── entry ───────────────────────────────────────────────────────
    "entry": {
        "targetSessionRule": "D 다음의 첫 번째 공식 시장 거래일",
        "priceField": "official opening price",
        "priceSource": ("R31 stitched official daily — KRX TDD_OPNPRC (2010~2019) / "
                        "공공데이터포털 FSC mkp·시가 (2020~)"),
        "executionModel": "PAPER_MARKET_ON_OPEN",
        "entryPriceKnownWhenRankingFrozen": False,
        "sameBarExecution": False,
        "nonFillRule": "NO_VALID_OFFICIAL_OPEN_MEANS_NO_FILL",
        "validOpenDefinition": "official open > 0 AND 당일 volume > 0",
        "nonFillCauses": ["거래정지", "당일 무거래(volume == 0, open == 0)"],
        "retryRule": "NO_LATER_ENTRY — 다음 날로 미루지 않는다",
        "failedEntryCapital": "RETAIN_AS_CASH",
        "failedEntryWeightRedistribution": False,
        "dataMissingIsNotNonFill": (
            "데이터가 없는 것과 거래가 없는 것을 구분한다. 데이터 누락은 "
            "DATA_INTEGRITY_BLOCKED 이고 no-fill 로 위장하지 않는다."),
    },

    # ── 종목선택 · 비중 ─────────────────────────────────────────────
    "selection": {
        "rule": "기존 R27.cohort() 그대로",
        "quantile": ("eligible >= 200 이면 decile(10분위), 아니면 quintile(5분위). "
                     "고정 top-N 이 아니다."),
        "bucket": "최상위 분위 전체",
        "weighting": "동일가중 (R25/R26/R27/R32 동일)",
        "weightRenormalizationAfterNonFill": False,
        "tieBreak": {
            "selectionPath": (
                "r27_analysis.cohort(): sorted(key=(value, ticker), reverse=True) "
                "→ value DESC, 동점 시 ticker **DESC**"),
            "turnoverPath": (
                "r32_engine.turnover_of(): sorted(key=(-value, ticker)) "
                "→ value DESC, 동점 시 ticker **ASC**"),
            "knownInconsistency": True,
            "note": ("두 경로의 동점 방향이 반대다. 둘 다 결정적이며 R32 동결본의 "
                     "실제 상태다. 이번 TASK 에서 고치지 않는다(§4·§18 — tie-break "
                     "변경 금지, R25~R32 계산코드 변경 금지). R33B 가 이 상태를 "
                     "그대로 승계한다."),
            "changedHere": False,
        },
    },

    # ── 유동성 (변경 없음) ──────────────────────────────────────────
    "liquidity": {
        "primaryRule": "20D median direct trade value >= 125,000,000 KRW",
        "lowRule": "25,000,000 KRW",
        "highRule": "250,000,000 KRW",
        "window": "결정일 D 직전 20거래일 (D 포함 안 함)",
        "measureSource": "KRX 공식 거래대금 컬럼 (close × volume 추정 아님)",
        "gateConditions": ["tradedOnLastDay", "observedDays20 >= 15",
                           "zeroVolumeDays20 <= 5",
                           "median20TradedValue >= threshold"],
        "changed": False,
    },

    # ── horizon / exit ──────────────────────────────────────────────
    "exit": {
        "nominalHoldingHorizon": "36 calendar months from entry target date",
        "horizonAnchorChange": (
            "R32 는 결정일 D 기준 36 스냅샷이었다. 번역 후에는 translated entry "
            "date 기준 36 calendar months 다."),
        "exitTargetDateRule": "entry target date + 36 calendar months",
        "exitTargetSessionRule": (
            "nominal exit target 당일 또는 그 이후의 첫 공식 거래일"),
        "priceField": "official opening price",
        "executionModel": "PAPER_MARKET_ON_OPEN",
        "noValidOpenRule": (
            "EXIT_PENDING_UNTIL_FIRST_SUBSEQUENT_VALID_OFFICIAL_OPEN"),
        "priceSubstitution": "FORBIDDEN — 더 최신 가격이나 종가로 대체하지 않는다",
        "delistingRule": "기존 R32A canonical terminal handling (UNKNOWN_RECOVERY)",
        "corporateActionRule": "기존 R32A canonical handling (R16/R17/R24 override)",
        "suspensionRule": ("entry 는 no-fill/cash, exit 는 pending 또는 canonical "
                           "terminal event"),
    },

    # ── 비용 — R32 실제 의미 복원(§15). 새 비용률 0. ────────────────
    "cost": {
        "restoredFrom": "r32_engine.cost_curve() + r27_run + r27_precommit.COST",
        "model": "TURNOVER_BASED_ANNUALIZED_DRAG",
        "formula": "costDragPct = annualTurnover × bps / 100",
        "appliedTo": ("연율화 gross 성과에서 차감 (netPct = grossPct − costDragPct). "
                      "체결 건별 notional 과금 모델이 아니다."),
        "bpsMeaning": "round-trip (r27_precommit.COST[*].roundTripBps)",
        "annualTurnoverDefinition": (
            "monthlyTurnover × 12, monthlyTurnover = 1 − |cur ∩ prev| / |prev|"),
        "stressScenarios": [0, 25, 50, 100],
        "primaryCost": ("R27/R32 정본에 단일 primary cost 가 없다. base 0bp 와 "
                        "high 100bp 가정이 병기돼 있을 뿐이다. 이번 TASK 에서 새 "
                        "primary cost 를 고르지 않는다 — R33B 는 4개 stress 를 "
                        "모두 보고하고 기존 R32 tie policy 를 그대로 쓴다."),
        "turnoverComputedOn": (
            "실제 체결된 holdings 계열. no-fill 로 편입되지 않은 종목은 보유가 "
            "아니므로 turnover 에 보유로 잡히지 않는다."),
        "noFillCost": 0,
        "cashCost": 0,
        "partialFillModel": "없음 (R32 에 없다. 새로 만들지 않는다)",
        "ratesChanged": False,
    },

    # ── 현금 ────────────────────────────────────────────────────────
    "cash": {
        "returnRule": "0% nominal",
        "why": ("R32 는 코호트 방식이라 현금 모델이 아예 없다"
                "(r32_engine: cashDrag = 'NOT_MODELLED_COHORT_METHOD'). "
                "§15 지시대로 정본이 없으므로 0% nominal 로 사전고정하고 "
                "그 사실을 기록한다. 0 으로 측정된 것이 아니라 고정한 것이다."),
        "measuredNotAssumed": False,
        "appliesTo": "no-fill 로 생긴 미체결 슬롯",
    },

    # ── 금지 ────────────────────────────────────────────────────────
    "prohibitions": {
        "currentListingFilter": "FORBIDDEN",
        "survivorshipFilter": "FORBIDDEN",
        "partialCohortWinnerDecision": "FORBIDDEN",
        "interimPerformanceDecision": "FORBIDDEN",
        "factorCombination": "FORBIDDEN",
        "alternativeEntryRuleComparison": "FORBIDDEN",
        "toleranceWidening": "FORBIDDEN",
    },

    # ── 상태 ────────────────────────────────────────────────────────
    "state": {
        "realMoneyApproved": False,
        "paperOnly": True,
        "publicationAllowed": False,
        "runtimeActivationAllowed": False,
        "oosLedgerCreationAllowed": False,
        "contractNextStage": "HISTORICAL_TRANSLATION_VALIDATION_R33B",
    },

    # ── 기존 연구 상태 범위 정정 (§17, append-only) ─────────────────
    "priorScopeAddendum": {
        "R27": {"liquidityDataAndThresholdValidation": "PASS",
                "prospectiveEntryTiming": "NOT_EVALUATED_AT_R27"},
        "R31": {"officialDataBackfillAndLiquidityGate": "PASS",
                "prospectiveEntryTiming": "NOT_EVALUATED_AT_R31"},
        "R32": {"historicalClosedBarDecision": "BOTH_SEPARATE",
                "prospectiveTranslatedDecision": "PENDING_R33B"},
        "R32A": {"frozenBoundaryAndRowLevelReproduction": "PASS",
                 "sameBarExecutionFeasibility":
                     "OUT_OF_SCOPE_THEN, IDENTIFIED_BY_R33"},
        "R33": {"contractStatus": "BLOCKED_PENDING_R33A_AND_R33B",
                "runtimeStatus": "NOT_ARMED",
                "ledgerStatus": "NOT_CREATED"},
        "sizeGradeScope":
            "LIQUIDITY_EXECUTABLE_STRONG_UNDER_HISTORICAL_CLOSE_ASSUMPTION",
        "bmGradeScope":
            "LIQUIDITY_EXECUTABLE_PROMISING_UNDER_HISTORICAL_CLOSE_ASSUMPTION",
        "forbiddenClaimsUntilR33B": [
            "prospective execution passed", "live executable",
            "real-money ready", "deployment ready", "OOS active",
            "automatically tradable"],
        "appendOnly": True,
    },
}


# ══════════════════════════════════════════════════════════════════════
# 직렬화 · 해시
# ══════════════════════════════════════════════════════════════════════
def canonical_json(obj=None) -> str:
    """결정적 직렬화. 키 정렬 + 고정 구분자 + UTF-8 그대로."""
    return json.dumps(obj if obj is not None else CONTRACT,
                      ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def contract_hash(obj=None) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def source_hash() -> str:
    return hashlib.sha256(Path(__file__).resolve().read_bytes()).hexdigest()


def frozen_precommit_hashes() -> dict:
    out = {}
    for name, prefix in FROZEN_PRECOMMITS.items():
        h = hashlib.sha256((SRC / f"{name}.py").read_bytes()).hexdigest()
        out[name] = {"sha256": h, "expectedPrefix": prefix,
                     "match": h.startswith(prefix)}
    out["allMatch"] = all(v["match"] for v in out.values() if isinstance(v, dict))
    return out


def build(generated_at: str) -> dict:
    """계약 + 해시. 성과값은 어디에도 들어가지 않는다."""
    return {
        "task": "R33A", "taskId": TASK_ID,
        "contractId": CONTRACT_ID,
        "translationRuleId": TRANSLATION_RULE_ID,
        "writtenBeforeResults": True,
        "generatedAt": generated_at,
        "contract": CONTRACT,
        "contractHash": contract_hash(),
        "contractSerialization": "json sort_keys=True separators=(',',':') utf-8",
        "sourceFileSha256": source_hash(),
        "frozenPrecommits": frozen_precommit_hashes(),
        "performanceAccess": {
            "performanceFunctionCalls": 0,
            "forwardReturnRowsRead": 0,
            "r32DecisionEngineCalls": 0,
            "alternativeEntryPerformanceRuns": 0,
            "entryRuleOptimizationRuns": 0,
            "publishedResultUsedAsInput": False,
            "historicalReturnsCalculated": 0,
        },
        "safety": {"networkCalls": 0, "marketDataApiCalls": 0,
                   "krxAuthKeyAccess": 0, "envChanges": 0,
                   "oosCohortCreated": 0, "runtimeHookCreated": 0,
                   "observerRegistered": 0, "newSchedulerCount": 0,
                   "newDaemonCount": 0, "publicRepoChanges": 0,
                   "legacy50dChanges": 0, "realOrders": 0, "brokerCalls": 0,
                   "realMoneyApproved": False, "paperOnly": True},
    }


def main(argv=None) -> int:
    import datetime
    import zoneinfo
    now = datetime.datetime.now(
        zoneinfo.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    out = build(now)
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r33a-translation-precommit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "contractId": out["contractId"],
        "translationRuleId": out["translationRuleId"],
        "contractHash": out["contractHash"],
        "sourceFileSha256": out["sourceFileSha256"],
        "frozenAllMatch": out["frozenPrecommits"]["allMatch"],
        "generatedAt": out["generatedAt"],
        "performanceAccess": out["performanceAccess"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
