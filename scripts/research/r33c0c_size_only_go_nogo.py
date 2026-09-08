#!/usr/bin/env python3
"""R33C0C — SIZE 단독 prospective OOS GO / NO-GO 결정 harness.

WABABA-SIZE-ONLY-PROSPECTIVE-OOS-GO-NOGO-R33C0C
SOURCE_COMMIT 836b03d (R33C0B · BM_PROSPECTIVE_RETIRED)

이 모듈은 결정만 한다. OOS engine·ledger·runtime·observer 를 만들지 않는다.

핵심 감사(§9): SIZE 의 historical universe 가 BM/PBR 존재에 조건부였는가.
  - R25  r25_analysis.cohort()      : factor-specific  (SIZE 만 있으면 포함)
  - R26  r26_analysis._build_scores(): 공유 universe   ("BM" and "SIZE_SMALL")
  - R27  r27_analysis._shared_universe(): 공유 universe (동일)
  - R32  r32_engine                 : R27.base[d]["BM"] 그대로 사용
  - R33B r33b_historical_validation : R27 을 상속하고 tsr 만 교체 → universe 동일

성과는 계산하지 않는다(§4·§8). PerfGuard 로 fail-closed 차단한다.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache"
PIT = CACHE / "pit-snapshots"
PROSPECT = CACHE / "prospective-research-data"

sys.path.insert(0, str(SRC))

TASK = "WABABA-SIZE-ONLY-PROSPECTIVE-OOS-GO-NOGO-R33C0C"
SOURCE_COMMIT = "836b03d"
CONTRACT_ID = "WABABA_SIZE_ONLY_GO_NOGO_R33C0C_V1"


# ══════════════════════════════════════════════════════════════════════
# §4·§8 성과 접근 fail-closed
# ══════════════════════════════════════════════════════════════════════
class PerformanceAccessViolation(RuntimeError):
    pass


class PerfGuard:
    """R27.tsr / cohort / paired 와 R32 결정엔진을 예외로 바꾼다."""

    TARGETS = (("r27_analysis", "R27", ("tsr", "cohort", "paired")),
               ("r26_analysis", "R26", ("tsr", "cohort", "paired")),
               ("r25_analysis", "R25", ("tsr", "cohort", "cohorts")))

    def __enter__(self):
        self._saved = []
        for mod, cls, names in self.TARGETS:
            try:
                m = __import__(mod)
                k = getattr(m, cls, None)
            except Exception:
                continue
            if k is None:
                continue
            for n in names:
                if hasattr(k, n):
                    self._saved.append((k, n, getattr(k, n)))

                    def boom(*a, _n=n, **kw):
                        raise PerformanceAccessViolation(
                            f"{_n}() 는 R33C0C 에서 금지다(§4).")
                    setattr(k, n, boom)
        return self

    def __exit__(self, *exc):
        for k, n, orig in self._saved:
            setattr(k, n, orig)
        return False


# ══════════════════════════════════════════════════════════════════════
# §10 precommit — 측정 이전에 동결한다
# ══════════════════════════════════════════════════════════════════════
SIZE_CONTRACT = {
    "factorLogicalName": "SIZE_SMALL",
    "factorVersionSource": "R25 precommit (r25_factors.compute)",
    "scoreFormula": "-ln(marketCap)",
    "scoreDirection": "+1 (값이 클수록 = 시총이 작을수록 상위)",
    "marketCapSource": "official direct market-cap field (스냅샷 marketCap = KRX 시가총액)",
    "closeTimesSharesAsCanonical": False,
    "rowPrerequisite": "close>0 · marketCap>0 · shares>0 (r25_factors.compute)",
    "rankingDate": "PIT 월별 결정일 (R25/R26/R27 동일)",
    "liquidityRule": "직전 20거래일 median 직접 거래대금 >= 125,000,000원 (R27 BASE)",
    "liquidityExtra": "tradedOnDecisionDate · observedDays20>=15 · zeroVolumeDays20<=5",
    "topBucketRule": "universe>=200 이면 10분위, 아니면 5분위 · 1분위(top) 채택",
    "tieBreak": "sorted(key=(value, ticker), reverse=True) → ticker DESC (R27.cohort)",
    "weighting": "동일가중",
    "signalRule": "R33A frozen D-close ranking",
    "entryRule": "R33A frozen next-session official open",
    "exitRule": "R33A frozen 36M official open",
    "noFillRule": "R33A frozen rule (미체결은 편입 실패, 비중 재분배 없음)",
    "cashRule": "R33A frozen rule",
    "costRule": "R33B1 corrected actual-filled(체결분) rule",
    "horizonMonths": 36,
}

# 감사 대상 universe 3종 — 반드시 분리해서 판정한다(§9)
UNIVERSE_VARIANTS = {
    "A_SIZE_FACTOR_SPECIFIC": {
        "where": "r25_analysis.R25.cohort()",
        "rule": 'elig = {t: v[factor] for t,v in vals.items() if factor in v}',
        "bmOrPbrRequired": False,
    },
    "B_R32_BM_SIZE_MATCHED_COMMON": {
        "where": "r27_analysis.R27._shared_universe() → r32_engine",
        "rule": 'elig = {t: x for t,x in v.items() if "BM" in x and "SIZE_SMALL" in x}',
        "bmOrPbrRequired": True,
    },
    "C_R33B_TRANSLATED_SIZE": {
        "where": "r33b_historical_validation.make_translated_r27 (R27 상속, tsr 만 교체)",
        "rule": "R27._shared_universe() 그대로 상속",
        "bmOrPbrRequired": True,
    },
}

# §16 — 결과를 보기 전에 동결하는 mandatory gate 26개
MANDATORY_GATES = [
    "G1_size_frozen_definition_recoverable",
    "G2_size_universe_independent_of_bm_pbr",
    "G3_market_data_only_selection_exact_reproduction",
    "G4_official_direct_market_cap_available",
    "G5_official_open_volume_tradevalue_available",
    "G6_current_listing_leakage_zero",
    "G7_future_date_input_zero",
    "G8_existing_official_daily_calendar_producer_active",
    "G9_existing_0740_scheduler_reusable",
    "G10_new_scheduler_required_zero",
    "G11_new_credential_required_zero",
    "G12_new_vendor_required_zero",
    "G13_paid_recurring_data_cost_zero",
    "G14_manual_company_security_mapping_zero",
    "G15_normal_monthly_founder_action_zero",
    "G16_normal_daily_founder_action_zero",
    "G17_preopen_deadline_margin_ge_10min",
    "G18_bm_pbr_dart_dependency_zero",
    "G19_historical_oos_backfill_required_zero",
    "G20_new_db_storage_service_required_zero",
    "G21_rowlevel_remote_upload_required_zero",
    "G22_documented_unresolved_research_question_exists",
    "G23_formal_review_timeline_acknowledged",
    "G24_recurring_operation_fully_automatic",
    "G25_failure_surfaced_through_existing_status_path",
    "G26_thin_size_only_engine_feasible",
]

# §16 장기 horizon 보정 — 하나라도 참이면 자동 NO-GO
AUTO_NOGO_BURDENS = [
    "recurringFounderAction", "recurringPaidData", "monthlyManualMapping",
    "newScheduler", "browserSessionDependence", "manualFileMovement",
    "regularCredentialRefresh", "publicOrRealMoneyBurden", "separateDbOrDashboard",
]

RESEARCH_QUESTION = (
    "공식 직접 시가총액과 실제 next-session-open 실행계약에서 SIZE_SMALL 의 "
    "역사적 초과성과가 순수 전향 monthly cohort 에서도 지속되는가?")

REVIEW_TIME_GATE = {
    "noFormalDecisionBeforeMaturity": True,
    "descriptiveCheckpointsOnly": [12, 24],
    "minMaturedMonthlyCohorts": 36,
    "minNonOverlappingAnchorCohorts": 2,
    "unresolvedDataIntegrityIssuesAllowed": 0,
    "historicalThresholdChangeAllowed": 0,
    "interimResultMayStopOrPromoteFactor": False,
    "note": "성과 threshold 가 아니라 '언제 정식 판단할 수 있는가' 의 time gate 다.",
}

CONTRACT = {
    "contractId": CONTRACT_ID,
    "task": TASK,
    "sourceCommit": SOURCE_COMMIT,
    "sizeContract": SIZE_CONTRACT,
    "universeVariants": UNIVERSE_VARIANTS,
    "mandatoryGates": MANDATORY_GATES,
    "autoNoGoBurdens": AUTO_NOGO_BURDENS,
    "researchQuestion": RESEARCH_QUESTION,
    "reviewTimeGate": REVIEW_TIME_GATE,
    "decisionEnum": ["SIZE_ONLY_OOS_GO", "PROSPECTIVE_OOS_NO_GO"],
    "conditionalGoAllowed": False,
    "moreResearchAllowed": False,
    "historicalPerformanceRecomputation": False,
    "historicalSelectionReproduction": "ALLOWED_WITHOUT_RETURNS",
    "oosImplementationAllowed": False,
    "bmReintroductionAllowed": False,
    "sizeDefinitionChangeAllowed": False,
    "universeTranslationAllowed": False,
    "priorEvidenceUse": "PRIOR_CANONICAL_EVIDENCE_ONLY (계산·최적화 입력 금지)",
}


def canonical_json(o):
    return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def contract_hash():
    return hashlib.sha256(canonical_json(CONTRACT).encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# §9 SIZE 계약·universe 복원 — 실제 소스코드에서 읽는다(추측 금지)
# ══════════════════════════════════════════════════════════════════════
def _src(name):
    return (SRC / name).read_text(encoding="utf-8")


def size_contract_inventory():
    """frozen 소스에서 정의와 universe 규칙을 실측 복원한다."""
    r25f, r25a = _src("r25_factors.py"), _src("r25_analysis.py")
    r26a, r27a = _src("r26_analysis.py"), _src("r27_analysis.py")
    r32e, r33b = _src("r32_engine.py"), _src("r33b_historical_validation.py")
    pit = _src("build_pit_snapshots.py")

    ev = {
        "sizeFormulaInR25Factors":
            'f["SIZE_SMALL"] = -math.log(mcap)' in r25f,
        "bmOnlyWhenPbrPositive":
            'if _pos(pbr):' in r25f and 'f["BM"] = 1.0 / pbr' in r25f,
        "r25FactorSpecificUniverse":
            "if factor in v}" in r25a,
        "r26SharedUniverse":
            'if "BM" in x and "SIZE_SMALL" in x' in r26a,
        "r27SharedUniverse":
            'if "BM" in x and "SIZE_SMALL" in x' in r27a,
        "r27CohortUsesBmKeyedSet":
            'uni = set(b["BM"])' in r27a,
        "r27PairedUsesBmKeyedSet":
            'self.tr.tradable_set(d, set(self.base[d]["BM"])' in r27a,
        "r32UsesR27BmKeyedSet":
            'set(self.K.base[d]["BM"]' in r32e or 'set(K.base[d]["BM"]' in r32e,
        "r33bInheritsR27":
            "class R27Translated(R27)" in r33b,
        "r33bOverridesOnlyTsr":
            "def tsr(self, i, j, t):" in r33b
            and "_shared_universe" not in r33b,
        "pitMarketCapIsDirectField":
            '"marketCap": r.get("시가총액")' in pit,
        "pitMarketCapIsCloseTimesShares":
            "close * shares" in pit or "close*shares" in pit,
    }

    # universe 분류(§9)
    factor_specific_exists = ev["r25FactorSpecificUniverse"]
    prospective_chain_bm_conditional = (
        ev["r26SharedUniverse"] and ev["r27SharedUniverse"]
        and ev["r27CohortUsesBmKeyedSet"] and ev["r32UsesR27BmKeyedSet"]
        and ev["r33bInheritsR27"])

    if factor_specific_exists and prospective_chain_bm_conditional:
        case = "CASE_3"
        why = ("R25 에 SIZE factor-specific universe 가 존재하지만, prospective "
               "계약을 실제로 짊어진 사슬(R26→R27→R31→R32→R33A→R33B→R33B1)은 "
               "전부 BM/PBR 조건부 공유 universe 위에 있다. 어느 쪽이 prospective "
               "정본인지 frozen 계약이 결정하지 않는다.")
    elif prospective_chain_bm_conditional:
        case = "CASE_2"
        why = "SIZE 결과가 BM/PBR 가용성을 요구하는 matched universe 에만 존재한다."
    elif factor_specific_exists:
        case = "CASE_1"
        why = "SIZE factor-specific universe 가 BM/PBR 과 무관하게 존재한다."
    else:
        case = "UNDETERMINED"
        why = "universe 규칙을 복원하지 못했다."

    return {
        "task": TASK, "contractHash": contract_hash(),
        "sizeContract": SIZE_CONTRACT,
        "evidence": ev,
        "universeVariants": UNIVERSE_VARIANTS,
        "case": case, "caseWhy": why,
        "sizeUniverseIndependentOfBmPbr": case == "CASE_1",
        "performanceFunctionCalls": 0,
    }


def universe_dependency(sample_dates=None, max_dates=24):
    """PBR 요구가 universe 를 얼마나 바꾸는지 '개수만' 실측한다.

    수익률을 읽지 않는다. 새 universe 를 만들지도 않는다 — 의존성의 크기만 잰다.
    """
    files = sorted(PIT.glob("*.csv.gz"))
    if sample_dates is None:
        files = files[-max_dates:]
    else:
        want = set(sample_dates)
        files = [f for f in files if f.name.split(".")[0] in want]
    rows = []
    for fp in files:
        d = fp.name.split(".")[0]
        with gzip.open(fp, "rt", encoding="utf-8") as fh:
            rr = list(csv.DictReader(fh))

        def pos(x):
            try:
                return float(x) > 0
            except (TypeError, ValueError):
                return False

        size_ok = [r for r in rr
                   if pos(r.get("close")) and pos(r.get("marketCap"))
                   and pos(r.get("shares"))]
        both_ok = [r for r in size_ok if pos(r.get("PBR"))]
        n_s, n_b = len(size_ok), len(both_ok)
        rows.append({
            "date": d, "sizeFactorSpecific": n_s, "bmAndSize": n_b,
            "droppedByPbrRequirement": n_s - n_b,
            "retainedPct": round(100.0 * n_b / n_s, 2) if n_s else None,
        })
    drops = [r["droppedByPbrRequirement"] for r in rows]
    keeps = [r["retainedPct"] for r in rows if r["retainedPct"] is not None]
    return {
        "task": TASK, "contractHash": contract_hash(),
        "datesMeasured": len(rows), "months": rows,
        "droppedMin": min(drops) if drops else None,
        "droppedMax": max(drops) if drops else None,
        "retainedPctMin": min(keeps) if keeps else None,
        "retainedPctMax": max(keeps) if keeps else None,
        "note": "개수만 측정. 수익률·selection 재현·universe 번역 없음.",
        "performanceFunctionCalls": 0,
    }


# ══════════════════════════════════════════════════════════════════════
# §12 공식 prospective source 감사 (읽기 전용)
# ══════════════════════════════════════════════════════════════════════
def _jload(p):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return None


def source_operability():
    """SIZE prospective 에 필요한 공식 field 가 실제로 있는지 실측한다."""
    st = _jload(RD / "r33c0-producer-status-latest.json") or {}
    dailydir = PROSPECT / "official-daily"
    daily = sorted(dailydir.glob("*.csv.gz")) if dailydir.exists() else []
    obs = _jload(PROSPECT / "calendar" / "observed.json") or {}
    fields, rowcount, leak, future = [], 0, 0, 0
    need = ("marketCap", "open", "close", "volume", "tradeValue",
            "market", "ticker", "shares")
    present = {}
    if daily:
        with gzip.open(daily[-1], "rt", encoding="utf-8") as fh:
            rr = list(csv.DictReader(fh))
        rowcount = len(rr)
        fields = sorted(rr[0].keys()) if rr else []
        # producer 는 snake_case 를 쓴다(market_cap · traded_value).
        # 표기 차이로 없다고 오판하지 않도록 정규화해서 대조한다.
        def norm(x):
            return x.lower().replace("_", "")
        low = {norm(f) for f in fields}
        alias = {"marketCap": ("marketcap",), "tradeValue": ("tradedvalue", "tradevalue"),
                 "open": ("open",), "close": ("close",), "volume": ("volume",),
                 "market": ("market",), "ticker": ("ticker",), "shares": ("shares",)}
        for k in need:
            present[k] = any(a in low for a in alias[k])
        today = date.today().isoformat()
        future = sum(1 for f in daily if f.name.split(".")[0] > today)
    return {
        "task": TASK, "contractHash": contract_hash(),
        "producerStatusArtifactPresent": bool(st),
        "officialDailyThrough": st.get("officialDailyThrough"),
        "officialOpenThrough": st.get("officialOpenThrough"),
        "officialLiquidityThrough": st.get("officialLiquidityThrough"),
        "calendarObservedThrough": st.get("calendarObservedThrough"),
        "calendarDuplicateCount": st.get("calendarDuplicateCount"),
        "holidayTableMismatch": (st.get("holidayTableValidation") or {}).get(
            "mismatchCount"),
        "nextScheduledSignalDate": st.get("nextScheduledSignalDate"),
        "nextEntrySession": st.get("nextEntrySession"),
        "officialDailyFileCount": len(daily),
        "latestOfficialDailyFile": daily[-1].name if daily else None,
        "latestFileRowCount": rowcount,
        "sampleRowFields": fields,
        "requiredFieldPresence": present,
        "allRequiredFieldsPresent": bool(present) and all(present.values()),
        "futureDatedFileCount": future,
        "oldSnapshotMutationCount": st.get("oldSnapshotMutationCount"),
        "pitProducerStatus": (st.get("pit") or {}).get("status"),
        "dartApiCalls": 0, "krxApiCalls": 0, "networkCalls": 0,
        "performanceFunctionCalls": 0,
    }


def scheduler_identity():
    """기존 07:40 task 를 이름이 아니라 action chain 으로 확인한다(읽기 전용)."""
    out = {"queried": False, "raw": None}
    try:
        r = subprocess.run(
            ["schtasks", "/Query", "/FO", "LIST", "/V"],
            capture_output=True, text=True, timeout=120,
            encoding="utf-8", errors="replace")
        out["queried"] = r.returncode == 0
        blocks, cur = [], {}
        for line in (r.stdout or "").splitlines():
            if not line.strip():
                if cur:
                    blocks.append(cur)
                    cur = {}
                continue
            if ":" in line:
                k, _, v = line.partition(":")
                cur[k.strip()] = v.strip()
        if cur:
            blocks.append(cur)
        hits = [b for b in blocks
                if "magic" in json.dumps(b, ensure_ascii=False).lower()
                or "wababa" in json.dumps(b, ensure_ascii=False).lower()]
        out["matchCount"] = len(hits)
        out["tasks"] = [{k: b.get(k) for k in
                         ("TaskName", "Status", "Next Run Time", "Last Run Time",
                          "Last Result", "Schedule Type", "Start Time",
                          "Run As User", "Task To Run")}
                        for b in hits][:8]
    except Exception as e:  # noqa: BLE001
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
    return out


# ══════════════════════════════════════════════════════════════════════
# §15 OOS timeline — 날짜만 계산한다(성과 0)
# ══════════════════════════════════════════════════════════════════════
def _add_months(iso, months):
    import calendar as _cal
    y, m, d = (int(x) for x in iso.split("-"))
    t = m - 1 + months
    y2, m2 = y + t // 12, t % 12 + 1
    return date(y2, m2, min(d, _cal.monthrange(y2, m2)[1])).isoformat()


def oos_timeline(first_signal, horizon=36):
    """monthly cadence · 36M horizon 에서 정보가 언제 쌓이는지."""
    sigs = [first_signal]
    for k in range(1, 40):
        sigs.append(_add_months(first_signal, k))
    mat = [_add_months(s, horizon) for s in sigs]
    anchors = [mat[0], _add_months(sigs[0], horizon * 2)]
    return {
        "task": TASK, "contractHash": contract_hash(),
        "cadence": "monthly", "horizonMonths": horizon,
        "firstSignal": sigs[0], "firstMaturity": mat[0],
        "maturity12th": mat[11], "maturity24th": mat[23], "maturity36th": mat[35],
        "secondNonOverlappingAnchorMaturity": anchors[1],
        "earliestFormalReview": max(mat[35], anchors[1]),
        "reviewTimeGate": REVIEW_TIME_GATE,
        "performanceFunctionCalls": 0,
    }


# ══════════════════════════════════════════════════════════════════════
# §18 SIZE-only 구조 단순화 감사
# ══════════════════════════════════════════════════════════════════════
def architecture_audit():
    removed = ["BM financial source", "DART producer", "PBR/BPS mapping",
               "BM ledger", "paired BM/SIZE completion", "combined pair state",
               "BM holdings hash", "BM no-fill/cash", "BM turnover",
               "factor pair incomplete state"]
    retained = ["common contract", "SIZE snapshot", "SIZE holdings",
                "entry evidence", "no-fill/cash", "filled-book turnover",
                "actual-filled-notional cost", "36M maturity",
                "append-only events", "source/holdings/event hashes",
                "missed run", "data-integrity blocked", "aggregate status",
                "first signal observer"]
    return {
        "task": TASK, "contractHash": contract_hash(),
        "originalR33cComponents": len(removed) + len(retained),
        "removedComponents": removed, "removedCount": len(removed),
        "retainedComponents": retained, "retainedCount": len(retained),
        "estimatedTrackedFiles": 2, "estimatedLocalLedgerRoots": 1,
        "newSchedulerCount": 0, "externalSourceCount": 1,
        "thinEngineFeasible": len(retained) <= 16,
        "performanceFunctionCalls": 0,
    }


# ══════════════════════════════════════════════════════════════════════
# §19 결정 엔진 — 동결된 mandatory gate 만 사용. 출력은 enum 2개 중 하나.
# ══════════════════════════════════════════════════════════════════════
DECISION_ENUM = ["SIZE_ONLY_OOS_GO", "PROSPECTIVE_OOS_NO_GO"]


def decide(gates, burdens=None):
    """mandatory gate 전부 PASS + 자동 NO-GO 부담 0 일 때만 GO."""
    unknown = [g for g in MANDATORY_GATES if g not in gates]
    failed = [g for g in MANDATORY_GATES if gates.get(g) is not True]
    burdens = burdens or {}
    hit = [b for b in AUTO_NOGO_BURDENS if burdens.get(b) is True]
    codes = []
    if unknown:
        codes += [f"GATE_NOT_MEASURED::{g}" for g in unknown]
    codes += [f"GATE_FAILED::{g}" for g in failed if g not in unknown]
    codes += [f"AUTO_NOGO_BURDEN::{b}" for b in hit]
    if not failed and not hit:
        return {"decision": "SIZE_ONLY_OOS_GO", "gateMatrix": gates,
                "failedMandatoryGates": [], "passedMandatoryGates": MANDATORY_GATES,
                "autoNoGoBurdensHit": [],
                "decisionReasonCodes": ["ALL_MANDATORY_GATES_PASS",
                                        "ZERO_MARGINAL_OPERATING_BURDEN"]}
    return {"decision": "PROSPECTIVE_OOS_NO_GO", "gateMatrix": gates,
            "failedMandatoryGates": failed,
            "passedMandatoryGates": [g for g in MANDATORY_GATES if g not in failed],
            "autoNoGoBurdensHit": hit, "decisionReasonCodes": codes}


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r33c0c-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    return p


if __name__ == "__main__":
    print("contract hash:", contract_hash())
