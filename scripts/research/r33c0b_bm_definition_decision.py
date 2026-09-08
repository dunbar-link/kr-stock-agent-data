#!/usr/bin/env python3
"""R33C0B — prospective BM 정의를 동결하거나 종료한다. 중간값 없음.

WABABA-BM-PROSPECTIVE-DEFINITION-DECISION-PRECOMMIT-R33C0B
SOURCE: WABABA-PIT-PBR-BPS-CANONICAL-SOURCE-RESOLUTION-AND-RESTORE-R33C0A (4cba54d)

R33C0A 가 확정한 것: 기존 BM(=1/KRX 공표 PBR)은 공식·지속 가능한 prospective
source 가 없다. pykrx 는 웹 세션 경로라 거부됐고, 공식 KRX OPEN API 에는 PBR/BPS 가
없으며, OpenDART 로는 KRX 공표 BPS 를 재현하지 못한다(1% 이내 43.7%).

그래서 이 TASK 는 "기존 BM 을 흉내내는 정의"를 찾지 않는다.
공식 재무제표로 **명시적 book-to-market** 을 하나 정의할 수 있는지만 본다.

결과는 정확히 둘 중 하나다.
    A. BM_DART_V2_DEFINITION_FROZEN
    B. BM_PROSPECTIVE_RETIRED

"일단 만들어 보자" · "후보 여러 개" · "provisional" 은 허용하지 않는다.
첫 후보가 gate 를 통과하지 못하면 다른 numerator/denominator 로 갈아타지 않는다.

── 수익률을 보지 않는다 ────────────────────────────────────────────
  성과 경로를 fail-closed 로 차단한 상태에서 회계 명확성·coverage·운영가능성만 본다.
  old BM overlap 도 acceptance 기준으로 쓰지 않는다(§8).

안전: 읽기·계산 전용. 성과계산 0 · 인증키 값 미출력 · R33C 미구현.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SNAP = ROOT / "_cache" / "pit-snapshots"
DART_ANNUAL = ROOT / "_cache" / "dart-statements"
DART_QUARTER = ROOT / "_cache" / "dart-statements-quarterly"
CORP_CODES = ROOT / "_cache" / "dart-corp-codes.json"

TASK_ID = "WABABA-BM-PROSPECTIVE-DEFINITION-DECISION-PRECOMMIT-R33C0B"
CONTRACT_ID = "WABABA_BM_DART_V2_DEFINITION_R33C0B_V1"
SOURCE_COMMIT = "4cba54d"


# ══════════════════════════════════════════════════════════════════════
# §7 성과 접근 차단 (fail-closed)
# ══════════════════════════════════════════════════════════════════════
class PerformanceAccessViolation(RuntimeError):
    """수익률 경로가 호출되면 즉시 실패시킨다."""


class PerfGuard:
    def __init__(self):
        self.calls = 0
        self._saved = []

    def __enter__(self):
        import r16_canonical as C
        import r25_engine as E
        import r27_analysis as A

        def block(label):
            def _f(*a, **k):
                self.calls += 1
                raise PerformanceAccessViolation(
                    f"성과 경로 호출됨: {label} — R33C0B 는 수익률을 보지 않는다")
            return _f

        for owner, attr, label in (
                (E.R25Engine, "tsr", "R25Engine.tsr"),
                (E.R25Engine, "cagr", "R25Engine.cagr"),
                (C.CanonicalWealth, "tsr", "CanonicalWealth.tsr"),
                (A.R27, "tsr", "R27.tsr"),
                (A.R27, "cohort", "R27.cohort"),
                (A.R27, "paired", "R27.paired"),
        ):
            if hasattr(owner, attr):
                self._saved.append((owner, attr, getattr(owner, attr)))
                setattr(owner, attr, block(label))
        return self

    def __exit__(self, *exc):
        for owner, attr, orig in self._saved:
            setattr(owner, attr, orig)
        return False


# ══════════════════════════════════════════════════════════════════════
# §9·§10 후보 선언 — 정확히 하나. coverage 계산 전에 고정한다.
# ══════════════════════════════════════════════════════════════════════
CANDIDATE = {
    "name": "BM_DART_V2",
    "equivalentToHistoricalBM": False,
    "isNewFactorVersion": True,
    "concept": ("issuer book equity (공식 재무제표) ÷ 그 issuer 대표 보통주의 "
                "공식 시장가치. KRX 공표 PBR 의 복제본이 아니다."),

    # numerator
    "numeratorAccountId": "ifrs-full_EquityAttributableToOwnersOfParent",
    "numeratorAccountName": "지배기업 소유주지분",
    "numeratorStatement": "BS (sj_div)",
    "fsDivPriority": ["CFS", "OFS"],
    "ofsFallbackRule": ("연결재무제표가 실제로 존재하지 않는 issuer 에 한해서만 "
                        "OFS 자본총계 사용. 같은 issuer 에서 계정 누락을 이유로 "
                        "조용히 OFS 로 내려가지 않는다."),
    "manualAccountOverride": "FORBIDDEN",

    # report / PIT
    "reportCodePriority": ["11011", "11014", "11012", "11013"],
    "reportCodePriorityNote": "사업보고서 > 3분기 > 반기 > 1분기 (최신 접수 우선)",
    "disclosureCutoff": "rcept_dt <= D-1",
    "disclosureCutoffWhy": ("OpenDART 는 접수 '시각'이 아니라 날짜만 준다. D 당일 "
                            "공시가 D 종가 이전에 공개됐다는 근거가 없으므로 "
                            "보수적으로 D-1 로 고정한다(§10)."),
    "restatementPolicy": ("cutoff 시점에 이용 가능한 최신 correction 만 사용. "
                          "이후 정정공시를 과거 signal 에 소급 적용하지 않는다."),
    "currency": "KRW only. 그 외 통화는 결측 처리(환산하지 않는다).",

    # denominator / security
    "denominator": "공식 일별 source 의 대표 보통주 market_cap (direct field)",
    "denominatorRecomputeRule": "close × shares 는 검증용. 정본 대체 금지.",
    "representativeSecurityPolicy": (
        "보통주만 사용한다 — 종목코드 끝자리 0. 기존 저장소 정본 규칙 "
        "(backtest_engine.py: '우선주는 본주와 중복이라 제외')과 동일하며, "
        "BM_V1 도 우선주에 PBR 이 없어 실질적으로 같은 집합이었다."),
    "multiClassPolicy": (
        "우선주·기타 클래스는 universe 에서 제외하고 분모에도 더하지 않는다. "
        "DART corp-code 에 우선주 stock_code 가 존재하지 않아 정본 issuer 매핑이 "
        "없기 때문이다. 5자리 휴리스틱 매핑은 만들지 않는다(§10 임의규칙 금지)."),
    "multiClassKnownBias": (
        "우선주를 발행한 issuer 는 분자(전체 클래스 장부가치) 대비 분모(보통주 "
        "시가총액)가 작아 B/M 이 과대평가된다. 이 편향은 측정·공개한다."),
    "manualSecurityOverride": "FORBIDDEN",

    # value policies
    "negativeBookPolicy": "book equity <= 0 이면 결측 — 랭킹에서 제외(대체값 금지)",
    "zeroBookPolicy": "위와 동일",
    "missingPolicy": ("결측을 채우지 않는다. 그 signal 의 BM universe 에서 제외한다 "
                      "(R25 noImputation 계승)."),

    # ranking / timing
    "formula": "BM_DART_V2 = bookEquity(issuer, cutoff<=D-1) / marketCap(대표보통주, D)",
    "rankingDirection": "higher book-to-market is better",
    "tieBreak": "ticker deterministic (R32 frozen selection path 계승)",
    "signalTiming": "D 재무 cutoff + D 공식 시가총액 → D+1 pre-open ranking freeze",
    "entryRule": "R33A frozen D_CLOSE_RANK_NEXT_SESSION_OPEN_V1 (D+1 공식 시가)",
    "liquidityRule": "R27 frozen BASE 125,000,000 KRW",
}

# §12 coverage gate — coverage 계산 **전에** 고정한다.
#   기존 R27 정본 minCoveragePctPerYear 90.0 과 정합.
COVERAGE_GATE = {
    "source": "R27 COVERAGE.minCoveragePctPerYear=90.0 정합 + R33C0B §12 fallback",
    "monthlySignalWindow": 12,
    "minSecurityCountCoverageEachMonth": 90.0,
    "minIssuerMarketCapCoverageEachMonth": 95.0,
    "minKospiSecurityCoverageEachMonth": 85.0,
    "minKosdaqSecurityCoverageEachMonth": 85.0,
    "unresolvedAccountMappingAllowed": 0,
    "unresolvedIssuerSecurityMappingAllowed": 0,
    "manualCompanyOverrideAllowed": 0,
    "futureFilingUseAllowed": 0,
    "currentListingLeakageAllowed": 0,
    "duplicateIssuerScoreAllowed": 0,
    "sourceHashCoverage": 100.0,
    "allMonthsMustPass": True,
    "averageCannotCoverFailingMonth": True,
}

CONTRACT = {
    "contractId": CONTRACT_ID,
    "taskId": TASK_ID,
    "sourceCommit": SOURCE_COMMIT,
    "historicalFactor": "BM_KRX_PUBLISHED_V1",
    "historicalFactorStatus": "HISTORICAL_ONLY",
    "prospectiveCandidate": "BM_DART_V2",
    "equivalentToHistoricalBM": False,
    "newFactorVersionCount": 1,
    "candidateCount": 1,
    "secondCandidateAllowed": False,
    "performanceUsedToDefine": False,
    "priorNonPerformanceEquivalenceMetricsSeen": True,
    "priorReturnMetricsSeenForCandidateSelection": False,
    "oldBmOverlapUsedAsAcceptance": False,
    "candidate": CANDIDATE,
    "coverageGate": COVERAGE_GATE,
    "historicalReturnValidation": "NOT_RUN",
    "oosActivationAllowed": False,
    "publicationAllowed": False,
    "realMoneyApproved": False,
}


def canonical_json(o):
    return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def contract_hash():
    return hashlib.sha256(canonical_json(CONTRACT).encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# §13 coverage feasibility — precommit hash 이후에만 실행
# ══════════════════════════════════════════════════════════════════════
def _f(x):
    try:
        v = float(str(x).replace(",", ""))
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def corp_map():
    cc = json.loads(CORP_CODES.read_text(encoding="utf-8"))
    return {k: (v.get("corp_code") if isinstance(v, dict) else v)
            for k, v in cc.items()}


def book_equity(corp, years=("2025", "2024")):
    """CANDIDATE 계약대로 지배주주지분만 본다. 조용한 OFS fallback 없음."""
    aid = CANDIDATE["numeratorAccountId"]
    for yr in years:
        fp = DART_ANNUAL / f"{corp}_{yr}_CFS.json"
        if not fp.exists():
            continue
        try:
            rows = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in rows:
            if r.get("sj_div") == "BS" and r.get("account_id") == aid:
                a = _f(r.get("thstrm_amount"))
                if a is not None:
                    return a, {"year": yr, "fsDiv": "CFS",
                               "reportCode": r.get("reprt_code"),
                               "rceptNo": bool(r.get("rcept_no"))}
    return None, None


def month_coverage(signal_date, cm):
    """한 signal 의 coverage. 개별 종목값은 반환하지 않는다."""
    p = SNAP / f"{signal_date}.csv.gz"
    if not p.exists():
        return None
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    # 대표 보통주만 (계약: 끝자리 0)
    ords = [r for r in rows if r["ticker"].endswith("0")]
    elig = [r for r in ords if _f(r.get("marketCap")) and _f(r.get("marketCap")) > 0]
    tot_mc = sum(_f(r.get("marketCap")) or 0 for r in elig)
    ok = ok_mc = 0
    neg = zero = miss_map = miss_stmt = 0
    by_mkt = {"KOSPI": [0, 0], "KOSDAQ": [0, 0]}
    for r in elig:
        t = r["ticker"]
        mk = r.get("market")
        if mk in by_mkt:
            by_mkt[mk][1] += 1
        c = cm.get(t)
        if not c:
            miss_map += 1
            continue
        be, _meta = book_equity(c)
        if be is None:
            miss_stmt += 1
            continue
        if be < 0:
            neg += 1
            continue
        if be == 0:
            zero += 1
            continue
        ok += 1
        ok_mc += _f(r.get("marketCap")) or 0
        if mk in by_mkt:
            by_mkt[mk][0] += 1
    n = len(elig)
    return {
        "signalDate": signal_date,
        "universeAll": len(rows), "ordinaryOnly": len(ords), "eligible": n,
        "validBm": ok,
        "securityCoveragePct": round(100.0 * ok / n, 2) if n else 0.0,
        "marketCapCoveragePct": round(100.0 * ok_mc / tot_mc, 2) if tot_mc else 0.0,
        "kospiCoveragePct": (round(100.0 * by_mkt["KOSPI"][0] / by_mkt["KOSPI"][1], 2)
                             if by_mkt["KOSPI"][1] else None),
        "kosdaqCoveragePct": (round(100.0 * by_mkt["KOSDAQ"][0] / by_mkt["KOSDAQ"][1], 2)
                              if by_mkt["KOSDAQ"][1] else None),
        "missingIssuerMapping": miss_map, "missingStatement": miss_stmt,
        "negativeBook": neg, "zeroBook": zero,
        "excludedNonOrdinary": len(rows) - len(ords),
    }


def coverage_audit(signal_dates):
    cm = corp_map()
    months = [m for m in (month_coverage(d, cm) for d in signal_dates) if m]
    g = COVERAGE_GATE
    fails = []
    for m in months:
        if m["securityCoveragePct"] < g["minSecurityCountCoverageEachMonth"]:
            fails.append({"month": m["signalDate"], "gate": "securityCount",
                          "value": m["securityCoveragePct"],
                          "required": g["minSecurityCountCoverageEachMonth"]})
        if m["marketCapCoveragePct"] < g["minIssuerMarketCapCoverageEachMonth"]:
            fails.append({"month": m["signalDate"], "gate": "marketCap",
                          "value": m["marketCapCoveragePct"],
                          "required": g["minIssuerMarketCapCoverageEachMonth"]})
        for k, req in (("kospiCoveragePct", g["minKospiSecurityCoverageEachMonth"]),
                       ("kosdaqCoveragePct", g["minKosdaqSecurityCoverageEachMonth"])):
            if m[k] is not None and m[k] < req:
                fails.append({"month": m["signalDate"], "gate": k,
                              "value": m[k], "required": req})
    return {"months": months, "monthsAudited": len(months),
            "gateFailures": fails, "gateFailureCount": len(fails),
            "allMonthsPass": len(fails) == 0}


# ══════════════════════════════════════════════════════════════════════
# §14 missingness 편향 — coverage 결측이 특정 집단에 몰리는지만 본다.
#     수익률과 연결하지 않는다(§3). 성과 함수 호출 0.
# ══════════════════════════════════════════════════════════════════════
SIZE_BUCKETS = ["대형", "중대형", "중형", "중소형", "소형"]


def missingness_bias(signal_date):
    cm = corp_map()
    p = SNAP / f"{signal_date}.csv.gz"
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    elig = [r for r in rows
            if r["ticker"].endswith("0") and (_f(r.get("marketCap")) or 0) > 0]
    have = {}
    for r in elig:
        c = cm.get(r["ticker"])
        be = book_equity(c)[0] if c else None
        have[r["ticker"]] = bool(c) and be is not None and be > 0
    # 시총 내림차순 5분위
    order = sorted(elig, key=lambda r: -(_f(r.get("marketCap")) or 0))
    n = len(order)
    size = []
    for i, name in enumerate(SIZE_BUCKETS):
        lo, hi = n * i // 5, n * (i + 1) // 5
        grp = order[lo:hi]
        c = sum(1 for r in grp if have[r["ticker"]])
        size.append({"bucket": name, "count": len(grp), "withBm": c,
                     "coveragePct": round(100.0 * c / len(grp), 2) if grp else None})
    mkt = {}
    for r in elig:
        m = r.get("market") or "UNKNOWN"
        d = mkt.setdefault(m, [0, 0])
        d[1] += 1
        if have[r["ticker"]]:
            d[0] += 1
    markets = [{"market": k, "count": v[1], "withBm": v[0],
                "coveragePct": round(100.0 * v[0] / v[1], 2) if v[1] else None}
               for k, v in sorted(mkt.items(), key=lambda kv: -kv[1][1])]
    cov = [b["coveragePct"] for b in size]
    monotone = all(cov[i] >= cov[i + 1] for i in range(len(cov) - 1))
    spread = round(max(cov) - min(cov), 2)
    return {
        "task": "R33C0B", "signalDate": signal_date,
        "eligible": n, "bySize": size, "byMarket": markets,
        "sizeCoverageSpreadPp": spread,
        "monotoneDecreasingWithSize": monotone,
        "biasFree": (spread <= 10.0) and not monotone,
        "note": "수익률과 연결하지 않음. coverage 편향만 측정.",
        "performanceFunctionCalls": 0,
    }


# ══════════════════════════════════════════════════════════════════════
# §18 결정 엔진 — 성과 미사용. 출력은 정확히 두 값 중 하나.
# ══════════════════════════════════════════════════════════════════════
DECISION_ENUM = ["BM_DART_V2_DEFINITION_FROZEN", "BM_PROSPECTIVE_RETIRED"]


def decide(gates):
    """mandatory gate 전부 PASS 여야 동결. 하나라도 실패하면 종료."""
    failed = [k for k, v in gates.items() if not v]
    if not failed:
        return {"decision": "BM_DART_V2_DEFINITION_FROZEN",
                "gateMatrix": gates, "failedGates": [],
                "reasonCodes": ["ALL_MANDATORY_GATES_PASS"]}
    return {"decision": "BM_PROSPECTIVE_RETIRED",
            "gateMatrix": gates, "failedGates": failed,
            "reasonCodes": [f"GATE_FAILED::{k}" for k in failed]
            + ["NO_SECOND_CANDIDATE_ATTEMPTED"]}


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r33c0b-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    return p
