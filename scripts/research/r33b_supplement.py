#!/usr/bin/env python3
"""R33B 보완 수집 — 부족 거래일 계획 동결 → local 재사용 → 공식 API 최소 보완.

WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B

R33A 가 확인한 부족 거래일만 채운다. 전체 기간 backfill 을 하지 않는다.

── 순서 (바꾸지 않는다) ────────────────────────────────────────────
  1. R33A availability artifact 에서 부족 집합 복원 (보고서 문구 아님)
  2. 목적별 분류 + 필요 key 확정
  3. plan hash 동결          ← 여기서부터 계획 밖 날짜 호출 금지
  4. 기존 local 공식 근거 재사용 확인
  5. 남은 것만 공식 API 호출 (예산·rate·circuit breaker)
  6. supplemental local-only 저장 + manifest
  7. 검증 분류 (valid / no-fill / 휴장 / 결손)

── R31 frozen cache 를 건드리지 않는다 ─────────────────────────────
  _cache/official-liquidity 는 R31 dataset manifest(3439dec9…)의 대상이다.
  거기 한 행이라도 쓰면 manifest hash 가 움직여 상위 동결이 깨진다.
  보완분은 전부 _cache/r33b-supplemental-open/ 에만 쓴다.

── 성과 계산 없음 ──────────────────────────────────────────────────
  이 모듈은 가격필드를 **수집·검증**만 한다. 수익률을 만들지 않는다.

안전: 인증키 값 미출력 · URL 미출력 · 계획 밖 날짜 호출 0 · R31 write 0.
"""
from __future__ import annotations

import calendar
import csv
import gzip
import hashlib
import json
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
OFFICIAL = ROOT / "_cache" / "official-liquidity"
SUPP = ROOT / "_cache" / "r33b-supplemental-open"
RAW = SUPP / "raw"
NORM = SUPP / "normalized"
MANIFEST = SUPP / "manifest"
AUDIT = SUPP / "audit"

HORIZON_MONTHS = 36
KRX_BOUNDARY_END = "2019-12-30"
FSC_BOUNDARY_START = "2020-01-02"

COLS = ["date", "ticker", "name", "market", "close", "open", "high", "low",
        "volume", "traded_value", "market_cap", "shares",
        "source", "source_version", "retrieved_at", "quality_flag"]


# ══════════════════════════════════════════════════════════════════════
# 달력 helper
# ══════════════════════════════════════════════════════════════════════
def add_months(iso, months):
    y, m, d = (int(x) for x in iso.split("-"))
    m2 = m + months
    y2 = y + (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    # 말일 절단: 1/31 + 1개월 → 2/28(29). range(d, 27, -1) 은 d<=27 일 때
    # 빈 range 라 항상 28일로 떨어지는 결함이 있었다(2026-09-08 R33B 에서 실측·수정).
    return date(y2, m2, min(d, calendar.monthrange(y2, m2)[1])).isoformat()


def cohort_dates(signal_dates, cal):
    """signal → entry(다음 거래일) → nominal exit → exit session. 성과 아님."""
    pos = {d: i for i, d in enumerate(cal)}
    out = []
    for d in signal_dates:
        i = pos.get(d)
        entry = cal[i + 1] if i is not None and i + 1 < len(cal) else None
        nom = add_months(entry, HORIZON_MONTHS) if entry else None
        nxt = [x for x in cal if x >= nom] if nom else []
        out.append({"signalDate": d, "entryDate": entry,
                    "nominalExitDate": nom,
                    "exitSession": nxt[0] if nxt else None})
    return out


# ══════════════════════════════════════════════════════════════════════
# §8 부족일 계획
# ══════════════════════════════════════════════════════════════════════
def source_for(d):
    if d <= KRX_BOUNDARY_END:
        return "KRX_OPENAPI", "KRX_OFFICIAL_OPEN_API"
    if d >= FSC_BOUNDARY_START:
        return "PUBLIC_DATA_PORTAL_FSC", "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"
    return "UNKNOWN_BOUNDARY", "UNKNOWN"


def local_evidence(d):
    """이미 공식 open 을 갖고 있는 local cache 가 있는지. 없으면 빈 리스트."""
    found = []
    for name, p, has_open in (
            ("official-liquidity", OFFICIAL / f"{d}.csv.gz", True),
            ("krx-overlap-2020", ROOT / "_cache" / "krx-overlap-2020" / f"{d}.csv.gz", True),
            ("r33b-supplemental", NORM / f"{d}.csv.gz", True),
            # 아래 둘은 open 컬럼 자체가 없다 — 공식 시가 근거가 될 수 없다.
            ("krx-liquidity", ROOT / "_cache" / "krx-liquidity" / f"{d}.csv.gz", False),
            ("pit-snapshots", ROOT / "_cache" / "pit-snapshots" / f"{d}.csv.gz", False),
    ):
        if p.exists():
            found.append({"cache": name, "hasOpenField": has_open,
                          "usableAsOpenSource": has_open})
    return found


def build_plan(availability_json=None, cohorts=None):
    """부족 집합을 machine-readable artifact 에서 복원해 목적별로 분류한다."""
    av = json.loads((availability_json or
                     (RD / "r33a-availability-latest.json")).read_text(encoding="utf-8"))
    ent = set(av["entryDateMissingDates"])
    ex = set(av["exitSessionMissingDates"])
    need = sorted(ent | ex)

    by_date = {}
    for d in need:
        if d in ent and d in ex:
            purpose = "ENTRY_AND_EXIT_REQUIRED"
        elif d in ent:
            purpose = "ENTRY_OPEN_REQUIRED"
        else:
            purpose = "EXIT_OPEN_REQUIRED"
        boundary, provider = source_for(d)
        ev = local_evidence(d)
        usable = [e for e in ev if e["usableAsOpenSource"]]
        markets = (["KOSPI", "KOSDAQ"] if boundary == "KRX_OPENAPI" else ["ALL"])
        affected = []
        if cohorts:
            for c in cohorts:
                if c["entryDate"] == d or c["exitSession"] == d:
                    affected.append(c["signalDate"])
        by_date[d] = {
            "required_date": d,
            "market": markets,
            "source_boundary": boundary,
            "source_provider": provider,
            "purpose": purpose,
            "required_fields": ["open", "volume", "traded_value", "close"],
            "affected_cohort_ids": affected,
            "affected_factor": ["BM", "SIZE_SMALL", "CONTROL"],
            "existing_local_evidence": ev,
            "network_fetch_required": len(usable) == 0,
        }

    krx = [d for d in need if by_date[d]["source_boundary"] == "KRX_OPENAPI"]
    fsc = [d for d in need if by_date[d]["source_boundary"] == "PUBLIC_DATA_PORTAL_FSC"]
    already = [d for d in need if not by_date[d]["network_fetch_required"]]
    absent = [d for d in need if by_date[d]["network_fetch_required"]]
    pairs = sum(len(by_date[d]["market"]) for d in absent)

    return {
        "expectedDistinctDates": 50,
        "distinctRequiredDates": len(need),
        "dates": need,
        "byDate": by_date,
        "purposeCounts": {
            "ENTRY_OPEN_REQUIRED": sum(
                1 for d in need if by_date[d]["purpose"] == "ENTRY_OPEN_REQUIRED"),
            "EXIT_OPEN_REQUIRED": sum(
                1 for d in need if by_date[d]["purpose"] == "EXIT_OPEN_REQUIRED"),
            "ENTRY_AND_EXIT_REQUIRED": sum(
                1 for d in need if by_date[d]["purpose"] == "ENTRY_AND_EXIT_REQUIRED"),
            "CALENDAR_MAPPING_REQUIRED": 0,
            "CORPORATE_ACTION_MAPPING_REQUIRED": 0,
            "DELISTING_TERMINAL_REQUIRED": 0,
            "OTHER": 0,
        },
        "fortySevenVsFiftyExplained": (
            "entry 전용 47 + exit 전용 3 + 교집합 0 = distinct 50. "
            "R33A 가 보고한 47 은 entry-date 결손이고, 나머지 3 은 exit session "
            "결손이다. 추측이 아니라 두 배열의 집합연산으로 확인했다."),
        "requiredDateMarketPairs": sum(len(by_date[d]["market"]) for d in need),
        "entryPurposeDates": len([d for d in need
                                  if by_date[d]["purpose"] != "EXIT_OPEN_REQUIRED"]),
        "exitPurposeDates": len([d for d in need
                                 if by_date[d]["purpose"] != "ENTRY_OPEN_REQUIRED"]),
        "bothPurposeDates": sum(
            1 for d in need if by_date[d]["purpose"] == "ENTRY_AND_EXIT_REQUIRED"),
        "affectedCohortCount": len({s for d in need
                                    for s in by_date[d]["affected_cohort_ids"]}),
        "krxSourceCount": len(krx), "fscSourceCount": len(fsc),
        "alreadyLocalCount": len(already), "trulyAbsentCount": len(absent),
        "expectedMinimumCalls": pairs,
        "maximumBoundedCalls": 250,
        "dateRange": {"min": need[0], "max": need[-1]} if need else None,
    }


def plan_hash(plan) -> str:
    payload = json.dumps({k: plan[k] for k in
                          ("dates", "purposeCounts", "krxSourceCount",
                           "fscSourceCount", "distinctRequiredDates")},
                         ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# §11 공식 API 최소 보완
# ══════════════════════════════════════════════════════════════════════
class Budget:
    def __init__(self, hard_max=250):
        self.hard_max = hard_max
        self.calls = 0
        self.retries = 0
        self.failures = 0
        self.rate_limited = 0
        self.stopped = None

    def spend(self, n=1, retry=False):
        """r31_source 가 budget.spend(retry=...) 로 부르므로 그 시그니처를 맞춘다."""
        if self.calls + n > self.hard_max:
            raise RuntimeError(f"API 예산 초과: {self.calls}+{n} > {self.hard_max}")
        self.calls += n
        if retry:
            self.retries += n


def _atomic_write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in COLS})
    tmp.replace(path)


def fetch_missing(plan, budget: Budget, allowed_dates: set, dry_run=False):
    """계획 안의 날짜만 가져온다. 계획 밖 날짜는 호출조차 하지 않는다."""
    import r30_source
    import r31_credential
    import r31_source

    results, off_plan = {}, 0
    krx_key = None

    for d in plan["dates"]:
        if d not in allowed_dates:
            off_plan += 1
            continue
        spec = plan["byDate"][d]
        if not spec["network_fetch_required"]:
            results[d] = {"status": "LOCAL_REUSE", "rows": 0, "calls": 0}
            continue
        if (NORM / f"{d}.csv.gz").exists():
            results[d] = {"status": "ALREADY_SUPPLEMENTED", "rows": 0, "calls": 0}
            continue
        if dry_run:
            results[d] = {"status": "DRY_RUN", "rows": 0,
                          "calls": len(spec["market"])}
            continue

        rows, calls, err = [], 0, None
        try:
            if spec["source_boundary"] == "KRX_OPENAPI":
                if krx_key is None:
                    krx_key = r31_credential.auth_key()
                import datetime as _dt
                import zoneinfo as _zi
                ts = _dt.datetime.now(
                    _zi.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
                for mkt in ("KOSPI", "KOSDAQ"):
                    before = budget.calls
                    # fetch_day 는 raw rows + meta 를 준다. 정규화는 별도 호출이다.
                    raw, meta = r31_source.fetch_day(mkt, d, krx_key, budget=budget)
                    calls += max(1, budget.calls - before)
                    rows.extend(r31_source.normalize(rr, d, mkt, ts) for rr in raw)
            else:
                cli = r30_source.Client() if hasattr(r30_source, "Client") else None
                if cli is None:
                    raise RuntimeError("r30_source.Client 없음 — 기존 수집기 재사용 실패")
                budget.spend(1)
                calls += 1
                got, st, meta = cli.fetch_day(d)
                if meta.get("pages", 1) > 1:
                    budget.spend(meta["pages"] - 1)
                    calls += meta["pages"] - 1
                rows = got or []
                if st not in ("OK", "EMPTY"):
                    err = st
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            budget.failures += 1

        if err:
            results[d] = {"status": "FETCH_FAILED", "error": str(err)[:200],
                          "rows": 0, "calls": calls}
            budget.stopped = f"{d}: {str(err)[:120]}"
            break

        for r in rows:
            r.setdefault("date", d)
        _atomic_write_csv(NORM / f"{d}.csv.gz", rows)
        results[d] = {"status": "FETCHED", "rows": len(rows), "calls": calls}

    return {"byDate": results, "offPlanAttempts": off_plan,
            "calls": budget.calls, "retries": budget.retries,
            "failures": budget.failures, "rateLimited": budget.rate_limited,
            "stopped": budget.stopped}


# ══════════════════════════════════════════════════════════════════════
# §13 보완 데이터 검증
# ══════════════════════════════════════════════════════════════════════
def _num(x):
    if x in (None, "", "None"):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return "NONNUM"


def load_supplement(d):
    p = NORM / f"{d}.csv.gz"
    if not p.exists():
        return None
    rows, dup = {}, 0
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            t = r.get("ticker")
            if not t:
                continue
            if t in rows:
                dup += 1
                continue
            rows[t] = r
    return {"rows": rows, "duplicates": dup}


def validate(plan, cal_set):
    out, agg = {}, {"VALID_DATE": 0, "INVALID_REQUIRED_DATE_PLAN": 0,
                    "DATA_INTEGRITY_MISSING": 0}
    tot_rows = tot_dup = tot_invalid = 0
    valid_open = no_fill = no_open = 0
    src = {}
    for d in plan["dates"]:
        if d not in cal_set:
            out[d] = {"class": "INVALID_REQUIRED_DATE_PLAN"}
            agg["INVALID_REQUIRED_DATE_PLAN"] += 1
            continue
        day = load_supplement(d)
        if day is None:
            out[d] = {"class": "DATA_INTEGRITY_MISSING"}
            agg["DATA_INTEGRITY_MISSING"] += 1
            continue
        n = len(day["rows"])
        inv = vo = nf = nf_no_open = 0
        for t, r in day["rows"].items():
            op, vol = _num(r.get("open")), _num(r.get("volume"))
            if op == "NONNUM" or vol == "NONNUM" or op is None or vol is None:
                inv += 1
                continue
            if op < 0 or vol < 0:
                inv += 1
                continue
            if op > 0 and vol > 0:
                vo += 1
            elif op == 0 and vol == 0:
                nf += 1
            elif op == 0 and vol > 0:
                # 거래는 있었는데 시가가 0 — 종가 단일가에만 체결된 초저유동
                # 종목이다. R33A 계약의 valid open 정의(open>0 AND volume>0)가
                # 이미 이 경우를 no-fill 로 규정한다. 계약을 바꾸지 않는다.
                nf_no_open += 1
            s = r.get("source") or ""
            src[s] = src.get(s, 0) + 1
        out[d] = {"class": "VALID_DATE", "rows": n, "validOpen": vo,
                  "noFill": nf, "noFillNoOpeningPrice": nf_no_open,
                  "invalid": inv, "duplicates": day["duplicates"]}
        agg["VALID_DATE"] += 1
        tot_rows += n
        tot_dup += day["duplicates"]
        tot_invalid += inv
        valid_open += vo
        no_fill += nf
        no_open += nf_no_open
    return {"byDate": out, "classCounts": agg, "rows": tot_rows,
            "duplicates": tot_dup, "invalid": tot_invalid,
            "validOpen": valid_open, "legitimateNoFill": no_fill,
            "noFillNoOpeningPrice": no_open,
            "rowsAccountedFor": valid_open + no_fill + no_open + tot_invalid,
            "sourceSplit": src,
            "completenessPass": (agg["DATA_INTEGRITY_MISSING"] == 0
                                 and agg["INVALID_REQUIRED_DATE_PLAN"] == 0
                                 and tot_dup == 0 and tot_invalid == 0)}


def write_manifest(plan, ph, fetched, val, contract_hash, spec_hash):
    import datetime
    import zoneinfo
    MANIFEST.mkdir(parents=True, exist_ok=True)
    m = {
        "task": "R33B", "localOnly": True, "gitTracked": False,
        "contractHash": contract_hash, "specHash": spec_hash,
        "missingDatePlanHash": ph,
        "provider": {"krx": "KRX_OPENAPI (stk_bydd_trd / ksq_bydd_trd)",
                     "fsc": "PUBLIC_DATA_PORTAL_FSC getStockPriceInfo"},
        "sourceBoundary": {"krxEnd": KRX_BOUNDARY_END,
                           "fscStart": FSC_BOUNDARY_START},
        "dates": plan["dates"], "markets": ["KOSPI", "KOSDAQ"],
        "requestCount": fetched["calls"],
        "localReuseCount": plan["alreadyLocalCount"],
        "networkFetchCount": sum(1 for v in fetched["byDate"].values()
                                 if v["status"] == "FETCHED"),
        "rowCount": val["rows"], "duplicateCount": val["duplicates"],
        "invalidCount": val["invalid"],
        "missingCount": val["classCounts"]["DATA_INTEGRITY_MISSING"],
        "validOpen": val["validOpen"],
        "legitimateNoFill": val["legitimateNoFill"],
        "sourceSplit": val["sourceSplit"],
        "collectedAt": datetime.datetime.now(
            zoneinfo.ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds"),
        "schemaVersion": "r33b-supplement-1",
        "r31FrozenCacheMutations": 0,
    }
    (MANIFEST / "r33b-supplement-manifest.json").write_text(
        json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
    return m
