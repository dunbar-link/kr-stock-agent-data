#!/usr/bin/env python3
"""R31 provenance-preserving stitch 검증 + manifest + R27 inventory 갱신.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

── stitching 은 "합치는 코드" 가 아니라 "경계를 지키는 계약" 이다 ────
  canonical 캐시(_cache/official-liquidity)는 날짜당 파일 하나이고 모든 행이
  source·source_version·retrieved_at 을 들고 있다. 그래서 2010~2019 를 KRX 로,
  2020+ 를 기존 R30 공공데이터포털로 채우면 그 자체가 provenance 를 보존한
  stitched dataset 이다. 값을 평균내거나 fill 하거나 덮어쓰는 코드가 없다 —
  애초에 그런 경로를 만들지 않는 것이 §10 계약이다.

  이 모듈이 하는 일은 그 계약이 **실제로 지켜졌는지 검증**하고 manifest 를 남기는 것이다.
    · 경계 중복 0 (한 날짜에 두 source 가 섞이지 않았는가)
    · source 구간이 계약과 일치하는가
    · provenance 필드 완전성
    · 2020 KRX 는 canonical 에 들어가지 않았는가(비교 근거 전용)

── R27 inventory 갱신 ─────────────────────────────────────────────
  r27_run.py 는 day coverage 표시를 위해 r27-data-inventory-latest.json 을 읽는다.
  그 파일은 원래 r27_collect(pykrx) 가 썼는데 그 경로는 R28 에서 금지됐다.
  그래서 **네트워크 0 으로 디스크 실측만** 반영해 같은 스키마로 갱신한다.
  판정 기준(coverage gate)은 건드리지 않는다 — 그건 r27_run 이 종목-시점으로 계산한다.

안전: 읽기·계산 전용. 네트워크 0. write 는 evidence 2개(manifest, inventory).
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r30_cache as K                                       # noqa: E402
import r31_acquire as A                                     # noqa: E402
import r31_source as S                                      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

BOUNDARY = {
    "krxRange": ["2010-01-04", "2019-12-31"],
    "dataGoKrFrom": "2020-01-01",
}
DGK_SOURCE = "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"
SCHEMA_VERSION = "r30_cache.COLS/v1"


def scan_canonical(days):
    """canonical 캐시를 훑어 source 구성·경계·provenance 를 확인한다."""
    per_source = Counter()
    per_year_source = {}
    mixed_days = []
    missing_prov = 0
    rows_total = 0
    dup_keys = 0
    present, missing = [], []
    dates_seen = []

    for d in days:
        rows = K.load_day(d)
        if rows is None:
            missing.append(d)
            continue
        present.append(d)
        dates_seen.append(d)
        srcs = set()
        seen = set()
        for t, r in rows.items():
            rows_total += 1
            s = r.get("source") or "UNKNOWN"
            srcs.add(s)
            per_source[s] += 1
            y = d[:4]
            per_year_source.setdefault(y, Counter())[s] += 1
            if not (r.get("source") and r.get("source_version")
                    and r.get("retrieved_at")):
                missing_prov += 1
            if t in seen:
                dup_keys += 1
            seen.add(t)
        if len(srcs) > 1:
            mixed_days.append({"date": d, "sources": sorted(srcs)})

    return {"daysPresent": len(present), "daysMissing": len(missing),
            "missingDays": missing[:50], "rows": rows_total,
            "rowsBySource": dict(per_source),
            "rowsByYearSource": {y: dict(c) for y, c in sorted(per_year_source.items())},
            "boundaryMixedDays": mixed_days,
            "duplicatePrimaryKeys": dup_keys,
            "rowsMissingProvenance": missing_prov,
            "minDate": min(dates_seen) if dates_seen else None,
            "maxDate": max(dates_seen) if dates_seen else None}


def verify_boundary(scan):
    """계약대로 source 가 나뉘었는지. 위반은 숨기지 않는다."""
    v = {"boundaryDuplicates": len(scan["boundaryMixedDays"]),
         "violations": []}
    for y, byS in scan["rowsByYearSource"].items():
        if "2010" <= y <= "2019":
            wrong = {s: n for s, n in byS.items() if s != S.SOURCE}
            if wrong:
                v["violations"].append({"year": y, "expected": S.SOURCE,
                                        "found": wrong})
        elif y >= "2020":
            wrong = {s: n for s, n in byS.items() if s != DGK_SOURCE}
            if wrong:
                v["violations"].append({"year": y, "expected": DGK_SOURCE,
                                        "found": wrong})
    v["ok"] = (v["boundaryDuplicates"] == 0 and not v["violations"]
               and scan["rowsMissingProvenance"] == 0
               and scan["duplicatePrimaryKeys"] == 0)
    return v


def overlap_isolated():
    """2020 KRX 가 canonical 을 오염시키지 않았는지 — 비교 캐시는 별도 폴더여야 한다."""
    ovl = A.OVERLAP_CACHE
    n = len(list(ovl.glob("*.csv.gz"))) if ovl.exists() else 0
    can2020 = [d for d in K.have_days() if d.startswith("2020")]
    krx_in_canonical_2020 = 0
    for d in can2020[:20]:          # 표본이 아니라 확인용 — 아래 scan 이 전수를 본다
        rows = K.load_day(d) or {}
        krx_in_canonical_2020 += sum(1 for r in rows.values()
                                     if (r.get("source") or "") == S.SOURCE)
    return {"overlapCacheDir": str(ovl.relative_to(ROOT)),
            "overlapCacheDays": n,
            "krxRowsFoundInCanonical2020Sample": krx_in_canonical_2020,
            "canonicalSeparated": krx_in_canonical_2020 == 0}


def manifest_hash(scan):
    payload = json.dumps({k: scan[k] for k in
                          ("daysPresent", "rows", "rowsBySource",
                           "rowsByYearSource", "minDate", "maxDate")},
                         ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def refresh_r27_inventory(scope, scan):
    """r27_run 이 표시용으로 읽는 inventory 를 디스크 실측으로 갱신(네트워크 0)."""
    p = RD / "r27-data-inventory-latest.json"
    inv = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    r27_have = K.r27_have_days()
    need = scope["neededDays"]
    collected = len([d for d in scope["_need"] if d in r27_have])
    inv.update({
        "task": "R27/R31",
        "status": "COLLECTED_VIA_R31_KRX_OFFICIAL_OPEN_API",
        "source": f"{S.SOURCE} ({S.SOURCE_VERSION}) + {DGK_SOURCE}",
        "sourceReuse": ("R30 캐시 스키마·투영 재사용. 새 data framework 0. "
                        "pykrx 대량수집 0 · 웹 스크래핑 0."),
        "lookbackTradingDays": 20,
        "tradingCalendarDays": scope["calendarDays"],
        "decisionDates": scope["decisionDates"],
        "daysRequired": need,
        "daysCollectedTotal": collected,
        "daysPending": need - collected,
        "coveragePct": round(100.0 * collected / need, 3) if need else 0.0,
        "cacheDir": str(ROOT / "_cache" / "krx-liquidity"),
        "canonicalRowsBySource": scan["rowsBySource"],
        "aborted": None,
        "paidData": 0,
        "newCredential": 0,
        "regeneratedBy": "r31_stitch.refresh_r27_inventory (network 0)",
    })
    p.write_text(json.dumps(inv, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"daysRequired": need, "daysCollectedTotal": collected,
            "coveragePct": inv["coveragePct"]}


def main() -> int:
    sc = A.scopes()
    need = sorted(set(sc["backfill"]) | set(sc["overlap"]))
    # canonical 전수 = R27 필요일 전체(2006~2026)
    from r27_collect import needed_days, trading_calendar
    import r16_canonical as C
    from r16_audit import contiguous_span
    all_need = needed_days(trading_calendar(),
                           contiguous_span(sorted(C.load_capital_series())))

    scan = scan_canonical(all_need)
    boundary = verify_boundary(scan)
    iso = overlap_isolated()
    scope = {"calendarDays": sc["calendarDays"], "decisionDates": sc["decisionDates"],
             "neededDays": sc["neededDays"], "_need": all_need}
    inv = refresh_r27_inventory(scope, scan)

    out = {"task": "R31", "phase": "stitch",
           "sourceBoundary": BOUNDARY,
           "schemaVersion": SCHEMA_VERSION,
           "stitchedRange": [scan["minDate"], scan["maxDate"]],
           "rowsBySource": scan["rowsBySource"],
           "rowsByYearSource": scan["rowsByYearSource"],
           "daysPresent": scan["daysPresent"], "daysMissing": scan["daysMissing"],
           "duplicatePrimaryKeys": scan["duplicatePrimaryKeys"],
           "rowsMissingProvenance": scan["rowsMissingProvenance"],
           "boundaryVerification": boundary,
           "overlapIsolation": iso,
           "manifestSha256": manifest_hash(scan),
           "r27Inventory": inv,
           "forbiddenOperationsUsed": {"average": 0, "maxPick": 0, "minPick": 0,
                                       "forwardFill": 0, "backwardFill": 0,
                                       "estimatedTradedValue": 0,
                                       "closeTimesVolumeProxy": 0,
                                       "currentListingFilter": 0,
                                       "delistedRemoval": 0},
           "storageClass": "LOCAL_ONLY"}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r31-stitch-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"stitched range : {scan['minDate']} .. {scan['maxDate']}")
    print(f"days present   : {scan['daysPresent']} · missing {scan['daysMissing']}")
    print(f"rows by source : {scan['rowsBySource']}")
    print(f"boundary dup   : {boundary['boundaryDuplicates']} · "
          f"violations {len(boundary['violations'])} · ok={boundary['ok']}")
    print(f"provenance miss: {scan['rowsMissingProvenance']} · "
          f"dupKeys {scan['duplicatePrimaryKeys']}")
    print(f"2020 isolation : {iso}")
    print(f"R27 inventory  : {inv}")
    print(f"manifest sha256: {out['manifestSha256']}")
    return 0 if boundary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
