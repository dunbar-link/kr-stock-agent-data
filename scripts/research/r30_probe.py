#!/usr/bin/env python3
"""R30 Probe Gate — 전체수집 **전에** 공식 소스가 R27 에 쓸 수 있는지 증명한다.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

바로 4,640일을 받지 않는다(§3). 먼저 소규모로 확인한다.

  §6  2007·2008·2009 실제 row 존재 — HTTP 200 은 coverage PASS 가 아니다
  §7  상폐종목 historical coverage — survivorship liquidity bias 금지
  §8  우선주 005935 · 6자리 leading zero · ticker semantics
  §5  거래량 / 거래대금 필드 — 없으면 close×volume 으로 대체하지 않는다
  §9  R27 기존 111일 캐시와 교차검증
  §10 위 전부 통과해야 전체수집 진입

허용오차는 **결과를 보기 전에** 고정한다(R29 와 동일 방식).

안전: 공식 무료 API · 소규모 호출 · 캐시/보고서만 write · 실주문 0.
"""
from __future__ import annotations

import csv
import gzip
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r30_source as S                                    # noqa: E402
from r27_collect import load_day as r27_load_day          # noqa: E402
from r27_collect import trading_calendar                  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
PIT = ROOT / "_cache" / "pit-snapshots"
R27_CACHE = ROOT / "_cache" / "krx-liquidity"

# ── 사전 고정 상수 (결과를 보기 전에 정한다) ─────────────────────────
PROBE_YEARS = [2007, 2008, 2009, 2010, 2015, 2020, 2026]
HARD_YEARS = [2007, 2008, 2009]           # §6 — 하나라도 없으면 FULL_PERIOD_FAIL
CROSSCHECK_MIN_DAYS = 20                  # §9
CROSSCHECK_MIN_TICKERS = 3                # §9
VOLUME_EXACT_MATCH_MIN = 0.99             # 거래량은 시장사실 — 사실상 완전일치
TRADED_VALUE_MEDIAN_ERR_MAX = 0.01        # 공식 필드끼리면 1% 이내여야 한다
DELISTED_SAMPLE_MIN = 2                   # §7
PREFERRED_TICKER = "005935"
BLUECHIP_TICKER = "005930"


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r30-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str),
                 encoding="utf-8")
    print(f"[r30] saved {p.name}", file=sys.stderr)
    return obj


# ══════════════════════ 표본 선정 (추측 금지 — 데이터로 고른다) ══════════════════════
def _pit_day(iso):
    p = PIT / f"{iso}.csv.gz"
    if not p.exists():
        return {}
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return {r["ticker"]: r for r in csv.DictReader(fh)}


def pit_days():
    return sorted(p.name[:-7] for p in PIT.glob("*.csv.gz"))


def pick_samples():
    """상폐·소형주·거래소 표본을 실제 스냅샷에서 **도출**한다. 종목 추측 금지."""
    days = pit_days()
    latest = _pit_day(days[-1]) if days else {}
    old = next((_pit_day(d) for d in days if d.startswith("2007")), {})
    live = set(latest)

    # 상폐 후보: 2007 스냅샷에 있었고 최신 스냅샷에 없는 종목 = 더 이상 상장 아님
    delisted = sorted(t for t in old if t not in live)

    def _cap(row):
        try:
            return float(row.get("marketCap") or 0)
        except (TypeError, ValueError):
            return 0.0

    kospi = [t for t, r in latest.items() if (r.get("market") or "") == "KOSPI"]
    kosdaq = [t for t, r in latest.items() if (r.get("market") or "") == "KOSDAQ"]
    small = sorted((t for t in live if _cap(latest[t]) > 0),
                   key=lambda t: _cap(latest[t]))[:1]
    return {
        "latestPitDate": days[-1] if days else None,
        "delistedCandidates": delisted[:5],
        "delistedCandidateCount": len(delisted),
        "smallCapCurrent": small,
        "kospiCount": len(kospi), "kosdaqCount": len(kosdaq),
        "kospiSample": sorted(kospi)[:1], "kosdaqSample": sorted(kosdaq)[:1],
        "derivedFrom": "PIT 스냅샷 2007 대비 최신 — 종목 하드코딩 아님",
    }


def probe_days(cal):
    """연도별 대표 거래일 1개 — 캐시된 거래일 캘린더에서만 고른다(평일 추정 금지)."""
    out = {}
    for y in PROBE_YEARS:
        cands = [d for d in cal if d.startswith(str(y))]
        if cands:
            out[y] = cands[len(cands) // 2] if y != 2026 else cands[-1]
    return out


# ══════════════════════ §6 연도별 실제 row ══════════════════════
def historical_coverage(cli, days, samples):
    res = {"rule": "HTTP 200 은 coverage PASS 가 아니다 — 실제 row 존재를 본다(§6)",
           "byYear": {}, "hardYears": HARD_YEARS}
    for y, d in sorted(days.items()):
        rows, st, meta = cli.fetch_day(d)
        entry = {"probeDate": d, "status": st,
                 "rows": len(rows) if rows else 0,
                 "pages": meta.get("pages", 0)}
        if rows:
            mk = {}
            for r in rows:
                mk[r["market"]] = mk.get(r["market"], 0) + 1
            entry["marketCounts"] = mk
            entry["fieldsSeen"] = sorted(rows[0].keys())
            entry["volumeNonNull"] = sum(1 for r in rows if r["volume"] is not None)
            entry["tradedValueNonNull"] = sum(
                1 for r in rows if r["traded_value"] is not None)
            entry["hasPreferred"] = any(r["ticker"] == PREFERRED_TICKER
                                        for r in rows)
            entry["hasBluechip"] = any(r["ticker"] == BLUECHIP_TICKER
                                       for r in rows)
            entry["delistedPresent"] = sorted(
                t for t in samples["delistedCandidates"]
                if any(r["ticker"] == t for r in rows))
            entry["sixDigitTickerPct"] = round(
                100.0 * sum(1 for r in rows
                            if len(r["ticker"]) == 6 and r["ticker"].isdigit())
                / len(rows), 2)
        res["byYear"][str(y)] = entry
        time.sleep(S.SLEEP)
    got = [y for y in HARD_YEARS
           if res["byYear"].get(str(y), {}).get("rows", 0) > 0]
    res["hardYearsWithRows"] = got
    res["fullPeriodPass"] = len(got) == len(HARD_YEARS)
    res["verdict"] = ("FULL_PERIOD_OK" if res["fullPeriodPass"]
                      else "PRIMARY_FULL_PERIOD_FAIL")
    return res


# ══════════════════════ §5 스키마 ══════════════════════
def schema_check(hist):
    years = [v for v in hist["byYear"].values() if v.get("rows")]
    vol_ok = bool(years) and all(v.get("volumeNonNull", 0) > 0 for v in years)
    tv_ok = bool(years) and all(v.get("tradedValueNonNull", 0) > 0 for v in years)
    return {
        "volumeFieldAliases": list(S.FIELD_ALIASES["volume"]),
        "tradedValueFieldAliases": list(S.FIELD_ALIASES["tradedValue"]),
        "requiredFields": list(S.REQUIRED_FIELDS),
        "volumeFieldPresent": vol_ok,
        "tradedValueFieldPresent": tv_ok,
        "status": ("OK" if (vol_ok and tv_ok) else
                   ("TRADED_VALUE_FIELD_MISSING" if vol_ok
                    else "VOLUME_FIELD_MISSING")),
        "noCloseTimesVolumeSubstitution": (
            "거래대금이 없으면 close×volume 을 canonical 로 쓰지 않는다. 실제 "
            "거래대금은 체결가 가중이라 종가×거래량과 다르다(§5)."),
    }


# ══════════════════════ §7·§8 상폐 · 우선주 ══════════════════════
def delisted_preferred(cli, hist, samples):
    res = {"delistedCandidates": samples["delistedCandidates"],
           "delistedCandidateCount": samples["delistedCandidateCount"],
           "preferredTicker": PREFERRED_TICKER, "byTicker": {}}
    hist_days = sorted(v["probeDate"] for y, v in hist["byYear"].items()
                       if v.get("rows") and int(y) <= 2010)
    found = set()
    # ① 연도별 전종목 응답에서 이미 관측된 것
    for v in hist["byYear"].values():
        found.update(v.get("delistedPresent", []))
    # ② 종목코드 직접 조회 — §7 이 요구하는 "실제 historical date 조회"
    probe_ticks = samples["delistedCandidates"][:3] + [PREFERRED_TICKER]
    for t in probe_ticks:
        rows_seen, dates_ok = 0, []
        for d in hist_days[:3]:
            items, st, _ = cli.page(bas_dt=d, ticker=t, rows=50)
            if st != "OK":
                res["byTicker"].setdefault(t, {}).setdefault("errors", []).append(st)
                continue
            hits = [i for i in (S.normalize(x, d) for x in items)
                    if i["ticker"] == t]
            if hits:
                rows_seen += len(hits)
                dates_ok.append(d)
            time.sleep(S.SLEEP)
        res["byTicker"][t] = {**res["byTicker"].get(t, {}),
                              "probedDates": hist_days[:3],
                              "datesWithRows": dates_ok, "rows": rows_seen}
        if rows_seen and t != PREFERRED_TICKER:
            found.add(t)
    res["delistedFoundInHistory"] = sorted(found)
    res["delistedHistorySupported"] = len(found) >= DELISTED_SAMPLE_MIN
    res["status"] = ("DELISTED_HISTORY_SUPPORTED"
                     if res["delistedHistorySupported"] else "NOT_SUPPORTED")
    res["survivorshipBiasRisk"] = not res["delistedHistorySupported"]
    pref_years = [y for y, v in hist["byYear"].items() if v.get("hasPreferred")]
    res["preferredFoundYears"] = sorted(pref_years)
    res["preferredSupported"] = bool(pref_years)
    res["leadingZeroPreserved"] = all(
        v.get("sixDigitTickerPct", 0) >= 95.0
        for v in hist["byYear"].values() if v.get("rows"))
    return res


# ══════════════════════ §9 R27 캐시 교차검증 ══════════════════════
def crosscheck(cli):
    """R27 기존 111일 캐시와 공식 API 를 겹치는 날짜/종목에서 대조한다."""
    r27_days = sorted(p.name[:-7] for p in R27_CACHE.glob("*.csv.gz")
                      if not p.name.startswith("_"))
    use = r27_days[:CROSSCHECK_MIN_DAYS] if len(r27_days) >= CROSSCHECK_MIN_DAYS \
        else r27_days
    res = {"tolerancesFixedBeforeResults": True,
           "volumeExactMatchMin": VOLUME_EXACT_MATCH_MIN,
           "tradedValueMedianErrMax": TRADED_VALUE_MEDIAN_ERR_MAX,
           "r27CacheDays": len(r27_days), "comparedDays": len(use),
           "sampleTickers": [], "byTicker": {}, "pairs": 0}
    if not use:
        res["status"] = "NO_OVERLAP"
        return res

    # 표본 종목: R27 캐시 첫날에 존재하고 거래량이 있는 종목 중 상위 유동성 3개
    first = r27_load_day(use[0]) or {}
    tickers = sorted(first, key=lambda t: -(first[t].get("tradedValue") or 0))
    tickers = [t for t in tickers if (first[t].get("volume") or 0) > 0]
    tickers = tickers[:CROSSCHECK_MIN_TICKERS] or tickers
    res["sampleTickers"] = tickers

    vol_hits, vol_tot = 0, 0
    verrs, terrs = [], []
    outliers = 0
    per = {t: {"days": 0, "volExact": 0, "volErrs": [], "tvErrs": []}
           for t in tickers}
    for d in use:
        official, st, _ = cli.fetch_day(d)
        if st != "OK" or not official:
            continue
        omap = {r["ticker"]: r for r in official}
        base = r27_load_day(d) or {}
        for t in tickers:
            a, b = base.get(t), omap.get(t)
            if not a or not b:
                continue
            if b["volume"] is None or b["traded_value"] is None:
                continue
            per[t]["days"] += 1
            vol_tot += 1
            res["pairs"] += 1
            if float(a["volume"]) == float(b["volume"]):
                vol_hits += 1
                per[t]["volExact"] += 1
                verrs.append(0.0)
                per[t]["volErrs"].append(0.0)
            else:
                den = max(abs(float(a["volume"])), 1.0)
                e = abs(float(b["volume"]) - float(a["volume"])) / den
                verrs.append(e)
                per[t]["volErrs"].append(e)
                if e > 0.05:
                    outliers += 1
            den = max(abs(float(a["tradedValue"])), 1.0)
            te = abs(float(b["traded_value"]) - float(a["tradedValue"])) / den
            terrs.append(te)
            per[t]["tvErrs"].append(te)
            if te > 0.05:
                outliers += 1
        time.sleep(S.SLEEP)

    def _p95(xs):
        if not xs:
            return None
        s = sorted(xs)
        return s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]

    for t, v in per.items():
        res["byTicker"][t] = {
            "days": v["days"],
            "volumeExactMatchPct": round(100.0 * v["volExact"] / v["days"], 3)
            if v["days"] else None,
            "volumeMedianErr": round(statistics.median(v["volErrs"]), 6)
            if v["volErrs"] else None,
            "tradedValueMedianErr": round(statistics.median(v["tvErrs"]), 6)
            if v["tvErrs"] else None}
    res.update({
        "volumeExactMatchRate": round(vol_hits / vol_tot, 4) if vol_tot else None,
        "volumeMedianErr": round(statistics.median(verrs), 6) if verrs else None,
        "tradedValueMedianErr": round(statistics.median(terrs), 6) if terrs else None,
        "volumeP95Err": _p95(verrs), "tradedValueP95Err": _p95(terrs),
        "outlierCount": outliers})
    ok = (vol_tot > 0
          and res["volumeExactMatchRate"] >= VOLUME_EXACT_MATCH_MIN
          and (res["tradedValueMedianErr"] or 0) <= TRADED_VALUE_MEDIAN_ERR_MAX)
    res["materialMismatch"] = not ok
    res["status"] = "CONSISTENT" if ok else (
        "INSUFFICIENT_OVERLAP" if vol_tot == 0 else "MATERIAL_MISMATCH")
    # §12 — 기존 111일을 재사용할지 결정하는 규칙(결과 보기 전에 고정)
    res["inheritReuseRule"] = (
        "거래량 완전일치율 >= 99% 이고 거래대금 중앙오차 <= 1% 이면 기존 111일을 "
        "reference 로 재사용한다(불필요 재다운로드 회피). 아니면 공식 소스로 "
        "다시 받아 canonical 을 단일 소스로 통일한다.")
    res["inheritReuseAllowed"] = bool(ok)
    return res


# ══════════════════════ §10·§11 probe 판정 ══════════════════════
def probe_verdict(cred, hist, schema, dp, cc, source_alive):
    checks = [
        ("CREDENTIAL", cred["status"] == "PRESENT"),
        ("ENDPOINT_ALIVE", bool(source_alive)),
        ("HISTORICAL_2007_2009", bool(hist.get("fullPeriodPass"))),
        ("KOSPI", any("KOSPI" in (v.get("marketCounts") or {})
                      for v in hist.get("byYear", {}).values())),
        ("KOSDAQ", any("KOSDAQ" in (v.get("marketCounts") or {})
                       for v in hist.get("byYear", {}).values())),
        ("DELISTED_HISTORY", bool(dp.get("delistedHistorySupported"))),
        ("VOLUME_FIELD", bool(schema.get("volumeFieldPresent"))),
        ("TRADED_VALUE_FIELD", bool(schema.get("tradedValueFieldPresent"))),
        ("TICKER_MAPPING", bool(dp.get("preferredSupported")
                                and dp.get("leadingZeroPreserved"))),
        ("R27_CACHE_CONSISTENT", cc.get("status") == "CONSISTENT"),
        ("AUTOMATION_ALLOWED", True),      # 공공데이터 개방 — 이용허락범위 제한 없음
        ("PAGINATION_RETRY", True),        # 클라이언트가 구현·측정한다
    ]
    failed = [n for n, ok in checks if not ok]
    if cred["status"] != "PRESENT":
        verdict = "CREDENTIAL_ABSENT"
    elif not hist.get("fullPeriodPass"):
        verdict = ("PROBE_PASS_2010_PLUS_ONLY"
                   if any(v.get("rows") for k, v in hist.get("byYear", {}).items()
                          if int(k) >= 2010) else "PROBE_FAIL_NO_HISTORY")
    elif not schema.get("tradedValueFieldPresent"):
        verdict = "NO_TRADED_VALUE"
    elif not dp.get("delistedHistorySupported"):
        verdict = "NO_DELISTED_HISTORY"
    elif failed:
        verdict = "PROBE_FAIL"
    else:
        verdict = "PROBE_PASS_FULL_PERIOD"
    return {"checks": {n: ok for n, ok in checks},
            "failedChecks": failed,
            "verdict": verdict,
            "fullAcquisitionAllowed": verdict == "PROBE_PASS_FULL_PERIOD",
            "rule": "하나라도 material 하게 실패하면 전체수집 시작 금지(§10)."}


# ══════════════════════ main ══════════════════════
def main() -> int:
    cred = S.credential_status()
    save("credential-status", {"task": "R30", **cred,
                               **S.source_meta(),
                               "secretExposure": 0})

    # 인증 없이도 엔드포인트 생존과 오류 규약은 확인한다(데이터 수집 아님).
    alive = None
    try:
        import requests
        r = requests.get(S.ENDPOINT, params={"resultType": "json", "numOfRows": 1,
                                             "pageNo": 1, "basDt": "20200102"},
                         timeout=S.TIMEOUT, headers={"User-Agent": S.UA})
        alive = {"httpStatus": r.status_code, "reachable": r.status_code < 500,
                 "errorContract": S._scrub(r.text[:200])}
    except Exception as e:                                   # noqa: BLE001
        alive = {"reachable": False, "error": type(e).__name__}
    # 이 1콜은 데이터 수집이 아니라 엔드포인트 생존 확인이다. 따로 센다.
    alive["unauthenticatedCalls"] = 1

    samples = pick_samples()
    cal = trading_calendar()
    days = probe_days(cal)
    base = {"task": "R30", **S.source_meta(),
            "credentialStatus": cred["status"],
            "endpointProbe": alive,
            "probeYears": PROBE_YEARS, "probeDates": days,
            "samples": samples}

    if cred["status"] != "PRESENT":
        empty_hist = {"byYear": {}, "fullPeriodPass": False,
                      "verdict": "NOT_RUN_CREDENTIAL_ABSENT",
                      "hardYears": HARD_YEARS, "hardYearsWithRows": []}
        empty_schema = {**schema_check(empty_hist),
                        "status": "NOT_RUN_CREDENTIAL_ABSENT"}
        empty_dp = {"status": "NOT_RUN_CREDENTIAL_ABSENT",
                    "delistedCandidates": samples["delistedCandidates"],
                    "delistedHistorySupported": False,
                    "preferredSupported": False, "leadingZeroPreserved": False}
        empty_cc = {"status": "NOT_RUN_CREDENTIAL_ABSENT",
                    "r27CacheDays": len(list(R27_CACHE.glob("*.csv.gz"))),
                    "comparedDays": 0, "pairs": 0,
                    "inheritReuseAllowed": False}
        pv = probe_verdict(cred, empty_hist, empty_schema, empty_dp, empty_cc,
                           alive.get("reachable"))
        save("probe", {**base, "calls": 0, "status": "CREDENTIAL_ABSENT"})
        save("historical-coverage-probe", {**base, **empty_hist})
        save("delisted-preferred-probe", {**base, **empty_dp})
        save("r27-cache-crosscheck", {**base, **empty_cc})
        save("source-verdict", {**base, "schema": empty_schema, **pv})
        print(json.dumps({"verdict": pv["verdict"],
                          "credential": cred["status"]}, ensure_ascii=False))
        return 4

    cli = S.Client()
    hist = historical_coverage(cli, days, samples)
    schema = schema_check(hist)
    dp = delisted_preferred(cli, hist, samples)
    cc = crosscheck(cli)
    pv = probe_verdict(cred, hist, schema, dp, cc, alive.get("reachable"))
    calls = {"calls": cli.calls, "retries": cli.retries,
             "rateLimitHits": cli.rateLimitHits, "quota": cli.quota}
    save("probe", {**base, **calls, "status": "RUN"})
    save("historical-coverage-probe", {**base, **hist, **calls})
    save("delisted-preferred-probe", {**base, **dp})
    save("r27-cache-crosscheck", {**base, **cc})
    save("source-verdict", {**base, "schema": schema, **pv, **calls})
    print(json.dumps({"verdict": pv["verdict"], "failed": pv["failedChecks"]},
                     ensure_ascii=False))
    return 0 if pv["fullAcquisitionAllowed"] else 5


if __name__ == "__main__":
    raise SystemExit(main())
