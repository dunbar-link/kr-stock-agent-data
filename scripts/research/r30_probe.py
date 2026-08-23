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
import re
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
# §6 역방향 규칙 — "HTTP 200 은 coverage PASS 가 아니다" 의 짝.
#   **API_ERROR 는 coverage FAIL 이 아니다.** 최신 통제연도(반드시 데이터가
#   있어야 하는 해)조차 row 를 못 받으면 그건 소스의 historical 한계가 아니라
#   호출 자체가 성립하지 않은 것이다. 그 상태에서 "historical 없음" 이라고
#   판정하면 유일한 합법 소스를 근거 없이 폐기하게 된다.
CONTROL_YEAR = 2026                       # 최신 = 반드시 존재해야 하는 해
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

    # ── 통제연도 — 인증이 실효했는지 먼저 본다 ────────────────────
    ctrl = res["byYear"].get(str(CONTROL_YEAR), {})
    res["controlYear"] = CONTROL_YEAR
    res["controlYearStatus"] = ctrl.get("status")
    res["controlYearRows"] = ctrl.get("rows", 0)
    res["authEffective"] = bool(ctrl.get("rows", 0) > 0)
    # 모든 연도가 같은 오류로 죽었는지 — 소스 한계가 아니라 호출 실패의 서명이다
    sts = {v.get("status") for v in res["byYear"].values()}
    res["allYearsSameFailure"] = (len(sts) == 1 and "OK" not in sts)
    res["coverageMeasurable"] = res["authEffective"]

    if not res["authEffective"]:
        # 측정 자체가 성립하지 않았다. coverage 결론을 내지 않는다(fail-closed).
        res["verdict"] = "NOT_MEASURABLE_AUTH_NOT_EFFECTIVE"
        res["fullPeriodPass"] = False
        res["note"] = (
            f"통제연도 {CONTROL_YEAR} 조차 row 0 (status={ctrl.get('status')}). "
            "이 소스에 historical 이 없다는 뜻이 아니라 호출이 성립하지 않은 "
            "것이다 — coverage 결론을 내지 않는다(§6 역방향).")
    else:
        res["verdict"] = ("FULL_PERIOD_OK" if res["fullPeriodPass"]
                          else "PRIMARY_FULL_PERIOD_FAIL")
    return res


def auth_discrimination(cli_key_present):
    """인증 실패인지 소스 한계인지 — 게이트웨이가 구분해 주는지 본다.

    같은 요청을 (a) 무효키 (b) 빈키 로 한 번씩 더 보낸다. 정상키와 **같은**
    오류코드가 돌아오면 게이트웨이는 인증 상태를 구분해 주지 않는 것이므로,
    응답만으로 "키가 틀렸다" 고도 "소스에 데이터가 없다" 고도 단정할 수 없다.
    그 사실 자체를 evidence 로 남긴다 — 다음 세션이 다시 추측하지 않도록.

    통제연도가 실패했을 때만 부른다(추가 2콜).
    """
    import requests
    out = {"rule": "무효키·빈키·정상키에 같은 코드가 오면 응답으로 인증 상태를 "
                   "판별할 수 없다 — 소스 부적합으로 단정 금지.",
           "probes": {}, "extraCalls": 0}
    params = {"resultType": "json", "numOfRows": 1, "pageNo": 1,
              "basDt": str(CONTROL_YEAR) + "0803"}
    for label, key in (("invalidKey", "X" * 32), ("emptyKey", "")):
        try:
            r = requests.get(S.ENDPOINT, params={**params, "serviceKey": key},
                             timeout=S.TIMEOUT, headers={"User-Agent": S.UA})
            out["extraCalls"] += 1
            m = re.search(r"returnReasonCode\D{0,6}(\d+)", r.text)
            e = re.search(r"errMsg[\"'>:\s]*([A-Z_]+)", r.text)
            out["probes"][label] = {"httpStatus": r.status_code,
                                    "code": m.group(1) if m else None,
                                    "errMsg": e.group(1) if e else None}
        except Exception as ex:                              # noqa: BLE001
            out["probes"][label] = {"error": type(ex).__name__}
        time.sleep(S.SLEEP)
    return out


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
def discover_history_start(cli, cal, hist):
    """소스가 실제로 데이터를 주기 시작하는 첫 거래일을 **실측**한다(§3).

    "2007 이 안 나온다" 로 끝내면 어디부터 되는지를 모른다. 그러면 다음 라운드가
    다시 처음부터 뒤져야 한다. 그래서 경계를 이분탐색으로 한 번만 확정한다.
    비용은 log2(거래일수) ≈ 12콜 수준이다.

    이분탐색이 성립하는 전제: 시작일 이후로는 데이터가 연속 존재한다.
    그래서 경계 확정 뒤 **바로 다음 거래일 2개를 추가 확인**해 단발 구멍이
    아님을 검증한다. 검증이 깨지면 경계를 확정하지 않고 UNVERIFIED 로 남긴다.
    """
    out = {"method": "binary search over cached trading calendar",
           "calls": 0, "probes": [], "historyStartDate": None,
           "verified": False}
    ok_days = sorted(v["probeDate"] for v in (hist.get("byYear") or {}).values()
                     if v.get("rows", 0) > 0)
    if not ok_days:
        out["status"] = "NOT_MEASURABLE_NO_POSITIVE_PROBE"
        return out
    try:
        hi = cal.index(ok_days[0])
    except ValueError:
        out["status"] = "CALENDAR_MISMATCH"
        return out
    lo = 0                                   # 캘린더 시작 = 확실히 없는 쪽 후보

    def rows_at(i):
        items, st, meta = cli.page(bas_dt=cal[i], rows=1)
        out["calls"] += 1
        n = meta.get("totalCount", 0) if st == "OK" else -1
        out["probes"].append({"date": cal[i], "status": st, "totalCount": n})
        time.sleep(S.SLEEP)
        return n

    if rows_at(lo) > 0:                      # 캘린더 첫날부터 있으면 그게 시작
        out.update({"historyStartDate": cal[lo], "verified": True,
                    "status": "FULL_CALENDAR"})
        return out
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if rows_at(mid) > 0:
            hi = mid
        else:
            lo = mid
    start = cal[hi]
    # 단발 구멍이 아닌지 확인 — 시작 직후 2거래일도 데이터가 있어야 한다
    nxt = [cal[i] for i in (hi + 1, hi + 2) if i < len(cal)]
    contiguous = all(rows_at(cal.index(d)) > 0 for d in nxt)
    out.update({"historyStartDate": start,
                "lastDateWithoutData": cal[lo],
                "followUpDates": nxt,
                "verified": bool(contiguous),
                "status": "MEASURED" if contiguous else "UNVERIFIED_GAP"})
    return out


def probe_verdict(cred, hist, schema, dp, cc, source_alive):
    # historical_coverage 가 기록한 통제연도 결과를 쓴다. 필드가 없는 옛 evidence
    # 는 "행 하나라도 받았으면 인증은 실효했다" 로 보수적으로 되돌린다.
    auth_effective = hist.get("authEffective")
    if auth_effective is None:
        auth_effective = any(v.get("rows", 0) > 0
                             for v in (hist.get("byYear") or {}).values())
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
    elif not auth_effective:
        # 키는 있는데 통제연도조차 못 받았다 → 인증이 실효하지 않은 것이다.
        # 원인(키 오류 / 활용신청 미승인 / 포털 반영 지연)은 응답으로 구분되지
        # 않는다 — 게이트웨이가 무효키·빈키·정상키에 같은 코드를 준다.
        # 그래서 "키가 틀렸다" 고 단정하지 않고 "실효하지 않았다" 로만 적는다.
        verdict = "CREDENTIAL_NOT_EFFECTIVE"
    elif not hist.get("fullPeriodPass"):
        # 인증은 실효했고 최신 연도는 나오는데 hard 연도가 비었다 =
        # **소스의 실제 historical 한계**다. 이때만 소스 한계로 판정한다.
        #   구 이름 PROBE_PASS_2010_PLUS_ONLY 는 시작연도를 2010 으로 못박아
        #   실측(2020-01-02)과 어긋났다. 시작일은 이름이 아니라 필드로 남긴다.
        verdict = ("PROBE_PASS_PARTIAL_PERIOD_ONLY"
                   if any(v.get("rows") for v in hist.get("byYear", {}).values())
                   else "PROBE_FAIL_NO_HISTORY")
    elif not schema.get("tradedValueFieldPresent"):
        verdict = "NO_TRADED_VALUE"
    elif not dp.get("delistedHistorySupported"):
        verdict = "NO_DELISTED_HISTORY"
    elif failed:
        verdict = "PROBE_FAIL"
    else:
        verdict = "PROBE_PASS_FULL_PERIOD"
    # 인증이 실효하지 않았으면 데이터 축들은 "실패" 가 아니라 "미측정" 이다.
    # 둘을 같은 칸에 적으면 소스가 부적합한 것처럼 읽힌다.
    not_measured = ([n for n in failed if n not in ("CREDENTIAL",)]
                    if verdict == "CREDENTIAL_NOT_EFFECTIVE" else [])
    return {"checks": {n: ok for n, ok in checks},
            "failedChecks": failed,
            "notMeasuredChecks": not_measured,
            "verdict": verdict,
            "fullAcquisitionAllowed": verdict == "PROBE_PASS_FULL_PERIOD",
            "authEffective": bool(auth_effective),
            "historyStart": hist.get("historyStart"),
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
    if not hist.get("authEffective"):
        disc = auth_discrimination(True)
        m = re.search(r"API_ERROR:(\d+)", str(hist.get("controlYearStatus") or ""))
        ctrl_code = m.group(1) if m else None
        codes = {v.get("code") for v in disc["probes"].values() if v.get("code")}
        disc["controlYearCode"] = ctrl_code
        disc["gatewayDiscriminates"] = bool(
            ctrl_code and codes and ctrl_code not in codes)
        disc["conclusion"] = (
            "게이트웨이가 인증 상태를 구분한다 — 정상키 응답을 신뢰할 수 있다."
            if disc["gatewayDiscriminates"] else
            "무효키·빈키와 같은 코드다 — 응답으로 키 유효성도 소스 한계도 "
            "판별할 수 없다. 소스 부적합으로 단정하지 않는다.")
        hist["authDiscrimination"] = disc
        save("auth-discrimination", {**base, **disc})
    if hist.get("authEffective") and not hist.get("fullPeriodPass"):
        # 인증은 되는데 과거가 비었다 → 어디부터 되는지를 실측해 둔다(§3).
        hist["historyStart"] = discover_history_start(cli, cal, hist)
        save("history-start-probe", {**base, **hist["historyStart"]})
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
