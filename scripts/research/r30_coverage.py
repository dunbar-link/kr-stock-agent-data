#!/usr/bin/env python3
"""R30 coverage 측정 — 전체수집 후 R27 gate 를 통과하는지 본다.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

threshold 는 R27 precommit 을 **그대로** 쓴다(§16). 연도별 90% 이상 coverage,
최소 15개 연도. 결과가 안 좋다고 기준을 바꾸지 않는다.

여기서 재는 것은 "받은 날/받아야 할 날" 과 그 날들의 시장·상폐 구성이다.
eligible 종목 대비 연도별 coverage 는 R27 자신의 분석기가 계산한다 —
같은 계산을 두 번 구현해 서로 다른 답이 나오게 만들지 않는다(재사용, §16).

미달이면 원인을 분리한다(§17): API historical limitation / ticker coverage /
delisted coverage / rate limit / mapping.

안전: 읽기·계산 전용. 네트워크 0. write 는 evidence 파일 하나.
"""
from __future__ import annotations

import csv
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r30_cache as K                                      # noqa: E402
from r27_precommit import COVERAGE                         # noqa: E402
from r30_acquire import required_days                      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
PIT = ROOT / "_cache" / "pit-snapshots"


def _latest_pit_tickers():
    days = sorted(p.name[:-7] for p in PIT.glob("*.csv.gz"))
    if not days:
        return set()
    with gzip.open(PIT / f"{days[-1]}.csv.gz", "rt", encoding="utf-8") as fh:
        return {r["ticker"] for r in csv.DictReader(fh)}


def scan_cache(days):
    """공식 캐시를 훑어 시장·상폐·결측 구성을 센다. 결측과 0 을 구분한다."""
    live = _latest_pit_tickers()
    per_year = {}
    markets, quality = {}, {}
    delisted_rows = 0
    total_rows = 0
    vol_missing = tv_missing = zero_vol = 0
    for d in days:
        rows = K.load_day(d)
        if rows is None:
            continue
        y = d[:4]
        e = per_year.setdefault(y, {"days": 0, "rows": 0, "tickers": set()})
        e["days"] += 1
        for t, r in rows.items():
            total_rows += 1
            e["rows"] += 1
            e["tickers"].add(t)
            m = r.get("market") or "UNKNOWN"
            markets[m] = markets.get(m, 0) + 1
            q = r.get("quality_flag") or "OK"
            quality[q] = quality.get(q, 0) + 1
            if r.get("volume") is None:
                vol_missing += 1
            elif r["volume"] == 0:
                zero_vol += 1
            if r.get("traded_value") is None:
                tv_missing += 1
            if live and t not in live:
                delisted_rows += 1
    for y, e in per_year.items():
        e["tickers"] = len(e["tickers"])
    return {"perYear": per_year, "marketCounts": markets,
            "qualityFlagCounts": quality, "rows": total_rows,
            "delistedRows": delisted_rows,
            "delistedRowPct": round(100.0 * delisted_rows / total_rows, 3)
            if total_rows else 0.0,
            "volumeMissingRows": vol_missing,
            "tradedValueMissingRows": tv_missing,
            "zeroVolumeRows": zero_vol,
            "missingVsZeroSeparated": True}


def cause_separation(need, have, scan, progress):
    """§17 — 미달 원인을 뭉뚱그리지 않고 나눈다."""
    causes = []
    # 수집을 시작조차 못했으면 그것이 원인이다. 연도별 공백을 API 한계로
    # 부르면 원인을 틀리게 기록하는 것이다(§17).
    if progress.get("status") == "NOT_STARTED_PROBE_GATE":
        return [{"cause": "ACQUISITION_NOT_STARTED",
                 "detail": f"probe gate 미통과({progress.get('probeVerdict')}) — "
                           f"공식 소스 실호출 0. 아래 연도 공백은 아직 "
                           f"API 한계로 판정할 근거가 없다."}]
    missing_years = sorted({d[:4] for d in need if d not in have})
    early = [y for y in missing_years if int(y) < 2010]
    if early:
        causes.append({"cause": "API_HISTORICAL_LIMITATION",
                       "detail": f"2010 이전 미확보 연도 {early}"})
    # 키가 있는데 값이 None 인 경우가 있다 — 기본값만으로는 못 막는다.
    if (progress.get("aborted") or "").startswith("DAILY_QUOTA") or \
       (progress.get("rateLimitHits") or 0) > 0:
        causes.append({"cause": "RATE_LIMIT",
                       "detail": f"quota/rate 이벤트 "
                                 f"{progress.get('rateLimitHits', 0)} · "
                                 f"aborted={progress.get('aborted')}"})
    if progress.get("failedDays", 0) > 0:
        causes.append({"cause": "FETCH_FAILURE",
                       "detail": f"실패일 {progress.get('failedDays')}"})
    if scan.get("delistedRows", 0) == 0 and scan.get("rows", 0) > 0:
        causes.append({"cause": "DELISTED_COVERAGE",
                       "detail": "상폐 종목 row 0 — survivorship liquidity bias 위험"})
    if scan.get("tradedValueMissingRows", 0) > 0.05 * max(scan.get("rows", 1), 1):
        causes.append({"cause": "MAPPING",
                       "detail": "거래대금 결측 비율 5% 초과 — 필드 매핑 확인"})
    if not causes and len(have) < len(need):
        causes.append({"cause": "TICKER_COVERAGE",
                       "detail": "일자는 받았으나 종목 coverage 부족"})
    return causes


def stitching_options(need):
    """threshold 를 건드리지 않고 gate 를 넘을 수 있는 조합을 산술로만 본다.

    기준을 바꾸는 것이 아니다. R27 이 이미 정해 둔 "연도별 90% + 최소 15개
    연도" 를 그대로 두고, 어느 시작일부터면 15개 연도가 확보되는지 센다.
    시작일은 실측(공식 API)과 문서(KRX Open API) 근거를 각각 표기한다.
    """
    opts = []
    for start, label, basis in (
            (_history_start() or "2020-01-02",
             "PUBLIC_DATA_PORTAL 단독", "실측 (이분탐색 확정)"),
            ("2010-01-04", "+ KRX_OFFICIAL_OPEN_API stitching",
             "R29 문서 근거 — 실측 아님(AUTH_KEY 부재)"),
            ("2007-01-02", "R27 요구 전체 구간", "R27 precommit")):
        days = [d for d in need if d >= start]
        years = sorted({d[:4] for d in days})
        opts.append({"startDate": start, "label": label, "basis": basis,
                     "days": len(days), "years": len(years),
                     "meetsMinYears": len(years) >= COVERAGE["minYearsCovered"]})
    return {"minYearsRequired": COVERAGE["minYearsCovered"],
            "thresholdChanged": False,
            "options": opts,
            "note": ("기준을 낮춘 것이 아니라 원래 기준으로 센 것이다. "
                     "2007~2009 를 못 채워도 15개 연도는 충족될 수 있다.")}


def _history_start():
    p = RD / "r30-history-start-probe-latest.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None
    return d.get("historyStartDate") if d.get("verified") else None


def main() -> int:
    need, cal_n, dec_n = required_days()
    have = K.have_days() | K.r27_have_days()      # R27 이 실제로 읽는 합집합
    official = K.have_days()
    got = [d for d in need if d in have]
    scan = scan_cache(sorted(official & set(need)))

    prog_path = RD / "r30-acquisition-progress-latest.json"
    progress = {}
    if prog_path.exists():
        try:
            progress = json.loads(prog_path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            progress = {}

    per_year_days = {}
    for d in need:
        y = d[:4]
        e = per_year_days.setdefault(y, {"required": 0, "have": 0})
        e["required"] += 1
        if d in have:
            e["have"] += 1
    for y, e in per_year_days.items():
        e["coveragePct"] = round(100.0 * e["have"] / e["required"], 3) \
            if e["required"] else 0.0
        e["meetsThreshold"] = e["coveragePct"] >= COVERAGE["minCoveragePctPerYear"]

    years_ok = [y for y, e in per_year_days.items() if e["meetsThreshold"]]
    day_cov = round(100.0 * len(got) / len(need), 3) if need else 0.0
    gate_pass = (len(years_ok) >= COVERAGE["minYearsCovered"]
                 and day_cov >= COVERAGE["minCoveragePctPerYear"])

    out = {"task": "R30",
           "thresholdsFrom": "R27 precommit — 변경 없음(§16)",
           "minCoveragePctPerYear": COVERAGE["minCoveragePctPerYear"],
           "minYearsCovered": COVERAGE["minYearsCovered"],
           "thresholdChanged": False,
           "requiredDays": len(need),
           "daysHave": len(got),
           "daysFromOfficialSource": len([d for d in need if d in official]),
           "daysInheritedFromR27": len([d for d in need
                                        if d in K.r27_have_days()]),
           "dayCoveragePct": day_cov,
           "annualDayCoverage": dict(sorted(per_year_days.items())),
           "yearsMeetingThreshold": sorted(years_ok),
           "yearsMeetingThresholdCount": len(years_ok),
           "cacheScan": scan,
           "coverageGatePreCheckPass": gate_pass,
           "r27AnalysisAllowed": gate_pass,
           "note": ("eligible 종목 대비 연도별 coverage 는 R27 분석기가 계산한다 "
                    "— 같은 계산을 두 번 구현하지 않는다(§16)."),
           "sourceHistoryStart": _history_start(),
           "stitching": stitching_options(need),
           "causes": [] if gate_pass else
           cause_separation(need, have, scan, progress)}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r30-final-coverage-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    print(json.dumps({k: out[k] for k in
                      ("requiredDays", "daysHave", "dayCoveragePct",
                       "yearsMeetingThresholdCount",
                       "coverageGatePreCheckPass")}, ensure_ascii=False))
    return 0 if gate_pass else 7


if __name__ == "__main__":
    raise SystemExit(main())
