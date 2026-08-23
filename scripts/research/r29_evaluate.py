#!/usr/bin/env python3
"""R29 소스 교차검증 · 상폐종목 · scorecard · 최종 판정.

WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29

R27 이 남긴 111일 캐시를 reference 로 삼아 새 후보의 거래량·거래대금이 같은
시장사실을 돌려주는지 실측한다(§16·§17). 캐시는 삭제하지 않는다.

안전: 소량 실호출 + 계산. 전체 수집 0. 결제 0. env 변경 0.
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

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r29_probe import NAVER_SISE, TIMEOUT, UA, save  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "krx-liquidity"
SNAP = ROOT / "_cache" / "pit-snapshots"

# §17 사전 고정 허용오차 — 결과를 보고 바꾸지 않는다.
TOLERANCE = {
    "volumeRelErrMax": 0.0,
    "volumeRule": "거래량은 원칙적으로 완전 일치해야 한다.",
    "tradedValueRelErrMax": 0.05,
    "tradedValueRule": ("거래대금은 source semantics(VWAP vs 종가)에 따라 차이가 "
                        "날 수 있다. 5% 이내는 정상 해석, 초과는 원인분리."),
}


def naver_daily(ticker, start, end):
    """siseJson 1콜. 반환 [(date, close_adj, volume)]."""
    r = requests.get(NAVER_SISE, params={
        "symbol": ticker, "requestType": "1", "startTime": start,
        "endTime": end, "timeframe": "day"}, timeout=TIMEOUT,
        headers={"User-Agent": UA})
    if r.status_code != 200:
        return None, f"HTTP_{r.status_code}"
    rows = re.findall(r"\[([^\[\]]+)\]", r.text or "")
    if not rows:
        return [], "EMPTY"
    out = []
    for row in rows[1:]:
        parts = [p.strip().strip("'\"") for p in row.split(",")]
        if len(parts) < 6 or not parts[0].isdigit():
            continue
        try:
            out.append((f"{parts[0][:4]}-{parts[0][4:6]}-{parts[0][6:]}",
                        float(parts[4]), float(parts[5])))
        except ValueError:
            continue
    return out, "OK"


def load_cache_day(d):
    p = CACHE / f"{d}.csv.gz"
    if not p.exists():
        return None
    out = {}
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                out[r["ticker"]] = {"close": float(r["close"]),
                                    "volume": float(r["volume"]),
                                    "tradedValue": float(r["tradedValue"]),
                                    "marketCap": float(r["marketCap"])}
            except (TypeError, ValueError):
                continue
    return out


def find_delisted():
    """2007 스냅샷에 있고 최신 스냅샷에 없는 종목 = 이후 상장폐지."""
    def tickers(p):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            return {r["ticker"] for r in csv.DictReader(fh)}
    snaps = sorted(SNAP.glob("*.csv.gz"))
    old = [p for p in snaps if p.name.startswith("2007")]
    if not old:
        return []
    gone = sorted(tickers(old[0]) - tickers(snaps[-1]))
    return gone[:3]


def main() -> int:
    days = sorted(p.name[:-7] for p in CACHE.glob("*.csv.gz"))
    ref_days = [d for d in days if d >= "2007-01-01"][:20]   # §15 최소 20 거래일
    tickers = ["005930", "005935", "000660"]

    # ── §16 R27 캐시 교차검증 ─────────────────────────────────────────
    cmp_rows, calls = [], 0
    if ref_days:
        s, e = ref_days[0].replace("-", ""), ref_days[-1].replace("-", "")
        for t in tickers:
            nav, st = naver_daily(t, s, e)
            calls += 1
            if st != "OK" or not nav:
                cmp_rows.append({"ticker": t, "status": st, "compared": 0})
                continue
            navmap = {d: (c, v) for d, c, v in nav}
            ok = vdiff = tvdiff = 0
            verrs, tverrs, samples = [], [], []
            for d in ref_days:
                cd = load_cache_day(d)
                if not cd or t not in cd or d not in navmap:
                    continue
                kc, kv, ktv = cd[t]["close"], cd[t]["volume"], cd[t]["tradedValue"]
                nc, nv = navmap[d]
                ok += 1
                ve = abs(nv - kv) / kv if kv else None
                if ve is not None:
                    verrs.append(ve)
                    vdiff += (ve > TOLERANCE["volumeRelErrMax"])
                # 거래대금 = KRX 공식값 vs (KRX 종가 × NAVER 거래량) 근사
                approx = kc * nv
                te = abs(approx - ktv) / ktv if ktv else None
                if te is not None:
                    tverrs.append(te)
                    tvdiff += (te > TOLERANCE["tradedValueRelErrMax"])
                if len(samples) < 2:
                    samples.append({"date": d, "krxClose": kc, "naverClose": nc,
                                    "krxVolume": kv, "naverVolume": nv,
                                    "krxTradedValue": ktv,
                                    "closeXVolumeApprox": approx})
            cmp_rows.append({
                "ticker": t, "status": "OK", "compared": ok,
                "volumeExactMatchPct": round(
                    100.0 * (ok - vdiff) / ok, 2) if ok else None,
                "volumeMedianRelErr": round(statistics.median(verrs), 8)
                if verrs else None,
                "tradedValueApproxMedianRelErr": round(
                    statistics.median(tverrs), 5) if tverrs else None,
                "tradedValueApproxWithinTolerancePct": round(
                    100.0 * (ok - tvdiff) / ok, 2) if ok else None,
                "closeAdjusted": bool(samples and
                                      abs(samples[0]["naverClose"]
                                          - samples[0]["krxClose"]) > 1),
                "samples": samples})
            time.sleep(0.4)

    save("r27-cache-crosscheck", {
        "task": "R29", "tolerance": TOLERANCE,
        "referenceDays": len(ref_days),
        "referenceRange": [ref_days[0], ref_days[-1]] if ref_days else None,
        "tickers": tickers, "byTicker": cmp_rows, "calls": calls,
        "cachePreserved": True,
        "note": ("R27 캐시는 reference evidence 전용이며 대량수집 재개 근거로 "
                 "쓰지 않는다(§16).")})

    # ── §18 상폐종목 coverage ─────────────────────────────────────────
    delisted = find_delisted()
    dl_rows, dcalls = [], 0
    for t in delisted[:2]:
        nav, st = naver_daily(t, "20070102", "20070131")
        dcalls += 1
        dl_rows.append({"ticker": t, "status": st,
                        "rows": len(nav) if nav else 0,
                        "hasHistory": bool(nav)})
        time.sleep(0.4)
    save("delisted-coverage", {
        "task": "R29", "sampledDelistedTickers": delisted,
        "probed": dl_rows, "calls": dcalls,
        "why": ("현재 상장 종목만 지원하는 source 는 R27 전체 연구에 부적합하다. "
                "상폐종목 과거 거래량이 안 나오면 SURVIVORSHIP_LIQUIDITY_BIAS(§18)."),
        "naverSurvivorshipBias": not any(r["hasHistory"] for r in dl_rows)
        if dl_rows else None})

    # ── §8 historical coverage 요약 ───────────────────────────────────
    dgk = json.loads((RD / "r29-data-go-kr-probe-latest.json")
                     .read_text(encoding="utf-8"))
    krx = json.loads((RD / "r29-krx-openapi-probe-latest.json")
                     .read_text(encoding="utf-8"))
    nav_p = json.loads((RD / "r29-free-alternative-probe-latest.json")
                       .read_text(encoding="utf-8"))["candidates"][0]
    save("historical-coverage", {
        "task": "R29",
        "requiredRange": {"start": "2007-01-02", "end": "2026-08-03",
                          "tradingDays": 4640},
        "byCandidate": {
            "PUBLIC_DATA_PORTAL_FSC": {
                "startVerified": None, "endVerified": None,
                "status": "UNVERIFIED_NO_CREDENTIAL",
                "endpointReachable": dgk.get("endpointReachable"),
                "probeHttpStatus": (dgk.get("unauthenticatedProbe") or {}).get(
                    "httpStatus"),
                "why": "serviceKey 부재로 연도별 실측 불가. 신규 발급은 §21 게이트."},
            "KRX_OFFICIAL_OPEN_API": {
                "documentedStart": krx.get("documentedStartDate"),
                "status": krx.get("status"),
                "gap": krx.get("gap2007to2009")},
            "NAVER_SISE": {
                "startVerified": "2007-01-02" if nav_p.get("reached2007") else None,
                "volume": nav_p.get("hasVolumeColumn"),
                "tradedValue": nav_p.get("hasTradedValueColumn"),
                "priceIsAdjusted": True,
                "termsRisk": nav_p.get("termsRisk")}}})

    save("cross-source-reconciliation", {
        "task": "R29",
        "performed": bool(cmp_rows),
        "pairs": ["NAVER_SISE vs R27_KRX_CACHE"],
        "notPerformed": ["PUBLIC_DATA_PORTAL (credential 부재)",
                         "KRX_OFFICIAL_OPEN_API (credential 부재)"],
        "byTicker": cmp_rows,
        "stitchingRequired": ("KRX Open API 단독은 2010-01-04 시작이라 2007~2009 "
                              "구간을 다른 공식 source 로 이어야 한다(§14·§15)."),
        "stitchingFeasible": "UNVERIFIED — 두 공식 source 모두 credential 부재"})

    print(json.dumps({
        "crosscheckTickers": len(cmp_rows),
        "volumeExactMatch": [r.get("volumeExactMatchPct") for r in cmp_rows],
        "tradedValueApproxErr": [r.get("tradedValueApproxMedianRelErr")
                                 for r in cmp_rows],
        "delistedProbed": len(dl_rows),
        "delistedHasHistory": [r["hasHistory"] for r in dl_rows],
        "calls": calls + dcalls}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
