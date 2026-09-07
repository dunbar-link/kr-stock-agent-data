#!/usr/bin/env python3
"""R31 2020 전구간 교차검증 + acceptance gate.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

KRX OPEN API 2020 (비교 근거 전용 캐시) 와 기존 R30 공공데이터포털 2020 canonical 을
**표본이 아니라 전체 공통구간**에서 비교한다.

── gate 는 결과를 보기 전에 동결됐다 (r31_precommit) ──────────────
  공식 거래일 coverage        100%
  R27 eligible common key     >= 99.0%
  거래대금 exact match         >= 99.5%
  거래량   exact match         >= 99.5%
  종가     exact match         >= 99.5%
  systematic scale mismatch   0
  미달이면 stitching·R27 판정으로 넘어가지 않는다.

── 비교 규약 ──────────────────────────────────────────────────────
  key    (trade_date, market, 6자리 단축코드)
  값      문자열 비교 금지 — numeric 캐스팅 후 비교. 코드는 leading zero 보존.
  시장    KONEX 는 R31 spec 이 명시 제외한 시장이라 비교 대상 밖이다
          (제외 사실과 건수를 숨기지 않고 EXPLAINED 로 집계한다).
  단계    RAW 전체와 R27_ELIGIBLE 을 분리 보고한다.

안전: 읽기·계산 전용. 네트워크 0. write 는 evidence 파일 하나.
"""
from __future__ import annotations

import csv
import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r30_cache as K                                       # noqa: E402
import r31_acquire as A                                     # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
PIT = ROOT / "_cache" / "pit-snapshots"

MARKETS = ("KOSPI", "KOSDAQ")
SCALE_RATIOS = (10, 100, 1000, 0.1, 0.01, 0.001)
SCALE_TOL = 1e-6

GATES = {
    "officialTradingDayCoverage": 1.0,
    "r27EligibleCommonKeyCoverage": 0.99,
    "directTradeValueExactMatch": 0.995,
    "volumeExactMatch": 0.995,
    "closeExactMatch": 0.995,
    "systematicScaleMismatchMax": 0,
}


def eligible_universe():
    """R27 이 실제로 쓰는 종목 집합 = PIT 스냅샷 universe 의 합집합.

    새 universe 정의를 만들지 않는다 — R27/R25 가 이미 쓰는 스냅샷을 읽는다.
    """
    tick = set()
    for p in PIT.glob("*.csv.gz"):
        try:
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    t = str(r.get("ticker") or "").strip()
                    if t:
                        tick.add(t)
        except OSError:
            continue
    return tick


def _sec_type(ticker):
    """단축코드 관례 — 끝자리 0 이면 보통주, 그 외는 우선주/기타. 휴리스틱임을 명시."""
    return "COMMON" if ticker.endswith("0") else "PREFERRED_OR_OTHER"


def _scale_class(a, b):
    """systematic 배율 오류인가. 아니면 None."""
    if a is None or b is None or b == 0:
        return None
    r = a / b
    for s in SCALE_RATIOS:
        if abs(r - s) <= max(SCALE_TOL, abs(s) * 1e-6):
            return f"x{s}"
    return None


def compare(days):
    elig = eligible_universe()
    st = {stage: {
        "commonKeys": 0, "krxOnly": 0, "dgkOnly": 0,
        "tvMatch": 0, "tvMismatch": 0,
        "volMatch": 0, "volMismatch": 0,
        "closeMatch": 0, "closeMismatch": 0,
        "scaleMismatch": Counter(),
        "mismatchExamples": [],
    } for stage in ("RAW", "R27_ELIGIBLE")}

    by_market = defaultdict(lambda: {"common": 0, "tvMismatch": 0,
                                     "volMismatch": 0, "closeMismatch": 0})
    by_month = defaultdict(lambda: {"common": 0, "tvMismatch": 0})
    by_sec = defaultdict(lambda: {"common": 0, "tvMismatch": 0})

    explained = Counter()
    day_stat = {"expected": len(days), "bothPresent": 0,
                "krxMissing": [], "dgkMissing": []}
    krx_rows = dgk_rows = 0

    for d in days:
        krx = A.load_overlap_day(d)
        dgk = K.load_day(d)
        if krx is None:
            day_stat["krxMissing"].append(d)
        if dgk is None:
            day_stat["dgkMissing"].append(d)
        if krx is None or dgk is None:
            continue
        day_stat["bothPresent"] += 1

        # KONEX 는 R31 대상 시장이 아니다 — 비교 밖으로 빼되 건수를 남긴다.
        dgk_scope, dgk_out = {}, 0
        for t, r in dgk.items():
            if (r.get("market") or "") in MARKETS:
                dgk_scope[(r["market"], t)] = r
            else:
                dgk_out += 1
        explained["MARKET_DIFFERENCE_OUT_OF_SCOPE(KONEX 등)"] += dgk_out

        krx_scope = {(r.get("market") or "", t): r for t, r in krx.items()
                     if (r.get("market") or "") in MARKETS}
        krx_rows += len(krx_scope)
        dgk_rows += len(dgk_scope)

        kk, dk = set(krx_scope), set(dgk_scope)
        for stage in ("RAW", "R27_ELIGIBLE"):
            if stage == "RAW":
                a, b = kk, dk
            else:
                a = {x for x in kk if x[1] in elig}
                b = {x for x in dk if x[1] in elig}
            s = st[stage]
            common = a & b
            s["commonKeys"] += len(common)
            s["krxOnly"] += len(a - b)
            s["dgkOnly"] += len(b - a)

            for key in common:
                x, y = krx_scope[key], dgk_scope[key]
                mkt, tk = key
                if stage == "RAW":
                    by_market[mkt]["common"] += 1
                    by_month[d[:7]]["common"] += 1
                    by_sec[_sec_type(tk)]["common"] += 1
                for fld, mk_, mm in (("traded_value", "tvMatch", "tvMismatch"),
                                     ("volume", "volMatch", "volMismatch"),
                                     ("close", "closeMatch", "closeMismatch")):
                    xa, yb = x.get(fld), y.get(fld)
                    if xa is not None and yb is not None and float(xa) == float(yb):
                        s[mk_] += 1
                        continue
                    s[mm] += 1
                    sc = _scale_class(xa, yb)
                    if sc:
                        s["scaleMismatch"][f"{fld}:{sc}"] += 1
                    if stage == "RAW":
                        if fld == "traded_value":
                            by_market[mkt]["tvMismatch"] += 1
                            by_month[d[:7]]["tvMismatch"] += 1
                            by_sec[_sec_type(tk)]["tvMismatch"] += 1
                        elif fld == "volume":
                            by_market[mkt]["volMismatch"] += 1
                        else:
                            by_market[mkt]["closeMismatch"] += 1
                    if len(s["mismatchExamples"]) < 20:
                        s["mismatchExamples"].append({
                            "date": d, "market": mkt, "ticker": tk,
                            "field": fld, "krx": xa, "dataGoKr": yb,
                            "ratio": (round(xa / yb, 6)
                                      if (xa is not None and yb) else None)})

    for stage in st:
        st[stage]["scaleMismatch"] = dict(st[stage]["scaleMismatch"])
    return {"days": day_stat, "krxRowsInScope": krx_rows,
            "dgkRowsInScope": dgk_rows, "stages": st,
            "byMarket": dict(by_market), "byMonth": dict(by_month),
            "bySecurityType": dict(by_sec),
            "explainedOutOfScope": dict(explained),
            "eligibleUniverseSize": len(elig)}


def evaluate(cmp_):
    d = cmp_["days"]
    day_cov = (d["bothPresent"] / d["expected"]) if d["expected"] else 0.0
    raw, el = cmp_["stages"]["RAW"], cmp_["stages"]["R27_ELIGIBLE"]

    def rate(s, m, mm):
        tot = s[m] + s[mm]
        return (s[m] / tot) if tot else 0.0

    krx_keys = el["commonKeys"] + el["krxOnly"]
    dgk_keys = el["commonKeys"] + el["dgkOnly"]
    smaller = min(krx_keys, dgk_keys) or 1
    key_cov = el["commonKeys"] / smaller

    scale_total = sum(el["scaleMismatch"].values()) + sum(raw["scaleMismatch"].values())

    res = {
        "officialTradingDayCoverage": round(day_cov, 6),
        "r27EligibleCommonKeyCoverage": round(key_cov, 6),
        "directTradeValueExactMatch": round(rate(el, "tvMatch", "tvMismatch"), 6),
        "volumeExactMatch": round(rate(el, "volMatch", "volMismatch"), 6),
        "closeExactMatch": round(rate(el, "closeMatch", "closeMismatch"), 6),
        "systematicScaleMismatch": scale_total,
        "rawDirectTradeValueExactMatch": round(rate(raw, "tvMatch", "tvMismatch"), 6),
        "rawVolumeExactMatch": round(rate(raw, "volMatch", "volMismatch"), 6),
        "rawCloseExactMatch": round(rate(raw, "closeMatch", "closeMismatch"), 6),
    }
    checks = {
        "officialTradingDayCoverage": res["officialTradingDayCoverage"] >= GATES["officialTradingDayCoverage"],
        "r27EligibleCommonKeyCoverage": res["r27EligibleCommonKeyCoverage"] >= GATES["r27EligibleCommonKeyCoverage"],
        "directTradeValueExactMatch": res["directTradeValueExactMatch"] >= GATES["directTradeValueExactMatch"],
        "volumeExactMatch": res["volumeExactMatch"] >= GATES["volumeExactMatch"],
        "closeExactMatch": res["closeExactMatch"] >= GATES["closeExactMatch"],
        "systematicScaleMismatch": res["systematicScaleMismatch"] <= GATES["systematicScaleMismatchMax"],
    }
    passed = all(checks.values())
    return {"metrics": res, "gates": GATES, "checks": checks,
            "gatePassed": passed,
            "verdict": "PASS" if passed else "BLOCKED",
            "reasonClass": (None if passed
                            else "KRX_DATA_GO_KR_OVERLAP_VALIDATION_FAILED")}


def main() -> int:
    sc = A.scopes()
    days = sc["overlap"]
    cmp_ = compare(days)
    ev = evaluate(cmp_)
    out = {"task": "R31", "phase": "overlap-2020",
           "overlapRange": [days[0], days[-1]] if days else None,
           "comparison": cmp_, "evaluation": ev,
           "keyDefinition": ["trade_date", "market", "canonical_short_code(6)"],
           "comparedFields": ["direct traded value (ACC_TRDVAL vs trPrc)",
                              "volume (ACC_TRDVOL vs trqu)", "close"],
           "eligibleDefinition": "PIT 스냅샷 universe 합집합 (R27/R25 가 이미 쓰는 정의)",
           "fullPeriodNotSample": True}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r31-overlap-2020-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    m = ev["metrics"]
    print(f"verdict: {ev['verdict']}")
    print(f"days both present: {cmp_['days']['bothPresent']}/{cmp_['days']['expected']}")
    print(f"RAW      common={cmp_['stages']['RAW']['commonKeys']} "
          f"krxOnly={cmp_['stages']['RAW']['krxOnly']} "
          f"dgkOnly={cmp_['stages']['RAW']['dgkOnly']}")
    print(f"ELIGIBLE common={cmp_['stages']['R27_ELIGIBLE']['commonKeys']} "
          f"krxOnly={cmp_['stages']['R27_ELIGIBLE']['krxOnly']} "
          f"dgkOnly={cmp_['stages']['R27_ELIGIBLE']['dgkOnly']}")
    for k in ("officialTradingDayCoverage", "r27EligibleCommonKeyCoverage",
              "directTradeValueExactMatch", "volumeExactMatch",
              "closeExactMatch", "systematicScaleMismatch"):
        print(f"  {k:<32} {m[k]}  gate={GATES.get(k, GATES.get(k + 'Max'))}"
              f"  {'OK' if ev['checks'][k] else 'FAIL'}")
    print(f"  raw tv/vol/close match: {m['rawDirectTradeValueExactMatch']} / "
          f"{m['rawVolumeExactMatch']} / {m['rawCloseExactMatch']}")
    print(f"  explained out-of-scope: {cmp_['explainedOutOfScope']}")
    return 0 if ev["gatePassed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
