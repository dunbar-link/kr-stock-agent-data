#!/usr/bin/env python3
"""R25 factor 계산 + coverage.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

precommit 에 고정한 정의만 계산한다(§2·§5). 정의를 여기서 바꾸지 않는다.
과거 창을 쓰는 factor 는 **과거 스냅샷만** 본다 — 미래정보 금지(§4).

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r25_engine import load_snapshots  # noqa: E402
from r25_precommit import ELIGIBILITY, FACTORS  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

FACTORS_VERSION = "r25-factors-1"


def _pos(x):
    return x is not None and x > 0


def _pct_ranks(vals):
    """{key: value} → {key: 백분위(0~1)}. 동점은 ticker 오름차순으로 결정적."""
    items = sorted(vals.items(), key=lambda kv: (kv[1], kv[0]))
    n = len(items)
    if n <= 1:
        return {k: 0.5 for k, _ in items}
    return {k: i / (n - 1) for i, (k, _) in enumerate(items)}


def compute(snaps, dates):
    """{date: {ticker: {factor: value}}} + 원자료 참조용 필드."""
    pos = {d: i for i, d in enumerate(dates)}
    out, meta = {}, {}
    for d in dates:
        rows = snaps.get(d) or {}
        i = pos[d]
        vals, mrows = {}, {}
        for t, r in rows.items():
            close, mcap = r.get("close"), r.get("marketCap")
            if not _pos(close) or not _pos(mcap) or not _pos(r.get("shares")):
                continue
            f = {}
            pbr, per = r.get("PBR"), r.get("PER")
            eps, bps = r.get("EPS"), r.get("BPS")
            if _pos(pbr):
                f["BM"] = 1.0 / pbr
            if _pos(per):
                f["EY"] = 1.0 / per
            if eps is not None and _pos(bps):
                f["ROE"] = eps / bps
            f["SIZE_SMALL"] = -math.log(mcap)
            if r.get("DIV") is not None:
                f["DIVIDEND_YIELD"] = r["DIV"]

            # ── 과거 창 factor: 과거 스냅샷만 본다(§4) ──────────────
            lo = max(0, i - FACTORS["EARNINGS_PERSISTENCE"]["window"])
            hist = [snaps.get(dates[k], {}).get(t) for k in range(lo, i)]
            eps_obs = [h["EPS"] for h in hist if h and h.get("EPS") is not None]
            if len(eps_obs) >= FACTORS["EARNINGS_PERSISTENCE"]["minObs"]:
                f["EARNINGS_PERSISTENCE"] = sum(
                    1 for x in eps_obs if x > 0) / len(eps_obs)
            dps_obs = [h["DPS"] for h in hist if h and h.get("DPS") is not None]
            if len(dps_obs) >= FACTORS["DIVIDEND_DISCIPLINE"]["minObs"]:
                f["DIVIDEND_DISCIPLINE"] = sum(
                    1 for x in dps_obs if x > 0) / len(dps_obs)
            w = FACTORS["BPS_GROWTH"]["window"]
            if i - w >= 0 and _pos(bps):
                old = (snaps.get(dates[i - w], {}).get(t) or {}).get("BPS")
                if _pos(old):
                    f["BPS_GROWTH"] = bps / old - 1.0

            if f:
                vals[t] = f
                mrows[t] = {"market": r.get("market"), "close": close,
                            "marketCap": mcap, "PBR": pbr, "PER": per,
                            "EPS": eps, "BPS": bps, "DPS": r.get("DPS")}

        # ── 합성 legacy factor: 같은 달 횡단면 순위 합 ────────────────
        for name, a, b in (("MAGIC_FORMULA_LEGACY", "EY", "ROE"),
                           ("BM_ROE_LEGACY", "BM", "ROE")):
            ra = _pct_ranks({t: v[a] for t, v in vals.items() if a in v})
            rb = _pct_ranks({t: v[b] for t, v in vals.items() if b in v})
            for t in ra:
                if t in rb:
                    vals[t][name] = ra[t] + rb[t]

        out[d] = vals
        meta[d] = mrows
    return out, meta


def coverage(fvals, snaps, dates):
    per_factor = {}
    for name in FACTORS:
        months, counts = 0, []
        first, last = None, None
        for d in dates:
            n = sum(1 for v in (fvals.get(d) or {}).values() if name in v)
            tot = len(snaps.get(d) or {})
            if n:
                months += 1
                counts.append((d, n, tot))
                if first is None:
                    first = d
                last = d
        elig = [c for _, c, _ in counts]
        usable = [c for c in elig if c >= ELIGIBILITY["minUniversePerMonth"]]
        per_factor[name] = {
            "family": FACTORS[name]["family"],
            "direction": FACTORS[name]["direction"],
            "formula": FACTORS[name]["formula"],
            "referenceOnly": bool(FACTORS[name].get("referenceOnly")),
            "monthsWithData": months,
            "monthsUsable": len(usable),
            "firstMonth": first, "lastMonth": last,
            "medianEligible": (sorted(elig)[len(elig) // 2] if elig else 0),
            "minEligible": min(elig) if elig else 0,
            "maxEligible": max(elig) if elig else 0,
            "medianCoveragePct": (
                round(100.0 * sorted(c / t for _, c, t in counts if t)[
                    len(counts) // 2], 2) if counts else 0.0),
        }
    return per_factor


def main() -> int:
    from r16_audit import contiguous_span
    import r16_canonical as C
    cap = C.load_capital_series()
    dates = contiguous_span(sorted(cap))
    snaps = load_snapshots(start=dates[0], end=dates[-1])
    fvals, _meta = compute(snaps, dates)
    cov = coverage(fvals, snaps, dates)
    out = {"task": "R25", "factorsVersion": FACTORS_VERSION,
           "months": len(dates), "period": {"start": dates[0], "end": dates[-1]},
           "minUniversePerMonth": ELIGIBILITY["minUniversePerMonth"],
           "noImputation": ELIGIBILITY["noImputation"],
           "byFactor": cov}
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r25-factor-coverage-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"months": len(dates),
                      "factors": {k: v["monthsUsable"] for k, v in cov.items()}},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
