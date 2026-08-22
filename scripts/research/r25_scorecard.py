#!/usr/bin/env python3
"""R25 factor scorecard + 최종 판정.

WABABA-CANONICAL-FACTOR-REDISCOVERY-R25

precommit 의 QUALIFICATION 을 **그대로** 적용한다. 여기서 기준을 만들거나
바꾸지 않는다(§18·§35). 억지로 승자를 만들지 않는다(§19).

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r25_precommit import FACTORS, HORIZONS, QUALIFICATION, SELECTION  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
PRIMARY_H = HORIZONS["primary"]


def L(n):
    return json.loads((RD / f"r25-{n}-latest.json").read_text(encoding="utf-8"))


def row(name, h, q, s, c, r, b, cc, dd, xx):
    hs = {f"{x}M": (h[name].get(f"{x}M") or {}).get("meanSpreadAnnPct")
          for x in PRIMARY_H + HORIZONS["secondary"]}
    prim = [hs[f"{x}M"] for x in PRIMARY_H]
    prim = [v for v in prim if v is not None]
    mono = (q[name].get("12M") or {}).get("monotonicity")
    roll = ((r[name].get("12M") or {}).get("overlapping") or {})
    ci_lo = (b[name] or {}).get("ci95LowPct")
    ci_hi = (b[name] or {}).get("ci95HighPct")
    p_pos = (b[name] or {}).get("pSpreadPositive")
    cohorts = (h[name].get("12M") or {}).get("cohorts") or 0
    ctrl = c[name]
    raw = ctrl.get("rawSpreadAnnPct")
    sz = ctrl.get("sizeMatchedSpreadAnnPct")
    ex = ctrl.get("exchangeMatchedSpreadAnnPct")
    sx = ctrl.get("sizeExchangeMatchedSpreadAnnPct")

    def same_sign(a, bb):
        return a is not None and bb is not None and (a > 0) == (bb > 0)

    return {
        "factor": name, "family": FACTORS[name]["family"],
        "direction": FACTORS[name]["direction"],
        "hypothesis": FACTORS[name]["hypothesis"],
        "referenceOnly": bool(FACTORS[name].get("referenceOnly")),
        "spreadAnnPct": hs, "cohorts12M": cohorts,
        "horizonsPositive": sum(1 for v in prim if v > 0),
        "horizonsNegative": sum(1 for v in prim if v < 0),
        "horizonsTested": len(prim),
        "monotonicity": mono,
        "subperiodPositive": s[name]["positive"],
        "subperiodTotal": s[name]["total"],
        "subperiodPositiveRatio": s[name]["positiveRatio"],
        "rollingPositiveRate": roll.get("positiveRate"),
        "rollingMedianSpreadAnnPct": roll.get("medianSpreadAnnPct"),
        "rollingP10Pct": roll.get("p10Pct"), "rollingP90Pct": roll.get("p90Pct"),
        "rawSpreadAnnPct": raw, "sizeMatchedSpreadAnnPct": sz,
        "exchangeMatchedSpreadAnnPct": ex, "sizeExchangeMatchedSpreadAnnPct": sx,
        "kospiSpreadAnnPct": (ctrl.get("exchange_KOSPI") or {}).get("meanSpreadAnnPct"),
        "kosdaqSpreadAnnPct": (ctrl.get("exchange_KOSDAQ") or {}).get("meanSpreadAnnPct"),
        "smallSpreadAnnPct": (ctrl.get("size_SMALL") or {}).get("meanSpreadAnnPct"),
        "midSpreadAnnPct": (ctrl.get("size_MID") or {}).get("meanSpreadAnnPct"),
        "largeSpreadAnnPct": (ctrl.get("size_LARGE") or {}).get("meanSpreadAnnPct"),
        "sizeControlSameSign": same_sign(raw, sz),
        "exchangeControlSameSign": same_sign(raw, ex),
        "sizeExchangeControlSameSign": same_sign(raw, sx),
        "ci95LowPct": ci_lo, "ci95HighPct": ci_hi, "pSpreadPositive": p_pos,
        "top1Pct": (cc[name] or {}).get("top1Pct"),
        "top3Pct": (cc[name] or {}).get("top3Pct"),
        "top5Pct": (cc[name] or {}).get("top5Pct"),
        "top10Pct": (cc[name] or {}).get("top10Pct"),
        "spreadExcludingTop3AnnPct": (cc[name] or {}).get(
            "spreadExcludingTop3AnnPct"),
        "concentratedSignal": bool((cc[name] or {}).get("concentratedSignal")),
        "topDelistingRatePct": dd[name]["TOP"]["delistingRatePct"],
        "bottomDelistingRatePct": dd[name]["BOTTOM"]["delistingRatePct"],
        "topLowPriceRatePct": dd[name]["TOP"]["lowPriceRatePct"],
        "topPersistentLossRatePct": dd[name]["TOP"]["persistentLossRatePct"],
        "tsrLimitationDiffPp": xx[name].get("diffPp"),
        "tsrLimitationExposed": bool(xx[name].get("exposed")),
    }


def classify(r):
    """precommit QUALIFICATION 그대로. 새 기준을 만들지 않는다."""
    Q = QUALIFICATION
    reasons = []
    sp12 = r["spreadAnnPct"].get("12M")
    if sp12 is None:
        return "UNRELIABLE", ["12M spread 없음"]

    # 1) UNRELIABLE 먼저
    if r["cohorts12M"] < Q["UNRELIABLE"]["minCohorts"]:
        reasons.append(f"유효 코호트 {r['cohorts12M']} < {Q['UNRELIABLE']['minCohorts']}")
    if r["tsrLimitationExposed"]:
        reasons.append("TSR_LIMITATION_EXPOSED")
    if r["concentratedSignal"]:
        reasons.append("CONCENTRATED_SIGNAL")
    if reasons:
        return "UNRELIABLE", reasons

    # 2) INVERTED
    inv = Q["INVERTED_SIGNAL"]
    if sp12 <= inv["maxAnnualSpreadPct"] and \
            r["horizonsNegative"] >= inv["horizonsNegative"]:
        return "INVERTED_SIGNAL", [
            f"12M spread {sp12}%p <= {inv['maxAnnualSpreadPct']}%p",
            f"{r['horizonsNegative']}/{r['horizonsTested']} horizon 음(-)",
            "사전 방향과 반대. 방향을 뒤집지 않는다(§7)."]

    # 3) STRONG
    S = Q["STRONG_SIGNAL"]
    checks = {
        "spread>=3.0%p": sp12 >= S["minAnnualSpreadPct"],
        "3/3 horizon 양(+)": r["horizonsPositive"] >= S["horizonsPositive"],
        "subperiod 과반 양(+)": (r["subperiodPositiveRatio"] or 0)
        >= S["minSubperiodPositiveRatio"],
        "rolling positive>=0.55": (r["rollingPositiveRate"] or 0)
        >= S["minRollingPositiveRate"],
        "size 통제 후 부호 유지": r["sizeControlSameSign"],
        "거래소 통제 후 부호 유지": r["exchangeControlSameSign"],
        "CI95 하한 > 0": (r["ci95LowPct"] is not None and r["ci95LowPct"] > 0),
        "집중 아님": not r["concentratedSignal"],
        "TSR 한계 노출 아님": not r["tsrLimitationExposed"],
        "monotonicity>=0.5": (r["monotonicity"] or 0) >= S["monotonicityMin"],
    }
    if all(checks.values()):
        return "STRONG_SIGNAL", [k for k in checks]
    failed = [k for k, v in checks.items() if not v]

    # 4) PROMISING
    P = Q["PROMISING_SIGNAL"]
    if (sp12 >= P["minAnnualSpreadPct"]
            and r["horizonsPositive"] >= P["horizonsPositive"]
            and (r["subperiodPositiveRatio"] or 0) >= P["minSubperiodPositiveRatio"]
            and (r["rollingPositiveRate"] or 0) >= P["minRollingPositiveRate"]):
        return "PROMISING_SIGNAL", ["STRONG 미충족: " + ", ".join(failed)]

    # 5) WEAK / NO
    if abs(sp12) >= Q["WEAK_SIGNAL"]["minAnnualSpreadPct"] \
            and r["horizonsPositive"] == r["horizonsTested"]:
        return "WEAK_SIGNAL", ["방향은 일관되나 통제·안정성 근거 부족: "
                               + ", ".join(failed)]
    return "NO_SIGNAL", [
        f"12M spread {sp12}%p · horizon 양(+) {r['horizonsPositive']}/"
        f"{r['horizonsTested']} — 방향이 일관되지 않거나 크기가 없다"]


def main() -> int:
    h, q, s = L("horizon-results"), L("quantile-results"), L("subperiod-results")
    c, r, b = L("size-exchange-controls"), L("rolling-cohorts"), L("bootstrap")
    cc, dd, xx = L("concentration"), L("distress"), L("tsr-limitation-exposure")
    H, Q, S = h["byFactor"], q["byFactor"], s["byFactor"]
    C_, R_, B_ = c["byFactor"], r["byFactor"], b["byFactor"]
    CC, DD, XX = cc["byFactor"], dd["byFactor"], xx["byFactor"]

    rows = []
    for name in FACTORS:
        rr = row(name, H, Q, S, C_, R_, B_, CC, DD, XX)
        v, why = classify(rr)
        rr["verdict"], rr["verdictWhy"] = v, why
        rows.append(rr)

    rows.sort(key=lambda x: -(x["spreadAnnPct"].get("12M") or -999))
    byv = {}
    for rr in rows:
        byv.setdefault(rr["verdict"], []).append(rr["factor"])

    primary = [rr["factor"] for rr in rows
               if rr["verdict"] == "STRONG_SIGNAL" and not rr["referenceOnly"]]
    secondary = [rr["factor"] for rr in rows
                 if rr["verdict"] == "PROMISING_SIGNAL" and not rr["referenceOnly"]]
    rejected = [rr["factor"] for rr in rows
                if rr["verdict"] in ("NO_SIGNAL", "WEAK_SIGNAL", "UNRELIABLE",
                                     "INVERTED_SIGNAL")]

    save = lambda n, o: (RD / f"r25-{n}-latest.json").write_text(  # noqa: E731
        json.dumps(o, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    save("factor-scorecard", {
        "task": "R25", "qualificationFromPrecommit": QUALIFICATION,
        "thresholdsUnchanged": True,
        "byVerdict": byv, "rows": rows})

    # ── §34 다음 작업 결정 규칙 ────────────────────────────────────────
    fams = {f: FACTORS[f]["family"] for f in primary}
    has_value = "VALUE" in fams.values()
    has_size = "SIZE" in fams.values()
    has_qual = "QUALITY" in fams.values()
    if not primary and not secondary:
        rule, nxt = "F", ("아무 factor 도 생존하지 않았다. 억지 조합 금지. "
                          "연구 질문 자체를 재평가한다.")
    elif has_value and has_qual:
        rule, nxt = "E", ("Value + Quality 결합 연구 — Founder 가 원래 물은 "
                          "'싸고 잘 버는 회사'.")
    elif len(primary) >= 2:
        rule, nxt = "B", (f"독립 PRIMARY 가 둘 이상({', '.join(primary)}). 두 factor "
                          "결합이 incremental alpha 를 만드는지 단일 combination 연구.")
    elif has_size and len(primary) == 1:
        rule, nxt = "C", ("SIZE 만 생존. small-cap premium 의 실제 거래가능성·"
                          "유동성을 먼저 검증한다.")
    elif has_qual and len(primary) == 1:
        rule, nxt = "D", "Quality 만 생존. long-horizon compounder portfolio 연구."
    elif len(primary) == 1:
        rule, nxt = "A", (f"PRIMARY 하나만 강하게 생존({primary[0]}). 해당 factor 의 "
                          "실행 가능한 portfolio construction 연구.")
    else:
        rule, nxt = "F", "PRIMARY 없음. SECONDARY 만으로 포트폴리오 연구를 시작하지 않는다."

    inverted = [rr for rr in rows if rr["verdict"] == "INVERTED_SIGNAL"]
    save("verdict", {
        "task": "R25",
        "PRIMARY_FACTOR": primary or "NONE",
        "SECONDARY_FACTOR": secondary or "NONE",
        "REJECTED_FACTOR": rejected,
        "byVerdict": byv,
        "selectionRule": SELECTION,
        "nextTaskRule": rule, "nextTask": nxt,
        "invertedFactors": [
            {"factor": rr["factor"], "hypothesis": rr["hypothesis"],
             "spread12MPct": rr["spreadAnnPct"]["12M"],
             "spread36MPct": rr["spreadAnnPct"].get("36M"),
             "spread60MPct": rr["spreadAnnPct"].get("60M")} for rr in inverted],
        "foundersQualityHypothesis": {
            "statement": "잘 버는 회사의 주가는 장기적으로 오른다(§23)",
            "testedWith": ["ROE", "EARNINGS_PERSISTENCE", "BPS_GROWTH"],
            "result": [{"factor": rr["factor"],
                        "verdict": rr["verdict"],
                        "spread": rr["spreadAnnPct"]} for rr in rows
                       if rr["family"] == "QUALITY"],
        },
        "noForcedWinner": SELECTION["noneRule"],
        "portfolioSearchDone": False,
        "combinationSearchDone": False,
    })
    print(json.dumps({"PRIMARY": primary or "NONE",
                      "SECONDARY": secondary or "NONE",
                      "byVerdict": byv, "nextRule": rule}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
