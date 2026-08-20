#!/usr/bin/env python3
"""R10 §11 — 성과 귀속: 성과가 BM 종목선정에서 왔는가?

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

사양준수(spec-conformant) 결과에 대해 초과수익의 출처를 분해한다.
질문 하나만 본다: **BM 이라서 벌었나, 다른 노출 때문에 벌었나?**

통제군은 전부 candidate 와 동일 구조(시작일·투입 스케줄·종목수·보유기간·교체주기·
비용·상폐 가정)이고 **selection factor 만** 제거한다.
  RANDOM              무작위
  SIZE_MATCHED        시총 10분위 일치
  MARKET_MATCHED      거래소 일치
  SIZE_MARKET_MATCHED 거래소 × 시총 5분위 동시 일치  ← 가장 엄격
그리고 benchmark 축으로 시장하락 회피·현금비중 효과를 별도 정량화한다.

새 parameter 탐색 0 (§12) · frozen 정의 변경 0.
안전: 계산 전용 · 네트워크 0 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r9_validate as V  # noqa: E402
import r10_validate as R10V  # noqa: E402
from r8_portfolio import RankCache, ew_universe_index  # noqa: E402
from r9_precommit import CONTROL_SEEDS, FROZEN  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CONTROLS = ("RANDOM", "SIZE_MATCHED", "MARKET_MATCHED", "SIZE_MARKET_MATCHED")


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def q(xs, p):
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    pos = p * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def main() -> int:
    R10V.install_engine()
    sn, nm = V.load_snapshots(), V.load_names()
    ds = contiguous_span(sorted(sn))
    cache = RankCache(sn, nm)
    idx = ew_universe_index(cache, ds)
    print(f"[r10] attribution {ds[0]} ~ {ds[-1]}", file=sys.stderr)

    _, base = V.run_case(cache, ds, idx, **V.BASE_KW)
    cand = pct(base["excess"])
    cand_cagr = pct(base["twrCagr"])

    controls = {}
    for mode in CONTROLS:
        ex, cg = [], []
        for s in CONTROL_SEEDS:
            _, e = V.run_case(cache, ds, idx, selection=mode, seed=s, **V.BASE_KW)
            if e:
                ex.append(pct(e["excess"]))
                cg.append(pct(e["twrCagr"]))
        better = sum(1 for x in ex if x < cand)
        controls[mode] = {
            "seeds": len(ex), "meanExcessPct": round(sum(ex) / len(ex), 2),
            "medianExcessPct": round(q(ex, 0.5), 2),
            "p10ExcessPct": round(q(ex, 0.10), 2),
            "p90ExcessPct": round(q(ex, 0.90), 2),
            "meanCagrPct": round(sum(cg) / len(cg), 2),
            "candidateExcessPct": cand,
            "candidateMinusControlMeanPct": round(cand - sum(ex) / len(ex), 2),
            "candidatePercentile": round(100 * better / len(ex), 1),
            "controlsBeatingCandidate": sum(1 for x in ex if x >= cand),
        }
        print(f"[r10]   {mode}: mean {controls[mode]['meanExcessPct']:+.2f}%p "
              f"pctile {controls[mode]['candidatePercentile']}", file=sys.stderr)

    core = json.loads((RD / "r10-core-latest.json").read_text(encoding="utf-8"))
    bm = json.loads((RD / "r10-benchmarks-latest.json").read_text(encoding="utf-8"))
    ca = json.loads((RD / "r10-cashaudit-latest.json").read_text(encoding="utf-8"))
    conc = core["concentration"]

    strict = controls["SIZE_MARKET_MATCHED"]
    valid_pbr = next((r for r in bm["rows"] if r["benchmark"] == "EW_VALID_PBR_UNIVERSE"),
                     None)

    out = {
        "schema": "wababa-r8-frozen-spec-forensic-reconciliation-r10/attribution@1",
        "question": "성과가 BM 종목선정에서 왔는가?",
        "candidate": {"cagrPct": cand_cagr, "excessVsFairEwPct": cand},
        "matchedControls": controls,
        "controlNote": ("모든 통제군은 candidate 와 동일 구조이고 selection 만 제거했다. "
                        "무작위 최고값과 비교하지 않고 분포와 percentile 로 본다."),
        "benchmarkAxis": {
            "rows": bm["rows"],
            "worst": bm["worstCaseBenchmark"],
            "vsValidPbrUniversePct": valid_pbr and valid_pbr["candidateExcessPct"],
            "missingPbrExclusionEffectPct": (
                round(cand - valid_pbr["candidateExcessPct"], 2)
                if valid_pbr and valid_pbr["candidateExcessPct"] is not None else None),
        },
        "cashAndTimingAxis": {
            "excessVsFairPct": ca["fullPeriod"]["excessVsFairPct"],
            "excessVsCashMatchedPct": ca["fullPeriod"]["excessVsCashMatchedPct"],
            "cashTimingContributionPct": round(
                ca["fullPeriod"]["excessVsFairPct"]
                - ca["fullPeriod"]["excessVsCashMatchedPct"], 2),
            "avgCashRatioPct": ca["cashProfile"]["avgCashRatioPct"],
            "first24mCorrWithMarket": (ca.get("entryScheduleDefect") or {}).get(
                "corr_first24mMarket_vs_5yExcess"),
            "reading": ("현금비중 일치 benchmark 대비 초과수익이 남으면 시장하락 회피·"
                        "현금효과로는 설명되지 않는다는 뜻이다."),
        },
        "exchangeAxis": core["marketSegments"],
        "sizeAxis": core["sizeControls"],
        "pnlByMarket": {k: {"basisSharePct": v["basisSharePct"],
                            "pnlSharePct": v["pnlSharePct"],
                            "avgRetPct": v["avgRetPct"]}
                        for k, v in core["pnlByMarket"].items()},
        "pnlBySize": {k: {"basisSharePct": v["basisSharePct"],
                          "pnlSharePct": v["pnlSharePct"],
                          "avgRetPct": v["avgRetPct"]}
                      for k, v in core["pnlBySize"].items()},
        "concentrationAxis": {
            "top1SharePct": conc["top"][0]["pnlSharePct"] if conc["top"] else None,
            "top5SharePct": conc["top5SharePct"],
            "top10SharePct": conc["top10SharePct"],
            "hhi": conc["hhiPositiveContrib"],
            "removal": conc["removal"],
        },
        "verdict": None,
    }

    # 귀속 판정 — 어떤 축이 초과수익을 설명하는가
    survives = []
    fails = []
    (survives if strict["candidatePercentile"] >= 90 else fails).append(
        f"SIZE_MARKET_MATCHED 통제군 percentile {strict['candidatePercentile']}")
    (survives if (out["benchmarkAxis"]["vsValidPbrUniversePct"] or -1) > 0
     else fails).append(
        f"PBR 유효종목 benchmark 대비 {out['benchmarkAxis']['vsValidPbrUniversePct']:+.2f}%p")
    (survives if (out["cashAndTimingAxis"]["excessVsCashMatchedPct"] or -1) > 0
     else fails).append(
        f"현금비중 일치 benchmark 대비 {out['cashAndTimingAxis']['excessVsCashMatchedPct']:+.2f}%p")
    r5 = next((r for r in conc["removal"] if r["topK"] == 5), None)
    (survives if (r5 and (r5["firstOrder"]["excessPct"] or -1) > 0) else fails).append(
        f"상위5 제거 1차근사 {r5['firstOrder']['excessPct']:+.2f}%p")
    (survives if (core["marketSegments"].get("KOSPI", {}).get("excessPct") or -1) > 0
     else fails).append(
        f"KOSPI 단독 {core['marketSegments']['KOSPI']['excessPct']:+.2f}%p")
    (survives if (core["sizeControls"].get("SMALL", {}).get("excessPct") or -1) > 0
     else fails).append(
        f"SMALL 구간 내 {core['sizeControls']['SMALL']['excessPct']:+.2f}%p")

    out["verdict"] = {
        "survivedAxes": survives, "failedAxes": fails,
        "survivedCount": len(survives), "totalAxes": len(survives) + len(fails),
        "reading": (
            "무작위·size·거래소 통제군 대비로는 BM 선정이 확실히 우위다(percentile 97~100). "
            "그러나 (a) 전략이 담을 수 없는 PBR 결측 종목을 benchmark 에서 제거하면 "
            f"초과수익이 {out['benchmarkAxis']['vsValidPbrUniversePct']:+.2f}%p 로 사라지고 "
            f"(b) 총손익의 {conc['top5SharePct']}% 가 상위 5종목이며 제거 시 음수가 되고 "
            f"(c) KOSPI 단독·SMALL 구간 내에서는 각각 "
            f"{core['marketSegments']['KOSPI']['excessPct']:+.2f}%p · "
            f"{core['sizeControls']['SMALL']['excessPct']:+.2f}%p 로 음수다. "
            "즉 '무작위보다 낫다'는 성립하지만 '재현 가능한 BM alpha'는 성립하지 않는다. "
            "설명력의 상당 부분이 분석불가 종목 회피 + 소형·KOSDAQ 노출 + 소수 대박종목이다."),
    }

    (RD / "r10-attribution-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    print(json.dumps({"candidateExcessPct": cand,
                      "strictControlPercentile": strict["candidatePercentile"],
                      "strictControlMeanPct": strict["meanExcessPct"],
                      "vsValidPbrPct": out["benchmarkAxis"]["vsValidPbrUniversePct"],
                      "survived": len(survives), "total": out["verdict"]["totalAxes"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
