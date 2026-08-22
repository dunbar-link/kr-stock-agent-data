#!/usr/bin/env python3
"""R26 결합 자격 판정.

WABABA-BM-SIZE-INCREMENTAL-COMBINATION-R26

precommit QUALIFICATION 을 **그대로** 적용한다. 여기서 기준을 만들거나 바꾸지
않는다(§28·§44). COMBO 가 두 단독 중 하나라도 못 이기면 억지로 살리지 않는다
(§29·§30).

안전: 계산·읽기 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r26_precommit import HORIZONS, NEXT_TASK_RULE, QUALIFICATION  # noqa: E402

RD = Path(__file__).resolve().parents[2] / "reports" / "research"
PH = HORIZONS["primary"]
ALL_H = HORIZONS["all"]


def L(n):
    return json.loads((RD / f"r26-{n}-latest.json").read_text(encoding="utf-8"))


def main() -> int:
    inc, sub = L("incremental-alpha"), L("subperiod-results")
    boot, conc = L("bootstrap"), L("concentration")
    dist, lim = L("distress-delisting"), L("tsr-limitation")
    ec, cond = L("exchange-and-cost"), L("conditional-attribution")
    base, quant = L("base-reproduction"), L("quantile-results")

    p = inc["byHorizon"][f"{PH}M"]
    vs_bm = p["incremental_vs_BM"]["meanPct"]
    vs_sz = p["incremental_vs_SIZE"]["meanPct"]

    hb = [inc["byHorizon"][f"{h}M"]["incremental_vs_BM"]["meanPct"]
          for h in ALL_H]
    hs = [inc["byHorizon"][f"{h}M"]["incremental_vs_SIZE"]["meanPct"]
          for h in ALL_H]
    both_pos_h = sum(1 for a, b in zip(hb, hs) if a > 0 and b > 0)

    S = QUALIFICATION["INCREMENTAL_COMBINATION_STRONG"]
    bmb = boot["byHorizon"][f"{PH}M"]["COMBO_minus_BM"]["movingBlock"]
    szb = boot["byHorizon"][f"{PH}M"]["COMBO_minus_SIZE"]["movingBlock"]
    rm3 = conc["removal"]["removeTop3"]

    checks = {
        "COMBO > BM (primary)": vs_bm > 0,
        "COMBO > SIZE (primary)": vs_sz > 0,
        f"양(+) horizon 둘 다 >= {S['minHorizonsPositiveBoth']}":
            both_pos_h >= S["minHorizonsPositiveBoth"],
        "부분기간 과반 (vsBM)":
            (sub["vsBM"][f"{PH}M"]["positiveRatio"] or 0)
            >= S["minSubperiodPositiveRatio"],
        "부분기간 과반 (vsSIZE)":
            (sub["vsSIZE"][f"{PH}M"]["positiveRatio"] or 0)
            >= S["minSubperiodPositiveRatio"],
        "롤링 과반 (vsBM)":
            p["incremental_vs_BM"]["positiveRatio"] >= S["minRollingPositiveRatio"],
        "롤링 과반 (vsSIZE)":
            p["incremental_vs_SIZE"]["positiveRatio"] >= S["minRollingPositiveRatio"],
        "부트스트랩 CI 하한 > 0 (vsBM)": bmb["ci95LowPct"] > 0,
        "부트스트랩 CI 하한 > 0 (vsSIZE)": szb["ci95LowPct"] > 0,
        "양 시장 방향 유지": bool(ec["exchange"]["directionHoldsBothMarkets_vsBM"]
                            and (ec["exchange"]["KOSPI"]["vsSIZE"]["meanPct"] > 0)
                            == (ec["exchange"]["KOSDAQ"]["vsSIZE"]["meanPct"] > 0)
                            and ec["exchange"]["KOSPI"]["vsSIZE"]["meanPct"] > 0),
        "top3 제거 후 부호 유지": not conc["signFlipsWhenTop3Removed"],
        "TSR 한계 미노출": not lim["exposed"],
        f"COMBO 상폐율 <= {S['maxComboTopDelistingRatePct']}%":
            dist["byArm"]["COMBO"]["delistingRatePct"]
            <= S["maxComboTopDelistingRatePct"],
        "상폐율이 부모보다 나쁘지 않음": not dist["comboWorseThanParents"],
    }

    fragile_reasons = []
    if conc["signFlipsWhenTop3Removed"]:
        fragile_reasons.append("top3 제거 시 incremental 부호 반전")
    if lim["exposed"]:
        fragile_reasons.append("TSR_LIMITATION_EXPOSED")
    if dist["byArm"]["COMBO"]["delistingRatePct"] > S["maxComboTopDelistingRatePct"]:
        fragile_reasons.append("COMBO TOP 상폐율 한계 초과")
    if dist["comboWorseThanParents"]:
        fragile_reasons.append("상폐 노출이 두 단독보다 악화")

    # precommit 순서 그대로: NO → FRAGILE → STRONG → PROMISING
    if vs_bm <= 0 or vs_sz <= 0:
        verdict = "NO_INCREMENTAL_COMBINATION"
        why = [f"primary {PH}M 에서 "
               + ("BM 단독 대비 " if vs_bm <= 0 else "")
               + ("SIZE 단독 대비 " if vs_sz <= 0 else "")
               + "incremental 이 0 이하",
               f"vs BM {vs_bm:+.3f}%p · vs SIZE {vs_sz:+.3f}%p",
               "두 단독 모두를 이겨야 결합이 의미가 있다(§10·§29·§30)."]
    elif fragile_reasons:
        verdict, why = "COMBINATION_FRAGILE", fragile_reasons
    elif all(checks.values()):
        verdict, why = "INCREMENTAL_COMBINATION_STRONG", list(checks)
    else:
        verdict = "INCREMENTAL_COMBINATION_PROMISING"
        why = ["STRONG 미충족: "
               + ", ".join(k for k, v in checks.items() if not v)]

    strong_or_promising = verdict in ("INCREMENTAL_COMBINATION_STRONG",
                                      "INCREMENTAL_COMBINATION_PROMISING")
    if strong_or_promising:
        nxt = NEXT_TASK_RULE["onStrongOrPromising"]
        single = None
    elif verdict == "COMBINATION_FRAGILE":
        nxt = NEXT_TASK_RULE["onFragile"]
        single = None
    else:
        # §29 — 실행가능성과 경제적 우위가 더 좋은 **단독** factor 하나
        b, s = dist["byArm"]["BM_ALONE"], dist["byArm"]["SIZE_ALONE"]
        lq = L("liquidity")["byArm"]
        bm_cleaner = (b["delistingRatePct"] < s["delistingRatePct"]
                      and b["persistentLossRatePct"] < s["persistentLossRatePct"]
                      and lq["BM_ALONE"]["medianClose"]
                      > lq["SIZE_ALONE"]["medianClose"])
        single = "BM" if bm_cleaner else "SIZE_SMALL"
        nxt = (f"{single} 단독 실행 포트폴리오 연구 — "
               + NEXT_TASK_RULE["onNoIncremental"])

    out = {
        "task": "R26", "verdict": verdict, "verdictWhy": why,
        "primaryHorizon": f"{PH}M",
        "qualificationFromPrecommit": QUALIFICATION,
        "thresholdsUnchanged": True,
        "checks": checks,
        "incrementalPrimary": {"vsBM": vs_bm, "vsSIZE": vs_sz},
        "incrementalByHorizon": {
            k: {"vsBM": v["incremental_vs_BM"]["meanPct"],
                "vsSIZE": v["incremental_vs_SIZE"]["meanPct"]}
            for k, v in sorted(inc["byHorizon"].items(),
                               key=lambda kv: int(kv[0][:-1]))},
        "betterThanBmAlone": vs_bm > 0,
        "betterThanSizeAlone": vs_sz > 0,
        "r25ReproductionExact": base["allExactMatch"],
        "independence": cond["independence"],
        "monotonicity": {a: v["monotonicity"] for a, v in quant["byArm"].items()},
        "portfolioResearchWorthwhile": strong_or_promising,
        "recommendedSingleFactor": single,
        "nextTask": nxt,
        "r26Forbidden": NEXT_TASK_RULE["r26Forbidden"],
        "portfolioSearchDone": False,
        "weightSearchDone": False,
        "intersectionTested": False,
        "noForcedCombination": (
            "두 factor 가 각각 강하다고 해서 합치면 더 강해야 하는 것은 아니다. "
            "실제 evidence 로 판단했다(§30)."),
    }
    (RD / "r26-verdict-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")
    print(json.dumps({"verdict": verdict, "vsBM": vs_bm, "vsSIZE": vs_sz,
                      "next": single or "R27", "failed":
                      [k for k, v in checks.items() if not v]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
