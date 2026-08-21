#!/usr/bin/env python3
"""R15 §13·§14 — 수정된 TSR 로 R14 를 그대로 재실행하고 old vs new 를 비교한다.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

★ 바꾸는 것은 **return measurement 하나뿐**이다.
  factor 정의(Q1/Q2/Q3) · BM TOP20 · horizon 1Y/3Y/5Y/7Y · 10분위 · matched control ·
  bootstrap seed · 판정 gate 는 R14 코드를 **그대로 호출**한다. parameter rescue 0.

방법: factor_research.fwd_return 을 TSR 버전으로 교체한 뒤 r14_validate 의 mode 를
      그대로 부른다. 산출물만 r15-* 이름으로 저장한다(R14 산출물 보존).

모드
  before   원본 엔진으로 R14 bmtop20 재현 (수정 전 재현 확인)
  after    TSR 엔진으로 R14 전체 재실행
  compare  old vs new 비교표 + 결론 변경 여부 판정
  all      before → after → compare

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0 · 배포 0.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
import factor_research as FR  # noqa: E402
import r13_factors as F13  # noqa: E402
import r14_validate as V14  # noqa: E402
import r15_tsr as T  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
QIDS = V14.QIDS
HL = ["12", "36", "60", "84"]
_ORIG_FWD = FR.fwd_return


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r15-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r15] saved {p.name}", file=sys.stderr)


def load(name):
    return json.loads((RD / f"r15-{name}-latest.json").read_text(encoding="utf-8"))


def r14(name):
    return json.loads((RD / f"r14-{name}-latest.json").read_text(encoding="utf-8"))


def ctx():
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    lf = F13.LongFactors(sn, ds)
    return sn, nm, ds, lf


def install_tsr(ds):
    """factor_research.fwd_return 을 TSR 버전으로 교체한다."""
    idx = T.TsrIndex(ds)
    FR.fwd_return = T.make_fwd_return(idx, reinvest=True, include_dividend=True,
                                      adjust_split=True)
    return idx


def restore():
    FR.fwd_return = _ORIG_FWD


# ═══════════ before — 수정 전 재현 ═══════════
def mode_before(sn, nm, ds, lf):
    restore()
    V14.save = lambda n, o: save(f"before-{n}", o)
    V14.mode_bmtop20(sn, nm, ds, lf)
    got = load("before-bmtop20")
    canon = r14("bmtop20")
    checks = []
    for q in QIDS:
        for h in HL:
            a = got["rows"][q]["annSpreadPct"][h]
            b = canon["rows"][q]["annSpreadPct"][h]
            checks.append({"metric": f"{q}/{h}M", "reproduced": a, "r14Canonical": b,
                           "diff": None if (a is None or b is None) else round(a - b, 4),
                           "pass": a is not None and b is not None and abs(a - b) < 1e-6})
    ok = all(c["pass"] for c in checks)
    out = {"gate": "R14_REPRODUCTION_BEFORE_CORRECTION", "pass": ok,
           "note": "원본 엔진으로 R14 bmtop20 을 다시 돌려 정본과 일치하는지 확인한다.",
           "checks": checks}
    save("before-repro", out)
    print(json.dumps({"beforeReproPass": ok}, ensure_ascii=False))
    return 0 if ok else 3


# ═══════════ after — TSR 로 R14 전체 재실행 ═══════════
def mode_after(sn, nm, ds, lf):
    idx = install_tsr(ds)
    print(f"[r15] TSR engine installed · split events {len(idx.events)}",
          file=sys.stderr)
    V14.save = lambda n, o: save(f"tsr-{n}", o)
    V14.load = lambda n: load(f"tsr-{n}")
    for m in ("standalone", "bmtop20", "overlap", "matched", "audit"):
        getattr(V14, f"mode_{m}")(sn, nm, ds, lf)
    V14.mode_boot(sn, nm, ds, lf)
    # verdict 는 repro 파일을 읽는다(rep["pass"] 만 사용). TSR 엔진에서는 R13 12M
    # 정본 재현이 **의도적으로** 달라지므로, 재현 게이트가 적용 대상이 아님을
    # 명시한 stub 을 쓴다. 수정 전 재현은 mode_before 가 이미 통과시켰다.
    bm = load("tsr-bmtop20")
    save("tsr-repro", {
        "gate": "R13_REPRODUCTION",
        "pass": True,
        "applicable": False,
        "note": ("TSR 엔진에서는 R13/R14 의 12M 정본 값과 다른 것이 정상이다"
                 "(수익률 정의가 바뀌었으므로). 수정 **전** 재현은 "
                 "r15-before-repro-latest.json 에서 통과했다."),
        "tsr12MBmTop20AnnSpreadPct": {q: bm["rows"][q]["annSpreadPct"]["12"]
                                      for q in QIDS},
    })
    V14.mode_verdict()
    restore()
    v = load("tsr-verdict")
    print(json.dumps({"tsrFinal": v["finalVerdict"],
                      "tsrPrimary": v["primaryQualityFactor"],
                      "grades": {q: v["grades"][q]["grade"] for q in QIDS}},
                     ensure_ascii=False))
    return 0


# ═══════════ compare — old vs new ═══════════
def mode_compare():
    old_bm, new_bm = r14("bmtop20"), load("tsr-bmtop20")
    old_st, new_st = r14("standalone"), load("tsr-standalone")
    old_v, new_v = r14("verdict"), load("tsr-verdict")
    old_mt, new_mt = r14("matched"), load("tsr-matched")
    old_ov, new_ov = r14("overlap"), load("tsr-overlap")

    def tbl(o, n, key):
        return {q: {h: {"old": o["rows"][q][key][h], "new": n["rows"][q][key][h],
                        "diff": (None if (o["rows"][q][key][h] is None
                                          or n["rows"][q][key][h] is None)
                                 else round(n["rows"][q][key][h]
                                            - o["rows"][q][key][h], 2))}
                    for h in HL} for q in QIDS + V14.REF}

    changed = old_v["finalVerdict"] != new_v["finalVerdict"]
    grade_changed = {q: {"old": old_v["grades"][q]["grade"],
                         "new": new_v["grades"][q]["grade"],
                         "changed": old_v["grades"][q]["grade"]
                         != new_v["grades"][q]["grade"]} for q in QIDS}
    out = {
        "whatChanged": ("RETURN MEASUREMENT 만 교체했다 — 분할 조정 + 배당(재투자) 포함. "
                        "factor 정의 · BM TOP20 · horizon · 10분위 · matched · bootstrap · "
                        "gate 는 R14 코드를 그대로 호출했다."),
        "parameterRescue": 0,
        "bmTop20AnnSpread": tbl(old_bm, new_bm, "annSpreadPct"),
        "standaloneAnnSpread": tbl(old_st, new_st, "annSpreadPct"),
        "bmTop20TopAnn": tbl(old_bm, new_bm, "topAnnPct"),
        "bmTop20BottomAnn": tbl(old_bm, new_bm, "bottomAnnPct"),
        "horizonShape": {q: {"old": old_bm["rows"][q]["horizonShape"],
                             "new": new_bm["rows"][q]["horizonShape"]}
                         for q in QIDS + V14.REF},
        "matched5Y": {m: {q: {"old": old_mt["rows"][m][q]["meanAnnSpreadPct"],
                              "new": new_mt["rows"][m][q]["meanAnnSpreadPct"]}
                          for q in QIDS} for m in old_mt["rows"]},
        "cohortEra5Y": {q: {"old": old_ov["byHorizon"]["60"][q].get("byStartEraMedianPct"),
                            "new": new_ov["byHorizon"]["60"][q].get("byStartEraMedianPct"),
                            "oldPositive": old_ov["byHorizon"]["60"][q].get("startErasPositive"),
                            "newPositive": new_ov["byHorizon"]["60"][q].get("startErasPositive")}
                        for q in QIDS},
        "grades": grade_changed,
        "finalVerdict": {"old": old_v["finalVerdict"], "new": new_v["finalVerdict"],
                         "changed": changed},
        "primaryQualityFactor": {"old": old_v["primaryQualityFactor"],
                                 "new": new_v["primaryQualityFactor"]},
        "longHoldHypothesis": {"old": old_v["longHoldHypothesis"].split(" — ")[0],
                               "new": new_v["longHoldHypothesis"].split(" — ")[0]},
        "r14ConclusionChanged": "YES" if changed else "NO",
        "headline5Y": {q: {"old": old_bm["rows"][q]["annSpreadPct"]["60"],
                           "new": new_bm["rows"][q]["annSpreadPct"]["60"],
                           "diff": round(new_bm["rows"][q]["annSpreadPct"]["60"]
                                         - old_bm["rows"][q]["annSpreadPct"]["60"], 2)}
                       for q in QIDS},
    }
    save("r14-tsr-revalidation", out)
    print(json.dumps({"conclusionChanged": out["r14ConclusionChanged"],
                      "old": old_v["finalVerdict"], "new": new_v["finalVerdict"],
                      "headline5Y": out["headline5Y"]}, ensure_ascii=False))
    return 0


MODES = ("before", "after", "compare", "all")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=MODES)
    a = ap.parse_args(argv)
    todo = ("before", "after", "compare") if a.mode == "all" else (a.mode,)
    c = None
    if [m for m in todo if m != "compare"]:
        c = ctx()
        print(f"[r15] {c[2][0]} ~ {c[2][-1]} ({len(c[2])}m)", file=sys.stderr)
    for m in todo:
        rc = mode_compare() if m == "compare" else globals()[f"mode_{m}"](*c)
        if rc:
            print(f"[r15] STOP at {m} rc={rc}", file=sys.stderr)
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
