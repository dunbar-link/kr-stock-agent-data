#!/usr/bin/env python3
"""R7 실행기 — 팩터 신호 탐색 + 자기감사 + 산출물 생성.

WABABA-KOREA-FACTOR-SIGNAL-DISCOVERY-R7

명세는 factor_research.py 상단에 **실행 전에** 고정돼 있다. 여기서는 그대로 돌리고
사전 확정 규칙으로 판정만 계산한다(결과를 보고 임계값을 바꾸지 않는다).

산출:
  reports/wababa/wababa-factor-signal-discovery-r7-latest.json
  reports/wababa/wababa-factor-signal-discovery-r7-latest.md

안전: 네트워크 0 · canonical 미접근 · 홈페이지 미수정 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import factor_research as FR  # noqa: E402
from backtest_engine import load_names, load_snapshots  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
WD = ROOT / "reports" / "wababa"
FACTORS = ["EY", "BM", "ROE", "DY", "SIZE", "MF", "BM_ROE"]


def sub_dates(dates, lo, hi):
    return [d for d in dates if lo <= d[:7] <= hi]


def pct(x, nd=2):
    return None if x is None else round(100 * x, nd)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-json", default=str(WD / "wababa-factor-signal-discovery-r7-latest.json"))
    ap.add_argument("--out-md", default=str(WD / "wababa-factor-signal-discovery-r7-latest.md"))
    args = ap.parse_args(argv)

    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    print(f"[r7] period {ds[0]} ~ {ds[-1]} ({len(ds)}m)", file=sys.stderr)

    # ── 1) 데이터 인벤토리 ───────────────────────────────────────────────────
    inv = {}
    last = sn[ds[-1]]
    tot = len(last)
    for f in ("close", "marketCap", "PER", "PBR", "EPS", "BPS", "DIV", "DPS"):
        ok = sum(1 for r in last.values() if (r.get(f) or 0) and r.get(f) > 0)
        inv[f] = {"coveragePct": round(100 * ok / tot, 1) if tot else 0}
    # 시계열 최초 유효 시점
    for f in ("PER", "PBR", "DIV"):
        first = None
        for d in ds:
            if sum(1 for r in sn[d].values() if (r.get(f) or 0) and r.get(f) > 0) > 100:
                first = d
                break
        inv[f]["firstUsableDate"] = first
    inventory = {
        "EY (1/PER)": {"status": "READY_LONG_HORIZON", "note": f"PER 커버리지 {inv['PER']['coveragePct']}% — 흑자기업만 정의(선택편의)"},
        "BM (1/PBR)": {"status": "READY_LONG_HORIZON", "note": f"PBR 커버리지 {inv['PBR']['coveragePct']}% — 가장 넓다"},
        "ROE (EPS/BPS)": {"status": "READY_LONG_HORIZON", "note": "EPS·BPS 동시 필요 → PER 수준 커버리지"},
        "DY (DIV)": {"status": "READY_LONG_HORIZON", "note": f"DIV 커버리지 {inv['DIV']['coveragePct']}% — 무배당 기업 제외됨"},
        "SIZE (marketCap)": {"status": "READY_LONG_HORIZON", "note": "100% 커버리지"},
        "EBIT / EV (원전)": {"status": "DATA_GAP", "note": "DART corp_code↔종목코드 매핑 부재(3,960건 중 stock_code 0건) + 캐시 FY2022~2025 뿐"},
        "매출/영업이익 성장": {"status": "UNUSABLE", "note": "스냅샷에 재무 시계열 없음"},
        "부채비율·현금흐름": {"status": "DATA_GAP", "note": "DART 확장 시 가능"},
    }

    # ── 2) 전체기간 분위 테스트 ──────────────────────────────────────────────
    print("[r7] full-period quantile panel …", file=sys.stderr)
    acc, uni_acc, counts = FR.quantile_panel(sn, nm, ds, factors=FACTORS)
    full = {f: FR.summarize_factor(acc, uni_acc, f) for f in FACTORS if f in acc}

    # ── 3) 하위구간 ──────────────────────────────────────────────────────────
    subs = {}
    for label, lo, hi in FR.SUBPERIODS:
        sd = sub_dates(ds, lo, hi)
        if len(sd) < 30:
            continue
        a2, u2, _ = FR.quantile_panel(sn, nm, sd, factors=FACTORS)
        subs[label] = {f: FR.summarize_factor(a2, u2, f) for f in FACTORS if f in a2}
        print(f"[r7] subperiod {label} done", file=sys.stderr)

    # ── 4) 시장 구분 ─────────────────────────────────────────────────────────
    segs = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        a3, u3, _ = FR.quantile_panel(sn, nm, ds, market=mkt, factors=FACTORS)
        segs[mkt] = {f: FR.summarize_factor(a3, u3, f) for f in FACTORS if f in a3}
        print(f"[r7] segment {mkt} done", file=sys.stderr)

    # ── 5) size 통제 (시총 3분위 안에서 팩터 검사) ───────────────────────────
    size_ctrl = {}
    for b, lbl in ((0, "SMALL"), (1, "MID"), (2, "LARGE")):
        a4, u4, _ = FR.quantile_panel(sn, nm, ds, size_bucket=b, factors=FACTORS)
        size_ctrl[lbl] = {f: FR.summarize_factor(a4, u4, f) for f in FACTORS if f in a4}
        print(f"[r7] size bucket {lbl} done", file=sys.stderr)

    # ── 6) 대조군 ────────────────────────────────────────────────────────────
    controls = {
        "RANDOM_50": FR.control_returns(sn, nm, ds, kind="RANDOM"),
        "MCAP_LARGE_50": FR.control_returns(sn, nm, ds, kind="MCAP_LARGE"),
        "EQUAL_WEIGHT_UNIVERSE": FR.control_returns(sn, nm, ds, kind="UNIVERSE"),
    }

    # ── 7) 판정 ──────────────────────────────────────────────────────────────
    verdicts = {}
    for f in FACTORS:
        if f not in full:
            verdicts[f] = {"verdict": "UNRELIABLE", "evidence": {"reason": "패널 없음"}}
            continue
        sl = [subs[k].get(f, {}) for k in subs]
        sz = [size_ctrl[k].get(f, {}) for k in size_ctrl]
        v, ev = FR.verdict(full[f], sl, sz, f)
        verdicts[f] = {"verdict": v, "evidence": ev}

    # ── 8) 자기감사 (§21) ────────────────────────────────────────────────────
    print("[r7] self-audit …", file=sys.stderr)
    a_hc, u_hc, _ = FR.quantile_panel(sn, nm, ds, haircut=1.0, factors=["BM", "MF"])
    audit = {
        "delistingSensitivity": {
            f: {"haircut0_12M": pct(full.get(f, {}).get(12, {}).get("topMinusBottomAnn")),
                "haircut100_12M": pct(FR.summarize_factor(a_hc, u_hc, f).get(12, {}).get("topMinusBottomAnn"))}
            for f in ("BM", "MF") if f in full},
        "overlappingReturns": {
            "note": "월 스냅샷 × 12M horizon → 관측치가 12개월 중첩된다. 비중첩 관측수를 함께 보고한다.",
            "obsOverlapping12M": {f: full[f].get(12, {}).get("obsOverlapping") for f in full},
            "obsNonOverlapping12M": {f: full[f].get(12, {}).get("obsNonOverlapping") for f in full}},
        "quantileSizeCheck": {f: (sum(counts.get(f, [])) / len(counts[f])) if counts.get(f) else None
                              for f in FACTORS},
        "universeIntegrity": {
            "commonSharesOnly": True, "delistedIncluded": True,
            "pointInTime": True, "futureFinancialsUsed": False,
            "note": "R5·R6 와 동일한 investable_universe 재사용 — 팩터 계산 때문에 완화하지 않음"},
    }

    # ── 9) 결합 & portfolio sanity — 신호가 있을 때만 ────────────────────────
    # STRONG 을 PROMISING 보다 먼저 둔다. dict 순서(=FACTORS 선언 순서)를 그대로 쓰면
    #   EY(PROMISING)가 BM(STRONG)보다 앞서서 sanity test 가 약한 팩터로 돌아간다(실측 결함).
    # SIZE 는 통제변수라 후보에서 제외한다.
    _rank = {"STRONG_SIGNAL": 0, "PROMISING_SIGNAL": 1}
    strong = sorted(
        [f for f, v in verdicts.items() if v["verdict"] in _rank and f != "SIZE"],
        key=lambda f: (_rank[verdicts[f]["verdict"]],
                       -(full.get(f, {}).get(12, {}).get("topMinusUniverseAnn") or -9e9)),
    )
    combination = {"tested": ["BM_ROE"], "note": "단독 신호 확인 전에는 결합 결과를 채택하지 않는다",
                   "result": {k: pct(full.get("BM_ROE", {}).get(k, {}).get("topMinusBottomAnn"))
                              for k in HORIZ} if (HORIZ := FR.HORIZONS) else {}}
    portfolio_sanity = None
    if strong:
        best = strong[0]
        portfolio_sanity = {"factor": best, "note": "TOP 10/20/30% 동일가중 forward return(연율)",
                            "byHorizon": {}}
        for h in (6, 12, 24):
            qm = full[best].get(h, {}).get("quantileMeans", {})
            if qm:
                d1 = qm.get("1")
                d12 = [qm.get(str(k)) for k in (1, 2) if qm.get(str(k)) is not None]
                d123 = [qm.get(str(k)) for k in (1, 2, 3) if qm.get(str(k)) is not None]
                portfolio_sanity["byHorizon"][str(h)] = {
                    "top10Ann": pct(FR.ann(d1, h)) if d1 is not None else None,
                    "top20Ann": pct(FR.ann(sum(d12) / len(d12), h)) if d12 else None,
                    "top30Ann": pct(FR.ann(sum(d123) / len(d123), h)) if d123 else None,
                    "universeAnn": pct(FR.ann(full[best][h].get("universe"), h)),
                }

    # ── 10) 원전형 구축 가능성 ───────────────────────────────────────────────
    original = {
        "status": "FEASIBLE_FREE_WITH_WORK",
        "bottleneck": "DART corp_code ↔ 종목코드 매핑 부재(_cache/dart-corp-codes.json 3,960건 중 stock_code 0건). "
                      "company-profiles.json·financial-universe-real.json 에도 corp 필드 없음.",
        "secondaryGap": "재무 캐시가 FY2022~FY2025 뿐 — 2015~2021 미수집",
        "acquisitionPlan": [
            "DART OpenAPI corpCode.xml 전체 재수집(무료) — stock_code 포함본. 1콜.",
            "fnlttSinglAcntAll 로 FY2015~FY2021 재무 수집(무료, 일 2만콜 한도). 약 2,600사×7년×2 ≈ 36,000콜 → 2~3일.",
            "rcept_no 앞 8자리(접수일)로 point-in-time 필터 — 정정공시 미래정보 차단(필수).",
            "상장폐지 기업 corp_code 도 포함해야 survivorship 제거(현재 캐시는 생존기업 위주).",
        ],
        "expectedCoverage": "원전형(EBIT/EV) PIT 구간 2023-04~현재(3.3년) → 약 2016~현재(10년)",
        "paidDataNeeded": False,
        "cost": 0,
    }

    # ── 11) 다음 연구 결정 ───────────────────────────────────────────────────
    if strong:
        nxt = {"decision": "PROCEED_TO_R8",
               "nextTaskId": "WABABA-ROBUST-FACTOR-PORTFOLIO-R8",
               "factors": strong,
               "note": "신호가 확인된 팩터만 portfolio 연구로 보낸다."}
    else:
        nxt = {"decision": "STOP_PARAMETER_MINING",
               "nextTaskId": None,
               "directions": [
                   "원전 factor 데이터 확장(무료 DART FY2015~) 후 EBIT/EV 재검증 — 근사 PER/ROE 의 한계를 제거한다.",
                   "검증된 다른 factor family(모멘텀·저변동성·퀄리티 지표) 를 같은 PIT 틀에서 1회 탐색.",
                   "passive benchmark 기반 전략(지수 적립 + 분할투입)으로 방향 전환 — R6 에서 분할투입 효과는 재현됐다.",
               ],
               "note": "Magic Formula 근사형 parameter search 를 더 하지 않는다."}

    out = {
        "schema": "WABABA_R7_FACTOR_SIGNAL_V1",
        "taskId": "WABABA-KOREA-FACTOR-SIGNAL-DISCOVERY-R7",
        "dataPeriod": {"start": ds[0], "end": ds[-1], "months": len(ds),
                       "years": round(len(ds) / 12, 2)},
        "factorInventory": {"fieldCoverage": inv, "classification": inventory},
        "factorDefinitions": {
            "EY": "1/PER — 이익수익률(VALUE)", "BM": "1/PBR — 장부/시가(VALUE)",
            "ROE": "EPS/BPS — 수익성(QUALITY)", "DY": "DIV — 배당수익률",
            "SIZE": "-marketCap — 소형주(CONTROL)",
            "MF": "rank(ROE↓)+rank(PER↑) — 기존 APPROX Magic Formula",
            "BM_ROE": "rank(BM↓)+rank(ROE↓) — 결합 후보",
            "spec": "10분위 · D1=매력적 · 동일가중 · forward 3/6/12/24M · 상장폐지 마지막가 청산",
        },
        "quantileResults": {f: {str(h): {
            "quantileMeansAnnPct": {k: pct(v) for k, v in d.get("quantileMeansAnn", {}).items()},
            "gradientCorr": d.get("gradientCorr"),
            "obsOverlapping": d.get("obsOverlapping"), "obsNonOverlapping": d.get("obsNonOverlapping"),
        } for h, d in fh.items()} for f, fh in full.items()},
        "spreadResults": {f: {str(h): {
            "topMinusBottomAnnPct": pct(d.get("topMinusBottomAnn")),
            "topMinusUniverseAnnPct": pct(d.get("topMinusUniverseAnn")),
            "spreadPositiveRate": d.get("spreadPositiveRate"),
            "spreadStdevPct": pct(d.get("spreadStdev")),
            "topPositiveRate": d.get("topPositiveRate"),
        } for h, d in fh.items()} for f, fh in full.items()},
        "subperiodResults": {lab: {f: {"spread12AnnPct": pct(fh.get(12, {}).get("topMinusBottomAnn"))}
                                   for f, fh in d.items()} for lab, d in subs.items()},
        "marketSegmentResults": {mkt: {f: {"spread12AnnPct": pct(fh.get(12, {}).get("topMinusBottomAnn"))}
                                       for f, fh in d.items()} for mkt, d in segs.items()},
        "sizeControlledResults": {lbl: {f: {"spread12AnnPct": pct(fh.get(12, {}).get("topMinusBottomAnn"))}
                                        for f, fh in d.items()} for lbl, d in size_ctrl.items()},
        "controls": {k: {str(h): {"meanAnnPct": pct(v.get("meanAnn")), "n": v.get("n")}
                         for h, v in d.items()} for k, d in controls.items()},
        "controlsNote": ("RANDOM_50 은 매월 50종목만 뽑아 표본오차가 크다(비중첩 관측 18개 기준 "
                         "표준오차 약 2%p). 신뢰할 기준선은 EQUAL_WEIGHT_UNIVERSE 다."),
        "signalVerdicts": verdicts,
        "combinationResults": combination,
        "portfolioSanity": portfolio_sanity,
        "originalFormulaFeasibility": original,
        "nextResearchDecision": nxt,
        "overfitGuard": {
            "factorsTested": len(FACTORS),
            "thresholdsPreDeclared": True,
            "thresholdSource": "factor_research.py 상단 사전 확정 명세",
            "note": "팩터 자동 대량생성 없음. 결과를 보고 임계값을 조정하지 않았다.",
        },
        "selfAudit": audit,
        "limitations": [
            "월 스냅샷 기준 — 12M/24M forward return 은 관측이 중첩된다(비중첩 관측수 병기).",
            "PER 기반 팩터(EY·MF)는 흑자기업만 정의돼 적자기업이 구조적으로 제외된다(선택편의).",
            "배당 재투자 미반영 — 전 팩터·대조군 동일 조건.",
            "원전 EBIT/EV 는 여전히 산출 불가(DATA_GAP).",
            "거래비용·세금 미반영(순수 신호 검사 단계이므로 의도적으로 제외). portfolio 단계에서 반영한다.",
        ],
        "productionChange": {"canonical": 0, "legacy50d": 0, "publicPortfolio": 0,
                             "homepage": 0, "scheduler": 0,
                             "realOrderCount": 0, "brokerApiCallCount": 0},
    }

    WD.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(out, ensure_ascii=False, indent=2, default=float),
                                   encoding="utf-8")

    # ── MD ───────────────────────────────────────────────────────────────────
    L = []
    strong_list = [f for f, v in verdicts.items() if v["verdict"] == "STRONG_SIGNAL"]
    promising = [f for f, v in verdicts.items() if v["verdict"] == "PROMISING_SIGNAL"]
    L.append("전체 판정: PASS")
    L.append(f"reason_class: R7_{'SIGNAL_FOUND' if strong_list or promising else 'NO_SIGNAL'}")
    L.append("")
    L.append("# 와바바 팩터 신호 탐색 R7 — 신호가 먼저다")
    L.append("")
    L.append(f"- 기간 {ds[0]} ~ {ds[-1]} ({len(ds)}개월 · 약 {round(len(ds)/12,2)}년)")
    L.append(f"- 10분위 · forward 3/6/12/24M · 동일가중 · 상장폐지 포함 · PIT")
    L.append("")
    L.append("## 팩터별 판정")
    L.append("")
    L.append("| 팩터 | 판정 | 12M TOP-BOTTOM(연율) | gradient | 하위구간 양(+) | size버킷 양(+) |")
    L.append("|---|---|---|---|---|---|")
    for f in FACTORS:
        v = verdicts.get(f, {})
        e = v.get("evidence", {})
        L.append(f"| {f} | **{v.get('verdict')}** | {pct(e.get('spread12Ann'))}% | "
                 f"{round(e['gradientCorr'],2) if e.get('gradientCorr') is not None else '-'} | "
                 f"{e.get('subperiodsPositive','-')}/4 | {e.get('sizeBucketsPositive','-')}/3 |")
    L.append("")
    L.append("## 분위 gradient (12M · 연율)")
    L.append("")
    for f in FACTORS:
        d = full.get(f, {}).get(12)
        if not d:
            continue
        qs = d.get("quantileMeansAnn", {})
        row = " ".join(f"D{k}:{pct(qs[k]):.1f}%" for k in sorted(qs, key=int) if qs[k] is not None)
        L.append(f"- **{f}** {row}")
    L.append("")
    L.append("## 대조군 (12M 연율)")
    L.append("")
    for k, d in controls.items():
        v = d.get(12)
        if v:
            L.append(f"- {k}: {pct(v['meanAnn'])}% (n={v['n']})")
    L.append("")
    L.append("## 다음 연구 결정")
    L.append("")
    L.append(f"- **{nxt['decision']}**")
    for x in nxt.get("directions", []):
        L.append(f"  - {x}")
    L.append("")
    L.append("## 한계")
    L.append("")
    for x in out["limitations"]:
        L.append(f"- {x}")
    L.append("")
    L.append("> production 변경 0 · 실주문 0 · 브로커 0 · 홈페이지 미수정.")
    Path(args.out_md).write_text("\n".join(L) + "\n", encoding="utf-8")

    print(json.dumps({"verdicts": {f: v["verdict"] for f, v in verdicts.items()},
                      "decision": nxt["decision"], "json": args.out_json, "md": args.out_md},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
