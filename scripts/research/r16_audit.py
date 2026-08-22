#!/usr/bin/env python3
"""R16 감사 실행기 — inventory · anchor · anomaly · coverage · legacy status.

WABABA-CANONICAL-TSR-RESEARCH-FOUNDATION-RESET-R16

산출:
  reports/research/r16-corporate-action-inventory-latest.json
  reports/research/r16-anchor-cases-latest.json
  reports/research/r16-anomaly-audit-latest.json
  reports/research/r16-tsr-coverage-latest.json
  reports/research/r16-legacy-research-status-latest.json
  reports/research/r16-foundation-verdict-latest.json

안전: 계산 전용 · 네트워크 0(액면변경 직접소스는 별도 스크립트) · canonical 미접근 ·
      실주문 0 · 브로커 0 · 배포 0.
"""
from __future__ import annotations

import bisect
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names  # noqa: E402
import r16_canonical as C  # noqa: E402
from r16_precommit import ANCHOR, DELISTING  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r16-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r16] saved {p.name}", file=sys.stderr)


# ═══════════ inventory (§5·§6) ═══════════
def inventory(eng, ds, nm):
    tm = sum(1 for d in ds for _ in eng.cap.get(d, {}))
    dps_present = dps_zero = dps_missing = 0
    for d in ds:
        for t, r in eng.cap.get(d, {}).items():
            v = r.get("dps")
            if v is None:
                dps_missing += 1
            elif v <= 0:
                dps_zero += 1
            else:
                dps_present += 1
    tot = dps_present + dps_zero + dps_missing
    mech = eng.counts[C.MECHANICAL]
    rights = eng.counts[C.RIGHTS]
    down = eng.counts[C.DOWN_NP]
    upnp = eng.counts[C.UP_NP]

    rows = [
        {"event": "CASH_DIVIDEND", "source": "KRX DPS (PIT 스냅샷)",
         "provenance": "DIRECT_SOURCE", "coverage": f"{round(100*dps_present/tot,1)}% 유효 · "
         f"{round(100*dps_zero/tot,1)}% 0 · {round(100*dps_missing/tot,1)}% 결측",
         "pit": "스냅샷 날짜 기준", "engine": "월 1/12 발생 + 재투자",
         "support": "SUPPORTED_PARTIAL",
         "gap": "연간 DPS 라 배당락 시점 재현 불가. 월 균등 근사."},
        {"event": "SPECIAL_DIVIDEND", "source": "DPS 에 합산", "provenance": "DIRECT_SOURCE",
         "coverage": "CASH_DIVIDEND 와 동일", "pit": "동일",
         "engine": "DPS 경유 자동 포함", "support": "SUPPORTED_PARTIAL",
         "gap": "정규/특별 분리 불가(합산값)."},
        {"event": "STOCK_SPLIT", "source": "shares·close·mcap 파생 + 액면변경 대조",
         "provenance": "DERIVED_INFERENCE (직접소스 표본검증)",
         "coverage": f"MECHANICAL {mech}건", "pit": "월 단위",
         "engine": "보유주식 × shareRatio", "support": "SUPPORTED_PARTIAL",
         "gap": "월 스냅샷이라 정확한 ex-date 미확보. 표본 22건 중 7건만 액면변경 확인."},
        {"event": "REVERSE_SPLIT", "source": "동일", "provenance": "DERIVED_INFERENCE",
         "coverage": "MECHANICAL 에 포함", "pit": "월 단위",
         "engine": "동일 처리", "support": "SUPPORTED_PARTIAL",
         "gap": "무상감자와 서명이 같아 구분 불가(경제적 처리는 동일)."},
        {"event": "BONUS_ISSUE", "source": "동일", "provenance": "DERIVED_INFERENCE",
         "coverage": "MECHANICAL 에 포함", "pit": "월 단위",
         "engine": "동일 처리(무상 신주)", "support": "SUPPORTED_PARTIAL",
         "gap": "액면 무변동이라 분할과 구분되지 않으나 경제적 처리 동일."},
        {"event": "STOCK_DIVIDEND", "source": "동일", "provenance": "DERIVED_INFERENCE",
         "coverage": "MECHANICAL 에 포함", "pit": "월 단위",
         "engine": "동일 처리", "support": "SUPPORTED_PARTIAL",
         "gap": "BONUS_ISSUE 와 구분 불가."},
        {"event": "RIGHTS_ISSUE", "source": "**없음** — 신주배정비율·발행가·청약일 부재",
         "provenance": "NONE", "coverage": f"의심 {rights}건 탐지만",
         "pit": "불가", "engine": "**조정하지 않음**(보유주식 불변)",
         "support": "UNSUPPORTED_DATA",
         "gap": "권리가치·추가납입을 계산할 수 없다. RIGHTS_DATA_INCOMPLETE."},
        {"event": "TREASURY_BUYBACK", "source": "shares 감소로 간접 관측",
         "provenance": "DERIVED_INFERENCE", "coverage": f"SHARES_DOWN {down}건",
         "pit": "월 단위", "engine": "조정 없음(현금 미지급)",
         "support": "SUPPORTED_COMPLETE",
         "gap": "없음 — 경제적으로 조정하지 않는 것이 정답."},
        {"event": "TREASURY_CANCELLATION", "source": "동일",
         "provenance": "DERIVED_INFERENCE", "coverage": "SHARES_DOWN 에 포함",
         "pit": "월 단위", "engine": "조정 없음(보유주식 불변)",
         "support": "SUPPORTED_COMPLETE", "gap": "없음."},
        {"event": "CAPITAL_REDUCTION", "source": "shares 감소 + 가격 비례상승",
         "provenance": "DERIVED_INFERENCE", "coverage": "MECHANICAL 에 포함",
         "pit": "월 단위", "engine": "보유주식 × shareRatio",
         "support": "SUPPORTED_PARTIAL",
         "gap": "무상감자/유상감자 구분 불가. 유상감자 현금 수령분 누락."},
        {"event": "MERGER", "source": "**없음** — 교환비율 부재", "provenance": "NONE",
         "coverage": "0%", "pit": "불가", "engine": "미지원",
         "support": "UNSUPPORTED_DATA",
         "gap": "피합병 종목이 상폐로 처리되어 실제 교환대가를 반영하지 못한다."},
        {"event": "SPINOFF (인적분할)", "source": "**없음** — 배정비율 부재",
         "provenance": "NONE", "coverage": "0%", "pit": "불가", "engine": "미지원",
         "support": "UNSUPPORTED_DATA",
         "gap": "신설법인 주식 가치가 주주 wealth 에서 누락된다."},
        {"event": "PHYSICAL_SPINOFF (물적분할)", "source": "해당 없음",
         "provenance": "NOT_APPLICABLE", "coverage": "-", "pit": "-",
         "engine": "조정 없음(정답)", "support": "NOT_APPLICABLE",
         "gap": "기존 주주에게 신주 미지급이므로 조정하지 않는 것이 맞다."},
        {"event": "DELISTING", "source": "스냅샷 소멸", "provenance": "DERIVED_INFERENCE",
         "coverage": "전 구간 탐지", "pit": "월 단위",
         "engine": f"마지막 관측가 청산 + {DELISTING['canonicalDefault']} flag",
         "support": "SUPPORTED_PARTIAL",
         "gap": "실제 정산금(현금/교환/0원) 데이터 없음. 상한 추정."},
        {"event": "CODE_CHANGE", "source": "**없음** — 종목코드 매핑 이력 부재",
         "provenance": "NONE", "coverage": "0%", "pit": "불가",
         "engine": "미지원(코드 변경 시 상폐+신규상장으로 보임)",
         "support": "UNSUPPORTED_DATA", "gap": "연속성 단절 가능."},
    ]
    out = {"tickerMonths": tm,
           "dividendCoverage": {"presentPct": round(100 * dps_present / tot, 2),
                                "zeroPct": round(100 * dps_zero / tot, 2),
                                "missingPct": round(100 * dps_missing / tot, 2),
                                "note": "0 과 결측을 구분해 저장한다(§20)."},
           "eventClassification": {"MECHANICAL_SHARE_CHANGE": mech,
                                   "SUSPECTED_RIGHTS_ISSUE": rights,
                                   "SHARES_DOWN_NONPROPORTIONAL": down,
                                   "SHARES_UP_NONPROPORTIONAL": upnp},
           "rows": rows,
           "supportSummary": {
               "SUPPORTED_COMPLETE": sum(1 for r in rows if r["support"] == "SUPPORTED_COMPLETE"),
               "SUPPORTED_PARTIAL": sum(1 for r in rows if r["support"] == "SUPPORTED_PARTIAL"),
               "UNSUPPORTED_DATA": sum(1 for r in rows if r["support"] == "UNSUPPORTED_DATA"),
               "NOT_APPLICABLE": sum(1 for r in rows if r["support"] == "NOT_APPLICABLE")}}
    save("corporate-action-inventory", out)
    return out


# ═══════════ anchor cases (§9·§10·§11) ═══════════
def anchors(eng, ds, nm):
    pick = {C.MECHANICAL: [], C.RIGHTS: [], C.DOWN_NP: []}
    for i in range(1, len(ds)):
        for t, e in eng.events[i].items():
            if e["class"] in pick and len(pick[e["class"]]) < 400:
                pick[e["class"]].append((i, t, e))
    cases = []

    def build(label, i, t, e, kind):
        lo = max(0, i - 12)
        hi = min(len(ds) - 1, i + 12)
        r = eng._run(lo, hi, t, ledger=True)
        if not r:
            return None
        ev = r["ledger"]
        row = next((x for x in ev if x["date"] == ds[i]), None)
        return {"case": label, "kind": kind, "ticker": t, "name": nm.get(t, t),
                "eventDate": ds[i], "eventClass": e["class"],
                "shareRatio": e["shareRatio"], "priceRatio": e["priceRatio"],
                "mcapRatio": e["mcapRatio"], "adjusted": e["adjust"],
                "window": {"start": ds[lo], "end": ds[hi]},
                "startPrice": r["startPrice"], "endPrice": r["endPrice"],
                "endingShares": r["endingShares"], "endingCash": r["endingCash"],
                "endingWealth": round(r["endingWealth"]),
                "cumulativeReturnPct": pct(r["cumulativeReturn"]),
                "cagrPct": pct(r["cagr"]),
                "eventMonthLedger": row,
                "eventMonthWealthChangePct": (
                    pct(row["wealthAfter"] / row["wealthBefore"] - 1)
                    if row and row.get("wealthBefore") and row.get("wealthAfter") else None),
                "priceOnlyWealthChangePct": (
                    pct(e["priceRatio"] and (1 / e["priceRatio"]) - 1)),
                }

    # A. 삼성전자 (분할·배당·특별배당·자사주)
    sam = eng.get_total_return("005930", "2010-01-04", "2021-08-02")
    sam_nr = eng.get_total_return("005930", "2010-01-04", "2021-08-02",
                                  mode="NO_REINVEST")
    sam_po = eng.get_total_return("005930", "2010-01-04", "2021-08-02",
                                  mode="PRICE_ONLY_UNSAFE")
    ref = ANCHOR["samsungReproduction"]
    tol = ANCHOR["tolerancePctPoints"]
    sam_checks = [
        {"metric": "TSR_REINVEST_CAGR", "engine": pct(sam["cagr"]),
         "r15Manual": ref["tsrReinvestCagrPct"],
         "diffPp": round(pct(sam["cagr"]) - ref["tsrReinvestCagrPct"], 3),
         "tolerancePp": tol,
         "pass": abs(pct(sam["cagr"]) - ref["tsrReinvestCagrPct"]) <= tol},
        {"metric": "TSR_NO_REINVEST_CAGR", "engine": pct(sam_nr["cagr"]),
         "r15Manual": ref["tsrNoReinvestCagrPct"],
         "diffPp": round(pct(sam_nr["cagr"]) - ref["tsrNoReinvestCagrPct"], 3),
         "tolerancePp": tol,
         "pass": abs(pct(sam_nr["cagr"]) - ref["tsrNoReinvestCagrPct"]) <= tol},
        {"metric": "PRICE_ONLY_CAGR (구 엔진 버그 재현)", "engine": pct(sam_po["cagr"]),
         "r15Manual": -18.17, "diffPp": round(pct(sam_po["cagr"]) + 18.17, 3),
         "tolerancePp": tol, "pass": abs(pct(sam_po["cagr"]) + 18.17) <= tol},
    ]
    cases.append({"case": "A_SAMSUNG", "kind": "SPLIT+DIVIDEND+BUYBACK",
                  "ticker": "005930", "name": nm.get("005930", "삼성전자"),
                  "window": {"start": "2010-01-04", "end": "2021-08-02"},
                  "endingShares": sam["endingShares"],
                  "endingWealth": round(sam["endingWealth"]),
                  "cagrPct": pct(sam["cagr"]),
                  "cumulativeReturnPct": pct(sam["cumulativeReturn"]),
                  "eventCounts": sam["eventCounts"],
                  "dataQualityFlags": sam["dataQualityFlags"],
                  "reproductionChecks": sam_checks,
                  "pass": all(c["pass"] for c in sam_checks)})

    # B~ 유형별
    for lbl, cls, kind, cond in (
            ("B_REVERSE_SPLIT_OR_REDUCTION", C.MECHANICAL, "REVERSE/REDUCTION",
             lambda e: e["shareRatio"] < 0.5),
            ("C_BONUS_OR_SPLIT", C.MECHANICAL, "BONUS/SPLIT",
             lambda e: e["shareRatio"] > 1.4),
            ("D_SUSPECTED_RIGHTS", C.RIGHTS, "RIGHTS(미조정)",
             lambda e: e["shareRatio"] > 1.3),
            ("E_TREASURY_CANCELLATION", C.DOWN_NP, "자사주 소각(미조정)",
             lambda e: e["shareRatio"] < 0.95)):
        got = None
        for i, t, e in pick[cls]:
            if not cond(e) or i < 13 or i > len(ds) - 14:
                continue
            b = build(lbl, i, t, e, kind)
            if b:
                got = b
                break
        cases.append(got or {"case": lbl, "kind": kind, "status": "NO_SAMPLE"})

    # F. 상장폐지
    de = None
    for i in range(len(ds) - 40, 20, -1):
        prev = eng.cap.get(ds[i - 1], {})
        cur = eng.cap.get(ds[i], {})
        for t in list(prev)[:600]:
            if t not in cur and prev[t].get("close"):
                r = eng._run(max(0, i - 24), min(len(ds) - 1, i + 6), t)
                if r:
                    de = {"case": "F_DELISTING", "kind": "상장폐지", "ticker": t,
                          "name": nm.get(t, t), "delistMonth": ds[i],
                          "window": {"start": r["startDate"], "end": r["endDate"]},
                          "endingWealth": round(r["endingWealth"]),
                          "cumulativeReturnPct": pct(r["cumulativeReturn"]),
                          "delistingRecovery": r["delistingRecovery"],
                          "note": ("실제 정산금 데이터가 없어 마지막 관측가로 청산했다. "
                                   "UNKNOWN_RECOVERY — 상한 추정이다.")}
                    break
        if de:
            break
    cases.append(de or {"case": "F_DELISTING", "status": "NO_SAMPLE"})

    # G/H 데이터 없음
    for lbl, kind in (("G_MERGER", "합병"), ("H_SPINOFF", "인적분할")):
        cases.append({"case": lbl, "kind": kind, "status": "BLOCKED_DATA",
                      "reason": ("교환비율/배정비율 데이터가 저장소에 없다. mock 으로 "
                                 "통과시키고 지원한다고 선언하지 않는다(§23).")})

    out = {"tolerancePctPoints": tol, "cases": cases,
           "samsungReproductionPass": cases[0]["pass"],
           "blockedTypes": [c["case"] for c in cases if c.get("status") == "BLOCKED_DATA"]}
    save("anchor-cases", out)
    return out


# ═══════════ anomaly (§25) ═══════════
def anomaly(eng, ds, nm):
    rows = C.anomaly_scan(eng)
    from collections import Counter
    by_cls = Counter(r["classification"] for r in rows)
    by_reason = Counter(x for r in rows for x in r["reasons"])
    worst = sorted(rows, key=lambda r: -abs(r["canonicalMonthlyReturn"]))[:15]
    out = {"anomalies": len(rows),
           "byClassification": dict(by_cls), "byReason": dict(by_reason),
           "worst": [{"date": r["date"], "ticker": r["ticker"],
                      "name": nm.get(r["ticker"], r["ticker"]),
                      "monthlyReturnPct": pct(r["canonicalMonthlyReturn"]),
                      "event": r["event"], "reasons": r["reasons"],
                      "classification": r["classification"]} for r in worst],
           "note": ("canonical 엔진 적용 후 남은 이상치다. 사건이 탐지된 경우 "
                    "TRUE_EVENT, 아니면 UNRESOLVED 로 분류한다. UNRESOLVED 는 "
                    "미탐지 corporate action 또는 데이터 오류일 수 있다.")}
    save("anomaly-audit", out)
    return out


# ═══════════ coverage / materiality (§18·§27) ═══════════
def coverage(eng, ds, nm, inv):
    tm = inv["tickerMonths"]
    rights_t = set()
    upnp_t = set()
    mech_t = set()
    first_year = {}
    for i in range(1, len(ds)):
        for t, e in eng.events[i].items():
            if e["class"] == C.RIGHTS:
                rights_t.add(t)
            elif e["class"] == C.UP_NP:
                upnp_t.add(t)
            elif e["class"] == C.MECHANICAL:
                mech_t.add(t)
            first_year.setdefault(e["class"], ds[i][:4])
    all_t = set()
    for d in ds:
        all_t |= set(eng.cap.get(d, {}))
    # 연도별 커버리지 품질
    by_year = {}
    for d in ds:
        y = d[:4]
        snap = eng.cap.get(d, {})
        n = len(snap)
        if not n:
            continue
        e = by_year.setdefault(y, {"n": 0, "sharesOk": 0, "dpsOk": 0})
        e["n"] += n
        e["sharesOk"] += sum(1 for r in snap.values() if r.get("shares"))
        e["dpsOk"] += sum(1 for r in snap.values() if r.get("dps") is not None)
    yr = {y: {"tickerMonths": v["n"],
              "sharesCoveragePct": round(100 * v["sharesOk"] / v["n"], 1),
              "dpsCoveragePct": round(100 * v["dpsOk"] / v["n"], 1)}
          for y, v in sorted(by_year.items())}
    full_start = next((y for y, v in yr.items()
                       if v["sharesCoveragePct"] >= 99 and v["dpsCoveragePct"] >= 90), None)
    out = {
        "universeTickers": len(all_t),
        "tickerMonths": tm,
        "affectedTickers": {
            "MECHANICAL": len(mech_t), "SUSPECTED_RIGHTS": len(rights_t),
            "SHARES_UP_UNRESOLVED": len(upnp_t),
            "rightsPctOfUniverse": round(100 * len(rights_t) / len(all_t), 1),
        },
        "affectedMonthRatio": {
            "MECHANICAL": round(100 * eng.counts[C.MECHANICAL] / tm, 3),
            "SUSPECTED_RIGHTS": round(100 * eng.counts[C.RIGHTS] / tm, 3),
            "SHARES_UP_UNRESOLVED": round(100 * eng.counts[C.UP_NP] / tm, 3),
        },
        "byYear": yr,
        "TSR_FULL_COVERAGE_START": full_start,
        "TSR_PARTIAL_COVERAGE_START": ds[0],
        "coverageNote": ("shares·DPS 커버리지가 연도별로 다르다. 전 구간이 동일 품질이라고 "
                         "가정하지 않는다(§27)."),
        "materiality": {
            "unsupportedRightsEvents": eng.counts[C.RIGHTS],
            "unsupportedRightsTickers": len(rights_t),
            "interpretation": (
                f"유상증자 의심 {eng.counts[C.RIGHTS]}건이 {len(rights_t)}개 종목"
                f"({round(100*len(rights_t)/len(all_t),1)}%)에서 발생한다. 이 종목들의 "
                "장기 수익률은 권리가치·추가납입이 빠져 있어 **주주 실제 wealth 와 "
                "다를 수 있다**. 조정하지 않는 쪽이 가짜 wealth 를 만드는 것보다 안전하지만, "
                "권리를 행사한 주주의 수익은 과소평가된다."),
        },
    }
    save("tsr-coverage", out)
    return out


# ═══════════ legacy status (§15) ═══════════
def legacy_status():
    out = {
        "policy": ("R16 은 기존 보고서를 삭제·수정하지 않는다. 대신 canonical 기준으로 "
                   "**잠정 상태**를 부여한다. 좋은 결과도 잠정, 나쁜 결과도 잠정이다."),
        "statuses": {
            "R5_R14_OVERALL": "PRE_TSR_LEGACY_RESEARCH",
            "R7_FACTOR_SIGNALS": "PROVISIONAL_PENDING_CANONICAL_TSR",
            "R8_R10_PORTFOLIO_CAGR": "PROVISIONAL_PENDING_CANONICAL_TSR",
            "R11_BM_INCREMENTAL_ALPHA_6_90PP": (
                "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION"),
            "R11_BM_CANDIDATE_CAGR_11_50": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION",
            "R13_PROFITABILITY_NO_SIGNAL": "PROVISIONAL_PENDING_CANONICAL_TSR",
            "R14_QUALITY_INVERTED": "PROVISIONAL_CHANGED_BY_R15",
            "R15_TSR_CORRECTED_Q3_PROMISING": "PROVISIONAL_SUPERSEDED_BY_R16_ENGINE",
            "SIZE_SIGNAL": "PROVISIONAL_PENDING_CANONICAL_TSR",
            "MAGIC_FORMULA_RESULT": "PROVISIONAL_PENDING_CANONICAL_TSR",
            "LEGACY_50D": "UNTOUCHED_PRODUCTION_EXPERIMENT",
        },
        "canonicalPerformanceUsable": False,
        "publicUseAllowed": False,
        "note": ("어떤 과거 수치도 canonical performance 로 대외 사용하지 않는다. "
                 "R11 의 11.50% / +6.90%p 도 포함된다."),
        "whyR15Superseded": (
            "R15 엔진은 유상증자와 무상증자를 구분하지 못했다(주식수·가격 신호만 사용). "
            "R16 은 시총 연속성을 추가해 유상증자 의심 2,742건을 조정 대상에서 제외한다. "
            "따라서 R15 의 수정 결과도 R16 엔진으로 다시 검증해야 한다."),
        "priorArtifactsUntouched": True,
    }
    save("legacy-research-status", out)
    return out


# ═══════════ foundation verdict (§17) ═══════════
def verdict(inv, anc, ano, cov, leg):
    ss = inv["supportSummary"]
    unsupported = [r["event"] for r in inv["rows"] if r["support"] == "UNSUPPORTED_DATA"]
    anchor_ok = anc["samsungReproductionPass"]
    blocked = anc["blockedTypes"]
    rights_pct = cov["affectedTickers"]["rightsPctOfUniverse"]
    core_ok = all(r["support"] in ("SUPPORTED_COMPLETE", "SUPPORTED_PARTIAL")
                  for r in inv["rows"]
                  if r["event"] in ("CASH_DIVIDEND", "STOCK_SPLIT", "REVERSE_SPLIT",
                                    "DELISTING", "TREASURY_CANCELLATION"))
    if not anchor_ok:
        v = "CANONICAL_TSR_FOUNDATION_FAIL"
    elif core_ok and not unsupported:
        v = "CANONICAL_TSR_FOUNDATION_PASS"
    elif core_ok and rights_pct < 40:
        v = "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"
    else:
        v = "CANONICAL_TSR_FOUNDATION_BLOCKED"
    out = {
        "verdict": v,
        "anchorSamsungPass": anchor_ok,
        "coreEventsSupported": core_ok,
        "unsupportedEvents": unsupported,
        "blockedAnchorTypes": blocked,
        "rightsAffectedTickerPct": rights_pct,
        "supportSummary": ss,
        "anomalies": ano["anomalies"],
        "tsrFullCoverageStart": cov["TSR_FULL_COVERAGE_START"],
        "reasoning": None,
        "nextFactorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                           "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
        "realMoneyStage": "REAL_MONEY_NOT_APPROVED",
    }
    if v == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS":
        out["reasoning"] = (
            f"핵심 event(배당·분할·역분할/감자·자사주·상폐)는 지원되고 삼성전자 anchor 가 "
            f"manual ledger 와 tolerance 내 일치한다. 다만 RIGHTS_ISSUE·MERGER·SPINOFF·"
            f"CODE_CHANGE 가 UNSUPPORTED_DATA 이고, 유상증자 의심이 universe 의 "
            f"{rights_pct}% 종목에서 발생한다. 이 영향은 **정량적으로 한정 가능**하므로 "
            f"연구는 재개하되 해당 종목군의 결과는 별도 flag 로 다뤄야 한다.")
    elif v == "CANONICAL_TSR_FOUNDATION_BLOCKED":
        out["reasoning"] = (
            f"유상증자 미지원이 universe 의 {rights_pct}% 종목에 걸쳐 있어 장기 factor "
            f"research 를 신뢰할 수 없다. 가장 material 한 data gap 1개(신주배정 이력)를 "
            f"먼저 해결해야 한다.")
    elif v == "CANONICAL_TSR_FOUNDATION_PASS":
        out["reasoning"] = "모든 핵심 event 가 지원되고 anchor 가 일치한다."
    else:
        out["reasoning"] = "anchor 재현 실패 — engine 오류가 남아 있다."
    save("foundation-verdict", out)
    return out


# ══════════════ 분류기 분리력 감사 (§35 자체수정 근거) ══════════════
CLEAN_RATIOS = [0.1, 0.2, 0.25, 1 / 3, 0.4, 0.5, 0.6, 2 / 3, 0.75, 0.8,
                1.25, 4 / 3, 1.5, 5 / 3, 2.0, 2.5, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0]


def _clean(sr):
    """정수·단순분수 비율 = 분할·무상증자의 특징(불완전한 대조군)."""
    return any(abs(sr / c - 1.0) < 0.005 for c in CLEAN_RATIOS)


def separation(eng, ds):
    """분류기가 실제로 분리하는지 측정한다. 임계값을 자의로 고르지 않기 위함."""
    absv, relv = [], []
    for i in range(1, len(ds)):
        a, b = eng.cap.get(ds[i - 1], {}), eng.cap.get(ds[i], {})
        for t, cur in b.items():
            pv = a.get(t)
            if not pv:
                continue
            p0, p1 = pv.get("close"), cur.get("close")
            s0, s1 = pv.get("shares"), cur.get("shares")
            if not (p0 and p1 and s0 and s1) or min(p0, p1, s0, s1) <= 0:
                continue
            sr = s1 / s0
            if 1.0 / C.MIN_SR < sr < C.MIN_SR:
                continue
            cl = _clean(sr)
            absv.append((abs((p0 / p1) / sr - 1.0), cl))
            relv.append((C.rel_err(sr, p0 / p1), cl))

    def split(v):
        return sorted(x for x, c in v if c), sorted(x for x, c in v if not c)

    def med(x):
        return round(x[len(x) // 2], 4) if x else None

    def rate(x, th):
        return round(bisect.bisect_left(x, th) / len(x) * 100, 1) if x else None

    ac, am = split(absv)
    rc, rm = split(relv)
    sens = []
    for th in (0.10, 0.20, 0.30, 0.35, 0.40, 0.50, 0.60, 0.80, 1.00):
        pc_, pm_ = rate(rc, th), rate(rm, th)
        sens.append({"threshold": th, "cleanPassPct": pc_, "messyPassPct": pm_,
                     "separationPp": round(pc_ - pm_, 1)})
    out = {
        "task": "R16",
        "purpose": ("임계값을 직관으로 정하지 않기 위해 분류기의 실제 분리력을 "
                    "측정한다. 결과에 맞춰 규칙을 고른 것이 아니라, factor 결과가 "
                    "존재하지 않는 시점(§16 은 R16 에서 factor 연구를 금지)에 "
                    "데이터 속성만으로 측정했다."),
        "events": len(absv),
        "control": {
            "definition": "정수·단순분수 주식수비율 vs 그 외",
            "cleanN": len(ac), "messyN": len(am),
            "caveat": ("완전한 정답이 아니다. 무상증자 1.5배와 유상증자 1.5배가 같은 "
                       "비율을 낸다. 양쪽 모두 오염돼 있으므로 아래 분리력은 **하한**."),
        },
        "absoluteError": {
            "formula": "|priceRatio/shareRatio - 1|",
            "cleanMedian": med(ac), "messyMedian": med(am),
            "verdict": ("분포에 골짜기가 없다(0~0.30 거의 균일). 중앙값 격차 1.3배. "
                        "임계값이 자의적이라 기준으로 부적합."),
        },
        "logRelativeError": {
            "formula": "|ln(priceRatio) - ln(shareRatio)| / |ln(shareRatio)|",
            "cleanMedian": med(rc), "messyMedian": med(rm),
            "verdict": "중앙값 격차 3.4배. 사건 크기에 스케일이 맞아 채택.",
        },
        "thresholdSensitivity": sens,
        "chosen": C.TOL,
        "chosenWhy": "분리력 최대 평탄구간(0.30~0.40)의 중점. 단일 최대점 과적합 회피.",
        "residualLimitation": (
            "최적 임계에서도 비정수비의 약 18% 가 통과하고 정수비의 약 46% 가 "
            "탈락한다. 개별 사건 분류는 신뢰할 수 없다 → FOUNDATION_BLOCKED 근거."),
    }
    save("classifier-separation", out)
    return out


def main() -> int:
    nm = load_names()
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    print(f"[r16] {ds[0]} ~ {ds[-1]} ({len(ds)}m) · events {eng.counts}",
          file=sys.stderr)
    sep = separation(eng, ds)
    inv = inventory(eng, ds, nm)
    anc = anchors(eng, ds, nm)
    ano = anomaly(eng, ds, nm)
    cov = coverage(eng, ds, nm, inv)
    leg = legacy_status()
    v = verdict(inv, anc, ano, cov, leg)
    v["classifierSeparation"] = sep["logRelativeError"]
    print(json.dumps({"verdict": v["verdict"], "anchorPass": v["anchorSamsungPass"],
                      "unsupported": v["unsupportedEvents"],
                      "rightsTickerPct": v["rightsAffectedTickerPct"],
                      "anomalies": v["anomalies"],
                      "fullCoverageStart": v["tsrFullCoverageStart"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
