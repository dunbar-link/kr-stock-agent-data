#!/usr/bin/env python3
"""R23 anchor · 전체 reconciliation · 기대편향 재계산 · foundation 최종 판정.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

기존 R16~R22 산출물을 덮어쓰지 않는다(§27).
threshold 와 bias 계산식은 R22 것을 그대로 쓴다(§18·§19 변경 금지).

안전: 계산 전용 · 네트워크 0 · production write 0.
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r17_wealth as W  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402
from r17_precommit import VERDICT_RULE  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r23-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r23] saved {p.name}", file=sys.stderr)


def merged():
    r22 = json.loads((RD / "r22-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r23 = json.loads((RD / "r23-final-terms-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in r23}
    out = []
    for m in r22:
        e = by.get((m["ticker"], m["date"]))
        if not e:
            out.append({**m, "r23Source": None})
            continue
        row = {**m, "r23Source": "R23_REGISTRATION",
               "r23AllocationStatus": e["allocationStatus"],
               "r23WealthStatus": e["wealthStatus"],
               "r23Confidence": e["confidence"],
               "r23Flags": e["flags"],
               "r23RightsRatio": e.get("finalRightsRatio"),
               "r23RatioBasis": e.get("ratioBasis"),
               "r23IssuePrice": e.get("finalIssuePrice"),
               "r23PriceSource": e.get("priceSource"),
               "r23EligibleOldShares": e.get("eligibleOldShares"),
               "r23TreasuryExcluded": e.get("treasuryExcluded"),
               "r23EntitledShares":
                   e.get("ENTITLED_TO_EXISTING_SHAREHOLDERS")}
        if e["wealthStatus"] == "WEALTH_CONFIRMED" and e.get("holderRightFinal"):
            row.update({"resolvedWealth": True, "holderRight": True,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R23_ALLOCATION_RECOVERED",
                        "label": "CONFIRMED_RIGHTS",
                        "dartRatio": e.get("finalRightsRatio"),
                        "issuePriceDerived": e.get("finalIssuePrice")})
        else:
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_PARTIAL",
                        "wealthReason": "R23_ALLOCATION_STILL_MISSING"})
        out.append(row)
    return out


# ═══════════ manual ledger (§16) ═══════════
def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    res = json.loads((RD / "r23-final-terms-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    ok = [e for e in res
          if e["wealthStatus"] == "WEALTH_CONFIRMED" and e.get("holderRightFinal")
          and e.get("finalRightsRatio") and e.get("finalIssuePrice")
          and eng.pos.get(e["date"], 0) > 0]
    cases = []
    for e in ok:
        t, dt = e["ticker"], e["date"]
        i = eng.pos[dt]
        p_cum, p_ex = eng.price(i - 1, t), eng.price(i, t)
        if not p_cum or not p_ex:
            continue
        r, k = e["finalRightsRatio"], e["finalIssuePrice"]
        # R17 POLICY A 정본(§10 변경 금지)
        if k < p_ex:
            s1, ext, note = 1.0 + r, r * k, "청약 (K < 권리락가)"
        else:
            s1, ext, note = 1.0, 0.0, "합리적 실권 (K >= 권리락가)"
        man = (s1 * p_ex - ext) / p_cum - 1.0
        run = rw.run(t, i - 1, i, reinvest=False)
        er = run["twr"] if run else None
        cases.append({
            "ticker": t, "date": dt, "confidence": e["confidence"],
            "allocationStatus": e["allocationStatus"],
            "ratioBasis": e.get("ratioBasis"),
            "priceSource": e.get("priceSource"),
            "manual": {
                "oldShares": 1.0, "startPrice": p_cum,
                "eligibleOldShares": e.get("eligibleOldShares"),
                "rightsRatio": r,
                "entitledShares": e.get("ENTITLED_TO_EXISTING_SHAREHOLDERS"),
                "finalIssuePrice": k, "externalContribution": ext,
                "resultingShares": s1, "exPrice": p_ex,
                "rightsEconomicValue": r * max(0.0, p_ex - k),
                "manualTsrAdjustment": man, "treatment": note},
            "engineTsrAdjustment": er,
            "diffPp": None if er is None else round((er - man) * 100, 6),
            "pass": er is not None and abs(er - man) < 5e-4})
    out = {"task": "R23", "cases": cases, "requestedMin": 5,
           "obtained": len(cases), "atLeastFive": len(cases) >= 5,
           "allResolvedCovered": len(cases) == len(ok),
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "policySource": "R17 POLICY_A + 실권 규칙 (§10 변경 금지)",
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


def continuity(eng, matches):
    rw = W.RightsWealth(eng, matches)
    tgt = [m for m in matches
           if m.get("holderRight") and m.get("issuePriceDerived")]
    free, diffs = 0, []
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=False)
        if a and a["externalContribution"] == 0 and a["endShares"] > 1.0 + 1e-9:
            free += 1
        b = rw.run(m["ticker"], i - 1, i, reinvest=True)
        c = eng._run(i - 1, i, m["ticker"], mode="CANONICAL_TSR")
        if b and c:
            diffs.append(b["twr"] - c["cumulativeReturn"])
    theo = [u for u in (W.understatement(m, eng) for m in tgt) if u is not None]
    tm = statistics.median(theo) if theo else None
    md = statistics.median(diffs) if diffs else None
    match = (tm is not None and md is not None and abs(md - tm) < 0.02)
    out = {"task": "R23", "method": "R18~R22 방식 재사용 — R16 대비 차분",
           "rightsEventsTested": len(tgt),
           "eventMonthMedianDiff": round(md, 4) if md else None,
           "theoreticalMedian": round(tm, 4) if tm else None,
           "diffMatchesTheory": match,
           "negativeDiffCount": sum(1 for x in diffs if x < -1e-9),
           "freeWealthEvents": free, "noFreeWealth": free == 0,
           "pass": match and free == 0 and all(x >= -1e-9 for x in diffs)}
    save("ex-rights-continuity", out)
    return out


# ═══════════ 기대편향 (§18) — R22 식 그대로 ═══════════
def expected_bias(eng, matches):
    us = [u for u in (W.understatement(m, eng) for m in matches
                      if m.get("holderRight") and m.get("issuePriceDerived"))
          if u is not None]
    cond = statistics.median(us) if us else None
    doc = json.loads((RD / "r18-document-parser-results-latest.json")
                     .read_text(encoding="utf-8"))["byMethod"]
    hr = sum(v for k, v in doc.items()
             if k in ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED"))
    tot = sum(v for k, v in doc.items() if k)
    p_pop = (hr / tot) if tot else 0.0
    unres = [m for m in matches if not m.get("resolvedWealth")]

    def assign_p(m):
        reason = m.get("wealthReason")
        if m.get("r21Entitlement") == "NO":
            return 0.0, "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R21"
        if reason in ("R23_ALLOCATION_STILL_MISSING",
                      "R22_FINAL_TERMS_STILL_INCOMPLETE",
                      "R21_ENTITLEMENT_TERMS_MISSING",
                      "R20_FINAL_TERMS_INCOMPLETE"):
            return 1.0, "HOLDER_RIGHT_TERMS_MISSING", "DIRECT"
        if m.get("r19Entitlement") == "NO":
            return 0.0, "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R19"
        if m.get("r19Entitlement") == "YES":
            return 1.0, "TRUE_RIGHTS", "DIRECT_R19"
        if reason == "REDUCTION_PAID_OR_FREE_UNKNOWN":
            return 0.0, "OTHER_CAPITAL_ACTION", "DIRECT_R17"
        return p_pop, "TRUE_UNRESOLVED", "POPULATION_ESTIMATE"

    contrib, ps = {}, []
    for m in unres:
        p, grp, src = assign_p(m)
        ps.append(p)
        c = contrib.setdefault(grp, {"events": 0, "sumP": 0.0, "pSource": src})
        c["events"] += 1
        c["sumP"] += p
    p_eff = (sum(ps) / len(ps)) if ps else 0.0
    exp = p_eff * cond if cond else 0.0
    for g, c in contrib.items():
        c["avgP"] = round(c["sumP"] / c["events"], 4) if c["events"] else 0.0
        c["biasContributionPct"] = round(
            (c["sumP"] / len(ps)) * cond * 100, 3) if (ps and cond) else 0.0

    tot_n = sum(c["events"] for c in contrib.values())
    tot_p = sum(c["sumP"] for c in contrib.values())
    sens = []
    for g, c in sorted(contrib.items()):
        n2, p2 = tot_n - c["events"], tot_p - c["sumP"]
        b2 = (p2 / n2 * cond * 100) if n2 else 0.0
        sens.append({"resolveGroup": g, "events": c["events"], "avgP": c["avgP"],
                     "biasIfFullyResolvedPct": round(b2, 3),
                     "passesGate": b2 <= 2.0})

    # §21 — 누적 감도: 이 군에서 k 건을 더 풀면 얼마가 되는가
    hr_grp = contrib.get("HOLDER_RIGHT_TERMS_MISSING")
    cumulative = []
    if hr_grp and cond:
        for k in range(0, hr_grp["events"] + 1):
            n2, p2 = tot_n - k, tot_p - k * 1.0
            b2 = (p2 / n2 * cond * 100) if n2 else 0.0
            cumulative.append({"resolvedFromGroup": k,
                               "biasPct": round(b2, 3), "passesGate": b2 <= 2.0})

    out = {"task": "R23",
           "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
           "formulaUnchangedFromR22": True,
           "conditionalMedian": round(cond, 4) if cond else None,
           "populationP": round(p_pop, 4),
           "unresolvedEvents": len(unres),
           "effectiveP": round(p_eff, 4),
           "expectedBias": round(exp, 5),
           "expectedBiasPct": round(exp * 100, 3),
           "progression": {"R18": 3.47, "R19": 5.316, "R20": 3.442,
                           "R21": 3.470, "R22": 2.416,
                           "R23": round(exp * 100, 3)},
           "thresholdPct": VERDICT_RULE[
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
               "medianUnderstatementPctMax"],
           "r22SensitivityProjection": 1.520,
           "projectionVsActual": (
               "R22 감도 1.520% 는 11건이 **전부** 해결될 때 값이다. 실제 해결분만 "
               "반영한 실측값을 판정에 쓴다."),
           "gateSensitivity": {"current": {"events": tot_n,
                                           "biasPct": round(exp * 100, 3)},
                               "ifGroupFullyResolved": sens,
                               "thresholdPct": 2.0},
           "cumulativeSensitivity": cumulative,
           "decomposition": contrib}
    save("expected-bias", out)
    return out


def materiality(eng, matches, universe, bias):
    unres = [m for m in matches if not m.get("resolvedWealth")]
    res = [m for m in matches if m.get("resolvedWealth")]
    ut = {m["ticker"] for m in unres}
    months = {m["date"] for m in unres}
    srs = sorted(abs(m["shareRatio"] - 1.0) for m in unres)
    allsr = sorted(abs(m["shareRatio"] - 1.0) for m in matches)
    extreme = [m for m in unres if m["shareRatio"] >= 2.0 or m["shareRatio"] <= 0.5]
    reasons, layers = {}, {}
    for m in unres:
        k = m.get("wealthReason", "?")
        reasons[k] = reasons.get(k, 0) + 1
    for m in matches:
        k = m.get("wealthLayer") or ("WEALTH_CONFIRMED" if m.get("resolvedWealth")
                                     else "WEALTH_UNRESOLVED")
        layers[k] = layers.get(k, 0) + 1
    out = {"task": "R23", "suspectedTotal": len(matches),
           "wealthResolved": len(res), "wealthUnresolved": len(unres),
           "byWealthLayer": layers, "unresolvedReasons": reasons,
           "unresolvedTickers": len(ut), "universeTickers": universe,
           "unresolvedTickerPct": round(len(ut) / universe * 100, 2),
           "unresolvedEventPct": round(len(unres) / len(matches) * 100, 2),
           "unresolvedMonthPct": round(len(months) / len(eng.dates) * 100, 2),
           "unresolvedMagnitudePct": round(sum(srs) / sum(allsr) * 100, 2)
           if allsr else None,
           "extremeDiscontinuityEvents": len(extreme),
           "extremeDiscontinuityPct": round(len(extreme) / len(matches) * 100, 2),
           "expectedBiasPct": bias["expectedBiasPct"],
           "biasDirection": "CONSERVATIVE_UNDERSTATEMENT",
           "biasWhy": ("미해결·PARTIAL 은 전부 미조정으로 남는다. 미조정은 직접 "
                       "권리가 없던 사건이면 정답이고 있었으면 과소평가다. 어느 "
                       "쪽도 wealth 를 부풀리지 않는다."),
           "progression": {"R16": 40.8, "R17": 25.57, "R18": 13.31, "R19": 6.74,
                           "R20": 5.43, "R21": 3.36, "R22": 3.04,
                           "R23": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


def verdict(mat, anc, cont, bias):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct, bpct = mat["unresolvedTickerPct"], bias["expectedBiasPct"]
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]
    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anc["allPass"],
        "exRightsContinuityPass": cont["pass"],
        "noFakeWealth": cont["noFreeWealth"],
        "unresolvedTickerPctUnder20": tpct < pl["unresolvedTickerPctMax"],
        "unresolvedTickerPctUnder5": tpct < p["unresolvedTickerPctMax"],
        "expectedBiasUnder2pct": bpct <= pl["medianUnderstatementPctMax"],
        "extremeDiscontinuityUnder1pct":
            mat["extremeDiscontinuityPct"] < p["extremeDiscontinuityPctMax"],
    }
    if not engine_ok:
        v = "CANONICAL_TSR_FOUNDATION_FAIL"
    elif (checks["unresolvedTickerPctUnder5"]
          and checks["extremeDiscontinuityUnder1pct"]
          and checks["expectedBiasUnder2pct"]):
        v = "CANONICAL_TSR_FOUNDATION_PASS"
    elif checks["unresolvedTickerPctUnder20"] and checks["expectedBiasUnder2pct"]:
        v = "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"
    else:
        v = "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"
    out = {"task": "R23", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct, "expectedBiasPct": bpct,
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "maxExpectedBiasPct": pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "biasFormulaUnchanged": bias["formulaUnchangedFromR22"],
           "progression": {"unresolvedTickerPct": mat["progression"],
                           "expectedBiasPct": bias["progression"]},
           "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                          "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("PASS 가 나도 기존 결과를 자동 승격하지 않는다(§24). "
                        "다음 작업에서 canonical TSR 로 처음부터 재계산한다.")}}
    save("foundation-verdict", out)
    return out


def main() -> int:
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    universe = len({t for d in ds for t in cap.get(d, {})})
    matches = merged()

    lay = {}
    for m in matches:
        k = m.get("wealthLayer") or ("WEALTH_CONFIRMED" if m.get("resolvedWealth")
                                     else "WEALTH_UNRESOLVED")
        lay[k] = lay.get(k, 0) + 1
    save("full-reconciliation", {
        "task": "R23", "total": len(matches),
        "r23Merged": sum(1 for m in matches if m.get("r23Source")),
        "byWealthLayer": lay,
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "note": "R22 2,742건에 R23 증권신고서 근거를 merge 한 최종 재분류.",
        "rows": matches})

    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    bias = expected_bias(eng, matches)
    mat = materiality(eng, matches, universe, bias)
    v = verdict(mat, anc, cont, bias)

    ft = json.loads((RD / "r23-final-terms-latest.json")
                    .read_text(encoding="utf-8"))
    al = json.loads((RD / "r23-allocation-tables-latest.json")
                    .read_text(encoding="utf-8"))
    save("wealth-resolution", {
        "task": "R23", "targets": ft["targets"],
        "byWealthStatus": ft["byWealthStatus"],
        "byConfidence": ft["byConfidence"],
        "byAllocationStatus": al["byAllocationStatus"],
        "byRatioBasis": al["byRatioBasis"],
        "byPriceSource": al["byPriceSource"],
        "flags": al["flags"], "fieldRecovery": al["fieldRecovery"],
        "treasuryExcludedCount": al["treasuryExcludedCount"],
        "preferredSeparatedCount": al["preferredSeparatedCount"],
        "allocationHardRule": ft["allocationHardRule"],
        "noResultEngineering": ft["noResultEngineering"]})

    print(json.dumps({"verdict": v["verdict"],
                      "unresolvedTickerPct": mat["unresolvedTickerPct"],
                      "expectedBiasPct": bias["expectedBiasPct"],
                      "biasProgression": bias["progression"],
                      "anchors": len(anc["cases"]), "anchorsPass": anc["allPass"],
                      "noFakeWealth": cont["noFreeWealth"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
