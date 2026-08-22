#!/usr/bin/env python3
"""R20 wealth 반영 · anchor · 전체 reconciliation · 기대편향 재계산 · 판정.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

기존 R16~R19 산출물을 덮어쓰지 않는다(§26).
threshold 와 bias 계산법은 R19 것을 그대로 쓴다(§18·§20 변경 금지).

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
    p = RD / f"r20-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r20] saved {p.name}", file=sys.stderr)


# ═══════════ R20 결과를 R19 전체 2,742건에 merge (§27) ═══════════
def merged():
    r19 = json.loads((RD / "r19-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r20 = json.loads((RD / "r20-final-terms-normalized-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in r20}
    out = []
    for m in r19:
        e = by.get((m["ticker"], m["date"]))
        if not e:
            out.append({**m, "r20Source": None})
            continue
        row = {**m, "r20Source": "R20_POST_ISSUANCE",
               "r20EventStatus": e["eventStatus"],
               "r20WealthStatus": e["wealthStatus"],
               "r20Confidence": e["confidence"],
               "r20Flags": e.get("flags", []),
               "r20RceptNo": (e.get("finalSource") or {}).get("rcept_no"),
               "r20FinalNewShares": e.get("finalNewSharesIssued"),
               "r20FinalIssuePrice": e.get("finalIssuePrice"),
               "r20EntitlementRatio": e.get("entitlementRightsRatio"),
               "r20TakeUpRatio": e.get("takeUpRightsRatio"),
               "r20ShareholderEntitledShares":
                   e.get("finalShareholderEntitledShares")}
        if e["wealthStatus"] != "WEALTH_CONFIRMED":
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_PARTIAL",
                        "wealthReason": "R20_FINAL_TERMS_INCOMPLETE"})
        elif e.get("holderRightFinal"):
            # 확정된 주주배정 — 엔진이 실제 조건으로 조정한다.
            row.update({"resolvedWealth": True, "holderRight": True,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R20_FINAL_TERMS_RECOVERED",
                        "label": "CONFIRMED_RIGHTS",
                        "dartRatio": e.get("entitlementRightsRatio"),
                        "issuePriceDerived": e.get("finalIssuePrice"),
                        "recordDate": e.get("actualSubscriptionStart"),
                        "paymentDate": e.get("actualPaymentDate")})
        else:
            # 실제로는 제3자·공모였거나 철회 — 미조정이 정답.
            row.update({"resolvedWealth": True, "holderRight": False,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": e.get("wealthReasonR20",
                                              "R20_NO_ADJUSTMENT"),
                        "label": "CONFIRMED_NON_RIGHTS"})
        out.append(row)
    return out


# ═══════════ manual anchor (§15) ═══════════
def anchors(eng, matches):
    """대형·중형·소형 + 정정 다수 + 실권 다수 — 최소 10건."""
    rw = W.RightsWealth(eng, matches)
    res = json.loads((RD / "r20-final-terms-normalized-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    ch = {(c["ticker"], c["date"]): c for c in
          json.loads((RD / "r20-correction-chains-latest.json")
                     .read_text(encoding="utf-8"))["list"]}
    by = {(m["ticker"], m["date"]): m for m in matches}

    ok = [e for e in res
          if e["wealthStatus"] == "WEALTH_CONFIRMED"
          and e.get("holderRightFinal")
          and e.get("entitlementRightsRatio") and e.get("finalIssuePrice")
          and eng.pos.get(e["date"], 0) > 0]
    ok.sort(key=lambda e: -(e.get("finalNewSharesIssued") or 0))

    picks, seen = [], set()

    def take(label, cand, n):
        for e in cand:
            k = (e["ticker"], e["date"])
            if k in seen or len(picks) >= 40:
                continue
            seen.add(k)
            picks.append((label, e))
            n -= 1
            if n <= 0:
                return

    third = max(1, len(ok) // 3)
    take("대형", ok[:third], 3)
    take("중형", ok[third:2 * third], 3)
    take("소형", ok[2 * third:], 3)
    take("정정 다수", sorted(ok, key=lambda e: -(ch.get((e["ticker"], e["date"]))
                                              or {}).get("filings", 0)), 2)
    take("실권 다수", sorted(ok, key=lambda e: (e.get("shareholderTakeUpRate")
                                             if e.get("shareholderTakeUpRate")
                                             is not None else 9)), 2)

    cases = []
    for label, e in picks:
        t, dt = e["ticker"], e["date"]
        i = eng.pos[dt]
        p_cum, p_ex = eng.price(i - 1, t), eng.price(i, t)
        if not p_cum or not p_ex:
            continue
        r = e["entitlementRightsRatio"]
        k = e["finalIssuePrice"]
        # manual — POLICY A (R17 정본): K < P_ex 이면 청약, 아니면 실권
        if k < p_ex:
            s1, ext = 1.0 + r, r * k
            note = "청약 (K < 권리락가)"
        else:
            s1, ext = 1.0, 0.0
            note = "합리적 실권 (K >= 권리락가)"
        w1 = s1 * p_ex
        man = (w1 - ext) / p_cum - 1.0
        run = rw.run(t, i - 1, i, reinvest=False)
        er = run["twr"] if run else None
        cases.append({
            "case": label, "ticker": t, "date": dt,
            "rcept_no": (e.get("finalSource") or {}).get("rcept_no"),
            "confidence": e["confidence"], "eventStatus": e["eventStatus"],
            "manual": {
                "oldShares": 1.0, "startPrice": p_cum,
                "rightsRatio": r, "issuePrice": k,
                "plannedRightsRatio": e.get("plannedRightsRatio"),
                "actualIssuedShares": e.get("finalNewSharesIssued"),
                "shareholderEntitledShares":
                    e.get("finalShareholderEntitledShares"),
                "takeUpRate": e.get("shareholderTakeUpRate"),
                "externalContribution": ext, "newShares": s1 - 1.0,
                "postEventShares": s1, "exPrice": p_ex,
                "rightsValue": r * max(0.0, p_ex - k),
                "manualTsrEffect": man, "treatment": note},
            "engineTsrEffect": er,
            "diffPp": None if er is None else round((er - man) * 100, 6),
            "pass": er is not None and abs(er - man) < 5e-4,
            "correctionFilings": (ch.get((t, dt)) or {}).get("filings"),
        })

    out = {"task": "R20", "cases": cases,
           "requestedMin": 10, "obtained": len(cases),
           "atLeastTen": len(cases) >= 10,
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "policySource": "R17 POLICY_A + 실권 규칙 (§13 변경 금지)",
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


# ═══════════ ex-rights 연속성 (§14) — R18/R19 방식 재사용 ═══════════
def continuity(eng, matches):
    rw = W.RightsWealth(eng, matches)
    tgt = [m for m in matches
           if m.get("holderRight") and m.get("issuePriceDerived")]
    windows, free = [], 0
    for back, fwd in ((0, 0), (1, 1), (3, 1), (3, 3), (6, 3)):
        diffs = []
        for m in tgt:
            i = eng.pos.get(m["date"])
            if i is None:
                continue
            lo, hi = i - 1 - back, i + fwd
            if lo < 0 or hi >= len(eng.dates):
                continue
            a = rw.run(m["ticker"], lo, hi, reinvest=True)
            b = eng._run(lo, hi, m["ticker"], mode="CANONICAL_TSR")
            if a and b:
                diffs.append(a["twr"] - b["cumulativeReturn"])
        windows.append({"window": f"-{back}~+{fwd}", "lookback": back,
                        "n": len(diffs),
                        "medianDiff": round(statistics.median(diffs), 4)
                        if diffs else None,
                        "negativeDiffCount": sum(1 for x in diffs if x < -1e-9)})
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=False)
        if a and a["externalContribution"] == 0 and a["endShares"] > 1.0 + 1e-9:
            free += 1
    theo = [u for u in (W.understatement(m, eng) for m in tgt) if u is not None]
    tm = statistics.median(theo) if theo else None
    ev = windows[0]
    match = (tm is not None and ev["medianDiff"] is not None
             and abs(ev["medianDiff"] - tm) < 0.02)
    out = {"task": "R20", "method": "R18/R19 방식 재사용 — R16 대비 차분",
           "rightsEventsTested": len(tgt), "windows": windows,
           "theoreticalMedian": round(tm, 4) if tm else None,
           "theoreticalDefinition": "r · max(0, P_ex - K) / P_cum",
           "eventMonthMedianDiff": ev["medianDiff"],
           "diffMatchesTheory": match,
           "eventMonthDiffNeverNegative": ev["negativeDiffCount"] == 0,
           "freeWealthEvents": free, "noFreeWealth": free == 0,
           "pass": match and ev["negativeDiffCount"] == 0 and free == 0}
    save("ex-rights-continuity", out)
    return out


# ═══════════ 기대편향 재계산 (§18·§19) — R19 방식 그대로 ═══════════
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
        """R19 규칙 그대로. 직접증거가 있으면 그것을, 없으면 모집단값을 쓴다."""
        ev = m.get("r19PrimaryEvent")
        reason = m.get("wealthReason")
        if m.get("r19Entitlement") == "NO":
            return 0.0, ev or "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R19"
        if m.get("r19Entitlement") == "YES":
            return 1.0, "TRUE_RIGHTS", "DIRECT_R19"
        if reason == "R20_FINAL_TERMS_INCOMPLETE":
            # R18 이 주주배정으로 확정했고 R20 도 최종조건을 못 얻었다.
            return 1.0, "HOLDER_RIGHT_TERMS_MISSING", "DIRECT_R18"
        if reason == "REDUCTION_PAID_OR_FREE_UNKNOWN":
            return 0.0, "OTHER_CAPITAL_ACTION", "DIRECT_R17"
        return p_pop, (ev or reason or "TRUE_UNRESOLVED"), "POPULATION_ESTIMATE"

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

    out = {"task": "R20",
           "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
           "formulaUnchangedFromR19": True,
           "conditionalMedian": round(cond, 4) if cond else None,
           "populationP": round(p_pop, 4),
           "unresolvedEvents": len(unres),
           "effectiveP": round(p_eff, 4),
           "expectedBias": round(exp, 5),
           "expectedBiasPct": round(exp * 100, 3),
           "progression": {"R18": 3.47, "R19": 5.316,
                           "R20": round(exp * 100, 3)},
           "thresholdPct": VERDICT_RULE[
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
               "medianUnderstatementPctMax"],
           "r19GroupContributionPp": 3.641,
           "arithmeticNote": (
               "지표는 미해결 1건당 기대값(sumP / N_unresolved × cond)이다. "
               "확정된 사건은 분자와 **분모에서 모두** 빠지므로 단순 뺄셈으로 "
               "예측되지 않는다. R19 보고서의 '5.316 - 3.641 = 1.675%' 투영은 "
               "분모 고정을 가정한 것이라 실제와 다르다 — 실측값을 쓴다."),
           "decomposition": contrib}
    save("expected-bias", out)
    return out


# ═══════════ materiality ═══════════
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
    out = {"task": "R20", "suspectedTotal": len(matches),
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
           "progression": {"R16": 40.8, "R17": 25.57, "R18": 13.31,
                           "R19": 6.74,
                           "R20": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


# ═══════════ 판정 (§20·§21) ═══════════
def verdict(mat, anc, cont, bias):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct, bpct = mat["unresolvedTickerPct"], bias["expectedBiasPct"]
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]
    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anc["allPass"],
        "anchorsAtLeastTen": anc["atLeastTen"],
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
    out = {"task": "R20", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct, "expectedBiasPct": bpct,
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "maxExpectedBiasPct": pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "biasFormulaUnchanged": bias["formulaUnchangedFromR19"],
           "progression": {"unresolvedTickerPct": mat["progression"],
                           "expectedBiasPct": bias["progression"]},
           "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                          "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("foundation 이 PASS 여도 기존 숫자를 canonical 로 올리지 "
                        "않는다(§23). 다음 작업에서 새 precommit 으로 원점 "
                        "재계산해야 한다.")}}
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
        "task": "R20", "total": len(matches),
        "r20Merged": sum(1 for m in matches if m.get("r20Source")),
        "byWealthLayer": lay,
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "note": "R19 2,742건에 R20 사후 확정조건을 merge 한 최종 재분류(§27).",
        "rows": matches})

    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    bias = expected_bias(eng, matches)
    mat = materiality(eng, matches, universe, bias)
    v = verdict(mat, anc, cont, bias)

    res = json.loads((RD / "r20-final-terms-normalized-latest.json")
                     .read_text(encoding="utf-8"))
    save("wealth-resolution", {
        "task": "R20", "targets": res["targets"],
        "byWealthStatus": res["byWealthStatus"],
        "byEventStatus": res["byEventStatus"],
        "byConfidence": res["byConfidence"],
        "flags": res["flags"], "fieldRecovery": res["fieldRecovery"],
        "ratioBasis": res["ratioBasis"],
        "note": "유형 확정과 wealth 확정을 분리한다(R18~R19 정본 승계)."})

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
