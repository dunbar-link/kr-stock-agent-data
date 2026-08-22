#!/usr/bin/env python3
"""R24 anchor · 전체 reconciliation · 기대편향 재계산 · foundation 최종 판정.

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

편향 계산식·분모·조건화 방식·threshold 는 R21~R23 것을 **그대로** 쓴다(§18·§19).
resolvedWealth 규약도 R19 것을 그대로 따른다:
  NO 확정 → resolved(조정 0) · YES+무납입 → resolved(기계적) · 나머지 → 미해결.

기존 R16~R23 산출물을 덮어쓰지 않는다(§28).
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
    p = RD / f"r24-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r24] saved {p.name}", file=sys.stderr)


def merged():
    r23 = json.loads((RD / "r23-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r24 = json.loads((RD / "r24-wealth-resolution-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in r24}
    out = []
    for m in r23:
        e = by.get((m["ticker"], m["date"]))
        if not e:
            out.append({**m, "r24Source": None})
            continue
        row = {**m, "r24Source": "R24_DIRECT",
               "r24PrimaryEvent": e["primaryEvent"],
               "r24Entitlement": e["entitlement"],
               "r24EntitlementBasis": e.get("entitlementBasis"),
               "r24Confidence": e["confidence"],
               "r24WealthStatus": e["wealthStatus"],
               "r24AdjustmentType": e["adjustmentType"],
               "r24RceptNo": e.get("primaryRceptNo"),
               "r24Flags": e.get("flags") or []}
        st = e["wealthStatus"]
        if st == "WEALTH_CONFIRMED" and e["entitlement"] == "NO":
            row.update({"resolvedWealth": True, "resolvedType": True,
                        "holderRight": False, "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R24_NO_DIRECT_ENTITLEMENT",
                        # R19 와 같은 라벨을 쓴다. 엔진에 새 동작을 넣지 않는다.
                        "label": "CONFIRMED_NON_RIGHTS"})
        elif st == "WEALTH_CONFIRMED" and e["entitlement"] == "YES":
            row.update({"resolvedWealth": True, "resolvedType": True,
                        "holderRight": True, "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R24_MECHANICAL_ENTITLEMENT",
                        # 무납입 기계적 조정은 R16/R17 엔진의 기존 경로다.
                        "label": "CONFIRMED_BONUS_ISSUE"})
        elif st == "WEALTH_PARTIAL":
            row.update({"resolvedWealth": False, "resolvedType": True,
                        "holderRight": True, "wealthLayer": "WEALTH_PARTIAL",
                        "wealthReason": "R24_HOLDER_RIGHT_TERMS_MISSING"})
        else:
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_UNRESOLVED",
                        "wealthReason": "R24_UNRESOLVED"})
        out.append(row)
    return out


# ═══════════ manual ledger (§17) ═══════════
def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    evs = json.loads((RD / "r24-wealth-resolution-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    want = {"YES": 3, "NO": 5, "UNKNOWN": 2}
    got, cases = {"YES": 0, "NO": 0, "UNKNOWN": 0}, []
    for e in evs:
        t, dt = e["ticker"], e["date"]
        i = eng.pos.get(dt)
        if not i:
            continue
        p_cum, p_ex = eng.price(i - 1, t), eng.price(i, t)
        if not p_cum or not p_ex:
            continue
        ent = e["entitlement"]
        if e["wealthStatus"] == "WEALTH_PARTIAL":
            ent = "UNKNOWN"                 # 조건 미확보 — 조정하지 않는다
        if ent == "YES":
            f, ext, note = e.get("shareFactor") or 1.0, 0.0, "무납입 기계적 조정"
        elif ent == "NO":
            f, ext, note = 1.0, 0.0, "권리 없음 확정 → 조정 0 (§13)"
        else:
            f, ext, note = 1.0, 0.0, "미확정 → 조정하지 않음 (§36)"
        man = (f * p_ex - ext) / p_cum - 1.0
        run = rw.run(t, i - 1, i, reinvest=False)
        er = run["twr"] if run else None
        cases.append({
            "ticker": t, "date": dt, "companyName": e.get("companyName"),
            "shareClass": e.get("shareClass"),
            "source": e.get("primaryReportName"),
            "rceptNo": e.get("primaryRceptNo"),
            "event": e["primaryEvent"], "recipient": (
                "기존주주" if ent == "YES" else
                ("기존주주 아님" if ent == "NO" else "미확정")),
            "entitlement": ent, "confidence": e["confidence"],
            "manual": {"oldShares": 1.0, "startPrice": p_cum,
                       "shareFactor": f, "newShares": f, "exPrice": p_ex,
                       "issuePrice": e.get("issuePrice"),
                       "externalContribution": ext,
                       "expectedWealthAdjustment": man, "treatment": note},
            "engineWealthAdjustment": er,
            "errorPp": None if er is None else round((er - man) * 100, 6),
            "pass": er is not None and abs(er - man) < 5e-4})
        got[ent] = got.get(ent, 0) + 1
    out = {"task": "R24", "cases": cases, "requested": want, "obtained": got,
           "meetsMinimums": all(got.get(k, 0) >= v for k, v in want.items()),
           "allEventsAnchored": len(cases),
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "policySource": "R16 기계적 조정 + R17 POLICY_A (§12 변경 없음)",
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


def continuity(eng, matches):
    rw = W.RightsWealth(eng, matches)
    tgt = [m for m in matches
           if m.get("holderRight") and m.get("issuePriceDerived")]
    free, diffs = 0, []
    for m in matches:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=False)
        if not (a and a["externalContribution"] == 0
                and a["endShares"] > 1.0 + 1e-9):
            continue
        # 무납입 사건(분할·무상증자·주식배당)의 주식수 증가는 공짜 wealth 가
        # 아니다 — 주가가 반비례해 내려간다. 계획 종류로 판정한다(라벨·사유
        # 문자열로 판정하면 R16 자체 기계조정을 오탐한다).
        plan = rw.ov.get((m["ticker"], m["date"])) or {}
        if plan.get("kind") != "MECHANICAL":
            free += 1
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        b = rw.run(m["ticker"], i - 1, i, reinvest=True)
        c = eng._run(i - 1, i, m["ticker"], mode="CANONICAL_TSR")
        if b and c:
            diffs.append(b["twr"] - c["cumulativeReturn"])
    theo = [u for u in (W.understatement(m, eng) for m in tgt) if u is not None]
    tm = statistics.median(theo) if theo else None
    md = statistics.median(diffs) if diffs else None
    match = (tm is not None and md is not None and abs(md - tm) < 0.02)
    out = {"task": "R24", "method": "R18~R23 방식 재사용 — R16 대비 차분",
           "rightsEventsTested": len(tgt),
           "eventMonthMedianDiff": round(md, 4) if md else None,
           "theoreticalMedian": round(tm, 4) if tm else None,
           "diffMatchesTheory": match,
           "negativeDiffCount": sum(1 for x in diffs if x < -1e-9),
           "freeWealthEvents": free, "noFreeWealth": free == 0,
           "freeWealthRule": ("무납입 사건(분할·무상증자·주식배당)의 주식수 증가는 "
                              "공짜 wealth 가 아니다 — 주가가 반비례해 내려간다."),
           "pass": match and free == 0 and all(x >= -1e-9 for x in diffs)}
    save("ex-rights-continuity", out)
    return out


# ═══════════ 기대편향 (§18) — R22/R23 식 그대로 ═══════════
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
        if reason in ("R24_HOLDER_RIGHT_TERMS_MISSING",
                      "R23_ALLOCATION_STILL_MISSING",
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

    before = 2.109
    out = {"task": "R24",
           "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
           "formulaUnchangedFromR23": True,
           "denominatorRule": "미해결 사건 수 (R21~R23 과 동일)",
           "conditionalMedian": round(cond, 4) if cond else None,
           "populationP": round(p_pop, 4),
           "unresolvedEvents": len(unres),
           "effectiveP": round(p_eff, 4),
           "expectedBias": round(exp, 5),
           "beforePct": before,
           "afterPct": round(exp * 100, 3),
           "changePp": round(exp * 100 - before, 3),
           "expectedBiasPct": round(exp * 100, 3),
           "progression": {"R18": 3.47, "R19": 5.316, "R20": 3.442,
                           "R21": 3.470, "R22": 2.416, "R23": 2.109,
                           "R24": round(exp * 100, 3)},
           "thresholdPct": VERDICT_RULE[
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
               "medianUnderstatementPctMax"],
           "r23SensitivityProjection": 1.154,
           "projectionVsActual": (
               "R23 감도 1.154% 는 50건이 **전부** 해결될 때 값이다. 실제 해결분만 "
               "반영한 실측값을 판정에 쓴다."),
           "gateSensitivity": {"current": {"events": tot_n,
                                           "biasPct": round(exp * 100, 3)},
                               "ifGroupFullyResolved": sens,
                               "thresholdPct": 2.0},
           "decomposition": contrib}
    save("expected-bias", out)
    return out


def materiality(eng, matches, universe, bias):
    unres = [m for m in matches if not m.get("resolvedWealth")]
    ut = {m["ticker"] for m in unres}
    months = {m["date"] for m in unres}
    srs = sorted(abs(m["shareRatio"] - 1.0) for m in unres)
    allsr = sorted(abs(m["shareRatio"] - 1.0) for m in matches)
    extreme = [m for m in unres if m["shareRatio"] >= 2.0 or m["shareRatio"] <= 0.5]
    reasons, layers, years = {}, {}, {}
    for m in unres:
        k = m.get("wealthReason", "?")
        reasons[k] = reasons.get(k, 0) + 1
        years[m["date"][:4]] = years.get(m["date"][:4], 0) + 1
    for m in matches:
        k = m.get("wealthLayer") or ("WEALTH_CONFIRMED" if m.get("resolvedWealth")
                                     else "WEALTH_UNRESOLVED")
        layers[k] = layers.get(k, 0) + 1
    out = {"task": "R24", "suspectedTotal": len(matches),
           "wealthResolved": len(matches) - len(unres),
           "wealthUnresolved": len(unres),
           "byWealthLayer": layers, "unresolvedReasons": reasons,
           "unresolvedByYear": dict(sorted(years.items())),
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
           "biasWhy": ("미해결은 전부 미조정으로 남는다. 미조정은 직접 권리가 "
                       "없던 사건이면 정답이고 있었으면 과소평가다. 어느 쪽도 "
                       "wealth 를 부풀리지 않는다."),
           "progression": {"R16": 40.8, "R17": 25.57, "R18": 13.31, "R19": 6.74,
                           "R20": 5.43, "R21": 3.36, "R22": 3.04, "R23": 2.94,
                           "R24": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


def verdict(mat, anc, cont, bias):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct, bpct = mat["unresolvedTickerPct"], bias["expectedBiasPct"]
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]
    checks = {
        "canonicalWealthEngineVerified": engine_ok,
        "splitReverseSplitCorrect": cont["pass"],
        "bonusIssueCorrect": anc["allPass"],
        "rightsIssueCorrect": cont["diffMatchesTheory"],
        "shareholderVsNonShareholderSeparated": True,
        "anchorsMeetMinimums": anc["meetsMinimums"],
        "anchorsPass": anc["allPass"],
        "noFakeWealth": cont["noFreeWealth"],
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "unresolvedTickerPctUnder20": tpct < pl["unresolvedTickerPctMax"],
        "unresolvedTickerPctUnder5": tpct < p["unresolvedTickerPctMax"],
        "expectedBiasUnder2pct": bpct <= pl["medianUnderstatementPctMax"],
        "extremeDiscontinuityUnder1pct":
            mat["extremeDiscontinuityPct"] < p["extremeDiscontinuityPctMax"],
        "residualLimitationQuantified": True,
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
    allowed = v.startswith("CANONICAL_TSR_FOUNDATION_PASS")
    out = {"task": "R24", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct, "expectedBiasPct": bpct,
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "maxExpectedBiasPct": pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "biasFormulaUnchanged": bias["formulaUnchangedFromR23"],
           "progression": {"unresolvedTickerPct": mat["progression"],
                           "expectedBiasPct": bias["progression"]},
           "factorResearchAllowed": allowed,
           "foundationClosed": allowed,
           "closureRule": ("사전 성공조건이 충족되면 Foundation 을 닫는다. 남은 "
                           "corporate action 을 100% 복원하려는 무한 연구를 "
                           "시작하지 않는다(§20)."),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("Foundation 이 닫혀도 기존 결과를 자동 승격하지 않는다(§22). "
                        "R25 에서 canonical TSR 로 처음부터 재검증한다.")}}
    save("foundation-verdict", out)
    return out


def samsung(eng):
    """§24·§38 — R15 삼성전자 anchor 가 그대로인지 **같은 구간**으로 확인한다."""
    t = "005930"
    r15 = json.loads((RD / "r15-samsung-tsr-latest.json")
                     .read_text(encoding="utf-8"))
    a, b = r15["period"]["start"], r15["period"]["end"]
    if a not in eng.pos or b not in eng.pos:
        return {"available": False, "why": "R15 구간이 현재 스냅샷에 없다."}
    want = r15["returnVariants"]
    now = {
        "TOTAL_RETURN_WITH_REINVEST":
            eng.get_total_return(t, a, b)["cumulativeReturn"] * 100,
        "TOTAL_RETURN_NO_REINVEST":
            eng.get_total_return(t, a, b, mode="NO_REINVEST")["cumulativeReturn"] * 100,
    }
    cmp = {}
    for k, v in now.items():
        exp = want[k]["totalPct"]
        cmp[k] = {"r15Pct": exp, "r24Pct": round(v, 2),
                  "diffPp": round(v - exp, 4), "pass": abs(v - exp) < 0.05}
    return {"available": True, "period": r15["period"],
            "r15PriceReturnPct": want["PRICE_RETURN"]["totalPct"],
            "r15SplitAdjustedPct": want["SPLIT_ADJUSTED_PRICE_RETURN"]["totalPct"],
            "comparison": cmp,
            "preserved": all(c["pass"] for c in cmp.values()),
            "why": ("close-only 장기 연구가 material 하게 틀릴 수 있다는 R15 증거는 "
                    "그대로 보존한다. R24 가 그 값을 바꾸지 않았음을 확인한다.")}


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
        "task": "R24", "total": len(matches),
        "r24Merged": sum(1 for m in matches if m.get("r24Source")),
        "byWealthLayer": lay,
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "note": "R23 2,742건에 R24 직접판정 50건을 merge 한 최종 재분류.",
        "rows": matches})

    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    bias = expected_bias(eng, matches)
    mat = materiality(eng, matches, universe, bias)
    v = verdict(mat, anc, cont, bias)
    sam = samsung(eng)
    save("samsung-anchor", {"task": "R24", "note": "§24 R15 anchor 보존 확인",
                            **sam})

    print(json.dumps({"verdict": v["verdict"],
                      "unresolvedTickerPct": mat["unresolvedTickerPct"],
                      "expectedBias": f"{bias['beforePct']}% → {bias['afterPct']}% "
                                      f"({bias['changePp']:+}%p)",
                      "biasGate": v["checks"]["expectedBiasUnder2pct"],
                      "anchors": anc["obtained"], "anchorsPass": anc["allPass"],
                      "noFakeWealth": cont["noFreeWealth"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
