#!/usr/bin/env python3
"""R21 anchor · 오분류 감사 · 전체 reconciliation · 기대편향 실측 재계산 · 판정.

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21

기존 R16~R20 산출물을 덮어쓰지 않는다(§27).
threshold 와 bias 계산식은 R20 것을 그대로 쓴다(§17·§19 변경 금지).

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
    p = RD / f"r21-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r21] saved {p.name}", file=sys.stderr)


# ═══════════ R21 결과를 R20 전체 2,742건에 merge (§23) ═══════════
def merged():
    r20 = json.loads((RD / "r20-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r21 = json.loads((RD / "r21-event-classification-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in r21}
    out = []
    for m in r20:
        e = by.get((m["ticker"], m["date"]))
        if not e:
            out.append({**m, "r21Source": None})
            continue
        row = {**m, "r21Source": "R21_DIRECT",
               "r21PrimaryEvent": e["primaryEvent"],
               "r21SecondaryEvents": e["secondaryEvents"],
               "r21DirectEntitlement": e["directEntitlement"],
               "r21Entitlement": e["entitlement"],
               "r21EntitlementWhy": e["entitlementWhy"],
               "r21WealthAdjustment": e["wealthAdjustment"],
               "r21Confidence": e["confidence"],
               "r21Provenance": e["provenance"],
               "r21Receipt": e.get("directReceipt"),
               "r21EventIdentity": e["eventIdentity"]}
        raw = e["wealthAdjustmentRaw"]
        if raw == "NOT_REQUIRED":
            # 직접증거로 기존 주주 권리 없음이 확정됐다 → 미조정이 정답.
            row.update({"resolvedWealth": True, "holderRight": False,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R21_NO_DIRECT_ENTITLEMENT",
                        "label": "CONFIRMED_NON_RIGHTS"})
        elif raw == "REQUIRED_MECHANICAL":
            # 주식분할·무상증자 — 관측 shareRatio 만으로 조정이 확정된다.
            row.update({"resolvedWealth": True, "holderRight": False,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R21_MECHANICAL_ENTITLEMENT",
                        "label": "CONFIRMED_BONUS_ISSUE"})
        elif raw == "REQUIRED":
            # 권리는 확정, 조건(비율·발행가) 미확보 → 보수적 미조정.
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_PARTIAL",
                        "wealthReason": "R21_ENTITLEMENT_TERMS_MISSING"})
        else:
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_UNRESOLVED",
                        "wealthReason": "R21_UNRESOLVED"})
        out.append(row)
    return out


# ═══════════ 오분류 감사 (§22) ═══════════
def fp_audit():
    ev = json.loads((RD / "r21-event-classification-latest.json")
                    .read_text(encoding="utf-8"))["events"]
    EXPECT = {
        "CONVERTIBLE_BOND_CONVERSION": ("전환",),
        "BW_WARRANT_EXERCISE": ("신주인수권",),
        "STOCK_OPTION_EXERCISE": ("주식매수선택권",),
        "EXCHANGEABLE_BOND_EXCHANGE": ("교환",),
        "MERGER_NEW_SHARES": ("합병",),
        "SHARE_SWAP": ("주식교환", "주식이전"),
        "SPINOFF_RELATED_SHARES": ("분할",),
        "STOCK_SPLIT": ("주식분할", "액면분할"),
        "BONUS_ISSUE": ("무상증자",),
        "TRUE_RIGHTS_ISSUE": ("유상증자",),
    }
    by = {}
    for e in ev:
        if e["confidence"] in ("HIGH", "MEDIUM") and e["primaryEvent"] != "UNRESOLVED":
            by.setdefault(e["primaryEvent"], []).append(e)
    checks, rows = [], []
    for kind, es in sorted(by.items()):
        es.sort(key=lambda x: (x["ticker"], x["date"]))
        n = min(10, len(es))
        step = max(1, len(es) // n)
        sample = [es[i * step] for i in range(n)]
        ok = 0
        for e in sample:
            nm = e.get("matchedReportName") or ""
            kws = EXPECT.get(kind) or ()
            kw_ok = any(k in nm for k in kws)
            sub_ok = not any(s in nm for s in ("종속회사", "자회사의"))
            good = kw_ok and sub_ok
            ok += good
            rows.append({"primaryEvent": kind, "ticker": e["ticker"],
                         "date": e["date"], "receipt": e.get("directReceipt"),
                         "reportName": nm, "expectedKeywords": list(kws),
                         "keywordPresent": kw_ok, "notSubsidiary": sub_ok,
                         "crossCheckPass": good})
        checks.append({"primaryEvent": kind, "population": len(es), "sampled": n,
                       "crossCheckPass": ok,
                       "falsePositiveRate": round((n - ok) / n, 4) if n else None})
    ts = sum(c["sampled"] for c in checks)
    to = sum(c["crossCheckPass"] for c in checks)
    out = {"task": "R21",
           "method": ("HIGH/MEDIUM 분류를 유형별로 결정론적 표본추출(각 10건 또는 "
                      "전수)해 **원문 공시명**과 대조한다. 분류 라벨이 아니라 실제 "
                      "공시 제목에 해당 키워드가 있는지, 종속회사 공시가 섞이지 "
                      "않았는지를 본다(R19 방식 재사용)."),
           "byPrimaryEvent": checks, "sampled": ts, "passed": to,
           "falsePositiveRate": round((ts - to) / ts, 4) if ts else None,
           "falseNegativeClue": ("BODY_KEYWORD 경로는 LOW 로 고정돼 resolved 에서 "
                                 "빠진다 = false negative 쪽으로 치우친 설계다."),
           "samples": rows}
    save("false-classification-audit", out)
    return out


# ═══════════ anchor (§21) ═══════════
ANCHOR_PLAN = [("CB 전환", "CONVERTIBLE_BOND_CONVERSION", 2),
               ("BW 행사", "BW_WARRANT_EXERCISE", 2),
               ("합병 신주", "MERGER_NEW_SHARES", 2),
               ("회사분할·주식교환", "SPINOFF_RELATED_SHARES", 1),
               ("주식분할(권리 YES)", "STOCK_SPLIT", 2),
               ("주주배정 유상증자(권리 YES)", "TRUE_RIGHTS_ISSUE", 2)]


def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    by = {}
    for m in matches:
        if m.get("r21Source") != "R21_DIRECT":
            continue
        by.setdefault(m["r21PrimaryEvent"], []).append(m)

    cases, missing = [], []
    for label, kind, want in ANCHOR_PLAN:
        cand = [c for c in by.get(kind, [])
                if c["r21Confidence"] in ("HIGH", "MEDIUM")
                and eng.pos.get(c["date"], 0) > 0]
        cand.sort(key=lambda c: (c["date"], c["ticker"]))
        if len(cand) < want:
            missing.append(f"{label} (확보 {len(cand)}/{want})")
        for m in cand[:want]:
            i = eng.pos[m["date"]]
            p_cum, p_ex = eng.price(i - 1, m["ticker"]), eng.price(i, m["ticker"])
            if not p_cum or not p_ex:
                continue
            ent = m["r21Entitlement"]
            raw = m["r21WealthAdjustment"]
            if ent == "NO":
                s1, ext = 1.0, 0.0
                treat = "기존 주주 직접 권리 없음 → 보유주식 불변 · 조정 정확히 0"
            elif m.get("wealthReason") == "R21_MECHANICAL_ENTITLEMENT":
                s1, ext = m["shareRatio"], 0.0
                treat = f"권리 YES · 기계적 조정 보유주식 ×{m['shareRatio']:.4f} · 납입 0"
            else:
                s1, ext = 1.0, 0.0
                treat = "권리 YES 이나 조건 미확보 → 보수적 미조정"
            man = (s1 * p_ex - ext) / p_cum - 1.0
            run = rw.run(m["ticker"], i - 1, i, reinvest=False)
            er = run["twr"] if run else None
            cases.append({
                "case": label, "primaryEvent": kind, "ticker": m["ticker"],
                "date": m["date"], "receipt": m.get("r21Receipt"),
                "confidence": m["r21Confidence"],
                "directEntitlement": m["r21DirectEntitlement"],
                "wealthAdjustment": raw, "shareRatio": m["shareRatio"],
                "manual": {"startShares": 1.0, "startPrice": p_cum,
                           "exPrice": p_ex, "endingShares": s1,
                           "externalContribution": ext,
                           "manualTsrEffect": man, "treatment": treat},
                "engineTsrEffect": er,
                "diffPp": None if er is None else round((er - man) * 100, 6),
                "adjustmentIsZero": (er is not None and abs(er - man) < 5e-4
                                     and abs(s1 - 1.0) < 1e-9),
                "pass": er is not None and abs(er - man) < 5e-4})

    no_ent = [c for c in cases if c["directEntitlement"] == "DIRECT_ENTITLEMENT_NO"]
    yes_ent = [c for c in cases if c["directEntitlement"] == "DIRECT_ENTITLEMENT_YES"]
    out = {"task": "R21", "cases": cases, "missingCaseTypes": missing,
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "noEntitlementAdjustmentExactlyZero":
               all(c["adjustmentIsZero"] for c in no_ent) if no_ent else None,
           "entitlementCaseAdjustsNonZero":
               any(abs(c["manual"]["endingShares"] - 1.0) > 1e-9 for c in yes_ent)
               if yes_ent else None,
           "whyBothSidesNeeded": (
               "권리 없는 사건에서 조정이 0 인 것만 보이면 '엔진이 아무것도 안 한다' "
               "와 구분되지 않는다. 권리 있는 사건에서 정확히 배율만큼 조정되는 "
               "것을 함께 보여야 판정이 의미를 갖는다."),
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


def _gate_sensitivity(contrib, cond, pl_max=2.0):
    """어떤 군을 해결해야 게이트를 통과하는가 — 지표 구조를 정량화한다.

    지표가 '미해결 1건당 기대값' 이므로, 어떤 군을 해결하면 그 군의 sumP 와
    events 가 **둘 다** 빠진다. p=0 인 군을 빼면 평균이 오히려 오른다.
    계산식은 바꾸지 않는다(§17). 이건 해석이지 새 지표가 아니다.
    """
    if not cond:
        return None
    tot_n = sum(c["events"] for c in contrib.values())
    tot_p = sum(c["sumP"] for c in contrib.values())
    rows = []
    for g, c in sorted(contrib.items()):
        n2 = tot_n - c["events"]
        p2 = tot_p - c["sumP"]
        b2v = (p2 / n2 * cond * 100) if n2 else 0.0
        rows.append({"resolveGroup": g, "events": c["events"],
                     "avgP": c["avgP"],
                     "biasIfFullyResolvedPct": round(b2v, 3),
                     "passesGate": b2v <= pl_max,
                     "direction": ("낮아짐" if b2v < tot_p / tot_n * cond * 100
                                   else "높아지거나 같음")})
    return {
        "current": {"events": tot_n,
                    "biasPct": round(tot_p / tot_n * cond * 100, 3)},
        "ifGroupFullyResolved": rows,
        "interpretation": (
            "p=0 으로 확정된 군(권리 없음·감자)을 더 해결해도 지표는 **내려가지 "
            "않는다** — 분모만 줄어 남은 pool 이 권리 있는 사건 쪽으로 농축되기 "
            "때문이다. 게이트를 통과시키는 유일한 경로는 **권리 있음이 확정됐지만 "
            "조건이 없는 군**의 최종조건을 확보해 그 군을 pool 에서 빼는 것이다."),
        "thresholdPct": pl_max,
    }


# ═══════════ 기대편향 실측 재계산 (§16·§17) ═══════════
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
        """R20 규칙 그대로. 직접증거 우선, 없으면 모집단값(§16 새 확률 신설 금지)."""
        reason = m.get("wealthReason")
        if m.get("r21Entitlement") == "NO":
            return 0.0, "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R21"
        if reason == "R21_ENTITLEMENT_TERMS_MISSING":
            return 1.0, "HOLDER_RIGHT_TERMS_MISSING", "DIRECT_R21"
        if m.get("r19Entitlement") == "NO":
            return 0.0, "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R19"
        if m.get("r19Entitlement") == "YES":
            return 1.0, "TRUE_RIGHTS", "DIRECT_R19"
        if reason == "R20_FINAL_TERMS_INCOMPLETE":
            return 1.0, "HOLDER_RIGHT_TERMS_MISSING", "DIRECT_R18"
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

    ent = json.loads((RD / "r21-entitlement-latest.json")
                     .read_text(encoding="utf-8"))
    pop_events = sum(c["events"] for c in contrib.values()
                     if c["pSource"] == "POPULATION_ESTIMATE")
    pop_pct = sum(c["biasContributionPct"] for c in contrib.values()
                  if c["pSource"] == "POPULATION_ESTIMATE")

    out = {"task": "R21",
           "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
           "formulaUnchangedFromR20": True,
           "conditionalMedian": round(cond, 4) if cond else None,
           "populationP": round(p_pop, 4),
           "unresolvedEvents": len(unres),
           "effectiveP": round(p_eff, 4),
           "expectedBias": round(exp, 5),
           "expectedBiasPct": round(exp * 100, 3),
           "progression": {"R18": 3.47, "R19": 5.316, "R20": 3.442,
                           "R21": round(exp * 100, 3)},
           "thresholdPct": VERDICT_RULE[
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
               "medianUnderstatementPctMax"],
           "estimateReplacement": {
               "group": "LOW_CONFIDENCE_NOT_COUNTED",
               "events": 96,
               "priorP": 0.296, "priorSource": "POPULATION_ESTIMATE",
               "priorContributionPp": 1.453,
               "measuredYesRateAmongKnown": ent["measuredYesRateAmongKnown"],
               "measuredYesRateAmongAll": ent["measuredYesRateAmongAll"],
               "residualContributionPp": round(
                   sum(c["biasContributionPct"] for g, c in contrib.items()
                       if c["pSource"] == "DIRECT_R21"), 3),
               "note": ("추정 0.296 을 실측으로 대체했다. 측정된 YES 비율이 "
                        "추정보다 낮으면 이 군의 기여는 줄지만, 남은 pool 의 "
                        "평균 P 는 오히려 올라갈 수 있다(아래 참조)."),
           },
           "metricStructureNote": (
               "★ 이 지표는 미해결 **1건당** 기대값이다(sumP / N × cond). 권리 없음이 "
               "확정된 사건은 분자에서 0 이 되면서 **분모에서도 빠진다**. 그래서 "
               "'권리 없음' 을 많이 확정할수록 남은 pool 은 권리 있는 사건 쪽으로 "
               "농축되고 평균 P 가 올라간다. 해결을 많이 할수록 이 지표가 나빠질 수 "
               "있다는 뜻이며, R19 에서 이미 관측된 구조적 성질이다. 계산식은 "
               "R20 것을 그대로 쓰되(§17) 이 성질을 보고서에 명시한다."),
           "gateSensitivity": _gate_sensitivity(contrib, cond, pl_max=2.0),
           "remainingEstimateDependency": {
               "events": pop_events,
               "contributionPct": round(pop_pct, 3),
               "note": "아직 실측이 아니라 모집단 추정치에 의존하는 잔여분.",
           },
           "decomposition": contrib}
    save("expected-bias", out)
    return out


# ═══════════ materiality (§23) ═══════════
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
    out = {"task": "R21", "suspectedTotal": len(matches),
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
                           "R19": 6.74, "R20": 5.43,
                           "R21": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


# ═══════════ 연속성 (§21 공짜 wealth 0) ═══════════
def continuity(eng, matches):
    rw = W.RightsWealth(eng, matches)
    tgt = [m for m in matches
           if m.get("holderRight") and m.get("issuePriceDerived")]
    free = 0
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=False)
        if a and a["externalContribution"] == 0 and a["endShares"] > 1.0 + 1e-9:
            free += 1
    diffs = []
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=True)
        b = eng._run(i - 1, i, m["ticker"], mode="CANONICAL_TSR")
        if a and b:
            diffs.append(a["twr"] - b["cumulativeReturn"])
    theo = [u for u in (W.understatement(m, eng) for m in tgt) if u is not None]
    tm = statistics.median(theo) if theo else None
    md = statistics.median(diffs) if diffs else None
    match = (tm is not None and md is not None and abs(md - tm) < 0.02)
    out = {"task": "R21", "method": "R18~R20 방식 재사용 — R16 대비 차분",
           "rightsEventsTested": len(tgt),
           "eventMonthMedianDiff": round(md, 4) if md else None,
           "theoreticalMedian": round(tm, 4) if tm else None,
           "diffMatchesTheory": match,
           "negativeDiffCount": sum(1 for x in diffs if x < -1e-9),
           "freeWealthEvents": free, "noFreeWealth": free == 0,
           "pass": match and free == 0 and all(x >= -1e-9 for x in diffs)}
    save("ex-rights-continuity", out)
    return out


# ═══════════ 판정 (§19·§20) ═══════════
def verdict(mat, anc, cont, bias, fp):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct, bpct = mat["unresolvedTickerPct"], bias["expectedBiasPct"]
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]
    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anc["allPass"],
        "noEntitlementAdjustmentZero":
            anc["noEntitlementAdjustmentExactlyZero"] is not False,
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
    out = {"task": "R21", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct, "expectedBiasPct": bpct,
           "falseClassificationRate": fp["falsePositiveRate"],
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "maxExpectedBiasPct": pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "biasFormulaUnchanged": bias["formulaUnchangedFromR20"],
           "progression": {"unresolvedTickerPct": mat["progression"],
                           "expectedBiasPct": bias["progression"]},
           "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                          "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("foundation 이 PASS 여도 기존 숫자를 자동 canonical 로 "
                        "승격하지 않는다(§25). 새 precommit 으로 원점 재계산해야 "
                        "한다.")}}
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
        "task": "R21", "total": len(matches),
        "r21Merged": sum(1 for m in matches if m.get("r21Source")),
        "byWealthLayer": lay,
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "note": "R20 2,742건에 R21 직접증거를 merge 한 최종 재분류(§23).",
        "rows": matches})

    fp = fp_audit()
    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    bias = expected_bias(eng, matches)
    mat = materiality(eng, matches, universe, bias)
    v = verdict(mat, anc, cont, bias, fp)
    print(json.dumps({"verdict": v["verdict"],
                      "unresolvedTickerPct": mat["unresolvedTickerPct"],
                      "expectedBiasPct": bias["expectedBiasPct"],
                      "biasProgression": bias["progression"],
                      "measuredYesRate":
                      bias["estimateReplacement"]["measuredYesRateAmongKnown"],
                      "anchors": len(anc["cases"]), "anchorsPass": anc["allPass"],
                      "fpRate": fp["falsePositiveRate"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
