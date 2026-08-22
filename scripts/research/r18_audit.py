#!/usr/bin/env python3
"""R18 anchor · false-positive audit · 전체 reconciliation · materiality · 판정.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

R16/R17 산출물을 덮어쓰지 않는다(§27). 전부 r18-* 로 저장한다.
threshold 는 R16/R17 사전 기준 그대로 쓴다(§20 변경 금지).

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
    p = RD / f"r18-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r18] saved {p.name}", file=sys.stderr)


HOLDER_LABELS = {"CONFIRMED_RIGHTS"}


# ═══════════ R18 결과를 R17 매칭행 형식으로 승격 ═══════════
def merged_matches():
    """R17 전체 2,742건 + R18 원문 결과 = 최종 재분류 (§18)."""
    r17 = json.loads((RD / "r17-rights-matching-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r18 = json.loads((RD / "r18-legacy-rights-normalized-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by_key = {(e["ticker"], e["date"]): e for e in r18}

    out = []
    for m in r17:
        k = (m["ticker"], m["date"])
        e = by_key.get(k)
        if not e:
            out.append({**m, "source": "R17"})
            continue
        ev = e["evidence"]
        lbl, layer = e["r18Label"], e["wealthLayer"]
        row = {**m, "source": "R18_DOCUMENT",
               "label": lbl if lbl != "UNRESOLVED" else m["label"],
               "r18Label": lbl, "wealthLayer": layer,
               "confidence": ev.get("confidence", m.get("confidence")),
               "provenance": ("DIRECT_DART_DOCUMENT" if lbl != "UNRESOLVED"
                              else m.get("provenance")),
               "evidencePath": ev.get("evidencePath"),
               "engineAction": ev.get("engineAction"),
               "r18Reason": ev.get("reason"),
               "documentsInWindow": ev.get("candidates", 0)}
        if layer == "WEALTH_CONFIRMED" and lbl in HOLDER_LABELS:
            row.update({"holderRight": True, "resolvedWealth": True,
                        "wealthReason": "R18_DOCUMENT_TERMS",
                        "dartRatio": ev.get("rightsRatio"),
                        "issuePriceDerived": ev.get("issuePrice"),
                        "recordDate": ev.get("recordDate"),
                        "paymentDate": ev.get("paymentDate"),
                        "listingDate": ev.get("listingDate")})
        elif layer == "WEALTH_CONFIRMED":
            row.update({"holderRight": False, "resolvedWealth": True,
                        "wealthReason": "R18_NO_HOLDER_RIGHT_IN_WINDOW"})
        elif layer == "WEALTH_PARTIAL":
            row.update({"holderRight": False, "resolvedWealth": False,
                        "wealthReason": "R18_TERMS_UNRELIABLE"})
        else:
            row.update({"holderRight": False, "resolvedWealth": False,
                        "wealthReason": "R18_NO_DOCUMENT"})
        out.append(row)
    return out


# ═══════════ false positive audit (§17) ═══════════
def fp_audit(matches):
    """HIGH confidence 분류를 원문 근거와 교차검증한다."""
    docs = {d["rcept_no"]: d for d in
            json.loads((RD / "r18-document-parser-results-latest.json")
                       .read_text(encoding="utf-8"))["documents"]}
    by_method = {}
    for d in docs.values():
        if d.get("confidence") == "HIGH" and d.get("issueMethod"):
            by_method.setdefault(d["issueMethod"], []).append(d)

    checks, rows = [], []
    for meth, ds in sorted(by_method.items()):
        # 결정론적 표본: 접수번호 정렬 후 균등 간격 10건(적으면 전수)
        ds.sort(key=lambda x: x["rcept_no"])
        n = min(10, len(ds))
        step = max(1, len(ds) // n)
        sample = [ds[i * step] for i in range(n)]
        ok = 0
        for d in sample:
            holder = meth in ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED")
            # 독립 구조 증거: 주주배정 전용 필드 vs 제3자 전용 필드
            struct_ok = (d.get("hasShareholderOnlyFields") if holder
                         else not d.get("hasShareholderOnlyFields")
                         or d.get("hasThirdPartyOnlyFields"))
            path_ok = d.get("evidencePath") in ("FIELD_CODE", "TABLE_LABEL")
            good = bool(struct_ok and path_ok)
            ok += good
            rows.append({"method": meth, "rcept_no": d["rcept_no"],
                         "issueMethodRaw": d.get("issueMethodRaw"),
                         "evidencePath": d.get("evidencePath"),
                         "hasShareholderOnlyFields": d.get("hasShareholderOnlyFields"),
                         "hasThirdPartyOnlyFields": d.get("hasThirdPartyOnlyFields"),
                         "crossCheckPass": good})
        checks.append({"method": meth, "population": len(ds), "sampled": n,
                       "crossCheckPass": ok,
                       "falsePositiveRate": round((n - ok) / n, 4) if n else None})

    total_s = sum(c["sampled"] for c in checks)
    total_ok = sum(c["crossCheckPass"] for c in checks)
    out = {"task": "R18",
           "method": ("HIGH confidence 문서를 방식별로 결정론적 표본추출(각 10건 또는 "
                      "전수)해 **독립 구조 증거**와 교차검증한다. 주주배정 계열이면 "
                      "배정기준일·1주당배정주식수·청약기간 같은 주주배정 전용 필드가 "
                      "있어야 하고, 제3자·공모면 없어야 한다. CI_MTH 문자열과 "
                      "독립적인 검사다."),
           "byMethod": checks,
           "sampled": total_s, "passed": total_ok,
           "falsePositiveRate": round((total_s - total_ok) / total_s, 4)
           if total_s else None,
           "falseNegativeClue": ("evidencePath 가 BODY_KEYWORD 인 문서는 LOW 로 "
                                 "고정돼 resolved 에서 빠진다 = false negative 쪽으로 "
                                 "치우친 설계다."),
           "samples": rows}
    save("false-positive-audit", out)
    return out


# ═══════════ anchor (§16) ═══════════
ANCHOR_PLAN = [("주주배정", "SHAREHOLDER_RIGHTS", 2),
               ("주주배정후 실권주 일반공모", "RIGHTS_THEN_PUBLIC", 1),
               ("제3자배정", "THIRD_PARTY", 1),
               ("일반공모", "PUBLIC_OFFERING", 1)]


def manual_ledger(m, eng):
    i = eng.pos.get(m["date"])
    if i is None or i == 0:
        return None
    p_cum, p_ex = eng.price(i - 1, m["ticker"]), eng.price(i, m["ticker"])
    if not p_cum or not p_ex:
        return None
    r, k = m.get("dartRatio"), m.get("issuePriceDerived")
    s0, w0 = 1.0, p_cum
    if m.get("holderRight") and k and r and k < p_ex:
        s1, ext, note = s0 * (1 + r), s0 * r * k, "주주배정 청약(K < 권리락가)"
    elif m.get("holderRight") and k and r:
        s1, ext, note = s0, 0.0, "K >= 권리락가 → 합리적 실권"
    else:
        s1, ext, note = s0, 0.0, "기존 주주 권리 없음 → 미조정"
    w1 = s1 * p_ex
    return {"startShares": s0, "startPrice": p_cum, "rightsRatio": r,
            "issuePrice": k, "externalContribution": ext,
            "endingShares": s1, "exRightPrice": p_ex,
            "rightsValue": (r * max(0.0, p_ex - k) if (r and k) else 0.0),
            "endWealth": w1, "manualReturn": (w1 - ext) / w0 - 1.0, "note": note}


def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    legacy = [m for m in matches
              if m.get("source") == "R18_DOCUMENT" and m["date"] < "2015"
              and m.get("confidence") == "HIGH"]
    pool = {}
    for m in legacy:
        lm = (m.get("evidence") or {}).get("methodsInWindow")
        key = m.get("r18Label")
        pool.setdefault(key, []).append(m)

    # 라벨이 아니라 실제 방식으로 고르기 위해 문서 방식을 재조회
    docs = {d["rcept_no"]: d for d in
            json.loads((RD / "r18-document-parser-results-latest.json")
                       .read_text(encoding="utf-8"))["documents"]}
    by_meth = {}
    for m in legacy:
        rn = m.get("rcept_no")
        d = docs.get(rn)
        meth = d.get("issueMethod") if d else None
        if meth:
            by_meth.setdefault(meth, []).append(m)

    cases, missing = [], []
    for label, meth, want in ANCHOR_PLAN:
        cand = [c for c in by_meth.get(meth, []) if manual_ledger(c, eng)]
        cand.sort(key=lambda c: (c["date"], c["ticker"]))
        if len(cand) < want:
            missing.append(f"{label} (확보 {len(cand)}/{want})")
        for m in cand[:want]:
            man = manual_ledger(m, eng)
            i = eng.pos[m["date"]]
            run = rw.run(m["ticker"], i - 1, i, reinvest=False, ledger=True)
            er = run["twr"] if run else None
            cases.append({
                "case": label, "ticker": m["ticker"], "date": m["date"],
                "rcept_no": m.get("rcept_no"), "issueMethod": meth,
                "confidence": m["confidence"], "provenance": m["provenance"],
                "manual": man, "engineReturn": er,
                "diffPp": None if er is None else round(
                    (er - man["manualReturn"]) * 100, 6),
                "pass": er is not None and abs(er - man["manualReturn"]) < 5e-4})

    # 정정 chain 이 긴 case (§16 마지막 요구)
    ch = json.loads((RD / "r18-correction-chains-latest.json")
                    .read_text(encoding="utf-8"))["chains"]
    ch = sorted(ch, key=lambda c: -c["filings"])
    # §16 은 legacy(2007~2014) anchor 를 요구한다. 정정 chain 도 legacy 에서 고른다.
    legacy_tk = {m["ticker"] for m in legacy}
    leg_ch = [c for c in ch if c["ticker"] in legacy_tk
              and str(c["first"]["date"])[:4] <= "2014"]
    corr = None
    if leg_ch:
        c = leg_ch[0]
        corr = {**c, "usedFinalTerms": True, "scope": "LEGACY_2007_2014",
                "why": "접수일 최신본을 final effective terms 로 썼다(§9)."}
    elif ch:
        c = ch[0]
        corr = {**c, "usedFinalTerms": True, "scope": "ANY_PERIOD",
                "note": "legacy 구간에 다건 정정 chain 이 없어 전체에서 골랐다.",
                "why": "접수일 최신본을 final effective terms 로 썼다(§9)."}
    corr_overall = {**ch[0], "scope": "OVERALL_LONGEST"} if ch else None

    out = {"task": "R18", "cases": cases, "missingCaseTypes": missing,
           "longestCorrectionChain": corr,
           "longestCorrectionChainOverall": corr_overall,
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "atLeastFive": len(cases) >= 5,
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


# ═══════════ ex-rights 연속성 (§15) — R17 방법 재사용 ═══════════
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
    theo_med = statistics.median(theo) if theo else None
    ev = windows[0]
    match = (theo_med is not None and ev["medianDiff"] is not None
             and abs(ev["medianDiff"] - theo_med) < 0.02)
    out = {"task": "R18", "method": "R17 방법 재사용 — R16 대비 차분",
           "rightsEventsTested": len(tgt), "windows": windows,
           "theoreticalMedian": round(theo_med, 4) if theo_med else None,
           "theoreticalDefinition": "r · max(0, P_ex - K) / P_cum",
           "eventMonthMedianDiff": ev["medianDiff"],
           "diffMatchesTheory": match,
           "eventMonthDiffNeverNegative": ev["negativeDiffCount"] == 0,
           "freeWealthEvents": free, "noFreeWealth": free == 0,
           "pass": match and ev["negativeDiffCount"] == 0 and free == 0}
    save("ex-rights-continuity", out)
    return out


# ═══════════ materiality (§19·§21) — R17 방법론 재사용 ═══════════
def materiality(eng, matches, universe):
    unres = [m for m in matches if not m.get("resolvedWealth")]
    res = [m for m in matches if m.get("resolvedWealth")]
    ut = {m["ticker"] for m in unres}
    months = {m["date"] for m in unres}
    total_months = len(eng.dates)

    srs = sorted(abs(m["shareRatio"] - 1.0) for m in unres)
    extreme = [m for m in unres if m["shareRatio"] >= 2.0 or m["shareRatio"] <= 0.5]
    us = [u for u in (W.understatement(m, eng) for m in matches
                      if m.get("holderRight") and m.get("issuePriceDerived"))
          if u is not None]
    cond = statistics.median(us) if us else None

    # P(주주배정 | 유상증자) — R18 원문 근거로 갱신. 이제 직접 관측이다.
    docs = json.loads((RD / "r18-document-parser-results-latest.json")
                      .read_text(encoding="utf-8"))["byMethod"]
    hr = sum(v for k, v in docs.items()
             if k in ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED"))
    tot = sum(v for k, v in docs.items() if k)
    p_hr = (hr / tot) if tot else None
    expected = (p_hr * cond) if (p_hr and cond) else None

    reasons = {}
    for m in unres:
        k = m.get("wealthReason", "?")
        reasons[k] = reasons.get(k, 0) + 1

    out = {"task": "R18", "suspectedTotal": len(matches),
           "wealthResolved": len(res), "wealthUnresolved": len(unres),
           "unresolvedReasons": reasons,
           "unresolvedTickers": len(ut), "universeTickers": universe,
           "unresolvedTickerPct": round(len(ut) / universe * 100, 2),
           "unresolvedEventPct": round(len(unres) / len(matches) * 100, 2),
           "unresolvedMonths": len(months), "totalMonths": total_months,
           "unresolvedMonthPct": round(len(months) / total_months * 100, 2),
           "unresolvedMedianShareChange": round(statistics.median(srs), 4)
           if srs else None,
           "extremeDiscontinuityEvents": len(extreme),
           "extremeDiscontinuityPct": round(len(extreme) / len(matches) * 100, 2),
           "understatementSample": {
               "n": len(us),
               "conditionalMedian": round(cond, 4) if cond else None,
               "pHolderRight": round(p_hr, 4) if p_hr else None,
               "pHolderRightSource": "R18 원문 직접관측 (문서 전수 방식 분포)",
               "expectedUnderstatement": round(expected, 4) if expected else None,
               "definition": "r · max(0, P_ex - K) / P_cum",
               "methodologyNote": "R17 방법론 그대로. 새 threshold 를 발명하지 않는다(§21).",
           },
           "biasDirection": "CONSERVATIVE_UNDERSTATEMENT",
           "biasWhy": ("미해결·WEALTH_PARTIAL 은 전부 미조정으로 남는다. 미조정은 "
                       "제3자·공모면 정답이고 주주배정이면 과소평가다. 어느 쪽도 "
                       "wealth 를 부풀리지 않는다."),
           "progression": {"R16": 40.8, "R17": 25.57,
                           "R18": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


# ═══════════ 판정 (§22) ═══════════
def verdict(mat, anc, cont, fp):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct = mat["unresolvedTickerPct"]
    us = (mat["understatementSample"]["expectedUnderstatement"] or 0) * 100
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]

    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anc["allPass"],
        "anchorsAtLeastFive": anc["atLeastFive"],
        "exRightsContinuityPass": cont["pass"],
        "noFakeWealth": cont["noFreeWealth"],
        "unresolvedTickerPctUnder20": tpct < pl["unresolvedTickerPctMax"],
        "unresolvedTickerPctUnder5": tpct < p["unresolvedTickerPctMax"],
        "expectedUnderstatementUnder2pct": us < pl["medianUnderstatementPctMax"],
        "extremeDiscontinuityUnder1pct":
            mat["extremeDiscontinuityPct"] < p["extremeDiscontinuityPctMax"],
    }
    if not engine_ok:
        v = "CANONICAL_TSR_FOUNDATION_FAIL"
    elif (checks["unresolvedTickerPctUnder5"]
          and checks["extremeDiscontinuityUnder1pct"]):
        v = "CANONICAL_TSR_FOUNDATION_PASS"
    elif (checks["unresolvedTickerPctUnder20"]
          and checks["expectedUnderstatementUnder2pct"]):
        v = "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"
    else:
        v = "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"

    out = {"task": "R18", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct,
           "expectedUnderstatementPct": round(us, 3),
           "falsePositiveRate": fp["falsePositiveRate"],
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "passWithLimitsMaxUnderstatementPct":
                   pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "progression": mat["progression"],
           "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                          "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("R18 이 foundation 을 풀어도 기존 수익률을 자동 부활시키지 "
                        "않는다(§23). 별도 factor rediscovery 에서 새 TSR 로 "
                        "원점 재실행해야 한다.")}}
    save("foundation-verdict", out)
    return out


def main() -> int:
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    universe = len({t for d in ds for t in cap.get(d, {})})
    matches = merged_matches()
    save("full-reconciliation", {
        "task": "R18", "total": len(matches),
        "bySource": {s: sum(1 for m in matches if m.get("source") == s)
                     for s in ("R17", "R18_DOCUMENT")},
        "byLabel": {k: sum(1 for m in matches if m["label"] == k)
                    for k in {m["label"] for m in matches}},
        "byWealthLayer": {k: sum(1 for m in matches if m.get("wealthLayer") == k)
                          for k in {m.get("wealthLayer") for m in matches}},
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "layerVsTypeNote": ("유형 확정과 wealth 확정은 다르다(§18). wealth 확정은 "
                            "엔진 동작이 정해지는지를 뜻한다."),
        "rows": matches})
    fp = fp_audit(matches)
    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    mat = materiality(eng, matches, universe)
    v = verdict(mat, anc, cont, fp)
    print(json.dumps({"verdict": v["verdict"], "progression": v["progression"],
                      "wealthResolved": mat["wealthResolved"],
                      "anchors": len(anc["cases"]), "anchorsPass": anc["allPass"],
                      "fpRate": fp["falsePositiveRate"],
                      "noFakeWealth": cont["noFreeWealth"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
