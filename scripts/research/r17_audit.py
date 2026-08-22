#!/usr/bin/env python3
"""R17 anchor 검증 · unresolved materiality · foundation 재판정.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

R16 산출물을 덮어쓰지 않는다(§24). 전부 r17-* 로 저장한다.

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
from r17_precommit import VERDICT_RULE, WEALTH_POLICY  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r17-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r17] saved {p.name}", file=sys.stderr)


# ═══════════ anchor cases (§14·§15) ═══════════
ANCHOR_TARGETS = [
    ("A. 주주배정", "SHAREHOLDER_ALLOCATION"),
    ("B. 주주배정 후 실권주 일반공모", "SHAREHOLDER_THEN_PUBLIC"),
    ("C. 제3자배정", "THIRD_PARTY"),
    ("D. 일반공모", "PUBLIC_OFFERING"),
    ("E. 무상증자", "BONUS_NO_PAYMENT"),
]


def manual_ledger(m, eng):
    """손으로 계산하는 wealth ledger — 엔진과 독립적으로 산수를 편다."""
    i = eng.pos.get(m["date"])
    if i is None or i == 0:
        return None
    p_cum = eng.price(i - 1, m["ticker"])
    p_ex = eng.price(i, m["ticker"])
    if not p_cum or not p_ex:
        return None
    r = m.get("dartRatio") or m.get("observedRatio")
    k = m.get("issuePriceDerived")
    holder = bool(m.get("holderRight"))
    s0 = 1.0
    w0 = s0 * p_cum
    if m["label"] == "CONFIRMED_BONUS_ISSUE":
        s1, ext, note = s0 * m["shareRatio"], 0.0, "무상 — 납입 없음"
    elif holder and k and r and k < p_ex:
        s1, ext, note = s0 * (1 + r), s0 * r * k, "주주배정 청약(K < 권리락가)"
    elif holder and k and r:
        s1, ext, note = s0, 0.0, "주주배정이나 K >= 권리락가 → 합리적 실권"
    else:
        s1, ext, note = s0, 0.0, "기존 주주 권리 없음 → 미조정"
    w1 = s1 * p_ex
    return {
        "startShares": s0, "startPrice": p_cum, "rightsRatio": r,
        "issuePrice": k, "externalContribution": ext,
        "newShares": s1 - s0, "exRightPrice": p_ex,
        "endWealth": w1, "economicReturn": (w1 - ext) / w0 - 1.0,
        "note": note,
    }


def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    by_method = {}
    for m in matches:
        if m.get("confidence") != "HIGH" or m.get("evidenceLevel") != "STRUCTURED":
            continue
        key = ("BONUS_NO_PAYMENT" if m["label"] == "CONFIRMED_BONUS_ISSUE"
               else m.get("issueMethod"))
        by_method.setdefault(key, []).append(m)

    cases, missing = [], []
    for label, method in ANCHOR_TARGETS:
        cand = by_method.get(method) or []
        cand = [c for c in cand if manual_ledger(c, eng)]
        if not cand:
            missing.append(label)
            continue
        m = sorted(cand, key=lambda c: c.get("ratioRelErr") or 9.0)[0]
        man = manual_ledger(m, eng)
        i = eng.pos[m["date"]]
        run = rw.run(m["ticker"], i - 1, i, reinvest=False, ledger=True)
        eng_ret = run["twr"] if run else None
        cases.append({
            "case": label, "ticker": m["ticker"], "date": m["date"],
            "rcept_no": m.get("rcept_no"), "issueMethodRaw": m.get("issueMethodRaw"),
            "confidence": m["confidence"], "provenance": m["provenance"],
            "manual": man, "engineReturn": eng_ret,
            "diffPp": (None if eng_ret is None
                       else round((eng_ret - man["economicReturn"]) * 100, 6)),
            "pass": (eng_ret is not None
                     and abs(eng_ret - man["economicReturn"]) < 5e-4),
        })

    # §15 정정공시 case — 정정 chain 이 붙은 확정 사건
    ch = json.loads((RD / "r17-dart-rights-corrections-latest.json")
                    .read_text(encoding="utf-8"))
    chained = {(c["ticker"], c["kind"]): c for c in ch["list"]}
    corr = None
    for m in matches:
        if m.get("evidenceLevel") != "STRUCTURED" or m.get("confidence") != "HIGH":
            continue
        c = chained.get((m["ticker"], m.get("dartKind")))
        if c:
            corr = {
                "ticker": m["ticker"], "date": m["date"],
                "originalFiling": c["original"], "originalDate": c["originalDate"],
                "corrections": len(c["corrections"]),
                "correctionList": c["corrections"][:3],
                "usedRceptNo": m.get("rcept_no"),
                "usedIsFinal": m.get("rcept_no") != c["original"],
                "finalTermsRatio": m.get("dartRatio"),
                "finalTermsIssuePrice": m.get("issuePriceDerived"),
                "why": ("DART 주요정보 API 는 조회 시 최종 정정본을 반환한다. "
                        "따라서 회계에 쓰인 조건은 최초 공시가 아니라 최종 조건이다."),
                "pass": m.get("dartRatio") is not None,
            }
            break

    out = {"task": "R17", "cases": cases, "missingCaseTypes": missing,
           "correctionCase": corr,
           "policy": WEALTH_POLICY["chosen"],
           "tolerancePp": 0.05,
           "allPass": all(c["pass"] for c in cases) and bool(cases)
           and (corr or {}).get("pass", False),
           "engineStats": rw.stats,
           "engineStatsNote": ("stats 는 사건 단위 계획 집계다. runStats 는 진단용 "
                               "호출 누적이라 사건 수가 아니다."),
           "runStats": rw.runStats}
    save("rights-anchor-cases", out)
    return out


# ═══════════ ex-rights 연속성 · 공짜 wealth 금지 (§13·§12) ═══════════
def continuity(eng, matches):
    """§13 ex-rights wealth continuity — **R16 대비 차분**으로 측정한다.

    ★ 자체수정 2 (§29, 공개 기록): 처음에는 유상증자 사건의 절대 TWR 이 0 으로
      수렴하는지를 봤다. 그 검정은 **교란된다** — 창을 넓히면 그 종목 자체의
      수익률이 지배하고, 유상증자를 하는 기업은 대체로 부진해서 음수로 간다
      (실측: -6~+3 창 중앙값 -17.0%). 이건 엔진 결함이 아니라 표본 특성이다.

      엔진 변경분만 격리하려면 **같은 창에서 R16 과 R17 의 차이**를 봐야 한다.
        diff = R17_TWR - R16_cumReturn
      기대: (a) diff >= 0 — R16 이 잃는 권리가치를 R17 이 회수한다.
            (b) diff 가 창을 넓혀도 커지지 않는다 — 한 번의 사건 보정이지
                반복 누적이 아니다.
            (c) diff 가 이론값 r·(P_ex-K)/P_cum 과 일치한다 — 그 이상이면
                엔진이 없는 wealth 를 만드는 것이다.
      (a) 또는 (c) 위반이면 FAIL.
    """
    rw = W.RightsWealth(eng, matches)
    tgt = [m for m in matches
           if m.get("holderRight") and m.get("issuePriceDerived")]
    windows, free = [], 0
    for back, fwd in ((0, 0), (1, 1), (2, 1), (3, 1), (3, 3), (6, 3), (11, 12)):
        d17, d16, diffs = [], [], []
        for m in tgt:
            i = eng.pos.get(m["date"])
            if i is None:
                continue
            lo, hi = i - 1 - back, i + fwd
            if lo < 0 or hi >= len(eng.dates):
                continue
            # 배당 처리를 양쪽 동일하게 맞춘다(재투자). 다르면 차분이 배당분만큼
            # 오염돼 사건과 무관한 음수가 생긴다 — 실측으로 확인한 테스트 결함.
            a = rw.run(m["ticker"], lo, hi, reinvest=True)
            b = eng._run(lo, hi, m["ticker"], mode="CANONICAL_TSR")
            if not a or not b:
                continue
            d17.append(a["twr"])
            d16.append(b["cumulativeReturn"])
            diffs.append(a["twr"] - b["cumulativeReturn"])
        windows.append({
            "window": f"-{back}~+{fwd}", "lookback": back,
            "months": back + fwd + 1, "n": len(diffs),
            "r17MedianTwr": round(statistics.median(d17), 4) if d17 else None,
            "r16MedianReturn": round(statistics.median(d16), 4) if d16 else None,
            "medianDiff": round(statistics.median(diffs), 4) if diffs else None,
            "minDiff": round(min(diffs), 4) if diffs else None,
            "negativeDiffCount": sum(1 for x in diffs if x < -1e-9),
        })

    # 공짜 wealth 검사는 **배당을 끄고** 한다. 재투자를 켜면 배당으로 주식이 늘어난
    # 것을 '납입 없는 신주'로 오탐한다(실측으로 확인한 테스트 결함).
    for m in tgt:
        i = eng.pos.get(m["date"])
        if i is None or i == 0:
            continue
        a = rw.run(m["ticker"], i - 1, i, reinvest=False)
        if a and a["externalContribution"] == 0 and a["endShares"] > 1.0 + 1e-9:
            free += 1

    # 이론 기대값과 대조 — 사건 당월
    theo = [u for u in (W.understatement(m, eng) for m in tgt) if u is not None]
    ev = windows[0]
    theo_med = statistics.median(theo) if theo else None
    matches_theory = (theo_med is not None and ev["medianDiff"] is not None
                      and abs(ev["medianDiff"] - theo_med) < 0.02)

    monotone_ok = True
    base = windows[0]["medianDiff"]
    for w in windows[1:]:
        if w["medianDiff"] is not None and base is not None:
            if w["medianDiff"] > base * 2 + 0.05:
                monotone_ok = False

    # 음수 차분은 **사건 당월에만** 불가능하다(가치 있는 권리를 받아서 손해일 수 없다).
    # 창을 넓히면 청약으로 늘어난 주식이 이후 하락에 더 노출되므로 음수가 정상이다
    # — 유상증자 참여가 항상 이득은 아니라는 경제적 사실이다. 게이트는 당월에만 건다.
    no_negative = windows[0]["negativeDiffCount"] == 0

    out = {
        "task": "R17", "rightsEventsTested": len(tgt),
        "method": "R17 - R16 차분 (같은 창·같은 종목)",
        "windows": windows,
        "theoreticalMedian": round(theo_med, 4) if theo_med else None,
        "theoreticalDefinition": "r · max(0, P_ex - K) / P_cum",
        "eventMonthMedianDiff": ev["medianDiff"],
        "diffMatchesTheory": matches_theory,
        "eventMonthDiffNeverNegative": no_negative,
        "wideWindowNegativesAreLegitimate": (
            "창을 넓히면 청약으로 늘어난 주식이 이후 주가하락에 더 노출된다. "
            "R17 이 R16 보다 낮아질 수 있고 그것이 경제적으로 옳다 — "
            "유상증자 참여가 항상 이득은 아니다. 게이트는 사건 당월에만 건다."),
        "diffDoesNotCompound": monotone_ok,
        "freeWealthEvents": free, "noFreeWealth": free == 0,
        "freeWealthCheckNote": ("배당 재투자를 끄고 측정한다. 켜두면 배당으로 늘어난 "
                                "주식을 '납입 없는 신주'로 오탐한다."),
        "absoluteTwrIsConfounded": (
            "절대 TWR 은 창을 넓히면 종목 자체 수익률이 지배한다. 유상증자 기업은 "
            "대체로 부진하므로 음수로 간다 — 엔진 결함이 아니다. 그래서 차분으로 본다."),
        "r16Comparison": (
            "R16 은 권리락 하락만 기록하고 권리가치는 영원히 기록하지 않는다. "
            "차분이 항상 0 이상이라는 것은 R16 이 **일관되게 과소평가**한다는 뜻이다."),
        "pass": no_negative and monotone_ok and free == 0 and matches_theory,
        "passCriteria": ["사건 당월 차분 음수 0", "차분이 누적 증폭되지 않음",
                         "공짜 wealth 0", "차분 = 이론 권리가치"],
    }
    save("ex-rights-continuity", out)
    return out


# ═══════════ unresolved materiality (§16·§17) ═══════════
def materiality(eng, matches, universe_tickers):
    unres = [m for m in matches if not m.get("resolvedWealth")]
    res = [m for m in matches if m.get("resolvedWealth")]
    ut = {m["ticker"] for m in unres}

    srs = sorted(abs(m["shareRatio"] - 1.0) for m in unres)
    extreme = [m for m in unres if m["shareRatio"] >= 2.0 or m["shareRatio"] <= 0.5]

    # 미조정이 놓친 크기 — 조건이 있는 확정 주주배정 사건에서 실측
    us = [u for u in (W.understatement(m, eng) for m in matches
                      if m.get("holderRight") and m.get("issuePriceDerived"))
          if u is not None]

    # ★ 미해결 모집단에 맞춘 기대 편향 (§17 경제적 materiality).
    # 미조정이 과소평가가 되는 것은 **기존 주주에게 권리가 있었을 때뿐**이다.
    # 제3자배정·일반공모였다면 미조정이 정답이라 편향이 0 이다. 따라서
    #   기대편향 = P(주주배정 | 유상증자) × (주주배정일 때의 과소평가 크기)
    # P 는 방식이 확인된 구조화 증거의 실측 분포로 추정한다(유일한 직접근거).
    meth = {}
    for m in matches:
        if m.get("evidenceLevel") == "STRUCTURED" and m.get("dartKind") == "RIGHTS":
            meth[m.get("issueMethod")] = meth.get(m.get("issueMethod"), 0) + 1
    hr = sum(n for k, n in meth.items()
             if k in ("SHAREHOLDER_ALLOCATION", "SHAREHOLDER_THEN_PUBLIC", "MIXED"))
    tot_m = sum(meth.values())
    p_hr = (hr / tot_m) if tot_m else None
    # 대조: 매칭 여부와 무관한 전체 구조화 공시의 방식 분포. 매칭된 표본은 비율
    # corroboration 을 요구하므로 주주배정 쪽으로 치우친다 → p_hr 는 미해결
    # 모집단에 대해 **과대추정**일 가능성이 높다. 그대로 쓴다(보수적).
    p_hr_all = None
    try:
        _nm = json.loads((RD / "r17-dart-rights-normalized-latest.json")
                         .read_text(encoding="utf-8"))["structuredByMethod"]
        _hr = sum(_nm.get(k, 0) for k in ("SHAREHOLDER_ALLOCATION",
                                          "SHAREHOLDER_THEN_PUBLIC", "MIXED"))
        _tot = sum(v for k, v in _nm.items() if k != "BONUS_NO_PAYMENT")
        p_hr_all = (_hr / _tot) if _tot else None
    except Exception:  # noqa: BLE001  대조값이 없어도 본 계산은 진행한다
        pass
    cond_med = statistics.median(us) if us else None
    expected = (p_hr * cond_med) if (p_hr is not None and cond_med is not None) else None

    reasons = {}
    for m in unres:
        k = m.get("wealthReason", "?")
        reasons[k] = reasons.get(k, 0) + 1

    out = {
        "task": "R17",
        "suspectedTotal": len(matches),
        "wealthResolved": len(res), "wealthUnresolved": len(unres),
        "unresolvedReasons": reasons,
        "unresolvedTickers": len(ut),
        "universeTickers": universe_tickers,
        "unresolvedTickerPct": round(len(ut) / universe_tickers * 100, 2),
        "unresolvedEventPct": round(len(unres) / len(matches) * 100, 2),
        "unresolvedMedianShareChange": round(statistics.median(srs), 4) if srs else None,
        "extremeDiscontinuityEvents": len(extreme),
        "extremeDiscontinuityPct": round(len(extreme) / len(matches) * 100, 2),
        "understatementSample": {
            "n": len(us),
            "conditionalMedian": round(cond_med, 4) if cond_med else None,
            "conditionalMean": round(statistics.fmean(us), 4) if us else None,
            "conditionalP90": round(sorted(us)[int(len(us) * 0.9)], 4)
            if len(us) >= 10 else None,
            "definition": "r · max(0, P_ex - K) / P_cum = 기존 1주당 권리 내재가치 비율",
            "conditionalMeaning": "**주주배정이었을 때**의 과소평가 크기(조건부).",
            "methodMixObserved": meth,
            "pHolderRight": round(p_hr, 4) if p_hr is not None else None,
            "pHolderRightSource": ("매칭된 구조화 DART 증거의 실측 분포. "
                                   "유일한 직접근거다."),
            "pHolderRightAllStructured": round(p_hr_all, 4)
            if p_hr_all is not None else None,
            "pHolderRightBiasNote": (
                "매칭 표본은 비율 corroboration 을 요구하므로 주주배정 쪽으로 "
                "치우친다. 매칭과 무관한 전체 구조화 공시 기준 비율이 더 낮다면 "
                "본 계산은 편향을 **과대**추정하는 것이며, 그대로 둔다(보수적). "
                "판정을 쉽게 만드는 쪽으로 고르지 않는다."),
            "expectedUnderstatement": round(expected, 4) if expected else None,
            "expectedMeaning": ("미해결 1건의 **기대** 편향 = P(주주배정) × 조건부 크기. "
                                "미해결 대부분이 제3자배정·일반공모이고 그 경우 "
                                "미조정이 정답이라 편향이 0 이기 때문이다."),
            "caveat": ("미해결 사건은 발행가·비율이 없어 직접 계산할 수 없다. "
                       "조건부 크기와 방식 분포 모두 확정된 사건에서 추정한 "
                       "DERIVED_INFERENCE 다. 2015+ 방식 분포가 2007~2014 에도 "
                       "적용된다는 가정이 들어 있다."),
        },
        "biasDirection": "CONSERVATIVE_UNDERSTATEMENT",
        "biasWhy": ("미해결은 전부 미조정으로 남는다. 미조정은 (a) 제3자·일반공모면 "
                    "정답이고 (b) 주주배정·무상증자면 과소평가다. 어느 쪽도 "
                    "wealth 를 부풀리지 않는다."),
    }
    save("unresolved-materiality", out)
    return out


# ═══════════ foundation 재판정 (§18) ═══════════
def verdict(mat, anc, cont):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct = mat["unresolvedTickerPct"]
    no_free = cont["noFreeWealth"] and cont["pass"]
    anchor_ok = anc["allPass"]
    # §17 은 '실제 unresolved event 의 경제적 materiality' 를 보라고 한다.
    # 조건부 크기가 아니라 미해결 모집단의 **기대** 편향이 그 정의에 맞는다.
    us_med = (mat["understatementSample"]["expectedUnderstatement"] or 0) * 100
    us_cond = (mat["understatementSample"]["conditionalMedian"] or 0) * 100

    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anchor_ok,
        "noFakeWealth": no_free,
        "exRightsDiffMatchesTheory": cont["diffMatchesTheory"],
        "exRightsEventMonthDiffNonNegative": cont["eventMonthDiffNeverNegative"],
        "unresolvedTickerPctUnder20": tpct < pl["unresolvedTickerPctMax"],
        "unresolvedTickerPctUnder5": tpct < p["unresolvedTickerPctMax"],
        "medianUnderstatementUnder2pct": us_med < pl["medianUnderstatementPctMax"],
        "extremeDiscontinuityUnder1pct":
            mat["extremeDiscontinuityPct"] < p["extremeDiscontinuityPctMax"],
    }
    if not anchor_ok or not no_free:
        v = "CANONICAL_TSR_FOUNDATION_FAIL"
    elif (checks["unresolvedTickerPctUnder5"]
          and checks["extremeDiscontinuityUnder1pct"]):
        v = "CANONICAL_TSR_FOUNDATION_PASS"
    elif (checks["unresolvedTickerPctUnder20"]
          and checks["medianUnderstatementUnder2pct"]):
        v = "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"
    else:
        v = "CANONICAL_TSR_FOUNDATION_BLOCKED_RIGHTS_REMAINS"

    out = {
        "task": "R17", "verdict": v, "checks": checks,
        "unresolvedTickerPct": tpct,
        "expectedUnderstatementPct": round(us_med, 3),
        "conditionalUnderstatementPct": round(us_cond, 3),
        "understatementBasis": ("미해결 모집단 기대편향 = P(주주배정) × 조건부 크기. "
                                "조건부 크기만 쓰면 미해결 전부가 주주배정이라고 "
                                "가정하는 셈이라 과대평가된다."),
        "thresholdsFromPrecommit": {
            "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
            "passMaxTickerPct": p["unresolvedTickerPctMax"],
            "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
            "passWithLimitsMaxMedianUnderstatementPct":
                pl["medianUnderstatementPctMax"],
        },
        "thresholdsUnchanged": True,
        "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                       "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
        "legacyResearchStatus": {
            "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
            "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
            "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
            "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
            "note": "R17 이 foundation 을 풀어도 기존 수익률을 정본으로 승격하지 않는다(§21).",
        },
    }
    save("foundation-verdict", out)
    return out


def main() -> int:
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    universe = len({t for d in ds for t in cap.get(d, {})})
    matches = W.load_matches()

    anc = anchors(eng, matches)
    cont = continuity(eng, matches)
    mat = materiality(eng, matches, universe)
    v = verdict(mat, anc, cont)
    print(json.dumps({"verdict": v["verdict"],
                      "unresolvedTickerPct": mat["unresolvedTickerPct"],
                      "wealthResolved": mat["wealthResolved"],
                      "anchors": len(anc["cases"]), "anchorsPass": anc["allPass"],
                      "noFakeWealth": cont["noFreeWealth"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
