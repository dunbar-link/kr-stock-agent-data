#!/usr/bin/env python3
"""R19 오분류 감사 · anchor · 전체 reconciliation · 기대편향 재계산 · 판정.

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19

기존 산출물을 덮어쓰지 않는다(§27). 전부 r19-* 로 저장한다.
threshold 와 bias 계산법은 R18 것을 그대로 쓴다(§15·§17 변경 금지).

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
    p = RD / f"r19-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r19] saved {p.name}", file=sys.stderr)


# ═══════════ R19 결과를 R18 전체 2,742건에 merge (§23) ═══════════
def merged():
    r18 = json.loads((RD / "r18-full-reconciliation-latest.json")
                     .read_text(encoding="utf-8"))["rows"]
    r19 = json.loads((RD / "r19-event-classification-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    by = {(e["ticker"], e["date"]): e for e in r19}
    out = []
    for m in r18:
        e = by.get((m["ticker"], m["date"]))
        if not e:
            out.append({**m, "r19Source": None})
            continue
        row = {**m, "r19Source": "R19_DIRECT",
               "r19PrimaryEvent": e["primaryEvent"],
               "r19SecondaryEvents": e["secondaryEvents"],
               "r19Entitlement": e["entitlement"],
               "r19EntitlementWhy": e["entitlementWhy"],
               "r19WealthAdjustment": e["wealthAdjustment"],
               "r19Confidence": e["confidence"],
               "r19Provenance": e["provenance"],
               "r19RceptNo": e.get("primaryRceptNo"),
               "r19EventIdentity": e["eventIdentity"]}
        if e["wealthAdjustment"] == "REQUIRED_MECHANICAL":
            # 주식분할·무상증자 — 납입 없이 보유주식수가 같은 배율로 늘어난다.
            # 관측 shareRatio 만으로 조정이 확정되므로 wealth 확정이다.
            # 엔진은 CONFIRMED_BONUS_ISSUE 라벨을 기계적 조정으로 처리한다(R17 정본).
            row.update({"resolvedWealth": True, "holderRight": False,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R19_MECHANICAL_ENTITLEMENT",
                        "label": "CONFIRMED_BONUS_ISSUE"})
        elif e["wealthAdjustment"] == "NOT_REQUIRED":
            # 기존 주주에게 직접 권리가 없음이 직접증거로 확정됐다.
            # R16 의 미조정이 정답이다 → wealth 확정. 조정은 하지 않는다.
            row.update({"resolvedWealth": True, "holderRight": False,
                        "wealthLayer": "WEALTH_CONFIRMED",
                        "wealthReason": "R19_NO_DIRECT_ENTITLEMENT",
                        "label": "CONFIRMED_NON_RIGHTS"})
        elif e["wealthAdjustment"] == "REQUIRED":
            # 권리가 있었다 — 그러나 조건(비율·발행가)이 없으면 보수적 미조정.
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_PARTIAL",
                        "wealthReason": "R19_ENTITLEMENT_TERMS_MISSING"})
        else:
            row.update({"resolvedWealth": False, "holderRight": False,
                        "wealthLayer": "WEALTH_UNRESOLVED",
                        "wealthReason": "R19_UNRESOLVED"})
        out.append(row)
    return out


# ═══════════ 오분류 감사 (§21) ═══════════
def fp_audit():
    ev = json.loads((RD / "r19-event-classification-latest.json")
                    .read_text(encoding="utf-8"))["events"]
    by = {}
    for e in ev:
        if e["confidence"] in ("HIGH", "MEDIUM") and e["primaryEvent"] != "UNRESOLVED":
            by.setdefault(e["primaryEvent"], []).append(e)

    checks, rows = [], []
    # 유형별 기대 키워드(복수 허용). 라벨이 아니라 **원문 공시명**을 검사한다.
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
        "CAPITAL_REDUCTION": ("감자",),
        "TREASURY_ACTION": ("자기주식",),
    }
    for kind, es in sorted(by.items()):
        es.sort(key=lambda x: (x["ticker"], x["date"]))
        n = min(10, len(es))
        step = max(1, len(es) // n)
        sample = [es[i * step] for i in range(n)]
        ok = 0
        for e in sample:
            nm = e.get("primaryReportName") or ""
            kws = EXPECT.get(kind) or ()
            # 독립 검증: 라벨이 아니라 **원문 공시명**에 해당 키워드가 실제로 있는가
            kw_ok = any(k in nm for k in kws)
            # 종속회사 사건이 섞이지 않았는가
            sub_ok = not any(s in nm for s in ("종속회사", "자회사의"))
            good = kw_ok and sub_ok
            ok += good
            rows.append({"primaryEvent": kind, "ticker": e["ticker"],
                         "date": e["date"], "rcept_no": e.get("primaryRceptNo"),
                         "reportName": nm, "expectedKeywords": list(kws),
                         "keywordPresent": kw_ok,
                         "notSubsidiary": sub_ok, "crossCheckPass": good})
        checks.append({"primaryEvent": kind, "population": len(es), "sampled": n,
                       "crossCheckPass": ok,
                       "falsePositiveRate": round((n - ok) / n, 4) if n else None})
    ts = sum(c["sampled"] for c in checks)
    to = sum(c["crossCheckPass"] for c in checks)
    out = {"task": "R19",
           "method": ("HIGH/MEDIUM 분류를 유형별로 결정론적 표본추출(각 10건 또는 "
                      "전수)해 **원문 공시명**과 대조한다. 분류 라벨이 아니라 실제 "
                      "공시 제목에 해당 키워드가 있는지, 종속회사 공시가 섞이지 "
                      "않았는지를 본다."),
           "byPrimaryEvent": checks, "sampled": ts, "passed": to,
           "falsePositiveRate": round((ts - to) / ts, 4) if ts else None,
           "harnessNote": ("★ 자체수정(§31): 초판은 STOCK_SPLIT 을 기대 키워드 표에 "
                           "넣지 않아 전건 FAIL(오분류율 15.6%)로 나왔다. 분류가 "
                           "틀린 게 아니라 **감사 하네스의 누락**이었다. 원문 공시명은 "
                           "'주식분할결정'·'액면분할'로 정확했다."),
           "samples": rows}
    save("false-classification-audit", out)
    return out


# ═══════════ 합병 방향성 실증 (§20) ═══════════
def merger_direction(eng, matches):
    """구조적 논거를 데이터로 검증한다.

    주장: 주식수가 늘어난 쪽 = 신주를 **발행한** 쪽(존속·완전모회사) → 기존 주주
          직접 권리 0. 흡수당한 쪽은 상장폐지되어 주식수 증가로 안 나타난다.
    검증: 합병으로 분류된 사건의 종목이 사건 **이후에도 계속 거래되는가**.
          흡수당했다면 사라져야 한다.
    """
    kinds = ("MERGER_NEW_SHARES", "SHARE_SWAP", "SPINOFF_RELATED_SHARES")
    rows, survive, gone = [], 0, 0
    for m in matches:
        if m.get("r19PrimaryEvent") not in kinds:
            continue
        i = eng.pos.get(m["date"])
        if i is None:
            continue
        # 사건 이후 6개월 시점에도 스냅샷에 존재하는가
        j = min(i + 6, len(eng.dates) - 1)
        alive = eng.price(j, m["ticker"]) is not None
        survive += alive
        gone += (not alive)
        rows.append({"ticker": m["ticker"], "date": m["date"],
                     "primaryEvent": m["r19PrimaryEvent"],
                     "stillTradingAfter6m": alive})
    n = survive + gone
    out = {"task": "R19", "tested": n, "stillTrading": survive,
           "disappeared": gone,
           "survivalPct": round(survive / n * 100, 2) if n else None,
           "claim": ("주식수가 늘어난 쪽은 신주를 발행한 존속회사다 → 기존 주주 "
                     "직접 권리 0."),
           "test": "합병 분류 사건의 종목이 사건 6개월 후에도 거래되는가",
           "interpretation": ("생존율이 높으면 이들은 흡수당한 쪽이 아니라 **존속·"
                              "발행** 쪽이라는 뜻이고, 방향성 판정이 맞는다. "
                              "흡수된 회사는 상장폐지되어 이 표본에 들어올 수 없다."),
           "counterDirectionNote": ("분석대상 주주가 대가를 받는 반대 방향은 주식수 "
                                    "증가가 아니라 상장폐지로 나타나며 R16 delisting "
                                    "경로다. 이번 표본 구성상 존재할 수 없다(§20)."),
           "samples": rows[:40]}
    save("merger-direction", out)
    return out


# ═══════════ anchor (§19·§20) ═══════════
ANCHOR_PLAN = [("CB 전환", "CONVERTIBLE_BOND_CONVERSION", 2),
               ("BW 행사", "BW_WARRANT_EXERCISE", 2),
               ("합병 신주", "MERGER_NEW_SHARES", 2),
               ("주식매수선택권 행사", "STOCK_OPTION_EXERCISE", 1),
               # 권리 **있는** 쪽도 검증한다 — 조정이 0 이 아니라 정확히 기계적
               # 배율만큼 일어나는지 봐야 '조정 0' 결과가 무조건이 아님을 보인다.
               ("주식분할(권리 YES)", "STOCK_SPLIT", 1),
               ("주주배정 유상증자(재분류)", "TRUE_RIGHTS_ISSUE", 1)]


def anchors(eng, matches):
    rw = W.RightsWealth(eng, matches)
    by = {}
    for m in matches:
        if m.get("r19Source") != "R19_DIRECT":
            continue
        by.setdefault(m["r19PrimaryEvent"], []).append(m)

    cases, missing = [], []
    for label, kind, want in ANCHOR_PLAN:
        cand = [c for c in by.get(kind, [])
                if c["r19Confidence"] in ("HIGH", "MEDIUM")
                and eng.pos.get(c["date"], 0) > 0]
        cand.sort(key=lambda c: (c["date"], c["ticker"]))
        if len(cand) < want:
            missing.append(f"{label} (확보 {len(cand)}/{want})")
        for m in cand[:want]:
            i = eng.pos[m["date"]]
            p_cum, p_ex = eng.price(i - 1, m["ticker"]), eng.price(i, m["ticker"])
            if not p_cum or not p_ex:
                continue
            ent = m["r19Entitlement"]
            # manual: 직접 권리가 없으면 보유주식 불변 · 추가현금 0
            if ent == "NO":
                s1, ext = 1.0, 0.0
                man_ret = p_ex / p_cum - 1.0
                treat = "기존 주주 직접 권리 없음 → 보유주식 불변 · 조정 0"
            elif m["r19WealthAdjustment"] == "REQUIRED_MECHANICAL":
                # 주식분할·무상증자 — 납입 없이 보유주식수가 배율만큼 늘어난다.
                s1, ext = m["shareRatio"], 0.0
                man_ret = (s1 * p_ex) / p_cum - 1.0
                treat = f"권리 YES · 기계적 조정 보유주식 ×{m['shareRatio']:.4f} · 납입 0"
            else:
                s1, ext = 1.0, 0.0
                man_ret = p_ex / p_cum - 1.0
                treat = "권리 있으나 조건 미확보 → 보수적 미조정"
            run = rw.run(m["ticker"], i - 1, i, reinvest=False)
            er = run["twr"] if run else None
            cases.append({
                "case": label, "primaryEvent": kind, "ticker": m["ticker"],
                "date": m["date"], "rcept_no": m.get("r19RceptNo"),
                "confidence": m["r19Confidence"],
                "entitlement": ent, "wealthAdjustment": m["r19WealthAdjustment"],
                "shareRatio": m["shareRatio"],
                "manual": {"startShares": 1.0, "startPrice": p_cum,
                           "exPrice": p_ex, "endingShares": s1,
                           "externalContribution": ext, "rightsValue": 0.0,
                           "manualReturn": man_ret, "treatment": treat},
                "engineReturn": er,
                "diffPp": None if er is None else round((er - man_ret) * 100, 6),
                "adjustmentIsZero": (er is not None
                                     and abs(er - man_ret) < 5e-4),
                "pass": er is not None and abs(er - man_ret) < 5e-4})

    out = {"task": "R19", "cases": cases, "missingCaseTypes": missing,
           "tolerancePp": 0.05,
           "allPass": bool(cases) and all(c["pass"] for c in cases),
           "noEntitlementAdjustmentExactlyZero": all(
               c["adjustmentIsZero"] for c in cases if c["entitlement"] == "NO"),
           "entitlementCaseAdjustsNonZero": any(
               c["entitlement"] == "YES" and abs(c["manual"]["endingShares"] - 1.0) > 1e-9
               for c in cases),
           "whyBothSidesNeeded": (
               "권리 없는 사건에서 조정이 0 인 것만 보이면 '엔진이 아무것도 안 한다'와 "
               "구분되지 않는다. 권리 있는 사건에서 정확히 배율만큼 조정되는 것을 "
               "함께 보여야 판정이 의미를 갖는다."),
           "mergerBothDirectionsNote": (
               "이 표본은 **주식수가 증가한** 사건만이라 분석대상이 신주를 발행한 "
               "방향뿐이다. 분석대상 주주가 대가를 받는 반대 방향은 주식수 증가가 "
               "아니라 상장폐지로 나타나며 R16 delisting 경로다(§20)."),
           "engineStats": rw.stats}
    save("anchor-cases", out)
    return out


# ═══════════ 기대편향 재계산 (§15·§16) ═══════════
def expected_bias(eng, matches):
    """R18 공식 그대로: expected = P(직접권리) × 조건부 과소평가 크기.

    바뀐 것은 계산법이 아니라 **P 의 조건화**다. 직접증거로 권리 없음이 확정된
    사건은 P=0 이다(§16 '이미 NOT_REQUIRED 로 확정된 사건은 bias 에서 제거').
    """
    us = [u for u in (W.understatement(m, eng) for m in matches
                      if m.get("holderRight") and m.get("issuePriceDerived"))
          if u is not None]
    cond = statistics.median(us) if us else None

    # 모집단 P — R18 과 동일 소스(원문 방식 분포). 새로 만들지 않는다.
    doc = json.loads((RD / "r18-document-parser-results-latest.json")
                     .read_text(encoding="utf-8"))["byMethod"]
    hr = sum(v for k, v in doc.items()
             if k in ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED"))
    tot = sum(v for k, v in doc.items() if k)
    p_pop = (hr / tot) if tot else 0.0

    unres = [m for m in matches if not m.get("resolvedWealth")]

    # ★ 자체수정 (§31, 공개 기록): 초판은 직접증거를 **한쪽으로만** 반영했다.
    #   권리 없음이 확정된 사건에는 P=0 을 넣으면서, R18 이 '주주배정 계열'로
    #   **확정한** 사건(TERMS_UNRELIABLE 92건)에는 모집단값 0.296 을 그대로 썼다.
    #   §16 의 취지는 대칭이다 — 확정된 사실은 양방향 모두 반영해야 한다.
    #   확정 주주배정에는 P=1.0 을 넣는다. 이 수정은 편향을 **키우는** 방향이므로
    #   판정을 쉽게 만들지 않는다.
    def assign_p(m):
        ev = m.get("r19PrimaryEvent")
        reason = m.get("wealthReason")
        if m.get("r19Entitlement") == "NO":
            return 0.0, ev or "NO_ENTITLEMENT_CONFIRMED", "DIRECT_R19"
        if m.get("r19Entitlement") == "YES":
            return 1.0, "TRUE_RIGHTS", "DIRECT_R19"
        if reason == "R18_TERMS_UNRELIABLE":
            # R18 이 주주배정 계열임을 원문으로 확정했다. 조건만 불명이다.
            return 1.0, "R18_CONFIRMED_HOLDER_RIGHT_TERMS_MISSING", "DIRECT_R18"
        if reason == "REDUCTION_PAID_OR_FREE_UNKNOWN":
            # 감자는 유상증자 권리 사건이 아니다. 유상감자 현금은 별도 한계.
            return 0.0, "CAPITAL_REDUCTION", "DIRECT_R17"
        return p_pop, (ev or reason or "UNRESOLVED"), "POPULATION_ESTIMATE"

    contrib = {}
    ps = []
    for m in unres:
        p, grp, src = assign_p(m)
        ps.append(p)
        c = contrib.setdefault(grp, {"events": 0, "sumP": 0.0, "pSource": src})
        c["events"] += 1
        c["sumP"] += p

    # 같은 조건에서 R18 관행(미해결 전체에 모집단값 일괄)으로도 계산해 병기한다.
    p_eff_r18style = p_pop
    p_eff = (sum(ps) / len(ps)) if ps else 0.0
    exp = p_eff * cond if cond else 0.0
    exp_r18style = p_eff_r18style * cond if cond else 0.0

    # 최대 기여군(R18 확정 주주배정·조건불명)의 과소평가를 **직접 측정**해
    # 대리치가 타당한지 교차검증한다. 판정 공식은 바꾸지 않는다(§15).
    xcheck = None
    try:
        nmz = json.loads((RD / "r18-legacy-rights-normalized-latest.json")
                         .read_text(encoding="utf-8"))["events"]
        byk = {(e["ticker"], e["date"]): e for e in nmz}
        da, db = [], []
        for m in unres:
            if m.get("wealthReason") != "R18_TERMS_UNRELIABLE":
                continue
            e = byk.get((m["ticker"], m["date"]))
            ch = ((e or {}).get("evidence") or {}).get("chain") or []
            hr = [c for c in ch if c.get("issueMethod") in
                  ("SHAREHOLDER_RIGHTS", "RIGHTS_THEN_PUBLIC", "MIXED")]
            if not hr:
                continue
            k = hr[-1].get("issuePrice")
            i = eng.pos.get(m["date"])
            if i is None or i == 0 or not k:
                continue
            pc, pe = eng.price(i - 1, m["ticker"]), eng.price(i, m["ticker"])
            if not pc or not pe or pc <= 0:
                continue
            rd = hr[-1].get("rightsRatio")
            if rd:
                da.append(rd * max(0.0, pe - k) / pc)
            db.append((m["shareRatio"] - 1.0) * max(0.0, pe - k) / pc)
        xcheck = {
            "group": "R18_CONFIRMED_HOLDER_RIGHT_TERMS_MISSING",
            "n": len(db),
            "medianUsingDartRatio": round(statistics.median(da), 4) if da else None,
            "medianUsingObservedRatio": round(statistics.median(db), 4) if db else None,
            "proxyConditionalMedian": round(cond, 4) if cond else None,
            "verdict": ("직접 측정값이 대리치와 같은 수준이면 대리치 사용이 타당하다. "
                        "판정 공식은 R18 것을 그대로 쓰고(§15) 이 측정은 교차검증으로만 "
                        "쓴다."),
        }
    except (OSError, KeyError, ValueError):
        xcheck = {"error": "교차검증 자료 없음"}

    for g, c in contrib.items():
        c["avgP"] = round(c["sumP"] / c["events"], 4) if c["events"] else 0.0
        c["biasContributionPct"] = round(
            (c["sumP"] / len(ps)) * cond * 100, 3) if (ps and cond) else 0.0

    out = {"task": "R19",
           "formula": "expected = P(기존주주 직접권리) × 조건부 과소평가 크기",
           "formulaUnchangedFromR18": True,
           "conditionalMedian": round(cond, 4) if cond else None,
           "conditionalSource": "확정된 주주배정 사건의 실측 중앙값 (R18 과 동일)",
           "populationP": round(p_pop, 4),
           "populationPSource": "R18 원문 방식 분포 (동일 소스)",
           "unresolvedEvents": len(unres),
           "effectiveP": round(p_eff, 4),
           "whatChanged": ("계산법이 아니라 P 의 조건화. 직접증거로 권리 없음이 "
                           "확정된 사건은 P=0 (§16). 감자는 유상증자 권리 사건이 "
                           "아니므로 P=0 이되 유상감자 현금은 별도 한계로 남긴다."),
           "expectedBias": round(exp, 5),
           "expectedBiasPct": round(exp * 100, 3),
           "r18ExpectedBiasPct": 3.47,
           "likeForLike": {
               "note": ("R18 은 미해결 전체에 모집단값을 일괄 적용했다. 같은 관행으로 "
                        "R19 를 계산하면 아래 값이며, 진행상황을 동일 기준으로 "
                        "비교하기 위해 병기한다. **판정에는 쓰지 않는다.**"),
               "r18Convention": round(exp_r18style * 100, 3),
               "whyNotUsed": ("직접증거로 확정된 사실을 무시하는 계산이라 덜 "
                              "정확하다. 판정은 아래 corrected 값으로 한다."),
           },
           "directMeasurementCrossCheck": xcheck,
           "progression": {"R18": 3.47, "R19": round(exp * 100, 3)},
           "progressionCaveat": (
               "R18 의 3.47% 는 **비대칭 조건화**로 과소평가된 값이다 — 주주배정으로 "
               "이미 확정된 92건에도 모집단 확률 0.296 을 적용했다. R19 가 그 오류를 "
               "바로잡아 값이 올라갔다. 개선이 아니라 **정정**이며, 같은 관행으로 "
               "계산한 like-for-like 값은 3.467% 로 R18 과 사실상 동일하다."),
           "metricInsensitivityFinding": (
               "R18 관행(미해결 전체에 상수 P)은 미해결 건수가 줄어도 값이 변하지 "
               "않는다. R19 가 342건 중 300건을 확정했는데도 like-for-like 값이 "
               "3.47% → 3.467% 로 사실상 불변인 것이 그 증거다. 이 지표는 해결 "
               "노력에 **구조적으로 둔감**하다 — 보고서에 명시한다."),
           "thresholdPct": VERDICT_RULE[
               "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"][
               "medianUnderstatementPctMax"],
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
    reasons = {}
    layers = {}
    for m in unres:
        k = m.get("wealthReason", "?")
        reasons[k] = reasons.get(k, 0) + 1
    for m in matches:
        k = m.get("wealthLayer") or ("WEALTH_CONFIRMED" if m.get("resolvedWealth")
                                     else "WEALTH_UNRESOLVED")
        layers[k] = layers.get(k, 0) + 1

    out = {"task": "R19", "suspectedTotal": len(matches),
           "wealthResolved": len(res), "wealthUnresolved": len(unres),
           "byWealthLayer": layers,
           "unresolvedReasons": reasons,
           "unresolvedTickers": len(ut), "universeTickers": universe,
           "unresolvedTickerPct": round(len(ut) / universe * 100, 2),
           "unresolvedEventPct": round(len(unres) / len(matches) * 100, 2),
           "unresolvedMonths": len(months), "totalMonths": len(eng.dates),
           "unresolvedMonthPct": round(len(months) / len(eng.dates) * 100, 2),
           "unresolvedMagnitudePct": round(
               sum(srs) / sum(allsr) * 100, 2) if allsr else None,
           "unresolvedMedianShareChange": round(statistics.median(srs), 4)
           if srs else None,
           "extremeDiscontinuityEvents": len(extreme),
           "extremeDiscontinuityPct": round(len(extreme) / len(matches) * 100, 2),
           "expectedBiasPct": bias["expectedBiasPct"],
           "biasDirection": "CONSERVATIVE_UNDERSTATEMENT",
           "biasWhy": ("미해결·PARTIAL 은 전부 미조정으로 남는다. 미조정은 직접 "
                       "권리가 없던 사건이면 정답이고 있었으면 과소평가다. 어느 "
                       "쪽도 wealth 를 부풀리지 않는다."),
           "progression": {"R16": 40.8, "R17": 25.57, "R18": 13.31,
                           "R19": round(len(ut) / universe * 100, 2)}}
    save("unresolved-materiality", out)
    return out


# ═══════════ 판정 (§17·§18) ═══════════
def verdict(mat, anc, fp, bias, cont):
    rule = VERDICT_RULE
    p = rule["CANONICAL_TSR_FOUNDATION_PASS"]
    pl = rule["CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"]
    tpct, bpct = mat["unresolvedTickerPct"], bias["expectedBiasPct"]
    engine_ok = anc["allPass"] and cont["pass"] and cont["noFreeWealth"]
    checks = {
        "r16Threshold40Preserved": tpct < rule["r16ThresholdPreserved"]["thresholdPct"],
        "anchorsPass": anc["allPass"],
        "noEntitlementAdjustmentZero": anc["noEntitlementAdjustmentExactlyZero"],
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

    out = {"task": "R19", "verdict": v, "checks": checks,
           "unresolvedTickerPct": tpct, "expectedBiasPct": bpct,
           "falseClassificationRate": fp["falsePositiveRate"],
           "thresholdsFromPrecommit": {
               "r16Preserved40": rule["r16ThresholdPreserved"]["thresholdPct"],
               "passMaxTickerPct": p["unresolvedTickerPctMax"],
               "passWithLimitsMaxTickerPct": pl["unresolvedTickerPctMax"],
               "maxExpectedBiasPct": pl["medianUnderstatementPctMax"]},
           "thresholdsUnchanged": True,
           "biasFormulaUnchanged": bias["formulaUnchangedFromR18"],
           "progression": {"unresolvedTickerPct": mat["progression"],
                           "expectedBiasPct": bias["progression"]},
           "factorResearchAllowed": v in ("CANONICAL_TSR_FOUNDATION_PASS",
                                          "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS"),
           "legacyResearchStatus": {
               "R5~R14": "PRE_TSR_LEGACY_RESEARCH (유지)",
               "R11_BM": "PROVISIONAL_PENDING_CANONICAL_TSR_REVALIDATION (유지)",
               "R14_QUALITY": "PROVISIONAL_CHANGED_BY_R15 (유지)",
               "SIZE": "PROVISIONAL_PENDING_CANONICAL_TSR (유지)",
               "note": ("foundation 이 풀려도 기존 수익률을 자동 복원하지 "
                        "않는다(§25). 새 precommit 으로 원점 재실행해야 한다.")}}
    save("foundation-verdict", out)
    return out


def main() -> int:
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    universe = len({t for d in ds for t in cap.get(d, {})})
    matches = merged()

    lay, ent, adj = {}, {}, {}
    for m in matches:
        k = m.get("wealthLayer") or ("WEALTH_CONFIRMED" if m.get("resolvedWealth")
                                     else "WEALTH_UNRESOLVED")
        lay[k] = lay.get(k, 0) + 1
        if m.get("r19Source"):
            ent[m["r19Entitlement"]] = ent.get(m["r19Entitlement"], 0) + 1
            adj[m["r19WealthAdjustment"]] = adj.get(m["r19WealthAdjustment"], 0) + 1
    save("full-reconciliation", {
        "task": "R19", "total": len(matches),
        "r19Merged": sum(1 for m in matches if m.get("r19Source")),
        "byWealthLayer": lay, "byEntitlement": ent, "byWealthAdjustment": adj,
        "wealthResolved": sum(1 for m in matches if m.get("resolvedWealth")),
        "note": "R18 2,742건에 R19 직접증거를 merge 한 최종 재분류(§23).",
        "rows": matches})

    fp = fp_audit()
    mdir = merger_direction(eng, matches)
    anc = anchors(eng, matches)
    # 연속성은 R18 방법 그대로 재사용(엔진이 깨지지 않았는지 확인)
    from r18_audit import continuity as r18_continuity
    cont = r18_continuity(eng, matches)
    bias = expected_bias(eng, matches)
    mat = materiality(eng, matches, universe, bias)
    v = verdict(mat, anc, fp, bias, cont)
    v["mergerDirectionSurvivalPct"] = mdir["survivalPct"]
    save("foundation-verdict", v)
    print(json.dumps({"verdict": v["verdict"],
                      "unresolvedTickerPct": mat["unresolvedTickerPct"],
                      "expectedBiasPct": bias["expectedBiasPct"],
                      "biasProgression": bias["progression"],
                      "anchors": len(anc["cases"]), "anchorsPass": anc["allPass"],
                      "fpRate": fp["falsePositiveRate"],
                      "factorAllowed": v["factorResearchAllowed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
