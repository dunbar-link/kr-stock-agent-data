#!/usr/bin/env python3
"""R20 최종조건 확정 — PLANNED vs FINAL_ACTUAL 분리 + 주식수 대조.

WABABA-HOLDER-RIGHTS-FINAL-TERMS-RECOVERY-R20

§5 대로 planned 와 final_actual 을 **다른 필드**에 저장한다.
§8 대로 계획치가 아니라 실제 발행분만 쓴다.
§9 대로 관측 주식수 변화를 rights 신주로 강제하지 않는다.

★ 권리 기준 (§10 · POLICY A 정합):
  canonical 질문은 "1주를 들고 있던 주주가 얼마를 벌었나" 다. 그 주주의 권리는
  **배정받은 주식수(FST_DV_CNT)** 이지, 다른 주주들이 실제로 얼마나 청약했는지가
  아니다. POLICY A(합리적이면 청약)는 R17 에서 고정됐고 §13 이 변경을 금지한다.
  따라서 wealth 계산의 비율은 entitlement 기준을 쓰고, 실제 청약률(take-up)은
  함께 기록해 투명하게 남긴다. 둘이 다르면 그 사실을 flag 로 표시한다.

안전: 네트워크 0 · 캐시/보고서만 읽고 쓴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r20_parser as P  # noqa: E402
from r20_targets import CONFIDENCE, SHARE_RECONCILIATION  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
DOC_CACHE = ROOT / "_cache" / "dart-documents"
LIST_CACHE = ROOT / "_cache" / "dart-r20-lists"
TOL = SHARE_RECONCILIATION["tolerance"]


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def load_lists():
    out = {}
    if not LIST_CACHE.exists():
        return out
    for p in sorted(LIST_CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        out[d["ticker"]] = d["filings"]
    return out


def other_events_same_month(ticker, date):
    """§9 — 같은 달 다른 자본행위를 R19 직접증거로 분리한다."""
    p = RD / "r19-event-classification-latest.json"
    if not p.exists():
        return []
    ev = json.loads(p.read_text(encoding="utf-8"))["events"]
    return [e["primaryEvent"] for e in ev
            if e["ticker"] == ticker and e["date"] == date
            and e["primaryEvent"] != "UNRESOLVED"]


def resolve_one(t, filings, docs):
    """대상 1건 → 최종조건. planned 와 final 을 분리해 담는다."""
    em = _mon(t["date"])
    sb0 = t.get("observedSharesBefore")
    sa0 = t.get("observedSharesAfter")
    obs_delta = (sa0 - sb0) if (sb0 and sa0) else None

    # ★ 자체수정 2 (§30): 초판은 창을 -1~+9 로 두고 **날짜 근접**으로 골랐다.
    #   실측 결과 실적보고서는 사건월보다 **앞서는** 경우가 많고(-5~-1 에 다수),
    #   한 종목에 여러 건의 증자가 있어 날짜만으로는 엉뚱한 회차를 집는다.
    #   §9·§11 이 요구하는 대로 **수량 대조**를 1순위 기준으로 바꾼다:
    #   보고된 총 발행주식수가 관측 주식수 증가분과 가장 가까운 회차를 고른다.
    cands, no_sh = [], []
    for f in filings:
        if f["kind"] != "ISSUE_RESULT_REPORT":
            continue
        fd = f.get("rcept_dt")
        if not fd:
            continue
        gap = _mon(fd) - em
        if not (-6 <= gap <= 9):
            continue
        d = docs.get(f["rcept_no"])
        if not d:
            continue
        tot = d.get("finalNewSharesIssued")
        rel = (abs(tot - obs_delta) / obs_delta
               if (tot and obs_delta and obs_delta > 0) else None)
        if d.get("hasShareholderGroup"):
            cands.append((gap, f, d, rel))
        else:
            no_sh.append((gap, f, d, rel))
    # 수량이 맞는 것 먼저, 그다음 수량오차, 마지막에 날짜 근접
    cands.sort(key=lambda c: (0 if (c[3] is not None and c[3] <= TOL) else 1,
                              c[3] if c[3] is not None else 9.0,
                              abs(c[0])))

    r = {**t, "finalSource": None, "flags": [],
         "otherEventsSameMonth": other_events_same_month(t["ticker"], t["date"]),
         "resultReportCandidates": len(cands)}

    picked = cands[0] if cands else None
    if picked is None:
        # ★ 자체수정 3 (§30): 구주주 배정이 **없는** 실적보고서가 관측 주식수
        #   증가분과 수량으로 일치하면, 그 증자는 실제로 제3자배정·일반공모로
        #   끝난 것이다. R18 은 '유상증자결정'의 CI_MTH(계획)로 주주배정이라고
        #   봤지만, §3·§8 은 계획보다 **사후 실제 결과**를 우선하라고 한다.
        #   이 경우 기존 주주 직접 권리는 0 이고 미조정이 정답이다.
        m = [c for c in no_sh if c[3] is not None and c[3] <= TOL]
        m.sort(key=lambda c: (c[3], abs(c[0])))
        if m:
            gap, f, d, rel = m[0]
            r.update({
                "eventStatus": "COMPLETED_WITHOUT_SHAREHOLDER_ALLOCATION",
                "wealthStatus": "WEALTH_CONFIRMED",
                "wealthReasonR20": "R20_NO_SHAREHOLDER_ALLOCATION_IN_FINAL",
                "holderRightFinal": False,
                "confidence": "HIGH",
                "finalSource": {"rcept_no": f["rcept_no"],
                                "filingDate": f["rcept_dt"],
                                "reportName": f["report_nm"], "gapMonths": gap,
                                "shareMatchRelErr": rel,
                                "selectionRule": "수량 일치 + 구주주 배정 없음"},
                "finalNewSharesIssued": d.get("finalNewSharesIssued"),
                "finalThirdPartyShares": d.get("thirdPartyAllocatedShares"),
                "finalPublicOfferingShares": d.get("publicAllocatedShares"),
                "finalEsopShares": d.get("esopAllocatedShares"),
                "finalIssuePrice": d.get("finalIssuePrice"),
                "entitlementRightsRatio": 0.0,
                "reason": ("실적보고서에 구주주 배정이 없고 발행수량이 관측 "
                           "주식수 증가와 일치한다 → 실제로는 제3자배정·일반공모로 "
                           "발행됐다. 기존 주주 직접 권리 0, 미조정이 정답."),
                "plannedVsFinalMethodChanged": True})
            r["flags"].append("PLANNED_SHAREHOLDER_BUT_FINAL_NOT")
            return r
        r.update({"eventStatus": "NO_RESULT_REPORT",
                  "wealthStatus": "WEALTH_PARTIAL",
                  "confidence": "LOW",
                  "holderRightFinal": False,
                  "resultReportsWithoutShareholderGroup": len(no_sh),
                  "reason": ("창 안에서 구주주 배정이 있는 증권발행실적보고서를 "
                             "찾지 못했고, 구주주 없는 보고서도 수량이 맞지 "
                             "않는다. 계획치로 승격하지 않는다(§11).")})
        return r

    gap, f, d, pick_rel = picked
    ent = d.get("shareholderEntitledShares")
    alloc = d.get("shareholderAllocatedShares")
    price = d.get("finalIssuePrice")
    sb = t.get("observedSharesBefore")

    r.update({
        "finalSource": {"rcept_no": f["rcept_no"], "filingDate": f["rcept_dt"],
                        "reportName": f["report_nm"], "gapMonths": gap,
                        "shareMatchRelErr": pick_rel,
                        "candidatesInWindow": len(cands),
                        "selectionRule": ("수량 대조 1순위 → 수량오차 → 날짜 근접. "
                                          "날짜만으로 고르지 않는다(§11).")},
        # ── FINAL_ACTUAL 레이어 (§5) ──
        "finalNewSharesIssued": d.get("finalNewSharesIssued"),
        "finalShareholderEntitledShares": ent,
        "finalShareholderSubscribedShares": d.get("shareholderSubscribedShares"),
        "finalShareholderAllocatedShares": alloc,
        "finalUnsubscribedShares": (
            (ent - alloc) if (ent is not None and alloc is not None) else None),
        "finalThirdPartyShares": d.get("thirdPartyAllocatedShares"),
        "finalPublicOfferingShares": d.get("publicAllocatedShares"),
        "finalEsopShares": d.get("esopAllocatedShares"),
        "finalIssuePrice": price,
        "finalIssuePriceSource": d.get("finalIssuePriceSource"),
        "actualSubscriptionStart": d.get("subscriptionStart"),
        "actualSubscriptionEnd": d.get("subscriptionEnd"),
        "actualPaymentDate": d.get("paymentDate"),
        "shareholderTakeUpRate": d.get("shareholderTakeUpRate"),
        "parserFlags": d.get("flags", []),
    })

    # ── 비율 두 가지를 모두 남긴다 (§10) ──
    r["entitlementRightsRatio"] = (ent / sb) if (ent and sb) else None
    r["takeUpRightsRatio"] = (alloc / sb) if (alloc and sb) else None
    r["ratioBasisUsed"] = "ENTITLEMENT"
    r["ratioBasisWhy"] = (
        "canonical 은 1주 보유자 관점이다. 그 주주의 권리는 배정주식수이지 "
        "다른 주주들의 청약률이 아니다. POLICY A(R17 정본) 와 정합한다.")
    if (r["entitlementRightsRatio"] and r["takeUpRightsRatio"]
            and abs(r["entitlementRightsRatio"] - r["takeUpRightsRatio"])
            / r["entitlementRightsRatio"] > TOL):
        r["flags"].append("TAKEUP_BELOW_ENTITLEMENT")

    # ── 계획치 대비 (§5·§8) ──
    pr = t.get("plannedRightsRatio")
    if pr and r["entitlementRightsRatio"]:
        r["plannedVsFinalRatioRelErr"] = abs(
            r["entitlementRightsRatio"] - pr) / pr
        if r["plannedVsFinalRatioRelErr"] > TOL:
            r["flags"].append("PLANNED_RATIO_DIFFERS")
    pp = t.get("plannedIssuePrice")
    if pp and price:
        r["plannedVsFinalPriceRelErr"] = abs(price - pp) / pp
        if r["plannedVsFinalPriceRelErr"] > TOL:
            r["flags"].append("PLANNED_PRICE_DIFFERS")

    # ── 주식수 대조 (§9) — 강제하지 않는다 ──
    sa = t.get("observedSharesAfter")
    if sb and sa:
        obs_delta = sa - sb
        r["observedShareDelta"] = obs_delta
        tot = d.get("finalNewSharesIssued")
        if tot:
            r["reportedVsObservedRelErr"] = abs(tot - obs_delta) / obs_delta \
                if obs_delta else None
            if (r["reportedVsObservedRelErr"] is not None
                    and r["reportedVsObservedRelErr"] > TOL):
                # 다른 자본행위가 같은 달에 있으면 그것이 차이를 설명한다.
                if r["otherEventsSameMonth"]:
                    r["flags"].append("DELTA_EXPLAINED_BY_OTHER_EVENT")
                else:
                    r["flags"].append("SHARE_COUNT_DATA_CONFLICT")

    # ── 완료 상태 (§8) ──
    if not r["finalNewSharesIssued"]:
        r["eventStatus"] = "CANCELLED_OR_UNKNOWN"
    elif "PARTIAL_SUBSCRIPTION" in (d.get("flags") or []):
        r["eventStatus"] = "PARTIALLY_COMPLETED"
    else:
        r["eventStatus"] = "COMPLETED"

    # ── confidence (§17) ──
    have_core = bool(ent and price and sb)
    conflict = "SHARE_COUNT_DATA_CONFLICT" in r["flags"]
    if have_core and not conflict:
        r["confidence"] = "HIGH"
    elif have_core:
        r["confidence"] = "MEDIUM"
    else:
        r["confidence"] = "LOW"

    # ── wealth 확정 (§16) ──
    if r["eventStatus"] == "CANCELLED_OR_UNKNOWN":
        r["wealthStatus"] = "WEALTH_CONFIRMED"
        r["wealthReasonR20"] = "R20_CANCELLED_NO_ADJUSTMENT"
        r["holderRightFinal"] = False
    elif have_core and r["confidence"] in ("HIGH", "MEDIUM"):
        r["wealthStatus"] = "WEALTH_CONFIRMED"
        r["wealthReasonR20"] = "R20_FINAL_TERMS_RECOVERED"
        r["holderRightFinal"] = True
    else:
        r["wealthStatus"] = "WEALTH_PARTIAL"
        r["wealthReasonR20"] = "R20_FINAL_TERMS_INCOMPLETE"
        r["holderRightFinal"] = False
    return r


def correction_chains(tg, lists):
    """§7 — 최초 → 정정 → 최종. 실적보고서까지 포함한 chain."""
    out = []
    for t in tg:
        em = _mon(t["date"])
        rows = [f for f in lists.get(t["ticker"], [])
                if f.get("rcept_dt") and -6 <= _mon(f["rcept_dt"]) - em <= 9]
        rows.sort(key=lambda x: x["rcept_dt"])
        if len(rows) < 2:
            continue
        out.append({
            "ticker": t["ticker"], "date": t["date"], "filings": len(rows),
            "sequence": [{"rcept_no": f["rcept_no"], "date": f["rcept_dt"],
                          "kind": f["kind"], "name": f["report_nm"]}
                         for f in rows],
            "hasCorrection": any("정정" in f["report_nm"] for f in rows),
            "hasResultReport": any(f["kind"] == "ISSUE_RESULT_REPORT"
                                   for f in rows),
        })
    return out


def main() -> int:
    pc = json.loads((RD / "r20-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))
    tg = pc["targets"]
    lists = load_lists()

    # 실적보고서 원문 파싱
    meta = json.loads((RD / "r20-post-issuance-filings-latest.json")
                      .read_text(encoding="utf-8"))["documents"]
    docs = {}
    for m in meta:
        if m.get("kind") != "ISSUE_RESULT_REPORT" or m.get("status") != "OK":
            continue
        f = DOC_CACHE / f"{m['rcept_no']}.xml"
        if not f.exists():
            continue
        try:
            docs[m["rcept_no"]] = P.parse(f.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            docs[m["rcept_no"]] = {"flags": ["PARSE_ERROR"],
                                   "error": type(e).__name__}

    rows = [resolve_one(t, lists.get(t["ticker"], []), docs) for t in tg]

    def cnt(key):
        d = {}
        for r in rows:
            d[r.get(key)] = d.get(r.get(key), 0) + 1
        return d

    flags = {}
    for r in rows:
        for f in r.get("flags", []) + r.get("parserFlags", []):
            flags[f] = flags.get(f, 0) + 1

    recov = {}
    for k in ("finalNewSharesIssued", "finalIssuePrice",
              "finalShareholderEntitledShares", "finalShareholderAllocatedShares",
              "entitlementRightsRatio", "actualPaymentDate",
              "actualSubscriptionStart", "finalThirdPartyShares"):
        n = sum(1 for r in rows if r.get(k) is not None)
        recov[k] = {"n": n, "pct": round(n / len(rows) * 100, 1)}

    (RD / "r20-final-terms-normalized-latest.json").write_text(json.dumps({
        "task": "R20", "parserVersion": P.PARSER_VERSION,
        "targets": len(rows),
        "byEventStatus": cnt("eventStatus"),
        "byWealthStatus": cnt("wealthStatus"),
        "byConfidence": cnt("confidence"),
        "flags": flags, "fieldRecovery": recov,
        "ratioBasis": {
            "used": "ENTITLEMENT",
            "why": rows[0].get("ratioBasisWhy") if rows else None,
            "alsoRecorded": "takeUpRightsRatio (실제 청약 기준)",
        },
        "layerSeparation": pc["layerRule"]["mustSeparate"],
        "events": rows,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    ch = correction_chains(tg, lists)
    (RD / "r20-correction-chains-latest.json").write_text(json.dumps({
        "task": "R20", "chains": len(ch),
        "withCorrection": sum(1 for c in ch if c["hasCorrection"]),
        "withResultReport": sum(1 for c in ch if c["hasResultReport"]),
        "policy": ("최초 공시 → 정정 → 실적보고서 순서를 보존한다. 회계에는 "
                   "실적보고서의 실제 발행 결과를 쓰고, 이전 조건은 provenance "
                   "로만 남긴다(§7)."),
        "list": ch,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    # 주식수 대조 산출물 (§9)
    rec = [{"ticker": r["ticker"], "date": r["date"],
            "observedSharesBefore": r.get("observedSharesBefore"),
            "observedSharesAfter": r.get("observedSharesAfter"),
            "observedShareDelta": r.get("observedShareDelta"),
            "reportedNewShares": r.get("finalNewSharesIssued"),
            "relErr": r.get("reportedVsObservedRelErr"),
            "otherEventsSameMonth": r.get("otherEventsSameMonth"),
            "flags": r.get("flags")} for r in rows]
    matched = sum(1 for x in rec
                  if x["relErr"] is not None and x["relErr"] <= TOL)
    (RD / "r20-share-count-reconciliation-latest.json").write_text(json.dumps({
        "task": "R20", "tolerance": TOL,
        "hardRule": SHARE_RECONCILIATION["hardRule"],
        "method": SHARE_RECONCILIATION["method"],
        "withReported": sum(1 for x in rec if x["reportedNewShares"]),
        "matchedWithinTolerance": matched,
        "explainedByOtherEvent": sum(
            1 for x in rec if "DELTA_EXPLAINED_BY_OTHER_EVENT" in (x["flags"] or [])),
        "unexplainedConflict": sum(
            1 for x in rec if "SHARE_COUNT_DATA_CONFLICT" in (x["flags"] or [])),
        "rows": rec,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    print(json.dumps({"targets": len(rows),
                      "byEventStatus": cnt("eventStatus"),
                      "byWealthStatus": cnt("wealthStatus"),
                      "byConfidence": cnt("confidence"),
                      "flags": flags,
                      "chains": len(ch),
                      "shareMatched": matched}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
