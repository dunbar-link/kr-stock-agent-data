#!/usr/bin/env python3
"""R22 최종조건 확정 — 다중 사후공시 결합.

WABABA-FINAL-27-HOLDER-RIGHTS-TERMS-RECOVERY-R22

§3 우선순위대로 여러 사후공시를 결합한다. 한 종류가 없으면 다음 종류로 우회한다:
  1 증권발행실적보고서   (배정대상별 실청약·실배정·확정가)
  2 증권발행결과 자율공시 (실제 발행주식수·발행방법·납입일)
  3 청약결과 자율공시     (모집·청약·실권 주식수)
  4 발행가액확정 공시     (확정발행가)
  5 추가상장 공시         (실제 상장 신주수)

§6 PLANNED 와 FINAL_ACTUAL 을 별도 필드에 담는다.
§8 계획 배정량을 실제 realized entitlement 로 그대로 쓰지 않는다.
§11 관측 주식수 delta 를 rights 하나에 전부 귀속하지 않는다.
§13 취소·부분발행은 실제 완료분만 반영한다.

안전: 네트워크 0 · 캐시/보고서만 읽고 쓴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r20_parser as P20  # noqa: E402  실적보고서 파서 재사용
import r22_parser as P22  # noqa: E402
from r22_targets import CONFIDENCE, WINDOW  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
DOC = ROOT / "_cache" / "dart-documents"
LIST = ROOT / "_cache" / "dart-r22-lists"
TOL = CONFIDENCE["ratioTolerance"]
LO, HI = WINDOW["months"]


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def load_lists():
    out = {}
    if not LIST.exists():
        return out
    for p in sorted(LIST.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        out[d["ticker"]] = d["filings"]
    return out


def same_month_others(ticker, date):
    """§11 — 같은 달 다른 자본행위(R19 직접증거)."""
    p = RD / "r19-event-classification-latest.json"
    if not p.exists():
        return []
    ev = json.loads(p.read_text(encoding="utf-8"))["events"]
    return sorted({e["primaryEvent"] for e in ev
                   if e["ticker"] == ticker and e["date"] == date
                   and e["primaryEvent"] != "UNRESOLVED"})


def _read(rn):
    f = DOC / f"{rn}.xml"
    return f.read_text(encoding="utf-8") if f.exists() else None


def gather(t, filings):
    """대상 1건의 창 안 사후공시를 종류별로 파싱해 모은다."""
    em = _mon(t["date"])
    got = {"ISSUE_RESULT_REPORT": [], "ISSUE_COMPLETION": [],
           "SUBSCRIPTION_RESULT": [], "FINAL_PRICE": [], "NEW_LISTING": []}
    for f in filings:
        k = f.get("kind")
        if k not in got:
            continue
        fd = f.get("rcept_dt")
        if not fd:
            continue
        gap = _mon(fd) - em
        if not (LO <= gap <= HI):
            continue
        txt = _read(f["rcept_no"])
        if txt is None:
            continue
        d = (P20.parse(txt) if k == "ISSUE_RESULT_REPORT"
             else P22.parse(k, txt))
        if d is None:
            continue
        got[k].append({"filing": f, "gap": gap, "data": d})
    return got


def _pick_by_quantity(cands, key, obs_delta):
    """수량이 관측 delta 와 가장 가까운 것을 고른다(§11 · 날짜만으로 고르지 않음)."""
    scored = []
    for c in cands:
        v = c["data"].get(key)
        rel = (abs(v - obs_delta) / obs_delta
               if (v and obs_delta and obs_delta > 0) else None)
        scored.append((0 if (rel is not None and rel <= TOL) else 1,
                       rel if rel is not None else 9.0, abs(c["gap"]), c, rel))
    if not scored:
        return None, None
    scored.sort(key=lambda x: (x[0], x[1], x[2]))
    return scored[0][3], scored[0][4]


def resolve_one(t, filings):
    obs = t.get("observedShareDelta")
    sb = t.get("observedSharesBefore")
    got = gather(t, filings)
    r = {**t, "flags": [], "sources": {},
         "otherEventsSameMonth": same_month_others(t["ticker"], t["date"]),
         "filingsInWindow": {k: len(v) for k, v in got.items()}}

    final_shares = alloc = entitled = price = None
    ratio_src = price_src = shares_src = None
    method = None
    completion = None

    # ── 1순위: 증권발행실적보고서 ───────────────────────────────
    sh_rep = [c for c in got["ISSUE_RESULT_REPORT"]
              if c["data"].get("hasShareholderGroup")]
    pick, rel = _pick_by_quantity(sh_rep, "finalNewSharesIssued", obs)
    if pick:
        d = pick["data"]
        entitled = d.get("shareholderEntitledShares")
        alloc = d.get("shareholderAllocatedShares")
        final_shares = d.get("finalNewSharesIssued")
        price, price_src = d.get("finalIssuePrice"), "ISSUE_RESULT_REPORT"
        shares_src = ratio_src = "ISSUE_RESULT_REPORT"
        method = "SHAREHOLDER_ALLOCATION"
        r["sources"]["issueResultReport"] = {
            "rcept_no": pick["filing"]["rcept_no"],
            "date": pick["filing"]["rcept_dt"], "gapMonths": pick["gap"],
            "shareMatchRelErr": rel}
        r["finalSubscribedShares"] = d.get("shareholderSubscribedShares")
        r["finalThirdPartyShares"] = d.get("thirdPartyAllocatedShares")
        r["finalPublicOfferingShares"] = d.get("publicAllocatedShares")
        r["actualPaymentDate"] = d.get("paymentDate")
        r["actualSubscriptionStart"] = d.get("subscriptionStart")
        r["actualSubscriptionEnd"] = d.get("subscriptionEnd")
        if "PARTIAL_SUBSCRIPTION" in (d.get("flags") or []):
            r["flags"].append("PARTIAL_SUBSCRIPTION")

    # ── 2순위: 증권발행결과 자율공시 ────────────────────────────
    if final_shares is None:
        eq = [c for c in got["ISSUE_COMPLETION"]
              if not c["data"].get("isBondNotEquity")
              and c["data"].get("isRights")]
        pick, rel = _pick_by_quantity(eq, "issuedShares", obs)
        if pick:
            d = pick["data"]
            final_shares, shares_src = d.get("issuedShares"), "ISSUE_COMPLETION"
            if d.get("derivedIssuePrice") and price is None:
                price, price_src = d["derivedIssuePrice"], "ISSUE_COMPLETION_DERIVED"
            method = ("SHAREHOLDER_ALLOCATION"
                      if d.get("isShareholderAllocation")
                      else ("THIRD_PARTY" if d.get("isThirdParty") else None))
            # ★ 자체수정 3 (§31): 발행방법이 '주주배정 유상증자' 면 발행예정주식수가
            #   곧 **구주주 배정량**(entitlement)이고 실제발행주식수가 배정 결과다.
            #   R20 의 ENTITLEMENT 기준(§12)과 같은 의미이며, 계획치를 realized 로
            #   쓰는 것이 아니라 '배정량'과 '발행량'을 각각 제자리에 넣는 것이다.
            if method == "SHAREHOLDER_ALLOCATION":
                if entitled is None and d.get("plannedShares"):
                    entitled, ratio_src = d["plannedShares"], "ISSUE_COMPLETION"
                if alloc is None and d.get("issuedShares") is not None:
                    alloc = d["issuedShares"]
            r["actualPaymentDate"] = r.get("actualPaymentDate") or d.get("paymentDate")
            r["sources"]["issueCompletion"] = {
                "rcept_no": pick["filing"]["rcept_no"],
                "date": pick["filing"]["rcept_dt"], "gapMonths": pick["gap"],
                "shareMatchRelErr": rel, "issueMethodRaw": d.get("issueMethod")}
            if "PARTIAL_ISSUANCE" in (d.get("flags") or []):
                r["flags"].append("PARTIAL_ISSUANCE")
            r["plannedSharesFromCompletion"] = d.get("plannedShares")

    # ── 3순위: 청약결과 자율공시 ────────────────────────────────
    subs = [c for c in got["SUBSCRIPTION_RESULT"]
            if c["data"].get("isShareholderAllocation")]
    if subs:
        subs.sort(key=lambda c: abs(c["gap"]))
        d = subs[0]["data"]
        if entitled is None and d.get("offeredShares"):
            entitled, ratio_src = d["offeredShares"], "SUBSCRIPTION_RESULT"
        if alloc is None and d.get("subscribedShares") is not None:
            alloc = d["subscribedShares"]
        r["sources"]["subscriptionResult"] = {
            "rcept_no": subs[0]["filing"]["rcept_no"],
            "date": subs[0]["filing"]["rcept_dt"], "gapMonths": subs[0]["gap"],
            "offeredShares": d.get("offeredShares"),
            "subscribedShares": d.get("subscribedShares"),
            "unsubscribedShares": d.get("unsubscribedShares"),
            "takeUpRate": d.get("takeUpRate")}
        if "PARTIAL_SUBSCRIPTION" in (d.get("flags") or []):
            if "PARTIAL_SUBSCRIPTION" not in r["flags"]:
                r["flags"].append("PARTIAL_SUBSCRIPTION")
        method = method or "SHAREHOLDER_ALLOCATION"

    # ── 4순위: 발행가액확정 ─────────────────────────────────────
    if price is None:
        fp = [c for c in got["FINAL_PRICE"]
              if c["data"].get("finalIssuePrice")]
        if fp:
            fp.sort(key=lambda c: abs(c["gap"]))
            price, price_src = fp[0]["data"]["finalIssuePrice"], "FINAL_PRICE"
            r["sources"]["finalPrice"] = {
                "rcept_no": fp[0]["filing"]["rcept_no"],
                "date": fp[0]["filing"]["rcept_dt"],
                "gapMonths": fp[0]["gap"],
                "stage": fp[0]["data"].get("stage")}
        else:
            notice = [c for c in got["FINAL_PRICE"]
                      if c["data"].get("noticePrice")]
            if notice:
                r["flags"].append("ONLY_NOTICE_PRICE_AVAILABLE")

    # ── 5순위: 추가상장 ─────────────────────────────────────────
    if final_shares is None:
        nl = [c for c in got["NEW_LISTING"] if c["data"].get("addedShares")]
        pick, rel = _pick_by_quantity(nl, "addedShares", obs)
        if pick:
            final_shares, shares_src = pick["data"]["addedShares"], "NEW_LISTING"
            r["sources"]["newListing"] = {
                "rcept_no": pick["filing"]["rcept_no"],
                "date": pick["filing"]["rcept_dt"], "gapMonths": pick["gap"],
                "shareMatchRelErr": rel,
                "listingDate": pick["data"].get("listingDate")}

    # ── 발행가 sanity (§7) ──────────────────────────────────────
    # 확정발행가가 권리락 주가의 0.1% 미만이면 파싱 오류로 본다. 추정으로
    # 채우지 않고 값을 버린다(§35 추정 금지).
    if price is not None and price < 10:
        r["flags"].append("IMPLAUSIBLE_PRICE_DISCARDED")
        r["discardedPrice"] = price
        price, price_src = None, None

    # ── FINAL_ACTUAL 레이어 (§6) ────────────────────────────────
    r.update({
        "finalIssueMethod": method,
        "finalActualNewShares": final_shares,
        "finalShareholderEntitledShares": entitled,
        "finalShareholderAllocatedShares": alloc,
        "finalUnsubscribedShares": (
            (entitled - alloc) if (entitled is not None and alloc is not None)
            else None),
        "finalIssuePrice": price,
        "sharesSource": shares_src, "priceSource": price_src,
        "ratioSource": ratio_src,
    })
    # 권리비율 — R20 정본대로 ENTITLEMENT 기준(§12)
    r["finalRightsRatio"] = (entitled / sb) if (entitled and sb) else None
    r["takeUpRightsRatio"] = (alloc / sb) if (alloc and sb) else None
    r["ratioBasisUsed"] = "ENTITLEMENT"
    if (r["finalRightsRatio"] and r["takeUpRightsRatio"]
            and abs(r["finalRightsRatio"] - r["takeUpRightsRatio"])
            / r["finalRightsRatio"] > TOL):
        r["flags"].append("TAKEUP_BELOW_ENTITLEMENT")

    # ── 계획 대비 (§6) ──────────────────────────────────────────
    pr, pp = t.get("plannedRightsRatio"), t.get("plannedIssuePrice")
    if pr and r["finalRightsRatio"]:
        r["plannedVsFinalRatioRelErr"] = abs(r["finalRightsRatio"] - pr) / pr
        if r["plannedVsFinalRatioRelErr"] > TOL:
            r["flags"].append("PLANNED_RATIO_DIFFERS")
    if pp and price:
        r["plannedVsFinalPriceRelErr"] = abs(price - pp) / pp
        if r["plannedVsFinalPriceRelErr"] > TOL:
            r["flags"].append("PLANNED_PRICE_DIFFERS")

    # ── 주식수 대조 (§11) ───────────────────────────────────────
    if final_shares and obs and obs > 0:
        r["reportedVsObservedRelErr"] = abs(final_shares - obs) / obs
        if r["reportedVsObservedRelErr"] > TOL:
            if r["otherEventsSameMonth"]:
                r["flags"].append("DELTA_EXPLAINED_BY_OTHER_EVENT")
            else:
                r["flags"].append("SHARE_COUNT_DATA_CONFLICT")

    # ── 완료 상태 (§13) ─────────────────────────────────────────
    if final_shares is None:
        completion = "NO_ACTUAL_EVIDENCE"
    elif final_shares == 0:
        completion = "CANCELLED_NO_WEALTH_EVENT"
    elif ("PARTIAL_SUBSCRIPTION" in r["flags"]
          or "PARTIAL_ISSUANCE" in r["flags"]):
        completion = "PARTIALLY_COMPLETED"
    else:
        completion = "COMPLETED"
    r["completionStatus"] = completion

    # ── confidence (§17) ────────────────────────────────────────
    core = bool(r["finalRightsRatio"] and price and sb)
    conflict = "SHARE_COUNT_DATA_CONFLICT" in r["flags"]
    direct_qty = shares_src in ("ISSUE_RESULT_REPORT", "ISSUE_COMPLETION",
                                "NEW_LISTING")
    if core and direct_qty and not conflict:
        r["confidence"] = "HIGH"
    elif core:
        r["confidence"] = "MEDIUM"
    else:
        r["confidence"] = "LOW"

    # ── wealth 확정 (§16) ───────────────────────────────────────
    if completion == "CANCELLED_NO_WEALTH_EVENT":
        r.update({"wealthStatus": "WEALTH_CONFIRMED",
                  "wealthReasonR22": "R22_CANCELLED_NO_ADJUSTMENT",
                  "holderRightFinal": False})
    elif core and r["confidence"] in ("HIGH", "MEDIUM"):
        r.update({"wealthStatus": "WEALTH_CONFIRMED",
                  "wealthReasonR22": "R22_FINAL_TERMS_RECOVERED",
                  "holderRightFinal": True})
    else:
        r.update({"wealthStatus": "WEALTH_PARTIAL",
                  "wealthReasonR22": "R22_FINAL_TERMS_STILL_INCOMPLETE",
                  "holderRightFinal": False})
    return r


def main() -> int:
    pc = json.loads((RD / "r22-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))
    tg = pc["targets"]
    lists = load_lists()
    rows = [resolve_one(t, lists.get(t["ticker"], [])) for t in tg]

    def cnt(key):
        d = {}
        for r in rows:
            d[r.get(key)] = d.get(r.get(key), 0) + 1
        return d

    flags = {}
    for r in rows:
        for f in r["flags"]:
            flags[f] = flags.get(f, 0) + 1
    recov = {}
    for k in ("finalActualNewShares", "finalIssuePrice", "finalRightsRatio",
              "finalShareholderAllocatedShares", "finalShareholderEntitledShares",
              "actualPaymentDate", "finalIssueMethod"):
        n = sum(1 for r in rows if r.get(k) is not None)
        recov[k] = {"n": n, "pct": round(n / len(rows) * 100, 1)}
    srcs = {}
    for r in rows:
        for k in r["sources"]:
            srcs[k] = srcs.get(k, 0) + 1

    (RD / "r22-final-terms-normalized-latest.json").write_text(json.dumps({
        "task": "R22", "parserVersions": {"resultReport": P20.PARSER_VERSION,
                                          "expanded": P22.PARSER_VERSION},
        "targets": len(rows), "window": WINDOW["months"],
        "byCompletionStatus": cnt("completionStatus"),
        "byWealthStatus": cnt("wealthStatus"),
        "byConfidence": cnt("confidence"),
        "bySharesSource": cnt("sharesSource"),
        "byPriceSource": cnt("priceSource"),
        "sourceUsage": srcs, "flags": flags, "fieldRecovery": recov,
        "ratioBasis": {"used": "ENTITLEMENT",
                       "why": pc["wealth"]["ratioBasis"]["why"],
                       "alsoRecorded": "takeUpRightsRatio"},
        "layerSeparation": pc["layerRule"]["mustSeparate"],
        "events": rows,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    # 정정 chain (§9 provenance)
    ch = []
    for t in tg:
        em = _mon(t["date"])
        seq = [f for f in lists.get(t["ticker"], [])
               if f.get("rcept_dt") and LO <= _mon(f["rcept_dt"]) - em <= HI]
        seq.sort(key=lambda x: x["rcept_dt"])
        if len(seq) < 2:
            continue
        ch.append({"ticker": t["ticker"], "date": t["date"], "filings": len(seq),
                   "hasCorrection": any("정정" in f["report_nm"] for f in seq),
                   "kinds": sorted({f["kind"] for f in seq}),
                   "sequence": [{"rcept_no": f["rcept_no"], "date": f["rcept_dt"],
                                 "kind": f["kind"], "name": f["report_nm"]}
                                for f in seq]})
    (RD / "r22-correction-chains-latest.json").write_text(json.dumps({
        "task": "R22", "chains": len(ch),
        "withCorrection": sum(1 for c in ch if c["hasCorrection"]),
        "policy": ("계획공시 → 정정 → 가격확정 → 청약 → 납입 → 상장 순서를 "
                   "보존한다. 회계에는 사후 실제 결과를 쓴다(§4·§6)."),
        "list": ch}, ensure_ascii=False, indent=1), encoding="utf-8")

    # 신주상장 / 자본금 대조 산출물
    nl = [{"ticker": r["ticker"], "date": r["date"],
           "source": r.get("sharesSource"),
           "finalActualNewShares": r.get("finalActualNewShares"),
           "observedShareDelta": r.get("observedShareDelta"),
           "relErr": r.get("reportedVsObservedRelErr"),
           "otherEventsSameMonth": r.get("otherEventsSameMonth"),
           "flags": r["flags"]} for r in rows]
    (RD / "r22-new-listing-reconciliation-latest.json").write_text(json.dumps({
        "task": "R22", "tolerance": TOL,
        "hardRule": ("관측 주식수 delta 를 rights 하나에 전부 귀속하지 않는다(§11). "
                     "직접공시가 있으면 직접값 우선."),
        "withActual": sum(1 for x in nl if x["finalActualNewShares"]),
        "matchedWithinTolerance": sum(
            1 for x in nl if x["relErr"] is not None and x["relErr"] <= TOL),
        "explainedByOtherEvent": sum(
            1 for x in nl if "DELTA_EXPLAINED_BY_OTHER_EVENT" in x["flags"]),
        "unexplainedConflict": sum(
            1 for x in nl if "SHARE_COUNT_DATA_CONFLICT" in x["flags"]),
        "rows": nl}, ensure_ascii=False, indent=1, default=float),
        encoding="utf-8")

    print(json.dumps({"targets": len(rows),
                      "byCompletion": cnt("completionStatus"),
                      "byWealth": cnt("wealthStatus"),
                      "byConfidence": cnt("confidence"),
                      "sourceUsage": srcs, "flags": flags,
                      "chains": len(ch)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
