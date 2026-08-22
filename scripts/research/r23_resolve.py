#!/usr/bin/env python3
"""R23 배정량 확정 — 증권신고서 근거 + R22 사후공시 결합.

WABABA-FINAL-11-HOLDER-RIGHTS-ALLOCATION-RECOVERY-R23

§5 우선순위대로 배정 근거를 고른다. §11 의 네 수량을 분리해 담고,
§13 정합성 검사를 하며, §14 대로 LOW 는 WEALTH_CONFIRMED 로 올리지 않는다.

★ 자체수정 (§31): R22 가 넘겨준 확정발행가 중 일부가 연도값이었다(2009.0,
  2010.6). 라벨 뒤 숫자를 집다가 날짜를 읽은 파싱 오류다. 권리락 주가 대비
  터무니없는 가격은 **버리고** 신고서 값을 쓴다. 추정으로 채우지 않는다.

안전: 네트워크 0 · 캐시/보고서만 읽고 쓴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r23_parser as P  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402
from r23_targets import CONFIDENCE, RECONCILIATION  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
DOC = ROOT / "_cache" / "dart-documents"
TOL = RECONCILIATION["tolerance"]

# 신고서 종류 선호도 — 확정 > 정정 > 원본 > 투자설명서
KIND_RANK = {"REG_CONFIRMED": 0, "REG_AMENDED": 1, "PROSPECTUS_AMENDED": 2,
             "REG_ORIGINAL": 3, "PROSPECTUS": 4}


def price_sane(price, p_ex):
    """§31 — 발행가가 권리락 주가와 자릿수가 맞는가."""
    if isinstance(price, dict):
        price = price.get("price")
    if not price or not p_ex:
        return False
    return 0.02 * p_ex <= price <= 3.0 * p_ex


def _registration_price(cands):
    """확정신고서의 모집금액 ÷ 모집주식수. 확정본을 우선한다."""
    for want in ("REG_CONFIRMED", "REG_AMENDED", "REG_ORIGINAL"):
        for c in cands:
            if c["meta"]["kind"] != want:
                continue
            d = c["data"]
            amt = d.get("offerAmount")
            n = d.get("offerSharesHeader") or d.get("offerTotalShares")
            if amt and n:
                return {"price": amt / n, "basis": "OFFER_AMOUNT_DIV_SHARES",
                        "rcept_no": c["meta"]["rcept_no"], "kind": want}
            if d.get("confirmedPrice"):
                return {"price": d["confirmedPrice"], "basis": "STATED",
                        "rcept_no": c["meta"]["rcept_no"], "kind": want}
    return None


def load_docs():
    meta = json.loads((RD / "r23-securities-filings-latest.json")
                      .read_text(encoding="utf-8"))["documents"]
    by = {}
    for m in meta:
        if m.get("status") != "OK":
            continue
        f = DOC / f"{m['rcept_no']}.xml"
        if not f.exists():
            continue
        try:
            d = P.parse(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        by.setdefault(m["ticker"], []).append({"meta": m, "data": d})
    for t in by:
        by[t].sort(key=lambda x: (KIND_RANK.get(x["meta"]["kind"], 9),
                                  -int(x["meta"]["rcept_dt"])))
    return by


def resolve_one(t, docs, eng):
    i = eng.pos.get(t["date"])
    p_cum = eng.price(i - 1, t["ticker"]) if (i and i > 0) else None
    p_ex = eng.price(i, t["ticker"]) if i is not None else None
    r = {**t, "flags": [], "exPrice": p_ex, "cumPrice": p_cum}

    cands = docs.get(t["ticker"], [])
    r["filingsExamined"] = len(cands)

    # ── 배정 근거 선택 (§5) ─────────────────────────────────────
    best = None
    for c in cands:
        d = c["data"]
        if d.get("hasAllocationEvidence"):
            best = c
            break
    if best is None:
        for c in cands:
            if c["data"].get("hasRatioEvidence"):
                best = c
                break

    if best is None:
        r.update({"allocationStatus": "NO_ALLOCATION_EVIDENCE",
                  "wealthStatus": "WEALTH_PARTIAL", "confidence": "LOW",
                  "holderRightFinal": False,
                  "wealthReasonR23": "R23_NO_ALLOCATION_EVIDENCE",
                  "reason": ("증권신고서·투자설명서에서 구주주 배정량도 배정비율도 "
                             "찾지 못했다. 계획치로 채우지 않는다(§36).")})
        return r

    d, m = best["data"], best["meta"]
    r["allocationSource"] = {
        "rcept_no": m["rcept_no"], "date": m["rcept_dt"], "kind": m["kind"],
        "reportName": m["report_nm"], "basis": d.get("allocationSource"),
        "eligibleSource": d.get("eligibleSource")}

    # ── §11 네 수량 분리 ────────────────────────────────────────
    r.update({
        "PLANNED_NEW_SHARES": t.get("plannedNewShares") or d.get("offerTotalShares"),
        "ENTITLED_TO_EXISTING_SHAREHOLDERS": d.get("shareholderAllocatedShares"),
        "ACTUALLY_ISSUED_TOTAL": t.get("knownFinalShares"),
        "ACTUALLY_SUBSCRIBED_BY_EXISTING_SHAREHOLDERS": None,
        "offerTotalShares": d.get("offerTotalShares"),
        "esopShares": d.get("esopShares"),
        "issuedCommonShares": d.get("issuedCommonShares"),
        "preferredShares": d.get("preferredShares"),
        "treasuryShares": d.get("treasuryShares"),
        "treasuryExcluded": d.get("treasuryExcluded"),
        "eligibleOldShares": d.get("eligibleOldShares"),
        "statedRatio": d.get("ratioPerOldShare"),
        "derivedRatio": d.get("derivedRatio"),
        "parserFlags": d.get("flags", []),
    })

    # ── 최종 배정비율 (§12) — 명시 비율 우선 ────────────────────
    ratio = d.get("ratioPerOldShare") or d.get("derivedRatio")
    # ★ 자체수정 5 (§31·§36): 비율이 **정확히 1.0** 인 건이 반복됐는데, 확인해
    #   보니 같은 문서에 실린 무상증자(1주당 1.00000)를 집은 것이었다. 문맥
    #   스코프로 대부분 걸렀지만 남는 경우가 있다. corroboration(파생비율 일치)
    #   없이 1.0 을 쓰지 않는다 — 추정으로 정본을 만들지 않는다.
    if (d.get("ratioPerOldShare") == 1.0
            and not (d.get("derivedRatio")
                     and abs(d["derivedRatio"] - 1.0) <= 0.15)):
        r["flags"].append("RATIO_EXACTLY_ONE_UNCORROBORATED")
        ratio = d.get("derivedRatio")
    r["finalRightsRatio"] = ratio
    if ratio is None:
        r["ratioBasis"] = None
    elif ratio == d.get("ratioPerOldShare"):
        r["ratioBasis"] = "STATED_IN_REGISTRATION"
    else:
        r["ratioBasis"] = "DERIVED_ALLOCATION_DIV_ELIGIBLE"
    # 폐기한 1.0 때문에 생긴 충돌은 충돌이 아니다. 남은 값끼리 다시 본다.
    if ("RATIO_DATA_CONFLICT" in (d.get("flags") or [])
            and "RATIO_EXACTLY_ONE_UNCORROBORATED" not in r["flags"]):
        r["flags"].append("RATIO_DATA_CONFLICT")

    # ── 확정발행가 (§9) ─────────────────────────────────────────
    # ★ 자체수정 4 (§31): R22 가 넘긴 확정발행가 중 2009.0·2010.6 처럼 **연도**가
    #   섞여 있었다(라벨 뒤 숫자 스캔이 날짜를 집은 것). 자릿수 검사만으로는
    #   걸러지지 않는다. 확정신고서의 **모집금액 ÷ 모집주식수**는 직접 명시된
    #   두 값의 산술이라 훨씬 신뢰도가 높으므로 그것을 1순위로 둔다.
    #   두 값이 크게 어긋나면 신고서 값을 쓰고 R22 값을 버린다.
    r22p = t.get("knownFinalIssuePrice")
    regp = _registration_price(docs.get(t["ticker"], []))
    price, psrc = None, None
    if regp and price_sane(regp, p_ex):
        price, psrc = regp["price"], f"R23_REGISTRATION:{regp['basis']}"
        if r22p and abs(r22p - regp["price"]) / regp["price"] > TOL:
            r["flags"].append("R22_PRICE_DISCARDED_DISAGREES_REGISTRATION")
            r["discardedR22Price"] = r22p
    elif r22p and price_sane(r22p, p_ex):
        price, psrc = r22p, f"R22:{t.get('knownFinalIssuePriceSource')}"
    elif r22p:
        r["flags"].append("R22_PRICE_DISCARDED_IMPLAUSIBLE")
        r["discardedR22Price"] = r22p
    r["finalIssuePrice"], r["priceSource"] = price, psrc
    r["registrationPrice"] = regp["price"] if regp else None

    # ── §13 정합성 ──────────────────────────────────────────────
    ent, elig = (r["ENTITLED_TO_EXISTING_SHAREHOLDERS"],
                 r["eligibleOldShares"])
    if ent and elig and ratio:
        r["reconTest1RelErr"] = abs(ent - elig * ratio) / (elig * ratio)
        if r["reconTest1RelErr"] > TOL:
            r["flags"].append("ALLOCATION_RATIO_MISMATCH")
    iss = r["ACTUALLY_ISSUED_TOTAL"]
    if ent and iss:
        r["reconTest2RelErr"] = abs(iss - (ent + (d.get("esopShares") or 0))) / iss
    # eligible 이 관측 발행주식수와 자릿수가 맞는지 (자기주식 제외 반영)
    sb = t.get("observedSharesBefore")
    if elig and sb:
        r["eligibleVsObservedRatio"] = elig / sb
        if not (0.5 <= elig / sb <= 1.02):
            r["flags"].append("ELIGIBLE_OUT_OF_RANGE")

    # ── confidence (§14) ────────────────────────────────────────
    conflict = any(x in r["flags"] for x in
                   ("RATIO_DATA_CONFLICT", "ALLOCATION_RATIO_MISMATCH",
                    "ELIGIBLE_OUT_OF_RANGE"))
    if ratio and price and ent and elig and not conflict:
        r["confidence"] = "HIGH"
    elif ratio and price and not conflict:
        r["confidence"] = "MEDIUM"
    else:
        r["confidence"] = "LOW"

    # ── wealth 확정 (§15) ───────────────────────────────────────
    if ratio and price and r["confidence"] in ("HIGH", "MEDIUM"):
        r.update({"allocationStatus": "ALLOCATION_RECOVERED",
                  "wealthStatus": "WEALTH_CONFIRMED",
                  "wealthReasonR23": "R23_ALLOCATION_RECOVERED",
                  "holderRightFinal": True})
    else:
        missing = [k for k, v in (("finalRightsRatio", ratio),
                                  ("finalIssuePrice", price)) if not v]
        r.update({"allocationStatus": "PARTIAL_EVIDENCE",
                  "wealthStatus": "WEALTH_PARTIAL",
                  "wealthReasonR23": "R23_ALLOCATION_STILL_INCOMPLETE",
                  "holderRightFinal": False, "missingFields": missing})
    return r


def main() -> int:
    pc = json.loads((RD / "r23-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))
    tg = pc["targets"]
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    eng = C.CanonicalWealth(ds, cap=cap)
    docs = load_docs()
    rows = [resolve_one(t, docs, eng) for t in tg]

    def cnt(key):
        d = {}
        for r in rows:
            d[r.get(key)] = d.get(r.get(key), 0) + 1
        return d

    flags = {}
    for r in rows:
        for f in r["flags"] + r.get("parserFlags", []):
            flags[f] = flags.get(f, 0) + 1
    recov = {}
    for k in ("finalRightsRatio", "finalIssuePrice", "eligibleOldShares",
              "ENTITLED_TO_EXISTING_SHAREHOLDERS", "treasuryShares",
              "issuedCommonShares", "ACTUALLY_ISSUED_TOTAL"):
        n = sum(1 for r in rows if r.get(k) is not None)
        recov[k] = {"n": n, "pct": round(n / len(rows) * 100, 1)}

    (RD / "r23-allocation-tables-latest.json").write_text(json.dumps({
        "task": "R23", "parserVersion": P.PARSER_VERSION,
        "targets": len(rows),
        "byAllocationStatus": cnt("allocationStatus"),
        "byRatioBasis": cnt("ratioBasis"),
        "byPriceSource": cnt("priceSource"),
        "flags": flags, "fieldRecovery": recov,
        "treasuryExcludedCount": sum(1 for r in rows if r.get("treasuryExcluded")),
        "preferredSeparatedCount": sum(1 for r in rows
                                       if r.get("preferredShares") is not None),
        "fourQuantities": pc["fourQuantities"],
        "eligibleRule": pc["eligible"],
        "rows": rows,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    (RD / "r23-final-terms-latest.json").write_text(json.dumps({
        "task": "R23", "targets": len(rows),
        "byWealthStatus": cnt("wealthStatus"),
        "byConfidence": cnt("confidence"),
        "allocationHardRule": pc["allocationHardRule"],
        "noResultEngineering": pc["noResultEngineering"],
        "events": [{k: v for k, v in r.items() if k != "parserFlags"}
                   for r in rows],
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    rec = [{"ticker": r["ticker"], "date": r["date"],
            "eligibleOldShares": r.get("eligibleOldShares"),
            "observedSharesBefore": r.get("observedSharesBefore"),
            "eligibleVsObserved": r.get("eligibleVsObservedRatio"),
            "entitled": r.get("ENTITLED_TO_EXISTING_SHAREHOLDERS"),
            "statedRatio": r.get("statedRatio"),
            "derivedRatio": r.get("derivedRatio"),
            "reconTest1RelErr": r.get("reconTest1RelErr"),
            "reconTest2RelErr": r.get("reconTest2RelErr"),
            "actuallyIssued": r.get("ACTUALLY_ISSUED_TOTAL"),
            "observedShareDelta": r.get("observedShareDelta"),
            "otherEventsSameMonth": r.get("otherEventsSameMonth"),
            "flags": r["flags"]} for r in rows]
    (RD / "r23-share-reconciliation-latest.json").write_text(json.dumps({
        "task": "R23", "tolerance": TOL,
        "test1": RECONCILIATION["test1"], "test2": RECONCILIATION["test2"],
        "onConflict": RECONCILIATION["onConflict"],
        "test1Pass": sum(1 for x in rec if x["reconTest1RelErr"] is not None
                         and x["reconTest1RelErr"] <= TOL),
        "test1Checked": sum(1 for x in rec if x["reconTest1RelErr"] is not None),
        "conflicts": sum(1 for x in rec
                         if "ALLOCATION_RATIO_MISMATCH" in x["flags"]),
        "rows": rec}, ensure_ascii=False, indent=1, default=float),
        encoding="utf-8")

    print(json.dumps({"targets": len(rows),
                      "byAllocationStatus": cnt("allocationStatus"),
                      "byWealthStatus": cnt("wealthStatus"),
                      "byConfidence": cnt("confidence"),
                      "flags": flags}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
