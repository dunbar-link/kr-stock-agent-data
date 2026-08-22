#!/usr/bin/env python3
"""R24 shareholder wealth resolution + 주식수 정합성 대조.

WABABA-FINAL-UNKNOWN-50-DIRECT-CLASSIFICATION-R24

판정 → wealth 로 옮기는 규칙(§12·§13, 전부 기존 정본):
  · NO 확정      → wealth adjustment = 0. 주가 움직임을 지우는 것이 아니라
                   없는 권리를 만들지 않는 것이다. WEALTH_CONFIRMED.
  · YES + 무납입 → 관측 shareRatio 로 기계적 조정. R16 정본 그대로.
                   외부 납입이 없으므로 조건이 더 필요하지 않다.
  · YES + 유상   → 배정비율·발행가가 있어야 확정. 없으면 PARTIAL(§12).
  · UNKNOWN      → 미해결 유지. 추정으로 채우지 않는다(§36).

안전: 계산 전용. 네트워크 0. production write 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r24_targets import MECHANICAL_TYPES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
TOL = 0.15


def resolve_one(e, t):
    r = {"ticker": e["ticker"], "date": e["date"],
         "companyName": e.get("companyName"), "shareClass": e.get("shareClass"),
         "primaryEvent": e["primaryEvent"], "entitlement": e["entitlement"],
         "entitlementBasis": e.get("entitlementBasis"),
         "confidence": e["confidence"], "shareRatio": e.get("shareRatio"),
         "primaryRceptNo": e.get("primaryRceptNo"),
         "primaryReportName": e.get("primaryReportName"),
         "flags": []}
    mech = e["primaryEvent"] in MECHANICAL_TYPES
    r["mechanical"] = mech

    if e["entitlement"] == "NO":
        r.update({"wealthStatus": "WEALTH_CONFIRMED",
                  "wealthAdjustment": 0.0,
                  "adjustmentType": "NONE_NO_ENTITLEMENT",
                  "holderRight": False, "externalContribution": 0.0,
                  "why": ("직접 evidence 로 기존주주 권리가 없다. 조정 0 이 정답이다. "
                          "주가 움직임은 그대로 둔다(§13).")})
        return r

    if e["entitlement"] == "YES" and mech:
        # 무납입 → 관측 주식수 비율이 곧 보유주식수 배수다(R16 정본).
        r.update({"wealthStatus": "WEALTH_CONFIRMED",
                  "adjustmentType": "MECHANICAL_R16",
                  "holderRight": True, "externalContribution": 0.0,
                  "shareFactor": e.get("shareRatio"),
                  "corroboratedRatio": e.get("bodyRatio"),
                  "ratioRelErr": e.get("bodyRatioRelErr"),
                  "why": ("무상 사건이라 납입이 없다. 보유주식수만 관측 비율만큼 "
                          "늘고 주가는 그만큼 내린다. R16 기계적 조정 그대로.")})
        if e.get("bodyRatioRelErr") is not None and e["bodyRatioRelErr"] > TOL:
            r["flags"].append("BODY_RATIO_DISAGREES_OBSERVED")
        return r

    if e["entitlement"] == "YES":
        ratio, price = e.get("allocPerShare"), e.get("issuePrice")
        # 라인 단위 주식수 변화와 1주 보유자의 배정비율은 다른 값이다.
        if ratio and e.get("shareRatio"):
            line = e["shareRatio"] - 1.0
            if line > 0 and abs(ratio - line) / line > TOL:
                r["flags"].append("RATIO_LINE_VS_HOLDER_CONFLICT")
        if ratio and price:
            r.update({"wealthStatus": "WEALTH_CONFIRMED",
                      "adjustmentType": "RIGHTS_R17_POLICY_A",
                      "holderRight": True, "rightsRatio": ratio,
                      "issuePrice": price,
                      "why": "구주주 배정 확정 + 배정비율·발행가 확보."})
        else:
            r.update({"wealthStatus": "WEALTH_PARTIAL",
                      "adjustmentType": "UNKNOWN",
                      "holderRight": True, "rightsRatio": ratio,
                      "issuePrice": price,
                      "why": ("구주주 배정은 확정했지만 배정비율·발행가가 모두 "
                              "확보되지 않았다. 계획치·추정으로 채우지 않는다(§12·§36).")})
        return r

    r.update({"wealthStatus": "WEALTH_UNRESOLVED", "adjustmentType": "UNKNOWN",
              "holderRight": None,
              "why": e.get("entitlementWhy") or "직접 evidence 없음. UNKNOWN 유지(§36)."})
    return r


def reconcile(e, t):
    """§16 — 관측 주식수 변화 ↔ 분류된 자본행위 주식수."""
    sb, sa = t.get("sharesBefore"), t.get("sharesAfter")
    obs = None if not (sb and sa) else sa - sb
    ev = e.get("sharesEvidence")
    row = {"ticker": e["ticker"], "date": e["date"],
           "primaryEvent": e["primaryEvent"],
           "observedBefore": sb, "observedAfter": sa, "observedDelta": obs,
           "filingShares": ev, "bodyRatio": e.get("bodyRatio"),
           "bodyRatioSemantics": e.get("bodyRatioSemantics"),
           "checked": False, "pass": None, "relErr": None,
           "secondaryEvents": e.get("secondaryEvents") or []}
    if e.get("bodyRatio") and sb:
        # 배수인지 구주 1주당 신주수인지에 따라 계산이 다르다(§31 SC9).
        implied = (sb * e["bodyRatio"]
                   if e.get("bodyRatioSemantics") == "MULTIPLIER"
                   else sb * (1.0 + e["bodyRatio"]))
        row.update({"impliedAfter": implied, "checked": True,
                    "relErr": abs(implied - sa) / sa if sa else None})
        row["pass"] = row["relErr"] is not None and row["relErr"] <= TOL
    elif ev and obs:
        row.update({"checked": True, "relErr": abs(ev - obs) / abs(obs)})
        row["pass"] = row["relErr"] <= TOL
    return row


def main() -> int:
    tg = {(t["ticker"], t["date"]): t for t in
          json.loads((RD / "r24-targets-precommit-latest.json")
                     .read_text(encoding="utf-8"))["targets"]}
    evs = json.loads((RD / "r24-event-classification-latest.json")
                     .read_text(encoding="utf-8"))["events"]

    res, rec = [], []
    for e in evs:
        t = tg[(e["ticker"], e["date"])]
        res.append(resolve_one(e, t))
        rec.append(reconcile(e, t))

    def cnt(rows, key):
        d = {}
        for r in rows:
            d[r.get(key)] = d.get(r.get(key), 0) + 1
        return dict(sorted(d.items(), key=lambda x: -x[1]))

    flags = {}
    for r in res:
        for f in r["flags"]:
            flags[f] = flags.get(f, 0) + 1

    out = {"task": "R24", "targets": len(res),
           "byWealthStatus": cnt(res, "wealthStatus"),
           "byAdjustmentType": cnt(res, "adjustmentType"),
           "byEntitlement": cnt(res, "entitlement"),
           "byConfidence": cnt(res, "confidence"),
           "mechanicalConfirmed": sum(
               1 for r in res if r.get("adjustmentType") == "MECHANICAL_R16"),
           "noEntitlementZeroAdjustment": sum(
               1 for r in res if r.get("adjustmentType") == "NONE_NO_ENTITLEMENT"),
           "flags": flags,
           "hardRule": ("YES + 조건부족은 WEALTH_PARTIAL 로 남긴다. 계획치를 "
                        "actual 로 쓰지 않는다(§12). NO 는 조정 0 이 정답이다(§13)."),
           "events": res}
    (RD / "r24-wealth-resolution-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    checked = [r for r in rec if r["checked"]]
    (RD / "r24-share-count-reconciliation-latest.json").write_text(
        json.dumps({
            "task": "R24", "rule": ("observed share delta ↔ 분류된 자본행위 주식수. "
                                    "marketCap 은 close × shares 라 독립근거로 "
                                    "쓰지 않는다(§16)."),
            "tolerance": TOL, "rows": len(rec), "checked": len(checked),
            "passed": sum(1 for r in checked if r["pass"]),
            "failed": sum(1 for r in checked if r["pass"] is False),
            "notCheckable": len(rec) - len(checked),
            "onConflict": "DATA_CONFLICT 로 보존한다. 억지로 맞추지 않는다.",
            "detail": rec}, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8")

    print(json.dumps({k: out[k] for k in
                      ("targets", "byWealthStatus", "byAdjustmentType",
                       "byEntitlement", "flags")}, ensure_ascii=False))
    print(json.dumps({"reconChecked": len(checked),
                      "reconPassed": sum(1 for r in checked if r["pass"])},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
