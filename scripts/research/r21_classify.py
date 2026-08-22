#!/usr/bin/env python3
"""R21 자본행위 분류 + 직접권리 판정 — R19 분류기 재사용(§26).

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21

새 규칙을 만들지 않는다. R19 가 342건에서 검증한 `r19_classify.classify_event`
를 그대로 호출하고, 결과를 §6 의 세 축으로 저장한다:

  EVENT_IDENTITY        CONFIRMED / PARTIAL / UNRESOLVED
  DIRECT_ENTITLEMENT    YES / NO / UNKNOWN
  WEALTH_ADJUSTMENT     REQUIRED / NOT_REQUIRED / PARTIAL / UNKNOWN

핵심(§5): 주식수 증가와 기존 주주 권리는 다른 개념이다. CB 전환·BW 행사·
옵션 행사·존속회사 합병신주는 주식수를 늘리지만 기존 일반주주에게 직접
entitlement 를 주지 않는다 → 미조정이 정답.

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 는 reports/research 만.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r19_classify as K  # noqa: E402  분류 정본 재사용
from r19_targets import MECHANICAL_TYPES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-capital-actions"

CLASSIFIER_VERSION = f"r21-via-{K.CLASSIFIER_VERSION}"


def load_filings():
    out = {}
    if not CACHE.exists():
        return out
    for p in sorted(CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        out[d["ticker"]] = d["filings"]
    return out


def same_month_others(ticker, date):
    """§14 — 같은 달 다른 자본행위를 R19/R20 증거로 확인한다."""
    p = RD / "r19-event-classification-latest.json"
    if not p.exists():
        return []
    ev = json.loads(p.read_text(encoding="utf-8"))["events"]
    return sorted({e["primaryEvent"] for e in ev
                   if e["ticker"] == ticker and e["date"] == date
                   and e["primaryEvent"] != "UNRESOLVED"})


def main() -> int:
    pc = json.loads((RD / "r21-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))
    tg = pc["targets"]
    fil = load_filings()

    rows = []
    for t in tg:
        # r19_classify 는 sharesBefore 를 수량 대조에 쓴다.
        ev = dict(t, sharesBefore=t.get("observedSharesBefore"))
        c = K.classify_event(ev, fil.get(t["ticker"], []))
        mech = c.get("primaryEvent") in MECHANICAL_TYPES
        adj = c["wealthAdjustment"]
        # §6 라벨 표기를 지시문 용어에 맞춘다(내용은 R19 정본 그대로).
        adj_label = {"NOT_REQUIRED": "WEALTH_ADJUSTMENT_NOT_REQUIRED",
                     "REQUIRED": "WEALTH_ADJUSTMENT_REQUIRED",
                     "REQUIRED_MECHANICAL": "WEALTH_ADJUSTMENT_REQUIRED",
                     "UNKNOWN": "WEALTH_ADJUSTMENT_UNKNOWN"}.get(
                         adj, "WEALTH_ADJUSTMENT_UNKNOWN")
        ent_label = {"YES": "DIRECT_ENTITLEMENT_YES",
                     "NO": "DIRECT_ENTITLEMENT_NO",
                     "UNKNOWN": "DIRECT_ENTITLEMENT_UNKNOWN"}[c["entitlement"]]
        rows.append({
            **t,
            "eventIdentity": c["eventIdentity"],
            "primaryEvent": c["primaryEvent"],
            "secondaryEvents": c["secondaryEvents"],
            "directEntitlement": ent_label,
            "entitlement": c["entitlement"],
            "entitlementWhy": c["entitlementWhy"],
            "wealthAdjustment": adj_label,
            "wealthAdjustmentRaw": adj,
            "mechanical": mech,
            "confidence": c["confidence"],
            "provenance": c["provenance"],
            "directReceipt": c.get("primaryRceptNo"),
            "matchedDate": c.get("primaryFilingDate"),
            "matchedReportName": c.get("primaryReportName"),
            "matchedGapMonths": c.get("primaryGapMonths"),
            "matchedQuantity": c.get("sharesInTitle"),
            "quantityRelErr": c.get("ratioRelErr"),
            "candidates": c.get("candidates"),
            "sameMonthOtherEvents": same_month_others(t["ticker"], t["date"]),
            "classifierVersion": CLASSIFIER_VERSION,
            "notes": ("R19 분류 정본을 그대로 적용했다. 새 규칙 없음(§26)."),
        })

    def cnt(key):
        d = {}
        for r in rows:
            d[r[key]] = d.get(r[key], 0) + 1
        return d

    out = {"task": "R21", "classifierVersion": CLASSIFIER_VERSION,
           "reusedFrom": "r19_classify (§26 새 framework 금지)",
           "targets": len(rows),
           "byPrimaryEvent": cnt("primaryEvent"),
           "byDirectEntitlement": cnt("directEntitlement"),
           "byWealthAdjustment": cnt("wealthAdjustment"),
           "byConfidence": cnt("confidence"),
           "byEventIdentity": cnt("eventIdentity"),
           "byProvenance": cnt("provenance"),
           "layerNote": "유형 확정과 권리 판정과 조정 필요성은 각각 다른 축이다(§6).",
           "events": rows}
    (RD / "r21-event-classification-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1, default=float),
        encoding="utf-8")

    ent = {"DIRECT_ENTITLEMENT_YES": [], "DIRECT_ENTITLEMENT_NO": [],
           "DIRECT_ENTITLEMENT_UNKNOWN": []}
    for r in rows:
        ent[r["directEntitlement"]].append({
            "ticker": r["ticker"], "date": r["date"],
            "primaryEvent": r["primaryEvent"], "confidence": r["confidence"],
            "directReceipt": r.get("directReceipt"),
            "why": r["entitlementWhy"]})
    n = len(rows)
    yes = len(ent["DIRECT_ENTITLEMENT_YES"])
    known = yes + len(ent["DIRECT_ENTITLEMENT_NO"])
    (RD / "r21-entitlement-latest.json").write_text(json.dumps({
        "task": "R21", "coreQuestion": pc["coreQuestion"],
        "coreDistinction": pc["coreDistinction"],
        "counts": {k: len(v) for k, v in ent.items()},
        "measuredYesRateAmongKnown": round(yes / known, 4) if known else None,
        "measuredYesRateAmongAll": round(yes / n, 4) if n else None,
        "priorPopulationEstimate": 0.296,
        "comparisonNote": ("측정된 YES 비율이 추정치 0.296 보다 높으면 편향이 "
                           "올라간다. 예단하지 않고 실측값을 그대로 쓴다(§18)."),
        "mergerDirectionRule": pc["typeRules"]["mergerDirection"],
        "detail": ent,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    print(json.dumps({"targets": n, "byPrimary": out["byPrimaryEvent"],
                      "byEntitlement": out["byDirectEntitlement"],
                      "byAdjustment": out["byWealthAdjustment"],
                      "byConfidence": out["byConfidence"],
                      "measuredYesRateAmongKnown":
                      round(yes / known, 4) if known else None,
                      "priorEstimate": 0.296}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
