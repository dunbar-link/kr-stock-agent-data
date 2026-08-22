#!/usr/bin/env python3
"""R18 원문 파싱 결과 → R17 전체 2,742건 최종 재분류.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

§18: 유형 확정(type)과 wealth 확정을 혼동하지 않는다.

핵심 판정 논리 — 창 안 **모든** 유상증자 원문을 근거로 쓴다:
  · 창 안 공시가 전부 제3자배정·일반공모  → 기존 주주는 어느 쪽이 원인이든 신주를
      받지 않는다 → 미조정이 정답 → WEALTH_CONFIRMED (비율 불일치와 무관)
  · 주주배정 계열이 섞여 있다 → 비율·발행가가 필요하다.
      숫자 정합성을 통과한 문서가 있으면 WEALTH_CONFIRMED, 아니면 WEALTH_PARTIAL
  · 원문이 없거나 방식을 못 읽었다 → WEALTH_UNRESOLVED

정정공시(§9): 같은 종목·창 안에서 접수일이 가장 늦은 문서를 final effective terms
로 쓴다. 초기 조건과 전체 chain 은 provenance 로 보존한다.

안전: 네트워크 0 · 캐시/보고서만 읽고 쓴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r18_parser as P  # noqa: E402
from r18_targets import (CONSISTENCY, HOLDER_RIGHT_METHODS,  # noqa: E402
                         NO_HOLDER_RIGHT_METHODS)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-documents"
TOL = CONSISTENCY["tolerance"]
WINDOW = (-6, 1)

LABEL_BY_METHOD = {
    "SHAREHOLDER_RIGHTS": "CONFIRMED_RIGHTS",
    "RIGHTS_THEN_PUBLIC": "CONFIRMED_RIGHTS",
    "MIXED": "CONFIRMED_RIGHTS",
    "THIRD_PARTY": "CONFIRMED_THIRD_PARTY_ISSUE",
    "PUBLIC_OFFERING": "CONFIRMED_PUBLIC_OFFERING",
}


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def parse_all(targets):
    """캐시된 원문을 전부 파싱한다. 관측 비율은 해당 이벤트 것을 넘긴다."""
    obs_by_rcept = {t["rcept_no"]: t.get("observedNewPerOld")
                    for t in targets if t.get("rcept_no")}
    docs = {}
    for f in sorted(CACHE.glob("*.xml")):
        rn = f.stem
        try:
            ev = P.parse(f.read_text(encoding="utf-8"), obs_by_rcept.get(rn))
        except (OSError, ValueError) as e:            # 원문 손상은 조용히 넘기지 않는다
            docs[rn] = {"rcept_no": rn, "issueMethod": None,
                        "confidence": "UNRESOLVED",
                        "flags": ["PARSE_ERROR"], "error": type(e).__name__}
            continue
        ev["rcept_no"] = rn
        docs[rn] = ev
    return docs


def build_index(targets, docs):
    """종목 → [(접수일, 문서)] — 창 판정을 위해 필요."""
    meta = json.loads((RD / "r18-document-download-status-latest.json")
                      .read_text(encoding="utf-8"))["documents"]
    by_rcept = {m["rcept_no"]: m for m in meta}
    idx = {}
    for rn, ev in docs.items():
        m = by_rcept.get(rn) or {}
        t = m.get("ticker")
        fd = m.get("filingDate") or (ev.get("boardDate"))
        if not t or not fd:
            continue
        idx.setdefault(t, []).append((str(fd), ev))
    for t in idx:
        idx[t].sort(key=lambda x: x[0])
    return idx


def resolve_event(ev_row, idx):
    """이벤트 1건 → (label, wealthLayer, evidence)."""
    t, date = ev_row["ticker"], ev_row["date"]
    em = _mon(date)
    obs = ev_row.get("observedNewPerOld")
    cands = [(fd, d) for fd, d in idx.get(t, [])
             if WINDOW[0] <= _mon(fd) - em <= WINDOW[1] and d.get("issueMethod")]
    if not cands:
        return ("UNRESOLVED", "WEALTH_UNRESOLVED",
                {"reason": "NO_DOCUMENT_IN_WINDOW", "candidates": 0})

    methods = {d["issueMethod"] for _, d in cands}
    holder = methods & HOLDER_RIGHT_METHODS
    # 정정 chain — 접수일 최신본이 final effective terms
    final_fd, final_doc = cands[-1]
    chain = [{"rcept_no": d["rcept_no"], "filingDate": fd,
              "issueMethod": d["issueMethod"],
              "rightsRatio": d.get("rightsRatio"),
              "issuePrice": d.get("issuePriceCommon")} for fd, d in cands]

    ev = {"candidates": len(cands), "methodsInWindow": sorted(methods),
          "finalRceptNo": final_doc["rcept_no"], "finalFilingDate": final_fd,
          "chain": chain,
          "confidence": max((d.get("confidence") for _, d in cands),
                            key=lambda c: {"HIGH": 3, "MEDIUM": 2,
                                           "LOW": 1, "UNRESOLVED": 0}.get(c, 0)),
          "evidencePath": final_doc.get("evidencePath")}

    if not holder:
        # 창 안 전부 제3자·일반공모 → 기존 주주는 신주를 받지 않는다.
        lbl = LABEL_BY_METHOD.get(final_doc["issueMethod"], "CONFIRMED_NON_RIGHTS")
        ev["reason"] = "ALL_WINDOW_FILINGS_NO_HOLDER_RIGHT"
        ev["engineAction"] = "NO_ADJUSTMENT"
        ev["why"] = ("어느 공시가 원인이든 기존 주주에게 신주인수권이 없다. "
                     "비율이 맞지 않아도 미조정이 정답이다.")
        return (lbl, "WEALTH_CONFIRMED", ev)

    # 주주배정 계열이 섞여 있다 → 조건이 필요하다.
    good = [d for _, d in cands
            if d["issueMethod"] in HOLDER_RIGHT_METHODS
            and d.get("rightsRatio") and d.get("issuePriceCommon")
            and "DATA_CONFLICT" not in (d.get("flags") or [])]
    if good:
        pick = good[-1]
        ev.update({"reason": "HOLDER_RIGHT_TERMS_AVAILABLE",
                   "engineAction": "RIGHTS_ENTITLEMENT",
                   "usedRceptNo": pick["rcept_no"],
                   "rightsRatio": pick["rightsRatio"],
                   "issuePrice": pick["issuePriceCommon"],
                   "issuePriceProvenance": pick.get("issuePriceProvenance"),
                   "recordDate": pick.get("recordDate"),
                   "paymentDate": pick.get("paymentDate"),
                   "listingDate": pick.get("listingDate")})
        return ("CONFIRMED_RIGHTS", "WEALTH_CONFIRMED", ev)

    # 방식은 알지만 숫자를 믿을 수 없다 → 유형만 확정.
    ev.update({"reason": "HOLDER_RIGHT_BUT_TERMS_UNRELIABLE",
               "engineAction": "NO_ADJUSTMENT_CONSERVATIVE",
               "why": ("주주배정 계열임은 확정했지만 비율·발행가가 없거나 관측 "
                       "주식수 변동과 충돌한다. 억지 숫자를 쓰지 않고 미조정으로 "
                       "남긴다 — 과소평가는 되어도 가짜 wealth 는 만들지 않는다.")})
    return ("CONFIRMED_RIGHTS", "WEALTH_PARTIAL", ev)


def main() -> int:
    tg = json.loads((RD / "r18-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    docs = parse_all(tg)
    idx = build_index(tg, docs)

    # ── 파서 결과 저장 ───────────────────────────────────────────
    conf, meth, path, flags = {}, {}, {}, {}
    for d in docs.values():
        conf[d.get("confidence")] = conf.get(d.get("confidence"), 0) + 1
        meth[d.get("issueMethod")] = meth.get(d.get("issueMethod"), 0) + 1
        path[d.get("evidencePath")] = path.get(d.get("evidencePath"), 0) + 1
        for f in d.get("flags") or []:
            flags[f] = flags.get(f, 0) + 1
    recov = {}
    for k in ("rightsRatio", "issuePriceCommon", "recordDate", "paymentDate",
              "listingDate", "allocPerShare", "subscriptionStart", "boardDate"):
        n = sum(1 for d in docs.values() if d.get(k) is not None)
        recov[k] = {"n": n, "pct": round(n / len(docs) * 100, 1) if docs else 0}
    (RD / "r18-document-parser-results-latest.json").write_text(json.dumps({
        "task": "R18", "parserVersion": P.PARSER_VERSION,
        "documentsParsed": len(docs),
        "byConfidence": conf, "byMethod": meth, "byEvidencePath": path,
        "flags": flags, "fieldRecovery": recov,
        "structuralPathPct": round(
            (path.get("FIELD_CODE", 0) + path.get("TABLE_LABEL", 0))
            / len(docs) * 100, 1) if docs else 0,
        "documents": list(docs.values()),
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    # ── 정정 chain 저장 (§9) ─────────────────────────────────────
    chains = []
    for t, rows in idx.items():
        if len(rows) < 2:
            continue
        chains.append({
            "ticker": t, "filings": len(rows),
            "first": {"rcept_no": rows[0][1]["rcept_no"], "date": rows[0][0],
                      "issueMethod": rows[0][1].get("issueMethod"),
                      "rightsRatio": rows[0][1].get("rightsRatio"),
                      "issuePrice": rows[0][1].get("issuePriceCommon")},
            "final": {"rcept_no": rows[-1][1]["rcept_no"], "date": rows[-1][0],
                      "issueMethod": rows[-1][1].get("issueMethod"),
                      "rightsRatio": rows[-1][1].get("rightsRatio"),
                      "issuePrice": rows[-1][1].get("issuePriceCommon")},
            "termsChanged": (rows[0][1].get("rightsRatio")
                             != rows[-1][1].get("rightsRatio")
                             or rows[0][1].get("issuePriceCommon")
                             != rows[-1][1].get("issuePriceCommon")),
        })
    changed = [c for c in chains if c["termsChanged"]]
    (RD / "r18-correction-chains-latest.json").write_text(json.dumps({
        "task": "R18", "tickersWithMultipleFilings": len(chains),
        "chainsWithChangedTerms": len(changed),
        "policy": ("접수일 최신본을 final effective terms 로 쓴다. 초기 조건과 "
                   "chain 전체를 보존한다(§9·§10). PIT ranking 에 최종조건을 "
                   "소급 사용하지 않는다 — 이번 작업에는 ranking 이 없다."),
        "chains": chains,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    # ── 대상 이벤트 재분류 ───────────────────────────────────────
    out = []
    for t in tg:
        lbl, layer, ev = resolve_event(t, idx)
        out.append({**t, "r18Label": lbl, "wealthLayer": layer, "evidence": ev})
    lb, ly = {}, {}
    for r in out:
        lb[r["r18Label"]] = lb.get(r["r18Label"], 0) + 1
        ly[r["wealthLayer"]] = ly.get(r["wealthLayer"], 0) + 1
    (RD / "r18-legacy-rights-normalized-latest.json").write_text(json.dumps({
        "task": "R18", "targets": len(out),
        "byLabel": lb, "byWealthLayer": ly,
        "windowMonths": f"{WINDOW[0]}~+{WINDOW[1]}",
        "layerDefinition": {
            "WEALTH_CONFIRMED": "엔진 동작이 확정된다(미조정이 정답이거나 조건 확보).",
            "WEALTH_PARTIAL": "유형은 확정, 조건 불충분 → 보수적 미조정.",
            "WEALTH_UNRESOLVED": "원문 미확보 또는 방식 미확인.",
        },
        "events": out,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    print(json.dumps({"docs": len(docs), "byMethod": meth, "byConfidence": conf,
                      "structuralPct": round(
                          (path.get("FIELD_CODE", 0) + path.get("TABLE_LABEL", 0))
                          / len(docs) * 100, 1) if docs else 0,
                      "targets": len(out), "byLabel": lb, "byLayer": ly,
                      "chains": len(chains), "chainsChanged": len(changed)},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
