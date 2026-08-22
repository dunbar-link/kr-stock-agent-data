#!/usr/bin/env python3
"""R19 자본행위 분류 + 기존 주주 직접권리 판정.

WABABA-NO-DIRECT-MATCH-CAPITAL-ACTION-RECOVERY-R19

§13 대로 세 축을 **분리해서** 기록한다:
  EVENT_IDENTITY                CONFIRMED / PARTIAL / UNRESOLVED
  SHAREHOLDER_DIRECT_ENTITLEMENT YES / NO / UNKNOWN
  WEALTH_ADJUSTMENT             REQUIRED / NOT_REQUIRED / PARTIAL / UNKNOWN

핵심(§4): 희석과 직접권리는 다르다. 다른 투자자의 CB 전환·BW 행사·옵션 행사로
주식수가 늘어난 것은 기존 보통주 주주에게 아무 권리도 주지 않는다 → 아무것도
더하지 않는 것이 정답이고, R16 의 미조정이 이미 맞다.

안전: 네트워크 0 · 캐시만 읽는다 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r19_targets import (CANNOT_EXPLAIN_SHARE_INCREASE,  # noqa: E402
                         CONFIDENCE, EVENT_TYPES, MECHANICAL_TYPES)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-capital-actions"
TOL = CONFIDENCE["ratioTolerance"]

CLASSIFIER_VERSION = "r19-classify-3"

# 공시명 → 유형. **긴 문구·구체적 문구 우선**(오분류 방지).
# 각 항목: (유형, 정규식, 창(개월, 앞/뒤))
RULES = [
    ("STOCK_OPTION_EXERCISE", r"주식매수선택권\s*행사", (-3, 3)),
    ("CONVERTIBLE_BOND_CONVERSION", r"전환청구권\s*행사|전환권\s*행사", (-3, 3)),
    ("BW_WARRANT_EXERCISE", r"신주인수권\s*행사|신주인수권증권\s*행사", (-3, 3)),
    ("EXCHANGEABLE_BOND_EXCHANGE", r"교환청구권\s*행사", (-3, 3)),
    ("SHARE_SWAP", r"주식교환|주식이전", (-6, 3)),
    # ★ 자체수정 2: '주식분할결정'(액면분할)은 회사분할이 아니다. 먼저 매칭한다.
    ("STOCK_SPLIT", r"주식분할|액면분할", (-6, 3)),
    ("SPINOFF_RELATED_SHARES", r"분할합병|회사분할|분할\s*결정", (-6, 3)),
    ("MERGER_NEW_SHARES", r"회사합병|합병\s*결정|소규모합병|간이합병", (-6, 3)),
    ("BONUS_ISSUE", r"무상증자", (-6, 1)),
    ("CAPITAL_REDUCTION", r"감자\s*결정|자본감소", (-6, 1)),
    ("TREASURY_ACTION", r"자기주식", (-6, 1)),
    ("TRUE_RIGHTS_ISSUE", r"유상증자\s*결정", (-6, 1)),
    # 사채 '발행결정' 은 전환·행사 자체가 아니다. 정황 근거로만 쓴다(LOW).
    ("CONVERTIBLE_BOND_CONVERSION", r"전환사채권?\s*발행", (-12, 0)),
    ("BW_WARRANT_EXERCISE", r"신주인수권부사채권?\s*발행", (-12, 0)),
]
WEAK_PATTERNS = (r"전환사채권?\s*발행", r"신주인수권부사채권?\s*발행")

# 모회사가 아니라 종속회사 사건을 가리키는 공시는 배제한다.
SUBSIDIARY = re.compile(r"종속회사|자회사의|피출자회사")


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def _shares_in(nm):
    """공시명에 주식수가 들어있는 경우가 있다. 없으면 None."""
    m = re.search(r"([\d,]{4,})\s*주", nm)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def classify_filing(nm):
    """공시명 → (유형, 약한근거여부). 매칭 실패는 None.

    ★ 자체수정 3 (§31): 초판은 창을 **유형 이름으로** 되찾았다(window_for).
      그런데 같은 유형에 규칙이 둘 있다 — '전환청구권행사'(-3~+3, 강한 근거)와
      '전환사채권 발행결정'(-12~0, 약한 근거). 이름으로 찾으면 항상 첫 규칙의
      창이 나와 발행결정이 창 밖으로 탈락했다. 매칭된 **그 규칙의 창**을 함께
      돌려주도록 고친다. unit test 가 잡았다.
    """
    if SUBSIDIARY.search(nm):
        return None, False, None
    for kind, pat, win in RULES:
        if re.search(pat, nm):
            weak = any(re.search(w, nm) for w in WEAK_PATTERNS)
            return kind, weak, win
    return None, False, None


def entitlement_of(kind, ev_row=None):
    """(YES/NO/UNKNOWN, 근거) — §4 의 핵심 질문에 답한다."""
    spec = EVENT_TYPES.get(kind) or {}
    e = spec.get("entitlement")
    if e is True:
        return "YES", spec.get("why", "")
    if e is False:
        return "NO", spec.get("why", "")
    if kind in ("MERGER_NEW_SHARES", "SHARE_SWAP", "SPINOFF_RELATED_SHARES"):
        # §7·§20 방향성. 이 표본은 **주식수가 증가한** 사건만 모은 것이다.
        # 주식수가 늘었다 = 신주를 발행한 쪽 = 상대회사 주주가 받는다.
        # 흡수당한 회사는 상장폐지되어 주식수 증가로 관측되지 않는다.
        return "NO", ("주식수 증가 = 분석대상이 신주를 **발행한** 쪽(존속·완전모회사). "
                      "신주는 상대회사 주주에게 간다. 분석대상 주주가 대가를 받는 "
                      "경우는 주식수 증가가 아니라 상장폐지로 나타난다.")
    if kind == "CAPITAL_REDUCTION":
        return "UNKNOWN", "유상감자(현금)와 무상감자를 공시명만으로 구분할 수 없다."
    return "UNKNOWN", spec.get("why", "")


def classify_event(ev, filings):
    """대상 사건 1건 → 분류 결과."""
    em = _mon(ev["date"])
    obs_new = ev["shareRatio"] - 1.0
    cands = []
    for f in filings:
        nm = f.get("report_nm") or ""
        fd = f.get("rcept_dt")
        if not fd:
            continue
        kind, weak, win = classify_filing(nm)
        if not kind:
            continue
        lo, hi = win
        gap = _mon(fd) - em
        if not (lo <= gap <= hi):
            continue
        sh = _shares_in(nm)
        rel = None
        if sh and ev.get("sharesBefore"):
            rel = abs(sh / ev["sharesBefore"] - obs_new) / obs_new if obs_new else None
        cands.append({"kind": kind, "weak": weak, "rcept_no": f.get("rcept_no"),
                      "filingDate": fd, "reportName": nm, "gapMonths": gap,
                      "sharesInTitle": sh, "ratioRelErr": rel})

    if not cands:
        return {"eventIdentity": "UNRESOLVED", "primaryEvent": "UNRESOLVED",
                "secondaryEvents": [], "entitlement": "UNKNOWN",
                "entitlementWhy": "창 안에서 자본행위 공시를 찾지 못했다.",
                "wealthAdjustment": "UNKNOWN", "confidence": "UNRESOLVED",
                "provenance": "NO_DIRECT_MATCH", "candidates": 0,
                "classifierVersion": CLASSIFIER_VERSION}

    # ★ 자체수정 1 (§31): 주식수가 **증가**한 사건이므로, 발행주식총수를 늘릴 수
    #   없는 행위(자기주식·감자·코드변경)는 PRIMARY 가 될 수 없다. 이 공시들은
    #   흔해서 '가장 가까운 공시' 규칙으로 PRIMARY 를 가로챈다(실측 60건 오분류).
    #   secondary 로는 그대로 남긴다.
    explains = [c for c in cands
                if c["kind"] not in CANNOT_EXPLAIN_SHARE_INCREASE]
    if not explains:
        return {"eventIdentity": "UNRESOLVED", "primaryEvent": "UNRESOLVED",
                "secondaryEvents": sorted({c["kind"] for c in cands}),
                "entitlement": "UNKNOWN",
                "entitlementWhy": ("창 안 공시가 전부 주식수를 늘릴 수 없는 "
                                   "행위(자기주식·감자)뿐이다. 증가 원인을 "
                                   "설명하지 못한다."),
                "wealthAdjustment": "UNKNOWN", "confidence": "UNRESOLVED",
                "provenance": "NO_EXPLANATORY_FILING",
                "candidates": len(cands),
                "classifierVersion": CLASSIFIER_VERSION}

    # PRIMARY 선정: 강한 근거(행사·결정 공시) 우선 → 사건월에 가까운 것 우선
    strong = [c for c in explains if not c["weak"]]
    pool = strong or explains
    pool.sort(key=lambda c: (abs(c["gapMonths"]),
                             0 if c["ratioRelErr"] is None else c["ratioRelErr"]))
    primary = pool[0]
    kinds = []
    for c in cands:
        if c["kind"] not in kinds:
            kinds.append(c["kind"])

    ent, why = entitlement_of(primary["kind"], ev)

    # confidence (§11·§12) — 제목만 맞고 수량이 전혀 안 맞으면 HIGH 금지
    if primary["weak"]:
        conf = "LOW"
    elif primary["ratioRelErr"] is not None and primary["ratioRelErr"] <= TOL:
        conf = "HIGH"
    elif primary["sharesInTitle"] is not None:
        conf = "MEDIUM"          # 수량이 있는데 안 맞음
    else:
        conf = "MEDIUM"          # 직접공시 유형은 확실, 수량 근거 없음

    identity = "CONFIRMED" if conf in ("HIGH", "MEDIUM") else "PARTIAL"
    mechanical = primary["kind"] in MECHANICAL_TYPES
    if ent == "NO":
        adj = "NOT_REQUIRED" if identity == "CONFIRMED" else "UNKNOWN"
    elif ent == "YES":
        # 주식분할·무상증자는 납입이 없어 관측 shareRatio 만으로 조정이 확정된다.
        adj = "REQUIRED_MECHANICAL" if mechanical else "REQUIRED"
    else:
        adj = "UNKNOWN"

    return {"eventIdentity": identity,
            "primaryEvent": primary["kind"],
            "secondaryEvents": [k for k in kinds if k != primary["kind"]],
            "entitlement": ent, "entitlementWhy": why,
            "wealthAdjustment": adj, "confidence": conf,
            "mechanical": mechanical,
            "provenance": "DIRECT_DART" if not primary["weak"] else "DERIVED_MATCH",
            "primaryRceptNo": primary["rcept_no"],
            "primaryFilingDate": primary["filingDate"],
            "primaryReportName": primary["reportName"],
            "primaryGapMonths": primary["gapMonths"],
            "sharesInTitle": primary["sharesInTitle"],
            "ratioRelErr": primary["ratioRelErr"],
            "candidates": len(cands),
            "classifierVersion": CLASSIFIER_VERSION}


def load_filings():
    out = {}
    if not CACHE.exists():
        return out
    for p in sorted(CACHE.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        out[d["ticker"]] = d["filings"]
    return out


def main() -> int:
    pc = json.loads((RD / "r19-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))
    tg = pc["targets"]
    fil = load_filings()

    # 관측 주식수(증자 전)를 스냅샷에서 붙여 수량 대조에 쓴다.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import r16_canonical as C
    from r16_audit import contiguous_span
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    pos = {d: i for i, d in enumerate(ds)}
    for t in tg:
        i = pos.get(t["date"])
        if i and i > 0:
            prev = cap.get(ds[i - 1], {}).get(t["ticker"]) or {}
            t["sharesBefore"] = prev.get("shares")

    rows = []
    for t in tg:
        r = classify_event(t, fil.get(t["ticker"], []))
        rows.append({**t, **r})

    def cnt(key):
        d = {}
        for r in rows:
            d[r[key]] = d.get(r[key], 0) + 1
        return d

    out = {"task": "R19", "classifierVersion": CLASSIFIER_VERSION,
           "targets": len(rows),
           "byPrimaryEvent": cnt("primaryEvent"),
           "byEntitlement": cnt("entitlement"),
           "byWealthAdjustment": cnt("wealthAdjustment"),
           "byConfidence": cnt("confidence"),
           "byIdentity": cnt("eventIdentity"),
           "byProvenance": cnt("provenance"),
           "layerNote": ("§13 — 유형 확정(eventIdentity)과 권리 판정(entitlement)과 "
                         "조정 필요성(wealthAdjustment)은 각각 다른 축이다."),
           "events": rows}
    (RD / "r19-event-classification-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1, default=float),
        encoding="utf-8")

    ent = {"YES": [], "NO": [], "UNKNOWN": []}
    for r in rows:
        ent[r["entitlement"]].append(
            {"ticker": r["ticker"], "date": r["date"],
             "primaryEvent": r["primaryEvent"], "confidence": r["confidence"],
             "why": r["entitlementWhy"], "rcept_no": r.get("primaryRceptNo")})
    (RD / "r19-shareholder-entitlement-latest.json").write_text(json.dumps({
        "task": "R19", "coreQuestion": pc["coreQuestion"],
        "coreDistinction": pc["coreDistinction"],
        "counts": {k: len(v) for k, v in ent.items()},
        "mergerDirectionRule": pc["mergerDirection"],
        "detail": ent,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    print(json.dumps({"targets": len(rows), "byPrimary": out["byPrimaryEvent"],
                      "byEntitlement": out["byEntitlement"],
                      "byAdjustment": out["byWealthAdjustment"],
                      "byConfidence": out["byConfidence"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
