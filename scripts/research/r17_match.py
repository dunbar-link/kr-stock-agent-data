#!/usr/bin/env python3
"""R17 SUSPECTED_RIGHTS 2,742건 ↔ DART 직접증거 대조.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

§9 원칙: **날짜 근접만으로 direct match 선언 금지.** 비율 corroboration 이
있어야 HIGH 다. §의 resolved 정의: HIGH + MEDIUM 만. LOW 는 UNRESOLVED 로 센다.

★ 사전규격 대비 조정 1건 (schema 실측 기반, §29 자체수정 — 공개 기록):
  precommit 은 매칭 날짜를 '상장일 또는 납입일 ±N개월'로 썼다. 실측 결과
  DART 주요정보 API 는 **상장일·납입일·청약일을 주지 않는다** — 제공되는 날짜는
  접수번호 앞 8자리(= 공시 접수일)뿐이다. 공시는 실제 주식수 증가보다 항상
  **앞선다**(이사회결의 → 배정기준일 → 청약 → 납입 → 상장, 통상 1~3개월).
  따라서 대칭 ±N 은 방향이 틀렸다. 창을 비대칭으로 바꾸되(뒤로 길게, 앞으로 짧게)
  **엄격도는 유지한다** — HIGH 는 여전히 비율 일치를 필수로 요구한다.

안전: 네트워크 0 · 캐시/보고서만 읽고 쓴다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r17_precommit import MATCHING  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

TOL_HIGH = MATCHING["ratioTolerance"]          # 0.15
TOL_MED = MATCHING["mediumRatioMax"]           # 0.50

# 비대칭 창 (개월). 음수 = 공시가 사건보다 앞선 개월수.
WIN = {"HIGH": (-6, 1), "MEDIUM": (-4, 1), "LOW": (-12, 3)}

LABEL_BY_KIND = {
    "BONUS": "CONFIRMED_BONUS_ISSUE",
    "REDUCTION": "CONFIRMED_OTHER_CAPITAL_ACTION",
}
LABEL_BY_METHOD = {
    "SHAREHOLDER_ALLOCATION": "CONFIRMED_RIGHTS",
    "SHAREHOLDER_THEN_PUBLIC": "CONFIRMED_RIGHTS",
    "MIXED": "CONFIRMED_RIGHTS",
    "THIRD_PARTY": "CONFIRMED_THIRD_PARTY_ISSUE",
    "PUBLIC_OFFERING": "CONFIRMED_PUBLIC_OFFERING",
    "UNKNOWN": "CONFIRMED_RIGHTS_METHOD_UNKNOWN",
}
# 기존 주주에게 신주인수권이 실제로 배정되는 라벨
HOLDER_RIGHT = {"CONFIRMED_RIGHTS"}

# ── resolved 의 두 층위를 분리한다 (§16 정직한 계수) ────────────────────
#   resolvedType   그 주식수 변동이 무엇이었는지 (유상/무상/감자/제3자…)
#   resolvedWealth 기존 주주 wealth 를 계산할 수 있는지 = 엔진 동작이 확정되는지
# 공시명만 있는 증거는 '유상증자였다'는 사실은 확정하지만 **배정방식을 모른다**.
# 방식을 모르면 미조정이 정답(제3자)인지 과소평가(주주배정)인지 갈리지 않는다.
WEALTH_KNOWN_NO_ADJUST = {"CONFIRMED_THIRD_PARTY_ISSUE", "CONFIRMED_PUBLIC_OFFERING"}
WEALTH_KNOWN_MECHANICAL = {"CONFIRMED_BONUS_ISSUE"}


def wealth_resolved(label, cand):
    """엔진 동작이 확정되는가."""
    if label in WEALTH_KNOWN_NO_ADJUST or label in WEALTH_KNOWN_MECHANICAL:
        return True, "ENGINE_ACTION_DETERMINED"
    if label == "CONFIRMED_RIGHTS":
        if cand.get("issue_price_derived") and cand.get("rights_ratio"):
            return True, "RIGHTS_TERMS_AVAILABLE"
        return False, "RIGHTS_TERMS_INCOMPLETE"
    if label == "CONFIRMED_RIGHTS_METHOD_UNKNOWN":
        return False, "ISSUE_METHOD_UNKNOWN"
    if label == "CONFIRMED_OTHER_CAPITAL_ACTION":
        return False, "REDUCTION_PAID_OR_FREE_UNKNOWN"
    return False, "UNRESOLVED"


def _mon(s: str) -> int:
    """'2020-01-02' 또는 '20200102' → 절대 월 인덱스."""
    s = s.replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def _tier(gap_m: int, rel: float | None) -> str | None:
    """(공시-사건) 월 차이와 비율 상대오차로 confidence 를 준다."""
    if rel is not None and rel <= TOL_HIGH and WIN["HIGH"][0] <= gap_m <= WIN["HIGH"][1]:
        return "HIGH"
    if WIN["MEDIUM"][0] <= gap_m <= WIN["MEDIUM"][1] and (rel is None or rel <= TOL_MED):
        return "MEDIUM"
    if WIN["LOW"][0] <= gap_m <= WIN["LOW"][1]:
        return "LOW"
    return None


RANK = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


def match_all(sus, norm):
    by_ticker = {}
    for e in norm["events"]:
        by_ticker.setdefault(e["ticker"], []).append(e)
    for f in norm["filings"]:
        by_ticker.setdefault(f["ticker"], []).append(f)

    out = []
    for s in sus:
        obs_new_per_old = s["shareRatio"] - 1.0        # 기존 1주당 신주 수
        em = _mon(s["date"])
        best = None
        for c in by_ticker.get(s["ticker"], []):
            fd = c.get("filing_date")
            if not fd or len(str(fd)) < 6:
                continue
            gap = _mon(str(fd)) - em
            r = c.get("rights_ratio")
            rel = (abs(r - obs_new_per_old) / obs_new_per_old
                   if (r and obs_new_per_old > 0) else None)
            tier = _tier(gap, rel)
            if not tier:
                continue
            score = (RANK[tier], -(rel if rel is not None else 9.0), -abs(gap))
            if best is None or score > best[0]:
                best = (score, tier, rel, gap, c)

        if best is None:
            out.append({**s, "label": "UNRESOLVED", "confidence": "NONE",
                        "provenance": "NO_DIRECT_MATCH", "holderRight": False,
                        "resolvedType": False, "resolvedWealth": False,
                        "wealthReason": "NO_DIRECT_MATCH",
                        "reason": "해당 종목에 증자 관련 DART 공시를 찾지 못함"})
            continue

        _, tier, rel, gap, c = best
        kind, method = c["kind"], c.get("issue_method", "UNKNOWN")
        label = (LABEL_BY_KIND.get(kind)
                 or LABEL_BY_METHOD.get(method, "CONFIRMED_RIGHTS_METHOD_UNKNOWN"))
        if tier == "LOW":
            # §9 — 비율 corroboration 없는 원거리 매칭은 resolved 로 세지 않는다.
            label, prov = "UNRESOLVED", "DERIVED_MATCH"
        else:
            prov = "DIRECT_DART"
        wres, wreason = wealth_resolved(label, c)
        if label == "UNRESOLVED":
            wres, wreason = False, "LOW_CONFIDENCE_NOT_COUNTED"
        out.append({
            "resolvedType": label != "UNRESOLVED",
            "resolvedWealth": wres, "wealthReason": wreason,
            **s, "label": label, "confidence": tier, "provenance": prov,
            "holderRight": label in HOLDER_RIGHT,
            "evidenceLevel": c["evidenceLevel"], "endpoint": c["endpoint"],
            "rcept_no": c.get("rcept_no"), "filingDate": c.get("filing_date"),
            "filingGapMonths": gap,
            "dartKind": kind, "issueMethod": method,
            "issueMethodRaw": c.get("issue_method_raw") or c.get("report_nm"),
            "dartRatio": c.get("rights_ratio"), "observedRatio": obs_new_per_old,
            "ratioRelErr": rel,
            "issuePriceDerived": c.get("issue_price_derived"),
        })
    return out


def main() -> int:
    sus = json.loads((RD / "r17-suspected-events-latest.json")
                     .read_text(encoding="utf-8"))["events"]
    norm = json.loads((RD / "r17-dart-rights-normalized-latest.json")
                      .read_text(encoding="utf-8"))
    rows = match_all(sus, norm)

    labels, conf, prov, ev = {}, {}, {}, {}
    for r in rows:
        labels[r["label"]] = labels.get(r["label"], 0) + 1
        conf[r["confidence"]] = conf.get(r["confidence"], 0) + 1
        prov[r["provenance"]] = prov.get(r["provenance"], 0) + 1
        k = r.get("evidenceLevel", "NONE")
        ev[k] = ev.get(k, 0) + 1

    resolved = [r for r in rows if r["label"] != "UNRESOLVED"]
    unres = [r for r in rows if r["label"] == "UNRESOLVED"]
    wres = [r for r in rows if r.get("resolvedWealth")]
    wunres = [r for r in rows if not r.get("resolvedWealth")]
    wreason = {}
    for r in wunres:
        k = r.get("wealthReason", "?")
        wreason[k] = wreason.get(k, 0) + 1
    (RD / "r17-rights-matching-latest.json").write_text(json.dumps({
        "task": "R17", "suspectedTotal": len(rows),
        "byLabel": labels, "byConfidence": conf, "byProvenance": prov,
        "byEvidenceLevel": ev,
        "resolved": len(resolved), "unresolved": len(unres),
        "resolvedTickers": len({r["ticker"] for r in resolved}),
        "unresolvedTickers": len({r["ticker"] for r in unres}),
        "resolvedDefinition": MATCHING["resolvedDefinition"],
        "twoLayerNote": (
            "resolvedType = 그 주식수 변동이 무엇이었는지 확정. "
            "resolvedWealth = 기존 주주 wealth 계산이 가능한지(엔진 동작 확정). "
            "공시명만 있는 증거는 '유상증자였다'는 확정하지만 배정방식을 모르므로 "
            "wealth 는 미해결이다. foundation 판정은 **resolvedWealth** 로 한다."),
        "wealthResolved": len(wres), "wealthUnresolved": len(wunres),
        "wealthUnresolvedReasons": wreason,
        "wealthUnresolvedTickers": len({r["ticker"] for r in wunres}),
        "windowAdaptation": {
            "found": "SELF_CORRECTION — DART 스키마 실측",
            "what": ("주요정보 API 는 상장일·납입일·청약일을 주지 않는다. 접수일만 "
                     "제공되며 공시는 주식수 증가보다 항상 앞선다."),
            "fix": "대칭 창 → 비대칭 창(뒤 -6~-12개월, 앞 +1~+3개월).",
            "strictnessPreserved": "HIGH 는 여전히 비율 일치(상대오차 0.15)를 필수로 요구.",
            "windows": WIN,
        },
        "rows": rows,
    }, ensure_ascii=False, indent=1), encoding="utf-8")

    print(json.dumps({"total": len(rows), "byLabel": labels,
                      "byConfidence": conf, "byEvidence": ev,
                      "typeUnresolvedTickers": len({r["ticker"] for r in unres}),
                      "wealthResolved": len(wres),
                      "wealthUnresolvedTickers":
                      len({r["ticker"] for r in wunres}),
                      "wealthUnresolvedReasons": wreason}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
