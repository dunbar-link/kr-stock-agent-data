#!/usr/bin/env python3
"""R33C0A — PBR/BPS canonical source 판별 + frozen BM 동등성 측정.

WABABA-PIT-PBR-BPS-CANONICAL-SOURCE-RESOLUTION-AND-RESTORE-R33C0A
SOURCE: WABABA-RESEARCH-DATA-PRODUCER-RESTORE-AND-SCHEDULE-R33C0 (deeda30)

R33C0 에서 막힌 노드는 하나다 — BM(=1/PBR)의 PBR·BPS 를 미래 시점에
공식 경로로 생산할 수 있는가.

── 이 모듈이 하는 일 ───────────────────────────────────────────────
  1. pykrx 가 실제로 부르는 endpoint 를 분류한다(인증정보·쿠키 미노출).
  2. frozen BM 의 BPS 의미를 historical snapshot 에서 복원한다.
  3. 기존 OpenDART cache 로 그 BPS 를 재현할 수 있는지 **측정**한다.

── 이 모듈이 하지 않는 일 ──────────────────────────────────────────
  BM 정의 변경 · 성과 계산 · decision engine 호출 · 새 vendor 도입 ·
  KRX 웹 로그인 · 새 credential 신청 · PIT snapshot 생산.

  동등성이 성립하지 않으면 producer 를 만들지 않는다. 비슷하다는 이유로
  채택하지 않는다(§13).

안전: 기존 local cache 읽기 전용. DART API 호출 0. 인증키 값 미출력.
"""
from __future__ import annotations

import csv
import glob
import gzip
import hashlib
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SNAP = ROOT / "_cache" / "pit-snapshots"
DART_ANNUAL = ROOT / "_cache" / "dart-statements"
DART_QUARTER = ROOT / "_cache" / "dart-statements-quarterly"
CORP_CODES = ROOT / "_cache" / "dart-corp-codes.json"

TASK_ID = "WABABA-PIT-PBR-BPS-CANONICAL-SOURCE-RESOLUTION-AND-RESTORE-R33C0A"
CONTRACT_ID = "WABABA_PIT_PBR_BPS_SOURCE_R33C0A_V1"
SOURCE_COMMIT = "deeda30"

# 자본 계정 후보. 결과를 보고 고르지 않는다 — 전부 측정해 보고한다.
EQUITY_ACCOUNTS = [
    ("ifrs-full_EquityAttributableToOwnersOfParent", "지배기업 소유주지분"),
    ("ifrs-full_Equity", "자본총계"),
]

# frozen BM 수용기준(§12). 결과를 보고 완화하지 않는다.
ACCEPTANCE = {
    "unexplainedFormulaMismatch": 0,
    "unexplainedDenominatorMismatch": 0,
    "unexplainedStatementBasisMismatch": 0,
    "unexplainedPitCutoffMismatch": 0,
    "selectedBucketMismatch": 0,
    "note": "잔여 수치차는 문서화된 결정적 반올림으로 전부 설명될 때만 허용",
}

CONTRACT = {
    "contractId": CONTRACT_ID,
    "taskId": TASK_ID,
    "sourceR33C0Commit": SOURCE_COMMIT,
    "pykrxWebAllowed": False,
    "newVendorAllowed": False,
    "bmDefinitionChangeAllowed": False,
    "performanceCalculation": "FORBIDDEN",
    "decisionEngineCall": "FORBIDDEN",
    "sourcePriority": [
        "1. 공식 문서화된 direct PBR/BPS API (있을 때만)",
        "2. 기존 공식 OpenDART pipeline — 동등성 증명될 때만",
        "3. fallback 없음",
    ],
    "equityAccountCandidates": [a for a, _ in EQUITY_ACCOUNTS],
    "acceptance": ACCEPTANCE,
}


def canonical_json(o):
    return json.dumps(o, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def contract_hash():
    return hashlib.sha256(canonical_json(CONTRACT).encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════════════
# §7 pykrx endpoint 분류
# ══════════════════════════════════════════════════════════════════════
OFFICIAL_KRX_HOSTS = {"openapi.krx.co.kr", "data-dbg.krx.co.kr"}
WEB_KRX_HOSTS = {"data.krx.co.kr"}


def classify_endpoints(records):
    """호스트·content-type·로그인 폼 유무로 분류한다. 추측하지 않는다."""
    hosts = {r.get("host") for r in records}
    login_forms = [r for r in records
                   if any(k in (r.get("dataKeys") or []) for k in ("pw", "mbrId"))]
    html = [r for r in records if r.get("looksHtml")]
    if hosts & WEB_KRX_HOSTS or login_forms:
        cls = "KRX_WEB_SESSION_ENDPOINT"
    elif hosts and hosts <= OFFICIAL_KRX_HOSTS:
        cls = "OFFICIAL_KRX_OPEN_API"
    else:
        cls = "UNKNOWN_OR_UNDOCUMENTED"
    return {
        "classification": cls,
        "canonicalEligibility": ("ACCEPTED" if cls == "OFFICIAL_KRX_OPEN_API"
                                 else "REJECTED"),
        "hosts": sorted(h for h in hosts if h),
        "loginFormRequests": len(login_forms),
        "htmlResponses": len(html),
        "endpointCount": len(records),
        "why": ("로그인 폼(mbrId/pw) 전송과 HTML 응답이 있으면 화면용 웹 세션 "
                "endpoint 다. 공식 OPEN API(AUTH_KEY 헤더, JSON 명세)가 아니다."),
    }


# ══════════════════════════════════════════════════════════════════════
# §11 frozen BM 의미 복원 + §13 동등성 측정
# ══════════════════════════════════════════════════════════════════════
def _f(x):
    try:
        v = float(str(x).replace(",", ""))
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def load_snapshot(d):
    p = SNAP / f"{d}.csv.gz"
    if not p.exists():
        return {}
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return {r["ticker"]: r for r in csv.DictReader(fh) if r.get("ticker")}


def corp_map():
    cc = json.loads(CORP_CODES.read_text(encoding="utf-8"))
    return {k: (v.get("corp_code") if isinstance(v, dict) else v)
            for k, v in cc.items()}


def equity_values(fp):
    """BS 에서 후보 자본계정 값을 뽑는다."""
    try:
        rows = json.loads(Path(fp).read_text(encoding="utf-8"))
    except Exception:
        return {}
    out = {}
    for r in rows:
        if r.get("sj_div") != "BS":
            continue
        aid = r.get("account_id")
        a = _f(r.get("thstrm_amount"))
        if aid and a is not None and aid not in out:
            out[aid] = a
    return out


def measure_equivalence(signal_date, year_pref=("2025", "2024")):
    """frozen KRX BPS 대비 DART 파생 BPS 재현율. 성과는 계산하지 않는다."""
    snap = load_snapshot(signal_date)
    cm = corp_map()
    per = {aid: {"n": 0, "within1pct": 0, "rel": []} for aid, _ in EQUITY_ACCOUNTS}
    krx_bm, dart_bm = {}, {}
    miss_corp = miss_stmt = 0
    for t, r in snap.items():
        bps = _f(r.get("BPS"))
        sh = _f(r.get("shares"))
        cl = _f(r.get("close"))
        pbr = _f(r.get("PBR"))
        if pbr and pbr > 0:
            krx_bm[t] = 1.0 / pbr
        if not bps or bps <= 0 or not sh or sh <= 0 or not cl or cl <= 0:
            continue
        c = cm.get(t)
        if not c:
            miss_corp += 1
            continue
        eqs = None
        for yr in year_pref:
            fp = DART_ANNUAL / f"{c}_{yr}_CFS.json"
            if fp.exists():
                e = equity_values(fp)
                if e:
                    eqs = e
                    break
        if not eqs:
            miss_stmt += 1
            continue
        for aid, _nm in EQUITY_ACCOUNTS:
            if aid in eqs:
                d = abs(eqs[aid] / sh - bps) / bps
                per[aid]["n"] += 1
                per[aid]["rel"].append(d)
                if d < 0.01:
                    per[aid]["within1pct"] += 1
        best = EQUITY_ACCOUNTS[0][0]
        if best in eqs and eqs[best] > 0:
            dart_bm[t] = (eqs[best] / sh) / cl

    out = {"signalDate": signal_date, "snapshotRows": len(snap),
           "missingCorpCode": miss_corp, "missingStatement": miss_stmt,
           "byAccount": {}}
    for aid, nm in EQUITY_ACCOUNTS:
        v = per[aid]
        if not v["n"]:
            out["byAccount"][aid] = {"name": nm, "compared": 0}
            continue
        rs = sorted(v["rel"])
        out["byAccount"][aid] = {
            "name": nm, "compared": v["n"], "within1pct": v["within1pct"],
            "within1pctRate": round(100.0 * v["within1pct"] / v["n"], 2),
            "medianRelErrorPct": round(statistics.median(rs) * 100, 3),
            "p25RelErrorPct": round(rs[len(rs) // 4] * 100, 3),
            "p75RelErrorPct": round(rs[3 * len(rs) // 4] * 100, 3),
        }
    out["selectedBucket"] = selected_bucket_diff(krx_bm, dart_bm)
    out["bmCoverage"] = {"krxBmNames": len(krx_bm), "dartBmNames": len(dart_bm),
                         "common": len(set(krx_bm) & set(dart_bm)),
                         "lostIfSwitched": len(set(krx_bm) - set(dart_bm))}
    return out


def selected_bucket_diff(krx_bm, dart_bm):
    """BM 상위 분위 멤버십 차이. 이것이 결정에 직접 닿는 숫자다."""
    common = sorted(set(krx_bm) & set(dart_bm))
    if len(common) < 100:
        return {"comparable": len(common), "insufficient": True}

    def top(scores):
        ranked = [t for t, _ in sorted(((t, scores[t]) for t in common),
                                       key=lambda kv: (kv[1], kv[0]),
                                       reverse=True)]
        return set(ranked[:max(1, len(ranked) // 10)])

    a, b = top(krx_bm), top(dart_bm)
    inter = len(a & b)
    return {"comparable": len(common), "bucketSize": len(a),
            "overlap": inter, "overlapPct": round(100.0 * inter / len(a), 2),
            "mismatch": len(a) - inter,
            "jaccard": round(inter / len(a | b), 4)}


def equivalence_verdict(res):
    """§12 수용기준 대조. 비슷하다고 통과시키지 않는다."""
    best = res["byAccount"].get(EQUITY_ACCOUNTS[0][0], {})
    sb = res.get("selectedBucket", {})
    checks = {
        "selected bucket mismatch 0": sb.get("mismatch") == 0,
        "BPS 1% 이내 재현 100%": best.get("within1pctRate") == 100.0,
        "BM coverage 손실 0": res["bmCoverage"]["lostIfSwitched"] == 0,
    }
    ok = all(checks.values())
    return {"checks": checks, "equivalent": ok,
            "verdict": "PASS" if ok else "FAIL",
            "reasonClass": (None if ok else
                            "R33C0A_OPENDART_BPS_PBR_NOT_EQUIVALENT_TO_FROZEN_BM")}


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r33c0a-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    return p
