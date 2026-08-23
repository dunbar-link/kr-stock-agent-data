#!/usr/bin/env python3
"""R29 소스 scorecard + 최종 판정.

WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29

§12 평가표 · §13 hard gate · §22 verdict · §23 우선순위 를 그대로 적용한다.
약관이 모호하면 PRIMARY 로 선정하지 않는다(§13).

안전: 계산·읽기 전용. 네트워크 0. 결제 0. 신규 credential 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r29_probe import save  # noqa: E402

RD = Path(__file__).resolve().parents[2] / "reports" / "research"

# §12 평가 축 — 모든 후보를 같은 표로 본다.
AXES = ["SOURCE", "OFFICIALITY", "AUTOMATION_ALLOWED", "CREDENTIAL_REQUIRED",
        "COST", "START_DATE", "END_DATE", "KOSPI", "KOSDAQ",
        "DELISTED_HISTORY", "VOLUME", "TRADED_VALUE", "PIT_USABLE",
        "DAILY_BULK_CAPABILITY", "REQUEST_LIMIT", "REPRODUCIBILITY",
        "TERMS_RISK", "IMPLEMENTATION_COST", "FOUNDER_ACTION"]


def L(n):
    p = RD / f"r29-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def main() -> int:
    cred = L("credential-inventory")["byCredential"]
    dgk, krx = L("data-go-kr-probe"), L("krx-openapi-probe")
    nav = L("free-alternative-probe")["candidates"][0]
    cc = L("r27-cache-crosscheck")
    dl = L("delisted-coverage")
    hist = L("historical-coverage")

    vmatch = [r.get("volumeExactMatchPct") for r in cc["byTicker"]
              if r.get("volumeExactMatchPct") is not None]
    tverr = [r.get("tradedValueApproxMedianRelErr") for r in cc["byTicker"]
             if r.get("tradedValueApproxMedianRelErr") is not None]

    rows = [
        {"SOURCE": "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE",
         "OFFICIALITY": "OFFICIAL (금융위원회 / 공공데이터포털)",
         "AUTOMATION_ALLOWED": "YES — 공공데이터 개방, 이용허락범위 제한 없음",
         "CREDENTIAL_REQUIRED": "YES — serviceKey",
         "CREDENTIAL_STATUS": cred["DATA_GO_KR_SERVICE_KEY"]["status"],
         "COST": 0,
         "START_DATE": "UNVERIFIED (키 부재로 실측 불가)",
         "END_DATE": "UNVERIFIED",
         "KOSPI": "UNVERIFIED", "KOSDAQ": "UNVERIFIED",
         "DELISTED_HISTORY": "UNVERIFIED",
         "VOLUME": "문서상 제공 — UNVERIFIED",
         "TRADED_VALUE": "문서상 제공 — UNVERIFIED",
         "PIT_USABLE": "일자 기준 조회 — 구조상 가능",
         "DAILY_BULK_CAPABILITY": "일자별 전종목 조회 지원(문서) — UNVERIFIED",
         "REQUEST_LIMIT": "개발계정 일일 트래픽 제한 존재 — UNVERIFIED",
         "REPRODUCIBILITY": "HIGH (REST, 문서화)",
         "TERMS_RISK": "LOW",
         "IMPLEMENTATION_COST": "LOW — 기존 requests 로 충분",
         "FOUNDER_ACTION": "serviceKey 발급 1건(무료·자동승인)",
         "endpointReachable": dgk.get("endpointReachable"),
         "probeHttpStatus": (dgk.get("unauthenticatedProbe") or {}).get("httpStatus"),
         "probeError": (dgk.get("unauthenticatedProbe") or {}).get("bodyHead", "")[:120]},

        {"SOURCE": "KRX_OFFICIAL_OPEN_API",
         "OFFICIALITY": "OFFICIAL (한국거래소 openapi.krx.co.kr)",
         "AUTOMATION_ALLOWED": "YES — 자동화 전제 공식 서비스",
         "CREDENTIAL_REQUIRED": "YES — AUTH_KEY (가입·신청·승인 절차)",
         "CREDENTIAL_STATUS": cred["KRX_OPENAPI_AUTH_KEY"]["status"],
         "COST": 0,
         "START_DATE": "2010-01-04 (문서상 유가증권 일별매매정보)",
         "END_DATE": "현재",
         "KOSPI": "YES(문서)", "KOSDAQ": "별도 API 필요 가능 — UNVERIFIED",
         "DELISTED_HISTORY": "UNVERIFIED",
         "VOLUME": "제공(문서)", "TRADED_VALUE": "제공(문서)",
         "PIT_USABLE": "일자 기준 — 가능",
         "DAILY_BULK_CAPABILITY": "일별 전종목 — 가능(문서)",
         "REQUEST_LIMIT": "UNVERIFIED",
         "REPRODUCIBILITY": "HIGH",
         "TERMS_RISK": "LOW",
         "IMPLEMENTATION_COST": "LOW",
         "FOUNDER_ACTION": "AUTH_KEY 신청 1건(무료, 승인 절차 있음)",
         "gap": krx.get("gap2007to2009")},

        {"SOURCE": "NAVER_FINANCE_SISE_JSON",
         "OFFICIALITY": "UNOFFICIAL (웹서비스 내부 endpoint)",
         "AUTOMATION_ALLOWED": "명시적 허용 근거 확인 못함",
         "CREDENTIAL_REQUIRED": "NO",
         "CREDENTIAL_STATUS": "N/A",
         "COST": 0,
         "START_DATE": "2007-01-02 실측 확인",
         "END_DATE": "현재(실측 범위 내)",
         "KOSPI": "YES(실측)", "KOSDAQ": "UNVERIFIED",
         "DELISTED_HISTORY": ("YES — 상폐 표본 "
                              f"{sum(1 for r in dl['probed'] if r['hasHistory'])}"
                              f"/{len(dl['probed'])} 이력 반환"),
         "VOLUME": f"YES — R27 KRX 캐시와 정확 일치 {vmatch}%",
         "TRADED_VALUE": "NO — 컬럼 없음. 종가×거래량 근사만 가능",
         "PIT_USABLE": "주가가 **분할조정가** — 당시 실거래가 아님(§12 위배 소지)",
         "DAILY_BULK_CAPABILITY": "종목별 기간조회 — 전종목 일괄 아님",
         "REQUEST_LIMIT": "미공개",
         "REPRODUCIBILITY": "MEDIUM (비문서화 endpoint, 변경 위험)",
         "TERMS_RISK": nav.get("termsRisk"),
         "IMPLEMENTATION_COST": "MEDIUM — 종목×기간 루프 필요",
         "FOUNDER_ACTION": "없음",
         "volumeExactMatchPct": vmatch,
         "tradedValueApproxMedianRelErr": tverr,
         "libraryInstalled": nav.get("libraryInstalled")},
    ]

    # ── §13 hard gate ─────────────────────────────────────────────────
    def gate(r):
        checks = {
            "AUTOMATION_ALLOWED=YES": r["AUTOMATION_ALLOWED"].startswith("YES"),
            "무료 또는 기존 승인비용": r["COST"] == 0,
            "historical coverage 충분": "UNVERIFIED" not in str(r["START_DATE"])
            and "2007" in str(r["START_DATE"]),
            "VOLUME 존재": str(r["VOLUME"]).startswith("YES"),
            "TRADED_VALUE 직접 또는 신뢰가능": str(r["TRADED_VALUE"]).startswith(
                ("YES", "제공")),
            "PIT 사용 가능": "위배" not in str(r["PIT_USABLE"]),
            "약관 명확(LOW risk)": r["TERMS_RISK"] == "LOW",
            "credential 확보": r.get("CREDENTIAL_STATUS") in ("PRESENT", "N/A"),
        }
        return checks

    for r in rows:
        c = gate(r)
        r["hardGate"] = c
        r["hardGatePass"] = all(c.values())
        r["hardGateFailed"] = [k for k, v in c.items() if not v]

    save("source-scorecard", {
        "task": "R29", "axes": AXES, "rows": rows,
        "hardGateRule": ("PRIMARY 는 AUTOMATION_ALLOWED · 무료 · coverage · VOLUME · "
                         "TRADED_VALUE · PIT · 약관명확 · credential 을 모두 "
                         "충족해야 한다. 약관이 모호하면 PRIMARY 금지(§13)."),
        "anyPass": any(r["hardGatePass"] for r in rows)})

    # ── §22·§23 판정 ──────────────────────────────────────────────────
    passed = [r for r in rows if r["hardGatePass"]]
    official_pending = [r for r in rows
                        if r["OFFICIALITY"].startswith("OFFICIAL")
                        and r.get("CREDENTIAL_STATUS") == "ABSENT"]

    if passed:
        verdict = "LIQUIDITY_SOURCE_READY"
        primary = passed[0]["SOURCE"]
        why = ["hard gate 전 항목 충족"]
        action = "없음"
    elif official_pending:
        verdict = "LIQUIDITY_SOURCE_SELECTED_PENDING_CREDENTIAL"
        # §23 우선순위 1 — 공식 공공 API + 전체기간 가능성
        primary = "PUBLIC_DATA_PORTAL_FSC_STOCK_PRICE"
        why = [
            "두 공식 후보 모두 credential 이 없어 coverage·필드를 실측하지 못했다.",
            "§21 에 따라 신규 키를 임의 발급하지 않았다.",
            "§23 우선순위 1(공식 공공 API)에 따라 공공데이터포털을 선정한다 — "
            "무료·자동승인·이용허락범위 제한 없음.",
            "NAVER 는 데이터 품질이 좋았지만 약관이 모호해 §13 으로 PRIMARY 불가.",
        ]
        action = ("공공데이터포털(data.go.kr) 회원가입 후 "
                  "'금융위원회_주식시세정보' 활용신청 → serviceKey 발급 1건")
    else:
        verdict = "NO_COMPLIANT_SOURCE_FOUND"
        primary = None
        why = ["hard gate 를 통과한 후보가 없다."]
        action = "유료 source ROI 비교를 다음 작업으로 검토"

    nav_row = rows[2]
    save("source-verdict", {
        "task": "R29", "verdict": verdict,
        "PRIMARY_LIQUIDITY_SOURCE": primary,
        "primaryStatus": "PENDING_CREDENTIAL" if verdict.endswith(
            "PENDING_CREDENTIAL") else "READY",
        "SECONDARY_FALLBACK": {
            "source": "KRX_OFFICIAL_OPEN_API",
            "role": "2010-01-04 이후 구간 stitching 후보",
            "credentialStatus": cred["KRX_OPENAPI_AUTH_KEY"]["status"],
            "gap": krx.get("gap2007to2009")},
        "EXCLUDED": [{
            "source": "NAVER_FINANCE_SISE_JSON",
            "dataQuality": {
                "volumeExactMatchPct": vmatch,
                "tradedValueApproxMedianRelErr": tverr,
                "reached2007": True,
                "delistedHistory": True},
            "excludedBecause": nav_row["hardGateFailed"],
            "note": ("데이터 품질은 실측상 우수했다(거래량 완전 일치). 그러나 "
                     "거래대금 컬럼이 없고 주가가 분할조정가이며, 무엇보다 "
                     "자동화 이용을 명시적으로 허용한 약관 근거가 없다. "
                     "R28 에서 같은 성격의 비공식 경로가 약관 위반으로 차단된 "
                     "전례가 있어 PRIMARY 로 쓰지 않는다.")},
            {"source": "KRX_WEBSITE_VIA_PYKRX",
             "excludedBecause": ["R28 약관 위반 통보 — 자동화 대량수집 금지"],
             "note": "credential 은 있으나(KRX_ID/PW) 그 경로 자체가 금지됐다."}],
        "termsVerdict": ("PRIMARY 후보는 공공데이터 개방 라이선스로 자동화 이용이 "
                         "명시 허용된다. 약관 위험 LOW."),
        "expectedCoverage2007to2026": "UNVERIFIED — credential 확보 후 R30 에서 실측",
        "fullAcquisitionPossible": "PENDING_CREDENTIAL",
        "additionalCredentialRequired": True,
        "additionalCostRequired": False,
        "founderAction": action,
        "founderActionCount": 1,
        "stitchingRequired": ("공공데이터포털이 2007 까지 못 덮으면 KRX Open API"
                              "(2010~)와 stitching 필요. 그 경우 겹침 20거래일 "
                              "cross-check 필수(§15)."),
        "nextTaskRule": "C",
        "nextTask": ("Founder 가 serviceKey 를 발급하면 "
                     "WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30 에서 "
                     "연도별 coverage 실측 → 전체 취득. 새 factor 연구 금지."),
        "fullAcquisitionDone": False,
        "factorResearchDone": False,
        "r27PrecommitUnchanged": True})

    print(json.dumps({"verdict": verdict, "primary": primary,
                      "founderActions": 1,
                      "gatePassed": [r["SOURCE"] for r in passed],
                      "naverExcludedBecause": nav_row["hardGateFailed"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
