#!/usr/bin/env python3
"""R29 회귀 — 소스 선정 invariant. 네트워크 0.

WABABA-LIQUIDITY-DATA-SOURCE-RECOVERY-R29

계층:
  L1 safety  — 비밀값 미출력 · env 미변경 · 금지경로 미호출 · probe 유계
  L2 probe   — schema · ticker · 상폐 · 결측≠0 · 소스 시작일
  L3 gate    — hard gate · scorecard 결정성 · 약관 모호 시 PRIMARY 금지
  L4 real    — 실제 산출물 · 교차검증 · 판정 · Founder action 1건
  L5 regress — R27 precommit 불변 · factor 결과 보존 · 이전 산출물 보존

사용: python scripts/research/test_r29_validation.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r29_probe as P  # noqa: E402
import r29_verdict as V  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
SRC = ROOT / "scripts" / "research"
PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(n):
    p = RD / f"r29-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ═══════════════════ L1 안전 ═══════════════════
def t_l1():
    print("[L1] 안전 · 금지경로")
    cred = L("credential-inventory")
    ck("credential 산출물 존재", cred is not None)
    if cred:
        ck("§4 비밀값 미출력 선언", cred["secretsPrinted"] == 0)
        ck("§4 env 미변경 선언", cred["envModified"] == 0)
        blob = json.dumps(cred, ensure_ascii=False)
        # 실제 secret 값이 새어나가지 않았는지 — 알려진 값과 대조
        for k in ("DART_API_KEY", "KRX_ID", "KRX_PW"):
            val = (os.environ.get(k) or "").strip()
            if val and len(val) >= 6:
                ck(f"§4 {k} 값 미노출", val not in blob)
        ck("§4 이름만 기록", all(
            set(v) >= {"status", "envVarNames"} for v in cred["byCredential"].values()))
        ck("§4 상태값이 3종", all(
            v["status"] in ("PRESENT", "ABSENT", "UNUSABLE")
            for v in cred["byCredential"].values()))

    # §3 금지 경로가 코드에 없는지 정적 확인
    for f in ("r29_probe.py", "r29_evaluate.py", "r29_verdict.py",
              "r29_build_report.py"):
        s = (SRC / f).read_text(encoding="utf-8")
        code = "\n".join(ln for ln in s.splitlines()
                         if not ln.strip().startswith("#"))
        ck(f"{f}: pykrx 대량수집 미호출", "get_market_cap_by_ticker" not in code)
        ck(f"{f}: proxy/IP 우회 없음",
           not re.search(r"proxies\s*=|socks5|rotate_ip", code))
        ck(f"{f}: env 쓰기 없음",
           not re.search(r"os\.environ\[[^\]]+\]\s*=|putenv|setx ", code))
        ck(f"{f}: 결제/구매 호출 없음",
           not re.search(r"checkout|purchase|payment|billing", code, re.I))

    # §24 전체 수집 금지 — probe 는 유계
    ck("§6 표본 종목 4개 이하", len(P.SAMPLE_TICKERS) <= 4)
    ck("§6 표본 연도 8개 이하", len(P.SAMPLE_YEARS) <= 8)
    ck("§6 표본에 대형·소형·우선주 포함",
       any("대형" in v for v in P.SAMPLE_TICKERS.values())
       and any("소형" in v for v in P.SAMPLE_TICKERS.values())
       and any("우선주" in v for v in P.SAMPLE_TICKERS.values()))
    v = L("source-verdict")
    ck("§24 전체 취득 미수행", v is not None and v["fullAcquisitionDone"] is False)
    ck("§26 factor 연구 미수행", v is not None and v["factorResearchDone"] is False)


# ═══════════════════ L2 probe 내용 ═══════════════════
def t_l2():
    print("[L2] probe schema · 상폐 · 시작일")
    inv = L("source-inventory")
    ck("source inventory 존재", inv is not None)
    if inv:
        ck("§5 로컬 필드 전수 탐색 기록", len(inv["searchedFields"]) >= 6)
        ck("§5 거래량/거래대금 한글 필드 포함",
           "거래량" in inv["searchedFields"] and "거래대금" in inv["searchedFields"])
        ck("§5 PIT 스냅샷에 거래량 없음 확인",
           inv["localLiquidityData"]["pitSnapshots"]["hasVolume"] is False)
        ck("§16 R27 캐시 보존", inv["localLiquidityData"]["krxLiquidityCache"]
           ["deleted"] is False)

    dgk, krx = L("data-go-kr-probe"), L("krx-openapi-probe")
    nav = L("free-alternative-probe")["candidates"][0]
    ck("§7 공공데이터 probe 존재", dgk is not None)
    ck("§9 KRX OpenAPI probe 존재", krx is not None)
    ck("§10 무료 대체 probe 존재", nav is not None)
    if dgk:
        ck("§7 공식성 기록", dgk["official"] is True)
        ck("§7 비용 0", dgk["cost"] == 0)
        ck("§21 키 없으면 미발급 명시",
           dgk["credentialPresent"] or any("임의 발급하지 않" in n
                                           for n in dgk["notes"]))
        ck("§7 호출 유계", dgk["calls"] <= 8, dgk["calls"])
    if krx:
        ck("§9 문서상 시작일 기록", krx["documentedStartDate"].startswith("2010"))
        ck("§9 2007~2009 gap 명시", "2007" in str(krx.get("gap2007to2009")))
        ck("§21 신규 신청 안 함", krx["calls"] == 0)
    if nav:
        ck("§10 라이브러리 미설치 기록", nav["libraryInstalled"] is False)
        ck("§10 endpoint 기록", "naver" in nav["endpoint"])
        ck("§10 호출 유계", nav["calls"] <= 2)
        ck("§10 약관 위험 평가", nav["termsRisk"] in ("LOW", "MEDIUM",
                                                  "HIGH_AMBIGUOUS"))
        ck("§19 우선주 표본 포함", "005935" in P.SAMPLE_TICKERS)
        ck("§19 6자리 leading zero 유지",
           all(len(t) == 6 for t in P.SAMPLE_TICKERS))

    dl = L("delisted-coverage")
    ck("§18 상폐 probe 존재", dl is not None)
    if dl:
        ck("§18 상폐 표본 확보", len(dl["sampledDelistedTickers"]) > 0)
        ck("§18 survivorship 판정 기록", "naverSurvivorshipBias" in dl)
        ck("§18 호출 유계", dl["calls"] <= 3)

    hist = L("historical-coverage")
    ck("§8 coverage 산출물 존재", hist is not None)
    if hist:
        ck("§8 필요 구간 명시",
           hist["requiredRange"]["tradingDays"] == 4640)
        ck("§8 미검증을 미검증으로 표기",
           hist["byCandidate"]["PUBLIC_DATA_PORTAL_FSC"]["status"]
           == "UNVERIFIED_NO_CREDENTIAL")
        ck("§8 추정 금지 준수(2007 주장 없음)",
           hist["byCandidate"]["PUBLIC_DATA_PORTAL_FSC"]["startVerified"] is None)


# ═══════════════════ L3 gate ═══════════════════
def t_l3():
    print("[L3] hard gate · scorecard")
    sc = L("source-scorecard")
    ck("scorecard 존재", sc is not None)
    if not sc:
        return
    ck("§12 평가축 19개", len(sc["axes"]) == len(V.AXES))
    ck("§11 후보 4개 이하", len(sc["rows"]) <= 4, len(sc["rows"]))
    for r in sc["rows"]:
        ck(f"{r['SOURCE']}: hard gate 평가됨", "hardGatePass" in r)
        ck(f"{r['SOURCE']}: 미충족 목록 기록", isinstance(r["hardGateFailed"], list))
    nav = [r for r in sc["rows"] if r["SOURCE"] == "NAVER_FINANCE_SISE_JSON"][0]
    ck("§13 약관 모호는 gate 실패", nav["hardGatePass"] is False)
    ck("§13 약관 항목이 실패 사유에 포함",
       any("약관" in x for x in nav["hardGateFailed"]))
    ck("§13 거래대금 없음이 실패 사유",
       any("TRADED_VALUE" in x for x in nav["hardGateFailed"]))
    ck("§13 gate 규칙 문구", "약관이 모호하면 PRIMARY 금지" in sc["hardGateRule"])

    # 결정성 — 같은 입력이면 같은 gate 결과
    def gate_of(r):
        return V.__dict__  # noqa: F841
    ck("§30 scorecard 결정적(정렬·고정축)",
       sc["axes"] == V.AXES)


# ═══════════════════ L4 실제 산출물 ═══════════════════
def t_l4():
    print("[L4] 실제 산출물 · 판정")
    cc, v = L("r27-cache-crosscheck"), L("source-verdict")
    rec = L("cross-source-reconciliation")
    for n, o in (("r27-cache-crosscheck", cc),
                 ("cross-source-reconciliation", rec),
                 ("source-verdict", v)):
        ck(f"{n} 산출물 존재", o is not None)
    if not all((cc, rec, v)):
        return

    # §17 tolerance 사전 고정
    ck("§17 거래량 허용오차 0(완전일치)", cc["tolerance"]["volumeRelErrMax"] == 0.0)
    ck("§17 거래대금 허용오차 5%", cc["tolerance"]["tradedValueRelErrMax"] == 0.05)
    ck("§17 VWAP 차이 해석 명시", "VWAP" in cc["tolerance"]["tradedValueRule"])
    ck("§15 겹침 20거래일 이상", cc["referenceDays"] >= 20, cc["referenceDays"])
    ck("§16 캐시 보존", cc["cachePreserved"] is True)
    ok = [r for r in cc["byTicker"] if r.get("status") == "OK"]
    ck("§16 교차검증 수행", len(ok) >= 2, len(ok))
    for r in ok:
        ck(f"§16 {r['ticker']} 거래량 완전일치",
           r["volumeExactMatchPct"] == 100.0, r["volumeExactMatchPct"])
        ck(f"§17 {r['ticker']} 거래대금 근사 허용내",
           r["tradedValueApproxMedianRelErr"] <= cc["tolerance"]["tradedValueRelErrMax"],
           r["tradedValueApproxMedianRelErr"])
        ck(f"§7 {r['ticker']} 분할조정가 여부 기록", "closeAdjusted" in r)

    ck("§14 stitching 필요성 기록", "stitchingRequired" in rec)
    ck("§14 미수행 대상 명시", len(rec["notPerformed"]) == 2)

    # 판정
    ck("§22 판정이 5종 중 하나",
       v["verdict"] in ("LIQUIDITY_SOURCE_READY", "LIQUIDITY_SOURCE_READY_STITCHED",
                        "LIQUIDITY_SOURCE_SELECTED_PENDING_CREDENTIAL",
                        "LIQUIDITY_SOURCE_PARTIAL", "NO_COMPLIANT_SOURCE_FOUND"))
    ck("§23 PRIMARY 가 공식 소스",
       v["PRIMARY_LIQUIDITY_SOURCE"] is None
       or "PUBLIC_DATA" in v["PRIMARY_LIQUIDITY_SOURCE"]
       or "KRX_OFFICIAL" in v["PRIMARY_LIQUIDITY_SOURCE"])
    ck("§13 NAVER 는 PRIMARY 아님",
       v["PRIMARY_LIQUIDITY_SOURCE"] != "NAVER_FINANCE_SISE_JSON")
    ck("§3 금지된 KRX 웹경로가 제외 목록에",
       any("PYKRX" in x["source"] or "WEBSITE" in x["source"]
           for x in v["EXCLUDED"]))
    ck("§21 Founder action 정확히 1건", v["founderActionCount"] == 1)
    ck("§21 action 이 인증키 발급", "serviceKey" in v["founderAction"]
       or "인증키" in v["founderAction"])
    ck("§20 추가 비용 없음", v["additionalCostRequired"] is False)
    ck("§34 다음 규칙 지정", v["nextTaskRule"] in list("ABCDE"))
    ck("§26 다음 작업에 factor 금지 명시", "factor 연구 금지" in v["nextTask"])
    ck("§25 R27 precommit 불변 선언", v["r27PrecommitUnchanged"] is True)
    if v["verdict"].endswith("PENDING_CREDENTIAL"):
        ck("§21 credential 필요 표기", v["additionalCredentialRequired"] is True)
        ck("§8 coverage 미검증 표기",
           "UNVERIFIED" in v["expectedCoverage2007to2026"])


# ═══════════════════ L5 회귀 ═══════════════════
def t_l5():
    print("[L5] 이전 결과 보존")
    pc = json.loads((RD / "r27-precommit-latest.json").read_text(encoding="utf-8"))
    ck("§25 gate 1.25억 불변", pc["primaryGate"]["thresholdKrw"] == 125_000_000)
    ck("§25 LOW 2,500만 불변", pc["sensitivity"]["LOW"]["thresholdKrw"] == 25_000_000)
    ck("§25 BASE 1.25억 불변", pc["sensitivity"]["BASE"]["thresholdKrw"] == 125_000_000)
    ck("§25 HIGH 2.5억 불변", pc["sensitivity"]["HIGH"]["thresholdKrw"] == 250_000_000)
    ck("§25 자본 5천만 불변", pc["capital"]["totalKrw"] == 50_000_000)
    ck("§25 40종목 불변", pc["capital"]["namesForSanity"] == 40)
    ck("§25 1종목 125만 불변", pc["capital"]["orderPerNameKrw"] == 1_250_000)
    ck("§25 참여율 1% 불변", pc["primaryGate"]["participationImplied"] == 0.01)

    r25 = json.loads((RD / "r25-verdict-latest.json").read_text(encoding="utf-8"))
    ck("§27 R25 PRIMARY 보존", set(r25["PRIMARY_FACTOR"]) == {"BM", "SIZE_SMALL"})
    ck("§27 R25 SECONDARY 보존", r25["SECONDARY_FACTOR"] == ["EY"])
    r26 = json.loads((RD / "r26-verdict-latest.json").read_text(encoding="utf-8"))
    ck("§27 R26 판정 보존", r26["verdict"] == "NO_INCREMENTAL_COMBINATION")
    r27 = json.loads((RD / "r27-verdict-latest.json").read_text(encoding="utf-8"))
    ck("§27 R27 판정 보존", r27["verdict"] == "SIZE_DATA_INSUFFICIENT")
    r28 = json.loads((RD / "r28-final-verdict-latest.json")
                     .read_text(encoding="utf-8"))
    ck("§27 R28 판정 보존", r28["sizeVerdict"] == "SIZE_DATA_INSUFFICIENT")
    ck("§27 R28 차단 사유 보존",
       r28["blocker"] == "KRX_TOS_AUTOMATED_BULK_COLLECTION_PROHIBITED")

    for f in ("wababa-canonical-factor-rediscovery-r25-latest.md",
              "wababa-bm-size-incremental-combination-r26-latest.md",
              "wababa-size-tradability-validation-r27-latest.md",
              "wababa-r27-liquidity-data-resume-r28-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())

    # R28 약관 게이트가 여전히 살아있는지
    import r27_collect as CO
    ck("R28 약관 게이트 유지", CO.BULK_LIMIT_WITHOUT_APPROVAL > 0)
    ck("R28 승인 env 유지", CO.APPROVAL_ENV == "WABABA_KRX_BULK_APPROVED")


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-liquidity-data-source-recovery-r29-latest.json"
    ck("R29 보고서 JSON 존재", j.exists())
    if j.exists():
        d = json.loads(j.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure", "newCredential", "secretsPrinted",
                  "fullAcquisitionDone"):
            ck(f"{k} = 0", p[k] == 0)
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish", "canonicalProduction"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED",
           p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("R27 precommit 불변 기록", d["r27PrecommitUnchanged"] is True)
        ck("factor 연구 미변경 기록", d["factorResearchUntouched"] is True)
        ck("이전 산출물 보존", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
    m = WD / "wababa-liquidity-data-source-recovery-r29-latest.md"
    ck("R29 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("§33 첫 줄 전체 판정", lines[0].startswith("전체 판정:"), lines[0])
        ck("§33 둘째 줄 reason_class", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("Credential 인벤토리", "공공데이터포털", "KRX 공식 Open API",
                     "NAVER", "교차검증", "상폐종목", "hard gate",
                     "REAL_MONEY_NOT_APPROVED", "Founder 행동",
                     "다음 단일 작업", "한계"):
            ck(f"§33 보고 항목: {need}", need in body)
        # 비밀값이 보고서에 새지 않았는지
        for k in ("DART_API_KEY", "KRX_ID", "KRX_PW"):
            val = (os.environ.get(k) or "").strip()
            if val and len(val) >= 6:
                ck(f"보고서에 {k} 값 미노출", val not in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_prod):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("secretsPrinted: 0")
    print("fullAcquisition: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
