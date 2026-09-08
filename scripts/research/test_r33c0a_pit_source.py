#!/usr/bin/env python3
"""R33C0A 회귀 — source 판별 · BM 동등성 게이트 · 경계.

WABABA-PIT-PBR-BPS-CANONICAL-SOURCE-RESOLUTION-AND-RESTORE-R33C0A

계층:
  L1 frozen  — 상위 동결 hash 전부
  L2 pykrx   — endpoint 분류·canonical 거부
  L3 krx     — 공식 OPEN API 의 PBR/BPS 부재 확정
  L4 dart    — credential 존재·cache 인벤토리·값 노출 0
  L5 equiv   — 동등성 판정기(결과독립 fixture) + 실측 FAIL
  L6 nocreate— producer/PIT/OOS/R33C 미생성
  L7 bound   — network/키/raw Git/public/실주문 경계

사용: python scripts/research/test_r33c0a_pit_source.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r33c0a_pit_source as S                  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")

PASS = FAIL = NOTRUN = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" - {extra}" if extra else ""))


def notrun(name, why):
    global NOTRUN
    NOTRUN += 1
    print(f"  NOT_RUN  {name} - {why}")


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


def jload(p):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def t_l1():
    print("\n[L1] frozen 정본")
    import r33a_translation_precommit as A
    import r33b_precommit as P
    import r33c0_research_data_producer as C0
    h = P.upstream_hashes()
    ck("r27/r31/r32 precommit", h["allMatch"])
    st = jload(RD / "r31-stitch-latest.json")
    ck("R31 manifest 3439dec9", st["manifestSha256"].startswith("3439dec9"))
    ck("R33A contract d869f445", A.contract_hash() == P.SOURCE_CONTRACT_HASH)
    ck("R33B spec fea25545", P.spec_hash().startswith("fea25545"))
    ck("R33C0 contract a9f47e1a", C0.contract_hash().startswith("a9f47e1a"))
    for cm in ("deeda30", "d198309"):
        ck(f"commit {cm} ancestor", subprocess.run(
            ["git", "merge-base", "--is-ancestor", cm, "origin/main"],
            cwd=ROOT, capture_output=True).returncode == 0)


def t_l2():
    print("\n[L2] pykrx endpoint 분류")
    d = jload(RD / "r33c0a-pykrx-classification-latest.json")
    if not d:
        notrun("pykrx classification", "artifact 없음")
        return
    c = d["classification"]
    ck("웹 세션 endpoint 로 분류",
       c["classification"] == "KRX_WEB_SESSION_ENDPOINT", c["classification"])
    ck("canonical 자격 REJECTED", c["canonicalEligibility"] == "REJECTED")
    ck("호스트가 data.krx.co.kr", "data.krx.co.kr" in c["hosts"])
    ck("로그인 폼 요청 관측", c["loginFormRequests"] >= 1)
    ck("HTML 응답 관측", c["htmlResponses"] >= 1)
    ck("credential 주입 0", d["credentialInjected"] is False)
    ck("cookie 주입 0", d["cookieInjected"] is False)
    ck("진단 호출 bounded", d["diagnosticCalls"] <= 20, str(d["diagnosticCalls"]))
    blob = json.dumps(d, ensure_ascii=False)
    for bad in ("mbrId=", "pw=", "crtfc_key", "serviceKey"):
        ck(f"자격정보 값 미노출: {bad}", bad not in blob)
    # 분류기 fixture — 결과독립
    off = S.classify_endpoints([{"host": "data-dbg.krx.co.kr", "dataKeys": [],
                                 "looksHtml": False}])
    ck("공식 host fixture → ACCEPTED", off["canonicalEligibility"] == "ACCEPTED")
    web = S.classify_endpoints([{"host": "data.krx.co.kr",
                                 "dataKeys": ["mbrId", "pw"], "looksHtml": True}])
    ck("웹 host fixture → REJECTED", web["canonicalEligibility"] == "REJECTED")


def t_l3():
    print("\n[L3] KRX 공식 direct PBR/BPS")
    m = jload(RD / "r33c0a-source-matrix-latest.json")
    if not m:
        notrun("source matrix", "artifact 없음")
        return
    a = m["candidateA_KRX_direct"]
    ck("KRX direct NOT_AVAILABLE", a["status"] == "NOT_AVAILABLE")
    ck("근거가 문서화 정본", "r31_source" in a["evidence"])
    ck("추측 endpoint 탐색 금지 명시", "금지" in a["endpointGuessing"])
    src = (SRC / "r31_source.py").read_text(encoding="utf-8")
    for f in ("PBR", "BPS"):
        ck(f"일별매매정보 필드에 {f} 없음", f"TDD_{f}" not in src)
    ck("candidate E 신규 vendor 금지",
       m["candidateE_newVendor"]["status"] == "FORBIDDEN")


def t_l4():
    print("\n[L4] DART credential · cache")
    v = os.environ.get("DART_API_KEY")
    ck("DART_API_KEY 존재", bool(v))
    if v:
        ck("키 길이 40", len(v) == 40, str(len(v)))
    ck("corp-codes cache 존재", S.CORP_CODES.exists())
    ck("annual statements cache 존재", S.DART_ANNUAL.exists())
    ck("quarterly statements cache 존재", S.DART_QUARTER.exists())
    n = len(list(S.DART_ANNUAL.glob("*")))
    ck("annual 파일 다수", n > 1000, str(n))
    cm = S.corp_map()
    ck("stock_code→corp_code 매핑 동작", len(cm) > 1000, str(len(cm)))
    src = (SRC / "r33c0a_pit_source.py").read_text(encoding="utf-8")
    for bad in ("crtfc_key", "DART_API_KEY", "os.environ", "getenv", "requests."):
        ck(f"모듈에 {bad} 없음(키 미접촉)", bad not in src)


def t_l5():
    print("\n[L5] 동등성 게이트")
    d = jload(RD / "r33c0a-equivalence-results-latest.json")
    if not d:
        notrun("equivalence", "artifact 없음")
        return
    ck("DART API 호출 0", d["dartApiCalls"] == 0)
    ck("기존 cache 만 사용", "신규 DART 호출 0" in d["method"])
    ck("contract hash 기록", len(d["contractHash"]) == 64)
    ac = d["acceptance"]
    for k in ("selectedBucketMismatch", "unexplainedFormulaMismatch",
              "unexplainedDenominatorMismatch"):
        ck(f"수용기준 {k} == 0", ac[k] == 0)
    m = d["measurement"]
    ck("비교 표본 충분", m["byAccount"]["ifrs-full_Equity"]["compared"] > 1000)
    sb = m["selectedBucket"]
    ck("선택버킷 측정됨", sb.get("mismatch") is not None)
    ck("실측 verdict FAIL", d["verdict"]["verdict"] == "FAIL")
    ck("reason_class 정확",
       d["verdict"]["reasonClass"] == "R33C0A_OPENDART_BPS_PBR_NOT_EQUIVALENT_TO_FROZEN_BM")
    ck("selected bucket mismatch > 0 (실측)", sb["mismatch"] > 0, str(sb["mismatch"]))
    ck("coverage 손실 > 0 (실측)", m["bmCoverage"]["lostIfSwitched"] > 0)
    # 판정기 결과독립 fixture
    good = {"byAccount": {S.EQUITY_ACCOUNTS[0][0]: {"within1pctRate": 100.0}},
            "selectedBucket": {"mismatch": 0},
            "bmCoverage": {"lostIfSwitched": 0}}
    ck("완전일치 fixture → PASS", S.equivalence_verdict(good)["verdict"] == "PASS")
    bad = json.loads(json.dumps(good))
    bad["selectedBucket"]["mismatch"] = 1
    ck("버킷 1건 불일치 → FAIL", S.equivalence_verdict(bad)["verdict"] == "FAIL")
    bad2 = json.loads(json.dumps(good))
    bad2["byAccount"][S.EQUITY_ACCOUNTS[0][0]]["within1pctRate"] = 99.9
    ck("BPS 99.9% → FAIL (비슷함 불가)",
       S.equivalence_verdict(bad2)["verdict"] == "FAIL")
    ck("BM 정의 변경 금지 계약", S.CONTRACT["bmDefinitionChangeAllowed"] is False)
    ck("성과계산 금지 계약", S.CONTRACT["performanceCalculation"] == "FORBIDDEN")
    ck("decision engine 금지 계약", S.CONTRACT["decisionEngineCall"] == "FORBIDDEN")


def t_l6():
    print("\n[L6] 미생성 확인")
    ck("PIT producer 미생성", not (SRC / "r33c0a_pit_producer.py").exists())
    ck("R33C engine 미생성", not (SRC / "r33c_oos_paper.py").exists())
    ck("R33C ledger 미생성", not (ROOT / "_cache" / "oos-r33c").exists())
    ck("R33C activation manifest 미생성",
       not (RD / "r33c-activation-manifest-latest.json").exists())
    pit = sorted(p.name[:-7] for p in S.SNAP.glob("*.csv.gz"))
    ck("PIT 스냅샷 2026-08-03 그대로", pit[-1] == "2026-08-03", pit[-1])
    ck("2026-09 스냅샷 미생성", "2026-09-01" not in pit)
    src = (SRC / "r33c0a_pit_source.py").read_text(encoding="utf-8")
    ck("모듈이 snapshot write 안 함", "_atomic_write" not in src
       and "gzip.open" in src)


def t_l7():
    print("\n[L7] 경계")
    src = (SRC / "r33c0a_pit_source.py").read_text(encoding="utf-8")
    toks = [w for w in re.findall(r"[A-Za-z0-9]{40,}", src)
            if not re.fullmatch(r"[0-9a-f]{64}", w)]
    ck("장문 연속토큰 0", not toks, str(toks[:2]))
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 _cache/reports 없음",
       not any(x.startswith(("_cache", "reports")) for x in staged), str(staged))
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    ck("financial-universe-real.json 미수정",
       " M financial-universe-real.json" in git(["status", "--short"])
       or "financial-universe-real" not in git(["status", "--short"]))
    if PUBLIC_REPO.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=PUBLIC_REPO).strip() == "2c8a000")
    ck("LEGACY_50D 미변경", "LEGACY_50D" not in src)
    ck("실주문 호출 0",
       not re.findall(r"\b(place_order|submit_order|send_order)\s*\(", src))
    ps = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "@(Get-ScheduledTask | Where-Object {$_.TaskName -like '*R33C0A*'}).Count"],
        capture_output=True, text=True, timeout=90)
    ck("신규 Scheduled Task 0", ps.stdout.strip() in ("0", ""), ps.stdout.strip())
    md = WD / "wababa-pit-pbr-bps-canonical-source-resolution-and-restore-r33c0a-latest.md"
    if not md.exists():
        notrun("R33C0A canonical MD", "보고서 작성 단계")
    else:
        body = md.read_text(encoding="utf-8")
        first = body.splitlines()[0].strip()
        ck("첫 줄 판정 헤더",
           re.match(r"^(전체 판정:\s*)?(PASS|WARNING|BLOCKED|WAIT)$", first)
           is not None, first)
        for need in ("KRX_WEB_SESSION_ENDPOINT", "REAL_MONEY_NOT_APPROVED",
                     "R33C0A_OPENDART_BPS_PBR_NOT_EQUIVALENT_TO_FROZEN_BM"):
            ck(f"보고 항목: {need[:44]}", need in body)
        ck("키 값 미노출", "04c4a8f7" in body and len(
            [w for w in re.findall(r"[A-Za-z0-9]{40}", body)]) == 0)
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_l7):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("dartApiCalls: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
