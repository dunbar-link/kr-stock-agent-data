#!/usr/bin/env python3
"""R30 회귀 — 공식 소스 수집 invariant. 네트워크 0.

WABABA-LIQUIDITY-OFFICIAL-FULL-ACQUISITION-R30

계층:
  L1 safety  — 비밀값 미출력 · env 미변경 · KRX 금지경로 미사용 · 값 미커밋
  L2 schema  — 필드 탐색 · 거래량/거래대금 · 결측≠0 · ticker semantics
  L3 cache   — 캐시 왕복 · 원자적 write · R27 투영 · checkpoint/resume
  L4 gate    — probe gate · coverage threshold 불변 · 미달 시 분석 금지
  L5 real    — 실제 산출물 · 판정 · reason_class · Founder action
  L6 regress — R27 precommit 불변 · R25/R26 재현 · 이전 산출물 보존

credential 이 없어 실호출로만 확인 가능한 항목은 **통과시키지 않고**
NOT_RUN_CREDENTIAL_ABSENT 로 표시한다. 없는 검증을 PASS 로 위장하지 않는다.

사용: python scripts/research/test_r30_validation.py
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r30_cache as K       # noqa: E402
import r30_probe as P       # noqa: E402
import r30_source as S      # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
SRC = ROOT / "scripts" / "research"
PASS = FAIL = SKIP = 0
CRED = S.credential_status()["status"] == "PRESENT"


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def need_cred(name):
    """실호출이 필요한 검증 — 없는 것을 PASS 로 위장하지 않는다."""
    global SKIP
    SKIP += 1
    print(f"  SKIP  {name}  [NOT_RUN_CREDENTIAL_ABSENT]")


def L(n):
    p = RD / f"r30-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ══════════════════ L1 안전 ══════════════════
def t_l1():
    print("[L1] 안전 · 비밀값 · 금지경로")
    cred = L("credential-status")
    ck("credential 산출물 존재", cred is not None)
    if cred:
        ck("§1 비밀값 미출력 선언", cred.get("secretExposure") == 0)
        ck("§1 credential 값 필드 없음",
           "serviceKey" not in json.dumps(cred)
           and "value" not in {k.lower() for k in cred})
        ck("§1 상태는 PRESENT/ABSENT 만",
           cred.get("status") in ("PRESENT", "ABSENT"))

    # 실제 secret 값이 어떤 R30 산출물·소스에도 없는지
    secrets = [v for k in ("DART_API_KEY", "KRX_ID", "KRX_PW",
                           *S.CRED_ENV_ALIASES)
               if len(v := (os.environ.get(k) or "").strip()) >= 8]
    blobs = []
    for p in list(RD.glob("r30-*-latest.json")) + list(SRC.glob("r30_*.py")) \
            + list(WD.glob("*r30*")):
        try:
            blobs.append((p.name, p.read_text(encoding="utf-8", errors="replace")))
        except OSError:
            pass
    leaked = [(n, ) for n, b in blobs for s in secrets if s in b]
    ck("§1 산출물·소스에 secret 값 미노출 0건", not leaked, str(leaked[:3]))

    # KRX 웹 자동화(R28 금지경로) 미사용
    src = "\n".join(b for n, b in blobs if n.endswith(".py"))
    ck("§R28 pykrx 대량수집 미사용", "get_market_cap_by_ticker" not in src)
    ck("§R28 KRX 웹 로그인 미사용",
       "KRX_ID" not in src and "KRX_PW" not in src)
    # §36 실주문 경로 없음 — 단어 유무가 아니라 **실제 나가는 호스트**로 본다.
    #   (production 선언 dict 에 "broker": 0 같은 문자열이 있는 것은 정상이다.)
    import re as _re
    hosts = {m.group(1) for m in _re.finditer(r"https?://([A-Za-z0-9.\-]+)", src)}
    ck("§36 외부 호출 호스트는 공식 API 하나뿐",
       hosts <= {"apis.data.go.kr"}, str(sorted(hosts)))
    ck("§36 주문 실행 API 미사용",
       not any(w in src for w in ("place_order", "send_order", "order_cash",
                                  "TradeAPI", "kiwoom", "ebest", "creon")))
    meta = S.source_meta()
    ck("§4 endpoint 는 R29 확정 소스", meta["endpoint"] == S.ENDPOINT
       and "apis.data.go.kr/1160100" in meta["endpoint"])
    ck("§4 공식·무료·자동화 허용", meta["cost"] == 0
       and meta["automationAllowed"].startswith("YES"))
    ck("§36 신규 env 구조 미생성 (기존 규약 재사용)",
       S.CRED_ENV_PRIMARY == "DATA_GO_KR_SERVICE_KEY"
       and S.ENV_LOCAL.name == ".env.local")

    # .env.local 은 gitignore 대상이어야 한다
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8", errors="replace")
    ck("§1 .env.local gitignore 대상", ".env.local" in gi)


# ══════════════════ L2 스키마 ══════════════════
ITEM = {"basDt": "20070102", "srtnCd": "005930", "isinCd": "KR7005930003",
        "itmsNm": "삼성전자", "mrktCtg": "KOSPI", "clpr": "607000",
        "mkp": "600000", "hipr": "610000", "lopr": "598000",
        "trqu": "381464", "trPrc": "231243411000",
        "lstgStCnt": "147299337", "mrktTotAmt": "89410597559000"}


def t_l2():
    print("[L2] 스키마 · 필드 · ticker · 결측")
    f = S.resolve_fields(ITEM)
    ck("§5 거래량 필드 탐색", f.get("volume") == "trqu")
    ck("§5 거래대금 필드 탐색", f.get("tradedValue") == "trPrc")
    for canon in S.REQUIRED_FIELDS:
        ck(f"§5 필수 필드 탐색: {canon}", canon in f)

    r = S.normalize(ITEM, "2007-01-02")
    ck("§5 거래량 파싱", r["volume"] == 381464.0)
    ck("§5 거래대금 파싱", r["traded_value"] == 231243411000.0)
    ck("§5 거래대금은 close×volume 아님",
       r["traded_value"] != r["close"] * r["volume"])
    ck("§14 schema 전체 키", set(K.COLS) - set(r) == {"retrieved_at"})
    ck("§8 6자리 ticker", r["ticker"] == "005930" and len(r["ticker"]) == 6)

    # 우선주 · leading zero
    pref = S.normalize({**ITEM, "srtnCd": "005935", "itmsNm": "삼성전자우"},
                       "2007-01-02")
    ck("§8 우선주 코드 보존", pref["ticker"] == "005935")
    lz = S.normalize({**ITEM, "srtnCd": "60"}, "2007-01-02")
    ck("§8 leading zero 복원", lz["ticker"] == "000060")
    ap = S.normalize({**ITEM, "srtnCd": "A005930"}, "2007-01-02")
    ck("§8 'A' 접두 제거", ap["ticker"] == "005930")

    # 결측 vs 0 (§14·§25)
    miss = S.normalize({**ITEM, "trqu": "", "trPrc": "-"}, "2007-01-02")
    ck("§14 결측은 None", miss["volume"] is None and miss["traded_value"] is None)
    ck("§14 결측 quality_flag", "VOLUME_MISSING" in miss["quality_flag"]
       and "TRADED_VALUE_MISSING" in miss["quality_flag"])
    zero = S.normalize({**ITEM, "trqu": "0", "trPrc": "0"}, "2007-01-02")
    ck("§25 실제 0 은 0", zero["volume"] == 0.0)
    ck("§25 zero 와 missing 구분", "ZERO_VOLUME" in zero["quality_flag"])

    # 필드가 아예 없으면 fail-closed (§5)
    try:
        S.normalize({"basDt": "20070102", "srtnCd": "005930", "clpr": "1"},
                    "2007-01-02")
        ck("§5 필수 필드 부재 시 fail-closed", False, "예외 없음")
    except S.SchemaMismatch:
        ck("§5 필수 필드 부재 시 fail-closed", True)

    ck("§1 scrub 이 serviceKey 를 가린다",
       "<redacted>" in S._scrub("https://x?serviceKey=ABC123&a=1"))


# ══════════════════ L3 캐시 · checkpoint ══════════════════
def t_l3():
    print("[L3] 캐시 왕복 · R27 투영 · checkpoint/resume")
    rows = [S.normalize(ITEM, "2007-01-02"),
            S.normalize({**ITEM, "srtnCd": "005935", "trqu": "0", "trPrc": "0"},
                        "2007-01-02"),
            S.normalize({**ITEM, "srtnCd": "000660", "trqu": "", "trPrc": ""},
                        "2007-01-02")]
    with tempfile.TemporaryDirectory() as td:
        orig_c, orig_r, orig_k = K.CACHE, K.R27_CACHE, K.CKPT
        try:
            K.CACHE = Path(td) / "official"
            K.R27_CACHE = Path(td) / "r27"
            K.CKPT = K.CACHE / "_checkpoint.json"
            K.write_day("2007-01-02", rows)
            back = K.load_day("2007-01-02")
            ck("§14 캐시 왕복 행수", back is not None and len(back) == 3)
            ck("§14 왕복 후 거래대금 보존",
               back["005930"]["traded_value"] == 231243411000.0)
            ck("§14 왕복 후 결측은 None",
               back["000660"]["volume"] is None)
            ck("§25 왕복 후 0 은 0", back["005935"]["volume"] == 0.0)
            ck("§14 provenance 보존",
               back["005930"]["source"] == S.SOURCE
               and back["005930"]["source_version"] == S.SOURCE_VERSION
               and bool(back["005930"]["retrieved_at"]))
            ck("§14 quality_flag 기록",
               back["000660"]["quality_flag"].startswith("VOLUME_MISSING"))

            # R27 투영 — 결측 종목은 빠지고 형식이 맞아야 한다
            st = K.project_to_r27("2007-01-02", rows)
            ck("§12 R27 투영 write", st == "WROTE")
            p = K.R27_CACHE / "2007-01-02.csv.gz"
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                head = fh.readline().strip().split(",")
            ck("§12 R27 컬럼 일치", head == K.R27_COLS, str(head))
            st2 = K.project_to_r27("2007-01-02", rows)
            ck("§12 기존 R27 파일 보존", st2 == "KEPT_EXISTING")

            # checkpoint / resume
            need = [f"2007-01-{d:02d}" for d in range(2, 12)]
            c1 = K.load_checkpoint(need)
            ck("§15 checkpoint 필드",
               all(k in c1 for k in ("required_days", "completed_days",
                                     "pending_days", "failed_days",
                                     "retry_count", "last_success",
                                     "last_error_class")))
            c1["completed_days"] = ["2007-01-02"]
            c1["last_success"] = "2007-01-02"
            K.save_checkpoint(c1)
            c2 = K.load_checkpoint(need)
            ck("§15 resume 완료분 유지", c2["completed_days"] == ["2007-01-02"])
            ck("§15 resume pending 만 남음",
               "2007-01-02" not in c2["pending_days"]
               and len(c2["pending_days"]) == 9)
            c3 = K.sync_checkpoint_from_disk(K.load_checkpoint(need), need)
            ck("§15 디스크 정본 동기화",
               c3["completed_days"] == ["2007-01-02"])
            ck("§15 처음부터 재수집 안 함", len(c3["pending_days"]) == 9)
        finally:
            K.CACHE, K.R27_CACHE, K.CKPT = orig_c, orig_r, orig_k


# ══════════════════ L4 gate ══════════════════
def t_l4():
    print("[L4] probe gate · coverage threshold")
    from r27_precommit import CAPITAL, COVERAGE, PRIMARY_GATE, SENSITIVITY
    ck("§16 연도 coverage threshold 90 불변",
       COVERAGE["minCoveragePctPerYear"] == 90.0)
    ck("§16 최소 연도수 15 불변", COVERAGE["minYearsCovered"] == 15)
    ck("§19 PRIMARY gate 1.25억 불변",
       PRIMARY_GATE["thresholdKrw"] == 125_000_000)
    ck("§19 sensitivity LOW/BASE/HIGH 불변",
       (SENSITIVITY["LOW"]["thresholdKrw"], SENSITIVITY["BASE"]["thresholdKrw"],
        SENSITIVITY["HIGH"]["thresholdKrw"])
       == (25_000_000, 125_000_000, 250_000_000))
    ck("§19 자본·종목수·1종목 주문액 불변",
       (CAPITAL["totalKrw"], CAPITAL["namesForSanity"],
        CAPITAL["orderPerNameKrw"]) == (50_000_000, 40, 1_250_000))

    cred_absent = {"status": "ABSENT"}
    empty_hist = {"byYear": {}, "fullPeriodPass": False}
    empty = {"volumeFieldPresent": False, "tradedValueFieldPresent": False}
    dp = {"delistedHistorySupported": False, "preferredSupported": False,
          "leadingZeroPreserved": False}
    cc = {"status": "NOT_RUN"}
    v = P.probe_verdict(cred_absent, empty_hist, empty, dp, cc, True)
    ck("§11-E credential 부재 → CREDENTIAL_ABSENT",
       v["verdict"] == "CREDENTIAL_ABSENT")
    ck("§10 credential 부재 시 전체수집 금지",
       v["fullAcquisitionAllowed"] is False)

    # §11-B 2010+ 만 있는 경우
    hist_b = {"byYear": {"2007": {"rows": 0}, "2008": {"rows": 0},
                         "2009": {"rows": 0}, "2010": {"rows": 100}},
              "fullPeriodPass": False}
    vb = P.probe_verdict({"status": "PRESENT"}, hist_b, empty, dp, cc, True)
    ck("§11-B 2007~2009 부재 → 2010_PLUS_ONLY",
       vb["verdict"] == "PROBE_PASS_2010_PLUS_ONLY")
    ck("§11-B 그래도 전체수집 금지", vb["fullAcquisitionAllowed"] is False)

    # §11-C 거래대금 없음
    hist_ok = {"byYear": {str(y): {"rows": 10, "marketCounts":
                                   {"KOSPI": 5, "KOSDAQ": 5}}
                          for y in P.HARD_YEARS}, "fullPeriodPass": True}
    vc = P.probe_verdict({"status": "PRESENT"}, hist_ok,
                         {"volumeFieldPresent": True,
                          "tradedValueFieldPresent": False}, dp, cc, True)
    ck("§11-C 거래대금 부재 → NO_TRADED_VALUE",
       vc["verdict"] == "NO_TRADED_VALUE")

    # §11-D 상폐 이력 없음
    vd = P.probe_verdict({"status": "PRESENT"}, hist_ok,
                         {"volumeFieldPresent": True,
                          "tradedValueFieldPresent": True},
                         {**dp, "preferredSupported": True,
                          "leadingZeroPreserved": True}, cc, True)
    ck("§11-D 상폐 이력 부재 → NO_DELISTED_HISTORY",
       vd["verdict"] == "NO_DELISTED_HISTORY")

    # §11-A 전부 통과
    va = P.probe_verdict({"status": "PRESENT"}, hist_ok,
                         {"volumeFieldPresent": True,
                          "tradedValueFieldPresent": True},
                         {"delistedHistorySupported": True,
                          "preferredSupported": True,
                          "leadingZeroPreserved": True},
                         {"status": "CONSISTENT"}, True)
    ck("§11-A 전부 통과 → PROBE_PASS_FULL_PERIOD",
       va["verdict"] == "PROBE_PASS_FULL_PERIOD")
    ck("§12 그때만 전체수집 허용", va["fullAcquisitionAllowed"] is True)

    # §6 역방향 — API_ERROR 는 coverage FAIL 이 아니다.
    #   통제연도까지 전멸이면 "historical 없음" 이 아니라 "인증 미실효" 다.
    hist_dead = {"byYear": {str(y): {"rows": 0,
                                     "status": "API_ERROR:10:INVALID_REQUEST_"
                                               "PARAMETER_ERROR"}
                            for y in P.PROBE_YEARS},
                 "fullPeriodPass": False, "authEffective": False,
                 "controlYear": P.CONTROL_YEAR}
    vn = P.probe_verdict({"status": "PRESENT"}, hist_dead, empty, dp, cc, True)
    ck("§6역 통제연도 전멸 → CREDENTIAL_NOT_EFFECTIVE",
       vn["verdict"] == "CREDENTIAL_NOT_EFFECTIVE", vn["verdict"])
    ck("§6역 그래도 전체수집 금지", vn["fullAcquisitionAllowed"] is False)
    ck("§6역 데이터 축은 FAIL 이 아니라 미측정",
       "HISTORICAL_2007_2009" in (vn.get("notMeasuredChecks") or []))
    ck("§6역 소스 부적합으로 단정 금지",
       vn["verdict"] not in ("PROBE_FAIL_NO_HISTORY",
                             "PROBE_PASS_2010_PLUS_ONLY",
                             "NO_TRADED_VALUE", "NO_DELISTED_HISTORY"))

    # 통제연도가 살아 있으면 옛 연도 부재는 진짜 소스 한계다 — 이 길은 막지 않는다.
    # 통제연도(2026)가 살아 있으면 정의상 2010+ 는 있으므로 §11-B 로 간다.
    hist_live = {"byYear": {"2007": {"rows": 0}, "2008": {"rows": 0},
                            "2009": {"rows": 0},
                            str(P.CONTROL_YEAR): {"rows": 2000}},
                 "fullPeriodPass": False, "authEffective": True}
    vl = P.probe_verdict({"status": "PRESENT"}, hist_live, empty, dp, cc, True)
    ck("§11-B 통제연도 정상 + 2007~2009 부재 → 소스 한계 판정 유지",
       vl["verdict"] == "PROBE_PASS_2010_PLUS_ONLY", vl["verdict"])
    ck("§11-B 소스 한계일 때는 미측정으로 흐리지 않는다",
       not vl.get("notMeasuredChecks"))

    ck("§6역 통제연도 정본 = 최신 probe 연도",
       P.CONTROL_YEAR == max(P.PROBE_YEARS))

    ck("§9 교차검증 허용오차 사전 고정",
       P.VOLUME_EXACT_MATCH_MIN == 0.99
       and P.TRADED_VALUE_MEDIAN_ERR_MAX == 0.01)
    ck("§3 probe 연도 정본", P.PROBE_YEARS == [2007, 2008, 2009, 2010,
                                             2015, 2020, 2026])
    ck("§6 hard 연도 2007~2009", P.HARD_YEARS == [2007, 2008, 2009])
    ck("§9 교차검증 최소 20일 × 3종목",
       P.CROSSCHECK_MIN_DAYS >= 20 and P.CROSSCHECK_MIN_TICKERS >= 3)


# ══════════════════ L5 산출물 · 판정 ══════════════════
def t_l5():
    print("[L5] 실제 산출물 · 판정 · reason_class")
    for n in ("credential-status", "probe", "historical-coverage-probe",
              "delisted-preferred-probe", "r27-cache-crosscheck",
              "source-verdict", "acquisition-progress", "final-coverage",
              "r27-reexecution", "final-verdict", "reproduction"):
        ck(f"§38 evidence: r30-{n}-latest.json", L(n) is not None)

    fv = L("final-verdict")
    if fv:
        ck("§41 판정 4종 중 하나",
           fv["judgement"] in ("PASS", "WARNING", "BLOCKED", "WAIT"))
        ck("§41 reason_class 존재", bool(fv.get("reasonClass")))
        ck("§36 REAL_MONEY_NOT_APPROVED",
           fv["production"]["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("§36 실주문·broker·실계좌 0",
           fv["production"]["realOrders"] == 0
           and fv["production"]["broker"] == 0
           and fv["production"]["realAccount"] == 0)
        ck("§36 deploy·외부발송·유료·env 0",
           all(fv["production"][k] == 0 for k in
               ("deploy", "externalSend", "paidData", "envOrToken")))
        ck("§36 production untouched",
           all(fv["production"][k] == "untouched" for k in
               ("legacy50d", "newBmForward", "publicRepo", "homepage",
                "scheduler", "autoApply", "autoPublish")))
        ck("§R28 KRX 웹 자동화 0", fv["production"]["krxWebAutomation"] == 0
           and fv["production"]["pykrxBulk"] == 0)
        ck("§34 포트폴리오 최적화 미수행",
           fv["forbidden"]["portfolioOptimization"] is False)
        ck("§35 새 factor 연구 미수행",
           fv["forbidden"]["newFactorResearch"] is False)
        ck("§31 새 taxonomy 미도입", fv["forbidden"]["newTaxonomy"] is False)
        ck("§16 threshold 미변경", fv["forbidden"]["thresholdChanged"] is False)
        ck("§18 R27 precommit 불변", fv["r27PrecommitUnchanged"] is True)

    sv = L("source-verdict")
    if sv:
        ck("§10 probe 체크 목록 존재", isinstance(sv.get("checks"), dict)
           and len(sv["checks"]) >= 10)
    prog = L("acquisition-progress")
    if prog:
        ck("§12 probe 전 전체수집 미실행",
           prog.get("newlyAcquiredDays", 0) == 0
           or sv.get("fullAcquisitionAllowed"))
        ck("§15 resumable 선언", prog.get("resumable", True) is True
           or prog.get("status") == "NOT_STARTED_PROBE_GATE")

    # 실호출로만 확인 가능한 항목 — 없는 것을 PASS 로 위장하지 않는다
    if not CRED:
        for n in ("2007 실제 row", "2008 실제 row", "2009 실제 row",
                  "2010 실제 row", "2026 실제 row", "KOSPI 실측",
                  "KOSDAQ 실측", "상폐 historical 실측", "우선주 실측",
                  "거래량 field 실측", "거래대금 field 실측",
                  "pagination 실측", "R27 캐시 교차검증 실측",
                  "annual coverage 실측", "20D PIT window 실측",
                  "tradability gate 실측", "included/excluded 실측"):
            need_cred(n)


# ══════════════════ L6 회귀 ══════════════════
def t_l6():
    print("[L6] R27 precommit 불변 · R25/R26 재현 · 이전 산출물 보존")
    import r30_run as R
    pre = R.r27_precommit_unchanged()
    ck("§18 R27 precommit 값 불변", pre["unchanged"] is True)

    rep = L("reproduction")
    ck("§20 재현 산출물 존재", rep is not None)
    if rep:
        ck("§20 R25 SIZE spread 재현", all(
            rep["byGroup"]["SIZE_R25_SPREAD"][h]["match"]
            for h in ("12M", "36M", "60M", "84M")))
        ck("§20 R26 SIZE TOP 재현", all(
            rep["byGroup"]["SIZE_R26_TOP"][h]["match"]
            for h in ("12M", "36M", "60M")))
        ck("§20 R26 BM TOP 재현", all(
            rep["byGroup"]["BM_R26_TOP"][h]["match"]
            for h in ("12M", "36M", "60M")))
        ck("§38 재현 검증은 R27 산출물 미변경",
           rep.get("r27ArtifactsMutated") is False)

    # 기존 R7~R29 산출물 보존 (§38)
    for n in ("r27-precommit", "r27-verdict", "r27-liquidity-coverage",
              "r27-size-reproduction", "r27-bm-reference",
              "r28-final-verdict", "r29-source-verdict"):
        ck(f"§38 이전 산출물 보존: {n}", (RD / f"{n}-latest.json").exists())
    for n in ("wababa-size-tradability-validation-r27-latest.md",
              "wababa-r27-liquidity-data-resume-r28-latest.md",
              "wababa-liquidity-data-source-recovery-r29-latest.md"):
        ck(f"§38 이전 보고서 보존: {n[:28]}…", (WD / n).exists())

    # R27 이 남긴 111일 캐시 보존 (§9·§12)
    n111 = len([p for p in (ROOT / "_cache" / "krx-liquidity").glob("*.csv.gz")])
    ck("§9 R27 기존 캐시 보존(>=111일)", n111 >= 111, str(n111))


# ══════════════════ 보고서 ══════════════════
def t_report():
    print("[보고서] canonical 형식")
    md = WD / "wababa-liquidity-official-full-acquisition-r30-latest.md"
    js = WD / "wababa-liquidity-official-full-acquisition-r30-latest.json"
    ck("§38 canonical MD", md.exists())
    ck("§38 canonical JSON", js.exists())
    if md.exists():
        lines = md.read_text(encoding="utf-8").splitlines()
        ck("§41 첫 줄 전체 판정", lines[0].startswith("전체 판정:"), lines[0])
        ck("§41 둘째 줄 reason_class", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("credential", "공식 API", "2007", "거래량", "거래대금",
                     "상폐", "우선주", "R27", "coverage", "checkpoint",
                     "REAL_MONEY_NOT_APPROVED", "HOTG", "Founder 행동",
                     "다음 단일 작업"):
            ck(f"§42 보고 항목: {need}", need in body)
        for k in ("DART_API_KEY", "KRX_ID", "KRX_PW", *S.CRED_ENV_ALIASES):
            val = (os.environ.get(k) or "").strip()
            if val and len(val) >= 8:
                ck(f"보고서에 {k} 값 미노출", val not in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6, t_report):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / SKIP {SKIP}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("secretsPrinted: 0")
    print(f"credentialPresent: {int(CRED)}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
