#!/usr/bin/env python3
"""R27 회귀 — SIZE 거래가능성 검증 invariant. 네트워크 0.

WABABA-SIZE-TRADABILITY-VALIDATION-R27

계층:
  L1 precommit — gate·민감도·bucket·horizon 사전 고정, rescue 금지
  L2 liquidity — PIT 타이밍(미래 거래량 금지) · 결측≠0 · 거래대금 계산
  L3 gate      — PRIMARY gate 하나 · BM/SIZE 동일 적용 · 탈락사유
  L4 real      — 실제 산출물 · R25/R26 정확 재현 · coverage gate · 판정
  L5 regress   — R26~R16 산출물·판정 보존

사용: python scripts/research/test_r27_validation.py
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r27_analysis as A27  # noqa: E402
import r27_collect as CO  # noqa: E402
from r25_engine import R25Engine  # noqa: E402
from r27_precommit import (CAPITAL, COVERAGE, FORBIDDEN, HORIZONS,  # noqa: E402
                           MISSING_RULE, PARTICIPATION, PRICE_BUCKETS,
                           PRIMARY_GATE, QUALIFICATION, SENSITIVITY,
                           SUSPENSION)

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PASS = FAIL = 0
PH = HORIZONS["primary"]


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {extra}")


def L(n):
    p = RD / f"r27-{n}-latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ═══════════════════ L1 precommit ═══════════════════
def t_l1():
    print("[L1] precommit 고정")
    pc = L("precommit")
    ck("precommit 존재", pc is not None)
    if not pc:
        return
    ck("§5 결과 보기 전 작성", pc["writtenBeforeResults"] is True)
    ck("§17 PRIMARY gate 하나", pc["primaryGate"]["onlyOne"].startswith("PRIMARY"))
    ck("§17 gate 이름 고정", PRIMARY_GATE["name"] == "TRADABLE_AT_DECISION")
    ck("§15 참여율 1% 로 보수적 고정",
       PRIMARY_GATE["participationImplied"] == 0.01)
    ck("§15 임계 125,000,000원", PRIMARY_GATE["thresholdKrw"] == 125_000_000)
    ck("§14 자본 5천만원", CAPITAL["totalKrw"] == 50_000_000)
    ck("§14 40종목 sanity", CAPITAL["namesForSanity"] == 40)
    ck("§14 1종목 125만원", CAPITAL["orderPerNameKrw"] == 1_250_000)
    ck("§14 임계 = 주문액 × 100",
       PRIMARY_GATE["thresholdKrw"] == CAPITAL["orderPerNameKrw"] * 100)
    ck("§26 민감도 3개 고정",
       {"LOW", "BASE", "HIGH"} <= set(SENSITIVITY))
    ck("§26 BASE 가 PRIMARY 와 동일",
       SENSITIVITY["BASE"]["thresholdKrw"] == PRIMARY_GATE["thresholdKrw"])
    ck("§26 최고성과 채택 금지 명시", "금지" in SENSITIVITY["rule"])
    ck("§19 primary horizon 36M", HORIZONS["primary"] == 36)
    ck("§19 horizon 12/36/60", HORIZONS["all"] == [12, 36, 60])
    ck("§12 가격 bucket 4개", len(PRICE_BUCKETS) == 4)
    ck("§12 저가주는 filter 아님", "제외조건" in pc["priceRule"]["notAFilter"]
       or "아니다" in pc["priceRule"]["notAFilter"])
    ck("§15 참여율 bucket 5 cut", len(PARTICIPATION["buckets"]) == 5)
    ck("§10 거래정지 데이터 없음 선언",
       SUSPENSION["status"] == "SUSPENSION_DATA_UNAVAILABLE")
    ck("§30 연도 coverage 90%", COVERAGE["minCoveragePctPerYear"] == 90.0)
    ck("§30 최소 연도 15", COVERAGE["minYearsCovered"] == 15)
    ck("§30 기간 몰래 이동 금지", "몰래" in COVERAGE["noSecretWindowShift"])
    ck("§31 판정 5종", len([k for k in QUALIFICATION
                          if k.startswith("SIZE_")]) == 5)
    ck("§7 BM/SIZE 동일 gate 명시", "다른 거래가능성 기준" in pc["sameGateBoth"])
    ck("§2 threshold 최적화 금지", any("optimization" in x for x in FORBIDDEN))
    ck("§2 penny rescue 금지", any("penny" in x for x in FORBIDDEN))
    ck("§3 유료데이터 0", pc["dataSources"]["paidData"] == 0)
    ck("§3 신규 credential 0", pc["dataSources"]["newCredential"] == 0)
    ck("§41 실패조건 정의", len(pc["failureConditions"]) >= 5)


# ═══════════════════ L2 유동성 계산 ═══════════════════
def t_l2():
    print("[L2] 유동성 지표 · PIT 타이밍")
    ck("§8 lookback 20거래일", CO.LOOKBACK == 20)
    cal = CO.trading_calendar()
    ck("거래일 캘린더 로드", len(cal) > 4000, len(cal))
    ck("캘린더가 실제 거래일(주말 없음)",
       all(__import__("datetime").date.fromisoformat(d).weekday() < 5
           for d in cal[:200]))

    # ★ look-ahead 금지 — 결정일 당일·이후가 창에 들어가면 안 된다
    dec = cal[100]
    need = CO.needed_days(cal, [dec])
    ck("§8 창이 결정일 직전 20일", len(need) == 20, len(need))
    ck("§8 결정일 당일 미포함", dec not in need)
    ck("§8 결정일 이후 미포함", all(d < dec for d in need))
    ck("§8 창이 캘린더상 연속", need == cal[80:100], (need[0], need[-1]))

    # 여러 결정일 합집합도 미래를 포함하지 않는다
    decs = [cal[100], cal[150]]
    n2 = CO.needed_days(cal, decs)
    ck("§8 다중 결정일도 미래 미포함", all(d < max(decs) for d in n2))

    # §30 결측 ≠ 0
    ck("§30 결측을 0 으로 취급 금지 명시", "0 으로 취급하지 않는다"
       in MISSING_RULE["missingIsNotZero"])
    ck("§30 결측은 tradable 통과 불가", "받을 수 없다"
       in MISSING_RULE["missingExcludedFromTradable"])
    ck("§11 거래량 0 은 관측값", "구분한다" in MISSING_RULE["zeroVolumeIsObserved"])

    # 지표 계산 — 합성 데이터로
    tr = A27.Tradability.__new__(A27.Tradability)
    tr.cal, tr.pos = cal, {d: i for i, d in enumerate(cal)}
    tr.metrics = {"2020-01-02": {
        "GOOD": {"observedDays20": 20, "zeroVolumeDays20": 0,
                 "median20TradedValue": 500_000_000,
                 "mean20TradedValue": 500_000_000,
                 "median20Volume": 10000, "tradedOnLastDay": True},
        "THIN": {"observedDays20": 20, "zeroVolumeDays20": 0,
                 "median20TradedValue": 10_000_000,
                 "mean20TradedValue": 10_000_000,
                 "median20Volume": 100, "tradedOnLastDay": True},
        "HALTED": {"observedDays20": 20, "zeroVolumeDays20": 12,
                   "median20TradedValue": 500_000_000,
                   "mean20TradedValue": 500_000_000,
                   "median20Volume": 0, "tradedOnLastDay": False},
        "SHORT": {"observedDays20": 8, "zeroVolumeDays20": 0,
                  "median20TradedValue": 500_000_000,
                  "mean20TradedValue": 500_000_000,
                  "median20Volume": 10000, "tradedOnLastDay": True},
        # 임계를 실제로 가로지르는 값 — 민감도가 판별력을 갖는지 본다.
        # 50M: LOW(25M) 통과 · BASE(125M) 탈락
        # 150M: BASE(125M) 통과 · HIGH(250M) 탈락
        "MID_LOW": {"observedDays20": 20, "zeroVolumeDays20": 0,
                    "median20TradedValue": 50_000_000,
                    "mean20TradedValue": 50_000_000,
                    "median20Volume": 5000, "tradedOnLastDay": True},
        "MID_HIGH": {"observedDays20": 20, "zeroVolumeDays20": 0,
                     "median20TradedValue": 150_000_000,
                     "mean20TradedValue": 150_000_000,
                     "median20Volume": 8000, "tradedOnLastDay": True},
    }}
    d = "2020-01-02"
    ck("gate 통과 — 유동성 충분", tr.evaluate(d, "GOOD")[0] is True)
    ck("gate 탈락 — 거래대금 부족",
       tr.evaluate(d, "THIN") == (False, "LOW_TRADED_VALUE"))
    ck("gate 탈락 — 최근 미거래",
       tr.evaluate(d, "HALTED") == (False, "NOT_TRADED_ON_DECISION_DATE"))
    ck("gate 탈락 — 관측일 부족",
       tr.evaluate(d, "SHORT") == (False, "INSUFFICIENT_DAYS"))
    ck("gate 탈락 — 데이터 없음",
       tr.evaluate(d, "NOSUCH") == (False, "MISSING_DATA"))
    ck("§30 결측은 tradable 아님", "NOSUCH" not in tr.tradable_set(
        d, {"GOOD", "NOSUCH"}))
    ck("tradable_set 은 통과만 반환", tr.tradable_set(
        d, {"GOOD", "THIN", "HALTED"}) == {"GOOD"})

    # 민감도 임계는 gate 를 실제로 움직인다 (임계를 가로지르는 값으로 확인)
    ck("BASE 에서 MID_LOW(5천만) 탈락", tr.evaluate(d, "MID_LOW")[0] is False)
    ck("LOW 완화하면 MID_LOW 통과",
       tr.evaluate(d, "MID_LOW", SENSITIVITY["LOW"]["thresholdKrw"])[0] is True)
    ck("BASE 에서 MID_HIGH(1.5억) 통과", tr.evaluate(d, "MID_HIGH")[0] is True)
    ck("HIGH 강화하면 MID_HIGH 탈락",
       tr.evaluate(d, "MID_HIGH", SENSITIVITY["HIGH"]["thresholdKrw"])[0] is False)
    ck("민감도 순서 LOW < BASE < HIGH",
       SENSITIVITY["LOW"]["thresholdKrw"] < SENSITIVITY["BASE"]["thresholdKrw"]
       < SENSITIVITY["HIGH"]["thresholdKrw"])

    # bucket 계산
    ck("가격 bucket <500", A27.price_bucket(300) == "<500")
    ck("가격 bucket 500~999", A27.price_bucket(700) == "500~999")
    ck("가격 bucket 1,000~4,999", A27.price_bucket(2000) == "1,000~4,999")
    ck("가격 bucket >=5,000", A27.price_bucket(9000) == ">=5,000")
    ck("참여율 bucket <=0.1%", A27.participation_bucket(0.0005) == "<=0.1%")
    ck("참여율 bucket >10%", A27.participation_bucket(0.5) == ">10%")
    ck("참여율 = 주문액/거래대금",
       abs(CAPITAL["orderPerNameKrw"] / 125_000_000 - 0.01) < 1e-12)


# ═══════════════════ L3 gate 공정성 ═══════════════════
def t_l3():
    print("[L3] gate 공정성")
    ck("§21 탈락사유 5종", len(A27.REASONS) == 5)
    ck("§21 LOW_PRICE 는 탈락사유가 아님", "LOW_PRICE" not in A27.REASONS)
    ck("§18 세 팔 정의", A27.ARMS == ["SIZE", "BM", "CONTROL"])
    ck("§14 주문액 상수 일치", A27.ORDER == CAPITAL["orderPerNameKrw"])
    ck("§19 primary horizon 일치", A27.PH == 36)
    # gate 는 factor 를 보지 않는다 — 같은 함수 하나
    src = (ROOT / "scripts" / "research" / "r27_analysis.py").read_text(
        encoding="utf-8")
    ck("§7 gate 가 factor 를 인자로 받지 않음",
       "def evaluate(self, d, t, threshold=None)" in src)
    ck("§18 tradable_set 이 universe 를 그대로 받음",
       "def tradable_set(self, d, universe, threshold=None)" in src)


# ═══════════════════ L4 실제 산출물 ═══════════════════
def t_l4():
    print("[L4] 실제 산출물")
    inv, cov = L("data-inventory"), L("liquidity-coverage")
    sz, bm, v = L("size-reproduction"), L("bm-reference"), L("verdict")
    td = L("tradability-definition")
    for n, o in (("data-inventory", inv), ("liquidity-coverage", cov),
                 ("size-reproduction", sz), ("bm-reference", bm),
                 ("tradability-definition", td), ("verdict", v)):
        ck(f"{n} 산출물 존재", o is not None)
    if not all((inv, cov, sz, bm, v, td)):
        return

    ck("§3 소스가 기존 pykrx 함수", "get_market_cap_by_ticker" in inv["source"])
    ck("§3 새 crawler 0 명시", "새 crawler 0" in inv["sourceReuse"])
    ck("§3 유료데이터 0", inv["paidData"] == 0)
    ck("§3 신규 credential 0", inv["newCredential"] == 0)
    ck("§8 lookback 기록", inv["lookbackTradingDays"] == 20)
    ck("§8 look-ahead 규칙 기록", "미사용" in inv["lookAheadRule"])
    ck("필요 거래일 기록", inv["daysRequired"] > 4000)
    ck("실제 수집량 기록", inv["daysCollectedTotal"] >= 0)
    ck("§41 재시도 정책 기록", inv.get("retryPolicy") is not None)
    if inv.get("retryPolicy"):
        ck("§41 세션 복구 포함", inv["retryPolicy"]["sessionResetOnFail"] is True)
        ck("§41 연속실패 중단", inv["retryPolicy"]["consecutiveFailAbort"] > 0)

    # ★ §6·§7 재현 — 이게 통과해야 나머지가 의미 있다
    ck("§6·§7 R25/R26 전부 정확 재현", sz["allExactMatch"] is True)
    for k, x in sz["r25"].items():
        ck(f"SIZE R25 {k} 재현", x["exactMatch"] is True,
           (x["r25SpreadAnnPct"], x["r27SpreadAnnPct"]))
    for k, x in sz["r26"].items():
        ck(f"SIZE R26 {k} TOP 재현", x["exactMatch"] is True)
    for k, x in bm["r26"].items():
        ck(f"BM R26 {k} TOP 재현", x["exactMatch"] is True)
    ck("§6 R26 SIZE 36M anchor 19.15", sz["r26Anchor36mTop"] == 19.15)
    ck("§6 SIZE 36M 실측 19.147", abs(sz["r26"]["36M"]["r27TopAnnPct"] - 19.147) < 1e-9)
    ck("§7 BM 36M 실측 15.01", abs(bm["r26"]["36M"]["r27TopAnnPct"] - 15.01) < 1e-9)
    ck("§7 BM reference 목적 명시", "다시 하는 것이 아니라" in bm["why"])

    # coverage gate
    ck("§30 연도별 coverage 산출", len(cov["byYear"]) > 0)
    ck("§30 거래소별 coverage 산출", len(cov["byExchange"]) > 0)
    ck("§30 크기구간별 coverage 산출", len(cov["bySizeBucket"]) == 3)
    ck("§30 gate 판정 기록", "coverageGatePass" in cov)
    ck("§30 기간 이동 금지 문구", "몰래" in cov["noSecretWindowShift"])
    ck("§10 거래정지 상태 기록",
       cov["suspension"]["status"] == "SUSPENSION_DATA_UNAVAILABLE")

    # 판정
    ck("§31 판정이 5종 중 하나",
       v["verdict"] in ("SIZE_EXECUTABLE_STRONG", "SIZE_EXECUTABLE_PROMISING",
                        "SIZE_ALPHA_LARGELY_NONTRADABLE", "SIZE_FRAGILE",
                        "SIZE_DATA_INSUFFICIENT"))
    ck("§31 기준 불변", v["thresholdsUnchanged"] is True)
    ck("§2 rescue 없음", v["noParameterRescue"] is True)
    ck("판정이 coverage 와 일관",
       (v["verdict"] == "SIZE_DATA_INSUFFICIENT")
       == (not cov["coverageGatePass"]))
    if v["verdict"] == "SIZE_DATA_INSUFFICIENT":
        ck("§34-D 다음 규칙", v["nextTaskRule"] == "D")
        ck("§35 포트폴리오 진입 불가", v["portfolioResearchEntryAllowed"] is False)
        ck("§32 근거 없는 BM vs SIZE 비교 안 함",
           (L("bm-vs-size") or {}).get("status") == "NOT_EVALUATED")
        ck("재현은 성공했음을 기록", v["sizeReproductionExact"] is True)
        # coverage 미달이면 하류 산출물이 없어야 정상이다
        for n in ("before-after", "participation", "included-excluded",
                  "concentration"):
            ck(f"§30 하류 미생성 {n}", L(n) is None)

    ck("§17 gate 정의 산출물 하나", td["onlyOnePrimaryGate"] is True)
    ck("§7 gate 가 BM/SIZE 공통", "다른 거래가능성 기준" in td["sameGateBoth"])


# ═══════════════════ L5 회귀 ═══════════════════
def t_l5():
    print("[L5] R26~R16 회귀")
    ds = [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01" for i in range(6)]

    def syn(rows):
        return {d: {"X": {"close": c, "shares": s, "dps": None}}
                for d, (c, s) in zip(ds, rows)}
    e = R25Engine(ds, cap=syn([(100000, 100)] + [(10000, 1000)] * 5),
                  overrides={})
    ck("10:1 분할 wealth 불변", abs(e.tsr(0, 2, "X")) < 1e-12)
    e2 = R25Engine(ds, cap=syn([(10000, 100)] + [(5000, 200)] * 5), overrides={})
    ck("무상증자 wealth 불변", abs(e2.tsr(0, 2, "X")) < 1e-12)

    for f in ("r24-foundation-verdict-latest.json",
              "r25-verdict-latest.json", "r25-horizon-results-latest.json",
              "r26-verdict-latest.json", "r26-horizon-results-latest.json",
              "r15-samsung-tsr-latest.json"):
        ck(f"이전 산출물 보존 {f}", (RD / f).exists())
    r24 = json.loads((RD / "r24-foundation-verdict-latest.json")
                     .read_text(encoding="utf-8"))
    ck("R24 판정 보존",
       r24["verdict"] == "CANONICAL_TSR_FOUNDATION_PASS_WITH_LIMITATIONS")
    r25 = json.loads((RD / "r25-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R25 PRIMARY 보존", set(r25["PRIMARY_FACTOR"]) == {"BM", "SIZE_SMALL"})
    r26 = json.loads((RD / "r26-verdict-latest.json").read_text(encoding="utf-8"))
    ck("R26 판정 보존", r26["verdict"] == "NO_INCREMENTAL_COMBINATION")
    r26h = json.loads((RD / "r26-horizon-results-latest.json")
                      .read_text(encoding="utf-8"))["byHorizon"]
    ck("R26 SIZE 36M TOP 19.147 보존",
       r26h["36M"]["SIZE_ALONE"]["meanTopAnnPct"] == 19.147)
    ck("R26 BM 36M TOP 15.01 보존",
       r26h["36M"]["BM_ALONE"]["meanTopAnnPct"] == 15.01)

    for f in ("wababa-canonical-factor-rediscovery-r25-latest.md",
              "wababa-bm-size-incremental-combination-r26-latest.md",
              "wababa-final-unknown-50-direct-classification-r24-latest.md"):
        ck(f"기존 보고서 보존 {f}", (WD / f).exists())


# ═══════════════════ production ═══════════════════
def t_prod():
    print("[PROD] production 보호")
    j = WD / "wababa-size-tradability-validation-r27-latest.json"
    ck("R27 보고서 JSON 존재", j.exists())
    if j.exists():
        d = json.loads(j.read_text(encoding="utf-8"))
        p = d["production"]
        for k in ("realOrders", "broker", "realAccount", "paidData",
                  "externalSend", "deploy", "envOrToken", "productionDbWrite",
                  "publicDisclosure", "newCredential"):
            ck(f"{k} = 0", p[k] == 0)
        for k in ("publicRepo", "legacy50d", "newBmForward", "scheduler",
                  "homepage", "autoApply", "autoPublish", "canonicalProduction"):
            ck(f"{k} 무변경", p[k] == "untouched")
        ck("REAL_MONEY_NOT_APPROVED",
           p["realMoneyStage"] == "REAL_MONEY_NOT_APPROVED")
        ck("이전 산출물 미덮어쓰기", d["priorArtifactsPreserved"] is True)
        ck("HOTG 신규 observer 0", d["hotg"]["newObservers"] == 0)
        ck("HOTG 신규 scheduler 0", d["hotg"]["newScheduler"] == 0)
        ck("§38 산출물 상태 표기", len(d["artifactStatus"]) >= 15)
        ck("§38 미수행을 숨기지 않음",
           "NOT_RUN" in set(d["artifactStatus"].values())
           or d["verdict"]["verdict"] != "SIZE_DATA_INSUFFICIENT")
    m = WD / "wababa-size-tradability-validation-r27-latest.md"
    ck("R27 보고서 MD 존재", m.exists())
    if m.exists():
        lines = m.read_text(encoding="utf-8").splitlines()
        ck("첫 줄 전체 판정 (§42)", lines[0].startswith("전체 판정:"), lines[0])
        ck("둘째 줄 reason_class (§42)", lines[1].startswith("reason_class:"))
        body = "\n".join(lines)
        for need in ("재현 게이트", "coverage", "자체수정", "PRIMARY gate",
                     "SUSPENSION_DATA_UNAVAILABLE", "REAL_MONEY_NOT_APPROVED",
                     "Founder 행동", "다음 단일 작업", "한계"):
            ck(f"§42 보고 항목: {need}", need in body)
        ck("답 못한 것을 명시", "답하지 못했다" in body
           or "판정할 수 없다" in body)
        ck("다음 단일 작업 1개",
           sum(1 for ln in lines if ln.startswith("- 다음 단일 작업")) == 1)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_prod):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("productionWrites: 0")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
