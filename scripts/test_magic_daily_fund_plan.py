# test_magic_daily_fund_plan.py
# 마법공식 펀드 일일 실행계획·보고계약(MF-DAILY-TOP10-AUTONOMOUS-FUND-REPORTING-CONTRACT) 회귀 테스트.
#   목적: (1) 자율운영 보고 문구 계약 — 정상일 [대장이 할 일] 없음, 금지 문구 부재
#         (2) 메일 라우팅 — PASS는 운영계정 보관만/개인메일 0명, WARNING·BLOCKED만 개인메일
#         (3) 실행계획 — entries/exits/holds, 50거래일 FIFO 매도(top10 이탈은 매도사유 아님),
#             수량 null(체결시 확정), 수수료 0(미반영 명시)
#         (4) 안전게이트 — stale/top10 수·중복/canonical 미반영/현금부족/휴장일/멱등성
#         (5) 불변 — 실주문 0, 브로커 0, SMTP 0, canonical write 0
#   방식: 전부 임시 디렉터리(tempfile) fixture. 실제 TEMP·운영 canonical 접근 0.
# 사용: python scripts\test_magic_daily_fund_plan.py

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import magic_daily_common as C  # noqa: E402
import magic_daily_fund_plan as F  # noqa: E402

_pass = 0
_fail = 0


def check(name, actual, expected):
    global _pass, _fail
    if actual == expected:
        _pass += 1
        print(f"  PASS  {name}")
    else:
        _fail += 1
        print(f"  FAIL  {name}  expected={expected!r} actual={actual!r}")


RULES = F.engine_rules()

TOP10 = [(1, "046940", "우원개발"), (2, "461300", "아이스크림미디어"), (3, "088130", "동아엘텍"),
         (4, "124500", "아이티센글로벌"), (5, "184230", "SGA솔루션즈"), (6, "171090", "선익시스템"),
         (7, "053580", "웹케시"), (8, "052400", "코나아이"), (9, "018290", "브이티"),
         (10, "484870", "엠앤씨솔루션")]


def make_ranking(date_iso, rows=TOP10, next_exec="2026-07-27"):
    return {
        "signalAsOfDate": date_iso, "universeBaseDate": date_iso,
        "nextExecutionDateCandidate": next_exec,
        "formulaVersion": "book-faithful-v1-2026-43B5", "eligibleCount": 1314,
        "top10": [{"rank": r, "code": c, "name": n, "valueRank": r,
                   "profitabilityRank": r * 2, "combinedRank": r * 3,
                   "signalClosePrice": 10000 + r} for r, c, n in rows],
    }


def make_canonical(*, last_exec="2026-07-24", seq=14, tdi=16, cash=36_000_000,
                   due_batch=False, missed=None):
    """OPEN batch 2개(각 lot 2개)로 축약한 canonical fixture."""
    batches, lots = [], []
    for i in (1, 2):
        planned = 3 if (due_batch and i == 1) else 99
        bid = f"MF-BATCH-FIX-{i}"
        batches.append({"batchId": bid, "status": "OPEN", "operationMode": "OFFICIAL",
                        "plannedSellTradingDayIndex": planned})
        for code, name in (("046940", "우원개발"), ("999999", "픽스처종목")):
            lots.append({"batchId": bid, "code": code, "name": name,
                         "quantity": 10 + i, "status": "OPEN"})
    return {
        "officialSequence": seq, "officialTradingDayIndex": tdi,
        "officialAvailableCash": cash,
        "officialExecutionCalendar": ["2026-07-22", "2026-07-23", last_exec],
        "missedRuns": missed or [],
        "batches": batches, "itemLots": lots,
    }


print("=== 마법공식 펀드 일일 실행계획·보고계약 회귀 테스트 (fixture 전용) ===")

# ── 1) 엔진 규칙을 코드에 복제하지 않고 읽어오는가 ──────────────────────────────
print("[1] 운영 규칙 출처(engine 상수 재사용)")
check("topN=10", RULES["topN"], 10)
check("holdTradingDays=50", RULES["holdTradingDays"], 50)
check("initialBatchCapital=1,000,000", RULES["initialBatchCapital"], 1_000_000)
check("수수료·세금 미반영(FEE_TAX_MODELED=False)", RULES["feeTaxModeled"], False)

# ── 2) 라우팅 계약 ─────────────────────────────────────────────────────────────
print("[2] 메일 라우팅 계약")
r_pass = F.resolve_routing("PASS")
check("PASS 수신자=운영계정만", r_pass["to"], [F.OPS_ARCHIVE_RECIPIENT])
check("PASS 개인메일 수신자 0명", r_pass["founderRecipientCount"], 0)
check("PASS founderNotified=False", r_pass["founderNotified"], False)
check("PASS 목적=ARCHIVE", r_pass["purpose"], "ARCHIVE")
check("PASS 라우팅에 개인메일 미포함", F.FOUNDER_ALERT_RECIPIENT in r_pass["to"], False)
for v in ("WARNING", "BLOCKED"):
    rv = F.resolve_routing(v)
    check(f"{v} 수신자=대장 개인메일", rv["to"], [F.FOUNDER_ALERT_RECIPIENT])
    check(f"{v} founderNotified=True", rv["founderNotified"], True)
    check(f"{v} 목적=EXCEPTION_ALERT", rv["purpose"], "EXCEPTION_ALERT")

# ── 3) 실행계획 ────────────────────────────────────────────────────────────────
print("[3] 실행계획(entries/exits/holds)")
ranking = make_ranking("2026-07-24")
canon = make_canonical()
plan = F.build_execution_plan("2026-07-27", "2026-07-24", ranking, canon, RULES)
check("entries=10종(신규 batch)", len(plan["entries"]), 10)
check("entries action=BUY", {e["action"] for e in plan["entries"]}, {"BUY"})
check("수량은 체결시 확정(null)", {e["targetQuantity"] for e in plan["entries"]}, {None})
check("수량정책 사유코드", plan["quantityPolicy"], F.R_QTY_AT_EXECUTION)
check("종목당 목표금액=100,000(동일가중)", plan["entries"][0]["targetAmount"], 100000.0)
check("편입 사유코드", plan["entries"][0]["reasonCode"], F.R_RULE_NEW_BATCH)
check("만기 미도래 → exits 0", len(plan["exits"]), 0)
check("holds=보유 2종", len(plan["holds"]), 2)
check("holds action=HOLD", {h["action"] for h in plan["holds"]}, {"HOLD"})
check("중복보유 반영(046940 현재수량 23)",
      next(e for e in plan["entries"] if e["stockCode"] == "046940")["currentQuantity"], 23)
check("cashBefore", plan["cashBefore"], 36_000_000.0)
check("plannedCashUse=배치정액", plan["plannedCashUse"], 1_000_000.0)
check("cashAfter", plan["cashAfter"], 35_000_000.0)
check("자금단계=초기배치", plan["cashPhase"], "INITIAL_BATCH_ALLOCATION")
check("수수료 0", plan["estimatedFees"], 0)
check("수수료 사유=미반영", plan["estimatedFeesReason"], F.R_FEE_NOT_MODELED)
check("realOrderCount=0", plan["realOrderCount"], 0)
check("executionStatus=미적용", plan["executionStatus"], "PLANNED_NOT_APPLIED")
check("seq before→after", (plan["officialSequenceBefore"], plan["officialSequenceAfter"]), (14, 15))

print("[3b] 매도는 시간기반 FIFO — top10 이탈은 매도사유 아님")
# 보유 중인 999999 는 오늘 top10 에 없지만 만기 전이므로 SELL 이 아니라 HOLD 여야 한다.
check("top10 이탈 종목이 exits 에 없음", any(x["stockCode"] == "999999" for x in plan["exits"]), False)
check("top10 이탈 종목은 HOLD", any(h["stockCode"] == "999999" for h in plan["holds"]), True)
# 만기 도달 batch 가 있으면 그때만 SELL
canon_due = make_canonical(due_batch=True)
plan_due = F.build_execution_plan("2026-07-27", "2026-07-24", ranking, canon_due, RULES)
check("만기 도달 batch → exits 2 lot", len(plan_due["exits"]), 2)
check("매도 사유코드=50거래일 FIFO", plan_due["exits"][0]["reasonCode"], F.R_RULE_FIFO_SELL)
check("매도 목표수량=0", plan_due["exits"][0]["targetQuantity"], 0)

# ── 4) 안전 게이트 ─────────────────────────────────────────────────────────────
print("[4] 안전 게이트")
g = F.evaluate_gates("2026-07-24", "2026-07-27", ranking, canon, plan, RULES)
check("정상 → PASS", g["verdict"], "PASS")

g_stale = F.evaluate_gates("2026-07-24", "2026-07-27", make_ranking("2026-07-23"), canon, plan, RULES)
check("시세 stale → BLOCKED", g_stale["verdict"], "BLOCKED")
check("stale 사유코드", g_stale["blocked"][0]["code"], F.R_UNIVERSE_STALE)

g_cnt = F.evaluate_gates("2026-07-24", "2026-07-27", make_ranking("2026-07-24", TOP10[:9]), canon, plan, RULES)
check("top10 9종 → BLOCKED", g_cnt["verdict"], "BLOCKED")
check("종목수 사유코드", g_cnt["blocked"][0]["code"], F.R_TOP10_INCOMPLETE)

dup_rows = TOP10[:9] + [(10, "046940", "우원개발")]
g_dup = F.evaluate_gates("2026-07-24", "2026-07-27", make_ranking("2026-07-24", dup_rows), canon, plan, RULES)
check("중복 종목 → BLOCKED", g_dup["verdict"], "BLOCKED")
check("중복 사유코드", any(b["code"] == F.R_TOP10_DUPLICATE for b in g_dup["blocked"]), True)

g_nosig = F.evaluate_gates("2026-07-24", "2026-07-27", None, canon, None, RULES)
check("신호 없음 → BLOCKED", g_nosig["blocked"][0]["code"], F.R_SIGNAL_NOT_READY)

g_nocanon = F.evaluate_gates("2026-07-24", "2026-07-27", ranking, None, None, RULES)
check("canonical 읽기실패 → BLOCKED", g_nocanon["blocked"][-1]["code"], F.R_CANONICAL_UNREADABLE)

canon_behind = make_canonical(last_exec="2026-07-21")
g_behind = F.evaluate_gates("2026-07-24", "2026-07-27", ranking, canon_behind, plan, RULES)
check("장부 미반영 거래일 존재 → BLOCKED", g_behind["verdict"], "BLOCKED")
check("미반영 사유코드", any(b["code"] == F.R_CANONICAL_BEHIND for b in g_behind["blocked"]), True)
# missedRuns 로 기록된 날짜는 '미반영'으로 세지 않는다
canon_missed = make_canonical(last_exec="2026-07-21",
                              missed=[{"date": "2026-07-22"}, {"date": "2026-07-23"},
                                      {"date": "2026-07-24"}])
g_missed = F.evaluate_gates("2026-07-24", "2026-07-27", ranking, canon_missed, plan, RULES)
check("missedRun 기록분은 미반영 아님 → PASS", g_missed["verdict"], "PASS")

canon_poor = make_canonical(cash=500_000)
plan_poor = F.build_execution_plan("2026-07-27", "2026-07-24", ranking, canon_poor, RULES)
g_poor = F.evaluate_gates("2026-07-24", "2026-07-27", ranking, canon_poor, plan_poor, RULES)
check("현금부족 → BLOCKED", g_poor["verdict"], "BLOCKED")
check("현금부족 사유코드", any(b["code"] == F.R_INSUFFICIENT_CASH for b in g_poor["blocked"]), True)

rank_nop = make_ranking("2026-07-24")
rank_nop["top10"][0]["signalClosePrice"] = None
g_nop = F.evaluate_gates("2026-07-24", "2026-07-27", rank_nop, canon, plan, RULES)
check("참고종가 누락 → WARNING", g_nop["verdict"], "WARNING")
check("가격누락 사유코드", g_nop["warnings"][0]["code"], F.R_MISSING_REFERENCE_PRICE)

# ── 5) 보고 문구 계약 ──────────────────────────────────────────────────────────
print("[5] 보고 문구 계약(자율운영)")
rep = F.render_report("2026-07-24", "2026-07-27", plan, g, F.resolve_routing("PASS"))
body = rep["body"]
check("정상일 제목", rep["subject"], "[와바바] 2026-07-27 마법공식 펀드 일일 실행계획")
check("제목 계약", "[제목] 마법공식 펀드 일일 실행계획" in body, True)
check("정상일 [대장이 할 일] 없음", "[대장이 할 일] 없음" in body, True)
check("[실주문] 0", "[실주문] 0" in body, True)
check("[브로커 호출] 0", "[브로커 호출] 0" in body, True)
check("[실제 이메일 발송] 0", "[실제 이메일 발송] 0" in body, True)
for banned in ("오늘 매수 여부를 직접 판단", "매수 추천", "대장 승인 후 매수",
               "투자 판단 필요", "매수 검토"):
    check(f"금지문구 부재: {banned}", banned in body, False)
for term in ("규칙 기반 편입 대상", "규칙 기반 제외 대상", "일일 실행계획"):
    check(f"대체용어 사용: {term}", term in body, True)
check("정상 본문에 개인메일 미노출", F.FOUNDER_ALERT_RECIPIENT in body, False)
check("정상 본문에 운영계정 라우팅 표시", F.OPS_ARCHIVE_RECIPIENT in body, True)

print("[5b] 예외 보고 문구")
rep_b = F.render_report("2026-07-24", "2026-07-27", plan, g_behind, F.resolve_routing("BLOCKED"))
check("BLOCKED 제목", rep_b["subject"], "[와바바][BLOCKED] 2026-07-24 마법공식 펀드 중단")
check("BLOCKED 대장 할일 1개 표기",
      "[대장이 할 일] 미반영 거래일의 장부 반영(승인·apply) 여부 확인" in rep_b["body"], True)
check("BLOCKED 영향범위 표기", "■ 영향 범위" in rep_b["body"], True)
check("BLOCKED 본문에 개인메일 라우팅", F.FOUNDER_ALERT_RECIPIENT in rep_b["body"], True)
rep_w = F.render_report("2026-07-24", "2026-07-27", plan, g_nop, F.resolve_routing("WARNING"))
check("WARNING 제목", rep_w["subject"], "[와바바][WARNING] 2026-07-24 마법공식 펀드 이상")

# ── 6) run_fund_plan 통합(임시 디렉터리 격리) ──────────────────────────────────
print("[6] run_fund_plan 통합")
_orig = C.TEMP_ROOT
try:
    with tempfile.TemporaryDirectory() as tmp:
        C.TEMP_ROOT = Path(tmp)
        sp = C.TEMP_ROOT / "reports" / "delivery-state.json"
        pp = C.TEMP_ROOT / "reports" / "preview.txt"
        cp = C.TEMP_ROOT / "canonical.json"
        cp.write_text(json.dumps(make_canonical(), ensure_ascii=False), encoding="utf-8")

        # 6-1) 휴장일 self-skip
        r = F.run_fund_plan("2026-07-25", state_path=sp, preview_path=pp, canonical_path=cp)
        check("토요일 self-skip", r["status"], F.SELF_SKIPPED_NON_TRADING_DAY)
        check("휴장일 개인메일 0명", r["founderRecipientCount"], 0)
        check("휴장일 emailSent=False", r["emailSent"], False)

        # 6-2) 신호 없음 → BLOCKED + 개인메일 대상
        r = F.run_fund_plan("2026-07-24", state_path=sp, preview_path=pp, canonical_path=cp)
        check("신호 없음 → PLAN_BLOCKED", r["status"], F.PLAN_BLOCKED)
        check("BLOCKED → 개인메일 1명", r["founderRecipientCount"], 1)
        check("BLOCKED → applyEligible=False", r["applyEligible"], False)

        # 6-3) 정상 → PASS + 운영계정 보관, 개인메일 0
        pkg = C.TEMP_ROOT / "2026-07-24"
        pkg.mkdir(parents=True, exist_ok=True)
        (pkg / "rankings.json").write_text(json.dumps(ranking, ensure_ascii=False), encoding="utf-8")
        r = F.run_fund_plan("2026-07-24", state_path=sp, preview_path=pp, canonical_path=cp)
        check("정상 → PLAN_READY", r["status"], F.PLAN_READY)
        check("정상 verdict=PASS", r["verdict"], "PASS")
        check("정상 → 개인메일 0명", r["founderRecipientCount"], 0)
        check("정상 → founderNotified=False", r["founderNotified"], False)
        check("정상 → 운영계정 보관", r["routing"]["to"], [F.OPS_ARCHIVE_RECIPIENT])
        check("정상 → applyEligible=True", r["applyEligible"], True)
        check("실주문 0", r["realOrderCount"], 0)
        check("브로커 호출 0", r["brokerApiCallCount"], 0)
        check("SMTP 호출 0", r["smtpCallCount"], 0)
        check("emailSent=False", r["emailSent"], False)
        check("canonical 변경 0", r["canonicalChanged"], False)
        check("production write 0", r["productionWriteCount"], 0)
        check("executionDate=신호의 다음 거래일", r["executionDate"], "2026-07-27")
        check("최초 duplicate=False", r["duplicate"], False)

        # 6-4) 동일 거래일 재실행 멱등성
        pp.parent.mkdir(parents=True, exist_ok=True)
        pp.write_text(F.render_preview_file(
            {"subject": r["emailSubject"], "body": r["emailBody"]}, r["routing"]), encoding="utf-8")
        r2 = F.run_fund_plan("2026-07-24", state_path=sp, preview_path=pp, canonical_path=cp)
        check("재실행 duplicate=True", r2["duplicate"], True)
        check("재실행 본문 동일(결정론)", r2["emailBody"], r["emailBody"])
        check("재실행도 개인메일 0명", r2["founderRecipientCount"], 0)

        # 6-5) 발송 완료 기록 후 → ALREADY_SENT
        F.mark_sent("2026-07-24", state_path=sp)
        r3 = F.run_fund_plan("2026-07-24", state_path=sp, preview_path=pp, canonical_path=cp)
        check("mark_sent 후 → ALREADY_SENT", r3["status"], F.ALREADY_SENT)
        check("ALREADY_SENT emailSent=False", r3["emailSent"], False)

        # 6-6) TTM 미혼입 — 공식 formulaVersion 만 사용
        check("formulaVersion 공식값", r["plan"]["formulaVersion"], "book-faithful-v1-2026-43B5")
        check("본문에 TTM 미등장", "TTM" in r["emailBody"], False)
finally:
    C.TEMP_ROOT = _orig

# ── 7) 코드 자체에 발송/주문 경로가 없는지 ─────────────────────────────────────
print("[7] 코드 정적 검사(발송·주문 경로 부재)")
src = Path(F.__file__).read_text(encoding="utf-8")
for banned in ("smtplib", "import smtplib", "sendmail", "SMTP(", "requests.post", "urllib.request"):
    check(f"미사용: {banned}", banned in src, False)
for banned in ("place_order", "send_order", "kiwoom", "broker_api"):
    check(f"미사용: {banned}", banned in src, False)
check("자격증명 env 이름만 예약(값 미참조)", "os.environ" in src or "getenv" in src, False)

print("")
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
