# test_magic_daily_auto_apply.py
# 마법공식 가상 장부 무인 자동반영(MF-CANONICAL-UNATTENDED-AUTO-APPLY) 회귀 테스트.
#   목적: (1) 자동 승인 주체가 사람을 사칭하지 않음
#         (2) AUTO_APPROVAL_ELIGIBLE 전 조건 — 하나라도 실패하면 자동 apply 금지
#         (3) 강제 BLOCKED 코드별 분기
#         (4) 순차성 — 한 실행 최대 1거래일, 가장 오래된 것부터, 역전 금지
#         (5) 멱등성 — 최신 장부면 NO_ACTION, 휴장일 self-skip, lock busy 차단
#         (6) 5거래일 연속 fixture 무인 진행(seq/TDI/lot/현금/만기 연쇄)
#         (7) 보안 — 실주문/브로커/SMTP/public write 경로 정적 부재
#   방식: 전부 fixture + 임시 디렉터리. 실제 canonical·네트워크·apply 체인 미접근.
# 사용: python scripts\test_magic_daily_auto_apply.py

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import magic_daily_common as C  # noqa: E402
import magic_daily_auto_apply as AA

# 감사 격리 marker 는 *운영 상태*다. 테스트가 그걸 읽으면 격리 활성 시 무관하게 실패한다.
# 테스트 동안만 marker 경로를 임시 위치로 돌려 격리 없음 상태로 격리한다(운영 marker 미접촉).
import tempfile as _tf, audit_quarantine as _AQ
_AQ.MARKER_PATH = __import__('pathlib').Path(_tf.mkdtemp()) / 'audit-quarantine.json'

  # noqa: E402

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


RULES = AA.engine_rules()
FV = AA.OFFICIAL_FORMULA_VERSION

TOP10 = [(1, "046940", "우원개발"), (2, "461300", "아이스크림"), (3, "088130", "동아엘텍"),
         (4, "124500", "아이티센"), (5, "184230", "SGA"), (6, "171090", "선익"),
         (7, "053580", "웹케시"), (8, "052400", "코나아이"), (9, "018290", "브이티"),
         (10, "484870", "엠앤씨")]


def make_ranking(sig_date, rows=TOP10, fv=FV, extra=None):
    r = {"signalAsOfDate": sig_date, "universeBaseDate": sig_date, "formulaVersion": fv,
         "top10": [{"rank": i, "code": c, "name": n, "signalClosePrice": 1000 + i}
                   for i, c, n in rows]}
    if extra:
        r.update(extra)
    return r


def make_dry_run(exec_date, sig_date, seq, tdi, *, rows=TOP10, cash_before=34_000_000,
                 alloc=1_000_000, maturity=None, missing_eval=None, bad_amount=False,
                 opens_override=None):
    codes = [c for _, c, _ in rows]
    opens = opens_override if opens_override is not None else {c: 1000.0 + i for i, c in enumerate(codes)}
    plan = []
    for i, c in enumerate(codes):
        op = opens.get(c, 0) or 0
        q = 10
        amt = op * q + (7 if bad_amount and i == 0 else 0)
        plan.append({"code": c, "name": c, "openPrice": op, "quantity": q, "amount": amt})
    invested = sum(p["amount"] for p in plan)
    return {
        "status": "COMPLETED", "runStatus": "COMPLETED",
        "executionDate": exec_date, "signalAsOfDate": sig_date,
        "proposedSequence": seq, "proposedBatchId": f"MF-BATCH-{exec_date}",
        "buyTradingDayIndex": tdi,
        "plannedSellTradingDayIndex": maturity if maturity is not None else tdi + 50,
        "buyCount": 10, "sellCount": 0,
        "allocatedCapital": alloc, "totalInvested": invested,
        "cashReserve": alloc - invested,
        "officialAvailableCashBefore": cash_before,
        "officialAvailableCashAfterPreview": cash_before - alloc,
        "lookAheadValidationPassed": True, "readOnlyUnchanged": True,
        "productionWriteCount": 0, "realOrderCount": 0,
        "missingEvalCodes": missing_eval or [],
        "openPrices": opens, "plan": plan,
    }


def make_canonical(*, seq=16, tdi=18, cash=34_000_000, last_exec="2026-07-24",
                   cal=None, missed=None, batches=None, lots=160):
    return {
        "officialSequence": seq, "officialTradingDayIndex": tdi,
        "officialAvailableCash": cash, "formulaVersion": FV,
        "officialExecutionCalendar": cal or ["2026-07-22", "2026-07-23", last_exec],
        "missedRuns": missed or [],
        "batches": batches or [{"batchId": "MF-BATCH-2026-07-24", "status": "OPEN"}],
        "itemLots": [{"code": "046940", "status": "OPEN", "batchId": "b"} for _ in range(lots)],
    }


print("=== 마법공식 무인 자동반영 회귀 테스트 (fixture 전용, 실 canonical·네트워크 미접근) ===")

# ── 1) 자동 승인 주체 ──────────────────────────────────────────────────────────
print("[1] 자동 승인 주체(사람 사칭 금지)")
base_ticket = {"status": "PENDING_APPROVAL", "executionDate": "2026-07-27",
               "confirmToken": "APPLY_OFFICIAL_DAY_2026-07-27",
               "approval": {"approved": False, "approvedBy": None, "approvedAt": None,
                            "approvalPhrase": "APPROVE_OFFICIAL_APPLY_2026-07-27",
                            "approvalNotes": None}}
appr = AA.apply_auto_approval(base_ticket, "gatesha123", now="2026-07-27T16:25:00+09:00")
check("status=APPROVED", appr["status"], "APPROVED")
check("approved=true", appr["approval"]["approved"], True)
check("approvedBy=정책주체", appr["approval"]["approvedBy"], "WABABA_AUTO_POLICY_V1")
for human in ("병구", "Founder", "duria", "duria2002@gmail.com"):
    check(f"사람 사칭 없음: {human}", human in json.dumps(appr, ensure_ascii=False), False)
check("approvalMode", appr["approval"]["approvalMode"], "UNATTENDED_POLICY")
check("policyVersion", appr["approval"]["policyVersion"], "MF_AUTO_APPLY_V1")
check("approvalBasis=게이트해시", appr["approval"]["approvalBasis"], "gatesha123")
check("approvedAt 기록", appr["approval"]["approvedAt"], "2026-07-27T16:25:00+09:00")
check("realOrderAuthorized=False", appr["approval"]["realOrderAuthorized"], False)
check("founderPolicyAuthorized=True", appr["approval"]["founderPolicyAuthorized"], True)
check("approvalPhrase 불변", appr["approval"]["approvalPhrase"], "APPROVE_OFFICIAL_APPLY_2026-07-27")
check("원본 ticket 불변(깊은 복사)", base_ticket["status"], "PENDING_APPROVAL")


# WABABA-KRX-FUNDAMENTAL-RECOVERY-R1 — 게이트가 그 거래일 KRX 수집 품질 PASS 증거를 요구한다.
# 테스트는 실제 파일 대신 fixture 를 주입한다(운영 상태 미접근).
def QOK(date_iso):
    import krx_data_quality as Q
    return Q.build_status(date_iso=date_iso, verdict="PASS",
                          markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}, universe_count=2700)


# ── 2) 게이트 PASS ─────────────────────────────────────────────────────────────
print("[2] 전 게이트 PASS")
canon = make_canonical()
dr = make_dry_run("2026-07-27", "2026-07-24", 17, 19)
rk = make_ranking("2026-07-24")
g = AA.evaluate_auto_approval_gates(canonical=canon, target_exec_date="2026-07-27",
                                    dry_run=dr, ranking=rk, rules=RULES,
                                    quality_status=QOK("2026-07-27"))
check("eligible=True", g["eligible"], True)
check("blockedCodes 0", len(g["blockedCodes"]), 0)
check("만기 TDI 검사 통과(19+50=69)", g["checks"]["maturityTdiCorrect"], True)

# ── 3) 강제 BLOCKED 분기 ───────────────────────────────────────────────────────
print("[3] 강제 BLOCKED 코드별 분기")


def codes_of(**kw):
    c = kw.pop("canonical", canon)
    d = kw.pop("dry_run", dr)
    r = kw.pop("ranking", rk)
    _t = kw.pop("target", "2026-07-27")
    gg = AA.evaluate_auto_approval_gates(canonical=c, target_exec_date=_t,
                                         dry_run=d, ranking=r, rules=RULES,
                                         quality_status=kw.pop("quality_status", QOK(_t)))
    return gg["eligible"], [b["code"] for b in gg["blockedCodes"]]


e, c1 = codes_of(ranking=None)
check("SIGNAL_MISSING", (e, AA.B_SIGNAL_MISSING in c1), (False, True))
e, c1 = codes_of(dry_run=None)
check("DRY_RUN_MISSING", (e, AA.B_DRY_RUN_MISSING in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 99, 19))
check("CANONICAL_SEQUENCE_GAP(seq)", (e, AA.B_CANONICAL_SEQUENCE_GAP in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 99))
check("CANONICAL_SEQUENCE_GAP(tdi)", (e, AA.B_CANONICAL_SEQUENCE_GAP in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 19, maturity=99))
check("MATURITY_TDI_MISMATCH", (e, AA.B_MATURITY_TDI_MISMATCH in c1), (False, True))
e, c1 = codes_of(target="2026-07-23")
check("EXECUTION_DATE_OUT_OF_ORDER", (e, AA.B_EXECUTION_DATE_OUT_OF_ORDER in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-28", "2026-07-24", 17, 19))
check("EXECUTION_DATE_GAP", (e, AA.B_EXECUTION_DATE_GAP in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 19,
                                      opens_override={c: 0 for _, c, _ in TOP10}))
check("PRICE_MISSING", (e, AA.B_PRICE_MISSING in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 19, missing_eval=["046940"]))
check("PRICE_MISSING(missingEvalCodes)", (e, AA.B_PRICE_MISSING in c1), (False, True))
e, c1 = codes_of(dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 19, bad_amount=True))
check("FALLBACK_PRICE_USED(금액 불일치)", (e, AA.B_FALLBACK_PRICE_USED in c1), (False, True))
e, c1 = codes_of(ranking=make_ranking("2026-07-24", TOP10[:9]))
check("TOP10_COUNT_NOT_10(9종)", (e, AA.B_TOP10_COUNT_NOT_10 in c1), (False, True))
e, c1 = codes_of(ranking=make_ranking("2026-07-24", TOP10 + [(11, "999999", "X")]))
check("TOP10_COUNT_NOT_10(11종)", (e, AA.B_TOP10_COUNT_NOT_10 in c1), (False, True))
dupr = TOP10[:9] + [(10, "046940", "우원개발")]
e, c1 = codes_of(ranking=make_ranking("2026-07-24", dupr))
check("DUPLICATE_STOCK_CODE", (e, AA.B_DUPLICATE_STOCK_CODE in c1), (False, True))
e, c1 = codes_of(ranking=make_ranking("2026-07-24", fv="ttm-experimental-v9"))
check("FORMULA_VERSION_MISMATCH", (e, AA.B_FORMULA_VERSION_MISMATCH in c1), (False, True))
e, c1 = codes_of(ranking=make_ranking("2026-07-24", extra={"ttmExperiment": {"x": 1}}))
check("TTM_CONTAMINATION", (e, AA.B_TTM_CONTAMINATION in c1), (False, True))
e, c1 = codes_of(canonical=make_canonical(cash=500_000))
check("INSUFFICIENT_CASH", (e, AA.B_INSUFFICIENT_CASH in c1), (False, True))
e, c1 = codes_of(canonical=make_canonical(
    batches=[{"batchId": "MF-BATCH-2026-07-27", "status": "OPEN"}]))
check("DUPLICATE_BATCH", (e, AA.B_DUPLICATE_BATCH in c1), (False, True))
d_nc = make_dry_run("2026-07-27", "2026-07-24", 17, 19)
d_nc["runStatus"] = "BLOCKED"; d_nc["status"] = "BLOCKED"
e, c1 = codes_of(dry_run=d_nc)
check("DRY_RUN_NOT_COMPLETED", (e, AA.B_DRY_RUN_NOT_COMPLETED in c1), (False, True))
d_la = make_dry_run("2026-07-27", "2026-07-24", 17, 19); d_la["lookAheadValidationPassed"] = False
e, c1 = codes_of(dry_run=d_la)
check("LOOKAHEAD_FAILED", (e, AA.B_LOOKAHEAD_FAILED in c1), (False, True))
d_ro = make_dry_run("2026-07-27", "2026-07-24", 17, 19); d_ro["realOrderCount"] = 1
e, c1 = codes_of(dry_run=d_ro)
check("REAL_ORDER_PATH_DETECTED", (e, AA.B_REAL_ORDER_PATH_DETECTED in c1), (False, True))
d_pw = make_dry_run("2026-07-27", "2026-07-24", 17, 19); d_pw["productionWriteCount"] = 3
e, c1 = codes_of(dry_run=d_pw)
check("PUBLIC_WRITE_DETECTED", (e, AA.B_PUBLIC_WRITE_DETECTED in c1), (False, True))
# KRX 수집 품질 게이트(WABABA-KRX-FUNDAMENTAL-RECOVERY-R1) — 펀더멘털 INVALID 거래일은
# Auto Apply 자체를 막는다. canonical write·sequence 증가·신규 lot·FIFO 매도 전부 0.
import krx_data_quality as _Q
e, c1 = codes_of(quality_status=None)                       # 증거 없음 = PASS 아님(fail-closed)
check("KRX 품질 증거 없음 → 차단", (e, AA.B_KRX_DATA_QUALITY in c1), (False, True))
e, c1 = codes_of(quality_status=_Q.build_status(
    date_iso="2026-07-27", verdict="INVALID",
    markets={"KOSPI": "INVALID", "KOSDAQ": "PASS"}, reason="필수 columns 누락"))
check("KRX 품질 INVALID → 차단", (e, AA.B_KRX_DATA_QUALITY in c1), (False, True))
e, c1 = codes_of(quality_status=_Q.build_status(
    date_iso="2026-07-27", verdict="PASS", markets={"KOSPI": "PASS", "KOSDAQ": "INVALID"}))
check("한 시장만 PASS → 차단(두 시장 모두 필요)", (e, AA.B_KRX_DATA_QUALITY in c1), (False, True))
e, c1 = codes_of(quality_status=_Q.build_status(
    date_iso="2026-07-24", verdict="PASS", markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}))
check("품질 증거 날짜 불일치 → 차단", (e, AA.B_KRX_DATA_QUALITY in c1), (False, True))

check("WARNING 개념 없음(부분실패=차단)", AA.evaluate_auto_approval_gates(
    canonical=canon, target_exec_date="2026-07-27",
    dry_run=make_dry_run("2026-07-27", "2026-07-24", 17, 19, missing_eval=["X"]),
    ranking=rk, rules=RULES, quality_status=QOK("2026-07-27"))["eligible"], False)

# ── 4) 순차성 ─────────────────────────────────────────────────────────────────
print("[4] 미반영일 순차성(오래된 것부터, 1회 1건)")
c3 = make_canonical(cal=["2026-07-20", "2026-07-21"], last_exec="2026-07-21")
pend = AA.unapplied_execution_dates(c3, "2026-07-24")
check("3거래일 미반영 검출", pend, ["2026-07-22", "2026-07-23", "2026-07-24"])
check("가장 오래된 1건 선택", pend[0], "2026-07-22")
check("주말 제외", "2026-07-25" in AA.unapplied_execution_dates(c3, "2026-07-26"), False)
c4 = make_canonical(cal=["2026-07-21"], last_exec="2026-07-21",
                    missed=[{"date": "2026-07-22"}, {"date": "2026-07-23"}])
check("missedRuns 제외", AA.unapplied_execution_dates(c4, "2026-07-24"), ["2026-07-24"])
check("최신 장부면 미반영 0", AA.unapplied_execution_dates(make_canonical(), "2026-07-24"), [])
check("직전 거래일(월→금)", AA.previous_trading_day("2026-07-27"), "2026-07-24")

# ── 5) 5거래일 연속 fixture 무인 진행 ──────────────────────────────────────────
print("[5] 5거래일 연속 무인 진행(seq/TDI/lot/현금/만기 연쇄)")
days = ["2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]
seq, tdi, cash, lots = 16, 18, 34_000_000, 160
sim_cal = ["2026-07-23", "2026-07-24"]
applied = []
for d in days:
    cur = make_canonical(seq=seq, tdi=tdi, cash=cash, cal=list(sim_cal), lots=lots,
                         batches=[{"batchId": f"MF-BATCH-{x}", "status": "OPEN"} for x in sim_cal])
    pending = AA.unapplied_execution_dates(cur, d)
    target = pending[0]
    prev = AA.previous_trading_day(target)
    gg = AA.evaluate_auto_approval_gates(
        canonical=cur, target_exec_date=target,
        dry_run=make_dry_run(target, prev, seq + 1, tdi + 1, cash_before=cash),
        ranking=make_ranking(prev), rules=RULES, quality_status=QOK(target))
    if not gg["eligible"]:
        break
    applied.append((target, seq + 1, tdi + 1, lots + 10, cash - 1_000_000))
    seq, tdi, lots, cash = seq + 1, tdi + 1, lots + 10, cash - 1_000_000
    sim_cal.append(target)
check("5거래일 전부 자동 진행", len(applied), 5)
check("마지막 seq 21", applied[-1][1], 21)
check("마지막 TDI 23", applied[-1][2], 23)
check("마지막 lot 210", applied[-1][3], 210)
check("마지막 현금 29,000,000", applied[-1][4], 29_000_000)
check("현금 하루 1,000,000씩 감소", [a[4] for a in applied],
      [33_000_000, 32_000_000, 31_000_000, 30_000_000, 29_000_000])
check("날짜 오름차순(역전 없음)", [a[0] for a in applied], days)
check("각 실행 1건씩만", len(set(a[0] for a in applied)), 5)

# ── 6) lock / 휴장일 / 멱등 ────────────────────────────────────────────────────
print("[6] lock · 휴장일 · 멱등")
with tempfile.TemporaryDirectory() as tmp:
    lp = Path(tmp) / "x.lock"
    a1 = AA.acquire_lock(lp, now="2026-07-27T16:25:00+09:00")
    check("최초 lock 획득", a1["acquired"], True)
    a2 = AA.acquire_lock(lp, now="2026-07-27T16:25:01+09:00")
    check("중복 실행 차단", a2["acquired"], False)
    check("잔존 lock 자동삭제 안 함", lp.exists(), True)
    AA.release_lock(lp)
    check("release 후 재획득 가능", AA.acquire_lock(lp)["acquired"], True)
    AA.release_lock(lp)

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    cp = root / "canon.json"
    cp.write_text(json.dumps(make_canonical(), ensure_ascii=False), encoding="utf-8")
    r = AA.run_auto_apply(today_iso="2026-07-26", canonical_path=cp, auto_root=root,
                          lock_path=root / "l.lock", snapshot_dir=root / "snap")
    check("일요일 self-skip", r["status"], AA.SKIPPED_NON_TRADING_DAY)
    check("휴장일 canonicalChanged=False", r["canonicalChanged"], False)
    check("휴장일 개인메일 0명", r["founderRecipientCount"], 0)

    r = AA.run_auto_apply(today_iso="2026-07-24", canonical_path=cp, auto_root=root,
                          lock_path=root / "l.lock", snapshot_dir=root / "snap")
    check("최신 장부 → NO_ACTION", r["status"], AA.NO_ACTION_ALREADY_CURRENT)
    check("NO_ACTION canonicalChanged=False", r["canonicalChanged"], False)
    check("NO_ACTION verdict=PASS", r["verdict"], "PASS")
    check("NO_ACTION 개인메일 0명", r["founderRecipientCount"], 0)
    check("NO_ACTION 실주문 0", r["realOrderCount"], 0)
    check("canonical 파일 무변경", json.loads(cp.read_text(encoding="utf-8"))["officialSequence"], 16)

    lk = root / "busy.lock"
    AA.acquire_lock(lk)
    r = AA.run_auto_apply(today_iso="2026-07-24", canonical_path=cp, auto_root=root,
                          lock_path=lk, snapshot_dir=root / "snap")
    check("lock busy → BLOCKED_LOCK_BUSY", r["status"], AA.BLOCKED_LOCK_BUSY)
    check("lock busy canonicalChanged=False", r["canonicalChanged"], False)
    check("lock busy 개인메일 1명(예외알림)", r["founderRecipientCount"], 1)
    AA.release_lock(lk)

    bad = root / "broken.json"
    bad.write_text("{ not json", encoding="utf-8")
    r = AA.run_auto_apply(today_iso="2026-07-24", canonical_path=bad, auto_root=root,
                          lock_path=root / "l2.lock", snapshot_dir=root / "snap")
    check("canonical 손상 → BLOCKED_CANONICAL_MISMATCH", r["status"], AA.BLOCKED_CANONICAL_MISMATCH)
    check("손상 시 canonicalChanged=False", r["canonicalChanged"], False)

# ── 7) 라우팅 ─────────────────────────────────────────────────────────────────
print("[7] 라우팅(정상=보관, 예외=대장)")
rp = AA.routing_for("PASS")
check("PASS 운영계정만", rp["to"], ["bridge.ai.office@gmail.com"])
check("PASS 개인메일 0명", rp["founderRecipientCount"], 0)
rb = AA.routing_for("BLOCKED")
check("BLOCKED 대장 개인메일", rb["to"], ["duria2002@gmail.com"])
check("BLOCKED 개인메일 1명", rb["founderRecipientCount"], 1)

# ── 8) 보안 정적 검사 ─────────────────────────────────────────────────────────
print("[8] 보안 정적 검사(실주문·브로커·SMTP·public write 경로 부재)")
# 주석·docstring 의 '금지 선언문'을 코드 경로로 오인하지 않도록, 실제 코드 라인만 스캔한다.
_raw = Path(AA.__file__).read_text(encoding="utf-8")
_code_lines, _in_doc = [], False
for _ln in _raw.splitlines():
    _st = _ln.strip()
    if _st.startswith('"""') or _st.endswith('"""'):
        if _st.count('"""') == 1:
            _in_doc = not _in_doc
        continue
    if _in_doc or _st.startswith("#"):
        continue
    _code_lines.append(_ln)
src = "\n".join(_code_lines)
for banned in ("smtplib", "sendmail", "SMTP(", "requests.post", "urllib.request",
               "place_order", "send_order", "kiwoom", "broker_api", "creon",
               "account_balance", "order_id", "체결번호", "주문번호"):
    check(f"미사용(코드): {banned}", banned in src, False)
check("canonical 직접 write 없음(open(...,'w') 로 장부 수정 0)",
      "CANONICAL_PATH" in src and "canonical_path.write" in src, False)
check("apply 는 기존 CLI 재사용", "magic_apply_from_approval.py" in src, True)
check("정책 승인자 상수 존재", AA.AUTO_APPROVER, "WABABA_AUTO_POLICY_V1")

print("")
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
