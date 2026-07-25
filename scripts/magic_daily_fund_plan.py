#!/usr/bin/env python3
"""마법공식 펀드 — 일일 실행계획 + 보고/메일 라우팅 미리보기 생성기.
(Phase MF-DAILY-TOP10-AUTONOMOUS-FUND-REPORTING-CONTRACT)

이 펀드는 사람의 종목 선택 재량 없이 정해진 룰로 운영된다. 정상일에는 대장의 매수 판단·승인이
없으며, 대장은 시스템/데이터/주문 예외만 감독한다. 따라서 이 스크립트는 "매수 추천"이 아니라
**규칙에 따라 확정된 실행계획**을 산출한다.

read-only / TEMP-only. canonical·public·REPO1 write 0. 실제 이메일 발송 0(SMTP/Gmail API 코드 없음) —
제목·본문·수신 라우팅을 텍스트로만 렌더링해 TEMP에 저장한다. 실주문·브로커 API 0.

운영 규칙 출처(추측 아님): scripts/magic_rolling_engine.py 상수·docstring + canonical 장부.
  - 매 개장일 상위10을 1개 batch로 매수(batch당 itemLot 10개)   [engine TOP_N=10]
  - 초기 1~50배치: batch당 allocatedCapital=1,000,000원          [INITIAL_BATCH_CAPITAL]
  - 각 lot은 50거래일 보유, 51번째 거래일에 가장 오래된 batch를 FIFO 전량매도 후 재투자
                                                                  [HOLD_TRADING_DAYS=50]
  - **top10 이탈은 매도 사유가 아니다**(매도는 오직 시간 기반 FIFO)
  - 매수·매도 체결가는 pykrx_open(시가)만 허용. 시가 누락 시 부분매수·fallback 없이 전체 BLOCKED
  - 수수료·세금 미반영(0 가정)                                    [FEE_TAX_MODELED=False]
  - 같은 종목이 여러 batch에 중복 보유될 수 있다(정상)

가격 시점 주의: 신호는 D일 종가 기준이고 체결은 D+1 개장 시가다. 따라서 16:20 시점에는
D+1 시가를 알 수 없으므로 **수량은 null**로 두고 체결 시점에 엔진이 확정한다(임의 추정 금지).

예) python scripts/magic_daily_fund_plan.py                 # 오늘(KST) 신호 기준
    python scripts/magic_daily_fund_plan.py --date 2026-07-24
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date as _date, timedelta
from pathlib import Path

import magic_daily_common as C

# ── 메일 라우팅 계약 ────────────────────────────────────────────────────────────
# 정상(PASS)은 운영 보관 계정으로만 남기고 대장 개인메일로 보내지 않는다.
# WARNING/BLOCKED(예외)만 대장 개인메일 수신 대상이 된다.
# 주소는 라우팅 설정이며 비밀값이 아니다. 인증정보(앱 비밀번호/OAuth 토큰)는 이 파일에 없다.
OPS_ARCHIVE_RECIPIENT = "bridge.ai.office@gmail.com"      # 정상 이력 보관(운영 계정)
FOUNDER_ALERT_RECIPIENT = "duria2002@gmail.com"           # 예외 알림 전용(대장 개인)
SENDER_ACCOUNT = OPS_ARCHIVE_RECIPIENT                    # 발신도 운영 계정
SENDER_CREDENTIAL_ENV = "WABABA_MAGIC_MAIL_APP_PASSWORD"  # 이름만 예약. 값은 읽지 않는다.

PREV_TRADING_DAY_LOOKBACK_LIMIT = 10
NEXT_TRADING_DAY_LOOKAHEAD_LIMIT = 10

# ── 상태 ────────────────────────────────────────────────────────────────────────
PLAN_READY = "PLAN_READY"                         # verdict PASS
PLAN_READY_WITH_WARNING = "PLAN_READY_WITH_WARNING"
PLAN_BLOCKED = "PLAN_BLOCKED"
SELF_SKIPPED_NON_TRADING_DAY = "SELF_SKIPPED_NON_TRADING_DAY"
ALREADY_SENT = "ALREADY_SENT"

# ── 사유 코드 ───────────────────────────────────────────────────────────────────
R_SIGNAL_NOT_READY = "SIGNAL_PACKAGE_NOT_READY"
R_UNIVERSE_STALE = "UNIVERSE_OR_PRICE_STALE"
R_TOP10_INCOMPLETE = "TOP10_COUNT_MISMATCH"
R_TOP10_DUPLICATE = "TOP10_DUPLICATE_CODE"
R_CANONICAL_UNREADABLE = "CANONICAL_LEDGER_UNREADABLE"
R_CANONICAL_BEHIND = "CANONICAL_LEDGER_BEHIND"
R_INSUFFICIENT_CASH = "INSUFFICIENT_AVAILABLE_CASH"
R_MISSING_REFERENCE_PRICE = "MISSING_REFERENCE_PRICE"
R_QTY_AT_EXECUTION = "QUANTITY_DETERMINED_AT_EXECUTION_OPEN"
R_RULE_NEW_BATCH = "RULE_TOP10_NEW_BATCH"
R_RULE_FIFO_SELL = "RULE_FIFO_50_TRADING_DAYS"
R_RULE_WITHIN_HOLD = "RULE_WITHIN_50_TRADING_DAY_HOLD"
R_FEE_NOT_MODELED = "FEE_TAX_NOT_MODELED_BY_DESIGN"

DELIVERY_STATE_PATH = C.REPORTS_DIR / "fund-plan-delivery-state.json"


# ── 엔진 규칙 상수(엔진에서 직접 읽어온다 — 값 복제·재정의 금지) ────────────────
def engine_rules() -> dict:
    import magic_rolling_engine as E
    return {
        "topN": E.TOP_N,
        "holdTradingDays": E.HOLD_TRADING_DAYS,
        "maxOpenBatches": E.MAX_OPEN_BATCHES,
        "initialCapital": E.INITIAL_CAPITAL,
        "initialBatchCapital": E.INITIAL_BATCH_CAPITAL,
        "priceSourceTrade": E.PRICE_SOURCE_TRADE,
        "feeTaxModeled": E.FEE_TAX_MODELED,
    }


# ── 입력 로딩 ───────────────────────────────────────────────────────────────────
def _signal_package_dir(date_iso: str) -> Path:
    return C.TEMP_ROOT / date_iso


def load_rankings(date_iso: str) -> dict | None:
    p = _signal_package_dir(date_iso) / "rankings.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def load_canonical(path: Path | None = None) -> dict | None:
    p = path or C.CANONICAL_PATH
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def previous_trading_day(date_iso: str) -> str | None:
    cursor = _date.fromisoformat(date_iso) - timedelta(days=1)
    for _ in range(PREV_TRADING_DAY_LOOKBACK_LIMIT):
        if C.is_krx_trading_day(cursor.isoformat()):
            return cursor.isoformat()
        cursor -= timedelta(days=1)
    return None


def next_trading_day(date_iso: str) -> str | None:
    cursor = _date.fromisoformat(date_iso) + timedelta(days=1)
    for _ in range(NEXT_TRADING_DAY_LOOKAHEAD_LIMIT):
        if C.is_krx_trading_day(cursor.isoformat()):
            return cursor.isoformat()
        cursor += timedelta(days=1)
    return None


def trading_days_between(start_exclusive: str, end_exclusive: str) -> list:
    """start(제외) ~ end(제외) 사이 거래일 목록."""
    try:
        s = _date.fromisoformat(str(start_exclusive)[:10])
        e = _date.fromisoformat(str(end_exclusive)[:10])
    except (TypeError, ValueError):
        return []
    out = []
    cursor = s + timedelta(days=1)
    while cursor < e:
        if C.is_krx_trading_day(cursor.isoformat()):
            out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


# ── 보유 현황 ───────────────────────────────────────────────────────────────────
def current_holdings(canonical: dict) -> dict:
    """canonical OPEN itemLot 을 종목코드별 합산. 같은 종목의 다중 batch 보유는 정상."""
    agg = {}
    for lot in (canonical.get("itemLots") or []):
        if lot.get("status") != "OPEN":
            continue
        code = str(lot.get("code"))
        row = agg.setdefault(code, {"code": code, "name": lot.get("name"), "quantity": 0, "lotCount": 0})
        row["quantity"] += int(lot.get("quantity") or 0)
        row["lotCount"] += 1
    return agg


def due_exit_batches(canonical: dict, execution_trading_day_index: int) -> list:
    """실행일 기준 50거래일 보유만기에 도달한 OPEN batch(FIFO).
    현재 규칙상 top10 이탈은 매도 사유가 아니다."""
    out = []
    for b in (canonical.get("batches") or []):
        if b.get("status") != "OPEN":
            continue
        planned = b.get("plannedSellTradingDayIndex")
        if planned is None:
            continue
        if int(planned) <= int(execution_trading_day_index):
            out.append(b)
    return out


# ── 실행계획 ────────────────────────────────────────────────────────────────────
def build_execution_plan(execution_date: str, signal_as_of: str, ranking: dict,
                         canonical: dict, rules: dict) -> dict:
    """공식 규칙으로 계산 가능한 범위에서만 실행계획을 만든다.
    체결가(실행일 시가)를 알 수 없으므로 targetQuantity 는 null + 사유코드로 남긴다(임의 추정 금지)."""
    top10 = ranking.get("top10") or []
    held = current_holdings(canonical)

    seq_before = int(canonical.get("officialSequence") or 0)
    tdi_before = int(canonical.get("officialTradingDayIndex") or 0)
    exec_tdi = tdi_before + 1

    batch_capital = float(rules["initialBatchCapital"])
    target_per_stock = round(batch_capital / rules["topN"], 2)
    cash_before = float(canonical.get("officialAvailableCash") or 0)

    exits_batches = due_exit_batches(canonical, exec_tdi)
    exit_batch_ids = {b.get("batchId") for b in exits_batches}

    entries, exits, holds = [], [], []

    # 규칙 기반 편입 대상(신규 batch)
    for t in top10:
        code = str(t.get("code"))
        ref = t.get("signalClosePrice")
        entries.append({
            "stockCode": code,
            "companyName": t.get("name"),
            "currentQuantity": held.get(code, {}).get("quantity", 0),
            "targetQuantity": None,
            "action": "BUY",
            "rank": t.get("rank"),
            "targetAmount": target_per_stock,
            "referencePrice": ref,
            "reasonCode": R_RULE_NEW_BATCH if ref not in (None, 0) else R_MISSING_REFERENCE_PRICE,
        })

    # 규칙 기반 제외 대상(50거래일 보유만기 FIFO)
    for lot in (canonical.get("itemLots") or []):
        if lot.get("status") != "OPEN" or lot.get("batchId") not in exit_batch_ids:
            continue
        exits.append({
            "stockCode": str(lot.get("code")),
            "companyName": lot.get("name"),
            "currentQuantity": int(lot.get("quantity") or 0),
            "targetQuantity": 0,
            "action": "SELL",
            "rank": None,
            "targetAmount": None,
            "referencePrice": None,
            "reasonCode": R_RULE_FIFO_SELL,
        })

    exit_codes = {x["stockCode"] for x in exits}
    for code, row in sorted(held.items()):
        if code in exit_codes:
            continue
        holds.append({
            "stockCode": code,
            "companyName": row["name"],
            "currentQuantity": row["quantity"],
            "targetQuantity": row["quantity"],
            "action": "HOLD",
            "rank": None,
            "targetAmount": None,
            "referencePrice": None,
            "reasonCode": R_RULE_WITHIN_HOLD,
        })

    # 자금: 초기 50배치 구간은 officialAvailableCash 에서 배치당 정액 차감.
    # 51배치 이후 rollover(매도대금 독립 복리) 구간은 매도 체결가에 의존하므로 여기서 추정하지 않는다.
    rollover_phase = seq_before >= int(rules["maxOpenBatches"])
    if rollover_phase:
        planned_cash_use, cash_after = None, None
    else:
        planned_cash_use = batch_capital
        cash_after = round(cash_before - batch_capital, 2)

    open_lots = sum(r["lotCount"] for r in held.values())
    return {
        "executionDate": execution_date,
        "signalAsOfDate": signal_as_of,
        "priceAsOfDate": signal_as_of,
        "formulaVersion": ranking.get("formulaVersion"),
        "batchId": f"MF-BATCH-{execution_date}",
        "officialSequenceBefore": seq_before,
        "officialSequenceAfter": seq_before + 1,
        "executionTradingDayIndex": exec_tdi,
        "currentHoldings": {"uniqueCodes": len(held), "openLots": open_lots,
                            "totalQuantity": sum(r["quantity"] for r in held.values())},
        "targetHoldings": {"uniqueCodesAfterPlan": len(set(held) | {e["stockCode"] for e in entries}),
                           "openLotsAfterPlan": open_lots + len(entries) - len(exits)},
        "entries": entries,
        "exits": exits,
        "holds": holds,
        "cashBefore": cash_before,
        "plannedCashUse": planned_cash_use,
        "cashAfter": cash_after,
        "cashPhase": "ROLLOVER_COMPOUND" if rollover_phase else "INITIAL_BATCH_ALLOCATION",
        "estimatedFees": 0,
        "estimatedFeesReason": R_FEE_NOT_MODELED,
        "realOrderCount": 0,
        "approvalRequired": True,
        "approvalNote": "실주문 미연결. 장부 반영(apply)은 기존 receipt/approval 경로에서 별도 수행.",
        "executionStatus": "PLANNED_NOT_APPLIED",
        "quantityPolicy": R_QTY_AT_EXECUTION,
    }


# ── 안전 게이트 ─────────────────────────────────────────────────────────────────
def evaluate_gates(date_iso: str, execution_date: str | None, ranking: dict | None,
                   canonical: dict | None, plan: dict | None, rules: dict) -> dict:
    """PASS / WARNING / BLOCKED 판정. BLOCKED면 실행계획을 apply 대상으로 넘기지 않는다."""
    blocked, warnings = [], []

    if ranking is None:
        blocked.append({"code": R_SIGNAL_NOT_READY, "detail": f"{date_iso} 신호 패키지 rankings.json 없음"})
        return {"verdict": "BLOCKED", "blocked": blocked, "warnings": warnings}

    if ranking.get("signalAsOfDate") != date_iso or ranking.get("universeBaseDate") != date_iso:
        blocked.append({"code": R_UNIVERSE_STALE,
                        "detail": f"signalAsOfDate={ranking.get('signalAsOfDate')} / "
                                  f"universeBaseDate={ranking.get('universeBaseDate')} != {date_iso}"})

    top10 = ranking.get("top10") or []
    if len(top10) != rules["topN"]:
        blocked.append({"code": R_TOP10_INCOMPLETE, "detail": f"top10 {len(top10)}종 != {rules['topN']}종"})
    codes = [str(t.get("code")) for t in top10]
    if len(set(codes)) != len(codes):
        blocked.append({"code": R_TOP10_DUPLICATE, "detail": "top10 내 중복 종목코드 존재"})
    missing_price = [c for c, t in zip(codes, top10) if t.get("signalClosePrice") in (None, 0)]
    if missing_price:
        warnings.append({"code": R_MISSING_REFERENCE_PRICE,
                         "detail": f"참고 종가 누락 {missing_price} (체결가는 실행일 시가로 별도 확정)"})

    if canonical is None:
        blocked.append({"code": R_CANONICAL_UNREADABLE, "detail": "canonical 장부 읽기 실패"})
        return {"verdict": "BLOCKED", "blocked": blocked, "warnings": warnings}

    # canonical 이 실제 거래일 진행보다 뒤처져 있으면 신규 batch 를 얹을 수 없다(seq/batch 정합 불가).
    cal = canonical.get("officialExecutionCalendar") or []
    missed = {str(m.get("date")) for m in (canonical.get("missedRuns") or [])}
    if cal and execution_date:
        unapplied = [d for d in trading_days_between(cal[-1], execution_date) if d not in missed]
        if unapplied:
            blocked.append({"code": R_CANONICAL_BEHIND,
                            "detail": f"장부 최종 반영일 {cal[-1]} 이후 미반영 거래일 {len(unapplied)}건 "
                                      f"{unapplied} — seq/batch 정합 불가"})

    if plan is not None and plan.get("plannedCashUse") is not None:
        if float(plan["cashBefore"]) < float(plan["plannedCashUse"]):
            blocked.append({"code": R_INSUFFICIENT_CASH,
                            "detail": f"가용현금 {plan['cashBefore']} < 필요 {plan['plannedCashUse']}"})

    verdict = "BLOCKED" if blocked else ("WARNING" if warnings else "PASS")
    return {"verdict": verdict, "blocked": blocked, "warnings": warnings}


# ── 메일 라우팅 계약 ────────────────────────────────────────────────────────────
def resolve_routing(verdict: str) -> dict:
    """정상은 운영 보관 계정만, 예외만 대장 개인메일. 정상일 개인메일 수신자는 0명이어야 한다."""
    if verdict == "PASS":
        return {"to": [OPS_ARCHIVE_RECIPIENT], "purpose": "ARCHIVE",
                "founderNotified": False, "founderRecipientCount": 0,
                "reason": "정상 실행계획 — 대장 개입 불필요, 운영 계정 보관만"}
    return {"to": [FOUNDER_ALERT_RECIPIENT], "purpose": "EXCEPTION_ALERT",
            "founderNotified": True, "founderRecipientCount": 1,
            "reason": f"{verdict} 예외 — 대장 감독 필요"}


# ── 렌더링 ──────────────────────────────────────────────────────────────────────
def _founder_action(verdict: str, gates: dict) -> str:
    if verdict == "PASS":
        return "없음"
    items = gates.get("blocked") or gates.get("warnings") or []
    if not items:
        return "없음"
    code = items[0]["code"]
    return {
        R_CANONICAL_BEHIND: "미반영 거래일의 장부 반영(승인·apply) 여부 확인",
        R_SIGNAL_NOT_READY: "신호 패키지 생성 실패 원인 확인(magic_daily_signal)",
        R_UNIVERSE_STALE: "시세·유니버스 갱신(daily_run) 실패 원인 확인",
        R_INSUFFICIENT_CASH: "가용현금 부족 — 배치 자금 계획 확인",
        R_TOP10_INCOMPLETE: "top10 산출 실패 원인 확인",
        R_TOP10_DUPLICATE: "top10 중복 종목 원인 확인",
        R_CANONICAL_UNREADABLE: "canonical 장부 파일 상태 확인",
        R_MISSING_REFERENCE_PRICE: "참고 종가 누락 종목 확인(체결에는 영향 없음)",
    }.get(code, f"{code} 원인 확인")


def render_report(date_iso: str, execution_date: str | None, plan: dict | None,
                  gates: dict, routing: dict) -> dict:
    verdict = gates["verdict"]
    if verdict == "PASS":
        subject = f"[와바바] {execution_date} 마법공식 펀드 일일 실행계획"
    else:
        tag = "WARNING" if verdict == "WARNING" else "BLOCKED"
        tail = "마법공식 펀드 이상" if verdict == "WARNING" else "마법공식 펀드 중단"
        subject = f"[와바바][{tag}] {date_iso} {tail}"

    L = []
    L.append("[프로젝트] 와바바")
    L.append("[제목] 마법공식 펀드 일일 실행계획")
    L.append(f"[기준 거래일] {date_iso}")
    L.append(f"[전체 판정] {verdict}")
    L.append("[핵심 요약]")
    if verdict == "PASS":
        L.append("- 공식 마법공식 top10 산출 정상")
        L.append("- 데이터 최신성·종목 수·중복 검사 정상")
        L.append("- 운영 규칙에 따른 실행계획 생성")
    else:
        for it in (gates["blocked"] or gates["warnings"])[:3]:
            L.append(f"- {it['code']}: {it['detail']}")
    L.append(f"[대장이 할 일] {_founder_action(verdict, gates)}")
    L.append("[실주문] 0")
    L.append("[브로커 호출] 0")
    L.append(f"[실제 이메일 발송] 0 (라우팅 대상: {', '.join(routing['to'])} · 목적 {routing['purpose']})")
    L.append("")

    if verdict != "PASS":
        L.append("■ 영향 범위")
        L.append(f"- 실행계획 apply 대상 여부: {'아니오(중단)' if verdict == 'BLOCKED' else '예(확인 후 진행)'}")
        L.append("- 장부/공개데이터 변경: 없음 · 실주문: 0건")
        L.append("")

    if plan is not None:
        L.append(f"실행 예정일(체결): {plan['executionDate']} 개장 시가 · 신호 기준일: {plan['signalAsOfDate']} 종가")
        L.append(f"formulaVersion: {plan['formulaVersion']} · batchId: {plan['batchId']} · "
                 f"seq {plan['officialSequenceBefore']} → {plan['officialSequenceAfter']}")
        L.append(f"보유: {plan['currentHoldings']['uniqueCodes']}종목 / "
                 f"{plan['currentHoldings']['openLots']}lot · 가용현금 {plan['cashBefore']:,.0f}원")
        if plan["plannedCashUse"] is not None:
            L.append(f"이번 배치 투입 예정 {plan['plannedCashUse']:,.0f}원 → 예상 잔여현금 {plan['cashAfter']:,.0f}원")
        L.append(f"수수료·세금: {plan['estimatedFees']} ({plan['estimatedFeesReason']})")
        L.append("")
        L.append(f"■ 규칙 기반 편입 대상 {len(plan['entries'])}종 (신규 batch)")
        L.append("순위\t종목코드\t종목명\t목표금액\t참고종가(신호일)\t현재보유\t수량")
        for e in plan["entries"]:
            L.append("{r}\t{c}\t{n}\t{a:,.0f}\t{p}\t{cur}\t{q}".format(
                r=e["rank"], c=e["stockCode"], n=e["companyName"], a=e["targetAmount"],
                p=e["referencePrice"], cur=e["currentQuantity"],
                q="체결시 확정" if e["targetQuantity"] is None else e["targetQuantity"]))
        L.append("")
        L.append(f"■ 규칙 기반 제외 대상 {len(plan['exits'])}종 (50거래일 보유만기 FIFO)")
        if not plan["exits"]:
            L.append("- 없음(만기 도달 배치 없음)")
        else:
            for x in plan["exits"]:
                L.append(f"- {x['companyName']}({x['stockCode']}) {x['currentQuantity']}주 · {x['reasonCode']}")
        L.append("")
        L.append(f"■ 보유 유지 {len(plan['holds'])}종")
        L.append(f"수량 정책: {plan['quantityPolicy']} — 체결가(실행일 시가) 확정 후 엔진이 수량을 산출한다.")
        L.append(f"실행 상태: {plan['executionStatus']} · realOrderCount={plan['realOrderCount']}")
    L.append("")
    L.append("※ 이 펀드는 정해진 규칙으로 자동 운영된다. 위 내용은 규칙에 따른 실행계획이며 "
             "종목 추천이나 대장의 매수 판단 요청이 아니다. 대장은 예외 상황만 감독한다.")
    return {"subject": subject, "body": "\n".join(L)}


# ── 미리보기 파일/멱등성 ────────────────────────────────────────────────────────
def preview_text_path(date_iso: str) -> Path:
    return C.REPORTS_DIR / f"fund-plan-preview-{date_iso}.txt"


def render_preview_file(report: dict, routing: dict) -> str:
    return (f"To: {', '.join(routing['to'])}\n"
            f"From: {SENDER_ACCOUNT}\n"
            f"Purpose: {routing['purpose']}\n"
            f"Subject: {report['subject']}\n\n{report['body']}\n")


def is_already_sent(date_iso: str, state_path: Path = DELIVERY_STATE_PATH) -> bool:
    if not state_path.exists():
        return False
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return bool((state.get("sentDates") or {}).get(date_iso))


def mark_sent(date_iso: str, *, state_path: Path = DELIVERY_STATE_PATH, now: str | None = None) -> None:
    """실제 발송 완료 후에만 호출할 마킹 함수. 이 스크립트 CLI 경로에서는 호출하지 않는다."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            state = {}
    sent = state.get("sentDates") or {}
    sent[date_iso] = now or C.now_kst().isoformat()
    state["sentDates"] = sent
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


# ── 실행 ────────────────────────────────────────────────────────────────────────
def run_fund_plan(date_iso: str, *, state_path: Path = DELIVERY_STATE_PATH,
                  preview_path: Path | None = None, canonical_path: Path | None = None,
                  now: str | None = None) -> dict:
    phase = "FUND_PLAN_PREVIEW"
    now = now or C.now_kst().isoformat()
    rules = engine_rules()

    if not C.is_krx_trading_day(date_iso):
        return {"status": SELF_SKIPPED_NON_TRADING_DAY, "phase": phase, "date": date_iso,
                "verdict": "PASS", "reason": f"{date_iso} not a KRX trading day",
                "founderNotified": False, "founderRecipientCount": 0,
                "realOrderCount": 0, "brokerApiCallCount": 0, "emailSent": False,
                "smtpCallCount": 0, "canonicalChanged": False, "productionWriteCount": 0,
                "createdAt": now}

    if is_already_sent(date_iso, state_path=state_path):
        return {"status": ALREADY_SENT, "phase": phase, "date": date_iso, "verdict": "PASS",
                "reason": f"{date_iso} 이미 발송 완료 기록 있음 — 중복 발송 방지",
                "founderNotified": False, "founderRecipientCount": 0,
                "realOrderCount": 0, "brokerApiCallCount": 0, "emailSent": False,
                "smtpCallCount": 0, "canonicalChanged": False, "productionWriteCount": 0,
                "createdAt": now}

    ranking = load_rankings(date_iso)
    canonical = load_canonical(canonical_path)
    execution_date = (ranking or {}).get("nextExecutionDateCandidate") or next_trading_day(date_iso)

    plan = None
    if ranking is not None and canonical is not None:
        plan = build_execution_plan(execution_date, date_iso, ranking, canonical, rules)

    gates = evaluate_gates(date_iso, execution_date, ranking, canonical, plan, rules)
    verdict = gates["verdict"]
    routing = resolve_routing(verdict)
    report = render_report(date_iso, execution_date, plan, gates, routing)

    ppath = preview_path or preview_text_path(date_iso)
    expected = render_preview_file(report, routing)
    duplicate = False
    if ppath.exists():
        try:
            duplicate = ppath.read_text(encoding="utf-8") == expected
        except OSError:
            duplicate = False

    status = {"PASS": PLAN_READY, "WARNING": PLAN_READY_WITH_WARNING}.get(verdict, PLAN_BLOCKED)
    return {
        "status": status, "phase": phase, "date": date_iso, "verdict": verdict,
        "executionDate": execution_date,
        "gates": gates, "plan": plan, "routing": routing,
        "founderNotified": routing["founderNotified"],
        "founderRecipientCount": routing["founderRecipientCount"],
        "emailSubject": report["subject"], "emailBody": report["body"],
        "previewPath": str(ppath), "duplicate": duplicate,
        "applyEligible": verdict != "BLOCKED",
        "canonicalChanged": False, "publicCopyCount": 0, "productionWriteCount": 0,
        "realOrderCount": 0, "brokerApiCallCount": 0,
        "emailSent": False, "smtpCallCount": 0,
        "createdAt": now,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="마법공식 펀드 일일 실행계획·라우팅 미리보기(dry-run, 실제 발송 없음)")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (생략 시 오늘 KST)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    date_iso = args.date or C.today_kst_iso()
    r = run_fund_plan(date_iso)
    C.write_json_report(C.REPORTS_DIR / f"fund-plan-preview-{date_iso}.json", r)
    if r.get("emailBody"):
        ppath = Path(r["previewPath"])
        ppath.parent.mkdir(parents=True, exist_ok=True)
        ppath.write_text(
            render_preview_file({"subject": r["emailSubject"], "body": r["emailBody"]}, r["routing"]),
            encoding="utf-8")

    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[FUND_PLAN {date_iso}] status={r['status']} verdict={r.get('verdict')} "
              f"founderNotified={r.get('founderNotified')} duplicate={r.get('duplicate')} "
              f"emailSent={r.get('emailSent')}")
    return 2 if r["status"] == PLAN_BLOCKED else 0


if __name__ == "__main__":
    sys.exit(main())
