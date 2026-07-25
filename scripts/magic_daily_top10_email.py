#!/usr/bin/env python3
"""마법공식 일일 top10 매수검토 이메일 — 미리보기/dry-run 생성기.
(Phase MF-DAILY-TOP10-EMAIL-PIPELINE-ROOT-CAUSE-AND-RECOVERY)

read-only / TEMP-only. canonical/public/REPO1 write 0. 실제 이메일 발송 0(SMTP/외부 API 호출 없음) —
제목·본문 텍스트만 생성해 TEMP에 저장한다. 이 Phase에서는 실제 발송기를 만들지 않는다(별도 승인 게이트).

입력: magic_daily_signal.py가 매일 15:40 KST 이후 생성하는 신호 패키지(rankings.json, TEMP,
공식 마법공식 연간 산식 그대로 재사용 — calculate_magic_formula_ranking, TTM 실험값 미사용).
신호가 READY가 아니면(비거래일/장마감전/유니버스 미갱신 등) 이메일을 만들지 않고 그 상태를 그대로 보고한다
(임의 가격·오래된 데이터로 이메일을 만들지 않는다).

예) python scripts/magic_daily_top10_email.py                 # 오늘(KST) 기준 미리보기
    python scripts/magic_daily_top10_email.py --date 2026-07-24
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path

import magic_daily_common as C

RECIPIENT_ENV_VAR = "WABABA_MAGIC_TOP10_EMAIL_RECIPIENT"  # 값은 여기서 읽지 않는다(발송기 Phase 전용)
PREV_TRADING_DAY_LOOKBACK_LIMIT = 10  # 직전 거래일 역탐색 상한(달력일)

BLOCKED_SIGNAL_NOT_READY = "BLOCKED_SIGNAL_NOT_READY"
BLOCKED_TOP10_INCOMPLETE = "BLOCKED_TOP10_INCOMPLETE"
BLOCKED_UNIVERSE_STALE = "BLOCKED_UNIVERSE_STALE"
ALREADY_SENT = "ALREADY_SENT"
DRY_RUN_EMAIL_READY = "DRY_RUN_EMAIL_READY"

DELIVERY_STATE_PATH = C.REPORTS_DIR / "top10-email-delivery-state.json"


def _signal_package_dir(date_iso: str) -> Path:
    return C.TEMP_ROOT / date_iso


def load_rankings(date_iso: str) -> dict | None:
    """해당 날짜 신호 패키지의 rankings.json(없으면 None). 네트워크 0, 로컬 파일만 읽는다."""
    p = _signal_package_dir(date_iso) / "rankings.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def previous_trading_day(date_iso: str) -> str | None:
    """date_iso 이전 가장 최근 거래일(문자열 YYYY-MM-DD). 없으면 None."""
    from datetime import date as _date
    cursor = _date.fromisoformat(date_iso) - timedelta(days=1)
    for _ in range(PREV_TRADING_DAY_LOOKBACK_LIMIT):
        text = cursor.isoformat()
        if C.is_krx_trading_day(text):
            return text
        cursor -= timedelta(days=1)
    return None


def compute_diff(today_top10: list, prev_top10: list | None) -> dict:
    """전 거래일 대비 신규 진입/제외/순위변화. prev_top10 없으면 전부 신규로 표시하지 않고 비교불가로 표기."""
    if prev_top10 is None:
        return {
            "comparisonAvailable": False,
            "rows": [
                {"rank": t.get("rank"), "code": t.get("code"), "name": t.get("name"),
                 "isNew": None, "rankChange": None}
                for t in today_top10
            ],
            "removed": [],
        }

    prev_rank_by_code = {str(t.get("code")): t.get("rank") for t in prev_top10}
    today_codes = {str(t.get("code")) for t in today_top10}
    rows = []
    for t in today_top10:
        code = str(t.get("code"))
        prev_rank = prev_rank_by_code.get(code)
        is_new = prev_rank is None
        rank_change = None if is_new else (prev_rank - t.get("rank"))
        rows.append({
            "rank": t.get("rank"), "code": code, "name": t.get("name"),
            "isNew": is_new, "rankChange": rank_change,
        })
    removed = [
        {"code": code, "name": t.get("name"), "prevRank": t.get("rank")}
        for t in prev_top10 if str(t.get("code")) not in today_codes
        for code in [str(t.get("code"))]
    ]
    return {"comparisonAvailable": True, "rows": rows, "removed": removed}


def _rank_change_text(row: dict) -> str:
    if row["isNew"] is None:
        return "비교불가(직전 거래일 데이터 없음)"
    if row["isNew"]:
        return "신규 진입"
    rc = row["rankChange"]
    if rc == 0:
        return "순위 변동 없음"
    return f"{'▲' if rc > 0 else '▼'}{abs(rc)}"


def render_email(date_iso: str, ranking: dict, diff: dict) -> dict:
    """이메일 제목·본문(텍스트) 렌더링. 실제 발송 없음 — 문자열만 생성한다."""
    subject = f"[와바바] {date_iso} 마법공식 매수 검토 10종목"

    lines = []
    lines.append("[프로젝트] 와바바")
    lines.append("[제목] 마법공식 일일 매수 검토 종목")
    lines.append(f"[기준 거래일] {date_iso}")
    lines.append("[전체 판정] PASS")
    lines.append(
        f"[핵심 요약] {ranking.get('signalAsOfDate')} 종가·{ranking.get('universeBaseDate')} 기준 "
        f"eligible {ranking.get('eligibleCount')}종목 재계산 결과 상위 10종목"
    )
    lines.append("[대장이 할 일] 오늘 매수 여부를 직접 판단")
    lines.append("[실주문] 0")
    lines.append("")
    lines.append(f"formulaVersion: {ranking.get('formulaVersion')}")
    lines.append(f"데이터 기준일(시세): {ranking.get('signalAsOfDate')}")
    lines.append(f"유니버스 기준일(재무): {ranking.get('universeBaseDate')}")
    lines.append("")
    lines.append("순위\t종목코드\t종목명\t싼회사순위\t잘버는회사순위\t종합점수\t기준종가\t전일대비")
    for row in diff["rows"]:
        # ranking에서 원본 행 찾아 valueRank/profitabilityRank/combinedRank/signalClosePrice 병합
        src = next((t for t in ranking.get("top10") or [] if str(t.get("code")) == row["code"]), {})
        lines.append(
            "{rank}\t{code}\t{name}\t{value}\t{prof}\t{combined}\t{price}\t{chg}".format(
                rank=row["rank"], code=row["code"], name=row["name"],
                value=src.get("valueRank"), prof=src.get("profitabilityRank"),
                combined=src.get("combinedRank"), price=src.get("signalClosePrice"),
                chg=_rank_change_text(row),
            )
        )
    lines.append("")
    if diff["removed"]:
        removed_text = ", ".join(f"{r['name']}({r['code']})" for r in diff["removed"])
        lines.append(f"어제 top10에서 제외된 종목: {removed_text}")
    else:
        lines.append("어제 top10 대비 제외된 종목 없음" if diff["comparisonAvailable"] else "전일 비교 데이터 없음")
    lines.append("")
    lines.append("※ 이 보고서는 마법공식 계산 결과이며 투자 추천이 아닙니다. 매수 여부·수량·시점 판단과 "
                  "실제 주문은 대장이 직접 수행합니다.")

    return {"subject": subject, "body": "\n".join(lines)}


def is_already_sent(date_iso: str, state_path: Path = DELIVERY_STATE_PATH) -> bool:
    """이 날짜에 이미 실제 발송 완료 기록이 있는지(멱등성 판정용, 순수 함수).
    이 Phase는 실제 발송을 하지 않으므로 상시 False가 정상이다 — 미래 발송기 Phase가 mark_sent()를 호출한다."""
    if not state_path.exists():
        return False
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return bool((state.get("sentDates") or {}).get(date_iso))


def mark_sent(date_iso: str, *, state_path: Path = DELIVERY_STATE_PATH, now: str | None = None) -> None:
    """실제 발송 완료 후에만 호출할 마킹 함수. 이 스크립트의 CLI 경로에서는 절대 호출하지 않는다
    (호출부는 미래의 실제 발송기 Phase). 원자적 쓰기는 아니지만 delivery-state는 TEMP 전용 저경합 파일이다."""
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


def preview_text_path(date_iso: str) -> Path:
    return C.REPORTS_DIR / f"top10-email-preview-{date_iso}.txt"


def render_preview_file(email: dict) -> str:
    """미리보기 파일(.txt) 내용. 같은 입력이면 항상 같은 문자열(멱등성 판정 기준)."""
    return f"Subject: {email['subject']}\n\n{email['body']}\n"


def run_top10_email(date_iso: str, *, state_path: Path = DELIVERY_STATE_PATH,
                    preview_path: Path | None = None, now: str | None = None) -> dict:
    phase = "TOP10_EMAIL_PREVIEW"
    now = now or C.now_kst().isoformat()

    # 1) 거래일 self-skip(신호가 애초에 생성되지 않는 날)
    if not C.is_krx_trading_day(date_iso):
        return {"status": "SELF_SKIPPED_NON_TRADING_DAY", "phase": phase, "date": date_iso,
                "reason": f"{date_iso} not a KRX trading day", "autoStopped": True,
                "noFakeTrade": True, "realOrderCount": 0, "emailSent": False, "createdAt": now}

    # 2) 이미 발송 완료(멱등성 — 이 Phase에서는 mark_sent가 호출된 적이 없어 항상 통과)
    if is_already_sent(date_iso, state_path=state_path):
        return {"status": ALREADY_SENT, "phase": phase, "date": date_iso,
                "reason": f"{date_iso} 이미 발송 완료 기록 있음 — 중복 발송 방지", "autoStopped": True,
                "noFakeTrade": True, "realOrderCount": 0, "emailSent": False, "createdAt": now}

    # 3) 신호 패키지 로드 — READY가 아니면(비거래일 외 사유) 이메일 생성 안 함
    ranking = load_rankings(date_iso)
    if ranking is None:
        return C.blocked_report(phase, BLOCKED_SIGNAL_NOT_READY, date_iso,
                                f"신호 패키지 rankings.json 없음: {_signal_package_dir(date_iso)}",
                                signal_as_of=date_iso, now=now,
                                recommended_fix="magic_daily_signal.py 실행/재실행 확인")

    # 4) 시세·유니버스 최신성 재확인(신호 패키지는 READY일 때만 만들어지므로 baseDate==signalAsOfDate가
    #    보장되지만, 방어적으로 한 번 더 확인한다 — 임의 가격으로 이메일을 만들지 않는다).
    signal_as_of = ranking.get("signalAsOfDate")
    universe_base = ranking.get("universeBaseDate")
    if signal_as_of != date_iso or universe_base != date_iso:
        return C.blocked_report(phase, BLOCKED_UNIVERSE_STALE, date_iso,
                                f"signalAsOfDate={signal_as_of} / universeBaseDate={universe_base} "
                                f"!= {date_iso} — 시세·유니버스가 최신이 아님",
                                signal_as_of=signal_as_of, now=now,
                                recommended_fix="magic_daily_signal.py 재실행 후 재시도")

    top10 = ranking.get("top10") or []
    if len(top10) != 10:
        return C.blocked_report(phase, BLOCKED_TOP10_INCOMPLETE, date_iso,
                                f"top10 길이 {len(top10)} != 10",
                                signal_as_of=signal_as_of, now=now,
                                recommended_fix="ranking 산출 확인")

    # 5) 전 거래일 대비 비교(있으면)
    prev_date = previous_trading_day(date_iso)
    prev_ranking = load_rankings(prev_date) if prev_date else None
    prev_top10 = (prev_ranking or {}).get("top10")
    diff = compute_diff(top10, prev_top10)

    email = render_email(date_iso, ranking, diff)

    # 6) 동일 거래일 재실행 멱등성: 이미 같은 내용의 미리보기가 있으면 duplicate 로 표시한다.
    #    (파일은 덮어써도 내용이 동일하므로 중복 산출물이 쌓이지 않는다 — 스케줄러 재실행 안전)
    ppath = preview_path or preview_text_path(date_iso)
    expected = render_preview_file(email)
    duplicate = False
    if ppath.exists():
        try:
            duplicate = ppath.read_text(encoding="utf-8") == expected
        except OSError:
            duplicate = False

    return {
        "status": DRY_RUN_EMAIL_READY, "phase": phase, "date": date_iso,
        "signalAsOfDate": signal_as_of, "universeBaseDate": universe_base,
        "formulaVersion": ranking.get("formulaVersion"),
        "prevTradingDate": prev_date, "comparisonAvailable": diff["comparisonAvailable"],
        "newEntryCount": sum(1 for r in diff["rows"] if r["isNew"]),
        "removedCount": len(diff["removed"]),
        "emailSubject": email["subject"], "emailBody": email["body"],
        "previewPath": str(ppath), "duplicate": duplicate,
        "recipientEnvVar": RECIPIENT_ENV_VAR, "recipientConfigured": False,
        "autoStopped": False, "noFakeTrade": True, "realOrderCount": 0, "brokerApiCallCount": 0,
        "emailSent": False, "smtpCallCount": 0,
        "createdAt": now,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 일일 top10 이메일 미리보기(dry-run, 실제 발송 없음)")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (생략 시 오늘 KST)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    date_iso = args.date or C.today_kst_iso()
    r = run_top10_email(date_iso)
    C.write_json_report(C.REPORTS_DIR / f"top10-email-preview-{date_iso}.json", r)
    if r.get("status") == DRY_RUN_EMAIL_READY:
        ppath = Path(r["previewPath"])
        ppath.parent.mkdir(parents=True, exist_ok=True)
        ppath.write_text(
            render_preview_file({"subject": r["emailSubject"], "body": r["emailBody"]}),
            encoding="utf-8")

    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[TOP10_EMAIL {date_iso}] status={r['status']} "
              f"duplicate={r.get('duplicate')} "
              f"reason={r.get('reason', '')} emailSent={r.get('emailSent')}")
    return 0 if r["status"] in (DRY_RUN_EMAIL_READY, "SELF_SKIPPED_NON_TRADING_DAY", ALREADY_SENT) else 2


if __name__ == "__main__":
    sys.exit(main())
