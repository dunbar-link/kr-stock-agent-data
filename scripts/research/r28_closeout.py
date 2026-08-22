#!/usr/bin/env python3
"""R28 closeout — 수집 재개 시도 결과 · coverage 재측정 · R27 재실행 상태.

WABABA-R27-LIQUIDITY-DATA-RESUME-R28

R27 precommit 을 **변경하지 않는다**(§1·§2). 새 연구설계·새 verdict taxonomy 를
만들지 않는다(§32). 기존 성공 111일을 지우고 처음부터 받지 않는다(§41).

안전: 계산·읽기 전용 + reports/research write. 네트워크 0 (수집은 별도 게이트).
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402
import r27_collect as CO  # noqa: E402
from r16_audit import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "krx-liquidity"
REQUIRED_COLS = {"ticker", "close", "marketCap", "volume", "tradedValue", "shares"}

# 2026-08-22 KRX 가 이 수집을 차단하며 반환한 실제 문구(요지).
KRX_NOTICE = (
    "자동화 수단을 통한 비정상 대량 조회가 감지되어 해당 IP의 접속이 일시적으로 "
    "제한되었습니다. KRX Data Marketplace 이용약관 제10조 제2호는 자동화 수단을 "
    "이용한 정보 무단 수집·복제·배포를 금지하고 있으며, 제6조 제2항에 따라 약관 "
    "위반시 홈페이지 이용이 제한될 수 있습니다. 해당 IP는 탐지일로부터 1일간 "
    "접속이 제한되며 ... 일정 기간의 데이터를 일괄적으로 이용하고자 하는 경우 "
    "KRX Data Marketplace의 화면 다운로드 기능 이용, 데이터 상품 구입 또는 "
    "KRX Open API(openapi.krx.co.kr) 등 공식 경로를 이용해 주시기 바랍니다.")


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r28-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r28] saved {p.name}", file=sys.stderr)


def validate_day(d):
    """(valid, rows, sha256). schema·행수 검증. 손상 캐시를 재사용하지 않는다(§3·§8)."""
    p = CACHE / f"{d}.csv.gz"
    if not p.exists():
        return False, 0, None
    try:
        raw = p.read_bytes()
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            r = csv.DictReader(fh)
            cols = set(r.fieldnames or [])
            rows = list(r)
    except (OSError, EOFError, ValueError):
        return False, 0, None
    if not REQUIRED_COLS <= cols or not rows:
        return False, len(rows), None
    # §8 missing ≠ 0 — 필드가 비어 있으면 0 으로 채우지 않고 결측으로 둔다.
    bad = sum(1 for x in rows
              if x.get("tradedValue") in (None, "", "None")
              or x.get("volume") in (None, "", "None"))
    if bad == len(rows):
        return False, len(rows), None
    return True, len(rows), hashlib.sha256(raw).hexdigest()[:16]


def main() -> int:
    cal = CO.trading_calendar()
    ds = contiguous_span(sorted(C.load_capital_series()))
    need = CO.needed_days(cal, ds)
    need_set = set(need)

    cached = sorted(p.name[:-7] for p in CACHE.glob("*.csv.gz"))
    valid, invalid, rows_total = [], [], 0
    for d in cached:
        ok, n, sha = validate_day(d)
        if ok:
            valid.append({"date": d, "rows": n, "sha256": sha,
                          "inNeed": d in need_set})
            rows_total += n
        else:
            invalid.append({"date": d, "rows": n})
    valid_in_need = [x for x in valid if x["inNeed"]]
    pending = [d for d in need if d not in {x["date"] for x in valid_in_need}]

    inv = json.loads((RD / "r27-data-inventory-latest.json")
                     .read_text(encoding="utf-8"))

    # ── §4 정확한 잔여 산출 ───────────────────────────────────────────
    save("collection-resume", {
        "task": "R28", "taskId": "WABABA-R27-LIQUIDITY-DATA-RESUME-R28",
        "requiredDays": len(need),
        "alreadyValid": len(valid_in_need),
        "cachedFilesTotal": len(cached),
        "cachedOutsideNeed": len(valid) - len(valid_in_need),
        "invalidOrCorrupt": len(invalid),
        "pending": len(pending),
        "newlyCollected": 0,
        "retriesNeeded": len(pending),
        "rowsInValidCache": rows_total,
        "cachedRange": [valid[0]["date"], valid[-1]["date"]] if valid else None,
        "r27ReportedCollected": inv.get("daysCollectedTotal"),
        "matchesR27Report": len(valid_in_need) == inv.get("daysCollectedTotal"),
        "reuseRule": ("기존 성공 111일은 schema·행수·sha 검증 후 재사용한다. "
                      "재다운로드 0. 기존 성공분 삭제 후 처음부터 수집 금지(§3·§41)."),
        "resumeAttempted": True,
        "resumeResult": "BLOCKED_KRX_TOS",
        "invalidDetail": invalid[:20]})

    # ── §11 coverage 재측정 (R27 precommit 기준 그대로) ───────────────
    pc = json.loads((RD / "r27-precommit-latest.json").read_text(encoding="utf-8"))
    covreq = pc["coverage"]
    day_cov = round(100.0 * len(valid_in_need) / len(need), 3) if need else 0.0
    cov27 = json.loads((RD / "r27-liquidity-coverage-latest.json")
                       .read_text(encoding="utf-8"))
    save("collection-coverage", {
        "task": "R28",
        "gateFromR27Precommit": covreq,
        "gateUnchanged": True,
        "dayCoveragePct": day_cov,
        "requiredDays": len(need), "collectedDays": len(valid_in_need),
        "byYear": cov27["byYear"],
        "byExchange": cov27["byExchange"],
        "bySizeBucket": cov27["bySizeBucket"],
        "yearsMeetingThreshold": cov27["yearsMeetingThreshold"],
        "minYearsCovered": covreq["minYearsCovered"],
        "coverageGatePass": False,
        "note": ("수집이 재개되지 않아 coverage 가 R27 과 동일하다. "
                 "게이트를 완화하지 않았다(§11)."),
        "suspension": pc["suspension"]})

    # ── §13 실패 원인 분리: collector bug vs source limitation ────────
    save("failure-retry", {
        "task": "R28",
        "rootCauseClass": "SERVICE_TERMS_PROHIBITION",
        "notCollectorBug": True,
        "evidence": {
            "loginPageReachable": True,
            "loginPageHttpStatus": 200,
            "loginApiReturnedHtmlErrorPage": True,
            "krxNotice": KRX_NOTICE,
            "blockDurationStated": "탐지일로부터 1일",
            "blockObservedLiftedAt": "2026-08-23 (단발 확인 — 대량 재개 안 함)"},
        "ruledOut": [
            "encoding — 응답이 정상 UTF-8 HTML 이었다",
            "date format — 차단 전 동일 포맷으로 111일 성공",
            "pykrx parameter mismatch — 동일 호출로 2007~2026 실측 성공한 바 있음",
            "empty dataframe/holiday handling — 캘린더 정본 사용, 빈 결과 0 저장 안 함",
            "cache corruption — 손상 캐시 0건(검증 완료)",
            "network timeout — 로그인 페이지 200 정상 응답"],
        "collectorFixesApplied": [
            "SC1 재시도·백오프·세션복구 + 연속실패 25회 중단(R27)",
            "SC2 pykrx import 실패도 status 로 기록(R27)",
            "SC3 약관 게이트 — 승인 없는 대량 수집 기본 차단(R28)"],
        "guard": {
            "bulkLimitWithoutApproval": CO.BULK_LIMIT_WITHOUT_APPROVAL,
            "approvalEnv": CO.APPROVAL_ENV,
            "exitCode": 3,
            "why": ("차단이 해제돼도 같은 대량 수집을 반복하면 약관 위반을 알고도 "
                    "되풀이하는 것이다. 또 재차단되면 pykrx 를 매일 쓰는 운영 "
                    "예약 runner(15:40/16:20/16:25)가 함께 죽는다.")},
        "sanctionedPaths": [
            {"path": "KRX Data Marketplace 화면 다운로드", "who": "Founder 수동",
             "cost": 0, "blockedBy": "자동화 불가 — 사람이 직접 받아야 한다"},
            {"path": "데이터 상품 구입", "who": "Founder 결제",
             "cost": "유료", "blockedBy": "R27 precommit·R28 §36 유료데이터 0"},
            {"path": "KRX Open API (openapi.krx.co.kr)", "who": "Founder 발급",
             "cost": 0, "blockedBy": "신규 credential 발급 — 승인 게이트"}],
        "founderDecisionRequired": True})

    # ── §14~§16 R27 재실행 상태 ───────────────────────────────────────
    sz = json.loads((RD / "r27-size-reproduction-latest.json")
                    .read_text(encoding="utf-8"))
    bm = json.loads((RD / "r27-bm-reference-latest.json")
                    .read_text(encoding="utf-8"))
    save("r27-reexecution", {
        "task": "R28",
        "precommitUnchanged": True,
        "precommitPath": "reports/research/r27-precommit-latest.json",
        "primaryGate": pc["primaryGate"],
        "sensitivity": pc["sensitivity"],
        "sizeReproduction": sz["r25"],
        "sizeReproductionR26Top": sz["r26"],
        "bmReproductionR26Top": bm["r26"],
        "allReproductionExact": sz["allExactMatch"],
        "r27RunExecuted": True,
        "r27RunStoppedAt": "COVERAGE_GATE",
        "downstreamAnalysis": "NOT_RUN",
        "downstreamNotRunList": [
            "before-after", "participation", "low-price", "included-excluded",
            "delisting-distress", "subperiod-rolling", "exchange",
            "concentration", "bm-vs-size"],
        "why": ("coverage 미달 상태에서 before/after 를 최종 사실처럼 계산하지 "
                "않는다(§12).")})

    v27 = json.loads((RD / "r27-verdict-latest.json").read_text(encoding="utf-8"))
    save("final-verdict", {
        "task": "R28",
        "sizeVerdict": v27["verdict"],
        "verdictTaxonomyFromR27": True,
        "newTaxonomy": False,
        "executionCandidate": v27["executionCandidate"],
        "portfolioResearchEntryAllowed": False,
        "blocker": "KRX_TOS_AUTOMATED_BULK_COLLECTION_PROHIBITED",
        "blockerClass": "BLOCKED_EXTERNAL_APPROVAL",
        "coveragePct": day_cov,
        "requiredDays": len(need), "collectedDays": len(valid_in_need),
        "pendingDays": len(pending),
        "nextTaskRule": "D",
        "nextTask": ("collector/data source 병목 하나만 — Founder 가 공식 경로 "
                     "3개 중 하나를 선택하면 그 경로로 유동성 데이터를 확보한다. "
                     "새 factor 연구 금지(§34-D)."),
        "founderDecisionRequired": True,
        "productionRisk": ("pykrx 는 운영 예약 runner(15:40 signal · 16:20 fund "
                           "plan · 16:25 auto apply)가 매일 쓴다. 대량 수집 재개로 "
                           "재차단되면 그 운영이 함께 멈춘다.")})

    print(json.dumps({"requiredDays": len(need),
                      "alreadyValid": len(valid_in_need),
                      "newlyCollected": 0, "pending": len(pending),
                      "invalid": len(invalid),
                      "coveragePct": day_cov,
                      "rootCause": "SERVICE_TERMS_PROHIBITION",
                      "sizeVerdict": v27["verdict"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
