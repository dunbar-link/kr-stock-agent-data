#!/usr/bin/env python3
"""와바바 공개 펀드 freshness 판정 — HOTG observer 가 읽을 evidence 를 만든다.

WABABA-HOTG-PUBLIC-FRESHNESS-OBSERVER-HARDENING-R2

왜 필요한가
-----------
기존 HOTG observer(AI-OPS-WABABA-STATUS-SHOW)는 sync 갱신시각·예약 Ready 상태만
보고 PASS 를 냈다. 그래서 2026-08-19~08-28 사이 publish 가 7거래일 연속 실패하는
동안에도 매일 PASS 였다(R1 감사에서 확인). 장부와 예약은 정말 정상이었고,
**공개 사이트가 멈춘 것만 아무도 안 봤다.**

이 스크립트가 그 빈칸을 메운다. 판정 근거는 mtime·sync timestamp 가 아니라
**논리 날짜**(logical trade date)다.

설계 원칙
---------
- 새 market calendar 를 만들지 않는다. `magic_daily_common.is_krx_trading_day`
  (기존 큐레이션 KRX 휴장표, 네트워크 0)를 그대로 쓴다.
- 오늘 날짜와 단순 비교하지 않는다. publish deadline 이 지나기 전에는 직전
  거래일까지만 요구한다(주말·휴장일·장중 false positive 방지).
- MISSED_RUN 으로 정당하게 기록된 과거 거래일은 gap 에서 제외한다. 과거 결번
  하나로 영구 BLOCKED 를 만들지 않는다.
- benchmark 는 base date 와 latest observation date 를 구분한다. base date 가
  과거인 것은 설계이지 stale 이 아니다.
- **검사 실패를 정상으로 위장하지 않는다.** fetch/parse/calendar 판정 불가는
  PASS 가 아니라 최소 WARNING 이다(fail-closed).

안전: 읽기 전용 + evidence JSON 1개 write. 네트워크는 공개 URL GET 만.
      매매 0 · 장부 수정 0 · 배포 0 · 전략 변경 0.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import magic_daily_common as C  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "magic-formula-official-state.json"
PUBLIC_REPO_JSON = Path("C:/work/kr-stock-agent/public/data/recommendation-history.json")
OUT_PATH = ROOT / "reports" / "wababa-public-freshness-latest.json"

PUBLIC_URL = "https://kr-stock-agent.vercel.app/data/recommendation-history.json"

# publish deadline — 기존 예약(Wababa Auto Publish, 거래일 17:00 KST)에 맞춘다.
# 배포 전파 여유를 30분 준다. 이 시각 전에는 "오늘치가 아직 안 올라온 것"이
# 정상이므로 직전 거래일까지만 요구한다.
PUBLISH_DEADLINE_HOUR = 17
PUBLISH_DEADLINE_GRACE_MIN = 30

HTTP_TIMEOUT = 25

# stale 1거래일은 다음 실행에서 자연 회복될 수 있어 WARNING,
# 2거래일 이상은 자연 회복이 이미 실패했다는 뜻이라 BLOCKED.
BLOCKED_STALE_TRADING_DAYS = 2


# ══════════════════ 거래일 유틸 (기존 캘린더 재사용) ══════════════════
def prev_trading_day(iso: str) -> str:
    d = _date.fromisoformat(str(iso)[:10]) - timedelta(days=1)
    for _ in range(30):
        if C.is_krx_trading_day(d.isoformat()):
            return d.isoformat()
        d -= timedelta(days=1)
    return d.isoformat()


def last_trading_day_on_or_before(iso: str) -> str:
    d = _date.fromisoformat(str(iso)[:10])
    for _ in range(30):
        if C.is_krx_trading_day(d.isoformat()):
            return d.isoformat()
        d -= timedelta(days=1)
    return d.isoformat()


def trading_days_between(start_exclusive: str, end_inclusive: str) -> list:
    """(start, end] 사이 실제 거래일 목록. 결과가 '거래일 수' 의 정본이다."""
    try:
        s = _date.fromisoformat(str(start_exclusive)[:10])
        e = _date.fromisoformat(str(end_inclusive)[:10])
    except (TypeError, ValueError):
        return []
    out, cur = [], s + timedelta(days=1)
    while cur <= e:
        if C.is_krx_trading_day(cur.isoformat()):
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def expected_public_trade_date(now: datetime) -> dict:
    """지금 시점에 공개 사이트가 보여주고 있어야 할 마지막 거래일.

    오늘 날짜를 그대로 요구하지 않는다 — 주말·휴장일·장중·publish 전이면
    직전 거래일이 정답이다. 이 함수가 false positive 방지의 핵심이다.
    """
    today = now.date().isoformat()
    today_is_trading = C.is_krx_trading_day(today)
    deadline = now.replace(hour=PUBLISH_DEADLINE_HOUR,
                           minute=PUBLISH_DEADLINE_GRACE_MIN,
                           second=0, microsecond=0)
    deadline_passed = today_is_trading and now >= deadline
    if today_is_trading and deadline_passed:
        expected = today
    elif today_is_trading:
        expected = prev_trading_day(today)          # 오늘치는 아직 정상 대기
    else:
        expected = last_trading_day_on_or_before(today)   # 주말·휴장일
    return {
        "expectedPublicTradeDate": expected,
        "today": today,
        "todayIsTradingDay": today_is_trading,
        "publishDeadline": deadline.isoformat(timespec="minutes"),
        "publishDeadlinePassed": bool(deadline_passed),
        "rule": ("거래일이고 publish deadline 이 지났으면 오늘, 아직이면 직전 거래일. "
                 "비거래일이면 직전 거래일. 오늘 날짜를 그대로 요구하지 않는다."),
    }


# ══════════════════ 논리 날짜 읽기 (mtime 아님) ══════════════════
def read_canonical() -> dict:
    out = {"source": str(CANONICAL), "ok": False, "latestTradeDate": None,
           "missedRunDates": [], "error": None}
    try:
        d = json.loads(CANONICAL.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        out["error"] = f"{type(e).__name__}: {e}"
        return out
    dl = d.get("dailyLedger") or []
    if not dl:
        out["error"] = "dailyLedger empty"
        return out
    out.update({
        "ok": True,
        "latestTradeDate": str(dl[-1].get("date"))[:10],
        "officialSequence": d.get("officialSequence"),
        "officialTradingDayIndex": d.get("officialTradingDayIndex"),
        "totalAsset": dl[-1].get("totalAsset"),
        "cumulativeReturnPct": dl[-1].get("cumulativeReturn"),
        "missedRunDates": sorted(str(m.get("date"))[:10]
                                 for m in (d.get("missedRuns") or [])),
    })
    return out


def _extract_public_dates(doc: dict) -> dict:
    """공개 산출물에서 표면별 **논리** 날짜를 뽑는다.

    benchmark 는 base date 가 아니라 **series 의 마지막 관측일**을 쓴다.
    base date(2026-06-17)는 펀드 시작일이라 영원히 과거다 — 그걸 stale 로
    읽으면 매일 오탐이 난다.
    """
    ms = doc.get("magicOfficialSummary") or {}
    td = doc.get("magicOfficialTradeDays") or []
    bm = doc.get("magicOfficialBenchmark") or {}
    bm_latest = None
    if isinstance(bm.get("latest"), dict):
        bm_latest = str(bm["latest"].get("date") or "")[:10] or None
    if not bm_latest:
        series = bm.get("series") or []
        if series:
            try:
                bm_latest = max(str(s.get("date"))[:10] for s in series)
            except (TypeError, ValueError):
                bm_latest = None
    trade_max = None
    if td:
        try:
            trade_max = max(str(x.get("date"))[:10] for x in td)
        except (TypeError, ValueError):
            trade_max = None
    return {
        "dashboardBaseDate": str(doc.get("baseDate") or "")[:10] or None,
        "fundDataDate": str(ms.get("dataDate") or "")[:10] or None,
        "rankingsDate": str(ms.get("latestTradingDate")
                            or ms.get("dataDate") or "")[:10] or None,
        "tradeDaysMaxDate": trade_max,
        "benchmarkLatestDate": bm_latest,
        "benchmarkBaseDate": str(bm.get("baseDate") or "")[:10] or None,
        "officialSequence": ms.get("officialSequence"),
        "totalAsset": ms.get("totalAsset"),
        "cumulativeReturnPct": ms.get("cumulativeReturn"),
    }


def read_public_http(url: str = PUBLIC_URL) -> dict:
    """운영이 실제로 서빙하는 산출물. 여기가 진짜 공개 상태다."""
    out = {"source": url, "ok": False, "error": None, "httpStatus": None}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "wababa-freshness-observer"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            out["httpStatus"] = r.status
            doc = json.loads(r.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError) as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        return out
    out["ok"] = True
    out.update(_extract_public_dates(doc))
    return out


def read_public_repo_file(path: Path = PUBLIC_REPO_JSON) -> dict:
    """공개 저장소 워킹트리 사본(보조 근거). 운영 판정의 정본은 HTTP 쪽이다."""
    out = {"source": str(path), "ok": False, "error": None}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        return out
    out["ok"] = True
    out.update(_extract_public_dates(doc))
    return out


# ══════════════════ 판정 ══════════════════
def gap_trading_days(latest: str, expected: str, missed: list) -> list:
    """latest 이후 expected 까지의 거래일 중 **정당한 MISSED_RUN 을 뺀** 결번."""
    if not latest or not expected:
        return []
    missed_set = {str(m)[:10] for m in (missed or [])}
    return [d for d in trading_days_between(latest, expected)
            if d not in missed_set]


def decide(now: datetime, canonical: dict, public: dict, repo_copy: dict) -> dict:
    exp = expected_public_trade_date(now)
    expected = exp["expectedPublicTradeDate"]
    missed = canonical.get("missedRunDates") or []

    checks, reasons = {}, []
    verdict = "PASS"

    def bump(v):
        nonlocal verdict
        order = {"PASS": 0, "WAIT": 1, "WARNING": 2, "BLOCKED": 3}
        if order[v] > order[verdict]:
            verdict = v

    # ── fail-closed: 검사 자체가 안 되면 정상으로 위장하지 않는다 ──
    if not canonical.get("ok"):
        checks["canonicalReadable"] = False
        reasons.append(f"canonical 판독 실패 — {canonical.get('error')}")
        bump("WARNING")
    else:
        checks["canonicalReadable"] = True

    if not public.get("ok"):
        checks["publicFetchable"] = False
        reasons.append(f"공개 산출물 fetch/parse 실패 — {public.get('error')} "
                       "(검사 실패를 정상으로 취급하지 않는다)")
        bump("WARNING")
    else:
        checks["publicFetchable"] = True

    if not C.is_krx_trading_day(expected) and expected:
        checks["calendarResolvable"] = False
        reasons.append("거래일 캘린더 판정 불가")
        bump("WARNING")
    else:
        checks["calendarResolvable"] = True

    canon_latest = canonical.get("latestTradeDate")
    pub_latest = public.get("fundDataDate") if public.get("ok") else None

    canon_gap = gap_trading_days(canon_latest, expected, missed) if canon_latest else []
    pub_gap = gap_trading_days(pub_latest, expected, missed) if pub_latest else []

    # ── canonical 장부 ──
    if canon_latest:
        checks["canonicalCurrent"] = not canon_gap
        if canon_gap:
            reasons.append(f"canonical 장부가 {len(canon_gap)}거래일 뒤처짐 "
                           f"({canon_latest} → 기대 {expected})")
            bump("BLOCKED" if exp["publishDeadlinePassed"] or not exp["todayIsTradingDay"]
                 else "WAIT")

    # ── 공개 핵심 표면(펀드 데이터) ──
    if pub_latest:
        checks["publicCurrent"] = not pub_gap
        if pub_gap:
            n = len(pub_gap)
            if not exp["publishDeadlinePassed"] and exp["todayIsTradingDay"] and n <= 1:
                reasons.append(f"오늘 publish 전 정상 대기 (공개 {pub_latest}, "
                               f"deadline {exp['publishDeadline']})")
                bump("WAIT")
            elif n >= BLOCKED_STALE_TRADING_DAYS:
                reasons.append(f"공개 펀드 데이터가 {n}거래일 stale "
                               f"({pub_latest} → 기대 {expected}) — 자연 회복이 "
                               f"이미 여러 번 실패했다는 뜻")
                bump("BLOCKED")
            else:
                reasons.append(f"공개 펀드 데이터가 {n}거래일 stale "
                               f"({pub_latest} → 기대 {expected})")
                bump("WARNING")
    elif public.get("ok"):
        checks["publicCurrent"] = False
        reasons.append("공개 산출물에 fundDataDate 가 없다 — 필드 누락")
        bump("WARNING")

    # ── 보조 표면 (설계상 같은 기준일을 써야 하는 것들) ──
    surface_mismatch = []
    if public.get("ok") and pub_latest:
        for key in ("dashboardBaseDate", "rankingsDate", "tradeDaysMaxDate",
                    "benchmarkLatestDate"):
            v = public.get(key)
            if v and v != pub_latest:
                surface_mismatch.append({"surface": key, "date": v})
    checks["publicSurfacesAligned"] = not surface_mismatch
    if surface_mismatch:
        reasons.append("공개 표면 기준일 불일치: "
                       + ", ".join(f"{m['surface']}={m['date']}" for m in surface_mismatch))
        bump("WARNING")

    # benchmark base date 는 펀드 시작일이라 과거인 게 정상 — stale 로 세지 않는다.
    checks["benchmarkBaseDateIgnored"] = True

    # canonical 은 최신인데 공개만 뒤처진 경우를 명시 표기(원인 분리용)
    publish_only = bool(canon_latest and not canon_gap and pub_gap)

    return {
        **exp,
        "verdict": verdict,
        "reasons": reasons,
        "checks": checks,
        "canonicalLatestTradeDate": canon_latest,
        "publicLatestTradeDate": pub_latest,
        "canonicalStaleTradingDays": len(canon_gap),
        "publicStaleTradingDays": len(pub_gap),
        "canonicalStaleDates": canon_gap,
        "publicStaleDates": pub_gap,
        "canonicalPublicGapTradingDays": len(
            gap_trading_days(pub_latest, canon_latest, missed)) if (pub_latest and canon_latest) else None,
        "missedRunDatesExcluded": missed,
        "publishOnlyStale": publish_only,
        "publicSurfaceMismatch": surface_mismatch,
        "blockedStaleThreshold": BLOCKED_STALE_TRADING_DAYS,
    }


def build(now=None, url=PUBLIC_URL, skip_http=False) -> dict:
    now = now or C.now_kst()
    canonical = read_canonical()
    public = ({"source": url, "ok": False, "error": "SKIPPED_BY_FLAG"}
              if skip_http else read_public_http(url))
    repo_copy = read_public_repo_file()
    d = decide(now, canonical, public, repo_copy)
    return {
        "task": "R2",
        "taskId": "WABABA-HOTG-PUBLIC-FRESHNESS-OBSERVER-HARDENING-R2",
        "generatedAt": now.isoformat(timespec="seconds"),
        "fund": "LEGACY_50D",
        **d,
        "canonical": canonical,
        "public": public,
        "publicRepoCopy": repo_copy,
        "strategyUnchanged": {"initialCapital": 50000000, "topN": 10,
                              "holdTradingDays": 50, "paperOnly": True,
                              "realOrders": 0, "broker": 0},
        "readOnly": True,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="와바바 공개 펀드 freshness 판정(HOTG observer evidence)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-http", action="store_true",
                    help="공개 URL 조회 생략(오프라인 점검용 — fail-closed 로 WARNING)")
    ap.add_argument("--out", default=str(OUT_PATH))
    args = ap.parse_args()

    res = build(skip_http=args.no_http)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str),
                   encoding="utf-8")

    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"[WABABA_FRESHNESS] verdict={res['verdict']} "
              f"expected={res['expectedPublicTradeDate']} "
              f"canonical={res['canonicalLatestTradeDate']} "
              f"public={res['publicLatestTradeDate']} "
              f"publicStale={res['publicStaleTradingDays']}거래일 "
              f"deadlinePassed={res['publishDeadlinePassed']}")
        for r in res["reasons"]:
            print(f"  - {r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
