#!/usr/bin/env python3
"""마법공식 A티어 — 운영 배포 JSON/performance read-only 검증 (Phase 45-AUTO3).

운영 URL의 recommendation-history.json(cache-busted) 과 /performance(HTTP 200·렌더 마커)를 *읽기만* 한다.
- Vercel deploy/promote 호출 0. 파일/장부/public write 0. 실제 주문 0.
- magicOfficialSummary.officialSequence·totalAsset, magicOfficialTradeDays 개수를 기대값과 대조.
- 불일치 시 BLOCKED_LIVE_MISMATCH(자동 중단). 네트워크는 fetch 주입으로 테스트에서 0.

예) python scripts/magic_live_verify.py --expect-sequence 3 --expect-trade-days 3 --expect-total-asset 49831105
"""
from __future__ import annotations

import argparse
import json
import sys

DEFAULT_BASE_URL = "https://kr-stock-agent.vercel.app"
JSON_PATH = "/data/recommendation-history.json"
PERFORMANCE_PATH = "/performance"
# /performance 가 실제로 렌더됐는지 확인할 마커(에러 페이지 구분용)
PERFORMANCE_MARKERS = ("마법공식", "magicOfficial", "performance", "공식")


def _now_iso():
    import magic_daily_common as C
    return C.now_kst().isoformat()


def _fetch(url: str, *, timeout=30):
    """기본 fetch(읽기 전용 GET). (status, text). 네트워크 실패 시 (None, '')."""
    import urllib.request
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache",
                                               "User-Agent": "wababa-magic-live-verify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (read-only GET)
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return None, ""


def verify(deployed_json: dict, performance_status, performance_text: str, *,
           expect_sequence=None, expect_trade_days=None, expect_total_asset=None,
           json_status=200, now=None) -> dict:
    """순수 검증(네트워크 0). 기대값 대조 후 PASS / BLOCKED_LIVE_MISMATCH."""
    now = now or _now_iso()
    mism = []
    summary = (deployed_json or {}).get("magicOfficialSummary") or {}
    seq = summary.get("officialSequence")
    total_asset = summary.get("totalAsset")
    trade_days = (deployed_json or {}).get("magicOfficialTradeDays") or []
    td_count = len(trade_days)

    if json_status != 200:
        mism.append(f"recommendation-history.json HTTP {json_status} != 200")
    if not summary:
        mism.append("magicOfficialSummary missing in deployed JSON")
    if expect_sequence is not None and seq != expect_sequence:
        mism.append(f"officialSequence {seq} != expect {expect_sequence}")
    if expect_trade_days is not None and td_count != expect_trade_days:
        mism.append(f"magicOfficialTradeDays count {td_count} != expect {expect_trade_days}")
    if expect_total_asset is not None and total_asset != expect_total_asset:
        mism.append(f"totalAsset {total_asset} != expect {expect_total_asset}")

    perf_ok = performance_status == 200 and any(m in (performance_text or "") for m in PERFORMANCE_MARKERS)
    if performance_status != 200:
        mism.append(f"/performance HTTP {performance_status} != 200")
    elif not perf_ok:
        mism.append("/performance rendered markers missing (error page?)")

    return {
        "status": "PASS" if not mism else "BLOCKED_LIVE_MISMATCH",
        "stage": "LIVE_VERIFY",
        "deployedSequence": seq, "deployedTotalAsset": total_asset, "tradeDayCount": td_count,
        "jsonHttp": json_status, "performanceHttp": performance_status, "performanceRendered": perf_ok,
        "expected": {"sequence": expect_sequence, "tradeDays": expect_trade_days,
                     "totalAsset": expect_total_asset},
        "mismatches": mism, "vercelDeployCalled": False,
        "productionWriteCount": 0, "publicCopyCount": 0, "canonicalChanged": False,
        "noFakeTrade": True, "createdAt": now,
    }


def run(base_url=DEFAULT_BASE_URL, *, expect_sequence=None, expect_trade_days=None,
        expect_total_asset=None, fetch_fn=None, now=None) -> dict:
    """배포 JSON/performance 를 읽어 verify(). fetch_fn 주입 시 네트워크 0(테스트)."""
    now = now or _now_iso()
    fetch_fn = fetch_fn or _fetch
    # cache-bust: now 문자열로 충분(시계 호출 회피; 결정성)
    bust = (now or "").replace(":", "").replace("-", "").replace(".", "")[:18]
    js_status, js_text = fetch_fn(f"{base_url}{JSON_PATH}?_={bust}")
    perf_status, perf_text = fetch_fn(f"{base_url}{PERFORMANCE_PATH}")
    try:
        deployed = json.loads(js_text) if js_text else {}
    except ValueError:
        deployed = {}
        js_status = js_status if js_status not in (None, 200) else 599
    return verify(deployed, perf_status, perf_text, expect_sequence=expect_sequence,
                  expect_trade_days=expect_trade_days, expect_total_asset=expect_total_asset,
                  json_status=js_status if js_status is not None else 0, now=now)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="마법공식 운영 배포 JSON/performance read-only 검증(deploy 호출 0)")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--expect-sequence", type=int, default=None)
    ap.add_argument("--expect-trade-days", type=int, default=None)
    ap.add_argument("--expect-total-asset", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    r = run(args.base_url, expect_sequence=args.expect_sequence, expect_trade_days=args.expect_trade_days,
            expect_total_asset=args.expect_total_asset)
    if args.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        print(f"[LIVE] {r['status']} seq={r['deployedSequence']} asset={r['deployedTotalAsset']} "
              f"tradeDays={r['tradeDayCount']} json={r['jsonHttp']} perf={r['performanceHttp']}")
        if r["mismatches"]:
            print(f"  mismatches: {r['mismatches']}")
    return 0 if r["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
