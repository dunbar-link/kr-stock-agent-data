#!/usr/bin/env python3
"""magic_live_verify 테스트 (Phase 45-AUTO3). fetch 주입으로 네트워크 0. Vercel deploy 호출 0 확인."""
from __future__ import annotations

import json

import magic_live_verify as LV

NOW = "2026-06-25T16:40:00+09:00"
SEQ, TD, ASSET = 3, 3, 49_831_105

DEPLOYED = {
    "magicOfficialSummary": {"officialSequence": SEQ, "totalAsset": ASSET, "dataDate": "2026-06-23"},
    "magicOfficialTradeDays": [{"date": "2026-06-23"}, {"date": "2026-06-19"}, {"date": "2026-06-17"}],
    "magicOfficialPortfolio": {"holdings": [{"code": "046940"}]},
}
PERF_HTML = "<html><body><h1>마법공식 OFFICIAL performance</h1>...</body></html>"


def _fetch_factory(urls, *, json_obj=DEPLOYED, json_status=200, perf_status=200, perf_text=PERF_HTML):
    def _fetch(url, *, timeout=30):
        urls.append(url)
        if "performance" in url:
            return perf_status, perf_text
        return json_status, json.dumps(json_obj)
    return _fetch


# ===== 테스트 =====

def t23_live_json_read_only_pass():
    urls = []
    r = LV.run("https://kr-stock-agent.vercel.app", expect_sequence=SEQ, expect_trade_days=TD,
               expect_total_asset=ASSET, fetch_fn=_fetch_factory(urls), now=NOW)
    assert r["status"] == "PASS", r
    assert r["deployedSequence"] == SEQ and r["tradeDayCount"] == TD and r["deployedTotalAsset"] == ASSET
    assert r["performanceRendered"] is True and r["performanceHttp"] == 200
    assert r["productionWriteCount"] == 0 and r["canonicalChanged"] is False


def t24_mismatch_blocked():
    urls = []
    r = LV.run("https://kr-stock-agent.vercel.app", expect_sequence=99, expect_trade_days=TD,
               expect_total_asset=ASSET, fetch_fn=_fetch_factory(urls), now=NOW)
    assert r["status"] == "BLOCKED_LIVE_MISMATCH", r
    assert any("officialSequence" in m for m in r["mismatches"])


def t25_no_vercel_deploy_call():
    urls = []
    r = LV.run("https://kr-stock-agent.vercel.app", expect_sequence=SEQ, expect_trade_days=TD,
               expect_total_asset=ASSET, fetch_fn=_fetch_factory(urls), now=NOW)
    assert r["vercelDeployCalled"] is False
    # 호출된 URL은 read-only GET(데이터 JSON + /performance)뿐, deploy/promote 0
    assert all(("recommendation-history.json" in u) or ("/performance" in u) for u in urls), urls
    assert not any(("deploy" in u) or ("promote" in u) or ("/api/" in u) for u in urls), urls
    # cache-bust 쿼리 포함
    assert any("?_=" in u for u in urls), urls


def t26_json_http_error_blocked():
    urls = []
    r = LV.run("https://kr-stock-agent.vercel.app", expect_sequence=SEQ, expect_trade_days=TD,
               expect_total_asset=ASSET,
               fetch_fn=_fetch_factory(urls, json_status=503, json_obj={}), now=NOW)
    assert r["status"] == "BLOCKED_LIVE_MISMATCH", r
    assert any("503" in m or "HTTP" in m for m in r["mismatches"])


def t27_performance_error_page_blocked():
    urls = []
    r = LV.run("https://kr-stock-agent.vercel.app", expect_sequence=SEQ, expect_trade_days=TD,
               expect_total_asset=ASSET,
               fetch_fn=_fetch_factory(urls, perf_status=200, perf_text="<html>500 error</html>"), now=NOW)
    assert r["status"] == "BLOCKED_LIVE_MISMATCH", r
    assert any("markers" in m for m in r["mismatches"])


TESTS = [
    ("23 live JSON read-only 검증 PASS", t23_live_json_read_only_pass),
    ("24 기대값 불일치 → BLOCKED_LIVE_MISMATCH", t24_mismatch_blocked),
    ("25 Vercel deploy 호출 0(read-only GET만)", t25_no_vercel_deploy_call),
    ("26 JSON HTTP 오류 → BLOCKED", t26_json_http_error_blocked),
    ("27 /performance 렌더 마커 없음 → BLOCKED", t27_performance_error_page_blocked),
]


def main():
    p = f = 0
    for name, fn in TESTS:
        try:
            fn(); print(f"[PASS] {name}"); p += 1
        except AssertionError as e:
            print(f"[FAIL] {name} -> {e}"); f += 1
        except Exception as e:  # noqa: BLE001
            import traceback; print(f"[ERROR] {name} -> {type(e).__name__}: {e}"); traceback.print_exc(); f += 1
    print(f"\n결과: {p} passed, {f} failed (총 {len(TESTS)})")
    return 0 if f == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
