#!/usr/bin/env python3
"""KRX 수집 방어 + 데이터 품질 게이트 테스트 (WABABA-KRX-FUNDAMENTAL-RECOVERY-R1).

전부 fixture/mock 기반 — 실제 KRX 네트워크 호출 0, 운영 데이터 write 0.
비거래일(주말)에도 전체 로직을 검증할 수 있게 설계했다.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd

import krx_fetch_guard as G
import krx_data_quality as Q

ROOT = Path(__file__).resolve().parents[1]

FUND_COLS = ["티커", "BPS", "PER", "PBR", "EPS", "DIV", "DPS"]


def fund_frame(rows=100, per_all_nan=False, pbr_all_nan=False, drop=()):
    data = {
        "티커": [f"{i:06d}" for i in range(rows)],
        "BPS": [10000] * rows, "PER": [None] * rows if per_all_nan else [12.3] * rows,
        "PBR": [None] * rows if pbr_all_nan else [1.1] * rows,
        "EPS": [800] * rows, "DIV": [2.0] * rows, "DPS": [500] * rows,
    }
    for c in drop:
        data.pop(c, None)
    return pd.DataFrame(data)


FUND_SPEC = G.FetchSpec(kind="fundamental", market="KOSPI",
                        required_columns=("티커", "PER", "PBR", "DIV"),
                        min_rows=50, numeric_columns=("PER", "PBR", "DIV"),
                        collapse_guard_columns=("PBR",), required=True)

NOSLEEP = lambda _s: None  # noqa: E731


def _expect_invalid(fn, code=None):
    try:
        fn()
    except G.KrxDataInvalid as e:
        if code and e.code != code:
            raise AssertionError(f"code={e.code} expected {code}") from None
        return e
    raise AssertionError("KrxDataInvalid 가 발생해야 함")


# ── 1~7 응답 형태별 검증 ──────────────────────────────────────────────────────
def t01_normal_dataframe():
    r = G.fetch_validated(lambda: fund_frame(), FUND_SPEC, sleep=NOSLEEP)
    assert r.verdict == "PASS" and r.attempts == 1, r
    assert r.frame.shape[0] == 100


def t02_empty_response_none():
    e = _expect_invalid(lambda: G.fetch_validated(lambda: None, FUND_SPEC, sleep=NOSLEEP),
                        "EMPTY_RESPONSE")
    assert e.evidence["attempts"] == G.MAX_ATTEMPTS, e.evidence   # 일시적 → 재시도 소진


def t03_empty_dataframe():
    _expect_invalid(lambda: G.fetch_validated(lambda: pd.DataFrame(), FUND_SPEC, sleep=NOSLEEP),
                    "EMPTY_DATAFRAME")


def t04_html_login_page_returned():
    """비-JSON(HTML 로그인/오류 페이지) → pykrx 가 JSONDecodeError 를 올리는 경로."""
    def boom():
        raise ValueError("Expecting value: line 1 column 1 (char 0)")
    e = _expect_invalid(lambda: G.fetch_validated(boom, FUND_SPEC, sleep=NOSLEEP))
    assert e.evidence["attempts"] == G.MAX_ATTEMPTS, "일시적 비-JSON 은 재시도 대상"
    assert G.classify_error(ValueError("Expecting value: line 1 column 1 (char 0)")) == "RETRYABLE_TRANSIENT"


def t05_malformed_json():
    assert G.classify_error(ValueError("Extra data: line 2 column 1")) == "RETRYABLE_TRANSIENT"


def t06_required_columns_missing():
    """실제 관측된 오류 형태 — 필수 columns 부재는 재시도로 숨기지 않는다."""
    e = _expect_invalid(lambda: G.fetch_validated(lambda: fund_frame(drop=("PER", "PBR")),
                                                  FUND_SPEC, sleep=NOSLEEP),
                        "REQUIRED_COLUMNS_MISSING")
    assert e.evidence["attempts"] == 1, "구조 결함은 즉시 중단(재시도 금지)"


def t07_optional_columns_missing_ok():
    """계산에 안 쓰는 선택 열(BPS/EPS/DPS) 결측으로 과도하게 막지 않는다."""
    r = G.fetch_validated(lambda: fund_frame(drop=("BPS", "EPS", "DPS")), FUND_SPEC, sleep=NOSLEEP)
    assert r.verdict == "PASS"


# ── 8~9 시장 독립 판정 ────────────────────────────────────────────────────────
def t08_kospi_only_fails():
    ok, code, _ = Q.evaluate(Q.build_status(date_iso="2026-07-29", verdict="INVALID",
                                            markets={"KOSPI": "INVALID", "KOSDAQ": "PASS"}),
                             "2026-07-29")
    assert not ok and code == "KRX_DATA_QUALITY_INVALID", code


def t09_kosdaq_only_fails():
    st = Q.build_status(date_iso="2026-07-29", verdict="PASS",
                        markets={"KOSPI": "PASS", "KOSDAQ": "INVALID"})
    ok, code, _ = Q.evaluate(st, "2026-07-29")
    assert not ok and code == "KRX_MARKET_NOT_PASS", code   # 두 시장 모두 PASS 필요


# ── 10~11 재시도 계약 ─────────────────────────────────────────────────────────
def t10_retry_then_success():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return fund_frame()
    r = G.fetch_validated(flaky, FUND_SPEC, sleep=NOSLEEP)
    assert r.verdict == "PASS" and r.attempts == 2, r.attempts


def t11_all_retries_fail():
    calls = {"n": 0}

    def always():
        calls["n"] += 1
        raise ValueError("Expecting value: line 1 column 1 (char 0)")
    _expect_invalid(lambda: G.fetch_validated(always, FUND_SPEC, sleep=NOSLEEP))
    assert calls["n"] == G.MAX_ATTEMPTS == 3, f"총 시도 {calls['n']} (최초1+재시도2=3 이어야 함)"


def t11b_no_infinite_retry():
    calls = {"n": 0}

    def always():
        calls["n"] += 1
        return None
    _expect_invalid(lambda: G.fetch_validated(always, FUND_SPEC, sleep=NOSLEEP))
    assert calls["n"] <= 3, "무한 재시도 금지"


def t11c_http_429_and_5xx_retryable():
    for msg in ("HTTP 429 Too Many Requests", "HTTP 503 Service Unavailable",
                "Connection reset by peer", "Read timed out"):
        assert G.classify_error(RuntimeError(msg)) == "RETRYABLE_TRANSIENT", msg


# ── 12 세션 만료 판정 ─────────────────────────────────────────────────────────
def t12_session_expiry_is_transient_not_confirmed_cause():
    """세션 만료 계열 메시지는 일시적으로 분류하되, 증거 없이 원인으로 확정하지 않는다."""
    # 비-JSON 응답은 세션 만료로도, 단순 upstream 오류로도 나타난다 → 분류만 하고 단정 금지
    assert G.classify_error(ValueError("Expecting value: line 1 column 1 (char 0)")) == "RETRYABLE_TRANSIENT"
    # 구조 변경은 재시도/재로그인으로 숨기지 않는다
    assert G.classify_error(KeyError("None of [Index(['PER'])] are in the columns")) == "FATAL_SCHEMA"


# ── 13 거래일 불일치 ──────────────────────────────────────────────────────────
def t13_trading_date_mismatch():
    st = Q.build_status(date_iso="2026-07-28", verdict="PASS",
                        markets={"KOSPI": "PASS", "KOSDAQ": "PASS"})
    ok, code, _ = Q.evaluate(st, "2026-07-29")
    assert not ok and code == "KRX_QUALITY_DATE_MISMATCH", code


# ── 14 종목 수 급감 ───────────────────────────────────────────────────────────
def t14_row_count_too_low():
    _expect_invalid(lambda: G.fetch_validated(lambda: fund_frame(rows=10), FUND_SPEC, sleep=NOSLEEP),
                    "ROW_COUNT_TOO_LOW")


def t14b_universe_min_count_constant_exists():
    # build_market_snapshot 를 import 하면 pykrx 가 즉시 KRX 로그인을 시도한다(네트워크·자격증명 사용,
    # 로그인 ID 가 stdout 에 출력됨). 테스트는 네트워크 0·비밀값 0 이어야 하므로 소스만 읽는다.
    src = (ROOT / "scripts" / "build_market_snapshot.py").read_text(encoding="utf-8")
    import re as _re
    m = _re.search(r'MIN_UNIVERSE_COUNT\s*=\s*int\(os\.environ\.get\(\s*"[^"]+"\s*,\s*"(\d+)"', src)
    assert m and int(m.group(1)) >= 1000, "universe 급감 하한이 설정돼 있어야 함"


# ── 15 전부 NaN ───────────────────────────────────────────────────────────────
def t15_all_nan_collapse():
    _expect_invalid(lambda: G.fetch_validated(lambda: fund_frame(pbr_all_nan=True),
                                              FUND_SPEC, sleep=NOSLEEP), "ALL_VALUES_NAN")


def t15b_per_all_nan_is_tolerated():
    """PER 은 적자기업이 많아 전량 결측이 정상일 수 있다 → 붕괴 검사 대상 아님(과도 차단 금지)."""
    r = G.fetch_validated(lambda: fund_frame(per_all_nan=True), FUND_SPEC, sleep=NOSLEEP)
    assert r.verdict == "PASS"


# ── 16~17 fail-closed 운영 보존 ───────────────────────────────────────────────
def t16_existing_public_file_preserved():
    """품질 INVALID 판정은 REPO1 public 산출물을 건드리지 않는다(SHA 불변)."""
    import hashlib
    pub = Path("C:/work/kr-stock-agent/public/data/recommendation-history.json")
    if not pub.exists():
        return
    before = hashlib.sha256(pub.read_bytes()).hexdigest()
    st = Q.build_status(date_iso="2026-07-29", verdict="INVALID",
                        markets={"KOSPI": "INVALID", "KOSDAQ": "PASS"})
    ok, _, _ = Q.evaluate(st, "2026-07-29")
    assert not ok
    after = hashlib.sha256(pub.read_bytes()).hexdigest()
    assert before == after, "품질 판정이 운영 public 파일을 변경하면 안 됨"


def t17_publish_gate_blocks_on_invalid_quality():
    """publish gate 가 품질 INVALID 를 BLOCKED 로 막는지(실제 함수 경로)."""
    import magic_publish_gate as P
    src = Path(P.__file__).read_text(encoding="utf-8")
    assert "krx_data_quality" in src, "publish gate 가 품질 모듈을 참조해야 함"
    assert "krxDataQualityPass" in src


def t17b_auto_apply_gate_requires_quality():
    import magic_daily_auto_apply as A
    src = Path(A.__file__).read_text(encoding="utf-8")
    assert "krx_data_quality" in src, "auto-apply gate 가 품질 증거를 요구해야 함"
    assert A.B_KRX_DATA_QUALITY == "KRX_DATA_QUALITY_NOT_PASS"


def t17c_missing_evidence_is_not_pass():
    """증거 파일이 아예 없으면 PASS 로 간주하지 않는다(fail-closed)."""
    ok, code, _ = Q.evaluate(None, "2026-07-29")
    assert not ok and code == "KRX_QUALITY_EVIDENCE_MISSING", code


# ── 18 정상 흐름 무회귀 ───────────────────────────────────────────────────────
def t18_normal_quality_passes_gate():
    st = Q.build_status(date_iso="2026-07-31", verdict="PASS",
                        markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}, universe_count=2763)
    ok, code, detail = Q.evaluate(st, "2026-07-31")
    assert ok, (code, detail)


def t18b_snapshot_fetchers_are_fail_closed():
    """safe_get_* 가 더 이상 빈 DataFrame 으로 실패를 숨기지 않는지(소스 계약).

    import 하면 pykrx 가 KRX 로그인을 시도하므로 소스만 읽는다(네트워크 0·자격증명 0).
    """
    src = (ROOT / "scripts" / "build_market_snapshot.py").read_text(encoding="utf-8")
    assert "_run_guarded" in src
    for fn in ("safe_get_ohlcv", "safe_get_fundamental", "safe_get_market_cap"):
        assert f"def {fn}" in src
    # 실패 시 빈 프레임을 돌려주던 옛 계약이 남아있지 않아야 한다
    assert 'return pd.DataFrame(columns=["symbol", "PER", "PBR", "DIV"])' not in src
    assert 'return pd.DataFrame(columns=["symbol", "price"])' not in src
    assert 'return pd.DataFrame(columns=["symbol", "marketCap"])' not in src


# ── 19 비밀값 노출 0 ──────────────────────────────────────────────────────────
def t18c_both_entrypoints_record_quality():
    """★ 2026-08-03~08-07 운영 중단 재발 방지.

    일일 파이프라인(Signal)은 build_market_snapshot_fast.build_payload 를 탄다.
    품질 증거 기록을 build_market_snapshot 쪽에만 넣으면 자연 실행에서 증거가 생기지 않아
    fail-closed gate 가 매 거래일 Auto Apply 를 막는다(실사고).
    두 entrypoint 가 모두 공용 수집·기록 함수를 호출하는지 소스로 고정한다.
    """
    slow = (ROOT / "scripts" / "build_market_snapshot.py").read_text(encoding="utf-8")
    fast = (ROOT / "scripts" / "build_market_snapshot_fast.py").read_text(encoding="utf-8")
    for name, src in (("build_market_snapshot", slow), ("build_market_snapshot_fast", fast)):
        assert "collect_markets_with_quality(" in src, f"{name} 이 공용 수집 함수를 호출하지 않음"
        assert "record_universe_quality(" in src, f"{name} 이 품질 기록을 하지 않음"
    # fast 가 옛 경로(get_market_frame 직접 호출)로 되돌아가지 않았는지
    assert "get_market_frame(base_date," not in fast, \
        "fast 가 get_market_frame 을 직접 호출하면 품질 기록을 건너뛴다"
    assert "collect_markets_with_quality" in fast.split("from build_market_snapshot import")[1][:400], \
        "fast 가 공용 함수를 import 하지 않음"


def t18d_quality_written_where_gate_reads():
    """품질 증거 생성 경로와 Auto Apply·Publish 소비 경로가 같은 파일을 가리키는지."""
    import krx_data_quality as Q
    assert Q.LATEST_JSON.parent.name == "wababa"
    assert Q.LATEST_JSON.name == "krx-data-quality-latest.json"
    # 날짜본도 같은 디렉터리
    assert Q.path_for("2026-08-03").parent == Q.LATEST_JSON.parent
    # gate 가 읽는 함수가 그 경로를 쓰는지
    src = Path(Q.__file__).read_text(encoding="utf-8")
    assert "def read_status" in src and "def gate" in src


def t19_no_secret_in_diagnostics():
    # 아래 값은 전부 합성 더미다(실제 자격증명 아님). redaction 이 실제로 지우는지 확인용.
    leaky = ("login failed KRX_ID=DUMMYID-NOT-REAL KRX_PW=DUMMYPW-NOT-REAL "
             "Cookie: JSESSIONID=DUMMYSESSION-NOT-REAL "
             "Authorization: Bearer DUMMYTOKEN-NOT-REAL")
    red = G.redact(leaky)
    for bad in ("DUMMYID-NOT-REAL", "DUMMYPW-NOT-REAL",
                "DUMMYSESSION-NOT-REAL", "DUMMYTOKEN-NOT-REAL"):
        assert bad not in red, f"redact 실패: {bad} 노출"
    assert "<REDACTED>" in red


def t19b_evidence_carries_no_raw_body():
    def boom():
        raise ValueError("Expecting value: line 1 column 1 (char 0) KRX_PW=DUMMYPW-NOT-REAL")
    e = _expect_invalid(lambda: G.fetch_validated(boom, FUND_SPEC, sleep=NOSLEEP))
    blob = json.dumps(e.evidence, ensure_ascii=False)
    assert "DUMMYPW-NOT-REAL" not in blob, "증거에 비밀값이 들어가면 안 됨"


def t19c_quality_status_has_no_secrets():
    st = Q.build_status(date_iso="2026-07-29", verdict="INVALID",
                        markets={"KOSPI": "INVALID"},
                        evidence=[{"errorFingerprint": G.redact(
                            "KRX_PW=DUMMYPW-NOT-REAL Cookie: JSESSIONID=DUMMYSESSION-NOT-REAL")}])
    blob = json.dumps(st, ensure_ascii=False)
    for bad in ("DUMMYPW-NOT-REAL", "DUMMYSESSION-NOT-REAL"):
        assert bad not in blob, blob


# ── 20 실주문·브로커 호출 0 ───────────────────────────────────────────────────
def t20_no_order_or_broker_path():
    for mod in (G, Q):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        for bad in ("주문", "order_submit", "place_order", "브로커", "broker_api"):
            assert bad not in src, f"{mod.__name__} 에 주문/브로커 경로 흔적: {bad}"
    st = Q.build_status(date_iso="2026-07-29", verdict="INVALID", markets={})
    assert st["realOrderCount"] == 0 and st["brokerApiCallCount"] == 0


def t20b_guard_writes_nothing():
    """guard 는 파일을 쓰지 않는다(순수 검증)."""
    src = Path(G.__file__).read_text(encoding="utf-8")
    for bad in ("open(", "write_text", "to_csv", "to_json"):
        assert bad not in src, f"krx_fetch_guard 가 파일 쓰기 흔적 보유: {bad}"


# ── 품질 기록 read/write 격리 검증 ────────────────────────────────────────────
def t21_status_roundtrip_isolated():
    with tempfile.TemporaryDirectory() as td:
        orig_dir, orig_latest = Q.QUALITY_DIR, Q.LATEST_JSON
        try:
            Q.QUALITY_DIR = Path(td) / "wababa"
            Q.LATEST_JSON = Q.QUALITY_DIR / "krx-data-quality-latest.json"
            st = Q.build_status(date_iso="2026-07-29", verdict="PASS",
                                markets={"KOSPI": "PASS", "KOSDAQ": "PASS"}, universe_count=2700)
            Q.write_status(st)
            got = Q.read_status("2026-07-29")
            assert got and got["verdict"] == "PASS"
            ok, _, _ = Q.evaluate(got, "2026-07-29")
            assert ok
            assert Q.read_status("2026-07-30") is None, "다른 날짜를 PASS 로 인정하면 안 됨"
        finally:
            Q.QUALITY_DIR, Q.LATEST_JSON = orig_dir, orig_latest


TESTS = [
    ("01 정상 DataFrame", t01_normal_dataframe),
    ("02 빈 응답(None) → 재시도 소진 후 INVALID", t02_empty_response_none),
    ("03 빈 DataFrame", t03_empty_dataframe),
    ("04 HTML 로그인·오류 페이지(비-JSON)", t04_html_login_page_returned),
    ("05 잘못된 JSON", t05_malformed_json),
    ("06 필수 columns 누락 → 즉시 BLOCKED", t06_required_columns_missing),
    ("07 선택 columns 누락 → 통과(과도 차단 금지)", t07_optional_columns_missing_ok),
    ("08 KOSPI만 실패", t08_kospi_only_fails),
    ("09 KOSDAQ만 실패", t09_kosdaq_only_fails),
    ("10 첫 호출 실패 후 재시도 성공", t10_retry_then_success),
    ("11 모든 재시도 실패(총 3회)", t11_all_retries_fail),
    ("11b 무한 재시도 금지", t11b_no_infinite_retry),
    ("11c 429·5xx·네트워크는 재시도 대상", t11c_http_429_and_5xx_retryable),
    ("12 세션 만료 계열 분류(원인 단정 금지)", t12_session_expiry_is_transient_not_confirmed_cause),
    ("13 거래일 불일치", t13_trading_date_mismatch),
    ("14 종목 수 급감(행 수 미달)", t14_row_count_too_low),
    ("14b universe 급감 하한 존재", t14b_universe_min_count_constant_exists),
    ("15 전부 NaN(값 붕괴)", t15_all_nan_collapse),
    ("15b PER 전량 결측은 허용", t15b_per_all_nan_is_tolerated),
    ("16 기존 정상 public 파일 보존", t16_existing_public_file_preserved),
    ("17 publish gate 가 품질 요구", t17_publish_gate_blocks_on_invalid_quality),
    ("17b auto-apply gate 가 품질 요구", t17b_auto_apply_gate_requires_quality),
    ("17c 증거 없음은 PASS 아님", t17c_missing_evidence_is_not_pass),
    ("18 정상 시 gate 통과(무회귀)", t18_normal_quality_passes_gate),
    ("18b safe_get_* fail-closed 계약", t18b_snapshot_fetchers_are_fail_closed),
    ("18c 두 entrypoint 모두 품질 기록(운영중단 재발방지)", t18c_both_entrypoints_record_quality),
    ("18d 품질 생성 경로 == gate 소비 경로", t18d_quality_written_where_gate_reads),
    ("19 비밀값 redaction", t19_no_secret_in_diagnostics),
    ("19b 증거에 원문·비밀값 없음", t19b_evidence_carries_no_raw_body),
    ("19c 품질 상태에 비밀값 없음", t19c_quality_status_has_no_secrets),
    ("20 실주문·브로커 경로 0", t20_no_order_or_broker_path),
    ("20b guard 는 파일 쓰기 0", t20b_guard_writes_nothing),
    ("21 품질 기록 격리 roundtrip", t21_status_roundtrip_isolated),
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
