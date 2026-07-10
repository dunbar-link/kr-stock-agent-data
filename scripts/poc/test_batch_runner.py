"""test_batch_runner — 배치 실행기 오프라인/모의 검증 (Phase MF-TTM-ELIGIBLE-BATCH-RUNNER-PREP).

실제 DART 호출 0. manifest·캐시·monkeypatch 로 dry-run/offline/resume/020/토큰/민감정보를 검증한다.
실행:  python scripts/poc/test_batch_runner.py   (exit 0 = 전부 PASS)
전제:  먼저 `node scripts/poc/build_batch_manifest.mjs` 로 manifest 생성.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import batch_runner as B
import quarterly_ttm_poc as Q

_PASS = 0
_FAIL = 0


def check(name, cond, detail=""):
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL  {name}  {detail}")


TMP = Path(B.OUT_DIR) / "test-batch-state"


def test_manifest_integrity():
    m = B.load_manifest()
    codes = [s["stockCode"] for s in m["stocks"]]
    check("manifest 누락0: eligibleCount==stocks", m["eligibleCount"] == len(m["stocks"]))
    check("manifest 중복0", len(codes) == len(set(codes)), f"dups={len(codes)-len(set(codes))}")
    check("manifest 배치≤200", all(v <= m["batchSize"] for v in m["integrity"]["byBatch"].values()))
    check("manifest 배치 합=총", sum(m["integrity"]["byBatch"].values()) == len(m["stocks"]))
    # 각 종목 정확히 한 배치
    per = {}
    for s in m["stocks"]:
        per.setdefault(s["stockCode"], set()).add(s["batchId"])
    check("종목당 정확히 1배치", all(len(v) == 1 for v in per.values()))


def test_confirm_token():
    bd = "2026-07-09"
    check("토큰 정확일치만 통과", B.valid_confirm_token(f"RUN_TTM_BACKFILL_{bd}_batch-01", bd, "batch-01"))
    check("토큰 배치 불일치 → 거부", not B.valid_confirm_token(f"RUN_TTM_BACKFILL_{bd}_batch-02", bd, "batch-01"))
    check("토큰 빈값 → 거부", not B.valid_confirm_token("", bd, "batch-01"))


def test_real_fetch_blocked_without_token():
    try:
        B.main(["--batch-id", "batch-01", "--real-fetch", "--output-dir", str(TMP)])
        check("토큰 없는 real-fetch 차단", False, "SystemExit 기대")
    except SystemExit as e:
        check("토큰 없는 real-fetch 차단(SystemExit)", "confirm" in str(e).lower() or "차단" in str(e))


def test_dryrun_zero_calls(net):
    net["n"] = 0
    rc = B.main(["--batch-id", "batch-07", "--dry-run", "--output-dir", str(TMP)])
    check("dry-run 정상종료", rc == 0)
    check("dry-run 실제 네트워크 호출 0", net["n"] == 0, str(net))


def test_offline_zero_calls(net):
    net["n"] = 0
    rc = B.main(["--batch-id", "batch-01", "--offline", "--output-dir", str(TMP)])
    check("offline 정상종료", rc == 0)
    check("offline 실제 네트워크 호출 0 (캐시만)", net["n"] == 0, str(net))


def test_020_stop_and_state():
    # process_stock 을 RateLimited 던지게 교체 → 즉시 중단·RATE_LIMITED 저장
    orig = Q.process_stock
    Q.process_stock = lambda *a, **k: (_ for _ in ()).throw(Q.RateLimited("mock 020"))
    try:
        bd = B.load_manifest()["universeBaseDate"]
        B.main(["--batch-id", "batch-01", "--real-fetch",
                "--confirm", f"RUN_TTM_BACKFILL_{bd}_batch-01", "--output-dir", str(TMP)])
        st = json.loads((TMP / "batch-state-batch-01.json").read_text(encoding="utf-8"))
        check("020 → rateLimited=True", st.get("rateLimited") is True)
        check("020 → RATE_LIMITED 상태 저장", any(v.get("status") == "RATE_LIMITED" for v in st["stocks"].values()))
        check("020 → resume 인덱스 기록", st.get("resumeAfterSelectionIndex") is not None)
    finally:
        Q.process_stock = orig


def test_fatal_dart_stop():
    # status 012(접근불가 IP)/901 = FatalDartError → 첫 종목에서 즉시 중단(전 종목 시도 안 함)
    orig = Q.process_stock
    calls = {"n": 0}

    def fatal(*a, **k):
        calls["n"] += 1
        raise Q.FatalDartError("mock 012 접근불가 IP")
    Q.process_stock = fatal
    try:
        bd = B.load_manifest()["universeBaseDate"]
        # 이전 state 제거해 깨끗이
        sp = TMP / "batch-state-batch-02.json"
        if sp.exists():
            sp.unlink()
        B.main(["--batch-id", "batch-02", "--real-fetch",
                "--confirm", f"RUN_TTM_BACKFILL_{bd}_batch-02", "--output-dir", str(TMP)])
        st = json.loads(sp.read_text(encoding="utf-8"))
        check("fatal(012) → 첫 종목에서 즉시 중단(1회만 시도)", calls["n"] == 1, f"attempts={calls['n']}")
        check("fatal(012) → fatalDartError 기록", "fatalDartError" in st and st["fatalDartError"])
        check("fatal(012) → resume 인덱스 기록", st.get("resumeAfterSelectionIndex") is not None)
    finally:
        Q.process_stock = orig


def test_resume_skips_completed():
    # 상태에 COMPLETE 주입 후 offline resume → 그 종목 재처리 안 함
    m = B.load_manifest()
    b1 = [s for s in m["stocks"] if s["batchId"] == "batch-01"]
    done_code = b1[0]["stockCode"]
    st = {"batchId": "batch-01", "stocks": {done_code: {"status": "COMPLETE"}},
          "apiCalls": 0, "rateLimited": False, "resumeAfterSelectionIndex": None}
    TMP.mkdir(parents=True, exist_ok=True)
    (TMP / "batch-state-batch-01.json").write_text(json.dumps(st), encoding="utf-8")
    B.main(["--batch-id", "batch-01", "--offline", "--resume", "--output-dir", str(TMP)])
    st2 = json.loads((TMP / "batch-state-batch-01.json").read_text(encoding="utf-8"))
    check("resume: 기존 COMPLETE 유지(재처리 안 함)", st2["stocks"][done_code]["status"] == "COMPLETE")


def test_state_no_secrets():
    files = list(TMP.glob("batch-state-*.json"))
    check("상태파일 존재", len(files) > 0)
    blob = " ".join(f.read_text(encoding="utf-8") for f in files).lower()
    bad = [w for w in ("crtfc_key", "password", "cookie", "token", "authorization", "api_key") if w in blob]
    check("상태파일 민감정보 0", bad == [], str(bad))


def test_annual_cache_untouched():
    # 배치 실행이 기존 연간 캐시 디렉토리를 건드리지 않음(경로 분리)
    ann = Q.ANNUAL_CACHE_DIR
    qua = Q.QUARTERLY_CACHE_DIR
    check("연간/분기 캐시 경로 분리", str(ann) != str(qua) and ann.name == "dart-statements" and qua.name == "dart-statements-quarterly")


def main():
    # 실제 네트워크(urlopen) 카운터 monkeypatch — 안전모드에서 0이어야(캐시 read 는 네트워크 아님)
    net = {"n": 0}
    orig_urlopen = Q.urlopen

    def counting_urlopen(*a, **k):
        net["n"] += 1
        raise AssertionError("안전모드에서 네트워크 호출 발생(있으면 안 됨)")
    Q.urlopen = counting_urlopen

    print("=== test_batch_runner ===")
    try:
        test_manifest_integrity()
        test_confirm_token()
        test_real_fetch_blocked_without_token()
        test_dryrun_zero_calls(net)
        test_offline_zero_calls(net)
        test_020_stop_and_state()
        test_fatal_dart_stop()
        test_resume_skips_completed()
        test_state_no_secrets()
        test_annual_cache_untouched()
    finally:
        Q.urlopen = orig_urlopen
    print(f"\n=== {_PASS} passed, {_FAIL} failed ===")
    print(f"total network(urlopen) calls during tests: {net['n']} (0 이어야)")
    return 0 if _FAIL == 0 and net["n"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
