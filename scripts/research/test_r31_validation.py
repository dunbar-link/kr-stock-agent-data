#!/usr/bin/env python3
"""R31 회귀 — 계약 동결·비밀값·경계 invariant. 네트워크 0.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

지시문 §19 는 30개 검증을 요구한다. 그 중 **KRX 실호출이 있어야만 확인되는
항목**은 인증키가 없는 지금 통과시킬 수 없다. 그런 항목은 PASS 로 위장하지
않고 `NOT_RUN_CREDENTIAL_ABSENT` 로 남긴다 — 없는 검증을 있다고 적으면
다음 세션이 그것을 사실로 인수인계한다(R27 §7 과 같은 이유).

계층:
  L1 credential — 게이트 fail-closed · 값 미출력 · 탐색 범위 준수
  L2 freeze     — R27 precommit 불변 · R31 spec 동결 · threshold 정확
  L3 calendar   — 공식 거래일 집합 · 평일 추정 0 · 경계일 실측
  L4 boundary   — raw/secret Git 제외 · public repo 무변경 · 금지경로 미사용
  L5 report     — 산출물 parse · 판정 헤더 · 비밀값 미노출
  L6 notrun     — 인증키 필요 항목을 정직하게 NOT_RUN 으로 표기

사용: python scripts/research/test_r31_validation.py
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r31_credential as CRED      # noqa: E402
import r31_precommit as SPEC       # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "research"
RD = ROOT / "reports" / "research"
WD = ROOT / "reports" / "wababa"
PUBLIC_REPO = Path(r"C:\work\kr-stock-agent")

PASS = FAIL = NOTRUN = 0
CREDENTIAL_PRESENT = CRED.credential_status()["status"] == "PRESENT"

# R30 이 기록해 둔 값. 이것과 달라지면 R27 계약이 흔들린 것이다.
R27_SHA256_EXPECTED = ("acee428846d358d84e4a1ab2f92e9294"
                       "fe0c49c376cc139a5998074ae8188a65")


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" - {extra}" if extra else ""))


def notrun(name, why):
    global NOTRUN
    NOTRUN += 1
    print(f"  NOT_RUN  {name} - {why}")


# ══════════════ L1 credential ══════════════
def t_l1():
    print("\n[L1] credential gate")
    st = CRED.credential_status()
    ck("1 credential_status 가 값을 담지 않는다",
       "value" not in st and st.get("secretPrinted") is False)
    ck("2 상태는 PRESENT/ABSENT 둘 중 하나", st["status"] in ("PRESENT", "ABSENT"))

    g = CRED.gate()
    if st["status"] == "ABSENT":
        ck("3 키 부재 → fail-closed(BLOCKED)",
           g["proceed"] is False and g["verdict"] == "BLOCKED")
        ck("4 reason_class 가 지시문 §5 문자열과 정확히 일치",
           g["reasonClass"] ==
           "KRX_AUTH_KEY_APPROVED_BUT_NOT_AVAILABLE_IN_EXISTING_SECRET_PATH")
        ck("5 키 부재 시 bulk/overlap/stitch 미착수 선언",
           set(["KRX 실호출", "bulk 수집", "overlap 검증", "stitching"])
           <= set(g["notStarted"]))
        ck("6 auth_key() 는 부재 시 예외로 fail-closed",
           _raises(CRED.auth_key, CRED.CredentialAbsent))
    else:
        ck("3 키 존재 → 게이트 통과", g["proceed"] is True)
        ck("4 지문은 8자, 값 길이만 기록",
           len(st["fingerprint8"]) == 8 and isinstance(st["valueLength"], int))

    # 탐색 범위: 승인된 4자리만. 전역 파일시스템 스캔 코드가 없어야 한다.
    src = (SRC / "r31_credential.py").read_text(encoding="utf-8")
    origins = {o for o, _ in CRED._sources()}
    ck("7 탐색 자리는 승인된 4곳뿐",
       origins == {"process-env", ".env.local",
                   "windows-user-env", "windows-machine-env"}, str(origins))
    ck("8 전역 무차별 secret 검색 코드 없음",
       not re.search(r"rglob\(|walk\(|glob\.glob\(|C:\\\\\*", src))
    ck("9 env 쓰기 코드 없음(setx/putenv/SetEnvironmentVariable)",
       not re.search(r"os\.environ\[[^\]]+\]\s*=|putenv|SetEnvironmentVariable|"
                     r"subprocess.*setx", src))
    ck("10 인증키 신청·재발급 자동화 코드 없음",
       not re.search(r"(?i)(apply|register|reissue|renew).*(auth_?key|service)", src))


def _raises(fn, exc):
    try:
        fn()
    except exc:
        return True
    except Exception:
        return False
    return False


# ══════════════ L2 freeze ══════════════
def t_l2():
    print("\n[L2] 계약 동결")
    r27_sha = hashlib.sha256((SRC / "r27_precommit.py").read_bytes()).hexdigest()
    ck("11 R27 precommit sha256 이 R30 기록과 동일(threshold 불변)",
       r27_sha == R27_SHA256_EXPECTED, f"got={r27_sha[:16]}")

    from r27_precommit import COVERAGE, PRIMARY_GATE, SENSITIVITY, CAPITAL
    ck("12 BASE threshold 125,000,000원", PRIMARY_GATE["thresholdKrw"] == 125_000_000)
    ck("13 LOW threshold 25,000,000원",
       SENSITIVITY["LOW"]["thresholdKrw"] == 25_000_000)
    ck("14 HIGH threshold 250,000,000원",
       SENSITIVITY["HIGH"]["thresholdKrw"] == 250_000_000)
    ck("15 연도 coverage 90.0% · 최소 15개 연도",
       COVERAGE["minCoveragePctPerYear"] == 90.0
       and COVERAGE["minYearsCovered"] == 15)
    ck("16 capital 50,000,000 / 1종목 1,250,000원",
       CAPITAL["totalKrw"] == 50_000_000
       and CAPITAL["orderPerNameKrw"] == 1_250_000)

    spec = SPEC.build()
    ck("17 R31 spec 이 결과 이전에 작성됨을 선언",
       spec["writtenBeforeResults"] is True)
    ck("18 R31 spec 이 R27 threshold 를 복제하지 않고 정본에서 읽음",
       spec["r27Contract"]["sha256"] == r27_sha
       and spec["r27Contract"]["changedByR31"] is False)
    ck("19 backfill 시장은 KOSPI/KOSDAQ 뿐",
       spec["backfill"]["markets"] == ["KOSPI", "KOSDAQ"])
    ck("20 KONEX/ETF/ETN/ELW 는 명시적 제외",
       {"KONEX", "ETF", "ETN", "ELW"} <= set(spec["backfill"]["marketsExcluded"]))
    ck("21 직접 거래대금만 사용 · close×volume 금지",
       spec["tradeValue"]["directTradeValueOnly"] is True
       and spec["tradeValue"]["priceTimesVolumeProxy"] == "FORBIDDEN")
    g = spec["overlapGate"]["gates"]
    ck("22 overlap gate 수치 동결(1.0 / 0.99 / 0.995×3 / scale 0)",
       g["officialTradingDayCoverage"]["min"] == 1.0
       and g["r27EligibleCommonKeyCoverage"]["min"] == 0.99
       and g["directTradeValueExactMatch"]["min"] == 0.995
       and g["volumeExactMatch"]["min"] == 0.995
       and g["closeExactMatch"]["min"] == 0.995
       and g["systematicScaleMismatch"]["max"] == 0)
    ck("23 overlap 실패 시 stitching·R27 판정 금지 명시",
       "stitched dataset 생성" in spec["overlapGate"]["onFail"]["forbidden"])
    ck("24 request budget 7,500회 · probe/retry 포함",
       spec["budget"]["hardKstDailyRequestCap"] == 7500
       and set(spec["budget"]["countsTowardCap"]) == {"probe", "retry", "bulk"})
    ck("25 stitch 금지목록에 fill/평균/추정 포함",
       {"forward fill", "backward fill", "평균값 병합", "close × volume 대체"}
       <= set(spec["stitch"]["forbidden"]))
    ck("26 raw KRX 데이터 LOCAL_ONLY · 공개 금지",
       spec["dataBoundary"]["rawKrxData"] == "LOCAL_ONLY"
       and spec["dataBoundary"]["publication"] == "FORBIDDEN")
    ck("27 endpoint 는 DOCUMENTED_NOT_VERIFIED 로 표기(추정 정본화 금지)",
       spec["backfill"]["endpointStatus"].startswith("DOCUMENTED_NOT_VERIFIED"))

    p = RD / "r31-precommit-latest.json"
    ck("28 R31 spec JSON 산출물 parse 가능", _parses(p), str(p))


def _parses(p: Path) -> bool:
    try:
        json.loads(p.read_text(encoding="utf-8"))
        return True
    except Exception:
        return False


# ══════════════ L3 calendar ══════════════
def t_l3():
    print("\n[L3] 공식 거래일 집합")
    try:
        from r27_collect import trading_calendar, needed_days
        import r16_canonical as C
        from r16_audit import contiguous_span
    except Exception as e:
        ck("29 calendar 모듈 로드", False, f"{type(e).__name__}: {e}")
        return
    cal = trading_calendar()
    ck("29 calendar 로드(캐시 사용 · pykrx 재호출 0)", len(cal) > 4000, f"days={len(cal)}")
    ck("30 calendar 는 정렬·중복 없음",
       cal == sorted(cal) and len(cal) == len(set(cal)))

    # 평일 추정 금지 — 주말이 캘린더에 있으면 안 된다.
    import datetime as dt
    wk = [d for d in cal if dt.date.fromisoformat(d).weekday() >= 5]
    ck("31 캘린더에 주말 0(평일 추정 금지)", len(wk) == 0, f"weekend={len(wk)}")

    ds = contiguous_span(sorted(C.load_capital_series()))
    need = needed_days(cal, ds)
    ck("32 R27 needed_days 재현 = 4,640일", len(need) == 4640, f"got={len(need)}")

    back = [d for d in need if "2010-01-04" <= d <= "2019-12-31"]
    ovl = [d for d in need if d.startswith("2020")]
    ck("33 backfill 대상일 확정(2010~2019)", len(back) > 0, f"n={len(back)}")
    ck("34 overlap 대상일 확정(2020)", len(ovl) > 0, f"n={len(ovl)}")
    ck("35 2019 마지막 거래일을 12-31 로 가정하지 않음",
       back[-1] == "2019-12-30", f"last={back[-1]}")
    ck("36 2020 마지막 거래일 실측 사용",
       [d for d in cal if d.startswith("2020")][-1] == "2020-12-30")

    off = ROOT / "_cache" / "official-liquidity"
    have2020 = {p.name[:-7] for p in off.glob("2020-*.csv.gz")} if off.exists() else set()
    ck("37 overlap 대조군(R30 2020)이 전부 존재",
       all(d in have2020 for d in ovl), f"{len([d for d in ovl if d in have2020])}/{len(ovl)}")


# ══════════════ L4 boundary ══════════════
def t_l4():
    print("\n[L4] 데이터·저장소 경계")
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8")
    ck("38 _cache/ 가 .gitignore 대상(raw 데이터 remote 제외)", "_cache/" in gi)
    ck("39 reports/ 가 .gitignore 대상", re.search(r"(?m)^reports/", gi) is not None)
    ck("40 .env.local 이 .gitignore 대상", ".env.local" in gi)

    tracked = _git(["ls-files"], ROOT).splitlines()
    bad = [f for f in tracked
           if f.startswith("_cache/") or f.startswith("reports/")
           or f.endswith(".env") or f.endswith(".env.local")]
    ck("41 raw/secret 이 Git 에 추적되지 않음", not bad, str(bad[:5]))

    # public repo 무변경 — READ_ONLY 계약
    if PUBLIC_REPO.exists():
        st = _git(["status", "--porcelain"], PUBLIC_REPO)
        touched = [ln for ln in st.splitlines()
                   if re.search(r"r31|krx.*backfill|stitch", ln, re.I)]
        ck("42 public repo 에 이번 작업 변경 0", not touched, str(touched[:3]))
    else:
        notrun("42 public repo 무변경", "public repo 경로 없음")

    # 금지 경로 미사용
    for name in ("r31_precommit.py", "r31_credential.py"):
        s = (SRC / name).read_text(encoding="utf-8")
        ck(f"43 {name}: pykrx 대량수집·웹 스크래핑 호출 0",
           not re.search(r"(?m)^\s*(import|from)\s+pykrx|BeautifulSoup|selenium", s))
        ck(f"44 {name}: 네트워크 호출 0",
           not re.search(r"(?m)^\s*(import|from)\s+requests|urlopen|httpx", s))


def _git(args, cwd):
    try:
        return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                              text=True, timeout=60).stdout
    except Exception:
        return ""


# ══════════════ L5 report ══════════════
def t_l5():
    print("\n[L5] 산출물")
    md = WD / "wababa-krx-official-backfill-stitch-and-r27-close-r31-latest.md"
    js = WD / "wababa-krx-official-backfill-stitch-and-r27-close-r31-latest.json"
    if not md.exists():
        notrun("45 canonical MD", "아직 생성 전(보고서 작성 단계에서 생성)")
        notrun("46 canonical JSON", "아직 생성 전")
        return
    body = md.read_text(encoding="utf-8")
    first = body.splitlines()[0].strip()
    ck("45 첫 줄이 판정 헤더",
       re.match(r"^(전체 판정:\s*)?(PASS|WARNING|WAIT|BLOCKED)$", first) is not None,
       first)
    ck("46 canonical JSON parse", _parses(js))
    for need in ("reason_class", "SIZE_SMALL", "R27", "REAL_MONEY_NOT_APPROVED",
                 "Founder", "다음 단일 작업"):
        ck(f"47 보고 항목: {need}", need in body)

    # 비밀값 미노출 — 실제 등록된 값들이 보고서에 없어야 한다.
    for k in ("DART_API_KEY", "DATA_GO_KR_SERVICE_KEY", *CRED.CRED_ENV_ALIASES):
        val = (os.environ.get(k) or "").strip()
        if val and len(val) >= 8:
            ck(f"48 보고서에 {k} 값 미노출", val not in body)
    # 키 모양 = 60자 이상 연속 토큰. 단 **SHA-256(소문자 hex 64자)** 은 이 보고서의
    # 동결 anchor 라서 의도적으로 들어간다 — 그것까지 잡으면 진짜 유출을 못 본다.
    blobs = [x for x in re.findall(r"[A-Za-z0-9+/=%]{60,}", body)
             if not re.fullmatch(r"[0-9a-f]{64}", x)]
    ck("49 보고서에 KRX/포털 키 모양 문자열 없음(sha256 제외)", not blobs,
       f"n={len(blobs)}")


# ══════════════ L6 정직한 NOT_RUN ══════════════
def t_l6():
    print("\n[L6] 인증키 필요 — PASS 로 위장하지 않는다")
    if CREDENTIAL_PRESENT:
        print("  (credential PRESENT — 다음 세션에서 실호출 검증 대상)")
    why = "KRX AUTH_KEY 부재 — 실호출 0이라 확인 불가"
    for n in ("sample anchor response parse",
              "KOSPI/KOSDAQ schema validation",
              "leading-zero code preservation (live)",
              "direct trade value field 사용 확인 (live)",
              "resume/idempotency (KRX collector)",
              "partial-file atomicity (KRX collector)",
              "duplicate primary key 0 (KRX)",
              "request budget accounting",
              "429 circuit breaker",
              "service approval failure fail-closed",
              "2020 overlap full-period comparison",
              "systematic scale mismatch 검사",
              "stitch boundary duplicate 0",
              "source provenance completeness",
              "PIT trailing-window 검사 (stitched)",
              "future leakage 0 (stitched)",
              "survivorship filter regression (stitched)",
              "연도 coverage 분모 회귀 (stitched)"):
        notrun(n, why)


def main() -> int:
    for f in (t_l1, t_l2, t_l3, t_l4, t_l5, t_l6):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_RUN {NOTRUN}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    print("networkCalls: 0")
    print("secretsPrinted: 0")
    print(f"credentialPresent: {int(CREDENTIAL_PRESENT)}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
