#!/usr/bin/env python3
"""R2 회귀 — KRX auth 관찰기 · 마법공식 최초 실패 · 경계.

WABABA-KRX-LOGIN-ERROR-CODE-SURFACE-AND-MF-LANE-BLOCK-ROOT-CAUSE-R2

이 테스트는 **네트워크를 쓰지 않고 pykrx 를 import 하지 않는다**
(import 만으로 KRX 로그인 POST 가 나가기 때문이다).

사용: python scripts/test_krx_auth_observer.py
"""
from __future__ import annotations

import ast
import base64
import csv
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[1]
SCR = ROOT / "scripts"
RD = ROOT / "reports" / "research"
sys.path.insert(0, str(SCR))
import krx_auth_observer as OBS      # noqa: E402

SITE = Path(sys.prefix) / "Lib" / "site-packages"
if not (SITE / "pykrx").exists():
    for p in sys.path:
        if p.endswith("site-packages") and (Path(p) / "pykrx").exists():
            SITE = Path(p)
            break

PASS = FAIL = NOTDUE = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}" + (f" - {extra}" if extra else ""))


def notdue(name, why):
    global NOTDUE
    NOTDUE += 1
    print(f"  NOT_YET_DUE  {name} - {why}")


def git(a, cwd=ROOT):
    try:
        return subprocess.run(["git", *a], cwd=cwd, capture_output=True,
                              text=True, timeout=90).stdout
    except Exception:
        return ""


class FakeResp:
    def __init__(self, body=b"", status=200, ctype="application/json"):
        self.content = body
        self.status_code = status
        self.headers = {"Content-Type": ctype}


def t_env():
    print("\n[L1] 저장소·런타임 상태")
    ck("HEAD == origin/main",
       git(["rev-parse", "HEAD"]).strip() == git(["rev-parse", "origin/main"]).strip())
    ck("known dirty 미스테이지",
       all(not x.startswith("M  financial-universe-real.json")
           for x in git(["status", "--short"]).splitlines()))
    ck("pykrx 설치 확인", (SITE / "pykrx").exists(), str(SITE))
    dist = list(SITE.glob("pykrx-*.dist-info"))
    ck("dist-info 1개", len(dist) == 1)
    if dist:
        meta = (dist[0] / "METADATA").read_text(encoding="utf-8", errors="replace")
        ck("version 1.2.7", "Version: 1.2.7" in meta)
        ck("PyPI 설치 (direct_url 없음)", not (dist[0] / "direct_url.json").exists())
        ck("INSTALLER pip",
           (dist[0] / "INSTALLER").read_text(encoding="utf-8").strip() == "pip")
        rec = dist[0] / "RECORD"
        ck("RECORD 존재", rec.exists())
        want = {}
        for row in csv.reader(rec.read_text(encoding="utf-8").splitlines()):
            if len(row) >= 2 and row[1].startswith("sha256="):
                want[row[0]] = row[1][7:]
        ck("RECORD 에 auth.py 등재", "pykrx/website/comm/auth.py" in want)
        bad = []
        for name, h in want.items():
            p = SITE / name
            if p.exists():
                d = base64.urlsafe_b64encode(
                    hashlib.sha256(p.read_bytes()).digest()).decode().rstrip("=")
                if d != h:
                    bad.append(name)
        ck("RECORD 전체 hash 일치(설치 후 변조 0)", not bad, str(bad[:3]))
        auth = SITE / "pykrx/website/comm/auth.py"
        webio = SITE / "pykrx/website/comm/webio.py"
        ck("auth.py sha256 기록",
           hashlib.sha256(auth.read_bytes()).hexdigest().startswith("32defaee"))
        ck("webio.py sha256 기록",
           hashlib.sha256(webio.read_bytes()).hexdigest().startswith("85df8800"))


def t_static():
    print("\n[L2] pykrx 정적 사실 (import 하지 않는다)")
    auth = (SITE / "pykrx/website/comm/auth.py").read_text(encoding="utf-8")
    webio = (SITE / "pykrx/website/comm/webio.py").read_text(encoding="utf-8")
    ck("로그인 endpoint 상수 존재", OBS.TARGET_PATH in auth)
    ck("관찰 host 가 실제 상수와 일치", OBS.TARGET_HOST in auth)
    ck("KRX_ID/KRX_PW 를 env 에서 읽음",
       'os.getenv("KRX_ID")' in auth and 'os.getenv("KRX_PW")' in auth)
    ck("CD001 성공 분기 존재", 'error_code == "CD001"' in auth)
    ck("CD010 분기 존재", 'error_code == "CD010"' in auth)
    ck("CD011 분기 존재", 'error_code == "CD011"' in auth)
    ck("명시적 invalid-credential 코드 mapping 없음",
       "INVALID" not in auth.upper().replace("INVALID_CREDENTIAL_PLACEHOLDER", ""))
    # auth.py 는 `return error_code == "CD001"` 로 **bool 만** 돌려준다.
    # 부분문자열 "return error_code" 로 보면 오탐이므로, 코드값이 밖으로
    # 나가는 경로(로그·반환값)가 없다는 것을 정확히 본다.
    ck("_error_code 를 bool 로만 축약(값 유실)",
       'return error_code == "CD001"' in auth
       and not re.search(r"print\([^)]*error_code", auth)
       and not re.search(r"(?m)^\s*return\s+error_code\s*$", auth))
    ck("import 시 자동 로그인 (webio 모듈 레벨)",
       "_session = build_krx_session()" in webio)
    # 이 테스트 자체가 pykrx 를 import 하지 않는지
    src = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    imports = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            imports += [a.name.split(".")[0] for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module:
            imports.append(n.module.split(".")[0])
    ck("테스트가 pykrx 를 import 하지 않음", "pykrx" not in imports)
    # 관찰기 docstring 은 pykrx 를 설명한다(왜 필요한지). 문자열 검색은 오탐이라
    # 실제 import 문만 AST 로 본다.
    obs_src = (SCR / "krx_auth_observer.py").read_text(encoding="utf-8")
    obs_imports = []
    for n in ast.walk(ast.parse(obs_src)):
        if isinstance(n, ast.Import):
            obs_imports += [a.name.split(".")[0] for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module:
            obs_imports.append(n.module.split(".")[0])
    ck("관찰기도 pykrx 를 import 하지 않음", "pykrx" not in obs_imports,
       str(obs_imports))


def t_classify():
    print("\n[L3] 코드 분류 fixture (네트워크 0)")
    C = OBS.classify
    ck("CD001 → 수용", C("CD001", json_parsed=True) == "STORED_CREDENTIAL_ACCEPTED")
    ck("CD010 → 비번변경/계정상태",
       C("CD010", json_parsed=True) == "PASSWORD_CHANGE_OR_ACCOUNT_STATE_REQUIRED")
    ck("CD011 → 중복로그인",
       C("CD011", json_parsed=True) == "DUPLICATE_LOGIN_RETRIED_BY_LIBRARY")
    for unknown in ("CD002", "CD099", "EX001"):
        ck(f"미매핑 코드 {unknown} → UNMAPPED (추측 금지)",
           C(unknown, json_parsed=True) == "AUTH_CODE_CAPTURED_BUT_UNMAPPED")
    ck("코드 없음 → 별도 분류",
       C(None, json_parsed=True) == "AUTH_CODE_ABSENT_IN_JSON")
    ck("HTML 응답 → HTML 미매핑",
       C(None, json_parsed=False, content_type="text/html; charset=UTF-8")
       == "NON_JSON_HTML_RESPONSE_UNMAPPED")
    ck("비JSON 응답 → 미매핑",
       C(None, json_parsed=False, content_type="text/plain")
       == "NON_JSON_RESPONSE_UNMAPPED")
    ck("mapping 근거가 설치 패키지",
       "auth.py" in OBS.CODE_MAP_EVIDENCE)


def t_extract():
    print("\n[L4] 안전 추출 — 본문·비밀 미기록")
    e = OBS._safe_extract(FakeResp(b'{"_error_code":"CD001","_error_message":"OK"}'))
    ck("성공 코드 추출", e["errorCode"] == "CD001" and e["success"] is True)
    ck("본문 길이만 기록", e["responseByteLength"] > 0)
    ck("본문 해시만 기록", isinstance(e["responseSha256"], str))
    blob = json.dumps(e, ensure_ascii=False)
    ck("응답 메시지 원문 미기록", "OK" not in blob or "_error_message" not in blob)
    ck("본문 자체 미기록", "_error_code" not in blob.replace("errorCode", ""))
    e2 = OBS._safe_extract(FakeResp(b"<html>login</html>", ctype="text/html"))
    ck("malformed/HTML fail-closed", e2["jsonParsed"] is False
       and e2["mappedClass"] == "NON_JSON_HTML_RESPONSE_UNMAPPED")
    e3 = OBS._safe_extract(FakeResp(b"{not json", ctype="application/json"))
    ck("깨진 JSON fail-closed", e3["jsonParsed"] is False)
    e4 = OBS._safe_extract(FakeResp(b"", status=302, ctype=""))
    ck("redirect/빈 본문 분류", e4["httpStatus"] == 302
       and e4["responseSha256"] is None)


def t_scope():
    print("\n[L5] 관찰 범위 · 복원")
    ck("정확 host+path 만 매치", OBS._matches(
        "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"))
    for other in ("https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                  "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd",
                  "https://opendart.fss.or.kr/api/list.json",
                  "https://example.com/contents/MDC/COMS/client/MDCCOMS001D1.cmd"):
        ck(f"타 endpoint 미간섭: {other.split('/')[2]}", not OBS._matches(other))
    ck("잘못된 URL fail-closed", not OBS._matches("not a url"))

    import requests
    orig = requests.sessions.Session.request
    ck("install 성공", OBS.install() is True)
    ck("install 후 래핑됨", requests.sessions.Session.request is not orig)
    ck("is_installed True", OBS.is_installed() is True)
    ck("중복 install 무해", OBS.install() is True)
    ck("uninstall 성공", OBS.uninstall() is True)
    ck("원래 method 복원", requests.sessions.Session.request is orig)
    ck("is_installed False", OBS.is_installed() is False)
    ck("uninstall 재호출 안전", OBS.uninstall() is False)
    ck("관찰기가 새 요청을 만들지 않음",
       "requests.post(" not in (SCR / "krx_auth_observer.py").read_text(encoding="utf-8")
       and "urlopen(" not in (SCR / "krx_auth_observer.py").read_text(encoding="utf-8"))


def t_lane():
    print("\n[L6] 마법공식 lane 최초 실패 사슬 (정적)")
    bms = (SCR / "build_market_snapshot.py").read_text(encoding="utf-8")
    ck("build_market_snapshot 모듈레벨 pykrx import",
       bool(re.search(r"(?m)^from pykrx import", bms)))
    ck("14일 영업일 탐색이 pykrx 호출",
       "get_market_ohlcv_by_ticker" in bms and "range(0, 14)" in bms)
    ck("탐색 실패 시 RuntimeError",
       "최근 영업일을 찾지 못했습니다" in bms)
    bmsf = (SCR / "build_market_snapshot_fast.py").read_text(encoding="utf-8")
    ck("fast 가 build_market_snapshot 재사용",
       "from build_market_snapshot import" in bmsf)
    pkg = (SCR / "build_magic_signal_package.py").read_text(encoding="utf-8")
    ck("signal package 가 fast 를 지연 import",
       "import build_market_snapshot_fast" in pkg)
    sig = (SCR / "magic_daily_signal.py").read_text(encoding="utf-8")
    ck("signal 이 package 를 호출", "build_magic_signal_package" in sig)
    ck("BLOCKED → exit 2 설계",
       'return 0 if r["status"] in ("READY", "ALREADY_PREPARED", '
       '"SELF_SKIPPED_NON_TRADING_DAY") else 2' in sig)
    fund = (SCR / "build_magic_formula_fund.py").read_text(encoding="utf-8")
    ck("fund 의 pykrx 는 함수내부 지연 import (조건부)",
       not re.search(r"(?m)^from pykrx import", fund)
       and "def fetch_open_map_pykrx" in fund)

    # ── pre-import hook 배선 (site-packages 미변경)
    ck("hook 이 pykrx import **앞**에 설치", bms.index("_krx_obs.install()")
       < bms.index("from pykrx import stock"))
    ck("hook 이 import **뒤**에 복원", bms.index("_krx_obs.uninstall()")
       > bms.index("from pykrx import stock"))
    ck("hook 실패 격리(try/except)",
       "except Exception:  # noqa: BLE001" in bms and "_krx_obs = None" in bms)
    ck("hook 이 additive (기존 import 유지)",
       "from pykrx import stock" in bms)
    ck("관찰기가 같은 디렉터리(sys.path 해석 가능)",
       (SCR / "krx_auth_observer.py").exists())
    ck("site-packages 수정 0 — 저장소 쪽에만 배선",
       "site-packages" not in bms)


def t_evidence():
    print("\n[L7] 자연 실행 증거 (오늘 실측)")
    import os as _os
    tmp = Path(_os.path.expandvars(r"%LOCALAPPDATA%\Temp\wababa-magic-signal")) / "reports"
    if not tmp.exists():
        notdue("TEMP 리포트", "디렉터리 없음")
        return
    sig = sorted(tmp.glob("signal-2026-*.json"))
    ck("signal 리포트 존재", bool(sig))
    if sig:
        d = json.loads(sig[-1].read_text(encoding="utf-8"))
        ck("최신 signal BLOCKED", d.get("status") == "BLOCKED", str(d.get("status")))
        ck("최초 실패 = 생성단계 오류",
           d.get("blockedCode") == "BLOCKED_GENERATION_ERROR", str(d.get("blockedCode")))
        ck("사유 = 영업일 탐색 실패",
           "최근 영업일을 찾지 못했습니다" in str(d.get("reason")))
        ck("production write 0", d.get("productionWriteCount", 0) == 0)
        ck("가짜거래 없음", d.get("noFakeTrade") is True)
    dry = sorted(tmp.glob("dry-run-2026-*.json"))
    if dry:
        d = json.loads(dry[-1].read_text(encoding="utf-8"))
        ck("dry-run 은 전파된 실패",
           d.get("blockedCode") == "BLOCKED_SIGNAL_PACKAGE_MISSING",
           str(d.get("blockedCode")))
    ap = sorted(tmp.glob("auto-apply-2026-*.json"))
    if ap:
        d = json.loads(ap[-1].read_text(encoding="utf-8"))
        ck("auto-apply 는 전파된 skip",
           d.get("status") == "SKIPPED_NOT_READY", str(d.get("status")))
        ck("auto-apply 실주문 0", d.get("realOrderCount", 0) == 0)
        ck("canonical 미변경", d.get("canonicalChanged") is False)


def t_capture():
    print("\n[L8] capture 결과 (자연 실행 도래 전이면 NOT_YET_DUE)")
    p = RD / "krx-web-auth-safe-status-latest.json"
    if not p.exists():
        notdue("safe auth code artifact", "다음 자연 15:40 실행 전")
        notdue("mappedClass 확정", "다음 자연 15:40 실행 전")
        return
    d = json.loads(p.read_text(encoding="utf-8"))
    ck("관찰기가 새 요청 0", d["newAuthRequestsCreatedByObserver"] == 0)
    ck("secret 노출 0", d["secretExposureCount"] == 0)
    ck("host/path 정확", d["host"] == OBS.TARGET_HOST and d["path"] == OBS.TARGET_PATH)
    lat = d.get("latest")
    if lat:
        ck("본문 미기록", lat["responseBodyRecorded"] is False)
        ck("헤더 미기록", lat["requestHeadersRecorded"] is False)
        ck("쿠키 미기록", lat["cookiesRecorded"] is False)
        ck("요청 본문 미기록", lat["requestBodyRecorded"] is False)
        ck("mappedClass 존재", bool(lat.get("mappedClass")))


def t_bound():
    print("\n[L9] 경계")
    obs = (SCR / "krx_auth_observer.py").read_text(encoding="utf-8")
    for bad in ("KRX_PW", "mbrId", '"pw"', "JSESSIONID", "Cookie", "Authorization",
                "getenv", "environ"):
        ck(f"관찰기에 {bad} 없음", bad not in obs)
    ck("응답 본문 저장 코드 없음",
       "resp.text" not in obs and "json.dump(body" not in obs)
    blob = ""
    p = RD / "krx-web-auth-safe-status-latest.json"
    if p.exists():
        blob = p.read_text(encoding="utf-8")
    ck("artifact 에 6자리 종목코드 없음", not re.findall(r'"\d{6}"', blob))
    ck("artifact 에 key 형태 토큰 없음",
       not [w for w in re.findall(r"[A-Za-z0-9]{40,}", blob)
            if not re.fullmatch(r"[0-9a-f]{64}", w) and re.search(r"[0-9]", w)])
    ck("_cache Git 추적 0", git(["ls-files", "_cache"]).strip() == "")
    staged = [x for x in git(["diff", "--cached", "--name-only"]).splitlines() if x]
    ck("staged 에 reports/_cache 없음",
       not any(x.startswith(("reports", "_cache")) for x in staged), str(staged))
    ck("site-packages 변경 0 (RECORD 대조는 L1)", True)
    pub = Path(r"C:\work\kr-stock-agent")
    if pub.exists():
        ck("public repo HEAD 2c8a000",
           git(["rev-parse", "--short", "HEAD"], cwd=pub).strip() == "2c8a000")
    ck("git diff --check clean",
       "trailing whitespace" not in git(["diff", "--check"]).lower())


def main() -> int:
    for f in (t_env, t_static, t_classify, t_extract, t_scope, t_lane,
              t_evidence, t_capture, t_bound):
        f()
    print(f"\n결과: PASS {PASS} / FAIL {FAIL} / NOT_YET_DUE {NOTDUE}")
    print(f"verdict: {'PASS' if FAIL == 0 else 'FAIL'}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
