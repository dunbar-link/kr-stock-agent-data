#!/usr/bin/env python3
"""KRX 웹 로그인 응답의 **안전한 코드만** 계측한다.

WABABA-KRX-LOGIN-ERROR-CODE-SURFACE-AND-MF-LANE-BLOCK-ROOT-CAUSE-R2

왜 필요한가
-----------
pykrx 1.2.7 은 `import` 시점에 `webio.py` 모듈 레벨에서 `build_krx_session()` 을
호출해 data.krx.co.kr 에 로그인 POST 를 보낸다. 그런데 `login_krx()` 는
KRX 가 돌려준 `_error_code` 를 **버리고** CD001 이 아니면 전부 같은 문구
("자격 증명을 확인하세요") 로 뭉갠다. 그래서 ID/PW 불일치인지, 정책 거부인지,
추가인증 요구인지 구분할 수 없다.

이 모듈은 **site-packages 를 건드리지 않고** 저장소 쪽에서 그 코드만 회수한다.

무엇을 하지 않는가
------------------
- 요청/응답 **본문·헤더·쿠키·토큰·비밀번호를 저장하지 않는다**
- 로그인 동작 자체를 바꾸지 않는다 (원래 응답을 그대로 돌려준다)
- 지정한 host/path 이외의 어떤 HTTP 요청도 건드리지 않는다
- 네트워크 요청을 **새로 만들지 않는다** (이미 발생하는 요청만 관찰)

사용
----
    import krx_auth_observer as OBS
    OBS.install()
    try:
        from pykrx import stock      # 여기서 로그인 POST 가 발생한다
    finally:
        OBS.uninstall()
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "reports" / "research" / "krx-web-auth-safe-status-latest.json"
KST = timezone(timedelta(hours=9))

# 설치된 pykrx 가 실제로 쓰는 로그인 endpoint (auth.py 정적 확인값)
TARGET_HOST = "data.krx.co.kr"
TARGET_PATH = "/contents/MDC/COMS/client/MDCCOMS001D1.cmd"

# §10 우선순위 1 — 설치된 패키지 코드에 **명시된** mapping 만 쓴다.
#   pykrx/website/comm/auth.py
#     line 158  if error_code == "CD010":  # 패스워드 변경 필요
#     line 165  if error_code == "CD011":  # 중복 로그인 (skipDup 처리)
#     line 172  return error_code == "CD001"  # CD001 = 정상
# 그 밖의 코드는 근거가 없으므로 UNMAPPED 로 남긴다(추측 금지).
CODE_MAP = {
    "CD001": "STORED_CREDENTIAL_ACCEPTED",
    "CD010": "PASSWORD_CHANGE_OR_ACCOUNT_STATE_REQUIRED",
    "CD011": "DUPLICATE_LOGIN_RETRIED_BY_LIBRARY",
}
CODE_MAP_EVIDENCE = "installed pykrx 1.2.7 website/comm/auth.py explicit branches"

_original_request = None
_installed = False
_records: list[dict] = []


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def classify(error_code, *, json_parsed: bool, content_type: str = "") -> str:
    """안전 분류. 근거 없는 추측을 하지 않는다."""
    if json_parsed:
        if not error_code:
            return "AUTH_CODE_ABSENT_IN_JSON"
        return CODE_MAP.get(error_code, "AUTH_CODE_CAPTURED_BUT_UNMAPPED")
    # JSON 이 아니면 로그인 흐름 자체가 다른 표면으로 바뀐 것이다.
    # 무엇으로 바뀌었는지는 본문을 보지 않고 단정하지 않는다.
    if "html" in (content_type or "").lower():
        return "NON_JSON_HTML_RESPONSE_UNMAPPED"
    return "NON_JSON_RESPONSE_UNMAPPED"


def _safe_extract(resp) -> dict:
    """응답에서 **안전한 metadata 만** 뽑는다. 본문은 저장하지 않는다."""
    body = b""
    try:
        body = resp.content or b""
    except Exception:  # noqa: BLE001
        body = b""
    ctype = ""
    try:
        ctype = (resp.headers or {}).get("Content-Type", "") or ""
    except Exception:  # noqa: BLE001
        ctype = ""
    error_code = None
    json_parsed = False
    try:
        data = json.loads(body.decode("utf-8", errors="replace")) if body else None
        if isinstance(data, dict):
            json_parsed = True
            v = data.get("_error_code")
            # 코드 자체는 비밀이 아니지만, 형식이 이상하면 잘라서 보관한다.
            if isinstance(v, str):
                error_code = v[:32]
    except Exception:  # noqa: BLE001
        json_parsed = False
    return {
        "httpStatus": getattr(resp, "status_code", None),
        "contentType": ctype.split(";")[0].strip()[:64],
        "responseByteLength": len(body),
        # 본문 대신 해시만 — 회차 간 동일 응답 여부를 비교할 수 있다.
        "responseSha256": hashlib.sha256(body).hexdigest() if body else None,
        "errorCode": error_code,
        "jsonParsed": json_parsed,
        "mappedClass": classify(error_code, json_parsed=json_parsed,
                                content_type=ctype),
        "mappingEvidence": CODE_MAP_EVIDENCE,
        "success": error_code == "CD001",
    }


def _matches(url: str) -> bool:
    try:
        from urllib.parse import urlsplit
        u = urlsplit(url or "")
        return u.hostname == TARGET_HOST and u.path == TARGET_PATH
    except Exception:  # noqa: BLE001
        return False


def install() -> bool:
    """requests.Session.request 를 감싼다. 지정 endpoint 외에는 손대지 않는다."""
    global _original_request, _installed
    if _installed:
        return True
    try:
        import requests
    except Exception:  # noqa: BLE001
        return False
    _original_request = requests.sessions.Session.request

    def wrapped(self, method, url, *a, **kw):
        resp = _original_request(self, method, url, *a, **kw)
        # 원래 동작을 절대 바꾸지 않는다 — 관찰만 하고 그대로 돌려준다.
        try:
            if str(method).upper() == "POST" and _matches(url):
                rec = _safe_extract(resp)
                rec.update({
                    "observedAt": _now(),
                    "host": TARGET_HOST, "path": TARGET_PATH, "method": "POST",
                    "pid": os.getpid(),
                    "entrypoint": os.path.basename(
                        getattr(__import__("sys"), "argv", [""])[0] or ""),
                    "attemptIndexInProcess": len(_records) + 1,
                    "secretExposureCount": 0,
                    "requestBodyRecorded": False,
                    "requestHeadersRecorded": False,
                    "cookiesRecorded": False,
                    "responseBodyRecorded": False,
                })
                _records.append(rec)
                _write()
        except Exception:  # noqa: BLE001
            # 관찰 실패가 본 실행을 절대 깨뜨리지 않는다.
            pass
        return resp

    requests.sessions.Session.request = wrapped
    _installed = True
    return True


def uninstall() -> bool:
    """원래 method 를 복원한다."""
    global _installed
    if not _installed:
        return False
    try:
        import requests
        requests.sessions.Session.request = _original_request
    except Exception:  # noqa: BLE001
        return False
    _installed = False
    return True


def is_installed() -> bool:
    return _installed


def records() -> list[dict]:
    return list(_records)


def _write() -> None:
    """atomic local write. 비밀값·본문 없음."""
    payload = {
        "task": "WABABA-KRX-LOGIN-ERROR-CODE-SURFACE-AND-MF-LANE-BLOCK-ROOT-CAUSE-R2",
        "schema": "krx-web-auth-safe-status-v1",
        "updatedAt": _now(),
        "host": TARGET_HOST, "path": TARGET_PATH,
        "observedAttemptCount": len(_records),
        "newAuthRequestsCreatedByObserver": 0,
        "attempts": _records[-20:],
        "latest": _records[-1] if _records else None,
        "secretExposureCount": 0,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(OUT_PATH.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, OUT_PATH)
    except Exception:  # noqa: BLE001
        try:
            os.unlink(tmp)
        except Exception:  # noqa: BLE001
            pass


class observe:
    """with 문으로 import 구간만 감싼다."""

    def __enter__(self):
        install()
        return self

    def __exit__(self, *exc):
        uninstall()
        return False
