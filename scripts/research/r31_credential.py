#!/usr/bin/env python3
"""R31 KRX OPEN API 인증키 게이트 — 존재 여부만 판정한다. 네트워크 0.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

왜 별도 모듈인가: 지시문 §5 는 "승인된 인증키를 **기존 승인된 secret 경로에서만**
찾고, 값은 한 글자도 출력하지 말라"고 한다. 그 판정을 매번 즉석 스크립트로
다시 짜면 탐색 범위가 세션마다 달라진다 — 그래서 한 곳에 고정한다.

── 새로 만들지 않은 것 ────────────────────────────────────────────
  탐색 자리(process env / .env.local / Windows 사용자 환경변수)와 그 이유는
  r30_source.py 가 이미 확립한 규약이다. 여기서는 **이름 별칭만 KRX 용으로**
  바꿔 같은 자리를 본다. 새 secret store·새 env 규약·새 파일을 만들지 않는다.

── 비밀값 규칙 ────────────────────────────────────────────────────
  값은 반환 경로(auth_key())로만 흐르고, 상태 보고(credential_status())에는
  이름·origin·길이·sha256 앞 8자만 남는다. 길이와 지문을 남기는 이유는
  "붙여넣다 잘렸는지"를 값 없이 판별하기 위해서다 — 둘로 키를 복원할 수 없다.

── 하지 않는 것 ───────────────────────────────────────────────────
  · 새 인증키 신청·재발급·연장·활용신청 자동 제출  (전부 승인 게이트)
  · env 쓰기(setx 포함)                            (Founder 만 한다)
  · C:\\ 또는 사용자 홈 무차별 secret 검색           (§5 금지)
  · KRX 실호출                                      (이 모듈은 네트워크 0)

사용: python scripts/research/r31_credential.py
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV_LOCAL = ROOT / ".env.local"

# 정본 이름 — R29/R30 보고서와 r29_probe.py 가 이미 이 이름을 쓴다.
CRED_ENV_PRIMARY = "KRX_OPENAPI_AUTH_KEY"

# 별칭: r29_probe.CRED_PATTERNS["KRX_OPENAPI_AUTH_KEY"] 와 동일한 의미 범위를
# 이름 목록으로 고정한다. 정규식 전수 스캔은 아래 name_scan() 이 따로 한다.
CRED_ENV_ALIASES = (
    "KRX_OPENAPI_AUTH_KEY",
    "KRX_OPEN_API_AUTH_KEY",
    "KRX_AUTH_KEY",
    "KRX_API_KEY",
    "AUTH_KEY",
)
CRED_NAME_PATTERN = re.compile(r"KRX_OPEN|KRX_API|KRX_AUTH|AUTH_KEY", re.I)


class CredentialAbsent(RuntimeError):
    """인증키 미등록. 값이 아니라 부재 사실만 담는다."""


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _read_env_local() -> dict:
    """.env.local 이름→값. 값은 반환만 하고 로그로 내보내지 않는다."""
    if not ENV_LOCAL.exists():
        return {}
    out = {}
    try:
        text = ENV_LOCAL.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$", line)
        if m:
            out[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return out


def _read_windows_env(hive_name: str) -> dict:
    """Windows 환경변수를 **읽기만** 한다. 쓰지 않는다.

    setx 로 넣은 키는 이미 열려 있던 터미널의 프로세스 환경에 반영되지 않는다.
    os.environ 만 보면 등록된 키를 ABSENT 로 오판한다(R30 에서 실제로 그랬다).
    """
    if sys.platform != "win32":
        return {}
    try:
        import winreg
    except ImportError:
        return {}
    hive, sub = {
        "user": (winreg.HKEY_CURRENT_USER, "Environment"),
        "machine": (winreg.HKEY_LOCAL_MACHINE,
                    r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
    }[hive_name]
    out = {}
    try:
        with winreg.OpenKey(hive, sub) as k:
            i = 0
            while True:
                try:
                    name, val, _ = winreg.EnumValue(k, i)
                except OSError:
                    break
                if isinstance(val, str) and val.strip():
                    out[name] = val.strip()
                i += 1
    except OSError:
        return {}
    return out


def _sources() -> list:
    return [
        ("process-env", dict(os.environ)),
        (".env.local", _read_env_local()),
        ("windows-user-env", _read_windows_env("user")),
        ("windows-machine-env", _read_windows_env("machine")),
    ]


def name_scan() -> list:
    """승인된 자리들에서 **이름만** 정규식으로 훑는다. 값은 담지 않는다.

    별칭 목록에 없는 이름으로 등록됐을 가능성을 이름 수준에서만 확인한다.
    (§5 금지: 전역 무차별 secret 검색 — 여기서는 위 4개 자리만 본다.)
    """
    found = []
    for origin, mapping in _sources():
        for name in mapping:
            if CRED_NAME_PATTERN.search(name):
                found.append({"origin": origin, "envVarName": name,
                              "present": bool((mapping.get(name) or "").strip())})
    return found


def credential_status() -> dict:
    """PRESENT / ABSENT + 어디서 왔는지. 값은 담지 않는다."""
    checked = []
    for origin, mapping in _sources():
        for name in CRED_ENV_ALIASES:
            val = (mapping.get(name) or "").strip()
            checked.append({"envVarName": name, "origin": origin,
                            "present": bool(val)})
            if val:
                return {
                    "status": "PRESENT",
                    "envVarName": name,
                    "sourceClass": origin,
                    "valueLength": len(val),
                    "fingerprint8": _fingerprint(val),
                    "checked": checked,
                    "secretPrinted": False,
                }
    return {
        "status": "ABSENT",
        "envVarName": None,
        "sourceClass": None,
        "valueLength": 0,
        "fingerprint8": None,
        "checked": checked,
        "nameScanHits": name_scan(),
        "expectedEnvVarName": CRED_ENV_PRIMARY,
        "expectedPaths": [
            "Windows 사용자 환경변수 (DART_API_KEY / DATA_GO_KR_SERVICE_KEY 와 동일 규약)",
            str(ENV_LOCAL) + " (기존 .gitignore 대상 mirror)",
        ],
        "secretPrinted": False,
    }


def auth_key() -> str:
    """실제 키. 호출자는 이 반환값을 절대 출력·기록하지 않는다."""
    for _origin, mapping in _sources():
        for name in CRED_ENV_ALIASES:
            val = (mapping.get(name) or "").strip()
            if val:
                return val
    raise CredentialAbsent(
        f"{CRED_ENV_PRIMARY} 미등록 — 값이 아니라 부재 사실만 기록한다.")


def gate() -> dict:
    """§5 게이트 판정. ABSENT 면 bulk 수집을 시작하지 않는다(fail-closed)."""
    st = credential_status()
    if st["status"] == "PRESENT":
        return {"proceed": True, "verdict": None, "reasonClass": None,
                "credential": st}
    return {
        "proceed": False,
        "verdict": "BLOCKED",
        "reasonClass": "KRX_AUTH_KEY_APPROVED_BUT_NOT_AVAILABLE_IN_EXISTING_SECRET_PATH",
        "credential": st,
        "founderAction": (
            "KRX Data Marketplace 로그인 → 발급내역에서 승인된 AUTH_KEY 확인 → "
            "채팅창에 붙이지 말고 저장소가 이미 쓰는 기존 경로에만 등록: "
            f"setx {CRED_ENV_PRIMARY} \"<발급된 키>\" (새 터미널에서 반영). "
            "새 env/security 방식을 만들지 않는다."),
        "notStarted": ["KRX 실호출", "bulk 수집", "overlap 검증", "stitching",
                       "R27 재실행"],
    }


def main() -> int:
    g = gate()
    print(json.dumps(g, ensure_ascii=False, indent=2))
    print("networkCalls: 0")
    print("secretsPrinted: 0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
