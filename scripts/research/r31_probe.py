#!/usr/bin/env python3
"""R31 KRX OPEN API 최소 probe — 활용승인 여부만 실호출로 확인한다.

WABABA-KRX-OFFICIAL-BACKFILL-STITCH-AND-R27-CLOSE-R31

목적은 데이터 수집이 아니다. **이 계정으로 두 서비스를 부를 수 있는가**만
최소 호출로 판정한다(§5). bulk 수집은 이 probe 가 통과한 뒤에만 시작한다.

── 비밀값 규칙 ────────────────────────────────────────────────────
  AUTH_KEY 는 Request Header 로만 전달하고, 값·전체 헤더·전체 URL 을 출력하지
  않는다. 실패 응답 본문은 진단에 필요한 만큼만 자르고, 그 안에 키가 섞여 있으면
  마스킹한다(응답에 키가 반사되는 게이트웨이가 있다).

── 추정 금지 ──────────────────────────────────────────────────────
  유가증권 endpoint 는 repo(r29_probe.py)가 기록해 둔 값이다.
  코스닥 endpoint 는 repo 에 기록이 없어 **후보를 시험**하고, 문서화된 스키마로
  응답이 확인될 때만 VERIFIED 로 승격한다. 확인 전에는 정본으로 쓰지 않는다.

── circuit breaker (§11 동결 spec) ────────────────────────────────
  auth 오류 1회 · 활용승인 오류 1회 · 429 1회 → 즉시 중단.
  동일 4xx 3연속 → 중단. 5xx 3연속 → bounded WAIT. malformed JSON 3연속 → 중단.
  probe·retry 호출도 전부 예산에 포함한다.

사용: python scripts/research/r31_probe.py
부작용: reports/research/r31-service-probe-latest.json 1개 write. 수집 0.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r31_credential as CRED  # noqa: E402

import requests  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

# repo 기록값 (r29_probe.KRX_OPENAPI_BASE) — 유가증권만 기록돼 있다.
KOSPI_ENDPOINT = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
# 코스닥: repo 기록 없음. 같은 서비스군의 명명 규칙 후보를 시험만 한다.
KOSDAQ_CANDIDATES = [
    "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd",
]
DATE_PARAM_CANDIDATES = ["basDd"]

PROBE_DATES = ["2020-01-02", "2010-01-04"]
# §4-B 승인 전파 재확인 간격. 테스트는 R31_RECHECK_DELAY_SEC=0 으로 낮춘다.
RECHECK_DELAY_SEC = int(os.environ.get("R31_RECHECK_DELAY_SEC", "60"))
TIMEOUT = 30
MAX_RETRY = 3
RETRY_BACKOFF = (1.0, 3.0, 8.0)

# 문서화된 일별매매정보 필드(있으면 스키마 확인). 없으면 실패로 표면화한다.
FIELD_ALIASES = {
    "date": ["BAS_DD", "basDd", "TRD_DD"],
    "code": ["ISU_CD", "isuCd", "ISU_SRT_CD", "SRT_CD"],
    "close": ["TDD_CLSPRC", "tddClsprc", "CLSPRC"],
    "volume": ["ACC_TRDVOL", "accTrdvol", "TRDVOL"],
    "tradedValue": ["ACC_TRDVAL", "accTrdval", "TRDVAL"],
}


class Budget:
    """probe·retry 를 한 계정에 모아 센다(§11)."""

    def __init__(self, cap=7500):
        self.cap = cap
        self.calls = 0
        self.retries = 0
        self.rate_limit_events = 0

    def spend(self, retry=False):
        self.calls += 1
        if retry:
            self.retries += 1
        if self.calls > self.cap:
            raise RuntimeError("BUDGET_EXCEEDED")


def _mask(text, key):
    """응답이 키를 반사해도 남기지 않는다."""
    t = str(text or "")
    if key and len(key) >= 8:
        t = t.replace(key, "[REDACTED_AUTH_KEY]")
    return t


def _classify(status, body, parsed):
    """공식 문서 문자열에 의존하지 않고 **관측된 신호**로 분류한다."""
    b = (body or "").lower()
    if status in (401, 403):
        # ★ 2026-09-06 교정: 초판은 본문에 'authoriz' 가 있으면 SERVICE_NOT_APPROVED
        #   로 단정했다. 근거 없이 단정하면 R30 의 오진(User-Agent 사고)을 반복한다.
        # ★ 2026-09-06 실측(discriminate 대조): 이 게이트웨이는 두 경우를 **다른 문구**로
        #   구분한다 — 무효키/빈키는 "Unauthorized Key", 등록키+미승인은
        #   "Unauthorized API Call". 추정이 아니라 대조로 확인된 동작이라 이제
        #   본문으로 나눈다. 그래도 discriminate() 대조는 유지해 회귀를 잡는다.
        if "unauthorized key" in b:
            return "UNAUTHORIZED_KEY"          # 키 자체 거부
        if "unauthorized api call" in b:
            return "UNAUTHORIZED_API_CALL"     # 키는 인식, 이 API 활용승인 없음
        return "AUTH_OR_APPROVAL_401"          # 문구가 바뀐 경우 — 단정하지 않는다
    if status == 429:
        return "RATE_LIMITED"
    if status == 404:
        return "ENDPOINT_NOT_FOUND"
    if 500 <= status < 600:
        return "SERVER_ERROR"
    if status == 200:
        if parsed is None:
            return "NON_JSON_200"
        return "OK"
    return f"HTTP_{status}"


def _rows_of(parsed):
    """응답 구조를 추정 하드코딩하지 않고 행 배열을 탐색한다."""
    if parsed is None:
        return None, None
    if isinstance(parsed, list):
        return parsed, "<root list>"
    if isinstance(parsed, dict):
        for k, v in parsed.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v, k
        for k, v in parsed.items():
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, list) and v2 and isinstance(v2[0], dict):
                        return v2, f"{k}.{k2}"
    return None, None


def _fields(rows):
    if not rows:
        return {}
    keys = set(rows[0])
    out = {}
    for logical, aliases in FIELD_ALIASES.items():
        hit = next((a for a in aliases if a in keys), None)
        out[logical] = hit
    return out


def call(endpoint, date_param, date_str, key, budget):
    """1회 호출. 값·전체 헤더·전체 URL 을 출력하지 않는다."""
    headers = {"AUTH_KEY": key}          # 공식 규격: 헤더로만 전달
    params = {date_param: date_str.replace("-", "")}
    last = None
    for attempt in range(MAX_RETRY):
        budget.spend(retry=(attempt > 0))
        try:
            r = requests.get(endpoint, headers=headers, params=params,
                             timeout=TIMEOUT)
        except requests.RequestException as e:
            last = {"status": 0, "kind": "NETWORK_ERROR",
                    "detail": type(e).__name__}
            if attempt < MAX_RETRY - 1:
                time.sleep(RETRY_BACKOFF[attempt])
                continue
            return last
        body = r.text or ""
        parsed = None
        try:
            parsed = r.json()
        except ValueError:
            parsed = None
        kind = _classify(r.status_code, body, parsed)
        rows, rows_key = _rows_of(parsed)
        res = {
            "status": r.status_code,
            "kind": kind,
            "contentType": (r.headers.get("Content-Type") or "").split(";")[0],
            "jsonParsed": parsed is not None,
            "rowsKey": rows_key,
            "rowCount": (len(rows) if rows is not None else None),
            "fields": _fields(rows),
            "bodyHead": _mask(body[:280], key) if kind != "OK" else "",
        }
        if kind == "RATE_LIMITED":
            budget.rate_limit_events += 1
            return res
        if kind == "SERVER_ERROR" and attempt < MAX_RETRY - 1:
            last = res
            time.sleep(RETRY_BACKOFF[attempt])
            continue
        return res
    return last


def discriminate(endpoint, date_param, key, budget, log):
    """401 이 '키 거부' 인지 '활용 미승인' 인지 대조로 가른다 (R30 §4-1-B 기법 재사용).

    게이트웨이가 무효키와 정상키를 **같은 응답**으로 처리하면 응답만으로는
    구분할 수 없다 — 그때는 단정하지 않고 INDISTINGUISHABLE 로 표면화한다.
    호출 2회. 예산에 포함된다.
    """
    probes = {}
    for label, k in (("invalidKey", "0" * len(key)), ("emptyKey", "")):
        res = call(endpoint, date_param, PROBE_DATES[0], k, budget)
        log.append({"market": "DISCRIMINATE", "endpoint": endpoint,
                    "dateParam": date_param, "date": PROBE_DATES[0],
                    "keyClass": label, **res})
        probes[label] = {"status": res["status"], "kind": res["kind"],
                         "bodyHead": res.get("bodyHead", "")}
    return probes


def probe_market(name, endpoints, key, budget, log):
    """endpoint 후보를 순서대로 시험한다. 첫 성공에서 멈춘다."""
    for ep in endpoints:
        for dp in DATE_PARAM_CANDIDATES:
            res = call(ep, dp, PROBE_DATES[0], key, budget)
            log.append({"market": name, "endpoint": ep, "dateParam": dp,
                        "date": PROBE_DATES[0], **res})
            if res["kind"] in ("AUTH_OR_APPROVAL_401", "UNAUTHORIZED_KEY",
                               "UNAUTHORIZED_API_CALL", "RATE_LIMITED"):
                return {"market": name, "endpoint": ep, "dateParam": dp,
                        "status": res["kind"], "endpointVerified": False,
                        "detail": res}
            if res["kind"] == "OK" and res["rowCount"]:
                hist = call(ep, dp, PROBE_DATES[1], key, budget)
                log.append({"market": name, "endpoint": ep, "dateParam": dp,
                            "date": PROBE_DATES[1], **hist})
                return {"market": name, "endpoint": ep, "dateParam": dp,
                        "status": "OK", "endpointVerified": True,
                        "recent": res, "historical2010": hist}
    return {"market": name, "endpoint": endpoints[-1], "dateParam": None,
            "status": "NO_WORKING_ENDPOINT", "endpointVerified": False}


def main() -> int:
    g = CRED.gate()
    if not g["proceed"]:
        out = {"task": "R31", "phase": "service-probe", "verdict": "BLOCKED",
               "reasonClass": g["reasonClass"], "calls": 0}
        RD.mkdir(parents=True, exist_ok=True)
        (RD / "r31-service-probe-latest.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({k: out[k] for k in ("verdict", "reasonClass")},
                         ensure_ascii=False))
        return 1

    key = CRED.auth_key()
    budget = Budget()
    log = []

    # ── §4 두 서비스 독립 판별 ────────────────────────────────────────────
    #   승인 메일 건수로 "둘 다 됐다" 고 가정하지 않는다 — 각각 실호출로 본다.
    #   단 키 자체가 거부되면(UNAUTHORIZED_KEY) 더 두드릴 이유가 없으므로 즉시 차단한다.
    kospi = probe_market("KOSPI", [KOSPI_ENDPOINT], key, budget, log)
    if kospi["status"] in ("UNAUTHORIZED_KEY", "RATE_LIMITED"):
        kosdaq = {"market": "KOSDAQ",
                  "status": "NOT_PROBED_CIRCUIT_OPEN",
                  "endpointVerified": False,
                  "why": f"KOSPI {kospi['status']} — 전체 회로 차단"}
    else:
        kosdaq = probe_market("KOSDAQ", KOSDAQ_CANDIDATES, key, budget, log)

    # ── §4-B 활용승인 전파 지연 재확인 (미승인 서비스만, 60초 간격 최대 2회) ──
    rechecks = {}
    for m in (kospi, kosdaq):
        if m["status"] != "UNAUTHORIZED_API_CALL":
            continue
        hist = [m["status"]]
        for i in range(2):
            time.sleep(RECHECK_DELAY_SEC)
            r = call(m["endpoint"], m["dateParam"], PROBE_DATES[0], key, budget)
            log.append({"market": m["market"] + "_RECHECK", "endpoint": m["endpoint"],
                        "dateParam": m["dateParam"], "date": PROBE_DATES[0],
                        "attempt": i + 2, **r})
            hist.append(r["kind"])
            if r["kind"] == "OK" and r["rowCount"]:
                m["status"] = "OK"
                m["endpointVerified"] = True
                m["recent"] = r
                break
        rechecks[m["market"]] = {"attempts": len(hist), "kinds": hist,
                                 "intervalSec": RECHECK_DELAY_SEC}

    # 401 문구가 바뀌었으면 어느 쪽인지 대조로 가른다(문구 회귀 방지).
    disc = None
    if "AUTH_OR_APPROVAL_401" in (kospi["status"], kosdaq["status"]):
        disc = discriminate(KOSPI_ENDPOINT, kospi.get("dateParam") or
                            DATE_PARAM_CANDIDATES[0], key, budget, log)
        ours = next((e for e in log
                     if e["market"] == "KOSPI" and e["kind"] == "AUTH_OR_APPROVAL_401"),
                    None)
        ours_body = (ours or {}).get("bodyHead", "")
        same_as_invalid = (disc["invalidKey"]["bodyHead"] == ours_body
                           and disc["invalidKey"]["status"] == (ours or {}).get("status"))
        disc["distinguishable"] = not same_as_invalid
        disc["interpretation"] = (
            "게이트웨이가 무효키와 등록키를 다르게 처리한다 → 키는 인식되고 "
            "이 서비스의 활용승인이 없다"
            if not same_as_invalid else
            "무효키와 등록키 응답이 동일하다 → 응답만으로 키 거부와 활용 미승인을 "
            "구분할 수 없다(단정 금지)")

    statuses = {kospi["status"], kosdaq["status"]}
    if statuses == {"OK"}:
        verdict, reason = "PASS", "KRX_BOTH_SERVICES_ACCESSIBLE"
    elif "UNAUTHORIZED_KEY" in statuses:
        verdict, reason = "BLOCKED", "KRX_AUTH_KEY_REJECTED_AFTER_SERVICE_APPROVAL"
    elif "RATE_LIMITED" in statuses:
        verdict, reason = "WAIT", "KRX_RATE_LIMIT_DURING_APPROVAL_PROBE"
    elif "UNAUTHORIZED_API_CALL" in statuses:
        # 60초 간격 3회 모두 동일 → 승인 전파 대기. 재신청·재등록하지 않는다.
        verdict, reason = "WAIT", "KRX_API_APPROVAL_PROPAGATION_PENDING"
    elif "AUTH_OR_APPROVAL_401" in statuses:
        if disc and disc.get("distinguishable"):
            verdict, reason = "BLOCKED", "KRX_API_SERVICE_APPLICATION_REQUIRED"
        else:
            verdict, reason = "BLOCKED", "KRX_API_401_AUTH_VS_APPROVAL_INDISTINGUISHABLE"
    else:
        verdict, reason = "BLOCKED", "KRX_SERVICE_PROBE_INCONCLUSIVE"

    key = None
    out = {
        "task": "R31", "phase": "service-probe",
        "verdict": verdict, "reasonClass": reason,
        "credential": {"present": True, "sourceClass": "WINDOWS_USER_ENV",
                       "valuePrinted": 0},
        "kospi": kospi, "kosdaq": kosdaq, "discrimination": disc,
        "approvalRechecks": rechecks,
        "budget": {"calls": budget.calls, "retries": budget.retries,
                   "rateLimitEvents": budget.rate_limit_events,
                   "cap": budget.cap},
        "callLog": log,
        "collectionStarted": False,
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r31-service-probe-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"verdict: {verdict}")
    print(f"reason_class: {reason}")
    print(f"KOSPI : {kospi['status']} · endpointVerified={kospi['endpointVerified']}")
    print(f"KOSDAQ: {kosdaq['status']} · endpointVerified={kosdaq['endpointVerified']}")
    print(f"calls={budget.calls} retries={budget.retries} "
          f"rateLimit={budget.rate_limit_events}")
    for e in log:
        print(f"  [{e['market']}] {e['date']} HTTP {e['status']} {e['kind']} "
              f"rows={e['rowCount']} ct={e['contentType']}")
        if e.get("bodyHead"):
            print(f"      body: {e['bodyHead'][:200]}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
