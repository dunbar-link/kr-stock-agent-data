#!/usr/bin/env python3
"""KRX 수집 응답 검증 + 제한 재시도 + redacted 진단 (WABABA-KRX-FUNDAMENTAL-RECOVERY-R1).

배경(실제 관측된 실패):
    Error occurred in get_market_fundamental_by_ticker:
    Expecting value: line 1 column 1 (char 0)          ← pykrx 내부 JSON 파싱 실패(비-JSON/빈 응답)
    [WARN] 펀더멘털 조회 실패 (KOSPI):
    None of [Index(['BPS','PER','PBR','EPS','DIV', ...])] are in the columns

  기존 build_market_snapshot.safe_get_* 는 어떤 예외든 삼키고 **빈 DataFrame** 을 돌려줘
  파이프라인이 그대로 진행됐다. 즉 일시적 upstream 장애가 "조용한 데이터 결측"으로 바뀌어
  ranking·signal·apply 까지 흘렀다. 이 모듈은 그 fail-open 을 fail-closed 로 바꾼다.

설계 원칙:
  - 외부 라이브러리(site-packages/pykrx)는 수정하지 않는다. 호출부 wrapper 로만 방어한다.
  - 재시도는 **일시적 장애일 때만**, 최초 1회 + 재시도 최대 2회 = 총 3회. 무한 재시도 없음.
  - schema 불일치·columns 지속 누락·숫자 변환 불가·전체 붕괴는 재시도로 숨기지 않고 즉시 BLOCKED.
  - 로그인 ID/PW/쿠키/세션/Authorization 값은 절대 기록하지 않는다(redacted 진단만).
  - 과거 값·0·전일 데이터로 위장하지 않는다. 정상 데이터가 없으면 예외를 올린다.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

MAX_ATTEMPTS = 3          # 최초 1 + 재시도 2
BACKOFF_SEC = (0.8, 2.0)  # 재시도 전 대기(짧게). 무한 재시도 금지.

# 일시적 장애(재시도 허용) 지문 — 응답이 비었거나 JSON 이 아니거나 429/5xx/네트워크
_RETRYABLE_PATTERNS = (
    r"expecting value",            # json.JSONDecodeError (빈/비-JSON 응답)
    r"extra data",                 # 잘린 JSON
    r"unterminated string",
    r"jsondecodeerror",
    r"timed out", r"timeout",
    r"connection (reset|aborted|refused|error)",
    r"temporarily unavailable",
    r"remote end closed",
    r"max retries exceeded",
    r"\b(429|500|502|503|504)\b",
    r"empty response",
)
# 재시도로 숨기면 안 되는 구조적 결함 — 즉시 BLOCKED
_FATAL_PATTERNS = (
    r"none of \[index",            # 필수 columns 부재(schema 변경)
    r"could not convert",
    r"invalid literal",
)

# 로그·보고서에 남기면 안 되는 값 패턴(방어적 redaction)
_SECRET_KEYS = re.compile(
    r"(?i)\b(krx_id|krx_pw|password|passwd|pwd|login_id|loginid|jsessionid|cookie|"
    r"set-cookie|authorization|token|api[_-]?key|secret)\b")


class KrxDataInvalid(Exception):
    """검증 실패로 이 거래일 데이터를 사용할 수 없음(fail-closed). 재시도 소진 포함."""

    def __init__(self, code: str, message: str, evidence: dict | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.evidence = evidence or {}


def redact(text: Any, limit: int = 160) -> str:
    """민감정보 제거 fingerprint. 응답 원문·인증정보를 그대로 남기지 않는다."""
    s = "" if text is None else str(text)
    # Authorization: Bearer <token> / Basic <blob> — 스킴 뒤 값까지 마스킹
    s = re.sub(r"(?i)\b(authorization)\s*[=:]\s*(bearer|basic|token)\s+\S+",
               r"\1=<REDACTED>", s)
    # key=value / key: value 형태의 비밀값 마스킹(값에 스킴이 붙은 경우 포함)
    s = re.sub(r"(?i)\b(krx_id|krx_pw|password|passwd|pwd|login_id|jsessionid|cookie|"
               r"authorization|token|api[_-]?key|secret)\s*[=:]\s*(?:bearer|basic)?\s*[^\s;,&]+",
               r"\1=<REDACTED>", s)
    # 쿠키 문자열 통째 마스킹
    s = re.sub(r"(?i)\bJSESSIONID=[^\s;]+", "JSESSIONID=<REDACTED>", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit]


def classify_error(err: BaseException) -> str:
    """예외를 재시도 가능/구조적 결함으로 분류. 증거 없이 원인을 단정하지 않는다."""
    msg = str(err).lower()
    for p in _FATAL_PATTERNS:
        if re.search(p, msg):
            return "FATAL_SCHEMA"
    for p in _RETRYABLE_PATTERNS:
        if re.search(p, msg):
            return "RETRYABLE_TRANSIENT"
    return "FATAL_UNKNOWN"


@dataclass
class FetchSpec:
    """한 번의 수집 호출에 대한 검증 계약."""
    kind: str                              # 'fundamental' | 'ohlcv' | 'marketcap' | 'tickers'
    market: str                            # 'KOSPI' | 'KOSDAQ'
    required_columns: Sequence[str] = ()   # 이게 없으면 계산 불가(구조 필수)
    optional_columns: Sequence[str] = ()   # 없어도 계산 가능(과도 차단 금지)
    min_rows: int = 1
    numeric_columns: Sequence[str] = ()    # 숫자 변환 가능해야 하는 열
    # 해당 열이 전 종목에서 전부 NaN/0 이면 붕괴로 본다(구조는 있는데 값이 죽은 경우)
    collapse_guard_columns: Sequence[str] = ()
    required: bool = True                  # False 면 실패해도 BLOCKED 아님(선택 데이터)


@dataclass
class FetchResult:
    frame: Any
    spec: FetchSpec
    attempts: int = 1
    verdict: str = "PASS"
    evidence: dict = field(default_factory=dict)


def _shape_of(frame) -> tuple:
    try:
        return tuple(frame.shape)
    except Exception:  # noqa: BLE001
        return ()


def _columns_of(frame) -> list:
    try:
        return [str(c) for c in frame.columns]
    except Exception:  # noqa: BLE001
        return []


def validate_frame(frame, spec: FetchSpec) -> tuple[bool, str, str]:
    """(ok, code, detail). 구조 검증만 한다 — 개별 종목의 정상 결측(예: 적자기업 PER)은 허용."""
    import pandas as pd

    if frame is None:
        return False, "EMPTY_RESPONSE", "반환값이 None"
    if not isinstance(frame, pd.DataFrame):
        return False, "NOT_A_DATAFRAME", f"DataFrame 아님({type(frame).__name__})"
    if frame.empty:
        return False, "EMPTY_DATAFRAME", "빈 DataFrame"

    cols = set(_columns_of(frame))
    missing = [c for c in spec.required_columns if c not in cols]
    if missing:
        return False, "REQUIRED_COLUMNS_MISSING", f"필수 columns 누락 {missing} (실제 {sorted(cols)[:10]})"

    rows = int(frame.shape[0])
    if rows < spec.min_rows:
        return False, "ROW_COUNT_TOO_LOW", f"행 수 {rows} < 최소 {spec.min_rows}"

    for c in spec.numeric_columns:
        if c not in cols:
            continue
        try:
            pd.to_numeric(frame[c], errors="coerce")
        except Exception as e:  # noqa: BLE001
            return False, "NUMERIC_CONVERSION_FAILED", f"{c} 숫자 변환 불가: {redact(e, 80)}"

    for c in spec.collapse_guard_columns:
        if c not in cols:
            continue
        series = pd.to_numeric(frame[c], errors="coerce")
        if series.notna().sum() == 0:
            return False, "ALL_VALUES_NAN", f"{c} 전 종목 NaN(값 붕괴)"
        if (series.fillna(0) == 0).all():
            return False, "ALL_VALUES_ZERO", f"{c} 전 종목 0(값 붕괴)"

    return True, "", ""


def fetch_validated(fetcher: Callable[[], Any], spec: FetchSpec, *,
                    sleep: Callable[[float], None] = time.sleep,
                    max_attempts: int = MAX_ATTEMPTS) -> FetchResult:
    """검증 통과한 DataFrame 만 돌려준다. 실패하면 KrxDataInvalid 를 올린다(빈 프레임 반환 금지).

    재시도: 일시적 장애로 분류될 때만. 구조적 결함은 즉시 중단(재시도로 숨기지 않음).
    """
    attempts = 0
    trail: list[dict] = []
    last_code, last_detail = "UNKNOWN", ""

    while attempts < max_attempts:
        attempts += 1
        step: dict = {"attempt": attempts}
        try:
            frame = fetcher()
        except BaseException as err:  # noqa: BLE001  (pykrx 는 광범위한 예외를 올린다)
            kind = classify_error(err)
            step.update({"outcome": "EXCEPTION", "errorClass": type(err).__name__,
                         "errorKind": kind, "errorFingerprint": redact(err)})
            trail.append(step)
            last_code, last_detail = ("UPSTREAM_EXCEPTION", redact(err))
            if kind == "RETRYABLE_TRANSIENT" and attempts < max_attempts:
                sleep(BACKOFF_SEC[min(attempts - 1, len(BACKOFF_SEC) - 1)])
                continue
            if kind == "FATAL_SCHEMA":
                last_code = "SCHEMA_MISMATCH"
            break

        ok, code, detail = validate_frame(frame, spec)
        step.update({"outcome": "OK" if ok else "INVALID", "shape": _shape_of(frame),
                     "columns": _columns_of(frame)[:12]})
        if not ok:
            step["invalidCode"] = code
            step["invalidDetail"] = redact(detail)
        trail.append(step)

        if ok:
            return FetchResult(frame=frame, spec=spec, attempts=attempts, verdict="PASS",
                               evidence={"kind": spec.kind, "market": spec.market,
                                         "attempts": attempts, "trail": trail,
                                         "shape": _shape_of(frame),
                                         "columns": _columns_of(frame)[:12]})

        last_code, last_detail = code, detail
        # 빈 응답/빈 프레임은 일시적일 수 있어 재시도. 구조·값 결함은 재시도 금지.
        if code in ("EMPTY_RESPONSE", "EMPTY_DATAFRAME", "ROW_COUNT_TOO_LOW") and attempts < max_attempts:
            sleep(BACKOFF_SEC[min(attempts - 1, len(BACKOFF_SEC) - 1)])
            continue
        break

    raise KrxDataInvalid(
        last_code, f"[{spec.kind}/{spec.market}] {last_detail} (시도 {attempts}회)",
        {"kind": spec.kind, "market": spec.market, "attempts": attempts,
         "trail": trail, "required": spec.required},
    )


def assert_no_secret(text: Any) -> None:
    """진단 문자열에 비밀값 키가 남아있지 않은지 확인(테스트·기록 직전 방어)."""
    s = str(text)
    if _SECRET_KEYS.search(s) and "<REDACTED>" not in s:
        raise KrxDataInvalid("SECRET_LEAK_GUARD", "진단 문자열에 비밀값 키가 포함됨(기록 차단)")
