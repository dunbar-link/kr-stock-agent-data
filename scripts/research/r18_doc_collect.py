#!/usr/bin/env python3
"""R18 DART 공시원문 수집기 — document.xml.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

대상은 R18 targets precommit 에 고정된 1,081건뿐이다(§2 범위 확대 금지).

원문은 ZIP(EUC-KR XML) 로 온다. **원문을 변형해 provenance 를 잃지 않는다**(§4):
디코딩만 하고 내용은 그대로 캐시한다. sha256 과 메타데이터를 함께 남긴다.

안전: 무료 공개 DART 만 · 기존 DART_API_KEY 재사용 · env/token 변경 0 ·
      파일 write 는 캐시와 보고서만 · 실주문 0 · production write 0.
재현: 접수번호 단위 checkpoint. 캐시 적중분은 재호출하지 않는다.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from krx_fetch_guard import BACKOFF_SEC, MAX_ATTEMPTS  # noqa: E402  기존 규약 재사용

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
CACHE = ROOT / "_cache" / "dart-documents"
API = "https://opendart.fss.or.kr/api/document.xml"
SLEEP = float(os.environ.get("DART_REQUEST_SLEEP_SEC", "0.12"))
PARSER_VERSION = "r18-doc-1"
ENCODINGS = ("euc-kr", "cp949", "utf-8")

_key = os.environ.get("DART_API_KEY", "").strip()


def fetch_doc(rcept_no: str):
    """(text, meta) 반환. 실패는 예외 대신 status 로 알린다."""
    url = f"{API}?{urllib.parse.urlencode({'crtfc_key': _key, 'rcept_no': rcept_no})}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=45) as r:
                data = r.read()
            break
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt >= MAX_ATTEMPTS:
                return None, {"status": "NETFAIL", "error": type(e).__name__}
            time.sleep(BACKOFF_SEC[min(attempt - 1, len(BACKOFF_SEC) - 1)])
    else:
        return None, {"status": "NETFAIL", "error": "exhausted"}

    if not data:
        return None, {"status": "EMPTY"}
    if data[:2] != b"PK":
        # DART 는 오류를 JSON/XML 본문으로 준다. 원문이 아니므로 그대로 기록한다.
        head = data[:200].decode("utf-8", "replace")
        return None, {"status": "NOT_ZIP", "head": head}
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        names = z.namelist()
        raw = z.read(names[0])
    except (zipfile.BadZipFile, IndexError) as e:
        return None, {"status": "BAD_ZIP", "error": type(e).__name__}

    for enc in ENCODINGS:
        try:
            txt = raw.decode(enc)
            break
        except UnicodeDecodeError:
            txt = None
    if txt is None:
        return None, {"status": "DECODE_FAIL",
                      "sha256": hashlib.sha256(raw).hexdigest()}
    return txt, {"status": "OK", "encoding": enc, "entries": names,
                 "bytes": len(raw),
                 "sha256": hashlib.sha256(raw).hexdigest()}


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS",
                          "reason": "DART_API_KEY 환경변수 없음"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r18-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    # 접수번호는 여러 사건이 공유할 수 있다. 문서는 한 번만 받는다.
    by_rcept = {}
    for t in tg:
        if t.get("rcept_no"):
            by_rcept.setdefault(t["rcept_no"], t)

    CACHE.mkdir(parents=True, exist_ok=True)
    meta_path = RD / "r18-document-download-status-latest.json"
    meta = {}
    if meta_path.exists():
        meta = {m["rcept_no"]: m for m in
                json.loads(meta_path.read_text(encoding="utf-8"))["documents"]}

    new = hit = fail = 0
    for i, (rn, t) in enumerate(sorted(by_rcept.items()), 1):
        f = CACHE / f"{rn}.xml"
        if f.exists() and meta.get(rn, {}).get("status") == "OK":
            hit += 1
            continue
        txt, m = fetch_doc(rn)
        rec = {"rcept_no": rn, "ticker": t["ticker"], "corpEventDate": t["date"],
               "filingDate": t.get("filingDate"), "reportName": t.get("reportName"),
               "sourceEndpoint": "opendart document.xml",
               "parserVersion": PARSER_VERSION, **m}
        if txt is not None:
            f.write_text(txt, encoding="utf-8")   # 저장은 UTF-8, 내용은 원문 그대로
            rec["cachedFile"] = f.name
            rec["storedEncoding"] = "utf-8"
            rec["originalEncoding"] = m.get("encoding")
            new += 1
        else:
            fail += 1
        meta[rn] = rec
        if i % 100 == 0:
            print(f"[r18] {i}/{len(by_rcept)} new={new} cached={hit} fail={fail}",
                  file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    docs = sorted(meta.values(), key=lambda x: x["rcept_no"])
    ok = sum(1 for d in docs if d["status"] == "OK")
    status_mix = {}
    for d in docs:
        status_mix[d["status"]] = status_mix.get(d["status"], 0) + 1
    meta_path.write_text(json.dumps({
        "task": "R18", "parserVersion": PARSER_VERSION,
        "targetEvents": len(tg), "uniqueReceipts": len(by_rcept),
        "retrieved": ok, "failed": len(docs) - ok,
        "newThisRun": new, "cacheHits": hit,
        "statusMix": status_mix,
        "cacheDir": str(CACHE.relative_to(ROOT)),
        "provenanceNote": ("원문을 변형하지 않는다. ZIP 해제 + 문자 디코딩만 하고 "
                           "내용은 그대로 캐시한다. sha256 은 **원본 바이트** 기준."),
        "documents": docs,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"receipts": len(by_rcept), "ok": ok,
                      "fail": len(docs) - ok, "new": new, "cached": hit,
                      "statusMix": status_mix}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
