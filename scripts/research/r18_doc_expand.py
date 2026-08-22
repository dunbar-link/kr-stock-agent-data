#!/usr/bin/env python3
"""R18 원문 수집 2차 — 대상 이벤트의 **창 안 모든 유상증자 공시** 확보.

WABABA-DART-LEGACY-RIGHTS-DOCUMENT-RECOVERY-R18

왜 필요한가(1차 수집 후 실측으로 드러난 이유):
  1차는 R17 이 고른 접수번호 1건씩만 받았다. 그런데 공시된 신주수는 **계획치**라
  실권·부분청약 때문에 실제 발행량과 자주 갈린다(실측: 관측 주식수 점프 대비
  상대오차 p75 = 0.56). 한 건만 보면 "이 공시가 이 주식수 변동의 원인인가"를
  확정할 수 없다.

  창 안의 **모든** 유상증자 공시를 보면 두 가지가 가능해진다:
    (a) 정정공시 chain 을 원문에서 복원한다(§9).
    (b) 창 안 공시가 **전부 제3자배정·일반공모**면, 어느 것이 원인이든 기존
        주주는 신주를 받지 않는다 → 미조정이 정답임을 비율과 무관하게 확정.

범위는 넓히지 않는다(§2): 대상 **이벤트**는 R18 targets precommit 그대로이고,
같은 이벤트에 대한 근거 문서만 늘린다. 새 종목·새 사건을 추가하지 않는다.

안전: 무료 공개 DART · 기존 키 재사용 · env 변경 0 · 캐시만 write.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r18_doc_collect import CACHE, PARSER_VERSION, SLEEP, fetch_doc, _key  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
WINDOW = (-6, 1)          # R17 매칭 창과 동일. 새 값을 발명하지 않는다.


def _mon(s):
    s = str(s).replace("-", "")
    return int(s[:4]) * 12 + int(s[4:6]) - 1


def candidates():
    tg = json.loads((RD / "r18-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    nm = json.loads((RD / "r17-dart-rights-normalized-latest.json")
                    .read_text(encoding="utf-8"))
    by_ticker = {}
    for f in nm["filings"]:
        by_ticker.setdefault(f["ticker"], []).append(f)

    out = {}
    for t in tg:
        em = _mon(t["date"])
        for f in by_ticker.get(t["ticker"], []):
            fd = f.get("filing_date")
            if not fd or len(str(fd)) < 6:
                continue
            if f["kind"] not in ("RIGHTS", "BOTH"):
                continue
            if WINDOW[0] <= _mon(fd) - em <= WINDOW[1]:
                out.setdefault(f["rcept_no"], {
                    "rcept_no": f["rcept_no"], "ticker": t["ticker"],
                    "corpEventDate": t["date"], "filingDate": fd,
                    "reportName": f.get("report_nm")})
    return out


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    cand = candidates()
    CACHE.mkdir(parents=True, exist_ok=True)
    meta_path = RD / "r18-document-download-status-latest.json"
    meta, base = {}, {}
    if meta_path.exists():
        base = json.loads(meta_path.read_text(encoding="utf-8"))
        meta = {m["rcept_no"]: m for m in base["documents"]}

    new = hit = fail = 0
    todo = [rn for rn in sorted(cand) if not (CACHE / f"{rn}.xml").exists()]
    print(f"[r18x] 창 내 후보 {len(cand)} · 미확보 {len(todo)}",
          file=sys.stderr, flush=True)
    for i, rn in enumerate(todo, 1):
        c = cand[rn]
        txt, m = fetch_doc(rn)
        rec = {"rcept_no": rn, "ticker": c["ticker"],
               "corpEventDate": c["corpEventDate"], "filingDate": c["filingDate"],
               "reportName": c["reportName"], "pass": "WINDOW_EXPANSION",
               "sourceEndpoint": "opendart document.xml",
               "parserVersion": PARSER_VERSION, **m}
        if txt is not None:
            (CACHE / f"{rn}.xml").write_text(txt, encoding="utf-8")
            rec["cachedFile"] = f"{rn}.xml"
            rec["storedEncoding"] = "utf-8"
            rec["originalEncoding"] = m.get("encoding")
            new += 1
        else:
            fail += 1
        meta[rn] = rec
        if i % 200 == 0:
            print(f"[r18x] {i}/{len(todo)} new={new} fail={fail}",
                  file=sys.stderr, flush=True)
        time.sleep(SLEEP)
    hit = len(cand) - len(todo)

    docs = sorted(meta.values(), key=lambda x: x["rcept_no"])
    ok = sum(1 for d in docs if d["status"] == "OK")
    mix = {}
    for d in docs:
        mix[d["status"]] = mix.get(d["status"], 0) + 1
    meta_path.write_text(json.dumps({
        **{k: v for k, v in base.items() if k != "documents"},
        "task": "R18", "parserVersion": PARSER_VERSION,
        "windowExpansion": {
            "window": f"{WINDOW[0]}~+{WINDOW[1]} 개월 (R17 매칭 창과 동일)",
            "candidates": len(cand), "downloadedThisRun": new,
            "alreadyCached": hit, "failed": fail,
            "why": ("공시 신주수는 계획치라 실제 발행량과 갈린다. 창 안 모든 공시를 "
                    "봐야 정정 chain 복원과 '전부 제3자배정이면 미조정이 정답' "
                    "판정이 가능하다."),
            "scopeNote": "대상 이벤트는 그대로. 같은 이벤트의 근거 문서만 늘렸다.",
        },
        "retrieved": ok, "failed": len(docs) - ok, "statusMix": mix,
        "totalDocuments": len(docs),
        "documents": docs,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"candidates": len(cand), "new": new, "cached": hit,
                      "fail": fail, "totalDocs": len(docs), "ok": ok,
                      "statusMix": mix}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
