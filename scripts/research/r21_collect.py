#!/usr/bin/env python3
"""R21 직접공시 수집기 — R19 수집 로직 재사용(§26 신규 framework 금지).

WABABA-LOW-CONFIDENCE-DIRECT-ENTITLEMENT-RECOVERY-R21

대상은 R21 targets precommit 의 96건뿐이다(§2).
R19 가 342건에서 검증한 `r19_collect.fetch_ticker` 를 그대로 쓴다 — 전 공시유형을
보되 자본행위 관련 제목만 보관한다. 창도 R19 와 동일(-6 ~ +3 개월): 결정형 공시는
사건에 선행하고 행사형은 후행하므로 양쪽을 덮는다.

캐시는 R19 캐시(`_cache/dart-capital-actions`)를 **공유**한다. 이미 받은 종목은
재호출하지 않는다.

안전: 무료 공개 DART · 기존 DART_API_KEY 재사용 · env 변경 0 · 캐시만 write.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from r19_collect import CACHE, SLEEP, fetch_ticker, window, _key  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"


def main() -> int:
    if not _key:
        print(json.dumps({"verdict": "BLOCKED_DART_ACCESS"}, ensure_ascii=False))
        return 2
    tg = json.loads((RD / "r21-targets-precommit-latest.json")
                    .read_text(encoding="utf-8"))["targets"]
    CACHE.mkdir(parents=True, exist_ok=True)

    jobs = {}
    for t in tg:
        if not t["corpCode"]:
            continue
        b, e = window(t["date"])
        j = jobs.setdefault(t["ticker"], {"corpCode": t["corpCode"],
                                          "bgn": b, "end": e, "events": []})
        j["bgn"] = min(j["bgn"], b)
        j["end"] = max(j["end"], e)
        j["events"].append(t["date"])

    new = hit = fail = 0
    for i, (tk, j) in enumerate(sorted(jobs.items()), 1):
        p = CACHE / f"{tk}.json"
        if p.exists():
            # 이미 받은 종목이라도 창이 더 넓으면 다시 받아야 한다.
            old = json.loads(p.read_text(encoding="utf-8"))
            if old.get("bgn", "9") <= j["bgn"] and old.get("end", "0") >= j["end"]:
                hit += 1
                continue
        rows, netfail = fetch_ticker(j["corpCode"], j["bgn"], j["end"])
        if netfail and not rows:
            fail += 1
        else:
            p.write_text(json.dumps({"ticker": tk, "corpCode": j["corpCode"],
                                     "bgn": j["bgn"], "end": j["end"],
                                     "events": j["events"],
                                     "filings": rows}, ensure_ascii=False),
                         encoding="utf-8")
            new += 1
        if i % 25 == 0:
            print(f"[r21] {i}/{len(jobs)} new={new} cached={hit} fail={fail}",
                  file=sys.stderr, flush=True)
        time.sleep(SLEEP)

    covered = [t for t in tg if (CACHE / f"{t['ticker']}.json").exists()]
    total = 0
    for tk in {t["ticker"] for t in covered}:
        total += len(json.loads((CACHE / f"{tk}.json")
                                .read_text(encoding="utf-8"))["filings"])
    (RD / "r21-direct-source-events-latest.json").write_text(json.dumps({
        "task": "R21", "endpoint": "opendart list.json (전 공시유형)",
        "collectorReused": "r19_collect.fetch_ticker (§26 신규 framework 금지)",
        "windowMonths": "-6 ~ +3 (R19 와 동일)",
        "targetEvents": len(tg), "targetTickers": len(jobs),
        "eventsCovered": len(covered),
        "filingsAvailable": total,
        "newThisRun": new, "cacheHits": hit, "hardFailures": fail,
        "cacheShared": "R19 캐시 공유 — 중복 다운로드를 피한다.",
        "cacheDir": str(CACHE.relative_to(ROOT)),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"tickers": len(jobs), "new": new, "cached": hit,
                      "fail": fail, "eventsCovered": len(covered),
                      "filings": total}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
