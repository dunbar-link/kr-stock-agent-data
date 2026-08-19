#!/usr/bin/env python3
"""공식 KOSPI/KOSDAQ 지수 시계열 1회 수집 → 캐시 (WABABA-BENCHMARK-SURVIVORSHIP-FIX-R5).

자체 동일가중 benchmark 와 **공식 지수**는 다른 것이다(§2). 혼동하지 않도록 별도 파일로 받는다.
공식 지수는 KRX 산출 시가총액가중이며 상장폐지 편입/제외를 지수사업자가 처리한다.

호출: 지수 2종 × 1콜 = 2콜. 선차단검사 후에만 수행하고, 캐시가 있으면 네트워크 0.
안전: read-only. canonical 미접근. 실주문 0 · 브로커 0.

사용: python scripts/research/fetch_official_index.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "_cache" / "pit-snapshots" / "_official-index.json"
INDEXES = {"1001": "KOSPI", "2001": "KOSDAQ"}


def main() -> int:
    if OUT.exists():
        try:
            d = json.loads(OUT.read_text(encoding="utf-8"))
            print(json.dumps({"cached": True, "series": {k: len(v) for k, v in d.items()}},
                             ensure_ascii=False))
            return 0
        except (OSError, ValueError):
            pass

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import pit_acquire_guard as G  # noqa: PLC0415
    st = G.block_status(G.default_fetch)
    if st["state"] != "CLEAR":
        print(json.dumps({"blocked": st}, ensure_ascii=False))
        return 3

    from pykrx import stock  # noqa: PLC0415
    out = {}
    for code, name in INDEXES.items():
        df = stock.get_index_ohlcv_by_date("19900101", "20261231", code)
        out[name] = {str(d)[:10]: float(v) for d, v in zip(df.index, df["종가"])}
        print(f"[idx] {name} {len(out[name])} rows", file=sys.stderr)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"cached": False, "series": {k: len(v) for k, v in out.items()},
                      "out": str(OUT), "krxCalls": 2}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
