#!/usr/bin/env python3
"""거래 당일 17:00 publish 직전, REPO1 public 산출물을 현재 canonical 기준으로 재생성한다.

왜 필요한가:
  Wababa Auto Daily 는 08:45 에 public 산출물을 만든다. 그런데 canonical 자동반영은 같은 날
  16:25 에 일어나므로, 08:45 산출물에는 *그 거래일 장부가 들어있지 않다*. 같은 거래일 저녁
  17:00 에 홈페이지를 반영하려면 publish 직전에 canonical 기준으로 한 번 더 파생해야 한다.

무엇을 재사용하는가:
  build_recommendation_history.write_public_recommendation_history() — 08:45 이 쓰는 것과
  **동일한 공식 함수**. 매매 파이프라인은 실행하지 않고(주문 로직 0), REPO2 내부 원장에
  canonical 파생 magicOfficial 3키를 병합해 sanitize 사본만 다시 쓴다.
  → 새 매핑/새 sanitize 로직 0, 수기 JSON 0, 임의 sequence 증가 0.

쓰는 파일: REPO1 public/data/recommendation-history.json 만.
canonical·REPO2 원장·snapshot 은 절대 쓰지 않는다(read-only).

exit 0 = 정상(변경 여부는 stdout JSON 의 changed 로 판단), 2 = 실패
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

INTERNAL_HISTORY = ROOT / "recommendation-history.json"
REPO1_PUBLIC = Path("C:/work/kr-stock-agent/public/data/recommendation-history.json")


def _sha(p: Path):
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()
    except OSError:
        return None


def run(*, internal_path: Path = INTERNAL_HISTORY, public_path: Path = REPO1_PUBLIC) -> dict:
    import build_recommendation_history as B

    if not internal_path.exists():
        return {"status": "BLOCKED", "reason": f"REPO2 내부 원장 없음: {internal_path}",
                "changed": False, "canonicalChanged": False, "filesWritten": 0}
    if not public_path.parent.exists():
        return {"status": "BLOCKED", "reason": f"REPO1 public 디렉터리 없음: {public_path.parent}",
                "changed": False, "canonicalChanged": False, "filesWritten": 0}

    before = _sha(public_path)
    data = json.loads(internal_path.read_text(encoding="utf-8"))

    # 공식 경로 그대로 호출. 내부에서 build_magic_public_summary + apply_magic_official_public
    # (canonical 파생) + sanitize allowlist 를 적용해 REPO1 public 에만 쓴다.
    prev_public_path = B.PUBLIC_DATA_PATH
    try:
        B.PUBLIC_DATA_PATH = public_path
        B.write_public_recommendation_history(data)
    finally:
        B.PUBLIC_DATA_PATH = prev_public_path

    after = _sha(public_path)
    doc = json.loads(public_path.read_text(encoding="utf-8"))
    s = doc.get("magicOfficialSummary") or {}
    return {
        "status": "OK",
        "changed": (before != after),
        "beforeSha256": (before or "")[:16],
        "afterSha256": (after or "")[:16],
        "ledgerBasisDate": s.get("dataDate"),
        "officialSequence": s.get("officialSequence"),
        "openItemLotCount": s.get("openItemLotCount"),
        "uniqueHoldings": len((doc.get("magicOfficialPortfolio") or {}).get("holdings") or []),
        "totalAsset": s.get("totalAsset"),
        "availableCash": s.get("officialAvailableCash"),
        "priceAsOf": doc.get("priceAsOf"),
        "canonicalChanged": False,
        "realOrderCount": 0,
        "brokerApiCallCount": 0,
        "filesWritten": 1,
        "target": str(public_path),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="canonical 기준 REPO1 public 산출물 재생성(공식 함수 재사용)")
    ap.add_argument("--internal-path", default=str(INTERNAL_HISTORY))
    ap.add_argument("--public-path", default=str(REPO1_PUBLIC))
    args = ap.parse_args(argv)
    r = run(internal_path=Path(args.internal_path), public_path=Path(args.public_path))
    print(json.dumps(r, ensure_ascii=False), flush=True)
    return 0 if r["status"] == "OK" else 2


if __name__ == "__main__":
    sys.exit(main())
