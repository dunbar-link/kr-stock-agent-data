"""extract_official_eligible — 공식 마법공식 eligible 집합을 그대로 재사용해 추출 (Phase MF-TTM-OFFICIAL-ELIGIBLE-RECONCILE-AND-BATCH01-PILOT).

eligibility 를 재구현하지 않는다. build_magic_formula_fund.calculate_book_faithful_magic_ranking()
(운영 랭킹과 동일 canonical 함수)을 read-only 로 호출해 final(eligible 정렬 배열)의 종목코드를 얻는다.
DART API 호출 없음(연간 캐시 read only). financial-universe-real.json 수정 안 함.

출력: _cache/ttm-poc-output/official-eligible.json (TEMP, gitignore)
실행:  python scripts/poc/extract_official_eligible.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]   # .../scripts
sys.path.insert(0, str(SCRIPTS))
import build_magic_formula_fund as F  # 운영 canonical 랭킹 모듈 재사용

ROOT = SCRIPTS.parent
OUT = ROOT / "_cache" / "ttm-poc-output" / "official-eligible.json"


def main() -> int:
    rows, meta = F.load_universe()
    base_date = meta.get("baseDate", "unknown")
    bl_doc = F.read_json(F.BLACKLIST_PATH, []) or []
    blacklist = set(str(x).strip() for x in bl_doc) if isinstance(bl_doc, list) else set()

    # 운영과 동일한 book_faithful canonical 함수 호출(재구현 아님)
    final, excluded, cov = F.calculate_book_faithful_magic_ranking(rows, blacklist)

    # fsDiv/marketCap 는 universe row 에서 보강(final 에는 fsDiv 없음)
    by_code = {str(r.get("symbol") or r.get("code") or ""): r for r in rows}
    codes = []
    for s in final:
        code = s["code"]
        r = by_code.get(code, {})
        codes.append({
            "code": code,
            "name": s.get("name"),
            "industry": s.get("industryName"),
            "marketCap": s.get("marketCap"),
            "fsDiv": r.get("dartFsDiv"),
            "rank": s.get("rank"),
            "combinedRank": s.get("combinedRank"),
        })

    # 대상 집합 hash(코드 정렬 후) — 결정론 검증용
    import hashlib
    code_set_sorted = sorted(c["code"] for c in codes)
    set_hash = hashlib.sha256("\n".join(code_set_sorted).encode("utf-8")).hexdigest()[:16]

    out = {
        "source": "build_magic_formula_fund.calculate_book_faithful_magic_ranking (canonical, read-only)",
        "universeBaseDate": base_date,
        "eligibleCount": len(codes),
        "excludedCounts": excluded,
        "dartCoverage": cov.get("dartCoverage"),
        "codeSetHash": set_hash,
        "orderBy": "combinedRank (공식 마법공식 순위)",
        "codes": codes,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"eligibleCount: {len(codes)}")
    print(f"excluded: {json.dumps(excluded, ensure_ascii=False)}")
    print(f"dartCoverage: {cov.get('dartCoverage')}")
    print(f"codeSetHash: {set_hash}")
    print(f"written: {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
