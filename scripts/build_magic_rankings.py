#!/usr/bin/env python3
"""마법공식 순위 검증용 public contract 파생 (Phase MF-RANKING-DATA-1).

signal rankings.json(top100 원값)을 읽어 magicOfficialRankings(cheapTop100/qualityTop100/
combinedTop10)를 파생한다. canonical·apply·기존 3키 publish(magic_publish_public)는 전혀
건드리지 않는 *독립* 파생이며 파일 write 0(dict 반환만). CLI는 stdout preview만.

명칭 매핑(사이트 표시용): valueRank→cheapRank, profitabilityRank→qualityRank,
combinedRank→magicScore, rank→finalRank, capitalBase→investedCapital, signalClosePrice→closePrice.
파생값: netWorkingCapital=currentAssets-currentLiabilities, netDebtApprox=totalLiabilities-cash.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

SCHEMA_VERSION = "magic-official-rankings-v1"
RANKINGS_KEY = "magicOfficialRankings"

# evidenceCompleteness 판정용 필수 원값(모두 존재해야 true).
_REQUIRED = ("ebit", "enterpriseValue", "investedCapital", "currentAssets",
             "currentLiabilities", "propertyPlantAndEquipment", "totalLiabilities",
             "cashAndCashEquivalents")


def _parse_data_source(s):
    """dataSource 문자열('DART 2025 CFS')을 (year:int|None, fsDiv:str|None)로 안전 파싱."""
    if not isinstance(s, str):
        return None, None
    parts = s.split()
    if (len(parts) == 3 and parts[0] == "DART" and parts[1].isdigit()
            and len(parts[1]) == 4 and parts[2] in ("CFS", "OFS")):
        return int(parts[1]), parts[2]
    return None, None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def ranking_item(r: dict, signal_as_of: str) -> dict:
    """signal rankings top100 원소 → 사이트 표시용 ranking item(명칭 매핑·파생, 재계산 0)."""
    ca = _num(r.get("currentAssets"))
    cl = _num(r.get("currentLiabilities"))
    tl = _num(r.get("totalLiabilities"))
    cash = _num(r.get("cashAndCashEquivalents"))
    nwc = (ca - cl) if (ca is not None and cl is not None) else None
    net_debt = (tl - cash) if (tl is not None and cash is not None) else None
    fy, fs_div = _parse_data_source(r.get("dataSource"))
    item = {
        "code": str(r.get("code") or ""),
        "name": str(r.get("name") or r.get("code") or ""),
        "finalRank": r.get("rank"),
        "cheapRank": r.get("valueRank"),
        "qualityRank": r.get("profitabilityRank"),
        "magicScore": r.get("combinedRank"),
        "earningsYield": r.get("earningsYield"),
        "returnOnCapital": r.get("returnOnCapital"),
        "ebit": r.get("EBIT"),
        "enterpriseValue": r.get("enterpriseValue"),
        "investedCapital": r.get("capitalBase"),
        "netWorkingCapital": nwc,
        "netDebtApprox": net_debt,
        "propertyPlantAndEquipment": r.get("propertyPlantAndEquipment"),
        "totalLiabilities": tl,
        "cashAndCashEquivalents": cash,
        "currentAssets": ca,
        "currentLiabilities": cl,
        "marketCap": r.get("marketCap"),
        "closePrice": r.get("signalClosePrice"),
        "priceAsOfDate": signal_as_of,
        "financialStatementYear": fy,
        "dartFsDiv": fs_div,
        "evMethod": r.get("evMethod"),
        "dataSource": r.get("dataSource"),
    }
    item["evidenceCompleteness"] = all(item[k] is not None for k in _REQUIRED)
    return item


def _rank_key(it: dict, key: str) -> float:
    v = it.get(key)
    return float(v) if isinstance(v, (int, float)) else 1e9


def build_magic_rankings(rankings_path, *, cheap_n=100, quality_n=100, combined_n=10) -> dict:
    """signal rankings.json → magicOfficialRankings dict(cheapTop100/qualityTop100/combinedTop10).
    write 0. top100 원값을 재계산 없이 명칭 매핑·정렬만 한다."""
    doc = json.loads(Path(rankings_path).read_text(encoding="utf-8"))
    top100 = doc.get("top100") or []
    signal_as_of = str(doc.get("signalAsOfDate") or "")
    items = [ranking_item(r, signal_as_of) for r in top100]
    ev_method = next((it["evMethod"] for it in items if it.get("evMethod")), None)

    cheap = sorted(items, key=lambda it: (_rank_key(it, "cheapRank"), it["code"]))[:cheap_n]
    quality = sorted(items, key=lambda it: (_rank_key(it, "qualityRank"), it["code"]))[:quality_n]
    combined = sorted(items, key=lambda it: (_rank_key(it, "magicScore"),
                                             _rank_key(it, "cheapRank"), it["code"]))[:combined_n]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "dataDate": signal_as_of,
        "signalAsOfDate": signal_as_of,
        "formulaVersion": doc.get("formulaVersion"),
        "formulaMode": doc.get("formulaMode"),
        "evMethod": ev_method,
        "rankingCount": doc.get("rankingCount"),
        "cheapTop100": cheap,
        "qualityTop100": quality,
        "combinedTop10": combined,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="signal rankings.json → magicOfficialRankings 파생(read-only, 파일 write 0)")
    ap.add_argument("--rankings", required=True, help="signal rankings.json 경로")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    out = build_magic_rankings(args.rankings)
    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(f"cheapTop100={len(out['cheapTop100'])} qualityTop100={len(out['qualityTop100'])} "
              f"combinedTop10={len(out['combinedTop10'])} evMethod={out['evMethod']} "
              f"dataDate={out['dataDate']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
