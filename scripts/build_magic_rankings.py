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

# rankingScope: 신규 signal은 전체 유효 universe 기준(global), 구형 signal은 combined 후보 subset.
SCOPE_GLOBAL = "global-eligible-universe"
SCOPE_SUBSET = "combined-subset"

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


def _global_list(rows, signal_as_of, kind: str, n: int) -> list:
    """전체 universe 기준 원천(valueTop100/profitabilityTop100)을 표시용으로 변환.
    - 표시 순위(cheapRank/qualityRank)는 목록 위치로 1..N *연속 강제*(원천에 gap이 있어도 보정).
    - 원래 universe 순위는 universeCheapRank/universeQualityRank로 별도 보존(표시 rank와 혼합 금지).
    kind='cheap'이면 valueRank 원천, 'quality'이면 profitabilityRank 원천."""
    disp_key = "cheapRank" if kind == "cheap" else "qualityRank"
    uni_key = "universeCheapRank" if kind == "cheap" else "universeQualityRank"
    out = []
    for i, r in enumerate(rows[:n], 1):
        it = ranking_item(r, signal_as_of)
        it[uni_key] = it.get(disp_key)   # 원천 universe 순위 보존
        it[disp_key] = i                 # 표시 순위는 1..N 연속 강제
        out.append(it)
    return out


def build_magic_rankings(rankings_path, *, cheap_n=100, quality_n=100, combined_n=10) -> dict:
    """signal rankings.json → magicOfficialRankings dict(cheapTop100/qualityTop100/combinedTop10).
    write 0. 재계산 없이 명칭 매핑·정렬만 한다.
    - 신규 signal(valueTop100/profitabilityTop100 존재): 전체 유효 universe 기준 독립 순위를
      쓰고 cheapTop100/qualityTop100 표시 순위를 1~100 연속으로 부여(rankingScope=global).
    - 구형 signal(해당 배열 없음): 기존대로 combined top100 후보를 각 순위로 정렬(rankingScope=subset).
    combinedTop10은 두 경우 모두 combined top100 후보(top10)에서 동일하게 생성(회귀 없음)."""
    doc = json.loads(Path(rankings_path).read_text(encoding="utf-8"))
    top100 = doc.get("top100") or []
    signal_as_of = str(doc.get("signalAsOfDate") or "")
    items = [ranking_item(r, signal_as_of) for r in top100]
    ev_method = next((it["evMethod"] for it in items if it.get("evMethod")), None)

    value_src = doc.get("valueTop100")
    profit_src = doc.get("profitabilityTop100")
    has_global = isinstance(value_src, list) and isinstance(profit_src, list) and value_src and profit_src

    if has_global:
        cheap = _global_list(value_src, signal_as_of, "cheap", cheap_n)
        quality = _global_list(profit_src, signal_as_of, "quality", quality_n)
        scope = SCOPE_GLOBAL
        if ev_method is None:
            ev_method = next((it["evMethod"] for it in cheap if it.get("evMethod")), None)
    else:
        cheap = sorted(items, key=lambda it: (_rank_key(it, "cheapRank"), it["code"]))[:cheap_n]
        quality = sorted(items, key=lambda it: (_rank_key(it, "qualityRank"), it["code"]))[:quality_n]
        scope = SCOPE_SUBSET

    # combinedTop10은 항상 combined top100 후보(top10 subset)에서 생성 — 기존 결과 회귀 없음.
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
        "rankingScope": scope,
        "eligibleCount": doc.get("eligibleCount"),
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
        print(f"scope={out['rankingScope']} cheapTop100={len(out['cheapTop100'])} "
              f"qualityTop100={len(out['qualityTop100'])} combinedTop10={len(out['combinedTop10'])} "
              f"evMethod={out['evMethod']} dataDate={out['dataDate']} eligibleCount={out['eligibleCount']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
