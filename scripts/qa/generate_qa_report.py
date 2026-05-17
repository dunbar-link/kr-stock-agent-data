"""와바바 추천 품질 QA 리포트 생성기 (MVP).

recommendation-history.json 을 읽어 사람이 빠르게 검토하기 좋은 형태로 요약한다.
- 추천/매매 로직은 건드리지 않는다 (read-only).
- 결과물: reports/qa-recommendation-YYYY-MM-DD.txt (+.json)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
REPORTS_DIR = ROOT_DIR / "reports"


def _fmt(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        try:
            return f"{value:.{digits}f}{suffix}"
        except Exception:
            return str(value)
    return str(value)


def _first_line(text: Any, max_len: int = 160) -> str:
    if text is None:
        return "-"
    if isinstance(text, list):
        text = " · ".join(str(x) for x in text if x)
    s = str(text).strip().replace("\r", " ").replace("\n", " ")
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s or "-"


def _pick_key_metrics(item: dict) -> dict:
    return {
        "symbol": item.get("symbol") or item.get("code"),
        "corpName": item.get("corpName") or item.get("name"),
        "industry": item.get("industryName"),
        "PER": item.get("PER"),
        "PBR": item.get("PBR"),
        "ROE": item.get("ROE"),
        "salesGrowth": item.get("salesGrowth"),
        "opIncomeGrowth": item.get("opIncomeGrowth"),
        "opMargin": item.get("opMargin"),
        "salesCagr3Y": item.get("salesCagr3Y"),
        "newsMomentumScore": item.get("newsMomentumScore"),
        "wababaScore": item.get("wababaScore"),
        "rank": item.get("rank"),
        "decision": item.get("decision"),
        "qualityLevel": item.get("qualityLevel"),
        "qualityWarnings": item.get("qualityWarnings") or [],
        "valueBuyPassed": item.get("valueBuyPassed"),
        "buyReason": item.get("buyReason"),
        "riskSummary": item.get("riskSummary"),
        "oneLineRecommendation": item.get("oneLineRecommendation"),
        "rankReason": item.get("rankReason"),
    }


def _anomaly_tags(item: dict) -> list[str]:
    tags: list[str] = []
    per = item.get("PER")
    pbr = item.get("PBR")
    sg = item.get("salesGrowth")
    op = item.get("opIncomeGrowth")
    warnings = item.get("qualityWarnings") or []

    # 성장 높지만 밸류 비쌈
    if isinstance(sg, (int, float)) and sg >= 30:
        if (isinstance(per, (int, float)) and per >= 30) or (
            isinstance(pbr, (int, float)) and pbr >= 5
        ):
            tags.append("성장↑ 밸류↑(고평가)")

    # 밸류 좋지만 성장 약함
    if isinstance(per, (int, float)) and per <= 10:
        if isinstance(sg, (int, float)) and sg < 5:
            tags.append("밸류↓ 성장약함")

    # 영업이익 변동성 극단 (전년 적자 → 흑전 가능성)
    if isinstance(op, (int, float)) and op >= 1000:
        tags.append("영업익 폭증(흑전/기저↑)")

    if warnings:
        tags.append("qualityWarning")

    return tags


def _render_text(report: dict) -> str:
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("와바바 추천 품질 QA 리포트")
    lines.append("=" * 72)
    lines.append(f"baseDate         : {report['baseDate']}")
    lines.append(f"generatedAt      : {report['generatedAt']}")
    lines.append(f"reportGeneratedAt: {report['reportGeneratedAt']}")
    lines.append(
        f"counts           : picks={report['counts']['wababaPicks']} "
        f"new={report['counts']['new']} cont={report['counts']['continued']} "
        f"removed={report['counts']['removed']} explore={report['counts']['explore']}"
    )
    lines.append("")

    # finalBestPick
    best = report.get("finalBestPick")
    lines.append("─" * 72)
    lines.append("[오늘의 finalBestPick]")
    lines.append("─" * 72)
    if best:
        lines.append(f"{best['corpName']} ({best['symbol']}) | {best['industry']}")
        lines.append(
            f"  PER={_fmt(best['PER'])} PBR={_fmt(best['PBR'])} "
            f"ROE={_fmt(best['ROE'])}% | "
            f"salesGrowth={_fmt(best['salesGrowth'])}% "
            f"opIncomeGrowth={_fmt(best['opIncomeGrowth'])}% "
            f"opMargin={_fmt(best['opMargin'])}%"
        )
        lines.append(
            f"  news={_fmt(best['newsMomentumScore'], 1)} "
            f"wababaScore={_fmt(best['wababaScore'], 1)} "
            f"decision={best['decision']} quality={best['qualityLevel']}"
        )
        lines.append(f"  추천한줄: {_first_line(best['oneLineRecommendation'])}")
        lines.append(f"  buyReason: {_first_line(best['buyReason'])}")
        lines.append(f"  rankReason: {_first_line(best['rankReason'])}")
        lines.append(f"  riskSummary: {_first_line(best['riskSummary'])}")
        warns = best.get("qualityWarnings") or []
        if warns:
            lines.append(f"  ⚠ qualityWarnings: {', '.join(warns)}")
    else:
        lines.append("  (finalBestPick 없음)")
    lines.append("")

    # wababaPicks
    lines.append("─" * 72)
    lines.append(f"[wababaPicks 전체 ({report['counts']['wababaPicks']}개)]")
    lines.append("─" * 72)
    for idx, p in enumerate(report["wababaPicks"], start=1):
        lines.append(
            f"{idx:>2}. {p['corpName']} ({p['symbol']}) | rank={p['rank']} "
            f"| {p['industry']}"
        )
        lines.append(
            f"    PER={_fmt(p['PER'])} PBR={_fmt(p['PBR'])} "
            f"ROE={_fmt(p['ROE'])}% | "
            f"sales={_fmt(p['salesGrowth'])}% op={_fmt(p['opIncomeGrowth'])}% "
            f"news={_fmt(p['newsMomentumScore'], 1)}"
        )
        lines.append(
            f"    decision={p['decision']} valueBuyPassed={p['valueBuyPassed']} "
            f"score={_fmt(p['wababaScore'], 1)}"
        )
        lines.append(f"    추천한줄: {_first_line(p['oneLineRecommendation'])}")
        lines.append(f"    buyReason: {_first_line(p['buyReason'])}")
        lines.append(f"    risk    : {_first_line(p['riskSummary'])}")
        warns = p.get("qualityWarnings") or []
        if warns:
            lines.append(f"    ⚠ warnings: {', '.join(warns)}")
        anomalies = p.get("anomalyTags") or []
        if anomalies:
            lines.append(f"    ⚑ anomaly : {', '.join(anomalies)}")
        lines.append("")

    # diff
    lines.append("─" * 72)
    lines.append("[전일 대비 변화]")
    lines.append("─" * 72)
    lines.append(f"  신규(new)     : {report['counts']['new']}")
    for p in report["newWababaPicks"]:
        lines.append(f"    + {p['corpName']} ({p['symbol']})")
    lines.append(f"  연속(continued): {report['counts']['continued']}")
    for p in report["continuedWababaPicks"]:
        lines.append(f"    = {p['corpName']} ({p['symbol']})")
    lines.append(f"  탈락(removed)  : {report['counts']['removed']}")
    for p in report["removedWababaPicks"]:
        lines.append(f"    - {p['corpName']} ({p['symbol']})")
    lines.append("")

    # anomalies summary
    lines.append("─" * 72)
    lines.append("[이상치 감지]")
    lines.append("─" * 72)
    anomalies = report["anomalies"]
    if not anomalies:
        lines.append("  (감지된 이상치 없음)")
    else:
        for a in anomalies:
            lines.append(
                f"  · {a['corpName']} ({a['symbol']}) → {', '.join(a['tags'])}"
            )
    lines.append("")

    # top-score not picked
    lines.append("─" * 72)
    lines.append("[exploreCandidates 점수 상위 — wababaPicks 미포함 상위 10]")
    lines.append("─" * 72)
    top_non_picks = report["topScoreNotPicked"]
    if not top_non_picks:
        lines.append("  (해당 없음)")
    else:
        for e in top_non_picks:
            lines.append(
                f"  · {e['corpName']} ({e['symbol']}) "
                f"score={_fmt(e['wababaScore'], 1)} "
                f"PER={_fmt(e['PER'])} ROE={_fmt(e['ROE'])}% "
                f"sales={_fmt(e['salesGrowth'])}%"
            )
    lines.append("")
    lines.append("=" * 72)
    lines.append("end of report")
    lines.append("=" * 72)
    return "\n".join(lines) + "\n"


def build_report(history: dict) -> dict:
    wababa_picks = history.get("wababaPicks") or []
    new_picks = history.get("newWababaPicks") or []
    continued_picks = history.get("continuedWababaPicks") or []
    removed_picks = history.get("removedWababaPicks") or []
    explore = history.get("exploreCandidates") or []

    enriched_picks = []
    for p in wababa_picks:
        m = _pick_key_metrics(p)
        m["anomalyTags"] = _anomaly_tags(p)
        enriched_picks.append(m)

    # de-dup explore by symbol, exclude picks, keep highest score row
    pick_symbols = {p.get("symbol") for p in wababa_picks if p.get("symbol")}
    seen: dict[str, dict] = {}
    for e in explore:
        sym = e.get("symbol")
        if not sym or sym in pick_symbols:
            continue
        prev = seen.get(sym)
        if prev is None or (e.get("wababaScore") or 0) > (prev.get("wababaScore") or 0):
            seen[sym] = e
    top_non_picks_raw = sorted(
        seen.values(), key=lambda x: x.get("wababaScore") or 0, reverse=True
    )[:10]
    top_non_picks = [_pick_key_metrics(e) for e in top_non_picks_raw]

    anomalies: list[dict] = []
    for p in wababa_picks:
        tags = _anomaly_tags(p)
        if tags:
            anomalies.append(
                {
                    "symbol": p.get("symbol"),
                    "corpName": p.get("corpName"),
                    "tags": tags,
                }
            )
    for e in top_non_picks_raw:
        tags = _anomaly_tags(e)
        if tags:
            anomalies.append(
                {
                    "symbol": e.get("symbol"),
                    "corpName": e.get("corpName"),
                    "tags": tags + ["(explore)"],
                }
            )

    return {
        "baseDate": history.get("baseDate"),
        "generatedAt": history.get("generatedAt"),
        "reportGeneratedAt": datetime.now().isoformat(timespec="seconds"),
        "counts": {
            "wababaPicks": len(wababa_picks),
            "new": len(new_picks),
            "continued": len(continued_picks),
            "removed": len(removed_picks),
            "explore": len(explore),
        },
        "finalBestPick": _pick_key_metrics(history["finalBestPick"])
        if history.get("finalBestPick")
        else None,
        "wababaPicks": enriched_picks,
        "newWababaPicks": [_pick_key_metrics(p) for p in new_picks],
        "continuedWababaPicks": [_pick_key_metrics(p) for p in continued_picks],
        "removedWababaPicks": [_pick_key_metrics(p) for p in removed_picks],
        "topScoreNotPicked": top_non_picks,
        "anomalies": anomalies,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="와바바 추천 품질 QA 리포트 생성")
    parser.add_argument(
        "--input",
        type=Path,
        default=RECOMMENDATION_HISTORY_PATH,
        help="recommendation-history.json 경로",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=REPORTS_DIR,
        help="리포트 출력 디렉토리",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="텍스트 리포트를 stdout으로도 출력",
    )
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"[qa] 입력 파일 없음: {args.input}", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as fh:
        history = json.load(fh)

    report = build_report(history)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    base = report["baseDate"] or "unknown"
    txt_path = args.out_dir / f"qa-recommendation-{base}.txt"
    json_path = args.out_dir / f"qa-recommendation-{base}.json"

    text = _render_text(report)
    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write(text)
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    print(f"[qa] written: {txt_path}")
    print(f"[qa] written: {json_path}")
    print(
        f"[qa] baseDate={report['baseDate']} "
        f"picks={report['counts']['wababaPicks']} "
        f"new={report['counts']['new']} "
        f"cont={report['counts']['continued']} "
        f"removed={report['counts']['removed']} "
        f"anomalies={len(report['anomalies'])}"
    )
    if args.stdout:
        sys.stdout.write("\n" + text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
