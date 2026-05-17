"""와바바 운영자용 DAILY SUMMARY 리포트 (MVP).

generate_qa_report.py의 helper를 재사용해
사람이 30초 안에 읽을 수 있는 한 페이지 MD 브리핑을 만든다.
- 추천/매매 로직은 건드리지 않는다 (read-only).
- 결과물: reports/daily-summary-YYYY-MM-DD.md
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
REPORTS_DIR = ROOT_DIR / "reports"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_qa_report import _anomaly_tags, _first_line  # noqa: E402


def _market_mood(picks: list[dict], new_count: int, continued_count: int) -> str:
    if not picks:
        return "관망 — 와바바 선정 종목 없음"
    news_vals = [
        p.get("newsMomentumScore") for p in picks
        if isinstance(p.get("newsMomentumScore"), (int, float))
    ]
    avg_news = sum(news_vals) / len(news_vals) if news_vals else 0
    if new_count == 0 and continued_count > 0:
        mood = "유지 성격 — 신규 진입 없이 기존 성장 지속형 유지"
    elif new_count > continued_count:
        mood = "전환 성격 — 신규 진입 우세"
    elif new_count > 0:
        mood = "혼합 — 신규/유지 균형"
    else:
        mood = "관망 — 신규/연속 모두 부재"
    if avg_news >= 5:
        mood += f" / 뉴스 모멘텀 활발 (평균 {avg_news:.1f})"
    elif avg_news <= 1:
        mood += " / 뉴스 모멘텀 조용"
    return mood


def _industry_distribution(picks: list[dict]) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for p in picks:
        counter[p.get("industryName") or "미분류"] += 1
    return counter.most_common()


def _common_traits(picks: list[dict]) -> list[str]:
    if not picks:
        return []
    traits: list[str] = []

    def _range(key: str, suffix: str, digits: int = 1) -> str | None:
        vals = [p.get(key) for p in picks if isinstance(p.get(key), (int, float))]
        if not vals:
            return None
        if len(vals) == 1:
            return f"{key.replace('salesGrowth','매출성장').replace('opIncomeGrowth','영업익 증가율').replace('PER','PER').replace('ROE','ROE')} {vals[0]:.{digits}f}{suffix}"
        return None  # handled below

    per_vals = [p.get("PER") for p in picks if isinstance(p.get("PER"), (int, float))]
    roe_vals = [p.get("ROE") for p in picks if isinstance(p.get("ROE"), (int, float))]
    sg_vals = [p.get("salesGrowth") for p in picks if isinstance(p.get("salesGrowth"), (int, float))]
    op_vals = [p.get("opIncomeGrowth") for p in picks if isinstance(p.get("opIncomeGrowth"), (int, float))]
    if per_vals:
        traits.append(f"PER 범위 {min(per_vals):.1f}~{max(per_vals):.1f}")
    if roe_vals:
        traits.append(f"ROE 범위 {min(roe_vals):.1f}~{max(roe_vals):.1f}%")
    if sg_vals:
        traits.append(f"매출성장 {min(sg_vals):.0f}~{max(sg_vals):.0f}%")
    if op_vals:
        traits.append(f"영업익 증가율 {min(op_vals):.0f}~{max(op_vals):.0f}%")
    return traits


def _risk_keywords(picks: list[dict]) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for p in picks:
        for w in (p.get("qualityWarnings") or []):
            counter[w] += 1
        for tag in _anomaly_tags(p):
            counter[tag] += 1
    return counter.most_common(3)


def _pick_brief(p: dict) -> list[str]:
    lines = [f"- **{p.get('corpName')}** ({p.get('symbol')})"]
    bits: list[str] = []
    if isinstance(p.get("PER"), (int, float)):
        bits.append(f"PER {p['PER']:.1f}")
    if isinstance(p.get("ROE"), (int, float)):
        bits.append(f"ROE {p['ROE']:.1f}%")
    if isinstance(p.get("opIncomeGrowth"), (int, float)):
        bits.append(f"영업익 {p['opIncomeGrowth']:+.0f}%")
    if bits:
        lines.append(f"  · {' / '.join(bits)}")
    return lines


def _top_non_picks(history: dict, limit: int = 5) -> list[dict]:
    picks = history.get("wababaPicks") or []
    explore = history.get("exploreCandidates") or []
    pick_syms = {p.get("symbol") for p in picks if p.get("symbol")}
    seen: dict[str, dict] = {}
    for e in explore:
        sym = e.get("symbol")
        if not sym or sym in pick_syms:
            continue
        prev = seen.get(sym)
        if prev is None or (e.get("wababaScore") or 0) > (prev.get("wababaScore") or 0):
            seen[sym] = e
    return sorted(seen.values(), key=lambda x: x.get("wababaScore") or 0, reverse=True)[:limit]


def render_summary(history: dict) -> str:
    base = history.get("baseDate") or "unknown"
    final_best = history.get("finalBestPick") or {}
    picks = history.get("wababaPicks") or []
    new_picks = history.get("newWababaPicks") or []
    cont_picks = history.get("continuedWababaPicks") or []
    removed_picks = history.get("removedWababaPicks") or []

    notable = []
    for p in picks:
        tags = _anomaly_tags(p)
        if tags:
            notable.append({"pick": p, "tags": tags})

    top_non_picks = _top_non_picks(history, limit=5)

    out: list[str] = []
    out.append("# 와바바 DAILY SUMMARY")
    out.append("")
    out.append(f"날짜: {base}  ")
    out.append(f"생성: {datetime.now().isoformat(timespec='seconds')}  ")
    out.append(
        f"picks: {len(picks)} (신규 {len(new_picks)} / 연속 {len(cont_picks)} / 탈락 {len(removed_picks)})"
    )
    out.append("")

    # 오늘의 종합 BEST
    out.append("## 오늘의 종합 BEST")
    if final_best:
        out.append(
            f"- **{final_best.get('corpName')}** ({final_best.get('symbol')}) — {final_best.get('industryName') or ''}"
        )
        bits: list[str] = []
        if isinstance(final_best.get("PER"), (int, float)):
            bits.append(f"PER {final_best['PER']:.1f}")
        if isinstance(final_best.get("PBR"), (int, float)):
            bits.append(f"PBR {final_best['PBR']:.1f}")
        if isinstance(final_best.get("ROE"), (int, float)):
            bits.append(f"ROE {final_best['ROE']:.1f}%")
        if isinstance(final_best.get("opIncomeGrowth"), (int, float)):
            bits.append(f"영업익 {final_best['opIncomeGrowth']:+.0f}%")
        if bits:
            out.append(f"- {' / '.join(bits)}")
        news = final_best.get("newsMomentumScore")
        if isinstance(news, (int, float)):
            label = "강함" if news >= 5 else "보통" if news >= 1 else "조용"
            out.append(f"- 뉴스 모멘텀: {label} ({news:.1f})")
        one_line = final_best.get("oneLineRecommendation")
        if one_line:
            out.append(f"- 추천 한줄: {_first_line(one_line)}")
        is_continued = any(cp.get("symbol") == final_best.get("symbol") for cp in cont_picks)
        if is_continued:
            out.append("- 와바바 연속 추천 중 (전일에도 포함)")
        else:
            out.append("- 오늘 신규 진입")
    else:
        out.append("- 없음")
    out.append("")

    # 신규 진입
    out.append("## 신규 진입")
    if new_picks:
        for p in new_picks:
            out.extend(_pick_brief(p))
    else:
        out.append("- 없음")
    out.append("")

    # 연속 추천
    out.append("## 연속 추천")
    if cont_picks:
        for p in cont_picks:
            out.extend(_pick_brief(p))
    else:
        out.append("- 없음")
    out.append("")

    # 탈락
    out.append("## 탈락")
    if removed_picks:
        for p in removed_picks:
            out.append(f"- {p.get('corpName')} ({p.get('symbol')})")
    else:
        out.append("- 없음")
    out.append("")

    # 오늘의 주의 종목
    out.append("## 오늘의 주의 종목")
    if notable:
        for n in notable:
            p = n["pick"]
            out.append(f"- **{p.get('corpName')}** ({p.get('symbol')})")
            warns = p.get("qualityWarnings") or []
            if warns:
                out.append(f"  · qualityWarnings: {', '.join(warns)}")
            for t in n["tags"]:
                out.append(f"  · {t}")
    else:
        out.append("- 없음")
    out.append("")

    # 점수 높지만 미선정
    out.append("## 점수 높지만 미선정 종목")
    if top_non_picks:
        for e in top_non_picks:
            out.append(
                f"- {e.get('corpName')} ({e.get('symbol')}) — score {(e.get('wababaScore') or 0):.0f}"
            )
    else:
        out.append("- 없음")
    out.append("")

    # 시장 분위기
    out.append("## 시장 분위기")
    out.append(f"- {_market_mood(picks, len(new_picks), len(cont_picks))}")
    out.append("")

    # 업종 분포
    out.append("## 업종 분포")
    dist = _industry_distribution(picks)
    if dist:
        for name, count in dist:
            out.append(f"- {name}: {count}")
    else:
        out.append("- 없음")
    out.append("")

    # 추천 종목 공통 특징
    out.append("## 추천 종목 공통 특징")
    traits = _common_traits(picks)
    if traits:
        for t in traits:
            out.append(f"- {t}")
    else:
        out.append("- 없음")
    out.append("")

    # 리스크 키워드 TOP3
    out.append("## 리스크 키워드 TOP3")
    risks = _risk_keywords(picks)
    if risks:
        for kw, count in risks:
            out.append(f"- {kw} ({count}건)")
    else:
        out.append("- 없음")
    out.append("")

    # 운영 코멘트
    out.append("## 운영 코멘트")
    comments: list[str] = []
    if len(new_picks) == 0 and len(cont_picks) > 0:
        comments.append("오늘은 신규 진입 없이 기존 성장 지속형 유지 성격")
    elif len(new_picks) > 0 and len(cont_picks) == 0:
        comments.append("오늘은 전면 교체 성격 — 신규 진입 위주")
    elif len(new_picks) > 0 and len(cont_picks) > 0:
        comments.append(f"신규 {len(new_picks)} + 연속 {len(cont_picks)} 혼합 성격")
    elif not picks:
        comments.append("오늘은 추천 종목 없음 — 관망")
    if dist:
        if len(dist) == 1:
            comments.append(f"업종 흐름: {dist[0][0]} 단일 집중")
        else:
            comments.append(f"업종 흐름: {dist[0][0]} 중심 + {len(dist)-1}개 업종 분산")
    if notable:
        comments.append(f"이상치 {len(notable)}건 — 매수 직전 재확인 권장")
    if not comments:
        comments.append("(자동 코멘트 없음)")
    for c in comments:
        out.append(f"- {c}")
    out.append("")

    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="와바바 운영자용 DAILY SUMMARY")
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
    parser.add_argument("--stdout", action="store_true", help="MD를 stdout으로도 출력")
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"[daily-summary] 입력 파일 없음: {args.input}", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as fh:
        history = json.load(fh)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    base = history.get("baseDate") or "unknown"
    md_path = args.out_dir / f"daily-summary-{base}.md"
    text = render_summary(history)
    with md_path.open("w", encoding="utf-8") as fh:
        fh.write(text)

    print(f"[daily-summary] written: {md_path}")
    print(
        f"[daily-summary] baseDate={base} picks={len(history.get('wababaPicks') or [])} "
        f"new={len(history.get('newWababaPicks') or [])} "
        f"cont={len(history.get('continuedWababaPicks') or [])} "
        f"removed={len(history.get('removedWababaPicks') or [])}"
    )
    if args.stdout:
        sys.stdout.write("\n" + text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
