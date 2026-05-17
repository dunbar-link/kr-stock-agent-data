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

# build_recommendation_history는 read-only 헬퍼만 사용한다.
# 절대 main() / apply_*_auto_trading / write_json 호출하지 않음.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _format_won(amt: float) -> str:
    try:
        return f"{amt:,.0f}원"
    except Exception:
        return str(amt)


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


def _compute_main_fund_orders(B, portfolio: dict, history: dict, today_map: dict) -> dict:
    policy = B.get_fund_policy(portfolio)
    positions = B.normalize_portfolio_positions(portfolio)
    cash = B.get_portfolio_cash(portfolio, positions)
    initial = B.safe_number(policy.get("initialCapital"))

    sells: list[dict] = []
    for pos in positions:
        code = str(pos.get("code") or "").strip()
        name = str(pos.get("name") or "").strip()
        item = today_map.get(code) or {"code": code, "name": name}
        cp = B.get_current_price(item)
        qty = B.safe_number(pos.get("quantity"))
        decision = B.build_portfolio_decision(item, pos)
        signal = decision.get("sellSignal") if isinstance(decision, dict) else {}
        urgency = str(signal.get("urgency") or "LOW") if isinstance(signal, dict) else "LOW"
        action = str(decision.get("action") or "")
        pr = decision.get("profitRate")
        prn = B.safe_number(pr) if pr is not None else 0
        sell_qty, sell_reason = 0, ""
        if cp > 0 and qty > 0 and urgency == "HIGH":
            sell_qty, sell_reason = qty, "매도 엔진 HIGH"
        elif cp > 0 and qty > 0 and action == "SELL_CHECK" and prn <= -8:
            sell_qty, sell_reason = qty, "손실 확대 + SELL_CHECK"
        elif cp > 0 and qty > 1 and action == "TAKE_PROFIT_CHECK" and prn >= 20:
            sell_qty, sell_reason = max(1, int(qty * 0.5)), "수익 20%+ 일부 차익"
        if sell_qty > 0:
            sells.append({
                "code": code, "name": name, "qty": sell_qty,
                "price": cp, "amount": cp * sell_qty, "reason": sell_reason,
            })

    pick = B.pick_auto_trade_candidate(
        history.get("finalBestPick"),
        history.get("wababaPicks"),
        history.get("exploreGroups") or {},
        portfolio,
        policy,
    )
    buy: dict | None = None
    skip_reason: str | None = None
    if pick:
        code = B.get_code(pick)
        name = B.get_name(pick)
        engine = pick.get("decisionEngine") if isinstance(pick.get("decisionEngine"), dict) else B.build_decision_engine(pick)
        sizing = pick.get("positionSizing") if isinstance(pick.get("positionSizing"), dict) else B.build_position_sizing(pick, engine)
        confidence = B.safe_int(engine.get("confidence"), 0) if isinstance(engine, dict) else 0
        action = str(engine.get("action") or "") if isinstance(engine, dict) else ""
        cp = B.get_current_price(pick)
        first_buy = B.safe_number(sizing.get("firstBuyAmount")) if isinstance(sizing, dict) else 0
        target_amt = B.safe_number(sizing.get("targetAmount")) if isinstance(sizing, dict) else 0
        buy_budget = first_buy if first_buy > 0 else min(target_amt, initial * 0.05)
        min_cash_after = initial * B.safe_number(policy.get("minCashRate")) / 100
        max_pos = B.safe_int(policy.get("maxPositions"), 12)
        already_held = any(str(p.get("code") or "").strip() == code for p in positions)

        if not code:
            skip_reason = "코드 없음"
        elif already_held:
            skip_reason = "이미 보유 중"
        elif len(positions) >= max_pos:
            skip_reason = "보유 종목 한도 도달"
        elif confidence < B.safe_int(policy.get("minBuyConfidence"), 70):
            skip_reason = f"신뢰도 {confidence}% 미달"
        elif action != "BUY_NOW":
            skip_reason = f"action != BUY_NOW (action={action})"
        elif cp <= 0:
            skip_reason = "현재가 없음"
        elif buy_budget <= 0:
            skip_reason = "매수 예산 0"
        elif cash - buy_budget < min_cash_after:
            skip_reason = "매수 가능 현금 부족"
        else:
            qty = int(buy_budget // cp)
            if qty <= 0:
                skip_reason = "1주 매수 예산 미달"
            else:
                label = sizing.get("label") if isinstance(sizing, dict) else ""
                buy = {
                    "code": code, "name": name, "qty": qty, "price": cp,
                    "amount": qty * cp,
                    "reason": f"신뢰도 {confidence}% · {label}".strip(" ·"),
                    "source": pick.get("autoTradeSource"),
                    "score": B.safe_number(pick.get("autoTradeScore")),
                    "confidence": confidence,
                }

    new_cash = cash
    for s in sells:
        new_cash += s["amount"]
    if buy:
        new_cash -= buy["amount"]

    return {
        "cash": cash, "newCash": new_cash, "initialCapital": initial,
        "positions": len(positions), "maxPositions": policy.get("maxPositions"),
        "maxPositionWeight": B.safe_number(policy.get("maxPositionWeight"), 10),
        "buys": [buy] if buy else [], "sells": sells, "skipReason": skip_reason,
    }


def _compute_ai_fund_orders(B, ai_portfolio: dict, all_wababa_candidates: list, today_map: dict) -> dict:
    policy = B.get_ai_fund_policy(ai_portfolio)
    positions = B.normalize_portfolio_positions(ai_portfolio)
    cash = B.get_portfolio_cash(ai_portfolio, positions)
    initial = B.safe_number(policy.get("initialCapital"))
    max_pw = B.safe_number(policy.get("maxPositionWeight"), 12)
    min_cash_rate = B.safe_number(policy.get("minCashRate"), 5)
    min_cash_after_buy = initial * min_cash_rate / 100
    max_pos = B.safe_int(policy.get("maxPositions"), 10)

    sells: list[dict] = []
    for pos in positions:
        code = str(pos.get("code") or "").strip()
        name = str(pos.get("name") or "").strip()
        item = today_map.get(code) or {"code": code, "name": name}
        cp = B.get_current_price(item)
        qty = B.safe_number(pos.get("quantity"))
        should_sell, reason = B.should_ai_sell_position(item, pos)
        if should_sell and cp > 0 and qty > 0:
            sells.append({
                "code": code, "name": name, "qty": qty,
                "price": cp, "amount": cp * qty, "reason": reason,
            })

    held_codes = {str(p.get("code") or "").strip() for p in positions}
    cur_cash = cash
    cur_pos_cnt = len(positions)
    buys: list[dict] = []
    ai_picks = B.pick_wababa_ai_trade_candidates(all_wababa_candidates, ai_portfolio, policy)
    for item in ai_picks:
        if cur_pos_cnt >= max_pos:
            break
        code = B.get_code(item)
        name = B.get_name(item)
        cp = B.get_current_price(item)
        ai_score = B.safe_number(item.get("aiFundScore"))
        if not code or cp <= 0 or code in held_codes:
            continue
        target_weight = 4
        if ai_score >= 260:
            target_weight = 10
        elif ai_score >= 230:
            target_weight = 8
        elif ai_score >= 200:
            target_weight = 6
        target_weight = min(target_weight, max_pw)
        buy_budget = int(initial * target_weight / 100 / 2)
        buy_budget = min(buy_budget, max(0, cur_cash - min_cash_after_buy))
        qty = int(buy_budget // cp) if buy_budget > 0 else 0
        if qty <= 0:
            continue
        amt = qty * cp
        buys.append({
            "code": code, "name": name, "qty": qty, "price": cp, "amount": amt,
            "weight": target_weight, "aiScore": ai_score,
            "reason": item.get("aiFundReason"),
        })
        cur_cash -= amt
        cur_pos_cnt += 1
        held_codes.add(code)

    new_cash = cash
    for s in sells:
        new_cash += s["amount"]
    for b in buys:
        new_cash -= b["amount"]

    return {
        "cash": cash, "newCash": new_cash, "initialCapital": initial,
        "positions": len(positions), "maxPositions": policy.get("maxPositions"),
        "maxPositionWeight": max_pw, "minCashAfter": min_cash_after_buy,
        "buys": buys, "sells": sells,
    }


def _evaluate_dry_run_risks(main: dict, ai: dict) -> list[str]:
    risks: list[str] = []
    if main["buys"] and main["newCash"] < 0:
        risks.append("WABABA: 매수 후 현금 음수")
    if ai["buys"] and ai["newCash"] < ai["minCashAfter"]:
        risks.append("AI: 매수 후 현금이 min_cash_rate 미달")
    for b in main["buys"]:
        if main["initialCapital"] > 0:
            w = b["amount"] / main["initialCapital"] * 100
            if w > main["maxPositionWeight"]:
                risks.append(f"WABABA: {b['code']} 비중 {w:.1f}% > {main['maxPositionWeight']}%")
    for b in ai["buys"]:
        if ai["initialCapital"] > 0:
            w = b["amount"] / ai["initialCapital"] * 100
            if w > ai["maxPositionWeight"]:
                risks.append(f"AI: {b['code']} 비중 {w:.1f}% > {ai['maxPositionWeight']}%")
    return risks


def compute_dry_run(history: dict) -> dict | None:
    """build_recommendation_history.py의 read-only 헬퍼만 사용해
    오늘 자동매매가 ON이었다면 어떤 주문이 만들어질지 계산한다.
    portfolio/trade/auto-trade-log 파일을 절대 쓰지 않는다.
    """
    try:
        import build_recommendation_history as B  # type: ignore
    except Exception as exc:
        return {"error": f"build_recommendation_history import 실패: {exc}"}

    try:
        market = B.read_json(B.MARKET_SNAPSHOT_PATH) or {}
        if not market:
            return {"error": "financial-universe-real.json 없음"}
        news = B.read_json(B.NEWS_MOMENTUM_PATH) or {}
        config = B.load_filter_config()
        portfolio = B.load_portfolio()
        ai_portfolio = B.load_ai_portfolio()

        raw_items = B.get_items(market)
        news_map = B.build_news_map(news)
        today_items = [B.normalize_item(it, news_map) for it in raw_items]
        today_map = {it["code"]: it for it in today_items if it.get("code")}

        base_date = B.get_base_date(market)
        base_date_text = str(base_date or "")[:10]
        market_open = B.is_market_open_day(base_date_text, B.get_fund_policy(portfolio))

        all_wababa_candidates = []
        for it in today_items:
            if B.is_wababa_candidate(it, config):
                all_wababa_candidates.append({
                    **it,
                    "generalWababaScore": B.calculate_wababa_score(it),
                    "stableScore": B.calculate_stable_score(it),
                    "growthScore": B.calculate_growth_score(it),
                    "opportunityScore": B.calculate_opportunity_score(it),
                    "qualityPenalty": B.calculate_quality_penalty(it),
                    "qualityLevel": B.classify_quality_level(it),
                    "qualityWarnings": B.build_quality_warnings(it),
                    "finalBestScore": B.calculate_final_best_score(it),
                    "todayPickScore": B.calculate_final_best_score(it),
                })

        main = _compute_main_fund_orders(B, portfolio, history, today_map)
        ai = _compute_ai_fund_orders(B, ai_portfolio, all_wababa_candidates, today_map)
        risks = _evaluate_dry_run_risks(main, ai)

        return {
            "baseDate": base_date_text,
            "marketOpen": bool(market_open),
            "main": main,
            "ai": ai,
            "risks": risks,
        }
    except Exception as exc:
        return {"error": f"dry-run 계산 실패: {exc}"}


def render_dry_run_section(dry: dict | None) -> list[str]:
    out: list[str] = ["## 오늘 예상 주문 (Dry-Run)"]
    if not dry:
        out.append("- dry-run 비활성화 또는 데이터 없음")
        out.append("")
        return out
    if "error" in dry:
        out.append(f"- ⚠ dry-run 계산 불가: {dry['error']}")
        out.append("")
        return out

    if not dry.get("marketOpen"):
        out.append("- ⚠ 오늘 MARKET_CLOSED 상태 — 실제 호출 시 주문 미발생. 아래는 시장 개장 가정 시 예상치")

    out.append("")
    out.append("### 와바바펀드")
    main = dry["main"]
    if main["buys"]:
        for b in main["buys"]:
            out.append(f"- BUY **{b['name']}** ({b['code']}) {b['qty']}주")
            out.append(f"  · 예상 금액: {_format_won(b['amount'])}")
            out.append(f"  · 사유: {b['reason']}")
            out.append(f"  · 주문 후 현금: {_format_won(main['newCash'])}")
    elif main.get("skipReason"):
        out.append(f"- BUY 없음 — {main['skipReason']}")
    else:
        out.append("- BUY 없음 — 가치매수 기준 통과 후보 없음")
    if main["sells"]:
        for s in main["sells"]:
            out.append(f"- SELL **{s['name']}** ({s['code']}) {s['qty']}주 — {_format_won(s['amount'])} ({s['reason']})")
    else:
        out.append("- SELL 없음")

    out.append("")
    out.append("### 와바바AI펀드")
    ai = dry["ai"]
    if ai["buys"]:
        for b in ai["buys"]:
            out.append(f"- BUY **{b['name']}** ({b['code']}) {b['qty']}주")
            out.append(f"  · 예상 금액: {_format_won(b['amount'])}")
            out.append(f"  · ai_score {b['aiScore']:.1f} / 목표 비중 {b['weight']}%")
            if b.get("reason"):
                out.append(f"  · 사유: {b['reason']}")
        out.append(f"- 주문 후 현금: {_format_won(ai['newCash'])}")
    else:
        out.append("- BUY 없음")
    if ai["sells"]:
        for s in ai["sells"]:
            out.append(f"- SELL **{s['name']}** ({s['code']}) {s['qty']}주 — {_format_won(s['amount'])} ({s['reason']})")
    else:
        out.append("- SELL 없음")

    out.append("")
    out.append("### 위험 체크")
    risks = dry.get("risks") or []
    if risks:
        for r in risks:
            out.append(f"- ⚠ {r}")
    else:
        out.append("- ✓ 명시적 위험 없음 (현금 부족 / 비중 초과 / 중복 매수 모두 OK)")
    if not dry.get("marketOpen"):
        out.append("- ℹ 오늘은 MARKET_CLOSED 상태 — 실제 주문 미발생")

    out.append("")
    return out


def render_summary(history: dict, dry: dict | None = None) -> str:
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

    # 오늘 예상 주문 (dry-run)
    out.extend(render_dry_run_section(dry))

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
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="오늘 예상 주문(Dry-Run) 섹션 생략 (빠른 모드)",
    )
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"[daily-summary] 입력 파일 없음: {args.input}", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as fh:
        history = json.load(fh)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    base = history.get("baseDate") or "unknown"
    md_path = args.out_dir / f"daily-summary-{base}.md"
    dry = None if args.no_dry_run else compute_dry_run(history)
    text = render_summary(history, dry)
    with md_path.open("w", encoding="utf-8") as fh:
        fh.write(text)

    print(f"[daily-summary] written: {md_path}")
    print(
        f"[daily-summary] baseDate={base} picks={len(history.get('wababaPicks') or [])} "
        f"new={len(history.get('newWababaPicks') or [])} "
        f"cont={len(history.get('continuedWababaPicks') or [])} "
        f"removed={len(history.get('removedWababaPicks') or [])}"
    )
    if dry:
        if "error" in dry:
            print(f"[daily-summary] dry-run: {dry['error']}")
        else:
            print(
                f"[daily-summary] dry-run: marketOpen={dry.get('marketOpen')} "
                f"mainBuy={len(dry['main']['buys'])} mainSell={len(dry['main']['sells'])} "
                f"aiBuy={len(dry['ai']['buys'])} aiSell={len(dry['ai']['sells'])} "
                f"risks={len(dry.get('risks') or [])}"
            )
    if args.stdout:
        sys.stdout.write("\n" + text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
