#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
qa_recommendation_narratives.py
와바바 추천 리포트 문장 QA 하네스

목적:
  추천 생성 없이 문장 품질(반복·수치 중복·금지 표현·길이)을 빠르게 검수.
  파일 저장 없이 콘솔 출력만 수행.

실행:
  cd C:\\work\\kr-stock-agent-data-new
  python scripts\\qa_recommendation_narratives.py
"""

import sys
import json
import re
from collections import Counter
from pathlib import Path

# ── 경로 설정 ──────────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
UNIVERSE_FILE = ROOT_DIR / "financial-universe-real.json"
REC_HISTORY_FILE = ROOT_DIR / "recommendation-history.json"

# ── build_recommendation_history.py에서 문장 생성 함수 import ────────────────
sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from build_recommendation_history import (
        build_fact_phrase,
        build_business_impact_phrase,
        build_valuation_phrase,
        build_decision_phrase,
    )
except ImportError as e:
    print(f"[오류] build_recommendation_history.py import 실패: {e}")
    print(f"       스크립트 위치: {SCRIPTS_DIR}")
    sys.exit(1)

# ── 필드 정규화 (financial-universe-real.json → build_* 함수 기대 형식) ───────
def normalize(raw: dict) -> dict:
    """raw 항목을 build_* 함수가 기대하는 형식으로 변환."""
    hyp = raw.get("hypothesis") or ""
    ev = raw.get("evidence") or ""
    return {
        "code": raw.get("symbol", ""),
        "name": raw.get("corpName", ""),
        "industryName": raw.get("industryName", ""),
        "marketName": raw.get("marketName", ""),
        "price": raw.get("price", 0),
        "marketCapBillionKrw": (raw.get("marketCap") or 0) / 1e8,
        "roe": raw.get("ROE") or 0,
        "per": raw.get("PER") or 0,
        "pbr": raw.get("PBR") or 0,
        "ebitMargin": raw.get("opMargin") or 0,
        "salesGrowth": raw.get("salesGrowth") or 0,
        "operatingProfitGrowth": raw.get("opIncomeGrowth") or 0,
        "salesCagr3Y": raw.get("salesCagr3Y") or 0,
        "EPSGrowth3Y": raw.get("EPSGrowth3Y") or 0,
        "debtRatio": raw.get("debtRatio") or 0,
        "dividendYield": raw.get("divYield") or 0,
        "newsScore": raw.get("newsMomentumScore") or 0,
        "hypothesis": hyp,
        "evidence": ev,
        "risk": raw.get("risk") or "",
        "news": {"hypothesis": hyp, "evidence": ev},
        "financialScore": 0,
        "score": 0,
    }

# ── 샘플 선택 (업종 다양성 우선) ──────────────────────────────────────────────
def pick_samples(items: list, n: int = 50) -> list:
    """다양한 업종·재무 특성을 가진 샘플 n개를 선택."""
    # 핵심 재무 지표가 어느 정도 있는 항목만 포함
    valid = [
        it for it in items
        if (it.get("salesGrowth") or 0) != 0
        and (it.get("opIncomeGrowth") or 0) != 0
        and (it.get("opMargin") or 0) > 0
        and (it.get("ROE") or 0) > 0
    ]

    # 업종별로 하나씩 선택 (다양성 확보)
    seen = set()
    selected = []
    for it in valid:
        ind = it.get("industryName", "")
        if ind not in seen:
            seen.add(ind)
            selected.append(it)
        if len(selected) >= n:
            break

    # 부족하면 나머지에서 추가
    if len(selected) < n:
        for it in valid:
            if it not in selected:
                selected.append(it)
            if len(selected) >= n:
                break

    return selected[:n]

# ── QA 경고 감지 ───────────────────────────────────────────────────────────────
FORBIDDEN_PHRASES = [
    "폭발적 성장", "초대박", "무조건 상승", "급등 확정", "반드시 오른다",
]

def check_warnings(fact: str, impact: str, valuation: str, decision: str) -> list:
    """반복·금지·길이 경고를 감지해 리스트로 반환."""
    warns = []
    full = f"{fact} {impact} {valuation} {decision}"

    # 1. fact↔businessImpact 수치 반복
    nums_fact = set(re.findall(r"\d+\.?\d*%", fact))
    nums_impact = set(re.findall(r"\d+\.?\d*%", impact))
    overlap = nums_fact & nums_impact
    if overlap:
        warns.append(f"[숫자반복] fact↔businessImpact 공통 수치: {', '.join(sorted(overlap))}")

    # 2. valuation↔decision 표현 반복
    key_terms = ["저평가", "보유", "매수", "성장", "per", "pbr", "roe"]
    dup = [t for t in key_terms if t in valuation.lower() and t in decision.lower()]
    if len(dup) >= 2:
        warns.append(f"[표현반복] valuation↔decision 중복 키워드: {', '.join(dup)}")

    # 3. "다만" 과도 반복
    cnt = full.count("다만")
    if cnt >= 2:
        warns.append(f"[구조반복] '다만' {cnt}회 등장")

    # 4. 금지 표현
    for phrase in FORBIDDEN_PHRASES:
        if phrase in full:
            warns.append(f"[금지표현] '{phrase}' 포함")

    # 5. 길이 검사
    for fname, text in [("fact", fact), ("businessImpact", impact),
                        ("valuation", valuation), ("decision", decision)]:
        if len(text) < 20:
            warns.append(f"[길이부족] {fname}: {len(text)}자 (권장 20자+)")
        elif len(text) > 200:
            warns.append(f"[길이초과] {fname}: {len(text)}자 (권장 200자-)")

    return warns

# ── 출력 헬퍼 ─────────────────────────────────────────────────────────────────
SEP_H = "=" * 72
SEP_L = "-" * 72

def sf(v) -> float:
    try:
        return float(v) if v else 0.0
    except Exception:
        return 0.0

def print_sample(idx: int, total: int, raw: dict, norm: dict) -> None:
    fact = build_fact_phrase(norm)
    impact = build_business_impact_phrase(norm)
    valuation = build_valuation_phrase(norm)
    decision = build_decision_phrase(norm)

    name = raw.get("corpName", "")
    code = raw.get("symbol", "")
    industry = raw.get("industryName", "")

    print(SEP_H)
    print(f"[{idx}/{total}] {name} ({code}) | {industry}")
    print(SEP_H)
    print(f"  ROE {sf(raw.get('ROE')):.1f}%  "
          f"PER {sf(raw.get('PER')):.1f}배  "
          f"PBR {sf(raw.get('PBR')):.2f}배  "
          f"매출성장 {sf(raw.get('salesGrowth')):.1f}%  "
          f"영업이익성장 {sf(raw.get('opIncomeGrowth')):.1f}%  "
          f"영업이익률 {sf(raw.get('opMargin')):.1f}%"
          + (f"  CAGR(3Y) {sf(raw.get('salesCagr3Y')):.1f}%" if raw.get("salesCagr3Y") else ""))
    print()
    print(f"  fact           ({len(fact):3d}자): {fact}")
    print(f"  businessImpact ({len(impact):3d}자): {impact}")
    print(f"  valuation      ({len(valuation):3d}자): {valuation}")
    print(f"  decision       ({len(decision):3d}자): {decision}")
    print()

    warnings = check_warnings(fact, impact, valuation, decision)
    if warnings:
        print(f"  [경고] {len(warnings)}건")
        for w in warnings:
            print(f"    * {w}")
    else:
        print("  [경고] 없음")

    print(SEP_L)
    print()

# ── 메인 ──────────────────────────────────────────────────────────────────────
def main() -> None:
    # 데이터 파일 확인
    if not UNIVERSE_FILE.exists():
        print(f"[오류] 데이터 파일 없음: {UNIVERSE_FILE}")
        print("       kr-stock-agent-data-new 디렉터리에서 실행하세요.")
        sys.exit(1)

    print(f"[QA 하네스] 와바바 추천 리포트 문장 품질 검수")
    print(f"데이터: {UNIVERSE_FILE.name}")

    with open(UNIVERSE_FILE, encoding="utf-8") as f:
        raw_data = json.load(f)

    items = raw_data.get("data", raw_data) if isinstance(raw_data, dict) else raw_data
    print(f"전체 종목: {len(items):,}개  →  업종별 다양 샘플 50개 선택")
    print()

    samples = pick_samples(items, n=50)
    if not samples:
        print("[오류] 재무 데이터가 있는 항목을 찾을 수 없습니다.")
        sys.exit(1)

    warn_total = 0
    impact_counter: Counter = Counter()
    decision_counter: Counter = Counter()
    valuation_counter: Counter = Counter()
    impact_to_names: dict = {}
    decision_to_names: dict = {}
    valuation_to_names: dict = {}
    for i, raw_item in enumerate(samples, 1):
        norm = normalize(raw_item)
        print_sample(i, len(samples), raw_item, norm)
        # 경고 집계
        fact = build_fact_phrase(norm)
        impact = build_business_impact_phrase(norm)
        valuation = build_valuation_phrase(norm)
        decision = build_decision_phrase(norm)
        warn_total += len(check_warnings(fact, impact, valuation, decision))
        impact_counter[impact] += 1
        decision_counter[decision] += 1
        valuation_counter[valuation] += 1
        name = raw_item.get("corpName", "")
        impact_to_names.setdefault(impact, []).append(name)
        decision_to_names.setdefault(decision, []).append(name)
        valuation_to_names.setdefault(valuation, []).append(name)

    # 동일 businessImpact / decision / valuation 반복 감지 (콘솔 출력 전용)
    dup_total = sum(c for c in impact_counter.values() if c >= 2)
    dup_groups = sum(1 for c in impact_counter.values() if c >= 2)
    dec_dup_total = sum(c for c in decision_counter.values() if c >= 2)
    dec_dup_groups = sum(1 for c in decision_counter.values() if c >= 2)
    val_dup_total = sum(c for c in valuation_counter.values() if c >= 2)
    val_dup_groups = sum(1 for c in valuation_counter.values() if c >= 2)
    print(f"QA 완료 | {len(samples)}개 샘플 검수 | 총 경고 {warn_total}건"
          f" | 동일 businessImpact 반복 {dup_total}건({dup_groups}그룹)"
          f" | 동일 decision 반복 {dec_dup_total}건({dec_dup_groups}그룹)"
          f" | 동일 valuation 반복 {val_dup_total}건({val_dup_groups}그룹)")
    # 반복 그룹은 상위 5개까지만 상세 출력 (대표 종목명 3개 동반)
    for txt, cnt in impact_counter.most_common(5):
        if cnt >= 2:
            names = ", ".join(impact_to_names.get(txt, [])[:3])
            print(f"  [BI 반복x{cnt}] ({names}) {txt[:80]}...")
    for txt, cnt in decision_counter.most_common(5):
        if cnt >= 2:
            names = ", ".join(decision_to_names.get(txt, [])[:3])
            print(f"  [DEC 반복x{cnt}] ({names}) {txt[:80]}...")
    for txt, cnt in valuation_counter.most_common(5):
        if cnt >= 2:
            names = ", ".join(valuation_to_names.get(txt, [])[:3])
            print(f"  [VAL 반복x{cnt}] ({names}) {txt[:80]}...")


if __name__ == "__main__":
    main()
