"""ttm_core — DART 분기보고서 기반 단일분기 복원 / TTM 계산 순수 로직 (Phase MF-TTM-DART-20-STOCK-POC).

네트워크·파일 IO 없음. quarterly_ttm_poc.py 가 이 모듈을 사용한다.
테스트(test_ttm_core.py)는 fixture row-set 으로 이 함수들을 직접 검증한다.

핵심 개념
- DART fnlttSinglAcntAll 응답의 손익계산서(IS/CIS) 계정은 보고서별로 "누적(cumulative)" 손익을 담는다.
  * 11013 1분기보고서  = 1분기 누적 (= 1~3월)
  * 11012 반기보고서    = 반기 누적 (= 1~6월)
  * 11014 3분기보고서   = 3분기 누적 (= 1~9월)
  * 11011 사업보고서    = 연간 (= 1~12월)
- 12월 결산 기준 단일분기:
  * Q1 = cum(11013)
  * Q2 = cum(11012) - cum(11013)
  * Q3 = cum(11014) - cum(11012)
  * Q4 = cum(11011) - cum(11014)
- 재무상태표(BS) 계정은 "최신 분기 말 시점값"이며 절대 누적 합산하지 않는다.
"""
from __future__ import annotations

from typing import Any, Optional

ANNUAL_REPORT_CODE = "11011"
Q1_REPORT_CODE = "11013"
HALF_REPORT_CODE = "11012"
Q3_REPORT_CODE = "11014"

# 손익계산서 계정: 반드시 sj_div ∈ {IS, CIS} 에서만 추출(BS 동명 계정 오염 방지)
IS_SJ_DIV = {"IS", "CIS"}
BS_SJ_DIV = {"BS"}

# 필수 계정 후보(account_id 우선, 그다음 account_nm). normalize 후 비교.
IS_ACCOUNTS = {
    "revenue": ["ifrs-full_Revenue", "매출액", "수익(매출액)", "영업수익"],
    "operatingIncome": [
        "dart_OperatingIncomeLoss",
        "ifrs-full_ProfitLossFromOperatingActivities",
        "영업이익",
        "영업이익(손실)",
    ],
    # 주의(MF-ITOOZA-LOGIN-READONLY-7STOCK-VERIFY, 2026-07-24): 여기의 "netIncome"은
    # 총계(지배주주+비지배주주) 당기순이익(ifrs-full_ProfitLoss)이다. 아이투자 등 공개 재무
    # 사이트가 표시하는 "순이익(지배)"는 별도 계정(ifrs-full_ProfitLossAttributableToOwnersOfParent,
    # 지배주주순이익)으로, 비지배지분이 큰 종목(예: 자회사를 별도 상장한 대기업)에서는 두 값이
    # 크게 다르거나 부호까지 달라질 수 있다(둘 다 정상적인 DART 원자료, 오류 아님). 외부(아이투자/
    # 언론) "순이익" 표시값과 1:1 대조 시 이 정의 차이를 감안할 것 — 아이투자 값으로 이 필드를
    # 덮어쓰거나 재계산하지 않는다.
    "netIncome": ["ifrs-full_ProfitLoss", "당기순이익", "당기순이익(손실)", "연결당기순이익"],
}
BS_ACCOUNTS = {
    "currentAssets": ["ifrs-full_CurrentAssets", "유동자산"],
    "currentLiabilities": ["ifrs-full_CurrentLiabilities", "유동부채"],
    "ppe": ["ifrs-full_PropertyPlantAndEquipment", "유형자산"],
    "cash": [
        "ifrs-full_CashAndCashEquivalents",
        "현금및현금성자산",
        "현금및현금성자산및단기금융상품",
    ],
    "totalDebt": ["ifrs-full_Liabilities", "부채총계"],
}


def safe_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            f = float(value)
        except Exception:
            return None
        return f
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace(" ", "").replace("　", "")
        if cleaned in ("", "-", "N/A", "n/a"):
            return None
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = f"-{cleaned[1:-1]}"
        try:
            return float(cleaned)
        except Exception:
            return None
    return None


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_account_text(value: Any) -> str:
    text = safe_text(value).lower()
    for ch in (" ", "-", "_", "(", ")", "　"):
        text = text.replace(ch, "")
    return text


def _match_row(rows: list[dict], sj_set: set[str], candidates: list[str]) -> Optional[dict]:
    """sj_div 가 sj_set 에 속하는 row 중 candidates(account_id/nm) 우선순위로 첫 매치 반환."""
    norm_candidates = [normalize_account_text(c) for c in candidates]
    # 후보 우선순위대로: 더 앞선 후보가 이기도록 순차 탐색
    for cand in norm_candidates:
        for row in rows:
            if safe_text(row.get("sj_div")) not in sj_set:
                continue
            aid = normalize_account_text(row.get("account_id"))
            anm = normalize_account_text(row.get("account_nm"))
            if cand and (cand == aid or cand == anm):
                return row
    return None


def cumulative_is_value(rows: list[dict], candidates: list[str], reprt_code: str) -> dict:
    """손익계산서 계정의 '누적' 금액을 뽑는다.

    반환 dict: value / field / accountId / accountName / found
    - 연간(11011): thstrm_amount = 연간 누적
    - 분기(11013/11012/11014): thstrm_add_amount(누적) 우선.
      * 1분기(11013)는 add_amount 부재 시 thstrm_amount(=3개월=누적) 사용 안전.
      * 반기/3분기는 add_amount 부재 시 thstrm_amount(=당해 분기 3개월)로 fallback하되
        'ambiguous' 플래그로 표시(누적이 아닐 수 있어 검증에서 경고 대상).
    """
    row = _match_row(rows, IS_SJ_DIV, candidates)
    if row is None:
        return {"value": None, "field": None, "found": False, "ambiguous": False}

    add = safe_number(row.get("thstrm_add_amount"))
    amt = safe_number(row.get("thstrm_amount"))
    account_id = safe_text(row.get("account_id"))
    account_name = safe_text(row.get("account_nm"))

    if reprt_code == ANNUAL_REPORT_CODE:
        value = amt if amt is not None else add
        field = "thstrm_amount" if amt is not None else "thstrm_add_amount"
        ambiguous = False
    else:
        if add is not None:
            value, field, ambiguous = add, "thstrm_add_amount", False
        elif reprt_code == Q1_REPORT_CODE:
            # 1분기 3개월 = 1분기 누적
            value, field, ambiguous = amt, "thstrm_amount", False
        else:
            # 반기/3분기에서 누적필드 부재 → 3개월값을 누적으로 오인할 위험
            value, field, ambiguous = amt, "thstrm_amount", True

    return {
        "value": value,
        "field": field,
        "found": value is not None,
        "ambiguous": ambiguous,
        "accountId": account_id,
        "accountName": account_name,
        "reprtCode": reprt_code,
    }


def prior_year_quarter_value(rows: list[dict], candidates: list[str]) -> Optional[float]:
    """손익계정의 전년 동기(당분기 기준) 값. DART frmtrm_q_amount(분기) → frmtrm_amount fallback.

    YoY 교차검증용. 분기보고서 응답에는 전년 동분기(frmtrm_q_amount)가 함께 온다.
    """
    row = _match_row(rows, IS_SJ_DIV, candidates)
    if row is None:
        return None
    for key in ("frmtrm_q_amount", "frmtrm_add_amount", "frmtrm_amount"):
        v = safe_number(row.get(key))
        if v is not None:
            return v
    return None


# TTM(2026Q1 종료) 윈도우 중 2025 회계연도에 속하는 단일분기(연간과 같은 해라 비교 가능)
SAME_YEAR_2025_QUARTERS = ["2025Q4", "2025Q3", "2025Q2"]


def sanity_flags_single_quarter(single_by_metric: dict, fy_by_metric: dict,
                                yoy_by_metric: dict, yoy_threshold: float = 3.0) -> list[str]:
    """단일분기 값의 타당성 검사(오탐 최소화, 방어적).

    규칙 A(매출 불가): 매출은 음수 불가, 그리고 '같은 회계연도(2025)' 단일분기 매출이 그 해 연간 매출을
        초과할 수 없다(누적 단조). 위반 시 데이터/복원 오류 → '불가'.
        * 손익(영업이익/순이익)은 분기별 부호가 바뀔 수 있어 |분기|>|연간|이 정상적으로 성립하므로
          '불가' 규칙을 적용하지 않는다(오탐 방지).
    규칙 B(YoY 급변, 검산 필요): 최신 분기(2026Q1)가 전년 동기 대비 threshold 배 초과 변동하거나
        흑↔적 부호가 바뀌면 '불가'가 아니라 '검산 필요'로 분류(아이투자 검산 Phase 대상).
        * 2026Q1 은 2026 회계연도라 2025 연간과 직접 비교할 수 없다 → 오직 전년 동기(frmtrm_q)와만 비교.
    """
    flags = []

    # 규칙 A: 매출 전용 (같은 해 2025 분기만)
    rev = single_by_metric.get("revenue", {})
    fy_rev = fy_by_metric.get("revenue")
    for q in SAME_YEAR_2025_QUARTERS:
        v = rev.get(q)
        if v is None:
            continue
        if v < 0:
            flags.append(f"[불가] revenue {q}={_fmt(v)} < 0 (매출 음수)")
        elif fy_rev is not None and fy_rev > 0 and v > fy_rev * 1.02:
            flags.append(f"[불가] revenue {q}={_fmt(v)} > FY2025={_fmt(fy_rev)} (분기>연간)")

    # 규칙 B: YoY 급변(2026Q1) — 전 metric, '검산 필요'
    for metric, singles in single_by_metric.items():
        prior = yoy_by_metric.get(metric)
        q26 = singles.get("2026Q1")
        if prior is None or q26 is None or prior == 0:
            continue
        sign_flip = (q26 < 0) != (prior < 0)
        growth = (q26 - prior) / abs(prior)
        if sign_flip:
            flags.append(f"[검산] {metric} 2026Q1 흑↔적 전환 (전년동기 {_fmt(prior)}→{_fmt(q26)})")
        elif abs(growth) > yoy_threshold:
            flags.append(f"[검산] {metric} 2026Q1 YoY {growth*100:.0f}% (전년동기 {_fmt(prior)}→{_fmt(q26)}) 급변")
    return flags


# 손익계산서 내부일관성 검증용 추가 계정
COGS_ACCOUNTS = ["ifrs-full_CostOfSales", "매출원가"]
GROSS_PROFIT_ACCOUNTS = ["ifrs-full_GrossProfit", "매출총이익", "매출총이익(손실)"]


def income_statement_consistency(rows: list[dict], tol: float = 0.02) -> dict:
    """손익계산서 상단 계층 정합성 검증(같은 DART 행들의 내부 일관성).

    극단적으로 큰 실적이라도 아래 항등/부등식이 성립하면 '데이터 오류'가 아니라 '실제 큰 값'으로 본다.
    (대장 지시: 동일 DART 행의 내부일관성이 확인되면 실제 실적으로 인정)
      - 매출 = 매출원가 + 매출총이익            (있을 때, tol 이내)
      - 영업이익 <= 매출총이익                   (매출총이익 양수일 때)
      - 매출총이익 <= 매출                       (매출 양수일 때)
    계정이 없어 검사 못 하면 해당 항목은 skip(정합 간주). 전부 skip이면 consistent=True, checkedCount=0.
    """
    R = cumulative_is_value(rows, IS_ACCOUNTS["revenue"], ANNUAL_REPORT_CODE)["value"]
    OP = cumulative_is_value(rows, IS_ACCOUNTS["operatingIncome"], ANNUAL_REPORT_CODE)["value"]
    COGS = cumulative_is_value(rows, COGS_ACCOUNTS, ANNUAL_REPORT_CODE)["value"]
    GP = cumulative_is_value(rows, GROSS_PROFIT_ACCOUNTS, ANNUAL_REPORT_CODE)["value"]

    # GrossProfit 태그 오용 감지: 정상 기업은 매출총이익 < 매출(GP = 매출 - 매출원가).
    # GP 가 매출과 사실상 같거나 크면(GP >= R*(1-tol)) XBRL 태깅에서 매출액이 ifrs-full_GrossProfit 에
    # 잘못 붙은 공시(일부 기업 태깅 특이). 이때 GP 기반 검사(R=COGS+GP 등)는 무의미하므로 스킵한다.
    # 매출 자체(R)는 유효하며 누적 단조·연간 역산으로 별도 검증된다. 종목 무관 일반 규칙.
    gp_tag_suspect = (R is not None and GP is not None and R > 0 and GP >= R * (1 - tol))

    checks = []
    ok = True
    if R is not None and COGS is not None and GP is not None and not gp_tag_suspect:
        c = abs(R - (COGS + GP)) <= abs(R) * tol
        checks.append(("revenue=COGS+grossProfit", c))
        ok = ok and c
    if GP is not None and OP is not None and GP >= 0 and not gp_tag_suspect:
        c = OP <= abs(GP) * (1 + tol)
        checks.append(("operatingIncome<=grossProfit", c))
        ok = ok and c
    if R is not None and GP is not None and R > 0 and not gp_tag_suspect:
        c = GP <= R * (1 + tol)
        checks.append(("grossProfit<=revenue", c))
        ok = ok and c
    return {"consistent": ok, "checks": checks, "checkedCount": len(checks),
            "grossProfitTagSuspect": gp_tag_suspect}


# 게이트 임계값 기본값(config gateThresholds 로 override). 코드 산발 하드코딩 방지용 단일 출처.
DEFAULT_GATE_THRESHOLDS = {
    "largeAbsOperatingIncomeKrw": 200_000_000_000,  # 2000억: '절대 규모 큼' 경계(100종 분포상 자연 갭)
    "yoyExtremePct": 3.0,                            # 전년동기 대비 ±300%
    "qoqExtremePct": 2.0,                            # 전분기 대비 +200%
}


def outlier_flags(single_by_metric: dict, fy_by_metric: dict, yoy_by_metric: dict,
                  thresholds: dict | None = None) -> dict:
    """이상치를 4등급으로 분리한다(흑↔적 단독을 외부검산에서 제외하는 튜닝 반영).

    revenueHard(→ BLOCKED): 매출 음수, 또는 같은 회계연도 분기 매출 > 그 해 연간(물리 불가).
    significantExtreme(→ WARNING/IR): 절대 규모가 큰(|2026Q1 영업익 또는 순익| >= largeAbs) 상태에서
        극단신호(흑↔적 전환 / 분기>직전연간 / YoY 급변 / QoQ 급증)가 하나라도 있음.
        → '절대 규모 + 극단'일 때만 외부확인 대상(삼성·SK·POSCO 같은 대형 극단).
    transitions(→ PASS_WITH_TRANSITION_NOTE): 흑↔적 전환(정상 이벤트). 절대 규모가 작으면 여기에만 남아 정보 기록.
    minorFlags(→ PASS): 분기>연간/YoY/QoQ 급변이지만 절대 규모가 작음(경미, 정보).

    핵심: '흑↔적 전환만' 또는 '비율 급변만'으로는 WARNING 을 만들지 않는다. 절대 규모를 함께 본다.
    시가총액이 아니라 '영업이익/순이익의 절대 원화 규모'를 기준으로 한다(시총은 진실성 기준 아님).
    """
    t = {**DEFAULT_GATE_THRESHOLDS, **(thresholds or {})}
    large_abs = float(t["largeAbsOperatingIncomeKrw"])
    yoy_th = float(t["yoyExtremePct"])
    qoq_th = float(t["qoqExtremePct"])

    revenue_hard, significant, transitions, minor = [], [], [], []

    rev = single_by_metric.get("revenue", {})
    fy_rev = fy_by_metric.get("revenue")
    for q in SAME_YEAR_2025_QUARTERS:
        v = rev.get(q)
        if v is None:
            continue
        if v < 0:
            revenue_hard.append(f"revenue {q}={_fmt(v)} < 0 (매출 음수)")
        elif fy_rev is not None and fy_rev > 0 and v > fy_rev * 1.02:
            revenue_hard.append(f"revenue {q}={_fmt(v)} > FY2025={_fmt(fy_rev)} (같은해 분기>연간)")

    # 절대 규모: 손익 2026Q1 절대값 최대치로 판정
    q26_abs_max = max(
        (abs(single_by_metric.get(m, {}).get("2026Q1"))
         for m in ("operatingIncome", "netIncome")
         if single_by_metric.get(m, {}).get("2026Q1") is not None),
        default=0.0,
    )
    is_large = q26_abs_max >= large_abs

    for metric in ("operatingIncome", "netIncome"):
        singles = single_by_metric.get(metric, {})
        fy = fy_by_metric.get(metric)
        q26 = singles.get("2026Q1")
        q25q4 = singles.get("2025Q4")
        prior = yoy_by_metric.get(metric)

        signals = []            # 이 metric 의 극단신호 목록
        if fy is not None and fy > 0 and q26 is not None and q26 > fy * 1.02:
            signals.append(f"{metric} 2026Q1={_fmt(q26)} > 직전연간 {_fmt(fy)}")
        if prior is not None and prior != 0 and q26 is not None:
            flip = (q26 < 0) != (prior < 0)
            growth = (q26 - prior) / abs(prior)
            if flip:
                transitions.append(f"{metric} 2026Q1 흑↔적 전환 ({_fmt(prior)}→{_fmt(q26)})")
                signals.append(f"{metric} 흑↔적")
            elif abs(growth) > yoy_th:
                signals.append(f"{metric} YoY {growth*100:.0f}%")
        if q25q4 is not None and q25q4 != 0 and q26 is not None:
            qoq = (q26 - q25q4) / abs(q25q4)
            if not ((q26 < 0) != (q25q4 < 0)) and abs(qoq) > qoq_th:
                signals.append(f"{metric} QoQ {qoq*100:.0f}%")

        for s in signals:
            if is_large:
                significant.append(f"[규모큼] {s} (|2026Q1|max={_fmt(q26_abs_max)}>=임계 {_fmt(large_abs)})")
            elif "흑↔적" not in s:
                minor.append(f"[경미] {s} (절대규모 작음)")

    return {"revenueHard": revenue_hard, "significantExtreme": significant,
            "transitions": transitions, "minorFlags": minor}


def match_official_ir(code: str, quarter: str, dart_values: dict, confirmations: list) -> dict:
    """공식 IR 확인 데이터(config)와 DART 단일분기 값을 종목-불문 일반 로직으로 대조.

    confirmations 원소: {code, quarter, metrics:{metric:officialValue}, tolerancePct, source, checkedAt}
    코드에 종목/숫자 하드코딩 없음 — 외부 검증 입력(config)만 참조한다.
    반환: matched(모든 대조 metric 이 허용오차 내) / diffs / source / matchedMetrics
    """
    for c in confirmations:
        if c.get("code") != code or c.get("quarter") != quarter:
            continue
        tol = float(c.get("tolerancePct", 0.01))
        diffs = {}
        matched_metrics = []
        all_ok = True
        for metric, official in (c.get("metrics") or {}).items():
            dv = dart_values.get(metric)
            offv = safe_number(official)
            if dv is None or offv is None or offv == 0:
                all_ok = False
                continue
            d = abs(dv - offv) / abs(offv)
            diffs[metric] = d
            if d <= tol:
                matched_metrics.append(metric)
            else:
                all_ok = False
        return {
            "matched": all_ok and len(matched_metrics) > 0,
            "diffs": diffs,
            "matchedMetrics": matched_metrics,
            "source": c.get("source"),
            "checkedAt": c.get("checkedAt"),
            "tolerancePct": tol,
        }
    return {"matched": False, "diffs": {}, "matchedMetrics": [], "source": None}


# 게이트 상태
STATUS_PASS = "PASS"
STATUS_PASS_TRANSITION = "PASS_WITH_TRANSITION_NOTE"
STATUS_PASS_IR = "PASS_OFFICIAL_IR_CONFIRMED"
STATUS_WARN_EXT = "WARNING_EXTERNAL_CONFIRMATION"
STATUS_BLOCKED = "BLOCKED_DATA_INCONSISTENCY"


def classify_ttm_quality(*, has_corp: bool, has_anchor: bool, missing_reports: bool,
                         ttm_complete: bool, annual_reconstructable: bool,
                         internal_consistent: bool, revenue_hard: bool,
                         significant_extreme: bool, transition: bool,
                         restatement: bool, ir_matched: bool) -> str:
    """5단계 게이트(순수 함수). 흑↔적 전환 단독을 외부검산에서 제외하는 튜닝 반영.

    우선순위:
      1) 매핑/앵커 실패, 매출 물리불가, 손익 내부일관성 실패 → BLOCKED_DATA_INCONSISTENCY
      2) 보고서 누락/역산 불가/TTM 미완성 → WARNING_EXTERNAL_CONFIRMATION
      3) 절대 규모 큰 극단(significant):
           - 공식 IR 일치 → PASS_OFFICIAL_IR_CONFIRMED
           - 미확인       → WARNING_EXTERNAL_CONFIRMATION
      4) 흑↔적 전환만(절대 규모 작음) → PASS_WITH_TRANSITION_NOTE (정상 이벤트, 외부확인 불필요)
      5) 그 외(경미 급변 포함) 정상 → PASS

    restatement(전년동기 비교표시 불일치)는 '당기값 기반 TTM'에 영향이 없어 상태를 낮추지 않고 정보로만 기록.
    """
    if not has_corp or not has_anchor:
        return STATUS_BLOCKED
    if revenue_hard or not internal_consistent:
        return STATUS_BLOCKED
    if missing_reports or not ttm_complete or not annual_reconstructable:
        return STATUS_WARN_EXT
    if significant_extreme:
        return STATUS_PASS_IR if ir_matched else STATUS_WARN_EXT
    if transition:
        return STATUS_PASS_TRANSITION
    return STATUS_PASS


def snapshot_bs_value(rows: list[dict], candidates: list[str], reprt_code: str) -> dict:
    """재무상태표 계정의 '최신 시점값'(thstrm_amount). 누적 아님."""
    row = _match_row(rows, BS_SJ_DIV, candidates)
    if row is None:
        return {"value": None, "field": None, "found": False}
    amt = safe_number(row.get("thstrm_amount"))
    return {
        "value": amt,
        "field": "thstrm_amount",
        "found": amt is not None,
        "accountId": safe_text(row.get("account_id")),
        "accountName": safe_text(row.get("account_nm")),
        "reprtCode": reprt_code,
    }


def reconstruct_single_quarters(cum: dict) -> dict:
    """누적 손익 dict → 단일분기 dict.

    입력 cum: 키 = (year, reprt_code), 값 = 누적 금액(float|None)
    12월 결산 기준. 최근 4개 단일분기(2026Q1,2025Q4,2025Q3,2025Q2)를 계산한다.
    각 세그먼트 결과: {value, method, formulaTrace, complete}
    """
    def g(key):
        return cum.get(key)

    def diff(minuend_key, subtrahend_key, label, minuend_lbl, subtrahend_lbl):
        a = g(minuend_key)
        b = g(subtrahend_key)
        if a is None or b is None:
            return {
                "value": None,
                "method": "diff",
                "complete": False,
                "formulaTrace": f"{label} = {minuend_lbl}({_fmt(a)}) - {subtrahend_lbl}({_fmt(b)})",
            }
        return {
            "value": a - b,
            "method": "diff",
            "complete": True,
            "formulaTrace": f"{label} = {minuend_lbl}({_fmt(a)}) - {subtrahend_lbl}({_fmt(b)}) = {_fmt(a - b)}",
        }

    def direct(key, label, src_lbl):
        v = g(key)
        return {
            "value": v,
            "method": "direct",
            "complete": v is not None,
            "formulaTrace": f"{label} = {src_lbl}({_fmt(v)})",
        }

    return {
        "2026Q1": direct((2026, Q1_REPORT_CODE), "2026Q1", "cum2026Q1"),
        "2025Q4": diff((2025, ANNUAL_REPORT_CODE), (2025, Q3_REPORT_CODE), "2025Q4", "cumFY2025", "cum2025Q3"),
        "2025Q3": diff((2025, Q3_REPORT_CODE), (2025, HALF_REPORT_CODE), "2025Q3", "cum2025Q3", "cum2025H1"),
        "2025Q2": diff((2025, HALF_REPORT_CODE), (2025, Q1_REPORT_CODE), "2025Q2", "cum2025H1", "cum2025Q1"),
    }


def assemble_ttm(single_quarters: dict) -> dict:
    """단일분기 4개 합 = TTM. 하나라도 미완성이면 partial."""
    order = ["2026Q1", "2025Q4", "2025Q3", "2025Q2"]
    values = []
    complete = True
    for q in order:
        seg = single_quarters.get(q, {})
        if not seg.get("complete"):
            complete = False
        values.append(seg.get("value"))
    ttm = None
    if complete and all(v is not None for v in values):
        ttm = sum(values)
    return {
        "ttm": ttm,
        "complete": complete,
        "quarters": order,
        "values": values,
    }


def validate_annual_reconstruction(cum: dict, tolerance_krw: float = 1.0) -> dict:
    """2025 단일분기 Q1..Q4 합 == 2025 연간 누적 여부.

    Q1+Q2+Q3+Q4 는 누적차분 구조상 연간으로 telescoping 되므로,
    4개 2025 보고서가 모두 있으면 정확히 일치한다. 이 검증의 실제 의미는
    '4개 보고서 확보 여부 + 누적값 결측 없음'이다. 결측 시 reconstructable=False.
    """
    q1 = cum.get((2025, Q1_REPORT_CODE))
    h1 = cum.get((2025, HALF_REPORT_CODE))
    q3c = cum.get((2025, Q3_REPORT_CODE))
    fy = cum.get((2025, ANNUAL_REPORT_CODE))
    if None in (q1, h1, q3c, fy):
        return {"reconstructable": False, "diff": None, "match": False,
                "missing": [k for k, v in [("Q1", q1), ("H1", h1), ("Q3", q3c), ("FY", fy)] if v is None]}
    q2 = h1 - q1
    q3 = q3c - h1
    q4 = fy - q3c
    recomposed = q1 + q2 + q3 + q4
    diff = recomposed - fy
    return {
        "reconstructable": True,
        "diff": diff,
        "match": abs(diff) <= tolerance_krw,
        "singles": {"Q1": q1, "Q2": q2, "Q3": q3, "Q4": q4},
    }


def check_monotonic_cumulative(cum_by_reprt: dict) -> dict:
    """매출 등 누적은 Q1 <= H1 <= 3Q <= FY 여야 정상(비감소). 위반 시 정정공시/데이터 이상 의심."""
    seq = [
        ("Q1", cum_by_reprt.get((2025, Q1_REPORT_CODE))),
        ("H1", cum_by_reprt.get((2025, HALF_REPORT_CODE))),
        ("Q3", cum_by_reprt.get((2025, Q3_REPORT_CODE))),
        ("FY", cum_by_reprt.get((2025, ANNUAL_REPORT_CODE))),
    ]
    present = [(k, v) for k, v in seq if v is not None]
    violations = []
    for i in range(1, len(present)):
        if present[i][1] < present[i - 1][1] - 1.0:
            violations.append(f"{present[i-1][0]}({_fmt(present[i-1][1])}) > {present[i][0]}({_fmt(present[i][1])})")
    return {"monotonic": not violations, "violations": violations}


def quarterly_cache_filename(corp_code: str, year: int, reprt_code: str, fs_div: str) -> str:
    """분기 캐시 파일명. 기존 연간 캐시({corp}_{year}_{fs_div}.json)와 충돌하지 않도록 reprt_code 포함."""
    return f"{corp_code}_{year}_{reprt_code}_{fs_div}.json"


def _fmt(v) -> str:
    if v is None:
        return "None"
    try:
        return f"{v:,.0f}"
    except Exception:
        return str(v)
