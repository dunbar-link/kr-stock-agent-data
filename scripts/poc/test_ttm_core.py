"""test_ttm_core — ttm_core 순수 로직 오프라인 검증 (Phase MF-TTM-DART-20-STOCK-POC).

네트워크·파일 write 0. fixture row-set 으로 단일분기 복원/TTM/BS 스냅샷/혼용방지/결정론을 검증한다.
실행:  python scripts/poc/test_ttm_core.py   (exit 0 = 전부 PASS)

필수 테스트(지시문 Loop4) 매핑:
 1 정상 12월 결산 CFS        → test_normal_cfs_ttm
 2 OFS fallback              → test_ofs_labeling
 3 보고서 1개 누락           → test_missing_one_report
 4 Q2 = 반기 - 1분기          → test_q2_diff
 5 Q3 = 3분기 - 반기          → test_q3_diff
 6 Q4 = 연간 - 3분기          → test_q4_diff
 7 음수 영업이익             → test_negative_operating_income
 8 account 명칭 차이         → test_account_name_variants
 9 연결/별도 혼용 차단        → test_no_fs_div_mixing
10 동일 입력 재실행 결정론    → test_determinism
11 기존 연간 캐시 무변경(파일명 분리) → test_cache_filename_separation
"""
from __future__ import annotations

import sys

import ttm_core as C

_PASS = 0
_FAIL = 0


def check(name, cond, detail=""):
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def is_row(account_id, account_nm, *, amount=None, add=None, sj="IS"):
    return {
        "sj_div": sj, "account_id": account_id, "account_nm": account_nm,
        "thstrm_amount": amount, "thstrm_add_amount": add,
    }


def bs_row(account_id, account_nm, amount):
    return {"sj_div": "BS", "account_id": account_id, "account_nm": account_nm, "thstrm_amount": amount}


# --- 누적 fixture: 매출 누적 (Q1=100, H1=250, 3Q=430, FY=600) → 단일 100/150/180/170 ---
def cum_reports_revenue():
    return {
        (2025, C.Q1_REPORT_CODE): [is_row("ifrs-full_Revenue", "매출액", amount="100")],
        (2025, C.HALF_REPORT_CODE): [is_row("ifrs-full_Revenue", "매출액", amount="130", add="250")],
        (2025, C.Q3_REPORT_CODE): [is_row("ifrs-full_Revenue", "매출액", amount="180", add="430")],
        (2025, C.ANNUAL_REPORT_CODE): [is_row("ifrs-full_Revenue", "매출액", amount="600")],
        (2026, C.Q1_REPORT_CODE): [is_row("ifrs-full_Revenue", "매출액", amount="120")],
    }


def build_cum(reports, metric="revenue"):
    cands = C.IS_ACCOUNTS[metric]
    return {k: C.cumulative_is_value(rows, cands, k[1])["value"] for k, rows in reports.items()}


def test_normal_cfs_ttm():
    cum = build_cum(cum_reports_revenue())
    singles = C.reconstruct_single_quarters(cum)
    # 2026Q1=120, 2025Q4=600-430=170, 2025Q3=430-250=180, 2025Q2=250-100=150
    check("test1 normal_cfs 2026Q1", singles["2026Q1"]["value"] == 120)
    check("test1 normal_cfs 2025Q4", singles["2025Q4"]["value"] == 170)
    check("test1 normal_cfs 2025Q3", singles["2025Q3"]["value"] == 180)
    check("test1 normal_cfs 2025Q2", singles["2025Q2"]["value"] == 150)
    ttm = C.assemble_ttm(singles)
    check("test1 normal_cfs TTM=620", ttm["ttm"] == 620 and ttm["complete"])


def test_q2_diff():
    cum = build_cum(cum_reports_revenue())
    s = C.reconstruct_single_quarters(cum)
    check("test4 Q2 = H1 - Q1 = 150", s["2025Q2"]["value"] == 150 and s["2025Q2"]["method"] == "diff")


def test_q3_diff():
    cum = build_cum(cum_reports_revenue())
    s = C.reconstruct_single_quarters(cum)
    check("test5 Q3 = 3Q - H1 = 180", s["2025Q3"]["value"] == 180)


def test_q4_diff():
    cum = build_cum(cum_reports_revenue())
    s = C.reconstruct_single_quarters(cum)
    check("test6 Q4 = FY - 3Q = 170", s["2025Q4"]["value"] == 170)
    ann = C.validate_annual_reconstruction(cum)
    check("test6 annual recon match", ann["reconstructable"] and ann["match"], str(ann))


def test_missing_one_report():
    reports = cum_reports_revenue()
    del reports[(2025, C.HALF_REPORT_CODE)]  # 반기 누락
    cum = build_cum(reports)
    s = C.reconstruct_single_quarters(cum)
    # Q2, Q3 는 반기 누적 의존 → 미완성
    check("test3 missing H1 → Q2 incomplete", not s["2025Q2"]["complete"])
    check("test3 missing H1 → Q3 incomplete", not s["2025Q3"]["complete"])
    ttm = C.assemble_ttm(s)
    check("test3 missing H1 → TTM incomplete", ttm["ttm"] is None and not ttm["complete"])
    ann = C.validate_annual_reconstruction(cum)
    check("test3 annual recon not reconstructable", not ann["reconstructable"])


def test_negative_operating_income():
    # 영업이익 누적: Q1=-10, H1=-30, 3Q=-25, FY=-40 → Q3 단일 = -25-(-30)=+5, Q4=-40-(-25)=-15
    reports = {
        (2025, C.Q1_REPORT_CODE): [is_row("dart_OperatingIncomeLoss", "영업이익", amount="-10")],
        (2025, C.HALF_REPORT_CODE): [is_row("dart_OperatingIncomeLoss", "영업이익", amount="-20", add="-30")],
        (2025, C.Q3_REPORT_CODE): [is_row("dart_OperatingIncomeLoss", "영업이익", amount="5", add="-25")],
        (2025, C.ANNUAL_REPORT_CODE): [is_row("dart_OperatingIncomeLoss", "영업이익", amount="-40")],
        (2026, C.Q1_REPORT_CODE): [is_row("dart_OperatingIncomeLoss", "영업이익", amount="-8")],
    }
    cum = build_cum(reports, "operatingIncome")
    s = C.reconstruct_single_quarters(cum)
    check("test7 neg opinc Q2 = -30-(-10) = -20", s["2025Q2"]["value"] == -20)
    check("test7 neg opinc Q3 = -25-(-30) = +5", s["2025Q3"]["value"] == 5)
    check("test7 neg opinc Q4 = -40-(-25) = -15", s["2025Q4"]["value"] == -15)


def test_account_name_variants():
    # account_id 부재, account_nm 만 "수익(매출액)" 형태
    rows = [is_row("", "수익(매출액)", amount="500")]
    info = C.cumulative_is_value(rows, C.IS_ACCOUNTS["revenue"], C.ANNUAL_REPORT_CODE)
    check("test8 name variant '수익(매출액)'", info["found"] and info["value"] == 500, str(info))
    # 괄호/공백 normalize
    rows2 = [is_row("", "영 업 이 익 (손실)", amount="10")]
    info2 = C.cumulative_is_value(rows2, C.IS_ACCOUNTS["operatingIncome"], C.ANNUAL_REPORT_CODE)
    check("test8 name variant spaced 영업이익(손실)", info2["found"] and info2["value"] == 10, str(info2))


def test_no_fs_div_mixing():
    # IS 계정 탐색 시 BS row 는 무시(sj_div 존중) → 손익에 BS 값 오염 안 됨
    rows = [
        bs_row("ifrs-full_Liabilities", "부채총계", "9999"),          # BS
        is_row("ifrs-full_ProfitLoss", "당기순이익", amount="50", sj="IS"),
    ]
    info = C.cumulative_is_value(rows, C.IS_ACCOUNTS["netIncome"], C.ANNUAL_REPORT_CODE)
    check("test9 IS extraction ignores BS rows", info["value"] == 50)
    # BS 추출은 BS row 만
    bs = C.snapshot_bs_value(rows, C.BS_ACCOUNTS["totalDebt"], C.Q1_REPORT_CODE)
    check("test9 BS snapshot from BS row", bs["value"] == 9999)
    # 손익 계정을 BS 후보로 못 찾음(혼용 차단)
    bs_bad = C.snapshot_bs_value(rows, C.BS_ACCOUNTS["currentAssets"], C.Q1_REPORT_CODE)
    check("test9 BS snapshot missing → not found", not bs_bad["found"])


def test_ofs_labeling():
    # fs_div 는 상위 오케스트레이터가 lock. 핵심 로직은 fs_div 에 무관하게 동일 계산.
    cum = build_cum(cum_reports_revenue())
    s = C.reconstruct_single_quarters(cum)
    check("test2 ofs same-logic TTM", C.assemble_ttm(s)["ttm"] == 620)


def test_determinism():
    r1 = C.reconstruct_single_quarters(build_cum(cum_reports_revenue()))
    r2 = C.reconstruct_single_quarters(build_cum(cum_reports_revenue()))
    check("test10 determinism", r1 == r2)


def test_cache_filename_separation():
    ann = "005930_2025_CFS.json"  # 기존 연간 캐시 형식
    q = C.quarterly_cache_filename("005930", 2025, C.ANNUAL_REPORT_CODE, "CFS")
    check("test11 quarterly filename has reprt_code", q == "005930_2025_11011_CFS.json")
    check("test11 quarterly != annual filename", q != ann)
    q13 = C.quarterly_cache_filename("005930", 2026, C.Q1_REPORT_CODE, "CFS")
    check("test11 q1 filename", q13 == "005930_2026_11013_CFS.json")


def test_ambiguous_cumulative_flag():
    # 반기보고서에서 add_amount 부재 → ambiguous=True (3개월값 오인 위험 표시)
    rows = [is_row("ifrs-full_Revenue", "매출액", amount="130")]  # add 없음
    info = C.cumulative_is_value(rows, C.IS_ACCOUNTS["revenue"], C.HALF_REPORT_CODE)
    check("extra ambiguous flag on H1 w/o add", info["ambiguous"] is True and info["value"] == 130)
    # 1분기 add 부재는 정상(3개월=누적)
    rows2 = [is_row("ifrs-full_Revenue", "매출액", amount="100")]
    info2 = C.cumulative_is_value(rows2, C.IS_ACCOUNTS["revenue"], C.Q1_REPORT_CODE)
    check("extra Q1 w/o add not ambiguous", info2["ambiguous"] is False and info2["value"] == 100)


def test_monotonic_violation():
    # 매출 누적 감소(H1 < Q1) → 비단조 감지
    cum = {
        (2025, C.Q1_REPORT_CODE): 100,
        (2025, C.HALF_REPORT_CODE): 90,   # 위반
        (2025, C.Q3_REPORT_CODE): 200,
        (2025, C.ANNUAL_REPORT_CODE): 300,
    }
    m = C.check_monotonic_cumulative(cum)
    check("extra monotonic violation detected", not m["monotonic"] and len(m["violations"]) == 1, str(m))


def test_sanity_revenue_impossible_blocked():
    # 매출 2025Q4 가 2025 연간 초과 → [불가] (데이터 오류)
    singles = {"revenue": {"2026Q1": 80.0, "2025Q4": 400.0, "2025Q3": 86.0, "2025Q2": 74.0}}
    fy = {"revenue": 333.0}
    yoy = {"revenue": 79.0}
    flags = C.sanity_flags_single_quarter(singles, fy, yoy)
    check("sanity revenue>FY → [불가]", any(f.startswith("[불가]") and "revenue" in f for f in flags), str(flags))
    # 매출 음수도 [불가]
    neg = C.sanity_flags_single_quarter({"revenue": {"2025Q2": -5.0}}, {"revenue": 100.0}, {})
    check("sanity revenue<0 → [불가]", any("음수" in f for f in neg), str(neg))


def test_sanity_income_yoy_review():
    # 영업이익 2026Q1 YoY +756% → [검산](불가 아님)
    singles = {"operatingIncome": {"2026Q1": 57.0, "2025Q4": 12.0, "2025Q3": 11.0, "2025Q2": 10.0}}
    fy = {"operatingIncome": 43.0}
    yoy = {"operatingIncome": 6.0}
    flags = C.sanity_flags_single_quarter(singles, fy, yoy)
    check("sanity opinc YoY → [검산]", any(f.startswith("[검산]") and "operatingIncome" in f for f in flags), str(flags))
    check("sanity opinc no [불가]", not any(f.startswith("[불가]") for f in flags), str(flags))


def test_sanity_income_sign_no_false_positive():
    # 순이익: FY 소액 적자(-1), 특정 분기 큰 적자(-2) → |분기|>|연간| 이지만 정상(부호 변동) → [불가] 없어야
    singles = {"netIncome": {"2026Q1": 0.3, "2025Q4": -2.0, "2025Q3": 0.5, "2025Q2": 0.2}}
    fy = {"netIncome": -1.0}
    yoy = {"netIncome": 0.25}
    flags = C.sanity_flags_single_quarter(singles, fy, yoy)
    check("sanity income sign-swing no [불가]", not any(f.startswith("[불가]") for f in flags), str(flags))


def test_sanity_clean_passes():
    # 정상: 각 분기 < FY, YoY 완만 → 플래그 없음
    singles = {"revenue": {"2026Q1": 80.0, "2025Q4": 93.0, "2025Q3": 86.0, "2025Q2": 74.0}}
    fy = {"revenue": 333.0}
    yoy = {"revenue": 79.0}
    flags = C.sanity_flags_single_quarter(singles, fy, yoy)
    check("sanity clean → no flags", flags == [], str(flags))


def test_prior_year_quarter_extraction():
    rows = [{"sj_div": "IS", "account_id": "ifrs-full_Revenue", "account_nm": "매출액",
             "thstrm_amount": "133", "thstrm_add_amount": "133", "frmtrm_q_amount": "79"}]
    prior = C.prior_year_quarter_value(rows, C.IS_ACCOUNTS["revenue"])
    check("prior-year quarter = frmtrm_q_amount", prior == 79, str(prior))


def main():
    tests = [
        test_normal_cfs_ttm, test_ofs_labeling, test_missing_one_report,
        test_q2_diff, test_q3_diff, test_q4_diff,
        test_negative_operating_income, test_account_name_variants,
        test_no_fs_div_mixing, test_determinism, test_cache_filename_separation,
        test_ambiguous_cumulative_flag, test_monotonic_violation,
        test_sanity_revenue_impossible_blocked, test_sanity_income_yoy_review,
        test_sanity_income_sign_no_false_positive, test_sanity_clean_passes,
        test_prior_year_quarter_extraction,
    ]
    print("=== test_ttm_core ===")
    for t in tests:
        t()
    print(f"\n=== {_PASS} passed, {_FAIL} failed ===")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
