#!/usr/bin/env python3
"""R11 §2·§3 — 공정 통제군 (EW_VALID_PBR) 정의와 구조 동일화.

WABABA-BM-INCREMENTAL-ALPHA-VALIDATION-R11

단일 연구질문:
  PBR 데이터가 실제로 존재하는 동일 투자가능 universe 를 그냥 동일가중으로 사는 것보다,
  BM 상위 20% 를 고르는 행위가 추가 수익(incremental alpha)을 만드는가?

공정성 원칙 (§2)
  candidate 가 PBR 결측 종목을 살 수 없다면 control 도 그 종목을 포함하면 안 된다.
  eligible set 은 양쪽 모두 `RankCache.ranked(d, "BM")` = investable ∩ PBR>0 로 동일하다.

구조 동일화 (§3) — 차이는 오직 selection 이어야 한다
  동일: PIT date · investability · eligible date · 거래 캘린더 · 12개월 월별 분할진입 ·
        24개월 보유 프레임 · 현금 처리 · 상장폐지 처리 · 비용/슬리피지 · 교체 타이밍 ·
        초기자본 · 결측 타이밍
  → R10 사양준수 엔진(r10_engine.simulate)을 **그대로** 쓰고 pick_fn 만 교체한다.

통제군 3종
  C1 EW_VALID_PBR_ALL     유효 PBR 전 종목 동일가중. §2 문자 그대로의 control.
                          종목수가 ~1,500개라 5천만원/12개월 tranche 로는 정수주가
                          전부 0이 된다(월 tranche 417만원 ÷ 1,500 ≈ 2,800원).
                          따라서 이 control 만 **소수주**를 허용한다 — 유일한 구조
                          차이이며 보고서에 그대로 명시한다. 비용·상폐·현금·타이밍은 동일.
  C2 EW_VALID_PBR_N40     동일 eligible set 에서 **무작위 40종목**. 정수주까지 candidate 와
                          완전히 동일한 구조. 30 seed 분포.
                          → "BM 순위가 같은 풀에서 무작위 선택보다 나은가" 를 순수 격리.
  C3 BM_REST80            동일 프레임에서 BM **하위 80%** 구간에서 40종목 균등간격.
                          → §12 TOP20 vs REST80 cross-sectional spread.

금지 (§1): P/N/H·스케줄·비용·benchmark·gate 변경 0. 새 후보 생성 0.
안전: 계산 전용 · 네트워크 0 · 파일 write 0(호출자가 저장) · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r10_engine as R10  # noqa: E402
from r9_engine import bench_same_schedule, evaluate  # noqa: E402

CANDIDATE = "CANDIDATE_BM_TOP20"
C1 = "EW_VALID_PBR_ALL"
C2 = "EW_VALID_PBR_N40"
C3 = "BM_REST80"
P1 = "BM_TOP20_POOL_ALL"
P2 = "BM_REST80_POOL_ALL"


# ───────────────────── eligible set (양쪽 공통) ─────────────────────
def eligible(cache, d, factor="BM"):
    """candidate 가 실제로 BM 을 계산할 수 있는 종목 = 유효 PBR investable 종목.

    RankCache.ranked 는 factor_values 의 BM(=1/PBR, PBR>0 만) 을 investable universe 로
    제한해 BM 내림차순 정렬한 리스트다. candidate 선정 풀의 모집단과 **정확히 같다**.
    """
    return cache.ranked(d, factor)


# ───────────────────── pick_fn 3종 ─────────────────────
def pick_ew_all(cache, d, uni, *, factor, percentile, n_holdings, selection, rng,
                banned):
    """C1 — 유효 PBR 전 종목. 균등 금액 배분은 엔진의 per = cash/len(picks) 가 처리한다."""
    b = set(banned or ())
    return [t for t in eligible(cache, d, factor) if t not in b]


def pick_random_valid_pbr(cache, d, uni, *, factor, percentile, n_holdings, selection,
                          rng, banned):
    """C2 — 동일 eligible set 에서 무작위 N종목. candidate 와 구조 100% 동일."""
    b = set(banned or ())
    pool = sorted(t for t in eligible(cache, d, factor) if t not in b)
    if not pool:
        return []
    return rng.sample(pool, min(n_holdings, len(pool)))


def pick_rest80(cache, d, uni, *, factor, percentile, n_holdings, selection, rng,
                banned):
    """C3 — BM 하위 80% 구간에서 N종목 균등간격.

    candidate 의 `_pick` 과 **완전히 같은 균등간격 규칙**을 쓰고 구간만 반대쪽이다.
    (candidate: ranked[:width] · 여기: ranked[width:])
    """
    b = set(banned or ())
    full = eligible(cache, d, factor)
    width = max(n_holdings, int(len(full) * percentile))
    pool = [t for t in full[width:] if t not in b]
    if len(pool) <= n_holdings:
        return pool
    step = len(pool) / n_holdings
    return [pool[min(len(pool) - 1, int(k * step))] for k in range(n_holdings)]


def pick_top20_pool(cache, d, uni, *, factor, percentile, n_holdings, selection, rng,
                    banned):
    """P1 — BM 상위 20% **풀 전체** 동일가중 (40종목 균등간격 샘플링 없이).

    §12 가 요구하는 pool 수준 cross-section 이다. 후보의 초과수익이 '40종목 균등간격
    추출' 이라는 샘플링 방식의 산물인지, BM 상위 20% 자체의 성질인지 분리한다.
    threshold 탐색이 아니다 — frozen P20 그대로이고 N 만 풀 전체다.
    """
    b = set(banned or ())
    full = eligible(cache, d, factor)
    width = max(n_holdings, int(len(full) * percentile))
    return [t for t in full[:width] if t not in b]


def pick_rest80_pool(cache, d, uni, *, factor, percentile, n_holdings, selection, rng,
                     banned):
    """P2 — BM 하위 80% 풀 전체 동일가중."""
    b = set(banned or ())
    full = eligible(cache, d, factor)
    width = max(n_holdings, int(len(full) * percentile))
    return [t for t in full[width:] if t not in b]


PICKERS = {C1: pick_ew_all, C2: pick_random_valid_pbr, C3: pick_rest80,
           P1: pick_top20_pool, P2: pick_rest80_pool}
FRACTIONAL = {C1: True, C2: False, C3: False, P1: True, P2: True}


# ───────────────────── 실행 ─────────────────────
def run(cache, dates, idx, *, arm=CANDIDATE, base_kw=None, seed=20260820, **over):
    """arm 하나를 R10 사양준수 엔진으로 실행한다. base_kw 는 frozen 파라미터."""
    kw = dict(base_kw or {})
    kw.update(over)
    if arm != CANDIDATE:
        kw["pick_fn"] = PICKERS[arm]
        kw["fractional_shares"] = FRACTIONAL[arm]
    kw["seed"] = seed
    res = R10.simulate(cache, dates, **kw)
    if not res:
        return None, None
    b = bench_same_schedule(idx, dates, tranches=res["tranches"])
    return res, evaluate(res, b)


def structural_diff(res_cand, res_ctrl):
    """candidate 와 control 의 구조가 실제로 같은지 수치로 증명한다(§3).

    실행구조 차이가 factor alpha 로 둔갑하는 것을 막는 검사다.
    """
    def con(r):
        out, c = [], 0.0
        for x in r["inflow"]:
            c += x
            out.append(c)
        return out
    ca, cb = con(res_cand), con(res_ctrl)
    return {
        "sameDates": res_cand["dates"] == res_ctrl["dates"],
        "sameInflowSchedule": all(abs(a - b) < 1e-6 for a, b in
                                 zip(res_cand["inflow"], res_ctrl["inflow"])),
        "sameContributed": abs(res_cand["contributed"] - res_ctrl["contributed"]) < 1.0,
        "sameContributionPath": all(abs(a - b) < 1e-6 for a, b in zip(ca, cb)),
        "sameTranches": res_cand["tranches"] == res_ctrl["tranches"],
        "sameCohortCount": res_cand["cohorts"] == res_ctrl["cohorts"],
        "sameBuyMonths": res_cand["buyMonths"] == res_ctrl["buyMonths"],
        "candidateContributed": res_cand["contributed"],
        "controlContributed": res_ctrl["contributed"],
        "candidateCohorts": res_cand["cohorts"],
        "controlCohorts": res_ctrl["cohorts"],
        "candidateAvgPositions": round(sum(res_cand["positions"])
                                       / len(res_cand["positions"]), 1),
        "controlAvgPositions": round(sum(res_ctrl["positions"])
                                     / len(res_ctrl["positions"]), 1),
        "candidateAvgCashPct": round(100 * sum(res_cand["cashRatio"])
                                     / len(res_cand["cashRatio"]), 2),
        "controlAvgCashPct": round(100 * sum(res_ctrl["cashRatio"])
                                   / len(res_ctrl["cashRatio"]), 2),
    }


__all__ = ["CANDIDATE", "C1", "C2", "C3", "P1", "P2", "PICKERS", "FRACTIONAL", "eligible", "run",
           "structural_diff", "pick_ew_all", "pick_random_valid_pbr", "pick_rest80"]
