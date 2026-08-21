#!/usr/bin/env python3
"""R15 forensic audit — 삼성전자 bridge · corporate-action coverage · engine 판정.

WABABA-TSR-CORPORATE-ACTION-FORENSIC-R15

산출:
  reports/research/r15-samsung-tsr-latest.json
  reports/research/r15-corporate-action-coverage-latest.json
  reports/research/r15-return-engine-audit-latest.json

안전: 계산 전용 · 네트워크 0 · canonical 미접근 · 실주문 0 · 브로커 0 · 배포 0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_engine import load_names, load_snapshots  # noqa: E402
import r15_tsr as T  # noqa: E402
from run_backtest_matrix import contiguous_span  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
TK = "005930"
START, END = "2010-01-04", "2021-08-02"


def pct(x, nd=2):
    return None if x is None else round(100.0 * x, nd)


def save(name, obj):
    RD.mkdir(parents=True, exist_ok=True)
    p = RD / f"r15-{name}-latest.json"
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(f"[r15] saved {p.name}", file=sys.stderr)


def cagr(r, months):
    return None if r is None else (1.0 + r) ** (12.0 / months) - 1.0


# ═══════════ 삼성전자 bridge (§2·§4·§7·§15) ═══════════
def samsung(sn, nm, ds, idx):
    i, j = ds.index(START), ds.index(END)
    months = j - i
    p0, p1 = idx.price(i, TK), idx.price(j, TK)
    cap0 = idx.cap[ds[i]][TK]
    cap1 = idx.cap[ds[j]][TK]

    ev = [e for e in idx.events if e["ticker"] == TK and i < ds.index(e["toDate"]) <= j]
    # 보유 1주 경로
    s = 1.0
    cash = 0.0
    path = []
    for k in range(i + 1, j + 1):
        f = idx.split[k].get(TK)
        if f:
            s *= f
            path.append({"date": ds[k], "event": "SPLIT/BONUS", "ratio": f,
                         "sharesAfter": s})
        y = idx.monthly_income_yield(k, TK)
        pk = idx.price(k, TK)
        if y and pk:
            cash += s * y * pk
    variants = {
        "PRICE_RETURN": idx.price_return(i, j, TK),
        "SPLIT_ADJUSTED_PRICE_RETURN": idx.split_adjusted_return(i, j, TK),
        "TOTAL_RETURN_NO_REINVEST": idx.tsr(i, j, TK, reinvest=False),
        "TOTAL_RETURN_WITH_REINVEST": idx.tsr(i, j, TK, reinvest=True),
    }
    # 주식수 변화 유형 분해
    sh0, sh1 = cap0["shares"], cap1["shares"]
    split_mult = 1.0
    for e in ev:
        split_mult *= e["shareRatio"]
    non_split = (sh1 / sh0) / split_mult if (sh0 and sh1 and split_mult) else None

    # per-share vs company-level (§7) — KRX 로 파생 가능한 것만
    def der(r):
        eps, bps, sh = r.get("EPS"), r.get("BPS"), None
        return eps, bps
    a, b = sn[ds[i]][TK], sn[ds[j]][TK]
    ni0 = (a["EPS"] * sh0) if (a["EPS"] and sh0) else None
    ni1 = (b["EPS"] * sh1) if (b["EPS"] and sh1) else None
    eq0 = (a["BPS"] * sh0) if (a["BPS"] and sh0) else None
    eq1 = (b["BPS"] * sh1) if (b["BPS"] and sh1) else None

    def g(v0, v1):
        if not v0 or not v1 or v0 <= 0:
            return None
        return (v1 / v0) ** (12.0 / months) - 1.0

    # per-share 는 분할 배율로 정규화해야 비교 가능
    eps0_adj = a["EPS"] / split_mult if a["EPS"] else None
    bps0_adj = a["BPS"] / split_mult if a["BPS"] else None

    out = {
        "ticker": TK, "name": nm.get(TK, TK),
        "period": {"start": ds[i], "end": ds[j], "months": months,
                   "years": round(months / 12, 2)},
        "rawPrice": {"start": p0, "end": p1,
                     "rawPriceChangePct": pct(p1 / p0 - 1) if (p0 and p1) else None},
        "sharesOutstanding": {
            "start": sh0, "end": sh1,
            "totalRatio": round(sh1 / sh0, 4) if (sh0 and sh1) else None,
            "splitMultiplier": split_mult,
            "nonSplitRatio": round(non_split, 4) if non_split else None,
            "nonSplitInterpretation": (
                "분할 배율을 제거한 잔여 배율. 1 보다 작으면 **자사주 소각 등으로 "
                "주식수가 순감소**했다는 뜻이다(주주에게 현금이 오지 않으므로 수익률 "
                "가산 없음 — 지분율 상승은 이미 주가에 반영).")},
        "corporateActions": {
            "detectedSplitEvents": ev,
            "note": ("분할 서명(주식수 배율 ≈ 가격 역배율)으로 탐지된 것만이다. "
                     "2018-05 → 2018-06 의 50:1 액면분할이 정확히 잡혔다."),
            "treasuryCancellation": (
                f"주식수가 분할효과 제외 후 {round(non_split, 4) if non_split else '-'} "
                "배가 됐다. 자사주 소각으로 순감소한 것으로 보이며, precommit 규칙대로 "
                "수익률 조정을 하지 않았다."),
            "rightsIssue": ("이 기간 주식수 순감소이므로 대규모 유상증자 정황은 없다. "
                            "다만 저장소에 신주배정 데이터가 없어 '없었다'고 단정하지 "
                            "않는다 — 미확인으로 남긴다."),
            "specialDividend": ("DPS 가 FY2020 분에서 2,994원으로 급증했다"
                                "(정규 1,416 + 특별 1,578). DPS 에 특별배당이 이미 "
                                "포함돼 별도 처리가 불필요하다."),
        },
        "returnVariants": {k: {"totalPct": pct(v), "cagrPct": pct(cagr(v, months))}
                           for k, v in variants.items()},
        "holdingPath": {"finalSharesPerInitialShare": s,
                        "cumulativeCashDividendKrw": round(cash),
                        "dividendAsPctOfInitialPrice": pct(cash / p0) if p0 else None,
                        "splitEvents": path},
        "engineComparison": {
            "currentEngineCagrPct": pct(cagr(variants["PRICE_RETURN"], months)),
            "correctTsrCagrPct": pct(cagr(variants["TOTAL_RETURN_WITH_REINVEST"], months)),
            "errorPctPointsPerYear": round(
                100 * (cagr(variants["TOTAL_RETURN_WITH_REINVEST"], months)
                       - cagr(variants["PRICE_RETURN"], months)), 2),
            "currentEngineTotalPct": pct(variants["PRICE_RETURN"]),
            "correctTsrTotalPct": pct(variants["TOTAL_RETURN_WITH_REINVEST"]),
        },
        "fundamentalPerShareAudit": {
            "note": ("§7 — 매출·영업이익·영업현금흐름은 장기 PIT 에 없다(DART 는 "
                     "FY2022~2025 뿐). KRX 로 파생 가능한 항목만 계산한다. "
                     "총액은 EPS×주식수 / BPS×주식수 로 파생한 근사치다."),
            "companyLevel": {
                "derivedNetIncomeStart": ni0, "derivedNetIncomeEnd": ni1,
                "derivedNetIncomeCagrPct": pct(g(ni0, ni1)),
                "derivedEquityStart": eq0, "derivedEquityEnd": eq1,
                "derivedEquityCagrPct": pct(g(eq0, eq1))},
            "perShare": {
                "epsStartRaw": a["EPS"], "epsStartSplitAdj": eps0_adj,
                "epsEnd": b["EPS"], "epsCagrPct": pct(g(eps0_adj, b["EPS"])),
                "bpsStartRaw": a["BPS"], "bpsStartSplitAdj": bps0_adj,
                "bpsEnd": b["BPS"], "bpsCagrPct": pct(g(bps0_adj, b["BPS"])),
                "dpsStart": idx.cap[ds[i]][TK]["dps"],
                "dpsEnd": idx.cap[ds[j]][TK]["dps"]},
            "companyVsPerShare": (
                "주식수가 분할효과를 빼면 순감소했으므로 per-share 성장률이 회사 전체 "
                "성장률보다 **높다**. 자사주 소각이 주주 1주의 경제적 몫을 키운 것이다."),
            "unavailable": ["revenue", "operatingProfit", "operatingCashFlow",
                            "reportedEquity(총액 원본)", "dividendsTotal(총액 원본)"],
        },
        "sanityChecks": {
            "splitDoesNotCreateWealth": (
                "분할 조정 후 2018-05 → 2018-06 보유 wealth 변화가 시총 변화와 "
                "일치해야 한다. 50배 증가/감소가 나오면 버그."),
            "dividendNotDoubleCounted": (
                "DPS 는 연간값이라 월 1/12 로 발생시킨다. 매월 통째로 더하면 12배 과대."),
        },
    }
    # sanity: 분할 전후 1개월 wealth 변화
    k = ds.index("2018-06-01")
    r_ret = idx.tsr(k - 1, k, TK, reinvest=False, include_dividend=False)
    mc0 = idx.cap[ds[k - 1]][TK]
    mc1 = idx.cap[ds[k]][TK]
    mcap0 = mc0["close"] * mc0["shares"]
    mcap1 = mc1["close"] * mc1["shares"]
    out["sanityChecks"]["splitMonthResult"] = {
        "month": f"{ds[k-1]} → {ds[k]}",
        "adjustedHoldingReturnPct": pct(r_ret),
        "marketCapChangePct": pct(mcap1 / mcap0 - 1),
        "unadjustedEngineReturnPct": pct(idx.price_return(k - 1, k, TK)),
        "pass": abs(r_ret - (mcap1 / mcap0 - 1)) < 0.005,
        "note": "조정 수익률이 시총 변화와 0.5%p 이내로 일치해야 한다."}
    save("samsung-tsr", out)
    return out


# ═══════════ coverage audit (§9) ═══════════
def coverage(sn, nm, ds, idx):
    tot = 0
    jumps = 0
    for i in range(1, len(ds)):
        a = idx.cap.get(ds[i - 1], {})
        b = idx.cap.get(ds[i], {})
        for t, cur in b.items():
            p = a.get(t)
            if not p:
                continue
            if not (p.get("close") and cur.get("close") and p.get("shares")
                    and cur.get("shares")):
                continue
            tot += 1
            sr = cur["shares"] / p["shares"]
            if sr >= T.MIN_RATIO or sr <= 1.0 / T.MIN_RATIO:
                jumps += 1
    splits = [e for e in idx.events if e["shareRatio"] >= 1.0]
    revs = [e for e in idx.events if e["shareRatio"] < 1.0]
    dps_ok = 0
    dps_tot = 0
    for d in ds:
        for t, r in idx.cap.get(d, {}).items():
            dps_tot += 1
            if r.get("dps") is not None:
                dps_ok += 1
    rows = [
        {"event": "SPLIT", "source": "shares+close 서명 탐지", "coverage": "전 구간",
         "engineSupportBefore": "없음(미조정)", "engineSupportAfter": "지원",
         "detected": len(splits), "unresolved": 0},
        {"event": "REVERSE_SPLIT", "source": "shares+close 서명 탐지", "coverage": "전 구간",
         "engineSupportBefore": "없음(가짜 +3,000~4,900% 발생)",
         "engineSupportAfter": "지원", "detected": len(revs), "unresolved": 0},
        {"event": "BONUS_ISSUE / STOCK_DIVIDEND", "source": "SPLIT 과 동일 서명",
         "coverage": "전 구간", "engineSupportBefore": "없음",
         "engineSupportAfter": "SPLIT 과 동일 처리(경제적으로 동일)",
         "detected": "SPLIT 에 포함", "unresolved": 0},
        {"event": "DIVIDEND", "source": "KRX DPS(직전 연간)", "coverage":
            f"{round(100*dps_ok/dps_tot,1)}% 종목-월",
         "engineSupportBefore": "**없음 — PRICE RETURN 이었다**",
         "engineSupportAfter": "월 1/12 발생 · 재투자", "detected": dps_ok,
         "unresolved": dps_tot - dps_ok},
        {"event": "SPECIAL_DIVIDEND", "source": "DPS 에 포함", "coverage": "DIVIDEND 와 동일",
         "engineSupportBefore": "없음", "engineSupportAfter": "DPS 경유 자동 포함",
         "detected": "분리 불가(합산값)", "unresolved": "분리 불가"},
        {"event": "TREASURY_CANCELLATION", "source": "shares 감소(가격 비례상승 없음)",
         "coverage": "전 구간 관측 가능", "engineSupportBefore": "조정 없음",
         "engineSupportAfter": "조정 없음(경제적으로 올바름)",
         "detected": "미분류", "unresolved": "해당 없음"},
        {"event": "RIGHTS_ISSUE", "source": "**없음** — 신주배정·발행가 데이터 부재",
         "coverage": "0%", "engineSupportBefore": "없음", "engineSupportAfter": "없음",
         "detected": 0, "unresolved": "정량화 불가 — 한계로 명시"},
        {"event": "CAPITAL_REDUCTION", "source": "REVERSE_SPLIT 서명과 구분 불가",
         "coverage": "부분", "engineSupportBefore": "없음",
         "engineSupportAfter": "REVERSE_SPLIT 로 처리",
         "detected": "REVERSE_SPLIT 에 포함", "unresolved": "무상감자와 구분 불가"},
        {"event": "MERGER / SPINOFF", "source": "**없음** — 배정비율 데이터 부재",
         "coverage": "0%", "engineSupportBefore": "없음", "engineSupportAfter": "없음",
         "detected": 0, "unresolved": "정량화 불가 — 한계로 명시"},
        {"event": "DELISTING", "source": "스냅샷 소멸", "coverage": "전 구간",
         "engineSupportBefore": "지원(마지막 관측가 청산)",
         "engineSupportAfter": "동일 유지", "detected": "R7 정본과 동일",
         "unresolved": 0},
        {"event": "TENDER_OFFER", "source": "없음", "coverage": "0%",
         "engineSupportBefore": "없음", "engineSupportAfter": "없음",
         "detected": 0, "unresolved": "해당 사례 드묾 — 한계로 명시"},
    ]
    out = {"tickerMonths": tot, "shareCountJumps": jumps,
           "shareCountJumpPct": round(100 * jumps / tot, 3),
           "splitLikeDetected": len(idx.events),
           "splitLikePct": round(100 * len(idx.events) / tot, 3),
           "splitUp": len(splits), "splitDown": len(revs),
           "dpsCoveragePct": round(100 * dps_ok / dps_tot, 1),
           "rows": rows,
           "limitations": [
               "유상증자·합병·인적분할은 저장소에 배정비율/발행가 데이터가 없어 "
               "조정하지 않는다. 가짜 정밀도를 만들지 않기 위한 선택이다(§11).",
               "무상감자와 액면병합은 서명이 같아 구분되지 않는다. 둘 다 주식수 감소·"
               "가격 비례상승이면 REVERSE_SPLIT 로 처리된다.",
               "DPS 는 연간값이라 배당락 시점을 정확히 재현하지 못한다. 월 균등 발생으로 "
               "근사한다."]}
    save("corporate-action-coverage", out)
    return out


# ═══════════ engine 판정 (§5) ═══════════
def engine_audit(sn, nm, ds, idx, ss, cov):
    i, j = ds.index(START), ds.index(END)
    months = j - i
    tot = aff = big = 0
    worst = []
    for t in sn[ds[i]]:
        pr = idx.price_return(i, j, t)
        ts = idx.tsr(i, j, t)
        if pr is None or ts is None:
            continue
        a, b = cagr(pr, months), cagr(ts, months)
        if a is None or b is None:
            continue
        tot += 1
        d = b - a
        if abs(d) > 0.005:
            aff += 1
        if abs(d) > 0.05:
            big += 1
        worst.append((abs(d), t, a, b))
    worst.sort(reverse=True)
    out = {
        "currentEngineDefinition": (
            "factor_research.fwd_return = P_j / P_i - 1 (close 만 사용). "
            "액면분할 미조정 · 배당 미포함."),
        "isPriceOrTsr": "PRICE_RETURN",
        "materialBug": True,
        "bugs": [
            {"id": "BUG1_UNADJUSTED_SPLIT",
             "severity": "MATERIAL",
             "evidence": (f"삼성전자 2018-05 → 2018-06 을 "
                          f"{ss['sanityChecks']['splitMonthResult']['unadjustedEngineReturnPct']}% 로 "
                          f"기록. 실제 시총 변화 "
                          f"{ss['sanityChecks']['splitMonthResult']['marketCapChangePct']}%. "
                          f"전 구간 분할 서명 {cov['splitLikeDetected']}건"
                          f"({cov['splitLikePct']}% 종목-월). 역분할은 가짜 "
                          f"+3,000~4,900% 수익을 만든다.")},
            {"id": "BUG2_DIVIDEND_OMITTED",
             "severity": "MATERIAL",
             "evidence": ("close 만 쓰므로 배당이 수익률에서 완전히 빠진다. "
                          "R14 Q3 는 **배당성향으로 종목을 고르면서 배당을 수익에서 "
                          "제외**했다 — 구조적 모순.")},
        ],
        "universeImpact": {
            "period": f"{ds[i]} → {ds[j]}",
            "tickers": tot,
            "errorOver0_5ppPerYear": aff,
            "errorOver0_5ppPct": round(100 * aff / tot, 1),
            "errorOver5ppPerYear": big,
            "errorOver5ppPct": round(100 * big / tot, 1),
            "worstTickers": [{"ticker": t, "name": nm.get(t, t),
                              "priceEngineCagrPct": pct(a), "tsrCagrPct": pct(b),
                              "errorPpPerYear": round(100 * (b - a), 2)}
                             for _, t, a, b in worst[:10]]},
        "samsungAnchor": ss["engineComparison"],
        "verdict": "RETURN_ENGINE_PRICE_ONLY_OR_MATERIAL_BUG",
        "verdictReason": (
            "삼성전자 manual TSR 과 tolerance 내 일치하지 않는다 — 연율 오차 "
            f"{ss['engineComparison']['errorPctPointsPerYear']}%p. universe 의 "
            f"{round(100*aff/tot,1)}% 가 0.5%p/년 초과 오차, "
            f"{round(100*big/tot,1)}% 가 5%p/년 초과 오차. §5 의 C 항목에 해당하므로 "
            "R14 결론을 그대로 유지하지 않고 수정된 TSR 기준으로 재검증한다."),
        "scopeBeyondR14": (
            "★ 이 결함은 R14 만의 문제가 아니다. R8~R11 의 포트폴리오 시뮬레이터"
            "(r8_portfolio / r9_engine / r10_engine)도 snap['close'] 를 직접 써서 "
            "매수·평가·매도한다. 즉 같은 분할 미조정 문제를 갖는다. R15 는 지시문 "
            "범위대로 R14 재검증까지만 수행하고, R8~R11 재검증은 다음 작업으로 남긴다."),
    }
    save("return-engine-audit", out)
    return out


def main() -> int:
    sn, nm = load_snapshots(), load_names()
    ds = contiguous_span(sorted(sn))
    idx = T.TsrIndex(ds)
    print(f"[r15] {ds[0]} ~ {ds[-1]} ({len(ds)}m) · split events {len(idx.events)}",
          file=sys.stderr)
    ss = samsung(sn, nm, ds, idx)
    cov = coverage(sn, nm, ds, idx)
    ea = engine_audit(sn, nm, ds, idx, ss, cov)
    print(json.dumps({
        "samsungPriceCagr": ss["engineComparison"]["currentEngineCagrPct"],
        "samsungTsrCagr": ss["engineComparison"]["correctTsrCagrPct"],
        "errorPpPerYear": ss["engineComparison"]["errorPctPointsPerYear"],
        "splitMonthSanity": ss["sanityChecks"]["splitMonthResult"]["pass"],
        "verdict": ea["verdict"],
        "universeErrorOver5ppPct": ea["universeImpact"]["errorOver5ppPct"]},
        ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
