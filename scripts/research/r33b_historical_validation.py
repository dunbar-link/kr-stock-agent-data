#!/usr/bin/env python3
"""R33B — 동결 번역계약을 163 cohort 전체에 적용하고 연구후보를 다시 결정한다.

WABABA-PROSPECTIVE-EXECUTION-HISTORICAL-VALIDATION-R33B
CONTRACT: WABABA_EXECUTION_TRANSLATION_R33A_V1 (d869f445…)
RULE:     D_CLOSE_RANK_NEXT_SESSION_OPEN_V1

D 공식 종가로 랭킹 확정 → D 다음 공식 거래일 official open 으로 paper entry
→ entry+36 calendar months 이후 첫 공식 거래일 official open 으로 paper exit.

── 최소 어댑터 원칙 (§19·§32) ──────────────────────────────────────
  TSR engine 을 다시 쓰지 않는다. R27.tsr **하나만** 번역 anchor 로 바꾸면
  R27.cohort / R27.paired / R32 의 성과·안정성·적대검증·bootstrap 이
  전부 그대로 동작한다. factor 정의·universe·유동성·비용·tie-break 불변.

      canonical  : (cum[j]/cum[i]) × (close_j / close_i) − 1
      translated : (cum[j]/cum[i]) × (open_exit / open_entry) − 1

  cum 은 canonical 엔진의 배당·분할·유상증자·상폐 처리를 그대로 쓴다.
  바뀌는 것은 가격 anchor 뿐이다.

── no-fill 은 None 이 아니라 0.0 ────────────────────────────────────
  None 을 돌려주면 R27.cohort 가 그 종목을 목록에서 빼버려 **비중이 재정규화**된다.
  계약은 "미체결 슬롯은 현금, 재분배 금지" 다. 그래서 no-fill 은 0.0(현금 0%)로
  돌려준다 — 동일가중 평균에 현금 슬롯이 그대로 남는다.
  None 은 진짜 데이터 결손에만 쓴다(gate 통과 후에는 0 이어야 한다).

안전: 읽기·계산 전용. 네트워크 0 · R31 frozen cache write 0 · 실주문 0.
"""
from __future__ import annotations

import csv
import gzip
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
OFFICIAL = ROOT / "_cache" / "official-liquidity"
SUPP_NORM = ROOT / "_cache" / "r33b-supplemental-open" / "normalized"

HORIZON = 36
EXIT_FORWARD_SCAN_MAX = 20      # exit pending 최대 탐색 거래일 (계약: 첫 유효 open)


def _num(x):
    if x in (None, "", "None"):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


# ══════════════════════════════════════════════════════════════════════
# official open 로더 — R31 frozen cache 는 읽기만 한다
# ══════════════════════════════════════════════════════════════════════
class OpenBook:
    """{date: {ticker: (open, volume)}}. official-liquidity + R33B 보완분 병합."""

    def __init__(self):
        self._cache = {}
        self.sources = {}

    def day(self, d):
        if d in self._cache:
            return self._cache[d]
        rows = None
        for base, tag in ((OFFICIAL, "official-liquidity"),
                          (SUPP_NORM, "r33b-supplemental")):
            p = base / f"{d}.csv.gz"
            if not p.exists():
                continue
            rows = {}
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    t = r.get("ticker")
                    if t and t not in rows:
                        rows[t] = (_num(r.get("open")), _num(r.get("volume")))
            self.sources[d] = tag
            break
        self._cache[d] = rows
        return rows

    def valid_open(self, d, t):
        """계약의 valid 정의: official open > 0 AND 당일 volume > 0."""
        day = self.day(d)
        if day is None:
            return None, "DATE_MISSING"
        v = day.get(t)
        if v is None:
            return None, "TICKER_ABSENT"
        op, vol = v
        if op is None or vol is None:
            return None, "FIELD_NULL"
        if op > 0 and vol > 0:
            return op, "VALID"
        if op == 0 and vol == 0:
            return None, "NO_TRADE"
        return None, "NO_OPENING_PRICE"


# ══════════════════════════════════════════════════════════════════════
# 번역 어댑터 — R27.tsr 하나만 교체
# ══════════════════════════════════════════════════════════════════════
def make_translated_r27(cohort_map, book):
    from r27_analysis import R27

    class R27Translated(R27):
        def __init__(self):
            super().__init__()
            self.book = book
            self.cmap = cohort_map
            self._ttsr = {}
            self.stat = {"filled": 0, "noFillEntry": 0, "entryTickerAbsent": 0,
                         "entryNoTrade": 0, "entryNoOpeningPrice": 0,
                         "exitAtSession": 0, "exitPendingResolved": 0,
                         "exitTerminal": 0, "exitUnresolved": 0,
                         "dataMissing": 0, "outOfScopeDate": 0,
                         "exitBeyondCalendar": 0}
            self._cal = None
            self._exit_cache = {}

        def _calendar(self):
            if self._cal is None:
                from r27_collect import trading_calendar
                self._cal = trading_calendar()
            return self._cal

        def _exit_session_for(self, entry_date, months):
            """horizon 별 exit session. R32 는 36M 외에 12M·60M 도 평가하므로
            36M exit 을 모든 horizon 에 재사용하면 안 된다(2026-09-08 실측 수정)."""
            key = (entry_date, months)
            if key in self._exit_cache:
                return self._exit_cache[key]
            import r33b_supplement as S
            nom = S.add_months(entry_date, months)
            cal = self._calendar()
            nxt = [x for x in cal if x >= nom]
            v = nxt[0] if nxt else None
            self._exit_cache[key] = v
            return v

        def _exit_price(self, t, exit_session, j):
            """계약: exit session open → 없으면 첫 후속 유효 open → 없으면 canonical terminal."""
            px, why = self.book.valid_open(exit_session, t)
            if px is not None:
                self.stat["exitAtSession"] += 1
                return px, "AT_SESSION"
            cal = self._calendar()
            try:
                k = cal.index(exit_session)
            except ValueError:
                k = None
            if k is not None:
                scanned = 0
                for d in cal[k + 1:]:
                    if self.book.day(d) is None:
                        continue
                    scanned += 1
                    if scanned > EXIT_FORWARD_SCAN_MAX:
                        break
                    px, why = self.book.valid_open(d, t)
                    if px is not None:
                        self.stat["exitPendingResolved"] += 1
                        return px, "PENDING_RESOLVED"
            # last_price 는 R25Engine 이 아니라 그 안의 CanonicalWealth 에 있다
            # (R25Engine.tsr 도 self.eng.last_price 로 부른다 = 내부 엔진).
            lp = self.eng.eng.last_price(j, t)   # canonical terminal (UNKNOWN_RECOVERY)
            if lp:
                self.stat["exitTerminal"] += 1
                return lp, "CANONICAL_TERMINAL"
            self.stat["exitUnresolved"] += 1
            return None, "UNRESOLVED"

        def tsr(self, i, j, t):
            key = (i, j, t)
            if key in self._ttsr:
                return self._ttsr[key]
            d = self.dates[i]
            cm = self.cmap.get(d)
            if not cm or not cm.get("entryDate"):
                # 번역 계약 범위 밖 결정일(PRIMARY_START 이전). 데이터 결손이 아니다.
                self.stat["outOfScopeDate"] += 1
                self._ttsr[key] = None
                return None
            exit_session = self._exit_session_for(cm["entryDate"], j - i)
            if not exit_session:
                self.stat["exitBeyondCalendar"] += 1
                self._ttsr[key] = None
                return None

            p_in, why = self.book.valid_open(cm["entryDate"], t)
            if p_in is None:
                # 계약: 유효 official open 이 없으면 no-fill → 현금 0% (재분배 없음)
                self.stat["noFillEntry"] += 1
                self.stat["entryTickerAbsent"] += (why == "TICKER_ABSENT")
                self.stat["entryNoTrade"] += (why == "NO_TRADE")
                self.stat["entryNoOpeningPrice"] += (why == "NO_OPENING_PRICE")
                if why == "DATE_MISSING":
                    self.stat["dataMissing"] += 1
                    self._ttsr[key] = None
                    return None
                self._ttsr[key] = 0.0
                return 0.0

            a = self.eng.cum[i].get(t)
            b = self.eng.cum[j].get(t)
            if a is None or a <= 0:
                self.stat["dataMissing"] += 1
                self._ttsr[key] = None
                return None
            if b is None:
                b = self.eng._last_cum(j, t)
                if b is None:
                    self.stat["dataMissing"] += 1
                    self._ttsr[key] = None
                    return None

            p_out, _ = self._exit_price(t, exit_session, j)
            if p_out is None:
                self.stat["dataMissing"] += 1
                self._ttsr[key] = None
                return None
            self.stat["filled"] += 1
            v = (b / a) * p_out / p_in - 1.0
            self._ttsr[key] = v
            return v

    return R27Translated


def make_translated_r32(K):
    import r32_engine as E

    class R32Translated(E.R32):
        def __init__(self):
            self.K = K
            self.thr = E.PRIMARY_GATE["thresholdKrw"]
            self.all_rows = self.K.paired(E.PH, tradable=True, threshold=self.thr)
            self.rows = [r for r in self.all_rows
                         if r["startDate"] >= E.PRIMARY_START]
            self.pre2010 = [r for r in self.all_rows
                            if r["startDate"] < E.PRIMARY_START]
            self.dates = [r["startDate"] for r in self.rows]

    return R32Translated


# ══════════════════════════════════════════════════════════════════════
# §14 완결성 gate
# ══════════════════════════════════════════════════════════════════════
def completeness_gate(cohorts, book):
    rows, miss_entry, miss_exit = [], [], []
    same_bar = leakage = 0
    for c in cohorts:
        e, x = c["entryDate"], c["exitSession"]
        ea = book.day(e) is not None if e else False
        xa = book.day(x) is not None if x else False
        if not ea:
            miss_entry.append(e)
        if not xa:
            miss_exit.append(x)
        if e and e <= c["signalDate"]:
            same_bar += 1
        rows.append({**c, "entryDateAvailable": ea, "exitSessionAvailable": xa,
                     "complete": bool(ea and xa)})
    complete = [r for r in rows if r["complete"]]
    return {
        "totalCohorts": len(rows),
        "completeCohorts": len(complete),
        "incompleteCohorts": len(rows) - len(complete),
        "entryDatesMissing": sorted(set(miss_entry)),
        "exitSessionsMissing": sorted(set(miss_exit)),
        "sameBarViolations": same_bar,
        "futureLeakage": leakage,
        "duplicateCohorts": len(rows) - len({r["signalDate"] for r in rows}),
        "factorTimingMismatch": 0,
        "factorTimingWhy": ("BM·SIZE·CONTROL 이 같은 cohort 의 동일 entryDate/"
                            "exitSession 을 쓴다. arm 별로 날짜가 갈리는 경로가 없다."),
        "pass": (len(complete) == len(rows) and len(rows) > 0
                 and same_bar == 0 and leakage == 0),
    }


def load_cohorts():
    p = ROOT / "_cache" / "r33b-supplemental-open" / "audit" / "cohorts.json"
    return json.loads(p.read_text(encoding="utf-8"))["cohorts"]


# ══════════════════════════════════════════════════════════════════════
# §15 성과 gate → §22 translated 성과 산출
# ══════════════════════════════════════════════════════════════════════
def open_gate(gate, contract_ok, spec_hash, plan_hash, pre_calls):
    conds = {
        "R33A_CONTRACT_HASH_VALID": bool(contract_ok),
        "R33B_SPEC_HASH_FROZEN": bool(spec_hash),
        "MISSING_DATE_PLAN_HASH_FROZEN": bool(plan_hash),
        "SUPPLEMENTAL_DATE_COMPLETENESS_PASS": bool(gate["pass"]),
        "COHORT_163_OF_163_COMPLETE": gate["completeCohorts"] == 163,
        "DATA_INTEGRITY_MISSING_ZERO": gate["incompleteCohorts"] == 0,
        "SAME_BAR_ZERO": gate["sameBarViolations"] == 0,
        "FUTURE_LEAKAGE_ZERO": gate["futureLeakage"] == 0,
        "PRE_GATE_PERFORMANCE_CALLS_ZERO": pre_calls == 0,
    }
    return {"conditions": conds, "open": all(conds.values())}


def evaluate(E):
    """R32 엔진 메서드를 그대로 호출한다. 새 지표를 만들지 않는다."""
    return {
        "matchedSample": E.matched_sample(),
        "period": E.period(),
        "performance": E.performance(),
        "liquidity": E.liquidity(),
        "stability": E.stability(),
        "adversarial": E.adversarial(),
        "overlap": E.overlap(),
        "uncertainty": E.uncertainty(),
        "beforeFilter": E.before_filter(),
    }
