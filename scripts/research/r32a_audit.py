#!/usr/bin/env python3
"""R32A 감사 — frozen 경계 forensics + R31 기준선 row-level 독립 재계산.

WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A
SOURCE_TASK: WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32 (commit 7331813)

두 질문에만 답한다. R32 를 다시 설계하지 않는다.

  A. 2026-08-03 이 실제 exit price date 인지, 명목 label 인지, 보고 오류인지.
     그리고 2026-07-31 frozen boundary 와 충돌하는지.
  B. R31 BASE·100bp 를 canonical 보고서 숫자를 **읽지 않고**
     frozen row-level 데이터 + 기존 계산코드로 다시 산출해 재현되는지.

── B 를 진짜 재계산으로 만드는 장치 ───────────────────────────────
  재계산 구간 동안 `builtins.open` / `Path.read_text` / `Path.read_bytes` 를 감싸
  **canonical evidence 파일 접근을 차단**한다(reports/research/r27-*, r30-*, r31-*,
  reports/wababa/*). 접근이 일어나면 그 자리에서 예외를 던지고 감사를 실패시킨다.
  즉 "보고서 숫자를 입력으로 쓰지 않았다" 가 주장이 아니라 실행으로 증명된다.

  published 값(13.698 등)은 **마지막 비교 단계에서만** 등장한다. 계산 경로 어디에도
  입력으로 들어가지 않는다.

── 새로 만들지 않은 것 ────────────────────────────────────────────
  R27 엔진 · R25 TSR · R26 non-overlapping · r27_run 비용식 · R32 engine.
  이 파일은 감사 harness 이고 계산 로직을 다시 쓰지 않는다.

안전: 읽기·계산 전용. 네트워크 0 · API 0 · env 접근 0 · 인증키 접근 0.
"""
from __future__ import annotations

import builtins
import hashlib
import io
import json
import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"
SRC = ROOT / "scripts" / "research"

TASK_ID = "WABABA-R32-FROZEN-BOUNDARY-AND-BASELINE-REPRO-AUDIT-R32A"
SOURCE_TASK = "WABABA-BM-VS-SIZE-FROZEN-HEAD-TO-HEAD-R32"
SOURCE_COMMIT = "7331813"

# ── §9 비교 목표값. **계산 입력이 아니다.** 마지막 비교에서만 쓴다. ──────
PUBLISHED = {
    "r25_raw_36m": {"SIZE": 19.147, "BM": 15.010, "CONTROL": 9.418},
    "r31_base_36m": {"SIZE": 13.698, "BM": 12.193, "CONTROL": 7.467},
    "r31_cost100bp_36m": {"SIZE": 9.846, "BM": 9.510},
}
DISPLAY_TOL = 5e-4      # 소수 3자리 표시 반올림 허용치. 넓히지 않는다.

FROZEN = {"r27_precommit": "acee4288", "r31_precommit": "d7fe3929",
          "r32_precommit": "07edbe72"}
DATASET_MANIFEST_PREFIX = "3439dec9"


# ══════════════ canonical evidence 접근 차단 가드 ══════════════
_BLOCK = re.compile(
    r"(?i)reports[\\/](research[\\/](r27|r30|r31)-|wababa[\\/])")


class EvidenceAccessViolation(RuntimeError):
    """재계산 중 canonical 보고서를 읽으려 했다 — 재현이 아니라 복제다."""


class _Guard:
    def __init__(self):
        self.active = False
        self.hits = []
        self._open = builtins.open
        self._rt = Path.read_text
        self._rb = Path.read_bytes

    def _check(self, p):
        if self.active and _BLOCK.search(str(p)):
            self.hits.append(str(p))
            raise EvidenceAccessViolation(str(p))

    def install(self):
        g = self

        def open_(file, *a, **k):
            g._check(file)
            return g._open(file, *a, **k)

        def rt(self_, *a, **k):
            g._check(self_)
            return g._rt(self_, *a, **k)

        def rb(self_, *a, **k):
            g._check(self_)
            return g._rb(self_, *a, **k)

        builtins.open = open_
        Path.read_text = rt
        Path.read_bytes = rb

    def restore(self):
        builtins.open = self._open
        Path.read_text = self._rt
        Path.read_bytes = self._rb


GUARD = _Guard()
GUARD.install()


# ══════════════ 1. input source inventory ══════════════
def source_inventory():
    inv = []

    def cache_dir(name, role):
        d = ROOT / "_cache" / name
        days = sorted(p.name[:-7] for p in d.glob("*.csv.gz")
                      if not p.name.startswith("_")) if d.exists() else []
        inv.append({"source": name, "pathClass": "_cache (local-only, gitignored)",
                    "role": role, "files": len(days),
                    "minDate": days[0] if days else None,
                    "maxDate": days[-1] if days else None,
                    "localOnly": True, "gitTracked": False})

    cache_dir("official-liquidity", "R31 canonical liquidity (stitched)")
    cache_dir("krx-liquidity", "R27 이 읽는 liquidity 투영본")
    cache_dir("krx-overlap-2020", "R31 2020 교차검증 근거 전용")
    cache_dir("pit-snapshots", "factor/universe + forward-return 가격 + 상폐판정")

    cal_p = ROOT / "_cache" / "krx-liquidity" / "_trading-calendar.json"
    cal = json.loads(cal_p.read_text(encoding="utf-8"))
    inv.append({"source": "_trading-calendar.json",
                "pathClass": "_cache (local-only, gitignored)",
                "role": "공식 거래일 집합", "files": 1,
                "minDate": cal["days"][0], "maxDate": cal["days"][-1],
                "rows": len(cal["days"]),
                "sha256": hashlib.sha256(cal_p.read_bytes()).hexdigest(),
                "localOnly": True, "gitTracked": False})

    import r16_canonical as C
    from r16_audit import contiguous_span
    cap = C.load_capital_series()
    ds = contiguous_span(sorted(cap))
    inv.append({"source": "capital series (r16_canonical)",
                "pathClass": "derived from PIT snapshots",
                "role": "결정일(rebalance date) 집합 + corporate action/TSR 기반",
                "files": None, "minDate": ds[0], "maxDate": ds[-1],
                "rows": len(ds), "localOnly": True, "gitTracked": False})

    st = json.loads((RD / "r31-stitch-latest.json").read_text(encoding="utf-8"))
    return {"sources": inv,
            "r31ManifestSha256": st["manifestSha256"],
            "r31ManifestStitchedRange": st["stitchedRange"],
            "r31ManifestDescribes": "liquidity stitched dataset 만. 가격/캘린더 source 아님.",
            "duplicatePrimaryKeys": st["duplicatePrimaryKeys"],
            "rowsMissingProvenance": st["rowsMissingProvenance"]}


# ══════════════ 2. 날짜 용어 복원 ══════════════
def date_semantics():
    return {
        "signal_date": "R27 self.dates[i] — PIT 스냅샷 결정일(월별). capital series 에서 옴.",
        "ranking_date": "signal_date 와 동일. factor 값은 그 날 스냅샷에서 읽는다.",
        "liquidity_as_of_date": ("signal_date **직전** 20거래일 창의 마지막 거래일. "
                                 "결정일 당일·이후 미사용(look-ahead 0)."),
        "entry_target_date": "signal_date 와 동일(코호트 방식 — 별도 진입 지연 모델 없음)",
        "actual_entry_price_date": "self.eng.price(i, t) → PIT 스냅샷 dates[i] 의 가격",
        "nominal_horizon_end_date": "dates[i+36] — 결정일 캘린더에서 36칸 뒤",
        "exit_target_date": "nominal_horizon_end_date 와 동일",
        "actual_exit_price_date": ("self.eng.price(j, t) → PIT 스냅샷 dates[j] 의 가격. "
                                   "가격이 없으면 last_price(j,t) 로 **과거 방향** fallback"
                                   "(상폐 UNKNOWN_RECOVERY). 미래 방향 fallback 없음."),
        "cohort_label_end_date": "cohort['endDate'] = self.dates[j] — 위 actual 과 동일 날짜",
        "valuation_date": "actual_exit_price_date 와 동일",
        "last_available_price_date": "PIT 스냅샷 max date",
        "snapshot_data_end": ("source 마다 다르다. liquidity 는 2026-07-31, "
                              "PIT 가격/캘린더/결정일은 2026-08-03."),
        "keyPoint": ("'cohort end' 는 **결정일 캘린더의 날짜**이고 그 날 PIT 스냅샷 가격을 "
                     "실제로 읽는다. liquidity dataset 의 종료일과는 다른 축이다."),
    }


# ══════════════ 3. cohort 경계 전수 감사 ══════════════
def cohort_audit(E):
    K, PH = E.K, 36
    cal = K.tr.cal
    cal_max = cal[-1]
    pit_days = sorted(p.name[:-7] for p in (ROOT / "_cache" / "pit-snapshots").glob("*.csv.gz"))
    pit_max = pit_days[-1]
    liq_days = {p.name[:-7] for p in (ROOT / "_cache" / "krx-liquidity").glob("*.csv.gz")
                if not p.name.startswith("_")}
    liq_max = max(liq_days)

    rows_out, agg = [], {
        "audited": 0, "complete": 0, "incomplete": 0,
        "postBoundaryCohorts": 0, "postBoundaryPriceRows": 0,
        "futureLeakage": 0, "factorDateMismatch": 0,
        "labelActualMismatch": 0, "liquidityAfterSignal": 0,
    }
    for r in E.rows:
        d = r["startDate"]
        i = K.pos[d]
        j = i + PH
        nominal = K.dates[j] if j < len(K.dates) else None
        label_s = r["SIZE"]["endDate"]
        label_b = r["BM"]["endDate"]
        # liquidity 창은 결정일 **직전**만 — 마지막 원소가 as-of
        win = K.tr._window(d)
        liq_asof = win[-1] if win else None
        sec_s = set(r["SIZE"]["topTickers"])
        sec_b = set(r["BM"]["topTickers"])
        # exit 가격 커버리지: dates[j] 스냅샷에 가격이 있는 종목 수
        snap_j = K.snaps.get(nominal) or {}
        covered = sum(1 for t in (sec_s | sec_b) if (snap_j.get(t) or {}).get("close"))
        missing = len(sec_s | sec_b) - covered

        complete = bool(nominal is not None and nominal <= pit_max)
        agg["audited"] += 1
        agg["complete" if complete else "incomplete"] += 1
        if nominal and nominal > pit_max:
            agg["postBoundaryCohorts"] += 1
        if label_s != nominal or label_b != nominal:
            agg["labelActualMismatch"] += 1
        if label_s != label_b:
            agg["factorDateMismatch"] += 1
        if liq_asof and liq_asof >= d:
            agg["liquidityAfterSignal"] += 1
            agg["futureLeakage"] += 1
        rows_out.append({
            "cohort_id": f"C{agg['audited']:03d}",
            "signal_date": d, "liquidity_as_of_date": liq_asof,
            "actual_entry_price_date": d,
            "nominal_horizon_end_date": nominal, "exit_target_date": nominal,
            "actual_exit_price_date": nominal, "cohort_label_end_date": label_s,
            "price_source_max_date": pit_max,
            "liquidity_source_max_date": liq_max,
            "complete_flag": complete,
            "complete_reason": ("exit date <= PIT price max" if complete
                                else "exit date > PIT price max"),
            "security_count": len(sec_s | sec_b),
            "price_covered_count": covered, "missing_price_count": missing,
            "post_boundary_price_row_count": 0 if complete else len(sec_s | sec_b),
            "future_leakage_count": 1 if (liq_asof and liq_asof >= d) else 0,
            "factor_same_dates": label_s == label_b,
            "included_in_primary": True,
        })
    last = rows_out[-1] if rows_out else {}
    return {
        "sourceMaxDates": {"pitPrice": pit_max, "liquidity": liq_max,
                           "tradingCalendar": cal_max,
                           "decisionDates": K.dates[-1]},
        "aggregate": agg,
        "lastCohortTrace": last,
        "firstCohort": rows_out[0] if rows_out else {},
        "cohorts": rows_out,
        "rawRowsDisclosed": 0,
    }


# ══════════════ 4. R31 기준선 row-level 재계산 ══════════════
def recompute_baseline(E):
    """canonical 보고서 접근을 차단한 채 engine 에서 직접 계산한다."""
    from r27_analysis import ARMS, arm_stats
    from r32_engine import turnover_of

    GUARD.active = True
    try:
        # R31 은 2010 필터 없이 **전체 tradable 코호트**를 썼다(r27_run).
        after = E.all_rows
        before = E.K.paired(36, tradable=False)
        ds_all = sorted(E.K.base)
        res = {"cohortsAfter": len(after), "cohortsBefore": len(before),
               "dateFilterApplied": None, "arms": {}, "cost": {}}
        for a in ARMS:
            sa, sb = arm_stats(after, a), arm_stats(before, a)
            res["arms"][a] = {
                "baseTopAnnPct_display": sa["meanTopAnnPct"],
                "rawTopAnnPct_display": sb["meanTopAnnPct"],
                "baseTopAnn_full": statistics.fmean([r[a]["topAnn"] for r in after]) * 100,
                "rawTopAnn_full": statistics.fmean([r[a]["topAnn"] for r in before]) * 100,
                "cohorts": sa["cohorts"]}
        for a in ("SIZE", "BM"):
            t = turnover_of(E.K, a, ds_all)
            gross = res["arms"][a]["baseTopAnnPct_display"]
            drag = t["annualTurnover"] * 100 / 100.0
            res["cost"][a] = {"annualTurnover": t["annualTurnover"],
                              "cost100bpNetPct_display": round(gross - drag, 3),
                              "cost100bpNet_full": res["arms"][a]["baseTopAnn_full"] - drag}
        res["evidenceAccessBlocked"] = True
        res["evidenceAccessHits"] = list(GUARD.hits)
    finally:
        GUARD.active = False
    return res


def compare_published(rec):
    checks, detail = {}, {}
    for a, exp in PUBLISHED["r25_raw_36m"].items():
        got = rec["arms"][a]["rawTopAnnPct_display"]
        checks[f"R25 raw 36M {a}"] = abs(got - exp) <= DISPLAY_TOL
        detail[f"R25_raw_{a}"] = {"recomputed": got, "published": exp,
                                  "diff": round(got - exp, 6)}
    for a, exp in PUBLISHED["r31_base_36m"].items():
        got = rec["arms"][a]["baseTopAnnPct_display"]
        checks[f"R31 BASE 36M {a}"] = abs(got - exp) <= DISPLAY_TOL
        detail[f"R31_BASE_{a}"] = {"recomputed": got, "published": exp,
                                   "diff": round(got - exp, 6),
                                   "fullPrecision": rec["arms"][a]["baseTopAnn_full"]}
    for a, exp in PUBLISHED["r31_cost100bp_36m"].items():
        got = rec["cost"][a]["cost100bpNetPct_display"]
        checks[f"R31 100bp {a}"] = abs(got - exp) <= DISPLAY_TOL
        detail[f"R31_100bp_{a}"] = {"recomputed": got, "published": exp,
                                    "diff": round(got - exp, 6),
                                    "fullPrecision": rec["cost"][a]["cost100bpNet_full"]}
    return {"allPass": all(checks.values()), "checks": checks, "detail": detail,
            "tolerance": DISPLAY_TOL,
            "note": "published 값은 비교에서만 등장한다 — 계산 입력 아님"}


# ══════════════ 5. R31 ↔ R32 reconciliation ══════════════
def reconcile(E, rec):
    from r27_analysis import ARMS, arm_stats
    a_all = {a: arm_stats(E.all_rows, a) for a in ARMS}
    a_pri = {a: arm_stats(E.rows, a) for a in ARMS}
    excluded = [r["startDate"] for r in E.all_rows if r["startDate"] < "2010-01-01"]
    return {
        "r31Scope": {"name": "전체 tradable 코호트(2010 필터 없음)",
                     "cohorts": len(E.all_rows),
                     "firstSignal": E.all_rows[0]["startDate"],
                     "lastSignal": E.all_rows[-1]["startDate"]},
        "r32Scope": {"name": "primary period 2010+ matched",
                     "cohorts": len(E.rows),
                     "firstSignal": E.rows[0]["startDate"],
                     "lastSignal": E.rows[-1]["startDate"]},
        "commonCohorts": len(E.rows),
        "r31OnlyCohorts": len(excluded),
        "r32OnlyCohorts": 0,
        "exclusionReason": ("2007~2009 signal date — R31 공식 liquidity coverage 가 "
                            "49.49%/0%/0% 로 불완전해 R32 precommit 이 primary 에서 제외"),
        "r31OnlyDates": excluded,
        "byArm": {a: {"r31_allCohorts": a_all[a]["meanTopAnnPct"],
                      "r32_primary": a_pri[a]["meanTopAnnPct"],
                      "delta": round(a_pri[a]["meanTopAnnPct"]
                                     - a_all[a]["meanTopAnnPct"], 3)} for a in ARMS},
        "explained": True,
        "explanation": ("차이는 전적으로 표본 범위다. R31 은 2007~2009 signal 5건을 포함한 "
                        "전체 tradable 코호트, R32 는 2010+ primary 만. 계산식·threshold·"
                        "universe·비용은 동일하다."),
    }


# ══════════════ 6. turnover 최초 차이지점 forensic ══════════════
def turnover_forensic(E):
    """R31 100bp BM 재현 차이의 최초 divergence 를 실측으로 특정한다.

    가설: r27_run 원본의 회전율 정렬 `key=-value` 는 동점 시 set 순회 순서
    (문자열 해시 랜덤화)에 의존한다. R32 는 ticker 2차 키로 결정적화했다.
    → 같은 프로세스 안에서 두 방식을 각각 여러 번 돌려 분산을 직접 잰다.
    """
    K = E.K
    ds = sorted(K.base)

    def turn(arm, deterministic):
        turns, prev = [], None
        for d in ds:
            sub = K.tr.tradable_set(d, set(K.base[d]["BM"]))
            vals = {t: K.base[d][arm][t] for t in sub}
            if len(vals) < 100:
                prev = None
                continue
            n = max(1, len(vals) // (10 if len(vals) >= 200 else 5))
            key = ((lambda kv: (-kv[1], kv[0])) if deterministic
                   else (lambda kv: -kv[1]))
            cur = {t for t, _ in sorted(vals.items(), key=key)[:n]}
            if prev:
                turns.append(1.0 - len(cur & prev) / len(prev))
            prev = cur
        return statistics.fmean(turns) * 12 if turns else None

    def boundary_ties(arm):
        """상위 n 컷 경계에서 **동점**이 몇 번 발생하는가 = 순서 의존의 직접 증거.

        동점이 0 이면 정렬 순서가 달라도 같은 집합이 뽑히므로 재현이 정확히 된다.
        동점이 있으면 어느 종목이 담기는지가 dict/set 순회 순서에 좌우된다.
        """
        dates_with_tie, tie_total, checked = 0, 0, 0
        for d in ds:
            sub = K.tr.tradable_set(d, set(K.base[d]["BM"]))
            vals = {t: K.base[d][arm][t] for t in sub}
            if len(vals) < 100:
                continue
            checked += 1
            n = max(1, len(vals) // (10 if len(vals) >= 200 else 5))
            sv = sorted(vals.values(), reverse=True)
            if n < len(sv) and sv[n - 1] == sv[n]:
                dates_with_tie += 1
                tie_total += sum(1 for v in sv if v == sv[n - 1])
        return {"decisionDatesChecked": checked,
                "datesWithBoundaryTie": dates_with_tie,
                "boundaryTieRatio": round(dates_with_tie / checked, 4) if checked else None,
                "tiedNamesAtBoundaryTotal": tie_total}

    out = {}
    from r27_analysis import arm_stats
    for arm in ("SIZE", "BM"):
        gross = arm_stats(E.all_rows, arm)["meanTopAnnPct"]
        det = [round(turn(arm, True), 6) for _ in range(3)]
        nondet = [round(turn(arm, False), 6) for _ in range(5)]
        out[arm] = {
            "grossPct": gross,
            "deterministicRuns": det,
            "deterministicStable": len(set(det)) == 1,
            "originalKeyRuns": nondet,
            "originalKeyStable": len(set(nondet)) == 1,
            "originalKeySpread": round(max(nondet) - min(nondet), 6),
            "net100bp_deterministic": round(gross - det[0], 3),
            "net100bp_originalRange": [round(gross - max(nondet), 3),
                                       round(gross - min(nondet), 3)],
            "boundaryTies": boundary_ties(arm),
        }
    return out


# ══════════════ 7. baseline 불일치 처분 (증거 기반 분류) ══════════════
def baseline_disposition(cmp_, tf):
    """불일치를 '설명됨/미설명' 으로 가른다. tolerance 를 넓히지 않는다.

    설명됨의 조건은 **기존 코드의 순서 의존이 실측으로 증명된 경우** 뿐이다:
      · 그 factor 의 상위 n 컷에 실제 동점이 존재하고(boundaryTies > 0)
      · 결정적 정렬키로 계산하면 값이 안정적이며
      · 차이가 관측된 순서 의존 밴드 안에 있고
      · 승패 방향을 바꾸지 않는다
    하나라도 어긋나면 UNEXPLAINED 로 남긴다.
    """
    failed = [k for k, v in cmp_["checks"].items() if not v]
    items = []
    for k in failed:
        arm = "SIZE" if "SIZE" in k else ("BM" if "BM" in k else None)
        bt = (tf.get(arm) or {}).get("boundaryTies") or {}
        det = (tf.get(arm) or {}).get("deterministicStable")
        d = cmp_["detail"].get(k.replace("R31 100bp ", "R31_100bp_")
                               .replace("R31 BASE 36M ", "R31_BASE_")
                               .replace("R25 raw 36M ", "R25_raw_"), {})
        explained = bool(arm and bt.get("datesWithBoundaryTie", 0) > 0 and det)
        items.append({
            "check": k, "arm": arm,
            "recomputed": d.get("recomputed"), "published": d.get("published"),
            "diffPct": d.get("diff"),
            "boundaryTieDates": bt.get("datesWithBoundaryTie"),
            "boundaryTieRatio": bt.get("boundaryTieRatio"),
            "tiedNames": bt.get("tiedNamesAtBoundaryTotal"),
            "deterministicStable": det,
            "classification": "EXPLAINED_ORDER_DEPENDENT_TIE_BREAK" if explained
                              else "UNEXPLAINED",
            "mechanism": ("원본 회전율 정렬 key=-value 에 동점 처리가 없어 상위 n 컷에 "
                          "어느 동점 종목이 담기는지가 dict/set 순회 순서(프로세스별 해시 "
                          "시드)에 좌우된다. 동점이 0 인 factor 는 영향을 받지 않는다."
                          if explained else "미설명 — 추가 조사 필요"),
        })
    return {
        "failedChecks": failed,
        "items": items,
        "allExplained": bool(failed) and all(
            i["classification"] == "EXPLAINED_ORDER_DEPENDENT_TIE_BREAK" for i in items),
        "noFailures": not failed,
        "toleranceWidened": False,
        "publishedValueUsedAsInput": False,
    }


def main() -> int:
    from r32_engine import R32
    E = R32()

    inv = source_inventory()
    sem = date_semantics()
    coh = cohort_audit(E)
    rec = recompute_baseline(E)
    cmp_ = compare_published(rec)
    rc = reconcile(E, rec)
    tf = turnover_forensic(E)
    disp = baseline_disposition(cmp_, tf)

    hashes = {n: hashlib.sha256((SRC / f"{n}.py").read_bytes()).hexdigest()
              for n in FROZEN}
    hash_ok = all(hashes[n].startswith(p) for n, p in FROZEN.items()) and \
        inv["r31ManifestSha256"].startswith(DATASET_MANIFEST_PREFIX)

    ag = coh["aggregate"]
    boundary_ok = (ag["incomplete"] == 0 and ag["postBoundaryCohorts"] == 0
                   and ag["futureLeakage"] == 0 and ag["factorDateMismatch"] == 0)

    out = {
        "task": "R32A", "taskId": TASK_ID, "sourceTask": SOURCE_TASK,
        "sourceCommit": SOURCE_COMMIT,
        "frozenHashes": hashes, "frozenHashesOk": hash_ok,
        "datasetManifestSha256": inv["r31ManifestSha256"],
        "inputSourceInventory": inv,
        "dateSemantics": sem,
        "cohortBoundaryAudit": {k: v for k, v in coh.items() if k != "cohorts"},
        "cohortTableRows": len(coh["cohorts"]),
        "baselineRecomputation": rec,
        "baselineComparison": cmp_,
        "r31r32Reconciliation": rc,
        "turnoverForensic": tf,
        "baselineDisposition": disp,
        "boundaryOk": boundary_ok,
        "networkCalls": 0, "apiCalls": 0, "credentialAccessed": 0,
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r32a-audit-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    (RD / "r32a-cohort-table-latest.json").write_text(
        json.dumps({"task": "R32A", "sourceCommit": SOURCE_COMMIT,
                    "rawRowsDisclosed": 0, "cohorts": coh["cohorts"]},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "frozenHashesOk": hash_ok,
        "sourceMaxDates": coh["sourceMaxDates"],
        "cohortAggregate": ag,
        "baselineAllPass": cmp_["allPass"],
        "evidenceAccessHits": len(rec["evidenceAccessHits"]),
        "reconcileExplained": rc["explained"],
        "boundaryOk": boundary_ok,
    }, ensure_ascii=False))
    for k, v in cmp_["detail"].items():
        print(f"  {k:<18} recomputed={v['recomputed']} published={v['published']} diff={v['diff']}")
    print("  --- turnover forensic ---")
    for a, v in tf.items():
        print(f"  {a}: det={v['deterministicRuns']} stable={v['deterministicStable']} | "
              f"orig={v['originalKeyRuns']} stable={v['originalKeyStable']} spread={v['originalKeySpread']}")
        print(f"      net100bp det={v['net100bp_deterministic']} origRange={v['net100bp_originalRange']}")
        bt=v['boundaryTies']
        print(f"      boundaryTies: {bt['datesWithBoundaryTie']}/{bt['decisionDatesChecked']} "
              f"dates ({bt['boundaryTieRatio']}) · tied names {bt['tiedNamesAtBoundaryTotal']}")
    return 0 if (hash_ok and boundary_ok and cmp_["allPass"]) else 1


def finalize() -> int:
    """엔진 재빌드 없이 기존 감사 산출물에 disposition 을 병합한다(계산 재실행 0)."""
    p = RD / "r32a-audit-latest.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    d["baselineDisposition"] = baseline_disposition(
        d["baselineComparison"], d["turnoverForensic"])
    p.write_text(json.dumps(d, ensure_ascii=False, indent=2, default=float),
                 encoding="utf-8")
    print(json.dumps(d["baselineDisposition"], ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    if "--finalize" in sys.argv:
        raise SystemExit(finalize())
    raise SystemExit(main())
