#!/usr/bin/env python3
"""R13 factor 값 계산기 — TRACK L(장기 KRX PIT) + TRACK D(DART 실물, 실제 공시일 PIT).

WABABA-VALUE-PROFITABILITY-FACTOR-DISCOVERY-R13

정의는 전부 `r13_precommit.py` 에 결과 이전에 고정돼 있다. 이 파일은 그 정의를
구현만 한다. 성과를 보고 정의를 바꾸지 않는다(§5).

★ look-ahead 방지가 최우선(§4)
  TRACK L : L1·L3 은 결정월 t 의 **이전** 스냅샷만 본다(t 자신도 안 본다).
            L2 는 t 스냅샷의 KRX 공표 DPS/EPS 만 본다.
  TRACK D : (기업, 회계연도) 재무는 **실제 공시일(rcept_no) + 1개월** 이후의 결정월
            부터만 사용한다. 결정월 시점에 공시된 것 중 가장 최근 회계연도를 쓴다.
            공시일 이전 사용은 테스트에서 FAIL 처리된다.

안전: 계산 전용 · 네트워크 0 · 파일 write 0 · canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import glob
import gzip
import json
import os
import re
import sys
from bisect import bisect_right
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parents[2]
SNAP_DIR = ROOT / "_cache" / "pit-snapshots"
DART_DIR = ROOT / "_cache" / "dart-statements"
CORP_CODES = ROOT / "_cache" / "dart-corp-codes.json"

L1, L2, L3 = "L1_EARNINGS_PERSISTENCE", "L2_CASH_PAYOUT_QUALITY", "L3_CAPITAL_COMPOUNDING"
D1, D2, D3 = "D1_ROIC_PROXY", "D2_OPERATING_PROFITABILITY", "D3_CASH_PROFITABILITY"
TRACK_L_IDS = [L1, L2, L3]
TRACK_D_IDS = [D1, D2, D3]

PERSIST_WINDOW = 36        # L1 직전 36개월
PERSIST_MIN_OBS = 12       # L1 최소 유효 관측
BPS_WINDOW = 36            # L3 직전 36개월
PAYOUT_CAP = 3.0           # L2 배당성향 절단 (사전확정)
DART_LAG_MONTHS = 1        # TRACK D 공시일 + 1개월 버퍼

IFRS = {
    "assets": "ifrs-full_Assets",
    "curliab": "ifrs-full_CurrentLiabilities",
    "op": "dart_OperatingIncomeLoss",
    "ocf": "ifrs-full_CashFlowsFromUsedInOperatingActivities",
    "rev": "ifrs-full_Revenue",
    "eq": "ifrs-full_Equity",
}


def _f(x):
    if x is None or x == "" or x == "nan":
        return None
    try:
        v = float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None
    return None if (v != v or v in (float("inf"), float("-inf"))) else v


# ═══════════════ TRACK L — 장기 스냅샷 보조 시계열 ═══════════════
def load_dps_series(snap_dir: Path = SNAP_DIR):
    """DPS 를 직접 읽는다.

    backtest_engine.load_snapshots 는 DIV/DPS 컬럼을 파싱하지 않는다(실측 — 그래서
    R7 의 DY factor 가 UNRELIABLE 로 나왔다). L2 에 필요하므로 여기서 따로 읽는다.
    """
    out = {}
    for p in sorted(snap_dir.glob("[12]*.csv.gz")):
        iso = p.name.split(".")[0]
        row = {}
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            head = fh.readline().rstrip("\n").split(",")
            ix = {c: i for i, c in enumerate(head)}
            if "DPS" not in ix:
                continue
            for line in fh:
                c = line.rstrip("\n").split(",")
                if len(c) < len(head):
                    continue
                row[c[ix["ticker"]]] = _f(c[ix["DPS"]])
        if row:
            out[iso] = row
    return out


class LongFactors:
    """TRACK L 값 계산기. 결정월 이전 스냅샷만 사용한다."""

    def __init__(self, snapshots, dates, dps_series=None):
        self.sn = snapshots
        self.dates = list(dates)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self.dps = dps_series if dps_series is not None else load_dps_series()
        self._cache = {}

    # ── L1 ────────────────────────────────────────────────────────────
    def persistence(self, uni, d, i):
        lo = max(0, i - PERSIST_WINDOW)
        window = self.dates[lo:i]          # ★ i 미포함 = 당월 정보도 안 쓴다
        out = {}
        for t in uni:
            pos = tot = 0
            for wd in window:
                r = self.sn[wd].get(t)
                if not r:
                    continue
                e = r["EPS"]
                if e is None:
                    continue
                tot += 1
                if e > 0:
                    pos += 1
            if tot >= PERSIST_MIN_OBS:
                out[t] = pos / tot
        return out

    # ── L2 ────────────────────────────────────────────────────────────
    def payout(self, uni, d, i):
        dps_now = self.dps.get(d, {})
        out = {}
        for t, r in uni.items():
            e = r["EPS"]
            if e is None or e <= 0:
                continue                    # 정의 불가 → 랭킹 제외
            dp = dps_now.get(t)
            v = 0.0 if (dp is None or dp <= 0) else dp / e   # 결측/0 = 배당성향 0
            out[t] = min(v, PAYOUT_CAP)
        return out

    # ── L3 ────────────────────────────────────────────────────────────
    def compounding(self, uni, d, i):
        j0, j1 = i - BPS_WINDOW, i - 1
        if j0 < 0 or j1 < 0:
            return {}
        d0, d1 = self.dates[j0], self.dates[j1]
        s0, s1 = self.sn[d0], self.sn[d1]
        months = j1 - j0
        out = {}
        for t in uni:
            a, b = s0.get(t), s1.get(t)
            if not a or not b:
                continue
            b0, b1 = a["BPS"], b["BPS"]
            if b0 is None or b1 is None or b0 <= 0 or b1 <= 0:
                continue
            out[t] = (b1 / b0) ** (12.0 / months) - 1.0
        return out

    def all_values(self, d, i):
        """날짜별로 **전 종목** 값을 한 번만 계산해 캐시한다.

        uni 를 키에 넣으면(전체 universe / BM 상위20% 등 호출마다 다름) 캐시가
        어긋나거나 id 재사용으로 잘못 맞을 수 있다. 값 자체는 uni 와 무관하므로
        날짜만으로 캐시하고 호출부에서 uni 로 걸러낸다.
        """
        if d in self._cache:
            return self._cache[d]
        allt = {t: r for t, r in self.sn[d].items()}
        v = {L1: self.persistence(allt, d, i),
             L2: self.payout(allt, d, i),
             L3: self.compounding(allt, d, i)}
        self._cache[d] = v
        return v

    def values(self, uni, d, i):
        av = self.all_values(d, i)
        return {k: {t: x for t, x in vals.items() if t in uni}
                for k, vals in av.items()}


# ═══════════════ TRACK D — DART 실물 (실제 공시일 PIT) ═══════════════
def _add_months(iso, k):
    y, m = int(iso[:4]), int(iso[5:7])
    m += k
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return f"{y:04d}-{m:02d}"


def load_dart_facts(dart_dir: Path = DART_DIR, corp_codes: Path = CORP_CODES):
    """(ticker) -> [ {fy, availFrom, roic, op, cp} ...] — availFrom 이후에만 사용 가능.

    availFrom = 실제 공시일(rcept_no 앞 8자리)의 다음달 + DART_LAG_MONTHS.
    """
    codes = json.loads(corp_codes.read_text(encoding="utf-8"))
    c2t = {v["corp_code"]: t for t, v in codes.items()}
    by_ticker = {}
    diag = {"files": 0, "mapped": 0, "noRcept": 0, "rows": 0,
            "d1": 0, "d2": 0, "d3": 0}
    for f in glob.glob(str(dart_dir / "*_CFS.json")):
        diag["files"] += 1
        m = re.match(r"(\d+)_(\d{4})_CFS\.json", os.path.basename(f))
        if not m:
            continue
        corp, fy = m.group(1), int(m.group(2))
        t = c2t.get(corp)
        if not t:
            continue
        diag["mapped"] += 1
        try:
            rows = json.loads(Path(f).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        vals, rcept = {}, None
        for r in rows:
            aid = (r.get("account_id") or "").strip()
            for k, tag in IFRS.items():
                if aid == tag and k not in vals:
                    v = _f(r.get("thstrm_amount"))
                    if v is not None:
                        vals[k] = v
            if rcept is None and r.get("rcept_no"):
                rcept = str(r["rcept_no"])[:8]
        if not rcept or len(rcept) != 8 or not rcept.isdigit():
            diag["noRcept"] += 1
            continue
        filed = f"{rcept[:4]}-{rcept[4:6]}-{rcept[6:8]}"
        # 공시월의 다음달 + 버퍼부터 사용 가능
        avail = _add_months(filed[:7], 1 + DART_LAG_MONTHS)
        a, cl, op, ocf = (vals.get("assets"), vals.get("curliab"),
                          vals.get("op"), vals.get("ocf"))
        rec = {"fy": fy, "filed": filed, "availFrom": avail,
               D1: None, D2: None, D3: None}
        ic = (a - cl) if (a is not None and cl is not None) else None
        if op is not None and ic is not None and ic > 0:
            rec[D1] = op / ic
            diag["d1"] += 1
        if op is not None and a is not None and a > 0:
            rec[D2] = op / a
            diag["d2"] += 1
        if ocf is not None and a is not None and a > 0:
            rec[D3] = ocf / a
            diag["d3"] += 1
        by_ticker.setdefault(t, []).append(rec)
        diag["rows"] += 1
    for t in by_ticker:
        by_ticker[t].sort(key=lambda r: (r["availFrom"], r["fy"]))
    return by_ticker, diag


class DartFactors:
    """TRACK D 값 계산기. 공시일 이전에는 절대 값을 내주지 않는다."""

    def __init__(self, facts=None, diag=None):
        if facts is None:
            facts, diag = load_dart_facts()
        self.facts = facts
        self.diag = diag or {}
        self._avail = {t: [r["availFrom"] for r in rs] for t, rs in facts.items()}

    def latest_available(self, ticker, ym):
        """결정월 ym 시점에 이미 공시된 것 중 가장 최근 회계연도 레코드."""
        rs = self.facts.get(ticker)
        if not rs:
            return None
        k = bisect_right(self._avail[ticker], ym) - 1
        if k < 0:
            return None
        best = None
        for r in rs[:k + 1]:
            if best is None or r["fy"] > best["fy"]:
                best = r
        return best

    def values(self, uni, d, i):
        ym = d[:7]
        out = {D1: {}, D2: {}, D3: {}}
        for t in uni:
            rec = self.latest_available(t, ym)
            if not rec:
                continue
            for fid in (D1, D2, D3):
                if rec[fid] is not None:
                    out[fid][t] = rec[fid]
        return out

    def coverage(self, uni, d):
        ym = d[:7]
        n = len(uni)
        got = sum(1 for t in uni if self.latest_available(t, ym))
        return got, n


# ═══════════════ 결합 value_fn (quantile_panel 주입용) ═══════════════
def make_value_fn(longf=None, dartf=None, only=None):
    def fn(uni, d, i):
        out = {}
        if longf is not None:
            out.update(longf.values(uni, d, i))
        if dartf is not None:
            out.update(dartf.values(uni, d, i))
        if only:
            out = {k: v for k, v in out.items() if k in only}
        return out
    return fn


# ═══════════════ BM 상위 20% universe filter (§8) ═══════════════
def make_bm_top20_filter(percentile=0.20):
    """R11 frozen P20 그대로. eligible = investable ∩ PBR>0 → BM 내림차순 상위 20%."""
    def fn(uni, d, i):
        items = [(t, 1.0 / r["PBR"]) for t, r in uni.items()
                 if r["PBR"] and r["PBR"] > 0]
        if not items:
            return {}
        items.sort(key=lambda kv: (-kv[1], kv[0]))
        width = max(1, int(len(items) * percentile))
        keep = {t for t, _ in items[:width]}
        return {t: r for t, r in uni.items() if t in keep}
    return fn


__all__ = ["L1", "L2", "L3", "D1", "D2", "D3", "TRACK_L_IDS", "TRACK_D_IDS",
           "LongFactors", "DartFactors", "load_dps_series", "load_dart_facts",
           "make_value_fn", "make_bm_top20_filter", "DART_LAG_MONTHS",
           "PERSIST_WINDOW", "BPS_WINDOW", "PAYOUT_CAP", "IFRS"]
