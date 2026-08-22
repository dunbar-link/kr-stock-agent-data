#!/usr/bin/env python3
"""R17 rights-aware wealth 계층 — R16 canonical engine 위에 유상증자 의미론을 얹는다.

WABABA-DART-RIGHTS-ISSUE-CANONICAL-RECOVERY-R17

R16 산출물을 덮어쓰지 않는다(§24). R16 엔진은 그대로 두고, DART 로 확정된 사건만
override 하는 얇은 계층이다.

정책(§10, precommit 고정): POLICY_A_ASSUME_FULL_EXERCISE + TIME_WEIGHTED
  주주배정형에서 보유자가 배정분을 청약했다고 본다. 추가 납입금은
  EXTERNAL_CONTRIBUTION 이며 **투자수익이 아니다**.

  W0 = S · P_cum                      (권리락 전 wealth)
  C  = S · r · K                      (외부 납입)
  W1 = S(1+r) · P_ex                  (권리락 후 wealth)
  TWR_t = (W1 - C) / W0 - 1           (외부 현금흐름 중립화)

  공정가격 P_ex = (P_cum + rK)/(1+r) 이면 TWR_t = 0 → 공정한 유상증자는
  wealth 중립. 이것이 §13 ex-rights sanity 의 정의다.

★ 자체수정 (§29, 공개 기록): precommit 은 '전량 청약'이라고만 썼다. 그러나
  발행가 K 가 권리락 주가 P_ex 보다 **높으면** 청약은 손해이고 합리적 주주는
  실권한다. 그대로 강제하면 엔진이 존재하지 않는 손실을 만든다. 따라서
  **K < P_ex 일 때만 청약**하고 아니면 실권(신주 0·납입 0)으로 처리한다.
  이 조정은 R16 대비 보정폭을 **줄이는** 방향이다(= 보수적).

라벨별 처리:
  CONFIRMED_BONUS_ISSUE        기계적 조정(무상이므로 납입 없음)
  CONFIRMED_RIGHTS             권리 배정 + 외부납입 분리 (holderRight)
  CONFIRMED_THIRD_PARTY_ISSUE  미조정 — 기존 주주는 신주를 받지 않는다
  CONFIRMED_PUBLIC_OFFERING    미조정 — 동일
  CONFIRMED_OTHER_CAPITAL_ACTION 미조정
  UNRESOLVED                   미조정 (R16 동작 유지 = 보수적)

안전: 계산 전용 · 네트워크 0 · 파일 write 0(호출자가 저장).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r16_canonical as C  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

ADJUST_MECHANICAL = {"CONFIRMED_BONUS_ISSUE"}
NO_ADJUST = {"CONFIRMED_THIRD_PARTY_ISSUE", "CONFIRMED_PUBLIC_OFFERING",
             "CONFIRMED_OTHER_CAPITAL_ACTION", "UNRESOLVED"}


def load_matches(path=None):
    p = Path(path) if path else (RD / "r17-rights-matching-latest.json")
    return json.loads(p.read_text(encoding="utf-8"))["rows"]


class RightsWealth:
    """R16 엔진 + DART 확정 사건 override."""

    def __init__(self, eng: "C.CanonicalWealth", matches):
        self.eng = eng
        self.ov = {}                       # (ticker, date) -> plan
        # 계획 단위 집계(사건 1건당 1회). run 호출 횟수와 섞이면 오해를 부른다.
        self.stats = {"MECHANICAL_APPLIED": 0, "RIGHTS_PLANNED": 0,
                      "RIGHTS_TERMS_MISSING": 0, "NO_ADJUST": 0}
        # run 단위 집계(호출마다 누적). 진단용이며 사건 수가 아니다.
        self.runStats = {"RIGHTS_EXERCISED": 0, "RIGHTS_LAPSED": 0}
        for m in matches:
            self.ov[(m["ticker"], m["date"])] = self._plan(m)

    # ── 사건 계획 ────────────────────────────────────────────────────
    def _plan(self, m):
        label = m["label"]
        if label in ADJUST_MECHANICAL:
            self.stats["MECHANICAL_APPLIED"] += 1
            return {"kind": "MECHANICAL", "factor": m["shareRatio"], "label": label}
        if label == "CONFIRMED_RIGHTS" and m.get("holderRight"):
            k = m.get("issuePriceDerived")
            r = m.get("dartRatio") or m.get("observedRatio")
            if not k or not r or r <= 0:
                self.stats["RIGHTS_TERMS_MISSING"] += 1
                return {"kind": "NO_ADJUST", "label": label,
                        "flag": "RIGHTS_TERMS_INCOMPLETE"}
            self.stats["RIGHTS_PLANNED"] += 1
            return {"kind": "RIGHTS", "ratio": r, "issuePrice": k, "label": label}
        self.stats["NO_ADJUST"] += 1
        return {"kind": "NO_ADJUST", "label": label}

    # ── 사건 적용 ────────────────────────────────────────────────────
    def apply(self, plan, shares, p_ex):
        """(new_shares, external_contribution, note) 반환. 공짜 신주를 만들지 않는다."""
        if plan["kind"] == "MECHANICAL":
            return shares * plan["factor"], 0.0, "MECHANICAL"
        if plan["kind"] == "RIGHTS":
            k, r = plan["issuePrice"], plan["ratio"]
            if not p_ex or k >= p_ex:                 # 합리적 실권
                return shares, 0.0, "LAPSED"
            new = shares * r
            return shares + new, new * k, "EXERCISED"
        return shares, 0.0, "NO_ADJUST"

    # ── TWR wealth ledger ────────────────────────────────────────────
    def run(self, ticker, i, j, *, reinvest=True, ledger=False):
        """시간가중 수익률. 외부 납입은 중립화되어 gain 이 되지 않는다."""
        eng = self.eng
        p0 = eng.price(i, ticker)
        if not p0 or p0 <= 0:
            return None
        shares, twr = 1.0, 1.0
        total_ext, rows = 0.0, []
        for x in range(i + 1, j + 1):
            p_prev = eng.price(x - 1, ticker)
            p_cur = eng.price(x, ticker)
            w0 = shares * p_prev if p_prev else None
            ext, note = 0.0, None

            e = eng.events[x].get(ticker)
            plan = self.ov.get((ticker, eng.dates[x]))
            if plan:
                shares, ext, note = self.apply(plan, shares, p_cur)
                if note == "EXERCISED":
                    self.runStats["RIGHTS_EXERCISED"] += 1
                elif note == "LAPSED":
                    self.runStats["RIGHTS_LAPSED"] += 1
            elif e and e["adjust"]:
                shares *= e["shareRatio"]
                note = "R16_MECHANICAL"

            if reinvest:
                y, _ = eng.dividend_state(x, ticker)
                if y:
                    shares *= (1.0 + y)

            w1 = shares * p_cur if p_cur else None
            if w0 and w1 is not None and w0 > 0:
                twr *= (w1 - ext) / w0        # ★ 외부 납입 중립화
            total_ext += ext
            if ledger:
                rows.append({"date": eng.dates[x], "wealth_before": w0,
                             "rights_entitlement": (plan or {}).get("ratio"),
                             "external_contribution": ext,
                             "shares_received": shares, "wealth_after": w1,
                             "note": note})
        out = {"ticker": ticker, "start": eng.dates[i], "end": eng.dates[j],
               "twr": twr - 1.0, "externalContribution": total_ext,
               "endShares": shares}
        if ledger:
            out["ledger"] = rows
        return out


def understatement(m, eng):
    """R16 미조정이 놓친 크기 = 기존 1주당 권리 내재가치 / 권리락 전 주가.

    understate = r · max(0, P_ex - K) / P_cum
    (파생: adjusted - unadjusted = r(P_ex - K)/P_cum, 실권 시 0)
    """
    i = eng.pos.get(m["date"])
    if i is None or i == 0:
        return None
    p_cum = eng.price(i - 1, m["ticker"])
    p_ex = eng.price(i, m["ticker"])
    k = m.get("issuePriceDerived")
    r = m.get("dartRatio") or m.get("observedRatio")
    if not (p_cum and p_ex and k and r) or p_cum <= 0:
        return None
    return r * max(0.0, p_ex - k) / p_cum
