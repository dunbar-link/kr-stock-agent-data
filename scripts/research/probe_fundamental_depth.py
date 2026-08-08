#!/usr/bin/env python3
"""PHASE 1 보조 probe — pykrx fundamental 의 (a) 최대 소급 깊이 (b) ROE 항등식 성립 여부.

왜 중요한가
-----------
DART 기반 원전 공식(EBIT/EV, EBIT/(NWC+PPE))은 로컬 캐시가 FY2022~FY2025 뿐이라
장기 백테스트가 불가능하다. 반면 KRX 가 매일 공표하는 PER/PBR/EPS/BPS 는
"그 날 시점에 공시돼 있던 최신 재무"로 계산되므로 **구조적으로 point-in-time** 이다.

  EarningsYield(proxy) = 1 / PER            (주주이익수익률)
  ReturnOnEquity(proxy) = PBR / PER         (= (P/B)/(P/E) = E/B)

이 항등식이 실데이터에서 성립하면, 저장소가 이미 갖고 있는 approx 모드
(ROE·PER 기반 마법공식)를 장기간 PIT 로 재현할 수 있다. 이 스크립트는 그것을 실측한다.

안전: read-only 조회. write 는 --out 1개뿐.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

DEEP_DATES = ["19990104", "20000104", "20030102", "20050103", "20070102", "20090102", "20100104"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)
    from pykrx import stock  # noqa: PLC0415

    rep = {"stage": "FUNDAMENTAL_DEPTH_PROBE", "project": "wababa",
           "createdAt": datetime.now().isoformat(timespec="seconds"),
           "realOrderCount": 0, "brokerApiCallCount": 0, "depth": [], "roeIdentity": []}

    # (a) 얼마나 과거까지 fundamental 이 나오는가
    for d in DEEP_DATES:
        row = {"date": d}
        try:
            df = stock.get_market_fundamental_by_ticker(d, market="ALL")
            row["rows"] = 0 if df is None else len(df)
            row["cols"] = None if df is None else list(df.columns)
            if df is not None and len(df):
                row["perPositive"] = int((df["PER"] > 0).sum())
                row["pbrPositive"] = int((df["PBR"] > 0).sum())
        except Exception as e:  # noqa: BLE001
            row["error"] = f"{type(e).__name__}: {e}"
        try:
            cap = stock.get_market_cap_by_ticker(d, market="ALL")
            row["capRows"] = 0 if cap is None else len(cap)
        except Exception as e:  # noqa: BLE001
            row["capError"] = f"{type(e).__name__}: {e}"
        rep["depth"].append(row)
        print(f"[depth] {d} rows={row.get('rows')} cap={row.get('capRows')}", file=sys.stderr)

    # (b) ROE = PBR / PER 항등식 검증 (여러 시점)
    for d in ("20100104", "20160104", "20230102", "20260807"):
        item = {"date": d}
        try:
            f = stock.get_market_fundamental_by_ticker(d, market="ALL")
            sub = f[(f["PER"] > 0) & (f["PBR"] > 0) & (f["EPS"] > 0) & (f["BPS"] > 0)]
            item["sampleN"] = int(len(sub))
            if len(sub):
                implied = sub["PBR"] / sub["PER"]        # = E/B
                direct = sub["EPS"] / sub["BPS"]          # = ROE 직접
                rel = ((implied - direct).abs() / direct.abs()).dropna()
                item["relErrMedian"] = float(rel.median())
                item["relErrP90"] = float(rel.quantile(0.9))
                item["within1pct"] = float((rel < 0.01).mean())
                item["roeMedian"] = float(direct.median())
        except Exception as e:  # noqa: BLE001
            item["error"] = f"{type(e).__name__}: {e}"
        rep["roeIdentity"].append(item)
        print(f"[roe] {d} n={item.get('sampleN')} within1pct={item.get('within1pct')}", file=sys.stderr)

    txt = json.dumps(rep, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(txt)
    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
