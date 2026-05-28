"""
Phase 37-A11 — Portfolio Snapshot Persistence
---------------------------------------------
recommendation-history.json의 펀드별 portfolio 구성을 읽어
일자별 ratios + state/action 분류를 portfolio-state-snapshots.json에
누적 저장한다.

원칙
- ranking/scoring/자동매매/recommendation-history schema 무수정.
- snapshot 파일은 별도(data/portfolio-state-snapshots.json).
- 같은 (date, fundKey)는 update, 없으면 append.
- ratios는 모두 0~100 범위로 통일.
- 분류 룰은 TS lib(build-portfolio-state / build-portfolio-action) 기준과
  최대한 동일하게 재현한다.
- 실패해도 daily_run 자체는 영향 받지 않도록 호출 측에서 try/except 격리.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Windows 콘솔 cp949에서 일부 특수문자(em-dash 등)가 깨지지 않도록 stdout utf-8 강제.
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except (AttributeError, ValueError):
    pass

ROOT_DIR = Path(__file__).resolve().parents[1]
RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
SNAPSHOT_DIR = ROOT_DIR / "data"
SNAPSHOT_PATH = SNAPSHOT_DIR / "portfolio-state-snapshots.json"

# 룰 임계값 — TS 분류 함수와 동일하게 맞춤
VAL_STRETCH_PER = 25.0
LONG_HOLD_RATIO_HEALTHY = 0.6
CYCLE_RATIO_EXPOSURE = 0.5
VAL_RATIO_STRETCHED = 0.4
CASH_READY_PERCENT = 60.0
CYCLE_TAG_THRESHOLD = 0.2
LONG_HOLD_TAG_THRESHOLD = 0.5


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8-sig") as fp:
        return json.load(fp)


def write_json(path: Path, data: Any) -> None:
    """UTF-8 (no BOM) + \\n 줄바꿈으로 일관 저장.

    Windows CRLF 자동 변환 방지 위해 newline="\\n" 명시.
    ensure_ascii=False로 한글 원문자 그대로 저장.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def as_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def as_num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def collect_metadata_by_code(history: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """wababaPicks + exploreGroups의 모든 종목을 code 기준 dict로 모음."""
    result: dict[str, dict[str, Any]] = {}

    picks = history.get("wababaPicks") or []
    if isinstance(picks, list):
        for item in picks:
            if not isinstance(item, dict):
                continue
            code = as_str(item.get("code")) or as_str(item.get("symbol"))
            if code and code not in result:
                result[code] = item

    explore_groups = history.get("exploreGroups") or {}
    if isinstance(explore_groups, dict):
        for value in explore_groups.values():
            if not isinstance(value, list):
                continue
            for item in value:
                if not isinstance(item, dict):
                    continue
                code = as_str(item.get("code")) or as_str(item.get("symbol"))
                if code and code not in result:
                    result[code] = item

    return result


def enrich_positions(
    positions: list[dict[str, Any]],
    meta_by_code: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """position(code/name/buyPrice/qty) + 메타를 합성한 holding 입력 리스트."""
    holdings: list[dict[str, Any]] = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        code = as_str(pos.get("code")) or as_str(pos.get("symbol"))
        meta = meta_by_code.get(code) if code else None
        merged: dict[str, Any] = dict(meta) if isinstance(meta, dict) else {}
        merged["code"] = code or merged.get("code")
        merged["name"] = (
            as_str(pos.get("name"))
            or as_str(merged.get("name"))
            or as_str(merged.get("corpName"))
        )
        holdings.append(merged)
    return holdings


def is_long_hold(h: dict[str, Any]) -> bool:
    long_view = as_str(h.get("longTermHoldView"))
    dur = as_str(h.get("growthDurabilityLabel"))
    cons = as_str(h.get("growthConsistencyLabel"))
    return (
        "장기보유" in long_view
        or "장기보유" in dur
        or "장기보유" in cons
    )


def is_cycle(h: dict[str, Any]) -> bool:
    return "회복 사이클" in as_str(h.get("sectorDurabilityLabel"))


def is_valuation_stretched(h: dict[str, Any]) -> bool:
    per = as_num(h.get("per")) or as_num(h.get("PER"))
    return per is not None and per > VAL_STRETCH_PER


def bucket_of(haystack: str) -> str:
    if any(k in haystack for k in ["방산", "조선", "해양", "국방", "무기", "항공", "중공업", "함정"]):
        return "방산·조선"
    if any(
        k in haystack
        for k in ["전력", "송배전", "변압기", "전선", "전력기기", "전력 인프라", "전력망"]
    ):
        return "전력 인프라"
    if any(
        k in haystack
        for k in ["반도체", "HBM", "메모리", "데이터센터", "파운드리", "AI 서버", "AI 인프라", "AI·반도체"]
    ):
        return "반도체·AI"
    if any(
        k in haystack
        for k in ["콘텐츠", "엔터", "엔터테인", "음반", "미디어", "K-POP", "케이팝", "IP 라이선싱"]
    ):
        return "콘텐츠·IP"
    return "기타 산업"


def classify_state(
    long_hold_ratio: float,
    cycle_ratio: float,
    val_ratio: float,
    total: int,
) -> tuple[str, str]:
    if total == 0:
        return ("OBSERVATION_BALANCED", "관찰")
    if val_ratio >= VAL_RATIO_STRETCHED:
        return ("VALUATION_STRETCHED", "관찰")
    if cycle_ratio >= CYCLE_RATIO_EXPOSURE:
        return ("CYCLE_EXPOSURE", "관찰")
    if long_hold_ratio >= LONG_HOLD_RATIO_HEALTHY:
        return ("HEALTHY_GROWTH", "건강")
    return ("OBSERVATION_BALANCED", "관찰")


def classify_action_mode(
    long_hold_ratio: float,
    cycle_ratio: float,
    cash_percent: float,
    total: int,
) -> str:
    if total == 0:
        return "OBSERVATION"
    if cycle_ratio >= CYCLE_RATIO_EXPOSURE:
        return "CYCLE_WATCH"
    if long_hold_ratio >= LONG_HOLD_RATIO_HEALTHY:
        return "GROWTH_FOCUS"
    if cash_percent >= CASH_READY_PERCENT:
        return "CASH_READY"
    return "BALANCED"


def build_top_tags(
    holdings: list[dict[str, Any]],
    cycle_ratio: float,
    long_hold_ratio: float,
) -> list[str]:
    bucket_count: dict[str, int] = {}
    for h in holdings:
        haystack = " ".join(
            [
                as_str(h.get("industryName")),
                as_str(h.get("industryTailwind")),
                as_str(h.get("name")),
            ]
        )
        b = bucket_of(haystack)
        bucket_count[b] = bucket_count.get(b, 0) + 1

    sorted_buckets = sorted(bucket_count.items(), key=lambda x: -x[1])
    tags = [name for name, _ in sorted_buckets if name != "기타 산업"][:3]

    if cycle_ratio >= CYCLE_TAG_THRESHOLD and "회복 사이클 일부" not in tags:
        tags.append("회복 사이클 일부")
    if long_hold_ratio >= LONG_HOLD_TAG_THRESHOLD:
        tags.append("장기보유 다수")

    return tags[:5]


def build_fund_snapshot(
    fund_key: str,
    portfolio: dict[str, Any],
    meta_by_code: dict[str, dict[str, Any]],
    base_date: str,
) -> dict[str, Any]:
    positions_raw = portfolio.get("positions") or []
    positions = [p for p in positions_raw if isinstance(p, dict)]
    holdings = enrich_positions(positions, meta_by_code)
    total = len(holdings)

    long_count = sum(1 for h in holdings if is_long_hold(h))
    cycle_count = sum(1 for h in holdings if is_cycle(h))
    val_count = sum(1 for h in holdings if is_valuation_stretched(h))

    long_ratio = (long_count / total) if total > 0 else 0.0
    cycle_ratio = (cycle_count / total) if total > 0 else 0.0
    val_ratio = (val_count / total) if total > 0 else 0.0

    cash = as_num(portfolio.get("cash")) or 0.0
    initial = as_num(portfolio.get("initialCapital")) or 0.0
    cash_percent = (cash / initial * 100.0) if initial > 0 else 0.0

    state_kind, health_level = classify_state(long_ratio, cycle_ratio, val_ratio, total)
    action_mode = classify_action_mode(long_ratio, cycle_ratio, cash_percent, total)
    top_tags = build_top_tags(holdings, cycle_ratio, long_ratio)

    return {
        "date": base_date,
        "fundKey": fund_key,
        "cashRatio": round(cash_percent, 1),
        "longHoldRatio": round(long_ratio * 100.0, 1),
        "cycleRatio": round(cycle_ratio * 100.0, 1),
        "valuationRatio": round(val_ratio * 100.0, 1),
        "healthLevel": health_level,
        "portfolioState": state_kind,
        "actionMode": action_mode,
        "topTags": top_tags,
        "holdingCount": total,
    }


def upsert_snapshot(
    snapshots: list[dict[str, Any]], new_snap: dict[str, Any]
) -> str:
    key = (new_snap.get("date"), new_snap.get("fundKey"))
    for i, s in enumerate(snapshots):
        if (s.get("date"), s.get("fundKey")) == key:
            snapshots[i] = new_snap
            return "updated"
    snapshots.append(new_snap)
    return "appended"


def main() -> None:
    print("")
    print("=" * 80)
    print("Phase 37-A11 — Portfolio Snapshot Persistence")
    print("=" * 80)

    history = read_json(RECOMMENDATION_HISTORY_PATH)
    if not isinstance(history, dict):
        raise RuntimeError(
            "recommendation-history.json이 dict 형태가 아닙니다 — snapshot 생성 중단."
        )

    base_date = as_str(history.get("baseDate")) or datetime.now().strftime("%Y-%m-%d")
    meta_by_code = collect_metadata_by_code(history)

    wababa_portfolio = history.get("portfolio") or {}
    ai_portfolio = history.get("aiPortfolio") or {}

    fund_snapshots: list[dict[str, Any]] = []
    if isinstance(wababa_portfolio, dict):
        fund_snapshots.append(
            build_fund_snapshot("wababa", wababa_portfolio, meta_by_code, base_date)
        )
    if isinstance(ai_portfolio, dict):
        fund_snapshots.append(
            build_fund_snapshot("ai", ai_portfolio, meta_by_code, base_date)
        )

    # 기존 snapshot 파일 로드
    existing = read_json(SNAPSHOT_PATH)
    if isinstance(existing, dict) and isinstance(existing.get("snapshots"), list):
        snapshots = list(existing["snapshots"])
    else:
        snapshots = []

    actions: list[str] = []
    for snap in fund_snapshots:
        action = upsert_snapshot(snapshots, snap)
        actions.append(f"{snap['fundKey']}:{action}")
        print(
            f"  [{snap['fundKey']:6}] date={snap['date']} state={snap['portfolioState']:22}"
            f" action={snap['actionMode']:13} health={snap['healthLevel']}"
            f" long={snap['longHoldRatio']}% cycle={snap['cycleRatio']}%"
            f" val={snap['valuationRatio']}% cash={snap['cashRatio']}% n={snap['holdingCount']}"
        )

    # 정렬: 날짜 → fundKey
    snapshots.sort(key=lambda s: (s.get("date", ""), s.get("fundKey", "")))

    payload = {
        "version": 1,
        "updatedAt": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "totalSnapshots": len(snapshots),
        "snapshots": snapshots,
    }
    write_json(SNAPSHOT_PATH, payload)

    print("")
    print(f"snapshot 파일: {SNAPSHOT_PATH}")
    print(f"누적 snapshot 수: {len(snapshots)}")
    print(f"이번 처리: {', '.join(actions)}")

    # UTF-8 readback verification — 한글이 파일에 정상 저장됐는지 즉시 확인.
    print("")
    print("[verify] UTF-8 readback")
    try:
        with SNAPSHOT_PATH.open("r", encoding="utf-8") as fp:
            verified = json.load(fp)
        for s in verified.get("snapshots", []):
            print(
                f"  {s.get('fundKey','?'):6} "
                f"health={s.get('healthLevel','?')} "
                f"state={s.get('portfolioState','?')} "
                f"action={s.get('actionMode','?')} "
                f"tags={s.get('topTags', [])}"
            )
    except Exception as verify_error:  # noqa: BLE001
        print(f"  [WARN] readback 실패: {verify_error}")

    print("=" * 80)


if __name__ == "__main__":
    main()
