from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"

RECOMMENDATION_HISTORY_PATH = ROOT_DIR / "recommendation-history.json"
DAILY_RUN_SUMMARY_PATH = ROOT_DIR / "daily-run-summary.json"


STEPS = [
    {"name": "1. 시장 재무 데이터 생성", "file": "build_market_snapshot_fast.py"},
    {"name": "2. 뉴스 모멘텀 생성", "file": "build_news_momentum_sample.py"},
    {"name": "3. 추천 이력 생성", "file": "build_recommendation_history.py"},
    {"name": "4. 일일 투자 판단 리포트 생성", "file": "build_daily_report.py"},
]


def read_json(path: Path) -> Any:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8-sig") as file:
        return json.load(file)


def write_json(path: Path, data: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=True, indent=2)


def get_latest_history_record(data: Any) -> dict[str, Any]:
    if isinstance(data, list):
        records = [item for item in data if isinstance(item, dict)]
        return records[-1] if records else {}

    if not isinstance(data, dict):
        return {}

    for key in ["latest", "current", "today", "summary", "meta"]:
        value = data.get(key)
        if isinstance(value, dict):
            return value

    for key in ["history", "runs", "items", "snapshots", "records"]:
        value = data.get(key)
        if isinstance(value, list):
            records = [item for item in value if isinstance(item, dict)]
            if records:
                return records[-1]

    return data


def pick_value(data: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in data:
            return data.get(key)

    return None


def count_value(data: dict[str, Any], keys: list[str]) -> int:
    value = pick_value(data, keys)

    if isinstance(value, list):
        return len(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0

    return 0


def build_summary(
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    raw_history = read_json(RECOMMENDATION_HISTORY_PATH)
    history = get_latest_history_record(raw_history)

    base_date = pick_value(
        history,
        ["baseDate", "date", "runDate", "snapshotDate"],
    )

    # 🔥 여기 변경 (새 구조 반영)
    buy_count = count_value(history, ["buyCandidates"])
    hold_count = count_value(history, ["holdCandidates"])
    sell_count = count_value(history, ["sellCandidates"])

    total_recommendation = buy_count + hold_count + sell_count

    if status == "success" and total_recommendation == 0:
        status_code = "NO_SIGNAL"
        status_label = "신호없음"
    elif status == "success":
        status_code = "SUCCESS"
        status_label = "정상"
    else:
        status_code = "FAILED"
        status_label = "실패"

    return {
        "ok": status == "success",
        "statusCode": status_code,
        "statusLabel": status_label,
        "baseDate": base_date,
        "startedAt": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "finishedAt": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "elapsedSeconds": round((finished_at - started_at).total_seconds(), 2),
        "buyCount": buy_count,
        "holdCount": hold_count,
        "sellCount": sell_count,
        "totalCount": total_recommendation,
        "errorMessage": error_message,
        "sourceFile": str(RECOMMENDATION_HISTORY_PATH),
    }


def run_step(step_name: str, script_file: str) -> None:
    script_path = SCRIPTS_DIR / script_file

    if not script_path.exists():
        raise FileNotFoundError(f"스크립트 파일이 없습니다: {script_path}")

    print("")
    print("=" * 80)
    print(step_name)
    print(f"실행 파일: {script_path}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        raise RuntimeError(f"{step_name} 실패: {script_file}")


def main() -> None:
    started_at = datetime.now()

    print("")
    print("국내주식 가치성장 투자 운영 에이전트")
    print("Daily Run 시작")
    print(f"시작 시간: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"작업 폴더: {ROOT_DIR}")

    try:
        for step in STEPS:
            run_step(step["name"], step["file"])

        finished_at = datetime.now()

        summary = build_summary(
            started_at=started_at,
            finished_at=finished_at,
            status="success",
        )

        write_json(DAILY_RUN_SUMMARY_PATH, summary)

        print("")
        print("=" * 80)
        print("Daily Run 완료")
        print(f"상태코드: {summary['statusCode']}")
        print(f"상태: {summary['statusLabel']}")
        print(f"기준일: {summary['baseDate']}")
        print(f"BUY: {summary['buyCount']}")
        print(f"HOLD: {summary['holdCount']}")
        print(f"SELL: {summary['sellCount']}")
        print(f"요약 파일: {DAILY_RUN_SUMMARY_PATH}")
        print(f"종료 시간: {finished_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"소요 시간: {finished_at - started_at}")
        print("=" * 80)

    except Exception as error:
        finished_at = datetime.now()

        summary = build_summary(
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            error_message=str(error),
        )

        write_json(DAILY_RUN_SUMMARY_PATH, summary)

        print("")
        print("=" * 80)
        print("Daily Run 실패")
        print(f"오류: {error}")
        print(f"요약 파일: {DAILY_RUN_SUMMARY_PATH}")
        print("=" * 80)

        raise


if __name__ == "__main__":
    main()