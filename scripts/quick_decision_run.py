import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"


STEPS = [
    {"name": "1. 와바바 추천 판단 재계산", "file": "build_recommendation_history.py"},
    {"name": "2. 일일 투자 판단 리포트 재생성", "file": "build_daily_report.py"},
]


def run_step(step_name, script_file):
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


def main():
    started_at = datetime.now()

    print("")
    print("와바바 빠른 판단 실행")
    print(f"시작 시간: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"작업 폴더: {ROOT_DIR}")

    for step in STEPS:
        run_step(step["name"], step["file"])

    finished_at = datetime.now()

    print("")
    print("=" * 80)
    print("와바바 빠른 판단 완료")
    print(f"종료 시간: {finished_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"소요 시간: {finished_at - started_at}")
    print("=" * 80)


if __name__ == "__main__":
    main()