#!/usr/bin/env python3
"""R10 Phase A/B — R8 frozen 사양의 forensic 확정.

WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10

질문은 하나다.
  R8 결과를 보기 **전에** 확정돼 있던 규칙은 A 인가 B 인가?
    A MONTHLY_STAGGER    12개월에 걸쳐 자본이 실제로 시장에 순차 투입된다
    B ONE_TWELFTH_WAIT   초기 1/12만 투입하고 장기간 현금을 유지한다

판정은 Founder 취향이나 R9 결과가 아니라 **결과 생성 이전에 존재한 증거**로만 한다.
증거 우선순위(지시문 §2): precommit/matrix spec > 실행 전 JSON > 당시 task/보고서 >
당시 코드 의도 주석 > R7→R8 전환기록 > 결과 이후 설명문 > 사람의 현재 해석.

파일 timestamp 하나만 믿지 않는다. git commit provenance + 내용증거를 함께 쓴다.

안전: 읽기 전용 조사. 네트워크 0 · 파일 write 는 reports/research/r10-* 만 ·
      canonical 미접근 · 실주문 0 · 브로커 0.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RD = ROOT / "reports" / "research"

R6_COMMIT = "6e21b19"      # 자본투입·매수주기 연구 (STAGGERED 개념 정의)
R7_COMMIT = "cdcdf89"      # 팩터 신호 탐색
R8_COMMIT = "0a4eba6"      # frozen candidate 가 나온 커밋
R9_COMMIT = "0a19d0a"      # adversarial validation

MONTHLY = "MONTHLY_STAGGER"
WAIT = "ONE_TWELFTH_WAIT"
AMB = "ambiguous"


def git(*args):
    try:
        return subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True,
                              text=True, encoding="utf-8", errors="replace",
                              check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def commit_time(sha):
    return git("show", "-s", "--format=%cI", sha).strip()


# ══════════════════════ 증거표 (§3) ══════════════════════
def evidence_rows():
    t6, t7, t8, t9 = (commit_time(c) for c in (R6_COMMIT, R7_COMMIT, R8_COMMIT, R9_COMMIT))
    spec_mtime = None
    res_mtime = None
    p1, p2 = RD / "r8-matrix-spec-latest.json", RD / "r8-matrix-latest.json"
    if p1.exists():
        spec_mtime = p1.stat().st_mtime
    if p2.exists():
        res_mtime = p2.stat().st_mtime

    return [
        {
            "id": "E1",
            "artifact": "reports/wababa/wababa-investment-rule-research-r6-latest.md "
                        "— 자본 투입방식 표",
            "gitCommit": R6_COMMIT,
            "commitTime": t6,
            "phase": "pre-result",
            "exactEvidence": ("STAGGERED_12 행의 실측 '평균 현금비중 = 3.63%'. "
                              "같은 표의 STAGGERED_3 3.08% · STAGGERED_6 2.53% · "
                              "STAGGERED_25 3.87% · STAGGERED_50 3.97%."),
            "supports": MONTHLY,
            "weight": "DECISIVE",
            "why": ("R8 보다 앞선 커밋에서 'STAGGERED_K 분할투입'이 **실측 현금비중 3~4%** "
                    "짜리 구조로 정의·측정됐다. 즉 이 프로젝트에서 '12개월 분할 투입'은 "
                    "자본이 그 12개월 안에 실제로 시장에 들어가고 남는 현금은 정수주 "
                    "반올림 잔여뿐이라는 뜻이다. frozen candidate 의 평균 16.9% · "
                    "첫 24개월 81% 는 이 정의와 양립할 수 없다."),
        },
        {
            "id": "E2",
            "artifact": "scripts/research/r8_portfolio.py — 모듈 docstring "
                        "'가장 중요한 공정성 장치 (§10)' + run_benchmark_same_schedule docstring",
            "gitCommit": R8_COMMIT,
            "commitTime": t8,
            "phase": "pre-result (설계 의도 · 결과 무관)",
            "exactEvidence": ("'benchmark 에도 같은 현금 투입 스케줄을 적용한다. 전략만 "
                              "12개월 분할하고 benchmark 는 1일차 일시투입으로 비교하면 "
                              "\"분할투입 효과\"가 \"BM alpha\"로 둔갑한다. 그걸 구조적으로 "
                              "막는다.' / '분할투입 효과를 benchmark 도 똑같이 누리게 해서 "
                              "BM alpha 만 남긴다.' "
                              "그리고 구현: benchmark 는 매월 도착한 tranche 를 그 달 지수로 "
                              "즉시 매수한다(units += inflow / idx[i])."),
            "supports": MONTHLY,
            "weight": "DECISIVE",
            "why": ("이 문장이 '투입 스케줄'의 의미를 스스로 정의한다 — 그 달에 시장으로 "
                    "들어가는 것이다. benchmark 는 정확히 그렇게 구현됐다. 전략이 대신 "
                    "현금을 들고 있으면 두 쪽이 '똑같이 누리게' 되지 않으므로 이 장치가 "
                    "막으려 한 바로 그 오염(분할/현금 효과가 BM alpha 로 둔갑)이 발생한다. "
                    "실제로 R9 가 측정한 왜곡이 그것이다."),
        },
        {
            "id": "E3",
            "artifact": "scripts/research/deployment_engine.py — deployment / replacement 정의 주석",
            "gitCommit": R6_COMMIT,
            "commitTime": t6,
            "phase": "pre-result",
            "exactEvidence": ("'STAGGERED_<K>  K개월에 걸쳐 균등 분할 투입' / "
                              "replacement 축에 'FIXED_MATURITY  만기 매도 → 현금으로 두고 "
                              "**다음 매수주기에 재투입**'."),
            "supports": MONTHLY,
            "weight": "STRONG",
            "why": ("현금 보유는 **만기대금이 다음 매수주기까지 다리를 놓는 경우**에만 "
                    "명명된 개념으로 존재했고, 그것도 replacement 축이다. 초기자본을 "
                    "장기간 현금으로 두는 개념은 deployment 축 어디에도 정의된 바 없다. "
                    "frozen candidate 는 FIXED_MATURITY_REPLACE 를 쓰지만 매수주기와 만기가 "
                    "일치하므로 이 다리 현금은 0 이다 — 81% 현금의 출처가 아니다."),
        },
        {
            "id": "E4",
            "artifact": "scripts/research/run_r8_portfolio.py — SPEC['cohortSweep'] 직상단 ★ 주석",
            "gitCommit": R8_COMMIT,
            "commitTime": t8,
            "phase": "pre-result (사전규격 본문)",
            "exactEvidence": ("'매수주기를 보유기간과 같게 하면 한 코호트만 굴린다 = 실제 "
                              "보유종목수 = N. buy_every=1(매월 매수)이면 N×hold 개 lot 이 "
                              "겹쳐 쌓여 사실상 수백 종목이 된다(실측: N20/H12 가 평균 "
                              "129종목·연 419거래·비용 47%). 사람이 실행할 수 없다.'"),
            "supports": MONTHLY,
            "weight": "STRONG",
            "why": ("buy_every=hold 를 넣은 **명시된 목적은 동시 보유종목수 통제(실행 "
                    "가능성)**다. 진입 타이밍이나 현금 대기는 목적으로 언급되지 않는다. "
                    "즉 buy_every 는 '재선정 주기' 축이고 deployment 는 '자본 투입' 축이다. "
                    "두 축이 곱해져 자본의 11/12 가 2년간 잠기는 것은 의도된 규칙이 아니라 "
                    "두 축의 상호작용 부작용이다."),
        },
        {
            "id": "E5",
            "artifact": "reports/research/r8-matrix-spec-latest.json (실행 전 저장)",
            "gitCommit": "(gitignored — 파일시스템 증거)",
            "commitTime": None,
            "phase": "pre-result (파일 순서로 입증)",
            "exactEvidence": (f"spec 파일 mtime {spec_mtime} < 결과 파일 "
                              f"r8-matrix-latest.json mtime {res_mtime}. "
                              "spec.note = '결과를 보고 조합을 추가하지 않는다. 이 규격이 "
                              "실행 전에 고정됐다.' cohortSweep 의 deployment 축 = ['STAG12M']."),
            "supports": AMB,
            "weight": "MODERATE",
            "why": ("사전규격이 실제로 결과 이전에 고정됐음을 입증한다(순서 증거). 다만 "
                    "'STAG12M' 라벨 자체는 A/B 를 구분하지 않는다 — 라벨의 의미는 E1·E2·E3 "
                    "가 정의한다. 그래서 이 항목은 '사전성'의 증거이고 '해석'의 증거는 아니다."),
        },
        {
            "id": "E6",
            "artifact": "docs/WABABA-LONG-HORIZON-MASTER-PLAN-R1.md — 투입방식 어휘",
            "gitCommit": "(R6 이전 문서)",
            "commitTime": None,
            "phase": "pre-result",
            "exactEvidence": ("'투입방식   LUMP_SUM · MONTHLY_DCA · QUARTERLY_DCA · "
                              "INITIAL_PLUS_MONTHLY' / 존재 이유에 "
                              "'종목선택·감정매매·**타이밍 스트레스 제거**'."),
            "supports": MONTHLY,
            "weight": "MODERATE",
            "why": ("이 프로젝트의 '투입방식' 어휘가 전부 DCA 계열이다. DCA 는 정의상 각 "
                    "회차가 시장에 투자된다. 또 프로젝트 존재 이유에 '타이밍 제거'가 적혀 "
                    "있어, 2년 현금대기라는 **의도적 시장 타이밍 오버레이**는 교리와 충돌한다."),
        },
        {
            "id": "E7",
            "artifact": "scripts/research/r8_build_report.py — 후보 설명 블록 문자열",
            "gitCommit": R8_COMMIT,
            "commitTime": t8,
            "phase": "post-result 서술이지만 규칙 서술(결과값 아님)",
            "exactEvidence": "'첫 투자         12개월에 걸쳐 매월 약 4,166,000원씩 분할 매수'",
            "supports": MONTHLY,
            "weight": "MODERATE",
            "why": ("R8 저자가 규칙을 '매월 분할 **매수**'로 서술했다(입금이 아니라 매수). "
                    "또 '첫 투자'라고 한정해 12분할이 **초기자본에만** 적용되고 이후 "
                    "회전은 만기대금 재투자라는 구조를 확인해준다. 결과 이후 문장이므로 "
                    "가중치는 낮게 두지만 E1·E2 와 방향이 같다."),
        },
        {
            "id": "E8",
            "artifact": "R8/R6 전 구간 — ONE_TWELFTH_WAIT 를 규정하는 사전 증거 탐색 결과",
            "gitCommit": f"{R6_COMMIT}..{R8_COMMIT}",
            "commitTime": t8,
            "phase": "pre-result (부재 증거)",
            "exactEvidence": ("검색어 현금·대기·유휴·idle·타이밍·timing 으로 R6/R7/R8 "
                              "코드·보고서를 전수 검색. 초기자본을 의도적으로 장기 현금 "
                              "보유한다는 규정은 **0건**. 발견된 현금 개념은 두 가지뿐 — "
                              "(1) '정수주 매수 … 잔여현금 보유'(반올림 잔여) "
                              "(2) FIXED_MATURITY 의 다음 매수주기까지 다리 현금."),
            "supports": MONTHLY,
            "weight": "STRONG",
            "why": ("B 를 지지하는 사전 증거가 존재하지 않는다. 81% 현금은 사전에 "
                    "규정된 규칙이 아니라 구현 부작용이다."),
        },
    ]


# ══════════════════════ Phase B 판정 (§4) ══════════════════════
def verdict(rows):
    w = {"DECISIVE": 8, "STRONG": 4, "MODERATE": 2, "WEAK": 1}
    score = {MONTHLY: 0, WAIT: 0, AMB: 0}
    for r in rows:
        score[r["supports"]] = score.get(r["supports"], 0) + w.get(r["weight"], 0)
    decisive_m = [r["id"] for r in rows
                  if r["supports"] == MONTHLY and r["weight"] == "DECISIVE"]
    decisive_w = [r["id"] for r in rows
                  if r["supports"] == WAIT and r["weight"] == "DECISIVE"]

    if decisive_m and not decisive_w and score[MONTHLY] > 3 * max(1, score[WAIT]):
        case = "CASE_1"
        label = "SPEC_MONTHLY_STAGGER_CONFIRMED"
        conclusion = "SPEC_CONFORMANCE_BUG"
    elif decisive_w and not decisive_m:
        case = "CASE_2"
        label = "SPEC_ONE_TWELFTH_WAIT_CONFIRMED"
        conclusion = "CODE_CORRECT_RECLASSIFY_ECONOMICS"
    else:
        case = "CASE_3"
        label = "SPEC_AMBIGUOUS"
        conclusion = "BLOCKED_SPEC_AMBIGUOUS"

    return {
        "case": case,
        "specVerdict": label,
        "conclusion": conclusion,
        "evidenceScore": score,
        "decisiveFor": {MONTHLY: decisive_m, WAIT: decisive_w},
        "counterEvidenceForWait": ("사전 증거 0건 — 초기자본 장기 현금보유를 규정한 "
                                   "artifact 가 R6~R8 어디에도 없다(E8)."),
        "reasoning": (
            "결과를 보기 전 증거만으로 판정했다. (1) R8 보다 앞선 R6 커밋이 "
            "'STAGGERED_12 = 평균 현금비중 3.63%' 로 분할투입을 실측 정의했다(E1). "
            "(2) R8 자신의 '가장 중요한 공정성 장치'가 투입 스케줄의 의미를 "
            "'그 달 시장 진입'으로 정의하고, benchmark 를 정확히 그렇게 구현했으며, "
            "그 목적이 '분할투입 효과가 BM alpha 로 둔갑하는 것을 막는 것'이라고 "
            "명시했다(E2). (3) 현금 보유는 replacement 축의 만기대금 다리로만 정의됐고 "
            "deployment 축에는 없다(E3). (4) buy_every=hold 의 명시 목적은 동시 "
            "보유종목수 통제이지 진입 타이밍이 아니다(E4). (5) B 를 지지하는 사전 "
            "증거는 0건이다(E8). "
            "따라서 사전확정 규칙은 MONTHLY_STAGGER 이고, 자본의 11/12 를 2년간 "
            "현금으로 두는 현재 동작은 사전규격 위반 = 구현 오류다. "
            "이것은 parameter 변경이 아니다 — BM·P20·N40·H24·universe·benchmark·"
            "비용·상폐 가정은 전부 그대로 두고 투입 스케줄만 사전규격과 일치시킨다."),
        "notAParameterChange": {
            "unchanged": ["BM = 1/PBR", "percentile 0.20", "holdings 40", "hold 24",
                          "PIT universe", "ranking logic", "investability filters",
                          "benchmark definition", "transaction cost", "delisting",
                          "data source", "rebalance semantics", "gate thresholds"],
            "changedOnly": "초기자본 12개월 분할이 실제로 시장에 투입되도록 하는 최소 구현",
        },
    }


# ══════════════════════ 사양-준수 구현 해석 (§5) ══════════════════════
def conformance_reading():
    """사전규격 두 제약을 **동시에** 만족하는 구현 해석을 명시한다.

    제약 1 (deployment, E1·E2): 초기자본 5천만원이 12개월에 걸쳐 실제로 시장에 들어간다.
    제약 2 (cohort, E4):        동시 보유종목수 = N = 40 (N×hold lot 적층 금지).

    두 제약을 동시에 만족하는 유일한 구조는 **하나의 40종목 코호트를 12개월에 걸쳐
    쌓아 올리는 것**이다.
      - 선정월(i % 24 == 0): BM 상위 20% 풀에서 40종목 선정 → 그 달 도착한 tranche 투입
      - 이후 tranche 월(i = 1..11): **재선정 없이 같은 40종목에 1/12 추가 매수**
      - 만기(선정월 + 24): 전량 매도 → 재선정 → 매도대금 전액 재투자
    이렇게 하면 동시 보유는 40종목으로 유지되고, 초기자본은 12개월 안에 시장에 들어간다.

    '만기 후 전량 교체'가 한 시점의 전량 매도를 요구하므로 만기는 **코호트 단위**다
    (tranche 별 개별 만기로 두면 만기가 어긋나 전량 교체가 성립하지 않는다).
    12분할은 R8 서술대로 '첫 투자'(초기자본)에만 적용되고, 이후 회전은 만기대금
    전액 재투자다(E7).

    민감도로 tranche 별 개별 만기(PER_TRANCHE_MATURITY) 도 함께 계산해 이 해석이
    결과를 좌우하지 않음을 보인다(§9 투명성).
    """
    return {
        "primary": "COHORT_BUILT_OVER_12M",
        "rule": [
            "선정월(i % 24 == 0): BM 상위 20% 풀에서 40종목 선정, 그 달 tranche 투입",
            "tranche 월 i=1..11: 재선정 없이 같은 40종목에 1/12 추가 매수(top-up)",
            "만기 = 선정월 + 24개월: 전량 매도 → 재선정 → 매도대금 전액 재투자",
            "동시 보유종목수 = 40 유지",
            "12분할은 초기자본에만 적용(첫 투자). 이후 회전은 만기대금 전액 재투자",
        ],
        "maturityUnit": "COHORT (전량 교체가 한 시점에 성립해야 하므로)",
        "sensitivityVariant": "PER_TRANCHE_MATURITY — tranche 별 개별 24개월 만기",
        "forbidden": ["결과를 좋게 만들기 위한 cash drag 조정",
                      "새 parameter 탐색", "gate threshold 변경",
                      "benchmark 정의 변경"],
    }


def main() -> int:
    rows = evidence_rows()
    v = verdict(rows)
    out = {
        "schema": "wababa-r8-frozen-spec-forensic-reconciliation-r10/forensics@1",
        "taskId": "WABABA-R8-FROZEN-SPEC-FORENSIC-RECONCILIATION-R10",
        "question": ("R8 결과를 보기 전에 확정돼 있던 자본 투입 규칙은 "
                     "MONTHLY_STAGGER 인가 ONE_TWELFTH_WAIT 인가"),
        "evidencePriority": ["precommit/matrix spec", "실행 전 JSON", "당시 task/보고서",
                             "당시 코드 의도 주석", "R7→R8 전환기록",
                             "결과 이후 설명문", "현재 사람의 해석"],
        "commits": {"R6": {"sha": R6_COMMIT, "time": commit_time(R6_COMMIT)},
                    "R7": {"sha": R7_COMMIT, "time": commit_time(R7_COMMIT)},
                    "R8": {"sha": R8_COMMIT, "time": commit_time(R8_COMMIT)},
                    "R9": {"sha": R9_COMMIT, "time": commit_time(R9_COMMIT)}},
        "evidenceTable": rows,
        "phaseBVerdict": v,
        "conformanceReading": conformance_reading(),
    }
    RD.mkdir(parents=True, exist_ok=True)
    (RD / "r10-forensics-latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"case": v["case"], "specVerdict": v["specVerdict"],
                      "conclusion": v["conclusion"], "score": v["evidenceScore"],
                      "evidenceRows": len(rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
