# 마법공식 OFFICIAL — Claude Code 프로젝트 로컬 allowlist 가이드 (Phase 45-E19)

목적: 마법공식 OFFICIAL 운용에서 *반복되는 read-only/검증 명령*만 프로젝트 로컬 allowlist로 자동
허용해 권한 프롬프트를 줄인다. **장부 저장·public push·git push·배포·실제 주문은 절대 자동 허용하지
않는다**(approval gate 유지).

- 전역(`~/.claude`) 설정은 수정하지 않는다. **프로젝트 로컬 `.claude/settings.local.json`만** 대상.
- 이 가이드는 REPO2(`C:\work\kr-stock-agent-data-new`)의 고정 스크립트를 전제로 한다.
- 자동 허용 후보는 *고정 스크립트*만 넣는다. 매번 달라지는 인라인 `python -c`·shell for-loop·grep
  파이프는 넣지 않는다(애초에 고정 스크립트로 대체했다).

## 고정 검증 스크립트 (이번 Phase에서 신설; 전부 read-only)

| 스크립트 | 역할 | 쓰기 |
|---|---|---|
| `scripts/check_magic_preconditions.py` | HEAD/origin/staged/canonical seq·idx·batch·SHA·signal pkg SHA 점검 | 0 |
| `scripts/check_magic_dry_run_log.py` | dry-run 로그 RESULT_JSON/EXIT_CODE/pykrx_open/readOnlyUnchanged 검증 | 0 |
| `scripts/run_magic_regression_tests.py` | 핵심 회귀 4종(apply-v2/eval/missed/public) | 0 |
| `scripts/run_magic_full_test_suite.py` | 전체 마법공식 테스트 15종 | 0 |
| `scripts/magic_live_verify.py` | 운영 배포 JSON/`/performance` seq·자산·거래일 read-only 검증(deploy 0) | 0 |

표준 호출 형태(allowlist 패턴과 매칭되도록 REPO2 루트에서):
```
python scripts/check_magic_preconditions.py --expect-sequence 3 --expect-trading-index 5
python scripts/check_magic_dry_run_log.py --date 2026-06-23 --expect-sequence 3 --expect-buy-index 5 --expect-sell-index 55
python scripts/run_magic_regression_tests.py
python scripts/run_magic_full_test_suite.py
```

## [항상 허용 후보] read-only / validation only

```
Bash(python scripts/check_magic_preconditions.py*)
Bash(python scripts/check_magic_dry_run_log.py*)
Bash(python scripts/run_magic_regression_tests.py*)
Bash(python scripts/run_magic_full_test_suite.py*)
Bash(python scripts/magic_daily_signal.py*)
Bash(python scripts/magic_daily_dry_run.py*)
Bash(python scripts/magic_daily_status.py*)
Bash(python scripts/magic_live_verify.py*)
Bash(git status*)
Bash(git diff*)
Bash(git log*)
Bash(git rev-parse*)
Bash(git ls-remote origin main)
Bash(python -m py_compile*)
```
- `npx tsc --noEmit` / `npm run build`는 REPO1(webapp) 빌드 검증용. 단 build 후 `next-env.d.ts`가
  변경되면 반드시 `git checkout -- next-env.d.ts`로 원복(자동 허용하더라도 원복 절차는 사람/스크립트가 보장).
- **A티어 일일 자동화(45-AUTO2)** `magic_daily_signal/dry_run/status.py`는 TEMP signal·log·report만
  쓰고 canonical/public/REPO1 write 0이라 항상 허용 가능. Task Scheduler(평일 15:40/15:45/16:05,
  KRX 거래일 self-skip)로도 자동 실행. **단 이 3종에 receipt 생성·apply --confirm·public 반영 기능이
  섞이면 즉시 항상-허용에서 제외**(그 순간 B/C 티어). 장부 저장 기능과 절대 한 스크립트에 합치지 않는다.
- **B티어 approval-ticket(45-AUTO3)**: `magic_live_verify.py`는 운영 JSON/`/performance`를 *읽기만*(Vercel
  deploy 호출 0, write 0)이라 항상 허용 가능. `magic_make_approval_ticket.py`(ticket 생성·TEMP only)와
  `magic_apply_from_approval.py --mode build-receipt|dry-run-apply`(TEMP receipt·미저장 검증)는 사안별
  **한 번만 허용**. `magic_apply_from_approval.py --mode apply`(장부 write)와 `magic_publish_public.py --mode apply`
  (REPO1 public write)는 **항상-허용 절대 금지**(사람 승인 게이트). ticket은 승인 파일이 아니라 *요청 문서*다.

## [한 번만 허용] 사안별 1회 승인 (always 등록 금지)

```
python scripts/magic_make_approval_ticket.py ...                      # approval-ticket 생성(TEMP only)
python scripts/magic_apply_from_approval.py --ticket ... --mode build-receipt    # receipt 생성(TEMP, write 0)
python scripts/magic_apply_from_approval.py --ticket ... --mode dry-run-apply    # dry-run apply(canonical write 0)
python scripts/magic_publish_public.py --mode plan                    # public 반영 plan(write 0)
python scripts/apply_magic_official_day.py --build-receipt-v2 ...     # receipt 생성(TEMP, 네트워크 0)
python scripts/apply_magic_official_day.py --append --receipt ... --signal-package ...   # dry-run apply(미저장)
python (OneDrive 백업 스크립트)
git add <특정 코드/문서 파일만>
git commit
git push
public JSON 표적 반영(REPO1 recommendation-history magicOfficial 3키)
Vercel 운영 URL read-only 조회
```

## [매번 명시 승인 / 자동 허용 절대 금지]

```
python scripts/magic_apply_from_approval.py --ticket ... --mode apply --confirm APPLY_OFFICIAL_DAY_*  # 장부 저장(승인 ticket)
python scripts/magic_publish_public.py --mode apply ...                                       # REPO1 public 표적 write
python scripts/apply_magic_official_day.py --append --apply --confirm APPLY_OFFICIAL_DAY_*   # 장부 저장
python scripts/record_magic_missed_run.py --apply --confirm RECORD_MISSED_RUN_*              # MISSED_RUN 저장
canonical ledger write / snapshot write
recommendation-history.json 갱신 / REPO1 public 반영
Vercel deploy / promote
환경변수 변경 · 실제 증권 주문
reset --hard · git clean -fd · force push · git add .
파일 삭제 / 대량 덮어쓰기
```
이들은 REPO2 `.claude/settings.local.json`의 `deny`(예: `git push*`, `git add .*`, `daily_run`,
`.env` 읽기)로 이미 차단되거나, 본질적으로 사람 승인 게이트를 둔다.

## [지양] — 고정 스크립트로 대체

- 긴 인라인 `python -c` 검증 → 위 4종 고정 스크립트
- `for t in ...; do python $t; done` → `run_magic_full_test_suite.py`
- `... | grep "passed,"` 요약 → 스크립트의 요약 출력/`--json`
- 날짜별 dry-run runner를 매번 새로 작성 → (다음 단계 AUTO1에서 파라미터화)

## 프로젝트 로컬 반영 메모

- REPO2 `.claude/settings.local.json`은 `permissions.allow` / `permissions.deny`(json schemastore)
  스키마라 위 [항상 허용 후보] read-only 스크립트(검증 4종 + A티어 3종 + `magic_live_verify.py`) 패턴 추가가 안전하다.
- **approval-ticket/apply/publish는 `.claude/settings.local.json` always allow 에 절대 넣지 않는다.**
  `magic_make_approval_ticket.py`·`magic_apply_from_approval.py`·`magic_publish_public.py`는 [한 번만 허용] 또는
  [금지]로만 다룬다(스키마가 명확할 때만, 사람 승인 하에 로컬 수정. 전역 설정 무수정).
- 단 `.claude/`는 `.gitignore` 대상(이미 tracked였더라도) → allowlist 변경은 **로컬 전용**(commit 안 함).
- Claude Code 권한은 *세션 cwd의 프로젝트* + 전역을 병합한다. 마법공식 작업을 REPO2 루트에서 수행할 때
  REPO2 allow가 적용된다. REPO1 루트 세션에서 REPO2 스크립트를 돌리면 REPO1 settings가 적용되므로,
  필요 시 동일 4종 패턴을 REPO1 프로젝트 로컬에도 사람 승인하에 추가한다(전역 금지).
