# 마법공식 OFFICIAL 일일 운용 자동화 설계 (Phase 45-AUTO1)

E13~E19에서 동일 패턴(신호→dry-run→receipt→apply→public→live)이 3회 이상 반복됐다(batch1 06-17,
batch2 06-19, batch3 06-23, missed 06-18/06-22). 이 문서는 **안전한 자동화 구조를 확정**한다.
이번 Phase는 설계만 — 실제 자동 저장 파이프라인은 켜지 않는다(AUTO2에서 구현).

## 0. 핵심 원칙

- **read-only / TEMP-only → 자동 실행 가능.** 장부 write·public push·git push·Vercel deploy·실제 주문
  → **approval-ticket(사람 승인) 게이트.** 자동 허용 절대 금지.
- 자동 실행은 *고정 스크립트*만. 인라인 `python -c`·shell for-loop·grep 파이프 금지(E19에서 4종으로 대체).
- look-ahead 규율 유지: signal은 반드시 *장전 윈도우*(전일 15:30~당일 09:00)에서 생성. gen ≥ open이면 BLOCKED.
- BLOCKED 시 자동 중단. 가짜 거래·종가/현재가 fallback·11위 이하 대체·수량 임의변경 금지.
- 전역(`~/.claude`) 설정 무수정. 프로젝트 로컬만. canonical은 untracked(commit 금지).

## 1. 반복 작업 자동화 분류표

티어: **A**=자동(read-only/TEMP) · **B**=approval-ticket 후 실행 · **C**=자동 금지(사람만)

| 작업 | 현재 수동 | 티어 | 실패 중단 조건 | 스크립트 후보 | 스케줄러 | 우선순위 |
|---|---|---|---|---|---|---|
| 종가 신호 패키지 생성 | build_magic_signal_package --signal-date | A | universeBaseDate≠signalDate, top10≠10, SHA불일치 | magic_daily_signal.py | Daily Signal 15:40 | P1 |
| nextExecutionDate 검증 | next_krx_trading_day | A | 거래일 아님/calendar 공백 | (signal에 포함) | — | P1 |
| read-only dry-run | 날짜별 runner 새로 작성 | A | NETWORK_TIMEOUT, MISSING_OPEN, LOOKAHEAD | magic_daily_dry_run.py | Daily Dry Run 15:40 | P1 |
| dry-run 로그 검증 | check_magic_dry_run_log.py | A | EXIT≠0, runStatus≠COMPLETED, readOnly≠true | check_magic_dry_run_log.py(기존) | (status에 포함) | P1 |
| precondition 점검 | check_magic_preconditions.py | A | HEAD/seq/idx/SHA 불일치 | check_magic_preconditions.py(기존) | — | P1 |
| regression/full test | run_magic_*_test*.py | A | 1개라도 fail | run_magic_full_test_suite.py(기존) | — | P2 |
| daily status report | (수동 종합) | A | — | magic_daily_status.py | Daily Status 16:00 | P1 |
| execution receipt 생성 | apply --build-receipt-v2 | B | receipt≠dry-run 총계 | magic_make_approval_ticket.py | — | P1 |
| dry-run apply(미저장) | apply --append (no --apply) | B | DRY_RUN_OK 아님 | (ticket 생성 시 동반) | — | P2 |
| **장부 apply --confirm** | apply --append --apply --confirm | **C→B** | RECEIPT/CANONICAL/PLAN mismatch | magic_apply_from_approval.py | — | P1 |
| snapshot 생성 | (apply에 포함) | C→B | post-write SHA 불일치 | (apply에 포함) | — | P1 |
| idempotency 검증 | apply 2회차 | B | ALREADY_PROCESSED 아님 | (apply 후 자동) | — | P2 |
| OneDrive 백업 | _backup_eNN.py | B | source≠backup SHA, 기존백업 변형 | magic_backup.py(파라미터화) | — | P2 |
| public model 생성 | build_magic_official_public | B | validate_canonical 실패 | magic_publish_public.py | — | P1 |
| **REPO1 public 반영** | _publish_eNN.py(표적 갱신) | **C→B** | untouchedDrift≠0, topKey 변동 | magic_publish_public.py | — | P1 |
| REPO1 tsc/build | npx tsc / npm run build | B | EXIT≠0 (next-env 원복) | (publish 검증에 포함) | — | P2 |
| **REPO1/REPO2 commit** | git add 특정파일 + commit | **C** | — | (사람 승인) | — | P1 |
| **git push** | git push origin | **C** | harness 권한 간헐 거부 | (사람 승인) | — | P1 |
| Vercel live JSON 확인 | python urllib | A | HTTP≠200, seq 불일치 | magic_live_verify.py | (status에 포함) | P1 |
| /performance 렌더 확인 | python urllib + 마커 | A | 실에러 마커, 값 누락 | magic_live_verify.py | — | P2 |
| MISSED_RUN 기록 | record_magic_missed_run --confirm | C→B | INVARIANT_VIOLATION | magic_apply_from_approval.py(type=missed) | — | P2 |
| BLOCKED 원인 분류 | (수동 판독) | A | — | magic_daily_status.py | — | P2 |
| 수동 push 안내 | (보고) | A | — | magic_daily_status.py | — | P3 |

(C→B = 본질은 사람 승인이지만 approval-ticket을 받으면 *고정 스크립트가 1회 실행*; 티켓 없이는 절대 실행 안 함)

## 2. AUTO1 3단계 구조

**A. 자동 실행 가능 (read-only 또는 TEMP only · 스케줄러/무인 OK)**
- 종가 신호 패키지 생성(TEMP) · 다음 거래일 계산 · read-only dry-run(저장 0) · dry-run 로그 검증 ·
  precondition 점검 · regression/full test · live JSON/performance 확인 · daily status/BLOCKED report 생성

**B. approval-ticket 필요 (티켓 승인 후 고정 스크립트 1회 실행)**
- execution receipt 생성 · dry-run apply(미저장) · **장부 apply --confirm** · snapshot/backup ·
  public model 생성 · **REPO1 public 반영** · idempotency 재실행 · MISSED_RUN 기록

**C. 자동 금지 (사람만 · 자동화·allowlist always 금지)**
- 실제 증권 주문 · 환경변수 변경 · Vercel deploy/promote · force push · reset --hard · git clean -fd ·
  git add . · git commit · git push · 가짜 거래 생성 · 종가/현재가 fallback · 파일 대량 삭제/덮어쓰기

## 3. approval-ticket JSON 구조

`C:\Users\duria\AppData\Local\Temp\wababa-magic-approval\<date>\approval-ticket.json` (저장소 밖 TEMP)

```json
{
  "ticketId": "MF-APPROVAL-<date>-<actionType>",
  "date": "YYYY-MM-DD",
  "actionType": "APPLY_BATCH | RECORD_MISSED_RUN | PUBLISH_PUBLIC",
  "status": "PENDING_APPROVAL",
  "signalPackageSha": "...", "dryRunLogSha": "...", "receiptSha": "...",
  "canonicalBeforeSha": "...",
  "expectedSequence": 4, "expectedTradingDayIndex": 6,
  "buyCount": 10, "sellCount": 0, "totalInvested": 0, "cashReserve": 0,
  "officialAvailableCashBefore": 0, "officialAvailableCashAfter": 0,
  "holdingsMarketValuePreview": 0, "missingEvalCodes": [],
  "commandsToRun": ["python scripts/apply_magic_official_day.py --append --apply --confirm APPLY_OFFICIAL_DAY_<date> --receipt <...> --signal-package <...>"],
  "risks": ["canonical write(되돌리기=백업/idempotent)", "..."],
  "blockedConditions": ["RECEIPT_MISMATCH", "CANONICAL_CHANGED", "PLAN_MISMATCH", "LOOKAHEAD", "MISSING_OPEN"],
  "approval": {"approvedBy": null, "approvedAt": null, "confirmToken": null},
  "createdAt": "..."
}
```
- ticket은 **승인 요청 문서**이지 자동 승인 파일이 아니다. `status=PENDING_APPROVAL`이 기본.
- 대장이 승인(approvedBy/confirmToken 입력) 전에는 `magic_apply_from_approval.py`가 **apply --confirm 실행 거부**.
- push/public/deploy는 ticket이 있어도 사람이 직접 실행(자동 승인 금지).

## 4. daily status report 구조

`C:\Users\duria\AppData\Local\Temp\wababa-magic-status\<date>\status.json` (+ 콘솔 요약)
```json
{
  "date": "...", "overall": "PASS | BLOCKED | PENDING_APPROVAL",
  "signal": {"status": "READY|BLOCKED", "signalAsOfDate": "...", "nextExec": "...", "sha": "..."},
  "dryRun": {"status": "COMPLETED|BLOCKED|NOT_RUN", "seq": 0, "batchId": "...", "buyCount": 0, "readOnlyUnchanged": true},
  "canonical": {"seq": 0, "idx": 0, "batches": 0, "sha": "..."},
  "live": {"deployedSeq": 0, "match": true, "http": 200},
  "pendingTickets": ["..."],
  "nextManualAction": "...", "createdAt": "..."
}
```

## 5. BLOCKED report 구조
```json
{"date": "...", "stage": "SIGNAL|DRY_RUN|RECEIPT|APPLY|PUBLISH|LIVE",
 "blockedCode": "LOOKAHEAD|MISSING_OPEN_PRICE|NETWORK_TIMEOUT|RECEIPT_MISMATCH|CANONICAL_CHANGED|...",
 "reason": "...", "evidencePaths": ["log/receipt/ticket"],
 "autoStopped": true, "noFakeTrade": true, "recommendedManualFix": "...", "createdAt": "..."}
```

## 6. Windows Task Scheduler 작업 목록 (AUTO2에서 등록 — 지금은 등록 안 함)

| 작업명 | 시각 | 실행 | 티어 |
|---|---|---|---|
| Wababa Magic Daily Signal | 매 거래일 15:40 | magic_daily_signal.py (TEMP signal only) | A |
| Wababa Magic Daily Dry Run | 다음 거래일 장마감 후(예: 15:40) | magic_daily_dry_run.py (read-only) | A |
| Wababa Magic Daily Status | 매일 16:00 | magic_daily_status.py (report only) | A |

- 비거래일/휴장은 작업 내부에서 KRX 캘린더로 self-skip(평일 추정 금지).
- 스케줄러는 A 티어만. B/C(apply/public/push/deploy)는 스케줄러에 넣지 않는다.

## 7. AUTO2 구현 스크립트 후보 (다음 Phase)

| 스크립트 | 티어 | 역할 | write |
|---|---|---|---|
| magic_daily_signal.py | A | 종가 신호 패키지 생성 + nextExec 검증 + status 갱신 | TEMP only |
| magic_daily_dry_run.py | A | 날짜 파라미터화 read-only dry-run(날짜별 runner 신규작성 제거) | 0 (로그만) |
| magic_daily_status.py | A | precondition+dry-run log+live 종합 → status/BLOCKED report | TEMP only |
| magic_make_approval_ticket.py | B-prep | receipt 생성 + ticket(PENDING) 생성. 승인 요청만 | TEMP only |
| magic_apply_from_approval.py | B | 승인된 ticket 확인 후 apply --confirm(장부) 또는 missed 기록 + 백업 | canonical(승인 시) |
| magic_publish_public.py | B | build_magic_official_public → REPO1 magicOfficial 3키 표적 갱신(drift 0 검증) | REPO1(승인 시) |
| magic_live_verify.py | A | 배포 JSON/performance seq·자산·거래일 검증 | 0 |

- `magic_daily_dry_run.py`는 E17의 날짜별 runner(run_magic_official_dry_run_YYYYMMDD.py)를 **파라미터화**해
  매번 새 파일 작성을 제거한다. eval union(보유∪TOP10) 처리 포함.
- `magic_apply_from_approval.py`/`magic_publish_public.py`는 기존 apply_official_append /
  _publish 로직 재사용(복제 0). 티켓 승인 없으면 즉시 거부.
- push·commit·deploy는 스크립트에 넣지 않는다(사람이 직접). status report가 "다음 수동 명령"만 안내.

## 8. 안전 게이트 요약

1. signal: 장전 윈도우 강제(gen<open), universeBaseDate=signalDate, top10=10, SHA 일치.
2. dry-run: read-only(전후 SHA 불변), pykrx_open 전부 양수(누락→BLOCKED, fallback 0), eval union 완전.
3. apply: receipt==재계산 1원/1주 일치, canonicalBeforeSha 일치, atomic write, idempotent, **ticket 승인 필수**.
4. public: magicOfficial 3키만, top-key 불변, drift 0, 와바바/AI/PILOT 보존.
5. push/deploy: 자동 금지, 사람 직접, status가 명령만 안내.
6. 어떤 단계든 BLOCKED → 자동 중단 + BLOCKED report, canonical/public 변경 0.

## 9. AUTO3 approval-ticket 흐름(3단계)

```
A티어 자동(read-only/TEMP·스케줄러):   signal → dry-run → status
                                              │ dry-run COMPLETED
                                              ▼
B티어 승인(ticket 1건 + 사람 승인 후 고정 스크립트 1회):
   make_approval_ticket(PENDING)  →  [사람: ticket.approval.approved=true]
   → apply_from_approval --mode build-receipt(TEMP)
   → apply_from_approval --mode dry-run-apply(미저장)
   → apply_from_approval --mode apply --confirm(장부 write)         ← 승인+문구+confirm+SHA 전부 일치 시만
   → OneDrive backup
   → publish_public --mode plan(write 0)  →  publish_public --mode apply(REPO1 3키)   ← 사람 승인
   → live_verify(운영 JSON/performance read-only)
C티어 사람(자동·allowlist always 금지):  git commit · git push · Vercel deploy/promote · 실제 주문 · 파괴적 ops
```

- ticket = 승인 *요청 문서*(status=PENDING_APPROVAL, approved=false 기본). Claude가 approved=true 로 바꾸지 않는다.
- apply/publish-apply 는 ticket이 있어도 사람 승인(approved=true)+SHA 일치가 없으면 즉시 BLOCKED(변경 0).
- 어떤 단계든 SHA 변동(canonical/signal/dry-run-log/receipt) → BLOCKED, fallback 가격·가짜 거래 금지.

## 진행 상태
- **45-AUTO2(완료)**: A 티어 3종 구현 — `scripts/magic_daily_signal.py`·`magic_daily_dry_run.py`(eval
  union 파라미터화)·`magic_daily_status.py` + 공통 `magic_daily_common.py`. 테스트 17종(signal5/dry6/status6).
  Task Scheduler 등록: Wababa Magic Daily Signal(평일 15:40) / Dry Run(15:45) / Status(16:05),
  KRX 거래일 self-skip, read-only/TEMP only. allowlist 3종 추가(로컬). 장부/public/push 미자동화.
- **45-AUTO3(완료)**: B 티어(approval-ticket) 4종 구현 —
  `scripts/magic_make_approval_ticket.py`(dry-run COMPLETED→PENDING ticket, TEMP only, idempotent),
  `scripts/magic_apply_from_approval.py`(verify/build-receipt/dry-run-apply/apply; apply는 승인+문구+confirm+SHA
  전부 일치 시만, 장부 로직은 apply_magic_official_day 재사용·복제 0),
  `scripts/magic_publish_public.py`(plan/apply; REPO1 magicOfficial 3키만 표적·drift 0; 이번 Phase apply 미실행),
  `scripts/magic_live_verify.py`(운영 JSON/`/performance` read-only·deploy 0). 신규 테스트 27종.
  full suite 209 PASS / regression 75 PASS. canonical/public/REPO1 write 0. allowlist 문서 갱신.
  ★ apply/publish-apply 는 실제로 실행하지 않음(스크립트·테스트만). 스케줄러 미등록(B/C 티어는 스케줄러 금지).
- **다음 = 45-AUTO4**: approval-ticket 기반 *실제 운영 리허설* — 하루 A 티어 자동 산출물을 ticket으로 변환,
  사람 승인 후 apply/public 분리 실행(첫 실 canonical write). push/deploy/실주문은 계속 사람 승인.
