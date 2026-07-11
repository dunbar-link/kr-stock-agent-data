# MF-TTM-DART-20-STOCK-POC — 분기 DART 기반 TTM 복원 PoC

DART 공식 분기보고서로 대표 20종목의 **최근 4개 단일분기 손익**을 복원하고 **TTM(Trailing Twelve Months)** 계산 가능성을 검증하는 PoC. 전 종목 수집·운영 공식 산식 변경·아이투자 자동화는 하지 않는다.

## 파일 구성 (REPO2: kr-stock-agent-data-new)

| 파일 | 역할 | commit |
|---|---|---|
| `scripts/poc/ttm_poc_config.json` | 20종목 선정 + 선정 근거 | O |
| `scripts/poc/ttm_core.py` | 순수 복원/TTM/검증 로직(네트워크·IO 0) | O |
| `scripts/poc/quarterly_ttm_poc.py` | DART 조회 오케스트레이터(캐시·검증·출력) | O |
| `scripts/poc/test_ttm_core.py` | 오프라인 단위 테스트(36) | O |
| `scripts/poc/README-ttm-poc.md` | 본 문서 | O |
| `_cache/dart-statements-quarterly/*.json` | 분기 원본 응답 캐시 | X (gitignore `_cache/`) |
| `_cache/ttm-poc-output/*.json,*.csv` | PoC 결과·아이투자 fixture | X (gitignore `_cache/`) |

> 운영 파일(`financial-universe-real.json`, `magic-formula-*.json`)과 기존 연간 캐시(`_cache/dart-statements/`)는 **읽기만** 한다. 쓰지 않는다.

## 실행

```
# REPO2 루트에서 DART 키 로드 후:
#   $env:DART_API_KEY = (Get-Content .env.local | Select-String '^DART_API_KEY=').ToString().Split('=')[1]
py scripts\poc\test_ttm_core.py                 # 오프라인 단위 테스트(무네트워크)
py scripts\poc\quarterly_ttm_poc.py             # 20종목 DART 조회 + TTM 복원
#   환경변수: POC_SLEEP(요청간격,기본0.5) POC_OFFLINE=1(캐시만) POC_LIMIT=N(앞 N종목)
```

> Windows 주의: `python`은 Store 스텁이라 실행 불가. 실제 인터프리터 `py` 또는 python.exe 직접 호출.

## 분기 캐시 구조

기존 연간 캐시와 **파일명으로 분리**해 충돌을 원천 차단한다.

```
기존(연간, 수정 금지):  _cache/dart-statements/{corp}_{year}_{CFS|OFS}.json      (reprt_code 없음=11011)
신규(분기, 이번 PoC):   _cache/dart-statements-quarterly/{corp}_{year}_{reprt}_{CFS|OFS}.json
```

- `reprt_code`: `11013`(1분기) `11012`(반기) `11014`(3분기) `11011`(사업/연간)
- 응답 body(list)만 저장. `crtfc_key`·요청 헤더·인증정보는 저장하지 않는다.
- 캐시 hit 시 네트워크 호출 0 → **재실행 결정론**(`POC_OFFLINE=1`로 무네트워크 재검증).

## reprt_code / fsDiv 정책

- **fsDiv lock**: 종목별 `dartFsDiv`(연간 기준)로 고정. CFS 우선, 별도만 있으면 OFS.
  한 종목의 5개 보고서는 **동일 fsDiv만** 사용 → 연결/별도 혼용 금지. 앵커(2025 사업보고서)로 확정.
- **손익(IS/CIS) vs 재무상태표(BS)**: `sj_div`를 존중해 분리 추출. 손익 계정은 IS/CIS에서만, BS 계정은 BS에서만 → 동명 계정 오염 방지.

## 단일분기 복원 산식 (12월 결산)

각 보고서의 **누적 손익**(분기=`thstrm_add_amount`, 1분기·연간=`thstrm_amount`)을 차분한다.

```
Q1 = cum(11013)
Q2 = cum(11012) - cum(11013)      # 반기누적 - 1분기누적
Q3 = cum(11014) - cum(11012)      # 3분기누적 - 반기누적
Q4 = cum(11011) - cum(11014)      # 연간   - 3분기누적
TTM = 최근 연속 단일분기 4개 합
```

**2026-07 기준 TTM 윈도우** = `2026Q1 + 2025Q4 + 2025Q3 + 2025Q2`
(2026 반기보고서 11012는 8월 공시로 미존재 → 최신은 2026 1분기까지)

- **재무상태표(BS)는 누적 합산하지 않는다.** 최신 분기 말(2026Q1, 없으면 2025FY) **시점값**을 사용.
- 손익 계정만 4개 단일분기를 합산한다.

## 검증 게이트

1. **연간 역산**: 2025 Q1+Q2+Q3+Q4 == 2025 연간. (누적차분 telescoping — 4개 보고서 확보 시 정확 일치. 결측 시 reconstructable=false)
2. **매출 단조성**: Q1≤H1≤3Q≤FY 아니면 정정공시/데이터 이상 경고.
3. **매출 물리 불가(→ BLOCKED_DATA_INCONSISTENCY)**: 매출 음수, 또는 같은 해 분기 매출 > 그 해 연간. (매출은 양수·연간 초과 불가)
4. **손익 내부일관성(→ 위반 시 BLOCKED_DATA_INCONSISTENCY)**: 매출=매출원가+매출총이익, 매출총이익≤매출, 영업이익≤매출총이익. 성립하면 극단값이라도 '데이터 오류'가 아니라 '실제 큰 값'으로 본다.
5. **극단 이상치(손익, → WARNING_EXTERNAL_CONFIRMATION / IR확인 시 PASS_OFFICIAL_IR_CONFIRMED)**
   - 영업이익/순이익 단일분기 > 직전 연간(같은 부호), 또는 2026Q1 YoY 급변(±300%↑)/흑↔적 전환.
   - **핵심: "분기 > 직전 연간"은 물리적 불가능이 아니다.** 2026Q1(2026 회계연도) vs 2025 연간은 서로 다른 해라 초고성장 시 성립 가능(삼성 2026Q1 영업익 57.2조 > 2025연간 43.6조 = 실제 슈퍼사이클).
6. **정정 신호(restatement, 정보 기록만)**: 2026Q1 보고서의 전년동기(`frmtrm_q`)가 2025Q1 원보고서와 불일치 → 비교표시 차이. **TTM은 각 보고서 당기값만 쓰므로 값에 영향 없음.** 상태를 낮추지 않고 기록만.

## 게이트 상태 (5단계) — 흑↔적 전환 튜닝 반영

| 상태 | 조건 |
|---|---|
| `PASS` | 이상치 없음, 또는 절대규모 작은 급변(경미)뿐 + 내부일관성 정상 |
| `PASS_WITH_TRANSITION_NOTE` | 흑↔적 전환만(절대규모 작음) — 정상 이벤트, 외부확인 불필요, note만 기록 |
| `PASS_OFFICIAL_IR_CONFIRMED` | 절대규모 큰 극단 + 공식 IR(회사 뉴스룸)과 허용오차(≤1%) 내 일치 |
| `WARNING_EXTERNAL_CONFIRMATION` | 절대규모 큰 극단 + IR 미확인, 또는 보고서 누락으로 TTM 미완성 |
| `BLOCKED_DATA_INCONSISTENCY` | 매핑/앵커 실패, 매출 물리불가, 손익 내부일관성 위반 |

**이상치 등급(`outlier_flags`)**: `revenueHard`(→BLOCKED) / `significantExtreme`(절대규모 큼 + 극단신호 → WARNING/IR) / `transitions`(흑↔적, 절대규모 작으면 note) / `minorFlags`(절대규모 작은 급변 → PASS).

**핵심 원칙**: 흑↔적 전환·비율 급변'만'으로는 WARNING을 만들지 않는다. **절대 원화 규모(영업익/순익 2026Q1 절대값)와 함께** 판단한다. 시가총액은 진실성 기준으로 쓰지 않는다.

**임계값(config `gateThresholds`, 코드에 산발 하드코딩 없음)**:
- `largeAbsOperatingIncomeKrw`=2000억(2e11): 100종 2026Q1 영업익 분포상 상위 4종(SK이노 2.16조·POSCO 7068억·롯데 2529억·두산에너 2335억)과 5위(212억) 사이 자연 갭. `yoyExtremePct`=3.0, `qoqExtremePct`=2.0.
- 코드의 `DEFAULT_GATE_THRESHOLDS`는 config 미제공 시 fallback 단일 상수(테스트로 config override 입증). 종목코드/기업명/실적값 하드코딩 0. `match_official_ir()`도 config `officialIrConfirmations`만 대조하는 종목-불문 로직.

### 게이트 튜닝 결과 (100종, 재분류)
| | 튜닝 전 | 튜닝 후 |
|---|---|---|
| PASS | 59 | 67 |
| PASS_WITH_TRANSITION_NOTE | — | 20 |
| PASS_OFFICIAL_IR_CONFIRMED | 2 | 2 |
| WARNING_EXTERNAL_CONFIRMATION | **39** | **11** |
| BLOCKED | 0 | 0 |

- WARNING 39→11(−28). 잔존 11 = 절대규모 큰 극단 6(SK이노·POSCO·SK텔레콤·롯데·두산에너·LG화학) + 보고서 누락 5(신규상장).
- 흑↔적 단독 20종은 `PASS_WITH_TRANSITION_NOTE`로 완화(전부 op26<2000억). 삼성·SK는 IR 확정 유지. 절대규모 크지만 극단신호 없는 안정적 대형주(기아·현대차 등)는 정상 `PASS`(잘못 완화 아님).
- 연간역산 96/96 diff=0 유지, 내부일관성 위반 0, 신규 DART 호출 0(캐시 재사용).

## PoC 결과 (2026-07-09, 20종목) — IR 게이트 적용 후

- **판정**: PASS 13 / PASS_OFFICIAL_IR_CONFIRMED 2 / WARNING_EXTERNAL_CONFIRMATION 5 / BLOCKED 0
- **재분류(정정 전→후)**: WARNING 7 → 삼성전자·SK하이닉스는 공식 IR 일치로 `PASS_OFFICIAL_IR_CONFIRMED`, 나머지 5(삼성SDI·와이솔·LG화학·롯데쇼핑·두산테스나)는 `WARNING_EXTERNAL_CONFIRMATION`. 기존 PASS 13종 **퇴행 0**.
- **공식 IR 대조**: 삼성 영업익 DART 57,232,797백만 vs IR 57조2328억(차이 −3백만, −0.00001%) / SK 37,610,283백만 vs IR 37조6103억(차이 −17백만, −0.00005%) → 사실상 완전 일치.
- **연간 역산**: 20종목 전부 diff = 0. **계정 확보율**: TTM 3종 + BS 5종 100%. **BLOCKED**: 0.
- **DART**: 95콜/20종목, status 020 없음, 평균 3.4초/종목.

### 오분류 교훈
직전 Phase는 "분기 영업익 > 직전 연간"을 물리적 불가능(DATA_ANOMALY)으로 오판했다. 그러나 이는 서로 다른 회계연도 비교이고, 반도체 슈퍼사이클로 삼성·SK 모두 실제 사상 최대 실적이었다. **단순 임계값이 아니라 내부일관성 + 공식 IR 대조로 '드문 실제값'과 '데이터 오류'를 구분한다.**

### 전 종목 확장 예상 (eligible 1316) — ROI
- 예상 DART 콜 ≈ 6,580(5~6/종목), 저속 ≈ 1.1시간. 캐시 재사용 시 급감.
- **공식 IR 검산은 전 종목이 아니라 극단 이상치 종목에만 적용.** 내부일관성 통과 일반 종목은 그대로 사용(PASS). 극단 이상치만 공식 IR/보조출처 확인 대상(WARNING_EXTERNAL) — 20종 표본에서 5건(=WARNING 후보를 일부러 담은 표본이라 실제 universe 비율은 더 낮음).

## 100종목 규모 검증 (MF-TTM-DART-100-STOCK-SCALE-CHECK)

`select_ttm_100.mjs`가 eligible universe에서 **결정론적으로 100종**을 선정(seed20 강제포함 + 시총밴드×업종 라운드로빈 + OFS≥18, 무작위 없음 → 재실행 md5 동일). `ttm_100_config.json`으로 실행:
```
node scripts/poc/select_ttm_100.mjs
POC_CONFIG=...\ttm_100_config.json POC_OUT_BASENAME=ttm-100-stock-latest POC_SLEEP=0.4 py scripts\poc\quarterly_ttm_poc.py
node scripts/poc/build_100_reports.mjs   # 요약/이상치/CSV
```

**선정 분포**: CFS 82 / OFS 18, 시총밴드 large 51·mid 30·small 19, 업종 21종, magic top100 5종 포함.

**결과(2026-07-09, 100종)**:
- 게이트: **PASS 59 / PASS_OFFICIAL_IR_CONFIRMED 2 / WARNING_EXTERNAL_CONFIRMATION 39 / BLOCKED 0**
- 연간역산: **diff=0 96종 / 누락 4종**(신규상장). 내부일관성 위반 0. 계정 확보율: TTM 3종 95%(미완성 5종=신규상장 분기 부재), BS 5종 100%.
- WARNING 39 = 극단이상치 36(흑↔적 36·YoY 17·분기>연간 24, 중복) + 보고서누락 5. → **소형·중형주 흑자↔적자 전환이 다수**. BLOCKED가 아니라 "외부확인 권장" 등급.
- DART: 400콜/cacheHit 100/noData 8, status 020 **없음**, 평균 2.79초/종(총 ~4.6분).

**전 universe 확장 추정(실측 기반, 종목당 신규 5콜·2.79초)**:
| universe | DART콜 | 시간 |
|---|---|---|
| eligible 1,316 | ≈6,580 | ≈1.0시간 |
| 비금융 2,365 | ≈11,825 | ≈1.8시간 |

DART 일일한도(계정 통상 ~2만콜) > 필요콜 → 하루 내 가능. 재실행은 캐시로 급감.

**권장 운영 배치**: 1회 전체수집은 eligible 1,316 기준 ~1시간·6,580콜로 **200종×7배치**(배치 간 여유)면 안전. 이후는 **공시시즌 증분**(분기보고서 신규 접수분만 갱신)으로 콜을 최소화. 공시 비수기엔 갱신 불필요. 극단 이상치(흑↔적/초고성장)만 공식 IR 확인 대상 — 100종 표본 기준 극단 36건이나, 흑↔적을 규모조건부로 완화하면 실질 외부확인 대상은 크게 감소(향후 게이트 튜닝 후보).

## eligible 배치 실행 프레임워크 (MF-TTM-ELIGIBLE-BATCH-RUNNER-PREP)

향후 eligible 전체를 **200종×배치로 저속·재개 가능하게** 수집하기 위한 준비. 기존 PoC 코드/캐시를 재사용(새 프레임워크 아님). **이번 단계에서 실제 DART 호출 0**(기본 안전모드).

```
node scripts/poc/build_batch_manifest.mjs         # 대상 선정 + 배치 분할(결정론). 전체 manifest→_cache(gitignore)
py scripts/poc/batch_runner.py --batch-id batch-01 --dry-run   # 계획만(호출 0)
py scripts/poc/batch_runner.py --batch-id batch-01 --offline   # 캐시만 재계산(호출 0)
py scripts/poc/batch_runner.py --batch-id batch-01 --real-fetch --confirm RUN_TTM_BACKFILL_2026-07-09_batch-01  # 승인 필요(미실행)
```

**대상(공식 magic eligible, 근사 제거)**: `extract_official_eligible.py`가 운영 canonical 함수
`build_magic_formula_fund.calculate_book_faithful_magic_ranking()`를 read-only 호출해 **정확히 1,316종**
(excluded가 rankings.json과 완전 일치: financial 309·ebitInvalid 610·marketCapBelow 429 등)을 추출.
근사(opMargin>0·cap≥300억, 1366종)는 공식과 60종 차이(근사에만 55=POSCO홀딩스 등 EBIT≤0 제외분, 공식에만 5=SK디앤디 등 근사 과다제외)라 **배치 대상에서 제거**(config `eligibilityFilterDeprecated_sampleOnly`, 100종 표본 전용). 공식 combinedRank 순 정렬 → **7배치**(200×6 + 116). 누락 0·중복 0·종목당 1배치. codeSetHash로 결정론 검증.

**DART 오류코드 처리**: 013/014(no data)=보고서 누락(정상), 020=RateLimited(재개 가능), **012(접근불가 IP)/901(키만료)/011(미등록 키)=FatalDartError로 즉시 중단**(재시도 무의미, 콜 낭비 방지).

**배치 실행기 옵션**: `--batch-id --dry-run --offline --real-fetch --confirm --resume --max-api-calls --sleep-seconds --stop-on-020 --output-dir`. **기본은 dry-run**(무옵션 시). real-fetch는 `--confirm RUN_TTM_BACKFILL_<baseDate>_<batchId>` 유효 토큰 필수.

**상태·재개**(state 파일 `_cache/.../batch-state-<id>.json`, gitignore): 종목별 PENDING/RUNNING/COMPLETE/PARTIAL/RATE_LIMITED/FAILED/SKIPPED_CACHE_HIT. `--resume`은 COMPLETE/SKIPPED 재호출 안 함. 캐시 완비 종목은 real-fetch에서 SKIPPED_CACHE_HIT(재호출 0). **status 020 → RATE_LIMITED 저장 + 즉시 중단**(자동 재시도 없음), resume 인덱스 기록. 상태파일에 키/URL키/쿠키/토큰 **미저장**.

**증분 수집 모드**(설계): initial-backfill(200종 단위) / disclosure-incremental(신규 공시 종목만) / retry-failed(실패·누락만, backoff) / offline-recompute(호출 0, 캐시로 게이트 재계산). 공시 접수목록 대량 조회는 새 API 구조 필요 → 설계·fixture까지만, 이번 범위 확장 안 함.

**설정 단일 출처(`batchConfig`)**: batchSize 200 · requestSleepSeconds 0.4 · maxApiCallsPerBatch 1200 · retryCount 2 · backoffSeconds 60 · stopOnStatus020 true. 기업별/종목별 예외 없음.

**예상 운영량(실측 5콜·2.79초/종 기반)**:
- 최초 backfill: pending 1291종 × 5 ≈ **6,455 신규콜**, cache hit 75종. 배치당 ~9~10분, 7배치 ≈ **1.1시간**.
- 저장용량: 1366×5 ≈ 6,830 분기캐시 파일 ≈ 200MB. DART 일일한도(~2만콜) > 필요콜.
- 게이트 예상(100종 표본 비율): WARNING_EXTERNAL ≈ 11%(≈150종), 공식 IR 확인 대상(규모큰극단) ≈ 6%(≈82종) — 사람 검토 가능 수준.

**실제 전체 수집 승인 게이트**: real-fetch·연속 배치·운영 반영·공식 산식·public/canonical·scheduler·push/deploy는 **대장 명시 승인** 필요. 승인 토큰 `RUN_TTM_BACKFILL_<baseDate>_<batchId>`. manifest/dry-run/offline/캐시검증/테스트/통계는 자동 허용.

### batch-01 real-fetch 파일럿 결과 (2026-07-09 기준) — WAIT
- batch-01(공식 상위 200종) real-fetch 시도 → **DART status=012 "접근할 수 없는 IP입니다"**로 즉시 중단. 데이터 수집 0.
- 원인: DART 오픈API 키가 **등록 IP만 허용**하는데 현재 IP(수집 환경)가 화이트리스트 밖. 외부 인프라 이슈(코드 문제 아님).
- 개선 전 구 코드는 012를 일반 FAILED로 처리해 189종 전부 시도(189콜 낭비) → **012/901을 FatalDartError로 즉시 중단**하도록 자체수정. 재실행 시 첫 종목 1콜만 소비하고 중단(실증 완료).
- **재개 조건(대장 조치 필요)**: DART 오픈API 콘솔에서 수집 환경 IP를 허용 IP에 추가하거나 IP 제한 해제. 이후 동일 토큰으로 `--resume` 재실행하면 미완료 종목부터 이어서 수집.

## BLOCKED 데이터 무결성 3종 처리 (1,316 통합)

통합에서 BLOCKED_DATA_INCONSISTENCY로 격리된 3종의 DART 원자료 원인과 처리:

| 종목 | 원인(원자료) | 처리 |
|---|---|---|
| 폴라리스오피스(041020) | 매출액이 `ifrs-full_GrossProfit`(nm=영업수익, 3,242억)에 태깅 → revenue행=grossProfit행 동일 → `매출=매출원가+매출총이익` 오탐. 매출 자체는 정상(누적 단조·연간역산 정상). | **C. 태깅 특이 → 일반화 수정**: GP≥매출×0.98이면 GP 태그 오용으로 보고 GP 기반 검사 스킵(`grossProfitTagSuspect`). → **PASS 복원**(TTM매출 3,253억) |
| 대신정보통신(020180) | 3분기 누적(2,190억) > 연간(2,029억) → Q4 단일 매출 −161억(음수). 2026Q1 미제출. 최신 캐시에서도 누적 역전. | **E. KEEP_BLOCKED**(원자료 모순, TTM 신뢰 복원 불가) |
| 일월지엠엘(178780) | 반기 누적(37억) < 1분기 누적(45억) → Q2 단일 매출 −8억(음수). 연간 339억이 3분기누적 70억 대비 과대(정정/소급 혼재). | **E. KEEP_BLOCKED**(원자료 모순) |

- **일반화 규칙(gp_tag_suspect)**: 정상 기업은 매출총이익 < 매출. GP가 매출과 사실상 같으면 XBRL 태깅에서 매출액이 GrossProfit 태그에 잘못 붙은 것으로 판단해 GP 기반 내부일관성 검사만 스킵(매출·게이트는 유지). 종목코드/기업명 하드코딩 0(테스트로 정상 기업은 검사 유지 입증).
- **매출 음수(누적 역전)는 revenue_hard → BLOCKED 유지**가 정상. 정정공시로 최신본을 골라도 캐시 역전이 남으면 회사 공시 자체 모순 → TTM 제외.
- 결과: BLOCKED 3→2(폴라리스 해제). 나머지 1,313종·삼성/SK IR 불변(퇴행 0).

## TTM vs 연간 비교 대시보드 PoC (조회 전용, 운영 미반영)

`build_ttm_comparison.py`가 **공식 연간 마법공식값(canonical 함수)** 과 **TTM 실험값**을 나란히 담은 비교 데이터셋을 만든다. REPO1 `app/internal/ttm-comparison`(nav 미노출 내부 route)이 소형 fixture(20종)로 조회.

**산식 대응(Gate B0)**: annual EBIT = `dart_OperatingIncomeLoss`(영업이익, IS→CIS). TTM operatingIncome = 동일 계정의 최근 4분기 합 → **정의 동일(기간만 롤링)** → experimental 순위 산출 허용. EV=시총×1e8+총부채−현금, 투입자본=유동자산−유동부채+유형자산(모두 최신 분기말 BS).

**포함/제외**: PASS·PASS_WITH_TRANSITION_NOTE·PASS_OFFICIAL_IR_CONFIRMED만 실험(1,213종). WARNING(101)·BLOCKED(2)·미완성은 제외(삭제 아님, 사유 목록 별도). experimental 순위 산출은 EBIT>0·투입자본>0·EV>0 유효 종목(1,162)만.

**필드 분리**: `annual.*`(공식값) vs `ttmExperiment.experimental*`(실험값). 혼동 방지 접두어. 순위변화는 동일 실험집합 내 재순위(subset) 기준으로 계산(selection bias 최소화).

**결과(전체 실험집합)**: 공식 top100 ∩ 실험 top100 = 63(신규진입 37/이탈 37). 평균 순위변화 0(subset zero-sum), 중앙 6, **50위+ 급변 957종** → TTM은 순위를 크게 흔든다(데이터 신선도 민감도 큼). *수익률 개선 주장 아님·백테스트 없음.*

**UI(조회 전용)**: 요약카드·순위민감도·필터(검색/상태/급변)·비교테이블·종목상세(4분기 영업익·TTM합·최신BS기준일·공식값·제외사유). "운영 미반영/투자 추천 아님/비교용 PoC" 배지. 해석성(추천·상승가능성 등) 문구 없음.

## 운영 반영 정책 (다음 Phase용, 이번엔 반영 안 함)
- **PASS / PASS_WITH_TRANSITION_NOTE / PASS_OFFICIAL_IR_CONFIRMED** = TTM 사용 후보.
- **WARNING_EXTERNAL_CONFIRMATION** = 외부(공식 IR) 검산 전에는 공식 TTM 산출에서 제외하거나 별도 플래그.
- **BLOCKED_DATA_INCONSISTENCY** = TTM 사용 금지.
- **TTM 제외 ≠ 연간 마법공식 순위 제외**: TTM은 실험값. 기존 연간 공식값과 혼합하지 않는다. TTM에서 제외돼도 연간 magic eligible/순위는 불변.

## Shadow Portfolio PoC (전진검증, 조회 전용)

과거 백테스트가 데이터 누수로 불가([BACKTEST-FEASIBILITY-AUDIT](BACKTEST-FEASIBILITY-AUDIT.md))하므로, 오늘 시점에 두 전략을 freeze하고 미래 성과를 병렬 관찰한다(미래 데이터 → look-ahead·survivorship·restatement 원천 없음).

```
node ... (선행: build_ttm_comparison.py)
py scripts/poc/build_shadow_freeze.py          # 공식 top30 / TTM top30 freeze
py scripts/poc/shadow_portfolio.py --initialize # 계산기 dry-run(가격조회 0)
```

- **freeze(전략 A=공식 연간 top30, B=TTM 실험 top30)**: freezeDate=priceAsOfDate=universe baseDate. 재무 최신(2026Q1, 5월 공시) < 가격일 → 누수 없음. 동일가중(종목당 100만·초기 3,000만), 정수 주식, 잔여현금 기록. TTM top30은 PASS 계열만(WARNING/BLOCKED 미포함, 증명 필드). 교집합 19 / 공식전용 11 / TTM전용 11.
- **계산기(shadow_portfolio.py)**: `compute_snapshot`(시장가치+현금−비용→총자산·수익률, 가격누락 임의보정 금지) · `performance`(누적·주간·MDD·변동성, turnover=0 buy-and-hold) · `is_duplicate_snapshot`(멱등). 기본 dry-run, `--real` 가격조회는 별도 승인. 거래정지/corporate-action/상장폐지 플래그.
- **config(shadow_portfolio_config.json)**: 왕복비용 가정 0.4%(단정 아님), benchmark=KOSPI 단일, 리밸런싱 없음, 관찰 최소 6개월·권장 12개월.
- **UI**: REPO1 `app/internal/ttm-shadow`(nav 미노출) — 두 전략 요약카드·교집합·보유종목표·스냅샷이력(관찰 전). "가상 포트폴리오·실주문 없음·투자 추천 아님" 배지, 우열 결론 문구 없음.
- **자동 관찰(설계만)**: 매주 금요일 장 마감 후 종가 스냅샷 권장. read-only·runtime write·멱등·휴장 처리. 운영 파이프라인 영향 0. **스케줄러 등록은 별도 승인.**

## 외부 검증 원천: 공식 IR 우선, 아이투자는 보조

극단 이상치 검증의 1차 원천은 **회사 공식 IR(뉴스룸/실적발표)**다 — 로그인 불필요, 공개 웹. 삼성·SK는 공식 IR만으로 확정됐다(아이투자 미사용).

**아이투자 역할(축소):** 공식 IR이 구조화 안 됐거나 접근 어려운 이상치 종목의 **보조 검산**, DART↔공식 IR 정의 차이 확인, 소수 샘플 대조에만 사용. **아이투자 유료 로그인은 전체 파이프라인의 필수조건이 아니다.**
- 사용 시 원칙: 대장 전용 프로필 1회 직접 로그인 / 비밀번호·쿠키·토큰 미저장 / 저속 read-only / 값 덮어쓰기 금지 / CAPTCHA 우회 금지 / 접근제한 확인 시 중단.
- 산출물: `_cache/ttm-poc-output/itooza-crosscheck-7-latest.{json,csv}`(공식 IR 재분류 결과, `itooza=null`), `itooza-compare-fixture.{json,csv}`(비교용).

## 안전(이번 PoC 준수)

실주문/broker API/canonical apply/public·rankings publish/recommendation-history write/운영 장부 write/공식 산식·formulaVersion 변경/전 종목 수집/스케줄러 등록/아이투자 로그인·크롤링/Vercel 배포/push/force push/`git add .`/`financial-universe-real.json` 수정/기존 연간 캐시 덮어쓰기/DART 대량·고속 호출 — **전부 미실행.**
