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

## 게이트 상태 (4단계)

| 상태 | 조건 |
|---|---|
| `PASS` | 이상치 없음 + 내부일관성 정상 |
| `PASS_OFFICIAL_IR_CONFIRMED` | 극단 이상치지만 공식 IR(회사 뉴스룸)과 허용오차(≤1%) 내 일치 |
| `WARNING_EXTERNAL_CONFIRMATION` | 내부일관성 정상 + 극단 이상치 + 공식 IR 미확인 → 외부확인 권장 |
| `BLOCKED_DATA_INCONSISTENCY` | 매핑/앵커 실패, 매출 물리불가, 손익 내부일관성 위반 |

- 공식 IR 확인값은 코드가 아닌 `ttm_poc_config.json`의 `officialIrConfirmations`(외부 검증 입력 데이터)로 관리. `match_official_ir()`은 종목-불문 일반 로직으로 대조 → **기업명/숫자 하드코딩·전용 예외문 없음.**

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

## 외부 검증 원천: 공식 IR 우선, 아이투자는 보조

극단 이상치 검증의 1차 원천은 **회사 공식 IR(뉴스룸/실적발표)**다 — 로그인 불필요, 공개 웹. 삼성·SK는 공식 IR만으로 확정됐다(아이투자 미사용).

**아이투자 역할(축소):** 공식 IR이 구조화 안 됐거나 접근 어려운 이상치 종목의 **보조 검산**, DART↔공식 IR 정의 차이 확인, 소수 샘플 대조에만 사용. **아이투자 유료 로그인은 전체 파이프라인의 필수조건이 아니다.**
- 사용 시 원칙: 대장 전용 프로필 1회 직접 로그인 / 비밀번호·쿠키·토큰 미저장 / 저속 read-only / 값 덮어쓰기 금지 / CAPTCHA 우회 금지 / 접근제한 확인 시 중단.
- 산출물: `_cache/ttm-poc-output/itooza-crosscheck-7-latest.{json,csv}`(공식 IR 재분류 결과, `itooza=null`), `itooza-compare-fixture.{json,csv}`(비교용).

## 안전(이번 PoC 준수)

실주문/broker API/canonical apply/public·rankings publish/recommendation-history write/운영 장부 write/공식 산식·formulaVersion 변경/전 종목 수집/스케줄러 등록/아이투자 로그인·크롤링/Vercel 배포/push/force push/`git add .`/`financial-universe-real.json` 수정/기존 연간 캐시 덮어쓰기/DART 대량·고속 호출 — **전부 미실행.**
