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
3. **물리 타당성**
   - `[불가]`(→BLOCKED): 매출 음수, 또는 같은 해 분기 매출 > 그 해 연간. (매출은 항상 양수·연간 초과 불가)
   - `[검산]`(→WARNING): 최신 분기(2026Q1) 전년동기(`frmtrm_q_amount`) 대비 YoY 급변(±300% 초과) 또는 흑↔적 전환.
     * 손익은 분기별 부호가 바뀔 수 있어 `|분기|>|연간|`이 정상 성립 → '불가' 규칙 미적용(오탐 방지).
     * 아이투자 검산 Phase에서 외부 검증 대상.

## PoC 결과 (2026-07-09, 20종목)

- **판정**: PASS 13 / WARNING 7 / BLOCKED 0
- **연간 역산**: 20종목 전부 diff = 0 (누적차분 로직 정확 입증)
- **계정 확보율**: TTM 매출/영업이익/순이익, 최신 BS 5종 모두 **100%**
- **[불가] 데이터 오류**: 0건
- **WARNING 7종**: 전부 2026Q1 `[검산]` 플래그
  - 급변: 삼성전자(영업이익 YoY+756%), SK하이닉스(+405%), 롯데쇼핑(순이익+694%)
  - 흑↔적 전환: 삼성SDI, 와이솔, LG화학, 두산테스나
  - → 2026 1분기 DART 원자료가 이례적(예: 삼성 2026Q1 영업이익 57조 > 2025 연간 43.6조). **순진한 "최신분기 누적→TTM"은 이상치를 그대로 먹는다.** 검증 게이트 없이는 운영 부적합.
- **DART**: 95콜/20종목(≈4.75/종목), status 020 없음, 평균 3.4초/종목

### 전 종목 확장 예상 (eligible 1316)

- 예상 DART 콜: ≈ 6,580 (5~6/종목)
- 저속(분당 100콜 가정) ≈ 66분 / 1.1시간. 캐시 재사용 시 재실행 비용 급감.
- **리스크**: 2026Q1류 원자료 이상치 → 반드시 검증 게이트 통과 후에만 운영 반영. DART 일일 한도 사전 확인 필요.

## 아이투자 검산 Phase (다음 단계, 설계 원칙만)

이번 PoC는 itooza.com에 로그인·크롤링하지 않는다. 비교용 fixture만 생성:
`_cache/ttm-poc-output/itooza-compare-fixture.{json,csv}` (종목코드·기준분기·TTM 매출/영업이익/순이익·최신 자산/부채)

**자동 검산 설계 원칙(구현은 별도 Phase, 대장 승인 후):**
- 대장이 전용 브라우저 프로필에 **1회 직접 로그인**.
- 비밀번호·쿠키·토큰 **출력·코드 저장 금지**.
- 소수 종목 **저속 read-only** 조회만. 아이투자 값으로 공식 데이터 **덮어쓰기 금지**.
- **차이 탐지·원인분류 전용.** CAPTCHA·추가인증 우회 금지. 이용약관·접근제한 확인 시 자동화 즉시 중단.

## 안전(이번 PoC 준수)

실주문/broker API/canonical apply/public·rankings publish/recommendation-history write/운영 장부 write/공식 산식·formulaVersion 변경/전 종목 수집/스케줄러 등록/아이투자 로그인·크롤링/Vercel 배포/push/force push/`git add .`/`financial-universe-real.json` 수정/기존 연간 캐시 덮어쓰기/DART 대량·고속 호출 — **전부 미실행.**
