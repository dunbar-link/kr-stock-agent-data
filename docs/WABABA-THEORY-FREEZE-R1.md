# WABABA THEORY FREEZE R1 — 현재 운영 공식 정본 동결

task_id: `WABABA-KOREA-MAGIC-FORMULA-LONG-HORIZON-MASTER-R1` / PHASE 0
동결 기준: 저장소 실측 (`scripts/build_magic_formula_fund.py`, `scripts/magic_rolling_engine.py`,
`magic-formula-rankings.json`, `magic-formula-official-state.json`) — 2026-08-08 시점.

> 이 문서의 목적은 **"지금 와바바가 실제로 무엇을 하고 있는가"를 문서로 박제**하는 것이다.
> 장기 백테스트로 새 전략을 찾더라도, 무엇에서 무엇으로 바뀌는지 비교 기준이 있어야 한다.

---

## 0. 가장 중요한 정정 — 현재 운영 규칙은 Magic Formula "원전"이 아니다

지금까지 문서·보고서에서 현재 50거래일 규칙을 "마법공식"이라고 불러 왔다.
실측 결과 **공식(ranking) 일부만 원전에 가깝고, 매매 규칙(trading rule)은 원전과 다르다.**

| 항목 | Greenblatt 원전 (*The Little Book That Beats the Market*) | 와바바 현재 운영 |
|---|---|---|
| 보유기간 | **1년** (세금 최적화 목적의 미세조정 포함) | **50거래일** (≈ 2.4개월) |
| 종목수 | **20~30종목** | **배치당 10종목**, 배치 50개 롤링 → 상시 약 500 lot |
| 매수 시점 | 연 1회 또는 분기 분할 | **매 개장일 1배치**(=매일 10종목) |
| 리밸런싱 | 연 1회 전량 교체 | 51번째 거래일부터 **FIFO 최고참 배치 전량매도 → 즉시 재투자** |
| 수익성 지표 | ROC = EBIT / (순운전자본 + 순유형자산) | 동일 (`EBIT / (순운전자본 + 유형자산)`) |
| 가치 지표 | EY = EBIT / EV | 동일 (`EV = 시총 + 부채총계 − 현금`) |
| 결합 | 두 순위의 합 | 동일 |
| 제외 | 금융·유틸리티, 초소형주 | 금융·유틸리티·시총 300억 미만·blacklist |
| 비용/세금 | 고려 필요 명시 | **미반영 (`FEE_TAX_MODELED = False`)** |

**결론: 현재 운영 규칙의 정확한 이름은 "Magic Formula 랭킹 + 와바바 50거래일 FIFO 롤링 매매규칙"이다.**
앞으로 문서·홈페이지에서 이것을 "마법공식 원전"이라고 표기하지 않는다.
`tradingRuleVersion = magic-50td-start-open-top10-v1` 이 이미 그 사실을 코드에 담고 있다.

---

## 1. 랭킹 공식 정본 (formulaVersion `book-faithful-v1-2026-43B5`, 2026-06-06 lock)

```
ReturnOnCapital = EBIT / (순운전자본 + 유형자산)      순운전자본 = 유동자산 − 유동부채
EarningsYield   = EBIT / EnterpriseValue              EV = 시가총액 + 부채총계 − 현금및현금성자산
profitabilityRank = ReturnOnCapital 내림차순
valueRank         = EarningsYield 내림차순
combinedRank      = profitabilityRank + valueRank      (낮을수록 우수)
```

- EBIT 원천: DART `dart_OperatingIncomeLoss` (대체 `ifrs-full_ProfitLossFromOperatingActivities`), IS→CIS 순
- 나머지 계정: `ifrs-full_CashAndCashEquivalents` / `CurrentAssets` / `CurrentLiabilities` /
  `PropertyPlantAndEquipment` / `Liabilities`
- 시가총액 단위: 억원 → `× 100,000,000` (43-B5에서 PER×순이익/mc, PBR×자본/mc 양쪽 median ≈ 1.0e8 로 검증)
- 재무 기준연도: 최신 사업보고서 CFS (2026-08-07 랭킹 = `DART 2025 CFS`)

### 1-1. 병행 보유 중인 근사(approx) 공식 — 장기 백테스트에서 결정적으로 중요

저장소에는 `formulaMode = "approx"` (`approx-roe-per-2026-43B3`) 구현이 **그대로 살아있다**.

```
profitabilityRank = ROE 내림차순           (동률 시 영업이익률, 종목코드)
valueRank         = PER 오름차순           (동률 시 PBR, 종목코드)
combinedRank      = 두 순위의 합
제외              = PER ≤ 0, ROE ≤ 0, 시총 < 300억, 금융, 유틸리티, blacklist
```

이 근사판은 **DART 없이 KRX 공표 PER/PBR/EPS/BPS 만으로 계산 가능**하다.
그래서 장기 point-in-time 백테스트가 가능한 유일한 경로다(§PHASE 1 참조).

---

## 2. 매매 규칙 정본 (`magic-50td-start-open-top10-v1`)

```
초기자본      50,000,000원
배치당 자본   1,000,000원        (5천만 ÷ 50배치)
배치당 종목   상위 10종목 (TOP_N = 10)
종목당 목표   약 100,000원        (동일가중 근사)
보유기간      50 거래일 (HOLD_TRADING_DAYS)
최대 open배치 50 (MAX_OPEN_BATCHES)
체결가        pykrx 시가(pykrx_open) 전용 — 시가 누락 시 부분매수 없이 전체 BLOCKED
매도          51번째 거래일부터 가장 오래된 open 배치 FIFO 전량매도
재투자        매도대금 + 그 배치 cashReserve → 신규 교체배치에 독립 복리 배정
              (다른 배치·전역 현금과 혼합하지 않음)
수수료/세금   0 가정 (FEE_TAX_MODELED = False)
```

- 교체는 **원자적**: 신규 매수가 BLOCKED 가능하면 기존 배치 매도도 0 (state 불변)
- due batch 2개 이상이면 자동 일괄매도 금지(`BLOCKED_MULTIPLE_OVERDUE_BATCHES`)
- `officialTradingDayIndex`(TDI) = 실제 KRX 거래일 카운터, `officialSequence`(seq) = 실제 반영 회차
- `plannedSellTradingDayIndex = buyTradingDayIndex + 50`

### 2-1. 현재 운영 실측 (2026-08-07 기준)

```
seq 27 · TDI 29 · 누적 lot 270 · 고유 보유종목 16 · 미반영 거래일 0
가상 현금 23,000,000원 · 평가액 26,166,701원 · 총자산 51,751,159원
missedRuns 2건 (2026-06-18, 2026-06-22 · NO_PREOPEN_SIGNAL_PACKAGE)
첫 만기 FIFO 매도 예정: TDI 51 (아직 도래 전 — 매도 실적 0건)
```

> **중요**: 현재 운영은 **아직 한 번도 만기 매도를 하지 않았다**(TDI 29 < 51).
> 즉 현재 공개 포트폴리오는 사실상 **매수만 27회 누적한 상태**이며,
> "50거래일 회전 전략"의 성과는 아직 단 한 사이클도 관측되지 않았다.
> 홈페이지에서 현재 수익률을 전략 성과로 제시하면 안 되는 이유다.

---

## 3. universe 정본

| 항목 | 값 |
|---|---|
| 원천 | pykrx (KOSPI + KOSDAQ 전종목) |
| 전체 universe (2026-08-07) | 2,763 |
| 적격(eligible) | 1,313 |
| DART coverage | 96.73% (2,017 시도 / 1,951 성공) |

제외 실측 (2026-08-07):

```
financial 308 · utilities 12 · ebitInvalid 611 · capitalBaseInvalid 27
dartMissing 66 · marketCapBelow 426 · evInvalid 0 · blacklisted 0
```

제외 규칙:

```
financeIndustries   금융 · 기타금융 · 증권 · 은행 · 보험
financeNameKeywords 증권 · 은행 · 지주 · 홀딩스 · 캐피탈 · 보험 · 파이낸셜 · 카드
utilityIndustries   전기·가스 · 전기·가스·수도
utilityNameKeywords 한국전력 · 한국가스공사 · 지역난방 · 수자원
minMarketCap        300 (억원)
```

**미구현 제외 항목(현재 코드에 없음 — 향후 연구 대상)**: 관리종목 · 거래정지 · SPAC · 리츠 · 우선주 ·
감사의견 거절. `financeNameKeywords` 의 "지주/홀딩스"는 비금융 지주회사까지 걸러낼 수 있어
**과대제외 가능성**이 있다(민감도 검사 대상).

---

## 4. 데이터 원천 정본

| 데이터 | 원천 | 실측 범위 |
|---|---|---|
| 가격(체결) | pykrx 시가 | 당일 |
| 가격(평가) | pykrx 종가 (거래가와 source 분리) | 당일 |
| 시가총액 | pykrx | 1999~ |
| PER/PBR/EPS/BPS/DIV/DPS | pykrx (KRX 공표) | **1999-01-04 ~** |
| 재무제표(EBIT 등) | DART OpenAPI → `_cache/dart-statements` | **FY2022 ~ FY2025 만** |
| 거래일 캘린더 | canonical `officialKrxTradingCalendar` | 운영 개시 이후 |
| 지수 benchmark | pykrx 1001/2001 | KOSPI 1990-01-03~, KOSDAQ 1996-07-01~ |

---

## 5. 현재 규칙의 알려진 한계 (백테스트로 검증해야 할 것)

1. **보유기간 50거래일의 근거가 없다.** 원전은 1년이다. 왜 50인지 실증 근거가 문서에 없다.
2. **배치당 10종목은 분산이 얕다.** 원전 권고는 20~30이다.
3. **매일 매수는 일반인이 따라 할 수 없다.** 개인이 매 개장일 10종목씩 사는 것은 비현실적이다.
4. **수수료·세금 0 가정.** 회전율이 높은 전략에서 이 가정은 성과를 크게 왜곡한다.
   (50거래일 회전 = 연 약 5회전. 매도세 0.20% + 수수료를 넣으면 연 2%p 이상 차감 가능)
5. **상장폐지·거래정지 처리 규칙이 없다.**
6. **benchmark 비교가 없다.** 절대 수익만으로는 전략 가치를 판정할 수 없다.
7. **아직 만기 사이클 0회** — forward-test 표본이 사실상 없다.

---

## 6. 동결 선언

- 이 문서에 적힌 것이 **2026-08-08 시점 와바바 운영 정본**이다.
- 장기 백테스트가 다른 결론을 내더라도 **현재 운영 규칙은 자동으로 바뀌지 않는다.**
  전환은 Founder 승인 + `LEGACY_50D` / `NEW_CANONICAL_STRATEGY` 병렬 관찰을 거친다.
- 현재 public paper portfolio 는 **legacy experiment(forward-test 자산)로 보존**한다.
