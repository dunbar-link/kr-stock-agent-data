# MF-TTM-BACKTEST-FEASIBILITY-AND-LEAKAGE-AUDIT

연간 공식 마법공식 vs TTM 실험 마법공식을 **과거 데이터로 공정 비교(백테스트)** 할 수 있는지 read-only 감사.
결론 요약: **과거 백테스트는 구조적 데이터 누수(look-ahead·survivorship·restatement)로 신뢰 불가 → shadow portfolio(전진검증)가 ROI 우위.**

## Gate B — 과거 point-in-time 재무 가용성

| 항목 | 상태 | 근거 |
|---|---|---|
| rcept_no·접수일 보존 | 가능 | 분기 캐시 각 파일에 `rcept_no`(예 20260310002820→접수일 20260310) 보존. publication delay 처리에 사용 가능. |
| 정정공시 이력 | **불가** | 파일당 rcept_no 1개 = fnlttSinglAcntAll이 반환한 **최신 정정본만** 저장. 원공시·이전 정정본 값 소실. |
| 과거 특정일 "그날 공개값" 재현 | **불가** | 현재 캐시 = 오늘 시점 최신본. 예: 일월지엠엘 2025 사업보고서 rcept 2026-07-07(정정 추정) — 과거 시점엔 다른 값이었을 것. |
| 원공시값 재수집 | **어려움** | DART `list.json`으로 정정공시 이력(rcept 계보)은 조회 가능하나, `fnlttSinglAcntAll`은 rcept_no 지정 조회 미지원(최신본만) → 원공시 재무값은 문서 원문 파싱 필요(대량·고난도). |

→ **판정: 현재 구조로는 point-in-time 재무 불가. 원공시 이력 재수집 필요(대규모).**

## Gate C — 과거 가격·시가총액

| 항목 | 상태 | 근거 |
|---|---|---|
| 과거 일별 종가·시총·상장주식수 | 재구축 가능 | pykrx로 과거일 조회 확인(2023-01-03 KOSPI 943종). KRX 기반 재구축(개발 필요). |
| 과거 시점 universe(상폐 전 포함) | 부분 가능 | pykrx `get_market_ticker_list(과거일)`로 그날 상장 종목 재구성 가능. |
| 생존편향 | **있음** | 현재 universe(2,765)는 각 시점 생존 종목만. 2개월 backup 대비 이탈 16종 → 3~5년이면 수백 종 상폐. 현재 universe 백테스트 = 생존편향. |
| 상폐 종목 재무 | 추가 수집 | 상폐 종목 DART 재무는 별도(정정 이력 문제 동일). |

→ **판정: 가격/시총은 pykrx 재구축 가능. 생존편향은 과거 티커로 완화 가능하나 상폐 종목 재무 추가 필요.**

## Gate D — 산식 필드 과거 가용성

| 필드 | 과거 시점 확보 | 비고 |
|---|---|---|
| 시가총액·EV(시총+부채−현금) | 가격은 pykrx 재구축 / 부채·현금은 재무(아래) | 가격 side 가능, 재무 side 정정이력 필요 |
| EBIT·유동자산·유동부채·유형자산·현금·총부채·투입자본 | **point-in-time 불가** | 정정본만 저장 → 과거 시점 원값 소실(Gate B) |

→ 연간·TTM 전략 모두 **동일 시점 재무를 못 쓰므로 현재로선 공정 과거 비교 불가.**

## Gate F — 데이터 누수·편향 판정표

| 편향 | 판정 | 근거 |
|---|---|---|
| look-ahead bias | **BLOCKED** | 현재 캐시=최신 정정본. 과거 시점 재현 불가 → 미래정보 누수. |
| survivorship bias | **BLOCKED** | 현재 universe=생존 종목만. 과거 상폐 미포함. |
| restatement bias | **BLOCKED** | 원공시값 소실(정정본만). 과거에 재작성 전 값을 못 씀. |
| publication delay | WARNING | rcept_no로 접수일 처리 가능. 단 발표당일 종가 사용 시 누수 → **익영업일 매수** 규칙 필수. |
| delisting bias | WARNING | 상폐 종목 재무/가격 이력 별도 수집 필요. |
| corporate-action bias | WARNING | 분할·합병·액면변경 미보정. 수정주가·상장주식수 보정 필요. |
| selection bias(WARNING 101 제외) | WARNING | TTM 실험 모집단이 공식(1,316)보다 작음(크래프톤·KT 등 대형주 제외). |
| transaction-cost omission | PASS | 설계에 거래비용·슬리피지 포함 가능. |
| data snooping | PASS | 단일 규칙 A(연간)/B(TTM) 비교, 파라미터 탐색 없음 → 낮음. |

→ **BLOCKED 3(look-ahead·survivorship·restatement)이 과거 백테스트의 신뢰성을 구조적으로 무너뜨림.**

## Gate G — 최소 백테스트 vs 대안

- 과거 백테스트를 하려면: (1) 원공시 이력 재수집(문서 원문 파싱, 1,316종×수년×4보고서), (2) 상폐 포함 point-in-time universe, (3) 과거 가격/시총/수정주가 재구축, (4) point-in-time 엔진. → **수 주 개발 + BLOCKED 편향 잔존.**
- **대안(권장): shadow portfolio(전진검증).** 오늘 공식 top-N과 TTM top-N을 freeze하고 6~12개월 **미래** 수익률을 병렬 관찰. 미래 데이터라 look-ahead·survivorship·restatement **원천 없음**.
  - 개발: top30 freeze + 주간 종가 스냅샷(pykrx) + 동일가중·거래비용 가정. 수 일 규모.
  - 측정: 누적수익·MDD·변동성·회전율·거래비용 반영 초과수익. TTM 50위+급변 957종 → 회전율↑ 실측.

## Gate H — ROI 판단

| | 과거 백테스트 | shadow portfolio |
|---|---|---|
| 개발량 | 대(정정이력·상폐·PIT 엔진) | 소(freeze+주간 스냅샷) |
| 데이터 수집 | 대량(원공시 원문·상폐·과거가격) | 최소(주간 가격) |
| 편향 신뢰성 | BLOCKED 3 잔존 | 누수 원천 없음 |
| 소요 | 수 주+ | 수 일 + 6~12개월 관찰 |

**결론: C. 과거 백테스트보다 shadow portfolio가 경제적.** TTM은 그때까지 **보조지표**로만 유지(공식 순위 미반영).

## 운영/커밋 영향
- 운영 마법공식·공식 순위·formulaVersion·canonical·public 불변. 이번 Phase는 read-only 감사 + 본 문서만.
- 기존 로컬 comparison commit(REPO2 d4f0feb / REPO1 a0437a4)은 조회 PoC로 유지. push는 대장 판단(shadow 결정 후 함께 처리 권장).
