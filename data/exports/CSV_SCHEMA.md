# Theme Analyzer CSV Schema

이 문서는 theme_analyzer 프로젝트가 생성하는 CSV 파일의 스키마와 데이터 해석 방법을 설명합니다.
다른 프로젝트에서 이 CSV를 읽어 활용할 때 참고하세요.

---

## 파일 목록

| 파일명 | 용도 | 갱신 주기 |
|--------|------|-----------|
| `themes_history.csv` | 날짜별 테마-종목 누적 기록 | 매일 추가 (같은 날 재실행 시 해당 날짜 덮어쓰기) |
| `themes_strength.csv` | 시간가중 테마 강도 스냅샷 | 매일 전체 재계산 |

인코딩: **UTF-8 with BOM** (`utf-8-sig`). pandas로 읽을 때 `encoding="utf-8-sig"` 권장.

---

## 1. themes_history.csv (누적 히스토리)

매일 텔레그램 채널에서 수집된 메시지를 분석하여, 어떤 종목이 어떤 산업 테마로 분류되었는지 기록합니다.

### 컬럼

| 컬럼 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `date` | str (YYYY-MM-DD) | 분류 기준일 (KST) | `2026-02-08` |
| `market` | str | 시장 구분 | `KR` (한국) / `US` (미국) |
| `sector` | str | 영문 섹터 코드 (20종 고정) | `semiconductor`, `energy`, `bio` |
| `theme` | str | 한글 테마명 (산업 테마) | `반도체장비`, `전력기기/변압기`, `AI칩/GPU` |
| `ticker` | str | 종목 티커 | `267260` (KR) / `TSLA` (US) |
| `stock_name` | str | 종목명 | `HD현대일렉트릭`, `TSLA` |
| `mention_count` | int | 해당일 채널 언급 횟수 | `3` |
| `sentiment` | str | 감성 (현재 미사용, 빈 값) | `` |
| `reason` | str | 분류 이유 (15자 이내, 사업내용 기반) | `전력기기 제조`, `클라우드 인프라 CAPEX 증가` |

### 특성
- **누적 구조**: 날짜별로 행이 추가됨. 2026-02-08, 2026-02-09, ... 데이터가 쌓임
- **같은 날 재실행**: 해당 날짜의 기존 데이터를 삭제하고 새 데이터로 덮어씀
- **1행 = 1종목-1테마**: 한 종목이 여러 테마에 속할 수 있음 (드문 경우)
- **date 기준 정렬**: 날짜 오름차순으로 저장됨

### 활용 예시 (pandas)
```python
import pandas as pd
df = pd.read_csv("themes_history.csv", encoding="utf-8-sig")

# 특정 날짜의 테마별 종목 수
df[df["date"] == "2026-02-08"].groupby("theme")["ticker"].count().sort_values(ascending=False)

# 특정 종목의 테마 이력
df[df["ticker"] == "267260"][["date", "theme", "mention_count", "reason"]]

# 섹터별 종목 수 추이
df.groupby(["date", "sector"])["ticker"].nunique().unstack().fillna(0)
```

---

## 2. themes_strength.csv (강도 스냅샷)

히스토리 데이터를 기반으로 시간 가중 강도 점수를 계산한 스냅샷입니다. 매일 전체 재계산됩니다.

### 컬럼

| 컬럼 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `market` | str | 시장 구분 | `KR` / `US` |
| `sector` | str | 영문 섹터 코드 | `semiconductor` |
| `theme` | str | 한글 테마명 | `반도체장비` |
| `ticker` | str | 종목 티커 | `272110` |
| `stock_name` | str | 종목명 | `케이엔제이` |
| `strength_score` | float | 시간가중 강도 점수 | `3.45` |
| `mention_total` | int | 전체 기간 누적 언급 수 | `7` |
| `last_mention_count` | int | 가장 최근일 언급 수 (0이면 최근일에 미언급) | `2` |
| `first_seen` | str (YYYY-MM-DD) | 최초 등장일 | `2026-02-06` |
| `last_seen` | str (YYYY-MM-DD) | 최근 등장일 | `2026-02-08` |
| `days_count` | int | 등장 일수 (히스토리에서 며칠 동안 나왔는지) | `3` |
| `trend` | str | 추세 상태 | `NEW` / `ACTIVE` / `INACTIVE` |
| `last_reason` | str | 가장 최근 분류 이유 | `반도체 장비 제조` |

### strength_score 계산 공식
```
strength_score = Σ (mention_count × 0.85^days_ago)
```
- 오늘(days_ago=0): 가중치 1.0
- 1일 전: 0.85
- 7일 전: 0.32
- 14일 전: 0.10
- 30일 전: 0.007 (거의 0)

**해석**: 점수가 높을수록 최근 자주, 많이 언급된 종목. 오래 전 데이터는 자연 감쇠.

### trend 값

| 값 | 의미 |
|----|------|
| `NEW` | 오늘 처음 등장 (first_seen == 기준일) |
| `ACTIVE` | 이전에도 있었고 오늘도 언급됨 (last_seen == 기준일) |
| `INACTIVE` | 이전에 있었지만 오늘은 언급 안 됨 |

### 활용 예시 (pandas)
```python
df = pd.read_csv("themes_strength.csv", encoding="utf-8-sig")

# 강도 상위 20 종목
df.nlargest(20, "strength_score")[["theme", "ticker", "stock_name", "strength_score", "trend"]]

# 오늘 신규 등장 종목
df[df["trend"] == "NEW"][["theme", "ticker", "stock_name", "last_reason"]]

# 테마별 평균 강도
df.groupby("theme")["strength_score"].mean().sort_values(ascending=False)

# 활성 테마만 (오늘 언급된 것)
active = df[df["trend"].isin(["NEW", "ACTIVE"])]
active.groupby("theme")["ticker"].count().sort_values(ascending=False)
```

---

## 3. 섹터 코드 (sector)

20개 고정 영문 코드. 테마의 상위 산업 분류로, groupby/필터링에 활용.

| 코드 | 의미 | 대표 테마 (KR) | 대표 테마 (US) |
|------|------|----------------|----------------|
| `semiconductor` | 반도체 | HBM메모리, AI반도체, 반도체장비, 반도체소재 | AI칩/GPU, 반도체장비, 반도체패키징 |
| `ai` | AI | AI소프트웨어, AI데이터센터 | AI인프라/클라우드, AI소프트웨어/에이전트 |
| `energy` | 에너지 | 전력기기/변압기, 원전/SMR, 태양광/신재생 | 청정에너지/전력, 원전/SMR/우라늄 |
| `battery` | 배터리 | 2차전지/배터리, 배터리소재 | 배터리/리튬 |
| `bio` | 바이오/헬스케어 | 바이오/신약, 의료기기/의료AI | 바이오테크/제약, 대마초/Cannabis |
| `defense` | 방산/항공 | 방산, 우주항공, 드론/UAV | 방산/우주항공, 드론/UAV, 스페이스 |
| `auto` | 자동차 | 전기차/자율주행, 자동차부품 | 전기차/EV, 자율주행 |
| `robot` | 로봇 | 로봇/자동화 | 로봇/자동화 |
| `media` | 미디어/엔터 | 게임, 엔터/미디어, K-콘텐츠 | 디지털미디어/스트리밍, 소셜미디어 |
| `shipbuilding` | 조선 | 조선/해운 | - |
| `finance` | 금융 | 금융/보험, 리츠/부동산, 증권 | 핀테크, 금융/보험 |
| `software` | 소프트웨어 | 클라우드/SaaS, 플랫폼 | SaaS/소프트웨어, 전자상거래 |
| `telecom` | 통신 | 통신/5G, 네트워크장비 | 네트워크/통신 |
| `consumer` | 소비재 | 화장품/뷰티, K-푸드/식음료, 담배 | 소비재/유통, 럭셔리/의류 |
| `materials` | 소재/산업재 | 화학/소재, 디스플레이, 철강/비철금속 | 산업재/소재, 광물/희토류 |
| `construction` | 건설 | 건설/인프라 | - |
| `quantum` | 양자 | 양자컴퓨팅 | 양자컴퓨팅 |
| `cybersecurity` | 보안 | 사이버보안 | - |
| `blockchain` | 블록체인 | 블록체인/암호화폐 | 블록체인/암호화폐, 스테이블코인/DeFi |
| `other` | 기타 | (미분류) | (미분류) |

---

## 4. 데이터 소스 및 특성

- **수집 소스**: 텔레그램 주식 관련 채널 (수동 등록된 검증 채널)
- **분석 모델**: Claude Haiku (claude-haiku-4-5-20251001) - 텍스트/이미지에서 종목 추출 + 테마 분류
- **시간대**: 모든 날짜는 KST (Asia/Seoul) 기준
- **테마 분류 방식**: 사전 정의된 산업 테마 목록(객관식)에서 선택. 시장 이벤트(신고가, 매매 동향 등)는 테마로 분류하지 않음
- **수집 주기**: 30분마다 자동 수집, 18:00 KST 일일 리포트 생성
- **한글 테마명**: theme 컬럼은 항상 한글. 영문 분류가 필요하면 sector 코드 사용
- **KR ticker**: 한국 종목은 6자리 숫자 코드 (예: `005930` = 삼성전자)
- **US ticker**: 미국 종목은 알파벳 심볼 (예: `NVDA`, `TSLA`)

---

## 5. 주의사항

- `sentiment` 컬럼은 현재 미사용 (항상 빈 값)
- `sector`가 `other`인 경우 사전 정의 테마에 맞지 않아 기타로 분류된 종목
- `mention_count`는 해당 날짜의 채널 언급 횟수이지 거래량/시가총액과 무관
- 같은 종목이 날짜별로 다른 테마에 분류될 수 있음 (채널 맥락에 따라)
- 데이터 시작일은 프로젝트 가동 시점부터 누적
