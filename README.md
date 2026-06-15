# Wooriport Mock Server

마이데이터 거래 이벤트를 흉내 내는 개발용 mock 서버입니다.
SQL로 알림을 직접 꽂는 대신, **실제 거래를 Kafka로 발행**해 백엔드 파이프라인
(컨슈머 → 챌린지/급여 서비스)을 그대로 거치게 해서 '진짜' 알림이 생성되도록 합니다.

> 실행 순서: **인프라 → 백엔드 → 목서버**
> 인프라(Docker)와 백엔드가 먼저 떠 있어야 합니다.

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Language | Python 3 |
| Kafka 프로듀서 | confluent-kafka 2.14 |
| DB 조회 | psycopg2 (PostgreSQL) |

---

## 시스템 아키텍처

```
mock_payment.py (자동 랜덤 발행)   mock_scenarios.py (시나리오 선택)
        │                                │ ① DB 조회 (급여계좌·활성 챌린지·카드)
        │                                ▼
        │                          PostgreSQL (:5432, users·assets·mini_challenges)
        │                                │ ② 시나리오 거래 생성
        └────────────┬───────────────────┘
                     ▼  send_transaction()
        Kafka (:9092, Topic: transaction-events, key=asset_number)
                     ▼
        Spring Boot Consumer ──► 챌린지/급여 서비스 ──► 알림 생성
```

- **Topic**: `transaction-events`
- **메시지 Key**: `asset_number` — 같은 카드 거래는 같은 파티션으로 라우팅(순서 보장)
- **거래 대상 카드**: 시작 시 DB `assets` 의 `asset_type='CREDIT_CARD'` 자산을 1회 로드
  - ⚠️ **CREDIT_CARD 자산이 없으면 실행되지 않습니다.** 시드로 카드를 만들고 `/linking` 으로 연동한 뒤, mock 을 **재시작**해야 새 카드가 반영됩니다.

---

## 스크립트 구성

| 파일 | 역할 |
|------|------|
| `mock_payment.py` | **자동 발행** — 2초에 1건씩 랜덤 거래를 무한 전송. `send_transaction()`·`generate_transaction()`·CREDIT_CARD 풀 제공 |
| `mock_scenarios.py` | **시나리오 트리거** — 월급 변동·챌린지 진행도 등 8개 알림 시나리오를 재현 (`mock_payment` 의 `send_transaction` 재사용) |

> `mock_scenarios.py` 는 `from mock_payment import send_transaction` 으로 Kafka 발행 로직을 재사용합니다(단일 출처). 두 파일은 같은 폴더에 함께 있어야 합니다.

---

## 실행 방법

### 1. 가상환경 + 의존성

**Windows (PowerShell)**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**macOS / Linux**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 자동 거래 발행

```bash
python mock_payment.py            # 2초마다 랜덤 거래 무한 발행 (Ctrl+C 중지)
```

### 3. 시나리오 트리거

```bash
python mock_scenarios.py          # 메뉴에서 대상 유저 + 시나리오 선택
python mock_scenarios.py 2        # 바로 2번(월급 +40만) 실행
```

> 다른 Kafka 주소로 보내려면 `KAFKA_BOOTSTRAP` 환경변수를 지정하세요.

---

## 자동 발행 동작 (`mock_payment.py`)

- 시작 시 DB에서 CREDIT_CARD 카드번호 풀을 로드하고, **카드마다 소비 성향 프로필**을 무작위 배정합니다.
  | 프로필 | 금액 배수 | spike 확률 | 비고 |
  |--------|-----------|-----------|------|
  | 절약형 | 0.7 | 1% | |
  | 일반형 | 1.0 | 2% | |
  | 과소비형 | 1.5 | 3% | |
  | 알림 트리거 테스트용 | 1.0 | 15% | 이상치(spike) 잦음 — ML 알림 테스트용 |
- 매 거래마다 카테고리 → 가맹점 → 금액(가맹점별 범위/고정)을 랜덤 선택하고, 프로필 배수·spike를 적용합니다.
- 카테고리/가맹점/금액 풀은 `CATEGORY_SENDERS` · `SENDER_AMOUNT` 에 정의돼 있습니다 (식비·교통·쇼핑·카페·급여·구독 등 15개 카테고리).

---

## 알림 시나리오 (`mock_scenarios.py`)

실행하면 먼저 **대상 유저**를 고르고(메뉴의 `u` 로 변경), 시나리오를 선택합니다.
기본 대상은 `TEST_EMAIL`(기본 `flowtest@wooriport.com`).

| # | 시나리오 | 동작 |
|---|----------|------|
| 1~3 | 월급 변동 (0 / +40만 / -50만) | `급여` 거래를 자동이체 출발 계좌로 전송 → 월급 입금 알림 (diff = 보낸급여 − user.salary) |
| 4~6 | 챌린지 50 / 80 / 90% | 활성 챌린지 target의 N%까지 소비 거래 전송 → NAG_50/80/90 |
| 7 | 챌린지 성공 | `started_at` 을 8일 전으로 백데이트 → 스케줄러 만료 판정(현재값 ≤ target) → 성공 알림 |
| 8 | 챌린지 실패 (100%+) | target 초과 소비 거래 → 즉시 실패 알림 |

**전제 조건**
- 대상 유저가 `/salary-select`(우리은행 급여통장 지정)·월급 리밸런싱을 완료
- 챌린지 시나리오(4~8)는 해당 유저의 챌린지가 `IN_PROGRESS` 여야 동작(없으면 스킵)

> Redis 진행값은 누적되므로, 정확한 % 테스트는 **새로 시작한(0%) 챌린지**에서 실행하세요.
> 시나리오 7은 `ChallengeScheduler` cron 이 운영용 `0 0 0 * * *` 면 자정까지 안 뜹니다 — 테스트용 `0 * * * * *`(매분)로 바꾸면 1분 내 발생.

---

## 미니챌린지 분류 기준

거래의 `category` · `sender_name` · `transactionAt` 조합으로 챌린지를 판정합니다 (백엔드 분류 기준과 일치).

| 챌린지 | 판정 기준 | 대표 가맹점 |
|--------|-----------|-------------|
| 커피 | `category=카페` | 스타벅스, 투썸플레이스, 빽다방 … |
| 배달 | `category=식비` & sender ∈ 배달앱 | 우아한형제들, 요기요, 쿠팡이츠 |
| 술 | `category=식비` & sender 키워드 `호프`/`주점`/`바` | 을지로호프, 연남동주점, 이태원와인바 |
| 야식 | `category=식비` & `23:00~04:00` | (식비 가맹점, 심야 시각) |
| 점심 | `category=식비` & `11:00~14:00` | (식비 가맹점, 점심 시각) |
| 쇼핑 | `category=쇼핑` | 무신사, 올리브영, 29CM, 지그재그 … |
| 택시 | `category=교통` & sender 키워드 `택시` | 카카오택시, 온다택시, 리본택시 |

> 야식·점심은 **시간대**로 판정하므로, 자동 발행은 실행 시각에만 찍힙니다.
> 특정 시간대 데이터는 시나리오 트리거(`LATE_NIGHT`/`LUNCH` 는 해당 시간대로 거래 시각을 맞춰 전송)를 사용하세요.

---

## 환경변수

| 이름 | 설명 | 기본값 |
|------|------|--------|
| `KAFKA_BOOTSTRAP` | Kafka 브로커 주소 | 로컬: `localhost:9092` / 도커: `host.docker.internal:9092` |
| `DB_HOST` | PostgreSQL 호스트 | 로컬: `localhost` / 도커: `host.docker.internal` |
| `DB_PORT` | PostgreSQL 포트 | `5432` |
| `DB_NAME` | DB 이름 | `wooriport` |
| `DB_USER` | DB 사용자 | `wooriport` |
| `DB_PASSWORD` | DB 비밀번호 | `wooriport1234` |
| `TEST_EMAIL` | 시나리오 대상 유저 | `flowtest@wooriport.com` |

---

## 메시지 스펙

| 필드 | 타입 | 설명 |
|------|------|------|
| `asset_number` | String | 카드/계좌번호 (급여는 자동이체 계좌, 소비는 CREDIT_CARD) |
| `amount` | Long | 거래 금액 |
| `category` | String | 카테고리 (급여, 식비, 교통, 쇼핑, 카페 …) |
| `sender_name` | String | 가맹점/송신자명 |
| `transactionAt` | LocalDateTime | 거래 일시 (ISO-8601, 초 단위) |

**샘플 페이로드**
```json
{
  "asset_number": "5429-4494-5284-1827",
  "amount": 12500,
  "category": "식비",
  "sender_name": "스타벅스 코리아",
  "transactionAt": "2026-05-14T12:34:56"
}
```
