# Mock Server (Python)

> 실행 순서: **인프라 → 백엔드 → AI서버 → 목서버**
>
> 인프라(Docker)와 백엔드가 먼저 실행된 상태여야 합니다.

`transactions` 테이블 스키마에 맞춘 더미 거래 이벤트를 Kafka로 발행하는 mock 프로듀서입니다.

| 파일 | 역할 |
|---|---|
| `mock_payment.py` | **자동 발행** — 2초에 1건씩 랜덤 거래를 무한 전송 |
| `mock_payment_manual.py` | **수동 발행** — 챌린지 템플릿 / 직접 선택으로 1건씩 골라 전송 |
| `mock_asset_portfolio.py` | AgentService 용 mock FastAPI (`/portfolio/profile` 등) |

- Topic: `transaction-events`
- 메시지 Key: `asset_number` (같은 카드 거래는 같은 파티션으로 → 순서 보장)
- 거래 대상 카드: DB `assets` 테이블의 `asset_type='CREDIT_CARD'` 자산을 시작 시 1회 로드
  - ⚠️ **CREDIT_CARD 자산이 없으면 실행되지 않습니다.** 시드(`backend/sql/dummy_full_flow_test.sql`)로 카드를 만든 뒤 `/linking` 으로 연동하세요. 카드 연동 후에는 mock 을 **재시작**해야 새 카드가 반영됩니다.

## 사전 준비

- 인프라 폴더에서 clone 후 인프라 컨테이너를 실행시켜 주세요.

---

## 1. 로컬(venv)로 실행

### 가상환경 생성 및 의존성 설치

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

### 자동 발행

```bash
python mock_payment.py
```

AgentService 용 mock FastAPI (`/portfolio/profile`, `/portfolio/rebalance`, `/asset-portfolio`) 는 별도 프로세스로:

```bash
uvicorn mock_asset_portfolio:app --host 0.0.0.0 --port 8000 --reload
```

기본 Kafka 주소는 `localhost:9092` 입니다. 다른 주소로 보내려면 환경변수로 지정하세요.

**Windows (PowerShell)**

```powershell
$env:KAFKA_BOOTSTRAP = "other-host:9092"
python mock_payment.py
```

**macOS / Linux**

```bash
KAFKA_BOOTSTRAP=other-host:9092 python mock_payment.py
```

---

## 2. 수동 발행 (`mock_payment_manual.py`)

카테고리 · 결제 시간대까지 직접 골라 한 건씩 보냅니다. 자동 발행으론 만들기 어려운
**야식(심야)·점심** 같은 시간대 의존 데이터를 핀포인트로 쏠 때 유용합니다.

```bash
python mock_payment_manual.py
```

두 가지 모드:

1. **챌린지 템플릿** — 챌린지를 고르면 카테고리·가맹점·시간대를 자동 세팅하고, 원하는 건수만큼 한 번에 전송
2. **직접 선택** — 카테고리 · 가맹점 · 금액 · 결제 시간 · 카드를 일일이 지정

> 가맹점/금액 풀과 전송 로직은 `mock_payment.py` 의 것을 그대로 import 해 재사용합니다(단일 출처).

**실행 예시 (야식 5건)**

```
모드 선택 → 1) 챌린지 템플릿으로 빠르게
🎯 챌린지 선택 → 4) 야식 (식비·23:00~04:00)
몇 건 보낼까요? → 5
💰 금액 → (엔터=가맹점 기본 랜덤)
→ 5건 미리보기 → 전송할까요? [Y/n] → Y
```

---

## 미니챌린지 분류 기준

거래의 `category` · `sender_name` · `transactionAt` 조합으로 챌린지를 판정합니다.
`mock_payment.py` 의 `CATEGORY_SENDERS` 와 `mock_payment_manual.py` 의 `CHALLENGE_TEMPLATES` 가
아래 기준에 맞춰져 있습니다.

| 챌린지 | 판정 기준 | 대표 가맹점 |
|---|---|---|
| 커피 | `category = 카페` | 스타벅스, 투썸플레이스, 빽다방 … |
| 배달 | `category = 식비` & sender ∈ 배달앱 | (주)우아한형제들, 요기요, 쿠팡이츠 |
| 술 | `category = 식비` & sender 키워드 `호프`/`주점`/`바` | 을지로호프, 연남동주점, 이태원와인바 |
| 야식 | `category = 식비` & 시간대 `23:00~04:00` | (식비 가맹점, 심야 시각) |
| 점심 | `category = 식비` & 시간대 `11:00~14:00` | (식비 가맹점, 점심 시각) |
| 쇼핑 | `category = 쇼핑` | 무신사, 올리브영, 29CM, 지그재그 … |
| 택시 | `category = 교통` & sender 키워드 `택시` | 카카오택시, 온다택시, 리본택시 |

> 야식·점심은 **시간대**로 판정하므로, 자동 발행(`mock_payment.py`)은 실행 시각에만 데이터가 찍힙니다.
> 특정 시간대 데이터가 필요하면 **수동 발행의 챌린지 템플릿**을 사용하세요(해당 시간대 안에서 랜덤 시각 생성).

---

## 환경변수

| 이름 | 설명 | 기본값 |
|---|---|---|
| `KAFKA_BOOTSTRAP` | Kafka 브로커 주소 | 로컬 실행: `localhost:9092` / 도커 실행: `host.docker.internal:9092` |
| `DB_HOST` | PostgreSQL 호스트 | 로컬 실행: `localhost` / 도커 실행: `host.docker.internal` |
| `DB_PORT` | PostgreSQL 포트 | `5432` |
| `DB_NAME` | DB 이름 | `wooriport` |
| `DB_USER` | DB 사용자 | `wooriport` |
| `DB_PASSWORD` | DB 비밀번호 | `wooriport1234` |

---

## 메시지 스펙

| 필드 | 타입 | 설명 |
|---|---|---|
| `asset_number` | String | 카드번호 (DB `assets` 테이블의 `asset_type='CREDIT_CARD'` 자산에서 선택) |
| `amount` | Long | 거래 금액 |
| `category` | String | 카테고리 (식비, 교통, 쇼핑, 카페 등) |
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

---
