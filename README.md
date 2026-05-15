# Mock Transaction Producer

`transactions` 테이블 스키마에 맞춘 더미 거래 이벤트를 Kafka로 발행하는 mock 프로듀서입니다.

- Topic: `transaction-events`
- 발행 주기: 2초에 1건
- 메시지 Key: `asset_number` (같은 카드 거래는 같은 파티션으로 → 순서 보장)

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

### 실행

```bash
python mock_payment.py
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
| `asset_number` | String | 카드번호 (DB `assets` 테이블의 `asset_type='CREDIT_CARD'` 자산에서 랜덤 선택) |
| `amount` | Long | 거래 금액 (1,000 ~ 500,000) |
| `category` | String | 카테고리 (식비, 교통, 쇼핑 등) |
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

