# Mock Transaction Producer

`transactions` 테이블 스키마에 맞춘 더미 거래 이벤트를 Kafka로 발행하는 mock 프로듀서입니다.

- Topic: `transaction-events`
- 발행 주기: 2초에 1건
- 메시지 Key: `asset_number` (같은 카드 거래는 같은 파티션으로 → 순서 보장)

## 사전 준비

- Kafka 브로커가 떠 있어야 합니다 (기본값은 호스트의 `localhost:9092`).
- 토픽 `transaction-events`는 자동 생성되도록 Kafka 설정이 되어 있거나, 미리 만들어 두세요.

---

## 1. Docker로 실행 (권장)

### 빌드

```bash
docker build -t mock-payment .
```

### 실행

**Windows / macOS (Docker Desktop)**

```bash
docker run --rm mock-payment
```

기본값으로 `host.docker.internal:9092`를 바라봅니다. 호스트 머신에 떠 있는 Kafka에 그대로 연결됩니다.

**Linux**

`host.docker.internal`이 기본 지원되지 않으므로 옵션을 하나 추가합니다.

```bash
docker run --rm --add-host=host.docker.internal:host-gateway mock-payment
```

**다른 Kafka 주소로 보내고 싶을 때**

```bash
docker run --rm -e KAFKA_BOOTSTRAP=other-host:9092 mock-payment
```

### 중지

`Ctrl + C`

---

## 2. 로컬(venv)로 실행

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

---

## 메시지 스펙

| 필드 | 타입 | 설명 |
|---|---|---|
| `asset_number` | String | 카드번호 형식 `XXXX-XXXX-XXXX-XXXX` (20개 풀에서 랜덤) |
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

## Troubleshooting

**컨테이너에서 Kafka 접속이 안 됩니다 (`Connection refused` 등)**

호스트 Kafka의 `advertised.listeners`가 `localhost:9092`로만 잡혀 있으면 컨테이너 안에서 접속이 깨집니다. 다음 중 하나를 시도하세요.

- Kafka 설정에 `PLAINTEXT://host.docker.internal:9092`를 advertised listener로 추가
- Kafka도 같은 docker network로 묶어 컨테이너 이름으로 접속
