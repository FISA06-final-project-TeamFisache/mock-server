import json
import os
import random
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer

# 카프카 설정 (도커 컨테이너에서는 host.docker.internal로 호스트의 Kafka에 접근)
conf = {'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 전송 실패: {err}")

# 같은 사용자/자산이 여러 번 거래하도록 풀에서 뽑아 사용
USER_IDS = [str(uuid.uuid4()) for _ in range(10)]
ASSET_IDS = [str(uuid.uuid4()) for _ in range(20)]

CATEGORIES = [
    "식비", "교통", "쇼핑", "카페", "문화/여가",
    "의료", "통신", "공과금", "급여", "이체",
]
SENDER_NAMES = [
    "스타벅스 코리아", "쿠팡(주)", "(주)우아한형제들", "넷플릭스", "코레일",
    "이마트", "GS25", "올리브영", "맥도날드", "교보문고",
    "(주)카카오", "배달의민족", "유튜브 프리미엄", "롯데시네마",
]

def generate_transaction():
    """transactions 테이블 스키마에 맞춘 거래 데이터 생성"""
    return {
        "id": str(uuid.uuid4()),                                # UUID, NOT NULL
        "user_id": random.choice(USER_IDS),                     # UUID, NOT NULL
        "asset_id": random.choice(ASSET_IDS),                   # UUID, NOT NULL
        "amount": random.randint(1000, 500000),                 # BIGINT
        "category": random.choice(CATEGORIES),                  # VARCHAR(50)
        "sender_name": random.choice(SENDER_NAMES),             # VARCHAR(100)
        "transactionAt": datetime.now().isoformat(timespec='seconds'),  # LocalDateTime
    }

topic_name = "transaction-events"

print(f"🚀 거래(transactions) 데이터 생성을 시작합니다... (Topic: {topic_name})")
print("중지하려면 Ctrl+C를 누르세요.")

try:
    while True:
        data = generate_transaction()

        producer.produce(
            topic_name,
            key=data['user_id'],  # 같은 사용자의 거래는 같은 파티션 → 순서 보장
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )

        producer.flush()

        # 터미널 확인용 출력 (부하 테스트 시 주석 처리)
        print(f"전송: {data['sender_name']} | {data['amount']}원 | {data['category']}")

        time.sleep(2.0)

except KeyboardInterrupt:
    print("\n🛑 데이터 생성을 중지합니다.")
