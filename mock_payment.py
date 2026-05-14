import json
import os
import random
import time
import string
from datetime import datetime
from confluent_kafka import Producer

# 카프카 설정 (도커 컨테이너에서는 host.docker.internal로 호스트의 Kafka에 접근)
conf = {'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 전송 실패: {err}")

def generate_card_id():
    """카드 식별자 PK 생성 (CARD + 28자리 영숫자)"""
    random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=28))
    return f"CARD{random_str}"

def generate_merchant_regno():
    """가맹점 사업자번호 생성 (NS 12자리: XXX-XX-XXXXX)"""
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(10000,99999)}"

def generate_mydata_approval():
    """금융분야 마이데이터 카드-008 (국내 승인내역) 규격에 맞춘 데이터 생성"""
    
    # 1. 취소 여부 (약 2% 확률로 true)
    is_canceled = random.random() < 0.02
    
    # 2. 승인 구분 및 할부 개월 수 (01:일시불 70%, 02:할부 20%, 기타 10%)
    approved_type = random.choices(['01', '02', '03', '04'], weights=[70, 20, 5, 5])[0]
    installment_term = None
    if approved_type == '02':
        installment_term = random.choice([3, 6, 12]) # N(2)
        
    merchants = ["스타벅스 코리아", "쿠팡(주)", "(주)우아한형제들", "넷플릭스", "코레일", "이마트"]

    # 카드-008 승인내역 JSON 구조
    payload = {
        "card_id": generate_card_id(),
        "approved_dtime": datetime.now().strftime("%Y%m%d%H%M%S"),  # DTIME
        "approved_num": f"{random.randint(10**14, (10**15)-1)}",    # aN(15)
        "approved_amt": float(random.randint(1000, 500000)),        # F(18,3) - 파이썬 float 처리
        "approved_type": approved_type,                             # aN(2)
        "merchant_name": random.choice(merchants),                  # AH(100)
        "merchant_regno": generate_merchant_regno(),                # NS(12)
        "is_canceled": is_canceled                                  # Boolean
    }
    
    # 할부일 경우에만 할부개월수 포함
    if installment_term is not None:
        payload["installment_term"] = installment_term
        
    return payload

topic_name = "card-approval-events" # 토픽 이름도 명세에 맞게 의미있게 변경

print(f"🚀 마이데이터 표준 API(v251001) 기반 데이터 생성을 시작합니다... (Topic: {topic_name})")
print("중지하려면 Ctrl+C를 누르세요.")

try:
    while True:
        data = generate_mydata_approval()
        
        producer.produce(
            topic_name, 
            key=data['card_id'], # PK를 Key로 사용하여 파티셔닝 순서 보장
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.flush()
        
        # 터미널에서 데이터 확인용 출력 (부하 테스트 시에는 주석 처리하세요)
        print(f"전송: {data['merchant_name']} | {data['approved_amt']}원 | 유형:{data['approved_type']}")
        
        time.sleep(2.0) # 0.5초마다 1건씩 전송

except KeyboardInterrupt:
    print("\n🛑 데이터 생성을 중지합니다.")