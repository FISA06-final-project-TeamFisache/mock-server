"""
Phase 1 결함 복구 라이브 데모용 단건 이벤트 발행기.
사용: python send_demo_event.py <asset_number> <amount> <category> [event_id]
  amount 양수 = 입금(급여 감지용), 음수/양수 무관하게 적재는 음수로 저장됨.
예)
  # C1 챌린지 재계산: 식비 점심 거래
  python send_demo_event.py 8888-0000-0000-6666 12500 식비
  # D1 멱등: 같은 event_id 2회
  python send_demo_event.py 5429-4494-5284-1111 9000 식비 DUP-TEST-1
  python send_demo_event.py 5429-4494-5284-1111 9000 식비 DUP-TEST-1
  # S2 급여 DLT: 자동이체 계좌로 급여 입금
  python send_demo_event.py 7777-0000-0000-5555 3000000 급여
"""
import json
import sys
import time
import uuid
from confluent_kafka import Producer

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TOPIC = "transaction-events"
producer = Producer({"bootstrap.servers": "localhost:9092"})


def send(asset_number, amount, category, event_id=None):
    data = {
        "event_id": event_id or str(uuid.uuid4()),
        "asset_number": asset_number,
        "amount": int(amount),
        "category": category,
        "sender_name": "데모가맹점",
        "transactionAt": "2026-05-14T12:34:56",  # 12:34 → LUNCH 매칭
        "producedAt": int(time.time() * 1000),
    }
    producer.produce(TOPIC, key=asset_number, value=json.dumps(data).encode("utf-8"))
    producer.flush()
    print(f"sent event_id={data['event_id']} asset={asset_number} amount={amount} category={category}")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    send(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else None)
