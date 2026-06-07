import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer
import psycopg2

# 카프카 설정 (도커 컨테이너에서는 host.docker.internal로 호스트의 Kafka에 접근)
conf = {'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 전송 실패: {err}")

def load_credit_card_asset_numbers():
    """assets 테이블에서 asset_type='CREDIT_CARD' 인 자산의 asset_number 목록 로드"""
    db_conf = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "wooriport"),
        "user": os.getenv("DB_USER", "wooriport"),
        "password": os.getenv("DB_PASSWORD", "wooriport1234"),
    }
    with psycopg2.connect(**db_conf) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT asset_number FROM assets "
            "WHERE asset_type = 'CREDIT_CARD' AND asset_number IS NOT NULL"
        )
        return [row[0] for row in cur.fetchall()]

# 시작 시점에 CREDIT_CARD 카드번호 풀을 DB에서 1회 로드하여 사용
ASSET_NUMBERS = load_credit_card_asset_numbers()
if not ASSET_NUMBERS:
    raise RuntimeError("CREDIT_CARD 자산이 없어 거래 데이터를 생성할 수 없습니다.")
print(f"💳 CREDIT_CARD {len(ASSET_NUMBERS)}건 로드 완료")

# 미니챌린지 감지를 위한 sender 키워드 (백엔드 분류 기준과 맞춤)
#   커피      = category '카페'
#   배달      = category '식비' & sender in DELIVERY_SENDERS (우아한형제들/요기요/쿠팡이츠)
#   술        = category '식비' & sender 키워드 '바'/'주점'/'호프'
#   야식      = category '식비' & 시간대 23:00~04:00            (※ transactionAt 시간 필요)
#   점심      = category '식비' & 시간대 11:00~14:00            (※ transactionAt 시간 필요)
#   쇼핑      = category '쇼핑' & sender 키워드 '올리브영'/'무신사'/'29CM'/'지그재그'/'에이블리'/'나이키'/'자라'
#   택시      = category '교통' & sender 키워드 '택시'
DELIVERY_SENDERS = ["(주)우아한형제들", "요기요", "쿠팡이츠"]

# 카테고리 → 가맹점 매핑. 가맹점은 반드시 해당 카테고리에서만 뽑힘.
CATEGORY_SENDERS = {
    "식비":     ["맥도날드", "롯데리아", "버거킹", "GS25", "CU편의점", "이마트24", "본죽",
                "(주)우아한형제들", "요기요", "쿠팡이츠",          # 배달 챌린지
                "을지로호프", "연남동주점", "이태원와인바"],       # 술 챌린지 (호프/주점/바)
    "교통":     ["코레일", "서울교통공사", "카카오모빌리티", "티머니", "제주항공", "대한항공",
                "카카오택시", "온다택시", "리본택시"],            # 택시 챌린지
    "쇼핑":     ["쿠팡(주)", "이마트", "롯데마트", "홈플러스", "올리브영", "무신사", "11번가",
                "29CM", "지그재그", "에이블리", "G마켓", "옥션", "SSG닷컴", "컬리",
                "다이소", "ABC마트", "유니클로", "나이키", "자라"],
    "카페":     ["스타벅스 코리아", "투썸플레이스", "이디야커피", "빽다방", "메가MGC커피", "할리스"],
    "문화/여가": ["CGV", "롯데시네마", "교보문고", "카카오게임즈"],
    "의료":     ["서울아산병원", "세브란스병원", "올리브영약국", "GC녹십자약국", "강남성심병원"],
    "통신":     ["SKT", "KT", "LG유플러스"],
    "공과금":   ["한국전력공사", "한국도시가스", "서울시상수도사업본부"],
    "급여":     ["(주)카카오", "삼성전자(주)", "현대자동차(주)", "네이버(주)"],
    "이체":     ["카카오뱅크", "토스(비바리퍼블리카)", "NH농협은행", "신한은행"],
    "주거":     ["아파트관리비", "월세", "이사코리아", "홈닥터서비스"],
    "보험":     ["삼성생명", "한화생명", "DB손해보험", "현대해상", "KB손해보험", "메리츠화재"],
    "교육":     ["클래스101", "에듀윌", "메가스터디", "야나두", "영단기", "해커스"],
    "구독":     ["넷플릭스", "유튜브 프리미엄", "멜론", "왓챠", "웨이브", "티빙", "쿠팡로켓와우", "애플뮤직"],
}

# 가맹점별 결제 금액. (min, max) 튜플이면 범위 내 랜덤, 정수면 고정 금액(구독료 등).
SENDER_AMOUNT = {
    "맥도날드":             (5000,   25000),
    "롯데리아":             (4000,   18000),
    "버거킹":               (6000,   25000),
    "(주)우아한형제들":     (12000,  60000),
    "요기요":               (12000,  50000),  # 배달
    "쿠팡이츠":             (12000,  50000),  # 배달
    "을지로호프":           (15000,  80000),  # 술
    "연남동주점":           (20000, 100000),  # 술
    "이태원와인바":         (20000,  90000),  # 술
    "GS25":                 (1000,   15000),
    "CU편의점":             (1000,   15000),
    "이마트24":             (1000,   20000),
    "본죽":                 (6000,   12000),
    "코레일":               (2800,   80000),
    "서울교통공사":         (1400,    1400),  # 기본 요금 고정
    "카카오모빌리티":       (3800,   30000),
    "티머니":               (1400,    5000),
    "카카오택시":           (3800,   30000),  # 택시
    "온다택시":             (3800,   30000),  # 택시
    "리본택시":             (3800,   28000),  # 택시
    "제주항공":             (50000, 200000),
    "대한항공":             (80000, 500000),
    "쿠팡(주)":             (10000, 300000),
    "이마트":               (15000, 200000),
    "롯데마트":             (15000, 150000),
    "홈플러스":             (15000, 150000),
    "올리브영":             (10000,  80000),
    "무신사":               (20000, 200000),
    "11번가":               (10000, 200000),
    "29CM":                 (15000, 150000),
    "지그재그":             (10000, 100000),
    "에이블리":             (10000,  80000),
    "G마켓":                (10000, 200000),
    "옥션":                 (10000, 150000),
    "SSG닷컴":              (15000, 250000),
    "컬리":                 (15000, 100000),
    "다이소":               (1000,   30000),
    "W컨셉":                (20000, 200000),
    "ABC마트":              (30000, 150000),
    "유니클로":             (15000, 120000),
    "나이키":               (40000, 250000),
    "자라":                 (20000, 150000),
    "스타벅스 코리아":      (4500,   15000),
    "투썸플레이스":         (4000,   14000),
    "이디야커피":           (3000,   10000),
    "빽다방":               (2000,    8000),
    "메가MGC커피":          (2000,    8000),
    "할리스":               (4000,   12000),
    "CGV":                  (12000,  20000),
    "롯데시네마":           (12000,  20000),
    "교보문고":             (10000,  50000),
    "카카오게임즈":         (1000,  100000),
    "서울아산병원":         (5000,  500000),
    "세브란스병원":         (5000,  400000),
    "올리브영약국":         (3000,   50000),
    "GC녹십자약국":         (3000,   50000),
    "강남성심병원":         (5000,  200000),
    "SKT":                  (30000,  80000),
    "KT":                   (30000,  80000),
    "LG유플러스":           (25000,  70000),
    "한국전력공사":         (30000, 200000),
    "한국도시가스":         (20000, 150000),
    "서울시상수도사업본부": (10000,  50000),
    "(주)카카오":         (2000000, 6000000),
    "삼성전자(주)":       (3000000, 8000000),
    "현대자동차(주)":     (2500000, 7000000),
    "네이버(주)":         (2500000, 7000000),
    "카카오뱅크":           (10000, 1000000),
    "토스(비바리퍼블리카)": (10000,  500000),
    "NH농협은행":           (10000, 1000000),
    "신한은행":             (10000, 1000000),
    "아파트관리비":        (150000, 400000),
    "월세":                (300000, 1500000),
    "이사코리아":          (300000, 1500000),
    "홈닥터서비스":         (50000, 200000),
    "삼성생명":             (30000, 300000),
    "한화생명":             (30000, 300000),
    "DB손해보험":           (20000, 200000),
    "현대해상":             (20000, 200000),
    "KB손해보험":           (20000, 200000),
    "메리츠화재":           (15000, 150000),
    "클래스101":            (10000, 100000),
    "에듀윌":               (30000, 300000),
    "메가스터디":           (50000, 500000),
    "야나두":               (15000,  50000),
    "영단기":               (30000, 200000),
    "해커스":               (30000, 300000),
    "넷플릭스":             13500,
    "유튜브 프리미엄":      10450,
    "멜론":                  7900,
    "왓챠":                  7900,
    "웨이브":                7900,
    "티빙":                 10900,
    "쿠팡로켓와우":          7890,
    "애플뮤직":              8900,
}

# 카드별 소비 성향을 4가지 타입 중 하나로 자동 배정. ML 알림 트리거 테스트용 spike 잦음 타입 포함.
_PROFILE_TYPES = [
    {"amount_multiplier": 0.7, "spike_prob": 0.01, "spike_scale": 3.0},  # 절약형
    {"amount_multiplier": 1.0, "spike_prob": 0.02, "spike_scale": 5.0},  # 일반형
    {"amount_multiplier": 1.5, "spike_prob": 0.03, "spike_scale": 4.0},  # 과소비형
    {"amount_multiplier": 1.0, "spike_prob": 0.15, "spike_scale": 8.0},  # 알림 트리거 테스트용
]

CARD_PROFILES = {card: random.choice(_PROFILE_TYPES) for card in ASSET_NUMBERS}
DEFAULT_PROFILE = {"amount_multiplier": 1.0, "spike_prob": 0.02, "spike_scale": 5.0}

def _pick_amount(sender_name: str) -> int:
    spec = SENDER_AMOUNT[sender_name]
    if isinstance(spec, tuple):
        return random.randint(spec[0], spec[1])
    return int(spec)

def generate_transaction():
    """카프카 적재용 거래 데이터 생성"""
    category = random.choice(list(CATEGORY_SENDERS.keys()))
    sender_name = random.choice(CATEGORY_SENDERS[category])

    asset_number = random.choice(ASSET_NUMBERS)
    profile = CARD_PROFILES.get(asset_number, DEFAULT_PROFILE)

    multiplier = profile["amount_multiplier"]
    if random.random() < profile["spike_prob"]:
        multiplier *= profile["spike_scale"]

    base_amount = _pick_amount(sender_name)
    amount = int(base_amount * multiplier)

    return {
        "event_id": str(uuid.uuid4()),                                  # PG 거래고유번호 = 멱등키
        "asset_number": asset_number,                                   # 카드번호 (XXXX-XXXX-XXXX-XXXX)
        "amount": amount,                                               # BIGINT
        "category": category,                                           # VARCHAR(50)
        "sender_name": sender_name,                                     # VARCHAR(100)
        "transactionAt": datetime.now().isoformat(timespec='seconds'),  # LocalDateTime
        "producedAt": int(time.time() * 1000),                          # 발행 epoch ms (E2E 지연 측정)
    }

topic_name = "transaction-events"


def _produce(data: dict):
    """카프카 produce 1건. 내부 큐가 가득 차면 poll 로 비우고 재시도."""
    payload = json.dumps(data).encode('utf-8')
    while True:
        try:
            producer.produce(
                topic_name,
                key=data['asset_number'],  # 같은 카드 거래는 같은 파티션 → 순서 보장
                value=payload,
                callback=delivery_report,
            )
            return
        except BufferError:
            producer.poll(0.1)  # 큐 비울 때까지 콜백 처리 후 재시도


def send_transaction(data: dict, flush: bool = True, verbose: bool = True):
    """단일 거래를 카프카로 전송 (수동 전송 스크립트에서도 재사용)."""
    _produce(data)
    if flush:
        producer.flush()
    if verbose:
        print(f"전송: {data['sender_name']} | {data['amount']}원 | {data['category']} | {data['transactionAt']} | {data['asset_number']}")


def run_burst(count: int):
    """count 건을 sleep 없이 최대속도로 발행 (성능 측정용). 끝에 flush 1회."""
    print(f"🚀 버스트 모드: {count}건 발행 시작 (Topic: {topic_name})")
    start = time.time()
    for i in range(count):
        _produce(generate_transaction())
        if i % 5000 == 0:
            producer.poll(0)            # 콜백 주기적 처리
            print(f"  produce {i}/{count}")
    producer.flush()                    # 전량 전송 보장
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"✅ 발행 완료: {count}건 / {elapsed:.2f}s / {rate:,.0f} msg/s (produce 기준)")


def run_continuous(rate: float):
    """rate 건/초로 지속 발행 (기존 동작). rate<=0 이면 최대속도."""
    print(f"🚀 거래 데이터 생성을 시작합니다... (Topic: {topic_name}, rate={rate}/s)")
    print("중지하려면 Ctrl+C를 누르세요.")
    interval = 1.0 / rate if rate > 0 else 0
    try:
        while True:
            send_transaction(generate_transaction())
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 데이터 생성을 중지합니다.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transaction-events 발행기")
    parser.add_argument("--count", type=int, default=0,
                        help="발행할 총 건수. 0이면 지속 발행 모드(기존 동작)")
    parser.add_argument("--rate", type=float, default=0.5,
                        help="지속 발행 모드 초당 건수. 0이면 최대속도. (--count 지정 시 무시)")
    args = parser.parse_args()

    if args.count > 0:
        run_burst(args.count)
    else:
        run_continuous(args.rate)
