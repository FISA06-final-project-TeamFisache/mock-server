"""시나리오 기반 알림 트리거 — SQL로 알림을 꽂는 대신, mock 서버에서 Kafka로
실제 거래를 쏴서 백엔드 파이프라인(컨슈머 → 챌린지/급여 서비스)으로
'진짜' 알림이 생성되게 한다.

시나리오:
  1) 월급 변동 없음        2) 월급 +400,000      3) 월급 -500,000
  4) 챌린지 50%            5) 챌린지 80%          6) 챌린지 90%
  7) 챌린지 성공(스케줄러)  8) 챌린지 실패(100%+)

[급여 1~3]  category="급여" 거래를 '자동이체(출발) 계좌'로 전송.
            → SalaryService.handleIfSalary → generateFromSalary → "월급이 들어왔네요" 알림.
            diff = (보낸 급여) − user.salary.  (TransactionService.persist 입금 부호 수정 반영)
[챌린지 4~6,8] 활성(IN_PROGRESS) 미니챌린지가 있을 때만 동작(없으면 스킵).
            챌린지 subType에 맞는 소비 거래를 target의 N%까지 전송 → NAG_50/80/90, 100%↑면 즉시 실패.
            ※ Redis 진행값은 누적된다. 각 시나리오는 '새로 시작한(0%) 챌린지'에서 실행할 것.
[챌린지 성공 7] 활성 챌린지의 started_at을 8일 전으로 백데이트(현재값 ≤ target 이어야 성공).
            ChallengeScheduler가 만료 판정 → CHALLENGE_COMPLETE 알림.
            운영 cron('0 0 0 * * *')이면 즉시 안 뜸 → 테스트용 '0 * * * * *'(매분)로 바꿔두면 1분 내 발생.

전제: TEST_EMAIL 유저가 /salary-select(우리은행 급여통장 지정)·월급 리밸런싱(portfolios)을 완료했고,
      챌린지 시나리오는 해당 유저의 챌린지가 IN_PROGRESS 여야 함.

실행:  python mock_scenarios.py        (메뉴)
       python mock_scenarios.py 2      (바로 시나리오 2)
환경:  KAFKA_BOOTSTRAP, DB_*(mock_payment.py와 동일), TEST_EMAIL(기본 flowtest@wooriport.com)
"""
import math
import os
import random
import sys
import time
from datetime import datetime

import psycopg2

# mock_payment 임포트 시 Kafka Producer + CREDIT_CARD 풀 로드가 함께 수행됨(기존 스크립트와 동일)
from mock_payment import send_transaction

TEST_EMAIL = os.getenv("TEST_EMAIL", "flowtest@wooriport.com")

# 급여 변동(2·3번) 후 백엔드가 이체 계획을 생성할 때까지 대기할 시간(초)
PLAN_WAIT_SEC = float(os.getenv("PLAN_WAIT_SEC", "3"))

DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "wooriport"),
    "user": os.getenv("DB_USER", "wooriport"),
    "password": os.getenv("DB_PASSWORD", "wooriport1234"),
}


def _query(sql, params=(), fetch="all"):
    """간단 쿼리 헬퍼. fetch: 'all' | 'one' | 'none'(UPDATE 등). with 종료 시 자동 commit."""
    with psycopg2.connect(**DB_CONF) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "none":
            return None
        return cur.fetchall()


# ─── 급여 시나리오 ───────────────────────────────────────
SALARY_SENDER = "(주)카카오"


def _salary_account():
    """(자동이체 출발 계좌번호, user.salary) — 급여는 이 계좌로 들어와야 handleIfSalary가 감지."""
    row = _query(
        """SELECT a.asset_number, u.salary
             FROM users u JOIN assets a ON a.id = u.auto_transfer_to_asset_id
            WHERE u.email = %s""",
        (TEST_EMAIL,), fetch="one")
    if not row or not row[0]:
        raise RuntimeError(
            f"{TEST_EMAIL} 의 자동이체(출발) 계좌가 없습니다. "
            "/salary-select 에서 우리은행 급여통장을 지정하세요.")
    return row[0], int(row[1] or 0)


# 급여 증감 시 Pori 분배 가이드(rebalance_comment)에 넣을 임의 문구
POS_REBALANCE_COMMENTS = [
    "월급이 {amt}원 늘어서 투자·저축 비중을 조금 더 키웠어요!",
    "여유분 {amt}원을 모음·투자 통장에 나눠 담았어요.",
    "늘어난 {amt}원만큼 목표 자금을 더 빠르게 모아봐요!",
]
NEG_REBALANCE_COMMENTS = [
    "월급이 {amt}원 줄어서 일부 이체 금액을 낮췄어요.",
    "{amt}원 감소분만큼 투자·저축을 잠시 줄였어요.",
    "이번 달은 {amt}원 적게 들어와 분배를 조정했어요.",
]


def _user_id():
    row = _query("SELECT id FROM users WHERE email = %s", (TEST_EMAIL,), fetch="one")
    if not row:
        raise RuntimeError(f"{TEST_EMAIL} 유저를 찾을 수 없습니다.")
    return row[0]


def _distribute_plan_deltas(user_id, diff):
    """generateFromSalary 가 baseline 으로 만든 이번 달 미확정 이체 계획 일부에
    증감분(diff)을 임의로 분배한다. → 월급 상세 화면에서 '바뀐 항목만 +/-' 가 보이게."""
    now = datetime.now()
    rows = _query(
        """SELECT id, planned_amount FROM transfer_plans
            WHERE user_id = %s AND year = %s AND month = %s
              AND is_confirmed = false AND deleted_at IS NULL
            ORDER BY created_at""",
        (user_id, now.year, now.month))
    if not rows:
        print("  ⚠️  이번 달 미확정 이체 계획이 없어 증감분을 못 넣었어요 "
              "(generateFromSalary 가 아직 안 돌았거나 포트폴리오 미설정).")
        return
    # 임의로 일부 계획에만 분배(나머지는 그대로) → '바뀐 항목만 +' UI 가 드러나게
    k = random.randint(1, len(rows))
    targets = random.sample(rows, k)
    base = diff // k
    deltas = [base] * k
    deltas[-1] += diff - base * k        # 나머지 보정 → 합계 = diff
    applied = []
    for (plan_id, planned), d in zip(targets, deltas):
        new_amount = max(0, int(planned) + int(d))
        _query("UPDATE transfer_plans SET planned_amount = %s WHERE id = %s",
               (new_amount, plan_id), fetch="none")
        applied.append(new_amount - int(planned))   # 0 클램프 반영된 실제 변동
    print(f"  → 이체 계획 {len(rows)}건 중 {k}건에 증감분 분배: "
          f"{', '.join(f'{a:+,}' for a in applied)} (합계 {sum(applied):+,}원)")

    # Pori 분배 가이드 문구(rebalance_comment) 임의 설정 — getTransferPlans 는 plans.get(0)
    # 의 값을 읽으므로, 이번 달 미확정 plans 전체에 동일 문구를 넣어 어느 행이 첫 행이든 잡히게 함
    pool = POS_REBALANCE_COMMENTS if diff > 0 else NEG_REBALANCE_COMMENTS
    comment = random.choice(pool).format(amt=f"{abs(diff):,}")
    _query(
        """UPDATE transfer_plans SET rebalance_comment = %s
            WHERE user_id = %s AND year = %s AND month = %s
              AND is_confirmed = false AND deleted_at IS NULL""",
        (comment, user_id, now.year, now.month), fetch="none")
    print(f"  → rebalance_comment 설정: \"{comment}\"")


def scenario_salary(diff):
    acc, base = _salary_account()
    amount = base + diff
    if amount < 0:
        print(f"⚠️  보낼 급여({amount:,})가 음수가 됩니다. user.salary({base:,}) 확인 필요 — 스킵")
        return
    data = {
        "asset_number": acc,
        "amount": int(amount),
        "category": "급여",
        "sender_name": SALARY_SENDER,
        "transactionAt": datetime.now().isoformat(timespec="seconds"),
    }
    send_transaction(data)
    print(f"  user.salary(기준)={base:,} / 보낸 급여={int(amount):,} / 예상 diff={int(amount) - base:+,}")

    # 변동(2·3번)일 때만: 백엔드가 baseline 계획을 만든 뒤 증감분을 임의 분배
    if diff != 0:
        print(f"  ⏳ 백엔드 이체 계획 생성 대기 {PLAN_WAIT_SEC}s ...")
        time.sleep(PLAN_WAIT_SEC)
        _distribute_plan_deltas(_user_id(), diff)


# ─── 챌린지 시나리오 ─────────────────────────────────────
# subType → (category, sender, hour) : ChallengeService.matchesChallenge 기준에 맞춤
SUBTYPE_TX = {
    "COFFEE":     ("카페", "스타벅스 코리아", None),
    "DELIVERY":   ("식비", "쿠팡이츠",       None),   # DELIVERY_SENDERS
    "ALCOHOL":    ("식비", "을지로호프",     None),   # sender contains '호프'
    "LATE_NIGHT": ("식비", "맥도날드",       2),      # 23~04시
    "LUNCH":      ("식비", "맥도날드",       12),     # 11~14시
    "SHOPPING":   ("쇼핑", "올리브영",       None),   # SHOPPING_SENDERS
    "TAXI":       ("교통", "카카오택시",     None),   # sender contains '택시'
}


def _active_challenge():
    row = _query(
        """SELECT c.id, c.challenge_sub_type, c.challenge_type, c.target, c.current_value, c.user_id
             FROM mini_challenges c JOIN users u ON u.id = c.user_id
            WHERE c.status = 'IN_PROGRESS' AND u.email = %s
            ORDER BY c.started_at DESC LIMIT 1""",
        (TEST_EMAIL,), fetch="one")
    if not row:
        return None
    return {"id": row[0], "sub_type": row[1], "type": row[2],
            "target": int(row[3] or 0), "current": int(row[4] or 0), "user_id": row[5]}


def _user_card(user_id):
    row = _query(
        "SELECT asset_number FROM assets "
        "WHERE user_id = %s AND asset_type = 'CREDIT_CARD' AND asset_number IS NOT NULL LIMIT 1",
        (user_id,), fetch="one")
    if not row:
        raise RuntimeError("해당 유저의 CREDIT_CARD 계좌가 없습니다. (소비 거래 전송 불가)")
    return row[0]


def _challenge_tx(card, sub_type, amount):
    category, sender, hour = SUBTYPE_TX[sub_type]
    dt = datetime.now()
    if hour is not None:
        dt = dt.replace(hour=hour, minute=30, second=0)
    return {
        "asset_number": card,
        "amount": int(amount),
        "category": category,
        "sender_name": sender,
        "transactionAt": dt.isoformat(timespec="seconds"),
    }


def scenario_challenge(pct):
    ch = _active_challenge()
    if not ch:
        print("⚠️  활성(IN_PROGRESS) 미니챌린지가 없습니다. 앱에서 챌린지를 먼저 시작(승인)하세요. — 스킵")
        return
    if ch["sub_type"] not in SUBTYPE_TX:
        print(f"⚠️  알 수 없는 subType: {ch['sub_type']} — 스킵")
        return
    card = _user_card(ch["user_id"])
    target = ch["target"]
    print(f"  활성 챌린지: subType={ch['sub_type']} type={ch['type']} "
          f"target={target:,} current(DB)={ch['current']:,}")
    if target <= 0:
        print("⚠️  target 이 0 이하 — 스킵")
        return
    if ch["current"] > 0:
        print("  ⚠️  주의: 이미 진행값(Redis 누적)이 있을 수 있어요. 정확한 % 테스트는 새 챌린지에서.")

    if ch["type"] == "AMOUNT":
        need = math.ceil(target * pct / 100)        # 0%에서 시작 가정
        send_transaction(_challenge_tx(card, ch["sub_type"], need))
        print(f"  → {need:,}원 소비 거래 전송 (≈{pct}%)")
    else:  # COUNT
        n = max(1, math.ceil(target * pct / 100))
        for _ in range(n):
            send_transaction(_challenge_tx(card, ch["sub_type"], 5000))
        print(f"  → {n}건 소비 거래 전송 (≈{pct}%)")


def scenario_challenge_success():
    ch = _active_challenge()
    if not ch:
        print("⚠️  활성(IN_PROGRESS) 미니챌린지가 없습니다. — 스킵")
        return
    if ch["target"] > 0 and ch["current"] > ch["target"]:
        print(f"⚠️  current({ch['current']:,}) > target({ch['target']:,}) → 성공 조건(현재≤target) 불충족. "
              "새 챌린지(또는 진행값 낮은 챌린지)로 실행하세요. — 스킵")
        return
    _query("UPDATE mini_challenges SET started_at = now() - interval '8 days' WHERE id = %s",
           (ch["id"],), fetch="none")
    print("  ✓ started_at 을 8일 전으로 백데이트했어요 (만료 대상).")
    print("  → ChallengeScheduler 가 만료 판정(현재값 ≤ target = 성공) 시 CHALLENGE_COMPLETE 알림 발송.")
    print("    ※ ChallengeScheduler 의 @Scheduled cron 이 운영용 '0 0 0 * * *' 면 자정까지 안 떠요.")
    print("      테스트용 '0 * * * * *'(매분) 로 바꿔두면 1분 내에 성공 알림이 생성돼요.")


# ─── 메뉴 ───────────────────────────────────────────────
SCENARIOS = {
    "1": ("월급 변동 없음 (diff 0)",   lambda: scenario_salary(0)),
    "2": ("월급 +400,000",            lambda: scenario_salary(400_000)),
    "3": ("월급 -500,000",            lambda: scenario_salary(-500_000)),
    "4": ("챌린지 50%",               lambda: scenario_challenge(50)),
    "5": ("챌린지 80%",               lambda: scenario_challenge(80)),
    "6": ("챌린지 90%",               lambda: scenario_challenge(90)),
    "7": ("챌린지 성공 (스케줄러)",    scenario_challenge_success),
    "8": ("챌린지 실패 (100%+)",      lambda: scenario_challenge(110)),
}


def run(key):
    name, fn = SCENARIOS[key]
    print(f"▶ [{key}] {name}  (대상 유저: {TEST_EMAIL})")
    fn()
    print("완료 — 백엔드 컨슈머 로그 / 대시보드 알림을 확인하세요.\n")


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg in SCENARIOS:
        run(arg)
        return
    print("=== 알림 시나리오 트리거 ===")
    while True:
        for k, (label, _) in SCENARIOS.items():
            print(f"  {k}) {label}")
        print("  0) 종료")
        sel = input("번호 선택 (0=종료): ").strip()
        if sel in ("0", "q", "quit", "exit"):
            print("종료합니다.")
            break
        if sel in SCENARIOS:
            run(sel)
        else:
            print("잘못된 입력입니다. 다시 선택하세요.\n")


if __name__ == "__main__":
    main()
