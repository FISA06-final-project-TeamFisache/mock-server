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
import sys
from datetime import datetime

import psycopg2

# mock_payment 임포트 시 Kafka Producer + CREDIT_CARD 풀 로드가 함께 수행됨(기존 스크립트와 동일)
from mock_payment import send_transaction

TEST_EMAIL = os.getenv("TEST_EMAIL", "flowtest@wooriport.com")

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
    # 이후 이체 계획 금액·안내 문구(rebalance_comment)는 백엔드 AI 리밸런싱이 생성한다.


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


# ─── 대상 유저 선택 ─────────────────────────────────────
def _list_users():
    """DB의 모든 유저 (email, salary, 우리은행 급여통장 지정 여부)."""
    return _query(
        """SELECT u.email, u.salary, (u.auto_transfer_to_asset_id IS NOT NULL)
             FROM users u
            ORDER BY u.email""")


def _pick_user():
    """DB 유저 목록을 보여주고 대상 유저를 고르게 한다. 선택 시 전역 TEST_EMAIL 갱신."""
    global TEST_EMAIL
    rows = _list_users()
    if not rows:
        print("⚠️  DB에 유저가 없습니다.")
        return
    print("\n=== 대상 유저 선택 ===")
    for i, (email, salary, has_acc) in enumerate(rows, 1):
        cur = " ←현재" if email == TEST_EMAIL else ""
        acc = "급여통장✅" if has_acc else "급여통장❌"
        print(f"  {i}) {email}  (salary {int(salary or 0):,}, {acc}){cur}")
    sel = input(f"유저 번호 (엔터=현재 '{TEST_EMAIL}' 유지): ").strip()
    if not sel:
        return
    if sel.isdigit() and 1 <= int(sel) <= len(rows):
        TEST_EMAIL = rows[int(sel) - 1][0]
        print(f"→ 대상 유저: {TEST_EMAIL}\n")
    else:
        print("잘못된 입력 — 현재 유저 유지\n")


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
    _pick_user()
    while True:
        print(f"[대상 유저: {TEST_EMAIL}]")
        for k, (label, _) in SCENARIOS.items():
            print(f"  {k}) {label}")
        print("  u) 대상 유저 변경")
        print("  0) 종료")
        sel = input("번호 선택 (0=종료): ").strip()
        if sel in ("0", "q", "quit", "exit"):
            print("종료합니다.")
            break
        if sel in ("u", "U"):
            _pick_user()
        elif sel in SCENARIOS:
            run(sel)
        else:
            print("잘못된 입력입니다. 다시 선택하세요.\n")


if __name__ == "__main__":
    main()
