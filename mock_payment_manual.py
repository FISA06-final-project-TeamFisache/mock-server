"""수동 거래 전송 스크립트.

두 가지 방식으로 한 건씩 카프카로 전송합니다.
  1) 챌린지 템플릿 — 커피/배달/술/야식/점심/쇼핑/택시를 고르면 카테고리·가맹점·시간대를 자동 세팅
  2) 직접 선택     — 카테고리·가맹점·금액·시간·카드를 일일이 지정

가맹점/금액 풀과 전송 로직은 mock_payment.py 것을 그대로 재사용합니다.

실행:  python mock_payment_manual.py
"""
import random
from datetime import datetime

from mock_payment import (
    CATEGORY_SENDERS,
    SENDER_AMOUNT,
    ASSET_NUMBERS,
    DELIVERY_SENDERS,
    _pick_amount,
    send_transaction,
)

# ── 챌린지별 가맹점 풀 ──────────────────────────────────
BAR_SENDERS   = ["을지로호프", "연남동주점", "이태원와인바"]          # 술 (호프/주점/바)
TAXI_SENDERS  = ["카카오택시", "온다택시", "리본택시"]                # 택시
NIGHT_SENDERS = DELIVERY_SENDERS + ["맥도날드", "롯데리아", "GS25", "CU편의점"]   # 야식
LUNCH_SENDERS = ["맥도날드", "롯데리아", "버거킹", "본죽"] + DELIVERY_SENDERS     # 점심

# ── 미니챌린지 템플릿 (백엔드 분류 기준과 맞춤) ──────────
#   senders=None → 해당 category 의 전체 가맹점에서 랜덤
#   window=(시작시, 끝시) → 그 시간대 안에서 랜덤 시각 (자정 넘김 자동 처리), None → 지금 시각
CHALLENGE_TEMPLATES = [
    {"name": "커피", "desc": "카페",                "category": "카페", "senders": None,             "window": None},
    {"name": "배달", "desc": "식비·배달앱",         "category": "식비", "senders": DELIVERY_SENDERS, "window": None},
    {"name": "술",   "desc": "식비·호프/주점/바",   "category": "식비", "senders": BAR_SENDERS,      "window": None},
    {"name": "야식", "desc": "식비·23:00~04:00",    "category": "식비", "senders": NIGHT_SENDERS,    "window": (23, 4)},
    {"name": "점심", "desc": "식비·11:00~14:00",    "category": "식비", "senders": LUNCH_SENDERS,    "window": (11, 14)},
    {"name": "쇼핑", "desc": "쇼핑",                "category": "쇼핑", "senders": None,             "window": None},
    {"name": "택시", "desc": "교통·택시",           "category": "교통", "senders": TAXI_SENDERS,     "window": None},
]


def _choose(title, options, allow_custom=True, default=None):
    """번호로 옵션 선택. 엔터=default(있을 때), 'd'=직접 입력."""
    print(f"\n{title}")
    for i, opt in enumerate(options, 1):
        print(f"  {i:>2}) {opt}")
    if allow_custom:
        print("   d) 직접 입력")
    hint = f" [엔터={default}]" if default is not None else ""
    while True:
        raw = input(f"선택{hint}: ").strip()
        if raw == "" and default is not None:
            return default
        if allow_custom and raw.lower() == "d":
            v = input("값 직접 입력: ").strip()
            if v:
                return v
            continue
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print("⚠️  올바른 번호를 입력하세요.")


def _amount_for(sender, fixed=None):
    """fixed 가 주어지면 그 값, 아니면 가맹점 기본 범위 랜덤."""
    if fixed is not None:
        return fixed
    return _pick_amount(sender) if sender in SENDER_AMOUNT else 10000


def _ask_fixed_amount():
    """금액 입력. 엔터=가맹점 기본 랜덤(None 반환), 숫자=고정."""
    raw = input("💰 금액 [엔터=가맹점 기본 랜덤 / 숫자=고정]: ").strip().replace(",", "")
    if raw == "":
        return None
    if raw.isdigit():
        return int(raw)
    print("⚠️  숫자가 아니라 기본 랜덤 사용")
    return None


def _random_dt_in_window(start_h, end_h):
    """[start_h, end_h] 시간대 안에서 오늘 날짜 기준 랜덤 시각. start>end 면 자정을 넘김."""
    if start_h <= end_h:
        hours = list(range(start_h, end_h + 1))
    else:
        hours = list(range(start_h, 24)) + list(range(0, end_h + 1))
    now = datetime.now()
    return now.replace(hour=random.choice(hours),
                       minute=random.randint(0, 59),
                       second=random.randint(0, 59))


def _ask_datetime():
    """결제 시간 입력. 엔터=지금, 'YYYY-MM-DD HH:MM' 또는 'HH:MM'(오늘) 지원."""
    raw = input(
        "🕒 결제 시간 [예: 2026-06-04 23:30 / 23:30(오늘) / 엔터=지금]: "
    ).strip()
    if raw == "":
        return datetime.now()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%H:%M":
                now = datetime.now()
                dt = dt.replace(year=now.year, month=now.month, day=now.day)
            return dt
        except ValueError:
            continue
    print("⚠️  형식 인식 실패 → 지금 시각 사용")
    return datetime.now()


def _make(category, sender, dt, amount):
    return {
        "asset_number": random.choice(ASSET_NUMBERS),
        "amount": amount,
        "category": category,
        "sender_name": sender,
        "transactionAt": dt.isoformat(timespec="seconds"),
    }


def _print_data(data):
    print(f"  → {data['category']:<6} | {data['sender_name']:<14} | "
          f"{data['amount']:>9,}원 | {data['transactionAt']} | {data['asset_number']}")


# ── 1) 챌린지 템플릿 모드 ───────────────────────────────
def run_template_mode():
    labels = [f"{t['name']}  ({t['desc']})" for t in CHALLENGE_TEMPLATES]
    pick = _choose("🎯 챌린지 선택", labels, allow_custom=False, default=labels[0])
    tpl = CHALLENGE_TEMPLATES[labels.index(pick)]

    raw = input("\n몇 건 보낼까요? [엔터=1]: ").strip()
    count = int(raw) if raw.isdigit() and int(raw) > 0 else 1
    fixed = _ask_fixed_amount()

    pool = tpl["senders"] or CATEGORY_SENDERS.get(tpl["category"], [])
    print(f"\n── '{tpl['name']}' {count}건 ──────────────")
    batch = []
    for _ in range(count):
        sender = random.choice(pool)
        dt = _random_dt_in_window(*tpl["window"]) if tpl["window"] else datetime.now()
        batch.append(_make(tpl["category"], sender, dt, _amount_for(sender, fixed)))

    for d in batch:
        _print_data(d)

    if input("\n전송할까요? [Y/n]: ").strip().lower() in ("", "y"):
        for d in batch:
            send_transaction(d)
        print(f"✅ {len(batch)}건 전송 완료")
    else:
        print("↩️  취소했습니다.")


# ── 2) 직접 선택 모드 ───────────────────────────────────
def run_manual_mode():
    category = _choose("📂 카테고리 선택", list(CATEGORY_SENDERS.keys()), allow_custom=True)
    senders = CATEGORY_SENDERS.get(category, [])
    sender = _choose("🏪 가맹점 선택", senders, allow_custom=True,
                     default=(senders[0] if senders else None))
    fixed = _ask_fixed_amount()
    amount = _amount_for(sender, fixed)
    dt = _ask_datetime()
    asset = _choose("💳 카드 선택", ASSET_NUMBERS, allow_custom=False,
                    default=random.choice(ASSET_NUMBERS))

    data = {
        "asset_number": asset,
        "amount": amount,
        "category": category,
        "sender_name": sender,
        "transactionAt": dt.isoformat(timespec="seconds"),
    }
    print("\n── 보낼 거래 ──────────────")
    _print_data(data)

    if input("\n전송할까요? [Y/n]: ").strip().lower() in ("", "y"):
        send_transaction(data)
        print("✅ 전송 완료")
    else:
        print("↩️  취소했습니다.")


def main():
    print("=== 수동 거래 전송 ===  (Ctrl+C 로 종료)")
    try:
        while True:
            mode = _choose("모드 선택", ["챌린지 템플릿으로 빠르게", "직접 하나씩 고르기"],
                           allow_custom=False, default="챌린지 템플릿으로 빠르게")
            if mode.startswith("챌린지"):
                run_template_mode()
            else:
                run_manual_mode()

            if input("\n계속할까요? [Y/n]: ").strip().lower() not in ("", "y"):
                break
    except KeyboardInterrupt:
        pass
    print("\n🛑 종료합니다.")


if __name__ == "__main__":
    main()
