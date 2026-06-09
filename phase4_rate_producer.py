# Phase 4 트랙 B — 지속 도착률 생산기
#   target rate(msg/s) 로 duration(s) 동안 페이싱 발행. mock_payment 의 생성 로직 재사용.
#   mock_payment.run_continuous 와 달리: 메시지당 flush/print 없음(고TPS 병목 제거),
#   20ms 틱 배치 페이싱, 지정 시간 후 종료.
# 사용: python phase4_rate_producer.py --rate 2000 --duration 180
import argparse
import time
import mock_payment as mp   # import 시 카드 풀 DB 로드 + producer 초기화

def run(rate: float, duration: float):
    print(f"[rateprod] target={rate}/s duration={duration}s cards={len(mp.ASSET_NUMBERS)}")
    start = time.time()
    end = start + duration
    tick = 0.02                  # 20ms 배치
    per_tick = rate * tick
    acc = 0.0
    next_t = start
    sent = 0
    last_log = start
    while time.time() < end:
        next_t += tick
        acc += per_tick
        n = int(acc); acc -= n
        for _ in range(n):
            mp._produce(mp.generate_transaction())
            sent += 1
        mp.producer.poll(0)
        now = time.time()
        if now - last_log >= 10:
            print(f"[rateprod] t={now-start:.0f}s sent={sent} inst_rate={sent/(now-start):.0f}/s")
            last_log = now
        sl = next_t - time.time()
        if sl > 0:
            time.sleep(sl)
        # behind schedule -> no sleep (best effort; producer-side ceiling)
    mp.producer.flush()
    elapsed = time.time() - start
    print(f"[rateprod] DONE sent={sent} elapsed={elapsed:.1f}s actual_rate={sent/elapsed:.0f}/s target={rate}/s")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Phase4 sustained-rate producer")
    p.add_argument("--rate", type=float, required=True, help="target msg/s")
    p.add_argument("--duration", type=float, default=180, help="seconds to sustain")
    a = p.parse_args()
    run(a.rate, a.duration)
