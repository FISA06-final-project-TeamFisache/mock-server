"""
Mock FastAPI — Spring AgentService 가 호출하는 3개 엔드포인트 흉내

  POST /portfolio/profile         ← AgentService.generateProfile  (PortiSurvey 화면)
  POST /portfolio/rebalance       ← AgentService.recommend        (AssetPrescription 화면)
  POST /portfolio/asset-portfolio ← AgentService.generatePrescriptions (PrescriptionComplete 화면)

실행:
    cd mock-server
    .\\venv\\Scripts\\Activate.ps1
    pip install fastapi uvicorn
    uvicorn mock_asset_portfolio:app --host 0.0.0.0 --port 8000 --reload
"""
from datetime import datetime
from fastapi import FastAPI, Request

app = FastAPI()

# 백엔드가 요청에 products 를 안 보내므로, 응답 portfolio[].name 매핑이 되도록
# seed_1_mydata.sql 의 products.name 과 정확히 일치하는 기본 상품 카탈로그를 둔다.
DEFAULT_PRODUCTS = [
    {"product_type": "DEPOSIT", "name": "WON 정기예금"},
    {"product_type": "SAVING",  "name": "26주 적금"},
    {"product_type": "SAVING",  "name": "토스 자유적금"},
    {"product_type": "STOCK",   "name": "TIGER 미국S&P500"},
    {"product_type": "STOCK",   "name": "KODEX 나스닥100"},
    {"product_type": "IRP",     "name": "미래에셋 TDF2045"},
]


@app.get("/")
async def health():
    return {"status": "ok"}


# ──────────────────────────────────────────────────────────────
# 미니챌린지 — Spring ChallengeAgentService 가 호출
#   POST /mini_challenge         ← recommend  body: {user_id, category_expense[], stock_themes[]}
#   POST /mini_challenge/adjust  ← adjust     body: {user_id, feedback}
# 응답: ChallengeProposalResponseDto
#   {created_at, title, description, category, challenge_sub_type, challenge_type,
#    target, estimated_saving, ticker}
#   challenge_sub_type ∈ COFFEE/DELIVERY/ALCOHOL/LATE_NIGHT/LUNCH/SHOPPING/TAXI
#   challenge_type     ∈ AMOUNT/COUNT
# ──────────────────────────────────────────────────────────────
def _proposal(title, description, category, sub_type, ctype, target, saving, ticker):
    return {
        "created_at": datetime.now().isoformat(),
        "title": title,
        "description": description,
        "category": category,
        "challenge_sub_type": sub_type,
        "challenge_type": ctype,
        "target": target,
        "estimated_saving": saving,
        "ticker": ticker,
    }


@app.post("/mini_challenge")
async def mini_challenge(req: Request):
    body = await req.json()
    print("\n[mock /mini_challenge] user_id={} themes={}".format(
        body.get("user_id"), body.get("stock_themes")))
    return _proposal(
        "커피 3잔만 마시기", "이번주 카페 지출을 줄여 절약 습관을 만들어봐요!",
        "카페", "COFFEE", "COUNT", 3, 24000, "삼성전자",
    )


@app.post("/mini_challenge/adjust")
async def mini_challenge_adjust(req: Request):
    body = await req.json()
    feedback = body.get("feedback") or ""
    print("\n[mock /mini_challenge/adjust] user_id={} feedback={}".format(
        body.get("user_id"), feedback))

    if "쉽게" in feedback:
        return _proposal(
            "커피 5잔까지 OK", "조금 여유있게! 이번주 커피 5잔 이내로 도전해요.",
            "카페", "COFFEE", "COUNT", 5, 14000, "삼성전자",
        )
    if "어렵게" in feedback:
        return _proposal(
            "커피 2잔만 마시기", "독하게! 이번주 커피는 딱 2잔까지만.",
            "카페", "COFFEE", "COUNT", 2, 35000, "삼성전자",
        )
    if "주제" in feedback:
        return _proposal(
            "배달 음식 2번만 시키기", "주제를 바꿔봤어요. 이번주 배달은 2번까지!",
            "식비", "DELIVERY", "COUNT", 2, 48000, "카카오",
        )
    # 그 외 피드백 → 기본 제안
    return _proposal(
        "커피 3잔만 마시기", "이번주 카페 지출을 줄여 절약 습관을 만들어봐요!",
        "카페", "COFFEE", "COUNT", 3, 24000, "삼성전자",
    )


# ──────────────────────────────────────────────────────────────
# POST /portfolio/profile  ← AgentService.generateProfile
# 응답: { expense_comment, invest_comment, savings_comment }
# ──────────────────────────────────────────────────────────────
@app.post("/portfolio/profile")
async def portfolio_profile(req: Request):
    body = await req.json()
    print("\n[mock /portfolio/profile] user_id={} porti_type={}".format(
        body.get("user_id"), body.get("porti_type")))
    return {
        "expense_comment": "식비·문화 지출이 평균보다 살짝 높지만 통제 가능한 범위예요. 한 카테고리만 줄여도 월 10만 원 여유가 생깁니다.",
        "invest_comment":  "안전 자산 비중이 충분합니다. 다만 장기 자산이 부족하니 IRP·ETF 비중을 점진적으로 늘려보세요.",
        "savings_comment": "현금성 자산이 잘 모여있어 비상금 베이스는 든든해요. 다음은 절세 계좌(IRP·ISA)로 자산을 분산해볼 시점입니다.",
    }


# ──────────────────────────────────────────────────────────────
# POST /portfolio/rebalance  ← AgentService.recommend
# 응답: { invest_amount: int, salary_rebalance: [{asset_id, category, ratio}] }
#   ratio = salary 대비 % (백엔드가 amount = salary * ratio / 100 계산)
# ──────────────────────────────────────────────────────────────
@app.post("/portfolio/rebalance")
async def portfolio_rebalance(req: Request):
    body = await req.json()
    salary = int(body.get("salary") or 3200000)
    fixed = int(body.get("fixed_expense") or 0)
    assets = body.get("assets") or []

    print("\n[mock /portfolio/rebalance] user_id={} salary={} fixed={} assets={}".format(
        body.get("user_id"), salary, fixed, len(assets)))

    def find_first(types):
        for a in assets:
            if a.get("asset_type") in types:
                return a.get("asset_id")
        return None

    checking_id = find_first(["CHECKING"])
    parking_id  = find_first(["PARKING", "CMA"])
    saving_id   = find_first(["SAVINGS", "DEPOSIT"])
    stock_id    = find_first(["STOCK"])
    irp_id      = find_first(["IRP"])

    # plans = 비투자 이체 (생활비/비상금/적금) — 합 45%
    # invest_amount = 투자 총액 (ETF/IRP) — 보유 자산에 따라 0~25%
    # 남은 약 30% = 변동지출 여유분
    # 백엔드는 각 plan 에서 amount(원)·account_purpose(→nickname)·comment 를 직접 읽음
    plan_specs = []
    if checking_id: plan_specs.append((checking_id, "생활비", 25, "매달 고정적으로 쓰는 생활비예요"))
    if parking_id:  plan_specs.append((parking_id,  "비상금", 10, "급한 일에 대비해 비상금을 모아둬요"))
    if saving_id:   plan_specs.append((saving_id,   "적금",   10, "목돈 마련을 위해 매달 자동으로 적립해요"))

    plans = [
        {
            "asset_id": aid,
            "account_purpose": purpose,
            "amount": salary * ratio // 100,
            "comment": comment,
        }
        for aid, purpose, ratio, comment in plan_specs
    ]

    invest_ratio = 0
    if stock_id: invest_ratio += 15
    if irp_id:   invest_ratio += 10
    invest_amount = salary * invest_ratio // 100

    print(f"  → salary_rebalance {len(plans)}건: {[(p['account_purpose'], p['amount']) for p in plans]}, invest={invest_amount}")

    return {
        "invest_amount": invest_amount,
        "salary_rebalance": plans,
    }


# ──────────────────────────────────────────────────────────────
# POST /portfolio/asset-portfolio  ← AgentService.generatePrescriptions
# 응답: { created_at, investment_flows: [{title, term, summary, funding_sources, gathering_id, portfolio}] }
# ──────────────────────────────────────────────────────────────
def _group(items, key):
    out = {}
    for it in items:
        out.setdefault(it.get(key), []).append(it)
    return out


def _pick_gathering(by_type, candidates):
    for t in candidates:
        lst = by_type.get(t) or []
        if lst:
            return lst[0]
    return None


# 상품 타입별 AI 코멘트 (mock) — portfolio[].comment → 백엔드 aiComment 로 매핑
PRODUCT_COMMENTS = {
    "DEPOSIT": "원금 보장으로 안전하게 굴리는 자산이에요",
    "SAVING":  "매달 자동으로 모아 목돈을 만드는 습관 자산이에요",
    "STOCK":   "장기 우상향에 투자하는 핵심 성장 자산이에요",
    "BOND":    "주식 변동성을 낮춰주는 안정 자산이에요",
    "IRP":     "세액공제 혜택까지 챙기는 노후 대비 자산이에요",
}

# 흐름(term)별 AI 수익률·코멘트 메타 (mock)
TERM_META = {
    "단기": {"expected_rr_pct": 3.5,  "investment_months": 6,
            "account_comment": "급할 때 바로 꺼낼 수 있게 파킹 통장에 모아요",
            "rr_comment": "안전 자산 위주라 변동 없이 또박또박 늘어나요"},
    "중기": {"expected_rr_pct": 6.5,  "investment_months": 36,
            "account_comment": "절세 혜택이 있는 ISA로 중기 목표를 키워요",
            "rr_comment": "주식과 채권을 섞어 안정과 수익의 균형을 맞췄어요"},
    "장기": {"expected_rr_pct": 9.0,  "investment_months": 120,
            "account_comment": "IRP로 세액공제 받으며 노후를 길게 준비해요",
            "rr_comment": "장기 복리 효과로 자산이 크게 불어나요"},
}


def _expected_amount(monthly, months, rr_pct):
    """월 납입(monthly, 원)을 months 개월간 연 rr_pct% 복리 적립 시 예상 평가액(원)"""
    mr = rr_pct / 100 / 12
    if mr == 0:
        return monthly * months
    return round(monthly * (((1 + mr) ** months - 1) / mr))


def _build_portfolio(products_by_type, type_ratio_pairs):
    chosen = []
    for ptype, ratio in type_ratio_pairs:
        lst = products_by_type.get(ptype) or []
        if lst:
            chosen.append({
                "name": lst[0]["name"],
                "ratio": ratio,
                "comment": PRODUCT_COMMENTS.get(ptype, "포트폴리오 균형을 위한 자산이에요"),
            })
    if chosen:
        diff = 100 - sum(c["ratio"] for c in chosen)
        chosen[0]["ratio"] = chosen[0]["ratio"] + diff
    return chosen


@app.post("/portfolio/asset-portfolio", status_code=201)
async def asset_portfolio(req: Request):
    body = await req.json()
    print("\n[mock /asset-portfolio]")
    print(f"  user_id       = {body.get('user_id')}")
    print(f"  invest_amount = {body.get('invest_amount')}")
    print(f"  porti_type    = {body.get('porti_type')}")
    print(f"  assets        = {len(body.get('invest_assets') or [])}")
    print(f"  products      = {len(body.get('products') or [])}")

    invest_amount = int(body.get("invest_amount") or 0)
    invest_assets = body.get("invest_assets") or []
    # 백엔드는 더 이상 요청에 products 를 보내지 않고, 응답 portfolio[].name 을
    # DB products.name 으로 매핑한다. → 시드된 상품명과 일치하는 기본 카탈로그로 폴백.
    products = body.get("products") or DEFAULT_PRODUCTS

    by_type = _group(invest_assets, "asset_type")
    products_by_type = _group(products, "product_type")

    cash_pool = []
    for t in ("CHECKING", "PARKING", "CMA", "SAVINGS", "DEPOSIT"):
        cash_pool.extend(by_type.get(t) or [])

    # 1단계: 흐름 후보를 모아 weight 만 기록 — 실제 amount 는 나중에 정규화
    # gathering / funding 둘 다 풀이 비면 cash_pool 전체로 폴백 → mock 은 항상 3개 생성 시도
    candidates = []

    def pick_funding(gathering_asset, max_n):
        if gathering_asset is None:
            return cash_pool[:max_n]
        f = [c for c in cash_pool if c["asset_id"] != gathering_asset["asset_id"]][:max_n]
        return f or [gathering_asset]   # 다른 게 없으면 gathering 자신을 funding 으로 재사용

    g1 = _pick_gathering(by_type, ["PARKING", "CMA", "CHECKING", "SAVINGS", "DEPOSIT", "ISA", "IRP"])
    if g1:
        candidates.append({
            "weight": 30,
            "title": "비상금·생활비 베이스",
            "term": "단기",
            "summary": "예상 못한 지출에 흔들리지 않게 든든히 준비해요",
            "gathering": g1,
            "funding": pick_funding(g1, 2),
            "portfolio_specs": [
                [("SAVING", 60), ("DEPOSIT", 40)],
                [("DEPOSIT", 100)],
                [("SAVING", 100)],
            ],
        })

    g2 = _pick_gathering(by_type, ["ISA", "CMA", "CHECKING", "SAVINGS", "DEPOSIT", "PARKING", "IRP"])
    if g2:
        candidates.append({
            "weight": 40,
            "title": "중기 목표 자산",
            "term": "중기",
            "summary": "3~5년 목표를 안정적으로 키워가요",
            "gathering": g2,
            "funding": pick_funding(g2, 1),
            "portfolio_specs": [
                [("BOND", 55), ("STOCK", 45)],
                [("BOND", 100)],
                [("STOCK", 100)],
            ],
        })

    g3 = _pick_gathering(by_type, ["IRP", "ISA", "CHECKING", "CMA", "PARKING", "SAVINGS", "DEPOSIT"])
    if g3:
        candidates.append({
            "weight": 30,
            "title": "은퇴·연금 장기 자산",
            "term": "장기",
            "summary": "세제혜택을 살려 길게 굴려요",
            "gathering": g3,
            "funding": pick_funding(g3, 1),
            "portfolio_specs": [
                [("STOCK", 70), ("IRP", 30)],
                [("STOCK", 100)],
                [("IRP", 100)],
            ],
        })

    # 단+장 만 있는 경우 단:장 = 6:4 로 분배 (중기 없을 때 단기 비중을 키움)
    terms_present = {c["term"] for c in candidates}
    if terms_present == {"단기", "장기"}:
        for c in candidates:
            c["weight"] = 60 if c["term"] == "단기" else 40

    # 계좌 추천 흐름 — 보유 계좌가 아니라 'AI가 새 통장 개설을 추천'하는 케이스
    #   gathering_id 없이 gathering_account 를 보내면 백엔드가 isRecommendation=true 로 저장
    #   (gatheringAsset.id=null → 프론트의 '✨ 추천 계좌 (아직 미보유)' UI)
    candidates.append({
        "weight": 20,
        "title": "추천! 고금리 적금 새 통장",
        "term": "단기",
        "summary": "지금 보유한 통장보다 금리가 높은 새 통장을 추천드려요",
        "gathering": None,
        "gathering_account": {
            "name": "토스뱅크 자유적금",
            "type": "SAVINGS",
            "institution": "토스뱅크",
            "interest_rate": 5.0,
        },
        "account_comment": "보유 통장보다 금리가 높아 같은 돈을 더 빠르게 불릴 수 있어요",
        "funding": cash_pool[:1],
        "portfolio_specs": [
            [("SAVING", 100)],
            [("DEPOSIT", 100)],
        ],
    })

    # 2단계: invest_amount 를 weight 비율로 분배 (합이 정확히 invest_amount 가 되도록)
    flows = []
    total_weight = sum(c["weight"] for c in candidates)
    allocated = 0
    for i, c in enumerate(candidates):
        if i == len(candidates) - 1:
            # 마지막 흐름은 잔액 — 반올림 오차 흡수
            flow_amount = invest_amount - allocated
        else:
            flow_amount = invest_amount * c["weight"] // total_weight if total_weight else 0
            allocated += flow_amount

        # funding_sources — 통장 잔액 기반의 초기 자본 (월 납입금액과 별개)
        #   각 통장 balance 의 50% 를 끌어와 초기 자본으로 넣는 가정 (최소 10만원)
        funding_sources = []
        for f in c["funding"]:
            bal = int(f.get("balance") or 0)
            amt = max(bal // 2, 100000)
            funding_sources.append({"asset_id": f["asset_id"], "amount": amt})

        portfolio = None
        for spec in c["portfolio_specs"]:
            portfolio = _build_portfolio(products_by_type, spec)
            if portfolio:
                break

        meta = TERM_META.get(c["term"], TERM_META["중기"])
        gathering = c.get("gathering")

        flow_out = {
            "title": c["title"],
            "term": c["term"],
            "summary": c["summary"],
            "funding_sources": funding_sources,
            # 보유 계좌면 gathering_id(문자열), 계좌 추천이면 gathering_account 객체.
            # 백엔드는 gathering_id 가 null 이면 gathering_account 로 isRecommendation=true 저장.
            "gathering_id": gathering["asset_id"] if gathering else None,
            "amount": flow_amount,
            # AI 생성 필드 — 백엔드가 그대로 portfolio_flows 에 저장 (null 방지)
            "account_comment": c.get("account_comment") or meta["account_comment"],
            "expected_rr_pct": meta["expected_rr_pct"],
            "investment_months": meta["investment_months"],
            "expected_amount": _expected_amount(
                flow_amount, meta["investment_months"], meta["expected_rr_pct"]),
            "rr_comment": meta["rr_comment"],
            "portfolio": portfolio or [],
        }
        if gathering is None:
            flow_out["gathering_account"] = c["gathering_account"]
        flows.append(flow_out)

    print(f"  → investment_flows {len(flows)}건 생성, 월납입 합 = {sum(f['amount'] for f in flows)} / invest_amount={invest_amount}")
    for f in flows:
        funding_sum = sum(fs['amount'] for fs in f['funding_sources'])
        print(f"     [{f['term']}] 월납입={f['amount']:,}, 끌어오기 합={funding_sum:,}")
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "investment_flows": flows,
    }
