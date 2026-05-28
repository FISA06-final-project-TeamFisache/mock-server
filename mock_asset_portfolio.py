"""
Mock FastAPI — Spring AgentService 가 호출하는 3개 엔드포인트 흉내

  POST /portfolio/profile   ← AgentService.generateProfile  (PortiSurvey 화면)
  POST /portfolio/rebalance ← AgentService.recommend        (AssetPrescription 화면)
  POST /asset-portfolio     ← AgentService.generatePrescriptions (PrescriptionComplete 화면)

실행:
    cd mock-server
    .\\venv\\Scripts\\Activate.ps1
    pip install fastapi uvicorn
    uvicorn mock_asset_portfolio:app --host 0.0.0.0 --port 8000 --reload
"""
from datetime import datetime
from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
async def health():
    return {"status": "ok"}


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
# 응답: { invest_amount: int, salary_rebalance: [{asset_number, category, ratio}] }
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

    # asset_number 추출 (백엔드가 asset_number 로 매핑함)
    def find_first(types):
        for a in assets:
            if a.get("asset_type") in types:
                return a.get("asset_number")
        return None

    checking_num = find_first(["CHECKING"])
    parking_num  = find_first(["PARKING", "CMA"])
    saving_num   = find_first(["SAVINGS", "DEPOSIT"])
    stock_num    = find_first(["STOCK"])
    irp_num      = find_first(["IRP"])

    # 월급 분배 비율 (총합 약 70%, 나머지 30% = 변동지출)
    plans = []
    if checking_num: plans.append({"asset_number": checking_num, "category": "생활비",   "ratio": 25})
    if parking_num:  plans.append({"asset_number": parking_num,  "category": "비상금",   "ratio": 10})
    if saving_num:   plans.append({"asset_number": saving_num,   "category": "적금",     "ratio": 10})
    if stock_num:    plans.append({"asset_number": stock_num,    "category": "ETF",      "ratio": 15})
    if irp_num:      plans.append({"asset_number": irp_num,      "category": "IRP",      "ratio": 10})

    print(f"  → salary_rebalance {len(plans)}건: {[(p['category'], p['asset_number']) for p in plans]}")

    return {
        "invest_amount": int(salary * 0.31),   # 약 31% 투자
        "salary_rebalance": plans,
    }


# ──────────────────────────────────────────────────────────────
# POST /asset-portfolio  ← AgentService.generatePrescriptions
# 응답: { created_at, investment_flows: [{title, term, summary, funding_sources, gathering_account, portfolio}] }
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


def _build_portfolio(products_by_type, type_ratio_pairs):
    chosen = []
    for ptype, ratio in type_ratio_pairs:
        lst = products_by_type.get(ptype) or []
        if lst:
            chosen.append({"name": lst[0]["name"], "ratio": ratio})
    if chosen:
        diff = 100 - sum(c["ratio"] for c in chosen)
        chosen[0]["ratio"] = chosen[0]["ratio"] + diff
    return chosen


@app.post("/asset-portfolio", status_code=201)
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
    products = body.get("products") or []

    by_type = _group(invest_assets, "asset_type")
    products_by_type = _group(products, "product_type")

    cash_pool = []
    for t in ("CHECKING", "PARKING", "CMA", "SAVINGS", "DEPOSIT"):
        cash_pool.extend(by_type.get(t) or [])

    flows = []

    # 단기 ─ 비상금
    g1 = _pick_gathering(by_type, ["PARKING", "CMA", "CHECKING"])
    if g1:
        funding = [c for c in cash_pool if c["asset_id"] != g1["asset_id"]][:2]
        if not funding:
            funding = [g1]
        amt_each = max(int(invest_amount * 0.3 / max(len(funding), 1)), 100000)
        flows.append({
            "title": "비상금·생활비 베이스",
            "term": "단기",
            "summary": "예상 못한 지출에 흔들리지 않게 든든히 준비해요",
            "funding_sources": [
                {"asset_id": f["asset_id"], "amount": amt_each} for f in funding
            ],
            "gathering_account": g1["asset_id"],
            "portfolio": _build_portfolio(products_by_type, [
                ("SAVING", 60), ("DEPOSIT", 40),
            ]) or _build_portfolio(products_by_type, [("DEPOSIT", 100)])
              or _build_portfolio(products_by_type, [("SAVING", 100)]),
        })

    # 중기 ─ ISA / 채권+주식
    g2 = _pick_gathering(by_type, ["ISA", "CHECKING", "CMA"])
    if g2:
        funding = [c for c in cash_pool if c["asset_id"] != g2["asset_id"]][:1]
        if funding:
            amt = max(int(invest_amount * 0.4), 100000)
            flows.append({
                "title": "중기 목표 자산",
                "term": "중기",
                "summary": "3~5년 목표를 안정적으로 키워가요",
                "funding_sources": [
                    {"asset_id": f["asset_id"], "amount": amt} for f in funding
                ],
                "gathering_account": g2["asset_id"],
                "portfolio": _build_portfolio(products_by_type, [
                    ("BOND", 55), ("STOCK", 45),
                ]) or _build_portfolio(products_by_type, [("BOND", 100)])
                  or _build_portfolio(products_by_type, [("STOCK", 100)]),
            })

    # 장기 ─ IRP / 주식+IRP
    g3 = _pick_gathering(by_type, ["IRP", "ISA", "CHECKING"])
    if g3:
        funding = [c for c in cash_pool if c["asset_id"] != g3["asset_id"]][:1]
        if funding:
            amt = max(int(invest_amount * 0.3), 100000)
            flows.append({
                "title": "은퇴·연금 장기 자산",
                "term": "장기",
                "summary": "세제혜택을 살려 길게 굴려요",
                "funding_sources": [
                    {"asset_id": f["asset_id"], "amount": amt} for f in funding
                ],
                "gathering_account": g3["asset_id"],
                "portfolio": _build_portfolio(products_by_type, [
                    ("STOCK", 70), ("IRP", 30),
                ]) or _build_portfolio(products_by_type, [("STOCK", 100)])
                  or _build_portfolio(products_by_type, [("IRP", 100)]),
            })

    print(f"  → investment_flows {len(flows)}건 생성")
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "investment_flows": flows,
    }
