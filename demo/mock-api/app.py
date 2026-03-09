"""
Mock SaaS API service for Pipeline Agent demos.
Serves realistic Stripe, Google Ads, Facebook Insights, and Slack webhook endpoints.
"""
from datetime import datetime, timedelta
from typing import Any

from fastapi import FastAPI, Header, Query, Path, Body
from fastapi.responses import JSONResponse

app = FastAPI(title="Demo Mock APIs")

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Stripe Mock — /v1/charges, /v1/customers
# ---------------------------------------------------------------------------

_STRIPE_CUSTOMERS = [
    {"id": f"cus_{1000+i}", "email": f"customer{i+1}@example.com",
     "name": f"Customer {i+1}", "created": int((datetime(2025, 6, 1) + timedelta(days=i*5)).timestamp()),
     "currency": "usd", "balance": 0, "delinquent": False,
     "metadata": {"source": "demo"}}
    for i in range(30)
]

_STRIPE_CHARGES = []
for i in range(50):
    dt = datetime(2025, 9, 1) + timedelta(days=i * 2, hours=i % 12)
    _STRIPE_CHARGES.append({
        "id": f"ch_{20000+i}",
        "amount": (i * 1337 + 2500) % 100000 + 500,
        "currency": "usd",
        "status": "succeeded" if i % 7 != 0 else "failed",
        "customer": f"cus_{1000 + (i % 30)}",
        "description": f"Payment for order {10001 + i}",
        "created": int(dt.timestamp()),
        "paid": i % 7 != 0,
        "refunded": i % 13 == 0,
        "amount_refunded": ((i * 1337 + 2500) % 100000 + 500) if i % 13 == 0 else 0,
        "metadata": {"order_id": f"ORD-{10001+i}"},
    })


def _paginate(data: list, limit: int, offset: int) -> dict:
    page = data[offset:offset + limit]
    return {
        "object": "list",
        "data": page,
        "has_more": (offset + limit) < len(data),
        "total_count": len(data),
    }


@app.get("/v1/charges")
def stripe_charges(
    limit: int = Query(default=25, le=100),
    starting_after: str = Query(default=None),
    authorization: str = Header(default=""),
):
    offset = 0
    if starting_after:
        for idx, c in enumerate(_STRIPE_CHARGES):
            if c["id"] == starting_after:
                offset = idx + 1
                break
    return _paginate(_STRIPE_CHARGES, limit, offset)


@app.get("/v1/customers")
def stripe_customers(
    limit: int = Query(default=25, le=100),
    starting_after: str = Query(default=None),
    authorization: str = Header(default=""),
):
    offset = 0
    if starting_after:
        for idx, c in enumerate(_STRIPE_CUSTOMERS):
            if c["id"] == starting_after:
                offset = idx + 1
                break
    return _paginate(_STRIPE_CUSTOMERS, limit, offset)


# ---------------------------------------------------------------------------
# Google Ads Mock — /v1/customers/{customer_id}/googleAds
# ---------------------------------------------------------------------------

_GOOGLE_ADS_CAMPAIGNS = []
for i in range(40):
    dt = datetime(2025, 10, 1) + timedelta(days=i * 2)
    _GOOGLE_ADS_CAMPAIGNS.append({
        "campaign_id": f"camp_{3000+i}",
        "campaign_name": f"Campaign {'ABCDE'[i%5]} - {'Brand' if i%3==0 else 'Performance'}",
        "status": "ENABLED" if i % 8 != 0 else "PAUSED",
        "date": dt.strftime("%Y-%m-%d"),
        "impressions": (i * 743 + 5000) % 50000 + 1000,
        "clicks": (i * 137 + 200) % 5000 + 50,
        "cost_micros": ((i * 2371 + 10000) % 500000 + 10000) * 1000,
        "conversions": (i * 17 + 5) % 100,
        "conversion_value": round(((i * 17 + 5) % 100) * 42.50, 2),
        "ctr": round(((i * 137 + 200) % 5000 + 50) / ((i * 743 + 5000) % 50000 + 1000), 4),
    })


@app.get("/v1/customers/{customer_id}/googleAds")
def google_ads_report(
    customer_id: str = Path(...),
    page_size: int = Query(default=25, le=100),
    page_token: str = Query(default=None),
):
    offset = int(page_token) if page_token else 0
    page = _GOOGLE_ADS_CAMPAIGNS[offset:offset + page_size]
    next_offset = offset + page_size
    return {
        "results": page,
        "next_page_token": str(next_offset) if next_offset < len(_GOOGLE_ADS_CAMPAIGNS) else None,
        "total_results_count": len(_GOOGLE_ADS_CAMPAIGNS),
    }


# ---------------------------------------------------------------------------
# Facebook Insights Mock — /v1/{ad_account_id}/insights
# ---------------------------------------------------------------------------

_FB_INSIGHTS = []
for i in range(45):
    dt = datetime(2025, 11, 1) + timedelta(days=i * 2)
    _FB_INSIGHTS.append({
        "ad_id": f"ad_{4000+i}",
        "ad_name": f"Ad Creative {i+1}",
        "adset_id": f"adset_{500 + i // 5}",
        "adset_name": f"Ad Set {i // 5 + 1}",
        "campaign_id": f"camp_{100 + i // 10}",
        "campaign_name": f"FB Campaign {i // 10 + 1}",
        "date_start": dt.strftime("%Y-%m-%d"),
        "date_stop": (dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        "impressions": (i * 891 + 3000) % 40000 + 500,
        "clicks": (i * 89 + 100) % 3000 + 20,
        "spend": round(((i * 1500 + 5000) % 30000 + 1000) / 100, 2),
        "actions": [
            {"action_type": "link_click", "value": str((i * 67 + 30) % 500 + 10)},
            {"action_type": "purchase", "value": str((i * 7 + 2) % 50)},
        ],
        "cost_per_action_type": [
            {"action_type": "link_click",
             "value": str(round(((i * 1500 + 5000) % 30000 + 1000) / 100 / max((i * 67 + 30) % 500 + 10, 1), 2))},
        ],
    })


@app.get("/v1/{ad_account_id}/insights")
def facebook_insights(
    ad_account_id: str = Path(...),
    limit: int = Query(default=25, le=100),
    after: str = Query(default=None),
    access_token: str = Query(default=""),
):
    offset = int(after) if after else 0
    page = _FB_INSIGHTS[offset:offset + limit]
    next_offset = offset + limit
    has_next = next_offset < len(_FB_INSIGHTS)
    return {
        "data": page,
        "paging": {
            "cursors": {
                "after": str(next_offset) if has_next else None,
            },
            "next": f"/v1/{ad_account_id}/insights?after={next_offset}&limit={limit}" if has_next else None,
        },
    }


# ---------------------------------------------------------------------------
# Mock Slack Webhook — /webhook/slack
# ---------------------------------------------------------------------------

_SLACK_MESSAGES: list[dict] = []


@app.post("/webhook/slack")
def slack_webhook(payload: dict = Body(...)):
    """Receive a Slack-formatted alert and store it for verification."""
    _SLACK_MESSAGES.append({
        "received_at": datetime.utcnow().isoformat(),
        "text": payload.get("text", ""),
        "payload": payload,
    })
    return {"ok": True}


@app.get("/webhook/slack/history")
def slack_history(limit: int = Query(default=50, le=200)):
    """Return list of received Slack webhook messages for verification."""
    return {
        "messages": _SLACK_MESSAGES[-limit:],
        "total_count": len(_SLACK_MESSAGES),
    }
