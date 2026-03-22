"""
Demo pipeline bootstrap — creates demo pipelines via the real DAPOS API.

Runs as a background task AFTER the API server is healthy, exercising the
same code paths a real user would: login, create pipelines, create transforms,
set catalog metadata, and trigger first runs.

Supports both local Docker Compose and cloud (Railway/Render) deployments.
Cloud: set DEMO_MYSQL_URL and DEMO_MONGO_URL env vars; data is seeded automatically.
"""
from __future__ import annotations
import asyncio
import logging
import os
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx

log = logging.getLogger("demo.bootstrap")

# ---------------------------------------------------------------------------
# Source connection resolution — env vars for cloud, defaults for Docker
# ---------------------------------------------------------------------------

def _mysql_config():
    """Resolve MySQL demo source connection."""
    url = os.getenv("DEMO_MYSQL_URL", "")
    if url:
        parsed = urlparse(url)
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "database": (parsed.path or "/demo_ecommerce").lstrip("/"),
            "user": parsed.username or "root",
            "password": parsed.password or "",
        }
    return {
        "host": os.getenv("DEMO_MYSQL_HOST", "localhost"),
        "port": int(os.getenv("DEMO_MYSQL_PORT", "3307")),
        "database": os.getenv("DEMO_MYSQL_DATABASE", "demo_ecommerce"),
        "user": os.getenv("DEMO_MYSQL_USER", "root"),
        "password": os.getenv("DEMO_MYSQL_PASSWORD", ""),
    }


def _mongo_config():
    """Resolve MongoDB demo source connection."""
    url = os.getenv("DEMO_MONGO_URL", "")
    if url:
        parsed = urlparse(url)
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 27017,
            "database": "demo_analytics",
            "url": url,
        }
    return {
        "host": os.getenv("DEMO_MONGO_HOST", "localhost"),
        "port": int(os.getenv("DEMO_MONGO_PORT", "27018")),
        "database": "demo_analytics",
        "url": "",
    }


def _target_config():
    """Read target connection from Config so Railway/cloud DATABASE_URL works."""
    from config import Config
    cfg = Config()
    return {
        "host": cfg.pg_host,
        "port": cfg.pg_port,
        "database": cfg.pg_database,
        "user": cfg.pg_user,
        "password": cfg.pg_password,
    }


# ---------------------------------------------------------------------------
# Data seeding — creates tables and inserts demo data if missing
# ---------------------------------------------------------------------------

async def _seed_mysql(cfg: dict) -> bool:
    """Seed demo e-commerce data into MySQL. Returns True if successful."""
    try:
        import pymysql
    except ImportError:
        log.warning("pymysql not installed, skipping MySQL seed")
        return False

    try:
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=10,
        )
        cursor = conn.cursor()

        # Create database if needed
        db_name = cfg["database"]
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")

        # Check if data already exists
        cursor.execute("SHOW TABLES LIKE 'orders'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM orders")
            count = cursor.fetchone()[0]
            if count > 0:
                log.info("MySQL demo data already seeded (%d orders), skipping.", count)
                conn.close()
                return True

        # Read and execute the init SQL
        init_path = os.path.join(os.path.dirname(__file__), "mysql-init", "01-schema.sql")
        if os.path.exists(init_path):
            with open(init_path, "r") as f:
                sql = f.read()
            # Remove USE statement (we already selected the DB)
            sql = sql.replace("USE demo_ecommerce;", "")
            # Split on semicolons and execute each statement
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cursor.execute(stmt)
            conn.commit()
            log.info("Seeded MySQL demo data into %s:%d/%s", cfg["host"], cfg["port"], db_name)
        else:
            log.warning("MySQL init file not found: %s", init_path)
            conn.close()
            return False

        conn.close()
        return True
    except Exception as e:
        log.warning("Could not seed MySQL demo data: %s", e)
        return False


async def _seed_mongo(cfg: dict) -> bool:
    """Seed demo analytics events into MongoDB. Returns True if successful."""
    try:
        from pymongo import MongoClient
    except ImportError:
        log.warning("pymongo not installed, skipping MongoDB seed")
        return False

    try:
        if cfg["url"]:
            client = MongoClient(cfg["url"], serverSelectionTimeoutMS=10000)
        else:
            client = MongoClient(cfg["host"], cfg["port"], serverSelectionTimeoutMS=10000)

        db = client[cfg["database"]]

        # Check if data already exists
        if db.events.count_documents({}) > 0:
            count = db.events.count_documents({})
            log.info("MongoDB demo data already seeded (%d events), skipping.", count)
            client.close()
            return True

        # Generate 200 events (equivalent to mongo-init/01-seed.js)
        event_types = ['page_view', 'click', 'add_to_cart', 'purchase', 'signup', 'search']
        pages = ['/home', '/products', '/products/detail', '/cart', '/checkout', '/account', '/search', '/about']
        browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
        devices = ['desktop', 'mobile', 'tablet']
        countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'BR']
        campaigns = [None, 'google_ads_q4', 'facebook_winter', 'email_newsletter', 'partner_ref']

        now = datetime.utcnow()
        events = []
        for i in range(200):
            days_ago = random.randint(0, 89)
            hours_ago = random.randint(0, 23)
            event_time = now - timedelta(days=days_ago, hours=hours_ago)
            user_id = f"user_{random.randint(1, 50)}"
            event_type = random.choice(event_types)

            event = {
                "event_id": f"evt_{10000 + i}",
                "event_type": event_type,
                "user_id": user_id,
                "session_id": f"sess_{random.randint(1, 500)}",
                "page_url": random.choice(pages),
                "timestamp": event_time,
                "browser": random.choice(browsers),
                "device": random.choice(devices),
                "country": random.choice(countries),
                "campaign": random.choice(campaigns),
                "properties": {},
            }

            if event_type == "page_view":
                event["properties"]["duration_seconds"] = random.randint(5, 304)
                event["properties"]["scroll_depth"] = random.randint(0, 99)
            elif event_type == "click":
                event["properties"]["element_id"] = f"btn_{random.randint(0, 19)}"
                event["properties"]["element_text"] = random.choice(['Buy Now', 'Learn More', 'Add to Cart', 'Sign Up'])
            elif event_type == "add_to_cart":
                event["properties"]["product_sku"] = f"SKU-{random.randint(1, 20):03d}"
                event["properties"]["quantity"] = random.randint(1, 3)
                event["properties"]["price"] = round(random.uniform(9.99, 500.99), 2)
            elif event_type == "purchase":
                event["properties"]["order_id"] = f"ORD-{10000 + random.randint(1, 30)}"
                event["properties"]["amount"] = round(random.uniform(20.00, 800.00), 2)
                event["properties"]["items_count"] = random.randint(1, 5)
            elif event_type == "search":
                event["properties"]["query"] = random.choice(['wireless mouse', 'keyboard', 'monitor', 'headphones', 'desk'])
                event["properties"]["results_count"] = random.randint(0, 49)

            events.append(event)

        db.events.insert_many(events)
        db.events.create_index("timestamp")
        db.events.create_index("event_type")

        log.info("Seeded %d events into MongoDB %s:%d/%s", len(events), cfg["host"], cfg["port"], cfg["database"])
        client.close()
        return True
    except Exception as e:
        log.warning("Could not seed MongoDB demo data: %s", e)
        return False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TARGET_CONNECTOR_NAME = "postgres-target-v1"
TARGET_SCHEMA = "raw"

# ---------------------------------------------------------------------------
# Build 26: Semantic tags, business context, and trust weights
# ---------------------------------------------------------------------------

DEMO_SEMANTIC_TAGS = {
    "demo-ecommerce-orders": {
        "id": {"semantic_name": "order_id", "domain": "technical", "description": "Unique order identifier", "pii": False, "unit": None, "source": "ai"},
        "customer_id": {"semantic_name": "customer_id", "domain": "identity", "description": "Reference to customer who placed the order", "pii": False, "unit": None, "source": "ai"},
        "order_number": {"semantic_name": "order_number", "domain": "operations", "description": "Human-readable order reference number", "pii": False, "unit": None, "source": "ai"},
        "status": {"semantic_name": "order_status", "domain": "operations", "description": "Current fulfillment status", "pii": False, "unit": None, "source": "ai"},
        "subtotal": {"semantic_name": "order_subtotal", "domain": "finance", "description": "Order amount before tax and shipping", "pii": False, "unit": "USD", "source": "ai"},
        "tax": {"semantic_name": "tax_amount", "domain": "finance", "description": "Tax charged on the order", "pii": False, "unit": "USD", "source": "ai"},
        "total": {"semantic_name": "order_total", "domain": "finance", "description": "Total order amount including tax and shipping", "pii": False, "unit": "USD", "source": "ai"},
        "created_at": {"semantic_name": "order_date", "domain": "temporal", "description": "When the order was placed", "pii": False, "unit": None, "source": "ai"},
        "updated_at": {"semantic_name": "last_modified", "domain": "temporal", "description": "When the order was last updated", "pii": False, "unit": None, "source": "ai"},
    },
    "demo-ecommerce-customers": {
        "id": {"semantic_name": "customer_id", "domain": "technical", "description": "Unique customer identifier", "pii": False, "unit": None, "source": "ai"},
        "email": {"semantic_name": "customer_email", "domain": "identity", "description": "Customer email address", "pii": True, "unit": None, "source": "ai"},
        "first_name": {"semantic_name": "first_name", "domain": "identity", "description": "Customer first name", "pii": True, "unit": None, "source": "ai"},
        "last_name": {"semantic_name": "last_name", "domain": "identity", "description": "Customer last name", "pii": True, "unit": None, "source": "ai"},
        "company": {"semantic_name": "company_name", "domain": "identity", "description": "Customer company or organization", "pii": False, "unit": None, "source": "ai"},
        "tier": {"semantic_name": "customer_tier", "domain": "operations", "description": "Subscription tier (free/pro/enterprise)", "pii": False, "unit": None, "source": "ai"},
        "created_at": {"semantic_name": "signup_date", "domain": "temporal", "description": "When the customer account was created", "pii": False, "unit": None, "source": "ai"},
        "updated_at": {"semantic_name": "last_modified", "domain": "temporal", "description": "When the customer record was last updated", "pii": False, "unit": None, "source": "ai"},
    },
    "demo-analytics-events": {
        "event_id": {"semantic_name": "event_id", "domain": "technical", "description": "Unique event identifier", "pii": False, "unit": None, "source": "ai"},
        "event_type": {"semantic_name": "event_type", "domain": "product", "description": "Type of user interaction (page_view, click, purchase, etc.)", "pii": False, "unit": None, "source": "ai"},
        "user_id": {"semantic_name": "user_id", "domain": "identity", "description": "Anonymous user identifier", "pii": False, "unit": None, "source": "ai"},
        "page_url": {"semantic_name": "page_url", "domain": "product", "description": "Page where the event occurred", "pii": False, "unit": None, "source": "ai"},
        "timestamp": {"semantic_name": "event_timestamp", "domain": "temporal", "description": "When the event occurred", "pii": False, "unit": None, "source": "ai"},
        "browser": {"semantic_name": "browser_type", "domain": "technical", "description": "User browser", "pii": False, "unit": None, "source": "ai"},
        "device": {"semantic_name": "device_type", "domain": "technical", "description": "Device category (desktop/mobile/tablet)", "pii": False, "unit": None, "source": "ai"},
        "country": {"semantic_name": "user_country", "domain": "geography", "description": "User country code", "pii": False, "unit": None, "source": "ai"},
        "campaign": {"semantic_name": "marketing_campaign", "domain": "marketing", "description": "Attribution campaign if any", "pii": False, "unit": None, "source": "ai"},
    },
}

DEMO_BUSINESS_CONTEXT = {
    "demo-ecommerce-orders": {
        "business_process": "Order fulfillment and revenue tracking",
        "consumers": "Finance team, executive dashboards, revenue forecasting models",
        "criticality": "High — daily revenue reporting depends on this data",
        "freshness_expectation": "Near real-time (< 1 hour)",
    },
    "demo-ecommerce-customers": {
        "business_process": "Customer management and segmentation",
        "consumers": "Marketing team, support team, customer analytics",
        "criticality": "Medium — used for weekly segmentation and campaign targeting",
        "freshness_expectation": "Daily",
    },
    "demo-analytics-events": {
        "business_process": "Product analytics and user behavior tracking",
        "consumers": "Product team, growth team, data science",
        "criticality": "Medium — product decisions and A/B test analysis depend on this",
        "freshness_expectation": "Near real-time (< 1 hour)",
    },
}

DEMO_TRUST_WEIGHTS = {
    "demo-ecommerce-orders": {"freshness": 0.40, "quality_gate": 0.25, "error_budget": 0.25, "schema_stability": 0.10},
}


# ---------------------------------------------------------------------------
# Build 29: Demo SQL Transforms — e-commerce analytics DAG
# ---------------------------------------------------------------------------

DEMO_TRANSFORMS = [
    # Layer 1: Daily aggregates
    {
        "transform_name": "daily_revenue",
        "description": "Daily order count, revenue, and average order value",
        "sql": """SELECT
    DATE(created_at) AS revenue_date,
    COUNT(*) AS order_count,
    COUNT(*) FILTER (WHERE status NOT IN ('cancelled')) AS completed_orders,
    SUM(CASE WHEN status != 'cancelled' THEN total ELSE 0 END) AS gross_revenue,
    SUM(CASE WHEN status = 'cancelled' THEN total ELSE 0 END) AS cancelled_revenue,
    ROUND(SUM(CASE WHEN status != 'cancelled' THEN total ELSE 0 END)::numeric
        / NULLIF(COUNT(*) FILTER (WHERE status != 'cancelled'), 0), 2) AS avg_order_value,
    SUM(CASE WHEN status != 'cancelled' THEN tax ELSE 0 END) AS total_tax
FROM {{ ref('demo_orders') }}
WHERE created_at >= NOW() - INTERVAL '{{ var("lookback_days") }} days'
GROUP BY DATE(created_at)
ORDER BY revenue_date""",
        "materialization": "table",
        "target_table": "daily_revenue",
        "variables": {"lookback_days": "365"},
        "refs": ["demo_orders"],
    },
    {
        "transform_name": "daily_active_users",
        "description": "Daily unique users, sessions, and event counts from web analytics",
        "sql": """SELECT
    DATE(timestamp) AS activity_date,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0), 1) AS events_per_user
FROM {{ ref('demo_analytics_events') }}
WHERE timestamp >= NOW() - INTERVAL '{{ var("lookback_days") }} days'
GROUP BY DATE(timestamp)
ORDER BY activity_date""",
        "materialization": "table",
        "target_table": "daily_active_users",
        "variables": {"lookback_days": "365"},
        "refs": ["demo_analytics_events"],
    },
    {
        "transform_name": "daily_funnel",
        "description": "Conversion funnel: page_view -> click -> add_to_cart -> purchase with rates",
        "sql": """SELECT
    DATE(timestamp) AS funnel_date,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS stage_1_views,
    COUNT(*) FILTER (WHERE event_type = 'click') AS stage_2_clicks,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS stage_3_add_to_cart,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS stage_4_purchase,
    ROUND(COUNT(*) FILTER (WHERE event_type = 'purchase')::numeric
        / NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0), 4) AS overall_conversion_rate
FROM {{ ref('demo_analytics_events') }}
WHERE timestamp >= NOW() - INTERVAL '{{ var("lookback_days") }} days'
GROUP BY DATE(timestamp)
ORDER BY funnel_date""",
        "materialization": "table",
        "target_table": "daily_funnel",
        "variables": {"lookback_days": "365"},
        "refs": ["demo_analytics_events"],
    },
    # Layer 2: Enriched / joined
    {
        "transform_name": "customer_orders_summary",
        "description": "Per-customer order stats: lifetime revenue, order count, RFM segment",
        "sql": """SELECT
    c.id AS customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.tier,
    COUNT(o.id) AS total_orders,
    COUNT(o.id) FILTER (WHERE o.status != 'cancelled') AS completed_orders,
    COALESCE(SUM(o.total) FILTER (WHERE o.status != 'cancelled'), 0) AS lifetime_revenue,
    COALESCE(ROUND(AVG(o.total) FILTER (WHERE o.status != 'cancelled'), 2), 0) AS avg_order_value,
    MIN(o.created_at) AS first_order_date,
    MAX(o.created_at) AS last_order_date,
    CASE
        WHEN COUNT(o.id) FILTER (WHERE o.status != 'cancelled') >= 5 THEN 'champion'
        WHEN COUNT(o.id) FILTER (WHERE o.status != 'cancelled') >= 3 THEN 'loyal'
        WHEN COUNT(o.id) FILTER (WHERE o.status != 'cancelled') >= 1 THEN 'active'
        ELSE 'prospect'
    END AS rfm_segment,
    c.created_at AS customer_since
FROM {{ ref('demo_customers') }} c
LEFT JOIN {{ ref('demo_orders') }} o ON o.customer_id = c.id
GROUP BY c.id, c.email, c.first_name, c.last_name, c.tier, c.created_at""",
        "materialization": "incremental",
        "target_table": "customer_orders_summary",
        "variables": {},
        "refs": ["demo_customers", "demo_orders"],
        "unique_key": ["customer_id"],
    },
    {
        "transform_name": "campaign_performance",
        "description": "Marketing campaign metrics: visitors, conversions, attributed revenue",
        "sql": """SELECT
    COALESCE(campaign, '(direct)') AS campaign_name,
    COUNT(DISTINCT user_id) AS unique_visitors,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    ROUND(COUNT(*) FILTER (WHERE event_type = 'purchase')::numeric
        / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS conversion_rate
FROM {{ ref('demo_analytics_events') }}
WHERE timestamp >= NOW() - INTERVAL '{{ var("lookback_days") }} days'
GROUP BY COALESCE(campaign, '(direct)')
ORDER BY purchases DESC""",
        "materialization": "table",
        "target_table": "campaign_performance",
        "variables": {"lookback_days": "365"},
        "refs": ["demo_analytics_events"],
    },
    # Layer 3: Unified views
    {
        "transform_name": "customer_360",
        "description": "Unified customer view joining orders, web activity, and segmentation",
        "sql": """SELECT
    cos.customer_id,
    cos.email,
    cos.first_name,
    cos.last_name,
    cos.tier,
    cos.total_orders,
    cos.completed_orders,
    cos.lifetime_revenue,
    cos.avg_order_value,
    cos.first_order_date,
    cos.last_order_date,
    cos.rfm_segment,
    cos.customer_since,
    COALESCE(ev.total_events, 0) AS total_web_events,
    COALESCE(ev.unique_sessions, 0) AS web_sessions,
    ev.last_event_date,
    ev.primary_device
FROM {{ ref('customer_orders_summary') }} cos
LEFT JOIN (
    SELECT
        user_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS unique_sessions,
        DATE(MAX(timestamp)) AS last_event_date,
        MODE() WITHIN GROUP (ORDER BY device) AS primary_device
    FROM {{ ref('demo_analytics_events') }}
    GROUP BY user_id
) ev ON ev.user_id = 'user_' || cos.customer_id::text""",
        "materialization": "view",
        "target_table": "customer_360",
        "variables": {},
        "refs": ["customer_orders_summary", "demo_analytics_events"],
    },
    {
        "transform_name": "monthly_kpis",
        "description": "Monthly KPIs: revenue, orders, users, conversion rate, MoM growth",
        "sql": """SELECT
    DATE_TRUNC('month', dr.revenue_date)::date AS kpi_month,
    SUM(dr.gross_revenue) AS monthly_revenue,
    SUM(dr.order_count) AS monthly_orders,
    SUM(dr.completed_orders) AS monthly_completed_orders,
    ROUND(SUM(dr.gross_revenue)::numeric / NULLIF(SUM(dr.completed_orders), 0), 2) AS monthly_aov,
    MAX(dau.unique_users) AS peak_daily_users,
    ROUND(AVG(dau.unique_users)::numeric, 0) AS avg_daily_users,
    SUM(dau.total_events) AS monthly_events,
    SUM(df.stage_4_purchase) AS monthly_purchases_from_funnel,
    ROUND(AVG(df.overall_conversion_rate)::numeric, 4) AS avg_conversion_rate
FROM {{ ref('daily_revenue') }} dr
LEFT JOIN {{ ref('daily_active_users') }} dau ON dau.activity_date = dr.revenue_date
LEFT JOIN {{ ref('daily_funnel') }} df ON df.funnel_date = dr.revenue_date
GROUP BY DATE_TRUNC('month', dr.revenue_date)
ORDER BY kpi_month""",
        "materialization": "table",
        "target_table": "monthly_kpis",
        "variables": {},
        "refs": ["daily_revenue", "daily_active_users", "daily_funnel"],
    },
]


# ---------------------------------------------------------------------------
# API-based bootstrap — the real deal
# ---------------------------------------------------------------------------

async def _wait_for_server(base_url: str, timeout: float = 60.0) -> bool:
    """Poll /health until the API server is ready. Returns True on success."""
    deadline = asyncio.get_event_loop().time() + timeout
    async with httpx.AsyncClient() as client:
        while asyncio.get_event_loop().time() < deadline:
            try:
                r = await client.get(f"{base_url}/health", timeout=5.0)
                if r.status_code == 200:
                    log.info("API server is healthy: %s", r.json())
                    return True
            except (httpx.ConnectError, httpx.ReadTimeout):
                pass
            await asyncio.sleep(1.0)
    log.error("API server not healthy within %.0fs", timeout)
    return False


async def _login(client: httpx.AsyncClient, base_url: str) -> str:
    """Login as admin and return JWT token."""
    r = await client.post(f"{base_url}/api/auth/login", json={
        "username": "admin",
        "password": "admin",
    })
    r.raise_for_status()
    token = r.json()["token"]
    log.info("Authenticated as admin for bootstrap.")
    return token


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _get_connectors(client: httpx.AsyncClient, base_url: str, headers: dict) -> dict:
    """Fetch connectors and return name->id mapping."""
    r = await client.get(f"{base_url}/api/connectors", headers=headers)
    r.raise_for_status()
    return {c["connector_name"]: c["connector_id"] for c in r.json()}


async def _create_pipeline_via_api(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    cfg: dict, source_id: str, target_id: str, tgt: dict,
) -> dict | None:
    """Create a pipeline via POST /api/pipelines. Returns pipeline summary or None."""
    payload = {
        "source_connector_id": source_id,
        "source_host": str(cfg["source_host"]),
        "source_port": cfg["source_port"],
        "source_database": cfg["source_database"],
        "source_schema": cfg.get("source_schema", ""),
        "source_table": cfg["source_table"],
        "source_user": cfg.get("source_user", ""),
        "source_password": cfg.get("source_password", ""),
        "target_connector_id": target_id,
        "target_host": str(tgt["host"]),
        "target_port": tgt["port"],
        "target_database": tgt["database"],
        "target_user": tgt["user"],
        "target_password": tgt["password"],
        "target_schema": TARGET_SCHEMA,
        "strategy": {
            "pipeline_name": cfg["pipeline_name"],
            "source_schema": cfg.get("source_schema", ""),
            "source_table": cfg["source_table"],
            "target_table": cfg["target_table"],
            "refresh_type": cfg["refresh_type"],
            "incremental_column": cfg.get("incremental_column", ""),
            "load_type": "append",
        },
        "schedule_cron": cfg["schedule_cron"],
        "tier": 3,
        "tags": {"environment": "demo"},
        "auto_approve_additive": True,
    }
    if "schema_change_policy" in cfg:
        payload["schema_change_policy"] = cfg["schema_change_policy"]
    try:
        r = await client.post(
            f"{base_url}/api/pipelines", json=payload, headers=headers, timeout=30.0,
        )
        r.raise_for_status()
        result = r.json()
        log.info("Created pipeline via API: %s (id=%s)", result.get("pipeline_name", "?"), result.get("pipeline_id", "?"))
        return result
    except Exception as e:
        log.error("Failed to create pipeline %s via API: %s", cfg.get("pipeline_name", "?"), e)
        return None


async def _create_pipeline_via_chat(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    message: str, session_id: str = "demo-bootstrap",
) -> dict | None:
    """Send a natural language command via POST /api/command. Returns response or None."""
    try:
        r = await client.post(
            f"{base_url}/api/command",
            json={"text": message, "session_id": session_id},
            headers=headers,
            timeout=30.0,
        )
        r.raise_for_status()
        result = r.json()
        log.info("Chat command routed: action=%s", result.get("routed_action", "?"))
        return result
    except Exception as e:
        log.error("Chat command failed: %s — %s", message[:60], e)
        return None


async def _set_catalog_metadata(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    pipeline_id: str, pipeline_name: str,
):
    """Set semantic tags, business context, and trust weights via catalog API."""
    # Semantic tags
    if pipeline_name in DEMO_SEMANTIC_TAGS:
        try:
            r = await client.put(
                f"{base_url}/api/catalog/tables/{pipeline_id}/tags",
                json=DEMO_SEMANTIC_TAGS[pipeline_name],
                headers=headers,
            )
            r.raise_for_status()
            log.info("Set %d semantic tags on %s", len(DEMO_SEMANTIC_TAGS[pipeline_name]), pipeline_name)
        except Exception as e:
            log.warning("Could not set semantic tags on %s: %s", pipeline_name, e)

    # Business context
    if pipeline_name in DEMO_BUSINESS_CONTEXT:
        try:
            r = await client.put(
                f"{base_url}/api/catalog/tables/{pipeline_id}/context",
                json=DEMO_BUSINESS_CONTEXT[pipeline_name],
                headers=headers,
            )
            r.raise_for_status()
            log.info("Set business context on %s", pipeline_name)
        except Exception as e:
            log.warning("Could not set business context on %s: %s", pipeline_name, e)

    # Trust weights
    if pipeline_name in DEMO_TRUST_WEIGHTS:
        try:
            r = await client.put(
                f"{base_url}/api/catalog/tables/{pipeline_id}/trust-weights",
                json=DEMO_TRUST_WEIGHTS[pipeline_name],
                headers=headers,
            )
            r.raise_for_status()
            log.info("Set trust weights on %s", pipeline_name)
        except Exception as e:
            log.warning("Could not set trust weights on %s: %s", pipeline_name, e)


async def _create_transforms_via_api(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    pipeline_id: str,
) -> list[dict]:
    """Create all demo transforms via POST /api/transforms. Returns list of created transforms."""
    created = []
    for t_cfg in DEMO_TRANSFORMS:
        payload = {
            "transform_name": t_cfg["transform_name"],
            "description": t_cfg["description"],
            "sql": t_cfg["sql"],
            "materialization": t_cfg["materialization"],
            "target_schema": "analytics",
            "target_table": t_cfg["target_table"],
            "variables": t_cfg.get("variables", {}),
            "refs": t_cfg.get("refs", []),
            "pipeline_id": pipeline_id,
            "approved": True,
        }
        try:
            r = await client.post(
                f"{base_url}/api/transforms", json=payload, headers=headers, timeout=15.0,
            )
            r.raise_for_status()
            result = r.json()
            created.append(result)
            log.info("Created transform via API: %s (id=%s)", t_cfg["transform_name"], result.get("transform_id", "?"))
        except Exception as e:
            log.error("Failed to create transform %s: %s", t_cfg["transform_name"], e)
    return created


async def _create_transform_pipeline_via_api(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    target_connector_id: str, tgt: dict, transform_ids: dict,
) -> dict | None:
    """Create the transform-only pipeline with step DAG via POST /api/pipelines."""
    # Build step definitions with dependency edges
    # Layer 1: independent
    steps = [
        {"step_name": "daily_revenue", "step_type": "transform",
         "config": {"transform_id": transform_ids["daily_revenue"]}},
        {"step_name": "daily_active_users", "step_type": "transform",
         "config": {"transform_id": transform_ids["daily_active_users"]}},
        {"step_name": "daily_funnel", "step_type": "transform",
         "config": {"transform_id": transform_ids["daily_funnel"]}},
    ]
    # Layer 2: depend on nothing (refs resolved at runtime, not via step deps)
    steps.extend([
        {"step_name": "customer_orders_summary", "step_type": "transform",
         "config": {"transform_id": transform_ids["customer_orders_summary"], "unique_key": ["customer_id"]}},
        {"step_name": "campaign_performance", "step_type": "transform",
         "config": {"transform_id": transform_ids["campaign_performance"]}},
    ])
    # Layer 3: depend on Layer 1 + Layer 2 via step_name references
    # We'll use depends_on with step names — the API resolves them
    steps.extend([
        {"step_name": "customer_360", "step_type": "transform",
         "depends_on_names": ["customer_orders_summary"],
         "config": {"transform_id": transform_ids["customer_360"]}},
        {"step_name": "monthly_kpis", "step_type": "transform",
         "depends_on_names": ["daily_revenue", "daily_active_users", "daily_funnel"],
         "config": {"transform_id": transform_ids["monthly_kpis"]}},
    ])

    payload = {
        "source_connector_id": "",  # transform-only pipeline — no source
        "target_connector_id": target_connector_id,
        "target_host": str(tgt["host"]),
        "target_port": tgt["port"],
        "target_database": tgt["database"],
        "target_user": tgt["user"],
        "target_password": tgt["password"],
        "target_schema": "analytics",
        "strategy": {
            "pipeline_name": "demo-analytics-transforms",
            "source_schema": "",
            "source_table": "",
            "target_table": "monthly_kpis",
            "refresh_type": "full",
            "load_type": "append",
        },
        "schedule_cron": "30 * * * *",
        "tier": 2,
        "tags": {"environment": "demo", "type": "transform"},
        "steps": steps,
    }
    try:
        r = await client.post(
            f"{base_url}/api/pipelines", json=payload, headers=headers, timeout=30.0,
        )
        r.raise_for_status()
        result = r.json()
        log.info("Created transform pipeline via API: %s (id=%s)",
                 result.get("pipeline_name", "?"), result.get("pipeline_id", "?"))
        return result
    except Exception as e:
        log.error("Failed to create transform pipeline via API: %s", e)
        return None


async def _trigger_pipeline(
    client: httpx.AsyncClient, base_url: str, headers: dict, pipeline_id: str, name: str,
):
    """Trigger a pipeline run via POST /api/pipelines/{id}/trigger."""
    try:
        r = await client.post(
            f"{base_url}/api/pipelines/{pipeline_id}/trigger", headers=headers, timeout=10.0,
        )
        r.raise_for_status()
        run_id = r.json().get("run_id", "?")
        log.info("Triggered first run for %s (run_id=%s)", name, run_id)
    except Exception as e:
        log.warning("Could not trigger %s: %s", name, e)


# ---------------------------------------------------------------------------
# Build demo pipeline definitions (same configs as before, just as dicts)
# ---------------------------------------------------------------------------

def _build_demo_pipelines(mysql_cfg: dict, mongo_cfg: dict, stripe_url: str) -> list[dict]:
    """Build demo pipeline definitions from resolved source configs."""
    pipelines = [
        {
            "pipeline_name": "demo-ecommerce-orders",
            "source_connector_name": "mysql-source-v1",
            "source_host": mysql_cfg["host"],
            "source_port": mysql_cfg["port"],
            "source_database": mysql_cfg["database"],
            "source_schema": mysql_cfg["database"],
            "source_table": "orders",
            "source_user": mysql_cfg["user"],
            "source_password": mysql_cfg["password"],
            "target_table": "demo_orders",
            "refresh_type": "full",
            "schedule_cron": "0 * * * *",
            "schema_change_policy": {
                "on_new_column": "propose",
                "on_dropped_column": "propose",
                "on_type_change": "propose",
                "on_nullable_change": "auto_accept",
                "propagate_to_downstream": True,
            },
        },
        {
            "pipeline_name": "demo-ecommerce-customers",
            "source_connector_name": "mysql-source-v1",
            "source_host": mysql_cfg["host"],
            "source_port": mysql_cfg["port"],
            "source_database": mysql_cfg["database"],
            "source_schema": mysql_cfg["database"],
            "source_table": "customers",
            "source_user": mysql_cfg["user"],
            "source_password": mysql_cfg["password"],
            "target_table": "demo_customers",
            "refresh_type": "incremental",
            "incremental_column": "updated_at",
            "schedule_cron": "0 * * * *",
        },
        {
            "pipeline_name": "demo-analytics-events",
            "source_connector_name": "mongo-source-v1",
            "source_host": mongo_cfg["url"] or mongo_cfg["host"],
            "source_port": mongo_cfg["port"],
            "source_database": mongo_cfg["database"],
            "source_schema": "default",
            "source_table": "events",
            "target_table": "demo_events",
            "refresh_type": "full",
            "schedule_cron": "0 * * * *",
        },
    ]

    # Only include Stripe demo if mock API is available
    if stripe_url:
        pipelines.append({
            "pipeline_name": "demo-stripe-charges",
            "source_connector_name": "stripe-source-v1",
            "source_host": stripe_url,
            "source_port": 0,
            "source_database": "sk_demo_key",
            "source_schema": "stripe",
            "source_table": "charges",
            "target_table": "demo_stripe_charges",
            "refresh_type": "full",
            "schedule_cron": "0 * * * *",
        })

    return pipelines


# ---------------------------------------------------------------------------
# Main entry point — called as background task from main.py
# ---------------------------------------------------------------------------

async def bootstrap_demo_pipelines(port: int = 8100) -> None:
    """Bootstrap demo environment via the real DAPOS API.

    Waits for the API server to be healthy, then creates demo pipelines,
    transforms, and catalog metadata through the same endpoints a user would use.
    This validates the full platform stack on every fresh deployment.
    """
    from config import Config
    config = Config()
    base_url = f"http://127.0.0.1:{port}"

    # Wait for server to be ready
    if not await _wait_for_server(base_url, timeout=90.0):
        return

    async with httpx.AsyncClient() as client:
        # Authenticate
        try:
            token = await _login(client, base_url)
        except Exception as e:
            log.error("Bootstrap login failed: %s", e)
            return
        headers = _auth_headers(token)

        # Check if pipelines already exist (idempotent guard)
        r = await client.get(f"{base_url}/health")
        health = r.json()
        existing_count = health.get("pipelines", 0)

        # Check if transforms already exist
        r = await client.get(f"{base_url}/api/transforms", headers=headers)
        existing_transforms = r.json() if r.status_code == 200 else []

        if existing_count > 0 and existing_transforms:
            # Check if transform pipeline exists
            r = await client.get(f"{base_url}/api/pipelines", headers=headers)
            pipelines = r.json() if r.status_code == 200 else []
            has_transform_pipeline = any(
                p.get("pipeline_name") == "demo-analytics-transforms" for p in pipelines
            )
            if has_transform_pipeline:
                log.info("Demo already bootstrapped (%d pipelines, %d transforms). Skipping.",
                         existing_count, len(existing_transforms))
                return
            # Transforms exist but pipeline missing — clean up orphans
            log.warning("Found %d orphaned transforms (no pipeline), cleaning up...", len(existing_transforms))
            for t in existing_transforms:
                tid = t.get("transform_id", "")
                if tid:
                    await client.delete(f"{base_url}/api/transforms/{tid}", headers=headers)

        if existing_count > 0:
            log.info("Pipelines exist (%d), skipping pipeline creation. Checking transforms...", existing_count)
            # Still need to create transforms if they don't exist
            if not existing_transforms:
                # Check if the transform pipeline already exists
                r = await client.get(f"{base_url}/api/pipelines", headers=headers)
                pipelines = r.json() if r.status_code == 200 else []
                existing_tp = next(
                    (p for p in pipelines if p.get("pipeline_name") == "demo-analytics-transforms"), None,
                )
                await _bootstrap_transforms(client, base_url, headers, existing_pipeline=existing_tp)
            return

        # --- Seed source data (MySQL, MongoDB) ---
        mysql_cfg = _mysql_config()
        mongo_cfg = _mongo_config()
        await _seed_mysql(mysql_cfg)
        await _seed_mongo(mongo_cfg)

        # Resolve connector IDs
        connectors = await _get_connectors(client, base_url, headers)
        target_id = connectors.get(TARGET_CONNECTOR_NAME)
        if not target_id:
            log.warning("Target connector '%s' not found, skipping demo bootstrap.", TARGET_CONNECTOR_NAME)
            return

        # Determine Stripe mock availability
        stripe_url = os.getenv("DEMO_STRIPE_API_URL", "")
        if not stripe_url and not os.getenv("DEMO_MYSQL_URL", ""):
            stripe_url = "http://localhost:8200"

        demo_pipelines = _build_demo_pipelines(mysql_cfg, mongo_cfg, stripe_url)
        tgt = _target_config()

        # --- Create pipelines via API ---
        created_pipelines: list[dict] = []
        for i, cfg in enumerate(demo_pipelines):
            source_id = connectors.get(cfg["source_connector_name"])
            if not source_id:
                log.warning("Source connector '%s' not found, skipping pipeline '%s'.",
                            cfg["source_connector_name"], cfg["pipeline_name"])
                continue

            if i == 0:
                # First pipeline: test the chat interface (validates routing + guided flow)
                log.info("Creating first pipeline via chat to validate agent routing...")
                chat_result = await _create_pipeline_via_chat(
                    client, base_url, headers,
                    f"list connectors",
                    session_id="demo-bootstrap",
                )
                if chat_result:
                    log.info("Chat routing validated: %s", chat_result.get("routed_action", "?"))

            # All pipelines created via REST API (exercises conversation.create_pipeline)
            result = await _create_pipeline_via_api(
                client, base_url, headers, cfg, source_id, target_id, tgt,
            )
            if result:
                created_pipelines.append(result)

        log.info("Demo bootstrap: %d pipelines created via API.", len(created_pipelines))

        # --- Set catalog metadata via API ---
        for p in created_pipelines:
            pid = p.get("pipeline_id", "")
            pname = p.get("pipeline_name", "")
            if pid and pname:
                await _set_catalog_metadata(client, base_url, headers, pid, pname)

        # --- Trigger first runs via API ---
        for p in created_pipelines:
            pid = p.get("pipeline_id", "")
            pname = p.get("pipeline_name", "")
            if pid:
                await _trigger_pipeline(client, base_url, headers, pid, pname)

        # --- Create transforms via API ---
        await _bootstrap_transforms(client, base_url, headers)

        # --- Validate via chat: ask agent about what we just created ---
        chat_result = await _create_pipeline_via_chat(
            client, base_url, headers,
            "How many pipelines do we have? List them.",
            session_id="demo-bootstrap-validate",
        )
        if chat_result:
            log.info("Bootstrap validation via chat: %s",
                     chat_result.get("agent_response", "")[:200])

        log.info("Demo bootstrap complete via API.")


async def _bootstrap_transforms(
    client: httpx.AsyncClient, base_url: str, headers: dict,
    existing_pipeline: dict | None = None,
):
    """Create demo transforms and transform pipeline via API."""
    tgt = _target_config()

    # Get connectors to find target ID
    connectors = await _get_connectors(client, base_url, headers)
    target_id = connectors.get(TARGET_CONNECTOR_NAME)
    if not target_id:
        log.warning("Target connector not found, skipping transform bootstrap.")
        return

    # Create transforms via API (will be associated with pipeline after creation)
    created_transforms = await _create_transforms_via_api(client, base_url, headers, pipeline_id="")
    if not created_transforms:
        log.warning("No transforms created, skipping transform pipeline.")
        return

    # Build transform_name -> transform_id mapping
    transform_ids = {t["transform_name"]: t["transform_id"] for t in created_transforms}

    # Use existing transform pipeline or create a new one
    if existing_pipeline:
        tp_id = existing_pipeline.get("pipeline_id", "")
        log.info("Using existing transform pipeline: %s", tp_id[:8])
    else:
        tp_result = await _create_transform_pipeline_via_api(
            client, base_url, headers, target_id, tgt, transform_ids,
        )
        if not tp_result:
            return
        tp_id = tp_result.get("pipeline_id", "")

    # Update transforms with pipeline_id via PATCH
    for t in created_transforms:
        tid = t.get("transform_id", "")
        if tid and tp_id:
            try:
                r = await client.patch(
                    f"{base_url}/api/transforms/{tid}",
                    json={"pipeline_id": tp_id},
                    headers=headers,
                )
                r.raise_for_status()
            except Exception as e:
                log.warning("Could not link transform %s to pipeline: %s", t.get("transform_name", "?"), e)

    # Set catalog metadata on transform pipeline
    await _set_catalog_metadata(client, base_url, headers, tp_id, "demo-analytics-transforms")

    # Add transform-specific metadata
    try:
        r = await client.put(
            f"{base_url}/api/catalog/tables/{tp_id}/context",
            json={
                "business_process": "E-commerce analytics and executive reporting",
                "consumers": "Executive team, product managers, marketing, data science",
                "criticality": "High — weekly business reviews and board reporting depend on this",
                "freshness_expectation": "Daily",
            },
            headers=headers,
        )
        r.raise_for_status()
    except Exception:
        pass

    log.info("Transform bootstrap complete: %d transforms, 1 transform pipeline.", len(created_transforms))
