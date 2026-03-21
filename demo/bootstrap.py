"""
Demo pipeline bootstrap -- auto-creates demo pipelines on first startup.

Only runs if no pipelines exist yet (first startup guard).
Supports both local Docker Compose and cloud (Railway/Render) deployments.
Cloud: set DEMO_MYSQL_URL and DEMO_MONGO_URL env vars; data is seeded automatically.
"""
from __future__ import annotations
import asyncio
import logging
import os
import random
import math
from datetime import datetime, timedelta
from urllib.parse import urlparse

from contracts.models import (
    PipelineContract, RunRecord, RunMode, PipelineStatus, RefreshType, ReplicationMethod, LoadType,
    NotificationPolicy,
)
from contracts.store import ContractStore
from connectors.registry import ConnectorRegistry

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


def _stripe_mock_available():
    """Check if DEMO_STRIPE_API_URL is set or default mock is likely reachable."""
    return bool(os.getenv("DEMO_STRIPE_API_URL", "")) or os.getenv("DEMO_MYSQL_URL", "") == ""


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
# Pipeline definitions (built dynamically from resolved configs)
# ---------------------------------------------------------------------------

TARGET_CONNECTOR_NAME = "postgres-target-v1"
TARGET_SCHEMA = "raw"


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
            "refresh_type": RefreshType.FULL,
            "schedule_cron": "0 * * * *",
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
            "refresh_type": RefreshType.INCREMENTAL,
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
            "refresh_type": RefreshType.FULL,
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
            "refresh_type": RefreshType.FULL,
            "schedule_cron": "0 * * * *",
        })

    return pipelines


# ---------------------------------------------------------------------------
# Main bootstrap
# ---------------------------------------------------------------------------

async def bootstrap_demo_pipelines(store: ContractStore, registry: ConnectorRegistry, runner=None) -> None:
    """Create demo pipelines if none exist yet.

    Profiles each source table to populate column_mappings so the target
    DDL is correct and pipeline execution works end-to-end.
    If runner is provided, triggers all created pipelines immediately.
    """
    existing = await store.list_pipelines()
    if existing:
        log.info("Pipelines already exist (%d), skipping demo bootstrap.", len(existing))
        return

    _tgt = _target_config()
    mysql_cfg = _mysql_config()
    mongo_cfg = _mongo_config()

    # Seed demo data into source databases (idempotent — skips if data exists)
    mysql_ok = await _seed_mysql(mysql_cfg)
    mongo_ok = await _seed_mongo(mongo_cfg)

    if not mysql_ok:
        log.warning("MySQL demo source not available — MySQL pipelines will be created but may fail on first run.")
    if not mongo_ok:
        log.warning("MongoDB demo source not available — MongoDB pipeline will be created but may fail on first run.")

    # Determine Stripe mock availability
    stripe_url = os.getenv("DEMO_STRIPE_API_URL", "")
    if not stripe_url and not os.getenv("DEMO_MYSQL_URL", ""):
        # Local Docker mode — assume mock API is running
        stripe_url = "http://localhost:8200"
    # In cloud mode without DEMO_STRIPE_API_URL, skip Stripe pipeline

    demo_pipelines = _build_demo_pipelines(mysql_cfg, mongo_cfg, stripe_url)

    # Resolve connector IDs by name
    connectors = await store.list_connectors(status="active")
    name_to_id = {c.connector_name: c.connector_id for c in connectors}

    target_id = name_to_id.get(TARGET_CONNECTOR_NAME)
    if not target_id:
        log.warning("Target connector '%s' not found, skipping demo bootstrap.", TARGET_CONNECTOR_NAME)
        return

    created_pipelines: list[PipelineContract] = []
    created = 0
    for cfg in demo_pipelines:
        source_id = name_to_id.get(cfg["source_connector_name"])
        if not source_id:
            log.warning("Source connector '%s' not found, skipping pipeline '%s'.",
                        cfg["source_connector_name"], cfg["pipeline_name"])
            continue

        # Profile the source table to get column mappings
        column_mappings = []
        try:
            src_params = {
                "host": cfg["source_host"],
                "port": cfg["source_port"],
                "database": cfg["source_database"],
                "user": cfg.get("source_user", ""),
                "password": cfg.get("source_password", ""),
            }
            source = await registry.get_source(source_id, src_params)
            profile = await source.profile_table(cfg["source_schema"], cfg["source_table"])
            column_mappings = profile.columns
            log.info("Profiled %s.%s: %d columns, ~%d rows",
                     cfg["source_schema"], cfg["source_table"],
                     len(column_mappings), profile.row_count_estimate)
        except Exception as e:
            log.warning("Could not profile %s.%s: %s (pipeline will be created without column mappings)",
                        cfg["source_schema"], cfg["source_table"], e)

        contract = PipelineContract(
            pipeline_name=cfg["pipeline_name"],
            status=PipelineStatus.ACTIVE,
            # Source
            source_connector_id=source_id,
            source_host=cfg["source_host"],
            source_port=cfg["source_port"],
            source_database=cfg["source_database"],
            source_schema=cfg.get("source_schema", ""),
            source_table=cfg["source_table"],
            source_user=cfg.get("source_user", ""),
            source_password=cfg.get("source_password", ""),
            # Target
            target_connector_id=target_id,
            target_host=_tgt["host"],
            target_port=_tgt["port"],
            target_database=_tgt["database"],
            target_user=_tgt["user"],
            target_password=_tgt["password"],
            target_schema=TARGET_SCHEMA,
            target_table=cfg["target_table"],
            # Schema
            column_mappings=column_mappings,
            # Strategy
            refresh_type=cfg["refresh_type"],
            incremental_column=cfg.get("incremental_column"),
            load_type=LoadType.APPEND,
            # Schedule
            schedule_cron=cfg["schedule_cron"],
            # Observability
            tier=3,
            tier_config={"digest_only": False},
            tags={"environment": "demo"},
            # Schema drift auto-remediation
            auto_approve_additive_schema=True,
        )
        await store.save_pipeline(contract)
        created_pipelines.append(contract)
        created += 1
        log.info("Created demo pipeline: %s", cfg["pipeline_name"])

    log.info("Demo bootstrap complete: %d pipelines created.", created)

    # Create demo notification policy (skip Slack webhook in cloud — no mock API)
    if created_pipelines:
        slack_target = os.getenv("DEMO_STRIPE_API_URL", "http://localhost:8200") + "/webhook/slack"
        policy = NotificationPolicy(
            policy_name="demo-slack-alerts",
            description="Demo notification policy routing alerts to mock Slack webhook",
            channels=[
                {
                    "type": "slack",
                    "target": slack_target,
                    "severity_filter": ["info", "warning", "critical"],
                },
            ],
        )
        await store.save_policy(policy)
        log.info("Created demo notification policy: %s", policy.policy_name)

        # Wire all demo pipelines to the notification policy
        for pipeline in created_pipelines:
            pipeline.notification_policy_id = policy.policy_id
            await store.save_pipeline(pipeline)
        log.info("Wired %d demo pipelines to notification policy.", len(created_pipelines))

    # Trigger all created pipelines immediately (don't wait for scheduler cron)
    if runner and created_pipelines:
        for pipeline in created_pipelines:
            try:
                run = RunRecord(
                    pipeline_id=pipeline.pipeline_id,
                    run_mode=RunMode.MANUAL,
                )
                asyncio.create_task(runner.execute(pipeline, run))
                log.info("Triggered first run for: %s", pipeline.pipeline_name)
            except Exception as e:
                log.warning("Could not trigger %s: %s", pipeline.pipeline_name, e)
