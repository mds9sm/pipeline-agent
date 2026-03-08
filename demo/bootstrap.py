"""
Demo pipeline bootstrap -- auto-creates demo pipelines on first startup.

Only runs if no pipelines exist yet (first startup guard).
Requires demo Docker services (demo-mysql, demo-mongo, demo-api) to be running.
"""
from __future__ import annotations
import logging

from contracts.models import (
    PipelineContract, PipelineStatus, RefreshType, ReplicationMethod, LoadType,
)
from contracts.store import ContractStore
from connectors.registry import ConnectorRegistry

log = logging.getLogger("demo.bootstrap")

# Demo pipeline definitions
DEMO_PIPELINES = [
    {
        "pipeline_name": "demo-ecommerce-orders",
        "source_connector_name": "mysql-source-v1",
        "source_host": "localhost",
        "source_port": 3307,
        "source_database": "demo_ecommerce",
        "source_schema": "demo_ecommerce",
        "source_table": "orders",
        "target_table": "demo_orders",
        "refresh_type": RefreshType.FULL,
        "schedule_cron": "0 * * * *",
    },
    {
        "pipeline_name": "demo-ecommerce-customers",
        "source_connector_name": "mysql-source-v1",
        "source_host": "localhost",
        "source_port": 3307,
        "source_database": "demo_ecommerce",
        "source_schema": "demo_ecommerce",
        "source_table": "customers",
        "target_table": "demo_customers",
        "refresh_type": RefreshType.INCREMENTAL,
        "incremental_column": "updated_at",
        "schedule_cron": "0 * * * *",
    },
    {
        "pipeline_name": "demo-analytics-events",
        "source_connector_name": "mongo-source-v1",
        "source_host": "localhost",
        "source_port": 27018,
        "source_database": "demo_analytics",
        "source_schema": "default",
        "source_table": "events",
        "target_table": "demo_events",
        "refresh_type": RefreshType.FULL,
        "schedule_cron": "0 * * * *",
    },
    {
        "pipeline_name": "demo-stripe-charges",
        "source_connector_name": "stripe-source-v1",
        "source_host": "http://localhost:8200",
        "source_port": 0,
        "source_database": "sk_demo_key",
        "source_schema": "stripe",
        "source_table": "charges",
        "target_table": "demo_stripe_charges",
        "refresh_type": RefreshType.FULL,
        "schedule_cron": "0 * * * *",
    },
]

# Target config -- all demo pipelines land in the local PostgreSQL
TARGET_CONNECTOR_NAME = "postgres-target-v1"
TARGET_HOST = "localhost"
TARGET_PORT = 5432
TARGET_DATABASE = "pipeline_agent"
TARGET_USER = "pipeline_agent"
TARGET_PASSWORD = "pipeline_agent"
TARGET_SCHEMA = "raw"


async def bootstrap_demo_pipelines(store: ContractStore, registry: ConnectorRegistry) -> None:
    """Create demo pipelines if none exist yet.

    Profiles each source table to populate column_mappings so the target
    DDL is correct and pipeline execution works end-to-end.
    """
    existing = await store.list_pipelines()
    if existing:
        log.info("Pipelines already exist (%d), skipping demo bootstrap.", len(existing))
        return

    # Resolve connector IDs by name
    connectors = await store.list_connectors(status="active")
    name_to_id = {c.connector_name: c.connector_id for c in connectors}

    target_id = name_to_id.get(TARGET_CONNECTOR_NAME)
    if not target_id:
        log.warning("Target connector '%s' not found, skipping demo bootstrap.", TARGET_CONNECTOR_NAME)
        return

    created = 0
    for cfg in DEMO_PIPELINES:
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
            # Target
            target_connector_id=target_id,
            target_host=TARGET_HOST,
            target_port=TARGET_PORT,
            target_database=TARGET_DATABASE,
            target_user=TARGET_USER,
            target_password=TARGET_PASSWORD,
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
            tags={"environment": "demo"},
        )
        await store.save_pipeline(contract)
        created += 1
        log.info("Created demo pipeline: %s", cfg["pipeline_name"])

    log.info("Demo bootstrap complete: %d pipelines created.", created)
