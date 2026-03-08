"""Shared fixtures for pipeline-agent test suite."""

from __future__ import annotations

import os
import sys
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

# Ensure the project root is on sys.path so bare imports work.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import Config
from contracts.models import (
    ConnectorRecord,
    ConnectorStatus,
    ConnectorType,
    ColumnMapping,
    PipelineContract,
    PipelineStatus,
    RefreshType,
    ReplicationMethod,
    LoadType,
    RunRecord,
    RunMode,
    RunStatus,
    QualityConfig,
    TestStatus,
    new_id,
    now_iso,
)


# ======================================================================
# Config
# ======================================================================


@pytest.fixture
def config() -> Config:
    """Config with test defaults: auth disabled, test DB, generated key."""
    from crypto import generate_key

    cfg = Config()
    cfg.pg_host = "localhost"
    cfg.pg_port = 5432
    cfg.pg_database = "pipeline_agent_test"
    cfg.pg_user = "pipeline_agent"
    cfg.pg_password = "pipeline_agent"
    cfg.auth_enabled = False
    cfg.jwt_secret = "test-secret-key-for-jwt"
    cfg.jwt_algorithm = "HS256"
    cfg.jwt_expiry_hours = 24
    cfg.encryption_key = generate_key()
    cfg.data_dir = "/tmp/pipeline_agent_test"
    cfg.max_disk_pct = 95.0
    cfg.batch_size = 1000
    cfg.max_concurrent = 2
    cfg.slack_webhook = ""
    cfg.email_smtp_host = ""
    cfg.email_from = ""
    cfg.pagerduty_key = ""
    return cfg


# ======================================================================
# PostgreSQL pool (session-scoped, requires running PG)
# ======================================================================

PG_TEST_DSN = os.getenv(
    "PG_TEST_DSN",
    "postgresql://pipeline_agent:pipeline_agent@localhost:5432/pipeline_agent_test",
)


@pytest_asyncio.fixture(scope="session")
async def pg_pool():
    """Create an asyncpg pool to the test database, yield it, then close."""
    import asyncpg

    try:
        pool = await asyncpg.create_pool(PG_TEST_DSN, min_size=1, max_size=5)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
        return

    yield pool
    await pool.close()


# ======================================================================
# ContractStore (function-scoped -- clean slate per test)
# ======================================================================

@pytest_asyncio.fixture
async def store(pg_pool):
    """Initialize ContractStore, create tables, yield, then drop all tables."""
    from contracts.store import ContractStore

    s = ContractStore()
    await s.initialize(pg_pool)
    await s.create_tables()
    yield s

    # Teardown: drop every table to ensure isolation
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables
                          WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS public.' ||
                            quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)


# ======================================================================
# Sample data fixtures
# ======================================================================

@pytest.fixture
def sample_connector() -> ConnectorRecord:
    return ConnectorRecord(
        connector_id=new_id(),
        connector_name=f"test-connector-{uuid.uuid4().hex[:8]}",
        connector_type=ConnectorType.SOURCE,
        source_target_type="mysql",
        version=1,
        generated_by="test",
        interface_version="1.0",
        code="class TestSource: pass",
        dependencies=[],
        test_status=TestStatus.PASSED,
        test_results={"tests": 1, "passed": 1},
        generation_attempts=0,
        generation_log=[],
        status=ConnectorStatus.ACTIVE,
        approved_by="test-user",
        approved_at=now_iso(),
    )


@pytest.fixture
def sample_pipeline(sample_connector) -> PipelineContract:
    src_id = sample_connector.connector_id
    tgt_id = new_id()
    return PipelineContract(
        pipeline_id=new_id(),
        pipeline_name=f"test-pipeline-{uuid.uuid4().hex[:8]}",
        version=1,
        status=PipelineStatus.ACTIVE,
        environment="test",
        source_connector_id=src_id,
        source_host="localhost",
        source_port=3306,
        source_database="testdb",
        source_schema="public",
        source_table="orders",
        target_connector_id=tgt_id,
        target_schema="raw",
        target_table="orders",
        refresh_type=RefreshType.INCREMENTAL,
        replication_method=ReplicationMethod.WATERMARK,
        incremental_column="updated_at",
        load_type=LoadType.MERGE,
        merge_keys=["id"],
        schedule_cron="0 * * * *",
        column_mappings=[
            ColumnMapping(
                source_column="id",
                source_type="INT",
                target_column="id",
                target_type="INTEGER",
                is_nullable=False,
                is_primary_key=True,
                is_incremental_candidate=False,
                ordinal_position=1,
            ),
            ColumnMapping(
                source_column="amount",
                source_type="DECIMAL(10,2)",
                target_column="amount",
                target_type="NUMERIC(10,2)",
                is_nullable=True,
                is_primary_key=False,
                is_incremental_candidate=False,
                ordinal_position=2,
            ),
            ColumnMapping(
                source_column="updated_at",
                source_type="TIMESTAMP",
                target_column="updated_at",
                target_type="TIMESTAMPTZ",
                is_nullable=True,
                is_primary_key=False,
                is_incremental_candidate=True,
                ordinal_position=3,
            ),
        ],
        quality_config=QualityConfig(),
    )


@pytest.fixture
def sample_run(sample_pipeline) -> RunRecord:
    return RunRecord(
        run_id=new_id(),
        pipeline_id=sample_pipeline.pipeline_id,
        status=RunStatus.PENDING,
        run_mode=RunMode.SCHEDULED,
        rows_extracted=1000,
    )


# ======================================================================
# Mock source / target engines
# ======================================================================

@pytest.fixture
def mock_source():
    """Mock SourceEngine with configurable return values."""
    source = MagicMock()
    source.extract = AsyncMock()
    source.profile_table = AsyncMock()
    source.test_connection = AsyncMock(return_value=True)
    return source


@pytest.fixture
def mock_target():
    """Mock TargetEngine with configurable return values for quality gate."""
    target = MagicMock()
    # Sync methods used by quality gate
    target.get_row_count = MagicMock(return_value=1000)
    target.get_column_types = MagicMock(return_value=[
        {"column_name": "id", "data_type": "INTEGER"},
        {"column_name": "amount", "data_type": "NUMERIC(10,2)"},
        {"column_name": "updated_at", "data_type": "TIMESTAMPTZ"},
        {"column_name": "_extracted_at", "data_type": "TIMESTAMPTZ"},
        {"column_name": "_source_schema", "data_type": "VARCHAR(255)"},
        {"column_name": "_source_table", "data_type": "VARCHAR(255)"},
        {"column_name": "_row_hash", "data_type": "VARCHAR(64)"},
    ])
    target.check_duplicates = MagicMock(return_value=0)
    target.get_null_rates = MagicMock(return_value={
        "id": 0.0,
        "amount": 0.02,
        "updated_at": 0.01,
    })
    target.get_cardinality = MagicMock(return_value={
        "id": 1000,
        "amount": 500,
        "updated_at": 800,
    })
    target.get_max_value = MagicMock(return_value=None)
    target.default_schema = "raw"

    # Async methods used by pipeline runner
    target.create_table_if_not_exists = AsyncMock()
    target.load_staging = AsyncMock()
    target.promote = AsyncMock()
    target.drop_staging = AsyncMock()
    return target
