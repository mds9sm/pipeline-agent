"""Tests for contracts/store.py -- async PostgreSQL persistence layer.

These tests require a running PostgreSQL instance.  They are skipped
automatically when PG_TEST_DSN is not reachable.
"""

from __future__ import annotations

import os
import sys
import uuid

import pytest
import pytest_asyncio

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from contracts.models import (
    ConnectorRecord, ConnectorType, ConnectorStatus, TestStatus,
    PipelineContract, PipelineStatus, RefreshType, ReplicationMethod,
    LoadType, ColumnMapping, QualityConfig,
    RunRecord, RunMode, RunStatus,
    ErrorBudget, ColumnLineage, AgentCostLog, User,
    AgentPreference, PreferenceScope, PreferenceSource,
    ContractChangeProposal, ProposalStatus, TriggerType, ChangeType,
    new_id, now_iso,
)

# All tests in this module need PG
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.getenv("SKIP_PG_TESTS", "1") == "1",
        reason="Set SKIP_PG_TESTS=0 and ensure PG is available to run store tests",
    ),
]


# ------------------------------------------------------------------
# Helper to build a minimal connector + pipeline pair (pipelines FK
# to connectors).
# ------------------------------------------------------------------

def _make_connector(name_suffix: str = "") -> ConnectorRecord:
    return ConnectorRecord(
        connector_id=new_id(),
        connector_name=f"store-test-{uuid.uuid4().hex[:8]}{name_suffix}",
        connector_type=ConnectorType.SOURCE,
        source_target_type="mysql",
        version=1,
        generated_by="test",
        interface_version="1.0",
        code="class X: pass",
        status=ConnectorStatus.ACTIVE,
        approved_by="tester",
        approved_at=now_iso(),
    )


def _make_pipeline(src_connector_id: str) -> PipelineContract:
    return PipelineContract(
        pipeline_id=new_id(),
        pipeline_name=f"store-pipe-{uuid.uuid4().hex[:8]}",
        version=1,
        status=PipelineStatus.ACTIVE,
        source_connector_id=src_connector_id,
        source_host="localhost",
        source_port=3306,
        source_database="testdb",
        source_schema="public",
        source_table="orders",
        target_connector_id=src_connector_id,
        target_schema="raw",
        target_table="orders",
        refresh_type=RefreshType.FULL,
        replication_method=ReplicationMethod.WATERMARK,
        load_type=LoadType.APPEND,
        column_mappings=[
            ColumnMapping(
                source_column="id", source_type="INT",
                target_column="id", target_type="INTEGER",
                is_nullable=False, is_primary_key=True,
                is_incremental_candidate=False, ordinal_position=1,
            ),
        ],
        quality_config=QualityConfig(),
    )


# ======================================================================
# Connector tests
# ======================================================================


class TestConnectorStore:

    async def test_save_and_get_connector(self, store):
        c = _make_connector()
        await store.save_connector(c)
        fetched = await store.get_connector(c.connector_id)
        assert fetched is not None
        assert fetched.connector_id == c.connector_id
        assert fetched.connector_name == c.connector_name
        assert fetched.connector_type == ConnectorType.SOURCE
        assert fetched.status == ConnectorStatus.ACTIVE

    async def test_list_connectors_filter(self, store):
        c1 = _make_connector("-src")
        c1.connector_type = ConnectorType.SOURCE
        c1.status = ConnectorStatus.ACTIVE
        await store.save_connector(c1)

        c2 = _make_connector("-tgt")
        c2.connector_type = ConnectorType.TARGET
        c2.status = ConnectorStatus.DRAFT
        await store.save_connector(c2)

        active_sources = await store.list_connectors(
            connector_type="source", status="active",
        )
        assert any(c.connector_id == c1.connector_id for c in active_sources)
        assert not any(c.connector_id == c2.connector_id for c in active_sources)


# ======================================================================
# Pipeline tests
# ======================================================================


class TestPipelineStore:

    async def test_save_and_get_pipeline(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        fetched = await store.get_pipeline(p.pipeline_id)
        assert fetched is not None
        assert fetched.pipeline_name == p.pipeline_name
        assert len(fetched.column_mappings) == 1
        assert fetched.column_mappings[0].source_column == "id"

    async def test_upsert_pipeline(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        p.version = 2
        p.pipeline_name = p.pipeline_name  # keep same name
        await store.save_pipeline(p)

        all_pipes = await store.list_pipelines()
        matching = [x for x in all_pipes if x.pipeline_id == p.pipeline_id]
        assert len(matching) == 1
        assert matching[0].version == 2


# ======================================================================
# Run tests
# ======================================================================


class TestRunStore:

    async def test_save_and_list_runs(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        runs = []
        for i in range(5):
            r = RunRecord(
                pipeline_id=p.pipeline_id,
                run_mode=RunMode.SCHEDULED,
                rows_extracted=100 * (i + 1),
                status=RunStatus.COMPLETE,
            )
            await store.save_run(r)
            runs.append(r)

        fetched = await store.list_runs(p.pipeline_id, limit=3)
        assert len(fetched) == 3

    async def test_get_volume_baseline(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        for count in [100, 200, 300, 400, 500]:
            r = RunRecord(
                pipeline_id=p.pipeline_id,
                run_mode=RunMode.SCHEDULED,
                rows_extracted=count,
                status=RunStatus.COMPLETE,
            )
            await store.save_run(r)

        baseline = await store.get_volume_baseline(p.pipeline_id, window=10)
        assert len(baseline) == 5
        assert all(isinstance(v, int) for v in baseline)


# ======================================================================
# Error budget tests
# ======================================================================


class TestErrorBudgetStore:

    async def test_save_and_get_error_budget(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        budget = ErrorBudget(
            pipeline_id=p.pipeline_id,
            window_days=7,
            total_runs=10,
            successful_runs=9,
            failed_runs=1,
            success_rate=0.9,
            budget_threshold=0.9,
            budget_remaining=0.0,
            escalated=False,
        )
        await store.save_error_budget(budget)

        fetched = await store.get_error_budget(p.pipeline_id)
        assert fetched is not None
        assert fetched.total_runs == 10
        assert fetched.success_rate == 0.9


# ======================================================================
# Column lineage tests
# ======================================================================


class TestColumnLineageStore:

    async def test_save_and_list_column_lineage(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        cl = ColumnLineage(
            source_pipeline_id=p.pipeline_id,
            source_schema="public",
            source_table="orders",
            source_column="id",
            target_pipeline_id=p.pipeline_id,
            target_schema="raw",
            target_table="orders",
            target_column="id",
            transformation="direct",
        )
        await store.save_column_lineage(cl)

        lineage_list = await store.list_column_lineage(p.pipeline_id)
        assert len(lineage_list) >= 1
        assert lineage_list[0].source_column == "id"

    async def test_get_downstream_columns(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        cl = ColumnLineage(
            source_pipeline_id=p.pipeline_id,
            source_schema="public",
            source_table="orders",
            source_column="amount",
            target_pipeline_id=p.pipeline_id,
            target_schema="analytics",
            target_table="revenue",
            target_column="total_amount",
            transformation="SUM",
        )
        await store.save_column_lineage(cl)

        downstream = await store.get_downstream_columns(
            p.pipeline_id, "amount",
        )
        assert len(downstream) >= 1
        assert downstream[0].target_column == "total_amount"


# ======================================================================
# Agent cost logs tests
# ======================================================================


class TestAgentCostStore:

    async def test_save_and_list_agent_costs(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        for op in ["drift_analysis", "connector_gen", "drift_analysis"]:
            entry = AgentCostLog(
                pipeline_id=p.pipeline_id,
                operation=op,
                model="claude-sonnet-4-6",
                input_tokens=500,
                output_tokens=200,
                total_tokens=700,
                latency_ms=1200,
            )
            await store.save_agent_cost(entry)

        costs = await store.list_agent_costs(p.pipeline_id, hours=1)
        assert len(costs) == 3

    async def test_get_total_cost_summary(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        for op in ["drift_analysis", "connector_gen"]:
            entry = AgentCostLog(
                pipeline_id=p.pipeline_id,
                operation=op,
                model="claude-sonnet-4-6",
                input_tokens=1000,
                output_tokens=500,
                total_tokens=1500,
                latency_ms=800,
            )
            await store.save_agent_cost(entry)

        summary = await store.get_total_cost_summary(hours=1)
        assert summary["call_count"] >= 2
        assert summary["total_tokens"] >= 3000


# ======================================================================
# User tests
# ======================================================================


class TestUserStore:

    async def test_save_and_get_user(self, store):
        u = User(
            username=f"testuser-{uuid.uuid4().hex[:8]}",
            password_hash="hashed_pw",
            role="admin",
        )
        await store.save_user(u)

        fetched = await store.get_user_by_username(u.username)
        assert fetched is not None
        assert fetched.username == u.username
        assert fetched.role == "admin"


# ======================================================================
# Proposal tests
# ======================================================================


class TestProposalStore:

    async def test_proposals_pending(self, store):
        c = _make_connector()
        await store.save_connector(c)
        p = _make_pipeline(c.connector_id)
        await store.save_pipeline(p)

        proposal = ContractChangeProposal(
            pipeline_id=p.pipeline_id,
            status=ProposalStatus.PENDING,
            trigger_type=TriggerType.QUALITY_ALERT,
            change_type=ChangeType.ADD_COLUMN,
            reasoning="Quality gate halted run",
        )
        await store.save_proposal(proposal)

        has_pending = await store.has_pending_halt_proposal(p.pipeline_id)
        assert has_pending is True


# ======================================================================
# Preference tests
# ======================================================================


class TestPreferenceStore:

    async def test_preferences_crud(self, store):
        pref = AgentPreference(
            scope=PreferenceScope.GLOBAL,
            preference_key="naming_convention",
            preference_value={"style": "snake_case"},
            source=PreferenceSource.USER_EXPLICIT,
            confidence=0.95,
        )
        await store.save_preference(pref)

        prefs = await store.get_preferences(scope="global")
        assert any(p.preference_id == pref.preference_id for p in prefs)

        await store.delete_preference(pref.preference_id)
        prefs_after = await store.get_preferences(scope="global")
        assert not any(p.preference_id == pref.preference_id for p in prefs_after)
