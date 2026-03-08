"""Tests for monitor/engine.py -- drift detection, freshness, lineage impact."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from contracts.models import (
    PipelineContract, ColumnMapping, ConnectorRecord,
    ContractChangeProposal, SchemaVersion, AlertRecord,
    FreshnessSnapshot, ColumnLineage, TableProfile,
    ConnectorType, ConnectorStatus, PipelineStatus,
    FreshnessStatus, AlertSeverity, ProposalStatus,
    TriggerType, ChangeType, RefreshType,
    new_id, now_iso,
)
from monitor.engine import MonitorEngine

pytestmark = pytest.mark.asyncio


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_config():
    config = MagicMock()
    config.has_encryption_key = False
    config.encryption_key = ""
    config.slack_webhook = ""
    config.email_smtp_host = ""
    config.email_from = ""
    config.pagerduty_key = ""
    return config


def _make_pipeline(**overrides) -> PipelineContract:
    defaults = dict(
        pipeline_id=new_id(),
        pipeline_name="monitor-test-pipe",
        status=PipelineStatus.ACTIVE,
        source_connector_id=new_id(),
        target_connector_id=new_id(),
        source_host="localhost",
        source_port=3306,
        source_database="testdb",
        source_schema="public",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        refresh_type=RefreshType.INCREMENTAL,
        incremental_column="updated_at",
        freshness_column="updated_at",
        schedule_cron="0 * * * *",
        tier=2,
        auto_approve_additive_schema=False,
        column_mappings=[
            ColumnMapping(
                source_column="id", source_type="INT",
                target_column="id", target_type="INTEGER",
                is_nullable=False, is_primary_key=True,
                is_incremental_candidate=False, ordinal_position=1,
            ),
            ColumnMapping(
                source_column="amount", source_type="DECIMAL(10,2)",
                target_column="amount", target_type="NUMERIC(10,2)",
                is_nullable=True, is_primary_key=False,
                is_incremental_candidate=False, ordinal_position=2,
            ),
        ],
    )
    defaults.update(overrides)
    return PipelineContract(**defaults)


def _make_store():
    store = AsyncMock()
    store.list_connectors = AsyncMock(return_value=[])
    store.list_pipelines = AsyncMock(return_value=[])
    store.save_proposal = AsyncMock()
    store.save_alert = AsyncMock()
    store.save_decision = AsyncMock()
    store.save_pipeline = AsyncMock()
    store.save_schema_version = AsyncMock()
    store.save_freshness_snapshot = AsyncMock()
    store.get_preferences = AsyncMock(return_value=[])
    store.get_downstream_columns = AsyncMock(return_value=[])
    store.get_last_successful_run = AsyncMock(return_value=None)
    return store


def _make_registry():
    registry = MagicMock()
    source = MagicMock()
    source.profile_table = AsyncMock()
    target = MagicMock()
    target.get_max_value = MagicMock(return_value=None)
    registry.get_source = MagicMock(return_value=source)
    registry.get_target = MagicMock(return_value=target)
    return registry, source, target


def _make_agent():
    agent = MagicMock()
    agent.analyze_drift = AsyncMock(return_value={
        "action": "create_proposal",
        "breaking_change": False,
        "data_loss_risk": "low",
        "estimated_backfill_time": "5 minutes",
        "reasoning": "New column detected",
        "confidence": 0.9,
        "rollback_plan": "Drop column",
    })
    return agent


def _make_monitor(store=None, registry=None, agent=None, config=None):
    _config = config or _make_config()
    _store = store or _make_store()
    _registry_tuple = registry or _make_registry()
    if isinstance(_registry_tuple, tuple):
        _registry = _registry_tuple[0]
    else:
        _registry = _registry_tuple
    _agent = agent or _make_agent()
    return MonitorEngine(
        config=_config, store=_store,
        registry=_registry, agent=_agent,
    )


# ======================================================================
# Drift detection tests
# ======================================================================


class TestDriftDetection:

    async def test_drift_detected_new_column(self):
        """New column detected, proposal created."""
        pipeline = _make_pipeline()
        store = _make_store()
        registry, source, target = _make_registry()
        agent = _make_agent()

        # Source connector exists and is active
        src_conn = ConnectorRecord(
            connector_id=pipeline.source_connector_id,
            connector_name="src",
            connector_type=ConnectorType.SOURCE,
            source_target_type="mysql",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[src_conn])

        # Profile returns existing columns + a new one
        source.profile_table = AsyncMock(return_value=TableProfile(
            schema_name="public",
            table_name="orders",
            row_count_estimate=1000,
            column_count=3,
            columns=[
                ColumnMapping(
                    source_column="id", source_type="INT",
                    target_column="id", target_type="INTEGER",
                    is_nullable=False, is_primary_key=True,
                    is_incremental_candidate=False, ordinal_position=1,
                ),
                ColumnMapping(
                    source_column="amount", source_type="DECIMAL(10,2)",
                    target_column="amount", target_type="NUMERIC(10,2)",
                    is_nullable=True, is_primary_key=False,
                    is_incremental_candidate=False, ordinal_position=2,
                ),
                # NEW column
                ColumnMapping(
                    source_column="email", source_type="VARCHAR(255)",
                    target_column="email", target_type="VARCHAR(255)",
                    is_nullable=True, is_primary_key=False,
                    is_incremental_candidate=False, ordinal_position=3,
                ),
            ],
        ))

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=agent,
        )
        await monitor._check_drift(pipeline)

        # Should create a proposal for the new column
        store.save_proposal.assert_awaited_once()
        proposal = store.save_proposal.await_args[0][0]
        assert proposal.trigger_type == TriggerType.SCHEMA_DRIFT
        assert proposal.pipeline_id == pipeline.pipeline_id

        # Should create an alert
        store.save_alert.assert_awaited_once()

    async def test_drift_auto_adapt(self):
        """Auto-adaptable new nullable column applied automatically."""
        pipeline = _make_pipeline(auto_approve_additive_schema=True)
        store = _make_store()
        registry, source, target = _make_registry()

        src_conn = ConnectorRecord(
            connector_id=pipeline.source_connector_id,
            connector_name="src",
            connector_type=ConnectorType.SOURCE,
            source_target_type="mysql",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[src_conn])

        new_col = ColumnMapping(
            source_column="email", source_type="VARCHAR(255)",
            target_column="email", target_type="VARCHAR(255)",
            is_nullable=True, is_primary_key=False,
            is_incremental_candidate=False, ordinal_position=3,
        )
        profile_result = TableProfile(
            schema_name="public",
            table_name="orders",
            row_count_estimate=1000,
            column_count=3,
            columns=list(pipeline.column_mappings) + [new_col],
        )
        source.profile_table = AsyncMock(return_value=profile_result)

        agent = _make_agent()
        agent.analyze_drift = AsyncMock(return_value={
            "action": "auto_adapt",
            "reasoning": "Safe additive change",
            "confidence": 0.95,
        })

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=agent,
        )
        await monitor._check_drift(pipeline)

        # Auto-adapt: pipeline saved with new columns, schema version created
        store.save_pipeline.assert_awaited()
        store.save_schema_version.assert_awaited()
        # Should NOT create a proposal (auto-applied)
        store.save_proposal.assert_not_awaited()

    async def test_drift_dropped_column_halts(self):
        """Dropped column triggers halt proposal."""
        pipeline = _make_pipeline()
        store = _make_store()
        registry, source, target = _make_registry()
        agent = _make_agent()

        src_conn = ConnectorRecord(
            connector_id=pipeline.source_connector_id,
            connector_name="src",
            connector_type=ConnectorType.SOURCE,
            source_target_type="mysql",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[src_conn])

        # Profile returns only 'id' -- 'amount' is dropped
        source.profile_table = AsyncMock(return_value=TableProfile(
            schema_name="public",
            table_name="orders",
            row_count_estimate=1000,
            column_count=1,
            columns=[
                ColumnMapping(
                    source_column="id", source_type="INT",
                    target_column="id", target_type="INTEGER",
                    is_nullable=False, is_primary_key=True,
                    is_incremental_candidate=False, ordinal_position=1,
                ),
            ],
        ))

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=agent,
        )
        await monitor._check_drift(pipeline)

        # Should create a proposal with DROP_COLUMN change type
        store.save_proposal.assert_awaited_once()
        proposal = store.save_proposal.await_args[0][0]
        assert proposal.change_type == ChangeType.DROP_COLUMN

        # Alert should be CRITICAL for dropped columns
        store.save_alert.assert_awaited_once()
        alert = store.save_alert.await_args[0][0]
        assert alert.severity == AlertSeverity.CRITICAL


# ======================================================================
# Freshness monitoring tests
# ======================================================================


class TestFreshnessMonitoring:

    async def test_freshness_fresh(self):
        """Pipeline within SLA, no alert."""
        pipeline = _make_pipeline(tier=2)
        store = _make_store()
        registry, source, target = _make_registry()

        tgt_conn = ConnectorRecord(
            connector_id=pipeline.target_connector_id,
            connector_name="tgt",
            connector_type=ConnectorType.TARGET,
            source_target_type="redshift",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[tgt_conn])

        # Data is 30 minutes old -- well within tier 2 SLA (120m warn)
        fresh_time = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target.get_max_value = MagicMock(return_value=fresh_time)

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=_make_agent(),
        )
        await monitor._check_freshness(pipeline)

        # Should save a snapshot but no alert
        store.save_freshness_snapshot.assert_awaited_once()
        snapshot = store.save_freshness_snapshot.await_args[0][0]
        assert snapshot.status == FreshnessStatus.FRESH
        store.save_alert.assert_not_awaited()

    async def test_freshness_warning(self):
        """Pipeline approaching SLA, warning alert created."""
        pipeline = _make_pipeline(tier=2)
        store = _make_store()
        registry, source, target = _make_registry()

        tgt_conn = ConnectorRecord(
            connector_id=pipeline.target_connector_id,
            connector_name="tgt",
            connector_type=ConnectorType.TARGET,
            source_target_type="redshift",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[tgt_conn])

        # Data is 200 minutes old -- between warn (120) and critical (360) for tier 2
        stale_time = (datetime.now(timezone.utc) - timedelta(minutes=200)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target.get_max_value = MagicMock(return_value=stale_time)

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=_make_agent(),
        )
        await monitor._check_freshness(pipeline)

        store.save_freshness_snapshot.assert_awaited_once()
        snapshot = store.save_freshness_snapshot.await_args[0][0]
        assert snapshot.status == FreshnessStatus.WARNING

        store.save_alert.assert_awaited_once()
        alert = store.save_alert.await_args[0][0]
        assert alert.severity == AlertSeverity.WARNING

    async def test_freshness_critical(self):
        """Pipeline past SLA, critical alert created."""
        pipeline = _make_pipeline(tier=2)
        store = _make_store()
        registry, source, target = _make_registry()

        tgt_conn = ConnectorRecord(
            connector_id=pipeline.target_connector_id,
            connector_name="tgt",
            connector_type=ConnectorType.TARGET,
            source_target_type="redshift",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[tgt_conn])

        # Data is 500 minutes old -- past critical (360) for tier 2
        very_stale = (datetime.now(timezone.utc) - timedelta(minutes=500)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target.get_max_value = MagicMock(return_value=very_stale)

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=_make_agent(),
        )
        await monitor._check_freshness(pipeline)

        store.save_freshness_snapshot.assert_awaited_once()
        snapshot = store.save_freshness_snapshot.await_args[0][0]
        assert snapshot.status == FreshnessStatus.CRITICAL

        store.save_alert.assert_awaited_once()
        alert = store.save_alert.await_args[0][0]
        assert alert.severity == AlertSeverity.CRITICAL


# ======================================================================
# Column impact analysis
# ======================================================================


class TestColumnImpactAnalysis:

    async def test_column_impact_analysis(self):
        """Drift in column that has downstream lineage includes impact in proposal."""
        pipeline = _make_pipeline()
        store = _make_store()
        registry, source, target = _make_registry()
        agent = _make_agent()

        src_conn = ConnectorRecord(
            connector_id=pipeline.source_connector_id,
            connector_name="src",
            connector_type=ConnectorType.SOURCE,
            source_target_type="mysql",
            status=ConnectorStatus.ACTIVE,
        )
        store.list_connectors = AsyncMock(return_value=[src_conn])

        # Simulate type change on 'amount' column
        source.profile_table = AsyncMock(return_value=TableProfile(
            schema_name="public",
            table_name="orders",
            row_count_estimate=1000,
            column_count=2,
            columns=[
                ColumnMapping(
                    source_column="id", source_type="INT",
                    target_column="id", target_type="INTEGER",
                    is_nullable=False, is_primary_key=True,
                    is_incremental_candidate=False, ordinal_position=1,
                ),
                ColumnMapping(
                    source_column="amount", source_type="VARCHAR(50)",  # type changed
                    target_column="amount", target_type="VARCHAR(50)",
                    is_nullable=True, is_primary_key=False,
                    is_incremental_candidate=False, ordinal_position=2,
                ),
            ],
        ))

        # Downstream lineage exists for 'amount'
        downstream_lineage = ColumnLineage(
            source_pipeline_id=pipeline.pipeline_id,
            source_schema="public",
            source_table="orders",
            source_column="amount",
            target_pipeline_id=new_id(),
            target_schema="analytics",
            target_table="revenue",
            target_column="total_amount",
            transformation="SUM",
        )
        store.get_downstream_columns = AsyncMock(return_value=[downstream_lineage])

        monitor = MonitorEngine(
            config=_make_config(), store=store,
            registry=registry, agent=agent,
        )
        await monitor._check_drift(pipeline)

        store.save_proposal.assert_awaited_once()
        proposal = store.save_proposal.await_args[0][0]

        # Impact analysis should include downstream column info
        impact = proposal.impact_analysis
        assert "downstream_column_impact" in impact
        assert len(impact["downstream_column_impact"]) >= 1
        downstream = impact["downstream_column_impact"][0]
        assert downstream["downstream_column"] == "total_amount"
        assert downstream["downstream_table"] == "revenue"
