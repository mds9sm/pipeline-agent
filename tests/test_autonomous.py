"""Tests for agent/autonomous.py -- PipelineRunner state machine."""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from contracts.models import (
    PipelineContract, RunRecord, GateRecord, ExtractResult,
    ErrorBudget, AlertRecord, ColumnLineage,
    ColumnMapping, QualityConfig,
    PipelineStatus, RunStatus, RunMode, GateDecision, CheckResult,
    CheckStatus, RefreshType, ConnectorStatus, AlertSeverity,
    new_id, now_iso,
)
from agent.autonomous import PipelineRunner

pytestmark = pytest.mark.asyncio


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_contract(**overrides) -> PipelineContract:
    defaults = dict(
        pipeline_id=new_id(),
        pipeline_name="auto-test-pipe",
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
        merge_keys=["id"],
        schedule_cron="0 * * * *",
        column_mappings=[
            ColumnMapping(
                source_column="id", source_type="INT",
                target_column="id", target_type="INTEGER",
                is_nullable=False, is_primary_key=True,
                is_incremental_candidate=False, ordinal_position=1,
            ),
            ColumnMapping(
                source_column="amount", source_type="DECIMAL",
                target_column="amount", target_type="NUMERIC",
                is_nullable=True, is_primary_key=False,
                is_incremental_candidate=False, ordinal_position=2,
            ),
        ],
        quality_config=QualityConfig(),
    )
    defaults.update(overrides)
    return PipelineContract(**defaults)


def _make_run(pipeline_id: str, rows: int = 1000) -> RunRecord:
    return RunRecord(
        run_id=new_id(),
        pipeline_id=pipeline_id,
        run_mode=RunMode.SCHEDULED,
        status=RunStatus.PENDING,
    )


def _build_runner(
    gate_decision: GateDecision = GateDecision.PROMOTE,
    extract_rows: int = 1000,
    has_pending_halt: bool = False,
    disk_ok: bool = True,
    deps: list = None,
    connectors: list = None,
    budget: ErrorBudget | None = None,
    runs_in_window: list = None,
):
    """Build a PipelineRunner with fully mocked dependencies."""
    config = MagicMock()
    config.has_encryption_key = False
    config.encryption_key = ""
    config.max_disk_pct = 90
    config.batch_size = 1000

    store = AsyncMock()
    store.save_run = AsyncMock()
    store.save_pipeline = AsyncMock()
    store.save_gate = AsyncMock()
    store.save_error_budget = AsyncMock()
    store.save_alert = AsyncMock()
    store.save_column_lineage = AsyncMock()
    store.has_pending_halt_proposal = AsyncMock(return_value=has_pending_halt)
    store.list_dependencies = AsyncMock(return_value=deps or [])
    store.get_last_successful_run = AsyncMock(return_value=None)
    store.get_error_budget = AsyncMock(return_value=budget)
    store.get_volume_baseline = AsyncMock(return_value=[])

    # Provide runs in window for error budget calculation
    if runs_in_window is not None:
        store.list_runs = AsyncMock(return_value=runs_in_window)
    else:
        store.list_runs = AsyncMock(return_value=[])

    # Default: two active connectors matching the contract
    if connectors is None:
        src_conn = MagicMock()
        src_conn.connector_id = "will-be-set"
        src_conn.status = ConnectorStatus.ACTIVE
        tgt_conn = MagicMock()
        tgt_conn.connector_id = "will-be-set"
        tgt_conn.status = ConnectorStatus.ACTIVE
        connectors = [src_conn, tgt_conn]
    store.list_connectors = AsyncMock(return_value=connectors)

    # Registry
    registry = MagicMock()
    source = MagicMock()
    source.extract = AsyncMock(return_value=ExtractResult(
        rows_extracted=extract_rows,
        max_watermark="2025-01-15 12:00:00",
        staging_path="/tmp/staging/test",
        staging_size_bytes=1024 * extract_rows,
        batch_count=1,
    ))
    target = MagicMock()
    target.create_table_if_not_exists = AsyncMock()
    target.load_staging = AsyncMock()
    target.promote = AsyncMock()
    target.drop_staging = AsyncMock()
    registry.get_source = MagicMock(return_value=source)
    registry.get_target = MagicMock(return_value=target)

    # Gate
    gate_record = GateRecord(
        run_id="placeholder",
        pipeline_id="placeholder",
        decision=gate_decision,
        checks=[
            CheckResult(
                check_name="count_reconciliation",
                status=CheckStatus.PASS,
                detail="OK",
            ),
        ],
    )
    gate = MagicMock()
    gate.run = AsyncMock(return_value=gate_record)

    # Staging
    staging = MagicMock()
    staging.ensure_run_dir = MagicMock(return_value="/tmp/staging/test")
    staging.check_disk_space = MagicMock(return_value=(disk_ok, 0.5 if disk_ok else 0.95))
    staging.cleanup_run = MagicMock()

    runner = PipelineRunner(
        config=config,
        store=store,
        registry=registry,
        gate=gate,
        staging=staging,
    )

    return runner, store, registry, gate, staging, source, target


def _set_connectors_for_contract(runner, contract, connectors_list):
    """Patch connector list to match contract's source/target IDs."""
    if len(connectors_list) >= 2:
        connectors_list[0].connector_id = contract.source_connector_id
        connectors_list[1].connector_id = contract.target_connector_id


# ======================================================================
# Tests
# ======================================================================


class TestSuccessfulRun:

    async def test_successful_run(self):
        """Full happy path: extract -> stage -> gate -> promote."""
        runner, store, registry, gate, staging, source, target = _build_runner()
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.COMPLETE
        assert result.rows_extracted == 1000
        source.extract.assert_awaited_once()
        target.promote.assert_awaited_once()
        staging.cleanup_run.assert_called_once()


class TestHaltBehavior:

    async def test_halt_preserves_staging(self):
        """Gate returns HALT, staging NOT cleaned up."""
        runner, store, registry, gate, staging, source, target = _build_runner(
            gate_decision=GateDecision.HALT,
        )
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.HALTED
        # Staging should NOT be cleaned up on halt
        staging.cleanup_run.assert_not_called()
        # Promote should NOT be called
        target.promote.assert_not_awaited()


class TestPreflightChecks:

    async def test_preflight_blocks_on_halt_proposal(self):
        """Pending halt proposal blocks run."""
        runner, store, *_ = _build_runner(has_pending_halt=True)
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.FAILED
        assert "halt proposal" in result.error.lower()

    async def test_preflight_blocks_on_disk_full(self):
        """Disk space check fails blocks run."""
        runner, store, *_ = _build_runner(disk_ok=False)
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        # Need to pass halt proposal check first
        store.has_pending_halt_proposal.return_value = False

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.FAILED
        assert "disk" in result.error.lower()


class TestIncrementalZeroRows:

    async def test_incremental_zero_rows_skips(self):
        """Extract returns 0 rows on incremental, run completes without staging."""
        runner, store, registry, gate, staging, source, target = _build_runner(
            extract_rows=0,
        )
        contract = _make_contract(refresh_type=RefreshType.INCREMENTAL)
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.COMPLETE
        assert result.rows_extracted == 0
        # Quality gate and promote should NOT be called
        gate.run.assert_not_awaited()
        target.promote.assert_not_awaited()


class TestErrorBudget:

    async def test_error_budget_updated_on_success(self):
        """After successful run, budget updated."""
        runner, store, registry, gate, staging, source, target = _build_runner()
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        await runner.execute(contract, run)

        store.save_error_budget.assert_awaited()

    async def test_error_budget_updated_on_failure(self):
        """After failed run, budget updated with correct metrics."""
        # Simulate a failure by making extract raise
        runner, store, registry, gate, staging, source, target = _build_runner()
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        source.extract.side_effect = RuntimeError("Connection refused")

        result = await runner.execute(contract, run)

        assert result.status == RunStatus.FAILED
        store.save_error_budget.assert_awaited()

    async def test_error_budget_escalation(self):
        """Budget exhausted triggers CRITICAL alert."""
        # Provide existing budget that is NOT yet escalated
        existing_budget = ErrorBudget(
            pipeline_id="will-set",
            window_days=7,
            total_runs=10,
            successful_runs=8,
            failed_runs=2,
            success_rate=0.8,
            budget_threshold=0.9,
            budget_remaining=-0.1,
            escalated=False,  # not yet escalated
        )

        # Create many failed runs in window to make success_rate < threshold
        failed_runs = []
        for _ in range(10):
            fr = RunRecord(
                pipeline_id="will-set",
                status=RunStatus.FAILED,
            )
            failed_runs.append(fr)
        # 1 complete out of 11 total -> success_rate ~0.09
        complete_run = RunRecord(
            pipeline_id="will-set",
            status=RunStatus.COMPLETE,
        )
        all_runs = failed_runs + [complete_run]

        runner, store, registry, gate, staging, source, target = _build_runner(
            budget=existing_budget,
            runs_in_window=all_runs,
        )
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)
        existing_budget.pipeline_id = contract.pipeline_id
        for r in all_runs:
            r.pipeline_id = contract.pipeline_id

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        await runner.execute(contract, run)

        # Should have saved alert for budget exhaustion
        store.save_alert.assert_awaited()
        # Check that the alert is CRITICAL
        alert_calls = store.save_alert.await_args_list
        has_critical = any(
            call.args[0].severity == AlertSeverity.CRITICAL
            for call in alert_calls
        )
        assert has_critical, "Expected a CRITICAL alert for exhausted budget"


class TestColumnLineage:

    async def test_column_lineage_tracked(self):
        """After promotion, lineage records created for each column mapping."""
        runner, store, registry, gate, staging, source, target = _build_runner()
        contract = _make_contract()
        run = _make_run(contract.pipeline_id)

        conns = (await store.list_connectors())
        conns[0].connector_id = contract.source_connector_id
        conns[1].connector_id = contract.target_connector_id

        await runner.execute(contract, run)

        # Should have called save_column_lineage for each column mapping
        assert store.save_column_lineage.await_count == len(contract.column_mappings)
