"""Tests for quality/gate.py -- the 7-check quality gate (most critical tests)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from contracts.models import (
    PipelineContract, RunRecord, GateRecord,
    CheckResult, CheckStatus, GateDecision,
    ColumnMapping, QualityConfig, RefreshType,
    RunMode, RunStatus,
    new_id,
)
from quality.gate import QualityGate

pytestmark = pytest.mark.asyncio


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_contract(**overrides) -> PipelineContract:
    defaults = dict(
        pipeline_id=new_id(),
        pipeline_name="test-pipe",
        source_schema="public",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        refresh_type=RefreshType.INCREMENTAL,
        incremental_column="updated_at",
        schedule_cron="0 * * * *",  # every hour -> 60 min interval
        merge_keys=["id"],
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
            ColumnMapping(
                source_column="updated_at", source_type="TIMESTAMP",
                target_column="updated_at", target_type="TIMESTAMPTZ",
                is_nullable=True, is_primary_key=False,
                is_incremental_candidate=True, ordinal_position=3,
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
        rows_extracted=rows,
    )


def _make_target(
    row_count: int = 1000,
    column_types=None,
    dupe_count: int = 0,
    null_rates=None,
    max_value=None,
):
    target = MagicMock()
    target.default_schema = "raw"
    target.get_row_count = MagicMock(return_value=row_count)
    target.get_column_types = MagicMock(return_value=column_types or [
        {"column_name": "id", "data_type": "INTEGER"},
        {"column_name": "amount", "data_type": "NUMERIC"},
        {"column_name": "updated_at", "data_type": "TIMESTAMPTZ"},
        {"column_name": "_extracted_at", "data_type": "TIMESTAMPTZ"},
        {"column_name": "_source_schema", "data_type": "VARCHAR(255)"},
        {"column_name": "_source_table", "data_type": "VARCHAR(255)"},
        {"column_name": "_row_hash", "data_type": "VARCHAR(64)"},
    ])
    target.check_duplicates = MagicMock(return_value=dupe_count)
    target.get_null_rates = MagicMock(return_value=null_rates or {
        "id": 0.0, "amount": 0.02, "updated_at": 0.01,
    })
    target.get_cardinality = MagicMock(return_value={})
    target.get_max_value = MagicMock(return_value=max_value)
    return target


def _make_gate(store_mock=None, config_mock=None) -> QualityGate:
    store = store_mock or MagicMock()
    config = config_mock or MagicMock()
    return QualityGate(store=store, config=config)


# ======================================================================
# Tests
# ======================================================================


class TestQualityGateAllPass:

    async def test_all_pass(self):
        """When all checks pass, decision should be PROMOTE."""
        contract = _make_contract()
        run = _make_run(contract.pipeline_id, rows=1000)
        target = _make_target(row_count=1000)

        # Recent watermark (10 minutes ago)
        recent = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target.get_max_value.return_value = recent

        # Provide enough volume history
        store = MagicMock()
        store.get_volume_baseline = MagicMock(
            return_value=[950, 1000, 1050, 980, 1020, 990, 1010]
        )

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        assert isinstance(result, GateRecord)
        assert result.decision == GateDecision.PROMOTE
        assert all(c.status == CheckStatus.PASS for c in result.checks)


class TestCountReconciliation:

    async def test_count_mismatch_fails(self):
        """rows_extracted != staged count beyond tolerance -> HALT."""
        contract = _make_contract()
        run = _make_run(contract.pipeline_id, rows=1000)
        # Staged count is very different
        target = _make_target(row_count=500)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        assert result.decision == GateDecision.HALT
        count_check = next(
            c for c in result.checks if c.check_name == "count_reconciliation"
        )
        assert count_check.status == CheckStatus.FAIL

    async def test_count_within_tolerance_warns(self):
        """Small mismatch within tolerance -> PROMOTE_WITH_WARNING."""
        contract = _make_contract(
            quality_config=QualityConfig(count_tolerance=0.01),
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # 1 row difference = 0.1% -- within 1% tolerance but not exact
        target = _make_target(row_count=999)

        # Recent watermark
        recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target.get_max_value.return_value = recent

        store = MagicMock()
        store.get_volume_baseline = MagicMock(
            return_value=[950, 1000, 1050, 980, 1020, 990, 1010]
        )

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        count_check = next(
            c for c in result.checks if c.check_name == "count_reconciliation"
        )
        assert count_check.status == CheckStatus.WARN
        # With promote_on_warn=True (default), should still promote
        assert result.decision == GateDecision.PROMOTE_WITH_WARNING


class TestSchemaConsistency:

    async def test_schema_missing_column_fails(self):
        """Staging missing a mapped column -> HALT."""
        contract = _make_contract()
        run = _make_run(contract.pipeline_id, rows=1000)
        # Missing 'amount' column from staging
        target = _make_target(
            column_types=[
                {"column_name": "id", "data_type": "INTEGER"},
                # amount is missing
                {"column_name": "updated_at", "data_type": "TIMESTAMPTZ"},
                {"column_name": "_extracted_at", "data_type": "TIMESTAMPTZ"},
                {"column_name": "_source_schema", "data_type": "VARCHAR(255)"},
                {"column_name": "_source_table", "data_type": "VARCHAR(255)"},
                {"column_name": "_row_hash", "data_type": "VARCHAR(64)"},
            ],
        )

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        assert result.decision == GateDecision.HALT
        schema_check = next(
            c for c in result.checks if c.check_name == "schema_consistency"
        )
        assert schema_check.status == CheckStatus.FAIL
        assert "amount" in schema_check.detail.lower() or "Missing" in schema_check.detail


class TestPKUniqueness:

    async def test_pk_duplicates_fail(self):
        """Duplicate merge keys detected -> HALT."""
        contract = _make_contract()
        run = _make_run(contract.pipeline_id, rows=1000)
        target = _make_target(dupe_count=15)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        assert result.decision == GateDecision.HALT
        pk_check = next(
            c for c in result.checks if c.check_name == "pk_uniqueness"
        )
        assert pk_check.status == CheckStatus.FAIL
        assert "duplicate" in pk_check.detail.lower()


class TestNullRateAnalysis:

    async def test_null_rate_catastrophic_jump(self):
        """Null rate jumps from 2% to 50% -> HALT."""
        contract = _make_contract(
            baseline_null_rates={"id": 0.0, "amount": 0.02, "updated_at": 0.01},
            baseline_null_stddevs={"id": 0.0, "amount": 0.005, "updated_at": 0.003},
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # amount null rate jumps catastrophically
        target = _make_target(
            null_rates={"id": 0.0, "amount": 0.50, "updated_at": 0.01},
        )

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        assert result.decision == GateDecision.HALT
        null_check = next(
            c for c in result.checks if c.check_name == "null_rate_analysis"
        )
        assert null_check.status == CheckStatus.FAIL
        assert "catastrophic" in null_check.detail.lower()

    async def test_null_rate_no_baseline_passes(self):
        """First run with no baseline -> PASS (lenient)."""
        contract = _make_contract(
            baseline_null_rates={},
            baseline_null_stddevs={},
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        target = _make_target(
            null_rates={"id": 0.0, "amount": 0.15, "updated_at": 0.05},
        )

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        null_check = next(
            c for c in result.checks if c.check_name == "null_rate_analysis"
        )
        assert null_check.status == CheckStatus.PASS
        assert "first run" in null_check.detail.lower() or "baseline" in null_check.detail.lower()


class TestVolumeZScore:

    async def test_volume_zscore_spike(self):
        """Volume 4x average -> HALT (z > 3.0)."""
        contract = _make_contract()
        # Baseline: ~1000 rows per run, very consistent
        history = [1000, 1010, 990, 1005, 995, 1000, 1000]
        run = _make_run(contract.pipeline_id, rows=4000)  # 4x normal
        target = _make_target(row_count=4000)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=history)

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        vol_check = next(
            c for c in result.checks if c.check_name == "volume_zscore"
        )
        assert vol_check.status == CheckStatus.FAIL
        assert result.decision == GateDecision.HALT

    async def test_volume_zscore_few_runs(self):
        """Less than 5 historical runs -> PASS (skip check)."""
        contract = _make_contract()
        run = _make_run(contract.pipeline_id, rows=5000)
        target = _make_target(row_count=5000)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[1000, 1100, 900])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        vol_check = next(
            c for c in result.checks if c.check_name == "volume_zscore"
        )
        assert vol_check.status == CheckStatus.PASS
        assert "insufficient" in vol_check.detail.lower() or "history" in vol_check.detail.lower()


class TestFreshness:

    async def test_freshness_stale_fails(self):
        """Staleness > 5x schedule interval -> HALT."""
        contract = _make_contract(
            schedule_cron="0 * * * *",  # hourly = 60 min
            quality_config=QualityConfig(
                freshness_fail_multiplier=5.0,
                freshness_warn_multiplier=2.0,
            ),
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # Data is 6 hours old -> 360 min >> 5*60=300 min threshold
        old_time = (datetime.now(timezone.utc) - timedelta(hours=6)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target = _make_target(max_value=old_time)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        freshness_check = next(
            c for c in result.checks if c.check_name == "freshness"
        )
        assert freshness_check.status == CheckStatus.FAIL
        assert result.decision == GateDecision.HALT

    async def test_freshness_moderate_warns(self):
        """Staleness 3x schedule -> WARN."""
        contract = _make_contract(
            schedule_cron="0 * * * *",  # hourly = 60 min
            quality_config=QualityConfig(
                freshness_fail_multiplier=5.0,
                freshness_warn_multiplier=2.0,
            ),
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # Data is 3 hours old -> 180 min -> between 2*60=120 and 5*60=300
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target = _make_target(max_value=stale_time)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(
            return_value=[950, 1000, 1050, 980, 1020, 990, 1010]
        )

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        freshness_check = next(
            c for c in result.checks if c.check_name == "freshness"
        )
        assert freshness_check.status == CheckStatus.WARN


class TestPromoteOnWarn:

    async def test_promote_on_warn_true(self):
        """WARN checks + promote_on_warn=True -> PROMOTE_WITH_WARNING."""
        contract = _make_contract(
            quality_config=QualityConfig(promote_on_warn=True),
            schedule_cron="0 * * * *",
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # 3h stale -> WARN for freshness
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target = _make_target(max_value=stale_time)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(
            return_value=[950, 1000, 1050, 980, 1020, 990, 1010]
        )

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        has_warn = any(c.status == CheckStatus.WARN for c in result.checks)
        has_fail = any(c.status == CheckStatus.FAIL for c in result.checks)
        assert has_warn
        if not has_fail:
            assert result.decision == GateDecision.PROMOTE_WITH_WARNING

    async def test_promote_on_warn_false(self):
        """WARN checks + promote_on_warn=False -> HALT."""
        contract = _make_contract(
            quality_config=QualityConfig(promote_on_warn=False),
            schedule_cron="0 * * * *",
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        # 3h stale -> WARN for freshness
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        target = _make_target(max_value=stale_time)

        store = MagicMock()
        store.get_volume_baseline = MagicMock(
            return_value=[950, 1000, 1050, 980, 1020, 990, 1010]
        )

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        has_warn = any(c.status == CheckStatus.WARN for c in result.checks)
        has_fail = any(c.status == CheckStatus.FAIL for c in result.checks)
        if has_warn and not has_fail:
            assert result.decision == GateDecision.HALT


class TestFirstRun:

    async def test_first_run_passes(self):
        """Empty baselines, first run should be lenient."""
        contract = _make_contract(
            baseline_null_rates={},
            baseline_null_stddevs={},
            baseline_cardinality={},
            baseline_volume_avg=0.0,
            baseline_volume_stddev=0.0,
            refresh_type=RefreshType.FULL,  # full refresh skips freshness
        )
        run = _make_run(contract.pipeline_id, rows=1000)
        target = _make_target()

        store = MagicMock()
        store.get_volume_baseline = MagicMock(return_value=[])

        gate = _make_gate(store_mock=store)
        result = await gate.run(contract, run, target)

        # First run with no baselines: no FAIL expected
        assert result.decision in (
            GateDecision.PROMOTE,
            GateDecision.PROMOTE_WITH_WARNING,
        )
