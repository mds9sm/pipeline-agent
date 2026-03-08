"""Tests for scheduler/manager.py -- cron scheduler with error budgets."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from contracts.models import (
    PipelineContract, PipelineDependency, RunRecord, ErrorBudget,
    PipelineStatus, RunMode, RunStatus, DependencyType,
    new_id, now_iso,
)
from scheduler.manager import Scheduler

pytestmark = pytest.mark.asyncio


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_config():
    config = MagicMock()
    config.max_concurrent = 4
    return config


def _make_pipeline(
    name: str = "test-pipe",
    cron: str = "0 * * * *",
    updated_minutes_ago: int = 120,
) -> PipelineContract:
    updated = (
        datetime.now(timezone.utc) - timedelta(minutes=updated_minutes_ago)
    ).isoformat()
    return PipelineContract(
        pipeline_id=new_id(),
        pipeline_name=name,
        status=PipelineStatus.ACTIVE,
        schedule_cron=cron,
        updated_at=updated,
        created_at=updated,
        retry_max_attempts=3,
        retry_backoff_seconds=10,
    )


# ======================================================================
# Schedule due tests
# ======================================================================


class TestIsDue:

    def test_is_due_when_scheduled(self):
        """Pipeline with past-due cron returns true."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        scheduler = Scheduler(config=config, store=store, runner=runner)

        # Updated 2 hours ago, hourly cron -> should be due
        pipeline = _make_pipeline(cron="0 * * * *", updated_minutes_ago=120)
        now = datetime.now(timezone.utc)

        assert scheduler._is_due(pipeline, now) is True

    def test_is_due_when_not_scheduled(self):
        """Pipeline with future cron returns false."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        scheduler = Scheduler(config=config, store=store, runner=runner)

        # Updated just now, hourly cron -> next run is ~1 hour from now
        pipeline = _make_pipeline(cron="0 * * * *", updated_minutes_ago=0)
        now = datetime.now(timezone.utc)

        assert scheduler._is_due(pipeline, now) is False


# ======================================================================
# Trigger tests
# ======================================================================


class TestTrigger:

    async def test_trigger_creates_run(self):
        """Manual trigger creates RunRecord."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        runner.execute = AsyncMock(return_value=RunRecord(
            status=RunStatus.COMPLETE,
        ))

        pipeline = _make_pipeline()
        store.get_pipeline = AsyncMock(return_value=pipeline)
        store.save_run = AsyncMock()

        scheduler = Scheduler(config=config, store=store, runner=runner)
        run = await scheduler.trigger(pipeline.pipeline_id)

        assert run.run_mode == RunMode.MANUAL
        assert run.status == RunStatus.PENDING
        store.save_run.assert_awaited_once()


# ======================================================================
# Error budget in tick
# ======================================================================


class TestErrorBudgetInScheduler:

    async def test_exhausted_budget_skipped(self):
        """Pipeline with exhausted budget skipped in tick."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()

        pipeline = _make_pipeline(updated_minutes_ago=120)

        store.list_pipelines = AsyncMock(return_value=[pipeline])
        store.list_dependencies = AsyncMock(return_value=[])

        # Budget is exhausted
        budget = ErrorBudget(
            pipeline_id=pipeline.pipeline_id,
            escalated=True,
            success_rate=0.5,
            budget_threshold=0.9,
        )
        store.get_error_budget = AsyncMock(return_value=budget)
        store.get_last_successful_run = AsyncMock(return_value=None)

        scheduler = Scheduler(config=config, store=store, runner=runner)

        # Patch _is_due to return True
        scheduler._is_due = MagicMock(return_value=True)

        # Patch asyncio.create_task to capture whether it was called
        with patch("scheduler.manager.asyncio.create_task") as mock_task:
            await scheduler._tick()
            # Should NOT have created a task because budget is exhausted
            mock_task.assert_not_called()


# ======================================================================
# Topological sort
# ======================================================================


class TestTopologicalSort:

    def test_topological_sort_simple(self):
        """Linear dependency chain sorted correctly."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        scheduler = Scheduler(config=config, store=store, runner=runner)

        # A -> B -> C (C depends on B, B depends on A)
        a = _make_pipeline("pipe-a")
        b = _make_pipeline("pipe-b")
        c = _make_pipeline("pipe-c")

        deps = [
            PipelineDependency(
                pipeline_id=b.pipeline_id,
                depends_on_id=a.pipeline_id,
                dependency_type=DependencyType.USER_DEFINED,
            ),
            PipelineDependency(
                pipeline_id=c.pipeline_id,
                depends_on_id=b.pipeline_id,
                dependency_type=DependencyType.USER_DEFINED,
            ),
        ]

        result = scheduler.topological_sort([a, b, c], deps)
        result_ids = [p.pipeline_id for p in result]

        # A must come before B, B must come before C
        assert result_ids.index(a.pipeline_id) < result_ids.index(b.pipeline_id)
        assert result_ids.index(b.pipeline_id) < result_ids.index(c.pipeline_id)

    def test_topological_sort_cycle(self):
        """Cycle detected -- pipelines in cycle may be skipped."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        scheduler = Scheduler(config=config, store=store, runner=runner)

        a = _make_pipeline("cycle-a")
        b = _make_pipeline("cycle-b")

        # A depends on B, B depends on A -> cycle
        deps = [
            PipelineDependency(
                pipeline_id=a.pipeline_id,
                depends_on_id=b.pipeline_id,
                dependency_type=DependencyType.USER_DEFINED,
            ),
            PipelineDependency(
                pipeline_id=b.pipeline_id,
                depends_on_id=a.pipeline_id,
                dependency_type=DependencyType.USER_DEFINED,
            ),
        ]

        # Should not raise -- cycles are handled gracefully
        result = scheduler.topological_sort([a, b], deps)
        # With a cycle, the result may be incomplete (some pipelines skipped)
        assert isinstance(result, list)


# ======================================================================
# Retry backoff
# ======================================================================


class TestRetry:

    async def test_retry_exponential_backoff(self):
        """Failed run retries with increasing delay."""
        config = _make_config()
        store = AsyncMock()
        runner = AsyncMock()
        scheduler = Scheduler(config=config, store=store, runner=runner)

        pipeline = _make_pipeline()
        pipeline.retry_max_attempts = 3
        pipeline.retry_backoff_seconds = 10

        failed_run = RunRecord(
            pipeline_id=pipeline.pipeline_id,
            run_mode=RunMode.SCHEDULED,
            status=RunStatus.FAILED,
            retry_count=0,
        )

        store.save_run = AsyncMock()

        with patch("scheduler.manager.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            # Mock _run_pipeline to avoid recursion
            scheduler._run_pipeline = AsyncMock()
            await scheduler._maybe_retry(pipeline, failed_run)

            # Should sleep with backoff: 10 * 2^0 = 10 seconds
            mock_sleep.assert_awaited_once_with(10)

        # Second retry: 10 * 2^1 = 20 seconds
        failed_run.retry_count = 1
        with patch("scheduler.manager.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            scheduler._run_pipeline = AsyncMock()
            await scheduler._maybe_retry(pipeline, failed_run)
            mock_sleep.assert_awaited_once_with(20)

        # Third retry should NOT happen (max_attempts=3, retry_count=3)
        failed_run.retry_count = 3
        with patch("scheduler.manager.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            scheduler._run_pipeline = AsyncMock()
            await scheduler._maybe_retry(pipeline, failed_run)
            mock_sleep.assert_not_awaited()
