"""
Pipeline execution state machine with error budget tracking and column lineage.
Runs extraction -> staging -> quality gate -> promotion for a single pipeline run.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

from config import Config
from contracts.models import (
    PipelineContract, RunRecord, GateRecord, ColumnLineage,
    ErrorBudget, AlertRecord, SchemaVersion,
    RunStatus, GateDecision, RunMode, ConnectorStatus, AlertSeverity,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from staging.local import LocalStagingManager
from quality.gate import QualityGate
from crypto import decrypt_dict, CREDENTIAL_FIELDS

log = logging.getLogger(__name__)


class PipelineRunner:
    """Executes extract -> stage -> gate -> promote flow with error budget tracking."""

    def __init__(
        self,
        config: Config,
        store: Store,
        registry: ConnectorRegistry,
        gate: QualityGate,
        staging: LocalStagingManager,
    ):
        self.config = config
        self.store = store
        self.registry = registry
        self.gate = gate
        self.staging = staging

    async def execute(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> RunRecord:
        """Run the full state machine for one pipeline execution."""
        log.info(
            "[%s] Starting run %s (mode=%s)",
            contract.pipeline_name, run.run_id[:8], run.run_mode.value,
        )
        try:
            # 1. Preflight checks
            if not await self._preflight(contract, run):
                await self._update_error_budget(contract, run)
                return run

            # Resolve connectors
            src_params, tgt_params = self._connector_params(contract, "both")
            source = await self.registry.get_source(
                contract.source_connector_id, src_params,
            )
            target = await self.registry.get_target(
                contract.target_connector_id, tgt_params,
            )

            # Ensure target table exists
            await target.create_table_if_not_exists(contract)

            # 2. EXTRACTING
            run.status = RunStatus.EXTRACTING
            run.watermark_before = contract.last_watermark
            await self.store.save_run(run)

            staging_dir = self.staging.ensure_run_dir(
                contract.pipeline_id, run.run_id,
            )
            extract_result = await source.extract(
                contract, run, staging_dir, self.config.batch_size,
            )
            run.rows_extracted = extract_result.rows_extracted
            run.staging_path = str(extract_result.staging_path)
            run.staging_size_bytes = extract_result.staging_size_bytes

            log.info(
                "[%s] Extracted %d rows",
                contract.pipeline_name, run.rows_extracted,
            )

            # 3. Skip if incremental with 0 rows
            if (
                run.rows_extracted == 0
                and contract.refresh_type.value == "incremental"
            ):
                log.info(
                    "[%s] No new rows -- marking complete.",
                    contract.pipeline_name,
                )
                run.status = RunStatus.COMPLETE
                run.completed_at = now_iso()
                await self.store.save_run(run)
                await self._update_error_budget(contract, run)
                return run

            # 4. LOADING to staging
            run.status = RunStatus.LOADING
            await self.store.save_run(run)
            await target.load_staging(contract, run)
            log.info(
                "[%s] Loaded to staging", contract.pipeline_name,
            )

            # 5. QUALITY GATE
            run.status = RunStatus.QUALITY_GATE
            await self.store.save_run(run)

            gate_record = await self.gate.run(contract, run, target)

            await self.store.save_gate(gate_record)
            run.gate_decision = gate_record.decision
            run.quality_results = {
                "decision": gate_record.decision.value,
                "checks": [
                    {
                        "name": c.check_name,
                        "status": c.status.value,
                        "detail": c.detail,
                    }
                    for c in gate_record.checks
                ],
            }

            # 6. On HALT: preserve staging, don't clean up
            if gate_record.decision == GateDecision.HALT:
                result = await self._handle_halt(contract, run, gate_record, target)
                await self._update_error_budget(contract, result)
                return result

            # 7. PROMOTING (PROMOTE or PROMOTE_WITH_WARNING)
            run.status = RunStatus.PROMOTING
            await self.store.save_run(run)
            await target.promote(contract, run)
            log.info("[%s] Promoted to target", contract.pipeline_name)

            # 8. Update watermark (skip for backfills)
            if (
                run.run_mode != RunMode.BACKFILL
                and extract_result.max_watermark
            ):
                run.watermark_after = extract_result.max_watermark
                contract.last_watermark = extract_result.max_watermark

            # Update baselines
            await self._update_baselines(contract, run)
            await self.store.save_pipeline(contract)

            # Cleanup staging
            self.staging.cleanup_run(contract.pipeline_id, run.run_id)

            # Track column lineage after successful promotion
            await self._track_column_lineage(contract)

            # 9. Mark COMPLETE
            run.status = RunStatus.COMPLETE
            run.completed_at = now_iso()
            await self.store.save_run(run)
            log.info(
                "[%s] Run complete -- %d rows extracted",
                contract.pipeline_name, run.rows_extracted,
            )

            # 10. Update error budget
            await self._update_error_budget(contract, run)

            return run

        except Exception as e:
            log.exception("[%s] Run failed: %s", contract.pipeline_name, e)
            run.error = str(e)
            run.status = RunStatus.FAILED
            run.completed_at = now_iso()
            await self.store.save_run(run)

            # Try to clean up staging table in target
            try:
                _, tgt_params = self._connector_params(contract, "target")
                target = await self.registry.get_target(
                    contract.target_connector_id, tgt_params,
                )
                await target.drop_staging(contract, run)
            except Exception:
                pass

            # Update error budget even on failure
            await self._update_error_budget(contract, run)

            return run

    # ------------------------------------------------------------------
    # Preflight
    # ------------------------------------------------------------------

    async def _preflight(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> bool:
        """Returns False if run should be skipped."""
        # Check pending halt proposals
        if await self.store.has_pending_halt_proposal(contract.pipeline_id):
            log.warning(
                "[%s] Skipping -- pending halt proposal.",
                contract.pipeline_name,
            )
            run.status = RunStatus.FAILED
            run.error = "Skipped: pending halt proposal requires approval."
            run.completed_at = now_iso()
            await self.store.save_run(run)
            return False

        # Check disk space
        has_space, used_pct = self.staging.check_disk_space(
            self.config.max_disk_pct,
        )
        if not has_space:
            log.error(
                "[%s] Insufficient disk space: %.0f%% used.",
                contract.pipeline_name, used_pct * 100,
            )
            run.status = RunStatus.FAILED
            run.error = f"Insufficient disk space: {used_pct:.0%} used."
            run.completed_at = now_iso()
            await self.store.save_run(run)
            return False

        # Check upstream dependencies
        deps = await self.store.list_dependencies(contract.pipeline_id)
        for dep in deps:
            last_run = await self.store.get_last_successful_run(dep.depends_on_id)
            if last_run is None:
                upstream = await self.store.get_pipeline(dep.depends_on_id)
                name = upstream.pipeline_name if upstream else dep.depends_on_id
                log.warning(
                    "[%s] Upstream %s has no successful run yet.",
                    contract.pipeline_name, name,
                )
                run.status = RunStatus.FAILED
                run.error = (
                    f"Upstream pipeline {name} has not completed successfully."
                )
                run.completed_at = now_iso()
                await self.store.save_run(run)
                return False

        # Validate connectors are active
        src_connector = None
        tgt_connector = None
        connectors = await self.store.list_connectors()
        for c in connectors:
            if c.connector_id == contract.source_connector_id:
                src_connector = c
            if c.connector_id == contract.target_connector_id:
                tgt_connector = c

        for conn_record, label in [
            (src_connector, "source"),
            (tgt_connector, "target"),
        ]:
            if not conn_record or conn_record.status != ConnectorStatus.ACTIVE:
                run.status = RunStatus.FAILED
                run.error = f"{label.capitalize()} connector is not active."
                run.completed_at = now_iso()
                await self.store.save_run(run)
                return False

        return True

    # ------------------------------------------------------------------
    # Halt handling
    # ------------------------------------------------------------------

    async def _handle_halt(
        self,
        contract: PipelineContract,
        run: RunRecord,
        gate: GateRecord,
        target,
    ) -> RunRecord:
        """Mark run as HALTED and preserve staging for investigation."""
        log.warning(
            "[%s] Quality gate HALT. Preserving staging.",
            contract.pipeline_name,
        )
        run.status = RunStatus.HALTED
        run.completed_at = now_iso()
        await self.store.save_run(run)
        # Staging table preserved for investigation -- DO NOT drop
        return run

    # ------------------------------------------------------------------
    # Baselines
    # ------------------------------------------------------------------

    async def _update_baselines(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> None:
        """Update rolling baseline metrics after a successful run."""
        contract.baseline_row_count = run.rows_extracted

        # Rolling average / stddev for volume from last 30 runs
        baseline = await self.store.get_volume_baseline(
            contract.pipeline_id, window=29,
        )
        if baseline is None:
            baseline = []
        baseline.append(run.rows_extracted)
        n = len(baseline)
        if n > 0:
            mean = sum(baseline) / n
            variance = sum((x - mean) ** 2 for x in baseline) / n
            contract.baseline_volume_avg = mean
            contract.baseline_volume_stddev = (
                math.sqrt(variance) if variance > 0 else 0.0
            )

    # ------------------------------------------------------------------
    # Error budget
    # ------------------------------------------------------------------

    async def _update_error_budget(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> None:
        """Calculate and store error budget after every run.

        If budget_remaining <= 0, set escalated=True and create CRITICAL alert.
        """
        try:
            # Get existing budget or create default
            budget = await self.store.get_error_budget(contract.pipeline_id)
            if budget is None:
                budget = ErrorBudget(
                    pipeline_id=contract.pipeline_id,
                    window_days=7,
                    budget_threshold=0.9,
                )

            # Get all runs within the rolling window
            runs = await self.store.list_runs(
                contract.pipeline_id,
                window_days=budget.window_days,
            )

            total_runs = len(runs)
            successful_runs = sum(
                1 for r in runs if r.status == RunStatus.COMPLETE
            )
            failed_runs = total_runs - successful_runs

            if total_runs > 0:
                success_rate = successful_runs / total_runs
            else:
                success_rate = 1.0

            budget_remaining = success_rate - budget.budget_threshold

            was_escalated = budget.escalated
            budget.total_runs = total_runs
            budget.successful_runs = successful_runs
            budget.failed_runs = failed_runs
            budget.success_rate = round(success_rate, 4)
            budget.budget_remaining = round(budget_remaining, 4)
            budget.escalated = budget_remaining <= 0 and total_runs > 0
            budget.last_calculated = now_iso()

            await self.store.save_error_budget(budget)

            # Create CRITICAL alert if budget just became exhausted
            if budget.escalated and not was_escalated:
                alert = AlertRecord(
                    severity=AlertSeverity.CRITICAL,
                    tier=contract.tier,
                    pipeline_id=contract.pipeline_id,
                    pipeline_name=contract.pipeline_name,
                    summary=(
                        f"Error budget exhausted: {success_rate:.1%} success rate "
                        f"over {budget.window_days}d window "
                        f"(threshold: {budget.budget_threshold:.0%}). "
                        f"{failed_runs}/{total_runs} runs failed."
                    ),
                    detail={
                        "success_rate": success_rate,
                        "threshold": budget.budget_threshold,
                        "total_runs": total_runs,
                        "failed_runs": failed_runs,
                        "window_days": budget.window_days,
                    },
                )
                await self.store.save_alert(alert)
                log.error(
                    "[%s] Error budget EXHAUSTED: %.1f%% success (%d/%d runs)",
                    contract.pipeline_name, success_rate * 100,
                    successful_runs, total_runs,
                )

        except Exception as e:
            log.warning(
                "[%s] Error budget calculation failed: %s",
                contract.pipeline_name, e,
            )

    # ------------------------------------------------------------------
    # Column lineage
    # ------------------------------------------------------------------

    async def _track_column_lineage(
        self,
        contract: PipelineContract,
    ) -> None:
        """Save column lineage records linking source -> target columns."""
        try:
            for mapping in contract.column_mappings:
                lineage = ColumnLineage(
                    source_pipeline_id=contract.pipeline_id,
                    source_schema=contract.source_schema,
                    source_table=contract.source_table,
                    source_column=mapping.source_column,
                    target_pipeline_id=contract.pipeline_id,
                    target_schema=contract.target_schema,
                    target_table=contract.target_table,
                    target_column=mapping.target_column,
                    transformation="direct",
                )
                await self.store.save_column_lineage(lineage)
        except Exception as e:
            log.warning(
                "[%s] Column lineage tracking failed: %s",
                contract.pipeline_name, e,
            )

    # ------------------------------------------------------------------
    # Connector param resolution
    # ------------------------------------------------------------------

    def _connector_params(
        self,
        contract: PipelineContract,
        which: str = "both",
    ) -> tuple[dict, dict]:
        """Build connection dicts from contract fields.

        All connection params come from the pipeline contract itself,
        keeping the runner fully source/target agnostic.
        Decrypts credentials if encryption_key is set.
        """
        src_params: dict = {}
        tgt_params: dict = {}

        if which in ("both", "source"):
            src_params = {
                "host": contract.source_host,
                "port": contract.source_port,
                "database": contract.source_database,
                "user": "",
                "password": "",
            }
            if self.config.has_encryption_key:
                src_params = decrypt_dict(
                    src_params, self.config.encryption_key, CREDENTIAL_FIELDS,
                )

        if which in ("both", "target"):
            tgt_params = {
                "host": contract.target_host,
                "port": contract.target_port,
                "database": contract.target_database,
                "user": contract.target_user,
                "password": contract.target_password,
                "default_schema": contract.target_schema,
            }
            if self.config.has_encryption_key:
                tgt_params = decrypt_dict(
                    tgt_params, self.config.encryption_key, CREDENTIAL_FIELDS,
                )

        return src_params, tgt_params
