"""
Pipeline execution state machine with error budget tracking and column lineage.
Runs extraction -> staging -> quality gate -> promotion for a single pipeline run.
"""
from __future__ import annotations

import decimal
import logging
import math
import time as _time
from datetime import date, datetime
from typing import Optional

from config import Config
from contracts.models import (
    PipelineContract, RunRecord, GateRecord, ColumnLineage,
    ErrorBudget, AlertRecord, SchemaVersion,
    RunStatus, GateDecision, RunMode, ConnectorStatus, AlertSeverity,
    CleanupOwnership,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from staging.local import LocalStagingManager
from quality.gate import QualityGate
from crypto import decrypt_dict, CREDENTIAL_FIELDS
from logging_config import PipelineContext

log = logging.getLogger(__name__)


def _json_safe(obj):
    """Convert a dict's values to JSON-serializable types."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return obj


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
        async with PipelineContext(
            contract.pipeline_id, contract.pipeline_name,
            run_id=run.run_id, component="runner",
        ):
            return await self._execute_inner(contract, run)

    async def _execute_inner(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> RunRecord:
        """Inner execution logic (runs inside PipelineContext)."""
        log.info("Starting run (mode=%s)", run.run_mode.value)
        try:
            # 1. Preflight checks
            if not await self._preflight(contract, run):
                await self._update_error_budget(contract, run)
                return run

            # Build 15: Load upstream run context for data-triggered runs
            upstream_run = None
            if run.triggered_by_run_id:
                upstream_run = await self.store.get_run(run.triggered_by_run_id)

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

            log.info("Extracted %d rows", run.rows_extracted)

            # 3. Skip if incremental with 0 rows
            if (
                run.rows_extracted == 0
                and contract.refresh_type.value == "incremental"
            ):
                log.info("No new rows -- marking complete.")
                run.status = RunStatus.COMPLETE
                run.completed_at = now_iso()
                await self.store.save_run(run)
                await self._update_error_budget(contract, run)
                return run

            # 4. LOADING to staging
            run.status = RunStatus.LOADING
            await self.store.save_run(run)
            await target.load_staging(contract, run)
            log.info("Loaded to staging")

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
            log.info("Promoted to target")

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

            # Write pipeline metadata (XCom-style)
            await self._write_run_metadata(contract, run, extract_result, upstream_run)

            # Execute post-promotion SQL hooks
            await self._execute_post_promotion_hooks(contract, run, target, upstream_run)

            # 9. Mark COMPLETE
            run.status = RunStatus.COMPLETE
            run.completed_at = now_iso()
            await self.store.save_run(run)
            log.info("Run complete -- %d rows extracted", run.rows_extracted)

            # 10. Update error budget
            await self._update_error_budget(contract, run)

            return run

        except Exception as e:
            log.exception("Run failed: %s", e)
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
            log.warning("Skipping -- pending halt proposal.")
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
                "Insufficient disk space: %.0f%% used.", used_pct * 100,
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
                log.warning("Upstream %s has no successful run yet.", name)
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
        log.warning("Quality gate HALT. Preserving staging.")
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

            # Get all runs within the rolling window (~1 run/hour max)
            runs = await self.store.list_runs(
                contract.pipeline_id,
                limit=budget.window_days * 24,
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
                    "Error budget EXHAUSTED: %.1f%% success (%d/%d runs)",
                    success_rate * 100, successful_runs, total_runs,
                )

        except Exception as e:
            log.warning("Error budget calculation failed: %s", e)

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
            log.warning("Column lineage tracking failed: %s", e)

    # ------------------------------------------------------------------
    # Pipeline metadata (XCom-style)
    # ------------------------------------------------------------------

    async def _write_run_metadata(
        self,
        contract: PipelineContract,
        run: RunRecord,
        extract_result,
        upstream_run: RunRecord = None,
    ) -> None:
        """Persist execution metadata for downstream consumption."""
        try:
            pid = contract.pipeline_id
            rid = run.run_id
            await self.store.set_metadata(
                pid, "last_run_id", {"value": run.run_id}, rid,
            )
            await self.store.set_metadata(
                pid, "last_row_count", {"value": run.rows_extracted}, rid,
            )
            if extract_result.max_watermark:
                await self.store.set_metadata(
                    pid, "last_max_watermark",
                    {"value": extract_result.max_watermark}, rid,
                )
            await self.store.set_metadata(
                pid, "last_completed_at",
                {"value": run.completed_at or now_iso()}, rid,
            )
            await self.store.set_metadata(
                pid, "last_staging_size_bytes",
                {"value": run.staging_size_bytes}, rid,
            )
            # Build 15: Write upstream context for data-triggered runs
            if upstream_run:
                await self.store.set_metadata(
                    pid, "upstream_run_id",
                    {"value": upstream_run.run_id}, rid, namespace="upstream",
                )
                await self.store.set_metadata(
                    pid, "upstream_pipeline_id",
                    {"value": upstream_run.pipeline_id}, rid, namespace="upstream",
                )
                await self.store.set_metadata(
                    pid, "upstream_watermark_after",
                    {"value": upstream_run.watermark_after}, rid, namespace="upstream",
                )
                await self.store.set_metadata(
                    pid, "upstream_rows_extracted",
                    {"value": upstream_run.rows_extracted}, rid, namespace="upstream",
                )
                await self.store.set_metadata(
                    pid, "upstream_completed_at",
                    {"value": upstream_run.completed_at}, rid, namespace="upstream",
                )
        except Exception as e:
            log.warning("Failed to write pipeline metadata: %s", e)

    # ------------------------------------------------------------------
    # Post-promotion SQL hooks
    # ------------------------------------------------------------------

    @staticmethod
    def _render_hook_sql(
        sql: str,
        contract: PipelineContract,
        run: RunRecord,
        upstream_run: RunRecord = None,
    ) -> str:
        """Replace {{variable}} placeholders with run context values.

        Supported variables (15 current + 9 upstream):
          {{pipeline_id}}, {{pipeline_name}},
          {{run_id}}, {{run_mode}},
          {{watermark_before}}, {{watermark_after}},
          {{rows_extracted}}, {{rows_loaded}},
          {{started_at}}, {{completed_at}},
          {{source_schema}}, {{source_table}},
          {{target_schema}}, {{target_table}},
          {{batch_id}} (alias for run_id[:8])
          {{upstream_run_id}}, {{upstream_pipeline_id}},
          {{upstream_watermark_before}}, {{upstream_watermark_after}},
          {{upstream_rows_extracted}}, {{upstream_rows_loaded}},
          {{upstream_started_at}}, {{upstream_completed_at}},
          {{upstream_batch_id}}
        """
        replacements = {
            "pipeline_id": contract.pipeline_id,
            "pipeline_name": contract.pipeline_name,
            "run_id": run.run_id,
            "run_mode": run.run_mode.value if hasattr(run.run_mode, "value") else str(run.run_mode),
            "watermark_before": run.watermark_before or "",
            "watermark_after": run.watermark_after or "",
            "rows_extracted": str(run.rows_extracted),
            "rows_loaded": str(run.rows_loaded),
            "started_at": run.started_at or "",
            "completed_at": run.completed_at or "",
            "source_schema": contract.source_schema,
            "source_table": contract.source_table,
            "target_schema": contract.target_schema,
            "target_table": contract.target_table,
            "batch_id": run.run_id[:8],
            # Build 15: upstream context variables
            "upstream_run_id": upstream_run.run_id if upstream_run else "",
            "upstream_pipeline_id": upstream_run.pipeline_id if upstream_run else "",
            "upstream_watermark_before": (upstream_run.watermark_before or "") if upstream_run else "",
            "upstream_watermark_after": (upstream_run.watermark_after or "") if upstream_run else "",
            "upstream_rows_extracted": str(upstream_run.rows_extracted) if upstream_run else "0",
            "upstream_rows_loaded": str(upstream_run.rows_loaded) if upstream_run else "0",
            "upstream_started_at": (upstream_run.started_at or "") if upstream_run else "",
            "upstream_completed_at": (upstream_run.completed_at or "") if upstream_run else "",
            "upstream_batch_id": upstream_run.run_id[:8] if upstream_run else "",
        }
        rendered = sql
        for key, value in replacements.items():
            rendered = rendered.replace("{{" + key + "}}", str(value))
        return rendered

    async def _execute_post_promotion_hooks(
        self,
        contract: PipelineContract,
        run: RunRecord,
        target,
        upstream_run: RunRecord = None,
    ) -> None:
        """Execute SQL hooks against the target and store results as metadata."""
        hooks = [h for h in contract.post_promotion_hooks if h.enabled]
        if not hooks:
            return

        log.info("Executing %d post-promotion hook(s)", len(hooks))
        for hook in hooks:
            rendered_sql = self._render_hook_sql(hook.sql, contract, run, upstream_run)

            # Build 16: Cleanup guard — check data contracts before DELETE/TRUNCATE
            if any(kw in rendered_sql.upper() for kw in ("DELETE", "TRUNCATE")):
                if not await self._check_cleanup_allowed(contract):
                    log.warning(
                        "Hook '%s' skipped: data contract blocks cleanup "
                        "(consumer has not acknowledged data)",
                        hook.name,
                    )
                    continue

            t0 = _time.monotonic()
            try:
                rows = await target.execute_sql(
                    rendered_sql, hook.timeout_seconds,
                )
                duration_ms = int((_time.monotonic() - t0) * 1000)
                # Store first row for SELECT, or empty dict for non-SELECT
                result_value = dict(rows[0]) if rows else {}
                # Sanitize for JSON (handle Decimal, datetime, etc.)
                result_value = _json_safe(result_value)
                result = {
                    "status": "success",
                    "duration_ms": duration_ms,
                    "rows_returned": len(rows),
                    "result": result_value,
                }
                if rendered_sql != hook.sql:
                    result["rendered_sql"] = rendered_sql
                log.info(
                    "Hook '%s' completed in %dms (%d rows)",
                    hook.name, duration_ms, len(rows),
                )
            except NotImplementedError:
                log.warning(
                    "Hook '%s' skipped: target does not support execute_sql",
                    hook.name,
                )
                continue
            except Exception as e:
                duration_ms = int((_time.monotonic() - t0) * 1000)
                result = {
                    "status": "error",
                    "duration_ms": duration_ms,
                    "error": str(e),
                }
                log.warning("Hook '%s' failed: %s", hook.name, e)
                if hook.fail_pipeline_on_error:
                    raise RuntimeError(
                        f"Post-promotion hook '{hook.name}' failed: {e}"
                    ) from e

            # Store result as metadata
            try:
                key = hook.metadata_key or hook.name
                await self.store.set_metadata(
                    contract.pipeline_id, key, result,
                    run.run_id, namespace="hooks",
                )
            except Exception as e:
                log.warning("Failed to store hook result for '%s': %s", hook.name, e)

    async def _check_cleanup_allowed(self, contract: PipelineContract) -> bool:
        """Return False if data contracts block cleanup for this producer.

        When cleanup_ownership is consumer_acknowledges, the consumer must
        have completed at least one run before the producer can delete data.
        """
        try:
            contracts = await self.store.list_data_contracts(
                producer_id=contract.pipeline_id, status="active",
            )
            for dc in contracts:
                if dc.cleanup_ownership == CleanupOwnership.CONSUMER_ACKNOWLEDGES:
                    consumer_run = await self.store.get_last_successful_run(
                        dc.consumer_pipeline_id,
                    )
                    if consumer_run is None:
                        return False
            return True
        except Exception as e:
            log.warning("Cleanup guard check failed: %s", e)
            return True  # fail-open to avoid blocking on guard errors

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
                "user": contract.source_user,
                "password": contract.source_password,
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
