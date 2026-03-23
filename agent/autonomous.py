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
    ContractChangeProposal, ChangeType, TriggerType, ProposalStatus,
    StepDefinition, StepExecution, StepType, StepStatus,
    RunStatus, GateDecision, RunMode, ConnectorStatus, AlertSeverity,
    CleanupOwnership,
    now_iso, new_id,
)
from dataclasses import asdict
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
        agent=None,
    ):
        self.config = config
        self.store = store
        self.registry = registry
        self.gate = gate
        self.staging = staging
        self.agent = agent  # AgentCore — used for agentic schema drift reasoning

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

    def _log_step(self, run: RunRecord, step: str, detail: str = "", status: str = "ok"):
        """Append a structured log entry to the run's execution_log."""
        if run.execution_log is None:
            run.execution_log = []
        run.execution_log.append({
            "ts": now_iso(),
            "step": step,
            "detail": detail,
            "status": status,
            "elapsed_ms": int((_time.time() - self._step_t0) * 1000) if hasattr(self, '_step_t0') else 0,
        })
        self._step_t0 = _time.time()

    async def _pre_extract_schema_check(
        self,
        contract: PipelineContract,
        run: RunRecord,
        source,
        target,
    ) -> str:
        """Compare source schema against pipeline column_mappings before extraction.

        The agent reasons about detected drift, generates migration SQL, and
        either auto-applies it or creates a proposal for human approval.

        Returns:
            "ok"      — no drift, proceed with extraction
            "applied" — drift detected and agent-generated SQL auto-applied
            "halted"  — drift detected, proposal with agent SQL created, run halted
        """
        try:
            profile = await source.profile_table(
                contract.source_schema, contract.source_table,
            )
            live_cols = {m.source_column: m for m in profile.columns}
            existing_cols = {m.source_column for m in contract.column_mappings}

            new_columns = [
                {"name": name, "target_type": live_cols[name].target_type,
                 "source_type": live_cols[name].source_type,
                 "nullable": live_cols[name].is_nullable}
                for name in live_cols if name not in existing_cols
            ]
            if not new_columns:
                self._log_step(run, "schema_check", "no drift detected")
                return "ok"

            new_names = [c["name"] for c in new_columns]
            policy = contract.get_schema_policy()
            log.info("Pre-extract drift: %d new column(s) %s, policy=%s",
                     len(new_columns), new_names, policy.on_new_column)

            if policy.on_new_column == "ignore":
                self._log_step(run, "schema_check", f"ignored {len(new_columns)} new column(s)")
                return "ok"

            # Build drift info for the agent
            drift_info = {"new_columns": new_columns}
            target_type = "postgresql"
            try:
                target_type = target.get_target_type()
            except Exception:
                pass

            # Agent generates migration SQL (uses LLM if available, rule-based fallback)
            if self.agent:
                migration = await self.agent.generate_migration_sql(
                    contract, drift_info, target_type,
                )
            else:
                # No agent available — use inline fallback
                from agent.core import AgentCore
                migration = AgentCore._rule_based_migration_sql(
                    None, contract, drift_info, target_type,
                )

            migration_sql = migration.get("migration_sql", [])
            reasoning = migration.get("reasoning", "")
            risk_assessment = migration.get("risk_assessment", "")
            rollback_sql = migration.get("rollback_sql", [])

            if not migration_sql:
                self._log_step(run, "schema_check", "agent generated no migration SQL — proceeding")
                return "ok"

            if policy.on_new_column == "auto_add":
                # Agent-generated SQL, auto-applied
                for stmt in migration_sql:
                    await target.execute_sql(stmt)
                # Update column mappings to match
                for col_info in new_columns:
                    col_name = col_info["name"]
                    if col_name in live_cols:
                        contract.column_mappings.append(live_cols[col_name])
                contract.version += 1
                contract.updated_at = now_iso()
                await self.store.save_pipeline(contract)
                sv = SchemaVersion(
                    pipeline_id=contract.pipeline_id,
                    version=contract.version,
                    column_mappings=contract.column_mappings,
                    change_summary=f"Pre-extract auto-add: {new_names}. Agent reasoning: {reasoning}",
                    change_type="add_column",
                    applied_by="agent",
                )
                await self.store.save_schema_version(sv)
                self._log_step(run, "schema_check",
                    f"auto-applied {len(migration_sql)} agent-generated statement(s) for {new_names}")
                return "applied"

            # policy.on_new_column == "propose" — agent SQL included in proposal
            proposal = ContractChangeProposal(
                pipeline_id=contract.pipeline_id,
                trigger_type=TriggerType.SCHEMA_DRIFT,
                trigger_detail={
                    "new_columns": new_columns,
                    "detected_at": "pre_extract",
                    "target_type": target_type,
                },
                change_type=ChangeType.ADD_COLUMN,
                current_state={"column_mappings": [asdict(m) for m in contract.column_mappings]},
                proposed_state={
                    "new_columns": new_columns,
                    "migration_sql": migration_sql,
                    "rollback_sql": rollback_sql,
                },
                reasoning=f"{reasoning}\n\nRisk: {risk_assessment}",
                confidence=0.9,
                contract_version_before=contract.version,
            )
            await self.store.save_proposal(proposal)

            # Halt the run — needs human approval of agent-generated SQL
            run.status = RunStatus.HALTED
            run.error = (
                f"Schema drift: {len(new_columns)} new column(s) need approval: {new_names}. "
                f"Agent generated {len(migration_sql)} SQL statement(s)."
            )
            run.completed_at = now_iso()
            await self.store.save_run(run)
            self._log_step(run, "schema_check",
                f"HALTED — agent proposed {len(migration_sql)} SQL statement(s) for approval", "halted")
            log.warning("Run %s halted: agent-generated schema migration needs approval", run.run_id[:8])
            return "halted"

        except Exception as e:
            # Don't block the run if schema check fails — log and proceed
            log.warning("Pre-extract schema check failed for %s: %s", contract.pipeline_id, e)
            self._log_step(run, "schema_check", f"check failed: {e}", "warn")
            return "ok"

    async def _execute_inner(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> RunRecord:
        """Dispatch to legacy or step-DAG execution path."""
        if contract.steps:
            return await self._execute_step_dag(contract, run)
        return await self._execute_legacy(contract, run)

    async def _execute_legacy(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> RunRecord:
        """Legacy execution: fixed extract → load → gate → promote flow."""
        self._step_t0 = _time.time()
        run.execution_log = []
        self._log_step(run, "start", f"mode={run.run_mode.value}")
        log.info("Starting run (mode=%s)", run.run_mode.value)
        try:
            # 1. Preflight checks
            if not await self._preflight(contract, run):
                self._log_step(run, "preflight", "failed", "error")
                await self._update_error_budget(contract, run)
                return run
            self._log_step(run, "preflight", "passed")

            # Build 15: Load upstream run context for data-triggered runs
            upstream_run = None
            upstream_context = {}
            if run.triggered_by_run_id:
                upstream_run = await self.store.get_run(run.triggered_by_run_id)
                # Build 28: Load enriched upstream context (quality, gate, metadata)
                try:
                    upstream_context = await self.store.load_upstream_context_for_run(run)
                except Exception as uc_err:
                    log.warning("Failed to load upstream context: %s", uc_err)

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
            self._log_step(run, "connectors", f"source={contract.source_connector_id[:8]}, target={contract.target_connector_id[:8]}")

            # Pre-extract schema drift check
            drift_result = await self._pre_extract_schema_check(
                contract, run, source, target,
            )
            if drift_result == "halted":
                # Run halted pending approval — not an error, just needs human review
                return run

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
            self._log_step(run, "extract", f"{run.rows_extracted} rows, {run.staging_size_bytes} bytes staged")

            # 3. Skip if incremental with 0 rows
            if (
                run.rows_extracted == 0
                and contract.refresh_type.value == "incremental"
            ):
                log.info("No new rows -- marking complete.")
                self._log_step(run, "skip", "no new rows (incremental)")
                run.status = RunStatus.COMPLETE
                run.completed_at = now_iso()
                self._log_step(run, "complete", "0 rows — skipped")
                await self.store.save_run(run)
                await self._update_error_budget(contract, run)
                return run

            # 4. LOADING to staging
            run.status = RunStatus.LOADING
            await self.store.save_run(run)
            await target.load_staging(contract, run)
            log.info("Loaded to staging")
            self._log_step(run, "load_staging", f"{run.rows_extracted} rows to staging table")

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
            self._log_step(run, "quality_gate", f"decision={gate_record.decision.value}, {len(gate_record.checks)} checks")

            # 6. On HALT: preserve staging, don't clean up
            if gate_record.decision == GateDecision.HALT:
                self._log_step(run, "halt", "quality gate halted run", "warn")
                result = await self._handle_halt(contract, run, gate_record, target)
                await self._update_error_budget(contract, result)
                return result

            # 7. PROMOTING (PROMOTE or PROMOTE_WITH_WARNING)
            run.status = RunStatus.PROMOTING
            await self.store.save_run(run)
            await target.promote(contract, run)
            log.info("Promoted to target")
            self._log_step(run, "promote", f"staging → {contract.target_table}")

            # 8. Update watermark (skip for backfills)
            if (
                run.run_mode != RunMode.BACKFILL
                and extract_result.max_watermark
            ):
                run.watermark_after = extract_result.max_watermark
                contract.last_watermark = extract_result.max_watermark
                self._log_step(run, "watermark", f"{run.watermark_before} → {run.watermark_after}")

            # Update baselines
            await self._update_baselines(contract, run)
            await self.store.save_pipeline(contract)

            # Cleanup staging
            self.staging.cleanup_run(contract.pipeline_id, run.run_id)
            self._log_step(run, "cleanup", "staging table dropped")

            # Track column lineage after successful promotion
            await self._track_column_lineage(contract)
            self._log_step(run, "column_lineage", "tracked")

            # Write pipeline metadata (XCom-style) + Build 28 upstream context propagation
            await self._write_run_metadata(contract, run, extract_result, upstream_run, upstream_context)
            self._log_step(run, "metadata", "run metadata written")

            # Execute post-promotion SQL hooks (with enriched upstream context)
            await self._execute_post_promotion_hooks(contract, run, target, upstream_run, upstream_context)
            self._log_step(run, "hooks", "post-promotion hooks executed")

            # 9. Mark COMPLETE
            run.status = RunStatus.COMPLETE
            run.completed_at = now_iso()
            self._log_step(run, "complete", f"{run.rows_extracted} rows promoted successfully")

            # 10. Generate run insights
            await self._generate_insights(contract, run)

            await self.store.save_run(run)
            log.info("Run complete -- %d rows extracted", run.rows_extracted)

            # 11. Update error budget
            await self._update_error_budget(contract, run)

            return run

        except Exception as e:
            log.exception("Run failed: %s", e)
            self._log_step(run, "error", str(e), "error")
            run.error = str(e)
            run.status = RunStatus.FAILED
            run.completed_at = now_iso()

            # Agent diagnoses the failure and recommends action
            diagnosis = {}
            if self.agent:
                try:
                    diagnosis = await self.agent.diagnose_run_failure(
                        contract, str(e), run.execution_log or [],
                    )
                    diag_detail = (
                        f"Category: {diagnosis.get('category', 'unknown')}. "
                        f"{diagnosis.get('root_cause', '')} "
                        f"Action: {diagnosis.get('recommended_action', '')}"
                    )
                    self._log_step(run, "agent_diagnosis", diag_detail)
                    # Enrich run error with agent diagnosis
                    run.error = (
                        f"{str(e)} | Agent: {diagnosis.get('root_cause', '')} "
                        f"[{diagnosis.get('category', 'unknown')}] "
                        f"→ {diagnosis.get('recommended_action', '')}"
                    )
                except Exception as diag_err:
                    log.warning("Agent failure diagnosis error: %s", diag_err)

            # Generate insights even for failed runs
            await self._generate_insights(contract, run)

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

            # Create alert if agent says this needs attention
            if diagnosis.get("should_alert", True):
                try:
                    alert = AlertRecord(
                        severity=AlertSeverity.CRITICAL if contract.tier == 1 else AlertSeverity.WARNING,
                        tier=contract.tier,
                        pipeline_id=contract.pipeline_id,
                        pipeline_name=contract.pipeline_name,
                        summary=f"Run failed: {diagnosis.get('root_cause', str(e)[:100])}",
                        detail={
                            "error": str(e),
                            "category": diagnosis.get("category", "unknown"),
                            "is_transient": diagnosis.get("is_transient", False),
                            "recommended_action": diagnosis.get("recommended_action", ""),
                        },
                        narrative=diagnosis.get("root_cause", ""),
                    )
                    await self.store.save_alert(alert)
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
        """Returns False if run should be skipped. Agent reasons about failures."""

        failure_reason = None
        context = {}

        # Check pending halt proposals
        if await self.store.has_pending_halt_proposal(contract.pipeline_id):
            failure_reason = "Pending halt proposal requires approval before running."
            context["blocker"] = "halt_proposal"

        # Check disk space
        if not failure_reason:
            has_space, used_pct = self.staging.check_disk_space(
                self.config.max_disk_pct,
            )
            if not has_space:
                failure_reason = f"Insufficient disk space: {used_pct:.0%} used."
                context["blocker"] = "disk_space"
                context["disk_used_pct"] = round(used_pct * 100, 1)

        # Check upstream dependencies
        if not failure_reason:
            deps = await self.store.list_dependencies(contract.pipeline_id)
            for dep in deps:
                last_run = await self.store.get_last_successful_run(dep.depends_on_id)
                if last_run is None:
                    upstream = await self.store.get_pipeline(dep.depends_on_id)
                    name = upstream.pipeline_name if upstream else dep.depends_on_id
                    failure_reason = f"Upstream pipeline {name} has not completed successfully."
                    context["blocker"] = "upstream_dependency"
                    context["upstream_pipeline"] = name
                    context["upstream_id"] = dep.depends_on_id
                    break

        # Validate connectors are active
        if not failure_reason:
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
                    failure_reason = f"{label.capitalize()} connector is not active."
                    context["blocker"] = "connector_inactive"
                    context["connector_label"] = label
                    break

        if not failure_reason:
            return True

        # Agent reasons about the preflight failure
        if self.agent:
            try:
                assessment = await self.agent.reason_about_preflight_failure(
                    contract, failure_reason, context,
                )
                enriched_error = (
                    f"{failure_reason} | Agent: {assessment.get('diagnosis', '')} "
                    f"→ {assessment.get('recommended_action', '')}"
                )
                self._log_step(run, "preflight_diagnosis", enriched_error)
                run.error = enriched_error
            except Exception as e:
                log.warning("Agent preflight reasoning failed: %s", e)
                run.error = failure_reason
        else:
            run.error = failure_reason

        log.warning("Preflight failed: %s", failure_reason)
        run.status = RunStatus.FAILED
        run.completed_at = now_iso()
        await self.store.save_run(run)
        return False

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
        """Agentic halt handling: diagnose, propose fix, create alert, generate insights."""
        log.warning("Quality gate HALT. Diagnosing with agent...")
        run.status = RunStatus.HALTED
        run.completed_at = now_iso()

        # --- Agent diagnoses the halt and proposes a fix ---
        diagnosis = {}
        if self.agent:
            try:
                diagnosis = await self.agent.diagnose_halt(
                    contract,
                    gate.checks,
                    gate.agent_reasoning or "",
                )
                diag_detail = (
                    f"Category: {diagnosis.get('category', 'unknown')}. "
                    f"{diagnosis.get('root_cause', '')} "
                    f"Fix: {diagnosis.get('recommended_action', '')}"
                )
                self._log_step(run, "agent_diagnosis", diag_detail)

                # Enrich run error with short summary (full diagnosis in diagnosis panel)
                root = diagnosis.get('root_cause', 'unknown')
                # Truncate long LLM root causes to first sentence
                if len(root) > 150:
                    root = root[:root.find('. ', 50) + 1] if '. ' in root[50:] else root[:150] + '…'
                category = diagnosis.get('category', 'unknown')
                run.error = f"Quality gate HALT — {root} [{category}]"
            except Exception as diag_err:
                log.warning("Agent halt diagnosis error: %s", diag_err)
                run.error = "Quality gate HALT — review failed checks"

        if not diagnosis:
            run.error = "Quality gate HALT — review failed checks"

        # --- Create approval proposal with the fix ---
        if diagnosis.get("fix_sql") or diagnosis.get("fix_config"):
            try:
                fix_sql = diagnosis.get("fix_sql", [])
                fix_config = diagnosis.get("fix_config", {})
                proposed = {}
                if fix_sql:
                    proposed["sql"] = fix_sql
                if fix_config:
                    proposed["quality_config"] = fix_config

                proposal = ContractChangeProposal(
                    pipeline_id=contract.pipeline_id,
                    trigger_type=TriggerType.QUALITY_ALERT,
                    change_type=ChangeType.QUALITY_FIX,
                    current_state={
                        "run_id": run.run_id,
                        "gate_decision": gate.decision.value,
                        "failed_checks": [
                            {"name": c.check_name, "status": c.status.value, "detail": c.detail}
                            for c in gate.checks if c.status.value == "fail"
                        ],
                    },
                    proposed_state=proposed,
                    reasoning=diagnosis.get("recommended_action", "Agent-proposed fix for quality gate halt"),
                    confidence=diagnosis.get("confidence", 0.5),
                    impact_analysis={
                        "category": diagnosis.get("category", "unknown"),
                        "fix_type": diagnosis.get("fix_type", "manual"),
                        "auto_fixable": diagnosis.get("auto_fixable", False),
                    },
                    rollback_plan="Revert ALTER TABLE changes or restore quality config" if fix_sql else "",
                )
                await self.store.save_proposal(proposal)
                self._log_step(run, "proposal_created",
                               f"Fix proposal {proposal.proposal_id[:8]} created for approval")
                log.info("Created halt fix proposal %s for pipeline %s",
                         proposal.proposal_id[:8], contract.pipeline_id[:8])
            except Exception as prop_err:
                log.warning("Failed to create halt fix proposal: %s", prop_err)

        # --- Generate insights ---
        await self._generate_insights(contract, run)

        await self.store.save_run(run)

        # --- Create alert ---
        if diagnosis.get("should_alert", True):
            try:
                alert = AlertRecord(
                    severity=AlertSeverity.CRITICAL if contract.tier == 1 else AlertSeverity.WARNING,
                    tier=contract.tier,
                    pipeline_id=contract.pipeline_id,
                    pipeline_name=contract.pipeline_name,
                    summary=f"Quality gate HALT: {diagnosis.get('root_cause', 'check failures')[:100]}",
                    detail={
                        "gate_decision": gate.decision.value,
                        "category": diagnosis.get("category", "unknown"),
                        "fix_type": diagnosis.get("fix_type", "manual"),
                        "recommended_action": diagnosis.get("recommended_action", ""),
                        "failed_checks": [c.check_name for c in gate.checks if c.status.value == "fail"],
                    },
                    narrative=diagnosis.get("root_cause", ""),
                )
                await self.store.save_alert(alert)
            except Exception:
                pass

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
    # Run insights
    # ------------------------------------------------------------------

    async def _generate_insights(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> None:
        """Generate agent insights for a completed run. Non-blocking — never fails the run."""
        if not self.agent:
            return
        try:
            prior_runs = await self.store.list_runs(contract.pipeline_id, limit=10)
            # Exclude current run from priors
            prior_runs = [r for r in prior_runs if r.run_id != run.run_id]
            run.insights = await self.agent.generate_run_insights(
                contract, run, prior_runs,
            )
            count = len(run.insights) if run.insights else 0
            if count:
                self._log_step(run, "insights", f"{count} insight(s) generated")
        except Exception as e:
            log.warning("Insight generation error: %s", e)

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

            # Agent diagnoses when budget becomes exhausted
            if budget.escalated and not was_escalated:
                budget_info = {
                    "success_rate": success_rate,
                    "threshold": budget.budget_threshold,
                    "total_runs": total_runs,
                    "failed_runs": failed_runs,
                    "window_days": budget.window_days,
                }

                # Agent diagnosis
                diagnosis = {}
                if self.agent:
                    try:
                        diagnosis = await self.agent.diagnose_error_budget(
                            contract, budget_info, runs,
                        )
                    except Exception as e:
                        log.warning("Agent budget diagnosis failed: %s", e)

                summary = (
                    f"Error budget exhausted: {success_rate:.1%} success rate "
                    f"over {budget.window_days}d window "
                    f"(threshold: {budget.budget_threshold:.0%}). "
                    f"{failed_runs}/{total_runs} runs failed."
                )
                if diagnosis.get("diagnosis"):
                    summary += f" Agent diagnosis: {diagnosis['diagnosis']}"

                alert = AlertRecord(
                    severity=AlertSeverity.CRITICAL,
                    tier=contract.tier,
                    pipeline_id=contract.pipeline_id,
                    pipeline_name=contract.pipeline_name,
                    summary=summary,
                    detail={
                        **budget_info,
                        "agent_diagnosis": diagnosis.get("diagnosis", ""),
                        "failure_pattern": diagnosis.get("pattern", "unknown"),
                        "recommended_actions": diagnosis.get("recommended_actions", []),
                        "should_pause": diagnosis.get("should_pause", False),
                        "estimated_recovery": diagnosis.get("estimated_recovery", ""),
                    },
                    narrative=diagnosis.get("diagnosis", ""),
                )
                await self.store.save_alert(alert)
                log.error(
                    "Error budget EXHAUSTED: %.1f%% success (%d/%d runs). Pattern: %s",
                    success_rate * 100, successful_runs, total_runs,
                    diagnosis.get("pattern", "unknown"),
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
        upstream_context: dict = None,
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
            # Build 28: Write gate decision and quality summary
            gate_str = ""
            if run.gate_decision:
                gate_str = run.gate_decision.value if hasattr(run.gate_decision, "value") else str(run.gate_decision)
            await self.store.set_metadata(
                pid, "last_gate_decision", {"value": gate_str}, rid,
            )
            if run.quality_results:
                await self.store.set_metadata(
                    pid, "last_quality_summary", {
                        "decision": run.quality_results.get("decision", ""),
                        "checks": run.quality_results.get("checks", []),
                    }, rid,
                )

            # Build 15 + 28: Write upstream context for data-triggered runs
            if upstream_run:
                uc = upstream_context or {}
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
                # Build 28: Propagate upstream quality and gate context
                if contract.auto_propagate_context:
                    await self.store.set_metadata(
                        pid, "upstream_gate_decision",
                        {"value": uc.get("upstream_gate_decision", "")},
                        rid, namespace="upstream",
                    )
                    await self.store.set_metadata(
                        pid, "upstream_quality_decision",
                        {"value": uc.get("upstream_quality_decision", "")},
                        rid, namespace="upstream",
                    )
                    await self.store.set_metadata(
                        pid, "upstream_quality_checks_passed",
                        {"value": uc.get("upstream_quality_checks_passed", "0")},
                        rid, namespace="upstream",
                    )
                    await self.store.set_metadata(
                        pid, "upstream_quality_checks_warned",
                        {"value": uc.get("upstream_quality_checks_warned", "0")},
                        rid, namespace="upstream",
                    )
                    await self.store.set_metadata(
                        pid, "upstream_quality_checks_failed",
                        {"value": uc.get("upstream_quality_checks_failed", "0")},
                        rid, namespace="upstream",
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
        upstream_context: dict = None,
    ) -> str:
        """Replace {{variable}} placeholders with run context values.

        Supported variables (15 run + 10 connection + 17 upstream = 42 total):

        Run context (15):
          {{pipeline_id}}, {{pipeline_name}},
          {{run_id}}, {{run_mode}},
          {{watermark_before}}, {{watermark_after}},
          {{rows_extracted}}, {{rows_loaded}},
          {{started_at}}, {{completed_at}},
          {{source_schema}}, {{source_table}},
          {{target_schema}}, {{target_table}},
          {{batch_id}} (alias for run_id[:8])

        Connection/environment (10):
          {{environment}},
          {{source_host}}, {{source_database}}, {{source_user}}, {{source_port}},
          {{target_host}}, {{target_database}}, {{target_user}}, {{target_port}},
          {{target_ddl}}

        Upstream (17 — Build 15 + Build 28):
          {{upstream_run_id}}, {{upstream_pipeline_id}}, {{upstream_pipeline_name}},
          {{upstream_watermark_before}}, {{upstream_watermark_after}},
          {{upstream_rows_extracted}}, {{upstream_rows_loaded}},
          {{upstream_started_at}}, {{upstream_completed_at}},
          {{upstream_batch_id}},
          {{upstream_gate_decision}},
          {{upstream_quality_decision}}, {{upstream_quality_checks_passed}},
          {{upstream_quality_checks_warned}}, {{upstream_quality_checks_failed}},
          {{upstream_metadata.<key>}} (dynamic, from upstream pipeline metadata)
        """
        # Start with upstream_context if provided (Build 28 enriched context)
        uc = upstream_context or {}

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
            # Connection/environment variables
            "environment": contract.environment,
            "source_host": contract.source_host,
            "source_database": contract.source_database,
            "source_user": contract.source_user,
            "source_port": str(contract.source_port),
            "target_host": contract.target_host,
            "target_database": contract.target_database,
            "target_user": contract.target_user,
            "target_port": str(contract.target_port),
            "target_ddl": contract.target_ddl or "",
            # Upstream context (Build 28 enriched; falls back to Build 15 upstream_run)
            "upstream_run_id": uc.get("upstream_run_id", upstream_run.run_id if upstream_run else ""),
            "upstream_pipeline_id": uc.get("upstream_pipeline_id", upstream_run.pipeline_id if upstream_run else ""),
            "upstream_pipeline_name": uc.get("upstream_pipeline_name", ""),
            "upstream_watermark_before": uc.get("upstream_watermark_before", (upstream_run.watermark_before or "") if upstream_run else ""),
            "upstream_watermark_after": uc.get("upstream_watermark_after", (upstream_run.watermark_after or "") if upstream_run else ""),
            "upstream_rows_extracted": uc.get("upstream_rows_extracted", str(upstream_run.rows_extracted) if upstream_run else "0"),
            "upstream_rows_loaded": uc.get("upstream_rows_loaded", str(upstream_run.rows_loaded) if upstream_run else "0"),
            "upstream_started_at": uc.get("upstream_started_at", (upstream_run.started_at or "") if upstream_run else ""),
            "upstream_completed_at": uc.get("upstream_completed_at", (upstream_run.completed_at or "") if upstream_run else ""),
            "upstream_batch_id": uc.get("upstream_batch_id", upstream_run.run_id[:8] if upstream_run else ""),
            # Build 28: upstream quality and gate context
            "upstream_gate_decision": uc.get("upstream_gate_decision", ""),
            "upstream_quality_decision": uc.get("upstream_quality_decision", ""),
            "upstream_quality_checks_passed": uc.get("upstream_quality_checks_passed", "0"),
            "upstream_quality_checks_warned": uc.get("upstream_quality_checks_warned", "0"),
            "upstream_quality_checks_failed": uc.get("upstream_quality_checks_failed", "0"),
        }

        rendered = sql
        for key, value in replacements.items():
            rendered = rendered.replace("{{" + key + "}}", str(value))

        # Build 28: Dynamic upstream_metadata.* variables
        upstream_meta = uc.get("upstream_metadata", {})
        if upstream_meta:
            import re
            for match in re.finditer(r"\{\{upstream_metadata\.(\w+)\}\}", rendered):
                meta_key = match.group(1)
                meta_val = upstream_meta.get(meta_key, "")
                rendered = rendered.replace(match.group(0), str(meta_val))

        return rendered

    async def _execute_post_promotion_hooks(
        self,
        contract: PipelineContract,
        run: RunRecord,
        target,
        upstream_run: RunRecord = None,
        upstream_context: dict = None,
    ) -> None:
        """Execute SQL hooks against the target and store results as metadata."""
        hooks = [h for h in contract.post_promotion_hooks if h.enabled]
        if not hooks:
            return

        log.info("Executing %d post-promotion hook(s)", len(hooks))
        for hook in hooks:
            rendered_sql = self._render_hook_sql(hook.sql, contract, run, upstream_run, upstream_context)

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

    # ==================================================================
    # Step DAG executor (Build 18)
    # ==================================================================

    @staticmethod
    def _topo_sort(steps: list[StepDefinition]) -> list[StepDefinition]:
        """Topological sort of step definitions by depends_on.

        Returns steps in execution order. Raises ValueError on cycles.
        """
        by_id = {s.step_id: s for s in steps}
        in_degree = {s.step_id: 0 for s in steps}
        dependents: dict[str, list[str]] = {s.step_id: [] for s in steps}

        for s in steps:
            for dep_id in s.depends_on:
                if dep_id not in by_id:
                    raise ValueError(
                        f"Step '{s.step_name}' depends on unknown step_id '{dep_id}'"
                    )
                in_degree[s.step_id] += 1
                dependents[dep_id].append(s.step_id)

        queue = [sid for sid, deg in in_degree.items() if deg == 0]
        ordered = []
        while queue:
            # Sort for deterministic order among peers
            queue.sort()
            sid = queue.pop(0)
            ordered.append(by_id[sid])
            for child in dependents[sid]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(steps):
            raise ValueError("Cycle detected in step dependencies")
        return ordered

    async def _execute_step_dag(
        self,
        contract: PipelineContract,
        run: RunRecord,
    ) -> RunRecord:
        """Execute pipeline as a DAG of composable steps."""
        self._step_t0 = _time.time()
        run.execution_log = []
        self._log_step(run, "start", f"mode={run.run_mode.value}, steps={len(contract.steps)}")
        log.info("Starting step-DAG run (mode=%s, %d steps)", run.run_mode.value, len(contract.steps))

        # Filter enabled steps only
        enabled_steps = [s for s in contract.steps if s.enabled]
        if not enabled_steps:
            log.warning("No enabled steps — marking complete.")
            run.status = RunStatus.COMPLETE
            run.completed_at = now_iso()
            self._log_step(run, "complete", "no enabled steps")
            await self.store.save_run(run)
            return run

        # Topological sort
        try:
            ordered = self._topo_sort(enabled_steps)
        except ValueError as e:
            run.status = RunStatus.FAILED
            run.error = f"Step DAG validation failed: {e}"
            run.completed_at = now_iso()
            self._log_step(run, "error", run.error, "error")
            await self.store.save_run(run)
            return run

        # Preflight
        if not await self._preflight(contract, run):
            self._log_step(run, "preflight", "failed", "error")
            await self._update_error_budget(contract, run)
            return run
        self._log_step(run, "preflight", "passed")

        # Resolve connectors once (steps that need them pull from context)
        # Transform-only pipelines may have no source connector
        source = None
        target = None
        if contract.source_connector_id:
            src_params, tgt_params = self._connector_params(contract, "both")
            source = await self.registry.get_source(contract.source_connector_id, src_params)
            target = await self.registry.get_target(contract.target_connector_id, tgt_params)
            await target.create_table_if_not_exists(contract)
            self._log_step(run, "connectors", f"source={contract.source_connector_id[:8]}, target={contract.target_connector_id[:8]}")
        elif contract.target_connector_id:
            _, tgt_params = self._connector_params(contract, "target")
            target = await self.registry.get_target(contract.target_connector_id, tgt_params)
            self._log_step(run, "connectors", f"target-only={contract.target_connector_id[:8]}")

        # Load upstream run context
        upstream_run = None
        upstream_context = {}
        if run.triggered_by_run_id:
            upstream_run = await self.store.get_run(run.triggered_by_run_id)
            # Build 28: Load enriched upstream context
            try:
                upstream_context = await self.store.load_upstream_context_for_run(run)
            except Exception as uc_err:
                log.warning("Failed to load upstream context: %s", uc_err)

        # Step context: shared dict passed between steps (XCom equivalent)
        ctx: dict = {
            "source": source,
            "target": target,
            "contract": contract,
            "run": run,
            "upstream_run": upstream_run,
            "upstream_context": upstream_context,
            "extract_result": None,
            "gate_record": None,
        }
        step_statuses: dict[str, StepStatus] = {}

        try:
            for step_def in ordered:
                # Check if all dependencies succeeded
                deps_ok = all(
                    step_statuses.get(d) == StepStatus.COMPLETE
                    for d in step_def.depends_on
                )
                if not deps_ok:
                    if step_def.skip_on_fail:
                        step_statuses[step_def.step_id] = StepStatus.SKIPPED
                        self._log_step(run, f"step:{step_def.step_name}", "skipped (dependency failed)", "warn")
                        await self._save_step_exec(
                            run, contract, step_def, StepStatus.SKIPPED,
                            error="Dependency not met",
                        )
                        continue
                    else:
                        step_statuses[step_def.step_id] = StepStatus.FAILED
                        self._log_step(run, f"step:{step_def.step_name}", "failed (dependency not met)", "error")
                        await self._save_step_exec(
                            run, contract, step_def, StepStatus.FAILED,
                            error="Dependency not met",
                        )
                        continue

                # Execute the step
                result_status = await self._execute_step(step_def, ctx, run, contract)
                step_statuses[step_def.step_id] = result_status

                # If a step halted (quality gate), stop the DAG
                if result_status == StepStatus.HALTED:
                    run.status = RunStatus.HALTED
                    run.completed_at = now_iso()
                    self._log_step(run, "halt", f"step '{step_def.step_name}' halted the run", "warn")
                    await self.store.save_run(run)
                    await self._update_error_budget(contract, run)
                    return run

                # If a step failed and skip_on_fail is False, fail the run
                if result_status == StepStatus.FAILED and not step_def.skip_on_fail:
                    run.status = RunStatus.FAILED
                    run.completed_at = now_iso()
                    self._log_step(run, "error", f"step '{step_def.step_name}' failed", "error")
                    await self.store.save_run(run)
                    await self._update_error_budget(contract, run)
                    return run

            # All steps done — finalize
            run.status = RunStatus.COMPLETE
            run.completed_at = now_iso()

            # Update baselines and watermark from context
            extract_result = ctx.get("extract_result")
            if extract_result and run.run_mode != RunMode.BACKFILL and extract_result.max_watermark:
                run.watermark_after = extract_result.max_watermark
                contract.last_watermark = extract_result.max_watermark

            await self._update_baselines(contract, run)
            await self.store.save_pipeline(contract)
            await self._track_column_lineage(contract)
            await self._write_run_metadata(contract, run, extract_result, upstream_run, upstream_context)

            self._log_step(run, "complete", f"{run.rows_extracted} rows, {len(ordered)} steps executed")
            await self._generate_insights(contract, run)
            await self.store.save_run(run)
            await self._update_error_budget(contract, run)
            return run

        except Exception as e:
            log.exception("Step-DAG run failed: %s", e)
            self._log_step(run, "error", str(e), "error")
            run.error = str(e)
            run.status = RunStatus.FAILED
            run.completed_at = now_iso()
            await self._generate_insights(contract, run)
            await self.store.save_run(run)
            await self._update_error_budget(contract, run)
            return run

    async def _execute_step(
        self,
        step_def: StepDefinition,
        ctx: dict,
        run: RunRecord,
        contract: PipelineContract,
    ) -> StepStatus:
        """Execute a single step and return its status."""
        t0 = _time.monotonic()
        step_exec = StepExecution(
            run_id=run.run_id,
            pipeline_id=contract.pipeline_id,
            step_id=step_def.step_id,
            step_name=step_def.step_name,
            step_type=step_def.step_type.value if hasattr(step_def.step_type, "value") else step_def.step_type,
            status=StepStatus.RUNNING,
            started_at=now_iso(),
        )

        handler = self._step_handlers.get(step_def.step_type)
        if not handler:
            step_exec.status = StepStatus.FAILED
            step_exec.error = f"Unknown step type: {step_def.step_type}"
            step_exec.completed_at = now_iso()
            step_exec.elapsed_ms = int((_time.monotonic() - t0) * 1000)
            await self.store.save_step_execution(step_exec)
            self._log_step(run, f"step:{step_def.step_name}", step_exec.error, "error")
            return StepStatus.FAILED

        retry_count = 0
        max_retries = step_def.retry_max

        while True:
            try:
                output = await handler(self, step_def, ctx, run, contract)
                step_exec.status = StepStatus.COMPLETE
                step_exec.output = _json_safe(output) if output else {}
                step_exec.completed_at = now_iso()
                step_exec.elapsed_ms = int((_time.monotonic() - t0) * 1000)
                step_exec.retry_count = retry_count
                await self.store.save_step_execution(step_exec)
                self._log_step(run, f"step:{step_def.step_name}", f"complete ({step_exec.elapsed_ms}ms)")
                return StepStatus.COMPLETE

            except _StepHalt as e:
                step_exec.status = StepStatus.HALTED
                step_exec.error = str(e)
                step_exec.completed_at = now_iso()
                step_exec.elapsed_ms = int((_time.monotonic() - t0) * 1000)
                step_exec.retry_count = retry_count
                await self.store.save_step_execution(step_exec)
                self._log_step(run, f"step:{step_def.step_name}", f"halted: {e}", "warn")
                return StepStatus.HALTED

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    log.warning(
                        "Step '%s' failed (attempt %d/%d): %s",
                        step_def.step_name, retry_count, max_retries + 1, e,
                    )
                    continue

                step_exec.status = StepStatus.FAILED
                step_exec.error = str(e)
                step_exec.completed_at = now_iso()
                step_exec.elapsed_ms = int((_time.monotonic() - t0) * 1000)
                step_exec.retry_count = retry_count - 1
                await self.store.save_step_execution(step_exec)
                self._log_step(run, f"step:{step_def.step_name}", f"failed: {e}", "error")
                return StepStatus.FAILED

    async def _save_step_exec(
        self,
        run: RunRecord,
        contract: PipelineContract,
        step_def: StepDefinition,
        status: StepStatus,
        error: str = "",
    ) -> None:
        """Save a step execution record for skipped/blocked steps."""
        step_exec = StepExecution(
            run_id=run.run_id,
            pipeline_id=contract.pipeline_id,
            step_id=step_def.step_id,
            step_name=step_def.step_name,
            step_type=step_def.step_type.value if hasattr(step_def.step_type, "value") else step_def.step_type,
            status=status,
            started_at=now_iso(),
            completed_at=now_iso(),
            error=error,
        )
        await self.store.save_step_execution(step_exec)

    # ------------------------------------------------------------------
    # Step type handlers
    # ------------------------------------------------------------------

    async def _step_extract(self, step_def, ctx, run, contract):
        """Extract data from source."""
        source = ctx["source"]
        target = ctx["target"]

        # Pre-extract schema drift check
        drift_result = await self._pre_extract_schema_check(
            contract, run, source, target,
        )
        if drift_result == "halted":
            raise RuntimeError("Schema drift: new columns need approval before extraction")

        run.status = RunStatus.EXTRACTING
        run.watermark_before = contract.last_watermark
        await self.store.save_run(run)

        staging_dir = self.staging.ensure_run_dir(contract.pipeline_id, run.run_id)
        extract_result = await source.extract(
            contract, run, staging_dir, self.config.batch_size,
        )
        run.rows_extracted = extract_result.rows_extracted
        run.staging_path = str(extract_result.staging_path)
        run.staging_size_bytes = extract_result.staging_size_bytes
        ctx["extract_result"] = extract_result

        return {
            "rows_extracted": extract_result.rows_extracted,
            "staging_size_bytes": extract_result.staging_size_bytes,
            "max_watermark": extract_result.max_watermark,
        }

    async def _step_transform(self, step_def, ctx, run, contract):
        """Execute SQL transform against target with ref/var resolution and materialization."""
        from transforms.engine import resolve_refs, resolve_vars, execute_materialization, parse_column_lineage
        from contracts.models import ColumnLineage

        target = ctx["target"]
        config = step_def.config

        # Resolve SQL source: catalog transform or inline
        transform_id = config.get("transform_id")
        if transform_id:
            transform = await self.store.get_sql_transform(transform_id)
            if not transform:
                raise ValueError(f"Transform {transform_id} not found in catalog")
            sql = transform.sql
            materialization = transform.materialization
            if hasattr(materialization, "value"):
                materialization = materialization.value
            target_schema = transform.target_schema
            target_table = transform.target_table or transform.transform_name
            variables = {**transform.variables, **config.get("variables", {})}
        else:
            sql = config.get("sql", "")
            materialization = config.get("materialization", "table")
            target_schema = config.get("target_schema", contract.target_schema or "public")
            target_table = config.get("target_table", "")
            variables = config.get("variables", {})

        if not sql:
            raise ValueError("Transform step requires 'sql' in config or a valid transform_id")

        # Phase 1: resolve {{ ref('...') }}
        resolved_sql, refs = await resolve_refs(sql, self.store, contract.pipeline_id)

        # Phase 2: resolve {{ var('...') }}
        resolved_sql = resolve_vars(resolved_sql, variables, getattr(contract, "tags", None) or {})

        # Phase 3: resolve {{ template_vars }} (existing hook variables)
        upstream_run = ctx.get("upstream_run")
        resolved_sql = self._render_hook_sql(resolved_sql, contract, run, upstream_run)

        # Phase 4: execute materialization
        timeout = step_def.timeout_seconds or 300
        unique_key = config.get("unique_key", [])
        result = await execute_materialization(
            target, materialization, target_schema, target_table,
            resolved_sql, unique_key=unique_key, timeout=timeout,
        )

        # Phase 5: track column lineage (best-effort)
        if target_table and refs:
            try:
                lineage_entries = parse_column_lineage(resolved_sql, target_table, refs)
                for entry in lineage_entries:
                    cl = ColumnLineage(
                        source_pipeline_id=contract.pipeline_id,
                        source_schema=entry.get("source_schema", contract.target_schema or "public"),
                        source_table=entry["source_table"],
                        source_column=entry["source_column"],
                        target_pipeline_id=contract.pipeline_id,
                        target_schema=target_schema,
                        target_table=target_table,
                        target_column=entry["target_column"],
                        transformation=entry.get("transformation", "sql_transform"),
                    )
                    await self.store.save_column_lineage(cl)
            except Exception as e:
                log.warning("Could not save transform lineage: %s", e)

        # Store output in step context for downstream steps
        ctx.setdefault("transform_outputs", {})[step_def.step_name] = {
            "schema": target_schema,
            "table": target_table,
            "materialization": materialization,
        }

        result["sql"] = resolved_sql
        result["refs"] = refs
        return result

    async def _step_quality_gate(self, step_def, ctx, run, contract):
        """Run quality gate checks."""
        target = ctx["target"]

        # Skip if 0 rows extracted and incremental
        if run.rows_extracted == 0 and contract.refresh_type.value == "incremental":
            return {"decision": "skip", "reason": "no new rows"}

        run.status = RunStatus.QUALITY_GATE
        await self.store.save_run(run)

        gate_record = await self.gate.run(contract, run, target)
        await self.store.save_gate(gate_record)
        run.gate_decision = gate_record.decision
        run.quality_results = {
            "decision": gate_record.decision.value,
            "checks": [
                {"name": c.check_name, "status": c.status.value, "detail": c.detail}
                for c in gate_record.checks
            ],
        }
        ctx["gate_record"] = gate_record

        if gate_record.decision == GateDecision.HALT:
            raise _StepHalt(f"Quality gate halted: {len(gate_record.checks)} checks")

        return {
            "decision": gate_record.decision.value,
            "checks_count": len(gate_record.checks),
        }

    async def _step_promote(self, step_def, ctx, run, contract):
        """Promote staging data to target table."""
        target = ctx["target"]

        # Skip promotion if 0 rows and incremental
        if run.rows_extracted == 0 and contract.refresh_type.value == "incremental":
            return {"action": "skipped", "reason": "no new rows"}

        run.status = RunStatus.PROMOTING
        await self.store.save_run(run)

        # Load to staging first if not already done
        run.status = RunStatus.LOADING
        await self.store.save_run(run)
        await target.load_staging(contract, run)

        run.status = RunStatus.PROMOTING
        await self.store.save_run(run)
        await target.promote(contract, run)

        return {"rows_promoted": run.rows_extracted}

    async def _step_cleanup(self, step_def, ctx, run, contract):
        """Clean up staging data."""
        self.staging.cleanup_run(contract.pipeline_id, run.run_id)
        return {"action": "staging_cleaned"}

    async def _step_hook(self, step_def, ctx, run, contract):
        """Execute a post-promotion SQL hook."""
        target = ctx["target"]
        sql = step_def.config.get("sql", "")
        if not sql:
            raise ValueError("Hook step requires 'sql' in config")

        upstream_run = ctx.get("upstream_run")
        rendered = self._render_hook_sql(sql, contract, run, upstream_run)

        # Cleanup guard
        if any(kw in rendered.upper() for kw in ("DELETE", "TRUNCATE")):
            if not await self._check_cleanup_allowed(contract):
                return {"action": "skipped", "reason": "cleanup blocked by data contract"}

        rows = await target.execute_sql(rendered, step_def.timeout_seconds or 300)
        result_value = _json_safe(dict(rows[0])) if rows else {}
        return {
            "rows_returned": len(rows) if rows else 0,
            "result": result_value,
        }

    async def _step_sensor(self, step_def, ctx, run, contract):
        """Wait for a condition to be met before proceeding.

        Config options:
          - sql: SQL query that must return at least 1 row
          - timeout_seconds: max wait time (default 300)
          - poll_seconds: check interval (default 30)
        """
        import asyncio
        target = ctx["target"]
        sql = step_def.config.get("sql", "")
        if not sql:
            raise ValueError("Sensor step requires 'sql' in config")

        timeout = step_def.timeout_seconds or step_def.config.get("timeout_seconds", 300)
        poll = step_def.config.get("poll_seconds", 30)
        upstream_run = ctx.get("upstream_run")
        rendered = self._render_hook_sql(sql, contract, run, upstream_run)

        deadline = _time.monotonic() + timeout
        attempts = 0
        while _time.monotonic() < deadline:
            attempts += 1
            rows = await target.execute_sql(rendered, 30)
            if rows:
                return {"triggered": True, "attempts": attempts, "rows": len(rows)}
            await asyncio.sleep(poll)

        raise TimeoutError(f"Sensor timed out after {timeout}s ({attempts} attempts)")

    async def _step_custom(self, step_def, ctx, run, contract):
        """Execute a custom step via its config.

        Currently supports 'sql' execution. Extensible for future step types.
        """
        target = ctx["target"]
        sql = step_def.config.get("sql", "")
        if sql:
            upstream_run = ctx.get("upstream_run")
            rendered = self._render_hook_sql(sql, contract, run, upstream_run)
            rows = await target.execute_sql(rendered, step_def.timeout_seconds or 300)
            return {"rows_returned": len(rows) if rows else 0}
        return {"action": "noop", "reason": "no sql in config"}

    # Map step types to handler methods
    _step_handlers = {
        StepType.EXTRACT: _step_extract,
        StepType.TRANSFORM: _step_transform,
        StepType.QUALITY_GATE: _step_quality_gate,
        StepType.PROMOTE: _step_promote,
        StepType.CLEANUP: _step_cleanup,
        StepType.HOOK: _step_hook,
        StepType.SENSOR: _step_sensor,
        StepType.CUSTOM: _step_custom,
    }


class _StepHalt(Exception):
    """Raised by a step handler to indicate the DAG should halt (not fail)."""
    pass
