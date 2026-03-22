"""
PostgreSQL + pgvector persistence layer for all pipeline-agent entities.

Requires asyncpg and the pgvector extension in the target database.
"""
from __future__ import annotations
import json
import logging
from dataclasses import asdict
from typing import Optional

import asyncpg

from contracts.models import (
    PipelineContract, RunRecord, GateRecord, ContractChangeProposal,
    SchemaVersion, PipelineDependency, NotificationPolicy, FreshnessSnapshot,
    AlertRecord, DecisionLog, AgentPreference, ConnectorRecord,
    ErrorBudget, ColumnLineage, AgentCostLog, ConnectorMigration, User,
    PipelineMetadata, SchemaChangePolicy, PostPromotionHook,
    DataContract, ContractViolation, ChatInteraction, PipelineChangeLog,
    PipelineChangeType, RegisteredSource, SqlTransform, MaterializationType,
    MetricDefinition, MetricSnapshot, MetricType, RunContext, BusinessKnowledge,
    ColumnMapping, QualityConfig, CheckResult,
    PipelineStatus, RunStatus, RunMode, RefreshType, ReplicationMethod,
    LoadType, GateDecision, CheckStatus, ProposalStatus, TriggerType,
    ChangeType, ConnectorStatus, ConnectorType, TestStatus, AlertSeverity,
    FreshnessStatus, DependencyType, PreferenceScope, PreferenceSource,
    DataContractStatus, CleanupOwnership, ContractViolationType,
    now_iso, new_id,
)

log = logging.getLogger(__name__)


class ContractStore:
    """Async PostgreSQL-backed store using an asyncpg connection pool."""

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self, pool: asyncpg.Pool) -> None:
        """Bind an existing asyncpg pool to this store instance."""
        self._pool = pool

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("ContractStore not initialised -- call initialize(pool) first")
        return self._pool

    # ==================================================================
    # Table creation (dev / testing; production uses Alembic migrations)
    # ==================================================================

    async def create_tables(self) -> None:
        """Create all tables with IF NOT EXISTS.  Safe to call repeatedly."""
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await conn.execute(_CREATE_TABLES_SQL)
            # Migrations for existing databases
            await conn.execute(_ALTER_TABLES_SQL)

    # ==================================================================
    # Connectors
    # ==================================================================

    async def save_connector(self, c: ConnectorRecord) -> None:
        c.updated_at = now_iso()
        await self.pool.execute("""
            INSERT INTO connectors (
                connector_id, connector_name, connector_type, source_target_type,
                version, generated_by, interface_version, code,
                dependencies, test_status, test_results,
                generation_attempts, generation_log, status,
                approved_by, approved_at, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            ON CONFLICT (connector_id) DO UPDATE SET
                connector_name = EXCLUDED.connector_name,
                version = EXCLUDED.version,
                code = EXCLUDED.code,
                dependencies = EXCLUDED.dependencies,
                test_status = EXCLUDED.test_status,
                test_results = EXCLUDED.test_results,
                generation_attempts = EXCLUDED.generation_attempts,
                generation_log = EXCLUDED.generation_log,
                status = EXCLUDED.status,
                approved_by = EXCLUDED.approved_by,
                approved_at = EXCLUDED.approved_at,
                updated_at = EXCLUDED.updated_at
        """,
            c.connector_id, c.connector_name, c.connector_type.value,
            c.source_target_type, c.version, c.generated_by,
            c.interface_version, c.code,
            json.dumps(c.dependencies), c.test_status.value,
            json.dumps(c.test_results), c.generation_attempts,
            json.dumps(c.generation_log), c.status.value,
            c.approved_by, c.approved_at, c.created_at, c.updated_at,
        )

    async def get_connector(self, connector_id: str) -> Optional[ConnectorRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM connectors WHERE connector_id = $1", connector_id
        )
        return _row_to_connector(row) if row else None

    async def get_connector_by_name(self, name: str) -> Optional[ConnectorRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM connectors WHERE connector_name = $1", name
        )
        return _row_to_connector(row) if row else None

    async def list_connectors(
        self,
        connector_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[ConnectorRecord]:
        sql = "SELECT * FROM connectors WHERE TRUE"
        params: list = []
        idx = 0
        if connector_type:
            idx += 1
            sql += f" AND connector_type = ${idx}"
            params.append(connector_type)
        if status:
            idx += 1
            sql += f" AND status = ${idx}"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_connector(r) for r in rows]

    # ==================================================================
    # Pipelines
    # ==================================================================

    async def save_pipeline(self, p: PipelineContract) -> None:
        p.updated_at = now_iso()
        mappings_json = json.dumps([asdict(m) for m in p.column_mappings])
        qc_json = json.dumps(asdict(p.quality_config))
        scp_json = json.dumps(asdict(p.schema_change_policy)) if p.schema_change_policy else json.dumps({})
        hooks_json = json.dumps([asdict(h) for h in p.post_promotion_hooks])
        steps_json = json.dumps([asdict(s) for s in p.steps])
        # FK constraints: empty string violates REFERENCES, use NULL instead
        source_cid = p.source_connector_id or None
        target_cid = p.target_connector_id or None
        await self.pool.execute("""
            INSERT INTO pipelines (
                pipeline_id, pipeline_name, version, created_at, updated_at,
                status, environment,
                source_connector_id, source_host, source_port, source_database,
                source_schema, source_table, source_user, source_password,
                target_connector_id, target_host, target_port, target_database,
                target_user, target_password,
                target_schema, target_table, target_options,
                refresh_type, replication_method, incremental_column, last_watermark,
                load_type, merge_keys,
                schedule_cron, retry_max_attempts, retry_backoff_seconds, timeout_seconds,
                column_mappings, target_ddl, quality_config, staging_adapter,
                tier, tier_config, notification_policy_id, tags, owner,
                freshness_column, agent_reasoning,
                baseline_row_count, baseline_null_rates, baseline_null_stddevs,
                baseline_cardinality, baseline_volume_avg, baseline_volume_stddev,
                auto_approve_additive_schema, approval_notification_channel,
                schema_change_policy, post_promotion_hooks, steps,
                semantic_tags, trust_weights, business_context,
                auto_propagate_context
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                $20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,
                $37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,
                $57,$58,$59,$60
            )
            ON CONFLICT (pipeline_id) DO UPDATE SET
                pipeline_name=EXCLUDED.pipeline_name, version=EXCLUDED.version,
                updated_at=EXCLUDED.updated_at, status=EXCLUDED.status,
                environment=EXCLUDED.environment,
                source_connector_id=EXCLUDED.source_connector_id,
                source_host=EXCLUDED.source_host, source_port=EXCLUDED.source_port,
                source_database=EXCLUDED.source_database,
                source_schema=EXCLUDED.source_schema, source_table=EXCLUDED.source_table,
                source_user=EXCLUDED.source_user, source_password=EXCLUDED.source_password,
                target_connector_id=EXCLUDED.target_connector_id,
                target_host=EXCLUDED.target_host, target_port=EXCLUDED.target_port,
                target_database=EXCLUDED.target_database,
                target_user=EXCLUDED.target_user, target_password=EXCLUDED.target_password,
                target_schema=EXCLUDED.target_schema, target_table=EXCLUDED.target_table,
                target_options=EXCLUDED.target_options,
                refresh_type=EXCLUDED.refresh_type,
                replication_method=EXCLUDED.replication_method,
                incremental_column=EXCLUDED.incremental_column,
                last_watermark=EXCLUDED.last_watermark,
                load_type=EXCLUDED.load_type, merge_keys=EXCLUDED.merge_keys,
                schedule_cron=EXCLUDED.schedule_cron,
                retry_max_attempts=EXCLUDED.retry_max_attempts,
                retry_backoff_seconds=EXCLUDED.retry_backoff_seconds,
                timeout_seconds=EXCLUDED.timeout_seconds,
                column_mappings=EXCLUDED.column_mappings,
                target_ddl=EXCLUDED.target_ddl,
                quality_config=EXCLUDED.quality_config,
                staging_adapter=EXCLUDED.staging_adapter,
                tier=EXCLUDED.tier, tier_config=EXCLUDED.tier_config,
                notification_policy_id=EXCLUDED.notification_policy_id,
                tags=EXCLUDED.tags, owner=EXCLUDED.owner,
                freshness_column=EXCLUDED.freshness_column,
                agent_reasoning=EXCLUDED.agent_reasoning,
                baseline_row_count=EXCLUDED.baseline_row_count,
                baseline_null_rates=EXCLUDED.baseline_null_rates,
                baseline_null_stddevs=EXCLUDED.baseline_null_stddevs,
                baseline_cardinality=EXCLUDED.baseline_cardinality,
                baseline_volume_avg=EXCLUDED.baseline_volume_avg,
                baseline_volume_stddev=EXCLUDED.baseline_volume_stddev,
                auto_approve_additive_schema=EXCLUDED.auto_approve_additive_schema,
                approval_notification_channel=EXCLUDED.approval_notification_channel,
                schema_change_policy=EXCLUDED.schema_change_policy,
                post_promotion_hooks=EXCLUDED.post_promotion_hooks,
                steps=EXCLUDED.steps,
                semantic_tags=EXCLUDED.semantic_tags,
                trust_weights=EXCLUDED.trust_weights,
                business_context=EXCLUDED.business_context,
                auto_propagate_context=EXCLUDED.auto_propagate_context
        """,
            p.pipeline_id, p.pipeline_name, p.version, p.created_at, p.updated_at,
            p.status.value, p.environment,
            source_cid, p.source_host, p.source_port, p.source_database,
            p.source_schema, p.source_table, p.source_user, p.source_password,
            target_cid, p.target_host, p.target_port, p.target_database,
            p.target_user, p.target_password,
            p.target_schema, p.target_table,
            json.dumps(p.target_options),
            p.refresh_type.value, p.replication_method.value,
            p.incremental_column, p.last_watermark,
            p.load_type.value, json.dumps(p.merge_keys),
            p.schedule_cron, p.retry_max_attempts, p.retry_backoff_seconds,
            p.timeout_seconds,
            mappings_json, p.target_ddl, qc_json, p.staging_adapter,
            p.tier, json.dumps(p.tier_config), p.notification_policy_id,
            json.dumps(p.tags), p.owner,
            p.freshness_column, json.dumps(p.agent_reasoning),
            p.baseline_row_count, json.dumps(p.baseline_null_rates),
            json.dumps(p.baseline_null_stddevs), json.dumps(p.baseline_cardinality),
            p.baseline_volume_avg, p.baseline_volume_stddev,
            p.auto_approve_additive_schema, p.approval_notification_channel,
            scp_json, hooks_json, steps_json,
            json.dumps(p.semantic_tags), json.dumps(p.trust_weights) if p.trust_weights else None,
            json.dumps(p.business_context),
            p.auto_propagate_context,
        )

    async def get_pipeline(self, pipeline_id: str) -> Optional[PipelineContract]:
        row = await self.pool.fetchrow(
            "SELECT * FROM pipelines WHERE pipeline_id = $1", pipeline_id
        )
        return _row_to_pipeline(row) if row else None

    async def get_pipeline_by_name(self, pipeline_name: str) -> Optional[PipelineContract]:
        row = await self.pool.fetchrow(
            "SELECT * FROM pipelines WHERE pipeline_name = $1", pipeline_name
        )
        return _row_to_pipeline(row) if row else None

    async def list_pipelines(self, status: Optional[str] = None) -> list[PipelineContract]:
        sql = "SELECT * FROM pipelines WHERE TRUE"
        params: list = []
        idx = 0
        if status:
            idx += 1
            sql += f" AND status = ${idx}"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_pipeline(r) for r in rows]

    # ==================================================================
    # Runs
    # ==================================================================

    async def save_run(self, r: RunRecord) -> None:
        await self.pool.execute("""
            INSERT INTO runs (
                run_id, pipeline_id, started_at, completed_at, status,
                run_mode, backfill_start, backfill_end,
                rows_extracted, rows_loaded, watermark_before, watermark_after,
                staging_path, staging_size_bytes,
                drift_detected, quality_results, gate_decision,
                error, retry_count,
                triggered_by_run_id, triggered_by_pipeline_id,
                execution_log, insights
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
            ON CONFLICT (run_id) DO UPDATE SET
                completed_at=EXCLUDED.completed_at, status=EXCLUDED.status,
                rows_extracted=EXCLUDED.rows_extracted, rows_loaded=EXCLUDED.rows_loaded,
                watermark_before=EXCLUDED.watermark_before,
                watermark_after=EXCLUDED.watermark_after,
                staging_path=EXCLUDED.staging_path,
                staging_size_bytes=EXCLUDED.staging_size_bytes,
                drift_detected=EXCLUDED.drift_detected,
                quality_results=EXCLUDED.quality_results,
                gate_decision=EXCLUDED.gate_decision,
                error=EXCLUDED.error, retry_count=EXCLUDED.retry_count,
                triggered_by_run_id=EXCLUDED.triggered_by_run_id,
                triggered_by_pipeline_id=EXCLUDED.triggered_by_pipeline_id,
                execution_log=EXCLUDED.execution_log,
                insights=EXCLUDED.insights
        """,
            r.run_id, r.pipeline_id, r.started_at, r.completed_at,
            r.status.value, r.run_mode.value,
            r.backfill_start, r.backfill_end,
            r.rows_extracted, r.rows_loaded,
            r.watermark_before, r.watermark_after,
            r.staging_path, r.staging_size_bytes,
            json.dumps(r.drift_detected) if r.drift_detected else None,
            json.dumps(r.quality_results) if r.quality_results else None,
            r.gate_decision.value if r.gate_decision else None,
            r.error, r.retry_count,
            r.triggered_by_run_id, r.triggered_by_pipeline_id,
            json.dumps(r.execution_log) if r.execution_log else None,
            json.dumps(r.insights) if r.insights else None,
        )

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        row = await self.pool.fetchrow(
            "SELECT * FROM runs WHERE run_id = $1", run_id
        )
        return _row_to_run(row) if row else None

    async def list_runs(self, pipeline_id: str, limit: int = 50) -> list[RunRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM runs WHERE pipeline_id = $1 ORDER BY started_at DESC LIMIT $2",
            pipeline_id, limit,
        )
        return [_row_to_run(r) for r in rows]

    async def list_stale_runs(self, stale_before: str) -> list[RunRecord]:
        """Find runs stuck in non-terminal states started before the given time."""
        rows = await self.pool.fetch("""
            SELECT * FROM runs
            WHERE status IN ('pending', 'extracting', 'staging', 'loading',
                             'quality_gate', 'promoting', 'retrying')
              AND started_at < $1
            ORDER BY started_at ASC
        """, stale_before)
        return [_row_to_run(r) for r in rows]

    async def get_last_successful_run(self, pipeline_id: str) -> Optional[RunRecord]:
        row = await self.pool.fetchrow("""
            SELECT * FROM runs
            WHERE pipeline_id = $1 AND status = 'complete'
            ORDER BY started_at DESC LIMIT 1
        """, pipeline_id)
        return _row_to_run(row) if row else None

    async def get_trigger_chain(self, run_id: str, max_depth: int = 10) -> list[RunRecord]:
        """Walk the trigger chain backwards: run → its trigger → root."""
        chain = []
        current_id = run_id
        for _ in range(max_depth):
            run = await self.get_run(current_id)
            if not run:
                break
            chain.append(run)
            if not run.triggered_by_run_id:
                break
            current_id = run.triggered_by_run_id
        return chain

    async def get_volume_baseline(self, pipeline_id: str, window: int = 30) -> list[int]:
        rows = await self.pool.fetch("""
            SELECT rows_extracted FROM runs
            WHERE pipeline_id = $1 AND status = 'complete'
            ORDER BY started_at DESC LIMIT $2
        """, pipeline_id, window)
        return [r["rows_extracted"] for r in rows]

    # ==================================================================
    # Gates
    # ==================================================================

    async def save_gate(self, g: GateRecord) -> None:
        checks_json = json.dumps([
            {"check_name": c.check_name, "status": c.status.value,
             "detail": c.detail, "metadata": c.metadata,
             "duration_ms": c.duration_ms}
            for c in g.checks
        ])
        await self.pool.execute("""
            INSERT INTO gates (
                gate_id, run_id, pipeline_id, decision,
                checks, agent_reasoning, evaluated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (gate_id) DO UPDATE SET
                decision=EXCLUDED.decision, checks=EXCLUDED.checks,
                agent_reasoning=EXCLUDED.agent_reasoning,
                evaluated_at=EXCLUDED.evaluated_at
        """,
            g.gate_id, g.run_id, g.pipeline_id, g.decision.value,
            checks_json, g.agent_reasoning, g.evaluated_at,
        )

    async def list_gates(self, pipeline_id: str) -> list[GateRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM gates WHERE pipeline_id = $1 ORDER BY evaluated_at DESC",
            pipeline_id,
        )
        return [_row_to_gate(r) for r in rows]

    # ==================================================================
    # Proposals
    # ==================================================================

    async def save_proposal(self, p: ContractChangeProposal) -> None:
        await self.pool.execute("""
            INSERT INTO proposals (
                proposal_id, pipeline_id, connector_id, created_at, resolved_at,
                status, trigger_type, trigger_detail, change_type,
                current_state, proposed_state, reasoning, confidence,
                impact_analysis, rollback_plan, resolved_by, resolution_note,
                rejection_learning, contract_version_before, contract_version_after
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            ON CONFLICT (proposal_id) DO UPDATE SET
                resolved_at=EXCLUDED.resolved_at, status=EXCLUDED.status,
                resolved_by=EXCLUDED.resolved_by,
                resolution_note=EXCLUDED.resolution_note,
                rejection_learning=EXCLUDED.rejection_learning,
                contract_version_after=EXCLUDED.contract_version_after
        """,
            p.proposal_id, p.pipeline_id, p.connector_id,
            p.created_at, p.resolved_at, p.status.value,
            p.trigger_type.value, json.dumps(p.trigger_detail),
            p.change_type.value,
            json.dumps(p.current_state), json.dumps(p.proposed_state),
            p.reasoning, p.confidence,
            json.dumps(p.impact_analysis), p.rollback_plan,
            p.resolved_by, p.resolution_note,
            json.dumps(p.rejection_learning) if p.rejection_learning else None,
            p.contract_version_before, p.contract_version_after,
        )

    async def get_proposal(self, proposal_id: str) -> Optional[ContractChangeProposal]:
        row = await self.pool.fetchrow(
            "SELECT * FROM proposals WHERE proposal_id = $1", proposal_id
        )
        return _row_to_proposal(row) if row else None

    async def list_proposals(self, status: Optional[str] = None) -> list[ContractChangeProposal]:
        sql = "SELECT * FROM proposals WHERE TRUE"
        params: list = []
        idx = 0
        if status:
            idx += 1
            sql += f" AND status = ${idx}"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_proposal(r) for r in rows]

    async def has_pending_halt_proposal(self, pipeline_id: str) -> bool:
        row = await self.pool.fetchrow("""
            SELECT 1 FROM proposals
            WHERE pipeline_id = $1 AND status = 'pending'
            AND trigger_type = 'quality_alert'
            LIMIT 1
        """, pipeline_id)
        return row is not None

    # ==================================================================
    # Schema versions
    # ==================================================================

    async def save_schema_version(self, sv: SchemaVersion) -> None:
        await self.pool.execute("""
            INSERT INTO schema_versions (
                version_id, pipeline_id, version, column_mappings,
                change_summary, change_type, proposal_id, applied_at, applied_by
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (version_id) DO UPDATE SET
                column_mappings=EXCLUDED.column_mappings,
                change_summary=EXCLUDED.change_summary,
                change_type=EXCLUDED.change_type,
                proposal_id=EXCLUDED.proposal_id,
                applied_at=EXCLUDED.applied_at,
                applied_by=EXCLUDED.applied_by
        """,
            sv.version_id, sv.pipeline_id, sv.version,
            json.dumps([asdict(m) for m in sv.column_mappings]),
            sv.change_summary, sv.change_type,
            sv.proposal_id, sv.applied_at, sv.applied_by,
        )

    async def list_schema_versions(self, pipeline_id: str) -> list[SchemaVersion]:
        rows = await self.pool.fetch(
            "SELECT * FROM schema_versions WHERE pipeline_id = $1 ORDER BY version DESC",
            pipeline_id,
        )
        return [_row_to_schema_version(r) for r in rows]

    # ==================================================================
    # Dependencies
    # ==================================================================

    async def save_dependency(self, d: PipelineDependency) -> None:
        await self.pool.execute("""
            INSERT INTO dependencies (
                dependency_id, pipeline_id, depends_on_id,
                dependency_type, created_at, notes
            ) VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (dependency_id) DO UPDATE SET
                pipeline_id=EXCLUDED.pipeline_id,
                depends_on_id=EXCLUDED.depends_on_id,
                dependency_type=EXCLUDED.dependency_type,
                notes=EXCLUDED.notes
        """,
            d.dependency_id, d.pipeline_id, d.depends_on_id,
            d.dependency_type.value, d.created_at, d.notes,
        )

    async def list_dependencies(self, pipeline_id: str) -> list[PipelineDependency]:
        rows = await self.pool.fetch(
            "SELECT * FROM dependencies WHERE pipeline_id = $1", pipeline_id
        )
        return [_row_to_dependency(r) for r in rows]

    async def delete_dependency(self, dependency_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM dependencies WHERE dependency_id = $1", dependency_id
        )

    async def list_dependents(self, depends_on_id: str) -> list[PipelineDependency]:
        """Find all pipelines that depend on the given pipeline (reverse lookup)."""
        rows = await self.pool.fetch(
            "SELECT * FROM dependencies WHERE depends_on_id = $1", depends_on_id
        )
        return [_row_to_dependency(r) for r in rows]

    # ==================================================================
    # Data contracts (Build 16)
    # ==================================================================

    async def save_data_contract(self, c: DataContract) -> None:
        c.updated_at = now_iso()
        await self.pool.execute("""
            INSERT INTO data_contracts (
                contract_id, producer_pipeline_id, consumer_pipeline_id,
                description, status, required_columns,
                freshness_sla_minutes, retention_hours, cleanup_ownership,
                last_validated_at, last_violation_at, violation_count,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (contract_id) DO UPDATE SET
                description=EXCLUDED.description,
                status=EXCLUDED.status,
                required_columns=EXCLUDED.required_columns,
                freshness_sla_minutes=EXCLUDED.freshness_sla_minutes,
                retention_hours=EXCLUDED.retention_hours,
                cleanup_ownership=EXCLUDED.cleanup_ownership,
                last_validated_at=EXCLUDED.last_validated_at,
                last_violation_at=EXCLUDED.last_violation_at,
                violation_count=EXCLUDED.violation_count,
                updated_at=EXCLUDED.updated_at
        """,
            c.contract_id, c.producer_pipeline_id, c.consumer_pipeline_id,
            c.description,
            c.status.value if hasattr(c.status, "value") else c.status,
            json.dumps(c.required_columns),
            c.freshness_sla_minutes, c.retention_hours,
            c.cleanup_ownership.value if hasattr(c.cleanup_ownership, "value") else c.cleanup_ownership,
            c.last_validated_at, c.last_violation_at, c.violation_count,
            c.created_at, c.updated_at,
        )

    async def get_data_contract(self, contract_id: str) -> Optional[DataContract]:
        row = await self.pool.fetchrow(
            "SELECT * FROM data_contracts WHERE contract_id = $1", contract_id
        )
        return _row_to_data_contract(row) if row else None

    async def list_data_contracts(
        self,
        producer_id: Optional[str] = None,
        consumer_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[DataContract]:
        sql = "SELECT * FROM data_contracts WHERE TRUE"
        params: list = []
        idx = 0
        if producer_id:
            idx += 1
            sql += f" AND producer_pipeline_id = ${idx}"
            params.append(producer_id)
        if consumer_id:
            idx += 1
            sql += f" AND consumer_pipeline_id = ${idx}"
            params.append(consumer_id)
        if status:
            idx += 1
            sql += f" AND status = ${idx}"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_data_contract(r) for r in rows]

    async def delete_data_contract(self, contract_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM contract_violations WHERE contract_id = $1", contract_id
        )
        await self.pool.execute(
            "DELETE FROM data_contracts WHERE contract_id = $1", contract_id
        )

    async def save_contract_violation(self, v: ContractViolation) -> None:
        await self.pool.execute("""
            INSERT INTO contract_violations (
                violation_id, contract_id, violation_type, detail,
                producer_pipeline_id, consumer_pipeline_id,
                resolved, resolved_at, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (violation_id) DO UPDATE SET
                resolved=EXCLUDED.resolved,
                resolved_at=EXCLUDED.resolved_at
        """,
            v.violation_id, v.contract_id,
            v.violation_type.value if hasattr(v.violation_type, "value") else v.violation_type,
            v.detail, v.producer_pipeline_id, v.consumer_pipeline_id,
            v.resolved, v.resolved_at, v.created_at,
        )

    async def list_contract_violations(
        self,
        contract_id: str,
        resolved: Optional[bool] = None,
    ) -> list[ContractViolation]:
        sql = "SELECT * FROM contract_violations WHERE contract_id = $1"
        params: list = [contract_id]
        if resolved is not None:
            sql += " AND resolved = $2"
            params.append(resolved)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_contract_violation(r) for r in rows]

    async def resolve_contract_violation(self, violation_id: str) -> None:
        await self.pool.execute("""
            UPDATE contract_violations SET resolved = TRUE, resolved_at = $2
            WHERE violation_id = $1
        """, violation_id, now_iso())

    # ==================================================================
    # Pipeline metadata (XCom-style key-value store)
    # ==================================================================

    async def set_metadata(
        self, pipeline_id: str, key: str, value: dict,
        run_id: str = None, namespace: str = "default",
    ) -> None:
        """Upsert a metadata key for a pipeline."""
        await self.pool.execute("""
            INSERT INTO pipeline_metadata (id, pipeline_id, namespace, key, value_json, updated_at, created_by_run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (pipeline_id, namespace, key) DO UPDATE SET
                value_json = EXCLUDED.value_json,
                updated_at = EXCLUDED.updated_at,
                created_by_run_id = EXCLUDED.created_by_run_id
        """, new_id(), pipeline_id, namespace, key, json.dumps(value), now_iso(), run_id)

    async def get_metadata(
        self, pipeline_id: str, key: str, namespace: str = "default",
    ) -> PipelineMetadata:
        row = await self.pool.fetchrow("""
            SELECT * FROM pipeline_metadata
            WHERE pipeline_id = $1 AND namespace = $2 AND key = $3
        """, pipeline_id, namespace, key)
        return _row_to_metadata(row) if row else None

    async def list_metadata(
        self, pipeline_id: str, namespace: str = None,
    ) -> list[PipelineMetadata]:
        if namespace:
            rows = await self.pool.fetch("""
                SELECT * FROM pipeline_metadata
                WHERE pipeline_id = $1 AND namespace = $2 ORDER BY key
            """, pipeline_id, namespace)
        else:
            rows = await self.pool.fetch("""
                SELECT * FROM pipeline_metadata
                WHERE pipeline_id = $1 ORDER BY namespace, key
            """, pipeline_id)
        return [_row_to_metadata(r) for r in rows]

    async def delete_metadata(
        self, pipeline_id: str, key: str, namespace: str = "default",
    ) -> None:
        await self.pool.execute("""
            DELETE FROM pipeline_metadata
            WHERE pipeline_id = $1 AND namespace = $2 AND key = $3
        """, pipeline_id, namespace, key)

    # ==================================================================
    # Notification policies
    # ==================================================================

    async def save_policy(self, p: NotificationPolicy) -> None:
        p.updated_at = now_iso()
        await self.pool.execute("""
            INSERT INTO notification_policies (
                policy_id, policy_name, description, channels,
                digest_hour, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (policy_id) DO UPDATE SET
                policy_name=EXCLUDED.policy_name,
                description=EXCLUDED.description,
                channels=EXCLUDED.channels,
                digest_hour=EXCLUDED.digest_hour,
                updated_at=EXCLUDED.updated_at
        """,
            p.policy_id, p.policy_name, p.description,
            json.dumps(p.channels), p.digest_hour,
            p.created_at, p.updated_at,
        )

    async def get_policy(self, policy_id: str) -> Optional[NotificationPolicy]:
        row = await self.pool.fetchrow(
            "SELECT * FROM notification_policies WHERE policy_id = $1", policy_id
        )
        if not row:
            return None
        return NotificationPolicy(
            policy_id=row["policy_id"], policy_name=row["policy_name"],
            description=row["description"],
            channels=json.loads(row["channels"]),
            digest_hour=row["digest_hour"],
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    async def list_policies(self, pipeline_id: Optional[str] = None) -> list[NotificationPolicy]:
        if pipeline_id:
            rows = await self.pool.fetch("""
                SELECT np.* FROM notification_policies np
                JOIN pipelines p ON p.notification_policy_id = np.policy_id
                WHERE p.pipeline_id = $1
                ORDER BY np.policy_name
            """, pipeline_id)
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM notification_policies ORDER BY policy_name"
            )
        return [NotificationPolicy(
            policy_id=r["policy_id"], policy_name=r["policy_name"],
            description=r["description"],
            channels=json.loads(r["channels"]),
            digest_hour=r["digest_hour"],
            created_at=r["created_at"], updated_at=r["updated_at"],
        ) for r in rows]

    async def delete_policy(self, policy_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM notification_policies WHERE policy_id = $1", policy_id
        )

    # ==================================================================
    # Freshness snapshots
    # ==================================================================

    async def save_freshness(self, s: FreshnessSnapshot) -> None:
        await self.pool.execute("""
            INSERT INTO freshness_snapshots (
                snapshot_id, pipeline_id, pipeline_name, tier,
                staleness_minutes, freshness_sla_minutes, sla_met,
                status, last_record_time, checked_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                staleness_minutes=EXCLUDED.staleness_minutes,
                sla_met=EXCLUDED.sla_met,
                status=EXCLUDED.status,
                last_record_time=EXCLUDED.last_record_time,
                checked_at=EXCLUDED.checked_at
        """,
            s.snapshot_id, s.pipeline_id, s.pipeline_name, s.tier,
            s.staleness_minutes, s.freshness_sla_minutes, s.sla_met,
            s.status.value, s.last_record_time, s.checked_at,
        )

    async def get_latest_freshness(self, pipeline_id: str) -> Optional[FreshnessSnapshot]:
        row = await self.pool.fetchrow("""
            SELECT * FROM freshness_snapshots WHERE pipeline_id = $1
            ORDER BY checked_at DESC LIMIT 1
        """, pipeline_id)
        return _row_to_freshness(row) if row else None

    async def list_freshness_history(
        self, pipeline_id: str, hours: int = 24,
    ) -> list[FreshnessSnapshot]:
        rows = await self.pool.fetch("""
            SELECT * FROM freshness_snapshots
            WHERE pipeline_id = $1
              AND checked_at::timestamptz >= NOW() - make_interval(hours => $2)
            ORDER BY checked_at ASC
        """, pipeline_id, hours)
        return [_row_to_freshness(r) for r in rows]

    # ==================================================================
    # Alerts
    # ==================================================================

    async def save_alert(self, a: AlertRecord) -> None:
        await self.pool.execute("""
            INSERT INTO alerts (
                alert_id, severity, tier, pipeline_id, pipeline_name,
                summary, detail, narrative, created_at, acknowledged,
                acknowledged_by, acknowledged_at, digested
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (alert_id) DO UPDATE SET
                acknowledged=EXCLUDED.acknowledged,
                acknowledged_by=EXCLUDED.acknowledged_by,
                acknowledged_at=EXCLUDED.acknowledged_at,
                digested=EXCLUDED.digested,
                narrative=EXCLUDED.narrative
        """,
            a.alert_id, a.severity.value, a.tier,
            a.pipeline_id, a.pipeline_name, a.summary,
            json.dumps(a.detail), a.narrative, a.created_at,
            a.acknowledged, a.acknowledged_by, a.acknowledged_at,
            a.digested,
        )

    async def list_alerts(
        self, severity: Optional[str] = None, hours: int = 48
    ) -> list[AlertRecord]:
        sql = """
            SELECT * FROM alerts
            WHERE created_at >= (NOW() - make_interval(hours => $1))::text
        """
        params: list = [hours]
        idx = 1
        if severity:
            idx += 1
            sql += f" AND severity = ${idx}"
            params.append(severity)
        sql += " ORDER BY created_at DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_alert(r) for r in rows]

    async def get_undigested_alerts(self) -> list[AlertRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM alerts WHERE digested = FALSE ORDER BY created_at DESC"
        )
        return [_row_to_alert(r) for r in rows]

    async def list_alerts_for_pipeline(
        self, pipeline_id: str, limit: int = 100,
    ) -> list[AlertRecord]:
        rows = await self.pool.fetch(
            "SELECT * FROM alerts WHERE pipeline_id = $1 "
            "ORDER BY created_at DESC LIMIT $2",
            pipeline_id, limit,
        )
        return [_row_to_alert(r) for r in rows]

    # ==================================================================
    # Decision log
    # ==================================================================

    async def save_decision(self, d: DecisionLog) -> None:
        result = await self.pool.fetchrow("""
            INSERT INTO decision_logs (
                pipeline_id, connector_id, decision_type,
                detail, reasoning, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6)
            RETURNING id
        """,
            d.pipeline_id, d.connector_id, d.decision_type,
            d.detail, d.reasoning, d.created_at,
        )
        d.id = result["id"]

    async def list_decisions(self, pipeline_id: str) -> list[DecisionLog]:
        rows = await self.pool.fetch("""
            SELECT * FROM decision_logs WHERE pipeline_id = $1
            ORDER BY created_at DESC
        """, pipeline_id)
        return [DecisionLog(
            id=r["id"], pipeline_id=r["pipeline_id"],
            connector_id=r["connector_id"],
            decision_type=r["decision_type"], detail=r["detail"],
            reasoning=r["reasoning"], created_at=r["created_at"],
        ) for r in rows]

    # ==================================================================
    # Preferences (with pgvector)
    # ==================================================================

    async def save_preference(self, p: AgentPreference) -> None:
        p.updated_at = now_iso()
        embedding_val = str(p.embedding) if p.embedding else None
        await self.pool.execute("""
            INSERT INTO preferences (
                preference_id, scope, scope_value, preference_key,
                preference_value, source, confidence,
                created_at, updated_at, usage_count,
                embedding, last_used
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::vector,$12)
            ON CONFLICT (preference_id) DO UPDATE SET
                preference_value=EXCLUDED.preference_value,
                confidence=EXCLUDED.confidence,
                updated_at=EXCLUDED.updated_at,
                usage_count=EXCLUDED.usage_count,
                embedding=EXCLUDED.embedding,
                last_used=EXCLUDED.last_used
        """,
            p.preference_id, p.scope.value, p.scope_value,
            p.preference_key, json.dumps(p.preference_value),
            p.source.value, p.confidence,
            p.created_at, p.updated_at, p.usage_count,
            embedding_val, p.last_used,
        )

    async def get_preferences(self, scope: Optional[str] = None, scope_value: Optional[str] = None) -> list[AgentPreference]:
        sql = "SELECT * FROM preferences WHERE TRUE"
        params: list = []
        idx = 0
        if scope:
            idx += 1
            sql += f" AND scope = ${idx}"
            params.append(scope)
        if scope_value:
            idx += 1
            sql += f" AND scope_value = ${idx}"
            params.append(scope_value)
        sql += " ORDER BY confidence DESC, usage_count DESC"
        rows = await self.pool.fetch(sql, *params)
        return [_row_to_preference(r) for r in rows]

    async def search_preferences(
        self, query_embedding: list, limit: int = 5
    ) -> list[AgentPreference]:
        rows = await self.pool.fetch("""
            SELECT *, embedding <=> $1::vector AS distance
            FROM preferences
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $1::vector
            LIMIT $2
        """, str(query_embedding), limit)
        return [_row_to_preference(r) for r in rows]

    async def delete_preference(self, preference_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM preferences WHERE preference_id = $1", preference_id
        )

    # ==================================================================
    # Error budgets
    # ==================================================================

    async def get_error_budget(self, pipeline_id: str) -> Optional[ErrorBudget]:
        row = await self.pool.fetchrow(
            "SELECT * FROM error_budgets WHERE pipeline_id = $1", pipeline_id
        )
        if not row:
            return None
        return ErrorBudget(
            pipeline_id=row["pipeline_id"],
            window_days=row["window_days"],
            total_runs=row["total_runs"],
            successful_runs=row["successful_runs"],
            failed_runs=row["failed_runs"],
            success_rate=row["success_rate"],
            budget_threshold=row["budget_threshold"],
            budget_remaining=row["budget_remaining"],
            escalated=row["escalated"],
            last_calculated=row["last_calculated"],
        )

    async def save_error_budget(self, b: ErrorBudget) -> None:
        await self.pool.execute("""
            INSERT INTO error_budgets (
                pipeline_id, window_days, total_runs, successful_runs,
                failed_runs, success_rate, budget_threshold,
                budget_remaining, escalated, last_calculated
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (pipeline_id) DO UPDATE SET
                window_days=EXCLUDED.window_days,
                total_runs=EXCLUDED.total_runs,
                successful_runs=EXCLUDED.successful_runs,
                failed_runs=EXCLUDED.failed_runs,
                success_rate=EXCLUDED.success_rate,
                budget_threshold=EXCLUDED.budget_threshold,
                budget_remaining=EXCLUDED.budget_remaining,
                escalated=EXCLUDED.escalated,
                last_calculated=EXCLUDED.last_calculated
        """,
            b.pipeline_id, b.window_days, b.total_runs,
            b.successful_runs, b.failed_runs, b.success_rate,
            b.budget_threshold, b.budget_remaining,
            b.escalated, b.last_calculated,
        )

    # ==================================================================
    # Column lineage
    # ==================================================================

    async def save_column_lineage(self, cl: ColumnLineage) -> None:
        await self.pool.execute("""
            INSERT INTO column_lineage (
                id, source_pipeline_id, source_schema, source_table,
                source_column, target_pipeline_id, target_schema,
                target_table, target_column, transformation, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO UPDATE SET
                source_pipeline_id=EXCLUDED.source_pipeline_id,
                source_schema=EXCLUDED.source_schema,
                source_table=EXCLUDED.source_table,
                source_column=EXCLUDED.source_column,
                target_pipeline_id=EXCLUDED.target_pipeline_id,
                target_schema=EXCLUDED.target_schema,
                target_table=EXCLUDED.target_table,
                target_column=EXCLUDED.target_column,
                transformation=EXCLUDED.transformation
        """,
            cl.id, cl.source_pipeline_id, cl.source_schema,
            cl.source_table, cl.source_column,
            cl.target_pipeline_id, cl.target_schema,
            cl.target_table, cl.target_column,
            cl.transformation, cl.created_at,
        )

    async def list_column_lineage(self, pipeline_id: str) -> list[ColumnLineage]:
        rows = await self.pool.fetch("""
            SELECT * FROM column_lineage
            WHERE source_pipeline_id = $1 OR target_pipeline_id = $1
            ORDER BY created_at DESC
        """, pipeline_id)
        return [_row_to_column_lineage(r) for r in rows]

    async def delete_column_lineage(self, pipeline_id: str) -> None:
        await self.pool.execute("""
            DELETE FROM column_lineage
            WHERE source_pipeline_id = $1 OR target_pipeline_id = $1
        """, pipeline_id)

    async def get_downstream_columns(
        self, source_pipeline_id: str, source_column: str
    ) -> list[ColumnLineage]:
        rows = await self.pool.fetch("""
            SELECT * FROM column_lineage
            WHERE source_pipeline_id = $1 AND source_column = $2
            ORDER BY target_pipeline_id, target_column
        """, source_pipeline_id, source_column)
        return [_row_to_column_lineage(r) for r in rows]

    # ==================================================================
    # Agent cost logs
    # ==================================================================

    async def save_agent_cost(self, entry: AgentCostLog) -> None:
        await self.pool.execute("""
            INSERT INTO agent_cost_logs (
                id, pipeline_id, operation, model,
                input_tokens, output_tokens, total_tokens,
                latency_ms, timestamp
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                pipeline_id=EXCLUDED.pipeline_id,
                operation=EXCLUDED.operation,
                model=EXCLUDED.model,
                input_tokens=EXCLUDED.input_tokens,
                output_tokens=EXCLUDED.output_tokens,
                total_tokens=EXCLUDED.total_tokens,
                latency_ms=EXCLUDED.latency_ms
        """,
            entry.id, entry.pipeline_id, entry.operation, entry.model,
            entry.input_tokens, entry.output_tokens, entry.total_tokens,
            entry.latency_ms, entry.timestamp,
        )

    async def list_agent_costs(
        self, pipeline_id: str, hours: int = 24
    ) -> list[AgentCostLog]:
        rows = await self.pool.fetch("""
            SELECT * FROM agent_cost_logs
            WHERE pipeline_id = $1
            AND timestamp >= (NOW() - make_interval(hours => $2))::text
            ORDER BY timestamp DESC
        """, pipeline_id, hours)
        return [AgentCostLog(
            id=r["id"], pipeline_id=r["pipeline_id"],
            operation=r["operation"], model=r["model"],
            input_tokens=r["input_tokens"], output_tokens=r["output_tokens"],
            total_tokens=r["total_tokens"], latency_ms=r["latency_ms"],
            timestamp=r["timestamp"],
        ) for r in rows]

    async def get_total_cost_summary(self, hours: int = 24) -> dict:
        row = await self.pool.fetchrow("""
            SELECT
                COUNT(*) AS call_count,
                COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
                COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
                COALESCE(SUM(total_tokens), 0) AS total_tokens,
                COALESCE(AVG(latency_ms), 0) AS avg_latency_ms,
                COUNT(DISTINCT pipeline_id) AS pipelines_served,
                COUNT(DISTINCT model) AS models_used
            FROM agent_cost_logs
            WHERE timestamp >= (NOW() - make_interval(hours => $1))::text
        """, hours)
        return dict(row) if row else {
            "call_count": 0, "total_input_tokens": 0,
            "total_output_tokens": 0, "total_tokens": 0,
            "avg_latency_ms": 0, "pipelines_served": 0, "models_used": 0,
        }

    # ==================================================================
    # Connector migrations
    # ==================================================================

    async def save_connector_migration(self, m: ConnectorMigration) -> None:
        await self.pool.execute("""
            INSERT INTO connector_migrations (
                id, connector_id, from_version, to_version,
                affected_pipelines, migration_status, migration_log,
                created_at, completed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                migration_status=EXCLUDED.migration_status,
                migration_log=EXCLUDED.migration_log,
                completed_at=EXCLUDED.completed_at
        """,
            m.id, m.connector_id, m.from_version, m.to_version,
            json.dumps(m.affected_pipelines), m.migration_status,
            m.migration_log, m.created_at, m.completed_at,
        )

    async def list_connector_migrations(
        self, connector_id: str
    ) -> list[ConnectorMigration]:
        rows = await self.pool.fetch(
            "SELECT * FROM connector_migrations WHERE connector_id = $1 ORDER BY created_at DESC",
            connector_id,
        )
        return [ConnectorMigration(
            id=r["id"], connector_id=r["connector_id"],
            from_version=r["from_version"], to_version=r["to_version"],
            affected_pipelines=json.loads(r["affected_pipelines"]),
            migration_status=r["migration_status"],
            migration_log=r["migration_log"],
            created_at=r["created_at"], completed_at=r["completed_at"],
        ) for r in rows]

    # ==================================================================
    # Users
    # ==================================================================

    async def get_user(self, user_id: str) -> Optional[User]:
        row = await self.pool.fetchrow(
            "SELECT * FROM users WHERE id = $1", user_id
        )
        return _row_to_user(row) if row else None

    async def get_user_by_username(self, username: str) -> Optional[User]:
        row = await self.pool.fetchrow(
            "SELECT * FROM users WHERE username = $1", username
        )
        return _row_to_user(row) if row else None

    async def save_user(self, u: User) -> None:
        await self.pool.execute("""
            INSERT INTO users (
                id, username, password_hash, role, email, created_at, last_login
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (id) DO UPDATE SET
                username=EXCLUDED.username,
                password_hash=EXCLUDED.password_hash,
                role=EXCLUDED.role,
                email=EXCLUDED.email,
                last_login=EXCLUDED.last_login
        """,
            u.id, u.username, u.password_hash,
            u.role, u.email, u.created_at, u.last_login,
        )


    # ------------------------------------------------------------------
    # Chat Interactions (audit + training log)
    # ------------------------------------------------------------------

    async def save_chat_interaction(self, ci: ChatInteraction) -> None:
        await self.pool.execute("""
            INSERT INTO chat_interactions (
                interaction_id, session_id, user_id, username,
                user_input, routed_action, action_params,
                agent_response, result_data,
                input_tokens, output_tokens, latency_ms,
                model, error, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        """,
            ci.interaction_id, ci.session_id, ci.user_id, ci.username,
            ci.user_input, ci.routed_action, json.dumps(ci.action_params),
            ci.agent_response, json.dumps(ci.result_data),
            ci.input_tokens, ci.output_tokens, ci.latency_ms,
            ci.model, ci.error, ci.created_at,
        )

    async def list_chat_interactions(
        self,
        session_id: Optional[str] = None,
        username: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ChatInteraction]:
        conditions = []
        args = []
        idx = 1
        if session_id:
            conditions.append(f"session_id = ${idx}")
            args.append(session_id)
            idx += 1
        if username:
            conditions.append(f"username = ${idx}")
            args.append(username)
            idx += 1
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        args.extend([limit, offset])
        rows = await self.pool.fetch(f"""
            SELECT * FROM chat_interactions
            {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
        """, *args)
        return [_row_to_chat_interaction(r) for r in rows]

    async def count_chat_interactions(
        self,
        session_id: Optional[str] = None,
        username: Optional[str] = None,
    ) -> int:
        conditions = []
        args = []
        idx = 1
        if session_id:
            conditions.append(f"session_id = ${idx}")
            args.append(session_id)
            idx += 1
        if username:
            conditions.append(f"username = ${idx}")
            args.append(username)
            idx += 1
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        row = await self.pool.fetchrow(f"""
            SELECT COUNT(*) as cnt FROM chat_interactions {where}
        """, *args)
        return row["cnt"] if row else 0

    # ------------------------------------------------------------------
    # Pipeline Change Log
    # ------------------------------------------------------------------

    async def save_pipeline_change(self, cl: PipelineChangeLog) -> None:
        await self.pool.execute("""
            INSERT INTO pipeline_changelog (
                change_id, pipeline_id, pipeline_name, change_type,
                changed_by, changed_by_id, source,
                changed_fields, reason, context, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        """,
            cl.change_id, cl.pipeline_id, cl.pipeline_name,
            cl.change_type.value if hasattr(cl.change_type, "value") else cl.change_type,
            cl.changed_by, cl.changed_by_id, cl.source,
            json.dumps(cl.changed_fields, default=str), cl.reason, cl.context,
            cl.created_at,
        )

    async def list_pipeline_changes(
        self,
        pipeline_id: str,
        change_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[PipelineChangeLog]:
        if change_type:
            rows = await self.pool.fetch("""
                SELECT * FROM pipeline_changelog
                WHERE pipeline_id = $1 AND change_type = $2
                ORDER BY created_at DESC LIMIT $3 OFFSET $4
            """, pipeline_id, change_type, limit, offset)
        else:
            rows = await self.pool.fetch("""
                SELECT * FROM pipeline_changelog
                WHERE pipeline_id = $1
                ORDER BY created_at DESC LIMIT $2 OFFSET $3
            """, pipeline_id, limit, offset)
        return [_row_to_pipeline_change(r) for r in rows]

    async def list_all_pipeline_changes(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> list[PipelineChangeLog]:
        rows = await self.pool.fetch("""
            SELECT * FROM pipeline_changelog
            ORDER BY created_at DESC LIMIT $1 OFFSET $2
        """, limit, offset)
        return [_row_to_pipeline_change(r) for r in rows]

    # ------------------------------------------------------------------
    # Registered Sources
    # ------------------------------------------------------------------

    async def save_registered_source(self, s: RegisteredSource) -> None:
        await self.pool.execute("""
            INSERT INTO registered_sources (
                source_id, display_name, connector_id, connector_name, source_type,
                connection_params, description, owner, tags,
                schema_cache, schema_cache_updated_at,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (source_id) DO UPDATE SET
                display_name=EXCLUDED.display_name,
                connector_id=EXCLUDED.connector_id,
                connector_name=EXCLUDED.connector_name,
                source_type=EXCLUDED.source_type,
                connection_params=EXCLUDED.connection_params,
                description=EXCLUDED.description,
                owner=EXCLUDED.owner,
                tags=EXCLUDED.tags,
                schema_cache=EXCLUDED.schema_cache,
                schema_cache_updated_at=EXCLUDED.schema_cache_updated_at,
                updated_at=EXCLUDED.updated_at
        """,
            s.source_id, s.display_name, s.connector_id, s.connector_name,
            s.source_type, json.dumps(s.connection_params),
            s.description, s.owner, json.dumps(s.tags),
            json.dumps(s.schema_cache), s.schema_cache_updated_at,
            s.created_at, s.updated_at,
        )

    async def get_registered_source(self, source_id: str) -> Optional[RegisteredSource]:
        row = await self.pool.fetchrow(
            "SELECT * FROM registered_sources WHERE source_id = $1", source_id
        )
        return _row_to_registered_source(row) if row else None

    async def get_registered_source_by_name(self, name: str) -> Optional[RegisteredSource]:
        row = await self.pool.fetchrow(
            "SELECT * FROM registered_sources WHERE LOWER(display_name) = LOWER($1)", name
        )
        return _row_to_registered_source(row) if row else None

    async def list_registered_sources(self, source_type: Optional[str] = None) -> list[RegisteredSource]:
        if source_type:
            rows = await self.pool.fetch(
                "SELECT * FROM registered_sources WHERE source_type = $1 ORDER BY display_name",
                source_type.lower(),
            )
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM registered_sources ORDER BY display_name"
            )
        return [_row_to_registered_source(r) for r in rows]

    async def delete_registered_source(self, source_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM registered_sources WHERE source_id = $1", source_id
        )

    async def update_source_schema_cache(
        self, source_id: str, cache: dict
    ) -> None:
        await self.pool.execute("""
            UPDATE registered_sources
            SET schema_cache = $2, schema_cache_updated_at = $3, updated_at = $3
            WHERE source_id = $1
        """, source_id, json.dumps(cache), now_iso())

    # ------------------------------------------------------------------
    # Step Executions (Build 18)
    # ------------------------------------------------------------------

    async def save_step_execution(self, se) -> None:
        """Upsert a step execution record."""
        from contracts.models import StepExecution
        await self.pool.execute("""
            INSERT INTO step_executions (
                step_execution_id, run_id, pipeline_id, step_id, step_name,
                step_type, status, started_at, completed_at, output,
                error, retry_count, elapsed_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (step_execution_id) DO UPDATE SET
                status=EXCLUDED.status,
                started_at=EXCLUDED.started_at,
                completed_at=EXCLUDED.completed_at,
                output=EXCLUDED.output,
                error=EXCLUDED.error,
                retry_count=EXCLUDED.retry_count,
                elapsed_ms=EXCLUDED.elapsed_ms
        """,
            se.step_execution_id, se.run_id, se.pipeline_id, se.step_id,
            se.step_name, se.step_type, se.status.value if hasattr(se.status, "value") else se.status,
            se.started_at, se.completed_at,
            json.dumps(se.output), se.error, se.retry_count, se.elapsed_ms,
        )

    async def list_step_executions(self, run_id: str) -> list:
        """List all step executions for a run."""
        from contracts.models import StepExecution, StepStatus
        rows = await self.pool.fetch(
            "SELECT * FROM step_executions WHERE run_id = $1 ORDER BY started_at ASC NULLS LAST",
            run_id,
        )
        results = []
        for row in rows:
            results.append(StepExecution(
                step_execution_id=row["step_execution_id"],
                run_id=row["run_id"],
                pipeline_id=row["pipeline_id"],
                step_id=row["step_id"],
                step_name=row["step_name"],
                step_type=row["step_type"],
                status=StepStatus(row["status"]),
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                output=json.loads(row["output"]) if row["output"] else {},
                error=row["error"],
                retry_count=row["retry_count"],
                elapsed_ms=row["elapsed_ms"],
            ))
        return results

    # ==================================================================
    # Diagnostic queries (Build 24)
    # ==================================================================

    async def list_recent_failures(self, hours: int = 48) -> list[RunRecord]:
        """All failed/halted runs across all pipelines in the given window."""
        rows = await self.pool.fetch("""
            SELECT * FROM runs
            WHERE status IN ('failed', 'halted')
              AND started_at >= (NOW() - make_interval(hours => $1))::text
            ORDER BY started_at DESC
        """, hours)
        return [_row_to_run(r) for r in rows]

    async def get_quality_trend(self, pipeline_id: str, limit: int = 20) -> list[GateRecord]:
        """Recent quality gate evaluations for a pipeline."""
        rows = await self.pool.fetch(
            "SELECT * FROM gates WHERE pipeline_id = $1 ORDER BY evaluated_at DESC LIMIT $2",
            pipeline_id, limit,
        )
        return [_row_to_gate(r) for r in rows]

    async def get_volume_history(self, pipeline_id: str, limit: int = 20) -> list[dict]:
        """Recent completed run row counts for volume trend analysis."""
        rows = await self.pool.fetch("""
            SELECT run_id, started_at, completed_at, status, rows_extracted, rows_loaded
            FROM runs WHERE pipeline_id = $1 AND status = 'complete'
            ORDER BY started_at DESC LIMIT $2
        """, pipeline_id, limit)
        return [dict(r) for r in rows]

    async def get_all_downstream_recursive(
        self, pipeline_id: str, max_depth: int = 10,
    ) -> list[dict]:
        """Walk the dependency graph to find all transitive downstream pipelines."""
        visited: set[str] = set()
        result: list[dict] = []
        queue: list[tuple[str, int]] = [(pipeline_id, 0)]
        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)
            dependents = await self.list_dependents(current_id)
            for dep in dependents:
                if dep.pipeline_id not in visited:
                    p = await self.get_pipeline(dep.pipeline_id)
                    if p:
                        result.append({
                            "pipeline_id": p.pipeline_id,
                            "pipeline_name": p.pipeline_name,
                            "status": p.status.value if hasattr(p.status, "value") else p.status,
                            "schedule_cron": p.schedule_cron,
                            "tier": p.tier,
                            "depth": depth + 1,
                        })
                    queue.append((dep.pipeline_id, depth + 1))
        return result

    # ==================================================================
    # SQL Transforms (Build 29)
    # ==================================================================

    async def save_sql_transform(self, t) -> None:
        from contracts.models import SqlTransform
        await self.pool.execute("""
            INSERT INTO sql_transforms (
                transform_id, transform_name, description, sql, materialization,
                target_schema, target_table, variables, refs, column_lineage,
                version, created_by, approved, pipeline_id, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            ON CONFLICT (transform_id) DO UPDATE SET
                transform_name=EXCLUDED.transform_name,
                description=EXCLUDED.description,
                sql=EXCLUDED.sql,
                materialization=EXCLUDED.materialization,
                target_schema=EXCLUDED.target_schema,
                target_table=EXCLUDED.target_table,
                variables=EXCLUDED.variables,
                refs=EXCLUDED.refs,
                column_lineage=EXCLUDED.column_lineage,
                version=EXCLUDED.version,
                approved=EXCLUDED.approved,
                pipeline_id=EXCLUDED.pipeline_id,
                updated_at=EXCLUDED.updated_at
        """,
            t.transform_id, t.transform_name, t.description, t.sql,
            t.materialization.value if hasattr(t.materialization, 'value') else t.materialization,
            t.target_schema, t.target_table or t.transform_name,
            json.dumps(t.variables), json.dumps(t.refs), json.dumps(t.column_lineage),
            t.version, t.created_by, t.approved, t.pipeline_id,
            t.created_at, t.updated_at,
        )

    async def get_sql_transform(self, transform_id: str):
        row = await self.pool.fetchrow(
            "SELECT * FROM sql_transforms WHERE transform_id = $1", transform_id
        )
        return _row_to_sql_transform(row) if row else None

    async def get_sql_transform_by_name(self, name: str):
        row = await self.pool.fetchrow(
            "SELECT * FROM sql_transforms WHERE transform_name = $1 ORDER BY version DESC LIMIT 1", name
        )
        return _row_to_sql_transform(row) if row else None

    async def list_sql_transforms(self, pipeline_id: str = "") -> list:
        if pipeline_id:
            rows = await self.pool.fetch(
                "SELECT * FROM sql_transforms WHERE pipeline_id = $1 ORDER BY transform_name", pipeline_id
            )
        else:
            rows = await self.pool.fetch("SELECT * FROM sql_transforms ORDER BY transform_name")
        return [_row_to_sql_transform(r) for r in rows]

    async def delete_sql_transform(self, transform_id: str) -> None:
        await self.pool.execute("DELETE FROM sql_transforms WHERE transform_id = $1", transform_id)

    # ==================================================================
    # Metrics (Build 31)
    # ==================================================================

    async def save_metric(self, m: MetricDefinition) -> None:
        await self.pool.execute("""
            INSERT INTO metrics (
                metric_id, pipeline_id, metric_name, description, sql_expression,
                metric_type, dimensions, schedule_cron, tags,
                created_by, enabled, reasoning, reasoning_history,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            ON CONFLICT (metric_id) DO UPDATE SET
                metric_name=EXCLUDED.metric_name,
                description=EXCLUDED.description,
                sql_expression=EXCLUDED.sql_expression,
                metric_type=EXCLUDED.metric_type,
                dimensions=EXCLUDED.dimensions,
                schedule_cron=EXCLUDED.schedule_cron,
                tags=EXCLUDED.tags,
                enabled=EXCLUDED.enabled,
                reasoning=EXCLUDED.reasoning,
                reasoning_history=EXCLUDED.reasoning_history,
                updated_at=EXCLUDED.updated_at
        """,
            m.metric_id, m.pipeline_id, m.metric_name, m.description,
            m.sql_expression,
            m.metric_type.value if hasattr(m.metric_type, "value") else m.metric_type,
            json.dumps(m.dimensions),
            m.schedule_cron,
            json.dumps(m.tags),
            m.created_by, m.enabled, m.reasoning,
            json.dumps(m.reasoning_history),
            m.created_at, m.updated_at,
        )

    async def get_metric(self, metric_id: str) -> Optional[MetricDefinition]:
        row = await self.pool.fetchrow(
            "SELECT * FROM metrics WHERE metric_id = $1", metric_id
        )
        return _row_to_metric(row) if row else None

    async def list_metrics(self, pipeline_id: str = "") -> list[MetricDefinition]:
        if pipeline_id:
            rows = await self.pool.fetch(
                "SELECT * FROM metrics WHERE pipeline_id = $1 ORDER BY created_at DESC", pipeline_id
            )
        else:
            rows = await self.pool.fetch("SELECT * FROM metrics ORDER BY created_at DESC")
        return [_row_to_metric(r) for r in rows]

    async def delete_metric(self, metric_id: str) -> None:
        await self.pool.execute("DELETE FROM metric_snapshots WHERE metric_id = $1", metric_id)
        await self.pool.execute("DELETE FROM metrics WHERE metric_id = $1", metric_id)

    async def save_metric_snapshot(self, s: MetricSnapshot) -> None:
        await self.pool.execute("""
            INSERT INTO metric_snapshots (
                snapshot_id, metric_id, pipeline_id, computed_at,
                value, dimension_values, metadata
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            s.snapshot_id, s.metric_id, s.pipeline_id, s.computed_at,
            s.value,
            json.dumps(s.dimension_values),
            json.dumps(s.metadata),
        )

    async def list_metric_snapshots(
        self, metric_id: str, limit: int = 100,
    ) -> list[MetricSnapshot]:
        rows = await self.pool.fetch(
            "SELECT * FROM metric_snapshots WHERE metric_id = $1 ORDER BY computed_at DESC LIMIT $2",
            metric_id, limit,
        )
        return [_row_to_metric_snapshot(r) for r in rows]

    async def get_pipeline_by_target_table(self, target_table: str):
        """Look up a pipeline by its target table name (used for ref() resolution)."""
        row = await self.pool.fetchrow(
            "SELECT * FROM pipelines WHERE target_table = $1 LIMIT 1", target_table
        )
        return self._row_to_pipeline(row) if row else None

    # ==================================================================
    # Build 28: Run Context
    # ==================================================================

    async def get_run_context(self, run_id: str) -> Optional[RunContext]:
        """Build a RunContext for a single run: own data + upstream + metadata."""
        run = await self.get_run(run_id)
        if not run:
            return None

        pipeline = await self.get_pipeline(run.pipeline_id)
        p_name = pipeline.pipeline_name if pipeline else ""

        # Quality summary from quality_results
        quality_summary = {}
        if run.quality_results:
            qr = run.quality_results
            checks = qr.get("checks", [])
            quality_summary = {
                "decision": qr.get("decision", ""),
                "checks_passed": sum(1 for c in checks if c.get("status") == "pass"),
                "checks_warned": sum(1 for c in checks if c.get("status") == "warn"),
                "checks_failed": sum(1 for c in checks if c.get("status") == "fail"),
                "total_checks": len(checks),
            }

        # Metadata snapshot
        metadata_items = await self.list_metadata(run.pipeline_id)
        metadata_snapshot = {
            m.key: m.value_json for m in metadata_items
            if m.created_by_run_id == run.run_id
        }

        # Build upstream context recursively (one level deep to avoid deep chains)
        upstream_context = {}
        if run.triggered_by_run_id:
            upstream_ctx = await self._build_upstream_context(run.triggered_by_run_id, depth=0, max_depth=5)
            if upstream_ctx:
                upstream_context = upstream_ctx

        return RunContext(
            run_id=run.run_id,
            pipeline_id=run.pipeline_id,
            pipeline_name=p_name,
            status=run.status.value if hasattr(run.status, "value") else str(run.status),
            rows_extracted=run.rows_extracted,
            rows_loaded=run.rows_loaded,
            watermark_before=run.watermark_before,
            watermark_after=run.watermark_after,
            started_at=run.started_at,
            completed_at=run.completed_at,
            gate_decision=run.gate_decision.value if run.gate_decision and hasattr(run.gate_decision, "value") else (str(run.gate_decision) if run.gate_decision else None),
            quality_summary=quality_summary,
            triggered_by_run_id=run.triggered_by_run_id,
            triggered_by_pipeline_id=run.triggered_by_pipeline_id,
            upstream_context=upstream_context,
            metadata_snapshot=metadata_snapshot,
        )

    async def _build_upstream_context(self, run_id: str, depth: int = 0, max_depth: int = 5) -> dict:
        """Recursively build upstream context chain."""
        if depth >= max_depth:
            return {}
        run = await self.get_run(run_id)
        if not run:
            return {}
        pipeline = await self.get_pipeline(run.pipeline_id)
        p_name = pipeline.pipeline_name if pipeline else ""

        quality_summary = {}
        if run.quality_results:
            qr = run.quality_results
            checks = qr.get("checks", [])
            quality_summary = {
                "decision": qr.get("decision", ""),
                "checks_passed": sum(1 for c in checks if c.get("status") == "pass"),
                "checks_warned": sum(1 for c in checks if c.get("status") == "warn"),
                "checks_failed": sum(1 for c in checks if c.get("status") == "fail"),
            }

        ctx = {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "pipeline_name": p_name,
            "status": run.status.value if hasattr(run.status, "value") else str(run.status),
            "rows_extracted": run.rows_extracted,
            "rows_loaded": run.rows_loaded,
            "watermark_before": run.watermark_before,
            "watermark_after": run.watermark_after,
            "gate_decision": run.gate_decision.value if run.gate_decision and hasattr(run.gate_decision, "value") else None,
            "quality_summary": quality_summary,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
        }

        # Recurse upstream
        if run.triggered_by_run_id:
            ctx["upstream"] = await self._build_upstream_context(
                run.triggered_by_run_id, depth + 1, max_depth,
            )

        return ctx

    async def get_context_chain(self, pipeline_id: str) -> list[dict]:
        """Get the upstream context chain for a pipeline: its latest run context
        plus each upstream dependency's latest run context, walking the DAG."""
        visited = set()
        chain = []
        await self._walk_context_chain(pipeline_id, chain, visited)
        return chain

    async def _walk_context_chain(self, pipeline_id: str, chain: list, visited: set) -> None:
        """Recursively walk upstream dependencies and collect latest run context."""
        if pipeline_id in visited:
            return
        visited.add(pipeline_id)

        pipeline = await self.get_pipeline(pipeline_id)
        if not pipeline:
            return

        last_run = await self.get_last_successful_run(pipeline_id)
        entry = {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline.pipeline_name,
            "tier": pipeline.tier,
            "auto_propagate_context": pipeline.auto_propagate_context,
            "last_run": None,
        }
        if last_run:
            quality_summary = {}
            if last_run.quality_results:
                qr = last_run.quality_results
                checks = qr.get("checks", [])
                quality_summary = {
                    "decision": qr.get("decision", ""),
                    "checks_passed": sum(1 for c in checks if c.get("status") == "pass"),
                    "checks_warned": sum(1 for c in checks if c.get("status") == "warn"),
                    "checks_failed": sum(1 for c in checks if c.get("status") == "fail"),
                }
            entry["last_run"] = {
                "run_id": last_run.run_id,
                "status": last_run.status.value,
                "rows_extracted": last_run.rows_extracted,
                "watermark_after": last_run.watermark_after,
                "gate_decision": last_run.gate_decision.value if last_run.gate_decision else None,
                "quality_summary": quality_summary,
                "completed_at": last_run.completed_at,
            }

        chain.append(entry)

        # Walk upstream
        deps = await self.list_dependencies(pipeline_id)
        for dep in deps:
            await self._walk_context_chain(dep.depends_on_id, chain, visited)

    async def load_upstream_context_for_run(self, run: RunRecord) -> dict:
        """Load full upstream context for a data-triggered run.
        Returns a dict suitable for template variable rendering."""
        if not run.triggered_by_run_id:
            return {}

        upstream_run = await self.get_run(run.triggered_by_run_id)
        if not upstream_run:
            return {}

        pipeline = await self.get_pipeline(upstream_run.pipeline_id)
        p_name = pipeline.pipeline_name if pipeline else ""

        # Gate decision
        gate_str = ""
        if upstream_run.gate_decision:
            gate_str = upstream_run.gate_decision.value if hasattr(upstream_run.gate_decision, "value") else str(upstream_run.gate_decision)

        # Quality summary
        quality_summary = {}
        if upstream_run.quality_results:
            qr = upstream_run.quality_results
            checks = qr.get("checks", [])
            quality_summary = {
                "decision": qr.get("decision", ""),
                "checks_passed": sum(1 for c in checks if c.get("status") == "pass"),
                "checks_warned": sum(1 for c in checks if c.get("status") == "warn"),
                "checks_failed": sum(1 for c in checks if c.get("status") == "fail"),
            }

        # Upstream pipeline metadata
        metadata_items = await self.list_metadata(upstream_run.pipeline_id)
        upstream_metadata = {m.key: m.value_json.get("value", m.value_json) for m in metadata_items}

        return {
            "upstream_run_id": upstream_run.run_id,
            "upstream_pipeline_id": upstream_run.pipeline_id,
            "upstream_pipeline_name": p_name,
            "upstream_gate_decision": gate_str,
            "upstream_quality_decision": quality_summary.get("decision", ""),
            "upstream_quality_checks_passed": str(quality_summary.get("checks_passed", 0)),
            "upstream_quality_checks_warned": str(quality_summary.get("checks_warned", 0)),
            "upstream_quality_checks_failed": str(quality_summary.get("checks_failed", 0)),
            "upstream_watermark_before": upstream_run.watermark_before or "",
            "upstream_watermark_after": upstream_run.watermark_after or "",
            "upstream_rows_extracted": str(upstream_run.rows_extracted),
            "upstream_rows_loaded": str(upstream_run.rows_loaded),
            "upstream_started_at": upstream_run.started_at or "",
            "upstream_completed_at": upstream_run.completed_at or "",
            "upstream_batch_id": upstream_run.run_id[:8],
            "upstream_metadata": upstream_metadata,
        }

    # ==================================================================
    # Build 32: Business Knowledge
    # ==================================================================

    async def get_business_knowledge(self) -> BusinessKnowledge:
        """Get the singleton business knowledge record."""
        row = await self.pool.fetchrow("SELECT * FROM business_knowledge LIMIT 1")
        if not row:
            return BusinessKnowledge()
        return BusinessKnowledge(
            company_name=row.get("company_name", ""),
            industry=row.get("industry", ""),
            business_description=row.get("business_description", ""),
            datasets_description=row.get("datasets_description", ""),
            glossary=json.loads(row.get("glossary", "{}") or "{}"),
            kpi_definitions=json.loads(row.get("kpi_definitions", "[]") or "[]"),
            custom_instructions=row.get("custom_instructions", ""),
            updated_at=row.get("updated_at", ""),
            updated_by=row.get("updated_by", ""),
        )

    async def save_business_knowledge(self, bk: BusinessKnowledge) -> None:
        """Upsert the singleton business knowledge record (id=1)."""
        bk.updated_at = now_iso()
        await self.pool.execute("""
            INSERT INTO business_knowledge (
                id, company_name, industry, business_description,
                datasets_description, glossary, kpi_definitions,
                custom_instructions, updated_at, updated_by
            ) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                company_name=EXCLUDED.company_name,
                industry=EXCLUDED.industry,
                business_description=EXCLUDED.business_description,
                datasets_description=EXCLUDED.datasets_description,
                glossary=EXCLUDED.glossary,
                kpi_definitions=EXCLUDED.kpi_definitions,
                custom_instructions=EXCLUDED.custom_instructions,
                updated_at=EXCLUDED.updated_at,
                updated_by=EXCLUDED.updated_by
        """,
            bk.company_name, bk.industry, bk.business_description,
            bk.datasets_description, json.dumps(bk.glossary),
            json.dumps(bk.kpi_definitions), bk.custom_instructions,
            bk.updated_at, bk.updated_by,
        )


# ======================================================================
# Row-to-model helpers (module-level, stateless)
# ======================================================================

def _row_to_connector(row: asyncpg.Record) -> ConnectorRecord:
    return ConnectorRecord(
        connector_id=row["connector_id"],
        connector_name=row["connector_name"],
        connector_type=ConnectorType(row["connector_type"]),
        source_target_type=row["source_target_type"],
        version=row["version"],
        generated_by=row["generated_by"],
        interface_version=row["interface_version"],
        code=row["code"],
        dependencies=json.loads(row["dependencies"]),
        test_status=TestStatus(row["test_status"]),
        test_results=json.loads(row["test_results"]),
        generation_attempts=row["generation_attempts"],
        generation_log=json.loads(row["generation_log"]),
        status=ConnectorStatus(row["status"]),
        approved_by=row["approved_by"],
        approved_at=row["approved_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_pipeline(row: asyncpg.Record) -> PipelineContract:
    raw_mappings = json.loads(row["column_mappings"])
    mappings = [ColumnMapping(**m) for m in raw_mappings]
    raw_qc = json.loads(row["quality_config"])
    qc = QualityConfig(**raw_qc) if raw_qc else QualityConfig()
    return PipelineContract(
        pipeline_id=row["pipeline_id"],
        pipeline_name=row["pipeline_name"],
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        status=PipelineStatus(row["status"]),
        environment=row["environment"],
        source_connector_id=row["source_connector_id"] or "",
        source_host=row["source_host"],
        source_port=row["source_port"],
        source_database=row["source_database"],
        source_schema=row["source_schema"],
        source_table=row["source_table"],
        source_user=row.get("source_user", ""),
        source_password=row.get("source_password", ""),
        target_connector_id=row["target_connector_id"] or "",
        target_host=row.get("target_host", ""),
        target_port=row.get("target_port", 0),
        target_database=row.get("target_database", ""),
        target_user=row.get("target_user", ""),
        target_password=row.get("target_password", ""),
        target_schema=row["target_schema"],
        target_table=row["target_table"],
        target_options=json.loads(row["target_options"]),
        refresh_type=RefreshType(row["refresh_type"]),
        replication_method=ReplicationMethod(row["replication_method"]),
        incremental_column=row["incremental_column"],
        last_watermark=row["last_watermark"],
        load_type=LoadType(row["load_type"]),
        merge_keys=json.loads(row["merge_keys"]),
        schedule_cron=row["schedule_cron"],
        retry_max_attempts=row["retry_max_attempts"],
        retry_backoff_seconds=row["retry_backoff_seconds"],
        timeout_seconds=row["timeout_seconds"],
        column_mappings=mappings,
        target_ddl=row["target_ddl"],
        quality_config=qc,
        staging_adapter=row["staging_adapter"],
        tier=row["tier"],
        tier_config=json.loads(row["tier_config"]),
        notification_policy_id=row["notification_policy_id"],
        tags=json.loads(row["tags"]),
        owner=row["owner"],
        freshness_column=row["freshness_column"],
        agent_reasoning=json.loads(row["agent_reasoning"]),
        baseline_row_count=row["baseline_row_count"],
        baseline_null_rates=json.loads(row["baseline_null_rates"]),
        baseline_null_stddevs=json.loads(row["baseline_null_stddevs"]),
        baseline_cardinality=json.loads(row["baseline_cardinality"]),
        baseline_volume_avg=row["baseline_volume_avg"],
        baseline_volume_stddev=row["baseline_volume_stddev"],
        auto_approve_additive_schema=row["auto_approve_additive_schema"],
        approval_notification_channel=row["approval_notification_channel"],
        schema_change_policy=_parse_schema_change_policy(row),
        post_promotion_hooks=_parse_post_promotion_hooks(row),
        steps=_parse_steps(row),
        semantic_tags=json.loads(row["semantic_tags"]) if row.get("semantic_tags") else {},
        trust_weights=json.loads(row["trust_weights"]) if row.get("trust_weights") else None,
        business_context=json.loads(row["business_context"]) if row.get("business_context") else {},
        auto_propagate_context=row.get("auto_propagate_context", True),
    )


def _row_to_run(row: asyncpg.Record) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        pipeline_id=row["pipeline_id"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        status=RunStatus(row["status"]),
        run_mode=RunMode(row["run_mode"]),
        backfill_start=row["backfill_start"],
        backfill_end=row["backfill_end"],
        rows_extracted=row["rows_extracted"],
        rows_loaded=row["rows_loaded"],
        watermark_before=row["watermark_before"],
        watermark_after=row["watermark_after"],
        staging_path=row["staging_path"],
        staging_size_bytes=row["staging_size_bytes"],
        drift_detected=json.loads(row["drift_detected"]) if row["drift_detected"] else None,
        quality_results=json.loads(row["quality_results"]) if row["quality_results"] else None,
        gate_decision=GateDecision(row["gate_decision"]) if row["gate_decision"] else None,
        error=row["error"],
        retry_count=row["retry_count"],
        execution_log=json.loads(row["execution_log"]) if row.get("execution_log") else None,
        triggered_by_run_id=row.get("triggered_by_run_id"),
        triggered_by_pipeline_id=row.get("triggered_by_pipeline_id"),
        insights=json.loads(row["insights"]) if row.get("insights") else None,
    )


def _row_to_gate(row: asyncpg.Record) -> GateRecord:
    raw_checks = json.loads(row["checks"])
    checks = [CheckResult(
        check_name=c["check_name"],
        status=CheckStatus(c["status"]),
        detail=c["detail"],
        metadata=c.get("metadata", {}),
        duration_ms=c.get("duration_ms", 0),
    ) for c in raw_checks]
    return GateRecord(
        gate_id=row["gate_id"],
        run_id=row["run_id"],
        pipeline_id=row["pipeline_id"],
        decision=GateDecision(row["decision"]),
        checks=checks,
        agent_reasoning=row["agent_reasoning"],
        evaluated_at=row["evaluated_at"],
    )


def _row_to_proposal(row: asyncpg.Record) -> ContractChangeProposal:
    return ContractChangeProposal(
        proposal_id=row["proposal_id"],
        pipeline_id=row["pipeline_id"],
        connector_id=row["connector_id"],
        created_at=row["created_at"],
        resolved_at=row["resolved_at"],
        status=ProposalStatus(row["status"]),
        trigger_type=TriggerType(row["trigger_type"]),
        trigger_detail=json.loads(row["trigger_detail"]),
        change_type=ChangeType(row["change_type"]),
        current_state=json.loads(row["current_state"]),
        proposed_state=json.loads(row["proposed_state"]),
        reasoning=row["reasoning"],
        confidence=row["confidence"],
        impact_analysis=json.loads(row["impact_analysis"]),
        rollback_plan=row["rollback_plan"],
        resolved_by=row["resolved_by"],
        resolution_note=row["resolution_note"],
        rejection_learning=json.loads(row["rejection_learning"]) if row["rejection_learning"] else None,
        contract_version_before=row["contract_version_before"],
        contract_version_after=row["contract_version_after"],
    )


def _row_to_schema_version(row: asyncpg.Record) -> SchemaVersion:
    raw = json.loads(row["column_mappings"])
    return SchemaVersion(
        version_id=row["version_id"],
        pipeline_id=row["pipeline_id"],
        version=row["version"],
        column_mappings=[ColumnMapping(**m) for m in raw],
        change_summary=row["change_summary"],
        change_type=row["change_type"],
        proposal_id=row["proposal_id"],
        applied_at=row["applied_at"],
        applied_by=row["applied_by"],
    )


def _row_to_metadata(row: asyncpg.Record) -> PipelineMetadata:
    return PipelineMetadata(
        id=row["id"],
        pipeline_id=row["pipeline_id"],
        namespace=row["namespace"],
        key=row["key"],
        value_json=json.loads(row["value_json"]) if isinstance(row["value_json"], str) else row["value_json"],
        updated_at=row["updated_at"],
        created_by_run_id=row["created_by_run_id"],
    )


def _parse_schema_change_policy(row: asyncpg.Record):
    """Deserialize schema_change_policy JSON from a pipeline row."""
    raw = row.get("schema_change_policy")
    if not raw:
        return None
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    if not parsed:
        return None
    return SchemaChangePolicy(**parsed)


def _parse_post_promotion_hooks(row: asyncpg.Record) -> list[PostPromotionHook]:
    """Deserialize post_promotion_hooks JSON from a pipeline row."""
    raw = row.get("post_promotion_hooks")
    if not raw:
        return []
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    if not parsed:
        return []
    return [PostPromotionHook(**h) for h in parsed]


def _parse_steps(row: asyncpg.Record) -> list:
    """Deserialize steps JSON from a pipeline row."""
    raw = row.get("steps")
    if not raw:
        return []
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    if not parsed:
        return []
    from contracts.models import StepDefinition, StepType
    result = []
    for s in parsed:
        st = s.get("step_type", "extract")
        if isinstance(st, str):
            try:
                s["step_type"] = StepType(st)
            except ValueError:
                s["step_type"] = StepType.EXTRACT
        result.append(StepDefinition(**s))
    return result


def _row_to_dependency(row: asyncpg.Record) -> PipelineDependency:
    return PipelineDependency(
        dependency_id=row["dependency_id"],
        pipeline_id=row["pipeline_id"],
        depends_on_id=row["depends_on_id"],
        dependency_type=DependencyType(row["dependency_type"]),
        created_at=row["created_at"],
        notes=row["notes"],
    )


def _row_to_freshness(row: asyncpg.Record) -> FreshnessSnapshot:
    return FreshnessSnapshot(
        snapshot_id=row["snapshot_id"],
        pipeline_id=row["pipeline_id"],
        pipeline_name=row["pipeline_name"],
        tier=row["tier"],
        staleness_minutes=row["staleness_minutes"],
        freshness_sla_minutes=row["freshness_sla_minutes"],
        sla_met=row["sla_met"],
        status=FreshnessStatus(row["status"]),
        last_record_time=row["last_record_time"],
        checked_at=row["checked_at"],
    )


def _row_to_alert(row: asyncpg.Record) -> AlertRecord:
    return AlertRecord(
        alert_id=row["alert_id"],
        severity=AlertSeverity(row["severity"]),
        tier=row["tier"],
        pipeline_id=row["pipeline_id"],
        pipeline_name=row["pipeline_name"],
        summary=row["summary"],
        detail=json.loads(row["detail"]),
        narrative=row.get("narrative", ""),
        created_at=row["created_at"],
        acknowledged=row["acknowledged"],
        acknowledged_by=row["acknowledged_by"],
        acknowledged_at=row["acknowledged_at"],
        digested=row["digested"],
    )


def _row_to_preference(row: asyncpg.Record) -> AgentPreference:
    embedding_raw = row["embedding"]
    if embedding_raw is not None and isinstance(embedding_raw, str):
        # pgvector returns text like '[0.1,0.2,...]'
        embedding_raw = json.loads(embedding_raw)
    return AgentPreference(
        preference_id=row["preference_id"],
        scope=PreferenceScope(row["scope"]),
        scope_value=row["scope_value"],
        preference_key=row["preference_key"],
        preference_value=json.loads(row["preference_value"]),
        source=PreferenceSource(row["source"]),
        confidence=row["confidence"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        usage_count=row["usage_count"],
        embedding=embedding_raw if embedding_raw else [],
        last_used=row["last_used"] or "",
    )


def _row_to_column_lineage(row: asyncpg.Record) -> ColumnLineage:
    return ColumnLineage(
        id=row["id"],
        source_pipeline_id=row["source_pipeline_id"],
        source_schema=row["source_schema"],
        source_table=row["source_table"],
        source_column=row["source_column"],
        target_pipeline_id=row["target_pipeline_id"],
        target_schema=row["target_schema"],
        target_table=row["target_table"],
        target_column=row["target_column"],
        transformation=row["transformation"],
        created_at=row["created_at"],
    )


def _row_to_user(row: asyncpg.Record) -> User:
    return User(
        id=row["id"],
        username=row["username"],
        password_hash=row["password_hash"],
        role=row["role"],
        email=row["email"] or "",
        created_at=row["created_at"],
        last_login=row["last_login"] or "",
    )


def _row_to_data_contract(row: asyncpg.Record) -> DataContract:
    return DataContract(
        contract_id=row["contract_id"],
        producer_pipeline_id=row["producer_pipeline_id"],
        consumer_pipeline_id=row["consumer_pipeline_id"],
        description=row["description"],
        status=DataContractStatus(row["status"]),
        required_columns=json.loads(row["required_columns"]) if isinstance(row["required_columns"], str) else (row["required_columns"] or []),
        freshness_sla_minutes=row["freshness_sla_minutes"],
        retention_hours=row["retention_hours"],
        cleanup_ownership=CleanupOwnership(row["cleanup_ownership"]),
        last_validated_at=row["last_validated_at"],
        last_violation_at=row["last_violation_at"],
        violation_count=row["violation_count"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_contract_violation(row: asyncpg.Record) -> ContractViolation:
    return ContractViolation(
        violation_id=row["violation_id"],
        contract_id=row["contract_id"],
        violation_type=ContractViolationType(row["violation_type"]),
        detail=row["detail"],
        producer_pipeline_id=row["producer_pipeline_id"],
        consumer_pipeline_id=row["consumer_pipeline_id"],
        resolved=row["resolved"],
        resolved_at=row["resolved_at"],
        created_at=row["created_at"],
    )


def _row_to_chat_interaction(row: asyncpg.Record) -> ChatInteraction:
    return ChatInteraction(
        interaction_id=row["interaction_id"],
        session_id=row["session_id"],
        user_id=row["user_id"],
        username=row["username"],
        user_input=row["user_input"],
        routed_action=row["routed_action"],
        action_params=json.loads(row["action_params"]) if row["action_params"] else {},
        agent_response=row["agent_response"],
        result_data=json.loads(row["result_data"]) if row["result_data"] else {},
        input_tokens=row["input_tokens"],
        output_tokens=row["output_tokens"],
        latency_ms=row["latency_ms"],
        model=row["model"],
        error=row["error"],
        created_at=row["created_at"],
    )


def _row_to_registered_source(row: asyncpg.Record) -> RegisteredSource:
    return RegisteredSource(
        source_id=row["source_id"],
        display_name=row["display_name"],
        connector_id=row["connector_id"],
        connector_name=row["connector_name"],
        source_type=row["source_type"],
        connection_params=json.loads(row["connection_params"]) if row["connection_params"] else {},
        description=row["description"],
        owner=row["owner"],
        tags=json.loads(row["tags"]) if row["tags"] else {},
        schema_cache=json.loads(row["schema_cache"]) if row["schema_cache"] else {},
        schema_cache_updated_at=row["schema_cache_updated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_pipeline_change(row: asyncpg.Record) -> PipelineChangeLog:
    return PipelineChangeLog(
        change_id=row["change_id"],
        pipeline_id=row["pipeline_id"],
        pipeline_name=row["pipeline_name"],
        change_type=PipelineChangeType(row["change_type"]),
        changed_by=row["changed_by"],
        changed_by_id=row["changed_by_id"],
        source=row["source"],
        changed_fields=json.loads(row["changed_fields"]) if row["changed_fields"] else {},
        reason=row["reason"],
        context=row["context"],
        created_at=row["created_at"],
    )


def _row_to_sql_transform(row: asyncpg.Record) -> SqlTransform:
    mat = row.get("materialization", "table")
    try:
        mat = MaterializationType(mat)
    except ValueError:
        mat = MaterializationType.TABLE
    return SqlTransform(
        transform_id=row["transform_id"],
        transform_name=row["transform_name"],
        description=row.get("description", ""),
        sql=row.get("sql", ""),
        materialization=mat,
        target_schema=row.get("target_schema", "analytics"),
        target_table=row.get("target_table", ""),
        variables=json.loads(row["variables"]) if row.get("variables") else {},
        refs=json.loads(row["refs"]) if row.get("refs") else [],
        column_lineage=json.loads(row["column_lineage"]) if row.get("column_lineage") else [],
        version=row.get("version", 1),
        created_by=row.get("created_by", "agent"),
        approved=row.get("approved", False),
        pipeline_id=row.get("pipeline_id", ""),
        created_at=row.get("created_at", ""),
        updated_at=row.get("updated_at", ""),
    )


def _row_to_metric(row: asyncpg.Record) -> MetricDefinition:
    mt = row.get("metric_type", "custom")
    try:
        mt = MetricType(mt)
    except ValueError:
        mt = MetricType.CUSTOM
    return MetricDefinition(
        metric_id=row["metric_id"],
        pipeline_id=row["pipeline_id"],
        metric_name=row.get("metric_name", ""),
        description=row.get("description", ""),
        sql_expression=row.get("sql_expression", ""),
        metric_type=mt,
        dimensions=json.loads(row["dimensions"]) if row.get("dimensions") else [],
        schedule_cron=row.get("schedule_cron", ""),
        tags=json.loads(row["tags"]) if row.get("tags") else {},
        created_by=row.get("created_by", "agent"),
        enabled=row.get("enabled", True),
        reasoning=row.get("reasoning", ""),
        reasoning_history=json.loads(row["reasoning_history"]) if row.get("reasoning_history") else [],
        created_at=row.get("created_at", ""),
        updated_at=row.get("updated_at", ""),
    )


def _row_to_metric_snapshot(row: asyncpg.Record) -> MetricSnapshot:
    return MetricSnapshot(
        snapshot_id=row["snapshot_id"],
        metric_id=row["metric_id"],
        pipeline_id=row["pipeline_id"],
        computed_at=row.get("computed_at", ""),
        value=float(row.get("value", 0.0)),
        dimension_values=json.loads(row["dimension_values"]) if row.get("dimension_values") else {},
        metadata=json.loads(row["metadata"]) if row.get("metadata") else {},
    )


# ======================================================================
# DDL for create_tables() -- dev/test convenience
# ======================================================================

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS connectors (
    connector_id TEXT PRIMARY KEY,
    connector_name TEXT UNIQUE NOT NULL,
    connector_type TEXT NOT NULL,
    source_target_type TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    generated_by TEXT NOT NULL DEFAULT 'seed',
    interface_version TEXT NOT NULL DEFAULT '1.0',
    code TEXT NOT NULL DEFAULT '',
    dependencies JSONB NOT NULL DEFAULT '[]',
    test_status TEXT NOT NULL DEFAULT 'untested',
    test_results JSONB NOT NULL DEFAULT '{}',
    generation_attempts INTEGER NOT NULL DEFAULT 0,
    generation_log JSONB NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'draft',
    approved_by TEXT,
    approved_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id TEXT PRIMARY KEY,
    pipeline_name TEXT UNIQUE NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    environment TEXT NOT NULL DEFAULT 'production',
    source_connector_id TEXT REFERENCES connectors(connector_id),
    source_host TEXT NOT NULL DEFAULT '',
    source_port INTEGER NOT NULL DEFAULT 0,
    source_database TEXT NOT NULL DEFAULT '',
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL DEFAULT '',
    source_user TEXT NOT NULL DEFAULT '',
    source_password TEXT NOT NULL DEFAULT '',
    target_connector_id TEXT REFERENCES connectors(connector_id),
    target_host TEXT NOT NULL DEFAULT '',
    target_port INTEGER NOT NULL DEFAULT 0,
    target_database TEXT NOT NULL DEFAULT '',
    target_user TEXT NOT NULL DEFAULT '',
    target_password TEXT NOT NULL DEFAULT '',
    target_schema TEXT NOT NULL DEFAULT 'raw',
    target_table TEXT NOT NULL DEFAULT '',
    target_options JSONB NOT NULL DEFAULT '{}',
    refresh_type TEXT NOT NULL DEFAULT 'full',
    replication_method TEXT NOT NULL DEFAULT 'watermark',
    incremental_column TEXT,
    last_watermark TEXT,
    load_type TEXT NOT NULL DEFAULT 'append',
    merge_keys JSONB NOT NULL DEFAULT '[]',
    schedule_cron TEXT NOT NULL DEFAULT '0 * * * *',
    retry_max_attempts INTEGER NOT NULL DEFAULT 3,
    retry_backoff_seconds INTEGER NOT NULL DEFAULT 60,
    timeout_seconds INTEGER NOT NULL DEFAULT 3600,
    column_mappings JSONB NOT NULL DEFAULT '[]',
    target_ddl TEXT NOT NULL DEFAULT '',
    quality_config JSONB NOT NULL DEFAULT '{}',
    staging_adapter TEXT NOT NULL DEFAULT 'local',
    tier INTEGER NOT NULL DEFAULT 2,
    tier_config JSONB NOT NULL DEFAULT '{}',
    notification_policy_id TEXT,
    tags JSONB NOT NULL DEFAULT '{}',
    owner TEXT,
    freshness_column TEXT,
    agent_reasoning JSONB NOT NULL DEFAULT '{}',
    baseline_row_count INTEGER NOT NULL DEFAULT 0,
    baseline_null_rates JSONB NOT NULL DEFAULT '{}',
    baseline_null_stddevs JSONB NOT NULL DEFAULT '{}',
    baseline_cardinality JSONB NOT NULL DEFAULT '{}',
    baseline_volume_avg DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    baseline_volume_stddev DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    auto_approve_additive_schema BOOLEAN NOT NULL DEFAULT FALSE,
    approval_notification_channel TEXT NOT NULL DEFAULT '',
    schema_change_policy JSONB NOT NULL DEFAULT '{}',
    post_promotion_hooks JSONB NOT NULL DEFAULT '[]',
    steps JSONB NOT NULL DEFAULT '[]',
    auto_propagate_context BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    run_mode TEXT NOT NULL DEFAULT 'scheduled',
    backfill_start TEXT,
    backfill_end TEXT,
    rows_extracted INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    watermark_before TEXT,
    watermark_after TEXT,
    staging_path TEXT NOT NULL DEFAULT '',
    staging_size_bytes BIGINT NOT NULL DEFAULT 0,
    drift_detected JSONB,
    quality_results JSONB,
    gate_decision TEXT,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gates (
    gate_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    checks JSONB NOT NULL DEFAULT '[]',
    agent_reasoning TEXT,
    evaluated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS proposals (
    proposal_id TEXT PRIMARY KEY,
    pipeline_id TEXT,
    connector_id TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    trigger_type TEXT NOT NULL,
    trigger_detail JSONB NOT NULL DEFAULT '{}',
    change_type TEXT NOT NULL,
    current_state JSONB NOT NULL DEFAULT '{}',
    proposed_state JSONB NOT NULL DEFAULT '{}',
    reasoning TEXT NOT NULL DEFAULT '',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    impact_analysis JSONB NOT NULL DEFAULT '{}',
    rollback_plan TEXT NOT NULL DEFAULT '',
    resolved_by TEXT,
    resolution_note TEXT,
    rejection_learning JSONB,
    contract_version_before INTEGER NOT NULL DEFAULT 0,
    contract_version_after INTEGER
);

CREATE TABLE IF NOT EXISTS schema_versions (
    version_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    version INTEGER NOT NULL,
    column_mappings JSONB NOT NULL DEFAULT '[]',
    change_summary TEXT NOT NULL DEFAULT '',
    change_type TEXT NOT NULL DEFAULT 'initial',
    proposal_id TEXT,
    applied_at TEXT NOT NULL,
    applied_by TEXT NOT NULL DEFAULT 'agent'
);

CREATE TABLE IF NOT EXISTS dependencies (
    dependency_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    depends_on_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    dependency_type TEXT NOT NULL DEFAULT 'user_defined',
    created_at TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS notification_policies (
    policy_id TEXT PRIMARY KEY,
    policy_name TEXT UNIQUE NOT NULL,
    description TEXT,
    channels JSONB NOT NULL DEFAULT '[]',
    digest_hour INTEGER NOT NULL DEFAULT 9,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS freshness_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    tier INTEGER NOT NULL,
    staleness_minutes DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    freshness_sla_minutes INTEGER NOT NULL,
    sla_met BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'fresh',
    last_record_time TEXT,
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    severity TEXT NOT NULL,
    tier INTEGER NOT NULL,
    pipeline_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TEXT,
    digested BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS decision_logs (
    id SERIAL PRIMARY KEY,
    pipeline_id TEXT,
    connector_id TEXT,
    decision_type TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    reasoning TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS preferences (
    preference_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL DEFAULT 'global',
    scope_value TEXT,
    preference_key TEXT NOT NULL,
    preference_value JSONB NOT NULL DEFAULT '{}',
    source TEXT NOT NULL DEFAULT 'user_explicit',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    usage_count INTEGER NOT NULL DEFAULT 0,
    embedding vector(1024),
    last_used TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS error_budgets (
    pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(pipeline_id),
    window_days INTEGER NOT NULL DEFAULT 7,
    total_runs INTEGER NOT NULL DEFAULT 0,
    successful_runs INTEGER NOT NULL DEFAULT 0,
    failed_runs INTEGER NOT NULL DEFAULT 0,
    success_rate DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    budget_threshold DOUBLE PRECISION NOT NULL DEFAULT 0.9,
    budget_remaining DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    escalated BOOLEAN NOT NULL DEFAULT FALSE,
    last_calculated TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS column_lineage (
    id TEXT PRIMARY KEY,
    source_pipeline_id TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL DEFAULT '',
    source_column TEXT NOT NULL DEFAULT '',
    target_pipeline_id TEXT NOT NULL,
    target_schema TEXT NOT NULL DEFAULT '',
    target_table TEXT NOT NULL DEFAULT '',
    target_column TEXT NOT NULL DEFAULT '',
    transformation TEXT NOT NULL DEFAULT 'direct',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_cost_logs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_migrations (
    id TEXT PRIMARY KEY,
    connector_id TEXT NOT NULL REFERENCES connectors(connector_id),
    from_version INTEGER NOT NULL DEFAULT 0,
    to_version INTEGER NOT NULL DEFAULT 0,
    affected_pipelines JSONB NOT NULL DEFAULT '[]',
    migration_status TEXT NOT NULL DEFAULT 'pending',
    migration_log TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'viewer',
    email TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    last_login TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    namespace TEXT NOT NULL DEFAULT 'default',
    key TEXT NOT NULL,
    value_json JSONB NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    created_by_run_id TEXT
);

CREATE TABLE IF NOT EXISTS data_contracts (
    contract_id TEXT PRIMARY KEY,
    producer_pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    consumer_pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id),
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    required_columns JSONB NOT NULL DEFAULT '[]',
    freshness_sla_minutes INTEGER NOT NULL DEFAULT 60,
    retention_hours INTEGER NOT NULL DEFAULT 168,
    cleanup_ownership TEXT NOT NULL DEFAULT 'none',
    last_validated_at TEXT,
    last_violation_at TEXT,
    violation_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_violations (
    violation_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL REFERENCES data_contracts(contract_id),
    violation_type TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    producer_pipeline_id TEXT NOT NULL,
    consumer_pipeline_id TEXT NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_interactions (
    interaction_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL DEFAULT 'default',
    user_id TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    user_input TEXT NOT NULL DEFAULT '',
    routed_action TEXT NOT NULL DEFAULT '',
    action_params JSONB NOT NULL DEFAULT '{}',
    agent_response TEXT NOT NULL DEFAULT '',
    result_data JSONB NOT NULL DEFAULT '{}',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    model TEXT NOT NULL DEFAULT '',
    error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registered_sources (
    source_id TEXT PRIMARY KEY,
    display_name TEXT UNIQUE NOT NULL,
    connector_id TEXT NOT NULL,
    connector_name TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT '',
    connection_params JSONB NOT NULL DEFAULT '{}',
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    tags JSONB NOT NULL DEFAULT '{}',
    schema_cache JSONB NOT NULL DEFAULT '{}',
    schema_cache_updated_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_changelog (
    change_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL DEFAULT '',
    change_type TEXT NOT NULL,
    changed_by TEXT NOT NULL DEFAULT '',
    changed_by_id TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'api',
    changed_fields JSONB NOT NULL DEFAULT '{}',
    reason TEXT NOT NULL DEFAULT '',
    context TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS step_executions (
    step_execution_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    step_name TEXT NOT NULL DEFAULT '',
    step_type TEXT NOT NULL DEFAULT 'extract',
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT,
    completed_at TEXT,
    output JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    elapsed_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sql_transforms (
    transform_id TEXT PRIMARY KEY,
    transform_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    sql TEXT NOT NULL DEFAULT '',
    materialization TEXT NOT NULL DEFAULT 'table',
    target_schema TEXT NOT NULL DEFAULT 'analytics',
    target_table TEXT NOT NULL DEFAULT '',
    variables JSONB NOT NULL DEFAULT '{}',
    refs JSONB NOT NULL DEFAULT '[]',
    column_lineage JSONB NOT NULL DEFAULT '[]',
    version INT NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL DEFAULT 'agent',
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    pipeline_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sql_transforms_pipeline ON sql_transforms(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_sql_transforms_name ON sql_transforms(transform_name);
CREATE INDEX IF NOT EXISTS idx_step_executions_run ON step_executions(run_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_pipeline ON step_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_chat_interactions_session ON chat_interactions(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_interactions_username ON chat_interactions(username, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_interactions_created ON chat_interactions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_changelog_pipeline ON pipeline_changelog(pipeline_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_changelog_user ON pipeline_changelog(changed_by, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_changelog_type ON pipeline_changelog(change_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs(pipeline_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_gates_run ON gates(run_id);
CREATE INDEX IF NOT EXISTS idx_gates_pipeline ON gates(pipeline_id, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_proposals_pipeline ON proposals(pipeline_id, status);
CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status);
CREATE INDEX IF NOT EXISTS idx_schema_versions_pipeline ON schema_versions(pipeline_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_dependencies_pipeline ON dependencies(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
CREATE INDEX IF NOT EXISTS idx_freshness_pipeline ON freshness_snapshots(pipeline_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_pipeline ON alerts(pipeline_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_digested ON alerts(digested) WHERE digested = FALSE;
CREATE INDEX IF NOT EXISTS idx_decisions_pipeline ON decision_logs(pipeline_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_preferences_scope ON preferences(scope, scope_value, preference_key);
CREATE INDEX IF NOT EXISTS idx_error_budgets_escalated ON error_budgets(escalated) WHERE escalated = TRUE;
CREATE INDEX IF NOT EXISTS idx_lineage_source ON column_lineage(source_pipeline_id, source_column);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON column_lineage(target_pipeline_id);
CREATE INDEX IF NOT EXISTS idx_cost_logs_pipeline ON agent_cost_logs(pipeline_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_cost_logs_timestamp ON agent_cost_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_connector_migrations_connector ON connector_migrations(connector_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_pipeline_ns_key ON pipeline_metadata(pipeline_id, namespace, key);
CREATE INDEX IF NOT EXISTS idx_metadata_pipeline ON pipeline_metadata(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_data_contracts_producer ON data_contracts(producer_pipeline_id);
CREATE INDEX IF NOT EXISTS idx_data_contracts_consumer ON data_contracts(consumer_pipeline_id);
CREATE INDEX IF NOT EXISTS idx_data_contracts_status ON data_contracts(status);
CREATE INDEX IF NOT EXISTS idx_contract_violations_contract ON contract_violations(contract_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_contract_violations_unresolved ON contract_violations(resolved) WHERE resolved = FALSE;

-- Build 31: Metrics / KPI layer
CREATE TABLE IF NOT EXISTS metrics (
    metric_id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    sql_expression TEXT NOT NULL DEFAULT '',
    metric_type TEXT NOT NULL DEFAULT 'custom',
    dimensions JSONB NOT NULL DEFAULT '[]',
    schedule_cron TEXT NOT NULL DEFAULT '',
    tags JSONB NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT 'agent',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    reasoning TEXT NOT NULL DEFAULT '',
    reasoning_history JSONB NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metrics_pipeline ON metrics(pipeline_id);

CREATE TABLE IF NOT EXISTS metric_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    metric_id TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    dimension_values JSONB NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_metric_snapshots_metric ON metric_snapshots(metric_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_metric_snapshots_pipeline ON metric_snapshots(pipeline_id, computed_at DESC);
"""

_ALTER_TABLES_SQL = """
-- Build 12: Add schema_change_policy column to existing pipelines table
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS schema_change_policy JSONB NOT NULL DEFAULT '{}';
-- Build 13: Add post_promotion_hooks column to existing pipelines table
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS post_promotion_hooks JSONB NOT NULL DEFAULT '[]';
-- Build 15: Run context propagation
ALTER TABLE runs ADD COLUMN IF NOT EXISTS triggered_by_run_id TEXT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS triggered_by_pipeline_id TEXT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS execution_log JSONB;
-- Build 16: Data contracts (tables created via CREATE IF NOT EXISTS; no ALTER needed)
-- Build 18: Composable step DAGs
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS steps JSONB NOT NULL DEFAULT '[]';
-- Build 26: Anomaly narratives on alerts
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS narrative TEXT NOT NULL DEFAULT '';
-- Build 26: Semantic tags, trust weights, business context
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS semantic_tags JSONB NOT NULL DEFAULT '{}';
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS trust_weights JSONB;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS business_context JSONB NOT NULL DEFAULT '{}';
-- Build 30: Run insights
ALTER TABLE runs ADD COLUMN IF NOT EXISTS insights JSONB;
-- Build 28: Auto-propagate upstream context flag
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS auto_propagate_context BOOLEAN NOT NULL DEFAULT TRUE;
-- Build 32: Metric reasoning
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS reasoning TEXT NOT NULL DEFAULT '';
ALTER TABLE metrics ADD COLUMN IF NOT EXISTS reasoning_history JSONB NOT NULL DEFAULT '[]';
-- Build 32: Business knowledge (singleton)
CREATE TABLE IF NOT EXISTS business_knowledge (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    company_name TEXT NOT NULL DEFAULT '',
    industry TEXT NOT NULL DEFAULT '',
    business_description TEXT NOT NULL DEFAULT '',
    datasets_description TEXT NOT NULL DEFAULT '',
    glossary JSONB NOT NULL DEFAULT '{}',
    kpi_definitions JSONB NOT NULL DEFAULT '[]',
    custom_instructions TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT ''
);
"""

# Alias used by several modules (agent, scheduler, monitor, api).
Store = ContractStore
