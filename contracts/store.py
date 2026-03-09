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
    ColumnMapping, QualityConfig, CheckResult,
    PipelineStatus, RunStatus, RunMode, RefreshType, ReplicationMethod,
    LoadType, GateDecision, CheckStatus, ProposalStatus, TriggerType,
    ChangeType, ConnectorStatus, ConnectorType, TestStatus, AlertSeverity,
    FreshnessStatus, DependencyType, PreferenceScope, PreferenceSource,
    now_iso,
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
        await self.pool.execute("""
            INSERT INTO pipelines (
                pipeline_id, pipeline_name, version, created_at, updated_at,
                status, environment,
                source_connector_id, source_host, source_port, source_database,
                source_schema, source_table,
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
                auto_approve_additive_schema, approval_notification_channel
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                $20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,
                $37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51
            )
            ON CONFLICT (pipeline_id) DO UPDATE SET
                pipeline_name=EXCLUDED.pipeline_name, version=EXCLUDED.version,
                updated_at=EXCLUDED.updated_at, status=EXCLUDED.status,
                environment=EXCLUDED.environment,
                source_connector_id=EXCLUDED.source_connector_id,
                source_host=EXCLUDED.source_host, source_port=EXCLUDED.source_port,
                source_database=EXCLUDED.source_database,
                source_schema=EXCLUDED.source_schema, source_table=EXCLUDED.source_table,
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
                approval_notification_channel=EXCLUDED.approval_notification_channel
        """,
            p.pipeline_id, p.pipeline_name, p.version, p.created_at, p.updated_at,
            p.status.value, p.environment,
            p.source_connector_id, p.source_host, p.source_port, p.source_database,
            p.source_schema, p.source_table,
            p.target_connector_id, p.target_host, p.target_port, p.target_database,
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
        )

    async def get_pipeline(self, pipeline_id: str) -> Optional[PipelineContract]:
        row = await self.pool.fetchrow(
            "SELECT * FROM pipelines WHERE pipeline_id = $1", pipeline_id
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
                error, retry_count
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
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
                error=EXCLUDED.error, retry_count=EXCLUDED.retry_count
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

    async def get_last_successful_run(self, pipeline_id: str) -> Optional[RunRecord]:
        row = await self.pool.fetchrow("""
            SELECT * FROM runs
            WHERE pipeline_id = $1 AND status = 'complete'
            ORDER BY started_at DESC LIMIT 1
        """, pipeline_id)
        return _row_to_run(row) if row else None

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

    # ==================================================================
    # Alerts
    # ==================================================================

    async def save_alert(self, a: AlertRecord) -> None:
        await self.pool.execute("""
            INSERT INTO alerts (
                alert_id, severity, tier, pipeline_id, pipeline_name,
                summary, detail, created_at, acknowledged,
                acknowledged_by, acknowledged_at, digested
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (alert_id) DO UPDATE SET
                acknowledged=EXCLUDED.acknowledged,
                acknowledged_by=EXCLUDED.acknowledged_by,
                acknowledged_at=EXCLUDED.acknowledged_at,
                digested=EXCLUDED.digested
        """,
            a.alert_id, a.severity.value, a.tier,
            a.pipeline_id, a.pipeline_name, a.summary,
            json.dumps(a.detail), a.created_at,
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
    approval_notification_channel TEXT NOT NULL DEFAULT ''
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

-- Indexes
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
"""

# Alias used by several modules (agent, scheduler, monitor, api).
Store = ContractStore
