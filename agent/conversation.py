"""
Conversation manager -- stateless onboarding / discovery flow with fallback handling.
Each method handles one step. The UI manages session state.
"""
from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Optional

from config import Config
from contracts.models import (
    PipelineContract, SchemaVersion, PipelineDependency, ColumnLineage,
    ColumnMapping, QualityConfig, RefreshType, ReplicationMethod,
    LoadType, PipelineStatus, DependencyType,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from agent.core import AgentCore
from crypto import encrypt_dict, CREDENTIAL_FIELDS

log = logging.getLogger(__name__)


class ConversationManager:
    """Handles the onboarding / discovery flow for pipeline creation."""

    def __init__(
        self,
        config: Config,
        store: Store,
        registry: ConnectorRegistry,
        agent: AgentCore,
    ):
        self.config = config
        self.store = store
        self.registry = registry
        self.agent = agent

    # ------------------------------------------------------------------
    # Step 1: Test connections
    # ------------------------------------------------------------------

    async def test_source_connection(
        self,
        connector_id: str,
        params: dict,
    ) -> dict:
        """Test a source connection. Returns dict with success, version, etc."""
        try:
            source = await self.registry.get_source(connector_id, params)
            result = await source.test_connection()
            return {
                "success": result.success,
                "version": result.version,
                "ssl_enabled": result.ssl_enabled,
                "connection_count": result.connection_count,
                "latency_ms": result.latency_ms,
                "error": result.error,
            }
        except Exception as e:
            log.warning("Source connection test failed: %s", e)
            return {"success": False, "error": str(e)}

    async def test_target_connection(
        self,
        connector_id: str,
        params: dict,
    ) -> dict:
        """Test a target connection. Returns dict with success, version, etc."""
        try:
            target = await self.registry.get_target(connector_id, params)
            result = await target.test_connection()
            return {
                "success": result.success,
                "version": result.version,
                "latency_ms": result.latency_ms,
                "error": result.error,
            }
        except Exception as e:
            log.warning("Target connection test failed: %s", e)
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Step 2: List schemas
    # ------------------------------------------------------------------

    async def list_schemas(
        self,
        connector_id: str,
        params: dict,
    ) -> list[dict]:
        """List available schemas in a source database."""
        source = await self.registry.get_source(connector_id, params)
        schemas = await source.list_schemas()
        return [
            {
                "schema_name": s.schema_name,
                "table_count": s.table_count,
                "tables": s.tables,
            }
            for s in schemas
        ]

    # ------------------------------------------------------------------
    # Step 3: Profile tables
    # ------------------------------------------------------------------

    async def profile_tables(
        self,
        connector_id: str,
        params: dict,
        schema: str,
        tables: Optional[list[str]] = None,
    ) -> list[dict]:
        """Profile one or more tables. Discovers tables if none specified."""
        source = await self.registry.get_source(connector_id, params)

        if not tables:
            schemas = await source.list_schemas()
            for s in schemas:
                if s.schema_name == schema:
                    tables = s.tables
                    break
            tables = tables or []

        profiles = []
        for table in tables:
            try:
                profile = await source.profile_table(schema, table)
                profiles.append({
                    "schema_name": schema,
                    "table_name": table,
                    "row_count_estimate": profile.row_count_estimate,
                    "column_count": profile.column_count,
                    "primary_keys": profile.primary_keys,
                    "timestamp_columns": profile.timestamp_columns,
                    "foreign_keys": profile.foreign_keys,
                    "null_rates": profile.null_rates,
                    "cardinality": profile.cardinality,
                    "columns": [asdict(c) for c in profile.columns],
                    "sample_rows": profile.sample_rows[:3],
                })
            except Exception as e:
                log.warning("Failed to profile %s.%s: %s", schema, table, e)
                profiles.append({
                    "schema_name": schema,
                    "table_name": table,
                    "error": str(e),
                })
        return profiles

    # ------------------------------------------------------------------
    # Step 4: Propose strategies
    # ------------------------------------------------------------------

    async def propose_strategies(
        self,
        connector_id: str,
        params: dict,
        schema: str,
        tables: Optional[list[str]] = None,
    ) -> list[dict]:
        """Profile each table, then propose ingestion strategy via agent.

        Falls back to rule-based strategy if LLM fails, with fallback_used=True.
        """
        source = await self.registry.get_source(connector_id, params)

        if not tables:
            schemas = await source.list_schemas()
            for s in schemas:
                if s.schema_name == schema:
                    tables = s.tables
                    break
            tables = tables or []

        proposals = []
        for table in tables:
            try:
                profile = await source.profile_table(schema, table)

                # Load preferences for context
                prefs = (
                    await self.store.get_preferences("schema")
                    + await self.store.get_preferences("global")
                )

                try:
                    strategy = await self.agent.propose_strategy(profile, prefs)
                    fallback_used = False
                except Exception as llm_err:
                    log.warning(
                        "LLM strategy proposal failed for %s.%s: %s. "
                        "Using rule-based fallback.",
                        schema, table, llm_err,
                    )
                    strategy = self.agent._rule_based_strategy(profile)
                    fallback_used = True

                proposals.append({
                    "table": table,
                    "profile_summary": {
                        "row_count_estimate": profile.row_count_estimate,
                        "column_count": profile.column_count,
                        "primary_keys": profile.primary_keys,
                        "timestamp_columns": profile.timestamp_columns,
                    },
                    "strategy": strategy,
                    "fallback_used": fallback_used,
                })

            except Exception as e:
                log.error("Failed to propose strategy for %s.%s: %s", schema, table, e)
                proposals.append({"table": table, "error": str(e)})

        return proposals

    # ------------------------------------------------------------------
    # Step 5: Preview pipeline
    # ------------------------------------------------------------------

    async def preview_pipeline(
        self,
        strategy: dict,
        source_connector_id: str,
        target_connector_id: str,
        source_params: dict,
        target_params: dict,
    ) -> dict:
        """Preview DDL, column mappings, and sample rows before creation."""
        source = await self.registry.get_source(source_connector_id, source_params)
        target = await self.registry.get_target(target_connector_id, target_params)

        schema_name = strategy.get("source_schema", "")
        table_name = strategy.get("source_table", "")

        profile = await source.profile_table(schema_name, table_name)

        # Build a temporary contract to generate DDL
        target_table = f"{schema_name}_{table_name}"
        temp_contract = PipelineContract(
            source_schema=schema_name,
            source_table=table_name,
            target_schema=strategy.get("target_schema", "raw"),
            target_table=target_table,
            column_mappings=profile.columns,
            refresh_type=RefreshType(strategy.get("refresh_type", "full")),
            replication_method=ReplicationMethod(
                strategy.get("replication_method", "watermark"),
            ),
            load_type=LoadType(strategy.get("load_type", "append")),
            merge_keys=strategy.get("merge_keys", []),
            incremental_column=strategy.get("incremental_column"),
            target_options=strategy.get("target_options", {}),
        )

        ddl = target.generate_ddl(temp_contract)

        return {
            "pipeline_name": f"{schema_name}.{table_name}",
            "source": f"{schema_name}.{table_name}",
            "target": f"{temp_contract.target_schema}.{target_table}",
            "ddl": ddl,
            "column_mappings": [asdict(m) for m in profile.columns],
            "strategy": {
                "refresh_type": strategy.get("refresh_type", "full"),
                "replication_method": strategy.get("replication_method", "watermark"),
                "load_type": strategy.get("load_type", "append"),
                "merge_keys": strategy.get("merge_keys", []),
                "incremental_column": strategy.get("incremental_column"),
                "tier": strategy.get("tier", 2),
            },
            "sample_rows": profile.sample_rows[:5],
        }

    # ------------------------------------------------------------------
    # Step 6: Create pipeline
    # ------------------------------------------------------------------

    async def create_pipeline(
        self,
        strategy: dict,
        source_connector_id: str,
        target_connector_id: str,
        source_params: dict,
        target_params: dict,
        schedule: str,
        owner: Optional[str] = None,
        tags: Optional[dict] = None,
    ) -> PipelineContract:
        """Build and persist a PipelineContract from a strategy dict.

        Encrypts credentials if config.has_encryption_key.
        Writes initial SchemaVersion and ColumnLineage.
        """
        schema_name = strategy.get("source_schema", "")
        table_name = strategy.get("source_table", "")
        target_table = f"{schema_name}_{table_name}"
        pipeline_name = f"{schema_name}.{table_name}"

        # Build column mappings from strategy or profile
        mappings: list[ColumnMapping] = []
        raw_mappings = strategy.get("column_mappings", [])
        for m in raw_mappings:
            if isinstance(m, dict):
                mappings.append(ColumnMapping(**m))
            elif isinstance(m, ColumnMapping):
                mappings.append(m)

        # Encrypt credentials if key is available
        encrypted_source_params = dict(source_params)
        encrypted_target_params = dict(target_params)
        if self.config.has_encryption_key:
            encrypted_source_params = encrypt_dict(
                source_params, self.config.encryption_key, CREDENTIAL_FIELDS,
            )
            encrypted_target_params = encrypt_dict(
                target_params, self.config.encryption_key, CREDENTIAL_FIELDS,
            )

        pipeline = PipelineContract(
            pipeline_name=pipeline_name,
            environment=strategy.get("environment", "production"),
            source_connector_id=source_connector_id,
            source_host=encrypted_source_params.get("host", ""),
            source_port=encrypted_source_params.get("port", 0),
            source_database=encrypted_source_params.get("database", ""),
            source_schema=schema_name,
            source_table=table_name,
            target_connector_id=target_connector_id,
            target_host=encrypted_target_params.get("host", ""),
            target_port=encrypted_target_params.get("port", 0),
            target_database=encrypted_target_params.get("database", ""),
            target_user=encrypted_target_params.get("user", ""),
            target_password=encrypted_target_params.get("password", ""),
            target_schema=strategy.get("target_schema", "raw"),
            target_table=target_table,
            target_options=strategy.get("target_options", {}),
            refresh_type=RefreshType(strategy.get("refresh_type", "full")),
            replication_method=ReplicationMethod(
                strategy.get("replication_method", "watermark"),
            ),
            incremental_column=strategy.get("incremental_column"),
            load_type=LoadType(strategy.get("load_type", "append")),
            merge_keys=strategy.get("merge_keys", []),
            schedule_cron=schedule,
            tier=strategy.get("tier", 2),
            owner=owner,
            tags=tags or {},
            quality_config=QualityConfig(),
            column_mappings=mappings,
            agent_reasoning=strategy.get("reasoning", {}),
            auto_approve_additive_schema=strategy.get(
                "auto_approve_additive", False,
            ),
            status=PipelineStatus.ACTIVE,
        )

        # Default freshness column to incremental column
        pipeline.freshness_column = pipeline.incremental_column

        await self.store.save_pipeline(pipeline)

        # Write initial schema version
        sv = SchemaVersion(
            pipeline_id=pipeline.pipeline_id,
            version=1,
            column_mappings=mappings,
            change_summary=(
                f"Initial schema: {len(mappings)} columns "
                f"from {schema_name}.{table_name}"
            ),
            change_type="initial",
            applied_by="user",
        )
        await self.store.save_schema_version(sv)

        # Write initial column lineage
        for mapping in mappings:
            lineage = ColumnLineage(
                source_pipeline_id=pipeline.pipeline_id,
                source_schema=schema_name,
                source_table=table_name,
                source_column=mapping.source_column,
                target_pipeline_id=pipeline.pipeline_id,
                target_schema=pipeline.target_schema,
                target_table=target_table,
                target_column=mapping.target_column,
                transformation="direct",
            )
            await self.store.save_column_lineage(lineage)

        log.info(
            "Created pipeline: %s (%s) with %d columns",
            pipeline.pipeline_name, pipeline.pipeline_id[:8], len(mappings),
        )
        return pipeline

    # ------------------------------------------------------------------
    # Batch creation
    # ------------------------------------------------------------------

    async def create_pipelines_batch(
        self,
        pipelines_config: list[dict],
    ) -> list[PipelineContract]:
        """Create multiple pipelines with per-pipeline error handling."""
        created: list[PipelineContract] = []
        for idx, cfg in enumerate(pipelines_config):
            try:
                pipeline = await self.create_pipeline(
                    strategy=cfg.get("strategy", {}),
                    source_connector_id=cfg.get("source_connector_id", ""),
                    target_connector_id=cfg.get("target_connector_id", ""),
                    source_params=cfg.get("source_params", {}),
                    target_params=cfg.get("target_params", {}),
                    schedule=cfg.get("schedule", "0 * * * *"),
                    owner=cfg.get("owner"),
                    tags=cfg.get("tags"),
                )
                created.append(pipeline)
            except Exception as e:
                table_name = cfg.get("strategy", {}).get("source_table", f"#{idx}")
                log.error(
                    "Failed to create pipeline for %s: %s", table_name, e,
                )
        return created

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    async def declare_dependency(
        self,
        upstream_id: str,
        downstream_id: str,
        dep_type: DependencyType = DependencyType.USER_DEFINED,
    ) -> PipelineDependency:
        """Declare a dependency between two pipelines."""
        dep = PipelineDependency(
            pipeline_id=downstream_id,
            depends_on_id=upstream_id,
            dependency_type=dep_type,
        )
        await self.store.save_dependency(dep)
        log.info(
            "Declared dependency: %s -> %s (%s)",
            upstream_id[:8], downstream_id[:8], dep_type.value,
        )
        return dep
