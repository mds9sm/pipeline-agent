"""001 initial schema

Create all pipeline-agent tables, indexes, and constraints for PostgreSQL
with pgvector support.

Revision ID: 001_initial
Revises: -
Create Date: 2026-03-06
"""
from alembic import op
import sqlalchemy as sa

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # ------------------------------------------------------------------
    # connectors
    # ------------------------------------------------------------------
    op.create_table(
        "connectors",
        sa.Column("connector_id", sa.Text, primary_key=True),
        sa.Column("connector_name", sa.Text, unique=True, nullable=False),
        sa.Column("connector_type", sa.Text, nullable=False),
        sa.Column("source_target_type", sa.Text, nullable=False),
        sa.Column("version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("generated_by", sa.Text, nullable=False, server_default="seed"),
        sa.Column("interface_version", sa.Text, nullable=False, server_default="1.0"),
        sa.Column("code", sa.Text, nullable=False, server_default=""),
        sa.Column("dependencies", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("test_status", sa.Text, nullable=False, server_default="untested"),
        sa.Column("test_results", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("generation_attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("generation_log", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("status", sa.Text, nullable=False, server_default="draft"),
        sa.Column("approved_by", sa.Text, nullable=True),
        sa.Column("approved_at", sa.Text, nullable=True),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # pipelines
    # ------------------------------------------------------------------
    op.create_table(
        "pipelines",
        sa.Column("pipeline_id", sa.Text, primary_key=True),
        sa.Column("pipeline_name", sa.Text, unique=True, nullable=False),
        sa.Column("version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="active"),
        sa.Column("environment", sa.Text, nullable=False, server_default="production"),
        sa.Column(
            "source_connector_id", sa.Text,
            sa.ForeignKey("connectors.connector_id"), nullable=True,
        ),
        sa.Column("source_host", sa.Text, nullable=False, server_default=""),
        sa.Column("source_port", sa.Integer, nullable=False, server_default="3306"),
        sa.Column("source_database", sa.Text, nullable=False, server_default=""),
        sa.Column("source_schema", sa.Text, nullable=False, server_default=""),
        sa.Column("source_table", sa.Text, nullable=False, server_default=""),
        sa.Column(
            "target_connector_id", sa.Text,
            sa.ForeignKey("connectors.connector_id"), nullable=True,
        ),
        sa.Column("target_schema", sa.Text, nullable=False, server_default="raw"),
        sa.Column("target_table", sa.Text, nullable=False, server_default=""),
        sa.Column("target_options", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("refresh_type", sa.Text, nullable=False, server_default="full"),
        sa.Column("replication_method", sa.Text, nullable=False, server_default="watermark"),
        sa.Column("incremental_column", sa.Text, nullable=True),
        sa.Column("last_watermark", sa.Text, nullable=True),
        sa.Column("load_type", sa.Text, nullable=False, server_default="append"),
        sa.Column("merge_keys", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("schedule_cron", sa.Text, nullable=False, server_default="0 * * * *"),
        sa.Column("retry_max_attempts", sa.Integer, nullable=False, server_default="3"),
        sa.Column("retry_backoff_seconds", sa.Integer, nullable=False, server_default="60"),
        sa.Column("timeout_seconds", sa.Integer, nullable=False, server_default="3600"),
        sa.Column("column_mappings", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("target_ddl", sa.Text, nullable=False, server_default=""),
        sa.Column("quality_config", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("staging_adapter", sa.Text, nullable=False, server_default="local"),
        sa.Column("tier", sa.Integer, nullable=False, server_default="2"),
        sa.Column("tier_config", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("notification_policy_id", sa.Text, nullable=True),
        sa.Column("tags", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("owner", sa.Text, nullable=True),
        sa.Column("freshness_column", sa.Text, nullable=True),
        sa.Column("agent_reasoning", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("baseline_row_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("baseline_null_rates", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("baseline_null_stddevs", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("baseline_cardinality", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("baseline_volume_avg", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("baseline_volume_stddev", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("auto_approve_additive_schema", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("approval_notification_channel", sa.Text, nullable=False, server_default=""),
    )

    # ------------------------------------------------------------------
    # runs
    # ------------------------------------------------------------------
    op.create_table(
        "runs",
        sa.Column("run_id", sa.Text, primary_key=True),
        sa.Column(
            "pipeline_id", sa.Text,
            sa.ForeignKey("pipelines.pipeline_id"), nullable=False,
        ),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("completed_at", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("run_mode", sa.Text, nullable=False, server_default="scheduled"),
        sa.Column("backfill_start", sa.Text, nullable=True),
        sa.Column("backfill_end", sa.Text, nullable=True),
        sa.Column("rows_extracted", sa.Integer, nullable=False, server_default="0"),
        sa.Column("rows_loaded", sa.Integer, nullable=False, server_default="0"),
        sa.Column("watermark_before", sa.Text, nullable=True),
        sa.Column("watermark_after", sa.Text, nullable=True),
        sa.Column("staging_path", sa.Text, nullable=False, server_default=""),
        sa.Column("staging_size_bytes", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("drift_detected", sa.JSON, nullable=True),
        sa.Column("quality_results", sa.JSON, nullable=True),
        sa.Column("gate_decision", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("retry_count", sa.Integer, nullable=False, server_default="0"),
    )

    # ------------------------------------------------------------------
    # gates
    # ------------------------------------------------------------------
    op.create_table(
        "gates",
        sa.Column("gate_id", sa.Text, primary_key=True),
        sa.Column("run_id", sa.Text, nullable=False),
        sa.Column("pipeline_id", sa.Text, nullable=False),
        sa.Column("decision", sa.Text, nullable=False),
        sa.Column("checks", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("agent_reasoning", sa.Text, nullable=True),
        sa.Column("evaluated_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # proposals
    # ------------------------------------------------------------------
    op.create_table(
        "proposals",
        sa.Column("proposal_id", sa.Text, primary_key=True),
        sa.Column("pipeline_id", sa.Text, nullable=True),
        sa.Column("connector_id", sa.Text, nullable=True),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("resolved_at", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("trigger_type", sa.Text, nullable=False),
        sa.Column("trigger_detail", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("change_type", sa.Text, nullable=False),
        sa.Column("current_state", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("proposed_state", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("reasoning", sa.Text, nullable=False, server_default=""),
        sa.Column("confidence", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("impact_analysis", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("rollback_plan", sa.Text, nullable=False, server_default=""),
        sa.Column("resolved_by", sa.Text, nullable=True),
        sa.Column("resolution_note", sa.Text, nullable=True),
        sa.Column("rejection_learning", sa.JSON, nullable=True),
        sa.Column("contract_version_before", sa.Integer, nullable=False, server_default="0"),
        sa.Column("contract_version_after", sa.Integer, nullable=True),
    )

    # ------------------------------------------------------------------
    # schema_versions
    # ------------------------------------------------------------------
    op.create_table(
        "schema_versions",
        sa.Column("version_id", sa.Text, primary_key=True),
        sa.Column(
            "pipeline_id", sa.Text,
            sa.ForeignKey("pipelines.pipeline_id"), nullable=False,
        ),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("column_mappings", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("change_summary", sa.Text, nullable=False, server_default=""),
        sa.Column("change_type", sa.Text, nullable=False, server_default="initial"),
        sa.Column("proposal_id", sa.Text, nullable=True),
        sa.Column("applied_at", sa.Text, nullable=False),
        sa.Column("applied_by", sa.Text, nullable=False, server_default="agent"),
    )

    # ------------------------------------------------------------------
    # dependencies
    # ------------------------------------------------------------------
    op.create_table(
        "dependencies",
        sa.Column("dependency_id", sa.Text, primary_key=True),
        sa.Column(
            "pipeline_id", sa.Text,
            sa.ForeignKey("pipelines.pipeline_id"), nullable=False,
        ),
        sa.Column(
            "depends_on_id", sa.Text,
            sa.ForeignKey("pipelines.pipeline_id"), nullable=False,
        ),
        sa.Column("dependency_type", sa.Text, nullable=False, server_default="user_defined"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("notes", sa.Text, nullable=True),
    )

    # ------------------------------------------------------------------
    # notification_policies
    # ------------------------------------------------------------------
    op.create_table(
        "notification_policies",
        sa.Column("policy_id", sa.Text, primary_key=True),
        sa.Column("policy_name", sa.Text, unique=True, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("channels", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("digest_hour", sa.Integer, nullable=False, server_default="9"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # freshness_snapshots
    # ------------------------------------------------------------------
    op.create_table(
        "freshness_snapshots",
        sa.Column("snapshot_id", sa.Text, primary_key=True),
        sa.Column("pipeline_id", sa.Text, nullable=False),
        sa.Column("pipeline_name", sa.Text, nullable=False),
        sa.Column("tier", sa.Integer, nullable=False),
        sa.Column("staleness_minutes", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("freshness_sla_minutes", sa.Integer, nullable=False),
        sa.Column("sla_met", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("status", sa.Text, nullable=False, server_default="fresh"),
        sa.Column("last_record_time", sa.Text, nullable=True),
        sa.Column("checked_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # alerts
    # ------------------------------------------------------------------
    op.create_table(
        "alerts",
        sa.Column("alert_id", sa.Text, primary_key=True),
        sa.Column("severity", sa.Text, nullable=False),
        sa.Column("tier", sa.Integer, nullable=False),
        sa.Column("pipeline_id", sa.Text, nullable=False),
        sa.Column("pipeline_name", sa.Text, nullable=False),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("detail", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("acknowledged", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("acknowledged_by", sa.Text, nullable=True),
        sa.Column("acknowledged_at", sa.Text, nullable=True),
        sa.Column("digested", sa.Boolean, nullable=False, server_default="false"),
    )

    # ------------------------------------------------------------------
    # decision_logs
    # ------------------------------------------------------------------
    op.create_table(
        "decision_logs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pipeline_id", sa.Text, nullable=True),
        sa.Column("connector_id", sa.Text, nullable=True),
        sa.Column("decision_type", sa.Text, nullable=False),
        sa.Column("detail", sa.Text, nullable=False, server_default=""),
        sa.Column("reasoning", sa.Text, nullable=False, server_default=""),
        sa.Column("created_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # preferences (with pgvector column)
    # ------------------------------------------------------------------
    op.create_table(
        "preferences",
        sa.Column("preference_id", sa.Text, primary_key=True),
        sa.Column("scope", sa.Text, nullable=False, server_default="global"),
        sa.Column("scope_value", sa.Text, nullable=True),
        sa.Column("preference_key", sa.Text, nullable=False),
        sa.Column("preference_value", sa.JSON, nullable=False, server_default="{}"),
        sa.Column("source", sa.Text, nullable=False, server_default="user_explicit"),
        sa.Column("confidence", sa.Float, nullable=False, server_default="1.0"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
        sa.Column("usage_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_used", sa.Text, nullable=False, server_default=""),
    )
    # Add vector column via raw SQL (SQLAlchemy does not natively support pgvector types)
    op.execute("ALTER TABLE preferences ADD COLUMN IF NOT EXISTS embedding vector(1024)")

    # ------------------------------------------------------------------
    # error_budgets
    # ------------------------------------------------------------------
    op.create_table(
        "error_budgets",
        sa.Column(
            "pipeline_id", sa.Text,
            sa.ForeignKey("pipelines.pipeline_id"), primary_key=True,
        ),
        sa.Column("window_days", sa.Integer, nullable=False, server_default="7"),
        sa.Column("total_runs", sa.Integer, nullable=False, server_default="0"),
        sa.Column("successful_runs", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failed_runs", sa.Integer, nullable=False, server_default="0"),
        sa.Column("success_rate", sa.Float, nullable=False, server_default="1.0"),
        sa.Column("budget_threshold", sa.Float, nullable=False, server_default="0.9"),
        sa.Column("budget_remaining", sa.Float, nullable=False, server_default="1.0"),
        sa.Column("escalated", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("last_calculated", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # column_lineage
    # ------------------------------------------------------------------
    op.create_table(
        "column_lineage",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("source_pipeline_id", sa.Text, nullable=False),
        sa.Column("source_schema", sa.Text, nullable=False, server_default=""),
        sa.Column("source_table", sa.Text, nullable=False, server_default=""),
        sa.Column("source_column", sa.Text, nullable=False, server_default=""),
        sa.Column("target_pipeline_id", sa.Text, nullable=False),
        sa.Column("target_schema", sa.Text, nullable=False, server_default=""),
        sa.Column("target_table", sa.Text, nullable=False, server_default=""),
        sa.Column("target_column", sa.Text, nullable=False, server_default=""),
        sa.Column("transformation", sa.Text, nullable=False, server_default="direct"),
        sa.Column("created_at", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # agent_cost_logs
    # ------------------------------------------------------------------
    op.create_table(
        "agent_cost_logs",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("pipeline_id", sa.Text, nullable=False, server_default=""),
        sa.Column("operation", sa.Text, nullable=False, server_default=""),
        sa.Column("model", sa.Text, nullable=False, server_default=""),
        sa.Column("input_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("output_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("latency_ms", sa.Integer, nullable=False, server_default="0"),
        sa.Column("timestamp", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # connector_migrations
    # ------------------------------------------------------------------
    op.create_table(
        "connector_migrations",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column(
            "connector_id", sa.Text,
            sa.ForeignKey("connectors.connector_id"), nullable=False,
        ),
        sa.Column("from_version", sa.Integer, nullable=False, server_default="0"),
        sa.Column("to_version", sa.Integer, nullable=False, server_default="0"),
        sa.Column("affected_pipelines", sa.JSON, nullable=False, server_default="[]"),
        sa.Column("migration_status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("migration_log", sa.Text, nullable=False, server_default=""),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("completed_at", sa.Text, nullable=False, server_default=""),
    )

    # ------------------------------------------------------------------
    # users
    # ------------------------------------------------------------------
    op.create_table(
        "users",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("username", sa.Text, unique=True, nullable=False),
        sa.Column("password_hash", sa.Text, nullable=False, server_default=""),
        sa.Column("role", sa.Text, nullable=False, server_default="viewer"),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("last_login", sa.Text, nullable=False, server_default=""),
    )

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------
    op.create_index("idx_runs_pipeline", "runs", ["pipeline_id", sa.text("started_at DESC")])
    op.create_index("idx_runs_status", "runs", ["status"])
    op.create_index("idx_gates_run", "gates", ["run_id"])
    op.create_index("idx_gates_pipeline", "gates", ["pipeline_id", sa.text("evaluated_at DESC")])
    op.create_index("idx_proposals_pipeline", "proposals", ["pipeline_id", "status"])
    op.create_index("idx_proposals_status", "proposals", ["status"])
    op.create_index("idx_schema_versions_pipeline", "schema_versions", ["pipeline_id", sa.text("version DESC")])
    op.create_index("idx_dependencies_pipeline", "dependencies", ["pipeline_id"])
    op.create_index("idx_dependencies_depends_on", "dependencies", ["depends_on_id"])
    op.create_index("idx_freshness_pipeline", "freshness_snapshots", ["pipeline_id", sa.text("checked_at DESC")])
    op.create_index("idx_alerts_pipeline", "alerts", ["pipeline_id", sa.text("created_at DESC")])
    op.create_index("idx_alerts_severity", "alerts", ["severity", sa.text("created_at DESC")])
    op.execute("CREATE INDEX IF NOT EXISTS idx_alerts_digested ON alerts(digested) WHERE digested = FALSE")
    op.create_index("idx_decisions_pipeline", "decision_logs", ["pipeline_id", sa.text("created_at DESC")])
    op.create_index("idx_preferences_scope", "preferences", ["scope", "scope_value", "preference_key"])
    op.execute("CREATE INDEX IF NOT EXISTS idx_error_budgets_escalated ON error_budgets(escalated) WHERE escalated = TRUE")
    op.create_index("idx_lineage_source", "column_lineage", ["source_pipeline_id", "source_column"])
    op.create_index("idx_lineage_target", "column_lineage", ["target_pipeline_id"])
    op.create_index("idx_cost_logs_pipeline", "agent_cost_logs", ["pipeline_id", sa.text("timestamp DESC")])
    op.create_index("idx_cost_logs_timestamp", "agent_cost_logs", [sa.text("timestamp DESC")])
    op.create_index("idx_connector_migrations_connector", "connector_migrations", ["connector_id", sa.text("created_at DESC")])
    op.create_index("idx_users_username", "users", ["username"])


def downgrade() -> None:
    # Drop tables in reverse dependency order
    op.drop_table("connector_migrations")
    op.drop_table("agent_cost_logs")
    op.drop_table("column_lineage")
    op.drop_table("error_budgets")
    op.drop_table("preferences")
    op.drop_table("decision_logs")
    op.drop_table("alerts")
    op.drop_table("freshness_snapshots")
    op.drop_table("notification_policies")
    op.drop_table("dependencies")
    op.drop_table("schema_versions")
    op.drop_table("proposals")
    op.drop_table("gates")
    op.drop_table("runs")
    op.drop_table("users")
    op.drop_table("pipelines")
    op.drop_table("connectors")
    op.execute("DROP EXTENSION IF EXISTS vector")
