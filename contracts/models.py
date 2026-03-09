from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import uuid
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PipelineStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    ARCHIVED = "archived"


class RunStatus(str, Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    STAGING = "staging"
    LOADING = "loading"
    QUALITY_GATE = "quality_gate"
    PROMOTING = "promoting"
    COMPLETE = "complete"
    FAILED = "failed"
    HALTED = "halted"
    RETRYING = "retrying"


class RunMode(str, Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    BACKFILL = "backfill"
    DATA_TRIGGERED = "data_triggered"


class RefreshType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class ReplicationMethod(str, Enum):
    WATERMARK = "watermark"
    CDC = "cdc"
    SNAPSHOT = "snapshot"


class LoadType(str, Enum):
    APPEND = "append"
    MERGE = "merge"


class GateDecision(str, Enum):
    PROMOTE = "promote"
    PROMOTE_WITH_WARNING = "promote_with_warning"
    HALT = "halt"


class CheckStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class ProposalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    APPLIED = "applied"
    ROLLED_BACK = "rolled_back"


class TriggerType(str, Enum):
    SCHEMA_DRIFT = "schema_drift"
    USER_REQUEST = "user_request"
    AGENT_RECOMMENDATION = "agent_recommendation"
    QUALITY_ALERT = "quality_alert"
    NEW_CONNECTOR = "new_connector"


class ChangeType(str, Enum):
    ADD_COLUMN = "add_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    DROP_COLUMN = "drop_column"
    CHANGE_REFRESH_TYPE = "change_refresh_type"
    CHANGE_LOAD_TYPE = "change_load_type"
    CHANGE_MERGE_KEYS = "change_merge_keys"
    CHANGE_SCHEDULE = "change_schedule"
    ADD_TABLE = "add_table"
    REMOVE_TABLE = "remove_table"
    NEW_CONNECTOR = "new_connector"
    UPDATE_CONNECTOR = "update_connector"


class ConnectorStatus(str, Enum):
    DRAFT = "draft"
    APPROVED = "approved"
    ACTIVE = "active"
    DEPRECATED = "deprecated"


class ConnectorType(str, Enum):
    SOURCE = "source"
    TARGET = "target"


class TestStatus(str, Enum):
    UNTESTED = "untested"
    PASSED = "passed"
    FAILED = "failed"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class FreshnessStatus(str, Enum):
    FRESH = "fresh"
    WARNING = "warning"
    CRITICAL = "critical"


class DependencyType(str, Enum):
    FK_INFERRED = "fk_inferred"
    USER_DEFINED = "user_defined"
    AGENT_RECOMMENDED = "agent_recommended"


class PreferenceScope(str, Enum):
    GLOBAL = "global"
    PIPELINE = "pipeline"
    SCHEMA = "schema"
    SOURCE_TYPE = "source_type"
    TARGET_TYPE = "target_type"


class PreferenceSource(str, Enum):
    USER_EXPLICIT = "user_explicit"
    REJECTION_INFERRED = "rejection_inferred"
    APPROVAL_PATTERN = "approval_pattern"


class SchemaColumnAction(str, Enum):
    AUTO_ADD = "auto_add"
    PROPOSE = "propose"
    IGNORE = "ignore"


class SchemaDropAction(str, Enum):
    HALT = "halt"
    PROPOSE = "propose"
    IGNORE = "ignore"


class SchemaTypeAction(str, Enum):
    AUTO_WIDEN = "auto_widen"
    PROPOSE = "propose"
    HALT = "halt"


class SchemaNullableAction(str, Enum):
    AUTO_ACCEPT = "auto_accept"
    PROPOSE = "propose"
    HALT = "halt"


# ---------------------------------------------------------------------------
# Tier defaults
# ---------------------------------------------------------------------------

TIER_DEFAULTS = {
    1: {
        "freshness_warn_minutes": 15,
        "freshness_critical_minutes": 30,
        "freshness_check_interval_seconds": 60,
        "max_consecutive_failures": 1,
        "quality_warn_threshold": 0.995,
        "quality_critical_threshold": 0.99,
        "alert_channels": ["slack:urgent"],
        "escalation_after_minutes": 10,
        "digest_only": False,
        "retry_urgency": "immediate",
    },
    2: {
        "freshness_warn_minutes": 120,
        "freshness_critical_minutes": 360,
        "freshness_check_interval_seconds": 300,
        "max_consecutive_failures": 3,
        "quality_warn_threshold": 0.98,
        "quality_critical_threshold": 0.95,
        "alert_channels": ["slack:alerts", "email"],
        "escalation_after_minutes": 60,
        "digest_only": False,
        "retry_urgency": "standard",
    },
    3: {
        "freshness_warn_minutes": 1440,
        "freshness_critical_minutes": 4320,
        "freshness_check_interval_seconds": 3600,
        "max_consecutive_failures": 5,
        "quality_warn_threshold": 0.95,
        "quality_critical_threshold": 0.90,
        "alert_channels": ["email:digest"],
        "escalation_after_minutes": 1440,
        "digest_only": True,
        "retry_urgency": "lazy",
    },
}


# ---------------------------------------------------------------------------
# Schema change policy (per-pipeline, with tier-based defaults)
# ---------------------------------------------------------------------------

@dataclass
class SchemaChangePolicy:
    on_new_column: str = "auto_add"        # auto_add | propose | ignore
    on_dropped_column: str = "propose"     # halt | propose | ignore
    on_type_change: str = "propose"        # auto_widen | propose | halt
    on_nullable_change: str = "auto_accept"  # auto_accept | propose | halt
    propagate_to_downstream: bool = False


SCHEMA_POLICY_TIER_DEFAULTS = {
    1: SchemaChangePolicy(
        on_new_column="auto_add",
        on_dropped_column="halt",
        on_type_change="propose",
        on_nullable_change="propose",
        propagate_to_downstream=True,
    ),
    2: SchemaChangePolicy(
        on_new_column="auto_add",
        on_dropped_column="propose",
        on_type_change="auto_widen",
        on_nullable_change="auto_accept",
        propagate_to_downstream=True,
    ),
    3: SchemaChangePolicy(
        on_new_column="auto_add",
        on_dropped_column="ignore",
        on_type_change="auto_widen",
        on_nullable_change="auto_accept",
        propagate_to_downstream=False,
    ),
}


@dataclass
class PostPromotionHook:
    """SQL hook executed against the target after promotion completes."""
    hook_id: str = field(default_factory=new_id)
    name: str = ""
    sql: str = ""
    metadata_key: str = ""
    description: str = ""
    enabled: bool = True
    timeout_seconds: int = 30
    fail_pipeline_on_error: bool = False


# ---------------------------------------------------------------------------
# Sub-models (stored as JSON inside parent entities)
# ---------------------------------------------------------------------------

@dataclass
class ColumnMapping:
    source_column: str
    source_type: str
    target_column: str
    target_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_incremental_candidate: bool = False
    ordinal_position: int = 0


@dataclass
class QualityConfig:
    count_tolerance: float = 0.001
    sample_size: int = 1000
    sample_match_warn: float = 0.99
    sample_match_fail: float = 0.95
    null_rate_stddev_threshold: float = 2.0
    null_rate_catastrophic_jump: float = 0.45
    null_rate_max_anomalies_warn: int = 3
    cardinality_deviation_threshold: float = 0.5
    volume_z_score_warn: float = 2.0
    volume_z_score_fail: float = 3.0
    volume_baseline_runs: int = 30
    freshness_warn_multiplier: float = 2.0
    freshness_fail_multiplier: float = 5.0
    halt_on_first_fail: bool = True
    promote_on_warn: bool = True


@dataclass
class CheckResult:
    check_name: str
    status: CheckStatus
    detail: str
    metadata: dict = field(default_factory=dict)
    duration_ms: int = 0


# ---------------------------------------------------------------------------
# Primary entities
# ---------------------------------------------------------------------------

@dataclass
class ConnectorRecord:
    connector_id: str = field(default_factory=new_id)
    connector_name: str = ""
    connector_type: ConnectorType = ConnectorType.SOURCE
    source_target_type: str = ""
    version: int = 1
    generated_by: str = "seed"
    interface_version: str = "1.0"
    code: str = ""
    dependencies: list[str] = field(default_factory=list)
    test_status: TestStatus = TestStatus.UNTESTED
    test_results: dict = field(default_factory=dict)
    generation_attempts: int = 0
    generation_log: list[dict] = field(default_factory=list)
    status: ConnectorStatus = ConnectorStatus.DRAFT
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass
class PipelineContract:
    pipeline_id: str = field(default_factory=new_id)
    pipeline_name: str = ""
    version: int = 1
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    status: PipelineStatus = PipelineStatus.ACTIVE
    environment: str = "production"

    # Source
    source_connector_id: str = ""
    source_host: str = ""
    source_port: int = 0
    source_database: str = ""
    source_schema: str = ""
    source_table: str = ""
    source_user: str = ""
    source_password: str = ""

    # Target
    target_connector_id: str = ""
    target_host: str = ""
    target_port: int = 0
    target_database: str = ""
    target_user: str = ""
    target_password: str = ""
    target_schema: str = "raw"
    target_table: str = ""
    target_options: dict = field(default_factory=dict)

    # Strategy
    refresh_type: RefreshType = RefreshType.FULL
    replication_method: ReplicationMethod = ReplicationMethod.WATERMARK
    incremental_column: Optional[str] = None
    last_watermark: Optional[str] = None
    load_type: LoadType = LoadType.APPEND
    merge_keys: list[str] = field(default_factory=list)

    # Schedule
    schedule_cron: str = "0 * * * *"
    retry_max_attempts: int = 3
    retry_backoff_seconds: int = 60
    timeout_seconds: int = 3600

    # Schema
    column_mappings: list[ColumnMapping] = field(default_factory=list)
    target_ddl: str = ""

    # Quality
    quality_config: QualityConfig = field(default_factory=QualityConfig)

    # Staging
    staging_adapter: str = "local"

    # Observability
    tier: int = 2
    tier_config: dict = field(default_factory=dict)
    notification_policy_id: Optional[str] = None
    tags: dict = field(default_factory=dict)
    owner: Optional[str] = None
    freshness_column: Optional[str] = None

    # Agent reasoning
    agent_reasoning: dict = field(default_factory=dict)

    # Profiling baselines
    baseline_row_count: int = 0
    baseline_null_rates: dict = field(default_factory=dict)
    baseline_null_stddevs: dict = field(default_factory=dict)
    baseline_cardinality: dict = field(default_factory=dict)
    baseline_volume_avg: float = 0.0
    baseline_volume_stddev: float = 0.0

    # Approval settings
    auto_approve_additive_schema: bool = False
    approval_notification_channel: str = ""

    # Schema change policy (Build 12)
    schema_change_policy: Optional[SchemaChangePolicy] = None

    # Post-promotion SQL hooks (Build 13)
    post_promotion_hooks: list[PostPromotionHook] = field(default_factory=list)

    def get_schema_policy(self) -> SchemaChangePolicy:
        """Return the effective schema change policy (explicit > tier default)."""
        if self.schema_change_policy:
            return self.schema_change_policy
        return SCHEMA_POLICY_TIER_DEFAULTS.get(self.tier, SCHEMA_POLICY_TIER_DEFAULTS[2])

    def get_tier_config(self) -> dict:
        defaults = TIER_DEFAULTS.get(self.tier, TIER_DEFAULTS[2]).copy()
        defaults.update(self.tier_config)
        return defaults

    def get_freshness_col(self) -> Optional[str]:
        return self.freshness_column or self.incremental_column


@dataclass
class RunRecord:
    run_id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    started_at: str = field(default_factory=now_iso)
    completed_at: Optional[str] = None
    status: RunStatus = RunStatus.PENDING
    run_mode: RunMode = RunMode.SCHEDULED
    backfill_start: Optional[str] = None
    backfill_end: Optional[str] = None
    rows_extracted: int = 0
    rows_loaded: int = 0
    watermark_before: Optional[str] = None
    watermark_after: Optional[str] = None
    staging_path: str = ""
    staging_size_bytes: int = 0
    drift_detected: Optional[dict] = None
    quality_results: Optional[dict] = None
    gate_decision: Optional[GateDecision] = None
    error: Optional[str] = None
    retry_count: int = 0
    # Build 15: upstream trigger context
    triggered_by_run_id: Optional[str] = None
    triggered_by_pipeline_id: Optional[str] = None


@dataclass
class GateRecord:
    gate_id: str = field(default_factory=new_id)
    run_id: str = ""
    pipeline_id: str = ""
    decision: GateDecision = GateDecision.PROMOTE
    checks: list[CheckResult] = field(default_factory=list)
    agent_reasoning: Optional[str] = None
    evaluated_at: str = field(default_factory=now_iso)


@dataclass
class ContractChangeProposal:
    proposal_id: str = field(default_factory=new_id)
    pipeline_id: Optional[str] = None
    connector_id: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    resolved_at: Optional[str] = None
    status: ProposalStatus = ProposalStatus.PENDING
    trigger_type: TriggerType = TriggerType.USER_REQUEST
    trigger_detail: dict = field(default_factory=dict)
    change_type: ChangeType = ChangeType.ADD_COLUMN
    current_state: dict = field(default_factory=dict)
    proposed_state: dict = field(default_factory=dict)
    reasoning: str = ""
    confidence: float = 0.0
    impact_analysis: dict = field(default_factory=dict)
    rollback_plan: str = ""
    resolved_by: Optional[str] = None
    resolution_note: Optional[str] = None
    rejection_learning: Optional[dict] = None
    contract_version_before: int = 0
    contract_version_after: Optional[int] = None


@dataclass
class SchemaVersion:
    version_id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    version: int = 1
    column_mappings: list[ColumnMapping] = field(default_factory=list)
    change_summary: str = ""
    change_type: str = "initial"
    proposal_id: Optional[str] = None
    applied_at: str = field(default_factory=now_iso)
    applied_by: str = "agent"


@dataclass
class PipelineDependency:
    dependency_id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    depends_on_id: str = ""
    dependency_type: DependencyType = DependencyType.USER_DEFINED
    created_at: str = field(default_factory=now_iso)
    notes: Optional[str] = None


@dataclass
class NotificationPolicy:
    policy_id: str = field(default_factory=new_id)
    policy_name: str = ""
    description: Optional[str] = None
    channels: list[dict] = field(default_factory=list)
    digest_hour: int = 9
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


@dataclass
class FreshnessSnapshot:
    snapshot_id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    pipeline_name: str = ""
    tier: int = 2
    staleness_minutes: float = 0.0
    freshness_sla_minutes: int = 120
    sla_met: bool = True
    status: FreshnessStatus = FreshnessStatus.FRESH
    last_record_time: Optional[str] = None
    checked_at: str = field(default_factory=now_iso)


@dataclass
class AlertRecord:
    alert_id: str = field(default_factory=new_id)
    severity: AlertSeverity = AlertSeverity.INFO
    tier: int = 2
    pipeline_id: str = ""
    pipeline_name: str = ""
    summary: str = ""
    detail: dict = field(default_factory=dict)
    created_at: str = field(default_factory=now_iso)
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None
    digested: bool = False


@dataclass
class DecisionLog:
    id: Optional[int] = None
    pipeline_id: Optional[str] = None
    connector_id: Optional[str] = None
    decision_type: str = ""
    detail: str = ""
    reasoning: str = ""
    created_at: str = field(default_factory=now_iso)


@dataclass
class AgentPreference:
    preference_id: str = field(default_factory=new_id)
    scope: PreferenceScope = PreferenceScope.GLOBAL
    scope_value: Optional[str] = None
    preference_key: str = ""
    preference_value: dict = field(default_factory=dict)
    source: PreferenceSource = PreferenceSource.USER_EXPLICIT
    confidence: float = 1.0
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    usage_count: int = 0
    embedding: list = field(default_factory=list)
    last_used: str = ""


# ---------------------------------------------------------------------------
# New entities (PostgreSQL migration)
# ---------------------------------------------------------------------------

@dataclass
class PipelineMetadata:
    id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    namespace: str = "default"
    key: str = ""
    value_json: dict = field(default_factory=dict)
    updated_at: str = field(default_factory=now_iso)
    created_by_run_id: Optional[str] = None


@dataclass
class ErrorBudget:
    pipeline_id: str = ""
    window_days: int = 7
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    success_rate: float = 1.0
    budget_threshold: float = 0.9
    budget_remaining: float = 1.0
    escalated: bool = False
    last_calculated: str = field(default_factory=now_iso)


@dataclass
class ColumnLineage:
    id: str = field(default_factory=new_id)
    source_pipeline_id: str = ""
    source_schema: str = ""
    source_table: str = ""
    source_column: str = ""
    target_pipeline_id: str = ""
    target_schema: str = ""
    target_table: str = ""
    target_column: str = ""
    transformation: str = "direct"
    created_at: str = field(default_factory=now_iso)


@dataclass
class AgentCostLog:
    id: str = field(default_factory=new_id)
    pipeline_id: str = ""
    operation: str = ""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    latency_ms: int = 0
    timestamp: str = field(default_factory=now_iso)


@dataclass
class ConnectorMigration:
    id: str = field(default_factory=new_id)
    connector_id: str = ""
    from_version: int = 0
    to_version: int = 0
    affected_pipelines: list = field(default_factory=list)
    migration_status: str = "pending"
    migration_log: str = ""
    created_at: str = field(default_factory=now_iso)
    completed_at: str = ""


@dataclass
class User:
    id: str = field(default_factory=new_id)
    username: str = ""
    password_hash: str = ""
    role: str = "viewer"  # admin / operator / viewer
    email: str = ""
    created_at: str = field(default_factory=now_iso)
    last_login: str = ""

    @property
    def user_id(self) -> str:
        return self.id


# ---------------------------------------------------------------------------
# Discovery / profiling results (not persisted directly -- used in-memory)
# ---------------------------------------------------------------------------

@dataclass
class ConnectionResult:
    success: bool
    version: str = ""
    ssl_enabled: bool = False
    connection_count: int = 0
    latency_ms: int = 0
    error: Optional[str] = None


@dataclass
class SchemaInfo:
    schema_name: str
    table_count: int
    tables: list[str] = field(default_factory=list)


@dataclass
class TableProfile:
    schema_name: str
    table_name: str
    row_count_estimate: int
    column_count: int
    columns: list[ColumnMapping] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    timestamp_columns: list[str] = field(default_factory=list)
    null_rates: dict = field(default_factory=dict)
    cardinality: dict = field(default_factory=dict)
    sample_rows: list[dict] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)


@dataclass
class ExtractResult:
    rows_extracted: int
    max_watermark: Optional[str]
    staging_path: str
    staging_size_bytes: int
    batch_count: int
    manifest: dict = field(default_factory=dict)
