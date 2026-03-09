"""
YAML codec for PipelineContract serialization and deserialization.

Converts between PipelineContract dataclasses and human-readable YAML
with logical field grouping (source/target/strategy/schedule/quality).

Pure functions — no store or async dependency.
"""
from __future__ import annotations

from dataclasses import asdict
from typing import Optional

import yaml

from contracts.models import (
    PipelineContract, ColumnMapping, QualityConfig,
    PipelineStatus, RefreshType, ReplicationMethod, LoadType,
    new_id, now_iso,
)

# Fields whose values should be masked on export
_CREDENTIAL_FIELDS = {"password"}


# ---------------------------------------------------------------------------
# Export: PipelineContract → dict / YAML
# ---------------------------------------------------------------------------

def pipeline_to_dict(
    contract: PipelineContract,
    mask_credentials: bool = True,
) -> dict:
    """Convert a PipelineContract to a nested dict for YAML serialization.

    Groups fields logically (source, target, strategy, schedule, etc.)
    rather than using the flat dataclass layout.
    """
    def _enum_val(v):
        return v.value if hasattr(v, "value") else v

    def _cap(v):
        """Cap float to avoid YAML infinity issues."""
        if isinstance(v, float):
            return min(v, 99999.0)
        return v

    src_password = "***" if mask_credentials else contract.source_password
    tgt_password = "***" if mask_credentials else contract.target_password

    d = {
        "pipeline_name": contract.pipeline_name,
        "environment": contract.environment,
        "status": _enum_val(contract.status),
        "tier": contract.tier,
        "owner": contract.owner,
        "tags": contract.tags or {},

        "source": {
            "connector_id": contract.source_connector_id,
            "host": contract.source_host,
            "port": contract.source_port,
            "database": contract.source_database,
            "schema": contract.source_schema,
            "table": contract.source_table,
            "user": contract.source_user,
            "password": src_password,
        },

        "target": {
            "connector_id": contract.target_connector_id,
            "host": contract.target_host,
            "port": contract.target_port,
            "database": contract.target_database,
            "schema": contract.target_schema,
            "table": contract.target_table,
            "user": contract.target_user,
            "password": tgt_password,
            "options": contract.target_options or {},
            "ddl": contract.target_ddl or "",
        },

        "strategy": {
            "refresh_type": _enum_val(contract.refresh_type),
            "replication_method": _enum_val(contract.replication_method),
            "incremental_column": contract.incremental_column,
            "load_type": _enum_val(contract.load_type),
            "merge_keys": contract.merge_keys or [],
        },

        "schedule": {
            "cron": contract.schedule_cron,
            "retry_max_attempts": contract.retry_max_attempts,
            "retry_backoff_seconds": contract.retry_backoff_seconds,
            "timeout_seconds": contract.timeout_seconds,
        },

        "columns": [asdict(m) for m in (contract.column_mappings or [])],

        "quality": asdict(contract.quality_config) if contract.quality_config else {},

        "approval": {
            "auto_approve_additive_schema": contract.auto_approve_additive_schema,
            "notification_channel": contract.approval_notification_channel or "",
        },

        "staging_adapter": contract.staging_adapter,
        "freshness_column": contract.freshness_column,
        "notification_policy_id": contract.notification_policy_id,
        "tier_config": contract.tier_config or {},
        "agent_reasoning": contract.agent_reasoning or {},

        "_metadata": {
            "pipeline_id": contract.pipeline_id,
            "version": contract.version,
            "created_at": contract.created_at,
            "updated_at": contract.updated_at,
        },

        "_state": {
            "last_watermark": contract.last_watermark,
            "baseline_row_count": contract.baseline_row_count,
            "baselines": {
                "volume_avg": _cap(contract.baseline_volume_avg),
                "volume_stddev": _cap(contract.baseline_volume_stddev),
                "null_rates": contract.baseline_null_rates or {},
                "null_stddevs": contract.baseline_null_stddevs or {},
                "cardinality": contract.baseline_cardinality or {},
            },
        },
    }

    return d


def pipeline_to_yaml(
    contract: PipelineContract,
    mask_credentials: bool = True,
) -> str:
    """Serialize a PipelineContract to a YAML string."""
    d = pipeline_to_dict(contract, mask_credentials=mask_credentials)
    return yaml.dump(d, default_flow_style=False, sort_keys=False, allow_unicode=True)


def pipelines_to_yaml(
    contracts: list[PipelineContract],
    mask_credentials: bool = True,
) -> str:
    """Serialize multiple PipelineContracts to a multi-document YAML string."""
    docs = [pipeline_to_dict(c, mask_credentials=mask_credentials) for c in contracts]
    return yaml.dump_all(
        docs, default_flow_style=False, sort_keys=False, allow_unicode=True,
    )


# ---------------------------------------------------------------------------
# Import: dict / YAML → PipelineContract
# ---------------------------------------------------------------------------

def dict_to_pipeline(
    data: dict,
    preserve_id: bool = False,
) -> PipelineContract:
    """Reconstruct a PipelineContract from a structured YAML dict.

    Args:
        data: Nested dict as parsed from YAML.
        preserve_id: If True, use pipeline_id from _metadata. If False, generate new.
    """
    src = data.get("source", {})
    tgt = data.get("target", {})
    strat = data.get("strategy", {})
    sched = data.get("schedule", {})
    qual = data.get("quality", {})
    appr = data.get("approval", {})
    meta = data.get("_metadata", {})
    state = data.get("_state", {})
    baselines = state.get("baselines", {})

    # Reconstruct column mappings
    raw_cols = data.get("columns", [])
    column_mappings = [ColumnMapping(**m) for m in raw_cols]

    # Reconstruct quality config
    quality_config = QualityConfig(**qual) if qual else QualityConfig()

    # Determine pipeline_id
    if preserve_id and meta.get("pipeline_id"):
        pipeline_id = meta["pipeline_id"]
    else:
        pipeline_id = new_id()

    # Handle masked credentials — leave as empty string
    src_password = src.get("password", "")
    if src_password == "***":
        src_password = ""
    tgt_password = tgt.get("password", "")
    if tgt_password == "***":
        tgt_password = ""

    return PipelineContract(
        pipeline_id=pipeline_id,
        pipeline_name=data.get("pipeline_name", ""),
        version=meta.get("version", 1),
        created_at=meta.get("created_at", now_iso()),
        updated_at=meta.get("updated_at", now_iso()),
        status=_to_enum(PipelineStatus, data.get("status", "active")),
        environment=data.get("environment", "production"),

        # Source
        source_connector_id=src.get("connector_id", ""),
        source_host=src.get("host", ""),
        source_port=src.get("port", 0),
        source_database=src.get("database", ""),
        source_schema=src.get("schema", ""),
        source_table=src.get("table", ""),
        source_user=src.get("user", ""),
        source_password=src_password,

        # Target
        target_connector_id=tgt.get("connector_id", ""),
        target_host=tgt.get("host", ""),
        target_port=tgt.get("port", 0),
        target_database=tgt.get("database", ""),
        target_user=tgt.get("user", ""),
        target_password=tgt_password,
        target_schema=tgt.get("schema", "raw"),
        target_table=tgt.get("table", ""),
        target_options=tgt.get("options", {}),
        target_ddl=tgt.get("ddl", ""),

        # Strategy
        refresh_type=_to_enum(RefreshType, strat.get("refresh_type", "full")),
        replication_method=_to_enum(
            ReplicationMethod, strat.get("replication_method", "watermark"),
        ),
        incremental_column=strat.get("incremental_column"),
        load_type=_to_enum(LoadType, strat.get("load_type", "append")),
        merge_keys=strat.get("merge_keys", []),

        # Schedule
        schedule_cron=sched.get("cron", "0 * * * *"),
        retry_max_attempts=sched.get("retry_max_attempts", 3),
        retry_backoff_seconds=sched.get("retry_backoff_seconds", 60),
        timeout_seconds=sched.get("timeout_seconds", 3600),

        # Schema
        column_mappings=column_mappings,

        # Quality
        quality_config=quality_config,

        # Staging
        staging_adapter=data.get("staging_adapter", "local"),

        # Observability
        tier=data.get("tier", 2),
        tier_config=data.get("tier_config", {}),
        notification_policy_id=data.get("notification_policy_id"),
        tags=data.get("tags", {}),
        owner=data.get("owner"),
        freshness_column=data.get("freshness_column"),

        # Agent
        agent_reasoning=data.get("agent_reasoning", {}),

        # Baselines (from _state)
        last_watermark=state.get("last_watermark"),
        baseline_row_count=state.get("baseline_row_count", 0),
        baseline_null_rates=baselines.get("null_rates", {}),
        baseline_null_stddevs=baselines.get("null_stddevs", {}),
        baseline_cardinality=baselines.get("cardinality", {}),
        baseline_volume_avg=baselines.get("volume_avg", 0.0),
        baseline_volume_stddev=baselines.get("volume_stddev", 0.0),

        # Approval
        auto_approve_additive_schema=appr.get("auto_approve_additive_schema", False),
        approval_notification_channel=appr.get("notification_channel", ""),
    )


def yaml_to_pipeline(
    yaml_str: str,
    preserve_id: bool = False,
) -> PipelineContract:
    """Deserialize a YAML string to a PipelineContract."""
    data = yaml.safe_load(yaml_str)
    if not data:
        raise ValueError("Empty YAML document")
    return dict_to_pipeline(data, preserve_id=preserve_id)


def yaml_to_pipelines(
    yaml_str: str,
    preserve_id: bool = False,
) -> list[PipelineContract]:
    """Deserialize multi-document YAML to a list of PipelineContracts."""
    docs = list(yaml.safe_load_all(yaml_str))
    return [dict_to_pipeline(d, preserve_id=preserve_id) for d in docs if d]


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

def diff_contracts(
    current: PipelineContract,
    incoming: PipelineContract,
) -> dict:
    """Compare two PipelineContracts and return field-level diffs.

    Skips _state (runtime baselines/watermarks) and _metadata
    (pipeline_id, created_at, updated_at, version).

    Returns:
        Dict with {field_path: {current: ..., incoming: ...}} for changed fields.
        Empty dict means contracts are equivalent.
    """
    cur_d = pipeline_to_dict(current, mask_credentials=False)
    inc_d = pipeline_to_dict(incoming, mask_credentials=False)

    # Remove sections we don't compare
    for skip in ("_state", "_metadata"):
        cur_d.pop(skip, None)
        inc_d.pop(skip, None)

    diffs = {}
    _diff_recursive(cur_d, inc_d, "", diffs)
    return diffs


def _diff_recursive(cur, inc, prefix: str, diffs: dict) -> None:
    """Walk two nested dicts/lists and record differences."""
    if isinstance(cur, dict) and isinstance(inc, dict):
        all_keys = set(cur.keys()) | set(inc.keys())
        for k in sorted(all_keys):
            path = f"{prefix}.{k}" if prefix else k
            if k not in cur:
                diffs[path] = {"current": None, "incoming": inc[k]}
            elif k not in inc:
                diffs[path] = {"current": cur[k], "incoming": None}
            else:
                _diff_recursive(cur[k], inc[k], path, diffs)
    elif isinstance(cur, list) and isinstance(inc, list):
        if cur != inc:
            diffs[prefix] = {"current": cur, "incoming": inc}
    else:
        if cur != inc:
            diffs[prefix] = {"current": cur, "incoming": inc}


# ---------------------------------------------------------------------------
# State snapshot
# ---------------------------------------------------------------------------

def snapshot_state(
    contract: PipelineContract,
    error_budget: Optional[dict] = None,
    dependencies: Optional[list[dict]] = None,
    schema_versions: Optional[list[dict]] = None,
) -> dict:
    """Build a _state snapshot dict from runtime data.

    The caller (API layer) fetches data from the store and passes it in.
    """
    state = {
        "last_watermark": contract.last_watermark,
        "baseline_row_count": contract.baseline_row_count,
        "baselines": {
            "volume_avg": min(contract.baseline_volume_avg, 99999.0),
            "volume_stddev": min(contract.baseline_volume_stddev, 99999.0),
            "null_rates": contract.baseline_null_rates or {},
            "null_stddevs": contract.baseline_null_stddevs or {},
            "cardinality": contract.baseline_cardinality or {},
        },
    }
    if error_budget:
        state["error_budget"] = error_budget
    if dependencies:
        state["dependencies"] = dependencies
    if schema_versions:
        state["schema_versions"] = schema_versions
    return state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_enum(enum_cls, value):
    """Safely convert a string to an enum, handling already-enum values."""
    if isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        return enum_cls(value.lower())
    return enum_cls(value)
