"""
Agent core -- LLM reasoning engine with cost tracking, embedding support,
and rule-based fallbacks for every operation.
"""
from __future__ import annotations

import inspect as _inspect
import json
import logging
import re
import time
from typing import Optional

import httpx

from config import Config
from contracts.models import (
    TableProfile, PipelineContract, RunRecord, CheckResult, AgentPreference,
    ContractChangeProposal, ConnectorRecord, AlertRecord, AgentCostLog,
    ConnectorType, ConnectorStatus, TestStatus, ChangeType, TriggerType,
    ProposalStatus, PreferenceScope, PreferenceSource,
    RunStatus, GateDecision,
    new_id, now_iso,
)
from contracts.store import Store
from sandbox import validate_connector_code

log = logging.getLogger(__name__)


class AgentCore:
    """LLM reasoning engine with cost tracking and rule-based fallbacks."""

    def __init__(self, config: Config, store: Store):
        self.config = config
        self.store = store
        self.has_api = config.has_api_key
        # Per-request token accumulator (reset before each command)
        self._req_input_tokens = 0
        self._req_output_tokens = 0

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _system_prompt(self) -> str:
        return (
            "You are an expert data engineer with deep knowledge of ETL patterns, "
            "database internals, data quality, and pipeline design. "
            "You are embedded in Pipeline Agent, an autonomous data pipeline platform. "
            "Your decisions are stored as queryable knowledge and used to guide future runs. "
            "Be precise, specific, and conservative -- data pipelines must be reliable above all else. "
            "Always respond with valid JSON unless instructed otherwise."
        )

    async def _call_claude(
        self,
        system: str,
        user_msg: str,
        pipeline_id: str = "",
        operation: str = "",
        temperature: float = 0.1,
    ) -> str:
        """Call Claude API, track token usage via AgentCostLog, return content text."""
        if not self.has_api:
            raise RuntimeError("No API key configured")

        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.config.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": self.config.model,
                    "max_tokens": 4096,
                    "temperature": temperature,
                    "system": system,
                    "messages": [{"role": "user", "content": user_msg}],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        latency_ms = int((time.monotonic() - t0) * 1000)
        content_text = data["content"][0]["text"]

        # Extract token usage from response
        usage = data.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        total_tokens = input_tokens + output_tokens

        # Accumulate for per-request tracking
        self._req_input_tokens += input_tokens
        self._req_output_tokens += output_tokens

        # Log cost
        cost_log = AgentCostLog(
            pipeline_id=pipeline_id,
            operation=operation,
            model=self.config.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            latency_ms=latency_ms,
        )
        try:
            await self.store.save_agent_cost(cost_log)
        except Exception as exc:
            log.warning("Failed to save agent cost log: %s", exc)

        log.debug(
            "Claude call: op=%s tokens=%d latency=%dms",
            operation, total_tokens, latency_ms,
        )
        return content_text

    async def _embed(self, text: str) -> list[float]:
        """Call Voyage API for text embedding. Returns empty list if no key."""
        if not self.config.has_embeddings:
            return []
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.voyageai.com/v1/embeddings",
                    headers={
                        "Authorization": f"Bearer {self.config.voyage_api_key}",
                        "content-type": "application/json",
                    },
                    json={
                        "model": self.config.embedding_model,
                        "input": [text],
                        "input_type": "document",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                return data["data"][0]["embedding"]
        except Exception as exc:
            log.warning("Voyage embedding error: %s", exc)
            return []

    def _extract_json(self, text: str) -> dict:
        """Extract the first JSON object from a Claude response."""
        text = text.strip()
        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        # Find JSON block in markdown fences
        fence_match = re.search(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", text)
        if fence_match:
            try:
                return json.loads(fence_match.group(1))
            except json.JSONDecodeError:
                pass
        # Find any JSON object
        match = re.search(r"\{[\s\S]+\}", text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"No JSON found in response: {text[:200]}")

    # ------------------------------------------------------------------
    # propose_strategy
    # ------------------------------------------------------------------

    async def propose_strategy(
        self,
        profile: TableProfile,
        preferences: Optional[list[AgentPreference]] = None,
    ) -> dict:
        """Propose an optimal ingestion strategy for a table.

        Uses Claude when available, falls back to rule-based heuristics.
        """
        if not self.has_api:
            return self._rule_based_strategy(profile)

        prefs_text = ""
        if preferences:
            prefs_text = "\n\nLearned preferences to apply:\n" + "\n".join(
                f"- {p.preference_key}: {json.dumps(p.preference_value)} "
                f"(source: {p.source.value}, confidence: {p.confidence:.0%})"
                for p in preferences
            )

        user_prompt = f"""
Analyze this table profile and propose an optimal ingestion strategy.

Table: {profile.schema_name}.{profile.table_name}
Estimated rows: {profile.row_count_estimate:,}
Columns: {profile.column_count}
Primary keys: {profile.primary_keys}
Timestamp columns: {profile.timestamp_columns}
Null rates (sample): {json.dumps(profile.null_rates, indent=2)}
Cardinality (sample): {json.dumps(profile.cardinality, indent=2)}
Foreign keys: {profile.foreign_keys}
{prefs_text}

Respond with a JSON object containing exactly these keys:
{{
  "refresh_type": "full" or "incremental",
  "replication_method": "watermark" (only option for now -- CDC and snapshot are stubs),
  "incremental_column": "column_name" or null,
  "load_type": "append" or "merge",
  "merge_keys": ["col1"] or [],
  "target_options": {{"sort_key": "col" or null, "dist_key": "col" or null}},
  "tier": 1, 2, or 3,
  "cost_estimate": {{
    "rows_per_run_estimate": integer,
    "strategy_cost_note": "plain English note about cost implications"
  }},
  "reasoning": {{
    "refresh_type_reason": "...",
    "replication_method_reason": "...",
    "load_type_reason": "...",
    "merge_keys_reason": "...",
    "tier_reason": "...",
    "incremental_column_reason": "..."
  }}
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                operation="propose_strategy",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("Claude API error in propose_strategy: %s. Using fallback.", e)
            return self._rule_based_strategy(profile)

    def _rule_based_strategy(self, profile: TableProfile) -> dict:
        rows = profile.row_count_estimate
        ts_cols = profile.timestamp_columns
        pks = profile.primary_keys

        # Prefer updated_at > modified_at > created_at > first timestamp
        preferred_order = ["updated_at", "modified_at", "updated", "modified", "created_at"]
        inc_col = next(
            (c for c in preferred_order if c in ts_cols),
            ts_cols[0] if ts_cols else None,
        )

        refresh_type = "incremental" if (rows > 10_000 and ts_cols) else "full"
        load_type = "merge" if pks else "append"
        merge_keys = pks[:1] if pks else []
        tier = 1 if rows > 10_000_000 else (2 if rows > 100_000 else 3)

        sort_key = inc_col
        dist_key = merge_keys[0] if merge_keys else None

        return {
            "refresh_type": refresh_type,
            "replication_method": "watermark",
            "incremental_column": inc_col,
            "load_type": load_type,
            "merge_keys": merge_keys,
            "target_options": {"sort_key": sort_key, "dist_key": dist_key},
            "tier": tier,
            "cost_estimate": {
                "rows_per_run_estimate": rows if refresh_type == "full" else int(rows * 0.01),
                "strategy_cost_note": "Rule-based fallback. Cost estimation unavailable.",
            },
            "reasoning": {
                "refresh_type_reason": (
                    f"{'Incremental' if refresh_type == 'incremental' else 'Full refresh'}: "
                    f"table has {'timestamp columns' if ts_cols else 'no timestamp columns'} "
                    f"and {rows:,} estimated rows."
                ),
                "replication_method_reason": "Watermark polling -- rule-based fallback.",
                "load_type_reason": (
                    f"{'Merge' if pks else 'Append'}: "
                    f"{'primary keys found' if pks else 'no primary keys detected'}."
                ),
                "merge_keys_reason": (
                    f"Using primary key: {merge_keys}" if merge_keys else "No merge keys."
                ),
                "tier_reason": f"Tier {tier} based on row count ({rows:,}).",
                "incremental_column_reason": (
                    f"Selected {inc_col} as watermark column." if inc_col
                    else "No timestamp column available."
                ),
            },
        }

    # ------------------------------------------------------------------
    # design_topology (Build 20)
    # ------------------------------------------------------------------

    async def design_topology(
        self,
        description: str,
        existing_pipelines: Optional[list[dict]] = None,
        existing_connectors: Optional[list[dict]] = None,
    ) -> dict:
        """Design a multi-pipeline architecture from a natural language description.

        Returns a structured topology proposal with pipelines, dependencies,
        data contracts, and scheduling recommendations.
        """
        if not self.has_api:
            return {
                "error": "Topology reasoning requires an API key.",
                "pipelines": [],
                "dependencies": [],
                "contracts": [],
            }

        existing_text = ""
        if existing_pipelines:
            lines = [f"  - {p.get('pipeline_name', 'unknown')}: {p.get('source', '?')} -> {p.get('target', '?')} ({p.get('status', '?')})" for p in existing_pipelines[:20]]
            existing_text += "\n\nExisting pipelines:\n" + "\n".join(lines)
        if existing_connectors:
            lines = [f"  - {c.get('connector_name', 'unknown')} ({c.get('connector_type', '?')}, {c.get('source_target_type', '?')})" for c in existing_connectors[:20]]
            existing_text += "\n\nAvailable connectors:\n" + "\n".join(lines)

        user_prompt = f"""
You are designing a multi-pipeline data architecture for DAPOS (an agentic data platform).
The user has described what they need. Design the optimal pipeline topology.

User request: "{description}"
{existing_text}

Supported patterns:
- Consume & merge: Stage -> upsert -> cleanup consumed rows
- Fan-in: Multiple sources -> unified table
- Fan-out: One source -> multiple targets
- SCD Type 2: Historical change tracking
- Quarantine: Bad rows -> error table, good rows -> production
- Cascading aggregation: Raw -> daily -> monthly -> dashboard
- Conditional routing: Branch on quality/volume thresholds
- Replay/reprocess: Re-run a time window idempotently

Design the topology and respond with JSON:
{{
  "summary": "One paragraph explaining the architecture",
  "pattern": "primary pattern name (e.g., fan-in, consume-and-merge)",
  "pipelines": [
    {{
      "name": "pipeline-name",
      "description": "what this pipeline does",
      "source_type": "source connector type (e.g., mysql, stripe, s3)",
      "source_detail": "database.schema.table or API resource",
      "target_type": "target connector type (e.g., postgresql, snowflake)",
      "target_detail": "schema.table",
      "refresh_type": "full or incremental",
      "load_type": "append or merge",
      "schedule_cron": "cron expression",
      "tier": 1-3,
      "merge_keys": ["key1"] or [],
      "incremental_column": "column_name or null",
      "hooks": ["optional post-promotion SQL descriptions"]
    }}
  ],
  "dependencies": [
    {{
      "from": "upstream-pipeline-name",
      "to": "downstream-pipeline-name",
      "type": "data_triggered or scheduled",
      "notes": "why this dependency exists"
    }}
  ],
  "contracts": [
    {{
      "producer": "producer-pipeline-name",
      "consumer": "consumer-pipeline-name",
      "freshness_sla_minutes": 60,
      "required_columns": ["col1", "col2"],
      "cleanup_ownership": "consumer_acknowledges or producer_ttl or none"
    }}
  ],
  "reasoning": "Detailed explanation of design decisions"
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                operation="design_topology",
                temperature=0.3,
            )
            result = self._extract_json(text)
            result.setdefault("pipelines", [])
            result.setdefault("dependencies", [])
            result.setdefault("contracts", [])
            result.setdefault("summary", "")
            result.setdefault("pattern", "custom")
            return result
        except Exception as e:
            log.warning("Claude API error in design_topology: %s", e)
            return {
                "error": str(e),
                "pipelines": [],
                "dependencies": [],
                "contracts": [],
                "summary": "Failed to generate topology. Please try again.",
            }

    # ------------------------------------------------------------------
    # analyze_drift
    # ------------------------------------------------------------------

    async def analyze_drift(
        self,
        contract: PipelineContract,
        drift_info: dict,
        preferences: Optional[list[AgentPreference]] = None,
    ) -> dict:
        """Evaluate schema drift and recommend action.

        Returns dict with action, confidence, breaking_change,
        data_loss_risk, rollback_plan.
        """
        if not self.has_api:
            return self._rule_based_drift(drift_info)

        prefs_text = ""
        if preferences:
            prefs_text = "\n\nLearned preferences:\n" + "\n".join(
                f"- {p.preference_key}: {json.dumps(p.preference_value)}"
                for p in preferences
            )

        user_prompt = f"""
Schema drift detected for pipeline: {contract.pipeline_name}
Source: {contract.source_schema}.{contract.source_table}
Current version: {contract.version}

Drift details:
{json.dumps(drift_info, indent=2)}
{prefs_text}

Evaluate this drift and respond with JSON:
{{
  "action": "auto_adapt" | "propose_change" | "halt",
  "confidence": 0.0-1.0,
  "reasoning": "explanation",
  "breaking_change": true/false,
  "data_loss_risk": "none" | "low" | "medium" | "high",
  "rollback_plan": "how to revert if applied",
  "estimated_backfill_time": "human-readable estimate or null"
}}

Use "auto_adapt" only for new nullable columns.
Use "propose_change" for type changes or non-nullable new columns.
Use "halt" for dropped columns or type narrowing (e.g. BIGINT -> INT).
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="analyze_drift",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("Claude API error in analyze_drift: %s. Using fallback.", e)
            return self._rule_based_drift(drift_info)

    def _rule_based_drift(self, drift_info: dict) -> dict:
        new_cols = drift_info.get("new_columns", [])
        dropped_cols = drift_info.get("dropped_columns", [])
        type_changes = drift_info.get("type_changes", [])

        if dropped_cols or type_changes:
            return {
                "action": "halt",
                "confidence": 0.9,
                "reasoning": (
                    f"Breaking changes detected: dropped={dropped_cols}, "
                    f"type_changes={type_changes}"
                ),
                "breaking_change": True,
                "data_loss_risk": "high",
                "rollback_plan": "Revert contract to previous version.",
                "estimated_backfill_time": None,
            }

        all_nullable = all(c.get("nullable", True) for c in new_cols)
        if new_cols and all_nullable:
            return {
                "action": "auto_adapt",
                "confidence": 0.95,
                "reasoning": (
                    f"New nullable columns detected: {[c['name'] for c in new_cols]}. "
                    "Safe to auto-apply."
                ),
                "breaking_change": False,
                "data_loss_risk": "none",
                "rollback_plan": "Drop the new columns from the target table.",
                "estimated_backfill_time": None,
            }

        return {
            "action": "propose_change",
            "confidence": 0.8,
            "reasoning": "Schema changes detected. Requires review.",
            "breaking_change": False,
            "data_loss_risk": "low",
            "rollback_plan": "Revert contract column mappings to previous version.",
            "estimated_backfill_time": None,
        }

    # ------------------------------------------------------------------
    # generate_migration_sql — agent-generated DDL for schema drift
    # ------------------------------------------------------------------

    async def generate_migration_sql(
        self,
        contract: PipelineContract,
        drift_info: dict,
        target_type: str = "postgresql",
    ) -> dict:
        """Have the agent generate migration SQL for detected schema drift.

        Returns dict with:
            migration_sql: list of SQL statements to execute
            reasoning: why these changes are needed
            risk_assessment: what could go wrong
            rollback_sql: list of SQL statements to undo the migration
        """
        schema = contract.target_schema or "raw"
        table = contract.target_table

        if not self.has_api:
            return self._rule_based_migration_sql(contract, drift_info, target_type)

        user_prompt = f"""Generate migration SQL for schema drift on pipeline "{contract.pipeline_name}".

Target database: {target_type}
Target table: "{schema}"."{table}"

Current column mappings:
{json.dumps([{{"source": m.source_column, "target": m.target_column, "type": m.target_type, "nullable": m.is_nullable}} for m in contract.column_mappings], indent=2)}

Detected drift:
{json.dumps(drift_info, indent=2)}

Generate the exact SQL to align the target table with the source schema.
Respond with JSON:
{{
  "migration_sql": ["ALTER TABLE ...", ...],
  "reasoning": "why each statement is needed",
  "risk_assessment": "what could go wrong and impact on existing data",
  "rollback_sql": ["ALTER TABLE ...", ...]
}}

Rules:
- Use IF NOT EXISTS / IF EXISTS where supported
- For new columns, use the target_type from drift info
- For type changes, only widen (never narrow) — e.g. INT -> BIGINT is safe
- For dropped columns, generate DROP COLUMN IF EXISTS
- Preserve existing data — never DROP and recreate
- Quote all identifiers with double quotes
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="generate_migration_sql",
            )
            result = self._extract_json(text)
            # Validate that we got SQL statements back
            if not result.get("migration_sql"):
                log.warning("Agent returned empty migration_sql, falling back to rule-based")
                return self._rule_based_migration_sql(contract, drift_info, target_type)
            return result
        except Exception as e:
            log.warning("Claude API error in generate_migration_sql: %s. Using fallback.", e)
            return self._rule_based_migration_sql(contract, drift_info, target_type)

    def _rule_based_migration_sql(
        self,
        contract: PipelineContract,
        drift_info: dict,
        target_type: str = "postgresql",
    ) -> dict:
        """Fallback: generate migration SQL without LLM when API is unavailable."""
        schema = contract.target_schema or "raw"
        table = contract.target_table
        migration_sql = []
        rollback_sql = []
        reasons = []

        for col in drift_info.get("new_columns", []):
            col_name = col.get("name", "")
            col_type = col.get("target_type", col.get("type", "TEXT"))
            migration_sql.append(
                f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}'
            )
            rollback_sql.append(
                f'ALTER TABLE "{schema}"."{table}" DROP COLUMN IF EXISTS "{col_name}"'
            )
            reasons.append(f"Add new column '{col_name}' ({col_type}) from source")

        for tc in drift_info.get("type_changes", []):
            col_name = tc.get("column", "")
            new_type = tc.get("to", "")
            if col_name and new_type:
                migration_sql.append(
                    f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN "{col_name}" TYPE {new_type}'
                )
                rollback_sql.append(
                    f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN "{col_name}" TYPE {tc.get("from", "TEXT")}'
                )
                reasons.append(f"Widen column '{col_name}' from {tc.get('from', '?')} to {new_type}")

        for col in drift_info.get("dropped_columns", []):
            col_name = col if isinstance(col, str) else col.get("name", "")
            migration_sql.append(
                f'ALTER TABLE "{schema}"."{table}" DROP COLUMN IF EXISTS "{col_name}"'
            )
            rollback_sql.append(f"-- Cannot auto-rollback dropped column '{col_name}' (data lost)")
            reasons.append(f"Drop column '{col_name}' (no longer in source)")

        return {
            "migration_sql": migration_sql,
            "reasoning": "; ".join(reasons) if reasons else "No changes needed",
            "risk_assessment": "Rule-based generation — review SQL before approval",
            "rollback_sql": rollback_sql,
        }

    # ------------------------------------------------------------------
    # Agentic quality gate decision (Tier 1A)
    # ------------------------------------------------------------------

    async def decide_quality_gate(
        self,
        contract: PipelineContract,
        checks: list[CheckResult],
        is_first_run: bool = False,
    ) -> dict:
        """Agent decides PROMOTE / PROMOTE_WITH_WARNING / HALT based on check results.

        The 7 quality checks provide signals; the agent makes the decision with
        contextual reasoning about whether failures are acceptable.

        Returns dict with:
            decision: "promote" | "promote_with_warning" | "halt"
            reasoning: why this decision was made
            root_cause: what likely caused failures/warnings
            recommended_action: what the team should do
            risk_assessment: risk of promoting despite warnings
        """
        checks_summary = "\n".join(
            f"- {c.check_name}: {c.status.value} — {c.detail}"
            for c in checks
        )
        failed = [c for c in checks if c.status.value == "fail"]
        warned = [c for c in checks if c.status.value == "warn"]

        if not self.has_api:
            return self._rule_based_gate_decision(contract, checks, is_first_run)

        user_prompt = f"""You are the quality gate decision maker for pipeline "{contract.pipeline_name}".

Pipeline context:
- Tier: {contract.tier} (1=critical, 2=standard, 3=development)
- Refresh type: {contract.refresh_type.value if hasattr(contract.refresh_type, 'value') else contract.refresh_type}
- Is first run: {is_first_run}
- Baseline row count: {contract.baseline_row_count or 'none (new pipeline)'}

Quality check results:
{checks_summary}

Based on these results, decide whether to:
- **promote**: All checks pass, data is safe to use
- **promote_with_warning**: Minor issues detected but data is usable (e.g., small volume variance, slight null rate increase on non-critical columns)
- **halt**: Serious quality issues that would corrupt downstream data or violate data contracts

Respond with JSON:
{{
  "decision": "promote" | "promote_with_warning" | "halt",
  "reasoning": "2-3 sentences explaining why you made this decision",
  "root_cause": "likely cause of any failures/warnings",
  "recommended_action": "what the team should do next",
  "risk_assessment": "risk of promoting despite any warnings (low/medium/high)"
}}

Decision guidelines:
- First runs with only WARN results should usually promote (establishing baselines)
- Tier 1 pipelines should be more conservative (halt on warnings)
- Volume z-score warnings alone rarely justify halting
- PK uniqueness failures are almost always halt-worthy
- Schema consistency failures are always halt-worthy
- Null rate spikes on primary key columns are halt-worthy
- Count reconciliation failures suggest data loss — usually halt
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="decide_quality_gate",
                temperature=0.1,
            )
            result = self._extract_json(text)
            if "decision" not in result:
                return self._rule_based_gate_decision(contract, checks, is_first_run)
            return result
        except Exception as e:
            log.warning("Claude API error in decide_quality_gate: %s. Using fallback.", e)
            return self._rule_based_gate_decision(contract, checks, is_first_run)

    def _rule_based_gate_decision(self, contract, checks, is_first_run) -> dict:
        """Fallback: threshold-based gate decision when API is unavailable."""
        failed = [c for c in checks if c.status.value == "fail"]
        warned = [c for c in checks if c.status.value == "warn"]

        if is_first_run and not failed:
            return {
                "decision": "promote_with_warning" if warned else "promote",
                "reasoning": "First run — establishing baselines. Warnings are expected.",
                "root_cause": "No prior baselines to compare against",
                "recommended_action": "Monitor subsequent runs for consistency",
                "risk_assessment": "low",
            }

        if failed:
            return {
                "decision": "halt",
                "reasoning": f"Failed checks: {[c.check_name for c in failed]}. Data quality below threshold.",
                "root_cause": "; ".join(c.detail for c in failed),
                "recommended_action": "Investigate failed checks and re-run",
                "risk_assessment": "high",
            }

        if warned:
            qc = contract.quality_config
            decision = "promote_with_warning" if qc.promote_on_warn else "halt"
            return {
                "decision": decision,
                "reasoning": f"Warning checks: {[c.check_name for c in warned]}. {'Promoting per policy.' if decision == 'promote_with_warning' else 'Halting per policy.'}",
                "root_cause": "; ".join(c.detail for c in warned),
                "recommended_action": "Review warning details for data quality trends",
                "risk_assessment": "medium",
            }

        return {
            "decision": "promote",
            "reasoning": "All quality checks passed.",
            "root_cause": "None — all checks healthy",
            "recommended_action": "No action needed",
            "risk_assessment": "low",
        }

    # ------------------------------------------------------------------
    # Agentic error budget diagnosis (Tier 1B)
    # ------------------------------------------------------------------

    async def diagnose_error_budget(
        self,
        contract: PipelineContract,
        budget_info: dict,
        recent_runs: list,
    ) -> dict:
        """Agent diagnoses why error budget is exhausted and recommends recovery.

        Returns dict with:
            diagnosis: what's causing failures
            pattern: transient | persistent | degrading
            recommended_actions: list of specific actions
            should_pause: whether to pause the pipeline
            estimated_recovery: when it might self-heal (or "manual intervention needed")
        """
        run_summary = "\n".join(
            f"- {r.started_at}: {r.status.value if hasattr(r.status, 'value') else r.status}"
            f"{' — ' + r.error if r.error else ''}"
            for r in recent_runs[:10]
        )

        if not self.has_api:
            return self._rule_based_budget_diagnosis(budget_info, recent_runs)

        user_prompt = f"""Pipeline "{contract.pipeline_name}" has exhausted its error budget.

Error budget status:
- Success rate: {budget_info.get('success_rate', 0):.1%}
- Threshold: {budget_info.get('threshold', 0.9):.0%}
- Window: {budget_info.get('window_days', 7)} days
- Total runs: {budget_info.get('total_runs', 0)}, Failed: {budget_info.get('failed_runs', 0)}

Recent runs (newest first):
{run_summary}

Pipeline context:
- Tier: {contract.tier}
- Schedule: {contract.schedule_cron}
- Source: {contract.source_schema}.{contract.source_table}
- Refresh type: {contract.refresh_type.value if hasattr(contract.refresh_type, 'value') else contract.refresh_type}

Diagnose the failure pattern and recommend recovery actions.
Respond with JSON:
{{
  "diagnosis": "root cause analysis",
  "pattern": "transient|persistent|degrading",
  "recommended_actions": ["action 1", "action 2"],
  "should_pause": true/false,
  "estimated_recovery": "description or 'manual intervention needed'"
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="diagnose_error_budget",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("diagnose_error_budget Claude error: %s", e)
            return self._rule_based_budget_diagnosis(budget_info, recent_runs)

    def _rule_based_budget_diagnosis(self, budget_info, recent_runs) -> dict:
        errors = [r.error for r in recent_runs if r.error]
        unique_errors = list(set(errors[:5]))
        is_same_error = len(unique_errors) == 1 and len(errors) > 1

        return {
            "diagnosis": f"{'Same error repeating' if is_same_error else 'Multiple failure types'}: {unique_errors[:2]}",
            "pattern": "persistent" if is_same_error else "degrading",
            "recommended_actions": [
                "Investigate most recent error",
                "Check source system availability",
                "Review connector logs",
            ],
            "should_pause": budget_info.get("failed_runs", 0) > 5,
            "estimated_recovery": "manual intervention needed",
        }

    # ------------------------------------------------------------------
    # Agentic freshness reasoning (Tier 1C)
    # ------------------------------------------------------------------

    async def reason_about_freshness(
        self,
        pipeline_name: str,
        pipeline_id: str,
        tier: int,
        staleness_minutes: float,
        sla_warn: float,
        sla_critical: float,
        schedule_cron: str,
        recent_run_errors: list,
        downstream_count: int,
    ) -> dict:
        """Agent reasons about freshness SLA violations — is the SLA realistic?
        Is this a one-off or degradation? What severity is appropriate?

        Returns dict with:
            severity: "critical" | "warning" | "info"
            is_sla_realistic: whether the SLA makes sense for the schedule
            reasoning: contextual explanation
            recommended_action: what to do
            should_alert: whether to fire an alert at all
        """
        if not self.has_api:
            return self._rule_based_freshness(
                staleness_minutes, sla_warn, sla_critical, schedule_cron,
            )

        user_prompt = f"""Pipeline "{pipeline_name}" has a freshness SLA violation.

Freshness status:
- Current staleness: {staleness_minutes:.0f} minutes
- SLA warning threshold: {sla_warn} minutes
- SLA critical threshold: {sla_critical} minutes
- Pipeline schedule: {schedule_cron}
- Pipeline tier: {tier} (1=critical, 2=standard, 3=dev)
- Downstream dependents: {downstream_count}
- Recent errors: {recent_run_errors[:3] if recent_run_errors else 'none'}

Evaluate this freshness violation and respond with JSON:
{{
  "severity": "critical" | "warning" | "info",
  "is_sla_realistic": true/false,
  "reasoning": "contextual explanation — is the SLA achievable given the schedule? Is this a one-off delay or trend?",
  "recommended_action": "what to do about it",
  "should_alert": true/false,
  "sla_recommendation": "if SLA is unrealistic, suggest a better threshold"
}}

Consider:
- If schedule is every 4 hours but SLA warning is 1 hour, the SLA is impossible
- If there are recent errors, staleness may be caused by a known issue
- Tier 3 (dev) pipelines rarely need critical alerts for freshness
- If downstream_count is 0, impact is limited
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=pipeline_id,
                operation="reason_about_freshness",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("reason_about_freshness Claude error: %s", e)
            return self._rule_based_freshness(
                staleness_minutes, sla_warn, sla_critical, schedule_cron,
            )

    def _rule_based_freshness(self, staleness, sla_warn, sla_critical, schedule_cron) -> dict:
        if staleness > sla_critical:
            severity = "critical"
        elif staleness > sla_warn:
            severity = "warning"
        else:
            severity = "info"
        return {
            "severity": severity,
            "is_sla_realistic": True,
            "reasoning": f"Staleness {staleness:.0f}m vs SLA warn={sla_warn}m, critical={sla_critical}m",
            "recommended_action": "Investigate pipeline run status",
            "should_alert": severity != "info",
            "sla_recommendation": None,
        }

    # ------------------------------------------------------------------
    # Agentic run failure diagnosis (Tier 2A)
    # ------------------------------------------------------------------

    async def diagnose_run_failure(
        self,
        contract: PipelineContract,
        run_error: str,
        execution_log: list,
    ) -> dict:
        """Agent diagnoses why a run failed and recommends recovery action.

        Returns dict with:
            root_cause: what went wrong
            category: connector | source | target | network | schema | config | unknown
            is_transient: whether a retry is likely to succeed
            recommended_action: specific action to take
            should_retry: whether to automatically retry
            should_alert: whether this needs human attention
        """
        log_summary = "\n".join(
            f"- [{e.get('step', '?')}] {e.get('status', '?')}: {e.get('detail', '')}"
            for e in (execution_log or [])[-8:]
        )

        if not self.has_api:
            return self._rule_based_failure_diagnosis(run_error)

        user_prompt = f"""Pipeline "{contract.pipeline_name}" run failed.

Error: {run_error}

Execution log (last steps):
{log_summary}

Pipeline context:
- Source: {contract.source_schema}.{contract.source_table} via connector {contract.source_connector_id[:8]}
- Target: {contract.target_schema}.{contract.target_table} via connector {contract.target_connector_id[:8]}
- Refresh type: {contract.refresh_type.value if hasattr(contract.refresh_type, 'value') else contract.refresh_type}
- Tier: {contract.tier}

Diagnose this failure and recommend next steps.
Respond with JSON:
{{
  "root_cause": "specific diagnosis of what went wrong",
  "category": "connector|source|target|network|schema|config|resource|unknown",
  "is_transient": true/false,
  "recommended_action": "specific action to take",
  "should_retry": true/false,
  "should_alert": true/false
}}

Transient examples: connection timeout, temporary lock, rate limit
Persistent examples: schema mismatch, missing table, authentication failure, disk full
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="diagnose_run_failure",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("diagnose_run_failure Claude error: %s", e)
            return self._rule_based_failure_diagnosis(run_error)

    def _rule_based_failure_diagnosis(self, error: str) -> dict:
        error_lower = (error or "").lower()
        if any(w in error_lower for w in ("timeout", "timed out", "connection reset")):
            return {"root_cause": error, "category": "network", "is_transient": True,
                    "recommended_action": "Retry — likely transient network issue",
                    "should_retry": True, "should_alert": False}
        if any(w in error_lower for w in ("permission", "authentication", "access denied", "password")):
            return {"root_cause": error, "category": "config", "is_transient": False,
                    "recommended_action": "Check credentials and permissions",
                    "should_retry": False, "should_alert": True}
        if any(w in error_lower for w in ("no such table", "relation", "does not exist", "column")):
            return {"root_cause": error, "category": "schema", "is_transient": False,
                    "recommended_action": "Schema mismatch — review source and target table definitions",
                    "should_retry": False, "should_alert": True}
        if any(w in error_lower for w in ("disk", "space", "no space")):
            return {"root_cause": error, "category": "resource", "is_transient": False,
                    "recommended_action": "Free disk space or increase storage",
                    "should_retry": False, "should_alert": True}
        return {"root_cause": error, "category": "unknown", "is_transient": False,
                "recommended_action": "Investigate error manually",
                "should_retry": False, "should_alert": True}

    # ------------------------------------------------------------------
    # Agentic run insights (Build 30)
    # ------------------------------------------------------------------

    async def generate_run_insights(
        self,
        contract: PipelineContract,
        run: RunRecord,
        prior_runs: list,
    ) -> list[dict]:
        """Agent analyzes completed run and generates actionable insights.

        Returns list of dicts, each with:
            category: quality | performance | strategy | schema | schedule | volume | configuration | error
            message: human-readable insight
            priority: high | medium | low
            action_type: patch_pipeline | investigate | acknowledge | null
            action_payload: dict for patch_pipeline actions, or null
        """
        is_first_run = len([r for r in prior_runs if r.status == RunStatus.COMPLETE]) == 0

        # Build run summary for prompt
        checks_summary = ""
        if run.quality_results and isinstance(run.quality_results.get("checks"), list):
            checks_summary = "\n".join(
                f"  - {c.get('name', '?')}: {c.get('status', '?')} — {c.get('detail', '')}"
                for c in run.quality_results["checks"]
            )

        prior_summary = ""
        if prior_runs:
            prior_summary = "\n".join(
                f"  - {r.status.value if hasattr(r.status, 'value') else r.status}: "
                f"{r.rows_extracted} rows, gate={r.gate_decision.value if r.gate_decision and hasattr(r.gate_decision, 'value') else r.gate_decision}"
                for r in prior_runs[:5]
            )

        refresh = contract.refresh_type.value if hasattr(contract.refresh_type, "value") else contract.refresh_type
        load = contract.load_type.value if hasattr(contract.load_type, "value") else contract.load_type

        if not self.has_api:
            return self._rule_based_run_insights(contract, run, prior_runs)

        user_prompt = f"""Pipeline "{contract.pipeline_name}" just completed a run. Analyze the results and provide actionable insights.

Run results:
- Status: {run.status.value if hasattr(run.status, 'value') else run.status}
- Rows extracted: {run.rows_extracted}
- Rows loaded: {run.rows_loaded}
- Gate decision: {run.gate_decision.value if run.gate_decision and hasattr(run.gate_decision, 'value') else run.gate_decision}
- Error: {run.error or "none"}
- Watermark: {run.watermark_before} → {run.watermark_after}
- Is first run: {is_first_run}

Quality checks:
{checks_summary or "  (none)"}

Pipeline configuration:
- Refresh type: {refresh}
- Load type: {load}
- Merge keys: {contract.merge_keys or "none"}
- Incremental column: {contract.incremental_column or "none"}
- Schedule: {contract.schedule_cron or "none"}
- Tier: {contract.tier}
- Source: {contract.source_schema}.{contract.source_table}
- Target: {contract.target_schema}.{contract.target_table}
- Column mappings: {len(contract.column_mappings)} columns
- Baselines: row_avg={contract.baseline_volume_avg}, row_stddev={contract.baseline_volume_stddev}

Recent run history (newest first):
{prior_summary or "  (no prior runs)"}

Generate 2-5 insights. Focus on:
- First runs: baseline establishment, strategy optimization, merge key suggestions, schedule recommendations
- Subsequent runs: volume trends, quality patterns, performance, error patterns
- Always: configuration improvements, strategy mismatches, actionable quick-wins

For action_type "patch_pipeline", provide the exact fields to PATCH in action_payload.

Respond with a JSON array:
[{{
  "category": "quality|performance|strategy|schema|schedule|volume|configuration|error",
  "message": "Clear, actionable insight in 1-2 sentences for a non-technical user",
  "priority": "high|medium|low",
  "action_type": "patch_pipeline" or "investigate" or "acknowledge" or null,
  "action_payload": {{}} or null
}}]
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="generate_run_insights",
            )
            result = self._extract_json(text)
            if isinstance(result, list):
                return result[:5]  # cap at 5 insights
            return []
        except Exception as e:
            log.warning("generate_run_insights Claude error: %s", e)
            return self._rule_based_run_insights(contract, run, prior_runs)

    def _rule_based_run_insights(
        self,
        contract: PipelineContract,
        run: RunRecord,
        prior_runs: list,
    ) -> list[dict]:
        """Rule-based fallback for run insights when API key is unavailable."""
        insights = []
        is_first_run = len([r for r in prior_runs if r.status == RunStatus.COMPLETE]) == 0
        refresh = contract.refresh_type.value if hasattr(contract.refresh_type, "value") else contract.refresh_type
        load = contract.load_type.value if hasattr(contract.load_type, "value") else contract.load_type
        status = run.status.value if hasattr(run.status, "value") else run.status

        if status == "complete":
            if is_first_run:
                insights.append({
                    "category": "volume",
                    "message": f"First run complete — {run.rows_extracted} rows established as your baseline. Future runs will be compared against this.",
                    "priority": "medium",
                    "action_type": None,
                    "action_payload": None,
                })
            # Suggest incremental if full refresh and has timestamp column
            if refresh == "full" and contract.incremental_column:
                insights.append({
                    "category": "strategy",
                    "message": f"This pipeline uses full refresh but has an incremental column '{contract.incremental_column}'. Switching to incremental would be faster and use less resources.",
                    "priority": "high",
                    "action_type": "patch_pipeline",
                    "action_payload": {"refresh_type": "incremental"},
                })
            # Suggest merge keys if using append
            if load == "append" and contract.merge_keys:
                insights.append({
                    "category": "strategy",
                    "message": "This pipeline appends data but has merge keys defined. Switching to merge (upsert) would prevent duplicate rows on re-runs.",
                    "priority": "high",
                    "action_type": "patch_pipeline",
                    "action_payload": {"load_type": "merge"},
                })
            # Volume anomaly check
            if (
                not is_first_run
                and contract.baseline_volume_avg
                and contract.baseline_volume_avg > 0
                and run.rows_extracted > 0
            ):
                ratio = run.rows_extracted / contract.baseline_volume_avg
                if ratio < 0.5:
                    insights.append({
                        "category": "volume",
                        "message": f"Row count ({run.rows_extracted}) is {ratio:.0%} of the baseline average ({contract.baseline_volume_avg:.0f}). This is a significant drop worth investigating.",
                        "priority": "high",
                        "action_type": "investigate",
                        "action_payload": None,
                    })
                elif ratio > 2.0:
                    insights.append({
                        "category": "volume",
                        "message": f"Row count ({run.rows_extracted}) is {ratio:.1f}x the baseline average ({contract.baseline_volume_avg:.0f}). Verify this volume spike is expected.",
                        "priority": "medium",
                        "action_type": "investigate",
                        "action_payload": None,
                    })
            # Quality warnings
            if run.gate_decision and (
                run.gate_decision == GateDecision.PROMOTE_WITH_WARNING
                or (hasattr(run.gate_decision, "value") and run.gate_decision.value == "promote_with_warning")
            ):
                insights.append({
                    "category": "quality",
                    "message": "Data was promoted with warnings. Review the quality check details above to see which checks flagged issues.",
                    "priority": "medium",
                    "action_type": "investigate",
                    "action_payload": None,
                })

        elif status == "failed":
            insights.append({
                "category": "error",
                "message": f"Run failed: {(run.error or 'unknown error')[:200]}. Check the error details and execution log for root cause.",
                "priority": "high",
                "action_type": "investigate",
                "action_payload": None,
            })
            # Count consecutive failures
            consec_failures = 0
            for r in prior_runs:
                s = r.status.value if hasattr(r.status, "value") else r.status
                if s == "failed":
                    consec_failures += 1
                else:
                    break
            if consec_failures >= 2:
                insights.append({
                    "category": "error",
                    "message": f"This is the {consec_failures + 1}{'rd' if consec_failures + 1 == 3 else 'th'} consecutive failure. Consider pausing the pipeline until the root cause is resolved.",
                    "priority": "high",
                    "action_type": "investigate",
                    "action_payload": None,
                })

        elif status == "halted":
            insights.append({
                "category": "quality",
                "message": "The quality gate halted this run — data was not promoted to production. Review the check results to understand why.",
                "priority": "high",
                "action_type": "investigate",
                "action_payload": None,
            })

        return insights[:5]

    # ------------------------------------------------------------------
    # Agentic preflight reasoning (Tier 2B)
    # ------------------------------------------------------------------

    async def reason_about_preflight_failure(
        self,
        contract: PipelineContract,
        failure_reason: str,
        context: dict,
    ) -> dict:
        """Agent reasons about why preflight failed and what to do.

        Returns dict with:
            diagnosis: what's wrong
            recommended_action: specific steps to fix
            can_auto_resolve: whether the platform can fix this automatically
            auto_resolve_action: what to do if auto-resolvable
        """
        if not self.has_api:
            return {
                "diagnosis": failure_reason,
                "recommended_action": "Investigate and resolve the preflight issue",
                "can_auto_resolve": False,
                "auto_resolve_action": None,
            }

        user_prompt = f"""Pipeline "{contract.pipeline_name}" failed preflight checks.

Failure reason: {failure_reason}

Context:
{json.dumps(context, indent=2, default=str)}

Pipeline:
- Schedule: {contract.schedule_cron}
- Tier: {contract.tier}
- Source: {contract.source_schema}.{contract.source_table}

What's wrong and what should we do? Can the platform auto-resolve this?
Respond with JSON:
{{
  "diagnosis": "what's actually wrong",
  "recommended_action": "specific steps to fix this",
  "can_auto_resolve": true/false,
  "auto_resolve_action": "what to do automatically, or null"
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="reason_about_preflight",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("reason_about_preflight Claude error: %s", e)
            return {
                "diagnosis": failure_reason,
                "recommended_action": "Investigate and resolve the preflight issue",
                "can_auto_resolve": False,
                "auto_resolve_action": None,
            }

    # ------------------------------------------------------------------
    # Agentic contract violation assessment (Tier 2C)
    # ------------------------------------------------------------------

    async def assess_contract_violation(
        self,
        violation_detail: str,
        violation_type: str,
        producer_name: str,
        consumer_name: str,
        producer_tier: int,
        contract_info: dict,
    ) -> dict:
        """Agent assesses impact of a data contract violation.

        Returns dict with:
            severity: "critical" | "warning" | "info"
            impact_assessment: how this affects the consumer
            recommended_action: what to do
            is_actionable: whether the team should act now vs. monitor
        """
        if not self.has_api:
            severity = "critical" if producer_tier == 1 else "warning"
            return {
                "severity": severity,
                "impact_assessment": f"Contract violation between {producer_name} and {consumer_name}: {violation_detail}",
                "recommended_action": "Investigate the violation and update contract or fix producer",
                "is_actionable": True,
            }

        user_prompt = f"""A data contract violation was detected.

Violation:
- Type: {violation_type}
- Detail: {violation_detail}
- Producer pipeline: {producer_name} (tier {producer_tier})
- Consumer pipeline: {consumer_name}

Contract terms:
{json.dumps(contract_info, indent=2, default=str)}

Assess the impact and recommend action.
Respond with JSON:
{{
  "severity": "critical|warning|info",
  "impact_assessment": "how this affects the consumer pipeline and downstream data",
  "recommended_action": "specific steps to resolve",
  "is_actionable": true/false
}}

Consider:
- Freshness SLA violations on tier 1 producers with active consumers are critical
- Missing columns that are actually used by the consumer are critical
- Missing columns that are optional or unused are info-level
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                operation="assess_contract_violation",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("assess_contract_violation Claude error: %s", e)
            severity = "critical" if producer_tier == 1 else "warning"
            return {
                "severity": severity,
                "impact_assessment": violation_detail,
                "recommended_action": "Investigate the violation",
                "is_actionable": True,
            }

    # ------------------------------------------------------------------
    # Agentic anomaly threshold setting (Tier 2D)
    # ------------------------------------------------------------------

    async def evaluate_anomaly_signals(
        self,
        pipeline_name: str,
        pipeline_id: str,
        tier: int,
        schedule_cron: str,
        volume_history: list,
        recent_failures: list,
        budget_info: dict | None,
    ) -> dict:
        """Agent evaluates whether observed signals are truly anomalous
        given the pipeline's history and context.

        Instead of hardcoded thresholds (30% deviation, 2 failures),
        the agent reasons about what's normal for THIS pipeline.

        Returns dict with:
            is_anomalous: whether this pipeline has real anomalies
            signals: list of confirmed anomaly signals with severity
            reasoning: why these are or aren't anomalies
        """
        vol_text = ""
        if volume_history:
            counts = [v.get("rows_extracted", 0) for v in volume_history[:10]]
            vol_text = f"Volume history (recent→old): {counts}"

        fail_text = ""
        if recent_failures:
            fail_text = f"Recent failures: {len(recent_failures)} in 24h — {[f.error[:80] for f in recent_failures[:3]]}"

        budget_text = ""
        if budget_info:
            budget_text = f"Error budget: {budget_info.get('success_rate', 1):.1%} success, {budget_info.get('budget_remaining', 1):.3f} remaining"

        if not self.has_api:
            return self._rule_based_anomaly_evaluation(
                volume_history, recent_failures, budget_info, tier,
            )

        user_prompt = f"""Evaluate whether pipeline "{pipeline_name}" (tier {tier}, schedule: {schedule_cron}) shows real anomalies.

{vol_text}
{fail_text}
{budget_text}

Consider:
- Is the volume pattern normal for this pipeline's history? (weekday/weekend patterns, growth trends)
- Are failures transient (one-off) or persistent?
- Does the error budget reflect a real problem or just a few unlucky runs?

Respond with JSON:
{{
  "is_anomalous": true/false,
  "signals": [
    {{
      "type": "volume_drop|volume_spike|repeated_failure|error_budget_low",
      "severity": "critical|warning|info",
      "observation": "what was observed",
      "is_expected": true/false,
      "reasoning": "why this is or isn't a concern"
    }}
  ],
  "reasoning": "overall assessment for this pipeline"
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=pipeline_id,
                operation="evaluate_anomaly_signals",
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("evaluate_anomaly_signals Claude error: %s", e)
            return self._rule_based_anomaly_evaluation(
                volume_history, recent_failures, budget_info, tier,
            )

    def _rule_based_anomaly_evaluation(self, volume_history, recent_failures, budget_info, tier) -> dict:
        signals = []
        if volume_history and len(volume_history) >= 3:
            counts = [v.get("rows_extracted", 0) for v in volume_history[:10]]
            avg = sum(counts) / len(counts) if counts else 0
            latest = counts[0] if counts else 0
            if avg > 0:
                deviation = abs(latest - avg) / avg
                if deviation > 0.3:
                    direction = "drop" if latest < avg else "spike"
                    signals.append({
                        "type": f"volume_{direction}",
                        "severity": "critical" if tier == 1 and deviation > 0.5 else "warning",
                        "observation": f"{direction}: {latest} vs avg {avg:.0f} ({deviation:.0%} deviation)",
                        "is_expected": False,
                        "reasoning": "Rule-based detection",
                    })
        if recent_failures and len(recent_failures) >= 2:
            signals.append({
                "type": "repeated_failure",
                "severity": "critical" if tier == 1 else "warning",
                "observation": f"{len(recent_failures)} failures in 24h",
                "is_expected": False,
                "reasoning": "Multiple failures suggest a persistent issue",
            })
        if budget_info and budget_info.get("budget_remaining", 1) < 0.05 and budget_info.get("total_runs", 0) > 0:
            signals.append({
                "type": "error_budget_low",
                "severity": "critical",
                "observation": f"Budget nearly exhausted: {budget_info.get('success_rate', 0):.1%} success",
                "is_expected": False,
                "reasoning": "Error budget below 5% remaining",
            })
        return {
            "is_anomalous": len(signals) > 0,
            "signals": signals,
            "reasoning": f"{len(signals)} anomaly signal(s) detected" if signals else "No anomalies",
        }

    # ------------------------------------------------------------------
    # generate_connector
    # ------------------------------------------------------------------

    async def generate_connector(
        self,
        connector_type: ConnectorType,
        source_target_type: str,
        connection_params: dict,
        attempt: int = 1,
        previous_error: Optional[str] = None,
    ) -> ConnectorRecord:
        """Generate a new connector using Claude with sandbox validation.

        Retries up to 3 times with error feedback. Saves as DRAFT ConnectorRecord
        and creates an approval proposal.
        """
        if not self.has_api:
            raise RuntimeError(
                "Connector generation requires an Anthropic API key. "
                "Set ANTHROPIC_API_KEY to enable this feature."
            )

        # Normalize connector_type to enum if passed as string
        if isinstance(connector_type, str):
            connector_type = ConnectorType(connector_type.lower())

        is_source = connector_type == ConnectorType.SOURCE
        base_class = "SourceEngine" if is_source else "TargetEngine"

        # Load reference seed code and abstract interface
        seed_name = "mysql-source-v1" if is_source else "redshift-target-v1"
        seed_record = await self.store.get_connector_by_name(seed_name)
        reference_code = (
            seed_record.code if seed_record and seed_record.code
            else "(no reference available)"
        )

        if is_source:
            base_mod = __import__("source.base", fromlist=["SourceEngine"])
            base_interface = _inspect.getsource(base_mod.SourceEngine)
        else:
            base_mod = __import__("target.base", fromlist=["TargetEngine"])
            base_interface = _inspect.getsource(base_mod.TargetEngine)

        error_note = ""
        if previous_error:
            error_note = (
                f"\n\nPrevious attempt (attempt {attempt - 1}) failed with this error:\n"
                f"{previous_error}\nFix this issue in your new implementation."
            )

        user_prompt = f"""
Generate a complete Python connector class for {source_target_type} as a {connector_type.value} connector.

Abstract interface to implement:
```python
{base_interface}
```

Reference implementation (MySQL {'source' if is_source else 'target'} -- use as a pattern):
```python
{reference_code}
```

Target type to implement: {source_target_type}
Connection parameters available: {json.dumps(connection_params)}
{error_note}

Requirements:
1. Class must extend {base_class} and implement ALL abstract methods
2. get_{'source' if is_source else 'target'}_type() must return "{source_target_type}"
3. Include all necessary imports at the top of the file
4. Handle connection errors gracefully -- return ConnectionResult(success=False, error=str(e)) on failure
5. Include pip package name in a module-level comment: # REQUIRES: package-name>=version
6. Add metadata columns (_extracted_at, _source_schema, _source_table, _row_hash) in extract()
7. _row_hash must be SHA-256 of all source column values concatenated

Respond with ONLY the Python code -- no explanation, no markdown fences.
"""

        max_attempts = 3
        generation_log = []
        code = ""
        last_error = previous_error

        for att in range(attempt, attempt + max_attempts):
            try:
                # Build prompt with error feedback for retries
                if att > attempt and last_error:
                    retry_prompt = (
                        user_prompt
                        + f"\n\nAttempt {att - 1} failed validation:\n{last_error}\n"
                        "Fix ALL issues."
                    )
                else:
                    retry_prompt = user_prompt

                log.info(
                    "Generating %s connector for %s (attempt %d)...",
                    connector_type.value, source_target_type, att,
                )
                code = await self._call_claude(
                    self._system_prompt(), retry_prompt,
                    operation="generate_connector",
                    temperature=0.3,
                )

                # Strip markdown fences if present
                code = re.sub(r"^```python\s*", "", code.strip())
                code = re.sub(r"```\s*$", "", code.strip())

                # Validate via sandbox
                valid, validation_error = validate_connector_code(code)
                if valid:
                    generation_log.append({
                        "attempt": att,
                        "status": "validated",
                        "code_length": len(code),
                    })
                    break
                else:
                    last_error = validation_error
                    generation_log.append({
                        "attempt": att,
                        "status": "validation_failed",
                        "error": validation_error,
                        "code_length": len(code),
                    })
                    log.warning(
                        "Connector validation failed (attempt %d): %s",
                        att, validation_error,
                    )

            except Exception as e:
                last_error = str(e)
                generation_log.append({
                    "attempt": att,
                    "status": "error",
                    "error": str(e),
                })
                log.error("Generation attempt %d failed: %s", att, e)
        else:
            # All attempts exhausted -- still save draft with last code
            log.error(
                "All %d generation attempts failed for %s %s",
                max_attempts, connector_type.value, source_target_type,
            )

        connector_name = f"{source_target_type}-{'source' if is_source else 'target'}-v1"
        existing = await self.store.get_connector_by_name(connector_name)
        if existing:
            connector_name = (
                f"{source_target_type}-{'source' if is_source else 'target'}"
                f"-v{existing.version + 1}"
            )

        # Extract required packages from code comment
        deps: list[str] = []
        for line in code.splitlines()[:10]:
            if "REQUIRES:" in line:
                deps = [p.strip() for p in line.split("REQUIRES:", 1)[1].split(",")]
                break

        record = ConnectorRecord(
            connector_name=connector_name,
            connector_type=connector_type,
            source_target_type=source_target_type,
            generated_by=self.config.model,
            interface_version="1.0",
            code=code,
            dependencies=deps,
            test_status=TestStatus.UNTESTED,
            generation_attempts=att if code else attempt + max_attempts - 1,
            generation_log=generation_log,
            status=ConnectorStatus.DRAFT,
        )
        await self.store.save_connector(record)

        # Create approval proposal
        from contracts.models import ContractChangeProposal
        proposal = ContractChangeProposal(
            connector_id=record.connector_id,
            trigger_type=TriggerType.NEW_CONNECTOR,
            change_type=ChangeType.NEW_CONNECTOR,
            proposed_state={
                "connector_name": connector_name,
                "connector_type": connector_type.value,
                "source_target_type": source_target_type,
                "dependencies": deps,
            },
            reasoning=(
                f"Auto-generated {connector_type.value} connector for "
                f"{source_target_type}. Requires manual approval before activation."
            ),
            confidence=0.6,
            status=ProposalStatus.PENDING,
        )
        await self.store.save_proposal(proposal)

        log.info(
            "Generated connector %s (id=%s, attempts=%d)",
            connector_name, record.connector_id[:8], len(generation_log),
        )
        return record

    # ------------------------------------------------------------------
    # learn_from_rejection
    # ------------------------------------------------------------------

    async def learn_from_rejection(
        self,
        proposal: ContractChangeProposal,
        resolution_note: str,
    ) -> Optional[AgentPreference]:
        """Extract and store a preference from a rejected proposal.

        Returns preference if confidence >= 0.7, else None.
        Generates embedding if Voyage API is available.
        """
        if not self.has_api:
            return None

        user_prompt = f"""
A proposal was rejected by a user. Extract a preference to apply to future proposals.

Proposal details:
- Pipeline: {proposal.pipeline_id}
- Change type: {proposal.change_type.value}
- Reasoning: {proposal.reasoning}
- Proposed: {json.dumps(proposal.proposed_state, indent=2)}
- User's rejection note: "{resolution_note}"

Respond with JSON:
{{
  "should_store_preference": true/false,
  "confidence": 0.0-1.0,
  "preference_key": "descriptive_key_like_preferred_merge_key",
  "preference_value": {{}},
  "scope": "global" | "pipeline" | "schema" | "source_type",
  "scope_value": "pipeline_id or schema_name or source_type or null for global",
  "interpretation": "plain English summary of what was learned"
}}

Only set should_store_preference=true if confidence >= 0.7 and the preference is clearly actionable.
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=proposal.pipeline_id or "",
                operation="learn_from_rejection",
            )
            result = self._extract_json(text)

            if not result.get("should_store_preference") or result.get("confidence", 0) < 0.7:
                return None

            # Generate embedding for semantic search
            embedding_text = (
                f"{result.get('preference_key', '')} "
                f"{json.dumps(result.get('preference_value', {}))} "
                f"{result.get('interpretation', '')}"
            )
            embedding = await self._embed(embedding_text)

            pref = AgentPreference(
                scope=PreferenceScope(result.get("scope", "pipeline")),
                scope_value=result.get("scope_value") or proposal.pipeline_id,
                preference_key=result["preference_key"],
                preference_value=result.get("preference_value", {}),
                source=PreferenceSource.REJECTION_INFERRED,
                confidence=result["confidence"],
                embedding=embedding,
            )
            await self.store.save_preference(pref)
            log.info(
                "Stored rejection-inferred preference: %s = %s (embedding=%s)",
                pref.preference_key, pref.preference_value,
                "yes" if embedding else "no",
            )
            return pref

        except Exception as e:
            log.warning("learn_from_rejection error: %s", e)
            return None

    # ------------------------------------------------------------------
    # generate_digest
    # ------------------------------------------------------------------

    async def generate_digest(
        self,
        alerts: list[AlertRecord],
        pipeline_names: dict[str, str],
    ) -> str:
        """Generate a daily alert digest. Groups alerts by pipeline, leads with critical."""
        if not alerts:
            return "No alerts in the last 24 hours."

        if not self.has_api:
            return self._rule_based_digest(alerts, pipeline_names)

        alert_text = "\n".join(
            f"- [{a.severity.value.upper()}] "
            f"{pipeline_names.get(a.pipeline_id, a.pipeline_id)}: {a.summary}"
            for a in alerts
        )
        user_prompt = f"""
Generate a daily data pipeline digest email body (plain text, no markdown) summarizing these alerts:

{alert_text}

Group by pipeline. Lead with the most critical issues. End with a one-line overall health summary.
Keep it under 200 words.
"""
        try:
            return await self._call_claude(
                self._system_prompt(), user_prompt,
                operation="generate_digest",
                temperature=0.3,
            )
        except Exception as e:
            log.warning("generate_digest error: %s", e)
            return self._rule_based_digest(alerts, pipeline_names)

    def _rule_based_digest(
        self,
        alerts: list[AlertRecord],
        pipeline_names: dict[str, str],
    ) -> str:
        # Count by severity
        severity_counts: dict[str, int] = {}
        grouped: dict[str, list[str]] = {}
        for a in alerts:
            sev = a.severity.value
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
            name = pipeline_names.get(a.pipeline_id, a.pipeline_id)
            grouped.setdefault(name, []).append(sev)

        lines = [f"Daily digest -- {len(alerts)} alert(s):"]
        summary_parts = []
        for sev in ("critical", "warning", "info"):
            count = severity_counts.get(sev, 0)
            if count:
                summary_parts.append(f"{count} {sev}")
        if summary_parts:
            lines.append(f"  Summary: {', '.join(summary_parts)}")
        lines.append("")

        # Group by pipeline, critical first
        for name in sorted(
            grouped.keys(),
            key=lambda n: (0 if "critical" in grouped[n] else 1, n),
        ):
            severities = grouped[name]
            lines.append(f"  {name}: {', '.join(severities)}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # route_command
    # ------------------------------------------------------------------

    async def route_command(
        self,
        user_text: str,
        context: Optional[dict] = None,
        history: Optional[list[dict]] = None,
    ) -> dict:
        """Route natural language commands to structured actions.

        Returns dict with keys: action, params, response_text, input_tokens, output_tokens.
        Falls back to keyword extraction when no API key.
        """
        # Reset per-request token accumulator
        self._req_input_tokens = 0
        self._req_output_tokens = 0

        if not self.has_api:
            return self._keyword_route(user_text, context)

        ctx_text = ""
        if context:
            ctx_text = f"\n\nCurrent context:\n{json.dumps(context, indent=2, default=str)}"

        history_text = ""
        if history:
            history_lines = []
            for msg in history[-10:]:  # Last 10 messages for context
                role = msg.get("role", "user")
                text = msg.get("text", "")
                history_lines.append(f"  {role}: {text}")
            history_text = "\n\nConversation history:\n" + "\n".join(history_lines)

        user_prompt = f"""
Parse this user command and determine the appropriate action.

User command: "{user_text}"
{ctx_text}
{history_text}

Available actions:
- list_pipelines: List all pipelines (params: status filter)
- list_connectors: List available source/target connectors (params: optional type "source"/"target")
- discover_tables: Discover tables in a source database (params: connector_id or connector_type like "sqlite"/"mysql", database path or host/port/database, user, password)
- profile_table: Profile a specific table to see schema, row counts, keys (params: connector_id or connector_type, database, schema, table)
- propose_strategy: Propose ingestion strategy for a table (params: same as profile_table)
- create_pipeline: Create a new pipeline (params: source_connector_type, source_database, source_schema, source_table, target_connector_type, target_host, target_port, target_database, target_user, target_password, target_schema, schedule_cron)
- check_freshness: Check freshness for a pipeline (params: pipeline_id or pipeline_name)
- trigger_run: Manually trigger a pipeline run (params: pipeline_id or pipeline_name)
- trigger_backfill: Trigger a backfill run (params: pipeline_id, start, end)
- generate_connector: Generate a new connector (params: type, source_target_type, connection_params)
- check_status: Get pipeline status/details (params: pipeline_id or pipeline_name)
- list_alerts: List recent alerts (params: severity, pipeline_id)
- approve_proposal: Approve a pending proposal (params: proposal_id)
- reject_proposal: Reject a pending proposal (params: proposal_id, note)
- pause_pipeline: Pause a pipeline (params: pipeline_id)
- resume_pipeline: Resume a paused pipeline (params: pipeline_id)
- design_topology: Design a multi-pipeline architecture from a description (params: description of what the user needs)
- diagnose_pipeline: Diagnose why a pipeline is failing or unhealthy (params: pipeline_name or pipeline_id)
- analyze_impact: Analyze downstream impact if a pipeline/table goes down (params: pipeline_name or pipeline_id)
- check_anomalies: Check for platform-wide anomalies and unusual patterns (params: none)
- explain: Explain something about the system (params: topic)
- unknown: Could not determine intent

IMPORTANT: Extract ONLY parameters the user explicitly stated. Do NOT invent or assume defaults.
- "discover tables in my SQLite at ./data/demo/sample.db" -> connector_type: "sqlite", database: "./data/demo/sample.db"
- "profile customers table in main schema" -> schema: "main", table: "customers"
- "create a pipeline for main.customers from SQLite ./data/demo/sample.db to Postgres localhost:5432/pipeline_agent" -> extract all params
- "I want to set up a pipeline from sqlite to postgres" -> source_connector_type: "sqlite", target_connector_type: "postgres" (do NOT fill in host/port/database/schema — those were not provided)

If the user refers to something from conversation history (like "that database" or "the customers table"), resolve it from history context.
Only include a param in the JSON if the user actually said it or it's clearly in history. Leave missing params out — the system will ask for them.

Respond with JSON:
{{
  "action": "action_name",
  "params": {{}},
  "response_text": "human-readable response to show the user",
  "confidence": 0.0-1.0
}}
"""
        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                operation="route_command",
                temperature=0.1,
            )
            result = self._extract_json(text)
            # Ensure required keys
            result.setdefault("action", "unknown")
            result.setdefault("params", {})
            result.setdefault("response_text", "")
            result["input_tokens"] = self._req_input_tokens
            result["output_tokens"] = self._req_output_tokens
            return result
        except Exception as e:
            log.warning("route_command Claude error: %s. Using keyword fallback.", e)
            fallback = self._keyword_route(user_text, context)
            fallback["input_tokens"] = self._req_input_tokens
            fallback["output_tokens"] = self._req_output_tokens
            return fallback

    def _keyword_route(self, user_text: str, context: Optional[dict] = None) -> dict:
        """Keyword-based command routing fallback."""
        text_lower = user_text.lower().strip()

        # Pipeline listing
        if any(kw in text_lower for kw in ("list pipeline", "show pipeline", "all pipeline", "my pipeline")):
            status = None
            for s in ("active", "paused", "failed", "archived"):
                if s in text_lower:
                    status = s
                    break
            return {
                "action": "list_pipelines",
                "params": {"status": status},
                "response_text": "Listing pipelines...",
            }

        # Freshness
        if any(kw in text_lower for kw in ("freshness", "stale", "fresh")):
            return {
                "action": "check_freshness",
                "params": {"query": user_text},
                "response_text": "Checking freshness...",
            }

        # Trigger run
        if any(kw in text_lower for kw in ("trigger", "run ", "execute")):
            if "backfill" in text_lower:
                return {
                    "action": "trigger_backfill",
                    "params": {"query": user_text},
                    "response_text": "Processing backfill request...",
                }
            return {
                "action": "trigger_run",
                "params": {"query": user_text},
                "response_text": "Triggering pipeline run...",
            }

        # Generate connector
        if any(kw in text_lower for kw in ("generate connector", "create connector", "new connector")):
            return {
                "action": "generate_connector",
                "params": {"query": user_text},
                "response_text": "Processing connector generation request...",
            }

        # Status
        if any(kw in text_lower for kw in ("status", "check ", "detail")):
            return {
                "action": "check_status",
                "params": {"query": user_text},
                "response_text": "Checking status...",
            }

        # Alerts
        if any(kw in text_lower for kw in ("alert", "warning", "critical", "error")):
            return {
                "action": "list_alerts",
                "params": {"query": user_text},
                "response_text": "Listing alerts...",
            }

        # Approve / reject
        if "approve" in text_lower:
            return {
                "action": "approve_proposal",
                "params": {"query": user_text},
                "response_text": "Processing approval...",
            }
        if "reject" in text_lower:
            return {
                "action": "reject_proposal",
                "params": {"query": user_text},
                "response_text": "Processing rejection...",
            }

        # List connectors
        if "connector" in text_lower and any(kw in text_lower for kw in (
            "list", "show", "available", "what", "which", "all",
        )):
            conn_type = None
            if "source" in text_lower:
                conn_type = "source"
            elif "target" in text_lower:
                conn_type = "target"
            return {
                "action": "list_connectors",
                "params": {"type": conn_type},
                "response_text": "Listing connectors...",
            }

        # Pause / resume
        if "pause" in text_lower:
            return {
                "action": "pause_pipeline",
                "params": {"query": user_text},
                "response_text": "Pausing pipeline...",
            }
        if "resume" in text_lower or "unpause" in text_lower:
            return {
                "action": "resume_pipeline",
                "params": {"query": user_text},
                "response_text": "Resuming pipeline...",
            }

        # SQL Transforms (Build 29)
        if any(kw in text_lower for kw in (
            "create transform", "generate transform", "sql transform",
            "new transform", "build transform", "add transform",
            "create a view", "create a table as",
        )):
            return {
                "action": "generate_transform",
                "params": {"description": user_text},
                "response_text": "Generating SQL transform...",
            }
        if any(kw in text_lower for kw in (
            "list transform", "show transform", "my transform", "all transform",
        )):
            return {
                "action": "list_transforms",
                "params": {},
                "response_text": "Listing transforms...",
            }

        # Design topology (Build 20)
        if any(kw in text_lower for kw in (
            "design", "architect", "topology", "multi-pipeline",
            "set up pipelines for", "build pipelines for",
            "data architecture", "pipeline architecture",
        )):
            return {
                "action": "design_topology",
                "params": {"description": user_text},
                "response_text": "Designing pipeline topology...",
            }

        # Diagnose (Build 24)
        if any(kw in text_lower for kw in (
            "diagnose", "root cause", "debug pipeline",
            "why is it failing", "why did it fail", "why failed",
        )):
            return {
                "action": "diagnose_pipeline",
                "params": {"query": user_text},
                "response_text": "Diagnosing pipeline...",
            }

        # Impact analysis (Build 24)
        if any(kw in text_lower for kw in (
            "impact", "what breaks", "what happens if",
            "blast radius", "downstream effect", "who depends",
        )):
            return {
                "action": "analyze_impact",
                "params": {"query": user_text},
                "response_text": "Analyzing downstream impact...",
            }

        # Anomalies (Build 24)
        if any(kw in text_lower for kw in (
            "anomal", "unusual", "something wrong", "platform health",
            "anything weird", "any issues",
        )):
            return {
                "action": "check_anomalies",
                "params": {},
                "response_text": "Checking for anomalies across all pipelines...",
            }

        # Explain / help
        if any(kw in text_lower for kw in ("explain", "help", "how", "what", "why")):
            return {
                "action": "explain",
                "params": {"topic": user_text},
                "response_text": "Let me explain...",
            }

        return {
            "action": "unknown",
            "params": {"raw_text": user_text},
            "response_text": (
                "I could not determine the intended action. "
                "Try: list pipelines, check freshness, trigger run, "
                "generate connector, check status, list alerts."
            ),
        }

    # ------------------------------------------------------------------
    # conversational_response
    # ------------------------------------------------------------------

    async def conversational_response(
        self,
        user_text: str,
        action: str,
        result_data: dict,
        history: Optional[list[dict]] = None,
    ) -> str:
        """Generate a natural, data-engineer-style conversational response.

        Takes the raw action result and crafts a helpful, guided response
        that thinks like a senior data analytics engineer.
        """
        if not self.has_api:
            return result_data.get("fallback_text", "Done.")

        history_text = ""
        if history:
            history_lines = []
            for msg in history[-10:]:
                role = msg.get("role", "user")
                text = msg.get("text", "")
                history_lines.append(f"  {role}: {text}")
            history_text = "\n\nConversation so far:\n" + "\n".join(history_lines)

        system = (
            "You are a senior data analytics engineer embedded in DAPOS, an agentic data platform. "
            "You guide users through discovering, profiling, and ingesting data — like a helpful colleague "
            "sitting next to them.\n\n"

            "CORE PRINCIPLE: Ask, don't assume. A good data engineer confirms before acting.\n"
            "- NEVER assume default schemas, schedules, target databases, or table selections.\n"
            "- If the user says 'create a pipeline' but hasn't specified a schedule, ASK what frequency they need.\n"
            "- If you discover multiple schemas, ASK which one they want — don't just pick 'main'.\n"
            "- If they haven't told you the target schema (raw, staging, analytics), ASK where they want data to land.\n"
            "- If you see multiple tables, ASK which ones matter to them and why — don't assume all.\n"
            "- If connection details are missing, ask for each one specifically (host, port, db, user, password).\n"
            "- If you see data quality issues in a profile, flag them and ASK how the user wants to handle them "
            "(e.g. 'customer_id looks like the real PK, not plan — should I use customer_id as the merge key?').\n\n"

            "WHAT A GOOD DATA ENGINEER DOES:\n"
            "- Observes patterns: 'I see updated_at on this table — are you expecting CDC-style updates?'\n"
            "- Raises concerns: 'This table has no timestamp columns, so incremental won't work — full refresh OK?'\n"
            "- Offers choices: 'Do you want hourly, daily, or a custom cron schedule?'\n"
            "- Thinks about downstream: 'Will anything query this table directly, or is it feeding a transform layer?'\n"
            "- Confirms understanding: 'So to confirm — customers from SQLite into raw.customers on Postgres, hourly?'\n\n"

            "STYLE:\n"
            "- Be concise but warm. Use plain text, no markdown headers.\n"
            "- Format data (tables, columns, schemas) cleanly with indentation.\n"
            "- Never dump raw JSON.\n"
            "- Present 2-3 options when appropriate, with your recommendation and why.\n"
            "- End with a clear question, not an open-ended 'what would you like to do?'.\n"
        )

        user_prompt = f"""
The user said: "{user_text}"

Action executed: {action}
Result data:
{json.dumps(result_data, indent=2, default=str)}
{history_text}

Generate a natural conversational response as a senior data engineer would.
Ask specific clarifying questions rather than assuming defaults.
Keep it concise (3-8 lines). Do not use markdown headers.
Use • for lists if needed. End with a specific question.
"""
        try:
            return await self._call_claude(
                system, user_prompt,
                operation="conversational_response",
                temperature=0.3,
            )
        except Exception as e:
            log.warning("conversational_response error: %s", e)
            return result_data.get("fallback_text", "Done.")

    # ------------------------------------------------------------------
    # parse_schedule — plain English → cron
    # ------------------------------------------------------------------

    _SCHEDULE_MAP = {
        "every minute": "* * * * *",
        "every 5 minutes": "*/5 * * * *",
        "every 10 minutes": "*/10 * * * *",
        "every 15 minutes": "*/15 * * * *",
        "every 30 minutes": "*/30 * * * *",
        "every half hour": "*/30 * * * *",
        "hourly": "0 * * * *",
        "every hour": "0 * * * *",
        "every 2 hours": "0 */2 * * *",
        "every 3 hours": "0 */3 * * *",
        "every 4 hours": "0 */4 * * *",
        "every 6 hours": "0 */6 * * *",
        "every morning": "0 8 * * *",
        "every evening": "0 18 * * *",
        "every night": "0 22 * * *",
        "daily": "0 0 * * *",
        "once a day": "0 8 * * *",
        "twice a day": "0 8,20 * * *",
        "three times a day": "0 8,14,20 * * *",
        "every weekday": "0 8 * * 1-5",
        "every weekday morning": "0 8 * * 1-5",
        "weekly": "0 0 * * 1",
        "every monday": "0 8 * * 1",
        "every sunday": "0 8 * * 0",
        "monthly": "0 0 1 * *",
        "real-time": "*/5 * * * *",
        "near real-time": "*/5 * * * *",
        "as fresh as possible": "*/5 * * * *",
    }

    async def parse_schedule(self, text: str) -> dict:
        """Convert natural language schedule to cron expression.

        Returns {cron, description, parsed_from}.
        """
        import re
        text_clean = text.lower().strip().rstrip(".")

        # Direct cron expression pass-through
        if re.match(r"^[\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+$", text_clean):
            return {"cron": text_clean, "description": text_clean, "parsed_from": "passthrough"}

        # Rule-based matching
        for phrase, cron in self._SCHEDULE_MAP.items():
            if phrase in text_clean:
                return {"cron": cron, "description": phrase, "parsed_from": "rule"}

        # "every N minutes/hours" pattern
        m = re.search(r"every\s+(\d+)\s+minute", text_clean)
        if m:
            n = int(m.group(1))
            return {"cron": f"*/{n} * * * *", "description": f"every {n} minutes", "parsed_from": "rule"}
        m = re.search(r"every\s+(\d+)\s+hour", text_clean)
        if m:
            n = int(m.group(1))
            return {"cron": f"0 */{n} * * *", "description": f"every {n} hours", "parsed_from": "rule"}

        # Claude fallback for complex expressions
        if self.has_api:
            try:
                resp = await self._call_claude(
                    "You convert schedule descriptions to cron expressions. "
                    "Respond with ONLY valid JSON: {\"cron\": \"...\", \"description\": \"...\"}",
                    f"Convert this schedule to a cron expression: \"{text}\"",
                    operation="parse_schedule",
                    temperature=0.0,
                )
                result = self._extract_json(resp)
                result["parsed_from"] = "llm"
                return result
            except Exception as e:
                log.warning("Schedule parse LLM fallback failed: %s", e)

        # Default: hourly
        return {"cron": "0 * * * *", "description": "hourly (default)", "parsed_from": "default"}

    # ------------------------------------------------------------------
    # guided_pipeline_response — analyst-friendly conversational guidance
    # ------------------------------------------------------------------

    async def guided_pipeline_response(
        self,
        user_text: str,
        guided_context: dict,
        result_data: dict,
        available_sources: list[dict] = None,
        history: Optional[list[dict]] = None,
    ) -> str:
        """Generate an analyst-friendly response for guided pipeline creation.

        Uses a different system prompt that avoids jargon and provides
        proactive recommendations.
        """
        if not self.has_api:
            return result_data.get("fallback_text", "")

        ctx_summary = json.dumps(guided_context, indent=2, default=str)
        sources_text = ""
        if available_sources:
            lines = []
            for s in available_sources:
                lines.append(f"  • {s['display_name']} ({s['source_type']}) — {s.get('description', '')}")
            sources_text = f"\n\nAvailable data sources:\n" + "\n".join(lines)

        history_text = ""
        if history:
            history_lines = []
            for msg in history[-10:]:
                role = msg.get("role", "user")
                text = msg.get("text", "")
                history_lines.append(f"  {role}: {text}")
            history_text = "\n\nConversation so far:\n" + "\n".join(history_lines)

        system = f"""You are a friendly data assistant helping a user set up a data pipeline.
You speak in plain, non-technical language. Never use jargon like "cron", "merge keys",
"incremental extraction", "watermark", or "DDL" unless the user clearly has technical expertise.

Instead of technical terms, use plain equivalents:
- "refresh type: incremental" → "We'll only sync new and updated rows"
- "merge keys" → "unique identifier to match records"
- "cron schedule" → just describe the timing naturally ("every morning at 8am")
- "quality gate" → "data quality checks"
- "tier" → "priority level"

Current pipeline creation progress:
{ctx_summary}
{sources_text}
{history_text}

Based on what we know so far, guide the user to the next step. Be proactive:
- If we have enough info, PROPOSE a complete setup and ask for confirmation
- If something is missing, ask ONE clear question at a time
- When showing options, present them as a numbered list
- Recommend the best option and explain briefly WHY
- Skip steps when you can infer the answer from context
- Keep responses under 8 lines
"""
        user_prompt = f"""User said: "{user_text}"

Data gathered so far: {json.dumps(result_data, indent=2, default=str)}

Respond naturally. Guide toward the next step or propose the final pipeline if ready."""

        try:
            return await self._call_claude(
                system, user_prompt,
                operation="guided_pipeline_response",
                temperature=0.3,
            )
        except Exception as e:
            log.warning("guided_pipeline_response error: %s", e)
            return result_data.get("fallback_text", "Let me help you set up that pipeline.")

    # ------------------------------------------------------------------
    # Build 24: Diagnostic & Reasoning Layer
    # ------------------------------------------------------------------

    async def diagnose_pipeline(self, pipeline_id: str) -> dict:
        """Root-cause diagnosis for a pipeline. Gathers all context, reasons via Claude."""
        # Gather data
        p = await self.store.get_pipeline(pipeline_id)
        if not p:
            return {"error": "Pipeline not found", "root_cause": "unknown", "summary": "Pipeline not found"}

        runs = await self.store.list_runs(pipeline_id, limit=10)
        gates = await self.store.get_quality_trend(pipeline_id, limit=10)
        budget = await self.store.get_error_budget(pipeline_id)
        deps = await self.store.list_dependencies(pipeline_id)
        alerts = await self.store.list_alerts_for_pipeline(pipeline_id, limit=10)
        volume = await self.store.get_volume_history(pipeline_id, limit=10)

        # Upstream health
        upstream_info = []
        for dep in deps:
            up = await self.store.get_pipeline(dep.depends_on_id)
            up_runs = await self.store.list_runs(dep.depends_on_id, limit=3) if up else []
            upstream_info.append({
                "name": up.pipeline_name if up else dep.depends_on_id,
                "status": up.status.value if up and hasattr(up.status, "value") else "unknown",
                "recent_runs": [
                    {"status": r.status.value if hasattr(r.status, "value") else r.status, "error": r.error or ""}
                    for r in up_runs
                ],
            })

        # Source connector status
        connectors = await self.store.list_connectors()
        src_conn = next((c for c in connectors if c.connector_id == p.source_connector_id), None)

        if not self.has_api:
            return self._rule_based_diagnosis(p, runs, gates, budget, upstream_info, src_conn)

        # Format context for Claude
        runs_text = "\n".join(
            f"  {r.started_at}: {r.status.value if hasattr(r.status, 'value') else r.status}"
            f" | rows={r.rows_extracted} | error={r.error or 'none'}"
            for r in runs
        ) or "  No recent runs"

        gates_text = "\n".join(
            f"  {g.evaluated_at}: {g.decision.value if hasattr(g.decision, 'value') else g.decision}"
            for g in gates
        ) or "  No quality gate history"

        budget_text = "No error budget data"
        if budget:
            budget_text = (
                f"success_rate={budget.success_rate:.1%}, "
                f"budget_remaining={budget.budget_remaining:.3f}, "
                f"escalated={budget.escalated}, "
                f"window={budget.window_days}d, "
                f"{budget.failed_runs}/{budget.total_runs} failed"
            )

        upstream_text = "\n".join(
            f"  {u['name']}: status={u['status']}, recent=[{', '.join(r['status'] for r in u['recent_runs'])}]"
            for u in upstream_info
        ) or "  No upstream dependencies"

        alerts_text = "\n".join(
            f"  [{a.severity.value if hasattr(a.severity, 'value') else a.severity}] {a.summary}"
            for a in alerts[:5]
        ) or "  No recent alerts"

        volume_text = "\n".join(
            f"  {v.get('started_at', '?')}: {v.get('rows_extracted', 0)} rows"
            for v in volume[:10]
        ) or "  No volume history"

        user_prompt = f"""Diagnose this pipeline and identify the root cause of its issues.

Pipeline: {p.pipeline_name} ({p.pipeline_id[:8]})
Status: {p.status.value if hasattr(p.status, 'value') else p.status}
Schedule: {p.schedule_cron}
Source: {p.source_schema}.{p.source_table} (connector: {src_conn.connector_name if src_conn else 'unknown'}, status: {src_conn.status.value if src_conn and hasattr(src_conn.status, 'value') else 'unknown'})
Target: {p.target_schema}.{p.target_table}
Environment: {p.environment}

Recent runs (newest first):
{runs_text}

Quality gate trend:
{gates_text}

Error budget: {budget_text}

Upstream dependencies:
{upstream_text}

Recent alerts:
{alerts_text}

Volume history:
{volume_text}

Respond with JSON:
{{
  "root_cause": "concise description of the primary issue",
  "category": "source_issue|connector_issue|upstream_dependency|quality_regression|scheduling|configuration|data_issue|unknown",
  "confidence": 0.0-1.0,
  "evidence": ["list of specific evidence points"],
  "recommended_actions": [
    {{"action": "description", "priority": "critical|high|medium|low", "automated": true/false}}
  ],
  "upstream_health": "healthy|degraded|failing",
  "pattern_detected": "description of any recurring pattern or null",
  "summary": "2-3 sentence human-readable summary"
}}"""

        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=pipeline_id,
                operation="diagnose_pipeline",
                temperature=0.2,
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("diagnose_pipeline Claude error: %s. Using rule-based fallback.", e)
            return self._rule_based_diagnosis(p, runs, gates, budget, upstream_info, src_conn)

    def _rule_based_diagnosis(self, p, runs, gates, budget, upstream_info, src_conn) -> dict:
        """Simple heuristic diagnosis when no API key is available."""
        evidence = []
        category = "unknown"
        root_cause = "Unable to determine root cause without more data"

        # Check last run
        if runs:
            last = runs[0]
            status = last.status.value if hasattr(last.status, "value") else last.status
            if status == "failed" and last.error:
                evidence.append(f"Last run failed: {last.error}")
                if "connector" in last.error.lower() or "connection" in last.error.lower():
                    category = "connector_issue"
                    root_cause = f"Connection failure: {last.error}"
                elif "upstream" in last.error.lower():
                    category = "upstream_dependency"
                    root_cause = f"Upstream dependency issue: {last.error}"
                else:
                    category = "data_issue"
                    root_cause = last.error
            elif status == "halted":
                evidence.append("Last run halted by quality gate")
                category = "quality_regression"
                root_cause = "Quality gate halted the last run — data quality check failed"

            # Count recent failures
            fail_count = sum(1 for r in runs if (r.status.value if hasattr(r.status, "value") else r.status) in ("failed", "halted"))
            if fail_count > 3:
                evidence.append(f"{fail_count}/{len(runs)} recent runs failed/halted")

        # Check upstream
        for u in upstream_info:
            if u["status"] != "active":
                evidence.append(f"Upstream '{u['name']}' is {u['status']}")
                category = "upstream_dependency"
                root_cause = f"Upstream pipeline '{u['name']}' is {u['status']}"
            for r in u["recent_runs"]:
                if r["status"] in ("failed", "halted"):
                    evidence.append(f"Upstream '{u['name']}' has recent {r['status']}")

        # Check connector
        if src_conn:
            conn_status = src_conn.status.value if hasattr(src_conn.status, "value") else src_conn.status
            if conn_status != "active":
                evidence.append(f"Source connector '{src_conn.connector_name}' is {conn_status}")
                category = "connector_issue"
                root_cause = f"Source connector is {conn_status}"

        # Check error budget
        if budget and budget.escalated:
            evidence.append(f"Error budget exhausted: {budget.success_rate:.1%} success rate")

        return {
            "root_cause": root_cause,
            "category": category,
            "confidence": 0.5,
            "evidence": evidence or ["No specific evidence found — pipeline may be healthy"],
            "recommended_actions": [
                {"action": "Check pipeline logs and recent run errors", "priority": "high", "automated": False},
            ],
            "upstream_health": "unknown",
            "pattern_detected": None,
            "summary": f"Rule-based diagnosis for {p.pipeline_name}: {root_cause}",
        }

    async def analyze_impact(self, pipeline_id: str) -> dict:
        """Analyze downstream impact if a pipeline goes down."""
        p = await self.store.get_pipeline(pipeline_id)
        if not p:
            return {"error": "Pipeline not found", "impact_severity": "unknown"}

        downstream = await self.store.get_all_downstream_recursive(pipeline_id)
        contracts = await self.store.list_data_contracts(producer_id=pipeline_id)

        # Column lineage
        lineage = []
        try:
            all_lineage = await self.store.list_column_lineage(pipeline_id)
            lineage = [
                {"source": f"{l.source_column}", "target": f"{l.target_table}.{l.target_column}"}
                for l in all_lineage[:20]
            ]
        except Exception:
            pass

        if not self.has_api:
            return self._rule_based_impact(p, downstream, contracts, lineage)

        downstream_text = "\n".join(
            f"  [{d['depth']}] {d['pipeline_name']} (status={d['status']}, schedule={d['schedule_cron']}, tier={d['tier']})"
            for d in downstream
        ) or "  No downstream pipelines"

        contracts_text = "\n".join(
            f"  consumer={c.consumer_pipeline_id[:8]}, freshness_sla={c.freshness_sla_minutes}m, cleanup={c.cleanup_ownership.value if hasattr(c.cleanup_ownership, 'value') else c.cleanup_ownership}"
            for c in contracts
        ) or "  No data contracts as producer"

        lineage_text = "\n".join(
            f"  {l['source']} → {l['target']}" for l in lineage[:15]
        ) or "  No column lineage tracked"

        user_prompt = f"""Analyze the downstream impact if this pipeline goes down or becomes unavailable.

Pipeline: {p.pipeline_name} ({p.pipeline_id[:8]})
Target table: {p.target_schema}.{p.target_table}
Tier: {p.tier}
Schedule: {p.schedule_cron}

Downstream pipelines (transitive, [depth] name):
{downstream_text}

Data contracts (this pipeline is producer):
{contracts_text}

Column lineage from this pipeline:
{lineage_text}

Respond with JSON:
{{
  "impact_severity": "critical|high|medium|low|none",
  "affected_pipelines": [
    {{"pipeline_name": "...", "pipeline_id": "...", "depth": N, "impact_type": "direct|transitive", "sla_at_risk": true/false}}
  ],
  "affected_contracts": [
    {{"consumer": "...", "freshness_sla_minutes": N, "will_violate": true/false}}
  ],
  "blast_radius": {{"pipelines": N, "tables": N, "contracts": N}},
  "mitigation_options": [
    {{"option": "description", "effort": "low|medium|high"}}
  ],
  "summary": "2-3 sentence plain English summary"
}}"""

        try:
            text = await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=pipeline_id,
                operation="analyze_impact",
                temperature=0.2,
            )
            return self._extract_json(text)
        except Exception as e:
            log.warning("analyze_impact Claude error: %s. Using rule-based fallback.", e)
            return self._rule_based_impact(p, downstream, contracts, lineage)

    def _rule_based_impact(self, p, downstream, contracts, lineage) -> dict:
        """Heuristic impact analysis."""
        total = len(downstream)
        severity = "none"
        if total >= 5 or p.tier == 1:
            severity = "critical"
        elif total >= 3 or contracts:
            severity = "high"
        elif total >= 1:
            severity = "medium"

        return {
            "impact_severity": severity,
            "affected_pipelines": [
                {
                    "pipeline_name": d["pipeline_name"],
                    "pipeline_id": d["pipeline_id"],
                    "depth": d["depth"],
                    "impact_type": "direct" if d["depth"] == 1 else "transitive",
                    "sla_at_risk": True,
                }
                for d in downstream
            ],
            "affected_contracts": [
                {
                    "consumer": c.consumer_pipeline_id[:8],
                    "freshness_sla_minutes": c.freshness_sla_minutes,
                    "will_violate": True,
                }
                for c in contracts
            ],
            "blast_radius": {
                "pipelines": total,
                "tables": total,
                "contracts": len(contracts),
            },
            "mitigation_options": [
                {"option": "Investigate and fix the root cause", "effort": "medium"},
                {"option": "Pause downstream pipelines to prevent cascading failures", "effort": "low"},
            ],
            "summary": f"{p.pipeline_name} has {total} downstream pipeline(s) and {len(contracts)} data contract(s). "
                       f"Impact severity: {severity}.",
        }

    async def reason_about_anomalies(self) -> dict:
        """Platform-wide anomaly detection with per-pipeline agent evaluation.

        Instead of hardcoded thresholds (30% deviation, 2 failures), the agent
        evaluates each pipeline's signals in context to determine what's truly
        anomalous vs. normal variation.
        """
        from datetime import datetime, timezone

        pipelines = await self.store.list_pipelines(status="active")
        recent_failures = await self.store.list_recent_failures(hours=24)

        # Gather raw signals per pipeline — no hardcoded threshold filtering
        all_anomalies = []
        candidates = []
        for p in pipelines:
            volume = await self.store.get_volume_history(p.pipeline_id, limit=10)
            p_failures = [f for f in recent_failures if f.pipeline_id == p.pipeline_id]
            budget = await self.store.get_error_budget(p.pipeline_id)

            # Only send to agent if there's ANY signal to evaluate
            has_signal = (
                (len(volume) >= 3) or
                (len(p_failures) >= 1) or
                (budget and budget.budget_remaining < 0.1 and budget.total_runs > 0)
            )
            if not has_signal:
                continue

            budget_info = None
            if budget:
                budget_info = {
                    "success_rate": budget.success_rate,
                    "budget_remaining": budget.budget_remaining,
                    "total_runs": budget.total_runs,
                }

            candidates.append({
                "pipeline": p,
                "volume": volume,
                "failures": p_failures,
                "budget_info": budget_info,
            })

        # Agent evaluates each candidate — determines what's truly anomalous
        for candidate in candidates[:20]:  # Cap at 20
            p = candidate["pipeline"]
            try:
                evaluation = await self.evaluate_anomaly_signals(
                    pipeline_name=p.pipeline_name,
                    pipeline_id=p.pipeline_id,
                    tier=p.tier,
                    schedule_cron=p.schedule_cron,
                    volume_history=candidate["volume"],
                    recent_failures=candidate["failures"],
                    budget_info=candidate["budget_info"],
                )
                if evaluation.get("is_anomalous"):
                    for signal in evaluation.get("signals", []):
                        all_anomalies.append({
                            "pipeline_id": p.pipeline_id,
                            "pipeline_name": p.pipeline_name,
                            "anomaly_type": signal.get("type", "unknown"),
                            "severity": signal.get("severity", "warning"),
                            "observation": signal.get("observation", ""),
                            "reasoning": signal.get("reasoning", ""),
                            "is_expected": signal.get("is_expected", False),
                            "recommended_action": "Review based on agent assessment",
                        })
            except Exception as e:
                log.warning("Anomaly evaluation failed for %s: %s", p.pipeline_name, e)

        if not all_anomalies:
            return {
                "anomalies": [],
                "cross_pipeline_patterns": [],
                "platform_health": "healthy",
                "summary": f"All {len(pipelines)} active pipelines are operating normally. No anomalies detected.",
            }

        # Cross-pipeline pattern analysis (uses LLM if available)
        cross_patterns = []
        if self.has_api and len(all_anomalies) > 1:
            try:
                now = datetime.now(timezone.utc)
                anomaly_text = "\n".join(
                    f"- {a['pipeline_name']}: {a['anomaly_type']} ({a['severity']}) — {a['observation']}"
                    for a in all_anomalies[:15]
                )
                pattern_prompt = f"""Given these {len(all_anomalies)} anomalies across the platform (today is {now.strftime('%A %Y-%m-%d')}):
{anomaly_text}

Are there cross-pipeline patterns? (shared source, same connector type, same schedule, correlated failures)
Respond with JSON: {{"cross_pipeline_patterns": ["pattern 1", ...], "platform_health": "healthy|degraded|critical", "summary": "2-3 sentence assessment"}}"""
                pattern_result = self._extract_json(
                    await self._call_claude(self._system_prompt(), pattern_prompt, operation="cross_pipeline_patterns")
                )
                cross_patterns = pattern_result.get("cross_pipeline_patterns", [])
                health = pattern_result.get("platform_health", "degraded")
                summary = pattern_result.get("summary", "")
            except Exception:
                health = "critical" if any(a["severity"] == "critical" for a in all_anomalies) else "degraded"
                summary = f"{len(all_anomalies)} anomalies across {len(set(a['pipeline_id'] for a in all_anomalies))} pipelines."
        else:
            health = "critical" if any(a["severity"] == "critical" for a in all_anomalies) else "degraded"
            summary = f"{len(all_anomalies)} anomalies detected. {len(recent_failures)} failures in last 24h."

        return {
            "anomalies": all_anomalies,
            "cross_pipeline_patterns": cross_patterns,
            "platform_health": health,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # generate_anomaly_narrative (Build 26)
    # ------------------------------------------------------------------

    async def generate_anomaly_narrative(
        self,
        pipeline_name: str,
        alert_summary: str,
        alert_detail: dict,
        severity: str,
        tier: int,
        downstream_count: int = 0,
        recent_run_errors: Optional[list[str]] = None,
        freshness_info: Optional[dict] = None,
        schedule_cron: str = "",
    ) -> str:
        """Generate a human-readable anomaly narrative with root cause and impact.

        Returns a 2-4 sentence narrative explaining what happened, why, and what's affected.
        """
        recent_run_errors = recent_run_errors or []

        if not self.has_api:
            return self._rule_based_narrative(
                pipeline_name, alert_summary, alert_detail, severity,
                tier, downstream_count, recent_run_errors, freshness_info, schedule_cron,
            )

        context_parts = [f"Alert: {alert_summary}"]
        if alert_detail:
            context_parts.append(f"Detail: {json.dumps(alert_detail)}")
        if recent_run_errors:
            context_parts.append(f"Recent errors: {'; '.join(recent_run_errors[:3])}")
        if freshness_info:
            context_parts.append(f"Freshness: {json.dumps(freshness_info)}")
        if downstream_count > 0:
            context_parts.append(f"Downstream pipelines affected: {downstream_count}")

        prompt = f"""Write a concise anomaly narrative (2-4 sentences) for a data pipeline alert.

Pipeline: {pipeline_name} (tier {tier}, schedule: {schedule_cron or 'unset'})
Severity: {severity}
{chr(10).join(context_parts)}

The narrative should:
1. State what happened in plain language
2. Explain the likely root cause
3. Describe downstream impact (if any)
4. Suggest expected recovery timeline

Write as a single paragraph. Be specific, not generic. Do not use JSON."""

        try:
            resp = await self._call_claude(self._system_prompt(), prompt)
            text = resp if isinstance(resp, str) else str(resp)
            # Clean up — just return the text, no JSON
            text = text.strip().strip('"')
            if text and len(text) > 20:
                return text
        except Exception as e:
            log.warning("Narrative generation failed: %s", e)

        return self._rule_based_narrative(
            pipeline_name, alert_summary, alert_detail, severity,
            tier, downstream_count, recent_run_errors, freshness_info, schedule_cron,
        )

    def _rule_based_narrative(
        self,
        pipeline_name: str,
        alert_summary: str,
        alert_detail: dict,
        severity: str,
        tier: int,
        downstream_count: int,
        recent_run_errors: list[str],
        freshness_info: Optional[dict],
        schedule_cron: str,
    ) -> str:
        """Generate a narrative without AI."""
        parts = [f"{pipeline_name}: {alert_summary}."]

        # Root cause hint
        if recent_run_errors:
            first_error = recent_run_errors[0]
            if "connection" in first_error.lower() or "refused" in first_error.lower():
                parts.append(f"Root cause: source database connection failure ({first_error[:80]}).")
            elif "timeout" in first_error.lower():
                parts.append("Root cause: query or connection timeout — likely a slow source or network issue.")
            elif "permission" in first_error.lower() or "denied" in first_error.lower():
                parts.append("Root cause: authentication or permission error on the source system.")
            else:
                parts.append(f"Last error: {first_error[:100]}.")

        # Freshness context
        if freshness_info:
            staleness = freshness_info.get("staleness_minutes", 0)
            if staleness > 0:
                hours = staleness / 60
                if hours >= 1:
                    parts.append(f"Data is {hours:.1f} hours stale.")
                else:
                    parts.append(f"Data is {staleness:.0f} minutes stale.")

        # Downstream impact
        if downstream_count > 0:
            parts.append(f"This affects {downstream_count} downstream pipeline{'s' if downstream_count > 1 else ''}.")

        # Recovery hint
        if schedule_cron:
            parts.append(f"Next scheduled run: {schedule_cron}.")

        return " ".join(parts)

    # ------------------------------------------------------------------
    # infer_semantic_tags (Build 26)
    # ------------------------------------------------------------------

    async def infer_semantic_tags(
        self,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        columns: list[dict],
        existing_tags: Optional[dict] = None,
    ) -> dict:
        """Infer semantic tags for columns using AI.

        Returns a dict keyed by column name with semantic metadata.
        Preserves any user-overridden tags from existing_tags.
        """
        existing_tags = existing_tags or {}

        # Skip columns already tagged by user
        columns_to_infer = []
        for col in columns:
            name = col.get("target_column") or col.get("source_column") or col.get("name", "")
            existing = existing_tags.get(name, {})
            if existing.get("source") == "user":
                continue  # Don't overwrite user-set tags
            columns_to_infer.append(col)

        if not columns_to_infer:
            return existing_tags

        col_descriptions = "\n".join(
            f"  - {c.get('target_column') or c.get('source_column') or c.get('name', '?')}: "
            f"{c.get('target_type') or c.get('source_type') or c.get('type', 'unknown')} "
            f"(nullable={c.get('is_nullable', c.get('nullable', True))}, pk={c.get('is_primary_key', c.get('primary_key', False))})"
            for c in columns_to_infer
        )

        if not self.has_api:
            # Rule-based fallback
            return self._infer_tags_rule_based(columns_to_infer, existing_tags)

        prompt = f"""Analyze these database columns and infer their business semantics.

Pipeline: {pipeline_name}
Source table: {source_table}
Target table: {target_table}

Columns:
{col_descriptions}

For each column, provide semantic metadata. Respond with JSON only:
{{
  "column_name": {{
    "semantic_name": "human-readable business name (e.g., monthly_recurring_revenue)",
    "domain": "business domain (e.g., finance, marketing, product, operations, identity, temporal)",
    "description": "one-line description of what this column represents",
    "pii": true/false,
    "unit": "unit if applicable (e.g., USD, count, percent, seconds) or null"
  }}
}}

Guidelines:
- semantic_name should be snake_case, descriptive, business-oriented
- PII includes: email, phone, name, address, SSN, IP address, geolocation
- domain should be one of: finance, marketing, product, operations, identity, temporal, technical, geography
- Be concise but accurate in descriptions
"""
        try:
            resp = await self._call_claude(self._system_prompt(), prompt)
            text = resp if isinstance(resp, str) else str(resp)
            parsed = self._extract_json(text)
            if isinstance(parsed, dict):
                result = dict(existing_tags)
                for col_name, tags in parsed.items():
                    if isinstance(tags, dict):
                        tags["source"] = "ai"
                        result[col_name] = tags
                return result
        except Exception as e:
            log.warning("Semantic tag inference failed: %s", e)

        return self._infer_tags_rule_based(columns_to_infer, existing_tags)

    def _infer_tags_rule_based(self, columns: list[dict], existing_tags: dict) -> dict:
        """Fallback: infer basic semantic tags from column names without AI."""
        PII_PATTERNS = {"email", "phone", "ssn", "address", "first_name", "last_name",
                        "name", "ip_address", "ip", "zip", "postal", "dob", "birth"}
        TEMPORAL_PATTERNS = {"created_at", "updated_at", "deleted_at", "timestamp",
                             "date", "time", "shipped_at", "delivered_at", "started_at",
                             "completed_at", "checked_at", "expires_at"}
        FINANCE_PATTERNS = {"price", "cost", "amount", "total", "subtotal", "tax",
                            "shipping", "revenue", "fee", "balance", "payment", "charge"}
        ID_PATTERNS = {"id", "uuid", "key", "code", "sku", "number"}

        result = dict(existing_tags)
        for col in columns:
            name = col.get("target_column") or col.get("source_column") or col.get("name", "")
            if not name or result.get(name, {}).get("source") == "user":
                continue

            name_lower = name.lower()
            words = set(name_lower.replace("-", "_").split("_"))

            # Detect domain and PII
            is_pii = bool(words & PII_PATTERNS)
            if words & TEMPORAL_PATTERNS or name_lower.endswith("_at") or name_lower.endswith("_date"):
                domain = "temporal"
            elif words & FINANCE_PATTERNS:
                domain = "finance"
            elif words & PII_PATTERNS:
                domain = "identity"
            elif words & ID_PATTERNS:
                domain = "technical"
            else:
                domain = "operations"

            # Generate semantic name
            semantic = name_lower.strip("_")

            # Unit inference
            unit = None
            if words & {"price", "cost", "amount", "total", "subtotal", "tax", "shipping", "fee", "charge"}:
                unit = "currency"
            elif name_lower.endswith("_pct") or name_lower.endswith("_percent"):
                unit = "percent"
            elif name_lower.endswith("_count") or name_lower.endswith("_qty"):
                unit = "count"
            elif name_lower.endswith("_seconds") or name_lower.endswith("_ms"):
                unit = "seconds" if "_seconds" in name_lower else "milliseconds"

            result[name] = {
                "semantic_name": semantic,
                "domain": domain,
                "description": f"Column {name} from source table",
                "pii": is_pii,
                "unit": unit,
                "source": "ai",
            }

        return result

    # ------------------------------------------------------------------
    # generate_business_context_questions (Build 26)
    # ------------------------------------------------------------------

    async def generate_business_context_questions(
        self,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        columns: list[dict],
    ) -> list[dict]:
        """Generate targeted questions to capture business context for a pipeline.

        Returns a list of questions with multiple-choice answers.
        """
        if not self.has_api:
            return self._default_context_questions(pipeline_name)

        col_names = ", ".join(
            c.get("target_column") or c.get("source_column") or c.get("name", "?")
            for c in (columns or [])[:20]
        )

        prompt = f"""You are helping a user document the business context of a data pipeline.

Pipeline: {pipeline_name}
Source: {source_table}
Target: {target_table}
Columns: {col_names}

Generate 3-5 targeted questions to understand the business context. Each question should have 4-5 multiple choice options.

Focus on:
1. What business process/function this data supports
2. Who consumes this data and for what purpose
3. How critical this data is (what decisions depend on it)
4. Expected update frequency from a business perspective

Respond with JSON only:
[
  {{
    "id": "business_process",
    "question": "What business function does this data primarily support?",
    "options": ["Option A", "Option B", "Option C", "Option D"],
    "allow_custom": true
  }}
]
"""
        try:
            resp = await self._call_claude(self._system_prompt(), prompt)
            text = resp if isinstance(resp, str) else str(resp)
            parsed = self._extract_json(text)
            if isinstance(parsed, list) and parsed:
                return parsed
        except Exception as e:
            log.warning("Context question generation failed: %s", e)

        return self._default_context_questions(pipeline_name)

    def _default_context_questions(self, pipeline_name: str) -> list[dict]:
        """Fallback: standard business context questions."""
        return [
            {
                "id": "business_process",
                "question": "What business function does this data support?",
                "options": [
                    "Revenue & billing",
                    "Customer analytics",
                    "Operations & logistics",
                    "Marketing & growth",
                    "Product usage & telemetry",
                    "Compliance & audit",
                ],
                "allow_custom": True,
            },
            {
                "id": "consumers",
                "question": "Who primarily consumes this data?",
                "options": [
                    "Executive dashboards",
                    "Data science / ML models",
                    "Business analysts",
                    "Downstream pipelines",
                    "Customer-facing product",
                    "Finance / accounting",
                ],
                "allow_custom": True,
            },
            {
                "id": "criticality",
                "question": "How critical is this data for decision-making?",
                "options": [
                    "Mission critical — outage blocks business operations",
                    "High — daily decisions depend on it",
                    "Medium — weekly reporting and analysis",
                    "Low — nice to have, exploratory",
                ],
                "allow_custom": False,
            },
            {
                "id": "freshness_expectation",
                "question": "How fresh does this data need to be?",
                "options": [
                    "Real-time (< 5 minutes)",
                    "Near real-time (< 1 hour)",
                    "Daily",
                    "Weekly or less frequent",
                ],
                "allow_custom": False,
            },
        ]

    # ------------------------------------------------------------------
    # generate_transform_sql (Build 29)
    # ------------------------------------------------------------------

    async def generate_transform_sql(
        self,
        description: str,
        available_tables: list[dict],
        materialization: str = "table",
        target_table: str = "",
    ) -> dict:
        """Generate SQL transform from natural language description.

        Args:
            description: What the transform should do
            available_tables: List of {schema, table, columns: [{name, type}]}
            materialization: table, view, incremental, ephemeral
            target_table: Desired output table name

        Returns dict with sql, target_table, description, refs, variables.
        """
        tables_desc = []
        for t in available_tables[:20]:
            cols = ", ".join(
                f"{c['name']} ({c.get('type', '?')})"
                for c in (t.get("columns") or [])[:30]
            )
            tables_desc.append(f"  - {t.get('schema', 'public')}.{t['table']}: {cols}")

        tables_text = "\n".join(tables_desc) if tables_desc else "  (no tables available)"

        prompt = f"""Generate a SQL transform for PostgreSQL based on this description:

Description: {description}

Available tables (use {{{{ ref('table_name') }}}} to reference them):
{tables_text}

Materialization: {materialization}
{f'Target table name: {target_table}' if target_table else 'Suggest a good target table name.'}

Respond with ONLY a JSON object (no markdown):
{{
    "sql": "SELECT ... FROM {{{{ ref('table_name') }}}} ...",
    "target_table": "output_table_name",
    "description": "Brief description of what this transform does",
    "refs": ["table_name1", "table_name2"],
    "variables": {{}},
    "unique_key": []
}}

Rules:
- Use {{{{ ref('table') }}}} syntax to reference source tables (not raw table names)
- Use {{{{ var('key') }}}} for configurable parameters
- For incremental, include a unique_key array
- Write clean, well-commented SQL
- Use CTEs for clarity on complex transforms"""

        if not self.has_api:
            return self._fallback_transform_sql(description, target_table)

        try:
            resp = await self._call_claude(self._system_prompt(), prompt)
            text = resp if isinstance(resp, str) else str(resp)
            # Extract JSON from response
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            result = json.loads(text)
            if "sql" in result:
                return result
        except Exception as e:
            log.warning("Transform SQL generation failed: %s", e)

        return self._fallback_transform_sql(description, target_table)

    def _fallback_transform_sql(self, description: str, target_table: str = "") -> dict:
        """Simple fallback when no API key available."""
        name = target_table or "transform_output"
        return {
            "sql": f"-- TODO: Implement transform\n-- Description: {description}\nSELECT 1 AS placeholder",
            "target_table": name,
            "description": description,
            "refs": [],
            "variables": {},
            "unique_key": [],
        }
