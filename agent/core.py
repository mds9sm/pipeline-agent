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
    TableProfile, PipelineContract, CheckResult, AgentPreference,
    ContractChangeProposal, ConnectorRecord, AlertRecord, AgentCostLog,
    ConnectorType, ConnectorStatus, TestStatus, ChangeType, TriggerType,
    ProposalStatus, PreferenceScope, PreferenceSource,
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
    # reason_about_quality
    # ------------------------------------------------------------------

    async def reason_about_quality(
        self,
        contract: PipelineContract,
        checks: list[CheckResult],
        decision: str,
    ) -> str:
        """Generate natural language explanation of gate results."""
        if not self.has_api:
            failed = [c for c in checks if c.status.value == "fail"]
            warned = [c for c in checks if c.status.value == "warn"]
            parts = [f"Gate decision: {decision}."]
            if failed:
                parts.append(f"Failed checks: {[c.check_name for c in failed]}.")
            if warned:
                parts.append(f"Warning checks: {[c.check_name for c in warned]}.")
            return " ".join(parts)

        checks_summary = "\n".join(
            f"- {c.check_name}: {c.status.value} -- {c.detail}"
            for c in checks
        )
        user_prompt = f"""
Pipeline: {contract.pipeline_name}
Gate decision: {decision}
Quality check results:
{checks_summary}

Provide a concise (2-4 sentence) natural language analysis:
1. What failed or warned and the likely root cause
2. What action the data engineering team should take
3. Whether this looks like a source issue, pipeline issue, or expected behavior

Respond with plain text (no JSON).
"""
        try:
            return await self._call_claude(
                self._system_prompt(), user_prompt,
                pipeline_id=contract.pipeline_id,
                operation="reason_about_quality",
                temperature=0.2,
            )
        except Exception as e:
            log.warning("Claude API error in reason_about_quality: %s", e)
            return f"Gate decision: {decision}. API reasoning unavailable."

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

        Returns dict with keys: action, params, response_text.
        Falls back to keyword extraction when no API key.
        """
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
            return result
        except Exception as e:
            log.warning("route_command Claude error: %s. Using keyword fallback.", e)
            return self._keyword_route(user_text, context)

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
