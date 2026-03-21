"""
Monitor engine -- schema drift detection, freshness monitoring,
column-level lineage impact analysis, and multi-channel alert dispatch
(Slack, Email, PagerDuty).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import smtplib
from dataclasses import asdict
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional

import httpx

from config import Config
from contracts.models import (
    PipelineContract, ContractChangeProposal, SchemaVersion,
    FreshnessSnapshot, AlertRecord, ColumnMapping, DecisionLog,
    ContractViolation,
    FreshnessStatus, AlertSeverity, TriggerType, ChangeType,
    ProposalStatus, ConnectorStatus, PipelineStatus, TIER_DEFAULTS,
    DataContractStatus, ContractViolationType, CleanupOwnership,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from agent.core import AgentCore
from crypto import decrypt_dict, CREDENTIAL_FIELDS
from logging_config import PipelineContext

log = logging.getLogger(__name__)


class MonitorEngine:
    """Monitor with column-level lineage impact analysis and PagerDuty."""

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
        self.tick_seconds = 300  # 5 minutes
        self._stop = False

    async def run_forever(self) -> None:
        """Main monitor loop -- ticks every 5 minutes."""
        log.info("Monitor started (tick=%ds)", self.tick_seconds)
        while not self._stop:
            try:
                await self._tick()
            except Exception as e:
                log.exception("Monitor tick error: %s", e)
            await asyncio.sleep(self.tick_seconds)

    def stop(self) -> None:
        self._stop = True

    async def _tick(self) -> None:
        """Check drift, freshness, and data contracts for all active pipelines."""
        pipelines = await self.store.list_pipelines(status="active")
        for pipeline in pipelines:
            with PipelineContext(pipeline.pipeline_id, pipeline.pipeline_name, component="monitor"):
                try:
                    await self._check_drift(pipeline)
                except Exception as e:
                    log.warning("Drift check error: %s", e)
                try:
                    await self._check_freshness(pipeline)
                except Exception as e:
                    log.warning("Freshness check error: %s", e)

        # Data contract validation (Build 16)
        try:
            await self._check_data_contracts()
        except Exception as e:
            log.warning("Data contract check error: %s", e)

    # ------------------------------------------------------------------
    # Schema drift detection
    # ------------------------------------------------------------------

    async def _check_drift(self, pipeline: PipelineContract) -> None:
        """Detect schema drift by comparing live source profile to contract.

        On drift: analyze with agent, query downstream column lineage for
        impact analysis, auto-apply or create proposal.
        """
        connectors = await self.store.list_connectors()
        src_conn = None
        for c in connectors:
            if c.connector_id == pipeline.source_connector_id:
                src_conn = c
                break

        if not src_conn or src_conn.status != ConnectorStatus.ACTIVE:
            return

        src_params = self._source_params(pipeline)
        source = await self.registry.get_source(
            pipeline.source_connector_id, src_params,
        )

        try:
            profile = await source.profile_table(
                pipeline.source_schema, pipeline.source_table,
            )
        except Exception as e:
            log.warning("Could not profile source table: %s", e)
            return

        current_cols = {m.source_column: m for m in pipeline.column_mappings}
        live_cols = {m.source_column: m for m in profile.columns}

        new_columns = [
            {
                "name": c,
                "type": live_cols[c].source_type,
                "nullable": live_cols[c].is_nullable,
            }
            for c in live_cols
            if c not in current_cols
        ]
        dropped_columns = [c for c in current_cols if c not in live_cols]
        type_changes = [
            {
                "column": c,
                "from": current_cols[c].source_type,
                "to": live_cols[c].source_type,
            }
            for c in live_cols
            if c in current_cols
            and live_cols[c].source_type != current_cols[c].source_type
        ]

        if not new_columns and not dropped_columns and not type_changes:
            return

        drift_info = {
            "new_columns": new_columns,
            "dropped_columns": dropped_columns,
            "type_changes": type_changes,
        }
        log.info("Drift detected: %s", drift_info)

        # Load relevant preferences
        prefs = (
            await self.store.get_preferences("pipeline", scope_value=pipeline.pipeline_id)
            + await self.store.get_preferences("global")
        )
        analysis = await self.agent.analyze_drift(pipeline, drift_info, prefs)

        # ---- Column-level impact analysis ----
        # For each affected column (dropped or type-changed), query downstream
        # lineage to show what breaks.
        affected_columns = (
            dropped_columns
            + [tc["column"] for tc in type_changes]
        )
        downstream_impact: list[dict] = []
        for col_name in affected_columns:
            try:
                downstream = await self.store.get_downstream_columns(
                    pipeline.pipeline_id,
                    pipeline.source_schema,
                    pipeline.source_table,
                    col_name,
                )
                for dc in downstream:
                    downstream_impact.append({
                        "affected_column": col_name,
                        "downstream_pipeline_id": dc.target_pipeline_id,
                        "downstream_schema": dc.target_schema,
                        "downstream_table": dc.target_table,
                        "downstream_column": dc.target_column,
                        "transformation": dc.transformation,
                    })
            except Exception as e:
                log.warning(
                    "Failed to query downstream lineage for column %s: %s",
                    col_name, e,
                )

        impact_analysis = {
            "breaking_change": analysis.get("breaking_change", False),
            "data_loss_risk": analysis.get("data_loss_risk", "unknown"),
            "estimated_backfill_time": analysis.get("estimated_backfill_time"),
            "downstream_column_impact": downstream_impact,
            "affected_column_count": len(affected_columns),
            "downstream_table_count": len(
                {d["downstream_table"] for d in downstream_impact}
            ),
        }

        # Policy-driven drift handling (Build 12)
        policy = pipeline.get_schema_policy()
        safe_type_changes = [tc for tc in type_changes if self._is_safe_type_widening(tc["from"], tc["to"])]
        breaking_type_changes = [tc for tc in type_changes if not self._is_safe_type_widening(tc["from"], tc["to"])]

        # Detect nullable changes
        nullable_changes = self._detect_nullable_changes(pipeline, profile)

        actions_to_apply = []   # (category, items)
        proposals_to_create = []  # (change_type, detail_dict)
        halt_reasons = []

        # --- New columns ---
        if new_columns:
            if policy.on_new_column == "auto_add":
                actions_to_apply.append(("new_columns", new_columns))
            elif policy.on_new_column == "propose":
                proposals_to_create.append((ChangeType.ADD_COLUMN, {"new_columns": new_columns}))
            # "ignore": do nothing

        # --- Dropped columns ---
        if dropped_columns:
            if policy.on_dropped_column == "halt":
                halt_reasons.append(f"Column(s) dropped: {dropped_columns}")
            elif policy.on_dropped_column == "propose":
                proposals_to_create.append((ChangeType.DROP_COLUMN, {"dropped_columns": dropped_columns}))
            # "ignore": do nothing

        # --- Type changes ---
        if safe_type_changes:
            if policy.on_type_change == "auto_widen":
                actions_to_apply.append(("safe_type_changes", safe_type_changes))
            elif policy.on_type_change == "propose":
                proposals_to_create.append((ChangeType.ALTER_COLUMN_TYPE, {"type_changes": safe_type_changes}))
            elif policy.on_type_change == "halt":
                halt_reasons.append(f"Type change(s): {[tc['column'] for tc in safe_type_changes]}")

        if breaking_type_changes:
            if policy.on_type_change == "halt":
                halt_reasons.append(f"Breaking type change(s): {[tc['column'] for tc in breaking_type_changes]}")
            else:
                proposals_to_create.append((ChangeType.ALTER_COLUMN_TYPE, {"type_changes": breaking_type_changes}))

        # --- Nullable changes ---
        if nullable_changes:
            if policy.on_nullable_change == "auto_accept":
                actions_to_apply.append(("nullable_changes", nullable_changes))
            elif policy.on_nullable_change == "propose":
                proposals_to_create.append((ChangeType.ALTER_COLUMN_TYPE, {"nullable_changes": nullable_changes}))
            elif policy.on_nullable_change == "halt":
                halt_reasons.append(f"Nullable change(s): {[nc['column'] for nc in nullable_changes]}")

        # --- Execute decisions ---
        if halt_reasons:
            await self._create_halt_proposal(pipeline, drift_info, impact_analysis, analysis, halt_reasons)
            return

        if actions_to_apply:
            auto_new = [item for cat, items in actions_to_apply if cat == "new_columns" for item in items]
            auto_type = [item for cat, items in actions_to_apply if cat == "safe_type_changes" for item in items]
            auto_nullable = [item for cat, items in actions_to_apply if cat == "nullable_changes" for item in items]
            await self._auto_apply_schema_changes(pipeline, auto_new, auto_type, auto_nullable)

            # Downstream propagation
            if policy.propagate_to_downstream and actions_to_apply:
                await self._propagate_schema_downstream(pipeline, actions_to_apply)

        if proposals_to_create:
            await self._create_drift_proposals(pipeline, proposals_to_create, drift_info, impact_analysis, analysis)

        # Log decision
        await self.store.save_decision(DecisionLog(
            pipeline_id=pipeline.pipeline_id,
            decision_type="drift_detected",
            detail=json.dumps(drift_info),
            reasoning=analysis.get("reasoning", ""),
        ))

    @staticmethod
    def _detect_nullable_changes(pipeline: PipelineContract, profile) -> list[dict]:
        """Detect columns where nullable status changed."""
        current_cols = {m.source_column: m for m in pipeline.column_mappings}
        live_cols = {m.source_column: m for m in profile.columns}
        changes = []
        for col_name in live_cols:
            if col_name in current_cols:
                if live_cols[col_name].is_nullable != current_cols[col_name].is_nullable:
                    changes.append({
                        "column": col_name,
                        "from_nullable": current_cols[col_name].is_nullable,
                        "to_nullable": live_cols[col_name].is_nullable,
                    })
        return changes

    @staticmethod
    def _is_safe_type_widening(from_type: str, to_type: str) -> bool:
        """Check if a type change is a safe widening that won't lose data."""
        f = from_type.upper().strip()
        t = to_type.upper().strip()

        # VARCHAR(N) -> VARCHAR(M) where M > N
        vm_from = re.match(r"VARCHAR\((\d+)\)", f)
        vm_to = re.match(r"VARCHAR\((\d+)\)", t)
        if vm_from and vm_to and int(vm_to.group(1)) > int(vm_from.group(1)):
            return True

        # Integer widening
        int_widening = {
            "SMALLINT": {"INT", "INTEGER", "BIGINT"},
            "INT": {"BIGINT"},
            "INTEGER": {"BIGINT"},
        }
        if f in int_widening and t in int_widening.get(f, set()):
            return True

        # Float widening
        float_widening = {
            "FLOAT": {"DOUBLE PRECISION", "DOUBLE"},
            "REAL": {"DOUBLE PRECISION", "DOUBLE"},
        }
        if f in float_widening and t in float_widening.get(f, set()):
            return True

        return False

    async def _auto_apply_schema_changes(
        self,
        pipeline: PipelineContract,
        new_columns: list[dict],
        safe_type_changes: list[dict] | None = None,
        nullable_changes: list[dict] | None = None,
    ) -> None:
        """Auto-apply new columns, safe type widenings, and nullable changes."""
        # Re-profile to get full ColumnMapping objects
        src_params = self._source_params(pipeline)
        source = await self.registry.get_source(
            pipeline.source_connector_id, src_params,
        )
        profile = await source.profile_table(
            pipeline.source_schema, pipeline.source_table,
        )
        live_cols = {m.source_column: m for m in profile.columns}

        # Append new columns
        for col_info in new_columns:
            col_name = col_info["name"]
            if col_name in live_cols:
                pipeline.column_mappings.append(live_cols[col_name])

        # Update types for safe widenings
        if safe_type_changes:
            for tc in safe_type_changes:
                col_name = tc["column"]
                for mapping in pipeline.column_mappings:
                    if mapping.source_column == col_name and col_name in live_cols:
                        mapping.source_type = live_cols[col_name].source_type
                        mapping.target_type = live_cols[col_name].target_type
                        break

        # Apply nullable changes
        if nullable_changes:
            for nc in nullable_changes:
                col_name = nc["column"]
                for mapping in pipeline.column_mappings:
                    if mapping.source_column == col_name:
                        mapping.is_nullable = nc["to_nullable"]
                        break

        changes_desc = []
        if new_columns:
            changes_desc.append(f"{len(new_columns)} new column(s): {[c['name'] for c in new_columns]}")
        if safe_type_changes:
            widening_list = [
                tc["column"] + ": " + tc["from"] + " -> " + tc["to"]
                for tc in safe_type_changes
            ]
            changes_desc.append(
                f"{len(safe_type_changes)} type widening(s): {widening_list}"
            )
        if nullable_changes:
            changes_desc.append(
                f"{len(nullable_changes)} nullable change(s): {[nc['column'] for nc in nullable_changes]}"
            )

        pipeline.version += 1
        pipeline.updated_at = now_iso()
        await self.store.save_pipeline(pipeline)

        sv = SchemaVersion(
            pipeline_id=pipeline.pipeline_id,
            version=pipeline.version,
            column_mappings=pipeline.column_mappings,
            change_summary=f"Auto-applied: {'; '.join(changes_desc)}",
            change_type="add_column" if new_columns and not safe_type_changes else "alter_column_type" if safe_type_changes else "add_column",
            applied_by="agent",
        )
        await self.store.save_schema_version(sv)
        log.info(
            "Auto-applied schema changes (v%d): %s",
            pipeline.version, "; ".join(changes_desc),
        )

    async def _create_halt_proposal(
        self, pipeline, drift_info, impact_analysis, analysis, halt_reasons,
    ):
        """Create a proposal and critical alert for halted schema changes."""
        proposal = ContractChangeProposal(
            pipeline_id=pipeline.pipeline_id,
            trigger_type=TriggerType.SCHEMA_DRIFT,
            trigger_detail={**drift_info, "halt_reasons": halt_reasons},
            change_type=ChangeType.DROP_COLUMN if "dropped" in str(halt_reasons) else ChangeType.ALTER_COLUMN_TYPE,
            current_state={"column_mappings": [asdict(m) for m in pipeline.column_mappings]},
            proposed_state={},
            reasoning=f"Policy halt: {'; '.join(halt_reasons)}. {analysis.get('reasoning', '')}",
            confidence=analysis.get("confidence", 0.5),
            impact_analysis=impact_analysis,
            rollback_plan=analysis.get("rollback_plan", ""),
            contract_version_before=pipeline.version,
        )
        await self.store.save_proposal(proposal)

        alert = AlertRecord(
            severity=AlertSeverity.CRITICAL,
            tier=pipeline.tier,
            pipeline_id=pipeline.pipeline_id,
            pipeline_name=pipeline.pipeline_name,
            summary=f"Schema change HALTED by policy: {'; '.join(halt_reasons)}",
            detail=drift_info,
        )
        await self.store.save_alert(alert)
        await self._dispatch_alert(alert, pipeline)
        log.warning("Schema change HALTED by policy for %s: %s", pipeline.pipeline_name, halt_reasons)

    async def _create_drift_proposals(
        self, pipeline, proposals_to_create, drift_info, impact_analysis, analysis,
    ):
        """Create proposals for schema changes that require human approval."""
        for change_type, detail in proposals_to_create:
            proposal = ContractChangeProposal(
                pipeline_id=pipeline.pipeline_id,
                trigger_type=TriggerType.SCHEMA_DRIFT,
                trigger_detail={**drift_info, "policy_action": "propose", "specific_changes": detail},
                change_type=change_type,
                current_state={"column_mappings": [asdict(m) for m in pipeline.column_mappings]},
                proposed_state=detail,
                reasoning=analysis.get("reasoning", ""),
                confidence=analysis.get("confidence", 0.5),
                impact_analysis=impact_analysis,
                rollback_plan=analysis.get("rollback_plan", ""),
                contract_version_before=pipeline.version,
            )
            await self.store.save_proposal(proposal)

        alert = AlertRecord(
            severity=AlertSeverity.WARNING,
            tier=pipeline.tier,
            pipeline_id=pipeline.pipeline_id,
            pipeline_name=pipeline.pipeline_name,
            summary=f"Schema changes require approval: {len(proposals_to_create)} proposal(s) created.",
            detail=drift_info,
        )
        await self.store.save_alert(alert)
        await self._dispatch_alert(alert, pipeline)

    async def _propagate_schema_downstream(
        self, pipeline: PipelineContract, applied_changes: list,
    ) -> None:
        """When schema changes are auto-applied, create proposals for
        downstream pipelines to update their schemas."""
        try:
            dependents = await self.store.list_dependents(pipeline.pipeline_id)
            if not dependents:
                return

            for dep in dependents:
                downstream = await self.store.get_pipeline(dep.pipeline_id)
                if not downstream or downstream.status != PipelineStatus.ACTIVE:
                    continue

                proposal = ContractChangeProposal(
                    pipeline_id=downstream.pipeline_id,
                    trigger_type=TriggerType.SCHEMA_DRIFT,
                    trigger_detail={
                        "propagated_from": pipeline.pipeline_id,
                        "propagated_from_name": pipeline.pipeline_name,
                        "upstream_changes": str(applied_changes),
                    },
                    change_type=ChangeType.ADD_COLUMN,
                    reasoning=(
                        f"Upstream pipeline '{pipeline.pipeline_name}' had schema changes "
                        f"auto-applied. Review if this downstream pipeline needs corresponding updates."
                    ),
                    confidence=0.6,
                    contract_version_before=downstream.version,
                )
                await self.store.save_proposal(proposal)
                log.info(
                    "Propagated schema change proposal to downstream pipeline %s.",
                    downstream.pipeline_name,
                )

        except Exception as e:
            log.warning("Downstream schema propagation failed: %s", e)

    # ------------------------------------------------------------------
    # Freshness monitoring
    # ------------------------------------------------------------------

    async def _check_freshness(self, pipeline: PipelineContract) -> None:
        """Check freshness against tier SLA and create alerts if needed."""
        connectors = await self.store.list_connectors()
        tgt_conn = None
        for c in connectors:
            if c.connector_id == pipeline.target_connector_id:
                tgt_conn = c
                break

        if not tgt_conn or tgt_conn.status != ConnectorStatus.ACTIVE:
            return

        tgt_params = self._target_params(pipeline)
        target = await self.registry.get_target(
            pipeline.target_connector_id, tgt_params,
        )

        tier_cfg = pipeline.get_tier_config()
        sla_warn = tier_cfg["freshness_warn_minutes"]
        sla_critical = tier_cfg["freshness_critical_minutes"]

        freshness_col = pipeline.get_freshness_col()
        now = datetime.now(timezone.utc)

        try:
            staleness: float
            last_record: Optional[str] = None

            if freshness_col:
                max_val = target.get_max_value(
                    pipeline.target_schema,
                    pipeline.target_table,
                    freshness_col,
                )
                if max_val:
                    max_dt = None
                    for fmt in (
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%dT%H:%M:%S.%f",
                    ):
                        try:
                            max_dt = datetime.strptime(
                                str(max_val)[:26], fmt,
                            ).replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue

                    if max_dt:
                        staleness = (now - max_dt).total_seconds() / 60
                    else:
                        staleness = 0.0
                    last_record = str(max_val)
                else:
                    staleness = float("inf")
                    last_record = None
            else:
                last_run = await self.store.get_last_successful_run(
                    pipeline.pipeline_id,
                )
                if last_run and last_run.completed_at:
                    last_dt = datetime.fromisoformat(
                        last_run.completed_at,
                    ).replace(tzinfo=timezone.utc)
                    staleness = (now - last_dt).total_seconds() / 60
                else:
                    staleness = float("inf")
                last_record = None

            # Determine status
            if staleness == float("inf"):
                status = FreshnessStatus.CRITICAL
                sla_met = False
            elif staleness > sla_critical:
                status = FreshnessStatus.CRITICAL
                sla_met = False
            elif staleness > sla_warn:
                status = FreshnessStatus.WARNING
                sla_met = False
            else:
                status = FreshnessStatus.FRESH
                sla_met = True

            snapshot = FreshnessSnapshot(
                pipeline_id=pipeline.pipeline_id,
                pipeline_name=pipeline.pipeline_name,
                tier=pipeline.tier,
                staleness_minutes=min(staleness, 99999),
                freshness_sla_minutes=sla_warn,
                sla_met=sla_met,
                status=status,
                last_record_time=last_record,
            )
            await self.store.save_freshness(snapshot)

            if status in (FreshnessStatus.WARNING, FreshnessStatus.CRITICAL):
                digest_only = tier_cfg.get("digest_only", False)
                severity = (
                    AlertSeverity.CRITICAL
                    if status == FreshnessStatus.CRITICAL
                    else AlertSeverity.WARNING
                )
                alert = AlertRecord(
                    severity=severity,
                    tier=pipeline.tier,
                    pipeline_id=pipeline.pipeline_id,
                    pipeline_name=pipeline.pipeline_name,
                    summary=(
                        f"Freshness {status.value}: {min(staleness, 99999):.0f}m stale "
                        f"(SLA warn={sla_warn}m, critical={sla_critical}m)"
                    ),
                    detail={
                        "staleness_minutes": round(min(staleness, 99999), 1),
                        "sla_warn_minutes": sla_warn,
                        "sla_critical_minutes": sla_critical,
                    },
                )
                await self.store.save_alert(alert)
                if not digest_only:
                    await self._dispatch_alert(alert, pipeline)

        except Exception as e:
            log.warning("Freshness check failed: %s", e)

    # ------------------------------------------------------------------
    # Data contract validation (Build 16)
    # ------------------------------------------------------------------

    async def _check_data_contracts(self) -> None:
        """Validate all active data contracts for freshness SLA and schema."""
        contracts = await self.store.list_data_contracts(status="active")
        for contract in contracts:
            try:
                await self._validate_contract(contract)
            except Exception as e:
                log.warning(
                    "Contract validation error for %s: %s",
                    contract.contract_id, e,
                )

    async def _validate_contract(self, contract) -> None:
        """Check freshness SLA and required columns for one data contract."""
        producer = await self.store.get_pipeline(contract.producer_pipeline_id)
        if not producer:
            return

        violations = []
        now = datetime.now(timezone.utc)

        # 1. Freshness SLA check
        last_run = await self.store.get_last_successful_run(
            contract.producer_pipeline_id,
        )
        if last_run and last_run.completed_at:
            completed = datetime.fromisoformat(
                last_run.completed_at,
            ).replace(tzinfo=timezone.utc)
            staleness_minutes = (now - completed).total_seconds() / 60
            if staleness_minutes > contract.freshness_sla_minutes:
                violations.append(ContractViolation(
                    contract_id=contract.contract_id,
                    violation_type=ContractViolationType.FRESHNESS_SLA,
                    detail=(
                        f"Producer data is {min(staleness_minutes, 99999):.0f}m old, "
                        f"SLA is {contract.freshness_sla_minutes}m"
                    ),
                    producer_pipeline_id=contract.producer_pipeline_id,
                    consumer_pipeline_id=contract.consumer_pipeline_id,
                ))

        # 2. Required columns check
        if contract.required_columns and producer.column_mappings:
            target_columns = {m.target_column for m in producer.column_mappings}
            missing = [
                c for c in contract.required_columns
                if c not in target_columns
            ]
            if missing:
                violations.append(ContractViolation(
                    contract_id=contract.contract_id,
                    violation_type=ContractViolationType.SCHEMA_MISMATCH,
                    detail=f"Missing required columns: {', '.join(missing)}",
                    producer_pipeline_id=contract.producer_pipeline_id,
                    consumer_pipeline_id=contract.consumer_pipeline_id,
                ))

        # 3. Record violations and update contract status
        for v in violations:
            await self.store.save_contract_violation(v)

        contract.last_validated_at = now_iso()
        if violations:
            contract.status = DataContractStatus.VIOLATED
            contract.last_violation_at = now_iso()
            contract.violation_count += len(violations)
            # Create alerts
            consumer = await self.store.get_pipeline(
                contract.consumer_pipeline_id,
            )
            for v in violations:
                alert = AlertRecord(
                    severity=AlertSeverity.WARNING,
                    tier=producer.tier,
                    pipeline_id=contract.producer_pipeline_id,
                    pipeline_name=producer.pipeline_name,
                    summary=f"Data contract violation: {v.detail}",
                    detail={
                        "contract_id": contract.contract_id,
                        "consumer": consumer.pipeline_name if consumer else contract.consumer_pipeline_id,
                        "violation_type": v.violation_type.value if hasattr(v.violation_type, "value") else v.violation_type,
                    },
                )
                await self.store.save_alert(alert)
                await self._dispatch_alert(alert, producer)
        else:
            contract.status = DataContractStatus.ACTIVE

        await self.store.save_data_contract(contract)

    # ------------------------------------------------------------------
    # Alert dispatch
    # ------------------------------------------------------------------

    async def _dispatch_alert(
        self,
        alert: AlertRecord,
        pipeline: PipelineContract,
    ) -> None:
        """Route alert to channels based on notification policy and tier config.

        Supports Slack, Email, PagerDuty, and digest-only.
        """
        channels = await self._resolve_channels(pipeline, alert.severity)
        for channel in channels:
            ch_type = channel.get("type", "")
            severity_filter = channel.get(
                "severity_filter",
                [alert.severity.value],
            )
            if alert.severity.value not in severity_filter:
                continue
            try:
                if ch_type == "slack":
                    await self._send_slack(
                        channel.get("target", ""), alert,
                    )
                elif ch_type == "email":
                    await self._send_email(
                        channel.get("target", ""), alert,
                    )
                elif ch_type == "pagerduty":
                    await self._send_pagerduty(
                        channel.get("target", ""), alert,
                    )
                elif ch_type == "digest":
                    pass  # handled by the daily digest loop
                else:
                    log.warning("Unknown alert channel type: %s", ch_type)
            except Exception as e:
                log.warning("Alert dispatch error (%s): %s", ch_type, e)

        # Always send PagerDuty for CRITICAL alerts if key is configured
        if (
            alert.severity == AlertSeverity.CRITICAL
            and self.config.pagerduty_key
        ):
            pagerduty_already_sent = any(
                ch.get("type") == "pagerduty" for ch in channels
            )
            if not pagerduty_already_sent:
                try:
                    await self._send_pagerduty(
                        self.config.pagerduty_key, alert,
                    )
                except Exception as e:
                    log.warning("PagerDuty escalation error: %s", e)

    async def _resolve_channels(
        self,
        pipeline: PipelineContract,
        severity: AlertSeverity,
    ) -> list[dict]:
        """Resolve notification channels from policy or tier defaults."""
        if pipeline.notification_policy_id:
            try:
                policy = await self.store.get_policy(pipeline.notification_policy_id)
                if policy and policy.channels:
                    return [ch for ch in policy.channels if isinstance(ch, dict)]
            except Exception as e:
                log.warning(
                    "Failed to load notification policy %s: %s",
                    pipeline.notification_policy_id, e,
                )

        tier_cfg = pipeline.get_tier_config()
        channels_raw = tier_cfg.get("alert_channels", [])
        result: list[dict] = []
        for ch in channels_raw:
            if isinstance(ch, dict):
                result.append(ch)
            elif isinstance(ch, str) and ":" in ch:
                ch_type, target = ch.split(":", 1)
                result.append({
                    "type": ch_type,
                    "target": target,
                    "severity_filter": ["info", "warning", "critical"],
                })
            elif isinstance(ch, str):
                result.append({
                    "type": ch,
                    "target": "",
                    "severity_filter": ["info", "warning", "critical"],
                })

        # Add PagerDuty channel for tier 1 critical
        if (
            pipeline.tier == 1
            and severity == AlertSeverity.CRITICAL
            and self.config.pagerduty_key
        ):
            has_pd = any(c.get("type") == "pagerduty" for c in result)
            if not has_pd:
                result.append({
                    "type": "pagerduty",
                    "target": self.config.pagerduty_key,
                    "severity_filter": ["critical"],
                })

        return result

    # ------------------------------------------------------------------
    # Slack
    # ------------------------------------------------------------------

    async def _send_slack(
        self,
        webhook_url: str,
        alert: AlertRecord,
    ) -> None:
        """POST alert to Slack webhook."""
        url = (
            webhook_url
            if webhook_url.startswith("http")
            else self.config.slack_webhook
        )
        if not url:
            log.debug("No Slack webhook configured, skipping.")
            return

        emoji = {
            "critical": ":red_circle:",
            "warning": ":warning:",
            "info": ":information_source:",
        }
        severity_str = alert.severity.value
        payload = {
            "text": (
                f"{emoji.get(severity_str, '')} "
                f"*[T{alert.tier}] {alert.pipeline_name}*\n"
                f"{alert.summary}"
            ),
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        log.debug("Slack alert sent for %s", alert.pipeline_name)

    # ------------------------------------------------------------------
    # Email
    # ------------------------------------------------------------------

    async def _send_email(
        self,
        to_addr: str,
        alert: AlertRecord,
    ) -> None:
        """Send alert via SMTP email."""
        if not self.config.email_smtp_host or not self.config.email_from:
            log.debug("Email not configured, skipping.")
            return

        to = to_addr if "@" in to_addr else self.config.email_from
        if not to:
            return

        msg = MIMEText(
            f"Pipeline: {alert.pipeline_name}\n"
            f"Tier: {alert.tier}\n"
            f"Severity: {alert.severity.value}\n"
            f"Summary: {alert.summary}\n"
            f"Time: {alert.created_at}\n"
            f"\nDetails:\n{json.dumps(alert.detail, indent=2, default=str)}"
        )
        msg["Subject"] = (
            f"[Pipeline Agent] {alert.severity.value.upper()}: "
            f"{alert.pipeline_name}"
        )
        msg["From"] = self.config.email_from
        msg["To"] = to

        # Run SMTP in executor to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                self._smtp_send,
                to,
                msg,
            )
            log.debug("Email alert sent to %s for %s", to, alert.pipeline_name)
        except Exception as e:
            log.warning("Email send error: %s", e)

    def _smtp_send(self, to: str, msg: MIMEText) -> None:
        """Synchronous SMTP send (called via run_in_executor)."""
        with smtplib.SMTP(
            self.config.email_smtp_host,
            self.config.email_smtp_port,
        ) as smtp:
            smtp.sendmail(self.config.email_from, [to], msg.as_string())

    # ------------------------------------------------------------------
    # PagerDuty
    # ------------------------------------------------------------------

    async def _send_pagerduty(
        self,
        routing_key: str,
        alert: AlertRecord,
    ) -> None:
        """POST alert to PagerDuty Events API v2."""
        key = routing_key or self.config.pagerduty_key
        if not key:
            log.debug("No PagerDuty routing key configured, skipping.")
            return

        # Map severity to PagerDuty severity
        pd_severity_map = {
            "critical": "critical",
            "warning": "warning",
            "info": "info",
        }
        pd_severity = pd_severity_map.get(
            alert.severity.value, "warning",
        )

        payload = {
            "routing_key": key,
            "event_action": "trigger",
            "dedup_key": (
                f"pipeline-agent-{alert.pipeline_id}-"
                f"{alert.severity.value}-{alert.alert_id[:8]}"
            ),
            "payload": {
                "summary": (
                    f"[T{alert.tier}] {alert.pipeline_name}: {alert.summary}"
                ),
                "severity": pd_severity,
                "source": "pipeline-agent",
                "component": alert.pipeline_name,
                "group": f"tier-{alert.tier}",
                "class": "data-pipeline",
                "custom_details": {
                    "pipeline_id": alert.pipeline_id,
                    "pipeline_name": alert.pipeline_name,
                    "tier": alert.tier,
                    "alert_id": alert.alert_id,
                    "detail": alert.detail,
                },
            },
        }

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
            )
            resp.raise_for_status()

        log.info(
            "PagerDuty alert triggered for %s (severity=%s)",
            alert.pipeline_name, pd_severity,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _source_params(self, pipeline: PipelineContract) -> dict:
        """Build source connection params, decrypting if needed."""
        params = {
            "host": pipeline.source_host,
            "port": pipeline.source_port,
            "database": pipeline.source_database,
            "user": "",
            "password": "",
        }
        if self.config.has_encryption_key:
            params = decrypt_dict(
                params, self.config.encryption_key, CREDENTIAL_FIELDS,
            )
        return params

    def _target_params(self, pipeline: PipelineContract) -> dict:
        """Build target connection params from the pipeline contract."""
        params = {
            "host": pipeline.target_host,
            "port": pipeline.target_port,
            "database": pipeline.target_database,
            "user": pipeline.target_user,
            "password": pipeline.target_password,
            "default_schema": pipeline.target_schema,
        }
        if self.config.has_encryption_key:
            params = decrypt_dict(
                params, self.config.encryption_key, CREDENTIAL_FIELDS,
            )
        return params
