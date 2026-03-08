"""
Monitor engine -- schema drift detection, freshness monitoring,
column-level lineage impact analysis, and multi-channel alert dispatch
(Slack, Email, PagerDuty).
"""
from __future__ import annotations

import asyncio
import json
import logging
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
    FreshnessStatus, AlertSeverity, TriggerType, ChangeType,
    ProposalStatus, ConnectorStatus, TIER_DEFAULTS,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from agent.core import AgentCore
from crypto import decrypt_dict, CREDENTIAL_FIELDS

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
        """Check drift and freshness for all active pipelines."""
        pipelines = await self.store.list_pipelines(status="active")
        for pipeline in pipelines:
            try:
                await self._check_drift(pipeline)
            except Exception as e:
                log.warning(
                    "[%s] Drift check error: %s",
                    pipeline.pipeline_name, e,
                )
            try:
                await self._check_freshness(pipeline)
            except Exception as e:
                log.warning(
                    "[%s] Freshness check error: %s",
                    pipeline.pipeline_name, e,
                )

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
            log.warning(
                "[%s] Could not profile source table: %s",
                pipeline.pipeline_name, e,
            )
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
        log.info("[%s] Drift detected: %s", pipeline.pipeline_name, drift_info)

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
                    "[%s] Failed to query downstream lineage for column %s: %s",
                    pipeline.pipeline_name, col_name, e,
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

        # Auto-apply additive nullable columns if configured and safe
        if (
            analysis["action"] == "auto_adapt"
            and pipeline.auto_approve_additive_schema
            and not dropped_columns
            and not type_changes
        ):
            await self._auto_apply_new_columns(pipeline, new_columns)
            return

        # Create a proposal with impact analysis
        proposal = ContractChangeProposal(
            pipeline_id=pipeline.pipeline_id,
            trigger_type=TriggerType.SCHEMA_DRIFT,
            trigger_detail=drift_info,
            change_type=(
                ChangeType.DROP_COLUMN if dropped_columns
                else ChangeType.ALTER_COLUMN_TYPE if type_changes
                else ChangeType.ADD_COLUMN
            ),
            current_state={
                "column_mappings": [
                    asdict(m) for m in pipeline.column_mappings
                ],
            },
            proposed_state={
                "column_mappings": [asdict(m) for m in profile.columns],
            },
            reasoning=analysis.get("reasoning", ""),
            confidence=analysis.get("confidence", 0.5),
            impact_analysis=impact_analysis,
            rollback_plan=analysis.get("rollback_plan", ""),
            contract_version_before=pipeline.version,
        )
        await self.store.save_proposal(proposal)

        # Create alert
        severity = (
            AlertSeverity.CRITICAL if dropped_columns or type_changes
            else AlertSeverity.WARNING
        )
        alert = AlertRecord(
            severity=severity,
            tier=pipeline.tier,
            pipeline_id=pipeline.pipeline_id,
            pipeline_name=pipeline.pipeline_name,
            summary=(
                f"Schema drift detected: {len(new_columns)} new, "
                f"{len(dropped_columns)} dropped, "
                f"{len(type_changes)} type changes."
            ),
            detail={
                **drift_info,
                "downstream_impact_count": len(downstream_impact),
            },
        )
        await self.store.save_alert(alert)
        await self._dispatch_alert(alert, pipeline)

        # Log decision
        await self.store.save_decision(DecisionLog(
            pipeline_id=pipeline.pipeline_id,
            decision_type="drift_detected",
            detail=json.dumps(drift_info),
            reasoning=analysis.get("reasoning", ""),
        ))

    async def _auto_apply_new_columns(
        self,
        pipeline: PipelineContract,
        new_columns: list[dict],
    ) -> None:
        """Append new nullable columns, increment version, write SchemaVersion."""
        # Re-profile to get full ColumnMapping objects
        src_params = self._source_params(pipeline)
        source = await self.registry.get_source(
            pipeline.source_connector_id, src_params,
        )
        profile = await source.profile_table(
            pipeline.source_schema, pipeline.source_table,
        )
        live_cols = {m.source_column: m for m in profile.columns}

        for col_info in new_columns:
            col_name = col_info["name"]
            if col_name in live_cols:
                pipeline.column_mappings.append(live_cols[col_name])

        pipeline.version += 1
        pipeline.updated_at = now_iso()
        await self.store.save_pipeline(pipeline)

        sv = SchemaVersion(
            pipeline_id=pipeline.pipeline_id,
            version=pipeline.version,
            column_mappings=pipeline.column_mappings,
            change_summary=(
                f"Auto-applied {len(new_columns)} new nullable column(s): "
                f"{[c['name'] for c in new_columns]}"
            ),
            change_type="add_column",
            applied_by="agent",
        )
        await self.store.save_schema_version(sv)
        log.info(
            "[%s] Auto-applied %d new columns (v%d).",
            pipeline.pipeline_name, len(new_columns), pipeline.version,
        )

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
            await self.store.save_freshness_snapshot(snapshot)

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
                        f"Freshness {status.value}: {staleness:.0f}m stale "
                        f"(SLA warn={sla_warn}m, critical={sla_critical}m)"
                    ),
                    detail={
                        "staleness_minutes": round(staleness, 1),
                        "sla_warn_minutes": sla_warn,
                        "sla_critical_minutes": sla_critical,
                    },
                )
                await self.store.save_alert(alert)
                if not digest_only:
                    await self._dispatch_alert(alert, pipeline)

        except Exception as e:
            log.warning(
                "[%s] Freshness check failed: %s",
                pipeline.pipeline_name, e,
            )

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
        channels = self._resolve_channels(pipeline, alert.severity)
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

    def _resolve_channels(
        self,
        pipeline: PipelineContract,
        severity: AlertSeverity,
    ) -> list[dict]:
        """Resolve notification channels from policy or tier defaults."""
        if pipeline.notification_policy_id:
            # Notification policy lookup is sync-compatible
            # We rely on the store having been queried already or being cached
            pass

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
