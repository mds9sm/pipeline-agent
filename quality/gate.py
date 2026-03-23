"""
7-check quality gate.
Runs after data is loaded into the staging table, before promotion to target.

Checks:
  1. count_reconciliation  — extracted vs staged row count
  2. schema_consistency    — staging columns vs contract mappings + metadata
  3. pk_uniqueness         — duplicate merge-key groups
  4. null_rate_analysis    — z-score vs baseline null rates
  5. volume_zscore         — rows_extracted vs 30-run rolling average
  6. sample_verification   — quick count consistency sanity check
  7. freshness             — watermark staleness vs schedule interval
"""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional

from config import Config
from contracts.models import (
    PipelineContract,
    RunRecord,
    GateRecord,
    CheckResult,
    CheckStatus,
    GateDecision,
    RefreshType,
    RunStatus,
    QualityConfig,
    now_iso,
)
from contracts.store import ContractStore
from target.base import TargetEngine

log = logging.getLogger(__name__)


class QualityGate:
    """
    Orchestrates seven quality checks against a staging table and returns a
    GateRecord with a PROMOTE / PROMOTE_WITH_WARNING / HALT decision.

    The agent makes the final decision — checks provide signals, the agent
    reasons about whether to promote, warn, or halt based on context.
    """

    def __init__(self, store: ContractStore, config: Config, agent=None):
        self.store = store
        self.config = config
        self.agent = agent  # AgentCore — drives the gate decision

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(
        self,
        contract: PipelineContract,
        run: RunRecord,
        target: TargetEngine,
    ) -> GateRecord:
        """Execute all seven quality checks and render a gate decision."""

        gate = GateRecord(run_id=run.run_id, pipeline_id=contract.pipeline_id)
        qc = contract.quality_config
        schema, staging = self._staging_coords(contract, run, target)

        # Fetch volume history once for the volume check
        volume_history = await self.store.get_volume_baseline(
            contract.pipeline_id, window=30
        )

        checks: list[CheckResult] = [
            await self._check_count_reconciliation(run, schema, staging, qc, target),
            await self._check_schema_consistency(contract, schema, staging, target),
            await self._check_pk_uniqueness(contract, schema, staging, qc, target),
            await self._check_null_rate_analysis(contract, schema, staging, qc, target),
            await self._check_volume_zscore(run, qc, volume_history),
            await self._check_sample_verification(run, schema, staging, target),
            await self._check_freshness(contract, run, schema, staging, qc, target),
        ]

        gate.checks = checks

        # Determine if this is the first run (no prior complete runs)
        prior_runs = await self.store.list_runs(contract.pipeline_id, limit=5)
        is_first_run = not any(
            r.run_id != run.run_id and r.status == RunStatus.COMPLETE
            for r in prior_runs
        )

        # ---- Agent-driven decision ----
        # The agent receives all check results and decides based on context
        if self.agent:
            try:
                agent_decision = await self.agent.decide_quality_gate(
                    contract, checks, is_first_run,
                )
                decision_str = agent_decision.get("decision", "halt")
                if decision_str == "promote":
                    gate.decision = GateDecision.PROMOTE
                elif decision_str == "promote_with_warning":
                    gate.decision = GateDecision.PROMOTE_WITH_WARNING
                else:
                    gate.decision = GateDecision.HALT
                gate.agent_reasoning = agent_decision.get("reasoning", "")
            except Exception as e:
                log.warning("Agent gate decision failed, using fallback: %s", e)
                gate.decision = self._fallback_decision(checks, is_first_run, qc)
        else:
            gate.decision = self._fallback_decision(checks, is_first_run, qc)

        gate.evaluated_at = now_iso()

        # Log gate decision and check summary
        check_summary = ", ".join(
            f"{c.check_name}={c.status.value}" for c in checks
        )
        if gate.decision == GateDecision.HALT:
            log.warning(
                "Quality gate HALT (%s)", check_summary,
            )
        elif gate.decision == GateDecision.PROMOTE_WITH_WARNING:
            log.info(
                "Quality gate PROMOTE_WITH_WARNING (%s)", check_summary,
            )
        else:
            log.info("Quality gate PROMOTE (%s)", check_summary)

        for c in checks:
            log.debug(
                "  check %s: %s (%dms) -- %s",
                c.check_name, c.status.value, c.duration_ms or 0, c.detail,
            )

        return gate

    # ------------------------------------------------------------------
    # Fallback decision (when agent unavailable)
    # ------------------------------------------------------------------

    @staticmethod
    def _fallback_decision(checks, is_first_run, qc) -> GateDecision:
        """Threshold-based fallback when agent is unavailable."""
        if is_first_run:
            # First run leniency: downgrade FAILs to WARNs
            for c in checks:
                if c.status == CheckStatus.FAIL:
                    c.status = CheckStatus.WARN
                    c.detail = f"[First run - auto-downgraded] {c.detail}"

        any_fail = any(c.status == CheckStatus.FAIL for c in checks)
        any_warn = any(c.status == CheckStatus.WARN for c in checks)

        if any_fail:
            return GateDecision.HALT
        if any_warn:
            return (
                GateDecision.PROMOTE_WITH_WARNING
                if qc.promote_on_warn
                else GateDecision.HALT
            )
        return GateDecision.PROMOTE

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _staging_coords(
        contract: PipelineContract, run: RunRecord, target: TargetEngine
    ) -> tuple[str, str]:
        """Delegate staging table naming to the target connector."""
        return target.staging_name(contract, run)

    # ==================================================================
    # Check 1: Count reconciliation
    # ==================================================================

    async def _check_count_reconciliation(
        self,
        run: RunRecord,
        schema: str,
        staging: str,
        qc: QualityConfig,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            extracted = run.rows_extracted

            # Edge case: first run / empty extract is always fine
            if extracted == 0:
                return CheckResult(
                    check_name="count_reconciliation",
                    status=CheckStatus.PASS,
                    detail="No rows extracted -- skipping count check.",
                    metadata={"extracted": 0, "staged": 0},
                    duration_ms=_elapsed(t0),
                )

            staged = target.get_row_count(schema, staging)
            diff = abs(staged - extracted)
            diff_pct = diff / max(extracted, 1)

            if diff == 0:
                status = CheckStatus.PASS
                detail = f"Exact match: {staged} rows."
            elif diff_pct <= qc.count_tolerance:
                status = CheckStatus.WARN
                detail = (
                    f"Minor discrepancy: extracted={extracted}, staged={staged} "
                    f"({diff_pct:.4%} within {qc.count_tolerance:.4%} tolerance)."
                )
            else:
                status = CheckStatus.FAIL
                detail = (
                    f"Count mismatch: extracted={extracted}, staged={staged} "
                    f"({diff_pct:.4%} exceeds {qc.count_tolerance:.4%} tolerance)."
                )

            return CheckResult(
                check_name="count_reconciliation",
                status=status,
                detail=detail,
                metadata={"extracted": extracted, "staged": staged, "diff_pct": round(diff_pct, 6)},
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("count_reconciliation", exc, t0)

    # ==================================================================
    # Check 2: Schema consistency
    # ==================================================================

    async def _check_schema_consistency(
        self,
        contract: PipelineContract,
        schema: str,
        staging: str,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            staged_cols = {
                r["column_name"]: r["data_type"]
                for r in target.get_column_types(schema, staging)
            }

            # Build expected columns: contract mappings + metadata
            expected_cols: dict[str, str] = {
                m.target_column: m.target_type for m in contract.column_mappings
            }
            metadata_cols = {
                "_extracted_at": "TIMESTAMPTZ",
                "_source_schema": "VARCHAR(255)",
                "_source_table": "VARCHAR(255)",
                "_row_hash": "VARCHAR(64)",
            }
            expected_cols.update(metadata_cols)

            missing = set(expected_cols.keys()) - set(staged_cols.keys())
            extra = set(staged_cols.keys()) - set(expected_cols.keys())

            # Type mismatch check (skip metadata columns for strict type comparison)
            # Normalize Postgres type aliases
            _TYPE_ALIASES = {
                "CHARACTER VARYING": "VARCHAR",
                "INT": "INTEGER",
                "INT4": "INTEGER",
                "INT8": "BIGINT",
                "FLOAT8": "DOUBLE PRECISION",
                "FLOAT4": "REAL",
                "BOOL": "BOOLEAN",
                "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
                "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
                "DECIMAL": "NUMERIC",
                "DEC": "NUMERIC",
                "SERIAL": "INTEGER",
                "BIGSERIAL": "BIGINT",
                "SMALLSERIAL": "SMALLINT",
            }

            def _normalize_type(t: str) -> str:
                base = t.upper().split("(")[0].strip()
                return _TYPE_ALIASES.get(base, base)

            type_mismatches: list[str] = []
            for col, expected_type in expected_cols.items():
                if col.startswith("_"):
                    continue  # metadata presence is enough
                if col in staged_cols:
                    staged_norm = _normalize_type(staged_cols[col])
                    expected_norm = _normalize_type(expected_type)
                    if staged_norm != expected_norm and staged_norm not in expected_norm and expected_norm not in staged_norm:
                        type_mismatches.append(
                            f"{col}: expected {expected_type}, got {staged_cols[col]}"
                        )

            if missing or type_mismatches:
                parts = []
                if missing:
                    parts.append(f"Missing columns: {sorted(missing)}.")
                if type_mismatches:
                    parts.append(f"Type mismatches: {type_mismatches}.")
                return CheckResult(
                    check_name="schema_consistency",
                    status=CheckStatus.FAIL,
                    detail=" ".join(parts),
                    metadata={
                        "missing": sorted(missing),
                        "type_mismatches": type_mismatches,
                    },
                    duration_ms=_elapsed(t0),
                )

            if extra:
                return CheckResult(
                    check_name="schema_consistency",
                    status=CheckStatus.WARN,
                    detail=f"Extra columns in staging: {sorted(extra)}.",
                    metadata={"extra": sorted(extra)},
                    duration_ms=_elapsed(t0),
                )

            return CheckResult(
                check_name="schema_consistency",
                status=CheckStatus.PASS,
                detail="Schema matches contract.",
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("schema_consistency", exc, t0)

    # ==================================================================
    # Check 3: PK uniqueness
    # ==================================================================

    async def _check_pk_uniqueness(
        self,
        contract: PipelineContract,
        schema: str,
        staging: str,
        qc: QualityConfig,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            # Edge case: no merge keys -- nothing to check
            if not contract.merge_keys:
                return CheckResult(
                    check_name="pk_uniqueness",
                    status=CheckStatus.PASS,
                    detail="No merge keys defined -- PK uniqueness check not applicable.",
                    duration_ms=_elapsed(t0),
                )

            dupe_count = target.check_duplicates(schema, staging, contract.merge_keys)
            if dupe_count > 0:
                return CheckResult(
                    check_name="pk_uniqueness",
                    status=CheckStatus.FAIL,
                    detail=(
                        f"Merge keys have {dupe_count} duplicate groups: "
                        f"{contract.merge_keys}."
                    ),
                    metadata={
                        "duplicate_groups": dupe_count,
                        "keys": contract.merge_keys,
                    },
                    duration_ms=_elapsed(t0),
                )

            # Cardinality deviation vs baseline (if available)
            deviations: list[str] = []
            if contract.baseline_cardinality:
                monitored = [
                    m.target_column
                    for m in contract.column_mappings
                    if not m.target_column.startswith("_")
                ]
                if monitored:
                    current = target.get_cardinality(schema, staging, monitored)
                    for col, cur_card in current.items():
                        baseline = contract.baseline_cardinality.get(col, 0)
                        if baseline > 0:
                            deviation = abs(cur_card - baseline) / baseline
                            if deviation > qc.cardinality_deviation_threshold:
                                deviations.append(
                                    f"{col}: {baseline} -> {cur_card} "
                                    f"({deviation:.0%} change)"
                                )

            if deviations:
                return CheckResult(
                    check_name="pk_uniqueness",
                    status=CheckStatus.WARN,
                    detail=f"Cardinality deviations: {'; '.join(deviations)}",
                    metadata={"deviations": deviations},
                    duration_ms=_elapsed(t0),
                )

            return CheckResult(
                check_name="pk_uniqueness",
                status=CheckStatus.PASS,
                detail="No duplicate merge-key groups. Cardinality within expected range.",
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("pk_uniqueness", exc, t0)

    # ==================================================================
    # Check 4: Null rate analysis
    # ==================================================================

    async def _check_null_rate_analysis(
        self,
        contract: PipelineContract,
        schema: str,
        staging: str,
        qc: QualityConfig,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            cols = [
                m.target_column
                for m in contract.column_mappings
                if not m.target_column.startswith("_")
            ]
            if not cols:
                return CheckResult(
                    check_name="null_rate_analysis",
                    status=CheckStatus.PASS,
                    detail="No columns to check.",
                    duration_ms=_elapsed(t0),
                )

            current = target.get_null_rates(schema, staging, cols)
            baseline = contract.baseline_null_rates
            stddevs = contract.baseline_null_stddevs

            # Edge case: no baseline yet (first run) -- store current as baseline, PASS
            if not baseline:
                return CheckResult(
                    check_name="null_rate_analysis",
                    status=CheckStatus.PASS,
                    detail="First run -- no baseline. Current null rates accepted as baseline.",
                    metadata={"current_null_rates": {k: round(v, 4) for k, v in current.items()}},
                    duration_ms=_elapsed(t0),
                )

            catastrophic: list[str] = []
            anomalous: list[str] = []

            for col, cur_rate in current.items():
                base_rate = baseline.get(col, cur_rate)
                base_std = stddevs.get(col, 0.0)

                # Catastrophic: was < 5% now > 45%
                if base_rate < 0.05 and cur_rate > qc.null_rate_catastrophic_jump:
                    catastrophic.append(
                        f"{col}: {base_rate:.1%} -> {cur_rate:.1%} (catastrophic)"
                    )
                    continue

                # Statistical anomaly via z-score
                if base_std > 0:
                    z = abs(cur_rate - base_rate) / base_std
                    if z > qc.null_rate_stddev_threshold:
                        anomalous.append(
                            f"{col}: {base_rate:.1%} -> {cur_rate:.1%} (z={z:.1f})"
                        )

            if catastrophic:
                return CheckResult(
                    check_name="null_rate_analysis",
                    status=CheckStatus.FAIL,
                    detail=f"Catastrophic null rate jump: {'; '.join(catastrophic)}",
                    metadata={"catastrophic": catastrophic, "anomalous": anomalous},
                    duration_ms=_elapsed(t0),
                )

            if len(anomalous) > qc.null_rate_max_anomalies_warn:
                return CheckResult(
                    check_name="null_rate_analysis",
                    status=CheckStatus.FAIL,
                    detail=(
                        f"{len(anomalous)} columns with anomalous null rates "
                        f"(>{qc.null_rate_max_anomalies_warn} threshold)."
                    ),
                    metadata={"anomalous": anomalous},
                    duration_ms=_elapsed(t0),
                )

            if anomalous:
                return CheckResult(
                    check_name="null_rate_analysis",
                    status=CheckStatus.WARN,
                    detail=f"Elevated null rates: {'; '.join(anomalous)}",
                    metadata={"anomalous": anomalous},
                    duration_ms=_elapsed(t0),
                )

            return CheckResult(
                check_name="null_rate_analysis",
                status=CheckStatus.PASS,
                detail="Null rates within expected range.",
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("null_rate_analysis", exc, t0)

    # ==================================================================
    # Check 5: Volume z-score
    # ==================================================================

    async def _check_volume_zscore(
        self,
        run: RunRecord,
        qc: QualityConfig,
        history: list[int],
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            rows = run.rows_extracted

            # Edge case: fewer than 5 historical runs -- skip
            if len(history) < 5:
                return CheckResult(
                    check_name="volume_zscore",
                    status=CheckStatus.PASS,
                    detail=(
                        f"Insufficient history ({len(history)} runs). "
                        "Skipping volume z-score."
                    ),
                    metadata={"history_count": len(history)},
                    duration_ms=_elapsed(t0),
                )

            # Zero rows when baseline is non-trivial
            if rows == 0 and max(history) > 100:
                return CheckResult(
                    check_name="volume_zscore",
                    status=CheckStatus.FAIL,
                    detail=(
                        f"Zero rows extracted when baseline average is "
                        f"{sum(history) / len(history):.0f}."
                    ),
                    metadata={"rows": 0, "baseline_avg": sum(history) / len(history)},
                    duration_ms=_elapsed(t0),
                )

            mean = sum(history) / len(history)
            variance = sum((x - mean) ** 2 for x in history) / len(history)
            std = math.sqrt(variance) if variance > 0 else 1.0
            z = abs(rows - mean) / std

            if z > qc.volume_z_score_fail:
                status = CheckStatus.FAIL
                detail = (
                    f"Volume anomaly: {rows} rows, z-score={z:.2f} "
                    f"(>{qc.volume_z_score_fail} threshold). "
                    f"Baseline avg={mean:.0f}."
                )
            elif z > qc.volume_z_score_warn:
                status = CheckStatus.WARN
                detail = (
                    f"Volume elevated: {rows} rows, z-score={z:.2f}. "
                    f"Baseline avg={mean:.0f}."
                )
            else:
                status = CheckStatus.PASS
                detail = f"{rows} rows, z-score={z:.2f}. Normal."

            return CheckResult(
                check_name="volume_zscore",
                status=status,
                detail=detail,
                metadata={
                    "rows": rows,
                    "z_score": round(z, 3),
                    "baseline_avg": round(mean, 1),
                    "baseline_std": round(std, 1),
                },
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("volume_zscore", exc, t0)

    # ==================================================================
    # Check 6: Sample verification
    # ==================================================================

    async def _check_sample_verification(
        self,
        run: RunRecord,
        schema: str,
        staging: str,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            if run.rows_extracted == 0:
                return CheckResult(
                    check_name="sample_verification",
                    status=CheckStatus.PASS,
                    detail="No rows to verify.",
                    duration_ms=_elapsed(t0),
                )

            staged = target.get_row_count(schema, staging)
            diff = abs(staged - run.rows_extracted)
            diff_pct = diff / max(run.rows_extracted, 1)

            if diff_pct < 0.001:
                status = CheckStatus.PASS
                detail = (
                    f"Staging count ({staged}) consistent with extraction "
                    f"({run.rows_extracted})."
                )
            else:
                status = CheckStatus.WARN
                detail = (
                    f"Minor count discrepancy: extracted={run.rows_extracted}, "
                    f"staged={staged}."
                )

            return CheckResult(
                check_name="sample_verification",
                status=status,
                detail=detail,
                metadata={"staged": staged, "extracted": run.rows_extracted},
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("sample_verification", exc, t0)

    # ==================================================================
    # Check 7: Freshness
    # ==================================================================

    async def _check_freshness(
        self,
        contract: PipelineContract,
        run: RunRecord,
        schema: str,
        staging: str,
        qc: QualityConfig,
        target: TargetEngine,
    ) -> CheckResult:
        t0 = time.monotonic()
        try:
            inc_col = contract.get_freshness_col()

            # Not applicable for full refreshes or missing incremental column
            if contract.refresh_type == RefreshType.FULL or not inc_col:
                return CheckResult(
                    check_name="freshness",
                    status=CheckStatus.PASS,
                    detail="Full refresh -- freshness check not applicable.",
                    duration_ms=_elapsed(t0),
                )

            # For first/initial runs, freshness against schedule is not meaningful
            # (data may be a historical backfill)
            prior_runs = await self.store.list_runs(contract.pipeline_id, limit=2)
            prior_success = [r for r in prior_runs if r.run_id != run.run_id and r.status.value == "complete"]
            if not prior_success:
                return CheckResult(
                    check_name="freshness",
                    status=CheckStatus.PASS,
                    detail="First run -- freshness check skipped (no baseline).",
                    duration_ms=_elapsed(t0),
                )

            max_val = target.get_max_value(schema, staging, inc_col)
            if max_val is None:
                return CheckResult(
                    check_name="freshness",
                    status=CheckStatus.WARN,
                    detail=f"Could not determine MAX({inc_col}) from staging.",
                    duration_ms=_elapsed(t0),
                )

            # Parse the schedule interval in minutes
            schedule_minutes = _cron_interval_minutes(contract.schedule_cron)
            if schedule_minutes <= 0:
                return CheckResult(
                    check_name="freshness",
                    status=CheckStatus.PASS,
                    detail="Could not parse schedule interval -- skipping freshness check.",
                    duration_ms=_elapsed(t0),
                )

            # Attempt to parse max_val as a datetime
            max_dt = _parse_watermark_datetime(max_val)
            if max_dt is None:
                return CheckResult(
                    check_name="freshness",
                    status=CheckStatus.PASS,
                    detail=(
                        f"Non-timestamp watermark ({inc_col}={max_val}) -- "
                        "skipping freshness."
                    ),
                    duration_ms=_elapsed(t0),
                )

            now = datetime.now(timezone.utc)
            staleness_minutes = (now - max_dt).total_seconds() / 60.0

            warn_threshold = schedule_minutes * qc.freshness_warn_multiplier
            fail_threshold = schedule_minutes * qc.freshness_fail_multiplier

            if staleness_minutes > fail_threshold:
                status = CheckStatus.FAIL
                detail = (
                    f"Data is stale: {staleness_minutes:.0f}m old "
                    f"(>{fail_threshold:.0f}m threshold, "
                    f"{qc.freshness_fail_multiplier}x schedule)."
                )
            elif staleness_minutes > warn_threshold:
                status = CheckStatus.WARN
                detail = (
                    f"Data slightly stale: {staleness_minutes:.0f}m old "
                    f"(>{warn_threshold:.0f}m, "
                    f"{qc.freshness_warn_multiplier}x schedule)."
                )
            else:
                status = CheckStatus.PASS
                detail = (
                    f"Data fresh: {staleness_minutes:.0f}m old "
                    f"(schedule={schedule_minutes}m)."
                )

            return CheckResult(
                check_name="freshness",
                status=status,
                detail=detail,
                metadata={
                    "staleness_minutes": round(staleness_minutes, 1),
                    "schedule_minutes": schedule_minutes,
                    "max_watermark": max_val,
                    "warn_threshold_minutes": round(warn_threshold, 1),
                    "fail_threshold_minutes": round(fail_threshold, 1),
                },
                duration_ms=_elapsed(t0),
            )
        except Exception as exc:
            return _error_result("freshness", exc, t0)


# ======================================================================
# Private helpers
# ======================================================================


def _elapsed(t0: float) -> int:
    return int((time.monotonic() - t0) * 1000)


def _error_result(check_name: str, exc: Exception, t0: float) -> CheckResult:
    return CheckResult(
        check_name=check_name,
        status=CheckStatus.FAIL,
        detail=f"{check_name} check error: {exc}",
        duration_ms=_elapsed(t0),
    )


def _parse_watermark_datetime(value: str) -> Optional[datetime]:
    """
    Best-effort parse of a watermark value as a UTC datetime.
    Returns None when the value is not a recognisable timestamp.
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _cron_interval_minutes(cron: str) -> int:
    """
    Best-effort parse of the typical interval (in minutes) between two
    consecutive firings of a cron expression.

    Uses ``croniter`` when available; falls back to a simple heuristic
    for common patterns.
    """
    try:
        from croniter import croniter
        import datetime as dt

        base = dt.datetime(2025, 1, 6, 0, 0)  # a Monday
        ci = croniter(cron, base)
        t1 = ci.get_next(dt.datetime)
        t2 = ci.get_next(dt.datetime)
        delta_minutes = int((t2 - t1).total_seconds() / 60)
        # Handle cross-midnight: croniter already accounts for this, but
        # guard against negatives just in case.
        return max(delta_minutes, 1)
    except Exception:
        pass

    # Fallback heuristic for common cron patterns
    try:
        parts = cron.strip().split()
        if len(parts) < 5:
            return 0

        minute, hour, dom, month, dow = parts[:5]

        # Every N minutes: */N * * * *
        if minute.startswith("*/") and hour == "*":
            return int(minute[2:])

        # Fixed minute, every hour: M * * * *
        if minute.isdigit() and hour == "*":
            return 60

        # Fixed minute + hour, every day: M H * * *
        if minute.isdigit() and hour.isdigit() and dom == "*":
            return 1440  # daily

        # Every N hours: 0 */N * * *
        if hour.startswith("*/"):
            return int(hour[2:]) * 60

        return 0
    except Exception:
        return 0
