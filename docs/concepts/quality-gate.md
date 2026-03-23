# Quality Gate

The quality gate is a connector-agnostic, 7-check verification system that runs after data loading but before promotion to the target table. It ensures only trusted data reaches production.

> **Agentic behavior**: The quality gate decision (PROMOTE / HALT / PROMOTE_WITH_WARNING) is made by the **AI agent**, not hardcoded thresholds. The 7 checks provide **signals** — the agent interprets them in context (pipeline tier, first run, refresh type, historical patterns) to make the final call. A rule-based fallback exists for when the API key is unavailable (see [Fallback Behavior](#rule-based-fallback)).

---

## How It Works

Every pipeline run follows this flow:

```
Extract → Load to Staging → Quality Gate → Promote (or Halt)
```

The gate evaluates 7 independent checks against the staging table. Each check returns **PASS**, **WARN**, or **FAIL** as a signal. The agent then decides:

| Agent Reasoning | Decision |
|-----------------|----------|
| Checks are healthy, data looks good | **PROMOTE** |
| Minor warnings but acceptable given context | **PROMOTE_WITH_WARNING** — data promoted, alert raised |
| Critical signal in context (e.g., null spike on Tier 1 pipeline) | **HALT** — data stays in staging |

The agent considers:
- **Pipeline tier** — Tier 1 pipelines get stricter evaluation than Tier 3
- **First run** — New pipelines with no baseline get leniency
- **Refresh type** — Full refresh vs. incremental have different expectations
- **Historical patterns** — Is this deviation normal for this pipeline?
- **Check severity** — Which checks failed and how badly

---

## The 7 Checks (Signal Providers)

Each check produces a signal (PASS/WARN/FAIL) with metadata. The agent uses these signals — along with context — to reason about the overall decision.

### 1. Count Reconciliation

Compares extracted row count vs. staged row count. Detects data loss during loading.

- Configurable tolerance (default 0.1%)
- Reports deviation percentage to agent

### 2. Schema Consistency

Validates that staging table columns match the pipeline contract's column mappings plus standard metadata columns.

**Metadata columns** (added by every source connector):
- `_extracted_at` — UTC extraction timestamp
- `_source_schema` — originating schema name
- `_source_table` — originating table name
- `_row_hash` — SHA-256 hash of all column values

**Reports**: missing columns, type mismatches, extra columns not in contract.

### 3. Primary Key Uniqueness

When `merge_keys` are defined, checks for duplicate key groups in the staging table.

- Reports cardinality deviation and duplicate counts
- Duplicate merge keys can corrupt upsert operations

### 4. Null Rate Analysis

Uses **z-score statistical analysis** to detect null rate jumps. Compares current null rates against a rolling baseline.

- Reports per-column null rates and z-scores
- Example: column historically < 5% nulls, current batch 45% → high z-score reported to agent

### 5. Volume Z-Score

Detects anomalous row volume using a **30-run rolling average**.

- Requires 5+ historical runs to build baseline
- Reports z-score and absolute deviation
- Example: pipeline usually loads ~1000 rows, suddenly loads 50 → extreme z-score

### 6. Sample Verification

Quick sanity check that staging row count matches extraction count within 0.1%. Fast double-check for count reconciliation.

### 7. Freshness Check

For **incremental pipelines** only, checks staleness of the maximum watermark value against the schedule interval.

- Reports watermark age relative to schedule interval
- Not applicable for full-refresh pipelines

---

## First-Run Leniency

On the **very first successful run** (no prior COMPLETE runs in history), the agent applies leniency — both in agentic mode and in the rule-based fallback. This allows the first run to:

1. Establish baseline metrics (row counts, null rates, cardinality)
2. Promote initial data to the target table
3. Create the reference point for future comparisons

Without first-run leniency, every new pipeline would halt on its first run because there's no baseline to compare against.

---

## Quality Configuration

Each pipeline has a `QualityConfig` that provides reference thresholds for the agent's reasoning:

```yaml
quality_config:
  count_tolerance: 0.001        # 0.1% row count tolerance
  null_rate_z_threshold: 2.0    # z-score reference for null rates
  volume_z_warn: 2.0            # z-score reference for volume
  volume_z_fail: 3.0            # z-score reference for extreme volume
  freshness_warn_multiplier: 2  # schedule_interval multiplier
  freshness_fail_multiplier: 5  # schedule_interval multiplier
  promote_on_warn: true         # hint for agent on warning tolerance
  min_historical_runs: 5        # runs needed before z-score is meaningful
```

These values are **inputs to the agent's reasoning**, not hard decision boundaries. The agent may override them based on context (e.g., a Tier 1 pipeline with a 3.1 z-score may still halt even if `volume_z_fail` is 3.5).

**Tier defaults** provide sensible starting references:

| Setting | Tier 1 | Tier 2 | Tier 3 |
|---------|--------|--------|--------|
| count_tolerance | 0.001 | 0.01 | 0.05 |
| volume_z_fail | 2.5 | 3.0 | 4.0 |
| promote_on_warn | false | true | true |

---

## Agent Reasoning Output

The gate record includes `agent_reasoning` — a natural language explanation of why the agent chose PROMOTE or HALT:

```json
{
  "decision": "halt",
  "agent_reasoning": "Halting due to 45% null rate on order_amount column (z-score 4.2). This is a Tier 1 pipeline and the null spike is not consistent with historical patterns. The volume is normal, suggesting a source-side data quality issue rather than an extraction problem.",
  "checks": { ... }
}
```

---

## Agentic Halt Handling

When the quality gate **halts** a run, the agent doesn't just stop — it diagnoses the problem and proposes a fix:

1. **Agent diagnoses** — `diagnose_halt()` analyzes the failed checks, pipeline context, and gate reasoning to identify the root cause (schema mismatch, volume anomaly, null spike, etc.)
2. **Proposes a fix** — For schema issues: generates `ALTER TABLE` SQL. For threshold issues: suggests quality config adjustments. The fix is stored as a `QUALITY_FIX` approval proposal.
3. **Enriches the run** — `run.error` is enriched with the diagnosis (root cause, category, recommended action), execution log includes `agent_diagnosis` and `proposal_created` steps
4. **Creates an alert** — Tier-based severity (CRITICAL for Tier 1, WARNING otherwise)
5. **User approves** — The fix appears inline in the Activity view with an **"Approve Fix & Re-run"** button. One click applies the SQL/config fix and triggers a new run.

### Halt Diagnosis Categories

| Category | Fix Type | Example |
|----------|----------|---------|
| `schema` | `alter_schema` — ALTER TABLE SQL | DECIMAL vs NUMERIC type mismatch |
| `volume` | `adjust_quality_config` — threshold change | Z-score threshold too strict for seasonal data |
| `nulls` | `adjust_quality_config` or `fix_source` | Null rate spike on non-critical column |
| `uniqueness` | `fix_source` or `manual` | Duplicate merge keys from source |
| `reconciliation` | `fix_source` or `manual` | Row count mismatch between extract and staging |

### Rule-Based Halt Fallback

> **⚠️ RULE-BASED**: `_rule_based_halt_diagnosis()` classifies by check type (schema fails → suggest ALTER TABLE, volume fails → suggest threshold adjustment). No fix SQL is generated — only generic recommendations.

---

## Rule-Based Fallback

> **⚠️ RULE-BASED**: When the Claude API key is unavailable, the quality gate falls back to `_fallback_decision()` — a static threshold-based decision engine. This is explicitly marked as non-agentic behavior.

The fallback applies these fixed rules:
- Any check = FAIL → **HALT** (unless first run → downgrade to WARN)
- Any check = WARN + `promote_on_warn=true` → **PROMOTE_WITH_WARNING**
- All checks = PASS → **PROMOTE**

This ensures pipelines continue operating without LLM access, but without contextual reasoning.

---

## Viewing Quality Results

**UI**: Quality tab shows per-pipeline gate history with check-level detail (pass/warn/fail breakdown), volume trends, null rate trends, and agent reasoning.

**API**: `GET /api/pipelines/{id}/quality` returns recent gate records with full check metadata and agent reasoning.

**CLI**: `python -m cli quality {pipeline_name}`

---

## Tips

- Quality gate is **connector-agnostic** — it queries the target engine interface, not specific databases
- The gate runs on the **staging table**, not the production table — failed data never touches production
- The agent learns from approval patterns — rejected HALT overrides inform future decisions
- Monitor quality trends over time to catch gradual degradation (slowly increasing null rates)
