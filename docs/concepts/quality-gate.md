# Quality Gate

The quality gate is a connector-agnostic, 7-check verification system that runs after data loading but before promotion to the target table. It ensures only trusted data reaches production.

---

## How It Works

Every pipeline run follows this flow:

```
Extract → Load to Staging → Quality Gate → Promote (or Halt)
```

The gate evaluates 7 independent checks against the staging table. Each check returns **PASS**, **WARN**, or **FAIL**. The overall decision is:

| Condition | Decision |
|-----------|----------|
| Any check = FAIL | **HALT** — data stays in staging, not promoted |
| Any check = WARN, `promote_on_warn=true` | **PROMOTE_WITH_WARNING** — data promoted, alert raised |
| All checks = PASS | **PROMOTE** — data merged/appended to target |

---

## The 7 Checks

### 1. Count Reconciliation

Compares extracted row count vs. staged row count. Detects data loss during loading.

- **Tolerance**: configurable (default 0.1%)
- **WARN**: staged count differs by > tolerance but < 5%
- **FAIL**: staged count differs by > 5%

### 2. Schema Consistency

Validates that staging table columns match the pipeline contract's column mappings plus standard metadata columns.

**Metadata columns** (added by every source connector):
- `_extracted_at` — UTC extraction timestamp
- `_source_schema` — originating schema name
- `_source_table` — originating table name
- `_row_hash` — SHA-256 hash of all column values

**Checks for**: missing columns, unexpected type mismatches, extra columns not in contract.

### 3. Primary Key Uniqueness

When `merge_keys` are defined, checks for duplicate key groups in the staging table.

- **WARN**: cardinality deviation > 50% from baseline
- **FAIL**: actual duplicate merge key values detected

This prevents corrupted merges where duplicate keys would create phantom rows.

### 4. Null Rate Analysis

Uses **z-score statistical analysis** to detect catastrophic null rate jumps. Compares current null rates against a rolling baseline.

- **Example**: if a column historically has < 5% nulls but current batch has 45% nulls, that's a z-score spike
- **WARN**: null rate z-score > 2.0
- **FAIL**: null rate z-score > 3.0 or any non-nullable column has nulls

### 5. Volume Z-Score

Detects anomalous row volume using a **30-run rolling average**.

- Requires 5+ historical runs to build baseline
- **WARN**: z-score > 2.0 (significantly more or fewer rows than usual)
- **FAIL**: z-score > 3.0 (extreme deviation)
- **Example**: pipeline usually loads ~1000 rows, suddenly loads 50 → FAIL

### 6. Sample Verification

Quick sanity check that staging row count matches extraction count within 0.1%. Acts as a fast double-check for count reconciliation with tighter bounds.

### 7. Freshness Check

For **incremental pipelines** only, checks staleness of the maximum watermark value against the schedule interval.

- **WARN**: watermark age > 2x schedule interval
- **FAIL**: watermark age > 5x schedule interval
- **Skip**: not applicable for full-refresh pipelines

---

## First-Run Leniency

On the **very first successful run** (no prior COMPLETE runs in history), all FAILs are automatically downgraded to WARNs. This allows the first run to:

1. Establish baseline metrics (row counts, null rates, cardinality)
2. Promote initial data to the target table
3. Create the reference point for future comparisons

Without first-run leniency, every new pipeline would halt on its first run because there's no baseline to compare against.

---

## Quality Configuration

Each pipeline has a `QualityConfig` that controls thresholds:

```yaml
quality_config:
  count_tolerance: 0.001        # 0.1% row count tolerance
  null_rate_z_threshold: 2.0    # z-score for null rate warnings
  volume_z_warn: 2.0            # z-score for volume warnings
  volume_z_fail: 3.0            # z-score for volume failures
  freshness_warn_multiplier: 2  # schedule_interval * 2 = warn
  freshness_fail_multiplier: 5  # schedule_interval * 5 = fail
  promote_on_warn: true         # promote with warnings or halt
  min_historical_runs: 5        # runs needed before z-score kicks in
```

**Tier defaults** provide sensible starting thresholds:

| Setting | Tier 1 | Tier 2 | Tier 3 |
|---------|--------|--------|--------|
| count_tolerance | 0.001 | 0.01 | 0.05 |
| volume_z_fail | 2.5 | 3.0 | 4.0 |
| promote_on_warn | false | true | true |

---

## Viewing Quality Results

**UI**: Quality tab shows per-pipeline gate history with check-level detail (pass/warn/fail breakdown), volume trends, and null rate trends.

**API**: `GET /api/pipelines/{id}/quality` returns recent gate records with full check metadata.

**CLI**: `python -m cli quality {pipeline_name}`

---

## Tips

- Quality gate is **connector-agnostic** — it queries the target engine interface, not specific databases
- The gate runs on the **staging table**, not the production table — failed data never touches production
- Merge with `promote_on_warn: false` on Tier 1 pipelines for strictest data quality
- Monitor quality trends over time to catch gradual degradation (slowly increasing null rates)
