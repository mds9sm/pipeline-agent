# Schema Drift Detection

DAPOS automatically detects when source schemas change and responds based on configurable per-tier policies — from auto-accepting additive changes to halting pipelines for review.

> **Agentic behavior**: Schema drift detection runs both at **pipeline execution time** (pre-extract check) and on the monitor tick. When drift is detected, the agent generates migration SQL via `generate_migration_sql()` with contextual reasoning about safe type widening, data preservation, and downstream impact. A template-based fallback (`_rule_based_migration_sql()`) exists for when the API key is unavailable.

---

## How Detection Works

Schema drift is detected in **two places**:

1. **Pre-extract check** (at pipeline run time) — Before extraction begins, the runner profiles the source and compares against `column_mappings`. If drift is detected, the agent generates migration SQL and either auto-applies or creates a proposal based on policy.
2. **Monitor loop** (every 5 minutes) — Periodic scan of all active pipelines for drift that may have occurred between runs.

The pre-extract check ensures drift is caught **before** it causes a COPY failure, not after. The monitor loop catches drift on pipelines that haven't run recently.

For both paths, the system profiles each active pipeline's source:

1. Queries the live source table for current columns and types
2. Compares against the pipeline contract's `column_mappings`
3. Detects four types of drift:
   - **New columns** — columns in source not in contract
   - **Dropped columns** — columns in contract not in source
   - **Type changes** — column exists but type differs
   - **Nullable changes** — column nullable status changed

---

## Schema Change Policies

Each pipeline has a `schema_change_policy` with configurable actions per drift type:

### Actions

| Action | Behavior |
|--------|----------|
| `auto_add` | Immediately add to contract, no approval needed |
| `auto_widen` | Expand type to accommodate (INT→BIGINT, VARCHAR(50)→VARCHAR(255)) |
| `auto_accept` | Accept change silently |
| `propose` | Create proposal for human approval |
| `halt` | Pause pipeline, require explicit action |
| `ignore` | Skip the change entirely |

### Per-Tier Defaults

| Drift Type | Tier 1 (Production) | Tier 2 (Standard) | Tier 3 (Casual) |
|------------|---------------------|--------------------|--------------------|
| `on_new_column` | auto_add | auto_add | auto_add |
| `on_dropped_column` | halt | propose | ignore |
| `on_type_change` | propose | auto_widen | auto_widen |
| `on_nullable_change` | propose | auto_accept | auto_accept |
| `propagate_downstream` | true | true | false |

### Tier 1 Rationale
- New columns are safe (additive)
- Dropped columns **halt** the pipeline — production data loss is unacceptable without review
- Type changes need human review — a VARCHAR→INT change could lose data
- Nullable changes need review — NOT NULL→NULL could indicate data quality issues

### Tier 3 Rationale
- Everything except drops is auto-accepted — low-priority data, minimal risk
- Downstream propagation disabled — casual pipelines shouldn't cascade changes

---

## Agent Analysis

When drift is detected, the agent analyzes it with context:

1. **What changed**: specific columns and types affected
2. **Migration SQL generation**: agent calls `generate_migration_sql()` to produce ALTER TABLE statements with reasoning about safe type widening, data preservation, and rollback strategy
3. **Downstream impact**: which pipelines and columns are affected (via column lineage)
4. **Risk assessment**: is this safe to auto-apply or does it need review?
5. **Recommendation**: propose action based on policy + context

For `propose` actions, a `ContractChangeProposal` is created with:
- Current state vs. proposed state (including agent-generated migration SQL)
- Impact analysis (affected downstream pipelines)
- Agent's confidence level
- Reasoning for the recommendation

### Migration SQL Generation

> **⚠️ RULE-BASED FALLBACK**: When the API key is unavailable, `_rule_based_migration_sql()` generates template-based ALTER TABLE statements (e.g., `ALTER TABLE t ADD COLUMN c type`). This lacks contextual reasoning about safe type widening or data preservation.

---

## Downstream Propagation

When `propagate_downstream: true`, schema changes flow to all consumer pipelines:

```
Source table adds column "region"
    ↓
Producer pipeline auto_adds "region" to its contract
    ↓
Consumer pipeline 1: "region" added to column_mappings
Consumer pipeline 2: "region" added to column_mappings
    ↓
Data contracts updated to include "region" in required_columns
```

---

## Override Per-Pipeline

```
PATCH /api/pipelines/{id}
{
  "schema_change_policy": {
    "on_new_column": "propose",
    "on_dropped_column": "halt",
    "on_type_change": "halt",
    "on_nullable_change": "propose",
    "propagate_downstream": true
  }
}
```

Override any tier default for a specific pipeline.

---

## Viewing Drift History

Schema changes are logged in the pipeline changelog:

```
GET /api/pipelines/{id}/changelog
```

Each entry includes: change type, affected columns, action taken (auto-applied or proposed), and who approved (if applicable).

---

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/schema-policy-defaults` | Tier default policies |
| PATCH | `/api/pipelines/{id}` | Override schema_change_policy |
| GET | `/api/approvals` | Pending schema change proposals |
| POST | `/api/approvals/{id}` | Approve/reject schema change |
