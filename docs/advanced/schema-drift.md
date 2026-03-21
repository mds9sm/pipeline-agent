# Schema Drift Detection

DAPOS automatically detects when source schemas change and responds based on configurable per-tier policies — from auto-accepting additive changes to halting pipelines for review.

---

## How Detection Works

The **monitor loop** (every 5 minutes) profiles each active pipeline's source:

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
2. **Downstream impact**: which pipelines and columns are affected (via column lineage)
3. **Risk assessment**: is this safe to auto-apply or does it need review?
4. **Recommendation**: propose action based on policy + context

For `propose` actions, a `ContractChangeProposal` is created with:
- Current state vs. proposed state
- Impact analysis (affected downstream pipelines)
- Agent's confidence level
- Reasoning for the recommendation

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
