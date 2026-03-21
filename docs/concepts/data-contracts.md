# Data Contracts

Data contracts formalize the relationship between a producer pipeline and a consumer pipeline. They define what data is guaranteed, how fresh it must be, and who owns cleanup.

---

## Why Data Contracts?

Without contracts, pipelines are loosely coupled — a producer can change its schema, stop running, or delete intermediate data without warning, silently breaking downstream consumers.

Data contracts make these agreements **explicit and enforced**:

- Producer guarantees freshness (SLA)
- Producer guarantees schema (required columns)
- Retention window is defined (how long data lives)
- Cleanup ownership is clear (who deletes intermediate data)

---

## Contract Fields

| Field | Type | Description |
|-------|------|-------------|
| `producer_pipeline_id` | string | Pipeline producing the data |
| `consumer_pipeline_id` | string | Pipeline consuming the data |
| `required_columns` | list | Columns the consumer needs to exist |
| `freshness_sla_minutes` | int | Maximum staleness consumer tolerates (default 60) |
| `retention_hours` | int | How long producer keeps data (default 168 = 7 days) |
| `cleanup_ownership` | enum | Who deletes intermediate data |
| `status` | enum | ACTIVE, VIOLATED, PAUSED, ARCHIVED |

### Cleanup Ownership

| Policy | Meaning |
|--------|---------|
| `producer_ttl` | Producer auto-deletes after retention_hours |
| `consumer_acknowledges` | Consumer signals when done; producer deletes after |
| `none` | No automatic cleanup (manual management) |

---

## Contract Validation

The monitor loop (every 5 minutes) validates all active contracts:

### Freshness SLA Check
- Reads producer's last successful run timestamp
- If `staleness > freshness_sla_minutes` → creates FRESHNESS_SLA violation

### Schema Check
- Verifies producer's target table contains all `required_columns`
- If any column missing or type incompatible → creates SCHEMA_MISMATCH violation

### Retention Check
- If cleanup_ownership = `producer_ttl`, verifies data is available for at least `retention_hours`
- If data deleted too early → creates RETENTION_EXPIRED violation

---

## Violation Types

| Type | Description |
|------|-------------|
| `freshness_sla` | Producer data older than SLA |
| `schema_mismatch` | Required columns missing or type changed |
| `retention_expired` | Producer deleted data before retention window |

Violations set the contract status to **VIOLATED** and dispatch alerts based on tier.

---

## Cleanup Guard

DAPOS enforces a critical safety rule: **never delete unconsumed data**.

When a hook or cleanup step attempts to DELETE from an intermediate table:
- The agent checks if any consumer contracts reference that table
- If consumers haven't acknowledged processing, the DELETE is blocked
- Static `DELETE FROM table` without watermark/batch bounds is always rejected

---

## Creating a Contract

```
POST /api/data-contracts
{
  "producer_pipeline_id": "raw-orders",
  "consumer_pipeline_id": "daily-aggregates",
  "required_columns": ["order_id", "amount", "created_at"],
  "freshness_sla_minutes": 30,
  "retention_hours": 72,
  "cleanup_ownership": "producer_ttl"
}
```

**Validation on create**:
- Producer and consumer must be different pipelines
- No duplicate contracts for the same pair
- Both pipelines must exist
- Auto-creates a dependency relationship (consumer depends on producer)

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/data-contracts` | Create contract |
| GET | `/api/data-contracts` | List all contracts |
| GET | `/api/data-contracts/{id}` | Contract detail with status |
| PATCH | `/api/data-contracts/{id}` | Update SLA, retention, cleanup |
| DELETE | `/api/data-contracts/{id}` | Remove contract |
| POST | `/api/data-contracts/{id}/validate` | Manual validation check |
| GET | `/api/data-contracts/{id}/violations` | List violations |

---

## DAG Visualization

Contracts appear as special edges in the DAG view, showing the producer→consumer relationship with SLA and status badges.
