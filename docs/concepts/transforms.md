# SQL Transforms

DAPOS includes native SQL transforms that replace dbt for in-warehouse data transformation. Transforms execute SQL against your target database, with `ref()` and `var()` template functions for composability.

---

## Overview

A SQL transform is a versioned SQL query that creates a table, view, or incremental dataset in your target database. Transforms:

- Execute as **pipeline steps** within the step DAG framework
- Support `{{ ref('table_name') }}` to reference other tables or transforms
- Support `{{ var('key') }}` for configurable parameters
- Track **column-level lineage** from source to output
- Can be **AI-generated** from natural language descriptions
- Follow **two-tier autonomy** — agent proposes, human approves

---

## Materialization Types

| Type | Behavior | When to use |
|------|----------|-------------|
| `table` | `DROP + CREATE TABLE AS` | Default. Full refresh of derived table. |
| `view` | `CREATE OR REPLACE VIEW` | Lightweight, always up-to-date. No storage cost. |
| `incremental` | `DELETE + INSERT` on unique key | Large tables where full refresh is expensive. |
| `ephemeral` | Not materialized (CTE when referenced) | Intermediate logic used by other transforms. |

---

## Template Functions

### ref()

Reference another table or transform's output:

```sql
SELECT
    o.order_date,
    COUNT(*) as order_count,
    SUM(o.total) as revenue
FROM {{ ref('demo_orders') }} o
GROUP BY o.order_date
```

`ref()` resolves in this order:
1. Other SQL transforms (by `transform_name`)
2. Pipeline target tables (by `target_table`)
3. Passthrough (raw table name if not found)

### var()

Reference configurable variables:

```sql
SELECT *
FROM {{ ref('orders') }}
WHERE created_at >= CURRENT_DATE - INTERVAL '{{ var('lookback_days') }} days'
```

Variables resolve from: step config > pipeline tags > unresolved warning.

### Template Variables

All 34+ existing template variables (`{{pipeline_id}}`, `{{run_id}}`, `{{watermark_after}}`, etc.) also work in transform SQL.

---

## API Endpoints

### Transform CRUD

| Method | Path | Description |
|--------|------|-------------|
| `POST /api/transforms` | Create a transform |
| `GET /api/transforms` | List transforms (filter by `pipeline_id`) |
| `GET /api/transforms/{id}` | Get transform detail |
| `PATCH /api/transforms/{id}` | Update SQL/config (bumps version) |
| `DELETE /api/transforms/{id}` | Delete a transform |

### Transform Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST /api/transforms/{id}/validate` | Dry-run EXPLAIN |
| `POST /api/transforms/{id}/preview` | Execute with LIMIT, return sample rows |
| `POST /api/transforms/generate` | AI-generate SQL from description |
| `GET /api/transforms/{id}/lineage` | Get parsed column lineage |

---

## Creating a Transform

### Via API

```bash
curl -X POST http://localhost:8100/api/transforms \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "transform_name": "daily_revenue",
    "sql": "SELECT DATE(created_at) as day, SUM(total) as revenue FROM {{ ref('\''demo_orders'\'') }} GROUP BY DATE(created_at)",
    "materialization": "table",
    "target_schema": "analytics",
    "target_table": "daily_revenue",
    "description": "Daily revenue from orders"
  }'
```

### Via AI Generation

```bash
curl -X POST http://localhost:8100/api/transforms/generate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Create a daily revenue summary from the orders table",
    "materialization": "table",
    "target_table": "daily_revenue"
  }'
```

### Via Chat

> "Create a transform for daily revenue from orders"

The agent generates the SQL, creates the transform in the catalog, and waits for approval.

---

## Using Transforms in Pipelines

Add a `transform` step to your pipeline's step DAG:

```json
{
  "step_name": "daily_revenue",
  "step_type": "transform",
  "depends_on": ["extract_step_id"],
  "config": {
    "transform_id": "uuid-of-transform",
    "variables": {"lookback_days": "30"}
  }
}
```

Or with inline SQL:

```json
{
  "step_name": "daily_revenue",
  "step_type": "transform",
  "config": {
    "sql": "SELECT DATE(created_at) as day, SUM(total) as revenue FROM {{ ref('demo_orders') }} GROUP BY DATE(created_at)",
    "materialization": "table",
    "target_schema": "analytics",
    "target_table": "daily_revenue"
  }
}
```

---

## Validation

Before executing a transform, validate the SQL:

```bash
curl -X POST http://localhost:8100/api/transforms/{id}/validate \
  -H "Authorization: Bearer $TOKEN"
```

Returns the EXPLAIN plan or an error message. No data is modified.

## Preview

See sample output before materializing:

```bash
curl -X POST http://localhost:8100/api/transforms/{id}/preview?limit=5 \
  -H "Authorization: Bearer $TOKEN"
```

---

## Column Lineage

Transforms automatically track column lineage — which source columns feed which output columns. View lineage:

```bash
curl http://localhost:8100/api/transforms/{id}/lineage \
  -H "Authorization: Bearer $TOKEN"
```

Lineage is parsed from the SQL using heuristics (explicit aliases, table.column references). For complex SQL, the agent can enrich lineage on request.
