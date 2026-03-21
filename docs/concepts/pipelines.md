# Pipelines

A **pipeline** is the core unit of work in DAPOS. It defines a data flow from a source to a target with a defined strategy, schedule, and quality expectations.

## Pipeline Contract

Every pipeline is defined by a `PipelineContract` — a structured specification that captures:

| Section | Fields | Purpose |
|---------|--------|---------|
| **Identity** | pipeline_id, pipeline_name, version, environment | Uniquely identifies the pipeline |
| **Source** | connector_id, host, port, database, schema, table, user, password | Where data comes from |
| **Target** | connector_id, host, port, database, schema, table, user, password, options, ddl | Where data goes |
| **Strategy** | refresh_type, replication_method, incremental_column, load_type, merge_keys | How data moves |
| **Schedule** | cron, retry_max_attempts, retry_backoff_seconds, timeout_seconds | When data moves |
| **Quality** | quality_config (7 checks), auto_approve_additive_schema | Acceptance criteria |
| **Steps** | step definitions (extract, transform, gate, promote, etc.) | Composable DAG (optional) |
| **Observability** | tier, owner, tags, freshness_column, notification_policy_id | SLA and alerting |

## Lifecycle

```
Created → Active → [Running] → Active → ...
                       ↓
                   Halted (quality gate)
                       ↓
                   Paused (manual)
                       ↓
                   Archived
```

## Refresh Types

| Type | Behavior | Use Case |
|------|----------|----------|
| `full` | Extract entire source table every run | Small tables, no reliable timestamp |
| `incremental` | Extract rows after last watermark | Large tables with updated_at column |

## Load Types

| Type | Behavior | Use Case |
|------|----------|----------|
| `append` | Insert all extracted rows | Event logs, immutable data |
| `merge` | Upsert using merge_keys | Mutable dimension tables |
| `replace` | Truncate + insert | Full refresh snapshots |

## Replication Methods

| Method | Behavior |
|--------|----------|
| `watermark` | Use incremental_column to track position |
| `full_table` | Extract entire table |
| `custom` | Connector-specific logic |

## Creating a Pipeline

### Via Chat
```
I want to set up a pipeline from MySQL orders table to PostgreSQL
```

### Via API
```bash
curl -X POST http://localhost:8100/api/pipelines \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_connector_id": "...",
    "source_schema": "ecommerce",
    "source_table": "orders",
    "target_schema": "raw",
    "schedule_cron": "0 * * * *",
    "strategy": {
      "refresh_type": "incremental",
      "replication_method": "watermark",
      "incremental_column": "updated_at",
      "load_type": "merge",
      "merge_keys": ["order_id"]
    }
  }'
```

### Via CLI
```bash
python -m cli pipelines create \
  --source-connector mysql-demo \
  --source-schema ecommerce \
  --source-table orders \
  --target-schema raw \
  --schedule "0 * * * *"
```

## YAML Export/Import

Pipelines can be exported to YAML for version control:

```yaml
pipeline_name: demo-ecommerce-orders
environment: production
status: active
tier: 2

source:
  connector_id: abc123
  host: demo-mysql
  port: 3307
  database: ecommerce
  schema: ecommerce
  table: orders

target:
  schema: raw
  table: orders

strategy:
  refresh_type: incremental
  replication_method: watermark
  incremental_column: updated_at
  load_type: merge
  merge_keys: [order_id]

schedule:
  cron: "0 * * * *"
  retry_max_attempts: 3
  timeout_seconds: 3600

steps:
  - step_name: extract
    step_type: extract
  - step_name: gate
    step_type: quality_gate
    depends_on: [extract-step-id]
  - step_name: promote
    step_type: promote
    depends_on: [gate-step-id]
```
