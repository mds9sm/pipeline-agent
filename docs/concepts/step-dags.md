# Step DAGs

Step DAGs replace the fixed extract→load→promote flow with composable, configurable pipeline steps. This is DAPOS's equivalent of Airflow's task DAGs.

## Overview

By default, pipelines use the **legacy execution path** — a fixed sequence of extract, load, quality gate, promote, cleanup, hooks. When you define `steps` on a pipeline, it switches to the **step DAG path** — steps execute in topological order based on `depends_on` relationships.

## Step Types

| Type | Purpose | Config |
|------|---------|--------|
| `extract` | Extract data from source to staging | *(uses pipeline source config)* |
| `transform` | Run SQL on the target warehouse | `sql`: SQL with template variables |
| `quality_gate` | Run 7-check quality gate | *(uses pipeline quality_config)* |
| `promote` | Load staging data + promote to target table | *(uses pipeline target config)* |
| `cleanup` | Clean up staging files | *(no config needed)* |
| `hook` | Execute post-promotion SQL | `sql`: SQL with template variables |
| `sensor` | Poll a SQL condition until true | `sql`, `poll_seconds`, `timeout_seconds` |
| `custom` | Extensible SQL execution | `sql`: arbitrary SQL |

## Defining Steps

```yaml
steps:
  - step_id: s1
    step_name: extract_orders
    step_type: extract
    depends_on: []

  - step_id: s2
    step_name: deduplicate
    step_type: transform
    depends_on: [s1]
    config:
      sql: |
        DELETE FROM {{target_schema}}.orders_stg
        WHERE id NOT IN (
          SELECT MIN(id) FROM {{target_schema}}.orders_stg GROUP BY order_id
        )

  - step_id: s3
    step_name: enrich
    step_type: transform
    depends_on: [s2]
    config:
      sql: |
        UPDATE {{target_schema}}.orders_stg o
        SET region = g.region
        FROM {{target_database}}.public.geo_lookup g
        WHERE o.country_code = g.code

  - step_id: s4
    step_name: quality_check
    step_type: quality_gate
    depends_on: [s3]

  - step_id: s5
    step_name: promote
    step_type: promote
    depends_on: [s4]

  - step_id: s6
    step_name: cleanup
    step_type: cleanup
    depends_on: [s5]
```

## Execution Model

1. **Topological sort** — Steps are sorted by dependencies. Cycles are rejected at validation time.
2. **Sequential execution** — Steps run one at a time in dependency order.
3. **Step context** — An in-memory dict (like Airflow's XCom) passes data between steps. Each step's output is available to downstream steps.
4. **Per-step retry** — Each step has its own `retry_max`. Extract can retry 3x while promote retries 0x.
5. **Skip on fail** — When `skip_on_fail: true`, a failed dependency skips the downstream step instead of failing the entire run.
6. **Halt vs Fail** — Quality gate steps halt the DAG (preserving staging for investigation) rather than failing it.

## Step Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `step_id` | string | auto-generated | Unique identifier |
| `step_name` | string | required | Human-readable name |
| `step_type` | enum | required | One of the 8 step types |
| `depends_on` | list[step_id] | `[]` | Steps that must complete first |
| `config` | dict | `{}` | Type-specific configuration |
| `retry_max` | int | `0` | Max retry attempts on failure |
| `timeout_seconds` | int | `0` | Step timeout (0 = no timeout) |
| `skip_on_fail` | bool | `false` | Skip if dependency failed |
| `enabled` | bool | `true` | Disabled steps are excluded |

## Template Variables in SQL Steps

All `transform`, `hook`, `sensor`, and `custom` steps support 34 template variables:

**Run context:** `{{pipeline_id}}`, `{{pipeline_name}}`, `{{run_id}}`, `{{batch_id}}`, `{{watermark_before}}`, `{{watermark_after}}`, `{{rows_extracted}}`, `{{rows_loaded}}`, `{{started_at}}`, `{{completed_at}}`, `{{run_mode}}`, `{{source_schema}}`, `{{source_table}}`, `{{target_schema}}`, `{{target_table}}`

**Connection/environment:** `{{environment}}`, `{{source_host}}`, `{{source_database}}`, `{{source_user}}`, `{{source_port}}`, `{{target_host}}`, `{{target_database}}`, `{{target_user}}`, `{{target_port}}`, `{{target_ddl}}`

**Upstream (data-triggered runs):** `{{upstream_run_id}}`, `{{upstream_pipeline_id}}`, `{{upstream_watermark_before}}`, `{{upstream_watermark_after}}`, `{{upstream_rows_extracted}}`, `{{upstream_rows_loaded}}`, `{{upstream_started_at}}`, `{{upstream_completed_at}}`, `{{upstream_batch_id}}`

## Sensor Pattern

Sensors poll a SQL query until it returns at least one row:

```yaml
- step_name: wait_for_upstream
  step_type: sensor
  config:
    sql: "SELECT 1 FROM {{target_database}}.public.daily_summary WHERE date = CURRENT_DATE"
    poll_seconds: 30
    timeout_seconds: 600
```

## Backward Compatibility

Pipelines with empty `steps` (or no steps field) use the legacy execution path unchanged. No migration required.

## API

```bash
# Get step definitions
GET /api/pipelines/{id}/steps

# Get step executions for a run
GET /api/runs/{run_id}/steps

# Validate a step DAG (cycle detection)
POST /api/pipelines/{id}/steps/validate

# Preview execution order
GET /api/pipelines/{id}/steps/preview
```
