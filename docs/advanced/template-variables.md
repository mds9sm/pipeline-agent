# Template Variables Reference

Template variables are `{{variable_name}}` placeholders available in post-promotion hooks, transform steps, and sensor conditions. They are resolved at execution time with actual run and connection values.

---

## All Variables (34 Total)

### Run Context

| Variable | Type | Description |
|----------|------|-------------|
| `{{run_id}}` | string | Current run UUID |
| `{{pipeline_id}}` | string | Pipeline UUID |
| `{{pipeline_name}}` | string | Human-readable pipeline name |
| `{{watermark_before}}` | timestamp | Extraction window start |
| `{{watermark_after}}` | timestamp | Extraction window end |
| `{{rows_extracted}}` | integer | Rows pulled from source |
| `{{rows_loaded}}` | integer | Rows loaded to staging |
| `{{staging_size_bytes}}` | integer | Data size in staging |
| `{{batch_count}}` | integer | Extraction batch count |
| `{{run_started_at}}` | timestamp | Run start time (UTC) |
| `{{run_mode}}` | string | SCHEDULED, MANUAL, BACKFILL, DATA_TRIGGERED |
| `{{source_schema}}` | string | Source schema/database name |
| `{{source_table}}` | string | Source table name |
| `{{target_schema}}` | string | Target schema name |
| `{{target_table}}` | string | Target table name |
| `{{timestamp_now}}` | timestamp | Current UTC timestamp |

### Connection / Environment

| Variable | Type | Description |
|----------|------|-------------|
| `{{environment}}` | string | Pipeline environment (production, staging, test) |
| `{{source_host}}` | string | Source connection hostname |
| `{{source_database}}` | string | Source database name |
| `{{source_user}}` | string | Source connection username |
| `{{source_port}}` | string | Source connection port |
| `{{target_host}}` | string | Target connection hostname |
| `{{target_database}}` | string | Target database name |
| `{{target_user}}` | string | Target connection username |
| `{{target_port}}` | string | Target connection port |
| `{{target_ddl}}` | string | Target table CREATE TABLE DDL |

### Upstream Context

Available only when `triggered_by_run_id` is set (dependency-triggered runs):

| Variable | Type | Description |
|----------|------|-------------|
| `{{upstream_pipeline_id}}` | string | Triggering pipeline UUID |
| `{{upstream_run_id}}` | string | Triggering run UUID |
| `{{upstream_rows_extracted}}` | integer | Rows the upstream extracted |
| `{{upstream_watermark_after}}` | timestamp | Upstream watermark value |
| `{{upstream_load_type}}` | string | Upstream load type (merge/append) |
| `{{upstream_merge_keys}}` | string | Upstream merge keys (comma-separated) |
| `{{upstream_target_schema}}` | string | Upstream target schema |
| `{{upstream_target_table}}` | string | Upstream target table |
| `{{upstream_completed_at}}` | timestamp | Upstream completion time |

---

## Usage Examples

### Environment-Portable Audit Log
```sql
INSERT INTO {{target_database}}.audit.pipeline_runs
  (environment, host, pipeline, rows, completed_at)
VALUES
  ('{{environment}}', '{{source_host}}', '{{pipeline_name}}', {{rows_extracted}}, '{{timestamp_now}}')
```

Same SQL works in test (`target_database=test_db`) and production (`target_database=prod_db`).

### Consume-and-Merge Pattern
```sql
DELETE FROM {{target_schema}}.staging_{{source_table}}
WHERE watermark_value <= '{{watermark_after}}'
  AND watermark_value >= '{{watermark_before}}'
```

### Upstream-Aware Aggregation
```sql
INSERT INTO {{target_schema}}.daily_metrics
SELECT '{{upstream_pipeline_id}}' as source_pipeline,
       COUNT(*) as row_count,
       '{{upstream_watermark_after}}' as data_through
FROM {{upstream_target_schema}}.{{upstream_target_table}}
WHERE _extracted_at >= '{{upstream_watermark_after}}'::timestamp - interval '1 hour'
```

### Conditional Cleanup by Run Mode
```sql
-- Only clean up staging for scheduled runs, keep for backfills
DO $$
BEGIN
  IF '{{run_mode}}' = 'SCHEDULED' THEN
    DELETE FROM {{target_schema}}.temp_{{target_table}}
    WHERE batch_watermark < '{{watermark_before}}';
  END IF;
END $$;
```

---

## Resolution

Variables are resolved by the `_render_hook_sql()` method in `agent/autonomous.py` using Python string `replace()`. Unresolved variables (e.g., upstream vars when not triggered by upstream) remain as literal strings.

---

## Metadata Namespaces

Hook results can be stored under named keys for downstream access:

| Namespace | Key Source | Access |
|-----------|-----------|--------|
| `default` | `metadata_key` on hook | `GET /api/pipelines/{id}/metadata?namespace=default` |
| `hooks` | Auto-stored by hook name | `GET /api/pipelines/{id}/metadata?namespace=hooks` |
| `upstream` | Propagated from trigger | `GET /api/pipelines/{id}/metadata?namespace=upstream` |
