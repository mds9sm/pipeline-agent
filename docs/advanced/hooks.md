# Post-Promotion Hooks

Post-promotion hooks execute SQL statements after a pipeline run successfully promotes data to the target table. They enable XCom-style computed metadata, cleanup operations, and cross-pipeline data sharing.

---

## How Hooks Work

```
Extract → Load → Quality Gate → Promote → Hooks Execute → Complete
```

Hooks run **after promotion**, so they can reference the newly-promoted data. Each hook is a SQL template with access to 34 template variables.

---

## Hook Definition

```json
{
  "sql": "INSERT INTO run_metadata (pipeline_id, run_id, row_count, watermark) VALUES ('{{pipeline_id}}', '{{run_id}}', {{rows_extracted}}, '{{watermark_after}}')",
  "metadata_key": "run_summary",
  "enabled": true,
  "fail_pipeline_on_error": false,
  "timeout_seconds": 30
}
```

| Field | Description |
|-------|-------------|
| `sql` | SQL template with `{{variable}}` placeholders |
| `metadata_key` | Namespace key for storing results (XCom-style) |
| `enabled` | Toggle without removing |
| `fail_pipeline_on_error` | If true, hook failure marks the entire run as FAILED |
| `timeout_seconds` | Max execution time (default 30s) |

---

## Template Variables (34 Total)

### Run Context (15)

| Variable | Example | Description |
|----------|---------|-------------|
| `{{run_id}}` | `run-abc123` | Current run identifier |
| `{{pipeline_id}}` | `pipe-xyz` | Pipeline identifier |
| `{{pipeline_name}}` | `demo-stripe-charges` | Pipeline name |
| `{{watermark_before}}` | `2026-03-20T00:00:00` | Watermark at extraction start |
| `{{watermark_after}}` | `2026-03-21T00:00:00` | Watermark at extraction end |
| `{{rows_extracted}}` | `1500` | Rows pulled from source |
| `{{rows_loaded}}` | `1500` | Rows loaded to staging |
| `{{staging_size_bytes}}` | `45000` | Staging data size |
| `{{batch_count}}` | `1` | Number of extraction batches |
| `{{run_started_at}}` | `2026-03-21T10:00:00Z` | Run start timestamp |
| `{{run_mode}}` | `SCHEDULED` | SCHEDULED, MANUAL, BACKFILL, DATA_TRIGGERED |
| `{{source_schema}}` | `ecommerce` | Source schema name |
| `{{source_table}}` | `orders` | Source table name |
| `{{target_schema}}` | `public` | Target schema name |
| `{{target_table}}` | `orders` | Target table name |

### Connection/Environment (10)

| Variable | Example | Description |
|----------|---------|-------------|
| `{{environment}}` | `production` | Pipeline environment |
| `{{source_host}}` | `db.example.com` | Source connection host |
| `{{source_database}}` | `ecommerce` | Source database name |
| `{{source_user}}` | `reader` | Source connection user |
| `{{source_port}}` | `3306` | Source connection port |
| `{{target_host}}` | `warehouse.example.com` | Target connection host |
| `{{target_database}}` | `analytics` | Target database name |
| `{{target_user}}` | `loader` | Target connection user |
| `{{target_port}}` | `5432` | Target connection port |
| `{{target_ddl}}` | `CREATE TABLE...` | Target table DDL |

### Upstream Context (9)

Available when the run was triggered by an upstream pipeline completion:

| Variable | Description |
|----------|-------------|
| `{{upstream_pipeline_id}}` | Upstream pipeline identifier |
| `{{upstream_run_id}}` | Upstream run identifier |
| `{{upstream_rows_extracted}}` | Rows the upstream pipeline extracted |
| `{{upstream_watermark_after}}` | Upstream pipeline's watermark |
| `{{upstream_load_type}}` | Upstream load type (merge/append) |
| `{{upstream_merge_keys}}` | Upstream merge keys (comma-separated) |
| `{{upstream_target_schema}}` | Upstream target schema |
| `{{upstream_target_table}}` | Upstream target table |
| `{{upstream_completed_at}}` | Upstream run completion timestamp |

---

## Use Cases

### Computed Metadata (XCom-Style)
```sql
INSERT INTO pipeline_metadata (key, value, run_id)
VALUES ('daily_total', (SELECT SUM(amount) FROM {{target_schema}}.{{target_table}} WHERE date = CURRENT_DATE), '{{run_id}}')
```

### Cleanup Consumed Rows
```sql
DELETE FROM {{target_schema}}.staging_{{target_table}}
WHERE watermark <= '{{watermark_after}}'
  AND run_id != '{{run_id}}'
```

### Cross-Environment Portability
Same SQL works across test/staging/production because connection details are variables:
```sql
INSERT INTO {{target_database}}.audit.load_log
(host, database, table_name, rows, timestamp)
VALUES ('{{source_host}}', '{{source_database}}', '{{source_table}}', {{rows_extracted}}, NOW())
```

### Cascading Aggregation
```sql
INSERT INTO {{target_schema}}.daily_summary
SELECT DATE(created_at), COUNT(*), SUM(amount)
FROM {{target_schema}}.{{target_table}}
WHERE created_at >= '{{watermark_before}}'
  AND created_at < '{{watermark_after}}'
GROUP BY DATE(created_at)
ON CONFLICT (date) DO UPDATE SET count = EXCLUDED.count, total = EXCLUDED.total
```

---

## Metadata Namespaces

Hook results are stored in pipeline metadata under three namespaces:

| Namespace | Purpose |
|-----------|---------|
| `default` | General XCom-style key/value pairs |
| `hooks` | Results from post-promotion hook execution |
| `upstream` | Trigger context from upstream pipeline |

Access via: `GET /api/pipelines/{id}/metadata?namespace=hooks`

---

## Testing Hooks

Validate hook SQL without executing:

```
POST /api/pipelines/{id}/hooks/test
{
  "sql": "SELECT COUNT(*) FROM {{target_schema}}.{{target_table}}",
  "metadata_key": "test"
}
```

Returns: validated SQL with variables resolved, any syntax issues.

---

## Safety

- **Cleanup guard**: `DELETE` without watermark/batch bounds is rejected by the agent
- **Timeout**: hooks that exceed `timeout_seconds` are killed
- **fail_pipeline_on_error**: when false (default), hook failure logs a warning but the run stays COMPLETE
- **Enabled toggle**: disable a hook without removing it from the configuration
