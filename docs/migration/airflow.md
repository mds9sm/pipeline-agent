# Airflow Migration

Migrate existing Airflow DAGs to DAPOS pipelines, transforms, and custom steps. The migration tool analyzes your DAG archive, proposes equivalent DAPOS resources, and auto-creates everything on approval.

---

## How It Works

The migration follows a 5-step lifecycle:

1. **Upload** — Upload a zip/tar.gz archive of your Airflow repo with optional additional context
2. **Parse** — 3-phase parsing: Python AST analysis, YAML config heuristic detection, agentic repo scanning
3. **Analyze** — Agent examines parsed DAGs + repo structure + your context to propose DAPOS equivalents
4. **Review** — Review proposed pipelines, transforms, connectors, custom steps, and dependencies
5. **Execute** — Auto-create all approved resources (connectors as DRAFT stubs, pipelines PAUSED)

## Upload with Additional Context

You can provide supplementary context alongside the archive — README files, architecture docs, team conventions, connection details, etc. This helps the agent produce more accurate migration plans, especially for repos with non-standard patterns.

**Via UI:** Click "+ Add Context" in the Migration view, paste your docs, then upload the archive.

**Via API:**
```bash
curl -X POST /api/migration/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@airflow_dags.zip" \
  -F "context=Our repo uses YAML-configured template DAGs. Redshift is our data warehouse.
Database connections: redshift_default (analytics), redshift_prd_backfill (prod).
Most DAGs are in dags/transform/conf/*.yaml with SQL in dags/transform/sql/."
```

The context is stored on the migration record and used for all subsequent analysis (including re-analyze). Maximum 50KB.

## What Gets Parsed

### Phase 1: Python AST (Universal)
- Standard `DAG()` and `with DAG(...)` patterns
- All operator types (PythonOperator, BashOperator, SQL operators, sensors, etc.)
- Task dependencies (`>>`, `<<`, `set_downstream`)
- Function source code extraction for Python/Bash operators
- Jinja template variable conversion (15+ mappings)

### Phase 2: YAML Config Heuristic
- Detects YAML files with Airflow keywords (`schedule_interval`, `steps`, `conn_id`)
- Parses steps, views, downstream DAGs, environment parameters
- Resolves SQL file references

### Phase 3: Agentic Repo Scan
- Full file tree analysis
- README extraction
- Config file sampling
- SQL file sampling
- User-provided context injection

## What Gets Proposed

| Resource | Description |
|----------|-------------|
| **Pipelines** | One per DAG, mapped to DAPOS source/target with schedule and strategy |
| **Transforms** | SQL transforms extracted from SQL operators and file references |
| **Custom Steps** | Converted PythonOperator/BashOperator code with Airflow→DAPOS replacements |
| **Connectors** | Stub connectors (DRAFT) for each unique Airflow connection |
| **Dependencies** | Inter-pipeline dependencies from DAG trigger relationships |

## Jinja Template Conversion

Airflow Jinja templates are automatically converted to DAPOS template variables:

| Airflow | DAPOS |
|---------|-------|
| `{{ ds }}` | `{{run_date}}` |
| `{{ execution_date }}` | `{{watermark_after}}` |
| `{{ data_interval_start }}` | `{{watermark_before}}` |
| `{{ data_interval_end }}` | `{{watermark_after}}` |
| `{{ run_id }}` | `{{run_id}}` |
| `{{ params.x }}` | `{{param_x}}` |
| `{{ var.value.x }}` | `{{var_x}}` |
| `{{ macros.ds_add(ds, N) }}` | `{{watermark_offset_N}}` |

Unconvertible Jinja blocks (`{% if %}`, `{% for %}`, custom macros) are preserved with warnings.

## Python/Bash Operator Conversion

Instead of flagging PythonOperator and BashOperator as unmapped, the migration tool converts them to DAPOS custom steps:

- `Variable.get()` → `os.environ.get()` (maps to DAPOS template variables)
- `ti.xcom_push/pull` → DAPOS run context propagation
- `from airflow.*` imports → removed (replaced with DAPOS equivalents)
- Airflow hooks → mapped to DAPOS connector references
- Function source code is extracted and included in the custom step

## Re-analyzing with Updated Context

After initial upload, you can re-analyze with updated or new context:

```bash
curl -X POST /api/migration/{id}/reanalyze \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"context": "Updated: we also have DOMO refresh DAGs that UNLOAD to S3."}'
```

## Execution

After approval, the execute endpoint creates:

1. **Connector stubs** (DRAFT status, empty code — need credentials configured)
2. **Pipelines** (PAUSED, with custom steps as StepDefinitions)
3. **SQL transforms** (linked to pipelines)
4. **Standalone custom step pipelines** (for Python/Bash tasks without a clear pipeline parent)
5. **Dependencies** (inter-pipeline relationships)

All created resources are tracked in the migration's execution log.

## API Reference

See [API Reference](../api-reference.md#airflow-migration) for the full endpoint list.
