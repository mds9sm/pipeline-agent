# Multi-Environment Setup

DAPOS supports running pipelines across test, staging, and production environments using connection parameterization and GitOps branching.

---

## Environment Strategy

Each pipeline has an `environment` field (default: `production`). Combined with template variables, the same pipeline definition can work across environments.

### Connection Parameterization

Different environments use different connection details, but the same SQL templates:

| Variable | Test | Staging | Production |
|----------|------|---------|------------|
| `{{environment}}` | `test` | `staging` | `production` |
| `{{source_host}}` | `test-db.local` | `stg-db.example.com` | `prod-db.example.com` |
| `{{source_database}}` | `test_ecommerce` | `stg_ecommerce` | `ecommerce` |
| `{{target_host}}` | `test-warehouse.local` | `stg-warehouse.example.com` | `warehouse.example.com` |
| `{{target_database}}` | `test_analytics` | `stg_analytics` | `analytics` |

### Same Hook SQL, Different Environments

```sql
INSERT INTO {{target_database}}.audit.load_log
  (env, source, target, rows, ts)
VALUES
  ('{{environment}}', '{{source_host}}:{{source_database}}',
   '{{target_host}}:{{target_database}}', {{rows_extracted}}, NOW())
```

---

## GitOps Branching

Use the GitOps integration for branch-per-environment:

```
PIPELINE_REPO_BRANCH=main        # production configs
PIPELINE_REPO_BRANCH=staging     # staging configs
PIPELINE_REPO_BRANCH=development # dev configs
```

Each branch contains pipeline YAML and connector code for that environment. Promote changes by merging branches.

---

## Configuration Per Environment

Set via environment variables when starting DAPOS:

```bash
# Production
ENVIRONMENT=production \
SOURCE_HOST=prod-db.example.com \
TARGET_HOST=warehouse.example.com \
python main.py

# Staging
ENVIRONMENT=staging \
SOURCE_HOST=stg-db.example.com \
TARGET_HOST=stg-warehouse.example.com \
python main.py
```

---

## YAML Export/Import Across Environments

Export from production, import to staging:

```bash
# Export from prod
python -m cli export > pipelines.yaml

# Edit connection details for staging
# Import to staging instance
curl -X POST http://staging:8100/api/contracts/yaml/import \
  -H "Content-Type: text/yaml" \
  --data-binary @pipelines.yaml
```

The import detects duplicate pipeline names and updates existing pipelines instead of creating duplicates.
