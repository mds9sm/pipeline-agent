# Multi-Environment Setup

DAPOS supports running pipelines across development, staging, and production environments using a **GitOps-driven promotion model** — separate instances per environment, with git as the promotion mechanism.

---

## Promotion Model: GitOps-Driven

The recommended approach uses **three DAPOS instances** (dev, staging, prod), each syncing pipeline configs from a **shared git repository** with branch-per-environment.

```
┌──────────────────┐     git push      ┌──────────────────┐
│   Dev Instance    │ ──────────────▶  │  GitOps Repo      │
│   (create/edit)   │                   │                   │
└──────────────────┘                   │  development ─┐   │
                                        │  staging ─────┤   │
                                        │  main (prod) ─┘   │
                                        └──────────────────┘
                                           │    PR review    │
┌──────────────────┐     git sync       ┌──┴───────────────┐
│  Stage Instance   │ ◀──────────────── │  staging branch   │
│   (validate)      │                   └──────────────────┘
└──────────────────┘                       │    PR merge
                                        ┌──┴───────────────┐
┌──────────────────┐     git sync       │  main branch      │
│  Prod Instance    │ ◀──────────────── └──────────────────┘
│   (execute)       │
└──────────────────┘
```

### How Promotion Works

1. **Develop** — Create and edit pipelines, transforms, and dependencies on the dev instance. Changes auto-commit to the `development` branch via GitOps.

2. **Promote to staging** — Open a PR from `development` → `staging`. The PR diff shows:
   - Pipeline YAML changes (schedule, strategy, quality thresholds)
   - Transform SQL changes (with approval status visible in YAML)
   - Dependency changes
   - Connector code changes

3. **Validate on staging** — Staging instance syncs from `staging` branch. Run pipelines against staging databases to validate.

4. **Promote to production** — Merge `staging` → `main`. Production instance syncs from `main`.

5. **Rollback** — `git revert` on the relevant branch. Instance syncs the reverted state.

### What Feeds Into the PR

| Change type | How it reaches git | Approval gate |
|------------|-------------------|---------------|
| Transform SQL edits | Auto-committed on save | Yes — `approved: false` until explicitly approved |
| Pipeline settings | Auto-committed on PATCH | No — changelog audit trail |
| Dependencies added/removed | Auto-committed on POST/DELETE | No — confirm step in UI, changelog audit |
| Connector code | Auto-committed on generation/approval | Yes — connector approval workflow |

---

## Environment Strategy

Each DAPOS instance is configured for its environment via environment variables. The same pipeline YAML works across environments because connection details come from the instance config, not the pipeline definition.

### Connection Parameterization

| Variable | Dev | Staging | Production |
|----------|-----|---------|------------|
| `{{environment}}` | `development` | `staging` | `production` |
| `{{source_host}}` | `dev-db.local` | `stg-db.example.com` | `prod-db.example.com` |
| `{{source_database}}` | `dev_ecommerce` | `stg_ecommerce` | `ecommerce` |
| `{{target_host}}` | `dev-warehouse.local` | `stg-warehouse.example.com` | `warehouse.example.com` |
| `{{target_database}}` | `dev_analytics` | `stg_analytics` | `analytics` |

### Same SQL, Different Environments

Hook SQL and transform SQL use template variables that resolve per-instance:

```sql
INSERT INTO {{target_database}}.audit.load_log
  (env, source, target, rows, ts)
VALUES
  ('{{environment}}', '{{source_host}}:{{source_database}}',
   '{{target_host}}:{{target_database}}', {{rows_extracted}}, NOW())
```

---

## GitOps Branching

Each instance points at a different branch of the same repo:

```bash
# Dev instance
PIPELINE_REPO_BRANCH=development

# Staging instance
PIPELINE_REPO_BRANCH=staging

# Production instance
PIPELINE_REPO_BRANCH=main
```

See [GitOps](/docs/gitops) for full repo configuration.

---

## Configuration Per Environment

```bash
# Development
ENVIRONMENT=development \
PIPELINE_REPO_BRANCH=development \
SOURCE_HOST=dev-db.local \
TARGET_HOST=dev-warehouse.local \
python main.py

# Staging
ENVIRONMENT=staging \
PIPELINE_REPO_BRANCH=staging \
SOURCE_HOST=stg-db.example.com \
TARGET_HOST=stg-warehouse.example.com \
python main.py

# Production
ENVIRONMENT=production \
PIPELINE_REPO_BRANCH=main \
SOURCE_HOST=prod-db.example.com \
TARGET_HOST=warehouse.example.com \
python main.py
```

---

## YAML Export/Import (Alternative)

For teams not using GitOps, manual promotion via YAML export/import:

```bash
# Export from dev
python -m cli export > pipelines.yaml

# Import to staging instance
curl -X POST http://staging:8100/api/contracts/yaml/import \
  -H "Content-Type: text/yaml" \
  --data-binary @pipelines.yaml
```

The import detects duplicate pipeline names and updates existing pipelines instead of creating duplicates.

---

## Best Practices

- **Never edit directly on production** — All changes flow through dev → staging → prod via git.
- **Use transform approval on dev** — Approve transforms before promoting. The `approved` field is part of the YAML, visible in PR diffs.
- **Tag releases** — Use git tags on the `main` branch for production releases. Enables point-in-time rollback.
- **Separate databases per environment** — Each instance should connect to its own source and target databases. Template variables make this seamless.
