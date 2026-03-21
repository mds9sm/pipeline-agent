# Configuration Reference

All configuration is via environment variables. Defaults are designed for local development.

## Core

| Variable | Default | Description |
|----------|---------|-------------|
| `API_HOST` | `0.0.0.0` | API server bind address |
| `API_PORT` | `8100` | API server port |
| `DATA_DIR` | `./data` | Base directory for staging files, logs, contracts |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_FORMAT` | `text` | Log format (`text` or `json`) |
| `LOG_MAX_BYTES` | `10485760` | Max log file size before rotation (10MB) |
| `LOG_BACKUP_COUNT` | `5` | Number of rotated log files to keep |

## PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `pipeline_agent` | Database name |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | `postgres` | Database password |
| `PG_POOL_MIN` | `2` | Minimum connection pool size |
| `PG_POOL_MAX` | `10` | Maximum connection pool size |

## AI Agent

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(none)* | Anthropic API key for Claude. Optional — without it, agent uses rule-based fallbacks |
| `MODEL` | `claude-sonnet-4-20250514` | Claude model to use |
| `VOYAGE_API_KEY` | *(none)* | Voyage AI key for embeddings (optional) |
| `EMBEDDING_MODEL` | `voyage-code-2` | Embedding model |

## Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_ENABLED` | `true` | Enable JWT authentication |
| `JWT_SECRET` | *(dev fallback)* | JWT signing secret — **set in production** |
| `JWT_EXPIRY_HOURS` | `24` | Token expiry time |

## Pipeline Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `10000` | Default extraction batch size |
| `MAX_DISK_PCT` | `0.9` | Maximum disk usage before blocking runs |

## Encryption

| Variable | Default | Description |
|----------|---------|-------------|
| `ENCRYPTION_KEY` | *(none)* | Fernet key for credential encryption at rest. Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |

## GitOps

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_REPO_PATH` | *(none)* | Path to git repo for pipeline YAML versioning. Empty = disabled |
| `PIPELINE_REPO_BRANCH` | `main` | Git branch (enables branch-per-environment: `dev`, `staging`, `prod`) |
| `PIPELINE_REPO_REMOTE` | *(none)* | Remote URL for shared repo (e.g., `git@github.com:org/dags-repo.git`) |
| `GITOPS_AUTO_PUSH` | `true` | Push to remote after every commit |
| `GITOPS_AUTO_PULL` | `true` | Pull from remote before every commit |

## Scheduler

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_TICK_SECONDS` | `60` | Scheduler evaluation interval |
| `MONITOR_TICK_SECONDS` | `300` | Monitor check interval (schema drift, freshness) |

## Multi-Environment Example

```bash
# Production
export PG_HOST=prod-db.internal
export PG_DATABASE=dapos_prod
export PIPELINE_REPO_BRANCH=prod
export PIPELINE_REPO_REMOTE=git@github.com:myorg/dapos-dags.git
export ENCRYPTION_KEY=your-fernet-key
export JWT_SECRET=your-production-secret
export ANTHROPIC_API_KEY=sk-ant-...

# Staging
export PG_HOST=stg-db.internal
export PG_DATABASE=dapos_stg
export PIPELINE_REPO_BRANCH=staging
export PIPELINE_REPO_REMOTE=git@github.com:myorg/dapos-dags.git
```

Pipelines defined with `{{target_database}}` and `{{environment}}` template variables automatically resolve to the correct values per environment.
