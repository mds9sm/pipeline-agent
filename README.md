# Pipeline Agent

An AI-powered data pipeline platform where the agent is the product.

The agent discovers schemas, proposes ingestion strategies, runs pipelines autonomously, validates every load through a 7-check quality gate, monitors for drift and freshness issues, generates its own connector code for new sources and targets on demand, executes native SQL transforms (replacing dbt), tracks KPI metrics with agentic trend interpretation, and learns from every human approval and rejection. Ships with 8 seed connectors and 4 demo pipelines that run end-to-end on first startup. Additional connectors are generated through conversation.

---

## What the Agent Does

### Autonomous (no human required)

- Extracts data from any registered source connector on a cron schedule
- Stages data locally as CSV batches with metadata columns (`_extracted_at`, `_source_schema`, `_source_table`, `_row_hash`)
- Runs a 7-check quality gate before any data reaches production
- Promotes via merge (delete + insert on merge keys) or append
- Retries transient failures with exponential backoff
- Monitors schema drift every 5 minutes and auto-adapts additive changes (new nullable columns)
- Monitors freshness via `MAX(watermark_column)` per pipeline and fires alerts
- Tracks error budgets per pipeline with automatic escalation when exhausted
- Tracks column-level lineage for impact analysis
- Logs agent cost (tokens, latency) per Claude API call
- Learns from every human approval and rejection -- accumulates `AgentPreference` entries over time
- Executes native SQL transforms with ref(), var(), and 4 materialization strategies
- Suggests, generates, and interprets KPI metrics on pipeline data
- Propagates upstream run context (quality, watermarks, metadata) to downstream pipelines
- Maintains per-metric agent reasoning that evolves with each interaction

### Gated (requires human approval)

- Schema changes: type alterations, dropped columns, non-nullable additions
- Strategy changes: refresh type, merge keys, replication method
- New connectors: all generated code requires review before activation
- Connector updates: regenerated connectors require re-approval

---

## Architecture

Pipeline Agent runs as a single Python async process with four concurrent loops:

1. **API Server** -- FastAPI + uvicorn, serves UI and REST endpoints
2. **Scheduler Loop** -- 60s tick, evaluates cron schedules, respects dependency graph
3. **Monitor Loop** -- 5m tick, schema drift detection, freshness checks, alert dispatch
4. **Observability Loop** -- 30s base tick, quality trend summaries, daily digest at 9 AM UTC

State lives in **PostgreSQL with pgvector** for persistence and semantic search. Reasoning uses the **Claude API** via direct httpx calls. Without an API key, the agent falls back to rule-based logic for all decisions (connector generation is unavailable). Connector code executes in an **AST-validated sandbox** with restricted builtins and an import whitelist.

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker (for PostgreSQL with pgvector)
- An Anthropic API key (optional -- rule-based fallback without one, but connector generation requires it)

### Setup

```bash
# 1. Start PostgreSQL, demo MySQL, demo MongoDB, mock SaaS APIs
docker compose up -d

# 2. Configure environment
cp .env.example .env
# Edit .env -- set ANTHROPIC_API_KEY for AI features (optional for demo)

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start the agent
python main.py
```

On first start:
1. An asyncpg connection pool is created against PostgreSQL
2. All database tables are created (with `IF NOT EXISTS`)
3. A default admin user is created (admin/admin)
4. 8 seed connectors are loaded (MySQL, SQLite, MongoDB, Stripe, Google Ads, Facebook Insights sources + PostgreSQL, Redshift targets)
5. 4 demo pipelines are auto-created (MySQL, MongoDB, Stripe sources → PostgreSQL)
6. All 4 demo pipelines are triggered immediately (first run uses quality gate leniency to establish baselines)
7. The API server, scheduler, monitor, and observability loops start concurrently

Open **http://localhost:8100** and log in with **admin / admin**.

---

## Security

### JWT Authentication

Authentication is **enabled by default** (`AUTH_ENABLED=true`). A default admin user (admin/admin) is auto-created on first startup. Three roles with RBAC enforcement:

| Role | Capabilities |
|------|-------------|
| **admin** | Full access — register users, generate/deprecate connectors, manage pipelines, approve proposals |
| **operator** | Run/manage pipelines, approve proposals, test connectors, view all data |
| **viewer** | Read-only access to all views and chat |

Tokens are issued via `POST /api/auth/login` and carry `sub`, `role`, `iat`, `exp` claims. Set `AUTH_ENABLED=false` to disable auth for development (all requests treated as admin).

Authentication methods:
- **Bearer token** in the `Authorization` header
- **API key** in the `X-API-Key` header (matches `JWT_SECRET`)

### Credential Encryption

Set `ENCRYPTION_KEY` to a Fernet key. All credential fields (`password`, `source_password`, `api_key`, `secret`, `token`, `ssl_ca`, `ssl_key`, `ssl_cert`) are encrypted at rest in PostgreSQL. Generate a key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Connector Sandboxing

All generated connector code goes through a three-layer security gate before execution:

1. **AST validation** -- The code is parsed and every import and function call is checked against allowlists and blocklists. Blocked modules include `subprocess`, `shutil`, `os` (direct), `socket`, `ctypes`, `multiprocessing`, `threading`. Blocked calls include `eval`, `exec`, `compile`, `__import__`, `globals`, `locals`.

2. **Restricted builtins** -- `safe_exec()` replaces the standard builtins with a filtered set. Dangerous builtins are removed. `__import__` is replaced with a guarded version that enforces the import allowlist at runtime.

3. **Import whitelist** -- Only approved modules can be imported: database drivers (`pymysql`, `psycopg2`, `cx_Oracle`, `asyncpg`, etc.), HTTP clients (`httpx`, `requests`), data handling (`csv`, `json`, `io`), standard library utilities (`datetime`, `hashlib`, `re`, etc.), and cloud SDKs (`boto3`, `google.cloud.bigquery`, `azure.storage.blob`).

Additionally, every generated connector requires human approval before it becomes active.

### Rate Limiting

API endpoints are rate-limited via slowapi. Default rate applies to all endpoints. Expensive operations (connector generation, connection testing) have tighter limits.

---

## Connector System

### Interfaces

All source connectors implement `source/base.py::SourceEngine`. All target connectors implement `target/base.py::TargetEngine`. These two abstract base classes are the only stable contracts in the system. Nothing outside of these files changes when a new connector is added.

The `TargetEngine` interface includes quality gate query methods (`get_row_count`, `get_max_value`, `check_duplicates`, `get_null_rates`, `get_cardinality`) so any target connector can run the full 7-check gate without special casing.

### Seed Connectors

Two implementations ship as string constants in `connectors/seeds.py`:

- **MySQL source** (`MySQLEngine`) -- PyMySQL with unbuffered streaming
- **Redshift target** (`RedshiftEngine`) -- psycopg2 with bulk COPY

On first startup, `bootstrap_seeds()` writes these to the connectors table as active records. From that point they are loaded via `exec()` identical to any agent-generated connector. The seeds serve as both working connectors and reference implementations for Claude when generating new ones.

### Agent-Generated Connectors

When a user needs a connector that does not exist:

1. The agent builds a prompt with the abstract interface + a seed connector as reference
2. Claude generates a complete Python class implementing the interface
3. The code is validated via AST analysis and the sandbox (`safe_exec`)
4. `test_connection()` runs against the user's live credentials
5. On failure, the error is appended to the prompt and retried (up to 3 attempts)
6. On success, a `ConnectorRecord(status="draft")` is saved and an approval proposal is created
7. The user reviews the generated code in the Approvals view and approves
8. The connector status becomes `active` and is hot-reloaded into the registry -- no restart needed

### Connector Versioning

When a connector is regenerated or updated, a `ConnectorMigration` record is created tracking:
- The previous and new version numbers
- All affected pipelines
- Migration status: `pending` -> `in_progress` -> `completed` / `failed` / `rolled_back`

---

## Quality Gate

7 checks run against the staging table before any data is promoted to the target:

| # | Check | What it does |
|---|---|---|
| 1 | **Count reconciliation** | Extracted rows vs staged rows -- within configurable tolerance |
| 2 | **Schema consistency** | Staging schema matches contract column mappings + metadata columns |
| 3 | **PK uniqueness / cardinality** | No merge key duplicates; distinct counts within baseline deviation |
| 4 | **Null rate analysis** | Null rates per column vs historical baseline (z-score + catastrophic jump detection) |
| 5 | **Volume z-score** | Row count vs 30-run rolling average |
| 6 | **Sample row verification** | Staging count consistent with extraction count |
| 7 | **Freshness** | `MAX(watermark_column)` vs schedule interval |

After the gate, Claude produces a natural language explanation of what happened and why, stored in `GateRecord.agent_reasoning`. The decision is one of: **PROMOTE**, **PROMOTE_WITH_WARNING**, or **HALT**.

---

## Error Budgets

Each pipeline has an error budget calculated over a rolling window (default: 7 days).

- **Success rate** = successful runs / total runs in the window
- **Budget threshold** = 90% (configurable)
- **Budget remaining** = success_rate - budget_threshold (normalized)

When a pipeline's error budget is exhausted (success rate drops below the threshold):
1. A **CRITICAL** alert is fired and dispatched to all configured channels
2. The `escalated` flag is set on the budget record
3. The scheduler **skips** the pipeline until the budget is manually reset or recovers

Query error budgets via `GET /api/error-budgets/{pipeline_id}`. The response includes total runs, successful/failed counts, success rate, budget remaining, and escalation status.

---

## Column-Level Lineage

Pipeline Agent tracks source-to-target column mappings as `ColumnLineage` records:

- **Automatic tracking** -- Lineage records are created when pipelines are created and updated when schemas change
- **Impact analysis** -- When schema drift is detected on a source column, the monitor queries downstream lineage to assess the blast radius: which target columns and downstream pipelines are affected
- **Query lineage** via `GET /api/lineage/{pipeline_id}` -- returns upstream dependencies, downstream dependents, and column-level mappings

Each lineage record tracks: source pipeline, source schema/table/column, target pipeline, target schema/table/column, and transformation type (default: `direct`).

---

## Agent Cost Tracking

Every Claude API call is logged with:

| Field | Description |
|---|---|
| `operation` | What the call was for (e.g. `propose_strategy`, `generate_connector`, `reason_about_quality`) |
| `model` | Claude model used |
| `input_tokens` | Tokens sent |
| `output_tokens` | Tokens received |
| `total_tokens` | Sum |
| `latency_ms` | Round-trip time |
| `pipeline_id` | Associated pipeline (if applicable) |

Query costs via:
- `GET /api/agent-costs` -- paginated list of cost log entries, filterable by pipeline and date range
- `GET /api/agent-costs/summary` -- aggregated totals by operation type

---

## Observability

### Freshness Monitoring

The monitor loop computes staleness per pipeline using `TargetEngine.get_max_value(freshness_column)`. For full-refresh tables, staleness is time since the last successful run. Freshness status (fresh / warning / critical) is determined by tier-specific SLA thresholds.

### Schema Drift Detection

Every 5 minutes, the monitor calls `SourceEngine.profile_table()` for each active pipeline and compares to the contract's column mappings. Additive nullable columns can be auto-adapted. Non-additive changes (type alterations, dropped columns) create proposals with reasoning and rollback plans.

### Alert Channels

Alerts are dispatched to configured channels based on severity and tier:

| Channel | Configuration |
|---|---|
| **Slack** | Set `SLACK_WEBHOOK_URL` -- incoming webhook |
| **Email** | Set `EMAIL_SMTP_HOST`, `EMAIL_SMTP_PORT`, `EMAIL_FROM` |
| **PagerDuty** | Set `PAGERDUTY_ROUTING_KEY` -- Events API v2, automatic dedup keys, severity mapping (CRITICAL -> critical, WARNING -> warning) |

### Daily Digest

At 9 AM UTC, the observability loop generates a summary of all undigested alerts via Claude (or a simple list without an API key), logs it, and marks the alerts as digested.

---

## Learning Loop

```
Rejected proposal
  -> learn_from_rejection(proposal, user_note)
  -> Claude extracts: {preference_key, preference_value, scope, confidence}
  -> If confidence >= 0.7: saved as AgentPreference

Future proposals for same pipeline/schema/source_type
  -> propose_strategy(profile, preferences=[...AgentPreferences])
  -> Claude applies learned preferences before reasoning
```

When pgvector embeddings are enabled (set `VOYAGE_API_KEY`), preferences are embedded via Voyage and stored with a `vector(1024)` column. Future preference lookups use semantic similarity search to find the most relevant past decisions, even across different pipelines.

---

## Environment Variables

### PostgreSQL

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `pipeline_agent` | Database name |
| `PG_USER` | `pipeline_agent` | Database user |
| `PG_PASSWORD` | `pipeline_agent` | Database password |
| `PG_POOL_MIN` | `2` | Minimum connection pool size |
| `PG_POOL_MAX` | `10` | Maximum connection pool size |

### Agent (Claude API)

| Variable | Default           | Description |
|---|-------------------|---|
| `ANTHROPIC_API_KEY` | *(empty)*         | Claude API key. Without this, rule-based fallback only -- connector generation unavailable. |
| `AGENT_MODEL` | `claude-opus-4-6` | Claude model for all reasoning and generation. |

### Embeddings (optional)

| Variable | Default | Description |
|---|---|---|
| `VOYAGE_API_KEY` | *(empty)* | Enables semantic preference search via pgvector. |
| `EMBEDDING_MODEL` | `voyage-3` | Voyage embedding model. |

### Staging

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `./data` | Root directory for staging CSV files and logs. |
| `MAX_DISK_PCT` | `85` | Pause pipelines if disk usage exceeds this percentage. |
| `BATCH_SIZE` | `50000` | Rows per CSV staging batch. |

### Scheduler

| Variable | Default | Description |
|---|---|---|
| `MAX_CONCURRENT_PIPELINES` | `4` | Max pipelines running simultaneously. |

### Alerts

| Variable | Default | Description |
|---|---|---|
| `SLACK_WEBHOOK_URL` | *(empty)* | Incoming webhook URL for Slack notifications. |
| `EMAIL_SMTP_HOST` | *(empty)* | SMTP server hostname. |
| `EMAIL_SMTP_PORT` | `587` | SMTP port. |
| `EMAIL_FROM` | *(empty)* | Sender address for email alerts. |
| `PAGERDUTY_ROUTING_KEY` | *(empty)* | PagerDuty Events API v2 routing key for critical alerts. |

### Server

| Variable | Default | Description |
|---|---|---|
| `API_HOST` | `0.0.0.0` | Bind address for the FastAPI server. |
| `API_PORT` | `8100` | Port. |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

### Authentication

| Variable | Default | Description |
|---|---|---|
| `AUTH_ENABLED` | `false` | Set to `true` to require JWT authentication on all API endpoints. |
| `JWT_SECRET` | *(empty)* | Secret key for signing JWT tokens. Change this in production. |
| `JWT_ALGORITHM` | `HS256` | JWT signing algorithm. |
| `JWT_EXPIRY_HOURS` | `24` | Token expiry in hours. |

### Encryption

| Variable | Default | Description |
|---|---|---|
| `ENCRYPTION_KEY` | *(empty)* | Fernet key for encrypting credentials at rest. Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. |

---

## API Endpoints

### Authentication

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/auth/login` | Authenticate and receive a JWT token |
| `POST` | `/api/auth/register` | Register a new user (admin only) |
| `GET` | `/api/auth/me` | Get current user info from token |

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check (`{"status": "ok"}`) |
| `GET` | `/metrics` | Runtime metrics (pipeline counts, run stats) |

### Command

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/command` | Agent-routed command interface -- send natural language, agent routes to the right action |

### Connection & Discovery

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/connection/test-source` | Test source connection with credentials |
| `POST` | `/api/connection/test-target` | Test target connection with credentials |
| `GET` | `/api/discovery/schemas` | List schemas via SourceEngine |
| `POST` | `/api/discovery/profile` | Profile a table, return TableProfile |
| `POST` | `/api/discovery/propose` | Run propose_strategy(), return reasoning + cost estimate |

### Connectors

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/connectors` | List connectors (filter: type, status) |
| `GET` | `/api/connectors/{id}` | Connector detail + code + test results |
| `POST` | `/api/connectors/generate` | Generate a new connector via Claude |
| `POST` | `/api/connectors/{id}/test` | Re-run test_connection() against live credentials |
| `DELETE` | `/api/connectors/{id}` | Deprecate connector (fails if active pipelines reference it) |

### Pipelines

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/pipelines` | List pipelines (filter: status, tier) |
| `GET` | `/api/pipelines/{id}` | Full pipeline contract |
| `POST` | `/api/pipelines` | Create a pipeline |
| `POST` | `/api/pipelines/batch` | Batch create pipelines |
| `PATCH` | `/api/pipelines/{id}` | Update pipeline configuration |
| `POST` | `/api/pipelines/{id}/trigger` | Trigger a manual run |
| `POST` | `/api/pipelines/{id}/backfill` | Backfill with `{start, end}` date range |
| `POST` | `/api/pipelines/{id}/pause` | Pause pipeline |
| `POST` | `/api/pipelines/{id}/resume` | Resume pipeline |
| `GET` | `/api/pipelines/{id}/preview` | Dry run: sample rows + DDL + strategy |
| `GET` | `/api/pipelines/{id}/runs` | Run history |
| `GET` | `/api/pipelines/{id}/schema-history` | Full SchemaVersion history |

### Approvals

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/approvals` | List proposals (default: pending) |
| `POST` | `/api/approvals/{id}` | Resolve: `{"action": "approve"\|"reject", "user": "...", "note": "..."}` |

### Quality & Observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/quality/{id}` | Gate history for a pipeline |
| `GET` | `/api/observability/freshness` | Current freshness report across all pipelines |
| `GET` | `/api/observability/alerts` | Alert feed (filter: severity, pipeline, acknowledged) |
| `POST` | `/api/observability/alerts/{id}/acknowledge` | Acknowledge an alert |

### Lineage

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/lineage/{id}` | Dependency graph + column-level lineage for a pipeline |
| `POST` | `/api/lineage` | Declare a pipeline dependency |
| `DELETE` | `/api/lineage/{id}` | Remove a dependency |

### Error Budgets

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/error-budgets/{id}` | Error budget status for a pipeline |

### Agent Costs

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/agent-costs` | Paginated cost log (filter: pipeline_id, date range) |
| `GET` | `/api/agent-costs/summary` | Aggregated cost summary by operation |

### Context API (Build 28)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/runs/{run_id}/context` | Full aggregated run context |
| `GET` | `/api/pipelines/{id}/context-chain` | Upstream dependency DAG context |

### SQL Transforms (Build 29)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/transforms` | Create transform |
| `GET` | `/api/transforms` | List transforms |
| `GET` | `/api/transforms/{id}` | Transform detail |
| `PATCH` | `/api/transforms/{id}` | Update transform |
| `DELETE` | `/api/transforms/{id}` | Delete transform |
| `POST` | `/api/transforms/generate` | AI-generate transform SQL |

### Metrics / KPIs (Build 31)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/metrics/suggest/{pipeline_id}` | Agent suggests metrics |
| `POST` | `/api/metrics` | Create metric |
| `GET` | `/api/metrics` | List metrics |
| `GET` | `/api/metrics/{metric_id}` | Metric detail with snapshots |
| `POST` | `/api/metrics/{metric_id}/compute` | Compute metric now |
| `GET` | `/api/metrics/{metric_id}/trend` | Agent trend interpretation |
| `PATCH` | `/api/metrics/{metric_id}` | Update metric |
| `DELETE` | `/api/metrics/{metric_id}` | Delete metric |

### Business Knowledge (Build 32)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/agent/system-prompt` | Read-only agent system prompt |
| `GET` | `/api/settings/business-knowledge` | Get business knowledge |
| `PUT` | `/api/settings/business-knowledge` | Update business knowledge |
| `POST` | `/api/settings/business-knowledge/parse-kpis` | Parse free-text KPIs |

### Connector Migrations

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/connector-migrations` | List migrations (filter: connector_id, status) |

### Notification Policies

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/policies` | List notification policies |
| `POST` | `/api/policies` | Create a policy |
| `GET` | `/api/policies/{id}` | Policy detail |
| `PATCH` | `/api/policies/{id}` | Update a policy |
| `DELETE` | `/api/policies/{id}` | Delete a policy |

### Preferences

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/preferences` | List agent preferences (filter: scope, pipeline_id) |
| `POST` | `/api/preferences` | Create a preference manually |
| `DELETE` | `/api/preferences/{id}` | Delete a preference |

---

## Project Structure

```
pipeline-agent/
├── main.py                   # Entry point -- wires 4 async loops: API, Scheduler, Monitor, Observability
├── config.py                 # Environment config with defaults
├── auth.py                   # JWT authentication (Bearer tokens, API key, role-based access)
├── crypto.py                 # Fernet encryption for credentials at rest
├── sandbox.py                # AST validation + restricted builtins + import whitelist for connectors
├── docker-compose.yml        # PostgreSQL 16 with pgvector
├── requirements.txt
├── .env.example
│
├── agent/
│   ├── core.py               # Claude API: propose_strategy, analyze_drift, generate_connector,
│   │                         #   reason_about_quality, learn_from_rejection, generate_digest
│   │                         #   metrics, topology, transforms, business knowledge
│   ├── conversation.py       # 10-step onboarding flow (stateless)
│   └── autonomous.py         # Pipeline execution state machine: PENDING -> COMPLETE/HALTED
│
├── connectors/
│   ├── registry.py           # exec()-based loader, validator, hot-reloader for all connectors
│   └── seeds.py              # MYSQL_SOURCE_CODE + REDSHIFT_TARGET_CODE as string constants
│
├── source/
│   └── base.py               # SourceEngine abstract interface (INTERFACE_VERSION = "1.0")
│
├── target/
│   └── base.py               # TargetEngine abstract interface (INTERFACE_VERSION = "1.0")
│
├── quality/
│   └── gate.py               # 7-check quality gate typed against TargetEngine
│
├── contracts/
│   ├── models.py             # All dataclasses + enums + tier defaults (PipelineContract,
│   │                         #   ConnectorRecord, RunRecord, ErrorBudget, ColumnLineage,
│   │                         #   AgentCostLog, ConnectorMigration, User, ...)
│   └── store.py              # PostgreSQL + asyncpg CRUD for all entities
│
├── staging/
│   └── local.py              # CSV staging manager
│
├── monitor/
│   └── engine.py             # Drift detection, freshness monitoring, lineage impact analysis,
│   │                         #   alert dispatch (Slack, Email, PagerDuty)
│
├── scheduler/
│   └── manager.py            # Cron scheduler + topological dependency sort + backfill + retry
│
├── transforms/
│   └── engine.py            # SQL transform engine — ref/var resolution, materialization
│
├── mcp_server.py            # MCP server — 12 resources, 24 tools, 3 prompts
│
├── docs/                    # Structured documentation
│   ├── index.md
│   ├── api-reference.md
│   └── ...
│
├── cli/
│   └── __main__.py          # CLI — 14 commands, pipeline management
│
├── api/
│   └── server.py             # FastAPI: all REST endpoints with JWT auth + rate limiting
│
├── ui/
│   ├── index.html
│   └── App.jsx               # React SPA: 13 views, dark sidebar, SVG icons (CDN React 18 + Tailwind)
│
├── test-pipeline-agent.sh    # Comprehensive curl-based test suite (~187 tests)
├── CLAUDE.md                 # Product context for Claude Code sessions
├── CHANGELOG.md              # Change log across builds
│
└── alembic/                  # Database migrations (production)
    ├── env.py
    └── versions/
        └── 001_initial.py
```

No subdirectories under `source/` or `target/`. All connector implementations live in the PostgreSQL `connectors` table as code strings.

---

## Database

### PostgreSQL with pgvector

Pipeline Agent uses PostgreSQL 16 with the pgvector extension. The `docker-compose.yml` provides a ready-to-use instance using the `pgvector/pgvector:pg16` image.

Connection pooling is managed via asyncpg with configurable min/max pool sizes (`PG_POOL_MIN`, `PG_POOL_MAX`).

### Tables

| Table | Purpose |
|---|---|
| `connectors` | Connector code, metadata, test results, generation log |
| `pipelines` | Pipeline contracts with strategy, schedule, schema, quality config |
| `runs` | Execution records with status, row counts, watermarks, gate decisions |
| `gates` | Quality gate results with per-check detail and agent reasoning |
| `proposals` | Change proposals with reasoning, confidence, impact analysis |
| `schema_versions` | Schema change history per pipeline |
| `dependencies` | Pipeline dependency graph |
| `notification_policies` | Alert routing configuration |
| `freshness_snapshots` | Point-in-time freshness measurements |
| `alerts` | Alert records with severity, acknowledgment status |
| `decision_logs` | Append-only log of every agent decision |
| `preferences` | Learned agent preferences with optional pgvector embeddings |
| `error_budgets` | Rolling window success rates per pipeline |
| `column_lineage` | Source-to-target column mappings |
| `agent_cost_logs` | Token usage and latency per Claude API call |
| `connector_migrations` | Version migration records with affected pipelines |
| `users` | User accounts with bcrypt password hashes and roles |
| `data_contracts` | Producer/consumer relationships, SLA, cleanup guards |
| `sources` | Source registry for analyst discovery |
| `transforms` | SQL transform definitions with ref/var resolution |
| `metrics` | KPI definitions with SQL expressions and agent reasoning |
| `metric_snapshots` | Time-series metric computation results |
| `business_knowledge` | Company context, glossary, KPI definitions (singleton) |
| `interactions` | Chat interaction audit log |
| `changelog` | Pipeline change history with reasons |

### Migrations

For development and testing, `Store.create_tables()` creates all tables with `IF NOT EXISTS`. For production, Alembic migrations are available in `alembic/versions/`.

---

## Testing

The test strategy is to test the real running app via curl API calls -- no mocks, no pytest, no isolated unit tests. The agent is the product; test it the way users interact with it.

```bash
# Prerequisites: app must be running
docker compose up -d
ANTHROPIC_API_KEY=sk-... python main.py

# Full test suite (~187 tests, ~20 min with LLM calls)
./test-pipeline-agent.sh

# Targeted test modes
./test-pipeline-agent.sh --api        # REST API endpoints only (fast, no LLM)
./test-pipeline-agent.sh --sources    # All source connector requests + generation
./test-pipeline-agent.sh --targets    # All target connector requests + generation
./test-pipeline-agent.sh --chat       # Multi-turn conversations + agent understanding
```

### Test Coverage

- **12 Core API endpoints** -- health, metrics, connectors, pipelines, approvals, freshness, alerts, costs, policies, preferences, web UI
- **16 Database sources** -- Oracle, SQL Server, MySQL, PostgreSQL, MongoDB, MariaDB, Cassandra, DynamoDB, CockroachDB, Redis, Elasticsearch, Neo4j, ClickHouse, SQLite, Teradata, DB2
- **30 SaaS/API sources** -- Stripe, Google Ads, Facebook Insights/Ads, Salesforce, HubSpot, Shopify, Google Analytics, Jira, Zendesk, Intercom, Twilio, SendGrid, Mailchimp, QuickBooks, Xero, Notion, Airtable, Slack, GitHub, LinkedIn Ads, Twitter Ads, TikTok Ads, Pinterest Ads, Marketo, Braze, Segment, Mixpanel, Amplitude, Snowplow
- **5 File/Cloud sources** -- S3, GCS, Azure Blob, SFTP, FTP
- **5 Streaming sources** -- Kafka, Kinesis, Pub/Sub, RabbitMQ, EventHub
- **18 Targets** -- PostgreSQL, Snowflake, BigQuery, Redshift, Databricks, ClickHouse, MySQL, SQL Server, Oracle, S3, GCS, Azure Synapse, Firebolt, DuckDB, Delta Lake, Apache Iceberg, Elasticsearch, MongoDB
- **20 Multi-turn pipeline conversations** -- e.g., Oracle->Snowflake, Stripe->Snowflake, Salesforce->Databricks
- **10 Agent understanding tests** -- capabilities, scheduling, refresh strategy, quality gates, error budgets, schema drift
- **9 Connector generation via API** -- Oracle, SQL Server, Stripe, Google Ads, Facebook, Snowflake, BigQuery, Redshift, Databricks
- **18 Pipeline CRUD** -- create, get, update (expanded PATCH), pause, resume, preview, runs, quality, lineage, error budgets
- **Approval workflow** -- list pending proposals, approve connectors
- **6 YAML contract-as-code** -- single export, bulk export, status filter, import, GitOps sync
- **11 Data contracts** -- create, list, get, validate, update, violations, auto-dep, delete
- **5 Step DAG** -- steps definition, validate, cycle detection, preview, PATCH
- **8 Agent diagnostics** -- diagnose, impact, anomalies, chat routing
- **15 Data catalog** -- search, trust, tags, context, weights, alert narratives
- **3 MCP server** -- import, resources, tools
- **13 SQL transforms** -- CRUD, lineage, generate, chat routing, demo transforms
- **8 Metrics / KPIs** -- suggest, create, list, get, update, trend, delete, chat
- **5 Context API** -- context chain, run context, detail field, PATCH toggle, 404
- **9 Business context** -- system prompt, business knowledge CRUD, parse-kpis, metric reasoning

---

## Key Design Decisions

**No static connector imports.** `source/mysql/` and `target/redshift/` do not exist as importable Python modules. All connector code lives in the database and is loaded via `exec()`. This makes seeds and generated connectors architecturally identical.

**PostgreSQL + pgvector for robustness.** SQLite was replaced with PostgreSQL to support connection pooling, concurrent writes from multiple async loops, JSONB for structured data, and the pgvector extension for semantic preference search.

**pgvector for semantic preference search.** Agent preferences can be embedded via Voyage and stored as `vector(1024)` columns. This enables similarity search across preferences when proposing strategies, even for pipelines the agent has not seen before.

**AST-validated sandbox for security.** Generated connector code is statically analyzed before execution and runs with restricted builtins. The import whitelist prevents filesystem access, subprocess execution, network operations outside of approved database drivers, and other dangerous capabilities.

**Error budgets for reliability.** Rolling-window success rates prevent flaky pipelines from consuming resources endlessly. When a pipeline's error budget is exhausted, the scheduler skips it and a CRITICAL alert is fired.

**Two-tier autonomy is a hard constraint, not a soft guideline.** Runtime decisions (extract/load/promote) are always autonomous. Structural changes (connectors, schema, strategy) always require a human signature. This line does not blur.

**Quality gate is connector-agnostic.** `quality/gate.py` types against `TargetEngine` (the abstract interface), not any specific database. Every target connector must implement the quality query methods.

**`target_options` is the escape hatch for target-specific behavior.** Snowflake `cluster_by`, Redshift `dist_key`/`sort_key`, BigQuery partition specs -- all go in `target_options` without touching the contract model.

**No LangChain, no external vector DB, no memory cache.** All state is in PostgreSQL. Reasoning is direct Claude API calls via httpx. The agent's "memory" is `AgentPreference` rows and `DecisionLog` entries.
