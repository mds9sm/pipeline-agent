# Pipeline Agent -- Product & Engineering Spec (v5)

---

## 1. What This Is

Pipeline Agent is an AI-powered data pipeline platform where the agent is the product, not a wrapper around it.

The agent discovers schemas, proposes ingestion strategies with reasoning and cost estimates, executes pipelines autonomously, validates every load through a 7-check quality gate, monitors for schema drift and freshness degradation, generates its own connector code for new sources and targets on demand, and learns from every human decision it observes.

There are no pre-built integrations. When a user needs a new source or target, they ask the agent. The agent generates the connector code against the abstract interface, tests it, and registers it. The connector ships as an approval proposal, not as a code deployment.

The system runs as a single Python async process. State lives in **PostgreSQL with pgvector** for persistence, concurrent access, and semantic search. Reasoning uses the Claude API. Everything degrades gracefully to rule-based logic if no API key is configured. The platform enforces security through **JWT authentication**, **Fernet encryption for credentials at rest**, **AST-validated sandboxed execution** for generated connector code, and **rate limiting** on all API endpoints.

---

## 2. Agent Behavior -- What the Agent Does and When

This section is the center of the spec. Implementation details follow. Understand the agent behaviors first.

### 2.1 Autonomous Runtime Behaviors (no human required)

These happen every run without approval:

| Behavior | Trigger | What happens |
|---|---|---|
| **Extract** | Scheduler tick or manual trigger | SourceEngine streams rows to CSV batches with metadata columns |
| **Stage** | After extract | TargetEngine creates staging table, loads CSV via bulk copy |
| **Quality gate** | After staging | 7 checks run in order. Short-circuit on FAIL if configured |
| **Promote** | Gate PASS or WARN | Merge or append to target table, drop staging, update watermark |
| **Retry** | Run failure | Exponential backoff up to `retry_max_attempts` |
| **Freshness check** | Monitor loop (5m) | Query MAX(freshness_column) per pipeline, emit FreshnessSnapshot |
| **Alert dispatch** | Any gate FAIL or freshness breach | Route to Slack/email/PagerDuty per NotificationPolicy |
| **Auto-adapt additive drift** | New nullable columns detected | Add columns to contract, write SchemaVersion, no proposal needed |
| **Daily digest** | 9 AM UTC | Summarize undigested alerts via Claude, mark digested |
| **Error budget calculation** | After each run | Recalculate rolling-window success rate, escalate if exhausted |
| **Column lineage update** | Pipeline creation or promotion | Track source-to-target column mappings |
| **Cost logging** | Every Claude API call | Log tokens (input/output/total) and latency |

### 2.2 Proposal-Gated Behaviors (require human approval)

The agent detects a need, generates a proposal with reasoning, confidence, and rollback plan, and waits:

| Behavior | Trigger | Proposal type |
|---|---|---|
| **Schema change** | Non-additive drift detected | `add_column`, `alter_column_type`, `drop_column` |
| **Strategy change** | Agent recommends a better approach | `change_refresh_type`, `change_load_type`, `change_merge_keys` |
| **Schedule change** | Freshness SLA violations suggest too-sparse runs | `change_schedule` |
| **New connector** | User requests a source or target that doesn't exist | `new_connector` |
| **Connector update** | Bug fix or regeneration of an existing connector | `update_connector` |
| **Pipeline halt** | Quality gate FAIL that can't auto-resolve | System alert + optional proposal |

### 2.3 Connector Generation -- The Core Self-Extension Loop

When a user asks for a new data source or destination:

```
1. User asks: "I need to connect to Snowflake"
   -> ConversationManager routes to generate_connector()

2. AgentCore builds a prompt containing:
   - The full TargetEngine abstract interface (source/base.py or target/base.py)
   - A seed connector from the DB as a reference implementation
   - The requested type (e.g. "snowflake") and connection params
   - Any previous attempt error (if retrying)

3. Claude generates a complete Python class implementing the interface.
   Temperature 0.3 -- slightly more creative than reasoning tasks.

4. Sandbox validation (three layers):
   a. AST analysis: parse the code and validate every import against the allowlist;
      reject blocked modules (subprocess, shutil, os, socket, ctypes, etc.)
      and blocked calls (eval, exec, compile, __import__, globals, locals)
   b. safe_exec(): execute with restricted builtins -- dangerous functions
      removed, __import__ replaced with a guarded version
   c. Class validation: find the concrete class implementing SourceEngine
      or TargetEngine, verify all abstract methods are implemented,
      verify get_source_type()/get_target_type() returns the expected value

5. If validation passes: instantiate with provided credentials,
   call test_connection() and list_schemas()/generate_ddl()
   Record test_results. Log token usage and latency to AgentCostLog.

6. If validation or test fails AND attempt < 3:
   Append the error to the prompt and retry.

7. On success:
   - Save ConnectorRecord(status="draft", test_status="passed")
   - Create ContractChangeProposal(change_type="new_connector")
   - If ENCRYPTION_KEY is set, credential fields are encrypted before storage
   - Proposal appears in UI for human review with full generated code

8. On approval:
   - ConnectorRecord.status -> "active"
   - Hot-reload into ConnectorRegistry (no restart)
   - Connector immediately available for new pipeline contracts

9. On rejection:
   - learn_from_rejection() runs
   - AgentPreference saved if confidence >= 0.7
   - Informs future generation attempts for this source/target type
```

### Allowed and Blocked Imports

**Allowed** (connector code may import these):
- Database drivers: `pymysql`, `psycopg2`, `cx_Oracle`, `pyodbc`, `sqlite3`, `asyncpg`, `aiomysql`, `aiopg`
- HTTP/API: `httpx`, `requests`, `urllib.parse`, `urllib.request`
- Data handling: `csv`, `json`, `io`, `gzip`, `zipfile`
- Standard lib: `os.path`, `pathlib`, `datetime`, `decimal`, `uuid`, `hashlib`, `base64`, `re`, `math`, `time`, `dataclasses`, `typing`, `abc`, `inspect`, `logging`, `collections`, `functools`, `itertools`
- Cloud SDKs: `boto3`, `botocore`, `google.cloud.bigquery`, `google.cloud.storage`, `azure.storage.blob`

**Blocked** (rejected at AST parse time):
- Modules: `subprocess`, `shutil`, `signal`, `ctypes`, `multiprocessing`, `threading`, `socket`, `code`, `codeop`, `compileall`, `importlib`, `runpy`, `pkgutil`, `os` (direct -- `os.path` is allowed)
- Calls: `eval()`, `exec()`, `compile()`, `__import__()`, `globals()`, `locals()`, `vars()`, `breakpoint()`, `exit()`, `quit()`
- os methods: `os.system`, `os.popen`, `os.execv`, `os.execve`, `os.fork`, `os.kill`, `os.remove`, `os.unlink`, `os.rmdir`, `os.rename`, `os.makedirs`, `os.listdir`

### 2.4 Schema Drift Response Loop

```
Monitor loop (5m tick)
  -> For each active pipeline:
       SourceEngine.profile_table() -> compare to column_mappings

       New nullable columns only -> auto_approve_additive_schema?
         YES -> apply directly, write SchemaVersion, no proposal
         NO  -> create proposal

       Type changes, dropped columns, non-nullable new columns
         -> analyze_drift() via Claude
         -> Query column_lineage to assess downstream impact
         -> action = "halt" | "propose_change" | "auto_adapt"
         -> create ContractChangeProposal with reasoning + rollback plan + lineage impact
         -> if "halt": pause pipeline until resolved
```

### 2.5 Quality Gate Reasoning Loop

After the 7-check gate runs:

```
reason_about_quality(contract, check_results, decision)
  -> Claude produces 2-4 sentence natural language analysis:
      - What failed/warned and likely root cause
      - Whether this is a source issue, pipeline issue, or expected behavior
      - What the data engineering team should do next
  -> Stored in GateRecord.agent_reasoning
  -> Surfaced in UI quality view
  -> Token usage logged to AgentCostLog
```

### 2.6 Learning From Rejections

Every rejected proposal is an opportunity to improve:

```
Proposal rejected by user with a note
  -> learn_from_rejection(proposal, resolution_note)
       -> Claude extracts a structured preference:
           {scope, preference_key, preference_value, confidence}
       -> If confidence >= 0.7: save AgentPreference
       -> If VOYAGE_API_KEY set: embed preference via Voyage,
          store vector(1024) in pgvector for semantic search
       -> Future propose_strategy() and generate_connector() calls
         consult AgentPreferences for this pipeline/schema/source_type
```

Examples of learned preferences:
- "This pipeline owner prefers merge keys on `user_id`, not `id`"
- "For MySQL sources in the analytics schema, always use incremental + watermark"
- "Generated Postgres connectors should use asyncpg, not psycopg2"

---

## 3. Architecture

### Runtime model

One long-running Python async process with four concurrent async loops:

1. **API Server** -- FastAPI + uvicorn, port 8100. Serves UI static files and all REST endpoints. JWT authentication and rate limiting on all routes.
2. **Scheduler Loop** -- 60s tick. Evaluates cron schedules, respects dependency graph, submits due pipelines. Skips pipelines with exhausted error budgets.
3. **Monitor Loop** -- 5m tick. Schema drift detection, freshness checks, column-level lineage impact analysis, proposal creation, alert dispatch (Slack, Email, PagerDuty).
4. **Observability Loop** -- 30s base tick. Quality trend recomputation every 15m, daily digest at 9 AM UTC.

### Persistence

PostgreSQL 16 with the pgvector extension. All state -- pipeline contracts, connector code, run records, agent preferences, lineage, error budgets, cost logs -- lives in PostgreSQL. Connection pooling via asyncpg with configurable min/max sizes.

For development and testing, `Store.create_tables()` creates all tables with `IF NOT EXISTS`. For production deployments, Alembic migrations in `alembic/versions/` provide versioned schema management.

### Concurrency

`asyncio.Semaphore(max_concurrent)` -- default 4. Each pipeline run is an async task. No external queue.

### External dependencies

| Dependency | Required |
|---|---|
| PostgreSQL 16 with pgvector | Yes |
| At least one registered active SourceEngine connector | Yes |
| At least one registered active TargetEngine connector | Yes |
| Claude API (HTTPS via httpx) | Optional -- rule-based fallback if no key |
| Voyage API (for preference embeddings) | Optional -- enhances learning loop |

Additional database drivers (e.g. `snowflake-connector-python`, `google-cloud-bigquery`) are not bundled. They are declared in the connector's `dependencies` field and installed on demand when a user approves a generated connector.

### Connector model

All source connectors implement `SourceEngine` (`source/base.py`). All target connectors implement `TargetEngine` (`target/base.py`). These abstract classes are the only stable contracts.

Connector implementations -- including the two seed connectors -- are stored as Python source strings in the `connectors` table and loaded dynamically via `safe_exec()` (sandboxed execution with AST validation and restricted builtins) at runtime by `ConnectorRegistry`. There is one code path for all connectors: seeds and agent-generated are treated identically at runtime.

### Seed connectors

Two seed implementations are included as string constants in `connectors/seeds.py`:
- `MYSQL_SOURCE_CODE` -- a complete `MySQLEngine(SourceEngine)` implementation
- `REDSHIFT_TARGET_CODE` -- a complete `RedshiftEngine(TargetEngine)` implementation

On first startup, `ConnectorRegistry.bootstrap_seeds()` writes these to the `connectors` table as `status="active"` records. From that point they are indistinguishable from agent-generated connectors -- they are loaded via `safe_exec()`, resolved by ID, and can be deprecated and replaced.

The seeds serve two purposes: (1) something works immediately, (2) Claude uses one as a reference implementation when generating a new connector of the same class.

### Local storage

```
{data_dir}/
├── staging/
│   └── {pipeline_id}/
│       └── {run_id}/
│           ├── batch_000001.csv
│           └── manifest.json
└── logs/
    └── pipeline-agent.log
```

### Sizing

t3.medium (2 vCPU, 4GB RAM) with 100GB gp3 EBS handles ~50 pipelines. ~$50/month plus RDS or self-hosted PostgreSQL.

---

## 4. Security Model

### JWT Authentication

When `AUTH_ENABLED=true`, all API endpoints require authentication. Three roles with decreasing privileges:

| Role | Capabilities |
|---|---|
| **admin** | Full access: create users, manage all pipelines and connectors, approve proposals |
| **editor** | Create and manage pipelines, approve proposals, trigger runs |
| **viewer** | Read-only access to all data |

Authentication methods:
- **Bearer token** -- `POST /api/auth/login` returns a JWT with `sub`, `role`, `iat`, `exp` claims. Token expiry defaults to 24 hours. Include as `Authorization: Bearer <token>`.
- **API key** -- Set `X-API-Key` header to the value of `JWT_SECRET`. Grants admin access.

When `AUTH_ENABLED=false` (default for development), all requests are treated as admin.

User management:
- `POST /api/auth/register` -- Create a new user (admin only). Password is bcrypt-hashed before storage.
- `GET /api/auth/me` -- Returns current user info decoded from the token.

### Fernet Encryption for Credentials at Rest

When `ENCRYPTION_KEY` is set to a valid Fernet key, all sensitive credential fields are encrypted before being stored in PostgreSQL. The encrypted fields are:

- `password`
- `api_key`
- `secret`
- `token`
- `ssl_ca`, `ssl_key`, `ssl_cert`

Encryption and decryption happen transparently at the API boundary -- credentials are encrypted on write and decrypted on read. The database never contains plaintext credentials when encryption is enabled.

Generate a key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

### Connector Sandboxing

Generated connector code passes through a three-layer security gate:

1. **AST validation** (`sandbox.validate_connector_code()`) -- Parse the source, walk the AST, check every `import` and `from ... import` against the allowlist, check every function call against the blocklist. Reject code with syntax errors.

2. **Restricted builtins** (`sandbox.safe_exec()`) -- Replace standard builtins with a filtered set. Remove dangerous builtins (`eval`, `exec`, `compile`, etc.). Replace `__import__` with a guarded version that enforces the import allowlist at runtime.

3. **Human approval** -- Every generated connector must be reviewed and approved by a human before `status="active"`. The full generated code, test results, and generation log are visible in the approval view.

See Section 2.3 for the full list of allowed and blocked imports.

### Rate Limiting

API endpoints are rate-limited via slowapi (backed by the client IP address). Limits are configurable and protect against abuse.

---

## 5. Two-Tier Autonomy Model

### Tier 1: Fully Autonomous

- Extract, stage, load, merge/append
- Retry on transient failures (exponential backoff)
- Run all 7 quality checks
- Promote on PASS/WARN, halt on FAIL
- Update watermarks and baselines after successful promotion
- Calculate error budgets after each run
- Update column-level lineage
- Log agent cost per Claude API call
- Send alerts per notification policy (Slack, Email, PagerDuty)
- Clean up staging after runs
- Auto-adapt additive schema changes (new nullable columns)

### Tier 2: Requires Human Approval

- Schema changes (type changes, dropped columns, non-nullable additions)
- Strategy changes (refresh type, merge keys, replication method)
- Schedule changes
- New connector code (always requires review before `status="active"`)
- Connector updates and regenerations

The agent generates a proposal with full reasoning, confidence score, impact analysis (downstream dependencies via column lineage, data loss risk, estimated backfill time), and a rollback plan. Rejections feed directly into the learning loop.

---

## 6. Pipeline Execution State Machine

```
PENDING -> EXTRACTING -> LOADING -> QUALITY_GATE -> PROMOTING -> COMPLETE
                                       |
                                  FAIL  v
                                     HALTED -> (alert + optional proposal)
     ^
     +-- RETRYING <- FAILED (if retries remaining)

BACKFILL: same state machine -- EXTRACTING uses date-range filter, watermark NOT updated
```

### Step-by-step

1. **PENDING** -- Scheduler picks up pipeline. Check `run_mode` (scheduled / manual / backfill). Check error budget -- skip if exhausted.
2. **Pre-flight checks** -- Abort if: pending HALT proposal exists, disk space low, upstream dependency failed, connector not `active`.
3. **EXTRACTING** -- SourceEngine connects. Incremental: `WHERE {col} > {watermark}`. Backfill: date range filter. Write CSV batches (50K rows/file default). Each row gets metadata columns: `_extracted_at`, `_source_schema`, `_source_table`, `_row_hash` (SHA-256 of all source column values).
4. **LOADING** -- TargetEngine creates staging table. Streams CSV batches via bulk copy equivalent. Verifies checksums.
5. **QUALITY_GATE** -- 7 checks in order. Short-circuit on FAIL if `halt_on_first_fail=true`.
6. **Decision** -- PASS/WARN -> PROMOTING. FAIL -> HALTED (preserve staging, create alert).
7. **PROMOTING** -- Merge (delete-then-insert on merge keys) or append to target. Drop staging. Clean CSV. Update watermark (not on backfill). Update column lineage.
8. **COMPLETE** -- Write RunRecord. Update baselines (volume, null rates, cardinality). Recalculate error budget.
9. **FAILED** -- Log error. Preserve staging. Retry with exponential backoff if attempts remain. Update error budget.

---

## 7. Quality Gate -- 7 Checks

All checks run against the staging table before promotion to target. The gate is the agent's last line of defense before data lands in production.

| # | Check | PASS | WARN | FAIL |
|---|---|---|---|---|
| 1 | **Count reconciliation** | Exact match of extracted vs staged rows | Within `count_tolerance` | Exceeds tolerance |
| 2 | **Schema consistency** | Staging matches contract + metadata columns | Extra unexpected columns | Missing columns or type mismatches |
| 3 | **Cardinality / PK uniqueness** | No merge key duplicates, cardinality in range | >50% deviation on any column | Merge key duplicates found |
| 4 | **Null rate analysis** | All within baseline threshold | 1-3 anomalous columns | >3 anomalous OR catastrophic jump (< 5% -> > 50%) |
| 5 | **Volume z-score** | z <= 2.0 vs 30-run rolling average | z 2.0-3.0 | z > 3.0 OR zero rows when baseline avg > 100 |
| 6 | **Sample row verification** | Staging count consistent with extraction | Minor count discrepancy | -- |
| 7 | **Freshness** | MAX(watermark) <= 2x schedule interval | 2-5x | > 5x |

```python
if any_check_failed:
    decision = HALT
elif any_check_warned:
    decision = PROMOTE_WITH_WARNING if quality_config.promote_on_warn else HALT
else:
    decision = PROMOTE
```

After the gate, `reason_about_quality()` produces a natural language explanation stored in `GateRecord.agent_reasoning`.

---

## 8. Error Budgets

Each pipeline maintains a rolling-window error budget that tracks reliability over time.

### Calculation

- **Window**: configurable, default 7 days
- **Success rate**: `successful_runs / total_runs` within the window
- **Budget threshold**: configurable, default 90%
- **Budget remaining**: `(success_rate - budget_threshold) / (1.0 - budget_threshold)` (normalized 0.0-1.0)

### Behavior when budget is exhausted

When a pipeline's success rate drops below its budget threshold:

1. The `escalated` flag is set to `true`
2. A **CRITICAL** alert is fired: "Error budget exhausted for pipeline {name}: {success_rate}% success rate over {window_days} days"
3. The alert is dispatched to all configured channels (Slack, Email, PagerDuty)
4. The scheduler **skips** the pipeline on future ticks until the budget recovers or is manually reset

### Manual reset

Reset via the API when the underlying issue is resolved. The budget recalculates on the next run.

### API

- `GET /api/error-budgets/{pipeline_id}` -- Returns: `window_days`, `total_runs`, `successful_runs`, `failed_runs`, `success_rate`, `budget_threshold`, `budget_remaining`, `escalated`, `last_calculated`

---

## 9. Column-Level Lineage

Pipeline Agent tracks source-to-target column mappings at the column level.

### Data model

Each `ColumnLineage` record contains:
- `source_pipeline_id`, `source_schema`, `source_table`, `source_column`
- `target_pipeline_id`, `target_schema`, `target_table`, `target_column`
- `transformation` (default: `direct`)

### Automatic tracking

- On pipeline creation: lineage records are generated from `column_mappings`
- On schema drift resolution: lineage is updated to reflect added, altered, or removed columns
- On promotion: lineage is verified and updated if the schema evolved

### Impact analysis

When schema drift is detected on a source column, the monitor queries the `column_lineage` table to determine:
- Which target columns are affected
- Which downstream pipelines consume those target columns
- The total blast radius of the change

This information is included in `ContractChangeProposal.impact_analysis` so the human reviewer can understand the full scope before approving or rejecting.

### API

- `GET /api/lineage/{pipeline_id}` -- Returns upstream dependencies, downstream dependents, and column-level mappings
- `POST /api/lineage` -- Declare a pipeline dependency
- `DELETE /api/lineage/{dependency_id}` -- Remove a dependency

---

## 10. Agent Cost Tracking

Every Claude API call is instrumented with cost and latency tracking.

### What is logged

Each `AgentCostLog` record contains:

| Field | Description |
|---|---|
| `pipeline_id` | Associated pipeline (empty for global operations) |
| `operation` | Function name: `propose_strategy`, `analyze_drift`, `generate_connector`, `reason_about_quality`, `learn_from_rejection`, `generate_digest` |
| `model` | Claude model used (e.g. `claude-sonnet-4-6`) |
| `input_tokens` | Tokens sent to the API |
| `output_tokens` | Tokens received |
| `total_tokens` | Sum of input + output |
| `latency_ms` | Round-trip time in milliseconds |
| `timestamp` | ISO 8601 timestamp |

### API

- `GET /api/agent-costs` -- Paginated list of cost entries. Filter by `pipeline_id` and date range.
- `GET /api/agent-costs/summary` -- Aggregated totals grouped by operation type. Returns total tokens and total calls per operation.

### Use cases

- Monitor Claude API spend per pipeline
- Identify expensive operations (e.g. connector generation with retries)
- Budget planning for API costs
- Latency monitoring for agent responsiveness

---

## 11. Connector Versioning

When a connector is regenerated, updated, or upgraded, the system creates a `ConnectorMigration` record.

### Data model

| Field | Description |
|---|---|
| `connector_id` | The connector being migrated |
| `from_version` | Previous version number |
| `to_version` | New version number |
| `affected_pipelines` | JSON array of pipeline IDs using this connector |
| `migration_status` | Workflow state |
| `migration_log` | Freeform notes and error details |
| `created_at` / `completed_at` | Timestamps |

### Status workflow

```
pending -> in_progress -> completed
                       -> failed -> rolled_back
```

- **pending**: Migration record created, new connector version saved as draft
- **in_progress**: Migration is being applied (pipelines being updated)
- **completed**: All affected pipelines updated to new connector version
- **failed**: Migration encountered an error
- **rolled_back**: Reverted to previous version after failure

### API

- `GET /api/connector-migrations` -- List migrations, filterable by `connector_id` and `migration_status`

---

## 12. Source Engine Interface

`source/base.py` defines the contract every source connector must implement. This file is the spec Claude generates against when creating a new source connector.

```python
class SourceEngine(ABC):
    INTERFACE_VERSION = "1.0"

    @abstractmethod
    async def test_connection(self) -> ConnectionResult:
        """Connect and return: version, ssl_enabled, connection_count, latency_ms."""

    @abstractmethod
    async def list_schemas(self) -> list[SchemaInfo]:
        """List schemas with table counts."""

    @abstractmethod
    async def profile_table(self, schema: str, table: str) -> TableProfile:
        """Full profile: row_count, columns, primary_keys, timestamp_columns,
        null_rates, cardinality, sample_rows."""

    @abstractmethod
    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging: LocalStagingManager) -> ExtractResult:
        """Extract data to staging CSV batches.
        Returns: rows_extracted, max_watermark, staging_manifest.
        Must add metadata columns to every row:
          _extracted_at, _source_schema, _source_table, _row_hash (SHA-256)."""

    @abstractmethod
    def map_type(self, source_type: str) -> str:
        """Map a source-native type string to a normalized target type."""

    @abstractmethod
    def get_source_type(self) -> str:
        """Return identifier: 'mysql', 'postgres', 'mongodb', etc."""
```

### Seed source (MySQL)

`connectors/seeds.py` contains `MYSQL_SOURCE_CODE` -- a complete `MySQLEngine(SourceEngine)` implementation using PyMySQL with unbuffered streaming. It serves as the reference implementation that Claude uses when asked to generate a new source connector. It is not imported -- it is stored as a string in the DB and loaded via `safe_exec()`.

### Agent-generated sources

When a user says "I need a Postgres source" or "add MongoDB":
- `generate_connector(ConnectorType.SOURCE, "postgres", connection_params)` is called
- Claude generates a full `PostgresEngine(SourceEngine)` class
- AST-validated, sandbox-executed, tested, registered as `ConnectorRecord`
- Approval required before use in any pipeline

There are no pre-built Postgres, MongoDB, S3, REST API, or other connectors. They are generated on demand.

---

## 13. Target Engine Interface

`target/base.py` defines the contract every target connector must implement. Includes both ETL operations and quality gate query methods so any target can run the full 7-check gate.

```python
class TargetEngine(ABC):
    INTERFACE_VERSION = "1.0"

    # ETL operations
    @abstractmethod
    async def test_connection(self) -> ConnectionResult: ...

    @abstractmethod
    def generate_ddl(self, contract: PipelineContract) -> str:
        """Generate CREATE TABLE statement.
        Uses contract.column_mappings for columns/types.
        Uses contract.target_options for target-specific hints
        (e.g. sort_key/dist_key for Redshift, cluster_by for Snowflake).
        Must include metadata columns: _extracted_at, _source_schema,
        _source_table, _row_hash."""

    @abstractmethod
    async def create_table_if_not_exists(self, contract: PipelineContract) -> None: ...

    @abstractmethod
    async def load_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        """Create staging table and stream all CSV batches into it."""

    @abstractmethod
    async def promote(self, contract: PipelineContract, run: RunRecord) -> None:
        """Atomically promote staging to target.
        merge: DELETE matching rows by merge_keys, INSERT from staging.
        append: INSERT from staging directly.
        Drop staging table after success."""

    @abstractmethod
    async def drop_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        """Drop staging table. Idempotent -- safe if table doesn't exist."""

    # Quality gate query methods -- used by all 7 checks
    @abstractmethod
    def get_column_types(self, schema: str, table: str) -> list[dict]: ...

    @abstractmethod
    def get_row_count(self, schema: str, table: str) -> int: ...

    @abstractmethod
    def get_max_value(self, schema: str, table: str, column: str) -> Optional[str]: ...

    @abstractmethod
    def check_duplicates(self, schema: str, table: str, keys: list[str]) -> int: ...

    @abstractmethod
    def get_null_rates(self, schema: str, table: str,
                       columns: list[str]) -> dict[str, float]: ...

    @abstractmethod
    def get_cardinality(self, schema: str, table: str,
                        columns: list[str]) -> dict[str, int]: ...

    @abstractmethod
    def get_target_type(self) -> str:
        """Return identifier: 'redshift', 'snowflake', 'bigquery', 'postgres', etc."""
```

### Seed target (Redshift)

`connectors/seeds.py` contains `REDSHIFT_TARGET_CODE` -- a complete `RedshiftEngine(TargetEngine)` implementation using psycopg2 with bulk COPY loading and transactional merge/append patterns. It is the reference implementation Claude uses when generating new target connectors. Stored as a string in the DB, loaded via `safe_exec()`.

### Agent-generated targets

When a user says "load into Snowflake" or "write to BigQuery":
- `generate_connector(ConnectorType.TARGET, "snowflake", connection_params)` is called
- Claude generates a full `SnowflakeEngine(TargetEngine)` class
- `target_options` carries target-specific config (e.g. `cluster_by` for Snowflake) without any model changes
- AST-validated, sandbox-executed, approval required before use

No Snowflake, BigQuery, Databricks, or other targets are pre-built.

---

## 14. Connector Registry (`connectors/registry.py`)

Central runtime component. One code path for all connectors -- seeds and generated are identical at runtime.

### Startup sequence

```
bootstrap_seeds()
  -> Import MYSQL_SOURCE_CODE, REDSHIFT_TARGET_CODE from connectors/seeds.py
  -> For each seed: if not in DB, write ConnectorRecord(status="active", code=seed_string)
  -> If in DB but code is empty: backfill code (handles upgrades)

load_all_active()
  -> Query all ConnectorRecord where status="active"
  -> For each: safe_exec(code) in sandboxed namespace
  -> Find concrete class implementing SourceEngine or TargetEngine
  -> Cache: connector_id -> class
```

### Resolution

`get_source(connector_id, **kwargs)` / `get_target(connector_id, **kwargs)`
- Look up class by connector_id in cache
- If not cached: load from DB on demand
- Instantiate with `**kwargs` (connection credentials passed at call time, decrypted from DB if encryption is enabled)

### Validation (before any connector is saved)

```
validate_connector_code(code)
  -> AST parse and validate imports/calls against allowlists
  -> safe_exec() with restricted builtins
  -> Find concrete class implementing the right base class
  -> Compare against abstract method set from SourceEngine/TargetEngine
  -> Return (valid: bool, error_message: str)
```

### Security model

Generated code runs through the sandbox -- AST-validated imports and restricted builtins. Mitigations:
- AST analysis blocks dangerous imports and function calls at parse time
- Restricted builtins remove dangerous functions and guard `__import__` at runtime
- Every generated connector must be approved by a human before `status="active"`
- `generation_log` records all Claude prompts and responses for audit
- Connector code is stored verbatim -- the exact approved code is what runs
- Credential fields are encrypted at rest when `ENCRYPTION_KEY` is set
- Deprecation is the only way to remove a connector from active use

---

## 15. Agent Core (`agent/core.py`)

### Claude API integration

- httpx async client, direct HTTPS to `api.anthropic.com`
- Model: `claude-sonnet-4-6`
- Temperature: 0.1 for reasoning, 0.3 for connector generation
- No LangChain, no external vector DB, no memory cache -- all state is in PostgreSQL
- Full graceful degradation to rule-based logic when no API key is present
- Every API call logged to `AgentCostLog` with tokens and latency

### Reasoning functions

**`propose_strategy(profile, preferences)`**
Analyzes a TableProfile. Returns: refresh_type, replication_method, incremental_column, load_type, merge_keys, target_options, tier, cost_estimate, reasoning. Consults `AgentPreferences` before calling Claude (semantic search via pgvector if embeddings enabled). Rule-based fallback: incremental if >10K rows + timestamps; merge if PKs exist; tier by row count.

**`analyze_drift(contract, drift_info, preferences)`**
Evaluates detected schema drift. Returns: action (`auto_adapt` | `propose_change` | `halt`), confidence, reasoning, breaking_change, data_loss_risk, rollback_plan. Rule-based fallback: halt on drops/type-changes, auto_adapt on new nullable columns.

**`reason_about_quality(contract, checks, decision)`**
Natural language analysis of gate results. 2-4 sentences: what failed, likely cause, recommended action, issue classification (source/pipeline/expected). Returns plain text stored in `GateRecord.agent_reasoning`.

**`generate_connector(connector_type, source_target_type, connection_params, attempt=1)`**
Core self-extension function. See Section 2.3 for the full loop. Up to 3 auto-retry attempts on validation/test failure. Not available without API key. Code passes through AST validation and sandbox execution.

**`learn_from_rejection(proposal, resolution_note)`**
Extracts structured preference from rejection note + proposal context. Saves `AgentPreference` if Claude's confidence >= 0.7. Scope can be global, pipeline, schema, or source_type. Optionally embeds via Voyage for pgvector semantic search.

**`generate_digest(alerts, pipeline_names)`**
Daily summary of undigested alerts, grouped by pipeline, sorted by severity. Plain text for email body. Falls back to a simple list without API key.

### Command Routing

The `POST /api/command` endpoint accepts natural language and routes it through Claude to determine the appropriate action. Instead of keyword matching, the agent understands intent:

- "I need to connect to Snowflake" -> connector generation flow
- "Show me the health of my pipelines" -> pipeline status summary
- "Why did the orders pipeline fail?" -> run history + gate reasoning lookup
- "Pause all tier 3 pipelines" -> batch pause operation

Claude determines the action, parameters, and response based on the full conversation context.

---

## 16. Conversation Manager (`agent/conversation.py`)

Stateless onboarding flow. UI holds session state per conversation.

```
Step 1: Test connection
  -> "What database do you want to connect to?"
  -> Check ConnectorRegistry for an active connector of that type
  -> If found: test_connection() with provided credentials
  -> If not found: "I don't have a connector for X yet. Want me to build one?"

Step 2: Generate connector (if needed)
  -> generate_connector() -> approval proposal
  -> On approval: connector becomes active, flow continues

Step 3: List schemas
  -> SourceEngine.list_schemas() -- present summary table

Step 4: Profile tables
  -> SourceEngine.profile_table() per table in selected schema

Step 5: Propose strategies
  -> propose_strategy() per table with reasoning and cost estimates
  -> User can override any recommendation

Step 6: Configure schedule, tier, environment, owner
  -> Accept cron expression or natural language ("every hour")

Step 7: Set load type
  -> Agent proposes merge (if PKs) or append
  -> User confirms or overrides

Step 8: Declare dependencies (optional)
  -> Agent suggests from detected FK relationships
  -> User confirms or adds custom

Step 9: Dry run preview
  -> 5 sample rows, DDL, strategy summary, cost estimate
  -> User approves

Step 10: Create pipeline
  -> Save PipelineContract + initial SchemaVersion + ColumnLineage records
  -> Pipeline enters active scheduling
```

---

## 17. Scheduler (`scheduler/manager.py`)

- `croniter` for cron evaluation. 60s tick.
- `asyncio.Semaphore(max_concurrent)` -- default 4.
- **Error budget check**: before submitting any pipeline, verify its error budget is not exhausted. If exhausted, skip and log.
- **Dependency graph**: reads `PipelineDependency` table. Topological sort at startup. Log warning and skip cycle members.
- **Connector check**: before submitting any pipeline, verify both `source_connector_id` and `target_connector_id` are `status="active"`. If either is not, skip and alert -- a paused connector halts all pipelines using it.
- **Backfill**: `POST /api/pipelines/{id}/backfill` creates a `RunRecord(run_mode="backfill")` with date bounds. Watermark NOT updated on backfill runs.
- **Retry**: exponential backoff -- `retry_backoff_seconds x (2 ^ retry_count)`.

---

## 18. Monitor (`monitor/engine.py`)

### Schema Drift Detection (5m tick)

For each active pipeline:
1. Call `SourceEngine.profile_table()` to get current live schema
2. Compare to `contract.column_mappings`
3. Classify changes: new columns (nullable/not), dropped columns, type changes

If `auto_approve_additive_schema=true` AND change is only new nullable columns: auto-apply, write SchemaVersion, no proposal.

Otherwise: call `analyze_drift()`, query `column_lineage` for downstream impact, create `ContractChangeProposal` with reasoning, rollback plan, and lineage-informed impact analysis.

### Freshness Monitoring

Compute staleness per pipeline using `TargetEngine.get_max_value(freshness_column)`.

For full-refresh tables: staleness = time since last successful run.

Save `FreshnessSnapshot`. Alert on WARNING or CRITICAL per tier thresholds and `NotificationPolicy`.

### Alert Dispatch

Alerts are dispatched based on severity, tier, and notification policy:

**Slack** -- POST to incoming webhook URL. Message includes severity emoji, pipeline name, tier badge, and summary.

**Email** -- SMTP via `smtplib`. Subject includes severity and pipeline name. Body includes full detail.

**PagerDuty** -- Events API v2 via `https://events.pagerduty.com/v2/enqueue`. Integration details:
- Uses the `PAGERDUTY_ROUTING_KEY` (Events API v2 routing key)
- Dedup key: `pipeline-agent-{pipeline_id}-{alert_type}` to prevent duplicate incidents
- Severity mapping: `CRITICAL` -> `critical`, `WARNING` -> `warning`, `INFO` -> `info`
- Payload includes: pipeline name, tier, summary, full detail, source, timestamp
- Only dispatched for alerts matching the notification policy severity filter

---

## 19. Approval Workflow

### Lifecycle

```
DETECTED -> PROPOSAL CREATED -> PENDING
  APPROVED  -> APPLIED (contract version++, SchemaVersion written if schema change)
  APPROVED (connector) -> ConnectorRecord.status = "active", registry hot-reloaded
  REJECTED  -> resolution_note stored -> learn_from_rejection() -> AgentPreference updated
  APPLIED   -> ROLLED BACK (on apply failure)
```

### What every proposal contains

- `change_type`: what kind of change
- `reasoning`: why the agent recommends this
- `confidence`: 0.0-1.0
- `current_state` / `proposed_state`: before/after as JSON
- `impact_analysis`: downstream pipelines affected (via column lineage), data_loss_risk, estimated_backfill_time, cost_delta
- `rollback_plan`: how to revert if applied and something goes wrong
- For connector proposals: full generated code in `ConnectorRecord.code`, viewable before approval

---

## 20. Data Models

All models stored in PostgreSQL. Python dataclasses. JSONB fields for nested/variable data. All datetimes ISO 8601.

### 20.1 PipelineContract

```
pipeline_id          TEXT PRIMARY KEY (UUID)
pipeline_name        TEXT UNIQUE
version              INTEGER
created_at / updated_at  TEXT
status               TEXT (active | paused | failed | archived)
environment          TEXT (default "production")

-- Source
source_connector_id  TEXT FK -> connectors
source_host / port / database / schema / table  TEXT/INT

-- Target
target_connector_id  TEXT FK -> connectors
target_schema        TEXT (default "raw")
target_table         TEXT
target_options       JSONB (connector-specific hints, e.g.
                       {"sort_key": "id"} for Redshift,
                       {"cluster_by": ["date"]} for Snowflake)

-- Strategy
refresh_type         TEXT (full | incremental)
replication_method   TEXT (watermark | cdc | snapshot)
incremental_column   TEXT (nullable)
last_watermark       TEXT (nullable)
load_type            TEXT (append | merge)
merge_keys           JSONB (array)

-- Schedule
schedule_cron        TEXT
retry_max_attempts   INTEGER (default 3)
retry_backoff_seconds INTEGER (default 60)
timeout_seconds      INTEGER (default 3600)

-- Schema
column_mappings      JSONB (array of ColumnMapping)
target_ddl           TEXT

-- Quality
quality_config       JSONB (QualityConfig)

-- Staging
staging_adapter      TEXT (default "local")

-- Observability
tier                 INTEGER (1 = Critical, 2 = Standard, 3 = Advisory)
tier_config          JSONB
notification_policy_id TEXT (nullable FK -> notification_policies)
tags                 JSONB
owner                TEXT (nullable)
freshness_column     TEXT (nullable)

-- Agent Reasoning
agent_reasoning      JSONB (refresh_type_reason, load_type_reason, merge_keys_reason,
                           tier_reason, cost_estimate: {rows_per_run_estimate, strategy_cost_note})

-- Profiling Baselines
baseline_row_count / baseline_null_rates / baseline_null_stddevs (JSONB)
baseline_cardinality (JSONB) / baseline_volume_avg / baseline_volume_stddev (DOUBLE PRECISION)

-- Approval settings
auto_approve_additive_schema  BOOLEAN (default false)
approval_notification_channel TEXT
```

### 20.2 ConnectorRecord

Stores generated connector code. The registry loads this at runtime.

```
connector_id         TEXT PRIMARY KEY (UUID)
connector_name       TEXT UNIQUE (e.g. "mysql-source-v1", "snowflake-target-v1")
connector_type       TEXT (source | target)
source_target_type   TEXT (mysql | postgres | redshift | snowflake | bigquery | ...)
version              INTEGER
generated_by         TEXT (claude-sonnet-4-6 | user_provided | seed)
interface_version    TEXT (which SourceEngine/TargetEngine version it implements)
code                 TEXT (full Python class -- loaded via safe_exec())
dependencies         JSONB (pip packages required, e.g. ["snowflake-connector-python>=3.0"])
test_status          TEXT (untested | passed | failed)
test_results         JSONB (nullable -- output of test_connection + schema discovery)
generation_attempts  INTEGER
generation_log       JSONB (nullable -- Claude's reasoning, prompts, responses for audit)
status               TEXT (draft | approved | active | deprecated)
approved_by          TEXT (nullable)
approved_at          TEXT (nullable)
created_at / updated_at  TEXT
```

### 20.3 ColumnMapping

Stored as JSONB array in PipelineContract.

```
source_column / source_type / target_column / target_type
is_nullable / is_primary_key / is_incremental_candidate / ordinal_position
```

### 20.4 QualityConfig

Stored as JSONB in PipelineContract.

```
count_tolerance              FLOAT (default 0.001)
null_rate_stddev_threshold   FLOAT (default 2.0)
null_rate_catastrophic_jump  FLOAT (default 0.45)
null_rate_max_anomalies_warn INTEGER (default 3)
cardinality_deviation_threshold FLOAT (default 0.5)
volume_z_score_warn          FLOAT (default 2.0)
volume_z_score_fail          FLOAT (default 3.0)
volume_baseline_runs         INTEGER (default 30)
freshness_warn_multiplier    FLOAT (default 2.0)
freshness_fail_multiplier    FLOAT (default 5.0)
halt_on_first_fail           BOOLEAN (default true)
promote_on_warn              BOOLEAN (default true)
```

### 20.5 TierConfig

| Field | Tier 1 (Critical) | Tier 2 (Standard) | Tier 3 (Advisory) |
|---|---|---|---|
| freshness_warn_minutes | 15 | 120 | 1440 |
| freshness_critical_minutes | 30 | 360 | 4320 |
| freshness_check_interval_seconds | 60 | 300 | 3600 |
| max_consecutive_failures | 1 | 3 | 5 |
| quality_warn_threshold | 0.995 | 0.98 | 0.95 |
| quality_critical_threshold | 0.99 | 0.95 | 0.90 |
| alert_channels | ["slack:urgent"] | ["slack:alerts", "email"] | ["email:digest"] |
| escalation_after_minutes | 10 | 60 | 1440 |
| digest_only | false | false | true |
| retry_urgency | immediate | standard | lazy |

### 20.6 RunRecord

```
run_id / pipeline_id / started_at / completed_at
status  TEXT (pending | extracting | staging | loading | quality_gate |
              promoting | complete | failed | halted | retrying)
run_mode TEXT (scheduled | manual | backfill)
backfill_start / backfill_end  TEXT (nullable)
rows_extracted / rows_loaded   INTEGER
watermark_before / watermark_after  TEXT (nullable)
staging_path / staging_size_bytes
drift_detected / quality_results  JSONB
gate_decision  TEXT (promote | promote_with_warning | halt)
error / retry_count
```

### 20.7-20.11 Supporting models

**GateRecord**: gate_id, run_id, pipeline_id, decision, checks (JSONB array of CheckResult), agent_reasoning, evaluated_at

**CheckResult** (JSONB in GateRecord): check_name, status (pass|warn|fail), detail, metadata, duration_ms

**ContractChangeProposal**: proposal_id, pipeline_id (nullable), connector_id (nullable), status, trigger_type, change_type, current_state, proposed_state (JSONB), reasoning, confidence, impact_analysis (JSONB -- includes column lineage impact), rollback_plan, resolved_by, resolution_note, rejection_learning (JSONB), contract_version_before/after

**SchemaVersion**: version_id, pipeline_id, version, column_mappings (JSONB), change_summary, change_type, proposal_id, applied_at, applied_by

**PipelineDependency**: dependency_id, pipeline_id (downstream), depends_on_id (upstream), dependency_type (fk_inferred | user_defined | agent_recommended), notes

### 20.12-20.16 Observability models

**NotificationPolicy**: policy_id, policy_name, channels (JSONB -- type, target, severity_filter per channel), digest_hour

**FreshnessSnapshot**: snapshot_id, pipeline_id, tier, staleness_minutes, freshness_sla_minutes, sla_met, status (fresh|warning|critical), last_record_time, checked_at

**AlertRecord**: alert_id, severity, tier, pipeline_id, summary, detail (JSONB), created_at, acknowledged, acknowledged_by, acknowledged_at, digested

**DecisionLog**: id (SERIAL), pipeline_id, connector_id, decision_type, detail, reasoning, created_at -- append-only log of every agent decision

**AgentPreference**: preference_id, scope (global|pipeline|schema|source_type|target_type), scope_value, preference_key, preference_value (JSONB), source (user_explicit|rejection_inferred|approval_pattern), confidence, usage_count, embedding (vector(1024) via pgvector), last_used

### 20.17-20.20 New entities

**ErrorBudget**: pipeline_id (PK, FK -> pipelines), window_days, total_runs, successful_runs, failed_runs, success_rate, budget_threshold, budget_remaining, escalated, last_calculated

**ColumnLineage**: id (PK), source_pipeline_id, source_schema, source_table, source_column, target_pipeline_id, target_schema, target_table, target_column, transformation, created_at

**AgentCostLog**: id (PK), pipeline_id, operation, model, input_tokens, output_tokens, total_tokens, latency_ms, timestamp

**ConnectorMigration**: id (PK), connector_id (FK -> connectors), from_version, to_version, affected_pipelines (JSONB), migration_status, migration_log, created_at, completed_at

**User**: id (PK), username (UNIQUE), password_hash (bcrypt), role (admin|editor|viewer), created_at, last_login

---

## 21. API Endpoints

### Authentication
- `POST /api/auth/login` -- Authenticate with username + password, receive JWT
- `POST /api/auth/register` -- Register new user (admin only)
- `GET /api/auth/me` -- Current user info from token

### Health & Metrics
- `GET /health` -- Health check
- `GET /metrics` -- Runtime metrics (pipeline counts, connector counts, run stats)

### Command
- `POST /api/command` -- Agent-routed natural language command interface

### Connection & Discovery
- `POST /api/connection/test-source` -- Test source connection with credentials
- `POST /api/connection/test-target` -- Test target connection with credentials
- `GET /api/discovery/schemas` -- List schemas via SourceEngine
- `POST /api/discovery/profile` -- Profile table, return TableProfile
- `POST /api/discovery/propose` -- Run propose_strategy(), return reasoning + cost estimate

### Connectors
- `GET /api/connectors` -- List (filter: type, status)
- `GET /api/connectors/{id}` -- Detail + code + test results + generation log
- `POST /api/connectors/generate` -- Trigger generate_connector() (body: connector_type, source_target_type, connection_params)
- `POST /api/connectors/{id}/test` -- Re-run test_connection() against live credentials
- `DELETE /api/connectors/{id}` -- Deprecate (fails if any active pipeline references it)

### Pipeline Management
- `GET /api/pipelines` -- List (filter: tier, status)
- `GET /api/pipelines/{id}` -- Full contract
- `POST /api/pipelines` -- Create | `POST /api/pipelines/batch` -- Batch create
- `PATCH /api/pipelines/{id}` -- Update
- `POST /api/pipelines/{id}/trigger` -- Manual run
- `POST /api/pipelines/{id}/backfill` -- Backfill with {start, end}
- `POST /api/pipelines/{id}/pause` | `resume`
- `GET /api/pipelines/{id}/preview` -- Dry run: sample rows + DDL + strategy
- `GET /api/pipelines/{id}/runs` -- Run history
- `GET /api/pipelines/{id}/schema-history` -- Full SchemaVersion history

### Approvals
- `GET /api/approvals` -- List (default: pending only)
- `POST /api/approvals/{id}` -- Resolve (body: action "approve"|"reject", user, note)

### Lineage
- `GET /api/lineage/{id}` -- Upstream + downstream dependency graph + column-level mappings
- `POST /api/lineage` -- Declare dependency
- `DELETE /api/lineage/{dependency_id}` -- Remove

### Quality & Observability
- `GET /api/quality/{id}` -- Gate history
- `GET /api/observability/freshness` -- Current freshness report
- `GET /api/observability/alerts` -- Alert feed (filter: severity, pipeline, acknowledged)
- `POST /api/observability/alerts/{id}/acknowledge` -- Acknowledge an alert

### Error Budgets
- `GET /api/error-budgets/{pipeline_id}` -- Budget status with success rate, remaining budget, escalation state

### Agent Costs
- `GET /api/agent-costs` -- Paginated cost log (filter: pipeline_id, date range)
- `GET /api/agent-costs/summary` -- Aggregated totals by operation type

### Connector Migrations
- `GET /api/connector-migrations` -- List (filter: connector_id, migration_status)

### Notification Policies
- `GET /api/policies` | `POST /api/policies` | `GET /api/policies/{id}` | `PATCH /api/policies/{id}` | `DELETE /api/policies/{id}`

### Preferences
- `GET /api/preferences` -- List (filter: scope, pipeline_id)
- `POST /api/preferences` -- Create manually
- `DELETE /api/preferences/{id}` -- Delete

---

## 22. UI

React SPA. CDN React 18 + Babel standalone + Tailwind. Light theme.

### Design tokens
- Background: `#f7f8fa`, surfaces: `#ffffff`, borders: `#e3e6ec`
- Text: primary `#1a1d24`, secondary `#4a5068`, muted `#7c8498`
- Status: green `#0d8c5e`, blue `#2563eb`, red `#dc2626`, amber `#d97706`, purple `#7c3aed`
- Tier badges: T1 = red, T2 = amber, T3 = blue
- Cards: `box-shadow: 0 1px 3px rgba(0,0,0,0.04)` -- JetBrains Mono for data, Outfit for UI

### Nine views

1. **Command** -- Chat interface. Agent avatar left, user right. Quick chips: "test connection", "discover schemas", "add a new source", "show pipelines". Connector generation happens here -- shows generation progress, test results, approval card inline. Agent-routed command processing (not keyword matching).

2. **Pipelines** -- List: status dot, tier badge, name, source connector -> target connector, refresh/load pills, schedule, owner. Expand for: config, agent reasoning panel (green card with cost estimate), recent runs, error budget status. Actions: Trigger, Backfill, Pause/Resume.

3. **Activity** -- Chronological run feed: status dot, tier badge, run mode badge, timestamp, pipeline name, row count, gate decision.

4. **Freshness** -- Grouped by tier: status dot, name, staleness progress bar, SLA label.

5. **Quality** -- Per-pipeline cards: tier badge, name, 7-day success rate, error budget indicator, 20-run color grid of check outcomes.

6. **Approvals** -- Pending proposals (amber tint): change type, trigger type, reasoning, confidence, lineage impact summary. For connector proposals: generated code in collapsible code block. Resolved history below.

7. **Lineage** -- DAG visualization: nodes (tier badge + status), edges (dependency type). Click for impact summary with column-level detail. Failed nodes red with downstream count.

8. **Connectors** -- Registry: all connectors with type, source/target type, version, status badge, test status, generated-by label. Actions: Test, Regenerate, View Code, Deprecate. Draft connectors show "Awaiting Approval" banner. Migration history visible per connector.

9. **Alerts** -- Feed: severity, tier badge, pipeline name, timestamp, summary, acknowledge. Error budget exhaustion alerts highlighted.

---

## 23. Configuration

### PostgreSQL

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `pipeline_agent` | Database name |
| `PG_USER` | `pipeline_agent` | Database user |
| `PG_PASSWORD` | `pipeline_agent` | Database password |
| `PG_POOL_MIN` | `2` | Minimum asyncpg connection pool size |
| `PG_POOL_MAX` | `10` | Maximum asyncpg connection pool size |

### Agent

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(empty)* | Claude API key. Without this, rule-based fallback only -- connector generation unavailable. |
| `AGENT_MODEL` | `claude-sonnet-4-6` | Claude model for all reasoning and generation. |

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
| `PAGERDUTY_ROUTING_KEY` | *(empty)* | PagerDuty Events API v2 routing key. |

### Server

| Variable | Default | Description |
|---|---|---|
| `API_HOST` | `0.0.0.0` | Bind address for the FastAPI server. |
| `API_PORT` | `8100` | Port. |
| `LOG_LEVEL` | `INFO` | Python logging level. |

### Authentication

| Variable | Default | Description |
|---|---|---|
| `AUTH_ENABLED` | `false` | Set `true` to require JWT auth on all endpoints. |
| `JWT_SECRET` | *(empty)* | Secret key for signing JWTs. Also used as API key when set in `X-API-Key` header. |
| `JWT_ALGORITHM` | `HS256` | JWT signing algorithm. |
| `JWT_EXPIRY_HOURS` | `24` | Token expiry in hours. |

### Encryption

| Variable | Default | Description |
|---|---|---|
| `ENCRYPTION_KEY` | *(empty)* | Fernet key for encrypting credentials at rest. Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. |

---

## 24. Project Structure

```
pipeline-agent/
├── main.py                     # Entry point -- wires 4 async loops
├── config.py                   # Environment config with defaults
├── auth.py                     # JWT authentication (Bearer tokens, API key, role-based access)
├── crypto.py                   # Fernet encryption for credentials at rest
├── sandbox.py                  # AST validation + restricted builtins + import whitelist
├── docker-compose.yml          # PostgreSQL 16 with pgvector
├── requirements.txt
├── .env.example
├── README.md
│
├── agent/
│   ├── core.py                 # Claude API reasoning + connector generation + learning + cost logging
│   ├── conversation.py         # Onboarding flow (10 steps, stateless)
│   └── autonomous.py           # Pipeline execution state machine
│
├── connectors/
│   ├── registry.py             # Load, resolve, validate, hot-reload -- safe_exec() based
│   └── seeds.py                # MYSQL_SOURCE_CODE + REDSHIFT_TARGET_CODE as strings
│
├── source/
│   └── base.py                 # SourceEngine abstract base class (INTERFACE_VERSION = "1.0")
│
├── target/
│   └── base.py                 # TargetEngine abstract base class (INTERFACE_VERSION = "1.0")
│
├── staging/
│   └── local.py                # CSV staging manager
│
├── quality/
│   └── gate.py                 # 7-check quality gate (types against TargetEngine)
│
├── contracts/
│   ├── models.py               # All dataclasses + enums + tier defaults (PipelineContract,
│   │                           #   ConnectorRecord, ErrorBudget, ColumnLineage, AgentCostLog,
│   │                           #   ConnectorMigration, User, ...)
│   └── store.py                # PostgreSQL + asyncpg CRUD for all entities
│
├── monitor/
│   └── engine.py               # Drift detection, freshness monitoring, lineage impact analysis,
│                               #   alert dispatch (Slack, Email, PagerDuty)
│
├── scheduler/
│   └── manager.py              # Cron scheduler + dependency resolution + error budget check + retry
│
├── api/
│   └── server.py               # FastAPI -- all routes with JWT auth + rate limiting
│
├── ui/
│   ├── index.html
│   └── App.jsx                 # Full React SPA -- all 9 views
│
├── tests/
│   ├── conftest.py             # Shared fixtures (PostgreSQL pool, store, registry)
│   ├── test_sandbox.py         # Sandbox AST validation + restricted execution
│   └── test_crypto.py          # Encryption round-trip tests
│
└── alembic/
    ├── env.py                  # Alembic environment config
    └── versions/
        └── 001_initial.py      # Initial migration (all tables)
```

There are no subdirectories under `source/` or `target/` for specific databases. All connector implementations live in the `connectors` table in PostgreSQL as code strings. `source/base.py` and `target/base.py` are the only importable engine files.

---

## 25. Dependencies

```
# Database
asyncpg>=0.29.0               # Async PostgreSQL driver + connection pooling
psycopg2-binary>=2.9.9        # Required by seed Redshift connector
pgvector>=0.2.4                # pgvector Python bindings
sqlalchemy>=2.0.0              # Used by Alembic
alembic>=1.13.0                # Database migrations

# Seed connectors
PyMySQL>=1.1.0                 # Required by seed MySQL connector

# API framework
fastapi>=0.109.0               # REST API
uvicorn[standard]>=0.27.0      # ASGI server
pydantic>=2.5.0                # Data validation
slowapi>=0.1.9                 # Rate limiting

# Auth
PyJWT>=2.8.0                   # JWT token creation and verification
bcrypt>=4.1.0                  # Password hashing

# Encryption
cryptography>=41.0.0           # Fernet symmetric encryption

# HTTP client
httpx>=0.26.0                  # Claude API + Voyage API + PagerDuty + Slack

# Scheduling
croniter>=2.0.1                # Cron schedule evaluation

# Testing
pytest>=8.0.0                  # Test runner
pytest-asyncio>=0.23.0         # Async test support
pytest-cov>=4.1.0              # Coverage reporting
```

Additional drivers (e.g. `snowflake-connector-python`, `google-cloud-bigquery`, `motor` for MongoDB) are only installed if and when the agent generates a connector that declares them in `dependencies`.

---

## 26. Testing Strategy

### Unit tests
- Quality gate: PASS/WARN/FAIL for each of the 7 checks independently
- Rule-based strategy: correct recommendations for representative table profiles
- ConnectorRegistry: load, validate, deprecate, hot-reload
- Sandbox: AST validation for allowed/blocked imports, restricted builtins, blocked calls
- Crypto: Fernet encrypt/decrypt round-trips, field-level dict encryption
- learn_from_rejection: AgentPreference created/updated on rejection
- generate_connector: mock Claude responses for pass/fail/retry scenarios
- Contract store CRUD: all entity types against PostgreSQL
- Error budget: calculation, exhaustion, escalation

### Integration tests
- Seed MySQL connector: test_connection, list_schemas, profile_table, extract -- against real/dockerized MySQL
- Seed Redshift connector: DDL, bulk load, merge, append -- against Redshift or local Postgres
- Full pipeline: MySQL -> staging -> quality gate -> Redshift
- Connector generation: valid/invalid/retry code -- verify sandbox + registry behavior
- Dependency graph: topological sort, cycle detection
- Schema drift: all change types (add/alter/drop), auto-adapt vs. propose, lineage impact
- Backfill: verify watermark NOT updated, date filter correct
- Auth: JWT token creation, verification, role-based access control
- Encryption: credential encrypt/decrypt through the API layer

### API tests
- Auth: login, register, token verification, role enforcement
- Connector generate -> approve -> pipeline create -> trigger -- full flow
- Rejection -> rejection_learning written -> AgentPreference created
- Backfill: run_mode="backfill", watermark unchanged after run
- Error budget: exhaustion triggers CRITICAL alert, scheduler skips pipeline
- Cost tracking: verify AgentCostLog entries created for Claude API calls

---

## 27. Roadmap

The roadmap is a list of **agent capabilities** to add -- not a list of connectors to build. Connectors are generated on demand.

### Phase 2: Transformation Agent

Agent proposes SQL transformations on raw tables -- joins, aggregations, derived metrics. Same contract model, quality gate, and approval workflow. Agent-generated dbt-equivalent models. Lineage graph extended to include transformation outputs.

### Phase 3: Delivery Agent

Reverse ETL -- push curated data to downstream consumers. Same contract model. Target connectors generated on demand (Salesforce, HubSpot, REST webhooks, S3 exports). Agent understands consumer freshness SLAs and schedules accordingly.

### Phase 4: CDC Replication

Upgrade `replication_method = "cdc"` from stub to full implementation for seed connectors (MySQL binlog, Postgres WAL). The interface already handles it -- this is a capability addition within the existing connector model, not a new model.

### Phase 5: Cross-Pipeline Intelligence

Agent reasons across the full pipeline graph: "five tables in the same source schema all degraded in the same hour -- likely a source-side issue, not individual pipeline failures." Requires mature lineage data and accumulated AgentPreferences.

### Phase 6: Connector Marketplace

Generated connectors are exportable and importable as `ConnectorRecord` artifacts. One team generates a Databricks target connector, exports the code string, another team imports and approves it in their instance. The `code` field in `ConnectorRecord` is the portable artifact -- no packaging, no publish step.
