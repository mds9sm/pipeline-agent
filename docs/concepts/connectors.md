# Connectors

Connectors are the bridge between external data systems and DAPOS. Every source and target is accessed through a connector — a sandboxed Python class that implements a standard interface.

---

## Architecture

All connector code lives in **PostgreSQL**, not on the filesystem. Connectors are loaded dynamically via `exec()` at runtime. This means:

- Seed connectors and AI-generated connectors are architecturally identical
- Connectors can be updated, versioned, and rolled back without redeployment
- New connectors take effect immediately after approval (hot-reload)

---

## Connector Types

### Source Connectors

Implement the `SourceEngine` abstract base class:

| Method | Purpose |
|--------|---------|
| `test_connection()` | Verify connectivity, return ConnectionResult |
| `list_schemas()` | Discover available schemas/databases |
| `profile_table(schema, table)` | Row count, columns, types, keys, statistics |
| `extract(contract, run, staging_dir)` | Pull data, write CSVs, return ExtractResult |
| `map_type(source_type)` | Map source-native types to standard types |
| `get_source_type()` | Return identifier (e.g., "mysql", "stripe") |

**ExtractResult** includes: `rows_extracted`, `watermark_value`, `csv_paths`, `bytes_extracted`.

### Target Connectors

Implement the `TargetEngine` abstract base class:

| Method | Purpose |
|--------|---------|
| `test_connection()` | Verify connectivity |
| `generate_ddl(contract)` | Generate CREATE TABLE statement |
| `create_table_if_not_exists(contract)` | Ensure target table exists |
| `load_staging(contract, run)` | Stream CSVs into staging table |
| `promote(contract, run)` | Merge or append staging → target |
| `drop_staging(contract, run)` | Clean up staging table |
| `execute_sql(sql)` | Run post-promotion hooks |

**Quality gate methods** (used by the 7-check gate):
- `get_column_types()`, `get_row_count()`, `get_max_value()`
- `check_duplicates()`, `get_null_rates()`, `get_cardinality()`

---

## Seed Connectors (8 Built-in)

DAPOS ships with 8 seed connectors that auto-install on first startup:

### Sources (6)

| Connector | System | Protocol |
|-----------|--------|----------|
| MySQL | MySQL 5.7+ / MariaDB | `pymysql` |
| SQLite | SQLite 3 | `sqlite3` (stdlib) |
| MongoDB | MongoDB 4+ | `pymongo` |
| Stripe | Stripe API | `httpx` REST |
| Google Ads | Google Ads API | `httpx` REST |
| Facebook Insights | Facebook Marketing API | `httpx` REST |

### Targets (2)

| Connector | System | Protocol |
|-----------|--------|----------|
| PostgreSQL | PostgreSQL 12+ | `asyncpg` / `psycopg2` |
| Redshift | Amazon Redshift | `psycopg2` with Redshift SQL dialect |

### Seed Auto-Update

When `connectors/seeds.py` changes and the app restarts, existing seed connectors are **automatically updated** with the new code (comparing `existing.code != code`).

---

## AI-Generated Connectors

The agent generates new connectors from natural language:

```
User: "I need to connect to Oracle database"
Agent: "I'll generate an Oracle source connector. Here's the proposal..."
```

### Generation Flow

1. **Request**: User describes the system via chat or API
2. **Generation**: Claude generates a complete SourceEngine/TargetEngine implementation
3. **AST Validation**: Code is statically analyzed for dangerous operations
4. **Sandbox Execution**: Code is exec'd in a restricted environment to verify it runs
5. **Interface Check**: Validates all abstract methods are implemented
6. **Proposal Created**: Human must approve before connector becomes active
7. **Hot-Reload**: Once approved, connector is immediately available

### Sandbox Security

All connector code runs through strict validation:

**AST Blocked**:
- `eval()`, `exec()`, `compile()`
- `__import__()`, `breakpoint()`
- `subprocess`, `os.system`, `os.exec*`
- `socket`, `threading`, `multiprocessing`

**Import Allowlist** (40+ modules):
- Database drivers: `pymysql`, `psycopg2`, `pymongo`, `sqlite3`, `cx_Oracle`, `pyodbc`
- HTTP: `httpx`, `requests`, `urllib`
- Cloud SDKs: `boto3`, `google.cloud`, `azure`
- Data: `csv`, `json`, `pandas`, `pyarrow`
- Standard lib: `os.path`, `datetime`, `hashlib`, `uuid`

**API**:
- `POST /api/connectors/generate` — generate connector (admin only, rate-limited 10/min)
- `POST /api/connectors/{id}/test` — test connector with parameters
- `POST /api/connectors/{id}/deprecate` — mark connector as deprecated

---

## Metadata Columns

Every source connector injects 4 metadata columns into extracted data:

| Column | Type | Purpose |
|--------|------|---------|
| `_extracted_at` | TIMESTAMP | UTC time of extraction |
| `_source_schema` | VARCHAR | Originating schema/database |
| `_source_table` | VARCHAR | Originating table name |
| `_row_hash` | VARCHAR | SHA-256 hash of all column values (dedup key) |

These enable:
- **Lineage tracking**: know exactly when and where each row came from
- **Deduplication**: `_row_hash` detects true duplicates across runs
- **Debugging**: trace data back to source with exact extraction timestamp

---

## Registered Sources

Admins can register named data sources so analysts pick from a friendly list instead of entering credentials each time.

| Field | Example |
|-------|---------|
| `display_name` | "Production MySQL" |
| `connector_name` | "mysql-source" |
| `connection_params` | `{host, port, user, password}` (encrypted) |
| `description` | "Main e-commerce database" |
| `owner` | "data-team" |
| `schema_cache` | Cached table/column discovery |

**API**:
- `POST /api/sources` — register (admin)
- `GET /api/sources` — list all sources
- `POST /api/sources/{id}/discover` — refresh schema cache

---

## Connector Lifecycle

```
Seeds loaded on startup
        ↓
User requests new connector (chat/API)
        ↓
Agent generates code (Claude)
        ↓
AST validation + sandbox exec
        ↓
Proposal created (PENDING)
        ↓
Human approves → Hot-reload → Available
        ↓
Updates tracked in GitOps repo
```
