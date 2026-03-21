# Connector Generation

The agent generates production-ready source and target connectors from natural language descriptions using Claude. Generated connectors go through AST validation, sandbox execution, and human approval before becoming active.

---

## Generation Flow

```
User request → Claude generates code → AST validation → Sandbox exec → Proposal → Approval → Hot-reload
```

### Step by Step

1. **Request**: User says "I need an Oracle source connector" via chat or `POST /api/connectors/generate`
2. **Context**: Agent provides Claude with the `SourceEngine` or `TargetEngine` interface definition, available import modules, and best practices
3. **Generation**: Claude writes a complete Python class implementing all abstract methods
4. **AST Validation**: Static analysis blocks dangerous operations (eval, exec, subprocess, socket, etc.)
5. **Sandbox Execution**: Code is `exec()`'d in a restricted environment with limited builtins
6. **Interface Check**: Verifies all abstract methods are implemented and `INTERFACE_VERSION = "1.0"` is declared
7. **Proposal Created**: `ContractChangeProposal` with `change_type=NEW_CONNECTOR`, status=PENDING
8. **Human Review**: Admin reviews generated code, approves or rejects
9. **Hot-Reload**: Once approved, connector is immediately available for pipeline creation

---

## What Gets Generated

### Source Connector
A complete class inheriting `SourceEngine` with:
- `test_connection()` — connectivity check using the appropriate driver
- `list_schemas()` — schema/database discovery
- `profile_table()` — row count, column types, keys, statistics
- `extract()` — incremental or full extraction writing CSVs with metadata columns
- `map_type()` — source-native to standard type mapping

### Target Connector
A complete class inheriting `TargetEngine` with:
- `test_connection()` — connectivity check
- `generate_ddl()` — CREATE TABLE statement in target dialect
- `load_staging()` — CSV streaming into staging table
- `promote()` — merge (upsert) or append from staging to target
- `drop_staging()` — cleanup
- Quality gate query methods (row count, null rates, duplicates, etc.)
- `execute_sql()` — for post-promotion hooks

---

## Security

### AST Validation (Static Analysis)
Blocked at parse time:
- `eval()`, `exec()`, `compile()`, `__import__()`
- `breakpoint()`, `exit()`, `quit()`
- `os.system()`, `os.exec*()`, `os.spawn*()`

### Import Allowlist
Only these modules can be imported (40+ total):

**Database drivers**: `pymysql`, `psycopg2`, `pymongo`, `sqlite3`, `cx_Oracle`, `pyodbc`, `cassandra`, `redis`, `elasticsearch`

**HTTP clients**: `httpx`, `requests`, `urllib`, `urllib3`

**Cloud SDKs**: `boto3`, `botocore`, `google.cloud`, `google.auth`, `azure.storage`

**Data processing**: `csv`, `json`, `pandas`, `pyarrow`, `io`, `gzip`

**Standard lib**: `os.path`, `datetime`, `hashlib`, `uuid`, `re`, `math`, `decimal`, `collections`

### Restricted Builtins
The `exec()` environment provides only safe built-in functions — no file I/O, no network access, no process management.

---

## Rate Limiting

Connector generation is rate-limited to **10 requests per minute** via slowapi. This prevents abuse of the Claude API.

---

## Connector Testing

After generation (before or after approval), test connectivity:

```
POST /api/connectors/{connector_id}/test
{
  "host": "db.example.com",
  "port": 1521,
  "user": "reader",
  "password": "secret",
  "database": "ORCL"
}
```

Returns connection test result with success/failure details.

---

## Supported Systems

The agent can generate connectors for virtually any system. Commonly requested:

### Databases
Oracle, SQL Server, PostgreSQL, MySQL, MongoDB, Cassandra, DynamoDB, CockroachDB, Redis, Elasticsearch, Neo4j, ClickHouse, Teradata, DB2, MariaDB

### SaaS/APIs
Stripe, Salesforce, HubSpot, Shopify, Jira, Zendesk, Intercom, Twilio, SendGrid, Mailchimp, QuickBooks, Notion, Airtable, Slack, GitHub

### Ad Platforms
Google Ads, Facebook Ads, LinkedIn Ads, Twitter Ads, TikTok Ads, Pinterest Ads

### Analytics
Google Analytics 4, Mixpanel, Amplitude, Segment, Snowplow

### Cloud Storage
S3, GCS, Azure Blob, SFTP, FTP

### Streaming
Kafka, Kinesis, Google Pub/Sub, RabbitMQ, Azure Event Hub

### Warehouses (Targets)
Snowflake, BigQuery, Redshift, Databricks, ClickHouse, Azure Synapse, Firebolt, DuckDB, Delta Lake, Apache Iceberg

---

## Usage

```bash
# Chat
"I need a Snowflake target connector"
"generate an Oracle source connector"

# API
POST /api/connectors/generate
{
  "connector_type": "source",
  "system_type": "oracle",
  "description": "Oracle 19c source connector with incremental extraction support"
}
```
