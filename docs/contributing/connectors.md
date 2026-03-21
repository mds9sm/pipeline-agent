# Adding Connectors

How to add a new source or target connector to DAPOS.

---

## Two Approaches

### 1. AI-Generated (Recommended)

Ask the agent to generate a connector:

```
"I need an Oracle source connector"
"Generate a Snowflake target connector"
```

The agent writes a complete implementation, validates it, and creates an approval proposal. Fastest path for most systems.

### 2. Seed Connector (Built-in)

For connectors that should ship with DAPOS, add them to `connectors/seeds.py`:

```python
SEEDS = {
    "my-source": {
        "connector_name": "my-source",
        "connector_type": "source",
        "description": "My Source description",
        "code": '''
import httpx
from source.base import SourceEngine, ConnectionResult, ExtractResult, SchemaInfo, TableProfile

class MySourceEngine(SourceEngine):
    INTERFACE_VERSION = "1.0"

    def __init__(self, config: dict):
        self.config = config

    async def test_connection(self) -> ConnectionResult:
        # Test connectivity
        ...

    async def list_schemas(self) -> list[SchemaInfo]:
        # Discover available schemas
        ...

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        # Profile table structure
        ...

    async def extract(self, contract, run, staging_dir: str) -> ExtractResult:
        # Extract data, write CSVs
        ...

    def map_type(self, source_type: str) -> str:
        # Map source types to standard types
        ...

    def get_source_type(self) -> str:
        return "my-source"
''',
    },
}
```

Seeds auto-install on first startup and auto-update when code changes.

---

## Source Interface

Implement `SourceEngine` from `source/base.py`:

| Method | Required | Description |
|--------|----------|-------------|
| `test_connection()` | Yes | Return `ConnectionResult(success=True/False, message=...)` |
| `list_schemas()` | Yes | Return list of `SchemaInfo(name, tables=[...])` |
| `profile_table(schema, table)` | Yes | Return `TableProfile` with row count, columns, keys |
| `extract(contract, run, staging_dir)` | Yes | Write CSVs with metadata columns, return `ExtractResult` |
| `map_type(source_type)` | Yes | Map native types to standard types |
| `get_source_type()` | Yes | Return string identifier |

### Metadata Columns

Every source **must** add these 4 columns to extracted data:

```python
row["_extracted_at"] = datetime.utcnow().isoformat()
row["_source_schema"] = schema
row["_source_table"] = table
row["_row_hash"] = hashlib.sha256("|".join(str(v) for v in row.values()).encode()).hexdigest()
```

### Incremental Extraction

For incremental support, use the watermark column:

```python
watermark_col = contract.watermark_column
watermark_val = run.watermark_value  # from last successful run

query = f"SELECT * FROM {table} WHERE {watermark_col} > %s"
# ... extract rows after watermark
```

Return the new watermark value in `ExtractResult.watermark_value`.

---

## Target Interface

Implement `TargetEngine` from `target/base.py`:

| Method | Required | Description |
|--------|----------|-------------|
| `test_connection()` | Yes | Verify connectivity |
| `generate_ddl(contract)` | Yes | CREATE TABLE statement |
| `create_table_if_not_exists(contract)` | Yes | Ensure target table exists |
| `load_staging(contract, run)` | Yes | Stream CSVs into staging table |
| `promote(contract, run)` | Yes | Merge or append staging → target |
| `drop_staging(contract, run)` | Yes | Clean up staging table |
| `execute_sql(sql)` | Yes | For post-promotion hooks |
| `get_row_count(schema, table)` | Yes | Quality gate: count rows |
| `get_column_types(schema, table)` | Yes | Quality gate: column types |
| `check_duplicates(schema, table, keys)` | Yes | Quality gate: duplicate check |
| `get_null_rates(schema, table)` | Yes | Quality gate: null rate per column |
| `get_cardinality(schema, table, column)` | Yes | Quality gate: distinct values |
| `get_max_value(schema, table, column)` | Yes | Quality gate: max watermark |
| `staging_name(contract, run)` | Yes | Return (schema, table) for staging |
| `get_target_type()` | Yes | Return string identifier |

---

## Allowed Imports

Connector code can only import from the allowlist (40+ modules):

**Database**: `pymysql`, `psycopg2`, `pymongo`, `sqlite3`, `cx_Oracle`, `pyodbc`, `cassandra`, `redis`, `elasticsearch`

**HTTP**: `httpx`, `requests`, `urllib`

**Cloud**: `boto3`, `google.cloud`, `azure.storage`

**Data**: `csv`, `json`, `pandas`, `pyarrow`, `io`, `gzip`

**Standard**: `os.path`, `datetime`, `hashlib`, `uuid`, `re`, `math`, `decimal`

---

## Testing a Connector

```bash
# Test via API
POST /api/connectors/{connector_id}/test
{
  "host": "...",
  "port": 5432,
  "user": "...",
  "password": "...",
  "database": "..."
}

# Test via CLI
python -m cli connectors test {connector_id}
```

---

## Adding Tests

Add entries to `test-pipeline-agent.sh`:

```bash
# In the SOURCES array (for source connector tests)
"My Source|Can you create a connector for My Source database"

# In the TARGETS array (for target connector tests)
"My Target|I need a My Target warehouse connector"

# In the PIPELINES array (for end-to-end pipeline tests)
"My Pipeline|my_source|my_target|I have a My Source database at host:port|I want to load the users table to My Target"
```
