# Your First Pipeline

Create your first data pipeline in DAPOS — end-to-end from source to target with quality gates.

---

## Prerequisites

- DAPOS running (`python main.py`)
- Docker services up (`docker compose up -d`) for demo databases
- Logged in at http://localhost:8100 (admin / admin)

---

## Option 1: Chat (Recommended)

The fastest way is to describe what you want in natural language.

### Step 1: Describe Your Source

Type in the chat:
```
I have a MySQL database at localhost:3307 with user root and password rootpass, database ecommerce
```

The agent tests the connection and discovers available tables.

### Step 2: Select a Table

```
I want to load the orders table
```

The agent profiles the table (row count, columns, keys, watermark candidates) and proposes an ingestion strategy.

### Step 3: Review and Create

The agent proposes:
- **Refresh type**: incremental (merge on `order_id`, watermark on `updated_at`)
- **Schedule**: every 15 minutes
- **Target**: PostgreSQL (local)
- **Quality gate**: enabled with Tier 2 defaults

```
Looks good, create it
```

Pipeline created and first run triggered automatically.

---

## Option 2: API

### Create Pipeline via REST

```bash
TOKEN=$(curl -s http://localhost:8100/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | jq -r '.token')

curl -X POST http://localhost:8100/api/pipelines \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "my-first-pipeline",
    "source_connector_id": "mysql-source",
    "target_connector_id": "postgresql-target",
    "source_host": "localhost",
    "source_port": 3307,
    "source_user": "root",
    "source_password": "rootpass",
    "source_database": "ecommerce",
    "source_schema": "ecommerce",
    "source_table": "orders",
    "target_schema": "public",
    "target_table": "my_orders",
    "refresh_type": "incremental",
    "load_type": "merge",
    "merge_keys": ["order_id"],
    "watermark_column": "updated_at",
    "schedule_cron": "*/15 * * * *",
    "tier": 2
  }'
```

### Trigger First Run

```bash
PIPELINE_ID=$(curl -s http://localhost:8100/api/pipelines \
  -H "Authorization: Bearer $TOKEN" | jq -r '.[0].pipeline_id')

curl -X POST http://localhost:8100/api/pipelines/$PIPELINE_ID/trigger \
  -H "Authorization: Bearer $TOKEN"
```

---

## Option 3: CLI

```bash
# Trigger a demo pipeline
python -m cli trigger demo-ecommerce-orders

# Check run status
python -m cli runs demo-ecommerce-orders
```

---

## What Happens During a Run

```
1. Extract    → Pull rows from MySQL (incremental: only rows after last watermark)
2. Load       → Stream CSVs into PostgreSQL staging table
3. Quality    → 7 checks: count reconciliation, schema, PK uniqueness, null rates, volume, sample, freshness
4. Decision   → PROMOTE (all pass) / PROMOTE_WITH_WARNING / HALT (any fail)
5. Promote    → Merge staging into target table (upsert on merge keys)
6. Cleanup    → Drop staging table
7. Metadata   → Execute post-promotion hooks (if configured)
8. Lineage    → Record column-level lineage
9. Complete   → Run marked COMPLETE, next schedule evaluated
```

---

## Verify Results

### Check Run Status
```bash
python -m cli runs my-first-pipeline
```

### View in UI
- **Activity tab**: execution timeline with 13 steps
- **Quality tab**: gate results per check
- **Freshness tab**: staleness monitoring
- **Lineage tab**: source→target column mapping

### Query Target Table
```sql
SELECT * FROM public.my_orders LIMIT 10;
```

---

## Next Steps

- [Configure quality thresholds](concepts/quality-gate.md)
- [Set up alerts](concepts/observability.md)
- [Add post-promotion hooks](advanced/hooks.md)
- [Create data contracts](concepts/data-contracts.md) between pipelines
- [Design multi-pipeline topologies](agent/topology.md)
