# Pipeline Agent - Demo Recording Guide

## Quick Start

```bash
# 1. Ensure Docker Postgres is running
docker compose up -d

# 2. Seed demo data
python demo/seed_demo_db.py

# 3. Start the app
python main.py

# 4. Run the terminal demo (interactive, step-by-step)
./demo/run_demo.sh
```

## Recording Tips

For screen recording, use **OBS Studio**, **QuickTime** (Mac), or a terminal recorder:

```bash
# Install asciinema for terminal recording
brew install asciinema
asciinema rec demo_recording.cast
./demo/run_demo.sh
# Ctrl+D to stop recording
```

## UI Demo Flow (for screen recording)

Open http://localhost:8100 in your browser. Walk through these screens:

### 1. Connectors (sidebar)
- Show 4 seed connectors: MySQL source, SQLite source, Redshift target, Postgres target
- Each shows status (active), type, and version
- Point out the "Generate New Connector" form at the top

### 2. Pipelines (sidebar) -> Create Pipeline
- Click **"+ Create Pipeline"** button
- **Step 1 - Connect:**
  - Source: select `sqlite-source-v1 (sqlite)`
  - Target: select `postgres-target-v1 (postgres)`
  - Database path: paste the full path to `demo/demo_source.db`
  - Schema: `main`
  - Click "Test Connection & Discover Tables"
- **Step 2 - Select Table:**
  - See all 3 tables discovered (customers, orders, events)
  - Click each to see profile: row count, columns, types, PKs, timestamps, sample data
  - Select `customers`
  - Click "Next: Configure Pipeline"
- **Step 3 - Configure:**
  - Refresh Type: Full Refresh
  - Load Type: Append
  - Tier: T2 - Standard
  - Schedule: `0 * * * *` (hourly)
  - Owner: `data-engineering`
  - Review the Pipeline Summary panel
  - Click **"Create Pipeline"**

### 3. Pipeline Detail
- Click on the newly created pipeline row to expand
- Show: incremental column, merge keys, version, agent reasoning
- Click **"Trigger Run"**
- Wait for "Run triggered!" alert
- Refresh the page to see the run appear with status progression:
  `pending -> extracting -> loading -> quality_gate -> promoting -> complete`

### 4. Quality (sidebar)
- Shows quality gate history for all pipelines
- Expand to see 7 check results:
  - Count Reconciliation (extracted vs loaded)
  - Schema Consistency (source vs target columns)
  - PK Uniqueness (no duplicates)
  - Null Rate Analysis (anomaly detection)
  - Volume Z-Score (deviation from baseline)
  - Sample Verification (row-level data integrity)
  - Freshness (data recency)

### 5. Lineage (sidebar)
- Shows column-level lineage: source -> target mappings
- Upstream/downstream dependencies between pipelines

### 6. Alerts (sidebar)
- Error budget exhaustion alerts
- Acknowledge functionality

### 7. Repeat for orders table
- Go back to Pipelines -> Create Pipeline
- This time select `orders` table
- Configure as **incremental** with `created_at` as watermark
- Set Tier to T1 (Critical) to show SLA tiers
- Trigger and watch it complete

## Key Talking Points

1. **Connector-agnostic**: Same platform handles SQLite, MySQL, PostgreSQL, Redshift. New connectors generated via AI.

2. **Autonomous quality gates**: 7 checks run automatically on every pipeline execution. Halt on failures, promote with warnings.

3. **Error budgets**: Rolling 7-day success rate tracking. Auto-escalation when budget exhausted.

4. **Column lineage**: Track data flow at the column level from source to target.

5. **Schema drift detection**: Automatic detection and proposals for schema changes.

6. **Tiered SLA**: T1 (critical), T2 (standard), T3 (best effort) with different alerting.

7. **Metadata enrichment**: Every row gets `_extracted_at`, `_source_schema`, `_source_table`, `_row_hash`.

## Cleanup (reset demo state)

```bash
docker exec pipeline-agent-postgres-1 psql -U pipeline_agent -d pipeline_agent -c "
  DELETE FROM runs;
  DELETE FROM gates;
  DELETE FROM column_lineage;
  DELETE FROM schema_versions;
  DELETE FROM error_budgets;
  DELETE FROM pipelines;
  DROP TABLE IF EXISTS raw.main_customers;
  DROP TABLE IF EXISTS raw.main_orders;
  DROP TABLE IF EXISTS raw.main_events;
"
```
