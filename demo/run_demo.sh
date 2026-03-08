#!/usr/bin/env bash
# =============================================================================
# Pipeline Agent -- Interactive Demo Script
# =============================================================================
# This script walks through the full pipeline lifecycle:
#   1. Health check & connectors
#   2. Test source connection (SQLite demo DB)
#   3. Discover & profile tables
#   4. Create a pipeline (customers: SQLite -> PostgreSQL)
#   5. Trigger a run & watch it complete
#   6. Verify data landed in Postgres
#   7. Check quality gate results
#   8. View column lineage
#   9. Check error budget
#  10. Create a second pipeline (orders) and trigger it
#
# Usage:
#   chmod +x demo/run_demo.sh
#   ./demo/run_demo.sh
#
# Prerequisites:
#   - App running on http://localhost:8100
#   - Docker Postgres running
#   - Demo SQLite DB seeded (python demo/seed_demo_db.py)
# =============================================================================

set -e

BASE="http://localhost:8100"
BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
YELLOW="\033[33m"
DIM="\033[2m"
RESET="\033[0m"

# Auto-detect connector IDs
SQLITE_ID=""
POSTGRES_ID=""

step=0
pause() {
  echo ""
  echo -e "${DIM}  Press Enter to continue...${RESET}"
  read -r
}

banner() {
  step=$((step + 1))
  echo ""
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}  Step ${step}: $1${RESET}"
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo ""
}

run() {
  echo -e "${YELLOW}  \$ $1${RESET}"
  echo ""
  eval "$1" 2>&1 | sed 's/^/    /'
  echo ""
}

# =============================================================================
echo ""
echo -e "${BOLD}${CYAN}"
echo "  ╔═══════════════════════════════════════════════════════════╗"
echo "  ║              PIPELINE AGENT  --  Live Demo               ║"
echo "  ║                                                          ║"
echo "  ║   AI-powered data pipeline platform with autonomous      ║"
echo "  ║   extraction, 7-check quality gates, error budgets,      ║"
echo "  ║   column lineage, and schema drift detection.            ║"
echo "  ╚═══════════════════════════════════════════════════════════╝"
echo -e "${RESET}"
pause

# =============================================================================
banner "Health Check"
echo "  Verify the app is running and PostgreSQL is connected."
echo ""
run "curl -s ${BASE}/health | python3 -m json.tool"
pause

# =============================================================================
banner "List Available Connectors"
echo "  The platform ships with seed connectors for MySQL, SQLite, Redshift, and PostgreSQL."
echo "  Engineers can also generate new connectors via the agent."
echo ""
run "curl -s ${BASE}/api/connectors | python3 -m json.tool"

# Capture connector IDs
SQLITE_ID=$(curl -s ${BASE}/api/connectors | python3 -c "
import sys, json
for c in json.load(sys.stdin):
    if c['source_target_type'] == 'sqlite' and c['connector_type'] == 'source':
        print(c['connector_id']); break
")
POSTGRES_ID=$(curl -s ${BASE}/api/connectors | python3 -c "
import sys, json
for c in json.load(sys.stdin):
    if c['source_target_type'] == 'postgres' and c['connector_type'] == 'target':
        print(c['connector_id']); break
")
echo -e "  ${GREEN}SQLite Source ID:  ${SQLITE_ID}${RESET}"
echo -e "  ${GREEN}Postgres Target ID: ${POSTGRES_ID}${RESET}"
pause

# =============================================================================
banner "Test Source Connection (SQLite)"
echo "  Connect to our demo SQLite database with 3 tables: customers, orders, events."
echo ""
DEMO_DB="$(cd "$(dirname "$0")" && pwd)/demo_source.db"
run "curl -s -X POST ${BASE}/api/connection/test-source \\
  -H 'Content-Type: application/json' \\
  -d '{\"connector_id\": \"${SQLITE_ID}\", \"params\": {\"database\": \"${DEMO_DB}\"}}' | python3 -m json.tool"
pause

# =============================================================================
banner "Discover & Profile Tables"
echo "  Profile all tables in the 'main' schema -- row counts, columns, PKs, timestamps, sample data."
echo ""
run "curl -s -X POST ${BASE}/api/discovery/profile \\
  -H 'Content-Type: application/json' \\
  -d '{\"connector_id\": \"${SQLITE_ID}\", \"params\": {\"database\": \"${DEMO_DB}\"}, \"schema_name\": \"main\"}' \\
  | python3 -c \"
import sys, json
data = json.load(sys.stdin)
for t in data:
    print(f\\\"  Table: {t['table_name']}\\\")
    print(f\\\"    Rows: {t.get('row_count_estimate', '?')}, Columns: {t.get('column_count', '?')}\\\")
    print(f\\\"    PKs: {t.get('primary_keys', [])}\\\")
    print(f\\\"    Timestamps: {t.get('timestamp_columns', [])}\\\")
    cols = t.get('columns', [])
    for c in cols:
        name = c.get('source_column', c.get('column_name', ''))
        dtype = c.get('source_type', c.get('data_type', ''))
        print(f\\\"      - {name} ({dtype})\\\")
    print()
\""
pause

# =============================================================================
banner "Create Pipeline: customers (SQLite -> PostgreSQL)"
echo "  Create a full-refresh pipeline for the customers table."
echo "  Strategy: full refresh, append load, hourly schedule, Tier 2 SLA."
echo ""
RESULT=$(curl -s -X POST ${BASE}/api/pipelines \
  -H 'Content-Type: application/json' \
  -d "{
    \"source_connector_id\": \"${SQLITE_ID}\",
    \"target_connector_id\": \"${POSTGRES_ID}\",
    \"source_host\": \"\",
    \"source_port\": 0,
    \"source_database\": \"${DEMO_DB}\",
    \"source_schema\": \"main\",
    \"source_table\": \"customers\",
    \"target_host\": \"localhost\",
    \"target_port\": 5432,
    \"target_database\": \"pipeline_agent\",
    \"target_user\": \"pipeline_agent\",
    \"target_password\": \"pipeline_agent\",
    \"target_schema\": \"raw\",
    \"schedule_cron\": \"0 * * * *\",
    \"tier\": 2,
    \"owner\": \"data-engineering\",
    \"strategy\": {
      \"refresh_type\": \"full\",
      \"load_type\": \"append\",
      \"replication_method\": \"snapshot\",
      \"column_mappings\": [
        {\"source_column\": \"customer_id\", \"source_type\": \"integer\", \"target_column\": \"customer_id\", \"target_type\": \"INTEGER\"},
        {\"source_column\": \"name\", \"source_type\": \"text\", \"target_column\": \"name\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"email\", \"source_type\": \"text\", \"target_column\": \"email\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"plan\", \"source_type\": \"text\", \"target_column\": \"plan\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"created_at\", \"source_type\": \"text\", \"target_column\": \"created_at\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"updated_at\", \"source_type\": \"text\", \"target_column\": \"updated_at\", \"target_type\": \"TEXT\"}
      ]
    }
  }")
echo "$RESULT" | python3 -m json.tool | sed 's/^/    /'
PIPELINE_ID=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['pipeline_id'])")
echo ""
echo -e "  ${GREEN}Pipeline created: ${PIPELINE_ID}${RESET}"
pause

# =============================================================================
banner "Trigger Pipeline Run"
echo "  Manually trigger the pipeline. The autonomous engine will:"
echo "    1. Extract data from SQLite"
echo "    2. Load to PostgreSQL staging table"
echo "    3. Run 7-check quality gate"
echo "    4. Promote to target table (if gate passes)"
echo "    5. Update error budget & column lineage"
echo ""
run "curl -s -X POST ${BASE}/api/pipelines/${PIPELINE_ID}/trigger | python3 -m json.tool"

echo "  Waiting for run to complete..."
for i in $(seq 1 15); do
  sleep 2
  STATUS=$(curl -s "${BASE}/api/pipelines/${PIPELINE_ID}/runs?limit=1" | python3 -c "
import sys, json
runs = json.load(sys.stdin)
if runs: print(runs[0]['status'])
else: print('waiting')
")
  echo -e "    ${DIM}(${i}s) Status: ${STATUS}${RESET}"
  if [ "$STATUS" = "complete" ] || [ "$STATUS" = "halted" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
done

echo ""
run "curl -s '${BASE}/api/pipelines/${PIPELINE_ID}/runs?limit=1' | python3 -m json.tool"
pause

# =============================================================================
banner "Verify Data in PostgreSQL"
echo "  Check that customer data landed in raw.main_customers with metadata columns."
echo ""
run "docker exec pipeline-agent-postgres-1 psql -U pipeline_agent -d pipeline_agent -c \\
  'SELECT customer_id, name, email, plan, _extracted_at::text FROM raw.main_customers;'"
pause

# =============================================================================
banner "Quality Gate Results"
echo "  The quality gate ran 7 checks: count reconciliation, schema consistency,"
echo "  PK uniqueness, null rate analysis, volume z-score, sample verification, freshness."
echo ""
run "curl -s '${BASE}/api/quality/${PIPELINE_ID}' | python3 -c \"
import sys, json
data = json.load(sys.stdin)
summary = data.get('summary', {})
print(f\\\"  Total gate evaluations: {summary.get('total_runs', 0)}\\\")
print(f\\\"  Pass rate: {summary.get('pass_rate', 0)*100:.0f}%\\\")
print()
for gate in data.get('gates', [])[:1]:
    print(f\\\"  Decision: {gate['decision'].upper()}\\\")
    for c in gate.get('checks', []):
        status_icon = {'pass': '+', 'warn': '~', 'fail': 'x'}[c['status']]
        print(f\\\"    [{status_icon}] {c['name']}: {c['status']}\\\")
        if c.get('detail'):
            detail = c['detail'][:120]
            print(f\\\"        {detail}\\\")
\""
pause

# =============================================================================
banner "Column Lineage"
echo "  Track data flow from source columns to target columns."
echo ""
run "curl -s '${BASE}/api/lineage/${PIPELINE_ID}' | python3 -c \"
import sys, json
data = json.load(sys.stdin)
print('  Column Lineage:')
for l in data.get('lineage', []):
    src = f\\\"{l['source_schema']}.{l['source_table']}.{l['source_column']}\\\"
    tgt = f\\\"{l['target_schema']}.{l['target_table']}.{l['target_column']}\\\"
    print(f\\\"    {src}  -->  {tgt}\\\")
\""
pause

# =============================================================================
banner "Error Budget"
echo "  Rolling 7-day error budget tracking. Alerts when budget is exhausted."
echo ""
run "curl -s '${BASE}/api/error-budgets/${PIPELINE_ID}' | python3 -m json.tool"
pause

# =============================================================================
banner "Create Second Pipeline: orders (Incremental)"
echo "  Create an incremental pipeline for the orders table using created_at as watermark."
echo ""
RESULT2=$(curl -s -X POST ${BASE}/api/pipelines \
  -H 'Content-Type: application/json' \
  -d "{
    \"source_connector_id\": \"${SQLITE_ID}\",
    \"target_connector_id\": \"${POSTGRES_ID}\",
    \"source_host\": \"\",
    \"source_port\": 0,
    \"source_database\": \"${DEMO_DB}\",
    \"source_schema\": \"main\",
    \"source_table\": \"orders\",
    \"target_host\": \"localhost\",
    \"target_port\": 5432,
    \"target_database\": \"pipeline_agent\",
    \"target_user\": \"pipeline_agent\",
    \"target_password\": \"pipeline_agent\",
    \"target_schema\": \"raw\",
    \"schedule_cron\": \"*/30 * * * *\",
    \"tier\": 1,
    \"owner\": \"data-engineering\",
    \"strategy\": {
      \"refresh_type\": \"incremental\",
      \"load_type\": \"append\",
      \"replication_method\": \"watermark\",
      \"incremental_column\": \"created_at\",
      \"merge_keys\": [\"order_id\"],
      \"column_mappings\": [
        {\"source_column\": \"order_id\", \"source_type\": \"integer\", \"target_column\": \"order_id\", \"target_type\": \"INTEGER\"},
        {\"source_column\": \"customer_id\", \"source_type\": \"integer\", \"target_column\": \"customer_id\", \"target_type\": \"INTEGER\"},
        {\"source_column\": \"product\", \"source_type\": \"text\", \"target_column\": \"product\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"amount\", \"source_type\": \"real\", \"target_column\": \"amount\", \"target_type\": \"NUMERIC(10,2)\"},
        {\"source_column\": \"status\", \"source_type\": \"text\", \"target_column\": \"status\", \"target_type\": \"TEXT\"},
        {\"source_column\": \"created_at\", \"source_type\": \"text\", \"target_column\": \"created_at\", \"target_type\": \"TEXT\"}
      ]
    }
  }")
echo "$RESULT2" | python3 -m json.tool | sed 's/^/    /'
PIPELINE2_ID=$(echo "$RESULT2" | python3 -c "import sys,json; print(json.load(sys.stdin)['pipeline_id'])")
echo ""
echo -e "  ${GREEN}Orders pipeline created: ${PIPELINE2_ID}${RESET}"
echo ""

echo "  Triggering run..."
curl -s -X POST ${BASE}/api/pipelines/${PIPELINE2_ID}/trigger > /dev/null

echo "  Waiting for completion..."
for i in $(seq 1 15); do
  sleep 2
  STATUS=$(curl -s "${BASE}/api/pipelines/${PIPELINE2_ID}/runs?limit=1" | python3 -c "
import sys, json
runs = json.load(sys.stdin)
if runs: print(runs[0]['status'])
else: print('waiting')
")
  echo -e "    ${DIM}(${i}s) Status: ${STATUS}${RESET}"
  if [ "$STATUS" = "complete" ] || [ "$STATUS" = "halted" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
done

echo ""
run "docker exec pipeline-agent-postgres-1 psql -U pipeline_agent -d pipeline_agent -c \\
  'SELECT order_id, customer_id, product, amount, status FROM raw.main_orders;'"
pause

# =============================================================================
banner "List All Pipelines"
echo "  Both pipelines are now active and scheduled."
echo ""
run "curl -s '${BASE}/api/pipelines' | python3 -c \"
import sys, json
for p in json.load(sys.stdin):
    print(f\\\"  {p['pipeline_name']:20s}  status={p['status']:8s}  tier=T{p['tier']}  refresh={p['refresh_type']:12s}  schedule={p['schedule_cron']}\\\")
\""
pause

# =============================================================================
banner "Open the UI"
echo "  The web UI is available at: http://localhost:8100"
echo ""
echo "  Views available:"
echo "    - Pipelines: see both pipelines, expand for runs, trigger, pause/resume"
echo "    - Quality: see gate check history and pass rates"
echo "    - Lineage: visualize column-level data flow"
echo "    - Connectors: manage source/target connectors"
echo "    - Alerts: error budget alerts and acknowledgment"
echo "    - Freshness: data freshness monitoring"
echo ""
echo -e "  ${GREEN}Opening browser...${RESET}"
open "http://localhost:8100" 2>/dev/null || echo "  Open http://localhost:8100 in your browser"
echo ""

echo -e "${BOLD}${CYAN}"
echo "  ╔═══════════════════════════════════════════════════════════╗"
echo "  ║                    Demo Complete!                         ║"
echo "  ║                                                          ║"
echo "  ║   What we demonstrated:                                  ║"
echo "  ║   - Connector-agnostic architecture (SQLite -> Postgres) ║"
echo "  ║   - Table profiling & schema discovery                   ║"
echo "  ║   - Full refresh & incremental pipelines                 ║"
echo "  ║   - 7-check autonomous quality gate                      ║"
echo "  ║   - Column-level lineage tracking                        ║"
echo "  ║   - Error budget monitoring                              ║"
echo "  ║   - Metadata enrichment (_extracted_at, _row_hash, etc.) ║"
echo "  ╚═══════════════════════════════════════════════════════════╝"
echo -e "${RESET}"
