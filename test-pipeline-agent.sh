#!/bin/bash
#
# Comprehensive Pipeline Agent Test Suite
# Tests the actual running app via curl APIs
# Covers all major source/destination combinations
#
# Usage:
#   ./test-pipeline-agent.sh              # Run all tests
#   ./test-pipeline-agent.sh --sources    # Sources only
#   ./test-pipeline-agent.sh --targets    # Targets only
#   ./test-pipeline-agent.sh --chat       # Chat/conversation tests only
#   ./test-pipeline-agent.sh --api        # REST API endpoint tests only
#

set -o pipefail

# ============================================================================
# Configuration
# ============================================================================
API_URL="${API_URL:-http://localhost:8100}"
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0
SKIP_COUNT=0
TOTAL_COUNT=0
TEST_MODE="${1:-all}"

# Timing
START_TIME=$(date +%s)

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ============================================================================
# Helpers
# ============================================================================
section() {
    echo ""
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${CYAN}║  $1$(printf '%*s' $((58 - ${#1})) '')║${NC}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
}

test_name() {
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    echo -e "\n${BLUE}[$TOTAL_COUNT] $1${NC}"
}

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo -e "  ${GREEN}PASS${NC} $1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo -e "  ${RED}FAIL${NC} $1"
}

warn() {
    WARN_COUNT=$((WARN_COUNT + 1))
    echo -e "  ${YELLOW}WARN${NC} $1"
}

skip() {
    SKIP_COUNT=$((SKIP_COUNT + 1))
    echo -e "  ${YELLOW}SKIP${NC} $1"
}

info() {
    echo -e "  ${NC}     $1"
}

# Send a chat command and return the response body
chat() {
    local text="$1"
    local session="${2:-default_session}"
    curl -s -m 60 -X POST "$API_URL/api/command" \
        -H 'Content-Type: application/json' \
        -d "{\"text\": \"$text\", \"session_id\": \"$session\"}" 2>/dev/null
}

# Send a chat command and extract the response text
chat_text() {
    local text="$1"
    local session="${2:-default_session}"
    chat "$text" "$session" | python3 -c "import sys,json; print(json.load(sys.stdin).get('response',''))" 2>/dev/null
}

# HTTP GET and return body + code
api_get() {
    curl -s -m 30 -w "\n%{http_code}" "$API_URL$1" 2>/dev/null
}

# HTTP POST with JSON
api_post() {
    curl -s -m 60 -w "\n%{http_code}" -X POST "$API_URL$1" \
        -H 'Content-Type: application/json' \
        -d "$2" 2>/dev/null
}

# Check if response contains a keyword (case insensitive)
contains() {
    echo "$1" | grep -qi "$2"
}

# ============================================================================
# Preflight: Is the app running?
# ============================================================================
echo -e "${BOLD}Pipeline Agent Comprehensive Test Suite${NC}"
echo -e "Target: $API_URL"
echo -e "Mode:   $TEST_MODE"
echo ""

HEALTH=$(curl -s -m 10 "$API_URL/health" 2>/dev/null)
if [ -z "$HEALTH" ]; then
    echo -e "${RED}ERROR: App not reachable at $API_URL${NC}"
    echo "Start it with: ANTHROPIC_API_KEY=sk-... python main.py"
    exit 1
fi
echo -e "${GREEN}App is running:${NC} $HEALTH"

# ============================================================================
# SECTION 1: Core API Endpoints
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--api" ]; then

section "CORE API ENDPOINTS"

# --- Health ---
test_name "GET /health"
RESP=$(api_get "/health")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ] && contains "$BODY" "ok"; then
    pass "Health endpoint returns 200 with status ok"
else
    fail "Health endpoint returned HTTP $CODE"
fi

# --- Metrics ---
test_name "GET /metrics (Prometheus)"
RESP=$(api_get "/metrics")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    pass "Metrics endpoint returns 200"
else
    fail "Metrics endpoint returned HTTP $CODE"
fi

# --- List Connectors ---
test_name "GET /api/connectors"
RESP=$(api_get "/api/connectors")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('connectors',[])))" 2>/dev/null)
    pass "Connectors endpoint returns 200 ($COUNT connectors)"
else
    fail "Connectors endpoint returned HTTP $CODE"
fi

# --- List Pipelines ---
test_name "GET /api/pipelines"
RESP=$(api_get "/api/pipelines")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('pipelines',[])))" 2>/dev/null)
    pass "Pipelines endpoint returns 200 ($COUNT pipelines)"
else
    fail "Pipelines endpoint returned HTTP $CODE"
fi

# --- List Approvals ---
test_name "GET /api/approvals"
RESP=$(api_get "/api/approvals")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Approvals endpoint returns 200"
else
    fail "Approvals endpoint returned HTTP $CODE"
fi

# --- Observability Freshness ---
test_name "GET /api/observability/freshness"
RESP=$(api_get "/api/observability/freshness")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Freshness endpoint returns 200"
else
    fail "Freshness endpoint returned HTTP $CODE"
fi

# --- Observability Alerts ---
test_name "GET /api/observability/alerts"
RESP=$(api_get "/api/observability/alerts")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Alerts endpoint returns 200"
else
    fail "Alerts endpoint returned HTTP $CODE"
fi

# --- Agent Costs ---
test_name "GET /api/agent-costs"
RESP=$(api_get "/api/agent-costs")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Agent costs endpoint returns 200"
else
    fail "Agent costs endpoint returned HTTP $CODE"
fi

# --- Agent Costs Summary ---
test_name "GET /api/agent-costs/summary"
RESP=$(api_get "/api/agent-costs/summary")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Agent costs summary endpoint returns 200"
else
    fail "Agent costs summary endpoint returned HTTP $CODE"
fi

# --- Policies ---
test_name "GET /api/policies"
RESP=$(api_get "/api/policies")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Policies endpoint returns 200"
else
    fail "Policies endpoint returned HTTP $CODE"
fi

# --- Preferences ---
test_name "GET /api/preferences"
RESP=$(api_get "/api/preferences")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Preferences endpoint returns 200"
else
    fail "Preferences endpoint returned HTTP $CODE"
fi

# --- Web UI ---
test_name "GET / (Web UI)"
RESP=$(api_get "/")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ] && contains "$BODY" "html"; then
    pass "Web UI returns HTML"
else
    fail "Web UI returned HTTP $CODE"
fi

fi # --api

# ============================================================================
# SECTION 2: Chat - Source Connector Requests
# Comprehensive list of data sources
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--sources" ]; then

section "CHAT: SOURCE CONNECTOR REQUESTS"

# --- Databases ---
SOURCES=(
    "Oracle|I need to connect to an Oracle database as a source"
    "SQL Server|I want to ingest data from Microsoft SQL Server"
    "MySQL|Set up MySQL as a data source"
    "PostgreSQL|I need to pull data from a PostgreSQL database"
    "MongoDB|Connect to MongoDB and extract collections"
    "MariaDB|I want to use MariaDB as a source"
    "Cassandra|Set up Apache Cassandra as a data source"
    "DynamoDB|I need to extract data from AWS DynamoDB"
    "CockroachDB|Connect to CockroachDB as a source"
    "Redis|I want to pull data from Redis"
    "Elasticsearch|Set up Elasticsearch as a data source"
    "Neo4j|I need to extract data from Neo4j graph database"
    "ClickHouse|Connect to ClickHouse as a source"
    "SQLite|I need to ingest data from a SQLite database"
    "Teradata|Set up Teradata as a data source"
    "DB2|I need to connect to IBM DB2 as a source"
)

# --- SaaS / APIs ---
SAAS_SOURCES=(
    "Stripe|I need to pull payment data from Stripe API"
    "Google Ads|Set up Google Ads as a data source for campaign metrics"
    "Facebook Insights|I want to ingest Facebook Insights analytics data"
    "Facebook Ads|Connect to Facebook Ads API for ad performance data"
    "Salesforce|I need to extract CRM data from Salesforce"
    "HubSpot|Set up HubSpot as a source for marketing data"
    "Shopify|Pull order and product data from Shopify"
    "Google Analytics|I want to ingest data from Google Analytics 4"
    "Jira|Connect to Jira for project management data"
    "Zendesk|Set up Zendesk as a source for support ticket data"
    "Intercom|I need to pull customer data from Intercom"
    "Twilio|Extract call and SMS data from Twilio"
    "SendGrid|Set up SendGrid as a source for email analytics"
    "Mailchimp|Pull email campaign data from Mailchimp"
    "QuickBooks|Connect to QuickBooks for accounting data"
    "Xero|I want to extract financial data from Xero"
    "Notion|Pull data from Notion databases"
    "Airtable|Set up Airtable as a data source"
    "Slack|I need to extract messages and analytics from Slack"
    "GitHub|Pull repository and PR data from GitHub API"
    "LinkedIn Ads|Set up LinkedIn Ads as a data source"
    "Twitter Ads|I want to ingest Twitter/X Ads performance data"
    "TikTok Ads|Connect to TikTok Ads API for campaign data"
    "Pinterest Ads|Set up Pinterest Ads as a data source"
    "Marketo|I need to extract marketing data from Marketo"
    "Braze|Connect to Braze for customer engagement data"
    "Segment|Pull event data from Segment"
    "Mixpanel|I want to extract analytics data from Mixpanel"
    "Amplitude|Set up Amplitude as a data source"
    "Snowplow|Pull event tracking data from Snowplow"
)

# --- File / Cloud Storage ---
FILE_SOURCES=(
    "S3|I need to ingest CSV/Parquet files from AWS S3"
    "GCS|Set up Google Cloud Storage as a file source"
    "Azure Blob|Pull data from Azure Blob Storage"
    "SFTP|I need to ingest files from an SFTP server"
    "FTP|Set up an FTP server as a data source"
)

# --- Streaming ---
STREAMING_SOURCES=(
    "Kafka|I need to consume events from Apache Kafka"
    "Kinesis|Set up AWS Kinesis as a streaming source"
    "Pub/Sub|I want to ingest data from Google Cloud Pub/Sub"
    "RabbitMQ|Connect to RabbitMQ as a message source"
    "EventHub|Pull events from Azure Event Hubs"
)

# Run database source tests
for entry in "${SOURCES[@]}"; do
    IFS='|' read -r name prompt <<< "$entry"
    test_name "Source: $name"
    SESSION="src_${name// /_}_$$"
    RESP=$(chat_text "$prompt" "$SESSION")
    if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
        if contains "$RESP" "$name" || contains "$RESP" "connect" || contains "$RESP" "source" || contains "$RESP" "credential" || contains "$RESP" "host" || contains "$RESP" "database"; then
            pass "Agent acknowledged $name source request (${#RESP} chars)"
            info "${RESP:0:120}..."
        else
            warn "Agent responded but didn't clearly reference $name"
            info "${RESP:0:120}..."
        fi
    else
        fail "No meaningful response for $name source"
        info "Response: $RESP"
    fi
done

# Run SaaS source tests
section "CHAT: SaaS/API SOURCE REQUESTS"
for entry in "${SAAS_SOURCES[@]}"; do
    IFS='|' read -r name prompt <<< "$entry"
    test_name "Source: $name"
    SESSION="saas_${name// /_}_$$"
    RESP=$(chat_text "$prompt" "$SESSION")
    if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
        if contains "$RESP" "$name" || contains "$RESP" "api" || contains "$RESP" "connect" || contains "$RESP" "key" || contains "$RESP" "token" || contains "$RESP" "credential"; then
            pass "Agent acknowledged $name source request (${#RESP} chars)"
            info "${RESP:0:120}..."
        else
            warn "Agent responded but didn't clearly reference $name"
            info "${RESP:0:120}..."
        fi
    else
        fail "No meaningful response for $name source"
        info "Response: $RESP"
    fi
done

# Run file/cloud storage tests
section "CHAT: FILE/CLOUD STORAGE SOURCES"
for entry in "${FILE_SOURCES[@]}"; do
    IFS='|' read -r name prompt <<< "$entry"
    test_name "Source: $name"
    SESSION="file_${name// /_}_$$"
    RESP=$(chat_text "$prompt" "$SESSION")
    if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
        pass "Agent acknowledged $name source request (${#RESP} chars)"
        info "${RESP:0:120}..."
    else
        fail "No meaningful response for $name source"
    fi
done

# Run streaming source tests
section "CHAT: STREAMING SOURCES"
for entry in "${STREAMING_SOURCES[@]}"; do
    IFS='|' read -r name prompt <<< "$entry"
    test_name "Source: $name"
    SESSION="stream_${name// /_}_$$"
    RESP=$(chat_text "$prompt" "$SESSION")
    if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
        pass "Agent acknowledged $name source request (${#RESP} chars)"
        info "${RESP:0:120}..."
    else
        fail "No meaningful response for $name source"
    fi
done

fi # --sources

# ============================================================================
# SECTION 3: Chat - Target/Destination Requests
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--targets" ]; then

section "CHAT: TARGET/DESTINATION REQUESTS"

TARGETS=(
    "PostgreSQL|I want to load data into PostgreSQL as a target"
    "Snowflake|Set up Snowflake as the data warehouse destination"
    "BigQuery|I want to load data into Google BigQuery"
    "Redshift|Set up Amazon Redshift as the target warehouse"
    "Databricks|I want to load data into Databricks lakehouse"
    "ClickHouse|Set up ClickHouse as an analytics destination"
    "MySQL|Load transformed data into MySQL target"
    "SQL Server|Set up SQL Server as a data destination"
    "Oracle|I want to load data into Oracle as a target"
    "S3 Parquet|Write data as Parquet files to S3"
    "GCS|Load data into Google Cloud Storage as Parquet"
    "Azure Synapse|Set up Azure Synapse as the target"
    "Firebolt|I want to load data into Firebolt for analytics"
    "DuckDB|Set up DuckDB as a local analytics target"
    "Delta Lake|Write data to Delta Lake format"
    "Apache Iceberg|Set up Apache Iceberg tables as target"
    "Elasticsearch|Load data into Elasticsearch for search"
    "MongoDB|Write data to MongoDB as a target"
)

for entry in "${TARGETS[@]}"; do
    IFS='|' read -r name prompt <<< "$entry"
    test_name "Target: $name"
    SESSION="tgt_${name// /_}_$$"
    RESP=$(chat_text "$prompt" "$SESSION")
    if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
        if contains "$RESP" "$name" || contains "$RESP" "target" || contains "$RESP" "load" || contains "$RESP" "destination" || contains "$RESP" "warehouse" || contains "$RESP" "connect"; then
            pass "Agent acknowledged $name target request (${#RESP} chars)"
            info "${RESP:0:120}..."
        else
            warn "Agent responded but didn't clearly reference $name"
            info "${RESP:0:120}..."
        fi
    else
        fail "No meaningful response for $name target"
        info "Response: $RESP"
    fi
done

fi # --targets

# ============================================================================
# SECTION 4: Chat - Source→Target Pipeline Conversations (Multi-turn)
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--chat" ]; then

section "CHAT: SOURCE→TARGET PIPELINE CONVERSATIONS"

# Each test: multi-turn conversation asking to set up a pipeline between source→target
PIPELINES=(
    "Oracle→Snowflake|oracle|snowflake|I want to ingest data from Oracle to Snowflake|The Oracle DB is at oracle-prod.company.com port 1521 with SID ORCL, schema HR table employees"
    "SQL Server→BigQuery|sql server|bigquery|Set up a pipeline from SQL Server to BigQuery|The SQL Server is at sqlserver.internal:1433 database sales schema dbo table orders"
    "MySQL→PostgreSQL|mysql|postgres|Configure ingestion from MySQL to PostgreSQL|Source is mysql-prod:3306 database ecommerce table customers"
    "Stripe→Snowflake|stripe|snowflake|I need to get Stripe payments into Snowflake|We want charges, customers, and subscriptions objects"
    "Google Ads→BigQuery|google ads|bigquery|Load Google Ads campaign data into BigQuery|I need daily campaign performance metrics"
    "Facebook Insights→Redshift|facebook|redshift|Ingest Facebook Insights data into Redshift|I want page insights and post-level engagement metrics"
    "Salesforce→Databricks|salesforce|databricks|Set up Salesforce to Databricks pipeline|We need Account, Contact, and Opportunity objects"
    "MongoDB→PostgreSQL|mongo|postgres|Migrate MongoDB collections to PostgreSQL|Source is mongodb-cluster:27017 database analytics collection events"
    "HubSpot→Snowflake|hubspot|snowflake|Ingest HubSpot CRM data into Snowflake|We need contacts, companies, and deals"
    "Shopify→BigQuery|shopify|bigquery|Set up Shopify data ingestion to BigQuery|We need orders, products, and customers"
    "Kafka→ClickHouse|kafka|clickhouse|Stream Kafka events into ClickHouse|Topic is user-events from the analytics Kafka cluster"
    "S3→Redshift|s3|redshift|Load CSV files from S3 into Redshift|Files are in s3://data-lake/raw/transactions/ in Parquet format"
    "Jira→PostgreSQL|jira|postgres|Ingest Jira project data into PostgreSQL|We need issues, sprints, and worklogs"
    "Zendesk→Snowflake|zendesk|snowflake|Set up Zendesk ticket data pipeline to Snowflake|We want tickets, users, and satisfaction ratings"
    "GitHub→BigQuery|github|bigquery|Pull GitHub repository data into BigQuery|We need pull requests, commits, and issues from our org"
    "Google Analytics→Snowflake|google analytics|snowflake|Ingest GA4 data into Snowflake|We want events, sessions, and user demographics"
    "LinkedIn Ads→Redshift|linkedin|redshift|Load LinkedIn Ads data into Redshift|Campaign performance and demographic breakdowns"
    "Elasticsearch→S3|elasticsearch|s3|Archive Elasticsearch indices to S3|We want to offload older logs to S3 in Parquet format"
    "PostgreSQL→Snowflake|postgres|snowflake|Replicate PostgreSQL tables to Snowflake|Source is prod-db:5432 database app schema public tables users and orders"
    "DynamoDB→BigQuery|dynamodb|bigquery|Migrate DynamoDB tables to BigQuery|Table is user-sessions in us-east-1"
)

for entry in "${PIPELINES[@]}"; do
    IFS='|' read -r name src tgt prompt1 prompt2 <<< "$entry"
    test_name "Pipeline: $name (multi-turn)"
    SESSION="pipe_${name// /_}_$$"

    # Turn 1: Declare intent
    RESP1=$(chat_text "$prompt1" "$SESSION")
    sleep 1

    # Turn 2: Provide details
    RESP2=$(chat_text "$prompt2" "$SESSION")

    if [ -n "$RESP1" ] && [ ${#RESP1} -gt 20 ] && [ -n "$RESP2" ] && [ ${#RESP2} -gt 20 ]; then
        # Check both turns produced coherent responses
        if (contains "$RESP1" "$src" || contains "$RESP1" "$tgt" || contains "$RESP1" "pipeline" || contains "$RESP1" "connect" || contains "$RESP1" "ingest") && \
           (contains "$RESP2" "schema" || contains "$RESP2" "table" || contains "$RESP2" "column" || contains "$RESP2" "pipeline" || contains "$RESP2" "incremental" || contains "$RESP2" "full" || contains "$RESP2" "schedule" || contains "$RESP2" "connect" || contains "$RESP2" "credential" || contains "$RESP2" "detail"); then
            pass "Multi-turn conversation for $name succeeded"
            info "Turn 1: ${RESP1:0:100}..."
            info "Turn 2: ${RESP2:0:100}..."
        else
            warn "Agent responded to both turns but context unclear"
            info "Turn 1: ${RESP1:0:100}..."
            info "Turn 2: ${RESP2:0:100}..."
        fi
    else
        fail "Multi-turn conversation for $name incomplete"
        info "Turn 1 length: ${#RESP1}"
        info "Turn 2 length: ${#RESP2}"
    fi
done

# ============================================================================
# SECTION 5: Chat - Agent Understanding Tests
# ============================================================================
section "CHAT: AGENT UNDERSTANDING & CAPABILITIES"

# --- Help / Capabilities ---
test_name "Chat: What can you do?"
RESP=$(chat_text "What can you help me with?" "understand_1_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 50 ]; then
    pass "Agent explains capabilities (${#RESP} chars)"
    info "${RESP:0:150}..."
else
    fail "Insufficient capabilities response"
fi

# --- List pipelines ---
test_name "Chat: Show my pipelines"
RESP=$(chat_text "show me all my pipelines" "understand_2_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 10 ]; then
    pass "Agent responds to pipeline listing request"
    info "${RESP:0:120}..."
else
    fail "No response to pipeline listing"
fi

# --- List connectors ---
test_name "Chat: What connectors are available?"
RESP=$(chat_text "what connectors do I have?" "understand_3_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 10 ]; then
    pass "Agent responds to connector listing request"
    info "${RESP:0:120}..."
else
    fail "No response to connector listing"
fi

# --- Pipeline status ---
test_name "Chat: How are my pipelines doing?"
RESP=$(chat_text "Are any of my pipelines failing or behind schedule?" "understand_4_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 10 ]; then
    pass "Agent responds to monitoring question"
    info "${RESP:0:120}..."
else
    fail "No response to monitoring question"
fi

# --- Quality gate ---
test_name "Chat: Explain quality gates"
RESP=$(chat_text "How do quality gates work in this system?" "understand_5_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 30 ]; then
    pass "Agent explains quality gates"
    info "${RESP:0:120}..."
else
    fail "No explanation of quality gates"
fi

# --- Schema drift ---
test_name "Chat: Schema drift detection"
RESP=$(chat_text "Has there been any schema drift on my data sources?" "understand_6_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 10 ]; then
    pass "Agent responds to schema drift query"
    info "${RESP:0:120}..."
else
    fail "No response to schema drift query"
fi

# --- Complex request ---
test_name "Chat: Complex multi-source request"
RESP=$(chat_text "I need to set up ingestion from 3 sources: Stripe for payments, Salesforce for CRM, and Google Analytics for website data. All should land in Snowflake." "understand_7_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 50 ]; then
    if contains "$RESP" "Stripe" || contains "$RESP" "Salesforce" || contains "$RESP" "Google Analytics" || contains "$RESP" "Snowflake" || contains "$RESP" "three" || contains "$RESP" "3"; then
        pass "Agent understands complex multi-source request"
        info "${RESP:0:150}..."
    else
        warn "Agent responded but may not have understood all sources"
        info "${RESP:0:150}..."
    fi
else
    fail "Insufficient response to complex request"
fi

# --- Scheduling ---
test_name "Chat: Scheduling question"
RESP=$(chat_text "I want to run my pipeline every 15 minutes" "understand_8_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 10 ]; then
    pass "Agent responds to scheduling request"
    info "${RESP:0:120}..."
else
    fail "No response to scheduling request"
fi

# --- Incremental vs full ---
test_name "Chat: Incremental vs full refresh"
RESP=$(chat_text "Should I use incremental or full refresh for a large orders table with 50 million rows?" "understand_9_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 30 ]; then
    if contains "$RESP" "incremental" || contains "$RESP" "watermark" || contains "$RESP" "full"; then
        pass "Agent gives advice on refresh strategy"
        info "${RESP:0:150}..."
    else
        warn "Agent responded but didn't address refresh strategy"
        info "${RESP:0:150}..."
    fi
else
    fail "No response to refresh strategy question"
fi

# --- Error budget ---
test_name "Chat: Error budget question"
RESP=$(chat_text "What happens when a pipeline exhausts its error budget?" "understand_10_$$")
if [ -n "$RESP" ] && [ ${#RESP} -gt 20 ]; then
    pass "Agent explains error budgets"
    info "${RESP:0:120}..."
else
    fail "No error budget explanation"
fi

fi # --chat

# ============================================================================
# SECTION 6: Connector Generation via API
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--sources" ] || [ "$TEST_MODE" = "--targets" ]; then

section "CONNECTOR GENERATION API"

# Generate a source connector for Oracle
test_name "Generate Source Connector: Oracle"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "source",
    "db_type": "oracle",
    "params": {"host": "oracle-prod.example.com", "port": 1521, "service_name": "ORCL", "user": "app_user"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    CID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('connector_id',''))" 2>/dev/null)
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Oracle source connector generated (ID: $CID, valid: $VALID)"
    info "$BODY"
else
    fail "Oracle source connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a source connector for SQL Server
test_name "Generate Source Connector: SQL Server"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "source",
    "db_type": "sqlserver",
    "params": {"host": "sqlserver.internal", "port": 1433, "database": "sales", "user": "etl_user"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "SQL Server source connector generated (valid: $VALID)"
else
    fail "SQL Server source connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a source connector for Stripe
test_name "Generate Source Connector: Stripe API"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "source",
    "db_type": "stripe",
    "params": {"api_key": "sk_test_fake", "objects": ["charges", "customers", "subscriptions"]}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Stripe source connector generated (valid: $VALID)"
else
    fail "Stripe source connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a source connector for Google Ads
test_name "Generate Source Connector: Google Ads"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "source",
    "db_type": "google_ads",
    "params": {"customer_id": "123-456-7890", "developer_token": "fake_token", "refresh_token": "fake_refresh"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Google Ads source connector generated (valid: $VALID)"
else
    fail "Google Ads source connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a source connector for Facebook Insights
test_name "Generate Source Connector: Facebook Insights"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "source",
    "db_type": "facebook_insights",
    "params": {"access_token": "fake_token", "page_id": "123456789", "metrics": ["page_impressions", "page_engaged_users"]}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Facebook Insights source connector generated (valid: $VALID)"
else
    fail "Facebook Insights source connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a target connector for Snowflake
test_name "Generate Target Connector: Snowflake"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "target",
    "db_type": "snowflake",
    "params": {"account": "xy12345.us-east-1", "warehouse": "ETL_WH", "database": "ANALYTICS", "schema": "RAW", "user": "loader"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Snowflake target connector generated (valid: $VALID)"
else
    fail "Snowflake target connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a target connector for BigQuery
test_name "Generate Target Connector: BigQuery"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "target",
    "db_type": "bigquery",
    "params": {"project_id": "my-gcp-project", "dataset": "raw_data", "credentials_json": "{}"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "BigQuery target connector generated (valid: $VALID)"
else
    fail "BigQuery target connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a target connector for Redshift
test_name "Generate Target Connector: Redshift"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "target",
    "db_type": "redshift",
    "params": {"host": "my-cluster.abc123.us-east-1.redshift.amazonaws.com", "port": 5439, "database": "warehouse", "user": "etl_loader"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Redshift target connector generated (valid: $VALID)"
else
    fail "Redshift target connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

# Generate a target connector for Databricks
test_name "Generate Target Connector: Databricks"
RESP=$(api_post "/api/connectors/generate" '{
    "connector_type": "target",
    "db_type": "databricks",
    "params": {"host": "dbc-abc123.cloud.databricks.com", "http_path": "/sql/1.0/warehouses/xyz", "token": "dapi123", "catalog": "main", "schema": "raw"}
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    VALID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('validation',{}).get('valid',''))" 2>/dev/null)
    pass "Databricks target connector generated (valid: $VALID)"
else
    fail "Databricks target connector generation failed (HTTP $CODE)"
    info "$BODY"
fi

fi # connector generation

# ============================================================================
# SECTION 7: Pipeline CRUD via REST API
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--api" ]; then

section "PIPELINE CRUD VIA REST API"

# First get connector IDs
CONNECTORS=$(curl -s "$API_URL/api/connectors" 2>/dev/null)
SRC_ID=$(echo "$CONNECTORS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for c in data.get('connectors', []):
    if c.get('connector_type','').lower() in ('source','SOURCE'):
        print(c['connector_id']); break
" 2>/dev/null)

TGT_ID=$(echo "$CONNECTORS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for c in data.get('connectors', []):
    if c.get('connector_type','').lower() in ('target','TARGET'):
        print(c['connector_id']); break
" 2>/dev/null)

if [ -n "$SRC_ID" ] && [ -n "$TGT_ID" ]; then
    # Create Pipeline
    test_name "POST /api/pipelines - Create pipeline"
    RESP=$(api_post "/api/pipelines" "{
        \"source_connector_id\": \"$SRC_ID\",
        \"target_connector_id\": \"$TGT_ID\",
        \"source_schema\": \"test\",
        \"source_table\": \"orders\",
        \"target_schema\": \"raw\",
        \"schedule_cron\": \"0 */2 * * *\",
        \"tier\": 2
    }")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ] || [ "$CODE" = "201" ]; then
        PID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pipeline_id',''))" 2>/dev/null)
        pass "Pipeline created (ID: $PID)"

        # Get pipeline
        test_name "GET /api/pipelines/$PID"
        RESP=$(api_get "/api/pipelines/$PID")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline retrieved"
        else
            fail "Pipeline retrieval failed (HTTP $CODE)"
        fi

        # Update pipeline
        test_name "PATCH /api/pipelines/$PID"
        RESP=$(curl -s -m 30 -w "\n%{http_code}" -X PATCH "$API_URL/api/pipelines/$PID" \
            -H 'Content-Type: application/json' \
            -d '{"tier": 1}' 2>/dev/null)
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline updated to tier 1"
        else
            fail "Pipeline update failed (HTTP $CODE)"
        fi

        # Pause pipeline
        test_name "POST /api/pipelines/$PID/pause"
        RESP=$(api_post "/api/pipelines/$PID/pause" '{}')
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline paused"
        else
            fail "Pipeline pause failed (HTTP $CODE)"
        fi

        # Resume pipeline
        test_name "POST /api/pipelines/$PID/resume"
        RESP=$(api_post "/api/pipelines/$PID/resume" '{}')
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline resumed"
        else
            fail "Pipeline resume failed (HTTP $CODE)"
        fi

        # Preview pipeline
        test_name "GET /api/pipelines/$PID/preview"
        RESP=$(api_get "/api/pipelines/$PID/preview")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline preview returned"
        else
            warn "Pipeline preview returned HTTP $CODE (may need active connectors)"
        fi

        # Run history
        test_name "GET /api/pipelines/$PID/runs"
        RESP=$(api_get "/api/pipelines/$PID/runs")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Pipeline run history returned"
        else
            fail "Pipeline run history failed (HTTP $CODE)"
        fi

        # Quality gate history
        test_name "GET /api/quality/$PID"
        RESP=$(api_get "/api/quality/$PID")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Quality gate history returned"
        else
            fail "Quality gate history failed (HTTP $CODE)"
        fi

        # Lineage
        test_name "GET /api/lineage/$PID"
        RESP=$(api_get "/api/lineage/$PID")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Lineage returned"
        else
            fail "Lineage failed (HTTP $CODE)"
        fi

        # Error budgets
        test_name "GET /api/error-budgets/$PID"
        RESP=$(api_get "/api/error-budgets/$PID")
        CODE=$(echo "$RESP" | tail -1)
        if [ "$CODE" = "200" ]; then
            pass "Error budget returned"
        else
            fail "Error budget failed (HTTP $CODE)"
        fi
    else
        fail "Pipeline creation failed (HTTP $CODE)"
        info "$BODY"
        skip "Dependent tests skipped (no pipeline ID)"
    fi
else
    skip "Pipeline CRUD tests skipped - need source ($SRC_ID) and target ($TGT_ID) connectors"
    info "Generate connectors first with --sources or --targets"
fi

fi # --api

# ============================================================================
# SECTION 8: Approval Workflow
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--api" ]; then

section "APPROVAL WORKFLOW"

test_name "GET /api/approvals - Check pending proposals"
RESP=$(api_get "/api/approvals")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    PENDING=$(echo "$BODY" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data) if isinstance(data, list) else len(data.get('proposals',[])))" 2>/dev/null)
    pass "Found $PENDING pending approvals"

    # Approve first if available
    if [ "$PENDING" -gt 0 ] 2>/dev/null; then
        PROPOSAL_ID=$(echo "$BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
proposals = data if isinstance(data, list) else data.get('proposals',[])
if proposals: print(proposals[0].get('proposal_id',''))
" 2>/dev/null)

        if [ -n "$PROPOSAL_ID" ]; then
            test_name "POST /api/approvals/$PROPOSAL_ID - Approve connector"
            RESP=$(api_post "/api/approvals/$PROPOSAL_ID" '{"action": "approve", "note": "Approved by test suite"}')
            CODE=$(echo "$RESP" | tail -1)
            if [ "$CODE" = "200" ]; then
                pass "Proposal approved"
            else
                warn "Approval returned HTTP $CODE"
            fi
        fi
    fi
else
    fail "Approvals endpoint failed (HTTP $CODE)"
fi

fi # --api

# ============================================================================
# Summary
# ============================================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${CYAN}║  TEST RESULTS SUMMARY                                        ║${NC}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Total tests:  ${BOLD}$TOTAL_COUNT${NC}"
echo -e "  ${GREEN}Passed:${NC}       $PASS_COUNT"
echo -e "  ${RED}Failed:${NC}       $FAIL_COUNT"
echo -e "  ${YELLOW}Warnings:${NC}     $WARN_COUNT"
echo -e "  ${YELLOW}Skipped:${NC}      $SKIP_COUNT"
echo -e "  Duration:     ${DURATION}s"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}ALL TESTS PASSED!${NC}"
else
    echo -e "  ${RED}${BOLD}$FAIL_COUNT TEST(S) FAILED${NC}"
fi

echo ""
echo "Test coverage:"
echo "  - Core API endpoints (health, connectors, pipelines, metrics, etc.)"
echo "  - Database sources: Oracle, SQL Server, MySQL, PostgreSQL, MongoDB, MariaDB,"
echo "    Cassandra, DynamoDB, CockroachDB, Redis, Elasticsearch, Neo4j, ClickHouse,"
echo "    SQLite, Teradata, DB2"
echo "  - SaaS/API sources: Stripe, Google Ads, Facebook Insights, Facebook Ads,"
echo "    Salesforce, HubSpot, Shopify, Google Analytics, Jira, Zendesk, Intercom,"
echo "    Twilio, SendGrid, Mailchimp, QuickBooks, Xero, Notion, Airtable, Slack,"
echo "    GitHub, LinkedIn Ads, Twitter Ads, TikTok Ads, Pinterest Ads, Marketo,"
echo "    Braze, Segment, Mixpanel, Amplitude, Snowplow"
echo "  - File/cloud sources: S3, GCS, Azure Blob, SFTP, FTP"
echo "  - Streaming sources: Kafka, Kinesis, Pub/Sub, RabbitMQ, EventHub"
echo "  - Targets: PostgreSQL, Snowflake, BigQuery, Redshift, Databricks, ClickHouse,"
echo "    MySQL, SQL Server, Oracle, S3, GCS, Azure Synapse, Firebolt, DuckDB,"
echo "    Delta Lake, Apache Iceberg, Elasticsearch, MongoDB"
echo "  - Pipeline CRUD: create, get, update, pause, resume, preview, runs, quality"
echo "  - Multi-turn conversations: 20 source→target pipeline scenarios"
echo "  - Agent understanding: capabilities, scheduling, refresh strategy, error budgets"
echo "  - Connector generation: Oracle, SQL Server, Stripe, Google Ads, Facebook,"
echo "    Snowflake, BigQuery, Redshift, Databricks"
echo "  - Approval workflow"
echo ""

exit $FAIL_COUNT
