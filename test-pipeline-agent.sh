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
TEST_USER="${TEST_USER:-admin}"
TEST_PASS="${TEST_PASS:-admin}"
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0
SKIP_COUNT=0
TOTAL_COUNT=0
TEST_MODE="${1:-all}"
AUTH_HEADER=""

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
# Auth -- login and get JWT token
# ============================================================================
login_response=$(curl -s -m 10 -X POST "$API_URL/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\": \"$TEST_USER\", \"password\": \"$TEST_PASS\"}" 2>/dev/null)

AUTH_TOKEN=$(echo "$login_response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
if [ -n "$AUTH_TOKEN" ] && [ "$AUTH_TOKEN" != "" ]; then
    AUTH_HEADER="Authorization: Bearer $AUTH_TOKEN"
    echo -e "${GREEN}Authenticated as $TEST_USER${NC}"
else
    # Auth might be disabled -- continue without token
    AUTH_HEADER=""
    echo -e "${YELLOW}No auth token (auth may be disabled)${NC}"
fi

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
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
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
    curl -s -m 30 -w "\n%{http_code}" \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        "$API_URL$1" 2>/dev/null
}

# HTTP POST with JSON
api_post() {
    curl -s -m 60 -w "\n%{http_code}" -X POST "$API_URL$1" \
        -H 'Content-Type: application/json' \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        -d "$2" 2>/dev/null
}

# HTTP PATCH with JSON
api_patch() {
    curl -s -m 30 -w "\n%{http_code}" -X PATCH "$API_URL$1" \
        -H 'Content-Type: application/json' \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        -d "$2" 2>/dev/null
}

# HTTP DELETE
api_delete() {
    curl -s -m 30 -w "\n%{http_code}" -X DELETE "$API_URL$1" \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} 2>/dev/null
}

# HTTP POST with plain-text body (for YAML import)
api_post_text() {
    curl -s -m 60 -w "\n%{http_code}" -X POST "$API_URL$1" \
        -H 'Content-Type: text/plain' \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        -d "$2" 2>/dev/null
}

# Extract a JSON field value from a response body
json_field() {
    echo "$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$2',''))" 2>/dev/null
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
CONNECTORS=$(curl -s ${AUTH_HEADER:+-H "$AUTH_HEADER"} "$API_URL/api/connectors" 2>/dev/null)
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

        # Update pipeline (basic)
        test_name "PATCH /api/pipelines/$PID - basic tier update"
        RESP=$(api_patch "/api/pipelines/$PID" '{"tier": 1}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            pass "Pipeline updated to tier 1"
        else
            fail "Pipeline update failed (HTTP $CODE)"
        fi

        # --- Build 10: Expanded PATCH tests ---

        # PATCH schedule fields
        test_name "PATCH schedule fields (cron, retry, backoff, timeout)"
        RESP=$(api_patch "/api/pipelines/$PID" '{"schedule_cron": "*/15 * * * *", "retry_max_attempts": 5, "retry_backoff_seconds": 120, "timeout_seconds": 7200, "reason": "test: schedule update"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            CRON=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('schedule_cron',''))" 2>/dev/null)
            RETRY=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('retry_max_attempts',''))" 2>/dev/null)
            TIMEOUT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('timeout_seconds',''))" 2>/dev/null)
            if [ "$CRON" = "*/15 * * * *" ] && [ "$RETRY" = "5" ] && [ "$TIMEOUT" = "7200" ]; then
                pass "Schedule fields updated (cron=$CRON, retry=$RETRY, timeout=$TIMEOUT)"
            else
                fail "Schedule fields not applied correctly (cron=$CRON, retry=$RETRY, timeout=$TIMEOUT)"
            fi
        else
            fail "Schedule PATCH failed (HTTP $CODE)"
        fi

        # PATCH strategy fields
        test_name "PATCH strategy fields (refresh_type, load_type, incremental_column)"
        RESP=$(api_patch "/api/pipelines/$PID" '{"refresh_type": "incremental", "load_type": "merge", "merge_keys": ["id"], "incremental_column": "updated_at", "replication_method": "watermark", "reason": "test: strategy update"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            RT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('refresh_type',''))" 2>/dev/null)
            LT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('load_type',''))" 2>/dev/null)
            IC=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('incremental_column',''))" 2>/dev/null)
            MK=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('merge_keys',''))" 2>/dev/null)
            if [ "$RT" = "incremental" ] && [ "$LT" = "merge" ] && [ "$IC" = "updated_at" ]; then
                pass "Strategy fields updated (refresh=$RT, load=$LT, inc_col=$IC, keys=$MK)"
            else
                fail "Strategy fields not applied correctly (refresh=$RT, load=$LT, inc_col=$IC)"
            fi
        else
            fail "Strategy PATCH failed (HTTP $CODE)"
        fi

        # PATCH quality config partial merge
        test_name "PATCH quality_config partial merge"
        RESP=$(api_patch "/api/pipelines/$PID" '{"quality_config": {"count_tolerance": 0.05, "volume_z_score_warn": 3.0, "promote_on_warn": false}, "reason": "test: quality update"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            CT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('quality_config',{}).get('count_tolerance',''))" 2>/dev/null)
            VZW=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('quality_config',{}).get('volume_z_score_warn',''))" 2>/dev/null)
            POW=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('quality_config',{}).get('promote_on_warn',''))" 2>/dev/null)
            VZF=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('quality_config',{}).get('volume_z_score_fail',''))" 2>/dev/null)
            if [ "$CT" = "0.05" ] && [ "$POW" = "False" ] && [ "$VZF" = "3.0" ]; then
                pass "Quality config partially merged (count_tol=$CT, promote_on_warn=$POW, vol_z_fail=$VZF unchanged)"
            else
                fail "Quality config merge incorrect (count_tol=$CT, promote_on_warn=$POW, vol_z_fail=$VZF)"
            fi
        else
            fail "Quality config PATCH failed (HTTP $CODE)"
        fi

        # PATCH observability fields
        test_name "PATCH observability fields (owner, freshness_column, auto_approve)"
        RESP=$(api_patch "/api/pipelines/$PID" '{"owner": "data-team", "freshness_column": "updated_at", "auto_approve_additive_schema": true, "reason": "test: observability update"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            OWNER=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('owner',''))" 2>/dev/null)
            FC=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('freshness_column',''))" 2>/dev/null)
            AA=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_approve_additive_schema',''))" 2>/dev/null)
            if [ "$OWNER" = "data-team" ] && [ "$FC" = "updated_at" ] && [ "$AA" = "True" ]; then
                pass "Observability fields updated (owner=$OWNER, freshness_col=$FC, auto_approve=$AA)"
            else
                fail "Observability fields incorrect (owner=$OWNER, freshness_col=$FC, auto_approve=$AA)"
            fi
        else
            fail "Observability PATCH failed (HTTP $CODE)"
        fi

        # PATCH watermark reset
        test_name "PATCH reset_watermark"
        RESP=$(api_patch "/api/pipelines/$PID" '{"reset_watermark": true, "reason": "test: watermark reset"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            WM=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('last_watermark','NOTNULL'))" 2>/dev/null)
            if [ "$WM" = "None" ]; then
                pass "Watermark reset to null"
            else
                fail "Watermark not reset (got: $WM)"
            fi
        else
            fail "Watermark reset PATCH failed (HTTP $CODE)"
        fi

        # PATCH no-change guard (version should not bump)
        test_name "PATCH with no changes (version guard)"
        V_BEFORE=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('version',''))" 2>/dev/null)
        RESP=$(api_patch "/api/pipelines/$PID" '{}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            V_AFTER=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('version',''))" 2>/dev/null)
            if [ "$V_BEFORE" = "$V_AFTER" ]; then
                pass "Empty PATCH did not bump version (v$V_BEFORE → v$V_AFTER)"
            else
                fail "Empty PATCH bumped version (v$V_BEFORE → v$V_AFTER)"
            fi
        else
            fail "Empty PATCH failed (HTTP $CODE)"
        fi

        # PATCH version bump on real change
        test_name "PATCH version bump on actual change"
        V_BEFORE=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('version',''))" 2>/dev/null)
        RESP=$(api_patch "/api/pipelines/$PID" '{"tier": 3, "reason": "test: version bump check"}')
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            V_AFTER=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('version',''))" 2>/dev/null)
            if [ "$V_AFTER" -gt "$V_BEFORE" ] 2>/dev/null; then
                pass "Version bumped on change (v$V_BEFORE → v$V_AFTER)"
            else
                fail "Version not bumped (v$V_BEFORE → v$V_AFTER)"
            fi
        else
            fail "Version bump PATCH failed (HTTP $CODE)"
        fi

        # Pipeline detail expanded fields (Build 10)
        test_name "GET /api/pipelines/$PID - expanded detail fields"
        RESP=$(api_get "/api/pipelines/$PID")
        CODE=$(echo "$RESP" | tail -1)
        BODY=$(echo "$RESP" | sed '$d')
        if [ "$CODE" = "200" ]; then
            FIELDS_OK=true
            for FIELD in replication_method retry_max_attempts retry_backoff_seconds timeout_seconds auto_approve_additive_schema freshness_column; do
                HAS=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print('yes' if '$FIELD' in d else 'no')" 2>/dev/null)
                if [ "$HAS" != "yes" ]; then
                    FIELDS_OK=false
                fi
            done
            QC_FIELDS=$(echo "$BODY" | python3 -c "import sys,json; qc=json.load(sys.stdin).get('quality_config',{}); print(len(qc))" 2>/dev/null)
            if [ "$FIELDS_OK" = "true" ] && [ "$QC_FIELDS" -ge 10 ] 2>/dev/null; then
                pass "Detail includes all expanded fields (quality_config has $QC_FIELDS fields)"
            else
                fail "Missing expanded fields (fields_ok=$FIELDS_OK, qc_fields=$QC_FIELDS)"
            fi
        else
            fail "Detail endpoint failed (HTTP $CODE)"
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

# ============================================================================
# SECTION 7b: Pipeline Timeline (Build 8)
# ============================================================================

section "PIPELINE TIMELINE (Build 8)"

# Use a demo pipeline for timeline tests (always exists)
DEMO_PID=$(curl -s ${AUTH_HEADER:+-H "$AUTH_HEADER"} "$API_URL/api/pipelines" 2>/dev/null | \
    python3 -c "import sys,json; ps=json.load(sys.stdin); print(ps[0]['pipeline_id'] if ps else '')" 2>/dev/null)

if [ -n "$DEMO_PID" ]; then
    # Timeline endpoint
    test_name "GET /api/pipelines/$DEMO_PID/timeline"
    RESP=$(api_get "/api/pipelines/$DEMO_PID/timeline?limit=20")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        EVENT_COUNT=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('event_count',len(d.get('events',[]))))" 2>/dev/null)
        HAS_TYPES=$(echo "$BODY" | python3 -c "
import sys,json
d = json.load(sys.stdin)
events = d.get('events', d if isinstance(d, list) else [])
types = set(e.get('type','') for e in events)
print(','.join(sorted(types)))
" 2>/dev/null)
        pass "Timeline returned ($EVENT_COUNT events, types: $HAS_TYPES)"
    else
        fail "Timeline endpoint failed (HTTP $CODE)"
    fi

    # Timeline has decision entries (from Build 10 PATCH tests above)
    test_name "Timeline contains decision events"
    DECISIONS=$(echo "$BODY" | python3 -c "
import sys,json
d = json.load(sys.stdin)
events = d.get('events', d if isinstance(d, list) else [])
decisions = [e for e in events if e.get('type') == 'decision']
print(len(decisions))
" 2>/dev/null)
    if [ "$DECISIONS" -gt 0 ] 2>/dev/null; then
        DTYPE=$(echo "$BODY" | python3 -c "
import sys,json
d = json.load(sys.stdin)
events = d.get('events', d if isinstance(d, list) else [])
decisions = [e for e in events if e.get('type') == 'decision']
if decisions: print(decisions[0].get('decision_type',''))
" 2>/dev/null)
        pass "Found $DECISIONS decision events (type: $DTYPE)"
    else
        warn "No decision events in timeline (expected if no PATCHes on demo pipeline)"
    fi

    # Request ID correlation (Build 8)
    test_name "X-Request-ID response header"
    REQ_ID_RESP=$(curl -s -m 10 -D - -o /dev/null "$API_URL/health" ${AUTH_HEADER:+-H "$AUTH_HEADER"} 2>/dev/null)
    if echo "$REQ_ID_RESP" | grep -qi "x-request-id"; then
        REQ_ID=$(echo "$REQ_ID_RESP" | grep -i "x-request-id" | head -1 | tr -d '\r' | awk '{print $2}')
        pass "X-Request-ID header present ($REQ_ID)"
    else
        warn "X-Request-ID header not found in response"
    fi
else
    skip "No pipelines found for timeline tests"
fi

# ============================================================================
# SECTION 7c: Contract-as-Code YAML (Build 9)
# ============================================================================

section "CONTRACT-AS-CODE YAML (Build 9)"

if [ -n "$DEMO_PID" ]; then
    # Single pipeline YAML export
    test_name "GET /api/pipelines/$DEMO_PID/export (YAML)"
    RESP=$(api_get "/api/pipelines/$DEMO_PID/export")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        if contains "$BODY" "pipeline_name" && contains "$BODY" "strategy" && contains "$BODY" "schedule"; then
            pass "Single pipeline YAML export contains expected sections"
            info "$(echo "$BODY" | head -3)"
        else
            fail "YAML export missing expected sections"
            info "$BODY"
        fi
    else
        fail "Single pipeline YAML export failed (HTTP $CODE)"
    fi

    # Single pipeline YAML export with state
    test_name "GET /api/pipelines/$DEMO_PID/export?include_state=true"
    RESP=$(api_get "/api/pipelines/$DEMO_PID/export?include_state=true")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        if contains "$BODY" "_state" || contains "$BODY" "baselines" || contains "$BODY" "last_watermark"; then
            pass "YAML export with state includes _state section"
        else
            warn "YAML export returned 200 but _state section not found"
        fi
    else
        fail "YAML export with state failed (HTTP $CODE)"
    fi

    # Bulk YAML export
    test_name "GET /api/pipelines/export (bulk)"
    RESP=$(api_get "/api/pipelines/export")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        DOC_COUNT=$(echo "$BODY" | grep -c "^pipeline_name:" 2>/dev/null || echo "0")
        if [ "$DOC_COUNT" -ge 1 ]; then
            pass "Bulk YAML export returned $DOC_COUNT pipeline documents"
        else
            warn "Bulk YAML export returned 200 but no pipeline_name fields found"
        fi
    else
        fail "Bulk YAML export failed (HTTP $CODE)"
    fi

    # Bulk YAML export with status filter
    test_name "GET /api/pipelines/export?status=active"
    RESP=$(api_get "/api/pipelines/export?status=active")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Bulk YAML export with status filter returned 200"
    else
        fail "Bulk YAML export with status filter failed (HTTP $CODE)"
    fi

    # YAML import (create mode - should 409 on existing)
    test_name "POST /api/pipelines/import (existing pipeline, expect 409)"
    EXPORT_YAML=$(curl -s -m 30 ${AUTH_HEADER:+-H "$AUTH_HEADER"} "$API_URL/api/pipelines/$DEMO_PID/export" 2>/dev/null)
    RESP=$(curl -s -m 60 -w "\n%{http_code}" -X POST "$API_URL/api/pipelines/import?mode=create" \
        -H 'Content-Type: text/plain' \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        --data-binary "$EXPORT_YAML" 2>/dev/null)
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "409" ] || [ "$CODE" = "200" ]; then
        pass "Import existing pipeline handled correctly (HTTP $CODE)"
    else
        warn "Import returned unexpected HTTP $CODE (expected 409 for duplicate or 200)"
    fi

    # GitOps sync dry-run
    test_name "POST /api/contracts/sync?dry_run=true"
    RESP=$(curl -s -m 60 -w "\n%{http_code}" -X POST "$API_URL/api/contracts/sync?dry_run=true" \
        -H 'Content-Type: text/plain' \
        ${AUTH_HEADER:+-H "$AUTH_HEADER"} \
        --data-binary "$EXPORT_YAML" 2>/dev/null)
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        UNCHANGED=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('unchanged',0))" 2>/dev/null)
        pass "Sync dry-run returned 200 (unchanged: $UNCHANGED)"
    else
        fail "Sync dry-run failed (HTTP $CODE)"
        info "$BODY"
    fi
else
    skip "No pipelines found for YAML tests"
fi

# ============================================================================
# SECTION 7d: Change Audit & YAML Persistence (Build 10)
# ============================================================================

section "CHANGE AUDIT & YAML PERSISTENCE (Build 10)"

if [ -n "$DEMO_PID" ]; then
    # Make a tracked change on the demo pipeline
    test_name "PATCH demo pipeline with audit reason"
    DEMO_NAME=$(curl -s ${AUTH_HEADER:+-H "$AUTH_HEADER"} "$API_URL/api/pipelines/$DEMO_PID" 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('pipeline_name',''))" 2>/dev/null)
    RESP=$(api_patch "/api/pipelines/$DEMO_PID" '{"owner": "test-suite-owner", "reason": "Automated test suite verification"}')
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        pass "Demo pipeline patched with audit reason"
    else
        fail "Demo pipeline patch failed (HTTP $CODE)"
    fi

    # Verify audit trail appears in timeline
    test_name "Verify contract_update in timeline after PATCH"
    RESP=$(api_get "/api/pipelines/$DEMO_PID/timeline?limit=5")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        HAS_UPDATE=$(echo "$BODY" | python3 -c "
import sys,json
d = json.load(sys.stdin)
events = d.get('events', d if isinstance(d, list) else [])
updates = [e for e in events if e.get('decision_type') == 'contract_update']
if updates:
    print(updates[0].get('reasoning',''))
else:
    print('')
" 2>/dev/null)
        if [ -n "$HAS_UPDATE" ]; then
            pass "contract_update found in timeline (reason: $HAS_UPDATE)"
        else
            fail "No contract_update event found in timeline"
        fi
    else
        fail "Timeline fetch failed (HTTP $CODE)"
    fi

    # Verify YAML file persisted to disk
    test_name "YAML auto-persistence to data/contracts/"
    SAFE_NAME=$(echo "$DEMO_NAME" | tr '/ ' '__')
    if [ -f "data/contracts/${SAFE_NAME}.yaml" ]; then
        YAML_SIZE=$(wc -c < "data/contracts/${SAFE_NAME}.yaml" | tr -d ' ')
        if contains "$(cat data/contracts/${SAFE_NAME}.yaml)" "pipeline_name"; then
            pass "YAML file exists (${YAML_SIZE} bytes) at data/contracts/${SAFE_NAME}.yaml"
        else
            fail "YAML file exists but doesn't contain pipeline_name"
        fi
    else
        warn "YAML file not found at data/contracts/${SAFE_NAME}.yaml (may need prior PATCH)"
    fi

    # Verify credentials are masked in YAML
    test_name "YAML file masks credentials"
    if [ -f "data/contracts/${SAFE_NAME}.yaml" ]; then
        if grep -q "password: '\*\*\*'" "data/contracts/${SAFE_NAME}.yaml" 2>/dev/null || \
           ! grep -q "password: '[^*]" "data/contracts/${SAFE_NAME}.yaml" 2>/dev/null; then
            pass "Credentials masked in YAML file"
        else
            fail "Unmasked credentials found in YAML file"
        fi
    else
        skip "No YAML file to check for credential masking"
    fi

    # Revert demo pipeline owner
    api_patch "/api/pipelines/$DEMO_PID" '{"owner": null}' > /dev/null 2>&1
else
    skip "No pipelines found for audit tests"
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

fi # --api (approval)

# ============================================================================
# SECTION 9: Data Contracts (Build 16)
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--api" ]; then

section "DATA CONTRACTS (Build 16)"

# Get two demo pipeline IDs for testing
PIPELINES_RESP=$(api_get "/api/pipelines")
PIPELINES_BODY=$(echo "$PIPELINES_RESP" | sed '$d')
PRODUCER_PID=$(echo "$PIPELINES_BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
pipelines = data if isinstance(data, list) else data.get('pipelines',[])
for p in pipelines:
    if 'orders' in p.get('pipeline_name','').lower() or 'stripe' in p.get('pipeline_name','').lower():
        print(p['pipeline_id']); break
" 2>/dev/null)

CONSUMER_PID=$(echo "$PIPELINES_BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
pipelines = data if isinstance(data, list) else data.get('pipelines',[])
for p in pipelines:
    if 'customer' in p.get('pipeline_name','').lower() or 'analytics' in p.get('pipeline_name','').lower():
        print(p['pipeline_id']); break
" 2>/dev/null)

if [ -n "$PRODUCER_PID" ] && [ -n "$CONSUMER_PID" ]; then

    # Test 1: Create data contract
    test_name "POST /api/data-contracts - Create contract"
    RESP=$(api_post "/api/data-contracts" "{
        \"producer_pipeline_id\": \"$PRODUCER_PID\",
        \"consumer_pipeline_id\": \"$CONSUMER_PID\",
        \"description\": \"Test contract for Build 16\",
        \"freshness_sla_minutes\": 120,
        \"retention_hours\": 48,
        \"cleanup_ownership\": \"consumer_acknowledges\",
        \"required_columns\": [\"id\"]
    }")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    CONTRACT_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('contract_id',''))" 2>/dev/null)
    if [ "$CODE" = "200" ] && [ -n "$CONTRACT_ID" ]; then
        pass "Created contract $CONTRACT_ID"
    else
        fail "Create contract failed (HTTP $CODE)"
    fi

    # Test 2: List data contracts
    test_name "GET /api/data-contracts - List contracts"
    RESP=$(api_get "/api/data-contracts")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    TOTAL=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total',0))" 2>/dev/null)
    if [ "$CODE" = "200" ] && [ "$TOTAL" -ge 1 ] 2>/dev/null; then
        pass "Found $TOTAL contract(s)"
    else
        fail "List contracts failed (HTTP $CODE, total=$TOTAL)"
    fi

    # Test 3: Get contract detail
    test_name "GET /api/data-contracts/$CONTRACT_ID - Get detail"
    RESP=$(api_get "/api/data-contracts/$CONTRACT_ID")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    STATUS=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
    if [ "$CODE" = "200" ] && [ "$STATUS" = "active" ]; then
        pass "Contract detail returned (status=$STATUS)"
    else
        fail "Get contract failed (HTTP $CODE)"
    fi

    # Test 4: Validate contract
    test_name "POST /api/data-contracts/$CONTRACT_ID/validate - Validate"
    RESP=$(api_post "/api/data-contracts/$CONTRACT_ID/validate" '{}')
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        V_COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('violations_found',0))" 2>/dev/null)
        pass "Validation completed ($V_COUNT violation(s) found)"
    else
        fail "Validate contract failed (HTTP $CODE)"
    fi

    # Test 5: Update contract SLA
    test_name "PATCH /api/data-contracts/$CONTRACT_ID - Update SLA"
    RESP=$(api_patch "/api/data-contracts/$CONTRACT_ID" '{"freshness_sla_minutes": 240}')
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Contract SLA updated"
    else
        fail "Patch contract failed (HTTP $CODE)"
    fi

    # Test 6: List violations
    test_name "GET /api/data-contracts/$CONTRACT_ID/violations - List violations"
    RESP=$(api_get "/api/data-contracts/$CONTRACT_ID/violations")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    if [ "$CODE" = "200" ]; then
        V_TOTAL=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total',0))" 2>/dev/null)
        pass "Listed $V_TOTAL violation(s)"
    else
        fail "List violations failed (HTTP $CODE)"
    fi

    # Test 7: Pipeline detail includes data_contracts field
    test_name "GET /api/pipelines/$PRODUCER_PID - Check data_contracts field"
    RESP=$(api_get "/api/pipelines/$PRODUCER_PID")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    HAS_DC=$(echo "$BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
dc = data.get('data_contracts', {})
print('yes' if dc.get('as_producer') is not None else 'no')
" 2>/dev/null)
    if [ "$CODE" = "200" ] && [ "$HAS_DC" = "yes" ]; then
        pass "Pipeline detail includes data_contracts"
    else
        fail "Pipeline detail missing data_contracts (HTTP $CODE)"
    fi

    # Test 8: Auto-dependency creation
    test_name "GET /api/pipelines/$CONSUMER_PID/dependencies - Auto-dep from contract"
    RESP=$(api_get "/api/pipelines/$CONSUMER_PID/dependencies")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    HAS_DEP=$(echo "$BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
upstream = data.get('upstream', [])
print('yes' if any(d.get('depends_on_id') == '$PRODUCER_PID' for d in upstream) else 'no')
" 2>/dev/null)
    if [ "$CODE" = "200" ] && [ "$HAS_DEP" = "yes" ]; then
        pass "Auto-dependency created"
    else
        warn "Auto-dependency check inconclusive (HTTP $CODE, has_dep=$HAS_DEP)"
    fi

    # Test 9: Duplicate contract rejected
    test_name "POST /api/data-contracts - Duplicate rejected (409)"
    RESP=$(api_post "/api/data-contracts" "{
        \"producer_pipeline_id\": \"$PRODUCER_PID\",
        \"consumer_pipeline_id\": \"$CONSUMER_PID\"
    }")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "409" ]; then
        pass "Duplicate contract correctly rejected"
    else
        fail "Expected 409 for duplicate, got HTTP $CODE"
    fi

    # Test 10: Self-contract rejected
    test_name "POST /api/data-contracts - Self-contract rejected (400)"
    RESP=$(api_post "/api/data-contracts" "{
        \"producer_pipeline_id\": \"$PRODUCER_PID\",
        \"consumer_pipeline_id\": \"$PRODUCER_PID\"
    }")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "400" ]; then
        pass "Self-contract correctly rejected"
    else
        fail "Expected 400 for self-contract, got HTTP $CODE"
    fi

    # Test 11: Delete contract
    test_name "DELETE /api/data-contracts/$CONTRACT_ID - Delete contract"
    RESP=$(api_delete "/api/data-contracts/$CONTRACT_ID")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Contract deleted"
    else
        fail "Delete contract failed (HTTP $CODE)"
    fi

else
    skip "No suitable demo pipelines found for data contract tests"
fi

fi # --api (data contracts)

# ============================================================================
# SECTION 10: DAG Visualization & Topology (Builds 19-20)
# ============================================================================
if [ "$TEST_MODE" = "all" ] || [ "$TEST_MODE" = "--api" ]; then

section "DAG VISUALIZATION & TOPOLOGY (Builds 19-20)"

# Test 1: DAG endpoint
test_name "GET /api/dag - Get pipeline dependency graph"
RESP=$(api_get "/api/dag")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    NODE_COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_pipelines',0))" 2>/dev/null)
    EDGE_COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_edges',0))" 2>/dev/null)
    pass "DAG returned: $NODE_COUNT nodes, $EDGE_COUNT edges"
else
    fail "DAG endpoint failed (HTTP $CODE)"
fi

# Test 2: DAG node structure
test_name "GET /api/dag - Verify node structure"
HAS_FIELDS=$(echo "$BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
nodes = data.get('nodes',[])
if nodes:
    n = nodes[0]
    required = ['id','name','status','tier','source','target','last_run']
    print('yes' if all(k in n for k in required) else 'no')
else:
    print('empty')
" 2>/dev/null)
if [ "$HAS_FIELDS" = "yes" ]; then
    pass "DAG nodes have correct structure"
elif [ "$HAS_FIELDS" = "empty" ]; then
    warn "No nodes in DAG to verify"
else
    fail "DAG nodes missing required fields"
fi

# Test 3: DAG includes contract info
test_name "GET /api/dag - Nodes include contract fields"
HAS_CONTRACTS=$(echo "$BODY" | python3 -c "
import sys,json
data=json.load(sys.stdin)
nodes = data.get('nodes',[])
if nodes:
    n = nodes[0]
    print('yes' if 'contracts_as_producer' in n and 'contracts_as_consumer' in n else 'no')
else:
    print('empty')
" 2>/dev/null)
if [ "$HAS_CONTRACTS" = "yes" ]; then
    pass "DAG nodes include contract fields"
elif [ "$HAS_CONTRACTS" = "empty" ]; then
    warn "No nodes to check"
else
    fail "DAG nodes missing contract fields"
fi

# Test 4: Topology design endpoint
test_name "POST /api/topology/design - Design pipeline architecture"
RESP=$(api_post "/api/topology/design" '{"description": "I need to ingest orders from MySQL and customers from MongoDB into PostgreSQL, then merge them into a unified customer_orders table"}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    PIPELINE_COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('pipelines',[])))" 2>/dev/null)
    PATTERN=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pattern',''))" 2>/dev/null)
    if [ "$PIPELINE_COUNT" -ge 1 ] 2>/dev/null; then
        pass "Topology designed: $PIPELINE_COUNT pipeline(s), pattern=$PATTERN"
    else
        warn "Topology returned but no pipelines (may need API key)"
    fi
else
    warn "Topology design returned HTTP $CODE (may need API key)"
fi

# Test 5: Topology via chat
test_name "Chat - Design topology via conversation"
RESP=$(chat "design a pipeline architecture to ingest Stripe charges and Shopify orders into Snowflake")
HAS_TOPOLOGY=$(echo "$RESP" | python3 -c "
import sys,json
data=json.load(sys.stdin)
r = data.get('response','').lower()
print('yes' if any(kw in r for kw in ['pipeline', 'topology', 'architecture', 'design', 'processing']) else 'no')
" 2>/dev/null)
if [ "$HAS_TOPOLOGY" = "yes" ]; then
    pass "Chat topology design returned response"
else
    warn "Chat topology response unclear"
fi

fi # --api (dag & topology)

# ============================================================================
# Build 21: Source Registry, Pipeline Changelog, Interaction Audit
# ============================================================================
if [ "$MODE" = "all" ] || [ "$MODE" = "api" ]; then

section "Build 21: Source Registry, Changelog, Interactions"

# --- Source Registry ---

# Test 1: Register a source
test_name "POST /api/sources - Register a new source"
RESP=$(api_post "/api/sources" '{
    "display_name": "Test Source",
    "connector_name": "mysql-source-v1",
    "source_type": "mysql",
    "connection_params": {"host": "localhost", "port": 3307, "database": "ecommerce", "user": "root", "password": "demo"},
    "description": "Test source for curl tests",
    "owner": "test-admin"
}')
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ] || [ "$CODE" = "201" ]; then
    TEST_SOURCE_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('source_id',''))" 2>/dev/null)
    pass "Source registered: $TEST_SOURCE_ID"
else
    warn "Source registration returned HTTP $CODE"
    TEST_SOURCE_ID=""
fi

# Test 2: List sources
test_name "GET /api/sources - List registered sources"
RESP=$(api_get "/api/sources")
CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
if [ "$CODE" = "200" ]; then
    SRC_COUNT=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else len(d.get('sources',[])))" 2>/dev/null)
    pass "Listed $SRC_COUNT registered source(s)"
else
    fail "List sources returned HTTP $CODE"
fi

# Test 3: Get source by ID
if [ -n "$TEST_SOURCE_ID" ]; then
    test_name "GET /api/sources/{id} - Get source by ID"
    RESP=$(api_get "/api/sources/$TEST_SOURCE_ID")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Got source by ID"
    else
        fail "Get source returned HTTP $CODE"
    fi
fi

# Test 4: Update source
if [ -n "$TEST_SOURCE_ID" ]; then
    test_name "PATCH /api/sources/{id} - Update source"
    RESP=$(api_patch "/api/sources/$TEST_SOURCE_ID" '{"description": "Updated test source"}')
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Source updated"
    else
        warn "Update source returned HTTP $CODE"
    fi
fi

# Test 5: Discover via source
if [ -n "$TEST_SOURCE_ID" ]; then
    test_name "POST /api/sources/{id}/discover - Discover tables from registered source"
    RESP=$(api_post "/api/sources/$TEST_SOURCE_ID/discover" '{}')
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Discovery from registered source succeeded"
    else
        warn "Discovery returned HTTP $CODE (source may not be reachable)"
    fi
fi

# Test 6: Delete source
if [ -n "$TEST_SOURCE_ID" ]; then
    test_name "DELETE /api/sources/{id} - Delete registered source"
    RESP=$(api_delete "/api/sources/$TEST_SOURCE_ID")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ] || [ "$CODE" = "204" ]; then
        pass "Source deleted"
    else
        warn "Delete source returned HTTP $CODE"
    fi
fi

# --- Pipeline Changelog ---

# Test 7: Per-pipeline changelog
test_name "GET /api/pipelines/{id}/changelog - Pipeline changelog"
if [ -n "$PIPELINE_ID" ]; then
    RESP=$(api_get "/api/pipelines/$PIPELINE_ID/changelog")
    CODE=$(echo "$RESP" | tail -1)
    if [ "$CODE" = "200" ]; then
        pass "Pipeline changelog returned"
    else
        fail "Pipeline changelog returned HTTP $CODE"
    fi
else
    skip "No pipeline ID available"
fi

# Test 8: Global changelog (admin)
test_name "GET /api/changelog - Global changelog"
RESP=$(api_get "/api/changelog")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Global changelog returned"
else
    fail "Global changelog returned HTTP $CODE"
fi

# Test 9: Pipeline detail includes recent_changes
test_name "Pipeline detail includes recent_changes field"
if [ -n "$PIPELINE_ID" ]; then
    RESP=$(api_get "/api/pipelines/$PIPELINE_ID")
    CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    HAS_CHANGES=$(echo "$BODY" | python3 -c "import sys,json; print('yes' if 'recent_changes' in json.load(sys.stdin) else 'no')" 2>/dev/null)
    if [ "$HAS_CHANGES" = "yes" ]; then
        pass "Pipeline detail includes recent_changes"
    else
        warn "recent_changes field not found in pipeline detail"
    fi
else
    skip "No pipeline ID available"
fi

# --- Interaction Audit ---

# Test 10: List interactions
test_name "GET /api/interactions - List chat interactions"
RESP=$(api_get "/api/interactions")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Interactions listed"
else
    fail "Interactions returned HTTP $CODE"
fi

# Test 11: Export interactions
test_name "GET /api/interactions/export - Export interactions as JSONL"
RESP=$(api_get "/api/interactions/export")
CODE=$(echo "$RESP" | tail -1)
if [ "$CODE" = "200" ]; then
    pass "Interactions exported"
else
    fail "Interactions export returned HTTP $CODE"
fi

fi # --api (Build 21)

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
echo "  - Expanded PATCH: schedule, strategy, quality partial merge, watermark reset,"
echo "    observability, version bump, no-change guard (Build 10)"
echo "  - Pipeline detail: expanded fields, full quality_config (Build 10)"
echo "  - Timeline: event listing, decision events, X-Request-ID header (Build 8)"
echo "  - YAML export: single, bulk, with state filter (Build 9)"
echo "  - YAML import: create mode duplicate detection (Build 9)"
echo "  - GitOps sync: dry-run reconciliation (Build 9)"
echo "  - Change audit: DecisionLog in timeline, YAML persistence, credential masking (Build 10)"
echo "  - Multi-turn conversations: 20 source→target pipeline scenarios"
echo "  - Agent understanding: capabilities, scheduling, refresh strategy, error budgets"
echo "  - Connector generation: Oracle, SQL Server, Stripe, Google Ads, Facebook,"
echo "    Snowflake, BigQuery, Redshift, Databricks"
echo "  - Approval workflow"
echo "  - Data contracts: create, list, get, validate, update, violations, auto-dep,"
echo "    duplicate/self rejection, delete (Build 16)"
echo "  - DAG visualization: graph endpoint, node structure, contract fields (Build 19)"
echo "  - Topology reasoning: design endpoint, chat routing (Build 20)"
echo "  - Source registry: register, list, get, update, discover, delete (Build 21)"
echo "  - Pipeline changelog: per-pipeline, global, in detail response (Build 21)"
echo "  - Interaction audit: list, export (Build 21)"
echo ""

exit $FAIL_COUNT
