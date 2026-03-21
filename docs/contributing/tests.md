# Adding Tests

DAPOS uses curl-based integration tests against the real running app. No pytest, no mocks — test the product like a user would.

---

## Running Tests

```bash
# Full suite (~165 tests, ~20 minutes — includes LLM calls)
./test-pipeline-agent.sh

# Fast: API endpoints only (~36 tests)
./test-pipeline-agent.sh --api

# Source connector generation tests
./test-pipeline-agent.sh --sources

# Target connector generation tests
./test-pipeline-agent.sh --targets

# Multi-turn conversation tests
./test-pipeline-agent.sh --chat
```

### Prerequisites
- DAPOS running with `ANTHROPIC_API_KEY` set
- Docker services running (PostgreSQL, demo databases, mock APIs)

---

## Test Structure

All tests are in `test-pipeline-agent.sh`. The script uses bash arrays and helper functions:

```bash
# Test helper — checks HTTP status code
expect_status() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$actual" -eq "$expected" ]; then
    pass "$desc"
  else
    fail "$desc (expected $expected, got $actual)"
  fi
}

# JSON assertion helper
expect_json_field() {
  local desc="$1" json="$2" field="$3" expected="$4"
  actual=$(echo "$json" | jq -r "$field")
  if [ "$actual" = "$expected" ]; then
    pass "$desc"
  else
    fail "$desc (expected '$expected', got '$actual')"
  fi
}
```

---

## Test Categories

| Category | Count | What It Tests |
|----------|-------|---------------|
| Core API | 12 | health, metrics, connectors, pipelines, approvals, freshness |
| Database sources | 16 | Oracle, SQL Server, MySQL, PostgreSQL, MongoDB, etc. |
| SaaS/API sources | 30 | Stripe, Salesforce, HubSpot, Shopify, etc. |
| File/Cloud sources | 5 | S3, GCS, Azure Blob, SFTP, FTP |
| Streaming sources | 5 | Kafka, Kinesis, Pub/Sub, RabbitMQ, EventHub |
| Targets | 18 | Snowflake, BigQuery, Redshift, Databricks, etc. |
| Multi-turn pipelines | 20 | End-to-end pipeline creation conversations |
| Agent understanding | 10 | Capabilities, monitoring, quality, scheduling |
| Pipeline CRUD | 18 | Create, get, update, pause, resume, preview |
| Data contracts | 11 | Create, validate, violations, auto-dep |
| Step DAGs | 5 | Steps, validate, preview, cycle detection |
| Diagnostics | 8 | Diagnose, impact, anomalies, chat routing |
| GitOps | 5 | Status, log, diff, pipeline history, restore |

---

## Adding a New Source Test

Add to the `SOURCES` or `SAAS_SOURCES` array:

```bash
SAAS_SOURCES=(
  # ... existing entries ...
  "My SaaS|Can you create a connector for My SaaS service"
)
```

Format: `"Display Name|Natural language prompt"`

The test sends the prompt to `/api/command` and verifies:
1. HTTP 200 response
2. Response contains connector-related content
3. No server errors

---

## Adding a New Target Test

Add to the `TARGETS` array:

```bash
TARGETS=(
  # ... existing entries ...
  "My Warehouse|I need a target connector for My Warehouse"
)
```

---

## Adding a Pipeline (Multi-Turn) Test

Add to the `PIPELINES` array:

```bash
PIPELINES=(
  # ... existing entries ...
  "My Pipeline|my_source_keyword|my_target_keyword|Turn 1: I have a My Source database at host:3306|Turn 2: Load the users table to My Target"
)
```

Format: `"Name|source_keyword|target_keyword|Turn 1 prompt|Turn 2 prompt"`

---

## Adding an API Test

Add a test function and call it from the API test section:

```bash
test_my_new_endpoint() {
  local desc="My new endpoint returns 200"
  local status=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    http://localhost:8100/api/my-endpoint)
  expect_status "$desc" 200 "$status"
}
```

---

## Tips

- Tests require a running DAPOS instance — they're integration tests, not unit tests
- Source/target/chat tests call the Claude API — they take ~30s each and cost tokens
- API tests are fast (no LLM calls) — use `--api` during development
- Each test is independent — failures don't cascade
- The script outputs a summary with pass/fail counts at the end
