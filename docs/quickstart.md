# Quickstart

Get DAPOS running locally in 5 minutes with the demo environment.

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- An Anthropic API key (optional — agent features require it, but pipelines work without)

## 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 16** (port 5432) — DAPOS metadata store
- **Demo MySQL** (port 3307) — e-commerce source data (orders, customers)
- **Demo MongoDB** (port 27018) — analytics events source
- **Mock SaaS APIs** (port 8200) — Stripe, Google Ads, Facebook Insights

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

## 3. Start DAPOS

```bash
# With AI agent (recommended)
ANTHROPIC_API_KEY=sk-ant-... python main.py

# Without AI agent (rule-based fallbacks only)
python main.py
```

## 4. Open the UI

Navigate to **http://localhost:8100**

Login: `admin` / `admin`

## What Happens on First Startup

DAPOS automatically:
1. Creates database tables
2. Seeds 8 connectors (MySQL, SQLite, MongoDB, Stripe, Google Ads, Facebook Insights sources + PostgreSQL, Redshift targets)
3. Creates 4 demo pipelines and triggers their first run immediately:

| Pipeline | Source | Target | Data |
|----------|--------|--------|------|
| demo-ecommerce-orders | Demo MySQL | Local PostgreSQL | 30 orders |
| demo-ecommerce-customers | Demo MySQL | Local PostgreSQL | 20 customers (incremental) |
| demo-analytics-events | Demo MongoDB | Local PostgreSQL | 200 web events |
| demo-stripe-charges | Mock Stripe API | Local PostgreSQL | 50 charges |

## 5. Try the Chat Interface

Click the **Chat** tab and try:

```
list my pipelines
```
```
why is demo-ecommerce-orders failing?
```
```
what breaks if demo-stripe-charges goes down?
```
```
are there any anomalies?
```

## 6. Try the CLI

```bash
# List pipelines
python -m cli pipelines list

# Diagnose a pipeline
python -m cli diagnose demo-stripe-charges

# Check platform health
python -m cli health

# Trigger a run
python -m cli trigger demo-stripe-charges
```

## 7. Create Your Own Pipeline

Via chat:
```
I want to set up a pipeline from MySQL to PostgreSQL for the orders table
```

Or via CLI:
```bash
python -m cli pipelines create \
  --source-connector mysql-demo \
  --source-schema ecommerce \
  --source-table orders \
  --target-schema raw \
  --schedule "0 * * * *"
```

Or via API:
```bash
curl -X POST http://localhost:8100/api/pipelines \
  -H "Authorization: Bearer $(python -m cli token)" \
  -H "Content-Type: application/json" \
  -d '{
    "source_connector_id": "<mysql-connector-id>",
    "source_schema": "ecommerce",
    "source_table": "orders",
    "target_schema": "raw",
    "schedule_cron": "0 * * * *"
  }'
```

## Next Steps

- [First Pipeline Guide](first-pipeline.md) — detailed walkthrough
- [Architecture](architecture.md) — understand how DAPOS works
- [Configuration](configuration.md) — customize for your environment
- [CLI Reference](cli-reference.md) — full CLI command list
