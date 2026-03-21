# Topology Design

The agent can design multi-pipeline architectures from a natural language description of your business problem. You describe what you need; the agent proposes the pipeline topology with the right patterns.

---

## How It Works

1. You describe a business problem or data flow requirement
2. The agent analyzes the description and selects appropriate patterns
3. A multi-pipeline topology is proposed with pipeline names, connections, strategies, and schedules
4. You review and approve before any pipelines are created

---

## Supported Patterns

| Pattern | Description | Example |
|---------|-------------|---------|
| **Consume & merge** | Stage → upsert → cleanup consumed rows | ETL with intermediate staging |
| **Fan-in** | Multiple sources → unified table | 3 MySQL databases → single warehouse table |
| **Fan-out** | One source → multiple targets | Raw events → analytics DB + data lake + reports |
| **SCD Type 2** | Historical change tracking with valid_from/valid_to | Customer dimension with change history |
| **Quarantine** | Bad rows → error table, good rows → production | Quality-segregated data loading |
| **Cascading aggregation** | Raw → daily → monthly → dashboard | Multi-level rollup pipeline |
| **Conditional routing** | Branch on quality/volume thresholds | Route based on data quality score |
| **Replay/reprocess** | Re-run a time window idempotently | Backfill historical data |

---

## Example

### Input
```
"I have orders from MySQL and Stripe, plus customer data from Salesforce.
I need a unified customer 360 view in Snowflake with daily aggregations."
```

### Agent Response
```
Proposed topology: 4 pipelines

1. mysql-orders (MySQL → Snowflake staging)
   - Incremental, merge on order_id, every 15 min

2. stripe-charges (Stripe API → Snowflake staging)
   - Incremental, merge on charge_id, every 30 min

3. salesforce-customers (Salesforce → Snowflake staging)
   - Incremental, merge on customer_id, every hour

4. customer-360-aggregation (Snowflake → Snowflake)
   - Depends on: mysql-orders, stripe-charges, salesforce-customers
   - SQL transform: JOIN + aggregate into unified view
   - Daily at 2 AM (after all sources refresh)

Dependencies: 1→4, 2→4, 3→4 (fan-in pattern)
```

---

## Usage

```bash
# Chat
"design a pipeline architecture for merging 3 MySQL databases"

# API
POST /api/topology/design
{
  "description": "I need to merge orders from MySQL and Stripe into a unified view in Snowflake"
}

# CLI
python -m cli chat design a fan-in topology for my 3 data sources
```

---

## Principles

- **Agent proposes, human approves** — topology design follows the two-tier autonomy model
- **Idempotent by default** — merge over append, watermark-bounded extraction
- **Cleanup ownership is explicit** — every intermediate table has a defined cleanup owner
- **Run context flows downstream** — watermarks and metadata propagate through the chain
