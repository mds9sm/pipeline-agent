# Diagnostics

The agent diagnostic layer lets DAPOS reason about why pipelines fail, what breaks if a pipeline goes down, and whether there are platform-wide anomalies — capabilities that traditional tools like Fivetran, Airflow, or Monte Carlo cannot provide without an LLM reasoning layer.

---

## Pipeline Diagnosis

**"Why is my pipeline failing?"**

The agent gathers 10 data sources and reasons about root cause:

### Data Gathered
1. Last 10 runs (status, errors, duration)
2. Quality gate trend (recent pass/warn/fail history)
3. Error budget (success rate, escalation status)
4. Upstream pipelines and their recent run health
5. Source connector status (last test result)
6. Active alerts for this pipeline
7. Volume history (row counts over recent runs)
8. Data contracts (any violations?)
9. Schema change history (recent drift?)
10. Pipeline configuration (schedule, strategy, tier)

### Root Cause Categories
| Category | Example |
|----------|---------|
| `source_issue` | Source database offline or credentials expired |
| `connector_issue` | Connector code bug or timeout |
| `upstream_dependency` | Upstream pipeline failing, blocking this one |
| `quality_regression` | Data quality degrading (null rate spike, volume drop) |
| `scheduling` | Missed schedule windows, error budget exhausted |
| `configuration` | Bad merge keys, wrong schema, stale watermark |
| `data_issue` | Source data changed unexpectedly |
| `unknown` | No clear pattern — needs manual investigation |

### Response Format
```json
{
  "root_cause": "upstream_dependency",
  "confidence": 0.85,
  "summary": "Pipeline demo-daily-agg is halted because upstream demo-raw-orders has failed 3 of its last 5 runs due to source MySQL connection timeouts.",
  "symptoms": [
    "3 consecutive HALTED runs",
    "Upstream demo-raw-orders: 60% failure rate",
    "Source MySQL connection test failed 2h ago"
  ],
  "recommended_actions": [
    "Check MySQL source connectivity",
    "Review demo-raw-orders error logs",
    "Consider increasing connection timeout"
  ],
  "pattern": "cascading_failure"
}
```

### Usage

```bash
# API
POST /api/pipelines/{id}/diagnose

# CLI
python -m cli diagnose demo-stripe-charges

# Chat
"why is my orders pipeline failing"
"diagnose stripe charges"
```

---

## Impact Analysis

**"What breaks if this pipeline goes down?"**

The agent traces downstream dependencies recursively to estimate blast radius.

### What's Analyzed
- All downstream pipelines (recursive BFS, max depth 10)
- Column-level lineage (which specific columns are affected)
- Active data contracts (SLA commitments at risk)
- Downstream pipeline tiers (Tier 1 impact is worse than Tier 3)

### Impact Severity

| Level | Criteria |
|-------|----------|
| `low` | 0-1 downstream pipelines, all Tier 3 |
| `medium` | 2-5 downstream, no Tier 1 |
| `high` | 5+ downstream or any Tier 1 affected |
| `critical` | Tier 1 pipeline with active data contracts violated |

### Response Format
```json
{
  "impact_severity": "high",
  "affected_pipelines": [
    {
      "pipeline_id": "daily-aggregates",
      "pipeline_name": "daily-aggregates",
      "tier": 1,
      "depth": 1,
      "affected_columns": ["order_id", "amount"]
    }
  ],
  "contracts_at_risk": [
    {
      "contract_id": "...",
      "consumer": "daily-aggregates",
      "freshness_sla_minutes": 30
    }
  ],
  "recommended_actions": [
    "Prioritize fix — Tier 1 downstream affected",
    "Notify daily-aggregates owner",
    "Check if data contract SLA will be breached"
  ]
}
```

### Usage

```bash
# API
POST /api/pipelines/{id}/impact

# CLI
python -m cli impact demo-ecommerce-orders

# Chat
"what breaks if stripe goes down"
"impact analysis for orders pipeline"
```

---

## Anomaly Detection

**"Is anything unusual happening across the platform?"**

Runs proactively every 15 minutes. Smart cost optimization ensures Claude is only called when anomalies exist.

### Pre-Filter (Zero Cost When Healthy)

Before calling Claude, the system scans all active pipelines for:

| Signal | Threshold |
|--------|-----------|
| Volume anomaly | > 30% deviation from 30-run average |
| Repeated failures | 2+ failures in last 24 hours |
| Error budget pressure | < 5% budget remaining |
| Freshness critical | Status = CRITICAL |

If **nothing anomalous** is detected → short-circuit, no LLM call, zero cost.

### LLM Reasoning (When Anomalies Found)

Claude receives anomaly signals with context and considers:
- Day-of-week patterns (weekend volume drops are expected)
- Pipeline tier (Tier 1 anomalies are more urgent)
- Correlated failures (multiple pipelines from same source = source issue)
- Historical patterns (this pipeline always drops on Sundays)

### Response
```json
{
  "status": "anomalies_detected",
  "anomalies": [
    {
      "pipeline": "demo-stripe-charges",
      "signal": "volume_drop",
      "detail": "Row count dropped 65% vs 30-run average",
      "severity": "warning",
      "recommendation": "Investigate — unlikely to be day-of-week pattern for Stripe charges"
    }
  ],
  "summary": "1 anomaly detected: volume drop on demo-stripe-charges"
}
```

### CRITICAL Alerts

When the proactive scan finds unexpected anomalies, it automatically creates CRITICAL alerts that are dispatched through configured notification channels.

### Usage

```bash
# API
GET /api/observability/anomalies

# CLI
python -m cli anomalies

# Chat
"are there any anomalies"
"platform health check"
```
