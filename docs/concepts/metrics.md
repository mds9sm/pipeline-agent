# Metrics & KPIs

DAPOS includes a lightweight metrics layer that lets you define, compute, and track KPIs on your pipeline data. The agent is at the heart of the system — it suggests metrics, generates SQL, and interprets trends.

## How It Works

1. **Agent suggests metrics** — Given a pipeline's schema and business context, the agent proposes relevant KPIs (row counts, conversion rates, revenue sums, etc.)
2. **Agent generates SQL** — You describe what you want to measure in plain English; the agent writes the SQL expression
3. **Scheduled computation** — Metrics with a `schedule_cron` are automatically computed every 5 minutes by the observability loop
4. **Agent interprets trends** — The agent analyzes time-series snapshots and produces a narrative explaining direction, anomalies, and recommendations

## Data Model

### MetricDefinition

| Field | Type | Description |
|-------|------|-------------|
| `metric_id` | string | Unique identifier |
| `pipeline_id` | string | Pipeline whose target table this metric queries |
| `metric_name` | string | Human-readable name |
| `description` | string | What this metric measures |
| `sql_expression` | string | SQL query that returns a single numeric value |
| `metric_type` | enum | `count`, `sum`, `avg`, `ratio`, `custom` |
| `dimensions` | list | Optional grouping dimensions |
| `schedule_cron` | string | Cron expression for automatic computation |
| `tags` | dict | Arbitrary key-value tags |
| `enabled` | bool | Whether the metric is active |

### MetricSnapshot

Each computation produces a snapshot:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | string | Unique identifier |
| `metric_id` | string | Parent metric |
| `computed_at` | string | ISO timestamp |
| `value` | float | Computed numeric value |
| `dimension_values` | dict | Values for each dimension |
| `metadata` | dict | Execution metadata (elapsed_ms, source) |

## API Endpoints

### Suggest Metrics (Agentic)
```
POST /api/metrics/suggest/{pipeline_id}
```
Agent analyzes the pipeline's target schema and business context, then suggests 3-5 relevant metrics with rationale.

### Create Metric
```
POST /api/metrics
{
  "pipeline_id": "...",
  "metric_name": "daily_revenue",
  "description": "Total revenue from completed orders"
}
```
If `sql_expression` is omitted, the agent generates it from the description.

### List Metrics
```
GET /api/metrics?pipeline_id=...
```

### Get Metric Detail
```
GET /api/metrics/{metric_id}
```
Returns the metric definition plus recent snapshots.

### Compute Now
```
POST /api/metrics/{metric_id}/compute
```
Executes the metric SQL against the pipeline's target database and stores a snapshot.

### Trend Analysis (Agentic)
```
GET /api/metrics/{metric_id}/trend
```
Agent interprets the metric's time-series data and returns:
- `direction`: increasing, decreasing, stable, volatile
- `narrative`: plain-English explanation of the trend
- `anomalies`: any detected outliers
- `recommendation`: suggested actions

### Update / Delete
```
PATCH /api/metrics/{metric_id}
DELETE /api/metrics/{metric_id}
```

## Chat Integration

You can interact with metrics through the chat interface:

- "Suggest some KPI metrics for my pipeline" → triggers `suggest_metrics` action
- "What's the trend for my revenue metric?" → triggers `interpret_metric_trend` action

## Scheduled Computation

The observability loop (30s tick) computes scheduled metrics every 5 minutes. Any metric with `schedule_cron` set and `enabled: true` will be computed automatically.

## Agentic Architecture

All three core operations use agent reasoning via Claude API:

| Operation | Agentic Method | Rule-based Fallback |
|-----------|---------------|-------------------|
| Suggest metrics | `agent.suggest_metrics()` | `_rule_based_suggest_metrics()` |
| Generate SQL | `agent.generate_metric_sql()` | `_rule_based_generate_metric_sql()` |
| Interpret trend | `agent.interpret_metric_trend()` | `_rule_based_interpret_trend()` |

The rule-based fallbacks activate only when no API key is configured. They provide basic metric suggestions (row count, null rate, freshness) and simple trend direction without the agent's contextual understanding.
