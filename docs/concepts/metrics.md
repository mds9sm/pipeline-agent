# Metrics & KPIs

DAPOS includes a lightweight metrics layer that lets you define, compute, and track KPIs on your pipeline data. The agent is at the heart of the system — it suggests metrics, generates SQL, and interprets trends.

## How It Works

1. **Agent suggests metrics** — Given a pipeline's schema, business context, and KPI definitions, the agent proposes relevant KPIs
2. **Agent generates SQL** — You describe what you want to measure in plain English; the agent writes the SQL expression
3. **Scheduled computation** — Metrics with a `schedule_cron` are automatically computed every 5 minutes by the observability loop
4. **Agent interprets trends** — The agent analyzes time-series snapshots and produces a narrative explaining direction, anomalies, and recommendations
5. **Per-metric reasoning** — Each metric carries a living reasoning document that the agent updates on creation, edits, and trend analysis

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
| `reasoning` | string | Current agent reasoning — why this metric matters, what it measures, latest insights |
| `reasoning_history` | list | Full history of reasoning updates with trigger, timestamp, author, and change summary |

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

## Per-Metric Reasoning (Build 32)

Every metric carries a **living reasoning document** that evolves with each interaction:

### How Reasoning Updates

| Trigger | When | What Happens |
|---------|------|-------------|
| `created` | Metric is first created | Agent explains why this metric matters and what it measures |
| `updated` | User or API updates metric fields | Agent re-reasons incorporating the change (e.g., "SQL was updated to filter by status") |
| `trend` | Trend analysis is requested | Agent updates reasoning with latest trend insights and anomaly context |
| `manual_edit` | User directly sets reasoning via PATCH | User-provided reasoning is stored as-is |

### Reasoning History

Every reasoning update is appended to `reasoning_history` with:
- `reasoning` — the full reasoning text
- `trigger` — what caused the update (created, updated, trend, manual_edit)
- `at` — ISO timestamp
- `by` — who triggered it (username or "agent")
- `change_summary` — what changed (for update triggers)

### API

- **Create** (`POST /api/metrics`): Pass `reasoning` to carry suggestion reasoning; otherwise agent generates it
- **Update** (`PATCH /api/metrics/{id}`): Agent auto-regenerates reasoning on meaningful changes; pass `reasoning` to override manually
- **Trend** (`GET /api/metrics/{id}/trend`): Automatically updates the metric's reasoning with trend insights
- **Detail** (`GET /api/metrics/{id}`): Returns `reasoning` and full `reasoning_history`

### Agent Method

```
agent.explain_metric(metric, trigger, change_summary, trend_context, pipeline_context)
```
Fallback: `_rule_based_explain_metric()` — template-based description.

### Business Knowledge Integration

When suggesting metrics, the agent incorporates:
- **KPI definitions** from the BusinessKnowledge entity (company-defined KPIs are prioritized)
- **Glossary terms** for domain-specific language understanding
- **Business context** from the pipeline's catalog entry
