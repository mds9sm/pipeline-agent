# Observability

DAPOS provides built-in observability across freshness monitoring, alerting, error budgets, cost tracking, and AI-powered anomaly detection — replacing external tools like Monte Carlo.

> **Agentic behavior**: Freshness severity, anomaly detection, error budget diagnosis, and contract violation assessment are all **agent-driven decisions**. The monitor and observability loops collect signals; the agent interprets them in context. Rule-based fallbacks exist for when the API key is unavailable and are explicitly marked as such.

---

## Freshness Monitoring

Freshness tracks how stale each pipeline's data is relative to its schedule and SLA.

### How It Works

The **monitor loop** (every 5 minutes) checks each active pipeline:

1. Reads the last successful run's completion timestamp
2. Calculates `staleness_minutes = now - last_completed_at`
3. Agent evaluates freshness context via `reason_about_freshness()`
4. Saves a `FreshnessSnapshot` record (rows accumulate for time-series)

### Agentic Freshness Evaluation

The agent receives staleness data along with context and reasons about:

| Factor | What the Agent Considers |
|--------|--------------------------|
| **SLA realism** | Is the freshness SLA achievable given the pipeline's schedule? (e.g., hourly SLA on a daily-scheduled pipeline is unrealistic) |
| **Tier context** | Tier 1 pipelines get urgent severity; Tier 3 gets informational |
| **Pattern recognition** | Is this pipeline always stale at this time of day? Weekend pattern? |
| **Alert necessity** | Should this actually trigger an alert, or is it expected? |

The agent returns severity (`fresh`, `warning`, `critical`), whether to alert, and reasoning.

### Rule-Based Fallback

> **⚠️ RULE-BASED**: When the API key is unavailable, freshness uses static tier-based thresholds:

| Tier | Warn | Critical |
|------|------|----------|
| Tier 1 (Production) | 15 minutes | 30 minutes |
| Tier 2 (Standard) | 120 minutes (2h) | 360 minutes (6h) |
| Tier 3 (Casual) | 1440 minutes (24h) | 4320 minutes (72h) |

Override per-pipeline via `freshness_sla_minutes` on the contract.

### Time-Series Charts

The UI Freshness tab renders staleness over time as line charts with SLA threshold reference lines. Data comes from accumulated snapshots.

**API**: `GET /api/observability/freshness/{pipeline_id}/history?hours=24`

---

## Alerts

Alerts are dispatched through multiple channels when the agent determines a condition warrants notification.

### Alert Severity

Alert severity is determined by the agent based on context, not fixed rules. General patterns:

| Severity | Typical Conditions |
|----------|-------------------|
| **INFO** | First-run warnings, minor schema additions |
| **WARNING** | Quality gate warnings, freshness approaching SLA, error budget pressure |
| **CRITICAL** | Pipeline halted, freshness SLA breached, error budget exhausted, contract violated |

### Dispatch Channels

| Channel | Configuration |
|---------|---------------|
| **Slack** | Webhook URL, channel name. Immediate for Tier 1-2. |
| **Email** | SMTP config, recipient list. Immediate or digest mode. |
| **PagerDuty** | Integration key, routing key. Tier 1 critical only. |

### Digest Mode

Tier 3 pipelines use **daily digest** (default 9 AM UTC) instead of immediate alerts. This prevents alert fatigue from low-priority pipelines.

### Alert Lifecycle

1. Alert created with agent-determined severity and detail (including reasoning)
2. Dispatched to configured channels based on tier policy
3. Visible in UI Alerts tab and via `GET /api/alerts`
4. Acknowledged by operator (tracked with `acknowledged_by` and timestamp)

**API**:
- `GET /api/alerts` — list alerts with severity/pipeline filtering
- `POST /api/alerts/{id}/acknowledge` — mark as acknowledged

**CLI**: `python -m cli alerts [--limit 50]`

---

## Error Budgets

Error budgets track pipeline reliability over a rolling window. When exhausted, the **agent diagnoses the failure pattern** and recommends recovery actions.

### How It Works

Over a **7-day rolling window**:

```
success_rate = successful_runs / total_runs
budget_remaining = (success_rate - threshold) / (1 - threshold)
escalated = success_rate < threshold
```

**Default threshold**: 90% (configurable per-pipeline).

### Agentic Error Budget Diagnosis

When the error budget is exhausted, the agent calls `diagnose_error_budget()` to:

| Analysis | What the Agent Does |
|----------|-------------------|
| **Pattern classification** | Identifies if failures are transient, persistent, or degrading |
| **Recovery recommendation** | Suggests specific actions (retry, investigate source, pause, escalate) |
| **Alert enrichment** | Includes diagnosis, pattern, and recommended_actions in the alert |
| **Pause decision** | Determines whether to actually pause scheduling based on pattern |

### Rule-Based Fallback

> **⚠️ RULE-BASED**: Without API key, error budget diagnosis uses keyword matching to classify failures as `transient` (timeout/connection keywords), `persistent` (auth/permission keywords), or `unknown`.

### Escalation

When `escalated = true` (success rate below threshold):
- Agent diagnoses the failure pattern
- **CRITICAL alert** dispatched with diagnosis
- **Scheduler stops triggering** the pipeline (if agent recommends pause)
- Pipeline requires manual intervention (fix root cause, then resume)

### Viewing

**UI**: Error budget cards shown on Pipeline detail with visual utilization bar (green → yellow → red).

**API**: Included in pipeline detail response under `error_budget`.

---

## Cost Tracking

Every LLM call made by the agent is logged with token counts and latency.

### What's Tracked

| Field | Description |
|-------|-------------|
| `pipeline_id` | Which pipeline triggered the call (if applicable) |
| `operation` | e.g., `propose_strategy`, `generate_connector`, `decide_quality_gate` |
| `model` | Claude model used |
| `input_tokens` | Prompt tokens consumed |
| `output_tokens` | Response tokens generated |
| `latency_ms` | API round-trip time |

### Viewing Costs

**UI**: Costs tab shows per-operation breakdown with totals.

**API**:
- `GET /api/costs` — detailed operation log
- `GET /api/costs/summary` — aggregated by operation type

---

## Anomaly Detection

DAPOS proactively scans for platform-wide anomalies every 15 minutes using **per-pipeline agentic evaluation**.

### How It Works

For each active pipeline, the agent calls `evaluate_anomaly_signals()` which:

1. Gathers signals: volume deviation, failure count, error budget pressure, freshness status
2. The **agent evaluates each pipeline's signals in context** — considering tier, schedule, day-of-week patterns, and historical norms
3. Returns whether the pipeline is anomalous, severity, and reasoning
4. After per-pipeline evaluation, the agent performs **cross-pipeline pattern analysis** (e.g., multiple pipelines from the same source failing = source issue, not pipeline issues)

### Agent Context for Anomaly Evaluation

| Signal | What the Agent Considers |
|--------|--------------------------|
| Volume deviation | Is this a weekend/holiday pattern? Is the source known to have variable volume? |
| Repeated failures | Are these transient (timeouts) or persistent (schema change)? |
| Error budget pressure | Is the pipeline degrading or recovering? |
| Freshness violation | Is the SLA realistic for the schedule? |
| Cross-pipeline correlation | Are multiple pipelines from the same source affected? |

### Rule-Based Fallback

> **⚠️ RULE-BASED**: Without API key, anomaly detection falls back to `_rule_based_anomaly_evaluation()` with fixed thresholds:
>
> | Signal | Threshold |
> |--------|-----------|
> | Volume deviation | > 30% from 30-run average |
> | Repeated failures | 2+ failures in 24 hours |
> | Error budget pressure | Budget remaining < 5% |
> | Freshness violation | Status = CRITICAL |
>
> If **no signals trigger** → short-circuit, no anomaly reported.

### CRITICAL Alerts

When the agent finds unexpected anomalies (not explained by known patterns), it automatically creates CRITICAL alerts with agent reasoning included in the alert detail.

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

---

## Anomaly Narratives

When an alert fires, you can generate a human-readable narrative that explains the alert in context. Claude analyzes the alert details alongside recent runs, downstream dependencies, freshness state, and pipeline tier to produce actionable prose.

### Generating a Narrative

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/observability/alerts/{alert_id}/narrative"
```

Returns:
```json
{
  "alert_id": "alert-789",
  "narrative": "The demo-ecommerce-orders pipeline has been failing for the last 2 runs due to a connection timeout to the source MySQL database. This is a Tier 1 pipeline with 3 downstream consumers. Current staleness is 45 minutes, exceeding the 30-minute SLA. Recommended action: check MySQL connectivity and disk space on demo-mysql.",
  "pipeline_name": "demo-ecommerce-orders",
  "severity": "critical"
}
```

### Context Used

The narrative draws from:
- Alert summary, detail, and severity
- Pipeline tier and schedule
- Downstream dependency count
- Last 3 run errors
- Current freshness staleness and status

### Cost

Each narrative is one Claude API call, logged under `generate_anomaly_narrative`. The narrative is saved on the alert record, so subsequent reads do not require regeneration.

Rate-limited to 10 requests per minute.

For a full deep dive, see [Anomaly Narratives](anomaly-narratives.md).

---

## Notification Policies

Configure per-tier alert dispatch rules:

```
POST /api/policies
{
  "policy_name": "tier-1-critical",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "..."}},
    {"type": "pagerduty", "config": {"integration_key": "..."}}
  ],
  "digest_hour": null  // immediate
}
```

**API**:
- `GET /api/policies` — list policies
- `POST /api/policies` — create policy
- `PATCH /api/policies/{id}` — update
- `DELETE /api/policies/{id}` — remove
