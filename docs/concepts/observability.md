# Observability

DAPOS provides built-in observability across freshness monitoring, alerting, error budgets, cost tracking, and AI-powered anomaly detection — replacing external tools like Monte Carlo.

---

## Freshness Monitoring

Freshness tracks how stale each pipeline's data is relative to its schedule and SLA.

### How It Works

The **monitor loop** (every 5 minutes) checks each active pipeline:

1. Reads the last successful run's completion timestamp
2. Calculates `staleness_minutes = now - last_completed_at`
3. Compares against the pipeline's freshness SLA
4. Saves a `FreshnessSnapshot` record (rows accumulate for time-series)

### Freshness Status

| Status | Condition |
|--------|-----------|
| **FRESH** | staleness < SLA threshold |
| **WARNING** | staleness > warn threshold |
| **CRITICAL** | staleness > critical threshold |

### Per-Tier SLA Defaults

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

Alerts are dispatched through multiple channels when pipelines breach thresholds.

### Alert Severity

| Severity | When |
|----------|------|
| **INFO** | First-run warnings, minor schema additions |
| **WARNING** | Quality gate warnings, freshness approaching SLA, error budget < 20% |
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

1. Alert created with severity and detail
2. Dispatched to configured channels based on tier policy
3. Visible in UI Alerts tab and via `GET /api/alerts`
4. Acknowledged by operator (tracked with `acknowledged_by` and timestamp)

**API**:
- `GET /api/alerts` — list alerts with severity/pipeline filtering
- `POST /api/alerts/{id}/acknowledge` — mark as acknowledged

**CLI**: `python -m cli alerts [--limit 50]`

---

## Error Budgets

Error budgets track pipeline reliability over a rolling window and automatically pause scheduling when reliability drops too low.

### How It Works

Over a **7-day rolling window**:

```
success_rate = successful_runs / total_runs
budget_remaining = (success_rate - threshold) / (1 - threshold)
escalated = success_rate < threshold
```

**Default threshold**: 90% (configurable per-pipeline).

### Escalation

When `escalated = true` (success rate below threshold):
- **Scheduler stops triggering** the pipeline
- **CRITICAL alert** dispatched
- Pipeline requires manual intervention (fix root cause, then resume)

This prevents cascading failures — a repeatedly failing pipeline won't keep consuming resources.

### Example

| Metric | Value |
|--------|-------|
| Window | 7 days |
| Total runs | 42 |
| Successful | 36 |
| Failed | 6 |
| Success rate | 85.7% |
| Threshold | 90% |
| Budget remaining | -43% (exhausted) |
| Escalated | **true** |

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
| `operation` | e.g., `propose_strategy`, `generate_connector`, `diagnose_pipeline` |
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

DAPOS proactively scans for platform-wide anomalies every 15 minutes using AI reasoning.

### Pre-Filter (No LLM Cost if Healthy)

Before calling Claude, the system pre-filters all active pipelines for:

| Signal | Threshold |
|--------|-----------|
| Volume deviation | > 30% from 30-run average |
| Repeated failures | 2+ failures in 24 hours |
| Error budget pressure | Budget remaining < 5% |
| Freshness violation | Status = CRITICAL |

If **no anomalies detected**, the check short-circuits — no Claude API call, zero cost.

### LLM Reasoning (When Anomalies Found)

When anomalies are present, Claude receives:
- Anomalous pipeline signals with context
- Day-of-week patterns (weekend volume drops are normal)
- Pipeline tier and SLA expectations
- Historical patterns

Returns: anomaly descriptions, severity, and recommended actions (investigate, auto-remediate, skip).

**API**: `GET /api/observability/anomalies`

**CLI**: `python -m cli anomalies`

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
