# Anomaly Narratives

Anomaly narratives turn raw alerts into human-readable explanations. When an alert fires, Claude analyzes the alert context, recent run history, downstream impact, and freshness state to produce a plain-English narrative describing what happened, why it matters, and what to do next.

---

## How It Works

When you request a narrative for an alert, the system:

1. **Loads alert details** — severity, summary text, pipeline name, tier
2. **Gathers pipeline context** — the pipeline contract, if the alert is pipeline-specific
3. **Checks downstream impact** — how many pipelines depend on this one
4. **Pulls recent run errors** — last 3 runs, extracting any error messages
5. **Reads freshness state** — current staleness and SLA status
6. **Sends to Claude** — all context is composed into a prompt that asks for a narrative

Claude returns a narrative that includes:
- **What happened** — the specific failure or threshold breach
- **Why it matters** — downstream impact, SLA implications, tier significance
- **What to do** — concrete recommended actions

The narrative is saved on the alert object so it does not need to be regenerated.

---

## API

### Generate a narrative

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/observability/alerts/{alert_id}/narrative"
```

**Response:**

```json
{
  "alert_id": "alert-789",
  "narrative": "The demo-ecommerce-orders pipeline has been failing for the last 2 runs due to a connection timeout to the source MySQL database. This is a Tier 1 pipeline with 3 downstream consumers including the daily revenue report. Current staleness is 45 minutes, exceeding the 30-minute critical SLA. Recommended action: check MySQL connectivity and disk space on demo-mysql, then manually trigger a re-run.",
  "pipeline_name": "demo-ecommerce-orders",
  "severity": "critical"
}
```

**Rate limit:** 10 requests per minute.

**Role:** Any authenticated user (viewer+).

---

## Context Sent to Claude

The narrative prompt includes:

| Signal | Source |
|--------|--------|
| Pipeline name | Alert record |
| Alert summary and detail | Alert record |
| Severity (info/warning/critical) | Alert record |
| Observability tier (1/2/3) | Alert record |
| Downstream pipeline count | `list_dependents()` |
| Recent run errors (last 3 runs) | `list_runs()` |
| Freshness staleness and status | `get_latest_freshness()` |
| Schedule (cron expression) | Pipeline contract |

Claude is instructed to write for a data engineer audience: concise, actionable, and focused on the specific alert rather than generic advice.

---

## Relationship to Anomaly Detection

Anomaly narratives and anomaly detection are complementary but separate:

| Feature | Anomaly Detection | Alert Narratives |
|---------|-------------------|------------------|
| Trigger | Proactive (every 15 min) | On-demand (per alert) |
| Scope | Platform-wide scan | Single alert |
| Cost | Zero if healthy (pre-filter) | 1 Claude call per request |
| Output | Anomaly list + severity + actions | Prose narrative |
| Storage | Cached in observability loop | Saved on alert record |
| API | `GET /api/observability/anomalies` | `POST /api/observability/alerts/{id}/narrative` |

Anomaly detection finds problems proactively. Alert narratives explain specific alerts in detail. Both use Claude for reasoning, but narratives are more focused and always involve an LLM call.

---

## Narrative Quality

Narratives are most useful when the system has rich context:

- **Pipelines with multiple runs** provide error patterns and volume trends
- **Pipelines with freshness SLAs** enable staleness impact statements
- **Pipelines with downstream dependencies** allow blast radius assessment
- **Tier 1 pipelines** receive more urgent language in narratives

For new pipelines with minimal history, narratives will be shorter and more general.

---

## Cost

Each narrative generation makes one Claude API call. The call is logged in the cost tracker with operation `generate_anomaly_narrative`. Narratives are saved on the alert, so regeneration is optional — you only pay for the initial generation or explicit re-generation.
