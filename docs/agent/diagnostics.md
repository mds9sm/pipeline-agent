# Diagnostics

The agent diagnostic layer lets DAPOS reason about why pipelines fail, what breaks if a pipeline goes down, and whether there are platform-wide anomalies — capabilities that traditional tools like Fivetran, Airflow, or Monte Carlo cannot provide without an LLM reasoning layer.

> **Agentic behavior**: All diagnostic decisions are made by the agent with contextual reasoning. Rule-based fallbacks are explicitly marked. See each section for fallback details.

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

## Run Failure Diagnosis

**Agentic**: When a pipeline run fails, the agent automatically classifies the failure via `diagnose_run_failure()`.

### What the Agent Does

The agent receives the error message, pipeline context, and recent run history, then:

1. **Classifies the failure** into a category: `connector_bug`, `source_unavailable`, `target_unavailable`, `network_error`, `schema_mismatch`, `configuration_error`, `resource_exhaustion`, or `unknown`
2. **Determines transience** — is this likely to succeed on retry?
3. **Decides whether to alert** — not every failure warrants a notification
4. **Enriches the error message** — the run record includes the agent's classification and reasoning

### Rule-Based Fallback

> **⚠️ RULE-BASED**: `_rule_based_failure_diagnosis()` uses keyword matching:
> - Timeout/connection/refused → `network_error`, transient
> - Auth/permission/denied → `configuration_error`, not transient
> - Column/type/schema → `schema_mismatch`, not transient
> - Default → `unknown`, not transient

---

## Preflight Failure Reasoning

**Agentic**: When preflight checks fail (missing connector, inactive pipeline, etc.), the agent calls `reason_about_preflight_failure()` to explain why and recommend action.

The agent receives the list of failure reasons with context (e.g., which connector is missing, what the pipeline status is) and produces actionable guidance.

---

## Impact Analysis

**"What breaks if this pipeline goes down?"**

The agent traces downstream dependencies recursively to estimate blast radius.

### What's Analyzed
- All downstream pipelines (recursive BFS, max depth 10)
- Column-level lineage (which specific columns are affected)
- Active data contracts (SLA commitments at risk)
- Downstream pipeline tiers (Tier 1 impact is worse than Tier 3)

### Agent-Determined Severity

The agent evaluates impact severity based on the full context — number of downstream pipelines, their tiers, active contracts, and current health state. The agent considers whether downstream pipelines have alternative data sources, what the business impact of staleness would be, and whether contracts are at risk.

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

Runs proactively every 15 minutes. Uses **per-pipeline agentic evaluation** for contextual anomaly detection.

### How It Works

1. For each active pipeline, gather signals: volume deviation, failure count, error budget, freshness
2. **Agent evaluates each pipeline individually** via `evaluate_anomaly_signals()` — considering tier, schedule, day-of-week, historical patterns
3. After per-pipeline evaluation, agent performs **cross-pipeline pattern analysis** — correlating failures across pipelines sharing sources, identifying platform-wide issues vs. isolated problems
4. Returns anomaly list with severity, reasoning, and whether each is expected

### What the Agent Considers Per Pipeline

| Signal | Agent Reasoning |
|--------|----------------|
| Volume drop | Is this a weekend/holiday pattern? Source-specific seasonality? |
| Repeated failures | Transient (timeouts) or persistent (schema change)? Correlated with upstream? |
| Error budget pressure | Degrading trend or recovering? Recent fixes deployed? |
| Freshness violation | Is the SLA realistic for this schedule? Expected maintenance window? |

### Cross-Pipeline Patterns

The agent identifies:
- **Source correlation**: Multiple pipelines from the same source failing → source issue, not pipeline issues
- **Cascading failures**: Upstream failure causing downstream halts
- **Platform-wide events**: Infrastructure issues affecting all pipelines

### Rule-Based Fallback

> **⚠️ RULE-BASED**: `_rule_based_anomaly_evaluation()` uses fixed thresholds:
>
> | Signal | Threshold |
> |--------|-----------|
> | Volume deviation | > 30% from 30-run average |
> | Repeated failures | 2+ failures in 24 hours |
> | Error budget pressure | Budget remaining < 5% |
> | Freshness violation | Status = CRITICAL |
>
> No contextual reasoning — signals are evaluated independently with no cross-pipeline analysis.

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

## Error Budget Diagnosis

**Agentic**: When a pipeline's error budget is exhausted, the agent calls `diagnose_error_budget()` to analyze the failure pattern.

### What the Agent Returns

| Field | Description |
|-------|-------------|
| `pattern` | `transient`, `persistent`, or `degrading` |
| `diagnosis` | Natural language explanation of what's happening |
| `recommended_actions` | Specific steps to recover |
| `should_pause` | Whether to stop scheduling this pipeline |

### Rule-Based Fallback

> **⚠️ RULE-BASED**: `_rule_based_budget_diagnosis()` classifies by scanning error messages for keywords:
> - Timeout/connection → `transient`, recommend retry with backoff
> - Auth/permission → `persistent`, recommend credential check
> - Default → `unknown`, recommend manual investigation

---

## Contract Violation Assessment

**Agentic**: When a data contract is violated (freshness SLA breach, schema incompatibility), the agent calls `assess_contract_violation()` to evaluate actual impact.

The agent considers:
- How many consumers are affected
- Whether consumers have alternative data sources
- The business criticality of the affected data
- Whether the violation is temporary or structural

Returns severity (INFO/WARNING/CRITICAL) and impact assessment narrative.

### Rule-Based Fallback

> **⚠️ RULE-BASED**: Without API key, all contract violations are assigned WARNING severity with a generic message. No impact assessment is performed.
