# Scheduling

DAPOS supports three scheduling modes: cron-based, dependency-triggered, and event-driven. The scheduler runs as an async loop ticking every 60 seconds.

---

## Cron Scheduling

Standard 5-field cron expressions on the `schedule_cron` field:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-7, Sun=0 or 7)
│ │ │ │ │
* * * * *
```

### Examples

| Schedule | Cron |
|----------|------|
| Every hour | `0 * * * *` |
| Every 15 minutes | `*/15 * * * *` |
| Daily at 2 AM | `0 2 * * *` |
| Weekdays at 9 AM | `0 9 * * 1-5` |
| Every 6 hours | `0 */6 * * *` |

### Natural Language Parsing

The agent can parse natural language schedules:

```
"every hour at :15"       → "15 * * * *"
"weekdays 9am"            → "0 9 * * 1-5"
"twice daily"             → "0 0,12 * * *"
"every 30 minutes"        → "*/30 * * * *"
```

### Evaluation

Cron is evaluated relative to the **last successful run**, not wall clock. If the app restarts after downtime, it correctly identifies missed windows and triggers a catch-up run (not multiple).

---

## Dependency-Triggered Execution

Pipelines can depend on other pipelines. The scheduler uses **topological sort** to determine execution order.

### Declaring Dependencies

```
POST /api/dependencies
{
  "pipeline_id": "downstream-pipeline",
  "depends_on_pipeline_id": "upstream-pipeline",
  "dependency_type": "user_defined"
}
```

**Dependency types**:
- `user_defined` — explicitly declared by user
- `fk_inferred` — detected from foreign key relationships
- `agent_recommended` — suggested by the agent during topology design

### How It Works

1. Scheduler tick evaluates all active pipelines
2. Topological sort orders pipelines by dependency depth
3. Upstream pipelines run first
4. Downstream pipelines only trigger when **all upstream dependencies** have a recent successful run
5. Run context propagates downstream (watermarks, batch IDs, row counts)

### Run Context Propagation

When a downstream pipeline is triggered by an upstream completion:

- `triggered_by_run_id` and `triggered_by_pipeline_id` are set on the RunRecord
- 9 upstream template variables become available in hooks:
  - `{{upstream_pipeline_id}}`, `{{upstream_run_id}}`
  - `{{upstream_rows_extracted}}`, `{{upstream_watermark_after}}`
  - `{{upstream_target_schema}}`, `{{upstream_target_table}}`
  - `{{upstream_completed_at}}`

**View trigger chains**: `GET /api/runs/{run_id}/trigger-chain`

---

## Error Budget Awareness

The scheduler respects error budgets — if a pipeline's success rate drops below threshold (default 90% over 7 days), scheduling is **automatically paused**.

| Budget Status | Scheduler Behavior |
|---------------|--------------------|
| Healthy (> 90%) | Normal scheduling |
| Low (< 20% remaining) | Schedules but raises WARNING alert |
| Exhausted (escalated) | **Skips scheduling**, CRITICAL alert |

This prevents a broken pipeline from consuming resources repeatedly.

---

## Manual Triggers

Trigger a pipeline run immediately, outside its schedule:

```bash
# API
POST /api/pipelines/{id}/trigger

# CLI
python -m cli trigger demo-stripe-charges

# Chat
"trigger the stripe pipeline"
```

Manual runs are tagged with `run_mode=MANUAL` to distinguish from scheduled runs.

---

## Backfill

Replay a historical time window with bounded watermarks:

```
POST /api/pipelines/{id}/trigger?mode=backfill
```

Backfill runs are tagged with `run_mode=BACKFILL` and can specify custom watermark bounds to reprocess specific date ranges.

---

## Pause and Resume

```bash
# Pause — scheduler stops triggering, in-flight run completes
POST /api/pipelines/{id}/pause

# Resume — scheduler resumes, next cron window triggers
POST /api/pipelines/{id}/resume
```

Paused pipelines:
- Do not trigger on cron schedule
- Do not trigger from upstream dependency completion
- Can still be manually triggered
- Show as "paused" status in UI

---

## Concurrency Control

The scheduler limits parallel pipeline executions via `asyncio.Semaphore`:

```
MAX_CONCURRENT=10  # default, configurable via environment
```

When all slots are occupied, additional pipelines queue until a slot opens. This prevents resource exhaustion during burst scheduling windows.

---

## Viewing Schedule State

**UI**: Pipeline detail shows `schedule_cron`, next expected run, last run timestamp.

**API**: `GET /api/pipelines/{id}` includes schedule fields and dependency list.

**CLI**: `python -m cli pipelines get {name}` shows schedule and dependency info.
