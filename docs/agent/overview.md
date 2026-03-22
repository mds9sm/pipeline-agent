# Agent Overview

The AI agent is the core of DAPOS — it's not an add-on feature but the primary interface for operating the platform. The agent reasons about data pipelines using Claude, with all state stored in PostgreSQL.

> **Agentic-first principle**: Every decision in DAPOS is made by the agent using contextual reasoning. Rule-based logic exists **only** as a fallback when the Claude API is unavailable, and is explicitly marked with `_rule_based_*` method naming. See [Rule-Based Fallbacks](#rule-based-fallbacks) for the complete list.

---

## Two-Tier Autonomy

DAPOS enforces a strict boundary between autonomous and human-approved actions:

### Autonomous (Runtime Decisions)
The agent acts independently for:
- **Extract**: Pull data from sources
- **Load**: Stream to staging tables
- **Pre-extract schema check**: Detect drift before extraction, generate migration SQL
- **Quality gate**: Evaluate 7 check signals and decide promote/halt with reasoning
- **Promote**: Merge staging to target
- **Cleanup**: Drop staging tables
- **Alerting**: Dispatch notifications with agent-determined severity
- **Scheduling**: Trigger runs based on cron/dependencies
- **Failure diagnosis**: Classify run failures, determine transience, enrich error messages
- **Error budget diagnosis**: Analyze failure patterns, recommend recovery actions
- **Freshness evaluation**: Assess SLA realism, determine alert necessity
- **Anomaly detection**: Per-pipeline contextual evaluation + cross-pipeline pattern analysis
- **Contract violation assessment**: Evaluate consumer impact, determine severity

### Human Approval Required (Structural Changes)
These always create proposals that require human review:
- **New connectors**: AI-generated source/target code
- **Schema changes**: Agent-generated migration SQL (via propose flow)
- **Strategy changes**: Switching from full-refresh to incremental
- **Topology design**: Multi-pipeline architectures
- **Connector upgrades**: New versions of existing connectors

This boundary is a **hard constraint** — no configuration can make structural changes autonomous.

---

## How the Agent Works

### System Prompt (Platform Context)

Every Claude API call includes a rich system prompt (`_SYSTEM_PROMPT` in `agent/core.py`, ~1,250 tokens) that gives the agent full platform awareness:

- **Identity**: DAPOS as a unified data platform replacing Fivetran + dbt + Airflow + Monte Carlo
- **Architecture**: Single process, 4 async loops, PostgreSQL-only state
- **Execution flow**: Full state machine (PENDING → extract → stage → gate → promote → COMPLETE)
- **Two-tier autonomy**: What the agent can do autonomously vs what requires human approval
- **Key concepts**: Connectors, refresh/load types, tiers, error budgets, contracts, schema drift policies, hooks, steps, transforms
- **Quality gate**: All 7 checks and the agent's role as decision-maker
- **Data patterns**: 8 supported patterns (consume-and-merge, fan-in, SCD2, quarantine, etc.)
- **Decision principles**: Idempotent-by-default, never delete unconsumed data, conservative on quality, context over thresholds, downstream awareness, explain reasoning

This ensures the agent reasons with full context on every call, not just the narrow task prompt.

### No External Dependencies
- **No LangChain** — direct Claude API via httpx
- **No external vector DB** — PostgreSQL + pgvector for embeddings
- **No memory cache** — all state in PostgreSQL
- **No Airflow/dbt** — native scheduling and transforms

### Cost Tracking
Every LLM call is logged with:
- Input/output token counts
- Latency in milliseconds
- Which pipeline and operation triggered the call
- Which model was used

View costs at `GET /api/costs` or in the UI Costs tab.

### Learning from Decisions
The agent learns from human approval/rejection patterns:

- **Approved strategies** increase confidence for similar future proposals
- **Rejected strategies** reduce confidence and record the rejection reason
- Preferences are scoped (global, per-source-type, per-pipeline)
- Sources: USER_EXPLICIT, REJECTION_INFERRED, APPROVAL_PATTERN

---

## Agent Operations

### Agentic Decision Methods

These methods use Claude to reason about signals in context. Each has a `_rule_based_*` fallback.

| Operation | What It Does | Fallback |
|-----------|--------------|----------|
| `decide_quality_gate()` | Evaluates 7 check signals with pipeline context → PROMOTE/HALT decision with reasoning | `_fallback_decision()` — threshold-based |
| `diagnose_run_failure()` | Classifies failures (connector/source/target/network/schema/config/resource), determines transience | `_rule_based_failure_diagnosis()` — keyword matching |
| `diagnose_error_budget()` | Analyzes failure pattern (transient/persistent/degrading), recommends recovery | `_rule_based_budget_diagnosis()` — keyword matching |
| `reason_about_freshness()` | Evaluates SLA realism, determines severity, decides whether to alert | `_rule_based_freshness()` — tier-based thresholds |
| `evaluate_anomaly_signals()` | Per-pipeline contextual anomaly evaluation with cross-pipeline patterns | `_rule_based_anomaly_evaluation()` — fixed thresholds |
| `assess_contract_violation()` | Evaluates consumer impact, determines alert severity | Rule-based: hardcoded WARNING severity |
| `reason_about_preflight_failure()` | Reasons about why preflight checks failed, recommends action | Rule-based: generic message |
| `generate_migration_sql()` | Generates ALTER TABLE SQL for schema drift with reasoning | `_rule_based_migration_sql()` — template-based ALTER |
| `generate_run_insights()` | Post-run analysis: 2-5 contextual suggestions with optional one-click actions | `_rule_based_run_insights()` — condition-based checks |

### Conversational & Design Methods

| Operation | What It Does |
|-----------|--------------|
| `route_command` | Classify natural language intent → action |
| `propose_strategy` | Recommend ingestion strategy for a source |
| `generate_connector` | Write SourceEngine/TargetEngine code |
| `diagnose_pipeline` | Root-cause analysis for failing pipelines |
| `analyze_impact` | Downstream impact if a pipeline breaks |
| `reason_about_anomalies` | Platform-wide anomaly detection (orchestrates per-pipeline evaluation) |
| `design_topology` | Multi-pipeline architecture from business description |
| `parse_schedule` | Natural language → cron expression |
| `guided_pipeline_response` | Multi-turn pipeline creation conversation |
| `generate_digest` | Daily alert digest summarization |

---

## Rule-Based Fallbacks

When the Claude API key is not configured or the API is unavailable, every agentic decision method has a rule-based fallback. These are explicitly marked:

> **⚠️ Convention**: All rule-based fallback methods are named `_rule_based_*()` (in `agent/core.py`) or `_fallback_*()` (in `quality/gate.py`). This naming convention is a **hard requirement** — any non-agentic decision logic must follow this pattern so it can be identified and audited.

| Agentic Method | Fallback Method | Fallback Behavior |
|----------------|-----------------|-------------------|
| `decide_quality_gate()` | `_fallback_decision()` | Any FAIL → HALT; WARN + promote_on_warn → PROMOTE_WITH_WARNING |
| `diagnose_run_failure()` | `_rule_based_failure_diagnosis()` | Keyword matching (timeout → transient, auth → persistent) |
| `diagnose_error_budget()` | `_rule_based_budget_diagnosis()` | Keyword matching for pattern classification |
| `reason_about_freshness()` | `_rule_based_freshness()` | Tier-based static thresholds (15m/2h/24h warn) |
| `evaluate_anomaly_signals()` | `_rule_based_anomaly_evaluation()` | Fixed thresholds (30% volume, 2+ failures, 5% budget) |
| `assess_contract_violation()` | *(inline)* | Hardcoded WARNING severity |
| `generate_migration_sql()` | `_rule_based_migration_sql()` | Template-based ALTER TABLE statements |
| `generate_run_insights()` | `_rule_based_run_insights()` | Condition-based: first-run baseline, strategy mismatch, volume deviation, consecutive failures |

---

## Command Routing

When a user sends text to the chat interface, `route_command` classifies intent:

### Keyword Patterns
| Keywords | Action |
|----------|--------|
| "diagnose", "why failing", "root cause" | `diagnose_pipeline` |
| "impact", "what breaks", "blast radius" | `analyze_impact` |
| "anomaly", "unusual", "platform health" | `check_anomalies` |
| "design", "topology", "architecture" | `design_topology` |
| "list pipelines", "show active" | List pipelines |
| "trigger", "run" | Trigger pipeline |
| "create pipeline", "set up" | Pipeline creation flow |

### LLM Classification
If keywords don't match, Claude classifies the intent from a predefined action list and extracts parameters.

---

## Conversation Flow

For multi-step operations like pipeline creation, the agent uses a stateless conversation flow:

1. **Test connection** — verify source is reachable
2. **Discover schemas** — list available tables
3. **Profile table** — analyze structure, row count, keys
4. **Propose strategy** — recommend refresh type, merge keys, schedule
5. **Create pipeline** — finalize with human confirmation

Each step is a separate API call. The UI manages session state in `sessionStorage`.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/command` | Route natural language command |
| POST | `/api/topology/design` | Design pipeline topology |
| GET | `/api/costs` | Agent LLM cost logs |
| GET | `/api/costs/summary` | Aggregated costs |
| GET | `/api/preferences` | Learned preferences |
| GET | `/api/interactions` | Chat interaction audit log |
| GET | `/api/interactions/export` | Export interaction history |
