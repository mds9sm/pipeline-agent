# Agent Overview

The AI agent is the core of DAPOS — it's not an add-on feature but the primary interface for operating the platform. The agent reasons about data pipelines using Claude, with all state stored in PostgreSQL.

---

## Two-Tier Autonomy

DAPOS enforces a strict boundary between autonomous and human-approved actions:

### Autonomous (Runtime Decisions)
The agent acts independently for:
- **Extract**: Pull data from sources
- **Load**: Stream to staging tables
- **Quality gate**: Evaluate 7 checks and decide promote/halt
- **Promote**: Merge staging to target
- **Cleanup**: Drop staging tables
- **Alerting**: Dispatch notifications
- **Scheduling**: Trigger runs based on cron/dependencies

### Human Approval Required (Structural Changes)
These always create proposals that require human review:
- **New connectors**: AI-generated source/target code
- **Schema changes**: Column additions, type changes, drops
- **Strategy changes**: Switching from full-refresh to incremental
- **Topology design**: Multi-pipeline architectures
- **Connector upgrades**: New versions of existing connectors

This boundary is a **hard constraint** — no configuration can make structural changes autonomous.

---

## How the Agent Works

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

| Operation | What It Does |
|-----------|--------------|
| `route_command` | Classify natural language intent → action |
| `propose_strategy` | Recommend ingestion strategy for a source |
| `generate_connector` | Write SourceEngine/TargetEngine code |
| `reason_about_quality` | Analyze quality gate results with context |
| `diagnose_pipeline` | Root-cause analysis for failing pipelines |
| `analyze_impact` | Downstream impact if a pipeline breaks |
| `reason_about_anomalies` | Platform-wide anomaly detection |
| `design_topology` | Multi-pipeline architecture from business description |
| `parse_schedule` | Natural language → cron expression |
| `guided_pipeline_response` | Multi-turn pipeline creation conversation |

### Rule-Based Fallbacks

When the Claude API key is not configured or the API is unavailable, every operation has a rule-based fallback:
- Strategy proposals use heuristics (table size, key presence)
- Diagnostics check error patterns, upstream status, budget pressure
- Impact analysis counts downstream pipelines and contracts
- Anomaly detection checks volume deviation and failure rates

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
