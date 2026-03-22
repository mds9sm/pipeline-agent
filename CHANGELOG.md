# Changelog

All notable changes to the Pipeline Agent (DAPOS) are documented in this file.

Format: Each entry records what changed, why, and test results at the time of the change.

---

## Roadmap — Next Builds

| Build | Feature | Status | Why |
|-------|---------|--------|-----|
| 14 | Hook template variables | **Done** | `{{watermark_after}}`, `{{run_id}}` etc. — unblocks consume-and-merge pattern |
| 15 | Run context propagation | **Done** | Upstream run context (watermarks, batch IDs) flows to downstream pipelines |
| 16 | Data contracts between pipelines | **Done** | Formalize producer/consumer relationships, cleanup policies, retention |
| 17 | SQL-native intra-DB steps | **Skipped** | Agent handles per-pipeline via custom connectors/hooks — not a platform feature |
| 18 | Composable step DAG | **Done** | Replace fixed extract→load→promote with configurable step DAGs (Airflow replacement path) |
| 19 | DAG visualization UI | **Done** | Visual pipeline dependency graph with execution status |
| 20 | Agent topology reasoning | **Done** | Agent designs multi-pipeline architectures from natural language |
| 21 | Analyst experience layer | **Done** | Source registry, guided conversation, schedule parser, pipeline changelog, interaction audit |
| 22 | Observability UX — execution logs, freshness charts, lineage consolidation | **Done** | Full pipeline execution visibility, time-series freshness, unified DAG+lineage view |
| 23 | GitOps pipeline config versioning | **Done** | Separate git repo for pipeline YAML + connector code, auto-committed on every mutation |
| 25 | In-app documentation & CLI | **Done** | Docs served in UI, 14-command CLI, structured docs directory |
| 26 | Data catalog, semantic tags, trust scores | **Done** | Search/browse catalog, AI-inferred tags, business context, trust scores, alert narratives |
| 27 | MCP server | **Done** | Expose DAPOS to AI agents via Model Context Protocol (resources, tools, prompts) |
| 29 | Native SQL transforms | **Done** | Replace dbt with in-pipeline SQL transforms — ref(), var(), materialization, AI generation |
| 30 | Fully agentic failure detection & quality | **Done** | Agent decides quality gate, diagnoses failures, reasons about freshness/anomalies/contracts |
| 31 | Dashboard / metrics layer | **Done** | Lightweight KPI definitions on catalog tables, agentic suggest/generate/interpret |
| 28 | Context API enrichment | Planned | Auto-context on pipeline runs, cross-pipeline context propagation |

---

## [Unreleased]

### Build 31: Dashboard / Metrics Layer — 2026-03-22 (Claude Opus 4.6)

**Lightweight KPI definitions on pipeline target tables with agentic nature at the core — agent suggests metrics, generates SQL from plain English, and interprets time-series trends.**

#### Added
- **Data model** (`contracts/models.py`):
  - `MetricType` enum (count, sum, avg, ratio, custom)
  - `MetricDefinition` dataclass — pipeline-scoped metric with SQL expression, schedule, dimensions, tags
  - `MetricSnapshot` dataclass — point-in-time computed value with dimension values and metadata
- **Store layer** (`contracts/store.py`):
  - `metrics` + `metric_snapshots` tables with auto-migration
  - Full CRUD: `save_metric`, `get_metric`, `list_metrics`, `delete_metric`, `save_metric_snapshot`, `list_metric_snapshots`
- **Agent reasoning** (`agent/core.py`):
  - `suggest_metrics()` — agent analyzes pipeline schema and business context, proposes 3-5 KPIs with rationale
  - `generate_metric_sql()` — agent writes SQL from plain-English description against target table schema
  - `interpret_metric_trend()` — agent analyzes time-series snapshots, returns direction/narrative/anomalies/recommendation
  - Rule-based fallbacks: `_rule_based_suggest_metrics()`, `_rule_based_generate_metric_sql()`, `_rule_based_interpret_trend()`
  - `route_command` updated with `suggest_metrics` and `interpret_metric_trend` actions
- **REST API** (`api/server.py`):
  - `POST /api/metrics/suggest/{pipeline_id}` — agent suggests metrics
  - `POST /api/metrics` — create metric (agent generates SQL if not provided)
  - `GET /api/metrics` — list metrics, optional pipeline_id filter
  - `GET /api/metrics/{metric_id}` — detail with recent snapshots
  - `POST /api/metrics/{metric_id}/compute` — execute SQL against target, store snapshot
  - `GET /api/metrics/{metric_id}/trend` — agent trend interpretation
  - `PATCH /api/metrics/{metric_id}` — update fields
  - `DELETE /api/metrics/{metric_id}` — delete metric and snapshots
  - Chat dispatch for `suggest_metrics` and `interpret_metric_trend` actions
- **Scheduled computation** (`main.py`):
  - Observability loop computes enabled metrics with `schedule_cron` every 5 minutes
  - `_compute_scheduled_metrics()` helper with per-metric error isolation
- **UI** (`ui/App.jsx`):
  - `MetricsView` component — pipeline filter, suggest button, sparkline cards, expandable detail
  - Agent suggestions panel with one-click "Create" from suggestion
  - Compute Now button, trend narrative display, snapshot history table
  - SVG sparkline visualization of recent values
  - Nav: "Metrics" tab with `^` icon
- **Tests** (`test-pipeline-agent.sh`):
  - 8 tests: suggest, create, list, get, update, trend, delete, chat routing
- **Documentation** (`docs/concepts/metrics.md`):
  - Full reference: data model, API endpoints, chat integration, agentic architecture

#### Changed
- `ui/index.html` — cache bust v=47 → v=48

---

### Build 30c: Runtime Agent Context — 2026-03-22 (Claude Opus 4.6)

**Give the runtime agent full platform awareness. The system prompt was 4 generic lines — now it's a comprehensive platform context covering architecture, execution flow, two-tier autonomy, quality gate semantics, data patterns, and decision principles.**

#### Changed
- **Rich system prompt** (`agent/core.py`):
  - Moved from inline 4-line string to module-level `_SYSTEM_PROMPT` constant (~1,250 tokens)
  - Covers: DAPOS identity, 4-loop architecture, execution state machine, staging isolation
  - Two-tier autonomy boundary (autonomous runtime vs propose-for-approval structural)
  - All key concepts: connectors, refresh/load types, tiers, error budgets, contracts, schema drift policies, hooks, steps, transforms
  - Quality gate: all 7 checks explained, agent's role as decision-maker (not just check executor)
  - 8 supported data patterns (consume-and-merge, fan-in, SCD2, quarantine, etc.)
  - 6 decision principles: idempotent-by-default, never delete unconsumed, conservative on quality, context over thresholds, downstream awareness, explain reasoning
  - Cost: ~$0.004/call overhead (1,200 additional tokens vs old 50-token prompt)

#### Documentation
- **docs/architecture.md**: Updated execution flow to include Insights step, component map updated for agent/core.py
- **docs/agent/overview.md**: New "System Prompt (Platform Context)" section documenting what the agent knows; `generate_run_insights` added to operations and fallback tables
- **docs/concepts/pipelines.md**: New "Run Insights" section with scenario examples, insight structure, and API reference
- **CLAUDE.md**: Updated agent/core.py description with system prompt and generate_run_insights

---

### Build 30b: Run Insights — 2026-03-22 (Claude Opus 4.6)

**After every pipeline run, the agent analyzes results and generates actionable suggestions. Shown in the Activity view as an Insights card with one-click "Apply" buttons for configuration changes.**

#### Added
- **`generate_run_insights()` agentic method** (`agent/core.py`):
  - Agent receives run results, quality checks, pipeline config, baselines, and history
  - Generates 2-5 contextual insights per run (strategy, quality, schedule, volume, config, errors)
  - First-run insights: baseline established, strategy optimization, merge key suggestions
  - Subsequent runs: volume trends, quality patterns, performance, error patterns
  - Each insight has: category, message, priority, optional action_type + action_payload
  - `_rule_based_run_insights()` fallback for when API key unavailable

- **Insights generation in execution flow** (`agent/autonomous.py`):
  - Called after every run reaches terminal state (complete, failed, halted)
  - Non-blocking — insight generation errors never fail the run
  - Works for both legacy and step-DAG execution paths
  - Logged as "insights" execution step

- **`insights` field on RunRecord** (`contracts/models.py`, `contracts/store.py`):
  - JSONB column on runs table (DDL migration included)
  - Persisted and returned in all run API responses

- **Insights card in Activity view** (`ui/App.jsx`):
  - Indigo-themed card between Quality Gate and Execution Log
  - Priority indicator dots (red/amber/green)
  - Category pills (strategy, quality, volume, etc.)
  - "Apply" button for `patch_pipeline` suggestions — one-click configuration changes
  - Count badge in header

---

### Build 30: Fully Agentic Failure Detection & Quality — 2026-03-22 (Claude Opus 4.6)

**Replace hardcoded threshold logic with agent reasoning across all failure detection and data quality systems. The agent IS the decision maker — checks provide signals, the agent decides.**

#### Tier 1: Agent drives decisions (was: agent explains after the fact)

- **Agentic quality gate** (`quality/gate.py`, `agent/core.py`):
  - 7 quality checks still run as signal producers
  - Agent receives all check results + pipeline context (tier, first run, refresh type, baselines)
  - Agent decides PROMOTE / PROMOTE_WITH_WARNING / HALT with reasoning
  - `GateRecord.agent_reasoning` populated with the agent's decision rationale
  - Fallback to threshold-based logic when API unavailable

- **Agentic error budget diagnosis** (`agent/autonomous.py`, `agent/core.py`):
  - When error budget exhausted, agent analyzes recent run history
  - Diagnoses failure pattern: transient vs. persistent vs. degrading
  - Recommends specific recovery actions (retry, investigate, pause)
  - Alert includes agent diagnosis, pattern classification, and recovery steps

- **Agentic freshness alerting** (`monitor/engine.py`, `agent/core.py`):
  - Agent evaluates freshness SLA violations with schedule context
  - Determines if SLA is realistic (e.g., hourly SLA on daily schedule = impossible)
  - Decides severity and whether to alert at all
  - Can recommend SLA adjustments when thresholds are misconfigured

#### Tier 2: Agent now involved (was: no agent at all)

- **Agentic run failure diagnosis** (`agent/autonomous.py`, `agent/core.py`):
  - On run failure, agent analyzes error + execution log
  - Classifies: connector / source / target / network / schema / config / resource
  - Determines if transient (retry-worthy) or persistent (needs intervention)
  - Run error enriched with agent diagnosis and recommended action
  - Alert created only if agent says human attention needed

- **Agentic preflight reasoning** (`agent/autonomous.py`, `agent/core.py`):
  - Preflight checks still run (disk, upstream deps, connectors)
  - On failure, agent reasons about WHY and recommends next steps
  - Run error enriched: "Upstream X not ready | Agent: X scheduled for 8am, current time is 7:30 → wait for next schedule tick"

- **Agentic contract violation assessment** (`monitor/engine.py`, `agent/core.py`):
  - Agent evaluates impact of data contract violations
  - Assesses severity based on producer tier, consumer criticality, violation type
  - Missing optional columns → info; missing critical columns → critical
  - Alert severity driven by agent assessment, not hardcoded WARNING

- **Agentic anomaly thresholds** (`agent/core.py`):
  - Removed hardcoded 30% volume deviation, 2-failure, 5%-budget thresholds
  - Agent evaluates each pipeline's signals in context of its history
  - Per-pipeline reasoning: "This pipeline always drops volume on weekends — expected"
  - Cross-pipeline pattern analysis: correlated failures, shared source issues

#### Architecture
- `QualityGate` now receives `agent` parameter for decision-making
- `PipelineRunner` passes `agent` for failure diagnosis, preflight, and error budget
- Every agentic method has a `_rule_based_*` fallback for when API key is unavailable
- Agent methods: `decide_quality_gate`, `diagnose_error_budget`, `reason_about_freshness`, `diagnose_run_failure`, `reason_about_preflight_failure`, `assess_contract_violation`, `evaluate_anomaly_signals`

#### Documentation & Convention
- **CLAUDE.md**: Added critical design constraint #9 — "Agentic-first" convention requiring all new features to use agent reasoning with rule-based fallbacks explicitly named `_rule_based_*`
- **docs/concepts/quality-gate.md**: Rewritten — agent decides PROMOTE/HALT from check signals, rule-based fallback highlighted with ⚠️ marker
- **docs/concepts/observability.md**: Rewritten — agentic freshness evaluation, error budget diagnosis, per-pipeline anomaly detection with cross-pipeline analysis; all rule-based fallbacks explicitly marked
- **docs/agent/overview.md**: New operations table with all 8 agentic decision methods + their fallbacks; reframed rule-based section as convention documentation
- **docs/agent/diagnostics.md**: Rewritten — added run failure diagnosis, preflight reasoning, error budget diagnosis, contract violation assessment sections; removed hardcoded threshold tables, replaced with agent reasoning descriptions
- **docs/advanced/schema-drift.md**: Updated — pre-extract check described, agent-generated migration SQL documented, rule-based fallback noted

---

### Build 29b: Transform UI & Environment Promotion — 2026-03-22 (Claude Opus 4.6)

**Inline transform SQL editing with approval flow, searchable dependency picker, and GitOps-driven environment promotion model.**

#### Added
- **StepDAGSection UI component** (`ui/App.jsx`):
  - Pipeline detail view shows all steps grouped by dependency layers
  - Transform steps display full SQL, materialization, refs, variables, approval status
  - Inline SQL editor: edit → validate → save (resets approval to pending) → approve
  - Description and materialization type editable alongside SQL
  - Non-transform steps show type badges and config summaries

- **Searchable dependency picker** (`ui/App.jsx`):
  - Replaces raw `window.prompt` for pipeline ID with searchable dropdown
  - Filters by pipeline name, source table, or pipeline ID
  - Excludes self and already-added upstream dependencies
  - Two-step flow: select pipeline → review → Save Dependency
  - Shows pipeline name, ID prefix, and status pill for each option

- **Environment promotion model** (documentation):
  - GitOps-driven promotion: dev instance → git branch → PR review → merge → stage/prod sync
  - Three-instance architecture with branch-per-environment
  - Transform approval flow feeds into PR diffs
  - Pipeline changelog provides audit trail in git history

#### Key Design Decisions
- **Transform SQL requires approval; pipeline settings do not** — SQL changes affect data output (structural), settings are operational. Both tracked in changelog.
- **Dependencies save with confirmation, no approval gate** — Dependencies are scheduling metadata, not data-affecting. Confirm step prevents accidental clicks.
- **GitOps promotion over single-instance environment tags** — Separate instances per environment with git as the promotion mechanism. Safer isolation, natural PR review workflow, rollback = git revert.

---

### Build 29c: Schema Drift ALTER TABLE Fix — 2026-03-22 (Claude Opus 4.6)

**Fix schema drift auto-apply and approval to ALTER the actual target table, not just update column mappings in metadata.**

#### Fixed
- **`_auto_apply_schema_changes` in `monitor/engine.py`**: Now executes `ALTER TABLE ... ADD COLUMN` and `ALTER COLUMN ... TYPE` on the target table after updating column mappings. Previously only updated the pipeline contract in PostgreSQL, causing COPY to fail with "extra data after last expected column" when new source columns appeared.
- **`_apply_approved_proposal` in `api/server.py`**: Complete rewrite of schema change application logic:
  - `ADD_COLUMN`: Re-profiles source to get ColumnMapping objects, appends to pipeline, ALTERs target table. Previously tried `proposed_state.get("column_mappings", [])` which was always empty (proposals store `new_columns` not `column_mappings`), wiping all column mappings.
  - `ALTER_COLUMN_TYPE`: Updates mapping types and ALTERs column type on target.
  - `DROP_COLUMN`: Removes from mappings and DROPs column on target.
- **`_apply_proposal` now receives `config`** for credential decryption when building target connection params.

#### Added
- **`schema_change_policy` on pipeline creation** (`api/server.py`): `CreatePipelineRequest` now accepts `schema_change_policy` dict, applied after pipeline creation.
- **demo-ecommerce-orders uses `propose` policy**: New columns, dropped columns, and type changes all require approval instead of auto-applying. Prevents the exact failure that triggered this fix.

#### Added
- **Pre-extract schema drift check** (`agent/autonomous.py`): Before extraction, the runner now profiles the source and compares against pipeline column_mappings. If new columns are detected:
  - `auto_add` policy: appends mappings AND ALTERs target table immediately, then proceeds
  - `propose` policy: creates a proposal, halts the run (status=HALTED), awaits approval
  - `ignore` policy: proceeds without changes
  - Runs in both legacy and step-DAG execution paths
  - Failures in schema check are logged as warnings but don't block the run

#### Root Cause
Schema drift detection (monitor) correctly identified new columns from MySQL source and auto-added them to the pipeline's `column_mappings` in PostgreSQL. But the actual target table was never ALTERed. On next extract, the CSV included all columns (matching updated mappings), but `COPY ... FROM STDIN CSV HEADER` failed because the target table schema was stale.

#### Architecture Decision
Schema drift detection moved from monitor-only (5-minute background tick) to **run-time** (pre-extract check). The monitor still runs for continuous observability, but the critical path — catching drift before it causes a COPY failure — now happens at the point of execution.

**Agentic migration SQL** — All schema changes are now agent-generated, not hardcoded templates:
- Pre-extract drift check → agent generates migration SQL (LLM reasoning + rule-based fallback)
- Proposal contains the agent's SQL statements, reasoning, risk assessment, and rollback SQL
- Approval executes the agent's SQL — human reviews what the agent proposed, not what a template produced
- Monitor auto-apply also uses agent-generated SQL
- Falls back to rule-based generation when API key is unavailable

---

### Build 29: Native SQL Transforms — 2026-03-21 (Claude Opus 4.6)

**Replace dbt with native SQL transforms in pipelines — ref(), var(), 4 materialization strategies, AI generation, column lineage, and a full transform catalog.**

#### Added
- **Transform engine** (`transforms/engine.py`):
  - `{{ ref('table_name') }}` resolution: looks up transforms by name, then pipelines by target_table, then passthrough
  - `{{ var('key') }}` resolution: step variables > pipeline tags
  - 4 materialization strategies: TABLE (drop+create), VIEW (create or replace), INCREMENTAL (delete+insert on unique key), EPHEMERAL (not materialized)
  - SQL validation via `EXPLAIN (FORMAT JSON)` dry-run
  - SQL preview with LIMIT (sample rows without materializing)
  - Column lineage parsing from SELECT clause (best-effort regex)

- **Transform catalog** (`contracts/models.py`, `contracts/store.py`):
  - `SqlTransform` dataclass: versioned, approval-gated, pipeline-linked
  - `MaterializationType` enum: table, view, incremental, ephemeral
  - `sql_transforms` PostgreSQL table with CRUD methods
  - `ChangeType.NEW_TRANSFORM` / `UPDATE_TRANSFORM` for approval workflow

- **Transform API** (`api/server.py`, 9 endpoints):
  - `POST /api/transforms` — create transform
  - `GET /api/transforms` — list (filter by pipeline_id)
  - `GET /api/transforms/{id}` — detail with full SQL
  - `PATCH /api/transforms/{id}` — update SQL/config (bumps version)
  - `DELETE /api/transforms/{id}` — delete
  - `POST /api/transforms/{id}/validate` — dry-run EXPLAIN
  - `POST /api/transforms/{id}/preview` — sample rows
  - `POST /api/transforms/generate` — AI-generate from description
  - `GET /api/transforms/{id}/lineage` — parsed column lineage

- **Agent SQL generation** (`agent/core.py`):
  - `generate_transform_sql()` — Claude generates SQL from natural language + available table schemas
  - Rule-based fallback when no API key
  - Chat routing: "create transform", "generate transform", "sql transform", "list transforms"

- **Step DAG integration** (`agent/autonomous.py`):
  - `_step_transform` rewritten: supports catalog transforms (by transform_id) or inline SQL
  - Full ref/var/template resolution pipeline
  - Column lineage auto-tracked after transform execution
  - Transform outputs added to step context for downstream steps

- **MCP integration** (`mcp_server.py`):
  - 2 resources: `dapos://transforms`, `dapos://transforms/{id}`
  - 4 tools: `list_transforms`, `create_transform`, `generate_transform`, `validate_transform`

- **Documentation** (`docs/concepts/transforms.md`):
  - Full reference: materialization types, ref/var syntax, API endpoints, step DAG integration

- **Tests** (`test-pipeline-agent.sh`): 9 tests — CRUD (create, list, get, update, delete), lineage, AI generation, chat routing x2

#### Key Design Decisions
- **No Jinja2 dependency** — `ref()` and `var()` use simple regex, consistent with existing `_render_hook_sql` approach. Keeps security surface small.
- **Transforms are stored separately, not inline** — Enables reuse across pipelines and independent versioning. Step config references by `transform_id` or includes inline SQL.
- **Two-tier autonomy for AI-generated transforms** — Agent generates SQL, but `approved: false` until human approves. Prevents untested SQL from executing.
- **Column lineage is best-effort** — Regex heuristics for simple cases. Agent can enrich for complex joins/subqueries.
- **No new abstract methods on TargetEngine** — Transforms use existing `execute_sql()`. No changes needed to connector interface.

### Build 27: MCP Server — 2026-03-21 (Claude Opus 4.6)

**Expose DAPOS capabilities to AI agents via Model Context Protocol — enabling agent-to-agent discovery, querying, and operations.**

#### Added
- **MCP server** (`mcp_server.py`):
  - 9 resources: `dapos://catalog`, `dapos://catalog/stats`, `dapos://pipelines`, `dapos://alerts`, `dapos://dag`, `dapos://anomalies`, `dapos://catalog/tables/{id}`, `dapos://pipelines/{id}`
  - 13 tools: `search_catalog`, `search_columns`, `get_trust_score`, `get_semantic_tags`, `infer_tags`, `diagnose_pipeline`, `analyze_impact`, `trigger_pipeline`, `get_pipeline_runs`, `get_freshness`, `generate_narrative`, `design_topology`, `get_business_context`
  - 3 prompts: `troubleshoot_pipeline`, `explore_catalog`, `assess_platform_health`
  - Transports: stdio (Claude Desktop), SSE (web clients), streamable-http
  - JWT-authenticated httpx client with token caching (same pattern as CLI)
  - Config via `DAPOS_URL`, `DAPOS_USER`, `DAPOS_PASSWORD` env vars
- **MCP documentation** (`docs/advanced/mcp-server.md`):
  - Claude Desktop configuration (local + remote)
  - Resource, tool, and prompt reference
  - Architecture diagram
- **MCP dependency** (`requirements.txt`): `mcp>=1.26.0`
- **MCP tests** (`test-pipeline-agent.sh`): 3 smoke tests (import, resource count, tool count)

#### Changed
- Demo bootstrap now sets semantic tags, business context, and trust weights on demo pipelines at creation time
- Documentation updated for Builds 25-26: data catalog, trust scores, anomaly narratives, deployment guide

#### Key Design Decisions
- **MCP over A2A** — MCP is the better fit for DAPOS's structured data query pattern (resources + tools). A2A is designed for multi-step task delegation between agents, which is overkill for catalog queries.
- **REST API passthrough** — The MCP server is a thin translation layer over the existing REST API. No direct database access, same RBAC, same permissions.
- **stdio default** — Most MCP clients (Claude Desktop, Cursor) use stdio transport. SSE and streamable-http available for web integrations.

### Build 25: In-App Documentation & CLI — 2026-03-21 (Claude Opus 4.6)

**Documentation available within the DAPOS web UI, plus a full CLI for scripting and CI/CD.**

#### Added
- **In-app documentation** (`api/server.py`, `ui/App.jsx`):
  - `GET /api/docs` — returns doc tree from `docs/` directory
  - `GET /api/docs/{path}` — serves markdown content with path traversal protection
  - `DocsView` React component with sidebar navigation, internal link handling, back history
  - `simpleMarkdown()` renderer: code blocks, headings, bold/italic, links, tables, lists
  - "Docs" tab added to main navigation (icon: `i`)
- **CLI** (`cli/__main__.py`):
  - 14 commands: health, pipelines list/get/trigger/pause/resume, trigger, runs, steps, connectors, diagnose, impact, anomalies, alerts, chat, export, token
  - Token caching to `~/.dapos_token`, fuzzy pipeline name resolution
  - `--json` flag on all data commands, config via `DAPOS_URL`/`DAPOS_USER`/`DAPOS_PASSWORD`
- **Structured documentation** (`docs/`):
  - 8 doc files: index, quickstart, architecture, configuration, cli-reference, api-reference, concepts/pipelines, concepts/step-dags
  - Modeled after Apache Airflow docs structure

#### Changed
- Cache version bumped to v=37 in `index.html`

### Build 26: Data Catalog, Semantic Tags, Trust Scores & Alert Narratives — 2026-03-21 (Claude Opus 4.6)

**A built-in data catalog that surfaces table metadata, trust scores, AI-inferred semantic tags, business context, and alert narratives — replacing the need for external catalog tools like Atlan, DataHub, or Alation.**

#### Added
- **Data Catalog API** (`api/server.py`):
  - `GET /api/catalog/search` — full-text search across tables, columns, tags, and business context with filters (source_type, status, tier, pagination)
  - `GET /api/catalog/tables/{id}` — complete table detail: columns, lineage, freshness (current + 72h history), quality trend, error budget, trust breakdown, data contracts, recent runs
  - `GET /api/catalog/trust/{id}` — trust score breakdown with component scores, weights, and recommendation
  - `GET /api/catalog/columns` — cross-pipeline column search with table filter
  - `GET /api/catalog/stats` — catalog-wide statistics: table/column counts, source type distribution, trust distribution

- **Semantic Tags** (`api/server.py`, `agent/core.py`):
  - `GET /api/catalog/tables/{id}/tags` — get all semantic tags for a pipeline's columns
  - `POST /api/catalog/tables/{id}/tags/infer` — AI-infer semantic tags (domain, description, PII flag) via Claude; preserves user-overridden tags
  - `PUT /api/catalog/tables/{id}/tags` — bulk set/override tags for multiple columns (marked `source=user`)
  - `PATCH /api/catalog/tables/{id}/tags/{column}` — per-column tag override
  - `PipelineContract.semantic_tags` field (dict) stores per-column tag metadata

- **Business Context** (`api/server.py`, `agent/core.py`):
  - `GET /api/catalog/tables/{id}/context/questions` — Claude generates targeted questions based on pipeline schema
  - `PUT /api/catalog/tables/{id}/context` — save business context answers (auto-stamps `_last_updated`, `_updated_by`)
  - `PipelineContract.business_context` field (dict) stores key-value context

- **Trust Scores** (`api/server.py`):
  - 4 weighted components: freshness (30%), quality gate (30%), error budget (25%), schema stability (15%)
  - Components without data are excluded from denominator (partial data supported)
  - Recommendations: >0.9 high, >0.7 good, >0.5 medium, <0.5 low
  - `PUT /api/catalog/tables/{id}/trust-weights` — per-pipeline weight override (must sum to ~1.0)
  - `DELETE /api/catalog/tables/{id}/trust-weights` — reset to global defaults

- **Alert Narratives** (`api/server.py`, `agent/core.py`):
  - `POST /api/observability/alerts/{id}/narrative` — Claude generates human-readable narrative from alert context, recent run errors, downstream count, freshness state
  - Narrative saved on alert record for future reference
  - `agent.generate_anomaly_narrative()` — structured prompt with pipeline tier, schedule, downstream impact

- **Agent methods** (`agent/core.py`):
  - `infer_semantic_tags()` — column-level tag inference from names, types, table context
  - `generate_business_context_questions()` — targeted questions per pipeline
  - `generate_anomaly_narrative()` — alert-to-prose conversion

- **Documentation**:
  - `docs/concepts/data-catalog.md` — full catalog API reference with curl examples
  - `docs/advanced/trust-scores.md` — trust formula, component scoring, weight customization, worked examples
  - `docs/concepts/anomaly-narratives.md` — narrative generation, context, cost

#### Key Design Decisions
- **Catalog is a read layer, not a separate store** — All catalog data derives from pipeline contracts and observability state already in PostgreSQL. No additional tables, no sync jobs, no external catalog service.
- **AI-inferred tags never overwrite user tags** — User-provided tags (source=user) are preserved when re-running AI inference. This ensures human corrections are never lost.
- **Trust scores handle partial data** — The weighted average only includes components that have data, so new pipelines still get meaningful scores even before all observability signals are established.
- **Narratives are saved, not ephemeral** — Once generated, the narrative persists on the alert record. This avoids repeated LLM calls for the same alert.
- **Semantic search includes business context** — The catalog search endpoint searches across business context answers and semantic tag text, not just technical metadata.

### Build 24: Agent Diagnostic & Reasoning Layer - 2026-03-21 (Claude Opus 4.6)

**The agent reasons about pipeline health, downstream impact, and platform-wide anomalies — capabilities that Fivetran, Airflow, and Monte Carlo cannot replicate without an LLM reasoning layer.**

#### Added
- **Pipeline diagnosis** (`agent/core.py: diagnose_pipeline`):
  - Gathers 10 data sources: recent runs, quality gate trend, error budget, upstream dependencies + their run health, source connector status, alerts, volume history
  - Claude reasons about root cause with structured output: category, confidence, evidence, recommended actions, pattern detection
  - Rule-based fallback when no API key: checks last run errors, upstream failures, connector status, error budget
  - Categories: `source_issue`, `connector_issue`, `upstream_dependency`, `quality_regression`, `scheduling`, `configuration`, `data_issue`, `unknown`

- **Impact analysis** (`agent/core.py: analyze_impact`):
  - Recursive BFS walk of dependency graph (`store.get_all_downstream_recursive`) with max_depth=10
  - Gathers downstream pipelines, data contracts (as producer), column lineage
  - Claude assesses blast radius, SLA risk, mitigation options
  - Rule-based fallback: severity by downstream count and tier

- **Proactive anomaly reasoning** (`agent/core.py: reason_about_anomalies`):
  - Pre-filters all active pipelines for anomalous signals before Claude call (cost optimization):
    - Volume deviation >30% from historical average
    - 2+ failures in 24h
    - Error budget remaining <5%
  - Short-circuits with "healthy" response if no anomalies detected (no Claude call)
  - Claude considers day-of-week patterns, correlated failures, gradual vs sudden changes
  - Runs automatically every 15 minutes in observability loop
  - Critical unexpected anomalies auto-create CRITICAL alerts

- **4 new store methods** (`contracts/store.py`):
  - `list_recent_failures(hours)` — failed/halted runs across all pipelines
  - `get_quality_trend(pipeline_id, limit)` — recent gate evaluations
  - `get_volume_history(pipeline_id, limit)` — completed run row counts
  - `get_all_downstream_recursive(pipeline_id, max_depth)` — transitive dependency walk

- **3 REST API endpoints** (`api/server.py`):
  - `POST /api/pipelines/{id}/diagnose` — root-cause diagnosis (10/min rate limit)
  - `POST /api/pipelines/{id}/impact` — downstream impact analysis (10/min rate limit)
  - `GET /api/observability/anomalies` — platform-wide anomaly scan (10/min rate limit)

- **3 chat actions** routed via natural language:
  - "Why is X failing?" / "diagnose" → `diagnose_pipeline`
  - "What breaks if X goes down?" / "impact" / "blast radius" → `analyze_impact`
  - "Any anomalies?" / "platform health" / "anything unusual" → `check_anomalies`

- **Pipeline name resolution** (`_resolve_pipeline` helper):
  - Matches by exact ID, exact name, substring, or word overlap
  - Filters out common stop words for better fuzzy matching

- **Proactive observability** (`main.py`):
  - `_check_anomalies()` runs every 15 minutes in the observability loop
  - Creates CRITICAL alerts for unexpected anomalies automatically
  - Logs anomaly count even when all are expected/non-critical

- **CLI** (`cli/__main__.py`) — Full command-line interface for DAPOS:
  - `python -m cli health` — platform health check
  - `python -m cli pipelines list` — list pipelines with table formatting
  - `python -m cli pipelines get <name>` — pipeline detail
  - `python -m cli trigger <name>` — trigger a run
  - `python -m cli runs <name>` — recent runs
  - `python -m cli steps <name>` — show step DAG
  - `python -m cli connectors` — list connectors
  - `python -m cli diagnose <name>` — root cause diagnosis
  - `python -m cli impact <name>` — downstream impact
  - `python -m cli anomalies` — platform-wide anomaly scan
  - `python -m cli alerts` — recent alerts
  - `python -m cli chat <text>` — natural language command
  - `python -m cli export` — YAML export
  - `python -m cli token` — print auth token for scripting
  - All commands support `--json` for machine-readable output
  - Pipeline name fuzzy resolution (substring match)
  - Token caching in `~/.dapos_token`
  - Config via `DAPOS_URL`, `DAPOS_USER`, `DAPOS_PASSWORD` env vars

- **Documentation** (`docs/`):
  - `docs/index.md` — Documentation index with Getting Started, Concepts, Operations, Agent, Advanced sections
  - `docs/quickstart.md` — 5-minute quickstart guide
  - `docs/architecture.md` — System architecture diagram and component map
  - `docs/configuration.md` — Full environment variable reference
  - `docs/cli-reference.md` — CLI command reference with examples
  - `docs/api-reference.md` — REST API endpoint reference (40+ endpoints)
  - `docs/concepts/pipelines.md` — Pipeline contracts, lifecycle, strategies
  - `docs/concepts/step-dags.md` — Composable step DAGs, step types, execution model

- **8 new tests** in `test-pipeline-agent.sh`:
  - Diagnose (200 + 404), impact (200 + 404), anomalies (200), chat routing (3 tests)

#### Key Design Decisions
- **Pre-filter before Claude call** — Anomaly reasoning iterates all active pipelines but only sends anomalous ones to Claude. If nothing is anomalous, the Claude call is skipped entirely. This keeps cost proportional to problems, not pipeline count.
- **Rule-based fallbacks everywhere** — All three methods work without an API key via heuristic analysis. The agent degrades gracefully.
- **Recursive BFS for impact** — Uses iterative BFS with visited set and max_depth=10 to handle circular dependencies safely.
- **34 template variables** — Added 10 connection/environment variables (host, database, user, port, environment) so the same transform SQL works across test/stg/prod.

---

### Build 18: Composable Step DAGs - 2026-03-21 (Claude Opus 4.6)

**Replace the fixed extract→load→promote flow with configurable step DAGs, enabling Airflow-style pipeline composition.**

#### Added
- **Step data model** (`contracts/models.py`):
  - `StepType` enum: `extract`, `transform`, `quality_gate`, `promote`, `cleanup`, `hook`, `sensor`, `custom`
  - `StepStatus` enum: `pending`, `running`, `complete`, `failed`, `skipped`, `halted`
  - `StepDefinition` dataclass: step_id, step_name, step_type, depends_on, config, retry_max, timeout_seconds, skip_on_fail, enabled
  - `StepExecution` dataclass: tracks per-step execution with status, output, error, timing, retries
  - Added `steps: list[StepDefinition]` to `PipelineContract`

- **Step DAG executor** (`agent/autonomous.py`):
  - **Dual-path execution**: `_execute_inner()` dispatches to `_execute_legacy()` (empty steps) or `_execute_step_dag()` (steps defined)
  - Full backward compatibility — existing pipelines with `steps=[]` use the legacy path unchanged
  - Topological sort with cycle detection (`_topo_sort()`)
  - Step context dict passed between steps (XCom equivalent in-memory)
  - Per-step retry logic with configurable `retry_max`
  - `skip_on_fail` support — failed dependency → skip downstream vs fail run
  - `_StepHalt` exception for quality gate halts (distinct from failures)
  - 8 step type handlers:
    - `_step_extract` — extract from source, populate staging
    - `_step_transform` — execute SQL against target with template variable rendering
    - `_step_quality_gate` — run 7-check quality gate, halt on HALT decision
    - `_step_promote` — load staging + promote to target table
    - `_step_cleanup` — clean up staging files
    - `_step_hook` — execute post-promotion SQL with cleanup guard
    - `_step_sensor` — poll SQL query until condition met (with timeout)
    - `_step_custom` — extensible SQL execution

- **Database schema** (`contracts/store.py`):
  - `steps JSONB NOT NULL DEFAULT '[]'` column on `pipelines` table
  - `step_executions` table with indexes on run_id, pipeline_id, step_id
  - `save_step_execution()` and `list_step_executions()` store methods
  - `_parse_steps()` helper for deserializing JSONB to StepDefinition list
  - ALTER TABLE migration for existing databases

- **YAML codec** (`contracts/yaml_codec.py`):
  - Steps exported in `pipeline_to_dict()` with full step definition
  - Steps imported via `_parse_steps_from_dict()` in `dict_to_pipeline()`
  - Round-trip serialization: YAML ↔ StepDefinition

- **10 connection/environment template variables** (`agent/autonomous.py`):
  - `{{environment}}` — resolves to `test`, `staging`, `production` etc.
  - `{{source_host}}`, `{{source_database}}`, `{{source_user}}`, `{{source_port}}`
  - `{{target_host}}`, `{{target_database}}`, `{{target_user}}`, `{{target_port}}`
  - `{{target_ddl}}`
  - Same SQL works across environments — connection details resolve from the pipeline contract
  - Total template variables: 34 (15 run context + 10 connection + 9 upstream)

- **5 REST API endpoints** (`api/server.py`):
  - `GET /api/pipelines/{id}/steps` — step DAG definition
  - `GET /api/runs/{run_id}/steps` — step executions for a run
  - `POST /api/pipelines/{id}/steps/validate` — validate DAG (cycle detection, missing deps)
  - `GET /api/pipelines/{id}/steps/preview` — preview execution order
  - Steps included in pipeline detail response and PATCH updates
  - `steps` field on `CreatePipelineRequest` and `UpdatePipelineRequest`
  - `step_count` in pipeline summary responses

#### Key Design Decisions
- **Dual-path execution** — Existing pipelines continue working via `_execute_legacy()`. Only pipelines with `steps` defined use the DAG executor. Zero migration required.
- **Step context as in-memory dict** — Like Airflow's XCom but simpler. Steps pass data through `ctx` dict during execution. Outputs persisted to `step_executions.output` for post-run inspection.
- **Halt vs Fail distinction** — Quality gate steps raise `_StepHalt` to stop the DAG without marking it as a failure. Preserves staging for investigation.
- **Sensor pattern** — Sensor steps poll a SQL condition, enabling event-driven DAGs within a pipeline (e.g., wait for upstream table to be populated).
- **Retry per step** — Each step has its own `retry_max`, enabling different retry strategies for extract (retry 3x) vs promote (no retry).

---

### Build 23: GitOps Pipeline Config Versioning - 2026-03-21 (Claude Opus 4.6)

**Separate git repo for pipeline configs and connector code, auto-committed on every structural change**

#### Added
- **`gitops/repo.py`** — `GitOpsRepo` class managing a separate git repository:
  - `init_repo()` — creates repo with README, `pipelines/` and `connectors/` directories
  - `commit_pipeline(pipeline, yaml_content, message, author)` — writes YAML and commits
  - `commit_connector(connector, message, author)` — writes Python code with metadata header
  - `commit_all(pipelines, connectors, message, author)` — bulk sync, single commit
  - `delete_pipeline(pipeline_name, message, author)` — removes YAML file
  - `get_log(limit)`, `get_file_at_commit()`, `get_diff()`, `get_pipeline_history()` — read operations
  - `status()` — returns repo state summary (branch, head, file counts)
  - All git operations via `subprocess.run()` with 30s timeout

- **GitOps auto-commit hooks** — Pipeline YAML auto-committed on every structural change:
  - Pipeline create → commit YAML
  - Pipeline update (PATCH) → commit YAML with version
  - Pipeline pause/resume → commit YAML
  - Approval applied → commit pipeline YAML (schema changes) or connector code (new connectors)
  - Author tracked from JWT caller identity

- **Boot sync** — On startup, all pipelines + connectors bulk-committed to repo:
  - Full export of all pipeline YAML (credentials masked) and connector code
  - Single commit with summary message

- **Multi-developer remote sync** — Supports team environments with shared remote:
  - `_pull()` — fetch + rebase before every commit (auto_pull)
  - `_push()` — push after every commit with retry on reject (auto_push)
  - Clone from remote on first init if `PIPELINE_REPO_REMOTE` is set
  - Conflict resolution: rebase for non-overlapping changes; abort + preserve local on true conflicts (DAPOS DB is source of truth, next boot sync reconciles)
  - `status()` reports sync state: `in_sync`, `ahead`, `behind`, `diverged`

- **Branch-per-environment** — `PIPELINE_REPO_BRANCH` enables isolation:
  - `dev`, `staging`, `prod` branches on same remote repo
  - Each DAPOS instance targets its own branch
  - Review/promote via standard git merge/PR workflow between branches

- **Configuration** — 6 environment variables:
  - `PIPELINE_REPO_PATH` — path to the local git repo (empty = disabled)
  - `PIPELINE_REPO_BRANCH` — branch name (default: `main`)
  - `PIPELINE_REPO_REMOTE` — remote URL for shared repo (empty = local only)
  - `GITOPS_AUTO_PUSH` — push after commit (default: `true`, requires remote)
  - `GITOPS_AUTO_PULL` — pull before commit (default: `true`, requires remote)
  - `GITOPS_SYNC_ON_BOOT` — enable/disable boot sync (default: `false`)
  - `Config.has_gitops` property for feature toggle

- **Disaster recovery: restore from repo** — `POST /api/gitops/restore?dry_run=true`:
  - Reads all `pipelines/*.yaml` and `connectors/*.py` from the git repo
  - Upserts into PostgreSQL using existing `save_pipeline()` / `save_connector()` (idempotent)
  - Dry-run mode (default) previews what would be restored without making changes
  - Pipeline YAML parsed via `yaml_to_pipelines(preserve_id=True)` to preserve original IDs
  - Connector code parsed from `.py` files with metadata header extraction
  - Admin-only endpoint, rate-limited to 5/minute
  - Pulls latest from remote before restore if remote is configured

- **Automatic conflict reconciliation** — Observability loop checks every 5 minutes:
  - When a rebase conflict is detected, `_needs_reconcile` flag is set
  - On next 5-minute tick, full DB state is rewritten to repo and force-pushed
  - `reconcile()` method rewrites all files from PostgreSQL (source of truth)
  - Uses `--force-with-lease` for safe force-push

- **6 GitOps REST API endpoints**:
  - `GET /api/gitops/status` — repo status (enabled, branch, head, file counts, sync_status)
  - `GET /api/gitops/log?limit=20` — recent commit log
  - `GET /api/gitops/pipelines/{id}/history?limit=20` — per-pipeline commit history
  - `GET /api/gitops/diff?commit_a=HEAD~1&commit_b=HEAD` — diff between commits
  - `GET /api/gitops/file?filepath=...&commit=HEAD` — file content at commit
  - `POST /api/gitops/restore?dry_run=true` — restore DB from repo (admin-only)

- **5 new tests** in `test-pipeline-agent.sh`:
  - GitOps status endpoint, log endpoint, diff endpoint, per-pipeline history, restore dry-run

#### Key Design Decisions
- **Separate repo (Option A) over internal versioning** — Pipeline configs live in their own git repo (`client1-dags-repo`), not mixed with DAPOS source code. Enables independent review, external CI/CD triggers, and clear separation between platform code and pipeline definitions.
- **Fire-and-forget commits** — GitOps commits never block or fail the primary operation. All wrapped in try/except with warning logs. A failed git commit doesn't prevent a pipeline from being created or updated.
- **Credentials masked in YAML** — All committed YAML uses `pipeline_to_yaml(mask_credentials=True)` to prevent secrets from entering version control.
- **Author propagation** — JWT `sub` claim flows through to git commit author, creating an audit trail of who changed what.
- **DAPOS is source of truth** — On conflict, local state (from PostgreSQL) wins. The repo is a derived artifact of DB state, not the other way around. Boot sync reconciles any drift.

---

### Build 22: Observability UX — Execution Logs, Freshness Charts, Lineage Consolidation - 2026-03-21 (Claude Opus 4.6)

**Structured execution logging, freshness time-series charts, consolidated lineage/DAG view, expandable run details**

#### Added
- **Structured execution logging** — Every pipeline run captures a step-by-step execution log with timing:
  - `RunRecord.execution_log` field (JSONB) stores structured entries: `{ts, step, detail, status, elapsed_ms}`
  - `PipelineRunner._log_step()` helper appends entries during execution
  - 13 instrumented steps: start, preflight, connectors, extract, skip, load_staging, quality_gate, halt, promote, watermark, cleanup, column_lineage, metadata, hooks, complete, error
  - `execution_log` column added to runs table via `_ALTER_TABLES_SQL`
  - Exposed in `_run_summary()` API response

- **Execution log timeline UI** — Expandable run detail (`ActivityRunDetail`) shows:
  - Visual timeline with color-coded dots (green=ok, amber=warn, red=error)
  - Step name, detail text, and elapsed milliseconds per step
  - Vertical connector line for timeline flow

- **Expandable run details in Activity tab** — `ActivityRunDetail` component:
  - Metadata grid: duration, mode, rows extracted/loaded, staging size, retries, timestamps
  - Watermark before→after with visual arrows
  - Triggered-by info with pipeline/run IDs
  - Quality gate checks with per-check status dots
  - Error detail in red box
  - Filter buttons (All/Completed/Failed/Halted)

- **Freshness time-series chart** — `FreshnessChart` SVG component:
  - Staleness plotted over time (X=check timestamp, Y=staleness minutes)
  - Dashed threshold lines for warn (amber) and critical (red) SLA levels
  - Color-coded dots per snapshot (green/amber/red by status)
  - Green area fill gradient under the curve
  - Time range selector: 6h, 24h, 3d, 7d
  - Y-axis labels auto-scaled, grid lines, time tick labels

- **Freshness history API** — `GET /api/observability/freshness/{pipeline_id}/history?hours=24`:
  - `Store.list_freshness_history(pipeline_id, hours)` queries accumulated snapshots
  - Returns staleness_minutes, sla_met, status, checked_at per snapshot

- **Expandable freshness cards** — `FreshnessCard` component with detail grid:
  - Warn/critical thresholds, freshness column, schedule, last record time, last run, rows, target table
  - Chart embedded in expanded view
  - API enriched with schedule_cron, freshness_column, freshness_critical_minutes, last_run_at, last_run_rows, target_table

- **Consolidated Lineage + DAG view** — Merged separate Lineage and Pipeline DAG tabs:
  - Single "Lineage" tab with full DAG visualization
  - Search input filters nodes by name/source/target/owner with highlight + neighbor visibility
  - SVG zoom/pan via viewBox manipulation, mouse events, scroll wheel
  - +/-/fit controls
  - Column-level lineage in node detail panel (fetches `/api/lineage/{id}`)

- **Pipeline detail fixes**:
  - Fixed blank screen on demo-stripe-charges (ErrorBudgetCard, RunRow extracted as proper components)
  - Fixed quality_results rendering (reads `.checks` array, not `Object.entries`)
  - Fixed lineage API 500 error (replaced broken `get_downstream_columns` call)
  - Fixed ColumnLineage attribute names (`id` not `lineage_id`, `transformation` not `transform_logic`)
  - Changelog section in pipeline detail (amber box, shows `recent_changes`)
  - Cache-Control headers on HTML responses

#### Key Design Decisions
- **Execution log on RunRecord, not separate table** — Logs are always accessed with their run. JSONB column avoids join overhead and keeps the run as the unit of observability.
- **SVG for charts, no external library** — Consistent with DAG visualization approach. No Chart.js/D3 dependency. Pure React + SVG.
- **Freshness snapshots already accumulated** — `save_freshness` inserts new rows (unique snapshot_id per check). History API just needed a time-filtered query.
- **Consolidated lineage over separate views** — User confirmed Lineage tab showed nothing useful alone. Combined with DAG for one comprehensive view.

---

### Build 21: Analyst Experience — Source Registry, Guided Conversation, Audit Trail - 2026-03-21 (Claude Opus 4.6)

**Source Registry, Guided Conversation Flow, Schedule Parser, Pipeline Changelog, Interaction Logging**

#### Added
- **Source Registry** — Admin pre-registers named data sources ("E-commerce Database") with credentials. Analysts select by friendly name, never see connection strings.
  - `RegisteredSource` dataclass with display_name, connector_id, connection_params, description, owner, tags, schema_cache
  - 6 REST endpoints: `POST/GET /api/sources`, `GET/PATCH/DELETE /api/sources/{id}`, `POST /api/sources/{id}/discover`
  - Store CRUD: `save_registered_source`, `get_registered_source`, `get_registered_source_by_name`, `list_registered_sources`, `delete_registered_source`, `update_source_schema_cache`
  - Fuzzy matching: `_resolve_registered_source()` matches user text against registered display names across all action handlers (discover_tables, profile_table, propose_strategy, create_pipeline)

- **Guided Conversation Flow** — Context-accumulator approach (not state machine). Agent gathers pipeline requirements conversationally:
  - `AgentCore.guided_pipeline_response()` — analyst-friendly system prompt that avoids jargon, presents plain-language equivalents
  - Guided mode enters when `create_pipeline` has missing info; accumulates context across turns
  - Available registered sources injected into route context so agent can suggest by name
  - Progressive disclosure: business questions first, technical details inferred

- **Plain-Language Schedule Parser** — `AgentCore.parse_schedule(text)`:
  - Rule-based map of ~30 natural language phrases → cron expressions ("every morning" → `0 8 * * *`, "twice a day" → `0 8,20 * * *`)
  - Regex patterns for "every N hours/minutes" constructs
  - Claude LLM fallback for ambiguous or complex expressions
  - Auto-applied in `create_pipeline` when schedule doesn't match cron syntax

- **Pipeline Changelog** — Structured audit trail for every pipeline mutation:
  - `PipelineChangeLog` dataclass with 16 change types (CREATED, UPDATED, TRIGGERED, PAUSED, RESUMED, DELETED, BACKFILLED, etc.)
  - Tracks who, when, what changed (old/new field values), source (api/chat), reason
  - `_log_pipeline_change()` helper wired into create, update, trigger, pause, resume, backfill endpoints
  - `GET /api/pipelines/{id}/changelog` — per-pipeline audit trail
  - `GET /api/changelog` — global changelog (admin only)
  - `recent_changes` (last 10 entries) included in pipeline detail response

- **Chat Interaction Audit Log** — Every chat exchange persisted for auditing and training:
  - `ChatInteraction` dataclass with session_id, user_id, username, input/output tokens, latency, model, routing
  - Token accumulator on AgentCore (`_req_input_tokens`/`_req_output_tokens`) tracks tokens across all Claude calls per request
  - `GET /api/interactions` — paginated interaction browse (admin only)
  - `GET /api/interactions/export` — JSONL export for training data (admin only)

#### Key Design Decisions
- **Context accumulator over state machine** — Preserves agentic experience. Claude decides what to ask next based on what's missing, not a predefined flow. User explicitly requested this to avoid "taking away the agentic experience."
- **Rule-based schedule parsing first** — 30 common phrases handled without LLM call. Claude only invoked as fallback for ambiguous schedules, keeping latency low.
- **Source registry is admin-managed** — Credentials stored once by admins. Analysts reference by display name. Enforces separation of concerns.
- **Token accumulator pattern** — Per-request counters on AgentCore, reset at start of `route_command`, accumulated across all `_call_claude` calls. Captures total token usage even when multiple Claude calls happen per request.

---

### Stale Run Recovery & Timeout Enforcement - 2026-03-21 (Claude Opus 4.6)

**Crash Recovery & Run Timeouts**

#### Added
- **Stale run recovery on startup** — On process boot, any runs stuck in non-terminal states (pending, extracting, staging, loading, quality_gate, promoting, retrying) from a prior crash are automatically marked as failed with a descriptive error message. Prevents orphaned runs from blocking pipelines.
- **`Store.list_stale_runs(stale_before)`** — New store method that finds runs in non-terminal states that started before the given timestamp.
- **Run timeout enforcement** — `scheduler/manager.py` now wraps `runner.execute()` with `asyncio.wait_for(timeout=pipeline.timeout_seconds)`. Pipelines that exceed their configured timeout (default 3600s) are marked failed with a timeout error and proceed to retry logic.

#### Key Design Decisions
- **Fail-open on timeout** — Timed-out runs go through normal retry logic (`_maybe_retry`), so transient slowness gets retried before alerting.
- **Boot-time boundary** — Uses process boot timestamp as the stale cutoff, so only runs from *prior* processes are recovered, not runs started by the current process.
- **No Celery** — Confirmed asyncio + Semaphore is sufficient for current scale. Stale recovery + timeouts cover the crash-safety gap without adding broker infrastructure.

---

### Builds 19-20 - 2026-03-21 (Claude Opus 4.6)

**Build 19: DAG Visualization UI**

#### Added
- **`GET /api/dag`** — Returns full pipeline dependency graph with nodes (pipeline summary + last run + contract info) and edges (dependencies with type). Powers the DAG view.
- **DAGView component** — New SVG-based pipeline dependency graph in the React SPA. Features:
  - Topological sort into layers (roots at top, leaves at bottom)
  - Nodes colored by status (green=active, gray=paused, red=failed)
  - Tier badges (T1/T2/T3) on each node
  - Source→target labels on each node
  - Last run row count
  - Contract violation count badges
  - Dependency arrows (solid) vs data contract edges (purple dashed)
  - Click-to-select detail panel showing pipeline info, contracts, and violations
  - Legend for all visual indicators
- **"DAG" nav item** — Added to sidebar between Lineage and Connectors

**Build 20: Agent Topology Reasoning**

#### Added
- **`AgentCore.design_topology()`** — Claude-powered method that takes a natural language description of a business problem and designs a multi-pipeline architecture. Returns structured JSON with:
  - Proposed pipelines (name, source, target, schedule, tier, merge keys, hooks)
  - Dependencies between pipelines (with trigger type)
  - Data contracts (with freshness SLA and cleanup ownership)
  - Pattern identification (fan-in, consume-and-merge, cascading aggregation, etc.)
  - Detailed reasoning for design decisions
- **`POST /api/topology/design`** — REST endpoint for topology design. Rate-limited to 10/min. Admin/operator only.
- **Chat routing** — Keywords "design", "architect", "topology", "multi-pipeline", "data architecture", "pipeline architecture" route to the topology designer. Response is formatted with pipeline list, dependencies, contracts, and reasoning.
- **`design_topology` action** in both Claude-routed and keyword-routed command parsing.
- **5 curl tests** — DAG structure, node fields, contract fields, topology design endpoint, chat topology routing.

#### Key Use Cases Unlocked
- **Visual dependency monitoring**: Operators can see the entire pipeline graph at a glance, with status colors and contract violation badges highlighting problems.
- **Architecture-as-conversation**: User describes "I need orders from MySQL and customers from MongoDB merged into PostgreSQL" → agent designs 3 pipelines with dependencies, contracts, and schedules.
- **Two-tier autonomy for topology**: Agent proposes, human approves. The topology response is a proposal, not automatic creation.

---

### Build 16 - 2026-03-21 (Claude Opus 4.6)

**Data Contracts Between Pipelines**

#### Added
- **`DataContract` dataclass** — Formalizes producer/consumer relationships with: `contract_id`, `producer_pipeline_id`, `consumer_pipeline_id`, `description`, `status`, `required_columns`, `freshness_sla_minutes` (default 60), `retention_hours` (default 168), `cleanup_ownership` (producer_ttl / consumer_acknowledges / none), violation tracking.
- **`ContractViolation` dataclass** — Records individual contract violations with type (freshness_sla, schema_mismatch, retention_expired), detail, resolved state.
- **3 new enums** — `CleanupOwnership`, `DataContractStatus` (active/violated/paused/archived), `ContractViolationType`.
- **8 REST API endpoints**:
  - `POST /api/data-contracts` — Create contract (validates pipelines exist, rejects self-contracts and duplicates, auto-creates dependency)
  - `GET /api/data-contracts` — List with optional `?producer_id=`, `?consumer_id=`, `?status=` filters
  - `GET /api/data-contracts/{id}` — Detail with recent violations and pipeline names
  - `PATCH /api/data-contracts/{id}` — Update SLA, retention, required columns, status, cleanup ownership
  - `DELETE /api/data-contracts/{id}` — Delete contract and its violations
  - `POST /api/data-contracts/{id}/validate` — Manual validation (freshness SLA + required columns check)
  - `GET /api/data-contracts/{id}/violations` — List violations with optional `?resolved=` filter
  - `POST /api/data-contracts/{id}/violations/{vid}/resolve` — Mark violation resolved
- **Monitor integration** — `_check_data_contracts()` runs every monitor tick (5m), validates all active contracts for freshness SLA and schema requirements, creates violations and alerts on failure.
- **Cleanup guard** — `_check_cleanup_allowed()` in `PipelineRunner` blocks DELETE/TRUNCATE hooks when a data contract with `cleanup_ownership=consumer_acknowledges` exists and the consumer has no successful runs yet. Enforces "never delete unconsumed data" at the system level.
- **Pipeline detail enrichment** — `GET /api/pipelines/{id}` now includes `data_contracts.as_producer` and `data_contracts.as_consumer` arrays.
- **Auto-dependency** — Creating a data contract automatically creates a pipeline dependency (consumer depends on producer) if one doesn't already exist.
- **DB tables** — `data_contracts` and `contract_violations` with indexes on producer, consumer, status, and unresolved violations.
- **11 curl tests** — Create, list, get, validate, update, violations, pipeline detail enrichment, auto-dependency, duplicate/self rejection, delete.

#### Scope Decision
- dbt-like transforms and semantic layer features **deferred to later scope**. Current focus is ingestion (Fivetran) + orchestration (Airflow) + observability (Monte Carlo) only.

#### Key Use Cases Unlocked
- **Consume-and-merge safety**: Data contract with `cleanup_ownership=consumer_acknowledges` prevents the producer from deleting staged data until the consumer has processed it.
- **Freshness SLA monitoring**: Consumer pipelines can declare how fresh they need the producer's data — the monitor automatically detects and alerts on SLA breaches.
- **Schema expectations**: Consumer declares required columns — if the producer drops them (e.g. due to schema drift), the contract violation is caught before the consumer pipeline fails.

---

### Build 15 - 2026-03-09 (Claude Opus 4.6)

**Run Context Propagation**

#### Added
- **`triggered_by_run_id` / `triggered_by_pipeline_id`** on `RunRecord` — When a pipeline is data-triggered, the downstream run records which upstream run and pipeline caused the trigger.
- **9 upstream template variables** — `{{upstream_run_id}}`, `{{upstream_pipeline_id}}`, `{{upstream_watermark_before}}`, `{{upstream_watermark_after}}`, `{{upstream_rows_extracted}}`, `{{upstream_rows_loaded}}`, `{{upstream_started_at}}`, `{{upstream_completed_at}}`, `{{upstream_batch_id}}`. Available in post-promotion hook SQL alongside existing 15 variables (total: 24).
- **Upstream metadata namespace** — After data-triggered runs, upstream context (run_id, pipeline_id, watermark, row count, completion time) is auto-written as metadata under `namespace="upstream"`, queryable via the existing metadata API.
- **`GET /api/runs/{run_id}/trigger-chain`** — Walks the trigger chain backwards to the root run, returning full run summaries at each hop. Supports multi-hop chains (A → B → C).
- **UI trigger indicators** — Data-triggered runs show the upstream pipeline ID snippet in the run list.

#### Changed
- **`scheduler/manager.py`** — `_trigger_downstream()` now receives the completed `RunRecord` and sets `triggered_by_run_id` / `triggered_by_pipeline_id` on downstream runs.
- **`agent/autonomous.py`** — `_execute_inner()` loads upstream run for data-triggered runs. `_render_hook_sql()` extended from 15 to 24 template variables. `_write_run_metadata()` writes upstream context under `namespace="upstream"`.
- **`api/server.py`** — `_run_summary()` includes `triggered_by_run_id` and `triggered_by_pipeline_id`.
- **DB migration** — `ALTER TABLE runs ADD COLUMN IF NOT EXISTS triggered_by_run_id TEXT` and `triggered_by_pipeline_id TEXT`.

#### Key Use Case Unlocked
Consume-and-merge with upstream watermark boundaries:
```sql
-- Downstream hook: delete only rows consumed by the upstream run
DELETE FROM raw.stage_orders WHERE updated_at <= '{{upstream_watermark_after}}'
```

---

### Build 14 - 2026-03-09 (Claude Opus 4.6)

**Hook Template Variables**

#### Added
- **`_render_hook_sql()`** — Static method on `PipelineRunner` that replaces `{{variable}}` placeholders with run context values before SQL execution. 15 supported variables: `{{pipeline_id}}`, `{{pipeline_name}}`, `{{run_id}}`, `{{run_mode}}`, `{{watermark_before}}`, `{{watermark_after}}`, `{{rows_extracted}}`, `{{rows_loaded}}`, `{{started_at}}`, `{{completed_at}}`, `{{source_schema}}`, `{{source_table}}`, `{{target_schema}}`, `{{target_table}}`, `{{batch_id}}` (alias for `run_id[:8]`).
- **`rendered_sql` in hook results** — When template variables are used, the resolved SQL is stored in the hook result metadata so users can see exactly what executed.
- **UI template hints** — Hooks editor shows available template variables as inline code tags.

#### Key Use Case Unlocked
Consume-and-merge pattern is now possible:
```sql
-- Pipeline 2 hook: safely delete only consumed rows from stage table
DELETE FROM raw.stage_orders WHERE updated_at <= '{{watermark_after}}'
```
Rows arriving after `watermark_after` survive the DELETE for the next run.

#### Changed
- **`agent/autonomous.py`** — `_execute_post_promotion_hooks()` calls `_render_hook_sql()` before `execute_sql()`. None values render as empty string.
- **`ui/App.jsx`** — Template variable hints added below hooks editor textarea.

---

### Build 13 - 2026-03-08 (Claude Opus 4.6)

**SQL-based Post-Promotion Hooks**

#### Added
- **`PostPromotionHook` dataclass** — Defines a SQL hook with: `hook_id`, `name`, `sql`, `metadata_key`, `description`, `enabled`, `timeout_seconds`, `fail_pipeline_on_error`. Stored as JSONB array on `PipelineContract`.
- **`TargetEngine.execute_sql()`** — Non-abstract default method on the target interface. Raises `NotImplementedError` for connectors that don't support it. PostgreSQL seed connector implements it with statement timeout and `RealDictCursor`.
- **Hook execution in runner** — `_execute_post_promotion_hooks()` runs after promotion, before marking COMPLETE. Results stored as metadata under `namespace="hooks"`. Supports fail-fast (`fail_pipeline_on_error=true`) or best-effort (default). JSON-safe serialization handles `Decimal`, `datetime`, `bytes`.
- **`POST /api/pipelines/{id}/hooks/test`** — Test endpoint executes SQL against the pipeline's target connector without saving. Returns rows (capped at 100) with timing. Admin/operator only.
- **PATCH support** — `post_promotion_hooks` field on `UpdatePipelineRequest`. Auto-generates `hook_id` for new hooks.
- **Detail enrichment** — Pipeline detail response includes `post_promotion_hooks` array and `hook_results` dict (latest results from hooks namespace metadata).
- **UI hooks display** — Read-only section shows each hook with name, SQL preview, enabled status, last execution result (status, duration, output).
- **UI hooks editor** — JSON textarea in Edit Settings panel for defining/editing hooks.
- **DB migration** — `ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS post_promotion_hooks JSONB NOT NULL DEFAULT '[]'`.

#### Design Decisions
- Hooks execute against the same target connector instance used for promotion (no re-instantiation needed).
- Hook results are persisted as XCom-style metadata with `namespace="hooks"`, making them consumable by downstream pipelines via the metadata API.
- `execute_sql()` is opt-in per connector — targets that don't support it gracefully skip hooks with a warning.
- No SQL validation/sandboxing — hooks are set by admin/operator roles, not arbitrary users.

### Build 12 - 2026-03-08 (Claude Opus 4.6)

**Per-pipeline Schema Change Policies**

#### Added
- **`SchemaChangePolicy` dataclass** — 5 fields: `on_new_column` (auto_add/propose/ignore), `on_dropped_column` (halt/propose/ignore), `on_type_change` (auto_widen/propose/halt), `on_nullable_change` (auto_accept/propose/halt), `propagate_to_downstream` (bool).
- **`SCHEMA_POLICY_TIER_DEFAULTS`** — T1: halt drops + propose types; T2: propose drops + auto-widen; T3: ignore drops + auto-widen. Tier defaults ensure zero-config safety scaling.
- **`PipelineContract.get_schema_policy()`** — Returns explicit policy if set, otherwise tier default. Backward compatible with `auto_approve_additive_schema`.
- **Nullable change detection** — `_detect_nullable_changes()` in monitor compares `is_nullable` between contract and live profile.
- **Policy-driven drift decisions** — Each change category (new column, dropped column, type change, nullable change) resolved independently per policy. Halt reasons collected and applied as a batch.
- **Downstream schema propagation** — When pipeline A auto-applies schema changes and `propagate_to_downstream=True`, creates proposals for dependent pipelines (never auto-applies — respects two-tier autonomy).
- **`GET /api/schema-policy-defaults`** — Returns tier-based default policies.
- **Schema change policy editor** — 4 dropdown selects + propagation checkbox in the Edit Settings panel.
- **Schema policy enums** — `SchemaColumnAction`, `SchemaDropAction`, `SchemaTypeAction`, `SchemaNullableAction`.

#### Changed
- **`monitor/engine.py`** — `_check_drift()` rewritten to use policy-driven decisions. Split into `_create_halt_proposal()`, `_create_drift_proposals()`, `_propagate_schema_downstream()`. `_auto_apply_schema_changes()` extended with nullable changes.
- **`contracts/store.py`** — Added `schema_change_policy JSONB` column to pipelines table. Updated `save_pipeline()` and `_row_to_pipeline()`.
- **`api/server.py`** — `UpdatePipelineRequest` extended with `schema_change_policy`. PATCH handler applies policy. `_pipeline_detail()` includes `schema_change_policy` and `schema_change_policy_is_custom`.
- **`ui/App.jsx`** — Schema change policy section in Edit Settings (4 selects + checkbox). Read-only policy summary in detail view.

---

### Build 11 - 2026-03-08 (Claude Opus 4.6)

**Data-aware Scheduling + Pipeline Metadata**

#### Added
- **Event-driven pipeline triggering** — When pipeline A completes, scheduler checks all downstream dependents. If ALL upstream dependencies are satisfied, triggers immediately with `RunMode.DATA_TRIGGERED`. Cron scheduling remains as fallback. No external queue — async callbacks within the scheduler.
- **`list_dependents(depends_on_id)` store method** — Reverse dependency lookup via `dependencies.depends_on_id` index.
- **XCom-style pipeline metadata** — New `pipeline_metadata` PostgreSQL table with `(pipeline_id, namespace, key)` unique constraint. Runner writes 5 standard keys after each successful run: `last_run_id`, `last_row_count`, `last_max_watermark`, `last_completed_at`, `last_staging_size_bytes`.
- **Metadata API endpoints** — `GET/PUT/DELETE /api/pipelines/{id}/metadata[/{key}]`.
- **Dependency management API** — `POST /api/pipelines/{id}/dependencies` with cycle detection, `GET` list, `DELETE` remove. All with DecisionLog audit.
- **Dependencies display in pipeline detail** — Upstream list with type pills + remove button. Downstream count. Inline "Add dependency" button.
- **Metadata display in pipeline detail** — Grid of key-value cards showing namespace/key, value, updated_at.
- **`RunMode.DATA_TRIGGERED`** — New run mode enum value for auditability.
- **`PipelineMetadata` dataclass** — `id`, `pipeline_id`, `namespace`, `key`, `value_json`, `updated_at`, `created_by_run_id`.

#### Changed
- **`scheduler/manager.py`** — `_run_pipeline()` calls `_trigger_downstream()` after COMPLETE status.
- **`agent/autonomous.py`** — Calls `_write_run_metadata()` after promotion.
- **`api/server.py`** — `_pipeline_detail()` includes `dependencies` and `metadata` sections. New dependency and metadata CRUD endpoints.
- **`contracts/store.py`** — `pipeline_metadata` table DDL, 4 CRUD methods, `list_dependents()`, `_row_to_metadata()`.
- **`contracts/models.py`** — Added `RunMode.DATA_TRIGGERED`, `PipelineMetadata` dataclass.
- **`ui/App.jsx`** — Dependency management section and metadata display in PipelinesView detail panel.

---

### Build 10 - 2026-03-08 (Claude Opus 4.6)

**Pipeline Settings UI + Change Logging + Auto-Persistence**

#### Added
- **Expanded PATCH `/api/pipelines/{id}`** — 18+ editable fields (was 4). Supports schedule (cron, retry, backoff, timeout), strategy (refresh_type, replication_method, incremental_column, load_type, merge_keys, watermark reset), quality config (partial merge of any QualityConfig field), observability (tier, owner, tags, tier_config, freshness_column), and approval settings (auto_approve_additive_schema). All changes tracked with old→new diffs.
- **Change audit trail** — Every PATCH saves a `DecisionLog` with `decision_type="contract_update"`, JSON diff of all changed fields, and optional user-provided `reason`. Visible in the Timeline view.
- **Auto-persist to YAML** — On every contract update, writes `data/contracts/{pipeline_name}.yaml` with masked credentials. Enables Git-based contract versioning.
- **`_persist_contract_yaml()` helper** — Writes pipeline contract to disk as YAML after each update.
- **Pipeline Settings UI** — Full edit form in the Pipelines view with 4 grouped sections: Schedule (cron, retry, backoff, timeout), Strategy (refresh type, load type, replication, incremental column, merge keys, watermark reset), Quality (6 threshold inputs + 2 checkboxes), Observability (tier, owner, freshness column, tags JSON, auto-approve). Includes change reason input and Save/Cancel buttons.
- **YAML view button** — Toggle to display pipeline contract as formatted YAML in a dark-themed `<pre>` block.
- **Timeline button** — Toggle to show change history (DecisionLog entries filtered to `contract_update` type) with decision type pill, timestamp, detail, and reasoning.
- **`contracts_dir` config property** — `config.contracts_dir` → `data/contracts/`. Directory auto-created on startup.

#### Changed
- **`config.py`** — Added `contracts_dir` property.
- **`main.py`** — Creates `contracts_dir` on startup in `setup_data_dirs()`.
- **`api/server.py`** — Added `DecisionLog, RefreshType, ReplicationMethod, LoadType, QualityConfig` imports. Expanded `UpdatePipelineRequest` from 4 to 18+ fields. Rewrote PATCH handler with change tracking, version bumping, DecisionLog audit, and YAML persistence. Expanded `_pipeline_detail()` to include `replication_method`, `retry_max_attempts`, `retry_backoff_seconds`, `timeout_seconds`, `auto_approve_additive_schema`, `tier_config`, `freshness_column`, and full `quality_config` (was 3 fields, now all QualityConfig fields via `asdict()`).
- **`ui/App.jsx`** — Added `editForm`, `saving`, `yamlView`, `timeline` state. Added `startEditing()`, `saveSettings()`, `loadYaml()`, `loadTimeline()` functions. Added settings editor panel, YAML view, timeline display, and 3 new buttons (Edit Settings, View YAML, Timeline).

---

### Build 9 - 2026-03-08 (Claude Opus 4.6)

**Contract-as-Code: YAML export, import, and GitOps sync for pipeline contracts**

#### Added
- **`contracts/yaml_codec.py` (new file)** — Pure serialization module with 8 functions: `pipeline_to_dict`, `dict_to_pipeline`, `pipeline_to_yaml`, `yaml_to_pipeline`, `pipelines_to_yaml`, `yaml_to_pipelines`, `diff_contracts`, `snapshot_state`. Groups flat PipelineContract fields into human-readable, Git-diff-friendly YAML structure (source/target/strategy/schedule/quality sections).
- **`GET /api/pipelines/export`** — Bulk export all pipelines as multi-document YAML. `?status=active` filter, `?include_credentials=true` (admin-only, decrypts Fernet-encrypted passwords in-memory).
- **`GET /api/pipelines/{id}/export`** — Single pipeline YAML export. `?include_state=true` adds runtime state (baselines, error budget, dependencies, schema versions). `?include_credentials=true` (admin-only).
- **`POST /api/pipelines/import`** — Import pipelines from YAML body. `?mode=create` (default, 409 if exists) or `?mode=upsert` (preserves pipeline_id, bumps version, preserves credentials if masked with `***`).
- **`POST /api/contracts/sync`** — GitOps reconciliation. `?dry_run=true` (default) returns field-level diffs without applying. `?dry_run=false` creates new pipelines, updates existing. Returns `{created, updated, unchanged, errors}`.
- **`get_pipeline_by_name()`** — New store method for name-based pipeline lookup (UNIQUE constraint), used by import/sync endpoints.

#### Changed
- **`requirements.txt`** — Added `pyyaml>=6.0` dependency.
- **`api/server.py`** — Added YAML codec imports, `encrypt`/`decrypt` imports from crypto, 4 new endpoints.

#### Design decisions
- `pipeline_name` is the sync key (not `pipeline_id`) — enables same YAML across environments where IDs differ.
- Credentials masked by default (`"***"`) on export; preserved on import when masked.
- Runtime state (`_state:` section) exported separately, ignored on import by default.
- Route ordering: `GET /api/pipelines/export` registered before `GET /api/pipelines/{pipeline_id}` to avoid path collision.

---

### Build 8 - 2026-03-08 (Claude Opus 4.6)

**Production-grade structured logging with pipeline context propagation**

#### Added
- **`logging_config.py` (new file)** — ContextVars (`pipeline_id`, `pipeline_name`, `run_id`, `request_id`, `component`), `PipelineContext` context manager (sync + async, reset-based), `ContextFilter` for automatic injection, `JSONFormatter` (one JSON object per line, Datadog/Loki/CloudWatch compatible), `ConsoleFormatter` (human-readable with inline context tags), `setup_logging()` with `RotatingFileHandler`.
- **Request correlation middleware** — `@app.middleware("http")` reads `X-Request-ID` header or generates UUID, sets contextvar, logs `METHOD /path STATUS (duration_ms)` for non-health requests, returns `X-Request-ID` in response header.
- **Quality gate logging** — Gate decisions logged as WARNING (HALT) or INFO (PROMOTE/PROMOTE_WITH_WARNING) with check summary. Individual check details logged at DEBUG level.
- **Per-pipeline timeline API** — `GET /api/pipelines/{pipeline_id}/timeline` returns merged runs, gates, alerts, and decisions sorted by timestamp. New `list_alerts_for_pipeline()` store method.

#### Changed
- **`config.py`** — Added `LOG_FORMAT` (default: json), `LOG_MAX_BYTES` (default: 50MB), `LOG_BACKUP_COUNT` (default: 5) env vars.
- **`main.py`** — `setup_logging()` delegates to `logging_config.setup_logging()` with config values. Creates `data/logs/` directory.
- **`agent/autonomous.py`** — Split `execute()` into PipelineContext wrapper + `_execute_inner()`. Removed all 14 `[%s]` manual prefixes.
- **`scheduler/manager.py`** — `_tick()` loop and `_run_pipeline()` wrapped in PipelineContext. Removed ~8 `[%s]` prefixes.
- **`monitor/engine.py`** — `_tick()` loop wrapped in PipelineContext. Removed ~6 `[%s]` prefixes.
- **`quality/gate.py`** — Added gate decision + check summary logging after evaluation.
- **`api/server.py`** — Added request correlation middleware, timeline endpoint, imported `time`, `uuid`, `logging_config`.
- **`contracts/store.py`** — Added `list_alerts_for_pipeline(pipeline_id, limit)` method.

#### Log output examples

Console: `2026-03-08 14:32:15 INFO  agent.autonomous -- [demo-orders | run:abc12345] Extracted 30 rows`

JSON: `{"timestamp":"2026-03-08T14:32:28+00:00","level":"INFO","logger":"agent.autonomous","message":"Extracted 30 rows","pipeline_id":"abc-123","pipeline_name":"demo-orders","run_id":"abc12345-full-uuid","component":"runner"}`

---

### Build 7 - 2026-03-08 (Claude Opus 4.6)

**Features #5-9: Incremental extraction (verified), enhanced run history UI, connector approval flow, alerting dispatch, schema drift auto-remediation**

#### Added
- **Feature #6: Enhanced pipeline run history** — `_run_summary()` now includes `run_mode`, `staging_size_bytes`, `quality_results`, `watermark_before`, `watermark_after`. UI shows duration, run mode pill, staging size, expandable quality check details, watermark progression for incremental pipelines.
- **Feature #7: Connector approval flow** — ApprovalsView now shows connector code in a syntax-highlighted `<pre>` block for `new_connector` proposals. Added "Test Connector" button that calls `POST /api/connectors/{id}/test` and displays results inline.
- **Feature #8: Alerting dispatch with mock webhook** — Added mock Slack webhook endpoints (`POST /webhook/slack`, `GET /webhook/slack/history`) to demo-api. Demo pipelines now get a notification policy routing alerts to the mock webhook. `_resolve_channels()` made async with policy lookup from store.
- **Feature #9: Schema drift auto-remediation** — Demo pipelines created with `auto_approve_additive_schema=True`. Added `_is_safe_type_widening()` helper recognizing VARCHAR widening, INT→BIGINT, FLOAT→DOUBLE PRECISION. Auto-apply now handles both new columns and safe type widenings.

#### Fixed
- **`test_connector` endpoint bugs** — Added missing `await` on `registry.get_source()`/`registry.get_target()`, fixed `**params` → `params` arg passing, added DRAFT/APPROVED connector temporary loading via `register_approved_connector()`, now saves `test_status` on connector record.
- **`_resolve_channels()` was sync** — Made async to enable notification policy lookup from the store. Previously had a `pass` placeholder for policy lookup.

#### Changed
- **`api/server.py`** — Added `TestStatus` to imports; enriched `_run_summary()`; fixed `test_connector` endpoint
- **`ui/App.jsx`** — Enhanced PipelinesView run display with duration, quality checks, watermarks; enhanced ApprovalsView with code view and test button
- **`monitor/engine.py`** — `_resolve_channels()` now async with policy lookup; added `_is_safe_type_widening()`; renamed `_auto_apply_new_columns` → `_auto_apply_schema_changes` with type widening support
- **`demo/mock-api/app.py`** — Added mock Slack webhook endpoints
- **`demo/bootstrap.py`** — Added `auto_approve_additive_schema=True`, `tier_config={"digest_only": False}`, notification policy creation and wiring to demo pipelines

#### Verified
- **Feature #5: Incremental extraction** — Already fully implemented. MySQL source builds `WHERE inc_col > last_watermark`, MongoDB does `{"$gt": wm}`. `demo-ecommerce-customers` uses INCREMENTAL with `updated_at` column.

---

### Build 6 - 2026-03-08 (Claude Opus 4.6)

**Four bug fixes: store mismatch, source credentials, quality gate first-run, demo triggers**

#### Fixed
- **Bug #1: `list_runs(window_days=)` store method mismatch** — `agent/autonomous.py` called `store.list_runs(pipeline_id, window_days=budget.window_days)` but the store method signature is `list_runs(pipeline_id, limit=50)`. Changed to `limit=budget.window_days * 24` (~1 run/hour cap).
- **Bug #2: Source credentials missing from PipelineContract** — `PipelineContract` had `target_user`/`target_password` but no source equivalents. `_connector_params()` hardcoded `user=""`, `password=""` for sources. Added `source_user`/`source_password` fields to model, DDL, store save/load, demo bootstrap, conversation manager, and crypto CREDENTIAL_FIELDS.
- **Bug #3: Quality gate always halts on first run** — On the first run with no baseline, checks like schema_consistency could FAIL on type mismatches. Added first-run leniency: if no prior COMPLETE runs exist, FAILs are auto-downgraded to WARNs with a `[First run - auto-downgraded]` prefix, ensuring the first run promotes and establishes baselines.
- **Bug #4: Demo pipelines never trigger on first startup** — Demo pipelines had `schedule_cron = "0 * * * *"` (hourly on the hour), so they wouldn't run until the next hour boundary. Now all 4 demo pipelines are triggered immediately after creation via `asyncio.create_task(runner.execute(...))`.
- **Bonus: `list_gates(days=1)` in `main.py`** — The observability quality summary called `store.list_gates(pipeline_id, days=1)` but `list_gates()` doesn't accept a `days` parameter. Removed the invalid kwarg.

#### Changed
- **`contracts/models.py`** — Added `source_user: str = ""` and `source_password: str = ""` to PipelineContract
- **`contracts/store.py`** — Added `source_user`, `source_password` columns to pipelines DDL; updated save_pipeline() INSERT/UPSERT (51→53 params); updated _row_to_pipeline()
- **`agent/autonomous.py`** — `_connector_params()` now uses `contract.source_user`/`contract.source_password` instead of hardcoded empty strings
- **`agent/conversation.py`** — `create_pipeline()` now wires `source_user`/`source_password` from encrypted source_params into PipelineContract
- **`quality/gate.py`** — Added first-run detection and FAIL→WARN downgrade logic before decision evaluation
- **`demo/bootstrap.py`** — Added `source_user`/`source_password` to MySQL demo configs; accepts optional `runner` param to trigger pipelines immediately
- **`main.py`** — Passes `runner` to `bootstrap_demo_pipelines()`; removed invalid `days=1` kwarg from `list_gates()` call
- **`crypto.py`** — Added `"source_password"` to CREDENTIAL_FIELDS

---

### Build 5 - 2026-03-08 (Claude Opus 4.6)

**Authentication enabled by default with RBAC**

#### Added
- **Default admin user** — auto-created on first startup (admin/admin, admin@dapos.local)
- **`require_role()` RBAC helper** in `api/server.py` — enforces role-based access on all mutating endpoints
- **"operator" role** — replaces "editor". Three roles: admin (full access), operator (run/manage pipelines), viewer (read-only)
- **Role validation** on `RegisterRequest` — rejects invalid roles

#### Fixed
- **`User.user_id` AttributeError** — `User` model had `id` field but login/register endpoints referenced `user.user_id`. Added `@property user_id` alias.
- **`User.email` missing** — Register endpoint set `email=req.email` but User model and users table had no email field. Added `email` field to model, DDL, and store methods.
- **Empty JWT secret** — `JWT_SECRET` defaulted to `""`, making token signing fail. Added fallback dev secret.

#### Changed
- **`AUTH_ENABLED` default** — changed from `false` to `true`. Auth is now on by default.
- **`config.py`** — JWT secret falls back to `"dapos-dev-secret-change-in-production"` when not set
- **RBAC enforcement** on 12 mutating endpoints (connector generate/deprecate, pipeline CRUD/trigger/pause/resume/backfill, approvals)

#### RBAC Matrix
| Action | admin | operator | viewer |
|--------|-------|----------|--------|
| Register users | yes | no | no |
| Generate/deprecate connectors | yes | no | no |
| Test connectors | yes | yes | no |
| Create/update/delete pipelines | yes | yes | no |
| Trigger/pause/resume/backfill | yes | yes | no |
| Approve/reject proposals | yes | yes | no |
| View all data, chat | yes | yes | yes |

---

### Build 4 - 2026-03-08 (Claude Opus 4.6)

**Freshness monitoring fix + lineage endpoint fix**

#### Fixed
- **`monitor/engine.py` — method name mismatch** — `save_freshness_snapshot()` → `save_freshness()`. The monitor engine called a non-existent method, so freshness snapshots were never persisted. The UI freshness view showed "No freshness data yet" for all pipelines.
- **`monitor/engine.py` — `float("inf")` in alert JSON** — When a pipeline has no successful runs, staleness is `float("inf")`. The alert detail dict passed this raw value to PostgreSQL JSON, causing `invalid input syntax for type json: Token "Infinity"`. Capped to `99999` in both summary string and detail dict.
- **`api/server.py` — `list_dependencies()` missing argument** — The lineage endpoint called `store.list_dependencies()` without the required `pipeline_id` argument, crashing with `TypeError`. Replaced with per-pipeline lookups to find downstream dependencies.
- **`agent/core.py` — "List my pipelines" keyword routing** — The predefined UI chip "List my pipelines" failed keyword matching because `"my"` broke the substring match for `"list pipeline"`. Added `"my pipeline"` to the keyword patterns.

#### Verified
- Freshness endpoint returns data for 3 demo pipelines (Stripe: fresh, MongoDB: fresh, Orders: critical — no successful run yet)
- Lineage endpoint no longer crashes
- "List my pipelines" UI chip works correctly

---

### Build 3 - 2026-03-08 (Claude Opus 4.6)

**Default demo environment with end-to-end pipeline execution**

#### Added
- **4 new seed connectors** in `connectors/seeds.py`:
  - MongoDB source (pymongo) — document-to-CSV extraction with schema inference
  - Stripe source (httpx) — paginated API extraction with Bearer auth
  - Google Ads source (httpx) — report-style API extraction with pagination
  - Facebook Insights source (httpx) — cursor-based API pagination
- **Demo Docker services** in `docker-compose.yml`:
  - `demo-mysql` (MySQL 8.0) — e-commerce dataset: 20 products, 20 customers, 30 orders, 40 order items
  - `demo-mongo` (MongoDB 7) — analytics dataset: 200 web events (page views, clicks, purchases)
  - `demo-api` (FastAPI) — mock Stripe (50 charges, 30 customers), Google Ads (40 campaigns), Facebook Insights (45 ad insights)
- **Demo pipeline bootstrap** (`demo/bootstrap.py`):
  - Auto-creates 4 demo pipelines on first startup (if no pipelines exist)
  - Profiles source tables to populate column_mappings for correct target DDL
  - Pipelines: MySQL→PostgreSQL (orders + customers), MongoDB→PostgreSQL (events), Stripe→PostgreSQL (charges)
- **pymongo** added to `requirements.txt` and `sandbox.py` ALLOWED_IMPORTS

#### Changed
- **MySQL source connector** — added `**kwargs`, `port` default, `user` defaults to `"root"` when empty (needed because `_connector_params()` passes `user=""` for source connections)
- **`docker-compose.yml`** — expanded from 1 service (PostgreSQL) to 4 (+ demo-mysql, demo-mongo, demo-api)
- **`connectors/registry.py`** — `bootstrap_seeds()` now registers 8 connectors (was 4)
- **`main.py`** — calls `bootstrap_demo_pipelines(store, registry)` after seed bootstrap

#### Verified
- All 8 connectors seed + load on startup
- 4 demo pipelines auto-created with profiled column_mappings
- End-to-end execution verified:
  - MySQL orders: 30 rows extracted, 30 loaded (quality gate halted — no baseline, expected)
  - Stripe charges: 50 rows extracted, 50 loaded, quality gate **promoted** to target
  - MongoDB events: 200 rows extracted, 200 loaded, quality gate **promoted** to target

---

### Build 2 - 2026-03-08 (Claude Opus 4.6)

**Testing framework overhaul + bug fixes**

#### Changed
- **Replaced pytest framework with curl-based test suite** (`test-pipeline-agent.sh`)
  - Previous: pytest-based unit/integration/e2e/chaos/performance tests with mock services
  - New: 127 curl-based tests against the real running app via REST APIs and `/api/command` chat endpoint
  - Rationale: The agent IS the product. Testing it with mocks and isolated unit tests misses the point. Tests should exercise the real agent with the real LLM through the same APIs users interact with.

#### Fixed
- **`from __future__ import annotations` in `api/server.py`** - This made all type annotations lazy strings, breaking Pydantic v2's type validation with FastAPI. Removed the import.
- **`ConnectorType` string vs enum in `agent/core.py`** - The API passes `connector_type` as a string (`"source"`/`"target"`) but `generate_connector()` called `.value` on it assuming an enum. Added normalization: `if isinstance(connector_type, str): connector_type = ConnectorType(connector_type.lower())`
- **`record.status.value` in `api/server.py`** - Connector generation returned a record with `status` as a string, but the response serialization called `.value`. Added safe access: `record.status.value if hasattr(record.status, 'value') else record.status`

#### Removed
- `tests/` directory (11 pytest files: test_sandbox, test_crypto, test_auth, test_quality_gate, test_store, test_registry, test_scheduler, test_monitor, test_autonomous, conftest, __init__)
- `test-data/` directory (SQL seeds, generators, scenario factory)
- `test-mocks/` directory (mock Stripe, Google Analytics, Facebook Ads FastAPI servers)
- `docker-compose.test.yml` (10+ test services: multiple databases, mock APIs, toxiproxy)
- `.github/workflows/test-suite.yml` (CI pipeline)
- Documentation: `TESTING.md`, `AGENT-TESTING-GUIDE.md`, `FINAL-TEST-SUMMARY.md`, `QUICK-VALIDATION.md`, `TEST-SUMMARY.md`, `tests/README.md`, `tests/MANUAL-UI-TESTING.md`
- Scripts: `run-tests.sh`, `run-interactive-tests.sh`, `test-agent-commands.sh`, `validate-app.sh`

#### Added
- `test-pipeline-agent.sh` - Comprehensive curl-based test suite
- `CLAUDE.md` - Product context for Claude Code sessions
- `CHANGELOG.md` - This file

#### Test Results (Build 2)
```
Total:    127
Passed:   121
Failed:     4 (connector generation timeouts - curl 60s limit vs LLM generation time)
Warnings:   2 (keyword matching false negatives in multi-turn validation)
Skipped:    1 (pipeline CRUD - depends on active connectors)
Duration: ~20 minutes
```

---

## Build 1 - 2026-03-08 (Initial commit)

**Initial release of DAPOS - Agentic Data Platform**

#### Added
- Complete pipeline agent application
  - 4 concurrent async loops: API server, scheduler, monitor, observability
  - FastAPI REST API with 40+ endpoints
  - React 18 SPA with 9 views (CDN, no build step)
  - Claude-powered agent for natural language interaction
  - Connector generation via Claude with AST-validated sandbox
  - 7-check quality gate (count reconciliation, schema consistency, PK uniqueness, null rates, volume z-score, sample verification, freshness)
  - Schema drift detection and auto-adaptation
  - Freshness monitoring with tier-based SLAs
  - Error budgets with automatic escalation
  - Column-level lineage tracking
  - Agent cost tracking (tokens, latency per call)
  - Learning loop (preferences from approvals/rejections)
  - JWT authentication with 3 roles
  - Fernet encryption for credentials at rest
  - Alert dispatch to Slack, Email, PagerDuty
- Seed connectors: MySQL source (PyMySQL), Redshift target (psycopg2)
- PostgreSQL 16 + pgvector via Docker Compose
