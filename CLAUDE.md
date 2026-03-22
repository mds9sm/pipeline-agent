# CLAUDE.md - Pipeline Agent Product Context

This file is loaded automatically by Claude Code at the start of every session.
It provides the product context, conventions, and test strategy needed to work on this codebase effectively.

---

## Product Identity

**DAPOS (Data Agent Platform Operating System)** — An AI-powered **full-scope data platform** where the agent IS the product. Covers **ingestion, transformation, orchestration, and observability** — not just EL, but ELT with native transform capabilities replacing external tools like dbt/Airflow.

Ships with 8 seed connectors (MySQL, SQLite, MongoDB, Stripe, Google Ads, Facebook Insights sources + PostgreSQL, Redshift targets) and 4 demo pipelines that auto-create on first startup. Additional connectors are generated through conversation with the Claude-powered agent.

## Product Vision & Scope

**Origin:** Started as an ingestion platform (extract + load). Now expanding to **full data platform** scope.

**What DAPOS replaces:** Fivetran (ingestion) + dbt (transforms) + Airflow (orchestration) + Monte Carlo (observability) — unified under one agentic system.

**Core capabilities (current + planned):**
- **Ingestion** — Extract from any source, load to any target. Connectors generated via AI. *(Implemented)*
- **Orchestration** — Pipeline DAGs with dependency-triggered execution, error budgets, cron + event-driven scheduling. *(Implemented)*
- **Quality & Observability** — 7-check quality gate, schema drift detection, freshness monitoring, alerting. *(Implemented)*
- **Post-promotion hooks** — SQL-based computed metadata after each pipeline run (XCom-style). *(Implemented)*
- **Transforms** — Native SQL transforms with ref(), var(), 4 materialization strategies, AI generation. *(Implemented)*
- **Composable pipeline steps** — Pipeline as a DAG of steps (extract, transform, gate, promote, cleanup) instead of fixed flow. *(Implemented)*
- **Data contracts** — Formalized producer/consumer relationships between pipelines with cleanup policies and retention. *(Implemented)*
- **DAG visualization** — UI-visible pipeline dependency graph with execution status. *(Implemented)*
- **Agent topology reasoning** — User describes a business problem, agent designs multi-pipeline architecture with the right patterns (consume-and-merge, fan-in, SCD, etc.). *(Implemented)*
- **Data catalog & AI enablement** — Built-in catalog with trust scores, semantic tags, business context, anomaly narratives. *(Implemented)*
- **MCP server** — Expose DAPOS to AI agents via Model Context Protocol (9 resources, 13 tools, 3 prompts). *(Implemented)*

**Key patterns the platform must support:**
| Pattern | Example |
|---------|---------|
| Consume & merge | Stage → upsert → cleanup consumed rows |
| Fan-in | Multiple sources → unified table |
| Fan-out | One source → multiple targets |
| SCD Type 2 | Historical change tracking |
| Quarantine | Bad rows → error table, good rows → production |
| Cascading aggregation | Raw → daily → monthly → dashboard |
| Conditional routing | Branch on quality/volume thresholds |
| Replay/reprocess | Re-run a time window idempotently |

**Architectural principles for expansion:**
1. **Run context flows downstream** — Watermarks, batch IDs, row counts propagate from producer to consumer pipelines as first-class data.
2. **Cleanup ownership is explicit** — Every intermediate table has a defined cleanup owner (producer TTL or consumer-after-processing).
3. **Intra-database optimization** — Same-database pipelines use SQL-native paths, not extract-to-file.
4. **Hooks reference run context** — Template variables (`{{watermark_after}}`, `{{run_id}}`) enable dynamic post-promotion logic.
5. **Agent proposes, human approves topology** — Two-tier autonomy extends to pipeline design: agent designs multi-pipeline solutions, human approves before creation.
6. **Idempotent by default** — Merge over append, watermark-bounded over full scan. Safe to re-run.
7. **GitOps-driven promotion** — Changes flow dev → staging → prod via git branches and PR review. Transform SQL requires approval; pipeline settings are tracked in changelog. Rollback = git revert.

## Architecture (single process, 4 async loops)

```
main.py
  |-- API Server (FastAPI, port 8100, serves REST + React SPA)
  |-- Scheduler (60s tick, cron eval, dependency graph, backfill/retry)
  |-- Monitor (5m tick, schema drift, freshness, alert dispatch)
  |-- Observability (30s tick, quality trends, daily digest at 9AM UTC)
  |
  v
PostgreSQL 16 + pgvector (all state: connectors, pipelines, runs, gates, preferences, lineage, costs)
```

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point - wires 4 async loops + dependency injection |
| `config.py` | Environment variable loading with defaults |
| `api/server.py` | FastAPI with 40+ endpoints, JWT auth, rate limiting |
| `agent/core.py` | Claude API calls: route_command, propose_strategy, generate_connector, reason_about_quality, parse_schedule, guided_pipeline_response |
| `agent/conversation.py` | Multi-turn onboarding/discovery flow |
| `agent/autonomous.py` | Pipeline execution state machine (PENDING -> COMPLETE/HALTED) with structured execution logging (13 steps) |
| `contracts/models.py` | All dataclasses + enums (PipelineContract, ConnectorRecord, RunRecord, etc.) |
| `contracts/store.py` | PostgreSQL CRUD via asyncpg for all entities |
| `connectors/registry.py` | exec()-based connector loader, validator, hot-reloader |
| `connectors/seeds.py` | 8 seed connectors as string constants (MySQL, SQLite, MongoDB, Stripe, Google Ads, Facebook Insights sources + PostgreSQL, Redshift targets) |
| `demo/bootstrap.py` | Auto-creates 4 demo pipelines on first startup with source profiling, semantic tags, business context, trust weights |
| `demo/mock-api/app.py` | Mock Stripe, Google Ads, Facebook Insights API service |
| `source/base.py` | Abstract SourceEngine interface (INTERFACE_VERSION = "1.0") |
| `target/base.py` | Abstract TargetEngine interface (INTERFACE_VERSION = "1.0") |
| `quality/gate.py` | 7-check quality gate typed against TargetEngine |
| `monitor/engine.py` | Drift detection, freshness, lineage impact, alert dispatch |
| `scheduler/manager.py` | Cron scheduler + topological sort + backfill + retry |
| `sandbox.py` | AST validation + restricted builtins + import whitelist |
| `auth.py` | JWT auth with 3 roles (admin, operator, viewer) |
| `crypto.py` | Fernet encryption for credentials at rest |
| `ui/App.jsx` | React 18 SPA (CDN, no build) - 11 views: Chat, Pipelines, Activity (expandable run details + execution logs), Freshness (time-series charts), Quality, Alerts, Lineage/DAG (consolidated with search/zoom/pan), Connectors, Settings, Sources, Docs |
| `gitops/repo.py` | Separate git repo manager for pipeline YAML + connector code versioning |
| `cli/__main__.py` | CLI interface — 14 commands, token caching, fuzzy pipeline resolution |
| `transforms/engine.py` | SQL transform engine — ref/var resolution, materialization, validation, lineage parsing |
| `mcp_server.py` | MCP server — 11 resources, 17 tools, 3 prompts; exposes DAPOS to AI agents via Model Context Protocol |
| `docs/` | Structured documentation — quickstart, architecture, concepts, API/CLI reference |

## Critical Design Constraints

1. **No static connector imports** — All connector code lives in PostgreSQL, loaded via `exec()`. Seeds and generated connectors are architecturally identical.
2. **Two-tier autonomy is a HARD constraint** — Runtime decisions (extract/load/promote) are always autonomous. Structural changes (connectors, schema, strategy, pipeline topology) always require human approval.
3. **Quality gate is connector-agnostic** — `quality/gate.py` types against `TargetEngine` interface, not specific databases.
4. **No LangChain, no external vector DB, no memory cache** — All state is PostgreSQL. Direct Claude API via httpx.
5. **AST-validated sandbox** — All generated connector code is statically analyzed before execution.
6. **No dbt, no Airflow** — DAPOS is the transform and orchestration layer. Native SQL transforms, not external tool delegation.
7. **Never delete unconsumed data** — Any cleanup hook must prove its boundary (watermark, batch_id, transaction scope). Static `DELETE FROM table` without bounds is rejected by the agent.
8. **Pipelines are composable** — Moving toward step DAGs (extract → transform → gate → promote → cleanup) instead of a fixed linear flow. New features should be built as composable steps.

## How to Start

```bash
docker compose up -d          # Start PostgreSQL, demo MySQL, demo MongoDB, mock SaaS APIs
ANTHROPIC_API_KEY=sk-... python main.py   # Start the app (seeds 8 connectors + 4 demo pipelines)
# Open http://localhost:8100  → Login: admin / admin
```

## Authentication & RBAC

Auth is **enabled by default** (`AUTH_ENABLED=true`). A default admin user (admin/admin) is auto-created on first startup.

**Roles:** `admin`, `operator`, `viewer`

| Action | admin | operator | viewer |
|--------|-------|----------|--------|
| Register users | yes | no | no |
| Generate/deprecate connectors | yes | no | no |
| Test connectors | yes | yes | no |
| Create/update/trigger pipelines | yes | yes | no |
| Approve/reject proposals | yes | yes | no |
| View all data, chat | yes | yes | yes |

**Config:**
- `AUTH_ENABLED` — `true` (default) or `false` to disable
- `JWT_SECRET` — set in production; dev fallback provided
- `JWT_EXPIRY_HOURS` — default 24

## Demo Environment

On first startup (no pipelines in DB), 4 demo pipelines are auto-created and **triggered immediately** (no waiting for cron schedule):

| Pipeline | Source | Target | Data |
|----------|--------|--------|------|
| demo-ecommerce-orders | demo MySQL (port 3307) | local PostgreSQL | 30 orders |
| demo-ecommerce-customers | demo MySQL (port 3307) | local PostgreSQL | 20 customers (incremental) |
| demo-analytics-events | demo MongoDB (port 27018) | local PostgreSQL | 200 web events |
| demo-stripe-charges | mock Stripe API (port 8200) | local PostgreSQL | 50 charges |

All 4 pipelines execute their first run immediately after creation. The quality gate uses first-run leniency (downgrades FAILs to WARNs) to ensure the first run promotes and establishes baselines.

Docker services: `demo-mysql` (e-commerce data), `demo-mongo` (analytics events), `demo-api` (mock Stripe/Google Ads/Facebook).

## Test Strategy

**Philosophy**: Test the real running app via curl, not isolated unit tests. The agent is the product - test it like a user would.

**Test script**: `./test-pipeline-agent.sh`

```bash
./test-pipeline-agent.sh              # Full suite (~165 tests, ~20 min)
./test-pipeline-agent.sh --api        # REST API endpoints only (~36 tests, fast)
./test-pipeline-agent.sh --sources    # Source connector requests + generation
./test-pipeline-agent.sh --targets    # Target connector requests + generation
./test-pipeline-agent.sh --chat       # Multi-turn conversations + agent understanding
```

### Test Coverage

| Category | Count | What it tests |
|----------|-------|---------------|
| Core API endpoints | 12 | health, metrics, connectors, pipelines, approvals, freshness, alerts, costs, policies, preferences, UI |
| Database sources | 16 | Oracle, SQL Server, MySQL, PostgreSQL, MongoDB, MariaDB, Cassandra, DynamoDB, CockroachDB, Redis, Elasticsearch, Neo4j, ClickHouse, SQLite, Teradata, DB2 |
| SaaS/API sources | 30 | Stripe, Google Ads, Facebook Insights/Ads, Salesforce, HubSpot, Shopify, GA4, Jira, Zendesk, Intercom, Twilio, SendGrid, Mailchimp, QuickBooks, Xero, Notion, Airtable, Slack, GitHub, LinkedIn Ads, Twitter Ads, TikTok Ads, Pinterest Ads, Marketo, Braze, Segment, Mixpanel, Amplitude, Snowplow |
| File/Cloud sources | 5 | S3, GCS, Azure Blob, SFTP, FTP |
| Streaming sources | 5 | Kafka, Kinesis, Pub/Sub, RabbitMQ, EventHub |
| Targets | 18 | PostgreSQL, Snowflake, BigQuery, Redshift, Databricks, ClickHouse, MySQL, SQL Server, Oracle, S3 Parquet, GCS, Azure Synapse, Firebolt, DuckDB, Delta Lake, Apache Iceberg, Elasticsearch, MongoDB |
| Multi-turn pipelines | 20 | Oracle->Snowflake, SQL Server->BigQuery, MySQL->PostgreSQL, Stripe->Snowflake, Google Ads->BigQuery, Facebook->Redshift, Salesforce->Databricks, MongoDB->PostgreSQL, HubSpot->Snowflake, Shopify->BigQuery, Kafka->ClickHouse, S3->Redshift, Jira->PostgreSQL, Zendesk->Snowflake, GitHub->BigQuery, GA4->Snowflake, LinkedIn Ads->Redshift, Elasticsearch->S3, PostgreSQL->Snowflake, DynamoDB->BigQuery |
| Agent understanding | 10 | capabilities, pipeline listing, connectors, monitoring, quality gates, schema drift, complex multi-source, scheduling, refresh strategy, error budgets |
| Connector generation | 9 | Oracle, SQL Server, Stripe, Google Ads, Facebook Insights, Snowflake, BigQuery, Redshift, Databricks |
| Pipeline CRUD | 18 | create, get, update (basic + expanded PATCH: schedule, strategy, quality merge, observability, watermark reset, version bump, no-change guard, detail fields), pause, resume, preview, runs, quality, lineage, error budgets |
| Timeline & logging | 3 | timeline events, decision events, X-Request-ID correlation (Build 8) |
| YAML contract-as-code | 6 | single export, export with state, bulk export, status filter, import duplicate detection, GitOps sync dry-run (Build 9) |
| Change audit | 4 | PATCH with audit reason, contract_update in timeline, YAML auto-persistence, credential masking (Build 10) |
| Approval workflow | 2 | list pending, approve |
| Data contracts | 11 | create, list, get, validate, update, violations, pipeline detail, auto-dep, duplicate/self rejection, delete (Build 16) |
| DAG visualization | 3 | graph endpoint, node structure, contract fields (Build 19) |
| Topology reasoning | 2 | design endpoint, chat routing (Build 20) |
| Source registry | 6 | register, list, get, update, delete, discover (Build 21) |
| Pipeline changelog | 3 | per-pipeline, global, included in detail (Build 21) |
| Interaction audit | 2 | list, export (Build 21) |
| GitOps API | 5 | status, log, diff, pipeline history, restore dry-run (Build 23) |
| Step DAG | 5 | steps definition, run steps, validate, preview, PATCH update (Build 18) |
| Agent diagnostics | 8 | diagnose (200+404), impact (200+404), anomalies, chat routing x3 (Build 24) |
| Data catalog & AI enablement | 15 | search, query, detail, trust, columns, stats, semantic tags (get/infer/override), context questions, context save, trust weights (set/reset), alert narratives (field+generate) (Build 26) |
| MCP server | 3 | server import, resource listing, tool listing (Build 27) |
| SQL transforms | 13 | CRUD (create, list, get, update, delete), lineage, AI generate, chat routing x2, demo transforms (count, VIEW, refs, pipeline) (Build 29) |

### Adding New Tests

All tests are in `test-pipeline-agent.sh`. To add a new source/target:
- Add an entry to the relevant bash array (SOURCES, SAAS_SOURCES, FILE_SOURCES, STREAMING_SOURCES, or TARGETS)
- Format: `"Display Name|Natural language prompt to the agent"`
- For pipeline tests, add to PIPELINES array: `"Name|src_keyword|tgt_keyword|Turn 1 prompt|Turn 2 prompt"`

## Coding Conventions

- **Python 3.11+**, async everywhere via asyncio
- **DO NOT** use `from __future__ import annotations` in files that define Pydantic models (breaks FastAPI)
- All enums are `(str, Enum)` with lowercase values
- When accepting enum parameters that may arrive as strings, normalize early: `if isinstance(x, str): x = MyEnum(x.lower())`
- Connector code must implement `SourceEngine` or `TargetEngine` abstract base class
- All credentials pass through `crypto.encrypt_dict()` before storage
- Use `contracts/models.py` for all data structures (dataclasses, not Pydantic for domain models)
- Pydantic is used ONLY for API request/response models in `api/server.py`

## Common Pitfalls

1. **Pydantic + `from __future__ import annotations`** = broken FastAPI. Never use it in `api/server.py`.
2. **`ConnectorType` passed as string** from API layer. Always normalize in receiving functions.
3. **`.value` on enums** - check `hasattr(x, 'value')` or normalize to enum first when the type is uncertain.
4. **Connector generation timeout** - Claude can take 30-60s to generate complex connectors. Set timeouts accordingly.
5. **Rate limiting** - connector generation is limited to 10/minute via slowapi.
6. **Method name mismatches between store and callers** — The store (`contracts/store.py`) is the source of truth for method names. Monitor, scheduler, and API code must match exactly (e.g., `save_freshness` not `save_freshness_snapshot`).
7. **`float("inf")` in JSON columns** — PostgreSQL rejects `Infinity` in JSON. Always cap with `min(value, 99999)` before storing in JSON/JSONB columns.
8. **Store methods require `pipeline_id`** — Most store methods like `list_dependencies()` require a `pipeline_id` argument. Never call them without it.
9. **Quality gate first-run leniency** — On the very first run (no prior COMPLETE runs), FAILs are auto-downgraded to WARNs so the first run establishes baselines. Don't rely on the first run's gate decision for regression testing.
10. **`source_user`/`source_password` on PipelineContract** — Source credentials are stored on the contract just like target credentials. When creating pipelines, pass source auth via `source_user`/`source_password` fields, not hardcoded empty strings.
11. **React hooks in `.map()` callbacks** — `useState`/`useEffect` inside `.map()` violates React's rules of hooks and crashes rendering. Extract to a proper component.
12. **Browser-based Babel + IIFEs in JSX** — `{(() => { ... })()}` patterns can fail with CDN Babel transpilation. Use extracted components instead.
13. **`checked_at` TEXT column in freshness_snapshots** — Column is TEXT not TIMESTAMP. Use `::timestamptz` cast when comparing to `NOW()` in SQL queries.
14. **`schedule_cron` not `schedule`** — PipelineContract uses `schedule_cron` field name. `p.schedule` will AttributeError.
15. **Cache busting for UI changes** — Static files need `?v=N` query params in `index.html` and `Cache-Control: no-cache` headers. Increment version on every UI change.

## Changelog

See `CHANGELOG.md` for a detailed record of all changes by build/session.
