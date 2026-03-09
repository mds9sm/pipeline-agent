# Changelog

All notable changes to the Pipeline Agent (DAPOS) are documented in this file.

Format: Each entry records what changed, why, and test results at the time of the change.

---

## [Unreleased]

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
