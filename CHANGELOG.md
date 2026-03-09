# Changelog

All notable changes to the Pipeline Agent (DAPOS) are documented in this file.

Format: Each entry records what changed, why, and test results at the time of the change.

---

## [Unreleased]

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
