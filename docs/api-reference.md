# API Reference

DAPOS exposes a REST API on port 8100 (configurable via `API_PORT`).

All endpoints require JWT authentication unless noted. Pass the token as `Authorization: Bearer <token>`.

## Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/login` | Login, returns JWT token |
| POST | `/api/auth/register` | Register new user (admin only) |

## Pipelines

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/pipelines` | List pipelines (optional `?status=active`) | viewer+ |
| POST | `/api/pipelines` | Create pipeline | operator+ |
| POST | `/api/pipelines/batch` | Batch create | operator+ |
| GET | `/api/pipelines/{id}` | Pipeline detail | viewer+ |
| PATCH | `/api/pipelines/{id}` | Update pipeline (18+ fields) | operator+ |
| POST | `/api/pipelines/{id}/trigger` | Trigger manual run | operator+ |
| POST | `/api/pipelines/{id}/pause` | Pause pipeline | operator+ |
| POST | `/api/pipelines/{id}/resume` | Resume pipeline | operator+ |
| GET | `/api/pipelines/{id}/runs` | List runs | viewer+ |
| GET | `/api/pipelines/{id}/quality` | Quality gate history | viewer+ |
| GET | `/api/pipelines/{id}/preview` | Preview pipeline config | viewer+ |

## Step DAGs (Build 18)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/pipelines/{id}/steps` | Get step definitions | viewer+ |
| GET | `/api/runs/{run_id}/steps` | Get step executions | viewer+ |
| POST | `/api/pipelines/{id}/steps/validate` | Validate step DAG | operator+ |
| GET | `/api/pipelines/{id}/steps/preview` | Preview execution order | viewer+ |

## Diagnostics (Build 24)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/pipelines/{id}/diagnose` | Root-cause diagnosis | viewer+ |
| POST | `/api/pipelines/{id}/impact` | Downstream impact analysis | viewer+ |
| GET | `/api/observability/anomalies` | Platform-wide anomaly scan | viewer+ |

## Connectors

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/connectors` | List connectors | viewer+ |
| GET | `/api/connectors/{id}` | Connector detail | viewer+ |
| POST | `/api/connectors/generate` | Generate connector via AI | admin |
| POST | `/api/connectors/{id}/test` | Test connector | operator+ |
| POST | `/api/connectors/{id}/deprecate` | Deprecate connector | admin |

## Approvals

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/approvals` | List pending proposals | viewer+ |
| POST | `/api/approvals/{id}` | Approve/reject proposal | operator+ |

## Observability

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/freshness` | All pipeline freshness | viewer+ |
| GET | `/api/observability/freshness/{id}/history` | Freshness time-series | viewer+ |
| GET | `/api/alerts` | List alerts | viewer+ |
| GET | `/api/costs` | Agent cost logs | viewer+ |
| GET | `/api/lineage/{id}` | Pipeline lineage | viewer+ |
| GET | `/api/dag` | Full dependency graph | viewer+ |

## Data Contracts (Build 16)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/data-contracts` | Create contract | operator+ |
| GET | `/api/data-contracts` | List contracts | viewer+ |
| GET | `/api/data-contracts/{id}` | Contract detail | viewer+ |
| PATCH | `/api/data-contracts/{id}` | Update contract | operator+ |
| DELETE | `/api/data-contracts/{id}` | Delete contract | operator+ |
| POST | `/api/data-contracts/{id}/validate` | Validate contract | viewer+ |
| GET | `/api/data-contracts/{id}/violations` | List violations | viewer+ |

## Dependencies

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/dependencies` | Declare dependency | operator+ |
| GET | `/api/dependencies/{pipeline_id}` | List dependencies | viewer+ |
| DELETE | `/api/dependencies/{id}` | Remove dependency | operator+ |

## Chat / Agent

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/command` | Natural language command | viewer+ |
| POST | `/api/topology/design` | Design pipeline topology | viewer+ |

## YAML Contract-as-Code

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/contracts/yaml` | Export pipeline(s) as YAML | viewer+ |
| POST | `/api/contracts/yaml/import` | Import pipelines from YAML | operator+ |

## GitOps (Build 23)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/gitops/status` | Repo status | viewer+ |
| GET | `/api/gitops/log` | Commit log | viewer+ |
| GET | `/api/gitops/diff` | Diff between commits | viewer+ |
| GET | `/api/gitops/file` | File at commit | viewer+ |
| GET | `/api/gitops/pipelines/{id}/history` | Per-pipeline history | viewer+ |
| POST | `/api/gitops/restore` | Restore DB from repo (admin, dry-run default) | admin |

## Data Catalog (Build 26)

### Search & Browse

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/catalog/search` | Search catalog (tables, columns, tags, context) | viewer+ |
| GET | `/api/catalog/tables/{id}` | Full table detail (columns, lineage, trust, quality) | viewer+ |
| GET | `/api/catalog/trust/{id}` | Trust score breakdown with weights | viewer+ |
| GET | `/api/catalog/columns` | Search columns across all tables | viewer+ |
| GET | `/api/catalog/stats` | Catalog-wide statistics | viewer+ |

### Semantic Tags

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/catalog/tables/{id}/tags` | Get semantic tags | viewer+ |
| POST | `/api/catalog/tables/{id}/tags/infer` | AI-infer semantic tags | operator+ |
| PUT | `/api/catalog/tables/{id}/tags` | Bulk set/override tags | operator+ |
| PATCH | `/api/catalog/tables/{id}/tags/{column}` | Per-column tag override | operator+ |

### Business Context

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/catalog/tables/{id}/context/questions` | AI-generated context questions | viewer+ |
| PUT | `/api/catalog/tables/{id}/context` | Save business context answers | operator+ |

### Trust Weights

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| PUT | `/api/catalog/tables/{id}/trust-weights` | Set per-pipeline trust weights | operator+ |
| DELETE | `/api/catalog/tables/{id}/trust-weights` | Reset to default weights | operator+ |

### Alert Narratives

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/observability/alerts/{id}/narrative` | Generate alert narrative via AI | viewer+ |

## Context API (Build 28)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/runs/{run_id}/context` | Full aggregated run context (upstream chain, quality summary, metadata) | viewer+ |
| GET | `/api/pipelines/{pipeline_id}/context-chain` | Upstream dependency DAG context chain | viewer+ |

> **Note:** The existing `PATCH /api/pipelines/{pipeline_id}` endpoint also accepts the `auto_propagate_context` boolean field (operator+) to toggle automatic context propagation to downstream pipelines.

## SQL Transforms (Build 29)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/transforms` | Create transform | operator+ |
| GET | `/api/transforms` | List transforms (optional `?pipeline_id=` filter) | viewer+ |
| GET | `/api/transforms/{transform_id}` | Transform detail | viewer+ |
| PATCH | `/api/transforms/{transform_id}` | Update transform | operator+ |
| DELETE | `/api/transforms/{transform_id}` | Delete transform | admin |
| GET | `/api/transforms/{transform_id}/lineage` | Transform lineage refs | viewer+ |
| POST | `/api/transforms/generate` | AI-generate transform SQL from description | operator+ |
| POST | `/api/transforms/{transform_id}/validate` | Validate transform SQL | operator+ |
| POST | `/api/transforms/{transform_id}/preview` | Preview materialized output | operator+ |

## Metrics / KPIs (Build 31)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| POST | `/api/metrics/suggest/{pipeline_id}` | Agent suggests metrics for a pipeline | operator+ |
| POST | `/api/metrics` | Create metric | operator+ |
| GET | `/api/metrics` | List metrics (optional `?pipeline_id=` filter) | viewer+ |
| GET | `/api/metrics/{metric_id}` | Metric detail with snapshots and reasoning history | viewer+ |
| POST | `/api/metrics/{metric_id}/compute` | Execute metric SQL and store snapshot | operator+ |
| GET | `/api/metrics/{metric_id}/trend` | Agent trend interpretation | viewer+ |
| PATCH | `/api/metrics/{metric_id}` | Update metric (triggers reasoning refresh) | operator+ |
| DELETE | `/api/metrics/{metric_id}` | Delete metric and snapshots | admin |

## Business Knowledge & Agent (Build 32)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/agent/system-prompt` | Read-only agent system prompt | viewer+ |
| GET | `/api/settings/business-knowledge` | Get business knowledge (glossary, KPIs, instructions) | viewer+ |
| PUT | `/api/settings/business-knowledge` | Update business knowledge | admin |
| POST | `/api/settings/business-knowledge/parse-kpis` | Agent parses free-text KPIs into structured definitions | admin |

## Settings & Branding (Build 33)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/settings/branding` | Get branding config (app_name, logo_url) | viewer+ |
| PUT | `/api/settings/branding` | Update app name and/or logo URL | admin |
| POST | `/api/settings/branding/logo` | Upload logo as base64 data URL (max 256KB) | admin |

## Platform Analytics (Build 33)

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/analytics/activity?days=N` | Platform usage: heatmap, top users, operation costs, summary stats | viewer+ |
| GET | `/api/analytics/timeline?days=N` | GitHub-style activity timeline: pipeline runs, chats, connectors grouped by date | viewer+ |

## System

| Method | Endpoint | Auth? | Description |
|--------|----------|-------|-------------|
| GET | `/health` | No | Health check |
| GET | `/metrics` | No | Prometheus metrics |
