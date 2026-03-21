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

## System

| Method | Endpoint | Auth? | Description |
|--------|----------|-------|-------------|
| GET | `/health` | No | Health check |
| GET | `/metrics` | No | Prometheus metrics |
