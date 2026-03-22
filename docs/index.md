# DAPOS Documentation

**Data Agent Platform Operating System** — An agentic data platform where you describe what you need and the AI builds, runs, and monitors your pipelines.

---

## Getting Started

| Guide | Description |
|-------|-------------|
| [Quickstart](quickstart.md) | Get DAPOS running in 5 minutes with Docker |
| [Installation](installation.md) | Production installation, dependencies, configuration |
| [First Pipeline](first-pipeline.md) | Create your first pipeline end-to-end |

## Core Concepts

| Topic | Description |
|-------|-------------|
| [Architecture](architecture.md) | Single-process async architecture, 4 event loops |
| [Pipelines](concepts/pipelines.md) | Pipeline contracts, lifecycle, refresh strategies |
| [Connectors](concepts/connectors.md) | AI-generated connectors, seed connectors, sandbox |
| [Step DAGs](concepts/step-dags.md) | Composable pipeline steps (extract, transform, gate, promote) |
| [Quality Gate](concepts/quality-gate.md) | 7-check quality gate, first-run leniency, baselines |
| [Scheduling](concepts/scheduling.md) | Cron, event-driven, dependency graph, backfill |
| [Data Contracts](concepts/data-contracts.md) | Producer/consumer relationships, SLA, cleanup guards |
| [Observability](concepts/observability.md) | Freshness, alerts, error budgets, anomaly reasoning |
| [Data Catalog](concepts/data-catalog.md) | Search, trust scores, semantic tags, business context |
| [Anomaly Narratives](concepts/anomaly-narratives.md) | AI-generated alert explanations |
| [SQL Transforms](concepts/transforms.md) | Native SQL transforms replacing dbt — ref(), var(), materialization |

## Operations

| Guide | Description |
|-------|-------------|
| [CLI Reference](cli-reference.md) | Command-line interface for pipeline operations |
| [API Reference](api-reference.md) | REST API endpoints (40+) |
| [Configuration](configuration.md) | Environment variables and defaults |
| [Authentication](authentication.md) | JWT auth, RBAC roles (admin/operator/viewer) |
| [GitOps](gitops.md) | Pipeline-as-code versioning with git |

## Agent Intelligence

| Topic | Description |
|-------|-------------|
| [Agent Overview](agent/overview.md) | How the AI agent works, two-tier autonomy |
| [Chat Interface](agent/chat.md) | Natural language commands, routing, conversation |
| [Diagnostics](agent/diagnostics.md) | Pipeline diagnosis, impact analysis, anomaly reasoning |
| [Topology Design](agent/topology.md) | Agent-designed multi-pipeline architectures |
| [Connector Generation](agent/connector-generation.md) | AI-generated source/target connectors |

## Advanced

| Topic | Description |
|-------|-------------|
| [Post-Promotion Hooks](advanced/hooks.md) | SQL hooks with 34 template variables |
| [Template Variables](advanced/template-variables.md) | Full reference for `{{variable}}` placeholders |
| [Schema Drift](advanced/schema-drift.md) | Detection, policies, auto-remediation |
| [Trust Scores](advanced/trust-scores.md) | Trust score formula, components, weight customization |
| [Multi-Environment](advanced/multi-environment.md) | Branch-per-environment, connection parameterization |
| [Disaster Recovery](advanced/disaster-recovery.md) | GitOps restore, crash recovery, stale run cleanup |
| [MCP Server](advanced/mcp-server.md) | Expose DAPOS to AI agents via Model Context Protocol |

## Contributing

| Topic | Description |
|-------|-------------|
| [Development Setup](contributing/development.md) | Local dev, test strategy, coding conventions |
| [Adding Connectors](contributing/connectors.md) | How to add a new source or target connector |
| [Adding Tests](contributing/tests.md) | curl-based test patterns |
