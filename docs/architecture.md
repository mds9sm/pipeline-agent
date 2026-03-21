# Architecture

DAPOS runs as a single Python async process with four concurrent event loops, all backed by PostgreSQL.

## System Overview

```
                    ┌─────────────────────────────────────────┐
                    │              DAPOS Process               │
                    │                                         │
  HTTP/REST ───────►│  API Server (FastAPI, port 8100)        │
  Browser UI ──────►│    ├── 40+ REST endpoints               │
  CLI ─────────────►│    ├── React SPA (CDN, no build)        │
                    │    └── JWT auth + RBAC                  │
                    │                                         │
                    │  Scheduler (60s tick)                    │
                    │    ├── Cron evaluation                   │
                    │    ├── Dependency graph (topo sort)      │
                    │    ├── Event-driven triggers             │
                    │    └── Backfill + retry                  │
                    │                                         │
                    │  Monitor (5m tick)                       │
                    │    ├── Schema drift detection            │
                    │    ├── Freshness checks                  │
                    │    ├── Lineage impact analysis           │
                    │    └── Alert dispatch                    │
                    │                                         │
                    │  Observability (30s tick)                │
                    │    ├── Quality trend summaries           │
                    │    ├── Anomaly reasoning (15m)           │
                    │    ├── Daily digest (9 AM UTC)           │
                    │    └── GitOps reconciliation (5m)        │
                    └──────────────┬──────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │  PostgreSQL 16 + pgvector                │
                    │    All state: pipelines, runs, gates,    │
                    │    connectors, alerts, lineage, costs,   │
                    │    preferences, data contracts, steps    │
                    └─────────────────────────────────────────┘
```

## Key Principles

1. **Single process, no external dependencies** — No Redis, no Celery, no Kafka. PostgreSQL is the only infrastructure.
2. **Agent IS the product** — Claude reasons about quality, designs topologies, generates connectors, diagnoses failures. Not a bolt-on.
3. **Two-tier autonomy** — Runtime decisions (extract/load/promote) are autonomous. Structural changes (connectors, schema, topology) require human approval.
4. **Database as source of truth** — All state lives in PostgreSQL. Git repo is a derived artifact. UI is a read layer.
5. **Connector-agnostic quality** — Quality gate types against `TargetEngine` interface, not specific databases.

## Pipeline Execution Flow

### Legacy Path (no steps defined)
```
Extract → Stage → Quality Gate → Promote → Cleanup → Hooks → Metadata
```

### Step DAG Path (steps defined)
```
Steps execute in topological order with per-step retry:

  extract ──► transform ──► quality_gate ──► promote ──► cleanup
                  │                              │
                  └──► transform_2 ──────────────┘
```

## Component Map

| Component | File | Purpose |
|-----------|------|---------|
| Entry point | `main.py` | Wires 4 async loops + dependency injection |
| Config | `config.py` | Environment variable loading |
| API | `api/server.py` | FastAPI, 40+ endpoints, JWT, rate limiting |
| Agent core | `agent/core.py` | Claude API: routing, strategy, diagnosis, topology |
| Conversation | `agent/conversation.py` | Multi-turn onboarding/discovery |
| Runner | `agent/autonomous.py` | Pipeline execution state machine + step DAG executor |
| Models | `contracts/models.py` | All dataclasses + enums |
| Store | `contracts/store.py` | PostgreSQL CRUD via asyncpg |
| YAML codec | `contracts/yaml_codec.py` | Pipeline ↔ YAML serialization |
| Connectors | `connectors/registry.py` | exec()-based connector loader |
| Seeds | `connectors/seeds.py` | 8 built-in connectors |
| Quality | `quality/gate.py` | 7-check quality gate |
| Monitor | `monitor/engine.py` | Drift, freshness, alerts |
| Scheduler | `scheduler/manager.py` | Cron + topo sort + backfill |
| Sandbox | `sandbox.py` | AST validation for generated code |
| Auth | `auth.py` | JWT + 3 roles |
| Crypto | `crypto.py` | Fernet encryption for credentials |
| GitOps | `gitops/repo.py` | Pipeline YAML + connector versioning |
| UI | `ui/App.jsx` | React 18 SPA (10 views) |
| CLI | `cli/` | Command-line interface |
