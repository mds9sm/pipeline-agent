# Development Setup

Guide for developing and testing DAPOS locally.

---

## Prerequisites

- Python 3.11+
- Docker & Docker Compose (for PostgreSQL, demo databases, mock APIs)
- An Anthropic API key (for agent features; rule-based fallbacks work without)

---

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Start DAPOS
ANTHROPIC_API_KEY=sk-... python main.py
```

This starts:
- **PostgreSQL 16** — main database (port 5432)
- **Demo MySQL** — e-commerce data (port 3307)
- **Demo MongoDB** — analytics events (port 27018)
- **Mock API** — Stripe, Google Ads, Facebook (port 8200)
- **DAPOS** — API + UI (port 8100)

On first startup, 8 seed connectors and 4 demo pipelines are auto-created.

---

## Architecture

Single Python async process with 4 event loops:

| Loop | Tick | Purpose |
|------|------|---------|
| API Server | continuous | FastAPI on port 8100, serves REST + React SPA |
| Scheduler | 60s | Cron evaluation, dependency graph, backfill/retry |
| Monitor | 300s (5m) | Schema drift, freshness, alert dispatch |
| Observability | 30s / 900s | Quality trends (30s), anomaly scan (15m), daily digest (9AM UTC) |

---

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point, wires all 4 loops |
| `config.py` | Environment variable loading |
| `api/server.py` | FastAPI with 100+ endpoints |
| `agent/core.py` | Claude API integration |
| `agent/autonomous.py` | Pipeline execution engine |
| `contracts/models.py` | All dataclasses and enums |
| `contracts/store.py` | PostgreSQL CRUD layer |
| `quality/gate.py` | 7-check quality gate |
| `monitor/engine.py` | Drift, freshness, alerts |
| `scheduler/manager.py` | Cron + dependency scheduler |
| `ui/App.jsx` | React 18 SPA (CDN, no build step) |

---

## Coding Conventions

- **Async everywhere** — all I/O operations use `async`/`await`
- **No `from __future__ import annotations`** in files with Pydantic models (breaks FastAPI)
- **Enums**: `(str, Enum)` with lowercase values
- **Domain models**: dataclasses in `contracts/models.py`
- **API models**: Pydantic only in `api/server.py`
- **Credentials**: always encrypted via `crypto.encrypt_dict()` before storage
- **Enum normalization**: `if isinstance(x, str): x = MyEnum(x.lower())`

---

## Common Pitfalls

1. **Pydantic + `__future__` annotations** breaks FastAPI model validation
2. **`ConnectorType` as string** from API layer — normalize early
3. **`.value` on enums** — check type first when uncertain
4. **`float("inf")` in JSON** — PostgreSQL rejects Infinity; cap with `min(value, 99999)`
5. **Store methods require `pipeline_id`** — never call without it
6. **React hooks in `.map()`** — extract to a proper component
7. **Browser caching** — increment `?v=N` in `index.html` after UI changes
8. **`schedule_cron` not `schedule`** — PipelineContract field name is `schedule_cron`

---

## Testing

DAPOS uses **curl-based integration tests** against the real running app. No pytest, no mocks.

```bash
./test-pipeline-agent.sh              # Full suite (~165 tests)
./test-pipeline-agent.sh --api        # API endpoints only (fast)
./test-pipeline-agent.sh --sources    # Source connector tests
./test-pipeline-agent.sh --targets    # Target connector tests
./test-pipeline-agent.sh --chat       # Agent conversation tests
```

See [Adding Tests](tests.md) for how to add new tests.
