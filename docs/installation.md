# Installation

Production installation guide for DAPOS.

---

## Requirements

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11+ | Runtime |
| PostgreSQL | 16+ | Main database (state, connectors, runs, lineage) |
| Docker | 20+ | Demo databases and mock APIs (optional in production) |
| Anthropic API key | — | Agent reasoning (optional — rule-based fallbacks work without) |

---

## Python Dependencies

Install from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Key packages:
- `fastapi`, `uvicorn` — API server
- `asyncpg` — async PostgreSQL driver
- `httpx` — HTTP client (Claude API, connectors)
- `croniter` — cron expression evaluation
- `bcrypt` — password hashing
- `pyjwt` — JWT authentication
- `cryptography` — Fernet encryption for credentials
- `slowapi` — rate limiting

---

## PostgreSQL Setup

DAPOS requires a PostgreSQL 16+ database with the `pgvector` extension:

```sql
CREATE DATABASE dapos;
\c dapos
CREATE EXTENSION IF NOT EXISTS vector;
```

Configure via environment variables:

```bash
PG_HOST=localhost
PG_PORT=5432
PG_USER=dapos
PG_PASSWORD=your-password
PG_DATABASE=dapos
```

Tables are auto-created on first startup. Migrations for existing tables are handled by `_ALTER_TABLES_SQL` in `contracts/store.py`.

---

## Configuration

All configuration is via environment variables. See [Configuration Reference](configuration.md) for the full list.

**Required**:
```bash
PG_HOST=your-postgres-host
PG_PASSWORD=your-postgres-password
```

**Recommended for production**:
```bash
JWT_SECRET=your-secret-key          # Strong random key for JWT signing
AUTH_ENABLED=true                    # Enabled by default
ANTHROPIC_API_KEY=sk-...            # For agent features
```

---

## Starting DAPOS

```bash
ANTHROPIC_API_KEY=sk-... python main.py
```

On first startup:
1. Database tables are created
2. 8 seed connectors are installed
3. Default admin user is created (admin/admin)
4. If demo databases are available, 4 demo pipelines are created and triggered

---

## Docker Compose (Development)

```bash
docker compose up -d    # Start PostgreSQL + demo databases + mock APIs
python main.py          # Start DAPOS
```

Services:
- `postgres` — PostgreSQL 16 with pgvector (port 5432)
- `demo-mysql` — MySQL with e-commerce data (port 3307)
- `demo-mongo` — MongoDB with analytics events (port 27018)
- `demo-api` — Mock Stripe/Google Ads/Facebook APIs (port 8200)

---

## Production Deployment

### Single Server

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment
export PG_HOST=your-db-host
export PG_PASSWORD=your-db-password
export JWT_SECRET=$(openssl rand -hex 32)
export ANTHROPIC_API_KEY=sk-...
export AUTH_ENABLED=true

# Start with process manager
gunicorn main:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8100
```

**Important**: DAPOS runs as a single process with 4 async loops. Do not use multiple workers.

### Reverse Proxy

Place behind nginx/traefed for TLS:

```nginx
server {
    listen 443 ssl;
    server_name dapos.example.com;

    location / {
        proxy_pass http://localhost:8100;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## Health Check

```bash
curl http://localhost:8100/health
# {"status":"ok","auth_enabled":true,"pipelines":4,"active":4,"pg_connected":true}
```

---

## Upgrading

1. Stop DAPOS
2. Pull latest code
3. `pip install -r requirements.txt` (for new dependencies)
4. Start DAPOS — database migrations run automatically
5. Seed connectors update if `seeds.py` changed
