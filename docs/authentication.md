# Authentication & RBAC

DAPOS uses JWT-based authentication with three roles: admin, operator, and viewer.

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_ENABLED` | `true` | Enable/disable auth (disable for development) |
| `JWT_SECRET` | dev fallback | Secret key for JWT signing — **set in production** |
| `JWT_EXPIRY_HOURS` | `24` | Token expiry time |

---

## Default User

On first startup, a default admin user is created:

- **Username**: `admin`
- **Password**: `admin`

Change this immediately in production.

---

## Roles & Permissions

| Action | admin | operator | viewer |
|--------|-------|----------|--------|
| Register new users | yes | - | - |
| Generate connectors (AI) | yes | - | - |
| Deprecate connectors | yes | - | - |
| Test connectors | yes | yes | - |
| Create/update pipelines | yes | yes | - |
| Trigger/pause/resume pipelines | yes | yes | - |
| Approve/reject proposals | yes | yes | - |
| Create data contracts | yes | yes | - |
| Import YAML | yes | yes | - |
| GitOps restore | yes | - | - |
| View all data | yes | yes | yes |
| Chat with agent | yes | yes | yes |
| Export YAML | yes | yes | yes |
| View diagnostics | yes | yes | yes |

---

## Login

```bash
# API
POST /api/auth/login
{
  "username": "admin",
  "password": "admin"
}

# Response
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "role": "admin",
  "username": "admin"
}
```

### Using the Token

Include in all subsequent requests:
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIs...
```

### CLI Token

The CLI caches tokens automatically:
```bash
# Auto-login with env vars
DAPOS_USER=admin DAPOS_PASSWORD=admin python -m cli pipelines list

# Print token for use in scripts
python -m cli token
```

Token cached at `~/.dapos_token`.

---

## Registering Users

Admin only:

```
POST /api/auth/register
{
  "username": "data-analyst",
  "password": "secure-password",
  "role": "viewer"
}
```

---

## Disabling Auth

For development/testing:

```bash
AUTH_ENABLED=false python main.py
```

All requests are treated as admin when auth is disabled.

---

## Session Expiry

Tokens expire after `JWT_EXPIRY_HOURS` (default 24h). The UI auto-redirects to login when a 401 response is received. The CLI re-authenticates automatically when the cached token expires.

---

## Public Endpoints

These endpoints do **not** require authentication:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /metrics` | Prometheus metrics |
| `POST /api/auth/login` | Login |
| `GET /api/docs` | Documentation list |
| `GET /api/docs/{path}` | Documentation content |
