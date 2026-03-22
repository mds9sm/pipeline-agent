# Data Catalog

DAPOS includes a built-in data catalog that surfaces table metadata, column details, trust scores, semantic tags, and business context — all derived from pipeline state already in the system. No external catalog tool required.

---

## Overview

The catalog treats every pipeline's target table as a catalog entry. Each entry includes:

- **Schema and column metadata** from the pipeline's `column_mappings`
- **Freshness, quality, and error budget** from the observability layer
- **Trust score** computed from 4 weighted components
- **Semantic tags** (AI-inferred or user-provided) per column
- **Business context** answers that describe the table's purpose and ownership
- **Column-level lineage** from source to target

All of this is derived automatically from data DAPOS already tracks. The catalog is not a separate data store — it is a read layer over pipeline contracts and observability state.

---

## Search & Browse Endpoints

### Search the catalog

Search across table names, column names, tags, and business context. Supports filtering by source type, pipeline status, and tier.

```bash
# Search for tables related to "orders"
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/search?q=orders"

# Filter by source type and tier
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/search?q=&source_type=mysql&tier=1&limit=20"

# Paginate results
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/search?q=&limit=10&offset=20"
```

**Response shape:**

```json
{
  "items": [
    {
      "pipeline_id": "abc-123",
      "pipeline_name": "demo-ecommerce-orders",
      "target_table": "public.orders",
      "source_table": "ecommerce.orders",
      "status": "active",
      "tier": 1,
      "owner": "data-team",
      "tags": {},
      "refresh_type": "incremental",
      "schedule_cron": "*/30 * * * *",
      "column_count": 8,
      "columns": [
        {"name": "id", "source_name": "id", "type": "integer", "nullable": false, "primary_key": true}
      ],
      "freshness": {"staleness_minutes": 12, "status": "fresh", "sla_met": true},
      "quality": {"decision": "promote", "checks_passed": 7, "total_checks": 7},
      "error_budget": {"success_rate": 95.2, "budget_remaining": 52.0, "escalated": false},
      "trust_score": 0.87,
      "trust_detail": {"freshness": {"score": 1.0, "weight": 0.3}, "...": "..."},
      "semantic_tags": {},
      "business_context": {},
      "created_at": "2026-03-20T10:00:00Z",
      "updated_at": "2026-03-21T08:30:00Z"
    }
  ],
  "total": 42,
  "limit": 50,
  "offset": 0
}
```

The search scans pipeline names, source/target tables, column names, tag keys and values, semantic tag fields (semantic_name, domain, description), and business context text.

### Table detail

Full catalog entry for a single table, including column-level lineage, quality trend, freshness history, schema version count, data contracts, and recent runs.

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}"
```

Returns columns, lineage arrays, freshness (current + 72h history), quality (latest + trend), error budget, trust score with breakdown, schema version count, related data contracts, and the 5 most recent runs.

### Trust score detail

Focused view of the trust score with component breakdown and recommendation.

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/trust/{pipeline_id}"
```

```json
{
  "pipeline_id": "abc-123",
  "pipeline_name": "demo-ecommerce-orders",
  "target_table": "public.orders",
  "trust_score": 0.87,
  "detail": {
    "freshness": {"score": 1.0, "weight": 0.3},
    "quality_gate": {"score": 0.857, "weight": 0.3},
    "error_budget": {"score": 0.952, "weight": 0.25},
    "schema_stability": {"score": 1.0, "weight": 0.15}
  },
  "recommendation": "Good trust — reliable for most use cases",
  "weights": {"freshness": 0.3, "quality_gate": 0.3, "error_budget": 0.25, "schema_stability": 0.15}
}
```

See [Trust Scores](../advanced/trust-scores.md) for a deep dive on the computation.

### Column search

Search columns across all pipelines. Filter by table name.

```bash
# Search for columns named "email" across all tables
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/columns?q=email"

# Filter to a specific table
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/columns?q=&table=public.customers"
```

```json
{
  "items": [
    {
      "column_name": "email",
      "source_column": "customer_email",
      "type": "varchar(255)",
      "nullable": true,
      "primary_key": false,
      "table": "public.customers",
      "pipeline_id": "cust-456",
      "pipeline_name": "demo-ecommerce-customers"
    }
  ],
  "total": 3,
  "limit": 100,
  "offset": 0
}
```

### Catalog statistics

High-level summary: total tables, active tables, column count, source type distribution, and trust score distribution.

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/stats"
```

```json
{
  "total_tables": 12,
  "active_tables": 10,
  "total_columns": 87,
  "source_types": {"mysql": 4, "mongodb": 2, "stripe_api": 3, "postgresql": 3},
  "trust_distribution": {"high": 6, "medium": 3, "low": 0, "unknown": 1}
}
```

---

## Semantic Tags

Semantic tags add meaning to columns beyond their raw names and types. Tags can be AI-inferred or manually set. User-provided tags are never overwritten by AI inference.

Each tag includes:
- `semantic_name` — human-readable name (e.g., "Customer Email Address")
- `domain` — business domain (e.g., "customer", "financial", "marketing")
- `description` — what the column represents
- `pii` — whether it contains personally identifiable information
- `source` — `"ai"` (inferred) or `"user"` (manually set)

### Get tags

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/tags"
```

```json
{
  "pipeline_id": "abc-123",
  "pipeline_name": "demo-ecommerce-orders",
  "target_table": "public.orders",
  "tags": {
    "customer_email": {
      "semantic_name": "Customer Email Address",
      "domain": "customer",
      "description": "Primary email for order notifications",
      "pii": true,
      "source": "ai"
    }
  },
  "column_count": 8,
  "tagged_count": 5,
  "ai_tagged": 4,
  "user_tagged": 1
}
```

### AI-infer tags

Ask Claude to analyze column names, types, and table context to infer semantic tags. User-overridden tags (source=user) are preserved.

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/tags/infer"
```

```json
{
  "pipeline_id": "abc-123",
  "tags": {"...": "..."},
  "inferred_count": 6,
  "user_preserved": 2
}
```

Rate-limited to 10 requests per minute. Requires `operator` or `admin` role.

### Bulk set tags

Set or override tags for multiple columns at once. All provided tags are marked `source=user`.

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_email": {
      "semantic_name": "Customer Email",
      "domain": "customer",
      "description": "Primary contact email",
      "pii": true
    },
    "order_total": {
      "semantic_name": "Order Total Amount",
      "domain": "financial",
      "description": "Total order value in USD",
      "pii": false
    }
  }' \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/tags"
```

### Per-column tag override

Update the tag for a single column without affecting others.

```bash
curl -s -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "compliance", "pii": true}' \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/tags/customer_email"
```

---

## Business Context

Business context captures the "why" behind a pipeline's data — who uses it, what questions it answers, how it fits into business workflows. Context is collected through AI-generated questions tailored to the pipeline's schema.

### Get context questions

Claude generates targeted questions based on the pipeline's source, target, and column structure. The response also includes any existing context already saved.

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/context/questions"
```

```json
{
  "pipeline_id": "abc-123",
  "pipeline_name": "demo-ecommerce-orders",
  "questions": [
    "What business decisions depend on this orders data?",
    "Who are the primary consumers of this table?",
    "Are there regulatory requirements for data retention?",
    "What is the expected volume growth over the next quarter?"
  ],
  "existing_context": {}
}
```

### Save context answers

Save answers as key-value pairs. Merges with existing context. Automatically stamps `_last_updated` and `_updated_by`.

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "primary_consumers": "Finance team, BI dashboards",
    "business_decisions": "Revenue forecasting, inventory planning",
    "retention_policy": "7 years per SOX compliance"
  }' \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/context"
```

Requires `operator` or `admin` role.

---

## Trust Weights

Each pipeline uses global default weights for trust score computation. You can override weights per-pipeline if certain components matter more (e.g., freshness matters most for a real-time dashboard table).

### View current weights

Weights are returned in the trust detail endpoint:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/trust/{pipeline_id}"
```

The `weights` field shows the global defaults. If the pipeline has custom weights, they are used in the `detail` computations.

### Set custom weights

Weights must sum to approximately 1.0 (tolerance of 0.05). Valid keys: `freshness`, `quality_gate`, `error_budget`, `schema_stability`.

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "freshness": 0.50,
    "quality_gate": 0.25,
    "error_budget": 0.15,
    "schema_stability": 0.10
  }' \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/trust-weights"
```

Returns the recomputed trust score using the new weights.

### Reset to defaults

```bash
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/trust-weights"
```

---

## Alert Narratives

When an alert fires, you can generate a human-readable narrative that explains what happened, why it matters, and what to do — using Claude to reason over the alert context, recent runs, downstream dependencies, and freshness state.

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/observability/alerts/{alert_id}/narrative"
```

```json
{
  "alert_id": "alert-789",
  "narrative": "The demo-ecommerce-orders pipeline has been failing for the last 2 runs due to a connection timeout to the source MySQL database. This is a Tier 1 pipeline with 3 downstream consumers. Current staleness is 45 minutes, exceeding the 30-minute SLA. Recommended action: check MySQL connectivity and disk space on demo-mysql.",
  "pipeline_name": "demo-ecommerce-orders",
  "severity": "critical"
}
```

The narrative is saved on the alert object for future reference. Rate-limited to 10 requests per minute.

See [Anomaly Narratives](anomaly-narratives.md) for details on how narratives are generated.

---

## Endpoint Reference

| Method | Endpoint | Description | Role |
|--------|----------|-------------|------|
| GET | `/api/catalog/search` | Search/browse catalog | viewer+ |
| GET | `/api/catalog/tables/{id}` | Table detail | viewer+ |
| GET | `/api/catalog/trust/{id}` | Trust score breakdown | viewer+ |
| GET | `/api/catalog/columns` | Column search | viewer+ |
| GET | `/api/catalog/stats` | Catalog statistics | viewer+ |
| GET | `/api/catalog/tables/{id}/tags` | Get semantic tags | viewer+ |
| POST | `/api/catalog/tables/{id}/tags/infer` | AI-infer tags | operator+ |
| PUT | `/api/catalog/tables/{id}/tags` | Bulk set tags | operator+ |
| PATCH | `/api/catalog/tables/{id}/tags/{col}` | Per-column tag override | operator+ |
| GET | `/api/catalog/tables/{id}/context/questions` | Get context questions | viewer+ |
| PUT | `/api/catalog/tables/{id}/context` | Save context answers | operator+ |
| PUT | `/api/catalog/tables/{id}/trust-weights` | Set custom weights | operator+ |
| DELETE | `/api/catalog/tables/{id}/trust-weights` | Reset to default weights | operator+ |
| POST | `/api/observability/alerts/{id}/narrative` | Generate alert narrative | viewer+ |
