# Trust Scores

Trust scores provide a single 0.0-1.0 metric for how much you should trust a pipeline's data. The score is computed from 4 weighted components drawn from existing DAPOS observability signals.

---

## Formula

```
trust_score = sum(component_score[i] * weight[i]) / sum(weight[i])
```

Only components with available data are included in the denominator. If a pipeline has no runs yet, the score is `null` with the recommendation "No data available yet."

---

## Components

Each component scores between 0.0 and 1.0:

### Freshness (default weight: 0.30)

How current is the data relative to the pipeline's SLA?

| Freshness Status | Score |
|------------------|-------|
| FRESH | 1.0 |
| WARNING | 0.5 |
| CRITICAL | 0.1 |

If no freshness data exists (pipeline never checked), this component is excluded from the calculation.

### Quality Gate (default weight: 0.30)

What proportion of quality checks passed on the last run?

```
score = checks_passed / total_checks
```

For example, if 6 out of 7 checks pass, the quality gate score is 0.857. If no quality gate evaluation exists, this component is excluded.

### Error Budget (default weight: 0.25)

How reliable has the pipeline been over the 7-day rolling window?

```
score = min(success_rate / 100.0, 1.0)
```

A pipeline with 95% success rate scores 0.95. A pipeline with 100% success rate scores 1.0. If no error budget exists, this component is excluded.

### Schema Stability (default weight: 0.15)

Has the pipeline's schema been stable, or has it required intervention?

| Condition | Score |
|-----------|-------|
| Has column mappings AND auto-approve additive schema is enabled | 1.0 |
| Has column mappings but auto-approve is disabled | 0.7 |
| No column mappings yet | Excluded |

This component rewards pipelines that have stable, well-defined schemas with auto-approval for safe additive changes.

---

## Recommendations

The trust score maps to a human-readable recommendation:

| Score Range | Level | Recommendation |
|-------------|-------|----------------|
| >= 0.9 | High | "High trust — safe for production decisions" |
| >= 0.7 | Good | "Good trust — reliable for most use cases" |
| >= 0.5 | Medium | "Medium trust — verify before critical decisions" |
| < 0.5 | Low | "Low trust — investigate quality and freshness issues" |
| null | Unknown | "No data available yet — run the pipeline to establish baselines" |

The catalog statistics endpoint (`GET /api/catalog/stats`) uses a slightly different bucketing for the trust distribution overview:

| Score Range | Bucket |
|-------------|--------|
| >= 0.8 | high |
| >= 0.5 | medium |
| < 0.5 | low |
| null | unknown |

---

## Weight Customization

Default weights apply globally to all pipelines:

```json
{
  "freshness": 0.30,
  "quality_gate": 0.30,
  "error_budget": 0.25,
  "schema_stability": 0.15
}
```

You can override weights per-pipeline when one component matters more than others. For example, a real-time dashboard pipeline might weight freshness at 50%:

### Set custom weights

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

**Validation rules:**
- All keys must be from the set: `freshness`, `quality_gate`, `error_budget`, `schema_stability`
- Weights must sum to approximately 1.0 (tolerance of +/- 0.05)
- Returns HTTP 400 if validation fails

The response includes the recomputed trust score using the new weights:

```json
{
  "pipeline_id": "abc-123",
  "weights": {"freshness": 0.50, "quality_gate": 0.25, "error_budget": 0.15, "schema_stability": 0.10},
  "trust_score": 0.82,
  "detail": {
    "freshness": {"score": 1.0, "weight": 0.50},
    "quality_gate": {"score": 0.857, "weight": 0.25},
    "error_budget": {"score": 0.952, "weight": 0.15},
    "schema_stability": {"score": 1.0, "weight": 0.10}
  },
  "recommendation": "Good trust — reliable for most use cases"
}
```

### Reset to defaults

```bash
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/tables/{pipeline_id}/trust-weights"
```

After reset, the pipeline uses the global default weights.

### View current weights

The trust detail endpoint shows the global defaults in its `weights` field. If a pipeline has custom weights, they are reflected in the `detail` component weights:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8100/api/catalog/trust/{pipeline_id}"
```

---

## Examples

### High-trust pipeline

A Tier 1 production pipeline running on schedule with all quality checks passing:

| Component | Score | Weight | Weighted |
|-----------|-------|--------|----------|
| Freshness (FRESH) | 1.000 | 0.30 | 0.300 |
| Quality gate (7/7 pass) | 1.000 | 0.30 | 0.300 |
| Error budget (98%) | 0.980 | 0.25 | 0.245 |
| Schema stability (auto-approve on) | 1.000 | 0.15 | 0.150 |
| **Total** | | | **0.995** |

Recommendation: "High trust — safe for production decisions"

### Medium-trust pipeline

A pipeline with freshness warnings and some quality gate failures:

| Component | Score | Weight | Weighted |
|-----------|-------|--------|----------|
| Freshness (WARNING) | 0.500 | 0.30 | 0.150 |
| Quality gate (5/7 pass) | 0.714 | 0.30 | 0.214 |
| Error budget (85%) | 0.850 | 0.25 | 0.213 |
| Schema stability (auto-approve off) | 0.700 | 0.15 | 0.105 |
| **Total** | | | **0.682** |

Recommendation: "Medium trust — verify before critical decisions"

### Partial data

A new pipeline with only one completed run (no freshness check yet, no error budget):

| Component | Score | Weight | Included? |
|-----------|-------|--------|-----------|
| Freshness | null | 0.30 | No |
| Quality gate (6/7 pass) | 0.857 | 0.30 | Yes |
| Error budget | null | 0.25 | No |
| Schema stability | 1.000 | 0.15 | Yes |

Effective calculation: `(0.857 * 0.30 + 1.000 * 0.15) / (0.30 + 0.15) = 0.907`

The denominator only includes weights for components that have data, so the score is still meaningful even with partial information.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/catalog/trust/{pipeline_id}` | Trust breakdown with weights and recommendation |
| PUT | `/api/catalog/tables/{pipeline_id}/trust-weights` | Set per-pipeline custom weights |
| DELETE | `/api/catalog/tables/{pipeline_id}/trust-weights` | Reset to global defaults |

Trust scores are also included in:
- `GET /api/catalog/search` — as `trust_score` and `trust_detail` per result
- `GET /api/catalog/tables/{pipeline_id}` — as `trust_score` and `trust_detail`
- `GET /api/catalog/stats` — as `trust_distribution` counts
