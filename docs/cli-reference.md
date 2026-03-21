# CLI Reference

The DAPOS CLI provides command-line access to all platform operations. It communicates with the DAPOS REST API.

## Installation

The CLI is included with DAPOS. No additional installation needed:

```bash
python -m cli --help
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DAPOS_URL` | `http://localhost:8100` | DAPOS API base URL |
| `DAPOS_USER` | `admin` | Login username |
| `DAPOS_PASSWORD` | `admin` | Login password |

Or pass as flags: `--url`, `--user`, `--password`

Authentication tokens are cached in `~/.dapos_token`.

## Commands

### Platform

```bash
# Check platform health
python -m cli health

# Print auth token (for use in curl/scripts)
python -m cli token
```

### Pipelines

```bash
# List all pipelines
python -m cli pipelines list
python -m cli pipelines list --status active
python -m cli pipelines list --json

# Get pipeline details
python -m cli pipelines get demo-stripe-charges
python -m cli pipelines get demo-stripe-charges --json

# Trigger a pipeline run
python -m cli trigger demo-stripe-charges
python -m cli pipelines trigger demo-stripe-charges

# Pause / resume
python -m cli pipelines pause demo-stripe-charges
python -m cli pipelines resume demo-stripe-charges

# List recent runs
python -m cli runs demo-stripe-charges
python -m cli runs demo-stripe-charges --limit 20

# Show step DAG
python -m cli steps demo-stripe-charges

# Export as YAML
python -m cli export --pipeline demo-stripe-charges
python -m cli export  # all pipelines
```

### Connectors

```bash
# List all connectors
python -m cli connectors
python -m cli connectors --type source
python -m cli connectors --type target
```

### Diagnostics (Build 24)

```bash
# Diagnose why a pipeline is failing
python -m cli diagnose demo-ecommerce-orders

# Analyze downstream impact
python -m cli impact demo-ecommerce-orders

# Check for platform-wide anomalies
python -m cli anomalies
```

### Alerts

```bash
# List recent alerts
python -m cli alerts
python -m cli alerts --limit 50
```

### Chat (Natural Language)

```bash
# Send any natural language command
python -m cli chat why is my orders pipeline failing
python -m cli chat list active pipelines
python -m cli chat what breaks if stripe goes down
python -m cli chat are there any anomalies
```

## JSON Output

All commands support `--json` for machine-readable output:

```bash
python -m cli pipelines list --json | jq '.[].pipeline_name'
python -m cli diagnose demo-stripe-charges --json | jq '.root_cause'
```

## Pipeline Name Resolution

Commands that accept a pipeline name support:
- Exact pipeline ID
- Exact pipeline name
- Substring match (e.g., `stripe` matches `demo-stripe-charges`)

## CI/CD Integration

```bash
# Trigger and check result
python -m cli trigger my-pipeline --json | jq '.run_id'

# Check health in monitoring
python -m cli health && echo "OK" || echo "FAIL"

# Export for backup
python -m cli export > pipelines_backup.yaml

# Use token in curl
TOKEN=$(python -m cli token)
curl -H "Authorization: Bearer $TOKEN" http://localhost:8100/api/pipelines
```
