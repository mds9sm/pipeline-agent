"""
DAPOS CLI — Command-line interface for the Data Agent Platform.

Usage:
    python -m cli <command> [options]
    python -m cli --help

Talks to the DAPOS REST API (default: http://localhost:8100).
"""
import argparse
import json
import os
import sys
import textwrap
from datetime import datetime

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = os.getenv("DAPOS_URL", "http://localhost:8100")
DEFAULT_USERNAME = os.getenv("DAPOS_USER", "admin")
DEFAULT_PASSWORD = os.getenv("DAPOS_PASSWORD", "admin")
TOKEN_FILE = os.path.expanduser("~/.dapos_token")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_token(base_url: str, username: str, password: str) -> str:
    """Authenticate and return JWT token. Caches to ~/.dapos_token."""
    # Check cached token
    if os.path.exists(TOKEN_FILE):
        token = open(TOKEN_FILE).read().strip()
        # Validate token
        try:
            resp = httpx.get(
                f"{base_url}/health",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                return token
        except Exception:
            pass

    # Login
    resp = httpx.post(
        f"{base_url}/api/auth/login",
        json={"username": username, "password": password},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"Login failed: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    token = resp.json().get("token", "")
    # Cache
    with open(TOKEN_FILE, "w") as f:
        f.write(token)
    return token


def _api(method: str, path: str, base_url: str, token: str, data=None, timeout=120):
    """Make an API call and return parsed JSON."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{base_url}{path}"
    try:
        if method == "GET":
            resp = httpx.get(url, headers=headers, timeout=timeout)
        elif method == "POST":
            resp = httpx.post(url, headers=headers, json=data or {}, timeout=timeout)
        elif method == "PATCH":
            resp = httpx.patch(url, headers=headers, json=data or {}, timeout=timeout)
        elif method == "DELETE":
            resp = httpx.delete(url, headers=headers, timeout=timeout)
        else:
            raise ValueError(f"Unsupported method: {method}")
    except httpx.ConnectError:
        print(f"Error: Cannot connect to DAPOS at {base_url}", file=sys.stderr)
        print("Is the server running? Start with: python main.py", file=sys.stderr)
        sys.exit(1)

    if resp.status_code >= 400:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)

    try:
        return resp.json()
    except Exception:
        return resp.text


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _table(rows: list[dict], columns: list[str], headers: list[str] = None):
    """Print a formatted table."""
    if not rows:
        print("  (no results)")
        return

    headers = headers or columns
    widths = [len(h) for h in headers]
    str_rows = []
    for row in rows:
        str_row = [str(row.get(c, ""))[:60] for c in columns]
        for i, v in enumerate(str_row):
            widths[i] = max(widths[i], len(v))
        str_rows.append(str_row)

    # Header
    header_line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(header_line)
    print("  ".join("-" * w for w in widths))

    # Rows
    for str_row in str_rows:
        print("  ".join(v.ljust(w) for v, w in zip(str_row, widths)))


def _json_out(data):
    """Pretty-print JSON."""
    print(json.dumps(data, indent=2, default=str))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_health(args, base_url, token):
    """Check platform health."""
    health = _api("GET", "/health", base_url, token, timeout=5)
    print(f"Status:     {health.get('status', 'unknown')}")
    print(f"Pipelines:  {health.get('pipelines', '?')}")
    print(f"Connectors: {health.get('connectors', '?')}")
    print(f"Uptime:     {health.get('uptime', '?')}")


def cmd_pipelines_list(args, base_url, token):
    """List all pipelines."""
    params = ""
    if args.status:
        params = f"?status={args.status}"
    pipelines = _api("GET", f"/api/pipelines{params}", base_url, token)
    if args.json:
        _json_out(pipelines)
        return

    _table(
        pipelines,
        ["pipeline_name", "status", "source", "target", "schedule_cron", "tier", "step_count"],
        ["NAME", "STATUS", "SOURCE", "TARGET", "SCHEDULE", "TIER", "STEPS"],
    )
    print(f"\n  {len(pipelines)} pipeline(s)")


def cmd_pipelines_get(args, base_url, token):
    """Get pipeline details."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    detail = _api("GET", f"/api/pipelines/{pid}", base_url, token)
    if args.json:
        _json_out(detail)
        return

    print(f"Pipeline:    {detail.get('pipeline_name', '?')}")
    print(f"ID:          {detail.get('pipeline_id', '?')}")
    print(f"Status:      {detail.get('status', '?')}")
    print(f"Version:     {detail.get('version', '?')}")
    print(f"Source:      {detail.get('source', '?')}")
    print(f"Target:      {detail.get('target', '?')}")
    print(f"Refresh:     {detail.get('refresh_type', '?')}")
    print(f"Load:        {detail.get('load_type', '?')}")
    print(f"Schedule:    {detail.get('schedule_cron', '?')}")
    print(f"Tier:        {detail.get('tier', '?')}")
    print(f"Owner:       {detail.get('owner', '?')}")
    print(f"Steps:       {len(detail.get('steps', []))}")
    print(f"Environment: {detail.get('environment', '?')}")

    budget = detail.get("error_budget")
    if budget:
        print(f"\nError Budget:")
        print(f"  Success rate:  {budget.get('success_rate', 0):.1%}")
        print(f"  Remaining:     {budget.get('budget_remaining', 0):.3f}")
        print(f"  Escalated:     {budget.get('escalated', False)}")


def cmd_pipelines_trigger(args, base_url, token):
    """Trigger a pipeline run."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    result = _api("POST", f"/api/pipelines/{pid}/trigger", base_url, token)
    print(f"Run triggered: {result.get('run_id', '?')}")
    print(f"Status:        {result.get('status', '?')}")


def cmd_pipelines_pause(args, base_url, token):
    """Pause a pipeline."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    result = _api("POST", f"/api/pipelines/{pid}/pause", base_url, token)
    print(f"Pipeline paused: {result.get('pipeline_name', '?')}")


def cmd_pipelines_resume(args, base_url, token):
    """Resume a paused pipeline."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    result = _api("POST", f"/api/pipelines/{pid}/resume", base_url, token)
    print(f"Pipeline resumed: {result.get('pipeline_name', '?')}")


def cmd_runs(args, base_url, token):
    """List recent runs for a pipeline."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    runs = _api("GET", f"/api/pipelines/{pid}/runs?limit={args.limit}", base_url, token)
    if args.json:
        _json_out(runs)
        return

    _table(
        runs,
        ["run_id", "status", "rows_extracted", "started_at", "gate_decision"],
        ["RUN_ID", "STATUS", "ROWS", "STARTED", "GATE"],
    )


def cmd_connectors(args, base_url, token):
    """List connectors."""
    connectors = _api("GET", "/api/connectors", base_url, token)
    if args.type:
        connectors = [c for c in connectors if c.get("source_target_type") == args.type]
    if args.json:
        _json_out(connectors)
        return

    _table(
        connectors,
        ["connector_name", "connector_type", "source_target_type", "status", "version"],
        ["NAME", "TYPE", "SRC/TGT", "STATUS", "VERSION"],
    )


def cmd_diagnose(args, base_url, token):
    """Diagnose a pipeline."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    print("Diagnosing... (this may take a few seconds)")
    result = _api("POST", f"/api/pipelines/{pid}/diagnose", base_url, token)
    if args.json:
        _json_out(result)
        return

    print(f"\nRoot Cause:  {result.get('root_cause', 'Unknown')}")
    print(f"Category:    {result.get('category', 'unknown')}")
    print(f"Confidence:  {result.get('confidence', 0):.0%}")
    print(f"Upstream:    {result.get('upstream_health', 'unknown')}")

    if result.get("evidence"):
        print("\nEvidence:")
        for e in result["evidence"]:
            print(f"  - {e}")

    if result.get("recommended_actions"):
        print("\nRecommended Actions:")
        for a in result["recommended_actions"]:
            print(f"  [{a.get('priority', '?')}] {a.get('action', '')}")

    if result.get("pattern_detected"):
        print(f"\nPattern: {result['pattern_detected']}")

    if result.get("summary"):
        print(f"\n{result['summary']}")


def cmd_impact(args, base_url, token):
    """Analyze downstream impact."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    print("Analyzing impact... (this may take a few seconds)")
    result = _api("POST", f"/api/pipelines/{pid}/impact", base_url, token)
    if args.json:
        _json_out(result)
        return

    print(f"\nSeverity:     {result.get('impact_severity', 'unknown')}")
    br = result.get("blast_radius", {})
    print(f"Blast Radius: {br.get('pipelines', 0)} pipelines, {br.get('contracts', 0)} contracts")

    affected = result.get("affected_pipelines", [])
    if affected:
        print(f"\nAffected Pipelines ({len(affected)}):")
        for a in affected:
            sla = " [SLA AT RISK]" if a.get("sla_at_risk") else ""
            print(f"  [{a.get('depth', '?')}] {a.get('pipeline_name', '?')} ({a.get('impact_type', '?')}){sla}")

    if result.get("mitigation_options"):
        print("\nMitigation:")
        for m in result["mitigation_options"]:
            print(f"  - {m.get('option', '')} (effort: {m.get('effort', '?')})")

    if result.get("summary"):
        print(f"\n{result['summary']}")


def cmd_anomalies(args, base_url, token):
    """Check for platform-wide anomalies."""
    print("Checking for anomalies... (this may take a few seconds)")
    result = _api("GET", "/api/observability/anomalies", base_url, token)
    if args.json:
        _json_out(result)
        return

    print(f"\nPlatform Health: {result.get('platform_health', 'unknown')}")

    anomalies = result.get("anomalies", [])
    if anomalies:
        print(f"\nAnomalies ({len(anomalies)}):")
        for a in anomalies:
            expected = " (expected)" if a.get("is_expected") else ""
            print(f"  [{a.get('severity', '?')}] {a.get('pipeline_name', '?')}: {a.get('observation', '')}{expected}")
            if a.get("reasoning"):
                print(f"         {a['reasoning']}")
    else:
        print("\n  No anomalies detected.")

    patterns = result.get("cross_pipeline_patterns", [])
    if patterns:
        print("\nCross-Pipeline Patterns:")
        for p in patterns:
            print(f"  - {p}")

    if result.get("summary"):
        print(f"\n{result['summary']}")


def cmd_alerts(args, base_url, token):
    """List recent alerts."""
    alerts = _api("GET", f"/api/alerts?limit={args.limit}", base_url, token)
    if args.json:
        _json_out(alerts)
        return

    _table(
        alerts,
        ["severity", "pipeline_name", "summary", "created_at"],
        ["SEVERITY", "PIPELINE", "SUMMARY", "CREATED"],
    )


def cmd_chat(args, base_url, token):
    """Send a natural language command."""
    text = " ".join(args.text)
    result = _api("POST", "/api/command", base_url, token, data={"text": text})
    response = result.get("response", result.get("fallback_text", ""))
    if args.json:
        _json_out(result)
        return
    if response:
        print(response)
    else:
        _json_out(result)


def cmd_token(args, base_url, token):
    """Print the current auth token (for use in scripts)."""
    print(token)


def cmd_export(args, base_url, token):
    """Export pipeline(s) as YAML."""
    if args.pipeline:
        pid = _resolve_pipeline_id(args.pipeline, base_url, token)
        resp = _api("GET", f"/api/contracts/yaml?pipeline_id={pid}&include_state=true", base_url, token)
    else:
        resp = _api("GET", f"/api/contracts/yaml?include_state=true", base_url, token)
    # resp is the YAML string
    if isinstance(resp, str):
        print(resp)
    else:
        print(resp.get("yaml", json.dumps(resp, indent=2)))


def cmd_steps(args, base_url, token):
    """Show step DAG for a pipeline."""
    pid = _resolve_pipeline_id(args.pipeline, base_url, token)
    result = _api("GET", f"/api/pipelines/{pid}/steps", base_url, token)
    steps = result.get("steps", [])
    if not steps:
        print(f"Pipeline {result.get('pipeline_name', '?')} uses legacy execution (no step DAG defined).")
        return

    if args.json:
        _json_out(result)
        return

    print(f"Steps for {result.get('pipeline_name', '?')} ({len(steps)} steps):\n")
    for i, s in enumerate(steps, 1):
        deps = ", ".join(s.get("depends_on", [])) or "(none)"
        enabled = "" if s.get("enabled", True) else " [DISABLED]"
        print(f"  {i}. {s.get('step_name', '?')} ({s.get('step_type', '?')}){enabled}")
        print(f"     depends_on: {deps}")
        if s.get("retry_max", 0) > 0:
            print(f"     retry: {s['retry_max']}x")


# ---------------------------------------------------------------------------
# Pipeline name resolution
# ---------------------------------------------------------------------------

def _resolve_pipeline_id(name_or_id: str, base_url: str, token: str) -> str:
    """Resolve a pipeline name or partial name to its ID."""
    # Try as direct ID first
    try:
        result = _api("GET", f"/api/pipelines/{name_or_id}", base_url, token)
        return result.get("pipeline_id", name_or_id)
    except SystemExit:
        pass

    # Search by name
    pipelines = _api("GET", "/api/pipelines", base_url, token)
    name_lower = name_or_id.lower()

    # Exact name match
    for p in pipelines:
        if p.get("pipeline_name", "").lower() == name_lower:
            return p["pipeline_id"]

    # Substring match
    for p in pipelines:
        if name_lower in p.get("pipeline_name", "").lower():
            return p["pipeline_id"]

    print(f"Error: Pipeline '{name_or_id}' not found.", file=sys.stderr)
    print(f"Available pipelines:", file=sys.stderr)
    for p in pipelines[:10]:
        print(f"  - {p.get('pipeline_name', '?')}", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="dapos",
        description="DAPOS CLI — Data Agent Platform Operating System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Environment variables:
              DAPOS_URL       API base URL (default: http://localhost:8100)
              DAPOS_USER      Login username (default: admin)
              DAPOS_PASSWORD  Login password (default: admin)

            Examples:
              python -m cli health
              python -m cli pipelines list
              python -m cli pipelines list --status active
              python -m cli diagnose demo-stripe-charges
              python -m cli impact demo-ecommerce-orders
              python -m cli anomalies
              python -m cli trigger demo-stripe-charges
              python -m cli chat why is my orders pipeline failing
              python -m cli runs demo-stripe-charges
              python -m cli steps demo-stripe-charges
              python -m cli export --pipeline demo-stripe-charges
              python -m cli token
        """),
    )
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="DAPOS API URL")
    parser.add_argument("--user", default=DEFAULT_USERNAME, help="Username")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Password")

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # health
    sub.add_parser("health", help="Check platform health")

    # pipelines
    p_pipelines = sub.add_parser("pipelines", help="Pipeline operations")
    p_sub = p_pipelines.add_subparsers(dest="subcommand")

    p_list = p_sub.add_parser("list", help="List all pipelines")
    p_list.add_argument("--status", choices=["active", "paused", "archived"], help="Filter by status")
    p_list.add_argument("--json", action="store_true", help="JSON output")

    p_get = p_sub.add_parser("get", help="Get pipeline details")
    p_get.add_argument("pipeline", help="Pipeline name or ID")
    p_get.add_argument("--json", action="store_true", help="JSON output")

    p_trigger = p_sub.add_parser("trigger", help="Trigger a pipeline run")
    p_trigger.add_argument("pipeline", help="Pipeline name or ID")

    p_pause = p_sub.add_parser("pause", help="Pause a pipeline")
    p_pause.add_argument("pipeline", help="Pipeline name or ID")

    p_resume = p_sub.add_parser("resume", help="Resume a paused pipeline")
    p_resume.add_argument("pipeline", help="Pipeline name or ID")

    # trigger (shortcut)
    p_trig = sub.add_parser("trigger", help="Trigger a pipeline run (shortcut)")
    p_trig.add_argument("pipeline", help="Pipeline name or ID")

    # runs
    p_runs = sub.add_parser("runs", help="List recent runs for a pipeline")
    p_runs.add_argument("pipeline", help="Pipeline name or ID")
    p_runs.add_argument("--limit", type=int, default=10, help="Number of runs")
    p_runs.add_argument("--json", action="store_true", help="JSON output")

    # steps
    p_steps = sub.add_parser("steps", help="Show step DAG for a pipeline")
    p_steps.add_argument("pipeline", help="Pipeline name or ID")
    p_steps.add_argument("--json", action="store_true", help="JSON output")

    # connectors
    p_conn = sub.add_parser("connectors", help="List connectors")
    p_conn.add_argument("--type", choices=["source", "target"], help="Filter by type")
    p_conn.add_argument("--json", action="store_true", help="JSON output")

    # diagnose
    p_diag = sub.add_parser("diagnose", help="Diagnose a pipeline (root cause analysis)")
    p_diag.add_argument("pipeline", help="Pipeline name or ID")
    p_diag.add_argument("--json", action="store_true", help="JSON output")

    # impact
    p_impact = sub.add_parser("impact", help="Analyze downstream impact")
    p_impact.add_argument("pipeline", help="Pipeline name or ID")
    p_impact.add_argument("--json", action="store_true", help="JSON output")

    # anomalies
    p_anom = sub.add_parser("anomalies", help="Check for platform-wide anomalies")
    p_anom.add_argument("--json", action="store_true", help="JSON output")

    # alerts
    p_alerts = sub.add_parser("alerts", help="List recent alerts")
    p_alerts.add_argument("--limit", type=int, default=20, help="Number of alerts")
    p_alerts.add_argument("--json", action="store_true", help="JSON output")

    # chat
    p_chat = sub.add_parser("chat", help="Send a natural language command")
    p_chat.add_argument("text", nargs="+", help="Command text")
    p_chat.add_argument("--json", action="store_true", help="JSON output")

    # export
    p_export = sub.add_parser("export", help="Export pipeline(s) as YAML")
    p_export.add_argument("--pipeline", help="Pipeline name or ID (omit for all)")

    # token
    sub.add_parser("token", help="Print auth token for scripting")

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    base_url = args.url
    token = _get_token(base_url, args.user, args.password)

    dispatch = {
        "health": cmd_health,
        "connectors": cmd_connectors,
        "diagnose": cmd_diagnose,
        "impact": cmd_impact,
        "anomalies": cmd_anomalies,
        "alerts": cmd_alerts,
        "chat": cmd_chat,
        "token": cmd_token,
        "export": cmd_export,
        "runs": cmd_runs,
        "steps": cmd_steps,
        "trigger": cmd_pipelines_trigger,
    }

    if args.command in dispatch:
        dispatch[args.command](args, base_url, token)
    elif args.command == "pipelines":
        if not args.subcommand:
            cmd_pipelines_list(args, base_url, token)
        elif args.subcommand == "list":
            cmd_pipelines_list(args, base_url, token)
        elif args.subcommand == "get":
            cmd_pipelines_get(args, base_url, token)
        elif args.subcommand == "trigger":
            cmd_pipelines_trigger(args, base_url, token)
        elif args.subcommand == "pause":
            cmd_pipelines_pause(args, base_url, token)
        elif args.subcommand == "resume":
            cmd_pipelines_resume(args, base_url, token)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
