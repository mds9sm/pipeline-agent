"""
DAPOS MCP Server — Expose the Data Agent Platform to AI agents via Model Context Protocol.

Usage:
    python mcp_server.py                        # stdio transport (Claude Desktop)
    python mcp_server.py --transport sse         # SSE transport (web clients)
    python mcp_server.py --transport streamable-http  # Streamable HTTP

Environment variables:
    DAPOS_URL       — DAPOS API base URL (default: http://localhost:8100)
    DAPOS_USER      — DAPOS username (default: admin)
    DAPOS_PASSWORD   — DAPOS password (default: admin)
"""
import json
import os
import sys
from typing import Optional

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DAPOS_URL = os.getenv("DAPOS_URL", "http://localhost:8100")
DAPOS_USER = os.getenv("DAPOS_USER", "admin")
DAPOS_PASSWORD = os.getenv("DAPOS_PASSWORD", "admin")

mcp = FastMCP(
    "DAPOS",
    description="Data Agent Platform — search data catalog, check trust scores, diagnose pipelines, analyze impact, and more.",
)

# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

_token: Optional[str] = None


def _get_token() -> str:
    """Authenticate with DAPOS and return a JWT token."""
    global _token
    if _token:
        # Validate
        try:
            resp = httpx.get(
                f"{DAPOS_URL}/health",
                headers={"Authorization": f"Bearer {_token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                return _token
        except Exception:
            pass

    resp = httpx.post(
        f"{DAPOS_URL}/api/auth/login",
        json={"username": DAPOS_USER, "password": DAPOS_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    _token = resp.json().get("token", "")
    return _token


def _api(method: str, path: str, data: Optional[dict] = None, params: Optional[dict] = None) -> dict:
    """Make an authenticated API call to DAPOS."""
    token = _get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{DAPOS_URL}{path}"

    if method == "GET":
        resp = httpx.get(url, headers=headers, params=params, timeout=30)
    elif method == "POST":
        resp = httpx.post(url, headers=headers, json=data or {}, timeout=60)
    elif method == "PUT":
        resp = httpx.put(url, headers=headers, json=data or {}, timeout=30)
    elif method == "PATCH":
        resp = httpx.patch(url, headers=headers, json=data or {}, timeout=30)
    elif method == "DELETE":
        resp = httpx.delete(url, headers=headers, timeout=30)
    else:
        raise ValueError(f"Unknown method: {method}")

    resp.raise_for_status()
    return resp.json()


def _fmt(data: dict, indent: int = 2) -> str:
    """Format dict as readable JSON string."""
    return json.dumps(data, indent=indent, default=str)


# ---------------------------------------------------------------------------
# MCP Resources — browsable data
# ---------------------------------------------------------------------------

@mcp.resource("dapos://catalog")
def catalog_overview() -> str:
    """Overview of all data tables with trust scores, freshness, and quality."""
    result = _api("GET", "/api/catalog/search")
    items = result.get("items", [])
    lines = [f"# DAPOS Data Catalog — {result.get('total', 0)} tables\n"]
    for t in items:
        trust = t.get("trust_score")
        trust_str = f"{trust:.0%}" if trust is not None else "N/A"
        fresh = t.get("freshness") or {}
        fresh_str = f"{fresh.get('staleness_minutes', '?')}m" if fresh else "unknown"
        lines.append(
            f"- **{t['target_table']}** (pipeline: {t['pipeline_name']})\n"
            f"  Trust: {trust_str} | Freshness: {fresh_str} | "
            f"Columns: {t.get('column_count', 0)} | Status: {t.get('status', '?')}"
        )
    return "\n".join(lines)


@mcp.resource("dapos://catalog/stats")
def catalog_stats() -> str:
    """High-level catalog statistics — table count, source types, trust distribution."""
    result = _api("GET", "/api/catalog/stats")
    return _fmt(result)


@mcp.resource("dapos://pipelines")
def pipelines_list() -> str:
    """All pipelines with status, schedule, and source/target info."""
    items = _api("GET", "/api/pipelines")
    lines = [f"# DAPOS Pipelines — {len(items)} total\n"]
    for p in items:
        lines.append(
            f"- **{p['pipeline_name']}** ({p.get('status', '?')})\n"
            f"  {p.get('source', '?')} → {p.get('target', '?')} | "
            f"Schedule: {p.get('schedule_cron', 'none')} | Tier: {p.get('tier', '?')}"
        )
    return "\n".join(lines)


@mcp.resource("dapos://alerts")
def recent_alerts() -> str:
    """Recent alerts with narratives (last 24 hours)."""
    items = _api("GET", "/api/observability/alerts", params={"hours": 24})
    if not items:
        return "No alerts in the last 24 hours. Platform is healthy."
    lines = ["# Recent Alerts\n"]
    for a in items:
        narrative = a.get("narrative", "")
        lines.append(
            f"- [{a.get('severity', '?').upper()}] **{a.get('pipeline_name', '?')}**: {a.get('summary', '')}\n"
            f"  {narrative}" if narrative else
            f"- [{a.get('severity', '?').upper()}] **{a.get('pipeline_name', '?')}**: {a.get('summary', '')}"
        )
    return "\n".join(lines)


@mcp.resource("dapos://dag")
def dependency_graph() -> str:
    """Pipeline dependency graph — nodes and edges."""
    result = _api("GET", "/api/dag")
    return _fmt(result)


@mcp.resource("dapos://anomalies")
def current_anomalies() -> str:
    """Current platform anomaly scan with AI reasoning."""
    result = _api("GET", "/api/observability/anomalies")
    health = result.get("platform_health", "unknown")
    summary = result.get("summary", "")
    anomalies = result.get("anomalies", [])

    lines = [f"# Platform Health: {health.upper()}\n", summary, ""]
    if anomalies:
        lines.append("## Anomalies\n")
        for a in anomalies:
            lines.append(
                f"- **{a.get('pipeline_name', '?')}** ({a.get('anomaly_type', '?')}, {a.get('severity', '?')})\n"
                f"  {a.get('reasoning', a.get('observation', ''))}\n"
                f"  Action: {a.get('recommended_action', 'N/A')}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP Resources — per-table detail (dynamic)
# ---------------------------------------------------------------------------

@mcp.resource("dapos://catalog/tables/{pipeline_id}")
def table_detail(pipeline_id: str) -> str:
    """Detailed catalog entry for a table — columns, lineage, trust, quality, freshness."""
    result = _api("GET", f"/api/catalog/tables/{pipeline_id}")
    return _fmt(result)


@mcp.resource("dapos://pipelines/{pipeline_id}")
def pipeline_detail(pipeline_id: str) -> str:
    """Full pipeline configuration and status."""
    result = _api("GET", f"/api/pipelines/{pipeline_id}")
    return _fmt(result)


# ---------------------------------------------------------------------------
# MCP Tools — actions the LLM can invoke
# ---------------------------------------------------------------------------

@mcp.tool()
def search_catalog(query: str = "", source_type: str = "", tier: int = 0) -> str:
    """Search the data catalog for tables by name, column, tag, or business context.

    Args:
        query: Free-text search (matches table names, column names, semantic tags, business context)
        source_type: Filter by source type (mysql, mongodb, stripe, etc.)
        tier: Filter by observability tier (1=critical, 2=standard, 3=low). 0 means no filter.
    """
    params = {}
    if query:
        params["q"] = query
    if source_type:
        params["source_type"] = source_type
    if tier:
        params["tier"] = tier
    result = _api("GET", "/api/catalog/search", params=params)
    items = result.get("items", [])
    if not items:
        return f"No tables found matching '{query}'."
    lines = [f"Found {result.get('total', 0)} tables:\n"]
    for t in items:
        trust = t.get("trust_score")
        trust_str = f"{trust:.0%}" if trust is not None else "N/A"
        lines.append(
            f"- {t['target_table']} (trust: {trust_str}, {t.get('column_count', 0)} columns, "
            f"pipeline: {t['pipeline_name']}, status: {t.get('status', '?')})"
        )
    return "\n".join(lines)


@mcp.tool()
def search_columns(query: str, table: str = "") -> str:
    """Search for columns across all tables in the catalog.

    Args:
        query: Column name to search for
        table: Optional table name filter
    """
    params = {"q": query}
    if table:
        params["table"] = table
    result = _api("GET", "/api/catalog/columns", params=params)
    items = result.get("items", [])
    if not items:
        return f"No columns found matching '{query}'."
    lines = [f"Found {result.get('total', 0)} columns:\n"]
    for c in items[:20]:
        lines.append(
            f"- {c['column_name']} ({c.get('type', '?')}) in {c.get('table', '?')} "
            f"(pipeline: {c.get('pipeline_name', '?')})"
        )
    return "\n".join(lines)


@mcp.tool()
def get_trust_score(pipeline_id: str) -> str:
    """Get the trust score breakdown for a table/pipeline.

    Args:
        pipeline_id: Pipeline ID to check trust for
    """
    result = _api("GET", f"/api/catalog/trust/{pipeline_id}")
    score = result.get("trust_score")
    score_str = f"{score:.0%}" if score is not None else "N/A"
    rec = result.get("recommendation", "")
    detail = result.get("detail", {})

    lines = [
        f"Trust Score: {score_str}",
        f"Recommendation: {rec}",
        "",
        "Component Breakdown:",
    ]
    for comp, info in detail.items():
        s = info.get("score")
        w = info.get("weight", 0)
        s_str = f"{s:.0%}" if s is not None else "N/A"
        lines.append(f"  - {comp}: {s_str} (weight: {w:.0%})")
    return "\n".join(lines)


@mcp.tool()
def get_semantic_tags(pipeline_id: str) -> str:
    """Get semantic tags (business meaning) for all columns in a table.

    Args:
        pipeline_id: Pipeline ID to get tags for
    """
    result = _api("GET", f"/api/catalog/tables/{pipeline_id}/tags")
    tags = result.get("tags", {})
    if not tags:
        return f"No semantic tags set for pipeline {result.get('pipeline_name', pipeline_id)}. Use infer_tags to generate them."

    lines = [
        f"Semantic Tags for {result.get('pipeline_name', '?')} "
        f"({result.get('tagged_count', 0)} tagged, "
        f"{result.get('ai_tagged', 0)} AI-inferred, {result.get('user_tagged', 0)} user-set)\n"
    ]
    for col, tag in tags.items():
        pii_flag = " [PII]" if tag.get("pii") else ""
        unit = f" ({tag.get('unit')})" if tag.get("unit") else ""
        lines.append(
            f"- **{col}**: {tag.get('semantic_name', '?')}{unit}{pii_flag}\n"
            f"  Domain: {tag.get('domain', '?')} | {tag.get('description', '')}"
        )
    return "\n".join(lines)


@mcp.tool()
def infer_tags(pipeline_id: str) -> str:
    """AI-infer semantic tags for all columns in a table. Preserves user-overridden tags.

    Args:
        pipeline_id: Pipeline ID to infer tags for
    """
    result = _api("POST", f"/api/catalog/tables/{pipeline_id}/tags/infer")
    return (
        f"Inferred tags for {result.get('inferred_count', 0)} columns "
        f"({result.get('user_preserved', 0)} user tags preserved)."
    )


@mcp.tool()
def diagnose_pipeline(pipeline_id: str) -> str:
    """Diagnose why a pipeline is failing or underperforming. Returns root cause analysis.

    Args:
        pipeline_id: Pipeline ID to diagnose
    """
    result = _api("POST", f"/api/pipelines/{pipeline_id}/diagnose")
    lines = [
        f"Diagnosis for pipeline {pipeline_id}:",
        f"Category: {result.get('category', '?')}",
        f"Root Cause: {result.get('root_cause', '?')}",
        f"Confidence: {result.get('confidence', '?')}",
        "",
        f"Recommended Actions:",
    ]
    for action in result.get("recommended_actions", []):
        lines.append(f"  - {action}")
    if result.get("additional_context"):
        lines.append(f"\nContext: {result.get('additional_context')}")
    return "\n".join(lines)


@mcp.tool()
def analyze_impact(pipeline_id: str) -> str:
    """Analyze the downstream blast radius if a pipeline fails or is modified.

    Args:
        pipeline_id: Pipeline ID to analyze impact for
    """
    result = _api("POST", f"/api/pipelines/{pipeline_id}/impact")
    lines = [
        f"Impact Analysis for pipeline {pipeline_id}:",
        f"Severity: {result.get('impact_severity', '?')}",
        f"Blast Radius: {result.get('blast_radius', '?')} downstream",
        "",
    ]
    affected = result.get("affected_pipelines", [])
    if affected:
        lines.append("Affected Pipelines:")
        for p in affected:
            lines.append(f"  - {p.get('pipeline_name', p.get('pipeline_id', '?'))}")
    if result.get("mitigation"):
        lines.append(f"\nMitigation: {result.get('mitigation')}")
    return "\n".join(lines)


@mcp.tool()
def trigger_pipeline(pipeline_id: str) -> str:
    """Trigger a manual run of a pipeline.

    Args:
        pipeline_id: Pipeline ID to trigger
    """
    result = _api("POST", f"/api/pipelines/{pipeline_id}/trigger")
    return f"Pipeline triggered. Run ID: {result.get('run_id', '?')}, Status: {result.get('status', '?')}"


@mcp.tool()
def get_pipeline_runs(pipeline_id: str, limit: int = 5) -> str:
    """Get recent run history for a pipeline.

    Args:
        pipeline_id: Pipeline ID
        limit: Number of runs to return (default 5)
    """
    result = _api("GET", f"/api/pipelines/{pipeline_id}/runs", params={"limit": limit})
    if not result:
        return "No runs found."
    lines = [f"Recent {len(result)} runs:\n"]
    for r in result:
        rows = r.get("rows_loaded", 0) or 0
        lines.append(
            f"- {r.get('status', '?')} | {r.get('started_at', '?')} | "
            f"Rows: {rows} | Run: {r.get('run_id', '?')[:8]}..."
        )
    return "\n".join(lines)


@mcp.tool()
def get_freshness(pipeline_id: str = "") -> str:
    """Check data freshness. Without pipeline_id, returns freshness for all pipelines.

    Args:
        pipeline_id: Optional pipeline ID for specific freshness. Empty for all.
    """
    if pipeline_id:
        result = _api("GET", f"/api/observability/freshness/{pipeline_id}/history", params={"hours": 24})
        if not result:
            return "No freshness data available."
        latest = result[-1] if result else {}
        return (
            f"Freshness for {pipeline_id}: {latest.get('staleness_minutes', '?')}m stale, "
            f"Status: {latest.get('status', '?')}, "
            f"Checked: {latest.get('checked_at', '?')}"
        )
    else:
        result = _api("GET", "/api/freshness")
        if not result:
            return "No freshness data available."
        lines = ["Pipeline Freshness:\n"]
        for f in result:
            lines.append(
                f"- {f.get('pipeline_name', '?')}: {f.get('staleness_minutes', '?')}m stale "
                f"({f.get('status', '?')})"
            )
        return "\n".join(lines)


@mcp.tool()
def generate_narrative(alert_id: str) -> str:
    """Generate a human-readable narrative explanation for an alert.

    Args:
        alert_id: Alert ID to generate narrative for
    """
    result = _api("POST", f"/api/observability/alerts/{alert_id}/narrative")
    return (
        f"[{result.get('severity', '?').upper()}] {result.get('pipeline_name', '?')}:\n"
        f"{result.get('narrative', 'No narrative generated.')}"
    )


@mcp.tool()
def design_topology(description: str) -> str:
    """Design a multi-pipeline data architecture from a natural language description.

    Args:
        description: What you need the data architecture to do (e.g., "merge customer data from Salesforce and Stripe into a unified customer table")
    """
    result = _api("POST", "/api/topology/design", data={"description": description})
    summary = result.get("summary", "")
    pattern = result.get("pattern", "")
    pipelines = result.get("pipelines", [])

    lines = [
        f"Topology Design: {pattern}",
        "",
        summary,
        "",
        f"Pipelines ({len(pipelines)}):",
    ]
    for p in pipelines:
        lines.append(
            f"  - {p.get('name', '?')}: {p.get('source_type', '?')} → {p.get('target_type', '?')}\n"
            f"    {p.get('description', '')}"
        )
    return "\n".join(lines)


@mcp.tool()
def get_business_context(pipeline_id: str) -> str:
    """Get the business context for a pipeline — who uses it, what for, how critical.

    Args:
        pipeline_id: Pipeline ID
    """
    result = _api("GET", f"/api/catalog/tables/{pipeline_id}")
    ctx = result.get("business_context", {})
    if not ctx:
        return f"No business context set for {result.get('pipeline_name', pipeline_id)}."
    lines = [f"Business Context for {result.get('pipeline_name', '?')}:\n"]
    for k, v in ctx.items():
        if not k.startswith("_"):
            lines.append(f"- {k}: {v}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP Prompts — reusable prompt templates
# ---------------------------------------------------------------------------

@mcp.prompt()
def troubleshoot_pipeline(pipeline_name: str) -> str:
    """Help troubleshoot a failing pipeline by gathering context and running diagnostics."""
    return (
        f"I need to troubleshoot the pipeline '{pipeline_name}' in DAPOS. Please:\n\n"
        f"1. Search for the pipeline in the catalog to find its ID and current status\n"
        f"2. Check its recent runs for errors\n"
        f"3. Check its freshness status\n"
        f"4. Run a diagnosis to identify the root cause\n"
        f"5. Check downstream impact\n"
        f"6. Summarize findings and recommend actions\n"
    )


@mcp.prompt()
def explore_catalog(topic: str = "all available data") -> str:
    """Explore the data catalog to understand what data is available and trustworthy."""
    return (
        f"I want to understand what data is available in DAPOS related to: {topic}\n\n"
        f"Please:\n"
        f"1. Search the catalog for relevant tables\n"
        f"2. For the most relevant tables, check their trust scores\n"
        f"3. Look at the semantic tags to understand column meanings\n"
        f"4. Check freshness to see how current the data is\n"
        f"5. Summarize what's available, how trustworthy it is, and any gaps\n"
    )


@mcp.prompt()
def assess_platform_health() -> str:
    """Assess overall platform health — anomalies, failing pipelines, trust scores."""
    return (
        "Please assess the overall health of the DAPOS data platform:\n\n"
        "1. Check for current anomalies across all pipelines\n"
        "2. Review recent alerts\n"
        "3. Check the catalog stats for trust distribution\n"
        "4. Identify any pipelines with low trust scores\n"
        "5. Provide an overall health summary with recommended actions\n"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DAPOS MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse", "streamable-http"], default="stdio")
    parser.add_argument("--port", type=int, default=8101)
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    elif args.transport == "sse":
        mcp.run(transport="sse", port=args.port)
    elif args.transport == "streamable-http":
        mcp.run(transport="streamable-http", port=args.port)
