# MCP Server

DAPOS exposes its capabilities via [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) — Anthropic's open standard for AI agent tool/data discovery. Any MCP-compatible client (Claude Desktop, Cursor, Windsurf, custom agents) can connect to DAPOS and interact with the data catalog, pipelines, and observability layer.

---

## Quick Start

```bash
# Install the MCP dependency
pip install mcp>=1.26.0

# Run in stdio mode (Claude Desktop)
python mcp_server.py

# Run in SSE mode (web clients)
python mcp_server.py --transport sse --port 8101

# Run in streamable-http mode
python mcp_server.py --transport streamable-http --port 8101
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DAPOS_URL` | `http://localhost:8100` | DAPOS API base URL |
| `DAPOS_USER` | `admin` | DAPOS username for authentication |
| `DAPOS_PASSWORD` | `admin` | DAPOS password |

---

## Claude Desktop Configuration

Add this to your Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dapos": {
      "command": "python",
      "args": ["/path/to/pipeline-agent/mcp_server.py"],
      "env": {
        "DAPOS_URL": "http://localhost:8100",
        "DAPOS_USER": "admin",
        "DAPOS_PASSWORD": "admin"
      }
    }
  }
}
```

For a remote DAPOS instance (e.g., Railway):

```json
{
  "mcpServers": {
    "dapos": {
      "command": "python",
      "args": ["/path/to/pipeline-agent/mcp_server.py"],
      "env": {
        "DAPOS_URL": "https://your-app.up.railway.app",
        "DAPOS_USER": "admin",
        "DAPOS_PASSWORD": "your-password"
      }
    }
  }
}
```

---

## Resources (Browsable Data)

Resources are read-only data that MCP clients can browse. DAPOS exposes 9 resources:

| Resource URI | Description |
|-------------|-------------|
| `dapos://catalog` | Overview of all data tables with trust scores, freshness, and quality |
| `dapos://catalog/stats` | High-level catalog statistics — table count, source types, trust distribution |
| `dapos://pipelines` | All pipelines with status, schedule, and source/target info |
| `dapos://alerts` | Recent alerts with narratives (last 24 hours) |
| `dapos://dag` | Pipeline dependency graph — nodes and edges |
| `dapos://anomalies` | Current platform anomaly scan with AI reasoning |
| `dapos://catalog/tables/{pipeline_id}` | Detailed catalog entry for a specific table |
| `dapos://pipelines/{pipeline_id}` | Full pipeline configuration and status |

---

## Tools (Actions)

Tools are actions that AI agents can invoke. DAPOS exposes 13 tools:

### Catalog & Discovery

| Tool | Description |
|------|-------------|
| `search_catalog` | Search the data catalog by name, column, tag, or business context |
| `search_columns` | Search for columns across all tables |
| `get_trust_score` | Get trust score breakdown for a pipeline |
| `get_semantic_tags` | Get semantic tags (business meaning) for all columns |
| `infer_tags` | AI-infer semantic tags for columns (preserves user overrides) |
| `get_business_context` | Get business context — who uses it, what for, how critical |

### Diagnostics & Operations

| Tool | Description |
|------|-------------|
| `diagnose_pipeline` | Root cause analysis for failing pipelines |
| `analyze_impact` | Downstream blast radius analysis |
| `trigger_pipeline` | Trigger a manual pipeline run |
| `get_pipeline_runs` | Get recent run history |
| `get_freshness` | Check data freshness (single pipeline or all) |
| `generate_narrative` | Generate human-readable alert explanation |
| `design_topology` | Design multi-pipeline architecture from natural language |

---

## Prompts (Reusable Templates)

Prompts are pre-built conversation starters that guide AI agents through common workflows:

| Prompt | Description |
|--------|-------------|
| `troubleshoot_pipeline` | Gather context, run diagnosis, check impact, recommend actions |
| `explore_catalog` | Search catalog, check trust scores, review semantic tags, assess freshness |
| `assess_platform_health` | Check anomalies, alerts, trust distribution, identify low-trust pipelines |

### Example usage in Claude Desktop

Ask Claude: *"Use the troubleshoot_pipeline prompt for demo-ecommerce-orders"*

Claude will:
1. Search for the pipeline in the catalog
2. Check recent runs for errors
3. Check freshness status
4. Run a diagnosis
5. Check downstream impact
6. Summarize findings and recommend actions

---

## Architecture

The MCP server is a thin translation layer between MCP protocol and the DAPOS REST API:

```
MCP Client (Claude Desktop, Cursor, etc.)
    |
    | MCP Protocol (stdio / SSE / streamable-http)
    |
    v
mcp_server.py (FastMCP)
    |
    | HTTP + JWT Auth
    |
    v
DAPOS API (FastAPI on port 8100)
    |
    v
PostgreSQL (all state)
```

- **No direct database access** — The MCP server communicates exclusively through the DAPOS REST API
- **JWT authentication** — Authenticates on first request, caches token for subsequent calls
- **Same permissions** — MCP tools respect the same RBAC as the REST API
