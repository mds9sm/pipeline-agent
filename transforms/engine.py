"""
Transform engine — ref/var resolution, materialization strategies, SQL validation, lineage parsing.

This module provides the core SQL transform capabilities for DAPOS,
replacing dbt's template engine + materialization logic with a simpler,
integrated approach that works within the step DAG framework.
"""
import logging
import re
from typing import Optional

log = logging.getLogger("transforms.engine")

# ---------------------------------------------------------------------------
# ref() resolution — {{ ref('table_name') }}
# ---------------------------------------------------------------------------

_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")


async def resolve_refs(sql: str, store, pipeline_id: str = "") -> tuple[str, list[str]]:
    """Replace {{ ref('table_name') }} with fully qualified schema.table.

    Looks up tables in three places (in order):
    1. Other SQL transforms by transform_name
    2. Pipeline target tables by target_table name
    3. Raw table name (passthrough if not found — allows referencing existing tables)

    Returns (resolved_sql, list_of_referenced_table_names).
    """
    refs_found = []
    matches = _REF_PATTERN.findall(sql)

    for table_name in matches:
        refs_found.append(table_name)
        resolved = table_name  # default: use as-is

        # Check transforms first
        transform = await store.get_sql_transform_by_name(table_name)
        if transform:
            schema = transform.target_schema or "analytics"
            tbl = transform.target_table or transform.transform_name
            resolved = f"{schema}.{tbl}"
        else:
            # Check pipelines by target_table
            pipeline = await store.get_pipeline_by_target_table(table_name)
            if pipeline:
                schema = pipeline.target_schema or "public"
                resolved = f"{schema}.{table_name}"

        # Replace this specific ref
        sql = sql.replace(
            _REF_PATTERN.search(sql).group(0) if _REF_PATTERN.search(sql) else "",
            resolved,
            1,
        )

    return sql, refs_found


# ---------------------------------------------------------------------------
# var() resolution — {{ var('key') }}
# ---------------------------------------------------------------------------

_VAR_PATTERN = re.compile(r"\{\{\s*var\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")


def resolve_vars(sql: str, step_variables: dict, pipeline_tags: dict = None) -> str:
    """Replace {{ var('key') }} with values from step config or pipeline tags.

    Lookup order: step_variables > pipeline_tags > raise error.
    """
    pipeline_tags = pipeline_tags or {}

    def _replace(match):
        key = match.group(1)
        if key in step_variables:
            return str(step_variables[key])
        if key in pipeline_tags:
            return str(pipeline_tags[key])
        log.warning("Unresolved variable: %s — leaving as-is", key)
        return match.group(0)

    return _VAR_PATTERN.sub(_replace, sql)


# ---------------------------------------------------------------------------
# Materialization strategies
# ---------------------------------------------------------------------------

async def execute_materialization(
    target,
    materialization: str,
    target_schema: str,
    target_table: str,
    sql: str,
    unique_key: list[str] = None,
    timeout: int = 300,
) -> dict:
    """Execute a transform SQL with the specified materialization strategy.

    Returns dict with rows_affected and strategy details.
    """
    unique_key = unique_key or []
    fq_table = f"{target_schema}.{target_table}" if target_schema else target_table

    if materialization == "view":
        ddl = f"CREATE OR REPLACE VIEW {fq_table} AS\n{sql}"
        await target.execute_sql(ddl, timeout)
        return {"strategy": "view", "target": fq_table, "rows_affected": 0}

    elif materialization == "incremental" and unique_key:
        # Use INSERT ... ON CONFLICT for incremental materialization
        # First ensure the table exists
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {fq_table} AS
        SELECT * FROM ({sql}) _src WHERE 1=0
        """
        try:
            await target.execute_sql(create_sql, timeout)
        except Exception:
            pass  # Table already exists

        # Insert with conflict handling on unique key
        uk_cols = ", ".join(unique_key)
        insert_sql = f"""
        INSERT INTO {fq_table}
        SELECT * FROM ({sql}) _src
        ON CONFLICT ({uk_cols}) DO UPDATE SET
        {", ".join(f"{col} = EXCLUDED.{col}" for col in ["*"])}
        """
        # Simpler approach: delete + insert for the incremental window
        merge_sql = f"""
        DELETE FROM {fq_table} WHERE ({uk_cols}) IN (
            SELECT {uk_cols} FROM ({sql}) _src
        );
        INSERT INTO {fq_table} SELECT * FROM ({sql}) _src
        """
        rows = await target.execute_sql(merge_sql, timeout)
        return {
            "strategy": "incremental",
            "target": fq_table,
            "unique_key": unique_key,
            "rows_affected": len(rows) if rows else 0,
        }

    elif materialization == "ephemeral":
        # Ephemeral transforms don't materialize — they're inlined as CTEs
        return {"strategy": "ephemeral", "target": fq_table, "rows_affected": 0}

    else:
        # Default: TABLE materialization (drop + create as)
        drop_sql = f"DROP TABLE IF EXISTS {fq_table} CASCADE"
        create_sql = f"CREATE TABLE {fq_table} AS\n{sql}"
        await target.execute_sql(drop_sql, timeout)
        rows = await target.execute_sql(create_sql, timeout)
        # Get row count
        count_rows = await target.execute_sql(f"SELECT COUNT(*) as cnt FROM {fq_table}", timeout)
        row_count = count_rows[0]["cnt"] if count_rows else 0
        return {"strategy": "table", "target": fq_table, "rows_affected": row_count}


# ---------------------------------------------------------------------------
# SQL validation (dry-run via EXPLAIN)
# ---------------------------------------------------------------------------

async def validate_sql(target, sql: str, timeout: int = 30) -> dict:
    """Validate SQL by running EXPLAIN. Returns plan or error."""
    try:
        explain_sql = f"EXPLAIN (FORMAT JSON) {sql}"
        result = await target.execute_sql(explain_sql, timeout)
        return {
            "valid": True,
            "plan": result[0] if result else {},
            "error": None,
        }
    except Exception as e:
        return {
            "valid": False,
            "plan": None,
            "error": str(e),
        }


async def preview_sql(target, sql: str, limit: int = 10, timeout: int = 30) -> dict:
    """Execute SQL with LIMIT and return sample rows."""
    try:
        preview_sql = f"SELECT * FROM ({sql}) _preview LIMIT {limit}"
        rows = await target.execute_sql(preview_sql, timeout)
        columns = list(rows[0].keys()) if rows else []
        return {
            "rows": rows,
            "columns": columns,
            "row_count": len(rows),
            "truncated": len(rows) >= limit,
        }
    except Exception as e:
        return {
            "rows": [],
            "columns": [],
            "row_count": 0,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# Column lineage parsing (best-effort regex + heuristics)
# ---------------------------------------------------------------------------

_SELECT_COL_PATTERN = re.compile(
    r"""(?ix)
    (?:(\w+)\.)?        # optional table alias
    (\w+)               # column name
    \s+(?:as\s+)?       # AS keyword (optional)
    (\w+)               # alias
    """,
)


def parse_column_lineage(sql: str, target_table: str, refs: list[str]) -> list[dict]:
    """Best-effort column lineage extraction from SQL.

    Parses SELECT clause to find column -> alias mappings.
    For complex SQL, returns empty list (agent can enrich later).
    """
    lineage = []

    # Extract the SELECT clause (before FROM)
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM\s+", sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return lineage

    select_clause = select_match.group(1)

    # Skip SELECT * — no meaningful lineage
    if select_clause.strip() == "*":
        return lineage

    # Split by comma, parse each column expression
    for col_expr in select_clause.split(","):
        col_expr = col_expr.strip()
        if not col_expr:
            continue

        # Pattern: [table.]column [AS] alias
        parts = col_expr.split()
        if len(parts) >= 3 and parts[-2].upper() == "AS":
            # Has explicit alias
            source_expr = " ".join(parts[:-2])
            target_col = parts[-1]
        elif len(parts) == 1:
            # Simple column reference
            source_expr = parts[0]
            target_col = parts[0].split(".")[-1]
        else:
            continue

        # Parse source table.column
        source_parts = source_expr.split(".")
        if len(source_parts) == 2:
            source_table = source_parts[0]
            source_col = source_parts[1]
        else:
            source_table = refs[0] if refs else ""
            source_col = source_parts[0]

        # Check for aggregate functions — mark transformation
        agg_funcs = ["sum", "count", "avg", "min", "max", "coalesce"]
        transformation = "direct"
        for func in agg_funcs:
            if func in col_expr.lower():
                transformation = col_expr.strip()
                break

        lineage.append({
            "source_table": source_table,
            "source_column": source_col,
            "target_table": target_table,
            "target_column": target_col,
            "transformation": transformation,
        })

    return lineage
