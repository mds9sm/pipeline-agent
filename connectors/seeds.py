"""
Seed connector code stored as strings.
On first startup these are written to the ConnectorRecord table and loaded via exec()
exactly like any agent-generated connector -- no special casing.
"""

MYSQL_SOURCE_META = {
    "connector_name": "mysql-source-v1",
    "connector_type": "source",
    "source_target_type": "mysql",
}

SQLITE_SOURCE_META = {
    "connector_name": "sqlite-source-v1",
    "connector_type": "source",
    "source_target_type": "sqlite",
}

REDSHIFT_TARGET_META = {
    "connector_name": "redshift-target-v1",
    "connector_type": "target",
    "source_target_type": "redshift",
}

POSTGRES_TARGET_META = {
    "connector_name": "postgres-target-v1",
    "connector_type": "target",
    "source_target_type": "postgres",
}

MONGO_SOURCE_META = {
    "connector_name": "mongo-source-v1",
    "connector_type": "source",
    "source_target_type": "mongodb",
}

STRIPE_SOURCE_META = {
    "connector_name": "stripe-source-v1",
    "connector_type": "source",
    "source_target_type": "stripe",
}

GOOGLE_ADS_SOURCE_META = {
    "connector_name": "google-ads-source-v1",
    "connector_type": "source",
    "source_target_type": "google_ads",
}

FACEBOOK_INSIGHTS_SOURCE_META = {
    "connector_name": "facebook-insights-source-v1",
    "connector_type": "source",
    "source_target_type": "facebook_insights",
}


MYSQL_SOURCE_CODE = '''
# REQUIRES: PyMySQL>=1.1.0
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pymysql
import pymysql.cursors

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType, ReplicationMethod,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

TYPE_MAP = {
    "tinyint": "SMALLINT", "smallint": "SMALLINT", "mediumint": "INTEGER",
    "int": "INTEGER", "integer": "INTEGER", "bigint": "BIGINT",
    "float": "FLOAT4", "double": "FLOAT8", "real": "FLOAT8",
    "decimal": "DECIMAL", "numeric": "DECIMAL",
    "bit": "BOOLEAN", "bool": "BOOLEAN", "boolean": "BOOLEAN",
    "char": "CHAR", "varchar": "VARCHAR",
    "tinytext": "VARCHAR(255)", "text": "VARCHAR(65535)",
    "mediumtext": "VARCHAR(65535)", "longtext": "VARCHAR(65535)",
    "tinyblob": "VARCHAR(65535)", "blob": "VARCHAR(65535)",
    "mediumblob": "VARCHAR(65535)", "longblob": "VARCHAR(65535)",
    "binary": "VARCHAR(65535)", "varbinary": "VARCHAR(65535)",
    "date": "DATE", "datetime": "TIMESTAMP", "timestamp": "TIMESTAMPTZ",
    "time": "VARCHAR(8)", "year": "SMALLINT",
    "json": "SUPER", "enum": "VARCHAR(255)", "set": "VARCHAR(255)",
    "geometry": "VARCHAR(65535)",
}

TIMESTAMP_TYPES = {"datetime", "timestamp", "date"}


class MySQLEngine(SourceEngine):

    def __init__(self, host: str, port: int = 3306, database: str = "",
                 user: str = "", password: str = "", ssl_ca: str = "", **kwargs):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user or "root"
        self.password = password
        self.ssl_ca = ssl_ca

    def _connect(self, buffered: bool = True) -> pymysql.Connection:
        ssl = {"ca": self.ssl_ca} if self.ssl_ca else None
        cursor_class = pymysql.cursors.DictCursor if buffered else pymysql.cursors.SSDictCursor
        return pymysql.connect(
            host=self.host, port=self.port, database=self.database,
            user=self.user, password=self.password, ssl=ssl,
            charset="utf8mb4", cursorclass=cursor_class,
            autocommit=True, connect_timeout=10,
        )

    def get_source_type(self) -> str:
        return "mysql"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION() AS v")
                version = cur.fetchone()["v"]
                cur.execute("SHOW STATUS LIKE \'Threads_connected\'")
                row = cur.fetchone()
                conn_count = int(row["Value"]) if row else 0
                cur.execute("SHOW VARIABLES LIKE \'ssl_type\'")
                ssl_row = cur.fetchone()
                ssl_enabled = bool(ssl_row and ssl_row.get("Value"))
            conn.close()
            return ConnectionResult(
                success=True, version=version, ssl_enabled=ssl_enabled,
                connection_count=conn_count,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA as schema_name, TABLE_NAME as table_name
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA NOT IN
                        (\'information_schema\',\'performance_schema\',\'mysql\',\'sys\')
                    AND TABLE_TYPE = \'BASE TABLE\'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                rows = cur.fetchall()
        finally:
            conn.close()
        schemas: dict[str, SchemaInfo] = {}
        for row in rows:
            s = row["schema_name"]
            if s not in schemas:
                schemas[s] = SchemaInfo(schema_name=s, table_count=0, tables=[])
            schemas[s].tables.append(row["table_name"])
            schemas[s].table_count += 1
        return list(schemas.values())

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_ROWS FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """, (schema, table))
                row = cur.fetchone()
                row_estimate = int(row["TABLE_ROWS"] or 0) if row else 0

                cur.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY,
                           CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                           ORDINAL_POSITION, COLUMN_TYPE
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """, (schema, table))
                col_rows = cur.fetchall()

                cur.execute("""
                    SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    AND CONSTRAINT_NAME = \'PRIMARY\'
                    ORDER BY ORDINAL_POSITION
                """, (schema, table))
                pks = [r["COLUMN_NAME"] for r in cur.fetchall()]

                cur.execute("""
                    SELECT kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME,
                           kcu.REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    JOIN information_schema.TABLE_CONSTRAINTS tc
                        ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                        AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    WHERE kcu.TABLE_SCHEMA = %s AND kcu.TABLE_NAME = %s
                    AND tc.CONSTRAINT_TYPE = \'FOREIGN KEY\'
                """, (schema, table))
                fks = [{"column": r["COLUMN_NAME"],
                        "referenced_table": r["REFERENCED_TABLE_NAME"],
                        "referenced_column": r["REFERENCED_COLUMN_NAME"]}
                       for r in cur.fetchall()]

                col_names = [r["COLUMN_NAME"] for r in col_rows]
                null_rates: dict[str, float] = {}
                cardinality: dict[str, int] = {}
                if col_names:
                    sample_limit = min(10000, max(row_estimate, 1))
                    null_exprs = ", ".join(
                        f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `null_{c}`,"
                        f"COUNT(DISTINCT `{c}`) AS `card_{c}`"
                        for c in col_names
                    )
                    cur.execute(
                        f"SELECT COUNT(*) AS n, {null_exprs} "
                        f"FROM (SELECT * FROM `{schema}`.`{table}` LIMIT {sample_limit}) s"
                    )
                    stats = cur.fetchone()
                    n = stats["n"] or 1
                    for c in col_names:
                        null_rates[c] = (stats.get(f"null_{c}") or 0) / n
                        cardinality[c] = stats.get(f"card_{c}") or 0

                cur.execute(f"SELECT * FROM `{schema}`.`{table}` LIMIT 5")
                sample_rows = cur.fetchall()
        finally:
            conn.close()

        mappings, ts_cols = [], []
        for r in col_rows:
            col = r["COLUMN_NAME"]
            dtype = r["DATA_TYPE"].lower()
            is_ts = dtype in TIMESTAMP_TYPES
            if is_ts:
                ts_cols.append(col)
            mappings.append(ColumnMapping(
                source_column=col, source_type=r["COLUMN_TYPE"].lower(),
                target_column=col, target_type=self._precise_type(dtype, r),
                is_nullable=(r["IS_NULLABLE"] == "YES"),
                is_primary_key=(col in pks),
                is_incremental_candidate=is_ts,
                ordinal_position=r["ORDINAL_POSITION"],
            ))

        return TableProfile(
            schema_name=schema, table_name=table,
            row_count_estimate=row_estimate, column_count=len(col_rows),
            columns=mappings, primary_keys=pks, timestamp_columns=ts_cols,
            null_rates=null_rates, cardinality=cardinality,
            sample_rows=[dict(r) for r in sample_rows], foreign_keys=fks,
        )

    def map_type(self, source_type: str) -> str:
        return TYPE_MAP.get(source_type.lower().split("(")[0].strip(), "VARCHAR(65535)")

    def _precise_type(self, dtype: str, col_row: dict) -> str:
        base = dtype.lower()
        if base in ("varchar", "char"):
            length = min(int(col_row.get("CHARACTER_MAXIMUM_LENGTH") or 255), 65535)
            return f"VARCHAR({length})" if base == "varchar" else f"CHAR({length})"
        if base in ("decimal", "numeric"):
            return f"DECIMAL({col_row.get(\'NUMERIC_PRECISION\') or 18},{col_row.get(\'NUMERIC_SCALE\') or 0})"
        if base == "tinyint" and "tinyint(1)" in (col_row.get("COLUMN_TYPE") or "").lower():
            return "BOOLEAN"
        return TYPE_MAP.get(base, "VARCHAR(65535)")

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        if contract.replication_method == ReplicationMethod.CDC:
            raise NotImplementedError(
                "MySQL CDC via binlog not yet implemented. "
                "Requires binlog_format=ROW and a binlog reader library."
            )
        os.makedirs(staging_dir, exist_ok=True)
        schema, table = contract.source_schema, contract.source_table
        inc_col = contract.incremental_column
        extracted_at = now_iso()

        where_parts, params = [], []
        if contract.refresh_type == RefreshType.INCREMENTAL and inc_col:
            if run.run_mode.value == "backfill" and run.backfill_start and run.backfill_end:
                where_parts.append(f"`{inc_col}` BETWEEN %s AND %s")
                params.extend([run.backfill_start, run.backfill_end])
            elif contract.last_watermark:
                where_parts.append(f"`{inc_col}` > %s")
                params.append(contract.last_watermark)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        order_sql = f"ORDER BY `{inc_col}`" if (inc_col and contract.refresh_type == RefreshType.INCREMENTAL) else ""
        query = f"SELECT * FROM `{schema}`.`{table}` {where_sql} {order_sql}"

        ssl = {"ca": self.ssl_ca} if self.ssl_ca else None
        conn = pymysql.connect(
            host=self.host, port=self.port, database=self.database,
            user=self.user, password=self.password, ssl=ssl,
            charset="utf8mb4", autocommit=True, connect_timeout=10,
            cursorclass=pymysql.cursors.SSDictCursor,
        )

        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest: dict = {"batches": []}

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns: Optional[list[str]] = None
                batch_rows: list[list] = []
                for row_dict in cur:
                    if columns is None:
                        columns = list(row_dict.keys())
                    row_values = [row_dict[c] for c in columns]
                    if inc_col and inc_col in row_dict and row_dict[inc_col] is not None:
                        wm = str(row_dict[inc_col])
                        if max_watermark is None or wm > max_watermark:
                            max_watermark = wm
                    row_hash = hashlib.sha256("|".join(str(v) for v in row_values).encode()).hexdigest()
                    row_values.extend([extracted_at, schema, table, row_hash])
                    batch_rows.append(row_values)
                    total_rows += 1
                    if len(batch_rows) >= batch_size:
                        batch_num += 1
                        fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                        total_bytes += fbytes
                        manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                        batch_rows = []
                if batch_rows and columns:
                    batch_num += 1
                    fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                    total_bytes += fbytes
                    manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
        finally:
            conn.close()

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(rows_extracted=total_rows, max_watermark=max_watermark,
                             staging_path=staging_dir, staging_size_bytes=total_bytes,
                             batch_count=batch_num, manifest=manifest)

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        fpath = os.path.join(staging_dir, f"batch_{batch_num:06d}.csv")
        all_cols = columns + ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            for row in rows:
                writer.writerow([
                    v.isoformat() if isinstance(v, datetime) else
                    (v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else v)
                    for v in row
                ])
        return fpath, os.path.getsize(fpath)
'''


REDSHIFT_TARGET_CODE = '''
# REQUIRES: psycopg2-binary>=2.9.9
from __future__ import annotations
import json
import logging
import os
import time
from typing import Optional

import psycopg2
import psycopg2.extras

from contracts.models import ConnectionResult, PipelineContract, RunRecord, LoadType
from target.base import TargetEngine

log = logging.getLogger(__name__)

METADATA_COLUMNS = [
    ("_extracted_at", "TIMESTAMPTZ"),
    ("_source_schema", "VARCHAR(255)"),
    ("_source_table", "VARCHAR(255)"),
    ("_row_hash", "VARCHAR(64)"),
]


class RedshiftEngine(TargetEngine):

    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, default_schema: str = "raw"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.default_schema = default_schema

    def _connect(self, autocommit: bool = True):
        conn = psycopg2.connect(
            host=self.host, port=self.port, dbname=self.database,
            user=self.user, password=self.password,
            connect_timeout=15, options="-c statement_timeout=3600000",
        )
        conn.autocommit = autocommit
        return conn

    def get_target_type(self) -> str:
        return "redshift"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
            conn.close()
            return ConnectionResult(success=True, version=version,
                                    latency_ms=int((time.monotonic() - t0) * 1000))
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    def generate_ddl(self, contract: PipelineContract) -> str:
        schema = contract.target_schema or self.default_schema
        opts = contract.target_options
        col_defs = [
            f\'    "{m.target_column}" {m.target_type}{"" if m.is_nullable else " NOT NULL"}\'
            for m in contract.column_mappings
        ] + [f\'    "{n}" {t}\' for n, t in METADATA_COLUMNS]
        columns_sql = ",\\n".join(col_defs)
        sort_key = opts.get("sort_key") or contract.incremental_column
        dist_key = opts.get("dist_key") or (contract.merge_keys[0] if contract.merge_keys else None)
        distkey_clause = f\'\\nDISTKEY("{dist_key}")\' if dist_key else (
            "\\nDISTSTYLE ALL" if opts.get("diststyle") == "all" else "\\nDISTSTYLE EVEN"
        )
        sortkey_clause = f\'\\nSORTKEY("{sort_key}")\' if sort_key else ""
        return (f\'CREATE TABLE IF NOT EXISTS "{schema}"."{contract.target_table}" (\\n\'
                f"{columns_sql}\\n){distkey_clause}{sortkey_clause};")

    async def create_table_if_not_exists(self, contract: PipelineContract) -> None:
        schema = contract.target_schema or self.default_schema
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'CREATE SCHEMA IF NOT EXISTS "{schema}"\')
                cur.execute(self.generate_ddl(contract))
        finally:
            conn.close()

    def staging_name(self, contract, run):
        schema = contract.target_schema or self.default_schema
        return schema, f"{contract.target_table}_staging_{run.run_id[:8]}"

    async def load_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        schema, staging = self.staging_name(contract, run)
        manifest_path = os.path.join(run.staging_path, "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Staging manifest not found: {manifest_path}")
        with open(manifest_path) as f:
            manifest = json.load(f)
        conn = self._connect(autocommit=False)
        try:
            with conn.cursor() as cur:
                cur.execute(f\'CREATE TABLE "{schema}"."{staging}" (LIKE "{schema}"."{contract.target_table}")\')
                conn.commit()
                rows_loaded = 0
                for batch in manifest.get("batches", []):
                    fpath = batch["file"]
                    if not os.path.exists(fpath):
                        raise FileNotFoundError(f"Batch file missing: {fpath}")
                    with open(fpath, "r", encoding="utf-8") as csvf:
                        header_line = csvf.readline().strip()
                        col_names = [c.strip().strip(\'"\') for c in header_line.split(",")]
                        col_list = ", ".join(f\'"{c}"\' for c in col_names)
                        cur.copy_expert(f\'COPY "{schema}"."{staging}" ({col_list}) FROM STDIN WITH CSV\', csvf)
                    rows_loaded += batch.get("rows", 0)
                conn.commit()
            run.rows_loaded = rows_loaded
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def promote(self, contract: PipelineContract, run: RunRecord) -> None:
        schema = contract.target_schema or self.default_schema
        target = f\'"{schema}"."{contract.target_table}"\'
        _, staging_name = self.staging_name(contract, run)
        staging = f\'"{schema}"."{staging_name}"\'
        conn = self._connect(autocommit=False)
        try:
            with conn.cursor() as cur:
                if contract.load_type == LoadType.MERGE and contract.merge_keys:
                    key_join = " AND ".join(f\'{target}."{k}" = s."{k}"\' for k in contract.merge_keys)
                    cur.execute(f"DELETE FROM {target} USING {staging} s WHERE {key_join}")
                cur.execute(f"INSERT INTO {target} SELECT * FROM {staging}")
                cur.execute(f"DROP TABLE {staging}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def drop_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        _, staging_name = self.staging_name(contract, run)
        schema = contract.target_schema or self.default_schema
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'DROP TABLE IF EXISTS "{schema}"."{staging_name}"\')
        except Exception as e:
            log.warning("Could not drop staging: %s", e)
        finally:
            conn.close()

    def get_column_types(self, schema: str, table: str) -> list[dict]:
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_row_count(self, schema: str, table: str) -> int:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT COUNT(*) FROM "{schema}"."{table}"\')
                return cur.fetchone()[0]
        finally:
            conn.close()

    def get_max_value(self, schema: str, table: str, column: str) -> Optional[str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT MAX("{column}") FROM "{schema}"."{table}"\')
                val = cur.fetchone()[0]
                return str(val) if val is not None else None
        finally:
            conn.close()

    def check_duplicates(self, schema: str, table: str, keys: list[str]) -> int:
        key_cols = ", ".join(f\'"{k}"\' for k in keys)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {key_cols}, COUNT(*) AS cnt
                        FROM "{schema}"."{table}"
                        GROUP BY {key_cols} HAVING COUNT(*) > 1
                    ) d
                """)
                return cur.fetchone()[0]
        finally:
            conn.close()

    def get_null_rates(self, schema: str, table: str, columns: list[str]) -> dict[str, float]:
        if not columns:
            return {}
        null_exprs = ", ".join(f\'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "null_{c}"\' for c in columns)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT COUNT(*) AS n, {null_exprs} FROM "{schema}"."{table}"\')
                row = cur.fetchone()
                n = row[0] or 1
                return {c: (row[i + 1] or 0) / n for i, c in enumerate(columns)}
        finally:
            conn.close()

    def get_cardinality(self, schema: str, table: str, columns: list[str]) -> dict[str, int]:
        if not columns:
            return {}
        card_exprs = ", ".join(f\'COUNT(DISTINCT "{c}") AS "card_{c}"\' for c in columns)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT {card_exprs} FROM "{schema}"."{table}"\')
                row = cur.fetchone()
                return {c: (row[i] or 0) for i, c in enumerate(columns)}
        finally:
            conn.close()
'''


# ---------------------------------------------------------------------------
# SQLite Source Connector
# ---------------------------------------------------------------------------

SQLITE_SOURCE_CODE = '''
# REQUIRES: (none -- sqlite3 is stdlib)
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

TYPE_MAP = {
    "integer": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "real": "FLOAT8",
    "float": "FLOAT8",
    "double": "FLOAT8",
    "numeric": "DECIMAL",
    "decimal": "DECIMAL",
    "text": "VARCHAR(65535)",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "blob": "VARCHAR(65535)",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMPTZ",
}

TIMESTAMP_TYPES = {"datetime", "timestamp", "date"}


class SQLiteEngine(SourceEngine):

    def __init__(self, database: str, **kwargs):
        self.database = database

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database)
        conn.row_factory = sqlite3.Row
        return conn

    def get_source_type(self) -> str:
        return "sqlite"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            conn = self._connect()
            cur = conn.execute("SELECT sqlite_version()")
            version = cur.fetchone()[0]
            conn.close()
            return ConnectionResult(
                success=True, version=version, ssl_enabled=False,
                connection_count=1,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type=\'table\' "
                "AND name NOT LIKE \'sqlite_%\' ORDER BY name"
            )
            tables = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
        return [SchemaInfo(schema_name="main", table_count=len(tables), tables=tables)]

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        conn = self._connect()
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM [{table}]")
            row_count = cur.fetchone()[0]

            cur = conn.execute(f"PRAGMA table_info([{table}])")
            pragma_rows = cur.fetchall()

            columns_info = []
            for r in pragma_rows:
                columns_info.append({
                    "cid": r[0], "name": r[1], "type": r[2],
                    "notnull": r[3], "pk": r[4],
                })

            col_names = [c["name"] for c in columns_info]
            pks = [c["name"] for c in columns_info if c["pk"]]

            # Foreign keys
            cur = conn.execute(f"PRAGMA foreign_key_list([{table}])")
            fk_rows = cur.fetchall()
            fks = [{"column": r[3], "referenced_table": r[2],
                     "referenced_column": r[4]} for r in fk_rows]

            # Null rates and cardinality via sample
            null_rates = {}
            cardinality = {}
            if col_names:
                null_exprs = ", ".join(
                    f"SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END)"
                    for c in col_names
                )
                card_exprs = ", ".join(f"COUNT(DISTINCT [{c}])" for c in col_names)
                cur = conn.execute(
                    f"SELECT COUNT(*), {null_exprs}, {card_exprs} FROM [{table}] LIMIT 10000"
                )
                stats = cur.fetchone()
                n = stats[0] or 1
                for i, c in enumerate(col_names):
                    null_rates[c] = (stats[1 + i] or 0) / n
                    cardinality[c] = stats[1 + len(col_names) + i] or 0

            # Sample rows
            cur = conn.execute(f"SELECT * FROM [{table}] LIMIT 5")
            sample_rows = [dict(row) for row in cur.fetchall()]

            # Timestamp columns
            ts_cols = []
            mappings = []
            for c in columns_info:
                dtype = (c["type"] or "text").lower().split("(")[0].strip()
                is_ts = dtype in TIMESTAMP_TYPES
                if is_ts:
                    ts_cols.append(c["name"])
                mappings.append(ColumnMapping(
                    source_column=c["name"],
                    source_type=(c["type"] or "text").lower(),
                    target_column=c["name"],
                    target_type=self.map_type(c["type"] or "text"),
                    is_nullable=(not c["notnull"]),
                    is_primary_key=bool(c["pk"]),
                    is_incremental_candidate=is_ts,
                    ordinal_position=c["cid"],
                ))
        finally:
            conn.close()

        return TableProfile(
            schema_name=schema, table_name=table,
            row_count_estimate=row_count, column_count=len(columns_info),
            columns=mappings, primary_keys=pks, timestamp_columns=ts_cols,
            null_rates=null_rates, cardinality=cardinality,
            sample_rows=sample_rows, foreign_keys=fks,
        )

    def map_type(self, source_type: str) -> str:
        base = (source_type or "text").lower().split("(")[0].strip()
        return TYPE_MAP.get(base, "VARCHAR(65535)")

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        os.makedirs(staging_dir, exist_ok=True)
        table = contract.source_table
        inc_col = contract.incremental_column
        extracted_at = now_iso()

        where_parts, params = [], []
        if contract.refresh_type == RefreshType.INCREMENTAL and inc_col:
            if run.run_mode.value == "backfill" and run.backfill_start and run.backfill_end:
                where_parts.append(f"[{inc_col}] BETWEEN ? AND ?")
                params.extend([run.backfill_start, run.backfill_end])
            elif contract.last_watermark:
                where_parts.append(f"[{inc_col}] > ?")
                params.append(contract.last_watermark)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        order_sql = f"ORDER BY [{inc_col}]" if (inc_col and contract.refresh_type == RefreshType.INCREMENTAL) else ""
        query = f"SELECT * FROM [{table}] {where_sql} {order_sql}"

        conn = self._connect()
        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest = {"batches": []}

        try:
            cur = conn.execute(query, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            batch_rows = []
            for row_tuple in cur:
                row_dict = dict(row_tuple)
                row_values = [row_dict[c] for c in columns]
                if inc_col and inc_col in row_dict and row_dict[inc_col] is not None:
                    wm = str(row_dict[inc_col])
                    if max_watermark is None or wm > max_watermark:
                        max_watermark = wm
                row_hash = hashlib.sha256(
                    "|".join(str(v) for v in row_values).encode()
                ).hexdigest()
                row_values.extend([extracted_at, contract.source_schema or "main", table, row_hash])
                batch_rows.append(row_values)
                total_rows += 1
                if len(batch_rows) >= batch_size:
                    batch_num += 1
                    fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                    total_bytes += fbytes
                    manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                    batch_rows = []
            if batch_rows and columns:
                batch_num += 1
                fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                total_bytes += fbytes
                manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
        finally:
            conn.close()

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(
            rows_extracted=total_rows, max_watermark=max_watermark,
            staging_path=staging_dir, staging_size_bytes=total_bytes,
            batch_count=batch_num, manifest=manifest,
        )

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        fpath = os.path.join(staging_dir, f"batch_{batch_num:06d}.csv")
        all_cols = columns + ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            for row in rows:
                writer.writerow([
                    v.isoformat() if isinstance(v, datetime) else v
                    for v in row
                ])
        return fpath, os.path.getsize(fpath)
'''


# ---------------------------------------------------------------------------
# PostgreSQL Target Connector
# ---------------------------------------------------------------------------

POSTGRES_TARGET_CODE = '''
# REQUIRES: psycopg2-binary>=2.9.9
from __future__ import annotations
import json
import logging
import os
import time
from typing import Optional

import psycopg2
import psycopg2.extras

from contracts.models import ConnectionResult, PipelineContract, RunRecord, LoadType
from target.base import TargetEngine

log = logging.getLogger(__name__)

METADATA_COLUMNS = [
    ("_extracted_at", "TIMESTAMPTZ"),
    ("_source_schema", "VARCHAR(255)"),
    ("_source_table", "VARCHAR(255)"),
    ("_row_hash", "VARCHAR(64)"),
]


class PostgresTargetEngine(TargetEngine):

    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, default_schema: str = "raw"):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.default_schema = default_schema

    def _connect(self, autocommit: bool = True):
        conn = psycopg2.connect(
            host=self.host, port=self.port, dbname=self.database,
            user=self.user, password=self.password,
            connect_timeout=15, options="-c statement_timeout=3600000",
        )
        conn.autocommit = autocommit
        return conn

    def get_target_type(self) -> str:
        return "postgres"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
            conn.close()
            return ConnectionResult(success=True, version=version,
                                    latency_ms=int((time.monotonic() - t0) * 1000))
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    def generate_ddl(self, contract: PipelineContract) -> str:
        schema = contract.target_schema or self.default_schema
        col_defs = []
        for m in contract.column_mappings:
            nullable = "" if m.is_nullable else " NOT NULL"
            col_defs.append(f\'    "{m.target_column}" {m.target_type}{nullable}\')
        for n, t in METADATA_COLUMNS:
            col_defs.append(f\'    "{n}" {t}\')
        columns_sql = ",\\n".join(col_defs)
        return (f\'CREATE TABLE IF NOT EXISTS "{schema}"."{contract.target_table}" (\\n\'
                f"{columns_sql}\\n);")

    async def create_table_if_not_exists(self, contract: PipelineContract) -> None:
        schema = contract.target_schema or self.default_schema
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'CREATE SCHEMA IF NOT EXISTS "{schema}"\')
                cur.execute(self.generate_ddl(contract))
        finally:
            conn.close()

    def staging_name(self, contract, run):
        schema = contract.target_schema or self.default_schema
        return schema, f"{contract.target_table}_stg_{run.run_id[:8]}"

    async def load_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        schema, staging = self.staging_name(contract, run)
        manifest_path = os.path.join(run.staging_path, "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Staging manifest not found: {manifest_path}")
        with open(manifest_path) as f:
            manifest = json.load(f)
        conn = self._connect(autocommit=False)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f\'CREATE TABLE "{schema}"."{staging}" \'
                    f\'(LIKE "{schema}"."{contract.target_table}" INCLUDING ALL)\'
                )
                conn.commit()
                rows_loaded = 0
                for batch in manifest.get("batches", []):
                    fpath = batch["file"]
                    if not os.path.exists(fpath):
                        raise FileNotFoundError(f"Batch file missing: {fpath}")
                    with open(fpath, "r", encoding="utf-8") as csvf:
                        # Read header to build explicit column list — prevents
                        # position mismatch when schema drift adds columns to the
                        # target table that the CSV does not contain.
                        header_line = csvf.readline().strip()
                        col_names = [c.strip().strip(\'"\') for c in header_line.split(",")]
                        col_list = ", ".join(f\'"{c}"\' for c in col_names)
                        cur.copy_expert(
                            f\'COPY "{schema}"."{staging}" ({col_list}) FROM STDIN WITH CSV\',
                            csvf,
                        )
                    rows_loaded += batch.get("rows", 0)
                conn.commit()
            run.rows_loaded = rows_loaded
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def promote(self, contract: PipelineContract, run: RunRecord) -> None:
        schema = contract.target_schema or self.default_schema
        target = f\'"{schema}"."{contract.target_table}"\'
        _, staging_name = self.staging_name(contract, run)
        staging = f\'"{schema}"."{staging_name}"\'
        conn = self._connect(autocommit=False)
        try:
            with conn.cursor() as cur:
                if contract.load_type == LoadType.MERGE and contract.merge_keys:
                    key_join = " AND ".join(
                        f\'{target}."{k}" = s."{k}"\' for k in contract.merge_keys
                    )
                    cur.execute(f"DELETE FROM {target} USING {staging} s WHERE {key_join}")
                cur.execute(f"INSERT INTO {target} SELECT * FROM {staging}")
                cur.execute(f"DROP TABLE IF EXISTS {staging}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def drop_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        _, staging_name = self.staging_name(contract, run)
        schema = contract.target_schema or self.default_schema
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'DROP TABLE IF EXISTS "{schema}"."{staging_name}"\')
        except Exception as e:
            log.warning("Could not drop staging: %s", e)
        finally:
            conn.close()

    def get_column_types(self, schema: str, table: str) -> list[dict]:
        conn = self._connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def get_row_count(self, schema: str, table: str) -> int:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT COUNT(*) FROM "{schema}"."{table}"\')
                return cur.fetchone()[0]
        finally:
            conn.close()

    def get_max_value(self, schema: str, table: str, column: str) -> Optional[str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT MAX("{column}") FROM "{schema}"."{table}"\')
                val = cur.fetchone()[0]
                return str(val) if val is not None else None
        finally:
            conn.close()

    def check_duplicates(self, schema: str, table: str, keys: list[str]) -> int:
        key_cols = ", ".join(f\'"{k}"\' for k in keys)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {key_cols}, COUNT(*) AS cnt
                        FROM "{schema}"."{table}"
                        GROUP BY {key_cols} HAVING COUNT(*) > 1
                    ) d
                """)
                return cur.fetchone()[0]
        finally:
            conn.close()

    def get_null_rates(self, schema: str, table: str, columns: list[str]) -> dict[str, float]:
        if not columns:
            return {}
        null_exprs = ", ".join(
            f\'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "null_{c}"\' for c in columns
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT COUNT(*) AS n, {null_exprs} FROM "{schema}"."{table}"\')
                row = cur.fetchone()
                n = row[0] or 1
                return {c: (row[i + 1] or 0) / n for i, c in enumerate(columns)}
        finally:
            conn.close()

    def get_cardinality(self, schema: str, table: str, columns: list[str]) -> dict[str, int]:
        if not columns:
            return {}
        card_exprs = ", ".join(f\'COUNT(DISTINCT "{c}") AS "card_{c}"\' for c in columns)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f\'SELECT {card_exprs} FROM "{schema}"."{table}"\')
                row = cur.fetchone()
                return {c: (row[i] or 0) for i, c in enumerate(columns)}
        finally:
            conn.close()

    async def execute_sql(self, sql: str, timeout_seconds: int = 30) -> list[dict]:
        conn = self._connect()
        try:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SET statement_timeout = {timeout_seconds * 1000}")
                cur.execute(sql)
                if cur.description:
                    return [dict(r) for r in cur.fetchall()]
                return []
        finally:
            conn.close()
'''


# ===================================================================
# MongoDB source connector
# ===================================================================

MONGO_SOURCE_CODE = '''
# REQUIRES: pymongo>=4.6.0
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pymongo

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

TYPE_MAP = {
    "int": "INTEGER", "int32": "INTEGER", "int64": "BIGINT",
    "long": "BIGINT", "double": "FLOAT8", "decimal": "DECIMAL",
    "string": "VARCHAR(65535)", "str": "VARCHAR(65535)",
    "bool": "BOOLEAN",
    "date": "TIMESTAMPTZ", "datetime": "TIMESTAMPTZ",
    "objectId": "VARCHAR(24)",
    "array": "VARCHAR(65535)", "list": "VARCHAR(65535)",
    "object": "VARCHAR(65535)", "dict": "VARCHAR(65535)",
}


class MongoDBEngine(SourceEngine):

    def __init__(self, host: str = "localhost", port: int = 27017,
                 database: str = "test", **kwargs):
        self.host = host
        self.port = int(port)
        self.database = database

    def _client(self) -> pymongo.MongoClient:
        return pymongo.MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)

    def get_source_type(self) -> str:
        return "mongodb"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            client = self._client()
            info = client.server_info()
            client.close()
            return ConnectionResult(
                success=True, version=info.get("version", "unknown"),
                ssl_enabled=False, connection_count=1,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        client = self._client()
        try:
            db = client[self.database]
            collections = db.list_collection_names()
            return [SchemaInfo(
                schema_name="default",
                table_count=len(collections),
                tables=sorted(collections),
            )]
        finally:
            client.close()

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        client = self._client()
        try:
            db = client[self.database]
            coll = db[table]
            row_count = coll.estimated_document_count()
            sample = list(coll.find().limit(100))

            # Infer columns from sample
            all_keys = {}
            for doc in sample:
                for k, v in doc.items():
                    if k == "_id":
                        continue
                    t = type(v).__name__
                    if k not in all_keys:
                        all_keys[k] = t

            mappings = []
            ts_cols = []
            for i, (col, pytype) in enumerate(all_keys.items()):
                if pytype == "datetime":
                    ts_cols.append(col)
                target_type = TYPE_MAP.get(pytype, "VARCHAR(65535)")
                mappings.append(ColumnMapping(
                    source_column=col, source_type=pytype,
                    target_column=col, target_type=target_type,
                    is_nullable=True, is_primary_key=False,
                    is_incremental_candidate=(pytype == "datetime"),
                    ordinal_position=i,
                ))

            # Null rates and cardinality
            null_rates = {}
            cardinality = {}
            total = max(len(sample), 1)
            for col in all_keys:
                nulls = sum(1 for d in sample if d.get(col) is None)
                distinct = len(set(str(d.get(col)) for d in sample if d.get(col) is not None))
                null_rates[col] = nulls / total
                cardinality[col] = distinct

            sample_rows = []
            for doc in sample[:5]:
                row = {}
                for k, v in doc.items():
                    if k == "_id":
                        row[k] = str(v)
                    elif isinstance(v, datetime):
                        row[k] = v.isoformat()
                    elif isinstance(v, (dict, list)):
                        row[k] = json.dumps(v, default=str)
                    else:
                        row[k] = v
                sample_rows.append(row)

            return TableProfile(
                schema_name=schema, table_name=table,
                row_count_estimate=row_count, column_count=len(mappings),
                columns=mappings, primary_keys=["_id"], timestamp_columns=ts_cols,
                null_rates=null_rates, cardinality=cardinality,
                sample_rows=sample_rows, foreign_keys=[],
            )
        finally:
            client.close()

    def map_type(self, source_type: str) -> str:
        return TYPE_MAP.get(source_type.lower(), "VARCHAR(65535)")

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        os.makedirs(staging_dir, exist_ok=True)
        table = contract.source_table
        inc_col = contract.incremental_column
        extracted_at = now_iso()

        query = {}
        if contract.refresh_type == RefreshType.INCREMENTAL and inc_col and contract.last_watermark:
            try:
                wm = datetime.fromisoformat(contract.last_watermark)
                query[inc_col] = {"$gt": wm}
            except ValueError:
                query[inc_col] = {"$gt": contract.last_watermark}

        sort_spec = [(inc_col, pymongo.ASCENDING)] if inc_col else None

        client = self._client()
        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest = {"batches": []}

        try:
            db = client[self.database]
            coll = db[table]
            cursor = coll.find(query, sort=sort_spec)

            columns = None
            batch_rows = []
            for doc in cursor:
                if columns is None:
                    columns = [k for k in doc.keys() if k != "_id"]

                row_values = []
                for c in columns:
                    v = doc.get(c)
                    if isinstance(v, datetime):
                        v = v.isoformat()
                    elif isinstance(v, (dict, list)):
                        v = json.dumps(v, default=str)
                    elif hasattr(v, "__str__") and not isinstance(v, (str, int, float, bool, type(None))):
                        v = str(v)
                    row_values.append(v)

                if inc_col and inc_col in doc and doc[inc_col] is not None:
                    wm_val = doc[inc_col]
                    wm_str = wm_val.isoformat() if isinstance(wm_val, datetime) else str(wm_val)
                    if max_watermark is None or wm_str > max_watermark:
                        max_watermark = wm_str

                row_hash = hashlib.sha256(
                    "|".join(str(v) for v in row_values).encode()
                ).hexdigest()
                row_values.extend([extracted_at, "default", table, row_hash])
                batch_rows.append(row_values)
                total_rows += 1

                if len(batch_rows) >= batch_size:
                    batch_num += 1
                    fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                    total_bytes += fbytes
                    manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                    batch_rows = []

            if batch_rows and columns:
                batch_num += 1
                fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                total_bytes += fbytes
                manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
        finally:
            client.close()

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(rows_extracted=total_rows, max_watermark=max_watermark,
                             staging_path=staging_dir, staging_size_bytes=total_bytes,
                             batch_count=batch_num, manifest=manifest)

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        meta_cols = ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        all_cols = columns + meta_cols
        fname = f"batch_{batch_num:04d}.csv"
        fpath = os.path.join(staging_dir, fname)
        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            writer.writerows(rows)
        return fpath, os.path.getsize(fpath)
'''


# ===================================================================
# Stripe source connector
# ===================================================================

STRIPE_SOURCE_CODE = '''
# REQUIRES: httpx>=0.26.0
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

STRIPE_RESOURCES = {
    "charges": {"columns": ["id", "amount", "currency", "status", "customer",
                             "description", "created", "paid", "refunded",
                             "amount_refunded"]},
    "customers": {"columns": ["id", "email", "name", "created", "currency",
                               "balance", "delinquent"]},
}


class StripeEngine(SourceEngine):

    def __init__(self, host: str = "https://api.stripe.com", database: str = "",
                 **kwargs):
        self.base_url = host.rstrip("/")
        self.api_key = database  # API key passed via database field

    def get_source_type(self) -> str:
        return "stripe"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(
                    f"{self.base_url}/v1/charges",
                    params={"limit": 1},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
                resp.raise_for_status()
            return ConnectionResult(
                success=True, version="stripe-api-v1",
                ssl_enabled=self.base_url.startswith("https"),
                connection_count=1,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        return [SchemaInfo(
            schema_name="stripe",
            table_count=len(STRIPE_RESOURCES),
            tables=sorted(STRIPE_RESOURCES.keys()),
        )]

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        resource = STRIPE_RESOURCES.get(table, {})
        cols = resource.get("columns", [])

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.get(
                    f"{self.base_url}/v1/{table}",
                    params={"limit": 5},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("data", [])
                total = data.get("total_count", len(items))
        except Exception:
            items = []
            total = 0

        mappings = []
        for i, col in enumerate(cols):
            mappings.append(ColumnMapping(
                source_column=col, source_type="string",
                target_column=col, target_type="VARCHAR(65535)",
                is_nullable=True, is_primary_key=(col == "id"),
                is_incremental_candidate=(col == "created"),
                ordinal_position=i,
            ))

        sample_rows = []
        for item in items[:5]:
            sample_rows.append({c: item.get(c) for c in cols if c in item})

        return TableProfile(
            schema_name=schema, table_name=table,
            row_count_estimate=total, column_count=len(cols),
            columns=mappings, primary_keys=["id"],
            timestamp_columns=["created"],
            null_rates={}, cardinality={},
            sample_rows=sample_rows, foreign_keys=[],
        )

    def map_type(self, source_type: str) -> str:
        return "VARCHAR(65535)"

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        os.makedirs(staging_dir, exist_ok=True)
        table = contract.source_table
        extracted_at = now_iso()

        resource = STRIPE_RESOURCES.get(table, {})
        columns = resource.get("columns", [])
        if not columns:
            return ExtractResult(rows_extracted=0, max_watermark=None,
                                 staging_path=staging_dir, staging_size_bytes=0,
                                 batch_count=0, manifest={})

        all_items = []
        starting_after = None
        with httpx.Client(timeout=30) as client:
            while True:
                params = {"limit": 100}
                if starting_after:
                    params["starting_after"] = starting_after
                resp = client.get(
                    f"{self.base_url}/v1/{table}",
                    params=params,
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("data", [])
                all_items.extend(items)
                if not data.get("has_more") or not items:
                    break
                starting_after = items[-1]["id"]

        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest = {"batches": []}
        batch_rows = []

        for item in all_items:
            row_values = []
            for col in columns:
                v = item.get(col)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, default=str)
                row_values.append(v)

            if "created" in item and item["created"] is not None:
                wm = str(item["created"])
                if max_watermark is None or wm > max_watermark:
                    max_watermark = wm

            row_hash = hashlib.sha256(
                "|".join(str(v) for v in row_values).encode()
            ).hexdigest()
            row_values.extend([extracted_at, "stripe", table, row_hash])
            batch_rows.append(row_values)
            total_rows += 1

            if len(batch_rows) >= batch_size:
                batch_num += 1
                fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                total_bytes += fbytes
                manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                batch_rows = []

        if batch_rows:
            batch_num += 1
            fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
            total_bytes += fbytes
            manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(rows_extracted=total_rows, max_watermark=max_watermark,
                             staging_path=staging_dir, staging_size_bytes=total_bytes,
                             batch_count=batch_num, manifest=manifest)

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        meta_cols = ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        all_cols = columns + meta_cols
        fname = f"batch_{batch_num:04d}.csv"
        fpath = os.path.join(staging_dir, fname)
        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            writer.writerows(rows)
        return fpath, os.path.getsize(fpath)
'''


# ===================================================================
# Google Ads source connector
# ===================================================================

GOOGLE_ADS_SOURCE_CODE = '''
# REQUIRES: httpx>=0.26.0
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

REPORT_TABLES = {
    "campaign_performance": {
        "columns": ["campaign_id", "campaign_name", "status", "date",
                     "impressions", "clicks", "cost_micros", "conversions",
                     "conversion_value", "ctr"],
    },
}


class GoogleAdsEngine(SourceEngine):

    def __init__(self, host: str = "https://googleads.googleapis.com",
                 database: str = "", port: int = 0, **kwargs):
        self.base_url = host.rstrip("/")
        self.api_key = database  # API key / developer token
        self.customer_id = str(port) if port else "0"

    def get_source_type(self) -> str:
        return "google_ads"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(
                    f"{self.base_url}/v1/customers/{self.customer_id}/googleAds",
                    params={"page_size": 1},
                )
                resp.raise_for_status()
            return ConnectionResult(
                success=True, version="google-ads-api-v1",
                ssl_enabled=self.base_url.startswith("https"),
                connection_count=1,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        return [SchemaInfo(
            schema_name="google_ads",
            table_count=len(REPORT_TABLES),
            tables=sorted(REPORT_TABLES.keys()),
        )]

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        report = REPORT_TABLES.get(table, {})
        cols = report.get("columns", [])

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.get(
                    f"{self.base_url}/v1/customers/{self.customer_id}/googleAds",
                    params={"page_size": 5},
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("results", [])
                total = data.get("total_results_count", len(items))
        except Exception:
            items = []
            total = 0

        mappings = []
        for i, col in enumerate(cols):
            target_type = "VARCHAR(65535)"
            if col in ("impressions", "clicks", "conversions"):
                target_type = "INTEGER"
            elif col in ("cost_micros",):
                target_type = "BIGINT"
            elif col in ("conversion_value", "ctr"):
                target_type = "FLOAT8"
            mappings.append(ColumnMapping(
                source_column=col, source_type="string",
                target_column=col, target_type=target_type,
                is_nullable=True,
                is_primary_key=(col == "campaign_id"),
                is_incremental_candidate=(col == "date"),
                ordinal_position=i,
            ))

        sample_rows = [{c: item.get(c) for c in cols} for item in items[:5]]
        return TableProfile(
            schema_name=schema, table_name=table,
            row_count_estimate=total, column_count=len(cols),
            columns=mappings, primary_keys=["campaign_id"],
            timestamp_columns=["date"],
            null_rates={}, cardinality={},
            sample_rows=sample_rows, foreign_keys=[],
        )

    def map_type(self, source_type: str) -> str:
        return "VARCHAR(65535)"

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        os.makedirs(staging_dir, exist_ok=True)
        table = contract.source_table
        extracted_at = now_iso()

        report = REPORT_TABLES.get(table, {})
        columns = report.get("columns", [])
        if not columns:
            return ExtractResult(rows_extracted=0, max_watermark=None,
                                 staging_path=staging_dir, staging_size_bytes=0,
                                 batch_count=0, manifest={})

        all_items = []
        page_token = None
        with httpx.Client(timeout=30) as client:
            while True:
                params = {"page_size": 100}
                if page_token:
                    params["page_token"] = page_token
                resp = client.get(
                    f"{self.base_url}/v1/customers/{self.customer_id}/googleAds",
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("results", [])
                all_items.extend(items)
                page_token = data.get("next_page_token")
                if not page_token or not items:
                    break

        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest = {"batches": []}
        batch_rows = []

        for item in all_items:
            row_values = [item.get(c) for c in columns]

            if "date" in item and item["date"] is not None:
                wm = str(item["date"])
                if max_watermark is None or wm > max_watermark:
                    max_watermark = wm

            row_hash = hashlib.sha256(
                "|".join(str(v) for v in row_values).encode()
            ).hexdigest()
            row_values.extend([extracted_at, "google_ads", table, row_hash])
            batch_rows.append(row_values)
            total_rows += 1

            if len(batch_rows) >= batch_size:
                batch_num += 1
                fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                total_bytes += fbytes
                manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                batch_rows = []

        if batch_rows:
            batch_num += 1
            fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
            total_bytes += fbytes
            manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(rows_extracted=total_rows, max_watermark=max_watermark,
                             staging_path=staging_dir, staging_size_bytes=total_bytes,
                             batch_count=batch_num, manifest=manifest)

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        meta_cols = ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        all_cols = columns + meta_cols
        fname = f"batch_{batch_num:04d}.csv"
        fpath = os.path.join(staging_dir, fname)
        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            writer.writerows(rows)
        return fpath, os.path.getsize(fpath)
'''


# ===================================================================
# Facebook Insights source connector
# ===================================================================

FACEBOOK_INSIGHTS_SOURCE_CODE = '''
# REQUIRES: httpx>=0.26.0
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from contracts.models import (
    ConnectionResult, SchemaInfo, TableProfile, ColumnMapping,
    PipelineContract, RunRecord, ExtractResult, RefreshType,
    now_iso,
)
from source.base import SourceEngine

log = logging.getLogger(__name__)

INSIGHT_TABLES = {
    "ad_insights": {
        "columns": ["ad_id", "ad_name", "adset_id", "adset_name",
                     "campaign_id", "campaign_name", "date_start", "date_stop",
                     "impressions", "clicks", "spend"],
    },
}


class FacebookInsightsEngine(SourceEngine):

    def __init__(self, host: str = "https://graph.facebook.com",
                 database: str = "", port: int = 0, **kwargs):
        self.base_url = host.rstrip("/")
        self.access_token = database  # access token via database field
        self.ad_account_id = str(port) if port else "act_demo"

    def get_source_type(self) -> str:
        return "facebook_insights"

    async def test_connection(self) -> ConnectionResult:
        t0 = time.monotonic()
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(
                    f"{self.base_url}/v1/{self.ad_account_id}/insights",
                    params={"limit": 1, "access_token": self.access_token},
                )
                resp.raise_for_status()
            return ConnectionResult(
                success=True, version="facebook-insights-api-v1",
                ssl_enabled=self.base_url.startswith("https"),
                connection_count=1,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except Exception as e:
            return ConnectionResult(success=False, error=str(e))

    async def list_schemas(self) -> list[SchemaInfo]:
        return [SchemaInfo(
            schema_name="facebook",
            table_count=len(INSIGHT_TABLES),
            tables=sorted(INSIGHT_TABLES.keys()),
        )]

    async def profile_table(self, schema: str, table: str) -> TableProfile:
        report = INSIGHT_TABLES.get(table, {})
        cols = report.get("columns", [])

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.get(
                    f"{self.base_url}/v1/{self.ad_account_id}/insights",
                    params={"limit": 5, "access_token": self.access_token},
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("data", [])
        except Exception:
            items = []

        mappings = []
        for i, col in enumerate(cols):
            target_type = "VARCHAR(65535)"
            if col in ("impressions", "clicks"):
                target_type = "INTEGER"
            elif col in ("spend",):
                target_type = "DECIMAL(12,2)"
            mappings.append(ColumnMapping(
                source_column=col, source_type="string",
                target_column=col, target_type=target_type,
                is_nullable=True,
                is_primary_key=(col == "ad_id"),
                is_incremental_candidate=(col == "date_start"),
                ordinal_position=i,
            ))

        sample_rows = [{c: item.get(c) for c in cols} for item in items[:5]]
        return TableProfile(
            schema_name=schema, table_name=table,
            row_count_estimate=len(items), column_count=len(cols),
            columns=mappings, primary_keys=["ad_id"],
            timestamp_columns=["date_start"],
            null_rates={}, cardinality={},
            sample_rows=sample_rows, foreign_keys=[],
        )

    def map_type(self, source_type: str) -> str:
        return "VARCHAR(65535)"

    async def extract(self, contract: PipelineContract, run: RunRecord,
                      staging_dir: str, batch_size: int = 100_000) -> ExtractResult:
        os.makedirs(staging_dir, exist_ok=True)
        table = contract.source_table
        extracted_at = now_iso()

        report = INSIGHT_TABLES.get(table, {})
        columns = report.get("columns", [])
        if not columns:
            return ExtractResult(rows_extracted=0, max_watermark=None,
                                 staging_path=staging_dir, staging_size_bytes=0,
                                 batch_count=0, manifest={})

        all_items = []
        after_cursor = None
        with httpx.Client(timeout=30) as client:
            while True:
                params = {"limit": 100, "access_token": self.access_token}
                if after_cursor:
                    params["after"] = after_cursor
                resp = client.get(
                    f"{self.base_url}/v1/{self.ad_account_id}/insights",
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
                items = data.get("data", [])
                all_items.extend(items)
                paging = data.get("paging", {})
                after_cursor = paging.get("cursors", {}).get("after")
                if not paging.get("next") or not items:
                    break

        total_rows = batch_num = total_bytes = 0
        max_watermark = contract.last_watermark
        manifest = {"batches": []}
        batch_rows = []

        for item in all_items:
            row_values = []
            for col in columns:
                v = item.get(col)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, default=str)
                row_values.append(v)

            if "date_start" in item and item["date_start"] is not None:
                wm = str(item["date_start"])
                if max_watermark is None or wm > max_watermark:
                    max_watermark = wm

            row_hash = hashlib.sha256(
                "|".join(str(v) for v in row_values).encode()
            ).hexdigest()
            row_values.extend([extracted_at, "facebook", table, row_hash])
            batch_rows.append(row_values)
            total_rows += 1

            if len(batch_rows) >= batch_size:
                batch_num += 1
                fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
                total_bytes += fbytes
                manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})
                batch_rows = []

        if batch_rows:
            batch_num += 1
            fpath, fbytes = self._write_batch(staging_dir, batch_num, columns, batch_rows)
            total_bytes += fbytes
            manifest["batches"].append({"file": fpath, "rows": len(batch_rows), "bytes": fbytes})

        manifest.update({"total_rows": total_rows, "total_bytes": total_bytes})
        with open(os.path.join(staging_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        return ExtractResult(rows_extracted=total_rows, max_watermark=max_watermark,
                             staging_path=staging_dir, staging_size_bytes=total_bytes,
                             batch_count=batch_num, manifest=manifest)

    def _write_batch(self, staging_dir, batch_num, columns, rows):
        meta_cols = ["_extracted_at", "_source_schema", "_source_table", "_row_hash"]
        all_cols = columns + meta_cols
        fname = f"batch_{batch_num:04d}.csv"
        fpath = os.path.join(staging_dir, fname)
        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(all_cols)
            writer.writerows(rows)
        return fpath, os.path.getsize(fpath)
'''
