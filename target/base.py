"""
Abstract base class for all target connectors.
INTERFACE_VERSION = "1.0"

To add a new target, implement all abstract methods and register via the ConnectorRegistry.
The agent uses this interface definition when generating new connectors via Claude.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Optional

from contracts.models import PipelineContract, RunRecord

# Re-use the same ConnectionResult definition as source/base for consistency.
ConnectionResult = namedtuple(
    "ConnectionResult",
    ["success", "message", "details"],
    defaults=[True, "", {}],
)


class TargetEngine(ABC):
    """Abstract interface that every target connector must implement."""

    INTERFACE_VERSION = "1.0"

    # -- connectivity --------------------------------------------------

    @abstractmethod
    async def test_connection(self) -> ConnectionResult:
        """Connect and return diagnostic info (version, latency, errors)."""

    # -- DDL -----------------------------------------------------------

    @abstractmethod
    def generate_ddl(self, contract: PipelineContract) -> str:
        """
        Generate a CREATE TABLE IF NOT EXISTS statement.

        Uses contract.column_mappings for columns/types and
        contract.target_options for target-specific hints (e.g.
        sort_key / dist_key for Redshift, cluster_by for Snowflake).
        Must include metadata columns:
          _extracted_at, _source_schema, _source_table, _row_hash.
        """

    @abstractmethod
    async def create_table_if_not_exists(self, contract: PipelineContract) -> None:
        """Execute DDL to create the target table if it does not already exist."""

    # -- load lifecycle ------------------------------------------------

    @abstractmethod
    async def load_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        """
        Create a staging table and stream all CSV batches from
        *run.staging_path* into it.  Update *run.rows_loaded* with
        the count of rows inserted.
        """

    @abstractmethod
    async def promote(self, contract: PipelineContract, run: RunRecord) -> None:
        """
        Atomically promote staging data to the target table.

        - merge: DELETE matching rows from target, INSERT from staging.
        - append: INSERT from staging.
        Drop the staging table after success.
        """

    @abstractmethod
    async def drop_staging(self, contract: PipelineContract, run: RunRecord) -> None:
        """Drop staging table.  Must be idempotent (safe if table missing)."""

    # -- quality gate query methods ------------------------------------
    # Every target engine must implement these so the 7-check quality
    # gate can run against the staging table before promotion.

    @abstractmethod
    def get_column_types(self, schema: str, table: str) -> list[dict]:
        """
        Return column metadata from the target information schema.
        Each dict: {column_name, data_type, is_nullable}.
        """

    @abstractmethod
    def get_row_count(self, schema: str, table: str) -> int:
        """Return exact row count of the given table."""

    @abstractmethod
    def get_max_value(self, schema: str, table: str, column: str) -> Optional[str]:
        """Return MAX(column) as a string, or None if the table is empty."""

    @abstractmethod
    def check_duplicates(self, schema: str, table: str, keys: list[str]) -> int:
        """Return count of duplicate key groups (rows where key combo > 1)."""

    @abstractmethod
    def get_null_rates(
        self, schema: str, table: str, columns: list[str]
    ) -> dict[str, float]:
        """Return null fraction per column: {column_name: float}."""

    @abstractmethod
    def get_cardinality(
        self, schema: str, table: str, columns: list[str]
    ) -> dict[str, int]:
        """Return approximate distinct count per column: {column_name: int}."""

    def staging_name(self, contract: PipelineContract, run: RunRecord) -> tuple[str, str]:
        """Return (schema, staging_table_name) for a given run.

        Default convention: {target_table}_stg_{run_id[:8]}.
        Connectors may override this to match their own naming convention.
        """
        schema = contract.target_schema or getattr(self, "default_schema", "raw")
        return schema, f"{contract.target_table}_stg_{run.run_id[:8]}"

    @abstractmethod
    def get_target_type(self) -> str:
        """Return identifier: 'redshift', 'snowflake', 'bigquery', etc."""

    # -- SQL execution (post-promotion hooks) ----------------------------

    async def execute_sql(self, sql: str, timeout_seconds: int = 30) -> list[dict]:
        """Execute SQL and return result rows as list of dicts.

        Override in connectors that support arbitrary SQL execution.
        Used by post-promotion hooks to compute derived metadata.
        """
        raise NotImplementedError(
            f"{self.get_target_type()} does not support execute_sql"
        )

    # -- lifecycle -----------------------------------------------------

    async def close(self) -> None:
        """Release any open connections.  Override if needed."""
        pass
