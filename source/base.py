"""
Abstract base class for all source connectors.
INTERFACE_VERSION = "1.0"

To add a new source, implement all abstract methods and register via the ConnectorRegistry.
The agent uses this interface definition when generating new connectors via Claude.

Metadata columns added to every extract:
  _extracted_at   TIMESTAMPTZ  — UTC timestamp of extraction
  _source_schema  VARCHAR      — originating schema name
  _source_table   VARCHAR      — originating table name
  _row_hash       VARCHAR(64)  — SHA-256 of all source column values concatenated with '|'
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Optional

from contracts.models import PipelineContract, RunRecord


# ---- Result types exposed to all source implementations ----

ConnectionResult = namedtuple(
    "ConnectionResult",
    ["success", "message", "details"],
    defaults=[True, "", {}],
)

SchemaInfo = namedtuple(
    "SchemaInfo",
    ["schema_name", "table_count", "tables"],
    defaults=[0, []],
)

TableProfile = namedtuple(
    "TableProfile",
    [
        "row_count",
        "columns",
        "primary_keys",
        "timestamp_columns",
        "foreign_keys",
        "null_rates",
        "cardinality",
        "sample_rows",
    ],
    defaults=[0, [], [], [], [], {}, {}, []],
)

ExtractResult = namedtuple(
    "ExtractResult",
    ["rows_extracted", "watermark_value", "csv_paths", "manifest"],
    defaults=[0, None, [], {}],
)


class SourceEngine(ABC):
    """Abstract interface that every source connector must implement."""

    INTERFACE_VERSION = "1.0"

    # -- connectivity --------------------------------------------------

    @abstractmethod
    async def test_connection(self) -> ConnectionResult:
        """Test connectivity and return diagnostic info."""

    # -- discovery -----------------------------------------------------

    @abstractmethod
    async def list_schemas(self) -> list[SchemaInfo]:
        """List all accessible schemas with their table counts."""

    @abstractmethod
    async def profile_table(self, schema: str, table: str) -> TableProfile:
        """Profile a single table for row count, columns, keys, stats."""

    # -- extraction ----------------------------------------------------

    @abstractmethod
    async def extract(
        self,
        contract: PipelineContract,
        run: RunRecord,
        staging_dir: str,
        batch_size: int = 100_000,
    ) -> ExtractResult:
        """
        Extract data to CSV batches in *staging_dir*.

        Each row must include four metadata columns appended after all source
        columns:
          _extracted_at   — ISO-8601 UTC timestamp of extraction start
          _source_schema  — contract.source_schema
          _source_table   — contract.source_table
          _row_hash       — SHA-256 hex digest of all source column values
                            joined with '|'
        """

    # -- type mapping --------------------------------------------------

    @abstractmethod
    def map_type(self, source_type: str) -> str:
        """Map a source-native type to a normalised target-compatible type."""

    @abstractmethod
    def get_source_type(self) -> str:
        """Return the source type identifier (e.g. 'mysql', 'postgres')."""

    # -- lifecycle -----------------------------------------------------

    async def close(self) -> None:
        """Release any open connections.  Override if needed."""
        pass
