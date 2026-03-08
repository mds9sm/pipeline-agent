"""
ConnectorRegistry -- loads, validates, and hot-reloads source/target connectors.

Connectors are stored as Python source code in ConnectorRecord.code and executed
at runtime inside a sandbox (restricted builtins, import allowlist, AST validation).
"""
from __future__ import annotations

import inspect
import logging
from typing import Optional

from config import Config
from contracts.models import (
    ConnectorRecord,
    ConnectorType,
    ConnectorStatus,
    PipelineStatus,
    now_iso,
    new_id,
)
from contracts.store import ContractStore
from crypto import decrypt_dict, CREDENTIAL_FIELDS
from sandbox import validate_connector_code as _ast_validate, safe_exec
from source.base import SourceEngine
from target.base import TargetEngine

log = logging.getLogger(__name__)

INTERFACE_VERSION = "1.0"


class ConnectorRegistry:
    """
    Sandboxed connector registry with versioning and hot-reload support.

    Connectors are:
      1. AST-validated (sandbox.validate_connector_code)
      2. Executed in a restricted namespace (sandbox.safe_exec)
      3. Introspected to find a concrete SourceEngine / TargetEngine subclass
    """

    def __init__(self, store: ContractStore, config: Config):
        self.store = store
        self.config = config
        # connector_id -> live class (not instance)
        self._source_classes: dict[str, type] = {}
        self._target_classes: dict[str, type] = {}

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    async def bootstrap_seeds(self) -> None:
        """Write seed connector records to the store (if absent) and load them."""
        from connectors.seeds import (
            MYSQL_SOURCE_CODE,
            MYSQL_SOURCE_META,
            SQLITE_SOURCE_CODE,
            SQLITE_SOURCE_META,
            REDSHIFT_TARGET_CODE,
            REDSHIFT_TARGET_META,
            POSTGRES_TARGET_CODE,
            POSTGRES_TARGET_META,
            MONGO_SOURCE_CODE,
            MONGO_SOURCE_META,
            STRIPE_SOURCE_CODE,
            STRIPE_SOURCE_META,
            GOOGLE_ADS_SOURCE_CODE,
            GOOGLE_ADS_SOURCE_META,
            FACEBOOK_INSIGHTS_SOURCE_CODE,
            FACEBOOK_INSIGHTS_SOURCE_META,
        )

        seeds = [
            (MYSQL_SOURCE_META, MYSQL_SOURCE_CODE),
            (SQLITE_SOURCE_META, SQLITE_SOURCE_CODE),
            (REDSHIFT_TARGET_META, REDSHIFT_TARGET_CODE),
            (POSTGRES_TARGET_META, POSTGRES_TARGET_CODE),
            (MONGO_SOURCE_META, MONGO_SOURCE_CODE),
            (STRIPE_SOURCE_META, STRIPE_SOURCE_CODE),
            (GOOGLE_ADS_SOURCE_META, GOOGLE_ADS_SOURCE_CODE),
            (FACEBOOK_INSIGHTS_SOURCE_META, FACEBOOK_INSIGHTS_SOURCE_CODE),
        ]
        for meta, code in seeds:
            name = meta["connector_name"]
            existing = await self.store.get_connector_by_name(name)
            if not existing:
                record = ConnectorRecord(
                    connector_name=name,
                    connector_type=ConnectorType(meta["connector_type"]),
                    source_target_type=meta["source_target_type"],
                    generated_by="seed",
                    interface_version=INTERFACE_VERSION,
                    code=code,
                    status=ConnectorStatus.ACTIVE,
                    approved_by="system",
                    approved_at=now_iso(),
                )
                await self.store.save_connector(record)
                log.info("Seeded connector: %s", name)
            elif not existing.code:
                existing.code = code
                await self.store.save_connector(existing)
                log.info("Updated seed code for: %s", name)

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    async def load_all_active(self) -> None:
        """Load every ACTIVE connector from the store."""
        records = await self.store.list_connectors(status="active")
        for record in records:
            self._load_connector(record)

    # ------------------------------------------------------------------
    # Single-connector load (sandboxed)
    # ------------------------------------------------------------------

    def _load_connector(self, record: ConnectorRecord) -> bool:
        """Validate via sandbox, exec, find class.  Returns True on success."""
        if not record.code:
            log.warning("Connector %s has no code, skipping.", record.connector_name)
            return False

        # Step 1: AST validation
        valid, err = _ast_validate(record.code)
        if not valid:
            log.error(
                "Connector %s failed AST validation: %s",
                record.connector_name,
                err,
            )
            return False

        # Step 2: sandboxed execution
        try:
            ns = safe_exec(record.code)
        except Exception as exc:
            log.error(
                "Connector %s failed sandboxed exec: %s",
                record.connector_name,
                exc,
            )
            return False

        # Step 3: locate the concrete class
        cls = self._find_class(ns, record.connector_type)
        if cls is None:
            log.error(
                "Connector %s: no concrete SourceEngine/TargetEngine subclass found.",
                record.connector_name,
            )
            return False

        if record.connector_type == ConnectorType.SOURCE:
            self._source_classes[record.connector_id] = cls
        else:
            self._target_classes[record.connector_id] = cls

        log.info(
            "Loaded connector: %s (%s)", record.connector_name, record.connector_id
        )
        return True

    # ------------------------------------------------------------------
    # Class introspection
    # ------------------------------------------------------------------

    @staticmethod
    def _find_class(ns: dict, connector_type: ConnectorType) -> Optional[type]:
        base = SourceEngine if connector_type == ConnectorType.SOURCE else TargetEngine
        for obj in ns.values():
            if (
                isinstance(obj, type)
                and issubclass(obj, base)
                and obj is not base
                and not inspect.isabstract(obj)
            ):
                return obj
        return None

    # ------------------------------------------------------------------
    # Full code validation (AST + exec + abstract-method check)
    # ------------------------------------------------------------------

    def validate_connector_code(
        self, code: str, connector_type: ConnectorType
    ) -> tuple[bool, str]:
        """
        Full validation pipeline for connector code:
          1. AST check via sandbox.validate_connector_code
          2. Sandboxed exec
          3. Verify all abstract methods are implemented
        Returns (valid, error_message).
        """
        # AST pass
        valid, err = _ast_validate(code)
        if not valid:
            return False, f"AST validation failed: {err}"

        # Exec pass
        try:
            ns = safe_exec(code)
        except SyntaxError as exc:
            return False, f"Syntax error: {exc}"
        except Exception as exc:
            return False, f"Execution error: {exc}"

        # Class discovery
        cls = self._find_class(ns, connector_type)
        if cls is None:
            return False, "No concrete class implementing SourceEngine/TargetEngine found."

        # Abstract-method coverage
        base = SourceEngine if connector_type == ConnectorType.SOURCE else TargetEngine
        abstract_methods = {
            name
            for name, val in inspect.getmembers(base)
            if getattr(val, "__isabstractmethod__", False)
        }
        implemented = set(cls.__dict__.keys())
        missing = abstract_methods - implemented
        if missing:
            return False, f"Missing implementations: {', '.join(sorted(missing))}"

        return True, ""

    # ------------------------------------------------------------------
    # Instance factories
    # ------------------------------------------------------------------

    async def get_source(self, connector_id: str, params: dict) -> SourceEngine:
        """Return an instantiated SourceEngine, decrypting params if needed."""
        cls = self._source_classes.get(connector_id)
        if cls is None:
            record = await self.store.get_connector(connector_id)
            if record and record.status == ConnectorStatus.ACTIVE:
                if self._load_connector(record):
                    cls = self._source_classes.get(connector_id)
            if cls is None:
                raise ValueError(f"No active source connector found: {connector_id}")

        resolved = self._decrypt_params(params)
        return cls(**resolved)

    async def get_target(self, connector_id: str, params: dict) -> TargetEngine:
        """Return an instantiated TargetEngine, decrypting params if needed."""
        cls = self._target_classes.get(connector_id)
        if cls is None:
            record = await self.store.get_connector(connector_id)
            if record and record.status == ConnectorStatus.ACTIVE:
                if self._load_connector(record):
                    cls = self._target_classes.get(connector_id)
            if cls is None:
                raise ValueError(f"No active target connector found: {connector_id}")

        resolved = self._decrypt_params(params)
        return cls(**resolved)

    def _decrypt_params(self, params: dict) -> dict:
        """Transparently decrypt credential fields when an encryption key is set."""
        if self.config.has_encryption_key:
            return decrypt_dict(params, self.config.encryption_key, CREDENTIAL_FIELDS)
        return dict(params)

    # ------------------------------------------------------------------
    # Hot-reload
    # ------------------------------------------------------------------

    def register_approved_connector(self, record: ConnectorRecord) -> bool:
        """Hot-reload a newly approved connector into the live registry."""
        return self._load_connector(record)

    # ------------------------------------------------------------------
    # Connector upgrade with migration tracking
    # ------------------------------------------------------------------

    async def upgrade_connector(
        self,
        connector_id: str,
        new_code: str,
        new_version: int,
    ) -> dict:
        """
        Upgrade a connector to *new_code* / *new_version*.

        Steps:
          1. Validate the new code.
          2. Find all ACTIVE pipelines that reference this connector.
          3. Create a ConnectorMigration-style record (stored as a
             ContractChangeProposal with change_type UPDATE_CONNECTOR).
          4. Update the ConnectorRecord in the store.
          5. Hot-reload the connector.

        Returns a summary dict with affected pipeline ids.
        """
        from contracts.models import (
            ContractChangeProposal,
            ChangeType,
            TriggerType,
            ProposalStatus,
        )

        # Fetch existing record
        record = await self.store.get_connector(connector_id)
        if record is None:
            raise ValueError(f"Connector not found: {connector_id}")

        # Validate new code
        valid, err = self.validate_connector_code(new_code, record.connector_type)
        if not valid:
            raise ValueError(f"New connector code is invalid: {err}")

        # Find affected pipelines
        all_pipelines = await self.store.list_pipelines(status="active")
        affected_ids: list[str] = []
        for p in all_pipelines:
            if record.connector_type == ConnectorType.SOURCE:
                if p.source_connector_id == connector_id:
                    affected_ids.append(p.pipeline_id)
            else:
                if p.target_connector_id == connector_id:
                    affected_ids.append(p.pipeline_id)

        # Create migration tracking proposal
        proposal = ContractChangeProposal(
            connector_id=connector_id,
            trigger_type=TriggerType.NEW_CONNECTOR,
            change_type=ChangeType.UPDATE_CONNECTOR,
            current_state={
                "version": record.version,
                "code_hash": hash(record.code),
            },
            proposed_state={
                "version": new_version,
                "code_hash": hash(new_code),
                "affected_pipelines": affected_ids,
            },
            reasoning=f"Upgrading connector {record.connector_name} from v{record.version} to v{new_version}.",
            confidence=1.0,
            impact_analysis={
                "affected_pipeline_count": len(affected_ids),
                "affected_pipeline_ids": affected_ids,
            },
            status=ProposalStatus.APPLIED,
            resolved_at=now_iso(),
            resolved_by="system",
        )
        await self.store.save_proposal(proposal)

        # Update the connector record
        record.code = new_code
        record.version = new_version
        record.updated_at = now_iso()
        await self.store.save_connector(record)

        # Hot-reload
        self._load_connector(record)

        log.info(
            "Upgraded connector %s to v%s (%d pipelines affected)",
            record.connector_name,
            new_version,
            len(affected_ids),
        )

        return {
            "connector_id": connector_id,
            "new_version": new_version,
            "affected_pipelines": affected_ids,
            "proposal_id": proposal.proposal_id,
        }

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def is_loaded(self, connector_id: str) -> bool:
        """Check whether a connector class is currently loaded in memory."""
        return (
            connector_id in self._source_classes
            or connector_id in self._target_classes
        )
