"""
Local disk staging manager.
Manages CSV batch files under {data_dir}/staging/{pipeline_id}/{run_id}/
"""
from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


class LocalStagingManager:
    """Manage local-disk staging directories for pipeline runs."""

    def __init__(self, data_dir: str):
        self._root = Path(data_dir) / "staging"

    # -- path helpers --------------------------------------------------

    def run_dir(self, pipeline_id: str, run_id: str) -> Path:
        """Return the staging directory path for a specific run."""
        return self._root / pipeline_id / run_id

    def ensure_run_dir(self, pipeline_id: str, run_id: str) -> Path:
        """Create (if needed) and return the staging directory for a run."""
        path = self.run_dir(pipeline_id, run_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    # -- disk space ----------------------------------------------------

    def check_disk_space(self, max_pct: float) -> tuple[bool, float]:
        """
        Check whether disk usage is below *max_pct* (0-100 scale).
        Returns ``(has_space, used_pct)`` where *used_pct* is 0-100.
        """
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(str(self._root))
            used_pct = (usage.used / usage.total) * 100.0
            return used_pct < max_pct, round(used_pct, 2)
        except OSError as exc:
            log.error("Disk space check failed: %s", exc)
            return False, 100.0

    # -- cleanup -------------------------------------------------------

    def cleanup_run(self, pipeline_id: str, run_id: str) -> None:
        """Remove all staging files for a completed/failed run."""
        path = self.run_dir(pipeline_id, run_id)
        if path.exists():
            shutil.rmtree(path)
            log.info("Cleaned staging: %s", path)

    # -- manifest & batch helpers --------------------------------------

    def get_manifest(self, pipeline_id: str, run_id: str) -> dict:
        """Load the manifest.json written by the source extract step."""
        manifest_path = self.run_dir(pipeline_id, run_id) / "manifest.json"
        if not manifest_path.exists():
            return {}
        with manifest_path.open() as fh:
            return json.load(fh)

    def list_batch_files(self, pipeline_id: str, run_id: str) -> list[Path]:
        """Return sorted list of batch CSV paths for a run."""
        run_path = self.run_dir(pipeline_id, run_id)
        if not run_path.exists():
            return []
        return sorted(
            p for p in run_path.iterdir()
            if p.name.startswith("batch_") and p.suffix == ".csv"
        )

    def total_size_bytes(self, pipeline_id: str, run_id: str) -> int:
        """Sum the size of all batch CSV files for a run."""
        total = 0
        for fpath in self.list_batch_files(pipeline_id, run_id):
            try:
                total += fpath.stat().st_size
            except OSError:
                pass
        return total
