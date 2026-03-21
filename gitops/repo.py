"""
GitOps repository manager for pipeline configs.

Manages a separate git repo (e.g. client1-dags-repo) that stores:
  - pipelines/{name}.yaml   — pipeline contract as YAML
  - connectors/{name}.py    — connector source code
  - README.md               — auto-generated index

Multi-developer workflow:
  - Each DAPOS instance (dev/staging/prod) can target the same remote repo
  - Pull-before-commit ensures latest state before writing
  - Auto-push after commit shares changes with team
  - Conflict resolution: auto-rebase for non-overlapping changes,
    force-write + recommit for true conflicts (DAPOS is source of truth)
  - Branch-per-environment: dev, staging, prod branches isolate changes

All structural changes (approvals, pipeline CRUD) auto-commit here.
"""

import asyncio
import logging
import os
import subprocess
import time
from typing import Optional

from contracts.models import PipelineContract, ConnectorRecord

log = logging.getLogger(__name__)


class GitOpsRepo:
    """Manages a git repository for pipeline configuration versioning.

    Supports multi-developer workflows via remote sync:
      - PIPELINE_REPO_REMOTE: URL of shared remote (GitHub/GitLab)
      - GITOPS_AUTO_PUSH: push after every commit (default: true)
      - GITOPS_AUTO_PULL: pull before every commit (default: true)
      - PIPELINE_REPO_BRANCH: branch per environment (e.g. dev, staging, prod)
    """

    def __init__(
        self,
        repo_path: str,
        branch: str = "main",
        remote_url: str = "",
        auto_push: bool = True,
        auto_pull: bool = True,
    ):
        self.repo_path = os.path.abspath(repo_path) if repo_path else ""
        self.branch = branch
        self.remote_url = remote_url
        self.auto_push = auto_push and bool(remote_url)
        self.auto_pull = auto_pull and bool(remote_url)
        self._enabled = bool(repo_path)
        self._needs_reconcile = False  # set True on conflict, cleared by reconcile()
        self._last_reconcile: float = 0

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def has_remote(self) -> bool:
        return bool(self.remote_url)

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    def init_repo(self) -> bool:
        """Initialize the repo if it doesn't exist. Returns True if ready."""
        if not self._enabled:
            return False

        try:
            if not os.path.isdir(self.repo_path):
                os.makedirs(self.repo_path, exist_ok=True)

            git_dir = os.path.join(self.repo_path, ".git")
            if not os.path.isdir(git_dir):
                if self.remote_url:
                    # Clone from remote if available
                    try:
                        self._git_raw(
                            "clone", "-b", self.branch,
                            self.remote_url, self.repo_path,
                        )
                        log.info(
                            "GitOps repo cloned from %s (branch: %s)",
                            self.remote_url, self.branch,
                        )
                    except RuntimeError:
                        # Remote might not have this branch yet — init fresh
                        log.info(
                            "Remote clone failed (branch may not exist), initializing fresh repo"
                        )
                        self._init_fresh()
                        self._setup_remote()
                else:
                    self._init_fresh()
            else:
                log.info("GitOps repo found at %s", self.repo_path)
                # Ensure remote is configured if URL provided
                if self.remote_url:
                    self._setup_remote()
                # Pull latest on boot
                if self.auto_pull:
                    self._pull()

            # Ensure directories exist
            os.makedirs(os.path.join(self.repo_path, "pipelines"), exist_ok=True)
            os.makedirs(os.path.join(self.repo_path, "connectors"), exist_ok=True)
            return True

        except Exception as e:
            log.error("Failed to init GitOps repo: %s", e)
            self._enabled = False
            return False

    def _init_fresh(self):
        """Initialize a brand new repo with README."""
        self._git("init")
        self._git("checkout", "-b", self.branch)
        readme_path = os.path.join(self.repo_path, "README.md")
        with open(readme_path, "w") as f:
            f.write(
                "# Pipeline Configs\n\n"
                "Auto-managed by DAPOS. Do not edit directly — "
                "changes are overwritten on next approval.\n\n"
                "## Structure\n\n"
                "```\n"
                "pipelines/       # Pipeline contract YAML files\n"
                "connectors/      # Connector source code (Python)\n"
                "```\n"
            )
        self._git("add", "README.md")
        self._commit("Initial commit — DAPOS pipeline config repo")
        log.info("GitOps repo initialized at %s", self.repo_path)

    def _setup_remote(self):
        """Configure the remote origin if not already set."""
        try:
            current = self._git("remote", "get-url", "origin").strip()
            if current != self.remote_url:
                self._git("remote", "set-url", "origin", self.remote_url)
                log.info("GitOps remote updated to %s", self.remote_url)
        except RuntimeError:
            # No remote configured yet
            self._git("remote", "add", "origin", self.remote_url)
            log.info("GitOps remote added: %s", self.remote_url)

        # Set upstream tracking
        try:
            self._git("fetch", "origin")
            # Check if remote branch exists
            try:
                self._git("rev-parse", f"origin/{self.branch}")
                # Remote branch exists — set tracking
                self._git(
                    "branch", f"--set-upstream-to=origin/{self.branch}", self.branch
                )
            except RuntimeError:
                # Remote branch doesn't exist yet — will be created on first push
                log.info("Remote branch %s doesn't exist yet, will create on first push", self.branch)
        except RuntimeError as e:
            log.warning("GitOps fetch failed (will retry on next commit): %s", e)

    # ------------------------------------------------------------------
    # Remote sync
    # ------------------------------------------------------------------

    def _pull(self):
        """Pull latest from remote. Handles conflicts by favoring local (DAPOS is source of truth)."""
        if not self.auto_pull:
            return

        try:
            self._git("fetch", "origin")
            # Check if remote branch exists
            try:
                self._git("rev-parse", f"origin/{self.branch}")
            except RuntimeError:
                return  # No remote branch yet

            # Try rebase (cleaner history than merge)
            try:
                self._git("rebase", f"origin/{self.branch}")
                log.debug("GitOps pull: rebased on origin/%s", self.branch)
            except RuntimeError:
                # Rebase conflict — abort and force-accept local
                # DAPOS DB is the source of truth; local state wins
                log.warning(
                    "GitOps rebase conflict — aborting rebase, marking for reconciliation."
                )
                try:
                    self._git("rebase", "--abort")
                except RuntimeError:
                    pass
                self._needs_reconcile = True
        except RuntimeError as e:
            log.warning("GitOps pull failed: %s", e)

    def _push(self):
        """Push to remote after commit."""
        if not self.auto_push:
            return

        try:
            self._git("push", "-u", "origin", self.branch)
            log.debug("GitOps push: pushed to origin/%s", self.branch)
        except RuntimeError:
            # Push rejected (remote has new commits) — pull and retry once
            try:
                self._pull()
                self._git("push", "-u", "origin", self.branch)
                log.debug("GitOps push: pushed after pull-rebase")
            except RuntimeError as e:
                log.warning("GitOps push failed after retry: %s", e)

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    @property
    def needs_reconcile(self) -> bool:
        return self._needs_reconcile

    def reconcile(
        self,
        pipelines: list[tuple[PipelineContract, str]],
        connectors: list[ConnectorRecord],
    ) -> Optional[str]:
        """Force-reconcile repo with DB state after a conflict.

        Rewrites all pipeline YAML and connector code from the DB (source of
        truth), commits, and force-pushes to overwrite the conflicting remote
        state.  Called by the observability loop on a 5-minute cadence when
        _needs_reconcile is True, or can be triggered manually.

        Returns commit hash or None.
        """
        if not self._enabled or not self._needs_reconcile:
            return None

        log.info("GitOps reconcile: rewriting all files from DB state...")
        try:
            # Write all files from DB (source of truth)
            for pipeline, yaml_content in pipelines:
                safe_name = self._safe_name(pipeline.pipeline_name)
                path = os.path.join(self.repo_path, "pipelines", f"{safe_name}.yaml")
                with open(path, "w") as f:
                    f.write(yaml_content)
                self._git("add", f"pipelines/{safe_name}.yaml")

            for connector in connectors:
                safe_name = self._safe_name(connector.connector_name)
                path = os.path.join(self.repo_path, "connectors", f"{safe_name}.py")
                with open(path, "w") as f:
                    f.write(f'"""\nConnector: {connector.connector_name}\n')
                    f.write(f"Type: {connector.connector_type}\n")
                    f.write(f"Version: {connector.version}\n")
                    f.write(f'"""\n\n')
                    f.write(connector.code or "# No code available\n")
                self._git("add", f"connectors/{safe_name}.py")

            commit = self._commit(
                f"Reconcile: full sync from DB ({len(pipelines)} pipelines, {len(connectors)} connectors)",
                author="dapos-reconcile",
            )

            if commit and self.auto_push:
                try:
                    self._git("push", "--force-with-lease", "-u", "origin", self.branch)
                    log.info("GitOps reconcile: force-pushed to origin/%s (%s)", self.branch, commit[:8])
                except RuntimeError as e:
                    log.warning("GitOps reconcile push failed: %s", e)

            self._needs_reconcile = False
            self._last_reconcile = time.time()
            log.info("GitOps reconcile complete: %s", commit[:8] if commit else "no changes")
            return commit

        except Exception as e:
            log.error("GitOps reconcile failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Restore (git repo → DB)
    # ------------------------------------------------------------------

    def read_all_pipeline_yamls(self) -> list[str]:
        """Read all pipeline YAML files from the repo. Returns list of YAML strings."""
        if not self._enabled:
            return []
        pipeline_dir = os.path.join(self.repo_path, "pipelines")
        if not os.path.isdir(pipeline_dir):
            return []
        results = []
        for fname in sorted(os.listdir(pipeline_dir)):
            if fname.endswith(".yaml"):
                path = os.path.join(pipeline_dir, fname)
                with open(path, "r") as f:
                    results.append(f.read())
        return results

    def read_all_connector_files(self) -> list[dict]:
        """Read all connector .py files from the repo.

        Returns list of dicts with keys: name, code, metadata (parsed from docstring header).
        """
        if not self._enabled:
            return []
        connector_dir = os.path.join(self.repo_path, "connectors")
        if not os.path.isdir(connector_dir):
            return []
        results = []
        for fname in sorted(os.listdir(connector_dir)):
            if fname.endswith(".py"):
                path = os.path.join(connector_dir, fname)
                with open(path, "r") as f:
                    content = f.read()
                meta = self._parse_connector_header(content)
                meta["code"] = self._strip_connector_header(content)
                meta["filename"] = fname
                results.append(meta)
        return results

    @staticmethod
    def _parse_connector_header(content: str) -> dict:
        """Parse the docstring header we write to connector files."""
        meta = {}
        if not content.startswith('"""'):
            return meta
        end = content.find('"""', 3)
        if end == -1:
            return meta
        header = content[3:end]
        for line in header.strip().split("\n"):
            if ":" in line:
                key, _, val = line.partition(":")
                key = key.strip().lower()
                val = val.strip()
                if key == "connector":
                    meta["name"] = val
                elif key == "type":
                    meta["connector_type"] = val
                elif key == "version":
                    try:
                        meta["version"] = int(val)
                    except ValueError:
                        meta["version"] = 1
                elif key == "id":
                    meta["connector_id"] = val
                elif key == "status":
                    meta["status"] = val
        return meta

    @staticmethod
    def _strip_connector_header(content: str) -> str:
        """Strip the docstring header, return just the code."""
        if not content.startswith('"""'):
            return content
        end = content.find('"""', 3)
        if end == -1:
            return content
        # Skip past the closing """ and any trailing newlines
        code_start = end + 3
        return content[code_start:].lstrip("\n")

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def commit_pipeline(
        self,
        pipeline: PipelineContract,
        yaml_content: str,
        message: str,
        author: str = "dapos",
    ) -> Optional[str]:
        """Write pipeline YAML and commit. Returns commit hash or None."""
        if not self._enabled:
            return None

        try:
            self._pull()

            safe_name = self._safe_name(pipeline.pipeline_name)
            path = os.path.join(self.repo_path, "pipelines", f"{safe_name}.yaml")
            with open(path, "w") as f:
                f.write(yaml_content)

            self._git("add", f"pipelines/{safe_name}.yaml")
            commit = self._commit(message, author)
            if commit:
                self._push()
            return commit

        except Exception as e:
            log.error("GitOps commit_pipeline failed: %s", e)
            return None

    def commit_connector(
        self,
        connector: ConnectorRecord,
        message: str,
        author: str = "dapos",
    ) -> Optional[str]:
        """Write connector code and commit. Returns commit hash or None."""
        if not self._enabled:
            return None

        try:
            self._pull()

            safe_name = self._safe_name(connector.connector_name)
            path = os.path.join(self.repo_path, "connectors", f"{safe_name}.py")

            # Write connector code with header
            with open(path, "w") as f:
                f.write(f'"""\nConnector: {connector.connector_name}\n')
                f.write(f"Type: {connector.connector_type}\n")
                f.write(f"Version: {connector.version}\n")
                f.write(f"ID: {connector.connector_id}\n")
                f.write(f"Status: {connector.status.value if hasattr(connector.status, 'value') else connector.status}\n")
                f.write(f'"""\n\n')
                f.write(connector.code or "# No code available\n")

            self._git("add", f"connectors/{safe_name}.py")
            commit = self._commit(message, author)
            if commit:
                self._push()
            return commit

        except Exception as e:
            log.error("GitOps commit_connector failed: %s", e)
            return None

    def commit_all(
        self,
        pipelines: list[tuple[PipelineContract, str]],
        connectors: list[ConnectorRecord],
        message: str,
        author: str = "dapos",
    ) -> Optional[str]:
        """Bulk write all pipelines and connectors, single commit."""
        if not self._enabled:
            return None

        try:
            self._pull()

            for pipeline, yaml_content in pipelines:
                safe_name = self._safe_name(pipeline.pipeline_name)
                path = os.path.join(self.repo_path, "pipelines", f"{safe_name}.yaml")
                with open(path, "w") as f:
                    f.write(yaml_content)
                self._git("add", f"pipelines/{safe_name}.yaml")

            for connector in connectors:
                safe_name = self._safe_name(connector.connector_name)
                path = os.path.join(self.repo_path, "connectors", f"{safe_name}.py")
                with open(path, "w") as f:
                    f.write(f'"""\nConnector: {connector.connector_name}\n')
                    f.write(f"Type: {connector.connector_type}\n")
                    f.write(f"Version: {connector.version}\n")
                    f.write(f'"""\n\n')
                    f.write(connector.code or "# No code available\n")
                self._git("add", f"connectors/{safe_name}.py")

            commit = self._commit(message, author)
            if commit:
                self._push()
            return commit

        except Exception as e:
            log.error("GitOps commit_all failed: %s", e)
            return None

    def delete_pipeline(
        self,
        pipeline_name: str,
        message: str,
        author: str = "dapos",
    ) -> Optional[str]:
        """Remove a pipeline YAML and commit."""
        if not self._enabled:
            return None

        try:
            self._pull()

            safe_name = self._safe_name(pipeline_name)
            path = os.path.join(self.repo_path, "pipelines", f"{safe_name}.yaml")
            if os.path.exists(path):
                os.remove(path)
                self._git("add", f"pipelines/{safe_name}.yaml")
                commit = self._commit(message, author)
                if commit:
                    self._push()
                return commit
            return None

        except Exception as e:
            log.error("GitOps delete_pipeline failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Read / status
    # ------------------------------------------------------------------

    def get_log(self, limit: int = 20) -> list[dict]:
        """Get recent commit log."""
        if not self._enabled:
            return []

        try:
            result = self._git(
                "log", f"--max-count={limit}",
                "--format=%H|%an|%ai|%s",
            )
            commits = []
            for line in result.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split("|", 3)
                if len(parts) == 4:
                    commits.append({
                        "hash": parts[0],
                        "author": parts[1],
                        "date": parts[2],
                        "message": parts[3],
                    })
            return commits
        except Exception:
            return []

    def get_file_at_commit(self, filepath: str, commit_hash: str) -> Optional[str]:
        """Read a file at a specific commit."""
        if not self._enabled:
            return None
        try:
            return self._git("show", f"{commit_hash}:{filepath}")
        except Exception:
            return None

    def get_diff(self, commit_a: str = "HEAD~1", commit_b: str = "HEAD") -> str:
        """Get diff between two commits."""
        if not self._enabled:
            return ""
        try:
            return self._git("diff", commit_a, commit_b)
        except Exception:
            return ""

    def get_pipeline_history(self, pipeline_name: str, limit: int = 20) -> list[dict]:
        """Get commit history for a specific pipeline file."""
        if not self._enabled:
            return []
        try:
            safe_name = self._safe_name(pipeline_name)
            result = self._git(
                "log", f"--max-count={limit}",
                "--format=%H|%an|%ai|%s",
                "--", f"pipelines/{safe_name}.yaml",
            )
            commits = []
            for line in result.strip().split("\n"):
                if not line.strip():
                    continue
                parts = line.split("|", 3)
                if len(parts) == 4:
                    commits.append({
                        "hash": parts[0],
                        "author": parts[1],
                        "date": parts[2],
                        "message": parts[3],
                    })
            return commits
        except Exception:
            return []

    def status(self) -> dict:
        """Return repo status summary."""
        if not self._enabled:
            return {"enabled": False}

        try:
            head = self._git("rev-parse", "--short", "HEAD").strip()
            branch = self._git("rev-parse", "--abbrev-ref", "HEAD").strip()
            pipeline_count = len([
                f for f in os.listdir(os.path.join(self.repo_path, "pipelines"))
                if f.endswith(".yaml")
            ]) if os.path.isdir(os.path.join(self.repo_path, "pipelines")) else 0
            connector_count = len([
                f for f in os.listdir(os.path.join(self.repo_path, "connectors"))
                if f.endswith(".py")
            ]) if os.path.isdir(os.path.join(self.repo_path, "connectors")) else 0

            info = {
                "enabled": True,
                "repo_path": self.repo_path,
                "branch": branch,
                "head": head,
                "pipeline_files": pipeline_count,
                "connector_files": connector_count,
                "remote": self.remote_url or None,
                "auto_push": self.auto_push,
                "auto_pull": self.auto_pull,
            }

            # Check if in sync with remote
            if self.has_remote:
                try:
                    self._git("fetch", "origin")
                    local = self._git("rev-parse", "HEAD").strip()
                    try:
                        remote = self._git("rev-parse", f"origin/{self.branch}").strip()
                        if local == remote:
                            info["sync_status"] = "in_sync"
                        else:
                            # Check direction
                            try:
                                self._git("merge-base", "--is-ancestor", local, f"origin/{self.branch}")
                                info["sync_status"] = "behind"
                            except RuntimeError:
                                try:
                                    self._git("merge-base", "--is-ancestor", f"origin/{self.branch}", local)
                                    info["sync_status"] = "ahead"
                                except RuntimeError:
                                    info["sync_status"] = "diverged"
                    except RuntimeError:
                        info["sync_status"] = "no_remote_branch"
                except RuntimeError:
                    info["sync_status"] = "fetch_failed"

            return info
        except Exception as e:
            return {"enabled": True, "error": str(e)}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _git(self, *args) -> str:
        """Run a git command in the repo directory."""
        result = subprocess.run(
            ["git"] + list(args),
            cwd=self.repo_path,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {args[0]} failed: {result.stderr.strip()}")
        return result.stdout

    def _git_raw(self, *args) -> str:
        """Run a git command without a repo directory (e.g. clone)."""
        result = subprocess.run(
            ["git"] + list(args),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {args[0]} failed: {result.stderr.strip()}")
        return result.stdout

    def _commit(self, message: str, author: str = "dapos") -> Optional[str]:
        """Commit staged changes. Returns commit hash or None if nothing to commit."""
        # Check if there are staged changes
        status = self._git("status", "--porcelain")
        if not status.strip():
            return None  # Nothing to commit

        self._git(
            "commit",
            "-m", message,
            f"--author={author} <{author}@dapos>",
        )
        commit_hash = self._git("rev-parse", "HEAD").strip()
        log.info("GitOps commit: %s — %s", commit_hash[:8], message)
        return commit_hash

    @staticmethod
    def _safe_name(name: str) -> str:
        """Convert a pipeline/connector name to a safe filename."""
        return name.replace("/", "_").replace(" ", "_").replace(".", "_")
