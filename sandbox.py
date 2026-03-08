"""Connector code validation and sandboxed execution."""

import ast
import builtins as _builtins
import logging
from typing import Tuple, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowed imports for generated connectors
# ---------------------------------------------------------------------------
ALLOWED_IMPORTS = {
    # Python internals needed by connectors
    "__future__",
    # Pipeline-agent interfaces (connectors must subclass these)
    "source.base", "source",
    "target.base", "target",
    "contracts.models", "contracts",
    # Database drivers
    "pymysql", "pymysql.cursors",
    "psycopg2", "psycopg2.extras", "psycopg2.sql",
    "cx_Oracle", "pyodbc", "sqlite3",
    "asyncpg", "aiomysql", "aiopg",
    # HTTP / API clients
    "httpx", "requests", "urllib.parse", "urllib.request",
    # Data handling
    "csv", "json", "io", "gzip", "zipfile",
    # Standard lib utilities
    "os", "os.path", "pathlib", "datetime", "decimal", "uuid",
    "hashlib", "base64", "re", "math", "time",
    "dataclasses", "typing", "abc", "inspect", "logging",
    "collections", "functools", "itertools",
    # Cloud SDKs
    "boto3", "botocore",
    "google.cloud.bigquery", "google.cloud.storage",
    "azure.storage.blob",
}

# ---------------------------------------------------------------------------
# Blocked patterns
# ---------------------------------------------------------------------------
BLOCKED_CALLS = {
    "eval", "exec", "compile", "__import__",
    "globals", "locals", "vars",
    "breakpoint", "exit", "quit",
}

BLOCKED_MODULES = {
    "subprocess", "shutil", "signal", "ctypes",
    "multiprocessing", "threading",
    "socket",
    "code", "codeop", "compileall",
    "importlib", "runpy", "pkgutil",
}


# ---------------------------------------------------------------------------
# AST validator
# ---------------------------------------------------------------------------
class _ImportValidator(ast.NodeVisitor):
    def __init__(self):
        self.errors: list[str] = []

    def visit_Import(self, node):
        for alias in node.names:
            self._check_module(alias.name, node.lineno)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module:
            self._check_module(node.module, node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name) and node.func.id in BLOCKED_CALLS:
            self.errors.append(
                f"Line {node.lineno}: blocked call '{node.func.id}()'"
            )
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in BLOCKED_CALLS:
                self.errors.append(
                    f"Line {node.lineno}: blocked call '.{node.func.attr}()'"
                )
            # Block os.system, os.popen, os.exec*, etc.
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "os":
                dangerous = {
                    "system", "popen", "execv", "execve", "execvp",
                    "spawn", "spawnl", "fork", "kill", "remove",
                    "unlink", "rmdir", "rename",
                }
                if node.func.attr in dangerous:
                    self.errors.append(
                        f"Line {node.lineno}: blocked os.{node.func.attr}()"
                    )
        self.generic_visit(node)

    def _check_module(self, module: str, lineno: int):
        root = module.split(".")[0]
        if root in BLOCKED_MODULES and not self._is_allowed(module):
            self.errors.append(f"Line {lineno}: blocked import '{module}'")
            return
        if not self._is_allowed(module):
            self.errors.append(
                f"Line {lineno}: import '{module}' not in allowlist"
            )

    def _is_allowed(self, module: str) -> bool:
        if module in ALLOWED_IMPORTS:
            return True
        parts = module.split(".")
        for i in range(len(parts)):
            if ".".join(parts[: i + 1]) in ALLOWED_IMPORTS:
                return True
        return False


def validate_connector_code(code: str) -> Tuple[bool, str]:
    """Validate connector code via AST analysis. Returns (valid, error_message)."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return False, f"Syntax error: {e}"

    validator = _ImportValidator()
    validator.visit(tree)

    if validator.errors:
        return False, "; ".join(validator.errors)

    return True, ""


def safe_exec(code: str, extra_globals: Optional[dict] = None) -> dict:
    """Execute connector code with restricted builtins and import whitelist."""
    safe_builtins = {}
    for name in dir(_builtins):
        if name.startswith("_") and name != "__build_class__":
            continue
        if name in BLOCKED_CALLS:
            continue
        safe_builtins[name] = getattr(_builtins, name)

    safe_builtins["__build_class__"] = _builtins.__build_class__
    safe_builtins["__name__"] = "__connector__"

    original_import = _builtins.__import__

    def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
        root = name.split(".")[0]
        if root in BLOCKED_MODULES and not _is_in_allowlist(name):
            raise ImportError(f"Import '{name}' is not allowed in connectors")
        if not _is_in_allowlist(name):
            raise ImportError(
                f"Import '{name}' is not in the connector allowlist"
            )
        return original_import(name, globals, locals, fromlist, level)

    safe_builtins["__import__"] = _safe_import

    namespace = {"__builtins__": safe_builtins}
    if extra_globals:
        namespace.update(extra_globals)

    exec(code, namespace)  # noqa: S102 — sandboxed via restricted builtins
    return namespace


def _is_in_allowlist(module: str) -> bool:
    if module in ALLOWED_IMPORTS:
        return True
    parts = module.split(".")
    for i in range(len(parts)):
        if ".".join(parts[: i + 1]) in ALLOWED_IMPORTS:
            return True
    return False
