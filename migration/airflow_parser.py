"""
Airflow DAG Python AST parser for migration to DAPOS.

Parses Airflow DAG files using static analysis (ast module) to extract:
- DAG definitions (dag_id, schedule, default_args)
- Task instances (operator, task_id, sql, dependencies)
- Connection references
- Variable references
- Import statements

No exec() — all analysis is read-only via Python's ast module.
"""

import ast
import io
import logging
import os
import tarfile
import tempfile
import zipfile
from dataclasses import asdict

from contracts.models import ParsedAirflowDag

log = logging.getLogger(__name__)

# ── Operator-to-DAPOS mapping table ──

OPERATOR_MAP = {
    # SQL operators → SQL Transform
    "PostgresOperator": {"dapos_type": "transform", "subtype": "sql"},
    "MySqlOperator": {"dapos_type": "transform", "subtype": "sql"},
    "BigQueryInsertJobOperator": {"dapos_type": "transform", "subtype": "sql"},
    "BigQueryOperator": {"dapos_type": "transform", "subtype": "sql"},
    "SnowflakeOperator": {"dapos_type": "transform", "subtype": "sql"},
    "RedshiftSQLOperator": {"dapos_type": "transform", "subtype": "sql"},
    "SQLExecuteQueryOperator": {"dapos_type": "transform", "subtype": "sql"},
    "TrinoOperator": {"dapos_type": "transform", "subtype": "sql"},

    # Transfer operators → Extract + Load pipeline
    "S3ToRedshiftOperator": {"dapos_type": "pipeline", "source": "s3", "target": "redshift"},
    "GCSToBigQueryOperator": {"dapos_type": "pipeline", "source": "gcs", "target": "bigquery"},
    "S3ToGCSOperator": {"dapos_type": "pipeline", "source": "s3", "target": "gcs"},
    "PostgresToGCSOperator": {"dapos_type": "pipeline", "source": "postgresql", "target": "gcs"},
    "MySQLToGCSOperator": {"dapos_type": "pipeline", "source": "mysql", "target": "gcs"},
    "GenericTransfer": {"dapos_type": "pipeline", "source": "generic", "target": "generic"},
    "SqlToSlackOperator": {"dapos_type": "hook", "subtype": "notification"},

    # Sensors → Sensor step
    "ExternalTaskSensor": {"dapos_type": "sensor", "subtype": "external_task"},
    "S3KeySensor": {"dapos_type": "sensor", "subtype": "s3"},
    "GCSObjectExistenceSensor": {"dapos_type": "sensor", "subtype": "gcs"},
    "HttpSensor": {"dapos_type": "sensor", "subtype": "http"},
    "SqlSensor": {"dapos_type": "sensor", "subtype": "sql"},
    "FileSensor": {"dapos_type": "sensor", "subtype": "file"},
    "TimeDeltaSensor": {"dapos_type": "sensor", "subtype": "time"},
    "TimeDeltaSensorAsync": {"dapos_type": "sensor", "subtype": "time"},

    # Notification/hook operators
    "EmailOperator": {"dapos_type": "hook", "subtype": "email"},
    "SlackWebhookOperator": {"dapos_type": "hook", "subtype": "slack"},
    "SlackAPIPostOperator": {"dapos_type": "hook", "subtype": "slack"},
    "PagerdutyAlertOperator": {"dapos_type": "hook", "subtype": "pagerduty"},

    # Python/Bash → Custom step (needs manual review)
    "PythonOperator": {"dapos_type": "custom", "subtype": "python", "needs_review": True},
    "BashOperator": {"dapos_type": "custom", "subtype": "bash", "needs_review": True},
    "PythonVirtualenvOperator": {"dapos_type": "custom", "subtype": "python", "needs_review": True},
    "ShortCircuitOperator": {"dapos_type": "custom", "subtype": "branch", "needs_review": True},
    "BranchPythonOperator": {"dapos_type": "custom", "subtype": "branch", "needs_review": True},
    "BranchSQLOperator": {"dapos_type": "custom", "subtype": "branch", "needs_review": True},

    # External compute → unsupported (flagged)
    "SparkSubmitOperator": {"dapos_type": "unsupported", "reason": "Spark compute"},
    "KubernetesPodOperator": {"dapos_type": "unsupported", "reason": "Kubernetes"},
    "DockerOperator": {"dapos_type": "unsupported", "reason": "Docker"},
    "EmrAddStepsOperator": {"dapos_type": "unsupported", "reason": "EMR"},
    "DataprocSubmitJobOperator": {"dapos_type": "unsupported", "reason": "Dataproc"},

    # Dummy/Empty → omit (structural only)
    "DummyOperator": {"dapos_type": "omit"},
    "EmptyOperator": {"dapos_type": "omit"},
}


def parse_archive(file_bytes: bytes, filename: str) -> tuple[list[dict], list[dict]]:
    """Parse an uploaded zip/tar archive of Airflow DAGs.

    Returns (parsed_dags_as_dicts, parse_errors).
    """
    parsed_dags = []
    parse_errors = []

    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, filename)
        with open(archive_path, "wb") as f:
            f.write(file_bytes)

        extract_dir = os.path.join(tmpdir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        # Extract archive
        try:
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, "r") as zf:
                    zf.extractall(extract_dir)
            elif tarfile.is_tarfile(archive_path):
                with tarfile.open(archive_path, "r:*") as tf:
                    tf.extractall(extract_dir, filter="data")
            else:
                return [], [{"file": filename, "error": "Unsupported archive format. Use .zip or .tar.gz"}]
        except Exception as e:
            return [], [{"file": filename, "error": f"Failed to extract archive: {e}"}]

        # Walk for Python files
        for root, _dirs, files in os.walk(extract_dir):
            for fname in sorted(files):
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, extract_dir)

                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as pf:
                        source_code = pf.read()
                    dags = parse_dag_file(source_code, rel_path)
                    for d in dags:
                        parsed_dags.append(asdict(d))
                except Exception as e:
                    parse_errors.append({"file": rel_path, "error": str(e)})

    return parsed_dags, parse_errors


def parse_dag_file(source_code: str, file_path: str = "") -> list[ParsedAirflowDag]:
    """Parse a single Python file and extract Airflow DAG definitions."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        return [ParsedAirflowDag(
            file_path=file_path,
            parse_warnings=[f"Syntax error: {e}"],
            raw_code=source_code[:2000],
        )]

    visitor = AirflowDagVisitor(source_code, file_path)
    visitor.run(tree)
    return visitor.dags


class AirflowDagVisitor(ast.NodeVisitor):
    """AST visitor that extracts Airflow DAG definitions."""

    def __init__(self, source_code: str, file_path: str):
        self.source = source_code
        self.file_path = file_path
        self.dags: list[ParsedAirflowDag] = []
        self.imports: list[str] = []
        self.variables_ref: list[str] = []
        self.connections_ref: list[str] = []
        self.tasks: dict[str, dict] = {}  # var_name -> task info
        self.dag_vars: dict[str, dict] = {}  # var_name -> dag info
        self.dependencies: list[tuple[str, str]] = []  # (upstream, downstream)
        self.current_dag_id: str = ""

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            self.imports.append(alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ""
        for alias in node.names:
            self.imports.append(f"{module}.{alias.name}")
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        """Capture DAG assignments and task assignments."""
        if isinstance(node.value, ast.Call):
            func_name = self._get_call_name(node.value)
            target_name = self._get_target_name(node.targets)

            if func_name == "DAG":
                dag_info = self._extract_dag_params(node.value)
                if target_name:
                    self.dag_vars[target_name] = dag_info
                if dag_info.get("dag_id"):
                    self.current_dag_id = dag_info["dag_id"]

            elif func_name in OPERATOR_MAP or func_name.endswith("Operator") or func_name.endswith("Sensor"):
                task_info = self._extract_task_params(node.value, func_name)
                if target_name:
                    self.tasks[target_name] = task_info

        self.generic_visit(node)

    def visit_With(self, node: ast.With):
        """Capture `with DAG(...) as dag:` context managers."""
        for item in node.items:
            if isinstance(item.context_expr, ast.Call):
                func_name = self._get_call_name(item.context_expr)
                if func_name == "DAG":
                    dag_info = self._extract_dag_params(item.context_expr)
                    if item.optional_vars and isinstance(item.optional_vars, ast.Name):
                        self.dag_vars[item.optional_vars.id] = dag_info
                    if dag_info.get("dag_id"):
                        self.current_dag_id = dag_info["dag_id"]
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        """Capture dependency chains: task1 >> task2 >> task3."""
        if isinstance(node.value, ast.BinOp):
            deps = self._extract_shift_chain(node.value)
            self.dependencies.extend(deps)

        # Capture set_downstream / set_upstream calls
        if isinstance(node.value, ast.Call):
            self._check_dependency_call(node.value)

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        """Capture Variable.get() and connection references."""
        name = self._get_call_name(node)
        if name == "Variable.get" and node.args:
            var_name = self._get_str_value(node.args[0])
            if var_name:
                self.variables_ref.append(var_name)
        self.generic_visit(node)

    def _finalize(self):
        """Build ParsedAirflowDag objects from collected data."""
        # If we have dag definitions, create one per DAG
        if self.dag_vars:
            for var_name, dag_info in self.dag_vars.items():
                dag = ParsedAirflowDag(
                    dag_id=dag_info.get("dag_id", var_name),
                    file_path=self.file_path,
                    schedule_interval=dag_info.get("schedule_interval", ""),
                    default_args=dag_info.get("default_args", {}),
                    tasks=list(self.tasks.values()),
                    connections_referenced=list(set(self.connections_ref)),
                    variables_referenced=list(set(self.variables_ref)),
                    python_imports=self.imports,
                    raw_code=self.source[:4000],
                    parse_warnings=[],
                )
                # Attach dependencies
                for task in dag.tasks:
                    task_deps = [
                        up for up, down in self.dependencies
                        if down == task.get("task_id", "")
                    ]
                    task["depends_on"] = task_deps
                self.dags.append(dag)
        elif self.tasks:
            # Tasks found but no explicit DAG definition — infer
            dag = ParsedAirflowDag(
                dag_id=os.path.splitext(os.path.basename(self.file_path))[0],
                file_path=self.file_path,
                schedule_interval="",
                tasks=list(self.tasks.values()),
                connections_referenced=list(set(self.connections_ref)),
                variables_referenced=list(set(self.variables_ref)),
                python_imports=self.imports,
                raw_code=self.source[:4000],
                parse_warnings=["No explicit DAG definition found; inferred from file name"],
            )
            for task in dag.tasks:
                task_deps = [
                    up for up, down in self.dependencies
                    if down == task.get("task_id", "")
                ]
                task["depends_on"] = task_deps
            self.dags.append(dag)

    def run(self, node):
        """Visit the full AST tree and then finalize."""
        super().visit(node)
        self._finalize()
        return self.dags

    # ── Helpers ──

    def _get_call_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Name):
            return node.func.id
        if isinstance(node.func, ast.Attribute):
            parts = []
            obj = node.func
            while isinstance(obj, ast.Attribute):
                parts.append(obj.attr)
                obj = obj.value
            if isinstance(obj, ast.Name):
                parts.append(obj.id)
            return ".".join(reversed(parts))
        return ""

    def _get_target_name(self, targets: list) -> str:
        if targets and isinstance(targets[0], ast.Name):
            return targets[0].id
        return ""

    def _get_str_value(self, node) -> str:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return ""

    def _get_keyword_value(self, keywords: list, key: str):
        for kw in keywords:
            if kw.arg == key:
                return kw.value
        return None

    def _extract_dag_params(self, call: ast.Call) -> dict:
        info = {}
        # First positional arg is dag_id
        if call.args:
            info["dag_id"] = self._get_str_value(call.args[0])

        # Keyword args
        for kw in call.keywords:
            if kw.arg == "dag_id":
                info["dag_id"] = self._get_str_value(kw.value)
            elif kw.arg in ("schedule_interval", "schedule"):
                val = self._get_str_value(kw.value)
                if not val and isinstance(kw.value, ast.Attribute):
                    val = f"@{kw.value.attr}" if hasattr(kw.value, "attr") else ""
                info["schedule_interval"] = val
            elif kw.arg == "default_args":
                info["default_args"] = self._extract_dict(kw.value)

        return info

    def _extract_task_params(self, call: ast.Call, operator_class: str) -> dict:
        task = {
            "task_id": "",
            "operator_class": operator_class,
            "sql": "",
            "python_callable": "",
            "bash_command": "",
            "params": {},
            "connection_id": "",
            "depends_on": [],
        }

        mapping = OPERATOR_MAP.get(operator_class, {})
        task["dapos_mapping"] = mapping

        for kw in call.keywords:
            if kw.arg == "task_id":
                task["task_id"] = self._get_str_value(kw.value)
            elif kw.arg == "sql":
                task["sql"] = self._get_str_value(kw.value)
            elif kw.arg == "python_callable":
                if isinstance(kw.value, ast.Name):
                    task["python_callable"] = kw.value.id
            elif kw.arg == "bash_command":
                task["bash_command"] = self._get_str_value(kw.value)
            elif kw.arg in ("conn_id", "postgres_conn_id", "mysql_conn_id",
                            "bigquery_conn_id", "snowflake_conn_id",
                            "redshift_conn_id", "gcp_conn_id", "aws_conn_id"):
                conn = self._get_str_value(kw.value)
                if conn:
                    task["connection_id"] = conn
                    self.connections_ref.append(conn)
            elif kw.arg == "op_kwargs":
                task["params"] = self._extract_dict(kw.value)
            elif kw.arg == "dag":
                pass  # skip dag assignment
            elif kw.arg in ("trigger_rule", "retries", "retry_delay", "pool",
                            "queue", "weight_rule", "priority_weight"):
                pass  # Airflow execution params — not relevant to DAPOS

        return task

    def _extract_shift_chain(self, node: ast.BinOp) -> list[tuple[str, str]]:
        """Extract task1 >> task2 >> task3 dependency chains."""
        deps = []
        if isinstance(node.op, (ast.RShift, ast.LShift)):
            left_names = self._extract_chain_names(node.left)
            right_names = self._extract_chain_names(node.right)

            if isinstance(node.op, ast.RShift):
                # left >> right: left is upstream of right
                for l in left_names:
                    for r in right_names:
                        deps.append((self._resolve_task_id(l), self._resolve_task_id(r)))
            else:
                # left << right: right is upstream of left
                for l in left_names:
                    for r in right_names:
                        deps.append((self._resolve_task_id(r), self._resolve_task_id(l)))

            # Recurse into left side for chained expressions
            if isinstance(node.left, ast.BinOp):
                deps.extend(self._extract_shift_chain(node.left))

        return deps

    def _extract_chain_names(self, node) -> list[str]:
        """Get variable names from a shift chain node (handles lists)."""
        if isinstance(node, ast.Name):
            return [node.id]
        if isinstance(node, ast.BinOp):
            # For chained: a >> b >> c, the rightmost is in node.right
            return self._extract_chain_names(node.right)
        if isinstance(node, (ast.List, ast.Tuple)):
            names = []
            for elt in node.elts:
                names.extend(self._extract_chain_names(elt))
            return names
        return []

    def _resolve_task_id(self, var_name: str) -> str:
        """Resolve a variable name to its task_id."""
        task = self.tasks.get(var_name)
        if task and task.get("task_id"):
            return task["task_id"]
        return var_name

    def _check_dependency_call(self, call: ast.Call):
        """Handle task.set_downstream(other) / task.set_upstream(other)."""
        if isinstance(call.func, ast.Attribute):
            if call.func.attr in ("set_downstream", "set_upstream"):
                obj_name = ""
                if isinstance(call.func.value, ast.Name):
                    obj_name = call.func.value.id
                if call.args:
                    arg_names = self._extract_chain_names(call.args[0])
                    for arg in arg_names:
                        if call.func.attr == "set_downstream":
                            self.dependencies.append((
                                self._resolve_task_id(obj_name),
                                self._resolve_task_id(arg),
                            ))
                        else:
                            self.dependencies.append((
                                self._resolve_task_id(arg),
                                self._resolve_task_id(obj_name),
                            ))

    def _extract_dict(self, node) -> dict:
        """Best-effort extraction of a dict literal from AST."""
        if not isinstance(node, ast.Dict):
            return {}
        result = {}
        for k, v in zip(node.keys, node.values):
            key = self._get_str_value(k) if k else ""
            if not key:
                continue
            if isinstance(v, ast.Constant):
                result[key] = v.value
            elif isinstance(v, ast.Name):
                result[key] = f"<{v.id}>"
            else:
                result[key] = "<complex>"
        return result
