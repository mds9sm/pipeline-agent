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
import re
import tarfile
import tempfile
import zipfile
from dataclasses import asdict

import yaml

from contracts.models import ParsedAirflowDag

log = logging.getLogger(__name__)

# ── Airflow Jinja → DAPOS template variable conversion ──

# Maps Airflow Jinja template variables to DAPOS equivalents.
# Airflow uses {{ var }} syntax in SQL, bash_command, and operator params.
# DAPOS uses {{var}} syntax in template variables.
JINJA_MAP = {
    # Date/time variables
    "ds": "run_date",
    "ds_nodash": "run_date_nodash",
    "ts": "run_timestamp",
    "ts_nodash": "run_timestamp_nodash",
    "execution_date": "watermark_after",
    "next_ds": "next_run_date",
    "next_ds_nodash": "next_run_date_nodash",
    "prev_ds": "prev_run_date",
    "prev_ds_nodash": "prev_run_date_nodash",
    "next_execution_date": "next_watermark",
    "prev_execution_date": "prev_watermark",
    "data_interval_start": "watermark_before",
    "data_interval_end": "watermark_after",
    "logical_date": "watermark_after",

    # Run metadata
    "run_id": "run_id",
    "dag_run.run_id": "run_id",
    "dag.dag_id": "pipeline_name",
    "task.task_id": "step_name",
    "task_instance_key_str": "run_id",

    # Connection/config
    "var.value.": "var_",  # Variable.get() → DAPOS config var (prefix match)
    "var.json.": "var_",   # Variable.get() JSON → DAPOS config var (prefix match)
}

# Airflow macros that need function-level conversion
MACRO_MAP = {
    "macros.ds_add": "date_add",      # {{ macros.ds_add(ds, 7) }}
    "macros.ds_format": "date_format", # {{ macros.ds_format(ds, ...) }}
    "macros.datetime": "datetime",
    "macros.timedelta": "timedelta",
    "macros.random": "random",
}


def convert_jinja_templates(text: str) -> tuple[str, list[str]]:
    """Convert Airflow Jinja templates to DAPOS template variables.

    Returns (converted_text, list_of_warnings).
    Handles: {{ ds }}, {{ params.x }}, {{ var.value.key }},
             {{ macros.ds_add(ds, 7) }}, {% if %} blocks, etc.
    """
    if not text or "{{" not in text and "{%" not in text:
        return text, []

    warnings = []
    converted = text

    # 1. Convert simple variable references: {{ ds }}, {{ execution_date }}, etc.
    def replace_simple_var(m):
        var_name = m.group(1).strip()

        # Direct mapping
        if var_name in JINJA_MAP:
            return "{{" + JINJA_MAP[var_name] + "}}"

        # params.X → pipeline config variable
        if var_name.startswith("params."):
            param_key = var_name[len("params."):]
            return "{{var_" + param_key + "}}"

        # var.value.X / var.json.X → DAPOS config variable
        if var_name.startswith("var.value.") or var_name.startswith("var.json."):
            key = var_name.split(".", 2)[-1]
            return "{{var_" + key + "}}"

        # conf.X → pipeline config
        if var_name.startswith("conf."):
            return "{{var_" + var_name[5:] + "}}"

        # task_instance / ti references
        if var_name.startswith("ti.") or var_name.startswith("task_instance."):
            warnings.append(f"TaskInstance reference '{var_name}' → use DAPOS run context")
            return "{{" + var_name + "}}"  # preserve as-is with warning

        # Macro calls — convert known macros
        for macro, dapos_fn in MACRO_MAP.items():
            if var_name.startswith(macro):
                warnings.append(f"Macro '{macro}' converted to DAPOS function — verify syntax")
                return "{{" + var_name.replace(macro, dapos_fn, 1) + "}}"

        # Unknown — preserve and warn
        warnings.append(f"Unknown Jinja variable '{var_name}' — needs manual mapping")
        return "{{" + var_name + "}}"

    # Match {{ ... }} (Jinja expression blocks)
    converted = re.sub(r"\{\{\s*(.+?)\s*\}\}", replace_simple_var, converted)

    # 2. Convert Jinja control blocks: {% if %}, {% for %}, {% set %}
    # These don't have DAPOS equivalents — convert to SQL CASE/comments
    if "{%" in converted:
        # {% if condition %} ... {% endif %} → SQL comment with warning
        warnings.append("Jinja control blocks ({% if/for/set %}) found — converted to comments, needs manual review")
        converted = re.sub(r"\{%[-\s]*if\s+(.+?)\s*[-]?%\}", r"/* IF \1 */", converted)
        converted = re.sub(r"\{%[-\s]*elif\s+(.+?)\s*[-]?%\}", r"/* ELIF \1 */", converted)
        converted = re.sub(r"\{%[-\s]*else\s*[-]?%\}", "/* ELSE */", converted)
        converted = re.sub(r"\{%[-\s]*endif\s*[-]?%\}", "/* ENDIF */", converted)
        converted = re.sub(r"\{%[-\s]*for\s+(.+?)\s*[-]?%\}", r"/* FOR \1 */", converted)
        converted = re.sub(r"\{%[-\s]*endfor\s*[-]?%\}", "/* ENDFOR */", converted)
        converted = re.sub(r"\{%[-\s]*set\s+(.+?)\s*[-]?%\}", r"/* SET \1 */", converted)

    return converted, warnings


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

    # Python/Bash → Custom step (converted with Airflow→DAPOS code translation)
    "PythonOperator": {"dapos_type": "custom", "subtype": "python", "convertible": True},
    "BashOperator": {"dapos_type": "custom", "subtype": "bash", "convertible": True},
    "PythonVirtualenvOperator": {"dapos_type": "custom", "subtype": "python", "convertible": True},
    "ShortCircuitOperator": {"dapos_type": "custom", "subtype": "branch", "convertible": True},
    "BranchPythonOperator": {"dapos_type": "custom", "subtype": "branch", "convertible": True},
    "BranchSQLOperator": {"dapos_type": "custom", "subtype": "branch", "convertible": True},

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


def scan_archive(file_bytes: bytes, filename: str) -> tuple[str, dict]:
    """Extract an archive and produce a structural summary for agent analysis.

    Returns (extract_dir_path, scan_result) where scan_result contains:
      - file_tree: list of relative paths
      - file_counts: {ext: count}
      - samples: {rel_path: first_lines} for interesting files
      - config_files: list of YAML/JSON config file paths + contents
      - sql_files: list of SQL file paths + contents
      - python_files: list of Python file paths
      - readme: README content if found
    """
    tmpdir = tempfile.mkdtemp()
    archive_path = os.path.join(tmpdir, filename)
    with open(archive_path, "wb") as f:
        f.write(file_bytes)

    extract_dir = os.path.join(tmpdir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)

    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
    elif tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path, "r:*") as tf:
            tf.extractall(extract_dir, filter="data")
    else:
        return extract_dir, {"error": "Unsupported archive format"}

    file_tree = []
    file_counts = {}
    python_files = []
    config_files = []
    sql_files = []
    samples = {}
    readme_content = ""

    for root, _dirs, files in os.walk(extract_dir):
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            rel_path = os.path.relpath(fpath, extract_dir)
            file_tree.append(rel_path)

            ext = os.path.splitext(fname)[1].lower()
            file_counts[ext] = file_counts.get(ext, 0) + 1

            if fname.endswith(".py"):
                python_files.append(rel_path)
            elif fname.endswith((".yaml", ".yml")):
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as cf:
                        content = cf.read()
                    config_files.append({"path": rel_path, "content": content[:5000]})
                except Exception:
                    config_files.append({"path": rel_path, "content": ""})
            elif fname.endswith((".json",)) and "conf" in rel_path.lower():
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as cf:
                        content = cf.read()
                    config_files.append({"path": rel_path, "content": content[:5000]})
                except Exception:
                    pass
            elif fname.endswith(".sql"):
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as sf:
                        content = sf.read()
                    sql_files.append({"path": rel_path, "content": content[:3000]})
                except Exception:
                    sql_files.append({"path": rel_path, "content": ""})

            if fname.lower() in ("readme.md", "readme.txt", "readme"):
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as rf:
                        readme_content = rf.read()[:3000]
                except Exception:
                    pass

            # Sample first 5 lines of interesting files (templates, factories)
            if ext in (".py", ".yaml", ".yml") and len(samples) < 30:
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as sf:
                        lines = sf.readlines()[:10]
                        samples[rel_path] = "".join(lines)
                except Exception:
                    pass

    return extract_dir, {
        "file_tree": file_tree[:500],  # Cap for very large repos
        "file_counts": file_counts,
        "python_files": python_files,
        "config_files": config_files[:100],
        "sql_files": sql_files[:200],
        "samples": samples,
        "readme": readme_content,
        "total_files": len(file_tree),
    }


def parse_archive(file_bytes: bytes, filename: str) -> tuple[list[dict], list[dict], dict]:
    """Parse an uploaded zip/tar archive of Airflow DAGs.

    Returns (parsed_dags_as_dicts, parse_errors, repo_scan).
    The repo_scan is the structural summary for agent-driven analysis of
    non-standard DAG patterns (YAML templates, config factories, etc.).
    """
    parsed_dags = []
    parse_errors = []

    try:
        extract_dir, repo_scan = scan_archive(file_bytes, filename)
    except Exception as e:
        return [], [{"file": filename, "error": f"Failed to extract archive: {e}"}], {}

    if "error" in repo_scan:
        return [], [{"file": filename, "error": repo_scan["error"]}], repo_scan

    # Phase 1: Standard Python AST parsing (universal — works for any Airflow repo)
    for rel_path in repo_scan.get("python_files", []):
        fpath = os.path.join(extract_dir, rel_path)
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as pf:
                source_code = pf.read()
            dags = parse_dag_file(source_code, rel_path)
            for d in dags:
                parsed_dags.append(asdict(d))
        except Exception as e:
            parse_errors.append({"file": rel_path, "error": str(e)})

    # Phase 2: Config-driven DAG parsing
    # Try YAML configs that look like DAG definitions (schedule_interval, steps, conn_id)
    for cfg in repo_scan.get("config_files", []):
        content = cfg.get("content", "")
        rel_path = cfg.get("path", "")
        if not content:
            continue
        # Quick heuristic: does this config define DAG-like structures?
        if any(kw in content for kw in ("schedule_interval", "steps:", "conn_id", "downstream_dags", "sql:")):
            try:
                dags = parse_yaml_template_dag(content, rel_path, extract_dir)
                for d in dags:
                    parsed_dags.append(asdict(d))
                    log.info("Parsed config-driven DAG: %s from %s", d.dag_id, rel_path)
            except Exception as e:
                parse_errors.append({"file": rel_path, "error": str(e)})

    # Cleanup extract dir (best effort)
    try:
        import shutil
        shutil.rmtree(os.path.dirname(extract_dir), ignore_errors=True)
    except Exception:
        pass

    return parsed_dags, parse_errors, repo_scan


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


def parse_yaml_template_dag(yaml_content: str, yaml_path: str, extract_dir: str) -> list[ParsedAirflowDag]:
    """Parse YAML-configured Airflow template DAGs (TransformDAGTemplate pattern).

    These are common in enterprise Airflow repos where DAGs are defined by
    YAML config + SQL files rather than Python code. Pattern:
      conf/{name}.yaml → defines steps, schedule, params
      sql/{name}/*.sql → SQL for each step

    Returns a list of ParsedAirflowDag, one per domain key in the YAML.
    """
    try:
        config = yaml.safe_load(yaml_content)
    except Exception as e:
        return [ParsedAirflowDag(
            file_path=yaml_path,
            parse_warnings=[f"YAML parse error: {e}"],
            raw_code=yaml_content[:2000],
        )]

    if not isinstance(config, dict):
        return []

    dags = []
    yaml_dir = os.path.dirname(yaml_path)
    yaml_basename = os.path.splitext(os.path.basename(yaml_path))[0]

    # Detect template type from directory structure
    is_transform = "transform" in yaml_dir.lower()
    is_domo = "domo" in yaml_dir.lower()
    is_dq = "dq_sync" in yaml_dir.lower() or "dq" in yaml_dir.lower()

    # Each top-level key in the YAML is typically a domain/DAG definition
    for domain_key, domain_config in config.items():
        if not isinstance(domain_config, dict):
            continue

        # Determine DAG ID pattern based on template type
        if is_transform:
            dag_id = f"TRANSFORM_DAG__{yaml_basename}__{domain_key}"
        elif is_domo:
            dag_id = f"DOMO_REFRESH_DAG__{yaml_basename}__{domain_key}"
        elif is_dq:
            dag_id = f"dq_sync__{yaml_basename}__{domain_key}"
        else:
            dag_id = f"{yaml_basename}__{domain_key}"

        schedule = domain_config.get("schedule_interval", "")
        connections = set()
        variables = []
        tasks = []
        parse_warnings = []

        # Extract environment params
        default_params = domain_config.get("default_params", {})
        env_params = {}
        for env_name, params in default_params.items():
            if isinstance(params, dict):
                env_params.update(params)
                # Track connection references from params
                for k, v in params.items():
                    if "conn" in k.lower():
                        connections.add(str(v))

        # Parse steps (CREATE_TABLE, TRANSFORM_DATA, LOAD_DATA, etc.)
        steps = domain_config.get("steps", {})
        if isinstance(steps, dict):
            for step_name, step_config in steps.items():
                if not isinstance(step_config, dict):
                    continue

                conn_id = step_config.get("conn_id", "")
                if conn_id:
                    connections.add(conn_id)

                sql_ref = step_config.get("sql", "")
                sql_content = ""

                # Try to read the referenced SQL file
                if sql_ref and extract_dir:
                    # SQL refs are relative to the dags/ directory
                    # Try multiple resolution paths
                    candidates = [
                        os.path.join(extract_dir, sql_ref),
                        os.path.join(extract_dir, "dags", sql_ref),
                        os.path.join(extract_dir, "dags", "transform", sql_ref),
                        os.path.join(os.path.dirname(os.path.join(extract_dir, yaml_path)), sql_ref),
                    ]
                    for candidate in candidates:
                        if os.path.isfile(candidate):
                            try:
                                with open(candidate, "r", encoding="utf-8", errors="replace") as sf:
                                    sql_content = sf.read()
                            except Exception:
                                pass
                            break

                # Convert Jinja templates in SQL
                jinja_warnings = []
                if sql_content:
                    sql_content, jinja_warnings = convert_jinja_templates(sql_content)

                is_batch = step_config.get("batch_processing", False)
                task = {
                    "task_id": step_name,
                    "operator_class": "SQLExecuteQueryOperator",  # Template DAGs use SQL operators
                    "sql": sql_content[:3000] if sql_content else f"-- SQL file: {sql_ref}",
                    "sql_file_ref": sql_ref,
                    "python_callable": "",
                    "bash_command": "",
                    "params": env_params,
                    "connection_id": conn_id,
                    "depends_on": [],
                    "dapos_mapping": {"dapos_type": "transform", "subtype": "sql"},
                    "batch_processing": is_batch,
                }
                if jinja_warnings:
                    task["jinja_warnings"] = jinja_warnings
                if is_batch:
                    task["dapos_mapping"]["batch"] = True
                    parse_warnings.append(f"Step '{step_name}' uses batch_processing — converts to incremental materialization with lookback window")

                tasks.append(task)

        # Parse views
        views = domain_config.get("views", {})
        if isinstance(views, dict):
            for view_name, view_config in views.items():
                if not isinstance(view_config, dict):
                    continue
                conn_id = view_config.get("conn_id", "")
                if conn_id:
                    connections.add(conn_id)

                sql_ref = view_config.get("sql", "")
                sql_content = ""
                if sql_ref and extract_dir:
                    candidates = [
                        os.path.join(extract_dir, sql_ref),
                        os.path.join(extract_dir, "dags", sql_ref),
                        os.path.join(extract_dir, "dags", "transform", sql_ref),
                        os.path.join(os.path.dirname(os.path.join(extract_dir, yaml_path)), sql_ref),
                    ]
                    for candidate in candidates:
                        if os.path.isfile(candidate):
                            try:
                                with open(candidate, "r", encoding="utf-8", errors="replace") as sf:
                                    sql_content = sf.read()
                            except Exception:
                                pass
                            break

                jinja_warnings = []
                if sql_content:
                    sql_content, jinja_warnings = convert_jinja_templates(sql_content)

                task = {
                    "task_id": view_name,
                    "operator_class": "SQLExecuteQueryOperator",
                    "sql": sql_content[:3000] if sql_content else f"-- SQL file: {sql_ref}",
                    "sql_file_ref": sql_ref,
                    "python_callable": "",
                    "bash_command": "",
                    "connection_id": conn_id,
                    "depends_on": [t["task_id"] for t in tasks],  # Views depend on all steps
                    "dapos_mapping": {"dapos_type": "transform", "subtype": "view"},
                }
                if jinja_warnings:
                    task["jinja_warnings"] = jinja_warnings
                tasks.append(task)

        # Parse downstream DAG triggers
        downstream = domain_config.get("downstream_dags", [])
        if isinstance(downstream, list):
            for ds_dag in downstream:
                tasks.append({
                    "task_id": f"trigger_{ds_dag}",
                    "operator_class": "TriggerDagRunOperator",
                    "sql": "",
                    "python_callable": "",
                    "bash_command": "",
                    "connection_id": "",
                    "depends_on": [t["task_id"] for t in tasks],
                    "dapos_mapping": {"dapos_type": "sensor", "subtype": "trigger"},
                    "triggered_dag": ds_dag,
                })

        # Build step dependency chain: CREATE → TRANSFORM → LOAD → VIEWS → TRIGGERS
        step_names = [t["task_id"] for t in tasks]
        for i, task in enumerate(tasks):
            if i > 0 and not task.get("depends_on"):
                # Auto-chain steps in order (unless already set)
                task["depends_on"] = [step_names[i - 1]]

        # Extract metadata
        refill_days = domain_config.get("refill_days", 0)
        delta_load = domain_config.get("delta_load", False)
        days_per_batch = domain_config.get("days_per_batch", 0)
        tags_list = domain_config.get("tags", [])

        dag = ParsedAirflowDag(
            dag_id=dag_id,
            file_path=yaml_path,
            schedule_interval=schedule,
            default_args={"refill_days": refill_days, "delta_load": delta_load, "days_per_batch": days_per_batch},
            tasks=tasks,
            connections_referenced=list(connections),
            variables_referenced=variables,
            python_imports=[],
            raw_code=yaml_content[:4000],
            parse_warnings=parse_warnings,
        )

        # Attach metadata that the agent can use for better analysis
        dag.tags = tags_list if isinstance(tags_list, list) else []
        dag.template_type = "transform" if is_transform else "domo_refresh" if is_domo else "dq_sync" if is_dq else "yaml_template"
        dag.env_params = env_params
        dag.refill_days = refill_days
        dag.delta_load = delta_load

        dags.append(dag)

    return dags


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
        self.function_sources: dict[str, str] = {}  # func_name -> source code

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            self.imports.append(alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ""
        for alias in node.names:
            self.imports.append(f"{module}.{alias.name}")
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """Capture function source code for python_callable resolution."""
        try:
            self.function_sources[node.name] = ast.get_source_segment(self.source, node) or ""
        except Exception:
            self.function_sources[node.name] = ""
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
        # Resolve python_callable source code (functions may be defined after task)
        for task in self.tasks.values():
            callable_name = task.get("python_callable", "")
            if callable_name and "python_source" not in task:
                func_src = self.function_sources.get(callable_name, "")
                if func_src:
                    task["python_source"] = func_src[:2000]
            # Convert Jinja templates in python_source
            if task.get("python_source"):
                task["python_source"], w = convert_jinja_templates(task["python_source"])
                if w:
                    task.setdefault("jinja_warnings", []).extend(w)

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
                    # Resolve function source code if available
                    func_src = self.function_sources.get(kw.value.id, "")
                    if func_src:
                        task["python_source"] = func_src[:2000]
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

        # Convert Jinja templates in SQL and bash_command
        jinja_warnings = []
        if task["sql"]:
            task["sql"], w = convert_jinja_templates(task["sql"])
            jinja_warnings.extend(w)
        if task["bash_command"]:
            task["bash_command"], w = convert_jinja_templates(task["bash_command"])
            jinja_warnings.extend(w)
        if jinja_warnings:
            task["jinja_warnings"] = jinja_warnings

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
