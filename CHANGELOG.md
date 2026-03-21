# Changelog

All notable changes to the Pipeline Agent (DAPOS) are documented in this file.

Format: Each entry records what changed, why, and test results at the time of the change.

---

## Roadmap — Next Builds

| Build | Feature | Status | Why |
|-------|---------|--------|-----|
| 14 | Hook template variables | **Done** | `{{watermark_after}}`, `{{run_id}}` etc. — unblocks consume-and-merge pattern |
| 15 | Run context propagation | **Done** | Upstream run context (watermarks, batch IDs) flows to downstream pipelines |
| 16 | Data contracts between pipelines | **Done** | Formalize producer/consumer relationships, cleanup policies, retention |
| 17 | SQL-native intra-DB steps | Pending | Skip CSV extract for same-database pipelines (INSERT INTO...SELECT) |
| 18 | Composable step DAG | Pending | Replace fixed extract→load→promote flow with configurable step graph (ingestion-focused, transforms deferred) |
| 19 | DAG visualization UI | **Done** | Visual pipeline dependency graph with execution status |
| 20 | Agent topology reasoning | **Done** | Agent designs multi-pipeline architectures from natural language |

---

## [Unreleased]

### Builds 19-20 - 2026-03-21 (Claude Opus 4.6)

**Build 19: DAG Visualization UI**

#### Added
- **`GET /api/dag`** — Returns full pipeline dependency graph with nodes (pipeline summary + last run + contract info) and edges (dependencies with type). Powers the DAG view.
- **DAGView component** — New SVG-based pipeline dependency graph in the React SPA. Features:
  - Topological sort into layers (roots at top, leaves at bottom)
  - Nodes colored by status (green=active, gray=paused, red=failed)
  - Tier badges (T1/T2/T3) on each node
  - Source→target labels on each node
  - Last run row count
  - Contract violation count badges
  - Dependency arrows (solid) vs data contract edges (purple dashed)
  - Click-to-select detail panel showing pipeline info, contracts, and violations
  - Legend for all visual indicators
- **"DAG" nav item** — Added to sidebar between Lineage and Connectors

**Build 20: Agent Topology Reasoning**

#### Added
- **`AgentCore.design_topology()`** — Claude-powered method that takes a natural language description of a business problem and designs a multi-pipeline architecture. Returns structured JSON with:
  - Proposed pipelines (name, source, target, schedule, tier, merge keys, hooks)
  - Dependencies between pipelines (with trigger type)
  - Data contracts (with freshness SLA and cleanup ownership)
  - Pattern identification (fan-in, consume-and-merge, cascading aggregation, etc.)
  - Detailed reasoning for design decisions
- **`POST /api/topology/design`** — REST endpoint for topology design. Rate-limited to 10/min. Admin/operator only.
- **Chat routing** — Keywords "design", "architect", "topology", "multi-pipeline", "data architecture", "pipeline architecture" route to the topology designer. Response is formatted with pipeline list, dependencies, contracts, and reasoning.
- **`design_topology` action** in both Claude-routed and keyword-routed command parsing.
- **5 curl tests** — DAG structure, node fields, contract fields, topology design endpoint, chat topology routing.

#### Key Use Cases Unlocked
- **Visual dependency monitoring**: Operators can see the entire pipeline graph at a glance, with status colors and contract violation badges highlighting problems.
- **Architecture-as-conversation**: User describes "I need orders from MySQL and customers from MongoDB merged into PostgreSQL" → agent designs 3 pipelines with dependencies, contracts, and schedules.
- **Two-tier autonomy for topology**: Agent proposes, human approves. The topology response is a proposal, not automatic creation.

---

### Build 16 - 2026-03-21 (Claude Opus 4.6)

**Data Contracts Between Pipelines**

#### Added
- **`DataContract` dataclass** — Formalizes producer/consumer relationships with: `contract_id`, `producer_pipeline_id`, `consumer_pipeline_id`, `description`, `status`, `required_columns`, `freshness_sla_minutes` (default 60), `retention_hours` (default 168), `cleanup_ownership` (producer_ttl / consumer_acknowledges / none), violation tracking.
- **`ContractViolation` dataclass** — Records individual contract violations with type (freshness_sla, schema_mismatch, retention_expired), detail, resolved state.
- **3 new enums** — `CleanupOwnership`, `DataContractStatus` (active/violated/paused/archived), `ContractViolationType`.
- **8 REST API endpoints**:
  - `POST /api/data-contracts` — Create contract (validates pipelines exist, rejects self-contracts and duplicates, auto-creates dependency)
  - `GET /api/data-contracts` — List with optional `?producer_id=`, `?consumer_id=`, `?status=` filters
  - `GET /api/data-contracts/{id}` — Detail with recent violations and pipeline names
  - `PATCH /api/data-contracts/{id}` — Update SLA, retention, required columns, status, cleanup ownership
  - `DELETE /api/data-contracts/{id}` — Delete contract and its violations
  - `POST /api/data-contracts/{id}/validate` — Manual validation (freshness SLA + required columns check)
  - `GET /api/data-contracts/{id}/violations` — List violations with optional `?resolved=` filter
  - `POST /api/data-contracts/{id}/violations/{vid}/resolve` — Mark violation resolved
- **Monitor integration** — `_check_data_contracts()` runs every monitor tick (5m), validates all active contracts for freshness SLA and schema requirements, creates violations and alerts on failure.
- **Cleanup guard** — `_check_cleanup_allowed()` in `PipelineRunner` blocks DELETE/TRUNCATE hooks when a data contract with `cleanup_ownership=consumer_acknowledges` exists and the consumer has no successful runs yet. Enforces "never delete unconsumed data" at the system level.
- **Pipeline detail enrichment** — `GET /api/pipelines/{id}` now includes `data_contracts.as_producer` and `data_contracts.as_consumer` arrays.
- **Auto-dependency** — Creating a data contract automatically creates a pipeline dependency (consumer depends on producer) if one doesn't already exist.
- **DB tables** — `data_contracts` and `contract_violations` with indexes on producer, consumer, status, and unresolved violations.
- **11 curl tests** — Create, list, get, validate, update, violations, pipeline detail enrichment, auto-dependency, duplicate/self rejection, delete.

#### Scope Decision
- dbt-like transforms and semantic layer features **deferred to later scope**. Current focus is ingestion (Fivetran) + orchestration (Airflow) + observability (Monte Carlo) only.

#### Key Use Cases Unlocked
- **Consume-and-merge safety**: Data contract with `cleanup_ownership=consumer_acknowledges` prevents the producer from deleting staged data until the consumer has processed it.
- **Freshness SLA monitoring**: Consumer pipelines can declare how fresh they need the producer's data — the monitor automatically detects and alerts on SLA breaches.
- **Schema expectations**: Consumer declares required columns — if the producer drops them (e.g. due to schema drift), the contract violation is caught before the consumer pipeline fails.

---

### Build 15 - 2026-03-09 (Claude Opus 4.6)

**Run Context Propagation**

#### Added
- **`triggered_by_run_id` / `triggered_by_pipeline_id`** on `RunRecord` — When a pipeline is data-triggered, the downstream run records which upstream run and pipeline caused the trigger.
- **9 upstream template variables** — `{{upstream_run_id}}`, `{{upstream_pipeline_id}}`, `{{upstream_watermark_before}}`, `{{upstream_watermark_after}}`, `{{upstream_rows_extracted}}`, `{{upstream_rows_loaded}}`, `{{upstream_started_at}}`, `{{upstream_completed_at}}`, `{{upstream_batch_id}}`. Available in post-promotion hook SQL alongside existing 15 variables (total: 24).
- **Upstream metadata namespace** — After data-triggered runs, upstream context (run_id, pipeline_id, watermark, row count, completion time) is auto-written as metadata under `namespace="upstream"`, queryable via the existing metadata API.
- **`GET /api/runs/{run_id}/trigger-chain`** — Walks the trigger chain backwards to the root run, returning full run summaries at each hop. Supports multi-hop chains (A → B → C).
- **UI trigger indicators** — Data-triggered runs show the upstream pipeline ID snippet in the run list.

#### Changed
- **`scheduler/manager.py`** — `_trigger_downstream()` now receives the completed `RunRecord` and sets `triggered_by_run_id` / `triggered_by_pipeline_id` on downstream runs.
- **`agent/autonomous.py`** — `_execute_inner()` loads upstream run for data-triggered runs. `_render_hook_sql()` extended from 15 to 24 template variables. `_write_run_metadata()` writes upstream context under `namespace="upstream"`.
- **`api/server.py`** — `_run_summary()` includes `triggered_by_run_id` and `triggered_by_pipeline_id`.
- **DB migration** — `ALTER TABLE runs ADD COLUMN IF NOT EXISTS triggered_by_run_id TEXT` and `triggered_by_pipeline_id TEXT`.

#### Key Use Case Unlocked
Consume-and-merge with upstream watermark boundaries:
```sql
-- Downstream hook: delete only rows consumed by the upstream run
DELETE FROM raw.stage_orders WHERE updated_at <= '{{upstream_watermark_after}}'
```

---

### Build 14 - 2026-03-09 (Claude Opus 4.6)

**Hook Template Variables**

#### Added
- **`_render_hook_sql()`** — Static method on `PipelineRunner` that replaces `{{variable}}` placeholders with run context values before SQL execution. 15 supported variables: `{{pipeline_id}}`, `{{pipeline_name}}`, `{{run_id}}`, `{{run_mode}}`, `{{watermark_before}}`, `{{watermark_after}}`, `{{rows_extracted}}`, `{{rows_loaded}}`, `{{started_at}}`, `{{completed_at}}`, `{{source_schema}}`, `{{source_table}}`, `{{target_schema}}`, `{{target_table}}`, `{{batch_id}}` (alias for `run_id[:8]`).
- **`rendered_sql` in hook results** — When template variables are used, the resolved SQL is stored in the hook result metadata so users can see exactly what executed.
- **UI template hints** — Hooks editor shows available template variables as inline code tags.

#### Key Use Case Unlocked
Consume-and-merge pattern is now possible:
```sql
-- Pipeline 2 hook: safely delete only consumed rows from stage table
DELETE FROM raw.stage_orders WHERE updated_at <= '{{watermark_after}}'
```
Rows arriving after `watermark_after` survive the DELETE for the next run.

#### Changed
- **`agent/autonomous.py`** — `_execute_post_promotion_hooks()` calls `_render_hook_sql()` before `execute_sql()`. None values render as empty string.
- **`ui/App.jsx`** — Template variable hints added below hooks editor textarea.

---

### Build 13 - 2026-03-08 (Claude Opus 4.6)

**SQL-based Post-Promotion Hooks**

#### Added
- **`PostPromotionHook` dataclass** — Defines a SQL hook with: `hook_id`, `name`, `sql`, `metadata_key`, `description`, `enabled`, `timeout_seconds`, `fail_pipeline_on_error`. Stored as JSONB array on `PipelineContract`.
- **`TargetEngine.execute_sql()`** — Non-abstract default method on the target interface. Raises `NotImplementedError` for connectors that don't support it. PostgreSQL seed connector implements it with statement timeout and `RealDictCursor`.
- **Hook execution in runner** — `_execute_post_promotion_hooks()` runs after promotion, before marking COMPLETE. Results stored as metadata under `namespace="hooks"`. Supports fail-fast (`fail_pipeline_on_error=true`) or best-effort (default). JSON-safe serialization handles `Decimal`, `datetime`, `bytes`.
- **`POST /api/pipelines/{id}/hooks/test`** — Test endpoint executes SQL against the pipeline's target connector without saving. Returns rows (capped at 100) with timing. Admin/operator only.
- **PATCH support** — `post_promotion_hooks` field on `UpdatePipelineRequest`. Auto-generates `hook_id` for new hooks.
- **Detail enrichment** — Pipeline detail response includes `post_promotion_hooks` array and `hook_results` dict (latest results from hooks namespace metadata).
- **UI hooks display** — Read-only section shows each hook with name, SQL preview, enabled status, last execution result (status, duration, output).
- **UI hooks editor** — JSON textarea in Edit Settings panel for defining/editing hooks.
- **DB migration** — `ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS post_promotion_hooks JSONB NOT NULL DEFAULT '[]'`.

#### Design Decisions
- Hooks execute against the same target connector instance used for promotion (no re-instantiation needed).
- Hook results are persisted as XCom-style metadata with `namespace="hooks"`, making them consumable by downstream pipelines via the metadata API.
- `execute_sql()` is opt-in per connector — targets that don't support it gracefully skip hooks with a warning.
- No SQL validation/sandboxing — hooks are set by admin/operator roles, not arbitrary users.

### Build 12 - 2026-03-08 (Claude Opus 4.6)

**Per-pipeline Schema Change Policies**

#### Added
- **`SchemaChangePolicy` dataclass** — 5 fields: `on_new_column` (auto_add/propose/ignore), `on_dropped_column` (halt/propose/ignore), `on_type_change` (auto_widen/propose/halt), `on_nullable_change` (auto_accept/propose/halt), `propagate_to_downstream` (bool).
- **`SCHEMA_POLICY_TIER_DEFAULTS`** — T1: halt drops + propose types; T2: propose drops + auto-widen; T3: ignore drops + auto-widen. Tier defaults ensure zero-config safety scaling.
- **`PipelineContract.get_schema_policy()`** — Returns explicit policy if set, otherwise tier default. Backward compatible with `auto_approve_additive_schema`.
- **Nullable change detection** — `_detect_nullable_changes()` in monitor compares `is_nullable` between contract and live profile.
- **Policy-driven drift decisions** — Each change category (new column, dropped column, type change, nullable change) resolved independently per policy. Halt reasons collected and applied as a batch.
- **Downstream schema propagation** — When pipeline A auto-applies schema changes and `propagate_to_downstream=True`, creates proposals for dependent pipelines (never auto-applies — respects two-tier autonomy).
- **`GET /api/schema-policy-defaults`** — Returns tier-based default policies.
- **Schema change policy editor** — 4 dropdown selects + propagation checkbox in the Edit Settings panel.
- **Schema policy enums** — `SchemaColumnAction`, `SchemaDropAction`, `SchemaTypeAction`, `SchemaNullableAction`.

#### Changed
- **`monitor/engine.py`** — `_check_drift()` rewritten to use policy-driven decisions. Split into `_create_halt_proposal()`, `_create_drift_proposals()`, `_propagate_schema_downstream()`. `_auto_apply_schema_changes()` extended with nullable changes.
- **`contracts/store.py`** — Added `schema_change_policy JSONB` column to pipelines table. Updated `save_pipeline()` and `_row_to_pipeline()`.
- **`api/server.py`** — `UpdatePipelineRequest` extended with `schema_change_policy`. PATCH handler applies policy. `_pipeline_detail()` includes `schema_change_policy` and `schema_change_policy_is_custom`.
- **`ui/App.jsx`** — Schema change policy section in Edit Settings (4 selects + checkbox). Read-only policy summary in detail view.

---

### Build 11 - 2026-03-08 (Claude Opus 4.6)

**Data-aware Scheduling + Pipeline Metadata**

#### Added
- **Event-driven pipeline triggering** — When pipeline A completes, scheduler checks all downstream dependents. If ALL upstream dependencies are satisfied, triggers immediately with `RunMode.DATA_TRIGGERED`. Cron scheduling remains as fallback. No external queue — async callbacks within the scheduler.
- **`list_dependents(depends_on_id)` store method** — Reverse dependency lookup via `dependencies.depends_on_id` index.
- **XCom-style pipeline metadata** — New `pipeline_metadata` PostgreSQL table with `(pipeline_id, namespace, key)` unique constraint. Runner writes 5 standard keys after each successful run: `last_run_id`, `last_row_count`, `last_max_watermark`, `last_completed_at`, `last_staging_size_bytes`.
- **Metadata API endpoints** — `GET/PUT/DELETE /api/pipelines/{id}/metadata[/{key}]`.
- **Dependency management API** — `POST /api/pipelines/{id}/dependencies` with cycle detection, `GET` list, `DELETE` remove. All with DecisionLog audit.
- **Dependencies display in pipeline detail** — Upstream list with type pills + remove button. Downstream count. Inline "Add dependency" button.
- **Metadata display in pipeline detail** — Grid of key-value cards showing namespace/key, value, updated_at.
- **`RunMode.DATA_TRIGGERED`** — New run mode enum value for auditability.
- **`PipelineMetadata` dataclass** — `id`, `pipeline_id`, `namespace`, `key`, `value_json`, `updated_at`, `created_by_run_id`.

#### Changed
- **`scheduler/manager.py`** — `_run_pipeline()` calls `_trigger_downstream()` after COMPLETE status.
- **`agent/autonomous.py`** — Calls `_write_run_metadata()` after promotion.
- **`api/server.py`** — `_pipeline_detail()` includes `dependencies` and `metadata` sections. New dependency and metadata CRUD endpoints.
- **`contracts/store.py`** — `pipeline_metadata` table DDL, 4 CRUD methods, `list_dependents()`, `_row_to_metadata()`.
- **`contracts/models.py`** — Added `RunMode.DATA_TRIGGERED`, `PipelineMetadata` dataclass.
- **`ui/App.jsx`** — Dependency management section and metadata display in PipelinesView detail panel.

---

### Build 10 - 2026-03-08 (Claude Opus 4.6)

**Pipeline Settings UI + Change Logging + Auto-Persistence**

#### Added
- **Expanded PATCH `/api/pipelines/{id}`** — 18+ editable fields (was 4). Supports schedule (cron, retry, backoff, timeout), strategy (refresh_type, replication_method, incremental_column, load_type, merge_keys, watermark reset), quality config (partial merge of any QualityConfig field), observability (tier, owner, tags, tier_config, freshness_column), and approval settings (auto_approve_additive_schema). All changes tracked with old→new diffs.
- **Change audit trail** — Every PATCH saves a `DecisionLog` with `decision_type="contract_update"`, JSON diff of all changed fields, and optional user-provided `reason`. Visible in the Timeline view.
- **Auto-persist to YAML** — On every contract update, writes `data/contracts/{pipeline_name}.yaml` with masked credentials. Enables Git-based contract versioning.
- **`_persist_contract_yaml()` helper** — Writes pipeline contract to disk as YAML after each update.
- **Pipeline Settings UI** — Full edit form in the Pipelines view with 4 grouped sections: Schedule (cron, retry, backoff, timeout), Strategy (refresh type, load type, replication, incremental column, merge keys, watermark reset), Quality (6 threshold inputs + 2 checkboxes), Observability (tier, owner, freshness column, tags JSON, auto-approve). Includes change reason input and Save/Cancel buttons.
- **YAML view button** — Toggle to display pipeline contract as formatted YAML in a dark-themed `<pre>` block.
- **Timeline button** — Toggle to show change history (DecisionLog entries filtered to `contract_update` type) with decision type pill, timestamp, detail, and reasoning.
- **`contracts_dir` config property** — `config.contracts_dir` → `data/contracts/`. Directory auto-created on startup.

#### Changed
- **`config.py`** — Added `contracts_dir` property.
- **`main.py`** — Creates `contracts_dir` on startup in `setup_data_dirs()`.
- **`api/server.py`** — Added `DecisionLog, RefreshType, ReplicationMethod, LoadType, QualityConfig` imports. Expanded `UpdatePipelineRequest` from 4 to 18+ fields. Rewrote PATCH handler with change tracking, version bumping, DecisionLog audit, and YAML persistence. Expanded `_pipeline_detail()` to include `replication_method`, `retry_max_attempts`, `retry_backoff_seconds`, `timeout_seconds`, `auto_approve_additive_schema`, `tier_config`, `freshness_column`, and full `quality_config` (was 3 fields, now all QualityConfig fields via `asdict()`).
- **`ui/App.jsx`** — Added `editForm`, `saving`, `yamlView`, `timeline` state. Added `startEditing()`, `saveSettings()`, `loadYaml()`, `loadTimeline()` functions. Added settings editor panel, YAML view, timeline display, and 3 new buttons (Edit Settings, View YAML, Timeline).

---

### Build 9 - 2026-03-08 (Claude Opus 4.6)

**Contract-as-Code: YAML export, import, and GitOps sync for pipeline contracts**

#### Added
- **`contracts/yaml_codec.py` (new file)** — Pure serialization module with 8 functions: `pipeline_to_dict`, `dict_to_pipeline`, `pipeline_to_yaml`, `yaml_to_pipeline`, `pipelines_to_yaml`, `yaml_to_pipelines`, `diff_contracts`, `snapshot_state`. Groups flat PipelineContract fields into human-readable, Git-diff-friendly YAML structure (source/target/strategy/schedule/quality sections).
- **`GET /api/pipelines/export`** — Bulk export all pipelines as multi-document YAML. `?status=active` filter, `?include_credentials=true` (admin-only, decrypts Fernet-encrypted passwords in-memory).
- **`GET /api/pipelines/{id}/export`** — Single pipeline YAML export. `?include_state=true` adds runtime state (baselines, error budget, dependencies, schema versions). `?include_credentials=true` (admin-only).
- **`POST /api/pipelines/import`** — Import pipelines from YAML body. `?mode=create` (default, 409 if exists) or `?mode=upsert` (preserves pipeline_id, bumps version, preserves credentials if masked with `***`).
- **`POST /api/contracts/sync`** — GitOps reconciliation. `?dry_run=true` (default) returns field-level diffs without applying. `?dry_run=false` creates new pipelines, updates existing. Returns `{created, updated, unchanged, errors}`.
- **`get_pipeline_by_name()`** — New store method for name-based pipeline lookup (UNIQUE constraint), used by import/sync endpoints.

#### Changed
- **`requirements.txt`** — Added `pyyaml>=6.0` dependency.
- **`api/server.py`** — Added YAML codec imports, `encrypt`/`decrypt` imports from crypto, 4 new endpoints.

#### Design decisions
- `pipeline_name` is the sync key (not `pipeline_id`) — enables same YAML across environments where IDs differ.
- Credentials masked by default (`"***"`) on export; preserved on import when masked.
- Runtime state (`_state:` section) exported separately, ignored on import by default.
- Route ordering: `GET /api/pipelines/export` registered before `GET /api/pipelines/{pipeline_id}` to avoid path collision.

---

### Build 8 - 2026-03-08 (Claude Opus 4.6)

**Production-grade structured logging with pipeline context propagation**

#### Added
- **`logging_config.py` (new file)** — ContextVars (`pipeline_id`, `pipeline_name`, `run_id`, `request_id`, `component`), `PipelineContext` context manager (sync + async, reset-based), `ContextFilter` for automatic injection, `JSONFormatter` (one JSON object per line, Datadog/Loki/CloudWatch compatible), `ConsoleFormatter` (human-readable with inline context tags), `setup_logging()` with `RotatingFileHandler`.
- **Request correlation middleware** — `@app.middleware("http")` reads `X-Request-ID` header or generates UUID, sets contextvar, logs `METHOD /path STATUS (duration_ms)` for non-health requests, returns `X-Request-ID` in response header.
- **Quality gate logging** — Gate decisions logged as WARNING (HALT) or INFO (PROMOTE/PROMOTE_WITH_WARNING) with check summary. Individual check details logged at DEBUG level.
- **Per-pipeline timeline API** — `GET /api/pipelines/{pipeline_id}/timeline` returns merged runs, gates, alerts, and decisions sorted by timestamp. New `list_alerts_for_pipeline()` store method.

#### Changed
- **`config.py`** — Added `LOG_FORMAT` (default: json), `LOG_MAX_BYTES` (default: 50MB), `LOG_BACKUP_COUNT` (default: 5) env vars.
- **`main.py`** — `setup_logging()` delegates to `logging_config.setup_logging()` with config values. Creates `data/logs/` directory.
- **`agent/autonomous.py`** — Split `execute()` into PipelineContext wrapper + `_execute_inner()`. Removed all 14 `[%s]` manual prefixes.
- **`scheduler/manager.py`** — `_tick()` loop and `_run_pipeline()` wrapped in PipelineContext. Removed ~8 `[%s]` prefixes.
- **`monitor/engine.py`** — `_tick()` loop wrapped in PipelineContext. Removed ~6 `[%s]` prefixes.
- **`quality/gate.py`** — Added gate decision + check summary logging after evaluation.
- **`api/server.py`** — Added request correlation middleware, timeline endpoint, imported `time`, `uuid`, `logging_config`.
- **`contracts/store.py`** — Added `list_alerts_for_pipeline(pipeline_id, limit)` method.

#### Log output examples

Console: `2026-03-08 14:32:15 INFO  agent.autonomous -- [demo-orders | run:abc12345] Extracted 30 rows`

JSON: `{"timestamp":"2026-03-08T14:32:28+00:00","level":"INFO","logger":"agent.autonomous","message":"Extracted 30 rows","pipeline_id":"abc-123","pipeline_name":"demo-orders","run_id":"abc12345-full-uuid","component":"runner"}`

---

### Build 7 - 2026-03-08 (Claude Opus 4.6)

**Features #5-9: Incremental extraction (verified), enhanced run history UI, connector approval flow, alerting dispatch, schema drift auto-remediation**

#### Added
- **Feature #6: Enhanced pipeline run history** — `_run_summary()` now includes `run_mode`, `staging_size_bytes`, `quality_results`, `watermark_before`, `watermark_after`. UI shows duration, run mode pill, staging size, expandable quality check details, watermark progression for incremental pipelines.
- **Feature #7: Connector approval flow** — ApprovalsView now shows connector code in a syntax-highlighted `<pre>` block for `new_connector` proposals. Added "Test Connector" button that calls `POST /api/connectors/{id}/test` and displays results inline.
- **Feature #8: Alerting dispatch with mock webhook** — Added mock Slack webhook endpoints (`POST /webhook/slack`, `GET /webhook/slack/history`) to demo-api. Demo pipelines now get a notification policy routing alerts to the mock webhook. `_resolve_channels()` made async with policy lookup from store.
- **Feature #9: Schema drift auto-remediation** — Demo pipelines created with `auto_approve_additive_schema=True`. Added `_is_safe_type_widening()` helper recognizing VARCHAR widening, INT→BIGINT, FLOAT→DOUBLE PRECISION. Auto-apply now handles both new columns and safe type widenings.

#### Fixed
- **`test_connector` endpoint bugs** — Added missing `await` on `registry.get_source()`/`registry.get_target()`, fixed `**params` → `params` arg passing, added DRAFT/APPROVED connector temporary loading via `register_approved_connector()`, now saves `test_status` on connector record.
- **`_resolve_channels()` was sync** — Made async to enable notification policy lookup from the store. Previously had a `pass` placeholder for policy lookup.

#### Changed
- **`api/server.py`** — Added `TestStatus` to imports; enriched `_run_summary()`; fixed `test_connector` endpoint
- **`ui/App.jsx`** — Enhanced PipelinesView run display with duration, quality checks, watermarks; enhanced ApprovalsView with code view and test button
- **`monitor/engine.py`** — `_resolve_channels()` now async with policy lookup; added `_is_safe_type_widening()`; renamed `_auto_apply_new_columns` → `_auto_apply_schema_changes` with type widening support
- **`demo/mock-api/app.py`** — Added mock Slack webhook endpoints
- **`demo/bootstrap.py`** — Added `auto_approve_additive_schema=True`, `tier_config={"digest_only": False}`, notification policy creation and wiring to demo pipelines

#### Verified
- **Feature #5: Incremental extraction** — Already fully implemented. MySQL source builds `WHERE inc_col > last_watermark`, MongoDB does `{"$gt": wm}`. `demo-ecommerce-customers` uses INCREMENTAL with `updated_at` column.

---

### Build 6 - 2026-03-08 (Claude Opus 4.6)

**Four bug fixes: store mismatch, source credentials, quality gate first-run, demo triggers**

#### Fixed
- **Bug #1: `list_runs(window_days=)` store method mismatch** — `agent/autonomous.py` called `store.list_runs(pipeline_id, window_days=budget.window_days)` but the store method signature is `list_runs(pipeline_id, limit=50)`. Changed to `limit=budget.window_days * 24` (~1 run/hour cap).
- **Bug #2: Source credentials missing from PipelineContract** — `PipelineContract` had `target_user`/`target_password` but no source equivalents. `_connector_params()` hardcoded `user=""`, `password=""` for sources. Added `source_user`/`source_password` fields to model, DDL, store save/load, demo bootstrap, conversation manager, and crypto CREDENTIAL_FIELDS.
- **Bug #3: Quality gate always halts on first run** — On the first run with no baseline, checks like schema_consistency could FAIL on type mismatches. Added first-run leniency: if no prior COMPLETE runs exist, FAILs are auto-downgraded to WARNs with a `[First run - auto-downgraded]` prefix, ensuring the first run promotes and establishes baselines.
- **Bug #4: Demo pipelines never trigger on first startup** — Demo pipelines had `schedule_cron = "0 * * * *"` (hourly on the hour), so they wouldn't run until the next hour boundary. Now all 4 demo pipelines are triggered immediately after creation via `asyncio.create_task(runner.execute(...))`.
- **Bonus: `list_gates(days=1)` in `main.py`** — The observability quality summary called `store.list_gates(pipeline_id, days=1)` but `list_gates()` doesn't accept a `days` parameter. Removed the invalid kwarg.

#### Changed
- **`contracts/models.py`** — Added `source_user: str = ""` and `source_password: str = ""` to PipelineContract
- **`contracts/store.py`** — Added `source_user`, `source_password` columns to pipelines DDL; updated save_pipeline() INSERT/UPSERT (51→53 params); updated _row_to_pipeline()
- **`agent/autonomous.py`** — `_connector_params()` now uses `contract.source_user`/`contract.source_password` instead of hardcoded empty strings
- **`agent/conversation.py`** — `create_pipeline()` now wires `source_user`/`source_password` from encrypted source_params into PipelineContract
- **`quality/gate.py`** — Added first-run detection and FAIL→WARN downgrade logic before decision evaluation
- **`demo/bootstrap.py`** — Added `source_user`/`source_password` to MySQL demo configs; accepts optional `runner` param to trigger pipelines immediately
- **`main.py`** — Passes `runner` to `bootstrap_demo_pipelines()`; removed invalid `days=1` kwarg from `list_gates()` call
- **`crypto.py`** — Added `"source_password"` to CREDENTIAL_FIELDS

---

### Build 5 - 2026-03-08 (Claude Opus 4.6)

**Authentication enabled by default with RBAC**

#### Added
- **Default admin user** — auto-created on first startup (admin/admin, admin@dapos.local)
- **`require_role()` RBAC helper** in `api/server.py` — enforces role-based access on all mutating endpoints
- **"operator" role** — replaces "editor". Three roles: admin (full access), operator (run/manage pipelines), viewer (read-only)
- **Role validation** on `RegisterRequest` — rejects invalid roles

#### Fixed
- **`User.user_id` AttributeError** — `User` model had `id` field but login/register endpoints referenced `user.user_id`. Added `@property user_id` alias.
- **`User.email` missing** — Register endpoint set `email=req.email` but User model and users table had no email field. Added `email` field to model, DDL, and store methods.
- **Empty JWT secret** — `JWT_SECRET` defaulted to `""`, making token signing fail. Added fallback dev secret.

#### Changed
- **`AUTH_ENABLED` default** — changed from `false` to `true`. Auth is now on by default.
- **`config.py`** — JWT secret falls back to `"dapos-dev-secret-change-in-production"` when not set
- **RBAC enforcement** on 12 mutating endpoints (connector generate/deprecate, pipeline CRUD/trigger/pause/resume/backfill, approvals)

#### RBAC Matrix
| Action | admin | operator | viewer |
|--------|-------|----------|--------|
| Register users | yes | no | no |
| Generate/deprecate connectors | yes | no | no |
| Test connectors | yes | yes | no |
| Create/update/delete pipelines | yes | yes | no |
| Trigger/pause/resume/backfill | yes | yes | no |
| Approve/reject proposals | yes | yes | no |
| View all data, chat | yes | yes | yes |

---

### Build 4 - 2026-03-08 (Claude Opus 4.6)

**Freshness monitoring fix + lineage endpoint fix**

#### Fixed
- **`monitor/engine.py` — method name mismatch** — `save_freshness_snapshot()` → `save_freshness()`. The monitor engine called a non-existent method, so freshness snapshots were never persisted. The UI freshness view showed "No freshness data yet" for all pipelines.
- **`monitor/engine.py` — `float("inf")` in alert JSON** — When a pipeline has no successful runs, staleness is `float("inf")`. The alert detail dict passed this raw value to PostgreSQL JSON, causing `invalid input syntax for type json: Token "Infinity"`. Capped to `99999` in both summary string and detail dict.
- **`api/server.py` — `list_dependencies()` missing argument** — The lineage endpoint called `store.list_dependencies()` without the required `pipeline_id` argument, crashing with `TypeError`. Replaced with per-pipeline lookups to find downstream dependencies.
- **`agent/core.py` — "List my pipelines" keyword routing** — The predefined UI chip "List my pipelines" failed keyword matching because `"my"` broke the substring match for `"list pipeline"`. Added `"my pipeline"` to the keyword patterns.

#### Verified
- Freshness endpoint returns data for 3 demo pipelines (Stripe: fresh, MongoDB: fresh, Orders: critical — no successful run yet)
- Lineage endpoint no longer crashes
- "List my pipelines" UI chip works correctly

---

### Build 3 - 2026-03-08 (Claude Opus 4.6)

**Default demo environment with end-to-end pipeline execution**

#### Added
- **4 new seed connectors** in `connectors/seeds.py`:
  - MongoDB source (pymongo) — document-to-CSV extraction with schema inference
  - Stripe source (httpx) — paginated API extraction with Bearer auth
  - Google Ads source (httpx) — report-style API extraction with pagination
  - Facebook Insights source (httpx) — cursor-based API pagination
- **Demo Docker services** in `docker-compose.yml`:
  - `demo-mysql` (MySQL 8.0) — e-commerce dataset: 20 products, 20 customers, 30 orders, 40 order items
  - `demo-mongo` (MongoDB 7) — analytics dataset: 200 web events (page views, clicks, purchases)
  - `demo-api` (FastAPI) — mock Stripe (50 charges, 30 customers), Google Ads (40 campaigns), Facebook Insights (45 ad insights)
- **Demo pipeline bootstrap** (`demo/bootstrap.py`):
  - Auto-creates 4 demo pipelines on first startup (if no pipelines exist)
  - Profiles source tables to populate column_mappings for correct target DDL
  - Pipelines: MySQL→PostgreSQL (orders + customers), MongoDB→PostgreSQL (events), Stripe→PostgreSQL (charges)
- **pymongo** added to `requirements.txt` and `sandbox.py` ALLOWED_IMPORTS

#### Changed
- **MySQL source connector** — added `**kwargs`, `port` default, `user` defaults to `"root"` when empty (needed because `_connector_params()` passes `user=""` for source connections)
- **`docker-compose.yml`** — expanded from 1 service (PostgreSQL) to 4 (+ demo-mysql, demo-mongo, demo-api)
- **`connectors/registry.py`** — `bootstrap_seeds()` now registers 8 connectors (was 4)
- **`main.py`** — calls `bootstrap_demo_pipelines(store, registry)` after seed bootstrap

#### Verified
- All 8 connectors seed + load on startup
- 4 demo pipelines auto-created with profiled column_mappings
- End-to-end execution verified:
  - MySQL orders: 30 rows extracted, 30 loaded (quality gate halted — no baseline, expected)
  - Stripe charges: 50 rows extracted, 50 loaded, quality gate **promoted** to target
  - MongoDB events: 200 rows extracted, 200 loaded, quality gate **promoted** to target

---

### Build 2 - 2026-03-08 (Claude Opus 4.6)

**Testing framework overhaul + bug fixes**

#### Changed
- **Replaced pytest framework with curl-based test suite** (`test-pipeline-agent.sh`)
  - Previous: pytest-based unit/integration/e2e/chaos/performance tests with mock services
  - New: 127 curl-based tests against the real running app via REST APIs and `/api/command` chat endpoint
  - Rationale: The agent IS the product. Testing it with mocks and isolated unit tests misses the point. Tests should exercise the real agent with the real LLM through the same APIs users interact with.

#### Fixed
- **`from __future__ import annotations` in `api/server.py`** - This made all type annotations lazy strings, breaking Pydantic v2's type validation with FastAPI. Removed the import.
- **`ConnectorType` string vs enum in `agent/core.py`** - The API passes `connector_type` as a string (`"source"`/`"target"`) but `generate_connector()` called `.value` on it assuming an enum. Added normalization: `if isinstance(connector_type, str): connector_type = ConnectorType(connector_type.lower())`
- **`record.status.value` in `api/server.py`** - Connector generation returned a record with `status` as a string, but the response serialization called `.value`. Added safe access: `record.status.value if hasattr(record.status, 'value') else record.status`

#### Removed
- `tests/` directory (11 pytest files: test_sandbox, test_crypto, test_auth, test_quality_gate, test_store, test_registry, test_scheduler, test_monitor, test_autonomous, conftest, __init__)
- `test-data/` directory (SQL seeds, generators, scenario factory)
- `test-mocks/` directory (mock Stripe, Google Analytics, Facebook Ads FastAPI servers)
- `docker-compose.test.yml` (10+ test services: multiple databases, mock APIs, toxiproxy)
- `.github/workflows/test-suite.yml` (CI pipeline)
- Documentation: `TESTING.md`, `AGENT-TESTING-GUIDE.md`, `FINAL-TEST-SUMMARY.md`, `QUICK-VALIDATION.md`, `TEST-SUMMARY.md`, `tests/README.md`, `tests/MANUAL-UI-TESTING.md`
- Scripts: `run-tests.sh`, `run-interactive-tests.sh`, `test-agent-commands.sh`, `validate-app.sh`

#### Added
- `test-pipeline-agent.sh` - Comprehensive curl-based test suite
- `CLAUDE.md` - Product context for Claude Code sessions
- `CHANGELOG.md` - This file

#### Test Results (Build 2)
```
Total:    127
Passed:   121
Failed:     4 (connector generation timeouts - curl 60s limit vs LLM generation time)
Warnings:   2 (keyword matching false negatives in multi-turn validation)
Skipped:    1 (pipeline CRUD - depends on active connectors)
Duration: ~20 minutes
```

---

## Build 1 - 2026-03-08 (Initial commit)

**Initial release of DAPOS - Agentic Data Platform**

#### Added
- Complete pipeline agent application
  - 4 concurrent async loops: API server, scheduler, monitor, observability
  - FastAPI REST API with 40+ endpoints
  - React 18 SPA with 9 views (CDN, no build step)
  - Claude-powered agent for natural language interaction
  - Connector generation via Claude with AST-validated sandbox
  - 7-check quality gate (count reconciliation, schema consistency, PK uniqueness, null rates, volume z-score, sample verification, freshness)
  - Schema drift detection and auto-adaptation
  - Freshness monitoring with tier-based SLAs
  - Error budgets with automatic escalation
  - Column-level lineage tracking
  - Agent cost tracking (tokens, latency per call)
  - Learning loop (preferences from approvals/rejections)
  - JWT authentication with 3 roles
  - Fernet encryption for credentials at rest
  - Alert dispatch to Slack, Email, PagerDuty
- Seed connectors: MySQL source (PyMySQL), Redshift target (psycopg2)
- PostgreSQL 16 + pgvector via Docker Compose
