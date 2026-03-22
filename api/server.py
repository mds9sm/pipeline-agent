"""
FastAPI REST API server with JWT auth, rate limiting, and agent-routed commands.
"""
import decimal
import json
import logging
import os
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

import bcrypt
from fastapi import FastAPI, HTTPException, Query, Depends, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import JSONResponse

from contracts.models import (
    PipelineStatus, ProposalStatus, ConnectorStatus, TestStatus,
    PipelineDependency, NotificationPolicy, AgentPreference,
    ContractChangeProposal, SchemaVersion, ColumnMapping,
    User, TriggerType, ChangeType, DependencyType,
    DecisionLog, RefreshType, ReplicationMethod, LoadType, QualityConfig,
    SchemaChangePolicy, SCHEMA_POLICY_TIER_DEFAULTS, PostPromotionHook,
    DataContract, ContractViolation,
    DataContractStatus, CleanupOwnership, ContractViolationType,
    PipelineChangeLog, PipelineChangeType, RegisteredSource,
    StepDefinition, StepType, CheckStatus,
    SqlTransform, MaterializationType,
    now_iso, new_id,
)
from contracts.store import Store
from connectors.registry import ConnectorRegistry
from agent.core import AgentCore
from agent.conversation import ConversationManager
from agent.autonomous import PipelineRunner
from scheduler.manager import Scheduler
from monitor.engine import MonitorEngine
from auth import AuthDependency, create_token
from crypto import encrypt_dict, decrypt_dict, encrypt, decrypt, CREDENTIAL_FIELDS
from config import Config
from logging_config import set_request_id, request_id_var
from contracts.yaml_codec import (
    pipeline_to_yaml, pipelines_to_yaml, yaml_to_pipelines,
    pipeline_to_dict, diff_contracts, snapshot_state,
)
import yaml

log = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

VALID_ROLES = ("admin", "operator", "viewer")


def require_role(caller: dict, *roles: str):
    """Raise 403 if the caller's role is not in the allowed roles."""
    if caller.get("role") not in roles:
        raise HTTPException(403, f"Requires role: {', '.join(roles)}")


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


class RegisterRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    email: str = ""

    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        if v not in ("admin", "operator", "viewer"):
            raise ValueError("Role must be admin, operator, or viewer")
        return v


class CommandRequest(BaseModel):
    text: str
    context: Optional[dict] = None
    session_id: Optional[str] = None


class ConnectionTestRequest(BaseModel):
    connector_id: str
    params: dict


class ProfileRequest(BaseModel):
    connector_id: str
    params: dict
    schema_name: str
    tables: Optional[list[str]] = None


class RegisterSourceRequest(BaseModel):
    display_name: str
    connector_id: str
    connection_params: dict = {}
    description: str = ""
    owner: str = ""
    tags: dict = {}

class UpdateSourceRequest(BaseModel):
    display_name: Optional[str] = None
    connection_params: Optional[dict] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[dict] = None


class ProposeRequest(BaseModel):
    connector_id: str
    params: dict
    schema_name: str
    tables: Optional[list[str]] = None


class CreatePipelineRequest(BaseModel):
    source_connector_id: str
    source_host: str = ""
    source_port: int = 0
    source_database: str = ""
    source_schema: str = ""
    source_table: str = ""
    source_user: Optional[str] = None
    source_password: Optional[str] = None
    target_connector_id: str = ""
    target_host: str = ""
    target_port: int = 0
    target_database: str = ""
    target_user: Optional[str] = None
    target_password: Optional[str] = None
    target_schema: str = "raw"
    strategy: dict = {}
    schedule_cron: str = "0 * * * *"
    tier: int = 2
    owner: Optional[str] = None
    tags: Optional[dict] = None
    environment: str = "production"
    column_mappings: Optional[list[dict]] = None
    auto_approve_additive: bool = False
    steps: Optional[list[dict]] = None
    schema_change_policy: Optional[dict] = None


class BatchCreateRequest(BaseModel):
    pipelines: list[CreatePipelineRequest]


class UpdatePipelineRequest(BaseModel):
    # Schedule
    schedule_cron: Optional[str] = None
    retry_max_attempts: Optional[int] = None
    retry_backoff_seconds: Optional[int] = None
    timeout_seconds: Optional[int] = None
    # Strategy
    refresh_type: Optional[str] = None
    replication_method: Optional[str] = None
    incremental_column: Optional[str] = None
    load_type: Optional[str] = None
    merge_keys: Optional[list[str]] = None
    last_watermark: Optional[str] = None
    reset_watermark: Optional[bool] = None
    # Quality
    quality_config: Optional[dict] = None
    # Observability
    tier: Optional[int] = None
    owner: Optional[str] = None
    tags: Optional[dict] = None
    tier_config: Optional[dict] = None
    freshness_column: Optional[str] = None
    # Approval
    auto_approve_additive_schema: Optional[bool] = None
    # Schema change policy (Build 12)
    schema_change_policy: Optional[dict] = None
    # Post-promotion hooks (Build 13)
    post_promotion_hooks: Optional[list[dict]] = None
    # Steps (Build 18)
    steps: Optional[list[dict]] = None
    # Context propagation (Build 28)
    auto_propagate_context: Optional[bool] = None
    # Audit
    reason: Optional[str] = None


class BackfillRequest(BaseModel):
    start: str
    end: str


class ApprovalRequest(BaseModel):
    action: str  # "approve" | "reject"
    note: Optional[str] = None


class GenerateConnectorRequest(BaseModel):
    connector_type: str  # "source" | "target"
    db_type: str
    params: dict


class DeclareDepRequest(BaseModel):
    pipeline_id: str
    depends_on_id: str
    notes: Optional[str] = None


class CreatePolicyRequest(BaseModel):
    policy_name: str
    description: Optional[str] = None
    channels: list[dict] = []
    digest_hour: int = 9


class UpdatePolicyRequest(BaseModel):
    policy_name: Optional[str] = None
    description: Optional[str] = None
    channels: Optional[list[dict]] = None
    digest_hour: Optional[int] = None


class SetPreferenceRequest(BaseModel):
    scope: str
    scope_value: Optional[str] = None
    preference_key: str
    preference_value: dict
    confidence: float = 1.0


class DiscoverySchemaParams(BaseModel):
    connector_id: str
    params: dict


# ---------------------------------------------------------------------------
# Build application
# ---------------------------------------------------------------------------

def create_app(
    config: Config,
    store: Store,
    registry: ConnectorRegistry,
    agent: AgentCore,
    conversation: ConversationManager,
    runner: PipelineRunner,
    scheduler: Scheduler,
    monitor: MonitorEngine,
    gitops=None,
) -> FastAPI:
    """Create and configure the FastAPI application with all routes."""

    app = FastAPI(title="Pipeline Agent", version="2.0.0")
    app.state.limiter = limiter

    # Rate limit error handler
    @app.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Please try again later."},
        )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_correlation(request: Request, call_next):
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        token = set_request_id(req_id)
        t0 = time.monotonic()
        try:
            response = await call_next(request)
            if request.url.path not in ("/api/health", "/health"):
                elapsed = int((time.monotonic() - t0) * 1000)
                log.info(
                    "%s %s %d (%dms)",
                    request.method, request.url.path,
                    response.status_code, elapsed,
                )
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            request_id_var.reset(token)

    auth_dep = AuthDependency(config)

    async def _log_pipeline_change(
        pipeline_id: str,
        pipeline_name: str,
        change_type: PipelineChangeType,
        caller: dict,
        changed_fields: dict = None,
        reason: str = "",
        source: str = "api",
        context: str = "",
    ):
        """Record a pipeline mutation in the changelog. Fail-safe."""
        try:
            _uid = caller.get("sub", "") if caller else ""
            _uname = ""
            if _uid and _uid not in ("anonymous", "api_key_user"):
                _u = await store.get_user(_uid)
                _uname = _u.username if _u else _uid
            else:
                _uname = _uid
            await store.save_pipeline_change(PipelineChangeLog(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                change_type=change_type,
                changed_by=_uname,
                changed_by_id=_uid,
                source=source,
                changed_fields=changed_fields or {},
                reason=reason,
                context=context,
            ))
        except Exception as e:
            log.warning("Failed to save pipeline changelog: %s", e)

    # -----------------------------------------------------------------------
    # Auth endpoints (no auth required for login)
    # -----------------------------------------------------------------------

    @app.post("/api/auth/login")
    @limiter.limit("20/minute")
    async def login(request: Request, req: LoginRequest = Body(...)):
        user = await store.get_user_by_username(req.username)
        if not user:
            raise HTTPException(401, "Invalid credentials")
        if not bcrypt.checkpw(
            req.password.encode("utf-8"),
            user.password_hash.encode("utf-8"),
        ):
            raise HTTPException(401, "Invalid credentials")
        token = create_token(
            user_id=user.user_id,
            secret=config.jwt_secret,
            algorithm=config.jwt_algorithm,
            expiry_hours=config.jwt_expiry_hours,
            role=user.role,
        )
        return {
            "token": token,
            "user_id": user.user_id,
            "username": user.username,
            "role": user.role,
        }

    @app.post("/api/auth/register")
    @limiter.limit("10/minute")
    async def register(
        request: Request,
        req: RegisterRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin")
        existing = await store.get_user_by_username(req.username)
        if existing:
            raise HTTPException(409, "Username already taken")
        hashed = bcrypt.hashpw(
            req.password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")
        user = User(
            username=req.username,
            password_hash=hashed,
            role=req.role,
            email=req.email,
        )
        await store.save_user(user)
        return {"user_id": user.user_id, "username": user.username, "role": user.role}

    @app.get("/api/auth/me")
    async def auth_me(caller: dict = Depends(auth_dep)):
        if not config.auth_enabled:
            return {
                "user_id": "anonymous",
                "username": "anonymous",
                "role": "admin",
                "email": "",
                "created_at": "",
            }
        user = await store.get_user(caller["sub"])
        if not user:
            raise HTTPException(404, "User not found")
        return {
            "user_id": user.user_id,
            "username": user.username,
            "role": user.role,
            "email": getattr(user, "email", ""),
            "created_at": user.created_at,
        }

    # -----------------------------------------------------------------------
    # Health & Metrics (no auth required)
    # -----------------------------------------------------------------------

    @app.get("/health")
    async def health():
        pipelines = await store.list_pipelines()
        active = [p for p in pipelines if p.status == PipelineStatus.ACTIVE]
        pg_ok = True
        try:
            pass
        except Exception:
            pg_ok = False
        return {
            "status": "ok",
            "auth_enabled": config.auth_enabled,
            "pipelines": len(pipelines),
            "active": len(active),
            "pg_connected": pg_ok,
        }

    @app.get("/metrics")
    async def prometheus_metrics():
        pipelines = await store.list_pipelines()
        lines = []
        for p in pipelines:
            label = f'pipeline="{p.pipeline_name}",tier="{p.tier}"'
            freshness = await store.get_latest_freshness(p.pipeline_id)
            if freshness:
                lines.append(
                    f"pipeline_agent_freshness_minutes{{{label}}} "
                    f"{freshness.staleness_minutes}"
                )
            runs = await store.list_runs(p.pipeline_id, limit=1)
            if runs:
                lines.append(
                    f"pipeline_agent_rows_loaded_total{{{label}}} "
                    f"{runs[0].rows_loaded}"
                )
        return PlainTextResponse(
            "\n".join(lines) + "\n", media_type="text/plain"
        )

    # -----------------------------------------------------------------------
    # Command (agent-routed)
    # -----------------------------------------------------------------------

    # In-memory conversation history + guided context per session
    _chat_sessions: dict[str, list[dict]] = {}
    _guided_contexts: dict[str, dict] = {}

    @app.post("/api/command")
    @limiter.limit("100/minute")
    async def command(
        request: Request,
        req: CommandRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        import time as _time
        _cmd_t0 = _time.monotonic()
        try:
            # Maintain conversation history per session
            session_id = req.session_id or "default"
            if session_id not in _chat_sessions:
                _chat_sessions[session_id] = []
            history = _chat_sessions[session_id]

            # Inject guided context + available sources into routing context
            guided_ctx = _guided_contexts.get(session_id, {})
            route_context = dict(req.context or {})
            if guided_ctx:
                route_context["guided_pipeline"] = guided_ctx

            # Load registered sources for agent awareness
            reg_sources = await store.list_registered_sources()
            if reg_sources:
                route_context["available_sources"] = [
                    {"name": s.display_name, "type": s.source_type, "id": s.source_id}
                    for s in reg_sources
                ]

            routed = await agent.route_command(req.text, route_context, history=history)
            action = routed.get("action", "unknown")
            params = routed.get("params", {})
            response_text = routed.get("response_text", "")

            # ----------------------------------------------------------
            # Helper: resolve a connector by id, name, or type keyword
            # ----------------------------------------------------------
            async def _resolve_connector(cid, ctype, role=None):
                """Return a real connector_id UUID, or '' if unresolved."""
                all_conns = await store.list_connectors(status="active")
                for c in all_conns:
                    if role and c.connector_type.value != role:
                        continue
                    if cid and (c.connector_id == cid or c.connector_name == cid):
                        return c.connector_id
                    if ctype and (ctype in c.source_target_type.lower() or ctype in c.connector_name.lower()):
                        return c.connector_id
                return ""

            # ----------------------------------------------------------
            # Helper: resolve registered source from user text or params
            # ----------------------------------------------------------
            async def _resolve_registered_source() -> Optional[RegisteredSource]:
                """Try to find a registered source from params or user text."""
                # Check explicit source_name param
                sn = params.get("source_name", "")
                if sn:
                    rs = await store.get_registered_source_by_name(sn)
                    if rs:
                        return rs
                # Fuzzy match user text against registered source display names
                _text_lower = req.text.lower()
                for rs in reg_sources:
                    if rs.display_name.lower() in _text_lower:
                        return rs
                # Match source_type from params
                _st = params.get("connector_type", "").lower()
                if _st:
                    for rs in reg_sources:
                        if rs.source_type == _st:
                            return rs
                return None

            # ----------------------------------------------------------
            # Execute action → collect result_data → let Claude respond
            # ----------------------------------------------------------
            result_data = {}
            use_conversational = True  # Let Claude craft the response

            if action == "list_pipelines":
                pipelines = await store.list_pipelines()
                result_data = {
                    "pipelines": [
                        {
                            "name": p.pipeline_name,
                            "source": f"{p.source_schema}.{p.source_table}",
                            "target": f"{p.target_schema}.{p.target_table}",
                            "status": p.status.value,
                            "tier": p.tier,
                            "refresh_type": p.refresh_type.value,
                            "schedule": p.schedule_cron,
                        }
                        for p in pipelines
                    ],
                    "fallback_text": f"Found {len(pipelines)} pipeline(s)." if pipelines else "No pipelines yet.",
                }

            elif action == "list_connectors":
                conn_type = params.get("type")
                connectors = await store.list_connectors(
                    connector_type=conn_type, status="active"
                )
                conn_list = [
                    {
                        "name": c.connector_name,
                        "type": c.connector_type.value,
                        "database_type": c.source_target_type,
                    }
                    for c in connectors
                ]
                sources = [c for c in conn_list if c["type"] == "source"]
                targets = [c for c in conn_list if c["type"] == "target"]
                lines = [f"Found {len(connectors)} connector(s):\n"]
                if sources:
                    lines.append("**Sources:**")
                    for c in sources:
                        lines.append(f"  • {c['name']} ({c['database_type']})")
                if targets:
                    lines.append("**Targets:**")
                    for c in targets:
                        lines.append(f"  • {c['name']} ({c['database_type']})")
                result_data = {
                    "connectors": conn_list,
                    "fallback_text": "\n".join(lines),
                }

            elif action == "discover_tables":
                connector_id = await _resolve_connector(
                    params.get("connector_id", ""),
                    params.get("connector_type", "").lower(),
                )
                database = params.get("database", "")

                # Try resolving from registered sources
                _reg = await _resolve_registered_source()
                if _reg:
                    connector_id = connector_id or await _resolve_connector(_reg.connector_id, "", role="source")
                    cp = _reg.connection_params
                    database = database or cp.get("database", "")
                    for k in ("host", "port", "user", "password"):
                        if k not in params or not params[k]:
                            params[k] = cp.get(k, "")

                if not connector_id:
                    # Show registered sources instead of raw connectors
                    if reg_sources:
                        result_data = {
                            "status": "need_connector",
                            "available_sources": [
                                {"name": s.display_name, "type": s.source_type, "description": s.description}
                                for s in reg_sources
                            ],
                            "fallback_text": "Which data source would you like to explore?",
                        }
                    else:
                        src_conns = await store.list_connectors(connector_type="source", status="active")
                        result_data = {
                            "status": "need_connector",
                            "available_sources": [{"name": c.connector_name, "type": c.source_target_type} for c in src_conns],
                            "fallback_text": "Which source database type would you like to connect to?",
                        }
                elif not database and not params.get("host"):
                    conn_rec = await store.get_connector(connector_id)
                    src_type = conn_rec.source_target_type if conn_rec else "database"
                    result_data = {
                        "status": "need_connection_details",
                        "connector_type": src_type,
                        "fallback_text": f"I found the {src_type} connector. What are the connection details?",
                    }
                else:
                    conn_params = {"database": database}
                    for k in ("host", "port", "user", "password"):
                        if k in params:
                            conn_params[k] = params[k]
                    try:
                        schemas = await conversation.list_schemas(connector_id, conn_params)
                        result_data = {
                            "status": "discovered",
                            "schemas": schemas,
                            "database": database,
                            "fallback_text": f"Found {sum(s['table_count'] for s in schemas)} table(s).",
                        }
                        # Cache discovery on the registered source
                        if _reg:
                            await store.update_source_schema_cache(_reg.source_id, {"schemas": schemas})
                    except Exception as e:
                        result_data = {"status": "error", "error": str(e), "fallback_text": f"Discovery failed: {e}"}

            elif action == "profile_table":
                connector_id = await _resolve_connector(
                    params.get("connector_id", ""),
                    params.get("connector_type", "").lower(),
                )
                database = params.get("database", "")
                schema_name = params.get("schema", "main")
                table_name = params.get("table", "")

                # Resolve from registered sources
                _reg = await _resolve_registered_source()
                if _reg:
                    connector_id = connector_id or await _resolve_connector(_reg.connector_id, "", role="source")
                    cp = _reg.connection_params
                    database = database or cp.get("database", "")
                    # For MySQL/MariaDB, schema = database name
                    if schema_name == "main":
                        _cached_schema = None
                        if _reg.schema_cache and "schemas" in _reg.schema_cache:
                            schemas = _reg.schema_cache["schemas"]
                            if schemas:
                                _cached_schema = schemas[0].get("schema_name")
                        schema_name = cp.get("schema") or _cached_schema or database or schema_name
                    for k in ("host", "port", "user", "password"):
                        if k not in params or not params[k]:
                            params[k] = cp.get(k, "")

                if not connector_id:
                    result_data = {"status": "need_connector", "fallback_text": "Which source database type?"}
                elif not table_name:
                    result_data = {"status": "need_table", "fallback_text": "Which table would you like me to profile?"}
                elif not database and not params.get("host"):
                    result_data = {"status": "need_connection", "fallback_text": "I need the database connection details."}
                else:
                    conn_params = {"database": database}
                    for k in ("host", "port", "user", "password"):
                        if k in params:
                            conn_params[k] = params[k]
                    try:
                        profiles = await conversation.profile_tables(
                            connector_id, conn_params, schema_name, [table_name]
                        )
                        if profiles and "error" not in profiles[0]:
                            result_data = {"status": "profiled", "profile": profiles[0], "fallback_text": "Profile complete."}
                        else:
                            result_data = {"status": "error", "error": profiles[0].get("error", "unknown") if profiles else "no data", "fallback_text": "Profiling failed."}
                    except Exception as e:
                        result_data = {"status": "error", "error": str(e), "fallback_text": f"Profiling failed: {e}"}

            elif action == "propose_strategy":
                connector_id = await _resolve_connector(
                    params.get("connector_id", ""),
                    params.get("connector_type", "").lower(),
                )
                database = params.get("database", "")
                schema_name = params.get("schema", "main")
                table_name = params.get("table", "")

                # Resolve from registered sources
                _reg = await _resolve_registered_source()
                if _reg:
                    connector_id = connector_id or await _resolve_connector(_reg.connector_id, "", role="source")
                    cp = _reg.connection_params
                    database = database or cp.get("database", "")
                    if schema_name == "main":
                        _cached_schema = None
                        if _reg.schema_cache and "schemas" in _reg.schema_cache:
                            schemas = _reg.schema_cache["schemas"]
                            if schemas:
                                _cached_schema = schemas[0].get("schema_name")
                        schema_name = cp.get("schema") or _cached_schema or database or schema_name
                    for k in ("host", "port", "user", "password"):
                        if k not in params or not params[k]:
                            params[k] = cp.get(k, "")

                if not connector_id or not table_name:
                    result_data = {"status": "need_info", "fallback_text": "I need the connector and table name."}
                else:
                    conn_params = {"database": database}
                    for k in ("host", "port", "user", "password"):
                        if k in params:
                            conn_params[k] = params[k]
                    try:
                        proposals = await conversation.propose_strategies(
                            connector_id, conn_params, schema_name, [table_name]
                        )
                        if proposals and "error" not in proposals[0]:
                            result_data = {"status": "proposed", "proposal": proposals[0], "fallback_text": "Strategy proposed."}
                        else:
                            result_data = {"status": "error", "error": proposals[0].get("error", "unknown") if proposals else "no data", "fallback_text": "Strategy failed."}
                    except Exception as e:
                        result_data = {"status": "error", "error": str(e), "fallback_text": f"Strategy failed: {e}"}

            elif action == "create_pipeline":
                src_connector_type = params.get("source_connector_type", "").lower()
                src_database = params.get("source_database", "")
                src_schema = params.get("source_schema", "main")
                src_table = params.get("source_table", "")
                tgt_connector_type = params.get("target_connector_type", "").lower()
                tgt_host = params.get("target_host", "localhost")
                tgt_port = params.get("target_port", 5432)
                tgt_database = params.get("target_database", "")
                tgt_user = params.get("target_user", "")
                tgt_password = params.get("target_password", "")
                tgt_schema = params.get("target_schema", "raw")
                schedule = params.get("schedule_cron", "0 * * * *")

                # Parse natural language schedule
                import re as _re
                if schedule and not _re.match(r"^[\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+ [\d\*/,-]+$", schedule):
                    parsed = await agent.parse_schedule(schedule)
                    schedule = parsed["cron"]

                # Try resolving from registered source if params mention one
                _source_name = params.get("source_name", "")
                _reg_src = None
                if _source_name:
                    _reg_src = await store.get_registered_source_by_name(_source_name)
                if not _reg_src and guided_ctx.get("source_id"):
                    _reg_src = await store.get_registered_source(guided_ctx["source_id"])
                if not _reg_src:
                    _reg_src_fuzzy = await _resolve_registered_source()
                    if _reg_src_fuzzy:
                        _reg_src = _reg_src_fuzzy
                        _source_name = _source_name or _reg_src.display_name

                # If we have a registered source, populate connection details
                if _reg_src:
                    src_connector_type = src_connector_type or _reg_src.source_type
                    cp = _reg_src.connection_params
                    if not src_database:
                        src_database = cp.get("database", "")
                    # Resolve schema from cache or database name
                    if src_schema == "main":
                        _cached_schema = None
                        if _reg_src.schema_cache and "schemas" in _reg_src.schema_cache:
                            _schemas = _reg_src.schema_cache["schemas"]
                            if _schemas:
                                _cached_schema = _schemas[0].get("schema_name")
                        src_schema = cp.get("schema") or _cached_schema or src_database or src_schema
                    if not params.get("source_host"):
                        params["source_host"] = cp.get("host", "localhost")
                    if not params.get("source_port"):
                        params["source_port"] = cp.get("port", 0)
                    if not params.get("source_user"):
                        params["source_user"] = cp.get("user", "")
                    if not params.get("source_password"):
                        params["source_password"] = cp.get("password", "")

                src_connector_id = await _resolve_connector(
                    params.get("source_connector_id", ""), src_connector_type, role="source"
                )
                tgt_connector_id = await _resolve_connector(
                    params.get("target_connector_id", ""), tgt_connector_type, role="target"
                )

                # Guided mode: when info is missing, accumulate context
                # instead of generic error messages
                _missing = []
                if not src_connector_id:
                    _missing.append("source")
                if not src_table:
                    _missing.append("table")
                if not tgt_connector_id:
                    # Auto-default to postgres-target-v1 (local PG)
                    tgt_connector_id = await _resolve_connector("", "postgres", role="target")
                if not tgt_database:
                    tgt_database = "pipeline_agent"  # local default

                if _missing:
                    # Enter/update guided context
                    _guided_contexts[session_id] = {
                        "mode": "pipeline_creation",
                        "missing": _missing,
                        "gathered": {
                            k: v for k, v in {
                                "source_type": src_connector_type,
                                "source_name": _source_name,
                                "source_id": _reg_src.source_id if _reg_src else "",
                                "database": src_database,
                                "schema": src_schema,
                                "table": src_table,
                                "schedule": schedule,
                            }.items() if v
                        },
                    }
                    # Build guided response with available sources
                    _src_list = [
                        {"display_name": s.display_name, "source_type": s.source_type, "description": s.description}
                        for s in reg_sources
                    ]
                    result_data = {
                        "status": "guided",
                        "missing": _missing,
                        "gathered": _guided_contexts[session_id]["gathered"],
                        "available_sources": _src_list,
                        "fallback_text": "Let me help you set up that pipeline. Which data source would you like to use?"
                            if "source" in _missing else
                            f"Great, using {_source_name or src_connector_type}. Which table do you need?",
                    }
                    use_conversational = True  # Let agent craft guided response
                else:
                    try:
                        src_params = {"database": src_database}
                        for k in ("host", "port", "user", "password"):
                            sk = f"source_{k}"
                            if sk in params:
                                src_params[k] = params[sk]

                        proposals = await conversation.propose_strategies(
                            src_connector_id, src_params, src_schema, [src_table]
                        )
                        if not proposals or "error" in proposals[0]:
                            result_data = {"status": "error", "error": proposals[0].get("error", "unknown") if proposals else "no data", "fallback_text": "Failed to profile source table."}
                        else:
                            strat = proposals[0]["strategy"]
                            strat["source_schema"] = src_schema
                            strat["source_table"] = src_table
                            strat["target_schema"] = tgt_schema

                            profiles = await conversation.profile_tables(
                                src_connector_id, src_params, src_schema, [src_table]
                            )
                            if profiles and "columns" in profiles[0]:
                                strat["column_mappings"] = profiles[0]["columns"]

                            tgt_params = {
                                "host": tgt_host, "port": tgt_port,
                                "database": tgt_database, "user": tgt_user,
                                "password": tgt_password, "default_schema": tgt_schema,
                            }

                            pipeline = await conversation.create_pipeline(
                                strategy=strat,
                                source_connector_id=src_connector_id,
                                target_connector_id=tgt_connector_id,
                                source_params=src_params,
                                target_params=tgt_params,
                                schedule=schedule,
                            )
                            result_data = {
                                "status": "created",
                                "pipeline": {
                                    "name": pipeline.pipeline_name,
                                    "id": pipeline.pipeline_id[:8],
                                    "source": f"{pipeline.source_schema}.{pipeline.source_table}",
                                    "target": f"{pipeline.target_schema}.{pipeline.target_table}",
                                    "refresh_type": pipeline.refresh_type.value,
                                    "load_type": pipeline.load_type.value,
                                    "schedule": pipeline.schedule_cron,
                                    "tier": pipeline.tier,
                                    "columns": len(strat.get("column_mappings", [])),
                                },
                                "strategy": {
                                    "refresh_type": strat.get("refresh_type"),
                                    "load_type": strat.get("load_type"),
                                    "incremental_column": strat.get("incremental_column"),
                                    "merge_keys": strat.get("merge_keys"),
                                },
                                "fallback_text": f"Pipeline {pipeline.pipeline_name} created successfully!",
                            }
                            await _log_pipeline_change(
                                pipeline.pipeline_id, pipeline.pipeline_name,
                                PipelineChangeType.CREATED, caller,
                                source="chat",
                                changed_fields={
                                    "source": f"{pipeline.source_schema}.{pipeline.source_table}",
                                    "target": f"{pipeline.target_schema}.{pipeline.target_table}",
                                    "schedule": pipeline.schedule_cron,
                                },
                                context=f"Created via chat: {req.text}",
                            )
                    except Exception as e:
                        log.exception("Pipeline creation error")
                        result_data = {"status": "error", "error": str(e), "fallback_text": f"Pipeline creation failed: {e}"}

            elif action == "check_freshness":
                use_conversational = False
                pipelines = await store.list_pipelines()
                lines = ["Freshness Report:\n"]
                for p in pipelines:
                    last_run = await store.get_last_successful_run(p.pipeline_id)
                    if last_run:
                        lines.append(f"  {p.pipeline_name}: last successful run at {last_run.completed_at}")
                    else:
                        lines.append(f"  {p.pipeline_name}: no successful runs yet")
                response_text = "\n".join(lines) if len(lines) > 1 else "No pipelines to check."

            elif action == "trigger_run":
                use_conversational = False
                pipelines = await store.list_pipelines()
                query_text = (params.get("query") or params.get("pipeline_name") or req.text).lower()
                target_pipeline = None
                for p in pipelines:
                    if p.pipeline_name.lower() in query_text or p.pipeline_id[:8] in query_text:
                        target_pipeline = p
                        break
                if not target_pipeline and len(pipelines) == 1:
                    target_pipeline = pipelines[0]
                if target_pipeline:
                    run = await scheduler.trigger(target_pipeline.pipeline_id)
                    response_text = f"Triggered run for {target_pipeline.pipeline_name} (run_id: {run.run_id[:8]}). Check the Pipelines view for progress."
                else:
                    names = [p.pipeline_name for p in pipelines]
                    response_text = f"Which pipeline? Available: {', '.join(names)}" if names else "No pipelines to trigger."

            elif action == "check_status":
                pipelines = await store.list_pipelines()
                if pipelines:
                    status_data = []
                    for p in pipelines:
                        runs = await store.list_runs(p.pipeline_id, limit=1)
                        last = runs[0] if runs else None
                        status_data.append({
                            "name": p.pipeline_name,
                            "status": p.status.value,
                            "last_run": f"{last.status.value} ({last.rows_extracted} rows)" if last else "no runs",
                        })
                    result_data = {"pipelines": status_data, "fallback_text": "Status retrieved."}
                else:
                    result_data = {"pipelines": [], "fallback_text": "No pipelines configured yet."}

            elif action == "list_alerts":
                alerts = await store.list_alerts()
                result_data = {
                    "alerts": [
                        {"severity": a.severity.value, "pipeline": a.pipeline_name, "summary": a.summary}
                        for a in alerts
                    ],
                    "fallback_text": f"{len(alerts)} alert(s)." if alerts else "No alerts. All systems healthy.",
                }

            elif action == "design_topology":
                desc = params.get("description", req.text)
                # Gather existing state for context
                existing_p = await store.list_pipelines()
                existing_c = await store.list_connectors(status="active")
                topology = await agent.design_topology(
                    desc,
                    existing_pipelines=[_pipeline_summary(p) for p in existing_p],
                    existing_connectors=[
                        {"connector_name": c.connector_name, "connector_type": c.connector_type.value, "source_target_type": c.source_target_type}
                        for c in existing_c
                    ],
                )
                # Format for display
                lines = []
                if topology.get("summary"):
                    lines.append(f"**Architecture:** {topology['summary']}")
                if topology.get("pattern"):
                    lines.append(f"**Pattern:** {topology['pattern']}")
                if topology.get("pipelines"):
                    lines.append(f"\n**Proposed Pipelines ({len(topology['pipelines'])}):**")
                    for i, pp in enumerate(topology["pipelines"], 1):
                        lines.append(f"  {i}. **{pp.get('name', 'unnamed')}** — {pp.get('description', '')}")
                        lines.append(f"     {pp.get('source_type', '?')}.{pp.get('source_detail', '?')} -> {pp.get('target_type', '?')}.{pp.get('target_detail', '?')}")
                        lines.append(f"     Schedule: {pp.get('schedule_cron', '?')} | {pp.get('refresh_type', '?')} | T{pp.get('tier', 2)}")
                if topology.get("dependencies"):
                    lines.append(f"\n**Dependencies ({len(topology['dependencies'])}):**")
                    for dep in topology["dependencies"]:
                        lines.append(f"  {dep.get('from', '?')} -> {dep.get('to', '?')} ({dep.get('type', '?')})")
                if topology.get("contracts"):
                    lines.append(f"\n**Data Contracts ({len(topology['contracts'])}):**")
                    for ct in topology["contracts"]:
                        lines.append(f"  {ct.get('producer', '?')} -> {ct.get('consumer', '?')} (SLA: {ct.get('freshness_sla_minutes', '?')}m)")
                if topology.get("reasoning"):
                    lines.append(f"\n**Reasoning:** {topology['reasoning']}")

                result_data = {
                    "topology": topology,
                    "fallback_text": "\n".join(lines) if lines else "Could not generate topology.",
                }

            # Build 24: Diagnostic & Reasoning actions
            elif action == "diagnose_pipeline":
                # Resolve pipeline by name/id from params or user text
                query = params.get("pipeline_name", params.get("pipeline_id", params.get("query", req.text)))
                target_p = await _resolve_pipeline(query, store)
                if target_p:
                    diagnosis = await agent.diagnose_pipeline(target_p.pipeline_id)
                    lines = [f"**Diagnosis for {target_p.pipeline_name}:**"]
                    lines.append(f"**Root cause:** {diagnosis.get('root_cause', 'Unknown')}")
                    lines.append(f"**Category:** {diagnosis.get('category', 'unknown')}")
                    if diagnosis.get("evidence"):
                        lines.append("**Evidence:**")
                        for ev in diagnosis["evidence"][:5]:
                            lines.append(f"  - {ev}")
                    if diagnosis.get("recommended_actions"):
                        lines.append("**Recommended actions:**")
                        for ra in diagnosis["recommended_actions"][:3]:
                            lines.append(f"  - [{ra.get('priority', '?')}] {ra.get('action', '')}")
                    if diagnosis.get("summary"):
                        lines.append(f"\n{diagnosis['summary']}")
                    result_data = {"diagnosis": diagnosis, "fallback_text": "\n".join(lines)}
                else:
                    result_data = {"fallback_text": "Could not identify which pipeline to diagnose. Please specify the pipeline name."}

            elif action == "analyze_impact":
                query = params.get("pipeline_name", params.get("pipeline_id", params.get("query", req.text)))
                target_p = await _resolve_pipeline(query, store)
                if target_p:
                    impact = await agent.analyze_impact(target_p.pipeline_id)
                    lines = [f"**Impact Analysis for {target_p.pipeline_name}:**"]
                    lines.append(f"**Severity:** {impact.get('impact_severity', 'unknown')}")
                    br = impact.get("blast_radius", {})
                    lines.append(f"**Blast radius:** {br.get('pipelines', 0)} pipelines, {br.get('contracts', 0)} contracts")
                    affected = impact.get("affected_pipelines", [])
                    if affected:
                        lines.append(f"**Affected pipelines ({len(affected)}):**")
                        for ap in affected[:10]:
                            lines.append(f"  - {ap.get('pipeline_name', '?')} (depth={ap.get('depth', '?')}, {ap.get('impact_type', '?')})")
                    if impact.get("mitigation_options"):
                        lines.append("**Mitigation options:**")
                        for m in impact["mitigation_options"][:3]:
                            lines.append(f"  - {m.get('option', '')} (effort: {m.get('effort', '?')})")
                    if impact.get("summary"):
                        lines.append(f"\n{impact['summary']}")
                    result_data = {"impact": impact, "fallback_text": "\n".join(lines)}
                else:
                    result_data = {"fallback_text": "Could not identify which pipeline to analyze. Please specify the pipeline name."}

            elif action == "check_anomalies":
                anomalies = await agent.reason_about_anomalies()
                lines = [f"**Platform Health:** {anomalies.get('platform_health', 'unknown')}"]
                for a in anomalies.get("anomalies", []):
                    sev = a.get("severity", "?")
                    lines.append(f"- **{a.get('pipeline_name', '?')}** [{sev}]: {a.get('observation', '')}")
                    if a.get("reasoning"):
                        lines.append(f"  _{a['reasoning']}_")
                if anomalies.get("cross_pipeline_patterns"):
                    lines.append("\n**Cross-pipeline patterns:**")
                    for pat in anomalies["cross_pipeline_patterns"]:
                        lines.append(f"  - {pat}")
                if anomalies.get("summary"):
                    lines.append(f"\n{anomalies['summary']}")
                result_data = {"anomalies": anomalies, "fallback_text": "\n".join(lines)}

            elif action == "suggest_metrics":
                # Agent suggests KPI metrics for a pipeline
                _pid = params.get("pipeline_id", "")
                _p = await store.get_pipeline(_pid) if _pid else None
                if _p:
                    _cols = _p.column_mappings if _p.column_mappings else []
                    suggestions = await agent.suggest_metrics(_p, _cols, _p.business_context or {})
                    result_data = {"pipeline_id": _pid, "pipeline_name": _p.pipeline_name,
                                   "suggestions": suggestions,
                                   "fallback_text": f"Here are suggested metrics for {_p.pipeline_name}."}
                else:
                    result_data = {"fallback_text": "Please specify which pipeline to suggest metrics for."}

            elif action == "interpret_metric_trend":
                # Agent interprets a metric's trend
                _mid = params.get("metric_id", "")
                _m = await store.get_metric(_mid) if _mid else None
                if _m:
                    _snaps = await store.list_metric_snapshots(_mid, limit=50)
                    _snap_dicts = [{"computed_at": s.computed_at, "value": s.value} for s in _snaps]
                    _p = await store.get_pipeline(_m.pipeline_id)
                    _pctx = {"pipeline_name": _p.pipeline_name, "target_table": _p.target_table} if _p else {}
                    analysis = await agent.interpret_metric_trend(_m.metric_name, _snap_dicts, _pctx)
                    result_data = {"metric_id": _mid, "metric_name": _m.metric_name, **analysis,
                                   "fallback_text": f"Trend analysis for {_m.metric_name}."}
                else:
                    result_data = {"fallback_text": "Please specify which metric to analyze."}

            elif action == "explain":
                # explain is inherently conversational — pass through
                result_data = {
                    "topic": params.get("topic", req.text),
                    "fallback_text": routed.get("response_text", ""),
                }

            elif action == "unknown":
                result_data = {
                    "user_text": req.text,
                    "fallback_text": routed.get("response_text", response_text),
                }

            # ----------------------------------------------------------
            # Generate conversational response via Claude
            # ----------------------------------------------------------
            _is_guided = result_data.get("status") == "guided"
            if use_conversational and result_data:
                try:
                    if _is_guided:
                        # Use guided response for analyst-friendly language
                        _src_info = [
                            {"display_name": s.display_name, "source_type": s.source_type, "description": s.description}
                            for s in reg_sources
                        ]
                        response_text = await agent.guided_pipeline_response(
                            req.text, _guided_contexts.get(session_id, {}),
                            result_data, available_sources=_src_info, history=history,
                        )
                    else:
                        response_text = await agent.conversational_response(
                            req.text, action, result_data, history=history,
                        )
                except Exception as e:
                    log.warning("Conversational response failed, using fallback: %s", e)
                    response_text = result_data.get("fallback_text", response_text)

            # Clear guided context when pipeline is created or user exits
            if result_data.get("status") == "created" and session_id in _guided_contexts:
                del _guided_contexts[session_id]

            # Save to conversation history
            history.append({"role": "user", "text": req.text})
            history.append({"role": "assistant", "text": response_text})
            # Keep last 20 messages
            if len(history) > 20:
                _chat_sessions[session_id] = history[-20:]

            # Persist interaction for audit + training
            _cmd_latency = int((_time.monotonic() - _cmd_t0) * 1000)
            try:
                from contracts.models import ChatInteraction
                _user_id = caller.get("sub", "") if caller else ""
                _username = ""
                if _user_id and _user_id not in ("anonymous", "api_key_user"):
                    _user_obj = await store.get_user(_user_id)
                    _username = _user_obj.username if _user_obj else _user_id
                else:
                    _username = _user_id
                ci = ChatInteraction(
                    session_id=session_id,
                    user_id=_user_id,
                    username=_username,
                    user_input=req.text,
                    routed_action=action,
                    action_params=params,
                    agent_response=response_text,
                    result_data=result_data,
                    input_tokens=agent._req_input_tokens,
                    output_tokens=agent._req_output_tokens,
                    latency_ms=_cmd_latency,
                    model=config.model if config.has_api_key else "rule-based",
                )
                await store.save_chat_interaction(ci)
            except Exception as log_err:
                log.warning("Failed to save chat interaction: %s", log_err)

            return {"response": response_text}
        except Exception as e:
            log.exception("Command routing error")
            raise HTTPException(500, f"Command failed: {str(e)}")

    # -----------------------------------------------------------------------
    # Chat Interaction Audit Log
    # -----------------------------------------------------------------------

    @app.get("/api/interactions")
    @limiter.limit("100/minute")
    async def list_interactions(
        request: Request,
        session_id: str = Query(default=None),
        username: str = Query(default=None),
        limit: int = Query(default=50, le=500),
        offset: int = Query(default=0),
        caller: dict = Depends(auth_dep),
    ):
        """List chat interactions for auditing and training data export.
        Admin only — returns full interaction logs with user input, agent response,
        routed action, token usage, and latency."""
        if caller and caller.get("role") != "admin":
            raise HTTPException(403, "Admin access required")
        interactions = await store.list_chat_interactions(
            session_id=session_id, username=username,
            limit=limit, offset=offset,
        )
        total = await store.count_chat_interactions(
            session_id=session_id, username=username,
        )
        return {
            "interactions": [
                {
                    "interaction_id": ci.interaction_id,
                    "session_id": ci.session_id,
                    "user_id": ci.user_id,
                    "username": ci.username,
                    "user_input": ci.user_input,
                    "routed_action": ci.routed_action,
                    "action_params": ci.action_params,
                    "agent_response": ci.agent_response,
                    "result_data": ci.result_data,
                    "input_tokens": ci.input_tokens,
                    "output_tokens": ci.output_tokens,
                    "latency_ms": ci.latency_ms,
                    "model": ci.model,
                    "error": ci.error,
                    "created_at": ci.created_at,
                }
                for ci in interactions
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    @app.get("/api/interactions/export")
    @limiter.limit("10/minute")
    async def export_interactions(
        request: Request,
        session_id: str = Query(default=None),
        username: str = Query(default=None),
        limit: int = Query(default=1000, le=10000),
        caller: dict = Depends(auth_dep),
    ):
        """Export interactions as JSONL for training data.
        Each line is a JSON object with user_input and agent_response."""
        if caller and caller.get("role") != "admin":
            raise HTTPException(403, "Admin access required")
        interactions = await store.list_chat_interactions(
            session_id=session_id, username=username, limit=limit,
        )
        lines = []
        for ci in interactions:
            lines.append(json.dumps({
                "user_input": ci.user_input,
                "routed_action": ci.routed_action,
                "action_params": ci.action_params,
                "agent_response": ci.agent_response,
                "model": ci.model,
                "latency_ms": ci.latency_ms,
                "created_at": ci.created_at,
            }))
        return PlainTextResponse(
            "\n".join(lines) + "\n" if lines else "",
            media_type="application/jsonl",
        )

    # -----------------------------------------------------------------------
    # Source Registry (admin-registered named connections)
    # -----------------------------------------------------------------------

    @app.post("/api/sources")
    @limiter.limit("100/minute")
    async def register_source(
        request: Request,
        req: RegisterSourceRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Register a named data source. Admin only."""
        require_role(caller, "admin")
        # Validate connector exists
        connector = await store.get_connector(req.connector_id)
        if not connector:
            raise HTTPException(404, f"Connector {req.connector_id} not found")
        # Check duplicate name
        existing = await store.get_registered_source_by_name(req.display_name)
        if existing:
            raise HTTPException(409, f"Source '{req.display_name}' already exists")

        src = RegisteredSource(
            display_name=req.display_name,
            connector_id=req.connector_id,
            connector_name=connector.connector_name,
            source_type=connector.source_target_type.lower(),
            connection_params=req.connection_params,
            description=req.description,
            owner=req.owner,
            tags=req.tags,
        )
        await store.save_registered_source(src)
        return {
            "source_id": src.source_id,
            "display_name": src.display_name,
            "source_type": src.source_type,
            "connector_name": src.connector_name,
            "description": src.description,
        }

    @app.get("/api/sources")
    @limiter.limit("100/minute")
    async def list_sources(
        request: Request,
        source_type: str = Query(default=None),
        caller: dict = Depends(auth_dep),
    ):
        """List registered data sources. All roles. Credentials are masked."""
        sources = await store.list_registered_sources(source_type=source_type)
        return {
            "sources": [
                {
                    "source_id": s.source_id,
                    "display_name": s.display_name,
                    "source_type": s.source_type,
                    "connector_name": s.connector_name,
                    "description": s.description,
                    "owner": s.owner,
                    "tags": s.tags,
                    "has_credentials": bool(s.connection_params),
                    "has_schema_cache": bool(s.schema_cache),
                    "schema_cache_updated_at": s.schema_cache_updated_at,
                    "created_at": s.created_at,
                }
                for s in sources
            ],
        }

    @app.get("/api/sources/{source_id}")
    @limiter.limit("100/minute")
    async def get_source(
        request: Request,
        source_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get source details. Credentials masked for non-admin."""
        src = await store.get_registered_source(source_id)
        if not src:
            raise HTTPException(404, "Source not found")
        result = {
            "source_id": src.source_id,
            "display_name": src.display_name,
            "source_type": src.source_type,
            "connector_id": src.connector_id,
            "connector_name": src.connector_name,
            "description": src.description,
            "owner": src.owner,
            "tags": src.tags,
            "has_credentials": bool(src.connection_params),
            "schema_cache": src.schema_cache if src.schema_cache else None,
            "schema_cache_updated_at": src.schema_cache_updated_at,
            "created_at": src.created_at,
        }
        # Only admin sees connection params
        if caller and caller.get("role") == "admin":
            # Mask passwords in response
            masked = dict(src.connection_params)
            for k in ("password", "secret", "api_key", "token"):
                if k in masked and masked[k]:
                    masked[k] = "***"
            result["connection_params"] = masked
        return result

    @app.patch("/api/sources/{source_id}")
    @limiter.limit("100/minute")
    async def update_source(
        request: Request,
        source_id: str,
        req: UpdateSourceRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Update a registered source. Admin only."""
        require_role(caller, "admin")
        src = await store.get_registered_source(source_id)
        if not src:
            raise HTTPException(404, "Source not found")
        if req.display_name is not None:
            src.display_name = req.display_name
        if req.connection_params is not None:
            src.connection_params = req.connection_params
        if req.description is not None:
            src.description = req.description
        if req.owner is not None:
            src.owner = req.owner
        if req.tags is not None:
            src.tags = req.tags
        src.updated_at = now_iso()
        await store.save_registered_source(src)
        return {"status": "updated", "source_id": source_id}

    @app.delete("/api/sources/{source_id}")
    @limiter.limit("100/minute")
    async def delete_source(
        request: Request,
        source_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Delete a registered source. Admin only."""
        require_role(caller, "admin")
        await store.delete_registered_source(source_id)
        return {"status": "deleted"}

    @app.post("/api/sources/{source_id}/discover")
    @limiter.limit("30/minute")
    async def discover_source(
        request: Request,
        source_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Discover schemas and tables from a registered source. Caches result."""
        require_role(caller, "admin", "operator")
        src = await store.get_registered_source(source_id)
        if not src:
            raise HTTPException(404, "Source not found")
        try:
            schemas = await conversation.list_schemas(src.connector_id, src.connection_params)
            cache = {"schemas": schemas}
            await store.update_source_schema_cache(source_id, cache)
            return {"source_id": source_id, "schemas": schemas}
        except Exception as e:
            raise HTTPException(500, f"Discovery failed: {e}")

    # -----------------------------------------------------------------------
    # Connection & Discovery
    # -----------------------------------------------------------------------

    @app.post("/api/connection/test-source")
    @limiter.limit("100/minute")
    async def test_source(
        request: Request,
        req: ConnectionTestRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        return await conversation.test_source_connection(
            req.connector_id, req.params
        )

    @app.post("/api/connection/test-target")
    @limiter.limit("100/minute")
    async def test_target(
        request: Request,
        req: ConnectionTestRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        return await conversation.test_target_connection(
            req.connector_id, req.params
        )

    @app.get("/api/discovery/schemas")
    @limiter.limit("100/minute")
    async def list_schemas(
        request: Request,
        connector_id: str = Query(...),
        caller: dict = Depends(auth_dep),
    ):
        return await conversation.list_schemas(connector_id, request.query_params)

    @app.post("/api/discovery/profile")
    @limiter.limit("100/minute")
    async def profile_tables(
        request: Request,
        req: ProfileRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        return await conversation.profile_tables(
            req.connector_id, req.params, req.schema_name, req.tables
        )

    @app.post("/api/discovery/propose")
    @limiter.limit("100/minute")
    async def propose_strategies(
        request: Request,
        req: ProposeRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        return await conversation.propose_strategies(
            req.connector_id, req.params, req.schema_name, req.tables
        )

    # -----------------------------------------------------------------------
    # Connectors
    # -----------------------------------------------------------------------

    @app.get("/api/connectors")
    @limiter.limit("100/minute")
    async def list_connectors(
        request: Request,
        type: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        connectors = await store.list_connectors(connector_type=type, status=status)
        return [_connector_summary(c) for c in connectors]

    @app.get("/api/connectors/{connector_id}")
    @limiter.limit("100/minute")
    async def get_connector(
        request: Request,
        connector_id: str,
        caller: dict = Depends(auth_dep),
    ):
        c = await store.get_connector(connector_id)
        if not c:
            raise HTTPException(404, "Connector not found")
        return _connector_detail(c)

    @app.post("/api/connectors/generate")
    @limiter.limit("10/minute")
    async def generate_connector(
        request: Request,
        req: GenerateConnectorRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin")
        record = await agent.generate_connector(
            req.connector_type, req.db_type, req.params
        )
        valid, error = registry.validate_connector_code(
            record.code, req.connector_type
        )
        record.test_results["validation"] = {"valid": valid, "error": error}
        if valid:
            proposal = ContractChangeProposal(
                connector_id=record.connector_id,
                trigger_type=TriggerType.NEW_CONNECTOR,
                change_type=ChangeType.NEW_CONNECTOR,
                trigger_detail={"db_type": req.db_type},
                reasoning=(
                    f"Generated {req.connector_type} connector for {req.db_type}."
                ),
                confidence=0.8,
                impact_analysis={"dependencies": req.params},
                rollback_plan=(
                    "Delete the connector record and recreate pipelines "
                    "with an alternative."
                ),
                contract_version_before=0,
            )
            await store.save_proposal(proposal)
        await store.save_connector(record)
        return {
            "connector_id": record.connector_id,
            "connector_name": record.connector_name,
            "status": record.status.value if hasattr(record.status, 'value') else record.status,
            "validation": record.test_results.get("validation"),
        }

    @app.post("/api/connectors/{connector_id}/test")
    @limiter.limit("100/minute")
    async def test_connector(
        request: Request,
        connector_id: str,
        caller: dict = Depends(auth_dep),
        params: Optional[dict] = None,
    ):
        require_role(caller, "admin", "operator")
        c = await store.get_connector(connector_id)
        if not c:
            raise HTTPException(404, "Connector not found")
        params = params or {}
        try:
            # Temporarily load DRAFT/APPROVED connectors for testing
            if c.status in (ConnectorStatus.DRAFT, ConnectorStatus.APPROVED):
                registry.register_approved_connector(c)
            ct = c.connector_type.value if hasattr(c.connector_type, 'value') else c.connector_type
            if ct == "source":
                engine = await registry.get_source(connector_id, params)
            else:
                engine = await registry.get_target(connector_id, params)
            result = await engine.test_connection()
            c.test_status = TestStatus.PASSED if result.success else TestStatus.FAILED
            c.test_results = {"success": result.success, "version": result.version, "error": result.error}
            await store.save_connector(c)
            return c.test_results
        except Exception as e:
            c.test_status = TestStatus.FAILED
            c.test_results = {"success": False, "error": str(e)}
            await store.save_connector(c)
            return c.test_results

    @app.delete("/api/connectors/{connector_id}")
    @limiter.limit("100/minute")
    async def deprecate_connector(
        request: Request,
        connector_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin")
        c = await store.get_connector(connector_id)
        if not c:
            raise HTTPException(404, "Connector not found")
        pipelines = await store.list_pipelines()
        in_use = any(
            (p.source_connector_id == connector_id or p.target_connector_id == connector_id)
            for p in pipelines
            if p.status == PipelineStatus.ACTIVE
        )
        if in_use:
            raise HTTPException(409, "Connector is in use by active pipelines.")
        c.status = ConnectorStatus.DEPRECATED
        await store.save_connector(c)
        return {"status": "deprecated"}

    # -----------------------------------------------------------------------
    # Pipelines
    # -----------------------------------------------------------------------

    @app.get("/api/pipelines")
    @limiter.limit("100/minute")
    async def list_pipelines(
        request: Request,
        status: Optional[str] = Query(None),
        tier: Optional[int] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        pipelines = await store.list_pipelines(status=status)
        if tier is not None:
            pipelines = [p for p in pipelines if getattr(p, "tier", None) == tier]
        return [_pipeline_summary(p) for p in pipelines]

    @app.get("/api/pipelines/export")
    @limiter.limit("30/minute")
    async def export_all_pipelines(
        request: Request,
        status: Optional[str] = Query(None),
        include_credentials: bool = Query(False),
        caller: dict = Depends(auth_dep),
    ):
        if include_credentials:
            require_role(caller, "admin")

        pipelines = await store.list_pipelines(status=status)
        if not pipelines:
            return PlainTextResponse(
                "# No pipelines found\n", media_type="application/x-yaml",
            )

        mask = not include_credentials
        if include_credentials and config.has_encryption_key:
            for p in pipelines:
                for fld in ("source_password", "target_password"):
                    val = getattr(p, fld, "")
                    if val:
                        try:
                            setattr(p, fld, decrypt(val, config.encryption_key))
                        except Exception:
                            pass

        yaml_str = pipelines_to_yaml(pipelines, mask_credentials=mask)
        return PlainTextResponse(
            yaml_str,
            media_type="application/x-yaml",
            headers={"Content-Disposition": 'attachment; filename="pipelines.yaml"'},
        )

    @app.get("/api/pipelines/{pipeline_id}")
    @limiter.limit("100/minute")
    async def get_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        return await _pipeline_detail(p, store)

    @app.post("/api/pipelines")
    @limiter.limit("100/minute")
    async def create_pipeline(
        request: Request,
        req: CreatePipelineRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        # Build strategy dict from request fields
        strategy = dict(req.strategy)
        strategy.setdefault("source_schema", req.source_schema)
        strategy.setdefault("source_table", req.source_table)
        strategy.setdefault("target_schema", req.target_schema)
        strategy.setdefault("environment", req.environment)
        strategy.setdefault("tier", req.tier)
        strategy.setdefault("auto_approve_additive", req.auto_approve_additive)
        if req.column_mappings:
            strategy.setdefault("column_mappings", req.column_mappings)

        source_params = {
            "host": req.source_host,
            "port": req.source_port,
            "database": req.source_database,
            "user": req.source_user or "",
            "password": req.source_password or "",
        }
        target_params = {
            "host": req.target_host,
            "port": req.target_port,
            "database": req.target_database,
            "user": req.target_user or "",
            "password": req.target_password or "",
            "default_schema": req.target_schema,
        }

        pipeline = await conversation.create_pipeline(
            strategy=strategy,
            source_connector_id=req.source_connector_id,
            target_connector_id=req.target_connector_id,
            source_params=source_params,
            target_params=target_params,
            schedule=req.schedule_cron,
            owner=req.owner,
            tags=req.tags,
        )

        # Build 18: Apply steps if provided
        if req.steps:
            pipeline.steps = _parse_step_dicts(req.steps)
            await store.save_pipeline(pipeline)

        # Apply schema change policy if provided
        if req.schema_change_policy:
            pipeline.schema_change_policy = SchemaChangePolicy(**req.schema_change_policy)
            await store.save_pipeline(pipeline)

        await _log_pipeline_change(
            pipeline.pipeline_id, pipeline.pipeline_name,
            PipelineChangeType.CREATED, caller,
            changed_fields={
                "source": f"{req.source_schema}.{req.source_table}",
                "target": f"{req.target_schema}",
                "schedule": req.schedule_cron,
            },
        )
        _gitops_commit_pipeline(gitops, pipeline, f"Create pipeline: {pipeline.pipeline_name}", caller)
        return _pipeline_summary(pipeline)

    @app.post("/api/pipelines/batch")
    @limiter.limit("100/minute")
    async def batch_create_pipelines(
        request: Request,
        req: BatchCreateRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        created = []
        for p_req in req.pipelines:
            create_kwargs = p_req.model_dump()
            if config.has_encryption_key:
                create_kwargs = encrypt_dict(
                    create_kwargs, config.encryption_key, CREDENTIAL_FIELDS
                )
            pipeline = await conversation.create_pipeline(**create_kwargs)
            created.append(_pipeline_summary(pipeline))
        return created

    @app.patch("/api/pipelines/{pipeline_id}")
    @limiter.limit("100/minute")
    async def update_pipeline(
        request: Request,
        pipeline_id: str,
        req: UpdatePipelineRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        import json as _json

        changes = {}

        # --- Schedule fields ---
        if req.schedule_cron is not None:
            changes["schedule_cron"] = {"old": p.schedule_cron, "new": req.schedule_cron}
            p.schedule_cron = req.schedule_cron
        if req.retry_max_attempts is not None:
            changes["retry_max_attempts"] = {"old": p.retry_max_attempts, "new": req.retry_max_attempts}
            p.retry_max_attempts = req.retry_max_attempts
        if req.retry_backoff_seconds is not None:
            changes["retry_backoff_seconds"] = {"old": p.retry_backoff_seconds, "new": req.retry_backoff_seconds}
            p.retry_backoff_seconds = req.retry_backoff_seconds
        if req.timeout_seconds is not None:
            changes["timeout_seconds"] = {"old": p.timeout_seconds, "new": req.timeout_seconds}
            p.timeout_seconds = req.timeout_seconds

        # --- Strategy fields ---
        if req.refresh_type is not None:
            old_val = p.refresh_type.value if hasattr(p.refresh_type, "value") else p.refresh_type
            new_enum = RefreshType(req.refresh_type.lower())
            changes["refresh_type"] = {"old": old_val, "new": new_enum.value}
            p.refresh_type = new_enum
        if req.replication_method is not None:
            old_val = p.replication_method.value if hasattr(p.replication_method, "value") else p.replication_method
            new_enum = ReplicationMethod(req.replication_method.lower())
            changes["replication_method"] = {"old": old_val, "new": new_enum.value}
            p.replication_method = new_enum
        if req.incremental_column is not None:
            changes["incremental_column"] = {"old": p.incremental_column, "new": req.incremental_column}
            p.incremental_column = req.incremental_column
        if req.load_type is not None:
            old_val = p.load_type.value if hasattr(p.load_type, "value") else p.load_type
            new_enum = LoadType(req.load_type.lower())
            changes["load_type"] = {"old": old_val, "new": new_enum.value}
            p.load_type = new_enum
        if req.merge_keys is not None:
            changes["merge_keys"] = {"old": p.merge_keys, "new": req.merge_keys}
            p.merge_keys = req.merge_keys

        # Reset watermark (clears it for full repull)
        if req.reset_watermark:
            changes["last_watermark"] = {"old": p.last_watermark, "new": None}
            p.last_watermark = None
        elif req.last_watermark is not None:
            changes["last_watermark"] = {"old": p.last_watermark, "new": req.last_watermark}
            p.last_watermark = req.last_watermark

        # --- Quality config partial merge ---
        if req.quality_config is not None:
            qc = p.quality_config or QualityConfig()
            qc_changes = {}
            for k, v in req.quality_config.items():
                if hasattr(qc, k):
                    old_v = getattr(qc, k)
                    if old_v != v:
                        qc_changes[k] = {"old": old_v, "new": v}
                        setattr(qc, k, v)
            if qc_changes:
                changes["quality_config"] = qc_changes
            p.quality_config = qc

        # --- Observability fields ---
        if req.tier is not None:
            changes["tier"] = {"old": p.tier, "new": req.tier}
            p.tier = req.tier
        if req.owner is not None:
            changes["owner"] = {"old": p.owner, "new": req.owner}
            p.owner = req.owner
        if req.tags is not None:
            changes["tags"] = {"old": p.tags, "new": req.tags}
            p.tags = req.tags
        if req.tier_config is not None:
            changes["tier_config"] = {"old": p.tier_config, "new": req.tier_config}
            p.tier_config = req.tier_config
        if req.freshness_column is not None:
            changes["freshness_column"] = {"old": p.freshness_column, "new": req.freshness_column}
            p.freshness_column = req.freshness_column

        # --- Approval ---
        if req.auto_approve_additive_schema is not None:
            changes["auto_approve_additive_schema"] = {"old": p.auto_approve_additive_schema, "new": req.auto_approve_additive_schema}
            p.auto_approve_additive_schema = req.auto_approve_additive_schema

        # --- Schema change policy ---
        if req.schema_change_policy is not None:
            from dataclasses import asdict as _asdict
            old_policy = _asdict(p.get_schema_policy())
            new_policy = SchemaChangePolicy(**req.schema_change_policy)
            changes["schema_change_policy"] = {"old": old_policy, "new": _asdict(new_policy)}
            p.schema_change_policy = new_policy

        if req.post_promotion_hooks is not None:
            from dataclasses import asdict as _asdict
            old_hooks = [_asdict(h) for h in p.post_promotion_hooks]
            new_hooks = []
            for h in req.post_promotion_hooks:
                if "hook_id" not in h or not h["hook_id"]:
                    h["hook_id"] = new_id()
                new_hooks.append(PostPromotionHook(**h))
            changes["post_promotion_hooks"] = {
                "old": old_hooks,
                "new": [_asdict(nh) for nh in new_hooks],
            }
            p.post_promotion_hooks = new_hooks

        # --- Steps (Build 18) ---
        if req.steps is not None:
            old_steps = [
                {"step_id": s.step_id, "step_name": s.step_name, "step_type": s.step_type.value if hasattr(s.step_type, "value") else s.step_type}
                for s in (p.steps or [])
            ]
            new_steps = _parse_step_dicts(req.steps)
            changes["steps"] = {
                "old": old_steps,
                "new": [{"step_id": s.step_id, "step_name": s.step_name, "step_type": s.step_type.value} for s in new_steps],
            }
            p.steps = new_steps

        # Build 28: auto_propagate_context
        if req.auto_propagate_context is not None:
            changes["auto_propagate_context"] = {"old": p.auto_propagate_context, "new": req.auto_propagate_context}
            p.auto_propagate_context = req.auto_propagate_context

        if not changes:
            return await _pipeline_detail(p, store)

        # Bump version and update timestamp
        p.version += 1
        p.updated_at = now_iso()
        await store.save_pipeline(p)

        # Audit: save DecisionLog (legacy)
        await store.save_decision(DecisionLog(
            pipeline_id=p.pipeline_id,
            decision_type="contract_update",
            detail=_json.dumps(changes, default=str),
            reasoning=req.reason or "",
            created_at=now_iso(),
        ))

        # Classify change type for changelog
        _change_type = PipelineChangeType.UPDATED
        _changed_keys = set(changes.keys())
        if _changed_keys <= {"schedule_cron", "retry_max_attempts", "retry_backoff_seconds", "timeout_seconds"}:
            _change_type = PipelineChangeType.SCHEDULE_CHANGED
        elif _changed_keys <= {"refresh_type", "replication_method", "incremental_column", "load_type", "merge_keys", "last_watermark"}:
            _change_type = PipelineChangeType.STRATEGY_CHANGED
        elif _changed_keys <= {"quality_config"}:
            _change_type = PipelineChangeType.QUALITY_CONFIG_CHANGED
        elif _changed_keys <= {"post_promotion_hooks"}:
            _change_type = PipelineChangeType.HOOK_CHANGED
        await _log_pipeline_change(
            p.pipeline_id, p.pipeline_name, _change_type, caller,
            changed_fields=changes, reason=req.reason or "",
        )

        # Persist contract YAML to disk
        _persist_contract_yaml(p, config)
        _gitops_commit_pipeline(gitops, p, f"Update pipeline: {p.pipeline_name} (v{p.version})", caller)

        return await _pipeline_detail(p, store)

    @app.post("/api/pipelines/{pipeline_id}/trigger")
    @limiter.limit("100/minute")
    async def trigger_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        run = await scheduler.trigger(pipeline_id)
        p = await store.get_pipeline(pipeline_id)
        await _log_pipeline_change(
            pipeline_id, p.pipeline_name if p else "",
            PipelineChangeType.TRIGGERED, caller,
            changed_fields={"run_id": run.run_id},
        )
        return {"run_id": run.run_id, "status": run.status.value}

    @app.post("/api/pipelines/{pipeline_id}/backfill")
    @limiter.limit("100/minute")
    async def backfill_pipeline(
        request: Request,
        pipeline_id: str,
        req: BackfillRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        run = await scheduler.trigger_backfill(pipeline_id, req.start, req.end)
        p = await store.get_pipeline(pipeline_id)
        await _log_pipeline_change(
            pipeline_id, p.pipeline_name if p else "",
            PipelineChangeType.BACKFILLED, caller,
            changed_fields={"run_id": run.run_id, "start": req.start, "end": req.end},
        )
        return {
            "run_id": run.run_id,
            "status": run.status.value,
            "backfill_start": req.start,
            "backfill_end": req.end,
        }

    @app.post("/api/pipelines/{pipeline_id}/pause")
    @limiter.limit("100/minute")
    async def pause_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        old_status = p.status.value if hasattr(p.status, "value") else p.status
        p.status = PipelineStatus.PAUSED
        await store.save_pipeline(p)
        await _log_pipeline_change(
            pipeline_id, p.pipeline_name,
            PipelineChangeType.PAUSED, caller,
            changed_fields={"status": {"old": old_status, "new": "paused"}},
        )
        _gitops_commit_pipeline(gitops, p, f"Pause pipeline: {p.pipeline_name}", caller)
        return {"status": "paused"}

    @app.post("/api/pipelines/{pipeline_id}/resume")
    @limiter.limit("100/minute")
    async def resume_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        old_status = p.status.value if hasattr(p.status, "value") else p.status
        p.status = PipelineStatus.ACTIVE
        await store.save_pipeline(p)
        await _log_pipeline_change(
            pipeline_id, p.pipeline_name,
            PipelineChangeType.RESUMED, caller,
            changed_fields={"status": {"old": old_status, "new": "active"}},
        )
        _gitops_commit_pipeline(gitops, p, f"Resume pipeline: {p.pipeline_name}", caller)
        return {"status": "active"}

    @app.get("/api/pipelines/{pipeline_id}/preview")
    @limiter.limit("100/minute")
    async def preview_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        try:
            return await conversation.preview_pipeline(pipeline_id)
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get("/api/pipelines/{pipeline_id}/changelog")
    @limiter.limit("100/minute")
    async def get_pipeline_changelog(
        request: Request,
        pipeline_id: str,
        change_type: str = Query(default=None),
        limit: int = Query(default=50, le=200),
        offset: int = Query(default=0),
        caller: dict = Depends(auth_dep),
    ):
        """Get the changelog for a specific pipeline — who changed what, when, why."""
        changes = await store.list_pipeline_changes(
            pipeline_id, change_type=change_type, limit=limit, offset=offset,
        )
        return {
            "pipeline_id": pipeline_id,
            "changes": [
                {
                    "change_id": c.change_id,
                    "change_type": c.change_type.value if hasattr(c.change_type, "value") else c.change_type,
                    "changed_by": c.changed_by,
                    "source": c.source,
                    "changed_fields": c.changed_fields,
                    "reason": c.reason,
                    "context": c.context,
                    "created_at": c.created_at,
                }
                for c in changes
            ],
        }

    @app.get("/api/changelog")
    @limiter.limit("100/minute")
    async def get_global_changelog(
        request: Request,
        limit: int = Query(default=50, le=200),
        offset: int = Query(default=0),
        caller: dict = Depends(auth_dep),
    ):
        """Get the global changelog across all pipelines."""
        if caller and caller.get("role") != "admin":
            raise HTTPException(403, "Admin access required")
        changes = await store.list_all_pipeline_changes(limit=limit, offset=offset)
        return {
            "changes": [
                {
                    "change_id": c.change_id,
                    "pipeline_id": c.pipeline_id,
                    "pipeline_name": c.pipeline_name,
                    "change_type": c.change_type.value if hasattr(c.change_type, "value") else c.change_type,
                    "changed_by": c.changed_by,
                    "source": c.source,
                    "changed_fields": c.changed_fields,
                    "reason": c.reason,
                    "context": c.context,
                    "created_at": c.created_at,
                }
                for c in changes
            ],
        }

    @app.get("/api/pipelines/{pipeline_id}/runs")
    @limiter.limit("100/minute")
    async def list_runs(
        request: Request,
        pipeline_id: str,
        limit: int = Query(50),
        caller: dict = Depends(auth_dep),
    ):
        runs = await store.list_runs(pipeline_id, limit=limit)
        return [_run_summary(r) for r in runs]

    @app.get("/api/runs/{run_id}/trigger-chain")
    @limiter.limit("100/minute")
    async def get_trigger_chain(
        request: Request,
        run_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Walk the trigger chain backwards from a run to its root trigger."""
        chain = await store.get_trigger_chain(run_id)
        if not chain:
            raise HTTPException(404, "Run not found")
        return {
            "run_id": run_id,
            "chain_length": len(chain),
            "chain": [_run_summary(r) for r in chain],
        }

    # -----------------------------------------------------------------------
    # Build 28: Run context & context chain
    # -----------------------------------------------------------------------

    @app.get("/api/runs/{run_id}/context")
    @limiter.limit("100/minute")
    async def get_run_context(
        request: Request,
        run_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Full aggregated context for a run: own data + upstream chain + metadata."""
        from dataclasses import asdict as _asdict
        ctx = await store.get_run_context(run_id)
        if not ctx:
            raise HTTPException(404, "Run not found")
        return _asdict(ctx)

    @app.get("/api/pipelines/{pipeline_id}/context-chain")
    @limiter.limit("100/minute")
    async def get_context_chain(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Walk the pipeline's upstream dependency DAG, returning latest run context
        for each pipeline in the chain."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        chain = await store.get_context_chain(pipeline_id)
        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": p.pipeline_name,
            "chain_length": len(chain),
            "chain": chain,
        }

    @app.get("/api/pipelines/{pipeline_id}/schema-history")
    @limiter.limit("100/minute")
    async def schema_history(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        versions = await store.list_schema_versions(pipeline_id)
        return [
            {
                "version_id": sv.version_id,
                "version": sv.version,
                "change_summary": sv.change_summary,
                "change_type": sv.change_type,
                "applied_at": sv.applied_at,
                "applied_by": sv.applied_by,
                "column_count": len(sv.column_mappings) if sv.column_mappings else 0,
            }
            for sv in versions
        ]

    @app.get("/api/pipelines/{pipeline_id}/timeline")
    @limiter.limit("100/minute")
    async def pipeline_timeline(
        request: Request,
        pipeline_id: str,
        limit: int = Query(50, ge=1, le=200),
        caller: dict = Depends(auth_dep),
    ):
        pipeline = await store.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(404, "Pipeline not found")

        events = []

        runs = await store.list_runs(pipeline_id, limit=limit)
        for r in runs:
            events.append({
                "type": "run",
                "timestamp": r.completed_at or r.started_at,
                "run_id": r.run_id,
                "status": r.status.value if hasattr(r.status, "value") else r.status,
                "run_mode": r.run_mode.value if hasattr(r.run_mode, "value") else r.run_mode,
                "rows_extracted": r.rows_extracted,
                "error": r.error,
            })

        gates = await store.list_gates(pipeline_id)
        for g in gates[:limit]:
            events.append({
                "type": "gate",
                "timestamp": g.evaluated_at,
                "run_id": g.run_id,
                "decision": g.decision.value if hasattr(g.decision, "value") else g.decision,
                "checks": [
                    {"name": c.check_name, "status": c.status.value if hasattr(c.status, "value") else c.status}
                    for c in (g.checks or [])
                ],
            })

        alerts = await store.list_alerts_for_pipeline(pipeline_id, limit=limit)
        for a in alerts:
            events.append({
                "type": "alert",
                "timestamp": a.created_at,
                "alert_id": a.alert_id,
                "severity": a.severity.value if hasattr(a.severity, "value") else a.severity,
                "summary": a.summary,
            })

        decisions = await store.list_decisions(pipeline_id)
        for d in decisions[:limit]:
            events.append({
                "type": "decision",
                "timestamp": d.created_at,
                "decision_type": d.decision_type,
                "detail": d.detail,
                "reasoning": d.reasoning,
            })

        events.sort(key=lambda e: e.get("timestamp") or "", reverse=True)
        events = events[:limit]

        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline.pipeline_name,
            "event_count": len(events),
            "events": events,
        }

    # -----------------------------------------------------------------------
    # Contract export / import / sync
    # -----------------------------------------------------------------------

    @app.get("/api/pipelines/{pipeline_id}/export")
    @limiter.limit("100/minute")
    async def export_pipeline(
        request: Request,
        pipeline_id: str,
        include_state: bool = Query(False),
        include_credentials: bool = Query(False),
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        if include_credentials:
            require_role(caller, "admin")
            if config.has_encryption_key:
                for fld in ("source_password", "target_password"):
                    val = getattr(p, fld, "")
                    if val:
                        try:
                            setattr(p, fld, decrypt(val, config.encryption_key))
                        except Exception:
                            pass

        mask = not include_credentials

        if include_state:
            error_budget = await store.get_error_budget(p.pipeline_id)
            dependencies = await store.list_dependencies(p.pipeline_id)
            schema_versions = await store.list_schema_versions(p.pipeline_id)

            eb_dict = asdict(error_budget) if error_budget else None
            dep_dicts = [asdict(d) for d in dependencies]
            sv_dicts = [
                {
                    "version": sv.version,
                    "change_summary": sv.change_summary,
                    "change_type": sv.change_type,
                    "applied_at": sv.applied_at,
                }
                for sv in schema_versions
            ]

            d = pipeline_to_dict(p, mask_credentials=mask)
            d["_state"] = snapshot_state(p, eb_dict, dep_dicts, sv_dicts)
            yaml_str = yaml.dump(
                d, default_flow_style=False, sort_keys=False, allow_unicode=True,
            )
        else:
            yaml_str = pipeline_to_yaml(p, mask_credentials=mask)

        return PlainTextResponse(
            yaml_str,
            media_type="application/x-yaml",
            headers={
                "Content-Disposition": f'attachment; filename="{p.pipeline_name}.yaml"',
            },
        )

    @app.post("/api/pipelines/import")
    @limiter.limit("10/minute")
    async def import_pipelines(
        request: Request,
        mode: str = Query("create"),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")

        if mode not in ("create", "upsert"):
            raise HTTPException(400, "mode must be 'create' or 'upsert'")

        body = await request.body()
        try:
            pipelines = yaml_to_pipelines(body.decode("utf-8"), preserve_id=False)
        except Exception as e:
            raise HTTPException(400, f"Invalid YAML: {e}")

        results = []
        for p in pipelines:
            existing = await store.get_pipeline_by_name(p.pipeline_name)

            if existing and mode == "create":
                raise HTTPException(
                    409,
                    f"Pipeline '{p.pipeline_name}' already exists. "
                    "Use mode=upsert to update.",
                )

            if existing and mode == "upsert":
                p.pipeline_id = existing.pipeline_id
                p.created_at = existing.created_at
                p.version = existing.version + 1
                if not p.source_password:
                    p.source_password = existing.source_password
                if not p.target_password:
                    p.target_password = existing.target_password
                action = "updated"
            else:
                action = "created"

            if config.has_encryption_key:
                for fld in ("source_password", "target_password"):
                    val = getattr(p, fld, "")
                    if val and val != "***":
                        setattr(p, fld, encrypt(val, config.encryption_key))

            p.updated_at = now_iso()
            await store.save_pipeline(p)
            results.append({
                "pipeline_id": p.pipeline_id,
                "pipeline_name": p.pipeline_name,
                "action": action,
                "version": p.version,
            })

        return results

    @app.post("/api/contracts/sync")
    @limiter.limit("10/minute")
    async def sync_contracts(
        request: Request,
        dry_run: bool = Query(True),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin")

        body = await request.body()
        try:
            incoming_pipelines = yaml_to_pipelines(
                body.decode("utf-8"), preserve_id=False,
            )
        except Exception as e:
            raise HTTPException(400, f"Invalid YAML: {e}")

        created = []
        updated = []
        unchanged = []
        errors = []

        for incoming in incoming_pipelines:
            try:
                existing = await store.get_pipeline_by_name(incoming.pipeline_name)

                if not existing:
                    if dry_run:
                        created.append({
                            "pipeline_name": incoming.pipeline_name,
                            "action": "create",
                        })
                    else:
                        if config.has_encryption_key:
                            for fld in ("source_password", "target_password"):
                                val = getattr(incoming, fld, "")
                                if val and val != "***":
                                    setattr(
                                        incoming, fld,
                                        encrypt(val, config.encryption_key),
                                    )
                        await store.save_pipeline(incoming)
                        created.append({
                            "pipeline_id": incoming.pipeline_id,
                            "pipeline_name": incoming.pipeline_name,
                        })
                else:
                    diffs = diff_contracts(existing, incoming)
                    if not diffs:
                        unchanged.append(existing.pipeline_name)
                    elif dry_run:
                        updated.append({
                            "pipeline_name": existing.pipeline_name,
                            "pipeline_id": existing.pipeline_id,
                            "diffs": diffs,
                        })
                    else:
                        incoming.pipeline_id = existing.pipeline_id
                        incoming.created_at = existing.created_at
                        incoming.version = existing.version + 1
                        if not incoming.source_password:
                            incoming.source_password = existing.source_password
                        if not incoming.target_password:
                            incoming.target_password = existing.target_password
                        if config.has_encryption_key:
                            for fld in ("source_password", "target_password"):
                                val = getattr(incoming, fld, "")
                                if val and val != "***":
                                    setattr(
                                        incoming, fld,
                                        encrypt(val, config.encryption_key),
                                    )
                        incoming.updated_at = now_iso()
                        await store.save_pipeline(incoming)
                        updated.append({
                            "pipeline_id": incoming.pipeline_id,
                            "pipeline_name": incoming.pipeline_name,
                            "diffs": diffs,
                        })
            except HTTPException:
                raise
            except Exception as e:
                errors.append({
                    "pipeline_name": incoming.pipeline_name,
                    "error": str(e),
                })

        return {
            "dry_run": dry_run,
            "created": created,
            "updated": updated,
            "unchanged": unchanged,
            "errors": errors,
        }

    # -----------------------------------------------------------------------
    # Approvals
    # -----------------------------------------------------------------------

    @app.get("/api/approvals")
    @limiter.limit("100/minute")
    async def list_approvals(
        request: Request,
        status: str = Query("pending"),
        caller: dict = Depends(auth_dep),
    ):
        proposals = await store.list_proposals(status=status)
        results = []
        for p in proposals:
            s = _proposal_summary(p)
            # Enrich with pipeline/connector names
            if p.pipeline_id:
                pipe = await store.get_pipeline(p.pipeline_id)
                s["pipeline_name"] = pipe.pipeline_name if pipe else None
            if p.connector_id:
                conn = await store.get_connector(p.connector_id)
                s["connector_name"] = conn.name if conn else None
            results.append(s)
        return results

    @app.post("/api/approvals/{proposal_id}")
    @limiter.limit("100/minute")
    async def resolve_approval(
        request: Request,
        proposal_id: str,
        req: ApprovalRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        proposal = await store.get_proposal(proposal_id)
        if not proposal:
            raise HTTPException(404, "Proposal not found")
        if proposal.status != ProposalStatus.PENDING:
            raise HTTPException(409, "Proposal is not pending")

        proposal.resolved_by = caller.get("sub", "unknown")
        proposal.resolved_at = now_iso()
        proposal.resolution_note = req.note or ""

        if req.action == "approve":
            proposal.status = ProposalStatus.APPROVED
            await _apply_proposal(proposal, store, registry, agent, gitops, config)
        elif req.action == "reject":
            proposal.status = ProposalStatus.REJECTED
            if req.note:
                await agent.learn_from_rejection(proposal, req.note)
        else:
            raise HTTPException(400, "action must be 'approve' or 'reject'")

        await store.save_proposal(proposal)
        return {"status": proposal.status.value}

    # -----------------------------------------------------------------------
    # Quality
    # -----------------------------------------------------------------------

    @app.get("/api/quality/{pipeline_id}")
    @limiter.limit("100/minute")
    async def quality_history(
        request: Request,
        pipeline_id: str,
        days: int = Query(7),
        caller: dict = Depends(auth_dep),
    ):
        gates = await store.list_gates(pipeline_id)
        if not gates:
            return {"pipeline_id": pipeline_id, "gates": [], "summary": {}}

        total = len(gates)
        passed = sum(
            1
            for g in gates
            if g.decision.value in ("promote", "promote_with_warning")
        )
        halted = total - passed

        check_stats: dict[str, dict] = {}
        for gate in gates:
            for check in gate.checks:
                if check.check_name not in check_stats:
                    check_stats[check.check_name] = {"pass": 0, "warn": 0, "fail": 0}
                check_stats[check.check_name][check.status.value] += 1

        return {
            "pipeline_id": pipeline_id,
            "summary": {
                "total_runs": total,
                "pass_rate": round(passed / total, 4) if total else 0,
                "halted": halted,
                "check_stats": check_stats,
            },
            "gates": [
                {
                    "gate_id": g.gate_id,
                    "decision": g.decision.value,
                    "evaluated_at": g.evaluated_at,
                    "agent_reasoning": g.agent_reasoning,
                    "checks": [
                        {
                            "name": c.check_name,
                            "status": c.status.value,
                            "detail": c.detail,
                        }
                        for c in g.checks
                    ],
                }
                for g in gates
            ],
        }

    # -----------------------------------------------------------------------
    # Observability
    # -----------------------------------------------------------------------

    @app.get("/api/observability/freshness")
    @limiter.limit("100/minute")
    async def freshness_report(
        request: Request,
        tier: Optional[int] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        pipelines = await store.list_pipelines()
        grouped: dict[int, list] = {}
        for p in pipelines:
            if tier is not None and p.tier != tier:
                continue
            snapshot = await store.get_latest_freshness(p.pipeline_id)
            if not snapshot:
                continue
            tier_cfg = p.get_tier_config()
            last_run = await store.get_last_successful_run(p.pipeline_id)
            grouped.setdefault(p.tier, []).append(
                {
                    "pipeline_id": p.pipeline_id,
                    "pipeline_name": p.pipeline_name,
                    "staleness_minutes": snapshot.staleness_minutes,
                    "freshness_sla_minutes": snapshot.freshness_sla_minutes,
                    "sla_met": snapshot.sla_met,
                    "status": snapshot.status.value if hasattr(snapshot.status, "value") else snapshot.status,
                    "last_record_time": snapshot.last_record_time,
                    "checked_at": snapshot.checked_at,
                    "schedule": p.schedule_cron,
                    "freshness_column": p.get_freshness_col(),
                    "freshness_critical_minutes": tier_cfg.get("freshness_critical_minutes"),
                    "last_run_at": last_run.completed_at if last_run else None,
                    "last_run_rows": last_run.rows_extracted if last_run else None,
                    "source_connector_id": p.source_connector_id,
                    "target_table": p.target_table,
                }
            )
        return {"tiers": {str(k): v for k, v in sorted(grouped.items())}}

    @app.get("/api/observability/freshness/{pipeline_id}/history")
    @limiter.limit("100/minute")
    async def freshness_history(
        request: Request,
        pipeline_id: str,
        hours: int = Query(24, ge=1, le=168),
        caller: dict = Depends(auth_dep),
    ):
        snapshots = await store.list_freshness_history(pipeline_id, hours)
        return [
            {
                "staleness_minutes": s.staleness_minutes,
                "sla_met": s.sla_met,
                "status": s.status.value if hasattr(s.status, "value") else s.status,
                "checked_at": s.checked_at,
            }
            for s in snapshots
        ]

    @app.get("/api/observability/alerts")
    @limiter.limit("100/minute")
    async def list_alerts(
        request: Request,
        severity: Optional[str] = Query(None),
        hours: int = Query(24),
        caller: dict = Depends(auth_dep),
    ):
        alerts = await store.list_alerts(severity=severity, hours=hours)
        return [
            {
                "alert_id": a.alert_id,
                "severity": a.severity.value if hasattr(a.severity, "value") else a.severity,
                "tier": a.tier,
                "pipeline_id": a.pipeline_id,
                "pipeline_name": a.pipeline_name,
                "summary": a.summary,
                "detail": a.detail,
                "narrative": a.narrative or "",
                "created_at": a.created_at,
                "acknowledged": a.acknowledged,
                "acknowledged_by": getattr(a, "acknowledged_by", None),
                "acknowledged_at": getattr(a, "acknowledged_at", None),
            }
            for a in alerts
        ]

    @app.post("/api/observability/alerts/{alert_id}/acknowledge")
    @limiter.limit("100/minute")
    async def acknowledge_alert(
        request: Request,
        alert_id: str,
        caller: dict = Depends(auth_dep),
    ):
        alerts = await store.list_alerts(hours=720)
        alert_obj = next((a for a in alerts if a.alert_id == alert_id), None)
        if not alert_obj:
            raise HTTPException(404, "Alert not found")
        alert_obj.acknowledged = True
        alert_obj.acknowledged_by = caller.get("sub", "unknown")
        alert_obj.acknowledged_at = now_iso()
        await store.save_alert(alert_obj)
        return {"status": "acknowledged"}

    @app.post("/api/observability/alerts/{alert_id}/narrative")
    @limiter.limit("10/minute")
    async def generate_alert_narrative(
        request: Request,
        alert_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Generate or regenerate a human-readable narrative for an alert."""
        alerts = await store.list_alerts(hours=720)
        alert_obj = next((a for a in alerts if a.alert_id == alert_id), None)
        if not alert_obj:
            raise HTTPException(404, "Alert not found")

        # Gather context
        p = await store.get_pipeline(alert_obj.pipeline_id) if alert_obj.pipeline_id else None
        downstream = await store.list_dependents(alert_obj.pipeline_id) if alert_obj.pipeline_id else []
        recent_runs = await store.list_runs(alert_obj.pipeline_id, limit=3) if alert_obj.pipeline_id else []
        recent_errors = [r.error for r in recent_runs if r.error]
        freshness = await store.get_latest_freshness(alert_obj.pipeline_id) if alert_obj.pipeline_id else None

        narrative = await agent.generate_anomaly_narrative(
            pipeline_name=alert_obj.pipeline_name,
            alert_summary=alert_obj.summary,
            alert_detail=alert_obj.detail,
            severity=alert_obj.severity.value if hasattr(alert_obj.severity, "value") else alert_obj.severity,
            tier=alert_obj.tier,
            downstream_count=len(downstream),
            recent_run_errors=recent_errors,
            freshness_info={
                "staleness_minutes": freshness.staleness_minutes,
                "status": freshness.status.value if hasattr(freshness.status, "value") else freshness.status,
            } if freshness else None,
            schedule_cron=p.schedule_cron if p else "",
        )

        # Save narrative to the alert
        alert_obj.narrative = narrative
        await store.save_alert(alert_obj)

        return {
            "alert_id": alert_id,
            "narrative": narrative,
            "pipeline_name": alert_obj.pipeline_name,
            "severity": alert_obj.severity.value if hasattr(alert_obj.severity, "value") else alert_obj.severity,
        }

    # -----------------------------------------------------------------------
    # Lineage
    # -----------------------------------------------------------------------

    @app.get("/api/lineage/{pipeline_id}")
    @limiter.limit("100/minute")
    async def get_lineage(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        deps = await store.list_dependencies(pipeline_id)

        upstream = [d for d in deps if d.pipeline_id == pipeline_id]

        # Find downstream: pipelines that depend on this one
        all_pipelines = await store.list_pipelines()
        downstream_deps = []
        for p in all_pipelines:
            if p.pipeline_id == pipeline_id:
                continue
            p_deps = await store.list_dependencies(p.pipeline_id)
            for d in p_deps:
                if d.depends_on_id == pipeline_id:
                    downstream_deps.append(d)

        async def enrich_upstream(dep):
            other_id = dep.depends_on_id
            other = await store.get_pipeline(other_id)
            last_run = await store.get_last_successful_run(other_id)
            return {
                "dependency_id": dep.dependency_id,
                "pipeline_id": other_id,
                "pipeline_name": other.pipeline_name if other else other_id,
                "tier": other.tier if other else None,
                "status": other.status.value if other else "unknown",
                "last_successful_run": last_run.started_at if last_run else None,
                "dependency_type": dep.dependency_type.value if hasattr(dep.dependency_type, "value") else dep.dependency_type,
                "notes": dep.notes,
            }

        async def enrich_downstream(dep):
            other_id = dep.pipeline_id
            other = await store.get_pipeline(other_id)
            last_run = await store.get_last_successful_run(other_id)
            return {
                "dependency_id": dep.dependency_id,
                "pipeline_id": other_id,
                "pipeline_name": other.pipeline_name if other else other_id,
                "tier": other.tier if other else None,
                "status": other.status.value if other else "unknown",
                "last_successful_run": last_run.started_at if last_run else None,
                "dependency_type": dep.dependency_type.value if hasattr(dep.dependency_type, "value") else dep.dependency_type,
                "notes": dep.notes,
            }

        # Column-level lineage
        all_column_lineage = await store.list_column_lineage(pipeline_id)
        column_lineage = [cl for cl in all_column_lineage if cl.source_pipeline_id == pipeline_id]
        downstream_columns = [cl for cl in all_column_lineage if cl.target_pipeline_id == pipeline_id and cl.source_pipeline_id != pipeline_id]

        upstream_enriched = []
        for d in upstream:
            upstream_enriched.append(await enrich_upstream(d))

        downstream_enriched = []
        for d in downstream_deps:
            downstream_enriched.append(await enrich_downstream(d))

        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": p.pipeline_name,
            "upstream": upstream_enriched,
            "downstream": downstream_enriched,
            "column_lineage": [
                {
                    "lineage_id": cl.id,
                    "source_column": cl.source_column,
                    "target_column": cl.target_column,
                    "transform_logic": cl.transformation,
                }
                for cl in column_lineage
            ] if column_lineage else [],
            "downstream_columns": [
                {
                    "lineage_id": dc.id,
                    "pipeline_id": dc.source_pipeline_id,
                    "source_column": dc.source_column,
                    "target_column": dc.target_column,
                }
                for dc in downstream_columns
            ] if downstream_columns else [],
        }

    @app.post("/api/lineage")
    @limiter.limit("100/minute")
    async def declare_dependency(
        request: Request,
        req: DeclareDepRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        dep = await conversation.declare_dependency(
            req.pipeline_id, req.depends_on_id, req.notes
        )
        return {"dependency_id": dep.dependency_id}

    @app.delete("/api/lineage/{dependency_id}")
    @limiter.limit("100/minute")
    async def delete_dependency(
        request: Request,
        dependency_id: str,
        caller: dict = Depends(auth_dep),
    ):
        await store.delete_dependency(dependency_id)
        return {"status": "deleted"}

    # -----------------------------------------------------------------------
    # Pipeline dependencies (Build 11)
    # -----------------------------------------------------------------------

    class AddDependencyRequest(BaseModel):
        depends_on_id: str
        dependency_type: Optional[str] = "user_defined"
        notes: Optional[str] = None

    @app.post("/api/pipelines/{pipeline_id}/dependencies")
    @limiter.limit("100/minute")
    async def add_pipeline_dependency(
        request: Request,
        pipeline_id: str,
        req: AddDependencyRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        upstream = await store.get_pipeline(req.depends_on_id)
        if not upstream:
            raise HTTPException(404, f"Upstream pipeline not found: {req.depends_on_id}")
        if pipeline_id == req.depends_on_id:
            raise HTTPException(400, "Pipeline cannot depend on itself")
        # Simple cycle check: if upstream depends on this pipeline
        upstream_deps = await store.list_dependencies(req.depends_on_id)
        for d in upstream_deps:
            if d.depends_on_id == pipeline_id:
                raise HTTPException(400, "Adding this dependency would create a cycle")

        dep = PipelineDependency(
            pipeline_id=pipeline_id,
            depends_on_id=req.depends_on_id,
            dependency_type=DependencyType(req.dependency_type.lower()),
            notes=req.notes,
        )
        await store.save_dependency(dep)

        await store.save_decision(DecisionLog(
            pipeline_id=pipeline_id,
            decision_type="dependency_added",
            detail=f"Added dependency on {upstream.pipeline_name}",
            reasoning=req.notes or "",
        ))
        await _log_pipeline_change(
            pipeline_id=pipeline_id,
            pipeline_name=p.pipeline_name,
            change_type=PipelineChangeType.DEPENDENCY_ADDED,
            changed_fields={"depends_on_id": req.depends_on_id, "depends_on_name": upstream.pipeline_name},
            caller=caller,
            context=f"Added upstream dependency on {upstream.pipeline_name}",
        )
        return {"dependency_id": dep.dependency_id}

    @app.get("/api/pipelines/{pipeline_id}/dependencies")
    @limiter.limit("100/minute")
    async def list_pipeline_dependencies(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        deps = await store.list_dependencies(pipeline_id)
        dependents = await store.list_dependents(pipeline_id)
        result = []
        for dep in deps:
            up = await store.get_pipeline(dep.depends_on_id)
            result.append({
                "dependency_id": dep.dependency_id,
                "depends_on_id": dep.depends_on_id,
                "depends_on_name": up.pipeline_name if up else dep.depends_on_id,
                "dependency_type": dep.dependency_type.value if hasattr(dep.dependency_type, "value") else dep.dependency_type,
                "notes": dep.notes,
            })
        return {
            "upstream": result,
            "downstream_count": len(dependents),
        }

    @app.delete("/api/pipelines/{pipeline_id}/dependencies/{dependency_id}")
    @limiter.limit("100/minute")
    async def remove_pipeline_dependency(
        request: Request,
        pipeline_id: str,
        dependency_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        await store.delete_dependency(dependency_id)
        await store.save_decision(DecisionLog(
            pipeline_id=pipeline_id,
            decision_type="dependency_removed",
            detail=f"Removed dependency {dependency_id}",
            reasoning="",
        ))
        if p:
            await _log_pipeline_change(
                pipeline_id=pipeline_id,
                pipeline_name=p.pipeline_name,
                change_type=PipelineChangeType.DEPENDENCY_REMOVED,
                changed_fields={"dependency_id": dependency_id},
                caller=caller,
                context=f"Removed upstream dependency",
            )
        return {"status": "deleted"}

    # -----------------------------------------------------------------------
    # Data contracts (Build 16)
    # -----------------------------------------------------------------------

    class CreateDataContractRequest(BaseModel):
        producer_pipeline_id: str
        consumer_pipeline_id: str
        description: Optional[str] = ""
        required_columns: Optional[list] = []
        freshness_sla_minutes: Optional[int] = 60
        retention_hours: Optional[int] = 168
        cleanup_ownership: Optional[str] = "none"

    class PatchDataContractRequest(BaseModel):
        description: Optional[str] = None
        required_columns: Optional[list] = None
        freshness_sla_minutes: Optional[int] = None
        retention_hours: Optional[int] = None
        cleanup_ownership: Optional[str] = None
        status: Optional[str] = None

    @app.post("/api/data-contracts")
    @limiter.limit("100/minute")
    async def create_data_contract(
        request: Request,
        req: CreateDataContractRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        # Validate pipelines exist
        producer = await store.get_pipeline(req.producer_pipeline_id)
        if not producer:
            raise HTTPException(404, "Producer pipeline not found")
        consumer = await store.get_pipeline(req.consumer_pipeline_id)
        if not consumer:
            raise HTTPException(404, "Consumer pipeline not found")
        if req.producer_pipeline_id == req.consumer_pipeline_id:
            raise HTTPException(400, "Producer and consumer cannot be the same pipeline")

        # Check for duplicate
        existing = await store.list_data_contracts(
            producer_id=req.producer_pipeline_id,
            consumer_id=req.consumer_pipeline_id,
        )
        if existing:
            raise HTTPException(409, "Data contract already exists between these pipelines")

        dc = DataContract(
            producer_pipeline_id=req.producer_pipeline_id,
            consumer_pipeline_id=req.consumer_pipeline_id,
            description=req.description or "",
            required_columns=req.required_columns or [],
            freshness_sla_minutes=req.freshness_sla_minutes or 60,
            retention_hours=req.retention_hours or 168,
            cleanup_ownership=CleanupOwnership(req.cleanup_ownership.lower()) if req.cleanup_ownership else CleanupOwnership.NONE,
        )
        await store.save_data_contract(dc)

        # Auto-create dependency (consumer depends on producer) if not exists
        deps = await store.list_dependencies(req.consumer_pipeline_id)
        has_dep = any(d.depends_on_id == req.producer_pipeline_id for d in deps)
        if not has_dep:
            dep = PipelineDependency(
                pipeline_id=req.consumer_pipeline_id,
                depends_on_id=req.producer_pipeline_id,
                dependency_type=DependencyType.USER_DEFINED,
                notes="auto-created by data contract",
            )
            await store.save_dependency(dep)

        await store.save_decision(DecisionLog(
            pipeline_id=req.producer_pipeline_id,
            decision_type="data_contract_created",
            detail=f"Contract with consumer {consumer.pipeline_name}",
            reasoning=req.description or "",
        ))

        return {
            "contract_id": dc.contract_id,
            "producer_pipeline_id": dc.producer_pipeline_id,
            "consumer_pipeline_id": dc.consumer_pipeline_id,
            "status": dc.status.value,
        }

    @app.get("/api/data-contracts")
    @limiter.limit("100/minute")
    async def list_data_contracts(
        request: Request,
        producer_id: Optional[str] = None,
        consumer_id: Optional[str] = None,
        status: Optional[str] = None,
        caller: dict = Depends(auth_dep),
    ):
        contracts = await store.list_data_contracts(
            producer_id=producer_id,
            consumer_id=consumer_id,
            status=status,
        )
        return {
            "contracts": [
                {
                    "contract_id": c.contract_id,
                    "producer_pipeline_id": c.producer_pipeline_id,
                    "consumer_pipeline_id": c.consumer_pipeline_id,
                    "description": c.description,
                    "status": c.status.value if hasattr(c.status, "value") else c.status,
                    "freshness_sla_minutes": c.freshness_sla_minutes,
                    "retention_hours": c.retention_hours,
                    "cleanup_ownership": c.cleanup_ownership.value if hasattr(c.cleanup_ownership, "value") else c.cleanup_ownership,
                    "required_columns": c.required_columns,
                    "violation_count": c.violation_count,
                    "last_validated_at": c.last_validated_at,
                    "created_at": c.created_at,
                }
                for c in contracts
            ],
            "total": len(contracts),
        }

    @app.get("/api/data-contracts/{contract_id}")
    @limiter.limit("100/minute")
    async def get_data_contract(
        request: Request,
        contract_id: str,
        caller: dict = Depends(auth_dep),
    ):
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")
        violations = await store.list_contract_violations(contract_id)
        producer = await store.get_pipeline(dc.producer_pipeline_id)
        consumer = await store.get_pipeline(dc.consumer_pipeline_id)
        return {
            "contract_id": dc.contract_id,
            "producer_pipeline_id": dc.producer_pipeline_id,
            "producer_pipeline_name": producer.pipeline_name if producer else None,
            "consumer_pipeline_id": dc.consumer_pipeline_id,
            "consumer_pipeline_name": consumer.pipeline_name if consumer else None,
            "description": dc.description,
            "status": dc.status.value if hasattr(dc.status, "value") else dc.status,
            "required_columns": dc.required_columns,
            "freshness_sla_minutes": dc.freshness_sla_minutes,
            "retention_hours": dc.retention_hours,
            "cleanup_ownership": dc.cleanup_ownership.value if hasattr(dc.cleanup_ownership, "value") else dc.cleanup_ownership,
            "violation_count": dc.violation_count,
            "last_validated_at": dc.last_validated_at,
            "last_violation_at": dc.last_violation_at,
            "created_at": dc.created_at,
            "updated_at": dc.updated_at,
            "recent_violations": [
                {
                    "violation_id": v.violation_id,
                    "violation_type": v.violation_type.value if hasattr(v.violation_type, "value") else v.violation_type,
                    "detail": v.detail,
                    "resolved": v.resolved,
                    "created_at": v.created_at,
                }
                for v in violations[:20]
            ],
        }

    @app.patch("/api/data-contracts/{contract_id}")
    @limiter.limit("100/minute")
    async def patch_data_contract(
        request: Request,
        contract_id: str,
        req: PatchDataContractRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")
        if req.description is not None:
            dc.description = req.description
        if req.required_columns is not None:
            dc.required_columns = req.required_columns
        if req.freshness_sla_minutes is not None:
            dc.freshness_sla_minutes = req.freshness_sla_minutes
        if req.retention_hours is not None:
            dc.retention_hours = req.retention_hours
        if req.cleanup_ownership is not None:
            dc.cleanup_ownership = CleanupOwnership(req.cleanup_ownership.lower())
        if req.status is not None:
            dc.status = DataContractStatus(req.status.lower())
        await store.save_data_contract(dc)
        return {"contract_id": dc.contract_id, "status": dc.status.value}

    @app.delete("/api/data-contracts/{contract_id}")
    @limiter.limit("100/minute")
    async def delete_data_contract(
        request: Request,
        contract_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")
        await store.delete_data_contract(contract_id)
        return {"status": "deleted"}

    @app.post("/api/data-contracts/{contract_id}/validate")
    @limiter.limit("30/minute")
    async def validate_data_contract(
        request: Request,
        contract_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")

        violations = []

        # Freshness SLA check
        last_run = await store.get_last_successful_run(dc.producer_pipeline_id)
        now = datetime.now(timezone.utc)
        if last_run and last_run.completed_at:
            completed = datetime.fromisoformat(last_run.completed_at).replace(tzinfo=timezone.utc)
            staleness_minutes = (now - completed).total_seconds() / 60
            if staleness_minutes > dc.freshness_sla_minutes:
                v = ContractViolation(
                    contract_id=dc.contract_id,
                    violation_type=ContractViolationType.FRESHNESS_SLA,
                    detail=f"Producer data is {staleness_minutes:.0f}m old, SLA is {dc.freshness_sla_minutes}m",
                    producer_pipeline_id=dc.producer_pipeline_id,
                    consumer_pipeline_id=dc.consumer_pipeline_id,
                )
                violations.append(v)
        elif not last_run:
            v = ContractViolation(
                contract_id=dc.contract_id,
                violation_type=ContractViolationType.FRESHNESS_SLA,
                detail="Producer has no successful runs",
                producer_pipeline_id=dc.producer_pipeline_id,
                consumer_pipeline_id=dc.consumer_pipeline_id,
            )
            violations.append(v)

        # Required columns check
        if dc.required_columns:
            producer = await store.get_pipeline(dc.producer_pipeline_id)
            if producer and producer.column_mappings:
                target_columns = {m.target_column for m in producer.column_mappings}
                missing = [c for c in dc.required_columns if c not in target_columns]
                if missing:
                    v = ContractViolation(
                        contract_id=dc.contract_id,
                        violation_type=ContractViolationType.SCHEMA_MISMATCH,
                        detail=f"Missing required columns: {', '.join(missing)}",
                        producer_pipeline_id=dc.producer_pipeline_id,
                        consumer_pipeline_id=dc.consumer_pipeline_id,
                    )
                    violations.append(v)

        # Record violations
        for v in violations:
            await store.save_contract_violation(v)

        dc.last_validated_at = now_iso()
        if violations:
            dc.status = DataContractStatus.VIOLATED
            dc.last_violation_at = now_iso()
            dc.violation_count += len(violations)
        else:
            dc.status = DataContractStatus.ACTIVE
        await store.save_data_contract(dc)

        return {
            "contract_id": dc.contract_id,
            "status": dc.status.value,
            "violations_found": len(violations),
            "violations": [
                {
                    "violation_type": v.violation_type.value,
                    "detail": v.detail,
                }
                for v in violations
            ],
        }

    @app.get("/api/data-contracts/{contract_id}/violations")
    @limiter.limit("100/minute")
    async def list_data_contract_violations(
        request: Request,
        contract_id: str,
        resolved: Optional[bool] = None,
        caller: dict = Depends(auth_dep),
    ):
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")
        violations = await store.list_contract_violations(contract_id, resolved=resolved)
        return {
            "violations": [
                {
                    "violation_id": v.violation_id,
                    "violation_type": v.violation_type.value if hasattr(v.violation_type, "value") else v.violation_type,
                    "detail": v.detail,
                    "producer_pipeline_id": v.producer_pipeline_id,
                    "consumer_pipeline_id": v.consumer_pipeline_id,
                    "resolved": v.resolved,
                    "resolved_at": v.resolved_at,
                    "created_at": v.created_at,
                }
                for v in violations
            ],
            "total": len(violations),
        }

    @app.post("/api/data-contracts/{contract_id}/violations/{violation_id}/resolve")
    @limiter.limit("100/minute")
    async def resolve_data_contract_violation(
        request: Request,
        contract_id: str,
        violation_id: str,
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        dc = await store.get_data_contract(contract_id)
        if not dc:
            raise HTTPException(404, "Data contract not found")
        await store.resolve_contract_violation(violation_id)
        return {"status": "resolved"}

    # -----------------------------------------------------------------------
    # Topology reasoning (Build 20)
    # -----------------------------------------------------------------------

    class TopologyDesignRequest(BaseModel):
        description: str

    @app.post("/api/topology/design")
    @limiter.limit("10/minute")
    async def design_topology_endpoint(
        request: Request,
        req: TopologyDesignRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        existing_p = await store.list_pipelines()
        existing_c = await store.list_connectors(status="active")
        topology = await agent.design_topology(
            req.description,
            existing_pipelines=[_pipeline_summary(p) for p in existing_p],
            existing_connectors=[
                {"connector_name": c.connector_name, "connector_type": c.connector_type.value, "source_target_type": c.source_target_type}
                for c in existing_c
            ],
        )
        return topology

    # -----------------------------------------------------------------------
    # DAG visualization (Build 19)
    # -----------------------------------------------------------------------

    @app.get("/api/dag")
    @limiter.limit("60/minute")
    async def get_dag(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        """Return full pipeline dependency graph for DAG visualization."""
        pipelines = await store.list_pipelines()
        nodes = []
        edges = []
        seen_edges = set()

        for p in pipelines:
            # Last run info
            last_run = await store.get_last_successful_run(p.pipeline_id)
            last_run_info = None
            if last_run:
                last_run_info = {
                    "run_id": last_run.run_id,
                    "status": last_run.status.value if hasattr(last_run.status, "value") else last_run.status,
                    "completed_at": last_run.completed_at,
                    "rows_loaded": last_run.rows_loaded,
                }

            # Data contracts summary
            produced = await store.list_data_contracts(producer_id=p.pipeline_id)
            consumed = await store.list_data_contracts(consumer_id=p.pipeline_id)
            contract_violations = sum(c.violation_count for c in produced + consumed)

            nodes.append({
                "id": p.pipeline_id,
                "name": p.pipeline_name,
                "status": p.status.value if hasattr(p.status, "value") else p.status,
                "tier": p.tier,
                "owner": p.owner,
                "schedule_cron": p.schedule_cron,
                "refresh_type": p.refresh_type.value if hasattr(p.refresh_type, "value") else p.refresh_type,
                "source": f"{p.source_schema}.{p.source_table}",
                "target": f"{p.target_schema}.{p.target_table}",
                "last_run": last_run_info,
                "contracts_as_producer": len(produced),
                "contracts_as_consumer": len(consumed),
                "contract_violations": contract_violations,
            })

            # Gather edges from dependencies
            deps = await store.list_dependencies(p.pipeline_id)
            for dep in deps:
                edge_key = f"{dep.depends_on_id}->{p.pipeline_id}"
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append({
                        "from": dep.depends_on_id,
                        "to": p.pipeline_id,
                        "type": dep.dependency_type.value if hasattr(dep.dependency_type, "value") else dep.dependency_type,
                        "notes": dep.notes,
                    })

        return {
            "nodes": nodes,
            "edges": edges,
            "total_pipelines": len(nodes),
            "total_edges": len(edges),
        }

    # -----------------------------------------------------------------------
    # Pipeline metadata (Build 11 - XCom-style)
    # -----------------------------------------------------------------------

    class SetMetadataRequest(BaseModel):
        value: dict
        namespace: Optional[str] = "default"

    @app.get("/api/pipelines/{pipeline_id}/metadata")
    @limiter.limit("100/minute")
    async def list_pipeline_metadata(
        request: Request,
        pipeline_id: str,
        namespace: Optional[str] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        items = await store.list_metadata(pipeline_id, namespace)
        return [
            {
                "id": m.id,
                "pipeline_id": m.pipeline_id,
                "namespace": m.namespace,
                "key": m.key,
                "value": m.value_json,
                "updated_at": m.updated_at,
                "created_by_run_id": m.created_by_run_id,
            }
            for m in items
        ]

    @app.get("/api/pipelines/{pipeline_id}/metadata/{key}")
    @limiter.limit("100/minute")
    async def get_pipeline_metadata(
        request: Request,
        pipeline_id: str,
        key: str,
        namespace: str = Query("default"),
        caller: dict = Depends(auth_dep),
    ):
        m = await store.get_metadata(pipeline_id, key, namespace)
        if not m:
            raise HTTPException(404, "Metadata key not found")
        return {
            "id": m.id,
            "pipeline_id": m.pipeline_id,
            "namespace": m.namespace,
            "key": m.key,
            "value": m.value_json,
            "updated_at": m.updated_at,
            "created_by_run_id": m.created_by_run_id,
        }

    @app.put("/api/pipelines/{pipeline_id}/metadata/{key}")
    @limiter.limit("100/minute")
    async def set_pipeline_metadata(
        request: Request,
        pipeline_id: str,
        key: str,
        req: SetMetadataRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        await store.set_metadata(pipeline_id, key, req.value, namespace=req.namespace)
        return {"status": "ok", "key": key}

    @app.delete("/api/pipelines/{pipeline_id}/metadata/{key}")
    @limiter.limit("100/minute")
    async def delete_pipeline_metadata(
        request: Request,
        pipeline_id: str,
        key: str,
        namespace: str = Query("default"),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        await store.delete_metadata(pipeline_id, key, namespace)
        return {"status": "deleted"}

    # -----------------------------------------------------------------------
    # Schema policy defaults (Build 12)
    # -----------------------------------------------------------------------

    @app.get("/api/schema-policy-defaults")
    @limiter.limit("100/minute")
    async def get_schema_policy_defaults(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        from dataclasses import asdict as _asdict
        return {
            str(tier): _asdict(policy)
            for tier, policy in SCHEMA_POLICY_TIER_DEFAULTS.items()
        }

    # -----------------------------------------------------------------------
    # Post-Promotion Hook Testing (Build 13)
    # -----------------------------------------------------------------------

    class TestHookRequest(BaseModel):
        sql: str
        timeout_seconds: int = 30

    @app.post("/api/pipelines/{pipeline_id}/hooks/test")
    @limiter.limit("30/minute")
    async def test_hook(
        request: Request,
        pipeline_id: str,
        req: TestHookRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        # Resolve target connector
        import json as _json
        tgt_params = {
            "host": p.target_host,
            "port": p.target_port,
            "database": p.target_database,
            "user": p.target_user,
            "password": p.target_password,
            "default_schema": p.target_schema,
        }
        if config.has_encryption_key:
            from crypto import decrypt_dict, CREDENTIAL_FIELDS
            tgt_params = decrypt_dict(
                tgt_params, config.encryption_key, CREDENTIAL_FIELDS,
            )
        target = await registry.get_target(
            p.target_connector_id, tgt_params,
        )

        t0 = time.monotonic()
        try:
            rows = await target.execute_sql(req.sql, req.timeout_seconds)
            duration_ms = int((time.monotonic() - t0) * 1000)
            # Convert to JSON-safe format
            safe_rows = []
            for r in rows[:100]:  # Cap at 100 rows for safety
                safe_row = {}
                for k, v in r.items():
                    if hasattr(v, "isoformat"):
                        safe_row[k] = v.isoformat()
                    elif isinstance(v, decimal.Decimal):
                        safe_row[k] = float(v)
                    elif isinstance(v, bytes):
                        safe_row[k] = v.decode("utf-8", errors="replace")
                    else:
                        safe_row[k] = v
                safe_rows.append(safe_row)
            return {
                "status": "success",
                "duration_ms": duration_ms,
                "rows_returned": len(rows),
                "rows": safe_rows,
            }
        except NotImplementedError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            duration_ms = int((time.monotonic() - t0) * 1000)
            return {
                "status": "error",
                "duration_ms": duration_ms,
                "error": str(e),
            }

    # -----------------------------------------------------------------------
    # Error Budgets
    # -----------------------------------------------------------------------

    @app.get("/api/error-budgets/{pipeline_id}")
    @limiter.limit("100/minute")
    async def get_error_budget(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        budget = await store.get_error_budget(pipeline_id)
        if not budget:
            return {
                "pipeline_id": pipeline_id,
                "budget": None,
                "message": "No error budget configured for this pipeline.",
            }
        return {
            "pipeline_id": pipeline_id,
            "window_days": budget.window_days,
            "total_runs": budget.total_runs,
            "successful_runs": budget.successful_runs,
            "failed_runs": budget.failed_runs,
            "success_rate": budget.success_rate,
            "budget_threshold": budget.budget_threshold,
            "budget_remaining": budget.budget_remaining,
            "escalated": budget.escalated,
            "last_calculated": budget.last_calculated,
            "status": (
                "ok"
                if not budget.escalated and budget.budget_remaining > 0.05
                else "warning"
                if not budget.escalated
                else "exhausted"
            ),
        }

    # -----------------------------------------------------------------------
    # Agent Costs
    # -----------------------------------------------------------------------

    @app.get("/api/agent-costs")
    @limiter.limit("100/minute")
    async def list_agent_costs(
        request: Request,
        pipeline_id: Optional[str] = Query(None),
        hours: int = Query(24),
        caller: dict = Depends(auth_dep),
    ):
        costs = await store.list_agent_costs(pipeline_id=pipeline_id, hours=hours)
        return [
            {
                "cost_id": c.cost_id,
                "pipeline_id": c.pipeline_id,
                "operation": c.operation,
                "model": c.model,
                "input_tokens": c.input_tokens,
                "output_tokens": c.output_tokens,
                "total_tokens": c.total_tokens,
                "cost_usd": c.cost_usd,
                "created_at": c.created_at,
            }
            for c in costs
        ]

    @app.get("/api/agent-costs/summary")
    @limiter.limit("100/minute")
    async def agent_costs_summary(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        summary = await store.get_total_cost_summary()
        return summary

    # -----------------------------------------------------------------------
    # Connector Migrations
    # -----------------------------------------------------------------------

    @app.get("/api/connector-migrations")
    @limiter.limit("100/minute")
    async def list_connector_migrations(
        request: Request,
        connector_id: Optional[str] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        migrations = await store.list_connector_migrations(connector_id=connector_id)
        return [
            {
                "migration_id": m.migration_id,
                "connector_id": m.connector_id,
                "from_version": m.from_version,
                "to_version": m.to_version,
                "migration_type": m.migration_type,
                "status": m.status,
                "started_at": m.started_at,
                "completed_at": m.completed_at,
                "rollback_available": m.rollback_available,
            }
            for m in migrations
        ]

    # -----------------------------------------------------------------------
    # Notification Policies
    # -----------------------------------------------------------------------

    @app.get("/api/policies")
    @limiter.limit("100/minute")
    async def list_policies(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        policies = await store.list_policies()
        return [
            {
                "policy_id": p.policy_id,
                "policy_name": p.policy_name,
                "description": p.description,
                "channels": p.channels,
                "digest_hour": p.digest_hour,
            }
            for p in policies
        ]

    @app.post("/api/policies")
    @limiter.limit("100/minute")
    async def create_policy(
        request: Request,
        req: CreatePolicyRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        policy = NotificationPolicy(
            policy_name=req.policy_name,
            description=req.description,
            channels=req.channels,
            digest_hour=req.digest_hour,
        )
        await store.save_policy(policy)
        return {"policy_id": policy.policy_id}

    @app.get("/api/policies/{policy_id}")
    @limiter.limit("100/minute")
    async def get_policy(
        request: Request,
        policy_id: str,
        caller: dict = Depends(auth_dep),
    ):
        policy = await store.get_policy(policy_id)
        if not policy:
            raise HTTPException(404, "Policy not found")
        return {
            "policy_id": policy.policy_id,
            "policy_name": policy.policy_name,
            "description": policy.description,
            "channels": policy.channels,
            "digest_hour": policy.digest_hour,
        }

    @app.patch("/api/policies/{policy_id}")
    @limiter.limit("100/minute")
    async def update_policy(
        request: Request,
        policy_id: str,
        req: UpdatePolicyRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        policy = await store.get_policy(policy_id)
        if not policy:
            raise HTTPException(404, "Policy not found")
        if req.policy_name is not None:
            policy.policy_name = req.policy_name
        if req.description is not None:
            policy.description = req.description
        if req.channels is not None:
            policy.channels = req.channels
        if req.digest_hour is not None:
            policy.digest_hour = req.digest_hour
        await store.save_policy(policy)
        return {"policy_id": policy_id, "status": "updated"}

    @app.delete("/api/policies/{policy_id}")
    @limiter.limit("100/minute")
    async def delete_policy(
        request: Request,
        policy_id: str,
        caller: dict = Depends(auth_dep),
    ):
        await store.delete_policy(policy_id)
        return {"status": "deleted"}

    # -----------------------------------------------------------------------
    # Agent Preferences
    # -----------------------------------------------------------------------

    @app.get("/api/preferences")
    @limiter.limit("100/minute")
    async def list_preferences(
        request: Request,
        scope: Optional[str] = Query(None),
        scope_value: Optional[str] = Query(None),
        caller: dict = Depends(auth_dep),
    ):
        prefs = await store.get_preferences(scope=scope, scope_value=scope_value)
        return [
            {
                "preference_id": p.preference_id,
                "scope": p.scope.value if hasattr(p.scope, "value") else p.scope,
                "scope_value": p.scope_value,
                "preference_key": p.preference_key,
                "preference_value": p.preference_value,
                "source": p.source.value if hasattr(p.source, "value") else p.source,
                "confidence": p.confidence,
                "usage_count": p.usage_count,
            }
            for p in prefs
        ]

    @app.post("/api/preferences")
    @limiter.limit("100/minute")
    async def set_preference(
        request: Request,
        req: SetPreferenceRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        pref = AgentPreference(
            scope=req.scope,
            scope_value=req.scope_value,
            preference_key=req.preference_key,
            preference_value=req.preference_value,
            source="user_explicit",
            confidence=req.confidence,
        )
        await store.save_preference(pref)
        return {"preference_id": pref.preference_id}

    @app.delete("/api/preferences/{preference_id}")
    @limiter.limit("100/minute")
    async def delete_preference(
        request: Request,
        preference_id: str,
        caller: dict = Depends(auth_dep),
    ):
        await store.delete_preference(preference_id)
        return {"status": "deleted"}

    # -----------------------------------------------------------------------
    # Build 32: Business Knowledge & Agent Context
    # -----------------------------------------------------------------------

    @app.get("/api/agent/system-prompt")
    @limiter.limit("100/minute")
    async def get_system_prompt(request: Request, caller: dict = Depends(auth_dep)):
        """Read-only view of the agent's full system prompt (base + business knowledge)."""
        prompt = agent._system_prompt()
        return {"system_prompt": prompt}

    @app.get("/api/settings/business-knowledge")
    @limiter.limit("100/minute")
    async def get_business_knowledge(request: Request, caller: dict = Depends(auth_dep)):
        """Get the business knowledge context used by the agent."""
        from dataclasses import asdict as _asdict
        bk = await store.get_business_knowledge()
        return _asdict(bk)

    @app.put("/api/settings/business-knowledge")
    @limiter.limit("30/minute")
    async def save_business_knowledge(request: Request, req: dict = Body(...), caller: dict = Depends(auth_dep)):
        """Update business knowledge — feeds into all agent reasoning."""
        require_role(caller, "admin", "operator")
        from contracts.models import BusinessKnowledge
        bk = await store.get_business_knowledge()
        # Merge fields
        if "company_name" in req:
            bk.company_name = req["company_name"]
        if "industry" in req:
            bk.industry = req["industry"]
        if "business_description" in req:
            bk.business_description = req["business_description"]
        if "datasets_description" in req:
            bk.datasets_description = req["datasets_description"]
        if "glossary" in req:
            bk.glossary = req["glossary"]
        if "kpi_definitions" in req:
            bk.kpi_definitions = req["kpi_definitions"]
        if "custom_instructions" in req:
            bk.custom_instructions = req["custom_instructions"]
        bk.updated_by = caller.get("username", "")
        await store.save_business_knowledge(bk)
        from dataclasses import asdict as _asdict
        return _asdict(bk)

    @app.post("/api/settings/business-knowledge/parse-kpis")
    @limiter.limit("10/minute")
    async def parse_kpi_text(request: Request, req: dict = Body(...), caller: dict = Depends(auth_dep)):
        """Agent parses free-text KPI definitions into structured format."""
        require_role(caller, "admin", "operator")
        text = req.get("text", "")
        if not text:
            raise HTTPException(400, "text is required")
        if agent.has_api:
            parsed = await agent.parse_kpi_definitions(text)
            return {"kpi_definitions": parsed}
        else:
            # Rule-based fallback: split on newlines, each line is a KPI
            kpis = []
            for line in text.strip().split("\n"):
                line = line.strip().lstrip("-•*").strip()
                if line:
                    kpis.append({"name": line, "description": line, "source": "user_text"})
            return {"kpi_definitions": kpis}

    # -----------------------------------------------------------------------
    # GitOps
    # -----------------------------------------------------------------------

    @app.get("/api/gitops/status")
    @limiter.limit("100/minute")
    async def gitops_status(request: Request, caller: dict = Depends(auth_dep)):
        if not gitops:
            return {"enabled": False}
        return gitops.status()

    @app.get("/api/gitops/log")
    @limiter.limit("100/minute")
    async def gitops_log(
        request: Request,
        limit: int = Query(20),
        caller: dict = Depends(auth_dep),
    ):
        if not gitops:
            return []
        return gitops.get_log(limit=limit)

    @app.get("/api/gitops/pipelines/{pipeline_id}/history")
    @limiter.limit("100/minute")
    async def gitops_pipeline_history(
        request: Request,
        pipeline_id: str,
        limit: int = Query(20),
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        if not gitops:
            return []
        return gitops.get_pipeline_history(p.pipeline_name, limit=limit)

    @app.get("/api/gitops/diff")
    @limiter.limit("100/minute")
    async def gitops_diff(
        request: Request,
        commit_a: str = Query("HEAD~1"),
        commit_b: str = Query("HEAD"),
        caller: dict = Depends(auth_dep),
    ):
        if not gitops:
            return {"diff": ""}
        return {"diff": gitops.get_diff(commit_a, commit_b)}

    @app.get("/api/gitops/file")
    @limiter.limit("100/minute")
    async def gitops_file_at_commit(
        request: Request,
        filepath: str = Query(...),
        commit: str = Query("HEAD"),
        caller: dict = Depends(auth_dep),
    ):
        if not gitops:
            raise HTTPException(404, "GitOps not enabled")
        content = gitops.get_file_at_commit(filepath, commit)
        if content is None:
            raise HTTPException(404, "File not found at commit")
        return PlainTextResponse(content)

    @app.post("/api/gitops/restore")
    @limiter.limit("5/minute")
    async def gitops_restore(
        request: Request,
        dry_run: bool = Query(True),
        caller: dict = Depends(auth_dep),
    ):
        """Restore pipelines and connectors from GitOps repo into the database.

        Reads all pipeline YAML and connector .py files from the repo and
        upserts them into PostgreSQL. Use dry_run=true (default) to preview
        what would be restored without making changes.
        """
        require_role(caller, "admin")
        if not gitops or not gitops.enabled:
            raise HTTPException(404, "GitOps not enabled")

        # Pull latest from remote before restore (offloaded to thread)
        if gitops.has_remote:
            import asyncio as _aio
            await _aio.get_running_loop().run_in_executor(None, gitops._pull)

        results = {
            "pipelines_found": 0,
            "pipelines_restored": 0,
            "pipelines_skipped": 0,
            "connectors_found": 0,
            "connectors_restored": 0,
            "connectors_skipped": 0,
            "errors": [],
            "dry_run": dry_run,
            "details": [],
        }

        # --- Restore pipelines from YAML ---
        yamls = gitops.read_all_pipeline_yamls()
        results["pipelines_found"] = len(yamls)

        for yaml_str in yamls:
            try:
                pipelines = yaml_to_pipelines(yaml_str, preserve_id=True)
                for p in pipelines:
                    existing = await store.get_pipeline(p.pipeline_id)
                    action = "update" if existing else "create"
                    results["details"].append({
                        "type": "pipeline",
                        "name": p.pipeline_name,
                        "id": p.pipeline_id,
                        "action": action,
                        "version": p.version,
                    })
                    if not dry_run:
                        await store.save_pipeline(p)
                        results["pipelines_restored"] += 1
                    else:
                        results["pipelines_skipped"] += 1
            except Exception as e:
                results["errors"].append(f"Pipeline YAML parse error: {e}")

        # --- Restore connectors from .py files ---
        connector_files = gitops.read_all_connector_files()
        results["connectors_found"] = len(connector_files)

        for cf in connector_files:
            try:
                name = cf.get("name", cf.get("filename", "unknown"))
                connector_id = cf.get("connector_id", "")
                code = cf.get("code", "")

                if not code or code.strip() == "# No code available":
                    results["details"].append({
                        "type": "connector",
                        "name": name,
                        "action": "skip_empty",
                    })
                    results["connectors_skipped"] += 1
                    continue

                # Try to find existing connector by ID or name
                existing = None
                if connector_id:
                    existing = await store.get_connector(connector_id)
                if not existing:
                    connectors = await store.list_connectors()
                    existing = next((c for c in connectors if c.connector_name == name), None)

                action = "update" if existing else "create"
                results["details"].append({
                    "type": "connector",
                    "name": name,
                    "id": connector_id or (existing.connector_id if existing else "new"),
                    "action": action,
                    "version": cf.get("version", 1),
                })

                if not dry_run:
                    if existing:
                        existing.code = code
                        existing.version = cf.get("version", existing.version)
                        existing.updated_at = now_iso()
                        await store.save_connector(existing)
                        registry.register_approved_connector(existing)
                    else:
                        from contracts.models import ConnectorRecord, ConnectorType, ConnectorStatus
                        ct = cf.get("connector_type", "source").lower()
                        new_conn = ConnectorRecord(
                            connector_id=connector_id or new_id(),
                            connector_name=name,
                            connector_type=ConnectorType(ct) if ct in ("source", "target") else ConnectorType.SOURCE,
                            code=code,
                            version=cf.get("version", 1),
                            status=ConnectorStatus.ACTIVE,
                            generated_by="gitops-restore",
                        )
                        await store.save_connector(new_conn)
                        registry.register_approved_connector(new_conn)
                    results["connectors_restored"] += 1
                else:
                    results["connectors_skipped"] += 1
            except Exception as e:
                results["errors"].append(f"Connector '{cf.get('name', '?')}' restore error: {e}")

        return results

    # -----------------------------------------------------------------------
    # Documentation API (Build 24)
    # -----------------------------------------------------------------------

    _docs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")

    @app.get("/api/docs")
    async def list_docs():
        """List available documentation files as a tree."""
        if not os.path.exists(_docs_dir):
            return {"docs": []}
        result = []
        for root, dirs, files in os.walk(_docs_dir):
            dirs.sort()
            for f in sorted(files):
                if not f.endswith(".md"):
                    continue
                rel = os.path.relpath(os.path.join(root, f), _docs_dir)
                # Parse title from first heading
                full = os.path.join(root, f)
                title = f.replace(".md", "").replace("-", " ").title()
                try:
                    with open(full, "r") as fh:
                        for line in fh:
                            line = line.strip()
                            if line.startswith("# "):
                                title = line[2:].strip()
                                break
                except Exception:
                    pass
                # Determine section from directory
                parts = rel.replace("\\", "/").split("/")
                section = parts[0] if len(parts) > 1 else "root"
                result.append({
                    "path": rel.replace("\\", "/"),
                    "title": title,
                    "section": section,
                })
        return {"docs": result}

    @app.get("/api/docs/{doc_path:path}")
    async def get_doc(doc_path: str):
        """Get a documentation file's markdown content."""
        if not doc_path.endswith(".md"):
            doc_path += ".md"
        safe_path = os.path.normpath(doc_path)
        if ".." in safe_path:
            raise HTTPException(400, "Invalid path")
        full = os.path.join(_docs_dir, safe_path)
        if not os.path.exists(full):
            raise HTTPException(404, "Document not found")
        try:
            with open(full, "r") as f:
                content = f.read()
            return {"path": safe_path, "content": content}
        except Exception as e:
            raise HTTPException(500, f"Error reading doc: {e}")

    # -----------------------------------------------------------------------
    # Agent Diagnostic & Reasoning (Build 24)
    # -----------------------------------------------------------------------

    @app.post("/api/pipelines/{pipeline_id}/diagnose")
    @limiter.limit("10/minute")
    async def diagnose_pipeline_endpoint(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Root-cause diagnosis for a pipeline."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        result = await agent.diagnose_pipeline(pipeline_id)
        return result

    @app.post("/api/pipelines/{pipeline_id}/impact")
    @limiter.limit("10/minute")
    async def analyze_impact_endpoint(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Analyze downstream impact if a pipeline goes down."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        result = await agent.analyze_impact(pipeline_id)
        return result

    @app.get("/api/observability/anomalies")
    @limiter.limit("10/minute")
    async def get_anomalies_endpoint(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        """Platform-wide anomaly detection with contextual reasoning."""
        result = await agent.reason_about_anomalies()
        return result

    # -----------------------------------------------------------------------
    # Step DAG endpoints (Build 18)
    # -----------------------------------------------------------------------

    @app.get("/api/pipelines/{pipeline_id}/steps")
    async def get_pipeline_steps(
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get step DAG definition for a pipeline."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": p.pipeline_name,
            "steps": [
                {
                    "step_id": s.step_id,
                    "step_name": s.step_name,
                    "step_type": s.step_type.value if hasattr(s.step_type, "value") else s.step_type,
                    "depends_on": s.depends_on,
                    "config": s.config,
                    "retry_max": s.retry_max,
                    "timeout_seconds": s.timeout_seconds,
                    "skip_on_fail": s.skip_on_fail,
                    "enabled": s.enabled,
                }
                for s in (p.steps or [])
            ],
        }

    @app.get("/api/runs/{run_id}/steps")
    async def get_run_steps(
        run_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get step executions for a specific run."""
        run = await store.get_run(run_id)
        if not run:
            raise HTTPException(404, "Run not found")
        step_execs = await store.list_step_executions(run_id)
        return {
            "run_id": run_id,
            "pipeline_id": run.pipeline_id,
            "steps": [
                {
                    "step_execution_id": se.step_execution_id,
                    "step_id": se.step_id,
                    "step_name": se.step_name,
                    "step_type": se.step_type,
                    "status": se.status.value if hasattr(se.status, "value") else se.status,
                    "started_at": se.started_at,
                    "completed_at": se.completed_at,
                    "elapsed_ms": se.elapsed_ms,
                    "output": se.output,
                    "error": se.error,
                    "retry_count": se.retry_count,
                }
                for se in step_execs
            ],
        }

    @app.post("/api/pipelines/{pipeline_id}/steps/validate")
    async def validate_steps(
        pipeline_id: str,
        steps: list[dict] = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Validate a step DAG for cycles and missing dependencies."""
        require_role(caller, "admin", "operator")
        parsed = _parse_step_dicts(steps)
        from agent.autonomous import PipelineRunner as _PR
        try:
            ordered = _PR._topo_sort(parsed)
            return {
                "valid": True,
                "execution_order": [
                    {"step_id": s.step_id, "step_name": s.step_name, "step_type": s.step_type.value}
                    for s in ordered
                ],
            }
        except ValueError as e:
            return {"valid": False, "error": str(e)}

    @app.get("/api/pipelines/{pipeline_id}/steps/preview")
    async def preview_step_execution(
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Preview the execution order for a pipeline's step DAG."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        if not p.steps:
            return {"pipeline_id": pipeline_id, "mode": "legacy", "execution_order": []}
        enabled = [s for s in p.steps if s.enabled]
        from agent.autonomous import PipelineRunner as _PR
        try:
            ordered = _PR._topo_sort(enabled)
            return {
                "pipeline_id": pipeline_id,
                "mode": "step_dag",
                "execution_order": [
                    {"step_id": s.step_id, "step_name": s.step_name, "step_type": s.step_type.value}
                    for s in ordered
                ],
            }
        except ValueError as e:
            return {"pipeline_id": pipeline_id, "mode": "step_dag", "error": str(e)}

    # -----------------------------------------------------------------------
    # Data Catalog API (Build 26)
    # -----------------------------------------------------------------------

    @app.get("/api/catalog/search")
    async def catalog_search(
        request: Request,
        q: str = Query("", description="Search query — matches table names, column names, pipeline names, tags"),
        source_type: Optional[str] = Query(None, description="Filter by source type (mysql, mongodb, etc.)"),
        status: Optional[str] = Query(None, description="Filter by pipeline status"),
        tier: Optional[int] = Query(None, description="Filter by observability tier (1/2/3)"),
        limit: int = Query(50, le=200),
        offset: int = Query(0),
        caller: dict = Depends(auth_dep),
    ):
        """Search the data catalog — returns tables with freshness, quality, lineage, and trust scores."""
        pipelines = await store.list_pipelines(status=status)

        results = []
        for p in pipelines:
            # Apply filters
            if tier is not None and p.tier != tier:
                continue

            # Text search across multiple fields
            if q:
                q_lower = q.lower()
                # Include semantic tags and business context in search
                sem_text = " ".join(
                    f"{t.get('semantic_name', '')} {t.get('domain', '')} {t.get('description', '')}"
                    for t in (p.semantic_tags or {}).values()
                )
                ctx_text = " ".join(str(v) for v in (p.business_context or {}).values() if isinstance(v, str))
                searchable = " ".join([
                    p.pipeline_name or "",
                    p.source_table or "",
                    p.target_table or "",
                    p.source_schema or "",
                    p.target_schema or "",
                    p.owner or "",
                    " ".join(str(v) for v in (p.tags or {}).values()),
                    " ".join(str(k) for k in (p.tags or {}).keys()),
                    " ".join(m.source_column for m in (p.column_mappings or [])),
                    " ".join(m.target_column for m in (p.column_mappings or [])),
                    sem_text,
                    ctx_text,
                ]).lower()
                if q_lower not in searchable:
                    continue

            # Source type filter
            if source_type:
                connector = await store.get_connector(p.source_connector_id) if p.source_connector_id else None
                if not connector or source_type.lower() not in (connector.source_target_type or "").lower():
                    continue

            # Gather freshness
            freshness = await store.get_latest_freshness(p.pipeline_id)
            freshness_info = None
            if freshness:
                freshness_info = {
                    "staleness_minutes": freshness.staleness_minutes,
                    "status": freshness.status.value if hasattr(freshness.status, "value") else freshness.status,
                    "sla_met": freshness.sla_met,
                    "checked_at": freshness.checked_at,
                }

            # Gather latest quality gate
            gates = await store.get_quality_trend(p.pipeline_id, limit=1)
            quality_info = None
            if gates:
                g = gates[0]
                checks_passed = sum(1 for c in (g.checks or []) if c.status in ("pass", CheckStatus.PASS))
                total_checks = len(g.checks or [])
                quality_info = {
                    "decision": g.decision.value if hasattr(g.decision, "value") else g.decision,
                    "checks_passed": checks_passed,
                    "total_checks": total_checks,
                    "evaluated_at": g.evaluated_at,
                }

            # Error budget
            budget = await store.get_error_budget(p.pipeline_id)
            budget_info = None
            if budget:
                budget_info = {
                    "success_rate": budget.success_rate,
                    "budget_remaining": budget.budget_remaining,
                    "escalated": budget.escalated,
                }

            # Compute trust score (0.0 - 1.0)
            trust = _compute_trust_score(freshness, gates[0] if gates else None, budget, p)

            # Column catalog
            columns = [
                {
                    "name": m.target_column or m.source_column,
                    "source_name": m.source_column,
                    "type": m.target_type or m.source_type,
                    "nullable": m.is_nullable,
                    "primary_key": m.is_primary_key,
                }
                for m in (p.column_mappings or [])
            ]

            results.append({
                "pipeline_id": p.pipeline_id,
                "pipeline_name": p.pipeline_name,
                "target_table": f"{p.target_schema}.{p.target_table}",
                "source_table": f"{p.source_schema}.{p.source_table}",
                "status": p.status.value if hasattr(p.status, "value") else p.status,
                "tier": p.tier,
                "owner": p.owner,
                "tags": p.tags or {},
                "refresh_type": p.refresh_type.value if hasattr(p.refresh_type, "value") else p.refresh_type,
                "schedule_cron": p.schedule_cron,
                "column_count": len(p.column_mappings or []),
                "columns": columns,
                "freshness": freshness_info,
                "quality": quality_info,
                "error_budget": budget_info,
                "trust_score": trust["score"],
                "trust_detail": trust["detail"],
                "semantic_tags": p.semantic_tags or {},
                "business_context": p.business_context or {},
                "created_at": p.created_at,
                "updated_at": p.updated_at,
            })

        # Paginate
        total = len(results)
        results = results[offset : offset + limit]
        return {"items": results, "total": total, "limit": limit, "offset": offset}

    @app.get("/api/catalog/tables/{pipeline_id}")
    async def catalog_table_detail(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Full catalog entry for a single table — includes columns, lineage, trust breakdown, quality history."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        # Freshness
        freshness = await store.get_latest_freshness(p.pipeline_id)
        freshness_history = await store.list_freshness_history(p.pipeline_id, hours=72)

        # Quality trend
        gates = await store.get_quality_trend(p.pipeline_id, limit=10)

        # Error budget
        budget = await store.get_error_budget(p.pipeline_id)

        # Column lineage
        lineage = await store.list_column_lineage(p.pipeline_id)

        # Schema versions
        schema_versions = await store.list_schema_versions(p.pipeline_id)

        # Recent runs
        runs = await store.list_runs(p.pipeline_id, limit=5)

        # Data contracts (as producer or consumer)
        contracts = await store.list_data_contracts()
        related_contracts = [
            {
                "contract_id": c.contract_id,
                "role": "producer" if c.producer_pipeline_id == p.pipeline_id else "consumer",
                "counterpart_id": c.consumer_pipeline_id if c.producer_pipeline_id == p.pipeline_id else c.producer_pipeline_id,
                "freshness_sla_minutes": c.freshness_sla_minutes,
                "status": c.status.value if hasattr(c.status, "value") else c.status,
            }
            for c in contracts
            if c.producer_pipeline_id == p.pipeline_id or c.consumer_pipeline_id == p.pipeline_id
        ]

        # Connector info
        source_connector = await store.get_connector(p.source_connector_id) if p.source_connector_id else None
        target_connector = await store.get_connector(p.target_connector_id) if p.target_connector_id else None

        # Trust score
        trust = _compute_trust_score(freshness, gates[0] if gates else None, budget, p)

        return {
            "pipeline_id": p.pipeline_id,
            "pipeline_name": p.pipeline_name,
            "target_table": f"{p.target_schema}.{p.target_table}",
            "source_table": f"{p.source_schema}.{p.source_table}",
            "source_type": source_connector.source_target_type if source_connector else None,
            "target_type": target_connector.source_target_type if target_connector else None,
            "status": p.status.value if hasattr(p.status, "value") else p.status,
            "tier": p.tier,
            "owner": p.owner,
            "tags": p.tags or {},
            "refresh_type": p.refresh_type.value if hasattr(p.refresh_type, "value") else p.refresh_type,
            "schedule_cron": p.schedule_cron,
            "columns": [
                {
                    "name": m.target_column or m.source_column,
                    "source_name": m.source_column,
                    "type": m.target_type or m.source_type,
                    "source_type": m.source_type,
                    "nullable": m.is_nullable,
                    "primary_key": m.is_primary_key,
                }
                for m in (p.column_mappings or [])
            ],
            "lineage": [
                {
                    "source_column": l.source_column,
                    "source_table": f"{l.source_schema}.{l.source_table}",
                    "target_column": l.target_column,
                    "target_table": f"{l.target_schema}.{l.target_table}",
                    "transformation": l.transformation,
                }
                for l in lineage
            ],
            "freshness": {
                "current": {
                    "staleness_minutes": freshness.staleness_minutes,
                    "status": freshness.status.value if hasattr(freshness.status, "value") else freshness.status,
                    "sla_met": freshness.sla_met,
                    "checked_at": freshness.checked_at,
                } if freshness else None,
                "history": [
                    {
                        "staleness_minutes": f.staleness_minutes,
                        "status": f.status.value if hasattr(f.status, "value") else f.status,
                        "checked_at": f.checked_at,
                    }
                    for f in freshness_history
                ],
            },
            "quality": {
                "latest": {
                    "decision": gates[0].decision.value if hasattr(gates[0].decision, "value") else gates[0].decision,
                    "checks": [
                        {
                            "check_name": c.check_name,
                            "status": c.status.value if hasattr(c.status, "value") else c.status,
                            "detail": c.detail,
                        }
                        for c in (gates[0].checks or [])
                    ],
                    "evaluated_at": gates[0].evaluated_at,
                } if gates else None,
                "trend": [
                    {
                        "decision": g.decision.value if hasattr(g.decision, "value") else g.decision,
                        "checks_passed": sum(1 for c in (g.checks or []) if c.status in ("pass", CheckStatus.PASS)),
                        "total_checks": len(g.checks or []),
                        "evaluated_at": g.evaluated_at,
                    }
                    for g in gates
                ],
            },
            "error_budget": {
                "success_rate": budget.success_rate,
                "budget_remaining": budget.budget_remaining,
                "total_runs": budget.total_runs,
                "escalated": budget.escalated,
            } if budget else None,
            "trust_score": trust["score"],
            "trust_detail": trust["detail"],
            "schema_versions": len(schema_versions),
            "data_contracts": related_contracts,
            "recent_runs": [
                {
                    "run_id": r.run_id,
                    "status": r.status.value if hasattr(r.status, "value") else r.status,
                    "rows_loaded": r.rows_loaded,
                    "started_at": r.started_at,
                    "completed_at": r.completed_at,
                }
                for r in runs
            ],
            "created_at": p.created_at,
            "updated_at": p.updated_at,
        }

    @app.get("/api/catalog/trust/{pipeline_id}")
    async def catalog_trust_detail(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Trust score breakdown with individual component scores and weights."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        freshness = await store.get_latest_freshness(p.pipeline_id)
        gates = await store.get_quality_trend(p.pipeline_id, limit=1)
        budget = await store.get_error_budget(p.pipeline_id)

        trust = _compute_trust_score(freshness, gates[0] if gates else None, budget, p)
        return {
            "pipeline_id": p.pipeline_id,
            "pipeline_name": p.pipeline_name,
            "target_table": f"{p.target_schema}.{p.target_table}",
            "trust_score": trust["score"],
            "detail": trust["detail"],
            "recommendation": trust["recommendation"],
            "weights": _TRUST_WEIGHTS,
        }

    @app.get("/api/catalog/columns")
    async def catalog_columns(
        request: Request,
        q: str = Query("", description="Search column names"),
        table: Optional[str] = Query(None, description="Filter by target table name"),
        limit: int = Query(100, le=500),
        offset: int = Query(0),
        caller: dict = Depends(auth_dep),
    ):
        """Search columns across all pipelines in the catalog."""
        pipelines = await store.list_pipelines()
        results = []
        q_lower = q.lower() if q else ""

        for p in pipelines:
            for m in (p.column_mappings or []):
                col_name = m.target_column or m.source_column
                # Table filter
                if table and table.lower() not in f"{p.target_schema}.{p.target_table}".lower():
                    continue
                # Text search
                if q_lower and q_lower not in (col_name or "").lower():
                    continue
                results.append({
                    "column_name": col_name,
                    "source_column": m.source_column,
                    "type": m.target_type or m.source_type,
                    "nullable": m.is_nullable,
                    "primary_key": m.is_primary_key,
                    "table": f"{p.target_schema}.{p.target_table}",
                    "pipeline_id": p.pipeline_id,
                    "pipeline_name": p.pipeline_name,
                })

        total = len(results)
        results = results[offset : offset + limit]
        return {"items": results, "total": total, "limit": limit, "offset": offset}

    @app.get("/api/catalog/stats")
    async def catalog_stats(
        request: Request,
        caller: dict = Depends(auth_dep),
    ):
        """High-level catalog statistics."""
        pipelines = await store.list_pipelines()
        active = [p for p in pipelines if p.status in (PipelineStatus.ACTIVE, "active")]
        total_columns = sum(len(p.column_mappings or []) for p in pipelines)

        # Source types
        source_types = {}
        for p in pipelines:
            if p.source_connector_id:
                c = await store.get_connector(p.source_connector_id)
                if c:
                    st = c.source_target_type or "unknown"
                    source_types[st] = source_types.get(st, 0) + 1

        # Trust distribution
        trust_counts = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
        for p in active:
            freshness = await store.get_latest_freshness(p.pipeline_id)
            gates = await store.get_quality_trend(p.pipeline_id, limit=1)
            budget = await store.get_error_budget(p.pipeline_id)
            trust = _compute_trust_score(freshness, gates[0] if gates else None, budget, p)
            score = trust["score"]
            if score is None:
                trust_counts["unknown"] += 1
            elif score >= 0.8:
                trust_counts["high"] += 1
            elif score >= 0.5:
                trust_counts["medium"] += 1
            else:
                trust_counts["low"] += 1

        return {
            "total_tables": len(pipelines),
            "active_tables": len(active),
            "total_columns": total_columns,
            "source_types": source_types,
            "trust_distribution": trust_counts,
        }

    # -----------------------------------------------------------------------
    # Semantic Tags & Business Context (Build 26 continued)
    # -----------------------------------------------------------------------

    @app.get("/api/catalog/tables/{pipeline_id}/tags")
    async def get_semantic_tags(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get semantic tags for all columns in a pipeline."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        return {
            "pipeline_id": p.pipeline_id,
            "pipeline_name": p.pipeline_name,
            "target_table": f"{p.target_schema}.{p.target_table}",
            "tags": p.semantic_tags or {},
            "column_count": len(p.column_mappings or []),
            "tagged_count": len(p.semantic_tags or {}),
            "ai_tagged": sum(1 for t in (p.semantic_tags or {}).values() if t.get("source") == "ai"),
            "user_tagged": sum(1 for t in (p.semantic_tags or {}).values() if t.get("source") == "user"),
        }

    @app.post("/api/catalog/tables/{pipeline_id}/tags/infer")
    @limiter.limit("10/minute")
    async def infer_semantic_tags(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """AI-infer semantic tags for columns. Preserves user-overridden tags."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        columns = [
            {
                "target_column": m.target_column,
                "source_column": m.source_column,
                "target_type": m.target_type,
                "source_type": m.source_type,
                "is_nullable": m.is_nullable,
                "is_primary_key": m.is_primary_key,
            }
            for m in (p.column_mappings or [])
        ]

        tags = await agent.infer_semantic_tags(
            pipeline_name=p.pipeline_name,
            source_table=f"{p.source_schema}.{p.source_table}",
            target_table=f"{p.target_schema}.{p.target_table}",
            columns=columns,
            existing_tags=p.semantic_tags,
        )

        p.semantic_tags = tags
        await store.save_pipeline(p)

        return {
            "pipeline_id": p.pipeline_id,
            "tags": tags,
            "inferred_count": sum(1 for t in tags.values() if t.get("source") == "ai"),
            "user_preserved": sum(1 for t in tags.values() if t.get("source") == "user"),
        }

    @app.put("/api/catalog/tables/{pipeline_id}/tags")
    async def set_semantic_tags(
        request: Request,
        pipeline_id: str,
        tags: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Set/override semantic tags for columns. Marks overridden tags as source=user."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        existing = p.semantic_tags or {}
        for col_name, tag_data in tags.items():
            if isinstance(tag_data, dict):
                tag_data["source"] = "user"  # Always mark manual overrides
                existing[col_name] = tag_data

        p.semantic_tags = existing
        await store.save_pipeline(p)

        return {
            "pipeline_id": p.pipeline_id,
            "tags": existing,
            "updated_columns": list(tags.keys()),
        }

    @app.patch("/api/catalog/tables/{pipeline_id}/tags/{column_name}")
    async def update_column_tag(
        request: Request,
        pipeline_id: str,
        column_name: str,
        tag: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Update semantic tag for a single column."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        existing = p.semantic_tags or {}
        current = existing.get(column_name, {})
        current.update(tag)
        current["source"] = "user"
        existing[column_name] = current

        p.semantic_tags = existing
        await store.save_pipeline(p)

        return {"pipeline_id": p.pipeline_id, "column": column_name, "tag": current}

    # -----------------------------------------------------------------------
    # Pipeline Business Context (Build 26)
    # -----------------------------------------------------------------------

    @app.get("/api/catalog/tables/{pipeline_id}/context/questions")
    async def get_context_questions(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get targeted business context questions for a pipeline."""
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        columns = [
            {"target_column": m.target_column, "source_column": m.source_column}
            for m in (p.column_mappings or [])[:20]
        ]

        questions = await agent.generate_business_context_questions(
            pipeline_name=p.pipeline_name,
            source_table=f"{p.source_schema}.{p.source_table}",
            target_table=f"{p.target_schema}.{p.target_table}",
            columns=columns,
        )

        return {
            "pipeline_id": p.pipeline_id,
            "pipeline_name": p.pipeline_name,
            "questions": questions,
            "existing_context": p.business_context or {},
        }

    @app.put("/api/catalog/tables/{pipeline_id}/context")
    async def set_business_context(
        request: Request,
        pipeline_id: str,
        context: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Save business context answers for a pipeline."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        existing = p.business_context or {}
        existing.update(context)
        existing["_last_updated"] = now_iso()
        existing["_updated_by"] = caller.get("username", "unknown")
        p.business_context = existing
        await store.save_pipeline(p)

        return {"pipeline_id": p.pipeline_id, "context": existing}

    # -----------------------------------------------------------------------
    # Configurable Trust Weights (Build 26)
    # -----------------------------------------------------------------------

    @app.put("/api/catalog/tables/{pipeline_id}/trust-weights")
    async def set_trust_weights(
        request: Request,
        pipeline_id: str,
        weights: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Override trust score weights for a pipeline. Weights must sum to ~1.0."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        # Validate weights
        valid_keys = {"freshness", "quality_gate", "error_budget", "schema_stability"}
        for k in weights:
            if k not in valid_keys:
                raise HTTPException(400, f"Invalid weight key: {k}. Must be one of: {valid_keys}")
        total = sum(weights.values())
        if abs(total - 1.0) > 0.05:
            raise HTTPException(400, f"Weights must sum to ~1.0, got {total:.2f}")

        p.trust_weights = weights
        await store.save_pipeline(p)

        # Recompute trust with new weights
        freshness = await store.get_latest_freshness(p.pipeline_id)
        gates = await store.get_quality_trend(p.pipeline_id, limit=1)
        budget = await store.get_error_budget(p.pipeline_id)
        trust = _compute_trust_score(freshness, gates[0] if gates else None, budget, p)

        return {
            "pipeline_id": p.pipeline_id,
            "weights": weights,
            "trust_score": trust["score"],
            "detail": trust["detail"],
            "recommendation": trust["recommendation"],
        }

    @app.delete("/api/catalog/tables/{pipeline_id}/trust-weights")
    async def reset_trust_weights(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Reset to global default trust weights."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        p.trust_weights = None
        await store.save_pipeline(p)
        return {"pipeline_id": p.pipeline_id, "weights": _TRUST_WEIGHTS, "message": "Reset to defaults"}

    # -----------------------------------------------------------------------
    # SQL Transforms (Build 29)
    # -----------------------------------------------------------------------

    @app.post("/api/transforms")
    async def create_transform(
        request: Request,
        body: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Create a new SQL transform in the catalog."""
        require_role(caller, "admin", "operator")
        name = body.get("transform_name", "").strip()
        sql = body.get("sql", "").strip()
        if not name:
            raise HTTPException(400, "transform_name is required")
        if not sql:
            raise HTTPException(400, "sql is required")

        mat = body.get("materialization", "table")
        try:
            mat_enum = MaterializationType(mat)
        except ValueError:
            raise HTTPException(400, f"Invalid materialization: {mat}")

        t = SqlTransform(
            transform_name=name,
            sql=sql,
            description=body.get("description", ""),
            materialization=mat_enum,
            target_schema=body.get("target_schema", "analytics"),
            target_table=body.get("target_table", name),
            variables=body.get("variables", {}),
            refs=body.get("refs", []),
            pipeline_id=body.get("pipeline_id", ""),
            created_by=caller.get("username", "api"),
            approved=body.get("approved", False),
        )
        await store.save_sql_transform(t)
        return {"transform_id": t.transform_id, "transform_name": t.transform_name, "status": "created"}

    @app.get("/api/transforms")
    async def list_transforms(
        request: Request,
        pipeline_id: str = Query(""),
        caller: dict = Depends(auth_dep),
    ):
        """List all SQL transforms, optionally filtered by pipeline."""
        transforms = await store.list_sql_transforms(pipeline_id)
        return [
            {
                "transform_id": t.transform_id,
                "transform_name": t.transform_name,
                "description": t.description,
                "materialization": t.materialization.value if hasattr(t.materialization, "value") else t.materialization,
                "target_schema": t.target_schema,
                "target_table": t.target_table,
                "version": t.version,
                "approved": t.approved,
                "pipeline_id": t.pipeline_id,
                "refs": t.refs,
                "created_by": t.created_by,
                "created_at": t.created_at,
                "updated_at": t.updated_at,
            }
            for t in transforms
        ]

    @app.get("/api/transforms/{transform_id}")
    async def get_transform(
        request: Request,
        transform_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get a transform by ID."""
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")
        from dataclasses import asdict as _asdict
        result = _asdict(t)
        result["materialization"] = t.materialization.value if hasattr(t.materialization, "value") else t.materialization
        return result

    @app.patch("/api/transforms/{transform_id}")
    async def update_transform(
        request: Request,
        transform_id: str,
        body: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Update a transform's SQL or config."""
        require_role(caller, "admin", "operator")
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")

        changed = []
        if "sql" in body:
            t.sql = body["sql"]
            changed.append("sql")
        if "description" in body:
            t.description = body["description"]
            changed.append("description")
        if "materialization" in body:
            try:
                t.materialization = MaterializationType(body["materialization"])
            except ValueError:
                raise HTTPException(400, f"Invalid materialization: {body['materialization']}")
            changed.append("materialization")
        if "target_schema" in body:
            t.target_schema = body["target_schema"]
            changed.append("target_schema")
        if "target_table" in body:
            t.target_table = body["target_table"]
            changed.append("target_table")
        if "variables" in body:
            t.variables = body["variables"]
            changed.append("variables")
        if "refs" in body:
            t.refs = body["refs"]
            changed.append("refs")
        if "approved" in body:
            t.approved = body["approved"]
            changed.append("approved")
        if "pipeline_id" in body:
            t.pipeline_id = body["pipeline_id"]
            changed.append("pipeline_id")

        if not changed:
            return {"transform_id": transform_id, "message": "No changes"}

        t.version += 1
        t.updated_at = now_iso()
        await store.save_sql_transform(t)

        # Write to pipeline changelog if transform is linked to a pipeline
        if t.pipeline_id:
            is_approval = changed == ["approved"] and t.approved
            change_type = PipelineChangeType.TRANSFORM_APPROVED if is_approval else PipelineChangeType.TRANSFORM_UPDATED
            pipeline = await store.get_pipeline(t.pipeline_id)
            p_name = pipeline.pipeline_name if pipeline else t.pipeline_id[:8]
            await _log_pipeline_change(
                pipeline_id=t.pipeline_id,
                pipeline_name=p_name,
                change_type=change_type,
                changed_fields={"transform_name": t.transform_name, "version": t.version, "changed": changed},
                caller=caller,
                context=f"Transform {t.transform_name} v{t.version}",
            )

        return {"transform_id": transform_id, "version": t.version, "changed": changed}

    @app.delete("/api/transforms/{transform_id}")
    async def delete_transform(
        request: Request,
        transform_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Delete a transform."""
        require_role(caller, "admin")
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")
        await store.delete_sql_transform(transform_id)
        return {"transform_id": transform_id, "status": "deleted"}

    @app.post("/api/transforms/{transform_id}/validate")
    async def validate_transform(
        request: Request,
        transform_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Dry-run EXPLAIN of a transform's SQL."""
        from transforms.engine import resolve_refs, resolve_vars, validate_sql
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")

        # Resolve refs and vars for validation
        resolved, refs = await resolve_refs(t.sql, store, t.pipeline_id)
        resolved = resolve_vars(resolved, t.variables)

        # Need a target engine to run EXPLAIN
        pipeline = await store.get_pipeline(t.pipeline_id) if t.pipeline_id else None
        if not pipeline:
            # Try to find any active pipeline for target connection
            pipelines = await store.list_pipelines()
            pipeline = pipelines[0] if pipelines else None
        if not pipeline:
            raise HTTPException(400, "No pipeline available for SQL validation")

        try:
            target = await registry.get_target(pipeline)
            result = await validate_sql(target, resolved)
            return {"transform_id": transform_id, "resolved_sql": resolved, "refs": refs, **result}
        except Exception as e:
            return {"transform_id": transform_id, "valid": False, "error": str(e)}

    @app.post("/api/transforms/{transform_id}/preview")
    async def preview_transform(
        request: Request,
        transform_id: str,
        limit: int = Query(10, ge=1, le=100),
        caller: dict = Depends(auth_dep),
    ):
        """Execute transform SQL with LIMIT and return sample rows."""
        from transforms.engine import resolve_refs, resolve_vars, preview_sql
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")

        resolved, refs = await resolve_refs(t.sql, store, t.pipeline_id)
        resolved = resolve_vars(resolved, t.variables)

        pipeline = await store.get_pipeline(t.pipeline_id) if t.pipeline_id else None
        if not pipeline:
            pipelines = await store.list_pipelines()
            pipeline = pipelines[0] if pipelines else None
        if not pipeline:
            raise HTTPException(400, "No pipeline available for SQL preview")

        try:
            target = await registry.get_target(pipeline)
            result = await preview_sql(target, resolved, limit=limit)
            return {"transform_id": transform_id, "resolved_sql": resolved, **result}
        except Exception as e:
            return {"transform_id": transform_id, "error": str(e), "rows": []}

    @app.post("/api/transforms/generate")
    @limiter.limit("10/minute")
    async def generate_transform(
        request: Request,
        body: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Generate SQL transform from natural language description using AI."""
        require_role(caller, "admin", "operator")
        description = body.get("description", "").strip()
        if not description:
            raise HTTPException(400, "description is required")

        pipeline_id = body.get("pipeline_id", "")
        materialization = body.get("materialization", "table")
        target_table = body.get("target_table", "")

        # Gather available tables from pipelines
        pipelines = await store.list_pipelines()
        available_tables = []
        for p in pipelines:
            cols = [{"name": c.source_column, "type": c.target_type or "text"} for c in (p.column_mappings or [])]
            available_tables.append({
                "schema": p.target_schema or "public",
                "table": p.target_table,
                "columns": cols,
            })

        # Also include existing transforms
        transforms = await store.list_sql_transforms()
        for t in transforms:
            available_tables.append({
                "schema": t.target_schema or "analytics",
                "table": t.target_table or t.transform_name,
                "columns": [],
            })

        result = await agent.generate_transform_sql(
            description=description,
            available_tables=available_tables,
            materialization=materialization,
            target_table=target_table,
        )

        # Auto-create the transform in catalog
        t = SqlTransform(
            transform_name=result.get("target_table", target_table or "generated_transform"),
            sql=result.get("sql", ""),
            description=result.get("description", description),
            materialization=MaterializationType(materialization),
            target_table=result.get("target_table", target_table),
            variables=result.get("variables", {}),
            refs=result.get("refs", []),
            pipeline_id=pipeline_id,
            created_by="agent",
            approved=False,
        )
        await store.save_sql_transform(t)

        return {
            "transform_id": t.transform_id,
            "transform_name": t.transform_name,
            "sql": t.sql,
            "description": t.description,
            "materialization": materialization,
            "refs": t.refs,
            "variables": t.variables,
            "approved": False,
            "message": "Transform generated. Approve before use in pipelines.",
        }

    @app.get("/api/transforms/{transform_id}/lineage")
    async def transform_lineage(
        request: Request,
        transform_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get parsed column lineage for a transform."""
        from transforms.engine import resolve_refs, resolve_vars, parse_column_lineage
        t = await store.get_sql_transform(transform_id)
        if not t:
            raise HTTPException(404, "Transform not found")

        resolved, refs = await resolve_refs(t.sql, store, t.pipeline_id)
        resolved = resolve_vars(resolved, t.variables)
        lineage = parse_column_lineage(resolved, t.target_table or t.transform_name, refs)
        return {"transform_id": transform_id, "lineage": lineage, "refs": refs}

    # -----------------------------------------------------------------------
    # Metrics / KPI layer (Build 31)
    # -----------------------------------------------------------------------

    @app.post("/api/metrics/suggest/{pipeline_id}")
    async def suggest_metrics(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Agent suggests KPI metrics for a pipeline's target table."""
        require_role(caller, "admin", "operator")
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        columns = [
            {"target_column": cm.target_column, "source_column": cm.source_column,
             "target_type": cm.target_type, "is_nullable": cm.is_nullable,
             "is_primary_key": cm.is_primary_key}
            for cm in (p.column_mappings or [])
        ]
        suggestions = await agent.suggest_metrics(p, columns, p.business_context or {})
        return {"pipeline_id": pipeline_id, "suggestions": suggestions}

    class CreateMetricRequest(BaseModel):
        pipeline_id: str
        metric_name: str
        description: str = ""
        sql_expression: str = ""
        metric_type: str = "custom"
        dimensions: list = []
        schedule_cron: str = ""
        tags: dict = {}
        reasoning: str = ""

    @app.post("/api/metrics")
    async def create_metric(
        request: Request,
        body: CreateMetricRequest,
        caller: dict = Depends(auth_dep),
    ):
        """Create a metric. If sql_expression is empty, agent generates it from description."""
        require_role(caller, "admin", "operator")
        from contracts.models import MetricDefinition, MetricType

        p = await store.get_pipeline(body.pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")

        sql_expr = body.sql_expression
        metric_type = body.metric_type

        # Agent generates SQL if not provided
        if not sql_expr and body.description:
            columns = [
                {"target_column": cm.target_column, "source_column": cm.source_column,
                 "target_type": cm.target_type, "is_nullable": cm.is_nullable,
                 "is_primary_key": cm.is_primary_key}
                for cm in (p.column_mappings or [])
            ]
            target_table = f"{p.target_schema}.{p.target_table}"
            # Build rich context for the agent
            existing = await store.list_metrics(body.pipeline_id)
            pipeline_ctx = {
                "pipeline_name": p.pipeline_name,
                "source_type": p.source_type if hasattr(p, "source_type") else "",
                "semantic_tags": p.semantic_tags or {},
                "business_context": p.business_context or {},
                "existing_metrics": [m.metric_name for m in existing],
                "tier": p.tier,
            }
            generated = await agent.generate_metric_sql(
                body.description, target_table, columns, pipeline_context=pipeline_ctx,
            )
            sql_expr = generated.get("sql_expression", "")
            metric_type = generated.get("metric_type", metric_type)

        try:
            mt = MetricType(metric_type.lower())
        except (ValueError, AttributeError):
            mt = MetricType.CUSTOM

        metric = MetricDefinition(
            pipeline_id=body.pipeline_id,
            metric_name=body.metric_name,
            description=body.description,
            sql_expression=sql_expr,
            metric_type=mt,
            dimensions=body.dimensions,
            schedule_cron=body.schedule_cron,
            tags=body.tags,
            created_by=caller.get("username", "api"),
        )

        # Generate initial reasoning (carry from suggestion or agent-generate)
        from contracts.models import now_iso
        initial_reasoning = body.reasoning
        if not initial_reasoning:
            pipeline_ctx = {"pipeline_name": p.pipeline_name, "tier": p.tier,
                            "business_context": p.business_context or {}}
            initial_reasoning = await agent.explain_metric(
                metric, trigger="created", pipeline_context=pipeline_ctx,
            )
        metric.reasoning = initial_reasoning
        metric.reasoning_history = [
            {"reasoning": initial_reasoning, "trigger": "created",
             "at": now_iso(), "by": caller.get("username", "api")}
        ]

        await store.save_metric(metric)
        return {"metric_id": metric.metric_id, "metric_name": metric.metric_name,
                "sql_expression": sql_expr, "reasoning": metric.reasoning,
                "status": "created"}

    @app.get("/api/metrics")
    async def list_metrics(
        request: Request,
        pipeline_id: str = "",
        caller: dict = Depends(auth_dep),
    ):
        """List metrics, optionally filtered by pipeline."""
        metrics = await store.list_metrics(pipeline_id)
        return {"metrics": [
            {"metric_id": m.metric_id, "pipeline_id": m.pipeline_id,
             "metric_name": m.metric_name, "description": m.description,
             "metric_type": m.metric_type.value if hasattr(m.metric_type, "value") else m.metric_type,
             "sql_expression": m.sql_expression,
             "dimensions": m.dimensions, "schedule_cron": m.schedule_cron,
             "tags": m.tags, "enabled": m.enabled, "created_by": m.created_by,
             "reasoning": m.reasoning,
             "created_at": m.created_at}
            for m in metrics
        ]}

    @app.get("/api/metrics/{metric_id}")
    async def get_metric(
        request: Request,
        metric_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Get metric detail with recent snapshots."""
        m = await store.get_metric(metric_id)
        if not m:
            raise HTTPException(404, "Metric not found")
        snapshots = await store.list_metric_snapshots(metric_id, limit=50)
        return {
            "metric_id": m.metric_id, "pipeline_id": m.pipeline_id,
            "metric_name": m.metric_name, "description": m.description,
            "metric_type": m.metric_type.value if hasattr(m.metric_type, "value") else m.metric_type,
            "sql_expression": m.sql_expression,
            "dimensions": m.dimensions, "schedule_cron": m.schedule_cron,
            "tags": m.tags, "enabled": m.enabled, "created_by": m.created_by,
            "reasoning": m.reasoning,
            "reasoning_history": m.reasoning_history,
            "created_at": m.created_at,
            "snapshots": [
                {"snapshot_id": s.snapshot_id, "computed_at": s.computed_at,
                 "value": s.value, "dimension_values": s.dimension_values,
                 "metadata": s.metadata}
                for s in snapshots
            ],
        }

    @app.post("/api/metrics/{metric_id}/compute")
    async def compute_metric(
        request: Request,
        metric_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Compute a metric now by executing its SQL against the target database."""
        require_role(caller, "admin", "operator")
        from contracts.models import MetricSnapshot, now_iso

        m = await store.get_metric(metric_id)
        if not m:
            raise HTTPException(404, "Metric not found")
        p = await store.get_pipeline(m.pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline for metric not found")

        # Resolve target connector and execute SQL
        try:
            tgt_params = {}
            if p.target_host:
                tgt_params["host"] = p.target_host
            if p.target_port:
                tgt_params["port"] = p.target_port
            if p.target_database:
                tgt_params["database"] = p.target_database
            if p.target_user:
                tgt_params["user"] = p.target_user
            if p.target_password:
                tgt_params["password"] = p.target_password
            if config.has_encryption_key:
                tgt_params = decrypt_dict(tgt_params, config.encryption_key, CREDENTIAL_FIELDS)
            if p.target_options:
                tgt_params.update(p.target_options)

            target = await registry.get_target(p.target_connector_id, tgt_params)
            conn = await target.test_connection()
            if not conn.success:
                raise HTTPException(500, f"Cannot connect to target: {conn.message}")

            # Execute the metric SQL
            import time as _time
            t0 = _time.monotonic()
            result = await target.execute_sql(m.sql_expression)
            elapsed_ms = int((_time.monotonic() - t0) * 1000)

            # Extract value from result
            value = 0.0
            if result and len(result) > 0:
                row = result[0]
                if isinstance(row, dict):
                    value = float(row.get("value", row.get(list(row.keys())[0], 0)))
                elif isinstance(row, (list, tuple)):
                    value = float(row[0])
                else:
                    value = float(row)

            snapshot = MetricSnapshot(
                metric_id=m.metric_id,
                pipeline_id=m.pipeline_id,
                value=value,
                metadata={"elapsed_ms": elapsed_ms, "sql": m.sql_expression},
            )
            await store.save_metric_snapshot(snapshot)

            return {"snapshot_id": snapshot.snapshot_id, "value": value,
                    "computed_at": snapshot.computed_at, "elapsed_ms": elapsed_ms}

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Metric computation failed: {e}")

    @app.get("/api/metrics/{metric_id}/trend")
    async def metric_trend(
        request: Request,
        metric_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Agent interprets the metric's time-series trend."""
        m = await store.get_metric(metric_id)
        if not m:
            raise HTTPException(404, "Metric not found")
        snapshots = await store.list_metric_snapshots(metric_id, limit=50)
        if len(snapshots) < 2:
            return {"metric_id": metric_id, "trend": "insufficient_data",
                    "interpretation": "Need at least 2 data points for trend analysis."}

        p = await store.get_pipeline(m.pipeline_id)
        snap_dicts = [{"computed_at": s.computed_at, "value": s.value} for s in snapshots]
        pipeline_ctx = {
            "pipeline_name": p.pipeline_name if p else "unknown",
            "tier": p.tier if p else 2,
            "business_context": p.business_context if p else {},
        }
        analysis = await agent.interpret_metric_trend(m.metric_name, snap_dicts, pipeline_ctx)

        # Update metric reasoning with trend insights
        from contracts.models import now_iso
        new_reasoning = await agent.explain_metric(
            m, trigger="trend", trend_context=analysis, pipeline_context=pipeline_ctx,
        )
        m.reasoning = new_reasoning
        m.reasoning_history.append({
            "reasoning": new_reasoning, "trigger": "trend",
            "at": now_iso(), "by": "agent",
        })
        m.updated_at = now_iso()
        await store.save_metric(m)

        return {"metric_id": metric_id, "metric_name": m.metric_name,
                "reasoning": m.reasoning, **analysis}

    @app.patch("/api/metrics/{metric_id}")
    async def update_metric(
        request: Request,
        metric_id: str,
        body: dict = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        """Update metric fields. Agent re-reasons on meaningful changes."""
        require_role(caller, "admin", "operator")
        from contracts.models import MetricType, now_iso
        m = await store.get_metric(metric_id)
        if not m:
            raise HTTPException(404, "Metric not found")

        # Track what changed for reasoning
        changes = []
        for field in ("metric_name", "description", "sql_expression", "schedule_cron", "enabled"):
            if field in body and getattr(m, field) != body[field]:
                changes.append(f"{field}: {getattr(m, field)!r} -> {body[field]!r}")
                setattr(m, field, body[field])
        if "tags" in body:
            m.tags = body["tags"]
        if "dimensions" in body:
            m.dimensions = body["dimensions"]
        if "metric_type" in body:
            try:
                m.metric_type = MetricType(body["metric_type"].lower())
                changes.append(f"metric_type -> {body['metric_type']}")
            except (ValueError, AttributeError):
                pass

        # Allow explicit reasoning override
        if "reasoning" in body and body["reasoning"]:
            m.reasoning = body["reasoning"]
            m.reasoning_history.append({
                "reasoning": body["reasoning"], "trigger": "manual_edit",
                "at": now_iso(), "by": caller.get("username", "api"),
            })
        elif changes:
            # Agent re-reasons on meaningful changes
            p = await store.get_pipeline(m.pipeline_id)
            pipeline_ctx = {"pipeline_name": p.pipeline_name, "tier": p.tier,
                            "business_context": p.business_context or {}} if p else {}
            change_summary = "; ".join(changes)
            new_reasoning = await agent.explain_metric(
                m, trigger="updated", change_summary=change_summary,
                pipeline_context=pipeline_ctx,
            )
            m.reasoning = new_reasoning
            m.reasoning_history.append({
                "reasoning": new_reasoning, "trigger": "updated",
                "change_summary": change_summary,
                "at": now_iso(), "by": caller.get("username", "api"),
            })

        m.updated_at = now_iso()
        await store.save_metric(m)
        return {"metric_id": metric_id, "reasoning": m.reasoning, "status": "updated"}

    @app.delete("/api/metrics/{metric_id}")
    async def delete_metric(
        request: Request,
        metric_id: str,
        caller: dict = Depends(auth_dep),
    ):
        """Delete a metric and all its snapshots."""
        require_role(caller, "admin")
        m = await store.get_metric(metric_id)
        if not m:
            raise HTTPException(404, "Metric not found")
        await store.delete_metric(metric_id)
        return {"metric_id": metric_id, "status": "deleted"}

    # -----------------------------------------------------------------------
    # Serve static UI
    # -----------------------------------------------------------------------

    _ui_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ui")
    if os.path.exists(_ui_dir):
        app.mount("/static", StaticFiles(directory=_ui_dir), name="static")

        @app.get("/")
        async def serve_ui():
            return FileResponse(
                os.path.join(_ui_dir, "index.html"),
                headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
            )

        @app.get("/{full_path:path}")
        async def catch_all(full_path: str):
            if full_path.startswith("api/") or full_path in ("health", "metrics"):
                raise HTTPException(404)
            return FileResponse(
                os.path.join(_ui_dir, "index.html"),
                headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
            )

    return app


# ---------------------------------------------------------------------------
# Proposal application helper
# ---------------------------------------------------------------------------

async def _apply_proposal(
    proposal: ContractChangeProposal,
    store: Store,
    registry: ConnectorRegistry,
    agent: AgentCore,
    gitops=None,
    config=None,
):
    """Apply an approved proposal, updating connectors or pipeline schemas."""
    if proposal.change_type == ChangeType.NEW_CONNECTOR and proposal.connector_id:
        c = await store.get_connector(proposal.connector_id)
        if c:
            c.status = ConnectorStatus.ACTIVE
            c.approved_by = proposal.resolved_by
            c.approved_at = proposal.resolved_at
            await store.save_connector(c)
            registry.register_approved_connector(c)
            proposal.status = ProposalStatus.APPLIED
            # GitOps: commit connector code (offloaded to thread)
            if gitops and gitops.enabled:
                import asyncio as _aio
                def _commit_connector():
                    try:
                        gitops.commit_connector(
                            c,
                            f"Approve connector: {c.connector_name} (v{c.version})",
                            author=proposal.resolved_by or "dapos",
                        )
                    except Exception as e:
                        log.warning("GitOps commit_connector failed: %s", e)
                _aio.get_running_loop().run_in_executor(None, _commit_connector)
        return

    if not proposal.pipeline_id:
        return
    pipeline = await store.get_pipeline(proposal.pipeline_id)
    if not pipeline:
        return

    if proposal.change_type in (
        ChangeType.ADD_COLUMN,
        ChangeType.ALTER_COLUMN_TYPE,
        ChangeType.DROP_COLUMN,
    ):
        # Use agent-generated SQL from proposal, or ask agent to generate it now
        migration_sql = proposal.proposed_state.get("migration_sql", [])

        if not migration_sql:
            # Proposal was created before agentic migration — ask agent to generate now
            drift_info = {}
            if proposal.change_type == ChangeType.ADD_COLUMN:
                drift_info["new_columns"] = proposal.proposed_state.get("new_columns", [])
            elif proposal.change_type == ChangeType.ALTER_COLUMN_TYPE:
                drift_info["type_changes"] = proposal.proposed_state.get("type_changes", [])
            elif proposal.change_type == ChangeType.DROP_COLUMN:
                drift_info["dropped_columns"] = proposal.proposed_state.get("dropped_columns", [])

            target_type = proposal.trigger_detail.get("target_type", "postgresql")
            migration = await agent.generate_migration_sql(pipeline, drift_info, target_type)
            migration_sql = migration.get("migration_sql", [])
            log.info("Agent generated %d migration statement(s) for proposal %s",
                     len(migration_sql), proposal.proposal_id[:8])

        # Execute agent-generated SQL on the actual target table
        if migration_sql:
            try:
                tgt_params = {
                    "host": pipeline.target_host, "port": pipeline.target_port,
                    "database": pipeline.target_database,
                    "user": pipeline.target_user, "password": pipeline.target_password,
                    "default_schema": pipeline.target_schema,
                }
                if config and config.has_encryption_key:
                    tgt_params = decrypt_dict(tgt_params, config.encryption_key, CREDENTIAL_FIELDS)
                target = await registry.get_target(pipeline.target_connector_id, tgt_params)
                for stmt in migration_sql:
                    await target.execute_sql(stmt)
                if hasattr(target, "close"):
                    await target.close()
                log.info("Executed %d agent-generated statement(s) for proposal %s",
                         len(migration_sql), proposal.proposal_id[:8])
            except Exception as e:
                log.error("Failed to execute migration SQL for proposal %s: %s",
                          proposal.proposal_id[:8], e)

        # Update column mappings by re-profiling source for current state
        if proposal.change_type == ChangeType.ADD_COLUMN:
            new_col_names = [c["name"] for c in proposal.proposed_state.get("new_columns", [])]
            if new_col_names:
                try:
                    src_params = {
                        "host": pipeline.source_host, "port": pipeline.source_port,
                        "database": pipeline.source_database,
                        "user": pipeline.source_user, "password": pipeline.source_password,
                    }
                    if config and config.has_encryption_key:
                        src_params = decrypt_dict(src_params, config.encryption_key, CREDENTIAL_FIELDS)
                    source = await registry.get_source(pipeline.source_connector_id, src_params)
                    profile = await source.profile_table(pipeline.source_schema, pipeline.source_table)
                    live_cols = {m.source_column: m for m in profile.columns}
                    for col_name in new_col_names:
                        if col_name in live_cols:
                            pipeline.column_mappings.append(live_cols[col_name])
                except Exception as e:
                    log.error("Failed to re-profile source for ADD_COLUMN proposal: %s", e)

        elif proposal.change_type == ChangeType.ALTER_COLUMN_TYPE:
            for tc in proposal.proposed_state.get("type_changes", []):
                col_name = tc.get("column", "")
                new_type = tc.get("to", "")
                for mapping in pipeline.column_mappings:
                    if mapping.source_column == col_name and new_type:
                        mapping.source_type = tc.get("from", mapping.source_type)
                        mapping.target_type = new_type
                        break

        elif proposal.change_type == ChangeType.DROP_COLUMN:
            for col_info in proposal.proposed_state.get("dropped_columns", []):
                col_name = col_info if isinstance(col_info, str) else col_info.get("name", "")
                pipeline.column_mappings = [
                    m for m in pipeline.column_mappings if m.source_column != col_name
                ]

        pipeline.version += 1
        proposal.contract_version_after = pipeline.version
        await store.save_pipeline(pipeline)

        sv = SchemaVersion(
            pipeline_id=pipeline.pipeline_id,
            version=pipeline.version,
            column_mappings=pipeline.column_mappings,
            change_summary=(
                f"Applied {proposal.change_type.value} "
                f"(proposal {proposal.proposal_id[:8]})"
            ),
            change_type=proposal.change_type.value,
            proposal_id=proposal.proposal_id,
            applied_by=proposal.resolved_by or "user",
        )
        await store.save_schema_version(sv)

        # GitOps: commit pipeline schema change (offloaded to thread)
        if gitops and gitops.enabled:
            import asyncio as _aio
            _yaml = pipeline_to_yaml(pipeline, mask_credentials=True)
            _msg = f"Schema change: {pipeline.pipeline_name} v{pipeline.version} ({proposal.change_type.value})"
            _author = proposal.resolved_by or "dapos"
            def _commit_schema():
                try:
                    gitops.commit_pipeline(pipeline, _yaml, _msg, author=_author)
                except Exception as e:
                    log.warning("GitOps commit_pipeline (schema) failed: %s", e)
            _aio.get_running_loop().run_in_executor(None, _commit_schema)

    proposal.status = ProposalStatus.APPLIED


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _connector_summary(c) -> dict:
    return {
        "connector_id": c.connector_id,
        "connector_name": c.connector_name,
        "connector_type": c.connector_type.value if hasattr(c.connector_type, "value") else c.connector_type,
        "source_target_type": c.source_target_type,
        "version": c.version,
        "generated_by": c.generated_by,
        "status": c.status.value if hasattr(c.status, "value") else c.status,
        "test_status": getattr(c, "test_status", None),
        "approved_by": c.approved_by,
        "created_at": c.created_at,
    }


async def _resolve_pipeline(query: str, store: Store):
    """Resolve a pipeline by name or id from a natural language query."""
    if not query:
        return None
    pipelines = await store.list_pipelines()
    query_lower = query.lower().strip()
    # Exact ID match
    for p in pipelines:
        if p.pipeline_id == query_lower or p.pipeline_id == query:
            return p
    # Exact name match
    for p in pipelines:
        if p.pipeline_name.lower() == query_lower:
            return p
    # Substring match on name
    for p in pipelines:
        if p.pipeline_name.lower() in query_lower or query_lower in p.pipeline_name.lower():
            return p
    # Word overlap match
    for p in pipelines:
        name_words = set(p.pipeline_name.lower().replace("-", " ").replace("_", " ").split())
        query_words = set(query_lower.replace("-", " ").replace("_", " ").split())
        if name_words & query_words - {"pipeline", "the", "my", "is", "why", "what", "if", "for", "a"}:
            return p
    return None


def _parse_step_dicts(raw_steps: list[dict]) -> list:
    """Parse step dicts from API request into StepDefinition list.

    Supports `depends_on_names` for name-based dependency resolution
    in addition to `depends_on` with explicit step IDs.
    """
    # First pass: create steps and build name->id map
    result = []
    name_to_id: dict[str, str] = {}
    for s in raw_steps:
        d = dict(s)
        if "step_id" not in d or not d["step_id"]:
            d["step_id"] = new_id()
        st = d.get("step_type", "extract")
        if isinstance(st, str):
            try:
                d["step_type"] = StepType(st.lower())
            except ValueError:
                d["step_type"] = StepType.EXTRACT
        if "depends_on" not in d:
            d["depends_on"] = []
        if "config" not in d:
            d["config"] = {}
        name_to_id[d.get("step_name", "")] = d["step_id"]
        # Stash name deps for second pass
        d["_depends_on_names"] = d.pop("depends_on_names", [])
        result.append(d)

    # Second pass: resolve name-based dependencies to IDs
    parsed = []
    for d in result:
        name_deps = d.pop("_depends_on_names", [])
        if name_deps:
            for name in name_deps:
                dep_id = name_to_id.get(name)
                if dep_id and dep_id not in d["depends_on"]:
                    d["depends_on"].append(dep_id)
        parsed.append(StepDefinition(**d))
    return parsed


def _connector_detail(c) -> dict:
    d = _connector_summary(c)
    d["code"] = c.code
    d["dependencies"] = c.dependencies
    d["test_results"] = c.test_results
    d["generation_log"] = c.generation_log
    return d


def _pipeline_summary(p) -> dict:
    return {
        "pipeline_id": p.pipeline_id,
        "pipeline_name": p.pipeline_name,
        "version": p.version,
        "status": p.status.value if hasattr(p.status, "value") else p.status,
        "tier": p.tier,
        "owner": p.owner,
        "source": f"{p.source_schema}.{p.source_table}",
        "target": f"{p.target_schema}.{p.target_table}",
        "refresh_type": p.refresh_type.value if hasattr(p.refresh_type, "value") else p.refresh_type,
        "load_type": p.load_type.value if hasattr(p.load_type, "value") else p.load_type,
        "schedule_cron": p.schedule_cron,
        "tags": p.tags,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
        "step_count": len(p.steps) if p.steps else 0,
        "source_connector_id": p.source_connector_id,
        "target_connector_id": p.target_connector_id,
    }


async def _pipeline_detail(p, store: Store) -> dict:
    d = _pipeline_summary(p)
    d.update(
        {
            "source_connector_id": p.source_connector_id,
            "target_connector_id": p.target_connector_id,
            "merge_keys": p.merge_keys,
            "incremental_column": p.incremental_column,
            "last_watermark": p.last_watermark,
            "replication_method": p.replication_method.value if hasattr(p.replication_method, "value") else p.replication_method,
            "retry_max_attempts": p.retry_max_attempts,
            "retry_backoff_seconds": p.retry_backoff_seconds,
            "timeout_seconds": p.timeout_seconds,
            "auto_approve_additive_schema": p.auto_approve_additive_schema,
            "tier_config": p.tier_config or {},
            "freshness_column": p.freshness_column,
            "column_mappings": [
                {
                    "source_column": m.source_column,
                    "source_type": m.source_type,
                    "target_column": m.target_column,
                    "target_type": m.target_type,
                    "is_nullable": m.is_nullable,
                    "is_primary_key": m.is_primary_key,
                }
                for m in (p.column_mappings or [])
            ],
            "target_ddl": p.target_ddl,
            "target_options": p.target_options,
            "quality_config": asdict(p.quality_config) if p.quality_config else {},
            "agent_reasoning": p.agent_reasoning,
            "baseline_row_count": p.baseline_row_count,
            "notification_policy_id": p.notification_policy_id,
        }
    )
    # Include error budget summary
    budget = await store.get_error_budget(p.pipeline_id)
    if budget:
        d["error_budget"] = {
            "window_days": budget.window_days,
            "total_runs": budget.total_runs,
            "successful_runs": budget.successful_runs,
            "failed_runs": budget.failed_runs,
            "success_rate": budget.success_rate,
            "budget_threshold": budget.budget_threshold,
            "budget_remaining": budget.budget_remaining,
            "escalated": budget.escalated,
            "last_calculated": budget.last_calculated,
        }
    else:
        d["error_budget"] = None

    # Dependencies (Build 11)
    deps = await store.list_dependencies(p.pipeline_id)
    dependents = await store.list_dependents(p.pipeline_id)
    upstream_list = []
    for dep in deps:
        up = await store.get_pipeline(dep.depends_on_id)
        upstream_list.append({
            "dependency_id": dep.dependency_id,
            "depends_on_id": dep.depends_on_id,
            "depends_on_name": up.pipeline_name if up else dep.depends_on_id,
            "dependency_type": dep.dependency_type.value if hasattr(dep.dependency_type, "value") else dep.dependency_type,
            "notes": dep.notes,
        })
    d["dependencies"] = {
        "upstream": upstream_list,
        "downstream_count": len(dependents),
    }

    # Metadata (Build 11)
    metadata = await store.list_metadata(p.pipeline_id)
    d["metadata"] = [
        {"key": m.key, "namespace": m.namespace, "value": m.value_json, "updated_at": m.updated_at}
        for m in metadata
    ]

    # Schema change policy (Build 12)
    d["schema_change_policy"] = asdict(p.get_schema_policy())
    d["schema_change_policy_is_custom"] = p.schema_change_policy is not None

    # Steps (Build 18)
    d["steps"] = [
        {
            "step_id": s.step_id,
            "step_name": s.step_name,
            "step_type": s.step_type.value if hasattr(s.step_type, "value") else s.step_type,
            "depends_on": s.depends_on,
            "config": s.config,
            "retry_max": s.retry_max,
            "timeout_seconds": s.timeout_seconds,
            "skip_on_fail": s.skip_on_fail,
            "enabled": s.enabled,
        }
        for s in (p.steps or [])
    ]

    # Post-promotion hooks (Build 13)
    d["post_promotion_hooks"] = [asdict(h) for h in p.post_promotion_hooks]

    # Hook results from metadata (namespace="hooks")
    hook_metadata = await store.list_metadata(p.pipeline_id, namespace="hooks")
    d["hook_results"] = {
        m.key: m.value_json for m in hook_metadata
    }

    # Data contracts (Build 16)
    produced = await store.list_data_contracts(producer_id=p.pipeline_id)
    consumed = await store.list_data_contracts(consumer_id=p.pipeline_id)
    d["data_contracts"] = {
        "as_producer": [
            {
                "contract_id": c.contract_id,
                "consumer_pipeline_id": c.consumer_pipeline_id,
                "status": c.status.value if hasattr(c.status, "value") else c.status,
                "freshness_sla_minutes": c.freshness_sla_minutes,
                "violation_count": c.violation_count,
            }
            for c in produced
        ],
        "as_consumer": [
            {
                "contract_id": c.contract_id,
                "producer_pipeline_id": c.producer_pipeline_id,
                "status": c.status.value if hasattr(c.status, "value") else c.status,
                "freshness_sla_minutes": c.freshness_sla_minutes,
                "violation_count": c.violation_count,
            }
            for c in consumed
        ],
    }

    # Build 28: Context propagation flag
    d["auto_propagate_context"] = p.auto_propagate_context

    # Recent changelog
    recent_changes = await store.list_pipeline_changes(p.pipeline_id, limit=10)
    d["recent_changes"] = [
        {
            "change_type": c.change_type.value if hasattr(c.change_type, "value") else c.change_type,
            "changed_by": c.changed_by,
            "source": c.source,
            "reason": c.reason,
            "created_at": c.created_at,
        }
        for c in recent_changes
    ]

    return d


def _persist_contract_yaml(p, config):
    """Write pipeline contract to YAML file on disk for auditability."""
    contracts_dir = config.contracts_dir
    os.makedirs(contracts_dir, exist_ok=True)
    safe_name = p.pipeline_name.replace("/", "_").replace(" ", "_")
    path = os.path.join(contracts_dir, f"{safe_name}.yaml")
    yaml_str = pipeline_to_yaml(p, mask_credentials=True)
    with open(path, "w") as f:
        f.write(yaml_str)


def _gitops_commit_pipeline_sync(gitops_repo, pipeline, message: str, caller=None):
    """Synchronous: commit pipeline YAML to GitOps repo (never raises)."""
    if not gitops_repo or not gitops_repo.enabled:
        return
    try:
        yaml_content = pipeline_to_yaml(pipeline, mask_credentials=True)
        author = (caller.get("sub", "dapos") if caller else "dapos")
        gitops_repo.commit_pipeline(pipeline, yaml_content, message, author=author)
    except Exception as e:
        log.warning("GitOps commit_pipeline failed: %s", e)


def _gitops_commit_pipeline(gitops_repo, pipeline, message: str, caller=None):
    """Fire-and-forget: offload git commit to a thread so it never blocks the event loop."""
    if not gitops_repo or not gitops_repo.enabled:
        return
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None, _gitops_commit_pipeline_sync, gitops_repo, pipeline, message, caller
        )
    except RuntimeError:
        # No running loop (e.g. called from sync context) — run directly
        _gitops_commit_pipeline_sync(gitops_repo, pipeline, message, caller)


def _run_summary(r) -> dict:
    return {
        "run_id": r.run_id,
        "pipeline_id": r.pipeline_id,
        "started_at": r.started_at,
        "completed_at": r.completed_at,
        "status": r.status.value if hasattr(r.status, "value") else r.status,
        "run_mode": r.run_mode.value if hasattr(r.run_mode, "value") else r.run_mode,
        "rows_extracted": r.rows_extracted,
        "rows_loaded": r.rows_loaded,
        "staging_size_bytes": r.staging_size_bytes,
        "watermark_before": r.watermark_before,
        "watermark_after": r.watermark_after,
        "gate_decision": (
            r.gate_decision.value
            if r.gate_decision and hasattr(r.gate_decision, "value")
            else r.gate_decision
        ),
        "quality_results": r.quality_results,
        "error": r.error,
        "retry_count": r.retry_count,
        "triggered_by_run_id": r.triggered_by_run_id,
        "triggered_by_pipeline_id": r.triggered_by_pipeline_id,
        "execution_log": r.execution_log,
        "insights": r.insights,
    }


# ---------------------------------------------------------------------------
# Trust score computation (Build 26)
# ---------------------------------------------------------------------------

# Default weights — exposed via /api/catalog/trust/{id} so users can see them
_TRUST_WEIGHTS = {
    "freshness": 0.30,       # Is the data current?
    "quality_gate": 0.30,    # Did it pass quality checks?
    "error_budget": 0.25,    # Is the pipeline reliable over time?
    "schema_stability": 0.15, # Has the schema been stable?
}


def _compute_trust_score(
    freshness,
    latest_gate,
    error_budget,
    pipeline,
) -> dict:
    """Compute a 0.0-1.0 trust score from available signals.

    Each component scores 0.0-1.0, then weighted by pipeline-specific
    or global _TRUST_WEIGHTS. Returns {"score": float|None, "detail": dict, "recommendation": str}.
    """
    # Use per-pipeline weights if configured, otherwise global defaults
    weights = (pipeline.trust_weights if hasattr(pipeline, "trust_weights") and pipeline.trust_weights else _TRUST_WEIGHTS)
    components = {}
    has_data = False

    # Freshness component (0.0 - 1.0)
    if freshness:
        has_data = True
        fs = freshness.status
        status_str = fs.value if hasattr(fs, "value") else fs
        if status_str == "fresh":
            components["freshness"] = 1.0
        elif status_str == "warning":
            components["freshness"] = 0.5
        else:  # critical
            components["freshness"] = 0.1
    else:
        components["freshness"] = None

    # Quality gate component (0.0 - 1.0)
    if latest_gate:
        has_data = True
        checks = latest_gate.checks or []
        total = len(checks)
        if total > 0:
            passed = sum(1 for c in checks if c.status in ("pass", CheckStatus.PASS))
            components["quality_gate"] = passed / total
        else:
            components["quality_gate"] = None
    else:
        components["quality_gate"] = None

    # Error budget component (0.0 - 1.0)
    if error_budget:
        has_data = True
        components["error_budget"] = min(error_budget.success_rate / 100.0, 1.0) if error_budget.success_rate else 0.0
    else:
        components["error_budget"] = None

    # Schema stability component (0.0 - 1.0)
    # Based on whether schema has been stable (no schema_change_policy overrides needed)
    has_data_for_schema = pipeline.column_mappings and len(pipeline.column_mappings) > 0
    if has_data_for_schema:
        has_data = True
        components["schema_stability"] = 1.0 if pipeline.auto_approve_additive_schema else 0.7
    else:
        components["schema_stability"] = None

    if not has_data:
        return {
            "score": None,
            "detail": {k: {"score": None, "weight": v} for k, v in weights.items()},
            "recommendation": "No data available yet — run the pipeline to establish baselines",
        }

    # Weighted average (only over components that have data)
    weighted_sum = 0.0
    weight_sum = 0.0
    detail = {}
    for key, weight in weights.items():
        val = components.get(key)
        detail[key] = {"score": round(val, 3) if val is not None else None, "weight": weight}
        if val is not None:
            weighted_sum += val * weight
            weight_sum += weight

    score = round(weighted_sum / weight_sum, 3) if weight_sum > 0 else None

    # Recommendation
    if score is None:
        rec = "Insufficient data to assess trust"
    elif score >= 0.9:
        rec = "High trust — safe for production decisions"
    elif score >= 0.7:
        rec = "Good trust — reliable for most use cases"
    elif score >= 0.5:
        rec = "Medium trust — verify before critical decisions"
    else:
        rec = "Low trust — investigate quality and freshness issues"

    return {"score": score, "detail": detail, "recommendation": rec}


def _proposal_summary(p) -> dict:
    return {
        "proposal_id": p.proposal_id,
        "pipeline_id": p.pipeline_id,
        "connector_id": p.connector_id,
        "status": p.status.value if hasattr(p.status, "value") else p.status,
        "trigger_type": p.trigger_type.value if hasattr(p.trigger_type, "value") else p.trigger_type,
        "change_type": p.change_type.value if hasattr(p.change_type, "value") else p.change_type,
        "reasoning": p.reasoning,
        "confidence": p.confidence,
        "impact_analysis": p.impact_analysis,
        "current_state": p.current_state,
        "proposed_state": p.proposed_state,
        "trigger_detail": p.trigger_detail,
        "rollback_plan": p.rollback_plan,
        "contract_version_before": p.contract_version_before,
        "contract_version_after": p.contract_version_after,
        "created_at": p.created_at,
        "resolved_at": p.resolved_at,
        "resolved_by": p.resolved_by,
        "resolution_note": p.resolution_note,
    }
