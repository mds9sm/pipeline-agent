"""
FastAPI REST API server with JWT auth, rate limiting, and agent-routed commands.
"""
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

import bcrypt
from fastapi import FastAPI, HTTPException, Query, Depends, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import JSONResponse

from contracts.models import (
    PipelineStatus, ProposalStatus, ConnectorStatus,
    PipelineDependency, NotificationPolicy, AgentPreference,
    ContractChangeProposal, SchemaVersion, ColumnMapping,
    User, TriggerType, ChangeType,
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
from crypto import encrypt_dict, decrypt_dict, CREDENTIAL_FIELDS
from config import Config

log = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)


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
    email: Optional[str] = None


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


class BatchCreateRequest(BaseModel):
    pipelines: list[CreatePipelineRequest]


class UpdatePipelineRequest(BaseModel):
    schedule_cron: Optional[str] = None
    tier: Optional[int] = None
    owner: Optional[str] = None
    tags: Optional[dict] = None


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

    auth_dep = AuthDependency(config)

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
        if caller.get("role") != "admin":
            raise HTTPException(403, "Only admins can register new users")
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

    # In-memory conversation history per session
    _chat_sessions: dict[str, list[dict]] = {}

    @app.post("/api/command")
    @limiter.limit("100/minute")
    async def command(
        request: Request,
        req: CommandRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        try:
            # Maintain conversation history per session
            session_id = req.session_id or "default"
            if session_id not in _chat_sessions:
                _chat_sessions[session_id] = []
            history = _chat_sessions[session_id]

            routed = await agent.route_command(req.text, req.context, history=history)
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
                result_data = {
                    "connectors": [
                        {
                            "name": c.connector_name,
                            "type": c.connector_type.value,
                            "database_type": c.source_target_type,
                        }
                        for c in connectors
                    ],
                    "fallback_text": f"Found {len(connectors)} connector(s).",
                }

            elif action == "discover_tables":
                connector_id = await _resolve_connector(
                    params.get("connector_id", ""),
                    params.get("connector_type", "").lower(),
                )
                database = params.get("database", "")

                if not connector_id:
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

                src_connector_id = await _resolve_connector(
                    params.get("source_connector_id", ""), src_connector_type, role="source"
                )
                tgt_connector_id = await _resolve_connector(
                    params.get("target_connector_id", ""), tgt_connector_type, role="target"
                )

                if not src_connector_id:
                    result_data = {"status": "need_source", "fallback_text": "What type of source database? (sqlite, mysql, etc.)"}
                elif not tgt_connector_id:
                    result_data = {"status": "need_target", "fallback_text": "What type of target database? (postgres, redshift, etc.)"}
                elif not src_table:
                    result_data = {"status": "need_table", "fallback_text": "Which table would you like to create a pipeline for?"}
                elif not tgt_database:
                    result_data = {"status": "need_target_db", "fallback_text": "What's the target database name?"}
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
            if use_conversational and result_data:
                try:
                    response_text = await agent.conversational_response(
                        req.text, action, result_data, history=history,
                    )
                except Exception as e:
                    log.warning("Conversational response failed, using fallback: %s", e)
                    response_text = result_data.get("fallback_text", response_text)

            # Save to conversation history
            history.append({"role": "user", "text": req.text})
            history.append({"role": "assistant", "text": response_text})
            # Keep last 20 messages
            if len(history) > 20:
                _chat_sessions[session_id] = history[-20:]

            return {"response": response_text}
        except Exception as e:
            log.exception("Command routing error")
            raise HTTPException(500, f"Command failed: {str(e)}")

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
        c = await store.get_connector(connector_id)
        if not c:
            raise HTTPException(404, "Connector not found")
        params = params or {}
        try:
            if c.connector_type in ("source", "SOURCE"):
                engine = registry.get_source(connector_id, **params)
            else:
                engine = registry.get_target(connector_id, **params)
            result = await engine.test_connection()
            c.test_results = {
                "success": result.success,
                "version": result.version,
                "error": result.error,
            }
            await store.save_connector(c)
            return c.test_results
        except Exception as e:
            return {"success": False, "error": str(e)}

    @app.delete("/api/connectors/{connector_id}")
    @limiter.limit("100/minute")
    async def deprecate_connector(
        request: Request,
        connector_id: str,
        caller: dict = Depends(auth_dep),
    ):
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
        return _pipeline_summary(pipeline)

    @app.post("/api/pipelines/batch")
    @limiter.limit("100/minute")
    async def batch_create_pipelines(
        request: Request,
        req: BatchCreateRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
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
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        if req.schedule_cron is not None:
            p.schedule_cron = req.schedule_cron
        if req.tier is not None:
            p.tier = req.tier
        if req.owner is not None:
            p.owner = req.owner
        if req.tags is not None:
            p.tags = req.tags
        await store.save_pipeline(p)
        return _pipeline_summary(p)

    @app.post("/api/pipelines/{pipeline_id}/trigger")
    @limiter.limit("100/minute")
    async def trigger_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        run = await scheduler.trigger(pipeline_id)
        return {"run_id": run.run_id, "status": run.status.value}

    @app.post("/api/pipelines/{pipeline_id}/backfill")
    @limiter.limit("100/minute")
    async def backfill_pipeline(
        request: Request,
        pipeline_id: str,
        req: BackfillRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
        run = await scheduler.trigger_backfill(pipeline_id, req.start, req.end)
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
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        p.status = PipelineStatus.PAUSED
        await store.save_pipeline(p)
        return {"status": "paused"}

    @app.post("/api/pipelines/{pipeline_id}/resume")
    @limiter.limit("100/minute")
    async def resume_pipeline(
        request: Request,
        pipeline_id: str,
        caller: dict = Depends(auth_dep),
    ):
        p = await store.get_pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        p.status = PipelineStatus.ACTIVE
        await store.save_pipeline(p)
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
        return [_proposal_summary(p) for p in proposals]

    @app.post("/api/approvals/{proposal_id}")
    @limiter.limit("100/minute")
    async def resolve_approval(
        request: Request,
        proposal_id: str,
        req: ApprovalRequest = Body(...),
        caller: dict = Depends(auth_dep),
    ):
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
            await _apply_proposal(proposal, store, registry, agent)
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
                }
            )
        return {"tiers": {str(k): v for k, v in sorted(grouped.items())}}

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
                "created_at": a.created_at,
                "acknowledged": a.acknowledged,
                "acknowledged_by": getattr(a, "acknowledged_by", None),
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
        column_lineage = await store.list_column_lineage(pipeline_id)
        downstream_columns = await store.get_downstream_columns(pipeline_id)

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
                    "lineage_id": cl.lineage_id,
                    "source_column": cl.source_column,
                    "target_column": cl.target_column,
                    "transform_logic": cl.transform_logic,
                }
                for cl in column_lineage
            ] if column_lineage else [],
            "downstream_columns": [
                {
                    "lineage_id": dc.lineage_id,
                    "pipeline_id": dc.pipeline_id,
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
            "budget_id": budget.budget_id,
            "total_budget_minutes": budget.total_budget_minutes,
            "consumed_minutes": budget.consumed_minutes,
            "remaining_minutes": budget.remaining_minutes,
            "budget_period": budget.budget_period,
            "period_start": budget.period_start,
            "period_end": budget.period_end,
            "utilization_pct": round(
                (budget.consumed_minutes / budget.total_budget_minutes) * 100, 2
            )
            if budget.total_budget_minutes > 0
            else 0,
            "status": (
                "ok"
                if budget.consumed_minutes < budget.total_budget_minutes * 0.8
                else "warning"
                if budget.consumed_minutes < budget.total_budget_minutes
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
    # Serve static UI
    # -----------------------------------------------------------------------

    _ui_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ui")
    if os.path.exists(_ui_dir):
        app.mount("/static", StaticFiles(directory=_ui_dir), name="static")

        @app.get("/")
        async def serve_ui():
            return FileResponse(os.path.join(_ui_dir, "index.html"))

        @app.get("/{full_path:path}")
        async def catch_all(full_path: str):
            if full_path.startswith("api/") or full_path in ("health", "metrics"):
                raise HTTPException(404)
            return FileResponse(os.path.join(_ui_dir, "index.html"))

    return app


# ---------------------------------------------------------------------------
# Proposal application helper
# ---------------------------------------------------------------------------

async def _apply_proposal(
    proposal: ContractChangeProposal,
    store: Store,
    registry: ConnectorRegistry,
    agent: AgentCore,
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
        proposed_cols = proposal.proposed_state.get("column_mappings", [])
        pipeline.column_mappings = [ColumnMapping(**m) for m in proposed_cols]
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
            "quality_config": {
                "count_tolerance": p.quality_config.count_tolerance,
                "promote_on_warn": p.quality_config.promote_on_warn,
                "halt_on_first_fail": p.quality_config.halt_on_first_fail,
            }
            if p.quality_config
            else None,
            "agent_reasoning": p.agent_reasoning,
            "baseline_row_count": p.baseline_row_count,
            "notification_policy_id": p.notification_policy_id,
        }
    )
    # Include error budget summary
    budget = await store.get_error_budget(p.pipeline_id)
    if budget:
        d["error_budget"] = {
            "total_budget_minutes": budget.total_budget_minutes,
            "consumed_minutes": budget.consumed_minutes,
            "remaining_minutes": budget.remaining_minutes,
            "utilization_pct": round(
                (budget.consumed_minutes / budget.total_budget_minutes) * 100, 2
            )
            if budget.total_budget_minutes > 0
            else 0,
        }
    else:
        d["error_budget"] = None
    return d


def _run_summary(r) -> dict:
    return {
        "run_id": r.run_id,
        "pipeline_id": r.pipeline_id,
        "started_at": r.started_at,
        "completed_at": r.completed_at,
        "status": r.status.value if hasattr(r.status, "value") else r.status,
        "rows_extracted": r.rows_extracted,
        "rows_loaded": r.rows_loaded,
        "gate_decision": (
            r.gate_decision.value
            if r.gate_decision and hasattr(r.gate_decision, "value")
            else r.gate_decision
        ),
        "error": r.error,
        "retry_count": r.retry_count,
    }


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
        "created_at": p.created_at,
        "resolved_at": p.resolved_at,
        "resolved_by": p.resolved_by,
        "resolution_note": p.resolution_note,
    }
