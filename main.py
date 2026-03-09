"""
Pipeline Agent -- entry point.
Wires all components with dependency injection and runs four concurrent async loops:
  1. API server (FastAPI + uvicorn)
  2. Scheduler (60s tick)
  3. Monitor (5m tick)
  4. Observability (30s tick -- freshness checks + daily digest)

PostgreSQL backend via asyncpg connection pool.
"""
from __future__ import annotations

import asyncio
import logging
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import sys
from datetime import datetime, timezone

import asyncpg
import uvicorn

from config import Config
from contracts.store import Store
from contracts.models import now_iso
from connectors.registry import ConnectorRegistry
from staging.local import LocalStagingManager
from agent.core import AgentCore
from agent.autonomous import PipelineRunner
from agent.conversation import ConversationManager
from scheduler.manager import Scheduler
from monitor.engine import MonitorEngine
from quality.gate import QualityGate
from api.server import create_app

log = logging.getLogger("pipeline-agent")


def setup_logging(config: Config):
    """Configure structured logging with context propagation and rotation."""
    from logging_config import setup_logging as _setup
    _setup(
        log_level=config.log_level,
        log_dir=os.path.join(config.data_dir, "logs"),
        max_bytes=config.log_max_bytes,
        backup_count=config.log_backup_count,
        json_logging=(config.log_format == "json"),
    )


def setup_data_dirs(config: Config):
    """Create required data directories."""
    os.makedirs(config.staging_dir, exist_ok=True)
    os.makedirs(os.path.join(config.data_dir, "logs"), exist_ok=True)


async def observability_loop(config: Config, store: Store, agent: AgentCore):
    """
    30s base tick:
    - Daily digest at 9 AM UTC via agent.generate_digest()
    - Quality trend summary every 15m (logged)
    """
    log.info("Observability loop started.")
    last_digest_day: int = -1
    tick = 0

    while True:
        try:
            await asyncio.sleep(30)
            tick += 1

            # Daily digest at 9 AM UTC
            now = datetime.now(timezone.utc)
            if now.hour == 9 and now.day != last_digest_day:
                last_digest_day = now.day
                await _send_daily_digest(store, agent)

            # Log quality trend summary every 15m (30 ticks x 30s)
            if tick % 30 == 0:
                await _log_quality_summary(store)

        except asyncio.CancelledError:
            log.info("Observability loop cancelled.")
            break
        except Exception as e:
            log.exception("Observability loop error: %s", e)


async def _send_daily_digest(store: Store, agent: AgentCore):
    """Generate and log daily alert digest."""
    alerts = await store.get_undigested_alerts()
    if not alerts:
        log.info("Daily digest: no undigested alerts.")
        return

    pipelines = await store.list_pipelines()
    pipeline_names = {p.pipeline_id: p.pipeline_name for p in pipelines}
    digest_text = await agent.generate_digest(alerts, pipeline_names)
    log.info("Daily digest:\n%s", digest_text)

    # Mark alerts as digested
    for alert in alerts:
        alert.digested = True
        await store.save_alert(alert)


async def _log_quality_summary(store: Store):
    """Log a summary of quality issues across active pipelines."""
    pipelines = await store.list_pipelines(status="active")
    if not pipelines:
        return
    issues = []
    for p in pipelines:
        gates = await store.list_gates(p.pipeline_id)
        if gates:
            halted = sum(1 for g in gates if g.decision.value == "halt")
            if halted:
                issues.append(f"{p.pipeline_name}: {halted} halt(s) in 24h")
    if issues:
        log.warning("Quality summary -- issues: %s", "; ".join(issues))
    else:
        log.info("Quality summary -- all pipelines healthy.")


async def main():
    # 1. Load config
    config = Config()

    # 2. Setup logging
    setup_logging(config)

    # 3. Setup data directories
    setup_data_dirs(config)

    log.info("Starting Pipeline Agent...")
    log.info("  Data dir: %s", config.data_dir)
    log.info("  API: %s:%d", config.api_host, config.api_port)
    log.info("  Agent model: %s", config.model if config.has_api_key else "rule-based (no API key)")
    log.info("  PostgreSQL: %s:%d/%s", config.pg_host, config.pg_port, config.pg_database)

    # 4. Create asyncpg connection pool
    pool = await asyncpg.create_pool(
        dsn=config.pg_dsn,
        min_size=config.pg_pool_min,
        max_size=config.pg_pool_max,
    )
    log.info("  PostgreSQL pool created (min=%d, max=%d)", config.pg_pool_min, config.pg_pool_max)

    try:
        # 5. Initialize store and create tables
        store = Store()
        await store.initialize(pool)
        await store.create_tables()
        log.info("  Database tables ready.")

        # 5b. Bootstrap default admin user
        existing_admin = await store.get_user_by_username("admin")
        if not existing_admin:
            import bcrypt as _bcrypt
            from contracts.models import User
            hashed = _bcrypt.hashpw(b"admin", _bcrypt.gensalt()).decode("utf-8")
            admin_user = User(
                username="admin",
                password_hash=hashed,
                role="admin",
                email="admin@dapos.local",
            )
            await store.save_user(admin_user)
            log.info("  Default admin user created (admin/admin)")
        else:
            log.info("  Admin user exists.")

        # 6. Build all components with dependency injection
        registry = ConnectorRegistry(store, config)
        agent = AgentCore(config, store)
        gate = QualityGate(store, config)
        staging_mgr = LocalStagingManager(config.data_dir)
        runner = PipelineRunner(config, store, registry, gate, staging_mgr)
        conversation = ConversationManager(config, store, registry, agent)
        scheduler = Scheduler(config, store, runner)
        monitor = MonitorEngine(config, store, registry, agent)

        # 7. Bootstrap seed connectors and load active ones
        await registry.bootstrap_seeds()
        await registry.load_all_active()
        log.info("  Connectors loaded.")

        # 7b. Bootstrap demo pipelines (first startup only)
        from demo.bootstrap import bootstrap_demo_pipelines
        await bootstrap_demo_pipelines(store, registry, runner)

        pipelines = await store.list_pipelines(status="active")
        log.info("  Active pipelines: %d", len(pipelines))

        # 8. Create FastAPI application
        app = create_app(
            config, store, registry, agent, conversation, runner, scheduler, monitor
        )

        # 9. Build uvicorn config and run all 4 loops concurrently
        uvi_config = uvicorn.Config(
            app=app,
            host=config.api_host,
            port=config.api_port,
            log_level=config.log_level.lower(),
            access_log=False,
        )
        server = uvicorn.Server(uvi_config)

        await asyncio.gather(
            server.serve(),
            scheduler.run_forever(),
            monitor.run_forever(),
            observability_loop(config, store, agent),
        )

    finally:
        # 10. Graceful shutdown: close pool
        log.info("Shutting down -- closing PostgreSQL pool...")
        await pool.close()
        log.info("PostgreSQL pool closed. Goodbye.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
