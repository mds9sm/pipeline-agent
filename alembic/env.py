"""Alembic migration environment — uses pg_dsn from Config."""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override DSN from environment if available
pg_dsn = os.getenv("PG_DSN", "")
if not pg_dsn:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DATABASE", "pipeline_agent")
    pg_user = os.getenv("PG_USER", "pipeline_agent")
    pg_pass = os.getenv("PG_PASSWORD", "pipeline_agent")
    pg_dsn = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

config.set_main_option("sqlalchemy.url", pg_dsn)


def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=None, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
