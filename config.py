"""Pipeline Agent configuration — connector-agnostic, reads from environment."""

import os
from urllib.parse import urlparse


class Config:
    def __init__(self):
        # PostgreSQL (main store + pgvector)
        # DATABASE_URL takes precedence (Railway, Render, Heroku auto-inject this)
        database_url = os.getenv("DATABASE_URL", "")
        if database_url:
            parsed = urlparse(database_url)
            self.pg_host = parsed.hostname or "localhost"
            self.pg_port = parsed.port or 5432
            self.pg_database = (parsed.path or "/pipeline_agent").lstrip("/")
            self.pg_user = parsed.username or "pipeline_agent"
            self.pg_password = parsed.password or ""
        else:
            self.pg_host = os.getenv("PG_HOST", "localhost")
            self.pg_port = int(os.getenv("PG_PORT", "5432"))
            self.pg_database = os.getenv("PG_DATABASE", "pipeline_agent")
            self.pg_user = os.getenv("PG_USER", "pipeline_agent")
            self.pg_password = os.getenv("PG_PASSWORD", "pipeline_agent")
        self.pg_pool_min = int(os.getenv("PG_POOL_MIN", "2"))
        self.pg_pool_max = int(os.getenv("PG_POOL_MAX", "10"))

        # Agent (Claude API)
        self.api_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.model = os.getenv("AGENT_MODEL", "claude-opus-4-6")

        # Embeddings (optional — enables semantic preference search via pgvector)
        self.voyage_api_key = os.getenv("VOYAGE_API_KEY", "")
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "voyage-3")

        # Staging
        self.data_dir = os.getenv("DATA_DIR", "./data")
        self.max_disk_pct = float(os.getenv("MAX_DISK_PCT", "85"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "50000"))

        # Scheduler
        self.max_concurrent = int(os.getenv("MAX_CONCURRENT_PIPELINES", "4"))

        # Alerts
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "")
        self.email_smtp_host = os.getenv("EMAIL_SMTP_HOST", "")
        self.email_smtp_port = int(os.getenv("EMAIL_SMTP_PORT", "587"))
        self.email_from = os.getenv("EMAIL_FROM", "")
        self.pagerduty_key = os.getenv("PAGERDUTY_ROUTING_KEY", "")

        # Server
        self.api_host = os.getenv("API_HOST", "0.0.0.0")
        self.api_port = int(os.getenv("PORT", os.getenv("API_PORT", "8100")))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_format = os.getenv("LOG_FORMAT", "json")
        self.log_max_bytes = int(os.getenv("LOG_MAX_BYTES", str(50 * 1024 * 1024)))
        self.log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))

        # Auth
        self.jwt_secret = os.getenv("JWT_SECRET", "") or "dapos-dev-secret-change-in-production"
        self.jwt_algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        self.jwt_expiry_hours = int(os.getenv("JWT_EXPIRY_HOURS", "24"))
        self.auth_enabled = os.getenv("AUTH_ENABLED", "true").lower() == "true"

        # Encryption (Fernet key for credentials at rest)
        self.encryption_key = os.getenv("ENCRYPTION_KEY", "")

        # GitOps (external repo for pipeline configs)
        self.pipeline_repo_path = os.getenv("PIPELINE_REPO_PATH", "")
        self.pipeline_repo_branch = os.getenv("PIPELINE_REPO_BRANCH", "main")
        self.gitops_sync_on_boot = os.getenv("GITOPS_SYNC_ON_BOOT", "false").lower() == "true"
        self.pipeline_repo_remote = os.getenv("PIPELINE_REPO_REMOTE", "")  # e.g. git@github.com:org/client1-dags-repo.git
        self.gitops_auto_push = os.getenv("GITOPS_AUTO_PUSH", "true").lower() == "true"
        self.gitops_auto_pull = os.getenv("GITOPS_AUTO_PULL", "true").lower() == "true"

    @property
    def staging_dir(self):
        return os.path.join(self.data_dir, "staging")

    @property
    def log_path(self):
        return os.path.join(self.data_dir, "logs")

    @property
    def contracts_dir(self):
        return os.path.join(self.data_dir, "contracts")

    @property
    def has_api_key(self):
        return bool(self.api_key)

    @property
    def has_encryption_key(self):
        return bool(self.encryption_key)

    @property
    def has_embeddings(self):
        return bool(self.voyage_api_key)

    @property
    def has_gitops(self):
        return bool(self.pipeline_repo_path)

    @property
    def pg_dsn(self):
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )
