from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    deepseek_api_key: Optional[str] = None
    deepseek_model: str = "deepseek-v4-pro"
    deepseek_repair_model: Optional[str] = None
    # Reasoning-capable model for planner/gap-analysis steps; must expose `reasoning_content`
    # so the "thinking" stream (frontend §4.1) can be shown. Falls back to deepseek_model if unset.
    deepseek_reasoner_model: Optional[str] = None
    langsmith_tracing: bool = False
    langsmith_api_key: Optional[str] = None
    langsmith_endpoint: str = "https://api.smith.langchain.com"
    langsmith_project: Optional[str] = None
    log_format: str = "text"
    prometheus_metrics_enabled: bool = True

    app_name: str = "Prompt Optimizer API"
    debug: bool = False
    # Comma-separated list of allowed CORS origins for the Vue SPA (dev: Vite 5173 / preview 4173).
    cors_allow_origins: str = "http://localhost:5173,http://localhost:4173"
    task_store_backend: str = "postgres"
    allow_memory_task_store: bool = False
    smoke_analyzer_report: Optional[str] = None
    job_max_attempts: int = 3
    worker_heartbeat_ttl_seconds: int = 30
    search_job_timeout_seconds: int = 300
    finalize_job_timeout_seconds: int = 600
    search_job_retention_seconds: int = 86400
    finalize_job_retention_seconds: int = 86400
    search_extraction_concurrency: int = 4
    search_extraction_timeout_seconds: int = 12
    search_extraction_max_redirects: int = 1
    search_domain_fail_threshold: int = 2
    search_domain_cooldown_seconds: int = 600
    analyzer_max_sources: int = 24
    analyzer_max_sources_per_domain: int = 3
    analyzer_max_sources_per_task: int = 6
    analyzer_payload_char_budget: int = 28000
    analyzer_conflict_source_limit: int = 12
    analyzer_evidence_source_limit: int = 12
    analyzer_local_repair_issue_threshold: int = 20
    use_langgraph_finalize_graph: bool = True
    langgraph_replan_max_loops: int = 0
    langgraph_verification_max_retries: int = 0
    langgraph_tie_break_max_loops: int = 0
    graph_step_event_history_limit: int = 250
    graph_step_event_retention_seconds: int = 86400
    graph_trail_history_limit: int = 200
    graph_trail_retention_seconds: int = 604800

    postgres_user: str = "app"
    postgres_password: str = "app"
    postgres_db: str = "multi_agent_search"
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    database_url: Optional[str] = None

    redis_url: Optional[str] = None
    use_redis_broker: bool = False
    redis_broker_pop_timeout_seconds: int = 2
    decompose_recovery_minutes: int = 10

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
