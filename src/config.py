from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    deepseek_api_key: Optional[str] = None
    deepseek_model: str = "deepseek-chat"

    app_name: str = "Prompt Optimizer API"
    debug: bool = False
    task_store_backend: str = "postgres"
    allow_memory_task_store: bool = False
    smoke_analyzer_report: Optional[str] = None
    job_max_attempts: int = 3
    worker_heartbeat_ttl_seconds: int = 30
    search_job_timeout_seconds: int = 300
    finalize_job_timeout_seconds: int = 300
    search_job_retention_seconds: int = 86400
    finalize_job_retention_seconds: int = 86400

    postgres_user: str = "app"
    postgres_password: str = "app"
    postgres_db: str = "multi_agent_search"
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    database_url: Optional[str] = None

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
