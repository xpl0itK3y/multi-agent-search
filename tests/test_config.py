from src.config import Settings


def test_settings_builds_database_url_from_parts():
    settings = Settings(
        _env_file=None,
        postgres_user="postgres",
        postgres_password="secret",
        postgres_host="db",
        postgres_port=5432,
        postgres_db="research",
        database_url=None,
    )

    assert (
        settings.resolved_database_url
        == "postgresql+psycopg://postgres:secret@db:5432/research"
    )


def test_settings_uses_explicit_database_url():
    settings = Settings(
        _env_file=None,
        database_url="postgresql+psycopg://custom:custom@localhost:5432/custom_db",
    )

    assert (
        settings.resolved_database_url
        == "postgresql+psycopg://custom:custom@localhost:5432/custom_db"
    )


def test_settings_defaults_to_postgres_task_store():
    settings = Settings(_env_file=None)

    assert settings.task_store_backend == "postgres"


def test_settings_supports_optional_smoke_analyzer_report():
    settings = Settings(_env_file=None, smoke_analyzer_report="stub report")

    assert settings.smoke_analyzer_report == "stub report"


def test_settings_supports_job_retry_and_worker_heartbeat_defaults():
    settings = Settings(_env_file=None)

    assert settings.job_max_attempts == 3
    assert settings.worker_heartbeat_ttl_seconds == 30
    assert settings.search_job_timeout_seconds == 300
    assert settings.finalize_job_timeout_seconds == 300
    assert settings.search_job_retention_seconds == 86400
    assert settings.finalize_job_retention_seconds == 86400
    assert settings.search_extraction_concurrency == 4
    assert settings.search_extraction_timeout_seconds == 12
    assert settings.search_extraction_max_redirects == 1
    assert settings.search_domain_fail_threshold == 2
    assert settings.search_domain_cooldown_seconds == 600


def test_settings_supports_optional_langsmith_flags():
    settings = Settings(
        _env_file=None,
        langsmith_tracing=True,
        langsmith_api_key="test-key",
        langsmith_endpoint="https://api.smith.langchain.com",
        langsmith_project="mas-dev",
    )

    assert settings.langsmith_tracing is True
    assert settings.langsmith_api_key == "test-key"
    assert settings.langsmith_endpoint == "https://api.smith.langchain.com"
    assert settings.langsmith_project == "mas-dev"
