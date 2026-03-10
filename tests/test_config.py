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
