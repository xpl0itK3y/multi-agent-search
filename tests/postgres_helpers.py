from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src.config import settings

# Shared throwaway database for integration tests. Tests must never depend on (or
# truncate) a developer's working database — it may sit on another branch's alembic
# revision (e.g. an unfinished feature branch), which breaks migrations and leaks
# test data into real work.
POSTGRES_TEST_DATABASE = "mas_postgres_tests"
# A host that drops packets (rather than refusing) otherwise stalls the first
# connection for minutes before the "server unreachable" skip.
_PROBE_CONNECT_ARGS = {"connect_timeout": 5}


def server_base_url() -> str:
    """Server URL without the database name, for CREATE/DROP DATABASE connections."""
    if settings.database_url:
        return settings.database_url.rsplit("/", 1)[0]
    return (
        f"postgresql+psycopg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}"
    )


def drop_throwaway_database(base_url: str, name: str) -> None:
    try:
        server = create_engine(
            f"{base_url}/postgres", isolation_level="AUTOCOMMIT", pool_pre_ping=True
        )
        with server.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        server.dispose()
    except SQLAlchemyError:
        pass  # best-effort cleanup


def create_migrated_throwaway_database(name: str = POSTGRES_TEST_DATABASE):
    """Create a throwaway database migrated to head; skips the test if no Postgres.

    Returns (engine, session_factory). Call drop_throwaway_database() when done."""
    import pytest

    base_url = server_base_url()
    try:
        server = create_engine(
            f"{base_url}/postgres",
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True,
            connect_args=_PROBE_CONNECT_ARGS,
        )
        with server.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        server.dispose()
    except SQLAlchemyError as exc:
        pytest.skip(f"Postgres integration test skipped (server unreachable): {exc}")

    # Migrate in-process; point alembic's env at the throwaway DB via the live
    # settings object, then restore whatever the developer had configured.
    original_db = settings.postgres_db
    original_url = settings.database_url
    settings.database_url = ""
    settings.postgres_db = name
    try:
        from alembic import command
        from alembic.config import Config

        command.upgrade(Config("alembic.ini"), "head")
    finally:
        settings.postgres_db = original_db
        settings.database_url = original_url

    engine = create_engine(f"{base_url}/{name}", pool_pre_ping=True)
    return engine, sessionmaker(bind=engine, autocommit=False, autoflush=False)


def require_postgres(session_factory) -> None:
    try:
        with session_factory() as session:
            session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        import pytest

        pytest.skip(f"Postgres integration test skipped: {exc}")


def truncate_runtime_tables(session_factory) -> None:
    with session_factory() as session:
        session.execute(
            text(
                "TRUNCATE TABLE search_task_jobs, research_finalize_jobs, search_results, search_tasks, researches, "
                "user_events, user_sessions, admin_audit_logs RESTART IDENTITY CASCADE"
            )
        )
        session.commit()
