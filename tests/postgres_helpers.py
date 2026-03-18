from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src.config import settings


def create_postgres_session_factory():
    engine = create_engine(settings.resolved_database_url, pool_pre_ping=True)
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
                "TRUNCATE TABLE search_task_jobs, research_finalize_jobs, search_results, search_tasks, researches "
                "RESTART IDENTITY CASCADE"
            )
        )
        session.commit()
