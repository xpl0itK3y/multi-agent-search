from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from src.config import settings


class Base(DeclarativeBase):
    pass


def get_database_url() -> str:
    return settings.resolved_database_url


def _engine_kwargs(url: str) -> dict:
    """Pool config per process. SQLite (dev/tests) ignores QueuePool sizing, so only
    apply the pool knobs for server databases (Postgres)."""
    kwargs: dict = {"pool_pre_ping": True}
    if not url.startswith("sqlite"):
        kwargs.update(
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            # Disable psycopg server-side prepared statements so the app works through a
            # PgBouncer transaction pool (and it's harmless connecting straight to Postgres).
            connect_args={"prepare_threshold": None},
        )
    return kwargs


def create_engine_from_settings():
    url = get_database_url()
    return create_engine(url, **_engine_kwargs(url))


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=None)
