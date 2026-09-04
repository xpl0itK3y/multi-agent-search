from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase

from src.config import settings


class Base(DeclarativeBase):
    pass


def get_database_url() -> str:
    return settings.resolved_database_url


def _engine_kwargs(
    url: str,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: int | None = None,
    pool_recycle: int | None = None,
) -> dict:
    """Pool config per process. SQLite (dev/tests) ignores QueuePool sizing, so only
    apply the pool knobs for server databases (Postgres)."""
    kwargs: dict = {"pool_pre_ping": True}
    if not url.startswith("sqlite"):
        kwargs.update(
            pool_size=settings.db_pool_size if pool_size is None else pool_size,
            max_overflow=settings.db_max_overflow if max_overflow is None else max_overflow,
            pool_timeout=settings.db_pool_timeout if pool_timeout is None else pool_timeout,
            pool_recycle=settings.db_pool_recycle if pool_recycle is None else pool_recycle,
            # Disable psycopg server-side prepared statements so the app works through a
            # PgBouncer transaction pool (and it's harmless connecting straight to Postgres).
            connect_args={"prepare_threshold": None},
        )
    return kwargs


@lru_cache(maxsize=8)
def _create_cached_engine(
    url: str,
    pool_size: int,
    max_overflow: int,
    pool_timeout: int,
    pool_recycle: int,
):
    return create_engine(
        url,
        **_engine_kwargs(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
        ),
    )


def create_engine_from_settings():
    return _create_cached_engine(
        get_database_url(),
        settings.db_pool_size,
        settings.db_max_overflow,
        settings.db_pool_timeout,
        settings.db_pool_recycle,
    )
