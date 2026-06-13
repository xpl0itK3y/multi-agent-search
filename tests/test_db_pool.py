from src.config import settings
from src.db.session import _engine_kwargs


def test_engine_kwargs_sqlite_skips_pool_sizing():
    # SQLite ignores QueuePool sizing — only pre_ping is set.
    assert _engine_kwargs("sqlite:///./dev.db") == {"pool_pre_ping": True}


def test_engine_kwargs_postgres_applies_pool_settings():
    kw = _engine_kwargs("postgresql+psycopg://u:p@h:5432/db")
    assert kw["pool_pre_ping"] is True
    assert kw["pool_size"] == settings.db_pool_size
    assert kw["max_overflow"] == settings.db_max_overflow
    assert kw["pool_timeout"] == settings.db_pool_timeout
    assert kw["pool_recycle"] == settings.db_pool_recycle
