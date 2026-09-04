from src.config import settings
from src.db import session as db_session
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


def test_create_engine_from_settings_reuses_engine(mocker):
    db_session._create_cached_engine.cache_clear()
    engine = object()
    create_engine = mocker.patch("src.db.session.create_engine", return_value=engine)
    mocker.patch(
        "src.db.session.get_database_url",
        return_value="postgresql+psycopg://u:p@h:5432/db",
    )
    try:
        first = db_session.create_engine_from_settings()
        second = db_session.create_engine_from_settings()

        assert first is engine
        assert second is engine
        create_engine.assert_called_once()
    finally:
        db_session._create_cached_engine.cache_clear()
