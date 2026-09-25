"""Individual migrations run against a throwaway database that starts at the revision
before them: states a fresh `upgrade head` never produces (an INVALID index left by a
failed CONCURRENTLY build) and data written by an earlier release.

Postgres-only; the database is separate from the shared test DB and dropped afterwards.
"""
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.postgres_helpers import (
    create_migrated_throwaway_database,
    drop_throwaway_database,
    migrate_throwaway_database,
    server_base_url,
)

pytestmark = pytest.mark.postgres

_DATABASE = "mas_postgres_migration_steps"


@pytest.fixture
def database_at():
    """Creates the throwaway database migrated to the given revision; returns its engine."""
    engines = []

    def create(revision: str):
        engine, _session_factory = create_migrated_throwaway_database(_DATABASE, revision)
        engines.append(engine)
        return engine

    yield create
    for engine in engines:
        engine.dispose()
    drop_throwaway_database(server_base_url(), _DATABASE)


def _index_is_valid(engine, name: str) -> bool | None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"), {"name": name}
        ).first()
    return None if row is None else row[0]


def _add_user(conn, user_id: str) -> None:
    conn.execute(
        text("INSERT INTO users (id, email, token_version, created_at) VALUES (:id, :email, 0, now())"),
        {"id": user_id, "email": f"{user_id}@example.com"},
    )


def _add_session(conn, user_id: str, session_id: str, minutes_ago: int) -> str:
    row_id = str(uuid.uuid4())
    conn.execute(
        text(
            "INSERT INTO user_sessions (id, user_id, session_id, device_type, started_at, last_active_at) "
            "VALUES (:id, :user_id, :session_id, 'desktop', now(), now() - make_interval(mins => :ago))"
        ),
        {"id": row_id, "user_id": user_id, "session_id": session_id, "ago": minutes_ago},
    )
    return row_id


def _add_event(conn, event_name: str, details_json: str, user_id: str | None = None) -> str:
    event_id = str(uuid.uuid4())
    conn.execute(
        text(
            "INSERT INTO user_events (id, user_id, event_name, event_category, details, created_at) "
            "VALUES (:id, :user_id, :name, 'prompt', CAST(:details AS jsonb), now())"
        ),
        {"id": event_id, "user_id": user_id, "name": event_name, "details": details_json},
    )
    return event_id


def test_000028_rebuilds_invalid_indexes_left_by_a_failed_build(database_at):
    engine = database_at("20260921_000027")
    with engine.begin() as conn:
        _add_user(conn, "u-dup")
        older = _add_session(conn, "u-dup", "tab-1", minutes_ago=10)
        newer = _add_session(conn, "u-dup", "tab-1", minutes_ago=1)
    autocommit = engine.execution_options(isolation_level="AUTOCOMMIT")
    # A first attempt whose unique build hit a duplicate: the INVALID index stays behind.
    with autocommit.connect() as conn, pytest.raises(DBAPIError):
        conn.execute(
            text("CREATE UNIQUE INDEX CONCURRENTLY uq_user_sessions_session_user ON user_sessions (session_id, user_id)")
        )
    # And a prompt-index build that timed out waiting for an open writer.
    with engine.connect() as writer:
        _add_event(writer, "tab_focus", "{}")  # holds its transaction open
        with autocommit.connect() as conn, pytest.raises(DBAPIError):
            conn.execute(text("SET lock_timeout = '300ms'"))
            conn.execute(
                text(
                    "CREATE INDEX CONCURRENTLY ix_user_events_prompt_research_id "
                    "ON user_events ((details ->> 'research_id')) "
                    "WHERE event_name IN ('chat_prompt', 'research_prompt')"
                )
            )
        writer.rollback()
    assert _index_is_valid(engine, "uq_user_sessions_session_user") is False
    assert _index_is_valid(engine, "ix_user_events_prompt_research_id") is False

    migrate_throwaway_database(_DATABASE, "20260924_000028")

    assert _index_is_valid(engine, "uq_user_sessions_session_user") is True
    assert _index_is_valid(engine, "ix_user_events_prompt_research_id") is True
    assert _index_is_valid(engine, "ix_user_sessions_session_id") is None
    with engine.begin() as conn:
        assert conn.execute(text("SELECT id FROM user_sessions")).scalars().all() == [newer]
        # The store's upsert: needs the unique index as its ON CONFLICT arbiter.
        conn.execute(
            text(
                "INSERT INTO user_sessions (id, user_id, session_id, device_type, started_at, last_active_at) "
                "VALUES (:id, 'u-dup', 'tab-1', 'desktop', now(), now()) "
                "ON CONFLICT (session_id, user_id) DO UPDATE SET last_active_at = excluded.last_active_at"
            ),
            {"id": str(uuid.uuid4())},
        )
        assert conn.execute(text("SELECT count(*) FROM user_sessions")).scalar_one() == 1
    assert older != newer

    migrate_throwaway_database(_DATABASE, "20260921_000027", downgrade=True)

    assert _index_is_valid(engine, "ix_user_sessions_session_id") is True
    assert _index_is_valid(engine, "uq_user_sessions_session_user") is None
