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


def test_000032_deletes_prompt_copies_of_researches_deleted_earlier(database_at):
    engine = database_at("20260925_000031")
    with engine.begin() as conn:
        _add_user(conn, "u-prompts")
        conn.execute(
            text(
                "INSERT INTO researches (id, prompt, language, user_id, depth, status, graph_state, graph_trail, "
                "task_ids, created_at, updated_at) VALUES ('r-alive', 'kept topic', 'en', 'u-prompts', 'easy', "
                "'completed', '{}', '[]', '[]', now(), now())"
            )
        )
        kept = {
            _add_event(conn, "research_prompt", '{"research_id": "r-alive", "prompt": "kept topic"}', "u-prompts"),
            _add_event(conn, "chat_prompt", '{"research_id": "r-alive", "prompt": "kept follow-up"}', "u-prompts"),
            # Not a prompt copy: left alone even though its research is gone.
            _add_event(conn, "tab_focus", '{"research_id": "r-gone"}', "u-prompts"),
        }
        _add_event(conn, "research_prompt", '{"research_id": "r-gone", "prompt": "SECRET topic"}', "u-prompts")
        _add_event(conn, "chat_prompt", '{"prompt": "names no research"}')
        # Enough orphans for several batches.
        conn.execute(
            text(
                "INSERT INTO user_events (id, event_name, event_category, details, created_at) "
                "SELECT 'orphan-' || lpad(g::text, 5, '0'), 'chat_prompt', 'prompt', "
                "jsonb_build_object('research_id', 'gone-' || g, 'prompt', 'SECRET follow-up'), now() "
                "FROM generate_series(1, 2500) g"
            )
        )

    migrate_throwaway_database(_DATABASE, "20260925_000032")

    with engine.connect() as conn:
        assert set(conn.execute(text("SELECT id FROM user_events")).scalars()) == kept


def test_000033_backfills_verified_emails_and_round_trips(database_at):
    """Google-linked and operator-provisioned accounts count as verified; any other local
    account stays unverified (it may be someone's squatted address)."""
    engine = database_at("20260925_000032")
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO users (id, email, password_hash, google_subject, token_version, created_at, "
                "admin_provisioned_at) VALUES "
                "('u-google', 'g@example.com', NULL, 'sub-g', 0, now(), NULL), "
                "('u-admin', 'a@example.com', 'hash', NULL, 0, now(), now()), "
                "('u-local', 'l@example.com', 'hash', NULL, 0, now(), NULL)"
            )
        )

    migrate_throwaway_database(_DATABASE, "20260925_000033")

    with engine.connect() as conn:
        verified = dict(conn.execute(text("SELECT id, email_verified_at IS NOT NULL FROM users")).all())
        purposes = conn.execute(
            text("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_auth_action_tokens_purpose'")
        ).scalar_one()
    assert verified == {"u-google": True, "u-admin": True, "u-local": False}
    assert "password_reset" in purposes and "email_verification" in purposes
    for name in ("ix_auth_action_tokens_token_hash", "ix_auth_action_tokens_user_purpose", "ix_auth_action_tokens_expires_at"):
        assert _index_is_valid(engine, name) is True, name
    with engine.begin() as conn:
        insert = (
            "INSERT INTO auth_action_tokens (id, user_id, purpose, token_hash, email, created_at, expires_at) "
            "VALUES (:id, 'u-local', :purpose, :hash, 'l@example.com', now(), now() + interval '1 hour')"
        )
        conn.execute(text(insert), {"id": "t-1", "purpose": "password_reset", "hash": "a" * 64})
    with engine.connect() as conn, pytest.raises(DBAPIError):  # the purpose CHECK
        conn.execute(text(insert), {"id": "t-2", "purpose": "magic_link", "hash": "b" * 64})
    with engine.connect() as conn, pytest.raises(DBAPIError):  # one row per token hash
        conn.execute(text(insert), {"id": "t-3", "purpose": "email_verification", "hash": "a" * 64})
    with engine.begin() as conn:  # the tokens go with their account
        conn.execute(text("DELETE FROM users WHERE id = 'u-local'"))
        assert conn.execute(text("SELECT count(*) FROM auth_action_tokens")).scalar_one() == 0

    migrate_throwaway_database(_DATABASE, "20260925_000032", downgrade=True)

    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('auth_action_tokens')")).scalar_one() is None
        columns = conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
        ).scalars().all()
    assert "email_verified_at" not in columns

    migrate_throwaway_database(_DATABASE, "20260925_000033")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM users WHERE email_verified_at IS NOT NULL")).scalar_one() == 2


def test_000034_gives_existing_search_jobs_lease_epoch_zero_and_round_trips(database_at):
    """Jobs queued before the upgrade start at epoch 0, like a fresh claim's."""
    engine = database_at("20260925_000033")
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO researches (id, prompt, language, depth, status, graph_state, graph_trail, task_ids, "
                "created_at, updated_at) VALUES ('r-1', 'lease topic', 'en', 'easy', 'processing', '{}', '[]', "
                "'[\"t-1\"]', now(), now())"
            )
        )
        conn.execute(
            text(
                "INSERT INTO search_tasks (id, research_id, description, queries, status, logs, search_metrics, "
                "created_at, updated_at) VALUES ('t-1', 'r-1', 'd', '[]', 'running', '[]', '{}', now(), now())"
            )
        )
        conn.execute(
            text(
                "INSERT INTO search_task_jobs (id, task_id, depth, attempt_count, max_attempts, status, created_at, updated_at) "
                "VALUES ('j-1', 't-1', 'easy', 1, 3, 'running', now(), now())"
            )
        )

    migrate_throwaway_database(_DATABASE, "20260928_000034")

    with engine.connect() as conn:
        assert conn.execute(text("SELECT lease_epoch FROM search_task_jobs WHERE id = 'j-1'")).scalar_one() == 0

    migrate_throwaway_database(_DATABASE, "20260925_000033", downgrade=True)
    with engine.connect() as conn:
        columns = conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = 'search_task_jobs'")
        ).scalars().all()
    assert "lease_epoch" not in columns
    migrate_throwaway_database(_DATABASE, "20260928_000034")
