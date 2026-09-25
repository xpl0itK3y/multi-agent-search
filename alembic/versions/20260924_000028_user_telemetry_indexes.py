"""key user_sessions on (session_id, user_id); index prompt events by research

Revision ID: 20260924_000028
Revises: 20260921_000027
Create Date: 2026-09-24 00:00:00

record_user_session matched on the client-supplied session_id alone, so an account
signing in on a tab another account had used updated that account's session row, and
two concurrent session_starts could insert duplicates. Sessions are now upserted on
(session_id, user_id); the unique index is the ON CONFLICT arbiter and supersedes the
plain session_id index it leads with.

Existing duplicate (session_id, user_id) rows are collapsed first, keeping the most
recently active one. A CONCURRENTLY build that fails (a duplicate raced in, a statement
or lock timeout, a killed deploy job) leaves an INVALID index, which IF NOT EXISTS would
then keep and PostgreSQL never uses, not even as an ON CONFLICT arbiter. The next upgrade
drops such an index and builds it again, and the plain session_id index is dropped only
once the unique one is valid: a stamped revision always has a working arbiter.

The research_prompt/chat_prompt rows copy prompt text into user_events, which has no
FK to researches; they are now deleted with their research (delete_research and the
retention sweep). The partial expression index finds them by details->>'research_id'.
"""

import sqlalchemy as sa
from alembic import context, op


revision = "20260924_000028"
down_revision = "20260921_000027"
branch_labels = None
depends_on = None


_DEDUPE_SESSIONS = (
    "DELETE FROM user_sessions older USING user_sessions newer "
    "WHERE older.session_id = newer.session_id AND older.user_id = newer.user_id "
    "AND (older.last_active_at, older.id) < (newer.last_active_at, newer.id)"
)


def _index_is_valid(name: str) -> bool | None:
    """pg_index.indisvalid of the index; None if it does not exist, or in offline (--sql)
    mode, where there is no database to ask."""
    if context.is_offline_mode():
        return None
    row = op.get_bind().execute(
        sa.text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"),
        {"name": name},
    ).first()
    return None if row is None else bool(row[0])


def _create_index_concurrently(name: str, on: str, *, unique: bool = False) -> None:
    """CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS <name> ON <on>, recovering from an
    earlier failed build, and raising unless the index ends up valid (IF NOT EXISTS also
    skips an index another session is still building)."""
    if _index_is_valid(name) is False:
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
    kind = "UNIQUE INDEX" if unique else "INDEX"
    op.execute(f"CREATE {kind} CONCURRENTLY IF NOT EXISTS {name} ON {on}")
    if not context.is_offline_mode() and _index_is_valid(name) is not True:
        raise RuntimeError(
            f"Index {name} is not valid after CREATE INDEX CONCURRENTLY (another build may "
            "still be running). Run the upgrade again once it has finished."
        )


def upgrade() -> None:
    op.execute(_DEDUPE_SESSIONS)
    with op.get_context().autocommit_block():
        _create_index_concurrently(
            "uq_user_sessions_session_user", "user_sessions (session_id, user_id)", unique=True
        )
        # Only now: the unique index is valid, so the upsert keeps an arbiter throughout.
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_user_sessions_session_id")
        _create_index_concurrently(
            "ix_user_events_prompt_research_id",
            "user_events ((details ->> 'research_id')) "
            "WHERE event_name IN ('chat_prompt', 'research_prompt')",
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_user_events_prompt_research_id")
        _create_index_concurrently("ix_user_sessions_session_id", "user_sessions (session_id)")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS uq_user_sessions_session_user")
