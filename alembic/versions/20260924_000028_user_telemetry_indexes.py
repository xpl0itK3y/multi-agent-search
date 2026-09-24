"""key user_sessions on (session_id, user_id)

Revision ID: 20260924_000028
Revises: 20260921_000027
Create Date: 2026-09-24 00:00:00

record_user_session matched on the client-supplied session_id alone, so an account
signing in on a tab another account had used updated that account's session row, and
two concurrent session_starts could insert duplicates. Sessions are now upserted on
(session_id, user_id); the unique index is the ON CONFLICT arbiter and supersedes the
plain session_id index it leads with.

Existing duplicate (session_id, user_id) rows are collapsed first, keeping the most
recently active one. If the index build still fails (a duplicate raced in between),
CONCURRENTLY leaves an INVALID index that IF NOT EXISTS would then skip: drop it
(DROP INDEX CONCURRENTLY uq_user_sessions_session_user) and upgrade again.
"""

from alembic import op


revision = "20260924_000028"
down_revision = "20260921_000027"
branch_labels = None
depends_on = None


_DEDUPE_SESSIONS = (
    "DELETE FROM user_sessions older USING user_sessions newer "
    "WHERE older.session_id = newer.session_id AND older.user_id = newer.user_id "
    "AND (older.last_active_at, older.id) < (newer.last_active_at, newer.id)"
)


def upgrade() -> None:
    op.execute(_DEDUPE_SESSIONS)
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_user_sessions_session_user "
            "ON user_sessions (session_id, user_id)"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_user_sessions_session_id")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_user_sessions_session_id "
            "ON user_sessions (session_id)"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS uq_user_sessions_session_user")
