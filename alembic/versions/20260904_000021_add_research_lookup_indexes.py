"""add indexes for research history, threads, and share-token lookups

Revision ID: 20260904_000021
Revises: 20260904_000020
Create Date: 2026-09-04 00:00:00
"""

from alembic import op


revision = "20260904_000021"
down_revision = "20260904_000020"
branch_labels = None
depends_on = None


_INDEXES = (
    (
        "ix_researches_thread_id",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_researches_thread_id "
        "ON researches ((graph_state ->> 'thread_id'))",
    ),
    (
        "ix_researches_share_token",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_researches_share_token "
        "ON researches ((graph_state ->> 'share_token'))",
    ),
    (
        "ix_researches_user_created",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_researches_user_created "
        "ON researches (user_id, created_at DESC)",
    ),
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for _, statement in _INDEXES:
            op.execute(statement)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _ in reversed(_INDEXES):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
