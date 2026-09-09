"""index search_tasks.research_id (hot FK lookup + cascade delete)

Revision ID: 20260622_000018
Revises: 20260622_000017
Create Date: 2026-06-22 00:00:00

The one missing FK index (AUD-010): get_tasks_by_research filters search_tasks by
research_id on every status poll / summary / sources call and on cascade delete, so
without this index those are sequential scans of a growing table.

The concurrent build keeps writes available while the migration runs on a live table.
"""
from alembic import op

revision = "20260622_000018"
down_revision = "20260622_000017"
branch_labels = None
depends_on = None


_INDEX = "ix_search_tasks_research_id"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} "
            "ON search_tasks (research_id)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
