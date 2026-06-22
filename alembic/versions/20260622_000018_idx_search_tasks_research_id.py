"""index search_tasks.research_id (hot FK lookup + cascade delete)

Revision ID: 20260622_000018
Revises: 20260622_000017
Create Date: 2026-06-22 00:00:00

The one missing FK index (AUD-010): get_tasks_by_research filters search_tasks by
research_id on every status poll / summary / sources call and on cascade delete, so
without this index those are sequential scans of a growing table.

On a large live table, create it by hand with CREATE INDEX CONCURRENTLY (outside the
alembic transaction) to avoid holding a write lock; this migration uses a plain index
build, which is fine for small/medium tables.
"""
from alembic import op

revision = "20260622_000018"
down_revision = "20260622_000017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_search_tasks_research_id", "search_tasks", ["research_id"])


def downgrade() -> None:
    op.drop_index("ix_search_tasks_research_id", table_name="search_tasks")
