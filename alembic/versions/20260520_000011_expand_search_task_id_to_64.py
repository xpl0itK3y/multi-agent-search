"""expand search_tasks.id to VARCHAR(64) for prefixed replan IDs

Revision ID: 20260520_000011
Revises: 20260405_000010
Create Date: 2026-05-20 00:11:00
"""

from alembic import op


revision = "20260520_000011"
down_revision = "20260405_000010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # replan task IDs are "replan-{uuid}" = 43 chars; VARCHAR(36) is too short.
    # search_task_jobs.task_id is a FK referencing search_tasks.id — must match.
    op.execute("ALTER TABLE search_tasks ALTER COLUMN id TYPE VARCHAR(64)")
    op.execute("ALTER TABLE search_task_jobs ALTER COLUMN task_id TYPE VARCHAR(64)")


def downgrade() -> None:
    op.execute("ALTER TABLE search_task_jobs ALTER COLUMN task_id TYPE VARCHAR(36)")
    op.execute("ALTER TABLE search_tasks ALTER COLUMN id TYPE VARCHAR(36)")
