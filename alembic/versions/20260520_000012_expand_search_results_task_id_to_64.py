"""expand search_results.task_id to VARCHAR(64) for prefixed replan IDs

Revision ID: 20260520_000012
Revises: 20260520_000011
Create Date: 2026-05-20 10:30:00
"""

from alembic import op


revision = "20260520_000012"
down_revision = "20260520_000011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE search_results ALTER COLUMN task_id TYPE VARCHAR(64)")


def downgrade() -> None:
    op.execute("ALTER TABLE search_results ALTER COLUMN task_id TYPE VARCHAR(36)")
