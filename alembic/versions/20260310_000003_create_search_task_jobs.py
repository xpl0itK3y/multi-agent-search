"""create search task jobs

Revision ID: 20260310_000003
Revises: 20260310_000002
Create Date: 2026-03-10 18:55:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_000003"
down_revision = "20260310_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "search_task_jobs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("task_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["search_tasks.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_search_task_jobs_task_id",
        "search_task_jobs",
        ["task_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_search_task_jobs_task_id", table_name="search_task_jobs")
    op.drop_table("search_task_jobs")
