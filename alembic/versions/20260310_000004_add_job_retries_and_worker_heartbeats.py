"""add job retries and worker heartbeats

Revision ID: 20260310_000004
Revises: 20260310_000003
Create Date: 2026-03-10 20:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_000004"
down_revision = "20260310_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "research_finalize_jobs",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "research_finalize_jobs",
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
    )
    op.add_column(
        "search_task_jobs",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "search_task_jobs",
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
    )
    op.alter_column("research_finalize_jobs", "attempt_count", server_default=None)
    op.alter_column("research_finalize_jobs", "max_attempts", server_default=None)
    op.alter_column("search_task_jobs", "attempt_count", server_default=None)
    op.alter_column("search_task_jobs", "max_attempts", server_default=None)

    op.create_table(
        "worker_heartbeats",
        sa.Column("worker_name", sa.String(length=64), primary_key=True),
        sa.Column("processed_jobs", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("worker_heartbeats")
    op.drop_column("search_task_jobs", "max_attempts")
    op.drop_column("search_task_jobs", "attempt_count")
    op.drop_column("research_finalize_jobs", "max_attempts")
    op.drop_column("research_finalize_jobs", "attempt_count")
