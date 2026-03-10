"""create initial tables

Revision ID: 20260310_000001
Revises: 
Create Date: 2026-03-10 12:45:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260310_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "researches",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("depth", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("final_report", sa.Text(), nullable=True),
        sa.Column("task_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "search_tasks",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_id", sa.String(length=36), nullable=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("queries", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("logs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["research_id"], ["researches.id"], ondelete="CASCADE"),
    )
    op.create_table(
        "search_results",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", sa.String(length=36), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("content", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["search_tasks.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_search_results_task_id", "search_results", ["task_id"])


def downgrade() -> None:
    op.drop_index("ix_search_results_task_id", table_name="search_results")
    op.drop_table("search_results")
    op.drop_table("search_tasks")
    op.drop_table("researches")
