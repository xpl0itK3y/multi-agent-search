"""create research finalize jobs

Revision ID: 20260310_000002
Revises: 20260310_000001
Create Date: 2026-03-10 18:05:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_000002"
down_revision = "20260310_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "research_finalize_jobs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["research_id"], ["researches.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_research_finalize_jobs_research_id",
        "research_finalize_jobs",
        ["research_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_research_finalize_jobs_research_id",
        table_name="research_finalize_jobs",
    )
    op.drop_table("research_finalize_jobs")
