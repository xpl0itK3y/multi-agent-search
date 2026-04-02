"""add search metrics to tasks

Revision ID: 20260402_000006
Revises: 20260401_000005
Create Date: 2026-04-02 00:00:06
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260402_000006"
down_revision = "20260401_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "search_tasks",
        sa.Column(
            "search_metrics",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("search_tasks", "search_metrics", server_default=None)


def downgrade() -> None:
    op.drop_column("search_tasks", "search_metrics")
