"""add extraction metrics to worker heartbeats

Revision ID: 20260401_000005
Revises: 20260310_000004
Create Date: 2026-04-01 00:00:05
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260401_000005"
down_revision = "20260310_000004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_heartbeats",
        sa.Column(
            "extraction_metrics",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("worker_heartbeats", "extraction_metrics", server_default=None)


def downgrade() -> None:
    op.drop_column("worker_heartbeats", "extraction_metrics")
