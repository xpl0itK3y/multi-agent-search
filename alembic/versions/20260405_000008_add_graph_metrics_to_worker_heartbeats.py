"""add graph metrics to worker heartbeats

Revision ID: 20260405_000008
Revises: 20260405_000007
Create Date: 2026-04-05 16:10:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260405_000008"
down_revision = "20260405_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_heartbeats",
        sa.Column(
            "graph_metrics",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("worker_heartbeats", "graph_metrics", server_default=None)


def downgrade() -> None:
    op.drop_column("worker_heartbeats", "graph_metrics")
