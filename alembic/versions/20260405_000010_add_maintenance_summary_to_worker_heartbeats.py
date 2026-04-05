"""add maintenance summary to worker heartbeats

Revision ID: 20260405_000010
Revises: 20260405_000009
Create Date: 2026-04-05 00:10:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260405_000010"
down_revision = "20260405_000009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_heartbeats",
        sa.Column(
            "maintenance_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("worker_heartbeats", "maintenance_summary", server_default=None)


def downgrade() -> None:
    op.drop_column("worker_heartbeats", "maintenance_summary")
