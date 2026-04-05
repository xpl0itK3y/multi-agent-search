"""add graph state to researches

Revision ID: 20260405_000007
Revises: 20260402_000006
Create Date: 2026-04-05 15:20:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260405_000007"
down_revision = "20260402_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "researches",
        sa.Column(
            "graph_state",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "researches",
        sa.Column(
            "graph_trail",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.alter_column("researches", "graph_state", server_default=None)
    op.alter_column("researches", "graph_trail", server_default=None)


def downgrade() -> None:
    op.drop_column("researches", "graph_trail")
    op.drop_column("researches", "graph_state")
