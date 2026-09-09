"""store the detected query language on researches

Revision ID: 20260909_000023
Revises: 20260909_000022
Create Date: 2026-09-09 00:30:00
"""

import sqlalchemy as sa
from alembic import op


revision = "20260909_000023"
down_revision = "20260909_000022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "researches",
        sa.Column(
            "language",
            sa.String(length=8),
            server_default=sa.text("'unknown'"),
            nullable=False,
        ),
    )
    op.alter_column("researches", "language", server_default=None)


def downgrade() -> None:
    op.drop_column("researches", "language")
