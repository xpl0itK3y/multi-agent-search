"""add a fencing epoch to research finalize job leases

Revision ID: 20260904_000020
Revises: 20260904_000019
Create Date: 2026-09-04 00:00:00
"""

import sqlalchemy as sa
from alembic import op


revision = "20260904_000020"
down_revision = "20260904_000019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "research_finalize_jobs",
        sa.Column(
            "lease_epoch",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("research_finalize_jobs", "lease_epoch")
