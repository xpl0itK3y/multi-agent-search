"""add users table and researches.user_id for auth scoping

Revision ID: 20260601_000014
Revises: 20260521_000013
Create Date: 2026-06-01 00:00:00

Adds:
  users        — email/password accounts for the auth layer
  researches.user_id — nullable owner reference so each user sees only their research
"""

import sqlalchemy as sa
from alembic import op


revision = "20260601_000014"
down_revision = "20260521_000013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("email", sa.String(length=200), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.add_column("researches", sa.Column("user_id", sa.String(length=36), nullable=True))
    op.create_index("ix_researches_user_id", "researches", ["user_id"])


def downgrade() -> None:
    op.drop_index("ix_researches_user_id", table_name="researches")
    op.drop_column("researches", "user_id")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
