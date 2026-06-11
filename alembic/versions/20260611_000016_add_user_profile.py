"""add name and avatar_url to users (OAuth profile)

Revision ID: 20260611_000016
Revises: 20260610_000015
Create Date: 2026-06-11 00:00:00

Adds optional profile fields populated from the OAuth provider (e.g. Google name
and avatar picture) so the UI can show the user's name and avatar.
"""

import sqlalchemy as sa
from alembic import op


revision = "20260611_000016"
down_revision = "20260610_000015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("name", sa.String(length=200), nullable=True))
    op.add_column("users", sa.Column("avatar_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("users", "avatar_url")
    op.drop_column("users", "name")
