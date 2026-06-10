"""add search_cache table for TTL-cached search results

Revision ID: 20260610_000015
Revises: 20260601_000014
Create Date: 2026-06-10 00:00:00

Adds:
  search_cache — cached web-search results keyed by backend+query, shared across
                 workers so a repeated query doesn't re-hit the (paid) search API.
                 Freshness is enforced on read via a TTL against created_at.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260610_000015"
down_revision = "20260601_000014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "search_cache",
        sa.Column("cache_key", sa.String(length=64), primary_key=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_search_cache_created_at", "search_cache", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_search_cache_created_at", table_name="search_cache")
    op.drop_table("search_cache")
