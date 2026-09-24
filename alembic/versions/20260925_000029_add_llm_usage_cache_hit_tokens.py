"""record cache-hit prompt tokens per LLM call

Revision ID: 20260925_000029
Revises: 20260924_000028
Create Date: 2026-09-25 00:00:00

llm_usage_logs now gets one row per LLM call (written by the provider's usage sink)
instead of one per successful finalization. DeepSeek bills cache-hit prompt tokens at a
fraction of the miss rate, so the row keeps them next to the prompt/completion counts.
A constant server default makes this a metadata-only change on Postgres 11+.
"""

import sqlalchemy as sa
from alembic import op


revision = "20260925_000029"
down_revision = "20260924_000028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "llm_usage_logs",
        sa.Column("cache_hit_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("llm_usage_logs", "cache_hit_tokens")
