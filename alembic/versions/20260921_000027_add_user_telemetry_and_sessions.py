"""add user_sessions and user_events tables and user telemetry columns

Revision ID: 20260921_000027
Revises: 20260917_000026
Create Date: 2026-09-21 11:00:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260921_000027"
down_revision = "20260917_000026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add telemetry columns to users table
    op.add_column("users", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("users", sa.Column("last_ip", sa.String(length=64), nullable=True))
    op.add_column("users", sa.Column("last_user_agent", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("last_device", sa.String(length=64), nullable=True))
    op.create_index("ix_users_last_seen_at", "users", ["last_seen_at"])

    # 2. Create user_sessions table
    op.create_table(
        "user_sessions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("device_type", sa.String(length=32), nullable=False, server_default="desktop"),
        sa.Column("browser", sa.String(length=64), nullable=True),
        sa.Column("os", sa.String(length=64), nullable=True),
        sa.Column("screen_res", sa.String(length=32), nullable=True),
        sa.Column("viewport", sa.String(length=32), nullable=True),
        sa.Column("language", sa.String(length=16), nullable=True),
        sa.Column("timezone", sa.String(length=64), nullable=True),
        sa.Column("country", sa.String(length=64), nullable=True),
        sa.Column("city", sa.String(length=64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_active_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"])
    op.create_index("ix_user_sessions_session_id", "user_sessions", ["session_id"])
    op.create_index("ix_user_sessions_started_at", "user_sessions", ["started_at"])
    op.create_index("ix_user_sessions_last_active", "user_sessions", ["last_active_at"])

    # 3. Create user_events table
    op.create_table(
        "user_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("user_id", sa.String(length=36), nullable=True),
        sa.Column("session_id", sa.String(length=64), nullable=True),
        sa.Column("event_name", sa.String(length=64), nullable=False),
        sa.Column("event_category", sa.String(length=32), nullable=False, server_default="general"),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_user_events_user_id", "user_events", ["user_id"])
    op.create_index("ix_user_events_event_name", "user_events", ["event_name"])
    op.create_index("ix_user_events_category", "user_events", ["event_category"])
    op.create_index("ix_user_events_created_at", "user_events", ["created_at"])
    op.create_index("ix_user_events_user_created", "user_events", ["user_id", "created_at"])


def downgrade() -> None:
    op.drop_table("user_events")
    op.drop_table("user_sessions")
    op.drop_index("ix_users_last_seen_at", table_name="users")
    op.drop_column("users", "last_device")
    op.drop_column("users", "last_user_agent")
    op.drop_column("users", "last_ip")
    op.drop_column("users", "last_seen_at")
