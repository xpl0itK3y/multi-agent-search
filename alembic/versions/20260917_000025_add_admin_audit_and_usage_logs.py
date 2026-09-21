"""add llm_usage_logs and admin_audit_logs tables

Revision ID: 20260917_000025
Revises: 20260909_000024
Create Date: 2026-09-17 12:30:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260917_000025"
down_revision = "20260909_000024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "llm_usage_logs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("research_id", sa.String(length=36), nullable=True),
        sa.Column("user_id", sa.String(length=36), nullable=True),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("completion_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("total_tokens", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("estimated_cost_usd", sa.Float(), nullable=False, server_default=sa.text("0.0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["research_id"], ["researches.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_llm_usage_logs_created_at", "llm_usage_logs", ["created_at"])
    op.create_index("ix_llm_usage_logs_model", "llm_usage_logs", ["model"])
    op.create_index("ix_llm_usage_logs_research_id", "llm_usage_logs", ["research_id"])
    op.create_index("ix_llm_usage_logs_user_id", "llm_usage_logs", ["user_id"])
    op.create_index("ix_llm_usage_logs_created_model", "llm_usage_logs", ["created_at", "model"])
    op.create_index("ix_llm_usage_logs_research_created", "llm_usage_logs", ["research_id", "created_at"])

    op.create_table(
        "admin_audit_logs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("actor_email", sa.String(length=200), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("target_type", sa.String(length=64), nullable=False),
        sa.Column("target_id", sa.String(length=128), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_admin_audit_logs_actor_email", "admin_audit_logs", ["actor_email"])
    op.create_index("ix_admin_audit_logs_action", "admin_audit_logs", ["action"])
    op.create_index("ix_admin_audit_logs_created_at", "admin_audit_logs", ["created_at"])
    op.create_index("ix_admin_audit_logs_actor_created", "admin_audit_logs", ["actor_email", "created_at"])
    op.create_index("ix_admin_audit_logs_action_created", "admin_audit_logs", ["action", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_admin_audit_logs_action_created", table_name="admin_audit_logs")
    op.drop_index("ix_admin_audit_logs_actor_created", table_name="admin_audit_logs")
    op.drop_index("ix_admin_audit_logs_created_at", table_name="admin_audit_logs")
    op.drop_index("ix_admin_audit_logs_action", table_name="admin_audit_logs")
    op.drop_index("ix_admin_audit_logs_actor_email", table_name="admin_audit_logs")
    op.drop_table("admin_audit_logs")

    op.drop_index("ix_llm_usage_logs_research_created", table_name="llm_usage_logs")
    op.drop_index("ix_llm_usage_logs_created_model", table_name="llm_usage_logs")
    op.drop_index("ix_llm_usage_logs_user_id", table_name="llm_usage_logs")
    op.drop_index("ix_llm_usage_logs_research_id", table_name="llm_usage_logs")
    op.drop_index("ix_llm_usage_logs_model", table_name="llm_usage_logs")
    op.drop_index("ix_llm_usage_logs_created_at", table_name="llm_usage_logs")
    op.drop_table("llm_usage_logs")
