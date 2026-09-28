"""add users.email_verified_at and auth_action_tokens (password reset, email verification)

Revision ID: 20260925_000033
Revises: 20260925_000032
Create Date: 2026-09-25 21:00:00

Sign-up verified no email, so a local account proved nothing about its address: whoever
registered someone's address first blocked that person's Google sign-in (409
oauth_conflict), and Google accounts created before 20260904_000019 (no google_subject, a
random password) could not sign in at all. users.email_verified_at records that the
address was proven, and auth_action_tokens holds the one-time links that prove it (email
verification) or recover the account through it (password reset).

A token is stored only as the sha256 hex of the random value sent in the link, and it is
valid only while the account's email still equals the address it was sent to.

Backfill: accounts linked to Google (Google verified the address) and accounts the
operator provisioned with scripts/create_admin.py count as verified. Every other local
account stays unverified until its owner opens a verification or reset link, or signs in
with Google (which then removes the password it was registered with). The users table is
small (one row per account), so the backfill is a single UPDATE; the new column has no
default, so adding it is a metadata-only change. The new table's indexes are built with
it, empty, so no CONCURRENTLY build is needed.
"""

import sqlalchemy as sa
from alembic import op


revision = "20260925_000033"
down_revision = "20260925_000032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("email_verified_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(
        "UPDATE users SET email_verified_at = now() "
        "WHERE email_verified_at IS NULL AND (google_subject IS NOT NULL OR admin_provisioned_at IS NOT NULL)"
    )

    op.create_table(
        "auth_action_tokens",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("purpose", sa.String(length=32), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("email", sa.String(length=200), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("requested_ip", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "purpose IN ('password_reset','email_verification')",
            name="ck_auth_action_tokens_purpose",
        ),
    )
    op.create_index("ix_auth_action_tokens_token_hash", "auth_action_tokens", ["token_hash"], unique=True)
    op.create_index("ix_auth_action_tokens_user_purpose", "auth_action_tokens", ["user_id", "purpose"])
    op.create_index("ix_auth_action_tokens_expires_at", "auth_action_tokens", ["expires_at"])


def downgrade() -> None:
    op.drop_table("auth_action_tokens")
    op.drop_column("users", "email_verified_at")
