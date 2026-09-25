"""record which ADMIN_EMAILS accounts the operator provisioned

Revision ID: 20260925_000030
Revises: 20260925_000029
Create Date: 2026-09-25 12:00:00

Sign-up verifies no email, so ADMIN_EMAILS membership alone made whoever registered an
address first its admin. Admin rights now also need a verified identity: a linked Google
account (google_subject) or this column, which scripts/create_admin.py sets when it
creates the account or replaces its password (revoking every earlier session).

No backfill on purpose: stamping existing ADMIN_EMAILS accounts would legitimize any
account squatted before the address was listed. An existing password-only admin runs
scripts/create_admin.py once after upgrading. A nullable column without a default is a
metadata-only change.
"""

import sqlalchemy as sa
from alembic import op


revision = "20260925_000030"
down_revision = "20260925_000029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("admin_provisioned_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "admin_provisioned_at")
