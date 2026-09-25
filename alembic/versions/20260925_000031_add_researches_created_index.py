"""index researches by (created_at, id) for the admin lists and exports

Revision ID: 20260925_000031
Revises: 20260925_000030
Create Date: 2026-09-25 18:00:00

The admin Tokens list and export page every research newest first (created_at DESC,
id DESC), and the streamed prompts export now reads each page from a keyset cursor on
created_at. With no index on created_at, every page was a scan and sort of the whole
table. An INVALID index left by a failed CONCURRENTLY build is dropped and built again.
"""

import sqlalchemy as sa
from alembic import context, op


revision = "20260925_000031"
down_revision = "20260925_000030"
branch_labels = None
depends_on = None


_INDEX = "ix_researches_created_id"


def _index_is_valid(name: str) -> bool | None:
    """pg_index.indisvalid of the index; None if it does not exist, or in offline (--sql)
    mode, where there is no database to ask."""
    if context.is_offline_mode():
        return None
    row = op.get_bind().execute(
        sa.text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"),
        {"name": name},
    ).first()
    return None if row is None else bool(row[0])


def upgrade() -> None:
    with op.get_context().autocommit_block():
        if _index_is_valid(_INDEX) is False:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
        op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} ON researches (created_at, id)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}")
