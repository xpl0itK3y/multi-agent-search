"""add a fencing epoch to search task job leases

Revision ID: 20260928_000034
Revises: 20260925_000033
Create Date: 2026-09-28 00:00:00

Search jobs get the lease fence finalize jobs have had since 20260904_000020: stale
recovery and requeues bump the epoch, and a runner's job and task writes land only
while the job is still RUNNING under the epoch it claimed. Before this, a runner that
recovery had taken the job from could still complete or fail it, schedule a retry,
overwrite the task's results and trigger finalization.

A constant server default makes this a metadata-only change on Postgres 11+.
"""

import sqlalchemy as sa
from alembic import op


revision = "20260928_000034"
down_revision = "20260925_000033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "search_task_jobs",
        sa.Column("lease_epoch", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("search_task_jobs", "lease_epoch")
