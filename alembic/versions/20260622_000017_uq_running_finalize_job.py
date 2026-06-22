"""partial-unique index: at most one RUNNING finalize job per research

Revision ID: 20260622_000017
Revises: 20260611_000016
Create Date: 2026-06-22 00:00:00

DB-level backstop for AUD-005 (the application already prevents duplicate finalize
enqueue via the atomic try_begin_finalization CAS). This guarantees that two workers
can never finalize the same research at once: a second pending->running claim violates
the index.

Scoped to status='running' (not pending) so a transient/leftover pending job — e.g. a
manual requeue creating a fresh row beside the active run — can coexist, matching real
lifecycle usage and the existing tests.

If upgrade fails with a unique violation, legacy duplicates exist; collapse them first:
  UPDATE research_finalize_jobs SET status='dead_letter'
   WHERE id IN (SELECT id FROM (
     SELECT id, row_number() OVER (PARTITION BY research_id ORDER BY updated_at DESC) rn
       FROM research_finalize_jobs WHERE status='running') t WHERE rn > 1);
"""
import sqlalchemy as sa
from alembic import op

revision = "20260622_000017"
down_revision = "20260611_000016"
branch_labels = None
depends_on = None

_INDEX = "uq_running_finalize_job_per_research"


def upgrade() -> None:
    op.create_index(
        _INDEX,
        "research_finalize_jobs",
        ["research_id"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="research_finalize_jobs")
