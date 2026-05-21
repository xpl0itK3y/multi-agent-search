"""add indexes on status columns for polling queries

Revision ID: 20260521_000013
Revises: 20260520_000012
Create Date: 2026-05-21 00:00:00

Workers and the API poll these tables by status on every tick. Without indexes
Postgres falls back to sequential scans, which becomes expensive as the tables grow.

Tables covered:
  researches              — polled by status to find active research jobs
  search_tasks            — polled by status to track task progress
  search_task_jobs        — polled by status (pending/running) by the job dispatcher
  research_finalize_jobs  — polled by status (pending/running) by the finalizer worker
  worker_heartbeats       — queried by status for health checks
"""

from alembic import op


revision = "20260521_000013"
down_revision = "20260520_000012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # CONCURRENTLY cannot run inside Alembic's implicit transaction; use standard CREATE INDEX.
    # On live production tables prefer running these manually with CONCURRENTLY to avoid locking.
    op.execute("CREATE INDEX IF NOT EXISTS ix_researches_status ON researches (status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_search_tasks_status ON search_tasks (status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_search_task_jobs_status ON search_task_jobs (status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_research_finalize_jobs_status ON research_finalize_jobs (status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_status ON worker_heartbeats (status)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_researches_status")
    op.execute("DROP INDEX IF EXISTS ix_search_tasks_status")
    op.execute("DROP INDEX IF EXISTS ix_search_task_jobs_status")
    op.execute("DROP INDEX IF EXISTS ix_research_finalize_jobs_status")
    op.execute("DROP INDEX IF EXISTS ix_worker_heartbeats_status")
