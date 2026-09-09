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


_INDEXES = (
    ("ix_researches_status", "researches", "status"),
    ("ix_search_tasks_status", "search_tasks", "status"),
    ("ix_search_task_jobs_status", "search_task_jobs", "status"),
    ("ix_research_finalize_jobs_status", "research_finalize_jobs", "status"),
    ("ix_worker_heartbeats_status", "worker_heartbeats", "status"),
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for name, table, column in _INDEXES:
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                f"ON {table} ({column})"
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _, _ in reversed(_INDEXES):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
