"""add covering and partial indexes for job queue maintenance

Revision ID: 20260909_000022
Revises: 20260904_000021
Create Date: 2026-09-09 00:00:00
"""

from alembic import op


revision = "20260909_000022"
down_revision = "20260904_000021"
branch_labels = None
depends_on = None


_INDEXES = (
    (
        "ix_search_task_jobs_pending",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_search_task_jobs_pending "
        "ON search_task_jobs (created_at) WHERE status = 'pending'",
    ),
    (
        "ix_research_finalize_jobs_pending",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_research_finalize_jobs_pending "
        "ON research_finalize_jobs (created_at) WHERE status = 'pending'",
    ),
    (
        "ix_search_task_jobs_status_updated",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_search_task_jobs_status_updated "
        "ON search_task_jobs (status, updated_at)",
    ),
    (
        "ix_research_finalize_jobs_status_updated",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_research_finalize_jobs_status_updated "
        "ON research_finalize_jobs (status, updated_at)",
    ),
    (
        "ix_search_task_jobs_status_created",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_search_task_jobs_status_created "
        "ON search_task_jobs (status, created_at)",
    ),
    (
        "ix_research_finalize_jobs_status_created",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_research_finalize_jobs_status_created "
        "ON research_finalize_jobs (status, created_at)",
    ),
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for _, statement in _INDEXES:
            op.execute(statement)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _ in reversed(_INDEXES):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
