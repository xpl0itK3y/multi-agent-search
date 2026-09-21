"""research owner FK with cascade delete and status CHECK constraints

Revision ID: 20260917_000026
Revises: 20260909_000024
Create Date: 2026-09-17 01:00:00

DATA-LIFECYCLE: researches.user_id becomes a real FK so deleting a user cascades
their researches (and through existing FKs: tasks, results, jobs — revoking every
public share token they issued). Status CHECK constraints (NOT VALID, then
validated) guard new writes against out-of-enum statuses.
"""

from alembic import op

revision = "20260917_000026"
down_revision = "20260917_000025"
branch_labels = None
depends_on = None

STATUS_CHECKS = [
    (
        "researches",
        "ck_researches_status",
        "('queued','clarifying','plan_review','processing','analyzing',"
        "'completed','failed','cancelled')",
    ),
    ("search_tasks", "ck_search_tasks_status", "('pending','running','completed','failed')"),
    (
        "research_finalize_jobs",
        "ck_research_finalize_jobs_status",
        "('pending','running','completed','failed','dead_letter')",
    ),
    (
        "search_task_jobs",
        "ck_search_task_jobs_status",
        "('pending','running','completed','failed','dead_letter')",
    ),
]

FK_NAME = "fk_researches_user_id_users"


def upgrade() -> None:
    # Orphaned owners can't satisfy the FK; legacy ownerless rows stay NULL.
    op.execute(
        "UPDATE researches SET user_id = NULL "
        "WHERE user_id IS NOT NULL AND user_id NOT IN (SELECT id FROM users)"
    )
    # ADD CONSTRAINT takes a lock that blocks writes for its full scan on live
    # tables; NOT VALID + VALIDATE keeps the write-blocking window minimal.
    with op.get_context().autocommit_block():
        op.execute(
            f"ALTER TABLE researches ADD CONSTRAINT {FK_NAME} "
            "FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE NOT VALID"
        )
    op.execute(f"ALTER TABLE researches VALIDATE CONSTRAINT {FK_NAME}")

    for table, name, values in STATUS_CHECKS:
        with op.get_context().autocommit_block():
            op.execute(
                f"ALTER TABLE {table} ADD CONSTRAINT {name} "
                f"CHECK (status IN {values}) NOT VALID"
            )
        op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}")


def downgrade() -> None:
    for table, name, _values in STATUS_CHECKS:
        with op.get_context().autocommit_block():
            op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE researches DROP CONSTRAINT IF EXISTS {FK_NAME}")
