"""delete prompt copies whose research no longer exists

Revision ID: 20260925_000032
Revises: 20260925_000031
Create Date: 2026-09-25 19:00:00

research_prompt and chat_prompt rows copy prompt text into user_events, which has no FK
to researches. Since 000028's release they are deleted with their research, but copies of
researches deleted before it stayed, readable in the admin Prompt and Event logs until
user_events retention removed them (90 days by default, never with retention off). This
deletes them once, with any prompt row that names no research at all. Other event rows
are kept.

The pass walks the prompt rows in id order, 1,000 per transaction, so a large table is
never one long delete. The rows cannot be restored: downgrade does nothing.
"""

import sqlalchemy as sa
from alembic import context, op


revision = "20260925_000032"
down_revision = "20260925_000031"
branch_labels = None
depends_on = None


_BATCH_SIZE = 1000
_PROMPT_EVENTS = "event_name IN ('chat_prompt', 'research_prompt')"
_ORPHANED = "NOT EXISTS (SELECT 1 FROM researches r WHERE r.id = {row}.details ->> 'research_id')"

# One batch: the next prompt rows by id, of which the orphans are deleted; returns the
# batch's last id and size, so the loop can continue after it.
_DELETE_BATCH = sa.text(
    "WITH batch AS ("
    f"  SELECT id FROM user_events WHERE {_PROMPT_EVENTS} AND id > :after ORDER BY id LIMIT :batch_size"
    "), deleted AS ("
    "  DELETE FROM user_events e USING batch WHERE e.id = batch.id"
    f"  AND {_ORPHANED.format(row='e')} RETURNING e.id"
    ") "
    "SELECT (SELECT max(id) FROM batch), (SELECT count(*) FROM batch), (SELECT count(*) FROM deleted)"
)


def upgrade() -> None:
    if context.is_offline_mode():  # no loop in a SQL script: one statement
        op.execute(f"DELETE FROM user_events e WHERE {_PROMPT_EVENTS} AND {_ORPHANED.format(row='e')}")
        return
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        after = ""
        while True:
            last_id, batch_count, _deleted = bind.execute(
                _DELETE_BATCH, {"after": after, "batch_size": _BATCH_SIZE}
            ).one()
            if batch_count < _BATCH_SIZE:
                return
            after = last_id


def downgrade() -> None:
    pass
