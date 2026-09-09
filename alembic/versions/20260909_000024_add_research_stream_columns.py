"""move streamed report content out of graph state

Revision ID: 20260909_000024
Revises: 20260909_000023
Create Date: 2026-09-09 01:00:00
"""

import sqlalchemy as sa
from alembic import op


revision = "20260909_000024"
down_revision = "20260909_000023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("researches", sa.Column("partial_report", sa.Text(), nullable=True))
    op.add_column("researches", sa.Column("partial_reasoning", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE researches
        SET partial_report = graph_state ->> 'partial_report',
            partial_reasoning = graph_state ->> 'partial_reasoning',
            graph_state = graph_state - 'partial_report' - 'partial_reasoning'
        WHERE graph_state ? 'partial_report' OR graph_state ? 'partial_reasoning'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE researches
        SET graph_state = graph_state || jsonb_strip_nulls(
            jsonb_build_object(
                'partial_report', partial_report,
                'partial_reasoning', partial_reasoning
            )
        )
        WHERE partial_report IS NOT NULL OR partial_reasoning IS NOT NULL
        """
    )
    op.drop_column("researches", "partial_reasoning")
    op.drop_column("researches", "partial_report")
