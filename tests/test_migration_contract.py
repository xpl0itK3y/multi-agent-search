"""AUD-037: guard against ORM<->migration drift. With the DB migrated to head, autogenerate
must detect zero changes — i.e. the declarative models exactly describe the migrated schema
(including the AUD-005 partial-unique index and the AUD-010 status indexes).

Postgres-only; runs against the shared throwaway DB migrated to head in conftest —
never the developer's working database.
"""
import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext

from src.db import Base

pytestmark = pytest.mark.postgres


def test_orm_models_match_migrated_schema(_postgres_test_db):
    engine, _session_factory = _postgres_test_db
    with engine.connect() as conn:
        ctx = MigrationContext.configure(
            conn,
            opts={"compare_type": True, "compare_server_default": True},
        )
        diff = compare_metadata(ctx, Base.metadata)
    assert diff == [], f"ORM models drifted from migrations: {diff}"
