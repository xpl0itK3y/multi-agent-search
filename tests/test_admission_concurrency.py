"""PostgreSQL regression tests for atomic research admission across API replicas."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import ResearchRequest, SearchDepth
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

pytestmark = pytest.mark.postgres


def test_postgres_concurrent_starts_respect_per_user_limit(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)
    workers = 10
    barrier = threading.Barrier(workers)
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=5)

    def admit(index):
        barrier.wait()
        return store.add_research_if_under_limit(
            ResearchRequest(prompt=f"postgres concurrent topic {index}", depth=SearchDepth.EASY),
            task_ids=[],
            user_id="same-user",
            graph_state={"decompose_pending": True},
            per_user_limit=1,
            global_limit=0,
            stale_before=stale_before,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        admitted = list(pool.map(admit, range(workers)))

    assert len([record for record in admitted if record is not None]) == 1
    assert len(store.list_researches(limit=20, user_id="same-user")) == 1
