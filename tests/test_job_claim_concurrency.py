"""AUD-037: regression guard for the core race-safety claim — claim_next_search_task_job uses
SELECT ... FOR UPDATE SKIP LOCKED, so concurrent workers never claim the same job twice.

Postgres-only (SKIP LOCKED is a real DB lock; the in-memory store doesn't model it). Run against
a throwaway DB — never the live stack.
"""
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.api.schemas import ResearchRequest, SearchDepth
from src.db import create_session_factory
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

pytestmark = pytest.mark.postgres

JOBS = 40
WORKERS = 8


def _drain(store, barrier, sink, lock):
    barrier.wait()  # release all workers at once so SKIP LOCKED is genuinely contended
    local = []
    while True:
        job = store.claim_next_search_task_job()
        if job is None:
            break
        local.append(job.id)
    with lock:
        sink.extend(local)


def test_concurrent_claim_never_double_claims():
    store = SQLAlchemyTaskStore(create_session_factory())
    rec = store.add_research(ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[])
    run = uuid.uuid4().hex[:8]  # unique per run so a non-fresh DB can't collide on task ids
    mine: set[str] = set()
    for i in range(JOBS):
        tid = f"concurrency-{run}-task-{i}"
        store.add_task(
            {"id": tid, "research_id": rec.id, "description": "d", "queries": ["q"], "status": "pending"}
        )
        mine.add(store.add_search_task_job(task_id=tid, depth="easy").id)

    barrier = threading.Barrier(WORKERS)
    lock = threading.Lock()
    claimed: list[str] = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for f in [pool.submit(_drain, store, barrier, claimed, lock) for _ in range(WORKERS)]:
            f.result()

    # The core invariant: no job is ever handed to two workers (robust to any PENDING jobs left
    # behind by earlier tests, which the workers may also drain).
    assert len(claimed) == len(set(claimed)), "a job was claimed by two workers — SKIP LOCKED is broken"
    assert mine <= set(claimed), "some of this test's jobs were never claimed"
