"""Writers that raced another transaction on the same row (SQLAlchemyTaskStore).

Each test holds a row in a second transaction, lets the store method run into that row lock,
then commits: the method must see the committed row, not the one it read before blocking.
Before the fixes these cases lost a write or failed:

- stale search-job recovery read RUNNING rows, then wrote them back by id, so a job its runner
  completed meanwhile went back to PENDING and ran twice;
- the job cleanups picked ids, then deleted by id, so a job requeued meanwhile was deleted;
- the search cache and the worker heartbeat checked for the row, then inserted, so the second
  of two first writers failed on the primary key;
- a search runner that recovery took its job from kept writing the task (SEARCH-LEASE);
  its leased task write now waits for the recovery and is refused after it.

begin_finalization is checked here too: its CAS and job insert commit or roll back together.

Postgres-only: these are real row and index locks.
"""
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, text, update

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskUpdate
from src.db.models import ResearchFinalizeJobORM, SearchCacheORM, SearchTaskJobORM, WorkerHeartbeatORM
from src.domain import FinalizeJobStatus, SearchJobStatus
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

pytestmark = pytest.mark.postgres

PAST = datetime.now(timezone.utc) - timedelta(days=30)


def _wait_until_blocked(session_factory, timeout=10.0):
    """Wait until some backend of this database waits on a lock (the store call has reached
    the held row), so the commit that follows is guaranteed to come after its read."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with session_factory() as probe:
            waiting = probe.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            ).scalar_one()
        if waiting:
            return
        time.sleep(0.05)
    raise AssertionError("the store call never blocked on the held row")


def _run_while_holding(session_factory, hold, call):
    """Run call() in a thread while another transaction holds a row (hold(session) writes it,
    uncommitted); commit once call() is blocked on it. Returns call()'s result or raises its error."""
    holder = session_factory()
    outcome: dict = {}

    def target():
        try:
            outcome["value"] = call()
        except Exception as exc:  # re-raised in the test thread
            outcome["error"] = exc

    try:
        hold(holder)
        holder.flush()
        worker = threading.Thread(target=target)
        worker.start()
        _wait_until_blocked(session_factory)
        holder.commit()
        worker.join(timeout=20)
        assert not worker.is_alive(), "the store call did not finish after the commit"
    finally:
        holder.close()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


@pytest.fixture
def store(postgres_session_factory):
    return SQLAlchemyTaskStore(postgres_session_factory)


def _search_job(store):
    research = store.add_research(ResearchRequest(prompt="race research", depth=SearchDepth.EASY), task_ids=[])
    task_id = f"race-{uuid.uuid4().hex[:8]}"
    store.add_task({"id": task_id, "research_id": research.id, "description": "d", "queries": ["q"], "status": "pending"})
    store.set_research_task_ids(research.id, [task_id])
    return research, store.add_search_task_job(task_id, "easy")


def test_stale_recovery_keeps_a_search_job_its_runner_completed_meanwhile(store, postgres_session_factory):
    _, job = _search_job(store)
    assert store.claim_next_search_task_job().id == job.id
    with postgres_session_factory() as session:
        session.execute(update(SearchTaskJobORM).where(SearchTaskJobORM.id == job.id).values(updated_at=PAST))
        session.commit()

    def runner_completes(session):
        session.execute(
            update(SearchTaskJobORM)
            .where(SearchTaskJobORM.id == job.id)
            .values(status=SearchJobStatus.COMPLETED.value, updated_at=datetime.now(timezone.utc))
        )

    recovered = _run_while_holding(
        postgres_session_factory,
        runner_completes,
        lambda: store.recover_stale_search_task_jobs(datetime.now(timezone.utc) - timedelta(minutes=5)),
    )

    assert recovered == []
    assert store.get_search_task_job(job.id).status == SearchJobStatus.COMPLETED


def test_search_job_cleanup_keeps_a_job_requeued_meanwhile(store, postgres_session_factory):
    _, job = _search_job(store)
    with postgres_session_factory() as session:
        session.execute(
            update(SearchTaskJobORM)
            .where(SearchTaskJobORM.id == job.id)
            .values(status=SearchJobStatus.DEAD_LETTER.value, updated_at=PAST)
        )
        session.commit()

    def admin_requeues(session):
        session.execute(
            update(SearchTaskJobORM)
            .where(SearchTaskJobORM.id == job.id)
            .values(status=SearchJobStatus.PENDING.value, attempt_count=0, updated_at=datetime.now(timezone.utc))
        )

    deleted = _run_while_holding(
        postgres_session_factory,
        admin_requeues,
        lambda: store.cleanup_old_search_task_jobs(datetime.now(timezone.utc) - timedelta(days=1)),
    )

    assert deleted == []
    assert store.get_search_task_job(job.id).status == SearchJobStatus.PENDING


def test_finalize_job_cleanup_keeps_a_job_requeued_meanwhile(store, postgres_session_factory):
    research, _ = _search_job(store)
    job = store.add_research_finalize_job(research.id)
    with postgres_session_factory() as session:
        session.execute(
            update(ResearchFinalizeJobORM)
            .where(ResearchFinalizeJobORM.id == job.id)
            .values(status=FinalizeJobStatus.DEAD_LETTER.value, updated_at=PAST)
        )
        session.commit()

    def admin_requeues(session):
        session.execute(
            update(ResearchFinalizeJobORM)
            .where(ResearchFinalizeJobORM.id == job.id)
            .values(status=FinalizeJobStatus.PENDING.value, attempt_count=0, updated_at=datetime.now(timezone.utc))
        )

    deleted = _run_while_holding(
        postgres_session_factory,
        admin_requeues,
        lambda: store.cleanup_old_research_finalize_jobs(datetime.now(timezone.utc) - timedelta(days=1)),
    )

    assert deleted == []
    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.PENDING


def test_two_first_writers_of_one_search_cache_entry_both_succeed(store, postgres_session_factory):
    key = f"race-{uuid.uuid4().hex[:16]}"

    def other_worker_caches(session):
        session.add(SearchCacheORM(cache_key=key, payload=[{"from": "other"}], created_at=datetime.now(timezone.utc)))

    _run_while_holding(postgres_session_factory, other_worker_caches, lambda: store.put_cached_search(key, [{"from": "store"}]))

    assert store.get_cached_search(key, max_age_seconds=3600) == [{"from": "store"}]


def test_a_first_heartbeat_racing_another_does_not_fail(store, postgres_session_factory):
    name = f"race-worker-{uuid.uuid4().hex[:8]}"

    def other_beat(session):
        session.add(WorkerHeartbeatORM(worker_name=name, processed_jobs=1, status="idle"))

    heartbeat = _run_while_holding(
        postgres_session_factory, other_beat, lambda: store.upsert_worker_heartbeat(name, 5, "running")
    )

    assert (heartbeat.processed_jobs, heartbeat.status) == (5, "running")
    assert store.get_worker_heartbeat(name).processed_jobs == 5


def test_a_leased_task_write_behind_a_stale_recovery_is_refused(store, postgres_session_factory):
    _, job = _search_job(store)
    claimed = store.claim_search_task_job_by_id(job.id)

    def recovery_takes_the_job(session):
        session.execute(
            update(SearchTaskJobORM)
            .where(SearchTaskJobORM.id == job.id)
            .values(status=SearchJobStatus.PENDING.value, lease_epoch=SearchTaskJobORM.lease_epoch + 1)
        )

    written = _run_while_holding(
        postgres_session_factory,
        recovery_takes_the_job,
        lambda: store.update_task_under_search_lease(
            job.task_id, TaskUpdate(log="late results"), job.id, claimed.lease_epoch
        ),
    )

    assert written is None
    assert "late results" not in store.get_task(job.task_id).logs


def test_begin_finalization_is_all_or_nothing(store, postgres_session_factory):
    """The CAS and the job insert are one transaction: an insert that fails leaves the
    research as it was, never ANALYZING without a job."""
    research, _ = _search_job(store)
    store.update_research_status(research.id, ResearchStatus.PROCESSING)

    def fail_the_job_insert(session, flush_context, instances):
        if any(isinstance(row, ResearchFinalizeJobORM) for row in session.new):
            raise RuntimeError("connection dropped")

    event.listen(postgres_session_factory, "before_flush", fail_the_job_insert)
    try:
        with pytest.raises(RuntimeError, match="connection dropped"):
            store.begin_finalization(research.id)
    finally:
        event.remove(postgres_session_factory, "before_flush", fail_the_job_insert)

    assert store.get_research(research.id).status == ResearchStatus.PROCESSING
    assert store.get_latest_research_finalize_job(research.id) is None
    assert store.begin_finalization(research.id) is not None  # nothing was left half-done
