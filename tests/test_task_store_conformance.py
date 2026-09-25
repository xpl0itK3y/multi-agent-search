"""TEST-STORE-CONFORMANCE: one behavioral suite over BOTH TaskStore backends.

Every test here runs against InMemoryTaskStore and (when Postgres is reachable)
SQLAlchemyTaskStore. The premise of the 500+ non-postgres tests is that the two
implementations behave identically; this suite is what keeps that true — a
method that drifts fails on the postgres leg instead of in production.

The postgres leg shares the conftest throwaway database (migrated to head),
so it never depends on a developer's working database.
"""
import ast
import inspect
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import event, select, update

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.config import settings
from src.db.models import (
    AdminAuditLogORM,
    LLMUsageLogORM,
    ResearchFinalizeJobORM,
    ResearchORM,
    SearchCacheORM,
    SearchTaskJobORM,
    UserEventORM,
    UserORM,
    WorkerHeartbeatORM,
)
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.repositories.protocols import STALE_FINALIZE_CLOSED_ERROR, TaskStore
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore
from tests.postgres_helpers import truncate_runtime_tables


@pytest.fixture(
    params=[
        "memory",
        pytest.param("postgres", marks=pytest.mark.postgres),
    ]
)
def store(request):
    if request.param == "memory":
        return InMemoryTaskStore()
    # Resolved lazily: requesting the DB fixture in the signature made its "Postgres
    # unreachable" skip hit the memory leg too, so no-DB runs executed neither leg.
    engine, session_factory = request.getfixturevalue("_postgres_test_db")
    truncate_runtime_tables(session_factory)  # fresh runtime tables per test
    return SQLAlchemyTaskStore(session_factory)


def _request(prompt="conformance topic", depth=SearchDepth.EASY):
    return ResearchRequest(prompt=prompt, depth=depth)


def _ensure_user(store, user_id):
    """The owner FK (DATA-LIFECYCLE) requires a real user row on the postgres leg.
    Delete-first keeps re-runs idempotent (users survive runtime truncation)."""
    store.delete_user(user_id)
    return store.create_user(user_id, f"{user_id}@example.com", None)


def _research(store, user_id=None, prompt="conformance topic"):
    if user_id is not None:
        _ensure_user(store, user_id)
    return store.add_research(_request(prompt), task_ids=[], user_id=user_id)


def _task(store, research_id=None, task_id=None):
    return store.add_task(
        {
            "id": task_id or f"task-{uuid.uuid4().hex[:8]}",
            "research_id": research_id,
            "description": "search",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )


def _user(store, tag=None):
    tag = tag or uuid.uuid4().hex[:8]
    store.delete_user(f"user-{tag}")  # users survive runtime truncation between runs
    return store.create_user(f"user-{tag}", f"user-{tag}@example.com", None)


_SQL_ROWS = {
    "research": (ResearchORM, ResearchORM.id),
    "finalize_job": (ResearchFinalizeJobORM, ResearchFinalizeJobORM.id),
    "search_job": (SearchTaskJobORM, SearchTaskJobORM.id),
    "worker": (WorkerHeartbeatORM, WorkerHeartbeatORM.worker_name),
    "cache": (SearchCacheORM, SearchCacheORM.cache_key),
    "audit": (AdminAuditLogORM, AdminAuditLogORM.id),
    "event": (UserEventORM, UserEventORM.id),
    "user": (UserORM, UserORM.id),
}


def _backdate(store, kind, row_id, **timestamps):
    """Set timestamp columns the stores stamp with now(), on either backend."""
    if not isinstance(store, InMemoryTaskStore):
        model, key = _SQL_ROWS[kind]
        with store.session_scope() as session:
            session.execute(update(model).where(key == row_id).values(**timestamps))
        return
    if kind == "user":  # users.last_seen_at lives in the telemetry side table
        store.user_telemetry.setdefault(row_id, {}).update(timestamps)
    elif kind == "cache":
        store.search_cache[row_id] = (timestamps["created_at"], store.search_cache[row_id][1])
    elif kind == "event":
        next(e for e in store.user_events if e["id"] == row_id).update(timestamps)
    else:
        rows = {
            "research": store.researches,
            "finalize_job": store.finalize_jobs,
            "search_job": store.search_jobs,
            "worker": store.worker_heartbeats,
            "audit": {item.id: item for item in store.admin_audit_logs},
        }[kind]
        for column, value in timestamps.items():
            setattr(rows[row_id], column, value)


# ── research lifecycle ────────────────────────────────────────────────────────


def test_research_roundtrip_and_status_updates(store):
    record = _research(store, user_id="u1")
    assert record.status == ResearchStatus.PROCESSING

    fetched = store.get_research(record.id)
    assert fetched is not None and fetched.prompt == "conformance topic"

    completed = store.update_research_status(record.id, ResearchStatus.COMPLETED, "final text")
    assert completed.status == ResearchStatus.COMPLETED
    assert completed.final_report == "final text"

    assert store.update_research_status("missing-id", ResearchStatus.FAILED) is None
    assert store.get_research("missing-id") is None


def test_final_report_clears_streamed_partials(store):
    record = _research(store)
    store.save_partial_report(record.id, "partial draft")
    store.save_partial_reasoning(record.id, "partial reasoning")
    assert store.get_research(record.id).partial_report == "partial draft"

    done = store.update_research_status(record.id, ResearchStatus.COMPLETED, "final")
    assert done.final_report == "final"
    assert done.partial_report is None
    assert done.partial_reasoning is None


def test_delete_research_cascades_tasks(store):
    record = _research(store)
    task = _task(store, research_id=record.id)
    store.set_research_task_ids(record.id, [task.id])

    assert store.delete_research(record.id) is True
    assert store.get_research(record.id) is None
    assert store.get_task(task.id) is None
    assert store.delete_research(record.id) is False


def test_list_researches_scopes_to_owner(store):
    mine = _research(store, user_id="owner-1", prompt="my topic")
    _research(store, user_id="owner-2", prompt="their topic")

    listed = store.list_researches(limit=10, user_id="owner-1")
    assert [item.id for item in listed] == [mine.id]


def test_thread_listing_groups_by_graph_thread_id(store):
    first = _research(store, prompt="thread one")
    second = _research(store, prompt="thread two")
    store.merge_research_graph_state(first.id, {"thread_id": "thread-x"})
    store.merge_research_graph_state(second.id, {"thread_id": "thread-x"})

    thread = store.list_thread_researches("thread-x")
    assert {item.id for item in thread} == {first.id, second.id}


def test_share_token_lookup_via_graph_state(store):
    record = _research(store)
    store.merge_research_graph_state(record.id, {"share_token": "tok-" + "x" * 40})

    found = store.get_research_by_share_token("tok-" + "x" * 40)
    assert found is not None and found.id == record.id
    assert store.get_research_by_share_token("unknown-token") is None


def test_merge_graph_state_patches_and_removes_keys(store):
    record = _research(store)
    store.merge_research_graph_state(record.id, {"a": 1, "b": 2})
    store.merge_research_graph_state(record.id, {"b": 3, "c": 4}, remove_keys=["a"])

    state = store.get_research(record.id).graph_state
    assert "a" not in state
    assert state["b"] == 3 and state["c"] == 4


def test_graph_state_list_append_keeps_other_keys_and_caps(store):
    record = _research(store)
    store.merge_research_graph_state(record.id, {"title": "kept"})

    assert store.append_research_graph_state_item(record.id, "messages", {"n": 1}) == [{"n": 1}]
    store.append_research_graph_state_item(record.id, "messages", {"n": 2}, max_items=2)
    capped = store.append_research_graph_state_item(record.id, "messages", {"n": 3}, max_items=2)

    assert capped == [{"n": 2}, {"n": 3}]
    state = store.get_research(record.id).graph_state
    assert state["messages"] == [{"n": 2}, {"n": 3}]
    assert state["title"] == "kept"
    assert store.append_research_graph_state_item("missing-research", "messages", {"n": 1}) is None


def test_reset_for_retry_is_status_guarded_and_drops_only_the_given_keys(store):
    record = _research(store)
    store.merge_research_graph_state(record.id, {"step": "verify", "red_team": {}, "title": "kept"})
    store.update_research_status(record.id, ResearchStatus.FAILED, "Research failed.")

    # Not in the expected (just-admitted) status: nothing changes.
    assert store.reset_research_for_retry(record.id, ResearchStatus.PROCESSING, ["step"]) is None
    assert store.get_research(record.id).final_report == "Research failed."

    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    reset = store.reset_research_for_retry(record.id, ResearchStatus.PROCESSING, ["step", "red_team"])

    assert reset is not None and reset.status == ResearchStatus.PROCESSING
    current = store.get_research(record.id)
    assert current.final_report is None
    assert current.graph_state == {"title": "kept"}
    assert store.reset_research_for_retry("missing-research", ResearchStatus.PROCESSING, []) is None


def test_graph_events_append_to_trail(store):
    record = _research(store)
    store.append_research_graph_event(record.id, {"step": "analyze", "detail": "one"})
    store.append_research_graph_event(record.id, {"step": "verify", "detail": "two"})

    trail = store.get_research(record.id).graph_trail
    assert [entry["detail"] for entry in trail] == ["one", "two"]


# ── admission control ─────────────────────────────────────────────────────────


def test_add_research_if_under_limit_admits_queues_and_rejects(store):
    _ensure_user(store, "limited")
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=5)

    admitted = store.add_research_if_under_limit(
        _request(), task_ids=[], user_id="limited", graph_state={},
        per_user_limit=1, global_limit=0, stale_before=stale_before,
    )
    assert admitted is not None and admitted.status == ResearchStatus.PROCESSING

    rejected = store.add_research_if_under_limit(
        _request(), task_ids=[], user_id="limited", graph_state={},
        per_user_limit=1, global_limit=0, stale_before=stale_before,
    )
    assert rejected is None


def test_add_research_if_under_limit_queues_at_global_cap(store):
    _research(store, user_id="someone")  # occupies the single global running slot
    _ensure_user(store, "another")
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=5)

    queued = store.add_research_if_under_limit(
        _request(), task_ids=[], user_id="another", graph_state={},
        per_user_limit=1, global_limit=1, stale_before=stale_before,
    )
    assert queued is not None and queued.status == ResearchStatus.QUEUED


def test_try_admit_research_requires_expected_status(store):
    record = _research(store, user_id="admit-user")
    store.update_research_status(record.id, ResearchStatus.PLAN_REVIEW)
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=5)

    wrong_state = store.try_admit_research(
        record.id, ResearchStatus.CLARIFYING, 1, 0, stale_before
    )
    assert wrong_state is False

    ok = store.try_admit_research(record.id, ResearchStatus.PLAN_REVIEW, 1, 0, stale_before)
    assert ok is True
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING


def test_try_claim_queued_research_is_atomic_once(store):
    _ensure_user(store, "claimer")
    record = store.add_research(_request(), task_ids=[], user_id="claimer")
    store.update_research_status(record.id, ResearchStatus.QUEUED)

    assert store.try_claim_queued_research(record.id) is True
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING
    assert store.try_claim_queued_research(record.id) is False


def test_try_begin_finalization_is_single_flight(store):
    # Flips a running (non-terminal) research INTO ANALYZING; exactly one winner.
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)

    assert store.try_begin_finalization(record.id) is True
    assert store.get_research(record.id).status == ResearchStatus.ANALYZING
    assert store.try_begin_finalization(record.id) is False


# ── tasks ─────────────────────────────────────────────────────────────────────


def test_task_crud_and_owner_scoping(store):
    user = _user(store)
    record = _research(store, user_id=user.id)
    task = _task(store, research_id=record.id)

    assert store.get_task(task.id).status == TaskStatus.PENDING
    updated = store.update_task(task.id, TaskUpdate(status=TaskStatus.COMPLETED, log="done"))
    assert updated.status == TaskStatus.COMPLETED

    assert [item.id for item in store.get_tasks_by_research(record.id)] == [task.id]
    # Owner scoping: a foreign user cannot see or mutate the task.
    assert store.get_task(task.id, user_id="intruder") is None
    assert store.update_task(task.id, TaskUpdate(status=TaskStatus.FAILED), user_id="intruder") is None
    assert [item.id for item in store.get_all_tasks(user_id="intruder")] == []
    assert store.get_task(task.id, user_id=user.id) is not None


# ── search jobs ───────────────────────────────────────────────────────────────


def test_search_job_claim_next_and_by_id(store):
    task = _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    assert job.status == SearchJobStatus.PENDING

    claimed = store.claim_next_search_task_job()
    assert claimed is not None and claimed.id == job.id
    assert claimed.status == SearchJobStatus.RUNNING

    assert store.claim_next_search_task_job() is None  # nothing pending left
    assert store.claim_search_task_job_by_id(job.id) is None  # not PENDING anymore


def test_search_job_failure_retries_then_dead_letters(store):
    task = _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value, max_attempts=2)

    # attempt_count grows on claim; a failure dead-letters once attempts ran out.
    store.claim_search_task_job_by_id(job.id)  # attempt 1
    retried = store.record_search_task_job_failure(job.id, "transient")
    assert retried.status == SearchJobStatus.PENDING
    assert retried.attempt_count == 1

    store.claim_search_task_job_by_id(job.id)  # attempt 2
    dead = store.record_search_task_job_failure(job.id, "broken again")
    assert dead.status == SearchJobStatus.DEAD_LETTER


def test_search_job_requeue_and_latest(store):
    task = _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.update_search_task_job(job.id, SearchJobStatus.DEAD_LETTER, error="boom")

    requeued = store.requeue_search_task_job(job.id)
    assert requeued.status == SearchJobStatus.PENDING

    latest = store.get_latest_search_task_job(task.id)
    assert latest is not None and latest.id == job.id
    assert store.get_search_task_job(job.id) is not None


def test_search_job_recovery_and_cleanup(store):
    task = _task(store)
    stale = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(stale.id)
    fresh = store.add_search_task_job(task.id, SearchDepth.EASY.value)

    recovered = store.recover_stale_search_task_jobs(
        datetime.now(timezone.utc) + timedelta(hours=1)  # everything is "stale"
    )
    assert stale.id in [job.id for job in recovered]
    assert store.get_search_task_job(stale.id).status == SearchJobStatus.PENDING

    store.update_search_task_job(stale.id, SearchJobStatus.COMPLETED)
    deleted = store.cleanup_old_search_task_jobs(datetime.now(timezone.utc) + timedelta(hours=1))
    assert stale.id in deleted
    assert fresh.id not in deleted
    assert store.get_search_task_job(stale.id) is None


def test_search_job_listings_by_status(store):
    task = _task(store)
    pending = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    running = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(running.id)
    dead = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.update_search_task_job(dead.id, SearchJobStatus.DEAD_LETTER)

    assert [job.id for job in store.get_pending_search_task_jobs()] == [pending.id]
    assert [job.id for job in store.get_running_search_task_jobs()] == [running.id]
    assert [job.id for job in store.get_dead_letter_search_task_jobs()] == [dead.id]


# ── finalize jobs ─────────────────────────────────────────────────────────────


def test_finalize_job_lease_fence_and_completion(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.ANALYZING)
    job = store.add_research_finalize_job(record.id)
    claimed = store.claim_next_research_finalize_job()
    assert claimed is not None and claimed.id == job.id

    epoch = claimed.lease_epoch
    assert store.renew_research_finalize_job_lease(job.id, epoch) is True
    assert store.renew_research_finalize_job_lease(job.id, epoch + 99) is False

    fenced = store.complete_research_finalize_job(
        job.id, record.id, lease_epoch=epoch + 1, report="late write"
    )
    assert fenced is None  # an evicted runner cannot complete

    completed = store.complete_research_finalize_job(
        job.id, record.id, lease_epoch=epoch, report="final"
    )
    assert completed is not None and completed.status == FinalizeJobStatus.COMPLETED
    research = store.get_research(record.id)
    assert research.status == ResearchStatus.COMPLETED
    assert research.final_report == "final"


def test_finalize_job_failure_retries_and_requeue(store):
    record = _research(store)
    job = store.add_research_finalize_job(record.id, max_attempts=2)

    # Failure is fenced: only the RUNNING lease-holder may record it.
    claimed = store.claim_research_finalize_job_by_id(job.id)  # attempt 1
    assert store.record_research_finalize_job_failure(
        job.id, "boom", lease_epoch=claimed.lease_epoch
    ).status == FinalizeJobStatus.PENDING
    assert store.record_research_finalize_job_failure(job.id, "stale", lease_epoch=claimed.lease_epoch) is None

    store.claim_research_finalize_job_by_id(job.id)  # attempt 2
    dead = store.record_research_finalize_job_failure(job.id, "boom again")
    assert dead.status == FinalizeJobStatus.DEAD_LETTER

    requeued = store.requeue_research_finalize_job(job.id)
    assert requeued.status == FinalizeJobStatus.PENDING


def test_requeue_only_accepts_stopped_jobs_and_fences_the_old_lease(store):
    record = _research(store)
    finalize_job = store.add_research_finalize_job(record.id, max_attempts=1)
    task = _task(store, record.id)
    search_job = store.add_search_task_job(task.id, SearchDepth.EASY.value, max_attempts=1)

    # PENDING and RUNNING jobs are live: a requeue would hand them to a second worker.
    assert store.requeue_research_finalize_job(finalize_job.id) is None
    assert store.requeue_search_task_job(search_job.id) is None
    old_epoch = store.claim_research_finalize_job_by_id(finalize_job.id).lease_epoch
    store.claim_search_task_job_by_id(search_job.id)
    assert store.requeue_research_finalize_job(finalize_job.id) is None
    assert store.requeue_search_task_job(search_job.id) is None
    assert store.get_research_finalize_job(finalize_job.id).status == FinalizeJobStatus.RUNNING

    store.record_research_finalize_job_failure(finalize_job.id, "boom", lease_epoch=old_epoch)
    store.record_search_task_job_failure(search_job.id, "boom")
    requeued = store.requeue_research_finalize_job(finalize_job.id)
    assert requeued.status == FinalizeJobStatus.PENDING
    assert requeued.lease_epoch == old_epoch + 1
    reclaimed = store.claim_research_finalize_job_by_id(finalize_job.id)
    assert store.renew_research_finalize_job_lease(finalize_job.id, old_epoch) is False
    assert store.renew_research_finalize_job_lease(finalize_job.id, reclaimed.lease_epoch) is True
    assert store.requeue_search_task_job(search_job.id).status == SearchJobStatus.PENDING

    store.update_search_task_job(search_job.id, SearchJobStatus.FAILED, error="task missing")
    assert store.requeue_search_task_job(search_job.id).status == SearchJobStatus.PENDING
    store.update_search_task_job(search_job.id, SearchJobStatus.COMPLETED)
    assert store.requeue_search_task_job(search_job.id) is None
    assert store.requeue_research_finalize_job("missing-job") is None


def test_finalize_job_stale_recovery_bumps_lease_epoch(store):
    record = _research(store)
    job = store.add_research_finalize_job(record.id)
    claimed = store.claim_research_finalize_job_by_id(job.id)
    assert claimed.lease_epoch == 0

    recovered = store.recover_stale_research_finalize_jobs(
        datetime.now(timezone.utc) + timedelta(hours=1)
    )
    assert job.id in [item.id for item in recovered]
    bumped = store.get_research_finalize_job(job.id)
    assert bumped.status == FinalizeJobStatus.PENDING
    assert bumped.lease_epoch == 1


def test_delete_research_tasks_removes_only_that_researchs_tasks_and_their_jobs(store):
    record = _research(store)
    other = _research(store)
    doomed, kept = _task(store, record.id), _task(store, record.id)
    foreign = _task(store, other.id)
    store.set_research_task_ids(record.id, [doomed.id, kept.id])
    job = store.add_search_task_job(doomed.id, SearchDepth.EASY.value)

    deleted = store.delete_research_tasks(record.id, [doomed.id, foreign.id, "missing-task"])

    assert deleted == 1
    assert [task.id for task in store.get_tasks_by_research(record.id)] == [kept.id]
    assert store.get_research(record.id).task_ids == [kept.id]
    assert store.get_task(doomed.id) is None and store.get_search_task_job(job.id) is None
    assert store.get_task(foreign.id) is not None
    assert store.delete_research_tasks(record.id, []) == 0
    assert store.delete_research_tasks("missing-research", [kept.id]) == 0


def test_transition_research_status_is_a_guarded_cas(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING, "partial")
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)

    assert store.transition_research_status(record.id, [ResearchStatus.ANALYZING], ResearchStatus.FAILED) is None
    # Written after the cutoff: somebody moved it again, the CAS must lose.
    assert store.transition_research_status(
        record.id, [ResearchStatus.PROCESSING], ResearchStatus.FAILED, "stalled", updated_before=cutoff
    ) is None
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING

    _backdate(store, "research", record.id, updated_at=cutoff - timedelta(minutes=5))
    moved = store.transition_research_status(
        record.id,
        [ResearchStatus.PROCESSING, ResearchStatus.ANALYZING],
        ResearchStatus.FAILED,
        "stalled",
        updated_before=cutoff,
    )

    assert moved.status == ResearchStatus.FAILED and moved.final_report == "stalled"
    current = store.get_research(record.id)
    assert current.status == ResearchStatus.FAILED and current.updated_at > cutoff
    kept = store.transition_research_status(record.id, [ResearchStatus.FAILED], ResearchStatus.ANALYZING)
    assert kept.status == ResearchStatus.ANALYZING and kept.final_report == "stalled"
    assert store.transition_research_status("missing", [ResearchStatus.PROCESSING], ResearchStatus.FAILED) is None


def test_list_stalled_research_ids_finds_only_researches_nothing_will_move(store):
    now = datetime.now(timezone.utc)

    def research(status=ResearchStatus.PROCESSING, **graph_state):
        record = _research(store)
        if graph_state:
            store.merge_research_graph_state(record.id, graph_state)
        if status != ResearchStatus.PROCESSING:
            store.update_research_status(record.id, status)
        return record.id

    def search_job(research_id, status):
        task = _task(store, research_id)
        job = store.add_search_task_job(task.id, SearchDepth.EASY.value)
        store.update_search_task_job(job.id, status)

    idle = research()
    lost_finalize = research(ResearchStatus.ANALYZING)
    _dead_letter_finalize_job(store, lost_finalize)
    searched = research()
    search_job(searched, SearchJobStatus.COMPLETED)
    searching = research()
    search_job(searching, SearchJobStatus.PENDING)
    analyzing = research(ResearchStatus.ANALYZING)
    running = store.add_research_finalize_job(analyzing)
    store.claim_research_finalize_job_by_id(running.id)
    decomposing = research(decompose_pending=True)
    failed = research(ResearchStatus.FAILED)
    in_review = research(ResearchStatus.PLAN_REVIEW)
    for minutes, research_id in enumerate(
        [idle, lost_finalize, searched, searching, analyzing, decomposing, failed, in_review]
    ):
        _backdate(store, "research", research_id, updated_at=now - timedelta(hours=2) + timedelta(minutes=minutes))
    fresh = research()

    cutoff = now - timedelta(minutes=10)
    assert store.list_stalled_research_ids(cutoff) == [idle, lost_finalize, searched]
    assert store.list_stalled_research_ids(cutoff, limit=2) == [idle, lost_finalize]
    # The fresh one is left out only by its age.
    assert store.list_stalled_research_ids(datetime.now(timezone.utc) + timedelta(minutes=1))[-1] == fresh


def _dead_letter_finalize_job(store, research_id):
    job = store.add_research_finalize_job(research_id, max_attempts=1)
    claimed = store.claim_research_finalize_job_by_id(job.id)
    store.record_research_finalize_job_failure(job.id, "boom", lease_epoch=claimed.lease_epoch)
    return store.get_research_finalize_job(job.id)


def test_requeue_failed_finalization_moves_job_and_research_together(store):
    record = _research(store)
    dead = _dead_letter_finalize_job(store, record.id)
    old_epoch = dead.lease_epoch  # the in-memory store hands out the live object
    store.update_research_status(record.id, ResearchStatus.FAILED, "analysis failed")

    requeued = store.requeue_failed_research_finalize_job(dead.id)

    assert requeued.id == dead.id and requeued.status == FinalizeJobStatus.PENDING
    assert requeued.attempt_count == 0 and requeued.error is None
    assert requeued.lease_epoch == old_epoch + 1
    assert store.get_research(record.id).status == ResearchStatus.ANALYZING
    # Now PENDING: a second (concurrent) requeue is refused.
    assert store.requeue_failed_research_finalize_job(dead.id) is None
    assert store.requeue_failed_research_finalize_job("missing-job") is None


@pytest.mark.parametrize(
    "status",
    [ResearchStatus.PROCESSING, ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.CANCELLED],
)
def test_requeue_failed_finalization_refuses_a_research_that_is_not_failed(store, status):
    record = _research(store)
    dead = _dead_letter_finalize_job(store, record.id)
    store.update_research_status(record.id, status, "state after a retry")

    assert store.requeue_failed_research_finalize_job(dead.id) is None
    assert store.get_research_finalize_job(dead.id).status == FinalizeJobStatus.DEAD_LETTER
    assert store.get_research(record.id).status == status


def test_requeue_failed_finalization_refuses_a_superseded_job(store):
    record = _research(store)
    superseded = _dead_letter_finalize_job(store, record.id)
    _backdate(store, "finalize_job", superseded.id, created_at=datetime.now(timezone.utc) - timedelta(minutes=5))
    newer = _dead_letter_finalize_job(store, record.id)
    store.update_research_status(record.id, ResearchStatus.FAILED, "analysis failed")

    assert store.requeue_failed_research_finalize_job(superseded.id) is None
    assert store.get_research(record.id).status == ResearchStatus.FAILED
    assert store.requeue_failed_research_finalize_job(newer.id).status == FinalizeJobStatus.PENDING


@pytest.mark.parametrize(
    "ended", [ResearchStatus.CANCELLED, ResearchStatus.COMPLETED, ResearchStatus.FAILED]
)
def test_finalize_stale_recovery_closes_the_job_of_an_ended_research(store, ended):
    live = _research(store)
    live_job = store.add_research_finalize_job(live.id)
    store.claim_research_finalize_job_by_id(live_job.id)
    done = _research(store)
    done_job = store.add_research_finalize_job(done.id)
    store.claim_research_finalize_job_by_id(done_job.id)
    store.update_research_status(done.id, ended, "ended while the job hung")

    recovered = {
        item.id: item
        for item in store.recover_stale_research_finalize_jobs(datetime.now(timezone.utc) + timedelta(hours=1))
    }

    assert recovered[live_job.id].status == FinalizeJobStatus.PENDING
    assert recovered[done_job.id].status == FinalizeJobStatus.COMPLETED
    closed = store.get_research_finalize_job(done_job.id)
    assert closed.status == FinalizeJobStatus.COMPLETED
    assert closed.error == STALE_FINALIZE_CLOSED_ERROR
    assert closed.lease_epoch == 1  # a runner still holding the old lease is fenced
    assert store.get_research(done.id).status == ended
    assert store.get_research(done.id).final_report == "ended while the job hung"
    assert store.claim_next_research_finalize_job().id == live_job.id


def test_finalize_job_latest_listings_and_cleanup(store):
    record = _research(store)
    job = store.add_research_finalize_job(record.id)
    assert store.get_latest_research_finalize_job(record.id).id == job.id

    store.update_research_finalize_job(job.id, FinalizeJobStatus.COMPLETED)
    assert [item.id for item in store.get_pending_research_finalize_jobs()] == []
    assert [item.id for item in store.get_running_research_finalize_jobs()] == []

    deleted = store.cleanup_old_research_finalize_jobs(
        datetime.now(timezone.utc) + timedelta(hours=1)
    )
    assert job.id in deleted
    assert store.get_research_finalize_job(job.id) is None


# ── users ─────────────────────────────────────────────────────────────────────


def test_user_lifecycle_lookups_and_deletion(store):
    user = _user(store)
    assert store.get_user_by_id(user.id) is not None
    assert store.get_user_by_email(user.email).id == user.id
    assert store.get_user_by_email(user.email.upper()).id == user.id  # case-insensitive

    updated = store.update_user_password(user.id, "new-hash")
    assert updated.token_version == user.token_version + 1

    store.update_user_profile(user.id, "Display Name", "https://avatar")
    profiled = store.get_user_by_id(user.id)
    assert profiled.name == "Display Name"

    owned = _research(store, user_id=user.id)
    assert store.delete_user(user.id) is True
    assert store.get_user_by_id(user.id) is None
    assert store.get_research(owned.id) is None  # cascade parity


def test_oauth_lookup_by_google_subject(store):
    tag = uuid.uuid4().hex[:8]
    user = store.create_user(f"g-{tag}", f"g-{tag}@example.com", None, google_subject=f"sub-{tag}")
    found = store.get_user_by_google_subject(f"sub-{tag}")
    assert found is not None and found.id == user.id
    assert store.get_user_by_google_subject("unknown-subject") is None


def test_admin_provisioning_stamp_is_written_with_the_password(store):
    """scripts/create_admin.py's stamp (admin_identity): set only when asked, in the same
    write that replaces the password and bumps token_version, and read back everywhere."""
    tag = uuid.uuid4().hex[:8]
    plain = store.create_user(f"p-{tag}", f"p-{tag}@example.com", "hash")
    assert plain.admin_provisioned_at is None
    assert store.update_user_password(plain.id, "hash-2").admin_provisioned_at is None

    stamped = store.update_user_password(plain.id, "hash-3", admin_provisioned=True)
    assert stamped.admin_provisioned_at is not None
    assert stamped.token_version == plain.token_version + 2
    assert store.get_user_by_email(plain.email).admin_provisioned_at == stamped.admin_provisioned_at
    # A later self-service password change keeps the operator's stamp.
    assert store.update_user_password(plain.id, "hash-4").admin_provisioned_at == stamped.admin_provisioned_at

    created = store.create_user(f"c-{tag}", f"c-{tag}@example.com", "hash", admin_provisioned=True)
    assert created.admin_provisioned_at is not None
    assert store.get_user_by_id(created.id).admin_provisioned_at == created.admin_provisioned_at
    linked = store.create_user(f"l-{tag}", f"l-{tag}@example.com", None, google_subject=f"sub-l-{tag}")
    assert store.get_user_by_google_subject(f"sub-l-{tag}").admin_provisioned_at is None
    assert linked.admin_provisioned_at is None


# ── heartbeats, queue metrics, cache ──────────────────────────────────────────


def test_worker_heartbeat_upsert_and_step_events(store):
    store.upsert_worker_heartbeat("conf-worker", processed_jobs=1, status="idle")
    beat = store.upsert_worker_heartbeat(
        "conf-worker",
        processed_jobs=2,
        status="busy",
        last_error="boom",
        graph_step_events=[
            {
                "step": "analyze",
                "worker": "conf-worker",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
    )
    assert beat.processed_jobs == 2

    stored = store.get_worker_heartbeat("conf-worker")
    assert stored.status == "busy" and stored.last_error == "boom"
    assert store.get_worker_heartbeat("never-seen") is None

    events = store.get_graph_step_events("conf-worker")
    assert events and events[0]["step"] == "analyze"
    store.compact_worker_graph_step_events()


def test_queue_metrics_counts_by_status(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.ANALYZING)
    store.add_research_finalize_job(record.id)
    task = _task(store)
    store.add_search_task_job(task.id, SearchDepth.EASY.value)

    metrics = store.get_queue_metrics()
    assert metrics.pending_finalize_jobs == 1
    assert metrics.pending_search_jobs == 1


def test_search_cache_put_get_and_cleanup(store):
    store.put_cached_search("conf-key", [{"url": "https://example.com"}])
    hit = store.get_cached_search("conf-key", max_age_seconds=3600)
    assert hit == [{"url": "https://example.com"}]
    assert store.get_cached_search("conf-miss", max_age_seconds=3600) is None

    deleted = store.cleanup_search_cache(datetime.now(timezone.utc) + timedelta(hours=1))
    assert deleted >= 1
    assert store.get_cached_search("conf-key", max_age_seconds=3600) is None


def test_search_cache_zero_max_age_is_always_expired(store):
    store.put_cached_search("conf-zero", [{"url": "https://example.com"}])
    assert store.get_cached_search("conf-zero", max_age_seconds=0) is None


def test_ping_is_alive(store):
    assert store.ping() is True


# ── user telemetry ────────────────────────────────────────────────────────────


def test_telemetry_writes_clip_ip_and_user_agent(store):
    """user_events.user_agent is VARCHAR(255) and every ip column VARCHAR(64): an odd
    header must be clipped, not raise a DataError that drops the row."""
    user = _user(store)
    long_ip, long_ua = "2001:db8:" + "f" * 100, "Mozilla/5.0 " + "x" * 400

    store.record_user_event("tab_focus", "ui", user_id=user.id, ip_address=long_ip, user_agent=long_ua)
    store.record_user_session(user.id, "clip-sess", ip_address=long_ip, user_agent=long_ua)
    store.touch_user_activity(user.id, ip_address=long_ip, user_agent=long_ua)

    event = store.get_admin_event_logs(user_id=user.id).events[0]
    assert (len(event.ip_address), len(event.user_agent)) == (64, 255)
    detail = store.get_admin_user_detail(user.id)
    assert len(detail.sessions[0]["ip_address"]) == 64
    assert len(detail.user.last_ip) == 64


def test_user_session_upsert_is_scoped_to_its_user(store):
    """session_id is client-chosen (and survived sign-out in the SPA): a second account
    reporting the same id gets its own row and never updates the first account's."""
    first, second = _user(store), _user(store)

    first_id = store.record_user_session(first.id, "shared-sid", ip_address="10.0.0.1", browser="Firefox")
    second_id = store.record_user_session(second.id, "shared-sid", ip_address="10.0.0.2", browser="Chrome")

    assert first_id != second_id
    first_sessions = store.get_admin_user_detail(first.id).sessions
    second_sessions = store.get_admin_user_detail(second.id).sessions
    assert [(s["ip_address"], s["browser"]) for s in first_sessions] == [("10.0.0.1", "Firefox")]
    assert [(s["ip_address"], s["browser"]) for s in second_sessions] == [("10.0.0.2", "Chrome")]


def test_user_session_repeat_bumps_the_same_row(store):
    user = _user(store)
    first_id = store.record_user_session(user.id, "repeat-sid", ip_address="10.0.0.1", browser="Firefox")
    before = store.get_admin_user_detail(user.id).sessions[0]["last_active_at"]

    again_id = store.record_user_session(user.id, "repeat-sid", ip_address="10.0.0.9", browser="Other")

    sessions = store.get_admin_user_detail(user.id).sessions
    assert again_id == first_id
    assert len(sessions) == 1
    assert sessions[0]["ip_address"] == "10.0.0.9"
    assert sessions[0]["browser"] == "Firefox"  # device details stay from the first report
    assert str(sessions[0]["last_active_at"]) >= str(before)


def test_user_activity_is_throttled_and_not_touched_by_events(store):
    user = _user(store)
    store.record_user_event("tab_focus", "ui", user_id=user.id)
    store.record_user_session(user.id, "touch-sess", ip_address="10.0.0.5")
    # Events and sessions leave users activity to the (throttled) request middleware.
    assert store.get_admin_user_detail(user.id).user.last_seen_at is None

    store.touch_user_activity(user.id, ip_address="10.0.0.1")
    store.touch_user_activity(user.id, ip_address="10.0.0.2")  # within the interval: skipped

    touched = store.get_admin_user_detail(user.id).user
    assert touched.last_seen_at is not None
    assert touched.last_ip == "10.0.0.1"


def _prompt_copy(store, name, research_id, prompt, user_id):
    store.record_user_event(
        name, "prompt", user_id=user_id, details={"research_id": research_id, "prompt": prompt}
    )


def test_deleting_a_research_removes_its_prompt_copies(store):
    doomed = _research(store, user_id="prompt-owner", prompt="sensitive research topic")
    kept = store.add_research(_request("kept research topic"), task_ids=[], user_id="prompt-owner")
    _prompt_copy(store, "research_prompt", doomed.id, "sensitive research topic", "prompt-owner")
    _prompt_copy(store, "chat_prompt", doomed.id, "sensitive follow-up", "prompt-owner")
    _prompt_copy(store, "chat_prompt", kept.id, "kept follow-up", "prompt-owner")
    store.record_user_event("tab_focus", "ui", user_id="prompt-owner", details={"research_id": doomed.id})

    assert store.delete_research(doomed.id) is True

    remaining = store.get_admin_event_logs(user_id="prompt-owner").events
    assert sorted((e.event_name, e.details.get("prompt")) for e in remaining) == [
        ("chat_prompt", "kept follow-up"),
        ("tab_focus", None),  # only the prompt copies are tied to the research
    ]
    chat_log = store.get_admin_prompts(prompt_type="chat", user_id="prompt-owner").prompts
    assert [item.prompt for item in chat_log] == ["kept follow-up"]


def test_research_retention_removes_its_prompt_copies(store):
    expired = _research(store, user_id="retention-owner", prompt="expired research topic")
    store.update_research_status(expired.id, ResearchStatus.COMPLETED, "done")
    _prompt_copy(store, "research_prompt", expired.id, "expired research topic", "retention-owner")
    _prompt_copy(store, "chat_prompt", expired.id, "expired follow-up", "retention-owner")

    deleted = store.cleanup_old_researches(datetime.now(timezone.utc) + timedelta(minutes=1))

    assert deleted == [expired.id]
    assert store.get_admin_event_logs(user_id="retention-owner").events == []


@contextmanager
def _statements(store):
    """(SQL, bind parameter count) of each statement the block runs; None on the memory leg."""
    if isinstance(store, InMemoryTaskStore):
        yield None
        return
    captured: list[tuple[str, int]] = []
    bind = store.session_factory.kw["bind"]

    def record(conn, cursor, statement, parameters, context, executemany):
        captured.append((statement.lstrip(), len(parameters or ())))

    event.listen(bind, "before_cursor_execute", record)
    try:
        yield captured
    finally:
        event.remove(bind, "before_cursor_execute", record)


def test_research_retention_deletes_in_bounded_batches(store, monkeypatch):
    """One IN list over every expired id failed past 65,535 bind parameters (psycopg binds
    one per value), so a large first sweep never deleted anything: batches instead."""
    monkeypatch.setattr(type(store), "_RETENTION_BATCH_SIZE", 2, raising=False)
    owner = _user(store)
    expired = []
    for n in range(5):
        research = store.add_research(_request(f"expired topic {n}"), task_ids=[], user_id=owner.id)
        store.update_research_status(research.id, ResearchStatus.COMPLETED, "done")
        _prompt_copy(store, "chat_prompt", research.id, f"expired follow-up {n}", owner.id)
        expired.append(research.id)
    active = store.add_research(_request("running topic"), task_ids=[], user_id=owner.id)
    _prompt_copy(store, "chat_prompt", active.id, "live follow-up", owner.id)

    with _statements(store) as statements:
        deleted = store.cleanup_old_researches(datetime.now(timezone.utc) + timedelta(minutes=1))

    assert sorted(deleted) == sorted(expired)
    assert store.get_research(active.id) is not None
    assert [e.details["prompt"] for e in store.get_admin_event_logs(user_id=owner.id).events] == ["live follow-up"]
    if statements is not None:
        assert len([sql for sql, _count in statements if sql.startswith("DELETE FROM researches")]) == 3
        prompt_deletes = [count for sql, count in statements if sql.startswith("DELETE FROM user_events")]
        assert max(prompt_deletes) <= 3 + 2  # the event names, the JSON key and one batch of ids


def test_telemetry_retention_deletes_rows_past_the_cutoff(store):
    user = _user(store)
    store.record_user_event("tab_focus", "ui", user_id=user.id)
    store.record_user_session(user.id, "retention-sess")
    store.record_admin_audit("admin@example.com", "delete_user", "user", target_id="gone")

    def sweep(older_than):
        return (
            store.cleanup_old_user_events(older_than),
            store.cleanup_old_user_sessions(older_than),
            store.cleanup_old_admin_audit_logs(older_than),
        )

    assert sweep(datetime.now(timezone.utc) - timedelta(days=1)) == (0, 0, 0)
    assert sweep(datetime.now(timezone.utc) + timedelta(minutes=1)) == (1, 1, 1)
    assert store.get_admin_event_logs(user_id=user.id).events == []
    assert store.get_admin_user_detail(user.id).sessions == []
    assert store.get_admin_audit_logs() == []


def test_telemetry_retention_deletes_across_batches(store):
    store._RETENTION_BATCH_SIZE = 2  # the SQL store deletes in batches of this size
    user = _user(store)
    for _ in range(5):
        store.record_user_event("tab_blur", "ui", user_id=user.id)

    assert store.cleanup_old_user_events(datetime.now(timezone.utc) + timedelta(minutes=1)) == 5
    assert store.get_admin_event_logs(user_id=user.id).total_count == 0


# ── retention, compaction, dead letters ───────────────────────────────────────


def test_cleanup_old_researches_takes_only_terminal_researches_past_the_cutoff(store):
    old = datetime.now(timezone.utc) - timedelta(days=2)
    expired = _research(store, prompt="expired topic")
    store.update_research_status(expired.id, ResearchStatus.COMPLETED, "done")
    running = _research(store, prompt="running topic")
    recent = _research(store, prompt="recent topic")
    store.update_research_status(recent.id, ResearchStatus.FAILED, "failed")
    _backdate(store, "research", expired.id, updated_at=old)
    _backdate(store, "research", running.id, updated_at=old)

    assert store.cleanup_old_researches(datetime.now(timezone.utc) - timedelta(days=1)) == [expired.id]
    assert store.get_research(expired.id) is None
    assert store.get_research(running.id) is not None and store.get_research(recent.id) is not None


def test_compact_graph_trails_trims_only_recently_active_researches(store, monkeypatch):
    two_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    monkeypatch.setattr(settings, "graph_trail_retention_seconds", 86400)
    active, idle = _research(store, prompt="active topic"), _research(store, prompt="idle topic")
    for record in (active, idle):
        store.append_research_graph_event(record.id, {"step": "search", "detail": "old", "timestamp": two_hours_ago})
        store.append_research_graph_event(record.id, {"step": "analyze", "detail": "new"})
    # The SQL sweep reads only rows updated within the retention window (a bounded set).
    _backdate(store, "research", idle.id, updated_at=datetime.now(timezone.utc) - timedelta(hours=3))
    monkeypatch.setattr(settings, "graph_trail_retention_seconds", 3600)

    assert store.compact_research_graph_trails() == [active.id]
    assert [event["detail"] for event in store.get_research(active.id).graph_trail] == ["new"]
    assert [event["detail"] for event in store.get_research(idle.id).graph_trail] == ["old", "new"]
    assert store.compact_research_graph_trails() == []  # nothing left to trim


def test_dead_letter_finalize_jobs_are_listed(store):
    record = _research(store)
    dead = store.add_research_finalize_job(record.id, max_attempts=1)
    claimed = store.claim_research_finalize_job_by_id(dead.id)
    store.record_research_finalize_job_failure(dead.id, "boom", lease_epoch=claimed.lease_epoch)
    store.add_research_finalize_job(record.id)  # still pending

    assert [job.id for job in store.get_dead_letter_research_finalize_jobs()] == [dead.id]


def test_finalize_job_update_with_a_lease_is_fenced(store):
    record = _research(store)
    job = store.add_research_finalize_job(record.id)
    epoch = store.claim_research_finalize_job_by_id(job.id).lease_epoch

    assert store.update_research_finalize_job(job.id, FinalizeJobStatus.FAILED, "stale", lease_epoch=epoch + 1) is None
    updated = store.update_research_finalize_job(job.id, FinalizeJobStatus.FAILED, "boom", lease_epoch=epoch)
    assert (updated.status, updated.error) == (FinalizeJobStatus.FAILED, "boom")
    # No longer RUNNING: a lease-holder's late write is refused too.
    assert store.update_research_finalize_job(job.id, FinalizeJobStatus.COMPLETED, lease_epoch=epoch) is None
    assert store.update_research_finalize_job("missing-job", FinalizeJobStatus.COMPLETED) is None


# ── admin overview & maintenance previews ─────────────────────────────────────


def test_admin_overview_counts_and_worker_liveness(store):
    _research(store, prompt="processing topic")
    analyzing = _research(store, prompt="analyzing topic")
    store.update_research_status(analyzing.id, ResearchStatus.ANALYZING)
    task = _task(store)
    store.add_search_task_job(task.id, SearchDepth.EASY.value)
    running = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(running.id)
    for status in (SearchJobStatus.DEAD_LETTER, SearchJobStatus.FAILED):
        store.update_search_task_job(store.add_search_task_job(task.id, SearchDepth.EASY.value).id, status)
    store.upsert_worker_heartbeat(
        "conf-alive", processed_jobs=3, status="busy", last_error="boom", extraction_metrics={"attempts": 4}
    )
    store.upsert_worker_heartbeat("conf-gone", processed_jobs=1, status="idle")
    _backdate(store, "worker", "conf-gone", last_seen_at=datetime.now(timezone.utc) - timedelta(minutes=5))

    overview = store.get_admin_overview()

    assert (overview.active_researches_count, overview.pending_tasks_count, overview.failed_tasks_count) == (1, 1, 2)
    workers = sorted(overview.workers, key=lambda worker: worker.worker_name)
    assert [(w.worker_name, w.status, w.processed_jobs, w.last_error, w.is_alive) for w in workers] == [
        ("conf-alive", "busy", 3, "boom", True),
        ("conf-gone", "idle", 1, None, False),
    ]
    assert [(w.extraction_metrics, w.graph_metrics, w.maintenance_summary) for w in workers] == [
        ({"attempts": 4}, {}, {}),
        ({}, {}, {}),
    ]
    assert overview.is_dev_mode == settings.auth_disabled


def _stale_running_job(store, kind):
    """A RUNNING job idle for 10 minutes, next to one just claimed. Each finalize job gets
    its own research: Postgres allows one RUNNING finalize job per research."""
    if kind == "finalize_job":
        stale, fresh = (store.add_research_finalize_job(_research(store).id) for _ in range(2))
        claim = store.claim_research_finalize_job_by_id
    else:
        task = _task(store)
        stale, fresh = (store.add_search_task_job(task.id, SearchDepth.EASY.value) for _ in range(2))
        claim = store.claim_search_task_job_by_id
    claim(stale.id)
    claim(fresh.id)
    _backdate(store, kind, stale.id, updated_at=datetime.now(timezone.utc) - timedelta(minutes=10))
    return stale.id


@pytest.mark.parametrize(
    ("action", "kind", "noun"),
    [
        ("recover_stale_finalize_jobs", "finalize_job", "finalize"),
        ("recover_stale_search_jobs", "search_job", "search"),
    ],
)
def test_preview_recover_counts_running_jobs_past_the_window(store, action, kind, noun):
    stale_id = _stale_running_job(store, kind)

    preview = store.preview_maintenance_action(action, {"stale_seconds": 300})

    assert (preview.action, preview.dry_run, preview.affected_count) == (action, True, 1)
    assert preview.sample_affected_ids == [stale_id]
    assert preview.summary.startswith(f"Would recover 1 stale {noun} jobs running before ")


def test_preview_cleanup_counts_old_completed_and_dead_jobs_and_cache(store):
    record, task = _research(store), _task(store)
    eight_days_ago = datetime.now(timezone.utc) - timedelta(days=8)
    old_finalize = []
    for status in (FinalizeJobStatus.COMPLETED, FinalizeJobStatus.DEAD_LETTER, FinalizeJobStatus.FAILED):
        job = store.add_research_finalize_job(record.id)
        store.update_research_finalize_job(job.id, status)
        _backdate(store, "finalize_job", job.id, updated_at=eight_days_ago)
        old_finalize.append(job.id)
    old_search = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.update_search_task_job(old_search.id, SearchJobStatus.COMPLETED)
    _backdate(store, "search_job", old_search.id, updated_at=eight_days_ago)
    recent_search = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.update_search_task_job(recent_search.id, SearchJobStatus.COMPLETED)
    store.put_cached_search("conf-old", [{"url": "https://example.com/old"}])
    store.put_cached_search("conf-new", [{"url": "https://example.com/new"}])
    _backdate(store, "cache", "conf-old", created_at=datetime.now(timezone.utc) - timedelta(days=4))

    jobs = store.preview_maintenance_action("cleanup_old_jobs", {"days": 7})
    cache = store.preview_maintenance_action("cleanup_search_cache", {"days": 3})

    assert jobs.affected_count == 3
    assert set(jobs.sample_affected_ids) == {old_finalize[0], old_finalize[1], old_search.id}
    assert jobs.summary == "Would delete 2 finalize and 1 search jobs older than 7 days"
    assert (cache.affected_count, cache.sample_affected_ids) == (1, [])
    assert cache.summary == "Would delete 1 cached search entries older than 3 days"


def test_preview_requeue_and_unknown_actions(store):
    requeue = store.preview_maintenance_action("requeue_finalize_job", {"target_id": "job-x"})
    assert (requeue.affected_count, requeue.sample_affected_ids, requeue.summary) == (
        1, ["job-x"], "Would requeue job job-x",
    )
    assert store.preview_maintenance_action("requeue_search_job").affected_count == 0

    unknown = store.preview_maintenance_action("drop_everything")
    assert (unknown.affected_count, unknown.summary) == (0, "Unknown maintenance action: drop_everything")


# ── admin audit & event logs ──────────────────────────────────────────────────


def test_admin_audit_logs_filter_and_page_newest_first(store):
    t0 = datetime.now(timezone.utc) - timedelta(hours=1)
    first = store.record_admin_audit("x@example.com", "delete_user", "user", target_id="u1", details={"n": 1})
    second = store.record_admin_audit("y@example.com", "cleanup_old_jobs", "maintenance", ip_address="10.0.0.1")
    third = store.record_admin_audit("x@example.com", "cleanup_old_jobs", "maintenance")
    for minutes, audit_id in enumerate((first, second, third)):
        _backdate(store, "audit", audit_id, created_at=t0 + timedelta(minutes=minutes))

    assert [item.id for item in store.get_admin_audit_logs()] == [third, second, first]
    assert [item.id for item in store.get_admin_audit_logs(action="cleanup_old_jobs")] == [third, second]
    assert [item.id for item in store.get_admin_audit_logs(actor_email="x@example.com")] == [third, first]
    assert [item.id for item in store.get_admin_audit_logs(limit=1, offset=1)] == [second]
    oldest = store.get_admin_audit_logs(offset=2)[0]
    assert (oldest.actor_email, oldest.action, oldest.target_type, oldest.target_id, oldest.details) == (
        "x@example.com", "delete_user", "user", "u1", {"n": 1},
    )
    assert store.get_admin_audit_logs(offset=1)[0].ip_address == "10.0.0.1"
    # Equal timestamps (one clock tick) are ordered by id, so pages never repeat a row.
    _backdate(store, "audit", first, created_at=t0 + timedelta(minutes=2))
    assert [item.id for item in store.get_admin_audit_logs(actor_email="x@example.com")] == sorted(
        [first, third], reverse=True
    )


def test_admin_event_logs_filter_page_and_join_the_email(store):
    user = _user(store)
    t0 = datetime.now(timezone.utc) - timedelta(hours=1)
    focus = store.record_user_event("tab_focus", "ui", user_id=user.id, session_id="s1", details={"path": "/"})
    start = store.record_user_event("session_start", "system", user_id=user.id, session_id="s1")
    anonymous = store.record_user_event("tab_blur", "ui")
    for minutes, event_id in enumerate((focus, start, anonymous)):
        _backdate(store, "event", event_id, created_at=t0 + timedelta(minutes=minutes))

    everything = store.get_admin_event_logs()
    assert [e.id for e in everything.events] == [anonymous, start, focus]
    assert (everything.total_count, everything.page, everything.page_size) == (3, 1, 50)
    ui = store.get_admin_event_logs(category="ui")
    assert ([e.id for e in ui.events], ui.total_count) == ([anonymous, focus], 2)
    assert [e.id for e in store.get_admin_event_logs(event_name="session_start").events] == [start]
    assert [e.id for e in store.get_admin_event_logs(user_id=user.id).events] == [start, focus]
    paged = store.get_admin_event_logs(limit=1, offset=1)
    assert ([e.id for e in paged.events], paged.total_count, paged.page, paged.page_size) == ([start], 3, 2, 1)
    oldest = everything.events[2]
    assert (oldest.user_id, oldest.user_email, oldest.session_id, oldest.event_category, oldest.details) == (
        user.id, user.email, "s1", "ui", {"path": "/"},
    )
    assert (everything.events[0].user_id, everything.events[0].user_email) == (None, None)
    _backdate(store, "event", focus, created_at=t0 + timedelta(minutes=1))  # a tie with `start`
    assert [e.id for e in store.get_admin_event_logs(user_id=user.id).events] == sorted([focus, start], reverse=True)


# ── admin users & telemetry summary ───────────────────────────────────────────


def test_users_list_online_filter_and_last_seen_order(store):
    stale, fresh, never = _user(store), _user(store), _user(store)
    store.touch_user_activity(stale.id, ip_address="10.0.0.1", device="mobile")
    store.touch_user_activity(fresh.id, ip_address="10.0.0.2")
    _backdate(store, "user", stale.id, last_seen_at=datetime.now(timezone.utc) - timedelta(minutes=10))
    store.touch_user_activity("no-such-user", ip_address="10.0.0.3")  # updates no row

    listing = store.get_admin_users_list()
    by_id = {u.id: u for u in listing.users}
    assert [u.id for u in listing.users] == [fresh.id, stale.id, never.id]  # never-seen last
    assert (listing.total_users, listing.online_users) == (3, 1)
    assert [(u.is_online, u.last_ip) for u in listing.users] == [(True, "10.0.0.2"), (False, "10.0.0.1"), (False, None)]
    assert (by_id[stale.id].last_device, by_id[never.id].last_seen_at) == ("mobile", None)
    online = store.get_admin_users_list(online_only=True)
    assert ([u.id for u in online.users], online.total_users) == ([fresh.id], 1)
    # created_at is the account's, not the time of the query.
    created = {u.id: u.created_at for u in listing.users}
    assert {u.id: u.created_at for u in store.get_admin_users_list().users} == created

    store.delete_user(fresh.id)
    assert store.get_admin_users_list().online_users == 0


def test_deleting_a_user_keeps_their_llm_usage_unattributed(store):
    user = _user(store)
    store.record_llm_usage(None, user.id, "deepseek-chat", 3, 2, 5, 0.01)

    assert store.delete_user(user.id) is True

    assert store.get_user_token_analytics(user.id)["total_tokens"] == 0  # the FK is SET NULL
    assert store.get_admin_token_analytics().total_tokens == 5


def _usage_owners(store) -> list[tuple[str | None, str | None]]:
    """(research_id, user_id) of every llm_usage_logs row, in a stable order."""
    if isinstance(store, InMemoryTaskStore):
        owners = [(u["research_id"], u["user_id"]) for u in store.llm_usage_logs]
    else:
        with store.session_scope() as session:
            owners = [tuple(row) for row in session.execute(select(LLMUsageLogORM.research_id, LLMUsageLogORM.user_id))]
    return sorted(owners, key=repr)


def test_llm_usage_of_an_owner_deleted_mid_call_is_kept_unattributed(store):
    """A call still running when its research or account is deleted was billed all the
    same: its row is kept with that owner NULL, as SET NULL leaves an earlier row."""
    research = _research(store, user_id="usage-owner")
    store.record_llm_usage(research.id, "usage-owner", "deepseek-chat", 3, 2, 5, 0.01)

    assert store.delete_research(research.id) is True
    store.record_llm_usage(research.id, "usage-owner", "deepseek-chat", 4, 1, 5, 0.02)  # ended after the delete

    assert _usage_owners(store) == [(None, "usage-owner"), (None, "usage-owner")]
    assert store.get_user_token_analytics("usage-owner")["total_tokens"] == 10

    assert store.delete_user("usage-owner") is True
    store.record_llm_usage(None, "usage-owner", "deepseek-chat", 1, 1, 2, 0.01)

    assert _usage_owners(store) == [(None, None)] * 3
    assert store.get_admin_token_analytics().total_tokens == 12


def test_telemetry_summary_counts_users_and_leaves_unknowns_out(store):
    seen_now, seen_days_ago, never = _user(store), _user(store), _user(store)
    store.touch_user_activity(seen_now.id)
    store.touch_user_activity(seen_days_ago.id)
    _backdate(store, "user", seen_days_ago.id, last_seen_at=datetime.now(timezone.utc) - timedelta(days=3))
    store.record_user_session(seen_now.id, "s1", browser="Firefox", os="Linux")
    store.record_user_session(seen_now.id, "s2", browser="Chrome", os="Linux", device_type="mobile")
    store.record_user_session(never.id, "s3")  # no browser/os reported
    store.add_research(_request("ten chars!"), task_ids=[], user_id=never.id)
    store.add_research(_request("twenty characters!!!", depth=SearchDepth.HARD), task_ids=[], user_id=never.id)
    for model in ("deepseek-chat", "deepseek-v4-pro", "deepseek-chat"):
        store.record_llm_usage(None, None, model, 10, 5, 15, 0.25)

    summary = store.get_admin_telemetry_summary()

    assert (summary.total_users, summary.total_researches) == (3, 2)
    assert (summary.total_tokens, summary.total_cost_usd) == (45, 0.75)
    # Per user (last_seen_at), not per session row.
    assert (summary.online_now, summary.dau, summary.wau, summary.mau) == (1, 1, 2, 2)
    assert (summary.online_users_now, summary.dau_today, summary.wau_7d, summary.mau_30d) == (1, 1, 2, 2)
    assert summary.by_os == [{"name": "Linux", "count": 2}]
    assert summary.by_browser == [{"name": "Chrome", "count": 1}, {"name": "Firefox", "count": 1}]
    assert summary.by_device == [{"name": "desktop", "count": 2}, {"name": "mobile", "count": 1}]
    assert summary.by_country == []
    assert (summary.os_breakdown, summary.browser_breakdown) == ({"Linux": 2}, {"Chrome": 1, "Firefox": 1})
    assert summary.device_breakdown == {"desktop": 2, "mobile": 1}
    assert summary.depth_distribution == {"easy": 1, "hard": 1}
    assert summary.popular_depths == [{"depth": "easy", "count": 1}, {"depth": "hard", "count": 1}]
    assert summary.popular_models == [{"model": "deepseek-chat", "count": 2}, {"model": "deepseek-v4-pro", "count": 1}]
    assert summary.avg_prompt_len == 15.0


# ── the suite covers the whole protocol ───────────────────────────────────────

# The modules whose `store` fixture runs every test on both backends.
CONFORMANCE_MODULES = ("test_task_store_conformance.py", "test_admin_store_conformance.py")


def _protocol_methods() -> list[str]:
    return sorted(name for name, value in vars(TaskStore).items() if callable(value) and not name.startswith("_"))


def _methods_called_on_the_store_fixture() -> set[str]:
    called: set[str] = set()
    for module in CONFORMANCE_MODULES:
        tree = ast.parse((Path(__file__).parent / module).read_text(encoding="utf-8"))
        called.update(
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "store"
        )
    return called


def test_every_protocol_method_runs_in_the_conformance_suite():
    """A TaskStore method added without a conformance test is exactly how the two stores
    drifted before (admin/telemetry methods, retry reset): fail until it has one."""
    assert [name for name in _protocol_methods() if name not in _methods_called_on_the_store_fixture()] == []


@pytest.mark.parametrize("implementation", [InMemoryTaskStore, SQLAlchemyTaskStore])
def test_both_stores_implement_the_protocol_signatures(implementation):
    def parameters(function):
        return [(p.name, p.kind, p.default) for p in inspect.signature(function).parameters.values()]

    mismatched = [
        name
        for name in _protocol_methods()
        if not callable(getattr(implementation, name, None))
        or parameters(getattr(implementation, name)) != parameters(getattr(TaskStore, name))
    ]
    assert mismatched == []
