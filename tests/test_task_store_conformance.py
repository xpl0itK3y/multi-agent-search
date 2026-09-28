"""TEST-STORE-CONFORMANCE: one behavioral suite over BOTH TaskStore backends.

Every test here runs against InMemoryTaskStore and (when Postgres is reachable)
SQLAlchemyTaskStore. The premise of the 500+ non-postgres tests is that the two
implementations behave identically; this suite is what keeps that true — a
method that drifts fails on the postgres leg instead of in production.

The postgres leg shares the conftest throwaway database (migrated to head),
so it never depends on a developer's working database.
"""
import ast
import hashlib
import inspect
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import event, select, update

from src.api.schemas import (
    AuthActionPurpose,
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
    AuthActionTokenORM,
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
    "token": (AuthActionTokenORM, AuthActionTokenORM.id),
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
    elif kind == "token":
        store.auth_action_tokens[row_id].update(timestamps)
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


def test_delete_research_cascades_its_jobs(store):
    record = _research(store)
    task = _task(store, research_id=record.id)
    store.set_research_task_ids(record.id, [task.id])
    search_job = store.add_search_task_job(task.id, "easy")
    finalize_job = store.add_research_finalize_job(record.id)
    other = _research(store, prompt="another topic")
    kept = store.add_research_finalize_job(other.id)

    assert store.delete_research(record.id) is True

    # The SQL cascades (search job via its task, finalize job via its research): nothing
    # of the deleted research stays listable or claimable.
    assert store.get_search_task_job(search_job.id) is None
    assert store.get_research_finalize_job(finalize_job.id) is None
    assert store.claim_next_search_task_job() is None
    assert store.get_research_finalize_job(kept.id) is not None


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


def _processing_research_with_a_queued_search(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    task = _task(store, record.id)
    store.add_search_task_job(task.id, SearchDepth.EASY.value)
    return record, task


def test_try_begin_finalization_can_require_every_search_settled(store):
    # The auto-finalize CAS: an admin requeue that lands after the caller found every search
    # settled leaves a PENDING task and job, and finalizing then would drain that search.
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    task = _task(store, record.id)
    store.update_task(task.id, TaskUpdate(status=TaskStatus.FAILED, log="Search job failed after all retries"))
    dead = _dead_letter_search_job(store, task.id)
    requeued = store.requeue_search_task_job_of_active_research(dead.id, "Search job manually requeued")
    _processing_research_with_a_queued_search(store)  # another research's search does not count

    assert store.try_begin_finalization(record.id, require_settled_searches=True) is False
    store.claim_search_task_job_by_id(requeued.id)
    store.update_task(task.id, TaskUpdate(status=TaskStatus.FAILED, log="Error: provider down"))
    # FAILED while its job still runs: the retry is not decided yet.
    assert store.try_begin_finalization(record.id, require_settled_searches=True) is False
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING

    assert store.record_search_task_job_failure(requeued.id, "provider down").status == SearchJobStatus.DEAD_LETTER
    assert store.try_begin_finalization(record.id, require_settled_searches=True) is True
    assert store.get_research(record.id).status == ResearchStatus.ANALYZING
    assert store.try_begin_finalization(record.id, require_settled_searches=True) is False
    assert store.try_begin_finalization("missing-research", require_settled_searches=True) is False

    # A COMPLETED task is settled whatever its job still does, as the service counts it.
    completed, completed_task = _processing_research_with_a_queued_search(store)
    store.update_task(completed_task.id, TaskUpdate(status=TaskStatus.COMPLETED, log="done"))
    assert store.try_begin_finalization(completed.id, require_settled_searches=True) is True
    # Without the flag the CAS does not look at the searches (stale finalize recovery, retry).
    unchecked, _ = _processing_research_with_a_queued_search(store)
    assert store.try_begin_finalization(unchecked.id) is True
    assert store.get_research(unchecked.id).status == ResearchStatus.ANALYZING


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


def test_task_results_read_back_alike_from_add_and_update(store):
    """The SQL add_task used to drop `result` (the in-memory one kept it), and an emptied
    result read back [] in memory but None on SQL: GET /v1/tasks/{id} differed too."""
    source = {"url": "https://real.example/page", "title": "Real title", "content": "Fetched page text"}

    def sources(task):
        return [(item["url"], item["title"], item["content"]) for item in task.result]

    seeded = store.add_task(
        {"id": "seeded", "description": "d", "queries": ["q"], "status": TaskStatus.COMPLETED, "result": [source]}
    )
    expected = [("https://real.example/page", "Real title", "Fetched page text")]
    assert sources(seeded) == sources(store.get_task("seeded")) == expected

    # No results read back as None, however the task got there.
    created_empty = store.add_task({"id": "created-empty", "description": "d", "queries": ["q"], "result": []})
    assert created_empty.result is None and store.get_task("created-empty").result is None
    assert _task(store).result is None
    emptied = store.update_task("seeded", TaskUpdate(status=TaskStatus.FAILED, result=[]))
    assert emptied.result is None and store.get_task("seeded").result is None
    refilled = store.update_task("seeded", TaskUpdate(result=[source]))
    assert sources(refilled) == sources(store.get_task("seeded")) == expected


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


def _dead_letter_search_job(store, task_id):
    job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    return store.record_search_task_job_failure(job.id, "boom")


def test_admin_search_requeue_resets_task_and_job_and_touches_the_research(store):
    record = _research(store)
    task = _task(store, record.id)
    store.update_task(task.id, TaskUpdate(status=TaskStatus.FAILED, log="Search job failed after all retries"))
    dead = _dead_letter_search_job(store, task.id)
    listed_before = datetime.now(timezone.utc) - timedelta(minutes=30)  # a stalled sweep's cutoff
    _backdate(store, "research", record.id, updated_at=listed_before - timedelta(minutes=30))

    requeued = store.requeue_search_task_job_of_active_research(dead.id, "Search job manually requeued")

    assert requeued.id == dead.id and requeued.status == SearchJobStatus.PENDING
    assert requeued.attempt_count == 0 and requeued.error is None
    assert store.get_search_task_job(dead.id).status == SearchJobStatus.PENDING
    reset = store.get_task(task.id)
    assert reset.status == TaskStatus.PENDING
    assert reset.logs == ["Search job failed after all retries", "Search job manually requeued"]
    # The sweep that listed the research before the requeue loses its CAS.
    assert store.transition_research_status(
        record.id, [ResearchStatus.PROCESSING], ResearchStatus.FAILED, "stalled", updated_before=listed_before
    ) is None
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING
    # Now PENDING: a second (concurrent) requeue is refused.
    assert store.requeue_search_task_job_of_active_research(dead.id, "again") is None
    assert store.requeue_search_task_job_of_active_research("missing-job", "again") is None
    assert store.get_task(task.id).logs[-1] == "Search job manually requeued"


@pytest.mark.parametrize(
    "status",
    [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED],
)
def test_admin_search_requeue_refuses_a_research_that_is_not_searching(store, status):
    record = _research(store)
    task = _task(store, record.id)
    store.update_task(task.id, TaskUpdate(status=TaskStatus.FAILED))
    dead = _dead_letter_search_job(store, task.id)
    store.update_research_status(record.id, status, "state after the job died")

    assert store.requeue_search_task_job_of_active_research(dead.id, "requeued") is None
    assert store.get_search_task_job(dead.id).status == SearchJobStatus.DEAD_LETTER
    assert store.get_task(task.id).status == TaskStatus.FAILED and store.get_task(task.id).logs == []
    assert store.get_research(record.id).status == status


def test_admin_search_requeue_refuses_a_superseded_job_and_takes_a_task_without_research(store):
    record = _research(store)
    task = _task(store, record.id)
    superseded = _dead_letter_search_job(store, task.id)
    _backdate(store, "search_job", superseded.id, created_at=datetime.now(timezone.utc) - timedelta(minutes=5))
    newer = _dead_letter_search_job(store, task.id)

    assert store.requeue_search_task_job_of_active_research(superseded.id, "requeued") is None
    assert store.get_search_task_job(superseded.id).status == SearchJobStatus.DEAD_LETTER
    assert store.requeue_search_task_job_of_active_research(newer.id, "requeued").status == SearchJobStatus.PENDING

    orphan = _task(store)  # no research: nothing to rewind
    orphan_job = _dead_letter_search_job(store, orphan.id)
    assert store.requeue_search_task_job_of_active_research(orphan_job.id, "requeued").status == SearchJobStatus.PENDING
    assert store.get_task(orphan.id).status == TaskStatus.PENDING and store.get_task(orphan.id).logs == ["requeued"]


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


def test_list_pending_decomposition_ids_finds_every_processing_research_with_the_marker(store):
    now = datetime.now(timezone.utc)

    def research(status=ResearchStatus.PROCESSING, **graph_state):
        record = _research(store)
        if graph_state:
            store.merge_research_graph_state(record.id, graph_state)
        if status != ResearchStatus.PROCESSING:
            store.update_research_status(record.id, status)
        return record.id

    # Any age: the oldest (the ones the stalled sweep leaves to recovery for good) first.
    ancient = research(decompose_pending=True, decompose_payload={"prompt": "p"})
    with_tasks = research(decompose_pending=True)
    store.set_research_task_ids(with_tasks, [_task(store, with_tasks).id])
    recent = research(decompose_pending=True, decompose_requested_at=now.isoformat())
    planned = research()
    queued = research(ResearchStatus.QUEUED, decompose_pending=True)
    failed = research(ResearchStatus.FAILED, decompose_pending=True)
    for age, research_id in [(timedelta(days=30), ancient), (timedelta(hours=2), with_tasks)]:
        _backdate(store, "research", research_id, updated_at=now - age)
    for research_id in (planned, queued, failed):
        _backdate(store, "research", research_id, updated_at=now - timedelta(days=60))

    assert store.list_pending_decomposition_ids() == [ancient, with_tasks, recent]
    assert store.list_pending_decomposition_ids(limit=2) == [ancient, with_tasks]
    store.merge_research_graph_state(ancient, remove_keys=["decompose_pending"])
    assert store.list_pending_decomposition_ids() == [with_tasks, recent]


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


def test_token_version_bump_touches_nothing_else(store):
    """Logout's revocation write (SEC2-2): token_version + 1 and no other column, so it
    cannot write back a stale password hash, profile or provisioning stamp."""
    tag = uuid.uuid4().hex[:8]
    local = store.create_user(f"b-{tag}", f"b-{tag}@example.com", "hash-1", admin_provisioned=True)
    store.update_user_profile(local.id, "Bumped", "https://avatar/b")
    before = store.get_user_by_id(local.id)

    bumped = store.bump_user_token_version(local.id)
    assert bumped.token_version == before.token_version + 1
    assert bumped.model_dump(exclude={"token_version"}) == before.model_dump(exclude={"token_version"})
    assert store.get_user_by_id(local.id) == bumped
    assert store.bump_user_token_version(local.id).token_version == before.token_version + 2

    google = store.create_user(f"bg-{tag}", f"bg-{tag}@example.com", None, google_subject=f"sub-b-{tag}")
    bumped_google = store.bump_user_token_version(google.id)
    assert (bumped_google.token_version, bumped_google.password_hash) == (google.token_version + 1, None)
    assert bumped_google.google_subject == f"sub-b-{tag}"

    assert store.bump_user_token_version(f"missing-{tag}") is None


def test_a_password_write_or_a_delete_checked_on_a_stale_token_version_changes_nothing(store):
    """SEC-REC2-1: set-password and account deletion check the current password on a read
    and write after it. A sign-out everywhere, a reset or a Google link that removed the
    password committed in between bumped token_version: the late write matches nothing."""
    tag = uuid.uuid4().hex[:8]
    user = store.create_user(f"cas-{tag}", f"cas-{tag}@example.com", "hash-1")
    checked = user.token_version
    store.bump_user_token_version(user.id)  # sign-out everywhere, after the check
    _link(store, user, AuthActionPurpose.PASSWORD_RESET, f"reset-cas-{tag}")
    before = store.get_user_by_id(user.id)

    assert store.update_user_password(user.id, "hash-late", expected_token_version=checked) is None
    assert store.delete_user(user.id, expected_token_version=checked) is False
    assert store.get_user_by_id(user.id) == before  # same hash, same token_version
    # Nor did the refused write retire the reset link, as a password write does.
    assert store.get_live_auth_action_token(_hash(f"reset-cas-{tag}"), AuthActionPurpose.PASSWORD_RESET) is not None

    store.link_user_google_subject(user.id, f"sub-cas-{tag}", clear_password=True)
    linked = store.get_user_by_id(user.id)
    stale = before.token_version
    assert store.update_user_password(user.id, "hash-squat", expected_token_version=stale) is None
    assert store.delete_user(user.id, expected_token_version=stale) is False
    assert store.get_user_by_id(user.id) == linked and linked.password_hash is None

    # The version as it is now writes, and bumps it as ever.
    written = store.update_user_password(user.id, "hash-2", expected_token_version=linked.token_version)
    assert (written.password_hash, written.token_version) == ("hash-2", linked.token_version + 1)
    assert store.update_user_password(f"missing-{tag}", "hash-3", expected_token_version=0) is None
    assert store.delete_user(f"missing-{tag}", expected_token_version=0) is False
    assert store.delete_user(user.id, expected_token_version=written.token_version) is True
    assert store.get_user_by_id(user.id) is None


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


def test_only_an_identity_that_arrives_verified_is_created_verified(store):
    """users.email_verified_at (AUTH-RECOVERY): Google verified the address, the operator
    vouched for it; a plain sign-up proves nothing. As migration 000033's backfill."""
    tag = uuid.uuid4().hex[:8]
    plain = store.create_user(f"v-{tag}", f"v-{tag}@example.com", "hash")
    google = store.create_user(f"vg-{tag}", f"vg-{tag}@example.com", None, google_subject=f"sub-v-{tag}")
    provisioned = store.create_user(f"vp-{tag}", f"vp-{tag}@example.com", "hash", admin_provisioned=True)

    assert plain.email_verified_at is None
    assert google.email_verified_at is not None
    assert store.get_user_by_id(provisioned.id).email_verified_at == provisioned.email_verified_at is not None
    assert store.update_user_password(plain.id, "hash-2").email_verified_at is None  # self-service
    stamped = store.update_user_password(plain.id, "hash-3", admin_provisioned=True)
    assert stamped.email_verified_at is not None
    # A later provisioning keeps the first stamp.
    again = store.update_user_password(plain.id, "hash-4", admin_provisioned=True)
    assert again.email_verified_at == stamped.email_verified_at


def test_linking_google_keeps_or_clears_the_password_in_one_write(store):
    tag = uuid.uuid4().hex[:8]
    verified = store.create_user(f"lk-{tag}", f"lk-{tag}@example.com", "hash-v", admin_provisioned=True)
    unverified = store.create_user(f"lu-{tag}", f"lu-{tag}@example.com", "hash-u")

    kept = store.link_user_google_subject(verified.id, f"sub-lk-{tag}", clear_password=False)
    assert (kept.google_subject, kept.password_hash, kept.token_version) == (
        f"sub-lk-{tag}",
        "hash-v",
        verified.token_version,
    )
    assert kept.email_verified_at == verified.email_verified_at  # the first stamp stays

    cleared = store.link_user_google_subject(unverified.id, f"sub-lu-{tag}", clear_password=True)
    assert (cleared.password_hash, cleared.token_version) == (None, unverified.token_version + 1)
    assert cleared.email_verified_at is not None
    assert store.get_user_by_google_subject(f"sub-lu-{tag}") == cleared

    # Never re-linked, never a subject another account holds, never a missing account.
    assert store.link_user_google_subject(unverified.id, f"sub-other-{tag}", clear_password=True) is None
    third = store.create_user(f"lt-{tag}", f"lt-{tag}@example.com", "hash-t")
    assert store.link_user_google_subject(third.id, f"sub-lk-{tag}", clear_password=False) is None
    assert store.get_user_by_id(third.id) == third
    assert store.link_user_google_subject(f"missing-{tag}", f"sub-m-{tag}", clear_password=False) is None


# ── one-time links (auth_action_tokens) ───────────────────────────────────────


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _in(minutes: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def _link(store, user, purpose, token, *, email=None, expires_at=None, requested_ip=None):
    return store.create_auth_action_token(
        user.id, purpose, _hash(token), email or user.email, expires_at or _in(60), requested_ip=requested_ip
    )


def test_a_new_link_invalidates_the_older_unused_links_of_its_purpose(store):
    user = store.create_user(f"t-{uuid.uuid4().hex[:8]}", "Link.Owner@Example.com", "hash-1")
    first = _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-1", requested_ip="203.0.113.9" * 10)
    verification = _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, "verify-1")
    second = _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-2")

    assert first.purpose == AuthActionPurpose.PASSWORD_RESET and first.used_at is None
    assert first.email == "link.owner@example.com" and first.user_id == user.id
    assert len(first.requested_ip) == 64  # clipped to the column
    assert first.id != second.id != verification.id
    assert store.reset_password_with_token(_hash("reset-1"), "hash-2") is None  # superseded
    assert store.reset_password_with_token(_hash("reset-2"), "hash-2").password_hash == "hash-2"
    assert store.verify_email_with_token(_hash("verify-1"), user.id) is not None  # another purpose: kept

    assert store.create_auth_action_token(
        f"missing-{uuid.uuid4().hex[:8]}", AuthActionPurpose.PASSWORD_RESET, _hash("nobody"), "x@example.com", _in(60)
    ) is None


def test_a_reset_link_redeems_once_and_revokes_every_session(store):
    user = store.create_user(f"r-{uuid.uuid4().hex[:8]}", f"r-{uuid.uuid4().hex[:8]}@example.com", "hash-1")
    link = _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-once")
    _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-other")
    _backdate(store, "token", link.id, used_at=None)  # two live links (as two racing requests could leave)

    assert store.verify_email_with_token(_hash("reset-once"), user.id) is None  # the wrong purpose
    reset = store.reset_password_with_token(_hash("reset-once"), "hash-2")

    assert (reset.password_hash, reset.token_version) == ("hash-2", user.token_version + 1)
    assert reset.email_verified_at is not None  # the link proved the mailbox
    assert store.get_user_by_id(user.id) == reset
    assert store.reset_password_with_token(_hash("reset-once"), "hash-3") is None  # used
    assert store.reset_password_with_token(_hash("reset-other"), "hash-3") is None  # invalidated with it
    assert store.reset_password_with_token(_hash("never-issued"), "hash-3") is None
    assert store.get_user_by_id(user.id).password_hash == "hash-2"


def test_an_expired_link_or_one_sent_to_another_address_does_not_redeem(store):
    user = store.create_user(f"e-{uuid.uuid4().hex[:8]}", f"e-{uuid.uuid4().hex[:8]}@example.com", "hash-1")
    expired = _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, "verify-expired")
    _backdate(store, "token", expired.id, expires_at=_in(-1))
    assert store.verify_email_with_token(_hash("verify-expired"), user.id) is None
    assert store.get_live_auth_action_token(_hash("verify-expired"), AuthActionPurpose.EMAIL_VERIFICATION) is None

    _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-elsewhere", email="previous@example.com")
    assert store.get_live_auth_action_token(_hash("reset-elsewhere"), AuthActionPurpose.PASSWORD_RESET) is None
    assert store.reset_password_with_token(_hash("reset-elsewhere"), "hash-2") is None
    unchanged = store.get_user_by_id(user.id)
    assert (unchanged.password_hash, unchanged.token_version, unchanged.email_verified_at) == (
        "hash-1",
        user.token_version,
        None,
    )


def test_a_verification_link_marks_the_email_verified_once_and_keeps_the_first_stamp(store):
    user = store.create_user(f"m-{uuid.uuid4().hex[:8]}", f"m-{uuid.uuid4().hex[:8]}@example.com", "hash-1")
    _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, "verify-a")

    verified = store.verify_email_with_token(_hash("verify-a"), user.id)

    assert verified.email_verified_at is not None
    assert (verified.password_hash, verified.token_version) == ("hash-1", user.token_version)
    assert store.verify_email_with_token(_hash("verify-a"), user.id) is None
    _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, "verify-b")
    assert store.verify_email_with_token(_hash("verify-b"), user.id).email_verified_at == verified.email_verified_at


def test_a_verification_link_redeems_only_for_its_own_account(store):
    """SEC-REC-1: the verify route passes the signed-in account; another account's link is
    left unused (for its own account to redeem), and the peek tells it from a dead one."""
    tag = uuid.uuid4().hex[:8]
    owner = store.create_user(f"vo-{tag}", f"vo-{tag}@example.com", "hash-o")
    other = store.create_user(f"vx-{tag}", f"vx-{tag}@example.com", "hash-x")
    link = _link(store, owner, AuthActionPurpose.EMAIL_VERIFICATION, "verify-own")

    assert store.verify_email_with_token(_hash("verify-own"), other.id) is None
    assert store.verify_email_with_token(_hash("verify-own"), f"missing-{tag}") is None

    assert store.get_user_by_id(owner.id).email_verified_at is None
    assert store.get_user_by_id(other.id).email_verified_at is None
    live = store.get_live_auth_action_token(_hash("verify-own"), AuthActionPurpose.EMAIL_VERIFICATION)
    assert (live.id, live.user_id, live.purpose, live.email, live.used_at) == (
        link.id,
        owner.id,
        AuthActionPurpose.EMAIL_VERIFICATION,
        owner.email,
        None,
    )
    assert store.get_live_auth_action_token(_hash("verify-own"), AuthActionPurpose.PASSWORD_RESET) is None
    assert store.get_live_auth_action_token(_hash("never-issued"), AuthActionPurpose.EMAIL_VERIFICATION) is None

    verified = store.verify_email_with_token(_hash("verify-own"), owner.id)

    assert verified.id == owner.id and verified.email_verified_at is not None
    assert store.get_live_auth_action_token(_hash("verify-own"), AuthActionPurpose.EMAIL_VERIFICATION) is None
    assert store.get_user_by_id(other.id).email_verified_at is None


def test_a_link_redeems_once_under_concurrent_redeems(store):
    user = store.create_user(f"c-{uuid.uuid4().hex[:8]}", f"c-{uuid.uuid4().hex[:8]}@example.com", "hash-1")
    _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-race")
    results: list = []
    start = threading.Barrier(6)

    def redeem(index: int) -> None:
        start.wait()
        results.append(store.reset_password_with_token(_hash("reset-race"), f"hash-race-{index}"))

    threads = [threading.Thread(target=redeem, args=(index,)) for index in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [result for result in results if result is not None]
    assert len(results) == 6 and len(winners) == 1
    assert store.get_user_by_id(user.id).token_version == user.token_version + 1


def test_link_cleanup_deletes_expired_and_used_links_across_batches(store):
    store._RETENTION_BATCH_SIZE = 2  # the SQL store deletes in batches of this size
    tag = uuid.uuid4().hex[:8]
    users = [store.create_user(f"k-{index}-{tag}", f"k-{index}-{tag}@example.com", "h") for index in range(5)]
    live = _link(store, users[0], AuthActionPurpose.PASSWORD_RESET, "live")
    for index, user in enumerate(users[1:], start=1):
        expired = _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, f"old-{index}")
        _backdate(store, "token", expired.id, expires_at=_in(-10))
    _link(store, users[1], AuthActionPurpose.PASSWORD_RESET, "used")
    store.reset_password_with_token(_hash("used"), "hash-2")

    assert store.cleanup_auth_action_tokens(_in(-60)) == 0  # nothing ended an hour ago
    assert store.cleanup_auth_action_tokens(_in(1)) == 5
    # The stored row survived, unused (`live` is only the snapshot create returned).
    stored = store.get_live_auth_action_token(_hash("live"), AuthActionPurpose.PASSWORD_RESET)
    assert stored is not None and (stored.id, stored.used_at) == (live.id, None)
    assert store.reset_password_with_token(_hash("live"), "hash-2") is not None


def test_a_password_write_retires_the_unused_reset_links(store):
    """SEC-REC-2: a reset link copied during brief access to the mailbox must not undo the
    password set after it (Settings and scripts/create_admin.py write update_user_password)
    nor put one back on an account whose Google link removed it."""
    tag = uuid.uuid4().hex[:8]
    user = store.create_user(f"pw-{tag}", f"pw-{tag}@example.com", "hash-1")
    for provisioned in (False, True):
        _link(store, user, AuthActionPurpose.PASSWORD_RESET, f"reset-{provisioned}")
        _link(store, user, AuthActionPurpose.EMAIL_VERIFICATION, f"verify-{provisioned}")

        store.update_user_password(user.id, f"hash-{provisioned}", admin_provisioned=provisioned)

        assert store.get_live_auth_action_token(_hash(f"reset-{provisioned}"), AuthActionPurpose.PASSWORD_RESET) is None
        assert store.reset_password_with_token(_hash(f"reset-{provisioned}"), "hash-taken") is None
        # Another purpose is left alone.
        verification = store.get_live_auth_action_token(
            _hash(f"verify-{provisioned}"), AuthActionPurpose.EMAIL_VERIFICATION
        )
        assert verification is not None
    assert store.get_user_by_id(user.id).password_hash == "hash-True"
    _link(store, user, AuthActionPurpose.PASSWORD_RESET, "reset-after")  # a later link works
    assert store.reset_password_with_token(_hash("reset-after"), "hash-2").password_hash == "hash-2"

    cleared = store.create_user(f"pc-{tag}", f"pc-{tag}@example.com", "hash-c")
    kept = store.create_user(f"pk-{tag}", f"pk-{tag}@example.com", "hash-k", admin_provisioned=True)
    _link(store, cleared, AuthActionPurpose.PASSWORD_RESET, "reset-cleared")
    _link(store, kept, AuthActionPurpose.PASSWORD_RESET, "reset-kept")

    store.link_user_google_subject(cleared.id, f"sub-pc-{tag}", clear_password=True)
    store.link_user_google_subject(kept.id, f"sub-pk-{tag}", clear_password=False)

    assert store.reset_password_with_token(_hash("reset-cleared"), "hash-taken") is None
    assert store.get_user_by_id(cleared.id).password_hash is None
    # A link that keeps the password changes nothing a reset link could undo.
    assert store.get_live_auth_action_token(_hash("reset-kept"), AuthActionPurpose.PASSWORD_RESET) is not None


def test_deleting_a_user_deletes_their_links(store):
    tag = uuid.uuid4().hex[:8]
    user = store.create_user(f"d-{tag}", f"d-{tag}@example.com", "hash-1")
    _link(store, user, AuthActionPurpose.PASSWORD_RESET, f"reset-{tag}")

    assert store.delete_user(user.id) is True
    again = store.create_user(f"d-{tag}", f"d-{tag}@example.com", "hash-new")

    assert store.reset_password_with_token(_hash(f"reset-{tag}"), "hash-2") is None  # the row is gone
    assert store.get_user_by_id(again.id).password_hash == "hash-new"
    assert store.cleanup_auth_action_tokens(_in(24 * 60)) == 0


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


# ── begin_finalization: the ANALYZING CAS and the finalize job in one step ──────


def test_begin_finalization_takes_the_cas_and_queues_a_job(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)

    job = store.begin_finalization(record.id, max_attempts=5)

    assert job is not None and job.status == FinalizeJobStatus.PENDING and job.max_attempts == 5
    assert store.get_research(record.id).status == ResearchStatus.ANALYZING
    assert store.get_latest_research_finalize_job(record.id).id == job.id
    # Lost CAS: nothing changes, no second job.
    assert store.begin_finalization(record.id) is None
    assert [j.id for j in store.get_pending_research_finalize_jobs() if j.research_id == record.id] == [job.id]
    assert store.begin_finalization("missing-research") is None


@pytest.mark.parametrize("ended", [ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED])
def test_begin_finalization_leaves_an_ended_research_alone(store, ended):
    record = _research(store)
    store.update_research_status(record.id, ended)

    assert store.begin_finalization(record.id) is None
    assert store.get_research(record.id).status == ended
    assert store.get_latest_research_finalize_job(record.id) is None


def test_begin_finalization_can_require_every_search_settled(store):
    record, _task_row = _processing_research_with_a_queued_search(store)

    assert store.begin_finalization(record.id, require_settled_searches=True) is None
    assert store.get_research(record.id).status == ResearchStatus.PROCESSING
    assert store.get_latest_research_finalize_job(record.id) is None
    # Without the requirement (a finalize-only retry) the queued search does not block.
    assert store.begin_finalization(record.id) is not None


def test_begin_finalization_reuses_a_queued_job_and_requeues_a_stopped_one(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    first = store.begin_finalization(record.id)
    claimed_epoch = store.claim_research_finalize_job_by_id(first.id).lease_epoch  # a number: memory hands out live objects
    store.update_research_status(record.id, ResearchStatus.PROCESSING)  # e.g. a retry reset it

    assert store.begin_finalization(record.id).id == first.id  # still held by a runner: reused

    store.update_research_finalize_job(first.id, FinalizeJobStatus.DEAD_LETTER, "boom")  # out of retries
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    requeued = store.begin_finalization(record.id)
    assert requeued.id == first.id and requeued.status == FinalizeJobStatus.PENDING
    assert (requeued.attempt_count, requeued.error) == (0, None)
    assert requeued.lease_epoch == claimed_epoch + 1  # fences the old runner

    store.update_research_finalize_job(first.id, FinalizeJobStatus.COMPLETED)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    fresh = store.begin_finalization(record.id)
    assert fresh.id != first.id and fresh.status == FinalizeJobStatus.PENDING


# ── search-job leases ───────────────────────────────────────────────────────────


def test_search_job_writes_with_a_lease_are_fenced(store):
    task = _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    claimed = store.claim_search_task_job_by_id(job.id)
    assert claimed.lease_epoch == job.lease_epoch == 0  # a claim keeps the epoch

    # Recovery takes the job from the runner: the epoch moves on.
    store.recover_stale_search_task_jobs(datetime.now(timezone.utc) + timedelta(hours=1))
    recovered = store.get_search_task_job(job.id)
    assert (recovered.status, recovered.lease_epoch) == (SearchJobStatus.PENDING, 1)
    reclaimed = store.claim_search_task_job_by_id(job.id)

    # The old runner's writes are refused and change nothing.
    assert store.update_search_task_job(job.id, SearchJobStatus.COMPLETED, lease_epoch=0) is None
    assert store.record_search_task_job_failure(job.id, "late", lease_epoch=0) is None
    assert store.update_task_under_search_lease(task.id, TaskUpdate(log="late"), job.id, 0) is None
    current = store.get_search_task_job(job.id)
    assert (current.status, current.error) == (SearchJobStatus.RUNNING, None)
    assert "late" not in store.get_task(task.id).logs

    # The new runner's writes land.
    assert store.update_task_under_search_lease(task.id, TaskUpdate(log="mine"), job.id, 1).logs[-1] == "mine"
    done = store.update_search_task_job(job.id, SearchJobStatus.COMPLETED, lease_epoch=reclaimed.lease_epoch)
    assert done.status == SearchJobStatus.COMPLETED
    # A settled job refuses leased writes too; an unleased write is not fenced.
    assert store.update_task_under_search_lease(task.id, TaskUpdate(log="after"), job.id, 1) is None
    assert store.update_search_task_job(job.id, SearchJobStatus.FAILED, "admin") is not None


def test_search_job_lease_fences_only_its_own_task(store):
    task, other = _task(store), _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(job.id)

    assert store.update_task_under_search_lease(other.id, TaskUpdate(log="x"), job.id, 0) is None
    assert store.update_task_under_search_lease("missing-task", TaskUpdate(log="x"), job.id, 0) is None
    assert store.update_task_under_search_lease(task.id, TaskUpdate(log="x"), "missing-job", 0) is None
    assert store.get_task(other.id).logs == []


def test_search_job_failure_with_a_lease_retries_then_dead_letters(store):
    task = _task(store)
    job = store.add_search_task_job(task.id, SearchDepth.EASY.value, max_attempts=2)
    first = store.claim_search_task_job_by_id(job.id)
    assert store.record_search_task_job_failure(job.id, "boom", lease_epoch=first.lease_epoch).status == SearchJobStatus.PENDING
    second = store.claim_search_task_job_by_id(job.id)
    assert second.lease_epoch == first.lease_epoch  # the retry is the same runner lineage
    dead = store.record_search_task_job_failure(job.id, "boom", lease_epoch=second.lease_epoch)
    assert dead.status == SearchJobStatus.DEAD_LETTER


def test_search_job_requeues_bump_the_lease_epoch(store):
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.PROCESSING)
    task = _task(store, record.id)
    job = _dead_letter_search_job(store, task.id)
    epoch = store.get_search_task_job(job.id).lease_epoch

    assert store.requeue_search_task_job(job.id).lease_epoch == epoch + 1
    store.claim_search_task_job_by_id(job.id)
    store.record_search_task_job_failure(job.id, "boom")
    store.update_search_task_job(job.id, SearchJobStatus.DEAD_LETTER, "boom")
    requeued = store.requeue_search_task_job_of_active_research(job.id, "Search job manually requeued")
    assert requeued.lease_epoch == epoch + 2
