"""TEST-STORE-CONFORMANCE: one behavioral suite over BOTH TaskStore backends.

Every test here runs against InMemoryTaskStore and (when Postgres is reachable)
SQLAlchemyTaskStore. The premise of the 500+ non-postgres tests is that the two
implementations behave identically; this suite is what keeps that true — a
method that drifts fails on the postgres leg instead of in production.

The postgres leg shares the conftest throwaway database (migrated to head),
so it never depends on a developer's working database.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.repositories.in_memory_task_store import InMemoryTaskStore
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
