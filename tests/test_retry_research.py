"""RETRY-REWRITE: POST /v1/research/{id}/retry on the service primitives.

The old implementation raised AttributeError on every call (ResearchStatus.TIMEOUT,
task_store.update_task_status), and behind that skipped admission, rewrote graph_state
without the row lock, left finalize-only retries in ANALYZING with no job and created
search jobs with swapped arguments.
"""
import threading
import uuid
from datetime import datetime, timedelta

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
from src.auth.llm_rate_limit import enforce_llm_rate_limit
from src.config import settings
from src.domain.errors import ConflictError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

KEPT_KEYS = {
    "title": "My research",
    "thread_id": "thread-1",
    "model": "deepseek-chat",
    "share_token": "share-me",
    "webhook_url": "https://hooks.example.com/x",
    "messages": [{"role": "user", "content": "hi", "sources": []}],
}
STALE_KEYS = {
    "step": "verify",
    "report": "failed draft",
    "error": "boom",
    "analyze_attempts": 2,
    "detected_conflicts": [{"claim": "x"}],
    "citation_audit": {"grounded": 0},
    "red_team": {"findings": []},
    "stance_balance": {"balance": "one-sided"},
    "numeric_check": {"issues": 1},
    # The failed attempt's [Sn] table: any stored list is authoritative for the readers.
    "canonical_sources": [{"source_id": "S7", "url": "https://old.example/7"}],
    "llm_token_usage": {"prompt_tokens": 10, "completion_tokens": 5},
}


class _StubAnalyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "retried report"


class _StubOrchestrator:
    def run_decompose(self, prompt, depth, **kwargs):
        return [{"id": "decomposed-1", "description": "d", "queries": ["q"], "status": TaskStatus.PENDING}]


class _FakeBroker:
    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


def _service(store=None, broker=None):
    return ResearchService(
        task_store=store or InMemoryTaskStore(),
        analyzer=_StubAnalyzer(),
        orchestrator=_StubOrchestrator(),
        broker=broker,
    )


def _failed_research(store, *tasks, user_id=None):
    research = store.add_research(
        ResearchRequest(prompt="retry this research", depth=SearchDepth.EASY), task_ids=[], user_id=user_id
    )
    for task in tasks:
        store.add_task({"research_id": research.id, "description": "d", "queries": ["q"], **task})
    store.set_research_task_ids(research.id, [task["id"] for task in tasks])
    store.merge_research_graph_state(research.id, {**KEPT_KEYS, **STALE_KEYS})
    store.update_research_status(research.id, ResearchStatus.FAILED, "Research failed during analysis.")
    return research


def _completed_task(task_id="t1"):
    return {
        "id": task_id,
        "status": TaskStatus.COMPLETED,
        "result": [{"url": f"https://example.com/{task_id}", "title": "A", "content": "Body " * 40}],
    }


def _dead_letter_finalize_job(store, research_id):
    job = store.add_research_finalize_job(research_id, max_attempts=1)
    claimed = store.claim_research_finalize_job_by_id(job.id)
    store.record_research_finalize_job_failure(job.id, "boom", lease_epoch=claimed.lease_epoch)
    return store.get_research_finalize_job(job.id)


def _dead_letter_search_job(store, task_id):
    job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    return store.record_search_task_job_failure(job.id, "boom")


def _assert_reset(store, research_id):
    research = store.get_research(research_id)
    assert research.final_report is None
    for key, value in KEPT_KEYS.items():
        assert research.graph_state[key] == value
    assert not set(STALE_KEYS) & set(research.graph_state)
    return research


# ── finalization retry ───────────────────────────────────────────────────────


def test_finalize_retry_requeues_the_dead_letter_job_with_a_new_lease():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    research = _failed_research(store, _completed_task())
    dead = _dead_letter_finalize_job(store, research.id)
    old_epoch = dead.lease_epoch

    retried = service.retry_research(research.id)

    assert retried.status == ResearchStatus.ANALYZING
    _assert_reset(store, research.id)
    job = store.get_research_finalize_job(dead.id)
    assert job.status == FinalizeJobStatus.PENDING
    assert job.attempt_count == 0
    assert job.lease_epoch == old_epoch + 1
    assert broker.finalize == [dead.id]

    service.process_finalize_job(dead.id)
    final = store.get_research(research.id)
    assert final.status == ResearchStatus.COMPLETED
    assert final.final_report == "retried report"


def test_finalize_retry_without_any_job_creates_one_instead_of_sticking_in_analyzing():
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task())

    service.retry_research(research.id)

    job = store.get_latest_research_finalize_job(research.id)
    assert job is not None and job.status == FinalizeJobStatus.PENDING
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    # Postgres-polling workers claim straight from the job table.
    assert store.claim_next_research_finalize_job().id == job.id


def test_finalize_retry_does_not_reuse_a_completed_job():
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task())
    old = store.add_research_finalize_job(research.id)
    store.update_research_finalize_job(old.id, FinalizeJobStatus.COMPLETED)

    service.retry_research(research.id)

    latest = store.get_latest_research_finalize_job(research.id)
    assert latest.id != old.id and latest.status == FinalizeJobStatus.PENDING
    assert store.get_research_finalize_job(old.id).status == FinalizeJobStatus.COMPLETED


# ── search retry ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("with_broker", [True, False])
def test_search_retry_resets_failed_and_stalled_tasks_and_always_creates_job_rows(with_broker):
    store = InMemoryTaskStore()
    broker = _FakeBroker() if with_broker else None
    service = _service(store, broker)
    research = _failed_research(
        store,
        _completed_task("done"),
        {"id": "failed", "status": TaskStatus.FAILED},
        {"id": "stalled", "status": TaskStatus.PENDING},
    )
    dead = _dead_letter_search_job(store, "failed")
    drained = store.add_search_task_job("stalled", SearchDepth.EASY.value)
    store.update_search_task_job(drained.id, SearchJobStatus.COMPLETED, "Research no longer active — search skipped")

    retried = service.retry_research(research.id)

    assert retried.status == ResearchStatus.PROCESSING
    _assert_reset(store, research.id)
    assert store.get_task("done").status == TaskStatus.COMPLETED
    assert store.get_task("failed").status == TaskStatus.PENDING
    assert store.get_task("stalled").status == TaskStatus.PENDING
    assert store.get_search_task_job(dead.id).status == SearchJobStatus.PENDING
    fresh = store.get_latest_search_task_job("stalled")
    assert fresh.id != drained.id and fresh.status == SearchJobStatus.PENDING
    assert fresh.depth == SearchDepth.EASY
    assert store.get_latest_search_task_job("done") is None
    if with_broker:
        assert sorted(broker.search) == sorted([dead.id, fresh.id])
    pending = {store.claim_next_search_task_job().id, store.claim_next_search_task_job().id}
    assert pending == {dead.id, fresh.id}


def test_search_retry_serves_the_task_pool_not_the_failed_attempts_source_table():
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(
        store,
        {
            "id": "done",
            "status": TaskStatus.COMPLETED,
            "result": [
                {"url": "https://one.example/a", "title": "One", "content": "first source text"},
                {"url": "https://two.example/b", "title": "Two", "content": "second source text"},
            ],
        },
        {"id": "failed", "status": TaskStatus.FAILED},
    )
    assert [s.source_id for s in service.get_research_sources(research.id)] == ["S7"]

    service.retry_research(research.id)

    assert "canonical_sources" not in store.get_research(research.id).graph_state
    assert [(s.source_id, s.url) for s in service.get_research_sources(research.id)] == [
        ("S1", "https://one.example/a"),
        ("S2", "https://two.example/b"),
    ]


def test_search_retry_leaves_a_job_that_is_still_running_alone():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    research = _failed_research(store, {"id": "t1", "status": TaskStatus.FAILED})
    running = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(running.id)

    service.retry_research(research.id)

    assert store.get_search_task_job(running.id).status == SearchJobStatus.RUNNING
    assert broker.search == []


# ── the dead-letter job a search-path retry leaves behind ────────────────────


def _complete_searches(service):
    """Stand-in for SearchAgent: the redispatched search finds one source."""

    def run_search_task(task_id, depth):
        service.task_store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": f"https://example.com/{task_id}", "title": "B", "content": "Body " * 40}],
            ),
        )

    service.run_search_task = run_search_task


def test_admin_requeue_of_the_pre_retry_dead_letter_job_cannot_rewind_the_research():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    _complete_searches(service)
    research = _failed_research(store, _completed_task("t1"), {"id": "t2", "status": TaskStatus.FAILED})
    dead_search = _dead_letter_search_job(store, "t2")
    stale = _dead_letter_finalize_job(store, research.id)

    service.retry_research(research.id)  # search path: t2 is FAILED

    with pytest.raises(ConflictError):
        service.requeue_research_finalize_job(stale.id)
    assert store.get_research(research.id).status == ResearchStatus.PROCESSING
    assert store.get_research_finalize_job(stale.id).status == FinalizeJobStatus.DEAD_LETTER

    service.process_search_task_job(dead_search.id)

    # Finalization reuses the stopped job instead of leaving it in the dead-letter list.
    assert store.get_latest_research_finalize_job(research.id).id == stale.id
    assert store.get_dead_letter_research_finalize_jobs() == []
    assert broker.finalize == [stale.id]
    service.process_finalize_job(stale.id)
    final = store.get_research(research.id)
    assert final.status == ResearchStatus.COMPLETED and final.final_report == "retried report"
    with pytest.raises(ConflictError):
        service.requeue_research_finalize_job(stale.id)
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED


def test_admin_requeue_refuses_the_dead_letter_job_of_a_cancelled_research():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    research = _failed_research(store, _completed_task())
    dead = _dead_letter_finalize_job(store, research.id)
    store.update_research_status(research.id, ResearchStatus.CANCELLED, "Cancelled by user.")

    with pytest.raises(ConflictError):
        service.requeue_research_finalize_job(dead.id)

    assert store.get_research(research.id).status == ResearchStatus.CANCELLED
    assert store.get_research_finalize_job(dead.id).status == FinalizeJobStatus.DEAD_LETTER
    assert broker.finalize == []


def test_admin_requeue_takes_only_the_latest_dead_letter_job():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    research = _failed_research(store, _completed_task())
    superseded = _dead_letter_finalize_job(store, research.id)
    superseded.created_at -= timedelta(minutes=5)
    latest = _dead_letter_finalize_job(store, research.id)

    with pytest.raises(ConflictError):
        service.requeue_research_finalize_job(superseded.id)
    assert store.get_research(research.id).status == ResearchStatus.FAILED

    requeued = service.requeue_research_finalize_job(latest.id)
    assert requeued.id == latest.id and requeued.status == FinalizeJobStatus.PENDING
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    assert broker.finalize == [latest.id]


def test_admin_requeue_losing_a_race_leaves_the_research_failed(monkeypatch):
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task())
    dead = _dead_letter_finalize_job(store, research.id)
    monkeypatch.setattr(store, "requeue_failed_research_finalize_job", lambda job_id: None)

    with pytest.raises(ConflictError):
        service.requeue_research_finalize_job(dead.id)

    assert store.get_research(research.id).status == ResearchStatus.FAILED


# ── decomposition retry ──────────────────────────────────────────────────────


def test_decompose_retry_replays_the_stored_request_in_the_background():
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store)
    store.merge_research_graph_state(
        research.id,
        {
            "decompose_pending": True,
            "decompose_payload": ResearchRequest(
                prompt="retry this research", depth=SearchDepth.MEDIUM, thread_id="thread-1"
            ).model_dump(mode="json"),
        },
    )

    class _Background:
        def __init__(self):
            self.calls = []

        def add_task(self, func, *args):
            self.calls.append((func, args))

    background = _Background()
    store.get_research(research.id).created_at -= timedelta(hours=1)  # an old research
    service.retry_research(research.id, background_tasks=background)

    current = store.get_research(research.id)
    assert current.status == ResearchStatus.PROCESSING
    # The marker ages from the retry, not from created_at: maintenance must not start a
    # second decomposition next to the background one.
    assert current.graph_state["decompose_pending"] is True
    assert current.graph_state["decompose_requested_at"]
    assert service.recover_pending_decompositions() == 0
    (func, (research_id, request)), = background.calls
    assert func == service.decompose_and_enqueue and research_id == research.id
    assert request.depth == SearchDepth.MEDIUM

    func(research_id, request)
    assert [task.id for task in store.get_tasks_by_research(research.id)] == ["decomposed-1"]
    assert store.get_latest_search_task_job("decomposed-1").status == SearchJobStatus.PENDING
    assert not {"decompose_pending", "decompose_requested_at"} & set(store.get_research(research.id).graph_state)


def _age_decompose_marker(store, research_id, minutes):
    graph_state = store.get_research(research_id).graph_state
    stamp = datetime.fromisoformat(graph_state["decompose_requested_at"]) - timedelta(minutes=minutes)
    store.merge_research_graph_state(research_id, {"decompose_requested_at": stamp.isoformat()})


def test_decompose_retry_lost_with_its_api_process_is_replayed_by_maintenance(monkeypatch):
    monkeypatch.setattr(settings, "decompose_recovery_minutes", 10)
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store)
    replayed = threading.Event()
    calls = []

    def decompose(research_id, request):
        calls.append((research_id, request.prompt))
        replayed.set()

    class _DiesWithTheProcess:
        def add_task(self, func, *args):
            pass  # the API process is killed before the background task runs

    service.retry_research(research.id, background_tasks=_DiesWithTheProcess())
    monkeypatch.setattr(service, "decompose_and_enqueue", decompose)
    assert service.recover_pending_decompositions() == 0  # it may still be running

    _age_decompose_marker(store, research.id, minutes=11)
    assert service.recover_pending_decompositions() == 1
    assert replayed.wait(5)
    assert calls == [(research.id, "retry this research")]
    # Re-stamped on replay: the next pass does not start a second one while it runs.
    assert service.recover_pending_decompositions() == 0


class _DiesWithTheProcess:
    def add_task(self, func, *args):
        pass  # the API process is killed before the background task runs


def test_a_lost_retry_decomposition_behind_the_newest_fifty_researches_is_replayed(monkeypatch):
    """Recovery scanned only the 50 newest researches. A retried one is usually older, and
    the stalled sweep leaves every research with the marker to recovery: it stayed
    PROCESSING for good."""
    monkeypatch.setattr(settings, "decompose_recovery_minutes", 10)
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store)
    store.get_research(research.id).created_at -= timedelta(days=2)
    for index in range(60):
        newer = store.add_research(ResearchRequest(prompt=f"newer {index}", depth=SearchDepth.EASY), task_ids=[])
        store.update_research_status(newer.id, ResearchStatus.COMPLETED, "done")
    service.retry_research(research.id, background_tasks=_DiesWithTheProcess())
    replayed = threading.Event()
    calls = []

    def decompose(research_id, request):
        calls.append((research_id, request.prompt))
        replayed.set()

    monkeypatch.setattr(service, "decompose_and_enqueue", decompose)
    _age_decompose_marker(store, research.id, minutes=11)

    assert service.recover_pending_decompositions() == 1
    assert replayed.wait(5)
    assert calls == [(research.id, "retry this research")]


def test_a_stale_decomposition_that_cannot_be_replayed_is_failed_so_it_can_be_retried(monkeypatch):
    monkeypatch.setattr(settings, "decompose_recovery_minutes", 10)
    store = InMemoryTaskStore()
    service = _service(store)
    research = store.add_research(ResearchRequest(prompt="request lost", depth=SearchDepth.EASY), task_ids=[])
    # A marker without the request it would replay: it headed every recovery batch for good.
    store.merge_research_graph_state(research.id, {"decompose_pending": True})
    record = store.get_research(research.id)
    record.created_at -= timedelta(minutes=11)

    # Written since the threshold: a decomposition still emitting progress wins.
    assert service.recover_pending_decompositions() == 0
    assert store.get_research(research.id).status == ResearchStatus.PROCESSING

    record.updated_at -= timedelta(minutes=11)
    assert service.recover_pending_decompositions() == 0
    failed = store.get_research(research.id)
    assert failed.status == ResearchStatus.FAILED and failed.final_report == service.STALLED_RESEARCH_REPORT
    assert store.list_pending_decomposition_ids() == []

    started = []
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda research_id, req: started.append(req.prompt))
    service.retry_research(research.id)
    assert started == ["request lost"]


def test_an_unreadable_decompose_stamp_ages_on_the_last_write(monkeypatch):
    monkeypatch.setattr(settings, "decompose_recovery_minutes", 10)
    store = InMemoryTaskStore()
    service = _service(store)
    request = ResearchRequest(prompt="stamp garbled", depth=SearchDepth.EASY)
    research = store.add_research(request, task_ids=[])
    store.merge_research_graph_state(
        research.id,
        {
            "decompose_pending": True,
            "decompose_requested_at": "not a date",
            "decompose_payload": request.model_dump(mode="json"),
        },
    )
    replayed = threading.Event()
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda research_id, req: replayed.set())

    assert service.recover_pending_decompositions() == 0  # written just now
    store.get_research(research.id).updated_at -= timedelta(minutes=11)
    assert service.recover_pending_decompositions() == 1
    assert replayed.wait(5)


def test_a_decomposition_started_on_an_old_research_is_not_doubled_by_recovery(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 1)
    store = InMemoryTaskStore()
    service = _service(store)
    request = ResearchRequest(prompt="queued for a long time", depth=SearchDepth.EASY)
    research = store.add_research(request, task_ids=[])
    store.update_research_status(research.id, ResearchStatus.QUEUED)
    store.merge_research_graph_state(
        research.id, {"decompose_pending": True, "decompose_payload": request.model_dump(mode="json")}
    )
    store.get_research(research.id).created_at -= timedelta(hours=1)
    started = []
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda research_id, req: started.append(research_id))

    assert service.promote_queued_researches() == 1

    assert service.recover_pending_decompositions() == 0


def test_a_decomposition_after_late_clarification_answers_is_not_doubled_by_recovery(monkeypatch):
    store = InMemoryTaskStore()
    service = _service(store)
    request = ResearchRequest(prompt="answered an hour later", depth=SearchDepth.EASY, plan_first=True)
    research = store.add_research(request, task_ids=[])
    store.merge_research_graph_state(
        research.id,
        {"decompose_payload": request.model_dump(mode="json"), "clarifications": {"questions": ["Which?"], "answers": []}},
    )
    store.update_research_status(research.id, ResearchStatus.CLARIFYING)
    store.get_research(research.id).created_at -= timedelta(hours=1)
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda research_id, req: None)

    service.submit_clarifications(research.id, ["This one"])

    assert store.get_research(research.id).graph_state["decompose_pending"] is True
    assert service.recover_pending_decompositions() == 0


def test_retry_that_cannot_dispatch_its_finalize_job_hands_the_research_back_as_failed(monkeypatch):
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task())
    _dead_letter_finalize_job(store, research.id)

    def connection_dropped(job_id):
        raise RuntimeError("DB connection dropped")

    monkeypatch.setattr(store, "requeue_research_finalize_job", connection_dropped)
    with pytest.raises(RuntimeError):
        service.retry_research(research.id)

    assert store.get_research(research.id).status == ResearchStatus.FAILED
    monkeypatch.undo()
    assert service.retry_research(research.id).status == ResearchStatus.ANALYZING


# ── admission and state guards ───────────────────────────────────────────────


def test_only_failed_research_can_be_retried():
    store = InMemoryTaskStore()
    service = _service(store)
    research = store.add_research(ResearchRequest(prompt="still running", depth=SearchDepth.EASY), task_ids=[])

    with pytest.raises(ConflictError):
        service.retry_research(research.id)


def test_retry_is_refused_while_the_user_is_at_the_concurrency_limit(monkeypatch):
    monkeypatch.setattr(settings, "max_concurrent_researches", 1)
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task(), user_id="user-1")
    store.add_research(ResearchRequest(prompt="another active one", depth=SearchDepth.EASY), task_ids=[], user_id="user-1")

    with pytest.raises(ConflictError):
        service.retry_research(research.id, user_id="user-1")

    assert store.get_research(research.id).status == ResearchStatus.FAILED
    assert store.get_latest_research_finalize_job(research.id) is None


def test_concurrent_retries_admit_exactly_one():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = _service(store, broker)
    research = _failed_research(store, _completed_task())
    # Let both requests pass the FAILED check before either one is admitted.
    both_checked = threading.Barrier(2, timeout=2)
    list_tasks = store.get_tasks_by_research

    def racing_list(research_id):
        tasks = list_tasks(research_id)
        both_checked.wait()
        return tasks

    store.get_tasks_by_research = racing_list
    outcomes: list[str] = []

    def retry():
        try:
            service.retry_research(research.id)
            outcomes.append("ok")
        except ConflictError as exc:
            assert exc.status_code == 409
            outcomes.append("conflict")

    callers = [threading.Thread(target=retry) for _ in range(2)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join()

    assert sorted(outcomes) == ["conflict", "ok"]
    assert len(store.get_pending_research_finalize_jobs()) == 1
    assert len(broker.finalize) == 1


def test_cancel_between_admission_and_reset_wins(monkeypatch):
    store = InMemoryTaskStore()
    service = _service(store)
    research = _failed_research(store, _completed_task())
    admit = store.try_admit_research

    def admit_then_cancel(*args, **kwargs):
        admitted = admit(*args, **kwargs)
        store.update_research_status(research.id, ResearchStatus.CANCELLED, "Cancelled by user.")
        return admitted

    monkeypatch.setattr(store, "try_admit_research", admit_then_cancel)

    with pytest.raises(ConflictError):
        service.retry_research(research.id)
    assert store.get_research(research.id).status == ResearchStatus.CANCELLED
    assert store.get_latest_research_finalize_job(research.id) is None


# ── route ────────────────────────────────────────────────────────────────────


def _route(app, path, method):
    return next(
        route for route in app.routes
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set())
    )


def test_retry_route_is_llm_rate_limited():
    from src.api.app import create_app

    route = _route(create_app(), "/v1/research/{research_id}/retry", "POST")
    assert enforce_llm_rate_limit in [dependency.call for dependency in route.dependant.dependencies]


def _route_research(store, monkeypatch, *tasks):
    """A failed research owned by a fresh user. The route tests also run in postgres-smoke,
    on a database shared with every other test: fixed ids would collide there, and other
    tests' researches would count against the global capacity."""
    monkeypatch.setattr(settings, "max_global_active_researches", 0)
    user_id = f"retry-owner-{uuid.uuid4().hex[:8]}"
    store.create_user(user_id, f"{user_id}@example.com", None)
    return _failed_research(store, *tasks, user_id=user_id)


@pytest.mark.anyio
async def test_retry_route_retries_a_failed_research(client, monkeypatch):
    store = client._transport.app.state.research_service.task_store
    failed_task = {
        "id": f"route-{uuid.uuid4().hex[:8]}",
        "status": TaskStatus.FAILED,
        "logs": ["Agent started search process", "Error: search provider down"],
    }
    research = _route_research(store, monkeypatch, failed_task)

    response = await client.post(f"/v1/research/{research.id}/retry")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "processing"
    again = await client.post(f"/v1/research/{research.id}/retry")
    assert again.status_code == 409


@pytest.mark.anyio
async def test_retry_route_without_an_analyzer_fails_before_taking_a_slot(client, monkeypatch):
    service = client._transport.app.state.research_service
    # Explicit: whether the test app has an LLM depends on DEEPSEEK_API_KEY in the env.
    monkeypatch.setattr(service, "analyzer", None)
    store = service.task_store
    research = _route_research(store, monkeypatch, _completed_task(f"route-{uuid.uuid4().hex[:8]}"))

    response = await client.post(f"/v1/research/{research.id}/retry")

    assert response.status_code == 503
    assert store.get_research(research.id).status == ResearchStatus.FAILED
