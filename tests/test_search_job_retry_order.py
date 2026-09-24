"""JOB-RETRY-ORDER: a search that fails once is retried before the research finalizes.

SearchAgent.run_task records its own failures as a FAILED task and returns normally.
Finalization used to be enqueued right there, before process_search_task_job decided
to retry, so the retry was then skipped as "research no longer active" and the
research finished without it (single task: FAILED, "All tasks failed.").
"""
import pytest

import src.services.research_service as research_service_module
from src.api.schemas import (
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _FlakyAgent:
    """Fails the first run of every task the way SearchAgent does: FAILED, no raise."""

    calls: dict[str, int] = {}
    failures_before_success = 1

    def __init__(self, task_store, **kwargs):
        self.task_store = task_store

    def run_task(self, task_id):
        attempt = self.calls.get(task_id, 0) + 1
        self.calls[task_id] = attempt
        self.task_store.update_task(task_id, TaskUpdate(status=TaskStatus.RUNNING, log="start"))
        if attempt <= self.failures_before_success:
            self.task_store.update_task(task_id, TaskUpdate(status=TaskStatus.FAILED, log="Error: transient"))
            return
        self.task_store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": f"https://example.com/{task_id}", "title": "A", "content": "Body " * 40}],
                log="ok",
            ),
        )


class _StubAnalyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report over " + ",".join(sorted(t.id for t in tasks if t.status == TaskStatus.COMPLETED))


class _FakeBroker:
    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


@pytest.fixture
def flaky_agent(monkeypatch):
    _FlakyAgent.calls = {}
    _FlakyAgent.failures_before_success = 1
    monkeypatch.setattr(research_service_module, "SearchAgent", _FlakyAgent)
    return _FlakyAgent


def _research_with_tasks(store, *tasks):
    research = store.add_research(ResearchRequest(prompt="retry order topic", depth=SearchDepth.EASY), task_ids=[])
    for task in tasks:
        store.add_task({"research_id": research.id, "description": "d", "queries": ["q"], **task})
    store.set_research_task_ids(research.id, [task["id"] for task in tasks])
    return research


def _work(store, service, job_id):
    """What a worker does with a job id popped from the broker."""
    claimed = store.claim_search_task_job_by_id(job_id)
    assert claimed is not None
    return service.process_search_task_job(claimed.id)


def test_single_task_failed_once_is_retried_and_the_research_completes(flaky_agent):
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)
    research = _research_with_tasks(store, {"id": "t1", "status": TaskStatus.PENDING})
    job = store.add_search_task_job("t1", SearchDepth.EASY.value)

    first = _work(store, service, job.id)

    assert first.status == SearchJobStatus.PENDING  # rescheduled, not finalized
    assert store.get_task("t1").status == TaskStatus.PENDING
    assert store.get_research(research.id).status == ResearchStatus.PROCESSING
    assert store.get_latest_research_finalize_job(research.id) is None
    assert broker.search == [job.id] and broker.finalize == []

    second = _work(store, service, broker.search[-1])

    assert second.status == SearchJobStatus.COMPLETED
    assert flaky_agent.calls == {"t1": 2}
    assert store.get_task("t1").status == TaskStatus.COMPLETED
    finalize_job = store.get_latest_research_finalize_job(research.id)
    assert finalize_job is not None and broker.finalize == [finalize_job.id]
    service.process_finalize_job(finalize_job.id)
    final = store.get_research(research.id)
    assert final.status == ResearchStatus.COMPLETED
    assert final.final_report == "report over t1"


def test_multi_task_research_waits_for_the_retried_task(flaky_agent):
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)
    research = _research_with_tasks(
        store,
        {
            "id": "t1",
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com/t1", "title": "X", "content": "Body " * 40}],
        },
        {"id": "t2", "status": TaskStatus.PENDING},
    )
    job = store.add_search_task_job("t2", SearchDepth.EASY.value)

    _work(store, service, job.id)
    assert store.get_research(research.id).status == ResearchStatus.PROCESSING
    assert broker.finalize == []

    _work(store, service, broker.search[-1])
    finalize_job = store.get_latest_research_finalize_job(research.id)
    service.process_finalize_job(finalize_job.id)

    assert flaky_agent.calls == {"t2": 2}
    assert store.get_research(research.id).final_report == "report over t1,t2"


def test_exhausted_failed_task_still_lets_the_research_finalize(flaky_agent):
    flaky_agent.failures_before_success = 99
    store, broker = InMemoryTaskStore(), _FakeBroker()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)
    research = _research_with_tasks(
        store,
        {
            "id": "t1",
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com/t1", "title": "X", "content": "Body " * 40}],
        },
        {"id": "t2", "status": TaskStatus.PENDING},
    )
    job = store.add_search_task_job("t2", SearchDepth.EASY.value, max_attempts=2)

    _work(store, service, job.id)
    dead = _work(store, service, broker.search[-1])

    assert dead.status == SearchJobStatus.DEAD_LETTER
    assert store.get_task("t2").status == TaskStatus.FAILED  # FAILED counts as settled
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    assert len(broker.finalize) == 1


def test_dead_letter_after_exceptions_settles_a_running_task_and_finalizes(mocker):
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer())
    research = _research_with_tasks(
        store,
        {
            "id": "t1",
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com/t1", "title": "X", "content": "Body " * 40}],
        },
        {"id": "t2", "status": TaskStatus.PENDING},
    )
    job = store.add_search_task_job("t2", SearchDepth.EASY.value, max_attempts=1)

    def crash(task_id, depth):
        store.update_task(task_id, TaskUpdate(status=TaskStatus.RUNNING))
        raise RuntimeError("worker-side crash")

    mocker.patch.object(service, "run_search_task", side_effect=crash)
    dead = _work(store, service, job.id)

    assert dead.status == SearchJobStatus.DEAD_LETTER
    assert store.get_task("t2").status == TaskStatus.FAILED
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    assert store.get_latest_research_finalize_job(research.id) is not None


def test_retry_is_skipped_once_finalization_has_begun(flaky_agent):
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer())
    research = _research_with_tasks(store, {"id": "t1", "status": TaskStatus.PENDING})
    job = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.update_research_status(research.id, ResearchStatus.ANALYZING)

    drained = _work(store, service, job.id)

    assert drained.status == SearchJobStatus.COMPLETED
    assert drained.error == "Research no longer active — search skipped"
    assert flaky_agent.calls == {}
