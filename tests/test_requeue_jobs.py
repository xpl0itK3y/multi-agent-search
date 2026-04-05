import pytest

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class StubAnalyzer:
    def run_analysis(self, prompt: str, tasks, depth=None):
        return "ok"


def test_requeue_search_job_resets_dead_letter_job_and_task():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
            "logs": ["failed once"],
        }
    )
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=1)
    store.claim_next_search_task_job()
    store.record_search_task_job_failure(job.id, "boom")

    service = ResearchService(task_store=store)
    requeued = service.requeue_search_task_job(job.id)

    assert requeued.status == SearchJobStatus.PENDING
    assert requeued.attempt_count == 0
    assert requeued.error is None
    task = store.get_task("task-1")
    assert task is not None
    assert task.status == TaskStatus.PENDING
    assert task.logs[-1] == "Search job manually requeued"


def test_requeue_finalize_job_resets_dead_letter_job_and_research_status():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    store.update_research_status(research.id, ResearchStatus.FAILED, "analysis failed")
    job = store.add_research_finalize_job(research.id, max_attempts=1)
    store.claim_next_research_finalize_job()
    store.record_research_finalize_job_failure(job.id, "boom")

    service = ResearchService(task_store=store, analyzer=StubAnalyzer())
    requeued = service.requeue_research_finalize_job(job.id)

    assert requeued.status == FinalizeJobStatus.PENDING
    assert requeued.attempt_count == 0
    assert requeued.error is None
    current = store.get_research(research.id)
    assert current is not None
    assert current.status == ResearchStatus.ANALYZING


def test_requeue_search_job_rejects_non_dead_letter_job():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    service = ResearchService(task_store=store)

    with pytest.raises(Exception) as exc_info:
        service.requeue_search_task_job(job.id)

    assert exc_info.value.status_code == 409


def test_requeue_finalize_job_rejects_non_dead_letter_job():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    job = store.add_research_finalize_job(research.id)
    service = ResearchService(task_store=store, analyzer=StubAnalyzer())

    with pytest.raises(Exception) as exc_info:
        service.requeue_research_finalize_job(job.id)

    assert exc_info.value.status_code == 409
