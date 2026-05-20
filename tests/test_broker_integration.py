"""
Tests that ResearchService calls the broker at the correct integration points:
enqueue, requeue — every place a job_id must be pushed to Redis.
"""
from unittest.mock import MagicMock

from src.api.schemas import ResearchRequest, SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_analyzer():
    analyzer = MagicMock()
    analyzer.run_analysis.return_value = "stub report"
    return analyzer


def _service_with_broker(task_store=None):
    broker = MagicMock()
    service = ResearchService(
        task_store=task_store or InMemoryTaskStore(),
        analyzer=_stub_analyzer(),
        broker=broker,
    )
    return service, broker


def _add_completed_task(task_store, research_id: str, task_id: str = "task-1"):
    task_store.add_task(
        {
            "id": task_id,
            "research_id": research_id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "T", "content": "Body"}],
        }
    )


# ---------------------------------------------------------------------------
# enqueue_research_finalization
# ---------------------------------------------------------------------------


def test_enqueue_finalization_pushes_to_broker():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    _add_completed_task(task_store, research.id)

    service, broker = _service_with_broker(task_store)
    _, job = service.enqueue_research_finalization(research.id)

    assert job is not None
    broker.push_finalize_job.assert_called_once_with(job.id)


def test_enqueue_finalization_does_not_push_when_no_broker():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    _add_completed_task(task_store, research.id)

    service = ResearchService(task_store=task_store, analyzer=_stub_analyzer())
    _, job = service.enqueue_research_finalization(research.id)

    assert job is not None  # джоб создан, но push не вызывался


# ---------------------------------------------------------------------------
# requeue_research_finalize_job
# ---------------------------------------------------------------------------


def test_requeue_finalize_job_pushes_to_broker():
    from src.api.schemas import ResearchStatus

    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_store.update_research_status(research.id, ResearchStatus.FAILED, "error")
    job = task_store.add_research_finalize_job(research.id, max_attempts=1)
    task_store.claim_next_research_finalize_job()
    task_store.record_research_finalize_job_failure(job.id, "boom")

    service, broker = _service_with_broker(task_store)
    requeued = service.requeue_research_finalize_job(job.id)

    broker.push_finalize_job.assert_called_once_with(requeued.id)


# ---------------------------------------------------------------------------
# requeue_search_task_job
# ---------------------------------------------------------------------------


def test_requeue_search_job_pushes_to_broker():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["q"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=1)
    task_store.claim_next_search_task_job()
    task_store.record_search_task_job_failure(job.id, "boom")

    service, broker = _service_with_broker(task_store)
    requeued = service.requeue_search_task_job(job.id)

    broker.push_search_job.assert_called_once_with(requeued.id)


# ---------------------------------------------------------------------------
# Broker attribute на сервисе
# ---------------------------------------------------------------------------


def test_service_stores_broker_reference():
    broker = MagicMock()
    service = ResearchService(task_store=InMemoryTaskStore(), broker=broker)
    assert service.broker is broker


def test_service_broker_is_none_by_default():
    service = ResearchService(task_store=InMemoryTaskStore())
    assert service.broker is None
