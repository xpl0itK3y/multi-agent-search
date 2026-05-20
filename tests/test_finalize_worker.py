from unittest.mock import MagicMock

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import FinalizeWorker


def test_finalize_worker_processes_pending_jobs(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "worker report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 1
    assert task_store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert task_store.get_research(research.id).final_report == "worker report"
    assert task_store.get_research_finalize_job(job.id).status.value == "completed"


def test_finalize_worker_uses_graph_runner_when_enabled(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    runner = mocker.patch.object(service.finalize_graph_runner, "run", return_value="graph report")
    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 1
    assert runner.called
    assert task_store.get_research(research.id).final_report == "graph report"


# ---------------------------------------------------------------------------
# Redis mode
# ---------------------------------------------------------------------------


def _make_research_with_completed_task(task_store):
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    return research


def test_finalize_worker_redis_mode_processes_job(mocker):
    task_store = InMemoryTaskStore()
    research = _make_research_with_completed_task(task_store)

    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "redis report"
    broker = MagicMock()
    service = ResearchService(task_store=task_store, analyzer=analyzer, broker=broker)
    _, job = service.enqueue_research_finalization(research.id)

    broker.pop_finalize_job.return_value = job.id

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 1
    broker.pop_finalize_job.assert_called_once()
    assert task_store.get_research(research.id).status == ResearchStatus.COMPLETED


def test_finalize_worker_redis_mode_returns_zero_when_queue_empty(mocker):
    broker = MagicMock()
    broker.pop_finalize_job.return_value = None

    service = ResearchService(task_store=InMemoryTaskStore(), broker=broker)
    process_job = mocker.patch.object(service, "process_finalize_job")

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 0
    process_job.assert_not_called()


def test_finalize_worker_redis_mode_skips_already_claimed_job(mocker):
    task_store = InMemoryTaskStore()
    research = _make_research_with_completed_task(task_store)

    analyzer = mocker.Mock()
    broker = MagicMock()
    service = ResearchService(task_store=task_store, analyzer=analyzer, broker=broker)
    _, job = service.enqueue_research_finalization(research.id)

    # Другой воркер уже забрал джоб
    task_store.claim_research_finalize_job_by_id(job.id)
    broker.pop_finalize_job.return_value = job.id

    process_job = mocker.patch.object(service, "process_finalize_job")

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 0
    process_job.assert_not_called()


def test_finalize_worker_postgres_mode_used_when_no_broker(mocker):
    task_store = InMemoryTaskStore()
    research = _make_research_with_completed_task(task_store)

    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "postgres report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    service.enqueue_research_finalization(research.id)

    processed_count = FinalizeWorker(service).run_once()

    assert processed_count == 1
    assert task_store.get_research(research.id).status == ResearchStatus.COMPLETED
