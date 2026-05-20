from unittest.mock import MagicMock

from src.api.schemas import SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import SearchWorker


def test_search_worker_processes_pending_jobs(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.HARD.value)
    service = ResearchService(task_store=task_store)
    process_job = mocker.patch.object(service, "process_search_task_job")

    processed_count = SearchWorker(service).run_once()

    assert processed_count == 1
    process_job.assert_called_once()
    assert task_store.get_search_task_job(job.id).status.value == "running"


# ---------------------------------------------------------------------------
# Redis mode
# ---------------------------------------------------------------------------


def test_search_worker_redis_mode_processes_job(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)

    broker = MagicMock()
    broker.pop_search_job.return_value = job.id

    service = ResearchService(task_store=task_store, broker=broker)
    process_job = mocker.patch.object(service, "process_search_task_job")

    processed_count = SearchWorker(service).run_once()

    assert processed_count == 1
    broker.pop_search_job.assert_called_once()
    process_job.assert_called_once_with(job.id)


def test_search_worker_redis_mode_returns_zero_when_queue_empty(mocker):
    broker = MagicMock()
    broker.pop_search_job.return_value = None

    service = ResearchService(task_store=InMemoryTaskStore(), broker=broker)
    process_job = mocker.patch.object(service, "process_search_task_job")

    processed_count = SearchWorker(service).run_once()

    assert processed_count == 0
    process_job.assert_not_called()


def test_search_worker_redis_mode_skips_already_claimed_job(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    # Имитируем что другой воркер уже забрал джоб — claim вернёт None
    task_store.claim_search_task_job_by_id(job.id)

    broker = MagicMock()
    broker.pop_search_job.return_value = job.id

    service = ResearchService(task_store=task_store, broker=broker)
    process_job = mocker.patch.object(service, "process_search_task_job")

    processed_count = SearchWorker(service).run_once()

    assert processed_count == 0
    process_job.assert_not_called()


def test_search_worker_postgres_mode_used_when_no_broker(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    task_store.add_search_task_job("task-1", SearchDepth.EASY.value)

    service = ResearchService(task_store=task_store)
    process_job = mocker.patch.object(service, "process_search_task_job")

    processed_count = SearchWorker(service).run_once()

    assert processed_count == 1
    process_job.assert_called_once()
