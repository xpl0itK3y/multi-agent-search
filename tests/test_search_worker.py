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
