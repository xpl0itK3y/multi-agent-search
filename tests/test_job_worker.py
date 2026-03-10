from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import JobWorker


def test_job_worker_updates_heartbeat_when_idle():
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)

    processed = JobWorker(service, worker_name="job-worker").run_once()

    heartbeat = task_store.get_worker_heartbeat("job-worker")
    assert processed == 0
    assert heartbeat is not None
    assert heartbeat.status == "idle"
    assert heartbeat.processed_jobs == 0
