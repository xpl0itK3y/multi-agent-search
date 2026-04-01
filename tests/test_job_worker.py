from src.repositories import InMemoryTaskStore
from src.providers.search import reset_extraction_metrics
from src.services import ResearchService
from src.workers import JobWorker


def test_job_worker_updates_heartbeat_when_idle():
    reset_extraction_metrics()
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)

    processed = JobWorker(service, worker_name="job-worker").run_once()

    heartbeat = task_store.get_worker_heartbeat("job-worker")
    assert processed == 0
    assert heartbeat is not None
    assert heartbeat.status == "idle"
    assert heartbeat.processed_jobs == 0
    assert heartbeat.extraction_metrics.attempts == 0


def test_job_worker_counts_recovered_jobs_as_work(mocker):
    reset_extraction_metrics()
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    mocker.patch("src.workers.job_worker.MaintenanceWorker.run_once", return_value=4)
    mocker.patch("src.workers.job_worker.SearchWorker.run_once", return_value=0)
    mocker.patch("src.workers.job_worker.FinalizeWorker.run_once", return_value=0)

    processed = JobWorker(service, worker_name="job-worker").run_once()

    heartbeat = task_store.get_worker_heartbeat("job-worker")
    assert processed == 4
    assert heartbeat is not None
    assert heartbeat.status == "busy"
    assert heartbeat.processed_jobs == 4
    assert heartbeat.extraction_metrics.attempts == 0
