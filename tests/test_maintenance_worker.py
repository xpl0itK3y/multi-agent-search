from datetime import datetime, timedelta, timezone

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
from src.workers import MaintenanceWorker


def test_maintenance_worker_recovers_stale_search_and_finalize_jobs(monkeypatch):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    search_job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    search_job.status = SearchJobStatus.RUNNING
    search_job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    finalize_job = task_store.add_research_finalize_job(research.id)
    finalize_job.status = FinalizeJobStatus.RUNNING
    finalize_job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)

    recovered_count = MaintenanceWorker(service).run_once()

    assert recovered_count == 2
    assert task_store.get_search_task_job(search_job.id).status == SearchJobStatus.PENDING
    assert task_store.get_research_finalize_job(finalize_job.id).status == FinalizeJobStatus.PENDING
    assert task_store.get_task("task-1").status == TaskStatus.PENDING
    assert task_store.get_research(research.id).status == ResearchStatus.ANALYZING


def test_maintenance_worker_is_idle_when_nothing_is_stale(monkeypatch):
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)

    recovered_count = MaintenanceWorker(service).run_once()

    assert recovered_count == 0
