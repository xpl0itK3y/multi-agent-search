from datetime import datetime, timedelta, timezone

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    SearchDepth,
    SearchJobStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def test_in_memory_store_cleans_up_old_completed_and_dead_letter_search_jobs():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    old_completed = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    old_dead = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    fresh_completed = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    old_completed.status = SearchJobStatus.COMPLETED
    old_dead.status = SearchJobStatus.DEAD_LETTER
    fresh_completed.status = SearchJobStatus.COMPLETED
    old_completed.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    old_dead.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    fresh_completed.updated_at = datetime.now(timezone.utc)

    deleted = store.cleanup_old_search_task_jobs(datetime.now(timezone.utc) - timedelta(days=1))

    assert set(deleted) == {old_completed.id, old_dead.id}
    assert store.get_search_task_job(fresh_completed.id) is not None


def test_in_memory_store_cleans_up_old_completed_and_dead_letter_finalize_jobs():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    old_completed = store.add_research_finalize_job(research.id)
    old_dead = store.add_research_finalize_job(research.id)
    fresh_completed = store.add_research_finalize_job(research.id)
    old_completed.status = FinalizeJobStatus.COMPLETED
    old_dead.status = FinalizeJobStatus.DEAD_LETTER
    fresh_completed.status = FinalizeJobStatus.COMPLETED
    old_completed.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    old_dead.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    fresh_completed.updated_at = datetime.now(timezone.utc)

    deleted = store.cleanup_old_research_finalize_jobs(datetime.now(timezone.utc) - timedelta(days=1))

    assert set(deleted) == {old_completed.id, old_dead.id}
    assert store.get_research_finalize_job(fresh_completed.id) is not None


def test_service_cleans_up_old_search_jobs(monkeypatch):
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    old_job = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    old_job.status = SearchJobStatus.COMPLETED
    old_job.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    service = ResearchService(task_store=store)

    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    result = service.cleanup_old_search_task_jobs()

    assert result.deleted_count == 1
    assert result.deleted_job_ids == [old_job.id]
    assert store.get_search_task_job(old_job.id) is None


def test_service_cleans_up_old_finalize_jobs(monkeypatch):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    old_job = store.add_research_finalize_job(research.id)
    old_job.status = FinalizeJobStatus.DEAD_LETTER
    old_job.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    service = ResearchService(task_store=store)

    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)
    result = service.cleanup_old_research_finalize_jobs()

    assert result.deleted_count == 1
    assert result.deleted_job_ids == [old_job.id]
    assert store.get_research_finalize_job(old_job.id) is None
