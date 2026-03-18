from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.repositories import SQLAlchemyTaskStore
from src.services.research_service import ResearchService


class StubOrchestrator:
    def run_decompose(self, prompt: str, depth: SearchDepth):
        return [
            {
                "id": "task-1",
                "description": "collect source one",
                "queries": ["query one"],
                "status": TaskStatus.PENDING,
            },
            {
                "id": "task-2",
                "description": "collect source two",
                "queries": ["query two"],
                "status": TaskStatus.PENDING,
            },
        ]


class StubAnalyzer:
    def run_analysis(self, prompt: str, tasks):
        assert prompt == "postgres lifecycle"
        assert len(tasks) == 2
        return "postgres final report"


@pytest.mark.postgres
def test_research_service_full_postgres_lifecycle(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    service = ResearchService(
        task_store=store,
        orchestrator=StubOrchestrator(),
        analyzer=StubAnalyzer(),
    )

    response = service.start_research(
        ResearchRequest(prompt="postgres lifecycle", depth=SearchDepth.EASY),
    )

    tasks = store.get_tasks_by_research(response.research_id)
    assert len(tasks) == 2

    for task in tasks:
        updated = store.update_task(
            task.id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": f"https://{task.id}.example", "title": task.description, "content": "body"}],
                log="completed for lifecycle test",
            ),
        )
        assert updated is not None
        assert updated.status == TaskStatus.COMPLETED

    current = service.get_research_status(response.research_id)
    assert current.status == ResearchStatus.PROCESSING
    assert current.task_ids == ["task-1", "task-2"]

    finalized = service.finalize_research(response.research_id)
    assert finalized.status == ResearchStatus.COMPLETED
    assert finalized.final_report == "postgres final report"
    assert finalized.task_ids == ["task-1", "task-2"]


@pytest.mark.postgres
def test_research_service_postgres_admin_job_operations(postgres_session_factory, monkeypatch):
    store = SQLAlchemyTaskStore(postgres_session_factory)
    service = ResearchService(
        task_store=store,
        analyzer=StubAnalyzer(),
    )

    research = store.add_research(
        ResearchRequest(prompt="postgres lifecycle", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = "task-admin"
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    )

    search_job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    store.claim_next_search_task_job()
    store.record_search_task_job_failure(search_job.id, "boom")
    requeued_search = service.requeue_search_task_job(search_job.id)
    assert requeued_search.status == SearchJobStatus.PENDING

    finalize_job = store.add_research_finalize_job(research.id, max_attempts=1)
    store.claim_next_research_finalize_job()
    store.record_research_finalize_job_failure(finalize_job.id, "boom")
    requeued_finalize = service.requeue_research_finalize_job(finalize_job.id)
    assert requeued_finalize.status == FinalizeJobStatus.PENDING

    running_search = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    store.update_search_task_job(running_search.id, SearchJobStatus.RUNNING)
    running_finalize = store.add_research_finalize_job(research.id)
    store.update_research_finalize_job(running_finalize.id, FinalizeJobStatus.RUNNING)

    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", -1)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", -1)
    recovered_search = service.recover_stale_search_task_jobs()
    recovered_finalize = service.recover_stale_research_finalize_jobs()
    assert running_search.id in recovered_search.recovered_job_ids
    assert running_finalize.id in recovered_finalize.recovered_job_ids

    old_search = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    old_finalize = store.add_research_finalize_job(research.id)
    store.update_search_task_job(old_search.id, SearchJobStatus.COMPLETED)
    store.update_research_finalize_job(old_finalize.id, FinalizeJobStatus.DEAD_LETTER, "boom")
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", -1)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", -1)
    cleaned_search = service.cleanup_old_search_task_jobs()
    cleaned_finalize = service.cleanup_old_research_finalize_jobs()
    assert old_search.id in cleaned_search.deleted_job_ids
    assert old_finalize.id in cleaned_finalize.deleted_job_ids
