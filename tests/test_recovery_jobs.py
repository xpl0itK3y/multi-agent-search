from datetime import datetime, timedelta, timezone

from src.api.schemas import (
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def test_in_memory_store_recovers_only_stale_running_search_jobs():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    stale = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    fresh = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    stale.status = SearchJobStatus.RUNNING
    stale.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    fresh.status = SearchJobStatus.RUNNING
    fresh.updated_at = datetime.now(timezone.utc)

    recovered = store.recover_stale_search_task_jobs(datetime.now(timezone.utc) - timedelta(minutes=5))

    assert [job.id for job in recovered] == [stale.id]
    assert stale.status == SearchJobStatus.PENDING
    assert fresh.status == SearchJobStatus.RUNNING


def test_in_memory_store_recovers_only_stale_running_finalize_jobs():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    stale = store.add_research_finalize_job(research.id)
    fresh = store.add_research_finalize_job(research.id)
    stale.status = stale.status.RUNNING
    stale.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    fresh.status = fresh.status.RUNNING
    fresh.updated_at = datetime.now(timezone.utc)

    recovered = store.recover_stale_research_finalize_jobs(datetime.now(timezone.utc) - timedelta(minutes=5))

    assert [job.id for job in recovered] == [stale.id]
    assert stale.status.value == "pending"
    assert fresh.status.value == "running"


def test_service_recovers_stale_search_jobs_and_resets_tasks(monkeypatch):
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    job.status = SearchJobStatus.RUNNING
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    service = ResearchService(task_store=store)

    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    result = service.recover_stale_search_task_jobs()

    assert result.recovered_count == 1
    assert result.recovered_job_ids == [job.id]
    task = store.get_task("task-1")
    assert task is not None
    assert task.status == TaskStatus.PENDING
    assert task.logs[-1] == "Recovered stale running search job"


def test_service_recovers_stale_finalize_jobs_and_preserves_analyzing(monkeypatch):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    store.update_research_status(research.id, ResearchStatus.ANALYZING)
    job = store.add_research_finalize_job(research.id)
    job.status = job.status.RUNNING
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    service = ResearchService(task_store=store)
    service.checkpoint_graph_state(
        research.id,
        {"step": "verify", "analyze_attempts": 1},
        {"step": "verify", "detail": "weak_support=True retry=True tie_break=False"},
    )

    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    result = service.recover_stale_research_finalize_jobs()

    assert result.recovered_count == 1
    assert result.recovered_job_ids == [job.id]
    current = store.get_research(research.id)
    assert current is not None
    assert current.status == ResearchStatus.ANALYZING
    assert current.graph_state["resume_after_stale_recovery"] is True
    assert current.graph_trail[-1]["step"] == "stale_recovered"
    assert "resume_from=verify" in current.graph_trail[-1]["detail"]
