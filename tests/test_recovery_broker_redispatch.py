"""AUD-006: stale-recovered jobs must be re-pushed to the broker (Redis mode has no
Postgres poll fallback, so a reset-to-PENDING job would otherwise never be claimed)."""
from datetime import datetime, timedelta, timezone

from src.api.schemas import ResearchRequest, SearchDepth, SearchJobStatus, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _FakeBroker:
    def __init__(self):
        self.search_pushes: list[str] = []
        self.finalize_pushes: list[str] = []

    def push_search_job(self, job_id):
        self.search_pushes.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize_pushes.append(job_id)


def test_recovered_search_job_is_redispatched_to_broker(monkeypatch):
    store = InMemoryTaskStore()
    store.add_task({"id": "task-1", "description": "t", "queries": ["q"], "status": TaskStatus.RUNNING})
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    job.status = SearchJobStatus.RUNNING
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    broker = _FakeBroker()
    service = ResearchService(task_store=store, broker=broker)

    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    service.recover_stale_search_task_jobs()

    assert broker.search_pushes == [job.id]


def test_recovered_finalize_job_is_redispatched_to_broker(monkeypatch):
    store = InMemoryTaskStore()
    research = store.add_research(ResearchRequest(prompt="topic here", depth=SearchDepth.EASY), task_ids=[])
    job = store.add_research_finalize_job(research.id)
    job.status = job.status.RUNNING
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    broker = _FakeBroker()
    service = ResearchService(task_store=store, broker=broker)

    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    service.recover_stale_research_finalize_jobs()

    assert broker.finalize_pushes == [job.id]
