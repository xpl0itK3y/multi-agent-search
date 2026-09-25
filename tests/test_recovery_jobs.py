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
    assert stale.lease_epoch == 1
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
    assert current.graph_trail[-1]["metrics"] == {"resume_from": "verify"}
    assert "resuming after step verify" in current.graph_trail[-1]["detail"]


# STALE-FINALIZE-ENDED: recovery must not resurrect a research that ended while its job hung.


class _Analyzer:
    llm = None

    def __init__(self):
        self.calls = 0

    def run_analysis(self, prompt, tasks, **kwargs):
        self.calls += 1
        return "report after recovery"


class _Broker:
    def __init__(self):
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        pass

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


def _analyzing_with_stale_job(store):
    research = store.add_research(ResearchRequest(prompt="hung finalization", depth=SearchDepth.EASY), task_ids=[])
    store.add_task(
        {
            "id": f"t-{research.id[:8]}",
            "research_id": research.id,
            "description": "d",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com/a", "title": "A", "content": "Body " * 40}],
        }
    )
    store.set_research_task_ids(research.id, [f"t-{research.id[:8]}"])
    store.update_research_status(research.id, ResearchStatus.ANALYZING)
    job = store.add_research_finalize_job(research.id)
    store.claim_research_finalize_job_by_id(job.id)
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    return research, job


def test_stale_recovery_leaves_a_cancelled_research_cancelled_and_closes_its_job(monkeypatch):
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    store, broker, analyzer = InMemoryTaskStore(), _Broker(), _Analyzer()
    service = ResearchService(task_store=store, analyzer=analyzer, broker=broker)
    research, job = _analyzing_with_stale_job(store)
    service.cancel_research(research.id)

    result = service.run_queue_maintenance()

    current = store.get_research(research.id)
    assert current.status == ResearchStatus.CANCELLED
    assert "resume_after_stale_recovery" not in current.graph_state
    assert all(entry.get("step") != "stale_recovered" for entry in current.graph_trail)
    closed = store.get_research_finalize_job(job.id)
    assert closed.status.value == "completed" and closed.lease_epoch == 1
    assert result.recovered_finalize_job_ids == [] and broker.finalize == []
    assert service.process_finalize_job(job.id).status.value == "completed"
    assert analyzer.calls == 0
    assert store.get_research(research.id).status == ResearchStatus.CANCELLED


def test_a_cancel_racing_the_recovery_still_wins(monkeypatch):
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    store, broker, analyzer = InMemoryTaskStore(), _Broker(), _Analyzer()
    service = ResearchService(task_store=store, analyzer=analyzer, broker=broker)
    research, job = _analyzing_with_stale_job(store)
    recover = store.recover_stale_research_finalize_jobs

    def recover_then_cancel(stale_before):
        jobs = recover(stale_before)
        service.cancel_research(research.id)  # lands after the store requeued the job
        return jobs

    monkeypatch.setattr(store, "recover_stale_research_finalize_jobs", recover_then_cancel)

    service.recover_stale_research_finalize_jobs()

    assert store.get_research(research.id).status == ResearchStatus.CANCELLED
    assert broker.finalize == [job.id]
    service.process_finalize_job(job.id)
    assert analyzer.calls == 0
    assert store.get_research_finalize_job(job.id).status.value == "completed"
    assert store.get_research(research.id).status == ResearchStatus.CANCELLED


def test_stale_recovery_of_a_live_research_still_resumes_it(monkeypatch):
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    store, broker, analyzer = InMemoryTaskStore(), _Broker(), _Analyzer()
    service = ResearchService(task_store=store, analyzer=analyzer, broker=broker)
    research, job = _analyzing_with_stale_job(store)

    assert service.recover_stale_research_finalize_jobs().recovered_job_ids == [job.id]
    assert broker.finalize == [job.id]
    service.process_finalize_job(job.id)

    final = store.get_research(research.id)
    assert final.status == ResearchStatus.COMPLETED and analyzer.calls == 1
