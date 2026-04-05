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
from src.graph.history import compact_graph_trail


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


def test_maintenance_worker_cleans_up_old_completed_and_dead_letter_jobs(monkeypatch):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    old_search = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    old_search.status = SearchJobStatus.COMPLETED
    old_search.updated_at = datetime.now(timezone.utc) - timedelta(days=2)

    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    old_finalize = task_store.add_research_finalize_job(research.id)
    old_finalize.status = FinalizeJobStatus.DEAD_LETTER
    old_finalize.updated_at = datetime.now(timezone.utc) - timedelta(days=2)

    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)

    processed_count = MaintenanceWorker(service).run_once()

    assert processed_count == 2
    assert task_store.get_search_task_job(old_search.id) is None
    assert task_store.get_research_finalize_job(old_finalize.id) is None


def test_maintenance_worker_combines_recovery_and_cleanup(monkeypatch):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    stale_search = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    stale_search.status = SearchJobStatus.RUNNING
    stale_search.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    old_search = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    old_search.status = SearchJobStatus.COMPLETED
    old_search.updated_at = datetime.now(timezone.utc) - timedelta(days=2)

    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    stale_finalize = task_store.add_research_finalize_job(research.id)
    stale_finalize.status = FinalizeJobStatus.RUNNING
    stale_finalize.updated_at = datetime.now(timezone.utc) - timedelta(days=2)
    old_finalize = task_store.add_research_finalize_job(research.id)
    old_finalize.status = FinalizeJobStatus.DEAD_LETTER
    old_finalize.updated_at = datetime.now(timezone.utc) - timedelta(days=2)

    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)

    processed_count = MaintenanceWorker(service).run_once()

    assert processed_count == 4


def test_maintenance_worker_is_idle_when_nothing_is_stale(monkeypatch):
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)

    recovered_count = MaintenanceWorker(service).run_once()

    assert recovered_count == 0


def test_maintenance_worker_compacts_graph_operational_data(monkeypatch):
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)
    monkeypatch.setattr("src.graph.history.settings.graph_step_event_retention_seconds", 60)
    monkeypatch.setattr("src.graph.history.settings.graph_trail_retention_seconds", 60)

    old_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=1,
        status="busy",
        graph_step_events=[
            {"timestamp": datetime.now(timezone.utc).isoformat(), "step": "verify", "elapsed_ms": 120.0, "worker_name": "job-worker"},
        ],
    )
    task_store.worker_graph_step_events["job-worker"].append(
        {"timestamp": old_ts, "step": "analyze", "elapsed_ms": 100.0, "worker_name": "job-worker"}
    )
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    research.graph_trail = compact_graph_trail(
        [],
        [
            {"timestamp": old_ts, "step": "collect_context", "detail": "old"},
            {"timestamp": datetime.now(timezone.utc).isoformat(), "step": "verify", "detail": "fresh"},
        ],
    ) + [{"timestamp": old_ts, "step": "collect_context", "detail": "old"}]

    processed_count = MaintenanceWorker(service).run_once()

    assert processed_count == 2
    assert task_store.get_graph_step_events("job-worker")[0]["step"] == "verify"
    assert task_store.get_research(research.id).graph_trail[0]["step"] == "verify"


def test_maintenance_worker_persists_last_maintenance_summary(monkeypatch):
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)

    MaintenanceWorker(service).run_once()

    heartbeat = task_store.get_worker_heartbeat("maintenance")
    assert heartbeat is not None
    assert heartbeat.maintenance_summary.last_run_at is not None
    assert heartbeat.maintenance_summary.total_count == 0
    assert len(heartbeat.maintenance_summary.recent_runs) == 1


def test_maintenance_worker_appends_maintenance_history(monkeypatch):
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store)
    monkeypatch.setattr("src.services.research_service.settings.search_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_timeout_seconds", 60)
    monkeypatch.setattr("src.services.research_service.settings.search_job_retention_seconds", 3600)
    monkeypatch.setattr("src.services.research_service.settings.finalize_job_retention_seconds", 3600)

    MaintenanceWorker(service).run_once()
    MaintenanceWorker(service).run_once()

    heartbeat = task_store.get_worker_heartbeat("maintenance")
    assert heartbeat is not None
    assert len(heartbeat.maintenance_summary.recent_runs) == 2
    assert heartbeat.maintenance_summary.trend.cleanup_volume_direction == "stable"
    assert len(heartbeat.maintenance_summary.recent_operational_health) == 2
