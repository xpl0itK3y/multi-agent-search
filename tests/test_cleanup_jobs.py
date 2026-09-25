from datetime import datetime, timedelta, timezone

import pytest

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


def test_service_research_retention_deletes_only_old_terminal_researches(monkeypatch):
    from src.api.schemas import ResearchStatus

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)

    old_done = store.add_research(
        ResearchRequest(prompt="finished long ago", depth=SearchDepth.EASY), task_ids=[]
    )
    store.update_research_status(old_done.id, ResearchStatus.COMPLETED, "done")
    old_done_row = store.researches[old_done.id]
    old_done_row.updated_at = datetime.now(timezone.utc) - timedelta(days=30)

    fresh_done = store.add_research(
        ResearchRequest(prompt="finished recently", depth=SearchDepth.EASY), task_ids=[]
    )
    store.update_research_status(fresh_done.id, ResearchStatus.COMPLETED, "done")

    still_running = store.add_research(
        ResearchRequest(prompt="ancient but running", depth=SearchDepth.EASY), task_ids=[]
    )
    store.update_research_status(still_running.id, ResearchStatus.PROCESSING)
    store.researches[still_running.id].updated_at = datetime.now(timezone.utc) - timedelta(days=30)

    # Default: retention disabled — nothing is deleted.
    assert service.cleanup_old_researches() == []
    assert store.get_research(old_done.id) is not None

    monkeypatch.setattr(
        "src.services.research_service.settings.research_retention_seconds", 7 * 86400
    )
    deleted = service.cleanup_old_researches()

    assert deleted == [old_done.id]
    assert store.get_research(old_done.id) is None
    assert store.get_research(fresh_done.id) is not None
    assert store.get_research(still_running.id) is not None


def test_store_cleanup_old_researches_cascades_tasks():
    from src.api.schemas import ResearchStatus, TaskStatus

    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="cascade me", depth=SearchDepth.EASY), task_ids=[]
    )
    store.add_task(
        {
            "id": "task-cascade",
            "description": "search",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "research_id": research.id,
        }
    )
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "done")
    store.researches[research.id].updated_at = datetime.now(timezone.utc) - timedelta(days=30)

    deleted = store.cleanup_old_researches(datetime.now(timezone.utc) - timedelta(days=7))

    assert deleted == [research.id]
    assert store.get_task("task-cascade") is None


def test_maintenance_sweeps_telemetry_per_retention_setting(monkeypatch):
    from src.config import Settings, settings

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    store.create_user("u-ret", "u-ret@example.com", None)
    long_ago = datetime.now(timezone.utc) - timedelta(days=100)
    for when in (long_ago, datetime.now(timezone.utc)):
        store.record_user_event("tab_focus", "ui", user_id="u-ret")
        store.user_events[-1]["created_at"] = when
        store.record_user_session("u-ret", f"sess-{when.timestamp()}")
        store.user_sessions[-1]["last_active_at"] = when
        store.record_admin_audit("admin@example.com", "delete_user", "user")
        store.admin_audit_logs[-1].created_at = when

    # The defaults: 90 days for events and sessions, the admin audit trail kept forever.
    assert Settings.model_fields["user_events_retention_seconds"].default == 90 * 86400
    assert Settings.model_fields["user_sessions_retention_seconds"].default == 90 * 86400
    assert Settings.model_fields["admin_audit_retention_seconds"].default == 0
    monkeypatch.setattr(settings, "user_events_retention_seconds", 90 * 86400)
    monkeypatch.setattr(settings, "user_sessions_retention_seconds", 90 * 86400)
    monkeypatch.setattr(settings, "admin_audit_retention_seconds", 0)
    assert service.cleanup_old_telemetry() == {"user_events": 1, "user_sessions": 1}
    assert len(store.user_events) == len(store.user_sessions) == 1
    assert len(store.admin_audit_logs) == 2

    monkeypatch.setattr(settings, "admin_audit_retention_seconds", 30 * 86400)
    monkeypatch.setattr(settings, "user_events_retention_seconds", 0)
    store.record_user_event("tab_blur", "ui", user_id="u-ret")
    store.user_events[-1]["created_at"] = long_ago

    service.run_queue_maintenance()

    assert len(store.admin_audit_logs) == 1
    assert len(store.user_events) == 2  # 0 = keep forever


def test_a_failing_maintenance_step_does_not_starve_the_others(monkeypatch):
    """Research retention stuck on a huge backlog used to abort the pass before the
    telemetry sweeps and graph compaction, on every pass. Each step now runs; the first
    failure is still raised afterwards, so the worker heartbeat reports it."""
    from src.config import settings

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    store.create_user("u-steps", "u-steps@example.com", None)
    store.record_user_event("tab_focus", "ui", user_id="u-steps")
    store.user_events[-1]["created_at"] = datetime.now(timezone.utc) - timedelta(days=100)
    monkeypatch.setattr(settings, "user_events_retention_seconds", 90 * 86400)
    monkeypatch.setattr(settings, "research_retention_seconds", 86400)

    def stuck(older_than):
        raise RuntimeError("number of parameters must be between 0 and 65535")

    monkeypatch.setattr(store, "cleanup_old_researches", stuck)
    compacted = []
    monkeypatch.setattr(service, "compact_graph_operational_data", lambda: compacted.append(True) or ([], []))

    with pytest.raises(RuntimeError, match="65535"):
        service.run_queue_maintenance()

    assert store.user_events == []  # the telemetry sweep after it still ran
    assert compacted == [True]
