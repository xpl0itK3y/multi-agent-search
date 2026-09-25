"""ADMIN-MAINTENANCE: /v1/admin/operations/* run through the service job paths.

Execution used to call raw store primitives: no broker push (so in Redis mode a
recovered/requeued job was never picked up), no dead-letter guard, no task/research
status reset, no lease bump, and a default stale window (300 s) below the finalize job
timeout (600 s). The in-memory store only previewed. The request was an untyped dict.
"""
from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import (
    FinalizeJobStatus,
    MaintenanceActionRequest,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
)
from src.config import settings
from src.domain.errors import ConflictError, UnprocessableError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _FakeBroker:
    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


class _StubAnalyzer:
    def run_analysis(self, prompt, tasks, **kwargs):
        return "report"


def _service():
    store, broker = InMemoryTaskStore(), _FakeBroker()
    return store, broker, ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)


def _execute(service, action, **params):
    return service.execute_maintenance_action(
        MaintenanceActionRequest(action=action, params=params), actor_email="admin@example.com"
    )


def _running_finalize_job(store, age_seconds, max_attempts=3, status=ResearchStatus.FAILED):
    research = store.add_research(ResearchRequest(prompt="maintenance topic", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(research.id, status, "stuck")
    job = store.add_research_finalize_job(research.id, max_attempts=max_attempts)
    store.claim_research_finalize_job_by_id(job.id)
    job.updated_at = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return research, job


def test_recover_stale_finalize_redispatches_and_fences_the_old_runner():
    store, broker, service = _service()
    # A hung finalization leaves its research ANALYZING; recovery never revives an ended
    # (failed/cancelled/completed) one, it closes that job instead.
    research, stale = _running_finalize_job(
        store, settings.finalize_job_timeout_seconds + 60, status=ResearchStatus.ANALYZING
    )
    _, busy = _running_finalize_job(store, settings.finalize_job_timeout_seconds - 60)

    result = _execute(service, "recover_stale_finalize_jobs")

    assert result.dry_run is False and result.affected_count == 1
    assert result.sample_affected_ids == [stale.id]
    recovered = store.get_research_finalize_job(stale.id)
    assert recovered.status == FinalizeJobStatus.PENDING and recovered.lease_epoch == 1
    assert broker.finalize == [stale.id]
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    # The default window is the finalize timeout, so a job younger than it keeps running.
    assert store.get_research_finalize_job(busy.id).status == FinalizeJobStatus.RUNNING
    audit = store.get_admin_audit_logs()[0]
    assert audit.action == "recover_stale_finalize_jobs"
    assert audit.details["params"] == {"stale_seconds": settings.finalize_job_timeout_seconds}


def test_recover_stale_search_resets_the_task_and_pushes_the_job():
    store, broker, service = _service()
    store.add_task({"id": "t1", "description": "d", "queries": ["q"], "status": TaskStatus.RUNNING})
    job = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(job.id)
    job.updated_at = datetime.now(timezone.utc) - timedelta(seconds=settings.search_job_timeout_seconds + 60)

    result = _execute(service, "recover_stale_search_jobs")

    assert result.affected_count == 1
    assert store.get_search_task_job(job.id).status == SearchJobStatus.PENDING
    assert store.get_task("t1").status == TaskStatus.PENDING
    assert broker.search == [job.id]


def test_stale_window_below_the_job_timeout_is_refused():
    store, _, service = _service()
    _, job = _running_finalize_job(store, settings.finalize_job_timeout_seconds - 60)
    request = MaintenanceActionRequest(
        action="recover_stale_finalize_jobs",
        params={"stale_seconds": settings.finalize_job_timeout_seconds - 1},
    )

    with pytest.raises(UnprocessableError):
        service.preview_maintenance_action(request)
    with pytest.raises(UnprocessableError):
        service.execute_maintenance_action(request, actor_email="admin@example.com")
    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.RUNNING
    assert store.get_admin_audit_logs() == []


def test_requeue_refuses_a_live_job_and_requeues_a_dead_letter_one():
    store, broker, service = _service()
    research, running = _running_finalize_job(store, 1, max_attempts=1)

    with pytest.raises(ConflictError):
        _execute(service, "requeue_finalize_job", target_id=running.id)
    assert store.get_research_finalize_job(running.id).status == FinalizeJobStatus.RUNNING
    assert broker.finalize == []

    assert store.record_research_finalize_job_failure(running.id, "boom").status == FinalizeJobStatus.DEAD_LETTER
    result = _execute(service, "requeue_finalize_job", target_id=running.id)

    assert result.affected_count == 1
    requeued = store.get_research_finalize_job(running.id)
    assert requeued.status == FinalizeJobStatus.PENDING and requeued.lease_epoch == 1
    assert broker.finalize == [running.id]
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING


def test_requeue_search_job_resets_its_task():
    store, broker, service = _service()
    store.add_task({"id": "t1", "description": "d", "queries": ["q"], "status": TaskStatus.FAILED})
    job = store.add_search_task_job("t1", SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    store.record_search_task_job_failure(job.id, "boom")

    _execute(service, "requeue_search_job", target_id=job.id)

    assert store.get_search_task_job(job.id).status == SearchJobStatus.PENDING
    assert store.get_task("t1").status == TaskStatus.PENDING
    assert broker.search == [job.id]


def test_in_memory_cleanup_executes_for_real():
    store, _, service = _service()
    store.add_task({"id": "t1", "description": "d", "queries": ["q"]})
    old = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.update_search_task_job(old.id, SearchJobStatus.COMPLETED)
    old.updated_at = datetime.now(timezone.utc) - timedelta(days=10)
    fresh = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.put_cached_search("stale-key", [{"url": "https://a.com"}])
    store.search_cache["stale-key"] = (datetime.now(timezone.utc) - timedelta(days=5), [])

    assert service.preview_maintenance_action(MaintenanceActionRequest(action="cleanup_search_cache")).affected_count == 1
    jobs = _execute(service, "cleanup_old_jobs", days=7)
    cache = _execute(service, "cleanup_search_cache")

    assert jobs.affected_count == 1 and store.get_search_task_job(old.id) is None
    assert store.get_search_task_job(fresh.id) is not None
    assert cache.affected_count == 1 and "stale-key" not in store.search_cache


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "drop_database"},
        {"action": "requeue_finalize_job"},
        {"action": "requeue_search_job", "params": {"target_id": "x" * 65}},
        {"action": "cleanup_old_jobs", "params": {"days": 0}},
        {"action": "cleanup_old_jobs", "params": {"unexpected": 1}},
        {"action": "recover_stale_finalize_jobs", "params": {"stale_seconds": 300}},
    ],
)
@pytest.mark.anyio
async def test_operations_routes_validate_the_request(client, payload):
    for path in ("/v1/admin/operations/preview", "/v1/admin/operations/execute"):
        response = await client.post(path, json=payload)
        assert response.status_code == 422, (path, response.text)


@pytest.mark.anyio
async def test_operations_preview_uses_the_job_timeout_by_default(client):
    response = await client.post("/v1/admin/operations/preview", json={"action": "recover_stale_search_jobs"})

    assert response.status_code == 200
    assert response.json()["dry_run"] is True
