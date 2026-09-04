import httpx
import pytest

from src.api.app import create_app
from src.domain.errors import ConflictError
from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.graph.metrics import record_graph_step, get_graph_metrics_snapshot, get_graph_step_events_snapshot, reset_graph_metrics
from src.observability.metrics import metric_route_template
from src.observability.context import bind_observability_context
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class StubOptimizer:
    def run(self, prompt: str) -> str:
        return f"optimized::{prompt}"


class StubOrchestrator:
    def __init__(self, tasks=None):
        self.tasks = tasks or [
            {
                "id": "task-1",
                "description": "Search for X",
                "queries": ["query X"],
                "status": TaskStatus.PENDING,
            }
        ]

    def run_decompose(self, prompt: str, depth: SearchDepth):
        return [dict(task) for task in self.tasks]


class StubAnalyzer:
    def __init__(self, report: str = "Final structured report"):
        self.report = report

    def run_analysis(self, prompt: str, tasks, depth=None):
        return self.report


@pytest.fixture
async def client():
    reset_graph_metrics()
    app = create_app()
    service = ResearchService(
        task_store=InMemoryTaskStore(),
        optimizer=StubOptimizer(),
        orchestrator=StubOrchestrator(),
        analyzer=StubAnalyzer(),
    )

    async with app.router.lifespan_context(app):
        app.state.research_service = service
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
            yield test_client


@pytest.mark.anyio
async def test_health_check(client):
    app_service = client._transport.app.state.research_service
    with bind_observability_context(worker_name="job-worker"):
        record_graph_step("analyze", 9000.0, research_id="research-health")
    app_service.task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=1,
        status="busy",
        graph_metrics=get_graph_metrics_snapshot(),
        graph_step_events=get_graph_step_events_snapshot(),
    )
    response = await client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "extraction_metrics" in payload
    assert "graph_metrics" in payload
    assert "graph_alerts" in payload
    assert "graph_alert_trend" in payload
    assert payload["extraction_metrics"]["attempts"] >= 0
    assert any(alert["code"] == "high_avg_ms" for alert in payload["graph_alerts"])
    assert "job-worker" in payload["graph_alert_trend"]["top_worker_names"]
    assert response.headers["X-Request-ID"]


@pytest.mark.anyio
async def test_run_queue_maintenance_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-maint",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    stale_search = app_service.task_store.add_search_task_job("task-maint", SearchDepth.EASY.value)
    stale_search.status = SearchJobStatus.RUNNING
    stale_search.updated_at = stale_search.updated_at.replace(year=2020)

    research = app_service.task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    app_service.task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    stale_finalize = app_service.task_store.add_research_finalize_job(research.id)
    stale_finalize.status = FinalizeJobStatus.RUNNING
    stale_finalize.updated_at = stale_finalize.updated_at.replace(year=2020)

    response = await client.post("/health/queues/maintenance")

    assert response.status_code == 200
    assert response.json()["recovered_count"] == 2
    assert response.json()["deleted_count"] == 0
    assert response.json()["total_count"] == 2
    assert response.json()["recovered_search_job_ids"] == [stale_search.id]
    assert response.json()["recovered_finalize_job_ids"] == [stale_finalize.id]


@pytest.mark.anyio
async def test_metrics_endpoint(client):
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]


@pytest.mark.anyio
async def test_start_research_preserves_service_error_status(client, mocker):
    service = client._transport.app.state.research_service
    mocker.patch.object(service, "start_research", side_effect=ConflictError("Research already running"))

    response = await client.post("/v1/research", json={"prompt": "A valid research prompt"})

    assert response.status_code == 409
    assert response.json() == {"detail": "Research already running"}


def test_metric_route_template_never_uses_a_raw_path():
    class Route:
        path = "/v1/public/{share_token}"

    assert metric_route_template(Route()) == "/v1/public/{share_token}"
    assert metric_route_template(None) == "unmatched"


@pytest.mark.anyio
async def test_queue_health_includes_extraction_metrics(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=1,
        status="busy",
        extraction_metrics={"attempts": 5, "success_count": 4, "failure_count": 1},
        graph_metrics={
            "resume_count": 2,
            "replan_pass_count": 1,
            "steps": {
                "collect_context": {"run_count": 2, "failure_count": 1, "total_ms": 40.0},
            },
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    assert payload["extraction_metrics"]["attempts"] == 5
    assert payload["extraction_metrics"]["success_count"] == 4
    assert payload["extraction_metrics"]["failure_count"] == 1
    assert payload["graph_metrics"]["resume_count"] == 2
    assert payload["graph_metrics"]["replan_pass_count"] == 1
    assert payload["graph_metrics"]["steps"]["collect_context"]["run_count"] == 2
    assert payload["graph_metrics"]["steps"]["collect_context"]["failure_count"] == 1
    assert payload["graph_metrics"]["steps"]["collect_context"]["avg_ms"] == 20.0
    step_failure_alert = next(alert for alert in payload["graph_alerts"] if alert["code"] == "step_failures")
    assert "hint" in step_failure_alert
    assert "Inspect logs" in step_failure_alert["hint"] or "Inspect source critic" in step_failure_alert["hint"]


@pytest.mark.anyio
async def test_queue_health_includes_maintenance_summary(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=3,
        status="busy",
        maintenance_summary={
            "compacted_count": 2,
            "total_count": 3,
            "compacted_graph_event_worker_names": ["job-worker"],
            "compacted_graph_trail_research_ids": ["research-1"],
            "last_run_at": "2026-04-05T12:00:00+00:00",
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    assert payload["maintenance_summary"]["compacted_count"] == 2
    assert payload["maintenance_summary"]["compacted_graph_event_worker_names"] == ["job-worker"]
    assert payload["maintenance_summary"]["compacted_graph_trail_research_ids"] == ["research-1"]
    assert payload["maintenance_summary"]["recent_runs"] == []


@pytest.mark.anyio
async def test_queue_health_includes_maintenance_history(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=3,
        status="busy",
        maintenance_summary={
            "total_count": 3,
            "last_run_at": "2026-04-05T12:00:00+00:00",
            "recent_runs": [
                {
                    "recovered_count": 1,
                    "deleted_count": 0,
                    "compacted_count": 2,
                    "total_count": 3,
                    "last_run_at": "2026-04-05T12:00:00+00:00",
                },
                {
                    "recovered_count": 0,
                    "deleted_count": 1,
                    "compacted_count": 1,
                    "total_count": 2,
                    "last_run_at": "2026-04-05T12:05:00+00:00",
                },
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["maintenance_summary"]["recent_runs"]) == 2
    assert payload["maintenance_summary"]["recent_runs"][1]["deleted_count"] == 1


@pytest.mark.anyio
async def test_queue_health_includes_maintenance_trend(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=8,
        status="busy",
        maintenance_summary={
            "total_count": 8,
            "last_run_at": "2026-04-05T12:30:00+00:00",
            "recent_runs": [
                {"total_count": 2, "compacted_count": 0, "last_run_at": "2026-04-05T12:00:00+00:00"},
                {"total_count": 4, "compacted_count": 1, "last_run_at": "2026-04-05T12:05:00+00:00"},
                {"total_count": 8, "compacted_count": 2, "last_run_at": "2026-04-05T12:10:00+00:00"},
                {"total_count": 12, "compacted_count": 3, "last_run_at": "2026-04-05T12:15:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    trend = payload["maintenance_summary"]["trend"]
    assert trend["cleanup_volume_direction"] == "growing"
    assert trend["average_compacted_count"] == 1.5
    assert trend["recent_total_counts"] == [2, 4, 8, 12]
    assert trend["recent_compacted_counts"] == [0, 1, 2, 3]
    alert_codes = {item["code"] for item in payload["maintenance_summary"]["alerts"]}
    assert "cleanup_volume_growing" in alert_codes
    assert payload["operational_health"]["status"] in {"warning", "critical"}
    assert payload["operational_health"]["score"] < 100
    assert any(reason.startswith("maintenance:") for reason in payload["operational_health"]["reasons"])
    assert payload["operational_health"]["trend"]["score_direction"] in {"stable", "worsening", "improving"}
    assert payload["operational_health"]["trend"]["recent_scores"]


@pytest.mark.anyio
async def test_queue_health_includes_maintenance_stale_alert(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_runs": [
                {"total_count": 1, "compacted_count": 0, "last_run_at": "2026-04-05T09:00:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    alert_codes = {item["code"] for item in payload["maintenance_summary"]["alerts"]}
    assert "maintenance_stale" in alert_codes
    assert any(reason == "maintenance:maintenance_stale" for reason in payload["operational_health"]["reasons"])


@pytest.mark.anyio
async def test_worker_health_includes_operational_health(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=1,
        status="busy",
        graph_metrics={
            "steps": {
                "analyze": {"run_count": 5, "failure_count": 3, "total_ms": 30000.0},
            },
        },
    )

    response = await client.get("/health/workers/job-worker")

    assert response.status_code == 200
    payload = response.json()
    assert payload["operational_health"]["status"] == "critical"
    assert payload["operational_health"]["score"] <= 50


@pytest.mark.anyio
async def test_queue_health_includes_operational_health_history(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=3,
        status="busy",
        maintenance_summary={
            "last_run_at": "2026-04-05T12:10:00+00:00",
            "recent_operational_health": [
                {
                    "status": "healthy",
                    "score": 96,
                    "reasons": [],
                    "timestamp": "2026-04-05T12:00:00+00:00",
                },
                {
                    "status": "warning",
                    "score": 78,
                    "reasons": ["maintenance:cleanup_volume_growing"],
                    "timestamp": "2026-04-05T12:05:00+00:00",
                },
            ],
            "recent_runs": [
                {"total_count": 2, "compacted_count": 0, "last_run_at": "2026-04-05T12:00:00+00:00"},
                {"total_count": 7, "compacted_count": 3, "last_run_at": "2026-04-05T12:05:00+00:00"},
                {"total_count": 9, "compacted_count": 4, "last_run_at": "2026-04-05T12:10:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    history = payload["operational_health"]["history"]
    assert len(history) >= 3
    assert history[-1]["timestamp"].startswith("2026-04-05T12:10:00")
    assert payload["operational_health"]["trend"]["recent_scores"]


@pytest.mark.anyio
async def test_queue_health_includes_operational_health_meta_alerts(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=3,
        status="busy",
        maintenance_summary={
            "last_run_at": "2026-04-05T12:10:00+00:00",
            "recent_operational_health": [
                {"status": "healthy", "score": 98, "timestamp": "2026-04-05T12:00:00+00:00"},
                {"status": "warning", "score": 84, "timestamp": "2026-04-05T12:02:00+00:00"},
                {"status": "critical", "score": 58, "timestamp": "2026-04-05T12:04:00+00:00"},
                {"status": "critical", "score": 42, "timestamp": "2026-04-05T12:06:00+00:00"},
            ],
            "recent_runs": [
                {"total_count": 2, "compacted_count": 0, "last_run_at": "2026-04-05T12:00:00+00:00"},
                {"total_count": 9, "compacted_count": 4, "last_run_at": "2026-04-05T12:05:00+00:00"},
                {"total_count": 11, "compacted_count": 5, "last_run_at": "2026-04-05T12:10:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    alert_codes = {item["code"] for item in payload["operational_health"]["alerts"]}
    assert "score_worsening" in alert_codes
    assert "repeated_critical_states" in alert_codes
    recommendations = payload["operational_health"]["recommendations"]
    assert any("worker parallelism" in item["message"] or "more workers" in item["message"] for item in recommendations)
    assert any("queue backlog" in item["message"] and "graph retries" in item["message"] for item in recommendations)
    assert all("shown_count" in item for item in recommendations)


@pytest.mark.anyio
async def test_queue_health_includes_operational_recovery_alert(client):
    # Recent timestamps so maintenance isn't flagged stale (which would degrade the
    # current health and suppress the recovery alert this test checks for).
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": now.isoformat(),
            "recent_operational_health": [
                {"status": "critical", "score": 40, "timestamp": (now - timedelta(minutes=10)).isoformat()},
                {"status": "warning", "score": 72, "timestamp": (now - timedelta(minutes=5)).isoformat()},
            ],
            "recent_runs": [
                {"total_count": 1, "compacted_count": 0, "last_run_at": (now - timedelta(minutes=10)).isoformat()},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    alert_codes = {item["code"] for item in payload["operational_health"]["alerts"]}
    assert "score_recovered" in alert_codes


@pytest.mark.anyio
async def test_queue_health_includes_maintenance_restart_recommendation(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_runs": [
                {"total_count": 1, "compacted_count": 0, "last_run_at": "2026-04-05T09:00:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    recommendations = payload["operational_health"]["recommendations"]
    assert any("Maintenance appears stale" in item["message"] for item in recommendations)


@pytest.mark.anyio
async def test_queue_health_includes_runbook_analytics_and_alerts(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T12:00:00+00:00",
            "recent_operational_recommendations": [
                {
                    "code": "reduce_queue_backlog",
                    "message": "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                    "shown_count": 4,
                    "active": True,
                    "acknowledged": False,
                    "resolved": False,
                    "first_shown_at": "2026-04-04T06:00:00+00:00",
                    "last_shown_at": "2026-04-05T11:00:00+00:00",
                },
                {
                    "code": "inspect_graph_failures_and_search_quality",
                    "message": "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                    "shown_count": 3,
                    "active": True,
                    "acknowledged": True,
                    "acknowledged_at": "2026-04-05T11:30:00+00:00",
                    "resolved": False,
                    "first_shown_at": "2026-04-05T08:00:00+00:00",
                    "last_shown_at": "2026-04-05T11:00:00+00:00",
                },
                {
                    "code": "restart_or_verify_maintenance_path",
                    "message": "Maintenance appears stale: verify the maintenance worker is running and trigger the maintenance path if needed.",
                    "shown_count": 2,
                    "active": True,
                    "acknowledged": False,
                    "resolved": False,
                    "first_shown_at": "2026-04-05T07:00:00+00:00",
                    "last_shown_at": "2026-04-05T11:45:00+00:00",
                },
            ],
            "recent_operational_recommendation_events": [
                {
                    "code": "reduce_queue_backlog",
                    "event_type": "shown",
                    "message": "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                    "timestamp": "2026-04-04T06:00:00+00:00",
                },
                {
                    "code": "reduce_queue_backlog",
                    "event_type": "reappeared",
                    "message": "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                    "timestamp": "2026-04-05T11:00:00+00:00",
                },
                {
                    "code": "inspect_graph_failures_and_search_quality",
                    "event_type": "shown",
                    "message": "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                    "timestamp": "2026-04-05T08:00:00+00:00",
                },
                {
                    "code": "inspect_graph_failures_and_search_quality",
                    "event_type": "acknowledged",
                    "message": "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                    "timestamp": "2026-04-05T11:30:00+00:00",
                },
                {
                    "code": "restart_or_verify_maintenance_path",
                    "event_type": "shown",
                    "message": "Maintenance appears stale: verify the maintenance worker is running and trigger the maintenance path if needed.",
                    "timestamp": "2026-04-05T07:00:00+00:00",
                },
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    analytics = payload["maintenance_summary"]["recommendation_analytics"]
    assert analytics["unresolved_count"] == 3
    assert analytics["repeated_reappeared_count"] == 1
    assert "reduce_queue_backlog" in analytics["top_recurring_codes"]
    alert_codes = {item["code"] for item in payload["maintenance_summary"]["alerts"]}
    assert "runbook_slow_resolution" in alert_codes or "runbook_unresolved_pressure" in alert_codes


@pytest.mark.anyio
async def test_queue_health_operational_recommendations_track_repeat_count(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_operational_recommendations": [
                {
                    "code": "restart_or_verify_maintenance_path",
                    "message": "Maintenance appears stale: verify the maintenance worker is running and trigger the maintenance path if needed.",
                    "shown_count": 2,
                    "active": True,
                    "acknowledged": False,
                    "last_shown_at": "2026-04-05T09:00:00+00:00",
                }
            ],
            "recent_runs": [
                {"total_count": 1, "compacted_count": 0, "last_run_at": "2026-04-05T09:00:00+00:00"},
            ],
        },
    )

    response = await client.get("/health/queues")

    assert response.status_code == 200
    payload = response.json()
    recommendations = payload["operational_health"]["recommendations"]
    stale_recommendation = next(
        item for item in recommendations if item["code"] == "restart_or_verify_maintenance_path"
    )
    assert stale_recommendation["shown_count"] == 3
    assert stale_recommendation["acknowledged"] is False


@pytest.mark.anyio
async def test_acknowledge_operational_recommendation_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_operational_recommendations": [
                {
                    "code": "reduce_queue_backlog",
                    "message": "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                    "shown_count": 2,
                    "active": True,
                    "acknowledged": False,
                    "last_shown_at": "2026-04-05T09:00:00+00:00",
                }
            ],
        },
    )

    response = await client.post("/health/queues/operational-health/recommendations/reduce_queue_backlog/ack")

    assert response.status_code == 200
    payload = response.json()
    assert payload["code"] == "reduce_queue_backlog"
    assert payload["acknowledged"] is True
    assert payload["acknowledged_at"] is not None

    follow_up = await client.get("/health/workers/maintenance")
    assert follow_up.status_code == 200
    recommendations = follow_up.json()["maintenance_summary"]["recent_operational_recommendations"]
    acknowledged_item = next(item for item in recommendations if item["code"] == "reduce_queue_backlog")
    assert acknowledged_item["acknowledged"] is True


@pytest.mark.anyio
async def test_resolve_operational_recommendation_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_operational_recommendations": [
                {
                    "code": "inspect_graph_failures_and_search_quality",
                    "message": "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                    "shown_count": 2,
                    "active": True,
                    "acknowledged": True,
                    "acknowledged_at": "2026-04-05T09:00:00+00:00",
                    "resolved": False,
                    "last_shown_at": "2026-04-05T09:00:00+00:00",
                }
            ],
        },
    )

    response = await client.post(
        "/health/queues/operational-health/recommendations/inspect_graph_failures_and_search_quality/resolve",
        json={"note": "Domain filters tightened and retry path verified."},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["code"] == "inspect_graph_failures_and_search_quality"
    assert payload["acknowledged"] is True
    assert payload["resolved"] is True
    assert payload["resolved_at"] is not None
    assert payload["resolution_note"] == "Domain filters tightened and retry path verified."

    follow_up = await client.get("/health/workers/maintenance")
    assert follow_up.status_code == 200
    recommendations = follow_up.json()["maintenance_summary"]["recent_operational_recommendations"]
    resolved_item = next(
        item for item in recommendations if item["code"] == "inspect_graph_failures_and_search_quality"
    )
    assert resolved_item["resolved"] is True
    assert resolved_item["resolution_note"] == "Domain filters tightened and retry path verified."


@pytest.mark.anyio
async def test_operational_recommendation_history_tracks_lifecycle_events(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T09:00:00+00:00",
            "recent_operational_recommendations": [
                {
                    "code": "reduce_queue_backlog",
                    "message": "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                    "shown_count": 2,
                    "active": True,
                    "acknowledged": False,
                    "resolved": False,
                    "last_shown_at": "2026-04-05T09:00:00+00:00",
                }
            ],
        },
    )

    ack_response = await client.post("/health/queues/operational-health/recommendations/reduce_queue_backlog/ack")
    assert ack_response.status_code == 200

    resolve_response = await client.post(
        "/health/queues/operational-health/recommendations/reduce_queue_backlog/resolve",
        json={"note": "Scaled workers and checked long-running jobs."},
    )
    assert resolve_response.status_code == 200

    follow_up = await client.get("/health/workers/maintenance")
    assert follow_up.status_code == 200
    events = follow_up.json()["maintenance_summary"]["recent_operational_recommendation_events"]
    event_types = [item["event_type"] for item in events]
    assert "acknowledged" in event_types
    assert "resolved" in event_types
    resolved_event = next(item for item in events if item["event_type"] == "resolved")
    assert resolved_event["note"] == "Scaled workers and checked long-running jobs."


@pytest.mark.anyio
async def test_optimize_endpoint(client):
    response = await client.post("/v1/optimize", json={"prompt": "raw input"})

    assert response.status_code == 200
    assert response.json()["optimized_prompt"] == "optimized::raw input"
    assert response.json()["status"] == "success"


@pytest.mark.anyio
async def test_optimize_invalid_payload(client):
    response = await client.post("/v1/optimize", json={"prompt": ""})
    assert response.status_code == 422


@pytest.mark.anyio
async def test_decompose_endpoint_creates_search_job(client):
    response = await client.post("/v1/decompose", json={"prompt": "test query", "depth": "easy"})

    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["id"] == "task-1"

    search_job_response = await client.get("/v1/tasks/task-1/search-job")
    assert search_job_response.status_code == 200
    assert search_job_response.json()["task_id"] == "task-1"
    assert search_job_response.json()["status"] == SearchJobStatus.PENDING


@pytest.mark.anyio
async def test_task_summary_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-1",
            "description": "Search for X",
            "queries": ["query X"],
            "status": TaskStatus.PENDING,
        }
    )
    app_service.task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    app_service.task_store.update_task(
        "task-1",
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[
                {
                    "url": "https://docs.python.org/3/tutorial/",
                    "title": "Python Tutorial",
                    "domain": "docs.python.org",
                    "source_quality": "high",
                    "extraction_status": "success",
                    "content": "A" * 500,
                }
            ],
            log="task complete",
        ),
    )

    response = await client.get("/v1/tasks/task-1/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "task-1"
    assert payload["result_count"] == 1
    assert payload["log_count"] >= 1
    assert len(payload["source_preview"]) == 1
    assert payload["source_preview"][0]["domain"] == "docs.python.org"
    assert payload["search_metrics"]["extraction_attempts"] == 0
    assert payload["latest_search_job"]["task_id"] == "task-1"


@pytest.mark.anyio
async def test_research_finalize_flow(client):
    response = await client.post("/v1/research", json={"prompt": "test research", "depth": "easy"})

    assert response.status_code == 200
    research_id = response.json()["research_id"]

    app_service = client._transport.app.state.research_service
    tasks = app_service.task_store.get_tasks_by_research(research_id)
    assert len(tasks) == 1

    app_service.task_store.update_task(
        tasks[0].id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"content": "data", "url": "http://a.com", "title": "A"}],
            log="done",
        ),
    )

    finalize_response = await client.post(f"/v1/research/{research_id}/finalize")
    assert finalize_response.status_code == 200
    finalize_payload = finalize_response.json()
    assert finalize_payload["research"]["status"] == ResearchStatus.ANALYZING
    assert finalize_payload["finalize_job_id"] is not None

    job_id = finalize_payload["finalize_job_id"]
    app_service.process_finalize_job(job_id)

    research_response = await client.get(f"/v1/research/{research_id}")
    assert research_response.status_code == 200
    assert research_response.json()["status"] == ResearchStatus.COMPLETED
    assert research_response.json()["final_report"] == "Final structured report"

    finalize_job_response = await client.get(f"/v1/research/finalize-jobs/{job_id}")
    assert finalize_job_response.status_code == 200
    assert finalize_job_response.json()["status"] == "completed"

    latest_finalize_job_response = await client.get(f"/v1/research/{research_id}/finalize-job")
    assert latest_finalize_job_response.status_code == 200
    assert latest_finalize_job_response.json()["id"] == job_id


@pytest.mark.anyio
async def test_research_summary_endpoint(client):
    response = await client.post("/v1/research", json={"prompt": "test research", "depth": "easy"})
    research_id = response.json()["research_id"]

    app_service = client._transport.app.state.research_service
    tasks = app_service.task_store.get_tasks_by_research(research_id)
    app_service.task_store.update_task(
        tasks[0].id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"url": "https://a.com", "title": "A", "domain": "a.com", "content": "hello"}],
            log="done",
        ),
    )

    summary_response = await client.get(f"/v1/research/{research_id}/summary")

    assert summary_response.status_code == 200
    payload = summary_response.json()
    assert payload["id"] == research_id
    assert payload["task_count"] == 1
    assert payload["completed_tasks"] == 1
    assert payload["collected_sources"] == 1
    assert payload["finalize_ready"] is True
    assert payload["has_final_report"] is False
    assert len(payload["tasks"]) == 1
    assert payload["tasks"][0]["result_count"] == 1
    assert payload["total_candidates"] == 0
    assert payload["total_extraction_attempts"] == 0
    assert "graph_execution_summary" in payload


@pytest.mark.anyio
async def test_research_report_endpoint(client):
    response = await client.post("/v1/research", json={"prompt": "test research", "depth": "easy"})
    research_id = response.json()["research_id"]
    app_service = client._transport.app.state.research_service
    app_service.task_store.update_research_status(research_id, ResearchStatus.COMPLETED, "Final report body")

    report_response = await client.get(f"/v1/research/{research_id}/report")

    assert report_response.status_code == 200
    payload = report_response.json()
    assert payload["research_id"] == research_id
    assert payload["status"] == ResearchStatus.COMPLETED
    assert payload["final_report"] == "Final report body"


@pytest.mark.anyio
async def test_research_verification_endpoint(client):
    response = await client.post("/v1/research", json={"prompt": "renewable energy growth", "depth": "easy"})
    research_id = response.json()["research_id"]
    app_service = client._transport.app.state.research_service
    tasks = app_service.task_store.get_tasks_by_research(research_id)
    # Three sources sharing tokens -> a multi-source evidence group (per-claim confidence).
    app_service.task_store.update_task(
        tasks[0].id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[
                {"url": "https://a.com", "title": "A", "domain": "a.com", "source_quality": "high",
                 "content": "Solar photovoltaic capacity expanded rapidly across emerging markets last year according to analysts."},
                {"url": "https://b.com", "title": "B", "domain": "b.com", "source_quality": "high",
                 "content": "Solar photovoltaic capacity additions reached record levels as installation costs declined steadily."},
                {"url": "https://c.com", "title": "C", "domain": "c.com", "source_quality": "medium",
                 "content": "Wind and solar photovoltaic capacity together now supply a growing share of electricity demand."},
            ],
            log="done",
        ),
    )
    app_service.task_store.update_research_status(
        research_id, ResearchStatus.COMPLETED, "# Report\n\n## Conclusion\nSolar capacity is rising [S1].\n"
    )

    resp = await client.get(f"/v1/research/{research_id}/verification")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["research_id"] == research_id
    # The stub plan sub-question ("Search for X") is not addressed by the report.
    assert payload["uncovered_questions"] == ["Search for X"]
    assert payload["coverage_ratio"] == 0.0
    assert len(payload["plan_coverage"]) == 1
    # A corroborated finding surfaces with its supporting source ids.
    assert payload["findings"], "expected at least one confidence finding"
    assert payload["findings"][0]["source_count"] >= 2
    assert payload["findings"][0]["support_level"] in {"strong", "medium", "weak"}


@pytest.mark.anyio
async def test_research_graph_endpoint(client):
    response = await client.post("/v1/research", json={"prompt": "test research", "depth": "easy"})
    research_id = response.json()["research_id"]
    app_service = client._transport.app.state.research_service
    app_service.checkpoint_graph_state(
        research_id,
        {"step": "collect_context"},
        {"step": "collect_context", "detail": "Collected 1 source"},
    )

    graph_response = await client.get(f"/v1/research/{research_id}/graph")

    assert graph_response.status_code == 200
    payload = graph_response.json()
    assert payload["research_id"] == research_id
    assert payload["graph_state"]["step"] == "collect_context"
    assert payload["graph_trail"][0]["detail"] == "Collected 1 source"


@pytest.mark.anyio
async def test_requeue_search_job_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-dead",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    )
    job = app_service.task_store.add_search_task_job("task-dead", SearchDepth.EASY.value, max_attempts=1)
    app_service.task_store.claim_next_search_task_job()
    app_service.task_store.record_search_task_job_failure(job.id, "boom")

    response = await client.post(f"/v1/search-jobs/{job.id}/requeue")

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["attempt_count"] == 0


@pytest.mark.anyio
async def test_requeue_finalize_job_endpoint(client):
    app_service = client._transport.app.state.research_service
    research = app_service.task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    app_service.task_store.update_research_status(research.id, ResearchStatus.FAILED, "analysis failed")
    job = app_service.task_store.add_research_finalize_job(research.id, max_attempts=1)
    app_service.task_store.claim_next_research_finalize_job()
    app_service.task_store.record_research_finalize_job_failure(job.id, "boom")

    response = await client.post(f"/v1/research/finalize-jobs/{job.id}/requeue")

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["attempt_count"] == 0


@pytest.mark.anyio
async def test_recover_stale_search_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-stale",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.RUNNING,
        }
    )
    job = app_service.task_store.add_search_task_job("task-stale", SearchDepth.EASY.value)
    job.status = SearchJobStatus.RUNNING
    job.updated_at = job.updated_at.replace(year=2020)

    response = await client.post("/v1/search-jobs/recover-stale")

    assert response.status_code == 200
    assert response.json()["recovered_count"] == 1
    assert response.json()["recovered_job_ids"] == [job.id]


@pytest.mark.anyio
async def test_recover_stale_finalize_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    research = app_service.task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    app_service.task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    job = app_service.task_store.add_research_finalize_job(research.id)
    job.status = FinalizeJobStatus.RUNNING
    job.updated_at = job.updated_at.replace(year=2020)

    response = await client.post("/v1/research/finalize-jobs/recover-stale")

    assert response.status_code == 200
    assert response.json()["recovered_count"] == 1
    assert response.json()["recovered_job_ids"] == [job.id]


@pytest.mark.anyio
async def test_list_running_and_dead_letter_search_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-list-search",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    running = app_service.task_store.add_search_task_job("task-list-search", SearchDepth.EASY.value)
    dead = app_service.task_store.add_search_task_job("task-list-search", SearchDepth.EASY.value)
    running.status = SearchJobStatus.RUNNING
    dead.status = SearchJobStatus.DEAD_LETTER

    running_response = await client.get("/v1/search-jobs?status=running")
    dead_response = await client.get("/v1/search-jobs?status=dead_letter")

    assert running_response.status_code == 200
    assert dead_response.status_code == 200
    assert [job["id"] for job in running_response.json()] == [running.id]
    assert [job["id"] for job in dead_response.json()] == [dead.id]


@pytest.mark.anyio
async def test_list_running_and_dead_letter_finalize_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    research = app_service.task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    running = app_service.task_store.add_research_finalize_job(research.id)
    dead = app_service.task_store.add_research_finalize_job(research.id)
    running.status = FinalizeJobStatus.RUNNING
    dead.status = FinalizeJobStatus.DEAD_LETTER

    running_response = await client.get("/v1/research/finalize-jobs?status=running")
    dead_response = await client.get("/v1/research/finalize-jobs?status=dead_letter")

    assert running_response.status_code == 200
    assert dead_response.status_code == 200
    assert [job["id"] for job in running_response.json()] == [running.id]
    assert [job["id"] for job in dead_response.json()] == [dead.id]


@pytest.mark.anyio
async def test_list_jobs_endpoint_rejects_unsupported_status(client):
    search_response = await client.get("/v1/search-jobs?status=pending")
    finalize_response = await client.get("/v1/research/finalize-jobs?status=pending")

    assert search_response.status_code == 422
    assert finalize_response.status_code == 422


@pytest.mark.anyio
async def test_cleanup_search_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    app_service.task_store.add_task(
        {
            "id": "task-clean-search",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = app_service.task_store.add_search_task_job("task-clean-search", SearchDepth.EASY.value)
    job.status = SearchJobStatus.COMPLETED
    job.updated_at = job.updated_at.replace(year=2020)

    response = await client.post("/v1/search-jobs/cleanup")

    assert response.status_code == 200
    assert response.json()["deleted_count"] == 1
    assert response.json()["deleted_job_ids"] == [job.id]


@pytest.mark.anyio
async def test_cleanup_finalize_jobs_endpoint(client):
    app_service = client._transport.app.state.research_service
    research = app_service.task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    job = app_service.task_store.add_research_finalize_job(research.id)
    job.status = FinalizeJobStatus.DEAD_LETTER
    job.updated_at = job.updated_at.replace(year=2020)

    response = await client.post("/v1/research/finalize-jobs/cleanup")

    assert response.status_code == 200
    assert response.json()["deleted_count"] == 1
    assert response.json()["deleted_job_ids"] == [job.id]


def test_search_agent_integration(mocker):
    from src.agents.search import SearchAgent

    task_store = InMemoryTaskStore()
    task_id = "test-agent-id"
    task_store.add_task(
        {
            "id": task_id,
            "description": "test",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )

    mock_search = mocker.patch("src.providers.search.SearchProvider.search")
    mock_search.return_value = [{"url": "http://example.com", "title": "Example"}]

    mock_extract = mocker.patch("src.providers.search.ContentExtractor.extract_content")
    mock_extract.return_value = "Full page content"

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task(task_id)

    final_task = task_store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result[0]["content"] == "Full page content"
    assert "Search completed" in final_task.logs[-1]


@pytest.mark.anyio
async def test_auth_register_login_me_flow(client, mocker):
    mocker.patch("src.api.dependencies.settings.auth_disabled", False)

    # Unauthenticated /me is rejected.
    unauth = await client.get("/v1/auth/me")
    assert unauth.status_code == 401

    # Register issues a JWT (Bearer + cookie) and returns the user.
    reg = await client.post("/v1/auth/register", json={"email": "x@y.com", "password": "secret12"})
    assert reg.status_code == 200
    body = reg.json()
    assert body["user"]["email"] == "x@y.com"
    assert body["token_type"] == "bearer"
    assert body["access_token"].count(".") == 2  # header.payload.signature

    # The httpx client persists the cookie, so /me now works.
    me = await client.get("/v1/auth/me")
    assert me.status_code == 200
    assert me.json()["email"] == "x@y.com"

    # Logout clears the cookie.
    out = await client.post("/v1/auth/logout", headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""})
    assert out.status_code == 200
    after = await client.get("/v1/auth/me")
    assert after.status_code == 401


@pytest.mark.anyio
async def test_bearer_jwt_authenticates_without_cookie(client, mocker):
    mocker.patch("src.api.dependencies.settings.auth_disabled", False)
    reg = await client.post("/v1/auth/register", json={"email": "bear@x.com", "password": "secret12"})
    token = reg.json()["access_token"]

    # Drop the cookie so only the Authorization: Bearer header can authenticate.
    client.cookies.clear()
    assert (await client.get("/v1/auth/me")).status_code == 401

    me = await client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json()["email"] == "bear@x.com"

    bad = await client.get("/v1/auth/me", headers={"Authorization": "Bearer not.a.jwt"})
    assert bad.status_code == 401


@pytest.mark.anyio
async def test_password_change_requires_current_password_and_revokes_old_token(client, mocker):
    mocker.patch("src.api.dependencies.settings.auth_disabled", False)
    reg = await client.post(
        "/v1/auth/register",
        json={"email": "rotate@x.com", "password": "secret12"},
    )
    old_token = reg.json()["access_token"]

    missing = await client.post(
        "/v1/auth/set-password",
        json={"password": "replacement12"},
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    assert missing.status_code == 400

    wrong = await client.post(
        "/v1/auth/set-password",
        json={"password": "replacement12", "current_password": "not-the-password"},
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    assert wrong.status_code == 401

    changed = await client.post(
        "/v1/auth/set-password",
        json={"password": "replacement12", "current_password": "secret12"},
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    assert changed.status_code == 200
    new_token = changed.json()["access_token"]
    assert new_token != old_token

    old_session = await client.get(
        "/v1/auth/me",
        headers={"Authorization": f"Bearer {old_token}"},
    )
    assert old_session.status_code == 401

    new_session = await client.get(
        "/v1/auth/me",
        headers={"Authorization": f"Bearer {new_token}"},
    )
    assert new_session.status_code == 200
    assert new_session.json()["email"] == "rotate@x.com"

    assert (
        await client.post(
            "/v1/auth/login",
            json={"email": "rotate@x.com", "password": "secret12"},
        )
    ).status_code == 401
    assert (
        await client.post(
            "/v1/auth/login",
            json={"email": "rotate@x.com", "password": "replacement12"},
        )
    ).status_code == 200


@pytest.mark.anyio
async def test_google_oauth_login_and_callback(client, mocker):
    mocker.patch("src.api.dependencies.settings.auth_disabled", False)
    mocker.patch("src.config.settings.google_client_id", "cid")
    mocker.patch("src.config.settings.google_client_secret", "sec")

    assert (await client.get("/v1/auth/config")).json()["google_oauth"] is True

    import urllib.parse as _up

    def _state_from(resp):
        return _up.parse_qs(_up.urlsplit(resp.headers["location"]).query)["state"][0]

    login = await client.get("/v1/auth/google/login", follow_redirects=False)
    assert login.status_code == 302
    assert "accounts.google.com" in login.headers["location"]
    state = _state_from(login)  # stateless signed token in the URL (no cookie)
    assert state

    mocker.patch(
        "src.api.app.fetch_userinfo",
        return_value={"email": "g@user.com", "email_verified": True, "sub": "123"},
    )
    cb = await client.get(
        f"/v1/auth/google/callback?code=abc&state={state}", follow_redirects=False
    )
    assert cb.status_code == 302
    assert cb.headers["location"] == "/set-password"  # new user → offered to set a password

    me = await client.get("/v1/auth/me")  # session cookie now established
    assert me.status_code == 200
    assert me.json()["email"] == "g@user.com"

    # Set a password, then email/password login works for this Google-created account.
    sp = await client.post(
        "/v1/auth/set-password",
        json={"password": "mypass123"},
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    assert sp.status_code == 200
    await client.post("/v1/auth/logout", headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""})
    relogin = await client.post("/v1/auth/login", json={"email": "g@user.com", "password": "mypass123"})
    assert relogin.status_code == 200

    # A returning Google user goes straight in (no set-password redirect).
    login2 = await client.get("/v1/auth/google/login", follow_redirects=False)
    state2 = _state_from(login2)
    cb2 = await client.get(
        f"/v1/auth/google/callback?code=abc&state={state2}", follow_redirects=False
    )
    assert cb2.headers["location"] == "/"


@pytest.mark.anyio
async def test_google_login_404_when_not_configured(client):
    assert (await client.get("/v1/auth/config")).json()["google_oauth"] is False
    assert (await client.get("/v1/auth/google/login")).status_code == 404


@pytest.mark.anyio
async def test_google_callback_rejects_bad_state(client, mocker):
    mocker.patch("src.config.settings.google_client_id", "cid")
    mocker.patch("src.config.settings.google_client_secret", "sec")
    cb = await client.get("/v1/auth/google/callback?code=abc&state=wrong", follow_redirects=False)
    assert cb.status_code == 400


@pytest.mark.anyio
async def test_research_access_scoped_to_owner(client, mocker):
    mocker.patch("src.api.dependencies.settings.auth_disabled", False)

    # User A registers and creates a research.
    await client.post("/v1/auth/register", json={"email": "a@x.com", "password": "secret12"})
    created = await client.post(
        "/v1/research",
        json={"prompt": "alpha topic", "depth": "easy"},
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    research_id = created.json()["research_id"]
    assert (await client.get(f"/v1/research/{research_id}")).status_code == 200
    app_service = client._transport.app.state.research_service
    task = app_service.task_store.get_tasks_by_research(research_id)[0]
    search_job = app_service.task_store.get_latest_search_task_job(task.id)
    finalize_job = app_service.task_store.add_research_finalize_job(research_id)
    assert search_job is not None
    assert (await client.get(f"/v1/tasks/{task.id}")).status_code == 200
    assert (await client.get(f"/v1/search-jobs/{search_job.id}")).status_code == 200
    assert (await client.get(f"/v1/research/finalize-jobs/{finalize_job.id}")).status_code == 200

    # Switch to user B.
    await client.post("/v1/auth/logout", headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""})
    await client.post("/v1/auth/register", json={"email": "b@x.com", "password": "secret12"})

    # B cannot see or open A's research.
    assert (await client.get("/v1/research")).json() == []
    assert (await client.get(f"/v1/research/{research_id}")).status_code == 404
    assert (await client.get(f"/v1/research/{research_id}/status")).status_code == 404
    assert (await client.get(f"/v1/research/{research_id}/report")).status_code == 404

    # Parallel task/job routes must enforce the same owner boundary.
    assert (await client.get(f"/v1/tasks/{task.id}")).status_code == 404
    assert (await client.get(f"/v1/tasks/{task.id}/summary")).status_code == 404
    assert (await client.get(f"/v1/tasks/{task.id}/search-job")).status_code == 404
    assert (await client.get(f"/v1/search-jobs/{search_job.id}")).status_code == 404
    assert (await client.get(f"/v1/research/finalize-jobs/{finalize_job.id}")).status_code == 404
    csrf = client.cookies.get("csrf_token") or ""
    task_update = await client.patch(
        f"/v1/tasks/{task.id}",
        json={"status": "failed"},
        headers={"X-CSRF-Token": csrf},
    )
    assert task_update.status_code == 404

    # Global operational listings are admin-only, not cross-user feeds.
    assert (await client.get("/v1/tasks")).status_code == 403
    assert (await client.get("/v1/search-jobs?status=running")).status_code == 403
    assert (await client.get("/v1/research/finalize-jobs?status=running")).status_code == 403
