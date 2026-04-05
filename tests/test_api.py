import httpx
import pytest

from src.api.app import create_app
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
    app_service = client._transport.app.state.research_service
    app_service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "last_run_at": "2026-04-05T12:10:00+00:00",
            "recent_operational_health": [
                {"status": "critical", "score": 40, "timestamp": "2026-04-05T12:00:00+00:00"},
                {"status": "warning", "score": 72, "timestamp": "2026-04-05T12:05:00+00:00"},
            ],
            "recent_runs": [
                {"total_count": 1, "compacted_count": 0, "last_run_at": "2026-04-05T12:00:00+00:00"},
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
