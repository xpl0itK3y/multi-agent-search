import json
import uuid
import time
from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from src.api.dependencies import get_research_service
from src.model_catalog import list_models as list_model_catalog
from src.api.schemas import (
    DecomposeRequest,
    DecomposeResponse,
    JobCleanupResponse,
    JobRecoveryResponse,
    OperationalHealth,
    OperationalRecommendationResolveRequest,
    OptimizeRequest,
    OptimizeResponse,
    QueueMetrics,
    QueueMaintenanceResponse,
    ResearchFinalizeJob,
    ResearchFinalizeResponse,
    ResearchGraphResponse,
    ResearchHistoryItem,
    ResearchRecord,
    ResearchReportResponse,
    ResearchRequest,
    ChatAsk,
    ChatMessage,
    ResearchConflict,
    ResearchPlan,
    ResearchPlanUpdate,
    ResearchResponse,
    ResearchSummary,
    ResearchStatusSummary,
    SearchTaskJob,
    SearchTask,
    SearchSourcePreview,
    SearchTaskSummary,
    TaskUpdate,
    WorkerHeartbeat,
)
from src.bootstrap import lifespan
from src.config import settings
from src.observability import bind_observability_context, observe_api_request, render_metrics


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

    allowed_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        started_at = time.perf_counter()
        with bind_observability_context(
            request_id=request_id,
            method=request.method,
            path=str(request.url.path),
        ):
            response = await call_next(request)
        observe_api_request(
            request.method,
            str(request.url.path),
            response.status_code,
            time.perf_counter() - started_at,
        )
        response.headers["X-Request-ID"] = request_id
        return response

    register_routes(app)
    return app


def register_routes(app: FastAPI) -> None:
    @app.get("/health")
    async def health_check(request: Request):
        return get_research_service(request).get_health_status()

    @app.get("/metrics")
    async def metrics_endpoint():
        payload, content_type = render_metrics()
        return Response(content=payload, media_type=content_type)

    @app.get("/health/queues", response_model=QueueMetrics)
    async def queue_health(request: Request):
        return get_research_service(request).get_queue_metrics()

    @app.post("/health/queues/maintenance", response_model=QueueMaintenanceResponse)
    async def run_queue_maintenance(request: Request):
        return get_research_service(request).run_queue_maintenance()

    @app.post(
        "/health/queues/operational-health/recommendations/{code}/ack",
        response_model=OperationalHealth.RecommendationEntry,
    )
    async def acknowledge_operational_recommendation(code: str, request: Request):
        return get_research_service(request).acknowledge_operational_recommendation(code)

    @app.post(
        "/health/queues/operational-health/recommendations/{code}/resolve",
        response_model=OperationalHealth.RecommendationEntry,
    )
    async def resolve_operational_recommendation(
        code: str,
        payload: OperationalRecommendationResolveRequest,
        request: Request,
    ):
        return get_research_service(request).resolve_operational_recommendation(code, payload.note)

    @app.get("/health/workers/{worker_name}", response_model=WorkerHeartbeat)
    async def worker_health(worker_name: str, request: Request):
        heartbeat = get_research_service(request).get_worker_heartbeat(worker_name)
        if not heartbeat:
            raise HTTPException(status_code=404, detail="Worker heartbeat not found")
        return heartbeat

    @app.get("/v1/models")
    async def list_models():
        return list_model_catalog()

    @app.post("/v1/optimize", response_model=OptimizeResponse)
    async def optimize_prompt(request: Request, payload: OptimizeRequest):
        try:
            optimized = get_research_service(request).optimize_prompt(payload.prompt)
            return OptimizeResponse(optimized_prompt=optimized)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/v1/decompose", response_model=DecomposeResponse)
    async def decompose_prompt(request: Request, payload: DecomposeRequest):
        try:
            return get_research_service(request).decompose_prompt(
                payload.prompt,
                payload.depth,
            )
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/v1/tasks", response_model=List[SearchTask])
    async def list_tasks(request: Request):
        return get_research_service(request).list_tasks()

    @app.get("/v1/tasks/{task_id}", response_model=SearchTask)
    async def get_task(task_id: str, request: Request):
        task = get_research_service(request).get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/summary", response_model=SearchTaskSummary)
    async def get_task_summary(task_id: str, request: Request):
        return get_research_service(request).get_task_summary(task_id)

    @app.patch("/v1/tasks/{task_id}", response_model=SearchTask)
    async def update_task(task_id: str, update: TaskUpdate, request: Request):
        task = get_research_service(request).update_task(task_id, update)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @app.get("/v1/tasks/{task_id}/search-job", response_model=SearchTaskJob)
    async def get_latest_search_job(task_id: str, request: Request):
        job = get_research_service(request).get_latest_search_task_job(task_id)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs/{job_id}", response_model=SearchTaskJob)
    async def get_search_job(job_id: str, request: Request):
        job = get_research_service(request).get_search_task_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Search job not found")
        return job

    @app.get("/v1/search-jobs", response_model=List[SearchTaskJob])
    async def list_search_jobs(status: str, request: Request):
        service = get_research_service(request)
        if status == "running":
            return service.list_running_search_task_jobs()
        if status == "dead_letter":
            return service.list_dead_letter_search_task_jobs()
        raise HTTPException(status_code=422, detail="Unsupported search job status filter")

    @app.post("/v1/search-jobs/{job_id}/requeue", response_model=SearchTaskJob)
    async def requeue_search_job(job_id: str, request: Request):
        return get_research_service(request).requeue_search_task_job(job_id)

    @app.post("/v1/search-jobs/recover-stale", response_model=JobRecoveryResponse)
    async def recover_stale_search_jobs(request: Request):
        return get_research_service(request).recover_stale_search_task_jobs()

    @app.post("/v1/search-jobs/cleanup", response_model=JobCleanupResponse)
    async def cleanup_search_jobs(request: Request):
        return get_research_service(request).cleanup_old_search_task_jobs()

    @app.get("/v1/research", response_model=List[ResearchHistoryItem])
    async def list_researches(request: Request, limit: int = 20):
        return get_research_service(request).list_researches(limit=limit)

    @app.post("/v1/research", response_model=ResearchResponse)
    async def start_research(request: Request, payload: ResearchRequest, background_tasks: BackgroundTasks):
        try:
            service = get_research_service(request)
            response, research_id = service.start_research(payload)
            # LLM decompose runs after response is sent — user gets research_id instantly
            background_tasks.add_task(service.decompose_and_enqueue, research_id, payload)
            return response
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/v1/research/finalize-jobs", response_model=List[ResearchFinalizeJob])
    async def list_finalize_jobs(status: str, request: Request):
        service = get_research_service(request)
        if status == "running":
            return service.list_running_research_finalize_jobs()
        if status == "dead_letter":
            return service.list_dead_letter_research_finalize_jobs()
        raise HTTPException(status_code=422, detail="Unsupported finalize job status filter")

    @app.get("/v1/research/finalize-jobs/{job_id}", response_model=ResearchFinalizeJob)
    async def get_finalize_job(job_id: str, request: Request):
        job = get_research_service(request).get_research_finalize_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        return job

    @app.get("/v1/research/{research_id}/finalize-job", response_model=ResearchFinalizeJob)
    async def get_latest_finalize_job(research_id: str, request: Request):
        job = get_research_service(request).get_latest_research_finalize_job(research_id)
        if not job:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        return job

    @app.post("/v1/research/finalize-jobs/{job_id}/requeue", response_model=ResearchFinalizeJob)
    async def requeue_finalize_job(job_id: str, request: Request):
        return get_research_service(request).requeue_research_finalize_job(job_id)

    @app.post("/v1/research/finalize-jobs/recover-stale", response_model=JobRecoveryResponse)
    async def recover_stale_finalize_jobs(request: Request):
        return get_research_service(request).recover_stale_research_finalize_jobs()

    @app.post("/v1/research/finalize-jobs/cleanup", response_model=JobCleanupResponse)
    async def cleanup_finalize_jobs(request: Request):
        return get_research_service(request).cleanup_old_research_finalize_jobs()

    @app.delete("/v1/research/{research_id}", status_code=204)
    async def delete_research(research_id: str, request: Request):
        deleted = get_research_service(request).delete_research(research_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Research not found")

    @app.get("/v1/research/{research_id}", response_model=ResearchRecord)
    async def get_research_status(research_id: str, request: Request):
        return get_research_service(request).get_research_status(research_id)

    @app.get("/v1/research/{research_id}/summary", response_model=ResearchSummary)
    async def get_research_summary(research_id: str, request: Request):
        return get_research_service(request).get_research_summary(research_id)

    @app.get("/v1/research/{research_id}/status", response_model=ResearchStatusSummary)
    async def get_research_status_summary(research_id: str, request: Request):
        # Cheap polling endpoint: no heavy analysis/LLM (unlike /summary).
        return get_research_service(request).get_research_status_summary(research_id)

    @app.get("/v1/research/{research_id}/report", response_model=ResearchReportResponse)
    async def get_research_report(research_id: str, request: Request):
        return get_research_service(request).get_research_report(research_id)

    @app.get("/v1/research/{research_id}/sources", response_model=List[SearchSourcePreview])
    async def get_research_sources(research_id: str, request: Request):
        return get_research_service(request).get_research_sources(research_id)

    @app.get("/v1/research/{research_id}/conflicts", response_model=List[ResearchConflict])
    async def get_research_conflicts(research_id: str, request: Request):
        return get_research_service(request).get_research_conflicts(research_id)

    @app.get("/v1/research/{research_id}/plan", response_model=ResearchPlan)
    async def get_research_plan(research_id: str, request: Request):
        return get_research_service(request).get_research_plan(research_id)

    @app.put("/v1/research/{research_id}/plan", response_model=ResearchPlan)
    async def update_research_plan(research_id: str, payload: ResearchPlanUpdate, request: Request):
        return get_research_service(request).update_research_plan(research_id, payload)

    @app.post("/v1/research/{research_id}/plan/approve", response_model=ResearchRecord)
    async def approve_research_plan(research_id: str, request: Request):
        return get_research_service(request).approve_research_plan(research_id)

    @app.get("/v1/research/{research_id}/messages", response_model=List[ChatMessage])
    async def list_research_messages(research_id: str, request: Request):
        return get_research_service(request).list_research_messages(research_id)

    @app.post("/v1/research/{research_id}/messages", response_model=ChatMessage)
    def ask_research(research_id: str, payload: ChatAsk, request: Request):
        # sync def -> runs in a threadpool so the blocking LLM call doesn't stall the event loop
        service = get_research_service(request)
        answer = service.generate_research_answer(research_id, payload.question)
        service.append_research_message(research_id, "user", payload.question)
        service.append_research_message(research_id, "assistant", answer)
        return ChatMessage(role="assistant", content=answer)

    @app.get("/v1/research/{research_id}/graph", response_model=ResearchGraphResponse)
    async def get_research_graph(research_id: str, request: Request):
        return get_research_service(request).get_research_graph(research_id)

    @app.get("/v1/research/{research_id}/events")
    def research_events(research_id: str, request: Request):
        """Server-Sent Events stream: live status, graph trace and report deltas (F1)."""
        service = get_research_service(request)

        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

        def event_stream():
            last_status: str | None = None
            last_report: str | None = None
            last_reasoning: str | None = None
            last_trail_len = 0
            idle_ticks = 0
            deadline = time.monotonic() + 900  # 10-minute safety cap
            yield ": connected\n\n"
            while time.monotonic() < deadline:
                research = service.task_store.get_research(research_id)
                if research is None:
                    yield sse("stream_error", {"detail": "Research not found"})
                    yield sse("done", {"status": "failed"})
                    return

                graph_state = research.graph_state or {}
                status = getattr(research.status, "value", str(research.status))
                if status != last_status:
                    last_status = status
                    idle_ticks = 0
                    yield sse("status_change", {"status": status})

                trail = research.graph_trail or []
                if len(trail) > last_trail_len:
                    for entry in trail[last_trail_len:]:
                        yield sse("trace_step", {"step": entry.get("step"), "detail": entry.get("detail")})
                    last_trail_len = len(trail)
                    idle_ticks = 0

                reasoning = graph_state.get("partial_reasoning")
                if reasoning and reasoning != last_reasoning:
                    last_reasoning = reasoning
                    idle_ticks = 0
                    yield sse("reasoning_delta", {"reasoning": reasoning, "phase": "analyze"})

                report = research.final_report or graph_state.get("partial_report")
                if report and report != last_report:
                    last_report = report
                    idle_ticks = 0
                    yield sse("report", {"report": report, "final": bool(research.final_report)})

                if status in ("completed", "failed"):
                    yield sse("done", {"status": status})
                    return

                idle_ticks += 1
                if idle_ticks >= 15:  # keep-alive comment so proxies don't drop the stream
                    idle_ticks = 0
                    yield ": ping\n\n"
                time.sleep(1.0)

            yield sse("done", {"status": "timeout"})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/v1/research/{research_id}/finalize", response_model=ResearchFinalizeResponse)
    async def finalize_research(research_id: str, request: Request):
        research, job = get_research_service(request).enqueue_research_finalization(research_id)
        return ResearchFinalizeResponse(
            research=research,
            finalize_job_id=job.id if job else None,
        )


app = create_app()
