import uuid
from typing import List

from fastapi import FastAPI, HTTPException, Request
from src.api.dependencies import get_research_service
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
    ResearchRecord,
    ResearchReportResponse,
    ResearchRequest,
    ResearchResponse,
    ResearchSummary,
    SearchTaskJob,
    SearchTask,
    SearchTaskSummary,
    TaskUpdate,
    WorkerHeartbeat,
)
from src.bootstrap import lifespan
from src.config import settings
from src.observability import bind_observability_context


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        with bind_observability_context(
            request_id=request_id,
            method=request.method,
            path=str(request.url.path),
        ):
            response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    register_routes(app)
    return app


def register_routes(app: FastAPI) -> None:
    @app.get("/health")
    async def health_check(request: Request):
        return get_research_service(request).get_health_status()

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

    @app.post("/v1/research", response_model=ResearchResponse)
    async def start_research(request: Request, payload: ResearchRequest):
        try:
            return get_research_service(request).start_research(payload)
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

    @app.get("/v1/research/{research_id}", response_model=ResearchRecord)
    async def get_research_status(research_id: str, request: Request):
        return get_research_service(request).get_research_status(research_id)

    @app.get("/v1/research/{research_id}/summary", response_model=ResearchSummary)
    async def get_research_summary(research_id: str, request: Request):
        return get_research_service(request).get_research_summary(research_id)

    @app.get("/v1/research/{research_id}/report", response_model=ResearchReportResponse)
    async def get_research_report(research_id: str, request: Request):
        return get_research_service(request).get_research_report(research_id)

    @app.get("/v1/research/{research_id}/graph", response_model=ResearchGraphResponse)
    async def get_research_graph(research_id: str, request: Request):
        return get_research_service(request).get_research_graph(research_id)

    @app.post("/v1/research/{research_id}/finalize", response_model=ResearchFinalizeResponse)
    async def finalize_research(research_id: str, request: Request):
        research, job = get_research_service(request).enqueue_research_finalization(research_id)
        return ResearchFinalizeResponse(
            research=research,
            finalize_job_id=job.id if job else None,
        )


app = create_app()
