from typing import List

from fastapi import FastAPI, HTTPException, Request
from src.api.dependencies import get_research_service
from src.api.schemas import (
    DecomposeRequest,
    DecomposeResponse,
    OptimizeRequest,
    OptimizeResponse,
    QueueMetrics,
    ResearchFinalizeJob,
    ResearchFinalizeResponse,
    ResearchRecord,
    ResearchRequest,
    ResearchResponse,
    SearchTaskJob,
    SearchTask,
    TaskUpdate,
    WorkerHeartbeat,
)
from src.bootstrap import lifespan
from src.config import settings


def create_app() -> FastAPI:
    return FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)


app = create_app()

@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/health/queues", response_model=QueueMetrics)
async def queue_health(request: Request):
    return get_research_service(request).get_queue_metrics()


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

@app.post("/v1/research", response_model=ResearchResponse)
async def start_research(request: Request, payload: ResearchRequest):
    try:
        return get_research_service(request).start_research(payload)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/research/{research_id}", response_model=ResearchRecord)
async def get_research_status(research_id: str, request: Request):
    return get_research_service(request).get_research_status(research_id)


@app.post("/v1/research/{research_id}/finalize", response_model=ResearchFinalizeResponse)
async def finalize_research(research_id: str, request: Request):
    research, job = get_research_service(request).enqueue_research_finalization(research_id)
    return ResearchFinalizeResponse(
        research=research,
        finalize_job_id=job.id if job else None,
    )


@app.get("/v1/research/finalize-jobs/{job_id}", response_model=ResearchFinalizeJob)
async def get_finalize_job(job_id: str, request: Request):
    job = get_research_service(request).get_research_finalize_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Finalize job not found")
    return job
