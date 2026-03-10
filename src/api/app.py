from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from src.api.dependencies import get_research_service
from src.api.schemas import (
    DecomposeRequest,
    DecomposeResponse,
    OptimizeRequest,
    OptimizeResponse,
    ResearchRecord,
    ResearchRequest,
    ResearchResponse,
    SearchTask,
    TaskUpdate,
)
from src.bootstrap import lifespan
from src.config import settings


def create_app() -> FastAPI:
    return FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)


app = create_app()

@app.get("/health")
def health_check():
    return {"status": "ok"}

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
async def decompose_prompt(request: Request, payload: DecomposeRequest, background_tasks: BackgroundTasks):
    try:
        return get_research_service(request).decompose_prompt(
            payload.prompt,
            payload.depth,
            background_tasks,
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

@app.post("/v1/research", response_model=ResearchResponse)
async def start_research(request: Request, payload: ResearchRequest, background_tasks: BackgroundTasks):
    try:
        return get_research_service(request).start_research(payload, background_tasks)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/research/{research_id}", response_model=ResearchRecord)
async def get_research_status(research_id: str, request: Request):
    return get_research_service(request).get_research_status(research_id)


@app.post("/v1/research/{research_id}/finalize", response_model=ResearchRecord)
async def finalize_research(research_id: str, request: Request):
    return get_research_service(request).finalize_research(research_id)
