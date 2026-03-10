from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException
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
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.analyzer import AnalyzerAgent
from src.providers.deepseek import DeepSeekProvider
from src.config import settings
from src.core.task_manager import task_manager
from src.services import ResearchService

app = FastAPI(title=settings.app_name, debug=settings.debug)

agent_optimizer = None
agent_orchestrator = None
agent_analyzer = None

try:
    llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
    agent_optimizer = PromptOptimizerAgent(llm)
    agent_orchestrator = OrchestratorAgent(llm)
    agent_analyzer = AnalyzerAgent(llm)
except Exception as e:
    print(f"Warning: Failed to initialize agents: {e}")

research_service = ResearchService(
    task_manager=task_manager,
    optimizer=agent_optimizer,
    orchestrator=agent_orchestrator,
    analyzer=agent_analyzer,
)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/v1/optimize", response_model=OptimizeResponse)
async def optimize_prompt(request: OptimizeRequest):
    try:
        optimized = research_service.optimize_prompt(request.prompt)
        return OptimizeResponse(optimized_prompt=optimized)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/decompose", response_model=DecomposeResponse)
async def decompose_prompt(request: DecomposeRequest, background_tasks: BackgroundTasks):
    try:
        return research_service.decompose_prompt(request.prompt, request.depth, background_tasks)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/tasks", response_model=List[SearchTask])
async def list_tasks():
    return task_manager.get_all_tasks()

@app.get("/v1/tasks/{task_id}", response_model=SearchTask)
async def get_task(task_id: str):
    task = task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.patch("/v1/tasks/{task_id}", response_model=SearchTask)
async def update_task(task_id: str, update: TaskUpdate):
    task = task_manager.update_task(task_id, update)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.post("/v1/research", response_model=ResearchResponse)
async def start_research(request: ResearchRequest, background_tasks: BackgroundTasks):
    try:
        return research_service.start_research(request, background_tasks)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/research/{research_id}", response_model=ResearchRecord)
async def get_research_status(research_id: str):
    return research_service.get_research_status(research_id)
