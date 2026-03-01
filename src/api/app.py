from fastapi import FastAPI, HTTPException
from typing import List
from src.api.schemas import (
    OptimizeRequest, OptimizeResponse, 
    DecomposeRequest, DecomposeResponse, 
    SearchTask, TaskUpdate
)
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.providers.deepseek import DeepSeekProvider
from src.config import settings
from src.core.task_manager import task_manager

app = FastAPI(title=settings.app_name, debug=settings.debug)

try:
    llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
    agent_optimizer = PromptOptimizerAgent(llm)
    agent_orchestrator = OrchestratorAgent(llm)
except Exception as e:
    print(f"Warning: Failed to initialize agents: {e}")

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/v1/optimize", response_model=OptimizeResponse)
async def optimize_prompt(request: OptimizeRequest):
    try:
        optimized = agent_optimizer.run(request.prompt)
        return OptimizeResponse(optimized_prompt=optimized)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/decompose", response_model=DecomposeResponse)
async def decompose_prompt(request: DecomposeRequest):
    try:
        tasks_raw = agent_orchestrator.run_decompose(request.prompt, request.depth)
        
        registered_tasks = []
        for task_dict in tasks_raw:
            task = task_manager.add_task(task_dict)
            registered_tasks.append(task)
            
        return DecomposeResponse(tasks=registered_tasks, depth=request.depth)
    except Exception as e:
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
