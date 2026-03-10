from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException
from src.api.schemas import (
    OptimizeRequest, OptimizeResponse, 
    DecomposeRequest, DecomposeResponse, 
    SearchTask, TaskUpdate, SearchDepth, TaskStatus,
    ResearchRequest, ResearchResponse, ResearchRecord, ResearchStatus
)
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.search import SearchAgent
from src.agents.analyzer import AnalyzerAgent
from src.providers.deepseek import DeepSeekProvider
from src.config import settings
from src.core.task_manager import task_manager

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


def require_agent(agent, agent_name: str):
    if agent is None:
        raise HTTPException(
            status_code=503,
            detail=f"{agent_name} is unavailable. Check service configuration.",
        )
    return agent

@app.get("/health")
def health_check():
    return {"status": "ok"}

def run_search_task(task_id: str, depth: SearchDepth):
    source_limit_map = {
        SearchDepth.EASY: 5,
        SearchDepth.MEDIUM: 12,
        SearchDepth.HARD: 20
    }
    limit = source_limit_map.get(depth, 5)
    agent = SearchAgent(max_sources=limit)
    agent.run_task(task_id)

@app.post("/v1/optimize", response_model=OptimizeResponse)
async def optimize_prompt(request: OptimizeRequest):
    try:
        optimizer = require_agent(agent_optimizer, "Prompt optimizer")
        optimized = optimizer.run(request.prompt)
        return OptimizeResponse(optimized_prompt=optimized)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/decompose", response_model=DecomposeResponse)
async def decompose_prompt(request: DecomposeRequest, background_tasks: BackgroundTasks):
    try:
        orchestrator = require_agent(agent_orchestrator, "Orchestrator")
        tasks_raw = orchestrator.run_decompose(request.prompt, request.depth)
        
        registered_tasks = []
        for task_dict in tasks_raw:
            task = task_manager.add_task(task_dict)
            registered_tasks.append(task)
            if task.status == TaskStatus.PENDING and task.queries:
                background_tasks.add_task(run_search_task, task.id, request.depth)
            
        return DecomposeResponse(tasks=registered_tasks, depth=request.depth)
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
        orchestrator = require_agent(agent_orchestrator, "Orchestrator")
        # 1. Decompose
        tasks_raw = orchestrator.run_decompose(request.prompt, request.depth)
        
        # 2. Register Research and Tasks
        # We need task IDs first to link them to research, but tasks need research_id.
        # So we generate dummy IDs or just add tasks first.
        task_ids = []
        registered_tasks = []
        
        research = task_manager.add_research(request, task_ids=[])
        
        for task_dict in tasks_raw:
            task_dict["research_id"] = research.id
            task = task_manager.add_task(task_dict)
            registered_tasks.append(task)
            task_ids.append(task.id)
            
        research.task_ids = task_ids
        
        # 3. Start Search Bots
        for task in registered_tasks:
            if task.status == TaskStatus.PENDING and task.queries:
                background_tasks.add_task(run_search_task, task.id, request.depth)
            
        return ResearchResponse(
            research_id=research.id,
            status="success",
            message=f"Research started with {len(registered_tasks)} tasks."
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/research/{research_id}", response_model=ResearchRecord)
async def get_research_status(research_id: str):
    research = task_manager.get_research(research_id)
    if not research:
        raise HTTPException(status_code=404, detail="Research not found")
        
    if research.status in [ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
        return research
        
    # Check if all tasks are done
    tasks = task_manager.get_tasks_by_research(research_id)
    all_done = True
    any_failed = False
    for t in tasks:
        if t.status not in ["completed", "failed"]:
            all_done = False
            break
        if t.status == "failed":
            any_failed = True
            
    if all_done:
        if any_failed and all(t.status == "failed" for t in tasks):
            task_manager.update_research_status(research_id, ResearchStatus.FAILED, "All tasks failed.")
        else:
            # Run AnalyzerAgent sync or async? For now, sync is okay for the getter, but background is better.
            # To avoid blocking the GET request too long, we can do it here if it's fast enough, 
            # but ideally it should be a background task too. Let's do it sync for simplicity of MVP.
            analyzer = require_agent(agent_analyzer, "Analyzer")
            task_manager.update_research_status(research_id, ResearchStatus.ANALYZING)
            try:
                report = analyzer.run_analysis(research.prompt, tasks)
                task_manager.update_research_status(research_id, ResearchStatus.COMPLETED, report)
            except Exception as e:
                task_manager.update_research_status(research_id, ResearchStatus.FAILED, f"Analysis failed: {str(e)}")
                
    # Re-fetch to return latest state
    return task_manager.get_research(research_id)
