from fastapi import FastAPI, HTTPException
from src.api.schemas import OptimizeRequest, OptimizeResponse, DecomposeRequest, DecomposeResponse
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.providers.deepseek import DeepSeekProvider
from src.config import settings

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
        tasks = agent_orchestrator.run_decompose(request.prompt, request.depth)
        return DecomposeResponse(tasks=tasks, depth=request.depth)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
