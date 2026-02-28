from fastapi import FastAPI, HTTPException
from src.api.schemas import OptimizeRequest, OptimizeResponse
from src.agents.optimizer import PromptOptimizerAgent
from src.providers.deepseek import DeepSeekProvider
from src.config import settings

app = FastAPI(title=settings.app_name, debug=settings.debug)

try:
    llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
    agent = PromptOptimizerAgent(llm)
except Exception as e:
    print(f"Warning: Failed to initialize agent: {e}")

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/v1/optimize", response_model=OptimizeResponse)
async def optimize_prompt(request: OptimizeRequest):
    try:
        optimized = agent.run(request.prompt)
        return OptimizeResponse(optimized_prompt=optimized)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
