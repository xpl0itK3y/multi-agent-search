from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.agents.analyzer import AnalyzerAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.config import settings
from src.providers.deepseek import DeepSeekProvider
from src.repositories import create_task_store
from src.services import ResearchService


def create_research_service() -> ResearchService:
    agent_optimizer = None
    agent_orchestrator = None
    agent_analyzer = None

    try:
        llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        agent_optimizer = PromptOptimizerAgent(llm)
        agent_orchestrator = OrchestratorAgent(llm)
        agent_analyzer = AnalyzerAgent(llm)
    except Exception as exc:
        print(f"Warning: Failed to initialize agents: {exc}")

    return ResearchService(
        task_store=create_task_store(),
        optimizer=agent_optimizer,
        orchestrator=agent_orchestrator,
        analyzer=agent_analyzer,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.research_service = create_research_service()
    yield
