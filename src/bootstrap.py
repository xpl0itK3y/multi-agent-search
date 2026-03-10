from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.agents.analyzer import AnalyzerAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.config import settings
from src.providers.deepseek import DeepSeekProvider
from src.repositories import create_task_store
from src.services import ResearchService


class StaticAnalyzerAgent:
    def __init__(self, report: str):
        self.report = report

    def run_analysis(self, prompt: str, tasks) -> str:
        return self.report


def create_research_service() -> ResearchService:
    agent_optimizer = None
    agent_orchestrator = None
    agent_analyzer = None

    if settings.smoke_analyzer_report:
        agent_analyzer = StaticAnalyzerAgent(settings.smoke_analyzer_report)

    try:
        llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        agent_optimizer = PromptOptimizerAgent(llm)
        agent_orchestrator = OrchestratorAgent(llm)
        if agent_analyzer is None:
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
