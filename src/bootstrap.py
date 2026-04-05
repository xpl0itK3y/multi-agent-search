from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.agents.analyzer import AnalyzerAgent
from src.agents.claim_verifier import ClaimVerifierAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.replan import ReplanAgent
from src.agents.source_critic import SourceCriticAgent
from src.config import settings
from src.observability import configure_logging
from src.providers.deepseek import DeepSeekProvider
from src.repositories import create_task_store
from src.services import ResearchService


class StaticAnalyzerAgent:
    def __init__(self, report: str):
        self.report = report

    def run_analysis(self, prompt: str, tasks, depth=None) -> str:
        return self.report


def create_research_service() -> ResearchService:
    configure_logging()
    agent_optimizer = None
    agent_orchestrator = None
    agent_analyzer = None
    source_critic = SourceCriticAgent()
    evidence_mapper = EvidenceMapperAgent()
    claim_verifier = ClaimVerifierAgent()
    replan_agent = ReplanAgent()

    if settings.smoke_analyzer_report:
        agent_analyzer = StaticAnalyzerAgent(settings.smoke_analyzer_report)

    try:
        llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        agent_optimizer = PromptOptimizerAgent(llm)
        agent_orchestrator = OrchestratorAgent(llm)
        if agent_analyzer is None:
            agent_analyzer = AnalyzerAgent(
                llm,
                source_critic=source_critic,
                evidence_mapper=evidence_mapper,
                claim_verifier=claim_verifier,
            )
    except Exception as exc:
        print(f"Warning: Failed to initialize agents: {exc}")

    return ResearchService(
        task_store=create_task_store(),
        optimizer=agent_optimizer,
        orchestrator=agent_orchestrator,
        analyzer=agent_analyzer,
        source_critic=source_critic,
        evidence_mapper=evidence_mapper,
        claim_verifier=claim_verifier,
        replan_agent=replan_agent,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.research_service = create_research_service()
    yield
