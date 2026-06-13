from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.agents.analyzer import AnalyzerAgent
from src.agents.chat import ChatAgent
from src.agents.clarifier import ClarifierAgent
from src.agents.claim_verifier import ClaimVerifierAgent
from src.agents.comparison import ComparisonAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.red_team import RedTeamAgent
from src.agents.replan import ReplanAgent
from src.agents.report_critic import ReportCriticAgent
from src.agents.source_critic import SourceCriticAgent
from src.brokers.redis_broker import RedisBroker
from src.config import settings
from src.observability import configure_logging
from src.providers.deepseek import DeepSeekProvider
from src.repositories import create_task_store
from src.services import ResearchService


class StaticAnalyzerAgent:
    def __init__(self, report: str):
        self.report = report

    def run_analysis(self, prompt: str, tasks, depth=None, **kwargs) -> str:
        return self.report


def _create_broker() -> RedisBroker | None:
    if not settings.use_redis_broker or not settings.redis_url:
        return None
    try:
        broker = RedisBroker(settings.redis_url, settings.redis_broker_pop_timeout_seconds)
        if broker.ping():
            print(f"Redis broker connected: {settings.redis_url}")
        else:
            print("Warning: Redis broker ping failed — broker disabled, falling back to Postgres polling")
            return None
        return broker
    except Exception as exc:
        print(f"Warning: Failed to initialize Redis broker: {exc} — falling back to Postgres polling")
        return None


def create_research_service() -> ResearchService:
    configure_logging()
    agent_optimizer = None
    agent_orchestrator = None
    agent_analyzer = None
    chat_agent = None
    clarifier_agent = None
    red_team_agent = None
    comparison_agent = None
    replan_agent: ReplanAgent = ReplanAgent()          # template-only fallback
    source_critic = SourceCriticAgent()
    evidence_mapper = EvidenceMapperAgent()
    claim_verifier = ClaimVerifierAgent()
    report_critic = ReportCriticAgent()

    if settings.smoke_analyzer_report:
        agent_analyzer = StaticAnalyzerAgent(settings.smoke_analyzer_report)

    try:
        llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        agent_optimizer = PromptOptimizerAgent(llm)
        agent_orchestrator = OrchestratorAgent(llm)
        replan_agent = ReplanAgent(llm=llm)            # Q-4: LLM-backed gap queries
        chat_agent = ChatAgent(llm)                    # grounded follow-up Q&A
        clarifier_agent = ClarifierAgent(llm)          # pre-plan clarifying questions
        red_team_agent = RedTeamAgent(llm)             # adversarial counter-evidence pass
        comparison_agent = ComparisonAgent(llm)        # structured comparison table
        if agent_analyzer is None:
            agent_analyzer = AnalyzerAgent(
                llm,
                source_critic=source_critic,
                evidence_mapper=evidence_mapper,
                claim_verifier=claim_verifier,
                report_critic=report_critic,
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
        report_critic=report_critic,
        replan_agent=replan_agent,
        chat_agent=chat_agent,
        clarifier=clarifier_agent,
        red_team_agent=red_team_agent,
        comparison_agent=comparison_agent,
        broker=_create_broker(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    service = create_research_service()
    app.state.research_service = service
    # Recover any decompositions that were lost when the previous process terminated (R-1).
    try:
        recovered = service.recover_pending_decompositions()
        if recovered:
            print(f"Startup: recovered {recovered} pending decomposition(s)")
    except Exception as exc:
        print(f"Warning: startup decomposition recovery failed: {exc}")
    yield
