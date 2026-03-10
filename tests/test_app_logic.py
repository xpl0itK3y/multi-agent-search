import pytest
from fastapi import BackgroundTasks, HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import ResearchRequest, SearchDepth, SearchTask, TaskStatus
from src.core.llm import LLMProvider
from src.repositories import InMemoryTaskStore
from src.services.research_service import ResearchService


class RecordingLLM(LLMProvider):
    def __init__(self, response: str = "ok"):
        self.response = response
        self.calls = []

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "kwargs": kwargs,
            }
        )
        return self.response


def test_decompose_requires_initialized_orchestrator(mocker):
    service = ResearchService(task_store=InMemoryTaskStore(), orchestrator=None)

    with pytest.raises(HTTPException) as exc_info:
        service.decompose_prompt(
            "test",
            SearchDepth.EASY,
            BackgroundTasks(),
        )

    assert exc_info.value.status_code == 503


def test_analyzer_agent_uses_llm_provider_contract():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert result == "report"
    assert len(llm.calls) == 1
    assert llm.calls[0]["system_prompt"] == agent.SYSTEM_PROMPT
    assert "original prompt" in llm.calls[0]["user_prompt"]
    assert llm.calls[0]["kwargs"]["temperature"] == 0.3


def test_decompose_does_not_schedule_failed_tasks(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "bad task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    ]
    service = ResearchService(task_store=InMemoryTaskStore(), orchestrator=orchestrator)
    background_tasks = BackgroundTasks()

    response = service.decompose_prompt(
        "test",
        SearchDepth.EASY,
        background_tasks,
    )

    assert len(response.tasks) == 1
    assert response.tasks[0].status == TaskStatus.FAILED
    assert background_tasks.tasks == []


def test_start_research_persists_task_ids_in_task_store(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "task one",
            "queries": ["query one"],
            "status": TaskStatus.PENDING,
        },
        {
            "id": "task-2",
            "description": "task two",
            "queries": ["query two"],
            "status": TaskStatus.PENDING,
        },
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)

    response = service.start_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        BackgroundTasks(),
    )

    research = task_store.get_research(response.research_id)
    assert research is not None
    assert research.task_ids == ["task-1", "task-2"]
