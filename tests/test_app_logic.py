import asyncio

import pytest
from fastapi import BackgroundTasks, HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.api.app import decompose_prompt
from src.api.schemas import DecomposeRequest, SearchDepth, SearchTask, TaskStatus
from src.core.llm import LLMProvider


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
    mocker.patch("src.api.app.agent_orchestrator", None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            decompose_prompt(
                DecomposeRequest(prompt="test", depth=SearchDepth.EASY),
                BackgroundTasks(),
            )
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
