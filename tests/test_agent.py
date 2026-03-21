from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.api.schemas import SearchDepth
import json

def test_optimizer_agent_run(mock_llm):
    agent = PromptOptimizerAgent(mock_llm)
    user_input = "test prompt"
    result = agent.run(user_input)
    
    assert "Optimized: test prompt" in result
    assert agent.SYSTEM_PROMPT is not None

def test_orchestrator_agent_decompose(mocker, mock_llm):
    agent = OrchestratorAgent(mock_llm)
    mock_response = '[{"description": "task 1", "queries": ["query 1"]}, {"description": "task 2", "queries": ["query 2"]}]'
    
    mocker.patch.object(mock_llm, "generate", return_value=mock_response)
    
    tasks = agent.run_decompose("complex task", SearchDepth.EASY)
    
    assert len(tasks) == 2
    assert tasks[0]["description"] == "task 1"
    assert "id" in tasks[0]
    assert tasks[0]["status"] == "pending"


def test_orchestrator_agent_rewrites_description_when_language_mismatches(mocker, mock_llm):
    agent = OrchestratorAgent(mock_llm)
    mock_response = '[{"description": "Comparar marcos de trabajo para APIs", "queries": ["fastapi django comparison"]}]'

    mocker.patch.object(mock_llm, "generate", return_value=mock_response)

    tasks = agent.run_decompose("Compare frameworks for Python APIs", SearchDepth.EASY)

    assert tasks[0]["description"].startswith("Search angle 1:")
    assert "Compare frameworks for Python APIs" in tasks[0]["description"]


def test_orchestrator_agent_keeps_matching_description_language(mocker, mock_llm):
    agent = OrchestratorAgent(mock_llm)
    mock_response = '[{"description": "Compare frameworks for Python APIs", "queries": ["fastapi django comparison"]}]'

    mocker.patch.object(mock_llm, "generate", return_value=mock_response)

    tasks = agent.run_decompose("Compare frameworks for Python APIs", SearchDepth.EASY)

    assert tasks[0]["description"] == "Compare frameworks for Python APIs"
