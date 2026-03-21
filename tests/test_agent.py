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


def test_orchestrator_agent_shapes_docs_queries_toward_official_reference(mocker, mock_llm):
    agent = OrchestratorAgent(mock_llm)
    mock_response = (
        '[{"description": "Сравнение FastAPI и Flask для REST API", '
        '"queries": ["FastAPI vs Flask performance benchmark 2024", "Flask FastAPI speed comparison REST API", "Python API framework performance comparison"]}]'
    )

    mocker.patch.object(mock_llm, "generate", return_value=mock_response)

    tasks = agent.run_decompose("Сравни FastAPI и Flask для небольшого REST API: что выбрать и почему?", SearchDepth.MEDIUM)

    queries = tasks[0]["queries"]
    assert "FastAPI official documentation REST API reference" in queries
    assert "Flask official documentation REST API patterns" in queries
    assert any("comparison" in query.lower() or "vs" in query.lower() for query in queries)
    assert len(queries) <= 3


def test_orchestrator_agent_shapes_fastapi_feature_task_queries_toward_docs(mocker, mock_llm):
    agent = OrchestratorAgent(mock_llm)
    mock_response = (
        '[{"description": "Основные характеристики и возможности FastAPI для REST API", '
        '"queries": ["FastAPI features for REST API development", "FastAPI vs other Python frameworks performance", "FastAPI async capabilities REST API"]}]'
    )

    mocker.patch.object(mock_llm, "generate", return_value=mock_response)

    tasks = agent.run_decompose("Сравни FastAPI и Flask для небольшого REST API: что выбрать и почему?", SearchDepth.MEDIUM)

    queries = tasks[0]["queries"]
    assert "FastAPI official documentation async reference" in queries
    assert any("performance" in query.lower() or "comparison" in query.lower() for query in queries)
    assert len(queries) <= 3
