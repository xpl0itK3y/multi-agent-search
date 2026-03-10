import pytest
from fastapi.testclient import TestClient

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_optimize_endpoint(client, mocker):
    # Мокаем работу агента внутри приложения
    mock_run = mocker.patch("src.api.app.agent_optimizer.run")
    mock_run.return_value = "Optimized version"
    
    payload = {"prompt": "raw input"}
    response = client.post("/v1/optimize", json=payload)
    
    assert response.status_code == 200
    assert response.json()["optimized_prompt"] == "Optimized version"
    assert response.json()["status"] == "success"

def test_optimize_invalid_payload(client):
    # Тест на пустой промпт
    response = client.post("/v1/optimize", json={"prompt": ""})
    assert response.status_code == 422 # Validation error

def test_decompose_endpoint(client, mocker):
    # Мокаем работу оркестратора
    mock_decompose = mocker.patch("src.api.app.agent_orchestrator.run_decompose")
    mock_decompose.return_value = [
        {"id": "test-id", "description": "Search for X", "queries": ["query X"], "status": "pending"},
    ]
    
    # Мокаем фоновую задачу
    mock_bg = mocker.patch("src.api.app.run_search_task")
    
    payload = {"prompt": "test query", "depth": "easy"}
    response = client.post("/v1/decompose", json=payload)
    
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1
    # Проверяем, что фоновая задача была запланирована
    mock_bg.assert_called_once()

def test_research_endpoints(client, mocker):
    from src.api.schemas import ResearchStatus
    # Mock orchestrator
    mock_decompose = mocker.patch("src.api.app.agent_orchestrator.run_decompose")
    mock_decompose.return_value = [
        {"id": "task-1", "description": "Search for X", "queries": ["query X"], "status": "pending"},
    ]
    
    mock_bg = mocker.patch("src.api.app.run_search_task")
    
    # Start research
    payload = {"prompt": "test research", "depth": "easy"}
    response = client.post("/v1/research", json=payload)
    
    assert response.status_code == 200
    res_data = response.json()
    assert "research_id" in res_data
    assert res_data["status"] == "success"
    
    research_id = res_data["research_id"]
    
    # Get research status (tasks pending)
    response_get = client.get(f"/v1/research/{research_id}")
    assert response_get.status_code == 200
    assert response_get.json()["status"] == ResearchStatus.PROCESSING
    
    # Update task to completed
    from src.api.schemas import TaskUpdate
    tasks = client.app.state.research_service.task_manager.get_tasks_by_research(research_id)
    assert len(tasks) == 1
    client.app.state.research_service.task_manager.update_task(tasks[0].id, TaskUpdate(status="completed", result=[{"content": "data", "url": "http://a.com"}], log="done"))
    
    # Mock analyzer
    mock_analysis = mocker.patch("src.api.app.agent_analyzer.run_analysis")
    mock_analysis.return_value = "Final structured report"
    
    # Get research status again, this should trigger analysis
    response_get2 = client.get(f"/v1/research/{research_id}")
    assert response_get2.status_code == 200
    assert response_get2.json()["status"] == ResearchStatus.COMPLETED
    assert response_get2.json()["final_report"] == "Final structured report"

def test_search_agent_integration(mocker):
    # Тестируем SearchAgent отдельно с моками провайдеров
    from src.agents.search import SearchAgent
    from src.repositories import InMemoryTaskStore
    
    task_store = InMemoryTaskStore()
    task_id = "test-agent-id"
    task_store.add_task({
        "id": task_id,
        "description": "test",
        "queries": ["query"],
        "status": "pending"
    })
    
    mock_search = mocker.patch("src.providers.search.SearchProvider.search")
    mock_search.return_value = [{"url": "http://example.com", "title": "Example"}]
    
    mock_extract = mocker.patch("src.providers.search.ContentExtractor.extract_content")
    mock_extract.return_value = "Full page content"
    
    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task(task_id)
    
    final_task = task_store.get_task(task_id)
    assert final_task.status == "completed"
    assert final_task.result[0]["content"] == "Full page content"
    assert "Search completed" in final_task.logs[-1]
