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
    mock_decompose = mocker.patch("src.api.app.agent_orchestrator.run_decompose")
    mock_decompose.return_value = [
        {"id": "123", "description": "Search for X", "queries": ["query X"], "status": "pending"},
        {"id": "456", "description": "Search for Y", "queries": ["query Y"], "status": "pending"}
    ]
    
    payload = {"prompt": "complex research", "depth": "easy"}
    response = client.post("/v1/decompose", json=payload)
    
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 2
    assert response.json()["tasks"][0]["id"] == "123"
    assert response.json()["tasks"][0]["status"] == "pending"
    assert response.json()["depth"] == "easy"
