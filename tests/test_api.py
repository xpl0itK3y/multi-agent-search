import pytest
from fastapi.testclient import TestClient

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_optimize_endpoint(client, mocker):
    # Мокаем работу агента внутри приложения
    mock_run = mocker.patch("src.api.app.agent.run")
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
