from src import bootstrap
from src.config import settings
from src.repositories import InMemoryTaskStore


def test_bootstrap_marks_missing_llm_configuration_unavailable(monkeypatch):
    monkeypatch.setattr(settings, "deepseek_api_key", None)
    monkeypatch.setattr(bootstrap, "create_task_store", InMemoryTaskStore)
    monkeypatch.setattr(bootstrap, "_create_broker", lambda: None)

    service = bootstrap.create_research_service()

    assert service.llm_available is False
    health = service.get_health_status()
    assert health["status"] == "degraded"
    assert health["dependencies"]["llm"] == "down"
