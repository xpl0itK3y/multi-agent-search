"""AUD-036: /health probes DB + Redis and reports degraded when a dependency is down."""
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def test_health_ok_when_db_up_no_broker():
    h = ResearchService(task_store=InMemoryTaskStore()).get_health_status()
    assert h["status"] == "ok"
    assert h["dependencies"]["database"] == "ok"
    assert h["dependencies"]["redis"] == "disabled"


def test_health_degraded_when_db_down(monkeypatch):
    store = InMemoryTaskStore()
    monkeypatch.setattr(store, "ping", lambda: False)
    h = ResearchService(task_store=store).get_health_status()
    assert h["status"] == "degraded"
    assert h["dependencies"]["database"] == "down"


def test_health_degraded_when_redis_down(monkeypatch):
    class _Broker:
        def ping(self):
            return False

    h = ResearchService(task_store=InMemoryTaskStore(), broker=_Broker()).get_health_status()
    assert h["status"] == "degraded"
    assert h["dependencies"]["redis"] == "down"
