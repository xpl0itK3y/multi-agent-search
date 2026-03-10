from src.config import Settings
from src.repositories.factory import create_task_store
from src.repositories.in_memory_task_store import InMemoryTaskStore


def test_create_task_store_returns_memory_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="memory", allow_memory_task_store=True),
    )

    store = create_task_store()

    assert isinstance(store, InMemoryTaskStore)


def test_create_task_store_returns_postgres_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="postgres"),
    )

    store = create_task_store()

    assert store.__class__.__name__ == "SQLAlchemyTaskStore"


def test_create_task_store_rejects_memory_backend_without_opt_in(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="memory", allow_memory_task_store=False, debug=False),
    )

    try:
        create_task_store()
    except ValueError as exc:
        assert "Memory task store is disabled" in str(exc)
    else:
        raise AssertionError("Expected create_task_store() to reject memory backend without opt-in")
